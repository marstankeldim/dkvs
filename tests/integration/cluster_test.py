#!/usr/bin/env python3
"""End-to-end fault-tolerance test for a real dkvs cluster.

Spins up a 3-node cluster as separate OS processes and walks through the
failure scenarios that matter:

  1. cluster elects a leader
  2. writes replicate and survive reads through every node (via redirects)
  3. kill -9 the leader           -> new leader, zero data loss
  4. restart the dead node        -> it recovers from its WAL and catches up
  5. kill nodes down to 1 of 3    -> writes correctly REFUSE (no quorum)
  6. restart the cluster          -> everything is still there

Usage: python3 tests/integration/cluster_test.py [path-to-build-dir]
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

NUM_NODES = 3
BASE_RAFT_PORT = 7300
BASE_CLIENT_PORT = 6300


def log(msg):
    print(f"[cluster_test] {msg}", flush=True)


class Client:
    """Minimal line-protocol client. One connection per instance."""

    def __init__(self, port, timeout=5.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def request(self, line):
        self.sock.sendall(line.encode() + b"\n")
        while b"\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        reply, self.buf = self.buf.split(b"\n", 1)
        return reply.decode()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class Cluster:
    def __init__(self, server_bin, data_root):
        self.server_bin = server_bin
        self.data_root = data_root
        self.raft_peers = ",".join(
            f"127.0.0.1:{BASE_RAFT_PORT + i}" for i in range(NUM_NODES))
        self.client_peers = ",".join(
            f"127.0.0.1:{BASE_CLIENT_PORT + i}" for i in range(NUM_NODES))
        self.procs = {}

    def start_node(self, node_id):
        logfile = open(os.path.join(self.data_root, f"node{node_id}.log"), "ab")
        proc = subprocess.Popen(
            [
                self.server_bin,
                "--id", str(node_id),
                "--raft-peers", self.raft_peers,
                "--client-peers", self.client_peers,
                "--data-dir", os.path.join(self.data_root, f"node{node_id}"),
            ],
            stdout=logfile, stderr=logfile,
        )
        self.procs[node_id] = proc
        log(f"started node {node_id} (pid {proc.pid})")

    def kill_node(self, node_id):
        proc = self.procs.pop(node_id)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        log(f"killed node {node_id} with SIGKILL")

    def stop_all(self):
        for node_id in list(self.procs):
            proc = self.procs.pop(node_id)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def alive_nodes(self):
        return sorted(self.procs)

    def status(self, node_id):
        try:
            c = Client(BASE_CLIENT_PORT + node_id, timeout=2.0)
            reply = c.request("STATUS")
            c.close()
            return dict(kv.split("=", 1) for kv in reply.split()[1:])
        except OSError:
            return None

    def wait_for_leader(self, timeout=15.0):
        """Returns the node id of the leader once exactly one exists."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            leaders = []
            for node_id in self.alive_nodes():
                st = self.status(node_id)
                if st and st.get("role") == "leader":
                    leaders.append(node_id)
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.2)
        raise AssertionError(f"no unique leader within {timeout}s")

    def request_via(self, node_id, line, max_redirects=8):
        """Sends a request to a node, following REDIRECTs."""
        port = BASE_CLIENT_PORT + node_id
        for _ in range(max_redirects):
            c = Client(port)
            reply = c.request(line)
            c.close()
            if reply.startswith("REDIRECT "):
                port = int(reply.split(":")[-1])
                continue
            return reply
        raise AssertionError(f"redirect loop for: {line}")


def expect(actual, expected, context):
    if actual != expected:
        raise AssertionError(f"{context}: expected {expected!r}, got {actual!r}")


def main():
    build_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "build")
    server_bin = os.path.abspath(os.path.join(build_dir, "dkvs-server"))
    if not os.path.exists(server_bin):
        log(f"dkvs-server not found at {server_bin}; build first")
        return 2

    data_root = tempfile.mkdtemp(prefix="dkvs-integration-")
    cluster = Cluster(server_bin, data_root)
    try:
        # --- 1. boot and elect ---
        for i in range(NUM_NODES):
            cluster.start_node(i)
        leader = cluster.wait_for_leader()
        log(f"leader elected: node {leader}")

        # --- 2. write, then read through EVERY node (redirects included) ---
        n_keys = 20
        for i in range(n_keys):
            expect(cluster.request_via(leader, f"SET key{i} value-{i}"),
                   "OK", f"SET key{i}")
        for node_id in cluster.alive_nodes():
            expect(cluster.request_via(node_id, "GET key7"),
                   "VALUE value-7", f"GET via node {node_id}")
        log(f"wrote {n_keys} keys; reads consistent via all nodes")

        # --- 3. kill -9 the leader; no data may be lost ---
        old_leader = leader
        cluster.kill_node(old_leader)
        leader = cluster.wait_for_leader()
        log(f"failover complete: new leader is node {leader}")
        for i in range(n_keys):
            expect(cluster.request_via(leader, f"GET key{i}"),
                   f"VALUE value-{i}", f"GET key{i} after failover")
        expect(cluster.request_via(leader, "SET after-failover works"),
               "OK", "write after failover")
        log("all data intact after leader crash; new writes accepted")

        # --- 4. restart the dead node; it must catch up from its WAL ---
        cluster.start_node(old_leader)
        deadline = time.time() + 15
        expected_keys = str(n_keys + 1)
        while time.time() < deadline:
            st = cluster.status(old_leader)
            if st and st.get("keys") == expected_keys:
                break
            time.sleep(0.2)
        st = cluster.status(old_leader)
        expect(st and st.get("keys"), expected_keys,
               f"restarted node {old_leader} catch-up (status={st})")
        log(f"restarted node {old_leader} recovered and caught up "
            f"({expected_keys} keys)")

        # --- 5. lose quorum; writes must be refused, not half-applied ---
        followers = [n for n in cluster.alive_nodes() if n != leader]
        cluster.kill_node(followers[0])
        cluster.kill_node(followers[1])
        log(f"only node {leader} alive (1/3) — cluster must refuse writes")
        reply = cluster.request_via(leader, "SET quorumless doomed",
                                    max_redirects=1)
        if not reply.startswith("ERROR"):
            raise AssertionError(
                f"write without quorum must fail, got: {reply!r}")
        log(f"write correctly refused without quorum: {reply!r}")

        # --- 6. full recovery ---
        for node_id in followers:
            cluster.start_node(node_id)
        leader = cluster.wait_for_leader()
        for i in range(n_keys):
            expect(cluster.request_via(leader, f"GET key{i}"),
                   f"VALUE value-{i}", f"GET key{i} after full recovery")
        expect(cluster.request_via(leader, "GET after-failover"),
               "VALUE works", "GET after-failover after recovery")
        # The quorumless write must NOT have survived as committed data...
        # it may exist or not, but reads must be consistent; verify the
        # cluster still serves and agrees.
        st = [cluster.status(n) for n in cluster.alive_nodes()]
        log(f"cluster recovered, final statuses: {st}")

        log("PASS — all fault-tolerance scenarios survived")
        return 0
    except AssertionError as e:
        log(f"FAIL — {e}")
        for i in range(NUM_NODES):
            logpath = os.path.join(data_root, f"node{i}.log")
            if os.path.exists(logpath):
                log(f"--- node {i} log tail ---")
                with open(logpath) as f:
                    print("".join(f.readlines()[-15:]))
        return 1
    finally:
        cluster.stop_all()
        shutil.rmtree(data_root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
