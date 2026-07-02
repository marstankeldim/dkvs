# dkvs — Distributed Key-Value Store

[![CI](https://github.com/marstankeldim/dkvs/actions/workflows/ci.yml/badge.svg)](https://github.com/marstankeldim/dkvs/actions/workflows/ci.yml)

A fault-tolerant, replicated key-value store written from scratch in C++20 —
no frameworks, no serialization libraries, no consensus libraries. Replication
and leader election are an original implementation of the
[Raft consensus algorithm](https://raft.github.io/raft.pdf); persistence is a
CRC-checked write-ahead log; the only runtime dependencies are POSIX sockets
and threads.

```text
$ scripts/run_cluster.sh                 # 3 nodes on localhost
$ build/dkvs-cli 127.0.0.1:6100,127.0.0.1:6101,127.0.0.1:6102
dkvs> SET account:1 500
OK
dkvs> STATUS
STATUS role=leader term=1 leader=1 commit=3 applied=3 log=3 keys=1

$ kill -9 <leader pid>                   # crash the leader mid-flight

dkvs> GET account:1                      # a new leader answers — data intact
VALUE 500
```

## What it does

- **Leader election & log replication (Raft).** One node is elected leader;
  every command is appended to a replicated log and acknowledged only after a
  majority of nodes has durably stored it.
- **Crash recovery.** Every node fsyncs entries to a write-ahead log before
  acknowledging them. `kill -9` a node, restart it, and it replays its log
  and rejoins the cluster. Torn or corrupted tail writes are detected by
  per-record CRC32 and discarded safely.
- **Automatic failover.** Kill the leader and the survivors elect a new one
  in a few hundred milliseconds, with zero committed data lost.
- **Linearizable operations.** Reads go through the replicated log like
  writes, so a `GET` observes exactly the writes committed before it — even
  across leader changes. A deposed leader stranded behind a partition can
  never serve stale data as committed.
- **Quorum safety.** With a majority of nodes down, the cluster refuses
  writes rather than diverging — consistency over availability (CP).
- **Client redirects.** Clients may contact any node; non-leaders answer
  `REDIRECT host:port` and `dkvs-cli` follows automatically.

## Architecture

```text
                        ┌──────────────────────── node 0 ─┐
 dkvs-cli ── text ────► │ client server (thread per conn) │
                        │        │ submit                 │
                        │        ▼                        │
                        │   RaftNode ◄──── raft RPCs ─────┼──── peers (nodes 1,2)
                        │    │     │                      │
                        │    │     └── Storage: meta+WAL  │  fsync before ack
                        │    ▼ apply (committed only)     │
                        │   KVStore (state machine)       │
                        └──────────────────────────────────┘
```

Every client command becomes a log entry. The leader replicates it to
followers; once a majority has fsynced it, the entry commits, each node's
applier feeds it to the in-memory `KVStore`, and the client handler that
submitted it gets the result. Details, threading model, and correctness
notes: [docs/architecture.md](docs/architecture.md).

## Build and run

Requires CMake ≥ 3.20 and a C++20 compiler (tested with AppleClang 17 and
GCC on Linux). GoogleTest is fetched automatically for tests.

```sh
cmake -S . -B build
cmake --build build -j

scripts/run_cluster.sh          # 3-node cluster on localhost
build/dkvs-cli 127.0.0.1:6100,127.0.0.1:6101,127.0.0.1:6102
scripts/stop_cluster.sh
```

Single node works too (a majority of 1):

```sh
build/dkvs-server --id 0 --raft-peers 127.0.0.1:7100 \
    --client-peers 127.0.0.1:6100 --data-dir data/solo
```

## Testing

Two layers, because they catch different bugs:

**Unit tests** (`ctest`, 41 tests) — includes consensus tests that run a
whole Raft cluster in-process over a loopback transport whose links can be
cut and healed on demand:

- exactly one stable leader is elected
- commands replicate to every node in the same order
- a partitioned leader is deposed; its unreplicated entries are discarded
- a restarted node recovers term/log from disk and catches up
- torn and corrupted WAL tails are detected and dropped on recovery

```sh
cd build && ctest --output-on-failure
```

**Integration test** — boots a real 3-node cluster as separate processes and
walks through the failure story end-to-end: replicated writes, `kill -9` the
leader, failover with zero data loss, WAL recovery of the dead node, refusal
of writes without quorum, full-cluster recovery:

```sh
python3 tests/integration/cluster_test.py build
```

## Client protocol

Newline-delimited text — you can drive a node with `nc`:

| Request            | Response                                  |
| ------------------ | ----------------------------------------- |
| `SET key value...` | `OK`                                      |
| `GET key`          | `VALUE value...` or `NOT_FOUND`           |
| `DEL key`          | `DELETED` or `NOT_FOUND`                  |
| `PING`             | `PONG`                                    |
| `STATUS`           | `STATUS role=... term=... leader=...`     |
| (any, non-leader)  | `REDIRECT host:port`                      |

Keys cannot contain whitespace; values are the rest of the line. Raft RPCs
between peers use a separate length-prefixed binary protocol
(`include/dkvs/raft_msgs.hpp`).

## Design history

The project was built in deliberate phases, each adding one distributed-
systems concept:

1. ✅ Single-node in-memory store (`KVStore`)
2. ✅ TCP server with a text protocol
3. ✅ Concurrent clients (thread-per-connection, shared-lock store)
4. ✅ Replication across nodes (Raft log replication)
5. ✅ Persistence and recovery (CRC-checked WAL + atomic metadata)
6. ✅ Leader election (Raft, with the §5.4.1 election restriction)
7. ✅ Fault-tolerance testing (partition tests + process-kill integration suite)
8. ⬜ Sharding across multiple Raft groups (consistent hashing) — next up

Early design notes are preserved in [docs/phase-1-design.md](docs/phase-1-design.md).

## Known limitations (deliberate scope cuts)

Honest list of what production systems add that this project does not have —
each is a documented extension point, not an accident:

- **No log compaction / snapshotting** — the WAL grows without bound and a
  restarted node replays from index 1 (Raft §7 / InstallSnapshot).
- **Reads pay full consensus** — linearizable but slow; ReadIndex or leader
  leases (Raft §8) would serve reads without log writes.
- **fsync per entry, no group commit** — throughput is bounded by disk sync
  latency (~1–2k writes/s on a laptop SSD).
- **Connection per RPC** — peers reconnect for every message; persistent
  connections with pipelining would cut latency.
- **Static membership** — no joint-consensus configuration changes (§6).

## References

- Diego Ongaro and John Ousterhout,
  [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) (USENIX ATC 2014)
- [The Raft site](https://raft.github.io/) and its
  [visualization](https://thesecretlivesofdata.com/raft/)
- Ongaro's PhD thesis,
  [*Consensus: Bridging Theory and Practice*](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
  (the accelerated log-backtracking scheme)
