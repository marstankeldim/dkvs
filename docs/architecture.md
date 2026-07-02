# Architecture

This document explains how dkvs works end to end: the layers, the threading
model, the on-disk formats, the wire protocols, and the reasoning behind the
design choices. It assumes you have skimmed the
[Raft paper](https://raft.github.io/raft.pdf); section references (§) point
into it.

## 1. Layers

```text
┌─────────────────────────────────────────────────────────────┐
│ dkvs-server (src/main.cpp)                                  │
│  ┌───────────────────────┐  ┌────────────────────────────┐  │
│  │ client server         │  │ raft RPC server            │  │
│  │ (Server, protocol)    │  │ (Server::handleRaftConn)   │  │
│  └──────────┬────────────┘  └──────────┬─────────────────┘  │
│             │ submit / wait            │ onRequestVote /    │
│             ▼                          ▼ onAppendEntries    │
│  ┌──────────────────────────────────────────────┐           │
│  │ RaftNode (raft.cpp) — consensus core         │           │
│  │   elections · replication · commit · apply   │           │
│  └───────┬─────────────────────────┬────────────┘           │
│          │ persist                 │ send RPCs              │
│          ▼                         ▼                        │
│  ┌───────────────┐        ┌────────────────┐                │
│  │ Storage       │        │ Transport      │                │
│  │ meta + WAL    │        │ (TCP / loopback)│               │
│  └───────────────┘        └────────────────┘                │
│          apply committed entries                            │
│          ▼                                                  │
│  ┌───────────────┐                                          │
│  │ KVStore       │  in-memory state machine                 │
│  └───────────────┘                                          │
└─────────────────────────────────────────────────────────────┘
```

Dependency rules that keep the system testable:

- `KVStore` knows nothing about Raft, sockets, or disk.
- `RaftNode` talks to the network only through the abstract `Transport`
  interface — the consensus tests swap in an in-process loopback transport
  with severable links, so partitions are simulated deterministically.
- `Storage` owns all disk I/O; `RaftNode` decides *what* is durable,
  `Storage` decides *how*.

## 2. The life of a command

1. A client sends `SET account:1 500` to any node.
2. If that node isn't the leader it answers `REDIRECT <leader addr>` (or an
   error if no leader is known) and the client retries there.
3. The leader's client handler calls `RaftNode::submit()`. The command is
   appended to the leader's log and **fsynced before anything else happens**.
   The handler then parks on a condition variable, keyed by log index.
4. Per-peer replicator threads ship the entry via `AppendEntries`. Each
   follower verifies the log-matching check (§5.3), fsyncs, and acks.
5. When a majority has the entry, the leader advances `commitIndex` (only
   for current-term entries — §5.4.2).
6. Every node's applier thread feeds newly committed entries, in log order,
   to its `KVStore`. On the leader, the applier also fulfills the parked
   client handler with the result (the value for `GET`, existence for `DEL`).
7. The handler wakes and writes the response line.

If leadership was lost meanwhile and a different entry committed at that
index, the term recorded at apply time won't match the term at submit time —
the client gets `ERROR lost leadership, retry`, never a false `OK`.

### Why GETs go through the log

Serving reads straight from the leader's local map looks tempting, but a
leader stranded behind a partition may not know it was deposed — it would
happily serve values the *new* leader has already overwritten. Routing reads
through the log means a read commits (majority round-trip) before answering,
which proves the responding node was still the leader at that point in log
order. That is what makes operations linearizable. The cost — one consensus
round per read — is the documented trade-off; ReadIndex and leader leases
(§8) are the standard optimizations and are deliberate future work.

## 3. Threading model

All Raft state is guarded by **one mutex**, and the invariant that makes the
design deadlock-free is simple: **the lock is never held across network or
blocking I/O** (the one exception: WAL fsyncs, held intentionally so
persistence is ordered with state changes).

Threads in a running node:

| Thread                | Count       | Job |
| --------------------- | ----------- | --- |
| ticker                | 1           | fires an election when the randomized timeout expires |
| applier               | 1           | applies committed entries to `KVStore`, wakes client waiters |
| replicator            | 1 per peer (leader only) | ships `AppendEntries` batches + heartbeats to one peer |
| vote sender           | 1 per peer per election (short-lived) | sends one `RequestVote`, reports back |
| raft RPC connection   | 1 per inbound peer connection | decodes a frame, calls `onRequestVote`/`onAppendEntries`, replies |
| client connection     | 1 per client | parses lines, submits commands, waits for apply |

Replicators wait on a condition variable with a heartbeat-interval timeout:
a new submission notifies them immediately (low latency), and if nothing
happens the timeout doubles as the heartbeat. When leadership is lost, the
role/term check at the top of the loop retires them; a stopping node counts
in-flight detached threads and waits for them to drain.

## 4. Persistence

Each node owns a data directory with two files.

**`meta`** — `currentTerm` and `votedFor`. Rewritten atomically on every
change: write `meta.tmp`, fsync, `rename()`, fsync the directory. It must
never be half-written — a node that forgets its vote could vote twice in one
term and elect two leaders.

**`wal`** — the replicated log, append-only:

```text
record  := [u32 payload_len][payload][u32 crc32(payload)]
payload := [u64 term][u32 cmd_len][cmd bytes]
```

Appends are fsynced **before** the entry is acknowledged to the leader (or,
on the leader, before replication starts). Raft's Log Matching property
assumes an acked entry is durable; an entry lost after acking would silently
break the majority-intersection argument that makes commits safe.
On macOS, `F_FULLFSYNC` is used instead of `fsync()` — plain fsync on Darwin
does not force the drive cache.

Recovery scans the file record by record. A record whose length field runs
past EOF or whose CRC mismatches is a torn tail write from a crash: it and
everything after it are discarded and the file is truncated. That is safe
precisely because of the fsync-before-ack rule — a torn record was by
definition never acknowledged, so discarding it is indistinguishable from
the write never happening.

Conflict truncation (a follower erasing entries that diverge from the new
leader's log, §5.3) is an `ftruncate()` to the byte offset of the first
conflicting entry; offsets are tracked in memory as records are appended or
loaded.

## 5. Wire protocols

**Client protocol** — newline-delimited text (see README table). Chosen so a
human can drive a node with `nc` and the integration test needs ~30 lines of
Python client.

**Peer protocol** — length-prefixed binary frames: `[u32 length][payload]`,
payload starting with a message-type byte, integers big-endian, strings
u32-length-prefixed. Serialization is hand-rolled (`codec.hpp`) — ~100 lines
that make the entire byte layout visible, instead of a protobuf dependency.
Every decoder returns `std::optional` and rejects truncated or malformed
input; garbage from the network cannot crash a node.

Transport is one short-lived TCP connection per RPC: connect (250 ms
timeout), one request frame, one reply frame (1 s timeout), close. Trivially
robust to peer restarts; connection reuse is a known optimization.

## 6. Correctness notes

Things the implementation is careful about, in the order they usually bite
people:

- **Election restriction (§5.4.1).** A vote is granted only if the
  candidate's log is at least as up-to-date as the voter's. This is what
  guarantees a new leader already holds every committed entry.
- **Commit only current-term entries by counting (§5.4.2).** A leader never
  commits an entry from a previous term by counting replicas — the paper's
  Figure 8 shows how that loses committed data. Instead, each new leader
  appends a no-op entry at the start of its term; committing the no-op
  implicitly commits everything below it.
- **Term checks on every RPC and every reply.** Any message carrying a
  higher term converts the receiver to follower and persists the new term
  before anything else. Stale replies (from an RPC sent in an older term)
  are detected by comparing the term captured at send time and dropped.
- **Randomized election timeouts** (300–600 ms default, re-randomized on
  every reset) break split-vote livelock.
- **Accelerated conflict backup.** A follower rejecting `AppendEntries`
  returns the first index of its conflicting term, so a divergent follower
  is repaired in O(terms) RPCs rather than O(entries).
- **Bounded batches.** A far-behind follower is caught up in batches of 100
  entries, keeping frames bounded.
- **Client acks imply commit, never mere receipt.** `OK` is sent only after
  the entry was applied with the same term it was submitted under.

## 7. What's deliberately missing

| Missing piece | Where it would go | Why it's out of scope |
| --- | --- | --- |
| Log compaction / snapshots (§7) | `Storage` + `InstallSnapshot` RPC | Unbounded WAL is fine at teaching scale; compaction is the natural next phase |
| ReadIndex / lease reads (§8) | `RaftNode::submit` fast path for GETs | Correctness first; optimization second |
| Group commit | `Storage::append` | fsync-per-entry is simpler to reason about; batching fsyncs is a mechanical change |
| Membership changes (§6) | new `AddServer`/`RemoveServer` path | Joint consensus doubles the state space; static clusters keep the core legible |
| Sharding across Raft groups | a router layer above `Server` | Planned phase 8: consistent hashing over independent Raft groups |

## 8. File map

```text
include/dkvs/ + src/
  kv_store.*     in-memory state machine (thread-safe map)
  codec.*        binary encoder/decoder + CRC32
  command.*      state-machine commands and log entries
  storage.*      meta file + write-ahead log, recovery, truncation
  net.*          sockets, framing, line I/O, connect-with-timeout
  raft_msgs.*    RequestVote/AppendEntries structs + ser/de
  transport.*    Transport interface + TCP implementation
  raft.*         the consensus core (elections, replication, commit, apply)
  protocol.*     client text-protocol parser
  server.*       node glue: listeners, client handlers, apply-waiters
  main.cpp       dkvs-server entry point
  client_main.cpp dkvs-cli (REPL + one-shot, follows redirects)

tests/
  *_test.cpp     unit tests (codec, storage, protocol, kv_store)
  raft_test.cpp  in-process cluster tests over a severable loopback network
  integration/cluster_test.py  real processes, real sockets, real kill -9
```
