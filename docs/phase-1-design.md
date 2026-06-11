# Phase 1 Design

Phase 1 is a single-process, single-node, in-memory key-value store.

The point is not to build Redis yet. The point is to establish clean boundaries
so later phases can add networking, concurrency, sharding, replication, and
persistence without rewriting everything.

## 1. Overall Architecture

For now, think of the system as three layers:

```text
+------------------+
| CLI / driver     |  Reads commands from a human or test harness.
+------------------+
         |
         v
+------------------+
| Command handling |  Turns text commands into store operations.
+------------------+
         |
         v
+------------------+
| KVStore          |  Owns the in-memory data structure.
+------------------+
```

In Phase 1, only `KVStore` needs real behavior. The CLI can stay tiny, and the
command parser can be added after the store itself is correct.

Later, the architecture will become:

```text
client -> TCP server -> command parser -> KVStore
```

That is why `KVStore` should not know anything about sockets, terminal input, or
text protocol formatting.

## 2. Classes To Create

Start with one main class:

### `dkvs::KVStore`

Responsible for the actual key-value operations:

- `set(key, value)`
- `get(key)`
- `remove(key)`
- `size()`
- maybe `contains(key)` if it helps tests/readability

Suggested internal storage:

```cpp
std::unordered_map<std::string, std::string>
```

This gives average-case O(1) lookup, insertion, and deletion.

## 3. Directory Structure

```text
.
├── CMakeLists.txt
├── README.md
├── docs/
│   └── phase-1-design.md
├── include/
│   └── dkvs/
│       └── kv_store.hpp
├── src/
│   ├── kv_store.cpp
│   └── main.cpp
└── tests/
    └── phase1_manual.md
```

## 4. Component Responsibilities

### `include/dkvs/kv_store.hpp`

Declares the public API. Other parts of the system should depend on this file,
not on private implementation details.

### `src/kv_store.cpp`

Owns the implementation of `KVStore`.

For Phase 1, this file should contain all direct interaction with
`std::unordered_map`.

### `src/main.cpp`

Temporary executable entry point.

For now, use it to manually exercise your store. In Phase 2, this will likely
give way to a TCP server binary or call into one.

### `tests/phase1_manual.md`

A checklist of behavior to verify before moving to networking.

## Design Questions For You

Before implementing, answer these in your own words:

1. Should `GET` return an empty string when a key is missing, or should missing
   keys be represented separately?
2. Should `DELETE` tell the caller whether a key existed?
3. Should empty keys be allowed?
4. Should values be allowed to contain spaces?
5. Which behavior will make the TCP protocol easier later?

Hint: `std::optional<std::string>` is often better than using an empty string to
mean "not found."

## Implementation Step

Implement `KVStore` first, then write a tiny manual driver in `main.cpp`.

Keep the first version boring. Boring code is easier to distribute later.

