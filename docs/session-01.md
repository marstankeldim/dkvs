# Session 01 — Project Setup & Phase 1 Start

**Date:** 2026-06-23

---

## What We Covered

### Project Goal
Building a distributed key-value store from scratch in C++20 as a portfolio and learning project. The store will eventually support SET, GET, and DELETE commands across multiple networked nodes.

### Roadmap (High Level)
| Phase | Goal |
|-------|------|
| 1 | Single-node in-memory store |
| 2 | TCP server |
| 3 | Concurrent clients |
| 4 | Sharding across multiple nodes |
| 5 | Replication |
| 6 | Persistence and recovery |
| 7 | Leader election |
| 8 | Fault tolerance testing |

---

## Phase 1 Architecture

Three layers, each with one job:

```
┌─────────────────────────────────┐
│         Client / CLI            │  ← sends raw command strings
└────────────────┬────────────────┘
                 │
┌────────────────▼────────────────┐
│         Command Parser          │  ← turns strings into structured commands
└────────────────┬────────────────┘
                 │
┌────────────────▼────────────────┐
│          KV Store Engine        │  ← executes commands, owns the data
└─────────────────────────────────┘
```

The store should not know or care whether commands came from a CLI, a TCP socket, or a test — this separation pays off in Phase 2.

---

## Project Structure

```
dkvs/
├── CMakeLists.txt
├── src/
│   ├── main.cpp          ← manual driver / REPL loop
│   └── kv_store.cpp      ← implementation of KVStore methods
├── include/
│   └── dkvs/
│       └── kv_store.hpp  ← class declaration
├── docs/
└── tests/
```

---

## Key Design Decisions

| Question | Decision | Reason |
|----------|----------|--------|
| What does GET return for a missing key? | `std::nullopt` (not found) vs value | Must distinguish "missing key" from "key with empty value" |
| How to signal bad input? | Return a result type, not exceptions | Bad client input is expected, not exceptional; exceptions can crash the server |
| Result type? | `std::variant<Value, Error>` | Explicit, composable, works well with C++20 |

---

## What Was Implemented

### `KVStore::set` — completed

**File:** `src/kv_store.cpp`

Replaced the placeholder stub with:

```cpp
data_[key] = value;
```

This stores `value` under `key` in the `unordered_map`. If the key already exists, the old value is replaced.

---

## Concepts Introduced

**`#include`** — loads a toolkit (e.g. `iostream` for printing to screen)

**`int main()`** — entry point of every C++ program; execution starts here

**`std::cout`** — prints output to the screen

**Header file (`.hpp`)** — declares what a class *can do* (the interface)

**Implementation file (`.cpp`)** — defines *how* it does it (the logic)

**`std::unordered_map`** — a dictionary: give it a key, get back a value. O(1) average lookup.

**`-I` compiler flag** — tells the compiler where to search for header files

---

## Build Issue Encountered

```
fatal error: 'dkvs/kv_store.hpp' file not found
```

**Cause:** Compiling `kv_store.cpp` directly with `g++` without telling it where the `include/` folder is.

**Fix:** Use the `-I include` flag, or better — build through CMake which handles this automatically.

**Next step:** Check `CMakeLists.txt` to confirm CMake is wired up correctly.

---

## Next Steps

1. Fix the build (verify CMakeLists.txt and compile successfully)
2. Implement `KVStore::get`
3. Implement `KVStore::remove`
4. Implement `KVStore::contains` and `KVStore::size`
5. Update `main.cpp` to manually test the store
