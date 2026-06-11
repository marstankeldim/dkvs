# Distributed Key-Value Store

An educational distributed key-value store written from scratch in C++20.

The project is intentionally incremental. Each phase should teach one backend or
distributed-systems concept before adding the next layer.

## Roadmap

1. Single-node in-memory key-value store using `std::unordered_map`
2. TCP server that accepts commands from clients
3. Concurrent clients
4. Multiple nodes with sharding
5. Replication
6. Persistence and recovery
7. Leader election
8. Fault tolerance testing

## Phase 1 Goal

Build a local in-memory store supporting:

- `SET key value`
- `GET key`
- `DELETE key`

Networking, replication, persistence, and concurrency are intentionally out of
scope for Phase 1.

## Build

```sh
cmake -S . -B build
cmake --build build
```

## Mentor Rule

This repository is set up so the architecture is visible before the
implementation is filled in. Start by reading [docs/phase-1-design.md](docs/phase-1-design.md).

