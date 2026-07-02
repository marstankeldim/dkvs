# Phase 1 Manual Test Checklist

> Historical note: this checklist is now fully automated in
> `tests/kv_store_test.cpp` (run via `ctest`). Kept as a record of the
> phase-1 design questions.

After implementing `KVStore`, verify these cases manually or with a small test
program:

- `GET` on a missing key returns `std::nullopt`
- `SET name ayan` followed by `GET name` returns `ayan`
- `SET name first`, then `SET name second`, then `GET name` returns `second`
- `DELETE missing` returns `false`
- `DELETE name` after setting `name` returns `true`
- `size()` increases after inserting a new key
- `size()` does not increase when replacing an existing key
- `size()` decreases after deleting an existing key

Stretch question:

- What behavior should empty keys and empty values have?

