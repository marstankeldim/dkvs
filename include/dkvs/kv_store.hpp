#pragma once

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace dkvs {

// Thread-safe in-memory key-value store.
//
// This is the replicated state machine: every node in the cluster applies the
// same sequence of committed log entries to its own KVStore, so all replicas
// converge to the same state. The store knows nothing about sockets, Raft, or
// persistence — it is deliberately the innermost, most boring layer.
class KVStore {
public:
    void set(std::string key, std::string value);
    [[nodiscard]] std::optional<std::string> get(const std::string& key) const;
    [[nodiscard]] bool remove(const std::string& key);
    [[nodiscard]] bool contains(const std::string& key) const;
    [[nodiscard]] std::size_t size() const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> data_;
};

} // namespace dkvs
