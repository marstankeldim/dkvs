#pragma once

#include "dkvs/command.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace dkvs {

// Durable storage for Raft's persistent state, backed by two files in the
// node's data directory:
//
//   meta — currentTerm and votedFor. Rewritten atomically (temp file + rename
//          + fsync) because it is tiny and must never be half-written: voting
//          twice in the same term would elect two leaders.
//
//   wal  — the replicated log, append-only. Each record is:
//              [u32 payload_len][payload][u32 crc32(payload)]
//          where payload = u64 term + u32 cmd_len + cmd bytes.
//          Appends are fsync'd before Raft acknowledges them (an entry a
//          follower acked but lost on crash would break the Log Matching
//          property). On recovery a torn or corrupt tail record is detected
//          by length/CRC checks and discarded — equivalent to the write
//          never having happened, which is safe because unacknowledged.
//
// Conflict truncation (a follower overwriting divergent entries shipped from
// a stale leader) is an ftruncate() to the byte offset of the first removed
// entry; offsets are tracked in memory as records are appended or loaded.
class Storage {
public:
    explicit Storage(std::filesystem::path dir);
    ~Storage();

    Storage(const Storage&) = delete;
    Storage& operator=(const Storage&) = delete;

    // Loads meta + log from disk (creating fresh files on first boot).
    // Returns false only on unrecoverable I/O errors.
    [[nodiscard]] bool load();

    [[nodiscard]] uint64_t currentTerm() const { return currentTerm_; }
    [[nodiscard]] int32_t votedFor() const { return votedFor_; }
    void saveMeta(uint64_t currentTerm, int32_t votedFor);

    // The recovered log. RaftNode takes ownership of the in-memory copy and
    // calls append/truncateFrom to keep disk in sync from then on.
    [[nodiscard]] const std::vector<LogEntry>& entries() const { return entries_; }

    // Appends one entry and fsyncs. Index is 1-based (Raft convention);
    // appends must be sequential.
    void append(const LogEntry& entry);

    // Removes entries at logical index >= fromIndex (1-based) from disk.
    void truncateFrom(uint64_t fromIndex);

private:
    void writeMetaFile();

    std::filesystem::path dir_;
    std::filesystem::path metaPath_;
    std::filesystem::path walPath_;

    uint64_t currentTerm_ = 0;
    int32_t votedFor_ = -1;

    std::vector<LogEntry> entries_;  // entries_[0] is logical index 1
    std::vector<uint64_t> offsets_;  // file offset where record i begins
    uint64_t walSize_ = 0;
    int walFd_ = -1;
};

} // namespace dkvs
