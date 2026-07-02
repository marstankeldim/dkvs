#pragma once

#include "dkvs/command.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dkvs {

// The two RPCs from the Raft paper (§5), plus binary ser/de for the wire.
// Frames are tagged with a message-type byte so one socket protocol carries
// both requests and replies.

enum class MsgType : uint8_t {
    RequestVoteArgs = 1,
    RequestVoteReply = 2,
    AppendEntriesArgs = 3,
    AppendEntriesReply = 4,
};

struct RequestVoteArgs {
    uint64_t term = 0;
    uint32_t candidateId = 0;
    uint64_t lastLogIndex = 0;
    uint64_t lastLogTerm = 0;

    [[nodiscard]] std::string encode() const;
    static std::optional<RequestVoteArgs> decode(std::string_view bytes);
};

struct RequestVoteReply {
    uint64_t term = 0;
    bool voteGranted = false;

    [[nodiscard]] std::string encode() const;
    static std::optional<RequestVoteReply> decode(std::string_view bytes);
};

struct AppendEntriesArgs {
    uint64_t term = 0;
    uint32_t leaderId = 0;
    uint64_t prevLogIndex = 0;
    uint64_t prevLogTerm = 0;
    uint64_t leaderCommit = 0;
    std::vector<LogEntry> entries; // empty ⇒ heartbeat

    [[nodiscard]] std::string encode() const;
    static std::optional<AppendEntriesArgs> decode(std::string_view bytes);
};

struct AppendEntriesReply {
    uint64_t term = 0;
    bool success = false;
    // On failure: the index the leader should retry from (accelerated
    // conflict backup, Raft paper §5.3) instead of decrementing one at a time.
    uint64_t conflictIndex = 0;

    [[nodiscard]] std::string encode() const;
    static std::optional<AppendEntriesReply> decode(std::string_view bytes);
};

} // namespace dkvs
