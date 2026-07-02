#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace dkvs {

// A state-machine command as stored in the replicated log. GETs go through
// the log too: replaying every read in log order is the simplest way to make
// reads linearizable (a GET observes exactly the writes committed before it).
// Faster read paths (ReadIndex, leader leases) are documented future work.
struct Command {
    enum class Op : uint8_t {
        Set = 1,
        Get = 2,
        Del = 3,
    };

    Op op = Op::Get;
    std::string key;
    std::string value; // only used by Set

    [[nodiscard]] std::string encode() const;
    static std::optional<Command> decode(std::string_view bytes);
};

// One entry in the replicated log.
struct LogEntry {
    uint64_t term = 0;
    std::string command; // encoded Command

    bool operator==(const LogEntry&) const = default;
};

} // namespace dkvs
