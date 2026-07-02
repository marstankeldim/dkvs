#pragma once

#include "dkvs/command.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace dkvs {

// The client-facing protocol is deliberately human-usable: newline-delimited
// text you can drive with `nc`. One request line, one response line.
//
//   SET <key> <value...>   -> OK
//   GET <key>              -> VALUE <value...> | NOT_FOUND
//   DEL <key>              -> DELETED | NOT_FOUND
//   PING                   -> PONG
//   STATUS                 -> STATUS role=<r> term=<t> leader=<id> ...
//   any request to a non-leader -> REDIRECT <host:port> | ERROR no leader known
//
// Keys cannot contain whitespace; values are the rest of the line and may
// contain spaces (but not newlines — the framing character).
struct ClientRequest {
    enum class Kind { Command, Ping, Status, Quit };
    Kind kind = Kind::Ping;
    Command command; // valid when kind == Command
};

std::optional<ClientRequest> parseClientRequest(std::string_view line);

} // namespace dkvs
