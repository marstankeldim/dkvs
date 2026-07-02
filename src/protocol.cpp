#include "dkvs/protocol.hpp"

namespace dkvs {

namespace {

std::string_view trimLeft(std::string_view s)
{
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) {
        s.remove_prefix(1);
    }
    return s;
}

// Consumes one whitespace-delimited token from the front of `rest`.
std::string_view takeToken(std::string_view& rest)
{
    rest = trimLeft(rest);
    std::size_t end = 0;
    while (end < rest.size() && rest[end] != ' ' && rest[end] != '\t') {
        ++end;
    }
    std::string_view token = rest.substr(0, end);
    rest.remove_prefix(end);
    return token;
}

bool equalsIgnoreCase(std::string_view a, std::string_view b)
{
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        char ca = a[i], cb = b[i];
        if (ca >= 'a' && ca <= 'z') ca = static_cast<char>(ca - 'a' + 'A');
        if (cb >= 'a' && cb <= 'z') cb = static_cast<char>(cb - 'a' + 'A');
        if (ca != cb) {
            return false;
        }
    }
    return true;
}

} // namespace

std::optional<ClientRequest> parseClientRequest(std::string_view line)
{
    std::string_view rest = line;
    std::string_view verb = takeToken(rest);
    if (verb.empty()) {
        return std::nullopt;
    }

    ClientRequest req;
    if (equalsIgnoreCase(verb, "PING")) {
        req.kind = ClientRequest::Kind::Ping;
        return req;
    }
    if (equalsIgnoreCase(verb, "STATUS")) {
        req.kind = ClientRequest::Kind::Status;
        return req;
    }
    if (equalsIgnoreCase(verb, "QUIT")) {
        req.kind = ClientRequest::Kind::Quit;
        return req;
    }

    if (equalsIgnoreCase(verb, "SET")) {
        std::string_view key = takeToken(rest);
        if (key.empty()) {
            return std::nullopt;
        }
        // Value is everything after the single space following the key —
        // spaces inside values survive, and an empty value is legal.
        if (!rest.empty() && rest.front() == ' ') {
            rest.remove_prefix(1);
        }
        req.kind = ClientRequest::Kind::Command;
        req.command = Command{Command::Op::Set, std::string(key), std::string(rest)};
        return req;
    }
    if (equalsIgnoreCase(verb, "GET") || equalsIgnoreCase(verb, "DEL") ||
        equalsIgnoreCase(verb, "DELETE")) {
        std::string_view key = takeToken(rest);
        if (key.empty() || !trimLeft(rest).empty()) {
            return std::nullopt;
        }
        req.kind = ClientRequest::Kind::Command;
        auto op = equalsIgnoreCase(verb, "GET") ? Command::Op::Get : Command::Op::Del;
        req.command = Command{op, std::string(key), ""};
        return req;
    }
    return std::nullopt;
}

} // namespace dkvs
