#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace dkvs::net {

struct Address {
    std::string host;
    uint16_t port = 0;

    static std::optional<Address> parse(std::string_view hostPort);
    [[nodiscard]] std::string str() const;
};

// Blocking connect with a hard deadline (non-blocking connect + poll).
// Returns the connected fd, or -1 on failure/timeout.
int tcpConnect(const Address& addr, int timeoutMs);

// Returns a listening fd bound to the given port (SO_REUSEADDR), or -1.
int tcpListen(uint16_t port);

// Blocks in accept(). Returns the client fd, or -1 (listener closed / error).
int tcpAccept(int listenFd);

void closeFd(int fd);

// All peer/client RPC traffic is framed: [u32 big-endian length][payload].
// Framing turns TCP's byte stream back into discrete messages.
bool sendFrame(int fd, std::string_view payload);
std::optional<std::string> recvFrame(int fd, int timeoutMs);

// Line-oriented I/O for the human-readable client protocol.
bool sendLine(int fd, std::string_view line); // appends '\n'
std::optional<std::string> recvLine(int fd, std::string& buffer, int timeoutMs);

} // namespace dkvs::net
