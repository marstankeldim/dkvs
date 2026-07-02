#include "dkvs/net.hpp"

#include "dkvs/codec.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace dkvs::net {

namespace {

constexpr uint32_t kMaxFrameBytes = 64u * 1024 * 1024;

bool setNonBlocking(int fd, bool nonBlocking)
{
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    if (nonBlocking) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    return ::fcntl(fd, F_SETFL, flags) == 0;
}

void disableSigpipe(int fd)
{
#ifdef SO_NOSIGPIPE
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#else
    (void)fd;
#endif
}

// Reads exactly n bytes, waiting up to timeoutMs for each chunk.
bool readExact(int fd, char* out, std::size_t n, int timeoutMs)
{
    std::size_t got = 0;
    while (got < n) {
        struct pollfd pfd{fd, POLLIN, 0};
        int rc = ::poll(&pfd, 1, timeoutMs);
        if (rc <= 0) {
            return false; // timeout or error
        }
        ssize_t r = ::recv(fd, out + got, n - got, 0);
        if (r <= 0) {
            if (r < 0 && errno == EINTR) {
                continue;
            }
            return false; // peer closed or error
        }
        got += static_cast<std::size_t>(r);
    }
    return true;
}

bool writeAll(int fd, std::string_view data)
{
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

} // namespace

std::optional<Address> Address::parse(std::string_view hostPort)
{
    auto colon = hostPort.rfind(':');
    if (colon == std::string_view::npos || colon == 0 ||
        colon + 1 >= hostPort.size()) {
        return std::nullopt;
    }
    Address addr;
    addr.host = std::string(hostPort.substr(0, colon));
    int port = 0;
    for (char c : hostPort.substr(colon + 1)) {
        if (c < '0' || c > '9') {
            return std::nullopt;
        }
        port = port * 10 + (c - '0');
        if (port > 65535) {
            return std::nullopt;
        }
    }
    if (port == 0) {
        return std::nullopt;
    }
    addr.port = static_cast<uint16_t>(port);
    return addr;
}

std::string Address::str() const
{
    return host + ":" + std::to_string(port);
}

int tcpConnect(const Address& addr, int timeoutMs)
{
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    std::string portStr = std::to_string(addr.port);
    if (::getaddrinfo(addr.host.c_str(), portStr.c_str(), &hints, &res) != 0 ||
        res == nullptr) {
        return -1;
    }

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        ::freeaddrinfo(res);
        return -1;
    }
    disableSigpipe(fd);

    setNonBlocking(fd, true);
    int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
    ::freeaddrinfo(res);

    if (rc != 0) {
        if (errno != EINPROGRESS) {
            ::close(fd);
            return -1;
        }
        struct pollfd pfd{fd, POLLOUT, 0};
        if (::poll(&pfd, 1, timeoutMs) <= 0) {
            ::close(fd);
            return -1;
        }
        int soErr = 0;
        socklen_t len = sizeof(soErr);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soErr, &len) != 0 || soErr != 0) {
            ::close(fd);
            return -1;
        }
    }
    setNonBlocking(fd, false);

    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

int tcpListen(uint16_t port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(fd, 64) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

int tcpAccept(int listenFd)
{
    int fd = ::accept(listenFd, nullptr, nullptr);
    if (fd >= 0) {
        disableSigpipe(fd);
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    return fd;
}

void closeFd(int fd)
{
    if (fd >= 0) {
        ::close(fd);
    }
}

bool sendFrame(int fd, std::string_view payload)
{
    Encoder enc;
    enc.u32(static_cast<uint32_t>(payload.size()));
    return writeAll(fd, enc.bytes()) && writeAll(fd, payload);
}

std::optional<std::string> recvFrame(int fd, int timeoutMs)
{
    char header[4];
    if (!readExact(fd, header, 4, timeoutMs)) {
        return std::nullopt;
    }
    Decoder dec(std::string_view(header, 4));
    uint32_t len = *dec.u32();
    if (len > kMaxFrameBytes) {
        return std::nullopt;
    }
    std::string payload(len, '\0');
    if (len > 0 && !readExact(fd, payload.data(), len, timeoutMs)) {
        return std::nullopt;
    }
    return payload;
}

bool sendLine(int fd, std::string_view line)
{
    std::string out(line);
    out.push_back('\n');
    return writeAll(fd, out);
}

std::optional<std::string> recvLine(int fd, std::string& buffer, int timeoutMs)
{
    for (;;) {
        auto nl = buffer.find('\n');
        if (nl != std::string::npos) {
            std::string line = buffer.substr(0, nl);
            buffer.erase(0, nl + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return line;
        }
        struct pollfd pfd{fd, POLLIN, 0};
        int rc = ::poll(&pfd, 1, timeoutMs);
        if (rc <= 0) {
            return std::nullopt;
        }
        char chunk[4096];
        ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return std::nullopt;
        }
        buffer.append(chunk, static_cast<std::size_t>(n));
        if (buffer.size() > kMaxFrameBytes) {
            return std::nullopt;
        }
    }
}

} // namespace dkvs::net
