// dkvs-cli — interactive client for a dkvs cluster.
//
//   dkvs-cli 127.0.0.1:6100,127.0.0.1:6101,127.0.0.1:6102
//   dkvs-cli 127.0.0.1:6100 SET greeting hello world   (one-shot mode)
//
// Any node can be contacted; non-leaders answer REDIRECT <addr> and the CLI
// follows it. If a node is down it tries the next one in the list.

#include "dkvs/net.hpp"

#include <csignal>
#include <cstdio>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace {

constexpr int kIoTimeoutMs = 5000;
constexpr int kConnectTimeoutMs = 1000;
constexpr int kMaxRedirects = 8;

struct Connection {
    int fd = -1;
    std::string buffer;
    dkvs::net::Address addr;
};

std::optional<Connection> connectAny(const std::vector<dkvs::net::Address>& servers,
                                     std::size_t preferred)
{
    for (std::size_t i = 0; i < servers.size(); ++i) {
        const auto& addr = servers[(preferred + i) % servers.size()];
        int fd = dkvs::net::tcpConnect(addr, kConnectTimeoutMs);
        if (fd >= 0) {
            return Connection{fd, "", addr};
        }
    }
    return std::nullopt;
}

// Sends one request line, following REDIRECTs and failing over between
// servers. Returns the final response, or nullopt if the cluster is
// unreachable.
std::optional<std::string> request(std::vector<dkvs::net::Address>& servers,
                                   std::optional<Connection>& conn,
                                   const std::string& line)
{
    for (int attempt = 0; attempt < kMaxRedirects; ++attempt) {
        if (!conn) {
            conn = connectAny(servers, 0);
            if (!conn) {
                return std::nullopt;
            }
        }
        if (!dkvs::net::sendLine(conn->fd, line)) {
            dkvs::net::closeFd(conn->fd);
            conn.reset();
            continue;
        }
        auto reply = dkvs::net::recvLine(conn->fd, conn->buffer, kIoTimeoutMs);
        if (!reply) {
            dkvs::net::closeFd(conn->fd);
            conn.reset();
            continue;
        }
        if (reply->rfind("REDIRECT ", 0) == 0) {
            auto target = dkvs::net::Address::parse(reply->substr(9));
            dkvs::net::closeFd(conn->fd);
            conn.reset();
            if (target) {
                int fd = dkvs::net::tcpConnect(*target, kConnectTimeoutMs);
                if (fd >= 0) {
                    conn = Connection{fd, "", *target};
                }
            }
            continue;
        }
        return reply;
    }
    return std::nullopt;
}

std::vector<dkvs::net::Address> parseServers(const std::string& csv)
{
    std::vector<dkvs::net::Address> out;
    std::size_t start = 0;
    while (start <= csv.size()) {
        auto comma = csv.find(',', start);
        std::string item = csv.substr(
            start, comma == std::string::npos ? std::string::npos : comma - start);
        if (!item.empty()) {
            auto addr = dkvs::net::Address::parse(item);
            if (!addr) {
                return {};
            }
            out.push_back(*addr);
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return out;
}

} // namespace

int main(int argc, char** argv)
{
    std::signal(SIGPIPE, SIG_IGN);

    if (argc < 2) {
        std::fprintf(stderr,
                     "Usage: %s <host:port[,host:port,...]> [command...]\n",
                     argv[0]);
        return 2;
    }
    auto servers = parseServers(argv[1]);
    if (servers.empty()) {
        std::fprintf(stderr, "invalid server list: %s\n", argv[1]);
        return 2;
    }

    std::optional<Connection> conn;

    if (argc > 2) { // one-shot mode
        std::string line;
        for (int i = 2; i < argc; ++i) {
            if (i > 2) {
                line += ' ';
            }
            line += argv[i];
        }
        auto reply = request(servers, conn, line);
        if (!reply) {
            std::fprintf(stderr, "error: cluster unreachable\n");
            return 1;
        }
        std::printf("%s\n", reply->c_str());
        return reply->rfind("ERROR", 0) == 0 ? 1 : 0;
    }

    std::fprintf(stderr, "dkvs-cli — commands: SET k v | GET k | DEL k | STATUS | PING | QUIT\n");
    std::string line;
    for (;;) {
        std::fputs("dkvs> ", stderr);
        std::fflush(stderr);
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line.empty()) {
            continue;
        }
        if (line == "QUIT" || line == "quit" || line == "exit") {
            break;
        }
        auto reply = request(servers, conn, line);
        if (!reply) {
            std::fprintf(stderr, "error: cluster unreachable\n");
            continue;
        }
        std::printf("%s\n", reply->c_str());
    }
    if (conn) {
        dkvs::net::closeFd(conn->fd);
    }
    return 0;
}
