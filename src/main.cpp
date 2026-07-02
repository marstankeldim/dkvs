#include "dkvs/server.hpp"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace {

dkvs::Server* g_server = nullptr;

void onSignal(int)
{
    if (g_server != nullptr) {
        g_server->stop();
    }
}

std::vector<dkvs::net::Address> parseAddrList(const std::string& csv)
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

void usage(const char* argv0)
{
    std::fprintf(stderr,
        "Usage: %s --id <n> --raft-peers <h:p,h:p,...> --client-peers <h:p,h:p,...>\n"
        "          --data-dir <path> [--heartbeat-ms N] [--election-min-ms N]\n"
        "          [--election-max-ms N]\n"
        "\n"
        "  --id            this node's index into the peer lists (0-based)\n"
        "  --raft-peers    raft RPC address of every node, in id order\n"
        "  --client-peers  client-facing address of every node, in id order\n"
        "  --data-dir      directory for this node's wal + meta files\n"
        "\n"
        "Example 3-node cluster (run each in its own terminal):\n"
        "  %s --id 0 --raft-peers 127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102 \\\n"
        "     --client-peers 127.0.0.1:6100,127.0.0.1:6101,127.0.0.1:6102 --data-dir data/node0\n",
        argv0, argv0);
}

} // namespace

int main(int argc, char** argv)
{
    std::signal(SIGPIPE, SIG_IGN);

    dkvs::NodeConfig config;
    bool haveId = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) {
                usage(argv[0]);
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--id") {
            config.id = static_cast<uint32_t>(std::strtoul(next(), nullptr, 10));
            haveId = true;
        } else if (arg == "--raft-peers") {
            config.raftAddrs = parseAddrList(next());
        } else if (arg == "--client-peers") {
            config.clientAddrs = parseAddrList(next());
        } else if (arg == "--data-dir") {
            config.dataDir = next();
        } else if (arg == "--heartbeat-ms") {
            config.raft.heartbeatInterval =
                std::chrono::milliseconds(std::strtol(next(), nullptr, 10));
        } else if (arg == "--election-min-ms") {
            config.raft.electionTimeoutMin =
                std::chrono::milliseconds(std::strtol(next(), nullptr, 10));
        } else if (arg == "--election-max-ms") {
            config.raft.electionTimeoutMax =
                std::chrono::milliseconds(std::strtol(next(), nullptr, 10));
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    if (!haveId || config.raftAddrs.empty() || config.dataDir.empty() ||
        config.raftAddrs.size() != config.clientAddrs.size() ||
        config.id >= config.raftAddrs.size()) {
        usage(argv[0]);
        return 2;
    }

    dkvs::Server server(config);
    if (!server.init()) {
        return 1;
    }

    g_server = &server;
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    server.run();
    return 0;
}
