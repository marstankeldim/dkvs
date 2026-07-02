#pragma once

#include "dkvs/net.hpp"
#include "dkvs/raft_msgs.hpp"

#include <optional>
#include <vector>

namespace dkvs {

// Abstracts "send an RPC to peer N and wait for its reply" so the consensus
// core never touches sockets. Production uses TcpTransport; tests use an
// in-process loopback transport that can drop messages to simulate
// partitions and crashes deterministically.
//
// Returning std::nullopt means the RPC failed (peer down, timeout, garbage
// reply) — indistinguishable outcomes by design, exactly as in a real
// network.
class Transport {
public:
    virtual ~Transport() = default;

    virtual std::optional<RequestVoteReply>
    requestVote(uint32_t peer, const RequestVoteArgs& args) = 0;

    virtual std::optional<AppendEntriesReply>
    appendEntries(uint32_t peer, const AppendEntriesArgs& args) = 0;
};

// One short-lived TCP connection per RPC: connect, send one frame, read one
// frame, close. Trivially robust to peer restarts; connection reuse is a
// documented future optimization.
class TcpTransport : public Transport {
public:
    explicit TcpTransport(std::vector<net::Address> peerAddrs,
                          int connectTimeoutMs = 250,
                          int ioTimeoutMs = 1000);

    std::optional<RequestVoteReply>
    requestVote(uint32_t peer, const RequestVoteArgs& args) override;

    std::optional<AppendEntriesReply>
    appendEntries(uint32_t peer, const AppendEntriesArgs& args) override;

private:
    std::optional<std::string> call(uint32_t peer, const std::string& request);

    std::vector<net::Address> peerAddrs_;
    int connectTimeoutMs_;
    int ioTimeoutMs_;
};

} // namespace dkvs
