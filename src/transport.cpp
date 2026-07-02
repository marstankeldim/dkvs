#include "dkvs/transport.hpp"

namespace dkvs {

TcpTransport::TcpTransport(std::vector<net::Address> peerAddrs,
                           int connectTimeoutMs, int ioTimeoutMs)
    : peerAddrs_(std::move(peerAddrs)),
      connectTimeoutMs_(connectTimeoutMs),
      ioTimeoutMs_(ioTimeoutMs)
{
}

std::optional<std::string> TcpTransport::call(uint32_t peer, const std::string& request)
{
    if (peer >= peerAddrs_.size()) {
        return std::nullopt;
    }
    int fd = net::tcpConnect(peerAddrs_[peer], connectTimeoutMs_);
    if (fd < 0) {
        return std::nullopt;
    }
    std::optional<std::string> reply;
    if (net::sendFrame(fd, request)) {
        reply = net::recvFrame(fd, ioTimeoutMs_);
    }
    net::closeFd(fd);
    return reply;
}

std::optional<RequestVoteReply>
TcpTransport::requestVote(uint32_t peer, const RequestVoteArgs& args)
{
    auto raw = call(peer, args.encode());
    if (!raw) {
        return std::nullopt;
    }
    return RequestVoteReply::decode(*raw);
}

std::optional<AppendEntriesReply>
TcpTransport::appendEntries(uint32_t peer, const AppendEntriesArgs& args)
{
    auto raw = call(peer, args.encode());
    if (!raw) {
        return std::nullopt;
    }
    return AppendEntriesReply::decode(*raw);
}

} // namespace dkvs
