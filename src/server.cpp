#include "dkvs/server.hpp"

#include "dkvs/codec.hpp"

#include <cstdio>
#include <utility>

namespace dkvs {

namespace {
constexpr int kRaftConnIdleMs = 5000;
constexpr int kClientConnIdleMs = 10 * 60 * 1000;
constexpr auto kCommitWait = std::chrono::milliseconds(3000);
} // namespace

Server::Server(NodeConfig config)
    : config_(std::move(config)),
      storage_(config_.dataDir),
      transport_(config_.raftAddrs)
{
}

Server::~Server()
{
    stop();
}

bool Server::init()
{
    if (!storage_.load()) {
        return false;
    }

    raftListenFd_ = net::tcpListen(config_.raftAddrs[config_.id].port);
    if (raftListenFd_ < 0) {
        std::fprintf(stderr, "[node %u] cannot listen on raft port %u\n",
                     config_.id, config_.raftAddrs[config_.id].port);
        return false;
    }
    clientListenFd_ = net::tcpListen(config_.clientAddrs[config_.id].port);
    if (clientListenFd_ < 0) {
        std::fprintf(stderr, "[node %u] cannot listen on client port %u\n",
                     config_.id, config_.clientAddrs[config_.id].port);
        return false;
    }

    raft_.emplace(
        config_.id, static_cast<uint32_t>(config_.raftAddrs.size()), storage_,
        transport_,
        [this](uint64_t index, uint64_t term, const std::string& cmd) {
            onApply(index, term, cmd);
        },
        config_.raft);
    return true;
}

void Server::run()
{
    raft_->start();
    raftAcceptor_ = std::thread([this] { raftAcceptLoop(); });
    clientAcceptor_ = std::thread([this] { clientAcceptLoop(); });

    std::fprintf(stderr, "[node %u] serving clients on %s, raft on %s\n",
                 config_.id, config_.clientAddrs[config_.id].str().c_str(),
                 config_.raftAddrs[config_.id].str().c_str());

    if (raftAcceptor_.joinable()) {
        raftAcceptor_.join();
    }
    if (clientAcceptor_.joinable()) {
        clientAcceptor_.join();
    }
    std::unique_lock lock(connMutex_);
    connCv_.wait(lock, [this] { return connThreads_.load() == 0; });
}

void Server::stop()
{
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true)) {
        return;
    }
    // Closing the listen fds unblocks the accept loops.
    net::closeFd(raftListenFd_);
    net::closeFd(clientListenFd_);
    if (raft_) {
        raft_->stop();
    }
    waitCv_.notify_all();
}

void Server::raftAcceptLoop()
{
    for (;;) {
        int fd = net::tcpAccept(raftListenFd_);
        if (fd < 0) {
            return; // listener closed
        }
        connThreads_.fetch_add(1);
        std::thread([this, fd] {
            handleRaftConnection(fd);
            if (connThreads_.fetch_sub(1) == 1) {
                std::lock_guard g(connMutex_);
                connCv_.notify_all();
            }
        }).detach();
    }
}

void Server::handleRaftConnection(int fd)
{
    while (!stopping_.load()) {
        auto frame = net::recvFrame(fd, kRaftConnIdleMs);
        if (!frame || frame->empty()) {
            break;
        }
        std::string reply;
        switch (static_cast<MsgType>(static_cast<uint8_t>((*frame)[0]))) {
        case MsgType::RequestVoteArgs: {
            auto args = RequestVoteArgs::decode(*frame);
            if (args) {
                reply = raft_->onRequestVote(*args).encode();
            }
            break;
        }
        case MsgType::AppendEntriesArgs: {
            auto args = AppendEntriesArgs::decode(*frame);
            if (args) {
                reply = raft_->onAppendEntries(*args).encode();
            }
            break;
        }
        default:
            break;
        }
        if (reply.empty() || !net::sendFrame(fd, reply)) {
            break;
        }
    }
    net::closeFd(fd);
}

void Server::clientAcceptLoop()
{
    for (;;) {
        int fd = net::tcpAccept(clientListenFd_);
        if (fd < 0) {
            return;
        }
        connThreads_.fetch_add(1);
        std::thread([this, fd] {
            handleClientConnection(fd);
            if (connThreads_.fetch_sub(1) == 1) {
                std::lock_guard g(connMutex_);
                connCv_.notify_all();
            }
        }).detach();
    }
}

void Server::handleClientConnection(int fd)
{
    std::string buffer;
    while (!stopping_.load()) {
        auto line = net::recvLine(fd, buffer, kClientConnIdleMs);
        if (!line) {
            break;
        }
        if (line->empty()) {
            continue;
        }
        auto req = parseClientRequest(*line);
        if (!req) {
            if (!net::sendLine(fd, "ERROR unknown command")) {
                break;
            }
            continue;
        }
        if (req->kind == ClientRequest::Kind::Quit) {
            net::sendLine(fd, "BYE");
            break;
        }
        if (!net::sendLine(fd, handleRequest(*req))) {
            break;
        }
    }
    net::closeFd(fd);
}

std::string Server::handleRequest(const ClientRequest& req)
{
    switch (req.kind) {
    case ClientRequest::Kind::Ping:
        return "PONG";
    case ClientRequest::Kind::Status: {
        RaftStatus s = raft_->status();
        std::string out = "STATUS role=";
        out += roleName(s.role);
        out += " term=" + std::to_string(s.term);
        out += " leader=" + std::to_string(s.leaderHint);
        out += " commit=" + std::to_string(s.commitIndex);
        out += " applied=" + std::to_string(s.lastApplied);
        out += " log=" + std::to_string(s.lastLogIndex);
        out += " keys=" + std::to_string(store_.size());
        return out;
    }
    case ClientRequest::Kind::Command:
        return executeCommand(req.command);
    case ClientRequest::Kind::Quit:
        break;
    }
    return "ERROR internal";
}

std::string Server::executeCommand(const Command& cmd)
{
    RaftNode::SubmitResult result;
    {
        // Register the waiter under waitMutex_ *before* releasing it so the
        // applier can't apply our index and skip us: onApply also takes
        // waitMutex_, so it can't record index N until we've parked for it.
        std::unique_lock wait(waitMutex_);
        result = raft_->submit(cmd.encode());
        if (!result.isLeader) {
            wait.unlock();
            if (result.leaderHint >= 0 &&
                result.leaderHint < static_cast<int>(config_.clientAddrs.size()) &&
                result.leaderHint != static_cast<int>(config_.id)) {
                return "REDIRECT " +
                       config_.clientAddrs[result.leaderHint].str();
            }
            return "ERROR no leader known, retry";
        }
        waiters_.emplace(result.index, PendingResult{});

        bool done = waitCv_.wait_for(wait, kCommitWait, [&] {
            if (stopping_.load()) {
                return true;
            }
            auto it = waiters_.find(result.index);
            return it != waiters_.end() && it->second.applied;
        });
        PendingResult pending;
        if (auto it = waiters_.find(result.index); it != waiters_.end()) {
            pending = it->second;
            waiters_.erase(it);
        }
        if (!done || !pending.applied) {
            return "ERROR commit timeout, retry";
        }
        if (pending.appliedTerm != result.term) {
            // A different leader's entry landed at our index — our command
            // was discarded, never committed.
            return "ERROR lost leadership, retry";
        }
        switch (cmd.op) {
        case Command::Op::Set:
            return "OK";
        case Command::Op::Get:
            return pending.value ? "VALUE " + *pending.value : "NOT_FOUND";
        case Command::Op::Del:
            return pending.existed ? "DELETED" : "NOT_FOUND";
        }
    }
    return "ERROR internal";
}

void Server::onApply(uint64_t index, uint64_t term, const std::string& rawCommand)
{
    std::optional<std::string> value;
    bool existed = false;

    if (!rawCommand.empty()) { // empty = leader no-op barrier, nothing to apply
        auto cmd = Command::decode(rawCommand);
        if (!cmd) {
            std::fprintf(stderr, "[node %u] undecodable command at index %llu\n",
                         config_.id, static_cast<unsigned long long>(index));
        } else {
            switch (cmd->op) {
            case Command::Op::Set:
                store_.set(cmd->key, cmd->value);
                break;
            case Command::Op::Get:
                value = store_.get(cmd->key);
                break;
            case Command::Op::Del:
                existed = store_.remove(cmd->key);
                break;
            }
        }
    }

    std::lock_guard g(waitMutex_);
    auto it = waiters_.find(index);
    if (it != waiters_.end()) {
        it->second.applied = true;
        it->second.appliedTerm = term;
        it->second.value = std::move(value);
        it->second.existed = existed;
        waitCv_.notify_all();
    }
}

} // namespace dkvs
