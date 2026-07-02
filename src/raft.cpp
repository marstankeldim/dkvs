#include "dkvs/raft.hpp"

#include <algorithm>
#include <cstdio>

namespace dkvs {

namespace {
// Cap on entries shipped per AppendEntries so a far-behind follower is
// caught up in bounded-size batches rather than one giant frame.
constexpr uint64_t kMaxBatch = 100;
} // namespace

const char* roleName(Role role)
{
    switch (role) {
    case Role::Follower: return "follower";
    case Role::Candidate: return "candidate";
    case Role::Leader: return "leader";
    }
    return "?";
}

RaftNode::RaftNode(uint32_t id, uint32_t clusterSize, Storage& storage,
                   Transport& transport, ApplyFn applyFn, RaftConfig config)
    : id_(id),
      clusterSize_(clusterSize),
      storage_(storage),
      transport_(transport),
      applyFn_(std::move(applyFn)),
      config_(config),
      nextIndex_(clusterSize, 1),
      matchIndex_(clusterSize, 0),
      rng_(std::random_device{}() ^ (id * 0x9E3779B9u))
{
    // Recover persistent state (Raft requires term/vote/log to survive
    // crashes; everything else is rebuilt).
    currentTerm_ = storage_.currentTerm();
    votedFor_ = storage_.votedFor();
    log_.clear();
    log_.push_back(LogEntry{0, ""}); // sentinel at index 0
    for (const auto& e : storage_.entries()) {
        log_.push_back(e);
    }
}

RaftNode::~RaftNode()
{
    stop();
}

void RaftNode::start()
{
    std::lock_guard lock(mutex_);
    resetElectionDeadline();
    ticker_ = std::thread([this] { tickerLoop(); });
    applier_ = std::thread([this] { applierLoop(); });
}

void RaftNode::stop()
{
    {
        std::lock_guard lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
        applyCv_.notify_all();
        replCv_.notify_all();
    }
    if (ticker_.joinable()) {
        ticker_.join();
    }
    if (applier_.joinable()) {
        applier_.join();
    }
    waitDetachedThreads();
}

void RaftNode::spawnDetached(std::function<void()> fn)
{
    inflight_.fetch_add(1, std::memory_order_relaxed);
    std::thread([this, fn = std::move(fn)] {
        fn();
        if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard g(inflightMutex_);
            inflightCv_.notify_all();
        }
    }).detach();
}

void RaftNode::waitDetachedThreads()
{
    std::unique_lock g(inflightMutex_);
    inflightCv_.wait(g, [this] { return inflight_.load(std::memory_order_acquire) == 0; });
}

void RaftNode::resetElectionDeadline()
{
    auto min = config_.electionTimeoutMin.count();
    auto max = config_.electionTimeoutMax.count();
    std::uniform_int_distribution<int64_t> dist(min, max);
    electionDeadline_ =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(rng_));
}

void RaftNode::becomeFollower(uint64_t term)
{
    bool termChanged = term > currentTerm_;
    if (termChanged) {
        currentTerm_ = term;
        votedFor_ = -1;
        storage_.saveMeta(currentTerm_, votedFor_);
        leaderHint_ = -1;
        resetElectionDeadline();
    }
    if (role_ != Role::Follower) {
        role_ = Role::Follower;
        replCv_.notify_all(); // retire this term's replicators
    }
}

void RaftNode::startElection()
{
    role_ = Role::Candidate;
    ++currentTerm_;
    votedFor_ = static_cast<int32_t>(id_);
    storage_.saveMeta(currentTerm_, votedFor_);
    leaderHint_ = -1;
    votesReceived_ = 1; // own vote
    resetElectionDeadline();

    if (votesReceived_ >= majority()) { // single-node cluster
        becomeLeader();
        return;
    }

    RequestVoteArgs args;
    args.term = currentTerm_;
    args.candidateId = id_;
    args.lastLogIndex = lastLogIndex();
    args.lastLogTerm = lastLogTerm();

    for (uint32_t peer = 0; peer < clusterSize_; ++peer) {
        if (peer == id_) {
            continue;
        }
        spawnDetached([this, peer, args] {
            auto reply = transport_.requestVote(peer, args);
            if (!reply) {
                return;
            }
            std::lock_guard lock(mutex_);
            if (stopping_) {
                return;
            }
            if (reply->term > currentTerm_) {
                becomeFollower(reply->term);
                return;
            }
            // Ignore stale replies from elections we have moved past.
            if (role_ != Role::Candidate || currentTerm_ != args.term ||
                !reply->voteGranted) {
                return;
            }
            if (++votesReceived_ >= majority()) {
                becomeLeader();
            }
        });
    }
}

void RaftNode::becomeLeader()
{
    role_ = Role::Leader;
    leaderHint_ = static_cast<int>(id_);
    std::fill(nextIndex_.begin(), nextIndex_.end(), lastLogIndex() + 1);
    std::fill(matchIndex_.begin(), matchIndex_.end(), uint64_t{0});

    // Commit a no-op immediately (§8): a new leader may not count replicas
    // of previous-term entries toward commitment, so without this, old
    // entries would stay uncommitted until the next client write.
    LogEntry noop{currentTerm_, ""};
    log_.push_back(noop);
    storage_.append(noop);
    matchIndex_[id_] = lastLogIndex();
    advanceCommit();

    std::fprintf(stderr, "[node %u] became leader in term %llu\n", id_,
                 static_cast<unsigned long long>(currentTerm_));

    uint64_t leaderTerm = currentTerm_;
    for (uint32_t peer = 0; peer < clusterSize_; ++peer) {
        if (peer == id_) {
            continue;
        }
        spawnDetached([this, peer, leaderTerm] { replicatorLoop(peer, leaderTerm); });
    }
}

void RaftNode::advanceCommit()
{
    if (role_ != Role::Leader) {
        return;
    }
    for (uint64_t n = lastLogIndex(); n > commitIndex_; --n) {
        if (log_[n].term != currentTerm_) {
            // §5.4.2: entries from earlier terms are never committed by
            // counting replicas; they commit implicitly once a current-term
            // entry above them commits.
            break;
        }
        uint32_t replicas = 0;
        for (uint32_t p = 0; p < clusterSize_; ++p) {
            if (matchIndex_[p] >= n) {
                ++replicas;
            }
        }
        if (replicas >= majority()) {
            commitIndex_ = n;
            applyCv_.notify_all();
            replCv_.notify_all(); // ship the new leaderCommit promptly
            break;
        }
    }
}

RaftNode::SubmitResult RaftNode::submit(std::string command)
{
    std::lock_guard lock(mutex_);
    if (stopping_ || role_ != Role::Leader) {
        return SubmitResult{false, leaderHint_, 0, 0};
    }
    LogEntry entry{currentTerm_, std::move(command)};
    log_.push_back(entry);
    storage_.append(entry); // fsync before acknowledging to anyone
    matchIndex_[id_] = lastLogIndex();
    advanceCommit(); // a single-node cluster commits right here
    replCv_.notify_all();
    return SubmitResult{true, static_cast<int>(id_), lastLogIndex(), currentTerm_};
}

RequestVoteReply RaftNode::onRequestVote(const RequestVoteArgs& args)
{
    std::lock_guard lock(mutex_);
    RequestVoteReply reply;
    reply.term = currentTerm_;
    reply.voteGranted = false;

    if (args.term < currentTerm_) {
        return reply;
    }
    if (args.term > currentTerm_) {
        becomeFollower(args.term);
    }
    reply.term = currentTerm_;

    // Election restriction (§5.4.1): only vote for candidates whose log is
    // at least as up-to-date as ours, so a leader always holds every
    // committed entry.
    bool logOk = args.lastLogTerm > lastLogTerm() ||
                 (args.lastLogTerm == lastLogTerm() &&
                  args.lastLogIndex >= lastLogIndex());
    bool canVote =
        votedFor_ == -1 || votedFor_ == static_cast<int32_t>(args.candidateId);

    if (logOk && canVote) {
        votedFor_ = static_cast<int32_t>(args.candidateId);
        storage_.saveMeta(currentTerm_, votedFor_);
        reply.voteGranted = true;
        resetElectionDeadline();
    }
    return reply;
}

AppendEntriesReply RaftNode::onAppendEntries(const AppendEntriesArgs& args)
{
    std::lock_guard lock(mutex_);
    AppendEntriesReply reply;
    reply.term = currentTerm_;
    reply.success = false;
    reply.conflictIndex = 0;

    if (args.term < currentTerm_) {
        return reply;
    }
    if (args.term > currentTerm_ || role_ != Role::Follower) {
        becomeFollower(args.term);
    }
    reply.term = currentTerm_;
    leaderHint_ = static_cast<int>(args.leaderId);
    resetElectionDeadline();

    // Log Matching check (§5.3): our log must contain the leader's
    // prevLogIndex with the same term.
    if (args.prevLogIndex > lastLogIndex()) {
        reply.conflictIndex = lastLogIndex() + 1;
        return reply;
    }
    if (log_[args.prevLogIndex].term != args.prevLogTerm) {
        // Accelerated backup: point the leader at the first entry of the
        // conflicting term instead of stepping back one index per RPC.
        uint64_t conflictTerm = log_[args.prevLogIndex].term;
        uint64_t ci = args.prevLogIndex;
        while (ci > 1 && log_[ci - 1].term == conflictTerm) {
            --ci;
        }
        reply.conflictIndex = ci;
        return reply;
    }

    // Append new entries, truncating our log at the first divergence.
    // Entries that already match are left untouched — this RPC may be a
    // stale retransmission and must not undo later appends.
    std::size_t i = 0;
    for (; i < args.entries.size(); ++i) {
        uint64_t pos = args.prevLogIndex + 1 + i;
        if (pos > lastLogIndex()) {
            break;
        }
        if (log_[pos].term != args.entries[i].term) {
            storage_.truncateFrom(pos);
            log_.resize(pos);
            break;
        }
    }
    for (; i < args.entries.size(); ++i) {
        log_.push_back(args.entries[i]);
        storage_.append(args.entries[i]);
    }

    if (args.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(args.leaderCommit, lastLogIndex());
        applyCv_.notify_all();
    }
    reply.success = true;
    return reply;
}

RaftStatus RaftNode::status()
{
    std::lock_guard lock(mutex_);
    RaftStatus s;
    s.role = role_;
    s.term = currentTerm_;
    s.leaderHint = leaderHint_;
    s.commitIndex = commitIndex_;
    s.lastApplied = lastApplied_;
    s.lastLogIndex = lastLogIndex();
    return s;
}

void RaftNode::tickerLoop()
{
    for (;;) {
        {
            std::lock_guard lock(mutex_);
            if (stopping_) {
                return;
            }
            if (role_ != Role::Leader &&
                std::chrono::steady_clock::now() >= electionDeadline_) {
                startElection();
            }
        }
        std::this_thread::sleep_for(config_.tickInterval);
    }
}

void RaftNode::applierLoop()
{
    std::unique_lock lock(mutex_);
    for (;;) {
        applyCv_.wait(lock, [this] { return stopping_ || commitIndex_ > lastApplied_; });
        if (stopping_) {
            return;
        }
        while (lastApplied_ < commitIndex_) {
            uint64_t idx = lastApplied_ + 1;
            LogEntry entry = log_[idx]; // copy; committed entries are immutable
            lock.unlock();
            applyFn_(idx, entry.term, entry.command);
            lock.lock();
            lastApplied_ = idx;
        }
    }
}

void RaftNode::replicatorLoop(uint32_t peer, uint64_t leaderTerm)
{
    std::unique_lock lock(mutex_);
    for (;;) {
        if (stopping_ || role_ != Role::Leader || currentTerm_ != leaderTerm) {
            return;
        }

        AppendEntriesArgs args;
        args.term = leaderTerm;
        args.leaderId = id_;
        uint64_t next = nextIndex_[peer];
        args.prevLogIndex = next - 1;
        args.prevLogTerm = log_[next - 1].term;
        args.leaderCommit = commitIndex_;
        for (uint64_t i = next;
             i <= lastLogIndex() && i - next < kMaxBatch; ++i) {
            args.entries.push_back(log_[i]);
        }

        lock.unlock();
        auto reply = transport_.appendEntries(peer, args);
        lock.lock();

        if (stopping_ || role_ != Role::Leader || currentTerm_ != leaderTerm) {
            return;
        }

        bool retryNow = false;
        if (reply) {
            if (reply->term > currentTerm_) {
                becomeFollower(reply->term);
                return;
            }
            if (reply->term == currentTerm_) {
                if (reply->success) {
                    uint64_t newMatch = args.prevLogIndex + args.entries.size();
                    matchIndex_[peer] = std::max(matchIndex_[peer], newMatch);
                    nextIndex_[peer] = matchIndex_[peer] + 1;
                    advanceCommit();
                } else {
                    uint64_t hint = std::max<uint64_t>(reply->conflictIndex, 1);
                    nextIndex_[peer] = std::min(hint, lastLogIndex() + 1);
                    retryNow = true; // walk back without waiting a heartbeat
                }
            }
        }
        if (retryNow) {
            continue;
        }

        if (reply) {
            // Wake early if new entries need shipping; otherwise heartbeat.
            replCv_.wait_for(lock, config_.heartbeatInterval, [&] {
                return stopping_ || role_ != Role::Leader ||
                       currentTerm_ != leaderTerm ||
                       lastLogIndex() > matchIndex_[peer];
            });
        } else {
            // Peer unreachable — plain heartbeat backoff, no busy reconnect.
            replCv_.wait_for(lock, config_.heartbeatInterval, [&] {
                return stopping_ || role_ != Role::Leader ||
                       currentTerm_ != leaderTerm;
            });
        }
    }
}

} // namespace dkvs
