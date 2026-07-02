#pragma once

#include "dkvs/raft_msgs.hpp"
#include "dkvs/storage.hpp"
#include "dkvs/transport.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace dkvs {

enum class Role : uint8_t { Follower, Candidate, Leader };

struct RaftStatus {
    Role role = Role::Follower;
    uint64_t term = 0;
    int leaderHint = -1; // -1 = unknown
    uint64_t commitIndex = 0;
    uint64_t lastApplied = 0;
    uint64_t lastLogIndex = 0;
};

struct RaftConfig {
    std::chrono::milliseconds heartbeatInterval{75};
    std::chrono::milliseconds electionTimeoutMin{300};
    std::chrono::milliseconds electionTimeoutMax{600};
    std::chrono::milliseconds tickInterval{15};
};

// The Raft consensus core (Ongaro & Ousterhout, "In Search of an
// Understandable Consensus Algorithm", 2014). Implements leader election,
// log replication, commitment, and crash recovery over an abstract
// Transport. Log compaction / snapshotting and membership changes are
// documented future work.
//
// Threading model — a single mutex guards all Raft state, and the invariant
// that makes the design deadlock-free is: THE LOCK IS NEVER HELD ACROSS
// NETWORK I/O. Threads per node:
//   ticker      — fires elections when the randomized timeout expires
//   applier     — feeds committed entries, in order, to the state machine
//   replicators — one per peer while leader; batch AppendEntries + heartbeats
//   vote senders — short-lived, one per peer per election
// RPC handler methods (onRequestVote/onAppendEntries) are called from the
// server's connection threads.
class RaftNode {
public:
    // Called from the applier thread, in log order, exactly once per index.
    using ApplyFn =
        std::function<void(uint64_t index, uint64_t term, const std::string& command)>;

    RaftNode(uint32_t id, uint32_t clusterSize, Storage& storage,
             Transport& transport, ApplyFn applyFn, RaftConfig config = {});
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void start();
    void stop();

    struct SubmitResult {
        bool isLeader = false;
        int leaderHint = -1;
        uint64_t index = 0;
        uint64_t term = 0;
    };

    // Leader-only: appends a command to the replicated log and returns its
    // (index, term). The caller learns the outcome when the applier reaches
    // that index — commitment is never guaranteed at submit time.
    SubmitResult submit(std::string command);

    RequestVoteReply onRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply onAppendEntries(const AppendEntriesArgs& args);

    RaftStatus status();

private:
    // --- helpers; all require mutex_ held ---
    [[nodiscard]] uint64_t lastLogIndex() const { return log_.size() - 1; }
    [[nodiscard]] uint64_t lastLogTerm() const { return log_.back().term; }
    [[nodiscard]] uint32_t majority() const { return clusterSize_ / 2 + 1; }
    void becomeFollower(uint64_t term);
    void becomeLeader();
    void startElection();
    void advanceCommit();
    void resetElectionDeadline();

    // --- thread bodies ---
    void tickerLoop();
    void applierLoop();
    void replicatorLoop(uint32_t peer, uint64_t leaderTerm);

    void spawnDetached(std::function<void()> fn);
    void waitDetachedThreads();

    const uint32_t id_;
    const uint32_t clusterSize_;
    Storage& storage_;
    Transport& transport_;
    ApplyFn applyFn_;
    const RaftConfig config_;

    std::mutex mutex_;
    Role role_ = Role::Follower;
    uint64_t currentTerm_ = 0;
    int32_t votedFor_ = -1;
    std::vector<LogEntry> log_; // log_[0] is a sentinel; real entries start at 1
    uint64_t commitIndex_ = 0;
    uint64_t lastApplied_ = 0;
    int leaderHint_ = -1;
    uint32_t votesReceived_ = 0;
    std::vector<uint64_t> nextIndex_;
    std::vector<uint64_t> matchIndex_;
    std::chrono::steady_clock::time_point electionDeadline_;
    std::mt19937 rng_;
    bool stopping_ = false;

    std::condition_variable applyCv_; // commitIndex advanced / stopping
    std::condition_variable replCv_;  // new entries / role change / stopping

    std::thread ticker_;
    std::thread applier_;

    // Detached vote/replicator threads are tracked so stop() can wait for
    // them without joining (a replicator that loses leadership can't join
    // itself).
    std::atomic<int> inflight_{0};
    std::mutex inflightMutex_;
    std::condition_variable inflightCv_;
};

const char* roleName(Role role);

} // namespace dkvs
