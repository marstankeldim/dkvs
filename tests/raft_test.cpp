// Consensus tests run a whole Raft cluster inside one process, wired through
// a loopback transport whose links can be cut and healed on demand. This
// exercises elections, replication, partitions, failover, and crash-restart
// deterministically — no real sockets, no sleeping for "long enough" and
// hoping.

#include "dkvs/command.hpp"
#include "dkvs/raft.hpp"
#include "dkvs/storage.hpp"
#include "dkvs/transport.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

namespace dkvs {
namespace {

using namespace std::chrono_literals;

class LoopbackNet {
public:
    explicit LoopbackNet(uint32_t size)
        : slots_(size), connected_(size, std::vector<bool>(size, true))
    {
        for (auto& slot : slots_) {
            slot = std::make_unique<Slot>();
        }
    }

    void registerNode(uint32_t id, RaftNode* node)
    {
        std::unique_lock lock(slots_[id]->m);
        slots_[id]->node = node;
    }

    // Blocks until in-flight RPCs into this node drain, then unlinks it so
    // the RaftNode can be safely destroyed.
    void deregisterNode(uint32_t id)
    {
        std::unique_lock lock(slots_[id]->m);
        slots_[id]->node = nullptr;
    }

    void setLink(uint32_t a, uint32_t b, bool up)
    {
        std::lock_guard lock(connMutex_);
        connected_[a][b] = up;
        connected_[b][a] = up;
    }

    void isolate(uint32_t id)
    {
        std::lock_guard lock(connMutex_);
        for (std::size_t other = 0; other < connected_.size(); ++other) {
            connected_[id][other] = false;
            connected_[other][id] = false;
        }
        connected_[id][id] = true;
    }

    void healAll()
    {
        std::lock_guard lock(connMutex_);
        for (auto& row : connected_) {
            std::fill(row.begin(), row.end(), true);
        }
    }

    std::optional<RequestVoteReply>
    requestVote(uint32_t from, uint32_t to, const RequestVoteArgs& args)
    {
        if (!linked(from, to)) {
            return std::nullopt;
        }
        std::shared_lock lock(slots_[to]->m);
        if (slots_[to]->node == nullptr) {
            return std::nullopt;
        }
        return slots_[to]->node->onRequestVote(args);
    }

    std::optional<AppendEntriesReply>
    appendEntries(uint32_t from, uint32_t to, const AppendEntriesArgs& args)
    {
        if (!linked(from, to)) {
            return std::nullopt;
        }
        std::shared_lock lock(slots_[to]->m);
        if (slots_[to]->node == nullptr) {
            return std::nullopt;
        }
        return slots_[to]->node->onAppendEntries(args);
    }

private:
    bool linked(uint32_t from, uint32_t to)
    {
        std::lock_guard lock(connMutex_);
        return connected_[from][to];
    }

    struct Slot {
        std::shared_mutex m;
        RaftNode* node = nullptr;
    };
    std::vector<std::unique_ptr<Slot>> slots_;
    std::mutex connMutex_;
    std::vector<std::vector<bool>> connected_;
};

class LoopbackTransport : public Transport {
public:
    LoopbackTransport(LoopbackNet& net, uint32_t self) : net_(net), self_(self) {}

    std::optional<RequestVoteReply>
    requestVote(uint32_t peer, const RequestVoteArgs& args) override
    {
        return net_.requestVote(self_, peer, args);
    }

    std::optional<AppendEntriesReply>
    appendEntries(uint32_t peer, const AppendEntriesArgs& args) override
    {
        return net_.appendEntries(self_, peer, args);
    }

private:
    LoopbackNet& net_;
    uint32_t self_;
};

RaftConfig fastConfig()
{
    RaftConfig cfg;
    cfg.heartbeatInterval = 20ms;
    cfg.electionTimeoutMin = 100ms;
    cfg.electionTimeoutMax = 200ms;
    cfg.tickInterval = 5ms;
    return cfg;
}

class RaftClusterTest : public ::testing::Test {
protected:
    struct Node {
        std::filesystem::path dir;
        std::unique_ptr<Storage> storage;
        std::unique_ptr<LoopbackTransport> transport;
        std::unique_ptr<RaftNode> raft;
        std::mutex mu;
        std::vector<std::string> applied; // non-noop commands, in apply order
    };

    void startCluster(uint32_t size)
    {
        size_ = size;
        net_ = std::make_unique<LoopbackNet>(size);
        nodes_.clear();
        for (uint32_t i = 0; i < size; ++i) {
            nodes_.push_back(std::make_unique<Node>());
            nodes_[i]->dir =
                std::filesystem::path(::testing::TempDir()) /
                ("dkvs-raft-" +
                 std::string(::testing::UnitTest::GetInstance()
                                 ->current_test_info()
                                 ->name()) +
                 "-node" + std::to_string(i));
            std::filesystem::remove_all(nodes_[i]->dir);
        }
        for (uint32_t i = 0; i < size; ++i) {
            startNode(i);
        }
    }

    void startNode(uint32_t id)
    {
        Node& n = *nodes_[id];
        n.storage = std::make_unique<Storage>(n.dir);
        ASSERT_TRUE(n.storage->load());
        n.transport = std::make_unique<LoopbackTransport>(*net_, id);
        n.raft = std::make_unique<RaftNode>(
            id, size_, *n.storage, *n.transport,
            [&n](uint64_t, uint64_t, const std::string& cmd) {
                if (!cmd.empty()) { // skip leader no-op barriers
                    std::lock_guard lock(n.mu);
                    n.applied.push_back(cmd);
                }
            },
            fastConfig());
        net_->registerNode(id, n.raft.get());
        n.raft->start();
    }

    void stopNode(uint32_t id)
    {
        Node& n = *nodes_[id];
        net_->deregisterNode(id); // drain in-flight RPCs first
        if (n.raft) {
            n.raft->stop();
            n.raft.reset();
        }
        n.storage.reset();
    }

    void TearDown() override
    {
        for (uint32_t i = 0; i < nodes_.size(); ++i) {
            stopNode(i);
        }
        for (auto& n : nodes_) {
            std::filesystem::remove_all(n->dir);
        }
    }

    // Waits until exactly one of `among` reports leadership, and returns it.
    int findLeaderAmong(const std::vector<uint32_t>& among,
                        std::chrono::milliseconds timeout = 5s)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::vector<int> leaders;
            for (uint32_t id : among) {
                if (nodes_[id]->raft &&
                    nodes_[id]->raft->status().role == Role::Leader) {
                    leaders.push_back(static_cast<int>(id));
                }
            }
            if (leaders.size() == 1) {
                return leaders[0];
            }
            std::this_thread::sleep_for(10ms);
        }
        return -1;
    }

    int findLeader(std::chrono::milliseconds timeout = 5s)
    {
        std::vector<uint32_t> all(size_);
        for (uint32_t i = 0; i < size_; ++i) {
            all[i] = i;
        }
        return findLeaderAmong(all, timeout);
    }

    bool submitToNode(uint32_t id, const std::string& key, const std::string& value)
    {
        Command cmd{Command::Op::Set, key, value};
        return nodes_[id]->raft->submit(cmd.encode()).isLeader;
    }

    bool waitAppliedCount(uint32_t id, std::size_t count,
                          std::chrono::milliseconds timeout = 5s)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::lock_guard lock(nodes_[id]->mu);
                if (nodes_[id]->applied.size() >= count) {
                    return true;
                }
            }
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }

    std::vector<std::string> appliedOn(uint32_t id)
    {
        std::lock_guard lock(nodes_[id]->mu);
        return nodes_[id]->applied;
    }

    uint32_t size_ = 0;
    std::unique_ptr<LoopbackNet> net_;
    std::vector<std::unique_ptr<Node>> nodes_;
};

TEST_F(RaftClusterTest, SingleNodeElectsItselfAndCommits)
{
    startCluster(1);
    int leader = findLeader();
    ASSERT_EQ(leader, 0);
    ASSERT_TRUE(submitToNode(0, "k", "v"));
    EXPECT_TRUE(waitAppliedCount(0, 1));
}

TEST_F(RaftClusterTest, ThreeNodesElectExactlyOneLeader)
{
    startCluster(3);
    int leader = findLeader();
    ASSERT_NE(leader, -1);

    // Leadership must be stable: no other node usurps within a few
    // election-timeout periods.
    std::this_thread::sleep_for(500ms);
    int leaderAgain = findLeader();
    EXPECT_EQ(leader, leaderAgain);
    uint64_t term = nodes_[leader]->raft->status().term;
    for (uint32_t i = 0; i < 3; ++i) {
        if (static_cast<int>(i) != leader) {
            EXPECT_EQ(nodes_[i]->raft->status().role, Role::Follower);
            EXPECT_EQ(nodes_[i]->raft->status().term, term);
        }
    }
}

TEST_F(RaftClusterTest, CommandsReplicateToAllNodes)
{
    startCluster(3);
    int leader = findLeader();
    ASSERT_NE(leader, -1);

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(submitToNode(static_cast<uint32_t>(leader),
                                 "key" + std::to_string(i), "value"));
    }
    for (uint32_t id = 0; id < 3; ++id) {
        EXPECT_TRUE(waitAppliedCount(id, 5)) << "node " << id << " missing entries";
    }
    EXPECT_EQ(appliedOn(0), appliedOn(1));
    EXPECT_EQ(appliedOn(1), appliedOn(2));
}

TEST_F(RaftClusterTest, SubmitToFollowerIsRejectedWithLeaderHint)
{
    startCluster(3);
    int leader = findLeader();
    ASSERT_NE(leader, -1);
    uint32_t follower = leader == 0 ? 1 : 0;

    Command cmd{Command::Op::Set, "k", "v"};
    auto result = nodes_[follower]->raft->submit(cmd.encode());
    EXPECT_FALSE(result.isLeader);
    EXPECT_EQ(result.leaderHint, leader);
}

TEST_F(RaftClusterTest, CommitProceedsWithOneFollowerDown)
{
    startCluster(3);
    int leader = findLeader();
    ASSERT_NE(leader, -1);
    uint32_t downFollower = leader == 0 ? 1 : 0;
    net_->isolate(downFollower);

    ASSERT_TRUE(submitToNode(static_cast<uint32_t>(leader), "k1", "v1"));
    // Majority (2/3) still commits.
    EXPECT_TRUE(waitAppliedCount(static_cast<uint32_t>(leader), 1));

    // The isolated node has nothing...
    EXPECT_TRUE(appliedOn(downFollower).empty());

    // ...until the partition heals, then it catches up.
    net_->healAll();
    EXPECT_TRUE(waitAppliedCount(downFollower, 1));
    EXPECT_EQ(appliedOn(downFollower), appliedOn(static_cast<uint32_t>(leader)));
}

TEST_F(RaftClusterTest, LeaderPartitionTriggersFailover)
{
    startCluster(3);
    int oldLeader = findLeader();
    ASSERT_NE(oldLeader, -1);
    ASSERT_TRUE(submitToNode(static_cast<uint32_t>(oldLeader), "before", "x"));
    for (uint32_t id = 0; id < 3; ++id) {
        ASSERT_TRUE(waitAppliedCount(id, 1));
    }

    net_->isolate(static_cast<uint32_t>(oldLeader));

    std::vector<uint32_t> rest;
    for (uint32_t i = 0; i < 3; ++i) {
        if (static_cast<int>(i) != oldLeader) {
            rest.push_back(i);
        }
    }
    int newLeader = findLeaderAmong(rest);
    ASSERT_NE(newLeader, -1);
    ASSERT_NE(newLeader, oldLeader);

    ASSERT_TRUE(submitToNode(static_cast<uint32_t>(newLeader), "after", "y"));
    for (uint32_t id : rest) {
        EXPECT_TRUE(waitAppliedCount(id, 2));
    }

    // Heal: the deposed leader must step down and converge.
    net_->healAll();
    EXPECT_TRUE(waitAppliedCount(static_cast<uint32_t>(oldLeader), 2));
    auto deadline = std::chrono::steady_clock::now() + 3s;
    while (nodes_[oldLeader]->raft->status().role == Role::Leader &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_NE(nodes_[oldLeader]->raft->status().role, Role::Leader);
    EXPECT_EQ(appliedOn(static_cast<uint32_t>(oldLeader)),
              appliedOn(static_cast<uint32_t>(newLeader)));
}

TEST_F(RaftClusterTest, UncommittedEntriesOnPartitionedLeaderAreDiscarded)
{
    startCluster(3);
    int oldLeader = findLeader();
    ASSERT_NE(oldLeader, -1);

    net_->isolate(static_cast<uint32_t>(oldLeader));

    // The deposed leader doesn't know it's cut off; it accepts writes that
    // can never reach a majority. Linearizability demands these are
    // eventually discarded, not resurrected.
    ASSERT_TRUE(submitToNode(static_cast<uint32_t>(oldLeader), "orphan", "doomed"));

    std::vector<uint32_t> rest;
    for (uint32_t i = 0; i < 3; ++i) {
        if (static_cast<int>(i) != oldLeader) {
            rest.push_back(i);
        }
    }
    int newLeader = findLeaderAmong(rest);
    ASSERT_NE(newLeader, -1);
    ASSERT_TRUE(submitToNode(static_cast<uint32_t>(newLeader), "surviving", "yes"));
    for (uint32_t id : rest) {
        ASSERT_TRUE(waitAppliedCount(id, 1));
    }

    net_->healAll();
    EXPECT_TRUE(waitAppliedCount(static_cast<uint32_t>(oldLeader), 1));

    // Give replication a moment to settle, then check convergence: every
    // node applied exactly the surviving command and nobody applied the
    // orphan.
    std::this_thread::sleep_for(300ms);
    for (uint32_t id = 0; id < 3; ++id) {
        auto applied = appliedOn(id);
        Command surviving{Command::Op::Set, "surviving", "yes"};
        EXPECT_EQ(applied, std::vector<std::string>{surviving.encode()})
            << "node " << id;
    }
}

TEST_F(RaftClusterTest, RestartedNodeRecoversLogAndCatchesUp)
{
    startCluster(3);
    int leader = findLeader();
    ASSERT_NE(leader, -1);
    uint32_t victim = leader == 0 ? 1 : 0;

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(submitToNode(static_cast<uint32_t>(leader),
                                 "pre" + std::to_string(i), "v"));
    }
    ASSERT_TRUE(waitAppliedCount(victim, 3));
    uint64_t termBefore = nodes_[victim]->raft->status().term;

    stopNode(victim);
    {
        std::lock_guard lock(nodes_[victim]->mu);
        nodes_[victim]->applied.clear(); // fresh process = empty state machine
    }

    // Cluster keeps working while the node is down.
    int leaderNow = findLeaderAmong(
        [&] {
            std::vector<uint32_t> alive;
            for (uint32_t i = 0; i < 3; ++i) {
                if (i != victim) {
                    alive.push_back(i);
                }
            }
            return alive;
        }());
    ASSERT_NE(leaderNow, -1);
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(submitToNode(static_cast<uint32_t>(leaderNow),
                                 "post" + std::to_string(i), "v"));
    }

    // Restart from the same data dir: term/log recover from disk, then the
    // leader replays what was missed. A restarted state machine re-applies
    // from index 1 — all 5 commands.
    startNode(victim);
    EXPECT_GE(nodes_[victim]->raft->status().term, termBefore);
    EXPECT_TRUE(waitAppliedCount(victim, 5));
    EXPECT_EQ(appliedOn(victim), appliedOn(static_cast<uint32_t>(leaderNow)));
}

} // namespace
} // namespace dkvs
