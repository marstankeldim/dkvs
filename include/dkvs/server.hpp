#pragma once

#include "dkvs/kv_store.hpp"
#include "dkvs/protocol.hpp"
#include "dkvs/raft.hpp"
#include "dkvs/storage.hpp"
#include "dkvs/transport.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dkvs {

struct NodeConfig {
    uint32_t id = 0;
    std::vector<net::Address> raftAddrs;   // index = node id
    std::vector<net::Address> clientAddrs; // index = node id
    std::filesystem::path dataDir;
    RaftConfig raft;
};

// One dkvs node: a Raft peer plus a client-facing TCP endpoint.
//
//   clients ── text lines ──> Server ── submit ──> RaftNode
//                                ^                    │ replicate to peers,
//                                │                    v commit
//                             waiters  <── apply ── applier thread ──> KVStore
//
// Every client command — including GET — is a log entry. A handler thread
// submits the command, then blocks until the applier reaches that index and
// hands back the result. If a different term's entry landed at that index
// (leadership changed and our entry was overwritten), the client gets an
// error and retries; an entry is never acknowledged unless it committed.
class Server {
public:
    explicit Server(NodeConfig config);
    ~Server();

    [[nodiscard]] bool init();
    void run(); // blocks until stop()
    void stop();

private:
    void raftAcceptLoop();
    void handleRaftConnection(int fd);
    void clientAcceptLoop();
    void handleClientConnection(int fd);
    std::string handleRequest(const ClientRequest& req);
    std::string executeCommand(const Command& cmd);
    void onApply(uint64_t index, uint64_t term, const std::string& rawCommand);

    NodeConfig config_;
    KVStore store_;
    Storage storage_;
    TcpTransport transport_;
    std::optional<RaftNode> raft_;

    // Client handlers parked here waiting for their log index to apply.
    struct PendingResult {
        bool applied = false;
        uint64_t appliedTerm = 0;
        std::optional<std::string> value; // GET result
        bool existed = false;             // DEL result
    };
    std::mutex waitMutex_;
    std::condition_variable waitCv_;
    std::unordered_map<uint64_t, PendingResult> waiters_;

    std::atomic<bool> stopping_{false};
    int raftListenFd_ = -1;
    int clientListenFd_ = -1;
    std::thread raftAcceptor_;
    std::thread clientAcceptor_;

    std::atomic<int> connThreads_{0};
    std::mutex connMutex_;
    std::condition_variable connCv_;
};

} // namespace dkvs
