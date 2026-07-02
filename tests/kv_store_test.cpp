#include "dkvs/kv_store.hpp"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

namespace dkvs {
namespace {

TEST(KVStore, GetMissingKeyReturnsNullopt)
{
    KVStore store;
    EXPECT_EQ(store.get("missing"), std::nullopt);
}

TEST(KVStore, SetThenGet)
{
    KVStore store;
    store.set("name", "ayan");
    EXPECT_EQ(store.get("name"), "ayan");
}

TEST(KVStore, SetOverwritesExistingValue)
{
    KVStore store;
    store.set("name", "first");
    store.set("name", "second");
    EXPECT_EQ(store.get("name"), "second");
    EXPECT_EQ(store.size(), 1u);
}

TEST(KVStore, RemoveMissingReturnsFalse)
{
    KVStore store;
    EXPECT_FALSE(store.remove("missing"));
}

TEST(KVStore, RemoveExistingReturnsTrueAndDeletes)
{
    KVStore store;
    store.set("name", "ayan");
    EXPECT_TRUE(store.remove("name"));
    EXPECT_EQ(store.get("name"), std::nullopt);
    EXPECT_EQ(store.size(), 0u);
}

TEST(KVStore, SizeTracksInsertions)
{
    KVStore store;
    EXPECT_EQ(store.size(), 0u);
    store.set("a", "1");
    store.set("b", "2");
    EXPECT_EQ(store.size(), 2u);
    EXPECT_TRUE(store.remove("a"));
    EXPECT_EQ(store.size(), 1u);
}

TEST(KVStore, EmptyKeysAndValuesAreAllowed)
{
    KVStore store;
    store.set("", "empty key");
    store.set("empty value", "");
    EXPECT_EQ(store.get(""), "empty key");
    EXPECT_EQ(store.get("empty value"), "");
}

TEST(KVStore, ConcurrentReadersAndWritersDoNotCorrupt)
{
    KVStore store;
    constexpr int kThreads = 8;
    constexpr int kOpsPerThread = 2000;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, t] {
            for (int i = 0; i < kOpsPerThread; ++i) {
                std::string key = "k" + std::to_string(i % 50);
                if (t % 2 == 0) {
                    store.set(key, std::to_string(t * 100000 + i));
                } else {
                    auto v = store.get(key); // must never crash or tear
                    if (v) {
                        EXPECT_FALSE(v->empty());
                    }
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_LE(store.size(), 50u);
}

} // namespace
} // namespace dkvs
