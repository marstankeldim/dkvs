#include "dkvs/storage.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace dkvs {
namespace {

class StorageTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        dir_ = std::filesystem::path(::testing::TempDir()) /
               ("dkvs-storage-" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        std::filesystem::remove_all(dir_);
    }

    void TearDown() override { std::filesystem::remove_all(dir_); }

    std::filesystem::path dir_;
};

TEST_F(StorageTest, FreshDirectoryLoadsEmpty)
{
    Storage storage(dir_);
    ASSERT_TRUE(storage.load());
    EXPECT_EQ(storage.currentTerm(), 0u);
    EXPECT_EQ(storage.votedFor(), -1);
    EXPECT_TRUE(storage.entries().empty());
}

TEST_F(StorageTest, MetaSurvivesReopen)
{
    {
        Storage storage(dir_);
        ASSERT_TRUE(storage.load());
        storage.saveMeta(42, 2);
    }
    Storage reopened(dir_);
    ASSERT_TRUE(reopened.load());
    EXPECT_EQ(reopened.currentTerm(), 42u);
    EXPECT_EQ(reopened.votedFor(), 2);
}

TEST_F(StorageTest, LogEntriesSurviveReopen)
{
    {
        Storage storage(dir_);
        ASSERT_TRUE(storage.load());
        storage.append(LogEntry{1, "first"});
        storage.append(LogEntry{1, "second"});
        storage.append(LogEntry{2, std::string("bin\0ary", 7)});
    }
    Storage reopened(dir_);
    ASSERT_TRUE(reopened.load());
    ASSERT_EQ(reopened.entries().size(), 3u);
    EXPECT_EQ(reopened.entries()[0], (LogEntry{1, "first"}));
    EXPECT_EQ(reopened.entries()[1], (LogEntry{1, "second"}));
    EXPECT_EQ(reopened.entries()[2], (LogEntry{2, std::string("bin\0ary", 7)}));
}

TEST_F(StorageTest, TruncateFromRemovesSuffixOnDiskToo)
{
    {
        Storage storage(dir_);
        ASSERT_TRUE(storage.load());
        for (int i = 1; i <= 5; ++i) {
            storage.append(LogEntry{1, "entry-" + std::to_string(i)});
        }
        storage.truncateFrom(3); // drop logical indexes 3,4,5
        ASSERT_EQ(storage.entries().size(), 2u);
        storage.append(LogEntry{2, "replacement"});
    }
    Storage reopened(dir_);
    ASSERT_TRUE(reopened.load());
    ASSERT_EQ(reopened.entries().size(), 3u);
    EXPECT_EQ(reopened.entries()[0].command, "entry-1");
    EXPECT_EQ(reopened.entries()[1].command, "entry-2");
    EXPECT_EQ(reopened.entries()[2].command, "replacement");
    EXPECT_EQ(reopened.entries()[2].term, 2u);
}

TEST_F(StorageTest, TornTailWriteIsDiscardedOnRecovery)
{
    {
        Storage storage(dir_);
        ASSERT_TRUE(storage.load());
        storage.append(LogEntry{1, "good-entry"});
    }
    // Simulate a crash mid-append: a record header claiming more bytes than
    // were actually written.
    {
        std::ofstream wal(dir_ / "wal", std::ios::binary | std::ios::app);
        const char torn[] = {0x00, 0x00, 0x01, 0x00, 'p', 'a', 'r', 't'};
        wal.write(torn, sizeof(torn));
    }
    Storage reopened(dir_);
    ASSERT_TRUE(reopened.load());
    ASSERT_EQ(reopened.entries().size(), 1u);
    EXPECT_EQ(reopened.entries()[0].command, "good-entry");

    // The torn bytes must be gone from disk so the next append is clean.
    reopened.append(LogEntry{2, "after-recovery"});
    Storage again(dir_);
    ASSERT_TRUE(again.load());
    ASSERT_EQ(again.entries().size(), 2u);
    EXPECT_EQ(again.entries()[1].command, "after-recovery");
}

TEST_F(StorageTest, CorruptedRecordIsDetectedByCrc)
{
    {
        Storage storage(dir_);
        ASSERT_TRUE(storage.load());
        storage.append(LogEntry{1, "aaaa"});
        storage.append(LogEntry{1, "bbbb"});
    }
    // Flip one payload byte in the second record.
    {
        std::fstream wal(dir_ / "wal",
                         std::ios::binary | std::ios::in | std::ios::out);
        wal.seekg(0, std::ios::end);
        auto size = static_cast<long>(wal.tellg());
        wal.seekp(size - 6); // inside the last record's payload
        wal.put('X');
    }
    Storage reopened(dir_);
    ASSERT_TRUE(reopened.load());
    ASSERT_EQ(reopened.entries().size(), 1u);
    EXPECT_EQ(reopened.entries()[0].command, "aaaa");
}

} // namespace
} // namespace dkvs
