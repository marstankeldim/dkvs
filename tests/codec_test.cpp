#include "dkvs/codec.hpp"
#include "dkvs/command.hpp"
#include "dkvs/raft_msgs.hpp"

#include <gtest/gtest.h>

namespace dkvs {
namespace {

TEST(Codec, IntegerRoundTrip)
{
    Encoder enc;
    enc.u8(0xAB);
    enc.u32(0xDEADBEEF);
    enc.u64(0x0123456789ABCDEFull);

    Decoder dec(enc.bytes());
    EXPECT_EQ(dec.u8(), 0xAB);
    EXPECT_EQ(dec.u32(), 0xDEADBEEF);
    EXPECT_EQ(dec.u64(), 0x0123456789ABCDEFull);
    EXPECT_TRUE(dec.done());
}

TEST(Codec, StringRoundTripIncludingEmbeddedNulAndNewline)
{
    Encoder enc;
    std::string nasty("with\0nul and\nnewline", 20);
    enc.str(nasty);
    enc.str("");

    Decoder dec(enc.bytes());
    EXPECT_EQ(dec.str(), nasty);
    EXPECT_EQ(dec.str(), "");
    EXPECT_TRUE(dec.done());
}

TEST(Codec, TruncatedInputReturnsNulloptNotCrash)
{
    Encoder enc;
    enc.u64(42);
    std::string bytes = enc.bytes();
    for (std::size_t len = 0; len < bytes.size(); ++len) {
        Decoder dec(std::string_view(bytes).substr(0, len));
        EXPECT_EQ(dec.u64(), std::nullopt) << "at length " << len;
    }
}

TEST(Codec, StringLengthBeyondBufferIsRejected)
{
    Encoder enc;
    enc.u32(1000); // claims 1000 bytes follow
    std::string bytes = enc.bytes() + "short";
    Decoder dec(bytes);
    EXPECT_EQ(dec.str(), std::nullopt);
}

TEST(Codec, Crc32MatchesKnownVector)
{
    // The standard CRC-32 check value.
    EXPECT_EQ(crc32("123456789"), 0xCBF43926u);
    EXPECT_EQ(crc32(""), 0x00000000u);
    EXPECT_NE(crc32("abc"), crc32("abd"));
}

TEST(Command, RoundTrip)
{
    Command cmd{Command::Op::Set, "user:42", "value with spaces"};
    auto decoded = Command::decode(cmd.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->op, Command::Op::Set);
    EXPECT_EQ(decoded->key, "user:42");
    EXPECT_EQ(decoded->value, "value with spaces");
}

TEST(Command, GarbageIsRejected)
{
    EXPECT_EQ(Command::decode(""), std::nullopt);
    EXPECT_EQ(Command::decode("\xFF\x00\x01"), std::nullopt);
    Command cmd{Command::Op::Get, "k", ""};
    std::string bytes = cmd.encode();
    bytes[0] = 99; // invalid opcode
    EXPECT_EQ(Command::decode(bytes), std::nullopt);
}

TEST(RaftMsgs, RequestVoteRoundTrip)
{
    RequestVoteArgs args{7, 2, 15, 6};
    auto decoded = RequestVoteArgs::decode(args.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->term, 7u);
    EXPECT_EQ(decoded->candidateId, 2u);
    EXPECT_EQ(decoded->lastLogIndex, 15u);
    EXPECT_EQ(decoded->lastLogTerm, 6u);

    RequestVoteReply reply{7, true};
    auto decodedReply = RequestVoteReply::decode(reply.encode());
    ASSERT_TRUE(decodedReply.has_value());
    EXPECT_EQ(decodedReply->term, 7u);
    EXPECT_TRUE(decodedReply->voteGranted);
}

TEST(RaftMsgs, AppendEntriesRoundTripWithEntries)
{
    AppendEntriesArgs args;
    args.term = 3;
    args.leaderId = 1;
    args.prevLogIndex = 10;
    args.prevLogTerm = 2;
    args.leaderCommit = 9;
    args.entries.push_back(LogEntry{3, "cmd-a"});
    args.entries.push_back(LogEntry{3, std::string("bin\0ary", 7)});

    auto decoded = AppendEntriesArgs::decode(args.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->term, 3u);
    EXPECT_EQ(decoded->leaderId, 1u);
    EXPECT_EQ(decoded->prevLogIndex, 10u);
    EXPECT_EQ(decoded->prevLogTerm, 2u);
    EXPECT_EQ(decoded->leaderCommit, 9u);
    ASSERT_EQ(decoded->entries.size(), 2u);
    EXPECT_EQ(decoded->entries[0], args.entries[0]);
    EXPECT_EQ(decoded->entries[1], args.entries[1]);
}

TEST(RaftMsgs, WrongTagIsRejected)
{
    RequestVoteArgs args{1, 0, 0, 0};
    std::string bytes = args.encode();
    EXPECT_EQ(AppendEntriesArgs::decode(bytes), std::nullopt);
    EXPECT_EQ(RequestVoteReply::decode(bytes), std::nullopt);
}

} // namespace
} // namespace dkvs
