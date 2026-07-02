#include "dkvs/raft_msgs.hpp"

#include "dkvs/codec.hpp"

namespace dkvs {

std::string RequestVoteArgs::encode() const
{
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::RequestVoteArgs));
    enc.u64(term);
    enc.u32(candidateId);
    enc.u64(lastLogIndex);
    enc.u64(lastLogTerm);
    return enc.take();
}

std::optional<RequestVoteArgs> RequestVoteArgs::decode(std::string_view bytes)
{
    Decoder dec(bytes);
    auto tag = dec.u8();
    if (!tag || *tag != static_cast<uint8_t>(MsgType::RequestVoteArgs)) {
        return std::nullopt;
    }
    RequestVoteArgs args;
    auto term = dec.u64();
    auto candidate = dec.u32();
    auto lastIdx = dec.u64();
    auto lastTerm = dec.u64();
    if (!term || !candidate || !lastIdx || !lastTerm || !dec.done()) {
        return std::nullopt;
    }
    args.term = *term;
    args.candidateId = *candidate;
    args.lastLogIndex = *lastIdx;
    args.lastLogTerm = *lastTerm;
    return args;
}

std::string RequestVoteReply::encode() const
{
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::RequestVoteReply));
    enc.u64(term);
    enc.u8(voteGranted ? 1 : 0);
    return enc.take();
}

std::optional<RequestVoteReply> RequestVoteReply::decode(std::string_view bytes)
{
    Decoder dec(bytes);
    auto tag = dec.u8();
    if (!tag || *tag != static_cast<uint8_t>(MsgType::RequestVoteReply)) {
        return std::nullopt;
    }
    RequestVoteReply reply;
    auto term = dec.u64();
    auto granted = dec.u8();
    if (!term || !granted || !dec.done()) {
        return std::nullopt;
    }
    reply.term = *term;
    reply.voteGranted = (*granted != 0);
    return reply;
}

std::string AppendEntriesArgs::encode() const
{
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::AppendEntriesArgs));
    enc.u64(term);
    enc.u32(leaderId);
    enc.u64(prevLogIndex);
    enc.u64(prevLogTerm);
    enc.u64(leaderCommit);
    enc.u32(static_cast<uint32_t>(entries.size()));
    for (const auto& e : entries) {
        enc.u64(e.term);
        enc.str(e.command);
    }
    return enc.take();
}

std::optional<AppendEntriesArgs> AppendEntriesArgs::decode(std::string_view bytes)
{
    Decoder dec(bytes);
    auto tag = dec.u8();
    if (!tag || *tag != static_cast<uint8_t>(MsgType::AppendEntriesArgs)) {
        return std::nullopt;
    }
    AppendEntriesArgs args;
    auto term = dec.u64();
    auto leader = dec.u32();
    auto prevIdx = dec.u64();
    auto prevTerm = dec.u64();
    auto commit = dec.u64();
    auto count = dec.u32();
    if (!term || !leader || !prevIdx || !prevTerm || !commit || !count) {
        return std::nullopt;
    }
    args.term = *term;
    args.leaderId = *leader;
    args.prevLogIndex = *prevIdx;
    args.prevLogTerm = *prevTerm;
    args.leaderCommit = *commit;
    args.entries.reserve(*count);
    for (uint32_t i = 0; i < *count; ++i) {
        auto entryTerm = dec.u64();
        auto cmd = dec.str();
        if (!entryTerm || !cmd) {
            return std::nullopt;
        }
        args.entries.push_back(LogEntry{*entryTerm, std::move(*cmd)});
    }
    if (!dec.done()) {
        return std::nullopt;
    }
    return args;
}

std::string AppendEntriesReply::encode() const
{
    Encoder enc;
    enc.u8(static_cast<uint8_t>(MsgType::AppendEntriesReply));
    enc.u64(term);
    enc.u8(success ? 1 : 0);
    enc.u64(conflictIndex);
    return enc.take();
}

std::optional<AppendEntriesReply> AppendEntriesReply::decode(std::string_view bytes)
{
    Decoder dec(bytes);
    auto tag = dec.u8();
    if (!tag || *tag != static_cast<uint8_t>(MsgType::AppendEntriesReply)) {
        return std::nullopt;
    }
    AppendEntriesReply reply;
    auto term = dec.u64();
    auto success = dec.u8();
    auto conflict = dec.u64();
    if (!term || !success || !conflict || !dec.done()) {
        return std::nullopt;
    }
    reply.term = *term;
    reply.success = (*success != 0);
    reply.conflictIndex = *conflict;
    return reply;
}

} // namespace dkvs
