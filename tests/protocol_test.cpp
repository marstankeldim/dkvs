#include "dkvs/protocol.hpp"

#include <gtest/gtest.h>

namespace dkvs {
namespace {

TEST(Protocol, ParsesSet)
{
    auto req = parseClientRequest("SET name ayan");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->kind, ClientRequest::Kind::Command);
    EXPECT_EQ(req->command.op, Command::Op::Set);
    EXPECT_EQ(req->command.key, "name");
    EXPECT_EQ(req->command.value, "ayan");
}

TEST(Protocol, SetValueKeepsSpaces)
{
    auto req = parseClientRequest("SET greeting hello world  spaced");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->command.value, "hello world  spaced");
}

TEST(Protocol, SetWithEmptyValueIsLegal)
{
    auto req = parseClientRequest("SET key ");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->command.value, "");

    auto noTrailing = parseClientRequest("SET key");
    ASSERT_TRUE(noTrailing.has_value());
    EXPECT_EQ(noTrailing->command.value, "");
}

TEST(Protocol, ParsesGetAndDel)
{
    auto get = parseClientRequest("GET name");
    ASSERT_TRUE(get.has_value());
    EXPECT_EQ(get->command.op, Command::Op::Get);
    EXPECT_EQ(get->command.key, "name");

    auto del = parseClientRequest("DEL name");
    ASSERT_TRUE(del.has_value());
    EXPECT_EQ(del->command.op, Command::Op::Del);

    auto del2 = parseClientRequest("DELETE name");
    ASSERT_TRUE(del2.has_value());
    EXPECT_EQ(del2->command.op, Command::Op::Del);
}

TEST(Protocol, VerbsAreCaseInsensitive)
{
    EXPECT_TRUE(parseClientRequest("set k v").has_value());
    EXPECT_TRUE(parseClientRequest("gEt k").has_value());
    EXPECT_TRUE(parseClientRequest("ping").has_value());
}

TEST(Protocol, GetWithTrailingGarbageIsRejected)
{
    EXPECT_FALSE(parseClientRequest("GET key extra").has_value());
    EXPECT_FALSE(parseClientRequest("DEL key extra").has_value());
}

TEST(Protocol, MissingKeyIsRejected)
{
    EXPECT_FALSE(parseClientRequest("GET").has_value());
    EXPECT_FALSE(parseClientRequest("SET").has_value());
    EXPECT_FALSE(parseClientRequest("DEL  ").has_value());
}

TEST(Protocol, UnknownVerbIsRejected)
{
    EXPECT_FALSE(parseClientRequest("FLY key").has_value());
    EXPECT_FALSE(parseClientRequest("").has_value());
    EXPECT_FALSE(parseClientRequest("   ").has_value());
}

TEST(Protocol, ControlRequests)
{
    EXPECT_EQ(parseClientRequest("PING")->kind, ClientRequest::Kind::Ping);
    EXPECT_EQ(parseClientRequest("STATUS")->kind, ClientRequest::Kind::Status);
    EXPECT_EQ(parseClientRequest("QUIT")->kind, ClientRequest::Kind::Quit);
}

} // namespace
} // namespace dkvs
