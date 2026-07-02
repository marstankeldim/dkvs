#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace dkvs {

// Minimal binary serialization used for both the on-disk log format and the
// Raft RPC wire format. Integers are big-endian; strings are length-prefixed
// with a u32. Hand-rolled instead of pulling in protobuf/JSON so the entire
// byte layout of the system is visible and auditable.

class Encoder {
public:
    void u8(uint8_t v);
    void u32(uint32_t v);
    void u64(uint64_t v);
    void str(std::string_view s);

    [[nodiscard]] const std::string& bytes() const { return buf_; }
    [[nodiscard]] std::string take() { return std::move(buf_); }

private:
    std::string buf_;
};

// Decoder returns std::nullopt on any out-of-bounds read so malformed or
// truncated input (torn disk writes, garbage from the network) can never
// crash the process.
class Decoder {
public:
    explicit Decoder(std::string_view data) : data_(data) {}

    std::optional<uint8_t> u8();
    std::optional<uint32_t> u32();
    std::optional<uint64_t> u64();
    std::optional<std::string> str();

    [[nodiscard]] bool done() const { return pos_ == data_.size(); }
    [[nodiscard]] std::size_t remaining() const { return data_.size() - pos_; }

private:
    std::string_view data_;
    std::size_t pos_ = 0;
};

// CRC-32 (IEEE, polynomial 0xEDB88320). Every log record carries a checksum
// so a torn or corrupted tail can be detected and truncated on recovery.
uint32_t crc32(std::string_view data);

} // namespace dkvs
