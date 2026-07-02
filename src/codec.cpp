#include "dkvs/codec.hpp"

#include <array>

namespace dkvs {

void Encoder::u8(uint8_t v)
{
    buf_.push_back(static_cast<char>(v));
}

void Encoder::u32(uint32_t v)
{
    for (int shift = 24; shift >= 0; shift -= 8) {
        buf_.push_back(static_cast<char>((v >> shift) & 0xFF));
    }
}

void Encoder::u64(uint64_t v)
{
    for (int shift = 56; shift >= 0; shift -= 8) {
        buf_.push_back(static_cast<char>((v >> shift) & 0xFF));
    }
}

void Encoder::str(std::string_view s)
{
    u32(static_cast<uint32_t>(s.size()));
    buf_.append(s);
}

std::optional<uint8_t> Decoder::u8()
{
    if (remaining() < 1) {
        return std::nullopt;
    }
    return static_cast<uint8_t>(data_[pos_++]);
}

std::optional<uint32_t> Decoder::u32()
{
    if (remaining() < 4) {
        return std::nullopt;
    }
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<uint8_t>(data_[pos_++]);
    }
    return v;
}

std::optional<uint64_t> Decoder::u64()
{
    if (remaining() < 8) {
        return std::nullopt;
    }
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(data_[pos_++]);
    }
    return v;
}

std::optional<std::string> Decoder::str()
{
    auto len = u32();
    if (!len || remaining() < *len) {
        return std::nullopt;
    }
    std::string s(data_.substr(pos_, *len));
    pos_ += *len;
    return s;
}

namespace {

std::array<uint32_t, 256> makeCrcTable()
{
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}

} // namespace

uint32_t crc32(std::string_view data)
{
    static const std::array<uint32_t, 256> table = makeCrcTable();
    uint32_t crc = 0xFFFFFFFFu;
    for (char ch : data) {
        crc = table[(crc ^ static_cast<uint8_t>(ch)) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

} // namespace dkvs
