#include "dkvs/command.hpp"

#include "dkvs/codec.hpp"

namespace dkvs {

std::string Command::encode() const
{
    Encoder enc;
    enc.u8(static_cast<uint8_t>(op));
    enc.str(key);
    enc.str(value);
    return enc.take();
}

std::optional<Command> Command::decode(std::string_view bytes)
{
    Decoder dec(bytes);
    auto op = dec.u8();
    auto key = dec.str();
    auto value = dec.str();
    if (!op || !key || !value || !dec.done()) {
        return std::nullopt;
    }
    if (*op < static_cast<uint8_t>(Op::Set) || *op > static_cast<uint8_t>(Op::Del)) {
        return std::nullopt;
    }
    Command cmd;
    cmd.op = static_cast<Op>(*op);
    cmd.key = std::move(*key);
    cmd.value = std::move(*value);
    return cmd;
}

} // namespace dkvs
