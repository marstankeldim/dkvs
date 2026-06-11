#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>

namespace dkvs {

class KVStore {
public:
    void set(std::string key, std::string value);
    [[nodiscard]] std::optional<std::string> get(const std::string& key) const;
    [[nodiscard]] bool remove(const std::string& key);
    [[nodiscard]] bool contains(const std::string& key) const;
    [[nodiscard]] std::size_t size() const;

private:
    std::unordered_map<std::string, std::string> data_;
};

} // namespace dkvs
