#include "dkvs/kv_store.hpp"

#include <mutex>

namespace dkvs {

void KVStore::set(std::string key, std::string value)
{
    std::unique_lock lock(mutex_);
    data_[std::move(key)] = std::move(value);
}

std::optional<std::string> KVStore::get(const std::string& key) const
{
    std::shared_lock lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool KVStore::remove(const std::string& key)
{
    std::unique_lock lock(mutex_);
    return data_.erase(key) > 0;
}

bool KVStore::contains(const std::string& key) const
{
    std::shared_lock lock(mutex_);
    return data_.find(key) != data_.end();
}

std::size_t KVStore::size() const
{
    std::shared_lock lock(mutex_);
    return data_.size();
}

} // namespace dkvs
