#include "dkvs/kv_store.hpp"

#include <stdexcept>

namespace dkvs {

void KVStore::set(std::string key, std::string value)
{
    // TODO: Add an unordered_map member to KVStore and store the key/value pair.
    // Question: should assigning the same key replace the old value?
    (void)key;
    (void)value;
    throw std::logic_error("KVStore::set is not implemented yet");
}

std::optional<std::string> KVStore::get(const std::string& key) const
{
    // TODO: Return std::nullopt when the key is not present.
    (void)key;
    throw std::logic_error("KVStore::get is not implemented yet");
}

bool KVStore::remove(const std::string& key)
{
    // TODO: Delete the key if present. Return true if something was removed.
    (void)key;
    throw std::logic_error("KVStore::remove is not implemented yet");
}

bool KVStore::contains(const std::string& key) const
{
    // TODO: Return whether the key exists.
    (void)key;
    throw std::logic_error("KVStore::contains is not implemented yet");
}

std::size_t KVStore::size() const
{
    // TODO: Return the number of key/value pairs currently stored.
    throw std::logic_error("KVStore::size is not implemented yet");
}

} // namespace dkvs

