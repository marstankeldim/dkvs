#include "dkvs/storage.hpp"

#include "dkvs/codec.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <system_error>

namespace dkvs {

namespace {

constexpr std::string_view kMetaMagic = "DKVSMETA";

void fsyncOrThrow(int fd, const char* what)
{
#ifdef F_FULLFSYNC
    // On macOS fsync() does not force the drive cache; F_FULLFSYNC does.
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
        return;
    }
    // Fall through: some filesystems reject F_FULLFSYNC.
#endif
    if (::fsync(fd) != 0) {
        throw std::system_error(errno, std::generic_category(), what);
    }
}

void writeAllOrThrow(int fd, std::string_view data, const char* what)
{
    std::size_t written = 0;
    while (written < data.size()) {
        ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::system_error(errno, std::generic_category(), what);
        }
        written += static_cast<std::size_t>(n);
    }
}

} // namespace

Storage::Storage(std::filesystem::path dir)
    : dir_(std::move(dir)),
      metaPath_(dir_ / "meta"),
      walPath_(dir_ / "wal")
{
}

Storage::~Storage()
{
    if (walFd_ >= 0) {
        ::close(walFd_);
    }
}

bool Storage::load()
{
    std::error_code ec;
    std::filesystem::create_directories(dir_, ec);
    if (ec) {
        std::fprintf(stderr, "storage: cannot create %s: %s\n",
                     dir_.c_str(), ec.message().c_str());
        return false;
    }

    // --- meta ---
    if (std::filesystem::exists(metaPath_)) {
        std::ifstream in(metaPath_, std::ios::binary);
        std::string raw((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>());
        bool ok = false;
        if (raw.size() > kMetaMagic.size() + 4 &&
            raw.compare(0, kMetaMagic.size(), kMetaMagic) == 0) {
            std::string_view payload(raw.data(), raw.size() - 4);
            Decoder tail(std::string_view(raw).substr(raw.size() - 4));
            auto storedCrc = tail.u32();
            if (storedCrc && *storedCrc == crc32(payload)) {
                Decoder dec(std::string_view(raw).substr(kMetaMagic.size(),
                            raw.size() - kMetaMagic.size() - 4));
                auto term = dec.u64();
                auto voted = dec.u32();
                if (term && voted && dec.done()) {
                    currentTerm_ = *term;
                    votedFor_ = static_cast<int32_t>(*voted);
                    ok = true;
                }
            }
        }
        if (!ok) {
            std::fprintf(stderr, "storage: corrupt meta file %s\n", metaPath_.c_str());
            return false;
        }
    }

    // --- wal ---
    walFd_ = ::open(walPath_.c_str(), O_RDWR | O_CREAT, 0644);
    if (walFd_ < 0) {
        std::fprintf(stderr, "storage: cannot open %s: %s\n",
                     walPath_.c_str(), std::strerror(errno));
        return false;
    }

    std::string raw;
    {
        std::ifstream in(walPath_, std::ios::binary);
        raw.assign((std::istreambuf_iterator<char>(in)),
                   std::istreambuf_iterator<char>());
    }

    entries_.clear();
    offsets_.clear();
    std::size_t pos = 0;
    while (pos < raw.size()) {
        Decoder header(std::string_view(raw).substr(pos));
        auto len = header.u32();
        if (!len || raw.size() - pos < 4 + *len + 4) {
            break; // torn tail write — discard from here
        }
        std::string_view payload(raw.data() + pos + 4, *len);
        Decoder tail(std::string_view(raw).substr(pos + 4 + *len, 4));
        auto storedCrc = tail.u32();
        if (!storedCrc || *storedCrc != crc32(payload)) {
            break; // corrupt tail record — discard from here
        }
        Decoder dec(payload);
        auto term = dec.u64();
        auto cmd = dec.str();
        if (!term || !cmd || !dec.done()) {
            break;
        }
        offsets_.push_back(pos);
        entries_.push_back(LogEntry{*term, std::move(*cmd)});
        pos += 4 + *len + 4;
    }

    if (pos != raw.size()) {
        std::fprintf(stderr,
                     "storage: discarding %zu bytes of torn/corrupt wal tail "
                     "(%zu entries recovered)\n",
                     raw.size() - pos, entries_.size());
        if (::ftruncate(walFd_, static_cast<off_t>(pos)) != 0) {
            std::fprintf(stderr, "storage: ftruncate failed: %s\n", std::strerror(errno));
            return false;
        }
        fsyncOrThrow(walFd_, "fsync wal after tail truncate");
    }
    walSize_ = pos;
    if (::lseek(walFd_, static_cast<off_t>(walSize_), SEEK_SET) < 0) {
        return false;
    }
    return true;
}

void Storage::saveMeta(uint64_t currentTerm, int32_t votedFor)
{
    currentTerm_ = currentTerm;
    votedFor_ = votedFor;
    writeMetaFile();
}

void Storage::writeMetaFile()
{
    Encoder enc;
    std::string payload;
    {
        Encoder body;
        body.u64(currentTerm_);
        body.u32(static_cast<uint32_t>(votedFor_));
        payload = std::string(kMetaMagic) + body.take();
    }
    enc.u32(crc32(payload));

    std::filesystem::path tmp = metaPath_;
    tmp += ".tmp";

    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "open meta.tmp");
    }
    try {
        writeAllOrThrow(fd, payload, "write meta.tmp");
        writeAllOrThrow(fd, enc.bytes(), "write meta.tmp crc");
        fsyncOrThrow(fd, "fsync meta.tmp");
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);

    if (::rename(tmp.c_str(), metaPath_.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(), "rename meta");
    }
    // fsync the directory so the rename itself survives a crash
    int dirFd = ::open(dir_.c_str(), O_RDONLY);
    if (dirFd >= 0) {
        ::fsync(dirFd);
        ::close(dirFd);
    }
}

void Storage::append(const LogEntry& entry)
{
    assert(walFd_ >= 0);
    Encoder payloadEnc;
    payloadEnc.u64(entry.term);
    payloadEnc.str(entry.command);
    std::string payload = payloadEnc.take();

    Encoder rec;
    rec.u32(static_cast<uint32_t>(payload.size()));
    std::string record = rec.take() + payload;
    Encoder crcEnc;
    crcEnc.u32(crc32(payload));
    record += crcEnc.bytes();

    writeAllOrThrow(walFd_, record, "append wal");
    fsyncOrThrow(walFd_, "fsync wal");

    offsets_.push_back(walSize_);
    walSize_ += record.size();
    entries_.push_back(entry);
}

void Storage::truncateFrom(uint64_t fromIndex)
{
    assert(walFd_ >= 0);
    if (fromIndex < 1 || fromIndex > entries_.size()) {
        return;
    }
    uint64_t offset = offsets_[fromIndex - 1];
    if (::ftruncate(walFd_, static_cast<off_t>(offset)) != 0) {
        throw std::system_error(errno, std::generic_category(), "ftruncate wal");
    }
    fsyncOrThrow(walFd_, "fsync wal after truncate");
    if (::lseek(walFd_, static_cast<off_t>(offset), SEEK_SET) < 0) {
        throw std::system_error(errno, std::generic_category(), "lseek wal");
    }
    walSize_ = offset;
    offsets_.resize(fromIndex - 1);
    entries_.resize(fromIndex - 1);
}

} // namespace dkvs
