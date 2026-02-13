#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

enum class BlobMode : uint8_t { Create = 0, Attach = 1 };
enum class BlobState : uint8_t { Free = 0, Writing = 1, Ready = 2, Reading = 3 };

namespace shared_blob_detail {
    inline std::string normalize_shm_name(std::string name) {
        if (name.empty()) {
            throw std::runtime_error("blob pool name cannot be empty");
        }
        if (name.front() != '/') {
            name.insert(name.begin(), '/');
        }
        return name;
    }
}

template <size_t BlobBytes>
struct SnapshotBlobSlot {
    uint32_t bytes{0};
    std::array<std::byte, BlobBytes> payload{};
};

template <size_t SlotCount, size_t BlobBytes>
class SnapshotBlobPool {
    static_assert((SlotCount & (SlotCount - 1)) == 0, "sz must be pow2");

    struct Header {
        uint64_t magic{0x534E4150424C4F42ULL};
        uint32_t version{1};
        uint32_t slots{SlotCount};
        uint32_t blob_bytes{BlobBytes};
        std::atomic<uint8_t> ready{0};
        std::array<std::atomic<uint8_t>, SlotCount> state{};
    };

    using Slot = SnapshotBlobSlot<BlobBytes>;
    using Storage = std::array<Slot, SlotCount>;

    int fd_{-1};
    void* map_{nullptr};
    Header* hdr_{nullptr};
    Storage* slots_{nullptr};
    std::string name_;
    bool owner_{false};

    static constexpr size_t align_up(size_t v, size_t a) { return (v + a - 1) & ~(a - 1); }

    static constexpr size_t bytes_needed() {
        return align_up(sizeof(Header), alignof(Storage)) + sizeof(Storage);
    }

public:
    SnapshotBlobPool(const std::string& name, BlobMode mode)
        : name_(shared_blob_detail::normalize_shm_name(name)) {
        int oflag = (mode == BlobMode::Create) ? (O_CREAT | O_EXCL | O_RDWR) : O_RDWR;
        fd_ = ::shm_open(name_.c_str(), oflag, 0600);
        if (fd_ < 0) {
            throw std::runtime_error("shm_open failed");
        }
        owner_ = (mode == BlobMode::Create);

        size_t nbytes = bytes_needed();
        if (owner_ && ::ftruncate(fd_, static_cast<off_t>(nbytes)) != 0) {
            throw std::runtime_error("ftruncate failed");
        }

        map_ = ::mmap(nullptr, nbytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (map_ == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }
        hdr_ = reinterpret_cast<Header*>(map_);
        auto off = align_up(sizeof(Header), alignof(Storage));
        slots_ = reinterpret_cast<Storage*>(static_cast<std::byte*>(map_) + off);

        if (owner_) {
            std::memset(hdr_, 0, sizeof(Header));
            for (size_t i = 0; i < SlotCount; ++i) {
                hdr_->state[i].store(static_cast<uint8_t>(BlobState::Free));
            }
            hdr_->ready.store(1, std::memory_order_release);
        }

    }

    ~SnapshotBlobPool() {
        if (map_ && map_ != MAP_FAILED) {
            ::munmap(map_, bytes_needed());
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    bool try_acquire(size_t& idx_out) {
        for (size_t i = 0; i < SlotCount; ++i) {
            uint8_t exp = static_cast<uint8_t>(BlobState::Free);
            if (hdr_->state[i].compare_exchange_strong(exp,
                                                       static_cast<uint8_t>(BlobState::Writing),
                                                       std::memory_order_acq_rel)) {
                idx_out = i;
                return true;
            }
        }
        return false;
    }

    Slot& writer_slot(size_t idx) { return (*slots_)[idx]; }

    void publish_ready(size_t idx) {
        hdr_->state[idx].store(static_cast<uint8_t>(BlobState::Ready),
                               std::memory_order_release);
    }

    const Slot& reader_slot(size_t idx) const { return (*slots_)[idx]; }

    bool mark_reading(size_t idx) {
        uint8_t exp = static_cast<uint8_t>(BlobState::Ready);
        return hdr_->state[idx].compare_exchange_strong(exp,
                                                        static_cast<uint8_t>(BlobState::Reading),
                                                        std::memory_order_acq_rel);
    }

    void release(size_t idx) {
        hdr_->state[idx].store(static_cast<uint8_t>(BlobState::Free),
                               std::memory_order_release);
    }
};
