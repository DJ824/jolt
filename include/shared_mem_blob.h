#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

enum class PoolMode : uint8_t { Create = 0, Attach = 1 };
enum class SlotState : uint8_t { Free = 0, Writing = 1, Ready = 2, Reading = 3 };

struct BlobHandle {
    uint32_t idx{0};
    uint32_t gen{0};
};

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
struct Page {
    uint32_t bytes{0};
    std::array<std::byte, BlobBytes> payload{};
};


template <size_t SlotCount, typename SlotT>
class SlotPool {
    static_assert((SlotCount & (SlotCount - 1)) == 0, "sz must be pow2");
    static_assert(std::is_trivially_copyable_v<SlotT>, "SlotT must be trivially copyable");
    static_assert(std::is_trivially_destructible_v<SlotT>, "SlotT must be trivially destructible");

public:
    struct Options {
        uint32_t initial_slots{SlotCount};
        uint32_t grow_slots{0};
    };

private:
    struct Header {
        uint64_t magic{0x534E4150424C4F42ULL};
        uint32_t version{1};
        uint32_t slots_capacity{SlotCount};
        uint32_t slot_bytes{sizeof(SlotT)};
        std::atomic<uint32_t> slots_active{SlotCount};
        std::atomic<uint8_t> ready{0};
        std::array<std::atomic<uint8_t>, SlotCount> state{};
        std::array<std::atomic<uint32_t>, SlotCount> gen{};
    };


    using Slot = SlotT;
    int fd_{-1};
    void* map_{nullptr};
    size_t mapped_bytes_{0};
    size_t mapped_slots_{0};
    Header* hdr_{nullptr};
    Slot* slots_{nullptr};
    std::string name_;
    bool owner_{false};
    uint32_t grow_slots_{0};

    static constexpr size_t align_up(size_t v, size_t a) { return (v + a - 1) & ~(a - 1); }

    static constexpr size_t storage_offset() {
        return align_up(sizeof(Header), alignof(Slot));
    }

    static constexpr size_t bytes_needed(const size_t slot_count) {
        return storage_offset() + slot_count * sizeof(Slot);
    }

    void refresh_views() {
        hdr_ = reinterpret_cast<Header*>(map_);
        slots_ = reinterpret_cast<Slot*>(static_cast<std::byte*>(map_) + storage_offset());
        mapped_slots_ = (mapped_bytes_ - storage_offset()) / sizeof(Slot);
    }

    bool resize_mapping(const size_t required_slots) {
        if (required_slots <= mapped_slots_) {
            return true;
        }
        if (required_slots > SlotCount) {
            return false;
        }

        size_t target_bytes = bytes_needed(required_slots);
        if (owner_) {
            if (::ftruncate(fd_, static_cast<off_t>(target_bytes)) != 0) {
                return false;
            }
        } else {
            struct stat st{};
            if (::fstat(fd_, &st) != 0) {
                return false;
            }
            if (static_cast<size_t>(st.st_size) < target_bytes) {
                return false;
            }
            target_bytes = static_cast<size_t>(st.st_size);
        }

#if defined(__linux__) && defined(MREMAP_MAYMOVE)
        void* moved = ::mremap(map_, mapped_bytes_, target_bytes, MREMAP_MAYMOVE);
        if (moved != MAP_FAILED) {
            map_ = moved;
            mapped_bytes_ = target_bytes;
            refresh_views();
            return true;
        }
#endif

        void* new_map = ::mmap(nullptr, target_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (new_map == MAP_FAILED) {
            return false;
        }
        if (map_ && map_ != MAP_FAILED) {
            ::munmap(map_, mapped_bytes_);
        }
        map_ = new_map;
        mapped_bytes_ = target_bytes;
        refresh_views();
        return true;
    }

    bool ensure_mapped_for_index(const size_t idx) {
        return resize_mapping(idx + 1);
    }

    [[nodiscard]] bool valid_index(const size_t idx) const {
        return idx < hdr_->slots_active.load(std::memory_order_acquire);
    }

public:
    SlotPool(const std::string& name, PoolMode mode, const Options& options = Options{})
        : name_(shared_blob_detail::normalize_shm_name(name)) {
        if (options.initial_slots == 0 || options.initial_slots > SlotCount) {
            throw std::runtime_error("blob pool initial_slots is invalid");
        }

        grow_slots_ = options.grow_slots;

        int oflag = (mode == PoolMode::Create) ? (O_CREAT | O_RDWR) : O_RDWR;

        fd_ = ::shm_open(name_.c_str(), oflag, 0600);
        if (fd_ < 0) {
            throw std::runtime_error("shm_open failed");
        }

        owner_ = (mode == PoolMode::Create);

        if (owner_) {
            mapped_bytes_ = bytes_needed(options.initial_slots);
            if (::ftruncate(fd_, static_cast<off_t>(mapped_bytes_)) != 0) {
                throw std::runtime_error("ftruncate failed");
            }
        } else {
            struct stat st{};
            if (::fstat(fd_, &st) != 0) {
                throw std::runtime_error("fstat failed");
            }
            mapped_bytes_ = static_cast<size_t>(st.st_size);
            if (mapped_bytes_ < bytes_needed(1)) {
                throw std::runtime_error("shared blob too small");
            }
        }

        map_ = ::mmap(nullptr, mapped_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (map_ == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }
        refresh_views();

        if (owner_) {
            new (hdr_) Header{};
            hdr_->slots_capacity = SlotCount;
            hdr_->slot_bytes = sizeof(Slot);
            hdr_->slots_active.store(options.initial_slots, std::memory_order_relaxed);
            for (size_t i = 0; i < SlotCount; ++i) {
                hdr_->state[i].store(static_cast<uint8_t>(SlotState::Free));
                hdr_->gen[i].store(1, std::memory_order_relaxed);
            }
            hdr_->ready.store(1, std::memory_order_release);
        } else {
            if (hdr_->slots_capacity != SlotCount || hdr_->slot_bytes != sizeof(Slot)) {
                throw std::runtime_error("shared blob shape mismatch");
            }
            const uint32_t active = hdr_->slots_active.load(std::memory_order_acquire);
            if (active == 0 || active > SlotCount) {
                throw std::runtime_error("shared blob active slot count is invalid");
            }
            if (active > mapped_slots_ && !resize_mapping(active)) {
                throw std::runtime_error("shared blob remap failed");
            }
        }

    }

    ~SlotPool() {
        if (map_ && map_ != MAP_FAILED) {
            ::munmap(map_, mapped_bytes_);
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    [[nodiscard]] uint32_t active_slots() const {
        return hdr_->slots_active.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint32_t max_slots() const {
        return SlotCount;
    }

    bool add_page(uint32_t* first_new_idx, uint32_t* end_new_idx_exclusive) {
        if (!owner_ || grow_slots_ == 0) {
            return false;
        }

        const uint32_t current = hdr_->slots_active.load(std::memory_order_acquire);
        if (current >= SlotCount) {
            return false;
        }

        const uint32_t next = std::min<uint32_t>(SlotCount, current + grow_slots_);
        if (!resize_mapping(next)) {
            return false;
        }

        for (uint32_t i = current; i < next; ++i) {
            hdr_->gen[i].store(1, std::memory_order_relaxed);
            hdr_->state[i].store(static_cast<uint8_t>(SlotState::Free), std::memory_order_release);
        }
        hdr_->slots_active.store(next, std::memory_order_release);
        if (first_new_idx) {
            *first_new_idx = current;
        }
        if (end_new_idx_exclusive) {
            *end_new_idx_exclusive = next;
        }
        return true;
    }

    bool add_page() {
        return add_page(nullptr, nullptr);
    }

    bool try_acquire(BlobHandle& out) {
        for (;;) {
            const size_t active = hdr_->slots_active.load(std::memory_order_acquire);
            for (size_t i = 0; i < active; ++i) {
                uint8_t exp = static_cast<uint8_t>(SlotState::Free);
                if (!hdr_->state[i].compare_exchange_strong(exp,
                                                            static_cast<uint8_t>(SlotState::Writing),
                                                            std::memory_order_acq_rel)) {
                    continue;
                }
                out.idx = static_cast<uint32_t>(i);
                out.gen = hdr_->gen[i].load(std::memory_order_acquire);
                return true;
            }
            if (!add_page()) {
                return false;
            }
        }
    }

    bool try_acquire(size_t& idx_out) {
        BlobHandle h{};
        if (!try_acquire(h)) {
            return false;
        }
        idx_out = static_cast<size_t>(h.idx);
        return true;
    }


    Slot* get_slot(const size_t idx) {
        if (!valid_index(idx) || !ensure_mapped_for_index(idx)) {
            return nullptr;
        }
        return &slots_[idx];
    }

    const Slot* read_slot(const size_t idx) const {
        auto* self = const_cast<SlotPool*>(this);
        if (!valid_index(idx) || !self->ensure_mapped_for_index(idx)) {
            return nullptr;
        }
        return &slots_[idx];
    }

    Slot& writer_slot(const BlobHandle& h) {
        auto* slot = get_slot(h.idx);
        if (!slot) {
            throw std::out_of_range("writer_slot: idx out of range");
        }
        return *slot;
    }

    Slot& writer_slot(size_t idx) {
        auto* slot = get_slot(idx);
        if (!slot) {
            throw std::out_of_range("writer_slot: idx out of range");
        }
        return *slot;
    }

    bool publish_ready(const BlobHandle& h) {
        const size_t idx = static_cast<size_t>(h.idx);
        if (!valid_index(idx)) {
            return false;
        }
        if (hdr_->gen[idx].load(std::memory_order_acquire) != h.gen) {
            return false;
        }
        hdr_->state[idx].store(static_cast<uint8_t>(SlotState::Ready),
                               std::memory_order_release);
        return true;
    }

    void publish_ready(size_t idx) {
        if (!valid_index(idx)) {
            return;
        }
        hdr_->state[idx].store(static_cast<uint8_t>(SlotState::Ready),
                               std::memory_order_release);
    }

    const Slot& reader_slot(const BlobHandle& h) const {
        const Slot* slot = read_slot(h.idx);
        if (!slot) {
            throw std::out_of_range("reader_slot: idx out of range");
        }
        return *slot;
    }

    const Slot& reader_slot(size_t idx) const {
        const Slot* slot = read_slot(idx);
        if (!slot) {
            throw std::out_of_range("reader_slot: idx out of range");
        }
        return *slot;
    }

    bool mark_reading(const BlobHandle& h) {
        const size_t idx = static_cast<size_t>(h.idx);
        if (!valid_index(idx)) {
            return false;
        }
        if (hdr_->gen[idx].load(std::memory_order_acquire) != h.gen) {
            return false;
        }
        uint8_t exp = static_cast<uint8_t>(SlotState::Ready);
        return hdr_->state[idx].compare_exchange_strong(exp,
                                                        static_cast<uint8_t>(SlotState::Reading),
                                                        std::memory_order_acq_rel);
    }

    bool mark_reading(size_t idx) {
        if (!valid_index(idx)) {
            return false;
        }
        uint8_t exp = static_cast<uint8_t>(SlotState::Ready);
        return hdr_->state[idx].compare_exchange_strong(exp,
                                                        static_cast<uint8_t>(SlotState::Reading),
                                                        std::memory_order_acq_rel);
    }

    bool release(const BlobHandle& h) {
        const size_t idx = static_cast<size_t>(h.idx);
        if (!valid_index(idx)) {
            return false;
        }
        if (hdr_->gen[idx].load(std::memory_order_acquire) != h.gen) {
            return false;
        }
        (void)hdr_->gen[idx].fetch_add(1, std::memory_order_acq_rel);
        hdr_->state[idx].store(static_cast<uint8_t>(SlotState::Free),
                               std::memory_order_release);
        return true;
    }

    void release(size_t idx) {
        if (!valid_index(idx)) {
            return;
        }
        (void)hdr_->gen[idx].fetch_add(1, std::memory_order_acq_rel);
        hdr_->state[idx].store(static_cast<uint8_t>(SlotState::Free),
                               std::memory_order_release);
    }
};
