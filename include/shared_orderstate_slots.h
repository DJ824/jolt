#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

enum class SharedSlotMode : uint8_t { Create = 0, Attach = 1 };
enum class SharedSlotState : uint8_t { Free = 0, Writing = 1, Ready = 2, Reading = 3 };

namespace shared_slot_detail {
    inline std::string normalize_shm_name(std::string name) {
        if (name.empty()) {
            throw std::runtime_error("shared slot name cannot be empty");
        }
        if (name.front() != '/') {
            name.insert(name.begin(), '/');
        }
        return name;
    }
}

template <typename T>
class SharedOrderStateSlots {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert(std::is_trivially_destructible_v<T>, "T must be trivially destructible");

public:
    struct Options {
        uint32_t initial_slots{1u << 15};
        uint32_t grow_slots{1u << 15};
    };

private:
    struct Header {
        uint64_t magic{0x534C4F5453544154ULL}; // "SLOTSTAT"
        uint32_t version{1};
        uint32_t slot_bytes{sizeof(T)};
        std::atomic<uint32_t> active_slots{0};
        uint32_t grow_slots{0};
        std::atomic<uint8_t> ready{0};
    };

    struct SlotRecord {
        std::atomic<uint8_t> state{static_cast<uint8_t>(SharedSlotState::Free)};
        T value{};
    };

    int fd_{-1};
    void* map_{nullptr};
    size_t mapped_bytes_{0};
    size_t mapped_slots_{0};
    Header* hdr_{nullptr};
    SlotRecord* records_{nullptr};
    std::string name_;
    bool owner_{false};
    uint32_t grow_slots_{0};

    static constexpr size_t align_up(const size_t value, const size_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    static constexpr size_t records_offset() {
        return align_up(sizeof(Header), alignof(SlotRecord));
    }

    static constexpr size_t bytes_needed(const size_t slot_count) {
        return records_offset() + slot_count * sizeof(SlotRecord);
    }

    void refresh_views() {
        hdr_ = reinterpret_cast<Header*>(map_);
        records_ = reinterpret_cast<SlotRecord*>(static_cast<std::byte*>(map_) + records_offset());
        mapped_slots_ = (mapped_bytes_ - records_offset()) / sizeof(SlotRecord);
    }

    bool remap_to_bytes(const size_t target_bytes) {
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

    bool grow_mapping_to_slots(const uint32_t required_slots) {
        if (required_slots <= mapped_slots_) {
            return true;
        }
        if (required_slots > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        const size_t target_bytes = bytes_needed(required_slots);
        if (owner_) {
            if (::ftruncate(fd_, static_cast<off_t>(target_bytes)) != 0) {
                return false;
            }
            return remap_to_bytes(target_bytes);
        }

        struct stat st{};
        if (::fstat(fd_, &st) != 0) {
            return false;
        }
        const size_t file_bytes = static_cast<size_t>(st.st_size);
        if (file_bytes < target_bytes) {
            return false;
        }
        return remap_to_bytes(file_bytes);
    }

    bool ensure_mapped_for_index(const uint32_t idx) {
        return grow_mapping_to_slots(idx + 1);
    }

    [[nodiscard]] bool valid_index(const uint32_t idx) const {
        return idx < hdr_->active_slots.load(std::memory_order_acquire);
    }

public:
    SharedOrderStateSlots(const std::string& name, const SharedSlotMode mode, const Options& options = Options{})
        : name_(shared_slot_detail::normalize_shm_name(name)) {
        if (options.initial_slots == 0) {
            throw std::runtime_error("initial_slots must be > 0");
        }
        if (options.grow_slots == 0) {
            throw std::runtime_error("grow_slots must be > 0");
        }

        const int oflag = (mode == SharedSlotMode::Create) ? (O_CREAT | O_RDWR) : O_RDWR;
        fd_ = ::shm_open(name_.c_str(), oflag, 0600);
        if (fd_ < 0) {
            throw std::runtime_error("shm_open failed");
        }

        owner_ = (mode == SharedSlotMode::Create);
        grow_slots_ = options.grow_slots;

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
                throw std::runtime_error("shared slot mapping too small");
            }
        }

        map_ = ::mmap(nullptr, mapped_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (map_ == MAP_FAILED) {
            throw std::runtime_error("mmap failed");
        }
        refresh_views();

        if (owner_) {
            std::memset(hdr_, 0, sizeof(Header));
            hdr_->magic = 0x534C4F5453544154ULL;
            hdr_->version = 1;
            hdr_->slot_bytes = sizeof(T);
            hdr_->grow_slots = options.grow_slots;
            hdr_->active_slots.store(options.initial_slots, std::memory_order_relaxed);
            for (uint32_t i = 0; i < options.initial_slots; ++i) {
                records_[i].state.store(static_cast<uint8_t>(SharedSlotState::Free), std::memory_order_relaxed);
            }
            hdr_->ready.store(1, std::memory_order_release);
        } else {
            if (hdr_->magic != 0x534C4F5453544154ULL || hdr_->version != 1 || hdr_->slot_bytes != sizeof(T)) {
                throw std::runtime_error("shared slot shape mismatch");
            }
            grow_slots_ = hdr_->grow_slots;
            const uint32_t active = hdr_->active_slots.load(std::memory_order_acquire);
            if (active == 0) {
                throw std::runtime_error("shared slot active size is invalid");
            }
            if (active > mapped_slots_ && !grow_mapping_to_slots(active)) {
                throw std::runtime_error("shared slot remap failed");
            }
        }
    }

    ~SharedOrderStateSlots() {
        if (map_ && map_ != MAP_FAILED) {
            ::munmap(map_, mapped_bytes_);
        }
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    [[nodiscard]] uint32_t active_slots() const {
        return hdr_->active_slots.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint32_t grow_slots() const {
        return grow_slots_;
    }

    bool add_page(uint32_t* first_new_idx, uint32_t* end_new_idx_exclusive) {
        if (!owner_) {
            return false;
        }
        const uint32_t current = hdr_->active_slots.load(std::memory_order_acquire);
        const uint64_t proposed = static_cast<uint64_t>(current) + static_cast<uint64_t>(grow_slots_);
        const uint32_t next = (proposed > std::numeric_limits<uint32_t>::max())
                                  ? std::numeric_limits<uint32_t>::max()
                                  : static_cast<uint32_t>(proposed);
        if (next <= current) {
            return false;
        }
        if (!grow_mapping_to_slots(next)) {
            return false;
        }
        for (uint32_t i = current; i < next; ++i) {
            records_[i].state.store(static_cast<uint8_t>(SharedSlotState::Free), std::memory_order_release);
        }
        hdr_->active_slots.store(next, std::memory_order_release);
        if (first_new_idx) {
            *first_new_idx = current;
        }
        if (end_new_idx_exclusive) {
            *end_new_idx_exclusive = next;
        }
        return true;
    }

    template <typename Fn>
    bool write_slot(const uint32_t idx, Fn&& writer) {
        if (!valid_index(idx) || !ensure_mapped_for_index(idx)) {
            return false;
        }
        auto& rec = records_[idx];

        uint8_t expected = static_cast<uint8_t>(SharedSlotState::Free);
        if (!rec.state.compare_exchange_strong(expected,
                                               static_cast<uint8_t>(SharedSlotState::Writing),
                                               std::memory_order_acq_rel)) {
            expected = static_cast<uint8_t>(SharedSlotState::Ready);
            if (!rec.state.compare_exchange_strong(expected,
                                                   static_cast<uint8_t>(SharedSlotState::Writing),
                                                   std::memory_order_acq_rel)) {
                return false;
            }
        }

        writer(rec.value);
        rec.state.store(static_cast<uint8_t>(SharedSlotState::Ready), std::memory_order_release);
        return true;
    }

    template <typename Fn>
    bool read_slot(const uint32_t idx, Fn&& reader) const {
        auto* self = const_cast<SharedOrderStateSlots*>(this);
        if (!valid_index(idx) || !self->ensure_mapped_for_index(idx)) {
            return false;
        }
        auto& rec = records_[idx];

        uint8_t expected = static_cast<uint8_t>(SharedSlotState::Ready);
        if (!rec.state.compare_exchange_strong(expected,
                                               static_cast<uint8_t>(SharedSlotState::Reading),
                                               std::memory_order_acq_rel)) {
            return false;
        }

        reader(rec.value);
        rec.state.store(static_cast<uint8_t>(SharedSlotState::Ready), std::memory_order_release);
        return true;
    }

    bool release_slot(const uint32_t idx) {
        if (!valid_index(idx) || !ensure_mapped_for_index(idx)) {
            return false;
        }
        auto& rec = records_[idx];
        uint8_t expected = static_cast<uint8_t>(SharedSlotState::Ready);
        return rec.state.compare_exchange_strong(expected,
                                                 static_cast<uint8_t>(SharedSlotState::Free),
                                                 std::memory_order_acq_rel);
    }

    [[nodiscard]] uint8_t slot_state(const uint32_t idx) const {
        if (!valid_index(idx)) {
            return static_cast<uint8_t>(SharedSlotState::Free);
        }
        return records_[idx].state.load(std::memory_order_acquire);
    }
};

