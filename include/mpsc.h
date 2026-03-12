#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <immintrin.h>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

template <typename T, size_t Capacity>
class MPSC {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
    static_assert(alignof(T) <= 64, "T alignment must be <= 64.");
    static_assert(std::is_copy_constructible_v<T>, "T must be copy constructible.");
    static_assert(std::is_copy_assignable_v<T>, "T must be copy assignable.");

    static constexpr size_t idx_mask_ = Capacity - 1;

    using Storage = std::aligned_storage_t<sizeof(T), alignof(T)>;

    struct alignas(64) Slot {
        std::atomic<uint64_t> seq{0};
        Storage storage{};

        T* value_ptr() {
            return std::launder(reinterpret_cast<T*>(&storage));
        }

        const T* value_ptr() const {
            return std::launder(reinterpret_cast<const T*>(&storage));
        }

        void* storage_ptr() {
            return &storage;
        }
    };

    alignas(64) std::atomic<uint64_t> tail_{0};
    alignas(64) uint64_t head_{0};
    alignas(64) std::array<Slot, Capacity> slots_{};

public:
    MPSC() {
        for (size_t i = 0; i < Capacity; ++i) {
            slots_[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    ~MPSC() {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            const uint64_t tail = tail_.load(std::memory_order_acquire);
            for (uint64_t pos = head_; pos < tail; ++pos) {
                Slot& slot = slots_[pos & idx_mask_];
                if (slot.seq.load(std::memory_order_acquire) == pos + 1) {
                    slot.value_ptr()->~T();
                }
            }
        }
    }

    MPSC(const MPSC&) = delete;
    MPSC& operator=(const MPSC&) = delete;

    template <typename U, typename = std::enable_if_t<std::is_constructible_v<T, U&&>>>
    bool enqueue(U&& value) {
        uint64_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = slots_[pos & idx_mask_];
            const uint64_t seq = slot.seq.load(std::memory_order_acquire);
            const intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (diff == 0) {
                if (tail_.compare_exchange_weak(
                    pos,
                    pos + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                    if constexpr (std::is_trivially_copyable_v<T>) {
                        if constexpr (std::is_same_v<std::decay_t<U>, T>) {
                            std::memcpy(slot.storage_ptr(), &value, sizeof(T));
                        }
                        else {
                            T tmp(std::forward<U>(value));
                            std::memcpy(slot.storage_ptr(), &tmp, sizeof(T));
                        }
                    }
                    else {
                        ::new (slot.storage_ptr()) T(std::forward<U>(value));
                    }
                    slot.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
            }
            else if (diff < 0) {
                return false; // queue full
            }
            else {
                pos = tail_.load(std::memory_order_relaxed);
            }
            _mm_pause();
        }
    }

    bool try_dequeue(T* out) {
        if (!out) {
            return false;
        }

        const uint64_t pos = head_;
        Slot& slot = slots_[pos & idx_mask_];
        const uint64_t seq = slot.seq.load(std::memory_order_acquire);
        const intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
        if (diff != 0) {
            return false;
        }

        *out = *slot.value_ptr();
        if constexpr (!std::is_trivially_destructible_v<T>) {
            slot.value_ptr()->~T();
        }
        slot.seq.store(pos + Capacity, std::memory_order_release);
        head_ = pos + 1;
        return true;
    }

    std::optional<T> dequeue() {
        T out{};
        if (!try_dequeue(&out)) {
            return std::nullopt;
        }
        return out;
    }
};
