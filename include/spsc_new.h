
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

struct RingAllocOpt {
    size_t align = 64;
    bool try_huge = true;
    bool prefault = true;
    bool mlock_pages = true;
    int numa_node = -1;
};

template <typename T, size_t SIZE>
class LockFreeQueue {
    static_assert((SIZE & (SIZE - 1)) == 0, "size must be power of 2");

    static constexpr size_t CAPACITY = SIZE;
    static constexpr size_t MASK = CAPACITY - 1;
    static constexpr size_t PADDING = (CACHE_LINE_SIZE - 1) / sizeof(T) + 1;

    struct alignas(CACHE_LINE_SIZE) WriterCacheLine {
        std::atomic<size_t> write_index_{0};
        size_t read_index_cache_{0};
        const size_t padding_cache_{PADDING};
    } writer_;

    struct alignas(CACHE_LINE_SIZE) ReaderCacheLine {
        std::atomic<size_t> read_index_{0};
        size_t write_index_cache_{0};
        size_t capacity_cache_{CAPACITY};
    } reader_;

    using Slot = std::aligned_storage_t<sizeof(T), alignof(T)>;
    static constexpr size_t NSLOTS = CAPACITY + 2 * PADDING;
    std::array<Slot, NSLOTS> buffer_{};

    [[nodiscard]] T* slot_ptr(size_t index) noexcept {
        return std::launder(
            reinterpret_cast<T*>(buffer_.data() + index + PADDING));
    }

public:
    explicit LockFreeQueue(const RingAllocOpt& opt = {}) {
        (void)opt;
    }

    ~LockFreeQueue() {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            size_t read_index = reader_.read_index_.load(std::memory_order_acquire);
            const size_t write_index = writer_.write_index_.load(std::memory_order_acquire);
            while (read_index != write_index) {
                slot_ptr(read_index)->~T();
                read_index = (read_index + 1) & MASK;
            }
        }
    }

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    template <typename... Args>
    void emplace(Args&&... args) noexcept {
        const size_t write_index = writer_.write_index_.load(std::memory_order_relaxed);
        const size_t next_write_index = (write_index + 1) & MASK;

        while (next_write_index == writer_.read_index_cache_) [[unlikely]] {
            writer_.read_index_cache_ = reader_.read_index_.load(std::memory_order_acquire);
        }

        T* dst = slot_ptr(write_index);
        if constexpr (sizeof...(Args) == 1) {
            auto&& arg = std::get<0>(std::forward_as_tuple(args...));
            using Arg = std::remove_cvref_t<decltype(arg)>;
            if constexpr (std::is_trivially_copyable_v<T> && std::is_same_v<Arg, T>) {
                std::memcpy(dst, std::addressof(arg), sizeof(T));
            } else {
                new (dst) T(std::forward<Args>(args)...);
            }
        } else {
            new (dst) T(std::forward<Args>(args)...);
        }
        writer_.write_index_.store(next_write_index, std::memory_order_release);
    }

    template <typename... Args>
    [[nodiscard]] bool try_emplace(Args&&... args) {
        const size_t write_index = writer_.write_index_.load(std::memory_order_relaxed);
        const size_t next_write_index = (write_index + 1) & MASK;

        if (next_write_index == writer_.read_index_cache_) [[unlikely]] {
            writer_.read_index_cache_ = reader_.read_index_.load(std::memory_order_acquire);
            if (next_write_index == writer_.read_index_cache_) {
                return false;
            }

        }

        T* dst = slot_ptr(write_index);
        if constexpr (sizeof...(Args) == 1) {
            auto&& arg = std::get<0>(std::forward_as_tuple(args...));
            using Arg = std::remove_cvref_t<decltype(arg)>;
            if constexpr (std::is_trivially_copyable_v<T> && std::is_same_v<Arg, T>) {
                std::memcpy(dst, std::addressof(arg), sizeof(T));
            } else {
                new (dst) T(std::forward<Args>(args)...);
            }
        } else {
            new (dst) T(std::forward<Args>(args)...);
        }
        writer_.write_index_.store(next_write_index, std::memory_order_release);
        return true;
    }

    void pop(T& out) noexcept {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);

        while (read_index == reader_.write_index_cache_) [[unlikely]] {
            reader_.write_index_cache_ = writer_.write_index_.load(std::memory_order_acquire);
        }

        T* src = slot_ptr(read_index);
        if constexpr (std::is_trivially_copyable_v<T>) {
            std::memcpy(std::addressof(out), src, sizeof(T));
        } else if constexpr (std::is_move_assignable_v<T>) {
            out = std::move(*src);
        } else {
            out = *src;
        }
        if constexpr (!std::is_trivially_destructible_v<T>) {
            src->~T();
        }
        reader_.read_index_.store((read_index + 1) & MASK, std::memory_order_release);
    }

    [[nodiscard]] bool try_pop(T& out) {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);

        if (read_index == reader_.write_index_cache_) [[unlikely]] {
            reader_.write_index_cache_ = writer_.write_index_.load(std::memory_order_acquire);
            if (read_index == reader_.write_index_cache_) {
                return false;
            }
        }

        T* src = slot_ptr(read_index);
        if constexpr (std::is_trivially_copyable_v<T>) {
            std::memcpy(std::addressof(out), src, sizeof(T));
        } else if constexpr (std::is_move_assignable_v<T>) {
            out = std::move(*src);
        } else {
            out = *src;
        }
        if constexpr (!std::is_trivially_destructible_v<T>) {
            src->~T();
        }
        reader_.read_index_.store((read_index + 1) & MASK, std::memory_order_release);
        return true;
    }

    template <typename U>
    bool enqueue(U&& item) {
        return try_emplace(std::forward<U>(item));
    }

    T* front() {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);
        if (read_index == reader_.write_index_cache_) [[unlikely]] {
            reader_.write_index_cache_ = writer_.write_index_.load(std::memory_order_acquire);
            if (read_index == reader_.write_index_cache_) {
                return nullptr;
            }
        }

        return slot_ptr(read_index);
    }

    void pop() {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);
        if constexpr (!std::is_trivially_destructible_v<T>) {
            slot_ptr(read_index)->~T();
        }
        reader_.read_index_.store((read_index + 1) & MASK, std::memory_order_release);
    }

    std::optional<T> dequeue() {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);
        if (read_index == reader_.write_index_cache_) [[unlikely]] {
            reader_.write_index_cache_ = writer_.write_index_.load(std::memory_order_acquire);
            if (read_index == reader_.write_index_cache_) {
                return std::nullopt;
            }
        }

        T* src = slot_ptr(read_index);
        std::optional<T> result{};
        if constexpr (std::is_trivially_copyable_v<T>) {
            result.emplace();
            std::memcpy(std::addressof(*result), src, sizeof(T));
        } else {
            result.emplace(std::move(*src));
        }
        if constexpr (!std::is_trivially_destructible_v<T>) {
            src->~T();
        }
        reader_.read_index_.store((read_index + 1) & MASK, std::memory_order_release);
        return result;
    }

    bool try_dequeue(T* out) {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);
        if (read_index == reader_.write_index_cache_) [[unlikely]] {
            reader_.write_index_cache_ = writer_.write_index_.load(std::memory_order_acquire);
            if (read_index == reader_.write_index_cache_) {
                return false;
            }
        }

        T* src = slot_ptr(read_index);
        if (out) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                std::memcpy(out, src, sizeof(T));
            } else {
                *out = std::move(*src);
            }
        }
        if constexpr (!std::is_trivially_destructible_v<T>) {
            src->~T();
        }
        reader_.read_index_.store((read_index + 1) & MASK, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return writer_.write_index_.load(std::memory_order_acquire) ==
               reader_.read_index_.load(std::memory_order_acquire);
    }

    size_t size() const {
        const size_t read_index = reader_.read_index_.load(std::memory_order_acquire);
        const size_t write_index = writer_.write_index_.load(std::memory_order_acquire);
        return (write_index - read_index) & MASK;
    }

    size_t capacity() const { return CAPACITY - 1; }

    bool using_huge_pages() const noexcept { return false; }


    T* get_tail_ptr() {
        const size_t write_index = writer_.write_index_.load(std::memory_order_relaxed);
        const size_t next_write_index = (write_index + 1) & MASK;

        if (next_write_index == writer_.read_index_cache_) [[unlikely]] {
            writer_.read_index_cache_ = reader_.read_index_.load(std::memory_order_acquire);
            if (next_write_index == writer_.read_index_cache_) {
                return nullptr;
            }

        }

        return slot_ptr(write_index);
    }

    void write() {
        const size_t write_index = writer_.write_index_.load(std::memory_order_relaxed);
        const size_t next_write_index = (write_index + 1) & MASK;
        writer_.write_index_.store(next_write_index, std::memory_order_release);
    }

    T* get_head_ptr() {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);
        if (read_index == reader_.write_index_cache_) [[unlikely]] {
            reader_.write_index_cache_ = writer_.write_index_.load(std::memory_order_acquire);
            if (read_index == reader_.write_index_cache_) {
                return nullptr;
            }
        }

        return slot_ptr(read_index);
    }

    void read() {
        const size_t read_index = reader_.read_index_.load(std::memory_order_relaxed);
        if constexpr (!std::is_trivially_destructible_v<T>) {
            slot_ptr(read_index)->~T();
        }
        const size_t next_read_index = (read_index + 1) & MASK;
        reader_.read_index_.store(next_read_index, std::memory_order_release);
    }

    template <typename Writer>
    bool try_write(Writer writer) {
        T* ptr = get_tail_ptr();
        if (!ptr) {
            return false;
        }

        writer(ptr);
        write();
        return true;
    }

    template <typename Reader>
    bool try_read(Reader reader) {
        T* ptr = get_head_ptr();
        if (!ptr) {
            return false;
        }
        reader(ptr);
        read();
        return true;
    }


    template <typename Fn>
      size_t drain(Fn&& fn, size_t max_items = CAPACITY - 1) {
        if (max_items == 0) {
            return 0;
        }

        size_t drained = 0;
        while (drained < max_items) {
            T* item = front();
            if (!item) {
                break;
            }
            fn(*item);
            pop();
            ++drained;
        }
        return drained;
    }


};




