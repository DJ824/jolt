//
//
// #pragma once
// #include <array>
// #include <atomic>
// #include <cstddef>
// #include <cstdlib>
// #include <cstring>
// #include <optional>
// #include <utility>
// #include <cstdint>
// #include <memory>
// #include <new>
// #include <type_traits>
// #include <sys/mman.h>
// #include <unistd.h>
//
// #ifndef CACHE_LINE_SIZE
// #define CACHE_LINE_SIZE 64
// #endif
//
// struct RingDeleter {
//     size_t bytes{0};
//     bool used_mmap{false};
//
//     void operator()(void* p) const noexcept {
//         if (!p) {
//             return;
//         }
//
//         if (used_mmap) {
//             ::munmap(p, bytes);
//         } else {
//             ::free(p);
//         }
//     }
// };
//
// struct RingAllocOpt {
//     size_t align = 64;
//     bool try_huge = true;
//     bool prefault = true;
//     bool mlock_pages = true;
//     int numa_node = -1;
// };
//
// inline std::unique_ptr<void, RingDeleter>
// allocate_ring_bytes(size_t nbytes, const RingAllocOpt& opt, bool& got_huge) {
//     got_huge = false;
//
//     const size_t HUGEPG = 2 * 1024 * 1024;
//     const bool can_try_huge = opt.try_huge && nbytes >= HUGEPG;
//
//     if (can_try_huge) {
//         size_t map_len = (nbytes + HUGEPG - 1) / HUGEPG * HUGEPG;
//         int flags = MAP_PRIVATE | MAP_ANONYMOUS;
//         void* p = ::mmap(nullptr, map_len, PROT_READ | PROT_WRITE, flags, -1, 0);
//
//         if (p != MAP_FAILED && p) {
//             (void)::madvise(p, map_len, MADV_HUGEPAGE);
//             if (opt.prefault) {
//                 (void)::madvise(p, map_len, MADV_WILLNEED);
//             }
//             if (opt.mlock_pages) {
//                 (void)::mlock(p, map_len);
//             }
//             got_huge = true;
//             return {p, RingDeleter{map_len, true}};
//         }
//     }
//
//     void* p = nullptr;
//     size_t align = opt.align;
//     if (align < alignof(std::max_align_t)) {
//         align = alignof(std::max_align_t);
//     }
//     if (int rc = ::posix_memalign(&p, align, nbytes); rc != 0 || !p) {
//         throw std::bad_alloc();
//     }
//     if (opt.prefault) {
//         std::memset(p, 0, nbytes);
//     }
//     return {p, RingDeleter{nbytes, false}};
// }
//
// template <typename T, size_t SIZE>
// class LockFreeQueue {
//     static_assert((SIZE & (SIZE - 1)) == 0, "size must be power of 2");
//     static constexpr size_t CAPACITY = SIZE;
//     static constexpr size_t MASK = CAPACITY - 1;
//     static constexpr size_t PADDING = (CACHE_LINE_SIZE - 1) / sizeof(T) + 1;
//
//     alignas(CACHE_LINE_SIZE) std::atomic<size_t> head_{0};
//     alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail_{0};
//     alignas(CACHE_LINE_SIZE) size_t head_cache_{0};
//     size_t pending_next_tail_{0};
//     bool write_pending_{false};
//     alignas(CACHE_LINE_SIZE) size_t tail_cache_{0};
//
//     using Slot = std::aligned_storage_t<sizeof(T), alignof(T)>;
//     const size_t nslots_ = CAPACITY + 2 * PADDING;
//     Slot* base_{nullptr};
//     bool using_huge_{false};
//     std::unique_ptr<void, RingDeleter> mem_;
//
//     T* get_slot(size_t idx) noexcept {
//         return reinterpret_cast<T*>(base_ + (idx & MASK) + PADDING);
//     }
//
// public:
//     explicit LockFreeQueue(const RingAllocOpt& opt = {}) {
//         const size_t bytes = nslots_ * sizeof(Slot);
//         mem_ = allocate_ring_bytes(bytes, opt, using_huge_);
//         base_ = reinterpret_cast<Slot*>(mem_.get());
//         if (!base_) {
//             throw std::bad_alloc();
//         }
//     }
//
//     ~LockFreeQueue() {
//         size_t curr_head = head_.load(std::memory_order_acquire);
//         size_t curr_tail = tail_.load(std::memory_order_acquire);
//         while (curr_head != curr_tail) {
//             if constexpr (!std::is_trivially_destructible_v<T>) {
//                 get_slot(curr_head)->~T();
//             }
//             curr_head = (curr_head + 1) & MASK;
//         }
//     }
//
//     LockFreeQueue(const LockFreeQueue&) = delete;
//     LockFreeQueue& operator=(const LockFreeQueue&) = delete;
//
//     T* request_write() {
//         if (write_pending_) [[unlikely]] {
//             return nullptr;
//         }
//
//         const size_t curr_tail = tail_.load(std::memory_order_relaxed);
//         const size_t next_tail = (curr_tail + 1) & MASK;
//
//         if (next_tail == head_cache_) [[unlikely]] {
//             head_cache_ = head_.load(std::memory_order_acquire);
//             if (next_tail == head_cache_) {
//                 return nullptr;
//             }
//         }
//
//         pending_next_tail_ = next_tail;
//         write_pending_ = true;
//         return get_slot(curr_tail);
//     }
//
//     void commit_write() {
//         tail_.store(pending_next_tail_, std::memory_order_release);
//         write_pending_ = false;
//     }
//
//     void cancel_write() noexcept {
//         write_pending_ = false;
//     }
//
//     template <typename U>
//     bool enqueue(U&& item) {
//         //__builtin_prefetch(get_slot(next_tail), 1, 1);
//         T* slot = request_write();
//         if (!slot) {
//             return false;
//         }
//         if constexpr (std::is_trivially_copyable_v<T> &&
//                       std::is_same_v<std::remove_cvref_t<U>, T>) {
//             std::memcpy(slot, std::addressof(item), sizeof(T));
//         } else {
//             new (slot) T(std::forward<U>(item));
//         }
//         commit_write();
//         return true;
//     }
//
//     T* front() {
//         const size_t curr_head = head_.load(std::memory_order_relaxed);
//         if (curr_head == tail_cache_) [[unlikely]] {
//             tail_cache_ = tail_.load(std::memory_order_acquire);
//             if (curr_head == tail_cache_) {
//                 return nullptr;
//             }
//         }
//
//         //__builtin_prefetch(get_slot((curr_head + 1) & MASK), 0, 1);
//         return get_slot(curr_head);
//     }
//
//     void pop() {
//         const size_t curr_head = head_.load(std::memory_order_relaxed);
//         if constexpr (!std::is_trivially_destructible_v<T>) {
//             get_slot(curr_head)->~T();
//         }
//
//         head_.store((curr_head + 1) & MASK, std::memory_order_release);
//     }
//
//     std::optional<T> dequeue() {
//         const size_t curr_head = head_.load(std::memory_order_relaxed);
//         if (curr_head == tail_cache_) [[unlikely]] {
//             tail_cache_ = tail_.load(std::memory_order_acquire);
//             if (curr_head == tail_cache_) return std::nullopt;
//         }
//
//         T* item_ptr = get_slot(curr_head);
//         std::optional<T> result(std::move(*item_ptr));
//         if constexpr (!std::is_trivially_destructible_v<T>) {
//             item_ptr->~T();
//         }
//
//         head_.store((curr_head + 1) & MASK, std::memory_order_release);
//         return result;
//     }
//
//     bool try_dequeue(T* out) {
//         const size_t curr_head = head_.load(std::memory_order_relaxed);
//         if (curr_head == tail_cache_) [[unlikely]] {
//             tail_cache_ = tail_.load(std::memory_order_acquire);
//             if (curr_head == tail_cache_) {
//                 return false;
//             }
//         }
//
//         T* item_ptr = get_slot(curr_head);
//         if (out) {
//             if constexpr (std::is_trivially_copyable_v<T>) {
//                 std::memcpy(out, item_ptr, sizeof(T));
//             } else {
//                 *out = std::move(*item_ptr);
//             }
//         }
//         if constexpr (!std::is_trivially_destructible_v<T>) {
//             item_ptr->~T();
//         }
//         head_.store((curr_head + 1) & MASK, std::memory_order_release);
//         return true;
//     }
//
//     bool try_pop(T& out) {
//         return try_dequeue(std::addressof(out));
//     }
//
//     bool empty() const {
//         return head_.load(std::memory_order_acquire) ==
//                tail_.load(std::memory_order_acquire);
//     }
//
//     size_t size() const {
//         const size_t h = head_.load(std::memory_order_acquire);
//         const size_t t = tail_.load(std::memory_order_acquire);
//         return (t - h) & MASK;
//     }
//
//     size_t capacity() const { return CAPACITY - 1; }
//
//     bool using_huge_pages() const noexcept { return using_huge_; }
//
//
//
// };
//



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



};
