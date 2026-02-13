#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

namespace shared_ring_detail {
    inline std::string normalize_shm_name(std::string name) {
        if (name.empty()) {
            throw std::runtime_error("shared ring name cannot be empty");
        }
        if (name.front() != '/') {
            name.insert(name.begin(), '/');
        }
        return name;
    }
}

enum class SharedRingMode : uint8_t { Create = 0, Attach = 1 };

struct SharedRingOptions {
    bool unlink_on_destroy{false};
    int permissions{0600};
    int wait_ms{1000};
};

template <typename T, size_t CAPACITY>
class SharedSpscQueue {
    static_assert((CAPACITY & (CAPACITY - 1)) == 0, "capacity must be power of 2");
    static_assert(std::is_trivially_copyable_v<T>, "shared rings require trivially copyable types");
    static_assert(std::is_trivially_destructible_v<T>, "shared rings require trivially destructible types");

        static constexpr uint64_t kMagic = 0x4A4F4C545152494EULL;
        static constexpr uint32_t kVersion = 1;
        static constexpr size_t kMask = CAPACITY - 1;

        struct SharedRingHeader {
            uint64_t magic{0};
            uint32_t version{0};
            uint32_t capacity{0};
            uint32_t elem_size{0};
            uint32_t elem_align{0};
            uint32_t reserved{0};
            alignas(CACHE_LINE_SIZE) std::atomic<size_t> head{0};
            alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail{0};
            alignas(CACHE_LINE_SIZE) std::atomic<uint8_t> ready{0};
        };

        using Storage = std::array<T, CAPACITY>;

        std::string name_;
        SharedRingOptions options_{};
        int fd_{-1};
        void* map_{nullptr};
        size_t map_size_{0};
        bool owner_{false};
        SharedRingHeader* header_{nullptr};
        Storage* base_{nullptr};
        size_t head_cache_{0};
        size_t tail_cache_{0};

public:
    SharedSpscQueue(const std::string& name, SharedRingMode mode, const SharedRingOptions& opt = {})
        : name_(shared_ring_detail::normalize_shm_name(name)), options_(opt) {
            const size_t bytes = bytes_needed();
            const int oflag = (mode == SharedRingMode::Create) ? (O_CREAT | O_EXCL | O_RDWR) : O_RDWR;
            fd_ = ::shm_open(name_.c_str(), oflag, opt.permissions);

            if (fd_ == -1) {
                throw std::runtime_error("shm_open failed");
            }

            owner_ = (mode == SharedRingMode::Create);
            if (owner_) {
                if (::ftruncate(fd_, static_cast<off_t>(bytes)) != 0) {
                    ::close(fd_);
                    throw std::runtime_error("ftruncate failed");
                }
            }

            map_size_ = bytes;
            map_ = ::mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
            if (map_ == MAP_FAILED) {
                ::close(fd_);
                throw std::runtime_error("mmap failed");
            }
            init_view(mode);
        }

        ~SharedSpscQueue() {
            if (map_ && map_ != MAP_FAILED) {
                ::munmap(map_, map_size_);
            }
            if (fd_ != -1) {
                ::close(fd_);
            }
            if (owner_ && options_.unlink_on_destroy) {
                ::shm_unlink(name_.c_str());
            }
        }

        SharedSpscQueue(const SharedSpscQueue&) = delete;
        SharedSpscQueue& operator=(const SharedSpscQueue&) = delete;

        SharedSpscQueue(SharedSpscQueue&& other) noexcept { *this = std::move(other); }

        SharedSpscQueue& operator=(SharedSpscQueue&& other) noexcept {
            if (this == &other) {
                return *this;
            }
            name_ = std::move(other.name_);
            options_ = other.options_;
            fd_ = other.fd_;
            map_ = other.map_;
            map_size_ = other.map_size_;
            owner_ = other.owner_;
            header_ = other.header_;
            base_ = other.base_;
            head_cache_ = other.head_cache_;
            tail_cache_ = other.tail_cache_;
            other.fd_ = -1;
            other.map_ = nullptr;
            other.map_size_ = 0;
            other.owner_ = false;
            other.header_ = nullptr;
            other.base_ = nullptr;
            return *this;
        }

        template <typename U>
        bool enqueue(U&& item) {
            const size_t curr_tail = header_->tail.load(std::memory_order_relaxed);
            const size_t next_tail = (curr_tail + 1) & kMask;
            if (next_tail == head_cache_) {
                head_cache_ = header_->head.load(std::memory_order_acquire);
                if (next_tail == head_cache_) {
                    return false;
                }
            }
            (*base_)[curr_tail & kMask] = std::forward<U>(item);
            header_->tail.store(next_tail, std::memory_order_release);
            return true;
        }

        std::optional<T> dequeue() {
            const size_t curr_head = header_->head.load(std::memory_order_relaxed);
            if (curr_head == tail_cache_) {
                tail_cache_ = header_->tail.load(std::memory_order_acquire);
                if (curr_head == tail_cache_) {
                    return std::nullopt;
                }
            }
            T* item_ptr = get_slot(curr_head);
            std::optional<T> result(std::move(*item_ptr));
            header_->head.store((curr_head + 1) & kMask, std::memory_order_release);
            return result;
        }

        bool empty() const {
            return header_->head.load(std::memory_order_acquire) ==
                header_->tail.load(std::memory_order_acquire);
        }

        size_t size() const {
            const size_t h = header_->head.load(std::memory_order_acquire);
            const size_t t = header_->tail.load(std::memory_order_acquire);
            return (t - h) & kMask;
        }

        size_t capacity() const { return CAPACITY - 1; }

    private:
        static size_t align_up(size_t value, size_t align) {
            return (value + align - 1) & ~(align - 1);
        }

        static size_t bytes_needed() {
            const size_t header = align_up(sizeof(SharedRingHeader), alignof(Storage));
            return header + sizeof(Storage);
        }

        void init_view(SharedRingMode mode) {
            header_ = reinterpret_cast<SharedRingHeader*>(map_);
            if (mode == SharedRingMode::Create) {
                std::memset(header_, 0, sizeof(SharedRingHeader));
                header_->magic = kMagic;
                header_->version = kVersion;
                header_->capacity = CAPACITY;
                header_->elem_size = sizeof(T);
                header_->elem_align = alignof(T);
                header_->head.store(0, std::memory_order_release);
                header_->tail.store(0, std::memory_order_release);
                header_->ready.store(1, std::memory_order_release);
            }
            else {
                wait_ready();
                if (header_->magic != kMagic || header_->version != kVersion ||
                    header_->capacity != CAPACITY || header_->elem_size != sizeof(T) ||
                    header_->elem_align != alignof(T)) {
                    throw std::runtime_error("shared ring header mismatch");
                }
            }

            const size_t header_bytes = align_up(sizeof(SharedRingHeader), alignof(Storage));
            base_ = reinterpret_cast<Storage*>(static_cast<std::byte*>(map_) + header_bytes);
        }

        void wait_ready() {
            const auto deadline = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(options_.wait_ms);
            while (header_->ready.load(std::memory_order_acquire) == 0) {
                if (std::chrono::steady_clock::now() > deadline) {
                    throw std::runtime_error("shared ring not ready");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        T* get_slot(size_t idx) noexcept {
            return &(*base_)[idx & kMask];
        }

    };
