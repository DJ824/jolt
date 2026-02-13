#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <cstdlib>

namespace jolt::ob {
    template <typename BlockT>
    class BlockPool {
    public:
        BlockPool() = default;

        ~BlockPool() {
            for (void* p : pages_) {
                std::free(p);
            }
        }

        BlockT* acquire() {
            if (free_list_) {
                BlockT* b = free_list_;
                free_list_ = free_list_->pool_next;
                *b = BlockT{};
                return b;
            }

            if (cursor_ + stride_ > end_) {
                new_page();
            }

            // compensate for misaligned blocks
            std::uintptr_t addr = reinterpret_cast<std::uintptr_t>(cursor_);
            std::size_t mis = addr % alignment_;
            if (mis) {
                cursor_ += (alignment_ - mis);
            }
            BlockT* blk = reinterpret_cast<BlockT*>(cursor_);
            cursor_ += stride_;
            *blk = BlockT{};
            return blk;
        }

        void release(BlockT* blk) {
            blk->pool_next = free_list_;
            free_list_ = blk;
        }

        BlockPool(const BlockPool&) = delete;
        BlockPool& operator=(const BlockPool&) = delete;

    private:
        void new_page() {
            constexpr std::size_t PAGE_SIZE = 1 << 20;
            const std::size_t page_bytes = (PAGE_SIZE + alignment_ - 1) / alignment_ * alignment_;
            void* mem = nullptr;
            const std::size_t palign = alignment_ < sizeof(void*) ? sizeof(void*) : alignment_;

            int rc = posix_memalign(&mem, palign, page_bytes);
            if (rc != 0 || mem == nullptr) {
                throw std::bad_alloc();
            }

            pages_.push_back(mem);
            cursor_ = static_cast<std::byte*>(mem);
            end_ = cursor_ + page_bytes;

            std::uintptr_t addr = reinterpret_cast<std::uintptr_t>(cursor_);
            std::size_t mis = addr % alignment_;
            if (mis) {
                cursor_ += (alignment_ - mis);
            }
        }

        std::vector<void*> pages_{};
        std::byte* cursor_{nullptr};
        std::byte* end_{nullptr};
        BlockT* free_list_{nullptr};

        static constexpr std::size_t alignment_ = alignof(BlockT);
        static constexpr std::size_t stride_ = (sizeof(BlockT) + alignment_ - 1) / alignment_ * alignment_;
        static_assert(stride_ % alignment_ == 0, "Stride must be multiple of alignment");
    };
}
