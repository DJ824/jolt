#pragma once
#include <cstdint>
#include <array>
#include <cstring>
#include <new>
#include <utility>
#include "block_pool.h"

namespace jolt::ob {
    // represents a block of orders
    template <typename SlotT, std::size_t K>
    struct alignas(64) Block {
        static_assert(K > 0 && (K % 64 == 0), "K must be a multiple of 64 for bitmasks");
        SlotT slots[K]{};
        // array of 64 bit bitmaps, 1 bit per slot to mark (live/tombstone)
        uint64_t live_mask[K / 64]{};
        uint16_t head{0};
        uint16_t tail{0};
        uint16_t live{0};
        Block* next{nullptr};
        Block* pool_next{nullptr};
    };

    template <typename SlotT, std::size_t K>
    class Fifo {
    public:
        using BlockT = Block<SlotT, K>;

        explicit Fifo(BlockPool<BlockT>& pool) : pool_(pool) {
        }

        struct Loc {
            BlockT* blk;
            uint16_t off;
        };

        bool empty() const { return head_ == nullptr; }
        std::size_t live_count() const { return live_count_; }

        // append a new order, returns loc (ptr to block, offset), for order access
        Loc append(const SlotT& s) {
            if (!tail_) {
                allocate_block();
            }

            if (tail_->tail == K) {
                allocate_block();
            }

            uint16_t off = tail_->tail++;
            tail_->slots[off] = s;
            set_live(tail_, off);
            ++live_count_;
            return {tail_, off};
        }

        // adds a new object via in place construction to the block
        template <typename... Args>
        Loc emplace(Args&&... args) {
            if (!tail_) {
                allocate_block();
            }
            if (tail_->tail == K) {
                allocate_block();
            }
            uint16_t off = tail_->tail++;
            ::new(static_cast<void*>(&tail_->slots[off])) SlotT{std::forward<Args>(args)...};
            set_live(tail_, off);
            ++live_count_;
            return {tail_, off};
        }

        // returns the first slot in the queue
        SlotT* head_slot() {
            skip_dead_slots();
            if (!head_) {
                return nullptr;
            }
            return &head_->slots[head_->head];
        }

        // removes orders starting from head, used for order matching
        void pop_head() {
            skip_dead_slots();
            if (!head_) {
                return;
            }
            // set slot as tombstone and advance head
            clear_live(head_, head_->head);
            ++head_->head;
            --live_count_;
            drop_empty_head_block();
        }

        // set the loc to tombstone, used for cancels
        void tombstone(const Loc& loc) {
            if (!loc.blk) {
                return;
            }
            BlockT* b = loc.blk;
            clear_live(b, loc.off);
            if (live_count_ > 0) {
                --live_count_;
            }
            if (b == head_) {
                drop_empty_head_block();
            }
        }

        template <typename Fn>
        void copy_live(Fn&& fn) const {
            BlockT* b = head_;
            while (b) {
                for (uint16_t start = b->head; start < b->tail; ++start) {
                    if (is_live(b, start)) {
                        fn(static_cast<const SlotT&>(b->slots[start]));
                    }
                }
                b = b->next;
            }
        }


        std::size_t blocks() const { return blocks_; }

    private:
        void allocate_block() {
            BlockT* nb = pool_.acquire();
            ++blocks_;
            if (!head_) {
                head_ = tail_ = nb;
            }
            else {
                tail_->next = nb;
                tail_ = nb;
            }
        }

        // map the offset slot to the bitmap and set the bit as live (1)
        static void set_live(BlockT* b, uint16_t off) {
            // get the index in the bitmap ary and then set the respective bit
            b->live_mask[off / 64] |= (1ull << (off % 64));
            ++b->live;
        }

        // map offset to bitmap and clear bit
        static void clear_live(BlockT* b, uint16_t off) {
            const uint64_t bit = (1ull << (off % 64));
            if (b->live_mask[off / 64] & bit) {
                b->live_mask[off / 64] &= ~bit;
                if (b->live > 0) {
                    --b->live;
                }
            }
        }

        static bool is_live(const BlockT* b, uint16_t off) {
            return (b->live_mask[off / 64] >> (off % 64)) & 1ull;
        }

        // advance head of the first block til we find a live order
        void skip_dead_slots() {
            while (head_) {
                while (head_->head < head_->tail && !is_live(head_, head_->head)) {
                    ++head_->head;
                }
                if (head_->head < head_->tail && is_live(head_, head_->head)) {
                    return;
                }
                drop_empty_head_block();
            }
        }

        void drop_empty_head_block() {
            if (!head_) {
                return;
            }
            if (head_->head >= head_->tail || head_->live == 0) {
                BlockT* old = head_;
                head_ = head_->next;
                if (!head_) {
                    tail_ = nullptr;
                }
                old->next = nullptr;
                old->head = old->tail = 0;
                old->live = 0;
                std::memset(old->live_mask, 0, sizeof(old->live_mask));
                pool_.release(old);
                if (blocks_ > 0) {
                    --blocks_;
                }
            }
        }


        BlockPool<BlockT>& pool_;
        BlockT* head_{nullptr};
        BlockT* tail_{nullptr};
        std::size_t live_count_{0};
        std::size_t blocks_{0};
    };
}
