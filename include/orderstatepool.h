#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace jolt {
    template <typename T, uint32_t PageSlots = (1u << 16)>
    class SlabPool {
        static_assert(PageSlots > 0, "PageSlots must be > 0");

        std::vector<std::unique_ptr<T[]>> pages_{};
        uint64_t capacity_slots_{0};

        T* ptr_from_index(const uint64_t index) {
            const uint64_t page_idx = index / PageSlots;
            const uint64_t slot_idx = index % PageSlots;
            return pages_[page_idx].get() + static_cast<size_t>(slot_idx);
        }

        const T* ptr_from_index(const uint64_t index) const {
            const uint64_t page_idx = index / PageSlots;
            const uint64_t slot_idx = index % PageSlots;
            return pages_[page_idx].get() + static_cast<size_t>(slot_idx);
        }

        void add_page() {
            pages_.push_back(std::make_unique<T[]>(PageSlots));
            capacity_slots_ += PageSlots;
        }


    public:
        SlabPool() = default;

        explicit SlabPool(const uint64_t preallocate_slots) {
            reserve(preallocate_slots);
        }

        void reserve(const uint64_t slot_count) {
            if (slot_count <= capacity_slots_) {
                return;
            }

            const uint64_t required_pages = (slot_count + PageSlots - 1) / PageSlots;
            while (pages_.size() < required_pages) {
                add_page();
            }
        }

        T* acquire(const uint64_t slot_id) {
            if (slot_id == 0) {
                return nullptr;
            }

            const uint64_t index = slot_id - 1;
            if (index >= capacity_slots_) {
                reserve(index + 1);
            }
            return ptr_from_index(index);
        }

        T* get(const uint64_t slot_id) {
            if (slot_id == 0) {
                return nullptr;
            }

            const uint64_t index = slot_id - 1;
            if (index >= capacity_slots_) {
                return nullptr;
            }
            return ptr_from_index(index);
        }

        const T* get(const uint64_t slot_id) const {
            if (slot_id == 0) {
                return nullptr;
            }

            const uint64_t index = slot_id - 1;
            if (index >= capacity_slots_) {
                return nullptr;
            }
            return ptr_from_index(index);
        }

        [[nodiscard]] uint64_t capacity_slots() const {
            return capacity_slots_;
        }

        [[nodiscard]] uint64_t page_count() const {
            return pages_.size();
        }


    };
}
