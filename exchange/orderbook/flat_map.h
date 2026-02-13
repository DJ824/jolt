#pragma once

#include <vector>
#include <cstdint>
#include <utility>
#include <functional>
#include <cassert>

namespace jolt::ob {
    inline std::size_t round_up_pow2(std::size_t n) {
        if (n < 2) {
            return 2;
        }
        --n;
        for (std::size_t i = 1; i < sizeof(std::size_t) * 8; i <<= 1) {
            n |= n >> i;
        }
        return ++n;
    }

    inline std::size_t next(std::size_t i, std::size_t mask) { return (i + 1) & mask; }
    inline std::size_t diff(std::size_t a, std::size_t b, std::size_t mask) { return (mask + 1 + a - b) & mask; }

    template <typename KeyT, typename ValueT>
    class FlatMap {
        struct Bucket {
            KeyT key;
            ValueT val;
        };

        KeyT empty_key_{};
        KeyT tombstone_key_{};
        float max_load_{};
        std::hash<KeyT> hasher_{};
        std::vector<Bucket> buckets_{};
        std::size_t size_{0};

        void reserve_if_needed(std::size_t want) {
            if (static_cast<float>(want) > max_load_ * buckets_.size()) {
                rehash(buckets_.size() << 1);
            }
        }

        void rehash(std::size_t new_cap) {
            new_cap = round_up_pow2(new_cap);
            std::vector<Bucket> new_buckets(new_cap, Bucket{empty_key_, ValueT{}});
            const std::size_t new_mask = new_cap - 1;
            for (const Bucket& b : buckets_) {
                if (b.key == empty_key_ || b.key == tombstone_key_) {
                    continue;
                }
                std::size_t idx = hasher_(b.key) & new_mask;
                while (new_buckets[idx].key != empty_key_) {
                    idx = next(idx, new_mask);
                }
                new_buckets[idx] = b;
            }
            buckets_.swap(new_buckets);
        }
    public:

        explicit FlatMap(std::size_t capacity = 1 << 15, KeyT empty_key = static_cast<KeyT>(~KeyT{}),
                         float max_load = 0.5f)
            : empty_key_(empty_key), tombstone_key_(static_cast<KeyT>(~KeyT{} - 1)), max_load_(max_load), hasher_(),
              buckets_(round_up_pow2(capacity), Bucket{empty_key_, ValueT{}}) {
        }

        bool empty() const noexcept { return size_ == 0; }
        std::size_t size() const noexcept { return size_; }
        std::size_t capacity() const noexcept { return buckets_.size(); }

        std::pair<ValueT&, bool> insert(const KeyT& key, const ValueT& value) {
            assert(key != empty_key_ && "key collides with sentinel");
            reserve_if_needed(size_ + 1);
            const std::size_t mask = buckets_.size() - 1;
            std::size_t idx = hasher_(key) & mask;
            std::size_t first_tomb = static_cast<std::size_t>(-1);

            for (;;) {
                if (buckets_[idx].key == empty_key_) {
                    std::size_t insert_idx = (first_tomb != static_cast<std::size_t>(-1)) ? first_tomb : idx;
                    buckets_[insert_idx] = Bucket{key, value};
                    ++size_;
                    return {buckets_[insert_idx].val, true};
                }
                if (buckets_[idx].key == tombstone_key_ && first_tomb == static_cast<std::size_t>(-1)) {
                    first_tomb = idx;
                }
                if (buckets_[idx].key == key) {
                    buckets_[idx].val = value;
                    return {buckets_[idx].val, false};
                }
                idx = next(idx, mask);
            }
        }

        ValueT* find(const KeyT& key) noexcept {
            if (key == empty_key_) return nullptr;
            const std::size_t mask = buckets_.size() - 1;
            std::size_t idx = hasher_(key) & mask;
            for (;;) {
                if (buckets_[idx].key == key) {
                    return &buckets_[idx].val;
                }
                if (buckets_[idx].key == empty_key_) {
                    return nullptr;
                }
                idx = next(idx, mask);
            }
        }

        const ValueT* find(const KeyT& key) const noexcept {
            if (key == empty_key_) return nullptr;
            const std::size_t mask = buckets_.size() - 1;
            std::size_t idx = hasher_(key) & mask;
            for (;;) {
                if (buckets_[idx].key == key) {
                    return &buckets_[idx].val;
                }
                if (buckets_[idx].key == empty_key_) {
                    return nullptr;
                }
                idx = next(idx, mask);
            }
        }

        std::size_t erase(const KeyT& key) noexcept {
            if (key == empty_key_) return 0;
            const std::size_t mask = buckets_.size() - 1;
            std::size_t idx = hasher_(key) & mask;
            for (;;) {
                if (buckets_[idx].key == key) {
                    buckets_[idx].key = tombstone_key_;
                    --size_;
                    return 1;
                }
                if (buckets_[idx].key == empty_key_) return 0;
                idx = next(idx, mask);
            }
        }

        ValueT& operator[](const KeyT& key) {
            if (key == empty_key_) {
                return nullptr;
            }
            const size_t mask = buckets_.size() - 1;
            size_t idx = hasher_(key) & mask;
            for (;;) {
                if (buckets_[idx].key == key) {
                    return buckets_[idx].val;
                }

                if (buckets_[idx].key == empty_key_) {
                    return nullptr;
                }

                idx = next(idx, mask);
            }
        }

        void reserve(std::size_t n) { rehash(static_cast<std::size_t>(n / max_load_)); }
    };
} // namespace jolt::ob
