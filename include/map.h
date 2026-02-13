#pragma once
#include <vector>
#include <cstdint>
#include <utility>
#include <functional>
#include <cassert>

struct Order;

inline std::size_t round_up_pow2(std::size_t n) {
    if (n < 2) {
        return 2;
    }
    --n;
    for (std::size_t i = 1; i < sizeof(std::size_t) * 8; i <<= 1)
        n |= n >> i;
    return ++n;
}

inline std::size_t get_next_idx(std::size_t i, std::size_t mask) {
    return (i + 1) & mask;
}

inline std::size_t diff(std::size_t a, std::size_t b, std::size_t mask) {
    return (mask + 1 + a - b) & mask;
}

class OrderMap {
    struct alignas(16) Bucket {
        uint64_t key;
        Order* ptr;
    };

    uint64_t empty_key_;
    float max_load_;
    std::hash<uint64_t> hasher_;
    std::vector<Bucket> buckets_;
    std::size_t size_ = 0;

    static_assert(sizeof(Bucket) == 16, "bucket must stay 16 B");

    void reserve_if_needed(std::size_t want) {
        if (static_cast<float>(want) > max_load_ * buckets_.size())
            rehash(buckets_.size() << 1);
    }

    void rehash(std::size_t new_cap) {
        new_cap = round_up_pow2(new_cap);
        std::vector<Bucket> new_buckets(new_cap,
                                        Bucket{empty_key_, nullptr});
        const std::size_t new_mask = new_cap - 1;

        for (const Bucket& b : buckets_) {
            if (b.key == empty_key_) continue;
            std::size_t idx = hasher_(b.key) & new_mask;
            while (new_buckets[idx].key != empty_key_)
                idx = get_next_idx(idx, new_mask);
            new_buckets[idx] = b;
        }
        buckets_.swap(new_buckets);
    }

public:
    explicit OrderMap(std::size_t capacity = 1 << 15,
                      uint64_t empty_key = UINT64_MAX,
                      float max_load = 0.5f)
        : empty_key_(empty_key),
          max_load_(max_load),
          hasher_(),
          buckets_(round_up_pow2(capacity),
                   Bucket{empty_key_, nullptr}) {
    }

    bool empty() const noexcept { return size_ == 0; }
    std::size_t size() const noexcept { return size_; }
    std::size_t capacity() const noexcept { return buckets_.size(); }

    std::pair<Order*&, bool> insert(uint64_t key, Order* value) {
        assert(key != empty_key_ && "key collides with sentinel");
        reserve_if_needed(size_ + 1);

        const std::size_t mask = buckets_.size() - 1;
        std::size_t idx = hasher_(key) & mask;

        for (;;) {
            if (buckets_[idx].key == empty_key_) {
                buckets_[idx] = {key, value};
                ++size_;
                return {buckets_[idx].ptr, true};
            }
            if (buckets_[idx].key == key) {
                buckets_[idx].ptr = value;
                return {buckets_[idx].ptr, false};
            }
            idx = get_next_idx(idx, mask);
        }
    }

    Order* find(uint64_t key) const noexcept {
        if (key == empty_key_) return nullptr;
        const std::size_t mask = buckets_.size() - 1;
        std::size_t idx = hasher_(key) & mask;

        for (;;) {
            if (buckets_[idx].key == key) {
                return buckets_[idx].ptr;
            }
            if (buckets_[idx].key == empty_key_) {
                return nullptr;
            }
            idx = get_next_idx(idx, mask);
        }
    }

    std::size_t erase(uint64_t key) noexcept {
        if (key == empty_key_) {
            return 0;
        }
        const std::size_t mask = buckets_.size() - 1;
        std::size_t target_idx = hasher_(key) & mask;

        // first loop to locate the key
        for (;;) {
            if (buckets_[target_idx].key == key) {
                break;
            }
            if (buckets_[target_idx].key == empty_key_) {
                return 0;
            }
            target_idx = get_next_idx(target_idx, mask);
        }

        std::size_t next_idx = get_next_idx(target_idx, mask);

        // shift delete if needed
        for (;;) {
            if (buckets_[next_idx].key == empty_key_) {
                buckets_[target_idx] = {empty_key_, nullptr};
                --size_;
                return 1;
            }
            std::size_t ideal = hasher_(buckets_[next_idx].key) & mask;
            if (diff(target_idx, ideal, mask) < diff(next_idx, ideal, mask)) {
                buckets_[target_idx] = buckets_[next_idx];
                target_idx = next_idx;
            }
            next_idx = get_next_idx(next_idx, mask);
        }
    }

    void reserve(std::size_t n) { rehash(n / max_load_); }
};
