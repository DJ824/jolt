// Simple dynamic bitset with next-set-bit helpers
#pragma once

#include <cstdint>
#include <vector>
#include <limits>

namespace jolt::ob {

class BitsetIndex {
public:
    explicit BitsetIndex(std::size_t size_bits = 0) { reset(size_bits); }

    void reset(std::size_t size_bits) {
        size_bits_ = size_bits;
        words_.assign((size_bits + 63) / 64, 0ull);
    }

    std::size_t size() const { return size_bits_; }

    inline void set(std::size_t idx) {
        auto [w, m] = mask(idx);
        words_[w] |= m;
    }
    inline void clear(std::size_t idx) {
        auto [w, m] = mask(idx);
        words_[w] &= ~m;
    }
    inline bool test(std::size_t idx) const {
        auto [w, m] = mask(idx);
        return (words_[w] & m) != 0ull;
    }

    // Returns next set bit at or after start; or npos if none
    std::size_t next_set(std::size_t start) const {
        if (start >= size_bits_) {
            return npos;
        }
        std::size_t wi = start / 64;
        uint64_t w = words_[wi] & (~0ull << (start % 64));
        while (true) {
            if (w) {
                return wi * 64 + ctz(w);
            }
            ++wi;
            if (wi >= words_.size()) break;
            w = words_[wi];
        }
        return npos;
    }

    // Returns previous set bit at or before start; or npos if none
    std::size_t prev_set(std::size_t start) const {
        if (size_bits_ == 0) {
            return npos;
        }
        if (start >= size_bits_) start = size_bits_ - 1;
        std::size_t wi = start / 64;
        uint64_t w = words_[wi] & (~0ull >> (63 - (start % 64)));
        while (true) {
            if (w) {
                return wi * 64 + (63 - clz(w));
            }
            if (wi == 0) break;
            --wi;
            w = words_[wi];
        }
        return npos;
    }

    static constexpr std::size_t npos = std::numeric_limits<std::size_t>::max();

private:
    static uint32_t ctz(uint64_t x) {
        return static_cast<uint32_t>(__builtin_ctzll(x));
    }

    static uint32_t clz(uint64_t x) {
        return static_cast<uint32_t>(__builtin_clzll(x));
    }

    static std::pair<std::size_t,uint64_t> mask(std::size_t idx) {
        return { idx / 64, 1ull << (idx % 64) };
    }

    std::size_t size_bits_{};
    std::vector<uint64_t> words_;
};

} // namespace jolt::ob
