#include "../client/FixClient.h"

#include <algorithm>
#include <array>
#include <bitset>
#include <charconv>
#include <cstdint>
#include <immintrin.h>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {
    constexpr char kFixDelim = '\x01';
    constexpr size_t kFixTrackedTagCount = 500;

    struct FixMsg {
        std::array<std::string_view, kFixTrackedTagCount> fields{};
        std::bitset<kFixTrackedTagCount> present{};
        char delim{kFixDelim};
    };

    bool parse_fix_scalar(std::string_view msg, FixMsg& out) {
        out.present.reset();
        char delim = kFixDelim;

        if (msg.find(kFixDelim) == std::string_view::npos && msg.find('|') != std::string_view::npos) {
            delim = '|';
        }
        out.delim = delim;

        size_t pos = 0;
        while (pos < msg.size()) {
            const size_t eq = msg.find('=', pos);
            if (eq == std::string_view::npos) {
                return false;
            }

            const std::string_view tag_view = msg.substr(pos, eq - pos);
            int tag = 0;
            auto [ptr, ec] = std::from_chars(tag_view.data(), tag_view.data() + tag_view.size(), tag);
            if (ec != std::errc{} || ptr != tag_view.data() + tag_view.size()) {
                return false;
            }

            const size_t value_start = eq + 1;
            size_t value_end = msg.find(delim, value_start);
            if (value_end == std::string_view::npos) {
                value_end = msg.size();
            }

            if (tag >= 0 && static_cast<size_t>(tag) < kFixTrackedTagCount) {
                out.fields[static_cast<size_t>(tag)] = msg.substr(value_start, value_end - value_start);
                out.present.set(static_cast<size_t>(tag));
            }
            pos = value_end + 1;
        }
        return true;
    }

    bool parse_fix_simd(std::string_view msg, FixMsg& out) {
        out.present.reset();
        char delim = kFixDelim;

        if (msg.find(kFixDelim) == std::string_view::npos && msg.find('|') != std::string_view::npos) {
            delim = '|';
        }
        out.delim = delim;

        const __m256i needle_delim = _mm256_set1_epi8(delim);
        const __m256i needle_eq = _mm256_set1_epi8('=');
        const char* base = msg.data();
        const char* p = base;
        const char* end = base + msg.size();
        const size_t npos = std::string_view::npos;

        size_t field_start = 0;
        size_t eq_pos = npos;

        auto emit_field = [&](size_t field_end) -> bool {
            if (eq_pos == npos || eq_pos < field_start || eq_pos >= field_end) {
                return false;
            }

            const std::string_view tag_view(base + field_start, eq_pos - field_start);
            uint64_t tag = 0;
            auto [ptr, ec] = std::from_chars(tag_view.data(), tag_view.data() + tag_view.size(), tag);
            if (ec != std::errc{} || ptr != tag_view.data() + tag_view.size()) {
                return false;
            }

            const std::string_view value_view(base + eq_pos + 1, field_end - (eq_pos + 1));
            if (tag < kFixTrackedTagCount) {
                out.fields[static_cast<size_t>(tag)] = value_view;
                out.present.set(static_cast<size_t>(tag));
            }
            field_start = field_end + 1;
            eq_pos = npos;
            return true;
        };

        while (p + 32 <= end) {
            const __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            const __m256i dv = _mm256_cmpeq_epi8(chunk, needle_delim);
            const __m256i eqv = _mm256_cmpeq_epi8(chunk, needle_eq);

            const uint32_t d_mask = _mm256_movemask_epi8(dv);
            const uint32_t eq_mask = _mm256_movemask_epi8(eqv);
            uint32_t combined = eq_mask | d_mask;

            while (combined) {
                const uint32_t bit = combined & (~combined + 1);
                const int idx = __builtin_ctz(combined);
                const size_t pos = static_cast<size_t>(p - base + idx);
                if (d_mask & bit) {
                    if (!emit_field(pos)) {
                        return false;
                    }
                } else if (eq_pos == npos) {
                    eq_pos = pos;
                }
                combined ^= bit;
            }
            p += 32;
        }

        for (; p < end; ++p) {
            if (*p == '=' && eq_pos == npos) {
                eq_pos = static_cast<size_t>(p - base);
            } else if (*p == delim) {
                if (!emit_field(static_cast<size_t>(p - base))) {
                    return false;
                }
            }
        }

        if (field_start < msg.size()) {
            if (!emit_field(msg.size())) {
                return false;
            }
        } else if (eq_pos != npos) {
            return false;
        }
        return true;
    }

    bool same_fix_msg(const FixMsg& lhs, const FixMsg& rhs) {
        if (lhs.delim != rhs.delim || lhs.present != rhs.present) {
            return false;
        }
        for (size_t tag = 0; tag < kFixTrackedTagCount; ++tag) {
            if (!lhs.present.test(tag)) {
                continue;
            }
            if (lhs.fields[tag] != rhs.fields[tag]) {
                return false;
            }
        }
        return true;
    }

    std::vector<std::string> generate_order_client_style_messages(size_t count) {
        jolt::client::FixClient fix{};
        fix.set_session("CLIENT_1", "ENTRY_GATEWAY");
        fix.set_account("CLIENT_1");

        std::vector<std::string> messages;
        messages.reserve(count);
        std::vector<std::string> active_orders;
        active_orders.reserve(count);

        for (size_t i = 0; i < count; ++i) {
            const std::string symbol = std::to_string((i % 4) + 1);
            const bool is_buy = (i & 1U) == 0U;
            const uint64_t qty = 1 + (i % 100);
            const uint64_t px = 60'000 + (i % 2'000);
            const uint64_t stop_px = px + 5;
            const int tif = (i % 3 == 0) ? 3 : 1;
            const size_t op = i % 5;

            std::string_view built{};
            if (op == 0 || active_orders.empty()) {
                const std::string cl = fix.next_cl_ord_id();
                built = fix.build_new_order_limit(cl, symbol, is_buy, qty, px, tif);
                active_orders.push_back(cl);
            } else if (op == 1) {
                const std::string cl = fix.next_cl_ord_id();
                built = fix.build_new_order_stop(cl, symbol, is_buy, qty, stop_px, tif);
                active_orders.push_back(cl);
            } else if (op == 2) {
                const std::string cl = fix.next_cl_ord_id();
                built = fix.build_new_order_stop_limit(cl, symbol, is_buy, qty, stop_px, px, tif);
                active_orders.push_back(cl);
            } else if (op == 3) {
                const size_t idx = i % active_orders.size();
                const std::string orig = active_orders[idx];
                const std::string cl = fix.next_cl_ord_id();
                built = fix.build_replace(cl, orig, symbol, is_buy, qty + 1, px + 1, tif);
                active_orders[idx] = cl;
            } else {
                const size_t idx = i % active_orders.size();
                const std::string orig = active_orders[idx];
                const std::string cl = fix.next_cl_ord_id();
                built = fix.build_cancel(cl, orig, symbol, is_buy);
                active_orders.erase(active_orders.begin() + static_cast<std::ptrdiff_t>(idx));
            }

            messages.emplace_back(built);
        }
        return messages;
    }
}

int main() {
    constexpr size_t kMessageCount = 500;
    const std::vector<std::string> messages = generate_order_client_style_messages(kMessageCount);

    FixMsg scalar{};
    FixMsg simd{};

    for (size_t i = 0; i < messages.size(); ++i) {
        const std::string_view msg = messages[i];
        if (!parse_fix_scalar(msg, scalar)) {
            std::cerr << "scalar parse failed at index " << i << "\n";
            return 1;
        }
        if (!parse_fix_simd(msg, simd)) {
            std::cerr << "simd parse failed at index " << i << "\n";
            return 1;
        }
        if (!same_fix_msg(scalar, simd)) {
            std::cerr << "parser mismatch at index " << i << "\n";
            return 1;
        }
    }

    std::cout << "OK: compared scalar vs SIMD parse on " << messages.size() << " messages\n";
    return 0;
}
