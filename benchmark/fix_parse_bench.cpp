//
// Created by djaiswal on 1/27/26.
//

#include <charconv>
#include <cstdio>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <time.h>
#include <x86intrin.h>

namespace {
    constexpr char kFixDelim = '\x01';
}

struct FixMsg {
    std::vector<std::pair<uint64_t, std::string_view>> fields_;
    char delim_{kFixDelim};
};

static double cycles_to_ns(uint64_t delta) noexcept {
    static double factor = [] {
        long khz = 0;

        // find out how many cycles in 100ms, then convert to ns/cycle
        uint64_t c0 = __rdtsc();
        struct timespec ts{0, 100000000};
        nanosleep(&ts, nullptr);
        uint64_t c1 = __rdtsc();
        khz = long((c1 - c0) / 100);
        return 1e6 / double(khz);
    }();

    return delta * factor;
}


static inline bool parse_fix(std::string_view msg, FixMsg& out) {
    out.fields_.clear();
    char delim = kFixDelim;

    if (msg.find(kFixDelim) == std::string_view::npos && msg.find('|') != std::string_view::npos) {
        delim = '|';
    }

    out.delim_ = delim;

    size_t pos = 0;
    while (pos < msg.size()) {
        size_t eq = msg.find('=', pos);
        if (eq == std::string_view::npos) {
            return false;
        }

        std::string_view tag_view = msg.substr(pos, eq - pos);
        uint64_t tag = 0;
        auto [ptr, ec] = std::from_chars(tag_view.data(), tag_view.data() + tag_view.size(), tag);
        if (ec != std::errc{} || ptr != tag_view.data() + tag_view.size()) {
            return false;
        }

        size_t value_start = eq + 1;
        size_t value_end = msg.find(delim, value_start);
        if (value_end == std::string_view::npos) {
            value_end = msg.size();
        }

        out.fields_.emplace_back(tag, msg.substr(value_start, value_end - value_start));
        pos = value_end + 1;
    }
    return true;
}

static void append_field(std::string& msg, std::string_view tag, std::string_view value) {
    msg.append(tag);
    msg.push_back('=');
    msg.append(value);
    msg.push_back(kFixDelim);
}

static void append_field_num(std::string& msg, std::string_view tag, uint64_t value) {
    char buf[32];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    if (ec != std::errc{}) {
        return;
    }
    msg.append(tag);
    msg.push_back('=');
    msg.append(buf, static_cast<size_t>(ptr - buf));
    msg.push_back(kFixDelim);
}

static std::string make_id(std::mt19937_64& rng, std::string_view prefix) {
    std::uniform_int_distribution<uint64_t> dist(1, 1'000'000);
    uint64_t id = dist(rng);
    std::string out(prefix);
    char buf[32];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), id);
    if (ec == std::errc{}) {
        out.append(buf, static_cast<size_t>(ptr - buf));
    }
    return out;
}

static std::string build_fix_message(char msg_type, std::mt19937_64& rng) {
    std::uniform_int_distribution<uint64_t> qty_dist(1, 10'000);
    std::uniform_int_distribution<uint64_t> price_dist(1'000, 50'000);
    std::uniform_int_distribution<uint64_t> seq_dist(1, 1'000'000);
    std::uniform_int_distribution<uint64_t> side_dist(1, 2);
    std::uniform_int_distribution<uint64_t> ord_type_dist(1, 3);

    std::string body;
    body.reserve(192);
    append_field(body, "35", std::string_view(&msg_type, 1));
    append_field(body, "49", "CLIENT1");
    append_field(body, "56", "GATEWAY");
    append_field_num(body, "34", seq_dist(rng));
    append_field(body, "52", "20250127-12:00:00.000");

    std::string cl_ord_id = make_id(rng, "CL");
    std::string symbol = make_id(rng, "SYM");
    append_field(body, "11", cl_ord_id);
    append_field(body, "55", symbol);
    append_field_num(body, "54", side_dist(rng));

    if (msg_type == 'D') {
        append_field_num(body, "38", qty_dist(rng));
        append_field_num(body, "40", ord_type_dist(rng));
        append_field_num(body, "44", price_dist(rng));
        append_field(body, "59", "1");
    }
    else if (msg_type == 'F') {
        std::string orig_id = make_id(rng, "ORIG");
        append_field(body, "41", orig_id);
    }
    else if (msg_type == 'G') {
        std::string orig_id = make_id(rng, "ORIG");
        append_field(body, "41", orig_id);
        append_field_num(body, "38", qty_dist(rng));
        append_field_num(body, "44", price_dist(rng));
    }

    std::string msg;
    msg.reserve(body.size() + 64);
    append_field(msg, "8", "FIX.4.4");
    append_field_num(msg, "9", body.size());
    msg.append(body);

    uint32_t checksum = 0;
    for (unsigned char c : msg) {
        checksum += c;
    }
    checksum %= 256;

    char chk[4];
    std::snprintf(chk, sizeof(chk), "%03u", checksum);
    append_field(msg, "10", std::string_view(chk, 3));
    return msg;
}

// 8=FIX.4.4|9=112|35=D|49=CLIENT1|56=GATEWAY|34=1|52=20250127-12:00:00.000|11=CLORD123|55=TEST|54=1|38=100|40=2|44=12345|59=1|10=000|

static bool parse_fix_simd(std::string_view msg, FixMsg& out) {
    out.fields_.clear();
    char delim = kFixDelim;

    if (msg.find(kFixDelim) == std::string_view::npos && msg.find('|') != std::string_view::npos) {
        delim = '|';
    }

    out.delim_ = delim;

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

        std::string_view tag_view(base + field_start, eq_pos - field_start);
        uint64_t tag = 0;
        auto [ptr, ec] = std::from_chars(tag_view.data(), tag_view.data() + tag_view.size(), tag);
        if (ec != std::errc{} || ptr != tag_view.data() + tag_view.size()) {
            return false;
        }

        std::string_view value_view(base + eq_pos + 1, field_end - (eq_pos + 1));
        out.fields_.emplace_back(tag, value_view);
        field_start = field_end + 1;
        eq_pos = npos;
        return true;
    };

    while (p + 32 <= end) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
        __m256i dv = _mm256_cmpeq_epi8(chunk, needle_delim);
        __m256i eqv = _mm256_cmpeq_epi8(chunk, needle_eq);

        uint32_t d_mask = _mm256_movemask_epi8(dv);
        uint32_t eq_mask = _mm256_movemask_epi8(eqv);
        uint32_t combined = eq_mask | d_mask;

        while (combined) {
            uint32_t bit = combined & (~combined + 1);
            int idx = __builtin_ctz(combined);
            size_t pos = p - base + idx;
            if (d_mask & bit) {
                if (!emit_field(pos)) {
                    return false;
                }
            }
            else if (eq_pos == npos) {
                eq_pos = pos;
            }
            combined ^= bit;
        }
        p += 32;
    }

    for (; p < end; ++p) {
        if (*p == '=' && eq_pos == npos) {
            eq_pos = static_cast<size_t>(p - base);
        }
        else if (*p == delim) {
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

static bool same_fields(const FixMsg& lhs, const FixMsg& rhs) {
    if (lhs.delim_ != rhs.delim_ || lhs.fields_.size() != rhs.fields_.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.fields_.size(); ++i) {
        if (lhs.fields_[i].first != rhs.fields_[i].first ||
            lhs.fields_[i].second != rhs.fields_[i].second) {
            return false;
        }
    }
    return true;
}

int main() {
    FixMsg out{};
    FixMsg scalar_out{};
    FixMsg simd_out{};

    const std::string fix_test =
        "8=FIX.4.4\x01"
        "9=112\x01"
        "35=D\x01"
        "49=CLIENT1\x01"
        "56=GATEWAY\x01"
        "34=1\x01"
        "52=20250127-12:00:00.000\x01"
        "11=CLORD123\x01"
        "55=TEST\x01"
        "54=1\x01"
        "38=100\x01"
        "40=2\x01"
        "44=12345\x01"
        "59=1\x01"
        "10=000\x01";
    if (!parse_fix(fix_test, scalar_out) || !parse_fix_simd(fix_test, simd_out)) {
        std::cerr << "parse failed on fixed test message\n";
        return 1;
    }
    if (!same_fields(scalar_out, simd_out)) {
        std::cerr << "simd parse mismatch on fixed test message\n";
        return 1;
    }
    auto find_tag = [&](uint64_t tag) -> std::string_view {
        for (const auto& [t, v] : simd_out.fields_) {
            if (t == tag) {
                return v;
            }
        }
        return {};
    };
    if (find_tag(35) != "D" || find_tag(11) != "CLORD123" || find_tag(38) != "100" || find_tag(55) != "TEST") {
        std::cerr << "simd parse failed: unexpected tag values\n";
        return 1;
    }

    std::mt19937_64 rng(42);
    std::vector<std::string> fixes;
    fixes.reserve(3000);
    for (size_t i = 0; i < 1000; ++i) {
        fixes.emplace_back(build_fix_message('D', rng));
        fixes.emplace_back(build_fix_message('F', rng));
        fixes.emplace_back(build_fix_message('G', rng));
    }

    for (size_t i = 0; i < fixes.size(); ++i) {
        if (!parse_fix(fixes[i], scalar_out)) {
            std::cerr << "scalar parse failed at idx=" << i << "\n";
            return 1;
        }
        if (!parse_fix_simd(fixes[i], simd_out)) {
            std::cerr << "simd parse failed at idx=" << i << "\n";
            return 1;
        }
        if (!same_fields(scalar_out, simd_out)) {
            std::cerr << "scalar/simd mismatch at idx=" << i << "\n";
            return 1;
        }
    }

    const std::vector<std::string> malformed = {
        std::string("8=FIX.4.4") + kFixDelim + "9=12" + kFixDelim + "35D" + kFixDelim,
        std::string("8=FIX.4.4") + kFixDelim + "9=12" + kFixDelim + "X=1" + kFixDelim,
        std::string("8=FIX.4.4") + kFixDelim + "9=12" + kFixDelim + "=bad" + kFixDelim,
    };
    for (const auto& msg : malformed) {
        if (parse_fix_simd(msg, simd_out)) {
            std::cerr << "simd parser accepted malformed message\n";
            return 1;
        }
    }

    constexpr size_t kWarmup = 1000;
    constexpr size_t kIters = 1000000;

    for (size_t i = 0; i < kWarmup; ++i) {
        if (!parse_fix(fixes[i % fixes.size()], out)) {
            std::cerr << "scalar warmup parse failed\n";
            return 1;
        }
    }

    for (size_t i = 0; i < kWarmup; ++i) {
        if (!parse_fix_simd(fixes[i % fixes.size()], out)) {
            std::cerr << "simd warmup parse failed\n";
            return 1;
        }
    }

    if (out.fields_.empty()) {
        std::cerr << "warmup parse failed: no fields parsed\n";
        return 1;
    }

    bool has_msg_type = false;
    for (const auto& [tag, value] : out.fields_) {
        if (tag == 35 && (value == "D" || value == "F" || value == "G")) {
            has_msg_type = true;
            break;
        }
    }
    if (!has_msg_type) {
        std::cerr << "warmup parse failed: missing MsgType=35\n";
        return 1;
    }

    uint64_t sink = 0;
    const uint64_t start_scalar = __rdtsc();
    for (size_t i = 0; i < kIters; ++i) {
        if (!parse_fix(fixes[i % fixes.size()], out)) {
            std::cerr << "scalar parse failed in benchmark loop\n";
            return 1;
        }
        sink += out.fields_.size();
    }
    const uint64_t end_scalar = __rdtsc();

    const uint64_t scalar_cycles = end_scalar - start_scalar;
    const double scalar_total_ns = cycles_to_ns(scalar_cycles);
    const double scalar_ns_per_msg = scalar_total_ns / static_cast<double>(kIters);

    const uint64_t start_simd = __rdtsc();
    for (size_t i = 0; i < kIters; ++i) {
        if (!parse_fix_simd(fixes[i % fixes.size()], out)) {
            std::cerr << "simd parse failed in benchmark loop\n";
            return 1;
        }
        sink += out.fields_.size();
    }
    const uint64_t end_simd = __rdtsc();

    const uint64_t simd_cycles = end_simd - start_simd;
    const double simd_total_ns = cycles_to_ns(simd_cycles);
    const double simd_ns_per_msg = simd_total_ns / static_cast<double>(kIters);

    std::cout << "iters=" << kIters
        << " scalar_ns_per_msg=" << scalar_ns_per_msg
        << " simd_ns_per_msg=" << simd_ns_per_msg
        << "\n";
    return 0;
}
