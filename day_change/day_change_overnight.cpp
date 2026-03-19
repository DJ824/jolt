#include "day_tick.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {
    int64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    std::string utc_time_string(const std::time_t t) {
        std::tm tm{};
        gmtime_r(&t, &tm);
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S UTC");
        return oss.str();
    }

    struct ReaderStats {
        std::atomic<int64_t> seen_ns{0};
        std::atomic<int64_t> seen_day{0};
        std::atomic<int64_t> seen_seq{0};
    };
}

int main() {
    DayChanger changer;
    std::thread watcher(&DayChanger::day_watcher, &changer);
    watcher.detach();

    while (true) {
        if (changer.day_seq.load(std::memory_order_acquire) > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    const int64_t start_day = changer.day_id.load(std::memory_order_acquire);
    const int64_t start_seq = changer.day_seq.load(std::memory_order_acquire);
    const std::time_t now = std::time(nullptr);
    const timespec next_midnight = changer.next_local_midnight_realtime(now);

    std::cout << "start_time=" << utc_time_string(now)
              << " start_day_id=" << start_day
              << " start_day_seq=" << start_seq
              << " next_market_midnight=" << utc_time_string(next_midnight.tv_sec)
              << '\n';

    constexpr size_t kReaders = 5;
    std::array<ReaderStats, kReaders> stats{};
    std::vector<std::thread> readers;
    readers.reserve(kReaders);

    for (size_t i = 0; i < kReaders; ++i) {
        readers.emplace_back([&, i]() {
            while (true) {
                const int64_t day = changer.day_id.load(std::memory_order_acquire);
                if (day != start_day) {
                    stats[i].seen_day.store(day, std::memory_order_release);
                    stats[i].seen_seq.store(changer.day_seq.load(std::memory_order_acquire),
                                            std::memory_order_release);
                    int64_t expected = 0;
                    const int64_t ts = now_ns();
                    (void)stats[i].seen_ns.compare_exchange_strong(expected, ts, std::memory_order_acq_rel);
                    break;
                }
                _mm_pause();
            }
        });
    }

    for (auto& t: readers) {
        t.join();
    }

    const int64_t first_seen_ns = stats[0].seen_ns.load(std::memory_order_acquire);
    const int64_t new_day = stats[0].seen_day.load(std::memory_order_acquire);
    const int64_t new_seq = stats[0].seen_seq.load(std::memory_order_acquire);

    std::cout << "day_change_observed new_day_id=" << new_day
              << " new_day_seq=" << new_seq << '\n';

    for (size_t i = 0; i < kReaders; ++i) {
        const int64_t seen_ns = stats[i].seen_ns.load(std::memory_order_acquire);
        const int64_t delta_us = (seen_ns - first_seen_ns) / 1000;
        const auto seen_tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(seen_ns));
        const std::time_t seen_t = std::chrono::system_clock::to_time_t(seen_tp);
        std::cout << "reader=" << i
                  << " seen_at=" << utc_time_string(seen_t)
                  << " delta_from_first_us=" << delta_us
                  << " day_seq=" << stats[i].seen_seq.load(std::memory_order_acquire)
                  << '\n';
    }

    return 0;
}
