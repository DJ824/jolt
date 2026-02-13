//
// Created by djaiswal on 2/11/26.
//

#include "DayTicker.h"

namespace jolt::exchange {
    namespace {
        constexpr uint64_t kSecondsPerDay = 24ull * 60ull * 60ull;
    }

    DayTicker::DayTicker() : day_id_(current_day_id_utc()) {
    }

    DayTicker::~DayTicker() {
        stop();
    }

    void DayTicker::start() {
        if (running_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        day_id_.store(current_day_id_utc(), std::memory_order_release);
        thread_ = std::thread(&DayTicker::run, this);
    }

    void DayTicker::stop() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) {
            return;
        }

        cv_.notify_all();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    uint64_t DayTicker::day_id() const {
        return day_id_.load(std::memory_order_acquire);
    }

    const std::atomic<uint64_t>& DayTicker::day_id_atomic() const {
        return day_id_;
    }

    uint64_t DayTicker::current_day_id_utc() {
        const auto now = std::chrono::system_clock::now();
        const auto secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        if (secs <= 0) {
            return 0;
        }
        return static_cast<uint64_t>(secs) / kSecondsPerDay;
    }

    std::chrono::system_clock::time_point DayTicker::next_utc_midnight(const uint64_t day_id) {
        const uint64_t next_day = day_id + 1;
        const auto next_secs = static_cast<int64_t>(next_day * kSecondsPerDay);
        return std::chrono::system_clock::time_point{std::chrono::seconds{next_secs}};
    }

    void DayTicker::run() {
        uint64_t last_day = day_id_.load(std::memory_order_acquire);

        while (running_.load(std::memory_order_acquire)) {
            const auto wake_at = next_utc_midnight(last_day);

            std::unique_lock<std::mutex> lk(mutex_);
            cv_.wait_until(lk, wake_at, [this] {
                return !running_.load(std::memory_order_acquire);
            });
            lk.unlock();

            if (!running_.load(std::memory_order_acquire)) {
                break;
            }

            const uint64_t now_day = current_day_id_utc();
            if (now_day > last_day) {
                day_id_.store(now_day, std::memory_order_release);
                last_day = now_day;
            }
        }
    }
}
