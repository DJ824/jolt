//
// Created by djaiswal on 2/11/26.
//

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace jolt::exchange {
    class DayTicker {
    public:
        DayTicker();
        ~DayTicker();

        DayTicker(const DayTicker&) = delete;
        DayTicker& operator=(const DayTicker&) = delete;
        DayTicker(DayTicker&&) = delete;
        DayTicker& operator=(DayTicker&&) = delete;

        void start();
        void stop();

        uint64_t day_id() const;
        const std::atomic<uint64_t>& day_id_atomic() const;

        static uint64_t current_day_id_utc();

    private:
        static std::chrono::system_clock::time_point next_utc_midnight(uint64_t day_id);
        void run();

        std::atomic<uint64_t> day_id_{0};
        std::atomic<bool> running_{false};
        std::thread thread_{};
        mutable std::mutex mutex_{};
        std::condition_variable cv_{};
    };
}
