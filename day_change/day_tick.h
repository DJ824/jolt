#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <mutex>
#include <thread>
#include <xmmintrin.h>

class DayChanger {
public:

    std::atomic<int64_t> day_seq{0};
    std::atomic<int64_t> day_id{0};
    std::thread run_thread{};
    std::thread work_thread{};
    int64_t curr_day{0};

    void configure_market_timezone() {
        static std::once_flag once;
        std::call_once(once, []() {
            ::setenv("TZ", "America/New_York", 1);
            ::tzset();
        });
    }

    int64_t local_day_id(std::time_t t) {
        configure_market_timezone();
        std::tm local_tm{};
        localtime_r(&t, &local_tm);
        local_tm.tm_hour = 0;
        local_tm.tm_min = 0;
        local_tm.tm_sec = 0;
        local_tm.tm_isdst = -1;
        const std::time_t midnight = mktime(&local_tm);
        return static_cast<int64_t>(midnight);
    }

    void sleep_until_realtime(const timespec& ts_abs) {
        while (true) {
            int rc = clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &ts_abs, nullptr);
            if (rc == 0) {
                return;
            }
            if (rc == EINTR) {
                continue;
            }
            return;
        }
    }

    timespec next_local_midnight_realtime(std::time_t now) {
        configure_market_timezone();
        std::tm local_tm{};
        localtime_r(&now, &local_tm);
        local_tm.tm_hour = 0;
        local_tm.tm_min = 0;
        local_tm.tm_sec = 0;
        local_tm.tm_mday += 1;
        local_tm.tm_isdst = -1;
        const std::time_t next_day = mktime(&local_tm);
        timespec ts{};
        ts.tv_sec = next_day;
        ts.tv_nsec = 0;
        return ts;
    }

    void day_watcher() {
        std::time_t now = std::time(nullptr);
        day_id.store(local_day_id(now), std::memory_order_release);
        day_seq.store(1, std::memory_order_release);

        while (true) {
            timespec ts_abs = next_local_midnight_realtime(std::time(nullptr));
            sleep_until_realtime(ts_abs);

            std::time_t t2 = std::time(nullptr);
            int64_t new_id = local_day_id(t2);

            int64_t old_id = day_id.load(std::memory_order_acquire);
            if (new_id != old_id) {
                day_id.store(new_id, std::memory_order_release);
                day_seq.fetch_add(1, std::memory_order_acq_rel);
            }
        }
    }

    void run() {
        run_thread = std::thread(&DayChanger::day_watcher, this);
        work_thread = std::thread(&DayChanger::check, this);
    }

    void check() {
        curr_day = local_day_id(std::time(nullptr));
        while (true) {
            if (curr_day != day_id.load(std::memory_order_acquire)) {
                // update
            }

            _mm_pause();
        }
    }


};
