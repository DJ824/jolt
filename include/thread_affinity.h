#pragma once

#include <thread>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <cstring>
#include <iostream>
#endif

namespace jolt::threading {

#if defined(__linux__)
    inline bool pin_pthread_to_cpu(const pthread_t thread, const int cpu_id, const char* const label) {
        if (cpu_id < 0) {
            return false;
        }

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_id, &cpuset);

        const int rc = pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
        if (rc != 0) {
            std::cerr << "[affinity] failed to pin " << (label ? label : "thread")
                      << " to cpu=" << cpu_id << " rc=" << rc
                      << " err=" << std::strerror(rc) << "\n";
            return false;
        }
        return true;
    }
#else
    inline bool pin_pthread_to_cpu(std::thread::native_handle_type, const int, const char* const) {
        return false;
    }
#endif

    inline bool pin_current_thread_to_cpu(const int cpu_id, const char* const label) {
#if defined(__linux__)
        return pin_pthread_to_cpu(pthread_self(), cpu_id, label);
#else
        (void)cpu_id;
        (void)label;
        return false;
#endif
    }

    inline bool pin_std_thread_to_cpu(std::thread& thread, const int cpu_id, const char* const label) {
        if (!thread.joinable()) {
            return false;
        }
#if defined(__linux__)
        return pin_pthread_to_cpu(thread.native_handle(), cpu_id, label);
#else
        (void)cpu_id;
        (void)label;
        return false;
#endif
    }

}
