#pragma once

#include "spsc.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

class AsyncLogger {
public:
    enum class Level { Info, Warn, Error };

    AsyncLogger() = default;
    ~AsyncLogger() { stop(); }

    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    void start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) {
            return;
        }
        worker_ = std::thread([this] { run(); });
    }

    void stop() {
        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false)) {
            return;
        }
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void log(Level level, std::string_view msg) {
        ensure_running();
        Record rec;
        rec.level = level;
        rec.len = static_cast<uint16_t>(std::min<std::size_t>(msg.size(), sizeof(rec.msg) - 1));
        std::memcpy(rec.msg, msg.data(), rec.len);
        rec.msg[rec.len] = '\0';
        (void)queue_.enqueue(std::move(rec));
    }

    void info(std::string_view msg) { log(Level::Info, msg); }
    void warn(std::string_view msg) { log(Level::Warn, msg); }
    void error(std::string_view msg) { log(Level::Error, msg); }

    static AsyncLogger& instance() {
        static AsyncLogger logger;
        logger.ensure_running();
        return logger;
    }

private:
    struct Record {
        Level level;
        uint16_t len{0};
        char msg[256]{};
    };

    static const char* level_tag(Level level) {
        switch (level) {
            case Level::Info: return "info";
            case Level::Warn: return "warn";
            case Level::Error: return "error";
        }
        return "info";
    }

    static std::ostream& level_stream(Level level) {
        if (level == Level::Error) {
            return std::cerr;
        }
        return std::cout;
    }

    static void write_line(Level level, const char* msg, std::size_t len) {
        auto& os = level_stream(level);
        os << '[' << level_tag(level) << "] ";
        os.write(msg, static_cast<std::streamsize>(len));
        os.put('\n');
        os.flush();
    }

    void ensure_running() {
        if (!running_.load(std::memory_order_acquire)) {
            start();
        }
    }

    void run() {
        while (running_.load(std::memory_order_relaxed)) {
            if (auto rec = queue_.dequeue()) {
                write_line(rec->level, rec->msg, rec->len);
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }
        while (auto rec = queue_.dequeue()) {
            write_line(rec->level, rec->msg, rec->len);
        }
    }

    LockFreeQueue<Record, 1 << 20> queue_;
    std::thread worker_;
    std::atomic<bool> running_{false};
};

#define log_info(...) ((void)0)
#define log_warn(...) ((void)0)
#define log_error(...) ((void)0)
// inline void log_info(std::string_view msg) { AsyncLogger::instance().info(msg); }
// inline void log_warn(std::string_view msg) { AsyncLogger::instance().warn(msg); }
// inline void log_error(std::string_view msg) { AsyncLogger::instance().error(msg); }
