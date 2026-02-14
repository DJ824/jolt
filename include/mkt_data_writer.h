#pragma once

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <liburing.h>
#include "Types.h"

class L3DataWriter {
    static constexpr size_t kDepth = 64;
    static constexpr size_t kMaxRecordsPerBatch = 1 << 20;
    static constexpr size_t kNumSymbols = jolt::kNumSymbols;

    std::string root_;
    std::string day_;
    std::array<int, kNumSymbols> symbol_fds_{};

    io_uring ring_{};
    bool ring_ready_{false};

    std::array<std::vector<jolt::ob::L3Data>, kDepth> slot_bufs_{};
    std::array<bool, kDepth> slot_in_flight_{};
    std::array<size_t, kDepth> slot_expected_bytes_{};
    size_t next_slot_{0};
    size_t in_flight_{0};


    struct DayHeader {
        uint64_t symbol_id;
        uint64_t day_id;
        uint64_t max_seq;
        uint64_t bytes;
    };

    static std::string today_yyyymmdd_utc() {
        using namespace std::chrono;
        const auto now = system_clock::now();
        const auto tt = system_clock::to_time_t(now);
        std::tm tm{};
        gmtime_r(&tt, &tm);

        char buf[9];
        const int wrote = std::snprintf(buf, sizeof(buf), "%04d%02d%02d",
                                        tm.tm_year + 1900,
                                        tm.tm_mon + 1,
                                        tm.tm_mday);
        if (wrote != 8) {
            throw std::runtime_error("failed to format UTC day");
        }
        return std::string(buf, 8);
    }

    static std::runtime_error make_errno_error(const char* what, int err) {
        const int code = (err < 0) ? -err : err;
        return std::runtime_error(std::string(what) + ": " + std::strerror(code));
    }

    void close_all_fds() noexcept {
        for (int& fd : symbol_fds_) {
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }
    }

    void ensure_open(uint16_t symbol_id) {
        if (!jolt::is_valid_symbol_id(symbol_id)) {
            throw std::runtime_error("invalid symbol id");
        }
        const size_t symbol_idx = static_cast<size_t>(symbol_id - jolt::kFirstSymbolId);

        const std::string day = today_yyyymmdd_utc();
        if (day != day_) {
            close_all_fds();
            day_ = day;
        }

        int& fd = symbol_fds_[symbol_idx];
        if (fd >= 0) {
            return;
        }

        std::filesystem::create_directories(root_ + "/" + day_);
        const std::string path = root_ + "/" + day_ + "/sym_" + std::to_string(symbol_id) + ".l3bin";

        fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND | O_CLOEXEC, 0644);
        if (fd < 0) {
            throw make_errno_error("open failed", errno);
        }
    }

    void complete_slot_from_cqe(io_uring_cqe* cqe) {
        const size_t slot = io_uring_cqe_get_data64(cqe);
        const int res = cqe->res;
        io_uring_cqe_seen(&ring_, cqe);

        if (slot >= kDepth || !slot_in_flight_[slot]) {
            throw std::runtime_error("invalid completion slot");
        }

        const size_t expected = slot_expected_bytes_[slot];
        slot_in_flight_[slot] = false;
        slot_expected_bytes_[slot] = 0;
        if (in_flight_ > 0) {
            --in_flight_;
        }

        if (res < 0) {
            throw make_errno_error("async write failed", res);
        }
        if (static_cast<size_t>(res) != expected) {
            throw std::runtime_error("short async write");
        }
    }

public:
    explicit L3DataWriter(std::string root) : root_(std::move(root)) {
        symbol_fds_.fill(-1);
        slot_in_flight_.fill(false);
        slot_expected_bytes_.fill(0);

        const int rc = io_uring_queue_init(kDepth, &ring_, 0);
        if (rc < 0) {
            throw make_errno_error("io_uring_queue_init failed", rc);
        }
        ring_ready_ = true;

        for (size_t i = 0; i < symbol_fds_.size(); ++i) {
            ensure_open(static_cast<uint16_t>(jolt::kFirstSymbolId + i));
        }

        for (auto& b : slot_bufs_) {
            b.reserve(kMaxRecordsPerBatch);
        }
    }

    ~L3DataWriter() noexcept {
        if (ring_ready_) {
            (void)io_uring_submit(&ring_);
            while (in_flight_ > 0) {
                reap_one_blocking();
            }
            io_uring_queue_exit(&ring_);
        }


        close_all_fds();
    }

    size_t acquire_free_slot_blocking() {
        for (;;) {
            if (in_flight_ < kDepth) {
                for (size_t i = 0; i < kDepth; ++i) {
                    const size_t slot = (next_slot_ + i) % kDepth;
                    if (!slot_in_flight_[slot]) {
                        next_slot_ = (slot + 1) % kDepth;
                        return slot;
                    }
                }
            }

            const int rc = io_uring_submit(&ring_);
            if (rc < 0) {
                throw make_errno_error("io_uring_submit failed", rc);
            }
            reap_one_blocking();
        }
    }

    void write_batch(uint16_t symbol_id, const jolt::ob::L3Data* data, size_t n) {
        if (n == 0) {
            return;
        }

        ensure_open(symbol_id);
        const size_t symbol_idx = static_cast<size_t>(symbol_id - jolt::kFirstSymbolId);

        reap_completions();

        const size_t slot = acquire_free_slot_blocking();

        auto& buf = slot_bufs_[slot];
        buf.assign(data, data + n);
        const size_t bytes = buf.size() * sizeof(jolt::ob::L3Data);

        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            const int s = io_uring_submit(&ring_);
            if (s < 0) {
                throw make_errno_error("io_uring_submit failed", s);
            }
            reap_one_blocking();

            sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                throw std::runtime_error("io_uring_get_sqe failed");
            }
        }

        io_uring_prep_write(sqe,
                            symbol_fds_[symbol_idx],
                            buf.data(),
                            static_cast<unsigned>(bytes),
                            static_cast<__u64>(-1));

        io_uring_sqe_set_data64(sqe, slot);

        slot_expected_bytes_[slot] = bytes;
        slot_in_flight_[slot] = true;
        ++in_flight_;

        const int ret = io_uring_submit(&ring_);
        if (ret < 0) {
            slot_in_flight_[slot] = false;
            slot_expected_bytes_[slot] = 0;
            --in_flight_;
            throw make_errno_error("io_uring_submit failed", ret);
        }
    }

    void reap_one_blocking() {
        io_uring_cqe* cqe = nullptr;
        const int rc = io_uring_wait_cqe(&ring_, &cqe);
        if (rc < 0) {
            throw make_errno_error("io_uring_wait_cqe failed", rc);
        }
        complete_slot_from_cqe(cqe);
    }

    void reap_completions() {
        io_uring_cqe* cqe = nullptr;
        while (io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe) {
            complete_slot_from_cqe(cqe);
        }
    }
};
