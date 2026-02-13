#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/epoll.h>

#include "FixControlSession.h"

namespace jolt::md {
    class MarketDataGateway;

    class ControlEventLoop {
    public:
        explicit ControlEventLoop(int listen_fd);
        ~ControlEventLoop();

        ControlEventLoop(const ControlEventLoop&) = delete;
        ControlEventLoop& operator=(const ControlEventLoop&) = delete;
        ControlEventLoop(ControlEventLoop&&) = delete;
        ControlEventLoop& operator=(ControlEventLoop&&) = delete;

        void set_gateway(MarketDataGateway* gateway);
        void poll_once(int timeout_ms);
        void start();
        void stop();
        void run();
        void remove_session(uint64_t id, int fd);
        FixControlSession* lookup(uint64_t id);

    private:
        void accept_sessions();
        void update_interest(int fd, uint64_t id, bool want_write);

        std::thread run_thread_;
        std::atomic<bool> running_{false};
        int epoll_fd_{-1};
        int listen_fd_{-1};
        MarketDataGateway* gateway_{nullptr};
        std::unordered_map<uint64_t, std::unique_ptr<FixControlSession>> sessions_;
        std::vector<epoll_event> events_{};
        uint64_t session_id_assign_{0};
    };
}
