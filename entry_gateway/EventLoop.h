#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include <sys/epoll.h>
#include "Client.h"
#include "FixSession.h"

namespace jolt::gateway {
    class FixGateway;

    class EventLoop {
    public:
        explicit EventLoop(int listen_fd);
        ~EventLoop();

        EventLoop(const EventLoop&) = delete;
        EventLoop& operator=(const EventLoop&) = delete;
        EventLoop(EventLoop&&) = delete;
        EventLoop& operator=(EventLoop&&) = delete;

        void set_gateway(FixGateway* gateway);
        void remove_session(uint64_t id, int fd);
        void poll_once(int timeout_ms);
        void run();
        void stop();
        void start();
        size_t connection_count() const;
        uint64_t session_id_assign_{0};
        FixSession* lookup(uint64_t id);

    private:
        std::thread run_thread;
        void accept_sessions();
        void update_interest(int fd, uint64_t id, bool want_write);
        std::atomic<bool> running_{false};
        int epoll_fd_{-1};
        int listen_fd_{-1};
        FixGateway* gateway_{nullptr};
        std::unordered_map<uint64_t, std::unique_ptr<FixSession>> sessions_;
        std::vector<std::unique_ptr<FixSession>> active_sessions_;
        std::vector<epoll_event> events_{};
    };
}
