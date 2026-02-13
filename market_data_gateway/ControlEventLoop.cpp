//
// Created by djaiswal on 2/3/26.
//

#include "ControlEventLoop.h"
#include "MarketDataGateway.h"

#include <cerrno>
#include <stdexcept>
#include <netinet/in.h>
#include <sys/socket.h>

namespace jolt::md {
    static constexpr uint64_t listen_id = 1ull << 63;

    ControlEventLoop::ControlEventLoop(int listen_fd) : listen_fd_(listen_fd) {
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            throw std::runtime_error("epoll_create1() failed");
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = listen_id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
            throw std::runtime_error("epoll_ctl() failed");
        }

        events_.resize(4096);
    }

    ControlEventLoop::~ControlEventLoop() = default;

    void ControlEventLoop::set_gateway(MarketDataGateway* gateway) {
        gateway_ = gateway;
    }

    void ControlEventLoop::accept_sessions() {
        for (;;) {
            sockaddr_in6 addr{};
            socklen_t len = sizeof(addr);
            int session_fd = accept4(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (session_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                break;
            }

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;

            auto session = std::make_unique<FixControlSession>("0", "0", session_fd);
            session->gateway_ = gateway_;
            session->session_id_ = ++session_id_assign_;
            sessions_.emplace(session->session_id_, std::move(session));

            ev.data.u64 = session_id_assign_;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, session_fd, &ev) < 0) {
                throw std::runtime_error("epoll_ctl() failed");
            }
        }
    }

    void ControlEventLoop::poll_once(int timeout_ms) {
        int drained = 0;
        while (auto msg = gateway_->outbound_.dequeue()) {
            auto* session = lookup(msg->session_id);
            if (!session) {
                continue;
            }
            session->queue_message({msg->data.data(), msg->len});
            update_interest(session->fd_, session->session_id_, true);

            ++drained;
            if (drained > 1024) {
                break;
            }
        }

        const int n = epoll_wait(epoll_fd_, events_.data(), events_.size(), timeout_ms);
        if (n <= 0) {
            return;
        }

        for (int i = 0; i < n; ++i) {
            const uint64_t id = events_[i].data.u64;
            const uint32_t mask = events_[i].events;

            if (id == listen_id) {
                accept_sessions();
                continue;
            }

            auto* session = lookup(id);
            if (!session) {
                continue;
            }

            if (mask & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                int fd = session->fd_;
                session->close();
                remove_session(id, fd);
                continue;
            }

            if (mask & EPOLLIN) {
                session->on_readable();
            }

            if (mask & EPOLLOUT) {
                session->on_writable();
            }

            update_interest(session->fd_, id, session->want_write());
        }
    }

    void ControlEventLoop::update_interest(int fd, uint64_t id, bool want_write) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
        if (want_write) {
            ev.events |= EPOLLOUT;
        }

        ev.data.u64 = id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
            throw std::runtime_error("epoll_ctl() failed");
        }
    }

    void ControlEventLoop::remove_session(uint64_t id, int fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        sessions_.erase(id);
    }

    FixControlSession* ControlEventLoop::lookup(uint64_t id) {
        auto it = sessions_.find(id);
        if (it == sessions_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    void ControlEventLoop::start() {
        running_ = true;
        run_thread_ = std::thread(&ControlEventLoop::run, this);
    }

    void ControlEventLoop::run() {
        while (running_) {
            poll_once(0);
        }
    }

    void ControlEventLoop::stop() {
        running_ = false;
        if (run_thread_.joinable()) {
            run_thread_.join();
        }
    }
}
