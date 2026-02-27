//
// Created by djaiswal on 1/21/26.
//

#include "EventLoop.h"

#include <cerrno>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "Client.h"
#include "FixGateway.h"

namespace jolt::gateway {
    namespace {
        constexpr uint64_t kListenId = 1ull << 63;
        constexpr uint64_t kWakeupId = (1ull << 63) - 1;
    }

    EventLoop::EventLoop(int listen_fd) {
        listen_fd_ = listen_fd;
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            throw std::runtime_error("epoll_create1() failed");
        }

        wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd_ < 0) {
            throw std::runtime_error("eventfd() failed");
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = kListenId;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
            throw std::runtime_error("epoll_ctl() failed for listen socket");
        }

        ev.events = EPOLLIN;
        ev.data.u64 = kWakeupId;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) < 0) {
            throw std::runtime_error("epoll_ctl() failed for wake fd");
        }

        events_.resize(8192);
        active_sessions_.resize(1);
    }

    void EventLoop::set_gateway(FixGateway* gateway) {
        gateway_ = gateway;
    }

    void EventLoop::accept_sessions() {
        for (;;) {
            sockaddr_in addr{};
            socklen_t len = sizeof(addr);
            const int session_fd = accept4(
                listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (session_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                break;
            }

            int one = 1;
            if (::setsockopt(session_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
                std::cerr << "[event_loop] failed to set TCP_NODELAY on session fd=" << session_fd << "\n";
            }

            int buf = 4 * 1024 * 1024;
            if (::setsockopt(session_fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf)) != 0) {
                std::cerr << "[event_loop] failed to set SO_RCVBUF on session fd=" << session_fd << "\n";
            }

            if (::setsockopt(session_fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf)) != 0) {
                std::cerr << "[event_loop] failed to set SO_SNDBUF on session fd=" << session_fd << "\n";
            }

            const uint32_t id = ++session_id_assign_;
            if (id > std::numeric_limits<uint32_t>::max()) {
                ::close(session_fd);
                continue;
            }

            auto session = std::make_unique<FixSession>("0", "0", session_fd);
            session.get()->gateway_ = gateway_;
            session.get()->session_id_ = id;

            if (id >= active_sessions_.size()) {
                active_sessions_.resize(id + 1);
            }
            active_sessions_[id] = std::move(session);

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
            ev.data.u64 = id;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, session_fd, &ev) < 0) {
                std::cerr << "[event_loop] failed adding accepted session to epoll id=" << id
                          << " fd=" << session_fd << " errno=" << errno << "\n";
                ::close(session_fd);
                active_sessions_[id] = nullptr;
            }
        }
    }

    bool EventLoop::enqueue_outbound(const FixMessage& msg) {
        const uint64_t id = msg.session_id;
        if (id >= active_sessions_.size()) {
            return false;
        }

        auto* session = active_sessions_[id].get();
        if (!session || session->closed_) {
            return false;
        }

        const int fd = session->fd_;
        session->queue_message({msg.data.data(), msg.len});
        if (!update_interest(session, fd, id, true)) {
            std::cerr << "[event_loop] failed enabling EPOLLOUT for session id=" << id
                      << " fd=" << fd << "\n";
            session->close();
            remove_session(id, fd);
            return false;
        }

        return true;
    }

    void EventLoop::notify() {
        if (wake_fd_ < 0) {
            return;
        }
        const uint64_t one = 1;
        const ssize_t wrote = ::write(wake_fd_, &one, sizeof(one));
        if (wrote < 0 && errno != EAGAIN) {
            std::cerr << "[event_loop] wake write failed errno=" << errno << "\n";
        }
    }

    void EventLoop::poll_once(int timeout_ms) {
        const int n = epoll_wait(epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);
        if (n <= 0) {
            return;
        }

        for (int i = 0; i < n; ++i) {
            const uint64_t id = events_[i].data.u64;
            const uint32_t mask = events_[i].events;

            if (id == kListenId) {
                accept_sessions();
                continue;
            }

            if (id == kWakeupId) {
                uint64_t value = 0;
                while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
                }
                gateway_->drain_exchange_events();
                continue;
            }

            if (id >= active_sessions_.size()) {
                std::cerr << "[event_loop] session id out of range: " << id << "\n";
                continue;
            }

            auto* session = active_sessions_[id].get();
            if (!session) {
                continue;
            }

            const int fd = session->fd_;

            if (mask & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                session->close();
                remove_session(id, fd);
                continue;
            }

            if (mask & EPOLLIN) {
                session->on_readable();
            }

            session = (id < active_sessions_.size()) ? active_sessions_[id].get() : nullptr;
            if (!session || session->closed_) {
                remove_session(id, fd);
                continue;
            }

            if (mask & EPOLLOUT) {
                session->on_writable();
            }

            session = (id < active_sessions_.size()) ? active_sessions_[id].get() : nullptr;
            if (!session || session->closed_) {
                remove_session(id, fd);
                continue;
            }

            if (!update_interest(session, fd, id, session->want_write())) {
                session->close();
                remove_session(id, fd);
            }
        }
    }

    bool EventLoop::update_interest(FixSession* session, int fd, uint64_t id, bool want_write) {
        if (!session) {
            return false;
        }

        if (session->write_interest_enabled_ == want_write) {
            return true;
        }

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
        if (want_write) {
            ev.events |= EPOLLOUT;
        }

        ev.data.u64 = id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
            std::cerr << "[event_loop] epoll_ctl MOD failed for session id=" << id
                      << " fd=" << fd << " errno=" << errno << "\n";
            return false;
        }
        session->write_interest_enabled_ = want_write;
        return true;
    }

    void EventLoop::remove_session(uint64_t id, int fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        if (id < active_sessions_.size()) {
            active_sessions_[id] = nullptr;
        }
    }

    void EventLoop::start() {
        running_ = true;
        run_thread = std::thread(&EventLoop::run, this);
    }

    void EventLoop::run() {
        while (running_) {
            poll_once(-1);
        }
    }

    void EventLoop::stop() {
        running_ = false;
        notify();
        if (run_thread.joinable()) {
            run_thread.join();
        }
    }

    size_t EventLoop::connection_count() const {
        size_t count = 0;
        for (const auto& session : active_sessions_) {
            if (session) {
                ++count;
            }
        }
        return count;
    }

    EventLoop::~EventLoop() {
        stop();
        if (wake_fd_ >= 0) {
            ::close(wake_fd_);
            wake_fd_ = -1;
        }
        if (epoll_fd_ >= 0) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
    }
}
