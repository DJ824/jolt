//
// Created by djaiswal on 1/21/26.
//

#include "EventLoop.h"
#include "EventLoop.h"
#include <cerrno>
#include <iostream>
#include <limits>

#include <netinet/in.h>
#include <sys/socket.h>

#include "Client.h"
#include "FixGateway.h"

namespace jolt::gateway {

    static constexpr uint64_t listen_id = 1ull << 63;

    EventLoop::EventLoop(int listen_fd) {
        listen_fd_ = listen_fd;
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            throw std::runtime_error("epoll_create1() failed");
        }

        epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.u64 = listen_id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
            throw std::runtime_error("epoll_ctl() failed");
        }
        events_.resize(8192);
        active_sessions_.resize(1);
    }

    void EventLoop::set_gateway(FixGateway* gateway) {
        gateway_ = gateway;
    }

    void EventLoop::accept_sessions() {
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

            epoll_event ev;
            ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;

            const uint32_t id = ++session_id_assign_;
            if (id > std::numeric_limits<uint32_t>::max()) {
                ::close(session_fd);
                continue;
            }

            auto session = std::make_unique<FixSession>("0","0", session_fd);
            session.get()->gateway_ = gateway_;
            session.get()->session_id_ = id;
            // sessions_.emplace(session_id_assign_, std::move(session));
            if (id >= active_sessions_.size()) {
                active_sessions_.resize(id + 1);
            }
            active_sessions_[id] = std::move(session);

            ev.data.u64 = id;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, session_fd, &ev) < 0) {
                throw std::runtime_error("epoll_ctl() failed");
            }

        }
    }

    void EventLoop::poll_once(int timeout_ms) {
        int ct = 0;
        while (auto msg = gateway_->outbound_.dequeue()) {

            auto id = msg->session_id;
            // auto session = lookup(id);
            if (id >= active_sessions_.size()) {
                std::cerr << "[event_loop] outbound session id out of range: " << id << "\n";
                continue;
            }
            auto session = active_sessions_[id].get();
            if (!session) {
                std::cerr << "[event_loop] session for outbound msg with id " << id << " disconnected\n";
                continue;
            }

            session->queue_message({msg->data.data(), msg->len});

            auto fd = session->fd_;
            epoll_event ev;
            ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLOUT;
            ev.data.u64 = id;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
                throw std::runtime_error("epoll_ctl() failed");
            }


            ++ct;
            if (ct > 1024) {
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

            // auto* session = lookup(id);

            if (id >= active_sessions_.size()) {
                std::cerr << "[event_loop] session id out of range: " << id << "\n";
                continue;
            }
            auto session = active_sessions_[id].get();
            if (!session) {
                std::cerr << "[event_loop] session for id " << id << " disconnected\n";
                continue;
            }

            auto fd = session->fd_;

            if (mask & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
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

            update_interest(fd, id, session->want_write());
        }
    }

    void EventLoop::update_interest(int fd, uint64_t id, bool want_write) {
        epoll_event ev;
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
        if (want_write) {
            ev.events |= EPOLLOUT;
        }

        ev.data.u64 = id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
            throw std::runtime_error("epoll_ctl() failed");
        }


    }

    void EventLoop::remove_session(uint64_t id, int fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, 0);
        sessions_.erase(id);
        if (id < active_sessions_.size()) {
            active_sessions_[id] = nullptr;
        }

    }

    FixSession* EventLoop::lookup(uint64_t id) {
        auto it = sessions_.find(id);
        if (it == sessions_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    void EventLoop::start() {
        running_ = true;
        run_thread = std::thread(&EventLoop::run, this);

    }

    void EventLoop::run() {
        while (running_) {
            poll_once(0);
        }
    }

    void EventLoop::stop() {
        running_ = false;
        run_thread.join();
    }

    size_t EventLoop::connection_count() const {
        return sessions_.size();
    }

    EventLoop::~EventLoop() {}
}
