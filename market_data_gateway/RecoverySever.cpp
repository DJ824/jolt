//
// Created by djaiswal on 2/8/26.
//

#include "RecoverySever.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace jolt::md {
    namespace {
        constexpr uint64_t kListenId = 1ull << 63;

        int make_listen_socket(const std::string& host, uint16_t port) {
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_PASSIVE;

            addrinfo* res = nullptr;
            const std::string port_str = std::to_string(port);
            const char* host_ptr = host.empty() ? nullptr : host.c_str();
            if (::getaddrinfo(host_ptr, port_str.c_str(), &hints, &res) != 0) {
                return -1;
            }

            int fd = -1;
            for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
                fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
                if (fd < 0) {
                    continue;
                }

                int yes = 1;
                ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

                if (::bind(fd, p->ai_addr, p->ai_addrlen) == 0) {
                    if (::listen(fd, 128) == 0) {
                        int flags = fcntl(fd, F_GETFL, 0);
                        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
                        break;
                    }
                }

                ::close(fd);
                fd = -1;
            }

            ::freeaddrinfo(res);
            return fd;
        }
    }

    RecoverySever::RecoverySever(const std::string& host, uint16_t port, const std::string& blob_name,
                                 const std::string& meta_name, const std::string& request_name)
        : snapshot_pool_(blob_name, BlobMode::Attach), snapshot_meta_q_(meta_name, SharedRingMode::Attach),
          snapshot_request_q_(request_name, SharedRingMode::Create),
          listen_host_(host),
          listen_port_(port) {
        listen_fd_ = make_listen_socket(host, port);
        if (listen_fd_ < 0) {
            throw std::runtime_error("failed to bind recovery server listen socket");
        }

        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            throw std::runtime_error("epoll_create1() failed");
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = kListenId;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
            throw std::runtime_error("epoll_ctl(ADD listen) failed");
        }

        events_.resize(4096);
    }

    RecoverySever::~RecoverySever() {
        stop();
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (epoll_fd_ >= 0) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }
    }

    void RecoverySever::accept_sessions() {
        for (;;) {
            sockaddr_in6 addr{};
            socklen_t len = sizeof(addr);
            const int session_fd = accept4(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len,
                                           SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (session_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                break;
            }

            const uint64_t id = ++session_id_assign_;
            auto session = std::make_unique<DataSession>(session_fd, id);

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
            ev.data.u64 = id;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, session_fd, &ev) < 0) {
                ::close(session_fd);
                throw std::runtime_error("epoll_ctl(ADD session) failed");
            }

            sessions_.emplace(id, std::move(session));
        }
    }

    void RecoverySever::update_interest(const int fd, const uint64_t id, const bool want_write) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
        if (want_write) {
            ev.events |= EPOLLOUT;
        }
        ev.data.u64 = id;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
            throw std::runtime_error("epoll_ctl(MOD) failed");
        }
    }

    RecoverySever::DataSession* RecoverySever::lookup(const uint64_t id) {
        auto it = sessions_.find(id);
        if (it == sessions_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    void RecoverySever::handle_snapshot_request(uint64_t request_id, uint64_t session_id, uint64_t symbol_id) {
        SnapshotRequest req{};
        req.symbol_id = symbol_id;
        req.request_id = request_id;
        req.session_id = session_id;
        snapshot_request_q_.enqueue(req);
    }

    void RecoverySever::handle_retransmission_request(uint64_t request_id, uint64_t session_id, uint64_t symbol_id, uint64_t start_seq, uint64_t end_seq) {
        RetransmissionRequest req{};
        req.symbol_id = symbol_id;
        req.request_id = request_id;
        req.session_id = session_id;
        req.start_seq = start_seq;
        req.end_seq = end_seq;

    }

    void RecoverySever::handle_snapshot_response() {
        auto front = snapshot_meta_q_.dequeue();
        if (front) {
            auto meta = *front;
            auto session = sessions_[meta.session_id].get();
            DataSession::TxItem hdr{};
            memcpy(hdr.payload.data(), &meta, sizeof(SnapshotMeta));
            hdr.kind = DataSession::TxItem::Kind::Header;
            hdr.payload.resize(sizeof(SnapshotMeta));
            hdr.offset = 0;

            DataSession::TxItem item{};
            item.bytes = meta.bytes;
            item.slot_idx = meta.slot_id;
            item.kind = DataSession::TxItem::Kind::Snapshot;
            item.offset = 0;

            session->tx_buf_.push_back(hdr);
            session->tx_buf_.push_back(item);
        }
    }

    bool RecoverySever::send_pending(DataSession& session) {
        while (!session.tx_buf_.empty()) {
            auto& frame = session.tx_buf_.front();
            if (frame.kind == DataSession::TxItem::Kind::Snapshot) {
                snapshot_pool_.mark_reading(frame.slot_idx);
                auto& slot = snapshot_pool_.reader_slot(frame.slot_idx);
                while (frame.offset < frame.bytes) {
                    int n = ::write(session.fd_, slot.payload.data() + frame.offset, frame.bytes - frame.offset);
                    if (n > 0) {
                        frame.offset += n;
                        continue;
                    }
                    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        return true;
                    }
                    return false;
                }
                snapshot_pool_.release(frame.slot_idx);
                session.tx_buf_.pop_front();
            }
            else {
                const size_t remaining = frame.payload.size() - frame.offset;
                const ssize_t n = ::write(session.fd_, frame.payload.data() + frame.offset, remaining);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        return true;
                    }
                    return false;
                }
                if (n == 0) {
                    return false;
                }

                if (static_cast<size_t>(n) == remaining) {
                    session.tx_buf_.pop_front();
                }
                else {
                    frame.offset += static_cast<size_t>(n);
                    return true;
                }
            }
        }

        return true;
    }

    void RecoverySever::close_session(const uint64_t id, DataSession& session) {
        if (session.closed_) {
            return;
        }
        session.closed_ = true;
        const int fd = session.fd_;
        session.fd_ = -1;
        if (fd >= 0) {
            ::close(fd);
        }
        remove_session(id, fd);
    }

    void RecoverySever::recv_pending(DataSession& session) {
        for (;;) {
            if (session.rx_len_ >= kRxCap) {
                break;
            }
            const ssize_t n = ::recv(session.fd_, session.rx_buf_.data() + session.rx_len_, kRxCap - session.rx_len_,
                                     0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                close_session(session.session_id_, session);
                return;
            }
            if (n == 0) {
                close_session(session.session_id_, session);
                return;
            }
            session.rx_len_ += static_cast<size_t>(n);
        }
    }

    void RecoverySever::on_readable(DataSession& session) {
        recv_pending(session);
        if (!session.closed_) {
            handle_read(session);
        }
    }

    void RecoverySever::handle_read(DataSession& session) {
        (void)session;
    }

    void RecoverySever::poll_once(const int timeout_ms) {
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

            auto* session = lookup(id);
            if (!session) {
                continue;
            }

            if (mask & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                close_session(id, *session);
                continue;
            }

            if (mask & EPOLLIN) {
                on_readable(*session);
            }

            if (mask & EPOLLOUT) {
                if (!send_pending(*session)) {
                    close_session(id, *session);
                    continue;
                }
            }

            if (!session->closed_) {
                update_interest(session->fd_, id, !session->tx_buf_.empty());
            }
        }
    }

    void RecoverySever::start() {
        if (running_.exchange(true)) {
            return;
        }
        run_thread_ = std::thread(&RecoverySever::run, this);
    }

    void RecoverySever::run() {
        while (running_.load(std::memory_order_acquire)) {
            poll_once(0);
        }
    }

    void RecoverySever::stop() {
        if (!running_.exchange(false)) {
            return;
        }
        if (run_thread_.joinable()) {
            run_thread_.join();
        }
    }

    // bool RecoverySever::queue_message(const uint64_t session_id, const std::string_view payload) {
    //     auto* session = lookup(session_id);
    //     if (!session || session->closed_) {
    //         return false;
    //     }
    //
    //     std::vector<char> msg(payload.size());
    //     if (!payload.empty()) {
    //         std::memcpy(msg.data(), payload.data(), payload.size());
    //     }
    //     session->tx_buf_.push_back(std::move(msg));
    //     update_interest(session->fd_, session_id, true);
    //     return true;
    // }

    void RecoverySever::remove_session(const uint64_t id, const int fd) {
        if (fd >= 0) {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
        sessions_.erase(id);
    }

    size_t RecoverySever::connection_count() const {
        return sessions_.size();
    }
}
