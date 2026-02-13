//
// Created by djaiswal on 2/3/26.
//

#include "FixControlSession.h"
#include "MarketDataGateway.h"

#include <charconv>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <unistd.h>
#include <sys/socket.h>

namespace jolt::md {
    FixControlSession::FixControlSession(const std::string& sender_comp_id, const std::string& target_comp_id, int fd)
        : sender_comp_id_(sender_comp_id),
          target_comp_id_(target_comp_id),
          fd_(fd) {
    }

    FixControlSession::~FixControlSession() = default;

    void FixControlSession::close() {
        if (closed_) {
            return;
        }
        ::close(fd_);
        fd_ = -1;
        closed_ = true;
        tx_buf_.clear();
        rx_len_ = 0;
        rx_off_ = 0;
        tx_off_ = 0;
        if (gateway_) {
            gateway_->on_disconnect(session_id_);
        }
    }

    void FixControlSession::recv_pending() {
        for (;;) {
            if (rx_len_ >= kRxCap) {
                break;
            }
            const ssize_t n = recv(fd_, rx_buf_.data() + rx_len_, kRxCap - rx_len_, 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                close();
                return;
            }
            if (n == 0) {
                close();
                return;
            }
            rx_len_ += static_cast<size_t>(n);
        }
    }

    void FixControlSession::on_readable() {
        recv_pending();
        std::string_view msg;
        while (extract_message(msg)) {
            FixMessage fix_msg{};
            if (msg.size() > fix_msg.data.size()) {
                close();
                return;
            }
            std::memcpy(fix_msg.data.data(), msg.data(), msg.size());
            fix_msg.len = msg.size();
            fix_msg.session_id = session_id_;
            gateway_->inbound_.enqueue(fix_msg);
        }
    }

    bool FixControlSession::send_pending() {
        while (!tx_buf_.empty()) {
            Message& m = tx_buf_.front();
            const size_t remaining = m.len - tx_off_;
            const ssize_t n = write(fd_, m.buf.data() + tx_off_, remaining);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return true;
                }
                close();
                return false;
            }
            if (n == 0) {
                close();
                return false;
            }

            if (static_cast<size_t>(n) == remaining) {
                tx_buf_.pop_front();
                tx_off_ = 0;
            } else {
                tx_off_ += static_cast<size_t>(n);
                return true;
            }
        }
        return true;
    }

    void FixControlSession::on_writable() {
        send_pending();
    }

    bool FixControlSession::want_write() const {
        return !tx_buf_.empty();
    }

    bool FixControlSession::extract_message(std::string_view& msg) {
        std::string_view view(rx_buf_.data() + rx_off_, rx_len_ - rx_off_);
        if (view.size() < 2) {
            return false;
        }

        if (view[0] != '8' || view[1] != '=') {
            size_t pos = view.find("8=");
            if (pos == std::string_view::npos) {
                rx_off_ = rx_len_ = 0;
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        const char* base = rx_buf_.data() + rx_off_;
        const char* end = rx_buf_.data() + rx_len_;
        const char* soh = static_cast<const char*>(memchr(base, '\x01', end - base));
        if (!soh) {
            return false;
        }

        const char* body_len_start = soh + 1;
        if (body_len_start + 2 > end || body_len_start[0] != '9' || body_len_start[1] != '=') {
            size_t rel = static_cast<size_t>(soh - base + 1);
            size_t pos = view.find("8=", rel);
            if (pos == std::string_view::npos) {
                rx_off_ = rx_len_ = 0;
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        const char* body_len_end = static_cast<const char*>(
            memchr(body_len_start + 2, '\x01', end - (body_len_start + 2)));
        if (!body_len_end) {
            return false;
        }

        size_t body_len = 0;
        auto [ptr, ec] = std::from_chars(body_len_start + 2, body_len_end, body_len);
        if (ec != std::errc{} || ptr != body_len_end) {
            rx_off_ = rx_len_ = 0;
            return false;
        }

        size_t body_start = static_cast<size_t>(body_len_end - base) + 1;
        size_t body_end = body_start + body_len;

        if (body_end + 7 > static_cast<size_t>(end - base)) {
            return false;
        }

        if (base[body_end] != '1' || base[body_end + 1] != '0' || base[body_end + 2] != '=') {
            size_t pos = view.find("8=", 1);
            if (pos == std::string_view::npos) {
                rx_off_ = rx_len_ = 0;
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        size_t trailer_end = body_end + 6;
        if (base[trailer_end] != '\x01') {
            size_t pos = view.find("8=", 1);
            if (pos == std::string_view::npos) {
                rx_off_ = rx_len_ = 0;
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        msg = std::string_view(base, trailer_end + 1);
        rx_off_ += trailer_end + 1;

        if (rx_off_ == rx_len_) {
            rx_off_ = rx_len_ = 0;
        } else if (rx_off_ > kRxCap / 2) {
            size_t rem = rx_len_ - rx_off_;
            memmove(rx_buf_.data(), rx_buf_.data() + rx_off_, rem);
            rx_len_ = rem;
            rx_off_ = 0;
        }
        return true;
    }

    void FixControlSession::queue_message(std::string_view msg) {
        if (msg.size() > kTxCap) {
            throw std::runtime_error("msg too big for client tx");
        }
        Message m{};
        m.len = msg.size();
        std::memcpy(m.buf.data(), msg.data(), m.len);
        tx_buf_.push_back(m);
    }
}
