//
// Created by djaiswal on 1/23/26.
//

#include "FixSession.h"
#include "FixGateway.h"
#include "../include/async_logger.h"

#include <stdexcept>
#include <unistd.h>
#include <sys/socket.h>

namespace jolt::gateway {
    namespace {
        constexpr char kFixDelim = '\x01';

        std::string_view find_fix_tag(std::string_view msg, std::string_view tag_with_eq) {
            size_t pos = 0;
            while (pos < msg.size()) {
                const size_t end = msg.find(kFixDelim, pos);
                const size_t field_end = (end == std::string_view::npos) ? msg.size() : end;
                const std::string_view field = msg.substr(pos, field_end - pos);
                if (field.size() > tag_with_eq.size() && field.substr(0, tag_with_eq.size()) == tag_with_eq) {
                    return field.substr(tag_with_eq.size());
                }
                if (end == std::string_view::npos) {
                    break;
                }
                pos = end + 1;
            }
            return {};
        }
    }

    FixSession::FixSession(const std::string& sender_comp_id, const std::string& target_comp_id, int fd) {
        sender_comp_id_ = sender_comp_id;
        target_comp_id_ = target_comp_id;
        rx_buf_ = {};
        fd_ = fd;
        clients_.reserve(64);
    }

    FixSession::~FixSession() {
    }

    void FixSession::add_client(uint64_t client_id, Client* client) {
        clients_[client_id] = client;
    }

    void FixSession::remove_client(uint64_t client_id) {
        clients_.erase(client_id);
    }

    void FixSession::close() {
        if (!closed_) {
            ::close(fd_);
            fd_ = -1;
            closed_ = true;
            tx_buf_.clear();
            rx_len_ = 0;
            rx_off_ = 0;
            tx_off_ = 0;


            FixMessage disconnect{};
            disconnect.len = 0;
            disconnect.session_id = session_id_;
            gateway_->inbound_.enqueue(disconnect);
        }
    }

    void FixSession::recv_pending() {
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


    void FixSession::on_readable() {
        recv_pending();
        std::string_view msg;
        while (extract_message(msg)) {
            if (msg.size() > kFixMaxMsg) {
                continue;
            }
            FixMessage fix_msg{};
            memcpy(fix_msg.data.data(), msg.data(), msg.size());
            fix_msg.len = msg.size();
            fix_msg.session_id = session_id_;
            gateway_->inbound_.enqueue(fix_msg);
        }
    }



    bool FixSession::send_pending() {
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
                const std::string_view msg_view(m.buf.data(), m.len);
                const std::string_view msg_type = find_fix_tag(msg_view, "35=");
                const std::string_view order_id = find_fix_tag(msg_view, "37=");
                std::string_view client_id = find_fix_tag(msg_view, "1=");
                if (client_id.empty()) {
                    client_id = find_fix_tag(msg_view, "49=");
                }
                log_info("[gtwy] gateway sent msg to client msg_type=" +
                         std::string(msg_type.empty() ? std::string_view{"?"} : msg_type) +
                         " order_id=" + std::string(order_id.empty() ? std::string_view{"unknown"} : order_id) +
                         " client_id=" + std::string(client_id.empty() ? std::string_view{"unknown"} : client_id) +
                         " session=" + std::to_string(session_id_));
                tx_buf_.pop_front();
                tx_off_ = 0;
            } else {
                tx_off_ += static_cast<size_t>(n);
                return true;
            }
        }
        return true;
    }


    void FixSession::on_writable() {
        send_pending();
    }

    bool FixSession::want_write() {
        return !tx_buf_.empty();
    }

    bool FixSession::extract_message(std::string_view& msg) {
        std::string_view view(rx_buf_.data() + rx_off_, rx_len_ - rx_off_);
        if (view.size() < 2) {
            return false;
        }

        if (view[0] != '8' || view[1] != '=') {
            size_t pos = view.find("8=");
            if (pos == std::string::npos) {
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
            if (pos == std::string::npos) {
                rx_off_ = rx_len_ = 0;
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        const char* body_len_end = static_cast<const char*>(memchr(body_len_start + 2, '\x01', end - (body_len_start + 2)));
        if (!body_len_end) {
            return false;
        }

        size_t body_len = 0;
        auto [ptr, ec] = std::from_chars(body_len_start + 2, body_len_end, body_len);
        if (ec != std::errc() || ptr != body_len_end) {
            rx_off_ = rx_len_ = 0;
            return false;
        }

        size_t body_start = (body_len_end - base) + 1;
        size_t body_end = body_start + body_len;

        if (body_end + 7 > static_cast<size_t>(end - base)) {
            return false;
        }

        if (base[body_end] != '1' || base[body_end + 1] != '0' || base[body_end + 2] != '=') {
            size_t pos = view.find("8=", rx_off_ + 1);
            if (pos == std::string::npos) {
                rx_off_ = rx_len_ = 0;
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        size_t trailer_end = body_end + 6;
        if (base[trailer_end] != '\x01') {
            size_t pos = view.find("8=", 1);
            if (pos == std::string::npos) {
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

    void FixSession::queue_message(std::string_view msg) {
        if (msg.size() > kTxCap) {
            throw std::runtime_error("msg too big for client tx");
        }
        Message m{};
        m.len = msg.size();
        memcpy(m.buf.data(), msg.data(), m.len);
        tx_buf_.push_back(m);
    }
}
