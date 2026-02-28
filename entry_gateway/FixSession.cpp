//
// Created by djaiswal on 1/23/26.
//

#include "FixSession.h"
#include "FixGateway.h"
#include "../include/async_logger.h"

#include <stdexcept>
#include <sys/uio.h>
#include <unistd.h>
#include <sys/socket.h>

namespace jolt::gateway {
    namespace {
        constexpr char kFixDelim = '\x01';

        bool parse_checksum_3(std::string_view digits, uint32_t& out) {
            if (digits.size() != 3) {
                return false;
            }
            uint32_t value = 0;
            for (char ch : digits) {
                if (ch < '0' || ch > '9') {
                    return false;
                }
                value = value * 10 + static_cast<uint32_t>(ch - '0');
            }
            out = value;
            return true;
        }

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
        if (!closed_.exchange(true, std::memory_order_acq_rel)) {
            ::close(fd_);
            fd_ = -1;
            write_interest_enabled_ = false;
            tx_batch_head_ = 0;
            tx_batch_size_ = 0;
            tx_armed_.store(false, std::memory_order_release);
            rx_len_ = 0;
            rx_off_ = 0;
            tx_off_ = 0;
            Message dropped{};
            while (tx_queue_.try_dequeue(dropped)) {
            }
            if (gateway_) {
                gateway_->on_disconnect(session_id_);
            }
        }
    }

    void FixSession::recv_pending() {
        if (rx_off_ >= rx_len_) {
            rx_off_ = rx_len_ = 0;
        } else if (rx_off_ > 0 && (rx_len_ == kRxCap || rx_off_ > kRxCap / 2)) {
            const size_t rem = rx_len_ - rx_off_;
            memmove(rx_buf_.data(), rx_buf_.data() + rx_off_, rem);
            rx_len_ = rem;
            rx_off_ = 0;
        }

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
            if (!gateway_ || !gateway_->on_fix_message(msg, session_id_)) {
                log_error("[gtwy] gateway failed handling inbound FIX from client session=" +
                          std::to_string(session_id_));
            }
        }
    }



    bool FixSession::send_pending() {
        while (true) {
            while (tx_batch_size_ < kTxWritevBatch) {
                Message next{};
                if (!tx_queue_.try_dequeue(next)) {
                    break;
                }
                tx_batch_[(tx_batch_head_ + tx_batch_size_) % kTxWritevBatch] = std::move(next);
                ++tx_batch_size_;
            }

            if (tx_batch_size_ == 0) {
                return true;
            }

            iovec iov[kTxWritevBatch]{};
            int iovcnt = 0;
            for (size_t index = 0; index < tx_batch_size_ && iovcnt < static_cast<int>(kTxWritevBatch); ++index) {
                const Message& m = tx_batch_[(tx_batch_head_ + index) % kTxWritevBatch];
                const size_t start = (index == 0) ? tx_off_ : 0;
                if (start >= m.len) {
                    continue;
                }
                iov[iovcnt].iov_base = const_cast<char*>(m.buf.data() + start);
                iov[iovcnt].iov_len = m.len - start;
                ++iovcnt;
            }

            if (iovcnt == 0) {
                tx_batch_head_ = 0;
                tx_batch_size_ = 0;
                tx_off_ = 0;
                return true;
            }

            const ssize_t n = ::writev(fd_, iov, iovcnt);
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

            size_t consumed = static_cast<size_t>(n);
            while (consumed > 0 && tx_batch_size_ != 0) {
                Message& front = tx_batch_[tx_batch_head_];
                const size_t remaining = front.len - tx_off_;
                if (consumed >= remaining) {
                    consumed -= remaining;
                    tx_batch_head_ = (tx_batch_head_ + 1) % kTxWritevBatch;
                    --tx_batch_size_;
                    tx_off_ = 0;
                }
                else {
                    tx_off_ += consumed;
                    consumed = 0;
                }
            }
        }
        return true;
    }


    void FixSession::on_writable() {
        send_pending();
    }

    bool FixSession::want_write() {
        return tx_batch_size_ != 0 || !tx_queue_.empty();
    }

    bool FixSession::extract_message(std::string_view& msg) {
        std::string_view view(rx_buf_.data() + rx_off_, rx_len_ - rx_off_);
        auto preserve_partial_fix_prefix = [this, &view]() {
            // Keep trailing '8' so a split "8=" across TCP packets can be recovered.
            if (!view.empty() && view.back() == '8') {
                rx_buf_[0] = '8';
                rx_off_ = 0;
                rx_len_ = 1;
                return;
            }
            rx_off_ = rx_len_ = 0;
        };
        if (view.size() < 2) {
            return false;
        }

        if (view[0] != '8' || view[1] != '=') {
            size_t pos = view.find("8=");
            if (pos == std::string::npos) {
                preserve_partial_fix_prefix();
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
        if (body_len_start + 2 > end) {
            // Partial header, wait for more bytes.
            return false;
        }
        if (body_len_start[0] != '9' || body_len_start[1] != '=') {
            size_t rel = static_cast<size_t>(soh - base + 1);
            size_t pos = view.find("8=", rel);
            if (pos == std::string::npos) {
                preserve_partial_fix_prefix();
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
            preserve_partial_fix_prefix();
            return false;
        }

        size_t body_start = (body_len_end - base) + 1;
        size_t body_end = body_start + body_len;

        if (body_end + 7 > static_cast<size_t>(end - base)) {
            return false;
        }

        if (base[body_end] != '1' || base[body_end + 1] != '0' || base[body_end + 2] != '=') {
            size_t pos = view.find("8=", 1);
            if (pos == std::string::npos) {
                preserve_partial_fix_prefix();
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        size_t trailer_end = body_end + 6;
        if (base[trailer_end] != '\x01') {
            size_t pos = view.find("8=", 1);
            if (pos == std::string::npos) {
                preserve_partial_fix_prefix();
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        const std::string_view checksum_digits(base + body_end + 3, 3);
        uint32_t advertised_checksum = 0;
        if (!parse_checksum_3(checksum_digits, advertised_checksum)) {
            size_t pos = view.find("8=", 1);
            if (pos == std::string::npos) {
                preserve_partial_fix_prefix();
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        uint32_t computed_checksum = 0;
        for (size_t i = 0; i < body_end; ++i) {
            computed_checksum += static_cast<unsigned char>(base[i]);
        }
        computed_checksum %= 256;
        if (computed_checksum != advertised_checksum) {
            size_t pos = view.find("8=", 1);
            if (pos == std::string::npos) {
                preserve_partial_fix_prefix();
                return false;
            }
            rx_off_ += pos;
            return false;
        }

        msg = std::string_view(base, trailer_end + 1);
        rx_off_ += trailer_end + 1;

        if (rx_off_ == rx_len_) {
            rx_off_ = rx_len_ = 0;
        }
        return true;
    }

    bool FixSession::queue_message(std::string_view msg) {
        if (closed_.load(std::memory_order_acquire)) {
            return false;
        }
        if (msg.size() > kTxCap) {
            throw std::runtime_error("msg too big for client tx");
        }
        Message m{};
        m.len = msg.size();
        memcpy(m.buf.data(), msg.data(), m.len);
        return tx_queue_.enqueue(std::move(m));
    }
}
