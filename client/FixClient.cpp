//
// Created by djaiswal on 1/20/26.
//

#include "FixClient.h"
#include "market_data_gateway/MarketDataTypes.h"
#include "../include/async_logger.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
    constexpr char kFixDelim = '\x01';
    constexpr size_t kDroppedPayloadPreviewBytes = 256;

    std::string payload_preview_for_log(std::string_view payload) {
        if (payload.empty()) {
            return "<empty>";
        }

        const size_t preview_len = std::min(payload.size(), kDroppedPayloadPreviewBytes);
        std::string out;
        out.reserve(preview_len * 4 + 32);

        constexpr char kHexDigits[] = "0123456789ABCDEF";
        for (size_t i = 0; i < preview_len; ++i) {
            const unsigned char ch = static_cast<unsigned char>(payload[i]);
            if (ch == static_cast<unsigned char>(kFixDelim)) {
                out += "|";
                continue;
            }
            if (std::isprint(ch) && ch != '\\') {
                out.push_back(static_cast<char>(ch));
                continue;
            }

            out += "\\x";
            out.push_back(kHexDigits[ch >> 4]);
            out.push_back(kHexDigits[ch & 0x0F]);
        }

        if (preview_len < payload.size()) {
            out += "...(truncated,total_bytes=";
            out += std::to_string(payload.size());
            out += ")";
        }
        return out;
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

    std::string fix_msg_type_for_log(std::string_view msg) {
        const std::string_view msg_type = find_fix_tag(msg, "35=");
        if (msg_type.empty()) {
            return "?";
        }
        return std::string(msg_type);
    }

    std::string fix_order_id_for_log(std::string_view msg) {
        const std::string_view order_id = find_fix_tag(msg, "37=");
        if (order_id.empty()) {
            return "unknown";
        }
        return std::string(order_id);
    }

    std::string fix_cl_ord_id_for_log(std::string_view msg) {
        const std::string_view cl_ord_id = find_fix_tag(msg, "11=");
        if (cl_ord_id.empty()) {
            return "unknown";
        }
        return std::string(cl_ord_id);
    }

    std::string fix_orig_cl_ord_id_for_log(std::string_view msg) {
        const std::string_view orig_cl_ord_id = find_fix_tag(msg, "41=");
        if (orig_cl_ord_id.empty()) {
            return "-";
        }
        return std::string(orig_cl_ord_id);
    }

    std::string fix_client_id_for_log(std::string_view msg,
                                      std::string_view account,
                                      std::string_view sender_comp_id) {
        if (const std::string_view account_tag = find_fix_tag(msg, "1="); !account_tag.empty()) {
            return std::string(account_tag);
        }
        if (const std::string_view sender_tag = find_fix_tag(msg, "49="); !sender_tag.empty()) {
            return std::string(sender_tag);
        }
        if (!account.empty()) {
            return std::string(account);
        }
        if (!sender_comp_id.empty()) {
            return std::string(sender_comp_id);
        }
        return "unknown";
    }

    bool send_all(int fd, const char* data, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t n = ::send(fd, data + off, len - off, 0);
            if (n <= 0) {
                return false;
            }
            off += static_cast<size_t>(n);
        }
        return true;
    }

    bool send_blob_tcp(const std::string& host, const std::string& port, const void* data, size_t len) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        addrinfo* res = nullptr;
        if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) {
            return false;
        }

        int fd = -1;
        for (addrinfo* p = res; p; p = p->ai_next) {
            fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (fd == -1) {
                continue;
            }
            if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
                break;
            }
            ::close(fd);
            fd = -1;
        }

        ::freeaddrinfo(res);
        if (fd == -1) {
            return false;
        }

        const bool ok = send_all(fd, static_cast<const char*>(data), len);
        ::close(fd);
        return ok;
    }

    bool send_blob_udp(const std::string& host, const std::string& port, const void* data, size_t len) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;

        addrinfo* res = nullptr;
        if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) {
            return false;
        }

        bool sent = false;
        for (addrinfo* p = res; p; p = p->ai_next) {
            const int fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (fd == -1) {
                continue;
            }

            const ssize_t n = ::sendto(fd,
                                       static_cast<const char*>(data),
                                       len,
                                       0,
                                       p->ai_addr,
                                       p->ai_addrlen);
            ::close(fd);
            if (n == static_cast<ssize_t>(len)) {
                sent = true;
                break;
            }
        }

        ::freeaddrinfo(res);
        return sent;
    }
}

namespace jolt::client {
    FixClient::~FixClient() {
        disconnect();
    }


    bool FixClient::connect_tcp(const std::string& host, const std::string& port) {
        disconnect();

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        addrinfo* res = nullptr;
        if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) {
            return false;
        }

        int fd = -1;
        for (addrinfo* p = res; p; p = p->ai_next) {
            fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (fd == -1) {
                continue;
            }
            if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
                break;
            }
            ::close(fd);
            fd = -1;
        }
        ::freeaddrinfo(res);

        if (fd == -1) {
            return false;
        }

        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        fd_ = fd;
        return true;
    }

    void FixClient::disconnect() {
        if (fd_ != -1) {
            ::close(fd_);
            fd_ = -1;
        }
        recv_buf_.fill(0);
        recv_off_ = recv_len_ = 0;
        inbound_.clear();
    }

    bool FixClient::is_connected() const {
        return fd_ != -1;
    }

    bool FixClient::append_bytes(const char* p, size_t n) {
        if (msg_len_ + n > buffer_.size()) {
            std::cerr << "buffer full\n";
            return false;
        }

        memcpy(buffer_.data() + msg_len_, p, n);
        msg_len_ += n;
        return true;
    }

    bool FixClient::append_tag(int tag, std::string_view val) {
        char tmp[32];
        auto [t_end, ec1] = std::to_chars(tmp, tmp+sizeof(tmp), tag);
        if (ec1 != std::errc{}) {
            return false;
        }

        if (!append_bytes(tmp, t_end - tmp)) {
            return false;
        }

        if (!append_bytes("=", 1)) {
            return false;
        }

        if (!append_bytes(val.data(), val.size())) {
            return false;
        }

        if (!append_bytes(&kFixDelim, 1)) {
            return false;
        }

        return true;
    }

    void FixClient::set_session(const std::string& sender_comp_id, const std::string& target_comp_id) {
        sender_comp_id_ = sender_comp_id;
        target_comp_id_ = target_comp_id;
    }

    void FixClient::set_account(const std::string& account) {
        account_ = account;
    }

    std::string FixClient::next_cl_ord_id() {
        return std::to_string(cl_ord_seq_++);
    }

    bool FixClient::send_raw(std::string_view msg) {
        if (fd_ == -1) {
            log_error("[client] client send failed: socket is not connected client_id=" +
                      fix_client_id_for_log(msg, account_, sender_comp_id_) +
                      " msg_type=" + fix_msg_type_for_log(msg) +
                      " cl_ord_id=" + fix_cl_ord_id_for_log(msg) +
                      " orig_cl_ord_id=" + fix_orig_cl_ord_id_for_log(msg));
            return false;
        }
        const bool ok = send_all(fd_, msg.data(), msg.size());
        if (!ok) {
            log_error("[client] client failed sending message to gateway client_id=" +
                      fix_client_id_for_log(msg, account_, sender_comp_id_) +
                      " msg_type=" + fix_msg_type_for_log(msg) +
                      " cl_ord_id=" + fix_cl_ord_id_for_log(msg) +
                      " orig_cl_ord_id=" + fix_orig_cl_ord_id_for_log(msg) +
                      " bytes=" + std::to_string(msg.size()));
            return false;
        }

        log_info("[client] client sent message to gateway client_id=" +
                 fix_client_id_for_log(msg, account_, sender_comp_id_) +
                 " msg_type=" + fix_msg_type_for_log(msg) +
                 " cl_ord_id=" + fix_cl_ord_id_for_log(msg) +
                 " orig_cl_ord_id=" + fix_orig_cl_ord_id_for_log(msg) +
                 " bytes=" + std::to_string(msg.size()));
        return true;
    }


    bool FixClient::send_message(std::string_view msg_type, const std::vector<std::pair<int, std::string>>& fields) {
        const std::string_view msg = build_fix_message(msg_type, fields);
        return send_raw(msg);
    }

    std::string_view FixClient::build_logon(int heartbeat_seconds) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(98, "0");
        fields.emplace_back(108, std::to_string(heartbeat_seconds));
        return build_fix_message("A", fields);
    }

    std::string_view FixClient::build_logout() {
        return build_fix_message("5", {});
    }

    std::string_view FixClient::build_heartbeat() {
        return build_fix_message("0", {});
    }

    std::string_view FixClient::build_new_order_limit(std::string_view cl_ord_id,
                                                      std::string_view symbol,
                                                      bool is_buy,
                                                      uint64_t qty,
                                                      uint64_t price,
                                                      int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "2");
        fields.emplace_back(44, std::to_string(price));
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("D", fields);
    }

    std::string_view FixClient::build_new_order_market(std::string_view cl_ord_id,
                                                       std::string_view symbol,
                                                       bool is_buy,
                                                       uint64_t qty,
                                                       int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "1");
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("D", fields);
    }

    std::string_view FixClient::build_new_order_stop(std::string_view cl_ord_id,
                                                     std::string_view symbol,
                                                     bool is_buy,
                                                     uint64_t qty,
                                                     uint64_t stop_px,
                                                     int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "3");
        fields.emplace_back(99, std::to_string(stop_px));
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("D", fields);
    }

    std::string_view FixClient::build_new_order_stop_limit(std::string_view cl_ord_id,
                                                           std::string_view symbol,
                                                           bool is_buy,
                                                           uint64_t qty,
                                                           uint64_t stop_px,
                                                           uint64_t limit_px,
                                                           int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "4");
        fields.emplace_back(99, std::to_string(stop_px));
        fields.emplace_back(44, std::to_string(limit_px));
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("D", fields);
    }

    std::string_view FixClient::build_cancel(std::string_view cl_ord_id,
                                             std::string_view orig_cl_ord_id,
                                             std::string_view symbol,
                                             bool is_buy) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        fields.emplace_back(41, std::string(orig_cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        return build_fix_message("F", fields);
    }

    std::string_view FixClient::build_replace(std::string_view cl_ord_id,
                                              std::string_view orig_cl_ord_id,
                                              std::string_view symbol,
                                              bool is_buy,
                                              uint64_t qty,
                                              uint64_t price,
                                              int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        fields.emplace_back(41, std::string(orig_cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "2");
        fields.emplace_back(44, std::to_string(price));
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("G", fields);
    }

    std::string_view FixClient::build_replace_stop(std::string_view cl_ord_id,
                                                   std::string_view orig_cl_ord_id,
                                                   std::string_view symbol,
                                                   bool is_buy,
                                                   uint64_t qty,
                                                   uint64_t stop_px,
                                                   int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        fields.emplace_back(41, std::string(orig_cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "3");
        fields.emplace_back(99, std::to_string(stop_px));
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("G", fields);
    }

    std::string_view FixClient::build_replace_stop_limit(std::string_view cl_ord_id,
                                                         std::string_view orig_cl_ord_id,
                                                         std::string_view symbol,
                                                         bool is_buy,
                                                         uint64_t qty,
                                                         uint64_t stop_px,
                                                         uint64_t limit_px,
                                                         int tif) {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(11, std::string(cl_ord_id));
        fields.emplace_back(41, std::string(orig_cl_ord_id));
        if (!account_.empty()) {
            fields.emplace_back(1, account_);
        }
        fields.emplace_back(55, std::string(symbol));
        fields.emplace_back(54, is_buy ? "1" : "2");
        fields.emplace_back(38, std::to_string(qty));
        fields.emplace_back(40, "4");
        fields.emplace_back(99, std::to_string(stop_px));
        fields.emplace_back(44, std::to_string(limit_px));
        fields.emplace_back(59, std::to_string(tif));
        return build_fix_message("G", fields);
    }

    bool FixClient::build_snapshot_request(const std::string& host,
                                           const std::string& port,
                                           const uint64_t session_id,
                                           const uint64_t symbol_id,
                                           const uint64_t request_id) {
        md::SnapshotRequest request{};
        request.session_id = session_id;
        request.symbol_id = symbol_id;
        request.request_id = request_id;
        return send_blob_tcp(host, port, &request, sizeof(request));
    }

    bool FixClient::build_udp_request(const std::string& host,
                                      const std::string& port,
                                      const uint64_t session_id,
                                      const uint64_t symbol_id,
                                      const uint64_t request_id) {
        md::DataRequest request{};
        request.session_id = session_id;
        request.symbol_id = symbol_id;
        request.request_id = request_id;
        return send_blob_udp(host, port, &request, sizeof(request));
    }

    bool FixClient::poll() {
        if (!read_socket()) {
            log_error("[client] client poll failed while reading from gateway client_id=" +
                      fix_client_id_for_log({}, account_, sender_comp_id_));
            return false;
        }
        std::string msg;
        while (extract_message(msg)) {
            const std::string msg_type = fix_msg_type_for_log(msg);
            log_info("[client] client received response from gateway msg_type=" + msg_type +
                     " order_id=" + fix_order_id_for_log(msg) +
                     " client_id=" + fix_client_id_for_log(msg, account_, sender_comp_id_));
            inbound_.push_back(std::move(msg));
        }
        return true;
    }

    std::optional<std::string_view> FixClient::next_message() {
        if (inbound_.empty()) {
            return std::nullopt;
        }
        last_message_ = std::move(inbound_.front());
        inbound_.pop_front();
        return std::string_view(last_message_);
    }

    bool FixClient::read_socket() {
        if (fd_ == -1) {
            log_error("[client] client read failed: socket is not connected client_id=" +
                      fix_client_id_for_log({}, account_, sender_comp_id_));
            return false;
        }

        if (recv_off_ >= recv_len_) {
            recv_off_ = recv_len_ = 0;
        }

        if (recv_len_ == recv_buf_.size() && recv_off_ > 0) {
            const size_t remaining = recv_len_ - recv_off_;
            std::memmove(recv_buf_.data(), recv_buf_.data() + recv_off_, remaining);
            recv_len_ = remaining;
            recv_off_ = 0;
        }

        if (recv_len_ == recv_buf_.size()) {
            log_warn("[client] client receive buffer full while reading gateway response, dropping buffered bytes client_id=" +
                     fix_client_id_for_log({}, account_, sender_comp_id_));
            recv_off_ = recv_len_ = 0;
        }

        const size_t space = recv_buf_.size() - recv_len_;
        if (space == 0) {
            return true;
        }

        ssize_t n = recv(fd_, recv_buf_.data() + recv_len_, space, MSG_DONTWAIT);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            }
            log_error("[client] client recv failed from gateway client_id=" +
                      fix_client_id_for_log({}, account_, sender_comp_id_));
            return false;
        }
        if (n == 0) {
            log_warn("[client] client socket closed by gateway client_id=" +
                     fix_client_id_for_log({}, account_, sender_comp_id_));
            disconnect();
            return false;
        }

        recv_len_ += static_cast<size_t>(n);
        return true;
    }

    bool FixClient::extract_message(std::string& out) {
        out.clear();
        if (recv_off_ >= recv_len_) {
            recv_off_ = recv_len_ = 0;
            return false;
        }

        std::string_view view(recv_buf_.data() + recv_off_, recv_len_ - recv_off_);
        size_t start = view.find("8=");

        if (start == std::string::npos) {
            log_warn("[client] client dropped non-FIX payload while parsing gateway response client_id=" +
                     fix_client_id_for_log({}, account_, sender_comp_id_) +
                     " payload=\"" + payload_preview_for_log(view) + "\"");
            recv_off_ = recv_len_ = 0;
            return false;
        }

        if (start > 0) {
            recv_off_ += start;
            view.remove_prefix(start);
        }

        size_t body_len_pos = view.find("9=", 2);
        size_t body_len_end = view.find('\x01', body_len_pos);

        if (body_len_pos == std::string::npos || body_len_end == std::string::npos) {
            // Partial frame, wait for more bytes.
            return false;
        }

        size_t body_len = 0;
        auto [ptr, ec] = std::from_chars(view.data() + body_len_pos + 2, view.data() + body_len_end, body_len);
        if (ec != std::errc{}) {
            log_error("[client] client failed parsing BodyLength from gateway response client_id=" +
                      fix_client_id_for_log({}, account_, sender_comp_id_));
            recv_off_ = recv_len_ = 0;
            return false;
        }

        size_t body_start = body_len_end + 1;
        size_t body_end = body_start + body_len;
        if (view.size() < body_end + 7) {
            return false;
        }

        if (view.compare(body_end, 3, "10=") != 0) {
            log_warn("[client] client FIX frame missing checksum trailer, resyncing client_id=" +
                     fix_client_id_for_log({}, account_, sender_comp_id_));
            recv_off_ += body_end;
            return false;
        }

        size_t trailer_end = view.find(kFixDelim, body_end);
        if (trailer_end == std::string::npos) {
            return false;
        }
        out.assign(view.data(), trailer_end + 1);
        recv_off_ += trailer_end + 1;

        if (recv_off_ > 0 && recv_off_ >= recv_len_ / 2) {
            ssize_t remaining = recv_len_ - recv_off_;
            std::memmove(recv_buf_.data(), recv_buf_.data() + recv_off_, remaining);
            recv_len_ = remaining;
            recv_off_ = 0;
        }
        return true;
    }

    std::string_view FixClient::build_fix_message(std::string_view msg_type,
                                                  const std::vector<std::pair<int, std::string>>& fields) {
        msg_len_ = 0;

        auto append_tag_u64 = [&](int tag, uint64_t value) -> bool {
            char val[32];
            auto [v_end, ec] = std::to_chars(val, val + sizeof(val), value);
            if (ec != std::errc{}) {
                return false;
            }
            return append_tag(tag, std::string_view(val, v_end - val));
        };

        if (!append_tag(35, msg_type)) {
            return {};
        }

        if (!append_tag(49, sender_comp_id_)) {
            return {};
        }

        if (!append_tag(56, target_comp_id_)) {
            return {};
        }

        if (!append_tag_u64(34, next_out_seq_++)) {
            return{};
        }

        if (!append_tag(1, account_)) {
            return {};
        }

        for (const auto& [tag,val] : fields) {
            if (!append_tag(tag, val)) {
                return{};
            }
        }

        const size_t body_len = msg_len_;

        char hdr[64];
        size_t hdr_len = 0;

        auto hdr_append = [&](int tag, std::string_view value) -> bool {
            char tmp[32];
            auto [t_end, ec] = std::to_chars(tmp, tmp + sizeof(tmp), tag);
            if (ec != std::errc{}) {
                return false;
            }
            size_t tag_len = (t_end - tmp);
            if (hdr_len + tag_len + 1 + value.size() + 1 > sizeof(hdr)) {
                return false;
            }
            std::memcpy(hdr + hdr_len, tmp, tag_len);
            hdr_len += tag_len;
            hdr[hdr_len++] = '=';
            std::memcpy(hdr + hdr_len, value.data(), value.size());
            hdr_len += value.size();
            hdr[hdr_len++] = '\x01';
            return true;
        };

        char body_len_buf[32];
        auto [bl_end, bl_ec] = std::to_chars(body_len_buf, body_len_buf + sizeof(body_len_buf), body_len);
        if (bl_ec != std::errc{}) {
            return{};
        }

        if (!hdr_append(8, "FIX.4.4")) {
            return {};
        }

        if (!hdr_append(9, std::string_view(body_len_buf, bl_end - body_len_buf))) {
            return {};
        }

        if (hdr_len + body_len + 7 > buffer_.size()) {
            return {};
        }

        std::memmove(buffer_.data() + hdr_len, buffer_.data(), body_len);
        std::memcpy(buffer_.data(), hdr, hdr_len);
        msg_len_ = hdr_len + body_len;

        uint32_t crc = 0;
        for (size_t i = 0; i < msg_len_; i++) {
            crc += static_cast<unsigned char>(buffer_[i]);
        }
        crc %= 256;

        char chk[4];
        std::snprintf(chk, sizeof(chk), "%03u", crc);
        if (!append_tag(10, std::string_view(chk, 3))) {
            return {};
        }

        return std::string_view(buffer_.data(), msg_len_);

    }
}
