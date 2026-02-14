//
// Created by djaiswal on 1/16/26.
//

#include "FixGateway.h"
#include "../include/async_logger.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <immintrin.h>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>

namespace {
    constexpr char kFixDelim = '\x01';

    struct FixMsg {
        std::unordered_map<int, std::string_view> fields{};
        char delim{kFixDelim};
    };

    std::string_view reject_reason_text(jolt::ob::RejectReason reason) {
        switch (reason) {
        case jolt::ob::RejectReason::InvalidQty:
            return "InvalidQty";
        case jolt::ob::RejectReason::InvalidPrice:
            return "InvalidPrice";
        case jolt::ob::RejectReason::NonExistent:
            return "NonExistent";
        case jolt::ob::RejectReason::TifExpired:
            return "TifExpired";
        case jolt::ob::RejectReason::NotFillable:
            return "NotFillable";
        case jolt::ob::RejectReason::InvalidType:
            return "InvalidType";
        case jolt::ob::RejectReason::NotApplicable:
        default:
            return "Rejected";
        }
    }

    bool utc_timestamp(char* buf, size_t cap, size_t& len) {
        using namespace std::chrono;
        const auto now = system_clock::now();
        const auto tt = system_clock::to_time_t(now);
        std::tm tm{};
        gmtime_r(&tt, &tm);
        const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
        const int wrote = std::snprintf(
            buf,
            cap,
            "%04d%02d%02d-%02d:%02d:%02d.%03d",
            tm.tm_year + 1900,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec,
            static_cast<int>(ms.count()));
        if (wrote <= 0 || static_cast<size_t>(wrote) >= cap) {
            return false;
        }
        len = static_cast<size_t>(wrote);
        return true;
    }

    bool parse_uint64(std::string_view s, uint64_t& out) {
        if (s.empty()) {
            return false;
        }
        uint64_t value = 0;
        auto first = s.data();
        auto last = s.data() + s.size();
        auto [ptr, ec] = std::from_chars(first, last, value);
        if (ec != std::errc{} || ptr != last) {
            return false;
        }
        out = value;
        return true;
    }

    bool parse_uint32(std::string_view s, uint32_t& out) {
        uint64_t value = 0;
        if (!parse_uint64(s, value) || value > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        out = static_cast<uint32_t>(value);
        return true;
    }

    std::string_view trim_ascii_ws(std::string_view s) {
        size_t b = 0;
        size_t e = s.size();
        while (b < e && std::isspace(static_cast<unsigned char>(s[b])) != 0) {
            ++b;
        }
        while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1])) != 0) {
            --e;
        }
        return s.substr(b, e - b);
    }

    bool parse_symbol_id(std::string_view raw, uint16_t& out) {
        const std::string_view s = trim_ascii_ws(raw);
        if (s.empty()) {
            return false;
        }

        uint32_t numeric = 0;
        if (parse_uint32(s, numeric) && numeric <= std::numeric_limits<uint16_t>::max()) {
            if (!jolt::is_valid_symbol_id(numeric)) {
                return false;
            }
            out = static_cast<uint16_t>(numeric);
            return true;
        }

        // Support common forms like SYM1 / SYMBOL_4 by parsing trailing digits.
        size_t end = s.size();
        while (end > 0 && std::isdigit(static_cast<unsigned char>(s[end - 1])) != 0) {
            --end;
        }
        if (end == s.size()) {
            return false;
        }

        const std::string_view digits = s.substr(end);
        if (!parse_uint32(digits, numeric) || numeric > std::numeric_limits<uint16_t>::max()) {
            return false;
        }
        if (!jolt::is_valid_symbol_id(numeric)) {
            return false;
        }
        out = static_cast<uint16_t>(numeric);
        return true;
    }

    bool is_digits(std::string_view s) {
        if (s.empty()) {
            return false;
        }
        return std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
    }

    uint64_t fnv1a_64(std::string_view s) {
        constexpr uint64_t kOffset = 14695981039346656037ull;
        constexpr uint64_t kPrime = 1099511628211ull;
        uint64_t hash = kOffset;
        for (unsigned char c : s) {
            hash ^= c;
            hash *= kPrime;
        }
        return hash;
    }

    uint64_t id_from_cl_ord_id(std::string_view cl_ord_id) {
        if (is_digits(cl_ord_id)) {
            uint64_t id = 0;
            if (parse_uint64(cl_ord_id, id)) {
                return id;
            }
        }

        constexpr std::string_view kClientPrefix = "CLIENT_";
        if (cl_ord_id.size() > kClientPrefix.size() &&
            cl_ord_id.substr(0, kClientPrefix.size()) == kClientPrefix) {
            const std::string_view suffix = cl_ord_id.substr(kClientPrefix.size());
            if (is_digits(suffix)) {
                uint64_t id = 0;
                if (parse_uint64(suffix, id) && id > 0) {
                    return id;
                }
            }
        }
        return fnv1a_64(cl_ord_id);
    }

    bool parse_fix_message(std::string_view msg, FixMsg& out) {
        out.fields.clear();
        char delim = kFixDelim;

        if (msg.find(kFixDelim) == std::string_view::npos && msg.find('|') != std::string_view::npos) {
            delim = '|';
        }

        out.delim = delim;

        size_t pos = 0;
        while (pos < msg.size()) {
            size_t eq = msg.find('=', pos);
            if (eq == std::string_view::npos) {
                return false;
            }

            std::string_view tag_view = msg.substr(pos, eq - pos);
            int tag = 0;
            auto [ptr, ec] = std::from_chars(tag_view.data(), tag_view.data() + tag_view.size(), tag);
            if (ec != std::errc{} || ptr != tag_view.data() + tag_view.size()) {
                return false;
            }

            size_t value_start = eq + 1;
            size_t value_end = msg.find(delim, value_start);
            if (value_end == std::string_view::npos) {
                value_end = msg.size();
            }

            out.fields[tag] = msg.substr(value_start, value_end - value_start);
            pos = value_end + 1;
        }
        return true;
    }

    struct FixBuffer {
        char* data{nullptr};
        size_t len{0};
        size_t cap{0};
    };

    bool append_bytes(FixBuffer& buf, std::string_view value) {
        if (value.empty()) {
            return true;
        }
        if (buf.len + value.size() > buf.cap) {
            return false;
        }
        std::memcpy(buf.data + buf.len, value.data(), value.size());
        buf.len += value.size();
        return true;
    }

    bool append_char(FixBuffer& buf, char value) {
        if (buf.len + 1 > buf.cap) {
            return false;
        }
        buf.data[buf.len++] = value;
        return true;
    }

    bool append_tag(FixBuffer& buf, int tag) {
        char tag_buf[16];
        auto [ptr, ec] = std::to_chars(tag_buf, tag_buf + sizeof(tag_buf), tag);
        if (ec != std::errc{}) {
            return false;
        }
        return append_bytes(buf, std::string_view(tag_buf, static_cast<size_t>(ptr - tag_buf))) &&
            append_char(buf, '=');
    }

    bool append_field(FixBuffer& buf, int tag, std::string_view value) {
        return append_tag(buf, tag) && append_bytes(buf, value) && append_char(buf, kFixDelim);
    }

    bool append_field(FixBuffer& buf, int tag, uint64_t value) {
        char val_buf[32];
        auto [ptr, ec] = std::to_chars(val_buf, val_buf + sizeof(val_buf), value);
        if (ec != std::errc{}) {
            return false;
        }
        return append_tag(buf, tag) &&
            append_bytes(buf, std::string_view(val_buf, static_cast<size_t>(ptr - val_buf))) &&
            append_char(buf, kFixDelim);
    }


    bool append_timestamp_field(FixBuffer& buf, int tag) {
        char ts_buf[32];
        size_t ts_len = 0;
        if (!utc_timestamp(ts_buf, sizeof(ts_buf), ts_len)) {
            return false;
        }
        return append_field(buf, tag, std::string_view(ts_buf, ts_len));
    }

    bool append_checksum(FixBuffer& buf, uint32_t checksum) {
        char chk_buf[4];
        const int wrote = std::snprintf(chk_buf, sizeof(chk_buf), "%03u", checksum);
        if (wrote != 3) {
            return false;
        }
        return append_field(buf, 10, std::string_view(chk_buf, 3));
    }

    std::vector<size_t> parse_soh(std::string_view msg) {
        std::vector<size_t> pos;
        pos.reserve(64);
        const char* p = msg.data();
        const __m256i delim = _mm256_set1_epi8(kFixDelim);
        while (p != msg.data()) {
            __m256i curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            __m256i res = _mm256_cmpeq_epi8(curr, delim);
            int mask = _mm256_movemask_epi8(res);

            while (mask) {
                int bit = __builtin_ctz(mask);
                pos.push_back(bit);
                mask &= mask - 1;
            }
            p += 32;
        }
    }


    std::string_view get_tag(const FixMsg& msg, int tag) {
        auto it = msg.fields.find(tag);
        if (it == msg.fields.end()) {
            return {};
        }
        return it->second;
    }

    std::string_view fix_ord_type(jolt::ob::OrderType type) {
        switch (type) {
        case jolt::ob::OrderType::Market:
            return "1";
        case jolt::ob::OrderType::Limit:
            return "2";
        case jolt::ob::OrderType::StopMarket:
            return "3";
        case jolt::ob::OrderType::StopLimit:
            return "4";
        case jolt::ob::OrderType::TakeProfit:
        default:
            return "2";
        }
    }

    bool parse_fix_ord_type(std::string_view tag, jolt::ob::OrderType& out) {
        if (tag.size() != 1) {
            return false;
        }
        switch (tag[0]) {
        case '1':
            out = jolt::ob::OrderType::Market;
            return true;
        case '2':
            out = jolt::ob::OrderType::Limit;
            return true;
        case '3':
            out = jolt::ob::OrderType::StopMarket;
            return true;
        case '4':
            out = jolt::ob::OrderType::StopLimit;
            return true;
        default:
            return false;
        }
    }

    std::string_view fix_tif(jolt::ob::TIF tif) {
        switch (tif) {
        case jolt::ob::TIF::IOC:
            return "3";
        case jolt::ob::TIF::FOK:
            return "4";
        case jolt::ob::TIF::GTC:
        default:
            return "1";
        }
    }

    const char* order_action_text(jolt::ob::OrderAction action) {
        switch (action) {
        case jolt::ob::OrderAction::New:
            return "New";
        case jolt::ob::OrderAction::Modify:
            return "Modify";
        case jolt::ob::OrderAction::Cancel:
            return "Cancel";
        }
        return "Unknown";
    }

    const char* exchange_msg_type_text(jolt::ExchToGtwyMsg::Type type) {
        switch (type) {
        case jolt::ExchToGtwyMsg::Type::Submitted:
            return "Submitted";
        case jolt::ExchToGtwyMsg::Type::Rejected:
            return "Rejected";
        case jolt::ExchToGtwyMsg::Type::Filled:
            return "Filled";
        }
        return "Unknown";
    }

    bool is_client_order_msg_type(std::string_view msg_type) {
        return msg_type == "D" || msg_type == "F" || msg_type == "G";
    }
}

static int make_listen_socket(uint16_t port) {
    int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_any;
    addr.sin6_port = htons(port);

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    if (::listen(fd, 128) < 0) {
        ::close(fd);
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return fd;
}


namespace jolt::gateway {
    FixGateway::FixGateway(const std::string& gtwy_to_exch_name, const std::string& exch_to_gtwy_name)
        : gtwy_exch_(gtwy_to_exch_name, SharedRingMode::Attach),
          exch_gtwy_(exch_to_gtwy_name, SharedRingMode::Attach),
          event_loop_(make_listen_socket(8080)) {
        event_loop_.set_gateway(this);
        sessions_.resize(1);
        next_client_traffic_log_ = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    }

    void FixGateway::load_clients(const std::vector<ClientInfo>& clients) {
        client_infos_.reserve(clients.size());
        for (const auto& client : clients) {
            client_infos_.insert(client.client_id, client);
        }
    }

    bool FixGateway::submit_order(const ob::OrderParams& order, ob::RejectReason& reason) {
        // auto* client = client_infos_.find(order.client_id);
        // if (!client) {
        //     reason = ob::RejectReason::NonExistent;
        //     log_error("[gtwy] gateway failed to forward order: client not found order_id=" +
        //               std::to_string(order.id) +
        //               " client_id=" + std::to_string(order.client_id));
        //     return false;
        // }
        // if (!risk_check(*client, order, reason)) {
        //     log_warn("[gtwy] gateway risk-check rejected order_id=" + std::to_string(order.id) +
        //              " client_id=" + std::to_string(order.client_id) +
        //              " reason=" + std::string(reject_reason_text(reason)));
        //     return false;
        // }

        GtwyToExchMsg msg{};
        msg.order = order;
        msg.client_id = order.client_id;
        if (!gtwy_exch_.enqueue(msg)) {
            reason = ob::RejectReason::NotApplicable;
            log_error("[gtwy] gateway->exchange enqueue failed order_id=" + std::to_string(order.id) +
                      " client_id=" + std::to_string(order.client_id));
            return false;
        }

        reason = ob::RejectReason::NotApplicable;
        return true;
    }

    void FixGateway::start() {
        event_loop_.start();
    }

    void FixGateway::stop() {
        event_loop_.stop();
    }

    SessionState* FixGateway::get_or_create_session(const uint64_t session_id) {
        if (session_id == 0 || session_id > std::numeric_limits<uint32_t>::max()) {
            return nullptr;
        }
        if (session_id >= sessions_.size()) {
            sessions_.resize(session_id + 1);
        }
        auto& session = sessions_[session_id];
        if (!session.initialized || session.session_id != session_id) {
            session.reset(session_id);
        }
        return &session;
    }

    bool FixGateway::build_exec_report(FixMessage& out,
                                       SessionState* session,
                                       const OrderState& state,
                                       uint64_t exec_id,
                                       bool accepted,
                                       ob::RejectReason reason) {
        FixMessage body_msg{};
        FixBuffer body{body_msg.data.data(), 0, body_msg.data.size()};

        if (!append_field(body, 35, "8")) {
            return false;
        }
        if (!append_field(body, 49, session->target_comp_id)) {
            return false;
        }
        if (!append_field(body, 56, session->sender_comp_id)) {
            return false;
        }
        if (!append_field(body, 34, session->seq++)) {
            return false;
        }
        if (!append_timestamp_field(body, 52)) {
            return false;
        }

        std::string_view exec_type = "0";
        std::string_view ord_status = "0";
        if (!accepted) {
            exec_type = "8";
            ord_status = "8";
        }
        else if (state.state == State::PendingNew) {
            exec_type = "A";
            ord_status = "A";
        }
        else if (state.state == State::PendingCancel) {
            exec_type = "6";
            ord_status = "6";
        }
        else if (state.state == State::PendingReplace) {
            exec_type = "E";
            ord_status = "E";
        }
        else if (state.state == State::New) {
            exec_type = "0";
            ord_status = "0";
        }
        else if (state.state == State::Replaced) {
            exec_type = "5";
            ord_status = "5";
        }
        else if (state.state == State::Cancelled) {
            exec_type = "4";
            ord_status = "4";
        }

        if (!append_field(body, 150, exec_type)) {
            return false;
        }
        if (!append_field(body, 39, ord_status)) {
            return false;
        }
        if (!append_field(body, 11, state.cl_ord_id)) {
            return false;
        }
        if (!state.orig_cl_ord_id.empty()) {
            if (!append_field(body, 41, state.orig_cl_ord_id)) {
                return false;
            }
        }
        if (!append_field(body, 37, state.params.id)) {
            return false;
        }
        if (!append_field(body, 17, exec_id)) {
            return false;
        }
        if (!append_field(body, 54, state.params.side == ob::Side::Buy ? "1" : "2")) {
            return false;
        }
        if (!append_field(body, 38, state.params.qty)) {
            return false;
        }
        if (!append_field(body, 40, fix_ord_type(state.params.type))) {
            return false;
        }
        if (state.params.type == ob::OrderType::Limit) {
            if (!append_field(body, 44, (state.params.price))) {
                return false;
            }
        }
        else if (state.params.type == ob::OrderType::StopLimit) {
            if (!append_field(body, 44, (state.params.limit_px))) {
                return false;
            }
            if (state.params.trigger != 0) {
                if (!append_field(body, 99, (state.params.trigger))) {
                    return false;
                }
            }
        }
        else if (state.params.type == ob::OrderType::StopMarket) {
            if (state.params.trigger != 0) {
                if (!append_field(body, 99, (state.params.trigger))) {
                    return false;
                }
            }
        }
        if (!append_field(body, 59, fix_tif(state.params.tif))) {
            return false;
        }
        if (!append_timestamp_field(body, 60)) {
            return false;
        }
        if (!accepted) {
            if (!append_field(body, 58, reject_reason_text(reason))) {
                return false;
            }
        }
        if (!state.symbol.empty()) {
            if (!append_field(body, 55, state.symbol)) {
                return false;
            }
        }

        FixBuffer msg{out.data.data(), 0, out.data.size()};
        if (!append_field(msg, 8, "FIX.4.4")) {
            return false;
        }
        if (!append_field(msg, 9, (body.len))) {
            return false;
        }
        if (!append_bytes(msg, std::string_view(body_msg.data.data(), body.len))) {
            return false;
        }

        uint32_t checksum = 0;
        for (size_t i = 0; i < msg.len; ++i) {
            checksum += static_cast<unsigned char>(msg.data[i]);
        }
        checksum %= 256;
        if (!append_checksum(msg, checksum)) {
            return false;
        }

        out.len = msg.len;
        out.session_id = state.session_id;
        return true;
    }

    bool FixGateway::build_logon(FixMessage& out,
                                 SessionState* session,
                                 uint32_t heartbeat_int,
                                 bool reset_seq) {
        FixMessage body_msg{};
        FixBuffer body{body_msg.data.data(), 0, body_msg.data.size()};

        if (!append_field(body, 35, "A")) {
            return false;
        }
        if (!append_field(body, 49, session->target_comp_id)) {
            return false;
        }
        if (!append_field(body, 56, session->sender_comp_id)) {
            return false;
        }
        if (!append_field(body, 34, session->seq++)) {
            return false;
        }
        if (!append_timestamp_field(body, 52)) {
            return false;
        }
        if (!append_field(body, 98, "0")) {
            return false;
        }
        if (!append_field(body, 108, (heartbeat_int))) {
            return false;
        }
        if (reset_seq) {
            if (!append_field(body, 141, "Y")) {
                return false;
            }
        }

        FixBuffer msg{out.data.data(), 0, out.data.size()};
        if (!append_field(msg, 8, "FIX.4.4")) {
            return false;
        }
        if (!append_field(msg, 9, (body.len))) {
            return false;
        }
        if (!append_bytes(msg, std::string_view(body_msg.data.data(), body.len))) {
            return false;
        }

        uint32_t checksum = 0;
        for (size_t i = 0; i < msg.len; ++i) {
            checksum += static_cast<unsigned char>(msg.data[i]);
        }
        checksum %= 256;
        if (!append_checksum(msg, checksum)) {
            return false;
        }

        out.len = msg.len;
        return true;
    }

    bool FixGateway::on_fix_message(std::string_view message, uint64_t session_id) {
        FixMsg msg{};
        if (!parse_fix_message(message, msg)) {
            log_error("[gtwy] gateway failed to parse FIX from client session=" + std::to_string(session_id));
            return false;
        }

        const auto msg_type = get_tag(msg, 35);
        if (msg_type.empty()) {
            log_error("[gtwy] gateway received FIX without MsgType session=" + std::to_string(session_id));
            return false;
        }

        auto* session = get_or_create_session(session_id);
        if (!session) {
            log_error("[gtwy] gateway failed to resolve session state session=" + std::to_string(session_id));
            return false;
        }

        const auto sender = get_tag(msg, 49);
        const auto target = get_tag(msg, 56);

        if (sender.empty() || target.empty()) {
            log_error("[gtwy] gateway received FIX missing CompIDs session=" + std::to_string(session_id));
            return false;
        }

        if (session->sender_comp_id.empty()) {
            session->sender_comp_id = std::string(sender);
        }
        if (session->target_comp_id.empty()) {
            session->target_comp_id = std::string(target);
        }

        uint64_t client_id = 0;
        auto account = get_tag(msg, 1);

        if (!account.empty()) {
            client_id = id_from_cl_ord_id(account);
        }
        else {
            client_id = id_from_cl_ord_id(sender);
        }

        auto client_it = clients_.find(client_id);

        if (client_it == clients_.end()) {
            auto client = std::make_unique<Client>(client_id);
            client->set_gateway(this);
            client->set_session_id(session_id);
            clients_.emplace(client_id, std::move(client));
        }
        else {
            client_it->second->set_session_id(session_id);
        }

        if (msg_type == "A") {
            FixMessage out{};
            if (build_logon(out, session, 30, false)) {
                session->logged_on = true;
                out.session_id = session_id;
                queue_fix_message(std::move(out));
                return true;
            }
            throw std::runtime_error("[gtwy] failed to build logon msg");
        }

        OrderState* state = nullptr;
        auto cl_ord_id = get_tag(msg, 11);
        if (cl_ord_id.empty()) {
            log_error("[gtwy] gateway received order without ClOrdID client_id=" + std::to_string(client_id) +
                      " session=" + std::to_string(session_id));
            return false;
        }

        auto orig_cl_ord_id = get_tag(msg, 41);

        if (orig_cl_ord_id.empty()) {
            orig_cl_ord_id = cl_ord_id;
        }

        if (msg_type.size() != 1) {
            log_error("[gtwy] gateway received invalid MsgType for order client_id=" + std::to_string(client_id) +
                      " session=" + std::to_string(session_id));
            return false;
        }

        if (is_client_order_msg_type(msg_type)) {
            log_info("[gtwy] gateway received order from client msg_type=" + std::string(msg_type) +
                     " cl_ord_id=" + std::string(cl_ord_id) +
                     " client_id=" + std::to_string(client_id) +
                     " session=" + std::to_string(session_id));
        }

        switch (msg_type[0]) {
        case 'D':
            state = new OrderState{};
            state->params.client_id = client_id;
            state->params.ts = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());

            state->session_id = session_id;
            state->params.action = ob::OrderAction::New;
            state->action = ob::OrderAction::New;
            state->cl_ord_id = std::string(cl_ord_id);
            order_states_[std::string(orig_cl_ord_id)] = state;
            state->order_id = next_order_id_++;
            state->params.id = state->order_id;


            break;
        case 'F':
            if (orig_cl_ord_id.empty()) {
                log_error("[gtwy] gateway cancel missing OrigClOrdID cl_ord_id=" + std::string(cl_ord_id) +
                          " client_id=" + std::to_string(client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            {
                auto it = order_states_.find(std::string(orig_cl_ord_id));
                if (it == order_states_.end() || !it->second) {
                    log_error("[gtwy] gateway cancel references unknown OrigClOrdID=" + std::string(orig_cl_ord_id) +
                              " client_id=" + std::to_string(client_id) +
                              " session=" + std::to_string(session_id));
                    return false;
                }
                state = it->second;
            }
            state->params.action = ob::OrderAction::Cancel;
            state->action = ob::OrderAction::Cancel;
            break;
        case 'G':
            if (orig_cl_ord_id.empty()) {
                log_error("[gtwy] gateway replace missing OrigClOrdID cl_ord_id=" + std::string(cl_ord_id) +
                          " client_id=" + std::to_string(client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            {
                auto it = order_states_.find(std::string(orig_cl_ord_id));
                if (it == order_states_.end() || !it->second) {
                    log_error("[gtwy] gateway replace references unknown OrigClOrdID=" + std::string(orig_cl_ord_id) +
                              " client_id=" + std::to_string(client_id) +
                              " session=" + std::to_string(session_id));
                    return false;
                }
                state = it->second;
            }
            state->params.action = ob::OrderAction::Modify;
            state->action = ob::OrderAction::Modify;
            break;
        default:
            log_error("[gtwy] gateway unsupported order MsgType=" + std::string(msg_type) +
                      " client_id=" + std::to_string(client_id) +
                      " session=" + std::to_string(session_id));
            return false;
        }

        state->cl_ord_id = std::string(cl_ord_id);

        if (!orig_cl_ord_id.empty()) {
            state->orig_cl_ord_id = std::string(orig_cl_ord_id);
        }

        state->params.id = state->order_id;

        auto symbol = get_tag(msg, 55);
        if (!symbol.empty()) {
            uint16_t symbol_id = 0;
            if (!parse_symbol_id(symbol, symbol_id)) {
                log_error("[gtwy] gateway failed parsing symbol tag55 value=" + std::string(symbol) +
                          " order_id=" + std::to_string(state->params.id) +
                          " client_id=" + std::to_string(state->params.client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            state->params.symbol_id = symbol_id;
            state->symbol = std::string(symbol);
        }
        else if (state->params.action == ob::OrderAction::New) {
            log_error("[gtwy] gateway new order missing symbol tag55 order_id=" + std::to_string(state->params.id) +
                      " client_id=" + std::to_string(state->params.client_id) +
                      " session=" + std::to_string(session_id));
            return false;
        }

        auto side_tag = get_tag(msg, 54);
        if (side_tag == "1") {
            state->params.side = ob::Side::Buy;
        }
        else if (side_tag == "2") {
            state->params.side = ob::Side::Sell;
        }
        else if (state->params.action == ob::OrderAction::New) {
            log_error("[gtwy] gateway new order missing side tag54 order_id=" + std::to_string(state->params.id) +
                      " client_id=" + std::to_string(state->params.client_id) +
                      " session=" + std::to_string(session_id));
            return false;
        }

        auto tif_tag = get_tag(msg, 59);
        if (tif_tag.size() == 1) {
            switch (tif_tag[0]) {
            case '3':
                state->params.tif = ob::TIF::IOC;
                break;
            case '4':
                state->params.tif = ob::TIF::FOK;
                break;
            default:
                state->params.tif = ob::TIF::GTC;
                break;
            }
        }
        else {
            state->params.tif = ob::TIF::GTC;
        }

        bool invalid_ord_type = false;
        auto ord_type_tag = get_tag(msg, 40);

        if (state->params.action == ob::OrderAction::New || state->params.action == ob::OrderAction::Modify) {
            if (ord_type_tag.empty()) {
                if (state->params.action == ob::OrderAction::New) {
                    invalid_ord_type = true;
                }
            } else if (!parse_fix_ord_type(ord_type_tag, state->params.type)) {
                invalid_ord_type = true;
            }
        }

        auto qty_tag = get_tag(msg, 38);

        if (!qty_tag.empty()) {
            uint64_t qty = 0;
            if (!parse_uint64(qty_tag, qty)) {
                log_error("[gtwy] gateway failed parsing qty tag38 value=" + std::string(qty_tag) +
                          " order_id=" + std::to_string(state->params.id) +
                          " client_id=" + std::to_string(state->params.client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            state->params.qty = qty;
        }

        auto price_tag = get_tag(msg, 44);
        if (!price_tag.empty()) {
            ob::PriceTick price = 0;
            if (!parse_uint32(price_tag, price)) {
                log_error("[gtwy] gateway failed parsing price tag44 value=" + std::string(price_tag) +
                          " order_id=" + std::to_string(state->params.id) +
                          " client_id=" + std::to_string(state->params.client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            if (state->params.type == ob::OrderType::StopLimit) {
                state->params.limit_px = price;
            }
            else {
                state->params.price = price;
            }
        }

        auto stop_px = get_tag(msg, 99);
        if (!stop_px.empty()) {
            ob::PriceTick trigger = 0;
            if (!parse_uint32(stop_px, trigger)) {
                log_error("[gtwy] gateway failed parsing stop tag99 value=" + std::string(stop_px) +
                          " order_id=" + std::to_string(state->params.id) +
                          " client_id=" + std::to_string(state->params.client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            state->params.trigger = trigger;
        }

        if (state->params.type == ob::OrderType::StopMarket ||
            state->params.type == ob::OrderType::StopLimit) {
            if (state->params.trigger != 0) {
                state->params.price = state->params.trigger;
            }
        }

        ob::RejectReason reason = ob::RejectReason::NotApplicable;
        if (state->params.action == ob::OrderAction::New) {
            if (invalid_ord_type) {
                reason = ob::RejectReason::InvalidType;
            } else if (state->params.type == ob::OrderType::Limit && state->params.price == 0) {
                reason = ob::RejectReason::InvalidPrice;
            } else if (state->params.type == ob::OrderType::StopMarket && state->params.trigger == 0) {
                reason = ob::RejectReason::InvalidPrice;
            } else if (state->params.type == ob::OrderType::StopLimit &&
                (state->params.trigger == 0 || state->params.limit_px == 0)) {
                reason = ob::RejectReason::InvalidPrice;
            }
        } else if (state->params.action == ob::OrderAction::Modify && invalid_ord_type) {
            reason = ob::RejectReason::InvalidType;
        }

        if (state->params.action == ob::OrderAction::Modify && state->params.qty == 0) {
            reason = ob::RejectReason::InvalidQty;
        }

        if (reason != ob::RejectReason::NotApplicable) {
            log_warn("[gtwy] gateway local reject order_id=" + std::to_string(state->params.id) +
                     " client_id=" + std::to_string(state->params.client_id) +
                     " session=" + std::to_string(session_id) +
                     " action=" + std::string(order_action_text(state->params.action)) +
                     " reason=" + std::string(reject_reason_text(reason)));
            FixMessage fix{};
            if (!build_exec_report(fix, session, *state, next_exec_id_++, false, reason)) {
                log_error("[gtwy] gateway failed building reject ExecReport order_id=" +
                          std::to_string(state->params.id) +
                          " client_id=" + std::to_string(state->params.client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            state->state = State::Rejected;
            queue_fix_message(std::move(fix));
            return false;
        }

        if (!submit_order(state->params, reason)) {
            log_error("[gtwy] gateway submit_order failed order_id=" + std::to_string(state->params.id) +
                      " client_id=" + std::to_string(state->params.client_id) +
                      " session=" + std::to_string(session_id) +
                      " reason=" + std::string(reject_reason_text(reason)));
            FixMessage fix{};
            if (!build_exec_report(fix, session, *state, next_exec_id_++, false, reason)) {
                log_error("[gtwy] gateway failed building submit-failed ExecReport order_id=" +
                          std::to_string(state->params.id) +
                          " client_id=" + std::to_string(state->params.client_id) +
                          " session=" + std::to_string(session_id));
                return false;
            }
            queue_fix_message(std::move(fix));
            return false;
        }

        const std::string state_key =
            orig_cl_ord_id.empty() ? std::string(cl_ord_id) : std::string(orig_cl_ord_id);
        if (state->params.action == ob::OrderAction::New) {
            state->state = State::PendingNew;
            order_states_[state_key] = state;
        }
        else if (state->params.action == ob::OrderAction::Modify) {
            state->state = State::PendingReplace;
            order_states_[std::string(cl_ord_id)] = state;
        }
        else {
            state->state = State::PendingCancel;
        }

        order_id_to_state_[state->order_id] = state;

        // GtwyToExchMsg out{};
        // out.token = state->order_id;
        // out.order = state->params;
        // out.client_id = client_id;
        // gtwy_exch_.enqueue(out);
        //
        // FixMessage report{};
        // if (!build_exec_report(report, session, *state, next_exec_id_++, true, reason)) {
        //     std::cerr << "[gtwy] err building exec report\n";
        // }
        //
        // queue_fix_message(report);

        return true;
    }


    void FixGateway::poll() {
        poll_io();
        poll_exchange();
    }

    void FixGateway::poll_exchange() {
        while (auto msg = exch_gtwy_.dequeue()) {
            log_info("[gtwy] gateway received response from exchange type=" +
                     std::string(exchange_msg_type_text(msg->type)) +
                     " order_id=" + std::to_string(msg->order_id) +
                     " client_id=" + std::to_string(msg->client_id));
            handle_exchange_msg(*msg);
        }
    }

    void FixGateway::poll_io() {
        while (auto msg = inbound_.dequeue()) {
            if (msg->len == 0) {
                const uint64_t disconnected_session_id = msg->session_id;
                for (auto& [_, client] : clients_) {
                    if (!client) {
                        continue;
                    }
                    if (client->get_session_id() == disconnected_session_id) {
                        client->set_session_id(UINT64_MAX);
                    }
                }
                continue;
            }
            if (!on_fix_message({msg->data.data(), msg->len}, msg->session_id)) {
                log_error("[gtwy] gateway failed handling inbound FIX from client session=" +
                          std::to_string(msg->session_id));
            }
        }
    }

    bool FixGateway::risk_check(const ClientInfo& client, const ob::OrderParams& order,
                                ob::RejectReason& reason) const {
        if (order.action == ob::OrderAction::Cancel) {
            reason = ob::RejectReason::NotApplicable;
            return true;
        }
        if (order.action == ob::OrderAction::New && order.qty == 0) {
            reason = ob::RejectReason::InvalidQty;
            return false;
        }
        if (order.qty != 0 && client.max_qty > 0 && order.qty > client.max_qty) {
            reason = ob::RejectReason::InvalidQty;
            return false;
        }
        if (order.action == ob::OrderAction::New &&
            client.max_open_orders > 0 && client.open_orders >= client.max_open_orders) {
            reason = ob::RejectReason::InvalidQty;
            return false;
        }
        if (order.action == ob::OrderAction::New && client.max_pos != 0) {
            const int64_t signed_qty = (order.side == ob::Side::Buy)
                                           ? static_cast<int64_t>(order.qty)
                                           : -static_cast<int64_t>(order.qty);
            const int64_t projected = client.net_pos + signed_qty;
            if (std::llabs(projected) > std::llabs(client.max_pos)) {
                reason = ob::RejectReason::InvalidQty;
                return false;
            }
        }
        reason = ob::RejectReason::NotApplicable;
        return true;
    }

    void FixGateway::handle_exchange_msg(const ExchToGtwyMsg& msg) {
        const uint64_t state_order_id = msg.order_id;
        auto it = order_id_to_state_.find(state_order_id);

        if (it == order_id_to_state_.end() || !it->second) {
            log_warn("[gtwy] gateway got exchange response for unknown order_id=" +
                     std::to_string(state_order_id) +
                     " client_id=" + std::to_string(msg.client_id));
            return;
        }
        auto order_state = it->second;

        const uint64_t sess_id = order_state->session_id;

        if (sess_id == UINT64_MAX || sess_id >= sessions_.size()) {
            log_warn("[gtwy] gateway cannot route exchange response, invalid session order_id=" +
                     std::to_string(state_order_id) +
                     " client_id=" + std::to_string(order_state->params.client_id) +
                     " session=" + std::to_string(sess_id));
            return;
        }
        auto* sess = &sessions_[sess_id];

        if (!sess->initialized) {
            log_warn("[gtwy] gateway session not initialized for exchange response order_id=" +
                     std::to_string(state_order_id) +
                     " client_id=" + std::to_string(order_state->params.client_id) +
                     " session=" + std::to_string(sess_id));
            return;
        }

        auto* state = it->second;
        switch (msg.type) {
        case ExchToGtwyMsg::Type::Submitted:
            {
                switch (state->state) {
                case State::PendingNew:
                    state->state = State::New;
                    break;
                case State::PendingCancel:
                    state->state = State::Cancelled;
                    break;
                case State::PendingReplace:
                    state->state = State::Replaced;
                    break;
                default:
                    break;
                }

                FixMessage fix_submit{};

                if (!build_exec_report(fix_submit, sess, *state, next_exec_id_++, true, msg.reason)) {
                    log_error("[gtwy] gateway failed building submit ExecReport order_id=" +
                              std::to_string(state_order_id) +
                              " client_id=" + std::to_string(state->params.client_id) +
                              " session=" + std::to_string(sess_id));
                    return;
                }

                queue_fix_message(fix_submit);
                break;

            }

        case ExchToGtwyMsg::Type::Rejected:
            {
                state->state = State::Rejected;
                FixMessage fix_reject{};

                if (!build_exec_report(fix_reject, sess, *state, next_exec_id_++, false, msg.reason)) {
                    log_error("[gtwy] gateway failed building reject ExecReport order_id=" +
                              std::to_string(state_order_id) +
                              " client_id=" + std::to_string(state->params.client_id) +
                              " session=" + std::to_string(sess_id));
                    return;
                }

                queue_fix_message(fix_reject);
                break;
            }
        case ExchToGtwyMsg::Type::Filled:
            {
                if (msg.fill_qty == state->params.qty) {
                    state->state = State::Filled;
                }
                else {
                    state->params.qty -= msg.fill_qty;
                }
                FixMessage fix_fill{};

                if (!build_exec_report(fix_fill, sess, *state, next_exec_id_++, true, msg.reason)) {
                    log_error("[gtwy] gateway failed building fill ExecReport order_id=" +
                              std::to_string(state_order_id) +
                              " client_id=" + std::to_string(state->params.client_id) +
                              " session=" + std::to_string(sess_id));
                    return;
                }

                queue_fix_message(fix_fill);
                break;
            }
            default: break;
        }
    }

    void FixGateway::queue_fix_message(const FixMessage& msg) {
        if (!outbound_.enqueue(msg)) {
            log_error("[gtwy] gateway outbound queue full while routing response session=" +
                      std::to_string(msg.session_id));
        }
    }

    void FixGateway::clear_session_for_client(uint64_t client_id) {
        auto client = clients_.find(client_id);
        if (client != clients_.end()) {
            client->second.get()->set_session_id(UINT64_MAX);
        }
        else {
            std::cerr << "[gtwy] session for client already disconnected\n";
        }
    }
}
