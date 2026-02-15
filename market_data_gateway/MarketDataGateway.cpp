//
// Created by djaiswal on 2/3/26.
//

#include "MarketDataGateway.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <unistd.h>
#include <x86intrin.h>

namespace {
    constexpr char kFixDelim = '\x01';
    constexpr int kTagMsgType = 35;
    constexpr int kTagSender = 49;
    constexpr int kTagTarget = 56;
    constexpr int kTagSeq = 34;
    constexpr int kTagSendingTime = 52;
    constexpr int kTagMDReqId = 262;
    constexpr int kTagSubType = 263;
    constexpr int kTagSymbol = 55;
    constexpr int kTagUpdateType = 265;
    constexpr int kTagAggregated = 266;
    constexpr int kTagReqReject = 281;
    constexpr int kTagText = 58;
    constexpr int kTagGroup = 13000;
    constexpr int kTagPort = 13001;
    constexpr int kTagRecoveryHost = 13002;
    constexpr int kTagRecoveryPort = 13003;
    constexpr char kDefaultMdGroup[] = "239.0.0.1";
    constexpr uint16_t kDefaultUdpBasePort = 20001;
    constexpr char kDefaultRecoveryHost[] = "127.0.0.1";
    constexpr uint16_t kDefaultRecoveryPort = 21001;

    struct FixMsg {
        std::unordered_map<int, std::string_view> fields{};
        char delim{kFixDelim};
    };

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

    bool append_field(FixBuffer& buf, int tag, std::string_view value) {
        char tmp[32];
        auto [t_end, t_ec] = std::to_chars(tmp, tmp + sizeof(tmp), tag);
        if (t_ec != std::errc{}) {
            return false;
        }
        if (!append_bytes(buf, std::string_view(tmp, t_end - tmp))) {
            return false;
        }
        if (!append_bytes(buf, "=")) {
            return false;
        }
        if (!append_bytes(buf, value)) {
            return false;
        }
        return append_bytes(buf, std::string_view(&kFixDelim, 1));
    }

    bool append_field(FixBuffer& buf, int tag, uint64_t value) {
        char tmp[32];
        auto [v_end, v_ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
        if (v_ec != std::errc{}) {
            return false;
        }
        return append_field(buf, tag, std::string_view(tmp, v_end - tmp));
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

    bool append_timestamp_field(FixBuffer& buf, int tag) {
        char ts_buf[32];
        size_t ts_len = 0;
        if (!utc_timestamp(ts_buf, sizeof(ts_buf), ts_len)) {
            return false;
        }
        return append_field(buf, tag, std::string_view(ts_buf, ts_len));
    }

    bool append_checksum(FixBuffer& buf) {
        uint32_t sum = 0;
        for (size_t i = 0; i < buf.len; ++i) {
            sum += static_cast<unsigned char>(buf.data[i]);
        }
        sum %= 256;
        char chk_buf[4];
        std::snprintf(chk_buf, sizeof(chk_buf), "%03u", sum);
        return append_field(buf, 10, std::string_view(chk_buf, 3));
    }

    void parse_fix_simd(std::string_view msg, FixMsg& out) {
        out.fields.clear();
        char delim = kFixDelim;

        // if (msg.find(kFixDelim) == std::string_view::npos && msg.find('|') != std::string_view::npos) {
        //     delim = '|';
        // }

        out.delim = delim;

        const __m256i needle_delim = _mm256_set1_epi8(delim);
        const __m256i needle_eq = _mm256_set1_epi8('=');
        const char* base = msg.data();
        const char* p = base;
        const char* end = base + msg.size();

        size_t last_delim = -1;
        size_t last_eq = -1;

        auto emit_field = [&](size_t curr_delim) {
            if (last_eq == static_cast<size_t>(-1) || last_eq <= last_delim || last_eq >= curr_delim) {
                last_delim = curr_delim;
                last_eq = -1;
                return;
            }

            size_t tag_start = (last_delim == static_cast<size_t>(-1)) ? 0 : last_delim + 1;
            std::string_view tag_view(base + tag_start, last_eq - tag_start);
            uint64_t tag = 0;

            auto [ptr, ec] = std::from_chars(tag_view.data(), tag_view.data() + tag_view.size(), tag);
            if (ec == std::errc{} && ptr == tag_view.data() + tag_view.size()) {
                std::string_view value_view(base + last_eq + 1, curr_delim - (last_eq + 1));
                out.fields[tag] = value_view;
            }

            last_delim = curr_delim;
            last_eq = static_cast<size_t>(-1);
        };

        while (p + 32 <= end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            __m256i dv = _mm256_cmpeq_epi8(chunk, needle_delim);
            __m256i eqv = _mm256_cmpeq_epi8(chunk, needle_eq);

            uint32_t d_mask = _mm256_movemask_epi8(dv);
            uint32_t eq_mask = _mm256_movemask_epi8(eqv);
            uint32_t combined = eq_mask | d_mask;

            while (combined) {
                uint32_t bit = combined & (~combined + 1);
                int idx = __builtin_ctz(combined);
                size_t pos = p - base + idx;
                if (d_mask & bit) {
                    emit_field(pos);
                }
                else if (last_eq == static_cast<size_t>(-1)) {
                    last_eq = pos;
                }
                combined ^= bit;
            }
            p += 32;
        }

        for (; p < end; ++p) {
            if (*p == '=' && last_eq == static_cast<size_t>(-1)) {
                last_eq = static_cast<size_t>(p - base);
            }
            else if (*p == delim) {
                emit_field(static_cast<size_t>(p - base));
            }
        }

        if (last_eq != static_cast<size_t>(-1) && last_eq < msg.size()) {
            emit_field(msg.size());
        }
    }


    std::string_view get_tag(const FixMsg& msg, int tag) {
        auto it = msg.fields.find(tag);
        if (it == msg.fields.end()) {
            return {};
        }
        return it->second;
    }

    int make_listen_socket(uint16_t port) {
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

}

namespace jolt::md {
    MarketDataGateway::MarketDataGateway() : event_loop_(make_listen_socket(80))
    {
        event_loop_.set_gateway(this);

        for (size_t i = 0; i < jolt::kNumSymbols; ++i) {
            const uint16_t symbol_id = static_cast<uint16_t>(jolt::kFirstSymbolId + i);
            const std::string symbol = std::to_string(symbol_id);
            add_symbol_channel(symbol, kDefaultMdGroup, static_cast<uint16_t>(kDefaultUdpBasePort + i));
            symbol_to_id_[symbol] = symbol_id;
        }
        set_recovery_endpoint(kDefaultRecoveryHost, kDefaultRecoveryPort);

        setup();
    }


    void MarketDataGateway::setup() {
        event_loop_.start();
        event_loop_.run();
    }

    void MarketDataGateway::add_symbol_channel(const std::string& symbol,
                                               const std::string& group,
                                               uint16_t port) {
        channels_[symbol] = ChannelInfo{group, port};
    }

    void MarketDataGateway::set_recovery_endpoint(const std::string& host, uint16_t port) {
        recovery_host_ = host;
        recovery_port_ = port;
    }

    void MarketDataGateway::poll() {
        poll_io();
    }

    void MarketDataGateway::poll_io() {
        while (auto msg = inbound_.dequeue()) {
            on_fix_message({msg->data.data(), msg->len}, msg->session_id);
        }
    }


    void MarketDataGateway::queue_fix_message(const FixMessage& msg) {
        outbound_.enqueue(msg);
    }

    bool MarketDataGateway::build_logon(FixMessage& out, SessionState& session, uint32_t heartbeat_int) {
        FixMessage body_msg{};
        FixBuffer body{body_msg.data.data(), 0, body_msg.data.size()};

        if (!append_field(body, kTagMsgType, "A")) {
            return false;
        }
        if (!append_field(body, kTagSender, session.target_comp_id)) {
            return false;
        }
        if (!append_field(body, kTagTarget, session.sender_comp_id)) {
            return false;
        }
        if (!append_field(body, kTagSeq, session.seq++)) {
            return false;
        }
        if (!append_timestamp_field(body, kTagSendingTime)) {
            return false;
        }
        if (!append_field(body, 98, "0")) {
            return false;
        }
        if (!append_field(body, 108, static_cast<uint64_t>(heartbeat_int))) {
            return false;
        }

        FixBuffer msg{out.data.data(), 0, out.data.size()};
        if (!append_field(msg, 8, "FIX.4.4")) {
            return false;
        }
        if (!append_field(msg, 9, static_cast<uint64_t>(body.len))) {
            return false;
        }
        if (!append_bytes(msg, std::string_view(body_msg.data.data(), body.len))) {
            return false;
        }
        if (!append_checksum(msg)) {
            return false;
        }

        out.len = msg.len;
        return true;
    }

    bool MarketDataGateway::build_subscribe_response(FixMessage& out,
                                                     SessionState& session,
                                                     std::string_view req_id,
                                                     std::string_view symbol,
                                                     const ChannelInfo& channel) {
        FixMessage body_msg{};
        FixBuffer body{body_msg.data.data(), 0, body_msg.data.size()};

        if (!append_field(body, kTagMsgType, "U")) {
            return false;
        }
        if (!append_field(body, kTagSender, session.target_comp_id)) {
            return false;
        }
        if (!append_field(body, kTagTarget, session.sender_comp_id)) {
            return false;
        }
        if (!append_field(body, kTagSeq, session.seq++)) {
            return false;
        }
        if (!append_timestamp_field(body, kTagSendingTime)) {
            return false;
        }
        if (!append_field(body, kTagMDReqId, req_id)) {
            return false;
        }
        if (!append_field(body, kTagSymbol, symbol)) {
            return false;
        }
        if (!append_field(body, kTagGroup, channel.group)) {
            return false;
        }
        if (!append_field(body, kTagPort, static_cast<uint64_t>(channel.port))) {
            return false;
        }
        if (!recovery_host_.empty()) {
            if (!append_field(body, kTagRecoveryHost, recovery_host_)) {
                return false;
            }
        }
        if (recovery_port_ != 0) {
            if (!append_field(body, kTagRecoveryPort, static_cast<uint64_t>(recovery_port_))) {
                return false;
            }
        }

        FixBuffer msg{out.data.data(), 0, out.data.size()};
        if (!append_field(msg, 8, "FIX.4.4")) {
            return false;
        }
        if (!append_field(msg, 9, static_cast<uint64_t>(body.len))) {
            return false;
        }
        if (!append_bytes(msg, std::string_view(body_msg.data.data(), body.len))) {
            return false;
        }
        if (!append_checksum(msg)) {
            return false;
        }

        out.len = msg.len;
        return true;
    }

    bool MarketDataGateway::build_md_reject(FixMessage& out,
                                            SessionState& session,
                                            std::string_view req_id,
                                            uint32_t reason_code,
                                            std::string_view reason_text) {
        FixMessage body_msg{};
        FixBuffer body{body_msg.data.data(), 0, body_msg.data.size()};

        if (!append_field(body, kTagMsgType, "Y")) {
            return false;
        }
        if (!append_field(body, kTagSender, session.target_comp_id)) {
            return false;
        }
        if (!append_field(body, kTagTarget, session.sender_comp_id)) {
            return false;
        }
        if (!append_field(body, kTagSeq, session.seq++)) {
            return false;
        }
        if (!append_timestamp_field(body, kTagSendingTime)) {
            return false;
        }
        if (!append_field(body, kTagMDReqId, req_id)) {
            return false;
        }
        if (!append_field(body, kTagReqReject, static_cast<uint64_t>(reason_code))) {
            return false;
        }
        if (!reason_text.empty()) {
            if (!append_field(body, kTagText, reason_text)) {
                return false;
            }
        }

        FixBuffer msg{out.data.data(), 0, out.data.size()};
        if (!append_field(msg, 8, "FIX.4.4")) {
            return false;
        }
        if (!append_field(msg, 9, static_cast<uint64_t>(body.len))) {
            return false;
        }
        if (!append_bytes(msg, std::string_view(body_msg.data.data(), body.len))) {
            return false;
        }
        if (!append_checksum(msg)) {
            return false;
        }

        out.len = msg.len;
        return true;
    }

    bool MarketDataGateway::on_fix_message(std::string_view message, uint64_t session_id) {
        FixMsg fix{};
        parse_fix_simd(message, fix);

        std::string_view msg_type = get_tag(fix, kTagMsgType);
        if (msg_type.empty()) {
            return false;
        }

        auto& session = sessions_[session_id];
        if (session.session_id == 0) {
            session.session_id = session_id;
        }

        if (msg_type == "A") {
            std::string_view sender = get_tag(fix, kTagSender);
            std::string_view target = get_tag(fix, kTagTarget);
            if (sender.empty() || target.empty()) {
                return false;
            }
            session.sender_comp_id = std::string(sender);
            session.target_comp_id = std::string(target);
            session.logged_on = true;

            FixMessage out{};
            if (!build_logon(out, session, 30)) {
                return false;
            }
            out.session_id = session_id;
            queue_fix_message(out);
            return true;
        }

        if (msg_type == "V") {
            std::string_view req_id = get_tag(fix, kTagMDReqId);
            std::string_view sub_type = get_tag(fix, kTagSubType);
            std::string_view symbol = get_tag(fix, kTagSymbol);
            if (req_id.empty() || sub_type.empty() || symbol.empty()) {
                FixMessage rej{};
                if (build_md_reject(rej, session, req_id, 4, "MissingFields")) {
                    rej.session_id = session_id;
                    queue_fix_message(rej);
                }
                return false;
            }

            if (auto update_type = get_tag(fix, kTagUpdateType); !update_type.empty() && update_type != "1") {
                FixMessage rej{};
                if (build_md_reject(rej, session, req_id, 6, "UnsupportedMDUpdateType")) {
                    rej.session_id = session_id;
                    queue_fix_message(rej);
                }
                return false;
            }

            if (auto agg = get_tag(fix, kTagAggregated); !agg.empty() && agg != "N") {
                FixMessage rej{};
                if (build_md_reject(rej, session, req_id, 7, "UnsupportedAggregatedBook")) {
                    rej.session_id = session_id;
                    queue_fix_message(rej);
                }
                return false;
            }

            auto chan_it = channels_.find(std::string(symbol));
            if (chan_it == channels_.end()) {
                FixMessage rej{};
                if (build_md_reject(rej, session, req_id, 0, "UnknownSymbol")) {
                    rej.session_id = session_id;
                    queue_fix_message(rej);
                }
                return false;
            }

            if (sub_type == "1") {
                auto& sess_set = session_subs_[session_id];
                if (!sess_set.contains(std::string(symbol))) {
                    sess_set.insert(std::string(symbol));
                    symbol_subs_[std::string(symbol)].push_back(session_id);
                }

                DataRequest req{};
                req.session_id = session_id;
                req.request_id = ++request_id_;
                req.symbol_id = symbol_to_id_[std::string(symbol)];

                (void)req;

                FixMessage out{};
                if (!build_subscribe_response(out, session, req_id, symbol, chan_it->second)) {
                    return false;
                }
                out.session_id = session_id;
                queue_fix_message(out);

                return true;
            }
            if (sub_type == "2") {
                auto sess_it = session_subs_.find(session_id);
                if (sess_it != session_subs_.end()) {
                    sess_it->second.erase(std::string(symbol));
                }
                auto sym_it = symbol_subs_.find(std::string(symbol));
                if (sym_it != symbol_subs_.end()) {
                    auto& vec = sym_it->second;
                    vec.erase(std::remove(vec.begin(), vec.end(), session_id), vec.end());
                }
                return true;
            }

            FixMessage rej{};
            if (build_md_reject(rej, session, req_id, 4, "UnsupportedSubscriptionRequestType")) {
                rej.session_id = session_id;
                queue_fix_message(rej);
            }
            return false;
        }

        return false;
    }

    void MarketDataGateway::on_disconnect(uint64_t session_id) {
        auto sess_it = session_subs_.find(session_id);
        if (sess_it == session_subs_.end()) {
            sessions_.erase(session_id);
            return;
        }
        for (const auto& symbol : sess_it->second) {
            auto sym_it = symbol_subs_.find(symbol);
            if (sym_it == symbol_subs_.end()) {
                continue;
            }
            auto& vec = sym_it->second;
            vec.erase(std::remove(vec.begin(), vec.end(), session_id), vec.end());
        }
        session_subs_.erase(sess_it);
        sessions_.erase(session_id);
    }
}
