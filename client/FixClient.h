#pragma once
#include <array>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace jolt::client {
    class FixClient {
        int fd_{-1};
        std::deque<std::string> inbound_{};
        std::string last_message_{};
        std::string sender_comp_id_{};
        std::string target_comp_id_{};
        std::string account_{};
        uint64_t next_out_seq_{1};
        uint64_t cl_ord_seq_{1};
        std::array<char, 1024> buffer_{};
        size_t msg_len_{0};
        bool append_bytes(const char* p, size_t n);
        bool append_tag(int tag, std::string_view val);
        std::array<char, 1024> recv_buf_;
        size_t recv_len_{0};
        size_t recv_off_{0};

        bool read_socket();
        bool extract_message(std::string& out);
        std::string_view build_fix_message(std::string_view msg_type,
                                           const std::vector<std::pair<int, std::string>>& fields);

    public:
        FixClient() = default;
        ~FixClient();

        FixClient(const FixClient&) = delete;
        FixClient& operator=(const FixClient&) = delete;

        bool connect_tcp(const std::string& host, const std::string& port);
        void disconnect();
        bool is_connected() const;

        void set_session(const std::string& sender_comp_id, const std::string& target_comp_id);
        void set_account(const std::string& account);
        std::string next_cl_ord_id();

        bool send_raw(std::string_view msg);
        bool send_message(std::string_view msg_type, const std::vector<std::pair<int, std::string>>& fields);

        std::string_view build_logon(int heartbeat_seconds = 30);
        std::string_view build_logout();
        std::string_view build_heartbeat();
        std::string_view build_new_order_limit(std::string_view cl_ord_id,
                                               std::string_view symbol,
                                               bool is_buy,
                                               uint64_t qty,
                                               uint64_t price,
                                               int tif = 1);
        std::string_view build_new_order_market(std::string_view cl_ord_id,
                                                std::string_view symbol,
                                                bool is_buy,
                                                uint64_t qty,
                                                int tif = 3);
        std::string_view build_new_order_stop(std::string_view cl_ord_id,
                                              std::string_view symbol,
                                              bool is_buy,
                                              uint64_t qty,
                                              uint64_t stop_px,
                                              int tif = 1);
        std::string_view build_new_order_stop_limit(std::string_view cl_ord_id,
                                                    std::string_view symbol,
                                                    bool is_buy,
                                                    uint64_t qty,
                                                    uint64_t stop_px,
                                                    uint64_t limit_px,
                                                    int tif = 1);
        std::string_view build_cancel(std::string_view cl_ord_id,
                                      std::string_view orig_cl_ord_id,
                                      std::string_view symbol,
                                      bool is_buy);
        std::string_view build_replace(std::string_view cl_ord_id,
                                       std::string_view orig_cl_ord_id,
                                       std::string_view symbol,
                                       bool is_buy,
                                       uint64_t qty,
                                       uint64_t price,
                                       int tif = 1);
        bool build_snapshot_request(const std::string& host,
                                    const std::string& port,
                                    uint64_t session_id,
                                    uint64_t symbol_id,
                                    uint64_t request_id);
        bool build_udp_request(const std::string& host,
                               const std::string& port,
                               uint64_t session_id,
                               uint64_t symbol_id,
                               uint64_t request_id);

        bool poll();
        std::optional<std::string_view> next_message();
    };
}
