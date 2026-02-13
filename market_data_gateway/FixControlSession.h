#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <string_view>

#include "MarketDataTypes.h"

namespace jolt::md {
    static constexpr size_t kRxCap = 8192;
    static constexpr size_t kTxCap = 1024;

    class MarketDataGateway;

    class FixControlSession {
    public:
        struct Message {
            std::array<char, kFixMaxMsg> buf{};
            size_t len{0};
        };

        FixControlSession(const std::string& sender_comp_id, const std::string& target_comp_id, int fd);
        ~FixControlSession();

        void on_writable();
        void on_readable();
        bool send_pending();
        bool want_write() const;
        void queue_message(std::string_view message);
        void recv_pending();
        bool extract_message(std::string_view& msg);
        void close();

        std::string sender_comp_id_{};
        std::string target_comp_id_{};
        int fd_{-1};
        uint64_t session_id_{0};
        MarketDataGateway* gateway_{nullptr};

    private:
        std::array<char, kRxCap> rx_buf_{};
        std::deque<Message> tx_buf_{};
        size_t rx_len_{0};
        size_t rx_off_{0};
        size_t tx_off_{0};
        bool closed_{false};
    };
}
