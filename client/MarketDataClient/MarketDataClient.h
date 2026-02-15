//
// Created by djaiswal on 2/15/26.
//

#pragma once

#include "../FixClient.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace jolt::client {
    struct MarketDataClientConfig {
        std::string host{"127.0.0.1"};
        std::string port{"80"};
        std::string sender_comp_id{"MD_CLIENT_1"};
        std::string target_comp_id{"MARKET_DATA_GATEWAY"};
        std::string symbol{"1"};
        std::string md_req_id{"1"};
        uint64_t logon_timeout_ms{2000};
        uint64_t subscribe_timeout_ms{2000};
        uint64_t udp_listen_ms{1000};
    };

    class MarketDataClient {
    public:
        explicit MarketDataClient(const MarketDataClientConfig& cfg);
        ~MarketDataClient();

        MarketDataClient(const MarketDataClient&) = delete;
        MarketDataClient& operator=(const MarketDataClient&) = delete;
        MarketDataClient(MarketDataClient&&) = delete;
        MarketDataClient& operator=(MarketDataClient&&) = delete;

        bool run();

    private:
        struct SubscribeEndpoints {
            std::string group{};
            uint16_t port{0};
            std::string recovery_host{};
            uint16_t recovery_port{0};
        };

        static std::string_view find_tag(std::string_view msg, std::string_view tag_with_eq);

        bool await_logon();
        bool send_subscribe();
        bool await_subscribe_response(SubscribeEndpoints& endpoints);
        bool connect_udp(const SubscribeEndpoints& endpoints);
        bool drain_udp(uint64_t listen_ms) const;
        void close_udp();

        const MarketDataClientConfig& cfg_;
        FixClient fix_{};
        int udp_fd_{-1};
    };
}
