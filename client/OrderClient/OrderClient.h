#pragma once

#include "../FixClient.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace jolt::client {
    struct ClientConfig {
        std::string host{"127.0.0.1"};
        std::string port{"8080"};
        size_t clients{50};
        uint64_t total_orders{500'000};
        uint64_t orders_per_client_override{0};
        uint64_t qty{1};
        uint64_t base_price{60'000};
        uint64_t price_step{1};
        uint64_t send_interval_us{10};
        size_t poll_every{0};
        uint64_t final_drain_ms{2000};
        bool use_market_orders{false};
        uint64_t target_active_limit{10'000};
        uint64_t target_active_stop{1'000};
        double markov_stay_prob{0.72};
        double markov_reverse_prob{0.14};
        double pareto_alpha{1.7};
        double pareto_scale{1.0};
        bool stay_connected{false};
        std::vector<std::string> symbols{"1", "2", "3", "4"};
    };

    struct ClientStats {
        std::atomic<uint64_t>& connected_ok;
        std::atomic<uint64_t>& connected_fail;
        std::atomic<uint64_t>& logons_sent;
        std::atomic<uint64_t>& orders_sent;
        std::atomic<uint64_t>& send_fail;
        std::atomic<uint64_t>& poll_fail;
        std::atomic<uint64_t>& responses_recv;
    };

    class OrderClient {
    public:
        OrderClient(size_t client_idx,
                    const ClientConfig& cfg,
                    ClientStats& stats,
                    uint64_t target_limit_per_client,
                    uint64_t target_stop_per_client);

        void run(uint64_t orders_for_client);

    private:
        size_t client_idx_;
        const ClientConfig& cfg_;
        ClientStats& stats_;
        uint64_t target_limit_per_client_;
        uint64_t target_stop_per_client_;
        FixClient fix_{};
        std::mt19937_64 rng_;
        std::string id_{};
    };
}
