//
// Created by djaiswal on 1/29/26.
//

#include "FixGateway.h"

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace {
    std::atomic<bool> g_run{true};

    void on_signal(int) {
        g_run.store(false, std::memory_order_release);
    }

    bool parse_u64(const std::string& s, uint64_t& out) {
        if (s.empty()) {
            return false;
        }
        const char* first = s.data();
        const char* last = s.data() + s.size();
        auto [ptr, ec] = std::from_chars(first, last, out);
        return ec == std::errc{} && ptr == last;
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

    uint64_t to_client_id(const std::string& account) {
        uint64_t numeric = 0;
        if (parse_u64(account, numeric)) {
            return numeric;
        }
        return fnv1a_64(account);
    }
}

int main() {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    jolt::gateway::FixGateway gateway("order_entry_q", "order_ack_q");

    std::vector<jolt::ClientInfo> clients;
    clients.reserve(1024);
    for (size_t i = 1; i <= 1024; ++i) {
        const std::string account = "CLIENT_" + std::to_string(i);
        jolt::ClientInfo info{};
        info.client_id = to_client_id(account);
        info.max_qty = 1'000'000;
        info.max_open_orders = 1'000'000;
        info.open_orders = 0;
        info.max_pos = std::numeric_limits<int64_t>::max() / 4;
        info.net_pos = 0;
        info.max_notional = std::numeric_limits<int64_t>::max() / 4;
        info.capital = 1e9f;
        clients.push_back(info);
    }
    gateway.load_clients(clients);

    gateway.start();

    while (g_run.load(std::memory_order_acquire)) {
        gateway.poll();
        // std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    gateway.stop();
    return 0;
}
