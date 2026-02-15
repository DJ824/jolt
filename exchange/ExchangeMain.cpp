//
// Created by djaiswal on 2/11/26.
//

#include "Exchange.h"

#include <array>
#include <atomic>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <xmmintrin.h>

namespace {
    std::atomic<bool> g_run{true};

    void on_signal(int) {
        g_run.store(false, std::memory_order_release);
    }
}

int main() {
    constexpr const char* kReqQ = "snapshot_req_q";
    std::unique_ptr<jolt::exchange::Exchange::RequestQ> req_q_owner;
    try {
        req_q_owner = std::make_unique<jolt::exchange::Exchange::RequestQ>(kReqQ, SharedRingMode::Create);
    } catch (...) {
        req_q_owner = std::make_unique<jolt::exchange::Exchange::RequestQ>(kReqQ, SharedRingMode::Attach);
    }

    constexpr jolt::ob::PriceTick kMinTick = 20'000;
    constexpr jolt::ob::PriceTick kMaxTick = 100'000;

    jolt::exchange::Exchange exchange(
        kMinTick,
        kMaxTick,
        "order_entry_q",
        "book_events_q",
        "order_ack_q",
        "exch_to_risk_q",
        "risk_to_exch_q",
        "snapshot_blob_pool",
        "snapshot_meta_q",
        kReqQ);


    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    exchange.start();
    while (g_run.load(std::memory_order_acquire)) {
        const bool did_work = exchange.poll_once();
        exchange.poll_requests();
        if (!did_work) {
            _mm_pause();
        }
    }
    exchange.stop();
    return 0;
}
