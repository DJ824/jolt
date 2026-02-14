//
// Created by djaiswal on 1/25/26.
//
#pragma once
#include <array>
#include <cstdint>
#include <string>
#include "../include/Types.h"
#include "../include/SharedMemoryRing.h"

namespace jolt {

    using GtwyToExch = SharedSpscQueue<GtwyToExchMsg, 1 << 20>;
    using ExchToGtwy = SharedSpscQueue<ExchToGtwyMsg, 1 << 20>;

    static constexpr size_t kFixMaxMsg = 1024;

    enum class State : uint8_t {PendingNew = 0, New = 1, PendingCancel = 2, Cancelled = 3, PendingReplace = 4, Replaced = 5, Filled = 6, Rejected = 7};

    struct FixMessage {
        std::array<char, kFixMaxMsg> data{};
        size_t len{0};
        uint64_t session_id{0};
    };

    struct OrderState {
        ob::OrderAction action{ob::OrderAction::New};
        std::string cl_ord_id;
        std::string orig_cl_ord_id;
        std::string symbol;
        uint64_t order_id{0};
        ob::OrderParams params{};
        uint64_t session_id{0};
        State state{State::PendingNew};
    };

    struct SessionState {
        std::string sender_comp_id{};
        std::string target_comp_id{};
        uint64_t session_id{0};
        uint64_t seq{1};
        bool logged_on{false};
        bool initialized{false};

        SessionState() = default;

        explicit SessionState(uint64_t n) {
            reset(n);
        }

        void reset(uint64_t n) {
            sender_comp_id.clear();
            target_comp_id.clear();
            session_id = n;
            seq = 1;
            logged_on = false;
            initialized = true;
        }
    };


}
