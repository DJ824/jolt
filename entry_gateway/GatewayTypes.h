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
    static constexpr size_t kOrderStateTextMaxLen = 64;
    static constexpr size_t kOrderStateTextBufLen = kOrderStateTextMaxLen + 1;

    enum class State : uint8_t {PendingNew = 0, New = 1, PendingCancel = 2, Cancelled = 3, PendingReplace = 4, Replaced = 5, Filled = 6, Rejected = 7};

    struct alignas(64) FixMessage {
        char data[kFixMaxMsg];
        uint64_t session_id{0};
        size_t len{0};
    };

    struct OrderState {
        std::array<char, kOrderStateTextBufLen> cl_ord_id{};
        std::array<char, kOrderStateTextBufLen> orig_cl_ord_id{};
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

    enum class IngressKind : uint8_t { ClientFix = 0, ExchMsg = 1, Disconnect = 2};

    struct ClientFixMsg {
        uint64_t session_id;
        uint64_t rx_ts_nsl;
        uint32_t slot_id;
        uint16_t len;
    };

    struct IngressEvent {
        IngressKind kind;
        union {
            ClientFixMsg client_fix_msg;
            ExchToGtwyMsg exch_msg;
            uint64_t diconnected_sess_id;
        };
    };

    enum class EgressKind : uint8_t { ToExch = 0, ToClient = 1, CloseSession = 2 };

    struct ToClientMsg {
        FixMessage fix;
    };

    struct EgressEvent {
        EgressKind kind;
        union {
            GtwyToExchMsg to_exch;
            ToClientMsg to_client;
            uint64_t close_session_id;
        };
    };


    struct SocketEvent {
        enum class Kind : uint8_t {Disconnect = 0};
        uint64_t session_id;
    };







}
