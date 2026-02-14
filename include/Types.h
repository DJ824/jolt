#pragma once
#include <array>
#include <cstddef>
#include <cstdint>

#include "../exchange/orderbook/ob_types.h"

namespace jolt {
    inline constexpr uint16_t kFirstSymbolId = 1;
    inline constexpr size_t kNumSymbols = 4;
    inline constexpr uint16_t kLastSymbolId = static_cast<uint16_t>(kFirstSymbolId + kNumSymbols - 1);

    inline constexpr bool is_valid_symbol_id(const uint64_t symbol_id) {
        return symbol_id >= kFirstSymbolId && symbol_id <= kLastSymbolId;
    }

    using Side = ob::Side;

    struct Order {
        uint64_t order_id;
        uint64_t client_id;
        uint32_t price;
        uint32_t sz;
        uint32_t remaining_sz;
        Side side;
    };

    struct ExchToGtwyMsg {
        enum class Type : uint8_t {Submitted = 0, Rejected = 1, Filled = 2};
        uint64_t client_id;
        uint64_t order_id;
        size_t fill_qty;
        ob::RejectReason reason;
        Type type;
        bool filled;
    };

    struct GtwyToExchMsg {
        ob::OrderParams order{};
        uint64_t client_id{0};
    };

    struct ExchangeToRiskMsg {
        ob::OrderParams order{};
        std::array<ob::BookEvent, 1024> fill_events_{};
        uint64_t ts{0};
        uint64_t num_fills{0};
        uint32_t bid{0};
        uint32_t ask{0};
    };

    struct RiskToExchMsg {
        ob::OrderParams order;
        uint64_t ts;
    };

    struct ClientInfo {
        uint64_t client_id;
        uint64_t max_qty;
        uint64_t max_open_orders;
        uint64_t open_orders;
        int64_t max_pos;
        int64_t net_pos;
        int64_t max_notional;
        float capital;
    };

    struct FillEvent {
        uint64_t ts;
        uint64_t maker_id;
        uint64_t taker_id;
        uint32_t px;
        uint32_t sz;
        Side maker_side;
    };

    struct SnapshotChunk {
        uint64_t request_id;
        uint32_t chunk_idx;
        uint32_t chunk_ct;
        uint16_t symbol_id;
        std::array<std::byte,4096> chunk;
    };

    struct L3DiskRecord {
        uint64_t seq;
        uint64_t ts;
        uint64_t id;
        uint32_t qty;
        uint32_t price;
        uint16_t symbol_id;
        uint8_t side;
        uint8_t event_type;
    };
}
