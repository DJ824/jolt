#pragma once

#include <cstdint>
#include <limits>
#include <vector>

namespace jolt::ob {
    using PriceTick = uint32_t;
    using Qty = uint32_t;
    using OrderId = uint64_t;
    using UserId = uint64_t;
    using SessionId = uint64_t;

    enum class Side : uint8_t { Buy = 0, Sell = 1 };

    enum class TIF : uint8_t { GTC = 0, IOC = 1, FOK = 2 };

    enum class OrderType : uint8_t { Limit = 0, Market = 1, StopMarket = 2, StopLimit = 3, TakeProfit = 4 };

    enum class OrderAction : uint8_t { New = 0, Modify = 1, Cancel = 2 };

    enum class BookEventType : uint8_t { New = 0, Cancel = 1, Modify = 2, Trade, Fill = 3, Reject = 4 };

    struct OrderParams {
        OrderAction action{OrderAction::New};
        OrderType type{OrderType::Limit};
        OrderId id{};
        uint64_t client_id;
        OrderId tp_id{0};
        OrderId sl_id{0};
        uint64_t ts{};
        Qty qty{};
        PriceTick price{};
        PriceTick trigger{};
        PriceTick limit_px{};
        PriceTick sl_trigger{0};
        PriceTick sl_limit_px{0};
        PriceTick tp_trigger{0};
        PriceTick tp_limit_px{0};
        TIF tif{TIF::GTC};
        TIF sl_tif{TIF::GTC};
        TIF tp_tif{TIF::GTC};
        Side side{Side::Buy};
        OrderType sl_post_type{OrderType::StopMarket};
    };


    enum class RejectReason : uint8_t {
        NotApplicable = 0,
        InvalidQty = 1,
        InvalidPrice = 2,
        NonExistent = 3,
        TifExpired = 4,
        NotFillable = 5,
        InvalidType = 6
    };

    struct Bbo {
        PriceTick best_bid;
        PriceTick best_ask;

        explicit Bbo(PriceTick bid, PriceTick ask) : best_bid(bid), best_ask(ask) {
        }
    };

    struct BookEvent {
        OrderId id{0};
        uint64_t ts{0};
        uint64_t seq{0};
        Qty qty{0};
        PriceTick price{0};
        Side side{};
        BookEventType event_type{};
        RejectReason reason;
    };

    struct MatchResult {
        std::vector<BookEvent> fills{1024};
        uint64_t filled{0};
        Qty qty{0};
        PriceTick last_px{0};

        void reset() {
            filled = 0;
            last_px = 0;
            qty = 0;
            fills.clear();
        }
    };

    struct L3Data {
        OrderId id{0};
        uint64_t ts{0};
        uint64_t seq{0};
        Qty qty{0};
        PriceTick price{0};
        uint16_t symbol_id{0};
        Side side{Side::Buy};
        BookEventType event_type{BookEventType::New};
    };

    struct SnapshotOrder {
        uint64_t id;
        uint32_t qty;
        uint32_t px;
        Side side;
    };

    struct BookSnapshot {
        std::vector<SnapshotOrder> orders;
        uint64_t symbol_id;
        uint64_t seq{};
        size_t bid_ct;
        size_t ask_ct;

    };
}
