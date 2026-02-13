#pragma once

#include <cstdint>
#include "ob_types.h"
#include "fifo.h"

namespace jolt::ob {
    struct OrderSlot {
        OrderId id{0};
        UserId owner{0};
        Qty og_qty{0};
        Qty remaining{0};
        uint64_t ts{0};
        PriceTick px{0};
    };

    struct StopSlot {
        OrderId id{0};
        UserId owner{0};
        Qty qty{0};
        PriceTick trigger{0};
        OrderType post_type{OrderType::StopMarket};
        PriceTick limit_px{0};
        TIF tif{TIF::GTC};
        uint64_t ts{0};
        OrderId parent_id{0};
    };

    struct TpSlot {
        OrderId id{0};
        UserId owner{0};
        Qty qty{0};
        PriceTick trigger{0};
        PriceTick limit_px{0};
        TIF tif{TIF::GTC};
        uint64_t ts{0};
        OrderId parent_id{0};
    };

    template <std::size_t BLOCK_K>
    struct Level {
        using OrderBlock = Block<OrderSlot, BLOCK_K>;
        using StopBlock = Block<StopSlot, BLOCK_K>;
        using TpBlock = Block<TpSlot, BLOCK_K>;

        explicit Level(BlockPool<OrderBlock>& ap, BlockPool<StopBlock>& sp, BlockPool<TpBlock>& tp)
            : order_fifo(ap), stop_fifo(sp), tp_fifo(tp) {
        }

        Fifo<OrderSlot, BLOCK_K> order_fifo;
        Fifo<StopSlot, BLOCK_K> stop_fifo;
        Fifo<TpSlot, BLOCK_K> tp_fifo;

        Qty active_qty{0};
        bool active_nonempty{false};
        bool stops_nonempty{false};
        bool tps_nonempty{false};
    };
}
