#pragma once
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <functional>
#include <optional>
#include <limits>
#include <cassert>
#include <chrono>
#include <queue>

#include "ob_types.h"
#include "block_pool.h"
#include "level.h"
#include "level_pool.h"
#include "flat_map.h"
#include "ob_types.h"

namespace jolt::ob {

    struct Order {
        uint64_t id;
        uint32_t price;
        uint32_t sz;
        Side side;
    };

    template <std::size_t BLOCK_K = 128>
    class MatchingOrderBook {
    public:
        using ActiveBlock = Block<OrderSlot, BLOCK_K>;
        using StopBlock = Block<StopSlot, BLOCK_K>;
        using TpBlock = Block<TpSlot, BLOCK_K>;
        using LevelT = Level<BLOCK_K>;

        // represents order location
        struct Locator {
            LevelT* level{nullptr};
            void* blk{nullptr};
            uint16_t off{0};

            enum class Kind : uint8_t { Active = 0, Stop = 1, TakeProfit = 2 };

            Kind kind{Kind::Active};
            Side side{Side::Buy};
            PriceTick price{};
        };

        MatchingOrderBook(PriceTick min_tick, PriceTick max_tick)
            : min_tick_(min_tick), max_tick_(max_tick), range_(static_cast<std::size_t>(max_tick - min_tick + 1)),
              bids_(range_, nullptr), asks_(range_, nullptr) {
            locators_.reserve(1 << 20);
        }

        BookEvent submit_order(const OrderParams& p) {
            ++seq;
            match_result.reset();
            switch (p.action) {
            case OrderAction::New:
                switch (p.type) {
                case OrderType::Limit:
                    return submit_limit(p);
                case OrderType::Market:
                    return submit_market(p);
                case OrderType::StopMarket:
                case OrderType::StopLimit:
                    return submit_stop(p);
                case OrderType::TakeProfit:
                    return submit_take_profit(p);
                }
                break;
            case OrderAction::Modify:
                return modify(p);
            case OrderAction::Cancel:
                return cancel(p);
            }
            return make_reject(p.id, RejectReason::InvalidType, p.ts);
        }


        PriceTick best_bid() const {
            if (best_buy_idx_ == npos) {
                return 0;
            }
            return price_from_bid_index(best_buy_idx_);
        }

        PriceTick best_ask() const {
            if (best_ask_idx_ == npos) {
                return 0;
            }
            return price_from_ask_index(best_ask_idx_);
        }

        Qty level_active_qty(Side side, PriceTick px) const {
            LevelT* lvl = level_of(side, px, false);
            if (!lvl || !lvl->active_nonempty) {
                return 0;
            }
            return lvl->active_qty;
        }

        std::size_t level_order_count(Side side, PriceTick px) const {
            LevelT* lvl = level_of(side, px, false);
            if (!lvl) {
                return 0;
            }
            return lvl->order_fifo.live_count();
        }

        OrderId level_head_order_id(Side side, PriceTick px) const {
            LevelT* lvl = level_of(side, px, false);
            if (!lvl) {
                return 0;
            }
            auto* head = const_cast<LevelT*>(lvl)->order_fifo.head_slot();
            return head ? head->id : 0;
        }

        Qty order_qty(OrderId id) const {
            auto* lptr = locators_.find(id);
            if (!lptr) {
                return 0;
            }
            const Locator loc = *lptr;
            if (loc.kind == Locator::Kind::Stop) {
                auto* blk = reinterpret_cast<typename LevelT::StopBlock*>(loc.blk);
                return blk->slots[loc.off].qty;
            }
            if (loc.kind == Locator::Kind::TakeProfit) {
                auto* blk = reinterpret_cast<typename LevelT::TpBlock*>(loc.blk);
                return blk->slots[loc.off].qty;
            }
            auto* blk = reinterpret_cast<typename LevelT::OrderBlock*>(loc.blk);
            return blk->slots[loc.off].remaining;
        }

        PriceTick get_best_ask() {
            return price_from_ask_index(best_ask_idx_);
        }

        PriceTick get_best_bid() {
            return price_from_bid_index(best_buy_idx_);
        }

        std::size_t active_limit_order_count() const { return active_limit_orders_; }
        std::size_t active_stop_order_count() const { return active_stop_orders_; }
        std::size_t active_limit_buy_count() const { return active_limit_buys_; }
        std::size_t active_limit_sell_count() const { return active_limit_sells_; }

        MatchResult match_result;
        uint64_t seq{0};


        void get_snapshot(BookSnapshot& out) {
            out.orders.clear();
            out.bid_ct = 0;
            out.ask_ct = 0;

            for (auto l : bids_) {
                if (l && l->active_nonempty) {
                    l->order_fifo.copy_live([&](const OrderSlot& slot) -> void {
                       out.orders.push_back(SnapshotOrder{slot.id, slot.remaining, slot.px, Side::Buy});
                       ++out.bid_ct;
                   });
                }
            }

            for (auto l : asks_) {
                if (l && l->active_nonempty) {
                    l->order_fifo.copy_live([&](const OrderSlot& slot) -> void {
                        out.orders.push_back(SnapshotOrder{slot.id, slot.remaining, slot.px, Side::Sell});
                        ++out.ask_ct;
                    });
                }
            }
            out.seq = seq;
        }

    private:

        uint16_t symbol_id_{0};
        PriceTick min_tick_{};
        PriceTick max_tick_{};
        std::size_t range_{};

        mutable std::vector<LevelT*> bids_{};
        mutable std::vector<LevelT*> asks_{};

        mutable BlockPool<ActiveBlock> active_pool_{};
        mutable BlockPool<StopBlock> stop_pool_{};
        mutable BlockPool<TpBlock> tp_pool_{};
        mutable LevelPool<LevelT> level_pool_{};

        FlatMap<OrderId, Locator> locators_{1 << 20};
        FlatMap<OrderId, Locator> stop_locators_{1 << 20};
        FlatMap<OrderId, Locator> tp_locators_{1 << 20};

        PriceTick last_trade_{0};
        PriceTick prev_trade_{0};

        static constexpr std::size_t npos = std::numeric_limits<std::size_t>::max();
        std::size_t best_buy_idx_{npos};
        std::size_t best_ask_idx_{npos};
        std::size_t active_limit_orders_{0};
        std::size_t active_stop_orders_{0};
        std::size_t active_limit_buys_{0};
        std::size_t active_limit_sells_{0};


        // updates best bid/ask idxs when a price level is added to the book
        inline void on_level_set(Side s, std::size_t idx) {
            if (s == Side::Buy) {
                if (best_buy_idx_ == npos || idx < best_buy_idx_) {
                    best_buy_idx_ = idx;
                }
            }
            else {
                if (best_ask_idx_ == npos || idx < best_ask_idx_) {
                    best_ask_idx_ = idx;
                }
            }
        }

        // updates best bid/ask idxs when a price level is cleared from the book
        inline void on_level_clear(Side s, std::size_t idx) {
            if (s == Side::Buy) {
                if (best_buy_idx_ != idx) {
                    return;
                }
                for (std::size_t i = idx + 1; i < range_; ++i) {
                    LevelT* lvl = bids_[i];
                    if (lvl && lvl->active_nonempty && lvl->order_fifo.live_count() > 0) {
                        best_buy_idx_ = i;
                        return;
                    }
                }
                best_buy_idx_ = npos;
                return;
            }
            if (best_ask_idx_ != idx) {
                return;
            }
            for (std::size_t i = idx + 1; i < range_; ++i) {
                LevelT* lvl = asks_[i];
                if (lvl && lvl->active_nonempty && lvl->order_fifo.live_count() > 0) {
                    best_ask_idx_ = i;
                    return;
                }
            }
            best_ask_idx_ = npos;
        }

        // submit a new limit order in the book
        BookEvent submit_limit(const OrderParams& p) {
            if (p.qty <= 0) {
                return make_reject(p.id, RejectReason::InvalidQty, p.ts);
            }

            Qty remaining = p.qty;
            if (crosses(p.side, p.price)) {
                match_aggressive(p.id, p.side, p.price, remaining, p.ts, true);
                if (remaining > match_result.qty) {
                    remaining -= match_result.qty;
                }
                else {
                    remaining = 0;
                }
                if (remaining == 0) {
                    return make_fill(p.id, p.side, match_result.last_px, p.qty, p.ts);
                }

                if (p.tif == TIF::IOC) {
                    if (match_result.qty > 0) {
                        return make_fill(p.id, p.side, match_result.last_px, match_result.qty, p.ts);
                    }
                    return make_reject(p.id, RejectReason::NotFillable, p.ts);
                }

                // reject fok if not fully filled when crossed
                if (p.tif == TIF::FOK) {
                    return make_reject(p.id, RejectReason::NotFillable, p.ts);
                }
            }

            if (p.tif == TIF::IOC || p.tif == TIF::FOK) {
                return make_reject(p.id, RejectReason::NotFillable, p.ts);
            }

            // get the price level of the order
            LevelT* lvl = level_of(p.side, p.price, true);
            // create an order slot (fix this later to avoid allocation), and append the order
            auto loc = lvl->order_fifo.emplace(p.id, static_cast<UserId>(0), remaining, remaining, p.ts, p.price);
            lvl->active_qty += remaining;
            lvl->active_nonempty = true;
            // maintain best pointers via side-aware index mapping
            on_level_set(p.side, side_index(p.side, p.price));
            // insert locator into lookup (fix later to avoid allocation)
            locators_.insert(p.id, Locator{lvl, loc.blk, loc.off, Locator::Kind::Active, p.side, p.price});
            ++active_limit_orders_;


            if (p.side == Side::Buy) {
                ++active_limit_buys_;
            }
            else {
                ++active_limit_sells_;
            }

            BookEvent e = make_new(p.id, p.side, p.price, remaining, p.ts);

            if (p.sl_id != 0 && p.sl_trigger != 0) {
                OrderParams sp{};
                sp.action = OrderAction::New;
                sp.type = p.sl_post_type;
                sp.id = p.sl_id;
                sp.side = opposite(p.side);
                sp.trigger = p.sl_trigger;
                sp.limit_px = p.sl_limit_px;
                sp.tif = p.sl_tif;
                sp.qty = remaining;
                sp.ts = p.ts;
                submit_stop(sp);
            }

            if (p.tp_id != 0 && p.tp_trigger != 0 && p.tp_limit_px != 0) {
                OrderParams tp{};
                tp.action = OrderAction::New;
                tp.type = OrderType::TakeProfit;
                tp.id = p.tp_id;
                tp.side = opposite(p.side);
                tp.trigger = p.tp_trigger;
                tp.limit_px = p.tp_limit_px;
                tp.tif = p.tp_tif;
                tp.qty = remaining;
                tp.ts = p.ts;
                submit_take_profit(tp);
            }
            return e;
        }

        // submits a market order
        BookEvent submit_market(const OrderParams& p) {
            if (p.qty <= 0) {
                return make_reject(p.id, RejectReason::InvalidQty, p.ts);
            }

            match_aggressive(
                p.id,
                p.side,
                (p.side == Side::Buy ? max_tick_ : min_tick_),
                p.qty,
                p.ts,
                true
            );

            if (match_result.fill_count == 0) {
                return make_reject(p.id, RejectReason::NotFillable, p.ts);
            }

            if (p.sl_id != 0 && p.sl_trigger != 0) {
                OrderParams sp{};
                sp.action = OrderAction::New;
                sp.type = p.sl_post_type;
                sp.id = p.sl_id;
                sp.side = opposite(p.side);
                sp.trigger = p.sl_trigger;
                sp.limit_px = p.sl_limit_px;
                sp.tif = p.sl_tif;
                sp.qty = p.qty;
                sp.ts = p.ts;
                submit_stop(sp);
            }
            if (p.tp_id != 0 && p.tp_trigger != 0 && p.tp_limit_px != 0) {
                OrderParams tp{};
                tp.action = OrderAction::New;
                tp.type = OrderType::TakeProfit;
                tp.id = p.tp_id;
                tp.side = opposite(p.side);
                tp.trigger = p.tp_trigger;
                tp.limit_px = p.tp_limit_px;
                tp.tif = p.tp_tif;
                tp.qty = p.qty;
                tp.ts = p.ts;
                submit_take_profit(tp);
            }
            return make_fill(p.id, p.side, match_result.last_px, match_result.qty, p.ts);
        }

        BookEvent cancel(const OrderParams& p) {
            if (cancel(p.id)) {
                BookEvent e = {};
                e.event_type = BookEventType::Cancel;
                e.id = p.id;
                e.price = p.price;
                e.qty = p.qty;
                e.ts = p.ts;
                return e;
            }
            return make_reject(p.id, RejectReason::NonExistent, p.ts);
        }


        BookEvent modify(const OrderParams& p) {
            if (modify(p.id, p.qty, p.price, p.tif, p.ts)) {
                BookEvent e = {};
                e.event_type = BookEventType::Modify;
                e.id = p.id;
                e.price = p.price;
                e.qty = p.qty;
                e.ts = p.ts;
                return e;
            }
            return make_reject(p.id, RejectReason::NonExistent, p.ts);
        }

        // cancels a resting order in the book (limit and stop)
        bool cancel(OrderId id) {
            // get the location of the order
            auto* lptr = locators_.find(id);
            if (!lptr) {
                return false;
            }

            Locator loc = *lptr;

            // if stop order, get the block that holds the stop, tombstone the slot
            if (loc.kind == Locator::Kind::Stop) {
                auto* blk = reinterpret_cast<typename LevelT::StopBlock*>(loc.blk);
                loc.level->stop_fifo.tombstone({blk, loc.off});

                if (loc.level->stop_fifo.live_count() == 0) {
                    loc.level->stops_nonempty = false;
                }
                if (active_stop_orders_ > 0) {
                    --active_stop_orders_;
                }
            }
            else if (loc.kind == Locator::Kind::TakeProfit) {
                auto* blk = reinterpret_cast<typename LevelT::TpBlock*>(loc.blk);
                loc.level->tp_fifo.tombstone({blk, loc.off});
                if (loc.level->tp_fifo.live_count() == 0) {
                    loc.level->tps_nonempty = false;
                }
            }
            else {
                auto* blk = reinterpret_cast<typename LevelT::OrderBlock*>(loc.blk);
                // get quantity of order to be canceled, and subtract from level
                Qty q = blk->slots[loc.off].remaining;
                if (loc.level->active_qty >= q) {
                    loc.level->active_qty -= q;
                }
                else {
                    loc.level->active_qty = 0;
                }
                // tombstone the slot, and if last order at level, update best idx
                loc.level->order_fifo.tombstone({blk, loc.off});
                if (loc.level->order_fifo.live_count() == 0) {
                    loc.level->active_nonempty = false;
                    loc.level->active_qty = 0;
                    auto idx = side_index(loc.side, loc.price);
                    on_level_clear(loc.side, idx);
                }
                if (active_limit_orders_ > 0) {
                    --active_limit_orders_;
                }
                if (loc.side == Side::Buy) {
                    if (active_limit_buys_ > 0) {
                        --active_limit_buys_;
                    }
                }
                else {
                    if (active_limit_sells_ > 0) {
                        --active_limit_sells_;
                    }
                }
            }

            locators_.erase(id);
            return true;
        }

        // submit an order modification request
        bool modify(OrderId id,
                    Qty new_qty,
                    PriceTick new_px,
                    TIF tif = TIF::GTC,
                    uint64_t ts = 0) {
            // get the order to be modified
            auto* lptr = locators_.find(id);

            if (!lptr) {
                return false;
            }

            Locator loc = *lptr;
            if (loc.kind == Locator::Kind::Stop) {
                auto* stop_block = reinterpret_cast<typename LevelT::StopBlock*>(loc.blk);
                StopSlot og_stop = stop_block->slots[loc.off];
                Qty old_qty = stop_block->slots[loc.off].qty;

                // if new qty == 0, cancel the order
                if (new_qty == 0) {
                    loc.level->stop_fifo.tombstone({stop_block, loc.off});
                    if (loc.level->stop_fifo.live_count() == 0) {
                        loc.level->stops_nonempty = false;
                    }
                    locators_.erase(id);
                    if (active_stop_orders_ > 0) {
                        --active_stop_orders_;
                    }
                    return true;
                }

                auto old_px = stop_block->slots[loc.off].trigger;

                // if price change or qty increase, requeue the order
                if (new_px != old_px || new_qty > old_qty) {
                    // remove from old price level
                    loc.level->stop_fifo.tombstone({stop_block, loc.off});
                    locators_.erase(id);
                    if (loc.level->stop_fifo.live_count() == 0) {
                        loc.level->stops_nonempty = false;
                    }

                    // insert into new price level
                    og_stop.qty = new_qty;
                    og_stop.trigger = new_px;
                    auto new_lvl = level_of(loc.side, new_px, true);

                    auto new_loc = new_lvl->stop_fifo.append(og_stop);
                    new_lvl->stops_nonempty = true;
                    locators_.insert(id, Locator{
                                         new_lvl, new_loc.blk, new_loc.off, Locator::Kind::Stop, loc.side, new_px
                                     });
                    return true;
                }

                // if qty decrease, adjust quantity of order and level
                if (new_qty < old_qty) {
                    stop_block->slots[loc.off].qty = new_qty;
                    return true;
                }
                return true;
            }
            if (loc.kind == Locator::Kind::TakeProfit) {
                auto* tp_block = reinterpret_cast<typename LevelT::TpBlock*>(loc.blk);
                TpSlot og_tp = tp_block->slots[loc.off];
                Qty old_qty = tp_block->slots[loc.off].qty;

                if (new_qty == 0) {
                    loc.level->tp_fifo.tombstone({tp_block, loc.off});
                    if (loc.level->tp_fifo.live_count() == 0) {
                        loc.level->tps_nonempty = false;
                    }
                    locators_.erase(id);
                    return true;
                }

                auto old_px = tp_block->slots[loc.off].trigger;
                if (new_px != old_px || new_qty > old_qty) {
                    loc.level->tp_fifo.tombstone({tp_block, loc.off});
                    locators_.erase(id);
                    if (loc.level->tp_fifo.live_count() == 0) {
                        loc.level->tps_nonempty = false;
                    }

                    og_tp.qty = new_qty;
                    og_tp.trigger = new_px;
                    auto new_lvl = level_of(loc.side, new_px, true);
                    auto new_loc = new_lvl->tp_fifo.append(og_tp);
                    new_lvl->tps_nonempty = true;
                    locators_.insert(id, Locator{
                                         new_lvl, new_loc.blk, new_loc.off, Locator::Kind::TakeProfit, loc.side, new_px
                                     });
                    return true;
                }

                if (new_qty < old_qty) {
                    tp_block->slots[loc.off].qty = new_qty;
                    return true;
                }
                return true;
            }
            // get the og order
            auto* order_block = reinterpret_cast<typename LevelT::OrderBlock*>(loc.blk);
            OrderSlot og_order = order_block->slots[loc.off];
            auto old_qty = og_order.remaining;
            auto old_px = og_order.px;

            // fifo and level for og order
            auto og_lvl = loc.level;
            // if new px == 0 or new qty == 0, treat as order cancel
            if (new_px == 0 || new_qty == 0) {
                og_lvl->order_fifo.tombstone({order_block, loc.off});
                og_lvl->active_qty -= old_qty;
                if (og_lvl->active_qty == 0) {
                    og_lvl->active_nonempty = false;
                    auto old_idx = side_index(loc.side, old_px);
                    on_level_clear(loc.side, old_idx);
                }
                locators_.erase(id);
                if (active_limit_orders_ > 0) {
                    --active_limit_orders_;
                }
                if (loc.side == Side::Buy) {
                    if (active_limit_buys_ > 0) {
                        --active_limit_buys_;
                    }
                }
                else {
                    if (active_limit_sells_ > 0) {
                        --active_limit_sells_;
                    }
                }
                return true;
            }

            // requeue the order if price change or qty increase
            if (new_px != old_px || new_qty > old_qty) {
                // remove from old level
                og_lvl->active_qty -= old_qty;
                og_lvl->order_fifo.tombstone({order_block, loc.off});
                locators_.erase(id);
                if (og_lvl->active_qty == 0) {
                    og_lvl->active_nonempty = false;
                    auto old_idx = side_index(loc.side, old_px);
                    on_level_clear(loc.side, old_idx);
                }

                Qty remaining = new_qty;

                // match orders if the new price crosses the book
                if (crosses(loc.side, new_px)) {
                    match_aggressive(id, loc.side, new_px, remaining, ts, true);
                    if (remaining > match_result.qty) {
                        remaining -= match_result.qty;
                    }
                    else {
                        remaining = 0;
                    }
                }

                if (remaining > 0) {
                    og_order.px = new_px;
                    og_order.remaining = remaining;

                    auto new_lvl = level_of(loc.side, new_px, true);
                    new_lvl->active_qty += remaining;
                    auto new_loc = new_lvl->order_fifo.append(og_order);
                    new_lvl->active_nonempty = true;
                    auto new_idx = side_index(loc.side, new_px);
                    on_level_set(loc.side, new_idx);
                    locators_.erase(id);
                    locators_.insert(id, Locator{
                                         new_lvl, new_loc.blk, new_loc.off, Locator::Kind::Active, loc.side, new_px
                                     });
                }
                else {
                    locators_.erase(id);
                    if (active_limit_orders_ > 0) {
                        --active_limit_orders_;
                    }
                    if (loc.side == Side::Buy) {
                        if (active_limit_buys_ > 0) {
                            --active_limit_buys_;
                        }
                    }
                    else {
                        if (active_limit_sells_ > 0) {
                            --active_limit_sells_;
                        }
                    }
                }

                return true;
            }

            // if qty decrease, adjust
            if (new_qty < old_qty) {
                og_lvl->active_qty -= old_qty;
                og_lvl->active_qty += new_qty;
                order_block->slots[loc.off].remaining = new_qty;

                return true;
            }


            return true;
        }




        void on_trade_last(PriceTick p, uint64_t ts) {
            if (last_trade_ == 0) {
                last_trade_ = p;
                prev_trade_ = p;
            }
            prev_trade_ = last_trade_;
            last_trade_ = p;
            if (p > prev_trade_) {
                drain_stops_range(Side::Buy, prev_trade_ + 1, p, ts);
                drain_tps_range(Side::Sell, prev_trade_ + 1, p, ts);
            }
            else if (p < prev_trade_) {
                drain_stops_range(Side::Sell, p, prev_trade_ - 1, ts);
                drain_tps_range(Side::Buy, p, prev_trade_ - 1, ts);
            }
        }


        const std::vector<LevelT*>& inspect_bids() const { return bids_; }
        const std::vector<LevelT*>& inspect_asks() const { return asks_; }
        std::size_t inspect_index(Side side, PriceTick px) const { return side_index(side, px); }
        PriceTick inspect_price_from_bid_index(std::size_t i) const { return price_from_bid_index(i); }
        PriceTick inspect_price_from_ask_index(std::size_t i) const { return price_from_ask_index(i); }

        // submits a stop order into the book, setting the price level in stop bitmap
        BookEvent submit_stop(const OrderParams& p) {
            if (p.qty <= 0) {
                return make_reject(p.id, RejectReason::InvalidQty, p.ts);
            }
            OrderType post_type =
                (p.type == OrderType::StopLimit) ? OrderType::StopLimit : OrderType::StopMarket;
            LevelT* lvl = level_of(p.side, p.trigger, true);
            auto loc = lvl->stop_fifo.emplace(
                p.id,
                static_cast<UserId>(0),
                p.qty,
                p.trigger,
                post_type,
                p.limit_px,
                p.tif,
                p.ts
            );
            lvl->stops_nonempty = true;
            locators_.insert(p.id, Locator{lvl, loc.blk, loc.off, Locator::Kind::Stop, p.side, p.trigger});
            ++active_stop_orders_;
            return make_new(p.id, p.side, p.trigger, p.qty, p.ts);
        }

        // submits a take-profit order (limit posted when triggered)
        BookEvent submit_take_profit(const OrderParams& p) {
            if (p.qty <= 0) {
                return make_reject(p.id, RejectReason::InvalidQty, p.ts);
            }
            LevelT* lvl = level_of(p.side, p.trigger, true);
            auto loc = lvl->tp_fifo.emplace(
                p.id,
                static_cast<UserId>(0),
                p.qty,
                p.trigger,
                p.limit_px,
                p.tif,
                p.ts
            );
            lvl->tps_nonempty = true;
            locators_.insert(p.id, Locator{lvl, loc.blk, loc.off, Locator::Kind::TakeProfit, p.side, p.trigger});
            return make_new(p.id, p.side, p.trigger, p.qty, p.ts);
        }



        void match_aggressive(OrderId taker_id, Side side, PriceTick limit_px, Qty qty, uint64_t ts,
                                     bool is_market = false) {
            match_result.reset();
            Qty filled_total = 0;
            PriceTick last_px_exec = 0;

            std::size_t opp_idx = (side == Side::Buy) ? best_ask_idx_ : best_buy_idx_;
            PriceTick best_px = (opp_idx == npos)
                                    ? 0
                                    : ((side == Side::Buy)
                                           ? price_from_ask_index(opp_idx)
                                           : price_from_bid_index(opp_idx));

            while (qty > 0 && best_px != 0) {
                // ensures limits that cross the book only fill at the select limit price
                if (!is_market) {
                    if (side == Side::Buy && best_px > limit_px) {
                        break;
                    }
                    if (side == Side::Sell && best_px < limit_px) {
                        break;
                    }
                }

                LevelT* lvl = level_of(opposite(side), best_px, false);
                assert(lvl && "best index points to null level");
                auto* head = lvl->order_fifo.head_slot();
                assert(head && "active level has no head order");

                Qty exec_qty = (head->remaining < qty ? head->remaining : qty);
                head->remaining -= exec_qty;
                qty -= exec_qty;
                lvl->active_qty -= exec_qty;
                filled_total += exec_qty;
                last_px_exec = best_px;
                ++match_result.fill_count;
                match_result.qty += exec_qty;

                BookEvent e{};
                e.id = head->id;
                e.qty = exec_qty;
                e.price = last_px_exec;
                e.ts = ts;
                e.event_type = BookEventType::Fill;
                match_result.fills.push_back(e);

                if (head->remaining == 0) {
                    OrderId maker_id = head->id;
                    lvl->order_fifo.pop_head();
                    locators_.erase(maker_id);
                    if (active_limit_orders_ > 0) {
                        --active_limit_orders_;
                    }
                    if (opposite(side) == Side::Buy) {
                        if (active_limit_buys_ > 0) {
                            --active_limit_buys_;
                        }
                    }
                    else {
                        if (active_limit_sells_ > 0) {
                            --active_limit_sells_;
                        }
                    }
                }

                if (lvl->order_fifo.live_count() == 0) {
                    lvl->active_nonempty = false;
                    lvl->active_qty = 0;
                    // recompute best price to execute on
                    on_level_clear(opposite(side), side_index(opposite(side), best_px));
                    opp_idx = side == Side::Buy ? best_ask_idx_ : best_buy_idx_;
                    best_px = opp_idx == npos
                                  ? 0
                                  : (side == Side::Buy)
                                  ? price_from_ask_index(opp_idx)
                                  : price_from_bid_index(opp_idx);
                }
            }

            match_result.last_px = last_px_exec;


            if (last_px_exec != 0) {
                on_trade_last(last_px_exec, ts);
            }


        }

        void drain_stops_range(Side side, PriceTick from_incl, PriceTick to_incl, uint64_t ts) {
            if (from_incl > to_incl) {
                return;
            }
            for (PriceTick px = from_incl; px <= to_incl; ++px) {
                LevelT* lvl = level_of(side, px, false);
                if (!lvl) continue;
                while (auto* s = lvl->stop_fifo.head_slot()) {
                    OrderParams lim{};
                    OrderParams mkt{};
                    if (s->post_type == OrderType::StopMarket) {
                        mkt.id = s->id;
                        mkt.side = side;
                        mkt.qty = s->qty;
                        mkt.tif = TIF::IOC;
                        mkt.ts = ts;
                        mkt.action = OrderAction::New;
                        mkt.type = OrderType::Market;
                        lvl->stop_fifo.pop_head();
                        locators_.erase(s->id);
                        if (active_stop_orders_ > 0) {
                            --active_stop_orders_;
                        }
                        submit_market(mkt);
                    }
                    else {
                        lim.id = s->id;
                        lim.side = side;
                        lim.price = s->limit_px;
                        lim.qty = s->qty;
                        lim.tif = s->tif;
                        lim.ts = ts;
                        lim.action = OrderAction::New;
                        lim.type = OrderType::Limit;
                        lvl->stop_fifo.pop_head();
                        locators_.erase(s->id);
                        if (active_stop_orders_ > 0) {
                            --active_stop_orders_;
                        }
                        submit_limit(lim);
                    }
                }
                lvl->stops_nonempty = false;
                if (px == to_incl) {
                    break;
                }
            }
        }

        void drain_tps_range(Side side, PriceTick start, PriceTick end, uint64_t ts) {
            if (start > end) {
                return;
            }
            for (PriceTick px = start; px <= end; ++px) {
                LevelT* lvl = level_of(side, px, false);
                if (!lvl) {
                    continue;
                }
                while (auto* t = lvl->tp_fifo.head_slot()) {
                    OrderParams lim{};
                    lim.id = t->id;
                    lim.side = side;
                    lim.price = t->limit_px;
                    lim.qty = t->qty;
                    lim.tif = t->tif;
                    lim.ts = ts;
                    lim.action = OrderAction::New;
                    lim.type = OrderType::Limit;
                    lvl->tp_fifo.pop_head();
                    locators_.erase(t->id);
                    submit_limit(lim);
                }
                lvl->tps_nonempty = false;
            }
        }

        inline bool crosses(Side side, PriceTick px) const {
            if (side == Side::Buy) {
                PriceTick ba = best_ask();
                return ba != 0 && px >= ba;
            }
            else {
                PriceTick bb = best_bid();
                return bb != 0 && px <= bb;
            }
        }

        inline std::size_t bid_index(PriceTick px) const { return max_tick_ - px; }
        inline std::size_t ask_index(PriceTick px) const { return px - min_tick_; }

        inline std::size_t side_index(Side side, PriceTick px) const {
            return (side == Side::Buy) ? bid_index(px) : ask_index(px);
        }

        inline PriceTick price_from_bid_index(std::size_t i) const { return static_cast<PriceTick>(max_tick_ - i); }
        inline PriceTick price_from_ask_index(std::size_t i) const { return static_cast<PriceTick>(min_tick_ + i); }

        LevelT* level_of(Side side, PriceTick px, bool create) const {
            auto idx = side_index(side, px);
            auto& vec = (side == Side::Buy) ? bids_ : asks_;
            LevelT* lvl = vec[idx];
            if (!lvl && create) {
                lvl = level_pool_.acquire(active_pool_, stop_pool_, tp_pool_);
                vec[idx] = lvl;
            }
            return lvl;
        }

        static Side opposite(Side s) {
            return s == Side::Buy ? Side::Sell : Side::Buy;
        }

        std::size_t next_active_index(Side s, std::size_t start) const {
            if (start >= range_) {
                return npos;
            }
            auto const& vec = (s == Side::Buy) ? bids_ : asks_;
            for (std::size_t i = start; i < range_; ++i) {
                LevelT* lvl = vec[i];
                if (lvl && lvl->active_nonempty && lvl->order_fifo.live_count() > 0) {
                    return i;
                }
            }
            return npos;
        }

        static BookEvent make_reject(OrderId id, RejectReason reason, uint64_t ts) {
            BookEvent e{};
            e.event_type = BookEventType::Reject;
            e.id = id;
            e.ts = ts;
            e.reason = reason;
            return e;
        }

        static BookEvent make_new(OrderId id, Side side, PriceTick price, Qty qty, uint64_t ts) {
            BookEvent e{};
            e.event_type = BookEventType::New;
            e.id = id;
            e.price = price;
            e.qty = qty;
            e.ts = ts;
            return e;
        }

        static BookEvent make_fill(OrderId id, Side side, PriceTick price, Qty qty, uint64_t ts) {
            BookEvent e{};
            e.event_type = BookEventType::Fill;
            e.id = id;
            e.price = price;
            e.qty = qty;
            e.ts = ts;
            return e;
        }

        static BookEvent make_trade(OrderId id, Side side, PriceTick price, Qty qty, uint64_t ts) {
            BookEvent e{};
            e.event_type = BookEventType::Trade;
            e.id = id;
            e.price = price;
            e.qty = qty;
            e.ts = ts;
            return e;
        }
    };
}
