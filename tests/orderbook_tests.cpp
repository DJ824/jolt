// #include <cstdint>
// #include <vector>
// #include <algorithm>
// #include <iostream>
//
// #include "test_harness.h"
// #include "../exchange/orderbook/matching_orderbook.h"
//
// using namespace jolt::ob;
//
// namespace {
//     static constexpr PriceTick MIN_TICK = 50;
//     static constexpr PriceTick MAX_TICK = 200;
//
//     static inline OrderParams L(OrderId id, Side side, PriceTick px, Qty qty, TIF tif = TIF::GTC, uint64_t ts = 0) {
//         OrderParams p{};
//         p.action = OrderAction::New;
//         p.type = OrderType::Limit;
//         p.id = id;
//         p.side = side;
//         p.price = px;
//         p.qty = qty;
//         p.tif = tif;
//         p.ts = ts;
//         return p;
//     }
//
//     static inline OrderParams M(OrderId id, Side side, Qty qty, TIF tif = TIF::IOC, uint64_t ts = 0) {
//         OrderParams p{};
//         p.action = OrderAction::New;
//         p.type = OrderType::Market;
//         p.id = id;
//         p.side = side;
//         p.qty = qty;
//         p.tif = tif;
//         p.ts = ts;
//         return p;
//     }
//
//     static inline OrderParams S(OrderId id, Side side, PriceTick trig, Qty qty, OrderType post = OrderType::StopMarket,
//                                 PriceTick lim_px = 0, TIF tif = TIF::GTC, uint64_t ts = 0) {
//         OrderParams p{};
//         p.action = OrderAction::New;
//         p.type = post;
//         p.id = id;
//         p.side = side;
//         p.trigger = trig;
//         p.qty = qty;
//         p.limit_px = lim_px;
//         p.tif = tif;
//         p.ts = ts;
//         return p;
//     }
// } // namespace
//
// TEST(Inspect_Book_Vectors_Stable_Indexing) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 1;
//     ob.submit_limit(L(id++, Side::Buy, 100, 1));
//     ob.submit_limit(L(id++, Side::Buy, 99, 1));
//     ob.submit_limit(L(id++, Side::Sell, 103, 1));
//     ob.submit_limit(L(id++, Side::Sell, 104, 1));
//
//     auto bi100 = ob.inspect_index(Side::Buy, 100);
//     auto bi99 = ob.inspect_index(Side::Buy, 99);
//     auto ai103 = ob.inspect_index(Side::Sell, 103);
//     auto ai104 = ob.inspect_index(Side::Sell, 104);
//
//     const auto& bids = ob.inspect_bids();
//     const auto& asks = ob.inspect_asks();
//
//     EXPECT_TRUE(bids[bi100] != nullptr);
//     EXPECT_TRUE(bids[bi100]->active_nonempty);
//     EXPECT_TRUE(bids[bi99] != nullptr);
//     EXPECT_TRUE(bids[bi99]->active_nonempty);
//
//     EXPECT_TRUE(asks[ai103] != nullptr);
//     EXPECT_TRUE(asks[ai103]->active_nonempty);
//     EXPECT_TRUE(asks[ai104] != nullptr);
//     EXPECT_TRUE(asks[ai104]->active_nonempty);
//
//     EXPECT_EQ(ob.best_bid(), 100u);
//     EXPECT_EQ(ob.best_ask(), 103u);
// }
//
// static inline void expect_not_crossed(MatchingOrderBook<>& ob) {
//     PriceTick bb = ob.best_bid();
//     PriceTick ba = ob.best_ask();
//     if (bb != 0 && ba != 0) {
//         EXPECT_TRUE(bb < ba);
//     }
// }
//
// TEST(Limit_Adds_CrossingAndResting) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 1;
//     ob.submit_limit(L(id++, Side::Buy, 100, 10));
//     EXPECT_EQ(ob.best_bid(), 100u);
//     EXPECT_EQ(ob.best_ask(), 0u);
//     ob.submit_limit(L(id++, Side::Sell, 101, 5));
//     EXPECT_EQ(ob.best_ask(), 101u);
//     OrderId ask_id = id - 1;
//     ob.submit_limit(L(id++, Side::Buy, 102, 3));
//     (void)ask_id;
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 101), 2u);
//     EXPECT_EQ(ob.best_ask(), 101u);
// }
//
// TEST(Limit_PartialFill_RestsRemaining) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 10;
//     ob.submit_limit(L(id++, Side::Sell, 101, 2)); // ask id 10
//     ob.submit_limit(L(id++, Side::Buy, 101, 10)); // buy id 11, crosses and leaves 8 resting
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 101), 8u);
//     //std::cout << ob.best_ask() << std::endl;
//     EXPECT_EQ(ob.best_ask(), 0u);
//     EXPECT_EQ(ob.best_bid(), 101u);
//     ob.submit_market(M(id++, Side::Sell, 8));
//     EXPECT_EQ(ob.best_bid(), 0u);
// }
//
// TEST(Limit_IOC_FOK_Behavior) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 100;
//
//     // IOC not crossing -> reject
//     ob.submit_limit(L(id++, Side::Buy, 100, 5, TIF::IOC));
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 100), 0u);
//
//     // FOK not crossing -> reject
//     ob.submit_limit(L(id++, Side::Sell, 105, 5, TIF::FOK));
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 105), 0u);
//
//     // Setup liquidity and test IOC/FOK crossing behaviors
//     ob.submit_limit(L(id, Side::Sell, 103, 2));
//     OrderId ask_id = id++;
//
//     // IOC crossing partial => trades then ack, no resting remainder
//     ob.submit_limit(L(id, Side::Buy, 103, 5, TIF::IOC));
//     OrderId ioc_id = id++;
//     (void)ask_id;
//     (void)ioc_id;
//     EXPECT_EQ(ob.best_ask(), 0u);
//     // std::cout << ob.level_active_qty(Side::Buy, 103);
//
//     ob.submit_limit(L(id, Side::Sell, 103, 2));
//     ask_id = id++;
//     ob.submit_limit(L(id, Side::Buy, 103, 5, TIF::FOK));
//     OrderId fok_id = id++;
//     (void)ioc_id;
//     (void)fok_id;
//     EXPECT_EQ(ob.best_ask(), 0u);
//
//     // FOK crossing full => ack
//     ob.submit_limit(L(id, Side::Sell, 103, 5));
//     ask_id = id++;
//     ob.submit_limit(L(id, Side::Buy, 103, 5, TIF::FOK));
//     EXPECT_EQ(ob.best_ask(), 0u);
// }
//
// // ---------- Market orders ----------
//
// TEST(Market_Matches_BestPrices) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 200;
//
//     // No liquidity
//     ob.submit_market(M(id++, Side::Buy, 5));
//     EXPECT_EQ(ob.best_ask(), 0u);
//
//     // Create two asks: 105@2 and 106@5
//     ob.submit_limit(L(id, Side::Sell, 105, 2));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 106, 5));
//     OrderId a2 = id++;
//
//     ob.submit_market(M(id++, Side::Buy, 6));
//     (void)a1;
//     (void)a2;
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 105), 0u);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 106), 1u);
//     EXPECT_EQ(ob.best_ask(), 106u);
// }
//
// TEST(Market_Sell_Matches_BestBids) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 230;
//
//     ob.submit_limit(L(id, Side::Buy, 95, 1));
//     OrderId b1 = id++;
//     ob.submit_limit(L(id, Side::Buy, 94, 2));
//     OrderId b2 = id++;
//     ob.submit_market(M(id++, Side::Sell, 2));
//     (void)b1;
//     (void)b2;
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 94), 1u);
//     EXPECT_EQ(ob.best_bid(), 94u);
// }
//
// // ---------- Cancel & Modify ----------
//
// TEST(Cancel_Removes_Order_From_Queue) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 300;
//     ob.submit_limit(L(id, Side::Buy, 100, 2));
//     OrderId b1 = id++;
//     ob.submit_limit(L(id, Side::Buy, 100, 2));
//     OrderId b2 = id++;
//
//     // Cancel head order
//     ob.cancel(b1);
//
//     // Market sell must match b2
//     EXPECT_EQ(ob.level_head_order_id(Side::Buy, 100), b2);
//     ob.submit_market(M(id++, Side::Sell, 1));
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 100), 1u);
// }
//
// TEST(Cancel_Clears_Best_When_Last) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 320;
//     ob.submit_limit(L(id, Side::Buy, 100, 1));
//     ob.submit_limit(L(100, Side::Buy, 99, 2));
//     OrderId b1 = id++;
//     EXPECT_EQ(ob.best_bid(), 100u);
//     ob.cancel(b1);
//     EXPECT_EQ(ob.best_bid(), 99u);
// }
//
// TEST(Modify_ChangePrice_Moves_Level) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 340;
//     ob.submit_limit(L(id, Side::Buy, 100, 5));
//     OrderId b1 = id++;
//     EXPECT_TRUE(ob.best_bid() == 100u);
//     bool ok = ob.modify(b1, 5, 101, TIF::GTC, 0);
//     EXPECT_TRUE(ok);
//     EXPECT_EQ(ob.best_bid(), 101u);
//     ob.submit_market(M(id++, Side::Sell, 1));
//     EXPECT_EQ(ob.order_qty(b1), 4u);
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 101), 4);
// }
//
// TEST(Modify_DecreaseQty_InPlace) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 360;
//     ob.submit_limit(L(id, Side::Buy, 100, 5));
//     OrderId b1 = id++;
//     bool ok = ob.modify(b1, 3, 100, TIF::GTC, 0);
//     EXPECT_TRUE(ok);
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 100), 3);
//     ob.submit_market(M(id++, Side::Sell, 5));
//     EXPECT_EQ(ob.best_bid(), 0u);
// }
//
// TEST(Modify_IncreaseQty_Requeues_To_Tail) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 380;
//     ob.submit_limit(L(id, Side::Buy, 100, 2));
//     OrderId b1 = id++;
//     ob.submit_limit(L(id, Side::Buy, 100, 2));
//     OrderId b2 = id++;
//     bool ok = ob.modify(b1, 3, 100, TIF::GTC, 0);
//     EXPECT_TRUE(ok);
//     EXPECT_EQ(ob.level_head_order_id(Side::Buy, 100), b2);
//     ob.submit_market(M(id++, Side::Sell, 1));
//     //std::cout <<ob.level_active_qty(Side::Buy, 100) << std::endl;
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 100), 4u);
// }
//
// TEST(Modify_ZeroQty_Cancels) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 400;
//     ob.submit_limit(L(id, Side::Buy, 100, 2));
//     OrderId b1 = id++;
//     EXPECT_TRUE(ob.modify(b1, 0, 100));
//     EXPECT_EQ(ob.best_bid(), 0u);
// }
//
// TEST(StopMarket_Buy_Triggers_On_UpMove) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 500;
//     ob.on_trade_last(100, 1);
//
//     ob.submit_limit(L(id, Side::Sell, 103, 1));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 104, 5));
//     OrderId a2 = id++;
//
//     ob.submit_stop(S(id, Side::Buy, 103, 3, OrderType::StopMarket));
//     OrderId smb = id++;
//
//     ob.on_trade_last(103, 2);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 103), 0u);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 104), 3u);
// }
//
// TEST(StopLimit_Buy_GTC_Rests_When_NotCrossing) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 540;
//     ob.on_trade_last(100, 1);
//
//     ob.submit_stop(S(id, Side::Buy, 103, 5, OrderType::StopLimit, 102, TIF::GTC));
//     OrderId slb = id++;
//     (void)slb;
//     ob.on_trade_last(103, 2);
//
//     EXPECT_TRUE(ob.best_bid() == 102u);
// }
//
// TEST(StopLimit_Buy_IOC_Rejects_When_NotCrossing) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 560;
//     ob.on_trade_last(100, 1);
//     ob.submit_stop(S(id, Side::Buy, 103, 5, OrderType::StopLimit, 102, TIF::IOC));
//     ob.on_trade_last(103, 2);
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 102), 0u);
// }
//
// TEST(StopMarket_Sell_Triggers_On_DownMove) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 580;
//     ob.on_trade_last(100, 1);
//     ob.submit_limit(L(id, Side::Buy, 98, 2));
//     OrderId b1 = id++;
//     ob.submit_limit(L(id, Side::Buy, 97, 5));
//     OrderId b2 = id++;
//     ob.submit_stop(S(id, Side::Sell, 98, 6));
//     OrderId sms = id++;
//     ob.on_trade_last(97, 2);
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 98), 0u);
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 97), 1u);
// }
//
// TEST(StopOrders_FIFO_At_Same_Trigger) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 620;
//     ob.on_trade_last(100, 1);
//     ob.submit_limit(L(id, Side::Sell, 103, 1));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 104, 10));
//     OrderId a2 = id++;
//     ob.submit_stop(S(id, Side::Buy, 103, 2));
//     OrderId s1 = id++;
//     ob.submit_stop(S(id, Side::Buy, 103, 2));
//     OrderId s2 = id++;
//     (void)s1;
//     (void)s2;
//     ob.on_trade_last(103, 2);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 103), 0u);
//     EXPECT_TRUE(ob.level_active_qty(Side::Sell, 104) <= 10u);
// }
//
// TEST(Invariant_NoCrossing_After_Sequence) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 700;
//     expect_not_crossed(ob);
//     ob.submit_limit(L(id++, Side::Buy, 100, 10));
//     expect_not_crossed(ob);
//     ob.submit_limit(L(id++, Side::Sell, 103, 5));
//     expect_not_crossed(ob);
//     ob.submit_limit(L(id++, Side::Buy, 103, 3));
//     expect_not_crossed(ob);
//     OrderId ask_id = id - 2; // previous ask's id is 701
//     bool ok = ob.modify(ask_id, 5, 104);
//     EXPECT_TRUE(ok);
//     expect_not_crossed(ob);
//     ob.submit_limit(L(id++, Side::Sell, 102, 2));
//     expect_not_crossed(ob);
//     ob.submit_market(M(id++, Side::Buy, 1000));
//     expect_not_crossed(ob);
// }
//
// TEST(Volume_Conservation_MarketBuys_SufficientLiquidity) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 740;
//     ob.submit_limit(L(id, Side::Sell, 101, 5));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 102, 7));
//     OrderId a2 = id++;
//     ob.submit_limit(L(id, Side::Sell, 103, 8));
//     OrderId a3 = id++;
//     (void)a1;
//     (void)a2;
//     (void)a3;
//
//     struct MO {
//         OrderId id;
//         Qty qty;
//     } mos[] = {{id, 6}, {id + 1, 7}, {id + 2, 5}};
//     id += 3;
//
//     for (auto m : mos) {
//         ob.submit_market(M(m.id, Side::Buy, m.qty));
//         expect_not_crossed(ob);
//     }
//
//     EXPECT_TRUE(ob.best_ask() == 103);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 103), 2u);
// }
//
// TEST(Volume_Conservation_MarketBuys_InsufficientLiquidity) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 780;
//     ob.submit_limit(L(id, Side::Sell, 101, 3));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 102, 5));
//     OrderId a2 = id++;
//     (void)a1;
//     (void)a2;
//     ob.submit_market(M(id++, Side::Buy, 10));
//     EXPECT_TRUE(ob.best_ask() == 0); // all liquidity consumed
//     expect_not_crossed(ob);
// }
//
// TEST(Volume_Conservation_MarketSells_SufficientLiquidity) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 820;
//     ob.submit_limit(L(id, Side::Buy, 99, 4));
//     OrderId b1 = id++;
//     ob.submit_limit(L(id, Side::Buy, 98, 6));
//     OrderId b2 = id++;
//     (void)b1;
//     (void)b2;
//
//     OrderId mid = id++;
//     ob.submit_market(M(mid, Side::Sell, 5));
//     expect_not_crossed(ob);
// }
//
// TEST(ZeroQty_NoAck_NoSideEffects) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 860;
//     ob.submit_limit(L(id++, Side::Buy, 100, 1));
//     ob.submit_limit(L(id++, Side::Sell, 101, 1));
//     ob.submit_limit(L(id++, Side::Buy, 100, 0));
//     ob.submit_market(M(id++, Side::Sell, 0));
//     expect_not_crossed(ob);
// }
//
// TEST(MultiBlock_FIFO_Correctness) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 900;
//     const int N = 130;
//     std::vector<OrderId> ids;
//     for (int i = 0; i < N; ++i) {
//         ob.submit_limit(L(id, Side::Buy, 100, 1));
//         ids.push_back(id);
//         id++;
//     }
//     expect_not_crossed(ob);
//     ob.submit_market(M(id++, Side::Sell, N - 2));
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 100), 2);
//     EXPECT_EQ(ob.best_bid(), 100u);
// }
//
// TEST(StopLimit_Buy_Crossing_After_Trigger_GTC) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 950;
//     ob.on_trade_last(100, 1);
//     ob.submit_limit(L(id, Side::Sell, 103, 2));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 104, 2));
//     OrderId a2 = id++;
//     (void)a1;
//     (void)a2;
//
//     ob.submit_stop(S(id, Side::Buy, 103, 3, OrderType::StopLimit, 104, TIF::GTC));
//     OrderId sl = id++;
//     (void)sl;
//     ob.on_trade_last(103, 2);
//
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 103), 0u);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 104), 1u);
//     expect_not_crossed(ob);
// }
//
// TEST(Stop_Modify_IncreaseQty_RequeuesFIFO) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 990;
//     ob.on_trade_last(100, 1);
//
//     ob.submit_limit(L(id, Side::Sell, 101, 10));
//     OrderId a = id++;
//     (void)a;
//
//     OrderId s1 = id;
//     ob.submit_stop(S(id++, Side::Buy, 101, 1));
//     OrderId s2 = id;
//     ob.submit_stop(S(id++, Side::Buy, 101, 1));
//
//     EXPECT_TRUE(ob.modify(s1, 2, 101));
//
//     ob.on_trade_last(101, 2);
//     EXPECT_TRUE(ob.level_active_qty(Side::Sell, 101) <= 8u);
// }
//
// TEST(Stop_Cancel_And_Modify) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 660;
//     ob.on_trade_last(100, 1);
//
//     ob.submit_stop(S(id, Side::Buy, 103, 3));
//     OrderId st = id++;
//     ob.cancel(st);
//     ob.on_trade_last(103, 2);
//     EXPECT_EQ(ob.best_ask(), 0u);
//
//     ob.submit_stop(S(id, Side::Buy, 104, 2));
//     OrderId st2 = id++;
//     EXPECT_TRUE(ob.modify(st2, 2, 103));
//     ob.submit_limit(L(id, Side::Sell, 103, 2));
//     ob.on_trade_last(102, 3);
//     ob.on_trade_last(103, 4);
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 103), 0u);
// }
//
//
// TEST(TP_Sell_Triggers_On_UpMove) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 2000;
//
//     ob.on_trade_last(100, 1);
//
//     ob.submit_limit(L(id, Side::Buy, 106, 1));
//     OrderId b1 = id++;
//     ob.submit_limit(L(id, Side::Buy, 107, 2));
//     OrderId b2 = id++;
//     (void)b1;
//     (void)b2;
//
//     OrderParams tp{};
//     tp.action = OrderAction::New;
//     tp.type = OrderType::TakeProfit;
//     tp.id = id++;
//     tp.side = Side::Sell;
//     tp.trigger = 106;
//     tp.limit_px = 106;
//     tp.tif = TIF::GTC;
//     tp.qty = 2;
//     tp.ts = 2;
//     ob.submit_take_profit(tp);
//     ob.on_trade_last(106, 3);
//
//     EXPECT_EQ(ob.level_active_qty(Side::Buy, 107), 0u);
//     EXPECT_EQ(ob.best_bid(), 106u);
// }
//
// TEST(TP_Buy_Triggers_On_DownMove) {
//     MatchingOrderBook<> ob(MIN_TICK, MAX_TICK);
//     OrderId id = 2100;
//
//     ob.on_trade_last(100, 1);
//
//     ob.submit_limit(L(id, Side::Sell, 94, 1));
//     OrderId a1 = id++;
//     ob.submit_limit(L(id, Side::Sell, 93, 2));
//     OrderId a2 = id++;
//     (void)a1;
//     (void)a2;
//
//     OrderParams tp{};
//     tp.action = OrderAction::New;
//     tp.type = OrderType::TakeProfit;
//     tp.id = id++;
//     tp.side = Side::Buy;
//     tp.trigger = 94;
//     tp.limit_px = 94;
//     tp.tif = TIF::GTC;
//     tp.qty = 2;
//     tp.ts = 2;
//     ob.submit_take_profit(tp);
//     ob.on_trade_last(94, 3);
//
//     EXPECT_EQ(ob.level_active_qty(Side::Sell, 93), 0u);
//     EXPECT_EQ(ob.best_ask(), 94u);
// }
//
// int main() {
//     return ::mini_test::run_all();
// }
