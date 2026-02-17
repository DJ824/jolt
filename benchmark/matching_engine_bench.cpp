#include "exchange/orderbook/matching_orderbook.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <x86gprintrin.h>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

using jolt::ob::BookEvent;
using jolt::ob::BookEventType;
using jolt::ob::MatchingOrderBook;
using jolt::ob::OrderAction;
using jolt::ob::OrderId;
using jolt::ob::OrderParams;
using jolt::ob::OrderType;
using jolt::ob::PriceTick;
using jolt::ob::Qty;
using jolt::ob::Side;
using jolt::ob::TIF;

namespace {
    constexpr PriceTick kMinTick = 1;
    constexpr PriceTick kMaxTick = 20'000;
    constexpr PriceTick kStartMid = 10'000;

    struct HawkesParams {
        double mu{50.0};
        double alpha{42.5};
        double beta{50.0};
    };

    struct BenchConfig {
        std::size_t events{5'000'000};
        std::size_t warmup{100'000};
        std::size_t preseed_limits{10'000};
        std::size_t preseed_stops{1'500};
        uint64_t seed{42};
        HawkesParams hawkes{};
    };

    enum class OpType : uint8_t {
        Limit,
        ModifyLimit,
        StopLimit,
        Stop,
        CancelStopLimit,
        CancelStop,
        CancelLimit,
        ModifyStopLimit,
        ModifyStop,
        LimitTaker,
        Market,
        Count
    };

    constexpr std::array<const char*, static_cast<std::size_t>(OpType::Count)> kOpNames = {
        "Limit",
        "ModifyLimit",
        "StopLimit",
        "Stop",
        "CancelStopLimit",
        "CancelStop",
        "CancelLimit",
        "ModifyStopLimit",
        "ModifyStop",
        "LimitTaker",
        "Market"
    };

    // Boosted Market + CancelLimit relative to baseline while keeping mix close.
    constexpr std::array<double, static_cast<std::size_t>(OpType::Count)> kOpWeights = {
        23.3, // Limit (down from 28.3)
        11.3, // ModifyLimit (down from 12.4)
        11.4, // StopLimit
        11.4, // Stop
        7.6, // CancelStopLimit
        7.6, // CancelStop
        10.0, // CancelLimit (up from 6.4)
        4.9, // ModifyStopLimit
        4.9, // ModifyStop
        2.7, // Limit taker
        5.0 // Market (up from 2.5)
    };

    struct IdPool {
        std::vector<OrderId> ids;
        std::unordered_map<OrderId, std::size_t> index;

        void reserve(std::size_t n) {
            ids.reserve(n);
            index.reserve(n);
        }

        void add(OrderId id) {
            if (index.find(id) != index.end()) {
                return;
            }
            index.emplace(id, ids.size());
            ids.push_back(id);
        }

        bool erase(OrderId id) {
            const auto it = index.find(id);
            if (it == index.end()) {
                return false;
            }

            const std::size_t pos = it->second;
            const OrderId tail = ids.back();
            ids[pos] = tail;
            index[tail] = pos;
            ids.pop_back();
            index.erase(it);
            return true;
        }

        [[nodiscard]] bool empty() const {
            return ids.empty();
        }

        [[nodiscard]] std::size_t size() const {
            return ids.size();
        }

        template <typename URNG>
        [[nodiscard]] std::optional<OrderId> pick(URNG& rng) const {
            if (ids.empty()) {
                return std::nullopt;
            }
            std::uniform_int_distribution<std::size_t> dist(0, ids.size() - 1);
            return ids[dist(rng)];
        }
    };

    struct OrderState {
        OrderType type{OrderType::Limit};
        Side side{Side::Buy};
        Qty qty{0};
        PriceTick price{0};
        PriceTick trigger{0};
        PriceTick limit_px{0};
    };

    struct Counters {
        std::array<std::size_t, static_cast<std::size_t>(OpType::Count)> attempted{};
        std::array<std::size_t, static_cast<std::size_t>(OpType::Count)> accepted{};
        std::size_t rejects{0};
    };

    [[nodiscard]] PriceTick clamp_tick(int64_t px) {
        if (px < static_cast<int64_t>(kMinTick)) {
            return kMinTick;
        }
        if (px > static_cast<int64_t>(kMaxTick)) {
            return kMaxTick;
        }
        return static_cast<PriceTick>(px);
    }

    [[nodiscard]] Qty mutate_qty(Qty current, std::mt19937_64& rng) {
        std::uniform_int_distribution<int> bump(-20, 35);
        int64_t next = static_cast<int64_t>(current) + bump(rng);
        if (next <= 0) {
            next = 1;
        }
        return static_cast<Qty>(next);
    }

    class BenchDriver {
    public:
        BenchDriver(MatchingOrderBook<>& book, uint64_t seed)
            : book_(book), rng_(seed), op_dist_(kOpWeights.begin(), kOpWeights.end()) {
            states_.reserve(32'768);
            limit_ids_.reserve(32'768);
            stop_ids_.reserve(16'384);
            stop_limit_ids_.reserve(16'384);
        }

        void preseed(const BenchConfig& cfg) {
            uint64_t ts = 1;
            for (std::size_t i = 0; i < cfg.preseed_limits; ++i) {
                OrderParams p = make_passive_limit(ts++);
                const BookEvent ev = book_.submit_order(p);
                after_submit(p, ev);
            }

            for (std::size_t i = 0; i < cfg.preseed_stops; ++i) {
                OrderParams s = make_stop_market(ts++);
                const BookEvent ev1 = book_.submit_order(s);
                after_submit(s, ev1);

                OrderParams sl = make_stop_limit(ts++);
                const BookEvent ev2 = book_.submit_order(sl);
                after_submit(sl, ev2);
            }
        }

        [[nodiscard]] OpType sample_op() {
            return static_cast<OpType>(op_dist_(rng_));
        }

        [[nodiscard]] OrderParams make_order(OpType op, uint64_t ts) {
            nudge_reference_price();

            switch (op) {
            case OpType::Limit:
                return make_passive_limit(ts);
            case OpType::ModifyLimit:
                return make_modify_limit(ts).value_or(make_passive_limit(ts));
            case OpType::StopLimit:
                return make_stop_limit(ts);
            case OpType::Stop:
                return make_stop_market(ts);
            case OpType::CancelStopLimit:
                return make_cancel_stop_limit(ts).value_or(make_stop_limit(ts));
            case OpType::CancelStop:
                return make_cancel_stop(ts).value_or(make_stop_market(ts));
            case OpType::CancelLimit:
                return make_cancel_limit(ts).value_or(make_passive_limit(ts));
            case OpType::ModifyStopLimit:
                return make_modify_stop_limit(ts).value_or(make_stop_limit(ts));
            case OpType::ModifyStop:
                return make_modify_stop(ts).value_or(make_stop_market(ts));
            case OpType::LimitTaker:
                return make_aggressive_limit(ts).value_or(make_passive_limit(ts));
            case OpType::Market:
                return make_market(ts);
            case OpType::Count:
                break;
            }

            return make_passive_limit(ts);
        }

        void apply(const OrderParams& p, const BookEvent& ev) {
            after_submit(p, ev);
        }

        void update_counters(OpType op, const BookEvent& ev, Counters& counters) const {
            const std::size_t i = static_cast<std::size_t>(op);
            counters.attempted[i]++;
            if (ev.event_type == BookEventType::Reject) {
                counters.rejects++;
            }
            else {
                counters.accepted[i]++;
            }
        }

        [[nodiscard]] std::size_t tracked_limits() const {
            return limit_ids_.size();
        }

        [[nodiscard]] std::size_t tracked_stops() const {
            return stop_ids_.size() + stop_limit_ids_.size();
        }

    private:
        MatchingOrderBook<>& book_;
        std::mt19937_64 rng_;
        std::discrete_distribution<int> op_dist_;

        OrderId next_id_{1};
        PriceTick ref_price_{kStartMid};

        std::unordered_map<OrderId, OrderState> states_;
        IdPool limit_ids_;
        IdPool stop_ids_;
        IdPool stop_limit_ids_;

        [[nodiscard]] Side random_side() {
            std::bernoulli_distribution coin(0.5);
            return coin(rng_) ? Side::Buy : Side::Sell;
        }

        void nudge_reference_price() {
            std::uniform_int_distribution<int> drift(-2, 2);
            const int d = drift(rng_);
            ref_price_ = clamp_tick(static_cast<int64_t>(ref_price_) + d);

            const PriceTick bb = book_.best_bid();
            const PriceTick ba = book_.best_ask();
            if (bb != 0 && ba != 0 && bb < ba) {
                ref_price_ = static_cast<PriceTick>((static_cast<uint64_t>(bb) + static_cast<uint64_t>(ba)) / 2U);
            }
        }

        [[nodiscard]] OrderId alloc_id() {
            return next_id_++;
        }

        [[nodiscard]] OrderParams make_passive_limit(uint64_t ts) {
            OrderParams p{};
            p.action = OrderAction::New;
            p.type = OrderType::Limit;
            p.id = alloc_id();
            p.side = random_side();

            std::uniform_int_distribution<int> gap(1, 8);
            const int offset = gap(rng_);

            if (p.side == Side::Buy) {
                PriceTick price = clamp_tick(static_cast<int64_t>(ref_price_) - offset);
                const PriceTick best_ask = book_.best_ask();
                if (best_ask != 0 && price >= best_ask) {
                    price = clamp_tick(static_cast<int64_t>(best_ask) - 1);
                }
                p.price = price;
            }
            else {
                PriceTick price = clamp_tick(static_cast<int64_t>(ref_price_) + offset);
                const PriceTick best_bid = book_.best_bid();
                if (best_bid != 0 && price <= best_bid) {
                    price = clamp_tick(static_cast<int64_t>(best_bid) + 1);
                }
                p.price = price;
            }

            std::uniform_int_distribution<uint32_t> qty_dist(10, 120);
            p.qty = qty_dist(rng_);
            p.tif = TIF::GTC;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_aggressive_limit(uint64_t ts) {
            OrderParams p{};
            p.action = OrderAction::New;
            p.type = OrderType::Limit;
            p.id = alloc_id();

            const PriceTick bb = book_.best_bid();
            const PriceTick ba = book_.best_ask();

            if (bb == 0 && ba == 0) {
                return std::nullopt;
            }

            std::bernoulli_distribution coin(0.5);
            p.side = coin(rng_) ? Side::Buy : Side::Sell;
            if (ba == 0) {
                p.side = Side::Sell;
            }
            else if (bb == 0) {
                p.side = Side::Buy;
            }

            std::uniform_int_distribution<int> pad(0, 3);
            if (p.side == Side::Buy) {
                p.price = clamp_tick(static_cast<int64_t>(ba) + pad(rng_));
            }
            else {
                p.price = clamp_tick(static_cast<int64_t>(bb) - pad(rng_));
            }

            std::uniform_int_distribution<uint32_t> qty_dist(1, 60);
            p.qty = qty_dist(rng_);
            p.tif = TIF::GTC;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] Side choose_taker_side() {
            const PriceTick bb = book_.best_bid();
            const PriceTick ba = book_.best_ask();

            if (bb == 0 && ba == 0) {
                return random_side();
            }
            if (ba == 0) {
                return Side::Sell;
            }
            if (bb == 0) {
                return Side::Buy;
            }

            std::bernoulli_distribution coin(0.5);
            return coin(rng_) ? Side::Buy : Side::Sell;
        }

        [[nodiscard]] OrderParams make_market(uint64_t ts) {
            OrderParams p{};
            p.action = OrderAction::New;
            p.type = OrderType::Market;
            p.id = alloc_id();
            p.side = choose_taker_side();
            std::uniform_int_distribution<uint32_t> qty_dist(1, 40);
            p.qty = qty_dist(rng_);
            p.tif = TIF::IOC;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] OrderParams make_stop_market(uint64_t ts) {
            OrderParams p{};
            p.action = OrderAction::New;
            p.type = OrderType::StopMarket;
            p.id = alloc_id();
            p.side = random_side();

            std::uniform_int_distribution<int> trigger_gap(2, 12);
            const int g = trigger_gap(rng_);

            if (p.side == Side::Buy) {
                p.trigger = clamp_tick(static_cast<int64_t>(ref_price_) + g);
            }
            else {
                p.trigger = clamp_tick(static_cast<int64_t>(ref_price_) - g);
            }

            std::uniform_int_distribution<uint32_t> qty_dist(1, 120);
            p.qty = qty_dist(rng_);
            p.tif = TIF::GTC;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] OrderParams make_stop_limit(uint64_t ts) {
            OrderParams p{};
            p.action = OrderAction::New;
            p.type = OrderType::StopLimit;
            p.id = alloc_id();
            p.side = random_side();

            std::uniform_int_distribution<int> trigger_gap(2, 12);
            std::uniform_int_distribution<int> limit_gap(0, 3);
            const int tg = trigger_gap(rng_);
            const int lg = limit_gap(rng_);

            if (p.side == Side::Buy) {
                p.trigger = clamp_tick(static_cast<int64_t>(ref_price_) + tg);
                p.limit_px = clamp_tick(static_cast<int64_t>(p.trigger) + lg);
            }
            else {
                p.trigger = clamp_tick(static_cast<int64_t>(ref_price_) - tg);
                p.limit_px = clamp_tick(static_cast<int64_t>(p.trigger) - lg);
            }

            std::uniform_int_distribution<uint32_t> qty_dist(1, 120);
            p.qty = qty_dist(rng_);
            p.tif = TIF::GTC;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_cancel_limit(uint64_t ts) {
            const auto id = pick_live_id(limit_ids_);
            if (!id.has_value()) {
                return std::nullopt;
            }

            OrderParams p{};
            p.action = OrderAction::Cancel;
            p.id = *id;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_cancel_stop(uint64_t ts) {
            const auto id = pick_live_id(stop_ids_);
            if (!id.has_value()) {
                return std::nullopt;
            }

            OrderParams p{};
            p.action = OrderAction::Cancel;
            p.id = *id;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_cancel_stop_limit(uint64_t ts) {
            const auto id = pick_live_id(stop_limit_ids_);
            if (!id.has_value()) {
                return std::nullopt;
            }

            OrderParams p{};
            p.action = OrderAction::Cancel;
            p.id = *id;
            p.ts = ts;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_modify_limit(uint64_t ts) {
            const auto id = pick_live_id(limit_ids_);
            if (!id.has_value()) {
                return std::nullopt;
            }

            const auto st_it = states_.find(*id);
            if (st_it == states_.end()) {
                return std::nullopt;
            }

            const OrderState& st = st_it->second;
            OrderParams p{};
            p.action = OrderAction::Modify;
            p.id = *id;
            p.ts = ts;
            p.qty = mutate_qty(st.qty, rng_);

            std::uniform_int_distribution<int> delta(-3, 3);
            int64_t next_price = static_cast<int64_t>(st.price) + delta(rng_);
            if (st.side == Side::Buy) {
                const PriceTick ba = book_.best_ask();
                if (ba != 0 && next_price >= static_cast<int64_t>(ba) + 8) {
                    next_price = static_cast<int64_t>(ba) + 7;
                }
            }
            else {
                const PriceTick bb = book_.best_bid();
                if (bb != 0 && next_price <= static_cast<int64_t>(bb) - 8) {
                    next_price = static_cast<int64_t>(bb) - 7;
                }
            }

            p.price = clamp_tick(next_price);
            p.tif = TIF::GTC;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_modify_stop(uint64_t ts) {
            const auto id = pick_live_id(stop_ids_);
            if (!id.has_value()) {
                return std::nullopt;
            }

            const auto st_it = states_.find(*id);
            if (st_it == states_.end()) {
                return std::nullopt;
            }

            const OrderState& st = st_it->second;
            OrderParams p{};
            p.action = OrderAction::Modify;
            p.id = *id;
            p.ts = ts;
            p.qty = mutate_qty(st.qty, rng_);

            std::uniform_int_distribution<int> delta(-5, 5);
            p.price = clamp_tick(static_cast<int64_t>(st.trigger) + delta(rng_));
            p.tif = TIF::GTC;
            return p;
        }

        [[nodiscard]] std::optional<OrderParams> make_modify_stop_limit(uint64_t ts) {
            const auto id = pick_live_id(stop_limit_ids_);
            if (!id.has_value()) {
                return std::nullopt;
            }

            const auto st_it = states_.find(*id);
            if (st_it == states_.end()) {
                return std::nullopt;
            }

            const OrderState& st = st_it->second;
            OrderParams p{};
            p.action = OrderAction::Modify;
            p.id = *id;
            p.ts = ts;
            p.qty = mutate_qty(st.qty, rng_);

            std::uniform_int_distribution<int> delta(-5, 5);
            p.price = clamp_tick(static_cast<int64_t>(st.trigger) + delta(rng_));
            p.tif = TIF::GTC;
            return p;
        }

        void add_state(const OrderParams& p, Qty resting_qty) {
            OrderState st{};
            st.type = p.type;
            st.side = p.side;
            st.qty = resting_qty;
            st.price = p.price;
            st.trigger = p.trigger;
            st.limit_px = p.limit_px;
            states_[p.id] = st;

            if (p.type == OrderType::Limit) {
                limit_ids_.add(p.id);
            }
            else if (p.type == OrderType::StopMarket) {
                stop_ids_.add(p.id);
            }
            else if (p.type == OrderType::StopLimit) {
                stop_limit_ids_.add(p.id);
            }
        }

        void erase_state(OrderId id) {
            const auto it = states_.find(id);
            if (it == states_.end()) {
                limit_ids_.erase(id);
                stop_ids_.erase(id);
                stop_limit_ids_.erase(id);
                return;
            }

            switch (it->second.type) {
            case OrderType::Limit:
                limit_ids_.erase(id);
                break;
            case OrderType::StopMarket:
                stop_ids_.erase(id);
                break;
            case OrderType::StopLimit:
                stop_limit_ids_.erase(id);
                break;
            case OrderType::Market:
            case OrderType::TakeProfit:
                break;
            }
            states_.erase(it);
        }

        void refresh_fills() {
            for (const BookEvent& fill : book_.match_result.fills) {
                const OrderId id = fill.id;
                const auto st_it = states_.find(id);
                if (st_it == states_.end()) {
                    continue;
                }
                const Qty q = book_.order_qty(id);
                if (q == 0) {
                    erase_state(id);
                }
                else {
                    st_it->second.qty = q;
                }
            }
        }

        void after_submit(const OrderParams& p, const BookEvent& ev) {
            refresh_fills();

            if (p.action == OrderAction::New) {
                if (ev.event_type == BookEventType::New) {
                    const Qty q = book_.order_qty(p.id);
                    if (q > 0) {
                        add_state(p, q);
                    }
                }
                return;
            }

            if (p.action == OrderAction::Cancel) {
                if (ev.event_type == BookEventType::Cancel || ev.event_type == BookEventType::Reject) {
                    erase_state(p.id);
                }
                return;
            }

            if (p.action == OrderAction::Modify) {
                if (ev.event_type == BookEventType::Reject) {
                    erase_state(p.id);
                    return;
                }

                const Qty q = book_.order_qty(p.id);
                if (q == 0) {
                    erase_state(p.id);
                    return;
                }

                auto st_it = states_.find(p.id);
                if (st_it == states_.end()) {
                    return;
                }

                OrderState& st = st_it->second;
                st.qty = q;
                if (st.type == OrderType::Limit) {
                    st.price = p.price;
                }
                else if (st.type == OrderType::StopMarket || st.type == OrderType::StopLimit) {
                    st.trigger = p.price;
                }
            }
        }

        [[nodiscard]] std::optional<OrderId> pick_live_id(IdPool& pool) {
            while (!pool.empty()) {
                const auto id = pool.pick(rng_);
                if (!id.has_value()) {
                    return std::nullopt;
                }

                const auto st_it = states_.find(*id);
                if (st_it == states_.end()) {
                    pool.erase(*id);
                    continue;
                }

                if (book_.order_qty(*id) == 0) {
                    erase_state(*id);
                    continue;
                }

                return *id;
            }
            return std::nullopt;
        }
    };

    [[nodiscard]] std::vector<uint64_t> build_hawkes_timestamps(
        std::size_t count,
        const HawkesParams& hp,
        std::mt19937_64& rng) {
        std::vector<uint64_t> ts;
        ts.reserve(count);

        std::uniform_real_distribution<double> unit(
            std::nextafter(0.0, std::numeric_limits<double>::infinity()),
            1.0);

        double t = 0.0;
        double z = 0.0;

        for (std::size_t i = 0; i < count; ++i) {
            while (true) {
                const double lambda_bar = hp.mu + hp.alpha * z;
                const double u1 = unit(rng);
                const double wait = -std::log(u1) / lambda_bar;

                t += wait;
                z *= std::exp(-hp.beta * wait);

                const double lambda_t = hp.mu + hp.alpha * z;
                const double u2 = unit(rng);
                if (u2 * lambda_bar <= lambda_t) {
                    z += 1.0;
                    ts.push_back(static_cast<uint64_t>(t * 1'000'000'000.0));
                    break;
                }
            }
        }

        return ts;
    }

    double cycles_to_ns(uint64_t delta) noexcept {
        static double factor = [] {
            long khz = 0;

            // find out how many cycles in 100ms, then convert to ns/cycle
            uint64_t c0 = __rdtsc();
            struct timespec ts{0, 100000000};
            nanosleep(&ts, nullptr);
            uint64_t c1 = __rdtsc();
            khz = long((c1 - c0) / 100);
            return 1e6 / double(khz);
        }();

        return delta * factor;
    }


    [[nodiscard]] std::vector<OpType> build_operation_plan(std::size_t count, uint64_t seed) {
        std::vector<OpType> ops;
        ops.reserve(count);

        std::mt19937_64 rng(seed);
        std::discrete_distribution<int> dist(kOpWeights.begin(), kOpWeights.end());
        for (std::size_t i = 0; i < count; ++i) {
            ops.push_back(static_cast<OpType>(dist(rng)));
        }
        return ops;
    }

    [[nodiscard]] bool parse_u64(std::string_view v, uint64_t& out) {
        if (v.empty()) {
            return false;
        }
        char* end = nullptr;
        const std::string tmp(v);
        const unsigned long long parsed = std::strtoull(tmp.c_str(), &end, 10);
        if (end == nullptr || *end != '\0') {
            return false;
        }
        out = static_cast<uint64_t>(parsed);
        return true;
    }

    [[nodiscard]] bool parse_args(int argc, char** argv, BenchConfig& cfg) {
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg(argv[i]);
            auto read_value = [&](uint64_t& out) -> bool {
                if (i + 1 >= argc) {
                    return false;
                }
                ++i;
                return parse_u64(argv[i], out);
            };

            uint64_t v = 0;
            if (arg == "--events") {
                if (!read_value(v)) return false;
                cfg.events = static_cast<std::size_t>(v);
            }
            else if (arg == "--warmup") {
                if (!read_value(v)) return false;
                cfg.warmup = static_cast<std::size_t>(v);
            }
            else if (arg == "--seed") {
                if (!read_value(v)) return false;
                cfg.seed = v;
            }
            else if (arg == "--preseed-limits") {
                if (!read_value(v)) return false;
                cfg.preseed_limits = static_cast<std::size_t>(v);
            }
            else if (arg == "--preseed-stops") {
                if (!read_value(v)) return false;
                cfg.preseed_stops = static_cast<std::size_t>(v);
            }
            else {
                return false;
            }
        }

        if (cfg.warmup > cfg.events) {
            cfg.warmup = cfg.events;
        }
        return true;
    }

    void print_usage(const char* prog) {
        std::cerr
            << "Usage: " << prog << " [--events N] [--warmup N] [--seed N] "
            << "[--preseed-limits N] [--preseed-stops N]\n";
    }

    void print_summary(
        const BenchConfig& cfg,
        const Counters& counters,
        std::size_t measured_events,
        double submit_only_ns,
        std::size_t tracked_limits,
        std::size_t tracked_stops) {
        const double branching_ratio = cfg.hawkes.alpha / cfg.hawkes.beta;
        const double throughput = (submit_only_ns > 0.0)
                                      ? static_cast<double>(measured_events) * 1'000'000'000.0 / submit_only_ns
                                      : 0.0;
        const uint64_t throughput_ops_per_sec = static_cast<uint64_t>(std::llround(throughput));
        const double avg_ns_per_op = (measured_events > 0)
                                         ? submit_only_ns / static_cast<double>(measured_events)
                                         : 0.0;

        std::cout << "Hawkes(mu=" << cfg.hawkes.mu
            << ", alpha=" << cfg.hawkes.alpha
            << ", beta=" << cfg.hawkes.beta
            << ", alpha/beta=" << branching_ratio
            << ")\n";

        std::cout << "events=" << cfg.events
            << " warmup=" << cfg.warmup
            << " measured=" << measured_events
            << " submit_only_ns_total=" << submit_only_ns
            << " avg_ns_per_op=" << avg_ns_per_op
            << " throughput_ops_per_sec=" << throughput_ops_per_sec
            << " rejects=" << counters.rejects
            << " tracked_limits=" << tracked_limits
            << " tracked_stops=" << tracked_stops
            << "\n";

        std::cout << "realized_mix(% of measured):\n";
        for (std::size_t i = 0; i < static_cast<std::size_t>(OpType::Count); ++i) {
            const double pct = (measured_events > 0)
                                   ? (100.0 * static_cast<double>(counters.attempted[i]) / static_cast<double>(
                                       measured_events))
                                   : 0.0;
            std::cout << "  " << kOpNames[i]
                << "=" << pct
                << "% accepted=" << counters.accepted[i]
                << "/" << counters.attempted[i]
                << "\n";
        }
    }
} // namespace

int main(int argc, char** argv) {
    BenchConfig cfg{};
    if (!parse_args(argc, argv, cfg)) {
        print_usage(argv[0]);
        return 1;
    }

    MatchingOrderBook<> book(kMinTick, kMaxTick);
    BenchDriver driver(book, cfg.seed);

    driver.preseed(cfg);

    std::mt19937_64 hawkes_rng(cfg.seed ^ 0x9e3779b97f4a7c15ULL);
    std::vector<uint64_t> timestamps = build_hawkes_timestamps(cfg.events, cfg.hawkes, hawkes_rng);
    std::vector<OpType> ops = build_operation_plan(cfg.events, cfg.seed ^ 0xbf58476d1ce4e5b9ULL);

    for (std::size_t i = 0; i < cfg.warmup; ++i) {
        const OrderParams p = driver.make_order(ops[i], timestamps[i]);
        const BookEvent ev = book.submit_order(p);
        driver.apply(p, ev);
    }

    Counters counters{};
    uint64_t submit_cycles = 0;
    for (std::size_t i = cfg.warmup; i < cfg.events; ++i) {
        const OpType op = ops[i];
        const OrderParams p = driver.make_order(op, timestamps[i]);
        const uint64_t t2 = __rdtsc();
        const BookEvent ev = book.submit_order(p);
        const uint64_t t3 = __rdtsc();
        submit_cycles += (t3 - t2);
        driver.apply(p, ev);
        driver.update_counters(op, ev, counters);
    }
    const double submit_only_ns = cycles_to_ns(submit_cycles);

    const std::size_t measured = cfg.events - cfg.warmup;

    print_summary(
        cfg,
        counters,
        measured,
        submit_only_ns,
        driver.tracked_limits(),
        driver.tracked_stops());

    return 0;
}
