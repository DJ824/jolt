#include "OrderClient.h"

#include <array>
#include <chrono>
#include <cmath>
#include <iostream>
#include <limits>
#include <thread>
#include <utility>

namespace jolt::client {
    namespace {
        enum class ScenarioOp : uint8_t {
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
            Market
        };
        constexpr size_t kScenarioOpCount = 11;

        constexpr size_t op_index(ScenarioOp op) {
            return static_cast<size_t>(op);
        }

        struct ManagedOrder {
            std::string cl_ord_id{};
            std::string symbol{};
            bool is_buy{true};
            uint64_t qty{0};
            uint64_t limit_px{0};
            uint64_t stop_px{0};
        };

        struct PriceModel {
            uint64_t price{1};
            uint64_t step{1};
            double stay_prob{0.72};
            double reverse_prob{0.14};
            double pareto_alpha{1.7};
            double pareto_scale{1.0};
            int dir{1};

            uint64_t next(std::mt19937_64& rng) {
                std::uniform_real_distribution<double> uni01(0.0, 1.0);
                const double r = uni01(rng);

                if (r >= stay_prob) {
                    if (r < stay_prob + reverse_prob) {
                        dir = -dir;
                    } else {
                        dir = (uni01(rng) < 0.5) ? 1 : -1;
                    }
                }

                double u = uni01(rng);
                if (u >= 1.0) {
                    u = std::nextafter(1.0, 0.0);
                }
                if (u <= 0.0) {
                    u = std::numeric_limits<double>::min();
                }

                const double raw_jump = pareto_scale / std::pow(1.0 - u, 1.0 / pareto_alpha);
                uint64_t jump_ticks = static_cast<uint64_t>(raw_jump);
                if (jump_ticks == 0) {
                    jump_ticks = 1;
                }
                if (jump_ticks > 2000) {
                    jump_ticks = 2000;
                }

                const uint64_t delta = jump_ticks * (step == 0 ? 1 : step);
                if (dir > 0) {
                    price += delta;
                } else {
                    price = (price > delta) ? (price - delta) : 1;
                }
                return price;
            }
        };

        constexpr std::array<std::pair<ScenarioOp, size_t>, 11> kOrderMix{{
            {ScenarioOp::Limit, 252},
            {ScenarioOp::ModifyLimit, 124},
            {ScenarioOp::StopLimit, 114},
            {ScenarioOp::Stop, 114},
            {ScenarioOp::CancelStopLimit, 76},
            {ScenarioOp::CancelStop, 76},
            {ScenarioOp::CancelLimit, 64},
            {ScenarioOp::ModifyStopLimit, 49},
            {ScenarioOp::ModifyStop, 49},
            {ScenarioOp::LimitTaker, 27},
            {ScenarioOp::Market, 55},
        }};

        constexpr std::array<ScenarioOp, kScenarioOpCount> kCoverageOrder{{
            ScenarioOp::Limit,
            ScenarioOp::ModifyLimit,
            ScenarioOp::CancelLimit,
            ScenarioOp::Stop,
            ScenarioOp::ModifyStop,
            ScenarioOp::CancelStop,
            ScenarioOp::StopLimit,
            ScenarioOp::ModifyStopLimit,
            ScenarioOp::CancelStopLimit,
            ScenarioOp::LimitTaker,
            ScenarioOp::Market,
        }};

        const std::vector<ScenarioOp>& mixed_order_schedule() {
            static const std::vector<ScenarioOp> schedule = [] {
                std::vector<ScenarioOp> out;
                out.reserve(1000);
                for (const auto& [op, weight] : kOrderMix) {
                    for (size_t i = 0; i < weight; ++i) {
                        out.push_back(op);
                    }
                }
                return out;
            }();
            return schedule;
        }

        bool op_requires_active(ScenarioOp op) {
            return op == ScenarioOp::ModifyLimit ||
                   op == ScenarioOp::ModifyStop ||
                   op == ScenarioOp::ModifyStopLimit ||
                   op == ScenarioOp::CancelLimit ||
                   op == ScenarioOp::CancelStop ||
                   op == ScenarioOp::CancelStopLimit;
        }

        bool op_has_liquidity(ScenarioOp op, size_t limit_ct, size_t stop_ct, size_t stop_limit_ct) {
            switch (op) {
            case ScenarioOp::ModifyLimit:
            case ScenarioOp::CancelLimit:
                return limit_ct > 0;
            case ScenarioOp::ModifyStop:
            case ScenarioOp::CancelStop:
                return stop_ct > 0;
            case ScenarioOp::ModifyStopLimit:
            case ScenarioOp::CancelStopLimit:
                return stop_limit_ct > 0;
            default:
                return true;
            }
        }

        ScenarioOp fallback_new_op(ScenarioOp op) {
            switch (op) {
            case ScenarioOp::ModifyLimit:
            case ScenarioOp::CancelLimit:
                return ScenarioOp::Limit;
            case ScenarioOp::ModifyStop:
            case ScenarioOp::CancelStop:
                return ScenarioOp::Stop;
            case ScenarioOp::ModifyStopLimit:
            case ScenarioOp::CancelStopLimit:
                return ScenarioOp::StopLimit;
            default:
                return op;
            }
        }

        bool pick_coverage_op(size_t limit_ct,
                              size_t stop_ct,
                              size_t stop_limit_ct,
                              const std::array<bool, kScenarioOpCount>& covered,
                              ScenarioOp& out) {
            for (const ScenarioOp desired : kCoverageOrder) {
                if (covered[op_index(desired)]) {
                    continue;
                }
                if (op_has_liquidity(desired, limit_ct, stop_ct, stop_limit_ct)) {
                    out = desired;
                    return true;
                }
                out = fallback_new_op(desired);
                return true;
            }
            return false;
        }

        ScenarioOp pick_op(std::mt19937_64& rng,
                           const std::vector<ScenarioOp>& schedule,
                           size_t limit_ct,
                           size_t stop_ct,
                           size_t stop_limit_ct,
                           size_t target_limit,
                           size_t target_stop,
                           bool market_only) {
            if (market_only) {
                return ScenarioOp::Market;
            }

            const size_t stop_total = stop_ct + stop_limit_ct;
            const uint64_t roll = rng() % 100;

            if (limit_ct + 32 < target_limit && roll < 45) {
                return ScenarioOp::Limit;
            }
            if (limit_ct > target_limit + 32 && limit_ct > 0 && roll < 35) {
                return ScenarioOp::CancelLimit;
            }

            if (stop_total + 16 < target_stop && roll < 45) {
                return ((rng() & 1ULL) == 0ULL) ? ScenarioOp::Stop : ScenarioOp::StopLimit;
            }
            if (stop_total > target_stop + 16 && stop_total > 0 && roll < 35) {
                if (stop_ct > 0 && stop_limit_ct > 0) {
                    return ((rng() & 1ULL) == 0ULL) ? ScenarioOp::CancelStop : ScenarioOp::CancelStopLimit;
                }
                return stop_ct > 0 ? ScenarioOp::CancelStop : ScenarioOp::CancelStopLimit;
            }

            for (size_t i = 0; i < 12; ++i) {
                const ScenarioOp op = schedule[static_cast<size_t>(rng() % schedule.size())];
                if (!op_requires_active(op) || op_has_liquidity(op, limit_ct, stop_ct, stop_limit_ct)) {
                    return op;
                }
            }

            const ScenarioOp sampled = schedule[static_cast<size_t>(rng() % schedule.size())];
            if (op_has_liquidity(sampled, limit_ct, stop_ct, stop_limit_ct)) {
                return sampled;
            }
            return fallback_new_op(sampled);
        }

        uint64_t clamp_price(uint64_t px) {
            return px == 0 ? 1 : px;
        }

        uint64_t sub_clamped(uint64_t value, uint64_t delta) {
            return value > delta ? value - delta : 1;
        }

        uint64_t passive_limit_px(uint64_t center, bool is_buy, uint64_t step) {
            const uint64_t offset = step * 2;
            return is_buy ? sub_clamped(center, offset) : center + offset;
        }

        uint64_t taker_limit_px(uint64_t center, bool is_buy, uint64_t step) {
            const uint64_t offset = step * 16;
            return is_buy ? center + offset : sub_clamped(center, offset);
        }

        uint64_t stop_trigger_px(uint64_t center, bool is_buy, uint64_t step) {
            const uint64_t offset = step * 3;
            return is_buy ? center + offset : sub_clamped(center, offset);
        }

        uint64_t stop_limit_px(uint64_t trigger_px, bool is_buy, uint64_t step) {
            const uint64_t offset = step;
            return is_buy ? trigger_px + offset : sub_clamped(trigger_px, offset);
        }

        uint64_t drain_fix_messages(FixClient& fix) {
            uint64_t drained = 0;
            while (fix.next_message().has_value()) {
                ++drained;
            }
            return drained;
        }
    }

    OrderClient::OrderClient(size_t client_idx,
                             const ClientConfig& cfg,
                             ClientStats& stats,
                             uint64_t target_limit_per_client,
                             uint64_t target_stop_per_client)
        : client_idx_(client_idx),
          cfg_(cfg),
          stats_(stats),
          target_limit_per_client_(target_limit_per_client),
          target_stop_per_client_(target_stop_per_client),
          rng_((static_cast<uint64_t>(client_idx_) + 1) * 0x9E3779B97F4A7C15ULL),
          id_("CLIENT_" + std::to_string(client_idx_ + 1)) {
        fix_.set_session(id_, "ENTRY_GATEWAY");
        fix_.set_account(id_);
    }

    void OrderClient::run(uint64_t orders_for_client) {
        if (orders_for_client == 0) {
            return;
        }

        std::vector<ManagedOrder> active_limits;
        std::vector<ManagedOrder> active_stops;
        std::vector<ManagedOrder> active_stop_limits;
        active_limits.reserve(static_cast<size_t>(target_limit_per_client_ + 512));
        active_stops.reserve(static_cast<size_t>(target_stop_per_client_ + 256));
        active_stop_limits.reserve(static_cast<size_t>(target_stop_per_client_ + 256));

        PriceModel price_model{};
        price_model.price = cfg_.base_price;
        price_model.step = cfg_.price_step;
        price_model.stay_prob = cfg_.markov_stay_prob;
        price_model.reverse_prob = cfg_.markov_reverse_prob;
        price_model.pareto_alpha = cfg_.pareto_alpha;
        price_model.pareto_scale = cfg_.pareto_scale;
        price_model.dir = ((rng_() & 1ULL) == 0ULL) ? 1 : -1;

        if (!fix_.connect_tcp(cfg_.host, cfg_.port)) {
            ++stats_.connected_fail;
            return;
        }
        ++stats_.connected_ok;

        if (!fix_.send_raw(fix_.build_logon(30))) {
            ++stats_.send_fail;
            fix_.disconnect();
            return;
        }
        ++stats_.logons_sent;

        const auto& schedule = mixed_order_schedule();
        const uint64_t step = cfg_.price_step == 0 ? 1 : cfg_.price_step;
        uint64_t local_orders_sent = 0;
        uint64_t local_responses_recv = 0;
        std::array<bool, kScenarioOpCount> covered_ops{};
        const bool enforce_mix_coverage = !cfg_.use_market_orders;

        auto send_msg = [&](std::string_view msg) -> bool {
            if (msg.empty() || !fix_.send_raw(msg)) {
                ++stats_.send_fail;
                return false;
            }
            ++stats_.orders_sent;
            ++local_orders_sent;
            return true;
        };
        auto submit_passive_limit = [&](uint64_t center_px,
                                        const std::string& cl_ord_id,
                                        const std::string& symbol,
                                        bool is_buy) -> bool {
            const uint64_t px = clamp_price(passive_limit_px(center_px, is_buy, step));
            if (!send_msg(fix_.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg_.qty, px, 1))) {
                return false;
            }
            active_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg_.qty, px, 0});
            return true;
        };
        auto submit_stop = [&](uint64_t center_px,
                               const std::string& cl_ord_id,
                               const std::string& symbol,
                               bool is_buy) -> bool {
            const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
            if (!send_msg(fix_.build_new_order_stop(cl_ord_id, symbol, is_buy, cfg_.qty, stop_px, 1))) {
                return false;
            }
            active_stops.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg_.qty, 0, stop_px});
            return true;
        };
        auto submit_stop_limit = [&](uint64_t center_px,
                                     const std::string& cl_ord_id,
                                     const std::string& symbol,
                                     bool is_buy) -> bool {
            const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
            const uint64_t limit_px = clamp_price(stop_limit_px(stop_px, is_buy, step));
            if (!send_msg(fix_.build_new_order_stop_limit(cl_ord_id, symbol, is_buy, cfg_.qty, stop_px, limit_px, 1))) {
                return false;
            }
            active_stop_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg_.qty, limit_px, stop_px});
            return true;
        };
        auto cancel_random_active = [&](std::vector<ManagedOrder>& active_orders,
                                        const std::string& cl_ord_id) -> bool {
            const size_t idx = static_cast<size_t>(rng_() % active_orders.size());
            const ManagedOrder ord = active_orders[idx];
            if (!send_msg(fix_.build_cancel(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy))) {
                return false;
            }
            active_orders[idx] = std::move(active_orders.back());
            active_orders.pop_back();
            return true;
        };
        auto poll_and_drain = [&](uint64_t* drained_out = nullptr) -> bool {
            const bool poll_ok = fix_.poll();
            if (!poll_ok) {
                ++stats_.poll_fail;
            }
            const uint64_t drained = drain_fix_messages(fix_);
            local_responses_recv += drained;
            stats_.responses_recv.fetch_add(drained, std::memory_order_relaxed);
            if (drained_out != nullptr) {
                *drained_out = drained;
            }
            // Allow one last drain pass on disconnect if messages were already buffered.
            return poll_ok || drained > 0;
        };

        for (uint64_t order_idx = 0; order_idx < orders_for_client; ++order_idx) {
            const uint64_t center_px = price_model.next(rng_);
            const bool is_buy = ((rng_() & 1ULL) == 0ULL);
            const std::string& symbol = cfg_.symbols[static_cast<size_t>(rng_() % cfg_.symbols.size())];

            ScenarioOp op{};
            if (!pick_coverage_op(active_limits.size(),
                                  active_stops.size(),
                                  active_stop_limits.size(),
                                  covered_ops,
                                  op) ||
                !enforce_mix_coverage) {
                op = pick_op(
                    rng_,
                    schedule,
                    active_limits.size(),
                    active_stops.size(),
                    active_stop_limits.size(),
                    static_cast<size_t>(target_limit_per_client_),
                    static_cast<size_t>(target_stop_per_client_),
                    cfg_.use_market_orders);
            }

            const std::string cl_ord_id = id_ + "_" + fix_.next_cl_ord_id();
            ScenarioOp emitted_op = op;
            bool sent = false;

            switch (op) {
            case ScenarioOp::Limit: {
                sent = submit_passive_limit(center_px, cl_ord_id, symbol, is_buy);
                break;
            }
            case ScenarioOp::Stop: {
                sent = submit_stop(center_px, cl_ord_id, symbol, is_buy);
                break;
            }
            case ScenarioOp::StopLimit: {
                sent = submit_stop_limit(center_px, cl_ord_id, symbol, is_buy);
                break;
            }
            case ScenarioOp::LimitTaker: {
                const uint64_t px = clamp_price(taker_limit_px(center_px, is_buy, step));
                sent = send_msg(fix_.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg_.qty, px, 3));
                break;
            }
            case ScenarioOp::Market: {
                sent = send_msg(fix_.build_new_order_market(cl_ord_id, symbol, is_buy, cfg_.qty, 3));
                break;
            }
            case ScenarioOp::ModifyLimit: {
                if (active_limits.empty()) {
                    emitted_op = ScenarioOp::Limit;
                    sent = submit_passive_limit(center_px, cl_ord_id, symbol, is_buy);
                    break;
                }
                auto& ord = active_limits[static_cast<size_t>(rng_() % active_limits.size())];
                const uint64_t new_px = clamp_price(passive_limit_px(center_px, ord.is_buy, step));
                const uint64_t new_qty = ord.qty + 1;
                sent = send_msg(fix_.build_replace(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy, new_qty, new_px, 1));
                if (sent) {
                    ord.cl_ord_id = cl_ord_id;
                    ord.limit_px = new_px;
                    ord.qty = new_qty;
                }
                break;
            }
            case ScenarioOp::ModifyStop: {
                if (active_stops.empty()) {
                    emitted_op = ScenarioOp::Stop;
                    sent = submit_stop(center_px, cl_ord_id, symbol, is_buy);
                    break;
                }
                auto& ord = active_stops[static_cast<size_t>(rng_() % active_stops.size())];
                const uint64_t new_stop = clamp_price(stop_trigger_px(center_px, ord.is_buy, step));
                const uint64_t new_qty = ord.qty + 1;
                sent = send_msg(fix_.build_replace_stop(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy, new_qty, new_stop, 1));
                if (sent) {
                    ord.cl_ord_id = cl_ord_id;
                    ord.stop_px = new_stop;
                    ord.qty = new_qty;
                }
                break;
            }
            case ScenarioOp::ModifyStopLimit: {
                if (active_stop_limits.empty()) {
                    emitted_op = ScenarioOp::StopLimit;
                    sent = submit_stop_limit(center_px, cl_ord_id, symbol, is_buy);
                    break;
                }
                auto& ord = active_stop_limits[static_cast<size_t>(rng_() % active_stop_limits.size())];
                const uint64_t new_stop = clamp_price(stop_trigger_px(center_px, ord.is_buy, step));
                const uint64_t new_limit = clamp_price(stop_limit_px(new_stop, ord.is_buy, step));
                const uint64_t new_qty = ord.qty + 1;
                sent = send_msg(fix_.build_replace_stop_limit(
                        cl_ord_id,
                        ord.cl_ord_id,
                        ord.symbol,
                        ord.is_buy,
                        new_qty,
                        new_stop,
                        new_limit,
                        1));
                if (sent) {
                    ord.cl_ord_id = cl_ord_id;
                    ord.stop_px = new_stop;
                    ord.limit_px = new_limit;
                    ord.qty = new_qty;
                }
                break;
            }
            case ScenarioOp::CancelLimit: {
                if (active_limits.empty()) {
                    emitted_op = ScenarioOp::Limit;
                    sent = submit_passive_limit(center_px, cl_ord_id, symbol, is_buy);
                    break;
                }
                sent = cancel_random_active(active_limits, cl_ord_id);
                break;
            }
            case ScenarioOp::CancelStop: {
                if (active_stops.empty()) {
                    emitted_op = ScenarioOp::Stop;
                    sent = submit_stop(center_px, cl_ord_id, symbol, is_buy);
                    break;
                }
                sent = cancel_random_active(active_stops, cl_ord_id);
                break;
            }
            case ScenarioOp::CancelStopLimit: {
                if (active_stop_limits.empty()) {
                    emitted_op = ScenarioOp::StopLimit;
                    sent = submit_stop_limit(center_px, cl_ord_id, symbol, is_buy);
                    break;
                }
                sent = cancel_random_active(active_stop_limits, cl_ord_id);
                break;
            }
            }

            if (sent) {
                covered_ops[op_index(emitted_op)] = true;
            }

            const bool should_poll_now =
                (cfg_.poll_every == 0) || (((order_idx + 1) % cfg_.poll_every) == 0);
            if (should_poll_now) {
                (void)poll_and_drain();
            }

            if (cfg_.send_interval_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(cfg_.send_interval_us));
            }
        }

        if (!cfg_.stay_connected) {
            const bool drain_has_timeout = cfg_.final_drain_ms > 0;
            const auto drain_start = std::chrono::steady_clock::now();
            const auto drain_deadline = drain_start + std::chrono::milliseconds(cfg_.final_drain_ms);
            auto last_progress_log = drain_start;
            bool poll_failed = false;
            bool drain_timed_out = false;

            if (local_responses_recv < local_orders_sent) {
                std::cerr << "[client] connected client_id=" << id_
                          << " waiting_for_responses sent=" << local_orders_sent
                          << " recv=" << local_responses_recv
                          << " missing=" << (local_orders_sent - local_responses_recv)
                          << "\n";
            }

            while (local_responses_recv < local_orders_sent) {
                if (drain_has_timeout && std::chrono::steady_clock::now() >= drain_deadline) {
                    drain_timed_out = true;
                    break;
                }

                uint64_t drained = 0;
                if (!poll_and_drain(&drained)) {
                    poll_failed = true;
                    break;
                }

                const auto now = std::chrono::steady_clock::now();
                if (now - last_progress_log >= std::chrono::seconds(5)) {
                    std::cerr << "[client] connected client_id=" << id_
                              << " waiting_for_responses sent=" << local_orders_sent
                              << " recv=" << local_responses_recv
                              << " missing=" << (local_orders_sent - local_responses_recv)
                              << "\n";
                    last_progress_log = now;
                }

                if (drained == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }

            if (local_responses_recv == local_orders_sent) {
                std::cerr << "[client] connected client_id=" << id_
                          << " all_responses_received recv=" << local_responses_recv
                          << "\n";
            }

            if (local_responses_recv < local_orders_sent) {
                if (drain_timed_out) {
                    std::cerr << "[client] response drain timed out client_id=" << id_
                              << " sent=" << local_orders_sent
                              << " recv=" << local_responses_recv
                              << " missing=" << (local_orders_sent - local_responses_recv)
                              << "\n";
                } else if (poll_failed) {
                    std::cerr << "[client] disconnected before all responses client_id=" << id_
                              << " sent=" << local_orders_sent
                              << " recv=" << local_responses_recv
                              << " missing=" << (local_orders_sent - local_responses_recv)
                              << "\n";
                }
            }
            fix_.disconnect();
            return;
        }

        while (true) {
            (void)poll_and_drain();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}
