//
// Created by djaiswal on 1/29/26.
//

#include "FixClient.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {
    enum class ParseResult : uint8_t {
        Ok = 0,
        Help = 1,
        Error = 2,
    };

    struct Config {
        std::string host{"127.0.0.1"};
        std::string port{"8080"};
        size_t clients{50};
        uint64_t total_orders{500'000};
        uint64_t orders_per_client_override{0};
        uint64_t qty{1};
        uint64_t base_price{60'000};
        uint64_t price_step{1};
        uint64_t send_interval_us{100};
        size_t poll_every{0};
        bool use_market_orders{false};
        uint64_t target_active_limit{10'000};
        uint64_t target_active_stop{1'000};
        double markov_stay_prob{0.72};
        double markov_reverse_prob{0.14};
        double pareto_alpha{1.7};
        double pareto_scale{1.0};
        bool stay_connected{false};
        std::vector<std::string> symbols{"1", "2", "3", "4"};
    };

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
        {ScenarioOp::Limit, 283},
        {ScenarioOp::ModifyLimit, 124},
        {ScenarioOp::StopLimit, 114},
        {ScenarioOp::Stop, 114},
        {ScenarioOp::CancelStopLimit, 76},
        {ScenarioOp::CancelStop, 76},
        {ScenarioOp::CancelLimit, 64},
        {ScenarioOp::ModifyStopLimit, 49},
        {ScenarioOp::ModifyStop, 49},
        {ScenarioOp::LimitTaker, 27},
        {ScenarioOp::Market, 25},
    }};

    const std::vector<ScenarioOp>& mixed_order_schedule() {
        static const std::vector<ScenarioOp> schedule = [] {
            std::vector<ScenarioOp> out;
            out.reserve(1001);
            for (const auto& [op, weight] : kOrderMix) {
                for (size_t i = 0; i < weight; ++i) {
                    out.push_back(op);
                }
            }
            return out;
        }();
        return schedule;
    }

    bool parse_u64(const std::string& s, uint64_t& out) {
        std::istringstream iss(s);
        iss >> out;
        return !iss.fail() && iss.eof();
    }

    bool parse_usize(const std::string& s, size_t& out) {
        uint64_t v = 0;
        if (!parse_u64(s, v)) {
            return false;
        }
        out = static_cast<size_t>(v);
        return true;
    }

    bool parse_double(const std::string& s, double& out) {
        std::istringstream iss(s);
        iss >> out;
        return !iss.fail() && iss.eof();
    }

    std::vector<std::string> split_csv(const std::string& csv) {
        std::vector<std::string> out;
        std::string curr;
        for (char c : csv) {
            if (c == ',') {
                if (!curr.empty()) {
                    out.push_back(curr);
                }
                curr.clear();
            } else {
                curr.push_back(c);
            }
        }
        if (!curr.empty()) {
            out.push_back(curr);
        }
        return out;
    }

    void print_usage(const char* prog) {
        std::cerr
            << "Usage: " << prog << " [options]\n"
            << "  --host <ip-or-host>          default: 127.0.0.1\n"
            << "  --port <port>                default: 8080\n"
            << "  --clients <n>                default: 50\n"
            << "  --total-orders <n>           default: 250000\n"
            << "  --orders-per-client <n>      optional override (total = clients*n)\n"
            << "  --symbols <csv>              default: 1,2,3,4\n"
            << "  --qty <n>                    default: 1\n"
            << "  --base-price <n>             default: 60000\n"
            << "  --price-step <n>             default: 1\n"
            << "  --target-active-limit <n>    default: 10000\n"
            << "  --target-active-stop <n>     default: 1000\n"
            << "  --markov-stay-prob <x>       default: 0.72\n"
            << "  --markov-reverse-prob <x>    default: 0.14\n"
            << "  --pareto-alpha <x>           default: 1.7\n"
            << "  --pareto-scale <x>           default: 1.0\n"
            << "  --send-interval-us <n>       default: 1000\n"
            << "  --poll-every <n>             default: 0 (disabled)\n"
            << "  --stay-connected             keep sessions alive with heartbeats after sending all orders\n"
            << "  --market                     send market orders only\n";
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

    uint64_t drain_fix_messages(jolt::client::FixClient& fix) {
        uint64_t drained = 0;
        while (fix.next_message().has_value()) {
            ++drained;
        }
        return drained;
    }

    ParseResult parse_args(int argc, char** argv, Config& cfg) {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto need_value = [&](const char* opt) -> std::string {
                if (i + 1 >= argc) {
                    std::cerr << "missing value for " << opt << "\n";
                    return {};
                }
                return argv[++i];
            };

            if (arg == "--host") {
                cfg.host = need_value("--host");
                if (cfg.host.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--port") {
                cfg.port = need_value("--port");
                if (cfg.port.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--clients") {
                const std::string v = need_value("--clients");
                if (!parse_usize(v, cfg.clients) || cfg.clients == 0) {
                    std::cerr << "invalid --clients value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--total-orders") {
                const std::string v = need_value("--total-orders");
                if (!parse_u64(v, cfg.total_orders) || cfg.total_orders == 0) {
                    std::cerr << "invalid --total-orders value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--orders-per-client") {
                const std::string v = need_value("--orders-per-client");
                if (!parse_u64(v, cfg.orders_per_client_override) || cfg.orders_per_client_override == 0) {
                    std::cerr << "invalid --orders-per-client value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--symbols") {
                const std::string v = need_value("--symbols");
                cfg.symbols = split_csv(v);
                if (cfg.symbols.empty()) {
                    std::cerr << "invalid --symbols value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--qty") {
                const std::string v = need_value("--qty");
                if (!parse_u64(v, cfg.qty) || cfg.qty == 0) {
                    std::cerr << "invalid --qty value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--base-price") {
                const std::string v = need_value("--base-price");
                if (!parse_u64(v, cfg.base_price) || cfg.base_price == 0) {
                    std::cerr << "invalid --base-price value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--price-step") {
                const std::string v = need_value("--price-step");
                if (!parse_u64(v, cfg.price_step) || cfg.price_step == 0) {
                    std::cerr << "invalid --price-step value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--target-active-limit") {
                const std::string v = need_value("--target-active-limit");
                if (!parse_u64(v, cfg.target_active_limit) || cfg.target_active_limit == 0) {
                    std::cerr << "invalid --target-active-limit value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--target-active-stop") {
                const std::string v = need_value("--target-active-stop");
                if (!parse_u64(v, cfg.target_active_stop) || cfg.target_active_stop == 0) {
                    std::cerr << "invalid --target-active-stop value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--markov-stay-prob") {
                const std::string v = need_value("--markov-stay-prob");
                if (!parse_double(v, cfg.markov_stay_prob) || cfg.markov_stay_prob < 0.0 || cfg.markov_stay_prob > 1.0) {
                    std::cerr << "invalid --markov-stay-prob value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--markov-reverse-prob") {
                const std::string v = need_value("--markov-reverse-prob");
                if (!parse_double(v, cfg.markov_reverse_prob) || cfg.markov_reverse_prob < 0.0 || cfg.markov_reverse_prob > 1.0) {
                    std::cerr << "invalid --markov-reverse-prob value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--pareto-alpha") {
                const std::string v = need_value("--pareto-alpha");
                if (!parse_double(v, cfg.pareto_alpha) || cfg.pareto_alpha <= 1.0) {
                    std::cerr << "invalid --pareto-alpha value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--pareto-scale") {
                const std::string v = need_value("--pareto-scale");
                if (!parse_double(v, cfg.pareto_scale) || cfg.pareto_scale <= 0.0) {
                    std::cerr << "invalid --pareto-scale value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--send-interval-us") {
                const std::string v = need_value("--send-interval-us");
                if (!parse_u64(v, cfg.send_interval_us)) {
                    std::cerr << "invalid --send-interval-us value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--poll-every") {
                const std::string v = need_value("--poll-every");
                if (!parse_usize(v, cfg.poll_every)) {
                    std::cerr << "invalid --poll-every value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--stay-connected") {
                cfg.stay_connected = true;
            } else if (arg == "--market") {
                cfg.use_market_orders = true;
            } else if (arg == "--help" || arg == "-h") {
                return ParseResult::Help;
            } else {
                std::cerr << "unknown option: " << arg << "\n";
                return ParseResult::Error;
            }
        }

        if (cfg.markov_stay_prob + cfg.markov_reverse_prob > 1.0) {
            std::cerr << "markov probabilities must satisfy stay+reverse <= 1\n";
            return ParseResult::Error;
        }

        if (cfg.orders_per_client_override > 0) {
            const uint64_t max_safe = std::numeric_limits<uint64_t>::max() / static_cast<uint64_t>(cfg.clients);
            if (cfg.orders_per_client_override > max_safe) {
                std::cerr << "orders-per-client too large\n";
                return ParseResult::Error;
            }
            cfg.total_orders = cfg.orders_per_client_override * static_cast<uint64_t>(cfg.clients);
        }

        return ParseResult::Ok;
    }
}

int main(int argc, char** argv) {
    Config cfg;
    const ParseResult parse_result = parse_args(argc, argv, cfg);
    if (parse_result != ParseResult::Ok) {
        print_usage(argv[0]);
        return parse_result == ParseResult::Help ? 0 : 1;
    }

    const uint64_t target_limit_per_client =
        (cfg.target_active_limit + static_cast<uint64_t>(cfg.clients) - 1) / static_cast<uint64_t>(cfg.clients);
    const uint64_t target_stop_per_client =
        (cfg.target_active_stop + static_cast<uint64_t>(cfg.clients) - 1) / static_cast<uint64_t>(cfg.clients);

    std::cout
        << "[client] host=" << cfg.host
        << " port=" << cfg.port
        << " clients=" << cfg.clients
        << " total_orders=" << cfg.total_orders
        << " symbols=" << cfg.symbols.size()
        << " base_price=" << cfg.base_price
        << " price_step=" << cfg.price_step
        << " target_limit=" << cfg.target_active_limit
        << " target_stop=" << cfg.target_active_stop
        << " mode=" << (cfg.use_market_orders ? "market-only" : "mixed")
        << "\n";

    std::atomic<uint64_t> connected_ok{0};
    std::atomic<uint64_t> connected_fail{0};
    std::atomic<uint64_t> logons_sent{0};
    std::atomic<uint64_t> orders_sent{0};
    std::atomic<uint64_t> send_fail{0};
    std::atomic<uint64_t> poll_fail{0};
    std::atomic<uint64_t> responses_recv{0};

    const auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    workers.reserve(cfg.clients);
    const auto& schedule = mixed_order_schedule();

    const uint64_t base_orders = cfg.total_orders / static_cast<uint64_t>(cfg.clients);
    const uint64_t extra_orders = cfg.total_orders % static_cast<uint64_t>(cfg.clients);

    for (size_t client_idx = 0; client_idx < cfg.clients; ++client_idx) {
        workers.emplace_back([&, client_idx] {
            const uint64_t orders_for_client = base_orders + (client_idx < extra_orders ? 1 : 0);
            if (orders_for_client == 0) {
                return;
            }

            jolt::client::FixClient fix;
            std::mt19937_64 rng((static_cast<uint64_t>(client_idx) + 1) * 0x9E3779B97F4A7C15ULL);

            const std::string id = "CLIENT_" + std::to_string(client_idx + 1);
            fix.set_session(id, "ENTRY_GATEWAY");
            fix.set_account(id);

            std::vector<ManagedOrder> active_limits;
            std::vector<ManagedOrder> active_stops;
            std::vector<ManagedOrder> active_stop_limits;
            active_limits.reserve(static_cast<size_t>(target_limit_per_client + 512));
            active_stops.reserve(static_cast<size_t>(target_stop_per_client + 256));
            active_stop_limits.reserve(static_cast<size_t>(target_stop_per_client + 256));

            PriceModel price_model{};
            price_model.price = cfg.base_price;
            price_model.step = cfg.price_step;
            price_model.stay_prob = cfg.markov_stay_prob;
            price_model.reverse_prob = cfg.markov_reverse_prob;
            price_model.pareto_alpha = cfg.pareto_alpha;
            price_model.pareto_scale = cfg.pareto_scale;
            price_model.dir = ((rng() & 1ULL) == 0ULL) ? 1 : -1;

            if (!fix.connect_tcp(cfg.host, cfg.port)) {
                ++connected_fail;
                return;
            }
            ++connected_ok;

            if (!fix.send_raw(fix.build_logon(30))) {
                ++send_fail;
                fix.disconnect();
                return;
            }
            ++logons_sent;

            const uint64_t step = cfg.price_step == 0 ? 1 : cfg.price_step;
            auto send_msg = [&](std::string_view msg) -> bool {
                if (msg.empty() || !fix.send_raw(msg)) {
                    ++send_fail;
                    return false;
                }
                ++orders_sent;
                return true;
            };

            for (uint64_t order_idx = 0; order_idx < orders_for_client; ++order_idx) {
                const uint64_t center_px = price_model.next(rng);
                const bool is_buy = ((rng() & 1ULL) == 0ULL);
                const std::string& symbol = cfg.symbols[static_cast<size_t>(rng() % cfg.symbols.size())];

                const ScenarioOp op = pick_op(
                    rng,
                    schedule,
                    active_limits.size(),
                    active_stops.size(),
                    active_stop_limits.size(),
                    static_cast<size_t>(target_limit_per_client),
                    static_cast<size_t>(target_stop_per_client),
                    cfg.use_market_orders);

                const std::string cl_ord_id = id + "_" + fix.next_cl_ord_id();

                switch (op) {
                case ScenarioOp::Limit: {
                    const uint64_t px = clamp_price(passive_limit_px(center_px, is_buy, step));
                    if (send_msg(fix.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg.qty, px, 1))) {
                        active_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, px, 0});
                    }
                    break;
                }
                case ScenarioOp::Stop: {
                    const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
                    if (send_msg(fix.build_new_order_stop(cl_ord_id, symbol, is_buy, cfg.qty, stop_px, 1))) {
                        active_stops.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, 0, stop_px});
                    }
                    break;
                }
                case ScenarioOp::StopLimit: {
                    const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
                    const uint64_t limit_px = clamp_price(stop_limit_px(stop_px, is_buy, step));
                    if (send_msg(fix.build_new_order_stop_limit(cl_ord_id, symbol, is_buy, cfg.qty, stop_px, limit_px, 1))) {
                        active_stop_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, limit_px, stop_px});
                    }
                    break;
                }
                case ScenarioOp::LimitTaker: {
                    const uint64_t px = clamp_price(taker_limit_px(center_px, is_buy, step));
                    (void)send_msg(fix.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg.qty, px, 3));
                    break;
                }
                case ScenarioOp::Market: {
                    (void)send_msg(fix.build_new_order_market(cl_ord_id, symbol, is_buy, cfg.qty, 3));
                    break;
                }
                case ScenarioOp::ModifyLimit: {
                    if (active_limits.empty()) {
                        const uint64_t px = clamp_price(passive_limit_px(center_px, is_buy, step));
                        if (send_msg(fix.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg.qty, px, 1))) {
                            active_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, px, 0});
                        }
                        break;
                    }
                    auto& ord = active_limits[static_cast<size_t>(rng() % active_limits.size())];
                    const uint64_t new_px = clamp_price(passive_limit_px(center_px, ord.is_buy, step));
                    const uint64_t new_qty = ord.qty + 1;
                    if (send_msg(fix.build_replace(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy, new_qty, new_px, 1))) {
                        ord.cl_ord_id = cl_ord_id;
                        ord.limit_px = new_px;
                        ord.qty = new_qty;
                    }
                    break;
                }
                case ScenarioOp::ModifyStop: {
                    if (active_stops.empty()) {
                        const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
                        if (send_msg(fix.build_new_order_stop(cl_ord_id, symbol, is_buy, cfg.qty, stop_px, 1))) {
                            active_stops.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, 0, stop_px});
                        }
                        break;
                    }
                    auto& ord = active_stops[static_cast<size_t>(rng() % active_stops.size())];
                    const uint64_t new_stop = clamp_price(stop_trigger_px(center_px, ord.is_buy, step));
                    const uint64_t new_qty = ord.qty + 1;
                    if (send_msg(fix.build_replace_stop(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy, new_qty, new_stop, 1))) {
                        ord.cl_ord_id = cl_ord_id;
                        ord.stop_px = new_stop;
                        ord.qty = new_qty;
                    }
                    break;
                }
                case ScenarioOp::ModifyStopLimit: {
                    if (active_stop_limits.empty()) {
                        const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
                        const uint64_t limit_px = clamp_price(stop_limit_px(stop_px, is_buy, step));
                        if (send_msg(fix.build_new_order_stop_limit(cl_ord_id, symbol, is_buy, cfg.qty, stop_px, limit_px, 1))) {
                            active_stop_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, limit_px, stop_px});
                        }
                        break;
                    }
                    auto& ord = active_stop_limits[static_cast<size_t>(rng() % active_stop_limits.size())];
                    const uint64_t new_stop = clamp_price(stop_trigger_px(center_px, ord.is_buy, step));
                    const uint64_t new_limit = clamp_price(stop_limit_px(new_stop, ord.is_buy, step));
                    const uint64_t new_qty = ord.qty + 1;
                    if (send_msg(fix.build_replace_stop_limit(
                            cl_ord_id,
                            ord.cl_ord_id,
                            ord.symbol,
                            ord.is_buy,
                            new_qty,
                            new_stop,
                            new_limit,
                            1))) {
                        ord.cl_ord_id = cl_ord_id;
                        ord.stop_px = new_stop;
                        ord.limit_px = new_limit;
                        ord.qty = new_qty;
                    }
                    break;
                }
                case ScenarioOp::CancelLimit: {
                    if (active_limits.empty()) {
                        const uint64_t px = clamp_price(passive_limit_px(center_px, is_buy, step));
                        if (send_msg(fix.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg.qty, px, 1))) {
                            active_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, px, 0});
                        }
                        break;
                    }
                    const size_t idx = static_cast<size_t>(rng() % active_limits.size());
                    const ManagedOrder ord = active_limits[idx];
                    if (send_msg(fix.build_cancel(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy))) {
                        active_limits[idx] = std::move(active_limits.back());
                        active_limits.pop_back();
                    }
                    break;
                }
                case ScenarioOp::CancelStop: {
                    if (active_stops.empty()) {
                        const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
                        if (send_msg(fix.build_new_order_stop(cl_ord_id, symbol, is_buy, cfg.qty, stop_px, 1))) {
                            active_stops.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, 0, stop_px});
                        }
                        break;
                    }
                    const size_t idx = static_cast<size_t>(rng() % active_stops.size());
                    const ManagedOrder ord = active_stops[idx];
                    if (send_msg(fix.build_cancel(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy))) {
                        active_stops[idx] = std::move(active_stops.back());
                        active_stops.pop_back();
                    }
                    break;
                }
                case ScenarioOp::CancelStopLimit: {
                    if (active_stop_limits.empty()) {
                        const uint64_t stop_px = clamp_price(stop_trigger_px(center_px, is_buy, step));
                        const uint64_t limit_px = clamp_price(stop_limit_px(stop_px, is_buy, step));
                        if (send_msg(fix.build_new_order_stop_limit(cl_ord_id, symbol, is_buy, cfg.qty, stop_px, limit_px, 1))) {
                            active_stop_limits.push_back(ManagedOrder{cl_ord_id, symbol, is_buy, cfg.qty, limit_px, stop_px});
                        }
                        break;
                    }
                    const size_t idx = static_cast<size_t>(rng() % active_stop_limits.size());
                    const ManagedOrder ord = active_stop_limits[idx];
                    if (send_msg(fix.build_cancel(cl_ord_id, ord.cl_ord_id, ord.symbol, ord.is_buy))) {
                        active_stop_limits[idx] = std::move(active_stop_limits.back());
                        active_stop_limits.pop_back();
                    }
                    break;
                }
                }

                const bool should_poll_now =
                    (cfg.poll_every == 0) || (((order_idx + 1) % cfg.poll_every) == 0);
                if (should_poll_now) {
                    if (!fix.poll()) {
                        ++poll_fail;
                    }
                    else {
                        responses_recv.fetch_add(drain_fix_messages(fix), std::memory_order_relaxed);
                    }
                }

                if (cfg.send_interval_us > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(cfg.send_interval_us));
                }
            }

            if (!cfg.stay_connected) {
                fix.disconnect();
                return;
            }

            // Optional keepalive mode after finite order send.
            auto last_heartbeat = std::chrono::steady_clock::now();
            while (true) {
                if (!fix.poll()) {
                    ++poll_fail;
                }
                else {
                    responses_recv.fetch_add(drain_fix_messages(fix), std::memory_order_relaxed);
                }

                const auto now = std::chrono::steady_clock::now();
                if (now - last_heartbeat >= std::chrono::seconds(15)) {
                    if (!fix.send_raw(fix.build_heartbeat())) {
                        ++send_fail;
                    }
                    last_heartbeat = now;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }

    for (auto& t : workers) {
        t.join();
    }

    const auto end = std::chrono::steady_clock::now();
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    const double elapsed_s = static_cast<double>(elapsed_ms) / 1000.0;
    const double order_rate = elapsed_s > 0.0
                                  ? static_cast<double>(orders_sent.load(std::memory_order_relaxed)) / elapsed_s
                                  : 0.0;

    std::cout
        << "[client] done in " << elapsed_ms << " ms\n"
        << "[client] connected_ok=" << connected_ok.load() << " connected_fail=" << connected_fail.load() << "\n"
        << "[client] logons_sent=" << logons_sent.load() << "\n"
        << "[client] orders_sent=" << orders_sent.load() << " send_fail=" << send_fail.load()
        << " poll_fail=" << poll_fail.load()
        << " responses_recv=" << responses_recv.load() << "\n"
        << "[client] avg_order_rate=" << static_cast<uint64_t>(order_rate) << " orders/sec\n";

    return connected_ok.load() == 0 ? 2 : 0;
}
