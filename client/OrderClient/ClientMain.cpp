//
// Created by djaiswal on 1/29/26.
//

#include "OrderClient.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <limits>
#include <pthread.h>
#include <sched.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
    using Config = jolt::client::ClientConfig;
    constexpr size_t kWorkerCoreStart = 2;

    enum class ParseResult : uint8_t {
        Ok = 0,
        Help = 1,
        Error = 2,
    };

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

    size_t online_cpu_count() {
#if defined(_SC_NPROCESSORS_ONLN)
        const long v = ::sysconf(_SC_NPROCESSORS_ONLN);
        if (v > 0) {
            return static_cast<size_t>(v);
        }
#endif
        const unsigned int hc = std::thread::hardware_concurrency();
        return hc > 0 ? static_cast<size_t>(hc) : 0;
    }

    bool pin_current_thread_to_core(const size_t core_id, std::string& error) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        const int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            error = std::strerror(rc);
            return false;
        }
        return true;
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
            << "  --final-drain-ms <n>         default: 0 (wait indefinitely for responses)\n"
            << "  --stay-connected             keep sessions open after sending all orders\n"
            << "  --market                     send market orders only\n";
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
            auto parse_u64_opt = [&](const char* opt, uint64_t& out, auto&& valid) -> bool {
                const std::string v = need_value(opt);
                if (!parse_u64(v, out) || !valid(out)) {
                    std::cerr << "invalid " << opt << " value\n";
                    return false;
                }
                return true;
            };
            auto parse_usize_opt = [&](const char* opt, size_t& out, auto&& valid) -> bool {
                const std::string v = need_value(opt);
                if (!parse_usize(v, out) || !valid(out)) {
                    std::cerr << "invalid " << opt << " value\n";
                    return false;
                }
                return true;
            };
            auto parse_double_opt = [&](const char* opt, double& out, auto&& valid) -> bool {
                const std::string v = need_value(opt);
                if (!parse_double(v, out) || !valid(out)) {
                    std::cerr << "invalid " << opt << " value\n";
                    return false;
                }
                return true;
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
                if (!parse_usize_opt("--clients", cfg.clients, [](size_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--total-orders") {
                if (!parse_u64_opt("--total-orders", cfg.total_orders, [](uint64_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--orders-per-client") {
                if (!parse_u64_opt("--orders-per-client",
                                   cfg.orders_per_client_override,
                                   [](uint64_t v) { return v > 0; })) {
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
                if (!parse_u64_opt("--qty", cfg.qty, [](uint64_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--base-price") {
                if (!parse_u64_opt("--base-price", cfg.base_price, [](uint64_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--price-step") {
                if (!parse_u64_opt("--price-step", cfg.price_step, [](uint64_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--target-active-limit") {
                if (!parse_u64_opt("--target-active-limit",
                                   cfg.target_active_limit,
                                   [](uint64_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--target-active-stop") {
                if (!parse_u64_opt("--target-active-stop",
                                   cfg.target_active_stop,
                                   [](uint64_t v) { return v > 0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--markov-stay-prob") {
                if (!parse_double_opt("--markov-stay-prob",
                                      cfg.markov_stay_prob,
                                      [](double v) { return v >= 0.0 && v <= 1.0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--markov-reverse-prob") {
                if (!parse_double_opt("--markov-reverse-prob",
                                      cfg.markov_reverse_prob,
                                      [](double v) { return v >= 0.0 && v <= 1.0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--pareto-alpha") {
                if (!parse_double_opt("--pareto-alpha", cfg.pareto_alpha, [](double v) { return v > 1.0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--pareto-scale") {
                if (!parse_double_opt("--pareto-scale", cfg.pareto_scale, [](double v) { return v > 0.0; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--send-interval-us") {
                if (!parse_u64_opt("--send-interval-us", cfg.send_interval_us, [](uint64_t) { return true; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--poll-every") {
                if (!parse_usize_opt("--poll-every", cfg.poll_every, [](size_t) { return true; })) {
                    return ParseResult::Error;
                }
            } else if (arg == "--final-drain-ms") {
                if (!parse_u64_opt("--final-drain-ms", cfg.final_drain_ms, [](uint64_t) { return true; })) {
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
        (cfg.target_active_limit + (cfg.clients) - 1) / (cfg.clients);
    const uint64_t target_stop_per_client =
        (cfg.target_active_stop + (cfg.clients) - 1) / (cfg.clients);

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

    jolt::client::ClientStats stats{
        connected_ok,
        connected_fail,
        logons_sent,
        orders_sent,
        send_fail,
        poll_fail,
        responses_recv
    };

    const auto start = std::chrono::steady_clock::now();
    const size_t cpu_count = online_cpu_count();
    const size_t pinnable_workers = cpu_count > kWorkerCoreStart ? (cpu_count - kWorkerCoreStart) : 0;
    if (pinnable_workers == 0) {
        std::cerr << "[client] warn: no CPU cores available at/after core " << kWorkerCoreStart
                  << "; worker threads will run without pinning\n";
    } else if (cfg.clients > pinnable_workers) {
        std::cerr << "[client] warn: clients=" << cfg.clients
                  << " exceeds pinnable cores from core " << kWorkerCoreStart
                  << " (" << pinnable_workers << "); extra workers will run without pinning\n";
    }

    std::vector<std::thread> workers;
    workers.reserve(cfg.clients);

    const uint64_t base_orders = cfg.total_orders / (cfg.clients);
    const uint64_t extra_orders = cfg.total_orders % (cfg.clients);

    for (size_t client_idx = 0; client_idx < cfg.clients; ++client_idx) {
        workers.emplace_back([&, client_idx] {
            if (client_idx < pinnable_workers) {
                const size_t core_id = kWorkerCoreStart + client_idx;
                std::string pin_error;
                if (!pin_current_thread_to_core(core_id, pin_error)) {
                    std::cerr << "[client] warn: failed to pin client_idx=" << client_idx
                              << " to core=" << core_id << " error=\"" << pin_error
                              << "\"; continuing unpinned\n";
                }
            }

            const uint64_t orders_for_client = base_orders + (client_idx < extra_orders ? 1 : 0);
            jolt::client::OrderClient client(
                client_idx,
                cfg,
                stats,
                target_limit_per_client,
                target_stop_per_client);
            client.run(orders_for_client);
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

    const uint64_t sent = orders_sent.load();
    const uint64_t recv = responses_recv.load();
    const uint64_t missing = sent > recv ? (sent - recv) : 0;

    std::cout
        << "[client] done in " << elapsed_ms << " ms\n"
        << "[client] connected_ok=" << connected_ok.load() << " connected_fail=" << connected_fail.load() << "\n"
        << "[client] logons_sent=" << logons_sent.load() << "\n"
        << "[client] orders_sent=" << sent << " send_fail=" << send_fail.load()
        << " poll_fail=" << poll_fail.load()
        << " responses_recv=" << recv
        << " responses_missing=" << missing << "\n"
        << "[client] avg_order_rate=" << static_cast<uint64_t>(order_rate) << " orders/sec\n";

    return connected_ok.load() == 0 ? 2 : 0;
}
