//
// Created by djaiswal on 1/29/26.
//

#include "FixClient.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
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
        size_t clients{250};
        size_t orders_per_client{1000};
        uint64_t qty{1};
        uint64_t base_price{10000};
        uint64_t price_step{1};
        uint64_t send_interval_us{0};
        size_t poll_every{0};
        bool use_market_orders{false};
        std::vector<std::string> symbols{"SYM0", "SYM1", "SYM2", "SYM3"};
    };

    void print_usage(const char* prog) {
        std::cerr
            << "Usage: " << prog << " [options]\n"
            << "  --host <ip-or-host>         default: 127.0.0.1\n"
            << "  --port <port>               default: 8080\n"
            << "  --clients <n>               default: 250\n"
            << "  --orders-per-client <n>     default: 1000\n"
            << "  --symbols <csv>             default: SYM0,SYM1,SYM2,SYM3\n"
            << "  --qty <n>                   default: 1\n"
            << "  --base-price <n>            default: 10000\n"
            << "  --price-step <n>            default: 1\n"
            << "  --send-interval-us <n>      default: 0\n"
            << "  --poll-every <n>            default: 0 (disabled)\n"
            << "  --market                    send market orders (default: limit)\n";
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
            } else if (arg == "--orders-per-client") {
                const std::string v = need_value("--orders-per-client");
                if (!parse_usize(v, cfg.orders_per_client) || cfg.orders_per_client == 0) {
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
            } else if (arg == "--market") {
                cfg.use_market_orders = true;
            } else if (arg == "--help" || arg == "-h") {
                return ParseResult::Help;
            } else {
                std::cerr << "unknown option: " << arg << "\n";
                return ParseResult::Error;
            }
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

    std::cout
        << "[client] host=" << cfg.host
        << " port=" << cfg.port
        << " clients=" << cfg.clients
        << " orders_per_client=" << cfg.orders_per_client
        << " symbols=" << cfg.symbols.size()
        << " mode=" << (cfg.use_market_orders ? "market" : "limit")
        << "\n";

    std::atomic<uint64_t> connected_ok{0};
    std::atomic<uint64_t> connected_fail{0};
    std::atomic<uint64_t> logons_sent{0};
    std::atomic<uint64_t> orders_sent{0};
    std::atomic<uint64_t> send_fail{0};
    std::atomic<uint64_t> poll_fail{0};

    const auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    workers.reserve(cfg.clients);

    for (size_t client_idx = 0; client_idx < cfg.clients; ++client_idx) {
        workers.emplace_back([&, client_idx] {
            jolt::client::FixClient fix;
            const std::string id = "CLIENT_" + std::to_string(client_idx + 1);
            fix.set_session(id, "ENTRY_GATEWAY");
            fix.set_account(id);

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

            for (size_t order_idx = 0; order_idx < cfg.orders_per_client; ++order_idx) {
                const std::string& symbol = cfg.symbols[(client_idx + order_idx) % cfg.symbols.size()];
                const bool is_buy = ((client_idx + order_idx) & 1ull) == 0;
                const uint64_t px = cfg.base_price + (order_idx * cfg.price_step);

                const std::string cl_ord_id = id + "_" + fix.next_cl_ord_id();
                std::string_view out;
                if (cfg.use_market_orders) {
                    out = fix.build_new_order_market(cl_ord_id, symbol, is_buy, cfg.qty);
                } else {
                    out = fix.build_new_order_limit(cl_ord_id, symbol, is_buy, cfg.qty, px);
                }

                if (out.empty() || !fix.send_raw(out)) {
                    ++send_fail;
                    continue;
                }
                ++orders_sent;

                if (cfg.poll_every > 0 && ((order_idx + 1) % cfg.poll_every) == 0) {
                    if (!fix.poll()) {
                        ++poll_fail;
                    }
                }

                if (cfg.send_interval_us > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(cfg.send_interval_us));
                }
            }

            (void)fix.send_raw(fix.build_logout());
            fix.disconnect();
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
        << " poll_fail=" << poll_fail.load() << "\n"
        << "[client] avg_order_rate=" << static_cast<uint64_t>(order_rate) << " orders/sec\n";

    return connected_ok.load() == 0 ? 2 : 0;
}
