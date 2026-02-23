#include "OrderTest.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

namespace {
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

    double ns_to_ms(const uint64_t ns) {
        return static_cast<double>(ns) / 1'000'000.0;
    }

    std::string format_ns_csv_as_ms(const std::vector<uint64_t>& values) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(3);
        for (size_t i = 0; i < values.size(); ++i) {
            if (i > 0) {
                oss << ",";
            }
            oss << ns_to_ms(values[i]);
        }
        return oss.str();
    }

    uint64_t avg_u64(const std::vector<uint64_t>& values) {
        if (values.empty()) {
            return 0;
        }
        const uint64_t sum = std::accumulate(values.begin(), values.end(), uint64_t{0});
        return sum / static_cast<uint64_t>(values.size());
    }

    uint64_t min_u64(const std::vector<uint64_t>& values) {
        if (values.empty()) {
            return 0;
        }
        return *std::min_element(values.begin(), values.end());
    }

    uint64_t max_u64(const std::vector<uint64_t>& values) {
        if (values.empty()) {
            return 0;
        }
        return *std::max_element(values.begin(), values.end());
    }

    void print_usage(const char* prog) {
        std::cerr
            << "Usage: " << prog << " [options]\n"
            << "  --host <ip-or-host>      default: 127.0.0.1\n"
            << "  --port <port>            default: 8080\n"
            << "  --sender <comp-id>       default: ORDER_TEST\n"
            << "  --target <comp-id>       default: ENTRY_GATEWAY\n"
            << "  --account <id>           default: ORDER_TEST\n"
            << "  --symbol <symbol>        default: 1\n"
            << "  --side <buy|sell>        default: buy\n"
            << "  --qty <n>                default: 1\n"
            << "  --price <n>              default: 60000\n"
            << "  --tif <n>                default: 1\n"
            << "  --logon-timeout-ms <n>   default: 2000\n"
            << "  --timeout-ms <n>         default: 5000\n"
            << "  --poll-sleep-us <n>      default: 100\n";
    }

    ParseResult parse_args(int argc, char** argv, jolt::client::OrderTestConfig& cfg) {
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
            } else if (arg == "--sender") {
                cfg.sender_comp_id = need_value("--sender");
                if (cfg.sender_comp_id.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--target") {
                cfg.target_comp_id = need_value("--target");
                if (cfg.target_comp_id.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--account") {
                cfg.account = need_value("--account");
                if (cfg.account.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--symbol") {
                cfg.symbol = need_value("--symbol");
                if (cfg.symbol.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--side") {
                const std::string side = need_value("--side");
                if (side == "buy") {
                    cfg.is_buy = true;
                } else if (side == "sell") {
                    cfg.is_buy = false;
                } else {
                    std::cerr << "invalid --side value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--qty") {
                if (!parse_u64(need_value("--qty"), cfg.qty) || cfg.qty == 0) {
                    std::cerr << "invalid --qty value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--price") {
                if (!parse_u64(need_value("--price"), cfg.price) || cfg.price == 0) {
                    std::cerr << "invalid --price value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--tif") {
                uint64_t tif = 0;
                if (!parse_u64(need_value("--tif"), tif)) {
                    std::cerr << "invalid --tif value\n";
                    return ParseResult::Error;
                }
                cfg.tif = static_cast<int>(tif);
            } else if (arg == "--logon-timeout-ms") {
                if (!parse_u64(need_value("--logon-timeout-ms"), cfg.logon_timeout_ms)) {
                    std::cerr << "invalid --logon-timeout-ms value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--timeout-ms") {
                if (!parse_u64(need_value("--timeout-ms"), cfg.response_timeout_ms)) {
                    std::cerr << "invalid --timeout-ms value\n";
                    return ParseResult::Error;
                }
            } else if (arg == "--poll-sleep-us") {
                if (!parse_u64(need_value("--poll-sleep-us"), cfg.poll_sleep_us)) {
                    std::cerr << "invalid --poll-sleep-us value\n";
                    return ParseResult::Error;
                }
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
    jolt::client::OrderTestConfig cfg;
    const ParseResult parse_result = parse_args(argc, argv, cfg);
    if (parse_result != ParseResult::Ok) {
        print_usage(argv[0]);
        return parse_result == ParseResult::Help ? 0 : 1;
    }

    jolt::client::OrderTest test(cfg);
    const jolt::client::OrderTestResult result = test.run_once();
    if (!result.ok) {
        std::cerr << "[order-test] failed error=\"" << result.error << "\"\n";
        return 1;
    }

    std::cout << "[order-test] ok"
              << " new_count=" << result.new_rtt_ns.size()
              << " modify_count=" << result.modify_rtt_ns.size()
              << " cancel_count=" << result.cancel_rtt_ns.size()
              << std::fixed << std::setprecision(3)
              << " new_avg_rtt_ms=" << ns_to_ms(avg_u64(result.new_rtt_ns))
              << " new_min_rtt_ms=" << ns_to_ms(min_u64(result.new_rtt_ns))
              << " new_max_rtt_ms=" << ns_to_ms(max_u64(result.new_rtt_ns))
              << " modify_avg_rtt_ms=" << ns_to_ms(avg_u64(result.modify_rtt_ns))
              << " modify_min_rtt_ms=" << ns_to_ms(min_u64(result.modify_rtt_ns))
              << " modify_max_rtt_ms=" << ns_to_ms(max_u64(result.modify_rtt_ns))
              << " cancel_avg_rtt_ms=" << ns_to_ms(avg_u64(result.cancel_rtt_ns))
              << " cancel_min_rtt_ms=" << ns_to_ms(min_u64(result.cancel_rtt_ns))
              << " cancel_max_rtt_ms=" << ns_to_ms(max_u64(result.cancel_rtt_ns))
              << " new_rtt_ms=[" << format_ns_csv_as_ms(result.new_rtt_ns) << "]"
              << " modify_rtt_ms=[" << format_ns_csv_as_ms(result.modify_rtt_ns) << "]"
              << " cancel_rtt_ms=[" << format_ns_csv_as_ms(result.cancel_rtt_ns) << "]"
              << '\n';
    return 0;
}
