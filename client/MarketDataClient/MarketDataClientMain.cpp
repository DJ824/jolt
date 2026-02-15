//
// Created by djaiswal on 2/15/26.
//

#include "MarketDataClient.h"

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>

namespace {
    using Config = jolt::client::MarketDataClientConfig;

    enum class ParseResult : uint8_t {
        Ok = 0,
        Help = 1,
        Error = 2,
    };

    bool parse_u64(const std::string& text, uint64_t& out) {
        std::istringstream iss(text);
        iss >> out;
        return !iss.fail() && iss.eof();
    }

    void print_usage(const char* prog) {
        std::cerr
            << "Usage: " << prog << " [options]\n"
            << "  --host <ip-or-host>             default: 127.0.0.1\n"
            << "  --port <tcp-port>               default: 80\n"
            << "  --sender <SenderCompID>         default: MD_CLIENT_1\n"
            << "  --target <TargetCompID>         default: MARKET_DATA_GATEWAY\n"
            << "  --symbol <symbol>               default: 1\n"
            << "  --req-id <md-req-id>            default: 1\n"
            << "  --logon-timeout-ms <ms>         default: 2000\n"
            << "  --subscribe-timeout-ms <ms>     default: 2000\n"
            << "  --udp-listen-ms <ms>            default: 1000 (0 disables receive loop)\n";
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
            } else if (arg == "--symbol") {
                cfg.symbol = need_value("--symbol");
                if (cfg.symbol.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--req-id") {
                cfg.md_req_id = need_value("--req-id");
                if (cfg.md_req_id.empty()) {
                    return ParseResult::Error;
                }
            } else if (arg == "--logon-timeout-ms") {
                uint64_t value = 0;
                if (!parse_u64(need_value("--logon-timeout-ms"), value)) {
                    std::cerr << "invalid --logon-timeout-ms\n";
                    return ParseResult::Error;
                }
                cfg.logon_timeout_ms = value;
            } else if (arg == "--subscribe-timeout-ms") {
                uint64_t value = 0;
                if (!parse_u64(need_value("--subscribe-timeout-ms"), value)) {
                    std::cerr << "invalid --subscribe-timeout-ms\n";
                    return ParseResult::Error;
                }
                cfg.subscribe_timeout_ms = value;
            } else if (arg == "--udp-listen-ms") {
                uint64_t value = 0;
                if (!parse_u64(need_value("--udp-listen-ms"), value)) {
                    std::cerr << "invalid --udp-listen-ms\n";
                    return ParseResult::Error;
                }
                cfg.udp_listen_ms = value;
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
    Config cfg{};
    const ParseResult parse_result = parse_args(argc, argv, cfg);
    if (parse_result != ParseResult::Ok) {
        print_usage(argv[0]);
        return parse_result == ParseResult::Help ? 0 : 1;
    }

    std::cout
        << "[md-client] control=" << cfg.host << ":" << cfg.port
        << " sender=" << cfg.sender_comp_id
        << " target=" << cfg.target_comp_id
        << " symbol=" << cfg.symbol
        << " req_id=" << cfg.md_req_id
        << "\n";

    jolt::client::MarketDataClient client(cfg);
    return client.run() ? 0 : 2;
}
