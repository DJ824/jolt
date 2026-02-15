//
// Created by djaiswal on 2/15/26.
//

#include "MarketDataClient.h"

#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
    constexpr char kFixDelim = '\x01';

    bool parse_u16(std::string_view text, uint16_t& out) {
        if (text.empty()) {
            return false;
        }
        uint64_t value = 0;
        for (char ch : text) {
            if (ch < '0' || ch > '9') {
                return false;
            }
            value = (value * 10) + static_cast<uint64_t>(ch - '0');
            if (value > 65535u) {
                return false;
            }
        }
        out = static_cast<uint16_t>(value);
        return true;
    }

    bool is_ipv4_multicast(const in_addr& addr) {
        const uint32_t ip = ntohl(addr.s_addr);
        return (ip & 0xF0000000u) == 0xE0000000u;
    }
}

namespace jolt::client {
    MarketDataClient::MarketDataClient(const MarketDataClientConfig& cfg) : cfg_(cfg) {
    }

    MarketDataClient::~MarketDataClient() {
        close_udp();
    }

    std::string_view MarketDataClient::find_tag(std::string_view msg, std::string_view tag_with_eq) {
        size_t pos = 0;
        while (pos < msg.size()) {
            const size_t end = msg.find(kFixDelim, pos);
            const size_t field_end = end == std::string_view::npos ? msg.size() : end;
            const std::string_view field = msg.substr(pos, field_end - pos);
            if (field.size() > tag_with_eq.size() && field.substr(0, tag_with_eq.size()) == tag_with_eq) {
                return field.substr(tag_with_eq.size());
            }
            if (end == std::string_view::npos) {
                break;
            }
            pos = end + 1;
        }
        return {};
    }

    bool MarketDataClient::await_logon() {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.logon_timeout_ms);

        while (std::chrono::steady_clock::now() < deadline) {
            if (!fix_.poll()) {
                return false;
            }

            while (const auto msg = fix_.next_message()) {
                const std::string_view type = find_tag(*msg, "35=");
                if (type == "A") {
                    return true;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
    }

    bool MarketDataClient::send_subscribe() {
        std::vector<std::pair<int, std::string>> fields;
        fields.emplace_back(262, cfg_.md_req_id);
        fields.emplace_back(263, "1");
        fields.emplace_back(55, cfg_.symbol);
        fields.emplace_back(265, "1");
        fields.emplace_back(266, "N");
        return fix_.send_message("V", fields);
    }

    bool MarketDataClient::await_subscribe_response(SubscribeEndpoints& endpoints) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.subscribe_timeout_ms);

        while (std::chrono::steady_clock::now() < deadline) {
            if (!fix_.poll()) {
                return false;
            }

            while (const auto msg = fix_.next_message()) {
                const std::string_view type = find_tag(*msg, "35=");
                if (type == "U") {
                    const std::string_view group = find_tag(*msg, "13000=");
                    const std::string_view port = find_tag(*msg, "13001=");
                    if (group.empty() || port.empty()) {
                        return false;
                    }

                    uint16_t udp_port = 0;
                    if (!parse_u16(port, udp_port)) {
                        return false;
                    }

                    endpoints.group = std::string(group);
                    endpoints.port = udp_port;

                    if (const std::string_view recovery_host = find_tag(*msg, "13002="); !recovery_host.empty()) {
                        endpoints.recovery_host = std::string(recovery_host);
                    }
                    if (const std::string_view recovery_port = find_tag(*msg, "13003="); !recovery_port.empty()) {
                        uint16_t parsed_recovery_port = 0;
                        if (!parse_u16(recovery_port, parsed_recovery_port)) {
                            return false;
                        }
                        endpoints.recovery_port = parsed_recovery_port;
                    }
                    return true;
                }

                if (type == "Y") {
                    const std::string_view reason_code = find_tag(*msg, "281=");
                    const std::string_view reason_text = find_tag(*msg, "58=");
                    std::cerr << "[md-client] subscription rejected"
                              << " code=" << (reason_code.empty() ? "-" : std::string(reason_code))
                              << " reason=" << (reason_text.empty() ? "-" : std::string(reason_text))
                              << "\n";
                    return false;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
    }

    bool MarketDataClient::connect_udp(const SubscribeEndpoints& endpoints) {
        in_addr group_addr{};
        if (::inet_pton(AF_INET, endpoints.group.c_str(), &group_addr) != 1) {
            std::cerr << "[md-client] invalid UDP group address: " << endpoints.group << "\n";
            return false;
        }

        const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) {
            std::cerr << "[md-client] failed creating UDP socket errno=" << errno << "\n";
            return false;
        }

        int one = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
            std::cerr << "[md-client] failed setting SO_REUSEADDR errno=" << errno << "\n";
            ::close(fd);
            return false;
        }

        sockaddr_in bind_addr{};
        bind_addr.sin_family = AF_INET;
        bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        bind_addr.sin_port = htons(endpoints.port);
        if (::bind(fd, reinterpret_cast<sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0) {
            std::cerr << "[md-client] failed binding UDP socket port=" << endpoints.port
                      << " errno=" << errno << "\n";
            ::close(fd);
            return false;
        }

        if (is_ipv4_multicast(group_addr)) {
            ip_mreq mreq{};
            mreq.imr_multiaddr = group_addr;
            mreq.imr_interface.s_addr = htonl(INADDR_ANY);
            if (::setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) != 0) {
                std::cerr << "[md-client] failed joining multicast group=" << endpoints.group
                          << " errno=" << errno << "\n";
                ::close(fd);
                return false;
            }
        }

        sockaddr_in remote{};
        remote.sin_family = AF_INET;
        remote.sin_addr = group_addr;
        remote.sin_port = htons(endpoints.port);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&remote), sizeof(remote)) != 0) {
            std::cerr << "[md-client] failed connecting UDP socket endpoint=" << endpoints.group
                      << ":" << endpoints.port
                      << " errno=" << errno << "\n";
            ::close(fd);
            return false;
        }

        udp_fd_ = fd;

        std::cout << "[md-client] subscribed symbol=" << cfg_.symbol
                  << " udp_endpoint=" << endpoints.group << ":" << endpoints.port;
        if (!endpoints.recovery_host.empty() && endpoints.recovery_port != 0) {
            std::cout << " recovery_endpoint=" << endpoints.recovery_host << ":" << endpoints.recovery_port;
        }
        std::cout << "\n";

        return true;
    }

    bool MarketDataClient::drain_udp(const uint64_t listen_ms) const {
        if (listen_ms == 0) {
            return true;
        }

        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(listen_ms);
        std::array<char, 2048> buf{};
        uint64_t datagrams = 0;
        uint64_t bytes = 0;

        while (std::chrono::steady_clock::now() < deadline) {
            const ssize_t n = ::recv(udp_fd_, buf.data(), buf.size(), MSG_DONTWAIT);
            if (n > 0) {
                ++datagrams;
                bytes += static_cast<uint64_t>(n);
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            if (n < 0) {
                std::cerr << "[md-client] UDP recv failed errno=" << errno << "\n";
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        std::cout << "[md-client] udp_rx datagrams=" << datagrams << " bytes=" << bytes << "\n";
        return true;
    }

    void MarketDataClient::close_udp() {
        if (udp_fd_ != -1) {
            ::close(udp_fd_);
            udp_fd_ = -1;
        }
    }

    bool MarketDataClient::run() {
        fix_.set_session(cfg_.sender_comp_id, cfg_.target_comp_id);
        fix_.set_account(cfg_.sender_comp_id);

        if (!fix_.connect_tcp(cfg_.host, cfg_.port)) {
            std::cerr << "[md-client] failed to connect control session host=" << cfg_.host
                      << " port=" << cfg_.port << "\n";
            return false;
        }

        if (!fix_.send_raw(fix_.build_logon(30))) {
            std::cerr << "[md-client] failed sending FIX logon\n";
            return false;
        }

        if (!await_logon()) {
            std::cerr << "[md-client] timed out waiting for logon ack\n";
            return false;
        }

        if (!send_subscribe()) {
            std::cerr << "[md-client] failed sending market data subscribe request\n";
            return false;
        }

        SubscribeEndpoints endpoints{};
        if (!await_subscribe_response(endpoints)) {
            std::cerr << "[md-client] did not receive market data subscribe response\n";
            return false;
        }

        if (!connect_udp(endpoints)) {
            return false;
        }

        return drain_udp(cfg_.udp_listen_ms);
    }
}
