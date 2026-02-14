//
// Created by djaiswal on 2/8/26.
//

#include "UdpSever.h"

#include <arpa/inet.h>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <sys/socket.h>
#include <unistd.h>

namespace jolt::md {
    sockaddr_in make_udp_dst(const std::string& ip, uint16_t port) {
        sockaddr_in dst{};
        dst.sin_family = AF_INET;
        dst.sin_port = htons(port);
        if (inet_pton(AF_INET, ip.c_str(), &dst.sin_addr) <= 0) {
            throw std::runtime_error("inet_pton failed");
        }
        return dst;
    }

    bool symbol_id_to_index(const uint16_t symbol_id, size_t& out_idx) {
        if (!jolt::is_valid_symbol_id(symbol_id)) {
            return false;
        }
        out_idx = static_cast<size_t>(symbol_id - jolt::kFirstSymbolId);
        return true;
    }

    UdpSever::UdpSever(const std::string& queue_name) : mkt_data_q_(queue_name, SharedRingMode::Attach) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) {
            throw std::runtime_error("err creating udp socket");
        }

        uint8_t ttl = 1;
        if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) != 0) {
            throw std::runtime_error("err setting multicast ttl");
        }

        uint8_t loop = 0;
        if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop)) != 0) {
            throw std::runtime_error("err setting multicast loop");
        }

        int tos = 0xB8;
        if (::setsockopt(fd_, IPPROTO_IP, IP_TOS, &tos, sizeof(tos)) != 0) {
            throw std::runtime_error("err setting to TOS");
        }

        int sz = 1 << 20;
        if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz)) != 0) {
            throw std::runtime_error("err setting SO_SNDBUF");
        }

        for (int i = 0; i < 4; i++) {
        }
    }

    UdpSever::~UdpSever() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }


    void UdpSever::configure_default_channels(const size_t num_symbols,
                                              const std::string& multicast_ip,
                                              const uint16_t base_port) {
        channels_.clear();
        for (size_t i = 0; i < num_symbols; ++i) {
            add_symbol_channel(static_cast<uint16_t>(jolt::kFirstSymbolId + i),
                               multicast_ip,
                               static_cast<uint16_t>(base_port + i));
        }
    }

    void UdpSever::add_symbol_channel(const uint16_t symbol_id, const std::string& ip, const uint16_t port) {
        channels_[symbol_id] = make_udp_dst(ip, port);
    }

    bool UdpSever::send_batch(const uint16_t symbol_id, const ob::L3Data* batch, const size_t count) {
        if (count == 0 || batch == nullptr) {
            return false;
        }

        const auto it = channels_.find(symbol_id);
        if (it == channels_.end()) {
            return false;
        }

        const size_t payload_len = count * sizeof(ob::L3Data);
        const size_t total_len = sizeof(L3Header) + payload_len;
        if (total_len > buf_.size()) {
            return false;
        }

        L3Header hdr{};
        hdr.symbol_id = htons(symbol_id);
        hdr.first_seq = batch[0].seq;
        hdr.count = htons(static_cast<uint16_t>(count));
        hdr.payload_len = static_cast<uint16_t>(payload_len);
        hdr.magic = kMagic;
        hdr.msg_type = kMsgType;
        hdr.version = kVersion;

        std::memcpy(buf_.data(), &hdr, sizeof(hdr));
        std::memcpy(buf_.data() + sizeof(hdr), batch, payload_len);

        const sockaddr_in& dst = it->second;
        const ssize_t sent = ::sendto(fd_,
                                      buf_.data(),
                                      total_len,
                                      0,
                                      reinterpret_cast<const sockaddr*>(&dst),
                                      sizeof(dst));
        return sent == static_cast<ssize_t>(total_len);
    }

    void UdpSever::poll_mkt_data() {
        for (;;) {
            auto msg = mkt_data_q_.dequeue();
            if (msg) {
                const uint16_t symbol_id = msg->symbol_id;
                size_t symbol_idx = 0;
                if (!symbol_id_to_index(symbol_id, symbol_idx)) {
                    continue;
                }
                auto& buf = symbol_buffers_[symbol_idx];
                buf.push_back(*msg);
                if (buf.size() == BUFFER_SIZE) {
                    send_batch(symbol_id, buf.data(), buf.size());
                    buf.clear();
                }
            }
        }
    }
}
