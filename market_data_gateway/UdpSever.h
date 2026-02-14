//
// Created by djaiswal on 2/8/26.
//

#ifndef JOLT_UDPSEVER_H
#define JOLT_UDPSEVER_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <netinet/in.h>

#include "MarketDataTypes.h"
#include "../exchange/orderbook/ob_types.h"
#include "../include/SharedMemoryRing.h"
#include "include/Types.h"


namespace jolt::md {
    class UdpSever {
        struct L3Header {
            uint64_t first_seq;
            uint16_t count;
            uint16_t symbol_id;
            uint16_t payload_len;
            uint16_t magic;
            uint8_t msg_type;
            uint8_t version;
        };

        using MktDataQ = SharedSpscQueue<ob::L3Data, 1 << 20>;



        static constexpr uint16_t kMagic = 0x4D44;
        static constexpr uint8_t kMsgType = 1;
        static constexpr uint8_t kVersion = 1;
        static constexpr size_t kMaxDatagram = 1600;
        static constexpr size_t BUFFER_SIZE = 38;

        int fd_{-1};
        std::array<char, kMaxDatagram> buf_{};
        std::unordered_map<uint16_t, sockaddr_in> channels_{};
        std::array<std::vector<ob::L3Data>, jolt::kNumSymbols> symbol_buffers_{};
        MktDataQ mkt_data_q_;

    public:
        UdpSever(const std::string& queue_name);
        ~UdpSever();

        UdpSever(const UdpSever&) = delete;
        UdpSever& operator=(const UdpSever&) = delete;
        UdpSever(UdpSever&&) = delete;
        UdpSever& operator=(UdpSever&&) = delete;


        void poll_mkt_data();
        void configure_default_channels(size_t num_symbols, const std::string& multicast_ip, uint16_t base_port);
        void add_symbol_channel(uint16_t symbol_id, const std::string& ip, uint16_t port);
        bool send_batch(uint16_t symbol_id, const ob::L3Data* batch, size_t count);
    };
}

#endif //JOLT_UDPSEVER_H
