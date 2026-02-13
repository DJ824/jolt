#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../exchange/orderbook/ob_types.h"
#include "../include/SharedMemoryRing.h"
#include "../include/spsc.h"
#include "ControlEventLoop.h"
#include "MarketDataTypes.h"
#include "UdpSever.h"
#include "exchange/orderbook/flat_map.h"
#include "include/shared_mem_blob.h"
#include "include/Types.h"

namespace jolt::md {
    class MarketDataGateway {

        static constexpr size_t NUM_SYMBOLS = 10;
        static constexpr size_t BATCH_SZ = 38;



        std::vector<std::array<ob::L3Data, BATCH_SZ>> symbol_buffers_{NUM_SYMBOLS};
        std::vector<size_t> batch_sizes_{NUM_SYMBOLS};
        ControlEventLoop event_loop_;


        uint64_t request_id_{0};
        std::unordered_map<uint64_t, SessionState> sessions_;
        std::unordered_map<std::string, ChannelInfo> channels_;
        std::unordered_map<std::string, std::vector<uint64_t>> symbol_subs_;
        std::unordered_map<uint64_t, std::unordered_set<std::string>> session_subs_;
        std::unordered_map<std::string, uint64_t> symbol_to_id_;
        std::string recovery_host_{};
        uint16_t recovery_port_{0};


        bool build_logon(FixMessage& out, SessionState& session, uint32_t heartbeat_int);
        bool build_subscribe_response(FixMessage& out,
                                      SessionState& session,
                                      std::string_view req_id,
                                      std::string_view symbol,
                                      const ChannelInfo& channel);
        bool build_md_reject(FixMessage& out,
                             SessionState& session,
                             std::string_view req_id,
                             uint32_t reason_code,
                             std::string_view reason_text);



    public:

        MarketDataGateway();
        void setup();
        void poll();
        void poll_io();
        bool on_fix_message(std::string_view message, uint64_t session_id);
        void on_disconnect(uint64_t session_id);

        void add_symbol_channel(const std::string& symbol, const std::string& group, uint16_t port);
        void set_recovery_endpoint(const std::string& host, uint16_t port);
        void queue_fix_message(const FixMessage& msg);

        LockFreeQueue<FixMessage, 8192> inbound_;
        LockFreeQueue<FixMessage, 8192> outbound_;


    };
}