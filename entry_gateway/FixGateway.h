#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include "fcntl.h"
#include <unistd.h>
#include "../include/spsc.h"
#include "../include/SharedMemoryRing.h"
#include "../exchange/orderbook/flat_map.h"
#include "../include/Types.h"
#include "Client.h"
#include "EventLoop.h"
#include "FixSession.h"
#include "GatewayTypes.h"

namespace jolt::gateway {
    class FixGateway {

        bool risk_check(const ClientInfo& client, const ob::OrderParams& order, ob::RejectReason& reason) const;
        void handle_exchange_msg(const ExchToGtwyMsg& msg);
        void queue_fix_message(const FixMessage& msg);
        SessionState* get_or_create_session(uint64_t session_id);

        static bool build_exec_report(FixMessage& out,
                                      SessionState* session,
                                      const OrderState& state,
                                      uint64_t exec_id,
                                      bool accepted,
                                      ob::RejectReason reason);
        static bool build_logon(FixMessage& out,
                                SessionState* session,
                                uint32_t heartbeat_int,
                                bool reset_seq);

        GtwyToExch gtwy_exch_;
        ExchToGtwy exch_gtwy_;
        ob::FlatMap<uint64_t, ClientInfo> client_infos_;
        std::unordered_map<std::string, OrderState*> order_states_;
        std::unordered_map<uint64_t, OrderState*> order_id_to_state_;
        std::deque<FixMessage> outbound_msgs_;
        uint64_t next_order_id_{1};
        uint64_t next_exec_id_{1};
        std::array<char, 1024> recv_buf_;
        std::array<char, 1024> send_buf_;
        EventLoop event_loop_;

    public:
        FixGateway(const std::string& gtwy_to_exch_name, const std::string& exch_to_gtwy_name);
        void start();
        void stop();
        void load_clients(const std::vector<ClientInfo>& clients);
        bool submit_order(const ob::OrderParams& order, ob::RejectReason& reason);
        bool on_fix_message(std::string_view message, uint64_t session_id);
        void poll();
        void poll_exchange();
        void poll_io();
        std::unordered_map<uint64_t, std::unique_ptr<Client>> clients_;
        void clear_session_for_client(uint64_t client_id);
        LockFreeQueue<FixMessage, 1 << 10> outbound_;
        LockFreeQueue<FixMessage, 1 << 10> inbound_;
        std::vector<SessionState> sessions_;

    };
}
