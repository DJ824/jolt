#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../exchange/orderbook/flat_map.h"
#include "../include/SharedMemoryRing.h"
#include "../include/Types.h"
#include "../include/orderstatepool.h"
#include "../include/spsc.h"
#include "Client.h"
#include "EventLoop.h"
#include "GatewayTypes.h"

namespace jolt::gateway {
    struct ClOrdMapKey {
        static constexpr uint8_t kEmptyLen = 0;
        static constexpr uint8_t kTombstoneLen = 0xFF;

        std::array<char, kOrderStateTextMaxLen> bytes{};
        uint8_t len{kEmptyLen};

        static ClOrdMapKey empty() noexcept {
            return {};
        }

        static ClOrdMapKey tombstone() noexcept {
            ClOrdMapKey key{};
            key.len = kTombstoneLen;
            return key;
        }
    };

    inline bool operator==(const ClOrdMapKey& lhs, const ClOrdMapKey& rhs) noexcept {
        if (lhs.len != rhs.len) {
            return false;
        }
        if (lhs.len == ClOrdMapKey::kEmptyLen || lhs.len == ClOrdMapKey::kTombstoneLen) {
            return true;
        }
        for (size_t i = 0; i < lhs.len; ++i) {
            if (lhs.bytes[i] != rhs.bytes[i]) {
                return false;
            }
        }
        return true;
    }

    static_assert(kOrderStateTextMaxLen < ClOrdMapKey::kTombstoneLen,
                  "cl_ord_id key length must leave room for tombstone sentinel");

    struct ClOrdMapKeyHash {
        size_t operator()(const ClOrdMapKey& key) const noexcept {
            if (key.len == ClOrdMapKey::kEmptyLen) {
                return 0x9e3779b97f4a7c15ull;
            }
            if (key.len == ClOrdMapKey::kTombstoneLen) {
                return 0xc2b2ae3d27d4eb4full;
            }
            return std::hash<std::string_view>{}(std::string_view(key.bytes.data(), key.len));
        }
    };

    class FixGateway {
        struct ClientTrafficStats {
            uint64_t received_from_client{0};
            uint64_t sent_to_client{0};
        };

        bool risk_check(const ClientInfo& client, const ob::OrderParams& order, ob::RejectReason& reason) const;
        void handle_exchange_msg(const ExchToGtwyMsg& msg);
        void queue_fix_message(const FixMessage& msg);
        SessionState* get_or_create_session(uint64_t session_id);
        void exchange_rx_loop();

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
        LockFreeQueue<ExchToGtwyMsg, 1 << 20> exchange_events_;
        ob::FlatMap<uint64_t, ClientInfo> client_infos_;
        ob::FlatMap<ClOrdMapKey, uint64_t, ClOrdMapKeyHash> cl_ord_id_to_order_id_;
        SlabPool<OrderState> order_state_pool_;
        uint64_t next_order_id_{1};
        uint64_t next_exec_id_{1};
        EventLoop event_loop_;
        std::unordered_map<uint64_t, ClientTrafficStats> client_traffic_;
        std::mutex client_traffic_mu_{};
        std::chrono::steady_clock::time_point next_client_traffic_log_{};
        std::atomic<bool> running_{false};
        std::thread exchange_rx_thread_;

    public:
        FixGateway(const std::string& gtwy_to_exch_name, const std::string& exch_to_gtwy_name);
        void start();
        void stop();
        void load_clients(const std::vector<ClientInfo>& clients);
        bool submit_order(const ob::OrderParams& order, ob::RejectReason& reason);
        bool on_fix_message(std::string_view message, uint64_t session_id);
        void on_disconnect(uint64_t session_id);
        void drain_exchange_events();
        void poll();
        std::unordered_map<uint64_t, std::unique_ptr<Client>> clients_;
        void clear_session_for_client(uint64_t client_id);
        std::vector<SessionState> sessions_;
    };
}
