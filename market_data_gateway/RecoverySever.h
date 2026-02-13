//
// Created by djaiswal on 2/8/26.
//

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <array>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include "../include/shared_mem_blob.h"

#include <sys/epoll.h>

#include "MarketDataTypes.h"
#include "include/SharedMemoryRing.h"
#include "include/Types.h"

namespace jolt::md {
    class RecoverySever {

        using SnapshotRequestQ = SharedSpscQueue<SnapshotRequest, 1 << 8>;
        using SnapshotMetaQ = SharedSpscQueue<SnapshotMeta, 1 << 8>;
        using SnapshotPool = SnapshotBlobPool<64, 1 << 20>;


        static constexpr size_t kRxCap = 64 * 1024;

        struct DataSession {
            explicit DataSession(int fd, uint64_t id) : fd_(fd), session_id_(id) {}

            struct TxItem {
                enum class Kind : uint8_t {Snapshot = 0, L3 = 1, Header = 2};

                size_t offset;
                Kind kind;
                uint32_t bytes;
                uint16_t slot_idx;
                std::vector<char> payload;
            };

            int fd_{-1};
            uint64_t session_id_{0};
            std::array<char, kRxCap> rx_buf_{};
            size_t rx_len_{0};
            size_t rx_off_{0};
            std::deque<TxItem> tx_buf_{};
            bool closed_{false};
        };

        void accept_sessions();
        void update_interest(int fd, uint64_t id, bool want_write);
        DataSession* lookup(uint64_t id);
        bool send_pending(DataSession& session);
        void recv_pending(DataSession& session);
        void on_readable(DataSession& session);
        void handle_read(DataSession& session);
        void close_session(uint64_t id, DataSession& session);
        void remove_session(uint64_t id, int fd);
        void handle_snapshot_request(uint64_t request_id, uint64_t session_id, uint64_t symbol_id);
        void handle_snapshot_response();
        void handle_retransmission_request(uint64_t request_id, uint64_t session_id, uint64_t symbol_id, uint64_t start_seq, uint64_t end_seq);
        SnapshotPool snapshot_pool_;
        std::thread run_thread_{};
        std::atomic<bool> running_{false};
        int epoll_fd_{-1};
        int listen_fd_{-1};
        std::string listen_host_{};
        uint16_t listen_port_{0};
        uint64_t session_id_assign_{0};
        std::unordered_map<uint64_t, std::unique_ptr<DataSession>> sessions_{};
        std::vector<epoll_event> events_{};

        SnapshotRequestQ snapshot_request_q_;
        SnapshotMetaQ snapshot_meta_q_;

    public:
        RecoverySever(const std::string& host, uint16_t port, const std::string& blob_name, const std::string& meta_name, const std::string& request_name);
        virtual ~RecoverySever();

        RecoverySever(const RecoverySever&) = delete;
        RecoverySever& operator=(const RecoverySever&) = delete;
        RecoverySever(RecoverySever&&) = delete;
        RecoverySever& operator=(RecoverySever&&) = delete;

        void poll_once(int timeout_ms);
        void start();
        void run();
        void stop();

        bool queue_message(uint64_t session_id, std::string_view payload);
        size_t connection_count() const;
    };
}