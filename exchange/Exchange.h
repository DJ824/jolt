//
// Created by djaiswal on 1/16/26.
//

#pragma once
#include <atomic>
#include <cstdint>
#include <string>

#include "DayTicker.h"
#include "orderbook/matching_orderbook.h"
#include "../include/Types.h"
#include "../entry_gateway/FixGateway.h"
#include "../risk/RiskEngine.h"
#include "../include/SharedMemoryRing.h"
#include "../include/shared_mem_blob.h"
#include "../include/mkt_data_writer.h"
#include "market_data_gateway/MarketDataTypes.h"

namespace jolt::exchange {

    static constexpr size_t NUM_SHARDS = 4;
    class Exchange {
    public:
        using GtwyToExch = SharedSpscQueue<GtwyToExchMsg, 1 << 20>;
        using MktDataQueue = SharedSpscQueue<ob::L3Data, 1 << 15>;
        using ExchToGtwy = SharedSpscQueue<ExchToGtwyMsg, 1 << 20>;
        using ExchToRisk = SharedSpscQueue<ExchangeToRiskMsg, 1 << 15>;
        using RiskToExch = SharedSpscQueue<RiskToExchMsg, 1 << 15>;
        using SnapshotMetaQ = SharedSpscQueue<md::SnapshotMeta, 1 << 8>;
        using SnapshotChunkQ = SharedSpscQueue<SnapshotChunk, 1 << 15>;
        using SnapshotBlob = SnapshotBlobPool<64, 1 << 20>;
        using RequestQ = SharedSpscQueue<md::DataRequest, 1 << 8>;


        Exchange(ob::PriceTick min_tick, ob::PriceTick max_tick, const std::string& inbound_name,
                 const std::string& book_name,
                 const std::string& exch_name, const std::string& risk_name, const std::string& exch_to_risk_name,
                 const std::string& blob_name, const std::string& meta_name, const std::string& request_name);
        void submit_order_direct(const ob::OrderParams& order);
        bool poll_once();
        void process_loop();
        void start();
        void stop();
        void handle_snapshot_request(uint64_t symbol_id, uint64_t request_seq, uint64_t request_id, uint64_t session_id);
        void poll_requests();

    private:
        void handle_order(const ob::OrderParams& order);
        void update_risk(const ExchangeToRiskMsg& msg);
        void publish_exchange_msg(const ExchToGtwyMsg& msg);
        void publish_book_event(const ob::L3Data& data);

        uint64_t seq_{0};
        uint64_t curr_day_{0};
        std::atomic<bool> running{false};
        std::vector<std::unique_ptr<ob::MatchingOrderBook<>>> orderbooks_;
        std::vector<uint64_t> orderbook_seqs_;
        std::array<ob::BookSnapshot, 4> snapshots_;
        std::array<std::vector<ob::L3Data>, 4> mkt_data_;
        size_t snapshot_head_{0};

        ob::PriceTick prev_bid_{0};
        ob::PriceTick prev_ask_{0};
        GtwyToExch gtwy_exch;
        MktDataQueue mkt_data_gtwy;
        ExchToGtwy exch_gtwy;
        ExchToRisk exch_risk;
        RiskToExch risk_exch;
        SnapshotBlob snapshot_pool_;
        SnapshotMetaQ snapshot_meta;
        RequestQ requests_;
        ob::FlatMap<uint64_t, ClientInfo> clients_;
        L3DataWriter writer_;
        DayTicker day_ticker_;
    };
}
