
#include "Exchange.h"
#include "../include/async_logger.h"

#include <cstring>
#include <limits>
#include <vector>
#include <xmmintrin.h>

namespace jolt::exchange {
    namespace {
        bool symbol_id_to_index(const uint64_t symbol_id, size_t& out_idx) {
            if (!jolt::is_valid_symbol_id(symbol_id)) {
                return false;
            }
            out_idx = static_cast<size_t>(symbol_id - jolt::kFirstSymbolId);
            return true;
        }

        const char* order_action_text(ob::OrderAction action) {
            switch (action) {
            case ob::OrderAction::New:
                return "New";
            case ob::OrderAction::Modify:
                return "Modify";
            case ob::OrderAction::Cancel:
                return "Cancel";
            }
            return "Unknown";
        }

        const char* exchange_msg_type_text(ExchToGtwyMsg::Type type) {
            switch (type) {
            case ExchToGtwyMsg::Type::Submitted:
                return "Submitted";
            case ExchToGtwyMsg::Type::Rejected:
                return "Rejected";
            case ExchToGtwyMsg::Type::Filled:
                return "Filled";
            }
            return "Unknown";
        }
    }

    Exchange::Exchange(ob::PriceTick min_tick,
                     ob::PriceTick max_tick,
                     const std::string& inbound_name,
                     const std::string& book_name,
                     const std::string& exch_name,
                     const std::string& risk_name,
                     const std::string& exch_to_risk_name,
                     const std::string& blob_name,
                     const std::string& meta_name,
                     const std::string& request_name)
        : snapshots_(), gtwy_exch(inbound_name, SharedRingMode::Create),
          mkt_data_gtwy(book_name, SharedRingMode::Create),
          exch_gtwy(exch_name, SharedRingMode::Create),
          exch_risk(risk_name, SharedRingMode::Create),
          risk_exch(exch_to_risk_name, SharedRingMode::Create),
          snapshot_pool_(blob_name, BlobMode::Create),
          snapshot_meta(meta_name, SharedRingMode::Create),
          requests_(request_name, SharedRingMode::Attach), writer_("../data") {
        orderbooks_.reserve(4);
        orderbook_seqs_.resize(4);

        for (auto& s : orderbook_seqs_) {
            s = 0;
        }

        for (auto m : mkt_data_) {
            m.reserve(1 << 10);
        }

        for (size_t i = 0; i < 4; ++i) {
            orderbooks_.emplace_back(std::make_unique<ob::MatchingOrderBook<>>(min_tick, max_tick));
        }

        for (auto& snapshot : snapshots_) {
            snapshot.orders.reserve(50'000);
        }
    }



    void Exchange::submit_order_direct(const ob::OrderParams& order) {
        handle_order(order);
    }

    bool Exchange::poll_once() {
        const uint64_t day = day_ticker_.day_id_atomic().load(std::memory_order_acquire);
        if (day != curr_day_) [[unlikely]] {
            curr_day_ = day;

            for (auto& book : orderbooks_) {
                book.get()->seq = 0;
            }
        }


        bool did_work = false;
        const size_t gtwy_drained = gtwy_exch.drain([&](const GtwyToExchMsg& msg) {
            log_info("[exch] exchange received order from gateway order_id=" +
                     std::to_string(msg.order.id) +
                     " client_id=" + std::to_string(msg.order.client_id) +
                     " action=" + std::string(order_action_text(msg.order.action)) +
                     " symbol_id=" + std::to_string(msg.order.symbol_id));
            handle_order(msg.order);
        });

        did_work = did_work || (gtwy_drained > 0);

        const bool poll_risk_now = (gtwy_drained == 0) || ((++risk_poll_tick_ & 0x7u) == 0);
        if (poll_risk_now) {
            const size_t risk_drained = risk_exch.drain([&](const RiskToExchMsg& msg) {
                handle_order(msg.order);
            });
            did_work = did_work || (risk_drained > 0);
        }

        return did_work;
    }

    void Exchange::start() {
        running.store(true, std::memory_order_release);
        day_ticker_.start();
    }

    void Exchange::stop() {
        running.store(false, std::memory_order_release);
        day_ticker_.stop();
    }

    void Exchange::process_loop() {
        while (running.load(std::memory_order_acquire)) {
            if (!poll_once()) {
                _mm_pause();
            }
        }
    }

    void Exchange::handle_order(const ob::OrderParams& order) {
        const uint16_t symbol_id = order.symbol_id;
        size_t symbol_idx = 0;
        if (!symbol_id_to_index(symbol_id, symbol_idx)) {
            log_error("[exch] exchange received invalid symbol_id from gateway symbol_id=" +
                      std::to_string(symbol_id) + " order_id=" + std::to_string(order.id) +
                      " client_id=" + std::to_string(order.client_id));
            ExchToGtwyMsg rej{};
            rej.type = ExchToGtwyMsg::Type::Rejected;
            rej.client_id = order.client_id;
            rej.order_id = order.id;
            rej.reason = ob::RejectReason::InvalidPrice;
            publish_exchange_msg(rej);
            return;
        }
        auto& book = *orderbooks_[symbol_idx];
        ob::BookEvent event = book.submit_order(order);
        auto seq = book.seq;

        event.seq = seq;
        if (orderbook_seqs_[symbol_idx] == 0) {
            orderbook_seqs_[symbol_idx] = seq;
        }

        if (seq - orderbook_seqs_[symbol_idx] >= 5'000) {
            auto& snapshot = snapshots_[snapshot_head_];
            book.get_snapshot(snapshot);
            snapshot_head_ = (snapshot_head_ + 1) % 4;
            orderbook_seqs_[symbol_idx] = seq;
        }

        if (event.event_type == ob::BookEventType::Reject) {
            ExchToGtwyMsg rej{};
            rej.type = ExchToGtwyMsg::Type::Rejected;
            rej.client_id = order.client_id;
            rej.order_id = order.id;
            publish_exchange_msg(rej);
            return;
        }

        const auto& fills = book.match_result.fills;
        if (!fills.empty()) {
            ob::Bbo b{book.get_best_bid(), book.get_best_ask()};
            ExchangeToRiskMsg risk_msg{};
            risk_msg.order = order;
            risk_msg.bid = b.best_bid;
            risk_msg.ask = b.best_ask;
            risk_msg.ts = order.ts;
            const auto fill_limit = risk_msg.fill_events_.size();
            const auto fills_to_copy = fills.size() < fill_limit ? fills.size() : fill_limit;
            risk_msg.num_fills = static_cast<uint64_t>(fills_to_copy);
            if (fills_to_copy > 0) {
                std::memcpy(
                    risk_msg.fill_events_.data(),
                    fills.data(),
                    fills_to_copy * sizeof(risk_msg.fill_events_[0]));
            }

            update_risk(risk_msg);
            for (const auto& fill_event : book.match_result.fills) {
                ob::L3Data data{};
                data.qty = fill_event.qty;
                data.id = fill_event.id;
                data.price = fill_event.price;
                data.event_type = fill_event.event_type;
                data.seq = fill_event.seq;
                data.symbol_id = symbol_id;
                data.side = fill_event.side;
                publish_book_event(data);

                ExchToGtwyMsg out{};
                out.filled = true;
                out.order_id = fill_event.id;
                out.fill_qty = fill_event.qty;
                out.type = ExchToGtwyMsg::Type::Filled;
                publish_exchange_msg(out);
            }

            book.match_result.fills.clear();
        }

        ExchToGtwyMsg ack{};
        ack.type = ExchToGtwyMsg::Type::Submitted;
        ack.reason = ob::RejectReason::NotApplicable;
        ack.client_id = order.client_id;
        ack.order_id = order.id;
        publish_exchange_msg(ack);

        ob::L3Data data{};
        data.qty = event.qty;
        data.event_type = event.event_type;
        data.seq = event.seq;
        data.side = event.side;
        data.price = event.price;
        data.event_type = event.event_type;
        data.symbol_id = symbol_id;
        publish_book_event(data);

        mkt_data_[symbol_idx].push_back(data);
        if (mkt_data_[symbol_idx].size() >= 1 << 10) {
            writer_.write_batch(symbol_id, mkt_data_[symbol_idx].data(), mkt_data_[symbol_idx].size());
            mkt_data_[symbol_idx].clear();
        }
    }

    void Exchange::update_risk(const ExchangeToRiskMsg& msg) {
        exch_risk.enqueue(msg);
    }

    void Exchange::publish_exchange_msg(const ExchToGtwyMsg& msg) {
        if (!exch_gtwy.enqueue(msg)) {
            log_error("[exch] exchange->gateway enqueue failed type=" +
                      std::string(exchange_msg_type_text(msg.type)) +
                      " order_id=" + std::to_string(msg.order_id) +
                      " client_id=" + std::to_string(msg.client_id));
            return;
        }
        log_info("[exch] exchange responded to gateway type=" +
                 std::string(exchange_msg_type_text(msg.type)) +
                 " order_id=" + std::to_string(msg.order_id) +
                 " client_id=" + std::to_string(msg.client_id));
    }

    void Exchange::publish_book_event(const ob::L3Data& data) {
        (void)mkt_data_gtwy.enqueue(data);
    }

    void Exchange::handle_snapshot_request(uint64_t symbol_id, uint64_t request_seq, uint64_t request_id, uint64_t session_id)  {
        (void)request_seq;
        size_t symbol_idx = 0;
        if (!symbol_id_to_index(symbol_id, symbol_idx)) {
            md::SnapshotMeta meta{};
            meta.accepted = false;
            meta.request_id = request_id;
            meta.session_id = session_id;
            if (symbol_id <= std::numeric_limits<uint16_t>::max()) {
                meta.symbol_id = static_cast<uint16_t>(symbol_id);
            }
            snapshot_meta.enqueue(meta);
            return;
        }

        auto& book = orderbooks_[symbol_idx];
        ob::BookSnapshot snapshot{};
        book->get_snapshot(snapshot);
        md::SnapshotMeta meta{};
        meta.ask_ct = snapshot.ask_ct;
        meta.bid_ct = snapshot.bid_ct;
        meta.accepted = true;
        meta.snapshot_seq = snapshot.seq;
        meta.bytes = (snapshot.ask_ct + snapshot.bid_ct) * sizeof(ob::SnapshotOrder);
        meta.request_id = request_id;
        meta.symbol_id = static_cast<uint16_t>(symbol_id);
        meta.session_id = session_id;
        size_t idx = UINT16_MAX;
        snapshot_pool_.try_acquire(idx);
        if (idx == UINT16_MAX) {
            meta.accepted = false;
            snapshot_meta.enqueue(meta);
            return;
        }

        auto& slot = snapshot_pool_.writer_slot(idx);
        std::memcpy(slot.payload.data(), snapshot.orders.data(), meta.bytes);
        snapshot_pool_.publish_ready(idx);
        meta.slot_id = idx;
        meta.symbol_id = static_cast<uint16_t>(symbol_id);
        snapshot_meta.enqueue(meta);
    }

    void Exchange::poll_requests() {
        (void)requests_.drain([&](const md::DataRequest& req) {
            handle_snapshot_request(req.symbol_id, 0, req.request_id, req.session_id);
        });
    }


}
