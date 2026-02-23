#include "OrderTest.h"

#include <cstddef>
#include <limits>
#include <thread>
#include <utility>

namespace {
    constexpr char kFixDelim = '\x01';
    constexpr size_t kBatchSize = 10;
}

namespace jolt::client {
    OrderTest::OrderTest(OrderTestConfig cfg) : cfg_(std::move(cfg)) {
        fix_.set_session(cfg_.sender_comp_id, cfg_.target_comp_id);
        fix_.set_account(cfg_.account);
    }

    std::string_view OrderTest::find_tag(std::string_view msg, std::string_view tag_with_eq) {
        size_t pos = 0;
        while (pos < msg.size()) {
            const size_t end = msg.find(kFixDelim, pos);
            const size_t field_end = (end == std::string_view::npos) ? msg.size() : end;
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

    bool OrderTest::has_matching_cl_ord_id(std::string_view msg, std::string_view cl_ord_id) {
        const std::string_view tag_11 = find_tag(msg, "11=");
        if (!tag_11.empty() && tag_11 == cl_ord_id) {
            return true;
        }

        const std::string_view tag_41 = find_tag(msg, "41=");
        return !tag_41.empty() && tag_41 == cl_ord_id;
    }

    bool OrderTest::wait_for_logon_ack(const std::chrono::steady_clock::time_point deadline, std::string& error) {
        while (std::chrono::steady_clock::now() < deadline) {
            if (!fix_.poll()) {
                error = "poll failed while waiting for logon ack";
                return false;
            }

            while (const auto msg = fix_.next_message()) {
                if (find_tag(*msg, "35=") == "A") {
                    return true;
                }
            }

            if (cfg_.poll_sleep_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(cfg_.poll_sleep_us));
            }
        }

        error = "timeout waiting for logon ack";
        return false;
    }

    bool OrderTest::wait_for_order_response(const std::string_view cl_ord_id,
                                            const std::chrono::steady_clock::time_point deadline,
                                            std::string& matched_msg,
                                            std::string& error) {
        while (std::chrono::steady_clock::now() < deadline) {
            if (!fix_.poll()) {
                error = "poll failed while waiting for order response";
                return false;
            }

            while (const auto msg = fix_.next_message()) {
                if (has_matching_cl_ord_id(*msg, cl_ord_id)) {
                    matched_msg.assign(msg->data(), msg->size());
                    return true;
                }
            }

            if (cfg_.poll_sleep_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(cfg_.poll_sleep_us));
            }
        }

        error = "timeout waiting for order response";
        return false;
    }

    OrderTestResult OrderTest::run_once() {
        OrderTestResult result{};
        result.new_rtt_ns.reserve(kBatchSize);
        result.modify_rtt_ns.reserve(kBatchSize);
        result.cancel_rtt_ns.reserve(kBatchSize);
        result.new_cl_ord_ids.reserve(kBatchSize);
        result.modify_cl_ord_ids.reserve(kBatchSize);
        result.cancel_cl_ord_ids.reserve(kBatchSize);
        result.new_response_msg_types.reserve(kBatchSize);
        result.modify_response_msg_types.reserve(kBatchSize);
        result.cancel_response_msg_types.reserve(kBatchSize);

        if (!fix_.connect_tcp(cfg_.host, cfg_.port)) {
            result.error = "failed to connect";
            return result;
        }

        const bool logon_sent = fix_.send_raw(fix_.build_logon(30));
        if (!logon_sent) {
            result.error = "failed to send logon";
            fix_.disconnect();
            return result;
        }

        std::string wait_error;
        const auto logon_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.logon_timeout_ms);
        if (!wait_for_logon_ack(logon_deadline, wait_error)) {
            result.error = wait_error;
            fix_.disconnect();
            return result;
        }

        auto benchmark_order_rtt = [&](std::string_view outbound_msg,
                                       std::string_view request_cl_ord_id,
                                       std::string_view stage_name,
                                       uint64_t& rtt_ns_out,
                                       std::string& response_msg_type_out) -> bool {
            if (outbound_msg.empty()) {
                result.error = "failed to build " + std::string(stage_name) + " message";
                return false;
            }

            const auto t0 = std::chrono::steady_clock::now();
            if (!fix_.send_raw(outbound_msg)) {
                result.error = "failed to send " + std::string(stage_name) + " message";
                return false;
            }

            std::string matched_msg;
            std::string stage_error;
            const auto response_deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.response_timeout_ms);
            if (!wait_for_order_response(request_cl_ord_id, response_deadline, matched_msg, stage_error)) {
                result.error = std::string(stage_name) + " " + stage_error;
                return false;
            }
            const auto t1 = std::chrono::steady_clock::now();

            rtt_ns_out =
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            response_msg_type_out = std::string(find_tag(matched_msg, "35="));
            return true;
        };

        auto saturating_increment = [](uint64_t value, uint64_t delta) -> uint64_t {
            const uint64_t max = std::numeric_limits<uint64_t>::max();
            if (delta > max - value) {
                return max;
            }
            return value + delta;
        };

        for (size_t i = 0; i < kBatchSize; ++i) {
            const std::string new_cl_ord_id = fix_.next_cl_ord_id();
            const std::string_view new_order_msg =
                fix_.build_new_order_limit(new_cl_ord_id, cfg_.symbol, cfg_.is_buy, cfg_.qty, cfg_.price, cfg_.tif);

            uint64_t rtt_ns = 0;
            std::string response_msg_type;
            if (!benchmark_order_rtt(new_order_msg, new_cl_ord_id, "new order", rtt_ns, response_msg_type)) {
                fix_.disconnect();
                return result;
            }

            result.new_cl_ord_ids.push_back(new_cl_ord_id);
            result.new_rtt_ns.push_back(rtt_ns);
            result.new_response_msg_types.push_back(std::move(response_msg_type));
        }

        for (size_t i = 0; i < kBatchSize; ++i) {
            const std::string modify_cl_ord_id = fix_.next_cl_ord_id();
            const uint64_t modify_qty = saturating_increment(cfg_.qty, static_cast<uint64_t>(i + 1));
            const uint64_t modify_price = saturating_increment(cfg_.price, static_cast<uint64_t>(i + 1));
            const std::string_view modify_msg = fix_.build_replace(modify_cl_ord_id,
                                                                   result.new_cl_ord_ids[i],
                                                                   cfg_.symbol,
                                                                   cfg_.is_buy,
                                                                   modify_qty,
                                                                   modify_price,
                                                                   cfg_.tif);

            uint64_t rtt_ns = 0;
            std::string response_msg_type;
            if (!benchmark_order_rtt(modify_msg, modify_cl_ord_id, "modify order", rtt_ns, response_msg_type)) {
                fix_.disconnect();
                return result;
            }

            result.modify_cl_ord_ids.push_back(modify_cl_ord_id);
            result.modify_rtt_ns.push_back(rtt_ns);
            result.modify_response_msg_types.push_back(std::move(response_msg_type));
        }

        for (size_t i = 0; i < kBatchSize; ++i) {
            const std::string cancel_cl_ord_id = fix_.next_cl_ord_id();
            const std::string_view cancel_msg =
                fix_.build_cancel(cancel_cl_ord_id, result.modify_cl_ord_ids[i], cfg_.symbol, cfg_.is_buy);

            uint64_t rtt_ns = 0;
            std::string response_msg_type;
            if (!benchmark_order_rtt(cancel_msg, cancel_cl_ord_id, "cancel order", rtt_ns, response_msg_type)) {
                fix_.disconnect();
                return result;
            }

            result.cancel_cl_ord_ids.push_back(cancel_cl_ord_id);
            result.cancel_rtt_ns.push_back(rtt_ns);
            result.cancel_response_msg_types.push_back(std::move(response_msg_type));
        }

        result.ok = true;
        fix_.disconnect();
        return result;
    }
}
