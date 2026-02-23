#pragma once

#include "FixClient.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace jolt::client {
    struct OrderTestConfig {
        std::string host{"3.133.154.91"};
        std::string port{"8080"};
        std::string sender_comp_id{"ORDER_TEST"};
        std::string target_comp_id{"ENTRY_GATEWAY"};
        std::string account{"ORDER_TEST"};
        std::string symbol{"1"};
        bool is_buy{true};
        uint64_t qty{1};
        uint64_t price{59000};
        int tif{1};
        uint64_t logon_timeout_ms{2'000};
        uint64_t response_timeout_ms{5'000};
        uint64_t poll_sleep_us{100};
    };

    struct OrderTestResult {
        bool ok{false};
        std::vector<uint64_t> new_rtt_ns{};
        std::vector<uint64_t> modify_rtt_ns{};
        std::vector<uint64_t> cancel_rtt_ns{};
        std::vector<std::string> new_cl_ord_ids{};
        std::vector<std::string> modify_cl_ord_ids{};
        std::vector<std::string> cancel_cl_ord_ids{};
        std::vector<std::string> new_response_msg_types{};
        std::vector<std::string> modify_response_msg_types{};
        std::vector<std::string> cancel_response_msg_types{};
        std::string error{};
    };

    class OrderTest {
    public:
        explicit OrderTest(OrderTestConfig cfg);
        OrderTestResult run_once();

    private:
        static std::string_view find_tag(std::string_view msg, std::string_view tag_with_eq);
        static bool has_matching_cl_ord_id(std::string_view msg, std::string_view cl_ord_id);
        bool wait_for_logon_ack(std::chrono::steady_clock::time_point deadline, std::string& error);
        bool wait_for_order_response(std::string_view cl_ord_id,
                                     std::chrono::steady_clock::time_point deadline,
                                     std::string& matched_msg,
                                     std::string& error);

        OrderTestConfig cfg_;
        FixClient fix_{};
    };
}
