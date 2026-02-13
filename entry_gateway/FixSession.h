//
// Created by djaiswal on 1/23/26.
//

#ifndef JOLT_FIXSESSION_H
#define JOLT_FIXSESSION_H
#include <array>
#include <cstdint>
#include <charconv>
#include <cstring>
#include <cerrno>
#include <deque>
#include <string>
#include <unordered_map>
#include <string_view>
#include <stdexcept>
#include <thread>
#include <vector>

#include "GatewayTypes.h"


namespace jolt::gateway {
    static constexpr size_t kRxCap = 8192;
    static constexpr size_t kTxCap = 1024;

    class Client;
    class FixGateway;

    class FixSession {
    public:

        struct Message {
            std::array<char, 1024> buf;
            size_t len{0};
        };

        std::string sender_comp_id_{0};
        std::string target_comp_id_{0};
        std::unordered_map<uint64_t, Client*> clients_;
        uint64_t seq_{0};
        int fd_{0};
        bool exchanged_logon_{false};

        void add_client(uint64_t client_id, Client* client);
        void remove_client(uint64_t client_id);

        FixSession(const std::string& sender_comp_id, const std::string& target_comp_id, int fd);
        ~FixSession();

        void on_writable();
        void on_readable();
        bool send_pending();
        bool want_write();
        void queue_message(std::string_view message);
        void recv_pending();

        void send_to_gateway(FixMessage msg);

        bool extract_message(std::string_view& msg);
        bool handle_message(std::string_view& msg);
        std::array<char, kRxCap> rx_buf_;
        std::deque<Message> tx_buf_;
        size_t rx_len_{0};
        size_t rx_off_{0};
        size_t tx_off_{0};

        FixGateway* gateway_{nullptr};

        bool closed_{false};
        void close();

        uint64_t session_id_{0};
        std::vector<uint64_t> client_ids_{};




    };

}

#endif //JOLT_FIXSESSION_H