#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string_view>

namespace jolt::gateway {
    class FixGateway;
    class FixSession;

    class Client {

        uint64_t client_id_{0};
        FixGateway* gateway_{nullptr};
        uint64_t session_id_{0};

    public:
        Client(uint64_t client_id);
        ~Client();

        Client(const Client&) = delete;
        Client& operator=(const Client&) = delete;
        Client(Client&&) = delete;
        Client& operator=(Client&&) = delete;
        uint64_t client_id() const;
        void set_session_id(uint64_t id);
        uint64_t get_session_id();
        void set_gateway(FixGateway* gateway);
    };
}
