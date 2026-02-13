//
// Created by djaiswal on 1/21/26.
//
#include "Client.h"

#include <charconv>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <unistd.h>
#include <sys/socket.h>

namespace jolt::gateway {
    Client::Client(uint64_t client_id) {
        client_id_ = client_id;
    }

    Client::~Client() {
    }


    uint64_t Client::client_id() const {
        return client_id_;
    }


    void Client::set_gateway(FixGateway* gateway) {
        gateway_ = gateway;
    }

    void Client::set_session_id(uint64_t id) {
        session_id_ = id;
    }

    uint64_t Client::get_session_id() {
        return session_id_;
    }
}
