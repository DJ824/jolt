//
// Created by djaiswal on 1/16/26.
//
#pragma once
#include "../include/Types.h"

namespace jolt::exchange {

class RiskEngine {
public:
    bool check(const ClientInfo& client, const ob::OrderParams& order, ob::RejectReason& reason) const;
    void on_accept(ClientInfo& client, const ob::OrderParams& order);
    void on_book_event(ClientInfo& client, const ob::BookEvent& event);
};

}


