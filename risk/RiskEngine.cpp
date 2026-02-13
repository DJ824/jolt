//
// Created by djaiswal on 1/16/26.
//

#include "RiskEngine.h"

namespace jolt::exchange {

bool RiskEngine::check(const ClientInfo& client, const ob::OrderParams& order, ob::RejectReason& reason) const {
    if (order.qty == 0) {
        reason = ob::RejectReason::InvalidQty;
        return false;
    }
    if (client.max_qty > 0 && order.qty > client.max_qty) {
        reason = ob::RejectReason::InvalidQty;
        return false;
    }
    reason = ob::RejectReason::NotApplicable;
    return true;
}

void RiskEngine::on_accept(ClientInfo& client, const ob::OrderParams& order) {
    (void)order;
    if (client.open_orders < client.max_open_orders) {
        ++client.open_orders;
    }
}

void RiskEngine::on_book_event(ClientInfo& client, const ob::BookEvent& event) {
    (void)event;
    if (client.open_orders > 0) {
        --client.open_orders;
    }
}

} // namespace jolt::exchange
