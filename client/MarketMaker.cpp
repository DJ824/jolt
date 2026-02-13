//
// Created by djaiswal on 2/11/26.
//

#include "MarketMaker.h"
#include "FixClient.h"
namespace jolt::client {
    class MarketMaker {
        struct ActiveOrder {
            std::string c_id;

        };

        FixClient client_;

    };
}