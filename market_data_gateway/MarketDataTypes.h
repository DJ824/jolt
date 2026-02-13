#pragma once
#include <array>
#include <cstdint>
#include <string>

namespace jolt::md {
    static constexpr size_t kFixMaxMsg = 1024;

    struct FixMessage {
        std::array<char, kFixMaxMsg> data{};
        size_t len{0};
        uint64_t session_id{0};
    };

    struct SessionState {
        std::string sender_comp_id{};
        std::string target_comp_id{};
        uint64_t session_id{0};
        uint64_t seq{1};
        bool logged_on{false};

        SessionState() = default;
        explicit SessionState(uint64_t id) : session_id(id) {}
    };

    struct DataRequest {
        uint64_t session_id{0};
        uint64_t symbol_id{0};
        uint64_t request_id{0};
    };

    struct SnapshotRequest {
        uint64_t session_id{0};
        uint64_t symbol_id{0};
        uint64_t request_id{0};
    };


    struct SnapshotMeta {
        uint64_t request_id;
        uint64_t session_id;
        uint64_t snapshot_seq;
        uint32_t bytes;
        uint32_t bid_ct;
        uint32_t ask_ct;
        uint16_t symbol_id;
        uint16_t slot_id;
        bool accepted;
    };

    struct RetransmissionRequest {
        uint64_t session_id{0};
        uint64_t symbol_id{0};
        uint64_t request_id{0};
        uint64_t start_seq{0};
        uint64_t end_seq{0};
    };

    struct Response {
        uint64_t request_id{0};
        uint16_t symbol_id{0};
        uint64_t snapshot_seq{0};
        size_t slot_idx{0};
        bool accepted{false};
    };

    struct ChannelInfo {
        std::string group{};
        uint16_t port{0};
    };
}
