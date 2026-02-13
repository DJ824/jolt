#pragma once

#include <cstddef>
#include <cstdlib>
#include <new>

namespace jolt::ob {

template <typename LevelT>
class LevelPool {
    struct Node {
        alignas(alignof(LevelT)) unsigned char storage[sizeof(LevelT)];
        Node* next{nullptr};
    };

    Node* free_{nullptr};

    static Node* make_node() {
        void* mem = nullptr;
        std::size_t align = alignof(LevelT) < sizeof(void*) ? sizeof(void*) : alignof(LevelT);
        if (posix_memalign(&mem, align, sizeof(Node)) != 0 || !mem) {
            throw std::bad_alloc{};
        }
        Node* n = reinterpret_cast<Node*>(mem);
        n->next = nullptr;
        return n;
    }

public:
    LevelPool() = default;

    ~LevelPool() {
        Node* n = free_;
        while (n) {
            Node* nx = n->next;
            std::free(n);
            n = nx;
        }
    }

    template <typename... Args>
    LevelT* acquire(Args&&... args) {
        Node* node = free_ ? free_ : make_node();
        if (free_) {
            free_ = free_->next;
        }
        auto* lvl = ::new (node->storage) LevelT(std::forward<Args>(args)...);
        return lvl;
    }

    void release(LevelT* lvl) {
        if (!lvl) {
            return;
        }
        lvl->~LevelT();
        Node* n = reinterpret_cast<Node*>(reinterpret_cast<unsigned char*>(lvl) - offsetof(Node, storage));
        n->next = free_;
        free_ = n;
    }

    LevelPool(const LevelPool&) = delete;
    LevelPool& operator=(const LevelPool&) = delete;
};

}

