#include "skiplist.h"
#include <atomic>
#include <cstdint>
#include <optional>
#include <vector>
#include <mutex>
#include <iostream>

std::mutex g_log_mutex;
template<typename... Args>
void log_print(Args... args) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    (std::cout << ... << args) << std::endl;
}

Skiplist::Skiplist(int k_max_height): 
    k_max_height_(k_max_height), head_(Node::create_node("", "", k_max_height)), current_max_height_(1) {
    for (int i = 0; i < k_max_height_; ++i) {
        head_->next_[i].store(nullptr, std::memory_order_relaxed);
    }
}

void Skiplist::find_prevs(const Key& key, std::vector<Node*>& prevs) const {
    prevs.resize(k_max_height_);
    Node* current = head_;
    
    // 从最高层开始向下搜索
    for (int i = k_max_height_ - 1; i >= 0; --i) {
        Node* next = current->next_[i].load(std::memory_order_acquire);
        while (next != nullptr && comparator_(next->key_, key) < 0) {
            current = next;
            next = current->next_[i].load(std::memory_order_acquire);
        }
        prevs[i] = current;
    }
}

int Skiplist::random_height() {
    static const std::uint32_t k_branching = 4;
    int height = 1;
    while (height < k_max_height_ && (rand() % k_branching) == 0) {
        height++;
    }
    return height;
}

bool Skiplist::insert(const Key& key, const Value& value) {
    // int cur_level = current_max_height_.load(std::memory_order_acquire);
    // 1. 先查找当前的 key 是否存在（第0层）
    // 2. 为新的节点生成一个随机的高度
    int insert_height = random_height();
    int current_height = current_max_height_.load(std::memory_order_acquire);
    while (insert_height > current_height) {
        if (current_max_height_.compare_exchange_weak(current_height, insert_height, std::memory_order_release)) {
            break;
        }
        // 如果失败，current_height 会被更新为 current_max_height_ 的值，然后重试
    }

    Node* new_node = Node::create_node(key, value, insert_height);
    while (true) {
        std::vector<Node*> prevs(k_max_height_);
        find_prevs(key, prevs);

        Node* level0_prev = prevs[0];
        Node* level0_pnext = level0_prev->next_[0].load(std::memory_order_acquire);
        // 如果存在，插入失败
        if (level0_pnext != nullptr && comparator_(level0_pnext->key_, key) == 0) {
            delete new_node;
            return false;
        }
        // 按照当前版本的 prevs 去设置 next，防止不一致
        for (int i = 0; i < insert_height; ++i) {
            new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
        }
        for (int i = 0; i < insert_height; ++i) {
            while (true) {
                // find_prevs(key, prevs);
                Node* expected_next = new_node->next_[i].load(std::memory_order_relaxed);
                // 可能有其他线程操作，使用 CAS
                if (prevs[i]->next_[i].compare_exchange_strong(expected_next, new_node, std::memory_order_release)) {
                    break;
                } 
                // log_print("[TID:", std::this_thread::get_id(), "] insert(", key, "): L", i, 
                // " trying CAS. prev=", level_prev->key_, 
                // ", next=", (level_pnext ? level_pnext->key_ : "null"));
                // 说明有其他线程修改了 prev 的 next，需要重新查找 prev
                find_prevs(key, prevs);
                // 更新新节点的 next 指针，因为 prevs[i]->next_[i] 可能已经被修改了
                new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
            }
        }
        return true;
    }
    return false;
}

bool Skiplist::contains(const Key& key) {
    Node* target = find_greater_or_equal(key);
    return (target != nullptr && comparator_(target->key_, key) == 0);
}

Node* Skiplist::find_greater_or_equal(const Key& key) const {
    Node* current = head_;
    int current_height = current_max_height_.load(std::memory_order_acquire);

    // 从当前最高层开始搜索
    for (int i = current_height - 1; i >= 0; --i) {
        Node* next = current->next_[i].load(std::memory_order_acquire);
        while (next != nullptr && comparator_(next->key_, key) < 0) {
            current = next;
            next = current->next_[i].load(std::memory_order_acquire);
        }
    }

    return current->next_[0].load(std::memory_order_acquire);
}

std::optional<Value> Skiplist::get(const Key& key) const {
    Node* target = find_greater_or_equal(key);
    if (target != nullptr && comparator_(target->key_, key) == 0) {
        return target->value_;
    }
    return std::nullopt;
}