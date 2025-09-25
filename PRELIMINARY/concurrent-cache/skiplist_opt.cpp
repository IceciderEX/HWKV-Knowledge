#include <atomic>
#include <cstddef>
#include <optional>
#include <vector>
#include <mutex>
#include <iostream>
#include <random>
#include "skiplist_opt.h"

std::mutex g_log_mutex;
template<typename... Args>
void log_print(Args... args) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    (std::cout << ... << args) << std::endl;
}

Skiplist::Skiplist(int k_max_height): 
    k_max_height_(k_max_height), head_(allocate_node(0, k_max_height)) {
    for (int i = 0; i < k_max_height_; ++i) {
        head_->set_next(i, nullptr);
    }
}

Node* Skiplist::allocate_node(size_t key_size, int height) {
    const size_t nexts_size = sizeof(std::atomic<Node*>) * (height - 1);
    const size_t total_size = nexts_size + sizeof(Node) + key_size;

    char* raw_memory = new char[total_size];
    Node* node_ptr = reinterpret_cast<Node*>(raw_memory + nexts_size);
    node_ptr->stash_height(height);
    return node_ptr;
}

char* Skiplist::allocate_key(size_t key_size) {
    int height = random_height();
    Node* node = allocate_node(key_size, height);
    return const_cast<char*>(node->key());
}

bool Skiplist::key_is_after_node(const char* key, Node* node) const {
    return (node != head_) && (comparator_(node->key(), key) < 0);
}

void Skiplist::find_prevs(const char* key, std::vector<Node*>& prevs) const {
    prevs.assign(k_max_height_, nullptr);
    Node* current = head_;
    
    int level = current_max_height_.load(std::memory_order_acquire) - 1;
    // 从最高层开始向下搜索
    for (int i = level; i >= 0; --i) {
        Node* next = current->next(i);
        while (next != nullptr && key_is_after_node(key, next)) {
            current = next;
            next = current->next(i);
        }
        prevs[i] = current;
    }
}

// int Skiplist::random_height() {
//     static const std::uint32_t k_branching = 4;
//     int height = 1;
//     while (height < k_max_height_ && (rand() % k_branching) == 0) {
//         height++;
//     }
//     return height;
// }

int Skiplist::random_height() {
    // 使用 thread_local 保证线程安全性
    static thread_local std::mt19937 generator(std::random_device{}());
    static thread_local std::uniform_int_distribution<int> distribution(0, 3); // 1/4的概率
    
    int h = 1;
    while (h < k_max_height_ && distribution(generator) == 0) {
        h++;
    }
    return h;
}

bool Skiplist::insert(const char* key) {
    // int cur_level = current_max_height_.load(std::memory_order_acquire);
    // 1. 先查找当前的 key 是否存在（第0层）
    // 2. 为新的节点生成一个随机的高度
    Node* new_node = (reinterpret_cast<Node*>(const_cast<char*>(key))) - 1;

    int insert_height = new_node->unstash_height();
    int current_height = current_max_height_.load(std::memory_order_acquire);
    while (insert_height > current_height) {
        if (current_max_height_.compare_exchange_weak(current_height, insert_height, std::memory_order_release)) {
            break;
        }
        // 如果失败，current_height 会被更新为 current_max_height_ 的值，然后重试
    }

    while (true) {
        std::vector<Node*> prevs(k_max_height_);
        find_prevs(key, prevs);

        Node* level0_prev = prevs[0];
        Node* level0_pnext = level0_prev->next(0);
        // 如果存在，插入失败
        if (level0_pnext != nullptr && comparator_(level0_pnext->key(), key) == 0) {
            return false;
        }
        // 按照当前版本的 prevs 去设置 next，防止不一致
        for (int i = 0; i < insert_height; ++i) {
            new_node->no_barrier_set_next(i, prevs[i]->next(i));
            // new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
        }
        for (int i = 0; i < insert_height; ++i) {
            while (true) {
                // find_prevs(key, prevs);
                Node* expected_next = new_node->no_barrier_next(i);
                // 可能有其他线程操作，使用 CAS
                if (prevs[i]->cas_set_next(i, expected_next, new_node)) {
                    break;
                } 
                // log_print("[TID:", std::this_thread::get_id(), "] insert(", key, "): L", i, 
                // " trying CAS. prev=", level_prev->key_, 
                // ", next=", (level_pnext ? level_pnext->key_ : "null"));
                // 说明有其他线程修改了 prev 的 next，需要重新查找 prev
                find_prevs(key, prevs);
                // 更新新节点的 next 指针，因为 prevs[i]->next_[i] 可能已经被修改了
                new_node->no_barrier_set_next(i, prevs[i]->next(i));
                //new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
            }
        }
        return true;
    }
    return false;
}

bool Skiplist::contains(const char* key) {
    Node* target = find_greater_or_equal(key);
    return (target != nullptr && comparator_(target->key(), key) == 0);
}

Node* Skiplist::find_greater_or_equal(const char* key) const {
    Node* current = head_;
    int current_height = current_max_height_.load(std::memory_order_acquire);

    // 从当前最高层开始搜索
    for (int i = current_height - 1; i >= 0; --i) {
        Node* next = current->next(i);
        while (next != nullptr && key_is_after_node(key, next)) {
            current = next;
            next = current->next(i);
        }
    }

    return current->next(0);
}

std::optional<Value> Skiplist::get(const char* key) const {
    Node* target = find_greater_or_equal(key);
    if (target != nullptr && comparator_(target->key(), key) == 0) {
        return std::string(key);
    }
    return std::nullopt;
}