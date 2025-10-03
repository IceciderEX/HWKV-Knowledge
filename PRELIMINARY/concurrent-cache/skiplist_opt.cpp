#include <atomic>
#include <cassert>
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
    seq_splice_ = allocate_splice();
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

// bool Skiplist::insert(const char* key) {
//     // int cur_level = current_max_height_.load(std::memory_order_acquire);
//     // 1. 先查找当前的 key 是否存在（第0层）
//     // 2. 为新的节点生成一个随机的高度
//     Node* new_node = (reinterpret_cast<Node*>(const_cast<char*>(key))) - 1;

//     int insert_height = new_node->unstash_height();
//     int current_height = current_max_height_.load(std::memory_order_acquire);
//     while (insert_height > current_height) {
//         if (current_max_height_.compare_exchange_weak(current_height, insert_height, std::memory_order_release)) {
//             break;
//         }
//         // 如果失败，current_height 会被更新为 current_max_height_ 的值，然后重试
//     }

//     while (true) {
//         std::vector<Node*> prevs(k_max_height_);
//         find_prevs(key, prevs);

//         Node* level0_prev = prevs[0];
//         Node* level0_pnext = level0_prev->next(0);
//         // 如果存在，插入失败
//         if (level0_pnext != nullptr && comparator_(level0_pnext->key(), key) == 0) {
//             return false;
//         }
//         // 按照当前版本的 prevs 去设置 next，防止不一致
//         for (int i = 0; i < insert_height; ++i) {
//             new_node->no_barrier_set_next(i, prevs[i]->next(i));
//             // new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
//         }
//         for (int i = 0; i < insert_height; ++i) {
//             while (true) {
//                 // find_prevs(key, prevs);
//                 Node* expected_next = new_node->no_barrier_next(i);
//                 // 可能有其他线程操作，使用 CAS
//                 if (prevs[i]->cas_set_next(i, expected_next, new_node)) {
//                     break;
//                 } 
//                 // log_print("[TID:", std::this_thread::get_id(), "] insert(", key, "): L", i, 
//                 // " trying CAS. prev=", level_prev->key_, 
//                 // ", next=", (level_pnext ? level_pnext->key_ : "null"));
//                 // 说明有其他线程修改了 prev 的 next，需要重新查找 prev
//                 find_prevs(key, prevs);
//                 // 更新新节点的 next 指针，因为 prevs[i]->next_[i] 可能已经被修改了
//                 new_node->no_barrier_set_next(i, prevs[i]->next(i));
//                 //new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
//             }
//         }
//         return true;
//     }
//     return false;
// }

template<bool use_cas>
bool Skiplist::insert(const char* key, Splice* splice) {
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

    // 加入对 hint 的支持
    // 首先检查 hint 是否有效
    int recompute_level = 0;
    if (splice->height_ < current_height) {
        // splice 的值已经过旧，重新计算
        splice->prev_[current_height] = head_;
        splice->next_[current_height] = nullptr;
        splice->height_ = current_height;
        recompute_level = current_height;
    } else {
        // 验证 hint 是否有效
        for (int i = insert_height - 1; i >= 0; --i) {
            if (splice->prev_[i] == nullptr || splice->prev_[i] == head_ || !key_is_after_node(key, splice->prev_[i])) {
                // prev[i] 为 head 或者仍然有效
                if (splice->next_[i] == nullptr || comparator_(splice->next_[i]->key(), key) >= 0) {
                    continue;
                }
            }
            // 在第 i 层失效
            recompute_level = i + 1;
            break;
        }
    }

    if (recompute_level > 0) {
        recompute_slices_levels(key, splice, recompute_level);
    }

    if (splice->next_[0] != nullptr && comparator_(splice->next_[0]->key(), key) == 0) {
        return false;
    }

    while (true) {
        // std::vector<Node*> prevs(k_max_height_);
        // find_prevs(key, prevs);

        // Node* level0_prev = prevs[0];
        // Node* level0_pnext = level0_prev->next(0);
        // // 如果存在，插入失败
        // if (level0_pnext != nullptr && comparator_(level0_pnext->key(), key) == 0) {
        //     return false;
        // }
        // 按照当前版本的 prevs 去设置 next，防止不一致
        // for (int i = 0; i < insert_height; ++i) {
        //     new_node->no_barrier_set_next(i, prevs[i]->next(i));
        //     // new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
        // }

        if (use_cas) {
            for (int i = 0; i < insert_height; ++i) {
                new_node->no_barrier_set_next(i, splice->next_[i]);
                while (true) {
                    // find_prevs(key, prevs);
                    Node* expected_next = splice->next_[i];
                    // 可能有其他线程操作，使用 CAS
                    // 验证 prev[i] 的 next 是否还是 next_[i]
                    if (!splice->prev_[i]->cas_set_next(i, expected_next, new_node)) {
                        break;
                    } 
                    // 说明 splice->prev_[i] 的 next 已经过时了，需要重新查找 prev 
                    find_splice_for_level(key, splice->prev_[i], i, &splice->prev_[i], &splice->next_[i]);
                    // 更新新节点的 next 指针，因为 prevs[i]->next_[i] 可能已经被修改了
                    new_node->no_barrier_set_next(i, splice->next_[i]);
                    //new_node->next_[i].store(prevs[i]->next_[i].load(std::memory_order_acquire), std::memory_order_relaxed);
                }
            }
        } else {
            for (int i = 0; i < insert_height; ++i) {
                splice->prev_[i]->no_barrier_set_next(i, new_node);
            }
        }
    }
    // 更新 hint 的 prev 值为 new_node
    for (int i = 0;i < insert_height; ++i) {
        splice->prev_[i] = new_node;
    }

    return true;
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

Skiplist::Splice* Skiplist::allocate_splice() {
    // node 数组的大小
    size_t node_array_size = sizeof(Node*) * k_max_height_;
    char* raw_memory = new char[node_array_size * 2 + sizeof(Splice)];
    Splice* splice = reinterpret_cast<Splice*>(raw_memory);
    splice->height_ = 0;
    // 将 splice 结构体后面的内存分配给 prev
    splice->prev_ = reinterpret_cast<Node**>(raw_memory + sizeof(Splice));
    // 接着分配给 next
    splice->next_ = reinterpret_cast<Node**>(raw_memory + sizeof(Splice) + node_array_size);
    return splice;
}

void Skiplist::find_splice_for_level(const char* key, Node* before, int level, Node** out_prev, Node** out_next) {
    while (true) {
        Node* next = before->next(level);
        // 找到第一个大于等于 key 的 node，设置好正确的 prev 与 next 
        if (next == nullptr && comparator_(next->key(), key) >= 0) {
            *out_prev = before;
            *out_next = next;
            return;
        }
        before = next;
    }
}

void Skiplist::recompute_slices_levels(const char* key, Splice* splice, int recompute_level) {
    assert(recompute_level > 0);
    // 使用上一层有效的 prev 进行查找
    for (int i = recompute_level - 1; i >= 0; --i) {
        // 从 i + 1 层有效的 splice 开始查找
        find_splice_for_level(key, splice->prev_[i + 1], i, &splice->prev_[i], &splice->next_[i]);
    }
}

bool Skiplist::insert(const char* key) {
    return insert<false>(key,  seq_splice_);
}

bool Skiplist::hint_insert(const char* key, void** hint) {
    assert(hint != nullptr);
    Splice* splice = reinterpret_cast<Splice*>(*hint);
    if (splice == nullptr) {
        splice = allocate_splice();
        *hint = splice;
    }
    return insert<true>(key, splice);
}

