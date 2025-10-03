#ifndef __SKIPLIST_OPT_H__
#define __SKIPLIST_OPT_H__

#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <vector>
#include <atomic>
#include <string>
#include <optional>
#include <assert.h>

// 暂时假定使用字符串为 key 类型
using Key = std::string;
using Value = std::string;

struct Node {
public:
    // 临时存储/读取节点高度，利用了在节点插入前next_[0]字段未被使用
    void stash_height(int height) {
        static_assert(sizeof(int) <= sizeof(next_[0]), "Height size exceeds pointer size");
        memcpy(static_cast<void*>(&next_[0]), &height, sizeof(int));
    }

    // 从 next[0] 处读取到 height，用于 insert
    int unstash_height() {
        int height;
        memcpy(&height, &next_[0], sizeof(int));
        return height;
    }

    const char* key() const {
        // key 的数据紧跟在 next 数组之后
        return reinterpret_cast<const char*>(&next_[1]);
    }   

    Node* next(int n) {
        assert(n >= 0);
        // acquire 确保完整初始化
        return (reinterpret_cast<std::atomic<Node*>*>(&next_[0]) - n)->load(std::memory_order_acquire);
    }

    void set_next(int n, Node* node) {
        assert(n >= 0);
        (reinterpret_cast<std::atomic<Node*>*>(&next_[0]) - n)->store(node, std::memory_order_release);
    }

    bool cas_set_next(int n, Node* expected, Node* node) {
        assert(n >= 0);
        return (reinterpret_cast<std::atomic<Node*>*>(&next_[0]) - n)->
            compare_exchange_strong(expected, node, std::memory_order_release);
    }

    Node* no_barrier_next(int n) {
        assert(n >= 0);
        return (reinterpret_cast<std::atomic<Node*>*>(&next_[0]) - n)->load(std::memory_order_relaxed); 
    }

    void no_barrier_set_next(int n, Node* node) {
        assert(n >= 0);
        (reinterpret_cast<std::atomic<Node*>*>(&next_[0]) - n)->store(node, std::memory_order_relaxed);
    }
private:
    // 只为 node 分配一个指针的空间
    // 为了保持内存连续性，不直接存储 key 与 height
    std::atomic<Node*> next_[1];
    // 只由 skiplist 控制创建
    Node() = default;
};

struct KeyComparator {
    int operator() (const char* k1, const char* k2) const {
        return strcmp(k1, k2);
    }
};

class Skiplist {
public:
    Skiplist(int k_max_height = 16);
    ~Skiplist() = default;

    // 分配一个大小足以存储Key的Node，返回指向Key存储区的指针。
    // 调用者需要手动将key内容复制到返回的指针位置。
    char* allocate_key(size_t key_size);

    // 将一个已经由AllocateKey分配并填充好数据的Key插入到跳表中。
    // 要求：key不能已存在。
    bool insert(const char* key);
    // + splice 版本的 insert
    bool hint_insert(const char* key, void** hint);
    bool contains(const char* key);
    std::optional<std::string> get(const char* key) const;
    // 找到 key 的前驱节点，返回大小为 height 的前驱节点数组
    void find_prevs(const char* key, std::vector<Node*>& prevs) const;
    // 
    Node* find_greater_or_equal(const char* key) const;
private:
    // Splice 缓存了上一次操作时，跳表每一层的前驱节点 (prev_) 和后继节点 (next_)
    struct Splice {
        int height_; // 缓存节点信息的最大高度
        Node** prev_; // 第 i 层的前驱节点
        Node** next_; // 第 i 层的后继节点
    };
    
    template<bool use_cas>
    bool insert(const char* key, Splice* splice);
    // 创建一个 node，在 key 的创建中完成
    Node* allocate_node(size_t key_size, int height);
    int random_height();
    // 判断 key 是否大于等于 node 
    bool key_is_after_node(const char* key, Node* node) const;
    Splice* allocate_splice();
    // 查找某一层的 splice。从一个已知的 before 节点开始向后查找
    void find_splice_for_level(const char* key, Node* before, int level, Node** out_prev, Node** out_next);
    // 从 recompute_level 出发，逐层向下调用 find_splice_for_level，进行修正
    void recompute_slices_levels(const char* key, Splice* splice, int recompute_level);

    const int k_max_height_;
    // 
    std::atomic<int> current_max_height_;
    // dummy head，不存储任何数据，作为所有层级链表的起点
    Node* head_;
    KeyComparator comparator_;
    Splice* seq_splice_;
};

#endif