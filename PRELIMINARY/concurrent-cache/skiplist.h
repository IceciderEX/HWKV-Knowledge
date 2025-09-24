#ifndef __SKIPLIST_H__
#define __SKIPLIST_H__

#include <memory>
#include <new>
#include <vector>
#include <atomic>
#include <string>
#include <optional>

using Key = std::string;
using Value = std::string;

struct Node {
public:
    const Key key_;
    const Value value_;
    // 节点的高度：节点拥有多少层 next 指针
    const int height_;
    // 由于可能有多线程对 next 进行修改，需要将其设置为 atomic
    std::atomic<Node*> next_[1];
    
    static Node* create_node(const Key& key, const Value& value, const int height) {
        // 使用 operator new 来简化内存分配操作，不使用内存池
        // 先不与 rocksdb 相同，先将 next 数组分配到后面
        void* alloc_memory = ::operator new(sizeof(Node) + (height - 1) * sizeof(std::atomic<Node*>));
        return new (alloc_memory) Node(key, value, height);
    }
private:
    // 私有的构造函数，使其只能通过工厂函数创建
    Node(const Key& key, const Value& value, const int height): key_(key), value_(value), height_(height) {}
};

struct KeyComparator {
    int operator() (const Key& k1, const Key& k2) const {
        if (k1 < k2) return -1;
        if (k1 > k2) return 1;
        return 0;
    }
};

class Skiplist {
public:
    Skiplist(int k_max_height);
    ~Skiplist() = default;

    bool insert(const Key& key, const Value& value);
    std::optional<Value> get(const Key& key) const;
    bool contains(const Key& key);
    // 找到 key 的前驱节点，返回大小为 height 的前驱节点数组
    void find_prevs(const Key& key, std::vector<Node*>& prevs) const;
    // 
    Node* find_greater_or_equal(const Key& key) const;
    int random_height();
private:
    const int k_max_height_;
    // 
    std::atomic<int> current_max_height_;
    // dummy head，不存储任何数据，作为所有层级链表的起点
    Node* head_;

    KeyComparator comparator_;
};

#endif