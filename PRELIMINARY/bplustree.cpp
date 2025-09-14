#include <cstddef>
#include <vector>
#include <memory>
#include <iostream>
#include <algorithm>
#include <queue>

// B+树的阶 (Order): 用 m 来表示。一个 m 阶的 B+ 树，每个节点最多有 m 个子节点指针和 m-1 个键。
// 为了保证树的平衡和空间利用率，我们要求非根节点至少有 ⌈m/2⌉ 个子节点（即 ⌈(m-1)/2⌉ 个键）。

// 节点类型: B+ 树有两种节点：内部节点 (Internal Node) 和 叶子节点 (Leaf Node)。
// 内部节点: 只存储键（作为索引）和指向子节点的指针。
// 叶子节点: 存储键和与键对应的值（数据）。并且，所有叶子节点形成一个双向链表，便于范围查询。

const int TREE_ORDER = 4; // 阶数

// 
template <typename KeyType, typename ValueType>
class Node {
public:
    Node() {};
    virtual ~Node() = default;

    int key_count = 0;
    std::vector<KeyType> keys;
    Node* parent = nullptr;
    bool is_leaf = false;
};


template <typename KeyType, typename ValueType>
class InternalNode: public Node<KeyType, ValueType> {
public:
    InternalNode() {
        // 内部节点的键数量最多为 ORDER - 1
        this->keys.resize(TREE_ORDER);
        // 子节点指针数量最多为 ORDER
        this->children.resize(TREE_ORDER + 1);
    }

    // 内部节点特有：指向子节点的指针数组
    // unique_ptr 保证当一个 InternalNode 被销毁时，它会自动销毁其拥有的所有子节点
    std::vector<std::unique_ptr<Node<KeyType, ValueType>>> children;
}; 

template <typename KeyType, typename ValueType>
class LeafNode: public Node<KeyType, ValueType> {
public:
    LeafNode() {
        this->is_leaf = true;
        this->keys.resize(TREE_ORDER);
        this->values.resize(TREE_ORDER);
    }

    std::vector<ValueType> values;
    // 一个叶子节点并不“拥有”它的兄弟节点。它们之间只是一种引用关系。所以使用裸指针
    LeafNode* prev;
    LeafNode* next;
};

template <typename KeyType, typename ValueType>
struct SplitResult {
    KeyType promoted_key{}; // 推上来的键
    std::unique_ptr<Node<KeyType, ValueType>> new_node;
    bool need_split; // 是否需要分裂

    SplitResult(): promoted_key{}, need_split(false), new_node(nullptr) {}

    SplitResult(const KeyType& key,  std::unique_ptr<Node<KeyType, ValueType>> node):
        promoted_key(key), new_node(std::move(node)), need_split(true) {}
};

template<typename KeyType, typename ValueType>
class BPlusTree {
public:
    BPlusTree() {};
    ~BPlusTree() = default;

    void print_tree() {
        if (!root) {
            std::cout << "Tree is empty." << std::endl;
            return;
        }

        std::queue<Node<KeyType, ValueType>*> q;
        q.push(root.get());
        int level = 0;

        while (!q.empty()) {
            int level_size = q.size();
            std::cout << "Level " << level << ": " << std::endl;
            for(size_t i = 0;i < level_size;++i) {
                auto node = q.front();
                q.pop();

                std::cout << " Node " << node << " | ";
                if(node->is_leaf) {
                    LeafNode<KeyType, ValueType>* leaf_node = static_cast<LeafNode<KeyType, ValueType>*>(node);
                    std::cout << "Leaf | Keys: [";
                    for(int j = 0;j < leaf_node->key_count;++j) {
                        std::cout << leaf_node->keys[j] << (j == leaf_node->key_count - 1 ? "" : ", ");
                    }
                    std::cout << "] | Prev: " << leaf_node->prev << " | Next: " << leaf_node->next << std::endl;
                } else {
                    InternalNode<KeyType, ValueType>* internal = static_cast<InternalNode<KeyType, ValueType>*>(node);
                    std::cout << "Internal | Keys: [";
                    for (int k = 0; k < internal->key_count; ++k) {
                        std::cout << internal->keys[k] << (k == internal->key_count - 1 ? "" : ", ");
                    }
                    std::cout << "]" << std::endl;

                    // 将子节点入队
                    for (int c = 0; c <= internal->key_count; ++c) {
                        if (internal->children[c]) {
                            q.push(internal->children[c].get());
                        }
                    }
                }
            }
            std::cout << "-----------------------------------------------------" << std::endl;
            ++level;
        }
    }

    bool search(const KeyType& key, ValueType& out_value) {
        if(!root) {
            return false;
        }

        Node<KeyType, ValueType>* current_node = root.get();
        // 循环找到叶子结点
        while(!current_node->is_leaf) {
            InternalNode<KeyType, ValueType>* node = static_cast<InternalNode<KeyType, ValueType>*>(current_node);
            // upper_bound 找到第一个 大于 key 的位置，这个位置的索引正好对应我们要去的子节点 children 的索引。
            auto it = std::upper_bound(node->keys.begin(), node->keys.begin() + node->key_count, key);
            int child_index = std::distance(node->keys.begin(), it);
            current_node = node->children[child_index].get();
        }

        LeafNode<KeyType, ValueType>* leaf_node = static_cast<LeafNode<KeyType, ValueType>*>(current_node);
        // std::lower_bound 找到第一个 不小于 key 的位置。
        auto it = std::lower_bound(leaf_node->keys.begin(), leaf_node->keys.begin() + leaf_node->key_count, key);
        int data_index = std::distance(leaf_node->keys.begin(), it);

        if (data_index < leaf_node->key_count && leaf_node->keys[data_index] == key) {
            out_value = leaf_node->values[data_index];
            return true;
        }
        
        return false;
    }

    /**
     * @brief 将一个键和一个指向新节点的指针插入到父节点中。
     * 这个函数的目的是在 B+ 树中插入一个新节点后，将这个新节点的键和指针插入到父节点中。
     * 这个过程可能会导致父节点的分裂，因为父节点的键数量可能会超过上限。
     * 负责分裂的函数返回一个“分裂结果”，比如一个包含“推上来的键”和“新节点的指针”的结构体。如果不需要分裂，就返回一个空的结果
     * 它的核心职责应该是：给定一个父节点，向其中插入一个键和子节点指针。
     * 它不应该负责：当父节点不存在时，自己创建一个新的树根 (root)。
    **/
    SplitResult<KeyType, ValueType> insert_into_parent(Node<KeyType, ValueType>* old_node, const KeyType& key, std::unique_ptr<Node<KeyType, ValueType>> new_node) {
        // 情况1：旧节点是根节点，没有父节点
        if (old_node->parent == nullptr) {
            // 直接返回分裂结果，让 insert 函数来创建新根
            return SplitResult<KeyType, ValueType>(key, std::move(new_node));
        }
        
        // 这个 Promoted Key 和指向新节点的指针插入到父节点中
        // 情况2：有父节点，正常插入
        InternalNode<KeyType, ValueType>* parent_node = static_cast<InternalNode<KeyType, ValueType>*>(old_node->parent);

        auto it = std::lower_bound(parent_node->keys.begin(), parent_node->keys.begin() + parent_node->key_count, key);
        int insert_pos = std::distance(parent_node->keys.begin(), it);
        
        // 手动移位
        for (int i = parent_node->key_count; i > insert_pos; --i) {
            parent_node->keys[i] = parent_node->keys[i - 1];
        }
        for (int i = parent_node->key_count + 1; i > insert_pos + 1; --i) {
            parent_node->children[i] = std::move(parent_node->children[i - 1]);
        }

        parent_node->keys[insert_pos] = key;
        parent_node->children[insert_pos + 1] = std::move(new_node);
        parent_node->children[insert_pos + 1]->parent = parent_node; // 别忘了设置parent
        parent_node->key_count++;

        // 如果父节点未满，返回一个空的分裂结果
        if (parent_node->key_count <= TREE_ORDER - 1) {
            return SplitResult<KeyType, ValueType>();
        }

        // 2. 如果父节点的键数量超过了上限，需要分裂
        // a. 分裂出一个新的内部节点，将后一半的键和指针移动到新节点。
        auto new_internal_node = std::make_unique<InternalNode<KeyType, ValueType>>();
        int mid = parent_node->key_count / 2;  
        KeyType promoted_key = parent_node->keys[mid];

        // 从 mid + 1 开始复制，中间的键应该被移动到父节点，而不应该留在任何一个子节点中
        std::copy(parent_node->keys.begin() + mid + 1, parent_node->keys.end(), new_internal_node->keys.begin());
        // 注意这里的 children 是 unique_ptr，所以需要使用 std::move 来转移所有权。
        // 复制后半部分的 keys
        std::copy(parent_node->keys.begin() + mid + 1, parent_node->keys.begin() + parent_node->key_count, new_internal_node->keys.begin());
        // 复制后半部分的 children
        for (int i = mid + 1; i <= parent_node->key_count; ++i) {
            new_internal_node->children[i - (mid + 1)] = std::move(parent_node->children[i]);
            if (new_internal_node->children[i - (mid + 1)]) {
                new_internal_node->children[i - (mid + 1)]->parent = new_internal_node.get();
            }
        }
        
        new_internal_node->key_count = parent_node->key_count - mid - 1;
        parent_node->key_count = mid;
        new_internal_node->parent = parent_node->parent; // 新节点的父节点是原来父节点的父节点

        // 递归调用，将分裂后的结果继续向上传递
        return insert_into_parent(parent_node, promoted_key, std::move(new_internal_node)); 
    }

    void insert(const KeyType& key, const ValueType& value) {
        // 检查树是否为空，这是初始化树的唯一入口
        if (!root) {
            // 1. 创建一个新的叶子节点
            auto new_leaf = std::make_unique<LeafNode<KeyType, ValueType>>();
            
            // 2. 将第一个键值对放入新节点
            new_leaf->keys[0] = key;
            new_leaf->values[0] = value;
            new_leaf->key_count = 1;

            // 3. 它的兄弟指针都为空
            new_leaf->prev = nullptr;
            new_leaf->next = nullptr;
            
            // 4. 将这个新节点设为树的根
            root = std::move(new_leaf);
            
            // 5. 插入完成，直接返回
            return;
        }

        Node<KeyType, ValueType>* current_node = root.get();
        // 循环找到叶子结点
        while(!current_node->is_leaf) {
            InternalNode<KeyType, ValueType>* node = static_cast<InternalNode<KeyType, ValueType>*>(current_node);
            // upper_bound 找到第一个 大于 key 的位置，这个位置的索引正好对应我们要去的子节点 children 的索引。
            auto it = std::upper_bound(node->keys.begin(), node->keys.begin() + node->key_count, key);
            int child_index = std::distance(node->keys.begin(), it);
            current_node = node->children[child_index].get();
        }

        LeafNode<KeyType, ValueType>* leaf_node = static_cast<LeafNode<KeyType, ValueType>*>(current_node);
        // std::lower_bound 找到第一个 不小于 key 的位置。
        auto it = std::lower_bound(leaf_node->keys.begin(), leaf_node->keys.begin() + leaf_node->key_count, key);
        int data_index = std::distance(leaf_node->keys.begin(), it);
        
        // 插入键值对
        leaf_node->keys.insert(it, key);
        leaf_node->values.insert(leaf_node->values.begin() + data_index, value);
        leaf_node->key_count++;

        // 1. 如果叶子节点的键数量没有超过上限，插入完成。
        if(leaf_node->key_count <= TREE_ORDER - 1) {
            return;
        }

        // 2. 如果叶子节点的键数量超过了上限，需要分裂
        // a. 分裂出一个新的叶子节点，将后一半的键和值移动到新节点。
        std::unique_ptr<LeafNode<KeyType, ValueType>> new_leaf_node = std::make_unique<LeafNode<KeyType, ValueType>>();

        int mid = leaf_node->key_count / 2;
        std::copy(leaf_node->keys.begin() + mid, leaf_node->keys.end(), new_leaf_node->keys.begin());
        std::copy(leaf_node->values.begin() + mid, leaf_node->values.end(), new_leaf_node->values.begin());
        new_leaf_node->key_count = leaf_node->key_count - mid;
        leaf_node->key_count = mid; // 不需要将新节点的 keys 的后半部分清空
        
        // b. 取新节点的第一个键作为待推向上一层的键(Promoted Key)
        KeyType promoted_key = new_leaf_node->keys[0];
        // c. 更新叶子节点的 next prev 指针
        new_leaf_node->prev = leaf_node;
        if (leaf_node->next != nullptr) {
            leaf_node->next->prev = new_leaf_node.get();
        }
        new_leaf_node->next = leaf_node->next;
        leaf_node->next = new_leaf_node.get();
        // d. 将这个 Promoted Key 和指向新节点的指针插入到父节点中
        SplitResult<KeyType, ValueType> result = insert_into_parent(leaf_node, promoted_key, std::move(new_leaf_node));
        // 如果 insert_into_parent 返回了需要分裂的结果，说明分裂一直传播到了根节点
        if (result.need_split) {
            auto new_root = std::make_unique<InternalNode<KeyType, ValueType>>();
            new_root->keys[0] = result.promoted_key;

            // 新根节点的第一个子节点是原来的根节点
            root->parent = new_root.get();
            new_root->children[0] = std::move(root);
            // 新根节点的第二个子节点是分裂后的新节点
            result.new_node->parent = new_root.get();
            new_root->children[1] = std::move(result.new_node);

            new_root->key_count = 1;
            root = std::move(new_root);
        }
    }

    Node<KeyType, ValueType>* find_sibling(Node<KeyType, ValueType>* node) {
        // 这里不能直接使用 node->prev 或者 node->next，因为 node 可能是内部节点
        return nullptr;
    }

    KeyType find_separator_key(Node<KeyType, ValueType>* node, Node<KeyType, ValueType>* sibling) {
        auto parent = node->parent;
        size_t node_index = std::find(parent->children.begin(), parent->children.end(), node) - parent->children.begin();
        // 1. 如果和左兄弟合并
        if (node->)
    }

    void handle_overflow(Node<KeyType, ValueType>* node) {
        // 1. 基线条件：如果没有下溢，直接返回
        if (node->key_count >= (TREE_ORDER - 1) / 2) {
            return;
        }
        // 2. 特殊情况：根节点
        // a. 内部节点

        // b. 叶子节点


        // 检查相邻的兄弟节点。如果兄弟节点比较“富裕”（键数量 > 最小值），就从它那里“借”一个键值对过来。
        auto sibling = find_sibling(node);
        // 这个过程需要更新父节点中的索引键。优先尝试此操作，因为它对树结构的改动较小。
        // a. 检查左兄弟节点
        if (node->prev != nullptr && node->parent == node->prev->parent && node->prev->key_count > (TREE_ORDER - 1) / 2) {
            // 从左兄弟节点借一个键值对
            auto left_sibling = node->prev;
            auto borrow_key = left_sibling->keys[left_sibling->key_count - 1];
            auto borrow_value = left_sibling->values[left_sibling->key_count - 1];

            for(size_t i = node->key_count; i > 0; --i) {
                node->keys[i] = node->keys[i - 1];
                node->values[i] = node->values[i - 1];
            }
            node->keys[0] = borrow_key;
            node->values[0] = borrow_value;
            left_sibling->key_count--;
            node->key_count++;

            // 更新父节点中的索引键
            auto parent = leaf_node->parent;
            parent->keys[parent->key_count - 1] = borrow_key;
        }   // b. 检查右兄弟节点 
        else if (leaf_node->next != nullptr && leaf_node->parent == leaf_node->next->parent && leaf_node->next->key_count > (TREE_ORDER - 1) / 2) {
            // 从右兄弟节点借一个键值对
            auto right_sibling = leaf_node->next;
            auto borrow_key = right_sibling->keys[0];
            auto borrow_value = right_sibling->values[0];

            for(size_t i = 0; i < right_sibling->key_count - 1; ++i) {
                right_sibling->keys[i] = right_sibling->keys[i + 1];
                right_sibling->values[i] = right_sibling->values[i + 1];
            }
            leaf_node->keys[leaf_node->key_count] = borrow_key;
            leaf_node->values[leaf_node->key_count] = borrow_value;
            right_sibling->key_count--;
            leaf_node->key_count++;

            // 更新父节点中的索引键
            auto parent = leaf_node->parent;
            parent->keys[parent->key_count - 1] = borrow_key;
        }   

        // 4. 如果兄弟节点也处于最小键数量，需要进行合并（Merge）
        // 合并当前节点和兄弟节点，将当前节点的键值对全部移动到兄弟节点，然后删除当前节点。
        // 这个过程需要更新父节点中的索引键，“拉下来”一个键。【需递归处理父节点可能的下溢】
        if (leaf_node->prev != nullptr && leaf_node->parent == leaf_node->prev->parent && leaf_node->prev->key_count == (TREE_ORDER - 1) / 2) {
            // 合并当前节点和左兄弟节点
            auto left_sibling = leaf_node->prev;
            for(size_t i = 0; i < leaf_node->key_count; ++i) {
                left_sibling->keys[left_sibling->key_count] = leaf_node->keys[i];
                left_sibling->values[left_sibling->key_count] = leaf_node->values[i];
                left_sibling->key_count++;
            }
            left_sibling->next = leaf_node->next;
            if (leaf_node->next != nullptr) {
                leaf_node->next->prev = left_sibling;
            }
            // 删除当前节点
            leaf_node->prev = nullptr;
            leaf_node->next = nullptr;
            leaf_node->key_count = 0;

            size_t leaf_index = std::find(left_sibling->parent->children.begin(), left_sibling->parent->children.end(), leaf_node) - left_sibling->parent->children.begin();
            left_sibling->parent->children.erase(left_sibling->parent->children.begin() + leaf_index);
            left_sibling->parent->keys.erase(left_sibling->parent->keys.begin() + leaf_index - 1);
            left_sibling->parent->key_count--;
        } else if (leaf_node->next != nullptr && leaf_node->parent == leaf_node->next->parent && leaf_node->next->key_count == (TREE_ORDER - 1) / 2) {
            
        } 
    }

    bool remove(const KeyType& key) {
        // 1. 查找要删除的键
        Node<KeyType, ValueType>* current_node = root.get();
        // 循环找到叶子结点
        while(!current_node->is_leaf) {
            InternalNode<KeyType, ValueType>* node = static_cast<InternalNode<KeyType, ValueType>*>(current_node);
            // upper_bound 找到第一个 大于 key 的位置，这个位置的索引正好对应我们要去的子节点 children 的索引。
            auto it = std::upper_bound(node->keys.begin(), node->keys.begin() + node->key_count, key);
            int child_index = std::distance(node->keys.begin(), it);
            current_node = node->children[child_index].get();
        }

        LeafNode<KeyType, ValueType>* leaf_node = static_cast<LeafNode<KeyType, ValueType>*>(current_node);
        // std::lower_bound 找到第一个 不小于 key 的位置。
        auto it = std::lower_bound(leaf_node->keys.begin(), leaf_node->keys.begin() + leaf_node->key_count, key);
        int data_index = std::distance(leaf_node->keys.begin(), it);

        if (data_index < leaf_node->key_count && leaf_node->keys[data_index] == key) {
            leaf_node->keys.erase(leaf_node->keys.begin() + data_index);
            leaf_node->values.erase(leaf_node->values.begin() + data_index);
            leaf_node->key_count--;
        }
        // 2. 如果节点未下溢（key_count >= lower[(m - 1) / 2]），则直接返回
        if(leaf_node->key_count >= (TREE_ORDER - 1) / 2) {
            return true;
        }
        // 3. 如果节点下溢，需要先进行重分配（Redistribution）
        handle_overflow(leaf_node);
    }

    std::unique_ptr<Node<KeyType, ValueType>> root = nullptr;
};

int main() {
    BPlusTree<int, int> tree;
    // 插入足够多的数据来触发分裂
    for (int i = 1; i <= 10; ++i) {
        tree.insert(i, i * 2);
        std::cout << "insert " << i << " " << i * 2 << std::endl;
        tree.print_tree();
    }


    int value = -1;
    bool found = tree.search(12, value);
    if (found) {
        std::cout << "Found key 12, value: " << value << std::endl;
    } else {
        std::cout << "Key 12 not found." << std::endl;
    }   
    return 0;
}