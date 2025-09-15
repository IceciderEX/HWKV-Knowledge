#include <algorithm>
#include <iostream>
#include "lsmtree.h"

// 从写满的Memtable中创建SSTable
SSTable::SSTable(std::map<Key, Value> mem_table) {
    m_data_.reserve(mem_table.size());

    for (const auto& pair: mem_table) {
        for (const auto& pair: mem_table) {
            m_data_.emplace_back(pair);
            m_size_ += pair.first.size() + pair.second.size();
        }
    }
}


// 由 Compaction 的结果创建
SSTable::SSTable(std::vector<KVPair> data) {
    // TODO
}

// get 
std::optional<Value> SSTable::get(const Key& key) const {
    auto res = std::lower_bound(m_data_.begin(), m_data_.end(), key, [](const KVPair p, const Key key) {
        return p.first < key;
    });

    if (res != m_data_.end() && res->first == key) {
        return res->second;
    }
    return std::nullopt;
}

LSMTree::LSMTree(size_t threshold_size): m_threshold_size_(threshold_size) {}

void LSMTree::put(const Key& key, const Value& value){
    // 1. 插入到 Memtable
    auto it = m_memtable_.find(key);
    // 2. 更新 memtable 的 size
    if (it != m_memtable_.end() && it->first == key) {
        m_memtable_size_ += (value.size() - it->second.size());
    } else {
        m_memtable_size_ += value.size();
    }
    // 3. 插入到 memtable 中
    m_memtable_[key] = value;
    std::cout << "Add a new KVPair to memtable: " << key << " -> " << value << std::endl; 
    if (m_memtable_size_ >= m_threshold_size_) {
        flush();
    }
}

void LSMTree::flush() {
    if (m_memtable_.empty()) {
        return;
    }
    // 将 memtable 转换为一个不可变的 sstable，并放置到 level 0
    auto new_sstable = std::make_shared<SSTable>(m_memtable_);

    if (m_sstables_.size() == 0) {
        m_sstables_.resize(1);
    }
    m_sstables_.emplace_back(new_sstable);

    m_memtable_.clear();
    m_memtable_size_ = 0;

    std::cout << "A Memtable has flushed!!!" << std::endl;

    compact();
}

void LSMTree::print() const {

}