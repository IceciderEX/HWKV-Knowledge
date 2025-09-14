#include <algorithm>
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


