#include <algorithm>
#include <iostream>
#include <queue>
#include <math.h>
#include <cmath>
#include "lsmtree.h"

// 从写满的Memtable中创建SSTable
SSTable::SSTable(std::map<Key, Value> mem_table) {
    m_data_.reserve(mem_table.size());

    for (const auto& pair: mem_table) {
        m_data_.emplace_back(pair);
        m_size_ += pair.first.size() + pair.second.size();
    }
}


// 由 Compaction 的结果创建
SSTable::SSTable(std::vector<KVPair> data) {
    m_data_ = std::move(data);
    for (const auto& pair: m_data_) {
        m_size_ += pair.first.size() + pair.second.size();
    }
}

std::optional<Key> SSTable::get_first_key() const {
    if (m_data_.empty()) {
        return std::nullopt;
    }
    return m_data_.front().first;
}

std::optional<Key> SSTable::get_last_key() const {
    if (m_data_.empty()) {
        return std::nullopt;
    }
    return m_data_.back().first;
}

TieringCompaction::TieringCompaction(size_t max_t): max_t(max_t) {}

bool TieringCompaction::should_compact(const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) const {
    for (const auto& level: levels) {
        if (level.size() >= max_t) {
            return true;
        }
    }
    return false;
}

void TieringCompaction::add_sstable(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, std::shared_ptr<SSTable> sstable) {
    if (levels.empty()) {
        levels.resize(1);
    }
    // 将 sstable 放置到 level 0
    levels[0].emplace_back(sstable);
}

void TieringCompaction::compact(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) {
    // 1. 遍历每个 level，将所有 sstable 合并到一个新的 sstable 中
    for (size_t i = 0; i < levels.size(); ++i) {
        auto& cur_level = levels[i];
        if (cur_level.size() >= max_t) {
            auto new_sstable = merge_sstables(cur_level, DELETED);
            
            if (i + 1>= levels.size()) {
                levels.resize(i + 2);
            }
            levels[i + 1].emplace_back(std::make_shared<SSTable>(new_sstable));
            // 清空当前 level 的 sstable
            levels[i].clear();
        }
    }
}

LevelingCompaction::LevelingCompaction(size_t max_level_0_size, size_t max_t, size_t max_level_1_size): 
    max_level_0_size_(max_level_0_size), max_t_(max_t), max_level_1_size_(max_level_1_size) {}

size_t LevelingCompaction::calculate_level_size(const std::vector<std::shared_ptr<SSTable>>& level) const {
    size_t res = 0;
    for (auto it = level.begin(); it != level.end(); it++) {
        res += (*it)->size();
    }
    return res;
}

bool LevelingCompaction::should_compact(const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) const {
    if (levels.empty()) return false;
    // 1. 先检查 Level 0
    if (levels[0].size() >= max_level_0_size_) {
        return true;
    }
    // 2. 再检查后续的 level 的大小是否超过阈值
    size_t threshold = max_level_1_size_;
    for (size_t i = 1; i < levels.size(); ++i) {
        if (calculate_level_size(levels[i]) >= threshold) {
            return true;
        }
        threshold *= max_t_;
    }
    return false;
}

void LevelingCompaction::add_sstable(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, std::shared_ptr<SSTable> sstable) {
    if (levels.empty()) {
        levels.resize(1);
    }
    // 将 sstable 放置到 level 0
    levels[0].emplace(levels[0].begin(), sstable);
    // levels[0].emplace_back(sstable);
}

void LevelingCompaction::compact(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) {
    if (levels[0].size() >= max_level_0_size_) {
        compact_level(levels, 0);
    }

    size_t current_level_threshold = max_level_1_size_;
    for (size_t i = 1; i < levels.size(); ++i) {
        if (calculate_level_size(levels[i]) >= current_level_threshold) {
            compact_level(levels, i);
        }
        current_level_threshold *= max_t_;
    }
}

void LevelingCompaction::compact_level(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, size_t level) {
    // auto merged_sstable = merge_sstables(levels[level], DELETED); 
    // 将当前 level 的 sstable 与下一 level 的 sstable 放到一起进行合并
    if (levels[level].empty()) {
        return;
    }
    std::cout << "Compaction start at level " << level << std::endl;
    std::vector<std::shared_ptr<SSTable>> tables_to_merge;
    // 只和 level + 1 中存在重叠的 key 进行压缩
    Key level_first_key = levels[level].front()->get_first_key().value();
    Key level_last_key = levels[level].back()->get_last_key().value();
    // 特例：L0 的 sstable 不是有序的
    for (const auto& sstable: levels[level]) {
        if (level == 0) {
            if (sstable->get_first_key().has_value() && sstable->get_first_key().value() < level_first_key) {
                level_first_key = sstable->get_first_key().value();
            }
            if (sstable->get_last_key().has_value() && sstable->get_last_key() > level_last_key) {
                level_last_key = sstable->get_last_key().value();
            }
        }
        tables_to_merge.emplace_back(sstable);
    }

    // 2. 找出 level + 1 层中与这些 key 重叠与不重叠的 sstable 
    std::vector<std::shared_ptr<SSTable>> overlapped_tables;
    std::vector<std::shared_ptr<SSTable>> non_overlapped_tables;

    // 如果下一层还不存在，创建
    if (level + 1 >= levels.size()) {
        levels.resize(level + 2);
    }
    for (const auto& table: levels[level + 1]) {
        try {
            Key level_next_first_key = table->get_first_key().value();
            Key level_next_last_key = table->get_last_key().value();
            // 如果和上一层存在重叠部分
            if (level_next_first_key > level_last_key || level_next_last_key < level_first_key) {
                non_overlapped_tables.emplace_back(table);
            } else {
                overlapped_tables.emplace_back(table);
            }
        } catch(std::bad_optional_access) {
            std::cout << "Leveling compaction bad optional access" << std::endl;
            return;
        }
    }

    tables_to_merge.insert(tables_to_merge.end(), overlapped_tables.begin(), overlapped_tables.end());
    if (tables_to_merge.empty()) {
        return;
    }
    auto res = merge_sstables(tables_to_merge, DELETED);
    levels[level].clear();
    levels[level + 1].clear();

    // 先加入合并后的数据
    if (!res.empty()) {
        levels[level + 1].emplace_back(std::make_shared<SSTable>(std::move(res)));
    }
    // 再加入未重叠的 sstable
    levels[level + 1].insert(levels[level + 1].end(), non_overlapped_tables.begin(), non_overlapped_tables.end());
    std::sort(levels[level + 1].begin(), levels[level + 1].end(), 
        [](const std::shared_ptr<SSTable>& t1, const std::shared_ptr<SSTable>& t2) {
            return t1->get_first_key().value() < t2->get_first_key().value();
        }
    );
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

LSMTree::LSMTree(size_t threshold_size, std::unique_ptr<Compaction> comp): m_threshold_size_(threshold_size), m_compaction_strategy_(std::move(comp)) {}

void LSMTree::put(const Key& key, const Value& value){
    // 1. 插入到 Memtable
    auto it = m_memtable_.find(key);
    // 2. 更新 memtable 的 size
    if (it != m_memtable_.end() && it->first == key) {
        // key 存在，加上新 value 与旧 value 的差值
        m_memtable_size_ += (value.size() - it->second.size());
    } else {
        // key 不存在，加上 key 与 value 的大小
        m_memtable_size_ += (key.size() + value.size());
    }
    // 3. 插入到 memtable 中
    m_memtable_[key] = value;
    std::cout << "Add a new KVPair to memtable: " << key << " -> " << value << std::endl; 
    std::cout << "Memtable size: " << m_memtable_size_ << std::endl;
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
    m_compaction_strategy_->add_sstable(m_sstables_, new_sstable);
    // m_sstables_[0].emplace_back(new_sstable);
    if (m_compaction_strategy_->should_compact(m_sstables_)) {
        std::cout << "Compaction start!!!" << std::endl;
        m_compaction_strategy_->compact(m_sstables_);
    }

    m_memtable_.clear();
    m_memtable_size_ = 0;

    std::cout << "A Memtable has flushed!!!" << std::endl;
}

void LSMTree::del(const Key& key) {
    // del 的过程与 put 是类似的，也是增加一条记录
    auto it = m_memtable_.find(key);
    if (it != m_memtable_.end() && it->first == key) {
        // key 存在，加上 deleted value 与旧 value 的差值
        m_memtable_size_ += (m_deleted_value_.size() - it->second.size());
    } else {
        // key 不存在，加上 key 与 deleted value 的大小
        m_memtable_size_ += (key.size() + m_deleted_value_.size());
    }

    m_memtable_[key] = m_deleted_value_;
    if (m_memtable_size_ >= m_threshold_size_) {
        flush();
    }
}

std::optional<Value> LSMTree::get(const Key& key) {
    // 1. 从 memtable 中查找
    auto it = m_memtable_.find(key);
    if (it != m_memtable_.end()) {
        // 如果是 TOMBSTONE，说明已删除，返回 nullopt
        if (it->second == m_deleted_value_) {
            return std::nullopt;
        }
        return it->second;
    }

    // L0 的 sstable 的键可能会重叠
    if (!m_sstables_.empty()) {
        for(const auto& sstable: m_sstables_[0]) {
            auto it = sstable->get(key);
            if (it.has_value() && it.value() != m_deleted_value_) {
                return it.value();
            }
        }
    }

    for (size_t i = 1; i < m_sstables_.size(); ++i) {
        const auto& level_sstables = m_sstables_[i];
        // 找到第一个 lastkey 大于 key 的 sstable
        if (level_sstables.empty()) continue;
        auto it = std::lower_bound(level_sstables.begin(), level_sstables.end(), key, 
                [](const std::shared_ptr<SSTable>& table, const Key& key) {
                    return table->get_last_key() < key;
                }
        );

        if (it != level_sstables.end()) {
            auto res = (*it)->get(key);
            if (res.has_value()) {
                if( res == m_deleted_value_) {
                    return std::nullopt;
                }
                return res.value();
            } 
        }
    }

    return std::nullopt;

    // 2. 从 sstable 中查找，可进行二分查找
    // for (const auto& level: m_sstables_) {
    //     // 因为 sstable 是按照 emplace_back 方法插入的，所以需要从后往前遍历
    //     // 这样才能查询到最新的数据
    //     for(auto it = level.rbegin(); it != level.rend(); ++it) {
    //         auto res = (*it)->get(key);
    //         if (res.has_value()) {
    //             if(res.value() == m_deleted_value_) {
    //                 return std::nullopt;
    //             }
    //             return res.value();
    //         }
    //     }
    // }

    return std::nullopt;
}

std::vector<KVPair> merge_sstables(std::vector<std::shared_ptr<SSTable>>& sstables, const Value deleted_value) {
    std::vector<KVPair> res;

    // 使用 priority_queue 进行 K 路合并操作进行 merge
    // [Key, table_idx, Value, data_idx]
    using t = std::tuple<Key, size_t, Value, size_t>;
    // 按照 key 的升序排序
    std::priority_queue<t, std::vector<t>, std::greater<t>> min_heap;

    // 将每个 sstable 的第一个元素加入 min_heap
    for (size_t i = 0; i < sstables.size();++i) {
        if (!sstables[i]->is_empty()) {
            auto& entries = sstables[i]->get_all_data();
            min_heap.push({entries[0].first, i, entries[0].second, 0});
        }
    }

    // 用于处理循环的第一次迭代（last_key 为空的情况）
    bool is_first = true;
    // 标识当前处理的最新的 key
    Key last_key;
    while (!min_heap.empty()) {
        auto [key, table_idx, value, data_idx] = min_heap.top();
        min_heap.pop();
        
        // 只保留最新的数据
        if (is_first || key != last_key) {
            res.emplace_back(key, value);
            last_key = key;
            is_first = false;
        } 

        auto& cur_sstable_entries = sstables[table_idx]->get_all_data();
        // 将当前 sstable 下一个元素加入 min_heap
        if (data_idx + 1 < cur_sstable_entries.size()) {
            auto next_entry = cur_sstable_entries[data_idx + 1];
            min_heap.push({next_entry.first, table_idx, next_entry.second, data_idx + 1});
        }
    }

    return res;
}

void LSMTree::print() const {
    std::cout << "--- LSM-Tree Structure ---" << std::endl;
    std::cout << "MemTable Size: " << m_memtable_size_ << " / " << m_threshold_size_ << " bytes" << std::endl;
    for (size_t i = 0; i < m_sstables_.size(); ++i) {
        std::cout << "Level " << i << " (" << m_sstables_[i].size() << " tables):" << std::endl;
        for (const auto& table : m_sstables_[i]) {
            std::cout << "  - SSTable (size: " << table->size() << " bytes, keys: " 
                      << table->get_all_data().size() << ")" << std::endl;
        }
    }
    std::cout << "--------------------------" << std::endl;
}