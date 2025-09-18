#ifndef __LSMTREE_H__
#define __LSMTREE_H__

#include <vector>
#include <map>
#include <string>
#include <optional>
#include <memory>

using Key = std::string;
using Value = std::string;
using KVPair = std::pair<Key, Value>;

const Value DELETED = "__TOMBSTONE__";

// MemTable 需要能够快速插入、删除和查找，并且需要保持键的有序性。
// SSTable 是一个有序的键值对集合，用于存储数据。
class SSTable {
public:
    // 构造函数
    // (1) SSTable从写满了的Memtable中新创建
    explicit SSTable(std::map<Key, Value> mem_table);
    // (2) Compaction
    explicit SSTable(std::vector<KVPair> data);
    ~SSTable() = default;

    // 查找
    std::optional<Value> get(const Key& key) const; 
    // 获取所有数据用于Compaction
    const std::vector<KVPair>& get_all_data() const { return m_data_; }

    // 获取SSTable的大小
    size_t size() const { return m_size_; }
    bool is_empty() { return m_data_.empty(); }
    // 获取该 sstable 的键范围
    std::optional<Key> get_first_key() const;
    std::optional<Key> get_last_key() const;
private:
    std::vector<KVPair> m_data_; // SSTable的数据
    size_t m_size_ = 0; // SSTable的大小
};

class Compaction {
public:
    Compaction() = default;
    virtual~Compaction() = default;
    // 是否需要Compaction
    virtual bool should_compact(const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) const = 0;
    // 进行Compaction
    virtual void compact(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) = 0;
    // 添加SSTable
    virtual void add_sstable(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, std::shared_ptr<SSTable> sstable) = 0;
};

class TieringCompaction: public Compaction {
public:
    TieringCompaction(size_t max_t);
    ~TieringCompaction() = default;
    // 是否需要Compaction
    bool should_compact(const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) const override;
    // 进行Compaction
    void compact(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) override;
    // 添加SSTable
    void add_sstable(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, std::shared_ptr<SSTable> sstable) override;
private:
    // max_t
    size_t max_t;
};

class LevelingCompaction: public Compaction {
public:
    LevelingCompaction(size_t max_level_0_size, size_t max_t, size_t max_level_1_size);
    ~LevelingCompaction() = default;
    // 是否需要 Compaction
    bool should_compact(const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) const override;
    // 进行 Compaction
    void compact(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels) override;
    void compact_level(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, size_t level);
    // 添加 SSTable
    void add_sstable(std::vector<std::vector<std::shared_ptr<SSTable>>>& levels, std::shared_ptr<SSTable> sstable) override;
    size_t calculate_level_size(const std::vector<std::shared_ptr<SSTable>>& level) const;
private:
    // level 0 的最大 sstable 数量
    size_t max_level_0_size_;
    // max_t;
    size_t max_t_;
    // level 1 的最大大小
    size_t max_level_1_size_;
};

// 将多个已排序的 sstable 通过多路归并算法合并为一个 
std::vector<KVPair> merge_sstables(std::vector<std::shared_ptr<SSTable>>& sstables, const Value deleted_value);

class LSMTree {
public:
    // 构造函数, threshold_size为Memtable的大小阈值
    explicit LSMTree(size_t threshold_size, std::unique_ptr<Compaction> comp);

    void put(const Key& key, const Value& value);
    void del(const Key& key);
    std::optional<Value> get(const Key& key);
    void print() const;

private:
    void flush();
    size_t get_memtable_size() const { return m_memtable_size_; };

    const Value m_deleted_value_ = "__TOMBSTONE__";
    std::map<Key, Value> m_memtable_; // Memtable
    std::vector<std::vector<std::shared_ptr<SSTable>>> m_sstables_; // SSTables
    
    size_t m_threshold_size_; // Memtable的大小阈值
    size_t m_memtable_size_ = 0; // Memtable的当前大小
    std::unique_ptr<Compaction> m_compaction_strategy_; // 使用的压缩策略
};

#endif