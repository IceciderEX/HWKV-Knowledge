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
    std::vector<KVPair> get_all_data() const { return m_data_; }

    // 获取SSTable的大小
    size_t size() const { return m_size_; }
    bool is_empty() { return m_data_.empty(); }
private:
    std::vector<KVPair> m_data_; // SSTable的数据
    size_t m_size_ = 0; // SSTable的大小
};

class LSMTree {
public:
    // 构造函数, threshold_size为Memtable的大小阈值
    explicit LSMTree(size_t threshold_size);

    void put(const Key& key, const Value& value);
    void del(const Key& key);
    std::optional<Value> get(const Key& key);

private:
    void flush();
    void compact();
    void print() const ;
    size_t get_memtable_size() const { return m_memtable_size_; };

    const Value m_deleted_value_ = "__TOMBSTONE__";
    std::map<Key, Value> m_memtable_; // Memtable
    std::vector<std::vector<std::shared_ptr<SSTable>>> m_sstables_; // SSTables
    
    size_t m_threshold_size_; // Memtable的大小阈值
    size_t m_memtable_size_ = 0; // Memtable的当前大小
};



#endif

