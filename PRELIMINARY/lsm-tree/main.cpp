#include "lsmtree.h"
#include <iostream>
#include <optional>

// 辅助函数，让打印更方便
void print_get_result(const std::string& key, const std::optional<Value>& val) {
    std::cout << "Getting '" << key << "': ";
    if (val.has_value()) {
        std::cout << "'" << *val << "'" << std::endl;
    } else {
        std::cout << "Not Found." << std::endl;
    }
}

void test_leveling() {
    std::cout << "\n===== Running Leveling Strategy Example =====\n" << std::endl;
    // Leveling策略：L0最多2个SSTable，层级大小比例为5倍
    auto strategy = std::make_unique<LevelingCompaction>(2, 3, 50);
    LSMTree tree(50, std::move(strategy));

    std::cout << "--- Stage 1: Flushing to L0, no compaction ---" << std::endl;
    tree.put("key:01", "some_value_a");
    tree.put("key:02", "some_value_b");
    tree.put("key:03", "some_value_c"); // Flush 1 -> L0 has 1 table
    tree.print();

    std::cout << "\n--- Stage 2: Flushing to L0, triggering L0->L1 compaction ---" << std::endl;
    tree.put("key:04", "another_value_d");
    tree.put("key:05", "another_value_e");
    tree.put("key:06", "another_value_f"); // Flush 2 -> L0 has 2 tables -> Compaction!
    tree.print();

    std::cout << "\n--- Stage 3: Checking results after compaction ---" << std::endl;
    // 所有数据现在应该都在L1的一个大SSTable里
    print_get_result("key:01", tree.get("key:01"));
    print_get_result("key:06", tree.get("key:06"));

    std::cout << "\n--- Stage 4: Overwriting a key and flushing again ---" << std::endl;
    tree.put("key:01", "new_value_for_key_01"); // 这个新值在MemTable
    print_get_result("key:01", tree.get("key:01")); // 应该能获取到新值
    tree.put("key:07", "value_g");
    tree.put("key:08", "value_h");
    tree.put("key:09", "value_i"); // Flush 3 -> L0 has 1 table again
    tree.print();
}

void test_tiering() {
    std::cout << "\n===== Running Tiering Strategy Example (Corrected) =====\n" << std::endl;
    // Tiering策略：Tier中SSTable数量达到2时合并
    // MemTable阈值：50字节
    auto strategy = std::make_unique<TieringCompaction>(2);
    LSMTree tree(50, std::move(strategy));

    std::cout << "--- Stage 1: Triggering the first flush ---" << std::endl;
    // 这两个put操作会超过50字节，触发第一次flush
    tree.put("user:1001", "alice_in_wonderland"); // key(8) + value(21) = 29 bytes
    tree.put("user:1002", "bob_the_builder");     // key(8) + value(15) = 23 bytes. Total: 52 > 50 -> FLUSH!
    tree.print();

    std::cout << "\n--- Stage 2: Triggering the second flush and compaction ---" << std::endl;
    // 下面的操作将产生第二个SSTable，从而使Tier 0的数量达到2，触发compaction
    tree.del("user:1001");                       // key(8) + TOMBSTONE(11) = 19 bytes
    tree.put("user:1003", "charlie_chaplin");     // key(8) + value(15) = 23 bytes.
    tree.put("user:1004", "david_copperfield");   // key(8) + value(17) = 25 bytes. Total: 67 > 50 -> FLUSH!
    
    // flush后，Tier 0将有两个SSTable，立即触发compaction
    std::cout << "\n--- Structure after compaction ---" << std::endl;
    tree.print();

    std::cout << "\n--- Stage 3: Checking results after compaction ---" << std::endl;
    // user:1001 应该因为墓碑而在合并过程中被物理删除了
    print_get_result("user:1001", tree.get("user:1001")); 
    // 其他键应该可以在合并后的Tier 1中找到
    print_get_result("user:1002", tree.get("user:1002")); 
    print_get_result("user:1004", tree.get("user:1004"));
}

int main() {
    // test_tiering();
    test_leveling();
    return 0;
}