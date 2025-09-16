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

int main() {
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

    return 0;
}