#include "lsmtree.h"
#include <iostream>
#include <optional>

// template class std::vector<Value>;
template class std::vector<std::shared_ptr<SSTable>>;

// 辅助函数，让打印更方便
void print_get_result(const std::string& key, const std::optional<Value>& val) {
    std::cout << "Getting '" << key << "': ";
    if (val.has_value()) {
        std::cout << "'" << *val << "'" << std::endl;
    } else {
        std::cout << "Not Found." << std::endl;
    }
}

std::string make_key(int i) {
    std::string s = std::to_string(i);
    // 给数字左边补0，例如 5 -> "key005"
    return "key" + std::string(3 - s.length(), '0') + s;
}

void test_leveling_advanced() {
    std::cout << "\n===== Running Advanced Leveling Strategy Test =====\n" << std::endl;
    // --- 参数设置 ---
    // L0 SSTable 数量达到4时，触发 L0->L1 合并
    // 层级大小比例为2倍 (L1 > L0 * 2, L2 > L1 * 2 ...)
    // L1 的基础大小阈值为 250 字节
    // MemTable 阈值为 100 字节
    auto strategy = std::make_unique<LevelingCompaction>(50, 4, 2, 250); // 您代码中的构造函数
    LSMTree tree(50, 50, std::move(strategy));

    // --- Stage 1: 填满L0并触发第一次 L0->L1 Compaction ---
    std::cout << "--- Stage 1: Filling L0 to trigger L0->L1 compaction ---" << std::endl;
    for (int i = 19; i >= 0; --i) {
        // 每个 K-V 对大概 6 + 15 = 21 字节, 5个左右触发一次flush
        tree.put(make_key(i), "initial_value_" + std::to_string(i));
    }
    // 此时 L0 应该有4个 SSTable，并已触发合并
    std::cout << "\n[State after initial L0->L1 compaction]" << std::endl;
    tree.print();
    // 验证：L0 应该为空，L1 应该有1个大的 SSTable
    print_get_result(make_key(5), tree.get(make_key(5)));  // 验证数据存在
    print_get_result(make_key(19), tree.get(make_key(19))); // 验证数据存在

    // --- Stage 2: 测试更新与删除 ---
    std::cout << "\n--- Stage 2: Testing updates and deletes ---" << std::endl;
    // 更新一个已在 L1 的 key
    std::cout << "\nUpdating a key that is now in L1..." << std::endl;
    tree.put(make_key(10), "UPDATED_VALUE_IN_MEMTABLE");
    print_get_result(make_key(10), tree.get(make_key(10))); // 应从 MemTable 读到新值

    // 删除一个已在 L1 的 key
    std::cout << "\nDeleting a key that is now in L1..." << std::endl;
    tree.del(make_key(15));
    print_get_result(make_key(15), tree.get(make_key(15))); // 应因 MemTable 中的墓碑而找不到

    // 再插入一些数据，将上述的更新和删除操作 flush 到 L0
    for (int i = 20; i < 25; ++i) {
        tree.put(make_key(i), "second_batch_value_" + std::to_string(i));
    }
    std::cout << "\n[State after flushing updates/deletes to L0]" << std::endl;
    tree.print();
    // 验证：L1 有旧数据，L0 有一个SSTable包含更新和删除标记

    // --- Stage 3: 合并包含更新和删除的L0 ---
    std::cout << "\n--- Stage 3: Compacting L0 which contains updates/deletes ---" << std::endl;
    // 继续插入数据填满L0，触发 L0->L1 的第二次合并
    for (int i = 25; i < 40; ++i) {
        tree.put(make_key(i), "third_batch_value_" + std::to_string(i));
    }
    std::cout << "\n[State after compacting L0 with updates/deletes into L1]" << std::endl;
    tree.print();
    
    // 验证：L1 现在应该只有一个更新后的大SSTable
    print_get_result(make_key(10), tree.get(make_key(10))); // 应读到合并后的新值 "UPDATED_VALUE_IN_MEMTABLE"
    print_get_result(make_key(15), tree.get(make_key(15))); // 应因墓碑被合并掉而彻底找不到

    // --- Stage 4: 触发级联合并 (Cascading Compaction) L1 -> L2 ---
    std::cout << "\n--- Stage 4: Triggering cascading compaction (L1 -> L2) ---" << std::endl;
    // L1 的大小阈值是 250 字节。我们已经写入了约40个记录，大小应该接近并可能超过阈值
    // 我们再执行两次 L0->L1 的合并，确保 L1 的大小远超阈值，从而触发 L1->L2
    std::cout << "\nForcefully growing L1 to trigger L1->L2 compaction..." << std::endl;
    for (int i = 60; i >= 40; --i) {
        // 大量写入数据
        tree.put(make_key(i), "cascade_trigger_value_" + std::to_string(i));
    }
    std::cout << "\n[Final state after cascading compaction]" << std::endl;
    tree.print();

    // 最终验证
    std::cout << "\n--- Final Verification ---" << std::endl;
    print_get_result(make_key(1), tree.get(make_key(1)));     // 验证早期数据是否还在 (在L2)
    print_get_result(make_key(10), tree.get(make_key(10)));   // 验证更新过的数据是否还在 (在L2)
    print_get_result(make_key(15), tree.get(make_key(15)));   // 验证删除的数据是否真的没了
    print_get_result(make_key(59), tree.get(make_key(59)));   // 验证最新数据是否还在 (在L0或L1)
}

void test_tiering() {
    std::cout << "\n===== Running Tiering Strategy Example (Corrected) =====\n" << std::endl;
    // Tiering策略：Tier中SSTable数量达到2时合并
    // MemTable阈值：50字节
    auto strategy = std::make_unique<TieringCompaction>(2, 50); // max_t = 2, sstable_size = 50
    LSMTree tree(50, 50, std::move(strategy));

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

void test_tiering_advanced() {
    std::cout << "\n===== Running Advanced Tiering Strategy Test =====\n" << std::endl;
    // --- 参数设置 ---
    // Tiering策略: 当一个层(Tier)的SSTable数量达到3时，触发合并
    // MemTable 阈值: 60 字节
    auto strategy = std::make_unique<TieringCompaction>(50, 3); // max_t = 3
    LSMTree tree(50, 50, std::move(strategy));

    // --- Stage 1: 填满L0并触发第一次 L0->L1 Compaction ---
    std::cout << "--- Stage 1: Filling L0 to trigger L0->L1 compaction ---" << std::endl;
    // 写入数据，触发3次 flush，生成3个SSTable到L0
    // 每个 K-V 对大概 6 + 10 = 16 字节, 4个触发一次flush
    for (int i = 0; i < 12; ++i) {
        tree.put(make_key(i), "value_" + std::to_string(i));
    }
    // 此时 L0 应该有3个SSTable，当下次flush发生时，会触发 L0->L1 的合并
    std::cout << "\nTriggering the compaction with one more flush..." << std::endl;
    tree.put("trigger", "compaction"); // 这次 put 会触发第4次 flush 和 L0 的合并

    std::cout << "\n[State after initial L0->L1 compaction]" << std::endl;
    tree.print();
    // 验证：L0 应该只包含1个SSTable(来自"trigger")，L1 应该有1个大的合并后的SSTable
    print_get_result(make_key(5), tree.get(make_key(5)));  // 验证数据存在
    print_get_result(make_key(11), tree.get(make_key(11))); // 验证数据存在

    // --- Stage 2: 测试更新与删除 ---
    std::cout << "\n--- Stage 2: Testing updates and deletes ---" << std::endl;
    // 更新一个已在 L1 的 key
    tree.put(make_key(10), "UPDATED_VALUE");
    // 删除一个已在 L1 的 key
    tree.del(make_key(5));
    print_get_result(make_key(10), tree.get(make_key(10))); // 应从 MemTable 读到新值
    print_get_result(make_key(5), tree.get(make_key(5))); // 应因 MemTable 中的墓碑而找不到

    // 再插入一些数据，将上述的更新和删除操作 flush 到 L0
    for (int i = 20; i < 25; ++i) {
        tree.put(make_key(i), "second_batch_value_" + std::to_string(i));
    }
    std::cout << "\n[State after flushing updates/deletes to L0]" << std::endl;
    tree.print();
    // 验证：L1 有旧数据，L0 有2个SSTable (1个来自Stage 1的trigger, 1个来自本次flush)

    // --- Stage 3: 再次填满 L0，使其合并并追加到 L1 ---
    std::cout << "\n--- Stage 3: Compacting L0 and appending to L1 ---" << std::endl;
    // L0已有2个SSTable，再触发1次flush即可再次填满L0
    for (int i = 30; i < 35; ++i) {
        tree.put(make_key(i), "third_batch_value_" + std::to_string(i));
    }
    std::cout << "\n[State after compacting L0 and appending to L1]" << std::endl;
    tree.print();
    // 验证：L0清空，L1现在应该有两个SSTable (一个是Stage 1的产物，一个是Stage 3的产物)
    // 此时查询 key(10) 和 key(5) 应该从L1中较新的SSTable（第二个）中获取状态
    print_get_result(make_key(10), tree.get(make_key(10))); // 应读到 "UPDATED_VALUE"
    print_get_result(make_key(5), tree.get(make_key(5)));   // 应该找不到

    // --- Stage 4: 触发级联合并 (Cascading Compaction) L1 -> L2 ---
    std::cout << "\n--- Stage 4: Triggering cascading compaction (L1 -> L2) ---" << std::endl;
    // L1 现在有2个SSTable，我们只需要再从L0合并1个SSTable过来，就能触发L1的合并
    std::cout << "\nCreating one more SSTable in L1 to trigger L1->L2 compaction..." << std::endl;
    for (int i = 40; i < 75; ++i) { // 产生3个新的SSTable到L0
        tree.put(make_key(i), "cascade_trigger_value_" + std::to_string(i));
    }
    tree.put("final_trigger", "cascade"); // 触发L0->L1合并

    // 上一步的flush将L0的3个table合并成1个新table并放入L1，
    // 此刻L1拥有了3个table，立即触发 L1->L2 的合并。
    std::cout << "\n[Final state after cascading compaction]" << std::endl;
    tree.print();

    // --- 最终验证 ---
    std::cout << "\n--- Final Verification ---" << std::endl;
    print_get_result(make_key(1), tree.get(make_key(1)));     // 验证早期数据 (在L2)
    print_get_result(make_key(10), tree.get(make_key(10)));   // 验证更新过的数据 (在L2，且是新值)
    print_get_result(make_key(5), tree.get(make_key(5)));     // 验证删除的数据是否真的没了
    print_get_result(make_key(50), tree.get(make_key(50)));   // 验证较新的数据 (在L2)
}

int main() {
    // test_tiering_advanced();
    test_leveling_advanced();
    return 0;
}