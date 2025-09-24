// main.cpp
#include "skiplist.h"
#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <mutex>
#include <cassert>
#include <memory>

// 用于在多线程测试中安全地打印
std::mutex g_print_mutex;

void print_test_result(const std::string& test_name, bool success) {
    std::lock_guard<std::mutex> lock(g_print_mutex);
    std::cout << "[ TEST ] " << test_name << ": " << (success ? "PASSED" : "FAILED") << std::endl;
}

// 单线程测试函数
void run_single_thread_tests() {
    std::cout << "\n--- Running Single-Threaded Tests ---\n" << std::endl;
    Skiplist sl(12);

    // 1. 基本插入和获取
    sl.insert("key1", "value1");
    sl.insert("key2", "value2");
    auto val1 = sl.get("key1");
    print_test_result("Basic Insert & Get", val1.has_value() && val1.value() == "value1");

    // 2. 包含性检查
    print_test_result("Contains (existing key)", sl.contains("key2"));
    print_test_result("Contains (non-existing key)", !sl.contains("key3"));

    // 3. 插入重复键
    bool insert_duplicate_result = sl.insert("key1", "value1_new");
    auto val1_after_duplicate = sl.get("key1");
    print_test_result("Insert Duplicate Key", !insert_duplicate_result && val1_after_duplicate.value() == "value1");

    // 4. 获取不存在的键
    auto non_existing_val = sl.get("key_non_exist");
    print_test_result("Get Non-existing Key", !non_existing_val.has_value());

    // 5. 顺序性测试
    sl.insert("d", "4");
    sl.insert("b", "2");
    sl.insert("a", "1");
    sl.insert("c", "3");
    
    Node* node_a = sl.find_greater_or_equal("a");
    Node* node_b = node_a ? node_a->next_[0].load() : nullptr;
    Node* node_c = node_b ? node_b->next_[0].load() : nullptr;
    Node* node_d = node_c ? node_c->next_[0].load() : nullptr;
    bool order_correct = (node_a && node_a->key_ == "a") &&
                         (node_b && node_b->key_ == "b") &&
                         (node_c && node_c->key_ == "c") &&
                         (node_d && node_d->key_ == "d");
    print_test_result("Key Order Test", order_correct);
}

// 多线程测试函数
void run_multi_thread_tests() {
    std::cout << "\n--- Running Multi-Threaded Tests ---\n" << std::endl;
    
    // 使用智能指针管理 Skiplist，方便在线程间共享
    auto sl = std::make_shared<Skiplist>(16);
    
    const int num_threads = 8;
    const int keys_per_thread = 100;
    std::vector<std::thread> threads;

    // 1. 并发插入测试
    std::cout << "Starting concurrent insert test..." << std::endl;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, keys_per_thread, sl]() {
            for (int j = 0; j < keys_per_thread; ++j) {
                int key_num = i * keys_per_thread + j;
                std::string key = "user_" + std::to_string(key_num);
                std::string value = "data_" + std::to_string(key_num);
                sl->insert(key, value);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 验证所有数据是否都已插入
    int count = 0;
    Node* current = sl->find_greater_or_equal(""); // 从头开始
    while (current != nullptr) {
        count++;
        std::cout << current->key_ << " ";
        current = current->next_[0].load();
    }
    print_test_result("Concurrent Insert Total Count", count == num_threads * keys_per_thread);
    // 2. 并发读写测试
    std::cout << "\nStarting concurrent read/write test..." << std::endl;
    threads.clear();
    std::atomic<bool> all_writers_done = false;
    std::atomic<int> read_failures = 0;

    // 创建写入线程
    for (int i = 0; i < num_threads / 2; ++i) {
        threads.emplace_back([i, sl]() {
            for (int j = 0; j < 500; ++j) {
                int key_num = 100000 + i * 500 + j; // 使用新的 key 范围
                sl->insert("rw_key_" + std::to_string(key_num), "rw_value");
            }
        });
    }
    // 创建读取线程
    for (int i = 0; i < num_threads / 2; ++i) {
        threads.emplace_back([&all_writers_done, &read_failures, sl]() {
            while (!all_writers_done) {

            }
            // 检查第一批插入的数据
            for (int k = 0; k < num_threads * keys_per_thread; ++k) {
                std::string key = "user_" + std::to_string(k);
                if (!sl->contains(key)) {
                    read_failures++;
                    std::lock_guard<std::mutex> lock(g_print_mutex);
                    std::cout << "  [FAIL] check failed to find key: " << key << std::endl;
                }
            }
            // 检查第二批（读写测试中）插入的数据
            for (int k = 0; k < (num_threads / 2) * 500; ++k) {
                int key_num = 100000 + k;
                std::string key = "rw_key_" + std::to_string(key_num);
                if (!sl->contains(key)) {
                    read_failures++;
                    std::lock_guard<std::mutex> lock(g_print_mutex);
                    std::cout << "  [FAIL] check failed to find key: " << key << std::endl;
                }
            }
            // // 写入完成后检查一次，确保所有数据都可读
            // for (int k = 0; k < (num_threads / 2) * 500; ++k) {
            //     int key_num = 100000 + k;
            //     if (!sl->contains("rw_key_" + std::to_string(key_num))) {
            //         read_failures++;
            //         std::lock_guard<std::mutex> lock(g_print_mutex);
            //         std::cout << "  [FAIL] Final check failed to find key: " << "rw_key_" + std::to_string(key_num) << std::endl;
            //     }
            // }
        });
    }

    // 等待所有写入线程结束
    for (size_t i = 0; i < num_threads / 2; ++i) {
        threads[i].join();
    }
    all_writers_done = true; // 通知读取线程可以停止随机读，并开始最终验证

    // 等待所有读取线程结束
    for (size_t i = num_threads / 2; i < threads.size(); ++i) {
        threads[i].join();
    }
    


    // 最终验证阶段：主线程亲自检查所有 key
    std::cout << "\n--- Final Verification ---\n" << std::endl;
    int final_read_failures = 0;
    // 检查第一批插入的数据
    for (int k = 0; k < num_threads * keys_per_thread; ++k) {
        std::string key = "user_" + std::to_string(k);
        if (!sl->contains(key)) {
            final_read_failures++;
            std::lock_guard<std::mutex> lock(g_print_mutex);
            std::cout << "  [FAIL] Final check failed to find key: " << key << std::endl;
        }
    }
    // 检查第二批（读写测试中）插入的数据
    for (int k = 0; k < (num_threads / 2) * 500; ++k) {
        int key_num = 100000 + k;
        std::string key = "rw_key_" + std::to_string(key_num);
        if (!sl->contains(key)) {
            final_read_failures++;
            std::lock_guard<std::mutex> lock(g_print_mutex);
            std::cout << "  [FAIL] Final check failed to find key: " << key << std::endl;
        }
    }

    print_test_result("Concurrent Read/Write (final consistency)", final_read_failures == 0);
    if (final_read_failures > 0) {
        std::cout << "Total final read failures: " << final_read_failures << std::endl;
    }
}

int main() {
    // 设置随机数种子
    srand(time(nullptr));

    // run_single_thread_tests();
    run_multi_thread_tests();

    return 0;
}