// main.cpp
#include "skiplist_opt.h"
#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <mutex>
#include <cassert>
#include <memory>
#include <cstring> // For strcpy

// 用于在多线程测试中安全地打印
std::mutex g_print_mutex;

void print_test_result(const std::string& test_name, bool success) {
    std::lock_guard<std::mutex> lock(g_print_mutex);
    std::cout << "[ TEST ] " << test_name << ": " << (success ? "PASSED" : "FAILED") << std::endl;
}

// 辅助函数，封装新的插入流程
void insert_key(Skiplist& sl, const std::string& key) {
    char* buffer = sl.allocate_key(key.length() + 1);
    strcpy(buffer, key.c_str());
    sl.insert(buffer);
}

// 单线程测试函数
void run_single_thread_tests() {
    std::cout << "\n--- Running Single-Threaded Tests ---\n" << std::endl;
    Skiplist sl(12);

    // 1. 基本插入和获取
    insert_key(sl, "key1");
    insert_key(sl, "key2");
    auto val1 = sl.get("key1");
    // 注意：由于优化后 Get() 返回的是 key 本身，所以我们验证返回的值是否等于 key
    print_test_result("Basic insert & Get", val1.has_value() && val1.value() == "key1");

    // 2. 包含性检查
    print_test_result("contains (existing key)", sl.contains("key2"));
    print_test_result("contains (non-existing key)", !sl.contains("key3"));

    // 3. 插入重复键
    char* buffer_dup = sl.allocate_key(std::string("key1").length() + 1);
    strcpy(buffer_dup, "key1");
    bool insert_duplicate_result = sl.insert(buffer_dup);
    auto val1_after_duplicate = sl.get("key1");
    print_test_result("insert Duplicate key", !insert_duplicate_result && val1_after_duplicate.value() == "key1");

    // 4. 获取不存在的键
    auto non_existing_val = sl.get("key_non_exist");
    print_test_result("get Non-existing key", !non_existing_val.has_value());

    // 5. 顺序性测试
    insert_key(sl, "d");
    insert_key(sl, "b");
    insert_key(sl, "a");
    insert_key(sl, "c");
    
    Node* node_a = sl.find_greater_or_equal("a");
    Node* node_b = node_a ? node_a->next(0) : nullptr;
    Node* node_c = node_b ? node_b->next(0) : nullptr;
    Node* node_d = node_c ? node_c->next(0) : nullptr;
    
    bool order_correct = (node_a && strcmp(node_a->key(), "a") == 0) &&
                         (node_b && strcmp(node_b->key(), "b") == 0) &&
                         (node_c && strcmp(node_c->key(), "c") == 0) &&
                         (node_d && strcmp(node_d->key(), "d") == 0);
    print_test_result("key Order Test", order_correct);
}

// 多线程测试函数
void run_multi_thread_tests() {
    std::cout << "\n--- Running Multi-Threaded Tests ---\n" << std::endl;
    
    auto sl = std::make_shared<Skiplist>(16);
    
    const int num_threads = 8;
    const int keys_per_thread = 1000;
    std::vector<std::thread> threads;

    // 1. 并发插入测试
    std::cout << "Starting concurrent insert test..." << std::endl;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, keys_per_thread, sl]() {
            for (int j = 0; j < keys_per_thread; ++j) {
                int key_num = i * keys_per_thread + j;
                std::string key = "user_" + std::to_string(key_num);
                
                // 使用新的两步插入法
                char* buffer = sl->allocate_key(key.length() + 1);
                strcpy(buffer, key.c_str());
                sl->insert(buffer);
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
        current = current->next(0); // 使用新的节点访问方法
    }
    print_test_result("Concurrent insert Total Count", count == num_threads * keys_per_thread);

    // 2. 并发读写测试
    std::cout << "\nStarting concurrent read/write test..." << std::endl;
    threads.clear();
    std::atomic<bool> writers_done = false;
    
    // 创建写入线程
    int writer_threads_count = num_threads / 2;
    for (int i = 0; i < writer_threads_count; ++i) {
        threads.emplace_back([i, sl]() {
            for (int j = 0; j < 500; ++j) {
                int key_num = 100000 + i * 500 + j; // 使用新的 key 范围
                std::string key = "rw_key_" + std::to_string(key_num);
                
                char* buffer = sl->allocate_key(key.length() + 1);
                strcpy(buffer, key.c_str());
                sl->insert(buffer);
            }
        });
    }
    // 创建读取线程
    int reader_threads_count = num_threads - writer_threads_count;
    std::atomic<int> read_failures = 0;
    for (int i = 0; i < reader_threads_count; ++i) {
        threads.emplace_back([&, sl]() {
            // 在写入线程工作时，可以进行一些随机读取（此部分简化）
            while (!writers_done.load()) {
                // 随机读一些可能存在的key
                int k = rand() % (num_threads * keys_per_thread);
                std::string key = "user_" + std::to_string(k);
                sl->contains(key.c_str());
            }
        });
    }

    // 等待所有写入线程结束
    for (size_t i = 0; i < writer_threads_count; ++i) {
        threads[i].join();
    }
    writers_done = true; // 通知读取线程可以停止

    // 等待所有读取线程结束
    for (size_t i = writer_threads_count; i < threads.size(); ++i) {
        threads[i].join();
    }
    
    // 最终验证阶段：主线程亲自检查所有 key
    std::cout << "\n--- Final Verification ---\n" << std::endl;
    int final_read_failures = 0;
    // 检查第一批插入的数据
    for (int k = 0; k < num_threads * keys_per_thread; ++k) {
        std::string key = "user_" + std::to_string(k);
        if (!sl->contains(key.c_str())) { // 使用 .c_str()
            final_read_failures++;
        }
    }
    // 检查第二批（读写测试中）插入的数据
    for (int i = 0; i < writer_threads_count; ++i) {
        for (int j = 0; j < 500; ++j) {
            int key_num = 100000 + i * 500 + j;
            std::string key = "rw_key_" + std::to_string(key_num);
            if (!sl->contains(key.c_str())) { // 使用 .c_str()
                final_read_failures++;
            }
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