#include <iostream>
#include <chrono>
#include "threadpool.h"

std::mutex cout_mutex;

// 示例线程任务函数
void example_task(int task_id) {
    // 使用 sleep_for 模拟任务
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "Task " << task_id << " executed by thread " << std::this_thread::get_id() << std::endl;
}

int main() {
    // 创建一个包含8个工作线程的线程池
    ThreadPool pool(8);

    std::cout << "Submitting 40 tasks to the thread pool..." << std::endl;

    // 提交40个任务
    for (int i = 0; i < 80; ++i) {
        pool.enqueue([i] {
            example_task(i);
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 100; i < 150; ++i) {
        pool.enqueue([i] {
            example_task(i);
        });
    }
    std::cout << "All tasks submitted. Waiting for them to complete..." << std::endl;
    std::cout << "Main thread finished. Thread pool will be destroyed upon exiting scope." << std::endl;
    return 0;
}
