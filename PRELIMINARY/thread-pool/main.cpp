#include <iostream>
#include <chrono>
#include "threadpool.h"

std::mutex cout_mutex;

void example_task(int task_id) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "Task " << task_id << " executed by thread " << std::this_thread::get_id() << std::endl;
}

int main() {
    // 创建一个包含4个工作线程的线程池
    ThreadPool pool(4);

    std::cout << "Submitting 20 tasks to the thread pool..." << std::endl;

    // 提交20个任务
    for (int i = 0; i < 20; ++i) {
        pool.enqueue([i] {
            example_task(i);
        });
    }

    std::cout << "All tasks submitted. Waiting for them to complete..." << std::endl;
    
    // main线程可以做其他事情
    std::this_thread::sleep_for(std::chrono::seconds(5));

    std::cout << "Main thread finished. Thread pool will be destroyed upon exiting scope." << std::endl;
    return 0;
}