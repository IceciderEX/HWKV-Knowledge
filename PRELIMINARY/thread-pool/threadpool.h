#ifndef __THREAD_POOL_H__
#define __THREAD_POOL_H__

#include <cstddef>
#include <iostream>
#include <atomic>
#include <memory>
#include <vector>
#include <thread>
#include <functional>
#include <mutex>
#include <condition_variable>

template <typename T>
class LockFreeQueue {
public:
    LockFreeQueue();
    ~LockFreeQueue();
    void push(const T data);
    bool pop(T& data);
    bool is_empty();
private:
    class Node {
    public:
        T data;
        std::atomic<Node*> next;

        // 
        Node(const T& data_): data(data_), next(nullptr) {};
        Node(T&& data_): data(std::move(data_)), next(nullptr) {};
        ~Node() = default;
    };

    // head 永远指向一个 dummy 节点，第一个数据节点为 head->next
    std::atomic<Node*> m_head_;
    std::atomic<Node*> m_tail_;
    std::atomic<size_t> m_data_size_ = 0;
};

class ThreadPool {
public:
    ThreadPool(size_t num_threads);
    ~ThreadPool();
    void enqueue(std::function<void()> task);
private:
    void worker_thread();

    std::atomic<bool> m_stop_;
    LockFreeQueue<std::function<void()>> m_task_queue_;
    std::vector<std::thread> m_workers_;
    std::mutex m_mutex_;
    std::condition_variable m_cv_;
};

#endif