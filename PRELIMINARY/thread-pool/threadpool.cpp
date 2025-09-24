#include "threadpool.h"
#include <atomic>
#include <functional>
#include <mutex>

template <typename T>
LockFreeQueue<T>::LockFreeQueue() {
    Node* dummy = new Node(T());
    m_head_.store(dummy, std::memory_order_relaxed);
    m_tail_.store(dummy, std::memory_order_relaxed);
}

template <typename T>
LockFreeQueue<T>::~LockFreeQueue() {
    
}

template <typename T>
void LockFreeQueue<T>::push(const T data) {
    Node* new_node = new Node(std::move(data));
    Node* old_tail = nullptr;

    while (true) { 
        // 1. 读取当前的 tail 
        old_tail = m_tail_.load(std::memory_order_acquire);

        // 2. 尝试将新节点挂载到原有的 tail 之后（设置为当前 tail 的 next）
        Node* expected = nullptr;
        // 如果尾节点的 next 为 nullptr，说明没有其他的线程修改过，将 new_node 赋值给 tail
        if (old_tail->next.compare_exchange_weak(expected, new_node, std::memory_order_release)) {
            break;
        }
        // 如果失败了，说明有其他线程更新了 old_tail->next（不为空），重试循环
    }

    // 尝试更新原来的 tail 指针，使其指向新的尾节点 new_node
    // 如果 tail 的指针没变，则将 tail 更新为 new_node
    m_tail_.compare_exchange_strong(old_tail, new_node, std::memory_order_release);
    m_data_size_.fetch_add(1, std::memory_order_release);
}

template<typename T>
bool LockFreeQueue<T>::pop(T& data) {
    Node* old_head = nullptr;
    Node* new_head = nullptr;
    Node* old_tail = nullptr;

    while (true) {
        // 头结点
        old_head = m_head_.load(std::memory_order_acquire);
        // 第一个数据节点 
        new_head = old_head->next.load(std::memory_order_acquire);
        // 尾节点
        old_tail = m_tail_.load(std::memory_order_acquire);

        // 确保在这期间 head 没有变动
        if (old_head == m_head_.load(std::memory_order_acquire)) {
            // 如果 head 和 tail 相同
            if (old_head == old_tail) {
                // 1. 可能队列确实为空
                if (new_head == nullptr) {
                    return false;
                }
                // 2. 可能 push 操作没有成功更新 tail，帮助其更新为第一个数据节点
                m_tail_.compare_exchange_strong(old_tail, new_head, std::memory_order_release);
            } else {
                // 尝试移动 head 为第一个数据节点，如果修改成功，就可以进行 pop 了
                if (m_head_.compare_exchange_strong(old_head, new_head, std::memory_order_release)) {
                    // 获得第一个数据节点的值，赋值给 data，第一个数据节点的值就被 pop 了
                    data = std::move(new_head->data);

                    delete old_head;
                    m_data_size_.fetch_sub(1, std::memory_order_release);
                    return true;
                }
            }
        }
    }
}

template <typename T>
bool LockFreeQueue<T>::is_empty() {
    return m_head_.load(std::memory_order_acquire) == m_tail_.load(std::memory_order_acquire);
}

ThreadPool::ThreadPool(size_t num_threads) {
    for (size_t i = 0; i < num_threads; ++i) {
        m_workers_.emplace_back([this](){ this->worker_thread(); } );
    }
}

ThreadPool::~ThreadPool() {
    m_stop_.store(true, std::memory_order_release);
    m_cv_.notify_all();
    for (auto& thread: m_workers_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    m_task_queue_.push(task);
    m_cv_.notify_one();
}

void ThreadPool::worker_thread() {
    while (true) {
        std::function<void()> task;
        if (m_task_queue_.pop(task)) {
            task();
        } else {
            std::unique_lock<std::mutex> lock(m_mutex_);
            // 调用 wait，直到有新的任务来到 / 停止
            m_cv_.wait(lock, [this]() -> bool {
                return !m_task_queue_.is_empty() || m_stop_.load();
            });

            // 如果停止，也没有余下的任务，直接退出
            if (m_stop_.load() && m_task_queue_.is_empty()) {
                break;
            }
        }
    }
}
