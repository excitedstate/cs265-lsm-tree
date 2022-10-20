#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>  // for thread synchronization
#include <future>
#include <functional>  // for std::function
#include <stdexcept>

class ThreadPool {
public:
    ThreadPool(size_t);

    // 调度函数
    template<class F, class... Args>
    auto enqueue(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>;

    ~ThreadPool();

protected:
    // need to keep track of threads, so we can join them
    std::vector <std::thread> workers;
private:
    // the task queue
    std::queue <std::function<void()>> tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
        : stop(false) {
    for (size_t i = 0; i < threads; ++i)
        workers.emplace_back(
                [this] {
                    for (;;) {
                        std::function<void()> task;
                        /**
                         * 注意这个block, 主要是为了 加锁和释放锁
                         */
                        {
                            std::unique_lock <std::mutex> lock(this->queue_mutex);
                            this->condition.wait(lock,
                                                 [this] { return this->stop || !this->tasks.empty(); });
                            if (this->stop && this->tasks.empty())
                                return;
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                        }

                        task();
                    }
                }
        );
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args)
-> std::future<typename std::result_of<F(Args...)>::type> {
    // using: 插入的新任务的函数的返回值
    using return_type = typename std::result_of<F(Args...)>::type;
    /**
     * std::make_shared: 做一个智能指针: shared_ptr
     * std::packaged_task: 包装一个可调用的对象，并且允许异步获取该可调用对象产生的结果
     * std::bind可以将可调用对象和参数一起绑定，绑定后的结果使用std::function进行保存，并延迟调用到任何我们需要的时候
     * 这句话就是返回一个共享指针, 类型是 packaged_task 异步可调用对象
     * 指针指向的是: 一个函数和他的参数的结合体, 这样的封装很有意思
     */
    auto task = std::make_shared < std::packaged_task < return_type() > > (
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    /**
     * get_future() 异步执行, 获取结果 封装在 future 中
     * 通过res.get()方法获取真实值 return_type 类型
     */
    std::future <return_type> res = task->get_future();
    {
        // C++ 栈上 锁, 出栈的时候释放
        std::unique_lock <std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if (stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");
        /**
         * stop==true, 入队
         */
        tasks.emplace([task]() { (*task)(); });
    }


    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
    {
        std::unique_lock <std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker: workers)
        worker.join();
}

#endif
