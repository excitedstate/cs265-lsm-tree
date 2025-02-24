#include <atomic>

using namespace std;

class SpinLock {
    atomic_flag flag = ATOMIC_FLAG_INIT;
public:
    // 自旋锁, 用的 while
    void lock(void) {
        while (flag.test_and_set(memory_order_acquire)) {}
    }

    void unlock(void) {
        flag.clear(memory_order_release);
    }
};
