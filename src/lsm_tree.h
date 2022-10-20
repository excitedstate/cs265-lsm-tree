#include <vector>

#include "buffer.h"
#include "level.h"
#include "spin_lock.h"
#include "types.h"
#include "worker_pool.h"

#define DEFAULT_TREE_DEPTH 5
#define DEFAULT_TREE_FANOUT 10
#define DEFAULT_BUFFER_NUM_PAGES 1000
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_BF_BITS_PER_ENTRY 0.5

class LSMTree {
    // C0
    Buffer buffer;
    // 线程池
    WorkerPool worker_pool;
    float bf_bits_per_entry;
    // 多个 Level
    vector<Level> levels;
    // 获取相应位置的run
    Run * get_run(int);
    // rolling merge
    void merge_down(vector<Level>::iterator);
public:
    LSMTree(int, int, int, int, float);
    void put(KEY_t, VAL_t);
    void get(KEY_t);
    void range(KEY_t, KEY_t);
    void del(KEY_t);
    void load(std::string);
};
