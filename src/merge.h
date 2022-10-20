#include <cassert>
#include <queue>

#include "types.h"

using namespace std;

/**
 * 可以看做 一个 run 的迭代器
 */
struct merge_entry {
    int precedence;         // 优先级, 优先权
    entry_t *entries;       // 键值对数据
    long num_entries;       // 键值对长度
    int current_index = 0;  // 当前位置
    entry_t head(void) const {return entries[current_index];}
    bool done(void) const {return current_index == num_entries;}
    /**
     * 比较函数: 首先: 为 head()方法的返回值，即entries[current_index]
     * 当head()方法返回值相同时, 按precedence排序
     * @param other
     * @return
     */
    bool operator>(const merge_entry& other) const {
        // Order first by keys, then by precedence
        if (head() == other.head()) {
            assert(precedence != other.precedence);
            return precedence > other.precedence;
        } else {
            return head() > other.head();
        }
    }
};

typedef struct merge_entry merge_entry_t;

class MergeContext {
    priority_queue<merge_entry_t, vector<merge_entry_t>, greater<merge_entry_t>> queue;
public:
    void add(entry_t *, long);
    entry_t next(void);
    bool done(void);
};
