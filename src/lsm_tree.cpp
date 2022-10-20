#include <cassert>
#include <fstream>
#include <iostream>
#include <map>

#include "lsm_tree.h"
#include "merge.h"
#include "sys.h"

using namespace std;

ostream& operator<<(ostream& stream, const entry_t& entry) {
    stream.write((char *)&entry.key, sizeof(KEY_t));
    stream.write((char *)&entry.val, sizeof(VAL_t));
    return stream;
}

istream& operator>>(istream& stream, entry_t& entry) {
    stream.read((char *)&entry.key, sizeof(KEY_t));
    stream.read((char *)&entry.val, sizeof(VAL_t));
    return stream;
}

/*
 * LSM Tree
 */

LSMTree::LSMTree(int buffer_max_entries, int depth, int fanout,
                 int num_threads, float bf_bits_per_entry) :
                 bf_bits_per_entry(bf_bits_per_entry),
                 buffer(buffer_max_entries),
                 worker_pool(num_threads)
{
    long max_run_size;

    max_run_size = buffer_max_entries;

    while ((depth--) > 0) {
        levels.emplace_back(fanout, max_run_size);
        max_run_size *= fanout;
    }
}

void LSMTree::merge_down(vector<Level>::iterator current) {
    vector<Level>::iterator next;
    MergeContext merge_ctx;
    entry_t entry;

    assert(current >= levels.begin());

    /**
     * 合法性判断
     * 1. current是否还能插入一个run, 如果可以, 直接return, 现在可以继续插入元素了
     * 2. 如果达到了最大的那一层Level, 则无法继续进行, 因为没有空间了
     */
    if (current->remaining() > 0) {
        return;
    } else if (current >= levels.end() - 1) {
        die("No more space in tree.");
    } else {
        next = current + 1;
    }

    /*
     * If the next level does not have space for the current level,
     * recursively merge the next level downwards to create some
     * 判断下一层是否有run空间，如果没有，则递归地调用merge_down，为本层留出一个run的空间
     */

    if (next->remaining() == 0) {
        merge_down(next);
        assert(next->remaining() > 0);
    }

    /*
     * Merge all runs in the current level into the first
     * run in the next level
     * 经过递归的Merge，断言下一层可以插入至少一个run，这个run就是要生成的新run，通过以下过程进行本层的Merge生成一个新run
     * 将本层的所有run添加到一个MergeContext对象中，注意run是有时间顺序的，插入顺序决定了优先队列中的排序。
     * 以读模式打开该层所有run的文件映射
     */

    for (auto& run : current->runs) {
        merge_ctx.add(run.map_read(), run.size);
    }

    /**
     * 向next的runs的队头添加一个新的run(越靠前的run越新)
     */
    next->runs.emplace_front(next->max_run_size, bf_bits_per_entry);
    /**
     * 接下来要开始向这个run写数据了, 以写模式打开刚插入run的文件映射
     * 注意run.put()是断言文件已经打开, 文件映射有效的
     */
    next->runs.front().map_write();

    while (!merge_ctx.done()) {
        entry = merge_ctx.next();

        // Remove deleted keys from the final level
        if (!(next == levels.end() - 1 && entry.val == VAL_TOMBSTONE)) {
            next->runs.front().put(entry);
        }
    }

    /**
     * 取消文件映射, 关闭映射文件
     */
    next->runs.front().unmap();

    for (auto& run : current->runs) {
        run.unmap();
    }

    /*
     * Clear the current level to delete the old (now
     * redundant) entry files.
     * 清空本层的全部run
     */

    current->runs.clear();
}

void LSMTree::put(KEY_t key, VAL_t val) {
    /*
     * Try inserting the key into the buffer
     */

    if (buffer.put(key, val)) {
        return;
    }

    /*
     * If the buffer is full, flush level 0 if necessary
     * to create space
     * 启动一次merge_down过程, 在L1层留出空间供Buffer写入
     */

    merge_down(levels.begin());

    /*
     * Flush the buffer to level 0
     * 在L0队头插入一个新的run（emplace_front()）
     * levels.front().runs.front指的是L0层的第一个run，就是刚插入的元素。
     */

    levels.front().runs.emplace_front(levels.front().max_run_size, bf_bits_per_entry);

    /**
     * 以写模式打开文件映射, 将缓冲区的每个元素都写入到run中，关闭文件映射
     */
    levels.front().runs.front().map_write();

    for (const auto& entry : buffer.entries) {
        levels.front().runs.front().put(entry);
    }

    levels.front().runs.front().unmap();

    /*
     * Empty the buffer and insert the key/value pair
     * 清空缓冲区，重新插入
     */

    buffer.empty();
    /**
     * 这个做法不对, release模式下会出错
     * TODO: 这里需要修改, 这个out不能放在assert里头, 应当修改为:
     * auto put_res = buffer.put(key, val);
     * assert(put_res);
     */
    assert(buffer.put(key, val));
}

Run * LSMTree::get_run(int index) {
    for (const auto& level : levels) {
        if (index < level.runs.size()) {
            return (Run *) &level.runs[index];
        } else {
            index -= level.runs.size();
        }
    }

    return nullptr;
};

void LSMTree::get(KEY_t key) {
    VAL_t *buffer_val;
    VAL_t latest_val;
    int latest_run;
    SpinLock lock;      // 在这里用了自旋锁
    atomic<int> counter;

    /*
     * Search buffer
     * 1. 试图从缓冲区中获取数据
     */

    buffer_val = buffer.get(key);

    /**
     * 1.1 如果数据在缓冲区中, 直接输出就可以, 这里要注意释放返回的value的空间
     */
    if (buffer_val != nullptr) {
        if (*buffer_val != VAL_TOMBSTONE) cout << *buffer_val;
        cout << endl;
        delete buffer_val;
        return;
    }

    /*
     * Search runs
     * 2. 在缓冲区里没有找到, 在runs中查找
     */

    counter = 0;
    latest_run = -1;

    /**
     * 保证了一定是在较新的run中找到的数据
     */
    worker_task search = [&] {
        int current_run;
        Run *run;
        VAL_t *current_val;

        /**
         * 3. 获取这个worker要搜索的run的index, 可以用get_run()获取相应的run
         */
        current_run = counter++;

        if (latest_run >= 0 || (run = get_run(current_run)) == nullptr) {
            // Stop search if we discovered a key in another run, or
            // if there are no more runs to search
            // 3.1 其他线程已经找到了这个键值对, 或者所有run都已经在搜索了, 就不必继续搜索了
            return;
        } else if ((current_val = run->get(key)) == nullptr) {
            // Couldn't find the key in the current run, so we need
            // to keep searching.
            // 3.2 在这个run中没有找到, 重新启动search过程
            search();
        } else {
            // Update val if the run is more recent than the
            // last, then stop searching since there's no need
            // to search later runs.
            // 找到了, 更新数据(latest_run和latest_val)
            lock.lock();
            // 如果其他线程也找到了这个key，就要进行比较了。
            // 只有比本线程更新，才能更新latest_run和latest_val
            if (latest_run < 0 || current_run < latest_run) {
                latest_run = current_run;
                latest_val = *current_val;
            }

            lock.unlock();
            delete current_val;
        }
    };

    /**
     * 使用线程池 做数据搜索
     */
    worker_pool.launch(search);
    worker_pool.wait_all();

    /**
     *
     */
    if (latest_run >= 0 && latest_val != VAL_TOMBSTONE) {
        cout << latest_val;
    }
    cout << endl;
}

void LSMTree::range(KEY_t start, KEY_t end) {
    vector<entry_t> *buffer_range;
    map<int, vector<entry_t> *> ranges;
    SpinLock lock;
    atomic<int> counter;
    MergeContext merge_ctx;
    entry_t entry;

    if (end <= start) {
        cout << endl;
        return;
    } else {
        // Convert to inclusive bound
        end -= 1;
    }

    /*
     * Search buffer
     */

    ranges.insert({0, buffer.range(start, end)});

    /*
     * Search runs
     * 对所有的run都执行range过程...
     */

    counter = 0;

    worker_task search = [&] {
        int current_run;
        Run *run;

        current_run = counter++;

        if ((run = get_run(current_run)) != nullptr) {
            lock.lock();
            // ranges不是线程安全的, 锁insert不就行啦, 都锁了干什么？
            /**
             * auto range_res = run->range(start, end);
             * ranges.insert({current_run + 1, range_res});
             */
            ranges.insert({current_run + 1, run->range(start, end)});
            lock.unlock();

            // Potentially more runs to search.
            // 一个线程可能需要搜索多个run
            search();
        }
    };

    worker_pool.launch(search);
    worker_pool.wait_all();

    /*
     * Merge ranges and print keys
     * 在这里利用MergeCtx, 这个想法很好
     */

    for (const auto& kv : ranges) {
        merge_ctx.add(kv.second->data(), kv.second->size());
    }

    while (!merge_ctx.done()) {
        entry = merge_ctx.next();
        if (entry.val != VAL_TOMBSTONE) {
            cout << entry.key << ":" << entry.val;
            if (!merge_ctx.done()) cout << " ";
        }
    }

    cout << endl;

    /*
     * Cleanup subrange vectors
     * 释放空间
     */

    for (auto& range : ranges) {
        delete range.second;
    }
}

void LSMTree::del(KEY_t key) {
    // put一个坟墓标志
    put(key, VAL_TOMBSTONE);
}

void LSMTree::load(string file_path) {
    ifstream stream;
    entry_t entry;

    /**
     * 打开LSM-Tree文件, 利用put()方法加载所有键值对
     */
    stream.open(file_path, ifstream::binary);

    if (stream.is_open()) {
        while (stream >> entry) {
            put(entry.key, entry.val);
        }
    } else {
        die("Could not locate file '" + file_path + "'.");
    }
}
