#include <cassert>
#include <iostream>

#include "merge.h"

void MergeContext::add(entry_t *entries, long num_entries) {
    merge_entry_t merge_entry;

    if (num_entries > 0) {
        merge_entry.entries = entries;
        merge_entry.num_entries = num_entries;
        merge_entry.precedence = queue.size();
        queue.push(merge_entry);
    }
}

entry_t MergeContext::next(void) {
    merge_entry_t current, next;
    entry_t entry;

    current = queue.top();
    next = current;

    // Only release the most recent value for a given key
    while (next.head().key == current.head().key && !queue.empty()) {
        // 先pop出来, 因为存的是元素，不是指针, 因此要采用这种方法
        queue.pop();

        // 修改next.current, 因为当前位置的元素将要被添加到合并后的run中
        next.current_index++;
        // 如果序列中还有元素, 再push进去
        if (!next.done()) queue.push(next);

        // 重新比较, 获取优先级最高的元素.
        next = queue.top();
    }

    return current.head();
}

bool MergeContext::done(void) {
    return queue.empty();
}
