#include <cassert>
#include <cstdio>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

#include "run.h"

using namespace std;

Run::Run(long max_size, float bf_bits_per_entry) :
         max_size(max_size),
         bloom_filter(max_size * bf_bits_per_entry)
{
    char *tmp_fn;

    size = 0;
    // 更改vector的容量（capacity），使vector至少可以容纳n个元素
    fence_pointers.reserve(max_size / getpagesize());
    // 对临时文件命名，后面几个X，就加多少随机后缀
    tmp_fn = strdup(TMP_FILE_PATTERN);
    tmp_file = mktemp(tmp_fn);

    mapping = nullptr;
    mapping_fd = -1;
}

Run::~Run(void) {
    assert(mapping == nullptr);
    // 删除临时文件
    remove(tmp_file.c_str());
}

entry_t * Run::map_read(size_t len, off_t offset) {
    assert(mapping == nullptr);

    mapping_length = len;

    mapping_fd = open(tmp_file.c_str(), O_RDONLY);
    assert(mapping_fd != -1);

    mapping = (entry_t *)mmap(0, mapping_length, PROT_READ, MAP_SHARED, mapping_fd, offset);
    assert(mapping != MAP_FAILED);

    return mapping;
}

entry_t * Run::map_read(void) {
    map_read(max_size * sizeof(entry_t), 0);
    return mapping;
}

entry_t * Run::map_write(void) {
    assert(mapping == nullptr);
    int result;

    mapping_length = max_size * sizeof(entry_t);

    mapping_fd = open(tmp_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    assert(mapping_fd != -1);

    // Set the file to the appropriate length
    result = lseek(mapping_fd, mapping_length - 1, SEEK_SET);
    assert(result != -1);
    result = write(mapping_fd, "", 1);
    assert(result != -1);

    mapping = (entry_t *)mmap(0, mapping_length, PROT_WRITE, MAP_SHARED, mapping_fd, 0);
    assert(mapping != MAP_FAILED);

    return mapping;
}

void Run::unmap(void) {
    assert(mapping != nullptr);

    munmap(mapping, mapping_length);
    close(mapping_fd);

    mapping = nullptr;
    mapping_length = 0;
    mapping_fd = -1;
}

VAL_t * Run::get(KEY_t key) {
    vector<KEY_t>::iterator next_page;
    long page_index;
    VAL_t *val;
    int i;

    val = nullptr;

    /**
     * 有点像B+树的查找，如果比当前run最小的还小, 比最大的还大
     * 或者bloom_filter中返回false(对于一个偏是的算法, 返回false一定不在)
     */
    if (key < fence_pointers[0] || key > max_key || !bloom_filter.is_set(key)) {
        return val;
    }

    /**
     * 找到第一个大于key的KEY的迭代器，并计算得到key可能存在的页的位置 page_index
     */
    next_page = upper_bound(fence_pointers.begin(), fence_pointers.end(), key);
    page_index = (next_page - fence_pointers.begin()) - 1;
    assert(page_index >= 0);

    /**
     * 以只读方式打开文件映射
     */
    map_read(getpagesize(), page_index * getpagesize());

    /**
     * 做顺序查找
     */
    for (i = 0; i < getpagesize() / sizeof(entry_t); i++) {
        if (mapping[i].key == key) {
            // 这里是不是可以break啦
            val = new VAL_t;
            *val = mapping[i].val;
        }
    }

    unmap();

    return val;
}

vector<entry_t> * Run::range(KEY_t start, KEY_t end) {
    vector<entry_t> *subrange;
    vector<KEY_t>::iterator next_page;
    long subrange_page_start, subrange_page_end, num_pages, num_entries, i;

    subrange = new vector<entry_t>;

    /**
     * start必须小于等于max_key, end必须大于等于fence_pointers[0](最小的key)
     */
    // If the ranges don't overlap, return an empty vector
    if (start > max_key || fence_pointers[0] > end) {
        return subrange;
    }

    /**
     * 获取起始页 和结束页 , 这里还是用了 upper_bound方法
     * 需要读取的页就是 [subrange_page_start, subrange_page_end]
     * 计算总页数
     */
    if (start < fence_pointers[0]) {
        subrange_page_start = 0;
    } else {
        next_page = upper_bound(fence_pointers.begin(), fence_pointers.end(), start);
        subrange_page_start = (next_page - fence_pointers.begin()) - 1;
    }

    if (end > max_key) {
        subrange_page_end = fence_pointers.size();
    } else {
        next_page = upper_bound(fence_pointers.begin(), fence_pointers.end(), end);
        subrange_page_end = next_page - fence_pointers.begin();
    }

    assert(subrange_page_start < subrange_page_end);

    num_pages = subrange_page_end - subrange_page_start;

    /**
     * 使用map_read()方法打开文件映射，并偏移到subrange_page_start页处
     */
    map_read(num_pages * getpagesize(), subrange_page_start * getpagesize());

    /**
     * 计算总的entry数量 申请空间, 将页上所有的数据写入到subrange中,
     */
    num_entries = num_pages * getpagesize() / sizeof(entry_t);
    subrange->reserve(num_entries);

    for (i = 0; i < num_entries; i++) {
        if (start <= mapping[i].key && mapping[i].key <= end) {
            subrange->push_back(mapping[i]);
        }
    }

    /**
     * 取消文件映射, 关闭映射文件
     */
    unmap();

    return subrange;
}

void Run::put(entry_t entry) {
    /**
     * 断言文件可写，mapping不为空
     * put的顺序必须是key有序的
     */
    assert(size < max_size);

    bloom_filter.set(entry.key);

    if (size % getpagesize() == 0) {
        //
        fence_pointers.push_back(entry.key);
    }

    // Set a final fence pointer to establish an upper
    // bound on the last page range.
    max_key = max(entry.key, max_key);

    mapping[size] = entry;
    size++;
}
