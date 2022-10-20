#include <vector>

#include "types.h"
#include "bloom_filter.h"

#define TMP_FILE_PATTERN "/tmp/lsm-XXXXXX"

using namespace std;

class Run {
    // 一个Run 包含 BloomFilter,
    BloomFilter bloom_filter;
    // 索引所有写入的Key
    vector<KEY_t> fence_pointers;
    // 最大的key
    KEY_t max_key;
    // 文件映射
    entry_t *mapping;
    size_t mapping_length;
    int mapping_fd;
    long file_size() {return max_size * sizeof(entry_t);}
public:
    long size, max_size;
    string tmp_file;
    Run(long, float);
    ~Run(void);

    // 以下几个methods用于文件映射管理
    entry_t * map_read(size_t, off_t);

    entry_t * map_read(void);

    entry_t * map_write(void);

    void unmap(void);

    // 向外提供的API, get和range方法应该提供相应的 空间释放方法,
    // 因为是在堆上申请的空间 (谁申请谁释放)
    VAL_t * get(KEY_t);
    vector<entry_t> * range(KEY_t, KEY_t);
    void put(entry_t);
};
