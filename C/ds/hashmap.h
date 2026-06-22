#ifndef __HASHMAP_H__
#define __HASHMAP_H__

typedef void *(*CopyFunc)(void *data);
typedef int (*CmpFunc)(void *data1, void *data2);
typedef void (*FreeFunc)(void *data);
typedef unsigned int (*HashFunc)(void *data);

typedef struct HashCell {
    void *data;
    int deleted;
} HashCell;

struct _Hashmap {
    HashCell *items;
    unsigned int num_items;
    unsigned int cap;
    CopyFunc copy;
    CmpFunc cmp;
    FreeFunc free;
    HashFunc hash;
};

typedef struct _Hashmap *Hashmap;

Hashmap hashmap_make(unsigned int cap, CopyFunc copy, CmpFunc cmp,
                     FreeFunc free, HashFunc hash);
void hashmap_free(Hashmap hm);
void hashmap_put(Hashmap hm, void *data);
void *hashmap_search(Hashmap hm, void *data);
void hashmap_delete(Hashmap hm, void *data);

#endif /* __HASHMAP_H__ */
