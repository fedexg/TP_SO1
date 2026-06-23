#include "hashmap.h"
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

int find_index(Hashmap hm, void *data);
void hashmap_resize(Hashmap hm);
void *nocopy(void *data);

Hashmap hashmap_make(unsigned int cap, CopyFunc copy, CmpFunc cmp,
                     FreeFunc free, HashFunc hash)
{
    Hashmap hm = malloc(sizeof(struct _Hashmap));
    assert(hm != NULL);
    hm->items = malloc(sizeof(HashCell)*cap);
    assert(hm->items != NULL);
    hm->num_items = 0;
    hm->cap = cap;
    hm->copy = copy;
    hm->cmp = cmp;
    hm->free = free;
    hm->hash = hash;

    for (int i = 0; i < cap; ++i) {
        hm->items[i].data = NULL;
        hm->items[i].deleted = 0;
    }

    pthread_mutex_init(&hm->mutex, NULL);
    return hm;
}

void hashmap_free(Hashmap hm)
{
    for (int i = 0; i < hm->cap; ++i)
        if (hm->items[i].data != NULL && !hm->items[i].deleted)
            hm->free(hm->items[i].data);

    pthread_mutex_destroy(&hm->mutex);
    free(hm->items);
    free(hm);
}

void hashmap_put(Hashmap hm, void *data)
{
    pthread_mutex_lock(&hm->mutex);
    int idx = find_index(hm, data);

    if (idx != -1) {
	pthread_mutex_unlock(&hm->mutex);
        return;
    }

    idx = hm->hash(data) % hm->cap;
    int i = idx;
    int free_cell = 0;

    do {
        free_cell = (hm->items[i].data == NULL || hm->items[i].deleted);
        i = (i + 1) % hm->cap;
    } while (i != idx && !free_cell);

    if (!free_cell) {
	pthread_mutex_unlock(&hm->mutex);
        return;
    }

    int index = (i > 0)? (i - 1) : 0;
    hm->items[index].data = hm->copy(data);
    hm->items[index].deleted = 0;
    ++hm->num_items;

    float balance_factor = (float)hm->num_items/hm->cap;
    if (balance_factor >= 0.75f)
        hashmap_resize(hm);
    pthread_mutex_unlock(&hm->mutex);
}

void *hashmap_search(Hashmap hm, void *data)
{
    int idx = find_index(hm, data);
    return (idx != -1)? hm->items[idx].data : NULL;
}

void hashmap_delete(Hashmap hm, void *data)
{
    pthread_mutex_lock(&hm->mutex);
    int idx = find_index(hm, data);

    if (idx != -1) {
        hm->free(hm->items[idx].data);
        hm->items[idx].data = NULL;
        hm->items[idx].deleted = 1;
        --hm->num_items;
    }
    pthread_mutex_unlock(&hm->mutex);
}

int find_index(Hashmap hm, void *data)
{
    int idx = hm->hash(data) % hm->cap;
    int i = idx;
    int cell_found = 0;

    do {
        cell_found = (hm->items[i].data != NULL &&
                     !hm->items[i].deleted &&
                     hm->cmp(hm->items[i].data, data) == 0);
        i = (i + 1) % hm->cap;
    } while (i != idx && !cell_found);

    return cell_found? (i - 1) : -1;
}

void hashmap_resize(Hashmap hm)
{
    unsigned int old_cap = hm->cap;
    HashCell *old_hm = hm->items;
    CopyFunc old_copy = hm->copy;

    hm->cap *= 2;
    hm->items = malloc(sizeof(HashCell)*hm->cap);
    assert(hm->items != NULL);
    for (int i = 0; i < hm->cap; ++i) {
        hm->items[i].data = NULL;
        hm->items[i].deleted = 0;
    }

    hm->copy = nocopy;
    for (int i = 0; i < old_cap; ++i)
        if (old_hm[i].data != NULL && !old_hm[i].deleted)
            hashmap_put(hm, old_hm[i].data);

    hm->copy = old_copy;
    free(old_hm);
}

void *nocopy(void *data)
{
    return data;
}
