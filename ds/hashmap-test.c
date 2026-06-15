#include <stdio.h>
#include <stdlib.h>
#include "hashmap.h"

int *cpy_int(int *x)
{
    int *p = malloc(sizeof(int));
    *p = *x;
    return p;
}

int cmp_int(int *x, int *y)
{
    int a = *x;
    int b = *y;
    return a - b;
}

void free_int(int *x)
{
    free(x);
}

unsigned int hash_int(int x)
{
    return 31*x;
}

int main()
{
    Hashmap hm = hashmap_make(256, (CopyFunc)cpy_int, (CmpFunc)cmp_int,
                             (FreeFunc)free_int, (HashFunc)hash_int);
    int x = 1;
    hashmap_put(hm, &x);
    x = 2;
    hashmap_put(hm, &x);

    int *p = hashmap_search(hm, &x);
    if (p)
        printf("Hallado, vale %d\n", *p);
    else
        printf(":(\n");

    hashmap_delete(hm, &x);
    p = hashmap_search(hm, &x);
    if (p)
        printf("Hallado, vale %d\n", *p);
    else
        printf(":(\n");


    hashmap_free(hm);
    return 0;
}
