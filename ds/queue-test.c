#include <stdio.h>
#include <stdlib.h>
#include "queue.h"
#include "hashmap.h"

int *cpy_int(int *x)
{
    int *p = malloc(sizeof(int));
    *p = *x;
    return p;
}

void free_int(int *x)
{
    free(x);
}

int main()
{
    Queue q = queue_make();
    int x = 1;
    q = enqueue(q, &x, (QueueCpyFunc)cpy_int);
    x = 2;
    q = enqueue(q, &x, (QueueCpyFunc)cpy_int);
    x = 3;
    q = enqueue(q, &x, (QueueCpyFunc)cpy_int);

    while (!queue_empty(q)) {
        void *pp = queue_head(q);
        int *p = (int *)queue_head(q);
        printf("%d\n", *p);
        q = dequeue(q, (QueueFreeFunc)free_int);
    }

    return 0;
}
