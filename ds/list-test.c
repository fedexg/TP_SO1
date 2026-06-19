#include <stdio.h>
#include <stdlib.h>
#include "list.h"

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
    List l = list_make();
    int x = 1;
    l = list_append(l, &x, (ListCpyFunc)cpy_int);
    x = 2;
    l = list_append(l, &x, (ListCpyFunc)cpy_int);
    x = 3;
    l = list_append(l, &x, (ListCpyFunc)cpy_int);

    for (ListNode *p = l; p != NULL; p = p->next)
        printf("%d\n", *(int *)p->data);

    list_free(l, (ListFreeFunc)free_int);
    return 0;
}
