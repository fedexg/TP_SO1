#include "list.h"
#include <stdlib.h>

ListNode *new_list_node(void *data, ListNode *next, ListCpyFunc cpy);

List list_make(void)
{
    return NULL;
}

void list_free(List l, ListFreeFunc destroy)
{
    ListNode *node = l;
    while (node) {
        l = l->next;
        destroy(node->data);
        free(node);
        node = l;
    }
}

int list_empty(List l)
{
    return l == NULL;
}

List list_append(List l, void *data, ListCpyFunc cpy)
{
    if (list_empty(l))
        return new_list_node(data, NULL, cpy);

    l->next = list_append(l->next, data, cpy);
    return l;
}

ListNode *new_list_node(void *data, ListNode *next, ListCpyFunc cpy)
{
    ListNode *node = malloc(sizeof(ListNode));
    node->data = cpy(data);
    node->next = next;
    return node;
}
