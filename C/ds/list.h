#ifndef LIST_H
#define LIST_H

typedef void (*ListFreeFunc)(void *);
typedef void *(*ListCpyFunc)(void *);

typedef struct _ListNode {
    void *data;
    struct _ListNode *next;
} ListNode;

typedef ListNode *List;

List list_make(void);
void list_free(List l, ListFreeFunc destroy);
int list_empty(List l);
List list_append(List l, void *data, ListCpyFunc cpy);

#endif // QUEUE_H
