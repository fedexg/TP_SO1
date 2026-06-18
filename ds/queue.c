#include "queue.h"
#include <stdlib.h>

QueueNode *new_node(void *data, QueueNode *next, QueueCpyFunc cpy);

Queue queue_make(void)
{
    return NULL;
}

void queue_free(Queue q, QueueFreeFunc destroy)
{
    QueueNode *node = q;
    while (node) {
        q = q->next;
        destroy(node->data);
        free(node);
        node = q;
    }
}

int queue_empty(Queue q)
{
    return q == NULL;
}

void *queue_head(Queue q)
{
    return q->data;
}

Queue enqueue(Queue q, void *data, QueueCpyFunc cpy)
{
    if (queue_empty(q))
        return new_node(data, NULL, cpy);

    q->next = enqueue(q->next, data, cpy);
    return q;
}

Queue dequeue(Queue q, QueueFreeFunc destroy)
{
    if (queue_empty(q))
        return NULL;

    QueueNode *node = q;
    q = q->next;
    destroy(node->data);
    free(node);

    return q;
}

QueueNode *new_node(void *data, QueueNode *next, QueueCpyFunc cpy)
{
    QueueNode *node = malloc(sizeof(QueueNode));
    node->data = cpy(data);
    node->next = next;
    return node;
}
