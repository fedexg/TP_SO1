#ifndef QUEUE_H
#define QUEUE_H

typedef void (*QueueFreeFunc)(void *);
typedef void *(*QueueCpyFunc)(void *);

typedef struct _QueueNode {
    void *data;
    struct _QueueNode *next;
} QueueNode;

typedef QueueNode *Queue;

Queue queue_make(void);
void queue_free(Queue q, QueueFreeFunc destroy);
int queue_empty(Queue q);
void *queue_head(Queue q);
Queue enqueue(Queue q, void *data, QueueCpyFunc cpy);
Queue dequeue(Queue q, QueueFreeFunc destroy);

#endif // QUEUE_H
