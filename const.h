#ifndef CONST_H
#define CONST_H

#define OK           0
#define FAIL         -1
#define UDP_PORT     12529
#define ERLANG_PORT  1337
#define DEFAULT_PORT 8080
#define DEFAULT_ERLANG_PORT 1337

#define EPOLL_MAX_EVENTS     10
#define EPOLL_TIMEOUT        -1
#define EPOLL_SHORT_TIMEOUT 500

#define CLEAN_SOCKETS  0b1
#define CLEAN_EPOLL   0b10

#define MAX_CPU 8
#define MAX_GPU 4
#define MAX_MEM 16384
#define BUFFER_MAX_SIZE 1024

#define CHECKER_QUEUE_USE_TIME          5
#define QUEUE_SLEEP_TIME                2
#define CHECKER_QUEUE_TIME_UNTIL_DELETE 15.0
#define WAIT_TIME_UNTIL_DELETE          15.0

#define HASHMAP_INITIAL_CAP 128

#define streq(a, b) (strcmp((a), (b)) == 0)

typedef enum {
    REQUEST_KIND_RESERVE = 0,
    REQUEST_KIND_RELEASE
} RequestKind;

typedef enum {
    RES_KIND_CPU = 0,
    RES_KIND_MEM,
    RES_KIND_GPU
} ResourceKind;

typedef enum {
    CONN_TYPE_PUBLIC = 0,
    CONN_TYPE_ERL,
    CONN_TYPE_UDP,
    CONN_TYPE_CLIENT_REMOTE,
    CONN_TYPE_CLIENT_ERLANG,
    CONN_TYPE_TIMER,
} ConnType;

#endif // CONST_H
