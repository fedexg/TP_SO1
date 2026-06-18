#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "ds/hashmap.h"

#define OK         0
#define FAIL       -1
#define PORT       12529

#define EPOLL_MAX_EVENTS 10
#define EPOLL_TIMEOUT -1

#define CLEAN_SOCKETS  0b1
#define CLEAN_EPOLL   0b10

#define MAX_CPU 8
#define MAX_GPU 4
#define MAX_MEM 16384
#define BUFFER_MAX_SIZE 128

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

// SOCK_STREAM -> TCP
// SOCK_DGRAM ->  UDP

// Local node resources
typedef struct _LocalResources {
    int cpu;
    int mem;
    int gpu;
} LocalResources;

// Cell of node_map
typedef struct _NodeMapCell {
    int ip;
    int port;
    LocalResources resources;
    time_t time_when_called;
} NodeMapCell;

// Cell of job_map 
typedef struct _JobMapCell {
    int job_id;
    LocalResources granted_resources;
} JobMapCell;

typedef struct _Request {
    long long job_id;
    RequestKind kind;
    ResourceKind res_kind;
    int amount;       
} Request;

int listen_public_sock, listen_erlang_sock;
int connect_public_sock, connect_erlang_sock;
int epoll_fd, num_fds_ready;
struct epoll_event ev, events[EPOLL_MAX_EVENTS];
Hashmap node_map, job_map;
Queue request_queue;

void error(const char *msg);
int init_sockets(void);
int init_agents_socket(void);
int init_listen_erlang_socket(void);
int init_epoll(void);
int startup(void);
int add_descriptors(void);
int add_descriptor(int fd);
void *epoll_handler(void *arg);
int set_socket_nonblocking(int sock);
void handle_c_agent(int c_agent_fd);
char **split(char *text, char *delimiter, int *len);
Request parse_request(char **request_fields, int n_fields);
void process_request(Request req, int fd);
bool exists_resource(ResourceKind kind, int amount);
void return_resources(LocalResources resources);
void increase_resources(LocalResources resources);
LocalResources get_request_resource(Request req);
void handle_erlang_client(int erlang_client_fd);
int close_epoll(void);
int close_sockets(void);
int cleanup(int flags);

int main()
{
    if (init_sockets() < 0)
        return 1;

    if (init_epoll() < 0)
        return cleanup(CLEAN_SOCKETS);

    if (add_descriptors())
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    if (startup() < 0)
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    pthread_t epoll_thread;
    if (pthread_create(&epoll_thread, NULL, epoll_handler, NULL) < 0)
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    pthread_join(epoll_thread, NULL);

    if (close_epoll() < 0)
        return 1;

    if (close_sockets() < 0)
        return 1;

    return 0;
}

void error(const char *msg)
{
    printf("%s: %s\n", msg, strerror(errno));
}

int init_sockets(void)
{
    if (init_agents_socket() < 0)
        return FAIL;

    if (init_listen_erlang_socket() < 0)
        return FAIL;

    return OK;
}

int init_agents_socket(void)
{
    int yes = 1;
    listen_public_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_public_sock < 0) {
        error("Error intentando iniciar el socket para otros agentes");
        return FAIL;
    }

    // Set agent sockets to be reused
    if (setsockopt(listen_public_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket público para ser reusado");
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_public;
    memset(&sa_public, 0, sizeof(sa_public));
    sa_public.sin_family = AF_INET;
    sa_public.sin_port = htons(PORT);
    sa_public.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listen_public_sock, (struct sockaddr *)&sa_public, sizeof(sa_public)) < 0) {
        error("Error intentando asignar una dirección al socket público");
        return FAIL;
    }

    if (listen(listen_public_sock, 10) < 0) {
        error("Error intentando setear el socket público para escuchar");
        return FAIL;
    }

    return OK;
}

int init_listen_erlang_socket(void)
{
    int yes = 1;
    listen_erlang_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_erlang_sock < 0) {
        error("Error intentando iniciar el socket para Erlang");
        return FAIL;
    }

    // Set erlang sockets to be reused
    if (setsockopt(listen_erlang_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket de Erlang para ser reusado");
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_erlang;
    memset(&sa_erlang, 0, sizeof(sa_erlang));
    sa_erlang.sin_family = AF_INET;
    sa_erlang.sin_port = htons(PORT);
    sa_erlang.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(listen_erlang_sock, (struct sockaddr *)&sa_erlang, sizeof(sa_erlang)) < 0) {
        error("Error intentando asignar una dirección al socket de Erlang");
        return FAIL;
    }

    if (listen(listen_erlang_sock, 10) < 0) {
        error("Error intentando setear el socket Erlang para escuchar");
        return FAIL;
    }

    return OK;
}

int init_epoll(void)
{
    epoll_fd = epoll_create(1);
    if (epoll_fd < 0) {
        error("Error intentando crear la instancia de epoll");
        return FAIL;
    }

    return OK;
}

int startup(void)
{
    assert(0 && "TODO: startup not implemented");
    return OK;
}

int add_descriptors(void)
{
    if (add_descriptor(listen_public_sock) < 0)
        return FAIL;

    if (add_descriptor(listen_erlang_sock) < 0)
        return FAIL;

    return OK;
}

int add_descriptor(int fd)
{
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        error("Error intentando añadir el socket a la instancia de epoll");
        return FAIL;
    }

    return OK;
}

void *epoll_handler(void *arg)
{
    for (;;) {
        num_fds_ready = epoll_wait(epoll_fd, events, EPOLL_MAX_EVENTS, EPOLL_TIMEOUT);
        if (num_fds_ready == -1) {
            error("Error en el bucle de epoll");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < num_fds_ready; ++i) {
            if (events[i].data.fd == listen_public_sock) {
                connect_public_sock = accept(listen_public_sock, NULL, NULL);
                if (connect_public_sock == -1) {
                    error("Error intentando aceptar un agente");
                    exit(EXIT_FAILURE);
                }

                set_socket_nonblocking(connect_public_sock);

                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = connect_public_sock;
                if (add_descriptor(connect_public_sock) < 0)
                    exit(EXIT_FAILURE);
            } else if (events[i].data.fd == listen_erlang_sock) {
                connect_erlang_sock = accept(listen_erlang_sock, NULL, NULL);
                if (connect_erlang_sock == -1) {
                    error("Error intentando aceptar un cliente Erlang");
                    exit(EXIT_FAILURE);
                }

                set_socket_nonblocking(connect_erlang_sock);

                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = connect_erlang_sock;
                if (add_descriptor(connect_erlang_sock) < 0)
                    exit(EXIT_FAILURE);
            } else {
                int client_fd = events[i].data.fd;

                // TODO: resolve possible deadlocks
                if (client_fd == connect_public_sock)
                    handle_c_agent(client_fd);
                else if (client_fd == connect_erlang_sock)
                    handle_erlang_client(client_fd);
            }
        }
    }
}

// https://stackoverflow.com/a/73879155
int set_socket_nonblocking(int sock)
{
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1)
        return FAIL;

    flags |= O_NONBLOCK;
    return fcntl(sock, F_SETFL, flags);
}

void handle_c_agent(int fd)
{
    char buffer[BUFFER_MAX_SIZE];
    memset(buffer, 0, BUFFER_MAX_SIZE);
    ssize_t bytes_read = read(fd, buffer, BUFFER_MAX_SIZE -1);
    if (bytes_read <= 0);
        // TODO (what do we do in case of error?)
    else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);
        Request request = parse_request(request_fields, length); // TODO
        process_request(request, fd); // TODO
    }
}

char **split(char *text, char *delimiter, int *len)
{
    int cap = 32;
    char **result = malloc(sizeof(char *)*cap);

    char *s2 = strdup(text);
    char *tok = strtok(s2, delimiter);
    int i = 0;
    result[i++] = tok;
    while (tok) {
        tok = strtok(NULL, delimiter);
        result[i++] = tok;
    }

    if (len)
        *len = i - 1;
    return result;
}

Request parse_request(char **request_fields, int n_fields)
{
    Request req;
    char *req_kind = request_fields[0];
    if (streq(req_kind, "RESERVE"))
        req.kind = REQUEST_KIND_RESERVE;
    else if (streq(req_kind, "RELEASE"))
        req.kind = REQUEST_KIND_RELEASE;

    req.job_id = atoll(request_fields[1]);
    char *resource_name = request_fields[2];
    if (streq(resource_name, "cpu"))
        req.res_kind = RES_KIND_CPU;
    else if (streq(resource_name, "mem"))
        req.res_kind = RES_KIND_MEM;
    else if (streq(resource_name, "gpu"))
        req.res_kind = RES_KIND_GPU;

    req.amount = atoi(request_fields[3]);

    return req;
}

void process_request(Request req, int fd)
{
    char response_to_agent[128] = {0};
    switch (req.kind) {
    case REQUEST_KIND_RESERVE:
        if (exists_resource(req.res_kind, req.amount)) {
            JobMapCell new_cell;
            new_cell.granted_resources = (LocalResources){0, 0, 0};
            switch (req.res_kind) {
            case RES_KIND_CPU:
                new_cell.granted_resources.cpu += req.amount;
                break;
            case RES_KIND_GPU:
                new_cell.granted_resources.gpu += req.amount;
                break;
            case RES_KIND_MEM:
                new_cell.granted_resources.mem += req.amount;
                break;
            default:
                // lol
                break;
            }

            hashmap_put(job_map, &new_cell);

            return_resources(get_request_resource(req));
            sprintf(response_to_agent, "GRANTED %d", req.job_id);
            send(fd, response_to_agent, 8, 0);
        } else {
            sprintf(response_to_agent, "DENIED %d", req.job_id);
            send(fd, response_to_agent, 8, 0);
            request_queue = enqueue(request_queue, req);
        }
        break;
    case REQUEST_KIND_RELEASE:
        increase_resources(get_request_resource(req.resources));
        JobMapCell new_cell;
        // TODO: modify job hashmap
        break;
    default:
        break;
    }
}

bool exists_resource(ResourceKind kind, int amount)
{
    assert(0 && "TODO: exists_resource not implemented");
    return false;
}

void return_resources(LocalResources resources)
{
    assert(0 && "TODO: return_resources not implemented");
}

void increase_resources(LocalResources resources)
{
    assert(0 && "TODO: increase_resources not implemented");
}

LocalResources get_request_resource(Request req)
{
    LocalResources res = { 0 };
    switch (req.res_kind) {
    case RES_KIND_CPU:
        res.cpu = req.amount;
        break;
    case RES_KIND_MEM:
        res.mem = req.amount;
        break;
    case RES_KIND_GPU:
        res.gpu = req.amount;
        break;
    }

    return res;
}

void handle_erlang_client(int fd)
{
    // TODO
}

int close_epoll(void)
{
    if (close(epoll_fd) < 0) {
        error("Error intentando destruir la instancia de epoll");
        return FAIL;
    }

    return OK;
}

int close_sockets(void)
{
    if (close(listen_erlang_sock) < 0) {
        error("Error intentando cerrar el socket de Erlang");
        return FAIL;
    }

    if (close(listen_public_sock) < 0) {
        error("Error intentando cerrar el socket para los agentes");
        return FAIL;
    }

    return OK;
}

int cleanup(int flags)
{
    if ((flags & 0b1) == CLEAN_SOCKETS)
        if (close_sockets() < 0)
            return 1;

    if ((flags & 0b10) == CLEAN_EPOLL)
        if (close_epoll() < 0)
            return 1;

    return 1;
}
