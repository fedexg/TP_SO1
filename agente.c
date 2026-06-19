// TODO LIST
// - deadlock prevention (timeouts, timerfd)
// - handle enqueued requests when REQUEST_KIND_RELEASE (tener una cola que guarde
//   requests completas, fijarnos si hay recursos para el request y fijarse si
//   grant o deny)

#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <time.h>

#include "ds/hashmap.h"
#include "ds/queue.h"

#define OK           0
#define FAIL         -1
#define UDP_PORT     12529
#define ERLANG_PORT  1337
#define DEFAULT_PORT 8080

#define EPOLL_MAX_EVENTS     10
#define EPOLL_TIMEOUT        -1
#define EPOLL_SHORT_TIMEOUT 500

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
    int current_cpu;
    int current_mem;
    int current_gpu;

    int cpu;
    int mem;
    int gpu;
} LocalResources;

// Represents the allocated resources in a remote node
typedef struct _RemoteAllocation {
    char ip[16];
    int port;
    LocalResources resources;
} RemoteAllocation;

// Cell of node_map
typedef struct _NodeMapCell {
    char *ip;
    int port;
    int socket_fd;
    LocalResources resources;
    time_t time_when_called;
} NodeMapCell;

// Cell of job_map 
typedef struct _JobMapCell {
    long long job_id;
    int num_remotely_allocated;
    RemoteAllocation *remote_allocations; // array of all the remotely allocated resources
    LocalResources granted_resources;
} JobMapCell;

typedef struct JobQueueNode {
    JobMapCell job_cell;
    time_t time_when_alloc;
} JobQueueNode;

typedef struct _Request {
    long long job_id;
    RequestKind kind;
    ResourceKind res_kind;
    int amount;       
} Request;

int listen_public_sock, listen_erlang_sock;
int connect_public_sock, connect_erlang_sock;
int udp_broadcast_sock;
int timer_fd;
int epoll_fd, num_fds_ready;
int agent_port;
static int seconds_passed = 0;
struct epoll_event ev, events[EPOLL_MAX_EVENTS];
int cluster_cpu, cluster_mem, cluster_gpu;
LocalResources node_resources;
Hashmap node_map, job_map;
Queue job_queue;
List timed_out_jobs;

// Job queue information
JobQueueNode *job_copy(JobQueueNode *j);
void job_free(JobQueueNode *j);

// General functions
void sigpipe_handler(int s);
void error(const char *msg);
int init_sockets(void);
int init_agents_socket(void);
int init_listen_erlang_socket(void);
int init_udp_socket(void);
int init_timer(void);
int init_epoll(void);
int startup(void);
void send_udp_announce(void);
int get_local_ip(char *ip_buffer, int buffer_size);
int add_descriptors(void);
int add_descriptor(int fd);
void *epoll_handler(void *arg);
int set_socket_nonblocking(int sock);
void handle_c_agent(int c_agent_fd);
void handle_unexpected_disconnection(int fd);
char **split(char *text, char *delimiter, int *len);
Request parse_request(char **request_fields, int n_fields);
void process_request(Request req, int fd);
bool exists_resource(ResourceKind kind, int amount);
void give_resources(ResourceKind kind, int amount);
void increase_resources(ResourceKind kind, int amount);
Request *request_dup(Request *req);
void handle_erlang_client(int erlang_client_fd);
void handle_job_request(char **request_fields, int request_fields_size, int fd);
NodeMapCell *parse_erlang_request(char **request_fields, int request_fields_size, int fd, int *len);
void find_requested_parameters(char **request_fields, int request_fields_size, char *resource, int *amount);
void handle_job_release(char **request_fields, int fd);
void handle_job_status(char **request_fields, int fd);
void handle_get_nodes(char **request_fields, int fd);
void send_release_to_agent(const char *agent_ip, long long job_id, const char *resource, int amount);
int get_agent_connection(NodeMapCell *node);
void check_agent_expiration_time(void);
void handle_udp_packet(int udp_fd);
int close_epoll(void);
int close_sockets(void);
int cleanup(int flags);
LocalResources get_initial_resources(void);

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2)
        agent_port = DEFAULT_PORT;
    else
        agent_port = atoi(argv[1]);

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

JobQueueNode *job_copy(JobQueueNode *j)
{
    long long job_id = j->job_cell.job_id;
    int num_remotely_allocated = j->job_cell.num_remotely_allocated;
    RemoteAllocation *remote_allocations = j->job_cell.remote_allocations;
    time_t time_when_alloc = j->time_when_alloc;

    JobQueueNode *cloned = malloc(sizeof(JobQueueNode));
    cloned->job_cell.job_id = job_id;
    cloned->job_cell.num_remotely_allocated = num_remotely_allocated;
    cloned->job_cell.remote_allocations = calloc(num_remotely_allocated, sizeof(RemoteAllocation));
    memcpy(cloned->job_cell.remote_allocations, remote_allocations, num_remotely_allocated);
    cloned->time_when_alloc = time_when_alloc;
    return cloned;
}

void job_free(JobQueueNode *j)
{
    free(j->job_cell.remote_allocations);
    free(j);
}

void error(const char *msg)
{
    printf("%s: %s\n", msg, strerror(errno));
}

// Initialize c agent and erlang sockets
int init_sockets(void)
{
    if (init_agents_socket() < 0)
        return FAIL;

    if (init_listen_erlang_socket() < 0)
        return FAIL;

    if (init_udp_socket() < 0)
        return FAIL;

    return OK;
}

// Initializes the sockets used by the c agent
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
        close(listen_public_sock);
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_public;
    memset(&sa_public, 0, sizeof(sa_public));
    sa_public.sin_family = AF_INET;
    sa_public.sin_port = htons(agent_port);
    sa_public.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listen_public_sock, (struct sockaddr *)&sa_public, sizeof(sa_public)) < 0) {
        error("Error intentando asignar una dirección al socket público");
        close(listen_public_sock);
        return FAIL;
    }

    if (listen(listen_public_sock, 10) < 0) {
        error("Error intentando setear el socket público para escuchar");
        close(listen_public_sock);
        return FAIL;
    }

    return OK;
}

// Initializes the erlang planner socket
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
        close(listen_erlang_sock);
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_erlang;
    memset(&sa_erlang, 0, sizeof(sa_erlang));
    sa_erlang.sin_family = AF_INET;
    sa_erlang.sin_port = htons(ERLANG_PORT);
    sa_erlang.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(listen_erlang_sock, (struct sockaddr *)&sa_erlang, sizeof(sa_erlang)) < 0) {
        error("Error intentando asignar una dirección al socket de Erlang");
        close(listen_erlang_sock);
        return FAIL;
    }

    if (listen(listen_erlang_sock, 10) < 0) {
        error("Error intentando setear el socket Erlang para escuchar");
        close(listen_erlang_sock);
        return FAIL;
    }

    return OK;
}

int init_udp_socket(void)
{
    int yes = 1;
    udp_broadcast_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_broadcast_sock < 0) {
        error("Error intentando iniciar el socket UDP");
        return FAIL;
    }

    // Give permission to be a broadcast socket
    if (setsockopt(udp_broadcast_sock, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket de UDP para permitir broadcast");
        return FAIL;
    }

    // Bind to any address found
    struct sockaddr_in sa_udp;
    memset(&sa_udp, 0, sizeof(sa_udp));
    sa_udp.sin_family = AF_INET;
    sa_udp.sin_port = htons(UDP_PORT);
    sa_udp.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(udp_broadcast_sock, (struct sockaddr *)&sa_udp, sizeof(sa_udp)) < 0) {
        error("Error intentando asignar una dirección al broadcast");
        close(udp_broadcast_sock);
        return FAIL;
    }

    set_socket_nonblocking(udp_broadcast_sock);
    return OK;
}

int init_timer(void)
{
    timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timer_fd < 0) {
        error("Error intentando crear el temporizador");
        return FAIL;
    }

    struct itimerspec expiration;

    // First-time expiration check
    expiration.it_value.tv_sec = 5;
    expiration.it_value.tv_nsec = 0;

    // Periodic expiration check
    expiration.it_interval.tv_sec = 5;
    expiration.it_value.tv_nsec = 0;

    if (timerfd_settime(timer_fd, 0, &expiration, NULL) < 0) {
        error("Error intentando configurar el temporizador");
        close(timer_fd);
        return FAIL;
    }

    if (add_descriptor(timer_fd) < 0) {
        close(timer_fd);
        return FAIL;
    }

    return OK;
}

// Initializes the epoll instance
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
    send_udp_announce();
    struct epoll_event startup_events[EPOLL_MAX_EVENTS];
    time_t start_time = time(NULL);

    if (time(NULL) - start_time < 2) {
        int ready = epoll_wait(epoll_fd, startup_events,
                               EPOLL_MAX_EVENTS, EPOLL_SHORT_TIMEOUT);
        for (int i = 0; i < ready; ++i) {
            if (startup_events[i].data.fd == udp_broadcast_sock)
                handle_udp_packet(udp_broadcast_sock);
        }
    }

    return OK;
}

void send_udp_announce(void)
{
    struct sockaddr_in sa_broadcast;
    memset(&sa_broadcast, 0, sizeof(sa_broadcast));
    sa_broadcast.sin_family = AF_INET;
    sa_broadcast.sin_port = htons(agent_port);
    sa_broadcast.sin_addr.s_addr = htonl(INADDR_BROADCAST);

    char my_ip[64];
    if (get_local_ip(my_ip, 64) < 0)
        strcpy(my_ip, "127.0.0.1");

    char buffer[BUFFER_MAX_SIZE] = { 0 };
    sprintf(buffer, "ANNOUNCE %s %d cpu:%d mem:%d gpu:%d",
            my_ip, agent_port,
            node_resources.cpu,
            node_resources.mem,
            node_resources.gpu);

    // Because this is a UDP socket, we use sendto
    ssize_t bytes = sendto(udp_broadcast_sock, buffer, strlen(buffer), 0,
                            (struct sockaddr *)&sa_broadcast,
                            sizeof(sa_broadcast));

    if (bytes < 0)
        error("Error intentando enviar ANNOUNCE");
}

int get_local_ip(char *ip_buffer, int buffer_size)
{
    struct ifaddrs *interfaces = NULL;
    struct ifaddrs *p = NULL;
    if (getifaddrs(&interfaces) < 0) {
        error("Error intentando conseguir IPs");
        return FAIL;
    }

    for (p = interfaces; p != NULL; p = p->ifa_next) {
        if (p->ifa_addr == NULL)
            continue;

        // Search for IPv4
        if (p->ifa_addr->sa_family == AF_INET)
            // Check if up and not localhost
            if ((p->ifa_flags & IFF_UP) && !(p->ifa_flags & IFF_LOOPBACK)) {
                struct sockaddr_in *sa = (struct sockaddr_in *)p->ifa_addr;
                if (inet_ntop(AF_INET, &(sa->sin_addr), ip_buffer, buffer_size) != NULL)
                    break;
            }
    }

    freeifaddrs(interfaces);
    return OK;
}

int add_descriptors(void)
{
    if (add_descriptor(listen_public_sock) < 0)
        return FAIL;

    if (add_descriptor(listen_erlang_sock) < 0)
        return FAIL;

    if (add_descriptor(udp_broadcast_sock) < 0)
        return FAIL;

    return OK;
}

// Adds the corresponding file descriptor to the epoll instance
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

// Handles incoming epoll events
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
            } else if (events[i].data.fd == udp_broadcast_sock)
                handle_udp_packet(events[i].data.fd);
            else if (events[i].data.fd == timer_fd) {
                long long expirations = 0;
                read(timer_fd, &expirations, sizeof(long long));
                check_agent_expiration_time();
            } else {
                int client_fd = events[i].data.fd;

                // TODO: resolve possible deadlocks
                if (client_fd == connect_public_sock)
                    handle_c_agent(client_fd);
                else if (client_fd == connect_erlang_sock)
                    handle_erlang_client(client_fd);
                else {
                    char response[BUFFER_MAX_SIZE] = { 0 };
                    ssize_t bytes = read(client_fd, response, sizeof(response) - 1);
                    if (bytes <= 0) {
                        close(client_fd);
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
                    }
                }
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
    ssize_t bytes_read = read(fd, buffer, BUFFER_MAX_SIZE - 1);
    if (bytes_read <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        error("Error intentando leer de un agente de C");

        handle_unexpected_disconnection(fd);

        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
    } else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);
        Request request = parse_request(request_fields, length);
        process_request(request, fd);
        free(request_fields);
    }
}

void handle_unexpected_disconnection(int fd)
{
    NodeMapCell *dead_node = NULL;

    // 1. Buscar qué nodo en el node_map coincide con el fd_muerto
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL && !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            if (node->socket_fd == fd) {
                dead_node = node;
                break;
            }
        }
    }

    if (dead_node == NULL)
        return;

    // 2. Recorrer el job_map para limpiar los trabajos afectados
    for (int i = 0; i < job_map->cap; ++i) {
        bool exists_job = job_map->items[i].data != NULL && !job_map->items[i].deleted;
        if (exists_job) {
            JobMapCell *job = (JobMapCell *)job_map->items[i].data;
            bool affected_job = false;

            // Primero vemos si este Job usaba al nodo que se murió
            for (int j = 0; j < job->num_remotely_allocated; ++j) {
                RemoteAllocation *remote = &job->remote_allocations[j];
                if (streq(remote->ip, dead_node->ip) &&
                        ((remote->resources.current_cpu > 0) ||
                        (remote->resources.current_mem > 0)  ||
                        (remote->resources.current_gpu > 0))) {
                    affected_job = true;
                    break;
                }
            }

            // Si el Job dependía del nodo muerto, el Job fracasa.
            // Debemos devolverle los recursos a todos los DEMÁS nodos que aportaron a este Job.
            if (affected_job) {
                // A) DEVOLVER A OTROS NODOS REMOTOS
                for (int j = 0; j < job->num_remotely_allocated; ++j) {
                    RemoteAllocation *remote = &job->remote_allocations[j];

                    // Si es el nodo muerto, no le devolvemos nada.
                    // Pero si es OTRO nodo remoto, le reintegramos sus recursos en el node_map
                    if (!streq(remote->ip, dead_node->ip)) {
                        // Buscamos al nodo sobreviviente en el node_map para sumarle sus recursos
                        NodeMapCell *alive_node = hashmap_search(node_map, &remote->ip);
                        if (alive_node != NULL) {
                            alive_node->resources.current_cpu += remote->resources.current_cpu;
                            alive_node->resources.current_mem += remote->resources.current_mem;
                            alive_node->resources.current_gpu += remote->resources.current_gpu;
                        }
                    }

                    // Limpiamos la asignación remota del Job
                    memset(&remote->resources, 0, sizeof(LocalResources));
                }

                if (job->granted_resources.current_cpu > 0)
                    give_resources(RES_KIND_CPU, job->granted_resources.current_cpu);
                if (job->granted_resources.current_mem > 0)
                    give_resources(RES_KIND_MEM, job->granted_resources.current_mem);
                if (job->granted_resources.current_gpu > 0)
                    give_resources(RES_KIND_GPU, job->granted_resources.current_gpu);

                // C) BORRAR EL JOB AFECTADO
                long long id_a_borrar = job->job_id;
                free(job->remote_allocations);
                hashmap_delete(job_map, &id_a_borrar);
            }
        }
    }

    // 3. Finalmente, borramos al nodo muerto del mapa de nodos y liberamos su memoria
    hashmap_delete(node_map, &dead_node->ip);
    free(dead_node);
}

// Given a string text and a delimiter, splits the string and stores in len the length of the char** it returns
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

// Given request_fields, returns a request type datastructure.
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

// Given a request and a file descriptor, processes the agent c request.
void process_request(Request req, int fd)
{
    char response_to_agent[128] = {0};
    switch (req.kind) {
    case REQUEST_KIND_RESERVE:
        if (exists_resource(req.res_kind, req.amount)) {
            JobMapCell *cell = hashmap_search(job_map, &req.job_id);

            // We didn't find a cell with this job id. We create it.
            if (cell == NULL)
                cell = calloc(1, sizeof(JobMapCell));

            // 1001 {current_cpu 1, current_mem 0, current_gpu 0}
            // RESERVE 1001 current_gpu 1 (r)
            // cell = r
            // Request {REQUEST_KIND_RESERVE, <job_id>, RES_KIND_GPU, 1};
            // cell->granted_resources.current_gpu += 1
            // hashmap_put(hm, cell)
            // 1001 {current_cpu 1, current_mem 0, current_gpu 1}
            switch (req.res_kind) {
            case RES_KIND_CPU:
                cell->granted_resources.current_cpu += req.amount;
                break;
            case RES_KIND_GPU:
                cell->granted_resources.current_gpu += req.amount;
                break;
            case RES_KIND_MEM:
                cell->granted_resources.current_mem += req.amount;
                break;
            default:
                // lol
                break;
            }

            hashmap_put(job_map, &cell);
            give_resources(req.res_kind, req.amount);
            sprintf(response_to_agent, "GRANTED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
        } else {
            sprintf(response_to_agent, "DENIED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
        }

        break;

    case REQUEST_KIND_RELEASE:
        increase_resources(req.res_kind, req.amount);
        JobMapCell *cell = hashmap_search(job_map, &req.job_id);

        // Cell not found, therefore no job with that id was found.
        if (cell == NULL) {
            sprintf(response_to_agent, "DENIED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
            return;
        }

        switch (req.res_kind) {
        case RES_KIND_CPU:
            cell->granted_resources.current_cpu -= req.amount;
            break;
        case RES_KIND_GPU:
            cell->granted_resources.current_gpu -= req.amount;
            break;
        case RES_KIND_MEM:
            cell->granted_resources.current_mem -= req.amount;
            break;
        default:
            break;
        }

        if (cell->granted_resources.current_cpu == 0 && cell->granted_resources.current_gpu == 0 &&
            cell->granted_resources.current_mem) {
            hashmap_delete(job_map,&req.job_id);
            free(cell);
        } else
            hashmap_put(job_map, &cell);

        break;
    default:
        break;
    }
}

// Returns true if there are enough resources requested in the local node
bool exists_resource(ResourceKind kind, int amount)
{
    switch (kind) {
    case RES_KIND_CPU:
        if (node_resources.current_cpu >= amount)
            return true;
        break;
    case RES_KIND_MEM:
        if (node_resources.current_mem >= amount)
            return true;
        break;
    case RES_KIND_GPU:
        if (node_resources.current_gpu >= amount)
            return true;
        break;
    default:
        break;
    }

    return false;
}

// Decreases a node resource (which is specified by kind) by a given amount
void give_resources(ResourceKind kind, int amount)
{
    switch (kind) {
    case RES_KIND_CPU:
        node_resources.current_cpu -= amount;
        break;
    case RES_KIND_MEM:
        node_resources.current_mem -= amount;
        break;
    case RES_KIND_GPU:
        node_resources.current_gpu -= amount;
        break;
    default:
        break;
    }
}

// Increases a node resource (which is specified by kind) by a given amount
void increase_resources(ResourceKind kind, int amount)
{
    switch (kind) {
    case RES_KIND_CPU:
        node_resources.current_cpu += amount;
        break;
    case RES_KIND_MEM:
        node_resources.current_mem += amount;
        break;
    case RES_KIND_GPU:
        node_resources.current_gpu += amount;
        break;
    default:
        break;
    }
}

Request *request_dup(Request *req)
{
    Request *cloned = malloc(sizeof(Request));
    cloned->job_id = req->job_id;
    cloned->kind = req->kind;
    cloned->res_kind = req->res_kind;
    cloned->amount = req->amount;

    return cloned;
}

// Handles an erlang client connection
void handle_erlang_client(int fd)
{
    char buffer[BUFFER_MAX_SIZE] = { 0 };
    ssize_t bytes_read = read(fd, buffer, BUFFER_MAX_SIZE - 1);
    if (bytes_read <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        error("Error intentando leer de un agente de C");
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
    } else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);

        if (streq(request_fields[0], "JOB_REQUEST"))
            handle_job_request(request_fields, length, fd);
        else if (streq(request_fields[0], "JOB_RELEASE"))
            handle_job_release(request_fields, fd);
        else if (streq(request_fields[0], "JOB_STATUS"))
            handle_job_status(request_fields, fd);
        else if (streq(request_fields[0], "GET_NODES"))
            handle_get_nodes(request_fields, fd);
        else {
            char *msg = "Error: comando desconocido";
            send(fd, msg, strlen(msg), 0);
        }

        free(request_fields);
    }
}

// Handles an incoming job request from the erlang client
void handle_job_request(char **request_fields, int request_fields_size, int fd)
{
    // ["JOB_REQUEST" "1001" "@host:res:amount" "@host:res:amount"]

    char msg[BUFFER_MAX_SIZE] = { 0 };
    long long job_id = atoll(request_fields[1]);
    int len = 0;
    NodeMapCell *parsed = parse_erlang_request(request_fields, request_fields_size, fd, &len);
    LocalResources res = get_initial_resources();

    int cpu = 0, mem = 0, gpu = 0;

    // Gather total resources from request to determine response
    for (int i = 0; i < len; ++i) {
        char **resource_fields = split(request_fields[i+2], ":", NULL);

        char resource[4] = { 0 };
        strcpy(resource, resource_fields[1]);
        int amount = atoi(resource_fields[2]);

        if (streq(resource, "cpu"))
            cpu += amount;
        if (streq(resource, "mem"))
            mem += amount;
        if (streq(resource, "gpu"))
            gpu += amount;

        free(resource_fields);
    }

    // Total amount of resources exceeds any resource that the
    // cluster had in the first place. Therefore, job is impossible
    // and we answer a TIMEOUT
    if (cpu > res.cpu || mem > res.mem || gpu > res.gpu) {
        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_TIMEOUT %lld", job_id);
        send(fd, msg, strlen(msg), 0);
        return;
    }

    if (cpu > res.current_cpu || mem > res.current_mem || gpu > res.current_gpu) {
        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_DENIED %lld", job_id);
        send(fd, msg, strlen(msg), 0);

        JobQueueNode job = {
            (JobMapCell){ job_id, 0, NULL, (LocalResources){ 0 } },
            time(NULL)
        };

        enqueue(job_queue, &job, (QueueCpyFunc)job_copy);
        return;
    }

    // Ask agents for what we need
    for (int i = 0; i < len; ++i) {
        int agent_fd = get_agent_connection(&parsed[i]);
        if (agent_fd == -1) {
            error("Error intentando conectarse a un nodo");
            return;
        }

        char msg[BUFFER_MAX_SIZE] = { 0 };

        // Find resource and amount
        char resource[4] = { 0 };
        int amount = 0;

        // Example:
        // {
        //  @host:cpu:1
        //  {
        //      @host
        //      cpu      [1]
        //      1        [2]
        //  }
        //  @host:gpu:2
        //  {
        //      @host
        //      gpu      [1]
        //      2        [2]
        //  }
        //  @host:mem:4096
        //  {
        //      @host
        //      mem      [1]
        //      4096     [2]
        //  }
        // }

        char **resource_fields = split(request_fields[i+2], ":", NULL);
        strcpy(resource, resource_fields[1]);
        amount = atoi(resource_fields[2]);

        // Send RESERVE message based on what we found
        sprintf(msg, "RESERVE %lld %s %d", job_id, resource, amount);
        send(agent_fd, msg, strlen(msg), 0);
        free(resource_fields);
    }

    memset(msg, 0, BUFFER_MAX_SIZE - 1);
    sprintf(msg, "JOB_GRANTEED %lld", job_id);
    send(fd, msg, strlen(msg), 0);

    free(parsed);
}

// Parses a request with the form JOB_REQUEST <job_id> [@ip:res:amount ... ]
// Returns the nodes that it will ask resources from
NodeMapCell *parse_erlang_request(char **request_fields, int request_fields_size, int fd, int *len)
{
    int cap = 256;
    NodeMapCell *nodes = calloc(cap, sizeof(NodeMapCell));
    int nodes_len = 0;

    // Read starting from the array of nodes
    for (int i = 2; i < request_fields_size; ++i) {
        char **node_information = split(request_fields[i], ":", NULL);
        nodes[nodes_len].ip = strdup(node_information[0] + 1);
        nodes[nodes_len].port = agent_port;
        nodes[nodes_len].socket_fd = fd;

        if (streq(node_information[1], "cpu"))
            nodes[nodes_len].resources.current_cpu = atoi(node_information[2]);
        else if (streq(node_information[1], "mem"))
            nodes[nodes_len].resources.current_mem = atoi(node_information[2]);
        else if (streq(node_information[1], "gpu"))
            nodes[nodes_len].resources.current_gpu = atoi(node_information[2]);

        free(node_information);
        ++nodes_len;

        if (nodes_len >= cap/2) {
            cap *= 2;
            nodes = realloc(nodes, cap*sizeof(NodeMapCell));
        }
    }

    *len = nodes_len;
    return nodes;
}

// Handles an incoming job release from the erlang client
void handle_job_release(char **request_fields, int fd)
{
    long long job_id = atoll(request_fields[1]);
    JobMapCell *cell = hashmap_search(job_map, &job_id);
    if (cell == NULL) {
        char error_buf[1024] = { 0 };
        sprintf(error_buf, "Error: no existe el job con id %lld", job_id);
        send(fd, error_buf, strlen(error_buf), 0);
        return;
    }

    increase_resources(RES_KIND_CPU, cell->granted_resources.current_cpu);
    increase_resources(RES_KIND_GPU, cell->granted_resources.current_gpu);
    increase_resources(RES_KIND_MEM, cell->granted_resources.current_mem);

    // Iterate over remotely allocated resources
    for (int i = 0; i < cell->num_remotely_allocated; i++) {
        RemoteAllocation remote = cell->remote_allocations[i];
        if (remote.resources.current_cpu > 0)
            send_release_to_agent(remote.ip, job_id, "cpu", remote.resources.current_cpu);
        if (remote.resources.current_gpu > 0)
            send_release_to_agent(remote.ip, job_id, "gpu", remote.resources.current_gpu);
        if (remote.resources.current_mem > 0)
            send_release_to_agent(remote.ip, job_id, "mem", remote.resources.current_mem);
    }

    free(cell->remote_allocations);
    hashmap_delete(job_map, &job_id);
}

// Handles an incoming job status from the erlang client
void handle_job_status(char **request_fields, int fd)
{
    long long job_id = atoll(request_fields[1]);
    JobMapCell *job = hashmap_search(job_map, &job_id);
    char buffer[BUFFER_MAX_SIZE] = { 0 };

    if (job != NULL) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_GRANTED %lld", job_id);
    } else if (find_in_queue(job_queue, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_DENIED %lld", job_id);
    } else if (find_in_list(timed_out_jobs, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_TIMEOUT %lld", job_id);
        delete_from_timed_out_jobs(job_id);
    }

    send(fd, buffer, strlen(buffer), 0);
}

// Handles an incoming get nodes from the erlang client
void handle_get_nodes(char **request_fields, int fd)
{
    // Format: NODES IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3;IP:PORT:cpu:NUM4:mem:NUM5:gpu:NUM6; ...
    int buffer_cap = 1024;
    char *buffer = calloc(buffer_cap, sizeof(char));
    strcpy(buffer, "NODES ");

    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            char node_buffer[1024] = { 0 };
            snprintf(node_buffer, sizeof(node_buffer), "%s:%d:cpu:%d:mem:%d:gpu:%d;",
                     node->ip,
                     node->port,
                     node->resources.cpu,
                     node->resources.mem,
                     node->resources.gpu);

            // If the new string won't fit, grow the buffer dynamically
            while (strlen(buffer) + strlen(node_buffer) >= buffer_cap) {
                buffer_cap *= 2;
                char *new_buffer = realloc(buffer, buffer_cap);
                buffer = new_buffer;
            }

            // Append what we wrote to the result buffer
            strcat(buffer, node_buffer);
        }
    }

    send(fd, buffer, strlen(buffer), 0);
    free(buffer);
}

void send_release_to_agent(const char *agent_ip, long long job_id, const char *resource, int amount)
{
    NodeMapCell *target_node = hashmap_search(node_map, &agent_ip);

    // This node is not in the cluster, so we're done
    if (target_node == NULL)
        return;

    int fd = get_agent_connection(target_node);
    if (fd == -1) {
        error("Error intentando conectarse a un nodo");
        return;
    }

    // We won't close fd because we want to keep it alive in the epoll instance
    char release_msg[BUFFER_MAX_SIZE];
    snprintf(release_msg, sizeof(release_msg), "RELEASE %lld %s %d", job_id, resource, amount);
    send(fd, release_msg, strlen(release_msg), 0);
}

int get_agent_connection(NodeMapCell *node)
{
    // If connection is up, reuse it
    if (node->socket_fd != -1)
        return node->socket_fd;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return FAIL;

    set_socket_nonblocking(sock);

    // Set up socket and get address information
    struct sockaddr_in agent_addr;
    memset(&agent_addr, 0, sizeof(agent_addr));
    agent_addr.sin_family = AF_INET;
    agent_addr.sin_port = htons(node->port);
    inet_pton(AF_INET, node->ip, &agent_addr.sin_addr);

    // We check if errno is EINPROGRESS because sock is non-blocking.
    int res = connect(sock, (struct sockaddr *)&agent_addr, sizeof(agent_addr));
    if (res < 0 && errno != EINPROGRESS) {
        close(sock);
        return FAIL;
    }

    // Add to epoll instance to listen for events
    if (add_descriptor(sock) < 0) {
        close(sock);
        return FAIL;
    }

    node->socket_fd = sock;
    return sock;
}

void check_agent_expiration_time(void)
{
    time_t now = time(NULL);

    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            double diff = difftime(now, node->time_when_called);
            if (diff >= 15.0) {
                // TODO: check if there are resources to free
                hashmap_delete(node_map, node);
            }
        }
    }

    // Tell other agents we're alive
    ++seconds_passed;
    if (seconds_passed >= 5) {
        send_udp_announce();
        seconds_passed = 0;
    }
}

void handle_udp_packet(int udp_fd)
{
    char buffer[BUFFER_MAX_SIZE] = { 0 };
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);

    ssize_t bytes_read = recvfrom(udp_fd, buffer, BUFFER_MAX_SIZE - 1, 0,
                                  (struct sockaddr *)&addr,
                                  &addrlen);

    if (bytes_read <= 0)
        return;

    // Parse UDP 'ANNOUNCE' message
    int len = 0;
    char **fields = split(buffer, " ", &len);

    // Not an ANNOUNCE message...
    if (!streq(fields[0], "ANNOUNCE"))
        return;

    char *ip = fields[1];
    NodeMapCell *node = hashmap_search(node_map, &ip);

    if (node == NULL)
        // TODO: node does not exist.
        return;

    // fields looks like
    // [ANNOUNCE <ip> <port> cpu:<x> mem:<y> gpu:<z>]
    //
    // So take fields[3] + 4 to only get <x>
    // Same with mem and gpu
    LocalResources resources = {
        atoi(fields[3] + 4),
        atoi(fields[4] + 4),
        atoi(fields[5] + 4),
    };

    node->resources = resources;
    node->time_when_called = time(NULL);
    hashmap_put(node_map, node);

    free(fields);
}

// Closes the epoll instance
int close_epoll(void)
{
    if (close(epoll_fd) < 0) {
        error("Error intentando destruir la instancia de epoll");
        return FAIL;
    }

    return OK;
}

// Closes global sockets
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

LocalResources get_initial_resources(void)
{
    LocalResources res = { 0 };
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            res.cpu = node->resources.cpu;
            res.mem = node->resources.mem;
            res.gpu = node->resources.gpu;

            res.current_cpu = node->resources.current_cpu;
            res.current_mem = node->resources.current_mem;
            res.current_gpu = node->resources.current_gpu;
        }
    }

    return res;
}
