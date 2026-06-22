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
#include "ds/list.h"
#include "utils.h"
#include "const.h"
#include "types.h"
#include "resources.h"
#include "agents.h"
#include "erl.h"

int listen_public_sock, listen_erlang_sock;
int connect_public_sock, connect_erlang_sock;
int udp_broadcast_sock;
int timer_fd;
int epoll_fd, num_fds_ready;
int agent_port;
static int seconds_passed = 0;
struct epoll_event ev, events[EPOLL_MAX_EVENTS];

AgentState state;

void *worker_thread_handler(void *arg);
void *checker_thread_handler(void *arg);
void delete_from_job_queue(JobQueueData *job);
int init_sockets(void);
int init_agents_socket(void);
int init_listen_erlang_socket(void);
int init_udp_socket(void);
int init_timer(void);
int init_epoll(void);
int startup(void);
void send_udp_announce(void);
int add_descriptors(void);
void *epoll_handler(void *arg);
void check_agent_expiration_time(void);
void handle_udp_packet(int udp_fd);
int close_epoll(void);
int close_sockets(void);
int cleanup(int flags);

int main(int argc, char **argv)
{
    pthread_mutex_init(&state.protection.mutex, NULL);
    pthread_cond_init(&state.protection.nonempty_queue_cond, NULL);

    state.node_resources.cpu = MAX_CPU;
    state.node_resources.gpu = MAX_GPU;
    state.node_resources.mem = MAX_MEM;
    state.node_resources.current_cpu = state.node_resources.cpu;
    state.node_resources.current_gpu = state.node_resources.gpu;
    state.node_resources.current_mem = state.node_resources.mem;

    state.node_map = hashmap_make(HASHMAP_INITIAL_CAP, (CopyFunc)node_cell_copy, (CmpFunc)node_cell_cmp, (FreeFunc)node_cell_free, (HashFunc)node_cell_hash); 
    state.job_map = hashmap_make(HASHMAP_INITIAL_CAP, (CopyFunc)job_cell_copy, (CmpFunc)job_cell_cmp, (FreeFunc)job_cell_free, (HashFunc)job_cell_hash);
    state.job_queue = queue_make();
    state.timed_out_jobs = list_make();

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

    pthread_t worker_thread;
    if (pthread_create(&worker_thread, NULL, worker_thread_handler, NULL) < 0)
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    pthread_t checker_thread;
    if (pthread_create(&checker_thread, NULL, checker_thread_handler, NULL) < 0)
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    pthread_join(checker_thread, NULL);

    pthread_t epoll_thread;
    if (pthread_create(&epoll_thread, NULL, epoll_handler, NULL) < 0)
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    pthread_join(epoll_thread, NULL);

    if (close_epoll() < 0)
        return 1;

    if (close_sockets() < 0)
        return 1;

    pthread_cond_destroy(&state.protection.nonempty_queue_cond);
    pthread_mutex_destroy(&state.protection.mutex);
    return 0;
}

void *worker_thread_handler(void *arg)
{
    while (true) {
        if (!queue_empty(state.job_queue)) {
            ErlangRequest erl = *(ErlangRequest *)queue_head(state.job_queue);
            handle_job_request(erl, epoll_fd, &state);
            state.job_queue = dequeue(state.job_queue, (QueueFreeFunc)job_free);
        }
    }

    return NULL;
}

void *checker_thread_handler(void *arg)
{
    while (true) {
        // If agent must use job_queue, give it time to do so
        sleep(CHECKER_QUEUE_USE_TIME);
        pthread_mutex_lock(&state.protection.mutex);

        while (queue_empty(state.job_queue))
            pthread_cond_wait(&state.protection.nonempty_queue_cond,
                              &state.protection.mutex);

        for (QueueNode *p = state.job_queue; p != NULL; p = p->next) {
            QueueNode *next = p->next;
            JobQueueData *job = (JobQueueData *)p->data;
            if (difftime(time(NULL), job->time_when_alloc) >= CHECKER_QUEUE_TIME_UNTIL_DELETE){
                char msg[BUFFER_MAX_SIZE] = { 0 };
                sprintf(msg,"JOB_TIMEOUT %lld",job->request.job_id);
                send(job->request.erlang_fd,msg,strlen(msg),0);
                delete_from_job_queue(job);
            }
            // We do this to prevent a potential use after free
            p = next;
        }

        pthread_mutex_unlock(&state.protection.mutex);
    }

    return NULL;
}

void delete_from_job_queue(JobQueueData *job)
{
    if (state.job_queue == NULL || job == NULL)
        return;

    QueueNode *prev = NULL;
    QueueNode *curr = state.job_queue;

    while (curr != NULL) {
        // Check if this node contains the target job data
        if (curr->data == job) {
            if (prev == NULL) // The target job is at the head of the queue
                state.job_queue = curr->next;
            else // The target job is in the middle or at the end
                prev->next = curr->next;

            job_free(job);
            free(curr);
            return;
        }

        prev = curr;
        curr = curr->next;
    }
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

    if (add_descriptor(epoll_fd, timer_fd) < 0) {
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
            state.node_resources.cpu,
            state.node_resources.mem,
            state.node_resources.gpu);

    // Because this is a UDP socket, we use sendto
    ssize_t bytes = sendto(udp_broadcast_sock, buffer, strlen(buffer), 0,
                            (struct sockaddr *)&sa_broadcast,
                            sizeof(sa_broadcast));

    if (bytes < 0)
        error("Error intentando enviar ANNOUNCE");
}

int add_descriptors(void)
{
    if (add_descriptor(epoll_fd, listen_public_sock) < 0)
        return FAIL;

    if (add_descriptor(epoll_fd, listen_erlang_sock) < 0)
        return FAIL;

    if (add_descriptor(epoll_fd, udp_broadcast_sock) < 0)
        return FAIL;

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
                if (add_descriptor(epoll_fd, connect_public_sock) < 0)
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
                if (add_descriptor(epoll_fd, connect_erlang_sock) < 0)
                    exit(EXIT_FAILURE);
            } else if (events[i].data.fd == udp_broadcast_sock)
                handle_udp_packet(events[i].data.fd);
            else if (events[i].data.fd == timer_fd) {
                long long expirations = 0;
                read(timer_fd, &expirations, sizeof(long long));
                check_agent_expiration_time();
            } else {
                int client_fd = events[i].data.fd;

                if (client_fd == connect_public_sock)
                    handle_c_agent(client_fd, epoll_fd, &state);
                else if (client_fd == connect_erlang_sock)
                    handle_erlang_client(client_fd, epoll_fd, &state);
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

void check_agent_expiration_time(void)
{
    time_t now = time(NULL);

    for (int i = 0; i < state.node_map->cap; ++i) {
        bool exists_item = state.node_map->items[i].data != NULL &&
                            !state.node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)state.node_map->items[i].data;
            double diff = difftime(now, node->time_when_called);
            if (diff >= 15.0) {
                release_affected_jobs(node, state.node_map,
                                      state.job_map, &state.node_resources);
                hashmap_delete(state.node_map, node);
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

    char *ip = inet_ntoa(addr.sin_addr);
    NodeMapCell *node = hashmap_search(state.node_map, &ip);

    // It's the first time the node was announced
    if (node == NULL){
        node = malloc(sizeof(NodeMapCell));
        node->ip = inet_ntoa(addr.sin_addr);
        node->port = atoi(fields[1]);
        node->socket_fd = -1;
    }


    // fields looks like
    // [ANNOUNCE <port> cpu:<x> mem:<y> gpu:<z>]
    //
    // So take fields[2] + 4 to only get <x>
    // Same with mem and gpu
    LocalResources resources = {
        atoi(fields[2] + 4),
        atoi(fields[3] + 4),
        atoi(fields[4] + 4),
    };

    node->resources = resources;
    node->time_when_called = time(NULL);
    hashmap_put(state.node_map, node);

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

    if (close(udp_broadcast_sock) < 0) {
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
