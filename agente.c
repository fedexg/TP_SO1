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
int add_descriptor(int fd);
void *epoll_handler(void *arg);
void handle_c_agent(int c_agent_fd);
void handle_unexpected_disconnection(int fd);
Request parse_request(char **request_fields, int n_fields);
void process_request(Request req, int fd);
void handle_erlang_client(int erlang_client_fd);
void handle_job_request(ErlangRequest erl);
void handle_job_release(ErlangRequest erl);
void handle_job_status(ErlangRequest erl);
bool find_in_queue(Queue queue, int id);
bool find_in_list(List list, int id);
void delete_from_timed_out_jobs(int id);
void handle_get_nodes(ErlangRequest erl);
void send_release_to_agent(const char *agent_ip, long long job_id, const char *resource, int amount);
int get_agent_connection(const char *ip, int port);
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

    return 0;
}

void *worker_thread_handler(void *arg)
{
    while (true) {
        if (job_queue != NULL) {
            ErlangRequest erl = *(ErlangRequest *)queue_head(job_queue);
            handle_job_request(erl);
            job_queue = dequeue(job_queue, (QueueFreeFunc)job_queue);
        }
    }

    return NULL;
}

void *checker_thread_handler(void *arg)
{
    while (true) {
        // If agent must use job_queue, give it time to do so
        sleep(CHECKER_QUEUE_USE_TIME);
        for (QueueNode *p = job_queue; p != NULL; p = p->next) {
            QueueNode *next = p->next;
            JobQueueData *job = (JobQueueData *)p->data;
            if (difftime(time(NULL), job->time_when_alloc) >= CHECKER_QUEUE_TIME_UNTIL_DELETE)
                delete_from_job_queue(job);

            // We do this to prevent a potential use after free
            p = next;
        }
    }

    return NULL;
}

void delete_from_job_queue(JobQueueData *job)
{
    if (job_queue == NULL || job == NULL)
        return;

    QueueNode *prev = NULL;
    QueueNode *curr = job_queue;

    while (curr != NULL) {
        // Check if this node contains the target job data
        if (curr->data == job) {
            if (prev == NULL) // The target job is at the head of the queue
                job_queue = curr->next;
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

    // Find dead node file descriptor
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

    // Find affected jobs
    for (int i = 0; i < job_map->cap; ++i) {
        bool exists_job = job_map->items[i].data != NULL && !job_map->items[i].deleted;
        if (exists_job) {
            JobMapCell *job = (JobMapCell *)job_map->items[i].data;
            bool affected_job = false;

            // Check if job depended on dead node
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

            // Return resources to affected nodes
            if (affected_job) {
                for (int j = 0; j < job->num_remotely_allocated; ++j) {
                    RemoteAllocation *remote = &job->remote_allocations[j];

                    // If it's a non-dead node, restore its resources
                    if (!streq(remote->ip, dead_node->ip)) {
                        NodeMapCell *alive_node = hashmap_search(node_map, &remote->ip);
                        if (alive_node != NULL) {
                            alive_node->resources.current_cpu += remote->resources.current_cpu;
                            alive_node->resources.current_mem += remote->resources.current_mem;
                            alive_node->resources.current_gpu += remote->resources.current_gpu;
                        }
                    }

                    memset(&remote->resources, 0, sizeof(LocalResources));
                }

                if (job->granted_resources.current_cpu > 0)
                    give_resources(&node_resources, RES_KIND_CPU, job->granted_resources.current_cpu);
                if (job->granted_resources.current_mem > 0)
                    give_resources(&node_resources, RES_KIND_MEM, job->granted_resources.current_mem);
                if (job->granted_resources.current_gpu > 0)
                    give_resources(&node_resources, RES_KIND_GPU, job->granted_resources.current_gpu);

                // Delete job
                long long delete_id = job->job_id;
                free(job->remote_allocations);
                hashmap_delete(job_map, &delete_id);
            }
        }
    }

    hashmap_delete(node_map, &dead_node->ip);
    free(dead_node);
}

// Given a request and a file descriptor, processes the agent c request.
void process_request(Request req, int fd)
{
    char response_to_agent[128] = {0};
    switch (req.kind) {
    case REQUEST_KIND_RESERVE:
        if (exists_resource(&node_resources, req.res_kind, req.amount)) {
            JobMapCell *cell = hashmap_search(job_map, &req.job_id);

            // We didn't find a cell with this job id. We create it.
            if (cell == NULL)
                cell = calloc(1, sizeof(JobMapCell));

            increase_resources(&(cell->granted_resources),req.res_kind, req.amount);
            hashmap_put(job_map, &cell);
            give_resources(&node_resources, req.res_kind, req.amount);
            sprintf(response_to_agent, "GRANTED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
        } else {
            sprintf(response_to_agent, "DENIED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
        }

        break;

    case REQUEST_KIND_RELEASE:
        increase_resources(&node_resources, req.res_kind, req.amount);
        JobMapCell *cell = hashmap_search(job_map, &req.job_id);

        // Cell not found, therefore no job with that id was found.
        if (cell == NULL) {
            sprintf(response_to_agent, "DENIED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
            return;
        }

        give_resources(&(cell->granted_resources), req.res_kind, req.amount);
        if (cell->granted_resources.current_cpu == 0 && cell->granted_resources.current_gpu == 0 &&
            cell->granted_resources.current_mem) {
            hashmap_delete(job_map, &req.job_id);
            free(cell);
        } else
            hashmap_put(job_map, &cell);

        while (!queue_empty(job_queue)) {
            // TODO: this doesn't type :/ 
            ErlangRequest erl = *(ErlangRequest *)queue_head(job_queue);
            job_queue = dequeue(job_queue, (QueueFreeFunc)job_free);
            handle_job_request(erl);
        }

        break;
    default:
        break;
    }
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
        ErlangRequest erl = parse_erlang_request(node_map, request_fields, length, fd);

        if (streq(request_fields[0], "JOB_REQUEST"))
            handle_job_request(erl);
        else if (streq(request_fields[0], "JOB_RELEASE"))
            handle_job_release(erl);
        else if (streq(request_fields[0], "JOB_STATUS"))
            handle_job_status(erl);
        else if (streq(request_fields[0], "GET_NODES"))
            handle_get_nodes(erl);
        else {
            char *msg = "Error: comando desconocido";
            send(fd, msg, strlen(msg), 0);
        }

        free(request_fields);
    }
}

// Handles an incoming job request from the erlang client
void handle_job_request(ErlangRequest erl)
{
    // ["JOB_REQUEST" "1001" "@host:res:amount" "@host:res:amount"]

    char msg[BUFFER_MAX_SIZE] = { 0 };
    long long job_id = erl.job_id;
    LocalResources res = get_initial_resources();

    int cpu = 0, mem = 0, gpu = 0;

    // Gather total resources from request to determine response
    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo alloc = erl.node_allocations[i];
        if (alloc.res_kind == RES_KIND_CPU)
            cpu += alloc.amount;
        if (alloc.res_kind == RES_KIND_MEM)
            mem += alloc.amount;
        if (alloc.res_kind == RES_KIND_GPU)
            gpu += alloc.amount;
    }

    // Total amount of resources exceeds any resource that the
    // cluster had in the first place. Therefore, job is impossible
    // and we answer a TIMEOUT
    if (cpu > res.cpu || mem > res.mem || gpu > res.gpu) {
        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_TIMEOUT %lld", job_id);
        send(erl.erlang_fd, msg, strlen(msg), 0);
        return;
    }

    // Ask agents for what we need
    int num_granted = 0;
    List agent_fds = list_make();
    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo alloc = erl.node_allocations[i];
        int agent_fd = alloc.agent_fd;
        if (agent_fd == -1) {
            agent_fd = get_agent_connection(alloc.ip, alloc.port);

            if (agent_fd == -1) {
                error("Error intentando conectarse a un nodo");
                return;
            }
        }

        char msg[BUFFER_MAX_SIZE] = { 0 };
        char resource[4] = { 0 };

        if (alloc.res_kind == RES_KIND_CPU)
            strcpy(resource, "cpu");
        if (alloc.res_kind == RES_KIND_MEM)
            strcpy(resource, "mem");
        if (alloc.res_kind == RES_KIND_GPU)
            strcpy(resource, "gpu");

        // Send RESERVE message based on what we found
        sprintf(msg, "RESERVE %lld %s %d", job_id, resource, alloc.amount);
        send(agent_fd, msg, strlen(msg), 0);

        // Count how many granteds we received
        char recv_buffer[BUFFER_MAX_SIZE] = { 0 };
        recv(agent_fd, recv_buffer, BUFFER_MAX_SIZE - 1, 0);
        if (strncmp(recv_buffer, "GRANTED", 7) == 0) {
            ++num_granted;
            agent_fds = list_append(agent_fds, &agent_fd, (ListCpyFunc)int_copy);
        }
    }

    if (num_granted < erl.num_allocations) {
        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_DENIED %lld", job_id);
        send(erl.erlang_fd, msg, strlen(msg), 0);

        for (ListNode *p = agent_fds; p != NULL; p = p->next) {
            for (int i = 0; i < erl.num_allocations; ++i) {
                NodeAllocationInfo alloc = erl.node_allocations[i];
                if (*(int *)p->data == alloc.agent_fd) {
                    char buffer[BUFFER_MAX_SIZE]  = { 0 };
                    sprintf(buffer, "RELEASE %lld", job_id);
                    send(alloc.agent_fd, buffer, strlen(buffer), 0);
                }
            }
        }

        enqueue(job_queue, &erl, (QueueCpyFunc)job_copy);
        return;
    }

    memset(msg, 0, BUFFER_MAX_SIZE - 1);
    sprintf(msg, "JOB_GRANTED %lld", job_id);
    send(erl.erlang_fd, msg, strlen(msg), 0);
}

// Handles an incoming job release from the erlang client
void handle_job_release(ErlangRequest erl)
{
    long long job_id = erl.job_id;
    JobMapCell *cell = hashmap_search(job_map, &job_id);
    if (cell == NULL) {
        char error_buf[1024] = { 0 };
        sprintf(error_buf, "Error: no existe el job con id %lld", job_id);
        send(erl.erlang_fd, error_buf, strlen(error_buf), 0);
        return;
    }

    increase_resources(&node_resources, RES_KIND_CPU, cell->granted_resources.current_cpu);
    increase_resources(&node_resources, RES_KIND_GPU, cell->granted_resources.current_gpu);
    increase_resources(&node_resources, RES_KIND_MEM, cell->granted_resources.current_mem);

    // Iterate over remotely allocated resources
    for (int i = 0; i < cell->num_remotely_allocated; i++) {
        RemoteAllocation remote = cell->remote_allocations[i];
        if (remote.resources.current_cpu > 0)
            send_release_to_agent(remote.ip, job_id, "cpu", remote.resources.current_cpu);
        if (remote.resources.current_mem > 0)
            send_release_to_agent(remote.ip, job_id, "mem", remote.resources.current_mem);
        if (remote.resources.current_gpu > 0)
            send_release_to_agent(remote.ip, job_id, "gpu", remote.resources.current_gpu);
    }

    free(cell->remote_allocations);
    hashmap_delete(job_map, &job_id);
}

// Handles an incoming job status from the erlang client
void handle_job_status(ErlangRequest erl)
{
    long long job_id = erl.job_id;
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

    send(erl.erlang_fd, buffer, strlen(buffer), 0);
}

// Given a queue of jobs, determines if there's a job with a given id
bool find_in_queue(Queue queue, int id)
{
    for (QueueNode* actual_node = queue; actual_node != NULL; actual_node = actual_node->next) {
        JobQueueData *job = (JobQueueData *)actual_node->data;
        if (job->job_cell.job_id == id)
            return true;
    }

    return false;
}

// Given a list of job ids, determines if there's a id is in the list
bool find_in_list(List list, int id)
{
    for (ListNode *actual_node = list; actual_node != NULL; actual_node = actual_node->next) {
        int job_id = *(int *)actual_node->data;
        if (job_id == id)
            return true;
    }

    return false;
}

// Delete a job with given id from timed out jobs list
void delete_from_timed_out_jobs(int id)
{
    ListNode *node_before = timed_out_jobs;
    if (*(int*)node_before->data == id){
        free(node_before->data);
        timed_out_jobs = timed_out_jobs->next;
        free(node_before);
    } else {
        ListNode *actual_node = timed_out_jobs->next;
        for(; *(int*)actual_node->data != id && actual_node != NULL; actual_node = actual_node->next)
            node_before = node_before->next;
        if (actual_node != NULL) {
            node_before->next = actual_node->next;
            free(actual_node->data);
            free(actual_node);
        }
    }
}

// Handles an incoming get nodes from the erlang client
void handle_get_nodes(ErlangRequest erl)
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

    send(erl.erlang_fd, buffer, strlen(buffer), 0);
    free(buffer);
}

// Sends a RELEASE message to the C agent specified
void send_release_to_agent(const char *agent_ip, long long job_id, const char *resource, int amount)
{
    NodeMapCell *target_node = hashmap_search(node_map, &agent_ip);

    // This node is not in the cluster, so we're done
    if (target_node == NULL)
        return;

    int fd = get_agent_connection(target_node->ip, target_node->port);
    if (fd == -1) {
        error("Error intentando conectarse a un nodo");
        return;
    }

    // We won't close fd because we want to keep it alive in the epoll instance
    char release_msg[BUFFER_MAX_SIZE];
    snprintf(release_msg, sizeof(release_msg), "RELEASE %lld %s %d", job_id, resource, amount);
    send(fd, release_msg, strlen(release_msg), 0);
}

// Get file descriptor of agent which we need to send messages to
int get_agent_connection(const char *ip, int port)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return FAIL;

    set_socket_nonblocking(sock);

    // Set up socket and get address information
    struct sockaddr_in agent_addr;
    memset(&agent_addr, 0, sizeof(agent_addr));
    agent_addr.sin_family = AF_INET;
    agent_addr.sin_port = htons(port);

    inet_pton(AF_INET, ip, &agent_addr.sin_addr);

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

    char *ip = inet_ntoa(addr.sin_addr);
    NodeMapCell *node = hashmap_search(node_map, &ip);

    if (node == NULL)
        // TODO: node does not exist.
        return;

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
            res.cpu += node->resources.cpu;
            res.mem += node->resources.mem;
            res.gpu += node->resources.gpu;

            res.current_cpu += node->resources.current_cpu;
            res.current_mem += node->resources.current_mem;
            res.current_gpu += node->resources.current_gpu;
        }
    }

    return res;
}
