// TODO LIST
// - implement startup
// - UDP socket broadcast
// - add error messages
// - implement parse_erlang_request
// - handle unexpected disconnections using SIGPIPE (cuando te intentas
//   conectar con TCP a algo y ese algo dejo de existir, te manda un error
//   de tipo SIGPIPE. y nada, eso es lo que tenemos que usar para manejar la
//   desconexion inesperada. Tenemos que cacheaar usando signal a SIGPIPE y
//   hacer algo)
// - implement handle_job_status
// - deadlock prevention (timeouts, timerfd)
// - handle enqueued requests when REQUEST_KIND_RELEASE (tener una cola que guarde
//   requests completas, fijarnos si hay recursos para el request y fijarse si
//   grant o deny)

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
#include "ds/queue.h"

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
    int job_id;
    int num_remotely_allocated;
    RemoteAllocation *remote_allocations; // array of all the remotely allocated resources
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
LocalResources node_resources;
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
void give_resources(ResourceKind kind, int amount);
void increase_resources(ResourceKind kind, int amount);
Request *request_dup(Request *req);
void handle_erlang_client(int erlang_client_fd);
void handle_job_request(char **request_fields, int request_fields_size, int fd);
NodeMapCell *parse_erlang_request(char **request_fields, int request_fields_size, int *len);
void find_requested_parameters(char **request_fields, int request_fields_size, char *resource, int *amount);
void handle_job_release(char **request_fields, int fd);
void handle_job_status(char **request_fields, int fd);
void handle_get_nodes(char **request_fields, int fd);
void send_release_to_agent(const char *agent_ip, long long job_id, const char *resource, int amount);
int get_agent_connection(NodeMapCell *node);
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

// Initialize c agent and erlang sockets
int init_sockets(void)
{
    if (init_agents_socket() < 0)
        return FAIL;

    if (init_listen_erlang_socket() < 0)
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

// Initializes the erlang planner socket
int init_listen_erlang_socket(void)
{
    // yeses the yes
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

            // 1001 {cpu 1, mem 0, gpu 0}
            // RESERVE 1001 gpu 1 (r)
            // cell = r
            // Request {REQUEST_KIND_RESERVE, <job_id>, RES_KIND_GPU, 1};
            // cell->granted_resources.gpu += 1
            // hashmap_put(hm, cell)
            // 1001 {cpu 1, mem 0, gpu 1}
            switch (req.res_kind) {
            case RES_KIND_CPU:
                cell->granted_resources.cpu += req.amount;
                break;
            case RES_KIND_GPU:
                cell->granted_resources.gpu += req.amount;
                break;
            case RES_KIND_MEM:
                cell->granted_resources.mem += req.amount;
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
            request_queue = enqueue(request_queue, &req, (QueueCpyFunc)request_dup);
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
            cell->granted_resources.cpu -= req.amount;
            break;
        case RES_KIND_GPU:
            cell->granted_resources.gpu -= req.amount;
            break;
        case RES_KIND_MEM:
            cell->granted_resources.mem -= req.amount;
            break;
        default:
            // lol
            break;
        }

        if (cell->granted_resources.cpu == 0 && cell->granted_resources.gpu == 0 &&
            cell->granted_resources.mem) {
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
        if (node_resources.cpu >= amount)
            return true;
        break;
    case RES_KIND_MEM:
        if (node_resources.mem >= amount)
            return true;
        break;
    case RES_KIND_GPU:
        if (node_resources.gpu >= amount)
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
        node_resources.cpu -= amount;
        break;
    case RES_KIND_MEM:
        node_resources.mem -= amount;
        break;
    case RES_KIND_GPU:
        node_resources.gpu -= amount;
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
        node_resources.cpu += amount;
        break;
    case RES_KIND_MEM:
        node_resources.mem += amount;
        break;
    case RES_KIND_GPU:
        node_resources.gpu += amount;
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
    if (bytes_read <= 0);
        // TODO error no bytes read
    else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);

        if (streq(request_fields[0], "JOB_REQUEST")) {
            handle_job_request(request_fields, length, fd);

            //char granted_buffer[BUFFER_MAX_SIZE] = { 0 };
            //sprintf(granted_buffer, "GRANTED %lld", atoi(request_fields[1]));
            //send(fd, granted_buffer, strlen(granted_buffer), 0);
        } else if (streq(request_fields[0],"JOB_RELEASE"))
            handle_job_release(request_fields,fd);
        else if (streq(request_fields[0],"JOB_STATUS"))
            handle_job_status(request_fields,fd);
        else if (streq(request_fields[0],"GET_NODES"))
            handle_get_nodes(request_fields,fd);
        else {
            // TODO: HANDLE ERROR
        }
    }
}

// Handles an incoming job request from the erlang client
void handle_job_request(char **request_fields, int request_fields_size, int fd)
{
    long long job_id = atoll(request_fields[1]);
    int len = 0;
    NodeMapCell *parsed = parse_erlang_request(request_fields, request_fields_size, &len);

    for (int i = 0; i < len; ++i) {
        int agent_fd = get_agent_connection(&parsed[i]);
        if (agent_fd == -1) {
            // TODO: handle error
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
        find_requested_parameters(request_fields + 2, request_fields_size - 2, resource, &amount);

        // Send RESERVE message based on what we found
        sprintf(msg, "RESERVE %lld %s %d", job_id, resource, amount);
        send(agent_fd, msg, strlen(msg), 0);
    }
}

NodeMapCell *parse_erlang_request(char **request_fields, int request_fields_size, int *len)
{
    return NULL;
}

void find_requested_parameters(char **request_fields, int request_fields_size, char *resource, int *amount)
{
    for (int i = 0; i < request_fields_size; ++i) {
        char **resource_fields = split(request_fields[i], ":", NULL);
        strcpy(resource, resource_fields[1]);
        *amount = atoi(resource_fields[2]);
    }
}

// Handles an incoming job release from the erlang client
// IMPLEMENTACION: RECIBE RELEASE -> LIBERA RECURSOS LOCALES (O NO) -> ELIMINA EL JOB DE JOB_MAP -> MANDA RELEASE A LOS AGENTES C CORRESPONDIENTES
void handle_job_release(char **request_fields, int fd)
{
    long long job_id = atoll(request_fields[1]);
    JobMapCell *cell = hashmap_search(job_map, &job_id);
    if (cell == NULL) {
        // TODO: que pasa si el job no existe?
    }

    increase_resources(RES_KIND_CPU, cell->granted_resources.cpu);
    increase_resources(RES_KIND_GPU, cell->granted_resources.gpu);
    increase_resources(RES_KIND_MEM, cell->granted_resources.mem);

    // Iterate over remotely allocated resources
    for (int i = 0; i < cell->num_remotely_allocated; i++) {
        RemoteAllocation remote = cell->remote_allocations[i];
        if (remote.resources.cpu > 0)
            send_release_to_agent(remote.ip, job_id, "cpu", remote.resources.cpu);
        if (remote.resources.gpu > 0)
            send_release_to_agent(remote.ip, job_id, "gpu", remote.resources.gpu);
        if (remote.resources.mem > 0)
            send_release_to_agent(remote.ip, job_id, "mem", remote.resources.mem);
    }

    free(cell->remote_allocations);
    hashmap_delete(job_map, &job_id);

    // TODO: HAY QUE RESPONDERLE A ERLANG??
}

// Handles an incoming job status from the erlang client
// TIMEOUT -> ?????
// GRANTED -> Estan los recursos disponibles (EN TODA LA RED) para que haga el job
// DENIED -> TODO
// IMPLEMENTACION: QUIEN SABE
void handle_job_status(char **request_fields, int fd)
{
    // TODO: wtf???
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
}

void send_release_to_agent(const char *agent_ip, long long job_id, const char *resource, int amount)
{
    NodeMapCell *target_node = hashmap_search(node_map, &agent_ip);

    // This node is not in the cluster, so we're done
    if (target_node == NULL)
        return;

    int fd = get_agent_connection(target_node);
    if (fd == -1) {
        // TODO: handle error
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
        // TODO: error message
        close(sock);
        return FAIL;
    }

    node->socket_fd = sock;
    return sock;
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
