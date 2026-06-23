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
#include "log/log.h"

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
    srand(time(NULL));
    // Inicializamos el estado global
    pthread_mutex_init(&state.protection.mutex, NULL);
    pthread_cond_init(&state.protection.nonempty_queue_cond, NULL);

    state.node_resources.cpu = MAX_CPU;
    state.node_resources.gpu = MAX_GPU;
    state.node_resources.mem = MAX_MEM;
    state.node_resources.current_cpu = state.node_resources.cpu;
    state.node_resources.current_gpu = state.node_resources.gpu;
    state.node_resources.current_mem = state.node_resources.mem;

    state.node_map = hashmap_make(HASHMAP_INITIAL_CAP, (CopyFunc)node_cell_copy,
                                 (CmpFunc)node_cell_cmp,
                                 (FreeFunc)node_cell_free,
                                 (HashFunc)node_cell_hash);
    state.job_map = hashmap_make(HASHMAP_INITIAL_CAP, (CopyFunc)job_cell_copy,
                                (CmpFunc)job_cell_cmp,
                                (FreeFunc)job_cell_free,
                                (HashFunc)job_cell_hash);
    state.job_queue = queue_make();
    state.timed_out_jobs = list_make();

    // Si un programa se desconecta inesperadamente, decidimos
    // ignorarlo, pues se maneja cuando read() <= 0
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2)
        agent_port = DEFAULT_PORT;
    else
        agent_port = atoi(argv[1]);

    log_message("[C]: Inicializando agente en el puerto %d", agent_port);

    // Estructura del programa:
    //  1. Inicializar instancia de epoll y sockets
    //  2. Añadir sockets de escucha y broadcast a la instancia de epoll
    //  3. Arranque inicial del cliente
    //  4. Llamar al thread que maneja requests
    //  5. Llamar al thread que chequea recursos estancados
    //  6. Llamar al bucle de epoll
    //  7. Destruir recursos

    log_message("[C]: Inicializando instancia epoll");
    if (init_epoll() < 0)
        return cleanup(CLEAN_SOCKETS);

    log_message("[C]: Inicializando sockets de escucha");
    if (init_sockets() < 0)
        return 1;

    log_message("[C]: Añadiendo descriptores a instancia epoll");
    if (add_descriptors())
        return cleanup(CLEAN_SOCKETS | CLEAN_EPOLL);

    log_message("[C]: Arranque inicial");
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

    log_message("[C]: Cerrando instancia de epoll");
    if (close_epoll() < 0)
        return 1;

    log_message("[C]: Cerrando sockets de escucha");
    if (close_sockets() < 0)
        return 1;

    pthread_cond_destroy(&state.protection.nonempty_queue_cond);
    pthread_mutex_destroy(&state.protection.mutex);
    list_free(state.timed_out_jobs, (ListFreeFunc)free);
    queue_free(state.job_queue, (QueueFreeFunc)job_free);
    hashmap_free(state.job_map);
    hashmap_free(state.node_map);
    return 0;
}

// Revisa regularmente la cola y si no está vacía,
// maneja el request encolado en job_queue
void *worker_thread_handler(void *arg)
{
    while (true) {
        int sleep_time = rand();
        sleep(sleep_time%QUEUE_SLEEP_TIME + 1);
        pthread_mutex_lock(&state.protection.mutex);
        while (queue_empty(state.job_queue))
            pthread_cond_wait(&state.protection.nonempty_queue_cond,
                              &state.protection.mutex);

        if (!queue_empty(state.job_queue)) {
            ErlangRequest erl = *(ErlangRequest *)queue_head(state.job_queue);
            handle_job_request(erl, epoll_fd, &state);
            state.job_queue = dequeue(state.job_queue, (QueueFreeFunc)job_free);
        }

        pthread_mutex_unlock(&state.protection.mutex);
    }

    return NULL;
}

// Revisa regularmente la cola, y si un job fue encolado
// por más de 15 segundos, es eliminado (pues ayuda a prevenir
// deadlocks) y se le informa al cliente de Erlang adecuado
// de un TIMEOUT
void *checker_thread_handler(void *arg)
{
    while (true) {
        // Si el agente debe usar job_queue, darle tiempo para que lo haga
        sleep(CHECKER_QUEUE_USE_TIME);
        pthread_mutex_lock(&state.protection.mutex);

        while (queue_empty(state.job_queue))
            pthread_cond_wait(&state.protection.nonempty_queue_cond,
                              &state.protection.mutex);

        // Buscamos en cada job de la cola de jobs, y si pasaron más de
        // CHECKER_QUEUE_TIME_UNTIL_DELETE (15) segundos, se envía
        // un timeout al cliente de Erlang que lo pidió
        for (QueueNode *p = state.job_queue; p != NULL; p = p->next) {
            QueueNode *next = p->next;
            JobQueueData *job = (JobQueueData *)p->data;
            if (difftime(time(NULL), job->time_when_alloc) >= CHECKER_QUEUE_TIME_UNTIL_DELETE) {
                char msg[BUFFER_MAX_SIZE] = { 0 };
                sprintf(msg, "JOB_TIMEOUT %lld", job->request.job_id);
                send(job->request.erlang_fd, msg, strlen(msg), 0);
                delete_from_job_queue(job);
            }

            // Hacemos esto para prevenir un posible use-after-free
            p = next;
        }

        pthread_mutex_unlock(&state.protection.mutex);
    }

    return NULL;
}

// Elimina un job de job_queue
void delete_from_job_queue(JobQueueData *job)
{
    // O la cola está vacía o el job no es válido.
    // Entonces no se elimina nada
    if (state.job_queue == NULL || job == NULL)
        return;

    QueueNode *prev = NULL;
    QueueNode *curr = state.job_queue;

    // Buscamos el job en la cola que coincida con job
    while (curr != NULL) {
        if (curr->data == job) {
            if (prev == NULL) // El job que buscamos está en el head de la cola
                state.job_queue = curr->next;
            else // El job que buscamos está después
                prev->next = curr->next;

            job_free(job);
            free(curr);
            return;
        }

        prev = curr;
        curr = curr->next;
    }
}

// Inicializa los sockets del agente C, el cliente Erlang
// el broadcast UDP, y el timer
int init_sockets(void)
{
    if (init_agents_socket() < 0)
        return FAIL;

    if (init_listen_erlang_socket() < 0)
        return FAIL;

    if (init_udp_socket() < 0)
        return FAIL;

    if (init_timer() < 0)
        return FAIL;

    return OK;
}

// Inicializa el socket de escucha del agente C
int init_agents_socket(void)
{
    log_message("[C]: Inicializando el socket de escucha entre agentes");

    int yes = 1;
    listen_public_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_public_sock < 0) {
        error("Error intentando iniciar el socket para otros agentes");
        return FAIL;
    }

    // Seteamos la opción de reusar la dirección y el puerto
    if (setsockopt(listen_public_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket público para ser reusado");
        close(listen_public_sock);
        return FAIL;
    }

    // Bindeamos y esperamos desde el socket
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

// Inicializa el socket de escucha del planeador de Erlang
int init_listen_erlang_socket(void)
{
    log_message("[C]: Inicializando el socket de escucha de Erlang");

    int yes = 1;
    listen_erlang_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_erlang_sock < 0) {
        error("Error intentando iniciar el socket para Erlang");
        return FAIL;
    }

    // Seteamos la opción de reusar la dirección y el puerto
    if (setsockopt(listen_erlang_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket de Erlang para ser reusado");
        close(listen_erlang_sock);
        return FAIL;
    }

    // Bindeamos y esperamos desde el socket
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

// Inicializa el socket de broadcast de UDP
int init_udp_socket(void)
{
    log_message("[C]: Inicializando el broadcast UDP");

    int yes = 1;
    udp_broadcast_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_broadcast_sock < 0) {
        error("Error intentando iniciar el socket UDP");
        return FAIL;
    }

    // Seteamos para que sea un socket de broadcast
    if (setsockopt(udp_broadcast_sock, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket de UDP para permitir broadcast");
        return FAIL;
    }

    // Bindeamos la dirección que encontremos
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
    log_message("[C]: Inicializando el temporizador");

    timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timer_fd < 0) {
        error("Error intentando crear el temporizador");
        return FAIL;
    }

    struct itimerspec expiration = { 0 };

    // Chequeo de primer expiración
    expiration.it_value.tv_sec = 5;
    expiration.it_value.tv_nsec = 0;

    // Chequeo periódico de expiración
    expiration.it_interval.tv_sec = 5;
    expiration.it_value.tv_nsec = 0;

    if (timerfd_settime(timer_fd, 0, &expiration, NULL) < 0) {
        error("Error intentando configurar el temporizador");
        close(timer_fd);
        return FAIL;
    }

    if (add_descriptor(epoll_fd, timer_fd, NULL) < 0) {
        close(timer_fd);
        return FAIL;
    }

    return OK;
}

// Inicializa la instancia de epoll
int init_epoll(void)
{
    epoll_fd = epoll_create(1);
    if (epoll_fd < 0) {
        error("Error intentando crear la instancia de epoll");
        return FAIL;
    }

    return OK;
}

// Arranque inicial del agente C
int startup(void)
{
    // Anuncia su existencia
    send_udp_announce();

    struct epoll_event startup_events[EPOLL_MAX_EVENTS];
    time_t start_time = time(NULL);

    // Espera dos segundos para encontrar nodos activos
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

// Anuncia la existencia del agente C usando el siguiente mensaje:
// ANNOUNCE <port> cpu:<x> mem:<y> gpu:<z>
void send_udp_announce(void)
{
    log_message("[C]: Enviando ANNOUNCE a los otros agentes");

    // Preparamos la configuracion del socket para
    // enviar el mensaje
    struct sockaddr_in sa_broadcast;
    memset(&sa_broadcast, 0, sizeof(sa_broadcast));
    sa_broadcast.sin_family = AF_INET;
    sa_broadcast.sin_port = htons(agent_port);
    sa_broadcast.sin_addr.s_addr = htonl(INADDR_BROADCAST);

    char buffer[BUFFER_MAX_SIZE] = { 0 };
    sprintf(buffer, "ANNOUNCE %d cpu:%d mem:%d gpu:%d\n",
            agent_port,
            state.node_resources.cpu,
            state.node_resources.mem,
            state.node_resources.gpu);

    // Mandamos el mensaje al socket de broadcast
    // Como es un socket UDP, se utiliza sendto
    ssize_t bytes = sendto(udp_broadcast_sock, buffer, strlen(buffer), 0,
                            (struct sockaddr *)&sa_broadcast,
                            sizeof(sa_broadcast));

    if (bytes < 0)
        error("Error intentando enviar ANNOUNCE");
}

// Añade los descriptores necesarios a la instancia de epoll
int add_descriptors(void)
{
    if (add_descriptor(epoll_fd, listen_public_sock, NULL) < 0)
        return FAIL;

    if (add_descriptor(epoll_fd, listen_erlang_sock, NULL) < 0)
        return FAIL;

    if (add_descriptor(epoll_fd, udp_broadcast_sock, NULL) < 0)
        return FAIL;

    return OK;
}

// Maneja eventos de epoll
void *epoll_handler(void *arg)
{
    while (true) {
        num_fds_ready = epoll_wait(epoll_fd, events, EPOLL_MAX_EVENTS, EPOLL_TIMEOUT);
        if (num_fds_ready == -1) {
            error("Error en el bucle de epoll");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < num_fds_ready; ++i) {
            if (events[i].data.fd == listen_public_sock) {
                log_message("[C]: Manejando evento de socket de escucha de agentes");

                // Creamos un socket de conexión al agente de C
                // para enviar y recibir mensajes
                connect_public_sock = accept(listen_public_sock, NULL, NULL);
                if (connect_public_sock == -1) {
                    error("Error intentando aceptar un agente");
                    exit(EXIT_FAILURE);
                }

                set_socket_nonblocking(connect_public_sock);

                // Lo sumamos a la instancia de epoll
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = connect_public_sock;
                if (add_descriptor(epoll_fd, connect_public_sock, &ev) < 0)
                    exit(EXIT_FAILURE);
            } else if (events[i].data.fd == listen_erlang_sock) {
                log_message("[C]: Manejando evento de socket de escucha de cliente Erlang");

                // Creamos un socket de conexión al cliente de Erlang
                // para enviar y recibir mensajes
                connect_erlang_sock = accept(listen_erlang_sock, NULL, NULL);
                if (connect_erlang_sock == -1) {
                    error("Error intentando aceptar un cliente Erlang");
                    exit(EXIT_FAILURE);
                }

                set_socket_nonblocking(connect_erlang_sock);

                // Lo sumamos a la instancia de epoll
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = connect_erlang_sock;
                if (add_descriptor(epoll_fd, connect_erlang_sock, &ev) < 0)
                    exit(EXIT_FAILURE);
            } else if (events[i].data.fd == udp_broadcast_sock) {
                log_message("[C]: Manejando evento de socket de broadcast UDP");

                // Si es el socket de broadcast, lo manejamos por separado
                handle_udp_packet(events[i].data.fd);
            } else if (events[i].data.fd == timer_fd) {
                log_message("[C]: Manejando evento de temporizador");

                // Chequeamos si el tiempo de un agente expiró
                long long expirations = 0;
                read(timer_fd, &expirations, sizeof(long long));
                check_agent_expiration_time();
            } else {
                // Manejamos al agente de C como un cliente o
                // al cliente de Erlang
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

// Chequea si pasaron 15 segundos sin que un agente mande
// un ANNOUNCE
void check_agent_expiration_time(void)
{
    time_t now = time(NULL);

    // Buscamos entre los nodos que tenemos y si la última vez
    // que se anunció fue hace más de 15 segundos, lo eliminamos
    // y liberamos los recursos de jobs que dependían de él
    for (int i = 0; i < state.node_map->cap; ++i) {
        bool exists_item = state.node_map->items[i].data != NULL &&
                            !state.node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)state.node_map->items[i].data;
            double diff = difftime(now, node->time_when_called);
            if (diff >= 15.0) {
                log_message("[C]: Un nodo no se anunció en 15 segundos. Eliminándolo de la tabla");
                release_affected_jobs(node, state.node_map,
                                      state.job_map, &state.node_resources);
                hashmap_delete(state.node_map, node);
            }
        }
    }

    // Avisamos que estamos vivos cada cinco segundos
    ++seconds_passed;
    if (seconds_passed >= 5) {
        send_udp_announce();
        seconds_passed = 0;
    }
}

// Maneja mensajes ANNOUNCE
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

    // Parseamos el mensaje 'ANNOUNCE' que llegue
    int len = 0;
    char **fields = split(buffer, " ", &len);

    // El mensaje que llegó no es un ANNOUNCE...
    if (!streq(fields[0], "ANNOUNCE"))
        return;

    char *ip = inet_ntoa(addr.sin_addr);
    NodeMapCell *node = hashmap_search(state.node_map, &ip);

    // Si no existe en nuestra tabla de nodos,
    // lo creamos para luego sumarlo
    if (node == NULL) {
        node = malloc(sizeof(NodeMapCell));
        node->ip = inet_ntoa(addr.sin_addr);
        node->port = atoi(fields[1]);
        node->socket_fd = -1;
    }

    // fields tiene la pinta
    // [ANNOUNCE <port> cpu:<x> mem:<y> gpu:<z>]
    //
    // Entonces si quiero el campo de CPU, fields[2] + 4 te da <x>
    // Misma situación con mem y gpu
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

// Cierra la instancia de epoll
int close_epoll(void)
{
    if (close(epoll_fd) < 0) {
        error("Error intentando destruir la instancia de epoll");
        return FAIL;
    }

    return OK;
}

// Cierra los sockets de escucha y broadcast
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

// Limpia los sockets o epoll, según el caso especificado
// por flags (usando OR)
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
