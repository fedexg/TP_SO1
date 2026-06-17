#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <string.h>
#include <unistd.h>

#define OK         0
#define FAIL       -1
#define PORT       12529

#define EPOLL_MAX_EVENTS 10
#define EPOLL_TIMEOUT -1

#define CLEAN_SOCKETS  0b1
#define CLEAN_EPOLL   0b10

// SOCK_STREAM -> TCP
// SOCK_DGRAM ->  UDP

int public_sock, erlang_sock;
int epoll_fd, num_fds_ready;
struct epoll_event ev, events[EPOLL_MAX_EVENTS];

void error(const char *msg);
int init_sockets(void);
int init_agents_socket(void);
int init_erlang_socket(void);
int init_epoll(void);
int startup(void);
int add_descriptors(void);
int add_descriptor(int fd);
void *epoll_handler(void *arg);
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

    if (init_erlang_socket() < 0)
        return FAIL;

    return OK;
}

int init_agents_socket(void)
{
    int yes = 1;
    public_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (public_sock < 0) {
        error("Error intentando iniciar el socket para otros agentes");
        return FAIL;
    }

    // Set agent sockets to be reused
    if (setsockopt(public_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket público para ser reusado");
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_public;
    memset(&sa_public, 0, sizeof(sa_public));
    sa_public.sin_family = AF_INET;
    sa_public.sin_port = htons(PORT);
    sa_public.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(public_sock, (struct sockaddr *)&sa_public, sizeof(sa_public)) < 0) {
        error("Error intentando asignar una dirección al socket público");
        return FAIL;
    }

    if (listen(public_sock, 10) < 0) {
        error("Error intentando setear el socket público para escuchar");
        return FAIL;
    }

    return OK;
}

int init_erlang_socket(void)
{
    int yes = 1;
    erlang_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (erlang_sock < 0) {
        error("Error intentando iniciar el socket para Erlang");
        return FAIL;
    }

    // Set erlang sockets to be reused
    if (setsockopt(erlang_sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        error("No se pudo configurar el socket de Erlang para ser reusado");
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_erlang;
    memset(&sa_erlang, 0, sizeof(sa_erlang));
    sa_erlang.sin_family = AF_INET;
    sa_erlang.sin_port = htons(PORT);
    sa_erlang.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(erlang_sock, (struct sockaddr *)&sa_erlang, sizeof(sa_erlang)) < 0) {
        error("Error intentando asignar una dirección al socket de Erlang");
        return FAIL;
    }

    if (listen(erlang_sock, 10) < 0) {
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
    if (add_descriptor(public_sock) < 0)
        return FAIL;

    if (add_descriptor(erlang_sock) < 0)
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
            if (events[i].data.fd == public_sock) {
                // TODO (para equipo): necesitamos saber la información que nos devuelve accept? (1)
                int connect_public_sock = accept(public_sock, NULL, NULL);
                if (connect_public_sock == -1) {
                    error("Error intentando aceptar un agente");
                    exit(EXIT_FAILURE);
                }

                // TODO: make connect_public_sock non-blocking
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = connect_public_sock;
                if (add_descriptor(connect_public_sock) < 0)
                    exit(EXIT_FAILURE);
            } else if (events[i].data.fd == erlang_sock) {
                // TODO (para equipo): misma pregunta que (1)
                int connect_erlang_sock = accept(erlang_sock, NULL, NULL);
                if (connect_erlang_sock == -1) {
                    error("Error intentando aceptar un agente");
                    exit(EXIT_FAILURE);
                }

                // TODO: make connect_erlang_sock non-blocking
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.fd = connect_erlang_sock;
                if (add_descriptor(connect_erlang_sock) < 0)
                    exit(EXIT_FAILURE);
            } else {
                // TODO: process event
            }
        }
    }
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
    if (close(erlang_sock) < 0) {
        error("Error intentando cerrar el socket de Erlang");
        return FAIL;
    }

    if (close(public_sock) < 0) {
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
