#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>

#define OK    0
#define FAIL -1
#define PORT  12529

// SOCK_STREAM -> TCP
// SOCK_DGRAM ->  UDP

int public_sock, erlang_sock;

void error(const char *msg);
int init_agents_socket(void);
int init_erlang_socket(void);
int init_sockets(void);
int init_epoll(void);
int startup(void);
int add_descriptors(void);
int close_epoll(void);
int close_sockets(void);

int main()
{
    if (init_sockets() < 0)
        return 1;

    if (init_epoll() < 0)
        return 1;

    if (startup() < 0)
        return 1;

    if (add_descriptors())
        return 1;

    for (;;) {
        // TODO
    }

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

int init_agents_socket(void)
{
    int yes = 1;
    public_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (public_sock < 0) {
        error("Error intentando iniciar el socket para otros agentes");
        return FAIL;
    }

    // Set agent sockets to be reused
    if (setsockopt(public_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == 1) {
        error("No se pudo configurar el socket público para ser reusado");
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_public;
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
    erlang_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (erlang_sock < 0) {
        error("Error intentando iniciar el socket para Erlang");
        return FAIL;
    }

    // Set erlang sockets to be reused
    if (setsockopt(erlang_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == 1) {
        error("No se pudo configurar el socket de Erlang para ser reusado");
        return FAIL;
    }

    // Bind and set socket to listen mode
    struct sockaddr_in sa_erlang;
    sa_erlang.sin_family = AF_UNIX;
    sa_erlang.sin_port = htons(PORT);
    sa_erlang.sin_addr.s_addr = htonl(INADDR_ANY);

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

int init_sockets(void)
{
    if (init_agents_socket() < 0)
        return FAIL;

    if (init_erlang_socket() < 0)
        return FAIL;

    return OK;
}

int init_epoll(void)
{
    assert(0 && "TODO: init_epoll not implemented");
    return OK;
}

int startup(void)
{
    assert(0 && "TODO: startup not implemented");
    return OK;
}

int add_descriptors(void)
{
    assert(0 && "TODO: add_descriptors not implemented");
    return OK;
}

int close_epoll(void)
{
    assert(0 && "TODO: close_epoll not implemented");
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
