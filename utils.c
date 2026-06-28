#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>

#include "const.h"
#include "utils.h"
#include "types.h"
#include "log/log.h"

// Imprime msg junto a un error de mensaje
// determinado por errno
void error(const char *msg)
{
    log_message("%s: %s", msg, strerror(errno));
}

// Guarda tu IP en ip_buffer
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

// Hace a sock no bloqueante con O_NONBLOCK
// https://stackoverflow.com/a/73879155
int set_socket_nonblocking(int sock)
{
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1)
        return FAIL;

    flags |= O_NONBLOCK;
    return fcntl(sock, F_SETFL, flags);
}

// Separa text en multiples strings determinadas por delimiter
char **split(char *text, char *delimiter, int *len)
{
    int cap = 32;
    char **result = malloc(cap*sizeof(char *));

    char *s2 = strdup(text);
    char *tok = strtok(s2, delimiter);
    int i = 0;
    result[i++] = tok;
    while (tok) {
        tok = strtok(NULL, delimiter);
        result[i++] = tok;

        if (i >= cap/2) {
            cap *= 2;
            result = realloc(result, cap*sizeof(char *));
        }
    }

    if (len)
        *len = i - 1;
    return result;
}

// Añade fd a la instancia de epoll
int add_descriptor(int epoll_fd, int fd, struct epoll_event *ev, int op)
{
    struct epoll_event evv;
    evv.events = EPOLLIN | EPOLLET;
    evv.data.fd = fd;
    if (ev == NULL)
        ev = &evv;

    if (epoll_ctl(epoll_fd, op, fd, ev) < 0) {
        error("Error intentando añadir el socket a la instancia de epoll");
        return FAIL;
    }

    return OK;
}

// Lee hasta \n en un stream de datos dado por fd
ssize_t read_full_line(int fd, char *buffer,
                       char *internal_buffer, int *internal_len)
{
    while (1) {
        // Intentamos leer lo que llegue del socket
        char temp[1024];
        ssize_t bytes = recv(fd, temp, sizeof(temp), 0);

        if (bytes == 0) return 0;  // Desconexión
        if (bytes < 0) {
            // Sin más datos por ahora
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }

            return FAIL; // Error real
        }

        // Acumulamos en el buffer interno del descriptor
        if (*internal_len + bytes >= BUFFER_MAX_SIZE)
            return -2; // Error: Buffer overflow

        memcpy(internal_buffer + *internal_len, temp, bytes);
        *internal_len += bytes;
        internal_buffer[*internal_len] = '\0';
    }

    // Ya tenemos una línea completa?
    char *newline = strchr(internal_buffer, '\n');
    if (newline != NULL) {
        size_t line_len = newline - internal_buffer + 1;

        // Copiamos la línea completa al buffer de salida
        memcpy(buffer, internal_buffer, line_len);
        buffer[line_len] = '\0';

        // Movemos el resto de los datos al principio del buffer interno
        int remaining = *internal_len - line_len;
        memmove(internal_buffer, internal_buffer + line_len, remaining);
        *internal_len = remaining;

        return line_len;
    }

    return FAIL; // Seguimos esperando el resto del mensaje
}

// Encuentra el puerto de un nodo con una ip dada
int find_port(Hashmap node_map, char *ip)
{
    int port = -1;
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            if (streq(ip, node->connection_info.ip))
                return node->connection_info.port;
        }
    }

    return port;
}
