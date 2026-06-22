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

// Imprime msg junto a un error de mensaje
// determinado por errno
void error(const char *msg)
{
    printf("%s: %s\n", msg, strerror(errno));
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

// Añade fd a la instancia de epoll
int add_descriptor(int epoll_fd, int fd, struct epoll_event *ev)
{
    struct epoll_event evv;
    evv.events = EPOLLIN;
    evv.data.fd = fd;
    if (ev == NULL)
        ev = &evv;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, ev) < 0) {
        error("Error intentando añadir el socket a la instancia de epoll");
        return FAIL;
    }

    return OK;
}
