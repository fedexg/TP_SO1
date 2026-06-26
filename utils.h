#ifndef UTILS_H
#define UTILS_H

#include <sys/epoll.h>

#include "ds/hashmap.h"

void error(const char *msg);
int get_local_ip(char *ip_buffer, int buffer_size);
int set_socket_nonblocking(int sock);
char **split(char *text, char *delimiter, int *len);
int add_descriptor(int epoll_fd, int fd, struct epoll_event *ev);
ssize_t read_full_line(int fd, char *buffer, int max_size);
int find_port(Hashmap node_map, char *ip);

#endif // UTILS_H
