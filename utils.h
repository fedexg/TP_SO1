#ifndef UTILS_H
#define UTILS_H

void error(const char *msg);
int get_local_ip(char *ip_buffer, int buffer_size);
int set_socket_nonblocking(int sock);
char **split(char *text, char *delimiter, int *len);
int add_descriptor(int epoll_fd, int fd);

#endif // UTILS_H
