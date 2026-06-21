#ifndef AGENTS_H
#define AGENTS_H

#include "types.h"
#include "ds/queue.h"

void handle_c_agent(int c_agent_fd,
                    Hashmap node_map, Hashmap job_map,
                    Queue job_queue,
                    LocalResources *node_resources,
                    int epoll_fd);
void process_request(Hashmap node_map, Hashmap job_map, Request req,
                     LocalResources *node_resources, Queue job_queue,
                     int fd, int epoll_fd);
void release_affected_jobs(NodeMapCell *node,
                           Hashmap node_map, Hashmap job_map,
                           LocalResources *node_resources);

#endif // AGENTS_H
