#ifndef AGENTS_H
#define AGENTS_H

#include "types.h"
#include "ds/queue.h"
#include "ds/list.h"

void handle_c_agent(int c_agent_fd,
                    Hashmap node_map, Hashmap job_map,
                    Queue job_queue, List timed_out_jobs,
                    LocalResources *node_resources,
                    MutexCond protection,
                    int epoll_fd);
void process_request(Hashmap node_map, Hashmap job_map,
                     Request req, LocalResources *node_resources,
                     List timed_out_jobs, Queue job_queue,
                     MutexCond protection,
                     int fd, int epoll_fd);
void release_affected_jobs(NodeMapCell *node,
                           Hashmap node_map, Hashmap job_map,
                           LocalResources *node_resources);

#endif // AGENTS_H
