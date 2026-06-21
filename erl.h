#ifndef ERL_H
#define ERL_H

#include "types.h"
#include "ds/queue.h"
#include "ds/list.h"

void handle_erlang_client(Hashmap node_map, Hashmap job_map,
                          Queue job_queue, List timed_out_jobs,
                          LocalResources *node_resources,
                          MutexCond protection,
                          int erlang_fd, int epoll_fd);
void handle_job_request(Hashmap node_map, Hashmap job_map,
                        LocalResources *node_resources,
                        Queue job_queue, ErlangRequest erl,
                        MutexCond protection,
                        int epoll_fd);
void handle_job_release(Hashmap node_map, Hashmap job_map,
                        LocalResources *node_resources, ErlangRequest erl,
                        int epoll_fd);
void handle_job_status(ErlangRequest erl, Hashmap job_map,
                       Queue job_queue, List timed_out_jobs);
void handle_get_nodes(Hashmap node_map, ErlangRequest erl);

#endif // ERL_H
