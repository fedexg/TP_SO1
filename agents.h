#ifndef AGENTS_H
#define AGENTS_H

#include "types.h"
#include "ds/queue.h"
#include "ds/list.h"

void handle_c_agent(int c_agent_fd, int epoll_fd, AgentState *state);
void process_request(int c_agent_fd, int epoll_fd, Request req, AgentState *state);
void release_affected_jobs(NodeMapCell *node,
                           Hashmap node_map, Hashmap job_map,
                           LocalResources *node_resources);

#endif // AGENTS_H
