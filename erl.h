#ifndef ERL_H
#define ERL_H

#include "types.h"
#include "ds/queue.h"
#include "ds/list.h"

void handle_erlang_client(int erlang_fd, int epoll_fd, AgentState *state);
void handle_job_request(ErlangRequest erl, int epoll_fd, AgentState *state);
void handle_job_release(ErlangRequest erl, int epoll_fd, AgentState *state);
void handle_job_status(ErlangRequest erl, AgentState *state);
void handle_get_nodes(Hashmap node_map, ErlangRequest erl);

#endif // ERL_H
