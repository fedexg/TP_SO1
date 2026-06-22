#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <errno.h>
#include <sys/epoll.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <stdlib.h>

#include "erl.h"
#include "types.h"
#include "utils.h"
#include "resources.h"
#include "ds/queue.h"
#include "ds/list.h"

bool find_in_queue(Queue queue, int id);
bool find_in_list(List list, int id);
void delete_from_timed_out_jobs(List timed_out_jobs, int id);
void send_release_to_agent(Hashmap node_map, const char *agent_ip, long long job_id,
                            const char *resource, int amount, int epoll_fd);
int get_agent_connection(const char *ip, int port, int epoll_fd);

// Handles an erlang client connection
void handle_erlang_client(int erlang_fd, int epoll_fd, AgentState *state)
{
    char buffer[BUFFER_MAX_SIZE] = { 0 };
    ssize_t bytes_read = read(erlang_fd, buffer, BUFFER_MAX_SIZE - 1);
    if (bytes_read <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        error("Error intentando leer de un agente de C");
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, erlang_fd, NULL);
        close(erlang_fd);
    } else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);
        ErlangRequest erl = parse_erlang_request(state->node_map,
                                                 request_fields,
                                                 length, erlang_fd);

        if (streq(request_fields[0], "JOB_REQUEST"))
            handle_job_request(erl, epoll_fd, state);
        else if (streq(request_fields[0], "JOB_RELEASE"))
            handle_job_release(erl, epoll_fd, state);
        else if (streq(request_fields[0], "JOB_STATUS"))
            handle_job_status(erl, state);
        else if (streq(request_fields[0], "GET_NODES"))
            handle_get_nodes(state->node_map, erl);
        else {
            char *msg = "Error: comando desconocido";
            send(erlang_fd, msg, strlen(msg), 0);
        }

        free(request_fields);
    }
}

// Handles an incoming job request from the erlang client
void handle_job_request(ErlangRequest erl, int epoll_fd, AgentState *state)
{
    char msg[BUFFER_MAX_SIZE] = { 0 };
    long long job_id = erl.job_id;
    LocalResources res = get_initial_resources(state->node_map);

    int cpu = 0, mem = 0, gpu = 0;

    // Gather total resources from request to determine response
    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo alloc = erl.node_allocations[i];
        if (alloc.res_kind == RES_KIND_CPU)
            cpu += alloc.amount;
        if (alloc.res_kind == RES_KIND_MEM)
            mem += alloc.amount;
        if (alloc.res_kind == RES_KIND_GPU)
            gpu += alloc.amount;
    }

    // Total amount of resources exceeds any resource that the
    // cluster had in the first place. Therefore, job is impossible
    // and we answer a TIMEOUT
    if (cpu > res.cpu || mem > res.mem || gpu > res.gpu) {
        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_TIMEOUT %lld", job_id);
        state->timed_out_jobs = list_append(state->timed_out_jobs, &job_id,
                                            (ListCpyFunc)int_copy);
        send(erl.erlang_fd, msg, strlen(msg), 0);
        return;
    }

    // Ask agents for what we need
    RemoteAllocation *remote_allocs = calloc(erl.num_allocations, sizeof(RemoteAllocation));
    int num_granted = 0;
    List agent_fds = list_make();

    char my_ip[64];
    get_local_ip(my_ip, 64);
    LocalResources local = { 0 };

    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo alloc = erl.node_allocations[i];
        int agent_fd = alloc.agent_fd;
        if (agent_fd == -1) {
            agent_fd = get_agent_connection(alloc.ip, alloc.port, epoll_fd);

            if (agent_fd == -1) {
                error("Error intentando conectarse a un nodo");
                return;
            }
        }

        char msg[BUFFER_MAX_SIZE] = { 0 };
        char resource[4] = { 0 };

        if (alloc.res_kind == RES_KIND_CPU)
            strcpy(resource, "cpu");
        if (alloc.res_kind == RES_KIND_MEM)
            strcpy(resource, "mem");
        if (alloc.res_kind == RES_KIND_GPU)
            strcpy(resource, "gpu");

        // Let's try not to send a message to ourselves...
        if (streq(alloc.ip, my_ip)) {
            if (exists_resource(&state->node_resources, alloc.res_kind, alloc.amount)) {
                give_resources(&state->node_resources, alloc.res_kind, alloc.amount);
                increase_resources(&local, alloc.res_kind, alloc.amount);
                ++num_granted;
            }
        } else {
            // Send RESERVE message based on what we found
            sprintf(msg, "RESERVE %lld %s %d", job_id, resource, alloc.amount);
            send(agent_fd, msg, strlen(msg), 0);

            // Count how many GRANTEDs we received
            char recv_buffer[BUFFER_MAX_SIZE] = { 0 };
            recv(agent_fd, recv_buffer, BUFFER_MAX_SIZE - 1, 0);
            if (strncmp(recv_buffer, "GRANTED", 7) == 0) {
                strcpy(remote_allocs[num_granted].ip, alloc.ip);
                remote_allocs[num_granted].port = alloc.port;
                if (alloc.res_kind == RES_KIND_CPU)
                    remote_allocs[num_granted].resources.current_cpu += alloc.amount;
                if (alloc.res_kind == RES_KIND_MEM)
                    remote_allocs[num_granted].resources.current_gpu += alloc.amount;
                if (alloc.res_kind == RES_KIND_GPU)
                    remote_allocs[num_granted].resources.current_mem += alloc.amount;

                ++num_granted;
                agent_fds = list_append(agent_fds, &agent_fd, (ListCpyFunc)int_copy);
            }
        }
    }

    if (num_granted < erl.num_allocations) {
        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_DENIED %lld", job_id);
        send(erl.erlang_fd, msg, strlen(msg), 0);

        for (ListNode *p = agent_fds; p != NULL; p = p->next) {
            for (int i = 0; i < erl.num_allocations; ++i) {
                NodeAllocationInfo alloc = erl.node_allocations[i];
                if (streq(alloc.ip, my_ip))
                    increase_resources(&state->node_resources, alloc.res_kind, alloc.amount);
                else {
                    if (*(int *)p->data == alloc.agent_fd) {
                        char buffer[BUFFER_MAX_SIZE]  = { 0 };
                        sprintf(buffer, "RELEASE %lld", job_id);
                        send(alloc.agent_fd, buffer, strlen(buffer), 0);
                    }
                }
            }
        }

        free(remote_allocs);
        enqueue(state->job_queue, &erl, (QueueCpyFunc)job_copy);
        pthread_cond_signal(&state->protection.nonempty_queue_cond);
        return;
    }

    JobMapCell *cell = calloc(1, sizeof(JobMapCell));
    cell->job_id = job_id;
    cell->num_remotely_allocated = erl.num_allocations;
    cell->remote_allocations = remote_allocs;
    cell->granted_resources = local;

    hashmap_put(state->job_map, cell);

    memset(msg, 0, BUFFER_MAX_SIZE - 1);
    sprintf(msg, "JOB_GRANTED %lld", job_id);
    send(erl.erlang_fd, msg, strlen(msg), 0);
}

// Handles an incoming job release from the erlang client
void handle_job_release(ErlangRequest erl, int epoll_fd, AgentState *state)
{
    long long job_id = erl.job_id;
    JobMapCell *cell = hashmap_search(state->job_map, &job_id);
    if (cell == NULL) {
        char error_buf[1024] = { 0 };
        sprintf(error_buf, "Error: no existe el job con id %lld", job_id);
        send(erl.erlang_fd, error_buf, strlen(error_buf), 0);
        return;
    }

    increase_resources(&state->node_resources, RES_KIND_CPU,
                       cell->granted_resources.current_cpu);
    increase_resources(&state->node_resources, RES_KIND_MEM,
                       cell->granted_resources.current_mem);
    increase_resources(&state->node_resources, RES_KIND_GPU,
                       cell->granted_resources.current_gpu);

    // Iterate over remotely allocated resources
    for (int i = 0; i < cell->num_remotely_allocated; i++) {
        RemoteAllocation remote = cell->remote_allocations[i];
        if (remote.resources.current_cpu > 0)
            send_release_to_agent(state->node_map, remote.ip, job_id,
                                  "cpu", remote.resources.current_cpu,
                                  epoll_fd);
        if (remote.resources.current_mem > 0)
            send_release_to_agent(state->node_map, remote.ip, job_id,
                                  "mem", remote.resources.current_mem,
                                  epoll_fd);
        if (remote.resources.current_gpu > 0)
            send_release_to_agent(state->node_map, remote.ip, job_id,
                                  "gpu", remote.resources.current_gpu,
                                  epoll_fd);
    }

    free(cell->remote_allocations);
    hashmap_delete(state->job_map, &job_id);
}

// Handles an incoming job status from the erlang client
void handle_job_status(ErlangRequest erl, AgentState *state)
{
    long long job_id = erl.job_id;
    JobMapCell *job = hashmap_search(state->job_map, &job_id);
    char buffer[BUFFER_MAX_SIZE] = { 0 };

    if (job != NULL) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_GRANTED %lld", job_id);
    } else if (find_in_queue(state->job_queue, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_DENIED %lld", job_id);
    } else if (find_in_list(state->timed_out_jobs, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_TIMEOUT %lld", job_id);
        delete_from_timed_out_jobs(state->timed_out_jobs, job_id);
    }

    send(erl.erlang_fd, buffer, strlen(buffer), 0);
}

// Given a queue of jobs, determines if there's a job with a given id
bool find_in_queue(Queue queue, int id)
{
    for (QueueNode* actual_node = queue; actual_node != NULL; actual_node = actual_node->next) {
        JobQueueData *job = (JobQueueData *)actual_node->data;
        if (job->request.job_id == id)
            return true;
    }

    return false;
}

// Given a list of job ids, determines if there's a id is in the list
bool find_in_list(List list, int id)
{
    for (ListNode *actual_node = list; actual_node != NULL; actual_node = actual_node->next) {
        int job_id = *(int *)actual_node->data;
        if (job_id == id)
            return true;
    }

    return false;
}

// Delete a job with given id from timed out jobs list
void delete_from_timed_out_jobs(List timed_out_jobs, int id)
{
    ListNode *node_before = timed_out_jobs;
    if (*(int*)node_before->data == id){
        free(node_before->data);
        timed_out_jobs = timed_out_jobs->next;
        free(node_before);
    } else {
        ListNode *actual_node = timed_out_jobs->next;
        for(; *(int*)actual_node->data != id && actual_node != NULL; actual_node = actual_node->next)
            node_before = node_before->next;
        if (actual_node != NULL) {
            node_before->next = actual_node->next;
            free(actual_node->data);
            free(actual_node);
        }
    }
}

// Handles an incoming get nodes from the erlang client
void handle_get_nodes(Hashmap node_map, ErlangRequest erl)
{
    // Format: NODES IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3;IP:PORT:cpu:NUM4:mem:NUM5:gpu:NUM6; ...
    int buffer_cap = 1024;
    char *buffer = calloc(buffer_cap, sizeof(char));
    strcpy(buffer, "NODES ");

    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            char node_buffer[1024] = { 0 };
            snprintf(node_buffer, sizeof(node_buffer), "%s:%d:cpu:%d:mem:%d:gpu:%d;",
                     node->ip,
                     node->port,
                     node->resources.cpu,
                     node->resources.mem,
                     node->resources.gpu);

            // If the new string won't fit, grow the buffer dynamically
            while (strlen(buffer) + strlen(node_buffer) >= buffer_cap) {
                buffer_cap *= 2;
                char *new_buffer = realloc(buffer, buffer_cap);
                buffer = new_buffer;
            }

            // Append what we wrote to the result buffer
            strcat(buffer, node_buffer);
        }
    }

    send(erl.erlang_fd, buffer, strlen(buffer), 0);
    free(buffer);
}

// Sends a RELEASE message to the C agent specified
void send_release_to_agent(Hashmap node_map, const char *agent_ip, long long job_id,
                            const char *resource, int amount, int epoll_fd)
{
    NodeMapCell *target_node = hashmap_search(node_map, &agent_ip);

    // This node is not in the cluster, so we're done
    if (target_node == NULL)
        return;

    int fd = get_agent_connection(target_node->ip, target_node->port, epoll_fd);
    if (fd == -1) {
        error("Error intentando conectarse a un nodo");
        return;
    }

    // We won't close fd because we want to keep it alive in the epoll instance
    char release_msg[BUFFER_MAX_SIZE];
    snprintf(release_msg, sizeof(release_msg), "RELEASE %lld %s %d", job_id, resource, amount);
    send(fd, release_msg, strlen(release_msg), 0);
}

// Get file descriptor of agent which we need to send messages to
int get_agent_connection(const char *ip, int port, int epoll_fd)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return FAIL;

    set_socket_nonblocking(sock);

    // Set up socket and get address information
    struct sockaddr_in agent_addr;
    memset(&agent_addr, 0, sizeof(agent_addr));
    agent_addr.sin_family = AF_INET;
    agent_addr.sin_port = htons(port);

    inet_pton(AF_INET, ip, &agent_addr.sin_addr);

    // We check if errno is EINPROGRESS because sock is non-blocking.
    int res = connect(sock, (struct sockaddr *)&agent_addr, sizeof(agent_addr));
    if (res < 0 && errno != EINPROGRESS) {
        close(sock);
        return FAIL;
    }

    // Add to epoll instance to listen for events
    if (add_descriptor(epoll_fd, sock) < 0) {
        close(sock);
        return FAIL;
    }

    return sock;
}
