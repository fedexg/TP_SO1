#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <errno.h>

#include "agents.h"
#include "utils.h"
#include "resources.h"
#include "types.h"
#include "erl.h"

void handle_unexpected_disconnection(Hashmap node_map, Hashmap job_map,
                                     LocalResources *node_resources, int fd);

void handle_c_agent(int c_agent_fd,
                    Hashmap node_map, Hashmap job_map,
                    Queue job_queue,
                    LocalResources *node_resources,
                    int epoll_fd)
{
    char buffer[BUFFER_MAX_SIZE];
    memset(buffer, 0, BUFFER_MAX_SIZE);
    ssize_t bytes_read = read(c_agent_fd, buffer, BUFFER_MAX_SIZE - 1);
    if (bytes_read <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        error("Error intentando leer de un agente de C");

        handle_unexpected_disconnection(node_map, job_map, node_resources, c_agent_fd);

        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c_agent_fd, NULL);
        close(c_agent_fd);
    } else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);
        Request request = parse_request(request_fields, length);
        process_request(node_map, job_map,
                        request, node_resources, job_queue,
                        c_agent_fd, epoll_fd);
        free(request_fields);
    }
}

// Given a request and a file descriptor, processes the agent c request.
void process_request(Hashmap node_map, Hashmap job_map,
                     Request req, LocalResources *node_resources, Queue job_queue,
                     int fd, int epoll_fd)
{
    char response_to_agent[128] = {0};
    switch (req.kind) {
    case REQUEST_KIND_RESERVE:
        if (exists_resource(node_resources, req.res_kind, req.amount)) {
            JobMapCell *cell = hashmap_search(job_map, &req.job_id);

            // We didn't find a cell with this job id. We create it.
            if (cell == NULL)
                cell = calloc(1, sizeof(JobMapCell));

            increase_resources(&(cell->granted_resources),req.res_kind, req.amount);
            hashmap_put(job_map, &cell);
            give_resources(node_resources, req.res_kind, req.amount);
            sprintf(response_to_agent, "GRANTED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
        } else {
            sprintf(response_to_agent, "DENIED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
        }

        break;

    case REQUEST_KIND_RELEASE:
        increase_resources(node_resources, req.res_kind, req.amount);
        JobMapCell *cell = hashmap_search(job_map, &req.job_id);

        // Cell not found, therefore no job with that id was found.
        if (cell == NULL) {
            sprintf(response_to_agent, "DENIED %lld", req.job_id);
            send(fd, response_to_agent, 8, 0);
            return;
        }

        give_resources(&(cell->granted_resources), req.res_kind, req.amount);
        if (cell->granted_resources.current_cpu == 0 && cell->granted_resources.current_gpu == 0 &&
            cell->granted_resources.current_mem) {
            hashmap_delete(job_map, &req.job_id);
            free(cell);
        } else
            hashmap_put(job_map, &cell);

        while (!queue_empty(job_queue)) {
            ErlangRequest erl = *(ErlangRequest *)queue_head(job_queue);
            job_queue = dequeue(job_queue, (QueueFreeFunc)job_free);
            handle_job_request(node_map, job_queue, erl, epoll_fd);
        }

        break;
    default:
        break;
    }
}

void release_affected_jobs(NodeMapCell *node, Hashmap node_map, Hashmap job_map,
                           LocalResources *node_resources)
{
    for (int i = 0; i < job_map->cap; ++i) {
        bool exists_job = job_map->items[i].data != NULL && !job_map->items[i].deleted;
        if (exists_job) {
            JobMapCell *job = (JobMapCell *)job_map->items[i].data;
            bool affected_job = false;

            // Check if job depended on dead node
            for (int j = 0; j < job->num_remotely_allocated; ++j) {
                RemoteAllocation *remote = &job->remote_allocations[j];
                if (streq(remote->ip, node->ip) &&
                        ((remote->resources.current_cpu > 0) ||
                        (remote->resources.current_mem > 0)  ||
                        (remote->resources.current_gpu > 0))) {
                    affected_job = true;
                    break;
                }
            }

            // Return resources to affected nodes
            if (affected_job) {
                for (int j = 0; j < job->num_remotely_allocated; ++j) {
                    RemoteAllocation *remote = &job->remote_allocations[j];

                    // If it's a non-dead node, restore its resources
                    if (!streq(remote->ip, dead_node->ip)) {
                        NodeMapCell *alive_node = hashmap_search(node_map, &remote->ip);
                        if (alive_node != NULL) {
                            alive_node->resources.current_cpu += remote->resources.current_cpu;
                            alive_node->resources.current_mem += remote->resources.current_mem;
                            alive_node->resources.current_gpu += remote->resources.current_gpu;
                        }
                    }

                    memset(&remote->resources, 0, sizeof(LocalResources));
                }

                if (job->granted_resources.current_cpu > 0)
                    give_resources(node_resources, RES_KIND_CPU, job->granted_resources.current_cpu);
                if (job->granted_resources.current_mem > 0)
                    give_resources(node_resources, RES_KIND_MEM, job->granted_resources.current_mem);
                if (job->granted_resources.current_gpu > 0)
                    give_resources(node_resources, RES_KIND_GPU, job->granted_resources.current_gpu);

                // Delete job
                long long delete_id = job->job_id;
                free(job->remote_allocations);
                hashmap_delete(job_map, &delete_id);
            }
        }
    }
}

void handle_unexpected_disconnection(Hashmap node_map, Hashmap job_map,
                                     LocalResources *node_resources, int fd)
{
    NodeMapCell *dead_node = NULL;

    // Find dead node file descriptor
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL && !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            if (node->socket_fd == fd) {
                dead_node = node;
                break;
            }
        }
    }

    if (dead_node == NULL)
        return;

    release_affected_jobs(dead_node, node_map, job_map, node_resources);

    hashmap_delete(node_map, &dead_node->ip);
    free(dead_node);
}
