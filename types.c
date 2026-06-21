#include <stdlib.h>
#include <string.h>

#include "utils.h"
#include "types.h"

// Start of node_map void* handling logic



// End of node_map void* handling logic

NodeMapCell *node_cell_copy(NodeMapCell* nmc){
    int port = nmc->port;
    int socket_fd = nmc->socket_fd;
    LocalResources resources = nmc->resources;
    time_t time_when_called = nmc->time_when_called;

    NodeMapCell* cloned = malloc(sizeof(NodeMapCell));
    cloned->ip = strdup(nmc->ip);
    cloned->port = port;
    cloned->socket_fd = socket_fd;
    cloned->resources = resources;
    cloned->time_when_called = time_when_called;
    return cloned;
}

int node_cell_cmp(NodeMapCell* nmc1, NodeMapCell* nmc2){
    return !streq(nmc1->ip,nmc2->ip);
}

void node_cell_free(NodeMapCell* nmc){
    free(nmc->ip);
    free(nmc);
}

// Start of job_map void* handling logic
// TODO: Modify job_cell_copy, job_cell_cmp and job_cell_free to comply with ErlangRequest
JobMapCell *job_cell_copy(JobMapCell* jmc){
    long long job_id = jmc->job_id;
    int num_remotely_allocated = jmc->num_remotely_allocated;
    RemoteAllocation *remote_allocations = jmc->remote_allocations;
    LocalResources granted_resources = jmc->granted_resources;

    JobMapCell* cloned = malloc(sizeof(JobMapCell));
    cloned->job_id = job_id;
    cloned->num_remotely_allocated = num_remotely_allocated;
    cloned->remote_allocations = calloc(num_remotely_allocated,sizeof(RemoteAllocation));
    memcpy(cloned->remote_allocations, remote_allocations, num_remotely_allocated);
    return cloned;
}
int job_cell_cmp(JobMapCell* jmc1, JobMapCell* jmc2){
    return jmc1->job_id - jmc2->job_id;

}
void job_cell_free(JobMapCell* jmc){
    free(jmc->remote_allocations);
    free(jmc);
}

// End of job_map void* handling logic

// Start of job_queue void* handling logic

JobQueueData *job_copy(JobQueueData *j)
{
    long long job_id = j->job_cell.job_id;
    int num_remotely_allocated = j->job_cell.num_remotely_allocated;
    RemoteAllocation *remote_allocations = j->job_cell.remote_allocations;
    time_t time_when_alloc = j->time_when_alloc;

    JobQueueData *cloned = malloc(sizeof(JobQueueData));
    cloned->job_cell.job_id = job_id;
    cloned->job_cell.num_remotely_allocated = num_remotely_allocated;
    cloned->job_cell.remote_allocations = calloc(num_remotely_allocated, sizeof(RemoteAllocation));
    memcpy(cloned->job_cell.remote_allocations, remote_allocations, num_remotely_allocated);
    cloned->time_when_alloc = time_when_alloc;
    return cloned;
}

void job_free(JobQueueData *j)
{
    free(j->job_cell.remote_allocations);
    free(j);
}

// End of JobQueueData void* handling logic

int *int_copy(int *x)
{
    int *p = malloc(sizeof(int));
    *p = *x;
    return p;
}

// Given request fields, serializes it into Request data structure
Request parse_request(char **request_fields, int n_fields)
{
    Request req;
    char *req_kind = request_fields[0];
    if (streq(req_kind, "RESERVE"))
        req.kind = REQUEST_KIND_RESERVE;
    else if (streq(req_kind, "RELEASE"))
        req.kind = REQUEST_KIND_RELEASE;

    req.job_id = atoll(request_fields[1]);
    char *resource_name = request_fields[2];
    if (streq(resource_name, "cpu"))
        req.res_kind = RES_KIND_CPU;
    else if (streq(resource_name, "mem"))
        req.res_kind = RES_KIND_MEM;
    else if (streq(resource_name, "gpu"))
        req.res_kind = RES_KIND_GPU;

    req.amount = atoi(request_fields[3]);

    return req;
}

// Allocate memory and copy a request
Request *request_dup(Request *req)
{
    Request *cloned = malloc(sizeof(Request));
    cloned->job_id = req->job_id;
    cloned->kind = req->kind;
    cloned->res_kind = req->res_kind;
    cloned->amount = req->amount;

    return cloned;
}

// Parses a request with the form JOB_REQUEST <job_id> [@ip:res:amount ... ]
// Then, serializes into an ErlangRequest data structure
ErlangRequest parse_erlang_request(Hashmap node_map, char **request_fields, int request_fields_size, int fd)
{
    ErlangRequest erl = { 0 };
    erl.erlang_fd = fd;
    erl.job_id = atoll(request_fields[1]);

    int cap = 256;
    NodeAllocationInfo *nodes = calloc(cap, sizeof(NodeAllocationInfo));
    int nodes_len = 0;

    // Read starting from the array of nodes
    for (int i = 2; i < request_fields_size; ++i) {
        char **node_information = split(request_fields[i], ":", NULL);
        nodes[nodes_len].ip = strdup(node_information[0] + 1);

        // We use this to get the port of the request
        NodeMapCell *cached_node = hashmap_search(node_map, &nodes[nodes_len].ip);
        if (cached_node != NULL)
            nodes[nodes_len].port = cached_node->port;
        else
            nodes[nodes_len].port = DEFAULT_PORT;

        nodes[nodes_len].agent_fd = -1;

        if (streq(node_information[1], "cpu")) {
            nodes[nodes_len].res_kind = RES_KIND_CPU;
            nodes[nodes_len].amount = atoi(node_information[2]);
        } else if (streq(node_information[1], "mem")) {
            nodes[nodes_len].res_kind = RES_KIND_MEM;
            nodes[nodes_len].amount = atoi(node_information[2]);
        } else if (streq(node_information[1], "gpu")) {
            nodes[nodes_len].res_kind = RES_KIND_GPU;
            nodes[nodes_len].amount = atoi(node_information[2]);
        }

        free(node_information);
        ++nodes_len;

        if (nodes_len >= cap/2) {
            cap *= 2;
            nodes = realloc(nodes, cap*sizeof(NodeAllocationInfo));
        }
    }

    erl.num_allocations = nodes_len;
    return erl;
}
