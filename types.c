#include <stdlib.h>
#include <string.h>

#include "utils.h"
#include "types.h"

// Aloca memoria para un clon de un NodeMapCell
NodeMapCell *node_cell_copy(NodeMapCell *nmc)
{
    int port = nmc->node_connection_info.port;
    int socket_fd = nmc->socket_fd;
    LocalResources resources = nmc->resources;
    time_t time_when_called = nmc->time_when_called;

    NodeMapCell* cloned = malloc(sizeof(NodeMapCell));
    cloned->node_connection_info.ip = strdup(nmc->node_connection_info.ip);
    cloned->node_connection_info.port = port;
    cloned->socket_fd = socket_fd;
    cloned->resources = resources;
    cloned->time_when_called = time_when_called;
    return cloned;
}

// Compara dos NodeMapCell por ip y puerto
int node_cell_cmp(NodeMapCell *nmc1, NodeMapCell *nmc2)
{
    return strcmp(nmc1->node_connection_info.ip, nmc2->node_connection_info.ip) && (nmc1->node_connection_info.port == nmc2->node_connection_info.port);
}

// Libera de memoria un NodeMapCell
void node_cell_free(NodeMapCell *nmc)
{
    free(nmc->node_connection_info.ip);
    free(nmc);
}

// Calcula K&R hash sobre la IP de un NodeMapCell
unsigned int node_cell_hash(NodeMapCell *nmp)
{
    unsigned int h = 0;
    for (int i = 0; nmp->node_connection_info.ip[i] != '\0'; ++i)
        h = 31*h + nmp->node_connection_info.ip[i];

    return h;
}

// Aloca memoria para un clon de un JobMapCell
JobMapCell *job_cell_copy(JobMapCell *jmc)
{
    long long job_id = jmc->job_id;
    int num_remotely_allocated = jmc->num_remotely_allocated;
    RemoteAllocation *remote_allocations = jmc->remote_allocations;
    LocalResources granted_resources = jmc->granted_resources;

    JobMapCell* cloned = malloc(sizeof(JobMapCell));
    cloned->job_id = job_id;
    cloned->num_remotely_allocated = num_remotely_allocated;
    cloned->remote_allocations = calloc(num_remotely_allocated, sizeof(RemoteAllocation));
    memcpy(cloned->remote_allocations, remote_allocations, num_remotely_allocated);
    cloned->granted_resources = granted_resources;
    return cloned;
}

// Compara dos JobMapCell por job_id
int job_cell_cmp(JobMapCell *jmc1, JobMapCell *jmc2)
{
    return jmc1->job_id - jmc2->job_id;
}

// Libera de memoria un JobMapCell
void job_cell_free(JobMapCell* jmc)
{
    free(jmc->remote_allocations);
    free(jmc);
}

// Calcula el hash de un JobMapCell segun su job_id
unsigned int job_cell_hash(JobMapCell *jmc)
{
    return 8191*jmc->job_id;
}

// Aloca memoria para un clon de un JobQueueData
JobQueueData *job_copy(JobQueueData *j)
{
    int erlang_fd = j->request.erlang_fd;
    long long job_id = j->request.job_id;
    int num_allocations = j->request.num_allocations;
    NodeAllocationInfo *node_allocations = j->request.node_allocations;
    time_t time_when_alloc = j->time_when_alloc;

    JobQueueData *cloned = malloc(sizeof(JobQueueData));
    cloned->request.erlang_fd = erlang_fd;
    cloned->request.job_id = job_id;
    cloned->request.num_allocations = num_allocations;
    cloned->request.node_allocations = calloc(num_allocations, sizeof(NodeAllocationInfo));
    memcpy(cloned->request.node_allocations, node_allocations, num_allocations);
    cloned->time_when_alloc = time_when_alloc;

    return cloned;
}

// Libera de memoria un JobQueueData
void job_free(JobQueueData *j)
{
    free(j->request.node_allocations);
    free(j);
}

// Copia un entero
int *int_copy(int *x)
{
    int *p = malloc(sizeof(int));
    *p = *x;
    return p;
}

// Crea una estructura de tipo Request a partir de un arreglo
// de strings representando la petición de un agente C
Request parse_request(char **request_fields, int n_fields)
{
    Request req;
    char *req_kind = request_fields[0];

    // Determinamos qué tipo de petición se hizo
    if (streq(req_kind, "RESERVE"))
        req.kind = REQUEST_KIND_RESERVE;
    else if (streq(req_kind, "RELEASE"))
        req.kind = REQUEST_KIND_RELEASE;

    // Determinamos qué tipo de recurso se pide
    req.job_id = atoll(request_fields[1]);
    char *resource_name = request_fields[2];
    if (streq(resource_name, "cpu"))
        req.res_kind = RES_KIND_CPU;
    else if (streq(resource_name, "mem"))
        req.res_kind = RES_KIND_MEM;
    else if (streq(resource_name, "gpu"))
        req.res_kind = RES_KIND_GPU;

    // Determinamos cuánto se pidió del tipo de recurso
    req.amount = atoi(request_fields[3]);

    return req;
}

// Parsea una petición del tipo JOB_REQUEST <job_id> [@ip:res:amount ... ]
// para crear un ErlangRequest a partir de la petición
ErlangRequest parse_erlang_request(Hashmap node_map, char **request_fields, int request_fields_size, int fd)
{
    ErlangRequest erl = { 0 };
    erl.erlang_fd = fd;
    erl.job_id = atoll(request_fields[1]);

    int cap = 256;
    NodeAllocationInfo *nodes = calloc(cap, sizeof(NodeAllocationInfo));
    int nodes_len = 0;

    // Empezamos desde el arreglo de nodos [@ip:res:amount ... ]
    for (int i = 2; i < request_fields_size; ++i) {
        char **node_information = split(request_fields[i], ":", NULL);
        nodes[nodes_len].erlang_connection_info.ip = strdup(node_information[0] + 1);

        // Hacemos esto para obtener el puerto de
        // a quién se le pide recursos
        NodeMapCell *cached_node = hashmap_search(node_map, &nodes[nodes_len].erlang_connection_info.ip);
        if (cached_node != NULL)
            nodes[nodes_len].erlang_connection_info.port = cached_node->node_connection_info.port;
        else
            nodes[nodes_len].erlang_connection_info.port = DEFAULT_PORT;

        nodes[nodes_len].agent_fd = -1;

        // Determinamos qué tipo de recurso se pide
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
    erl.node_allocations = calloc(erl.num_allocations, sizeof(NodeAllocationInfo));
    memcpy(erl.node_allocations, nodes, erl.num_allocations);
    return erl;
}
