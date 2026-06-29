#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "const.h"
#include "utils.h"
#include "types.h"
#include "log/log.h"

unsigned int int_hash(unsigned int x);

// Aloca memoria para un clon de un NodeMapCell
NodeMapCell *node_cell_copy(NodeMapCell *nmc)
{
    int port = nmc->connection_info.port;
    int socket_fd = nmc->socket_fd;
    LocalResources resources = nmc->resources;
    time_t time_when_called = nmc->time_when_called;

    NodeMapCell* cloned = malloc(sizeof(NodeMapCell));
    cloned->connection_info.ip = strdup(nmc->connection_info.ip);
    cloned->connection_info.port = port;
    cloned->socket_fd = socket_fd;
    cloned->resources = resources;
    cloned->time_when_called = time_when_called;
    return cloned;
}

// Compara dos NodeMapCell por ip y puerto
int node_cell_cmp(NodeMapCell *nmc1, NodeMapCell *nmc2)
{
    if (nmc1->connection_info.port != nmc2->connection_info.port)
        return -1;

    return strcmp(nmc1->connection_info.ip, nmc2->connection_info.ip);
}

// Libera de memoria un NodeMapCell
void node_cell_free(NodeMapCell *nmc)
{
    free(nmc->connection_info.ip);
    free(nmc);
}

// Calcula hash sobre la IP y puerto de un NodeMapCell
unsigned int node_cell_hash(NodeMapCell *nmp)
{
    // Hasheamos la IP
    unsigned int ip_hash = 0;
    for (int i = 0; nmp->connection_info.ip[i] != '\0'; ++i)
        ip_hash = 31*ip_hash + nmp->connection_info.ip[i];

    // Hasheamos el puerto
    unsigned int port_hash = int_hash(nmp->connection_info.port);

    // Combinamos los hash, basado en
    // https://www.boost.org/doc/libs/1_53_0/doc/html/hash/reference.html#boost.hash_combine
    return ip_hash ^ (port_hash + 0x9e3779b9u + (ip_hash << 6) + (ip_hash >> 2));
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
    if (remote_allocations)
        memcpy(cloned->remote_allocations, remote_allocations, sizeof(RemoteAllocation)*num_remotely_allocated);
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
    return int_hash(jmc->job_id);
}

// Calcula el hash para enteros
// https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
unsigned int int_hash(unsigned int x)
{
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;

    return x;
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
    memcpy(cloned->request.node_allocations, node_allocations, sizeof(NodeAllocationInfo)*num_allocations);
    cloned->time_when_alloc = time_when_alloc;

    return cloned;
}

// Libera de memoria un JobQueueData
void job_free(JobQueueData *j)
{
    free(j->request.node_allocations);
    free(j);
}

// Compara dos JobQueueData por job id
int job_cmp(JobQueueData *j1, JobQueueData *j2)
{
    return j1->request.job_id - j2->request.job_id;
}

PendingJob *pending_job_copy(PendingJob *p)
{
    PendingJob *cloned = calloc(1, sizeof(PendingJob));
    cloned->job_id = p->job_id;
    cloned->erlang_socket = p->erlang_socket;
    cloned->total_resources_needed = p->total_resources_needed;
    cloned->resources_granted = p->resources_granted;
    cloned->status = p->status;
    cloned->num_remote_allocated = p->num_remote_allocated;
    cloned->my_resources_granted = p->my_resources_granted;
    cloned->remote_allocations = calloc(p->num_remote_allocated, sizeof(RemoteAllocation));
    memcpy(cloned->remote_allocations, p->remote_allocations, sizeof(RemoteAllocation)*p->num_remote_allocated);

    return cloned;
}

void pending_job_free(PendingJob *p)
{
    free(p->remote_allocations);
    free(p);
}

// Copia un entero
long long *int_copy(long long *x)
{
    long long *p = malloc(sizeof(long long));
    *p = *x;
    return p;
}

// Crea una estructura de tipo Request a partir de un arreglo
// de strings representando la petición de un agente C
int parse_request(Request *req, char **request_fields, int n_fields)
{
    char *req_kind = request_fields[0];

    // Determinamos qué tipo de petición se hizo
    if (streq(req_kind, "RESERVE"))
        req->kind = REQUEST_KIND_RESERVE;
    else if (streq(req_kind, "RELEASE"))
        req->kind = REQUEST_KIND_RELEASE;
    else if(streq(req_kind,"GRANTED"))
        req->kind = REQUEST_KIND_GRANTED;
    else if(streq(req_kind,"DENIED"))
        req->kind = REQUEST_KIND_DENIED;
    else
        return FAIL;

    req->job_id = atoll(request_fields[1]);

    char *resource_name = request_fields[2];
    if (streq(resource_name, "cpu"))
        req->res_kind = RES_KIND_CPU;
    else if (streq(resource_name, "mem"))
        req->res_kind = RES_KIND_MEM;
    else if (streq(resource_name, "gpu"))
        req->res_kind = RES_KIND_GPU;

    // Determinamos cuánto se pidió del tipo de recurso
    req->amount = atoi(request_fields[3]);
    return OK;
}

// Parsea una petición del tipo JOB_REQUEST <job_id> [@ip:res:amount ... ]
// para llenar los campos de un ErlangRequest a partir de la petición
int parse_erlang_request(ErlangRequest *erl,
                         Hashmap node_map,
                         char **request_fields,
                         int request_fields_size, int fd)
{
    erl->erlang_fd = fd;
    erl->job_id = atoll(request_fields[1]);

    int cap = 256;
    NodeAllocationInfo *nodes = calloc(cap, sizeof(NodeAllocationInfo));
    int nodes_len = 0;

    // Empezamos desde el arreglo de nodos [@ip:res:amount ... ]
    for (int i = 2; i < request_fields_size; ++i) {
        int length = 0;
        char **node_information = split(request_fields[i], ":", &length);

        // El nodo está mal definido
        if (length < 3) {
            free(node_information);
            free(nodes);
            return FAIL;
        }

        nodes[nodes_len].erlang_connection_info.ip = strdup(node_information[0] + 1);
        nodes[nodes_len].erlang_connection_info.port = find_port(node_map, node_information[0] + 1);
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

    erl->num_allocations = nodes_len;
    erl->node_allocations = calloc(erl->num_allocations, sizeof(NodeAllocationInfo));
    memcpy(erl->node_allocations, nodes, sizeof(NodeAllocationInfo)*erl->num_allocations);
    return OK;
}
