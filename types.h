#ifndef TYPES_H
#define TYPES_H

#include <pthread.h>
#include <time.h>
#include "const.h"
#include "ds/hashmap.h"

// Local node resources
typedef struct _LocalResources {
    int current_cpu;
    int current_mem;
    int current_gpu;

    int cpu;
    int mem;
    int gpu;
} LocalResources;

// Represents the allocated resources in a remote node
typedef struct _RemoteAllocation {
    char ip[16];
    int port;
    LocalResources resources;
} RemoteAllocation;

// Cell of node_map
typedef struct _NodeMapCell {
    char *ip;
    int port;
    int socket_fd;
    LocalResources resources;
    time_t time_when_called;
} NodeMapCell;

// Cell of job_map 
typedef struct _JobMapCell {
    long long job_id;
    int num_remotely_allocated;
    RemoteAllocation *remote_allocations; // array of all the remotely allocated resources
    LocalResources granted_resources;
} JobMapCell;

// Represents C agent request
typedef struct _Request {
    long long job_id;
    RequestKind kind;
    ResourceKind res_kind;
    int amount;
} Request;

// Represents a single node inside an Erlang request
typedef struct _NodeAllocationInfo {
    char *ip;
    int port;
    int agent_fd;  // Stores the connection to the C agent
    ResourceKind res_kind;
    int amount;
} NodeAllocationInfo;

// Represents an Erlang request
typedef struct _ErlangRequest {
    int erlang_fd;  // Stores the connection to the Erlang planner
    long long job_id;
    NodeAllocationInfo *node_allocations;
    int num_allocations;
} ErlangRequest;

// The data element of job_queue
typedef struct JobQueueData {
    ErlangRequest request;
    time_t time_when_alloc;
} JobQueueData;

// Pair of mutex and condition variable
typedef struct MutexCond {
    pthread_mutex_t mutex;
    pthread_cond_t nonempty_queue_cond;
} MutexCond;

NodeMapCell *node_cell_copy(NodeMapCell *nmp);
int node_cell_cmp(NodeMapCell *nmp1, NodeMapCell *nmp2);
void node_cell_free(NodeMapCell *nmp);
unsigned int node_cell_hash(NodeMapCell *nmp);
JobMapCell *job_cell_copy(JobMapCell *jmc);
int job_cell_cmp(JobMapCell *jmc1, JobMapCell *jmc2);
void job_cell_free(JobMapCell *jmc);
unsigned int job_cell_hash(JobMapCell *jmc);
JobQueueData *job_copy(JobQueueData *j);
void job_free(JobQueueData *j);
int *int_copy(int *x);
Request parse_request(char **request_fields, int n_fields);
Request *request_dup(Request *req);
void request_free(Request *req);
ErlangRequest parse_erlang_request(Hashmap node_map, char **request_fields,
                                   int request_fields_size, int fd);

#endif // TYPES_H
