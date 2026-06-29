#ifndef TYPES_H
#define TYPES_H

#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <sys/types.h>

#include "const.h"
#include "ds/hashmap.h"
#include "ds/queue.h"
#include "ds/list.h"

// Recursos de un nodo
typedef struct _LocalResources {
    int current_cpu;
    int current_mem;
    int current_gpu;

    int cpu;
    int mem;
    int gpu;
} LocalResources;

typedef struct Client {
    int fd;
    ConnType type;
    char recv_buffer[BUFFER_MAX_SIZE];
    int recv_len;
} Client;

// Informacion con respecto a la conexion en la red
typedef struct ConnectionInfo {
    char *ip;
    int port;
} ConnectionInfo;

// Guarda los recursos alocados en un nodo remoto
typedef struct _RemoteAllocation {
    ConnectionInfo connection_info;
    LocalResources resources;
    int remote_agent_fd;
} RemoteAllocation;

// Elemento de la tabla de nodos
typedef struct _NodeMapCell {
    ConnectionInfo connection_info;
    int socket_fd;
    LocalResources resources;
    time_t time_when_called;
} NodeMapCell;

// Elemento de la tabla de jobs activos
typedef struct _JobMapCell {
    long long job_id;
    int num_remotely_allocated;
    RemoteAllocation *remote_allocations; // arreglo con todos los recursos alocados
    LocalResources granted_resources;
} JobMapCell;

// Guarda datos de una petición hecha por un agente C
typedef struct _Request {
    long long job_id;
    RequestKind kind;
    ResourceKind res_kind;
    int amount;
} Request;

// Guarda la información de un nodo en una petición hecha
// por el cliente Erlang
typedef struct _NodeAllocationInfo {
    ConnectionInfo erlang_connection_info;
    int agent_fd;  // Guarda la conexión con el agente C
    ResourceKind res_kind;
    int amount;
} NodeAllocationInfo;

// Guarda datos de una petición hecha por un cliente Erlang
typedef struct _ErlangRequest {
    int erlang_fd;  // Guarda la conexión con el planificador de Erlang
    long long job_id;
    NodeAllocationInfo *node_allocations;
    int num_allocations;
} ErlangRequest;

// Elemento de la cola de jobs en espera
typedef struct JobQueueData {
    ErlangRequest request;
    time_t time_when_alloc;
} JobQueueData;

// Par con un mutex y una variable de condición
typedef struct MutexCond {
    pthread_mutex_t mutex;
    pthread_cond_t nonempty_queue_cond;
} MutexCond;

// Estructura que representa un job de erlang antes de estar granteado/denegado
typedef struct PendingJob {
    long long job_id;
    int erlang_socket;
    int total_resources_needed;
    int resources_granted;
    int status; // 0 -> Pendiente, 1 -> Listo
    int num_remote_allocated;
    LocalResources my_resources_granted;        // Recursos alocados al job pendiente del nodo local
    RemoteAllocation *remote_allocations;       // Recursos alocados al job pendiente de nodos remotos
} PendingJob;


// Guarda el estado global del agente
typedef struct AgentState {
    int agent_port;
    LocalResources node_resources;
    Hashmap node_map, job_map;
    Queue job_queue;
    List pending_jobs;
    List timed_out_jobs;
    MutexCond protection; // Usamos esto para manipular job_queue atomicamente
    pthread_mutex_t res_protection;
} AgentState;

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
int job_cmp(JobQueueData *j1, JobQueueData *j2);
PendingJob *pending_job_copy(PendingJob *p);
void pending_job_free(PendingJob *p);
long long *int_copy(long long *x);
int parse_request(Request *req, char **request_fields, int n_fields);
void request_free(Request *req);
int parse_erlang_request(ErlangRequest *erl,
                         Hashmap node_map,
                         char **request_fields,
                         int request_fields_size, int fd);

#endif // TYPES_H
