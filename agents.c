#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <errno.h>

#include "agents.h"
#include "const.h"
#include "ds/list.h"
#include "utils.h"
#include "resources.h"
#include "types.h"
#include "erl.h"
#include <fcntl.h>

#include "log/log.h"

void handle_unexpected_disconnection(Hashmap node_map, Hashmap job_map,
                                     LocalResources *node_resources, int fd);
NodeMapCell *find_node_by_fd(Hashmap node_map, int fd);
PendingJob *find_in_pending_job_list(List list, long long id);

// Manejar requests (si es posible) proveniente de agentes de C
// Si ocurre una desconexión inesperada, se toma en cuenta
void handle_c_agent(int c_agent_fd, char *buffer, ssize_t bytes_read,
                    int epoll_fd, AgentState *state)
{
    log_message("[C]: Procesando agente C como cliente");
    if (bytes_read < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        error("Error intentando leer de un agente de C");
        handle_unexpected_disconnection(state->node_map, state->job_map,
                                        &state->node_resources, c_agent_fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c_agent_fd, NULL);
        close(c_agent_fd);
    } else {
        // Procesamos la petición que nos hizo
        log_message("[C]: Procesando petición enviada por un agente C: %s", buffer);
        int length = 0;
        char **request_fields = split(buffer, " ", &length);

        Request request;
        if (length < 1 || parse_request(&request, request_fields, length) < 0) {
            log_message("Error en el parseo de la request");
            return;
        } else {
            process_request(c_agent_fd, epoll_fd, request, state);
            free(request_fields);
        }
    }
}

// Procesa la petición proveniente de un agente C
void process_request(int c_agent_fd, int epoll_fd, Request req, AgentState *state)
{
    char response_to_agent[128] = {0};
    switch (req.kind) {
    case REQUEST_KIND_RESERVE:
        log_message("[C]: Procesando petición de tipo RESERVE sobre job id %lld", req.job_id);
        log_message("[C]: Reserve: Recursos que tenemos disponibles: %d CPU, %d MEM %d GPU",
                state->node_resources.current_cpu,
                state->node_resources.current_mem,
                state->node_resources.current_gpu);

        // Chequeamos que podemos reservar recursos
        if (exists_resource(&state->node_resources, req.res_kind, req.amount)) {
            JobMapCell search_job;
            search_job.job_id = req.job_id;

            JobMapCell *cell = hashmap_search(state->job_map, &search_job);
            // No encontramos un trabajo con este id, así que lo creamos
            if (cell == NULL) {
                cell = calloc(1, sizeof(JobMapCell));
                cell->job_id = req.job_id;
                cell->remote_allocations = NULL;
                cell->granted_resources.cpu = state->node_resources.cpu;
                cell->granted_resources.mem = state->node_resources.mem;
                cell->granted_resources.gpu = state->node_resources.gpu;
            }

            increase_resources(&cell->granted_resources, req.res_kind, req.amount);
            hashmap_put(state->job_map, cell);
            give_resources(&state->node_resources, req.res_kind, req.amount);
            // Se pudo reservar recursos, así que enviamos GRANTED
            log_message("[C]: Enviando GRANTED al agente con descriptor %d", c_agent_fd);
            sprintf(response_to_agent, "GRANTED %lld\n", req.job_id);
            send(c_agent_fd, response_to_agent, strlen(response_to_agent), 0);
        } else { // En caso contrario, enviamos DENIED
            log_message("[C]: Enviando DENIED al agente con descriptor %d", c_agent_fd);
            sprintf(response_to_agent, "DENIED %lld\n", req.job_id);
            send(c_agent_fd, response_to_agent, strlen(response_to_agent), 0);
        }

        break;

    case REQUEST_KIND_RELEASE:
        log_message("[C]: Procesando petición de tipo RELEASE sobre job id %lld", req.job_id);
        // Retomamos los recursos que dimos al agente que pidió recursos
        JobMapCell search_job;
        search_job.job_id = req.job_id;
 
        JobMapCell *cell = hashmap_search(state->job_map, &search_job);

        // No participamos en el job, entonces no hacemos nada
        if (cell == NULL) {
            return;
        }

        // El trabajo existe, entonces devolvemos los recursos nuestro agente,
        // modificamos la tabla de jobs para reflejar que ahora damos menos recursos
        // y eliminamos el trabajo en caso de no tener más recursos que usar;
        // De lo contrario, lo ponemos en nuestra tabla de trabajos para seguir
        // usandolo
        increase_resources(&state->node_resources, req.res_kind, req.amount);
        give_resources(&cell->granted_resources, req.res_kind, req.amount);
        if (cell->granted_resources.current_cpu == 0 && cell->granted_resources.current_gpu == 0 &&
            cell->granted_resources.current_mem == 0) {
            log_message("[C]: El job no tiene más recursos que pedir; eliminando job de la tabla de nodos");
            hashmap_delete(state->job_map, &search_job);
        } else {
            log_message("[C]: Actualizando job en la tabla de nodos");
            hashmap_put(state->job_map, cell);
        }

        // Atendemos solicitudes encoladas en orden
        while (!queue_empty(state->job_queue)) {
            // Debemos proteger la cola para evitar conflictos con
            // worker_thread_handler en main
            JobQueueData *job = (JobQueueData *)queue_head(state->job_queue);

            pthread_mutex_unlock(&state->protection.mutex);
            handle_job_request(job->request, job->time_when_alloc, epoll_fd, state);

            pthread_mutex_lock(&state->protection.mutex);
            state->job_queue = dequeue(state->job_queue, (QueueFreeFunc)job_free);
        }

        pthread_mutex_unlock(&state->protection.mutex);

        break;
    case REQUEST_KIND_GRANTED: {
        PendingJob* pending_job = find_in_pending_job_list(state->pending_jobs,req.job_id); // TODO: implementar esta funcion (te devuelve el void* data de state->pending_jobs)
        pending_job->resources_granted++;
        if (pending_job->resources_granted == pending_job->total_resources_needed){
            JobMapCell* new_job = calloc(1,sizeof(JobMapCell));
            new_job->granted_resources = pending_job->my_resources_granted;
            new_job->job_id = pending_job->job_id;
            new_job->num_remotely_allocated = pending_job->num_remote_allocated;
            char granted_msg[BUFFER_MAX_SIZE] = { 0 };
            sprintf(granted_msg, "JOB_GRANTED %lld\n", req.job_id);
            send(pending_job->erlang_socket, granted_msg, strlen(granted_msg), 0);
        }
        break;
    }
    case REQUEST_KIND_DENIED: {
        // Si recibimos un denied, ya el trabajo no se va a poder hacer, no importa cuantos granteds recibamos
        PendingJob* pending_job = find_in_pending_job_list(state->pending_jobs,req.job_id); 
        // Liberamos nuestros propios recursos
        pthread_mutex_lock(&state->res_protection);
        if (pending_job->my_resources_granted.current_cpu > 0)
            increase_resources(&state->node_resources, RES_KIND_CPU, pending_job->my_resources_granted.current_cpu);
        if (pending_job->my_resources_granted.current_mem > 0)
            increase_resources(&state->node_resources, RES_KIND_MEM, pending_job->my_resources_granted.current_mem);
        if (pending_job->my_resources_granted.current_gpu > 0)
            increase_resources(&state->node_resources, RES_KIND_GPU, pending_job->my_resources_granted.current_gpu);
        pthread_mutex_unlock(&state->res_protection);
        // Liberamos los recursos remotos (mandamos RELEASE a todos, total si le mandamos RELEASE a un nodo que no dio recursos no deberia pasar nada)
        for (int i = 0; i < pending_job->num_remote_allocated;i++){
            char release_msg[BUFFER_MAX_SIZE] = { 0 };
            if (pending_job->remote_allocations[i].resources.current_cpu > 0) {
                sprintf(release_msg, "RELEASE %lld cpu %d\n", req.job_id, req.amount);
                send(pending_job->erlang_socket, release_msg, strlen(release_msg), 0);
            } else if (pending_job->remote_allocations[i].resources.current_mem > 0) {
                sprintf(release_msg, "RELEASE %lld mem %d\n", req.job_id, req.amount);
                send(pending_job->erlang_socket, release_msg, strlen(release_msg), 0);
            } else if (pending_job->remote_allocations[i].resources.current_gpu > 0) {
                sprintf(release_msg, "RELEASE %lld gpu %d\n", req.job_id, req.amount);
                send(pending_job->erlang_socket, release_msg, strlen(release_msg), 0);
            }
        }
        // Metemos el Job a la queue para que se pueda hacer en otro momento
        pthread_mutex_lock(&state->protection.mutex);
        JobQueueData* job_to_enqueue = calloc(1,sizeof(JobQueueData));
        job_to_enqueue->request.erlang_fd = pending_job->erlang_socket;
        job_to_enqueue->request.job_id = req.job_id;
        job_to_enqueue->request.num_allocations = pending_job->num_remote_allocated;
        job_to_enqueue->request.node_allocations = calloc(pending_job->num_remote_allocated, sizeof(NodeAllocationInfo));
        for (int i = 0; i < pending_job->num_remote_allocated; i++) {
            NodeAllocationInfo *alloc = &job_to_enqueue->request.node_allocations[i];
            alloc->erlang_connection_info.ip = strdup(pending_job->remote_allocations[i].connection_info.ip);
            alloc->erlang_connection_info.port = pending_job->remote_allocations[i].connection_info.port;
            alloc->agent_fd = pending_job->remote_allocations[i].remote_agent_fd;

            int cpu = pending_job->remote_allocations[i].resources.current_cpu;
            int mem = pending_job->remote_allocations[i].resources.current_mem;
            int gpu = pending_job->remote_allocations[i].resources.current_gpu;
            if (cpu > 0) {
                alloc->res_kind = RES_KIND_CPU;
                alloc->amount = cpu;
            } else if (mem > 0) {
                alloc->res_kind = RES_KIND_MEM;
                alloc->amount = mem;
            } else if (gpu > 0) {
                alloc->res_kind = RES_KIND_GPU;
                alloc->amount = gpu;
            }       
        }
        job_to_enqueue->time_when_alloc = time(NULL);
        pthread_mutex_unlock(&state->protection.mutex);

        // Notificamos al erlang que el job no se pudo hacer
        char denied_msg[BUFFER_MAX_SIZE] = { 0 };
        sprintf(denied_msg, "JOB_DENIED %lld\n", req.job_id);
        send(pending_job->erlang_socket, denied_msg, strlen(denied_msg), 0);
        break;
    }
    default:
        break;
    }
}

// Maneja la liberación de recursos afectados por un nodo muerto
void release_affected_jobs(NodeMapCell *node, Hashmap node_map, Hashmap job_map,
                           LocalResources *node_resources)
{
    log_message("[C]: Liberando jobs dependientes del nodo con IP %s:%d",
                node->connection_info.ip,
                node->connection_info.port);

    // Buscamos jobs que dependan de node, y si es así, liberamos sus recursos
    // y los sacamos de la tabla de trabajos
    for (int i = 0; i < job_map->cap; ++i) {
        bool exists_job = job_map->items[i].data != NULL && !job_map->items[i].deleted;
        if (!exists_job)
            continue;

        JobMapCell *job = (JobMapCell *)job_map->items[i].data;
        bool affected_job = false;

        // Chequeamos que el job dependa de node
        for (int j = 0; j < job->num_remotely_allocated; ++j) {
            RemoteAllocation *remote = &job->remote_allocations[j];
            if (streq(remote->connection_info.ip,
                      node->connection_info.ip) &&
                    ((remote->resources.current_cpu > 0) ||
                    (remote->resources.current_mem > 0)  ||
                    (remote->resources.current_gpu > 0))) {
                affected_job = true;
                break;
            }
        }

        // Devolvemos los recursos a nodos con trabajos afectados
        if (affected_job) {
            for (int j = 0; j < job->num_remotely_allocated; ++j) {
                RemoteAllocation *remote = &job->remote_allocations[j];

                // Si no está muerto, restauramos los recursos
                if (!streq(remote->connection_info.ip,
                           node->connection_info.ip)) {
                    NodeMapCell *alive_node = hashmap_search(node_map, &remote->connection_info);
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

            // Borramos el job afectado
            JobMapCell search_job;
            search_job.job_id = job->job_id;
            log_message("[C]: Eliminando job con id %lld de la tabla de jobs", job->job_id);
            hashmap_delete(job_map, &search_job);
        }
    }

    log_message("[C]: Se liberaron exitosamente los jobs dependientes");
}

// Maneja desconexiones inesperadas
void handle_unexpected_disconnection(Hashmap node_map, Hashmap job_map,
                                     LocalResources *node_resources, int fd)
{
    NodeMapCell *dead_node = find_node_by_fd(node_map, fd);

    // No hay desconexión inesperada; proseguimos
    if (dead_node == NULL)
        return;

    log_message("[C]: Manejando conexión inesperada del nodo %s:%d",
                dead_node->connection_info.ip,
                dead_node->connection_info.port);

    // Liberamos los recursos de los jobs que dependan de este nodo
    release_affected_jobs(dead_node, node_map, job_map, node_resources);

    log_message("[C]: Eliminando el nodo de la tabla de nodos");
    hashmap_delete(node_map, &dead_node->connection_info);
    log_message("[C]: Se eliminó exitosamente el nodo");
}

NodeMapCell *find_node_by_fd(Hashmap node_map, int fd)
{
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL && !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            if (node->socket_fd == fd)
                return node;
        }
    }

    return NULL;
}

PendingJob *find_in_pending_job_list(List list, long long id)
{
    for (ListNode *actual_node = list; actual_node != NULL; actual_node = actual_node->next) {
        long long job_id = *(long long *)actual_node->data;
        if (job_id == id)
            return (PendingJob *)actual_node->data;
    }

    return NULL;
}