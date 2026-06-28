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
#include <fcntl.h>

#include "log/log.h"

void handle_unexpected_disconnection(Hashmap node_map, Hashmap job_map,
                                     LocalResources *node_resources, int fd);

// Manejar requests (si es posible) proveniente de agentes de C
// Si ocurre una desconexión inesperada, se toma en cuenta
void handle_c_agent(int c_agent_fd, int epoll_fd, AgentState *state)
{
    log_message("[C]: Procesando agente C como cliente");
    char buffer[BUFFER_MAX_SIZE];
    memset(buffer, 0, BUFFER_MAX_SIZE);

    // Hacemos temporalmente al socket del agente bloqueante
    int flags = fcntl(c_agent_fd, F_GETFL, 0);
    fcntl(c_agent_fd, F_SETFL, flags & ~O_NONBLOCK);
    // Leemos el mensaje que nos envía el agente de C
    ssize_t bytes_read = read_full_line(c_agent_fd, buffer, BUFFER_MAX_SIZE - 1);
    // Restauramos la propiedad no bloqueante del socket
    fcntl(c_agent_fd, F_SETFL, flags | O_NONBLOCK);
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

        log_message("1");
        Request request;
        int result = parse_request(&request, request_fields, length);

        if (result < 0) {
            log_message("Error en el parseo de la request");
            return;
        } else {
            log_message("2");
            process_request(c_agent_fd, epoll_fd, request, state);
            log_message("3");
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
        increase_resources(&state->node_resources, req.res_kind, req.amount);
        JobMapCell search_job;
        search_job.job_id = req.job_id;
 
        JobMapCell *cell = hashmap_search(state->job_map, &search_job);

        // No encontramos un trabajo con ese id, así que enviamos DENIED
        if (cell == NULL) {
            log_message("[C]: Enviando DENIED al agente con descriptor %d", c_agent_fd);
            sprintf(response_to_agent, "DENIED %lld\n", req.job_id);
            send(c_agent_fd, response_to_agent, strlen(response_to_agent), 0);
            return;
        }

        // El trabajo existe, entonces devolvemos los recursos a ese agente
        // y eliminamos el trabajo en caso de no tener más recursos que usar;
        // De lo contrario, lo ponemos en nuestra tabla de trabajos para seguir
        // usandolo
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
    NodeMapCell *dead_node = NULL;

    // Encontramos el nodo muerto según fd
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
