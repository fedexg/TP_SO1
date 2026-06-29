#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <errno.h>
#include <sys/epoll.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <fcntl.h>

#include "erl.h"
#include "const.h"
#include "ds/hashmap.h"
#include "types.h"
#include "utils.h"
#include "resources.h"
#include "ds/queue.h"
#include "ds/list.h"

#include "log/log.h"

bool find_in_queue(Queue queue, long long id);
bool find_in_list(List list, long long id);
List delete_from_list_by_id(List list, long long id, ListFreeFunc fr);
void send_release_to_agent(Hashmap node_map, ConnectionInfo conn_info, long long job_id,
                            const char *resource, int amount, int epoll_fd);

// Manejar requests (si es posible) proveniente de un cliente Erlang
void handle_erlang_client(int erlang_fd, char *buffer, ssize_t bytes_read,
                          time_t time, int epoll_fd, AgentState *state)
{
    log_message("[C]: Procesando cliente Erlang con descriptor %d", erlang_fd);

    if (bytes_read < 0) {
        // Error de lectura
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        error("Error intentando leer de un cliente Erlang");
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, erlang_fd, NULL);
        close(erlang_fd);
    } else if (bytes_read == 0) {
        // Falta que lleguen datos
        return;
    } else {
        int length = 0;
        char **request_fields = split(buffer, " ", &length);

        // Si no es una petición que conozcamos,
        // informamos que es desconocida
        if (streq(request_fields[0], "\n") ||
           (!streq(request_fields[0], "JOB_REQUEST") &&
            !streq(request_fields[0], "JOB_RELEASE") &&
            !streq(request_fields[0], "JOB_STATUS") &&
            !streq(request_fields[0], "GET_NODES\n"))) {
            log_message("[C]: Petición desconocida recibida: \"%s\"", request_fields[0]);
            char *msg = "Error: comando desconocido\n";
            send(erlang_fd, msg, strlen(msg), 0);
            return;
        }

        log_message("[C]: Petición recibida de Erlang: %s", buffer);

        if (streq(request_fields[0], "GET_NODES\n")) {
            handle_get_nodes(state->node_map, erlang_fd);
            return;
        }

        ErlangRequest erl;
        int result = parse_erlang_request(&erl, state->node_map,
                                         request_fields,
                                         length, erlang_fd);

        if (result < 0) {
            char *msg = "Error: petición mal formada\n";
            send(erlang_fd, msg, strlen(msg), 0);
            return;
        }

        // Manejamos el tipo de petición que se nos hizo
        if (streq(request_fields[0], "JOB_REQUEST")) {
            if (length < 3) {
                char *msg = "Error: JOB_REQUEST mal formado\n";
                send(erlang_fd, msg, strlen(msg), 0);
                return;
            }
            handle_job_request(erl, time, epoll_fd, state);
        } else if (streq(request_fields[0], "JOB_RELEASE")) {
            if (length < 2) {
                char *msg = "Error: JOB_RELEASE mal formado\n";
                send(erlang_fd, msg, strlen(msg), 0);
                return;
            }
            handle_job_release(erl, epoll_fd, state);
        } else if (streq(request_fields[0], "JOB_STATUS")) {
            if (length < 2) {
                char *msg = "Error: JOB_STATUS mal formado\n";
                send(erlang_fd, msg, strlen(msg), 0);
                return;
            }
            handle_job_status(erl, state);
        }

        free(request_fields);
    }
}

// Maneja una petición del tipo JOB_REQUEST
void handle_job_request(ErlangRequest erl, time_t time_req, int epoll_fd, AgentState *state)
{
    char msg[BUFFER_MAX_SIZE] = { 0 };
    long long job_id = erl.job_id;
    LocalResources res = get_initial_resources(state->node_map);

    log_message("[C]: Procesando petición de recursos sobre job id %lld", job_id);
    log_message("[C]: Request: Recursos que tenemos disponibles: %d CPU, %d MEM %d GPU",
                state->node_resources.current_cpu,
                state->node_resources.current_mem,
                state->node_resources.current_gpu);

    int cpu = 0, mem = 0, gpu = 0;

    // Juntamos todos los recursos de la petición antes de mandar una respuesta
    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo alloc = erl.node_allocations[i];
        if (alloc.res_kind == RES_KIND_CPU)
            cpu += alloc.amount;
        if (alloc.res_kind == RES_KIND_MEM)
            mem += alloc.amount;
        if (alloc.res_kind == RES_KIND_GPU)
            gpu += alloc.amount;
    }

    // La cantidad total de recursos excede cualquier recurso
    // que teníamos inicialmente del cluster. Entonces, el job
    // no se puede hacer y enviamos TIMEOUT
    if (cpu > res.cpu || mem > res.mem || gpu > res.gpu) {
        log_message("[C]: Enviando timeout al cliente Erlang");

        memset(msg, 0, BUFFER_MAX_SIZE - 1);
        sprintf(msg, "JOB_TIMEOUT %lld\n", job_id);

        // Sumamos esta petición a la lista de aquellos que recibieron
        // un TIMEOUT
        pthread_mutex_lock(&state->protection.mutex);
        state->timed_out_jobs = list_append(state->timed_out_jobs, &job_id,
                                            (ListCpyFunc)int_copy);
        pthread_mutex_unlock(&state->protection.mutex);
        send(erl.erlang_fd, msg, strlen(msg), 0);
        return;
    }

    // Necesitamos la IP para no mandarnos
    // mensajes a nosotros mismos
    char my_ip[64];
    get_local_ip(my_ip, 64);
    LocalResources local = { 0 };

    // Creamos el nuevo PendingJob
    PendingJob *new_pending_job = calloc(1, sizeof(PendingJob));
    new_pending_job->job_id = erl.job_id;
    new_pending_job->erlang_socket = erl.erlang_fd;
    new_pending_job->total_resources_needed = erl.num_allocations;
    new_pending_job->resources_granted = 0;
    new_pending_job->status = 0;
    new_pending_job->num_remote_allocated = 0;
    new_pending_job->remote_allocations = calloc(erl.num_allocations, sizeof(RemoteAllocation));
    new_pending_job->my_resources_granted = local;
    pthread_mutex_lock(&state->protection.mutex);
    state->pending_jobs = list_append(state->pending_jobs, new_pending_job, (ListCpyFunc)pending_job_copy);
    pthread_mutex_unlock(&state->protection.mutex);
    
    // Mandamos los reserve que tengamos que mandar
    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo *alloc = &erl.node_allocations[i];
        char *ip = alloc->erlang_connection_info.ip;
        int port = alloc->erlang_connection_info.port;

        NodeMapCell search_node;
        search_node.connection_info.ip = ip;
        search_node.connection_info.port = port;
        NodeMapCell *node = hashmap_search(state->node_map, &search_node);

        int agent_fd = alloc->agent_fd;
        if (agent_fd == -1) {
            if (node == NULL) {
                log_message("[C]: Error buscando el nodo %s:%d", ip, port);
                continue;
            }
            agent_fd = node->socket_fd;
            alloc->agent_fd = agent_fd;
        }

        char resource[4] = { 0 };
        if (alloc->res_kind == RES_KIND_CPU) strcpy(resource, "cpu");
        if (alloc->res_kind == RES_KIND_MEM) strcpy(resource, "mem");
        if (alloc->res_kind == RES_KIND_GPU) strcpy(resource, "gpu");

        // Caso 1: El recurso pedido esta en nuestro propio nodo
        if (streq(ip, my_ip) && port == state->agent_port) {
            log_message("[C]: Tomando recursos de nosotros mismos: %s:%d", ip, port);
            pthread_mutex_lock(&state->res_protection);
            if (exists_resource(&state->node_resources, alloc->res_kind, alloc->amount)) {
                give_resources(&state->node_resources, alloc->res_kind, alloc->amount);
                increase_resources(&local, alloc->res_kind, alloc->amount);
                increase_resources(&new_pending_job->my_resources_granted,alloc->res_kind,alloc->amount);
                // Como es local, lo contabilizamos como concedido inmediatamente
                new_pending_job->resources_granted++;
            }
            pthread_mutex_unlock(&state->res_protection);
        } 
        // Caso 2: El recurso es de otro nodo
        else {
            int idx = new_pending_job->num_remote_allocated;

            new_pending_job->remote_allocations[idx].connection_info.ip = strdup(ip);
            new_pending_job->remote_allocations[idx].connection_info.port = port;
            new_pending_job->remote_allocations[idx].remote_agent_fd = alloc->agent_fd;
            switch (alloc->res_kind) {
            case RES_KIND_CPU:
                new_pending_job->remote_allocations[idx].resources.current_cpu += alloc->amount;
                break;
            case RES_KIND_MEM:
                new_pending_job->remote_allocations[idx].resources.current_mem += alloc->amount;
                break;
            case RES_KIND_GPU:
                new_pending_job->remote_allocations[idx].resources.current_gpu += alloc->amount;
                break;
            default:
                break;
            }
            new_pending_job->num_remote_allocated++;

            // Mandamos el RESERVE
            log_message("[C]: Enviando RESERVE al agente remoto (%s:%d)", ip, port);
            char reserve_msg[BUFFER_MAX_SIZE] = { 0 };
            sprintf(reserve_msg, "RESERVE %lld %s %d\n", job_id, resource, alloc->amount);
            send(agent_fd, reserve_msg, strlen(reserve_msg), 0);
        }
    }

    // Si todos los recursos pedidos fueron locales, nos sacamos aca mismo el caso de encima.
    if (new_pending_job->resources_granted == new_pending_job->total_resources_needed){
        char reserve_msg[BUFFER_MAX_SIZE] = { 0 };
        sprintf(reserve_msg, "JOB_GRANTED %lld\n", job_id);
        send(erl.erlang_fd, reserve_msg, strlen(reserve_msg), 0);
        JobMapCell* new_job = calloc(1,sizeof(JobMapCell));
        new_job->granted_resources = local;
        new_job->job_id = erl.job_id;
        new_job->num_remotely_allocated = 0;
        new_job->remote_allocations = NULL;
        hashmap_put(state->job_map, new_job);
        pthread_mutex_lock(&state->protection.mutex);
        state->pending_jobs = delete_from_list_by_id(state->pending_jobs,new_pending_job->job_id, (ListFreeFunc)pending_job_free);
        pthread_mutex_unlock(&state->protection.mutex);
    }
}


// Maneja una petición del tipo JOB_RELEASE
void handle_job_release(ErlangRequest erl, int epoll_fd, AgentState *state)
{
    long long job_id = erl.job_id;
    JobMapCell *cell = hashmap_search(state->job_map, &job_id);

    // No encontramos un trabajo con este id, informarlo al cliente
    if (cell == NULL) {
        char error_buf[1024] = { 0 };
        sprintf(error_buf, "JOB_ERROR %lld\n", job_id); // El job id no existe
        send(erl.erlang_fd, error_buf, strlen(error_buf), 0);
        log_message("[C]: Enviando JOB_ERROR al cliente Erlang");
        return;
    }

    log_message("[C]: Procesando liberación de recursos sobre job id %lld", job_id);

    // Devolvemos los recursos al nodo que los dio
    increase_resources(&state->node_resources, RES_KIND_CPU,
                       cell->granted_resources.current_cpu);
    increase_resources(&state->node_resources, RES_KIND_MEM,
                       cell->granted_resources.current_mem);
    increase_resources(&state->node_resources, RES_KIND_GPU,
                       cell->granted_resources.current_gpu);

    // Iteramos por cada recurso alocado, y le avisamos al agente
    // correspondiente que se libera cada recurso que dio
    for (int i = 0; i < cell->num_remotely_allocated; i++) {
        RemoteAllocation remote = cell->remote_allocations[i];
        if (remote.resources.current_cpu > 0)
            send_release_to_agent(state->node_map, remote.connection_info, job_id,
                                  "cpu", remote.resources.current_cpu,
                                  epoll_fd);
        if (remote.resources.current_mem > 0)
            send_release_to_agent(state->node_map, remote.connection_info, job_id,
                                  "mem", remote.resources.current_mem,
                                  epoll_fd);
        if (remote.resources.current_gpu > 0)
            send_release_to_agent(state->node_map, remote.connection_info, job_id,
                                  "gpu", remote.resources.current_gpu,
                                  epoll_fd);
    }

    // Eliminamos el job de la tabla de jobs activos
    hashmap_delete(state->job_map, &job_id);
}

// Maneja una petición del tipo JOB_STATUS
void handle_job_status(ErlangRequest erl, AgentState *state)
{
    log_message("[C]: Status: Recursos que tenemos disponibles: %d CPU, %d MEM %d GPU",
                state->node_resources.current_cpu,
                state->node_resources.current_mem,
                state->node_resources.current_gpu);

    long long job_id = erl.job_id;
    JobMapCell *job = hashmap_search(state->job_map, &job_id);
    char buffer[BUFFER_MAX_SIZE] = { 0 };

    pthread_mutex_lock(&state->protection.mutex);

    log_message("[C]: Procesando un status sobre el job id %lld", job_id);

    // Si el job existe, le informamos al cliente de Erlang que
    // fue dado. De lo contrario, si está en la cola es porque
    // fue negado. Si no, está en la lista de jobs que recibió
    // un TIMEOUT

    if (job != NULL) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_GRANTED %lld\n", job_id);
        log_message("[C]: Enviando JOB_GRANTED al cliente Erlang");
    }

    if (find_in_queue(state->job_queue, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_DENIED %lld\n", job_id);
        log_message("[C]: Enviando JOB_DENIED al cliente Erlang");
    }

    if (find_in_list(state->timed_out_jobs, job_id)) {
        pthread_mutex_unlock(&state->protection.mutex);
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_TIMEOUT %lld\n", job_id);
        log_message("[C]: Enviando timeout al cliente Erlang");

        pthread_mutex_lock(&state->protection.mutex);
        state->timed_out_jobs = delete_from_list_by_id(state->timed_out_jobs, job_id, free);
    }

    pthread_mutex_unlock(&state->protection.mutex);

    // Enviamos el mensaje al cliente de Erlang que nos preguntó
    send(erl.erlang_fd, buffer, strlen(buffer), 0);
}

// Determina si id está en queue
bool find_in_queue(Queue queue, long long id)
{
    for (QueueNode* actual_node = queue; actual_node != NULL; actual_node = actual_node->next) {
        JobQueueData *job = (JobQueueData *)actual_node->data;
        if (job->request.job_id == id)
            return true;
    }

    return false;
}

// Determina si id está en list
bool find_in_list(List list, long long id)
{
    for (ListNode *actual_node = list; actual_node != NULL; actual_node = actual_node->next) {
        long long job_id = *(long long *)actual_node->data;
        if (job_id == id)
            return true;
    }

    return false;
}

// Elimina un elemento de list por id
List delete_from_list_by_id(List list, long long id, ListFreeFunc fr)
{
    if (list_empty(list))
        return list;

    long long *data = (long long *)list->data;
    if (*data == id) {
        ListNode *node = list;
        list = list->next;
        fr(node->data);
        free(node);
        return list;
    }

    list->next = delete_from_list_by_id(list->next, id, fr);
    return list;
}

// Maneja una petición del tipo GET_NODES
void handle_get_nodes(Hashmap node_map, int erlang_fd)
{
    log_message("[C]: Procesando una petición de tipo GET_NODES");

    // Formato: NODES IP:PORT:cpu:NUM1:mem:NUM2:gpu:NUM3;IP:PORT:cpu:NUM4:mem:NUM5:gpu:NUM6; ...
    int buffer_cap = 1024;
    char *buffer = calloc(buffer_cap, sizeof(char));
    strcpy(buffer, "NODES ");

    // Buscamos entre los nodos en nuestra tabla, y si hay uno,
    // sumamos su información al mensaje que vamos a enviar
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            char node_buffer[1024] = { 0 };
            snprintf(node_buffer, sizeof(node_buffer), "%s:%d:cpu:%d:mem:%d:gpu:%d;",
                     node->connection_info.ip,
                     node->connection_info.port,
                     node->resources.cpu,
                     node->resources.mem,
                     node->resources.gpu);

            // Si el string que creamos no tiene suficiente tamaño, lo expandimos
            while (strlen(buffer) + strlen(node_buffer) >= buffer_cap) {
                buffer_cap *= 2;
                char *new_buffer = realloc(buffer, buffer_cap);
                buffer = new_buffer;
            }

            // Sumamos lo que escribimos al resultado final
            strcat(buffer, node_buffer);
        }
    }

    buffer[strlen(buffer)] = '\n';
    buffer[strlen(buffer) + 1] = '\0';

    // Enviamos la información de todos los nodos en el cluster
    // y liberamos el mensaje
    send(erlang_fd, buffer, strlen(buffer), 0);
    free(buffer);
}

// Envia un RELEASE al agente especificado por ip
void send_release_to_agent(Hashmap node_map, ConnectionInfo conn_info, long long job_id,
                            const char *resource, int amount, int epoll_fd)
{
    NodeMapCell search_node;
    search_node.connection_info = conn_info;
    NodeMapCell *target_node = hashmap_search(node_map, &search_node);

    // El nodo no está en el cluster, así que no hacemos nada
    if (target_node == NULL)
        return;

    // Intentamos buscar su descriptor de archivo para enviar el mensaje
    int fd = target_node->socket_fd;
    if (fd == -1) {
        log_message("[C]: Error buscando el nodo %s:%d",
                    target_node->connection_info.ip,
                    target_node->connection_info.port);
        return;
    }

    // Preparamos y enviamos el mensaje
    char release_msg[BUFFER_MAX_SIZE];
    snprintf(release_msg, sizeof(release_msg), "RELEASE %lld %s %d\n", job_id, resource, amount);
    send(fd, release_msg, strlen(release_msg), 0);

    // ¡No cerramos fd para que se quede en la instancia de epoll!
}
