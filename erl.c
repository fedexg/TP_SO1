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
#include "types.h"
#include "utils.h"
#include "resources.h"
#include "ds/queue.h"
#include "ds/list.h"

#include "log/log.h"

bool find_in_queue(Queue queue, long long id);
bool find_in_list(List list, long long id);
List delete_from_timed_out_jobs(List timed_out_jobs, long long id);
void send_release_to_agent(Hashmap node_map, ConnectionInfo conn_info, long long job_id,
                            const char *resource, int amount, int epoll_fd);

// Manejar requests (si es posible) proveniente de un cliente Erlang
void handle_erlang_client(int erlang_fd, time_t time, int epoll_fd, AgentState *state)
{
    log_message("[C]: Procesando cliente Erlang con descriptor %d", erlang_fd);
    char buffer[BUFFER_MAX_SIZE] = { 0 };

    // Leemos el mensaje que nos envía el cliente Erlang
    ssize_t bytes_read = read_full_line(erlang_fd, buffer, BUFFER_MAX_SIZE - 1);
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
            pthread_mutex_lock(&state->protection.mutex);
            handle_job_request(erl, time, epoll_fd, state);
            pthread_mutex_unlock(&state->protection.mutex);
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
        state->timed_out_jobs = list_append(state->timed_out_jobs, &job_id,
                                            (ListCpyFunc)int_copy);
        send(erl.erlang_fd, msg, strlen(msg), 0);
        return;
    }

    // Pedimos a los agentes lo que necesitamos
    RemoteAllocation *remote_allocs = calloc(erl.num_allocations, sizeof(RemoteAllocation));
    int num_granted = 0;

    // Como pueden repetirse IPs en una petición de este tipo,
    // usamos una lista, pues no se asegura que hayan erl.num_allocations agentes
    List agent_fds = list_make();

    // Necesitamos la IP para no mandarnos
    // mensajes a nosotros mismos
    char my_ip[64];
    get_local_ip(my_ip, 64);
    LocalResources local = { 0 };

    for (int i = 0; i < erl.num_allocations; ++i) {
        NodeAllocationInfo alloc = erl.node_allocations[i];
        char *ip = alloc.erlang_connection_info.ip;
        int port = alloc.erlang_connection_info.port;

        // Determinamos a quién hay que enviarle un mensaje
        int agent_fd = alloc.agent_fd;
        if (agent_fd == -1) {
            // Buscamos el nodo con la IP y puerto que tenemos
            // para poder mandarle mensajes
            NodeMapCell search_node;
            search_node.connection_info.ip = ip;
            search_node.connection_info.port = port;
            NodeMapCell *node = hashmap_search(state->node_map, &search_node);

            if (node == NULL) {
                log_message("[C]: Error buscando el nodo %s:%d", ip, port);
                continue;
            }

            agent_fd = node->socket_fd;
        }

        // Preparamos el tipo de recurso a pedir
        char msg[BUFFER_MAX_SIZE] = { 0 };
        char resource[4] = { 0 };

        if (alloc.res_kind == RES_KIND_CPU)
            strcpy(resource, "cpu");
        if (alloc.res_kind == RES_KIND_MEM)
            strcpy(resource, "mem");
        if (alloc.res_kind == RES_KIND_GPU)
            strcpy(resource, "gpu");

        // Evitamos enviarnos un mensaje a nosotros mismos; simplemente
        // tomamos nuestros propios recursos, y lo consideramos GRANTED
        if (streq(ip, my_ip) && port == state->agent_port) {
            log_message("[C]: Tomando recursos de nosotros mismos: %s:%d", ip, port);
            if (exists_resource(&state->node_resources, alloc.res_kind, alloc.amount)) {
                give_resources(&state->node_resources, alloc.res_kind, alloc.amount);
                increase_resources(&local, alloc.res_kind, alloc.amount);
                ++num_granted;
            }
        } else {
            log_message("[C]: Enviando RESERVE al agente C pedido (%s:%d)", ip, port);

            // Avisamos a otro agente C que queremos reservar un recurso
            sprintf(msg, "RESERVE %lld %s %d\n", job_id, resource, alloc.amount);
            ssize_t bytes_read = send(agent_fd, msg, strlen(msg), 0);
            if (bytes_read < 0)
                error("Error enviando mensaje");

            // Contamos la cantidad de GRANTEDs que recibimos de parte del agente C
            char recv_buffer[BUFFER_MAX_SIZE] = { 0 };

            // Hacemos al socket (temporalmente) no bloqueante para que se reciba
            // el mensaje adecuadamente
            int flags = fcntl(agent_fd, F_GETFL, 0);
            fcntl(agent_fd, F_SETFL, flags & ~O_NONBLOCK);

            bytes_read = read_full_line(agent_fd, recv_buffer, BUFFER_MAX_SIZE - 1);

            // Restauramos la propiedad de no bloqueante
            fcntl(agent_fd, F_SETFL, flags | O_NONBLOCK);

            if (bytes_read < 0) {
                // Error de lectura
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    log_message("[C]: No llegó el mensaje entero todavía");
                    continue;
                }
            }


            // Con saber que recibimos GRANTED podemos guardar los recursos;
            // no necesitamos el job_id en este caso
            if (strncmp(recv_buffer, "GRANTED", 7) == 0) {
                remote_allocs[num_granted].connection_info.ip = strdup(ip);
                remote_allocs[num_granted].connection_info.port = port;
                if (alloc.res_kind == RES_KIND_CPU)
                    remote_allocs[num_granted].resources.current_cpu += alloc.amount;
                if (alloc.res_kind == RES_KIND_MEM)
                    remote_allocs[num_granted].resources.current_mem += alloc.amount;
                if (alloc.res_kind == RES_KIND_GPU)
                    remote_allocs[num_granted].resources.current_gpu += alloc.amount;

                ++num_granted;
                agent_fds = list_append(agent_fds, &agent_fd, (ListCpyFunc)int_copy);
            }
        }
    }

    // Recibimos menos GRANTEDs de los que necesitamos,
    // entonces negamos el trabajo y liberamos los recursos
    // de jobs que se aceptaron
    if (num_granted < erl.num_allocations) {
        log_message("[C]: Enviando JOB_DENIED al cliente Erlang");

        if (!erl.sent_message) {
            memset(msg, 0, BUFFER_MAX_SIZE - 1);
            sprintf(msg, "JOB_DENIED %lld\n", job_id);
            send(erl.erlang_fd, msg, strlen(msg), 0);
            erl.sent_message = true;
        }

        // Primero chequeamos si nosotros liberamos los recursos
        for (int i = 0; i < erl.num_allocations; ++i) {
            NodeAllocationInfo alloc = erl.node_allocations[i];
            if (streq(alloc.erlang_connection_info.ip, my_ip) &&
                    alloc.erlang_connection_info.port == state->agent_port) {
                log_message("[C]: Devolviendo recursos a nosotros mismos %s:%d", my_ip, state->agent_port);
                increase_resources(&state->node_resources, alloc.res_kind, alloc.amount);
            }
        }

        // Luego buscamos en los otros nodos
        for (ListNode *p = agent_fds; p != NULL; p = p->next) {
            for (int i = 0; i < erl.num_allocations; ++i) {
                NodeAllocationInfo alloc = erl.node_allocations[i];
                if (*(int *)p->data == alloc.agent_fd) {
                    log_message("[C]: Enviando RELEASE al agente de C al que se le pidió recursos");

                    char buffer[BUFFER_MAX_SIZE]  = { 0 };
                    sprintf(buffer, "RELEASE %lld\n", job_id);
                    send(alloc.agent_fd, buffer, strlen(buffer), 0);
                }
            }
        }

        time_t current_time;
        if (find_in_queue(state->job_queue, erl.job_id))
            current_time = time_req;
        else
            current_time = time(NULL);
        
        free(remote_allocs);
        JobQueueData data = {erl, current_time};

        log_message("[C]: Metiendo Job a la queue");
        state->job_queue = enqueue(state->job_queue, &data, (QueueCpyFunc)job_copy);
        pthread_cond_signal(&state->protection.nonempty_queue_cond);

        log_message("[C]: Encolando el job con id %lld", job_id);
        return;
    }

    // Creamos el job y lo insertamos en la tabla de jobs activos
    JobMapCell *cell = calloc(1, sizeof(JobMapCell));
    cell->job_id = job_id;
    cell->num_remotely_allocated = erl.num_allocations;
    cell->remote_allocations = remote_allocs;
    cell->granted_resources = local;

    hashmap_put(state->job_map, cell);

    log_message("[C]: Enviando JOB_GRANTED al cliente Erlang");

    memset(msg, 0, BUFFER_MAX_SIZE - 1);

    if (!erl.sent_message) {
        sprintf(msg, "JOB_GRANTED %lld\n", job_id);
        send(erl.erlang_fd, msg, strlen(msg), 0);
        erl.sent_message = true;
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

    pthread_mutex_lock(&state->protection.mutex);
    if (find_in_queue(state->job_queue, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_DENIED %lld\n", job_id);
        log_message("[C]: Enviando JOB_DENIED al cliente Erlang");
    }
     pthread_mutex_unlock(&state->protection.mutex);
    if (find_in_list(state->timed_out_jobs, job_id)) {
        memset(buffer, 0, BUFFER_MAX_SIZE - 1);
        sprintf(buffer, "JOB_TIMEOUT %lld\n", job_id);
        log_message("[C]: Enviando timeout al cliente Erlang");

        state->timed_out_jobs = delete_from_timed_out_jobs(state->timed_out_jobs, job_id);
    }


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

// Elimina un job que recibió un TIMEOUT con id 'id'
List delete_from_timed_out_jobs(List timed_out_jobs, long long id)
{
    if (list_empty(timed_out_jobs))
        return timed_out_jobs;

    long long *data = (long long *)timed_out_jobs->data;
    if (*data == id) {
        ListNode *node = timed_out_jobs;
        timed_out_jobs = timed_out_jobs->next;
        free(node);
        return timed_out_jobs;
    }

    timed_out_jobs->next = delete_from_timed_out_jobs(timed_out_jobs->next, id);
    return timed_out_jobs;
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
