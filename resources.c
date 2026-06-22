#include "resources.h"

// Determina si hay una cantidad de un recurso dado
bool exists_resource(LocalResources *res, ResourceKind kind, int amount)
{
    switch (kind) {
    case RES_KIND_CPU:
        if (res->current_cpu >= amount)
            return true;
        break;
    case RES_KIND_MEM:
        if (res->current_mem >= amount)
            return true;
        break;
    case RES_KIND_GPU:
        if (res->current_gpu >= amount)
            return true;
        break;
    default:
        break;
    }

    return false;
}

// Decrementa el recurso de un nodo especificado por tipo y cantidad
void give_resources(LocalResources *res, ResourceKind kind, int amount)
{
    switch (kind) {
    case RES_KIND_CPU:
        res->current_cpu -= amount;
        break;
    case RES_KIND_MEM:
        res->current_mem -= amount;
        break;
    case RES_KIND_GPU:
        res->current_gpu -= amount;
        break;
    default:
        break;
    }
}

// Incrementa el recurso de un nodo especificado por tipo y cantidad
void increase_resources(LocalResources *res, ResourceKind kind, int amount)
{
    switch (kind) {
    case RES_KIND_CPU:
        res->current_cpu += amount;
        break;
    case RES_KIND_MEM:
        res->current_mem += amount;
        break;
    case RES_KIND_GPU:
        res->current_gpu += amount;
        break;
    default:
        break;
    }
}

// Calcula el total de recursos de todos los nodos
LocalResources get_initial_resources(Hashmap node_map)
{
    // Itera por todos los nodos y suma sus recursos
    LocalResources res = { 0 };
    for (int i = 0; i < node_map->cap; ++i) {
        bool exists_item = node_map->items[i].data != NULL &&
                            !node_map->items[i].deleted;
        if (exists_item) {
            NodeMapCell *node = (NodeMapCell *)node_map->items[i].data;
            res.cpu += node->resources.cpu;
            res.mem += node->resources.mem;
            res.gpu += node->resources.gpu;

            res.current_cpu += node->resources.current_cpu;
            res.current_mem += node->resources.current_mem;
            res.current_gpu += node->resources.current_gpu;
        }
    }

    return res;
}
