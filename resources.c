#include "resources.h"

// Check if there are enough resources requested
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

// Decreases a node resource specified by kind by amount
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

// Increases a node resource specified by kind by amount
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

