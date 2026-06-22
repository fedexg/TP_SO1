#ifndef RESOURCES_H
#define RESOURCES_H

#include "types.h"
#include "ds/hashmap.h"
#include <stdbool.h>

bool exists_resource(LocalResources *res, ResourceKind kind, int amount);
void give_resources(LocalResources *res, ResourceKind kind, int amount);
void increase_resources(LocalResources *res, ResourceKind kind, int amount);
LocalResources get_initial_resources(Hashmap node_map);

#endif // RESOURCES_H
