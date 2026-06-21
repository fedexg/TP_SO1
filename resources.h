#ifndef RESOURCES_H
#define RESOURCES_H

#include "types.h"

bool exists_resource(LocalResources *res, ResourceKind kind, int amount);
void give_resources(LocalResources *res, ResourceKind kind, int amount);
void increase_resources(LocalResources *res, ResourceKind kind, int amount);

#endif // RESOURCES_H
