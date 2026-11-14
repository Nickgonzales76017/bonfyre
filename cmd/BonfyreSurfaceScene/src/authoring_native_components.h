#ifndef BONFYRE_AUTHORING_NATIVE_COMPONENTS_H
#define BONFYRE_AUTHORING_NATIVE_COMPONENTS_H
#include <stddef.h>
#include <stdio.h>
#include "../../BonfyreWebAddress/generated/native_authoring_tree.h"
/* Pure, bounded C renderer over the typed native authoring tree.
 * All HTML belongs to compiled first-party Mustach templates. It never
 * authorizes publication, identity, source rights or external effects. */
int bf_authoring_native_render(FILE *out,const BfAuthoringTree *tree,size_t max_output);
#endif
