#ifndef BONFYRE_FABRIC_H
#define BONFYRE_FABRIC_H

#include <stdio.h>

/*
 * Fabric Core is the small, authoritative control plane shared by native
 * front-ends.  It deliberately records intent before effects and keeps
 * physical roots, stable addresses, catalog truth, execution evidence, and
 * mission continuity in one SQLite store.
 */

#define BF_FABRIC_VERSION "0.1.0"

int bf_fabric_dispatch(int argc, char **argv, FILE *out, FILE *err);
int bf_fabric_bootstrap(char *state_dir, size_t state_dir_size,
                        char *db_path, size_t db_path_size, FILE *err);

#endif
