#ifndef BONFYRE_PARTICIPATION_ACTIVATION_H
#define BONFYRE_PARTICIPATION_ACTIVATION_H

#include <sqlite3.h>

int bf_part_activation_compile(sqlite3 *db, const char *external_bindings,
                               const char *manifest_path,
                               const char *frontier_path,
                               int frontier_limit);

#endif
