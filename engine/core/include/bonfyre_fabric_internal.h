#ifndef BONFYRE_FABRIC_INTERNAL_H
#define BONFYRE_FABRIC_INTERNAL_H

#include <sqlite3.h>
#include <stdio.h>

#define BF_FABRIC_NOT_HANDLED (-4096)

int bf_fabric_extended_dispatch(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err);

#endif
