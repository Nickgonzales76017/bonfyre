#ifndef BF_FILESYSTEM_H
#define BF_FILESYSTEM_H

#include <sqlite3.h>
#include <stdio.h>

int bf_filesystem_project(sqlite3 *database, const char *target, FILE *output, FILE *error);

#endif
