#ifndef BONFYRE_SURFACE_IR_H
#define BONFYRE_SURFACE_IR_H

#include <stdio.h>
#include <sqlite3.h>

int bf_surface_ir_compile(sqlite3 *db,
                          const char *namespace_name,
                          const char *page,
                          const char *slot,
                          const char *audience,
                          FILE *out);

#endif
