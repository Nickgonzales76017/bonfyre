#ifndef BF_RECORD_FACTOR_H
#define BF_RECORD_FACTOR_H
#include <stddef.h>
#include <stdio.h>
#include <sqlite3.h>
/* Exact source-record factoring. This derived representation owns no business
 * state. Equal source fragments are stored once; every read verifies raw bytes. */
int bf_record_factor_build(const char *source, const char *target, FILE *report);
int bf_record_factor_open(const char *path, sqlite3 **db);
int bf_record_factor_read(sqlite3 *db, sqlite3_int64 row, char **body, size_t *length);
int bf_record_factor_export(const char *path, FILE *out);
int bf_record_factor_verify(const char *path, const char *source, FILE *report);
typedef int (*BfRecordVisitor)(void *, sqlite3_int64, const char *, const char *, const char *);
/* A bounded read against this representation, shared by CLI and WebAddress. */
int bf_record_factor_visit(sqlite3 *db, const char *search, int limit, int offset,
                           BfRecordVisitor visitor, void *context);
/* Returns -1 for commands outside this family. */
int bf_record_factor_command(int argc, char **argv);
#endif
