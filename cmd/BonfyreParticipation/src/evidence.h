#ifndef BONFYRE_PARTICIPATION_EVIDENCE_H
#define BONFYRE_PARTICIPATION_EVIDENCE_H

#include <sqlite3.h>

int bf_part_evidence_init(sqlite3 *db);
int bf_part_evidence_load(sqlite3 *db, const char *path, long long *raw_rows);

#endif
