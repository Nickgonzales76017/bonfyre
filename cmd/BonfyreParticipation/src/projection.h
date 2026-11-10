#ifndef BONFYRE_PARTICIPATION_PROJECTION_H
#define BONFYRE_PARTICIPATION_PROJECTION_H

#include <sqlite3.h>

int bf_part_project_manifest(sqlite3 *db, const char *external_bindings,
                             long long raw_evidence_rows, const char *output_path);

#endif
