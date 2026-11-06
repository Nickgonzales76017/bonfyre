#ifndef BONFYRE_EXPERIENCE_BINDING_DOMAIN_BINDING_H
#define BONFYRE_EXPERIENCE_BINDING_DOMAIN_BINDING_H

#include <sqlite3.h>

int bf_experience_bind_domain(sqlite3 *db, const char *occurrence_id,
                              const char *observation_receipts,
                              const char *ingest_dir,
                              const char *output_path);

#endif
