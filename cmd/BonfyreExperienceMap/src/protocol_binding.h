#ifndef BONFYRE_EXPERIENCE_PROTOCOL_BINDING_H
#define BONFYRE_EXPERIENCE_PROTOCOL_BINDING_H

#include <sqlite3.h>

int bf_experience_bind_protocol(sqlite3 *json_db, const char *occurrence_id,
                                const char *evidence_path,
                                const char *native_receipt_path,
                                const char *output_path);

#endif
