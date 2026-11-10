#ifndef BONFYRE_EXPERIENCE_JSON_CANON_H
#define BONFYRE_EXPERIENCE_JSON_CANON_H

#include <sqlite3.h>
#include <stddef.h>

/* Canonical JSON follows the historical Python bridge byte contract:
 * sorted object keys, compact separators and UTF-8 text.  Keeping that
 * contract stable lets native bindings consume old receipt digests without
 * minting a second hash space. */
char *bf_experience_json_canonical(sqlite3 *db, const char *json);
int bf_experience_json_sha256(sqlite3 *db, const char *json, char out[65]);

int bf_experience_json_text(sqlite3 *db, const char *json, const char *path,
                            char *out, size_t out_size);
int bf_experience_json_int(sqlite3 *db, const char *json, const char *path,
                           long long *out);
int bf_experience_json_true(sqlite3 *db, const char *json, const char *path);

#endif
