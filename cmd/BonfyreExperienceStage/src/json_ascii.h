#ifndef BONFYRE_EXPERIENCE_STAGE_JSON_ASCII_H
#define BONFYRE_EXPERIENCE_STAGE_JSON_ASCII_H

#include <sqlite3.h>

/* Python json.dumps(sort_keys=True,separators=(",",":"),ensure_ascii=True). */
char *bf_stage_json_ascii_canonical(sqlite3 *db, const char *json);

#endif
