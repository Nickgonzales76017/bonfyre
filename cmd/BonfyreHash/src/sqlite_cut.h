#ifndef BONFYRE_HASH_SQLITE_CUT_H
#define BONFYRE_HASH_SQLITE_CUT_H
/* Read-only, typed SQLite table digest; no serialized row collection. */
int bf_hash_sqlite_table(const char *path, const char *table);
#endif
