#ifndef BONFYRE_PARTICIPATION_JSON_H
#define BONFYRE_PARTICIPATION_JSON_H

#include <sqlite3.h>
#include <stddef.h>

typedef struct {
    char **items;
    size_t count;
} BfPartStrings;

char *bf_part_json_text(sqlite3 *db, const char *json, const char *path);
char *bf_part_json_raw(sqlite3 *db, const char *json, const char *path);
char *bf_part_json_quote(sqlite3 *db, const char *text);
int bf_part_json_int(sqlite3 *db, const char *json, const char *path, long long *out);
int bf_part_json_true(sqlite3 *db, const char *json, const char *path);
int bf_part_json_array(sqlite3 *db, const char *json, const char *path, BfPartStrings *out);

int bf_part_strings_add(BfPartStrings *values, const char *value);
int bf_part_strings_has(const BfPartStrings *values, const char *value);
void bf_part_strings_sort(BfPartStrings *values);
void bf_part_strings_free(BfPartStrings *values);

#endif
