#ifndef BONFYRE_FRAGMENT_MIGRATION_UTIL_H
#define BONFYRE_FRAGMENT_MIGRATION_UTIL_H
#include <stddef.h>
#include <stdio.h>

char *bf_fm_read_file(const char *path);
int bf_fm_ensure_dir(const char *path);
void bf_fm_json_string(FILE *out, const char *value);
void bf_fm_atom(const char *value, char *out, size_t cap, size_t limit);
char *bf_fm_normalize_space(const char *value);
void bf_fm_digest_parts(const char *const *parts, size_t count, char hex[65]);
int bf_fm_safe_ident(const char *value);
int bf_fm_run(char *const argv[]);
char *bf_fm_path_join(const char *dir, const char *leaf);

#endif
