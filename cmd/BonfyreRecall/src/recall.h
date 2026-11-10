#ifndef BONFYRE_RECALL_H
#define BONFYRE_RECALL_H

#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#define BF_RECALL_VERSION "0.2.0"

void bf_recall_init(const char *argv0);
const char *bf_opt(int argc, char **argv, const char *name);
const char *bf_recall_binpath(const char *env, const char *name,
                              char *buf, size_t cap);
int bf_recall_run(char *const argv[], char *out, size_t out_cap,
                  char *err, size_t err_cap);
void bf_json_file(FILE *out, const char *text);
void bf_json_stdout(const char *text);
int bf_mkdir_p(const char *path);
int bf_nonempty_jsonl_count(const char *path);

int bf_recall_query(int argc, char **argv);
int bf_recall_index_jsonl(int argc, char **argv);
int bf_recall_index_fragments(int argc, char **argv);
int bf_recall_index_sources(int argc, char **argv);
int bf_recall_word_index(const char *store, const char *input);

static inline int bf_parse_positive(const char *text, int fallback, int ceiling) {
    char *end = NULL;
    long value;
    if (!text || !*text) return fallback;
    value = strtol(text, &end, 10);
    if (end == text || *end || value < 1 || value > ceiling) return -1;
    return (int)value;
}

#endif
