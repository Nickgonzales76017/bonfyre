#ifndef BF_COMMAND_LOOKUP_H
#define BF_COMMAND_LOOKUP_H

#include <stddef.h>
#include <string.h>

/* A CLI resolves its verb once.  Tables are sorted by name; the owner keeps
 * argument, database and authority semantics in its descriptor/handler. */
typedef struct {
    const char *name;
    int id;
    int mode;
    const char *path_option;
} BfCommandEntry;

static inline const BfCommandEntry *bf_command_lookup(
        const BfCommandEntry *entries, size_t count, const char *name) {
    size_t lo = 0, hi = count;
    if (!entries || !name) return NULL;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int order = strcmp(name, entries[mid].name);
        if (order == 0) return &entries[mid];
        if (order < 0) hi = mid;
        else lo = mid + 1;
    }
    return NULL;
}

#endif
