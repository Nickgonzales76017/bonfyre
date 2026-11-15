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

#define BF_COMMAND_COUNT(table) (sizeof(table) / sizeof((table)[0]))

/* The id of `name` in a sorted table, or `missing` when it is not a verb.
 * Lets a CLI resolve once and `switch` on the id. */
static inline int bf_command_id(const BfCommandEntry *entries, size_t count,
                                const char *name, int missing) {
    const BfCommandEntry *entry = bf_command_lookup(entries, count, name);
    return entry ? entry->id : missing;
}

/* Declare a verb vocabulary once as an X-macro list of X("verb", ID) rows in
 * strcmp order; this emits the id enum and the sorted lookup table. */
#define BF_COMMAND_ENUM_ROW_(name, id) id,
#define BF_COMMAND_TABLE_ROW_(name, id) {name, id, 0, NULL},
#define BF_COMMAND_TABLE(table, LIST) \
    enum { LIST(BF_COMMAND_ENUM_ROW_) }; \
    static const BfCommandEntry table[] = { LIST(BF_COMMAND_TABLE_ROW_) }

/* 0 when the table is strictly sorted; otherwise 1 + the first index whose
 * name does not sort after its predecessor (binary search would miss it). */
static inline size_t bf_command_table_unsorted(const BfCommandEntry *entries,
                                               size_t count) {
    for (size_t i = 1; i < count; i++)
        if (strcmp(entries[i - 1].name, entries[i].name) >= 0) return i + 1;
    return 0;
}

#endif
