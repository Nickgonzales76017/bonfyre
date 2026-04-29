/*
 * bf_hashmap.h — Robin-hood open-addressing hash map
 *
 * Features:
 *   - Power-of-2 capacity, load factor ~0.75
 *   - Robin-hood insertion with backshift deletion (no tombstones)
 *   - Configurable hash/eq callbacks for generic key types
 *   - Default: FNV-1a for byte-string keys
 */

#ifndef BF_HASHMAP_H
#define BF_HASHMAP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Callbacks ───────────────────────────── */

/* Hash a key to uint64_t. */
typedef uint64_t (*bf_hashmap_hash_fn)(const void *key, size_t key_len);

/* Compare two keys. Return 0 if equal. */
typedef int (*bf_hashmap_eq_fn)(const void *a, size_t a_len,
                                const void *b, size_t b_len);

/* Free a key or value. If NULL, no-op. */
typedef void (*bf_hashmap_free_fn)(void *ptr);

/* ── Map handle ──────────────────────────── */

typedef struct bf_hashmap bf_hashmap_t;

/* ── Configuration ───────────────────────── */

typedef struct {
    size_t               initial_cap;   /* 0 = default (64) */
    bf_hashmap_hash_fn   hash;          /* NULL = FNV-1a */
    bf_hashmap_eq_fn     eq;            /* NULL = memcmp */
    bf_hashmap_free_fn   free_key;      /* NULL = no-op */
    bf_hashmap_free_fn   free_value;    /* NULL = no-op */
} bf_hashmap_opts_t;

/* ── Lifecycle ───────────────────────────── */

bf_hashmap_t *bf_hashmap_new(const bf_hashmap_opts_t *opts); /* opts may be NULL */
void          bf_hashmap_free(bf_hashmap_t *m);
void          bf_hashmap_clear(bf_hashmap_t *m);

/* ── Core operations ─────────────────────── */

/*
 * Insert or update. Copies `key_len` bytes from `key`, stores `value` pointer.
 * Returns 0 on success, -1 on alloc failure.
 * If key exists, old value is freed (if free_value set) and replaced.
 */
int bf_hashmap_set(bf_hashmap_t *m, const void *key, size_t key_len,
                   void *value);

/*
 * Lookup. Returns value pointer, or NULL if not found.
 */
void *bf_hashmap_get(const bf_hashmap_t *m, const void *key, size_t key_len);

/*
 * Delete. Returns 0 if found and removed, -1 if not found.
 */
int bf_hashmap_del(bf_hashmap_t *m, const void *key, size_t key_len);

/* Check membership. */
int bf_hashmap_has(const bf_hashmap_t *m, const void *key, size_t key_len);

/* Number of entries. */
size_t bf_hashmap_count(const bf_hashmap_t *m);

/* ── Iteration ───────────────────────────── */

/*
 * Visitor callback. Return 0 to continue, non-zero to stop.
 */
typedef int (*bf_hashmap_iter_fn)(const void *key, size_t key_len,
                                  void *value, void *userdata);

void bf_hashmap_each(const bf_hashmap_t *m, bf_hashmap_iter_fn fn,
                     void *userdata);

/* ── String convenience ──────────────────── */

/* Wrappers that use strlen(key) as key_len: */
int   bf_hashmap_sets(bf_hashmap_t *m, const char *key, void *value);
void *bf_hashmap_gets(const bf_hashmap_t *m, const char *key);
int   bf_hashmap_dels(bf_hashmap_t *m, const char *key);

#ifdef __cplusplus
}
#endif

#endif /* BF_HASHMAP_H */
