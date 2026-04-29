/*
 * bf_hashmap.c — Robin-hood open-addressing hash map
 *
 * Layout: single allocation for `cap` slots.
 * Each slot: { hash64, key_copy, key_len, value, psl }.
 * Robin-hood: on collision, entry with lower PSL (probe sequence length)
 * is displaced by the richer entry. Deletion uses backshift (no tombstones).
 */

#include "bf_hashmap.h"

#include <stdlib.h>
#include <string.h>

/* ── Internal types ──────────────────────── */

typedef struct {
    uint64_t  hash;
    void     *key;
    size_t    key_len;
    void     *value;
    uint32_t  psl;        /* probe sequence length; 0 = empty */
} slot_t;

struct bf_hashmap {
    slot_t              *slots;
    size_t               cap;       /* always power of 2 */
    size_t               count;
    size_t               mask;      /* cap - 1 */
    bf_hashmap_hash_fn   hash_fn;
    bf_hashmap_eq_fn     eq_fn;
    bf_hashmap_free_fn   free_key;
    bf_hashmap_free_fn   free_value;
};

/* ── Default FNV-1a ──────────────────────── */

static uint64_t fnv1a(const void *key, size_t len)
{
    const uint8_t *p = (const uint8_t *)key;
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static int default_eq(const void *a, size_t a_len, const void *b, size_t b_len)
{
    if (a_len != b_len) return 1;
    return memcmp(a, b, a_len);
}

/* ── Helpers ─────────────────────────────── */

static size_t next_pow2(size_t v)
{
    v--;
    v |= v >> 1;  v |= v >> 2;  v |= v >> 4;
    v |= v >> 8;  v |= v >> 16; v |= v >> 32;
    return v + 1;
}

static inline size_t slot_index(uint64_t hash, size_t mask)
{
    return (size_t)(hash & mask);
}

/* ── Lifecycle ───────────────────────────── */

bf_hashmap_t *bf_hashmap_new(const bf_hashmap_opts_t *opts)
{
    bf_hashmap_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;

    size_t initial = 64;
    if (opts && opts->initial_cap > 0) initial = opts->initial_cap;
    m->cap  = next_pow2(initial);
    m->mask = m->cap - 1;

    m->slots = calloc(m->cap, sizeof(slot_t));
    if (!m->slots) { free(m); return NULL; }

    m->hash_fn    = (opts && opts->hash)       ? opts->hash       : fnv1a;
    m->eq_fn      = (opts && opts->eq)         ? opts->eq         : default_eq;
    m->free_key   = opts ? opts->free_key   : NULL;
    m->free_value = opts ? opts->free_value : NULL;

    return m;
}

void bf_hashmap_free(bf_hashmap_t *m)
{
    if (!m) return;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->slots[i].psl > 0) {
            if (m->free_key)   m->free_key(m->slots[i].key);
            if (m->free_value) m->free_value(m->slots[i].value);
        }
    }
    free(m->slots);
    free(m);
}

void bf_hashmap_clear(bf_hashmap_t *m)
{
    if (!m) return;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->slots[i].psl > 0) {
            if (m->free_key)   m->free_key(m->slots[i].key);
            if (m->free_value) m->free_value(m->slots[i].value);
        }
    }
    memset(m->slots, 0, m->cap * sizeof(slot_t));
    m->count = 0;
}

/* ── Resize ──────────────────────────────── */

static int hashmap_grow(bf_hashmap_t *m)
{
    size_t new_cap = m->cap * 2;
    size_t new_mask = new_cap - 1;
    slot_t *new_slots = calloc(new_cap, sizeof(slot_t));
    if (!new_slots) return -1;

    for (size_t i = 0; i < m->cap; i++) {
        slot_t entry = m->slots[i];
        if (entry.psl == 0) continue;

        entry.psl = 1;
        size_t idx = slot_index(entry.hash, new_mask);

        for (;;) {
            if (new_slots[idx].psl == 0) {
                new_slots[idx] = entry;
                break;
            }
            /* robin-hood: displace if current entry is richer */
            if (entry.psl > new_slots[idx].psl) {
                slot_t tmp = new_slots[idx];
                new_slots[idx] = entry;
                entry = tmp;
            }
            entry.psl++;
            idx = (idx + 1) & new_mask;
        }
    }

    free(m->slots);
    m->slots = new_slots;
    m->cap   = new_cap;
    m->mask  = new_mask;
    return 0;
}

/* ── Set ─────────────────────────────────── */

int bf_hashmap_set(bf_hashmap_t *m, const void *key, size_t key_len,
                   void *value)
{
    if (!m || !key) return -1;

    /* grow at ~75% load */
    if (m->count * 4 >= m->cap * 3) {
        if (hashmap_grow(m) != 0) return -1;
    }

    uint64_t h = m->hash_fn(key, key_len);
    size_t idx = slot_index(h, m->mask);

    /* check if key already exists */
    slot_t entry;
    entry.hash    = h;
    entry.key_len = key_len;
    entry.value   = value;
    entry.psl     = 1;

    /* copy key */
    entry.key = malloc(key_len);
    if (!entry.key) return -1;
    memcpy(entry.key, key, key_len);

    for (;;) {
        slot_t *s = &m->slots[idx];

        if (s->psl == 0) {
            /* empty slot */
            *s = entry;
            m->count++;
            return 0;
        }

        /* existing key? */
        if (s->hash == h && m->eq_fn(s->key, s->key_len, entry.key, entry.key_len) == 0) {
            if (m->free_value) m->free_value(s->value);
            s->value = value;
            /* free the copy we made */
            free(entry.key);
            return 0;
        }

        /* robin-hood swap */
        if (entry.psl > s->psl) {
            slot_t tmp = *s;
            *s = entry;
            entry = tmp;
        }

        entry.psl++;
        idx = (idx + 1) & m->mask;
    }
}

/* ── Get ─────────────────────────────────── */

void *bf_hashmap_get(const bf_hashmap_t *m, const void *key, size_t key_len)
{
    if (!m || !key || m->count == 0) return NULL;

    uint64_t h = m->hash_fn(key, key_len);
    size_t idx = slot_index(h, m->mask);
    uint32_t psl = 1;

    for (;;) {
        const slot_t *s = &m->slots[idx];
        if (s->psl == 0 || psl > s->psl) return NULL;

        if (s->hash == h && m->eq_fn(s->key, s->key_len, key, key_len) == 0)
            return s->value;

        psl++;
        idx = (idx + 1) & m->mask;
    }
}

/* ── Has ─────────────────────────────────── */

int bf_hashmap_has(const bf_hashmap_t *m, const void *key, size_t key_len)
{
    return bf_hashmap_get(m, key, key_len) != NULL;
}

/* ── Delete (backshift) ──────────────────── */

int bf_hashmap_del(bf_hashmap_t *m, const void *key, size_t key_len)
{
    if (!m || !key || m->count == 0) return -1;

    uint64_t h = m->hash_fn(key, key_len);
    size_t idx = slot_index(h, m->mask);
    uint32_t psl = 1;

    for (;;) {
        slot_t *s = &m->slots[idx];
        if (s->psl == 0 || psl > s->psl) return -1;

        if (s->hash == h && m->eq_fn(s->key, s->key_len, key, key_len) == 0) {
            /* found — free and backshift */
            if (m->free_key)   m->free_key(s->key);
            if (m->free_value) m->free_value(s->value);

            /* backshift: shift subsequent entries back */
            size_t prev = idx;
            size_t cur  = (idx + 1) & m->mask;
            while (m->slots[cur].psl > 1) {
                m->slots[prev] = m->slots[cur];
                m->slots[prev].psl--;
                prev = cur;
                cur = (cur + 1) & m->mask;
            }
            memset(&m->slots[prev], 0, sizeof(slot_t));
            m->count--;
            return 0;
        }

        psl++;
        idx = (idx + 1) & m->mask;
    }
}

/* ── Count ───────────────────────────────── */

size_t bf_hashmap_count(const bf_hashmap_t *m)
{
    return m ? m->count : 0;
}

/* ── Iteration ───────────────────────────── */

void bf_hashmap_each(const bf_hashmap_t *m, bf_hashmap_iter_fn fn,
                     void *userdata)
{
    if (!m || !fn) return;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->slots[i].psl > 0) {
            if (fn(m->slots[i].key, m->slots[i].key_len,
                   m->slots[i].value, userdata) != 0)
                return;
        }
    }
}

/* ── String convenience ──────────────────── */

int bf_hashmap_sets(bf_hashmap_t *m, const char *key, void *value)
{
    return bf_hashmap_set(m, key, strlen(key), value);
}

void *bf_hashmap_gets(const bf_hashmap_t *m, const char *key)
{
    return bf_hashmap_get(m, key, strlen(key));
}

int bf_hashmap_dels(bf_hashmap_t *m, const char *key)
{
    return bf_hashmap_del(m, key, strlen(key));
}
