/*
 * bf_bloom.h — Split-block Bloom filter for probabilistic dedup
 *
 * Fixed 8 KB filter with k=7 hash functions, optimized for
 * ~10,000 items at 0.01% false-positive rate.
 *
 * Uses FNV-1a seeded with different constants for each hash.
 * Split-block layout: 8 blocks of 1 KB each — each insertion
 * touches only one cache line per block, improving cache locality.
 */

#ifndef BF_BLOOM_H
#define BF_BLOOM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_BLOOM_SIZE_BYTES  8192   /* 64 Kbits */
#define BF_BLOOM_K           7      /* Number of hash functions */
#define BF_BLOOM_BITS        (BF_BLOOM_SIZE_BYTES * 8)

typedef struct {
    uint8_t  bits[BF_BLOOM_SIZE_BYTES];
    uint64_t count;   /* Items inserted (informational) */
} bf_bloom_t;

/* ── Lifecycle ───────────────────────────────────────────────── */

/* Initialize / reset a bloom filter. */
void bf_bloom_init(bf_bloom_t *b);

/* ── Operations ──────────────────────────────────────────────── */

/* Insert a key into the filter. */
void bf_bloom_add(bf_bloom_t *b, const void *key, size_t len);

/* Check if a key is probably in the filter.
 * Returns 1 if probably present, 0 if definitely absent. */
int bf_bloom_check(const bf_bloom_t *b, const void *key, size_t len);

/* Add a key and return whether it was already (probably) present.
 * Equivalent to check + add, but single pass over hashes. */
int bf_bloom_add_check(bf_bloom_t *b, const void *key, size_t len);

/* ── Convenience: string / hash keys ─────────────────────────── */

void bf_bloom_add_str(bf_bloom_t *b, const char *s);
int  bf_bloom_check_str(const bf_bloom_t *b, const char *s);

/* Add a pre-computed hash (e.g., SHA-256 digest). */
void bf_bloom_add_hash(bf_bloom_t *b, const uint8_t *hash, size_t hash_len);
int  bf_bloom_check_hash(const bf_bloom_t *b, const uint8_t *hash, size_t hash_len);

/* ── Persistence ─────────────────────────────────────────────── */

/* Save filter to file. Returns 0 on success. */
int bf_bloom_save(const bf_bloom_t *b, const char *path);

/* Load filter from file. Returns 0 on success. */
int bf_bloom_load(bf_bloom_t *b, const char *path);

/* ── Stats ───────────────────────────────────────────────────── */

/* Estimated false-positive rate at current occupancy. */
double bf_bloom_fpr(const bf_bloom_t *b);

/* Number of bits set. */
uint64_t bf_bloom_popcount(const bf_bloom_t *b);

/* ── Merge ───────────────────────────────────────────────────── */

/* OR two bloom filters together. dst |= src. */
void bf_bloom_merge(bf_bloom_t *dst, const bf_bloom_t *src);

#ifdef __cplusplus
}
#endif

#endif /* BF_BLOOM_H */
