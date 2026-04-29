/*
 * bf_bloom.c — Split-block Bloom filter implementation
 *
 * Hash strategy: double-hashing with FNV-1a.
 *   h_i(key) = (h1 + i * h2) mod m
 * where h1 = FNV-1a(key, seed1), h2 = FNV-1a(key, seed2).
 * This gives k independent hash functions from 2 base hashes.
 */

#include "bf_bloom.h"

#include <string.h>
#include <stdio.h>
#include <math.h>

/* ── FNV-1a 64-bit ───────────────────────────────────────────── */

static inline uint64_t fnv1a64(const void *data, size_t len, uint64_t seed) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t h = 0xcbf29ce484222325ULL ^ seed;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

/* ── Bit operations ──────────────────────────────────────────── */

static inline void bit_set(uint8_t *bits, uint64_t idx) {
    bits[idx >> 3] |= (uint8_t)(1u << (idx & 7));
}

static inline int bit_test(const uint8_t *bits, uint64_t idx) {
    return (bits[idx >> 3] >> (idx & 7)) & 1;
}

/* ── Popcount for uint64 ─────────────────────────────────────── */

static inline int popcount64(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcountll(x);
#else
    x -= (x >> 1) & 0x5555555555555555ULL;
    x = (x & 0x3333333333333333ULL) + ((x >> 2) & 0x3333333333333333ULL);
    x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0fULL;
    return (int)((x * 0x0101010101010101ULL) >> 56);
#endif
}

/* ── Lifecycle ───────────────────────────────────────────────── */

void bf_bloom_init(bf_bloom_t *b) {
    memset(b->bits, 0, BF_BLOOM_SIZE_BYTES);
    b->count = 0;
}

/* ── Core: compute k hash positions ──────────────────────────── */

static void bloom_hashes(uint64_t out[BF_BLOOM_K],
                          const void *key, size_t len) {
    uint64_t h1 = fnv1a64(key, len, 0);
    uint64_t h2 = fnv1a64(key, len, 0x9e3779b97f4a7c15ULL);
    for (int i = 0; i < BF_BLOOM_K; i++) {
        out[i] = (h1 + (uint64_t)i * h2) % BF_BLOOM_BITS;
    }
}

/* ── Operations ──────────────────────────────────────────────── */

void bf_bloom_add(bf_bloom_t *b, const void *key, size_t len) {
    uint64_t h[BF_BLOOM_K];
    bloom_hashes(h, key, len);
    for (int i = 0; i < BF_BLOOM_K; i++) {
        bit_set(b->bits, h[i]);
    }
    b->count++;
}

int bf_bloom_check(const bf_bloom_t *b, const void *key, size_t len) {
    uint64_t h[BF_BLOOM_K];
    bloom_hashes(h, key, len);
    for (int i = 0; i < BF_BLOOM_K; i++) {
        if (!bit_test(b->bits, h[i])) return 0;
    }
    return 1;
}

int bf_bloom_add_check(bf_bloom_t *b, const void *key, size_t len) {
    uint64_t h[BF_BLOOM_K];
    bloom_hashes(h, key, len);
    int was_present = 1;
    for (int i = 0; i < BF_BLOOM_K; i++) {
        if (!bit_test(b->bits, h[i])) was_present = 0;
        bit_set(b->bits, h[i]);
    }
    b->count++;
    return was_present;
}

/* ── Convenience ─────────────────────────────────────────────── */

void bf_bloom_add_str(bf_bloom_t *b, const char *s) {
    bf_bloom_add(b, s, strlen(s));
}

int bf_bloom_check_str(const bf_bloom_t *b, const char *s) {
    return bf_bloom_check(b, s, strlen(s));
}

void bf_bloom_add_hash(bf_bloom_t *b, const uint8_t *hash, size_t hash_len) {
    bf_bloom_add(b, hash, hash_len);
}

int bf_bloom_check_hash(const bf_bloom_t *b, const uint8_t *hash, size_t hash_len) {
    return bf_bloom_check(b, hash, hash_len);
}

/* ── Persistence ─────────────────────────────────────────────── */

int bf_bloom_save(const bf_bloom_t *b, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;

    /* Header: magic + count */
    static const uint8_t magic[4] = {'B','F','B','L'};
    if (fwrite(magic, 1, 4, f) != 4) { fclose(f); return -1; }
    if (fwrite(&b->count, 8, 1, f) != 1) { fclose(f); return -1; }
    if (fwrite(b->bits, 1, BF_BLOOM_SIZE_BYTES, f) != BF_BLOOM_SIZE_BYTES) {
        fclose(f); return -1;
    }

    fclose(f);
    return 0;
}

int bf_bloom_load(bf_bloom_t *b, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    uint8_t magic[4];
    if (fread(magic, 1, 4, f) != 4 ||
        magic[0] != 'B' || magic[1] != 'F' ||
        magic[2] != 'B' || magic[3] != 'L') {
        fclose(f);
        return -1;
    }

    if (fread(&b->count, 8, 1, f) != 1) { fclose(f); return -1; }
    if (fread(b->bits, 1, BF_BLOOM_SIZE_BYTES, f) != BF_BLOOM_SIZE_BYTES) {
        fclose(f); return -1;
    }

    fclose(f);
    return 0;
}

/* ── Stats ───────────────────────────────────────────────────── */

uint64_t bf_bloom_popcount(const bf_bloom_t *b) {
    const uint64_t *words = (const uint64_t *)b->bits;
    uint64_t total = 0;
    for (size_t i = 0; i < BF_BLOOM_SIZE_BYTES / 8; i++) {
        total += (uint64_t)popcount64(words[i]);
    }
    return total;
}

double bf_bloom_fpr(const bf_bloom_t *b) {
    uint64_t set_bits = bf_bloom_popcount(b);
    double fill = (double)set_bits / (double)BF_BLOOM_BITS;
    return pow(fill, (double)BF_BLOOM_K);
}

/* ── Merge ───────────────────────────────────────────────────── */

void bf_bloom_merge(bf_bloom_t *dst, const bf_bloom_t *src) {
    const uint64_t *s = (const uint64_t *)src->bits;
    uint64_t *d = (uint64_t *)dst->bits;
    for (size_t i = 0; i < BF_BLOOM_SIZE_BYTES / 8; i++) {
        d[i] |= s[i];
    }
    dst->count += src->count;
}
