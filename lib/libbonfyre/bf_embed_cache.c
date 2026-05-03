/*
 * bf_embed_cache.c — Content-addressable embed + KV cache store
 *
 * Two namespaces, both keyed by SHA-256 hash, both living under
 * ~/.local/share/bonfyre/ so every binary that links libbonfyre
 * shares the same store without any daemon or IPC:
 *
 *   embeds/   — float32 embedding vectors (384-dim for MiniLM, etc.)
 *   kvcache/  — compressed KV projection blobs (from bonfyre-kvcache)
 *
 * Storage is intentionally boring: flat files, atomic rename-on-write,
 * mmap-friendly alignment. The OS page cache handles cross-process
 * sharing; we never need to coordinate.
 *
 * Embed file format (.bfembed):
 *   uint32_t magic   = 0x45424643 ("CFBE" little-endian)
 *   uint32_t dim     — number of float32 elements
 *   float32[dim]     — the vector
 *
 * KV blob format (.bfkv):
 *   uint32_t magic   = 0x564B4642 ("BFKV" little-endian)
 *   uint64_t len     — byte length of payload
 *   uint8_t[len]     — raw bytes (bonfyre-kvcache E8+RVQ encoded block)
 */
#define _DEFAULT_SOURCE
#include "include/bonfyre.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#define EMBED_MAGIC  0x45424643u   /* "CFBE" */
#define KV_MAGIC     0x564B4642u   /* "BFKV" */
#define MAX_STORE_PATH 4096

/* ── path helpers ──────────────────────────────────────────── */

static const char *home_dir(void) {
    const char *h = getenv("HOME");
    return h ? h : "/tmp";
}

static void hash_to_hex(const uint8_t hash[32], char hex[65]) {
    static const char hc[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        hex[i * 2]     = hc[hash[i] >> 4];
        hex[i * 2 + 1] = hc[hash[i] & 0xf];
    }
    hex[64] = '\0';
}

static void embed_path(const uint8_t hash[32], char *buf, size_t sz) {
    char hex[65];
    hash_to_hex(hash, hex);
    snprintf(buf, sz, "%s/.local/share/bonfyre/embeds/%s.bfembed",
             home_dir(), hex);
}

static void kv_dir(const uint8_t model_hash[32], char *buf, size_t sz) {
    char hex[65];
    hash_to_hex(model_hash, hex);
    snprintf(buf, sz, "%s/.local/share/bonfyre/kvcache/%s",
             home_dir(), hex);
}

static void kv_path(const uint8_t model_hash[32], const uint8_t ctx_hash[32],
                    char *buf, size_t sz) {
    char mhex[65], chex[65];
    hash_to_hex(model_hash, mhex);
    hash_to_hex(ctx_hash, chex);
    snprintf(buf, sz, "%s/.local/share/bonfyre/kvcache/%s/%s.bfkv",
             home_dir(), mhex, chex);
}

/* Ensure parent directory of a file path exists. */
static void ensure_parent(const char *filepath) {
    char dir[MAX_STORE_PATH];
    snprintf(dir, sizeof(dir), "%s", filepath);
    char *sl = strrchr(dir, '/');
    if (sl) { *sl = '\0'; bf_ensure_dir(dir); }
}

/* ── Embed cache ───────────────────────────────────────────── */

/*
 * bf_embed_lookup — check the store for a cached embedding.
 *
 * Returns 0 on hit: *out is malloc'd by this function (caller frees),
 *                   *out_dim is set to the stored dimension.
 * Returns -1 on miss or any read error.
 */
int bf_embed_lookup(const uint8_t hash[32], float **out, uint32_t *out_dim) {
    char path[MAX_STORE_PATH];
    embed_path(hash, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    uint32_t magic, dim;
    if (fread(&magic, sizeof(uint32_t), 1, f) != 1 || magic != EMBED_MAGIC ||
        fread(&dim,   sizeof(uint32_t), 1, f) != 1 || dim == 0 || dim > 65536) {
        fclose(f);
        return -1;
    }

    float *vec = (float *)malloc(dim * sizeof(float));
    if (!vec) { fclose(f); return -1; }

    if (fread(vec, sizeof(float), dim, f) != dim) {
        free(vec);
        fclose(f);
        return -1;
    }

    fclose(f);
    *out     = vec;
    *out_dim = dim;
    return 0;
}

/*
 * bf_embed_store — persist an embedding vector keyed by content hash.
 *
 * Write is atomic (tmp + rename). If the path already exists we skip
 * the write — the store is immutable: same hash → same content.
 */
void bf_embed_store(const uint8_t hash[32], const float *vec, uint32_t dim) {
    char path[MAX_STORE_PATH], tmp[MAX_STORE_PATH + 4];
    embed_path(hash, path, sizeof(path));

    /* Already cached — skip write. */
    struct stat st;
    if (stat(path, &st) == 0) return;

    ensure_parent(path);
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "wb");
    if (!f) return;

    uint32_t magic = EMBED_MAGIC;
    int ok = (fwrite(&magic, sizeof(uint32_t), 1, f) == 1 &&
              fwrite(&dim,   sizeof(uint32_t), 1, f) == 1 &&
              fwrite(vec, sizeof(float), dim, f) == dim);
    fclose(f);

    if (ok) rename(tmp, path);   /* POSIX atomic */
    else    unlink(tmp);
}

/* ── KV-cache object store ─────────────────────────────────── */

/*
 * bf_kvcache_store — persist a compressed KV blob.
 *
 * Keyed by (model_hash, ctx_hash): allows the same context to be
 * stored under different quantized model versions independently.
 * Returns 0 on success, -1 on I/O error.
 */
int bf_kvcache_store(const uint8_t model_hash[32], const uint8_t ctx_hash[32],
                     const void *data, size_t len) {
    char path[MAX_STORE_PATH], tmp[MAX_STORE_PATH + 4];
    kv_path(model_hash, ctx_hash, path, sizeof(path));

    /* Already cached — idempotent. */
    struct stat st;
    if (stat(path, &st) == 0) return 0;

    char dir[MAX_STORE_PATH];
    kv_dir(model_hash, dir, sizeof(dir));
    bf_ensure_dir(dir);

    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "wb");
    if (!f) return -1;

    uint32_t magic = KV_MAGIC;
    uint64_t sz = (uint64_t)len;
    int ok = (fwrite(&magic, sizeof(uint32_t), 1, f) == 1 &&
              fwrite(&sz,    sizeof(uint64_t), 1, f) == 1 &&
              fwrite(data, 1, len, f) == len);
    fclose(f);

    if (ok) { rename(tmp, path); return 0; }
    unlink(tmp);
    return -1;
}

/*
 * bf_kvcache_fetch — retrieve a compressed KV blob.
 *
 * Returns 0 on hit: *out_data is malloc'd (caller frees), *out_len set.
 * Returns -1 on miss or I/O error.
 */
int bf_kvcache_fetch(const uint8_t model_hash[32], const uint8_t ctx_hash[32],
                     void **out_data, size_t *out_len) {
    char path[MAX_STORE_PATH];
    kv_path(model_hash, ctx_hash, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    uint32_t magic;
    uint64_t sz;
    if (fread(&magic, sizeof(uint32_t), 1, f) != 1 || magic != KV_MAGIC ||
        fread(&sz,    sizeof(uint64_t), 1, f) != 1 || sz == 0) {
        fclose(f);
        return -1;
    }

    void *buf = malloc((size_t)sz);
    if (!buf) { fclose(f); return -1; }

    if (fread(buf, 1, (size_t)sz, f) != (size_t)sz) {
        free(buf);
        fclose(f);
        return -1;
    }

    fclose(f);
    *out_data = buf;
    *out_len  = (size_t)sz;
    return 0;
}

/* ── KV commit chain (Merkle DAG) ──────────────────────────────────── *
 *
 * Each KV commit is identified by:
 *   ctx_hash = SHA-256(model_hash || parent_ctx_hash || data)
 *
 * This makes ctx_hash a cryptographic commitment to the entire history
 * of the context (every parent, transitively). Same data + same lineage
 * = same hash. Two sequences that diverge produce different hashes even
 * if their terminal KV blobs are identical.
 *
 * On-disk format (.bfkv2):
 *   uint32_t magic  = KV2_MAGIC (0x564B4643 "CFKV")
 *   uint8_t  parent[32]  — SHA-256 of parent ctx (zero = root)
 *   uint64_t len
 *   uint8_t[len] data
 */

#define KV2_MAGIC 0x564B4643u   /* "CFKV" */

static void kv_chain_dir_(const uint8_t model_hash[32], char *buf, size_t sz) {
    char hex[65];
    hash_to_hex(model_hash, hex);
    snprintf(buf, sz, "%s/.local/share/bonfyre/kvcache-chain/%s",
             home_dir(), hex);
}

static void kv_chain_path_(const uint8_t model_hash[32],
                           const uint8_t ctx_hash[32],
                           char *buf, size_t sz) {
    char mhex[65], chex[65];
    hash_to_hex(model_hash, mhex);
    hash_to_hex(ctx_hash,   chex);
    snprintf(buf, sz, "%s/.local/share/bonfyre/kvcache-chain/%s/%s.bfkv2",
             home_dir(), mhex, chex);
}

/*
 * bf_kvcache_chain — store a KV blob as a chain commit.
 *
 * Computes new_ctx_hash = SHA-256(model_hash || parent_ctx_hash || data),
 * writes the blob with its parent pointer, fills new_ctx_hash.
 * parent_ctx_hash = {0} means root of a new sequence.
 * Idempotent: if the computed hash already exists on disk, skips write.
 * Returns 0 on success, -1 on I/O error.
 */
int bf_kvcache_chain(const uint8_t model_hash[32],
                     const uint8_t parent_ctx_hash[32],
                     const void *data, size_t len,
                     uint8_t new_ctx_hash[32]) {
    /* Derive deterministic hash from full lineage */
    BfSha256 sha;
    bf_sha256_init(&sha);
    bf_sha256_update(&sha, model_hash,      32);
    bf_sha256_update(&sha, parent_ctx_hash, 32);
    bf_sha256_update(&sha, (const uint8_t *)data, len);
    bf_sha256_final(&sha, new_ctx_hash);

    char path[MAX_STORE_PATH], tmp[MAX_STORE_PATH + 4];
    kv_chain_path_(model_hash, new_ctx_hash, path, sizeof(path));

    /* Idempotent */
    struct stat st;
    if (stat(path, &st) == 0) return 0;

    char dir[MAX_STORE_PATH];
    kv_chain_dir_(model_hash, dir, sizeof(dir));
    bf_ensure_dir(dir);

    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE *f = fopen(tmp, "wb");
    if (!f) return -1;

    uint32_t magic = KV2_MAGIC;
    uint64_t sz    = (uint64_t)len;
    int ok = (fwrite(&magic,          4,  1, f) == 1 &&
              fwrite(parent_ctx_hash, 1, 32, f) == 32 &&
              fwrite(&sz,             8,  1, f) == 1 &&
              fwrite(data,            1, len, f) == len);
    fclose(f);

    if (ok) { rename(tmp, path); return 0; }
    unlink(tmp);
    return -1;
}

/*
 * bf_kvcache_ancestry — walk the parent chain for a context hash.
 *
 * Fills hashes[0..depth-1] from newest to oldest (hashes[0] = ctx_hash).
 * Stops at root (all-zero parent) or when a file is missing.
 * Returns actual depth traversed.
 */
int bf_kvcache_ancestry(const uint8_t model_hash[32],
                        const uint8_t ctx_hash[32],
                        uint8_t (*hashes)[32], int max_depth) {
    static const uint8_t zero[32] = {0};
    uint8_t cur[32];
    memcpy(cur, ctx_hash, 32);
    int depth = 0;

    while (depth < max_depth && memcmp(cur, zero, 32) != 0) {
        memcpy(hashes[depth], cur, 32);
        depth++;

        char path[MAX_STORE_PATH];
        kv_chain_path_(model_hash, cur, path, sizeof(path));

        FILE *f = fopen(path, "rb");
        if (!f) break;

        uint32_t magic;
        uint8_t  parent[32];
        if (fread(&magic,  4,  1, f) != 1 || magic != KV2_MAGIC ||
            fread(parent,  1, 32, f) != 32) {
            fclose(f); break;
        }
        fclose(f);
        memcpy(cur, parent, 32);
    }
    return depth;
}
