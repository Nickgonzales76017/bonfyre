/*
 * bf_embed_pack.c — Pack format for the embed object store
 *
 * git uses packfiles to consolidate loose objects. We do the same.
 *
 * A .bfpack file is a single mmap-able file:
 *
 *   Header (48 bytes):
 *     [0-3]   uint32 magic    = PACK_MAGIC (0x4B504642 "BFPK")
 *     [4-7]   uint32 version  = 1
 *     [8-11]  uint32 n        — number of vectors
 *     [12-15] uint32 dim      — dimension (uniform per pack)
 *     [16-23] uint64 idx_off  — byte offset of index section (= 48)
 *     [24-31] uint64 dat_off  — byte offset of data section
 *     [32-47] uint8[16] reserved
 *
 *   Index (sorted ascending by hash, n × 40 bytes):
 *     [0-31]  uint8[32] hash      — SHA-256 of original input text
 *     [32-39] uint64    data_idx  — i-th vector in data section
 *
 *   Data:
 *     float32[dim] × n  — packed, same order as index
 *
 * Lookup: O(log n) binary search with zero file I/O — just memcmp and
 * pointer arithmetic into the mmap'd pages. Multiple processes mapping
 * the same file share physical pages via the kernel page cache.
 * No daemon, no IPC, no coordination.
 *
 * GC: after packing, loose .bfembed files that exist in the pack can be
 * removed. bf_embed_lookup() falls back to loose files on pack miss, so
 * the transition is safe.
 */
#define _DEFAULT_SOURCE
#include "include/bonfyre.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <math.h>

#define PACK_MAGIC     0x4B504642u   /* "BFPK" */
#define PACK_VERSION   1u
#define PACK_HDR_SIZE  48u
#define PACK_IDX_ENTRY 40u           /* 32 hash + 8 data_idx */
#define EMBED_MAGIC_   0x45424643u   /* must match bf_embed_cache.c */
#define MAX_PP         4096

/* ── helpers ─────────────────────────────────────────────────── */

static const char *home_p_(void) {
    const char *h = getenv("HOME");
    return h ? h : "/tmp";
}

static void embeds_dir_p_(char *buf, size_t sz) {
    snprintf(buf, sz, "%s/.local/share/bonfyre/embeds", home_p_());
}

static int hex_to_hash_p_(const char *hex, uint8_t out[32]) {
    if (strlen(hex) < 64) return -1;
    for (int i = 0; i < 32; i++) {
        unsigned v;
        if (sscanf(hex + i * 2, "%02x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

/* ── loose-object collection ─────────────────────────────────── */

typedef struct { uint8_t hash[32]; char path[MAX_PP]; uint32_t dim; float *data; } PackEnt_;

static int cmp_pack_ent_(const void *a, const void *b) {
    return memcmp(((const PackEnt_ *)a)->hash, ((const PackEnt_ *)b)->hash, 32);
}

/*
 * bf_embed_pack_build — consolidate loose .bfembed files into one pack.
 *
 * Scans ~/.local/share/bonfyre/embeds/ for *.bfembed, reads each,
 * sorts by hash, writes a single pack file at pack_path.
 * Mixed-dim files are skipped. *out_n = vectors written.
 * Returns 0 on success, -1 on I/O error.
 */
int bf_embed_pack_build(const char *pack_path, uint32_t *out_n) {
    char dir[MAX_PP];
    embeds_dir_p_(dir, sizeof(dir));

    DIR *d = opendir(dir);
    if (!d) { *out_n = 0; return 0; }  /* empty store — nothing to pack */

    size_t cap = 64;
    PackEnt_ *ents = malloc(cap * sizeof(PackEnt_));
    if (!ents) { closedir(d); return -1; }
    size_t n = 0;
    uint32_t pack_dim = 0;

    struct dirent *de;
    while ((de = readdir(d))) {
        const char *name = de->d_name;
        size_t nlen = strlen(name);
        /* Loose embeds are named exactly: <64 hex>.bfembed = 72 chars */
        if (nlen != 72 || strcmp(name + 64, ".bfembed") != 0) continue;

        PackEnt_ e;
        memset(&e, 0, sizeof(e));
        if (hex_to_hash_p_(name, e.hash) != 0) continue;
        snprintf(e.path, sizeof(e.path), "%s/%s", dir, name);

        FILE *f = fopen(e.path, "rb");
        if (!f) continue;

        uint32_t magic, dim;
        if (fread(&magic, 4, 1, f) != 1 || magic != EMBED_MAGIC_ ||
            fread(&dim,   4, 1, f) != 1 || dim == 0 || dim > 65536) {
            fclose(f); continue;
        }

        if (pack_dim == 0) pack_dim = dim;
        if (dim != pack_dim) { fclose(f); continue; } /* skip mixed-dim */

        e.dim  = dim;
        e.data = malloc(dim * sizeof(float));
        if (!e.data || fread(e.data, sizeof(float), dim, f) != (size_t)dim) {
            free(e.data); fclose(f); continue;
        }
        fclose(f);

        if (n == cap) {
            cap *= 2;
            PackEnt_ *tmp = realloc(ents, cap * sizeof(PackEnt_));
            if (!tmp) { free(e.data); break; }
            ents = tmp;
        }
        ents[n++] = e;
    }
    closedir(d);

    if (n == 0) { free(ents); *out_n = 0; return 0; }

    qsort(ents, n, sizeof(PackEnt_), cmp_pack_ent_);

    /* Atomic write: tmp + rename */
    char tmp[MAX_PP + 4];
    snprintf(tmp, sizeof(tmp), "%s.tmp", pack_path);
    FILE *pf = fopen(tmp, "wb");
    if (!pf) {
        for (size_t i = 0; i < n; i++) free(ents[i].data);
        free(ents);
        return -1;
    }

    /* Header */
    uint32_t hdr_magic   = PACK_MAGIC;
    uint32_t hdr_version = PACK_VERSION;
    uint32_t hdr_n       = (uint32_t)n;
    uint32_t hdr_dim     = pack_dim;
    uint64_t hdr_idx_off = PACK_HDR_SIZE;
    uint64_t hdr_dat_off = PACK_HDR_SIZE + (uint64_t)n * PACK_IDX_ENTRY;
    uint8_t  hdr_pad[16] = {0};
    fwrite(&hdr_magic,   4, 1, pf);
    fwrite(&hdr_version, 4, 1, pf);
    fwrite(&hdr_n,       4, 1, pf);
    fwrite(&hdr_dim,     4, 1, pf);
    fwrite(&hdr_idx_off, 8, 1, pf);
    fwrite(&hdr_dat_off, 8, 1, pf);
    fwrite(hdr_pad,     16, 1, pf);

    /* Index (hash + sequential data_idx) */
    for (size_t i = 0; i < n; i++) {
        uint64_t data_idx = (uint64_t)i;
        fwrite(ents[i].hash, 1, 32, pf);
        fwrite(&data_idx,    8,  1, pf);
    }

    /* Data (float32 vectors) */
    for (size_t i = 0; i < n; i++) {
        fwrite(ents[i].data, sizeof(float), pack_dim, pf);
        free(ents[i].data);
    }
    free(ents);

    int ok = (ferror(pf) == 0);
    fclose(pf);
    if (!ok) { unlink(tmp); return -1; }
    rename(tmp, pack_path);
    *out_n = (uint32_t)n;
    return 0;
}

/*
 * bf_embed_pack_open — mmap a .bfpack file.
 *
 * The entire file is mapped MAP_SHARED|PROT_READ. All processes that map
 * the same path share the same physical pages via the kernel page cache —
 * no copies, no IPC. Reading a cached vector is one binary search +
 * one memcpy from mmap'd memory.
 */
int bf_embed_pack_open(BfEmbedPack *pack, const char *pack_path) {
    memset(pack, 0, sizeof(*pack));
    int fd = open(pack_path, O_RDONLY);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)PACK_HDR_SIZE) {
        close(fd); return -1;
    }

    void *base = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { close(fd); return -1; }

    const uint8_t *b = (const uint8_t *)base;
    uint32_t magic, version, n, dim;
    uint64_t idx_off, dat_off;
    memcpy(&magic,   b,      4);
    memcpy(&version, b +  4, 4);
    memcpy(&n,       b +  8, 4);
    memcpy(&dim,     b + 12, 4);
    memcpy(&idx_off, b + 16, 8);
    memcpy(&dat_off, b + 24, 8);

    if (magic   != PACK_MAGIC ||
        version != PACK_VERSION ||
        n == 0 || dim == 0 || dim > 65536 ||
        idx_off != PACK_HDR_SIZE ||
        dat_off < idx_off + (uint64_t)n * PACK_IDX_ENTRY ||
        (off_t)(dat_off + (uint64_t)n * dim * 4) > st.st_size) {
        munmap(base, (size_t)st.st_size);
        close(fd);
        return -1;
    }

    pack->fd         = fd;
    pack->base       = base;
    pack->map_size   = (size_t)st.st_size;
    pack->n          = n;
    pack->dim        = dim;
    pack->index_base = b + idx_off;
    pack->data_base  = (const float *)(b + dat_off);
    return 0;
}

/*
 * bf_embed_pack_lookup — O(log n) binary search.
 *
 * Returns a pointer directly into the mmap'd data region (valid until
 * bf_embed_pack_close). NULL on miss. Do NOT free the returned pointer.
 * Cost: ~log2(n) memcmp calls + zero file I/O on hit.
 */
const float *bf_embed_pack_lookup(const BfEmbedPack *pack,
                                   const uint8_t hash[32]) {
    if (!pack || !pack->base || pack->n == 0) return NULL;
    uint32_t lo = 0, hi = pack->n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        const uint8_t *entry = pack->index_base + (size_t)mid * PACK_IDX_ENTRY;
        int c = memcmp(hash, entry, 32);
        if (c == 0) {
            uint64_t data_idx;
            memcpy(&data_idx, entry + 32, 8);
            return pack->data_base + data_idx * pack->dim;
        }
        if (c < 0) hi = mid;
        else       lo = mid + 1;
    }
    return NULL;
}

/* bf_embed_pack_close — munmap and close. */
void bf_embed_pack_close(BfEmbedPack *pack) {
    if (!pack) return;
    if (pack->base && pack->map_size)
        munmap(pack->base, pack->map_size);
    if (pack->fd > 0)
        close(pack->fd);
    memset(pack, 0, sizeof(*pack));
}

/* ── Index-based accessors (canonical, 40B stride) ─────────────────── */

/* bf_embed_pack_hash_at — pointer to hash[32] of entry idx (in mmap).
 * Returns NULL on out-of-range. Pointer valid while pack is open. */
const uint8_t *bf_embed_pack_hash_at(const BfEmbedPack *pack, uint32_t idx) {
    if (!pack || !pack->index_base || idx >= pack->n) return NULL;
    return pack->index_base + (size_t)idx * PACK_IDX_ENTRY;
}

/* bf_embed_pack_vec_at — copy float[pack->dim] for entry idx into out.
 * out must be pre-allocated with at least pack->dim floats.
 * Returns 0 on success, -1 on error. */
int bf_embed_pack_vec_at(const BfEmbedPack *pack, uint32_t idx, float *out) {
    if (!pack || !pack->index_base || !pack->data_base || idx >= pack->n) return -1;
    const uint8_t *entry = pack->index_base + (size_t)idx * PACK_IDX_ENTRY;
    uint64_t data_idx;
    memcpy(&data_idx, entry + 32, 8);
    memcpy(out, pack->data_base + data_idx * pack->dim, pack->dim * sizeof(float));
    return 0;
}


/*
 * bf_embed_lookup_fast — pack-first, loose-file fallback.
 *
 * On pack hit: copies dim*4 bytes from mmap'd pages into a malloc'd
 * buffer (so the caller has a stable pointer after pack close). Zero
 * file I/O, just a memcpy from hot cache pages.
 *
 * On pack miss (or pack == NULL): falls through to bf_embed_lookup().
 *
 * Returns 0 on hit, -1 on miss. *out is malloc'd by callee; caller frees.
 */
int bf_embed_lookup_fast(const uint8_t hash[32], const BfEmbedPack *pack,
                         float **out, uint32_t *out_dim) {
    if (pack && pack->base) {
        const float *p = bf_embed_pack_lookup(pack, hash);
        if (p) {
            float *copy = malloc(pack->dim * sizeof(float));
            if (!copy) return -1;
            memcpy(copy, p, pack->dim * sizeof(float));
            *out     = copy;
            *out_dim = pack->dim;
            return 0;
        }
    }
    return bf_embed_lookup(hash, out, out_dim);
}

/* ── Pack v2: INT8 quantized format ───────────────────────────────────────
 *
 * Pack v2 uses per-vector INT8 quantization: each float32 vector is stored
 * as a scale (float32) + int8[dim].  For 384-dim MiniLM:
 *   v1 float32: 4 * 384 = 1536 B/vector
 *   v2 int8:    4 + 384  =  388 B/vector  (4.0× compression)
 *
 * 4.0× more vectors fit in the same page-cache budget, which means 4×
 * more ANN candidates per cache-warm access window.
 *
 * v2 on-disk layout (header identical to v1 except version=2):
 *   Header 48B     (same structure, version=2)
 *   Index  n×40B  (same: hash[32] + data_idx u64)
 *   Scales n×4B   (float32 per-vector scale, at dat_off)
 *   Data   n×dimB (int8 per element, at dat_off + n*4)
 *
 * The header's reserved[0..7] bytes store data2_off (uint64) pointing
 * to the int8 data section.  For v1 files those bytes are zero.
 */

#define PACK_VERSION2  2u

static void quantize_vec_(const float *v, uint32_t dim,
                          float *out_scale, int8_t *out_q) {
    float max_abs = 0.0f;
    for (uint32_t i = 0; i < dim; i++) {
        float a = fabsf(v[i]);
        if (a > max_abs) max_abs = a;
    }
    if (max_abs < 1e-9f) max_abs = 1e-9f;
    float scale = max_abs / 127.0f;
    *out_scale = scale;
    for (uint32_t i = 0; i < dim; i++) {
        float q = v[i] / scale;
        if (q >  127.0f) q =  127.0f;
        if (q < -127.0f) q = -127.0f;
        out_q[i] = (int8_t)(int)roundf(q);
    }
}

/*
 * bf_embed_pack_build_q8 — build INT8-quantized pack v2.
 *
 * Same as bf_embed_pack_build but quantizes each vector to INT8 + per-vector
 * scale before writing. Resulting file is ~4× smaller for 384-dim vectors.
 * Returns 0 on success; *out_n = vectors packed.
 */
int bf_embed_pack_build_q8(const char *pack_path, uint32_t *out_n) {
    char dir[MAX_PP];
    embeds_dir_p_(dir, sizeof(dir));

    DIR *d = opendir(dir);
    if (!d) { *out_n = 0; return 0; }

    size_t cap = 64;
    PackEnt_ *ents = malloc(cap * sizeof(PackEnt_));
    if (!ents) { closedir(d); return -1; }
    size_t n = 0;
    uint32_t pack_dim = 0;

    struct dirent *de;
    while ((de = readdir(d))) {
        const char *nm = de->d_name;
        size_t nl = strlen(nm);
        if (nl != 72 || strcmp(nm + 64, ".bfembed") != 0) continue;
        PackEnt_ e;
        memset(&e, 0, sizeof(e));
        if (hex_to_hash_p_(nm, e.hash) != 0) continue;
        snprintf(e.path, sizeof(e.path), "%s/%s", dir, nm);
        FILE *f = fopen(e.path, "rb");
        if (!f) continue;
        uint32_t magic, dim;
        if (fread(&magic, 4, 1, f) != 1 || magic != EMBED_MAGIC_ ||
            fread(&dim,   4, 1, f) != 1 || dim == 0 || dim > 65536) {
            fclose(f); continue;
        }
        if (pack_dim == 0) pack_dim = dim;
        if (dim != pack_dim) { fclose(f); continue; }
        e.dim  = dim;
        e.data = malloc(dim * sizeof(float));
        if (!e.data || fread(e.data, sizeof(float), dim, f) != (size_t)dim) {
            free(e.data); fclose(f); continue;
        }
        fclose(f);
        if (n == cap) {
            cap *= 2;
            PackEnt_ *tmp = realloc(ents, cap * sizeof(PackEnt_));
            if (!tmp) { free(e.data); break; }
            ents = tmp;
        }
        ents[n++] = e;
    }
    closedir(d);

    if (n == 0) { free(ents); *out_n = 0; return 0; }
    qsort(ents, n, sizeof(PackEnt_), cmp_pack_ent_);

    /* Compute section offsets */
    uint64_t idx_off    = PACK_HDR_SIZE;
    uint64_t scales_off = PACK_HDR_SIZE + (uint64_t)n * PACK_IDX_ENTRY;  /* dat_off */
    uint64_t data2_off  = scales_off + (uint64_t)n * 4;                  /* int8 data */

    char tmp[MAX_PP + 4];
    snprintf(tmp, sizeof(tmp), "%s.tmp", pack_path);
    FILE *pf = fopen(tmp, "wb");
    if (!pf) {
        for (size_t i = 0; i < n; i++) free(ents[i].data);
        free(ents); return -1;
    }

    /* Header (version=2; reserved[0..7] = data2_off) */
    uint32_t hdr_magic   = PACK_MAGIC;
    uint32_t hdr_version = PACK_VERSION2;
    uint32_t hdr_n       = (uint32_t)n;
    uint32_t hdr_dim     = pack_dim;
    /* dat_off = scales section; data2_off in reserved[0..7] */
    uint8_t  hdr_pad[16];
    memset(hdr_pad, 0, sizeof(hdr_pad));
    memcpy(hdr_pad, &data2_off, 8);  /* store data2_off in first 8 bytes of reserved */

    fwrite(&hdr_magic,   4, 1, pf);
    fwrite(&hdr_version, 4, 1, pf);
    fwrite(&hdr_n,       4, 1, pf);
    fwrite(&hdr_dim,     4, 1, pf);
    fwrite(&idx_off,     8, 1, pf);
    fwrite(&scales_off,  8, 1, pf);
    fwrite(hdr_pad,     16, 1, pf);

    /* Index (same format as v1) */
    for (size_t i = 0; i < n; i++) {
        uint64_t data_idx = (uint64_t)i;
        fwrite(ents[i].hash, 1, 32, pf);
        fwrite(&data_idx,    8,  1, pf);
    }

    /* Scales: float32[n] */
    int8_t *qbuf = malloc((size_t)pack_dim);
    for (size_t i = 0; i < n; i++) {
        float scale;
        quantize_vec_(ents[i].data, pack_dim, &scale, qbuf);
        fwrite(&scale, 4, 1, pf);
        /* temporarily reuse qbuf slot */
        (void)qbuf;
    }
    /* Data: int8[n * dim] */
    for (size_t i = 0; i < n; i++) {
        quantize_vec_(ents[i].data, pack_dim, &(float){0.0f}, qbuf);
        fwrite(qbuf, 1, (size_t)pack_dim, pf);
        free(ents[i].data);
    }
    free(qbuf);
    free(ents);

    int ok = (ferror(pf) == 0);
    fclose(pf);
    if (!ok) { unlink(tmp); return -1; }
    rename(tmp, pack_path);
    *out_n = (uint32_t)n;
    return 0;
}

/*
 * bf_embed_pack_get — unified dequantizing accessor for v1 and v2 packs.
 *
 * Always returns a malloc'd float32[dim] copy (caller frees). Handles
 * both v1 (memcpy) and v2 (dequantize int8 × scale).  This is the safe
 * API to use when you don’t know the pack version; use bf_embed_pack_lookup
 * only when you know you have v1 and want the zero-copy path.
 *
 * Returns 0 on hit, -1 on miss or error.
 */
int bf_embed_pack_get(const BfEmbedPack *pack, const uint8_t hash[32],
                      float **out, uint32_t *out_dim) {
    if (!pack || !pack->base || pack->n == 0) return -1;

    /* Binary search index */
    uint32_t lo = 0, hi = pack->n;
    uint64_t data_idx = 0;
    int found = 0;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        const uint8_t *entry = pack->index_base + (size_t)mid * PACK_IDX_ENTRY;
        int c = memcmp(hash, entry, 32);
        if (c == 0) {
            memcpy(&data_idx, entry + 32, 8);
            found = 1; break;
        }
        if (c < 0) hi = mid;
        else       lo = mid + 1;
    }
    if (!found) return -1;

    float *dst = malloc(pack->dim * sizeof(float));
    if (!dst) return -1;

    /* Check version field from header */
    uint32_t version;
    memcpy(&version, (const uint8_t *)pack->base + 4, 4);

    if (version == PACK_VERSION2) {
        /* v2: dat_base = scales, data2_off in reserved[0..7] */
        const float *scales = (const float *)pack->data_base;
        uint64_t data2_off;
        memcpy(&data2_off, (const uint8_t *)pack->base + 32, 8);  /* reserved[0..7] */
        const int8_t *qdata = (const int8_t *)((const uint8_t *)pack->base + data2_off);
        float scale = scales[data_idx];
        const int8_t *qvec = qdata + (size_t)data_idx * pack->dim;
        for (uint32_t i = 0; i < pack->dim; i++)
            dst[i] = (float)qvec[i] * scale;
    } else {
        /* v1: data_base = float32 data */
        memcpy(dst, pack->data_base + (size_t)data_idx * pack->dim,
               pack->dim * sizeof(float));
    }

    *out     = dst;
    *out_dim = pack->dim;
    return 0;
}
