/*
 * bf_bqfp.h — the .bqfp tensor file: one definition for every organ that
 * writes or reads one.
 *
 * bonfyre-quant writes it; bonfyre-sli and bonfyre-fpqx read it. Each used to
 * carry its own copy of the layout and its own idea of what a valid file was,
 * and one of them returned success on a file that ended early. The layout,
 * the checks and the block reconstruction live here once.
 *
 * Layout, little-endian, version 0:
 *
 *   u32 magic "QBFP"   u32 version   u32 tensor_count   u32 bits
 *   per tensor:
 *     char name[256]   u64 elements   u64 blocks   u32 tiles   u32 pad
 *     f32  codebook[tiles][16]
 *     block[blocks]:  f32 scale, f32 warp_norm, i8 e8[256], u8 tile[16]
 *
 * A block is 256 weights after random signs and a Walsh-Hadamard transform,
 * divided by their RMS (scale), mu-law warped, divided by the RMS of the
 * warped values (warp_norm) and multiplied by the lattice scale 8*bits. Each
 * group of eight is snapped to the E8 lattice at spacing `bits`: e8[] holds
 * twice the lattice point in units of that spacing, so half-integer points
 * are exact. tile[] names, per sixteen coefficients, the codebook entry
 * nearest to what the snap left over.
 *
 * Header-only on purpose: an organ includes it and needs nothing rebuilt.
 */
#ifndef BF_BQFP_H
#define BF_BQFP_H

#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BF_BQFP_MAGIC      0x50464251u
#define BF_BQFP_VERSION    0u
#define BF_BQFP_BLOCK_DIM  256
#define BF_BQFP_TILE_DIM   16
#define BF_BQFP_PAIRS      16
#define BF_BQFP_MAX_TILES  256
#define BF_BQFP_MAX_TENSORS 4096u
#define BF_BQFP_MU_BETA    8.0f

typedef struct {
    float   scale;
    float   warp_norm;
    int8_t  e8[BF_BQFP_BLOCK_DIM];
    uint8_t tile[BF_BQFP_PAIRS];
} BfBqfpBlock;

typedef struct {
    char     name[256];
    uint64_t elements;
    uint64_t blocks;
    uint32_t tiles;
} BfBqfpTensor;

/* ── what the numbers mean ───────────────────────────────────────────────── */

static inline float bf_bqfp_lattice_scale(uint32_t bits) { return 8.0f * (float)bits; }
/* One unit of e8[] in the scaled domain: half the lattice spacing. */
static inline float bf_bqfp_e8_unit(uint32_t bits) { return (float)bits / 2.0f; }

static inline uint64_t bf_bqfp_seed(const char *name) {
    uint64_t seed = 0x12345678ULL;
    for (const char *p = name; *p; p++) seed = seed * 31 + (uint64_t)(unsigned char)*p;
    return seed;
}

static inline uint64_t bf_bqfp_xorshift64(uint64_t s) { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }

/* The sign pattern is its own inverse. */
static inline void bf_bqfp_signs(float *d, int n, uint64_t seed) {
    uint64_t s = seed;
    for (int i = 0; i < n; i++) { s = bf_bqfp_xorshift64(s); d[i] *= (s & 1) ? 1.0f : -1.0f; }
}

/* Orthonormal, so applying it twice returns the input. */
static inline void bf_bqfp_fwht(float *d, int n) {
    for (int len = 1; len < n; len <<= 1)
        for (int i = 0; i < n; i += len << 1)
            for (int j = 0; j < len; j++) {
                float a = d[i + j], b = d[i + j + len];
                d[i + j] = a + b; d[i + j + len] = a - b;
            }
    float norm = 1.0f / sqrtf((float)n);
    for (int i = 0; i < n; i++) d[i] *= norm;
}

static inline float bf_bqfp_mu_warp(float x, float beta) {
    float sign = (x >= 0) ? 1.0f : -1.0f;
    return sign * logf(1.0f + beta * fabsf(x)) / logf(1.0f + beta);
}

static inline float bf_bqfp_mu_unwarp(float y, float beta) {
    float sign = (y >= 0) ? 1.0f : -1.0f;
    return sign * (expf(fabsf(y) * logf(1.0f + beta)) - 1.0f) / beta;
}

/* ── reading ─────────────────────────────────────────────────────────────── */

/* Reads and checks the file header. Returns 0 and fills tensor_count and
 * bits, or -1 with a reason. */
static inline int bf_bqfp_read_header(FILE *f, uint32_t *tensor_count, uint32_t *bits, const char **why) {
    uint32_t hdr[4];
    if (fread(hdr, 4, 4, f) != 4) { *why = "shorter than a header"; return -1; }
    if (hdr[0] != BF_BQFP_MAGIC) { *why = "not a .bqfp file"; return -1; }
    if (hdr[1] != BF_BQFP_VERSION) { *why = "unknown version"; return -1; }
    if (hdr[2] == 0 || hdr[2] > BF_BQFP_MAX_TENSORS) { *why = "tensor count out of range"; return -1; }
    if (hdr[3] < 2 || hdr[3] > 8) { *why = "bits out of range"; return -1; }
    *tensor_count = hdr[2];
    *bits = hdr[3];
    return 0;
}

/* Reads one tensor record and checks that everything it promises is in the
 * file: a terminated name, a block for every 256 weights, a codebook no
 * larger than a tile index can address, and the bytes for both. Leaves the
 * file positioned at the codebook. */
static inline int bf_bqfp_read_tensor(FILE *f, BfBqfpTensor *t, const char **why) {
    uint32_t pad;
    long start, end;
    uint64_t book, payload;
    if (fread(t->name, 1, 256, f) != 256 || fread(&t->elements, 8, 1, f) != 1 ||
        fread(&t->blocks, 8, 1, f) != 1 || fread(&t->tiles, 4, 1, f) != 1 || fread(&pad, 4, 1, f) != 1) {
        *why = "tensor record cut short"; return -1;
    }
    if (!memchr(t->name, 0, 256)) { *why = "tensor name is not terminated"; return -1; }
    if (t->elements == 0) { *why = "tensor holds no weights"; return -1; }
    if (t->tiles == 0 || t->tiles > BF_BQFP_MAX_TILES) { *why = "codebook size out of range"; return -1; }
    if (t->blocks != t->elements / BF_BQFP_BLOCK_DIM + (t->elements % BF_BQFP_BLOCK_DIM != 0)) {
        *why = "block count does not cover the weights"; return -1;
    }
    book = (uint64_t)t->tiles * BF_BQFP_TILE_DIM * sizeof(float);
    if (t->blocks > (UINT64_MAX - book) / sizeof(BfBqfpBlock)) { *why = "block count overflows"; return -1; }
    payload = book + t->blocks * sizeof(BfBqfpBlock);
    if (payload > (uint64_t)LONG_MAX || (start = ftell(f)) < 0 || fseek(f, 0, SEEK_END) != 0) {
        *why = "cannot measure the file"; return -1;
    }
    end = ftell(f);
    if (fseek(f, start, SEEK_SET) != 0 || end < start) { *why = "cannot measure the file"; return -1; }
    if (payload > (uint64_t)(end - start)) { *why = "file ends before the tensor's blocks do"; return -1; }
    return 0;
}

/* Skips the codebook and blocks of a tensor whose record was just read. */
static inline int bf_bqfp_skip_payload(FILE *f, const BfBqfpTensor *t) {
    uint64_t payload = (uint64_t)t->tiles * BF_BQFP_TILE_DIM * sizeof(float) + t->blocks * sizeof(BfBqfpBlock);
    return fseek(f, (long)payload, SEEK_CUR);
}

/* Rebuilds the weights of one block. */
static inline void bf_bqfp_decode_block(const BfBqfpBlock *blk, const float *tiles, uint32_t tile_count,
                                        float *out, size_t dim, uint64_t seed, uint32_t bits) {
    float corrected[BF_BQFP_BLOCK_DIM];
    float lattice = bf_bqfp_lattice_scale(bits), unit = bf_bqfp_e8_unit(bits);
    for (int p = 0; p < BF_BQFP_PAIRS; p++) {
        uint32_t ti = blk->tile[p];
        const float *tile = ti < tile_count ? tiles + (size_t)ti * BF_BQFP_TILE_DIM : NULL;
        for (int d = 0; d < BF_BQFP_TILE_DIM; d++)
            corrected[p * BF_BQFP_TILE_DIM + d] =
                (float)blk->e8[p * BF_BQFP_TILE_DIM + d] * unit + (tile ? tile[d] : 0.0f);
    }
    for (int i = 0; i < BF_BQFP_BLOCK_DIM; i++)
        corrected[i] = bf_bqfp_mu_unwarp(corrected[i] / lattice * blk->warp_norm, BF_BQFP_MU_BETA) * blk->scale;
    bf_bqfp_fwht(corrected, BF_BQFP_BLOCK_DIM);
    bf_bqfp_signs(corrected, BF_BQFP_BLOCK_DIM, seed);
    memcpy(out, corrected, (dim < BF_BQFP_BLOCK_DIM ? dim : BF_BQFP_BLOCK_DIM) * sizeof(float));
}

/* Reads the first tensor of a file whole: its record, its codebook and,
 * when weights is not NULL, every block decoded to float32. A file that
 * ends early is an error, never a shorter tensor. The caller frees
 * *codebook and *weights. */
static inline int bf_bqfp_load_first(const char *path, BfBqfpTensor *t, uint32_t *bits,
                                     float **codebook, float **weights, const char **why) {
    uint32_t tensor_count;
    FILE *f = fopen(path, "rb");
    *codebook = NULL;
    if (weights) *weights = NULL;
    if (!f) { *why = "cannot open"; return -1; }
    if (bf_bqfp_read_header(f, &tensor_count, bits, why) != 0 || bf_bqfp_read_tensor(f, t, why) != 0) {
        fclose(f); return -1;
    }
    size_t book = (size_t)t->tiles * BF_BQFP_TILE_DIM;
    *codebook = (float *)malloc(book * sizeof(float));
    if (!*codebook || fread(*codebook, sizeof(float), book, f) != book) {
        *why = "codebook cut short"; goto fail;
    }
    if (weights) {
        uint64_t seed = bf_bqfp_seed(t->name);
        *weights = (float *)malloc((size_t)t->elements * sizeof(float));
        if (!*weights) { *why = "out of memory"; goto fail; }
        for (uint64_t b = 0; b < t->blocks; b++) {
            BfBqfpBlock blk;
            size_t off = (size_t)b * BF_BQFP_BLOCK_DIM;
            size_t dim = off + BF_BQFP_BLOCK_DIM <= t->elements ? BF_BQFP_BLOCK_DIM : (size_t)t->elements - off;
            if (fread(&blk, sizeof(blk), 1, f) != 1) { *why = "a block is missing"; goto fail; }
            bf_bqfp_decode_block(&blk, *codebook, t->tiles, *weights + off, dim, seed ^ b, *bits);
        }
    }
    fclose(f);
    return 0;
fail:
    fclose(f);
    free(*codebook); *codebook = NULL;
    if (weights) { free(*weights); *weights = NULL; }
    return -1;
}

/* ── writing ─────────────────────────────────────────────────────────────── */

/* Writes a file holding one tensor. Returns 0, or -1 having removed
 * whatever part of the file was written. */
static inline int bf_bqfp_write_one(const char *path, const char *name, uint32_t bits, uint64_t elements,
                                    const float *codebook, uint32_t tiles,
                                    const BfBqfpBlock *blocks, uint64_t block_count) {
    uint32_t header[4] = { BF_BQFP_MAGIC, BF_BQFP_VERSION, 1, bits };
    char name_field[256];
    uint32_t pad = 0;
    FILE *out;
    int ok;
    if (bits < 2 || bits > 8 || tiles == 0 || tiles > BF_BQFP_MAX_TILES || elements == 0 ||
        block_count != elements / BF_BQFP_BLOCK_DIM + (elements % BF_BQFP_BLOCK_DIM != 0) ||
        strlen(name) >= sizeof(name_field)) return -1;
    memset(name_field, 0, sizeof(name_field));
    memcpy(name_field, name, strlen(name));
    out = fopen(path, "wb");
    if (!out) return -1;
    ok = fwrite(header, 4, 4, out) == 4 && fwrite(name_field, 1, 256, out) == 256 &&
         fwrite(&elements, 8, 1, out) == 1 && fwrite(&block_count, 8, 1, out) == 1 &&
         fwrite(&tiles, 4, 1, out) == 1 && fwrite(&pad, 4, 1, out) == 1 &&
         fwrite(codebook, sizeof(float), (size_t)tiles * BF_BQFP_TILE_DIM, out) == (size_t)tiles * BF_BQFP_TILE_DIM &&
         fwrite(blocks, sizeof(BfBqfpBlock), (size_t)block_count, out) == (size_t)block_count;
    if (fclose(out) != 0) ok = 0;
    if (!ok) { remove(path); return -1; }
    return 0;
}

#endif /* BF_BQFP_H */
