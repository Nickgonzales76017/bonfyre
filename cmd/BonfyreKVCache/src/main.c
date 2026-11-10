/*
 * bonfyre-kvcache — v8 RLF KV cache compression.
 *
 * Compresses key/value projection tensors for LLM inference using
 * the same E8 + μ-law + 16D RVQ pipeline as bonfyre-quant.
 *
 * KV cache is harder than weight quantization because errors compound
 * across layers and tokens. Results at 3-bit: +49.7% PPL regression.
 * At 4-bit: +23.6%. Use 4+ bits for acceptable quality.
 *
 * Usage:
 *   bonfyre-kvcache compress  <input.bin> <output.bfkv> [--bits 4]
 *   bonfyre-kvcache roundtrip <input.bin> [--bits 4]
 *   bonfyre-kvcache benchmark <input.bin> [--bits 4]
 *   bonfyre-kvcache --help
 *
 * Benchmark results (Qwen-0.5B, WikiText-2):
 *   3-bit KV: PPL 17.89 (+49.7%), avg cos 0.9997
 *   4-bit KV: PPL 14.77 (+23.6%), avg cos 0.9999
 */
/* clock_gettime is POSIX and hidden under -std=c11 without this. */
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
/* mkdtemp and nanosecond file identity are provided by the native libc. */
#define _DARWIN_C_SOURCE
#endif

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <math.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <limits.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <bonfyre.h>
#include <bf_jsonw.h>

/* ═══════════════════════════════════════════════════════════════════
 * Constants (shared with bonfyre-quant)
 * ═══════════════════════════════════════════════════════════════════ */

#define BLOCK_DIM       256
#define E8_DIM          8
#define E8_GROUPS       32
#define E8_PAIRS        16
#define TILE_DIM        16
#define RVQ_TILES       256
#define RVQ_ITERS       20
#define MU_BETA         8.0f
#define MAX_TRAIN       8192

/* ═══════════════════════════════════════════════════════════════════
 * Walsh-Hadamard + PRNG (identical to bonfyre-quant)
 * ═══════════════════════════════════════════════════════════════════ */

static void fwht(float *data, int n) {
    for (int len = 1; len < n; len <<= 1) {
        for (int i = 0; i < n; i += len << 1) {
            for (int j = 0; j < len; j++) {
                float a = data[i + j];
                float b = data[i + j + len];
                data[i + j]       = a + b;
                data[i + j + len] = a - b;
            }
        }
    }
    float norm = 1.0f / sqrtf((float)n);
    for (int i = 0; i < n; i++) data[i] *= norm;
}

static uint64_t xorshift64(uint64_t s) {
    s ^= s << 13; s ^= s >> 7; s ^= s << 17;
    return s;
}

static void random_signs(float *data, int n, uint64_t seed) {
    uint64_t s = seed;
    for (int i = 0; i < n; i++) {
        s = xorshift64(s);
        data[i] *= (s & 1) ? 1.0f : -1.0f;
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * μ-law Companding
 * ═══════════════════════════════════════════════════════════════════ */

static float mu_warp(float x, float beta) {
    float log1pb = logf(1.0f + beta);
    return (x >= 0 ? 1.0f : -1.0f) * logf(1.0f + beta * fabsf(x)) / log1pb;
}

static float mu_unwarp(float y, float beta) {
    float log1pb = logf(1.0f + beta);
    return (y >= 0 ? 1.0f : -1.0f) * (expf(fabsf(y) * log1pb) - 1.0f) / beta;
}

/* ═══════════════════════════════════════════════════════════════════
 * E8 Lattice Snap
 * ═══════════════════════════════════════════════════════════════════ */

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>

static void e8_snap(const float *x, float *out) {
    float32x4_t x_lo = vld1q_f32(x);
    float32x4_t x_hi = vld1q_f32(x + 4);
    float32x4_t half = vdupq_n_f32(0.5f);

    float32x4_t r_lo = vrndnq_f32(x_lo);
    float32x4_t r_hi = vrndnq_f32(x_hi);
    int32x4_t   i_lo = vcvtq_s32_f32(r_lo);
    int32x4_t   i_hi = vcvtq_s32_f32(r_hi);

    int32x4_t s4 = vaddq_s32(i_lo, i_hi);
    int32x2_t s2 = vadd_s32(vget_low_s32(s4), vget_high_s32(s4));
    int32x2_t s1 = vpadd_s32(s2, s2);
    int sum0 = vget_lane_s32(s1, 0);

    float c0[8];
    vst1q_f32(c0, r_lo); vst1q_f32(c0 + 4, r_hi);
    if (sum0 & 1) {
        float best_margin = -1.0f; int best_idx = 0;
        for (int d = 0; d < 8; d++) {
            float m = fabsf(x[d] - c0[d]);
            if (m > best_margin) { best_margin = m; best_idx = d; }
        }
        c0[best_idx] += (x[best_idx] > c0[best_idx]) ? 1.0f : -1.0f;
    }

    float32x4_t h_lo = vaddq_f32(vrndmq_f32(x_lo), half);
    float32x4_t h_hi = vaddq_f32(vrndmq_f32(x_hi), half);
    int32x4_t f_lo = vcvtq_s32_f32(vrndmq_f32(h_lo));
    int32x4_t f_hi = vcvtq_s32_f32(vrndmq_f32(h_hi));
    int32x4_t fs = vaddq_s32(f_lo, f_hi);
    int32x2_t fs2 = vadd_s32(vget_low_s32(fs), vget_high_s32(fs));
    int32x2_t fs1 = vpadd_s32(fs2, fs2);
    int fsum1 = vget_lane_s32(fs1, 0);

    float c1[8];
    vst1q_f32(c1, h_lo); vst1q_f32(c1 + 4, h_hi);
    if (fsum1 & 1) {
        float best_margin = -1.0f; int best_idx = 0;
        for (int d = 0; d < 8; d++) {
            float m = fabsf(x[d] - c1[d]);
            if (m > best_margin) { best_margin = m; best_idx = d; }
        }
        c1[best_idx] += (x[best_idx] > c1[best_idx]) ? 1.0f : -1.0f;
    }

    float d0 = 0, d1 = 0;
    for (int d = 0; d < 8; d++) {
        float e0 = x[d] - c0[d]; d0 += e0 * e0;
        float e1 = x[d] - c1[d]; d1 += e1 * e1;
    }
    const float *best = (d0 <= d1) ? c0 : c1;
    for (int d = 0; d < 8; d++) out[d] = best[d];
}

#else
static void e8_snap(const float *x, float *out) {
    float c0[8], c1[8];
    int sum0 = 0;
    for (int d = 0; d < 8; d++) { c0[d] = roundf(x[d]); sum0 += (int)c0[d]; }
    if (sum0 & 1) {
        float best = -1.0f; int wi = 0;
        for (int d = 0; d < 8; d++) {
            float m = fabsf(x[d] - c0[d]);
            if (m > best) { best = m; wi = d; }
        }
        c0[wi] += (x[wi] > c0[wi]) ? 1.0f : -1.0f;
    }
    int fsum = 0;
    for (int d = 0; d < 8; d++) {
        c1[d] = floorf(x[d]) + 0.5f; fsum += (int)floorf(c1[d]);
    }
    if (fsum & 1) {
        float best = -1.0f; int wi = 0;
        for (int d = 0; d < 8; d++) {
            float m = fabsf(x[d] - c1[d]);
            if (m > best) { best = m; wi = d; }
        }
        c1[wi] += (x[wi] > c1[wi]) ? 1.0f : -1.0f;
    }
    float d0 = 0, d1 = 0;
    for (int d = 0; d < 8; d++) {
        float e0 = x[d] - c0[d]; d0 += e0 * e0;
        float e1 = x[d] - c1[d]; d1 += e1 * e1;
    }
    const float *best = (d0 <= d1) ? c0 : c1;
    for (int d = 0; d < 8; d++) out[d] = best[d];
}
#endif

/* ═══════════════════════════════════════════════════════════════════
 * 16D RVQ (K-means)
 * ═══════════════════════════════════════════════════════════════════ */

static inline float dist16d(const float *a, const float *b, float best) {
    float d = 0;
    for (int i = 0; i < 8; i++) { float e = a[i] - b[i]; d += e * e; }
    if (d >= best) return d;
    for (int i = 8; i < TILE_DIM; i++) { float e = a[i] - b[i]; d += e * e; }
    return d;
}

static int find_nearest_tile(const float *vec, const float *tiles, int k, int seed) {
    float best_d = FLT_MAX; int best = 0;
    if (seed >= 0 && seed < k) {
        best_d = dist16d(vec, tiles + seed * TILE_DIM, FLT_MAX); best = seed;
    }
    for (int t = 0; t < k; t++) {
        if (t == seed) continue;
        float d = dist16d(vec, tiles + t * TILE_DIM, best_d);
        if (d < best_d) { best_d = d; best = t; }
    }
    return best;
}

static void learn_tiles(const float *residuals, size_t n_pairs,
                        float *tiles, int k) {
    if (n_pairs == 0 || k == 0) return;
    size_t train_n = (n_pairs > MAX_TRAIN) ? MAX_TRAIN : n_pairs;
    size_t train_step = n_pairs / train_n;
    if (train_step < 1) train_step = 1;

    size_t step = n_pairs / (size_t)k;
    if (step < 1) step = 1;
    for (int t = 0; t < k; t++) {
        size_t idx = (size_t)t * step;
        if (idx >= n_pairs) idx = n_pairs - 1;
        memcpy(tiles + t * TILE_DIM, residuals + idx * TILE_DIM,
               TILE_DIM * sizeof(float));
    }

    int *assign = (int *)malloc(train_n * sizeof(int));
    float *sums = (float *)calloc((size_t)k * TILE_DIM, sizeof(float));
    int *counts = (int *)calloc((size_t)k, sizeof(int));

    for (int iter = 0; iter < RVQ_ITERS; iter++) {
        for (size_t vi = 0; vi < train_n; vi++) {
            size_t v = vi * train_step;
            if (v >= n_pairs) v = n_pairs - 1;
            assign[vi] = find_nearest_tile(residuals + v * TILE_DIM, tiles, k, -1);
        }
        memset(sums, 0, (size_t)k * TILE_DIM * sizeof(float));
        memset(counts, 0, (size_t)k * sizeof(int));
        for (size_t vi = 0; vi < train_n; vi++) {
            int t = assign[vi];
            size_t v = vi * train_step;
            if (v >= n_pairs) v = n_pairs - 1;
            for (int d = 0; d < TILE_DIM; d++)
                sums[t * TILE_DIM + d] += residuals[v * TILE_DIM + d];
            counts[t]++;
        }
        for (int t = 0; t < k; t++) {
            if (counts[t] > 0) {
                for (int d = 0; d < TILE_DIM; d++)
                    tiles[t * TILE_DIM + d] =
                        sums[t * TILE_DIM + d] / (float)counts[t];
            }
        }
    }
    free(assign); free(sums); free(counts);
}

/* ═══════════════════════════════════════════════════════════════════
 * KV Block Encode / Decode
 *
 * KV tensors have smaller dims than weight tensors (typically
 * head_dim=64..128 × n_heads=4..32). We pad to BLOCK_DIM and
 * operate on the same RLF pipeline, but with separate codebooks
 * per-layer for better adaptation.
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float   scale;
    float   warp_norm;
    float   e8_points[BLOCK_DIM];
    uint8_t tile_idx[E8_PAIRS];
} KVBlock;

static void encode_kv_block(const float *input, size_t dim, KVBlock *kb,
                             uint64_t seed, float lattice_scale) {
    float buf[BLOCK_DIM];
    memset(buf, 0, sizeof(buf));
    if (dim > BLOCK_DIM) dim = BLOCK_DIM;
    memcpy(buf, input, dim * sizeof(float));

    random_signs(buf, BLOCK_DIM, seed);
    fwht(buf, BLOCK_DIM);

    float rms = 0;
    for (int i = 0; i < BLOCK_DIM; i++) rms += buf[i] * buf[i];
    rms = sqrtf(rms / (float)BLOCK_DIM);
    if (rms < 1e-10f) rms = 1e-10f;
    kb->scale = rms;
    for (int i = 0; i < BLOCK_DIM; i++) buf[i] /= rms;

    float warped[BLOCK_DIM];
    for (int i = 0; i < BLOCK_DIM; i++) warped[i] = mu_warp(buf[i], MU_BETA);
    float wnorm = 0;
    for (int i = 0; i < BLOCK_DIM; i++) wnorm += warped[i] * warped[i];
    wnorm = sqrtf(wnorm / (float)BLOCK_DIM);
    if (wnorm < 1e-10f) wnorm = 1e-10f;
    kb->warp_norm = wnorm;

    for (int g = 0; g < E8_GROUPS; g++) {
        float scaled[E8_DIM];
        for (int d = 0; d < E8_DIM; d++)
            scaled[d] = warped[g * E8_DIM + d] / wnorm * lattice_scale;
        e8_snap(scaled, kb->e8_points + g * E8_DIM);
    }
}

static void decode_kv_block(const KVBlock *kb, const float *tiles, int ek,
                             float *output, size_t dim, uint64_t seed,
                             float lattice_scale) {
    float corrected[BLOCK_DIM];
    for (int p = 0; p < E8_PAIRS; p++) {
        int ti = kb->tile_idx[p];
        const float *tile = (ti >= 0 && ti < ek) ? tiles + ti * TILE_DIM : NULL;
        for (int d = 0; d < TILE_DIM; d++) {
            corrected[p * TILE_DIM + d] = kb->e8_points[p * TILE_DIM + d] +
                                          (tile ? tile[d] : 0.0f);
        }
    }
    for (int i = 0; i < BLOCK_DIM; i++) {
        float lat_val = corrected[i] / lattice_scale * kb->warp_norm;
        corrected[i] = mu_unwarp(lat_val, MU_BETA) * kb->scale;
    }
    fwht(corrected, BLOCK_DIM);
    random_signs(corrected, BLOCK_DIM, seed);

    if (dim > BLOCK_DIM) dim = BLOCK_DIM;
    memcpy(output, corrected, dim * sizeof(float));
}

/* ═══════════════════════════════════════════════════════════════════
 * KV Tensor Roundtrip (simulates per-layer compression)
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    double cosine;
    double rmse;
    float  bpw;
    size_t n_blocks;
    int    effective_k;
} KVResult;

static KVResult kv_roundtrip(const float *data, size_t total,
                              const char *name, int bits) {
    KVResult res = {0};
    size_t n_blocks = (total + BLOCK_DIM - 1) / BLOCK_DIM;
    res.n_blocks = n_blocks;
    float lattice_scale = 8.0f * (float)bits;

    uint64_t seed = 0x4B565EEDULL;  /* "KVSEED" */
    if (name) {
        for (const char *p = name; *p; p++)
            seed = seed * 31 + (uint64_t)*p;
    }

    /* Encode */
    KVBlock *blocks = (KVBlock *)calloc(n_blocks, sizeof(KVBlock));
    for (size_t b = 0; b < n_blocks; b++) {
        size_t off = b * BLOCK_DIM;
        size_t dim = (off + BLOCK_DIM <= total) ? BLOCK_DIM : (total - off);
        encode_kv_block(data + off, dim, &blocks[b],
                        seed ^ (uint64_t)b, lattice_scale);
    }

    /* Collect residuals */
    size_t total_pairs = n_blocks * E8_PAIRS;
    float *all_res = (float *)calloc(total_pairs * TILE_DIM, sizeof(float));
    for (size_t b = 0; b < n_blocks; b++) {
        size_t off = b * BLOCK_DIM;
        size_t dim = (off + BLOCK_DIM <= total) ? BLOCK_DIM : (total - off);
        float buf[BLOCK_DIM];
        memset(buf, 0, sizeof(buf));
        memcpy(buf, data + off, dim * sizeof(float));
        random_signs(buf, BLOCK_DIM, seed ^ (uint64_t)b);
        fwht(buf, BLOCK_DIM);
        float rms = blocks[b].scale;
        for (int i = 0; i < BLOCK_DIM; i++) buf[i] /= rms;
        float warped[BLOCK_DIM];
        for (int i = 0; i < BLOCK_DIM; i++) warped[i] = mu_warp(buf[i], MU_BETA);
        float wnorm = blocks[b].warp_norm;
        for (int p = 0; p < E8_PAIRS; p++) {
            size_t ridx = (b * E8_PAIRS + (size_t)p) * TILE_DIM;
            for (int d = 0; d < TILE_DIM; d++) {
                float scaled = warped[p * TILE_DIM + d] / wnorm * lattice_scale;
                all_res[ridx + d] = scaled - blocks[b].e8_points[p * TILE_DIM + d];
            }
        }
    }

    /* Learn + assign tiles */
    int ek = RVQ_TILES;
    if (total_pairs < (size_t)ek * 4) ek = (int)(total_pairs / 4);
    if (ek < 16) ek = 16;
    if (ek > RVQ_TILES) ek = RVQ_TILES;
    res.effective_k = ek;

    float *tiles = (float *)calloc((size_t)ek * TILE_DIM, sizeof(float));
    learn_tiles(all_res, total_pairs, tiles, ek);

    int prev_seeds[E8_PAIRS];
    for (int p = 0; p < E8_PAIRS; p++) prev_seeds[p] = -1;
    for (size_t b = 0; b < n_blocks; b++) {
        for (int p = 0; p < E8_PAIRS; p++) {
            size_t ridx = (b * E8_PAIRS + (size_t)p) * TILE_DIM;
            int ti = find_nearest_tile(all_res + ridx, tiles, ek, prev_seeds[p]);
            blocks[b].tile_idx[p] = (uint8_t)ti;
            prev_seeds[p] = ti;
        }
    }

    /* Decode + measure */
    double dot = 0, na2 = 0, nb2 = 0, mse = 0;
    for (size_t b = 0; b < n_blocks; b++) {
        size_t off = b * BLOCK_DIM;
        size_t dim = (off + BLOCK_DIM <= total) ? BLOCK_DIM : (total - off);
        float decoded[BLOCK_DIM];
        decode_kv_block(&blocks[b], tiles, ek, decoded, dim,
                        seed ^ (uint64_t)b, lattice_scale);
        for (size_t i = 0; i < dim; i++) {
            float a = data[off + i], d = decoded[i];
            dot += (double)a * (double)d;
            na2 += (double)a * (double)a;
            nb2 += (double)d * (double)d;
            float e = a - d; mse += (double)(e * e);
        }
    }
    res.cosine = dot / (sqrt(na2) * sqrt(nb2) + 1e-20);
    res.rmse = sqrt(mse / (double)total);
    double data_bits = (double)total * (double)bits +
                       (double)n_blocks * E8_PAIRS * 8.0 +
                       (double)n_blocks * 64.0 +
                       (double)ek * TILE_DIM * 32.0;
    res.bpw = (float)(data_bits / (double)total);

    free(blocks); free(all_res); free(tiles);
    return res;
}

/* ═══════════════════════════════════════════════════════════════════
 * CLI
 * ═══════════════════════════════════════════════════════════════════ */

static void usage(void) {
    fprintf(stderr,
        "bonfyre-kvcache — v8 RLF KV cache compression\n"
        "\n"
        "Usage:\n"
        "  bonfyre-kvcache store      <model_hash_hex> <ctx_hash_hex> <input.bfkv>\n"
        "  bonfyre-kvcache fetch      <model_hash_hex> <ctx_hash_hex> [--out file]\n"
        "  bonfyre-kvcache roundtrip  <input.bin> [--bits N]\n"
        "  bonfyre-kvcache benchmark  <input.bin> [--bits N]\n"
        "  bonfyre-kvcache context-compile --bodies DIR --token-budget N --tokenizer TOKENIZER.json\n"
        "                             --gigatoken-bin PATH --out FILE [--witnesses FILE] [--task REF] [--revision REV]\n"
        "  bonfyre-kvcache context-verify --receipt COMPILE.json --bodies DIR --context FILE\n"
        "                             [--task REF] [--revision REV]\n"
        "  bonfyre-kvcache --help\n"
        "\n"
        "Commit chain (Merkle DAG) subcommands:\n"
        "  bonfyre-kvcache chain     <model_hex> <parent_hex|0> <data_file>\n"
        "  bonfyre-kvcache ancestry  <model_hex> <ctx_hex> [--max N]\n"
        "\n"
        "Pack / store subcommands:\n"
        "  bonfyre-kvcache kvpack    [--out <pack.bfkvpack>]\n"
        "  bonfyre-kvcache kvgc      [--pack <pack.bfkvpack>]\n"
        "  bonfyre-kvcache kvlog     <model_hex>\n"
        "\n"
        "KV cache compression using E8 lattice + μ-law + 16D RVQ.\n"
        "Error compounds across layers — use 4+ bits for production.\n"
        "\n"
        "Results (Qwen-0.5B, WikiText-2):\n"
        "  3-bit: PPL 17.89 (+49.7%%), avg cos 0.9997\n"
        "  4-bit: PPL 14.77 (+23.6%%), avg cos 0.9999\n"
        "\n"
        "Options:\n"
        "  --bits N     Quantization bits (3, 4, 5; default: 4)\n"
    );
}

/* ═══════════════════════════════════════════════════════════════════
 * Context compile
 *
 * Given bodies of text, sentences that must survive, and a token budget, choose
 * which bodies go into the context and write it. Tokens are counted by the
 * tokenizer the model uses; a required sentence is checked by reading the
 * written context back. Nothing here is estimated.
 *
 * What stood here before took counts and multiplied them by constants: so many
 * milliseconds per block, so much residual per missing witness. It reported a
 * continuity verdict for text it had never been shown.
 * ═══════════════════════════════════════════════════════════════════ */

#define CTX_MAX_UNITS 4096
#define CTX_MAX_WITNESSES 1024

typedef struct {
    char path[PATH_MAX];
    char *text;
    size_t len;
    uint32_t tokens;
    int required;
    int selected;
} CtxUnit;

typedef struct {
    char *text;
    int in_source;      /* index of the newest body carrying it, or -1 */
    int in_context;     /* found when the written context was read back */
} CtxWitness;

static char *ctx_read_file_(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (size < 0) { fclose(f); return NULL; }
    char *buf = (char *)malloc((size_t)size + 1);
    if (!buf) { fclose(f); return NULL; }
    if (fread(buf, 1, (size_t)size, f) != (size_t)size) { fclose(f); free(buf); return NULL; }
    fclose(f);
    buf[size] = '\0';
    *len = (size_t)size;
    return buf;
}

static int ctx_cmp_unit_(const void *a, const void *b) {
    return strcmp(((const CtxUnit *)a)->path, ((const CtxUnit *)b)->path);
}

/* Token counts for a list of files, from the tokenizer bridge: a count of
 * documents, then for each a count of tokens followed by that many ids. */
static int ctx_count_tokens_(const char *bridge, const char *tokenizer,
                             char **paths, size_t n, uint32_t *counts) {
    int fds[2];
    if (pipe(fds) != 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(fds[0]); close(fds[1]); return -1; }
    if (pid == 0) {
        char **av = (char **)calloc(n + 3, sizeof(char *));
        if (!av) _exit(127);
        av[0] = (char *)bridge;
        av[1] = (char *)tokenizer;
        for (size_t i = 0; i < n; i++) av[i + 2] = paths[i];
        dup2(fds[1], STDOUT_FILENO);
        close(fds[0]);
        close(fds[1]);
        execv(bridge, av);
        _exit(127);
    }
    close(fds[1]);
    FILE *in = fdopen(fds[0], "rb");
    int ok = 0;
    uint32_t docs = 0;
    if (in && fread(&docs, 4, 1, in) == 1 && docs == (uint32_t)n) {
        ok = 1;
        for (size_t i = 0; i < n && ok; i++) {
            uint32_t count = 0;
            if (fread(&count, 4, 1, in) != 1) { ok = 0; break; }
            counts[i] = count;
            for (uint32_t skipped = 0; skipped < count; ) {
                uint32_t scratch[4096];
                uint32_t want = count - skipped > 4096 ? 4096 : count - skipped;
                if (fread(scratch, 4, want, in) != want) { ok = 0; break; }
                skipped += want;
            }
        }
    }
    if (in) fclose(in); else close(fds[0]);
    int status = 0;
    waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) ok = 0;
    return ok ? 0 : -1;
}

static void ctx_json_string_(const char *s) {
    putchar('"');
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c == '\n') fputs("\\n", stdout);
        else if (c == '\t') fputs("\\t", stdout);
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

static int ctx_write_selection_(const char *out_path, const CtxUnit *units, size_t n) {
    FILE *out = fopen(out_path, "wb");
    if (!out) return -1;
    int failed = fchmod(fileno(out), 0600) != 0;
    for (size_t i = 0; i < n && !failed; i++) {
        if (!units[i].selected) continue;
        if (fwrite(units[i].text, 1, units[i].len, out) != units[i].len) failed = 1;
        if (!failed && (units[i].len == 0 || units[i].text[units[i].len - 1] != '\n'))
            failed = fputc('\n', out) == EOF;
    }
    if (fflush(out) != 0 || ferror(out)) failed = 1;
    if (!failed && fsync(fileno(out)) != 0) failed = 1;
    if (fclose(out) != 0) failed = 1;
    return failed ? -1 : 0;
}

/* The tokenizer reads a private candidate, never the currently published file.
 * It may count that candidate but must not change the selected source bytes. */
static int ctx_selection_matches_(const char *text, size_t len, const CtxUnit *units, size_t n) {
    size_t at = 0;
    for (size_t i = 0; i < n; i++) {
        if (!units[i].selected) continue;
        if (units[i].len > len - at || memcmp(text + at, units[i].text, units[i].len)) return 0;
        at += units[i].len;
        if (units[i].len == 0 || units[i].text[units[i].len - 1] != '\n') {
            if (at == len || text[at++] != '\n') return 0;
        }
    }
    return at == len;
}

static int ctx_same_file_(const struct stat *a, const struct stat *b) {
    return a->st_dev == b->st_dev && a->st_ino == b->st_ino;
}

static int ctx_same_version_(const struct stat *a, const struct stat *b) {
    if (!ctx_same_file_(a, b) || a->st_size != b->st_size || a->st_mode != b->st_mode ||
        a->st_uid != b->st_uid || a->st_gid != b->st_gid || a->st_nlink != b->st_nlink) return 0;
#if defined(__APPLE__)
    return a->st_mtimespec.tv_sec == b->st_mtimespec.tv_sec && a->st_mtimespec.tv_nsec == b->st_mtimespec.tv_nsec &&
           a->st_ctimespec.tv_sec == b->st_ctimespec.tv_sec && a->st_ctimespec.tv_nsec == b->st_ctimespec.tv_nsec;
#else
    return a->st_mtim.tv_sec == b->st_mtim.tv_sec && a->st_mtim.tv_nsec == b->st_mtim.tv_nsec &&
           a->st_ctim.tv_sec == b->st_ctim.tv_sec && a->st_ctim.tv_nsec == b->st_ctim.tv_nsec;
#endif
}

/* A task or revision reference: short, printable, no separators that could
 * smuggle a path or a second field. */
static int ctx_ref_ok_(const char *s) {
    size_t n = strlen(s);
    if (n == 0 || n > 128) return 0;
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '.' || c == '_' || c == ':' || c == '-')) return 0;
    }
    return 1;
}

static int kvcache_cmd_context_compile_(int argc, char **argv) {
    const char *bodies_dir = bf_arg_value(argc, argv, "--bodies");
    const char *witness_path = bf_arg_value(argc, argv, "--witnesses");
    const char *budget_arg = bf_arg_value(argc, argv, "--token-budget");
    const char *tokenizer = bf_arg_value(argc, argv, "--tokenizer");
    const char *bridge = bf_arg_value(argc, argv, "--gigatoken-bin");
    const char *out_path = bf_arg_value(argc, argv, "--out");
    const char *task = bf_arg_value(argc, argv, "--task");
    const char *revision = bf_arg_value(argc, argv, "--revision");
    if (!bridge) bridge = getenv("BONFYRE_GIGATOKEN_BIN");
    if ((task && !ctx_ref_ok_(task)) || (revision && !ctx_ref_ok_(revision))) {
        fprintf(stderr, "context-compile: bad --task or --revision\n"); return 1;
    }
    if (!bodies_dir || !budget_arg || !tokenizer || !bridge || !out_path) {
        fprintf(stderr,
            "usage: bonfyre-kvcache context-compile --bodies DIR --token-budget N --tokenizer TOKENIZER.json\n"
            "         --gigatoken-bin PATH --out FILE [--witnesses FILE] [--task REF] [--revision REV]\n"
            "  bodies     one file per body; they are ordered by name, oldest first\n"
            "  witnesses  one required sentence per line; each must appear in the written context\n"
            "  BONFYRE_GIGATOKEN_BIN may name the tokenizer bridge instead of --gigatoken-bin\n");
        return 1;
    }
    errno = 0;
    char *budget_end = NULL;
    unsigned long long parsed_budget = strtoull(budget_arg, &budget_end, 10);
    if (!*budget_arg || strspn(budget_arg, "0123456789") != strlen(budget_arg) || errno ||
        !budget_end || *budget_end || parsed_budget == 0 || parsed_budget > UINT32_MAX) {
        fprintf(stderr, "context-compile: --token-budget must be an integer from 1 to 4294967295\n"); return 1;
    }
    long budget = (long)parsed_budget;
    if (access(bridge, X_OK) != 0) { fprintf(stderr, "context-compile: no tokenizer bridge at %s\n", bridge); return 1; }
    if (access(tokenizer, R_OK) != 0) { fprintf(stderr, "context-compile: no tokenizer at %s\n", tokenizer); return 1; }

    CtxUnit *units = (CtxUnit *)calloc(CTX_MAX_UNITS, sizeof(CtxUnit));
    CtxWitness *witnesses = (CtxWitness *)calloc(CTX_MAX_WITNESSES, sizeof(CtxWitness));
    if (!units || !witnesses) { free(units); free(witnesses); return 1; }
    size_t n = 0, nw = 0;
    int rc = 1;
    char *witness_text = NULL, *context = NULL;
    char **paths = NULL;
    uint32_t *counts = NULL;
    char stage_dir[PATH_MAX] = {0}, staged_path[PATH_MAX] = {0};
    struct stat output_before;
    int output_exists = lstat(out_path, &output_before) == 0;
    if ((!output_exists && errno != ENOENT) || (output_exists && !S_ISREG(output_before.st_mode))) {
        fprintf(stderr, "context-compile: output must be a regular file or a new path\n"); goto done;
    }
    const char *protected_paths[3] = {tokenizer, bridge, witness_path};
    if (output_exists) for (size_t i = 0; i < 3; i++) {
        struct stat st;
        if (protected_paths[i] && stat(protected_paths[i], &st) == 0 && ctx_same_file_(&st, &output_before)) {
            fprintf(stderr, "context-compile: output aliases a compiler input\n"); goto done;
        }
    }

    DIR *dir = opendir(bodies_dir);
    if (!dir) { perror(bodies_dir); goto done; }
    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        if (n == CTX_MAX_UNITS) { fprintf(stderr, "context-compile: more than %d bodies\n", CTX_MAX_UNITS); closedir(dir); goto done; }
        struct stat st;
        int length = snprintf(units[n].path, sizeof(units[n].path), "%s/%s", bodies_dir, ent->d_name);
        if (length < 0 || (size_t)length >= sizeof(units[n].path)) {
            fprintf(stderr, "context-compile: body path is too long\n"); closedir(dir); goto done;
        }
        if (stat(units[n].path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        if (output_exists && ctx_same_file_(&st, &output_before)) {
            fprintf(stderr, "context-compile: output aliases a source body\n"); closedir(dir); goto done;
        }
        n++;
    }
    closedir(dir);
    if (n == 0) { fprintf(stderr, "context-compile: no bodies in %s\n", bodies_dir); goto done; }
    qsort(units, n, sizeof(CtxUnit), ctx_cmp_unit_);

    for (size_t i = 0; i < n; i++) {
        units[i].text = ctx_read_file_(units[i].path, &units[i].len);
        if (!units[i].text) { fprintf(stderr, "context-compile: cannot read %s\n", units[i].path); goto done; }
        if (units[i].len == 0) { fprintf(stderr, "context-compile: %s is empty\n", units[i].path); goto done; }
        if (strlen(units[i].text) != units[i].len) { fprintf(stderr, "context-compile: %s is not text\n", units[i].path); goto done; }
    }

    paths = (char **)calloc(n + 1, sizeof(char *));
    counts = (uint32_t *)calloc(n + 1, sizeof(uint32_t));
    if (!paths || !counts) goto done;
    for (size_t i = 0; i < n; i++) paths[i] = units[i].path;
    if (ctx_count_tokens_(bridge, tokenizer, paths, n, counts) != 0) {
        fprintf(stderr, "context-compile: the tokenizer did not count these bodies; nothing was written\n");
        goto done;
    }
    uint64_t source_tokens = 0;
    for (size_t i = 0; i < n; i++) { units[i].tokens = counts[i]; source_tokens += counts[i]; }

    if (witness_path) {
        size_t wlen = 0;
        witness_text = ctx_read_file_(witness_path, &wlen);
        if (!witness_text) { fprintf(stderr, "context-compile: cannot read %s\n", witness_path); goto done; }
        if (strlen(witness_text) != wlen) { fprintf(stderr, "context-compile: witnesses are not text\n"); goto done; }
        for (char *line = witness_text; line && *line; ) {
            char *end = strchr(line, '\n');
            if (end) *end = '\0';
            size_t len = strlen(line);
            while (len > 0 && (line[len - 1] == '\r' || line[len - 1] == ' ' || line[len - 1] == '\t')) line[--len] = '\0';
            while (*line == ' ' || *line == '\t') line++;
            if (*line) {
                if (nw == CTX_MAX_WITNESSES) { fprintf(stderr, "context-compile: more than %d witnesses\n", CTX_MAX_WITNESSES); goto done; }
                witnesses[nw].text = line;
                witnesses[nw].in_source = -1;
                nw++;
            }
            line = end ? end + 1 : NULL;
        }
    }

    /* A required sentence pins the newest body that carries it. */
    size_t absent = 0;
    uint64_t required_tokens = 0;
    for (size_t w = 0; w < nw; w++) {
        for (size_t i = n; i-- > 0; ) {
            if (strstr(units[i].text, witnesses[w].text)) { witnesses[w].in_source = (int)i; break; }
        }
        if (witnesses[w].in_source < 0) { absent++; continue; }
        CtxUnit *u = &units[witnesses[w].in_source];
        if (!u->required) { u->required = 1; u->selected = 1; required_tokens += u->tokens; }
    }

    const char *verdict = "PASS", *reason = "";
    uint32_t written_tokens = 0;
    size_t selected = 0, preserved = 0;
    if (absent > 0) {
        verdict = "FAIL"; reason = "a required sentence appears in no body";
    } else if (required_tokens > (uint64_t)budget) {
        verdict = "FAIL"; reason = "the bodies carrying required sentences exceed the token budget";
    } else {
        /* Fill what is left with the newest bodies that fit. */
        uint64_t used = required_tokens;
        for (size_t i = n; i-- > 0; ) {
            if (units[i].selected) continue;
            if (used + units[i].tokens <= (uint64_t)budget) { units[i].selected = 1; used += units[i].tokens; }
        }
        /* The count that matters is the written context's, which joining
         * bodies can move by a token or two. Shed the oldest optional body
         * until the tokenizer agrees it fits. */
        const char *slash = strrchr(out_path, '/');
        size_t parent_length = slash ? (size_t)(slash - out_path + 1) : 0;
        if (parent_length + sizeof(".bonfyre-context.XXXXXX") >= sizeof(stage_dir)) {
            fprintf(stderr, "context-compile: output path is too long\n"); goto done;
        }
        if (parent_length) memcpy(stage_dir, out_path, parent_length);
        strcpy(stage_dir + parent_length, ".bonfyre-context.XXXXXX");
        if (!mkdtemp(stage_dir)) { stage_dir[0] = '\0'; perror("context-compile: staging"); goto done; }
        int stage_length = snprintf(staged_path, sizeof(staged_path), "%s/context", stage_dir);
        if (stage_length < 0 || (size_t)stage_length >= sizeof(staged_path)) goto done;
        for (;;) {
            if (ctx_write_selection_(staged_path, units, n) != 0) { fprintf(stderr, "context-compile: cannot write candidate for %s\n", out_path); goto done; }
            char *one[1] = { staged_path };
            uint32_t got = 0;
            if (ctx_count_tokens_(bridge, tokenizer, one, 1, &got) != 0) {
                fprintf(stderr, "context-compile: the tokenizer did not count the candidate context\n");
                goto done;
            }
            written_tokens = got;
            if ((long)got <= budget) break;
            size_t drop = n;
            for (size_t i = 0; i < n; i++) if (units[i].selected && !units[i].required) { drop = i; break; }
            if (drop == n) { verdict = "FAIL"; reason = "the required bodies, once joined, exceed the token budget"; break; }
            units[drop].selected = 0;
        }
    }

    if (!strcmp(verdict, "PASS")) {
        size_t clen = 0;
        context = ctx_read_file_(staged_path, &clen);
        if (!context || !ctx_selection_matches_(context, clen, units, n)) {
            fprintf(stderr, "context-compile: candidate differs from the selected source bytes\n"); goto done;
        }
        for (size_t w = 0; w < nw; w++) {
            witnesses[w].in_context = strstr(context, witnesses[w].text) != NULL;
            if (witnesses[w].in_context) preserved++;
        }
        if (preserved != nw) { verdict = "FAIL"; reason = "a required sentence is missing from the written context"; }
    }
    if (!strcmp(verdict, "PASS")) {
        struct stat current;
        int exists_now = lstat(out_path, &current) == 0;
        if ((!exists_now && errno != ENOENT) || exists_now != output_exists ||
            (exists_now && (!S_ISREG(current.st_mode) || !ctx_same_version_(&current, &output_before)))) {
            fprintf(stderr, "context-compile: output changed during compilation\n"); goto done;
        }
        int identical = 0;
        if (exists_now && current.st_size >= 0 && (uint64_t)current.st_size == strlen(context)) {
            size_t previous_len = 0;
            char *previous = ctx_read_file_(out_path, &previous_len);
            if (!previous) { fprintf(stderr, "context-compile: cannot compare existing output\n"); goto done; }
            identical = previous_len == strlen(context) && !memcmp(previous, context, previous_len);
            free(previous);
            if (lstat(out_path, &current) != 0 || !ctx_same_version_(&current, &output_before)) {
                fprintf(stderr, "context-compile: output changed during comparison\n"); goto done;
            }
        }
        if (!identical) {
            if (output_exists) {
                if (rename(staged_path, out_path) != 0) { perror("context-compile: publish"); goto done; }
                staged_path[0] = '\0';
            } else {
                /* An absent destination is published without overwriting a late creator. */
                if (link(staged_path, out_path) != 0) { perror("context-compile: publish"); goto done; }
            }
        }
    }
    for (size_t i = 0; i < n; i++) selected += (size_t)units[i].selected;

    printf("{\n  \"schema_version\": \"bonfyre.context.compile.v2\",\n  \"tokenizer\": ");
    ctx_json_string_(tokenizer);
    printf(",\n  \"task\": ");
    if (task) ctx_json_string_(task); else fputs("null", stdout);
    printf(",\n  \"revision\": ");
    if (revision) ctx_json_string_(revision); else fputs("null", stdout);
    printf(",\n  \"token_budget\": %ld,\n", budget);
    printf("  \"source\": {\"bodies\": %zu, \"tokens\": %llu},\n", n, (unsigned long long)source_tokens);
    if (!strcmp(verdict, "PASS")) {
        printf("  \"context\": {\"path\": ");
        ctx_json_string_(out_path);
        char context_sha[65];
        bf_sha256_hex((const uint8_t *)context, strlen(context), context_sha);
        printf(", \"sha256\": \"%s\", \"bodies\": %zu, \"tokens\": %u},\n", context_sha, selected, written_tokens);
    } else {
        printf("  \"context\": null,\n");
    }
    printf("  \"bodies\": [\n");
    for (size_t i = 0; i < n; i++) {
        const char *base = strrchr(units[i].path, '/');
        printf("    {\"body\": ");
        ctx_json_string_(base ? base + 1 : units[i].path);
        char body_sha[65];
        bf_sha256_hex((const uint8_t *)units[i].text, units[i].len, body_sha);
        printf(", \"sha256\": \"%s\"", body_sha);
        printf(", \"tokens\": %u, \"required\": %s, \"in_context\": %s}%s\n", units[i].tokens,
               units[i].required ? "true" : "false",
               (units[i].selected && !strcmp(verdict, "PASS")) ? "true" : "false", i + 1 < n ? "," : "");
    }
    printf("  ],\n  \"witnesses\": [\n");
    for (size_t w = 0; w < nw; w++) {
        printf("    {\"sentence\": ");
        ctx_json_string_(witnesses[w].text);
        printf(", \"in_source\": %s, \"in_context\": %s}%s\n", witnesses[w].in_source >= 0 ? "true" : "false",
               witnesses[w].in_context ? "true" : "false", w + 1 < nw ? "," : "");
    }
    printf("  ],\n  \"witnesses_required\": %zu,\n  \"witnesses_preserved\": %zu,\n", nw, preserved);
    printf("  \"verdict\": \"%s\"", verdict);
    if (reason[0]) { printf(",\n  \"reason\": "); ctx_json_string_(reason); }
    printf("\n}\n");
    rc = strcmp(verdict, "PASS") == 0 ? 0 : 2;

done:
    if (staged_path[0]) unlink(staged_path);
    if (stage_dir[0]) rmdir(stage_dir);
    for (size_t i = 0; i < n; i++) free(units[i].text);
    free(units);
    free(witnesses);
    free(witness_text);
    free(context);
    free(paths);
    free(counts);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 * Context verify
 *
 * A compiled context is a handoff only while it still is what was compiled:
 * every body it names hashes as it did, no body has appeared or vanished, the
 * written context hashes as it did and still carries every required sentence,
 * and it was bound to the task and revision the caller is about to hand it to.
 * Anything else is a stale context and is refused rather than substituted.
 * The receipt this prints is what Meter's context-compare accepts.
 * ═══════════════════════════════════════════════════════════════════ */

static const bf_json_node_t *ctx_get_(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k, bf_json_type_t t) {
    const bf_json_node_t *n = o ? bf_json_obj_get(d, o, k) : NULL;
    return n && n->type == t ? n : NULL;
}

static int ctx_str_is_(const bf_json_node_t *n, const char *want) {
    int len = 0;
    const char *s = n ? bf_json_get_str(n, &len) : NULL;
    return s && want && (size_t)len == strlen(want) && !memcmp(s, want, (size_t)len);
}

static int kvcache_cmd_context_verify_(int argc, char **argv) {
    const char *receipt_path = bf_arg_value(argc, argv, "--receipt");
    const char *bodies_dir = bf_arg_value(argc, argv, "--bodies");
    const char *context_path = bf_arg_value(argc, argv, "--context");
    const char *task = bf_arg_value(argc, argv, "--task");
    const char *revision = bf_arg_value(argc, argv, "--revision");
    if (!receipt_path || !bodies_dir || !context_path) {
        fprintf(stderr, "usage: see bonfyre-kvcache --help\n");
        return 1;
    }
    if ((task && !ctx_ref_ok_(task)) || (revision && !ctx_ref_ok_(revision))) {
        fprintf(stderr, "context-verify: bad --task or --revision\n"); return 1;
    }
    size_t rlen = 0, clen = 0;
    char *receipt = ctx_read_file_(receipt_path, &rlen);
    char *context = ctx_read_file_(context_path, &clen);
    bf_json_doc_t *doc = receipt ? bf_json_parse(receipt, rlen, NULL, 0) : NULL;
    const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
    const char *reason = NULL;
    char receipt_sha[65] = {0}, context_sha[65] = {0};
    size_t listed = 0, on_disk = 0, in_context = 0, nw = 0, kept = 0;
    int64_t tokens = 0, budget = 0, source_tokens = 0;
    const bf_json_node_t *ctx = NULL;
    if (receipt) bf_sha256_hex((const uint8_t *)receipt, rlen, receipt_sha);
    if (context) bf_sha256_hex((const uint8_t *)context, clen, context_sha);

    if (!root || root->type != BF_JSON_OBJECT) reason = "not a compile receipt";
    else if (!ctx_str_is_(ctx_get_(doc, root, "schema_version", BF_JSON_STRING), "bonfyre.context.compile.v2"))
        reason = "not a compile receipt";
    else if (!ctx_str_is_(ctx_get_(doc, root, "verdict", BF_JSON_STRING), "PASS")) reason = "compile did not pass";
    else if (!(ctx = ctx_get_(doc, root, "context", BF_JSON_OBJECT))) reason = "no written context";
    else if (!context) reason = "context unreadable";
    else if (!ctx_get_(doc, ctx, "sha256", BF_JSON_STRING)) reason = "receipt has no digests; recompile";
    else if (!ctx_str_is_(ctx_get_(doc, ctx, "sha256", BF_JSON_STRING), context_sha)) reason = "context file changed";
    else if (strlen(context) != clen) reason = "context is not text";
    if (!reason && task) {
        const bf_json_node_t *t = ctx_get_(doc, root, "task", BF_JSON_STRING);
        if (!t) reason = "not bound to a task";
        else if (!ctx_str_is_(t, task)) reason = "compiled for another task";
    }
    if (!reason && revision) {
        const bf_json_node_t *r = ctx_get_(doc, root, "revision", BF_JSON_STRING);
        if (!r) reason = "not bound to a revision";
        else if (!ctx_str_is_(r, revision)) reason = "compiled from another source revision";
    }
    const bf_json_node_t *bodies = reason ? NULL : ctx_get_(doc, root, "bodies", BF_JSON_ARRAY);
    if (!reason && !bodies) reason = "no bodies listed";
    for (const bf_json_node_t *b = bodies ? bf_json_child_first(doc, bodies) : NULL; b && !reason; b = bf_json_child_next(doc, b)) {
        char name[NAME_MAX + 1], path[PATH_MAX], sha[65];
        const bf_json_node_t *nm = ctx_get_(doc, b, "body", BF_JSON_STRING), *hs = ctx_get_(doc, b, "sha256", BF_JSON_STRING);
        if (!nm || !hs || nm->val.str.len <= 0 || (size_t)nm->val.str.len >= sizeof(name)) { reason = "body without name or digest"; break; }
        memcpy(name, nm->val.str.ptr, (size_t)nm->val.str.len); name[nm->val.str.len] = 0;
        if (name[0] == '.' || strchr(name, '/') || strchr(name, '\\')) { reason = "body name is not a file name"; break; }
        int pl = snprintf(path, sizeof(path), "%s/%s", bodies_dir, name);
        struct stat st;
        if (pl < 0 || (size_t)pl >= sizeof(path) || lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) { reason = "a body is gone"; break; }
        if (bf_sha256_file(path, sha) != 0 || !ctx_str_is_(hs, sha)) { reason = "a body changed"; break; }
        listed++;
        const bf_json_node_t *ic = ctx_get_(doc, b, "in_context", BF_JSON_BOOL);
        if (ic && bf_json_get_bool(ic)) in_context++;
    }
    if (!reason) {
        DIR *dir = opendir(bodies_dir);
        struct dirent *ent;
        if (!dir) reason = "bodies unreadable";
        while (dir && (ent = readdir(dir)) != NULL) {
            char path[PATH_MAX]; struct stat st;
            if (ent->d_name[0] == '.') continue;
            int pl = snprintf(path, sizeof(path), "%s/%s", bodies_dir, ent->d_name);
            if (pl > 0 && (size_t)pl < sizeof(path) && stat(path, &st) == 0 && S_ISREG(st.st_mode)) on_disk++;
        }
        if (dir) closedir(dir);
        if (!reason && on_disk != listed) reason = "a body appeared";
    }
    /* A handoff carries the decisions it must not lose; without one there is
     * nothing to check it against, so it is not a verified handoff. */
    const bf_json_node_t *ws = reason ? NULL : ctx_get_(doc, root, "witnesses", BF_JSON_ARRAY);
    for (const bf_json_node_t *w = ws ? bf_json_child_first(doc, ws) : NULL; w && !reason; w = bf_json_child_next(doc, w)) {
        const bf_json_node_t *t = ctx_get_(doc, w, "sentence", BF_JSON_STRING);
        size_t ulen = 0;
        char *sentence = t ? bf_json_unescape(t->val.str.ptr, t->val.str.len, &ulen) : NULL;
        nw++;
        if (sentence && ulen && strlen(sentence) == ulen && strstr(context, sentence)) kept++;
        free(sentence);
    }
    if (!reason && nw == 0) reason = "needs at least one witness";
    else if (!reason && kept != nw) reason = "a witness is missing from the context";
    if (!reason) {
        const bf_json_node_t *n;
        if ((n = ctx_get_(doc, ctx, "tokens", BF_JSON_INT))) tokens = bf_json_get_int(n);
        if ((n = ctx_get_(doc, root, "token_budget", BF_JSON_INT))) budget = bf_json_get_int(n);
        const bf_json_node_t *src = ctx_get_(doc, root, "source", BF_JSON_OBJECT);
        if ((n = ctx_get_(doc, src, "tokens", BF_JSON_INT))) source_tokens = bf_json_get_int(n);
        if (tokens <= 0 || budget <= 0 || tokens > budget || source_tokens < tokens) reason = "inconsistent token counts";
    }

    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema_version"); bf_jsonw_str(&w, "bonfyre.context.handoff.v1");
    bf_jsonw_key(&w, "verdict"); bf_jsonw_str(&w, reason ? "REFUSED" : "PASS");
    if (reason) { bf_jsonw_key(&w, "reason"); bf_jsonw_str(&w, reason); }
    bf_jsonw_key(&w, "receipt_sha256"); if (receipt) bf_jsonw_str(&w, receipt_sha); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "context_sha256"); if (context) bf_jsonw_str(&w, context_sha); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "task"); if (task) bf_jsonw_str(&w, task); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "revision"); if (revision) bf_jsonw_str(&w, revision); else bf_jsonw_null(&w);
    if (!reason) {
        bf_jsonw_key(&w, "bodies_checked"); bf_jsonw_int(&w, (int64_t)listed);
        bf_jsonw_key(&w, "bodies_in_context"); bf_jsonw_int(&w, (int64_t)in_context);
        bf_jsonw_key(&w, "witnesses_preserved"); bf_jsonw_int(&w, (int64_t)kept);
        bf_jsonw_key(&w, "source_tokens"); bf_jsonw_int(&w, source_tokens);
        bf_jsonw_key(&w, "context_tokens"); bf_jsonw_int(&w, tokens);
        bf_jsonw_key(&w, "token_budget"); bf_jsonw_int(&w, budget);
        bf_jsonw_key(&w, "token_basis"); bf_jsonw_str(&w, "tokenizer_count_not_provider_usage");
    }
    bf_jsonw_obj_end(&w);
    int rc = w.oom ? 1 : reason ? 2 : 0;
    if (!w.oom) { fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); }
    bf_jsonw_free(&w);
    if (doc) bf_json_free(doc);
    free(receipt); free(context);
    return rc;
}

static void hex_kv_(const uint8_t h[32], char out[65]) {
    static const char hc[] = "0123456789abcdef";
    for (int i=0;i<32;i++){out[i*2]=hc[h[i]>>4];out[i*2+1]=hc[h[i]&0xf];}
    out[64]='\0';
}

/* Forward declaration needed by helpers below */
static int parse_hex32(const char *hex, uint8_t out[32]);

/* ── chain / ancestry / kvlog / kvpack static helpers ────────── */

static int kvcache_cmd_chain_(int argc, char **argv) {
    /* chain <model_hex> <parent_hex|0> <data_file> */
    if (argc < 4) {
        fprintf(stderr,
            "Usage: bonfyre-kvcache chain <model_hex> <parent_hex|0> <data_file>\n"
            "  parent_hex = 0 (or 64 zeros) for root of new sequence\n"); return 1;
    }
    uint8_t model_hash[32], parent_hash[32];
    if (parse_hex32(argv[1], model_hash) != 0) {
        /* try all-zeros for convenience */
        fprintf(stderr, "chain: model_hex must be 64 hex chars\n"); return 1;
    }
    const char *phex = argv[2];
    if (strcmp(phex, "0") == 0 || strlen(phex) < 64) {
        memset(parent_hash, 0, 32);
    } else {
        if (parse_hex32(phex, parent_hash) != 0) {
            fprintf(stderr, "chain: parent_hex must be 64 hex chars or '0'\n"); return 1;
        }
    }
    size_t len = 0;
    void *data = (void *)bf_read_file(argv[3], &len);
    if (!data) { perror(argv[3]); return 1; }

    uint8_t new_ctx[32];
    int rc = bf_kvcache_chain(model_hash, parent_hash, data, len, new_ctx);
    free(data);
    if (rc != 0) { fprintf(stderr, "chain: write failed\n"); return 1; }

    char hex[65]; hex_kv_(new_ctx, hex);
    printf("chain: new_ctx_hash=%s\n", hex);
    printf("  model=%.16s...\n  parent=%.16s%s\n",
           argv[1], argv[2], strlen(argv[2]) >= 64 ? "..." : "");
    return 0;
}

static int kvcache_cmd_ancestry_(int argc, char **argv) {
    /* ancestry <model_hex> <ctx_hex> [--max N] */
    if (argc < 3) {
        fprintf(stderr, "Usage: bonfyre-kvcache ancestry <model_hex> <ctx_hex> [--max N]\n");
        return 1;
    }
    uint8_t model_hash[32], ctx_hash[32];
    if (parse_hex32(argv[1], model_hash) != 0 ||
        parse_hex32(argv[2], ctx_hash)   != 0) {
        fprintf(stderr, "ancestry: hashes must be 64 hex chars\n"); return 1;
    }
    int max_depth = 64;
    for (int i = 3; i < argc; i++)
        if (strcmp(argv[i], "--max") == 0 && i+1 < argc) max_depth = atoi(argv[++i]);

    uint8_t (*hashes)[32] = malloc((size_t)max_depth * 32);
    if (!hashes) return 1;
    int depth = bf_kvcache_ancestry(model_hash, ctx_hash, hashes, max_depth);
    printf("ancestry: %d ancestors\n", depth);
    for (int i = 0; i < depth; i++) {
        char hex[65]; hex_kv_(hashes[i], hex);
        printf("  [%2d] %s%s\n", i, hex, i==0?" (newest)":i==depth-1?" (root)":"");
    }
    free(hashes);
    return 0;
}

static int kvcache_cmd_kvlog_(int argc, char **argv) {
    /* kvlog <model_hex> — list all .bfkv files for a model */
    if (argc < 2) {
        fprintf(stderr, "Usage: bonfyre-kvcache kvlog <model_hex>\n"); return 1;
    }
    const char *home = getenv("HOME");
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s/.local/share/bonfyre/kvcache/%s",
             home ? home : "/tmp", argv[1]);
    DIR *d = opendir(dir);
    if (!d) { printf("kvlog: no entries for model %.16s...\n", argv[1]); return 0; }
    int count = 0;
    struct dirent *de;
    printf("KV objects for model %.16s...:\n", argv[1]);
    while ((de = readdir(d))) {
        const char *nm = de->d_name;
        size_t nl = strlen(nm);
        if (nl != 69 || strcmp(nm+64, ".bfkv") != 0) continue;
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s", dir, nm);
        struct stat st;
        if (stat(path, &st) == 0) {
            printf("  %.16s...  %lld B\n", nm, (long long)st.st_size);
            count++;
        }
    }
    closedir(d);
    /* Also check chain dir */
    snprintf(dir, sizeof(dir), "%s/.local/share/bonfyre/kvcache-chain/%s",
             home ? home : "/tmp", argv[1]);
    d = opendir(dir);
    if (d) {
        printf("KV chain commits for model %.16s...:\n", argv[1]);
        while ((de = readdir(d))) {
            const char *nm = de->d_name;
            size_t nl = strlen(nm);
            if (nl != 71 || strcmp(nm+64, ".bfkv2") != 0) continue;
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s", dir, nm);
            struct stat st;
            if (stat(path, &st) == 0) {
                printf("  %.16s...  %lld B\n", nm, (long long)st.st_size);
                count++;
            }
        }
        closedir(d);
    }
    if (count == 0) printf("  (none)\n");
    return 0;
}

static int kvcache_cmd_kvpack_(int argc, char **argv) {
    const char *home = getenv("HOME");
    char pack_path[4096];
    /* default path */
    const char *out = NULL;
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], "--out") == 0 && i+1 < argc) out = argv[++i];
    if (out) snprintf(pack_path, sizeof(pack_path), "%s", out);
    else snprintf(pack_path, sizeof(pack_path),
                  "%s/.local/share/bonfyre/kvcache/pack.bfkvpack",
                  home ? home : "/tmp");

    uint32_t n = 0;
    if (bf_kvcache_pack_build(pack_path, &n) != 0) {
        fprintf(stderr, "kvpack: build failed\n"); return 1;
    }
    if (n == 0) { printf("kvpack: no loose .bfkv objects to pack\n"); return 0; }
    printf("kvpack: packed %u KV blobs → %s\n", n, pack_path);
    printf("  run 'bonfyre-kvcache kvgc' to remove loose files\n");
    return 0;
}

static int kvcache_cmd_kvgc_(int argc, char **argv) {
    const char *home = getenv("HOME");
    char pack_path[4096];
    const char *pp = NULL;
    for (int i = 1; i < argc; i++)
        if (strcmp(argv[i], "--pack") == 0 && i+1 < argc) pp = argv[++i];
    if (pp) snprintf(pack_path, sizeof(pack_path), "%s", pp);
    else snprintf(pack_path, sizeof(pack_path),
                  "%s/.local/share/bonfyre/kvcache/pack.bfkvpack",
                  home ? home : "/tmp");

    BfKVCachePack pack = {0};
    if (bf_kvcache_pack_open(&pack, pack_path) != 0) {
        printf("kvgc: no pack found — run 'bonfyre-kvcache kvpack' first\n"); return 0;
    }
    int removed = bf_kvcache_pack_gc(&pack);
    bf_kvcache_pack_close(&pack);
    printf("kvgc: removed %d loose .bfkv files\n", removed);
    return 0;
}

/* ─────────────────────────────────────────────────────────── */

/* Parse a 64-char hex string into 32 bytes. Returns 0 on success. */
static int parse_hex32(const char *hex, uint8_t out[32]) {
    if (strlen(hex) != 64) return -1;
    for (int i = 0; i < 32; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%02x", &byte) != 1) return -1;
        out[i] = (uint8_t)byte;
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }

    const char *cmd = argv[1];
    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) {
        usage(); return 0;
    }

    int bits = 4;  /* default 4-bit for KV (safer) */
    const char *v;
    if ((v = bf_arg_value(argc, argv, "--bits"))) bits = atoi(v);

    /* ── store: persist a .bfkv blob by (model_hash, ctx_hash) ── */
    if (strcmp(cmd, "store") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Usage: bonfyre-kvcache store <model_hash_hex> <ctx_hash_hex> <file.bfkv>\n");
            return 1;
        }
        uint8_t mhash[32], chash[32];
        if (parse_hex32(argv[2], mhash) != 0 || parse_hex32(argv[3], chash) != 0) {
            fprintf(stderr, "store: hashes must be 64 hex chars each\n"); return 1;
        }
        const char *inpath = argv[4];
        size_t len = 0;
        void *data = (void *)bf_read_file(inpath, &len);
        if (!data) { perror(inpath); return 1; }
        int rc = bf_kvcache_store(mhash, chash, data, len);
        free(data);
        if (rc != 0) { fprintf(stderr, "store: write failed\n"); return 1; }
        fprintf(stdout, "stored: model=%.8s... ctx=%.8s... bytes=%zu\n",
                argv[2], argv[3], len);
        return 0;
    }

    /* ── fetch: retrieve a .bfkv blob by (model_hash, ctx_hash) ── */
    if (strcmp(cmd, "fetch") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Usage: bonfyre-kvcache fetch <model_hash_hex> <ctx_hash_hex> [--out file]\n");
            return 1;
        }
        uint8_t mhash[32], chash[32];
        if (parse_hex32(argv[2], mhash) != 0 || parse_hex32(argv[3], chash) != 0) {
            fprintf(stderr, "fetch: hashes must be 64 hex chars each\n"); return 1;
        }
        const char *outpath = (v = bf_arg_value(argc, argv, "--out")) ? v : NULL;
        void *data = NULL;
        size_t len = 0;
        if (bf_kvcache_fetch(mhash, chash, &data, &len) != 0) {
            fprintf(stderr, "fetch: cache miss — model=%.8s... ctx=%.8s...\n",
                    argv[2], argv[3]);
            return 2;  /* 2 = miss (distinguishable from error) */
        }
        if (outpath) {
            FILE *f = fopen(outpath, "wb");
            if (!f) { perror(outpath); free(data); return 1; }
            fwrite(data, 1, len, f);
            fclose(f);
            fprintf(stdout, "fetched: %zu bytes → %s\n", len, outpath);
        } else {
            fwrite(data, 1, len, stdout);
        }
        free(data);
        return 0;
    }

    if (strcmp(cmd, "roundtrip") == 0) {
        if (argc < 3) { usage(); return 1; }
        const char *input_path = argv[2];

        /* Read raw float tensor */
        FILE *f = fopen(input_path, "rb");
        if (!f) { perror(input_path); return 1; }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);
        size_t total = (size_t)fsize / sizeof(float);
        float *data = (float *)malloc((size_t)fsize);
        if (fread(data, sizeof(float), total, f) != total) {
            fprintf(stderr, "Read error\n");
            fclose(f); free(data); return 1;
        }
        fclose(f);

        printf("KV roundtrip: %zu floats (%zu blocks) @ %d-bit\n",
               total, (total + BLOCK_DIM - 1) / BLOCK_DIM, bits);

        KVResult r = kv_roundtrip(data, total, input_path, bits);
        printf("  cos=%.6f  rmse=%.6f  bpw=%.2f  tiles=%d\n",
               r.cosine, r.rmse, r.bpw, r.effective_k);

        free(data);
        return 0;
    }

    if (strcmp(cmd, "benchmark") == 0) {
        /* A benchmark measures a tensor somebody recorded. Gaussian noise
         * stood here once and reported a pass; noise is not a key/value
         * cache, and its cosine says nothing about one. */
        const char *input_path = (argc >= 3 && argv[2][0] != '-') ? argv[2] : NULL;
        if (!input_path) {
            fprintf(stderr, "bonfyre-kvcache benchmark: a recorded KV tensor file is required "
                            "(raw float32). There is no built-in tensor to measure.\n");
            return 1;
        }
        FILE *f = fopen(input_path, "rb");
        if (!f) { perror(input_path); return 1; }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);
        size_t total = fsize > 0 ? (size_t)fsize / sizeof(float) : 0;
        if (total < BLOCK_DIM) {
            fprintf(stderr, "bonfyre-kvcache benchmark: %s holds fewer than %d floats\n", input_path, BLOCK_DIM);
            fclose(f); return 1;
        }
        float *data = (float *)malloc(total * sizeof(float));
        if (!data || fread(data, sizeof(float), total, f) != total) {
            fprintf(stderr, "Read error\n");
            fclose(f); free(data); return 1;
        }
        fclose(f);

        printf("═══════════════════════════════════════════════════════\n");
        printf(" bonfyre-kvcache v8 RLF — %s @%d-bit\n", input_path, bits);
        printf("═══════════════════════════════════════════════════════\n\n");
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        KVResult r = kv_roundtrip(data, total, input_path, bits);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double secs = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
        printf("  %zu floats  cos=%.6f  rmse=%.6f  bpw=%.2f  tiles=%d\n",
               total, r.cosine, r.rmse, r.bpw, r.effective_k);
        printf("  time: %.3f s (%.2f M floats/s)\n", secs, secs > 0 ? (double)total / secs / 1e6 : 0.0);
        printf("  %s 0.999 cosine\n", r.cosine > 0.999 ? "at or above" : "below");
        printf("\n═══════════════════════════════════════════════════════\n");
        free(data);
        return r.cosine > 0.999 ? 0 : 2;
    }

    if (strcmp(cmd, "context-verify") == 0) {
        return kvcache_cmd_context_verify_(argc - 1, argv + 1);
    }
    if (strcmp(cmd, "context-compile") == 0) {
        return kvcache_cmd_context_compile_(argc - 1, argv + 1);
    }

    /* ── new subcommands: chain/ancestry/kvlog/kvpack/kvgc ─── */
    if (strcmp(cmd, "chain")    == 0) return kvcache_cmd_chain_   (argc-1, argv+1);
    if (strcmp(cmd, "ancestry") == 0) return kvcache_cmd_ancestry_(argc-1, argv+1);
    if (strcmp(cmd, "kvlog")    == 0) return kvcache_cmd_kvlog_   (argc-1, argv+1);
    if (strcmp(cmd, "kvpack")   == 0) return kvcache_cmd_kvpack_  (argc-1, argv+1);
    if (strcmp(cmd, "kvgc")     == 0) return kvcache_cmd_kvgc_    (argc-1, argv+1);

    fprintf(stderr, "Unknown command: %s\n", cmd);
    usage();
    return 1;
}
