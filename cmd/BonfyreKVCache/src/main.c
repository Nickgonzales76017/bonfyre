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
 *   bonfyre-kvcache benchmark [--bits 4]
 *   bonfyre-kvcache --help
 *
 * Benchmark results (Qwen-0.5B, WikiText-2):
 *   3-bit KV: PPL 17.89 (+49.7%), avg cos 0.9997
 *   4-bit KV: PPL 14.77 (+23.6%), avg cos 0.9999
 */
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <dirent.h>
#include <sys/stat.h>
#include <bonfyre.h>

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
        "  bonfyre-kvcache benchmark  [--bits N]\n"
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
        printf("═══════════════════════════════════════════════════════\n");
        printf(" bonfyre-kvcache v8 RLF — Self-test@%d-bit\n", bits);
        printf("═══════════════════════════════════════════════════════\n\n");

        /* Simulate KV projection: head_dim=64, n_heads=16, seq_len=128 */
        size_t head_dim = 64, n_heads = 16, seq_len = 128;
        size_t total = head_dim * n_heads * seq_len;  /* 131072 */
        float *kv_data = (float *)malloc(total * sizeof(float));

        uint64_t rng = 0xCAFEBEEF;
        for (size_t i = 0; i < total; i++) {
            rng = xorshift64(rng);
            float u1 = ((float)(rng & 0xFFFF) + 1.0f) / 65537.0f;
            rng = xorshift64(rng);
            float u2 = ((float)(rng & 0xFFFF) + 1.0f) / 65537.0f;
            kv_data[i] = sqrtf(-2.0f * logf(u1)) * cosf(6.2832f * u2) * 0.02f;
        }

        /* Test K projection */
        printf("  K projection (%zux%zux%zu = %zu floats):\n",
               n_heads, seq_len, head_dim, total);
        KVResult rk = kv_roundtrip(kv_data, total, "k_proj", bits);
        printf("    cos=%.6f  rmse=%.6f  bpw=%.2f  tiles=%d\n",
               rk.cosine, rk.rmse, rk.bpw, rk.effective_k);

        /* Test V projection */
        printf("  V projection (%zux%zux%zu = %zu floats):\n",
               n_heads, seq_len, head_dim, total);
        KVResult rv = kv_roundtrip(kv_data, total, "v_proj", bits);
        printf("    cos=%.6f  rmse=%.6f  bpw=%.2f  tiles=%d\n",
               rv.cosine, rv.rmse, rv.bpw, rv.effective_k);

        double avg_cos = (rk.cosine + rv.cosine) / 2.0;
        printf("\n  Average cos=%.6f\n", avg_cos);

        if (avg_cos > 0.999) {
            printf("  ✓ PASS — KV compression within tolerance\n");
        } else {
            printf("  ⚠ WARNING — KV cos below 0.999 (compound error risk)\n");
        }

        /* Artifact */
        BfArtifact art;
        bf_artifact_init(&art);
        strncpy(art.artifact_type, "kvcache-benchmark", sizeof(art.artifact_type) - 1);
        strncpy(art.source_system, "BonfyreKVCache", sizeof(art.source_system) - 1);
        bf_iso_timestamp(art.created_at, sizeof(art.created_at));
        bf_artifact_compute_keys(&art);
        char json[2048];
        bf_artifact_to_json(&art, json, sizeof(json));
        printf("\n  Artifact: %s\n", json);

        printf("\n═══════════════════════════════════════════════════════\n");
        free(kv_data);
        return 0;
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

/* ─── stub comment (placeholder removed) ─── */
