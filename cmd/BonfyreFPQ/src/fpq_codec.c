/* M_PI is not in ISO C11; glibc gates it behind a feature macro while
   macOS exposes it from <math.h> unconditionally. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/*
 * fpq_codec.c — FPQ v3 Encode/Decode Pipeline
 *
 * v3 — FOUR LAMBDA CALCULUS OPTIMIZATIONS:
 *
 *   1. MEMOIZATION: Cache seed patterns across blocks with similar
 *      statistics. Memory grows with unique patterns; compute shrinks.
 *
 *   2. GRAPH REDUCTION: Tensor-level "base seed" shared across all blocks
 *      as a DAG node. Per-block seeds capture only the delta. "Virtual
 *      Model Scaling" — shared logic, smaller VRAM footprint.
 *
 *   3. LAZY EVALUATION / THUNKS: Dual-mode encoding. FWHT mode for
 *      unstructured weights (radius concentration). DIRECT mode for
 *      structured weights (seeds exploit raw angular patterns).
 *      "Depth-on-demand" — skip FWHT when structure is more valuable.
 *
 *   4. SUPERCOMBINATOR MAPPING: In direct mode, the raw weight structure
 *      maps directly to lambda terms without the FWHT data-oblivious
 *      randomization layer. Seeds become "native logic blocks" that
 *      capture the actual weight patterns (smooth, periodic, low-rank).
 *
 * THE KEY INSIGHT:
 *   FWHT destroys compressible structure. After FWHT, angles are random
 *   noise that seeds can't compress. In direct mode, seeds work on the
 *   RAW polar angles which retain the original weight structure.
 *   Cost: per-block radius (0.125 bpw). Benefit: 10-50x better seeds.
 *
 * ENCODE PIPELINE:
 *   Phase 1: Compute all blocks' target vectors (mode-dependent)
 *   Phase 2: Graph reduction — mean target → base seed (shared DAG)
 *   Phase 3: Per-block delta seed with memoization
 *   Phase 4: QJL residual correction
 *
 * DECODE PIPELINE:
 *   1. Expand base seed ONCE (graph reduction benefit)
 *   2. Per block: expand delta + base + expected → polar inverse
 */
#include "fpq.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ═══════════════════════════════════════════════════════════════════
 * LLOYD-MAX GAUSSIAN QUANTIZERS (Supercombinator #1)
 *
 * Optimal n-level quantizers for N(0,1) distribution.
 * After FWHT, each coordinate ~ N(0, σ²/N). Scale by RMS to normalize.
 *
 *   2-bit: MSE = 0.1175σ²  → cosine ≈ 0.946
 *   3-bit: MSE = 0.03454σ² → cosine ≈ 0.983
 * ═══════════════════════════════════════════════════════════════════ */

/* 2-bit (4 levels) */
static const float LLOYD2_BOUNDS[3]  = { -0.9816f, 0.0f, 0.9816f };
static const float LLOYD2_CENTERS[4] = { -1.5104f, -0.4528f, 0.4528f, 1.5104f };

/* 3-bit (8 levels) */
static const float LLOYD3_BOUNDS[7]  = {
    -1.7479f, -1.0500f, -0.5006f, 0.0f, 0.5006f, 1.0500f, 1.7479f
};
static const float LLOYD3_CENTERS[8] = {
    -2.1520f, -1.3440f, -0.7560f, -0.2451f,
     0.2451f,  0.7560f,  1.3440f,  2.1520f
};

/* 4-bit (16 levels) — optimal for N(0,1), MSE = 0.009497 */
static const float LLOYD4_BOUNDS[15] = {
    -2.4008f, -1.8440f, -1.4371f, -1.0993f, -0.7977f, -0.5157f, -0.2451f,
     0.0f,
     0.2451f,  0.5157f,  0.7977f,  1.0993f,  1.4371f,  1.8440f,  2.4008f
};
static const float LLOYD4_CENTERS[16] = {
    -2.7326f, -2.0690f, -1.6180f, -1.2562f, -0.9423f, -0.6568f, -0.3881f, -0.1284f,
     0.1284f,  0.3881f,  0.6568f,  0.9423f,  1.2562f,  1.6180f,  2.0690f,  2.7326f
};

static inline int lloyd_quantize_2(float v) {
    if (v < LLOYD2_BOUNDS[0]) return 0;
    if (v < LLOYD2_BOUNDS[1]) return 1;
    if (v < LLOYD2_BOUNDS[2]) return 2;
    return 3;
}

static inline int lloyd_quantize_3(float v) {
    if (v < LLOYD3_BOUNDS[0]) return 0;
    if (v < LLOYD3_BOUNDS[1]) return 1;
    if (v < LLOYD3_BOUNDS[2]) return 2;
    if (v < LLOYD3_BOUNDS[3]) return 3;
    if (v < LLOYD3_BOUNDS[4]) return 4;
    if (v < LLOYD3_BOUNDS[5]) return 5;
    if (v < LLOYD3_BOUNDS[6]) return 6;
    return 7;
}

static inline int lloyd_quantize_4(float v) {
    /* Binary search over 16 levels for speed */
    if (v < LLOYD4_BOUNDS[7]) {
        if (v < LLOYD4_BOUNDS[3]) {
            if (v < LLOYD4_BOUNDS[1]) return (v < LLOYD4_BOUNDS[0]) ? 0 : 1;
            return (v < LLOYD4_BOUNDS[2]) ? 2 : 3;
        } else {
            if (v < LLOYD4_BOUNDS[5]) return (v < LLOYD4_BOUNDS[4]) ? 4 : 5;
            return (v < LLOYD4_BOUNDS[6]) ? 6 : 7;
        }
    } else {
        if (v < LLOYD4_BOUNDS[11]) {
            if (v < LLOYD4_BOUNDS[9]) return (v < LLOYD4_BOUNDS[8]) ? 8 : 9;
            return (v < LLOYD4_BOUNDS[10]) ? 10 : 11;
        } else {
            if (v < LLOYD4_BOUNDS[13]) return (v < LLOYD4_BOUNDS[12]) ? 12 : 13;
            return (v < LLOYD4_BOUNDS[14]) ? 14 : 15;
        }
    }
}

static inline int lloyd_quantize(float v, int bits) {
    if (bits == 4) return lloyd_quantize_4(v);
    if (bits == 3) return lloyd_quantize_3(v);
    return lloyd_quantize_2(v);
}

static inline float lloyd_dequantize(int idx, int bits) {
    if (bits == 4) return LLOYD4_CENTERS[idx];
    if (bits == 3) return LLOYD3_CENTERS[idx];
    return LLOYD2_CENTERS[idx];
}

/* ── Utility functions ── */

float fpq_mse(const float *a, const float *b, size_t n) {
    float sum = 0.0f;
    for (size_t i = 0; i < n; i++) {
        float d = a[i] - b[i];
        sum += d * d;
    }
    return sum / (float)n;
}

float fpq_cosine_sim(const float *a, const float *b, size_t n) {
    float dot = 0.0f, na = 0.0f, nb = 0.0f;
    for (size_t i = 0; i < n; i++) {
        dot += a[i] * b[i];
        na  += a[i] * a[i];
        nb  += b[i] * b[i];
    }
    float denom = sqrtf(na) * sqrtf(nb);
    return denom > 1e-10f ? dot / denom : 0.0f;
}

static size_t next_pow2(size_t n) {
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

/* ── Type Inference ── */

fpq_type_info_t fpq_infer_type(const float *weights, size_t n_weights,
                                size_t block_dim) {
    fpq_type_info_t info = {0};

    float sum = 0.0f, sum_sq = 0.0f;
    for (size_t i = 0; i < n_weights; i++) {
        sum    += weights[i];
        sum_sq += weights[i] * weights[i];
    }
    float mean = sum / (float)n_weights;
    float var  = sum_sq / (float)n_weights - mean * mean;
    info.sigma = sqrtf(var > 0 ? var : 1e-10f);
    info.expected_radius = info.sigma * sqrtf((float)block_dim);

    size_t n_blocks = (n_weights + block_dim - 1) / block_dim;
    float *block_radii = (float *)malloc(n_blocks * sizeof(float));
    for (size_t b = 0; b < n_blocks; b++) {
        size_t offset = b * block_dim;
        size_t this_dim = (offset + block_dim <= n_weights) ? block_dim : (n_weights - offset);
        float r_sq = 0.0f;
        for (size_t i = 0; i < this_dim; i++)
            r_sq += weights[offset + i] * weights[offset + i];
        block_radii[b] = sqrtf(r_sq);
    }
    float mean_r = 0.0f;
    for (size_t b = 0; b < n_blocks; b++) mean_r += block_radii[b];
    mean_r /= (float)n_blocks;
    float var_r = 0.0f;
    for (size_t b = 0; b < n_blocks; b++) {
        float d = block_radii[b] - mean_r;
        var_r += d * d;
    }
    info.radius_variance = var_r / (float)n_blocks;
    info.expected_radius = mean_r;
    free(block_radii);
    return info;
}

/* ── Expected Angles ── */

void fpq_expected_angles(size_t block_dim, float *expected) {
    if (block_dim <= 1) return;
    size_t n_angles = block_dim - 1;
    for (size_t i = 0; i < n_angles - 1; i++)
        expected[i] = (float)M_PI / 2.0f;
    expected[n_angles - 1] = (float)M_PI;
}

/* ═══════════════════════════════════════════════════════════════════
 * OPTIMIZATION #1: MEMOIZATION (Sub-problem Caching)
 *
 * A hash table mapping (quantized_mean, quantized_std, dim) to cached
 * seed programs. When a new block has similar statistics to a previously
 * solved block, we clone the cached seed and verify it instead of
 * running full discovery.
 *
 * In a model like Gemma 4, many weight vectors share similar "semantic
 * headings" on the hypersphere. The cache matures over time: first
 * blocks are slow (discovery), subsequent blocks are fast (cache hit).
 *
 * Cost scaling: memory = O(unique_patterns), compute = O(1) per hit.
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float       mean;
    float       std;
    size_t      dim;
    fpq_seed_t *seed;
    int         valid;
} fpq_memo_entry_t;

static fpq_memo_entry_t g_memo[FPQ_MEMO_SLOTS];
static int g_memo_initialized = 0;

void fpq_memo_reset(void) {
    for (int i = 0; i < FPQ_MEMO_SLOTS; i++) {
        if (g_memo[i].valid && g_memo[i].seed)
            fpq_seed_free(g_memo[i].seed);
        g_memo[i].valid = 0;
        g_memo[i].seed = NULL;
    }
    g_memo_initialized = 1;
}

static uint32_t memo_hash(float mean, float std, size_t dim) {
    int32_t qm = (int32_t)(mean * 50.0f);
    int32_t qs = (int32_t)(std * 50.0f);
    uint32_t h = (uint32_t)qm * 2654435761U ^ (uint32_t)qs * 40503U ^ (uint32_t)dim;
    return h % FPQ_MEMO_SLOTS;
}

static float vec_mean(const float *x, size_t n) {
    float s = 0.0f;
    for (size_t i = 0; i < n; i++) s += x[i];
    return s / (float)n;
}

static float vec_std(const float *x, size_t n, float mean) {
    float s = 0.0f;
    for (size_t i = 0; i < n; i++) {
        float d = x[i] - mean;
        s += d * d;
    }
    return sqrtf(s / (float)n);
}

static fpq_seed_t *memo_lookup(const float *target, size_t dim,
                                float mean, float std) {
    if (!g_memo_initialized) return NULL;
    uint32_t slot = memo_hash(mean, std, dim);
    fpq_memo_entry_t *e = &g_memo[slot];

    if (!e->valid || e->dim != dim) return NULL;
    if (fabsf(e->mean - mean) > 0.15f) return NULL;
    if (fabsf(e->std - std) > 0.15f) return NULL;

    fpq_seed_t *clone = fpq_seed_clone(e->seed);
    if (!clone) return NULL;

    /* Re-measure distortion on the new target (lazy: subsample) */
    float *tmp = (float *)malloc(dim * sizeof(float));
    fpq_seed_expand(clone, tmp);
    float mse = fpq_mse(target, tmp, dim);
    free(tmp);
    clone->distortion = mse;
    return clone;
}

static void memo_store(float mean, float std, size_t dim,
                        const fpq_seed_t *seed) {
    if (!g_memo_initialized) fpq_memo_reset();
    uint32_t slot = memo_hash(mean, std, dim);
    fpq_memo_entry_t *e = &g_memo[slot];

    /* Only replace if new seed is better or slot is empty */
    if (e->valid && e->seed && e->seed->distortion < seed->distortion)
        return;

    if (e->valid && e->seed) fpq_seed_free(e->seed);
    e->mean = mean;
    e->std = std;
    e->dim = dim;
    e->seed = fpq_seed_clone(seed);
    e->valid = 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * OPTIMIZATION #3/#4: MODE SELECTION (Lazy Thunks + Supercombinators)
 *
 * FWHT is data-oblivious: it randomizes the weight vector so all
 * blocks have identical statistical properties (sphere hardening).
 * This is great for radius concentration but DESTROYS the spatial
 * structure that seeds could exploit.
 *
 * DIRECT MODE: Skip FWHT entirely. The raw weight values have rich
 * structure: smooth gradients, periodic patterns, low-rank components.
 * Seeds are "supercombinators" that compile directly to these patterns.
 *
 * The mode decision is the "lazy thunk": we don't force FWHT evaluation
 * unless the tensor actually needs it (unstructured/random weights).
 * Structured tensors stay in their natural representation.
 *
 * Decision: lag-1 autocorrelation > 0.3 → DIRECT, else → FWHT
 * ═══════════════════════════════════════════════════════════════════ */

static float compute_autocorrelation(const float *data, size_t n) {
    if (n < 2) return 0.0f;
    /* Subsample for speed */
    size_t stride = (n > 10000) ? n / 10000 : 1;
    size_t count = 0;
    float mean = 0.0f;
    for (size_t i = 0; i < n; i += stride) { mean += data[i]; count++; }
    mean /= (float)count;

    float cov = 0.0f, var = 0.0f;
    for (size_t i = 0; i < n - stride; i += stride) {
        float di = data[i] - mean;
        float di1 = data[i + stride] - mean;
        cov += di * di1;
        var += di * di;
    }
    size_t last = ((n - 1) / stride) * stride;
    float dl = data[last] - mean;
    var += dl * dl;

    return (var > 1e-10f) ? cov / var : 0.0f;
}

/* ── Block target computation (mode-dependent) ── */

static float compute_block_target(const float *block, size_t dim,
                                   uint64_t haar_seed, int mode,
                                   float *target_out) {
    size_t padded = next_pow2(dim);
    float *buf = (float *)calloc(padded, sizeof(float));
    memcpy(buf, block, dim * sizeof(float));

    float *angles = (float *)malloc((padded - 1) * sizeof(float));
    float radius;

    if (mode == FPQ_MODE_FWHT) {
        fpq_random_signs(buf, padded, haar_seed);
        fpq_fwht(buf, padded);
        radius = fpq_polar_encode(buf, padded, angles);

        float *expected = (float *)malloc((padded - 1) * sizeof(float));
        fpq_expected_angles(padded, expected);
        for (size_t i = 0; i < padded - 1; i++)
            target_out[i] = angles[i] - expected[i];
        free(expected);
    } else {
        /* Direct + pairwise: preserves local weight structure.
         * Adjacent-pair angles capture sin/cos patterns directly. */
        radius = fpq_polar_encode_pairwise(buf, padded, angles);
        memcpy(target_out, angles, (padded - 1) * sizeof(float));
    }

    free(buf);
    free(angles);
    return radius;
}

/* ── Block reconstruction (inverse of compute_block_target) ── */

static void reconstruct_block(const float *angles, float radius,
                               uint64_t haar_seed, int mode,
                               size_t dim, float *output) {
    size_t padded = next_pow2(dim);
    float *buf = (float *)malloc(padded * sizeof(float));

    if (mode == FPQ_MODE_FWHT) {
        fpq_polar_decode(radius, angles, padded, buf);
        fpq_fwht_inverse(buf, padded);
        fpq_random_signs_inverse(buf, padded, haar_seed);
    } else {
        /* Direct + pairwise: inverse hierarchical decomposition */
        fpq_polar_decode_pairwise(radius, angles, padded, buf);
    }

    memcpy(output, buf, dim * sizeof(float));
    free(buf);
}

/* ── LAZY MODE SELECTION: True Thunk Evaluation ──
 *
 * Instead of a cheap heuristic (autocorrelation), we actually TRY both
 * modes on a few sample blocks and measure ROUNDTRIP cosine similarity.
 * This is the "lazy thunk" — defer the evaluation until we see the data.
 *
 * Cost: ~6 extra seed discoveries (3 samples × 2 modes) + COORD eval.
 * Benefit: always picks the objectively best mode for each tensor.
 */
static uint8_t g_auto_coord_bits = 2;  /* chosen by auto_select_mode */

static int auto_select_mode(const float *weights, size_t total,
                             size_t block_dim, uint64_t haar_seed,
                             size_t max_seed_nodes, float tolerance) {
    size_t padded = next_pow2(block_dim);
    size_t target_dim = padded - 1;
    size_t n_blocks = (total + block_dim - 1) / block_dim;
    size_t n_samples = (n_blocks < 3) ? n_blocks : 3;

    float fwht_cosine_sum = 0.0f;
    float direct_cosine_sum = 0.0f;

    for (size_t s = 0; s < n_samples; s++) {
        size_t b = (n_samples > 1) ? s * (n_blocks - 1) / (n_samples - 1) : 0;
        size_t offset = b * block_dim;
        size_t this_dim = (offset + block_dim <= total) ? block_dim : (total - offset);

        /* ── Evaluate FWHT and DIRECT modes (seed-based) ── */
        for (int mode = 0; mode <= 1; mode++) {
            float *target = (float *)malloc(target_dim * sizeof(float));
            float radius = compute_block_target(
                weights + offset, this_dim,
                haar_seed ^ (uint64_t)b, mode, target);

            fpq_seed_t *seed = fpq_seed_discover(target, target_dim,
                                                   max_seed_nodes, tolerance);

            /* Full roundtrip to Cartesian to measure true quality */
            float *expanded = (float *)malloc(target_dim * sizeof(float));
            fpq_seed_expand(seed, expanded);

            float *angles = (float *)malloc(target_dim * sizeof(float));
            if (mode == FPQ_MODE_FWHT) {
                float *exp_a = (float *)malloc(target_dim * sizeof(float));
                fpq_expected_angles(padded, exp_a);
                for (size_t i = 0; i < target_dim; i++)
                    angles[i] = expanded[i] + exp_a[i];
                free(exp_a);
            } else {
                memcpy(angles, expanded, target_dim * sizeof(float));
            }

            float *recon = (float *)malloc(this_dim * sizeof(float));
            reconstruct_block(angles, radius,
                              haar_seed ^ (uint64_t)b, mode,
                              this_dim, recon);

            float cosine = fpq_cosine_sim(weights + offset, recon, this_dim);

            if (mode == FPQ_MODE_FWHT)
                fwht_cosine_sum += cosine;
            else
                direct_cosine_sum += cosine;

            fpq_seed_free(seed);
            free(target);
            free(expanded);
            free(angles);
            free(recon);
        }
    }

    int choice = (direct_cosine_sum > fwht_cosine_sum) ? FPQ_MODE_DIRECT : FPQ_MODE_FWHT;

    /* ── Evaluate COORD mode: FWHT + Lloyd-Max (no polar, no seed) ──
     * This is the "supercombinator hardware mapping": compile the lambda
     * term to native quantization logic. Try 2-bit, 3-bit, and 4-bit. */
    float coord2_cosine_sum = 0.0f;
    float coord3_cosine_sum = 0.0f;
    float coord4_cosine_sum = 0.0f;

    for (size_t s = 0; s < n_samples; s++) {
        size_t b = (n_samples > 1) ? s * (n_blocks - 1) / (n_samples - 1) : 0;
        size_t offset = b * block_dim;
        size_t this_dim = (offset + block_dim <= total) ? block_dim : (total - offset);

        float *buf = (float *)calloc(padded, sizeof(float));
        memcpy(buf, weights + offset, this_dim * sizeof(float));

        fpq_random_signs(buf, padded, haar_seed ^ (uint64_t)b);
        fpq_fwht(buf, padded);

        /* Per-block RMS scale */
        float rms = 0.0f;
        for (size_t i = 0; i < padded; i++) rms += buf[i] * buf[i];
        rms = sqrtf(rms / (float)padded);
        if (rms < 1e-10f) rms = 1e-10f;

        /* Try 2-bit, 3-bit, 4-bit */
        for (int cbits = 2; cbits <= 4; cbits++) {
            float *q = (float *)malloc(padded * sizeof(float));
            for (size_t i = 0; i < padded; i++) {
                int idx = lloyd_quantize(buf[i] / rms, cbits);
                q[i] = lloyd_dequantize(idx, cbits) * rms;
            }
            fpq_fwht_inverse(q, padded);
            fpq_random_signs_inverse(q, padded, haar_seed ^ (uint64_t)b);
            float cos = fpq_cosine_sim(weights + offset, q, this_dim);
            if (cbits == 2) coord2_cosine_sum += cos;
            else if (cbits == 3) coord3_cosine_sum += cos;
            else coord4_cosine_sum += cos;
            free(q);
        }

        free(buf);
    }

    /* Pick the overall winner — prefer lower bpw at similar quality.
     * Upgrade bit depth only if it's meaningfully better (>2% improvement). */
    float best_seed_cos = (choice == FPQ_MODE_DIRECT) ? direct_cosine_sum : fwht_cosine_sum;
    int coord_bits = 2;
    float coord_cosine_sum = coord2_cosine_sum;
    if (coord3_cosine_sum > coord_cosine_sum * 1.02f) {
        coord_bits = 3;
        coord_cosine_sum = coord3_cosine_sum;
    }
    if (coord4_cosine_sum > coord_cosine_sum * 1.02f) {
        coord_bits = 4;
        coord_cosine_sum = coord4_cosine_sum;
    }

    if (coord_cosine_sum > best_seed_cos)
        choice = FPQ_MODE_COORD;

    fprintf(stderr, "  Thunk eval: FWHT=%.4f DIRECT=%.4f COORD@2=%.4f @3=%.4f @4=%.4f → %s",
            fwht_cosine_sum / (float)n_samples,
            direct_cosine_sum / (float)n_samples,
            coord2_cosine_sum / (float)n_samples,
            coord3_cosine_sum / (float)n_samples,
            coord4_cosine_sum / (float)n_samples,
            choice == FPQ_MODE_DIRECT ? "DIRECT" :
            choice == FPQ_MODE_COORD  ? "COORD"  : "FWHT");
    if (choice == FPQ_MODE_COORD)
        fprintf(stderr, "@%d\n", coord_bits);
    else
        fprintf(stderr, "\n");

    /* Stash coord_bits in a static for encode_tensor to pick up.
     * This is a workaround for the current API — the mode selection
     * needs to communicate the chosen bit depth. */
    g_auto_coord_bits = (uint8_t)coord_bits;

    return choice;
}

/* ═══════════════════════════════════════════════════════════════════
 * FULL TENSOR ENCODE (v3: all 4 lambda optimizations)
 *
 * Phase 1: Compute all blocks' target vectors (first pass)
 * Phase 2: Graph Reduction — discover shared base seed (DAG node)
 * Phase 3: Per-block delta seed discovery with Memoization
 * Phase 4: QJL residual correction
 * ═══════════════════════════════════════════════════════════════════ */

fpq_tensor_t *fpq_encode_tensor(const float *weights, size_t rows, size_t cols,
                                 const char *name, size_t max_seed_nodes,
                                 float tolerance) {
    size_t total = rows * cols;
    size_t block_dim = FPQ_BLOCK_DIM;
    size_t n_blocks = (total + block_dim - 1) / block_dim;
    size_t padded = next_pow2(block_dim);
    size_t target_dim = padded - 1; /* 255 angles */

    fpq_tensor_t *tensor = (fpq_tensor_t *)calloc(1, sizeof(fpq_tensor_t));
    if (name) strncpy(tensor->name, name, sizeof(tensor->name) - 1);
    tensor->original_rows = rows;
    tensor->original_cols = cols;
    tensor->n_blocks = n_blocks;

    tensor->haar_seed = 0x12345678ULL;
    if (name) {
        for (const char *p = name; *p; p++)
            tensor->haar_seed = tensor->haar_seed * 31 + (uint64_t)*p;
    }

    /* ── OPTIMIZATION #3: Mode Selection (Lazy Thunk) ──
     * Try BOTH modes on sample blocks. Measure ROUNDTRIP cosine.
     * Pick the mode that gives objectively better reconstruction.
     * This is the true lazy thunk: defer evaluation until needed. */
    tensor->mode = auto_select_mode(weights, total, block_dim,
                                     tensor->haar_seed,
                                     max_seed_nodes, tolerance);

    /* Type inference */
    fpq_type_info_t type_info = fpq_infer_type(weights, total, block_dim);
    (void)type_info; /* used implicitly via block radii */

    /* Allocate per-block arrays */
    tensor->seeds = (fpq_seed_t **)calloc(n_blocks, sizeof(fpq_seed_t *));
    tensor->qjl   = (fpq_qjl_t **)calloc(n_blocks, sizeof(fpq_qjl_t *));
    tensor->radii  = (float *)calloc(n_blocks, sizeof(float));

    /* ══════════════════════════════════════════════════════════
     * COORD MODE: Direct 2-bit Lloyd-Max quantization
     *
     * This is the "supercombinator" optimization taken to its logical
     * extreme: the lambda term compiles directly to a hardware-efficient
     * quantization grid. No polar decomposition (eliminates O(N) error
     * amplification). No seed discovery (2-bit quantization is optimal
     * for i.i.d. Gaussian coordinates post-FWHT).
     *
     * MSE = 0.1175σ² → cosine ≈ 0.946 at 2 bpw.
     * ══════════════════════════════════════════════════════════ */
    if (tensor->mode == FPQ_MODE_COORD) {
        tensor->coord_bits = g_auto_coord_bits;
        tensor->coord_scales = (float *)calloc(n_blocks, sizeof(float));
        tensor->coord_quants = (uint8_t **)calloc(n_blocks, sizeof(uint8_t *));
        tensor->coord_residual_norms = (float *)calloc(n_blocks, sizeof(float));

        int cbits = (int)tensor->coord_bits;

        for (size_t b = 0; b < n_blocks; b++) {
            size_t offset = b * block_dim;
            size_t this_dim = (offset + block_dim <= total) ? block_dim : (total - offset);

            float *buf = (float *)calloc(padded, sizeof(float));
            memcpy(buf, weights + offset, this_dim * sizeof(float));

            /* FWHT transform (decorrelate) */
            fpq_random_signs(buf, padded, tensor->haar_seed ^ (uint64_t)b);
            fpq_fwht(buf, padded);

            /* Per-block RMS scale */
            float rms = 0.0f;
            for (size_t i = 0; i < padded; i++) rms += buf[i] * buf[i];
            rms = sqrtf(rms / (float)padded);
            if (rms < 1e-10f) rms = 1e-10f;
            tensor->coord_scales[b] = rms;

            /* Lloyd-Max quantization at chosen bit depth */
            tensor->coord_quants[b] = (uint8_t *)malloc(padded * sizeof(uint8_t));
            for (size_t i = 0; i < padded; i++)
                tensor->coord_quants[b][i] = (uint8_t)lloyd_quantize(buf[i] / rms, cbits);

            /* ── QJL 1-bit sign correction on quantization residual ──
             * The residual = true_fwht - dequantized_fwht captures the
             * quantization error. QJL stores 1-bit random projections of
             * this residual, enabling partial reconstruction at decode. */
            float *residual = (float *)malloc(padded * sizeof(float));
            float rnorm_sq = 0.0f;
            for (size_t i = 0; i < padded; i++) {
                float deq = lloyd_dequantize(tensor->coord_quants[b][i], cbits) * rms;
                residual[i] = buf[i] - deq;
                rnorm_sq += residual[i] * residual[i];
            }
            tensor->coord_residual_norms[b] = sqrtf(rnorm_sq);

            tensor->qjl[b] = fpq_qjl_encode(residual, padded,
                                              tensor->haar_seed ^ (uint64_t)b ^ 0xC00DULL);

            free(residual);
            free(buf);
        }

        /* Bit accounting: scale(32b) + indices(cbits×padded) + qjl(64b) + rnorm(32b) per block */
        tensor->total_bits = n_blocks * (32 + padded * (size_t)cbits +
                                          FPQ_QJL_PROJECTIONS + 32);
        tensor->total_seed_nodes = 0;
        tensor->avg_distortion = 0.0f;

        fprintf(stderr, "  Mode: COORD@%d+QJL | %zu blocks\n", cbits, n_blocks);
        return tensor;
    }

    /* ══════════════════════════════════════════════════════════
     * PHASE 1: Compute all blocks' target vectors
     *
     * This first pass enables Graph Reduction: we need ALL targets
     * to compute the shared mean pattern before encoding blocks.
     * ══════════════════════════════════════════════════════════ */
    float **block_targets = (float **)malloc(n_blocks * sizeof(float *));

    for (size_t b = 0; b < n_blocks; b++) {
        size_t offset = b * block_dim;
        size_t this_dim = (offset + block_dim <= total) ? block_dim : (total - offset);

        block_targets[b] = (float *)malloc(target_dim * sizeof(float));
        tensor->radii[b] = compute_block_target(
            weights + offset, this_dim,
            tensor->haar_seed ^ (uint64_t)b,
            tensor->mode, block_targets[b]);
    }

    /* ══════════════════════════════════════════════════════════
     * PHASE 2: GRAPH REDUCTION (Optimization #2)
     *
     * Compute the MEAN target vector across ALL blocks.
     * This captures the shared "logical core" of the tensor:
     * e.g., common rotational symmetry across attention heads.
     *
     * The base seed is a DAG node: multiple blocks point to the
     * same reduction result. "Virtual Model Scaling" — the model
     * appears larger than its physical footprint because logic
     * is shared across the graph.
     *
     * Instead of N copies of the base pattern, we store ONE.
     * ══════════════════════════════════════════════════════════ */
    float *mean_target = (float *)calloc(target_dim, sizeof(float));
    for (size_t b = 0; b < n_blocks; b++)
        for (size_t i = 0; i < target_dim; i++)
            mean_target[i] += block_targets[b][i];
    for (size_t i = 0; i < target_dim; i++)
        mean_target[i] /= (float)n_blocks;

    /* Base seed budget: 1/3 of total. Remaining goes to per-block deltas. */
    size_t base_budget = (max_seed_nodes > 8) ? max_seed_nodes / 3 : max_seed_nodes;
    tensor->base_seed = fpq_seed_discover(mean_target, target_dim,
                                           base_budget, tolerance);

    float *base_expanded = (float *)malloc(target_dim * sizeof(float));
    if (tensor->base_seed)
        fpq_seed_expand(tensor->base_seed, base_expanded);
    else
        memset(base_expanded, 0, target_dim * sizeof(float));

    /* Check if base seed is useful (captures > 10% of mean variance) */
    float mean_var = 0.0f;
    for (size_t i = 0; i < target_dim; i++) {
        mean_var += mean_target[i] * mean_target[i];
    }
    mean_var /= (float)target_dim;

    /* Only use base seed in DIRECT mode. In FWHT mode, mean deviations
     * are ~zero by construction, so the base seed captures nothing useful. */
    int base_useful = (tensor->mode == FPQ_MODE_DIRECT) &&
                      tensor->base_seed &&
                      tensor->base_seed->distortion < mean_var * 0.5f;

    if (!base_useful) {
        /* Base seed doesn't help — give all budget to per-block seeds */
        fpq_seed_free(tensor->base_seed);
        tensor->base_seed = NULL;
        memset(base_expanded, 0, target_dim * sizeof(float));
    }

    /* ══════════════════════════════════════════════════════════
     * PHASE 3: Per-block delta seeds with MEMOIZATION (#1)
     *
     * For each block:
     *   1. delta = target - base (graph reduction)
     *   2. Check memo cache for similar delta (memoization)
     *   3. Cache miss → discover → store to cache
     *
     * Benefit: blocks with similar distributions skip discovery.
     * Exponential speedup as the cache matures.
     * ══════════════════════════════════════════════════════════ */
    fpq_memo_reset();
    size_t base_nodes = (tensor->base_seed) ? tensor->base_seed->tree_size : 0;
    size_t delta_budget = (max_seed_nodes > base_nodes + 4) ?
                           max_seed_nodes - base_nodes : 4;

    size_t total_nodes = base_nodes;
    float total_distortion = 0.0f;
    size_t memo_hits = 0;

    for (size_t b = 0; b < n_blocks; b++) {
        /* Graph reduction: subtract shared base pattern */
        float *delta = (float *)malloc(target_dim * sizeof(float));
        for (size_t i = 0; i < target_dim; i++)
            delta[i] = block_targets[b][i] - base_expanded[i];

        /* Memoization: check cache */
        float d_mean = vec_mean(delta, target_dim);
        float d_std  = vec_std(delta, target_dim, d_mean);

        fpq_seed_t *delta_seed = NULL;
        fpq_seed_t *cached = memo_lookup(delta, target_dim, d_mean, d_std);

        if (cached && cached->distortion < tolerance * 3.0f) {
            /* Cache HIT reuses discovery work. memo_lookup returns a clone:
             * this is NOT physical program sharing and must not be reported
             * as storage deduplication. */
            delta_seed = cached;
            memo_hits++;
        } else {
            /* Cache MISS — full discovery */
            if (cached) fpq_seed_free(cached);
            delta_seed = fpq_seed_discover(delta, target_dim,
                                            delta_budget, tolerance);
            /* Store to cache for future blocks */
            memo_store(d_mean, d_std, target_dim, delta_seed);
        }

        tensor->seeds[b] = delta_seed;

        /* QJL on final residual */
        float *seed_expanded = (float *)malloc(target_dim * sizeof(float));
        fpq_seed_expand(delta_seed, seed_expanded);
        float *residual = (float *)malloc(target_dim * sizeof(float));
        for (size_t i = 0; i < target_dim; i++)
            residual[i] = delta[i] - seed_expanded[i];

        tensor->qjl[b] = fpq_qjl_encode(residual, target_dim,
                                          tensor->haar_seed ^ (uint64_t)b ^ 0xDEADBEEFULL);

        total_nodes += delta_seed ? delta_seed->tree_size : 0;
        total_distortion += delta_seed ? delta_seed->distortion : 0.0f;

        free(delta);
        free(seed_expanded);
        free(residual);
    }

    /* Cleanup phase 1 */
    for (size_t b = 0; b < n_blocks; b++) free(block_targets[b]);
    free(block_targets);
    free(mean_target);
    free(base_expanded);

    tensor->total_seed_nodes = total_nodes;
    tensor->avg_distortion = total_distortion / (float)n_blocks;

    /* Bit accounting */
    size_t delta_nodes = total_nodes - base_nodes;
    size_t seed_bits = base_nodes * 24 + delta_nodes * 24;
    size_t qjl_bits = n_blocks * FPQ_QJL_PROJECTIONS;
    size_t radius_bits = n_blocks * 32;
    tensor->total_bits = seed_bits + qjl_bits + radius_bits;

    fprintf(stderr, "  Mode: %s | Base: %zu nodes | "
                    "Memo: %zu/%zu (%.0f%%)\n",
            tensor->mode == FPQ_MODE_DIRECT ? "DIRECT" : "FWHT",
            base_nodes, memo_hits, n_blocks,
            n_blocks > 0 ? 100.0f * (float)memo_hits / (float)n_blocks : 0.0f);

    return tensor;
}

/* ═══════════════════════════════════════════════════════════════════
 * FULL TENSOR DECODE (v3: graph reduction + dual mode)
 *
 * The graph reduction benefit is realized here: the base seed is
 * expanded ONCE and reused across ALL blocks. In a model with 10K
 * blocks, this means 10K fewer seed expansions.
 * ═══════════════════════════════════════════════════════════════════ */

void fpq_decode_tensor(const fpq_tensor_t *tensor, float *output) {
    size_t total = tensor->original_rows * tensor->original_cols;
    size_t block_dim = FPQ_BLOCK_DIM;
    size_t padded = next_pow2(block_dim);
    size_t target_dim = padded - 1;
    /* ── COORD mode: dequantize + QJL residual correction ── */
    if (tensor->mode == FPQ_MODE_COORD) {
        int cbits = (int)tensor->coord_bits;
        for (size_t b = 0; b < tensor->n_blocks; b++) {
            size_t offset = b * block_dim;
            size_t this_dim = (offset + block_dim <= total) ? block_dim : (total - offset);

            float *buf = (float *)malloc(padded * sizeof(float));
            float scale = tensor->coord_scales[b];

            /* Dequantize */
            for (size_t i = 0; i < padded; i++)
                buf[i] = lloyd_dequantize(tensor->coord_quants[b][i], cbits) * scale;

            /* QJL 1-bit sign correction: reconstruct residual approx
             * and add it back to the dequantized FWHT coordinates.
             * This partially recovers the quantization error. */
            if (tensor->qjl && tensor->qjl[b] &&
                tensor->coord_residual_norms &&
                tensor->coord_residual_norms[b] > 1e-10f) {
                float *resid_approx = (float *)malloc(padded * sizeof(float));
                fpq_qjl_reconstruct(tensor->qjl[b],
                                     tensor->coord_residual_norms[b],
                                     resid_approx);
                for (size_t i = 0; i < padded; i++)
                    buf[i] += resid_approx[i];
                free(resid_approx);
            }

            /* Inverse FWHT → Cartesian */
            fpq_fwht_inverse(buf, padded);
            fpq_random_signs_inverse(buf, padded,
                                      tensor->haar_seed ^ (uint64_t)b);

            memcpy(output + offset, buf, this_dim * sizeof(float));
            free(buf);
        }
        return;
    }

    /* ── Seed-based modes (FWHT / DIRECT) ── */
    /* Expand base seed ONCE (graph reduction: shared DAG node) */
    float *base = (float *)calloc(target_dim, sizeof(float));
    if (tensor->base_seed)
        fpq_seed_expand(tensor->base_seed, base);

    /* Expected angles (only for FWHT mode) */
    float *expected = NULL;
    if (tensor->mode == FPQ_MODE_FWHT) {
        expected = (float *)malloc(target_dim * sizeof(float));
        fpq_expected_angles(padded, expected);
    }

    for (size_t b = 0; b < tensor->n_blocks; b++) {
        size_t offset = b * block_dim;
        size_t this_dim = (offset + block_dim <= total) ? block_dim : (total - offset);

        /* Expand per-block delta seed */
        float *delta = (float *)malloc(target_dim * sizeof(float));
        fpq_seed_expand(tensor->seeds[b], delta);

        /* Reconstruct angles: delta + base + expected */
        float *angles = (float *)malloc(target_dim * sizeof(float));
        for (size_t i = 0; i < target_dim; i++) {
            angles[i] = delta[i] + base[i];
            if (expected) angles[i] += expected[i];
        }

        /* Inverse polar → Cartesian → (inverse FWHT if applicable) */
        reconstruct_block(angles, tensor->radii[b],
                          tensor->haar_seed ^ (uint64_t)b,
                          tensor->mode, this_dim, output + offset);

        free(delta);
        free(angles);
    }

    free(base);
    free(expected);
}

/* ── Compressed inner product ── */

float fpq_dot_product(const fpq_tensor_t *a, size_t row_a,
                      const fpq_tensor_t *b, size_t row_b) {
    size_t n_a = a->original_rows * a->original_cols;
    size_t n_b = b->original_rows * b->original_cols;
    float *da = (float *)malloc(n_a * sizeof(float));
    float *db = (float *)malloc(n_b * sizeof(float));
    fpq_decode_tensor(a, da);
    fpq_decode_tensor(b, db);

    size_t cols = a->original_cols;
    float dot = 0.0f;
    for (size_t i = 0; i < cols; i++)
        dot += da[row_a * cols + i] * db[row_b * cols + i];

    free(da);
    free(db);
    return dot;
}

/* ── Tensor cleanup ── */

void fpq_tensor_free(fpq_tensor_t *tensor) {
    if (!tensor) return;
    for (size_t i = 0; i < tensor->n_blocks; i++) {
        if (tensor->seeds) fpq_seed_free(tensor->seeds[i]);
        if (tensor->qjl)   fpq_qjl_free(tensor->qjl[i]);
        if (tensor->coord_quants) free(tensor->coord_quants[i]);
    }
    free(tensor->seeds);
    free(tensor->qjl);
    free(tensor->radii);
    free(tensor->coord_scales);
    free(tensor->coord_quants);
    free(tensor->coord_residual_norms);
    free(tensor->chaos_r_idx);
    free(tensor->sbb_scale_delta);
    fpq_ghost_free(tensor->ghost);
    fpq_seed_free(tensor->base_seed);
    free(tensor);
}

/* ── Compression stats ── */

static void fpq_seed_density(const fpq_tensor_t *tensor,
                             size_t *unique_programs,
                             size_t *exact_repeats,
                             size_t *unique_nodes,
                             size_t *program_bytes) {
    *unique_programs = 0;
    *exact_repeats = 0;
    *unique_nodes = 0;
    *program_bytes = 0;
    if (!tensor || !tensor->seeds) return;

    for (size_t i = 0; i < tensor->n_blocks; i++) {
        const fpq_seed_t *seed = tensor->seeds[i];
        if (!seed) continue;
        const uint64_t hash = fpq_seed_hash(seed);
        int seen = 0;
        for (size_t j = 0; j < i; j++) {
            const fpq_seed_t *prior = tensor->seeds[j];
            if (!prior) continue;
            if (fpq_seed_hash(prior) == hash && fpq_seed_equal(prior, seed)) {
                seen = 1;
                break;
            }
        }
        if (seen) {
            (*exact_repeats)++;
        } else {
            (*unique_programs)++;
            *unique_nodes += seed->tree_size;
            *program_bytes += fpq_seed_program_bytes(seed);
        }
    }
}


float fpq_bits_per_weight(const fpq_tensor_t *tensor) {
    size_t total_weights = tensor->original_rows * tensor->original_cols;
    if (total_weights == 0) return 0.0f;
    return (float)tensor->total_bits / (float)total_weights;
}

void fpq_report(const fpq_tensor_t *tensor) {
    size_t total = tensor->original_rows * tensor->original_cols;
    float bpw = fpq_bits_per_weight(tensor);
    float original_mb = (float)(total * sizeof(float)) / (1024.0f * 1024.0f);
    float compressed_mb = (float)(tensor->total_bits / 8) / (1024.0f * 1024.0f);
    size_t base_nodes = tensor->base_seed ? tensor->base_seed->tree_size : 0;
    size_t unique_programs = 0, exact_repeats = 0, unique_nodes = 0;
    size_t unique_program_bytes = 0;
    fpq_seed_density(tensor, &unique_programs, &exact_repeats,
                     &unique_nodes, &unique_program_bytes);
    const size_t base_program_bytes = fpq_seed_program_bytes(tensor->base_seed);

    const char *mode_str;
    if (tensor->mode == FPQ_MODE_COORD) {
        static char coord_label[64];
        snprintf(coord_label, sizeof(coord_label),
                 "COORD (%d-bit Lloyd-Max)", (int)tensor->coord_bits);
        mode_str = coord_label;
    } else if (tensor->mode == FPQ_MODE_DIRECT) {
        mode_str = "DIRECT (structured)";
    } else {
        mode_str = "FWHT (randomized)";
    }

    fprintf(stderr,
        "FPQ v3 Report: %s\n"
        "  Shape:           %zu × %zu (%zu weights)\n"
        "  Mode:            %s\n"
        "  Blocks:          %zu (dim=%d)\n",
        tensor->name,
        tensor->original_rows, tensor->original_cols, total,
        mode_str,
        tensor->n_blocks, FPQ_BLOCK_DIM);

    if (tensor->mode == FPQ_MODE_COORD) {
        fprintf(stderr,
            "  Quantization:    %d-bit Lloyd-Max (%d levels)\n"
            "  Bits/weight:     %.2f (fp32=32, q4_0=4.5, TQ=3.5)\n"
            "  Original:        %.2f MB\n"
            "  Compressed:      %.2f MB\n"
            "  Ratio:           %.1fx\n",
            (int)tensor->coord_bits,
            1 << (int)tensor->coord_bits,
            bpw, original_mb, compressed_mb,
            original_mb > 0 ? original_mb / compressed_mb : 0.0f);
    } else {
        fprintf(stderr,
            "  Base seed:       %zu nodes (shared DAG)\n"
            "  Seed nodes:      %zu logical instances (%.1f avg/block)\n"
            "  Seed programs:   %zu exact unique / %zu blocks; %zu exact repeats\n"
            "  Unique seed body:%zu nodes, %zu native bytes + %zu base bytes\n"
            "  Bits/weight:     %.2f (fp32=32, q4_0=4.5, TQ=3.5)\n"
            "  Original:        %.2f MB\n"
            "  Compressed:      %.2f MB\n"
            "  Ratio:           %.1fx\n"
            "  Avg distortion:  %.6f MSE\n"
            "  Density note:    FPQ! v4 factors exact program bodies; per-block evidence remains separate. Native bytes above describe memory, not stored file size.\n",
            base_nodes,
            tensor->total_seed_nodes,
            (float)tensor->total_seed_nodes / (float)tensor->n_blocks,
            unique_programs,
            tensor->n_blocks,
            exact_repeats,
            unique_nodes,
            unique_program_bytes,
            base_program_bytes,
            bpw, original_mb, compressed_mb,
            original_mb > 0 ? original_mb / compressed_mb : 0.0f,
            tensor->avg_distortion);
    }
}
