/* BonfyreQuant encodes Safetensors weights into BQFP and measures their
 * reconstruction through the shared BQFP reader. Reported storage includes
 * headers, codebooks and blocks. Inference quality needs a workload evaluation.
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif


#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <float.h>
#include <unistd.h>
#include <sys/stat.h>
#include <bonfyre.h>
#include <bf_bqfp.h>

/* ═══════════════════════════════════════════════════════════════════
 * Constants
 * ═══════════════════════════════════════════════════════════════════ */

#define BLOCK_DIM       256
#define E8_DIM          8
#define E8_GROUPS       32      /* 256 / 8 */
#define E8_PAIRS        16      /* 256 / 16 */
#define TILE_DIM        16      /* 2 adjacent E8 groups → 16D */
#define RVQ_TILES       256     /* codebook size */
#define RVQ_ITERS       20      /* K-means iterations */
#define MU_BETA         8.0f    /* μ-law companding parameter */
#define MAX_TRAIN       8192    /* K-means subsample cap */

/* ═══════════════════════════════════════════════════════════════════
 * Walsh-Hadamard Transform
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

/* ═══════════════════════════════════════════════════════════════════
 * xorshift64 PRNG (matches BonfyreFPQ exactly)
 * ═══════════════════════════════════════════════════════════════════ */

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



/* ═══════════════════════════════════════════════════════════════════
 * E8 Lattice Snap (Conway-Sloane algorithm)
 * ═══════════════════════════════════════════════════════════════════ */

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>

static void e8_snap(const float *x, float *out) {
    /* D₈ coset: integer round, fix parity */
    float32x4_t x_lo = vld1q_f32(x);
    float32x4_t x_hi = vld1q_f32(x + 4);
    float32x4_t half = vdupq_n_f32(0.5f);

    float32x4_t r_lo = vrndnq_f32(x_lo);
    float32x4_t r_hi = vrndnq_f32(x_hi);
    int32x4_t   i_lo = vcvtq_s32_f32(r_lo);
    int32x4_t   i_hi = vcvtq_s32_f32(r_hi);

    /* Sum of 8 integers for parity */
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

    /* D₈+½ coset: half-integer, fix floor-sum parity */
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

    /* Pick closer coset */
    float d0 = 0, d1 = 0;
    for (int d = 0; d < 8; d++) {
        float e0 = x[d] - c0[d]; d0 += e0 * e0;
        float e1 = x[d] - c1[d]; d1 += e1 * e1;
    }
    const float *best = (d0 <= d1) ? c0 : c1;
    for (int d = 0; d < 8; d++) out[d] = best[d];
}

#else
/* Scalar fallback */
static void e8_snap(const float *x, float *out) {
    float c0[8], c1[8];
    int sum0 = 0;
    for (int d = 0; d < 8; d++) {
        c0[d] = roundf(x[d]);
        sum0 += (int)c0[d];
    }
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
        c1[d] = floorf(x[d]) + 0.5f;
        fsum += (int)floorf(c1[d]);
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
    const float *winner = (d0 <= d1) ? c0 : c1;
    for (int d = 0; d < 8; d++) out[d] = winner[d];
}
#endif

/* ═══════════════════════════════════════════════════════════════════
 * 16D RVQ Tile Learning (K-means with subsampling)
 * ═══════════════════════════════════════════════════════════════════ */

static inline float dist16d(const float *a, const float *b, float best) {
    float d = 0;
    for (int i = 0; i < 8; i++) { float e = a[i] - b[i]; d += e * e; }
    if (d >= best) return d;
    for (int i = 8; i < TILE_DIM; i++) { float e = a[i] - b[i]; d += e * e; }
    return d;
}

static int find_nearest_tile(const float *vec, const float *tiles, int k, int seed) {
    float best_d = FLT_MAX;
    int best = 0;
    if (seed >= 0 && seed < k) {
        best_d = dist16d(vec, tiles + seed * TILE_DIM, FLT_MAX);
        best = seed;
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

    /* Initialize from evenly-spaced data */
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
            assign[vi] = find_nearest_tile(residuals + v * TILE_DIM,
                                           tiles, k, -1);
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
 * v8 RLF Encode/Decode Block
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    double cosine;
    double rmse;
    float  bpw;
    size_t n_blocks;
    int    effective_k;
    uint64_t physical_bytes;
} RoundtripResult;

static RoundtripResult roundtrip_tensor(const float *weights, size_t total,
                                         const char *name, int bits);

/* ═══════════════════════════════════════════════════════════════════
 * CLI
 * ═══════════════════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════════════════
 * Reading the model being measured
 *
 * A .safetensors file is an 8-byte little-endian header length, a JSON header
 * naming each tensor's dtype, shape and byte range, then the bytes. One tensor
 * is read at a time so a model larger than memory can still be measured.
 * ═══════════════════════════════════════════════════════════════════ */

static float half_to_float(uint16_t h) {
    uint32_t sign = (uint32_t)(h >> 15) << 31, exp = (h >> 10) & 0x1F, man = h & 0x3FF, bits;
    float f;
    if (exp == 0) {
        if (man == 0) bits = sign;
        else {
            int shift = 0;
            while (!(man & 0x400)) { man <<= 1; shift++; }
            man &= 0x3FF;
            bits = sign | ((uint32_t)(113 - shift) << 23) | (man << 13);
        }
    } else if (exp == 31) bits = sign | 0x7F800000u | (man << 13);
    else bits = sign | ((exp + 112) << 23) | (man << 13);
    memcpy(&f, &bits, sizeof(f));
    return f;
}

static float bf16_to_float(uint16_t h) {
    uint32_t bits = (uint32_t)h << 16;
    float f;
    memcpy(&f, &bits, sizeof(f));
    return f;
}

/* One tensor's floats, or NULL when its dtype is not a float this organ reads. */
static float *read_tensor(FILE *fp, uint64_t data_start, const char *dtype,
                          uint64_t begin, uint64_t end, size_t *count) {
    size_t width = !strcmp(dtype, "F32") ? 4 : (!strcmp(dtype, "F16") || !strcmp(dtype, "BF16")) ? 2 : 0;
    if (!width || end <= begin || (end - begin) % width) return NULL;
    size_t n = (size_t)((end - begin) / width);
    float *out = (float *)malloc(n * sizeof(float));
    if (!out) return NULL;
    if (fseeko(fp, (off_t)(data_start + begin), SEEK_SET) != 0) { free(out); return NULL; }
    if (width == 4) {
        if (fread(out, 4, n, fp) != n) { free(out); return NULL; }
    } else {
        uint16_t *raw = (uint16_t *)malloc(n * 2);
        if (!raw || fread(raw, 2, n, fp) != n) { free(raw); free(out); return NULL; }
        int bf = !strcmp(dtype, "BF16");
        for (size_t i = 0; i < n; i++) out[i] = bf ? bf16_to_float(raw[i]) : half_to_float(raw[i]);
        free(raw);
    }
    *count = n;
    return out;
}

/* ═══════════════════════════════════════════════════════════════════
 * Writing a tensor as a .bqfp file
 *
 * The file's layout, its checks and its block reconstruction are defined
 * once, in bf_bqfp.h, for this organ and for the two that read what it
 * writes. What is done here is the encoding itself, with the same steps the
 * roundtrip above measures: random signs, Walsh-Hadamard, RMS scale, mu-law
 * warp, an E8 snap, and a 16-dimensional codebook learned on what the snap
 * left over.
 *
 * The lattice is snapped at spacing `bits` in the scaled domain, which is
 * the spacing the file's signed bytes can carry exactly: a byte holds twice
 * the lattice point, so the half-integer points of E8 are whole numbers.
 * ═══════════════════════════════════════════════════════════════════ */

static const char *g_bqfp_out = NULL;        /* encode: where to write */
static const char *g_compare_weights = NULL; /* compare: decoded float32 to check */
static int g_skip_tensors = 0;               /* encode, compare: eligible tensors to pass over */

static int write_bqfp(const float *weights, size_t total, const char *name, int bits, const char *out_path, int verbose) {
    size_t n_blocks = (total + BLOCK_DIM - 1) / BLOCK_DIM;
    size_t total_pairs = n_blocks * E8_PAIRS;
    float lattice_scale = bf_bqfp_lattice_scale((uint32_t)bits);
    float unit = bf_bqfp_e8_unit((uint32_t)bits);
    float spacing = (float)bits;
    uint64_t haar_seed = bf_bqfp_seed(name);
    size_t clipped = 0;

    BfBqfpBlock *blocks = (BfBqfpBlock *)calloc(n_blocks, sizeof(BfBqfpBlock));
    float *residual = (float *)calloc(total_pairs * TILE_DIM, sizeof(float));
    if (!blocks || !residual) { free(blocks); free(residual); return 1; }

    for (size_t b = 0; b < n_blocks; b++) {
        size_t off = b * BLOCK_DIM;
        size_t dim = (off + BLOCK_DIM <= total) ? BLOCK_DIM : (total - off);
        float buf[BLOCK_DIM];
        memset(buf, 0, sizeof(buf));
        memcpy(buf, weights + off, dim * sizeof(float));
        random_signs(buf, BLOCK_DIM, haar_seed ^ (uint64_t)b);
        fwht(buf, BLOCK_DIM);

        float rms = 0;
        for (int i = 0; i < BLOCK_DIM; i++) rms += buf[i] * buf[i];
        rms = sqrtf(rms / (float)BLOCK_DIM);
        if (rms < 1e-10f) rms = 1e-10f;
        float wnorm = 0;
        for (int i = 0; i < BLOCK_DIM; i++) { buf[i] = mu_warp(buf[i] / rms, MU_BETA); wnorm += buf[i] * buf[i]; }
        wnorm = sqrtf(wnorm / (float)BLOCK_DIM);
        if (wnorm < 1e-10f) wnorm = 1e-10f;
        blocks[b].scale = rms;
        blocks[b].warp_norm = wnorm;

        for (int g = 0; g < E8_GROUPS; g++) {
            float scaled[E8_DIM], in_spacing[E8_DIM], point[E8_DIM];
            for (int d = 0; d < E8_DIM; d++) {
                scaled[d] = buf[g * E8_DIM + d] / wnorm * lattice_scale;
                in_spacing[d] = scaled[d] / spacing;
            }
            e8_snap(in_spacing, point);
            for (int d = 0; d < E8_DIM; d++) {
                float twice = roundf(point[d] * 2.0f);
                /* A point outside what a byte holds is brought to the edge;
                 * the codebook then carries the difference, and the count of
                 * such coefficients is reported. */
                if (twice > 127.0f) { twice = 127.0f; clipped++; }
                if (twice < -127.0f) { twice = -127.0f; clipped++; }
                blocks[b].e8[g * E8_DIM + d] = (int8_t)twice;
                residual[b * BLOCK_DIM + (size_t)(g * E8_DIM + d)] = scaled[d] - twice * unit;
            }
        }
    }

    int ek = RVQ_TILES;
    if (total_pairs < (size_t)ek * 4) ek = (int)(total_pairs / 4);
    if (ek < 16) ek = 16;
    if (ek > BF_BQFP_MAX_TILES) ek = BF_BQFP_MAX_TILES;
    float *tiles = (float *)calloc((size_t)ek * TILE_DIM, sizeof(float));
    if (!tiles) { free(blocks); free(residual); return 1; }
    learn_tiles(residual, total_pairs, tiles, ek);
    for (size_t b = 0; b < n_blocks; b++)
        for (int p = 0; p < E8_PAIRS; p++)
            blocks[b].tile[p] = (uint8_t)find_nearest_tile(residual + (b * E8_PAIRS + (size_t)p) * TILE_DIM, tiles, ek, -1);

    int rc = bf_bqfp_write_one(out_path, name, (uint32_t)bits, (uint64_t)total, tiles, (uint32_t)ek,
                               blocks, (uint64_t)n_blocks);
    free(blocks); free(residual); free(tiles);
    if (rc != 0) { fprintf(stderr, "bonfyre-quant: cannot write %s\n", out_path); return 1; }
    if (verbose) printf("encoded %s: %zu weights, %zu blocks, %d tiles, %d bits, %zu coefficients at the byte's edge -> %s\n",
           name, total, n_blocks, ek, bits, clipped, out_path);
    return 0;
}

/* Benchmark the bytes readers actually consume, including file overhead. */
static RoundtripResult roundtrip_tensor(const float *weights, size_t total,
                                        const char *name, int bits) {
    RoundtripResult res = {.cosine = NAN};
    char path[] = "/tmp/bonfyre-quant-roundtrip-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return res;
    close(fd);
    if (write_bqfp(weights, total, name, bits, path, 0) != 0) { unlink(path); return res; }
    BfBqfpTensor tensor;
    uint32_t stored_bits;
    float *tiles = NULL, *decoded = NULL;
    const char *why = NULL;
    struct stat statbuf;
    if (stat(path, &statbuf) != 0 ||
        bf_bqfp_load_first(path, &tensor, &stored_bits, &tiles, &decoded, &why) != 0) {
        fprintf(stderr, "bonfyre-quant: roundtrip read failed: %s\n", why ? why : "stat failed");
        unlink(path); return res;
    }
    unlink(path);
    double dot = 0, a2 = 0, b2 = 0, mse = 0;
    for (size_t i = 0; i < total; i++) {
        double a = weights[i], b = decoded[i], e = a - b;
        dot += a*b; a2 += a*a; b2 += b*b; mse += e*e;
    }
    res.cosine = (a2 == 0 && b2 == 0) ? 1 : dot / (sqrt(a2)*sqrt(b2) + 1e-20);
    res.rmse = sqrt(mse / (double)total);
    res.bpw = (float)((double)statbuf.st_size * 8.0 / (double)total);
    res.physical_bytes = (uint64_t)statbuf.st_size;
    res.n_blocks = (size_t)tensor.blocks;
    res.effective_k = (int)tensor.tiles;
    free(tiles); free(decoded);
    return res;
}

/* Compares a tensor with float32 values somebody else decoded for it. */
static int compare_weights(const float *weights, size_t total, const char *name, const char *path) {
    FILE *in = fopen(path, "rb");
    float *other = (float *)malloc(total * sizeof(float));
    if (!in || !other || fread(other, sizeof(float), total, in) != total) {
        fprintf(stderr, "bonfyre-quant: %s does not hold %zu float32 values for %s\n", path, total, name);
        if (in) fclose(in);
        free(other);
        return 1;
    }
    int extra = fgetc(in) != EOF;
    fclose(in);
    if (extra) { fprintf(stderr, "bonfyre-quant: %s holds more than %zu values\n", path, total); free(other); return 1; }
    double dot = 0, a2 = 0, b2 = 0, mse = 0;
    for (size_t i = 0; i < total; i++) {
        double a = weights[i], b = other[i];
        dot += a * b; a2 += a * a; b2 += b * b; mse += (a - b) * (a - b);
    }
    free(other);
    double cosine = (a2 == 0 && b2 == 0) ? 1 : dot / (sqrt(a2) * sqrt(b2) + 1e-20);
    printf("compared %s: %zu weights  cos=%.6f  rmse=%.6f\n", name, total, cosine, sqrt(mse / (double)total));
    return cosine >= 0.99 ? 0 : 2;
}

static int measure_model(const char *path, int bits, int limit, const char *filter) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { fprintf(stderr, "bonfyre-quant: cannot open %s\n", path); return 1; }
    uint8_t lenb[8];
    uint64_t hlen = 0;
    if (fread(lenb, 1, 8, fp) != 8) { fclose(fp); fprintf(stderr, "bonfyre-quant: %s is not a safetensors file\n", path); return 1; }
    for (int i = 7; i >= 0; i--) hlen = (hlen << 8) | lenb[i];
    if (hlen < 2 || hlen > (uint64_t)256 * 1024 * 1024) { fclose(fp); fprintf(stderr, "bonfyre-quant: %s is not a safetensors file\n", path); return 1; }
    char *header = (char *)malloc((size_t)hlen + 1);
    if (!header || fread(header, 1, (size_t)hlen, fp) != (size_t)hlen) {
        free(header); fclose(fp);
        fprintf(stderr, "bonfyre-quant: %s has a truncated header\n", path); return 1;
    }
    header[hlen] = '\0';
    if (header[0] != '{') { free(header); fclose(fp); fprintf(stderr, "bonfyre-quant: %s is not a safetensors file\n", path); return 1; }
    uint64_t data_start = 8 + hlen;

    if (!g_bqfp_out && !g_compare_weights) {
    printf("═══════════════════════════════════════════════════════\n");
    printf(" bonfyre-quant v8 RLF — Roundtrip@%d\n", bits);
    printf(" Model: %s\n", path);
    printf("═══════════════════════════════════════════════════════\n\n");
    }

    int measured = 0, skipped = 0, below = 0;
    double weighted_cos = 0.0, total_bits = 0.0, min_cos = 2.0;
    size_t total_elems = 0;
    uint64_t physical_bytes = 0;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Each tensor entry is "name":{..."dtype":"X"..."data_offsets":[a,b]...}. */
    const char *p = header + 1;
    while (*p) {
        const char *q = strchr(p, '"');
        if (!q) break;
        const char *name_end = q + 1;
        while (*name_end && *name_end != '"') { if (*name_end == '\\' && name_end[1]) name_end++; name_end++; }
        if (!*name_end) break;
        const char *open = name_end + 1;
        while (*open == ' ' || *open == ':') open++;
        if (*open != '{') { p = name_end + 1; continue; }
        int depth = 0;
        const char *close = open;
        for (; *close; close++) {
            if (*close == '{') depth++;
            else if (*close == '}' && --depth == 0) break;
        }
        if (!*close) break;

        char name[256];
        size_t name_len = (size_t)(name_end - q - 1);
        if (name_len >= sizeof(name)) name_len = sizeof(name) - 1;
        memcpy(name, q + 1, name_len);
        name[name_len] = '\0';
        p = close + 1;
        if (!strcmp(name, "__metadata__")) continue;
        if (filter && !strstr(name, filter)) continue;

        char dtype[16] = "";
        const char *d = strstr(open, "\"dtype\"");
        if (d && d < close) {
            d = strchr(d + 7, '"');
            if (d && d < close) {
                size_t i = 0;
                for (d++; *d && *d != '"' && i + 1 < sizeof(dtype); d++) dtype[i++] = *d;
                dtype[i] = '\0';
            }
        }
        uint64_t begin = 0, end = 0;
        const char *o = strstr(open, "\"data_offsets\"");
        if (!o || o > close || sscanf(strchr(o, '['), "[%llu ,%llu", (unsigned long long *)&begin, (unsigned long long *)&end) != 2) {
            skipped++; continue;
        }
        size_t n = 0;
        float *w = read_tensor(fp, data_start, dtype, begin, end, &n);
        if (!w) { skipped++; continue; }
        if (n < BLOCK_DIM) { free(w); skipped++; continue; }

        if (g_bqfp_out || g_compare_weights) { /* --skip N passes over the first N eligible tensors, so a second * tensor of the same model can be named without knowing its name. */ if (g_skip_tensors > 0) { g_skip_tensors--; free(w); continue; }
            int rc = g_bqfp_out ? write_bqfp(w, n, name, bits, g_bqfp_out, 1)
                                : compare_weights(w, n, name, g_compare_weights);
            free(w); free(header); fclose(fp);
            return rc;
        }

        RoundtripResult r = roundtrip_tensor(w, n, name, bits);
        free(w);
        if (!isfinite(r.cosine)) { free(header); fclose(fp); return 1; }
        printf("  %-56.56s %10zu  cos=%.6f  rmse=%.6f  bpw=%.2f\n", name, n, r.cosine, r.rmse, r.bpw);
        weighted_cos += (double)r.cosine * (double)n;
        total_bits += (double)r.bpw * (double)n;
        physical_bytes += r.physical_bytes;
        total_elems += n;
        if (r.cosine < min_cos) min_cos = r.cosine;
        if (r.cosine < 0.999) below++;
        measured++;
        if (limit > 0 && measured >= limit) break;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    free(header);
    fclose(fp);

    if (measured == 0) {
        fprintf(stderr, "bonfyre-quant: no float tensor of at least %d elements%s%s in %s (%d skipped)\n",
                BLOCK_DIM, filter ? " matching " : "", filter ? filter : "", path, skipped);
        return 1;
    }
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    printf("\n  tensors measured: %d (%d skipped)   elements: %zu\n", measured, skipped, total_elems);
    printf("  cosine: mean %.6f (element-weighted), lowest %.6f\n", weighted_cos / (double)total_elems, min_cos);
    printf("  bits per weight: %.3f (physical BQFP bytes, including headers)\n", total_bits / (double)total_elems);
    printf("  saved tensor bytes: %llu (%.3fx smaller than float32)\n",
           (unsigned long long)physical_bytes, (double)total_elems * 4.0 / (double)physical_bytes);
    printf("  time: %.2f s (%.2f M elements/s)\n", secs, secs > 0 ? (double)total_elems / secs / 1e6 : 0.0);
    printf("  tensors below 0.999 cosine: %d\n", below);
    printf("\n═══════════════════════════════════════════════════════\n");
    return below == 0 ? 0 : 2;
}

static void usage(void) {
    fprintf(stderr,
        "bonfyre-quant — v8 Recursive Lattice-Flow weight quantization\n"
        "\n"
        "Usage:\n"
        "  bonfyre-quant roundtrip MODEL.safetensors [--bits N] [--limit N] [--tensor NAME]\n"
        "  bonfyre-quant benchmark MODEL.safetensors [--bits N] [--limit N] [--tensor NAME]\n"
        "  bonfyre-quant encode    MODEL.safetensors --out FILE.bqfp [--tensor NAME] [--skip N] [--bits N]\n"
        "  bonfyre-quant compare   MODEL.safetensors --weights FILE.f32 [--tensor NAME] [--skip N]\n"
        "  bonfyre-quant --help\n"
        "\n"
        "BQFP: E8 lattice + μ-law warp + 16D RVQ; roundtrip measures saved files\n"
        "\n"
        "Options:\n"
        "  --bits N     Quantization bits (2, 3, 4; default: 3)\n"
        "  --limit N    Process only first N eligible tensors\n"
        "  --tensor S   Process only tensor matching S\n"
    );
}

static void print_version(void) {
    printf("bonfyre-quant v8.0.0 (Recursive Lattice-Flow)\n"
           "  E8 lattice snap + μ-law warp (β=8) + 256-tile 16D RVQ\n"
           "  ARM NEON vectorized on Apple Silicon\n");
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }

    const char *cmd = argv[1];
    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) {
        usage(); return 0;
    }
    if (strcmp(cmd, "--version") == 0) {
        print_version(); return 0;
    }

    /* Parse options */
    int bits = 3;
    int limit = 0;
    const char *tensor_filter = NULL;

    const char *v;
    if ((v = bf_arg_value(argc, argv, "--bits")))   bits = atoi(v);
    if ((v = bf_arg_value(argc, argv, "--limit")))  limit = atoi(v);
    tensor_filter = bf_arg_value(argc, argv, "--tensor");

    if (strcmp(cmd, "roundtrip") == 0 || strcmp(cmd, "benchmark") == 0) {
        const char *model = (argc >= 3 && argv[2][0] != '-') ? argv[2] : NULL;
        if (!model) {
            fprintf(stderr, "bonfyre-quant %s: a model file is required. "
                            "There is no built-in tensor to measure.\n", cmd);
            return 1;
        }
        return measure_model(model, bits, limit, tensor_filter);
    }

    if (strcmp(cmd, "encode") == 0 || strcmp(cmd, "compare") == 0) {
        const char *model = (argc >= 3 && argv[2][0] != '-') ? argv[2] : NULL;
        const char *target = bf_arg_value(argc, argv, strcmp(cmd, "encode") == 0 ? "--out" : "--weights");
        if (!model || !target) {
            fprintf(stderr, "usage: bonfyre-quant encode MODEL.safetensors --out FILE.bqfp [--tensor NAME] [--bits N]\n"
                            "       bonfyre-quant compare MODEL.safetensors --weights FILE.f32 [--tensor NAME]\n");
            return 1;
        }
        if (strcmp(cmd, "encode") == 0) g_bqfp_out = target; else g_compare_weights = target; if ((v = bf_arg_value(argc, argv, "--skip"))) g_skip_tensors = atoi(v);
        return measure_model(model, bits, 1, tensor_filter);
    }

    fprintf(stderr, "Unknown command: %s\n", cmd);
    usage();
    return 1;
}
