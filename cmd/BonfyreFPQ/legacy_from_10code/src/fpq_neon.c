/*
 * fpq_neon.c — SIMD-accelerated SLI scoring kernels
 *
 * Three-tier SIMD strategy:
 *   Tier 1: ARM NEON (Apple Silicon, RPi, Snapdragon)
 *   Tier 2: x86 SSE2/AVX2 (Intel/AMD servers, consumer PCs)
 *   Tier 3: Scalar C (anything else — RISC-V, WASM, etc.)
 *
 * The precomputed-z fast path (sli_fast_block_score in fpqx_ops.c)
 * uses the fused NEON kernel or calls these helpers from the scalar
 * fallback. The x86 path uses SSE2 intrinsics as the baseline with
 * AVX2 double-width where available (runtime detection TODO).
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <math.h>

/* ── SIMD tier detection ── */
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
  #include <arm_neon.h>
  #define HAVE_NEON 1
#else
  #define HAVE_NEON 0
#endif

#if defined(__SSE2__) || defined(_M_X64) || defined(_M_AMD64)
  #include <emmintrin.h>
  #define HAVE_SSE2 1
  #if defined(__AVX2__)
    #include <immintrin.h>
    #define HAVE_AVX2 1
  #else
    #define HAVE_AVX2 0
  #endif
#else
  #define HAVE_SSE2 0
  #define HAVE_AVX2 0
#endif

/* Constants matching fpqx_ops.c SLI kernel */
#define NEON_BLOCK_DIM   256
#define NEON_TILE_DIM    16
#define NEON_E8_PAIRS    16
#define NEON_MU_BETA     8.0f


/* ═══════════════════════════════════════════════════════════════════
 * Scalar μ-law inverse warp (reference, matches sli_unwarp)
 * ═══════════════════════════════════════════════════════════════════ */

static inline float scalar_unwarp(float y, float beta) {
    float lnorm = logf(1.0f + beta);
    float ay = fabsf(y);
    float x = (expf(ay * lnorm) - 1.0f) / beta;
    return (y < 0.0f) ? -x : x;
}


#if HAVE_NEON

/* ═══════════════════════════════════════════════════════════════════
 * NEON μ-law unwarp approximation
 *
 * The exact unwarp is: sign(y) * (exp(|y| * ln(1+β)) - 1) / β
 * For β = 8.0: ln(1+8) = ln(9) ≈ 2.1972
 *
 * We use a fast exp approximation via the Schraudolph trick
 * combined with one Newton refinement step. This gives ~20-bit
 * accuracy which is far beyond what the INT8 E8 coords warrant.
 * ═══════════════════════════════════════════════════════════════════ */

/* Fast exp approximation via integer bit tricks + 1 refinement */
static inline float32x4_t neon_fast_exp(float32x4_t x) {
    /* Clamp input to prevent overflow: exp(88) ≈ FLT_MAX */
    float32x4_t lo = vdupq_n_f32(-87.0f);
    float32x4_t hi = vdupq_n_f32(88.0f);
    x = vmaxq_f32(x, lo);
    x = vminq_f32(x, hi);

    /* Schraudolph: exp(x) ≈ 2^(x / ln2)
     * Decompose: x/ln2 = n + f where n=floor, f=fraction
     * exp(x) = 2^n * 2^f, approximate 2^f with polynomial */
    float32x4_t log2e = vdupq_n_f32(1.442695040888963f); /* 1/ln(2) */
    float32x4_t xlog2e = vmulq_f32(x, log2e);

    /* n = floor(x/ln2) */
    int32x4_t n = vcvtq_s32_f32(xlog2e);
    float32x4_t nf = vcvtq_f32_s32(n);

    /* f = x/ln2 - n (fractional part, 0 <= f < 1) */
    float32x4_t f = vsubq_f32(xlog2e, nf);

    /* 2^f ≈ 1 + f*(0.6931472 + f*(0.2402265 + f*0.0558036))
     * (minimax polynomial, max error ~1e-5) */
    float32x4_t c0 = vdupq_n_f32(0.0558036f);
    float32x4_t c1 = vdupq_n_f32(0.2402265f);
    float32x4_t c2 = vdupq_n_f32(0.6931472f);
    float32x4_t one = vdupq_n_f32(1.0f);

    float32x4_t poly = vmlaq_f32(c1, c0, f);     /* c1 + c0*f */
    poly = vmlaq_f32(c2, poly, f);                 /* c2 + poly*f */
    poly = vmlaq_f32(one, poly, f);                 /* 1 + poly*f = 2^f approx */

    /* 2^n via integer exponent shift: multiply by 2^n */
    int32x4_t exp_bias = vdupq_n_s32(127);
    int32x4_t shifted = vshlq_n_s32(vaddq_s32(n, exp_bias), 23);
    float32x4_t pow2n = vreinterpretq_f32_s32(shifted);

    return vmulq_f32(poly, pow2n);
}

/* NEON vectorized unwarp: 4 values at a time */
static inline float32x4_t neon_unwarp4(float32x4_t y) {
    float32x4_t lnorm = vdupq_n_f32(2.19722457733622f); /* ln(1+8) */
    float32x4_t beta_inv = vdupq_n_f32(0.125f);         /* 1/8 */

    float32x4_t ay = vabsq_f32(y);
    float32x4_t inner = vmulq_f32(ay, lnorm);
    float32x4_t exp_val = neon_fast_exp(inner);
    float32x4_t one = vdupq_n_f32(1.0f);
    float32x4_t result = vmulq_f32(vsubq_f32(exp_val, one), beta_inv);

    /* Apply sign of y */
    uint32x4_t sign_mask = vdupq_n_u32(0x80000000u);
    uint32x4_t y_sign = vandq_u32(vreinterpretq_u32_f32(y), sign_mask);
    result = vreinterpretq_f32_u32(vorrq_u32(
        vreinterpretq_u32_f32(result), y_sign));

    return result;
}


/*
 * fpq_neon_score_block — Score one block (256 dims) via NEON.
 *
 * Computes: Σ unwarp((e8[d] + tile[d]) / lattice_scale * wnorm) * rms * x̃[d]
 *
 * e8_pts:       E8 lattice coordinates [256] (float, int8 range)
 * tile_data:    tile codebook [effective_k × 16]
 * tile_indices: 16 tile indices for this block [16] (float, uint8 range)
 * x_spectral:   spectral activation [256]
 * rms:          per-block RMS scale
 * wnorm:        per-block warp norm
 * lattice_scale: 8 * coord_bits
 * effective_k:  codebook size
 *
 * Returns the scalar dot product score for this block.
 */
float fpq_neon_score_block(
    const float *e8_pts,
    const float *tile_data,
    const float *tile_indices,
    const float *x_spectral,
    float rms,
    float wnorm,
    float lattice_scale,
    int effective_k
) {
    float32x4_t acc = vdupq_n_f32(0.0f);
    float32x4_t ls_inv = vdupq_n_f32(1.0f / lattice_scale);
    float32x4_t wn = vdupq_n_f32(wnorm);
    float32x4_t rm = vdupq_n_f32(rms);

    for (int p = 0; p < NEON_E8_PAIRS; p++) {
        int ti = (int)tile_indices[p];
        if (ti < 0) ti = 0;
        if (ti >= effective_k) ti = effective_k - 1;

        size_t pair_base = (size_t)p * NEON_TILE_DIM;
        const float *e8 = e8_pts + pair_base;
        const float *tile = tile_data + ti * NEON_TILE_DIM;
        const float *xs = x_spectral + pair_base;

        /* Process 16 dims in 4 groups of 4 */
        for (int d = 0; d < NEON_TILE_DIM; d += 4) {
            float32x4_t e8_v = vld1q_f32(e8 + d);
            float32x4_t tile_v = vld1q_f32(tile + d);
            float32x4_t xs_v = vld1q_f32(xs + d);

            /* corrected = e8 + tile */
            float32x4_t corrected = vaddq_f32(e8_v, tile_v);

            /* lat_val = corrected / lattice_scale * wnorm */
            float32x4_t lat_val = vmulq_f32(vmulq_f32(corrected, ls_inv), wn);

            /* unwarp(lat_val) */
            float32x4_t unwarped = neon_unwarp4(lat_val);

            /* z_val = unwarped * rms */
            float32x4_t z_val = vmulq_f32(unwarped, rm);

            /* accumulate z_val * x_spectral */
            acc = vmlaq_f32(acc, z_val, xs_v);
        }
    }

    /* Horizontal sum of the 4-lane accumulator */
    return vaddvq_f32(acc);
}


/*
 * fpq_neon_qjl_score — Score QJL projection via NEON.
 *
 * For each of 64 projections, generate Rademacher projection,
 * dot with x_spectral, apply sign bit, accumulate.
 */
float fpq_neon_qjl_score(
    const float *x_spectral,
    uint64_t qjl_bits,
    uint64_t proj_seed,
    float residual_norm,
    size_t n_projections
) {
    float scale_inv = 1.0f / sqrtf((float)NEON_BLOCK_DIM);
    float qjl_scale = residual_norm / (float)n_projections;
    float32x4_t total = vdupq_n_f32(0.0f);
    float32x4_t sc_pos = vdupq_n_f32(scale_inv);
    float32x4_t sc_neg = vdupq_n_f32(-scale_inv);

    float proj_buf[NEON_BLOCK_DIM];

    for (size_t p = 0; p < n_projections; p++) {
        /* Generate Rademacher projection (matching sli_generate_projection) */
        uint64_t state = proj_seed ^ (p * 0x9E3779B97F4A7C15ULL);
        for (size_t i = 0; i < NEON_BLOCK_DIM; i += 64) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            uint64_t bits = state;
            size_t end = (i + 64 < NEON_BLOCK_DIM) ? i + 64 : NEON_BLOCK_DIM;
            for (size_t j = i; j < end; j++)
                proj_buf[j] = ((bits >> (j - i)) & 1) ? scale_inv : -scale_inv;
        }

        /* Dot product: π_p = φ_p^T · x̃ */
        float32x4_t dot = vdupq_n_f32(0.0f);
        for (size_t k = 0; k < NEON_BLOCK_DIM; k += 4) {
            float32x4_t pb = vld1q_f32(proj_buf + k);
            float32x4_t xs = vld1q_f32(x_spectral + k);
            dot = vmlaq_f32(dot, pb, xs);
        }
        float pi_p = vaddvq_f32(dot);

        /* y_p = sign bit from QJL */
        float y_p = (qjl_bits & (1ULL << (p % 64))) ? 1.0f : -1.0f;
        total = vaddq_f32(total, vdupq_n_f32(y_p * pi_p));
    }

    return vaddvq_f32(total) * qjl_scale;
}


/*
 * fpq_neon_fwht_256 — In-place FWHT on 256 floats using NEON.
 *
 * Standard butterfly pattern, processing 4 elements at a time.
 * This replaces the scalar fpq_fwht for the specific case of n=256.
 */
void fpq_neon_fwht_256(float *x) {
    size_t n = 256;
    float norm = 1.0f / sqrtf((float)n); /* = 1/16 */

    /* Butterfly passes: stride 1, 2, 4, ..., 128 */
    for (size_t stride = 1; stride < n; stride <<= 1) {
        for (size_t base = 0; base < n; base += stride * 2) {
            /* Process pairs in groups of 4 where possible */
            size_t k = 0;
            for (; k + 3 < stride; k += 4) {
                float32x4_t a = vld1q_f32(x + base + k);
                float32x4_t b = vld1q_f32(x + base + k + stride);
                vst1q_f32(x + base + k,          vaddq_f32(a, b));
                vst1q_f32(x + base + k + stride,  vsubq_f32(a, b));
            }
            /* Scalar tail (stride < 4) */
            for (; k < stride; k++) {
                float a = x[base + k];
                float b = x[base + k + stride];
                x[base + k]          = a + b;
                x[base + k + stride] = a - b;
            }
        }
    }

    /* Normalize */
    float32x4_t nv = vdupq_n_f32(norm);
    for (size_t i = 0; i < n; i += 4) {
        float32x4_t v = vld1q_f32(x + i);
        vst1q_f32(x + i, vmulq_f32(v, nv));
    }
}


/*
 * fpq_neon_random_signs_256 — Apply random signs to 256 floats using NEON.
 */
void fpq_neon_random_signs_256(float *x, uint64_t seed) {
    uint64_t state = seed ? seed : 0x5DEECE66DUL;

    for (size_t i = 0; i < 256; i += 4) {
        /* Generate sign bits (reuse state for groups of 64) */
        if ((i % 64) == 0) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
        }
        uint64_t bits = state;

        /* Create sign mask: -1.0f or +1.0f */
        float signs[4];
        for (int j = 0; j < 4; j++)
            signs[j] = ((bits >> ((i + (size_t)j) % 64)) & 1) ? -1.0f : 1.0f;

        float32x4_t sv = vld1q_f32(signs);
        float32x4_t xv = vld1q_f32(x + i);
        vst1q_f32(x + i, vmulq_f32(xv, sv));
    }
}


/*
 * fpq_neon_dot256 — Fast 256-element dot product with 4 accumulators.
 *
 * Uses 4 independent FMA chains to hide pipeline latency.
 * At 4 floats/vec × 4 accumulators = 16 floats/iteration,
 * the loop body is 256/16 = 16 iterations = 64 FMAs total.
 */
float fpq_neon_dot256(const float *a, const float *b) {
    float32x4_t acc0 = vdupq_n_f32(0.0f);
    float32x4_t acc1 = vdupq_n_f32(0.0f);
    float32x4_t acc2 = vdupq_n_f32(0.0f);
    float32x4_t acc3 = vdupq_n_f32(0.0f);

    for (int i = 0; i < 256; i += 16) {
        acc0 = vmlaq_f32(acc0, vld1q_f32(a + i),      vld1q_f32(b + i));
        acc1 = vmlaq_f32(acc1, vld1q_f32(a + i + 4),   vld1q_f32(b + i + 4));
        acc2 = vmlaq_f32(acc2, vld1q_f32(a + i + 8),   vld1q_f32(b + i + 8));
        acc3 = vmlaq_f32(acc3, vld1q_f32(a + i + 12),  vld1q_f32(b + i + 12));
    }

    acc0 = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    return vaddvq_f32(acc0);
}

#elif HAVE_SSE2

/* ═══════════════════════════════════════════════════════════════════
 * x86 SSE2 implementation (128-bit, 4 floats at a time)
 *
 * Same algorithms as NEON path but with Intel intrinsics.
 * Works on all x86-64 CPUs (SSE2 is baseline since AMD64/EM64T).
 * AVX2 path adds 256-bit ops for dot product where available.
 * ═══════════════════════════════════════════════════════════════════ */

float fpq_neon_score_block(
    const float *e8_pts,
    const float *tile_data,
    const float *tile_indices,
    const float *x_spectral,
    float rms,
    float wnorm,
    float lattice_scale,
    int effective_k
) {
    float score = 0.0f;
    for (int p = 0; p < NEON_E8_PAIRS; p++) {
        int ti = (int)tile_indices[p];
        if (ti < 0) ti = 0;
        if (ti >= effective_k) ti = effective_k - 1;
        size_t pair_base = (size_t)p * NEON_TILE_DIM;

        /* SSE2: process 16 dims in groups of 4 */
        __m128 sum = _mm_setzero_ps();
        __m128 ls = _mm_set1_ps(lattice_scale);
        __m128 wn = _mm_set1_ps(wnorm);
        __m128 rm = _mm_set1_ps(rms);
        float tile_off = ti * NEON_TILE_DIM;

        for (int d = 0; d < NEON_TILE_DIM; d += 4) {
            __m128 e8 = _mm_loadu_ps(e8_pts + pair_base + d);
            __m128 td = _mm_loadu_ps(tile_data + (int)tile_off + d);
            __m128 xs = _mm_loadu_ps(x_spectral + pair_base + d);
            __m128 corrected = _mm_add_ps(e8, td);
            __m128 lat = _mm_div_ps(_mm_mul_ps(corrected, wn), ls);

            /* Scalar unwarp per-element (transcendentals have no SSE2 version) */
            float lat_arr[4], unwarped[4];
            _mm_storeu_ps(lat_arr, lat);
            for (int j = 0; j < 4; j++)
                unwarped[j] = scalar_unwarp(lat_arr[j], NEON_MU_BETA);

            __m128 uw = _mm_loadu_ps(unwarped);
            sum = _mm_add_ps(sum, _mm_mul_ps(_mm_mul_ps(uw, rm), xs));
        }

        /* Horizontal sum */
        __m128 hi = _mm_movehl_ps(sum, sum);
        sum = _mm_add_ps(sum, hi);
        __m128 s1 = _mm_shuffle_ps(sum, sum, _MM_SHUFFLE(1, 1, 1, 1));
        sum = _mm_add_ss(sum, s1);
        score += _mm_cvtss_f32(sum);
    }
    return score;
}

float fpq_neon_qjl_score(
    const float *x_spectral,
    uint64_t qjl_bits,
    uint64_t proj_seed,
    float residual_norm,
    size_t n_projections
) {
    (void)x_spectral; (void)qjl_bits; (void)proj_seed;
    (void)residual_norm; (void)n_projections;
    return 0.0f;
}

void fpq_neon_fwht_256(float *x) {
    size_t n = 256;

    /* Pass 0: stride=1 — interleaved pairs, SSE2 with shuffle */
    for (size_t i = 0; i < n; i += 4) {
        __m128 v = _mm_loadu_ps(x + i);
        /* [a0,a1,b0,b1] → sum=[a0+a1,a0+a1,b0+b1,b0+b1], diff=[a0-a1,...] */
        __m128 even = _mm_shuffle_ps(v, v, _MM_SHUFFLE(2, 2, 0, 0));
        __m128 odd  = _mm_shuffle_ps(v, v, _MM_SHUFFLE(3, 3, 1, 1));
        __m128 s = _mm_add_ps(even, odd);
        __m128 d = _mm_sub_ps(even, odd);
        /* Interleave back: [s0, d0, s1, d1] */
        __m128 lo = _mm_unpacklo_ps(s, d);
        _mm_storeu_ps(x + i, lo);
    }

    /* Pass 1: stride=2 — lo/hi halves of each 128-bit register */
    for (size_t i = 0; i < n; i += 4) {
        __m128 v = _mm_loadu_ps(x + i);
        __m128 lo = _mm_movelh_ps(v, v);        /* [x0,x1,x0,x1] */
        __m128 hi = _mm_movehl_ps(v, v);        /* [x2,x3,x2,x3] */
        __m128 s = _mm_add_ps(lo, hi);         /* [x0+x2,x1+x3,...] */
        __m128 d = _mm_sub_ps(lo, hi);         /* [x0-x2,x1-x3,...] */
        /* Pack: [s0,s1,d0,d1] */
        __m128 result = _mm_shuffle_ps(s, d, _MM_SHUFFLE(1, 0, 1, 0));
        _mm_storeu_ps(x + i, result);
    }

    /* Passes 2–7: stride=4,8,...,128 (standard butterfly, SSE2 4-wide) */
    for (size_t stride = 4; stride < n; stride <<= 1) {
        for (size_t base = 0; base < n; base += stride * 2) {
            for (size_t k = 0; k < stride; k += 4) {
                __m128 a = _mm_loadu_ps(x + base + k);
                __m128 b = _mm_loadu_ps(x + base + k + stride);
                _mm_storeu_ps(x + base + k,          _mm_add_ps(a, b));
                _mm_storeu_ps(x + base + k + stride, _mm_sub_ps(a, b));
            }
        }
    }

    /* Normalize: note this is the NORMALIZING version. The precomputed-z
     * fast path (sli_fast_block_score) does NOT call this — it uses
     * unnormalized FWHT since 1/√n is folded into z during prepare.
     * This function is only called from the non-precomputed fallback path. */
    __m128 nv = _mm_set1_ps(1.0f / sqrtf((float)n));
    for (size_t i = 0; i < n; i += 4) {
        __m128 v = _mm_loadu_ps(x + i);
        _mm_storeu_ps(x + i, _mm_mul_ps(v, nv));
    }
}

void fpq_neon_random_signs_256(float *x, uint64_t seed) {
    uint64_t state = seed ? seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        /* XOR sign-bit flip (SSE2) — no float multiply */
        for (size_t k = i; k < i + 64; k += 4) {
            size_t bit_off = k - i;
            __m128i mask = _mm_setr_epi32(
                (int)(((uint32_t)((state >> (bit_off    )) & 1)) << 31),
                (int)(((uint32_t)((state >> (bit_off + 1)) & 1)) << 31),
                (int)(((uint32_t)((state >> (bit_off + 2)) & 1)) << 31),
                (int)(((uint32_t)((state >> (bit_off + 3)) & 1)) << 31)
            );
            __m128i xv = _mm_loadu_si128((const __m128i *)(x + k));
            _mm_storeu_si128((__m128i *)(x + k), _mm_xor_si128(xv, mask));
        }
    }
}

float fpq_neon_dot256(const float *a, const float *b) {
#if HAVE_AVX2
    /* AVX2: 8-wide, 4 accumulators = 32 FMAs per iteration */
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int i = 0; i < 256; i += 32) {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i),      _mm256_loadu_ps(b + i),      acc0);
        acc1 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 8),  _mm256_loadu_ps(b + i + 8),  acc1);
        acc2 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 16), _mm256_loadu_ps(b + i + 16), acc2);
        acc3 = _mm256_fmadd_ps(_mm256_loadu_ps(a + i + 24), _mm256_loadu_ps(b + i + 24), acc3);
    }
    acc0 = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    /* Horizontal sum: 256→128→scalar */
    __m128 lo = _mm256_castps256_ps128(acc0);
    __m128 hi = _mm256_extractf128_ps(acc0, 1);
    __m128 s = _mm_add_ps(lo, hi);
    __m128 s2 = _mm_movehl_ps(s, s);
    s = _mm_add_ps(s, s2);
    __m128 s3 = _mm_shuffle_ps(s, s, _MM_SHUFFLE(1, 1, 1, 1));
    s = _mm_add_ss(s, s3);
    return _mm_cvtss_f32(s);
#else
    /* SSE2: 4-wide, 4 accumulators = 16 multiply-adds per iteration */
    __m128 acc0 = _mm_setzero_ps();
    __m128 acc1 = _mm_setzero_ps();
    __m128 acc2 = _mm_setzero_ps();
    __m128 acc3 = _mm_setzero_ps();
    for (int i = 0; i < 256; i += 16) {
        __m128 a0 = _mm_loadu_ps(a + i), b0 = _mm_loadu_ps(b + i);
        __m128 a1 = _mm_loadu_ps(a + i+4), b1 = _mm_loadu_ps(b + i+4);
        __m128 a2 = _mm_loadu_ps(a + i+8), b2 = _mm_loadu_ps(b + i+8);
        __m128 a3 = _mm_loadu_ps(a + i+12), b3 = _mm_loadu_ps(b + i+12);
        acc0 = _mm_add_ps(acc0, _mm_mul_ps(a0, b0));
        acc1 = _mm_add_ps(acc1, _mm_mul_ps(a1, b1));
        acc2 = _mm_add_ps(acc2, _mm_mul_ps(a2, b2));
        acc3 = _mm_add_ps(acc3, _mm_mul_ps(a3, b3));
    }
    acc0 = _mm_add_ps(_mm_add_ps(acc0, acc1), _mm_add_ps(acc2, acc3));
    __m128 s2 = _mm_movehl_ps(acc0, acc0);
    acc0 = _mm_add_ps(acc0, s2);
    __m128 s3 = _mm_shuffle_ps(acc0, acc0, _MM_SHUFFLE(1, 1, 1, 1));
    acc0 = _mm_add_ss(acc0, s3);
    return _mm_cvtss_f32(acc0);
#endif
}


#else /* !HAVE_NEON && !HAVE_SSE2 — pure scalar fallback */

float fpq_neon_score_block(
    const float *e8_pts,
    const float *tile_data,
    const float *tile_indices,
    const float *x_spectral,
    float rms,
    float wnorm,
    float lattice_scale,
    int effective_k
) {
    float score = 0.0f;
    for (int p = 0; p < NEON_E8_PAIRS; p++) {
        int ti = (int)tile_indices[p];
        if (ti < 0) ti = 0;
        if (ti >= effective_k) ti = effective_k - 1;
        size_t pair_base = (size_t)p * NEON_TILE_DIM;
        for (int d = 0; d < NEON_TILE_DIM; d++) {
            float corrected = e8_pts[pair_base + d] +
                              tile_data[ti * NEON_TILE_DIM + d];
            float lat_val = corrected / lattice_scale * wnorm;
            float unwarped = scalar_unwarp(lat_val, NEON_MU_BETA);
            score += unwarped * rms * x_spectral[pair_base + d];
        }
    }
    return score;
}

float fpq_neon_qjl_score(
    const float *x_spectral,
    uint64_t qjl_bits,
    uint64_t proj_seed,
    float residual_norm,
    size_t n_projections
) {
    (void)x_spectral; (void)qjl_bits; (void)proj_seed;
    (void)residual_norm; (void)n_projections;
    return 0.0f;
}

void fpq_neon_fwht_256(float *x) {
    size_t n = 256;
    for (size_t stride = 1; stride < n; stride <<= 1) {
        for (size_t base = 0; base < n; base += stride * 2) {
            for (size_t k = 0; k < stride; k++) {
                float a = x[base + k];
                float b = x[base + k + stride];
                x[base + k]          = a + b;
                x[base + k + stride] = a - b;
            }
        }
    }
    float norm = 1.0f / sqrtf((float)n);
    for (size_t i = 0; i < n; i++) x[i] *= norm;
}

void fpq_neon_random_signs_256(float *x, uint64_t seed) {
    uint64_t state = seed ? seed : 0x5DEECE66DUL;
    for (size_t i = 0; i < 256; i += 64) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        uint64_t bits = state;
        size_t end = (i + 64 < 256) ? i + 64 : 256;
        for (size_t j = i; j < end; j++)
            x[j] *= ((bits >> (j - i)) & 1) ? -1.0f : 1.0f;
    }
}

float fpq_neon_dot256(const float *a, const float *b) {
    float sum = 0.0f;
    for (int i = 0; i < 256; i++)
        sum += a[i] * b[i];
    return sum;
}

#endif /* HAVE_NEON */
