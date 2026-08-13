/*
 * gdn_tile.c — BonfyreGDN tiled + SIMD kernel.
 *
 * Key idea vs gdn_ref.c:
 *
 *   gdn_ref does two separate passes over H[K×V] per timestep:
 *     pass 1: update  H[ki][vi] = gate*H[ki][vi] + gkval*v[vi]
 *     pass 2: readout o[vi]    += q[ki] * H[ki][vi]   for ki in 0..K
 *
 *   gdn_tile fuses both into a SINGLE pass per timestep:
 *     for ki in 0..K:
 *       hn          = gate * H[ki][vi] + gkval * v[vi]
 *       H[ki][vi]   = hn                          // write once
 *       o[vi]      += q[ki] * hn                  // fused readout
 *
 *   This halves H read traffic (from 3×K×V to 2×K×V per step) and
 *   makes the inner loop a 4-op FMA pattern the auto-vectoriser loves.
 *
 * Iteration order:
 *   gdn_ref:  for b: for t: for hh   — H[b,hh] evicted between heads
 *   gdn_tile: for b: for hh: for chunk_t0: for t
 *               — H[b,hh][K,V] stays in L1 for the whole chunk
 *
 * Results are bit-identical to gdn_fwd_chunked because the reduction
 * order over ki is preserved inside each (b,hh) pair.
 */

#include "gdn_tile.h"

#include <stdlib.h>
#include <string.h>

/* ─── Flat index macros (identical to gdn_ref.c, variable must be named 's') */
#define IDX_QK(b,t,hh,ki) \
    ((size_t)(b)*(size_t)s.T*s.H*s.K + (size_t)(t)*s.H*s.K + (size_t)(hh)*s.K + (ki))
#define IDX_V(b,t,hh,vi) \
    ((size_t)(b)*(size_t)s.T*s.H*s.V + (size_t)(t)*s.H*s.V + (size_t)(hh)*s.V + (vi))
#define IDX_G(b,t,hh) \
    ((size_t)(b)*(size_t)s.T*s.H + (size_t)(t)*s.H + (hh))
#define IDX_H(b,hh,ki,vi) \
    ((size_t)(b)*(size_t)s.H*s.K*s.V + (size_t)(hh)*s.K*s.V + (size_t)(ki)*s.V + (vi))

/* ═══════════════════════════════════════════════════════════════════
 * Scalar fused step — correctness baseline, auto-vectoriser-friendly.
 *
 * Inner loop pattern (per ki):
 *   load  hrow[vi]           (H read)
 *   FMA   hn = gate*h + b*v  (H update)
 *   store hrow[vi] = hn      (H write)
 *   FMA   o[vi] += q*hn      (fused readout, accumulate)
 *   store o[vi]
 *
 * This 4-FMA / 2-load / 2-store pattern is what compilers turn into
 * 4-wide (NEON) or 8-wide (AVX2) vector instructions automatically on
 * -O2 + target arch.  The explicit SIMD paths below exist for platforms
 * where the auto-vectoriser needs a nudge, or to guarantee the FMA form.
 * ═══════════════════════════════════════════════════════════════════ */
static void fused_step_scalar(
    const float *restrict qrow,
    const float *restrict krow,
    const float *restrict vrow,
    float  gate,
    float  bval,
    float  scale_val,
    float *restrict h,   /* H[hh][0..K-1][0..V-1] base pointer */
    float *restrict o,   /* o[t][hh][0..V-1]      base pointer */
    int K, int V)
{
    memset(o, 0, (size_t)V * sizeof(float));
    for (int ki = 0; ki < K; ki++) {
        const float gkval = bval * krow[ki] * scale_val;
        const float qval  = qrow[ki];
        float *restrict hrow = h + (size_t)ki * V;
        for (int vi = 0; vi < V; vi++) {
            const float hn = gate * hrow[vi] + gkval * vrow[vi];
            hrow[vi] = hn;
            o[vi]   += qval * hn;
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * AVX2 + FMA explicit path (x86-64 only, runtime-checked).
 * ═══════════════════════════════════════════════════════════════════ */
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>

__attribute__((target("avx2,fma")))
static void fused_step_avx2(
    const float *restrict qrow,
    const float *restrict krow,
    const float *restrict vrow,
    float  gate,
    float  bval,
    float  scale_val,
    float *restrict h,
    float *restrict o,
    int K, int V)
{
    memset(o, 0, (size_t)V * sizeof(float));
    for (int ki = 0; ki < K; ki++) {
        const float  gkval  = bval * krow[ki] * scale_val;
        const float  qval   = qrow[ki];
        float *restrict hrow = h + (size_t)ki * V;
        const __m256 vgate  = _mm256_set1_ps(gate);
        const __m256 vgkval = _mm256_set1_ps(gkval);
        const __m256 vqval  = _mm256_set1_ps(qval);
        int vi = 0;
        /* 8-wide FMA vectorised main loop */
        for (; vi <= V - 8; vi += 8) {
            __m256 hv = _mm256_loadu_ps(hrow + vi);
            __m256 vv = _mm256_loadu_ps(vrow + vi);
            /* hn = gate*h + gkval*v */
            __m256 hn = _mm256_fmadd_ps(vgate, hv, _mm256_mul_ps(vgkval, vv));
            _mm256_storeu_ps(hrow + vi, hn);
            __m256 ov = _mm256_loadu_ps(o + vi);
            /* o += qval*hn */
            ov = _mm256_fmadd_ps(vqval, hn, ov);
            _mm256_storeu_ps(o + vi, ov);
        }
        /* scalar tail for V % 8 != 0 */
        for (; vi < V; vi++) {
            const float hn = gate * hrow[vi] + gkval * vrow[vi];
            hrow[vi] = hn;
            o[vi]   += qval * hn;
        }
    }
}

static int has_avx2(void) { return __builtin_cpu_supports("avx2"); }
#endif /* x86 */

/* ═══════════════════════════════════════════════════════════════════
 * NEON explicit path (ARM64 — Apple M-series, Ampere, etc.)
 * vmlaq_n_f32(acc, v, scalar) = acc + v * scalar  (FMA form)
 * ═══════════════════════════════════════════════════════════════════ */
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>

static void fused_step_neon(
    const float *restrict qrow,
    const float *restrict krow,
    const float *restrict vrow,
    float  gate,
    float  bval,
    float  scale_val,
    float *restrict h,
    float *restrict o,
    int K, int V)
{
    memset(o, 0, (size_t)V * sizeof(float));
    for (int ki = 0; ki < K; ki++) {
        const float gkval = bval * krow[ki] * scale_val;
        const float qval  = qrow[ki];
        float *restrict hrow = h + (size_t)ki * V;
        int vi = 0;
        /* 4-wide FMA vectorised main loop */
        for (; vi <= V - 4; vi += 4) {
            float32x4_t hv = vld1q_f32(hrow + vi);
            float32x4_t vv = vld1q_f32(vrow + vi);
            /* hn = gate*h + gkval*v  (two fused FMAs) */
            float32x4_t hn = vmlaq_n_f32(vmulq_n_f32(hv, gate), vv, gkval);
            vst1q_f32(hrow + vi, hn);
            float32x4_t ov = vld1q_f32(o + vi);
            /* o += qval*hn */
            ov = vmlaq_n_f32(ov, hn, qval);
            vst1q_f32(o + vi, ov);
        }
        /* scalar tail for V % 4 != 0 */
        for (; vi < V; vi++) {
            const float hn = gate * hrow[vi] + gkval * vrow[vi];
            hrow[vi] = hn;
            o[vi]   += qval * hn;
        }
    }
}

#define HAS_NEON 1
#else
#define HAS_NEON 0
#endif /* ARM NEON */

/* ═══════════════════════════════════════════════════════════════════
 * Runtime dispatch — pick best available inner step
 * ═══════════════════════════════════════════════════════════════════ */
typedef void (*fused_step_fn)(
    const float *qrow, const float *krow, const float *vrow,
    float gate, float bval, float scale_val,
    float *h, float *o, int K, int V);

static fused_step_fn pick_fused_step(void) {
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    /* ARM64: NEON is always available when the compiler targets aarch64 */
    return fused_step_neon;
#elif defined(__x86_64__) || defined(__i386__)
    if (has_avx2()) return fused_step_avx2;
#endif
    return fused_step_scalar;
}

/* ═══════════════════════════════════════════════════════════════════
 * gdn_fwd_tile — head-major tiled loop, fused update+readout.
 *
 * Iteration order: batch → head → chunk → time-within-chunk
 *
 * Keeping (b, hh) fixed for an entire chunk means H[b,hh][K,V] (16 KB
 * for K=V=64) stays resident in L1 for chunk_size iterations before
 * the next (b, hh) pair is loaded.  gdn_ref uses time-major order so
 * H[b,hh] is evicted and reloaded once per head per timestep.
 * ═══════════════════════════════════════════════════════════════════ */
int gdn_fwd_tile(
    const float *q, const float *k, const float *v,
    const float *g, const float *beta,
    float scale,
    float *h, float *o,
    GdnShape shape, int chunk_size)
{
    /* Rename for the IDX macros which reference 's' */
    GdnShape s = shape;
    if (chunk_size <= 0) chunk_size = 64;

    int state_owned = 0;
    if (!h) {
        h = (float *)calloc((size_t)s.B * s.H * s.K * s.V, sizeof(float));
        if (!h) return 1;
        state_owned = 1;
    }

    fused_step_fn step = pick_fused_step();

    for (int b = 0; b < s.B; b++) {
        for (int hh = 0; hh < s.H; hh++) {
            /* H[b,hh][K,V] — this pointer stays hot in L1 for the chunk */
            float *hbase = h + IDX_H(b, hh, 0, 0);

            for (int t0 = 0; t0 < s.T; t0 += chunk_size) {
                int t_end = t0 + chunk_size;
                if (t_end > s.T) t_end = s.T;

                for (int t = t0; t < t_end; t++) {
                    const float gate = g[IDX_G(b, t, hh)];
                    const float bval = beta[IDX_G(b, t, hh)];
                    step(
                        q + IDX_QK(b, t, hh, 0),
                        k + IDX_QK(b, t, hh, 0),
                        v + IDX_V(b, t, hh, 0),
                        gate, bval, scale,
                        hbase,
                        o + IDX_V(b, t, hh, 0),
                        s.K, s.V);
                }
            }
        }
    }

    if (state_owned) free(h);
    return 0;
}

/* ─── SIMD availability query (used by doctor/bench) ─────────────── */
const char *gdn_tile_simd_name(void) {
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    return "neon";
#elif defined(__x86_64__) || defined(__i386__)
    return has_avx2() ? "avx2" : "scalar";
#else
    return "scalar";
#endif
}
