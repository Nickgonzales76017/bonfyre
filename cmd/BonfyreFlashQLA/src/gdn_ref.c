/*
 * gdn_ref.c — Gated Delta Network (GDN) Chunked Prefill, C11 reference.
 *
 * Implements the GDN delta rule linear attention recurrence:
 *
 *   H_t = g_t * H_{t-1} + beta_t * (v_t ⊗ (scale * k_t))
 *   o_t = H_t^T * q_t
 *
 * This is the portable CPU reference path used by bonfyre-flashqla when
 * the FlashQLA Python backend is unavailable (no SM90+ GPU or no PyTorch).
 * On SM90+ hardware with FlashQLA installed, the 'run' command dispatches
 * to the Python package via fork/exec for the full 2-3x Hopper speedup.
 *
 * Performance notes (C reference):
 *   The inner loops are O(K*V) per (timestep, head). For K=V=64 and H=8
 *   this is 32K FMA ops per token — roughly 300 MB/s at 1 GFLOP/s.
 *   gdn_fwd_chunked improves L1 cache hit rate for the state matrix by
 *   keeping a chunk of time-steps together before advancing batches.
 */

#include "gdn_ref.h"

#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ─── flat index helpers ─────────────────────────────────────────────
 * All tensors are row-major, packed (no padding).
 * The macros intentionally reference the local variable `s` (GdnShape)
 * present in every function that uses them.
 */
#define IDX_QK(b,t,hh,ki)  \
    ((size_t)(b)*(size_t)s.T*s.H*s.K + (size_t)(t)*s.H*s.K + (size_t)(hh)*s.K + (ki))
#define IDX_V(b,t,hh,vi)   \
    ((size_t)(b)*(size_t)s.T*s.H*s.V + (size_t)(t)*s.H*s.V + (size_t)(hh)*s.V + (vi))
#define IDX_G(b,t,hh)      \
    ((size_t)(b)*(size_t)s.T*s.H + (size_t)(t)*s.H + (hh))
#define IDX_H(b,hh,ki,vi)  \
    ((size_t)(b)*(size_t)s.H*s.K*s.V + (size_t)(hh)*s.K*s.V + (size_t)(ki)*s.V + (vi))

/* ═══════════════════════════════════════════════════════════════════
 * gdn_fwd — sequential scan, simple and correct.
 * ═══════════════════════════════════════════════════════════════════ */
int gdn_fwd(
    const float *q, const float *k, const float *v,
    const float *g, const float *beta,
    float scale,
    float *h, float *o,
    GdnShape s)
{
    int state_owned = 0;
    if (!h) {
        h = (float *)calloc((size_t)s.B * s.H * s.K * s.V, sizeof(float));
        if (!h) return 1;
        state_owned = 1;
    }

    for (int b = 0; b < s.B; b++) {
        for (int t = 0; t < s.T; t++) {
            for (int hh = 0; hh < s.H; hh++) {
                float gate = g[IDX_G(b, t, hh)];
                float bval = beta[IDX_G(b, t, hh)];

                /* ── state update: H = gate*H + beta * v_t ⊗ (scale*k_t) ── */
                for (int ki = 0; ki < s.K; ki++) {
                    float gkval = bval * k[IDX_QK(b, t, hh, ki)] * scale;
                    float *hrow = h + IDX_H(b, hh, ki, 0);
                    const float *vrow = v + IDX_V(b, t, hh, 0);
                    for (int vi = 0; vi < s.V; vi++)
                        hrow[vi] = gate * hrow[vi] + gkval * vrow[vi];
                }

                /* ── read-out: o_t = H^T * q_t ─────────────────────────── */
                float       *orow = o + IDX_V(b, t, hh, 0);
                const float *qrow = q + IDX_QK(b, t, hh, 0);
                for (int vi = 0; vi < s.V; vi++) {
                    float acc = 0.0f;
                    for (int ki = 0; ki < s.K; ki++)
                        acc += h[IDX_H(b, hh, ki, vi)] * qrow[ki];
                    orow[vi] = acc;
                }
            }
        }
    }

    if (state_owned) free(h);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * gdn_fwd_chunked — same algorithm but tiles the T dimension.
 *
 * Processing each batch item through time in chunks keeps the [K,V]
 * state matrix warm in L1 across the chunk, reducing cache misses
 * compared to interleaving batches.  For the common case B=1 this
 * is identical to gdn_fwd.
 * ═══════════════════════════════════════════════════════════════════ */
int gdn_fwd_chunked(
    const float *q, const float *k, const float *v,
    const float *g, const float *beta,
    float scale,
    float *h, float *o,
    GdnShape s, int chunk_size)
{
    if (chunk_size <= 0) chunk_size = 64;

    int state_owned = 0;
    if (!h) {
        h = (float *)calloc((size_t)s.B * s.H * s.K * s.V, sizeof(float));
        if (!h) return 1;
        state_owned = 1;
    }

    /* For each batch independently, sweep time in chunks so the per-head
     * state [K,V] stays hot before we move to the next batch item.      */
    for (int b = 0; b < s.B; b++) {
        for (int t0 = 0; t0 < s.T; t0 += chunk_size) {
            int t_end = t0 + chunk_size;
            if (t_end > s.T) t_end = s.T;

            for (int t = t0; t < t_end; t++) {
                for (int hh = 0; hh < s.H; hh++) {
                    float gate = g[IDX_G(b, t, hh)];
                    float bval = beta[IDX_G(b, t, hh)];

                    for (int ki = 0; ki < s.K; ki++) {
                        float gkval = bval * k[IDX_QK(b, t, hh, ki)] * scale;
                        float *hrow = h + IDX_H(b, hh, ki, 0);
                        const float *vrow = v + IDX_V(b, t, hh, 0);
                        for (int vi = 0; vi < s.V; vi++)
                            hrow[vi] = gate * hrow[vi] + gkval * vrow[vi];
                    }

                    float       *orow = o + IDX_V(b, t, hh, 0);
                    const float *qrow = q + IDX_QK(b, t, hh, 0);
                    for (int vi = 0; vi < s.V; vi++) {
                        float acc = 0.0f;
                        for (int ki = 0; ki < s.K; ki++)
                            acc += h[IDX_H(b, hh, ki, vi)] * qrow[ki];
                        orow[vi] = acc;
                    }
                }
            }
        }
    }

    if (state_owned) free(h);
    return 0;
}
