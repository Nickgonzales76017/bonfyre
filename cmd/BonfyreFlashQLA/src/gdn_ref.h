/*
 * gdn_ref.h — Gated Delta Network (GDN) Chunked Prefill, portable C11 reference.
 *
 * Implements the linear attention recurrence from the Gated Delta Rule:
 *
 *   H_t = g_t * H_{t-1} + beta_t * (v_t ⊗ k_t)     [state update]
 *   o_t = H_t^T * q_t                                [read-out]
 *
 * where per timestep t, per head hh:
 *   g_t    : scalar gate in (0,1) — controls history retention
 *   beta_t : scalar update scale
 *   k_t, q_t : [K] key/query vectors (q_t scaled by `scale` arg)
 *   v_t    : [V] value vector
 *   H      : [K, V] state matrix
 *
 * Tensor layouts (row-major, packed float32):
 *   q, k   : [B, T, H, K]
 *   v      : [B, T, H, V]
 *   g, beta: [B, T, H]
 *   h      : [B, H, K, V]  (state — caller-owned, updated in place)
 *   o      : [B, T, H, V]  (output)
 *
 * Model-agnostic: works with any architecture using the GDN delta rule,
 * including Qwen3/3.6 and any future model adopting linear attention with
 * exponential-decay gates.
 *
 * Hardware dispatch:
 *   bonfyre-flashqla run checks BONFYRE_FLASHQLA=1 and SM90+ to dispatch
 *   to the Python FlashQLA package for 2-3x forward speedup on Hopper.
 *   This C reference is the fallback for all other hardware.
 *
 * Binary file format magic numbers (little-endian):
 *   GDN_INPUT_MAGIC  0x414C4742  'BGLA'
 *   GDN_OUTPUT_MAGIC 0x4F47444E  'NGDO'
 */
#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Shape descriptor */
typedef struct {
    int B;  /* batch size                  */
    int T;  /* sequence length             */
    int H;  /* number of heads             */
    int K;  /* key / query head dimension  */
    int V;  /* value head dimension        */
} GdnShape;

/*
 * gdn_fwd — sequential GDN forward pass.
 *
 * h: state buffer [B, H, K, V].  Pass NULL for zero initial state
 *    (allocated internally and freed on return — state not preserved).
 *    Pass a caller-allocated, zero-initialised buffer to carry state
 *    across calls for streaming / long-context inference.
 *
 * Returns 0 on success, 1 on malloc failure.
 */
int gdn_fwd(
    const float *q,     /* [B, T, H, K] queries                         */
    const float *k,     /* [B, T, H, K] keys                            */
    const float *v,     /* [B, T, H, V] values                          */
    const float *g,     /* [B, T, H]    gate  (0–1 per head-step)       */
    const float *beta,  /* [B, T, H]    update scale per head-step      */
    float        scale, /* scalar query scale, e.g. 1/sqrt(K)           */
    float       *h,     /* [B, H, K, V] state — in/out (or NULL)        */
    float       *o,     /* [B, T, H, V] output                          */
    GdnShape     shape);

/*
 * gdn_fwd_chunked — like gdn_fwd but processes the time dimension in tiles
 * of chunk_size steps, improving L1 cache reuse on the state matrix.
 *
 * chunk_size <= 0 uses the default (64).
 */
int gdn_fwd_chunked(
    const float *q,
    const float *k,
    const float *v,
    const float *g,
    const float *beta,
    float        scale,
    float       *h,
    float       *o,
    GdnShape     shape,
    int          chunk_size);

/* File-format magic numbers */
#define GDN_INPUT_MAGIC  0x414C4742u   /* 'BGLA' little-endian */
#define GDN_OUTPUT_MAGIC 0x4F47444Eu   /* 'NGDO' little-endian */
#define GDN_FORMAT_VER   1u

#ifdef __cplusplus
}
#endif
