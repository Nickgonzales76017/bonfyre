/*
 * gdn_tile.h — BonfyreGDN tiled kernel (cache-friendly, fused, SIMD-ready).
 *
 * This is the production Bonfyre kernel.  gdn_ref.c is the correctness oracle.
 *
 * Optimisations over gdn_ref:
 *
 *   1. Fused update+readout — one pass over H[ki][:] per timestep.
 *      Eliminates the second full H-read in gdn_ref (-33% H bandwidth).
 *
 *   2. Head-major chunk tiling — the hot state H[b,hh][K,V] stays in L1
 *      cache for the entire chunk (chunk_size time-steps).  gdn_ref uses
 *      time-major order which evicts H between heads.
 *
 *   3. Explicit SIMD inner paths:
 *        AVX2/FMA  — x86-64 (runtime check via __builtin_cpu_supports)
 *        NEON      — ARM64 (Apple M-series, always available when compiled
 *                           for aarch64)
 *      The scalar fallback is auto-vectoriser-friendly (tight FMA pattern).
 *
 * Drop-in replacement for gdn_fwd_chunked.  Wire it in via gdn_backend.h.
 */
#pragma once

#include "gdn_ref.h"

/*
 * gdn_fwd_tile — fused-update GDN forward pass.
 *
 * Same contract as gdn_fwd_chunked:
 *   h  — [B,H,K,V] state; pass NULL for zero initial state.
 *   o  — [B,T,H,V] output written by this call.
 *   chunk_size <= 0 uses the default (64).
 *
 * Results are bit-identical to gdn_fwd_chunked for all (B,T,H,K,V).
 */
int gdn_fwd_tile(
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
