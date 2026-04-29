/*
 * gdn_backend.h — BonfyreGDN backend selection and dispatch.
 *
 * Backends:
 *   ref   — gdn_fwd_chunked (two-pass scalar; correctness oracle)
 *   tile  — gdn_fwd_tile    (fused, head-major, SIMD-dispatched; DEFAULT)
 *   avx2  — alias for tile (SIMD dispatch happens inside gdn_fwd_tile)
 *   neon  — alias for tile (ditto)
 *   auto  — tile (best available path)
 *
 * Environment variable:
 *   BONFYRE_FLASHQLA_BACKEND=ref|tile|avx2|neon|auto
 *   (overrides --backend CLI arg if set; unset defaults to auto/tile)
 */
#pragma once

#include <string.h>

#include "gdn_ref.h"
#include "gdn_tile.h"

/* Backend enum ─────────────────────────────────────────────────── */
typedef enum {
    GDN_BACKEND_AUTO  = 0,   /* choose best available (= tile)    */
    GDN_BACKEND_REF   = 1,   /* gdn_fwd_chunked — correctness ref */
    GDN_BACKEND_TILE  = 2,   /* gdn_fwd_tile    — production path */
    GDN_BACKEND_AVX2  = 3,   /* tile (AVX2 selected inside tile)  */
    GDN_BACKEND_NEON  = 4,   /* tile (NEON selected inside tile)  */
} GdnBackend;

/* Function pointer type matching both gdn_fwd_chunked and gdn_fwd_tile */
typedef int (*GdnFwdFn)(
    const float *q, const float *k, const float *v,
    const float *g, const float *beta,
    float scale, float *h, float *o,
    GdnShape shape, int chunk_size);

/* ─── Inline helpers ──────────────────────────────────────────────── */

static inline const char *gdn_backend_name(GdnBackend b) {
    switch (b) {
        case GDN_BACKEND_REF:  return "ref";
        case GDN_BACKEND_TILE: return "tile";
        case GDN_BACKEND_AVX2: return "avx2";
        case GDN_BACKEND_NEON: return "neon";
        default:               return "auto";
    }
}

static inline GdnBackend gdn_backend_from_str(const char *s) {
    if (!s || strcmp(s, "auto") == 0) return GDN_BACKEND_AUTO;
    if (strcmp(s, "ref")  == 0)       return GDN_BACKEND_REF;
    if (strcmp(s, "tile") == 0)       return GDN_BACKEND_TILE;
    if (strcmp(s, "avx2") == 0)       return GDN_BACKEND_AVX2;
    if (strcmp(s, "neon") == 0)       return GDN_BACKEND_NEON;
    return GDN_BACKEND_AUTO;
}

/*
 * Resolve the effective backend:
 *   1. Honour BONFYRE_FLASHQLA_BACKEND env var if set.
 *   2. Fall back to the provided 'requested' value.
 *   3. auto / tile / avx2 / neon all resolve to gdn_fwd_tile
 *      (SIMD selection happens inside gdn_fwd_tile at runtime).
 */
static inline GdnBackend gdn_backend_resolve(GdnBackend requested) {
    const char *env = getenv("BONFYRE_FLASHQLA_BACKEND");
    if (env && *env) {
        GdnBackend e = gdn_backend_from_str(env);
        if (e != GDN_BACKEND_AUTO) return e;
    }
    return requested;
}

/*
 * Map a resolved backend to the corresponding function.
 * All non-ref backends use gdn_fwd_tile; SIMD dispatch is internal.
 */
static inline GdnFwdFn gdn_backend_fn(GdnBackend b) {
    return (b == GDN_BACKEND_REF) ? gdn_fwd_chunked : gdn_fwd_tile;
}
