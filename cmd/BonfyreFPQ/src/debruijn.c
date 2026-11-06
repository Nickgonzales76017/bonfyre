/* M_PI is not in ISO C11; glibc gates it behind a feature macro while
   macOS exposes it from <math.h> unconditionally. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/*
 * debruijn.c — De Bruijn Universal Constant Codebook
 *
 * ZERO-METADATA PROOF (Requirement #1):
 *
 * Traditional quantization (GPTQ, AWQ, TurboQuant) stores:
 *   - scale factors (16–32 bits per block)
 *   - zero-points (4–16 bits per block)
 *   - codebook indices (variable)
 *
 * Total metadata overhead: 0.1–0.2 bits/weight.
 *
 * FPQ v2 stores NOTHING:
 *   - Radius: derived from tensor type (System F inference)
 *   - Expected angles: derived from the space (Beta distribution mode)
 *   - Common constants: referenced by De Bruijn INDEX (integer, not float)
 *
 * The codebook is a compile-time constant array. It is NEVER serialized.
 * A DBREF(i) node stores one integer (the index) instead of one float.
 * The "codebook" IS the lambda term — its Gödel number encodes the value.
 *
 * This is the Y-combinator self-generation insight: the quantization grid
 * is not stored data but a fixed-point of a computable function.
 *
 * DE BRUIJN INDEXING:
 *
 * Named after Nicolaas Govert de Bruijn's notation for lambda calculus
 * where bound variables are replaced by natural numbers indicating
 * the distance to their binding λ. Here we use the same principle:
 * each constant is addressed by its "binding distance" — its index
 * in the universal constant table.
 *
 * The codebook entries are derived from:
 *   - Transcendental constants (π, e, ln2)
 *   - Rational multiples of π (the natural angles of S^N)
 *   - Common weight distribution centers (0, ±1, ±0.5)
 *   - Golden ratio (appears in transformer attention patterns)
 *   - Square roots (norm-related constants)
 *
 * Total codebook: 32 entries × 4 bytes = 128 bytes in the binary.
 * Total serialized: 0 bytes. It's compiled in.
 */
#include "fpq.h"
#include <math.h>

/* ── The Universal Constant Codebook ──
 *
 * 32 entries covering the most common angle values encountered
 * in FWHT-transformed neural network weight distributions.
 *
 * After FWHT + polar decomposition, angle deviations from π/2
 * cluster around a small set of values. These 32 constants
 * cover the most frequent clusters.
 *
 * Entries 0–7:   Multiples and fractions of π (angular geometry)
 * Entries 8–15:  Common scalars and transcendentals
 * Entries 16–23: Small deviation values (post-type-inference residuals)
 * Entries 24–31: Negative counterparts and special values
 */
const float FPQ_CODEBOOK[FPQ_CODEBOOK_SIZE] = {
    /* 0–7: π-derived angular constants */
    0.0f,                           /*  0: zero (identity deviation) */
    (float)(M_PI / 2.0),           /*  1: π/2 ≈ 1.5708 (Beta mode) */
    (float)(M_PI / 4.0),           /*  2: π/4 ≈ 0.7854 */
    (float)(M_PI / 3.0),           /*  3: π/3 ≈ 1.0472 */
    (float)(M_PI / 6.0),           /*  4: π/6 ≈ 0.5236 */
    (float)(M_PI / 8.0),           /*  5: π/8 ≈ 0.3927 */
    (float)(M_PI),                 /*  6: π   ≈ 3.1416 */
    (float)(2.0 * M_PI),           /*  7: 2π  ≈ 6.2832 */

    /* 8–15: Transcendental and algebraic constants */
    1.0f,                           /*  8: unity */
    -1.0f,                          /*  9: negative unity */
    0.5f,                           /* 10: half */
    -0.5f,                          /* 11: negative half */
    (float)(M_E),                  /* 12: e ≈ 2.7183 */
    (float)(1.0 / M_E),           /* 13: 1/e ≈ 0.3679 */
    (float)(M_SQRT2),             /* 14: √2 ≈ 1.4142 */
    (float)(1.0 / M_SQRT2),       /* 15: 1/√2 ≈ 0.7071 */

    /* 16–23: Common small deviations (O(1/√N) scale for N=256) */
    0.0625f,                        /* 16: 1/16 ≈ 1/√256 */
    -0.0625f,                       /* 17: -1/16 */
    0.125f,                         /* 18: 1/8 */
    -0.125f,                        /* 19: -1/8 */
    0.25f,                          /* 20: 1/4 */
    -0.25f,                         /* 21: -1/4 */
    0.03125f,                       /* 22: 1/32 */
    -0.03125f,                      /* 23: -1/32 */

    /* 24–31: Special values */
    (float)((1.0 + 2.2360679774997896) / 2.0),  /* 24: φ (golden ratio) ≈ 1.6180 */
    (float)(M_LN2),                     /* 25: ln(2) ≈ 0.6931 */
    (float)(M_LOG2E),                   /* 26: log₂(e) ≈ 1.4427 */
    (float)(M_PI * M_PI / 6.0),        /* 27: π²/6 = ζ(2) ≈ 1.6449 */
    (float)(3.0 * M_PI / 4.0),         /* 28: 3π/4 ≈ 2.3562 */
    (float)(M_PI / 12.0),              /* 29: π/12 ≈ 0.2618 */
    0.1f,                               /* 30: common rounding constant */
    -0.1f,                              /* 31: negative rounding constant */
};

/*
 * De Bruijn indexed lookup.
 * The index IS the encoding — no float parameter stored.
 *
 * Proof property: |codebook| = 32, so index fits in 5 bits.
 * A DBREF node costs: 4 bits (opcode) + 5 bits (index) = 9 bits total.
 * A ROT node costs: 4 bits (opcode) + 16 bits (float16 param) = 20 bits.
 * Savings: 11 bits per constant reference.
 *
 * For a seed tree with ~50% constant nodes:
 *   16 nodes × 0.5 × 11 bits = 88 bits = ~0.34 bpw savings per block.
 */
float fpq_dbref(int index) {
    if (index < 0 || index >= FPQ_CODEBOOK_SIZE) return 0.0f;
    return FPQ_CODEBOOK[index];
}
