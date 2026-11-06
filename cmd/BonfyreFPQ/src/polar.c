/* M_PI is not in ISO C11; glibc gates it behind a feature macro while
   macOS exposes it from <math.h> unconditionally. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/*
 * polar.c — Recursive S^N Polar Decomposition
 *
 * The core geometric insight: an N-dimensional vector is NOT a list
 * of N numbers. It is ONE radius (energy) + N-1 phase angles on
 * a hypersphere S^{N-1}.
 *
 * In high dimensions (N → ∞), sphere hardening concentrates ALL
 * Gaussian-distributed vectors onto a thin shell at radius ≈ √N.
 * The radius becomes a constant. We spend 100% of bits on angles.
 *
 * The recursive decomposition:
 *   1. Take vector (x₁, x₂, ..., xₙ)
 *   2. Compute r = ||x||, normalize to unit sphere
 *   3. Recursively decompose using:
 *      θ₁ = atan2(√(x₂² + ... + xₙ²), x₁)
 *      θ₂ = atan2(√(x₃² + ... + xₙ²), x₂)
 *      ...
 *      θₙ₋₁ = atan2(xₙ, xₙ₋₁)
 *
 * This maps the vector to: {r, θ₁, θ₂, ..., θₙ₋₁}
 * where each θᵢ ∈ [0, π] except the last which is ∈ [0, 2π].
 *
 * After FWHT, these angles follow known Beta distributions.
 */
#include "fpq.h"
#include <math.h>
#include <string.h>
#include <stdlib.h>

/*
 * Encode: Cartesian → Polar (recursive S^N decomposition)
 *
 * Returns the radius. angles[0..n-2] are filled with N-1 phase angles.
 * This is the exact N-sphere coordinate transform — not paired 2D polar.
 */
float fpq_polar_encode(const float *x, size_t n, float *angles) {
    if (n == 0) return 0.0f;
    if (n == 1) return fabsf(x[0]);

    /* Compute the full radius (L2 norm) */
    float r_sq = 0.0f;
    for (size_t i = 0; i < n; i++) {
        r_sq += x[i] * x[i];
    }
    float radius = sqrtf(r_sq);

    if (radius < 1e-30f) {
        memset(angles, 0, (n - 1) * sizeof(float));
        return 0.0f;
    }

    /*
     * Recursive S^N decomposition:
     *   For i = 0..n-2:
     *     cumulative_r = sqrt(x[i+1]² + x[i+2]² + ... + x[n-1]²)
     *     angles[i] = atan2(cumulative_r, x[i])
     *
     * Last angle uses atan2(x[n-1], x[n-2]) ∈ [0, 2π]
     */

    /* Precompute suffix norms: suffix_r[i] = sqrt(sum of x[i]² .. x[n-1]²) */
    float *suffix_sq = (float *)malloc(n * sizeof(float));
    suffix_sq[n - 1] = x[n - 1] * x[n - 1];
    for (size_t i = n - 1; i > 0; i--) {
        suffix_sq[i - 1] = suffix_sq[i] + x[i - 1] * x[i - 1];
    }

    /* First n-2 angles: θᵢ = atan2(suffix_norm[i+1], x[i]) ∈ [0, π] */
    for (size_t i = 0; i < n - 2; i++) {
        float suffix_r = sqrtf(suffix_sq[i + 1]);
        angles[i] = atan2f(suffix_r, x[i]);
    }

    /* Last angle: θₙ₋₁ = atan2(xₙ₋₁, xₙ₋₂) ∈ [0, 2π] */
    angles[n - 2] = atan2f(x[n - 1], x[n - 2]);
    /* Map to [0, 2π] */
    if (angles[n - 2] < 0.0f) {
        angles[n - 2] += 2.0f * (float)M_PI;
    }

    free(suffix_sq);
    return radius;
}

/*
 * Decode: Polar → Cartesian (inverse S^N decomposition)
 *
 * Given radius and N-1 angles, reconstruct the N-dimensional vector.
 *
 *   x[0] = r * cos(θ₁)
 *   x[1] = r * sin(θ₁) * cos(θ₂)
 *   x[2] = r * sin(θ₁) * sin(θ₂) * cos(θ₃)
 *   ...
 *   x[n-2] = r * sin(θ₁)*...*sin(θₙ₋₂) * cos(θₙ₋₁)
 *   x[n-1] = r * sin(θ₁)*...*sin(θₙ₋₂) * sin(θₙ₋₁)
 */
void fpq_polar_decode(float radius, const float *angles, size_t n, float *x) {
    if (n == 0) return;
    if (n == 1) {
        x[0] = radius;
        return;
    }

    /* Accumulate product of sines */
    float sin_product = radius;

    for (size_t i = 0; i < n - 1; i++) {
        float c = cosf(angles[i]);
        float s = sinf(angles[i]);

        x[i] = sin_product * c;
        sin_product *= s;
    }

    /* Last coordinate gets the remaining sine product.
     * But we split it using the last angle's cos/sin:
     * x[n-2] uses cos(θ_{n-2}), x[n-1] uses sin(θ_{n-2})
     * This is already handled by the loop for x[n-2].
     * The final sin_product IS x[n-1]. */
    x[n - 1] = sin_product;
}

/*
 * PAIRWISE Recursive Polar Encode
 *
 * Bottom-up: pairs of values → (radius, angle) at each level.
 *
 * Level 0: pairs (x[0],x[1]), (x[2],x[3]), ... → N/2 angles + N/2 radii
 * Level 1: pairs of radii → N/4 angles + N/4 radii
 * ...
 * Level log2(N)-1: last pair → 1 angle + final_radius
 *
 * For structured weights (positional embeddings), adjacent-pair angles
 * directly capture the sin/cos pattern: atan2(cos(ωt), sin(ωt)) = ωt.
 * A simple RAMP seed compresses this perfectly.
 *
 * Error amplification: O(log N) — each angle error affects only its
 * 2^k subtree, not the entire vector like N-sphere does.
 */
float fpq_polar_encode_pairwise(const float *x, size_t n, float *angles) {
    if (n == 0) return 0.0f;
    if (n == 1) return fabsf(x[0]);

    float *cur = (float *)malloc(n * sizeof(float));
    memcpy(cur, x, n * sizeof(float));

    size_t cur_size = n;
    size_t angle_pos = 0;

    while (cur_size > 1) {
        size_t pairs = cur_size / 2;
        float *next = (float *)malloc(pairs * sizeof(float));

        for (size_t p = 0; p < pairs; p++) {
            float a = cur[2 * p];
            float b = cur[2 * p + 1];
            float r = sqrtf(a * a + b * b);
            float theta = atan2f(b, a);
            /* Keep in [-π, π] — natural range for atan2 */
            angles[angle_pos++] = theta;
            next[p] = r;
        }

        free(cur);
        cur = next;
        cur_size = pairs;
    }

    float radius = cur[0];
    free(cur);
    return radius;
}

/*
 * PAIRWISE Recursive Polar Decode
 *
 * Top-down: starting from a single radius, expand using angles
 * at each level from the highest to the lowest.
 *
 *   Level L-1: 1 radius + angles[n-2] → 2 radii
 *   Level L-2: 2 radii + angles[n/2+n/4+...] → 4 radii
 *   ...
 *   Level 0:   N/2 radii + angles[0..N/2-1] → N values
 */
void fpq_polar_decode_pairwise(float radius, const float *angles,
                                size_t n, float *x) {
    if (n == 0) return;
    if (n == 1) { x[0] = radius; return; }

    /* Count levels */
    size_t n_levels = 0;
    for (size_t s = n; s > 1; s >>= 1) n_levels++;

    /* Precompute angle offsets and counts per level */
    size_t *loff = (size_t *)malloc(n_levels * sizeof(size_t));
    size_t *lcnt = (size_t *)malloc(n_levels * sizeof(size_t));
    {
        size_t off = 0, cnt = n / 2;
        for (size_t l = 0; l < n_levels; l++) {
            loff[l] = off;
            lcnt[l] = cnt;
            off += cnt;
            cnt /= 2;
        }
    }

    /* Start with single radius, decode top-down */
    float *cur = (float *)malloc(sizeof(float));
    cur[0] = radius;

    for (int l = (int)n_levels - 1; l >= 0; l--) {
        size_t pairs = lcnt[l];
        float *next = (float *)malloc(2 * pairs * sizeof(float));

        for (size_t p = 0; p < pairs; p++) {
            float r = cur[p];
            float theta = angles[loff[l] + p];
            next[2 * p]     = r * cosf(theta);
            next[2 * p + 1] = r * sinf(theta);
        }

        free(cur);
        cur = next;
    }

    memcpy(x, cur, n * sizeof(float));
    free(cur);
    free(loff);
    free(lcnt);
}
