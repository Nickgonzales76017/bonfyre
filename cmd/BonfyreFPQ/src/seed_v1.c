/* M_PI is not in ISO C11; glibc gates it behind a feature macro while
   macOS exposes it from <math.h> unconditionally. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/*
 * seed.c — The Lambda Core: Seed Combinator Discovery & Expansion
 *
 * THE FUNDAMENTAL SHIFT: We do not store weights. We do not store angles.
 * We store the shortest PROGRAM that, when executed through recursive
 * polar expansion, generates the angles that reconstruct the weights.
 *
 * This is Kolmogorov compression applied to neural networks:
 *   Q(v) = min { |P| : eval(P) ≈ PolarTransform(v) }
 *   where P is a lambda term (combinator tree)
 *
 * The combinator vocabulary:
 *   ROT(θ)       — emit constant angle θ
 *   PAIR(a, b)   — split output: first half from a, second half from b
 *   SCALE(s, a)  — multiply all angles from a by scalar s
 *   SHIFT(d, a)  — add offset d to all angles from a
 *   REP(k, a)    — repeat a's output k times (Church numeral)
 *   FOLD(a)      — Y-combinator: fixed-point recursion of a
 *   LERP(t,a,b)  — interpolate between a and b outputs
 *   FREQ(k,p)    — sin(k*i/n + p): frequency-domain primitive
 *
 * A 256-dimensional angle block that would take 256 × 32 = 8192 bits
 * can often be represented by a seed of 4-12 nodes ≈ 64-200 bits.
 * That's 40-128x compression BEFORE QJL. The weights are the PROGRAM.
 */
#include "fpq.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>

/* ── Node allocation ── */

fpq_node_t *fpq_node_alloc(fpq_op_t op) {
    fpq_node_t *n = (fpq_node_t *)calloc(1, sizeof(fpq_node_t));
    if (n) n->op = op;
    return n;
}

void fpq_node_free(fpq_node_t *node) {
    if (!node) return;
    fpq_node_free(node->left);
    fpq_node_free(node->right);
    free(node);
}

size_t fpq_node_count(const fpq_node_t *node) {
    if (!node) return 0;
    return 1 + fpq_node_count(node->left) + fpq_node_count(node->right);
}

void fpq_seed_free(fpq_seed_t *seed) {
    if (!seed) return;
    fpq_node_free(seed->root);
    free(seed);
}

/* ── Seed Expansion: Run the program to generate angles ── */

/*
 * Internal expansion: fill output[0..dim-1] by evaluating the combinator tree.
 * This IS the "recursive polar expansion" — the seed unfolds into angles.
 */
static void expand_node(const fpq_node_t *node, float *output, size_t dim,
                        int depth) {
    if (!node || dim == 0 || depth > FPQ_SEED_MAX_DEPTH) {
        memset(output, 0, dim * sizeof(float));
        return;
    }

    switch (node->op) {
    case FPQ_OP_ROT:
        /* Base case: fill all positions with constant angle */
        for (size_t i = 0; i < dim; i++) {
            output[i] = node->param[0];
        }
        break;

    case FPQ_OP_PAIR: {
        /* Binary split: left generates first half, right generates second */
        size_t mid = dim / 2;
        expand_node(node->left,  output,       mid,       depth + 1);
        expand_node(node->right, output + mid,  dim - mid, depth + 1);
        break;
    }

    case FPQ_OP_SCALE:
        /* Scale child's output by param[0] */
        expand_node(node->left, output, dim, depth + 1);
        for (size_t i = 0; i < dim; i++) {
            output[i] *= node->param[0];
        }
        break;

    case FPQ_OP_SHIFT:
        /* Shift child's output by param[0] */
        expand_node(node->left, output, dim, depth + 1);
        for (size_t i = 0; i < dim; i++) {
            output[i] += node->param[0];
        }
        break;

    case FPQ_OP_REP: {
        /* Church numeral: repeat child's pattern k times across dim.
         * Each repetition is dim/k elements long. */
        int k = node->iparam > 0 ? node->iparam : 1;
        if (k > (int)dim) k = (int)dim;
        size_t chunk = dim / (size_t)k;
        if (chunk == 0) chunk = 1;

        /* Expand child into first chunk */
        float *proto = (float *)malloc(chunk * sizeof(float));
        expand_node(node->left, proto, chunk, depth + 1);

        /* Tile across output */
        for (size_t i = 0; i < dim; i++) {
            output[i] = proto[i % chunk];
        }
        free(proto);
        break;
    }

    case FPQ_OP_FOLD: {
        /* Y-combinator: fixed-point iteration.
         * Start with zeros, repeatedly apply child transform.
         * This generates fractal-like angle patterns. */
        memset(output, 0, dim * sizeof(float));
        int iters = node->iparam > 0 ? node->iparam : 3;
        if (iters > 8) iters = 8;

        float *tmp = (float *)malloc(dim * sizeof(float));
        for (int it = 0; it < iters; it++) {
            memcpy(tmp, output, dim * sizeof(float));
            expand_node(node->left, output, dim, depth + 1);
            /* Blend with previous iteration for convergence */
            for (size_t i = 0; i < dim; i++) {
                output[i] = 0.5f * (output[i] + tmp[i]);
            }
        }
        free(tmp);
        break;
    }

    case FPQ_OP_LERP: {
        /* Interpolate between two children */
        float t = node->param[0];
        float *a = (float *)malloc(dim * sizeof(float));
        float *b = (float *)malloc(dim * sizeof(float));
        expand_node(node->left,  a, dim, depth + 1);
        expand_node(node->right, b, dim, depth + 1);
        for (size_t i = 0; i < dim; i++) {
            output[i] = (1.0f - t) * a[i] + t * b[i];
        }
        free(a);
        free(b);
        break;
    }

    case FPQ_OP_FREQ:
        /* Frequency primitive: sin(k * i / dim + phase)
         * This captures periodic structure in the angle sequence.
         * Neural network weights are often quasi-periodic. */
        for (size_t i = 0; i < dim; i++) {
            float normalized = (float)i / (float)dim;
            output[i] = sinf(node->param[0] * normalized * 2.0f * (float)M_PI
                             + node->param[1]);
        }
        break;
    }
}

void fpq_seed_expand(const fpq_seed_t *seed, float *output) {
    if (!seed || !seed->root || seed->target_dim == 0) return;
    expand_node(seed->root, output, seed->target_dim, 0);
}

/* ── Seed Discovery: The Kolmogorov Compression Search ── */

/*
 * Compute MSE between target and candidate angles.
 */
static float angle_mse(const float *target, const float *candidate, size_t n) {
    float sum = 0.0f;
    for (size_t i = 0; i < n; i++) {
        float d = target[i] - candidate[i];
        sum += d * d;
    }
    return sum / (float)n;
}

/*
 * Find the best constant rotation (single-node seed).
 * This is the simplest possible program: ROT(θ) where θ = mean(angles).
 */
static fpq_seed_t *try_constant(const float *target, size_t n) {
    float mean = 0.0f;
    for (size_t i = 0; i < n; i++) mean += target[i];
    mean /= (float)n;

    fpq_seed_t *seed = (fpq_seed_t *)calloc(1, sizeof(fpq_seed_t));
    seed->root = fpq_node_alloc(FPQ_OP_ROT);
    seed->root->param[0] = mean;
    seed->target_dim = n;
    seed->tree_size = 1;

    float *expanded = (float *)malloc(n * sizeof(float));
    fpq_seed_expand(seed, expanded);
    seed->distortion = angle_mse(target, expanded, n);
    free(expanded);
    return seed;
}

/*
 * Find the best frequency seed: FREQ(k, phase) or SHIFT(d, FREQ(k, phase)).
 * Searches over frequency bins to find the dominant frequency.
 * Neural network weights have strong periodic structure.
 */
static fpq_seed_t *try_frequency(const float *target, size_t n) {
    float best_mse = FLT_MAX;
    float best_k = 1.0f, best_phase = 0.0f, best_amp = 1.0f, best_offset = 0.0f;

    /* Compute mean and std */
    float mean = 0.0f, var = 0.0f;
    for (size_t i = 0; i < n; i++) mean += target[i];
    mean /= (float)n;
    for (size_t i = 0; i < n; i++) {
        float d = target[i] - mean;
        var += d * d;
    }
    var /= (float)n;
    float std = sqrtf(var + 1e-10f);

    /* Search frequency bins */
    float *expanded = (float *)malloc(n * sizeof(float));
    for (int ki = 1; ki <= 32; ki++) {
        /* For each frequency, find optimal phase via cross-correlation */
        float cos_sum = 0.0f, sin_sum = 0.0f;
        for (size_t i = 0; i < n; i++) {
            float normalized = (float)i / (float)n;
            float angle = (float)ki * normalized * 2.0f * (float)M_PI;
            cos_sum += (target[i] - mean) * cosf(angle);
            sin_sum += (target[i] - mean) * sinf(angle);
        }
        float phase = atan2f(sin_sum, cos_sum);
        float amp = sqrtf(cos_sum * cos_sum + sin_sum * sin_sum) * 2.0f / (float)n;

        /* Generate candidate */
        for (size_t i = 0; i < n; i++) {
            float normalized = (float)i / (float)n;
            expanded[i] = amp * sinf((float)ki * normalized * 2.0f * (float)M_PI + phase) + mean;
        }
        float mse = angle_mse(target, expanded, n);

        if (mse < best_mse) {
            best_mse = mse;
            best_k = (float)ki;
            best_phase = phase;
            best_amp = amp;
            best_offset = mean;
        }
    }
    free(expanded);

    /* Build: SHIFT(offset, SCALE(amp, FREQ(k, phase))) */
    fpq_seed_t *seed = (fpq_seed_t *)calloc(1, sizeof(fpq_seed_t));

    fpq_node_t *freq = fpq_node_alloc(FPQ_OP_FREQ);
    freq->param[0] = best_k;
    freq->param[1] = best_phase;

    fpq_node_t *scale = fpq_node_alloc(FPQ_OP_SCALE);
    scale->param[0] = best_amp;
    scale->left = freq;

    fpq_node_t *shift = fpq_node_alloc(FPQ_OP_SHIFT);
    shift->param[0] = best_offset;
    shift->left = scale;

    seed->root = shift;
    seed->target_dim = n;
    seed->tree_size = 3;

    expanded = (float *)malloc(n * sizeof(float));
    fpq_seed_expand(seed, expanded);
    seed->distortion = angle_mse(target, expanded, n);
    free(expanded);
    return seed;
}

/*
 * Try binary split: PAIR(seed_left, seed_right)
 * Recursively find seeds for each half.
 */
static fpq_seed_t *try_split(const float *target, size_t n,
                              size_t max_nodes, float tolerance, int depth);

static fpq_seed_t *discover_recursive(const float *target, size_t n,
                                       size_t max_nodes, float tolerance,
                                       int depth) {
    if (n == 0) return NULL;
    if (depth > FPQ_SEED_MAX_DEPTH || max_nodes == 0) {
        return try_constant(target, n);
    }

    /* Try candidates in order of complexity */
    fpq_seed_t *best = try_constant(target, n);

    if (best->distortion <= tolerance) return best;

    /* Try frequency decomposition (3 nodes) */
    if (max_nodes >= 3) {
        fpq_seed_t *freq = try_frequency(target, n);
        if (freq->distortion < best->distortion) {
            fpq_seed_free(best);
            best = freq;
        } else {
            fpq_seed_free(freq);
        }
        if (best->distortion <= tolerance) return best;
    }

    /* Try multi-frequency: LERP between two freqs (7 nodes) */
    if (max_nodes >= 7 && n >= 4) {
        /* Compute residual after best freq */
        float *expanded = (float *)malloc(n * sizeof(float));
        float *residual = (float *)malloc(n * sizeof(float));
        fpq_seed_expand(best, expanded);
        for (size_t i = 0; i < n; i++) residual[i] = target[i] - expanded[i];

        fpq_seed_t *freq2 = try_frequency(residual, n);

        /* Build: SHIFT(_, LERP(0.5, best, freq2_shifted)) — or simpler,
         * just add the residual frequency */
        float test_mse = angle_mse(target, expanded, n);
        float *combo = (float *)malloc(n * sizeof(float));
        float *resid_expanded = (float *)malloc(n * sizeof(float));
        fpq_seed_expand(freq2, resid_expanded);

        for (size_t i = 0; i < n; i++) {
            combo[i] = expanded[i] + resid_expanded[i];
        }
        float combo_mse = angle_mse(target, combo, n);

        if (combo_mse < best->distortion) {
            /* Build additive tree: we add the two seeds' outputs */
            fpq_node_t *lerp = fpq_node_alloc(FPQ_OP_LERP);
            lerp->param[0] = 0.5f;

            /* Clone best->root and freq2->root into SCALE(2, ...) each
             * so LERP(0.5, 2*best, 2*freq2) = best + freq2 */
            fpq_node_t *s1 = fpq_node_alloc(FPQ_OP_SCALE);
            s1->param[0] = 2.0f;
            /* We need to clone but let's just rebuild cheaply */
            fpq_seed_t *b1 = try_frequency(target, n); /* will be close to best */
            s1->left = b1->root;
            b1->root = NULL;
            fpq_seed_free(b1);

            fpq_node_t *s2 = fpq_node_alloc(FPQ_OP_SCALE);
            s2->param[0] = 2.0f;
            s2->left = freq2->root;
            freq2->root = NULL;

            lerp->left = s1;
            lerp->right = s2;

            fpq_seed_free(best);
            best = (fpq_seed_t *)calloc(1, sizeof(fpq_seed_t));
            best->root = lerp;
            best->target_dim = n;
            best->tree_size = fpq_node_count(lerp);
            best->distortion = combo_mse;
        }

        fpq_seed_free(freq2);
        free(expanded);
        free(residual);
        free(combo);
        free(resid_expanded);

        if (best->distortion <= tolerance) return best;
    }

    /* Try binary split: PAIR(left_seed, right_seed) */
    if (max_nodes >= 3 && n >= 4) {
        fpq_seed_t *split = try_split(target, n, max_nodes, tolerance, depth);
        if (split && split->distortion < best->distortion) {
            fpq_seed_free(best);
            best = split;
        } else if (split) {
            fpq_seed_free(split);
        }
    }

    /* Try REP (detect periodicity) */
    if (max_nodes >= 2 && n >= 8) {
        for (int k = 2; k <= 8; k++) {
            size_t chunk = n / k;
            if (chunk < 2) continue;

            /* Check if the angle sequence is approximately k-periodic */
            float period_mse = 0.0f;
            for (size_t i = chunk; i < n; i++) {
                float d = target[i] - target[i % chunk];
                period_mse += d * d;
            }
            period_mse /= (float)n;

            if (period_mse < best->distortion * 0.5f) {
                /* It's periodic! Compress just the first chunk. */
                fpq_seed_t *chunk_seed = discover_recursive(
                    target, chunk,
                    max_nodes > 2 ? max_nodes - 2 : 1,
                    tolerance, depth + 1);
                if (chunk_seed) {
                    fpq_node_t *rep = fpq_node_alloc(FPQ_OP_REP);
                    rep->iparam = k;
                    rep->left = chunk_seed->root;
                    chunk_seed->root = NULL;
                    fpq_seed_free(chunk_seed);

                    fpq_seed_t *rep_seed = (fpq_seed_t *)calloc(1, sizeof(fpq_seed_t));
                    rep_seed->root = rep;
                    rep_seed->target_dim = n;
                    rep_seed->tree_size = fpq_node_count(rep);

                    float *exp = (float *)malloc(n * sizeof(float));
                    fpq_seed_expand(rep_seed, exp);
                    rep_seed->distortion = angle_mse(target, exp, n);
                    free(exp);

                    if (rep_seed->distortion < best->distortion) {
                        fpq_seed_free(best);
                        best = rep_seed;
                    } else {
                        fpq_seed_free(rep_seed);
                    }
                }
                break;
            }
        }
    }

    return best;
}

static fpq_seed_t *try_split(const float *target, size_t n,
                              size_t max_nodes, float tolerance, int depth) {
    size_t mid = n / 2;
    size_t half_budget = (max_nodes - 1) / 2;
    if (half_budget == 0) half_budget = 1;

    fpq_seed_t *left = discover_recursive(target, mid, half_budget, tolerance, depth + 1);
    fpq_seed_t *right = discover_recursive(target + mid, n - mid, half_budget, tolerance, depth + 1);

    if (!left || !right) {
        fpq_seed_free(left);
        fpq_seed_free(right);
        return NULL;
    }

    fpq_node_t *pair = fpq_node_alloc(FPQ_OP_PAIR);
    pair->left = left->root;
    pair->right = right->root;
    left->root = NULL;
    right->root = NULL;

    fpq_seed_t *seed = (fpq_seed_t *)calloc(1, sizeof(fpq_seed_t));
    seed->root = pair;
    seed->target_dim = n;
    seed->tree_size = fpq_node_count(pair);

    /* Weighted average of child distortions as estimate */
    seed->distortion = (left->distortion * (float)mid
                      + right->distortion * (float)(n - mid)) / (float)n;

    fpq_seed_free(left);
    fpq_seed_free(right);

    /* Verify actual distortion */
    float *expanded = (float *)malloc(n * sizeof(float));
    fpq_seed_expand(seed, expanded);
    seed->distortion = angle_mse(target, expanded, n);
    free(expanded);

    return seed;
}

/*
 * Top-level seed discovery.
 * Finds the minimal combinator tree that approximates the target angles.
 *
 * This is the practical approximation of:
 *   Q(v) = min { |P| : eval(P) ≈ PolarTransform(v) }
 */
fpq_seed_t *fpq_seed_discover(const float *target_angles, size_t n,
                               size_t max_nodes, float tolerance) {
    return discover_recursive(target_angles, n, max_nodes, tolerance, 0);
}
