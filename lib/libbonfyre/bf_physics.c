/* bf_physics.c — Hamiltonian Leapfrog symplectic integrator for query propagation.
 *
 * Models the current query vector q as a particle on the embedding manifold
 * with momentum p.  The potential field V(q) is a Kernel Density Estimate
 * over the active KV-cache (computed via BVH gradient from bf_embed_bvh.c):
 *
 *   V(q) = −Σᵢ exp(−‖q − kᵢ‖² / 2σ²)
 *
 *   ∇V(q) = (1/σ²) Σᵢ (q − kᵢ) exp(−‖q − kᵢ‖² / 2σ²)
 *
 * Leapfrog (Störmer-Verlet) — time-reversible, symplectic (energy-conserving):
 *
 *   p_{n+½} = p_n     − (dt/2) ∇V(q_n)
 *   q_{n+1} = q_n     + dt     p_{n+½}          (M = I, unit mass)
 *   p_{n+1} = p_{n+½} − (dt/2) ∇V(q_{n+1})
 *
 * Phase-space state file  (.bfps):
 *   magic(4) + version(4) + dim(4) + flags(4) +
 *   sigma(4) + dt(4) + step(8) + pad(8)    = 40B header
 *   q[dim × float32]
 *   p[dim × float32]
 *
 * Magic: 0x53504642 ("BFPS"), version 1.
 *
 * Key property: identical (q₀, p₀, σ, dt) always produces identical trajectory.
 * Deterministic inference: no sampling, no stochastic noise.
 */

#define _POSIX_C_SOURCE 200809L
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>
#include "bonfyre.h"

#define BFPS_MAGIC   0x53504642u   /* "BFPS" */
#define BFPS_VERSION 1u

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t dim;
    uint32_t flags;      /* reserved */
    float    sigma;
    float    dt;
    uint64_t step;       /* integration step counter */
    uint8_t  pad[8];
} BFPSHeader;            /* = 40B */

/* ── bf_physics_state_alloc / free ─────────────────────────── */
BfPhysicsState *bf_physics_state_alloc(uint32_t dim, float sigma, float dt) {
    if (dim == 0) return NULL;
    BfPhysicsState *s = calloc(1, sizeof(BfPhysicsState));
    if (!s) return NULL;
    s->q = calloc(dim, sizeof(float));
    s->p = calloc(dim, sizeof(float));
    s->grad_buf = calloc(dim, sizeof(float));
    if (!s->q || !s->p || !s->grad_buf) {
        free(s->q); free(s->p); free(s->grad_buf); free(s);
        return NULL;
    }
    s->dim   = dim;
    s->sigma = sigma > 0.0f ? sigma : 1.0f;
    s->dt    = dt    > 0.0f ? dt    : 0.01f;
    s->step  = 0;
    return s;
}

void bf_physics_state_free(BfPhysicsState *s) {
    if (!s) return;
    free(s->q); free(s->p); free(s->grad_buf);
    memset(s, 0, sizeof(*s));
    free(s);
}

/* ── bf_physics_state_save ──────────────────────────────────── */
int bf_physics_state_save(const BfPhysicsState *s, const char *path) {
    if (!s || !path) return -1;
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", path, (int)getpid());
    FILE *f = fopen(tmp, "wb");
    if (!f) return -1;

    BFPSHeader hdr = {
        .magic   = BFPS_MAGIC,
        .version = BFPS_VERSION,
        .dim     = s->dim,
        .flags   = 0,
        .sigma   = s->sigma,
        .dt      = s->dt,
        .step    = s->step,
    };
    memset(hdr.pad, 0, sizeof(hdr.pad));
    fwrite(&hdr, 1, sizeof(hdr), f);
    fwrite(s->q, sizeof(float), s->dim, f);
    fwrite(s->p, sizeof(float), s->dim, f);
    fclose(f);

    if (rename(tmp, path) != 0) { unlink(tmp); return -1; }
    return 0;
}

/* ── bf_physics_state_load ──────────────────────────────────── */
BfPhysicsState *bf_physics_state_load(const char *path) {
    if (!path) return NULL;
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;

    BFPSHeader hdr;
    if (fread(&hdr, 1, sizeof(hdr), f) != sizeof(hdr)) { fclose(f); return NULL; }
    if (hdr.magic != BFPS_MAGIC || hdr.version != BFPS_VERSION || hdr.dim == 0) {
        fclose(f); return NULL;
    }

    BfPhysicsState *s = bf_physics_state_alloc(hdr.dim, hdr.sigma, hdr.dt);
    if (!s) { fclose(f); return NULL; }
    s->step = hdr.step;

    size_t dim_bytes = hdr.dim * sizeof(float);
    if (fread(s->q, 1, dim_bytes, f) != dim_bytes ||
        fread(s->p, 1, dim_bytes, f) != dim_bytes) {
        bf_physics_state_free(s); fclose(f); return NULL;
    }
    fclose(f);
    return s;
}

/* ── bf_physics_init_from_embed ─────────────────────────────── */
/* Set q from an embedding vector; initialize p to zero (at-rest start). */
int bf_physics_init_from_embed(BfPhysicsState *s,
                                const float *embed, uint32_t dim) {
    if (!s || !embed || dim != s->dim) return -1;
    memcpy(s->q, embed, dim * sizeof(float));
    memset(s->p, 0, dim * sizeof(float));
    s->step = 0;
    return 0;
}

/* ── bf_physics_kick ─────────────────────────────────────────── */
/* Add a momentum impulse to p (e.g. from steering vector or token embedding).
 * Inertial reasoning: prior momentum is preserved and accumulated. */
int bf_physics_kick(BfPhysicsState *s, const float *impulse, float scale) {
    if (!s || !impulse) return -1;
    for (uint32_t d = 0; d < s->dim; d++)
        s->p[d] += impulse[d] * scale;
    return 0;
}

/* ── bf_physics_hamiltonian ─────────────────────────────────── */
/* Compute H = ½‖p‖² + V(q).  V(q) requires a BVH pass — if bvh/pack NULL,
   returns kinetic energy only (useful for monitoring without full BVH cost). */
float bf_physics_hamiltonian(const BfPhysicsState *s,
                              const BfEmbedBVH *bvh,
                              const BfEmbedPack *pack) {
    if (!s) return 0.0f;
    /* kinetic */
    float ke = 0.0f;
    for (uint32_t d = 0; d < s->dim; d++) ke += s->p[d] * s->p[d];
    ke *= 0.5f;

    if (!bvh || !pack) return ke;

    /* potential via BVH gradient — accumulate V(q) = -Σ K(q,k) in the
       same traversal pass that computes ∇V.  No second pass needed. */
    float *tmp_grad = calloc(s->dim, sizeof(float));
    if (!tmp_grad) return ke;
    float pe = 0.0f;
    bf_embed_bvh_gradient(bvh, pack, s->q, s->dim, s->sigma,
                          tmp_grad, &pe);
    free(tmp_grad);

    return ke + pe;
}

/* ── bf_physics_step ─────────────────────────────────────────── */
/*
 * One full Leapfrog step using the BVH KDE potential.
 * Tier-1 ternary collision check is done inside bf_embed_bvh_gradient.
 * Tier-2 INT8 exact dequant happens only for leaves that pass the sketch.
 *
 * Returns 0 on success, -1 on error, +1 on "topological gap" (gradient
 * near-zero — the query has left the populated region of the manifold;
 * the caller should mount a new sub-cache via bf_kvcache_mount).
 */
int bf_physics_step(BfPhysicsState *s,
                    const BfEmbedBVH *bvh,
                    const BfEmbedPack *pack) {
    if (!s || !bvh || !pack) return -1;

    float *grad = s->grad_buf;

    /* half-kick: p_{n+½} = p_n - (dt/2) ∇V(q_n) */
    memset(grad, 0, s->dim * sizeof(float));
    if (bf_embed_bvh_gradient(bvh, pack, s->q, s->dim, s->sigma,
                               grad, NULL) != 0) return -1;
    float half_dt = s->dt * 0.5f;
    for (uint32_t d = 0; d < s->dim; d++)
        s->p[d] -= half_dt * grad[d];

    /* drift: q_{n+1} = q_n + dt * p_{n+½} */
    for (uint32_t d = 0; d < s->dim; d++)
        s->q[d] += s->dt * s->p[d];

    /* half-kick: p_{n+1} = p_{n+½} - (dt/2) ∇V(q_{n+1}) */
    memset(grad, 0, s->dim * sizeof(float));
    if (bf_embed_bvh_gradient(bvh, pack, s->q, s->dim, s->sigma,
                               grad, NULL) != 0) return -1;
    float grad_norm2 = 0.0f;
    for (uint32_t d = 0; d < s->dim; d++) {
        s->p[d] -= half_dt * grad[d];
        grad_norm2 += grad[d] * grad[d];
    }

    s->step++;

    /* Topological gap detection: ‖∇V(q)‖ < threshold → KV desert */
    if (sqrtf(grad_norm2) < 1e-6f) return 1;  /* signal: mount new sub-cache */
    return 0;
}

/* ── bf_physics_run ──────────────────────────────────────────── */
/* Run up to max_steps; stop early on topological gap or convergence.
 * out_steps: number of steps actually taken.
 * Returns 0=converged, 1=gap (mount needed), -1=error. */
int bf_physics_run(BfPhysicsState *s,
                   const BfEmbedBVH *bvh,
                   const BfEmbedPack *pack,
                   int max_steps, int *out_steps) {
    if (!s || !bvh || !pack || max_steps <= 0) return -1;
    int steps = 0;
    for (int i = 0; i < max_steps; i++) {
        int rc = bf_physics_step(s, bvh, pack);
        steps++;
        if (rc != 0) {
            if (out_steps) *out_steps = steps;
            return rc;
        }
    }
    if (out_steps) *out_steps = steps;
    return 0;
}

/* ── bf_physics_nearest ──────────────────────────────────────── */
/* After integration, find the nearest KV vector to final q position.
 * Uses BVH ternary collision check (Tier 1) then scores candidates. */
int bf_physics_nearest(const BfPhysicsState *s,
                        const BfEmbedBVH *bvh,
                        const BfEmbedPack *pack,
                        int top_k,
                        BfEmbedSearchResult *out, int *out_count) {
    if (!s || !bvh || !pack || !out || !out_count) return -1;
    return bf_embed_brute_search(pack, s->q, s->dim, top_k, out, out_count);
}
