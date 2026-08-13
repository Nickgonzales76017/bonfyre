/*
 * bonfyre-leapfrog — Hamiltonian conservation and reversibility test
 *
 * Tests the symplectic leapfrog integrator directly — no circuit, no
 * HE-SLI force, no mount changes, no momentum kick after init.
 *
 * Protocol:
 *   1. Seed q from pack vector 0 (L2-normalised corpus point).
 *      Seed p from pack vector 1 scaled to p_scale (non-zero initial momentum).
 *   2. Save (q_0, p_0, H_0).
 *   3. Forward pass: N steps with dt > 0.
 *      Record H_t at every step.
 *   4. Report max |H_t - H_0| and mean |H_t - H_0| over the forward pass.
 *   5. Reverse pass: negate dt, run N steps from (q_N, p_N).
 *      This should return to (q_0, p_0) under exact arithmetic.
 *   6. Report reversibility error: ‖q_rev - q_0‖₂ and ‖p_rev - p_0‖₂.
 *   7. Repeat for two dt values to show O(dt²) drift scaling.
 *
 * Build:
 *   make -C cmd/BonfyreLeapfrog
 *
 * Uses the pack/BVH produced by bonfyre-violence (or builds them fresh).
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <dirent.h>
#include <bonfyre.h>

/* ── config ─────────────────────────────────────────────────────────── */
#define EMBED_DIM     32u
#define N_VECS        128u
#define N_STEPS       1000
#define SIGMA         0.5f
#define P_SCALE       0.3f          /* initial momentum magnitude              */
static const char *pack_path = "/tmp/bonfyre-violence.bfpk";
static const char *bvh_path = "/tmp/bonfyre-violence.bfvh";
#define REAL_DATA_DEFAULT "real-data"
/* Override corpus path via BF_REAL_DATA env var, fallback to ./real-data */
static const char *real_data_path(void) {
    const char *e = getenv("BF_REAL_DATA");
    return e ? e : REAL_DATA_DEFAULT;
}
/* dt values to sweep — shows O(dt²) scaling when halved */
static const float DT_VALUES[] = { 0.05f, 0.01f };
#define N_DT_VALUES   2

/* ── embed generation (same as violence) ─────────────────────────────── */
static void hash_to_vec(const uint8_t h[32], float *out, uint32_t dim) {
    for (uint32_t i = 0; i < dim; i++)
        out[i] = ((float)(int8_t)h[i % 32]) / 128.0f;
    float norm = 0.0f;
    for (uint32_t i = 0; i < dim; i++) norm += out[i] * out[i];
    norm = sqrtf(norm);
    if (norm > 1e-6f)
        for (uint32_t i = 0; i < dim; i++) out[i] /= norm;
}

static int file_sha256(const char *path, uint8_t hash[32]) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    BfSha256 s; bf_sha256_init(&s);
    bf_sha256_update(&s, (const uint8_t *)buf, n);
    bf_sha256_final(&s, hash);
    return 0;
}

static int build_corpus(void) {
    DIR *d = opendir(real_data_path());
    if (!d) { fprintf(stderr, "leapfrog: cannot open %s\n", real_data_path()); return -1; }
    uint32_t n = 0;
    struct dirent *ent;
    float vec[EMBED_DIM];
    while ((ent = readdir(d)) != NULL && n < N_VECS) {
        if (ent->d_name[0] == '.') continue;
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", real_data_path(), ent->d_name);
        uint8_t hash[32];
        if (file_sha256(path, hash) != 0) continue;
        hash_to_vec(hash, vec, EMBED_DIM);
        bf_embed_store(hash, vec, EMBED_DIM);
        n++;
    }
    closedir(d);
    if (n == 0) { fprintf(stderr, "leapfrog: no files in %s\n", real_data_path()); return -1; }
    printf("  corpus: %u embeddings (dim=%u)\n", n, EMBED_DIM);
    return 0;
}

/* ── vector utilities ─────────────────────────────────────────────────── */
static float vec_norm2(const float *v, uint32_t dim) {
    float s = 0.0f;
    for (uint32_t i = 0; i < dim; i++) s += v[i] * v[i];
    return sqrtf(s);
}

static float vec_dist2(const float *a, const float *b, uint32_t dim) {
    float s = 0.0f;
    for (uint32_t i = 0; i < dim; i++) { float d = a[i]-b[i]; s += d*d; }
    return sqrtf(s);
}

/* ── single dt trial ─────────────────────────────────────────────────── */
static void run_trial(const BfEmbedPack *pack, const BfEmbedBVH *bvh, float dt) {
    const char *sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
    printf("\n%s\n", sep);
    printf("  dt = %.4f   N = %d steps\n", dt, N_STEPS);
    printf("%s\n", sep);

    BfPhysicsState *ps = bf_physics_state_alloc(EMBED_DIM, SIGMA, dt);
    if (!ps) { fprintf(stderr, "leapfrog: alloc failed\n"); return; }

    /* seed q from pack[0] */
    float q0[EMBED_DIM], p0[EMBED_DIM];
    if (bf_embed_pack_vec_at(pack, 0, q0) != 0) {
        fprintf(stderr, "leapfrog: vec_at 0 failed\n"); bf_physics_state_free(ps); return;
    }
    memcpy(ps->q, q0, EMBED_DIM * sizeof(float));

    /* seed p from pack[1], scaled to P_SCALE */
    float p_raw[EMBED_DIM];
    if (bf_embed_pack_vec_at(pack, 1, p_raw) != 0) {
        fprintf(stderr, "leapfrog: vec_at 1 failed\n"); bf_physics_state_free(ps); return;
    }
    float pnorm = vec_norm2(p_raw, EMBED_DIM);
    if (pnorm < 1e-9f) pnorm = 1.0f;
    for (uint32_t d = 0; d < EMBED_DIM; d++)
        ps->p[d] = p_raw[d] / pnorm * P_SCALE;
    memcpy(p0, ps->p, EMBED_DIM * sizeof(float));

    float H0 = bf_physics_hamiltonian(ps, bvh, pack);
    printf("  H_0        : %+.8f\n", H0);
    printf("  ‖q_0‖      : %.6f\n",  vec_norm2(ps->q, EMBED_DIM));
    printf("  ‖p_0‖      : %.6f\n",  vec_norm2(ps->p, EMBED_DIM));

    /* ── forward pass ────────────────────────────────────────────── */
    printf("\n  ── Forward %d steps ──\n", N_STEPS);
    printf("  %6s   %14s   %14s\n", "step", "H_t", "H_t - H_0");

    float *H_hist = malloc(N_STEPS * sizeof(float));
    if (!H_hist) { bf_physics_state_free(ps); return; }

    float max_dH = 0.0f, sum_dH = 0.0f;
    int report_every = N_STEPS / 10;  /* print 10 checkpoints */

    for (int i = 0; i < N_STEPS; i++) {
        int rc = bf_physics_step(ps, bvh, pack);
        float H = bf_physics_hamiltonian(ps, bvh, pack);
        H_hist[i] = H;
        float dH = fabsf(H - H0);
        if (dH > max_dH) max_dH = dH;
        sum_dH += dH;
        if ((i + 1) % report_every == 0 || i == 0) {
            printf("  %6d   %+14.8f   %+14.8f%s\n",
                   i + 1, H, H - H0,
                   rc == 1 ? " [GAP]" : "");
        }
    }

    float H_final = H_hist[N_STEPS - 1];
    printf("\n  ── Hamiltonian drift ──\n");
    printf("  H_final    : %+.8f\n",  H_final);
    printf("  max |ΔH|   : %.8f\n",   max_dH);
    printf("  mean |ΔH|  : %.8f\n",   sum_dH / N_STEPS);
    free(H_hist);

    /* ── reverse pass ────────────────────────────────────────────── */
    printf("\n  ── Reverse %d steps (dt = %.4f → %.4f) ──\n",
           N_STEPS, dt, -dt);
    ps->dt = -dt;  /* negate timestep; leapfrog is time-reversible */

    float max_dH_rev = 0.0f, sum_dH_rev = 0.0f;
    for (int i = 0; i < N_STEPS; i++) {
        bf_physics_step(ps, bvh, pack);
        float H = bf_physics_hamiltonian(ps, bvh, pack);
        float dH = fabsf(H - H0);
        if (dH > max_dH_rev) max_dH_rev = dH;
        sum_dH_rev += dH;
    }
    ps->dt = dt;  /* restore for clean reporting */

    float eq = vec_dist2(ps->q, q0, EMBED_DIM);
    float ep = vec_dist2(ps->p, p0, EMBED_DIM);

    printf("  max |ΔH| (rev) : %.8f\n",   max_dH_rev);
    printf("  mean |ΔH| (rev): %.8f\n",   sum_dH_rev / N_STEPS);
    printf("\n  ── Reversibility ──\n");
    printf("  ‖q_rev - q_0‖  : %.2e\n", eq);
    printf("  ‖p_rev - p_0‖  : %.2e\n", ep);
    printf("  ‖p_rev‖        : %.6f  (should match ‖p_0‖ = %.6f)\n",
           vec_norm2(ps->p, EMBED_DIM), vec_norm2(p0, EMBED_DIM));

    bf_physics_state_free(ps);
}

/* ── main ─────────────────────────────────────────────────────────────── */
int main(int argc, char **argv) {
    for (int index = 1; index < argc; index++) {
        if (strcmp(argv[index], "--data") == 0 && index + 1 < argc) {
            setenv("BF_REAL_DATA", argv[++index], 1);
        } else if (strcmp(argv[index], "--pack") == 0 && index + 1 < argc) {
            pack_path = argv[++index];
        } else if (strcmp(argv[index], "--bvh") == 0 && index + 1 < argc) {
            bvh_path = argv[++index];
        } else {
            fprintf(stderr, "usage: bonfyre-leapfrog [--data DIR] [--pack FILE] [--bvh FILE]\n");
            return 1;
        }
    }
    const char *sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
    printf("\n%s\n", sep);
    printf("  BONFYRE LEAPFROG TEST\n");
    printf("  Hamiltonian conservation + reversibility\n");
    printf("%s\n\n", sep);

    bf_component_registry_init();

    /* ── ensure pack + BVH exist ── */
    {
        FILE *f = fopen(pack_path, "rb");
        if (!f) {
            printf("  building corpus (pack/BVH not found)...\n");
            if (build_corpus() != 0) return 1;
            uint32_t n_packed = 0;
            if (bf_embed_pack_build(pack_path, &n_packed) != 0) {
                fprintf(stderr, "leapfrog: pack build failed\n"); return 1;
            }
            if (bf_embed_bvh_build(pack_path, bvh_path) != 0) {
                fprintf(stderr, "leapfrog: bvh build failed\n"); return 1;
            }
            printf("  pack built: %u vecs → %s\n", n_packed, pack_path);
            printf("  BVH  built: %s\n", bvh_path);
        } else {
            fclose(f);
            printf("  reusing existing pack: %s\n", pack_path);
        }
    }

    BfEmbedPack pack = {0};
    BfEmbedBVH  bvh  = {0};
    if (bf_embed_pack_open(&pack, pack_path) != 0) {
        fprintf(stderr, "leapfrog: cannot open pack\n"); return 1;
    }
    if (bf_embed_bvh_open(&bvh, bvh_path) != 0) {
        fprintf(stderr, "leapfrog: cannot open BVH\n");
        bf_embed_pack_close(&pack); return 1;
    }
    printf("  pack: n=%u dim=%u\n", pack.n, pack.dim);
    printf("  BVH : n_nodes=%u\n", bvh.n_nodes);
    printf("  sigma=%.3f  p_scale=%.3f\n", SIGMA, P_SCALE);

    /* ── run trials at multiple dt to demonstrate O(dt²) scaling ── */
    for (int i = 0; i < N_DT_VALUES; i++)
        run_trial(&pack, &bvh, DT_VALUES[i]);

    printf("\n%s\n", sep);
    printf("  If reversibility error scales as dt² (ratio ≈ 25x when\n");
    printf("  dt goes from 0.05 → 0.01), the leapfrog claim is credible.\n");
    printf("%s\n\n", sep);

    bf_embed_bvh_close(&bvh);
    bf_embed_pack_close(&pack);
    return 0;
}
