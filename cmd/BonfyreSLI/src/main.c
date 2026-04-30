/*
 * bonfyre-sli — Structured Layer Inference
 *
 * Executes a sequence of quantized BQFP transform fragments in order,
 * routing input through the best-matching family (via bonfyre-model route)
 * and optionally applying cross-family alignment (bonfyre-fpqx).
 *
 * SLI operates at the embedding level: it takes float32 embeddings as input,
 * applies the decompressed transform weights (reconstructed from BQFP tiles),
 * and produces transformed embeddings as output.
 *
 * Usage:
 *   bonfyre-sli run  --in vectors.bin --family T16 --model <model.bqfp> --out output.bin
 *   bonfyre-sli chain --in vectors.bin --chain T04:T16 --models-dir DIR --out output.bin
 *   bonfyre-sli route --in vectors.bin --stats corpus_stats.json --models-dir DIR --out output.bin
 *   bonfyre-sli bench --model <model.bqfp> --n 1000
 *   bonfyre-sli inspect --model <model.bqfp>
 *   bonfyre-sli --help
 *
 * Input format (vectors.bin):
 *   uint32 n_vectors
 *   uint32 dim
 *   float32[n_vectors × dim]
 *
 * Output format: same
 *
 * Transform application:
 *   For each encoded block in the BQFP:
 *     decoded = decode_block(qblocks, tiles, dim)
 *   The decoded weight matrix is applied as a linear transform:
 *     out_vec = W × in_vec  (first tensor treated as projection matrix)
 *
 * Routing:
 *   bonfyre-model route <corpus_stats.json> → family ID
 *   Load <models-dir>/<family>.bqfp
 *
 * Chain:
 *   Apply T04 transform then T16 transform sequentially.
 *   Optionally with FPQx alignment matrix between steps.
 */
#include <bonfyre.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>

#define VERSION    "1.1.0"
#define BLOCK_DIM  256
#define TILE_DIM   16
#define E8_PAIRS   16
#define MAX_PATH   4096
#define MAX_LOOP   32
#define MU_BETA    8.0f
#define BQFP_MAGIC 0x50464251u
#define FPQX_MAGIC 0x58515046u

/* ═══════════════════════════════════════════════════════════════════
 * μ-law / FWHT / E8 reconstruction (mirrors bonfyre-quant)
 * ═══════════════════════════════════════════════════════════════════ */

static float mu_unwarp(float y, float beta) {
    float sign = (y >= 0) ? 1.0f : -1.0f;
    return sign * (expf(fabsf(y) * logf(1.0f + beta)) - 1.0f) / beta;
}

static uint64_t xorshift64(uint64_t s) {
    s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s;
}

static void random_signs_vec(float *d, int n, uint64_t seed) {
    uint64_t s = seed;
    for (int i = 0; i < n; i++) { s = xorshift64(s); d[i] *= (s & 1) ? 1.0f : -1.0f; }
}

static void fwht(float *d, int n) {
    for (int len = 1; len < n; len <<= 1)
        for (int i = 0; i < n; i += len<<1)
            for (int j = 0; j < len; j++) {
                float a = d[i+j], b = d[i+j+len];
                d[i+j] = a+b; d[i+j+len] = a-b;
            }
    float norm = 1.0f / sqrtf((float)n);
    for (int i = 0; i < n; i++) d[i] *= norm;
}

/* ═══════════════════════════════════════════════════════════════════
 * BQFP block structure (must match bonfyre-quant)
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float   scale;
    float   warp_norm;
    int8_t  e8i[BLOCK_DIM];
    uint8_t tile_idx[E8_PAIRS];
} BqfpBlock;

/* Decode a single BQFP block back to float32 */
static void decode_bqfp_block(const BqfpBlock *blk, const float *tiles, int eff_k,
                                float *out, size_t dim,
                                uint64_t haar_seed, float lattice_scale) {
    float corrected[BLOCK_DIM] = {0};
    float e8_scale = lattice_scale / 8.0f / 2.0f; /* reverse of e8i encoding */

    for (int p = 0; p < E8_PAIRS; p++) {
        int ti = blk->tile_idx[p];
        const float *tile = (ti < eff_k) ? tiles + ti * TILE_DIM : NULL;
        for (int d = 0; d < TILE_DIM; d++) {
            float e8_val = (float)blk->e8i[p * TILE_DIM + d] * e8_scale;
            corrected[p * TILE_DIM + d] = e8_val + (tile ? tile[d] : 0.0f);
        }
    }

    /* Inverse scale + unwarp */
    for (int i = 0; i < BLOCK_DIM; i++) {
        float lat_val = corrected[i] / lattice_scale * blk->warp_norm;
        corrected[i] = mu_unwarp(lat_val, MU_BETA) * blk->scale;
    }

    /* Inverse FWHT + undo signs */
    fwht(corrected, BLOCK_DIM);
    random_signs_vec(corrected, BLOCK_DIM, haar_seed);

    size_t n = (dim < BLOCK_DIM) ? dim : BLOCK_DIM;
    memcpy(out, corrected, n * sizeof(float));
}

/* ═══════════════════════════════════════════════════════════════════
 * BQFP tensor: loads first tensor from a BQFP file into float32 weights
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    float   *weights;     /* reconstructed float32 */
    size_t   n_elements;
    int      bits;
    int      eff_k;
    size_t   n_blocks;
    char     name[256];
} SliTensor;

static int load_bqfp_first_tensor(const char *path, SliTensor *t) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return 1; }

    uint32_t hdr[4];
    if (fread(hdr, 4, 4, f) != 4 || hdr[0] != BQFP_MAGIC) {
        fprintf(stderr, "sli: not a BQFP file: %s\n", path);
        fclose(f); return 1;
    }
    t->bits = (int)hdr[3];
    float lattice_scale = 8.0f * (float)t->bits;

    /* Read first tensor */
    char name[256];
    uint64_t n_elements, n_blocks;
    uint32_t eff_k, _pad;
    if (fread(name, 1, 256, f) != 256 ||
        fread(&n_elements, 8, 1, f) != 1 ||
        fread(&n_blocks, 8, 1, f) != 1 ||
        fread(&eff_k, 4, 1, f) != 1 ||
        fread(&_pad, 4, 1, f) != 1) {
        fclose(f); return 1;
    }
    memcpy(t->name, name, 256);
    t->n_elements = (size_t)n_elements;
    t->n_blocks   = (size_t)n_blocks;
    t->eff_k      = (int)eff_k;

    /* Read codebook */
    size_t cb_n = (size_t)eff_k * TILE_DIM;
    float *tiles = (float *)malloc(cb_n * sizeof(float));
    if (!tiles || fread(tiles, sizeof(float), cb_n, f) != cb_n) {
        free(tiles); fclose(f); return 1;
    }

    /* Read blocks and decode to float32 */
    t->weights = (float *)malloc(n_elements * sizeof(float));
    if (!t->weights) { free(tiles); fclose(f); return 1; }

    uint64_t haar_seed = 0x12345678ULL;
    for (const char *p = name; *p; p++)
        haar_seed = haar_seed * 31 + (uint64_t)(unsigned char)*p;

    for (size_t b = 0; b < n_blocks; b++) {
        BqfpBlock blk;
        if (fread(&blk, sizeof(BqfpBlock), 1, f) != 1) break;
        size_t off = b * BLOCK_DIM;
        size_t dim = (off + BLOCK_DIM <= n_elements) ? BLOCK_DIM : (n_elements - off);
        decode_bqfp_block(&blk, tiles, (int)eff_k, t->weights + off, dim,
                          haar_seed ^ (uint64_t)b, lattice_scale);
    }

    free(tiles); fclose(f);
    return 0;
}

static void sli_tensor_free(SliTensor *t) { free(t->weights); t->weights = NULL; }

/* ═══════════════════════════════════════════════════════════════════
 * Vector file I/O
 * vectors.bin: [n_vecs:u32][dim:u32][float32 × n_vecs × dim]
 * ═══════════════════════════════════════════════════════════════════ */

static int load_vectors(const char *path, float **vecs, uint32_t *n_vecs, uint32_t *dim) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return 1; }
    if (fread(n_vecs, 4, 1, f) != 1 || fread(dim, 4, 1, f) != 1) { fclose(f); return 1; }
    size_t total = (size_t)(*n_vecs) * (size_t)(*dim);
    *vecs = (float *)malloc(total * sizeof(float));
    if (!*vecs) { fclose(f); return 1; }
    if (fread(*vecs, sizeof(float), total, f) != total) {
        free(*vecs); fclose(f); return 1;
    }
    fclose(f);
    return 0;
}

static int save_vectors(const char *path, const float *vecs, uint32_t n_vecs, uint32_t dim) {
    FILE *f = fopen(path, "wb");
    if (!f) { perror(path); return 1; }
    fwrite(&n_vecs, 4, 1, f);
    fwrite(&dim, 4, 1, f);
    fwrite(vecs, sizeof(float), (size_t)n_vecs * (size_t)dim, f);
    fclose(f);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Transform application: W × v (matrix-vector multiply)
 *   W: rows × cols float32 (decoded from BQFP first tensor)
 *   v: dim-vector
 *   out: rows-vector
 * ═══════════════════════════════════════════════════════════════════ */

static void apply_transform(const float *W, size_t rows, size_t cols,
                              const float *in_vec, float *out_vec) {
    for (size_t r = 0; r < rows; r++) {
        float acc = 0;
        size_t lim = (cols < rows) ? cols : rows; /* use square portion if rectangular */
        (void)lim;
        for (size_t c = 0; c < cols; c++)
            acc += W[r * cols + c] * in_vec[c % cols];  /* safe wrap for dim mismatch */
        out_vec[r] = acc;
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * Commands
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_inspect_model(const char *bqfp_path) {
    FILE *f = fopen(bqfp_path, "rb");
    if (!f) { perror(bqfp_path); return 1; }
    uint32_t hdr[4];
    if (fread(hdr, 4, 4, f) != 4 || hdr[0] != BQFP_MAGIC) {
        fprintf(stderr, "sli inspect: not a BQFP file\n"); fclose(f); return 1;
    }
    printf("BQFP model: %s\n", bqfp_path);
    printf("  version:  %u\n  tensors:  %u\n  bits:     %u\n\n", hdr[1], hdr[2], hdr[3]);
    for (uint32_t t = 0; t < hdr[2]; t++) {
        char name[256]; uint64_t ne, nb; uint32_t ek, pad;
        if (fread(name, 1, 256, f) != 256) break;
        if (fread(&ne, 8, 1, f) != 1 || fread(&nb, 8, 1, f) != 1) break;
        if (fread(&ek, 4, 1, f) != 1 || fread(&pad, 4, 1, f) != 1) break;
        printf("  tensor[%u]: %-50s  %lu elem  eff_k=%u\n",
               t, name[0] ? name : "(anon)", (unsigned long)ne, ek);
        fseek(f, (long)((size_t)ek * TILE_DIM * 4 + nb * sizeof(BqfpBlock)), SEEK_CUR);
    }
    fclose(f);
    return 0;
}

static int cmd_run(const char *in_path, const char *model_path, const char *out_path) {
    printf("sli run: %s  →  %s  →  %s\n", in_path, model_path, out_path);

    float *vecs = NULL; uint32_t n_vecs = 0, dim = 0;
    if (load_vectors(in_path, &vecs, &n_vecs, &dim)) return 1;
    printf("  input:  %u vectors × %u dim\n", n_vecs, dim);

    SliTensor st;
    if (load_bqfp_first_tensor(model_path, &st)) { free(vecs); return 1; }
    printf("  model:  %s  (%zu elements reconstructed)\n",
           st.name[0] ? st.name : "(anon)", st.n_elements);

    /* Treat decoded weights as a dim×dim projection matrix */
    size_t rows = (size_t)dim, cols = (size_t)dim;
    /* If weight tensor is smaller, use what we have as a flat remap */
    if (st.n_elements < rows * cols) {
        rows = cols = (size_t)sqrtf((float)st.n_elements);
        if (rows == 0) rows = cols = 1;
    }

    float *out_vecs = (float *)malloc((size_t)n_vecs * (size_t)dim * sizeof(float));
    if (!out_vecs) { sli_tensor_free(&st); free(vecs); return 1; }

    for (uint32_t v = 0; v < n_vecs; v++) {
        float *inv = vecs + (size_t)v * dim;
        float *outv = out_vecs + (size_t)v * dim;
        if (rows == dim && cols == dim) {
            apply_transform(st.weights, rows, cols, inv, outv);
        } else {
            /* Passthrough with scaling by first decoded weight */
            float scale = (st.n_elements > 0) ? st.weights[0] : 1.0f;
            for (uint32_t d = 0; d < dim; d++) outv[d] = inv[d] * scale;
        }
    }

    int rc = save_vectors(out_path, out_vecs, n_vecs, dim);
    printf("  output: %u vectors × %u dim → %s\n", n_vecs, dim, out_path);

    free(out_vecs); sli_tensor_free(&st); free(vecs);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 * GDN chain step — inline Gated Delta Network for embedding sequences.
 *
 * Interprets the input vector batch [n_vecs, dim] as a temporal sequence
 * (T = n_vecs, K = V = dim, B = 1, H = 1) and applies the GDN delta rule:
 *
 *   H_t = gate * H_{t-1} + beta * (x_t ⊗ (scale * x_t))   [self-attention]
 *   o_t = H_t^T * x_t
 *
 * This contextualises each embedding with a weighted summary of all previous
 * embeddings via an exponentially-decaying associative memory.
 *
 * gate (default 0.9): history retention per step (higher → longer memory)
 * beta (default 0.1/sqrt(dim)): rank-1 update strength
 *
 * Invoked by chain steps named "gdn" in any chain spec, e.g. "T04:gdn:T16".
 * NOTE: O(dim²) state — practical for dim ≤ 1024; refuse above that limit.
 * ═══════════════════════════════════════════════════════════════════ */
static int sli_gdn_step(const char *in_path, const char *out_path,
                         float gdn_gate, float gdn_beta) {
    float   *vecs  = NULL;
    uint32_t n_vecs = 0, dim = 0;
    if (load_vectors(in_path, &vecs, &n_vecs, &dim)) return 1;

    if (dim > 1024) {
        fprintf(stderr, "sli gdn: dim=%u > 1024, state matrix would be %.1f MB "
                        "(use bonfyre-flashqla directly with explicit H/K/V)\n",
                dim, (double)dim * dim * 4 / (1024.0 * 1024.0));
        free(vecs); return 1;
    }

    float scale = 1.0f / sqrtf((float)dim);
    if (gdn_beta < 0.0f) gdn_beta = 0.1f * scale;   /* auto */

    /* State h: [dim, dim] = K × V with K=V=dim */
    size_t   h_sz     = (size_t)dim * dim;
    float   *h        = calloc(h_sz, sizeof(float));
    float   *out_vecs = malloc((size_t)n_vecs * dim * sizeof(float));
    if (!h || !out_vecs) {
        free(h); free(out_vecs); free(vecs);
        fprintf(stderr, "sli gdn: out of memory\n"); return 1;
    }

    for (uint32_t t = 0; t < n_vecs; t++) {
        const float *xt = vecs + (size_t)t * dim;

        /* H = gate * H + beta * xt ⊗ (scale * xt) */
        for (uint32_t ki = 0; ki < dim; ki++) {
            float gkval = gdn_beta * xt[ki] * scale;
            float *hrow = h + (size_t)ki * dim;
            for (uint32_t vi = 0; vi < dim; vi++)
                hrow[vi] = gdn_gate * hrow[vi] + gkval * xt[vi];
        }

        /* o_t = H^T * x_t */
        float *ot = out_vecs + (size_t)t * dim;
        for (uint32_t vi = 0; vi < dim; vi++) {
            float acc = 0.0f;
            for (uint32_t ki = 0; ki < dim; ki++)
                acc += h[(size_t)ki * dim + vi] * xt[ki];
            ot[vi] = acc;
        }
    }

    int rc = save_vectors(out_path, out_vecs, n_vecs, dim);
    printf("  gdn: %u vecs × %u dim  gate=%.3f beta=%.5f scale=%.5f\n",
           n_vecs, dim, (double)gdn_gate, (double)gdn_beta, (double)scale);

    free(h); free(out_vecs); free(vecs);
    return rc;
}

static int cmd_chain(const char *in_path, const char *chain_spec,
                      const char *models_dir, const char *out_path,
                      float gdn_gate, float gdn_beta) {
    /* chain_spec: "T04:T16" or "T04:gdn:T16" → apply steps in order */
    printf("sli chain: %s  [%s]  → %s\n", in_path, chain_spec, out_path);

    char spec[256]; strncpy(spec, chain_spec, sizeof(spec)-1);
    char *families[8]; int n_fam = 0;
    char *tok = strtok(spec, ":,"); /* support both T04:T16 and T04,T16 */
    while (tok && n_fam < 8) { families[n_fam++] = tok; tok = strtok(NULL, ":,"); }

    if (n_fam == 0) {
        fprintf(stderr, "sli chain: empty chain spec '%s'\n", chain_spec); return 1;
    }

    char cur_in[MAX_PATH], cur_out[MAX_PATH];
    strncpy(cur_in, in_path, MAX_PATH-1);

    for (int fi = 0; fi < n_fam; fi++) {
        snprintf(cur_out, sizeof(cur_out), "%s.sli_step%d.bin", out_path, fi);

        /* For last step, write to final output */
        const char *step_out = (fi == n_fam - 1) ? out_path : cur_out;

        if (strcmp(families[fi], "gdn") == 0) {
            /* ── GDN linear-attention step ─────────────────────────────── */
            printf("  step %d/%d: gdn  gate=%.3f beta=%s\n",
                   fi + 1, n_fam,
                   (double)gdn_gate,
                   gdn_beta < 0.0f ? "auto" : "custom");
            if (sli_gdn_step(cur_in, step_out, gdn_gate, gdn_beta)) return 1;
        } else {
            /* ── BQFP transform step ───────────────────────────────────── */
            char model_path[MAX_PATH];
            snprintf(model_path, sizeof(model_path), "%s/%s.bqfp", models_dir, families[fi]);
            printf("  step %d/%d: family=%s  model=%s\n",
                   fi + 1, n_fam, families[fi], model_path);
            if (cmd_run(cur_in, model_path, step_out)) return 1;
        }

        /* Cleanup intermediate */
        if (fi > 0 && strcmp(cur_in, in_path) != 0)
            remove(cur_in);
        strncpy(cur_in, step_out, MAX_PATH-1);
    }
    return 0;
}

static int cmd_route(const char *in_path, const char *stats_path,
                      const char *models_dir, const char *out_path) {
    /* Call bonfyre-model route to pick family, then run that model */
    char model_bin[MAX_PATH] = "bonfyre-model";

    /* Look for bonfyre-model in standard locations */
    const char *candidates[] = {
        "./cmd/BonfyreModel/bonfyre-model",
        "../BonfyreModel/bonfyre-model",
        "bonfyre-model",
        NULL
    };
    for (int i = 0; candidates[i]; i++) {
        struct stat st;
        if (stat(candidates[i], &st) == 0) {
            strncpy(model_bin, candidates[i], MAX_PATH-1);
            break;
        }
    }

    /* Run bonfyre-model route and capture output */
    char cmd[MAX_PATH + 256];
    snprintf(cmd, sizeof(cmd), "'%s' route '%s' 2>/dev/null", model_bin, stats_path);
    FILE *p = popen(cmd, "r");
    if (!p) {
        fprintf(stderr, "sli route: failed to run bonfyre-model route\n"); return 1;
    }
    char line[512] = "";
    if (!fgets(line, sizeof(line), p)) {
        pclose(p); fprintf(stderr, "sli route: no route output\n"); return 1;
    }
    pclose(p);

    /* Parse "model_id=... family=F04 ..." */
    char family[64] = "";
    const char *fp = strstr(line, "family=");
    if (fp) {
        fp += 7;
        size_t i = 0;
        while (*fp && *fp != ' ' && *fp != '\n' && i < sizeof(family)-1)
            family[i++] = *fp++;
        family[i] = '\0';
    }
    if (!family[0]) {
        fprintf(stderr, "sli route: could not parse family from: %s\n", line);
        return 1;
    }
    printf("sli route: selected family=%s\n", family);

    char model_path[MAX_PATH];
    snprintf(model_path, sizeof(model_path), "%s/%s.bqfp", models_dir, family);
    return cmd_run(in_path, model_path, out_path);
}

static int cmd_bench(const char *model_path, int n) {
    printf("sli bench: %s  n=%d\n", model_path, n);

    SliTensor st;
    if (load_bqfp_first_tensor(model_path, &st)) return 1;
    printf("  tensor:  %zu elements  eff_k=%d\n", st.n_elements, st.eff_k);

    /* Synthetic input vectors */
    uint32_t dim = 384;
    float *vec = (float *)malloc(dim * sizeof(float));
    float *out = (float *)malloc(dim * sizeof(float));
    if (!vec || !out) { sli_tensor_free(&st); free(vec); free(out); return 1; }
    for (uint32_t i = 0; i < dim; i++) vec[i] = 0.1f * (float)(i % 10);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    size_t rows = (st.n_elements >= dim * dim) ? (size_t)dim : (size_t)sqrtf((float)st.n_elements);
    for (int iter = 0; iter < n; iter++) {
        apply_transform(st.weights, rows > 0 ? rows : 1, rows > 0 ? rows : 1, vec, out);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    printf("  %d transforms in %.3f s  =  %.1f μs/transform\n",
           n, elapsed, elapsed / n * 1e6);

    free(vec); free(out); sli_tensor_free(&st);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * auto-run helpers
 * ═══════════════════════════════════════════════════════════════════ */

/* cosine similarity between two float vectors */
static float cosine_sim(const float *a, const float *b, size_t dim) {
    double dot = 0.0, na = 0.0, nb = 0.0;
    for (size_t i = 0; i < dim; i++) {
        dot += (double)a[i] * (double)b[i];
        na  += (double)a[i] * (double)a[i];
        nb  += (double)b[i] * (double)b[i];
    }
    if (na < 1e-30 || nb < 1e-30) return 0.0f;
    return (float)(dot / (sqrt(na) * sqrt(nb)));
}

/* mean (1 − cosine_sim) across n vectors — measures how much they changed */
static float cosine_delta_batch(const float *old_v, const float *new_v,
                                 uint32_t n, uint32_t dim) {
    double sum = 0.0;
    for (uint32_t i = 0; i < n; i++)
        sum += 1.0 - (double)cosine_sim(old_v + (size_t)i*dim,
                                         new_v + (size_t)i*dim, dim);
    return (n > 0) ? (float)(sum / n) : 0.0f;
}

/* routing helper — wraps popen(bonfyre-model route), returns family string */
static int sli_route_family(const char *stats_path, char *family_out, size_t fam_len) {
    char model_bin[MAX_PATH] = "bonfyre-model";
    const char *candidates[] = {
        "./cmd/BonfyreModel/bonfyre-model",
        "../BonfyreModel/bonfyre-model",
        "bonfyre-model", NULL
    };
    for (int i = 0; candidates[i]; i++) {
        struct stat st;
        if (stat(candidates[i], &st) == 0) {
            strncpy(model_bin, candidates[i], MAX_PATH-1);
            model_bin[MAX_PATH-1] = '\0';
            break;
        }
    }
    char cmd[MAX_PATH + 256];
    snprintf(cmd, sizeof(cmd), "'%s' route '%s' 2>/dev/null", model_bin, stats_path);
    FILE *p = popen(cmd, "r");
    if (!p) return 1;
    char line[512] = "";
    int ok = (fgets(line, sizeof(line), p) != NULL);
    pclose(p);
    if (!ok) return 1;
    const char *fp = strstr(line, "family=");
    if (!fp) return 1;
    fp += 7;
    size_t i = 0;
    while (*fp && *fp != ' ' && *fp != '\n' && i < fam_len-1)
        family_out[i++] = *fp++;
    family_out[i] = '\0';
    return (i > 0) ? 0 : 1;
}

/* apply a loaded BQFP model to all n_vecs vectors in place (writes to out_vecs) */
static int apply_transform_all(const char *model_path, const float *in_vecs,
                                uint32_t n_vecs, uint32_t dim, float *out_vecs) {
    SliTensor st;
    if (load_bqfp_first_tensor(model_path, &st)) return 1;
    size_t rows = (st.n_elements >= (size_t)dim * dim) ? (size_t)dim
                  : (size_t)sqrtf((float)st.n_elements);
    if (rows < 1) rows = 1;
    for (uint32_t v = 0; v < n_vecs; v++)
        apply_transform(st.weights, rows, rows,
                        in_vecs  + (size_t)v * dim,
                        out_vecs + (size_t)v * dim);
    sli_tensor_free(&st);
    return 0;
}

/* apply 16×16 FPQX alignment matrix tiled across all vectors */
static int apply_fpqx_alignment(float *vecs, uint32_t n_vecs, uint32_t dim,
                                  const char *align_path) {
    FILE *f = fopen(align_path, "rb");
    if (!f) return 1;
    uint32_t magic = 0;
    if (fread(&magic, 4, 1, f) != 1 || magic != FPQX_MAGIC) { fclose(f); return 1; }
    uint32_t hdr[3];
    if (fread(hdr, 4, 3, f) != 3) { fclose(f); return 1; }
    float M[TILE_DIM * TILE_DIM];
    if (fread(M, sizeof(float), TILE_DIM*TILE_DIM, f) != (size_t)(TILE_DIM*TILE_DIM)) {
        fclose(f); return 1;
    }
    fclose(f);
    uint32_t n_tiles = dim / TILE_DIM;
    float tmp[TILE_DIM];
    for (uint32_t v = 0; v < n_vecs; v++) {
        float *vec = vecs + (size_t)v * dim;
        for (uint32_t t = 0; t < n_tiles; t++) {
            float *tile = vec + (size_t)t * TILE_DIM;
            memset(tmp, 0, sizeof(tmp));
            for (int r = 0; r < TILE_DIM; r++)
                for (int c = 0; c < TILE_DIM; c++)
                    tmp[r] += M[r*TILE_DIM + c] * tile[c];
            memcpy(tile, tmp, sizeof(tmp));
        }
    }
    return 0;
}

/* write per-iteration artifact.json */
static void write_iter_artifact(const char *path, int iter, const char *family,
                                  float delta, int converged,
                                  uint32_t n_vecs, uint32_t dim) {
    FILE *f = fopen(path, "w");
    if (!f) return;
    fprintf(f,
        "{\n"
        "  \"bonfyre_artifact\": true,\n"
        "  \"tool\": \"bonfyre-sli\",\n"
        "  \"command\": \"auto-run\",\n"
        "  \"iteration\": %d,\n"
        "  \"family\": \"%s\",\n"
        "  \"cosine_delta\": %.8f,\n"
        "  \"converged\": %s,\n"
        "  \"n_vecs\": %u,\n"
        "  \"dim\": %u\n"
        "}\n",
        iter, family, (double)(delta < 0 ? 0.0f : delta),
        converged ? "true" : "false", n_vecs, dim);
    fclose(f);
}

/* ─────────────────────────────────────────────────────────────────────────
 * cmd_auto_run — routing + transform + FPQx alignment + convergence loop
 *
 * Options:
 *   --in         embeddings.bin       (required)
 *   --stats      corpus_stats.json    (required; drives geometry routing)
 *   --out        results/             (required; creates iter-1/, iter-2/, …)
 *   --loop       N                    (max iterations, default 3)
 *   --chain      auto | fragment:auto | T04:T16 | none
 *                  auto          — re-route each iter
 *                  fragment:auto — iter 1 uses <family>-frag if present, then auto
 *                  T04:T16       — fixed family sequence (wraps)
 *                  none          — always T04
 *   --fpqx       auto | none          (default none — auto loads <prev>-<next>-align.bin)
 *   --thresh     0.001                (stop when mean cosine delta < threshold)
 *   --models-dir DIR                  (where .bqfp files live, default .)
 * ─────────────────────────────────────────────────────────────────────────
 */
static int cmd_auto_run(int argc, char **argv) {
    const char *in_path    = bf_arg_value(argc, argv, "--in");
    const char *stats_path = bf_arg_value(argc, argv, "--stats");
    const char *out_dir    = bf_arg_value(argc, argv, "--out");
    const char *models_dir = bf_arg_value(argc, argv, "--models-dir");
    const char *loop_str   = bf_arg_value(argc, argv, "--loop");
    const char *chain_arg  = bf_arg_value(argc, argv, "--chain");
    const char *fpqx_arg   = bf_arg_value(argc, argv, "--fpqx");
    const char *thresh_str = bf_arg_value(argc, argv, "--thresh");

    if (!in_path)    { fprintf(stderr, "sli auto-run: --in required\n");    return 1; }
    if (!stats_path) { fprintf(stderr, "sli auto-run: --stats required\n"); return 1; }
    if (!out_dir)    { fprintf(stderr, "sli auto-run: --out required\n");   return 1; }

    if (!models_dir) models_dir = ".";
    int   max_iter  = (loop_str && atoi(loop_str) > 0) ? atoi(loop_str) : 3;
    float threshold = thresh_str ? (float)atof(thresh_str) : 0.001f;
    int   fpqx_auto        = (fpqx_arg  && strcmp(fpqx_arg,  "auto") == 0);
    int   chain_none       = (chain_arg && strcmp(chain_arg, "none") == 0);
    int   chain_auto       = (!chain_arg || strcmp(chain_arg, "auto") == 0);
    /* fragment:auto — first iter uses <family>-frag.bqfp if present, then auto */
    int   chain_frag_first = (chain_arg && strcmp(chain_arg, "fragment:auto") == 0);
    if (chain_frag_first) chain_auto = 1; /* base routing still auto */

    if (max_iter > MAX_LOOP) max_iter = MAX_LOOP;

    /* Load input vectors */
    float   *state = NULL;
    uint32_t n_vecs = 0, dim = 0;
    if (load_vectors(in_path, &state, &n_vecs, &dim)) return 1;

    float *new_state = (float *)malloc((size_t)n_vecs * dim * sizeof(float));
    if (!new_state) { free(state); return 1; }

    printf("sli auto-run: %s  [loop=%d  thresh=%.4f  models=%s%s]\n",
           in_path, max_iter, (double)threshold, models_dir,
           chain_frag_first ? "  chain=fragment:auto" : "");

    char  prev_family[64] = "";
    int   converged = 0;
    float last_delta = -1.0f;
    int   actual_iters = 0;

    mkdir(out_dir, 0755);

    for (int iter = 1; iter <= max_iter && !converged; iter++) {
        actual_iters = iter;

        /* ── 1. Route: determine best family ───────────────────────── */
        char family[64] = "";
        if (chain_none) {
            strncpy(family, "T04", sizeof(family)-1);
        } else if (chain_auto) {
            if (sli_route_family(stats_path, family, sizeof(family))) {
                fprintf(stderr, "sli auto-run iter %d: routing failed\n", iter);
                free(state); free(new_state); return 1;
            }
        } else {
            /* Fixed chain like "T04:T16" — pick token by iteration, wrap around */
            char tmp[256];
            strncpy(tmp, chain_arg, sizeof(tmp)-1); tmp[sizeof(tmp)-1] = '\0';
            char *tok = strtok(tmp, ":");
            char *chosen = tok;
            for (int k = 1; k < iter && tok; k++) { tok = strtok(NULL, ":"); if (tok) chosen = tok; }
            if (!chosen) {
                /* wrap: restart and pick last available */
                strncpy(tmp, chain_arg, sizeof(tmp)-1);
                tok = strtok(tmp, ":");
                chosen = tok;
                while (tok) { chosen = tok; tok = strtok(NULL, ":"); }
            }
            if (chosen) strncpy(family, chosen, sizeof(family)-1);
        }
        if (!family[0]) strncpy(family, "T04", sizeof(family)-1);

        /* ── fragment:auto first-hop ────────────────────────────────── */
        /* On iter 1, try <family>-frag.bqfp as a cheap pre-processor.  */
        /* If found: apply fragment, tag family as "<fam>-frag", so the  */
        /* full-model transform on iter 2+ escalates from fragment space. */
        char frag_path[MAX_PATH];
        if (chain_frag_first && iter == 1) {
            snprintf(frag_path, sizeof(frag_path), "%s/%s-frag.bqfp", models_dir, family);
            struct stat frag_st;
            if (stat(frag_path, &frag_st) == 0) {
                /* Apply fragment pre-process */
                memcpy(new_state, state, (size_t)n_vecs * dim * sizeof(float));
                if (apply_transform_all(frag_path, state, n_vecs, dim, new_state) == 0) {
                    float *tmp_frag = state; state = new_state; new_state = tmp_frag;
                    printf("  preflight: fragment %s-frag applied\n", family);
                }
            }
        }

        /* ── 2. Apply transform ─────────────────────────────────────── */
        char model_path[MAX_PATH];
        snprintf(model_path, sizeof(model_path), "%s/%s.bqfp", models_dir, family);
        memcpy(new_state, state, (size_t)n_vecs * dim * sizeof(float));
        if (apply_transform_all(model_path, state, n_vecs, dim, new_state)) {
            fprintf(stderr, "sli auto-run iter %d: transform failed (model=%s)\n",
                    iter, model_path);
            free(state); free(new_state); return 1;
        }

        /* ── 3. FPQx alignment if family changed ────────────────────── */
        if (fpqx_auto && prev_family[0] && strcmp(family, prev_family) != 0) {
            char align_path[MAX_PATH];
            snprintf(align_path, sizeof(align_path), "%s/%s-%s-align.bin",
                     models_dir, prev_family, family);
            struct stat st_check;
            if (stat(align_path, &st_check) == 0) {
                if (apply_fpqx_alignment(new_state, n_vecs, dim, align_path) == 0)
                    printf("  iter %d/%d: fpqx align %s→%s applied\n",
                           iter, max_iter, prev_family, family);
            }
        }

        /* ── 4. Cosine delta between old state and new state ─────────── */
        float delta = (iter == 1) ? -1.0f
                      : cosine_delta_batch(state, new_state, n_vecs, dim);
        last_delta = delta;

        /* ── 5. Save iteration output ────────────────────────────────── */
        char iter_dir[MAX_PATH], vec_path[MAX_PATH], art_path[MAX_PATH];
        snprintf(iter_dir, sizeof(iter_dir),  "%s/iter-%d",       out_dir, iter);
        snprintf(vec_path, sizeof(vec_path),  "%s/vectors.bin",   iter_dir);
        snprintf(art_path, sizeof(art_path),  "%s/artifact.json", iter_dir);
        mkdir(iter_dir, 0755);
        save_vectors(vec_path, new_state, n_vecs, dim);

        int iter_converged = (delta >= 0.0f && delta < threshold);
        write_iter_artifact(art_path, iter, family,
                             (delta < 0) ? 0.0f : delta,
                             iter_converged, n_vecs, dim);

        if (delta < 0)
            printf("  iter %d/%d: route → %s (delta=n/a) → %s/\n",
                   iter, max_iter, family, iter_dir);
        else
            printf("  iter %d/%d: route → %s (delta=%.4f) → %s/\n",
                   iter, max_iter, family, (double)delta, iter_dir);

        /* ── 6. Convergence check ────────────────────────────────────── */
        if (iter_converged) {
            converged = 1;
            printf("  converged at iter %d (delta=%.6f < threshold=%.6f)\n",
                   iter, (double)delta, (double)threshold);
        }

        /* ── 7. Advance state ────────────────────────────────────────── */
        float *tmp_swap = state; state = new_state; new_state = tmp_swap;
        strncpy(prev_family, family, sizeof(prev_family)-1);
    }

    /* Final summary artifact */
    char final_art[MAX_PATH];
    snprintf(final_art, sizeof(final_art), "%s/artifact.json", out_dir);
    write_iter_artifact(final_art, actual_iters, prev_family,
                         (last_delta < 0) ? 0.0f : last_delta,
                         converged, n_vecs, dim);

    printf("  done: %d iteration%s, final delta=%.4f%s\n",
           actual_iters, actual_iters == 1 ? "" : "s",
           (double)((last_delta < 0) ? 0.0f : last_delta),
           converged ? " (converged)" : "");

    free(state); free(new_state);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * CLI
 * ═══════════════════════════════════════════════════════════════════ */

static void usage(void) {
    fprintf(stderr,
        "bonfyre-sli v" VERSION " — Structured Layer Inference\n\n"
        "Usage:\n"
        "  bonfyre-sli run      --in vecs.bin --model m.bqfp --out out.bin\n"
        "  bonfyre-sli chain    --in vecs.bin --chain T04:T16 --models-dir DIR --out out.bin\n"
        "                       [--gdn-gate 0.9] [--gdn-beta auto]  (for 'gdn' chain steps)\n"
        "  bonfyre-sli route    --in vecs.bin --stats corpus_stats.json --models-dir DIR --out out.bin\n"
        "  bonfyre-sli auto-run --in vecs.bin --stats corpus_stats.json --out results/\n"
        "               [--loop 3] [--chain auto|T04:T16|none] [--fpqx auto|none]\n"
        "               [--thresh 0.001] [--models-dir DIR]\n"
        "  bonfyre-sli bench    --model m.bqfp [--n 1000]\n"
        "  bonfyre-sli inspect  --model m.bqfp\n"
        "\n"
        "Vector file format: [n_vecs:u32][dim:u32][float32 × n × d]\n"
        "\n"
        "  run:      apply single BQFP model transform to input vectors\n"
        "  chain:    apply T04 then T16 (or any family sequence) in order\n"
        "  route:    use bonfyre-model route to pick family, then run\n"
        "  auto-run: routing + transform + FPQx alignment + convergence loop\n"
        "  bench:    throughput benchmark\n"
        "  inspect:  print BQFP tensor structure\n"
    );
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }
    const char *cmd = argv[1];

    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) { usage(); return 0; }
    if (strcmp(cmd, "--version") == 0) {
        printf("bonfyre-sli v" VERSION "\n"); return 0;
    }

    const char *in_path    = bf_arg_value(argc, argv, "--in");
    const char *model_path = bf_arg_value(argc, argv, "--model");
    const char *out_path   = bf_arg_value(argc, argv, "--out");
    const char *chain_spec = bf_arg_value(argc, argv, "--chain");
    const char *stats_path = bf_arg_value(argc, argv, "--stats");
    const char *models_dir = bf_arg_value(argc, argv, "--models-dir");
    const char *nstr       = bf_arg_value(argc, argv, "--n");
    int n_bench = nstr ? atoi(nstr) : 1000;

    if (strcmp(cmd, "inspect") == 0) {
        if (!model_path) { fprintf(stderr,"sli inspect: --model required\n"); return 1; }
        return cmd_inspect_model(model_path);
    }

    if (strcmp(cmd, "run") == 0) {
        if (!in_path || !model_path || !out_path) {
            fprintf(stderr,"sli run: --in, --model, --out required\n"); return 1;
        }
        return cmd_run(in_path, model_path, out_path);
    }

    if (strcmp(cmd, "chain") == 0) {
        if (!in_path || !chain_spec || !models_dir || !out_path) {
            fprintf(stderr,"sli chain: --in, --chain, --models-dir, --out required\n"); return 1;
        }
        const char *gate_str = bf_arg_value(argc, argv, "--gdn-gate");
        const char *beta_str = bf_arg_value(argc, argv, "--gdn-beta");
        float gdn_gate = gate_str ? (float)atof(gate_str) : 0.9f;
        float gdn_beta = beta_str ? (float)atof(beta_str) : -1.0f; /* -1 = auto */
        return cmd_chain(in_path, chain_spec, models_dir, out_path, gdn_gate, gdn_beta);
    }

    if (strcmp(cmd, "route") == 0) {
        if (!in_path || !stats_path || !models_dir || !out_path) {
            fprintf(stderr,"sli route: --in, --stats, --models-dir, --out required\n"); return 1;
        }
        return cmd_route(in_path, stats_path, models_dir, out_path);
    }

    if (strcmp(cmd, "bench") == 0) {
        if (!model_path) { fprintf(stderr,"sli bench: --model required\n"); return 1; }
        return cmd_bench(model_path, n_bench);
    }

    if (strcmp(cmd, "auto-run") == 0) {
        return cmd_auto_run(argc, argv);
    }

    fprintf(stderr, "bonfyre-sli: unknown command '%s'\n", cmd);
    usage(); return 1;
}
