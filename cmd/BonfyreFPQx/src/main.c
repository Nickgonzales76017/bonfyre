/*
 * bonfyre-fpqx — Cross-family FPQ alignment
 *
 * Takes two .bqfp files (from different transform families, e.g. T04 + T15)
 * and produces a linear alignment matrix that maps one family's quantized
 * embedding space to the other's.
 *
 * The alignment is computed by:
 *   1. Reconstruct codebook centroids from both files (tile-level)
 *   2. Match shared anchor points via cosine similarity (greedy)
 *   3. Compute a least-squares alignment matrix A such that B ≈ A × C_A
 *   4. Write alignment to fpqx_alignment.json + fpqx_alignment.bin
 *
 * Usage:
 *   bonfyre-fpqx align  <family_a.bqfp> <family_b.bqfp> --out DIR
 *   bonfyre-fpqx eval   <family_a.bqfp> <fpqx_alignment.json>
 *   bonfyre-fpqx inspect <fpqx_alignment.json>
 *   bonfyre-fpqx --help
 *
 * Alignment artifact (DIR/fpqx_alignment.json):
 *   {
 *     "type":          "fpqx_alignment",
 *     "family_a":      "T04",
 *     "family_b":      "T15",
 *     "tile_dim":      16,
 *     "n_anchors":     32,
 *     "cosine_mean":   0.97x,
 *     "matrix_path":   "fpqx_alignment.bin",
 *     "created_at":    "..."
 *   }
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
#define TILE_DIM   16
#define MAX_TILES  256
#define MAX_PATH   4096
#define BQFP_MAGIC 0x50464251u

/* ═══════════════════════════════════════════════════════════════════
 * BQFP reader — extracts first tensor's codebook (representative tiles)
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    char     family[64];      /* from filename, e.g. "T04" */
    char     path[MAX_PATH];
    float   *codebook;        /* eff_k × TILE_DIM float32 */
    int      eff_k;
    uint32_t bits;
} BqfpSummary;

static void infer_family_from_path(const char *path, char *fam, size_t n) {
    /* Extract T04, T15, T16 etc. from filenames like "T04-family.bqfp" */
    const char *p = strrchr(path, '/');
    if (!p) p = path; else p++;
    fam[0] = '\0';
    while (*p && *p != 'T') p++;
    if (*p == 'T' && *(p+1) >= '0' && *(p+1) <= '9') {
        size_t i = 0;
        while (*p && *p != '-' && *p != '.' && *p != '_' && i < n-1)
            fam[i++] = *p++;
        fam[i] = '\0';
    }
    if (!fam[0]) snprintf(fam, n, "unknown");
}

static int bqfp_load(const char *path, BqfpSummary *s) {
    strncpy(s->path, path, MAX_PATH-1);
    infer_family_from_path(path, s->family, sizeof(s->family));

    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return 1; }

    uint32_t hdr[4];
    if (fread(hdr, 4, 4, f) != 4 || hdr[0] != BQFP_MAGIC) {
        fprintf(stderr, "bonfyre-fpqx: not a BQFP file: %s\n", path);
        fclose(f); return 1;
    }
    uint32_t n_tensors = hdr[2];
    s->bits = hdr[3];
    s->codebook = NULL; s->eff_k = 0;

    /* Read first tensor's codebook (representative of this family's embedding) */
    for (uint32_t t = 0; t < n_tensors; t++) {
        char name[256];
        uint64_t n_elements, n_blocks;
        uint32_t eff_k, _pad;
        if (fread(name, 1, 256, f) != 256) break;
        if (fread(&n_elements, 8, 1, f) != 1) break;
        if (fread(&n_blocks, 8, 1, f) != 1) break;
        if (fread(&eff_k, 4, 1, f) != 1) break;
        if (fread(&_pad, 4, 1, f) != 1) break;

        size_t cb_bytes = (size_t)eff_k * TILE_DIM * sizeof(float);
        /* Fixed block size: scale(4) + warp_norm(4) + e8i[256](256) + tile_idx[16](16) = 280 */
        size_t blk_bytes = (size_t)n_blocks * (4 + 4 + 256 + 16);

        if (t == 0 && eff_k > 0 && eff_k <= MAX_TILES) {
            s->codebook = (float *)malloc(cb_bytes);
            if (s->codebook && fread(s->codebook, 1, cb_bytes, f) == cb_bytes) {
                s->eff_k = (int)eff_k;
                fseek(f, (long)blk_bytes, SEEK_CUR);
            } else {
                free(s->codebook); s->codebook = NULL;
                fseek(f, (long)(cb_bytes + blk_bytes), SEEK_CUR);
            }
        } else {
            fseek(f, (long)(cb_bytes + blk_bytes), SEEK_CUR);
        }
    }
    fclose(f);
    if (!s->codebook) {
        fprintf(stderr, "bonfyre-fpqx: no codebook in %s\n", path);
        return 1;
    }
    return 0;
}

static void bqfp_free(BqfpSummary *s) { free(s->codebook); s->codebook = NULL; }

/* ═══════════════════════════════════════════════════════════════════
 * Cosine similarity between two 16D vectors
 * ═══════════════════════════════════════════════════════════════════ */

static float cosine16(const float *a, const float *b) {
    float dot = 0, na2 = 0, nb2 = 0;
    for (int i = 0; i < TILE_DIM; i++) {
        dot += a[i] * b[i];
        na2 += a[i] * a[i];
        nb2 += b[i] * b[i];
    }
    float denom = sqrtf(na2) * sqrtf(nb2);
    return (denom > 1e-12f) ? dot / denom : 0.0f;
}

/* ═══════════════════════════════════════════════════════════════════
 * Greedy anchor matching: find best-matching tile pairs
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct { int ia; int ib; float cosine; } Anchor;

static int anchor_cmp(const void *x, const void *y) {
    const Anchor *a = (const Anchor *)x;
    const Anchor *b = (const Anchor *)y;
    return (b->cosine > a->cosine) - (b->cosine < a->cosine);
}

static int find_anchors(const BqfpSummary *sa, const BqfpSummary *sb,
                         Anchor *anchors, int max_anchors) {
    int used_a[MAX_TILES] = {0}, used_b[MAX_TILES] = {0};
    int n_anchors = 0;
    int ka = sa->eff_k, kb = sb->eff_k;
    int limit = (ka < kb) ? ka : kb;
    if (max_anchors < limit) limit = max_anchors;

    /* Greedy: for each unmatched tile in A, find best match in B */
    for (int iter = 0; iter < limit; iter++) {
        float best_cos = -2.0f; int best_ia = -1, best_ib = -1;
        for (int ia = 0; ia < ka; ia++) {
            if (used_a[ia]) continue;
            for (int ib = 0; ib < kb; ib++) {
                if (used_b[ib]) continue;
                float c = cosine16(sa->codebook + ia * TILE_DIM,
                                   sb->codebook + ib * TILE_DIM);
                if (c > best_cos) { best_cos = c; best_ia = ia; best_ib = ib; }
            }
        }
        if (best_ia < 0) break;
        anchors[n_anchors].ia = best_ia;
        anchors[n_anchors].ib = best_ib;
        anchors[n_anchors].cosine = best_cos;
        used_a[best_ia] = used_b[best_ib] = 1;
        n_anchors++;
    }
    /* Sort by cosine descending */
    qsort(anchors, (size_t)n_anchors, sizeof(Anchor), anchor_cmp);
    return n_anchors;
}

/* ═══════════════════════════════════════════════════════════════════
 * One-sided Jacobi SVD for TILE_DIM × TILE_DIM matrix.
 * Decomposes H → U Σ V^T where U,V are orthogonal.
 *
 * Algorithm: apply Jacobi column-pair rotations to H from the right,
 * accumulating V.  After convergence, columns of H are orthogonal (= U*Σ).
 * ═══════════════════════════════════════════════════════════════════ */
static void jacobi_svd16(float H[TILE_DIM][TILE_DIM],
                          float U[TILE_DIM][TILE_DIM],
                          float V[TILE_DIM][TILE_DIM]) {
    const int n = TILE_DIM;
    float A[TILE_DIM][TILE_DIM];
    memcpy(A, H, sizeof(A));
    memset(V, 0, sizeof(float) * (size_t)(n * n));
    for (int i = 0; i < n; i++) V[i][i] = 1.0f;

    for (int sweep = 0; sweep < 30; sweep++) {
        float total_off = 0.0f;
        for (int p = 0; p < n-1; p++) {
            for (int q = p+1; q < n; q++) {
                float cpp = 0, cqq = 0, cpq = 0;
                for (int i = 0; i < n; i++) {
                    cpp += A[i][p] * A[i][p];
                    cqq += A[i][q] * A[i][q];
                    cpq += A[i][p] * A[i][q];
                }
                total_off += cpq * cpq;
                float tol = 1e-9f * sqrtf(cpp * cqq + 1e-30f);
                if (fabsf(cpq) <= tol) continue;
                float tau = (cqq - cpp) / (2.0f * cpq);
                float t   = (tau >= 0.0f)
                            ? 1.0f / (tau + sqrtf(1.0f + tau * tau))
                            : 1.0f / (tau - sqrtf(1.0f + tau * tau));
                float c   = 1.0f / sqrtf(1.0f + t * t);
                float s   = t * c;
                for (int i = 0; i < n; i++) {
                    float ap = A[i][p], aq = A[i][q];
                    A[i][p] =  c * ap - s * aq;
                    A[i][q] =  s * ap + c * aq;
                }
                for (int i = 0; i < n; i++) {
                    float vp = V[i][p], vq = V[i][q];
                    V[i][p] =  c * vp - s * vq;
                    V[i][q] =  s * vp + c * vq;
                }
            }
        }
        if (total_off < 1e-18f) break;
    }

    /* Normalise columns of A to get U; column norms are singular values */
    for (int j = 0; j < n; j++) {
        float norm = 0.0f;
        for (int i = 0; i < n; i++) norm += A[i][j] * A[i][j];
        norm = sqrtf(norm);
        if (norm > 1e-10f) {
            for (int i = 0; i < n; i++) U[i][j] = A[i][j] / norm;
        } else {
            for (int i = 0; i < n; i++) U[i][j] = (i == j) ? 1.0f : 0.0f;
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * Orthogonal Procrustes alignment matrix (16×16)
 *
 * Given anchor pairs (a_i ∈ R^16, b_i ∈ R^16), find the orthogonal
 * rotation R = argmin_R ||B - A R^T||_F  s.t.  R R^T = I
 *
 * Solution (Schönemann 1966):
 *   H = A^T B   (weighted cross-product)
 *   H = U Σ V^T  (SVD)
 *   R = V U^T
 *
 * Compared to unconstrained least squares: R preserves angles and
 * norms between tiles — no shearing or scaling artefacts.
 * ═══════════════════════════════════════════════════════════════════ */
static void compute_alignment_matrix(const BqfpSummary *sa, const BqfpSummary *sb,
                                      const Anchor *anchors, int n_anchors,
                                      float *mat /* TILE_DIM × TILE_DIM output */) {
    memset(mat, 0, TILE_DIM * TILE_DIM * sizeof(float));

    if (n_anchors == 0) {
        for (int i = 0; i < TILE_DIM; i++) mat[i * TILE_DIM + i] = 1.0f;
        return;
    }

    /* Weighted cross-product H = A^T B (TILE_DIM × TILE_DIM) */
    float H[TILE_DIM][TILE_DIM];
    memset(H, 0, sizeof(H));
    float w_total = 0.0f;
    for (int k = 0; k < n_anchors; k++) {
        const float *av = sa->codebook + anchors[k].ia * TILE_DIM;
        const float *bv = sb->codebook + anchors[k].ib * TILE_DIM;
        float w = anchors[k].cosine > 0.0f ? anchors[k].cosine : 0.0f;
        w_total += w;
        for (int i = 0; i < TILE_DIM; i++)
            for (int j = 0; j < TILE_DIM; j++)
                H[i][j] += w * av[i] * bv[j];
    }
    /* Normalise so conditioning doesn't depend on anchor count */
    if (w_total > 1e-12f) {
        float inv = 1.0f / w_total;
        for (int i = 0; i < TILE_DIM; i++)
            for (int j = 0; j < TILE_DIM; j++)
                H[i][j] *= inv;
    }

    /* SVD: H = U Σ V^T via one-sided Jacobi */
    float U[TILE_DIM][TILE_DIM], V[TILE_DIM][TILE_DIM];
    jacobi_svd16(H, U, V);

    /* Orthogonal Procrustes solution: R = V U^T */
    /* mat[i][j] = Σ_k V[i][k] * U[j][k] */
    for (int i = 0; i < TILE_DIM; i++)
        for (int j = 0; j < TILE_DIM; j++) {
            float s = 0.0f;
            for (int k = 0; k < TILE_DIM; k++) s += V[i][k] * U[j][k];
            mat[i * TILE_DIM + j] = s;
        }
}

/* ═══════════════════════════════════════════════════════════════════
 * File I/O helpers
 * ═══════════════════════════════════════════════════════════════════ */

static void mkdirp(const char *path) {
    char tmp[MAX_PATH]; snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp+1; *p; p++) { if (*p=='/') { *p='\0'; mkdir(tmp,0755); *p='/'; } }
    mkdir(tmp, 0755);
}

/* ═══════════════════════════════════════════════════════════════════
 * Commands
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_align(const char *path_a, const char *path_b, const char *out_dir) {
    BqfpSummary sa, sb;
    if (bqfp_load(path_a, &sa)) return 1;
    if (bqfp_load(path_b, &sb)) { bqfp_free(&sa); return 1; }

    printf("bonfyre-fpqx align: %s (%s, k=%d)  ×  %s (%s, k=%d)\n",
           sa.family, path_a, sa.eff_k, sb.family, path_b, sb.eff_k);

    /* Find anchor pairs */
    Anchor anchors[MAX_TILES];
    int n_anchors = find_anchors(&sa, &sb, anchors, MAX_TILES);

    double cosine_sum = 0;
    for (int i = 0; i < n_anchors; i++) cosine_sum += anchors[i].cosine;
    double cosine_mean = n_anchors > 0 ? cosine_sum / n_anchors : 0.0;

    printf("  anchors:     %d\n", n_anchors);
    printf("  cosine_mean: %.4f\n", cosine_mean);

    /* Compute 16×16 alignment matrix */
    float mat[TILE_DIM * TILE_DIM];
    compute_alignment_matrix(&sa, &sb, anchors, n_anchors, mat);

    mkdirp(out_dir);
    char bin_path[MAX_PATH], json_path[MAX_PATH];
    snprintf(bin_path,  sizeof(bin_path),  "%s/fpqx_alignment.bin",  out_dir);
    snprintf(json_path, sizeof(json_path), "%s/fpqx_alignment.json", out_dir);

    /* Write binary matrix */
    FILE *bf = fopen(bin_path, "wb");
    if (!bf) { perror(bin_path); bqfp_free(&sa); bqfp_free(&sb); return 1; }
    uint32_t header[4] = { 0x58515046u /* "FPQX" */, 1, TILE_DIM, TILE_DIM };
    fwrite(header, 4, 4, bf);
    fwrite(mat, sizeof(float), TILE_DIM * TILE_DIM, bf);
    fclose(bf);

    /* Write JSON manifest */
    char ts[32]; bf_iso_timestamp(ts, sizeof(ts));
    FILE *jf = fopen(json_path, "w");
    if (jf) {
        fprintf(jf,
            "{\n"
            "  \"type\":         \"fpqx_alignment\",\n"
            "  \"version\":      \"" VERSION "\",\n"
            "  \"family_a\":     \"%s\",\n"
            "  \"family_b\":     \"%s\",\n"
            "  \"path_a\":       \"%s\",\n"
            "  \"path_b\":       \"%s\",\n"
            "  \"tile_dim\":     %d,\n"
            "  \"n_anchors\":    %d,\n"
            "  \"cosine_mean\":  %.6f,\n"
            "  \"matrix_path\":  \"fpqx_alignment.bin\",\n"
            "  \"created_at\":   \"%s\"\n"
            "}\n",
            sa.family, sb.family, path_a, path_b,
            TILE_DIM, n_anchors, cosine_mean, ts);
        fclose(jf);
    }

    printf("  matrix:      %s  (16×16 float32)\n", bin_path);
    printf("  manifest:    %s\n", json_path);

    /* Top 5 anchors */
    printf("\n  Top anchor pairs (IA→IB, cosine):\n");
    int show = n_anchors < 5 ? n_anchors : 5;
    for (int i = 0; i < show; i++)
        printf("    tile[%3d] → tile[%3d]  cos=%.4f\n",
               anchors[i].ia, anchors[i].ib, anchors[i].cosine);

    bqfp_free(&sa); bqfp_free(&sb);
    return 0;
}

static int cmd_inspect_alignment(const char *json_path) {
    FILE *f = fopen(json_path, "r");
    if (!f) { perror(json_path); return 1; }
    char buf[4096]; size_t n = fread(buf, 1, sizeof(buf)-1, f); fclose(f);
    buf[n] = '\0';
    printf("fpqx alignment manifest:\n%s\n", buf);
    return 0;
}

static int cmd_eval(const char *bqfp_path, const char *align_json) {
    /* Load the alignment matrix and evaluate a BQFP's codebook through it */
    BqfpSummary s;
    if (bqfp_load(bqfp_path, &s)) return 1;

    /* Find matrix bin path from JSON */
    FILE *jf = fopen(align_json, "r");
    if (!jf) { perror(align_json); bqfp_free(&s); return 1; }
    char jbuf[4096]; size_t jn = fread(jbuf, 1, sizeof(jbuf)-1, jf); fclose(jf);
    jbuf[jn] = '\0';

    const char *mp = strstr(jbuf, "\"matrix_path\"");
    if (!mp) { fprintf(stderr,"fpqx eval: no matrix_path in %s\n", align_json); bqfp_free(&s); return 1; }
    mp = strchr(mp, ':'); if (!mp) { bqfp_free(&s); return 1; }
    mp++; while (*mp == ' ' || *mp == '"') mp++;
    char mat_fname[256]; int mfi = 0;
    while (*mp && *mp != '"' && mfi < 255) mat_fname[mfi++] = *mp++;
    mat_fname[mfi] = '\0';

    /* Resolve relative path */
    char mat_path[MAX_PATH];
    const char *slash = strrchr(align_json, '/');
    if (slash) {
        snprintf(mat_path, sizeof(mat_path), "%.*s/%s",
                 (int)(slash - align_json), align_json, mat_fname);
    } else {
        snprintf(mat_path, sizeof(mat_path), "%s", mat_fname);
    }

    FILE *bf = fopen(mat_path, "rb");
    if (!bf) { perror(mat_path); bqfp_free(&s); return 1; }
    uint32_t hdr[4];
    if (fread(hdr, 4, 4, bf) != 4 || hdr[0] != 0x58515046u) {
        fprintf(stderr, "fpqx eval: bad matrix file %s\n", mat_path);
        fclose(bf); bqfp_free(&s); return 1;
    }
    float mat[TILE_DIM * TILE_DIM];
    if (fread(mat, sizeof(float), TILE_DIM*TILE_DIM, bf) != TILE_DIM*TILE_DIM) {
        fclose(bf); bqfp_free(&s); return 1;
    }
    fclose(bf);

    /* Apply matrix to first codebook tile and report */
    printf("fpqx eval: %s  (k=%d tiles)\n", bqfp_path, s.eff_k);
    float self_cos_sum = 0;
    for (int ti = 0; ti < s.eff_k; ti++) {
        float *tile = s.codebook + ti * TILE_DIM;
        float mapped[TILE_DIM] = {0};
        for (int i = 0; i < TILE_DIM; i++)
            for (int j = 0; j < TILE_DIM; j++)
                mapped[i] += mat[i * TILE_DIM + j] * tile[j];
        self_cos_sum += cosine16(tile, mapped);
    }
    printf("  mean cosine(tile, A×tile): %.4f  (1.0 = identity alignment)\n",
           s.eff_k > 0 ? self_cos_sum / s.eff_k : 0);
    bqfp_free(&s);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * CLI
 * ═══════════════════════════════════════════════════════════════════ */

static void usage(void) {
    fprintf(stderr,
        "bonfyre-fpqx v" VERSION " — Cross-family FPQ alignment\n\n"
        "Usage:\n"
        "  bonfyre-fpqx align   <a.bqfp> <b.bqfp> --out DIR\n"
        "  bonfyre-fpqx eval    <a.bqfp> <fpqx_alignment.json>\n"
        "  bonfyre-fpqx inspect <fpqx_alignment.json>\n"
        "\n"
        "  align:   compute 16×16 linear alignment between two BQFP codebooks\n"
        "  eval:    apply alignment to a BQFP, report mean cosine preservation\n"
        "  inspect: print alignment manifest\n"
    );
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }
    const char *cmd = argv[1];

    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) { usage(); return 0; }
    if (strcmp(cmd, "--version") == 0) {
        printf("bonfyre-fpqx v" VERSION "\n"); return 0;
    }

    if (strcmp(cmd, "align") == 0) {
        if (argc < 4) {
            fprintf(stderr, "usage: bonfyre-fpqx align <a.bqfp> <b.bqfp> --out DIR\n");
            return 1;
        }
        const char *out = bf_arg_value(argc, argv, "--out");
        if (!out) { fprintf(stderr, "bonfyre-fpqx align: --out required\n"); return 1; }
        return cmd_align(argv[2], argv[3], out);
    }

    if (strcmp(cmd, "eval") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: bonfyre-fpqx eval <a.bqfp> <align.json>\n"); return 1; }
        return cmd_eval(argv[2], argv[3]);
    }

    if (strcmp(cmd, "inspect") == 0) {
        if (argc < 3) { fprintf(stderr, "usage: bonfyre-fpqx inspect <align.json>\n"); return 1; }
        return cmd_inspect_alignment(argv[2]);
    }

    fprintf(stderr, "bonfyre-fpqx: unknown command '%s'\n", cmd);
    usage(); return 1;
}
