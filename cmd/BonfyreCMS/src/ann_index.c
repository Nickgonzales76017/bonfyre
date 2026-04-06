/*
 * BonfyreGraph — Upgrade III: ANN Index over Tensor Bindings
 *
 * LSH with random hyperplane projections for cosine similarity.
 * Binding vectors extracted from _tensor_cells (compressed domain).
 * Multi-probe lookup across ANN_NUM_TABLES independent tables.
 */

#include "ann_index.h"
#include "canonical.h"
#include "bench_metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include <time.h>

#define ANN_QUANT_LEVELS 16
#define ANN_QUANT_BITS 4
#define ANN_BRUTE_THRESHOLD 5000

extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

typedef struct {
    int target_id;
    int family_id;
    double dist;
    int votes;
} AnnCandidate;

/* ================================================================
 * Bootstrap
 * ================================================================ */

int ann_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        /* LSH bucket assignments */
        "CREATE TABLE IF NOT EXISTS _ann_buckets ("
        "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type TEXT NOT NULL,"
        "  table_idx   INTEGER NOT NULL,"
        "  bucket_hash TEXT NOT NULL,"
        "  family_id   INTEGER NOT NULL,"
        "  target_id   INTEGER NOT NULL,"
        "  UNIQUE(content_type, table_idx, target_id)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_ann_lookup "
        "ON _ann_buckets(content_type, table_idx, bucket_hash);",

        "CREATE INDEX IF NOT EXISTS idx_ann_target "
        "ON _ann_buckets(content_type, target_id);",

        /* Random projection vectors (generated once, reused) */
        "CREATE TABLE IF NOT EXISTS _ann_projections ("
        "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type TEXT NOT NULL,"
        "  table_idx  INTEGER NOT NULL,"
        "  bit_idx    INTEGER NOT NULL,"
        "  dimension  INTEGER NOT NULL,"
        "  weight     REAL NOT NULL,"
        "  UNIQUE(content_type, table_idx, bit_idx, dimension)"
        ");",

        /* TurboQuant-inspired sidecar: fixed rotated 4-bit codes plus a
           lightweight residual sign sketch for approximate reranking. */
        "CREATE TABLE IF NOT EXISTS _ann_quantized ("
        "  id            INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type  TEXT NOT NULL,"
        "  family_id     INTEGER NOT NULL,"
        "  target_id     INTEGER NOT NULL,"
        "  dim           INTEGER NOT NULL,"
        "  rot_dim       INTEGER NOT NULL,"
        "  norm          REAL NOT NULL,"
        "  clip          REAL NOT NULL,"
        "  residual_avg  REAL NOT NULL,"
        "  q4_codes      BLOB NOT NULL,"
        "  residual_bits BLOB NOT NULL,"
        "  UNIQUE(content_type, target_id)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_ann_quantized_lookup "
        "ON _ann_quantized(content_type, target_id);",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * Internal: seeded PRNG for reproducible projections
 * ================================================================ */

static double prng_normal(unsigned long long *state) {
    /* Box-Muller transform from uniform PRNG */
    *state = *state * 6364136223846793005ULL + 1442695040888963407ULL;
    double u1 = ((double)(*state >> 11)) / (double)(1ULL << 53);
    if (u1 < 1e-15) u1 = 1e-15;
    *state = *state * 6364136223846793005ULL + 1442695040888963407ULL;
    double u2 = ((double)(*state >> 11)) / (double)(1ULL << 53);
    return sqrt(-2.0 * log(u1)) * cos(2.0 * 3.14159265358979323846 * u2);
}

static unsigned long long splitmix64_next(unsigned long long *state) {
    unsigned long long z;
    *state += 0x9e3779b97f4a7c15ULL;
    z = *state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static int ann_next_pow2(int n) {
    int p = 1;
    if (n <= 1) return 1;
    while (p < n && p < ANN_MAX_DIM) p <<= 1;
    if (p > ANN_MAX_DIM) p = ANN_MAX_DIM;
    return p;
}

static int ann_content_dim(sqlite3 *db, const char *content_type) {
    sqlite3_stmt *q;
    int max_dim = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT MAX(position) + 1 FROM _tensor_cells c "
        "JOIN _families f ON f.id = c.family_id "
        "WHERE f.content_type=?1",
        -1, &q, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
    if (sqlite3_step(q) == SQLITE_ROW) max_dim = sqlite3_column_int(q, 0);
    sqlite3_finalize(q);
    if (max_dim <= 0) return 0;
    if (max_dim > ANN_MAX_DIM) max_dim = ANN_MAX_DIM;
    return max_dim;
}

static void fwht_inplace(double *vals, int n) {
    for (int len = 1; len < n; len <<= 1) {
        for (int i = 0; i < n; i += (len << 1)) {
            for (int j = 0; j < len; j++) {
                double a = vals[i + j];
                double b = vals[i + j + len];
                vals[i + j] = a + b;
                vals[i + j + len] = a - b;
            }
        }
    }
}

static void rotate_unit_vector(const char *content_type, const double *unit_vec,
                               int base_dim, double *out, int *out_dim) {
    int rot_dim;
    unsigned long long state;

    if (!out) return;
    if (base_dim <= 0) {
        memset(out, 0, sizeof(double) * ANN_MAX_DIM);
        if (out_dim) *out_dim = 0;
        return;
    }

    if (base_dim > ANN_MAX_DIM) base_dim = ANN_MAX_DIM;
    rot_dim = ann_next_pow2(base_dim);
    memset(out, 0, sizeof(double) * ANN_MAX_DIM);
    memcpy(out, unit_vec, sizeof(double) * (size_t)base_dim);

    state = fnv1a64(content_type, strlen(content_type)) ^ (unsigned long long)rot_dim;
    for (int i = 0; i < rot_dim; i++) {
        if (splitmix64_next(&state) & 1ULL) out[i] = -out[i];
    }

    fwht_inplace(out, rot_dim);
    {
        double scale = 1.0 / sqrt((double)rot_dim);
        for (int i = 0; i < rot_dim; i++) out[i] *= scale;
    }

    for (int i = rot_dim - 1; i > 0; i--) {
        unsigned long long r = splitmix64_next(&state);
        int j = (int)(r % (unsigned long long)(i + 1));
        double tmp = out[i];
        out[i] = out[j];
        out[j] = tmp;
    }

    for (int i = 0; i < rot_dim; i++) {
        if (splitmix64_next(&state) & 1ULL) out[i] = -out[i];
    }

    if (out_dim) *out_dim = rot_dim;
}

typedef struct {
    int initialized;
    double centroids[ANN_QUANT_LEVELS];
    double bounds[ANN_QUANT_LEVELS + 1];
} LloydMaxCodebook;

static LloydMaxCodebook g_ann_q4_codebook = {0};

static double normal_pdf(double x) {
    if (isinf(x)) return 0.0;
    return exp(-0.5 * x * x) / sqrt(2.0 * 3.14159265358979323846);
}

static double normal_cdf(double x) {
    if (x == INFINITY) return 1.0;
    if (x == -INFINITY) return 0.0;
    return 0.5 * erfc(-x / sqrt(2.0));
}

static double gaussian_interval_mean(double lo, double hi) {
    double mass = normal_cdf(hi) - normal_cdf(lo);
    if (mass < 1e-12) {
        if (isinf(lo) && isinf(hi)) return 0.0;
        if (isinf(lo)) return hi - 1e-3;
        if (isinf(hi)) return lo + 1e-3;
        return 0.5 * (lo + hi);
    }
    return (normal_pdf(lo) - normal_pdf(hi)) / mass;
}

static const LloydMaxCodebook *ann_q4_codebook(void) {
    if (!g_ann_q4_codebook.initialized) {
        double centroids[ANN_QUANT_LEVELS];
        double next_centroids[ANN_QUANT_LEVELS];
        double bounds[ANN_QUANT_LEVELS + 1];

        for (int i = 0; i < ANN_QUANT_LEVELS; i++) {
            centroids[i] = -2.75 + (5.5 * (double)i) / (double)(ANN_QUANT_LEVELS - 1);
        }

        for (int iter = 0; iter < 96; iter++) {
            bounds[0] = -INFINITY;
            bounds[ANN_QUANT_LEVELS] = INFINITY;
            for (int i = 1; i < ANN_QUANT_LEVELS; i++) {
                bounds[i] = 0.5 * (centroids[i - 1] + centroids[i]);
            }
            for (int i = 0; i < ANN_QUANT_LEVELS; i++) {
                next_centroids[i] = gaussian_interval_mean(bounds[i], bounds[i + 1]);
            }
            for (int i = 0; i < ANN_QUANT_LEVELS; i++) {
                centroids[i] = next_centroids[i];
            }
        }

        g_ann_q4_codebook.bounds[0] = -INFINITY;
        g_ann_q4_codebook.bounds[ANN_QUANT_LEVELS] = INFINITY;
        for (int i = 0; i < ANN_QUANT_LEVELS; i++) {
            g_ann_q4_codebook.centroids[i] = centroids[i];
            if (i > 0) g_ann_q4_codebook.bounds[i] = 0.5 * (centroids[i - 1] + centroids[i]);
        }
        g_ann_q4_codebook.initialized = 1;
    }
    return &g_ann_q4_codebook;
}

static double quant_sigma_for_dim(int rot_dim) {
    if (rot_dim <= 0) return 1.0;
    return 1.0 / sqrt((double)rot_dim);
}

static double quant_span_for_dim(int rot_dim) {
    const LloydMaxCodebook *cb = ann_q4_codebook();
    double sigma = quant_sigma_for_dim(rot_dim);
    double tail = fabs(cb->centroids[ANN_QUANT_LEVELS - 1]);
    return sigma * tail;
}

static int prepare_quant_query(const char *content_type, const double *vec,
                               int base_dim, double *rotated,
                               int *rot_dim_out, double *norm_out) {
    double unit_vec[ANN_MAX_DIM];
    double norm_sq = 0.0;

    if (!vec || !rotated || base_dim <= 0) return 0;
    if (base_dim > ANN_MAX_DIM) base_dim = ANN_MAX_DIM;
    memset(unit_vec, 0, sizeof(unit_vec));
    for (int i = 0; i < base_dim; i++) norm_sq += vec[i] * vec[i];
    if (norm_out) *norm_out = sqrt(norm_sq);

    if (norm_sq > 0.0) {
        double inv_norm = 1.0 / sqrt(norm_sq);
        for (int i = 0; i < base_dim; i++) unit_vec[i] = vec[i] * inv_norm;
    }

    rotate_unit_vector(content_type, unit_vec, base_dim, rotated, rot_dim_out);
    return base_dim;
}

static double q4_decode(unsigned char code, double sigma) {
    const LloydMaxCodebook *cb = ann_q4_codebook();
    if (code >= ANN_QUANT_LEVELS || sigma <= 0.0) return 0.0;
    return sigma * cb->centroids[code];
}

static void quantize_rotated_vector(const double *rotated, int rot_dim, double sigma,
                                    unsigned char *codes,
                                    unsigned char *residual_bits,
                                    double *residual_avg_out) {
    const LloydMaxCodebook *cb = ann_q4_codebook();
    double abs_sum = 0.0;
    for (int i = 0; i < rot_dim; i++) {
        double z = sigma > 0.0 ? rotated[i] / sigma : rotated[i];
        int enc;
        double decoded;
        double residual;

        enc = ANN_QUANT_LEVELS - 1;
        for (int j = 1; j < ANN_QUANT_LEVELS; j++) {
            if (z < cb->bounds[j]) {
                enc = j - 1;
                break;
            }
        }

        if ((i & 1) == 0) {
            codes[i / 2] = (unsigned char)(enc & 0x0F);
        } else {
            codes[i / 2] |= (unsigned char)((enc & 0x0F) << 4);
        }

        decoded = q4_decode((unsigned char)enc, sigma);
        residual = rotated[i] - decoded;
        abs_sum += fabs(residual);
        if (residual >= 0.0) residual_bits[i / 8] |= (unsigned char)(1U << (i % 8));
    }

    if (residual_avg_out) {
        *residual_avg_out = rot_dim > 0 ? abs_sum / (double)rot_dim : 0.0;
    }
}

static int reconstruct_quantized_rotated(const unsigned char *codes, size_t code_len,
                                         const unsigned char *residual_bits, size_t residual_len,
                                         int rot_dim, double sigma, double residual_avg,
                                         double *out) {
    double norm_sq = 0.0;

    if (!codes || !residual_bits || !out) return -1;
    if ((size_t)((rot_dim + 1) / 2) > code_len) return -1;
    if ((size_t)((rot_dim + 7) / 8) > residual_len) return -1;

    memset(out, 0, sizeof(double) * ANN_MAX_DIM);
    for (int i = 0; i < rot_dim; i++) {
        unsigned char packed = codes[i / 2];
        unsigned char code = (i & 1) == 0 ? (packed & 0x0F) : ((packed >> 4) & 0x0F);
        int sign = (residual_bits[i / 8] & (unsigned char)(1U << (i % 8))) ? 1 : -1;
        double val = q4_decode(code, sigma) + residual_avg * (double)sign;
        out[i] = val;
        norm_sq += val * val;
    }

    if (norm_sq > 0.0) {
        double inv_norm = 1.0 / sqrt(norm_sq);
        for (int i = 0; i < rot_dim; i++) out[i] *= inv_norm;
    }
    return rot_dim;
}

static int store_quantized_vector(sqlite3 *db, const char *content_type,
                                  int family_id, int target_id,
                                  const double *vec, int base_dim) {
    double rotated[ANN_MAX_DIM];
    double norm = 0.0;
    double sigma;
    double span;
    double residual_avg = 0.0;
    int rot_dim = 0;
    size_t code_len, residual_len;
    unsigned char *codes = NULL;
    unsigned char *residual_bits = NULL;
    sqlite3_stmt *ins = NULL;
    int rc = -1;

    if (prepare_quant_query(content_type, vec, base_dim, rotated, &rot_dim, &norm) <= 0 || rot_dim <= 0)
        return -1;

    sigma = quant_sigma_for_dim(rot_dim);
    span = quant_span_for_dim(rot_dim);
    code_len = (size_t)(rot_dim + 1) / 2;
    residual_len = (size_t)(rot_dim + 7) / 8;
    codes = calloc(code_len, 1);
    residual_bits = calloc(residual_len, 1);
    if (!codes || !residual_bits) goto cleanup;

    quantize_rotated_vector(rotated, rot_dim, sigma, codes, residual_bits, &residual_avg);

    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _ann_quantized "
        "(content_type, family_id, target_id, dim, rot_dim, norm, clip, residual_avg, q4_codes, residual_bits) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        -1, &ins, NULL) != SQLITE_OK) goto cleanup;

    sqlite3_bind_text(ins, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(ins, 2, family_id);
    sqlite3_bind_int(ins, 3, target_id);
    sqlite3_bind_int(ins, 4, base_dim);
    sqlite3_bind_int(ins, 5, rot_dim);
    sqlite3_bind_double(ins, 6, norm);
    sqlite3_bind_double(ins, 7, span);
    sqlite3_bind_double(ins, 8, residual_avg);
    sqlite3_bind_blob(ins, 9, codes, (int)code_len, SQLITE_TRANSIENT);
    sqlite3_bind_blob(ins, 10, residual_bits, (int)residual_len, SQLITE_TRANSIENT);
    if (sqlite3_step(ins) == SQLITE_DONE) rc = 0;

cleanup:
    sqlite3_finalize(ins);
    free(codes);
    free(residual_bits);
    return rc;
}

static int load_quantized_vector(sqlite3 *db, const char *content_type,
                                 int target_id, double *rotated,
                                 int *rot_dim_out, double *norm_out) {
    sqlite3_stmt *q = NULL;
    int rc = 0;

    if (!rotated) return 0;
    memset(rotated, 0, sizeof(double) * ANN_MAX_DIM);

    if (sqlite3_prepare_v2(db,
        "SELECT rot_dim, norm, residual_avg, q4_codes, residual_bits "
        "FROM _ann_quantized WHERE content_type=?1 AND target_id=?2",
        -1, &q, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(q, 2, target_id);

    if (sqlite3_step(q) == SQLITE_ROW) {
        int rot_dim = sqlite3_column_int(q, 0);
        double norm = sqlite3_column_double(q, 1);
        double residual_avg = sqlite3_column_double(q, 2);
        const unsigned char *codes = sqlite3_column_blob(q, 3);
        int code_len = sqlite3_column_bytes(q, 3);
        const unsigned char *residual_bits = sqlite3_column_blob(q, 4);
        int residual_len = sqlite3_column_bytes(q, 4);
        double sigma = quant_sigma_for_dim(rot_dim);

        if (rot_dim > 0 && rot_dim <= ANN_MAX_DIM &&
            reconstruct_quantized_rotated(codes, (size_t)code_len,
                                          residual_bits, (size_t)residual_len,
                                          rot_dim, sigma, residual_avg, rotated) > 0) {
            if (rot_dim_out) *rot_dim_out = rot_dim;
            if (norm_out) *norm_out = norm;
            rc = rot_dim;
        }
    }

    sqlite3_finalize(q);
    return rc;
}

static double quant_space_distance(const double *qrot, double qnorm, int qrot_dim,
                                   const double *crot, double cnorm, int crot_dim) {
    double sum_sq = 0.0;
    int dim = qrot_dim > crot_dim ? qrot_dim : crot_dim;
    if (dim > ANN_MAX_DIM) dim = ANN_MAX_DIM;

    for (int i = 0; i < dim; i++) {
        double qv = i < qrot_dim ? qnorm * qrot[i] : 0.0;
        double cv = i < crot_dim ? cnorm * crot[i] : 0.0;
        double diff = qv - cv;
        sum_sq += diff * diff;
    }
    return sqrt(sum_sq);
}

/* ================================================================
 * Internal: ensure projection vectors exist for a content type
 * ================================================================ */

static int ensure_projections(sqlite3 *db, const char *content_type, int dim) {
    /* Check if projections exist */
    sqlite3_stmt *chk;
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _ann_projections WHERE content_type=?1",
        -1, &chk, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(chk, 1, content_type, -1, SQLITE_STATIC);
    int existing = 0;
    if (sqlite3_step(chk) == SQLITE_ROW) existing = sqlite3_column_int(chk, 0);
    sqlite3_finalize(chk);
    if (existing > 0) return 0;

    /* Generate random projection vectors */
    unsigned long long seed = fnv1a64(content_type, strlen(content_type));

    sqlite3_stmt *ins;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO _ann_projections "
        "(content_type, table_idx, bit_idx, dimension, weight) "
        "VALUES (?1, ?2, ?3, ?4, ?5)",
        -1, &ins, NULL) != SQLITE_OK) return -1;

    for (int t = 0; t < ANN_NUM_TABLES; t++) {
        for (int b = 0; b < ANN_HASH_BITS; b++) {
            for (int d = 0; d < dim; d++) {
                double w = prng_normal(&seed);
                sqlite3_reset(ins);
                sqlite3_bind_text(ins, 1, content_type, -1, SQLITE_STATIC);
                sqlite3_bind_int(ins, 2, t);
                sqlite3_bind_int(ins, 3, b);
                sqlite3_bind_int(ins, 4, d);
                sqlite3_bind_double(ins, 5, w);
                sqlite3_step(ins);
            }
        }
    }
    sqlite3_finalize(ins);
    return 0;
}

/* ================================================================
 * Internal: load projection matrix for one table into memory
 * ================================================================ */

typedef struct {
    double weights[ANN_HASH_BITS][ANN_MAX_DIM];
    int dim;
} ProjTable;

static int load_projections(sqlite3 *db, const char *content_type,
                            int table_idx, ProjTable *proj) {
    memset(proj, 0, sizeof(ProjTable));

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT bit_idx, dimension, weight FROM _ann_projections "
        "WHERE content_type=?1 AND table_idx=?2 "
        "ORDER BY bit_idx, dimension",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(q, 2, table_idx);

    while (sqlite3_step(q) == SQLITE_ROW) {
        int b = sqlite3_column_int(q, 0);
        int d = sqlite3_column_int(q, 1);
        if (b < ANN_HASH_BITS && d < ANN_MAX_DIM) {
            proj->weights[b][d] = sqlite3_column_double(q, 2);
            if (d + 1 > proj->dim) proj->dim = d + 1;
        }
    }
    sqlite3_finalize(q);
    return 0;
}

/* ================================================================
 * Internal: compute LSH hash for a binding vector
 * ================================================================ */

static void compute_lsh_hash(const ProjTable *proj, const double *vec,
                             int dim, char *hash_out) {
    /* Each bit: sign(dot(projection_vector, binding_vector)) */
    unsigned char bits[ANN_HASH_BITS / 8 + 1];
    memset(bits, 0, sizeof(bits));

    int d = dim < proj->dim ? dim : proj->dim;
    for (int b = 0; b < ANN_HASH_BITS; b++) {
        double dot = 0.0;
        for (int i = 0; i < d; i++) {
            dot += proj->weights[b][i] * vec[i];
        }
        if (dot >= 0.0) bits[b / 8] |= (unsigned char)(1 << (b % 8));
    }

    /* Convert to hex string */
    int nbytes = (ANN_HASH_BITS + 7) / 8;
    for (int i = 0; i < nbytes; i++) {
        snprintf(hash_out + i * 2, 3, "%02x", bits[i]);
    }
    hash_out[nbytes * 2] = '\0';
}

/* ================================================================
 * Internal: extract binding vector from tensor cells
 *
 * Numeric positions → value directly
 * String positions → FNV hash normalized to [0,1]
 * ================================================================ */

static int extract_vector(sqlite3 *db, int family_id, int target_id,
                          double *vec, int max_dim) {
    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT position, val_text, val_num, val_type "
        "FROM _tensor_cells WHERE family_id=?1 AND target_id=?2 "
        "ORDER BY position",
        -1, &q, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_int(q, 1, family_id);
    sqlite3_bind_int(q, 2, target_id);

    int dim = 0;
    while (sqlite3_step(q) == SQLITE_ROW && dim < max_dim) {
        int pos = sqlite3_column_int(q, 0);
        int vtype = sqlite3_column_int(q, 3);
        if (pos >= max_dim) break;

        if (vtype == 1) {
            /* Numeric: use directly */
            vec[pos] = sqlite3_column_double(q, 2);
        } else {
            /* String/other: hash to [0,1] */
            const char *vt = (const char *)sqlite3_column_text(q, 1);
            if (vt) {
                unsigned long long h = fnv1a64(vt, strlen(vt));
                vec[pos] = (double)(h >> 1) / (double)(ULLONG_MAX >> 1);
            }
        }
        if (pos + 1 > dim) dim = pos + 1;
    }
    sqlite3_finalize(q);
    return dim;
}

/* ================================================================
 * Internal: extract vector from JSON (for query)
 * ================================================================ */

static int extract_vector_from_json(sqlite3 *db, const char *content_type,
                                    const char *json, double *vec, int max_dim) {
    /* Parse the JSON, get canonical field order from any family */
    sqlite3_stmt *fq;
    if (sqlite3_prepare_v2(db,
        "SELECT generator FROM _families WHERE content_type=?1 LIMIT 1",
        -1, &fq, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(fq, 1, content_type, -1, SQLITE_STATIC);

    char generator[32768] = {0};
    if (sqlite3_step(fq) == SQLITE_ROW) {
        const char *g = (const char *)sqlite3_column_text(fq, 0);
        if (g) strncpy(generator, g, sizeof(generator) - 1);
    }
    sqlite3_finalize(fq);
    if (!generator[0]) return 0;

    /* Use bindings extraction from canonical.c */
    char *bindings = extract_bindings(json);
    if (!bindings) return 0;

    /* Parse bindings array into vector */
    const char *p = bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    int dim = 0;
    while (*p && *p != ']' && dim < max_dim) {
        while (*p && (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t')) p++;
        if (*p == ']' || !*p) break;

        if (*p == '"') {
            /* String value: hash */
            p++;
            const char *start = p;
            while (*p && *p != '"') {
                if (*p == '\\') p++;
                p++;
            }
            size_t len = (size_t)(p - start);
            unsigned long long h = fnv1a64(start, len);
            vec[dim] = (double)(h >> 1) / (double)(ULLONG_MAX >> 1);
            if (*p == '"') p++;
        } else {
            /* Number */
            char *end = NULL;
            double d = strtod(p, &end);
            vec[dim] = d;
            p = end ? end : p + 1;
        }
        dim++;
    }
    free(bindings);
    return dim;
}

static int candidate_add_or_vote(AnnCandidate **cands, int *n_cands, int *cand_cap,
                                 int family_id, int target_id) {
    for (int i = 0; i < *n_cands; i++) {
        if ((*cands)[i].target_id == target_id) {
            (*cands)[i].votes++;
            return 0;
        }
    }

    if (*n_cands >= *cand_cap) {
        int next_cap = (*cand_cap == 0) ? 256 : (*cand_cap * 2);
        AnnCandidate *next = realloc(*cands, (size_t)next_cap * sizeof(AnnCandidate));
        if (!next) return -1;
        *cands = next;
        *cand_cap = next_cap;
    }

    (*cands)[*n_cands].target_id = target_id;
    (*cands)[*n_cands].family_id = family_id;
    (*cands)[*n_cands].dist = -1.0;
    (*cands)[*n_cands].votes = 1;
    (*n_cands)++;
    return 0;
}

static void sort_candidates(AnnCandidate *cands, int n_cands) {
    for (int i = 1; i < n_cands; i++) {
        AnnCandidate tmp = cands[i];
        int j = i - 1;
        while (j >= 0 && cands[j].dist > tmp.dist) {
            cands[j + 1] = cands[j];
            j--;
        }
        cands[j + 1] = tmp;
    }
}

static char *format_candidates_json(const AnnCandidate *cands, int n_cands, int k) {
    int result_count = n_cands < k ? n_cands : k;
    size_t cap = 2048;
    size_t off = 0;
    char *out = malloc(cap);
    if (!out) return NULL;

    out[off++] = '[';
    for (int i = 0; i < result_count; i++) {
        while (off + 128 >= cap) {
            char *next;
            cap *= 2;
            next = realloc(out, cap);
            if (!next) {
                free(out);
                return NULL;
            }
            out = next;
        }
        if (i > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off,
            "{\"target_id\":%d,\"distance\":%.6f,\"votes\":%d}",
            cands[i].target_id, cands[i].dist, cands[i].votes);
    }
    out[off++] = ']';
    out[off] = '\0';
    return out;
}

static int parse_knn_ids(const char *json, int *ids, int max_ids) {
    int count = 0;
    const char *p = json;
    if (!json || !ids || max_ids <= 0) return 0;

    while (*p && count < max_ids) {
        const char *tag = strstr(p, "\"target_id\":");
        if (!tag) break;
        tag += 12;
        ids[count++] = atoi(tag);
        p = tag;
    }
    return count;
}

static int ids_contains(const int *ids, int n_ids, int target_id) {
    for (int i = 0; i < n_ids; i++) {
        if (ids[i] == target_id) return 1;
    }
    return 0;
}

static int ids_overlap(const int *a, int na, const int *b, int nb) {
    int overlap = 0;
    for (int i = 0; i < na; i++) {
        if (ids_contains(b, nb, a[i])) overlap++;
    }
    return overlap;
}

static double exact_space_distance(const double *qvec, int qdim,
                                   const double *cvec, int cdim) {
    double sum_sq = 0.0;
    int d = cdim < qdim ? cdim : qdim;
    for (int j = 0; j < d; j++) {
        double diff = qvec[j] - cvec[j];
        sum_sq += diff * diff;
    }
    return sqrt(sum_sq);
}

static int ann_quant_rerank_limit(int n_cands, int k) {
    int limit = k * 4;
    if (limit < 32) limit = 32;
    if (limit > 128) limit = 128;
    if (limit > n_cands) limit = n_cands;
    return limit;
}

static int ann_quant_target_rerank(int k) {
    int limit = k * 4;
    if (limit < 32) limit = 32;
    if (limit > 128) limit = 128;
    return limit;
}

static int rerank_quantized_shortlist_exact(sqlite3 *db,
                                            const double *qvec, int qdim,
                                            AnnCandidate *cands, int n_cands, int k) {
    int rerank_limit = ann_quant_rerank_limit(n_cands, k);

    for (int i = 0; i < rerank_limit; i++) {
        double cvec[ANN_MAX_DIM];
        int cdim;

        memset(cvec, 0, sizeof(cvec));
        cdim = extract_vector(db, cands[i].family_id, cands[i].target_id, cvec, ANN_MAX_DIM);
        cands[i].dist = exact_space_distance(qvec, qdim, cvec, cdim);
    }
    sort_candidates(cands, rerank_limit);
    return rerank_limit;
}

/* ================================================================
 * ann_build_index — full index build
 * ================================================================ */

int ann_build_index(sqlite3 *db, const char *content_type) {
    /* Clear existing index */
    {
        sqlite3_stmt *del;
        if (sqlite3_prepare_v2(db,
            "DELETE FROM _ann_buckets WHERE content_type=?1",
            -1, &del, NULL) == SQLITE_OK) {
            sqlite3_bind_text(del, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_step(del);
            sqlite3_finalize(del);
        }
        if (sqlite3_prepare_v2(db,
            "DELETE FROM _ann_quantized WHERE content_type=?1",
            -1, &del, NULL) == SQLITE_OK) {
            sqlite3_bind_text(del, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_step(del);
            sqlite3_finalize(del);
        }
    }

    /* Get fixed content dimension used by both projections and quantization. */
    int max_dim = ann_content_dim(db, content_type);
    if (max_dim <= 0 || max_dim > ANN_MAX_DIM) max_dim = ANN_MAX_DIM;

    /* Ensure projection vectors exist */
    ensure_projections(db, content_type, max_dim);

    /* Load all projection tables */
    ProjTable *projs = calloc(ANN_NUM_TABLES, sizeof(ProjTable));
    for (int t = 0; t < ANN_NUM_TABLES; t++) {
        load_projections(db, content_type, t, &projs[t]);
    }

    /* Iterate all members and index */
    sqlite3_stmt *mem_q;
    if (sqlite3_prepare_v2(db,
        "SELECT c.family_id, c.target_id FROM _tensor_cells c "
        "JOIN _families f ON f.id = c.family_id "
        "WHERE f.content_type=?1 "
        "GROUP BY c.family_id, c.target_id",
        -1, &mem_q, NULL) != SQLITE_OK) {
        free(projs);
        return -1;
    }
    sqlite3_bind_text(mem_q, 1, content_type, -1, SQLITE_STATIC);

    sqlite3_stmt *ins;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _ann_buckets "
        "(content_type, table_idx, bucket_hash, family_id, target_id) "
        "VALUES (?1, ?2, ?3, ?4, ?5)",
        -1, &ins, NULL) != SQLITE_OK) {
        sqlite3_finalize(mem_q);
        free(projs);
        return -1;
    }

    int indexed = 0;
    while (sqlite3_step(mem_q) == SQLITE_ROW) {
        int fam_id = sqlite3_column_int(mem_q, 0);
        int tid = sqlite3_column_int(mem_q, 1);

        double vec[ANN_MAX_DIM];
        memset(vec, 0, sizeof(vec));
        int dim = extract_vector(db, fam_id, tid, vec, ANN_MAX_DIM);
        if (dim <= 0) continue;

        store_quantized_vector(db, content_type, fam_id, tid, vec, max_dim);

        for (int t = 0; t < ANN_NUM_TABLES; t++) {
            char hash[ANN_HASH_BITS / 4 + 4];
            compute_lsh_hash(&projs[t], vec, dim, hash);

            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 2, t);
            sqlite3_bind_text(ins, 3, hash, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(ins, 4, fam_id);
            sqlite3_bind_int(ins, 5, tid);
            sqlite3_step(ins);
        }
        indexed++;
    }
    sqlite3_finalize(ins);
    sqlite3_finalize(mem_q);
    free(projs);
    return indexed;
}

/* ================================================================
 * ann_index_member — incremental add
 * ================================================================ */

int ann_index_member(sqlite3 *db, int family_id, int target_id) {
    /* Get content type */
    sqlite3_stmt *ctq;
    char ct[256] = {0};
    int base_dim;
    if (sqlite3_prepare_v2(db,
        "SELECT content_type FROM _families WHERE id=?1",
        -1, &ctq, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(ctq, 1, family_id);
    if (sqlite3_step(ctq) == SQLITE_ROW) {
        const char *c = (const char *)sqlite3_column_text(ctq, 0);
        if (c) strncpy(ct, c, 255);
    }
    sqlite3_finalize(ctq);
    if (!ct[0]) return -1;

    double vec[ANN_MAX_DIM];
    memset(vec, 0, sizeof(vec));
    int dim = extract_vector(db, family_id, target_id, vec, ANN_MAX_DIM);
    if (dim <= 0) return -1;

    base_dim = ann_content_dim(db, ct);
    if (base_dim <= 0) base_dim = dim;

    ensure_projections(db, ct, base_dim);
    store_quantized_vector(db, ct, family_id, target_id, vec, base_dim);

    sqlite3_stmt *ins;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _ann_buckets "
        "(content_type, table_idx, bucket_hash, family_id, target_id) "
        "VALUES (?1, ?2, ?3, ?4, ?5)",
        -1, &ins, NULL) != SQLITE_OK) return -1;

    for (int t = 0; t < ANN_NUM_TABLES; t++) {
        ProjTable proj;
        load_projections(db, ct, t, &proj);
        char hash[ANN_HASH_BITS / 4 + 4];
        compute_lsh_hash(&proj, vec, dim, hash);

        sqlite3_reset(ins);
        sqlite3_bind_text(ins, 1, ct, -1, SQLITE_STATIC);
        sqlite3_bind_int(ins, 2, t);
        sqlite3_bind_text(ins, 3, hash, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(ins, 4, family_id);
        sqlite3_bind_int(ins, 5, target_id);
        sqlite3_step(ins);
    }
    sqlite3_finalize(ins);
    return 0;
}

/* ================================================================
 * ann_knn_query — k nearest neighbors via multi-probe LSH
 * ================================================================ */

int ann_knn_query(sqlite3 *db, const char *content_type,
                  const char *query_json, int k, char **out_json) {
    *out_json = NULL;

    double qvec[ANN_MAX_DIM];
    memset(qvec, 0, sizeof(qvec));
    int qdim = extract_vector_from_json(db, content_type, query_json,
                                         qvec, ANN_MAX_DIM);
    if (qdim <= 0) return 0;

    ensure_projections(db, content_type, qdim);

    /* Collect candidate set from all tables */
    typedef struct { int target_id; int family_id; double dist; int votes; } Cand;
    int cand_cap = 256;
    Cand *cands = calloc((size_t)cand_cap, sizeof(Cand));
    int n_cands = 0;

    for (int t = 0; t < ANN_NUM_TABLES; t++) {
        ProjTable proj;
        load_projections(db, content_type, t, &proj);
        char hash[ANN_HASH_BITS / 4 + 4];
        compute_lsh_hash(&proj, qvec, qdim, hash);

        /* Lookup bucket */
        sqlite3_stmt *bq;
        if (sqlite3_prepare_v2(db,
            "SELECT family_id, target_id FROM _ann_buckets "
            "WHERE content_type=?1 AND table_idx=?2 AND bucket_hash=?3",
            -1, &bq, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(bq, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(bq, 2, t);
        sqlite3_bind_text(bq, 3, hash, -1, SQLITE_STATIC);

        while (sqlite3_step(bq) == SQLITE_ROW) {
            int fid = sqlite3_column_int(bq, 0);
            int tid = sqlite3_column_int(bq, 1);

            /* Check if already in candidates */
            int found = -1;
            for (int i = 0; i < n_cands; i++) {
                if (cands[i].target_id == tid) { found = i; break; }
            }
            if (found >= 0) {
                cands[found].votes++;
            } else {
                if (n_cands >= cand_cap) {
                    cand_cap *= 2;
                    cands = realloc(cands, (size_t)cand_cap * sizeof(Cand));
                }
                cands[n_cands].target_id = tid;
                cands[n_cands].family_id = fid;
                cands[n_cands].dist = -1.0;
                cands[n_cands].votes = 1;
                n_cands++;
            }
        }
        sqlite3_finalize(bq);
    }

    /* Compute exact distances for candidates */
    bench_metrics_record_ann_exact(n_cands);
    for (int i = 0; i < n_cands; i++) {
        double cvec[ANN_MAX_DIM];
        memset(cvec, 0, sizeof(cvec));
        int cdim = extract_vector(db, cands[i].family_id, cands[i].target_id,
                                  cvec, ANN_MAX_DIM);
        int d = cdim < qdim ? cdim : qdim;

        /* Euclidean distance */
        double sum_sq = 0.0;
        for (int j = 0; j < d; j++) {
            double diff = qvec[j] - cvec[j];
            sum_sq += diff * diff;
        }
        cands[i].dist = sqrt(sum_sq);
    }

    /* Sort by distance (insertion sort — candidates typically small) */
    for (int i = 1; i < n_cands; i++) {
        Cand tmp = cands[i];
        int j = i - 1;
        while (j >= 0 && cands[j].dist > tmp.dist) {
            cands[j + 1] = cands[j];
            j--;
        }
        cands[j + 1] = tmp;
    }

    /* Format top-k results */
    int result_count = n_cands < k ? n_cands : k;
    size_t cap = 2048;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';

    for (int i = 0; i < result_count; i++) {
        while (off + 128 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (i > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off,
            "{\"target_id\":%d,\"distance\":%.6f,\"votes\":%d}",
            cands[i].target_id, cands[i].dist, cands[i].votes);
    }

    out[off++] = ']'; out[off] = '\0';
    *out_json = out;
    free(cands);
    return result_count;
}

int ann_knn_query_quantized(sqlite3 *db, const char *content_type,
                            const char *query_json, int k, char **out_json) {
    double qvec[ANN_MAX_DIM];
    double qrot[ANN_MAX_DIM];
    int qdim;
    int base_dim;
    int qrot_dim = 0;
    double qnorm = 0.0;
    int cand_cap = 256;
    int n_cands = 0;
    AnnCandidate *cands = NULL;

    *out_json = NULL;
    memset(qvec, 0, sizeof(qvec));
    memset(qrot, 0, sizeof(qrot));

    qdim = extract_vector_from_json(db, content_type, query_json, qvec, ANN_MAX_DIM);
    if (qdim <= 0) return 0;

    base_dim = ann_content_dim(db, content_type);
    if (base_dim <= 0) base_dim = qdim;
    prepare_quant_query(content_type, qvec, base_dim, qrot, &qrot_dim, &qnorm);

    ensure_projections(db, content_type, base_dim);

    cands = calloc((size_t)cand_cap, sizeof(AnnCandidate));
    if (!cands) return 0;

    for (int t = 0; t < ANN_NUM_TABLES; t++) {
        ProjTable proj;
        char hash[ANN_HASH_BITS / 4 + 4];
        sqlite3_stmt *bq;

        load_projections(db, content_type, t, &proj);
        compute_lsh_hash(&proj, qvec, qdim, hash);

        if (sqlite3_prepare_v2(db,
            "SELECT family_id, target_id FROM _ann_buckets "
            "WHERE content_type=?1 AND table_idx=?2 AND bucket_hash=?3",
            -1, &bq, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(bq, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(bq, 2, t);
        sqlite3_bind_text(bq, 3, hash, -1, SQLITE_STATIC);

        while (sqlite3_step(bq) == SQLITE_ROW) {
            int fid = sqlite3_column_int(bq, 0);
            int tid = sqlite3_column_int(bq, 1);
            if (candidate_add_or_vote(&cands, &n_cands, &cand_cap, fid, tid) != 0) {
                sqlite3_finalize(bq);
                free(cands);
                return 0;
            }
        }
        sqlite3_finalize(bq);
    }

    for (int i = 0; i < n_cands; i++) {
        double crot[ANN_MAX_DIM];
        int crot_dim = 0;
        double cnorm = 0.0;

        memset(crot, 0, sizeof(crot));
        if (load_quantized_vector(db, content_type, cands[i].target_id, crot, &crot_dim, &cnorm) <= 0) {
            double cvec[ANN_MAX_DIM];
            memset(cvec, 0, sizeof(cvec));
            if (extract_vector(db, cands[i].family_id, cands[i].target_id, cvec, ANN_MAX_DIM) > 0) {
                prepare_quant_query(content_type, cvec, base_dim, crot, &crot_dim, &cnorm);
            }
        }
        cands[i].dist = quant_space_distance(qrot, qnorm, qrot_dim, crot, cnorm, crot_dim);
    }

    sort_candidates(cands, n_cands);
    {
        int rerank_limit = rerank_quantized_shortlist_exact(db, qvec, qdim, cands, n_cands, k);
        bench_metrics_record_ann_quant(rerank_limit);
        *out_json = format_candidates_json(cands, rerank_limit, k);
    }
    free(cands);
    return n_cands < k ? n_cands : k;
}

/* ================================================================
 * ann_knn_by_entry — k-NN using a stored entry's vector directly
 *
 * Avoids the JSON → bindings → vector extraction pipeline mismatch.
 * Looks up the target_id's stored tensor cells and uses them as the
 * query vector for LSH bucket lookup.
 * ================================================================ */

int ann_knn_by_entry(sqlite3 *db, const char *content_type,
                     int query_target_id, int k, char **out_json) {
    *out_json = NULL;

    /* Find the family for this entry */
    sqlite3_stmt *fq;
    int query_family_id = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT family_id FROM _family_members WHERE target_type=?1 AND target_id=?2 LIMIT 1",
        -1, &fq, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(fq, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(fq, 2, query_target_id);
    if (sqlite3_step(fq) == SQLITE_ROW)
        query_family_id = sqlite3_column_int(fq, 0);
    sqlite3_finalize(fq);
    if (query_family_id <= 0) return 0;

    /* Extract the stored vector for the query entry */
    double qvec[ANN_MAX_DIM];
    memset(qvec, 0, sizeof(qvec));
    int qdim = extract_vector(db, query_family_id, query_target_id, qvec, ANN_MAX_DIM);
    if (qdim <= 0) return 0;

    ensure_projections(db, content_type, qdim);

    /* Brute-force bypass: if few members exist, skip LSH entirely and
       scan all vectors directly — avoids bucket degeneration overhead. */
    int total_members = 0;
    {
        sqlite3_stmt *cntq;
        if (sqlite3_prepare_v2(db,
            "SELECT COUNT(*) FROM _family_members WHERE target_type=?1",
            -1, &cntq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cntq, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(cntq) == SQLITE_ROW)
                total_members = sqlite3_column_int(cntq, 0);
            sqlite3_finalize(cntq);
        }
    }

    typedef struct { int target_id; int family_id; double dist; int votes; } Cand;

    if (total_members > 0 && total_members < ANN_BRUTE_THRESHOLD) {
        /* Linear scan — no LSH overhead */
        int cand_cap = total_members + 1;
        Cand *cands = calloc((size_t)cand_cap, sizeof(Cand));
        int n_cands = 0;

        sqlite3_stmt *allq;
        if (sqlite3_prepare_v2(db,
            "SELECT family_id, target_id FROM _family_members WHERE target_type=?1",
            -1, &allq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(allq, 1, content_type, -1, SQLITE_STATIC);
            while (sqlite3_step(allq) == SQLITE_ROW) {
                int fid = sqlite3_column_int(allq, 0);
                int tid = sqlite3_column_int(allq, 1);
                double cvec[ANN_MAX_DIM];
                memset(cvec, 0, sizeof(cvec));
                int cdim = extract_vector(db, fid, tid, cvec, ANN_MAX_DIM);
                int d = cdim < qdim ? cdim : qdim;
                double sum_sq = 0.0;
                for (int j = 0; j < d; j++) {
                    double diff = qvec[j] - cvec[j];
                    sum_sq += diff * diff;
                }
                cands[n_cands].target_id = tid;
                cands[n_cands].family_id = fid;
                cands[n_cands].dist = sqrt(sum_sq);
                cands[n_cands].votes = 1;
                n_cands++;
            }
            sqlite3_finalize(allq);
        }

        /* Sort by distance */
        bench_metrics_record_ann_exact(n_cands);
        for (int i = 1; i < n_cands; i++) {
            Cand tmp = cands[i];
            int j = i - 1;
            while (j >= 0 && cands[j].dist > tmp.dist) { cands[j + 1] = cands[j]; j--; }
            cands[j + 1] = tmp;
        }

        int result_count = n_cands < k ? n_cands : k;
        size_t cap = 2048;
        char *out = malloc(cap);
        size_t off = 0;
        out[off++] = '[';
        for (int i = 0; i < result_count; i++) {
            while (off + 128 >= cap) { cap *= 2; out = realloc(out, cap); }
            if (i > 0) out[off++] = ',';
            off += (size_t)snprintf(out + off, cap - off,
                "{\"target_id\":%d,\"distance\":%.6f,\"votes\":%d}",
                cands[i].target_id, cands[i].dist, cands[i].votes);
        }
        out[off++] = ']'; out[off] = '\0';
        *out_json = out;
        free(cands);
        return result_count;
    }

    /* Full LSH path for large datasets */
    int cand_cap = 256;
    Cand *cands = calloc((size_t)cand_cap, sizeof(Cand));
    int n_cands = 0;

    for (int t = 0; t < ANN_NUM_TABLES; t++) {
        ProjTable proj;
        load_projections(db, content_type, t, &proj);
        char hash[ANN_HASH_BITS / 4 + 4];
        compute_lsh_hash(&proj, qvec, qdim, hash);

        sqlite3_stmt *bq;
        if (sqlite3_prepare_v2(db,
            "SELECT family_id, target_id FROM _ann_buckets "
            "WHERE content_type=?1 AND table_idx=?2 AND bucket_hash=?3",
            -1, &bq, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(bq, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(bq, 2, t);
        sqlite3_bind_text(bq, 3, hash, -1, SQLITE_STATIC);

        while (sqlite3_step(bq) == SQLITE_ROW) {
            int fid = sqlite3_column_int(bq, 0);
            int tid = sqlite3_column_int(bq, 1);
            int found = -1;
            for (int i = 0; i < n_cands; i++) {
                if (cands[i].target_id == tid) { found = i; break; }
            }
            if (found >= 0) {
                cands[found].votes++;
            } else {
                if (n_cands >= cand_cap) {
                    cand_cap *= 2;
                    cands = realloc(cands, (size_t)cand_cap * sizeof(Cand));
                }
                cands[n_cands].target_id = tid;
                cands[n_cands].family_id = fid;
                cands[n_cands].dist = -1.0;
                cands[n_cands].votes = 1;
                n_cands++;
            }
        }
        sqlite3_finalize(bq);
    }

    /* Compute exact distances */
    bench_metrics_record_ann_exact(n_cands);
    for (int i = 0; i < n_cands; i++) {
        double cvec[ANN_MAX_DIM];
        memset(cvec, 0, sizeof(cvec));
        int cdim = extract_vector(db, cands[i].family_id, cands[i].target_id,
                                  cvec, ANN_MAX_DIM);
        int d = cdim < qdim ? cdim : qdim;
        double sum_sq = 0.0;
        for (int j = 0; j < d; j++) {
            double diff = qvec[j] - cvec[j];
            sum_sq += diff * diff;
        }
        cands[i].dist = sqrt(sum_sq);
    }

    /* Sort by distance */
    for (int i = 1; i < n_cands; i++) {
        Cand tmp = cands[i];
        int j = i - 1;
        while (j >= 0 && cands[j].dist > tmp.dist) {
            cands[j + 1] = cands[j];
            j--;
        }
        cands[j + 1] = tmp;
    }

    /* Format results */
    int result_count = n_cands < k ? n_cands : k;
    size_t cap = 2048;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    for (int i = 0; i < result_count; i++) {
        while (off + 128 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (i > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off,
            "{\"target_id\":%d,\"distance\":%.6f,\"votes\":%d}",
            cands[i].target_id, cands[i].dist, cands[i].votes);
    }
    out[off++] = ']'; out[off] = '\0';
    *out_json = out;
    free(cands);
    return result_count;
}

int ann_knn_by_entry_quantized(sqlite3 *db, const char *content_type,
                               int query_target_id, int k, char **out_json) {
    sqlite3_stmt *fq;
    int query_family_id = 0;
    double qvec[ANN_MAX_DIM];
    double qrot[ANN_MAX_DIM];
    int qdim;
    int base_dim;
    int qrot_dim = 0;
    double qnorm = 0.0;
    int total_members = 0;
    int cand_cap = 256;
    int n_cands = 0;
    AnnCandidate *cands = NULL;

    *out_json = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT family_id FROM _family_members WHERE target_type=?1 AND target_id=?2 LIMIT 1",
        -1, &fq, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(fq, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(fq, 2, query_target_id);
    if (sqlite3_step(fq) == SQLITE_ROW) query_family_id = sqlite3_column_int(fq, 0);
    sqlite3_finalize(fq);
    if (query_family_id <= 0) return 0;

    memset(qvec, 0, sizeof(qvec));
    memset(qrot, 0, sizeof(qrot));
    qdim = extract_vector(db, query_family_id, query_target_id, qvec, ANN_MAX_DIM);
    if (qdim <= 0) return 0;

    base_dim = ann_content_dim(db, content_type);
    if (base_dim <= 0) base_dim = qdim;
    prepare_quant_query(content_type, qvec, base_dim, qrot, &qrot_dim, &qnorm);
    ensure_projections(db, content_type, base_dim);

    {
        sqlite3_stmt *cntq;
        if (sqlite3_prepare_v2(db,
            "SELECT COUNT(*) FROM _family_members WHERE target_type=?1",
            -1, &cntq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cntq, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(cntq) == SQLITE_ROW) total_members = sqlite3_column_int(cntq, 0);
            sqlite3_finalize(cntq);
        }
    }

    if (total_members > 0 && total_members < ANN_BRUTE_THRESHOLD) {
        sqlite3_stmt *allq;
        cand_cap = total_members + 1;
        cands = calloc((size_t)cand_cap, sizeof(AnnCandidate));
        if (!cands) return 0;

        if (sqlite3_prepare_v2(db,
            "SELECT family_id, target_id FROM _family_members WHERE target_type=?1",
            -1, &allq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(allq, 1, content_type, -1, SQLITE_STATIC);
            while (sqlite3_step(allq) == SQLITE_ROW) {
                int fid = sqlite3_column_int(allq, 0);
                int tid = sqlite3_column_int(allq, 1);
                double crot[ANN_MAX_DIM];
                int crot_dim = 0;
                double cnorm = 0.0;

                memset(crot, 0, sizeof(crot));
                if (load_quantized_vector(db, content_type, tid, crot, &crot_dim, &cnorm) <= 0) {
                    double cvec[ANN_MAX_DIM];
                    memset(cvec, 0, sizeof(cvec));
                    if (extract_vector(db, fid, tid, cvec, ANN_MAX_DIM) > 0) {
                        prepare_quant_query(content_type, cvec, base_dim, crot, &crot_dim, &cnorm);
                    }
                }

                cands[n_cands].target_id = tid;
                cands[n_cands].family_id = fid;
                cands[n_cands].votes = 1;
                cands[n_cands].dist = quant_space_distance(qrot, qnorm, qrot_dim, crot, cnorm, crot_dim);
                n_cands++;
            }
            sqlite3_finalize(allq);
        }
    } else {
        cands = calloc((size_t)cand_cap, sizeof(AnnCandidate));
        if (!cands) return 0;

        for (int t = 0; t < ANN_NUM_TABLES; t++) {
            ProjTable proj;
            char hash[ANN_HASH_BITS / 4 + 4];
            sqlite3_stmt *bq;

            load_projections(db, content_type, t, &proj);
            compute_lsh_hash(&proj, qvec, qdim, hash);

            if (sqlite3_prepare_v2(db,
                "SELECT family_id, target_id FROM _ann_buckets "
                "WHERE content_type=?1 AND table_idx=?2 AND bucket_hash=?3",
                -1, &bq, NULL) != SQLITE_OK) continue;
            sqlite3_bind_text(bq, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_bind_int(bq, 2, t);
            sqlite3_bind_text(bq, 3, hash, -1, SQLITE_STATIC);

            while (sqlite3_step(bq) == SQLITE_ROW) {
                int fid = sqlite3_column_int(bq, 0);
                int tid = sqlite3_column_int(bq, 1);
                if (candidate_add_or_vote(&cands, &n_cands, &cand_cap, fid, tid) != 0) {
                    sqlite3_finalize(bq);
                    free(cands);
                    return 0;
                }
            }
            sqlite3_finalize(bq);
        }

        for (int i = 0; i < n_cands; i++) {
            double crot[ANN_MAX_DIM];
            int crot_dim = 0;
            double cnorm = 0.0;

            memset(crot, 0, sizeof(crot));
            if (load_quantized_vector(db, content_type, cands[i].target_id, crot, &crot_dim, &cnorm) <= 0) {
                double cvec[ANN_MAX_DIM];
                memset(cvec, 0, sizeof(cvec));
                if (extract_vector(db, cands[i].family_id, cands[i].target_id, cvec, ANN_MAX_DIM) > 0) {
                    prepare_quant_query(content_type, cvec, base_dim, crot, &crot_dim, &cnorm);
                }
            }
            cands[i].dist = quant_space_distance(qrot, qnorm, qrot_dim, crot, cnorm, crot_dim);
        }
    }

    sort_candidates(cands, n_cands);
    {
        int rerank_limit = rerank_quantized_shortlist_exact(db, qvec, qdim, cands, n_cands, k);
        bench_metrics_record_ann_quant(rerank_limit);
        *out_json = format_candidates_json(cands, rerank_limit, k);
    }
    free(cands);
    return n_cands < k ? n_cands : k;
}

/* ================================================================
 * ann_range_query — similarity search within threshold
 * ================================================================ */

int ann_range_query(sqlite3 *db, const char *content_type,
                    const char *query_json, double threshold,
                    char **out_json) {
    /* Reuse knn with high k, then filter by threshold */
    char *knn_json = NULL;
    int n = ann_knn_query(db, content_type, query_json, 10000, &knn_json);
    if (n <= 0 || !knn_json) {
        *out_json = strdup("[]");
        free(knn_json);
        return 0;
    }

    /* Filter: parse knn_json and keep only distance < threshold */
    size_t cap = 2048;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int count = 0;

    /* Simple parse: look for "target_id":N,"distance":D patterns */
    const char *p = knn_json;
    while (*p) {
        const char *tid_tag = strstr(p, "\"target_id\":");
        if (!tid_tag) break;
        tid_tag += 12;
        int tid = atoi(tid_tag);

        const char *dist_tag = strstr(tid_tag, "\"distance\":");
        if (!dist_tag) break;
        dist_tag += 11;
        double dist = strtod(dist_tag, NULL);

        if (dist <= threshold) {
            while (off + 64 >= cap) { cap *= 2; out = realloc(out, cap); }
            if (count > 0) out[off++] = ',';
            off += (size_t)snprintf(out + off, cap - off,
                "{\"target_id\":%d,\"distance\":%.6f}", tid, dist);
            count++;
        }

        p = dist_tag + 1;
    }

    out[off++] = ']'; out[off] = '\0';
    *out_json = out;
    free(knn_json);
    return count;
}

/* ================================================================
 * ann_remove_member
 * ================================================================ */

int ann_remove_member(sqlite3 *db, int family_id, int target_id) {
    (void)family_id;
    sqlite3_stmt *del;
    if (sqlite3_prepare_v2(db,
        "DELETE FROM _ann_buckets WHERE target_id=?1",
        -1, &del, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(del, 1, target_id);
    sqlite3_step(del);
    sqlite3_finalize(del);
    if (sqlite3_prepare_v2(db,
        "DELETE FROM _ann_quantized WHERE target_id=?1",
        -1, &del, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(del, 1, target_id);
    sqlite3_step(del);
    sqlite3_finalize(del);
    return 0;
}

/* ================================================================
 * ann_stats
 * ================================================================ */

int ann_stats(sqlite3 *db, const char *content_type, char **out_json) {
    *out_json = NULL;

    int total_entries = 0, total_bucket_entries = 0;
    int unique_buckets = 0;
    int quantized_members = 0;
    double avg_bucket_size = 0.0;
    long long quantized_bytes = 0;

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(DISTINCT target_id), COUNT(*) "
        "FROM _ann_buckets WHERE content_type=?1",
        -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(q) == SQLITE_ROW) {
            total_entries = sqlite3_column_int(q, 0);
            total_bucket_entries = sqlite3_column_int(q, 1);
        }
        sqlite3_finalize(q);
    }

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM ("
        "  SELECT table_idx, bucket_hash "
        "  FROM _ann_buckets WHERE content_type=?1 "
        "  GROUP BY table_idx, bucket_hash"
        ")",
        -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(q) == SQLITE_ROW) {
            unique_buckets = sqlite3_column_int(q, 0);
        }
        sqlite3_finalize(q);
    }
    avg_bucket_size = unique_buckets > 0
        ? (double)total_bucket_entries / (double)unique_buckets : 0.0;

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*), COALESCE(SUM(LENGTH(q4_codes) + LENGTH(residual_bits)),0) "
        "FROM _ann_quantized WHERE content_type=?1",
        -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(q) == SQLITE_ROW) {
            quantized_members = sqlite3_column_int(q, 0);
            quantized_bytes = sqlite3_column_int64(q, 1);
        }
        sqlite3_finalize(q);
    }

    char *out = malloc(768);
    if (!out) return -1;
    snprintf(out, 768,
        "{\"indexed_members\":%d,\"total_bucket_entries\":%d,"
        "\"tables\":%d,\"hash_bits\":%d,\"avg_bucket_size\":%.2f,"
        "\"quantized_members\":%d,\"quantized_bytes\":%lld,"
        "\"quantized_bytes_per_member\":%.1f}",
        total_entries, total_bucket_entries,
        ANN_NUM_TABLES, ANN_HASH_BITS, avg_bucket_size,
        quantized_members, quantized_bytes,
        quantized_members > 0 ? (double)quantized_bytes / (double)quantized_members : 0.0);
    *out_json = out;
    return 0;
}

int ann_quant_bench(sqlite3 *db, const char *content_type,
                    int query_limit, int k, char **out_json) {
    sqlite3_stmt *q;
    int queries = 0;
    int exact_self_hits = 0;
    int quant_self_hits = 0;
    int exact_returned = 0;
    int quant_returned = 0;
    int overlap_sum = 0;
    int overlap_pairs = 0;
    int quantized_members = 0;
    long long quantized_bytes = 0;

    *out_json = NULL;
    if (query_limit <= 0) query_limit = 50;
    if (k <= 0) k = 10;

    if (sqlite3_prepare_v2(db,
        "SELECT target_id FROM _family_members WHERE target_type=?1 ORDER BY target_id LIMIT ?2",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(q, 2, query_limit);

    while (sqlite3_step(q) == SQLITE_ROW) {
        int target_id = sqlite3_column_int(q, 0);
        char *exact_json = NULL;
        char *quant_json = NULL;
        int exact_ids[64];
        int quant_ids[64];
        int exact_n = ann_knn_by_entry(db, content_type, target_id, k, &exact_json);
        int quant_n = ann_knn_by_entry_quantized(db, content_type, target_id, k, &quant_json);
        int exact_count = parse_knn_ids(exact_json, exact_ids, 64);
        int quant_count = parse_knn_ids(quant_json, quant_ids, 64);

        if (exact_n > 0) {
            exact_returned += exact_n;
            if (ids_contains(exact_ids, exact_count, target_id)) exact_self_hits++;
        }
        if (quant_n > 0) {
            quant_returned += quant_n;
            if (ids_contains(quant_ids, quant_count, target_id)) quant_self_hits++;
        }
        if (exact_count > 0 && quant_count > 0) {
            overlap_sum += ids_overlap(exact_ids, exact_count, quant_ids, quant_count);
            overlap_pairs++;
        }

        queries++;
        free(exact_json);
        free(quant_json);
    }
    sqlite3_finalize(q);

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*), COALESCE(SUM(LENGTH(q4_codes) + LENGTH(residual_bits)),0) "
        "FROM _ann_quantized WHERE content_type=?1",
        -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(q) == SQLITE_ROW) {
            quantized_members = sqlite3_column_int(q, 0);
            quantized_bytes = sqlite3_column_int64(q, 1);
        }
        sqlite3_finalize(q);
    }

    {
        char *out = malloc(1024);
        int rerank_limit = ann_quant_target_rerank(k);
        if (!out) return -1;
        snprintf(out, 1024,
            "{\"queries\":%d,\"k\":%d,"
            "\"quant_rerank_shortlist\":%d,"
            "\"exact_self_recall_pct\":%.1f,\"quant_self_recall_pct\":%.1f,"
            "\"topk_overlap_pct\":%.1f,\"exact_avg_k\":%.1f,\"quant_avg_k\":%.1f,"
            "\"quantized_members\":%d,\"quantized_bytes\":%lld,"
            "\"quantized_bytes_per_member\":%.1f}",
            queries, k, rerank_limit,
            queries > 0 ? (100.0 * (double)exact_self_hits / (double)queries) : 0.0,
            queries > 0 ? (100.0 * (double)quant_self_hits / (double)queries) : 0.0,
            overlap_pairs > 0 ? (100.0 * (double)overlap_sum / (double)(overlap_pairs * k)) : 0.0,
            queries > 0 ? (double)exact_returned / (double)queries : 0.0,
            queries > 0 ? (double)quant_returned / (double)queries : 0.0,
            quantized_members, quantized_bytes,
            quantized_members > 0 ? (double)quantized_bytes / (double)quantized_members : 0.0);
        *out_json = out;
    }

    return 0;
}
