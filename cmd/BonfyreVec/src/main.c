/*
 * BonfyreVec — local vector search via sqlite-vec.
 *
 * Single-file retrieval system. No server. No Weaviate. No Python.
 * SQLite C API + vec0 loadable extension = embedded semantic search.
 *
 * Usage:
 *   bonfyre-vec init <db>                          → create vector table
 *   bonfyre-vec insert <db> <embeddings.json>      → bulk insert vectors
 *   bonfyre-vec search <db> <query-embedding.json> [--top N]  → nearest neighbors
 *   bonfyre-vec search <db> <query.json> --exact [--top N]    → exact SIMD cosine
 *   bonfyre-vec compare <db> <id1> <id2>           → pairwise cosine similarity
 *   bonfyre-vec count <db>                         → row count
 */
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <math.h>
#include <sqlite3.h>
#ifdef __ARM_NEON
#include <arm_neon.h>
#endif

#define VEC_DIMS 384
#define VECF_MAGIC 0x46434556u  /* "VECF" little-endian */

/* ── binary vector reader (VECF format) ─────────────────────── */

static int read_vector_binary(const char *path, float *out, int max_dims) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    uint32_t magic, d;
    if (fread(&magic, 4, 1, f) != 1 || magic != VECF_MAGIC) { fclose(f); return 0; }
    if (fread(&d, 4, 1, f) != 1 || (int)d > max_dims || d == 0) { fclose(f); return 0; }
    size_t rd = fread(out, sizeof(float), (size_t)d, f);
    fclose(f);
    return (int)rd;
}

/* detect VECF magic in first 4 bytes */
static int is_vecf_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    uint32_t magic;
    int ok = (fread(&magic, 4, 1, f) == 1 && magic == VECF_MAGIC);
    fclose(f);
    return ok;
}

/* ── sqlite-vec extension resolution ─────────────────────────── */

static const char *resolve_vec_ext(void) {
    const char *env = getenv("BONFYRE_VEC_EXT");
    if (env && env[0]) return env;
    static const char *paths[] = {
        /* macOS Homebrew */
        "/opt/homebrew/lib/sqlite_vec/vec0",
        /* Linux system-wide */
        "/usr/lib/x86_64-linux-gnu/sqlite_vec/vec0",
        "/usr/lib/aarch64-linux-gnu/sqlite_vec/vec0",
        "/usr/lib/sqlite_vec/vec0",
        /* universal fallback */
        "/usr/local/lib/sqlite_vec/vec0",
        NULL
    };
    for (int i = 0; paths[i]; i++) {
        char buf[PATH_MAX];
        snprintf(buf, sizeof(buf), "%s.dylib", paths[i]);
        struct stat st;
        if (stat(buf, &st) == 0) return paths[i];
        snprintf(buf, sizeof(buf), "%s.so", paths[i]);
        if (stat(buf, &st) == 0) return paths[i];
    }
    return "vec0";  /* last resort: let SQLite search LD_LIBRARY_PATH */
}

static int load_vec_ext(sqlite3 *db) {
    const char *ext = resolve_vec_ext();
    char *err = NULL;
    int rc = sqlite3_load_extension(db, ext, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[vec] Failed to load sqlite-vec extension '%s': %s\n",
                ext, err ? err : "unknown");
        sqlite3_free(err);
        return -1;
    }
    return 0;
}

/* ── SIMD cosine similarity ──────────────────────────────────── */

static float cosine_similarity(const float *a, const float *b, int n) {
#ifdef __ARM_NEON
    float32x4_t s_ab = vdupq_n_f32(0), s_aa = vdupq_n_f32(0), s_bb = vdupq_n_f32(0);
    int i = 0;
    for (; i + 4 <= n; i += 4) {
        float32x4_t va = vld1q_f32(a + i), vb = vld1q_f32(b + i);
        s_ab = vfmaq_f32(s_ab, va, vb);
        s_aa = vfmaq_f32(s_aa, va, va);
        s_bb = vfmaq_f32(s_bb, vb, vb);
    }
    float dot = vaddvq_f32(s_ab), na = vaddvq_f32(s_aa), nb = vaddvq_f32(s_bb);
    for (; i < n; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
#else
    float dot = 0, na = 0, nb = 0;
    for (int i = 0; i < n; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
#endif
    float denom = sqrtf(na) * sqrtf(nb);
    return denom > 0.0f ? dot / denom : 0.0f;
}

/* Read a vector blob from vec_artifacts by ID */
static int read_vec_from_db(sqlite3 *db, const char *id, float *out, int max_dims) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT embedding FROM vec_artifacts WHERE id = ?", -1, &stmt, NULL);
    if (rc != SQLITE_OK) return 0;
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
    int dims = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const void *blob = sqlite3_column_blob(stmt, 0);
        int bytes = sqlite3_column_bytes(stmt, 0);
        dims = bytes / (int)sizeof(float);
        if (dims > max_dims) dims = max_dims;
        if (blob) memcpy(out, blob, (size_t)dims * sizeof(float));
    }
    sqlite3_finalize(stmt);
    return dims;
}

/* Result struct for exact search */
typedef struct {
    char id[256];
    float score;
    char source[512];
    char type[128];
} VecResult;

static int cmp_results_desc(const void *a, const void *b) {
    float sa = ((const VecResult *)a)->score;
    float sb = ((const VecResult *)b)->score;
    if (sb > sa) return 1;
    if (sb < sa) return -1;
    return 0;
}

/* ── tiny JSON helpers ──────────────────────────────────────── */

static const char *json_skip_ws(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

static const char *json_str_value(const char *json, const char *key,
                                   char *buf, size_t bufsz) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    p = json_skip_ws(p);
    if (*p != ':') return NULL;
    p = json_skip_ws(p + 1);
    if (*p != '"') return NULL;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < bufsz) {
        if (*p == '\\' && p[1]) { p++; }
        buf[i++] = *p++;
    }
    buf[i] = '\0';
    return buf;
}

static int json_parse_float_array(const char *json, const char *key,
                                   float *out, int max_count) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    p = json_skip_ws(p);
    if (*p != ':') return 0;
    p = json_skip_ws(p + 1);
    if (*p != '[') return 0;
    p++;
    int count = 0;
    while (*p && *p != ']' && count < max_count) {
        p = json_skip_ws(p);
        if (*p == ']') break;
        char *end;
        out[count++] = strtof(p, &end);
        p = end;
        p = json_skip_ws(p);
        if (*p == ',') p++;
    }
    return count;
}

static const char *json_find_embeddings_array(const char *json) {
    const char *p = strstr(json, "\"embeddings\"");
    if (p) {
        p += strlen("\"embeddings\"");
        p = json_skip_ws(p);
        if (*p == ':') p = json_skip_ws(p + 1);
        if (*p == '[') return p;
    }
    p = json_skip_ws(json);
    if (*p == '[') return p;
    return NULL;
}

static const char *json_skip_object(const char *p) {
    if (*p != '{') return p;
    int depth = 1;
    p++;
    int in_string = 0;
    while (*p && depth > 0) {
        if (in_string) {
            if (*p == '\\') { p++; if (*p) p++; continue; }
            if (*p == '"') in_string = 0;
        } else {
            if (*p == '"') in_string = 1;
            else if (*p == '{') depth++;
            else if (*p == '}') depth--;
        }
        p++;
    }
    return p;
}

/* ── commands ───────────────────────────────────────────────── */

static int cmd_init(const char *db_path) {
    fprintf(stderr, "[vec] Initializing vector DB: %s\n", db_path);

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); return 1; }

    char *err = NULL;
    int rc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS artifacts("
        "  id TEXT PRIMARY KEY,"
        "  source TEXT,"
        "  type TEXT,"
        "  text TEXT,"
        "  metadata TEXT,"
        "  created_at TEXT"
        ")", NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[vec] Failed to create artifacts table: %s\n", err);
        sqlite3_free(err);
        sqlite3_close(db);
        return 1;
    }

    char sql[256];
    snprintf(sql, sizeof(sql),
        "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0("
        "  id TEXT PRIMARY KEY,"
        "  embedding float[%d]"
        ")", VEC_DIMS);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[vec] Failed to create vec_artifacts table: %s\n", err);
        sqlite3_free(err);
        sqlite3_close(db);
        return 1;
    }

    sqlite3_close(db);
    printf("OK: %s initialized\n", db_path);
    return 0;
}

static int cmd_insert(const char *db_path, const char *json_path) {
    fprintf(stderr, "[vec] Inserting vectors from %s into %s\n", json_path, db_path);

    FILE *f = fopen(json_path, "rb");
    if (!f) { fprintf(stderr, "[vec] Cannot open %s\n", json_path); return 1; }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *json = malloc((size_t)sz + 1);
    if (!json) { fclose(f); return 1; }
    fread(json, 1, (size_t)sz, f);
    json[sz] = '\0';
    fclose(f);

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        free(json);
        return 1;
    }
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); free(json); return 1; }

    sqlite3_stmt *meta_stmt = NULL;
    sqlite3_stmt *vec_stmt = NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO artifacts(id, source, type, text, metadata, created_at) "
        "VALUES (?,?,?,?,?,datetime('now'))",
        -1, &meta_stmt, NULL);
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO vec_artifacts(id, embedding) VALUES (?,?)",
        -1, &vec_stmt, NULL);

    if (!meta_stmt || !vec_stmt) {
        fprintf(stderr, "[vec] Failed to prepare statements: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(meta_stmt);
        sqlite3_finalize(vec_stmt);
        sqlite3_close(db);
        free(json);
        return 1;
    }

    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);

    const char *arr = json_find_embeddings_array(json);
    int count = 0;
    if (arr) {
        const char *p = arr + 1;
        while (*p) {
            p = json_skip_ws(p);
            if (*p == ']') break;
            if (*p == ',') { p++; continue; }
            if (*p != '{') break;

            const char *obj_start = p;
            const char *obj_end = json_skip_object(p);
            size_t obj_len = (size_t)(obj_end - obj_start);
            char *obj = malloc(obj_len + 1);
            if (!obj) break;
            memcpy(obj, obj_start, obj_len);
            obj[obj_len] = '\0';

            char id[256] = "", source[512] = "", type[128] = "", text[4096] = "";
            float embedding[VEC_DIMS];
            json_str_value(obj, "id", id, sizeof(id));
            json_str_value(obj, "source", source, sizeof(source));
            json_str_value(obj, "type", type, sizeof(type));
            json_str_value(obj, "text", text, sizeof(text));
            int dims = json_parse_float_array(obj, "embedding", embedding, VEC_DIMS);

            if (id[0] == '\0') snprintf(id, sizeof(id), "%d", count);

            if (dims > 0) {
                sqlite3_reset(meta_stmt);
                sqlite3_bind_text(meta_stmt, 1, id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 2, source, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 3, type, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 4, text, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 5, "{}", -1, SQLITE_STATIC);
                sqlite3_step(meta_stmt);

                sqlite3_reset(vec_stmt);
                sqlite3_bind_text(vec_stmt, 1, id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_blob(vec_stmt, 2, embedding,
                                  dims * (int)sizeof(float), SQLITE_TRANSIENT);
                sqlite3_step(vec_stmt);
                count++;
            }

            free(obj);
            p = obj_end;
        }
    }

    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_finalize(meta_stmt);
    sqlite3_finalize(vec_stmt);
    sqlite3_close(db);
    free(json);

    printf("%d vectors inserted\n", count);
    return 0;
}

static int cmd_search(const char *db_path, const char *query_file, int top_k) {
    fprintf(stderr, "[vec] Searching %s (top %d)\n", db_path, top_k);

    float query_vec[VEC_DIMS];
    int dims = 0;

    /* Try VECF binary first, then JSON */
    if (is_vecf_file(query_file)) {
        dims = read_vector_binary(query_file, query_vec, VEC_DIMS);
        if (dims > 0) fprintf(stderr, "[vec] Read %d dims from VECF binary\n", dims);
    }
    if (dims == 0) {
        FILE *f = fopen(query_file, "rb");
        if (!f) { fprintf(stderr, "[vec] Cannot open %s\n", query_file); return 1; }
        fseek(f, 0, SEEK_END);
        long sz = ftell(f);
        fseek(f, 0, SEEK_SET);
        char *json = malloc((size_t)sz + 1);
        if (!json) { fclose(f); return 1; }
        fread(json, 1, (size_t)sz, f);
        json[sz] = '\0';
        fclose(f);
        dims = json_parse_float_array(json, "embedding", query_vec, VEC_DIMS);
        if (dims == 0) dims = json_parse_float_array(json, "vector", query_vec, VEC_DIMS);
        free(json);
    }

    if (dims == 0) {
        fprintf(stderr, "[vec] No embedding found in %s\n", query_file);
        return 1;
    }

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); return 1; }

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT v.id, v.distance, a.source, a.type, a.text "
        "FROM vec_artifacts v "
        "LEFT JOIN artifacts a ON v.id = a.id "
        "WHERE v.embedding MATCH ? AND k = ? "
        "ORDER BY v.distance",
        -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[vec] Query prepare failed: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    sqlite3_bind_blob(stmt, 1, query_vec, dims * (int)sizeof(float), SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, top_k);

    printf("{\n  \"results\": [\n");
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (!first) printf(",\n");
        first = 0;
        const char *rid = (const char *)sqlite3_column_text(stmt, 0);
        double dist = sqlite3_column_double(stmt, 1);
        const char *src = (const char *)sqlite3_column_text(stmt, 2);
        const char *typ = (const char *)sqlite3_column_text(stmt, 3);
        const char *txt = (const char *)sqlite3_column_text(stmt, 4);
        printf("    {\"id\":\"%s\",\"distance\":%.6f,\"source\":\"%s\","
               "\"type\":\"%s\",\"text\":\"%s\"}",
               rid ? rid : "", dist,
               src ? src : "", typ ? typ : "", txt ? txt : "");
    }
    printf("\n  ],\n  \"query_file\": \"%s\",\n  \"top_k\": %d\n}\n", query_file, top_k);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

static int cmd_count(const char *db_path) {
    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); return 1; }

    sqlite3_stmt *stmt = NULL;
    int meta = 0, vec = 0;

    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM artifacts", -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) meta = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    stmt = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM vec_artifacts", -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) vec = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    printf("artifacts: %d, vectors: %d\n", meta, vec);
    return 0;
}

/* ── compare: pairwise SIMD cosine ──────────────────────────── */

static int cmd_compare(const char *db_path, const char *id1, const char *id2) {
    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); return 1; }

    float v1[VEC_DIMS], v2[VEC_DIMS];
    int d1 = read_vec_from_db(db, id1, v1, VEC_DIMS);
    int d2 = read_vec_from_db(db, id2, v2, VEC_DIMS);
    sqlite3_close(db);

    if (d1 == 0) { fprintf(stderr, "[vec] No vector for '%s'\n", id1); return 1; }
    if (d2 == 0) { fprintf(stderr, "[vec] No vector for '%s'\n", id2); return 1; }
    if (d1 != d2) { fprintf(stderr, "[vec] Dimension mismatch: %d vs %d\n", d1, d2); return 1; }

    float sim = cosine_similarity(v1, v2, d1);
    printf("{\"id1\":\"%s\",\"id2\":\"%s\",\"cosine_similarity\":%.8f,"
           "\"distance\":%.8f,\"dims\":%d}\n",
           id1, id2, sim, 1.0f - sim, d1);
    return 0;
}

/* ── exact search: brute-force SIMD cosine scan ─────────────── */

static int cmd_search_exact(const char *db_path, const char *query_file, int top_k) {
    fprintf(stderr, "[vec] Exact SIMD cosine search %s (top %d)\n", db_path, top_k);

    float query_vec[VEC_DIMS];
    int dims = 0;

    if (is_vecf_file(query_file)) {
        dims = read_vector_binary(query_file, query_vec, VEC_DIMS);
        if (dims > 0) fprintf(stderr, "[vec] Read %d dims from VECF binary\n", dims);
    }
    if (dims == 0) {
        FILE *f = fopen(query_file, "rb");
        if (!f) { fprintf(stderr, "[vec] Cannot open %s\n", query_file); return 1; }
        fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
        char *json = malloc((size_t)sz + 1);
        if (!json) { fclose(f); return 1; }
        fread(json, 1, (size_t)sz, f); json[sz] = '\0'; fclose(f);
        dims = json_parse_float_array(json, "embedding", query_vec, VEC_DIMS);
        if (dims == 0) dims = json_parse_float_array(json, "vector", query_vec, VEC_DIMS);
        free(json);
    }
    if (dims == 0) { fprintf(stderr, "[vec] No embedding in %s\n", query_file); return 1; }

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); return 1; }

    /* Count vectors for allocation */
    int total = 0;
    sqlite3_stmt *cnt = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM vec_artifacts", -1, &cnt, NULL) == SQLITE_OK) {
        if (sqlite3_step(cnt) == SQLITE_ROW) total = sqlite3_column_int(cnt, 0);
        sqlite3_finalize(cnt);
    }
    if (total == 0) {
        printf("{\"results\":[],\"query_file\":\"%s\",\"top_k\":%d,\"mode\":\"exact-simd\"}\n",
               query_file, top_k);
        sqlite3_close(db); return 0;
    }

    VecResult *results = calloc((size_t)total, sizeof(VecResult));
    if (!results) { sqlite3_close(db); return 1; }

    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db,
        "SELECT v.id, v.embedding, a.source, a.type "
        "FROM vec_artifacts v LEFT JOIN artifacts a ON v.id = a.id",
        -1, &stmt, NULL);

    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && count < total) {
        const char *id = (const char *)sqlite3_column_text(stmt, 0);
        const void *blob = sqlite3_column_blob(stmt, 1);
        int bytes = sqlite3_column_bytes(stmt, 1);
        const char *src = (const char *)sqlite3_column_text(stmt, 2);
        const char *typ = (const char *)sqlite3_column_text(stmt, 3);

        int vdims = bytes / (int)sizeof(float);
        if (vdims != dims || !blob) continue;

        VecResult *r = &results[count++];
        snprintf(r->id, sizeof(r->id), "%s", id ? id : "");
        r->score = cosine_similarity(query_vec, (const float *)blob, dims);
        snprintf(r->source, sizeof(r->source), "%s", src ? src : "");
        snprintf(r->type, sizeof(r->type), "%s", typ ? typ : "");
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    qsort(results, (size_t)count, sizeof(VecResult), cmp_results_desc);

    int show = count < top_k ? count : top_k;
    printf("{\n  \"results\": [\n");
    for (int i = 0; i < show; i++) {
        if (i > 0) printf(",\n");
        printf("    {\"id\":\"%s\",\"cosine_similarity\":%.8f,"
               "\"distance\":%.8f,\"source\":\"%s\",\"type\":\"%s\"}",
               results[i].id, results[i].score, 1.0f - results[i].score,
               results[i].source, results[i].type);
    }
    printf("\n  ],\n  \"query_file\": \"%s\",\n  \"top_k\": %d,"
           "\n  \"mode\": \"exact-simd\",\n  \"scanned\": %d\n}\n",
           query_file, top_k, count);

    free(results);
    return 0;
}

typedef struct {
    char id[256];
    float base;
    float sae_overlap;
    float score;
} RerankRow;

static int cmp_rerank_desc(const void *a, const void *b) {
    float sa = ((const RerankRow *)a)->score;
    float sb = ((const RerankRow *)b)->score;
    if (sb > sa) return 1;
    if (sb < sa) return -1;
    return 0;
}

static char *read_text_file(const char *path, long *size_out) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size <= 0 || size > (16 * 1024 * 1024)) {
        fclose(fp);
        return NULL;
    }
    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(fp);
        return NULL;
    }
    size_t n = fread(buffer, 1, (size_t)size, fp);
    fclose(fp);
    if ((long)n != size) {
        free(buffer);
        return NULL;
    }
    buffer[size] = '\0';
    if (size_out) *size_out = size;
    return buffer;
}

static int extract_feature_ids_json(const char *json, int *ids, int max_ids) {
    const char *needle = "\"feature_id\"";
    int count = 0;
    const char *p = json;
    while ((p = strstr(p, needle)) && count < max_ids) {
        p += strlen(needle);
        p = strchr(p, ':');
        if (!p) break;
        p++;
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
        ids[count++] = atoi(p);
    }
    return count;
}

static int id_exists(const int *ids, int n, int id) {
    for (int i = 0; i < n; i++) if (ids[i] == id) return 1;
    return 0;
}

static float feature_overlap_jaccard(const int *a, int na, const int *b, int nb) {
    if (na <= 0 || nb <= 0) return 0.0f;
    int inter = 0;
    for (int i = 0; i < na; i++) {
        if (id_exists(b, nb, a[i])) inter++;
    }
    int uni = na + nb - inter;
    return uni > 0 ? (float)inter / (float)uni : 0.0f;
}

static int parse_vec_results(const char *json, RerankRow *rows, int max_rows) {
    int count = 0;
    const char *p = json;
    const char *id_key = "\"id\":\"";
    while ((p = strstr(p, id_key)) && count < max_rows) {
        p += strlen(id_key);
        const char *id_end = strchr(p, '"');
        if (!id_end) break;

        size_t id_len = (size_t)(id_end - p);
        if (id_len >= sizeof(rows[count].id)) id_len = sizeof(rows[count].id) - 1;
        memcpy(rows[count].id, p, id_len);
        rows[count].id[id_len] = '\0';

        const char *obj_end = strchr(id_end, '}');
        if (!obj_end) break;
        const char *score_key = strstr(id_end, "\"cosine_similarity\":");
        float base = 0.0f;
        if (score_key && score_key < obj_end) {
            score_key += strlen("\"cosine_similarity\":");
            base = strtof(score_key, NULL);
        }
        rows[count].base = base;
        rows[count].sae_overlap = 0.0f;
        rows[count].score = base;
        count++;
        p = obj_end + 1;
    }
    return count;
}

static int load_candidate_feature_ids(const char *dir, const char *id, int *ids, int max_ids) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s.features.json", dir, id);
    struct stat st;
    if (stat(path, &st) != 0) {
        snprintf(path, sizeof(path), "%s/%s.json", dir, id);
        if (stat(path, &st) != 0) return 0;
    }
    long sz = 0;
    char *json = read_text_file(path, &sz);
    if (!json) return 0;
    int n = extract_feature_ids_json(json, ids, max_ids);
    free(json);
    return n;
}

static int cmd_sae_rerank(const char *results_json_path,
                          const char *query_features_path,
                          const char *features_dir,
                          int top_k,
                          float sae_weight) {
    long rsz = 0, qsz = 0;
    char *results_json = read_text_file(results_json_path, &rsz);
    char *query_json = read_text_file(query_features_path, &qsz);
    if (!results_json || !query_json) {
        fprintf(stderr, "rerank: cannot read input JSON files\n");
        free(results_json);
        free(query_json);
        return 1;
    }

    int query_ids[4096];
    int nq = extract_feature_ids_json(query_json, query_ids, 4096);
    if (nq <= 0) {
        fprintf(stderr, "rerank: no feature_id entries in query manifest\n");
        free(results_json);
        free(query_json);
        return 1;
    }

    RerankRow rows[2048];
    int count = parse_vec_results(results_json, rows, 2048);
    if (count <= 0) {
        fprintf(stderr, "rerank: no vec results parsed\n");
        free(results_json);
        free(query_json);
        return 1;
    }

    if (sae_weight < 0.0f) sae_weight = 0.0f;
    if (sae_weight > 1.0f) sae_weight = 1.0f;

    for (int i = 0; i < count; i++) {
        int cand_ids[4096];
        int nc = load_candidate_feature_ids(features_dir, rows[i].id, cand_ids, 4096);
        float overlap = feature_overlap_jaccard(query_ids, nq, cand_ids, nc);
        rows[i].sae_overlap = overlap;
        rows[i].score = (1.0f - sae_weight) * rows[i].base + sae_weight * overlap;
    }

    qsort(rows, (size_t)count, sizeof(RerankRow), cmp_rerank_desc);
    int show = count < top_k ? count : top_k;

    printf("{\n  \"results\": [\n");
    for (int i = 0; i < show; i++) {
        if (i > 0) printf(",\n");
        printf("    {\"id\":\"%s\",\"cosine_similarity\":%.8f,\"sae_overlap\":%.8f,\"reranked_score\":%.8f}",
               rows[i].id, rows[i].base, rows[i].sae_overlap, rows[i].score);
    }
    printf("\n  ],\n  \"mode\": \"sae-rerank\",\n  \"top_k\": %d,\n  \"sae_weight\": %.3f\n}\n",
           show, sae_weight);

    free(results_json);
    free(query_json);
    return 0;
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-vec — local vector search (sqlite-vec, pure C, SIMD cosine)\n\n"
        "Usage:\n"
        "  bonfyre-vec init <db>\n"
        "  bonfyre-vec insert <db> <embeddings.json>\n"
        "  bonfyre-vec search <db> <query.json> [--top N]\n"
        "  bonfyre-vec search <db> <query.json> --exact [--top N]\n"
        "  bonfyre-vec compare <db> <id1> <id2>\n"
        "  bonfyre-vec count <db>\n"
        "  bonfyre-vec sae-rerank <results.json> <query.features.json> <features-dir> [--top N] [--sae-weight W]\n"
        "  bonfyre-vec status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-vec\",\"status\":\"ok\",\"version\":\"3.0.0\","
               "\"backend\":\"sqlite-vec-native\",\"simd\":\""
#ifdef __ARM_NEON
               "neon"
#else
               "scalar"
#endif
               "\"}\n");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];

    if (strcmp(cmd, "init") == 0) {
        return cmd_init(argv[2]);
    } else if (strcmp(cmd, "insert") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_insert(argv[2], argv[3]);
    } else if (strcmp(cmd, "search") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        int top_k = 10, exact = 0;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc)
                top_k = atoi(argv[++i]);
            else if (strcmp(argv[i], "--exact") == 0)
                exact = 1;
        }
        return exact ? cmd_search_exact(argv[2], argv[3], top_k)
                     : cmd_search(argv[2], argv[3], top_k);
    } else if (strcmp(cmd, "compare") == 0) {
        if (argc < 5) { print_usage(); return 1; }
        return cmd_compare(argv[2], argv[3], argv[4]);
    } else if (strcmp(cmd, "count") == 0) {
        return cmd_count(argv[2]);
    } else if (strcmp(cmd, "sae-rerank") == 0) {
        if (argc < 5) { print_usage(); return 1; }
        int top_k = 10;
        float sae_weight = 0.25f;
        for (int i = 5; i < argc; i++) {
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc)
                top_k = atoi(argv[++i]);
            else if (strcmp(argv[i], "--sae-weight") == 0 && i + 1 < argc)
                sae_weight = strtof(argv[++i], NULL);
        }
        return cmd_sae_rerank(argv[2], argv[3], argv[4], top_k, sae_weight);
    }

    print_usage();
    return 1;
}
