/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreVec — local vector search via sqlite-vec.
 *
 * Single-file retrieval system. No server. No Weaviate. No Python.
 * SQLite C API + vec0 loadable extension = embedded semantic search.
 *
 * Usage:
 *   bonfyre-vec init <db> [--dims N]               → create vector table at that width (default 384)
 *   bonfyre-vec insert <db> <embeddings.json>      → bulk insert vectors
 *   bonfyre-vec search <db> <query-embedding.json> [--top N]  → nearest neighbors
 *   bonfyre-vec search <db> <query.json> --exact [--top N]    → exact SIMD cosine
 *   bonfyre-vec neighbors <db> [--top N]                      → every item's k-NN, one pass
 *   bonfyre-vec compare <db> <id1> <id2>           → pairwise cosine similarity
 *   bonfyre-vec count <db>                         → row count
 */
#include <errno.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
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
#include "bf_lex.h"
#endif

#define VEC_DIMS 384
/* The widest vector a query buffer may hold. VEC_DIMS is the width `init`
 * creates a store at; it is not the width every store has. Searching used a
 * VEC_DIMS buffer, so a 768-dim query against a 768-dim store was cut to 384
 * floats and bound against a float[768] column -- the store and the query
 * describing different vectors, reported as a search. */
#define VEC_MAX_DIMS 4096
#define VEC_FIXTURE_MAX_DIMS 4096
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

/* The width a store actually holds, from its own first vector.
 *
 * Asked of the data rather than assumed from a constant, because the store is
 * written by whichever embedder filled it: bonfyre-embed opens it at the width
 * its model returns, which for nomic-embed-text-onnx is 768. Returns 0 for an
 * empty store. */
static int store_dims(sqlite3 *db)
{
    sqlite3_stmt *st = NULL;
    int dims = 0;
    if (sqlite3_prepare_v2(db, "SELECT embedding FROM vec_artifacts LIMIT 1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    if (sqlite3_step(st) == SQLITE_ROW)
        dims = sqlite3_column_bytes(st, 0) / (int)sizeof(float);
    sqlite3_finalize(st);
    return dims;
}

/* A query and a store must be one geometry. Distances between vectors of
 * different widths are not small or large, they are undefined, and the two
 * search paths failed that differently: the ANN path bound a truncated query,
 * the exact path skipped every row whose width differed and returned an empty
 * result that read as "nothing is similar". */
static int refuse_width(int query_dims, int stored_dims, const char *query_file)
{
    if (query_dims < 0) {
        fprintf(stderr, "[vec] %s holds a vector wider than %d dims; refused rather "
                        "than truncated\n", query_file, VEC_MAX_DIMS);
        return 1;
    }
    if (stored_dims && query_dims != stored_dims) {
        fprintf(stderr, "[vec] query is %d dims and the store holds %d; they are "
                        "different geometries and cannot be compared\n",
                query_dims, stored_dims);
        return 1;
    }
    return 0;
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

/* Delegates to the one implementation. This copy dropped the backslash and
 * kept the letter, so \\t arrived as t. */
static const char *json_str_value(const char *json, const char *key,
                                   char *buf, size_t bufsz) {
    return bf_lex_json_string(json, key, buf, bufsz) ? buf : NULL;
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
    while (*p && *p != ']') {
        p = json_skip_ws(p);
        if (*p == ']') break;
        /* A vector longer than the store's width was silently truncated to
         * max_count and then inserted as though it were the whole body. That is
         * worse than refusing it: the store ends up holding a different vector
         * from the one that was offered, and every later neighbour computed
         * against it is wrong in a way nothing records. Report the real length
         * and let the caller refuse. */
        if (count >= max_count) {
            int extra = count;
            while (*p && *p != ']') {
                char *skip;
                strtof(p, &skip);
                if (skip == p) break;
                extra++;
                p = json_skip_ws(skip);
                if (*p == ',') p++;
            }
            return -extra;
        }
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

/* This fixture deliberately uses ordinary SQLite storage rather than the
 * optional sqlite-vec extension.  It exercises the production persistence
 * and exact cosine path with the native-MoE 2048 dimensional embedding that
 * BonfyreEmbed emits, including a reopen after the mutation sequence. */
static int fixture_insert(sqlite3 *db, const char *id, const float *vector,
                          int dimensions, const char *metadata) {
    sqlite3_stmt *statement = NULL;
    int ok = sqlite3_prepare_v2(db,
        "INSERT INTO vector_fixture(id,embedding,dimensions,metadata,updated_at) VALUES(?,?,?,?,datetime('now'))",
        -1, &statement, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(statement, 2, vector, (size_t)dimensions * sizeof(float), SQLITE_TRANSIENT);
        sqlite3_bind_int(statement, 3, dimensions);
        sqlite3_bind_text(statement, 4, metadata, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(statement) == SQLITE_DONE;
    }
    sqlite3_finalize(statement);
    return ok;
}

static int fixture_count(sqlite3 *db) {
    sqlite3_stmt *statement = NULL;
    int count = -1;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM vector_fixture", -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW) count = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    return count;
}

static int cmd_fixture(const char *db_path, const char *embedding_path) {
    FILE *input = fopen(embedding_path, "rb");
    char *json = NULL;
    float *native = NULL, *near = NULL, *distant = NULL;
    sqlite3 *db = NULL;
    long bytes;
    int dimensions, result = 1, inserted = 0, update_ok = 0, filtered = 0, reference_neighbor = 0;
    double norm = 0.0;

    if (!input) { fprintf(stderr, "vec fixture: cannot open native embedding %s\n", embedding_path); return 1; }
    if (fseek(input, 0, SEEK_END) || (bytes = ftell(input)) <= 0 || bytes > 16 * 1024 * 1024 || fseek(input, 0, SEEK_SET)) {
        fclose(input); fprintf(stderr, "vec fixture: invalid embedding input\n"); return 1;
    }
    json = malloc((size_t)bytes + 1);
    if (!json || fread(json, 1, (size_t)bytes, input) != (size_t)bytes) goto done;
    json[bytes] = '\0';
    fclose(input); input = NULL;
    if (!strstr(json, "\"backend\": \"native-moe-token-embedding\"") ||
        !strstr(json, "\"normalized\": true")) {
        fprintf(stderr, "vec fixture: input is not a normalized native-MoE embedding\n"); goto done;
    }
    native = calloc(VEC_FIXTURE_MAX_DIMS, sizeof(float));
    near = calloc(VEC_FIXTURE_MAX_DIMS, sizeof(float));
    distant = calloc(VEC_FIXTURE_MAX_DIMS, sizeof(float));
    if (!native || !near || !distant) goto done;
    dimensions = json_parse_float_array(json, "vector", native, VEC_FIXTURE_MAX_DIMS);
    if (dimensions <= 1 || dimensions != 2048) {
        fprintf(stderr, "vec fixture: expected 2048 native-MoE dimensions, got %d\n", dimensions); goto done;
    }
    for (int index = 0; index < dimensions; ++index) {
        if (!isfinite(native[index])) { fprintf(stderr, "vec fixture: non-finite embedding\n"); goto done; }
        norm += (double)native[index] * native[index];
        near[index] = native[index];
        distant[index] = -native[index];
    }
    if (fabs(sqrt(norm) - 1.0) > 1e-3) { fprintf(stderr, "vec fixture: embedding is not normalized\n"); goto done; }
    /* Keep a meaningful comparison neighbor, but far enough from the exact
     * native reference that round-off cannot displace the reference result. */
    near[0] += 0.10f;
    {
        double near_norm = 0.0;
        for (int index = 0; index < dimensions; ++index) near_norm += (double)near[index] * near[index];
        for (int index = 0; index < dimensions; ++index) near[index] = (float)(near[index] / sqrt(near_norm));
    }
    if (sqlite3_open(db_path, &db) != SQLITE_OK) { fprintf(stderr, "vec fixture: cannot open %s\n", db_path); goto done; }
    if (sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS vector_fixture("
        "id TEXT PRIMARY KEY,embedding BLOB NOT NULL,dimensions INTEGER NOT NULL,metadata TEXT NOT NULL,updated_at TEXT NOT NULL)",
        NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_exec(db, "DELETE FROM vector_fixture", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (!fixture_insert(db, "native-reference", native, dimensions, "{\"group\":\"keep\",\"role\":\"native\"}") ||
        !fixture_insert(db, "comparison-near", near, dimensions, "{\"group\":\"keep\",\"role\":\"comparison\"}") ||
        !fixture_insert(db, "comparison-delete", distant, dimensions, "{\"group\":\"drop\",\"role\":\"comparison\"}")) goto done;
    inserted = fixture_count(db) == 3;
    update_ok = sqlite3_exec(db,
        "UPDATE vector_fixture SET metadata='{\"group\":\"keep\",\"role\":\"comparison-updated\"}',updated_at=datetime('now') WHERE id='comparison-near'",
        NULL, NULL, NULL) == SQLITE_OK;
    sqlite3_stmt *scan = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT id,embedding,dimensions FROM vector_fixture WHERE metadata LIKE '%\"group\":\"keep\"%'", -1, &scan, NULL) != SQLITE_OK) goto done;
    float best_score = -2.0f;
    char best_id[256] = "";
    while (sqlite3_step(scan) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(scan, 0);
        const void *blob = sqlite3_column_blob(scan, 1);
        int blob_dimensions = sqlite3_column_int(scan, 2);
        if (!blob || blob_dimensions != dimensions) continue;
        filtered++;
        float score = cosine_similarity(native, (const float *)blob, dimensions);
        if (score > best_score) { best_score = score; snprintf(best_id, sizeof(best_id), "%s", id ? id : ""); }
    }
    sqlite3_finalize(scan); scan = NULL;
    reference_neighbor = filtered == 2 && !strcmp(best_id, "native-reference") && best_score > 0.99999f;
    if (sqlite3_exec(db, "DELETE FROM vector_fixture WHERE id='comparison-delete'", NULL, NULL, NULL) != SQLITE_OK) goto done;
    int deleted = fixture_count(db) == 2;
    sqlite3_close(db); db = NULL; /* explicit persistent-store reopen */
    if (sqlite3_open(db_path, &db) != SQLITE_OK) goto done;
    int restart_persisted = fixture_count(db) == 2;
    printf("{\"backend\":\"native-moe-token-embedding\",\"model_identity\":\"qwen1.5-moe-a2.7b-chat\","
           "\"dimensions\":%d,\"distance_metric\":\"cosine\",\"inserted\":%s,\"updated\":%s,"
           "\"nearest_neighbor\":\"%s\",\"metadata_filtered\":%s,\"deleted\":%s,"
           "\"restart_persisted\":%s,\"reference_neighbor\":%s}\n",
           dimensions, inserted ? "true" : "false", update_ok ? "true" : "false", best_id,
           filtered == 2 ? "true" : "false", deleted ? "true" : "false",
           restart_persisted ? "true" : "false", reference_neighbor ? "true" : "false");
    result = inserted && update_ok && deleted && restart_persisted && reference_neighbor ? 0 : 1;
done:
    if (input) fclose(input);
    if (db) sqlite3_close(db);
    free(json); free(native); free(near); free(distant);
    return result;
}

/* ── commands ───────────────────────────────────────────────── */

/* The width a store was made at, read from the store itself.  The vector
 * table's own declaration says it (float[N]); an older store with rows but an
 * unreadable declaration is measured from a row.  Nothing here assumes 384. */
static int declared_width(sqlite3 *db)
{
    sqlite3_stmt *st = NULL;
    int width = 0;
    if (sqlite3_prepare_v2(db, "SELECT sql FROM sqlite_master WHERE name='vec_artifacts'", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) {
        const char *sql = (const char *)sqlite3_column_text(st, 0);
        const char *at = sql ? strstr(sql, "float[") : NULL;
        if (at) width = atoi(at + 6);
    }
    sqlite3_finalize(st);
    if (width < 1 || width > VEC_MAX_DIMS) width = store_dims(db);
    return width >= 1 && width <= VEC_MAX_DIMS ? width : VEC_DIMS;
}

static int cmd_init(const char *db_path, int width) {
    if (width < 1 || width > VEC_MAX_DIMS) {
        fprintf(stderr, "[vec] a store is between 1 and %d dimensions wide; %d refused\n", VEC_MAX_DIMS, width);
        return 1;
    }
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
        ")", width);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[vec] Failed to create vec_artifacts table: %s\n", err);
        sqlite3_free(err);
        sqlite3_close(db);
        return 1;
    }
    if (declared_width(db) != width) {
        fprintf(stderr, "[vec] this store already exists at %d dimensions; it was not changed to %d\n",
                declared_width(db), width);
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

    /* What this store holds is asked of the store. */
    const int want = declared_width(db);
    static float embedding[VEC_MAX_DIMS];

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
    int count = 0, skipped = 0;
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
            json_str_value(obj, "id", id, sizeof(id));
            json_str_value(obj, "source", source, sizeof(source));
            json_str_value(obj, "type", type, sizeof(type));
            json_str_value(obj, "text", text, sizeof(text));
            int dims = json_parse_float_array(obj, "embedding", embedding, VEC_MAX_DIMS);

            if (id[0] == '\0') snprintf(id, sizeof(id), "%d", count);

            /* The width is checked before anything is written.
             *
             * This wrote the metadata row first and then stepped the vector
             * insert without looking at the result. A body of the wrong width is
             * rejected by the vector table, so the store was left holding an
             * artifact with no vector -- and the command still counted it and
             * reported success. `artifacts: 1, vectors: 0` was reachable from a
             * run that said `inserted: 1`.
             *
             * Refusing first means a rejected body leaves nothing behind. */
            if (dims != 0 && dims != want) {
                fprintf(stderr, "[vec] refusing %s: %d dimensions, store holds %d\n",
                        id, dims < 0 ? -dims : dims, want);
                skipped++;
            } else if (dims == want) {
                sqlite3_reset(meta_stmt);
                sqlite3_bind_text(meta_stmt, 1, id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 2, source, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 3, type, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 4, text, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(meta_stmt, 5, "{}", -1, SQLITE_STATIC);
                int meta_ok = sqlite3_step(meta_stmt) == SQLITE_DONE;

                sqlite3_reset(vec_stmt);
                sqlite3_bind_text(vec_stmt, 1, id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_blob(vec_stmt, 2, embedding,
                                  dims * (int)sizeof(float), SQLITE_TRANSIENT);
                int vec_ok = sqlite3_step(vec_stmt) == SQLITE_DONE;
                if (meta_ok && vec_ok) {
                    count++;
                } else {
                    fprintf(stderr, "[vec] %s not stored: %s\n", id, sqlite3_errmsg(db));
                    skipped++;
                }
            }

            free(obj);
            p = obj_end;
        }
    } else {
        /* BonfyreEmbed emits one direct {"vector": [...]} JSON document.
         * Accept it as a single artifact so Embed → Vec does not require a
         * lossy or hand-built wrapper. */
        char id[256] = "", source[512] = "", type[128] = "embedding", text[4096] = "";
        json_str_value(json, "id", id, sizeof(id));
        json_str_value(json, "source", source, sizeof(source));
        json_str_value(json, "type", type, sizeof(type));
        json_str_value(json, "text", text, sizeof(text));
        int dims = json_parse_float_array(json, "embedding", embedding, VEC_MAX_DIMS);
        if (dims == 0) dims = json_parse_float_array(json, "vector", embedding, VEC_MAX_DIMS);
        if (id[0] == '\0') {
            const char *base = strrchr(json_path, '/');
            snprintf(id, sizeof(id), "%s", base ? base + 1 : json_path);
        }
        if (dims != 0 && dims != want) {
            fprintf(stderr, "[vec] refusing %s: %d dimensions, store holds %d\n",
                    id, dims < 0 ? -dims : dims, want);
            skipped++;
        } else if (dims == want) {
            sqlite3_bind_text(meta_stmt, 1, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(meta_stmt, 2, source, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(meta_stmt, 3, type, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(meta_stmt, 4, text, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(meta_stmt, 5, "{}", -1, SQLITE_STATIC);
            int meta_ok = sqlite3_step(meta_stmt) == SQLITE_DONE;
            sqlite3_bind_text(vec_stmt, 1, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_blob(vec_stmt, 2, embedding, dims * (int)sizeof(float), SQLITE_TRANSIENT);
            int vec_ok = sqlite3_step(vec_stmt) == SQLITE_DONE;
            if (meta_ok && vec_ok) {
                count = 1;
            } else {
                fprintf(stderr, "[vec] %s not stored: %s\n", id, sqlite3_errmsg(db));
                skipped++;
            }
        }
    }

    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_finalize(meta_stmt);
    sqlite3_finalize(vec_stmt);
    sqlite3_close(db);
    free(json);

    /* A refusal is reported, and an import that stored nothing while being
     * offered something is a failure. Exiting 0 after storing nothing is how a
     * caller ends up believing an empty store is populated. */
    if (skipped) printf("%d vectors inserted, %d refused\n", count, skipped);
    else printf("%d vectors inserted\n", count);
    return (count == 0 && skipped > 0) ? 1 : 0;
}

/* Remove exact passage IDs from both halves of the index in one transaction.
 * No source file, embedding cache, backup or other database is erased. */
static int cmd_delete(const char *db_path, int id_count, char **ids) {
    for (int i = 0; i < id_count; i++) {
        if (!ids[i][0]) {
            fprintf(stderr, "[vec] delete requires nonempty exact IDs\n");
            return 1;
        }
    }
    sqlite3 *db = NULL;
    sqlite3_stmt *vectors = NULL, *artifacts = NULL;
    int transaction = 0, result = 1;
    sqlite3_int64 removed_vectors = 0, removed_artifacts = 0;
    /* A typo must not create an empty database and appear to remove data. */
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK)
        goto failed;
    sqlite3_busy_timeout(db, 5000);
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) goto done;
    if (sqlite3_prepare_v2(db, "DELETE FROM vec_artifacts WHERE id = ?",
                           -1, &vectors, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "DELETE FROM artifacts WHERE id = ?",
                           -1, &artifacts, NULL) != SQLITE_OK) goto failed;
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK)
        goto failed;
    transaction = 1;
    for (int i = 0; i < id_count; i++) {
        if (sqlite3_reset(vectors) != SQLITE_OK ||
            sqlite3_bind_text(vectors, 1, ids[i], -1, SQLITE_TRANSIENT) != SQLITE_OK ||
            sqlite3_step(vectors) != SQLITE_DONE) goto failed;
        removed_vectors += sqlite3_changes(db);
        if (sqlite3_reset(artifacts) != SQLITE_OK ||
            sqlite3_bind_text(artifacts, 1, ids[i], -1, SQLITE_TRANSIENT) != SQLITE_OK ||
            sqlite3_step(artifacts) != SQLITE_DONE) goto failed;
        removed_artifacts += sqlite3_changes(db);
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto failed;
    transaction = 0;
    printf("{\"requested_ids\":%d,\"deleted_vectors\":%lld,\"deleted_artifacts\":%lld}\n",
           id_count, (long long)removed_vectors, (long long)removed_artifacts);
    result = 0;
    goto done;
failed:
    fprintf(stderr, "[vec] delete failed: %s\n", db ? sqlite3_errmsg(db) : "cannot open database");
done:
    sqlite3_finalize(vectors);
    sqlite3_finalize(artifacts);
    if (transaction) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_close(db);
    return result;
}

static int cmd_search(const char *db_path, const char *query_file, int top_k) {
    fprintf(stderr, "[vec] Searching %s (top %d)\n", db_path, top_k);

    static float query_vec[VEC_MAX_DIMS];
    int dims = 0;

    /* Try VECF binary first, then JSON */
    if (is_vecf_file(query_file)) {
        dims = read_vector_binary(query_file, query_vec, VEC_MAX_DIMS);
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
        dims = json_parse_float_array(json, "embedding", query_vec, VEC_MAX_DIMS);
        if (dims == 0) dims = json_parse_float_array(json, "vector", query_vec, VEC_MAX_DIMS);
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
    if (refuse_width(dims, store_dims(db), query_file)) { sqlite3_close(db); return 1; }

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
        /* Escaped. Result text is whatever was embedded -- here, headlines
         * with quotes in them -- and printing it raw produced output that was
         * not JSON the moment a title contained one. */
        char eid[512], esrc[1024], etyp[256], etxt[4096];
        bf_lex_json_escape(rid ? rid : "", eid, sizeof(eid));
        bf_lex_json_escape(src ? src : "", esrc, sizeof(esrc));
        bf_lex_json_escape(typ ? typ : "", etyp, sizeof(etyp));
        bf_lex_json_escape(txt ? txt : "", etxt, sizeof(etxt));
        printf("    {\"id\":\"%s\",\"distance\":%.6f,\"source\":\"%s\","
               "\"type\":\"%s\",\"text\":\"%s\"}",
               eid, dist, esrc, etyp, etxt);
    }
    printf("\n  ],\n  \"query_file\": \"%s\",\n  \"top_k\": %d\n}\n", query_file, top_k);

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

/* Every stored vector's nearest neighbours, in one process.
 *
 * Corroboration across a corpus is "which items are near which other items",
 * and `search` answers that one query file at a time. Asking it for 22,962
 * items meant 22,962 invocations, each reopening the store and reloading the
 * extension to run a single k-NN. This walks the store once and runs every
 * k-NN on one connection.
 *
 * Similarity is reported as cosine, computed here from both vectors, rather
 * than the store's native distance: vec0 ranks by L2, and an L2 figure is only
 * a cosine in disguise when every vector is unit length, which is a property
 * of the embedder and not something this command can see. The L2 ranking
 * chooses the candidates; cosine is what is reported about them.
 *
 * Output is one JSON line per (item, neighbour) pair. No threshold is applied:
 * what counts as "the same event" is a calibration nobody has done, and baking
 * one in would present a guess as a property of the data. */
static int cmd_neighbors(const char *db_path, int top_k)
{
    sqlite3 *db = NULL;
    sqlite3_stmt *ids = NULL, *knn = NULL;
    char **idv = NULL;
    int n = 0, cap = 0, dims, i, pairs = 0, skipped = 0;
    static float q[VEC_MAX_DIMS], v[VEC_MAX_DIMS];

    if (top_k < 1) top_k = 1;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "[vec] Cannot open DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_enable_load_extension(db, 1);
    if (load_vec_ext(db) != 0) { sqlite3_close(db); return 1; }
    dims = store_dims(db);
    if (dims <= 0) {
        fprintf(stderr, "{\"schema\":\"bonfyre-vec-neighbors.v1\",\"items\":0,\"pairs\":0}\n");
        sqlite3_close(db);
        return 0;
    }

    /* Ids first, then queries: stepping a k-NN cursor on vec_artifacts while
     * another cursor is still walking the same virtual table is the kind of
     * interleaving that works until it does not. */
    if (sqlite3_prepare_v2(db, "SELECT id FROM vec_artifacts", -1, &ids, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    while (sqlite3_step(ids) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(ids, 0);
        if (!id) continue;
        if (n == cap) {
            char **grown;
            cap = cap ? cap * 2 : 1024;
            grown = realloc(idv, sizeof(char *) * (size_t)cap);
            if (!grown) break;
            idv = grown;
        }
        idv[n++] = strdup(id);
    }
    sqlite3_finalize(ids);

    if (sqlite3_prepare_v2(db,
            "SELECT id FROM vec_artifacts WHERE embedding MATCH ? AND k = ? ORDER BY distance",
            -1, &knn, NULL) != SQLITE_OK) {
        fprintf(stderr, "[vec] k-NN prepare failed: %s\n", sqlite3_errmsg(db));
        for (i = 0; i < n; i++) free(idv[i]);
        free(idv);
        sqlite3_close(db);
        return 1;
    }

    for (i = 0; i < n; i++) {
        char eid[512];
        if (read_vec_from_db(db, idv[i], q, VEC_MAX_DIMS) != dims) { skipped++; continue; }
        bf_lex_json_escape(idv[i], eid, sizeof(eid));
        sqlite3_reset(knn);
        sqlite3_bind_blob(knn, 1, q, dims * (int)sizeof(float), SQLITE_TRANSIENT);
        sqlite3_bind_int(knn, 2, top_k + 1);      /* +1: the item finds itself */
        while (sqlite3_step(knn) == SQLITE_ROW) {
            const char *nid = (const char *)sqlite3_column_text(knn, 0);
            char enid[512];
            if (!nid || strcmp(nid, idv[i]) == 0) continue;
            if (read_vec_from_db(db, nid, v, VEC_MAX_DIMS) != dims) continue;
            bf_lex_json_escape(nid, enid, sizeof(enid));
            printf("{\"id\":\"%s\",\"neighbor\":\"%s\",\"cosine\":%.6f}\n",
                   eid, enid, cosine_similarity(q, v, dims));
            pairs++;
        }
    }
    sqlite3_finalize(knn);
    for (i = 0; i < n; i++) free(idv[i]);
    free(idv);
    sqlite3_close(db);
    fprintf(stderr, "{\"schema\":\"bonfyre-vec-neighbors.v1\",\"items\":%d,\"dims\":%d,"
                    "\"top_k\":%d,\"pairs\":%d,\"skipped_width_mismatch\":%d,"
                    "\"cannot_infer\":[\"a near neighbour -> the same event; no similarity "
                    "threshold has been calibrated against labelled pairs\"]}\n",
            n, dims, top_k, pairs, skipped);
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

    static float v1[VEC_MAX_DIMS], v2[VEC_MAX_DIMS];
    int d1 = read_vec_from_db(db, id1, v1, VEC_MAX_DIMS);
    int d2 = read_vec_from_db(db, id2, v2, VEC_MAX_DIMS);
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

    static float query_vec[VEC_MAX_DIMS];
    int dims = 0;

    if (is_vecf_file(query_file)) {
        dims = read_vector_binary(query_file, query_vec, VEC_MAX_DIMS);
        if (dims > 0) fprintf(stderr, "[vec] Read %d dims from VECF binary\n", dims);
    }
    if (dims == 0) {
        FILE *f = fopen(query_file, "rb");
        if (!f) { fprintf(stderr, "[vec] Cannot open %s\n", query_file); return 1; }
        fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
        char *json = malloc((size_t)sz + 1);
        if (!json) { fclose(f); return 1; }
        fread(json, 1, (size_t)sz, f); json[sz] = '\0'; fclose(f);
        dims = json_parse_float_array(json, "embedding", query_vec, VEC_MAX_DIMS);
        if (dims == 0) dims = json_parse_float_array(json, "vector", query_vec, VEC_MAX_DIMS);
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
    if (refuse_width(dims, store_dims(db), query_file)) { sqlite3_close(db); return 1; }

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
        char eid[512], esrc[1024], etyp[256];
        bf_lex_json_escape(results[i].id, eid, sizeof(eid));
        bf_lex_json_escape(results[i].source, esrc, sizeof(esrc));
        bf_lex_json_escape(results[i].type, etyp, sizeof(etyp));
        printf("    {\"id\":\"%s\",\"cosine_similarity\":%.8f,"
               "\"distance\":%.8f,\"source\":\"%s\",\"type\":\"%s\"}",
               eid, results[i].score, 1.0f - results[i].score, esrc, etyp);
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
        "  bonfyre-vec init <db> [--dims N]     (default 384; match your embedding model)\n"
        "  bonfyre-vec insert <db> <embeddings.json>\n"
        "  bonfyre-vec delete <db> <exact-id> [exact-id ...]\n"
        "  bonfyre-vec search <db> <query.json> [--top N]\n"
        "  bonfyre-vec search <db> <query.json> --exact [--top N]\n"
        "  bonfyre-vec compare <db> <id1> <id2>\n"
        "  bonfyre-vec count <db>\n"
        "  bonfyre-vec fixture <db> <native-moe-embedding.json>\n"
        "  bonfyre-vec sae-rerank <results.json> <query.features.json> <features-dir> [--top N] [--sae-weight W]\n"
        "  bonfyre-vec status\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { print_usage(); return 0; }
    enum { VC_STATUS };
    static const BfCommandEntry vec_commands[] = {
        {"status", VC_STATUS, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(vec_commands,
        sizeof(vec_commands) / sizeof(vec_commands[0]), argv[1]);
    if (command && command->id == VC_STATUS) {
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
        int width = VEC_DIMS;
        for (int i = 3; i + 1 < argc; i++) if (strcmp(argv[i], "--dims") == 0) width = atoi(argv[i + 1]);
        return cmd_init(argv[2], width);
    } else if (strcmp(cmd, "insert") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_insert(argv[2], argv[3]);
    } else if (strcmp(cmd, "delete") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_delete(argv[2], argc - 3, argv + 3);
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
    } else if (strcmp(cmd, "neighbors") == 0) {
        int k = 5;
        if (argc < 3) { fprintf(stderr, "usage: bonfyre-vec neighbors <db> [--top N]\n"); return 1; }
        for (int i = 3; i < argc; i++)
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc) k = atoi(argv[++i]);
        return cmd_neighbors(argv[2], k);
    } else if (strcmp(cmd, "count") == 0) {
        return cmd_count(argv[2]);
    } else if (strcmp(cmd, "fixture") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_fixture(argv[2], argv[3]);
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
