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
 *   bonfyre-vec count <db>                         → row count
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>

#define VEC_DIMS 384

/* ── sqlite-vec extension resolution ─────────────────────────── */

static const char *resolve_vec_ext(void) {
    const char *env = getenv("BONFYRE_VEC_EXT");
    if (env && env[0]) return env;
    static const char *paths[] = {
        "/Users/nickgonzales/Library/Python/3.9/lib/python/site-packages/sqlite_vec/vec0",
        "/opt/homebrew/lib/sqlite_vec/vec0",
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
    return "vec0";
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

static int cmd_search(const char *db_path, const char *query_json, int top_k) {
    fprintf(stderr, "[vec] Searching %s (top %d)\n", db_path, top_k);

    FILE *f = fopen(query_json, "rb");
    if (!f) { fprintf(stderr, "[vec] Cannot open %s\n", query_json); return 1; }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *json = malloc((size_t)sz + 1);
    if (!json) { fclose(f); return 1; }
    fread(json, 1, (size_t)sz, f);
    json[sz] = '\0';
    fclose(f);

    float query_vec[VEC_DIMS];
    int dims = json_parse_float_array(json, "embedding", query_vec, VEC_DIMS);
    if (dims == 0) dims = json_parse_float_array(json, "vector", query_vec, VEC_DIMS);
    free(json);

    if (dims == 0) {
        fprintf(stderr, "[vec] No embedding found in %s\n", query_json);
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
    printf("\n  ],\n  \"query_file\": \"%s\",\n  \"top_k\": %d\n}\n", query_json, top_k);

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

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-vec — local vector search (sqlite-vec, pure C)\n\n"
        "Usage:\n"
        "  bonfyre-vec init <db>\n"
        "  bonfyre-vec insert <db> <embeddings.json>\n"
        "  bonfyre-vec search <db> <query.json> [--top N]\n"
        "  bonfyre-vec count <db>\n"
        "  bonfyre-vec status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-vec\",\"status\":\"ok\",\"version\":\"2.0.0\","
               "\"backend\":\"sqlite-vec-native\"}\n");
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
        int top_k = 10;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc)
                top_k = atoi(argv[++i]);
        }
        return cmd_search(argv[2], argv[3], top_k);
    } else if (strcmp(cmd, "count") == 0) {
        return cmd_count(argv[2]);
    }

    print_usage();
    return 1;
}
