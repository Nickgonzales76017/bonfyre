/*
 * BonfyreRecipe — Recipe Registry Management
 *
 * SQLite-backed registry for pipeline recipes with FTS5 search,
 * SHA-256 content addressing, and schema validation.
 *
 * Usage:
 *   bonfyre-recipe init [--db PATH]
 *   bonfyre-recipe list [--category CAT] [--tag TAG]
 *   bonfyre-recipe show <RECIPE_ID>
 *   bonfyre-recipe add <RECIPE.json>
 *   bonfyre-recipe validate <RECIPE.json>
 *   bonfyre-recipe search <QUERY>
 *   bonfyre-recipe hash <RECIPE.json>
 */

#include <errno.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define MAX_PATH 4096
#define MAX_JSON 1048576  /* 1 MB max recipe size */

/* ═══════════════════════════════════════════════════════════════════
 * Utilities
 * ═══════════════════════════════════════════════════════════════════ */

static char *get_db_path(const char *custom_path) {
    static char path[MAX_PATH];
    
    if (custom_path) {
        snprintf(path, sizeof(path), "%s", custom_path);
        return path;
    }
    
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    
    snprintf(path, sizeof(path), "%s/.bonfyre/recipes.db", home);
    return path;
}

static char *get_catalog_db_path(const char *custom_path) {
    static char path[MAX_PATH];
    if (custom_path) {
        snprintf(path, sizeof(path), "%s", custom_path);
        return path;
    }
    bf_catalog_default_db_path(path, sizeof(path));
    return path;
}

static void sync_catalog(const char *catalog_db_path) {
    bf_catalog_sync_default(catalog_db_path);
}

static int ensure_dir(const char *path) {
    return bf_ensure_dir(path) == 0 ? 0 : -1;
}

static char *read_file(const char *path, size_t *out_size) {
    char *buf = bf_read_file(path, out_size);
    if (buf && out_size && *out_size > MAX_JSON) {
        free(buf);
        return NULL;
    }
    return buf;
}

static int looks_like_json_text(const char *text) {
    if (!text) return 0;
    while (*text && isspace((unsigned char)*text)) text++;
    return *text == '{' || *text == '[';
}

static void compute_sha256(const char *data, size_t len, char *out_hex) {
    bf_sha256_hex((const uint8_t *)data, len, out_hex);
}

static const char *json_extract_string(const char *json, const char *key) {
    static char value[1024];
    if (!bf_json_str(json, key, value, sizeof(value))) return NULL;
    return value;
}

static int json_has_key(const char *json, const char *key) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    return json && strstr(json, needle) != NULL;
}

static int json_extract_string_array_preview(const char *json, const char *key,
                                             char *out, size_t out_sz) {
    char pattern[128];
    const char *p;
    size_t used = 0;
    int count = 0;

    if (!json || !key || !out || out_sz == 0) return 0;
    out[0] = '\0';
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    p = strstr(json, pattern);
    if (!p) return 0;
    p = strchr(p, '[');
    if (!p) return 0;
    p++;

    while (*p && *p != ']') {
        while (*p && *p != '"' && *p != ']') p++;
        if (*p != '"') break;
        p++;
        {
            const char *start = p;
            size_t len = 0;
            while (*p && *p != '"') {
                if (*p == '\\' && p[1]) p++;
                p++;
                len++;
            }
            if (*p != '"') break;
            if (count > 0 && used + 2 < out_sz) {
                out[used++] = ',';
                out[used++] = ' ';
            }
            if (used + len >= out_sz) len = out_sz - used - 1;
            memcpy(out + used, start, len);
            used += len;
            out[used] = '\0';
            count++;
            p++;
        }
    }

    return count;
}

/* ═══════════════════════════════════════════════════════════════════
 * Database Schema
 * ═══════════════════════════════════════════════════════════════════ */

static const char *SCHEMA_SQL = 
    "CREATE TABLE IF NOT EXISTS recipes ("
    "  recipe_id TEXT PRIMARY KEY,"
    "  name TEXT NOT NULL,"
    "  version TEXT NOT NULL,"
    "  hash TEXT UNIQUE NOT NULL,"
    "  json_data TEXT NOT NULL,"
    "  created_at INTEGER NOT NULL,"
    "  category TEXT,"
    "  UNIQUE(recipe_id, version)"
    ");"
    
    "CREATE TABLE IF NOT EXISTS recipe_tags ("
    "  recipe_id TEXT,"
    "  tag TEXT,"
    "  FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),"
    "  PRIMARY KEY(recipe_id, tag)"
    ");"
    
    "CREATE TABLE IF NOT EXISTS models ("
    "  model_name TEXT PRIMARY KEY,"
    "  hash TEXT NOT NULL,"
    "  size_mb INTEGER,"
    "  source_url TEXT"
    ");"
    
    "CREATE TABLE IF NOT EXISTS recipe_models ("
    "  recipe_id TEXT,"
    "  model_name TEXT,"
    "  FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),"
    "  FOREIGN KEY(model_name) REFERENCES models(model_name),"
    "  PRIMARY KEY(recipe_id, model_name)"
    ");"
    
    "CREATE VIRTUAL TABLE IF NOT EXISTS recipe_fts USING fts5("
    "  recipe_id UNINDEXED, name, description, tags"
    ");";

/* ═══════════════════════════════════════════════════════════════════
 * Command: init
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_init(const char *db_path) {
    printf("Initializing recipe registry at: %s\n", db_path);
    
    /* Ensure directory exists */
    char dir[MAX_PATH];
    snprintf(dir, sizeof(dir), "%s", db_path);
    char *last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        if (ensure_dir(dir) != 0) {
            fprintf(stderr, "Failed to create directory: %s\n", dir);
            return 1;
        }
    }
    
    /* Open database */
    sqlite3 *db;
    int rc = bf_sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    /* Create schema */
    char *err_msg = NULL;
    rc = sqlite3_exec(db, SCHEMA_SQL, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }
    
    printf("✓ Schema created\n");
    printf("✓ Registry ready\n");
    printf("\nNext steps:\n");
    printf("  bonfyre-recipe add <recipe.json>\n");
    printf("  bonfyre-recipe list\n");
    
    sqlite3_close(db);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Command: list
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_list(const char *db_path, const char *category, const char *tag) {
    sqlite3 *db;
    int rc = bf_sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    const char *sql = 
        "SELECT r.recipe_id, r.name, r.category, "
        "  GROUP_CONCAT(t.tag, ',') as tags "
        "FROM recipes r "
        "LEFT JOIN recipe_tags t ON r.recipe_id = t.recipe_id "
        "GROUP BY r.recipe_id "
        "ORDER BY r.recipe_id";
    
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    printf("%-8s %-30s %-20s %-30s\n", "ID", "Name", "Category", "Tags");
    printf("────────────────────────────────────────────────────────────────────────────────\n");
    
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(stmt, 0);
        const char *name = (const char *)sqlite3_column_text(stmt, 1);
        const char *cat = (const char *)sqlite3_column_text(stmt, 2);
        const char *tags_str = (const char *)sqlite3_column_text(stmt, 3);
        
        /* Filter by category/tag if specified */
        if (category && (!cat || strcmp(cat, category) != 0)) continue;
        if (tag && (!tags_str || !strstr(tags_str, tag))) continue;
        
        printf("%-8s %-30s %-20s %-30s\n", 
               id ? id : "",
               name ? name : "",
               cat ? cat : "",
               tags_str ? tags_str : "");
    }
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

static int cmd_list_catalog(const char *db_path, const char *category, const char *tag) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;
    int shown = 0;

    (void)bf_catalog_sync_default(db_path);
    rc = bf_sqlite3_open_ro(db_path, &db);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_prepare_v2(db,
        "SELECT external_id, name, category, json_data "
        "FROM catalog_nodes WHERE kind='recipe' ORDER BY external_id",
        -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    printf("%-8s %-30s %-20s %-30s\n", "ID", "Name", "Category", "Tags");
    printf("────────────────────────────────────────────────────────────────────────────────\n");

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(stmt, 0);
        const char *name = (const char *)sqlite3_column_text(stmt, 1);
        const char *cat = (const char *)sqlite3_column_text(stmt, 2);
        const char *json = (const char *)sqlite3_column_text(stmt, 3);
        char tags[256];

        tags[0] = '\0';
        if (json) json_extract_string_array_preview(json, "tags", tags, sizeof(tags));

        if (category && (!cat || strcmp(cat, category) != 0)) continue;
        if (tag && (!tags[0] || !strstr(tags, tag))) continue;

        printf("%-8s %-30s %-20s %-30s\n",
               id ? id : "",
               name ? name : "",
               cat ? cat : "",
               tags);
        shown++;
    }

    if (shown == 0) printf("(no catalog-backed recipes matched)\n");
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Command: show
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_show(const char *db_path, const char *recipe_id) {
    sqlite3 *db;
    int rc = bf_sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    const char *sql = "SELECT json_data FROM recipes WHERE recipe_id = ?";
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    
    sqlite3_bind_text(stmt, 1, recipe_id, -1, SQLITE_STATIC);
    
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        printf("%s\n", json);
    } else {
        fprintf(stderr, "Recipe not found: %s\n", recipe_id);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 1;
    }
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

static int cmd_show_catalog(const char *db_path, const char *recipe_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;

    (void)bf_catalog_sync_default(db_path);
    rc = bf_sqlite3_open_ro(db_path, &db);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_prepare_v2(db,
        "SELECT json_data, source_path FROM catalog_nodes WHERE kind='recipe' AND external_id = ?",
        -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    sqlite3_bind_text(stmt, 1, recipe_id, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        const char *source_path = (const char *)sqlite3_column_text(stmt, 1);
        if (json && json[0] && !looks_like_json_text(json)) {
            printf("%s\n", json);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return 0;
        }
        if ((!json || !json[0]) && source_path && source_path[0]) {
            size_t raw_size = 0;
            char *raw = read_file(source_path, &raw_size);
            if (raw) {
                printf("%s\n", raw);
                free(raw);
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return 0;
            }
        }
        sqlite3_finalize(stmt);

        printf("{\n  \"recipe\": %s,\n", json ? json : "{}");

        rc = sqlite3_prepare_v2(db,
            "SELECT n.external_id FROM catalog_edges e "
            "JOIN catalog_nodes n ON n.node_id = e.dst_node_id "
            "WHERE e.src_node_id=? AND e.rel='uses_model' AND n.kind='model' "
            "ORDER BY n.external_id",
            -1, &stmt, NULL);
        if (rc == SQLITE_OK) {
            char recipe_node_id[160];
            int first = 1;
            snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_id);
            sqlite3_bind_text(stmt, 1, recipe_node_id, -1, SQLITE_STATIC);
            printf("  \"related_models\": [");
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                if (!first) printf(", ");
                first = 0;
                printf("\"%s\"", (const char *)sqlite3_column_text(stmt, 0));
            }
            printf("],\n");
            sqlite3_finalize(stmt);
        } else {
            printf("  \"related_models\": [],\n");
        }

        rc = sqlite3_prepare_v2(db,
            "SELECT n.external_id FROM catalog_edges e "
            "JOIN catalog_nodes n ON n.node_id = e.dst_node_id "
            "WHERE e.src_node_id=? AND e.rel='has_run_manifest' AND n.kind='run_manifest' "
            "ORDER BY n.external_id DESC",
            -1, &stmt, NULL);
        if (rc == SQLITE_OK) {
            char recipe_node_id[160];
            int first = 1;
            snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_id);
            sqlite3_bind_text(stmt, 1, recipe_node_id, -1, SQLITE_STATIC);
            printf("  \"run_manifests\": [");
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                if (!first) printf(", ");
                first = 0;
                printf("\"%s\"", (const char *)sqlite3_column_text(stmt, 0));
            }
            printf("],\n");
            sqlite3_finalize(stmt);
        } else {
            printf("  \"run_manifests\": [],\n");
        }

        rc = sqlite3_prepare_v2(db,
            "SELECT n.external_id FROM catalog_edges e "
            "JOIN catalog_nodes n ON n.node_id = e.dst_node_id "
            "WHERE e.src_node_id=? AND e.rel='implements_workflow' AND n.kind='workflow' "
            "ORDER BY n.external_id",
            -1, &stmt, NULL);
        if (rc == SQLITE_OK) {
            char recipe_node_id[160];
            int first = 1;
            snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_id);
            sqlite3_bind_text(stmt, 1, recipe_node_id, -1, SQLITE_STATIC);
            printf("  \"workflow_profiles\": [");
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                if (!first) printf(", ");
                first = 0;
                printf("\"%s\"", (const char *)sqlite3_column_text(stmt, 0));
            }
            printf("]\n");
            sqlite3_finalize(stmt);
        } else {
            printf("  \"workflow_profiles\": []\n");
        }
        printf("}\n");
        sqlite3_close(db);
        return 0;
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 1;
}

static int cmd_search(const char *db_path, const char *query) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc = bf_sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    if (sqlite3_prepare_v2(db,
        "SELECT r.recipe_id, r.name, r.category FROM recipe_fts f "
        "JOIN recipes r ON r.recipe_id = f.recipe_id "
        "WHERE recipe_fts MATCH ? ORDER BY r.recipe_id",
        -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    sqlite3_bind_text(stmt, 1, query, -1, SQLITE_STATIC);
    printf("%-8s %-30s %-20s\n", "ID", "Name", "Category");
    printf("────────────────────────────────────────────────────────────\n");
    int shown = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        printf("%-8s %-30s %-20s\n",
               sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "",
               sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "",
               sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2) : "");
        shown++;
    }
    if (shown == 0) printf("(no registry-backed recipes matched)\n");
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

static int cmd_search_catalog(const char *db_path, const char *query) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;
    int shown = 0;

    (void)bf_catalog_sync_default(db_path);
    rc = bf_sqlite3_open_ro(db_path, &db);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_prepare_v2(db,
        "SELECT n.external_id, n.name, n.category "
        "FROM catalog_fts f JOIN catalog_nodes n ON n.rowid = f.rowid "
        "WHERE f.catalog_fts MATCH ? AND n.kind='recipe' "
        "ORDER BY n.external_id",
        -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(stmt, 1, query, -1, SQLITE_STATIC);

    printf("%-8s %-30s %-20s\n", "ID", "Name", "Category");
    printf("────────────────────────────────────────────────────────────\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        printf("%-8s %-30s %-20s\n",
               sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "",
               sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "",
               sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2) : "");
        shown++;
    }
    if (shown == 0) printf("(no catalog-backed recipes matched)\n");
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Command: add
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_add(const char *db_path, const char *catalog_db_path, const char *recipe_path) {
    /* Read recipe file */
    size_t json_size;
    char *json = read_file(recipe_path, &json_size);
    if (!json) {
        fprintf(stderr, "Cannot read file: %s\n", recipe_path);
        return 1;
    }
    
    /* Extract fields */
    const char *tmp;
    char *recipe_id = NULL, *name = NULL, *version = NULL, *category = NULL;
    
    tmp = json_extract_string(json, "recipe_id");
    if (!tmp) tmp = json_extract_string(json, "code");
    if (!tmp) tmp = json_extract_string(json, "recipe");
    if (tmp) recipe_id = strdup(tmp);
    
    tmp = json_extract_string(json, "name");
    if (tmp) name = strdup(tmp);
    
    tmp = json_extract_string(json, "version");
    if (tmp) version = strdup(tmp);
    
    tmp = json_extract_string(json, "category");
    if (tmp) category = strdup(tmp);
    
    if (!recipe_id || !name || !version) {
        fprintf(stderr, "Invalid recipe: missing recipe_id/code/recipe, name, or version\n");
        free(json);
        if (recipe_id) free(recipe_id);
        if (name) free(name);
        if (version) free(version);
        if (category) free(category);
        return 1;
    }
    
    /* Compute hash */
    char hash_hex[65];
    compute_sha256(json, json_size, hash_hex);
    
    printf("Adding recipe: %s (%s) v%s\n", recipe_id, name, version);
    printf("Hash: sha256:%s\n", hash_hex);
    
    /* Open database */
    sqlite3 *db;
    int rc = bf_sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        free(json);
        sqlite3_close(db);
        return 1;
    }
    
    /* Insert recipe */
    const char *insert_sql = 
        "INSERT OR REPLACE INTO recipes "
        "(recipe_id, name, version, hash, json_data, created_at, category) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)";
    
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, insert_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        free(json);
        sqlite3_close(db);
        return 1;
    }
    
    char hash_full[72];
    snprintf(hash_full, sizeof(hash_full), "sha256:%s", hash_hex);
    
    sqlite3_bind_text(stmt, 1, recipe_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, version, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, hash_full, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, json, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 6, (sqlite3_int64)time(NULL));
    sqlite3_bind_text(stmt, 7, category, -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert recipe: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        free(json);
        sqlite3_close(db);
        return 1;
    }
    
    printf("✓ Recipe added to registry\n");
    
    sqlite3_finalize(stmt);
    free(recipe_id);
    free(name);
    free(version);
    if (category) free(category);
    free(json);
    sqlite3_close(db);
    sync_catalog(catalog_db_path);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Command: validate
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_validate(const char *recipe_path) {
    size_t json_size;
    char *json = read_file(recipe_path, &json_size);
    if (!json) {
        fprintf(stderr, "Cannot read file: %s\n", recipe_path);
        return 1;
    }
    
    printf("Validating: %s\n", recipe_path);
    
    /* Check required fields */
    const char *tmp;
    char recipe_id[1024] = "";
    char name[1024] = "";
    char version[1024] = "";
    char description[1024] = "";

    tmp = json_extract_string(json, "recipe_id");
    if (!tmp) tmp = json_extract_string(json, "code");
    if (!tmp) tmp = json_extract_string(json, "recipe");
    if (tmp) snprintf(recipe_id, sizeof(recipe_id), "%s", tmp);

    tmp = json_extract_string(json, "name");
    if (tmp) snprintf(name, sizeof(name), "%s", tmp);

    tmp = json_extract_string(json, "version");
    if (tmp) snprintf(version, sizeof(version), "%s", tmp);

    tmp = json_extract_string(json, "description");
    if (tmp) snprintf(description, sizeof(description), "%s", tmp);
    
    int valid = 1;
    
    if (!recipe_id[0]) {
        fprintf(stderr, "✗ Missing: recipe_id, code, or recipe\n");
        valid = 0;
    } else {
        printf("✓ recipe_id: %s\n", recipe_id);
    }
    
    if (!name[0]) {
        fprintf(stderr, "⚠ Warning: Missing name\n");
    } else {
        printf("✓ name: %s\n", name);
    }
    
    if (!version[0]) {
        fprintf(stderr, "⚠ Warning: Missing version\n");
    } else {
        printf("✓ version: %s\n", version);
    }
    
    if (!description[0]) {
        fprintf(stderr, "⚠ Warning: Missing description\n");
    }
    
    /* Check for recipe/run contract fields. inputs may be an object or array. */
    if (!json_has_key(json, "inputs")) {
        fprintf(stderr, "⚠ Warning: Missing inputs (runner may supply --input)\n");
    } else {
        printf("✓ inputs defined\n");
    }
    
    if (!json_has_key(json, "outputs")) {
        fprintf(stderr, "⚠ Warning: Missing outputs\n");
    } else {
        printf("✓ outputs defined\n");
    }
    
    if (!json_has_key(json, "stages") && !json_has_key(json, "steps")) {
        fprintf(stderr, "✗ Missing: stages or steps array\n");
        valid = 0;
    } else {
        printf("✓ %s defined\n", json_has_key(json, "stages") ? "stages" : "steps");
    }

    if (!json_has_key(json, "operator") &&
        !json_has_key(json, "uses") &&
        !json_has_key(json, "bin")) {
        fprintf(stderr, "✗ Missing: stage operator, uses, or bin binding\n");
        valid = 0;
    } else {
        printf("✓ native step bindings defined\n");
    }

    /* Compute hash */
    char hash_hex[65];
    compute_sha256(json, json_size, hash_hex);
    printf("✓ SHA-256: %s\n", hash_hex);
    
    free(json);
    
    if (valid) {
        printf("\n✓ Recipe is valid\n");
        return 0;
    } else {
        printf("\n✗ Recipe validation failed\n");
        return 1;
    }
}

/* ═══════════════════════════════════════════════════════════════════
 * Command: hash
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_hash(const char *recipe_path) {
    size_t json_size;
    char *json = read_file(recipe_path, &json_size);
    if (!json) {
        fprintf(stderr, "Cannot read file: %s\n", recipe_path);
        return 1;
    }
    
    char hash_hex[65];
    compute_sha256(json, json_size, hash_hex);
    printf("sha256:%s\n", hash_hex);
    
    free(json);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════ */

static void print_usage(void) {
    fprintf(stderr,
            "BonfyreRecipe — Recipe Registry Management\n\n"
            "Usage:\n"
            "  bonfyre-recipe init [--db PATH]\n"
            "  bonfyre-recipe list [--category CAT] [--tag TAG]\n"
            "  bonfyre-recipe show <RECIPE_ID>\n"
            "  bonfyre-recipe add <RECIPE.json>\n"
            "  bonfyre-recipe validate <RECIPE.json>\n"
            "  bonfyre-recipe search <QUERY>\n"
            "  bonfyre-recipe hash <RECIPE.json>\n\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    
    const char *cmd = argv[1];
    const char *db_path_override = NULL;
    
    /* Parse --db option */
    for (int i = 2; i < argc - 1; i++) {
        if (strcmp(argv[i], "--db") == 0) {
            db_path_override = argv[i + 1];
        }
    }
    
    const char *db_path = get_db_path(db_path_override);
    const char *catalog_db_path = get_catalog_db_path(db_path_override);
    
    if (strcmp(cmd, "init") == 0) {
        return cmd_init(db_path);
    }
    else if (strcmp(cmd, "list") == 0) {
        const char *category = NULL;
        const char *tag = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--category") == 0) category = argv[i + 1];
            if (strcmp(argv[i], "--tag") == 0) tag = argv[i + 1];
        }
        if (cmd_list_catalog(catalog_db_path, category, tag) == 0) return 0;
        return cmd_list(db_path, category, tag);
    }
    else if (strcmp(cmd, "show") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-recipe show <RECIPE_ID>\n");
            return 1;
        }
        if (cmd_show_catalog(catalog_db_path, argv[2]) == 0) return 0;
        return cmd_show(db_path, argv[2]);
    }
    else if (strcmp(cmd, "add") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-recipe add <RECIPE.json>\n");
            return 1;
        }
        return cmd_add(db_path, catalog_db_path, argv[2]);
    }
    else if (strcmp(cmd, "search") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-recipe search <QUERY>\n");
            return 1;
        }
        if (cmd_search_catalog(catalog_db_path, argv[2]) == 0) return 0;
        return cmd_search(db_path, argv[2]);
    }
    else if (strcmp(cmd, "validate") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-recipe validate <RECIPE.json>\n");
            return 1;
        }
        return cmd_validate(argv[2]);
    }
    else if (strcmp(cmd, "hash") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-recipe hash <RECIPE.json>\n");
            return 1;
        }
        return cmd_hash(argv[2]);
    }
    else {
        fprintf(stderr, "Unknown command: %s\n", cmd);
        print_usage();
        return 1;
    }
    
    return 0;
}
