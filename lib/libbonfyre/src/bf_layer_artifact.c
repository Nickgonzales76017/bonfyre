#include "bonfyre.h"
#include "bf_json.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static int json_copy_str(const bf_json_doc_t *doc, const bf_json_node_t *obj,
                         const char *key, char *out, size_t out_sz) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key);
    if (!n) {
        if (out_sz) out[0] = '\0';
        return 0;
    }
    return bf_json_get_str_copy(n, out, out_sz) > 0;
}

static int json_array_join(const bf_json_doc_t *doc, const bf_json_node_t *obj,
                           const char *key, char *out, size_t out_sz) {
    const bf_json_node_t *arr = bf_json_obj_get(doc, obj, key);
    if (!arr || arr->type != BF_JSON_ARRAY || out_sz == 0) {
        if (out_sz) out[0] = '\0';
        return 0;
    }
    size_t used = 0;
    out[0] = '\0';
    for (const bf_json_node_t *child = bf_json_child_first(doc, arr);
         child;
         child = bf_json_child_next(doc, child)) {
        char item[256];
        if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
        int n = snprintf(out + used, out_sz - used, "%s%s", used ? ", " : "", item);
        if (n < 0 || (size_t)n >= out_sz - used) break;
        used += (size_t)n;
    }
    return used > 0;
}

static int json_string_equals(const bf_json_doc_t *doc, const bf_json_node_t *obj,
                              const char *key, const char *value) {
    char buf[128];
    return json_copy_str(doc, obj, key, buf, sizeof(buf)) && strcmp(buf, value) == 0;
}

static int read_sum_double(sqlite3 *db, const char *sql, const char *artifact_id, double *out_sum, int *out_count) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, artifact_id, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) {
        if (out_count) *out_count = sqlite3_column_int(st, 0);
        if (out_sum) *out_sum = sqlite3_column_double(st, 1);
    }
    sqlite3_finalize(st);
    return rc == SQLITE_ROW ? 0 : 1;
}

static int bf_path_dir_exists(const char *path) {
    struct stat st;
    return path && path[0] && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

int bf_layer_resolve_root(const char *root, char *buf, size_t sz, char *attempted, size_t attempted_sz) {
    const char *home;
    char repo_root[PATH_MAX];
    char repo_relative[PATH_MAX];

    if (!buf || sz == 0) return 1;
    buf[0] = '\0';
    if (attempted && attempted_sz) attempted[0] = '\0';

    if (!root || !root[0]) {
        home = getenv("HOME");
        if (!home) home = "/tmp";
        if (snprintf(buf, sz, "%s/.local/share/bonfyre", home) >= (int)sz) return 1;
        return 0;
    }

    if (attempted && attempted_sz)
        snprintf(attempted, attempted_sz, "%s", root);
    if (bf_path_dir_exists(root)) {
        snprintf(buf, sz, "%s", root);
        return 0;
    }

    if (bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) {
        if (snprintf(repo_relative, sizeof(repo_relative), "%s/%s", repo_root, root) < (int)sizeof(repo_relative)) {
            if (attempted && attempted_sz) {
                size_t used = strlen(attempted);
                snprintf(attempted + used, attempted_sz > used ? attempted_sz - used : 0,
                         "%s%s", used ? " ; " : "", repo_relative);
            }
            if (bf_path_dir_exists(repo_relative)) {
                snprintf(buf, sz, "%s", repo_relative);
                return 0;
            }
        }
    }

    fprintf(stderr, "bonfyre-layer: unable to resolve --root '%s'\n", root);
    if (attempted && attempted[0])
        fprintf(stderr, "  attempted: %s\n", attempted);
    return 1;
}

static int json_appendf(char **buf, size_t *len, size_t *cap, const char *fmt, ...) {
    va_list ap;
    int need;
    char *next;
    if (!buf || !len || !cap || !fmt) return 1;
    if (!*buf) {
        *cap = 1024;
        *len = 0;
        *buf = (char *)malloc(*cap);
        if (!*buf) return 1;
        (*buf)[0] = '\0';
    }
    while (1) {
        va_start(ap, fmt);
        need = vsnprintf(*buf + *len, *cap - *len, fmt, ap);
        va_end(ap);
        if (need < 0) return 1;
        if ((size_t)need < *cap - *len) {
            *len += (size_t)need;
            return 0;
        }
        *cap = (*cap * 2) + (size_t)need + 32;
        next = (char *)realloc(*buf, *cap);
        if (!next) return 1;
        *buf = next;
    }
}

static char *json_quote_sql(const char *s) {
    char *esc;
    char *out;
    size_t n;
    if (!s) return strdup("\"\"");
    esc = sqlite3_mprintf("%q", s);
    if (!esc) return NULL;
    n = strlen(esc);
    out = (char *)malloc(n + 3);
    if (!out) {
        sqlite3_free(esc);
        return NULL;
    }
    out[0] = '"';
    memcpy(out + 1, esc, n);
    out[n + 1] = '"';
    out[n + 2] = '\0';
    sqlite3_free(esc);
    return out;
}

static void utc_now_iso(char out[32]) {
    time_t now = time(NULL);
    struct tm tmv;
    gmtime_r(&now, &tmv);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static char *bf_read_file_full(const char *path) {
    FILE *fp;
    long sz;
    char *buf;
    if (!path) return NULL;
    fp = fopen(path, "rb");
    if (!fp) return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    sz = ftell(fp);
    if (sz < 0 || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    buf = (char *)malloc((size_t)sz + 1);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    if (fread(buf, 1, (size_t)sz, fp) != (size_t)sz) {
        fclose(fp);
        free(buf);
        return NULL;
    }
    buf[sz] = '\0';
    fclose(fp);
    return buf;
}

static int layer_init_index_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS layer_index ("
        " artifact_id TEXT PRIMARY KEY,"
        " artifact_kind TEXT,"
        " source_model TEXT,"
        " source_recipe TEXT,"
        " source_collection TEXT,"
        " families_json TEXT,"
        " capabilities_json TEXT,"
        " workflow_steps_json TEXT,"
        " verification_status TEXT,"
        " materialization_status TEXT,"
        " lifecycle_status TEXT,"
        " payload_hash TEXT,"
        " manifest_hash TEXT,"
        " compatibility_tags_json TEXT"
        ");";
    return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK ? 0 : 1;
}

static int layer_init_graph_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS atoms ("
        " atom_id TEXT PRIMARY KEY,"
        " artifact_id TEXT,"
        " atom_kind TEXT,"
        " payload_hash TEXT,"
        " payload_json TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS operators ("
        " operator_id TEXT PRIMARY KEY,"
        " artifact_id TEXT,"
        " operator_kind TEXT,"
        " payload_json TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS realizations ("
        " realization_id TEXT PRIMARY KEY,"
        " artifact_id TEXT,"
        " realization_kind TEXT,"
        " payload_hash TEXT,"
        " payload_json TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS edges ("
        " src_id TEXT,"
        " rel TEXT,"
        " dst_id TEXT,"
        " meta_json TEXT,"
        " PRIMARY KEY(src_id, rel, dst_id)"
        ");";
    return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK ? 0 : 1;
}

static int layer_init_queue_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS jobs ("
        " job_id TEXT PRIMARY KEY,"
        " job_type TEXT NOT NULL,"
        " artifact_id TEXT NOT NULL,"
        " payload_json TEXT NOT NULL,"
        " status TEXT NOT NULL,"
        " priority INTEGER NOT NULL,"
        " created_at TEXT NOT NULL"
        ");";
    return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK ? 0 : 1;
}

static int json_array_contains(const bf_json_doc_t *doc, const bf_json_node_t *obj,
                               const char *key, const char *needle) {
    const bf_json_node_t *arr = bf_json_obj_get(doc, obj, key);
    if (!arr || arr->type != BF_JSON_ARRAY || !needle || !needle[0]) return 0;
    for (const bf_json_node_t *child = bf_json_child_first(doc, arr);
         child; child = bf_json_child_next(doc, child)) {
        char item[256];
        if (bf_json_get_str_copy(child, item, sizeof(item)) > 0 && strcmp(item, needle) == 0) {
            return 1;
        }
    }
    return 0;
}

int bf_layer_state_db_path(const char *root, const char *db_name, char *buf, size_t sz) {
    const char *home;
    char resolved_root[PATH_MAX];
    char attempted[PATH_MAX * 2];
    if (!db_name || !buf || sz == 0) return 1;
    if (root && root[0]) {
        if (bf_layer_resolve_root(root, resolved_root, sizeof(resolved_root), attempted, sizeof(attempted)) != 0)
            return 1;
        return snprintf(buf, sz, "%s/%s", resolved_root, db_name) >= (int)sz ? 1 : 0;
    }
    home = getenv("HOME");
    if (!home) home = "/tmp";
    return snprintf(buf, sz, "%s/.local/share/bonfyre/%s", home, db_name) >= (int)sz ? 1 : 0;
}

int bf_layer_load_json(const char *root, const char *artifact_id, char **json_out) {
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    const unsigned char *txt;
    char *dup = NULL;

    if (!artifact_id || !json_out) return 1;
    *json_out = NULL;
    if (bf_layer_state_db_path(root, "layers.db", db_path, sizeof(db_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db, "SELECT artifact_json FROM layer_artifacts WHERE artifact_id=?", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, artifact_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        sqlite3_close(db);
        return 1;
    }
    txt = sqlite3_column_text(st, 0);
    if (txt) dup = strdup((const char *)txt);
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (!dup) return 1;
    *json_out = dup;
    return 0;
}

int bf_layer_report_md(const char *artifact_json, char **out_md) {
    char err[128], artifact_id[256], artifact_kind[128], families[512], steps[512];
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
    char *buf;
    int n;
    if (!artifact_json || !out_md) return 1;
    doc = bf_json_parse_str(artifact_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", artifact_id, sizeof(artifact_id));
    json_copy_str(doc, root, "artifact_kind", artifact_kind, sizeof(artifact_kind));
    json_array_join(doc, root, "families", families, sizeof(families));
    json_array_join(doc, root, "workflow_steps", steps, sizeof(steps));
    n = snprintf(NULL, 0,
                 "# Layer Report\n\n- artifact_id: %s\n- artifact_kind: %s\n- families: %s\n- workflow_steps: %s\n",
                 artifact_id, artifact_kind, families, steps);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) { bf_json_free(doc); return 1; }
    snprintf(buf, (size_t)n + 1,
             "# Layer Report\n\n- artifact_id: %s\n- artifact_kind: %s\n- families: %s\n- workflow_steps: %s\n",
             artifact_id, artifact_kind, families, steps);
    *out_md = buf;
    bf_json_free(doc);
    return 0;
}

int bf_layer_auth_source_json(const char *artifact_json, char **out_json) {
    char err[128], artifact_id[256], source_model[256], source_collection[256], source_recipe[256], source_file[PATH_MAX];
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
    char *buf;
    int n;
    if (!artifact_json || !out_json) return 1;
    doc = bf_json_parse_str(artifact_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", artifact_id, sizeof(artifact_id));
    json_copy_str(doc, root, "source_model", source_model, sizeof(source_model));
    json_copy_str(doc, root, "source_collection", source_collection, sizeof(source_collection));
    json_copy_str(doc, root, "source_recipe", source_recipe, sizeof(source_recipe));
    json_copy_str(doc, root, "source_file", source_file, sizeof(source_file));
    n = snprintf(NULL, 0,
        "{\n"
        "  \"access_status\": \"metadata-accessible\",\n"
        "  \"artifact_id\": \"%s\",\n"
        "  \"auth_provider\": \"%s\",\n"
        "  \"requires_auth\": false,\n"
        "  \"source_collection\": \"%s\",\n"
        "  \"source_file\": \"%s\",\n"
        "  \"source_model\": \"%s\",\n"
        "  \"source_recipe\": \"%s\"\n"
        "}",
        artifact_id, source_model[0] ? "huggingface" : "", source_collection, source_file, source_model, source_recipe);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) { bf_json_free(doc); return 1; }
    snprintf(buf, (size_t)n + 1,
        "{\n"
        "  \"access_status\": \"metadata-accessible\",\n"
        "  \"artifact_id\": \"%s\",\n"
        "  \"auth_provider\": \"%s\",\n"
        "  \"requires_auth\": false,\n"
        "  \"source_collection\": \"%s\",\n"
        "  \"source_file\": \"%s\",\n"
        "  \"source_model\": \"%s\",\n"
        "  \"source_recipe\": \"%s\"\n"
        "}",
        artifact_id, source_model[0] ? "huggingface" : "", source_collection, source_file, source_model, source_recipe);
    *out_json = buf;
    bf_json_free(doc);
    return 0;
}

int bf_layer_gate_json(const char *artifact_json, const char *operation, char **out_json) {
    char err[128], artifact_id[256], verification[64], materialization[64];
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
    const char *decision = "allow";
    const char *reason = "metadata policy check passed";
    char *buf;
    int n;
    if (!artifact_json || !operation || !out_json) return 1;
    doc = bf_json_parse_str(artifact_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", artifact_id, sizeof(artifact_id));
    json_copy_str(doc, root, "verification_status", verification, sizeof(verification));
    json_copy_str(doc, root, "materialization_status", materialization, sizeof(materialization));
    if ((strcmp(operation, "materialize") == 0 || strcmp(operation, "compress") == 0) &&
        strcmp(verification, "unverified") == 0) {
        decision = "warn";
        reason = "tensor surface is not verified yet";
    }
    if (strcmp(operation, "run") == 0 && strcmp(materialization, "materialized") != 0) {
        decision = "deny";
        reason = "runtime execution requires a materialized artifact";
    }
    n = snprintf(NULL, 0,
                 "{\n  \"artifact_id\": \"%s\",\n  \"decision\": \"%s\",\n  \"operation\": \"%s\",\n  \"reasons\": [\n    \"%s\"\n  ]\n}",
                 artifact_id, decision, operation, reason);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) { bf_json_free(doc); return 1; }
    snprintf(buf, (size_t)n + 1,
             "{\n  \"artifact_id\": \"%s\",\n  \"decision\": \"%s\",\n  \"operation\": \"%s\",\n  \"reasons\": [\n    \"%s\"\n  ]\n}",
             artifact_id, decision, operation, reason);
    *out_json = buf;
    bf_json_free(doc);
    return 0;
}

double bf_layer_estimated_cost(const char *operation) {
    if (!operation) return 0.01;
    if (strcmp(operation, "verify") == 0) return 0.02;
    if (strcmp(operation, "materialize") == 0) return 1.25;
    if (strcmp(operation, "compose") == 0) return 0.05;
    if (strcmp(operation, "run") == 0) return 0.25;
    return 0.01;
}

int bf_layer_tier_json(const char *artifact_json, char **out_json) {
    char err[128], artifact_id[256], artifact_kind[128], materialization[64], verification[64];
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
    const char *tier = "instant";
    char *buf;
    int n;
    if (!artifact_json || !out_json) return 1;
    doc = bf_json_parse_str(artifact_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", artifact_id, sizeof(artifact_id));
    json_copy_str(doc, root, "artifact_kind", artifact_kind, sizeof(artifact_kind));
    json_copy_str(doc, root, "materialization_status", materialization, sizeof(materialization));
    json_copy_str(doc, root, "verification_status", verification, sizeof(verification));
    if (strcmp(artifact_kind, "virtual_composite") == 0) tier = "deep";
    else if (strcmp(materialization, "materialized") == 0) tier = "batch";
    else if (strcmp(artifact_kind, "onnx_slice") == 0 || strcmp(artifact_kind, "hf_tensor_pack") == 0) tier = "batch";
    else if (strcmp(verification, "verified") == 0) tier = "fast";
    n = snprintf(NULL, 0,
                 "{\n  \"artifact_id\": \"%s\",\n  \"artifact_kind\": \"%s\",\n  \"materialization_status\": \"%s\",\n  \"recommended_tier\": \"%s\",\n  \"verification_status\": \"%s\"\n}",
                 artifact_id, artifact_kind, materialization, tier, verification);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) { bf_json_free(doc); return 1; }
    snprintf(buf, (size_t)n + 1,
             "{\n  \"artifact_id\": \"%s\",\n  \"artifact_kind\": \"%s\",\n  \"materialization_status\": \"%s\",\n  \"recommended_tier\": \"%s\",\n  \"verification_status\": \"%s\"\n}",
             artifact_id, artifact_kind, materialization, tier, verification);
    *out_json = buf;
    bf_json_free(doc);
    return 0;
}

int bf_layer_economy_json(const char *artifact_json, const char *operation, char **out_json) {
    char err[128], artifact_id[256], artifact_kind[128], materialization[64], verification[64];
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
    const char *tier = "instant";
    double cost = bf_layer_estimated_cost(operation);
    char *buf;
    int n;
    if (!artifact_json || !operation || !out_json) return 1;
    doc = bf_json_parse_str(artifact_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", artifact_id, sizeof(artifact_id));
    json_copy_str(doc, root, "artifact_kind", artifact_kind, sizeof(artifact_kind));
    json_copy_str(doc, root, "materialization_status", materialization, sizeof(materialization));
    json_copy_str(doc, root, "verification_status", verification, sizeof(verification));
    if (strcmp(artifact_kind, "virtual_composite") == 0) tier = "deep";
    else if (strcmp(materialization, "materialized") == 0) tier = "batch";
    else if (strcmp(artifact_kind, "onnx_slice") == 0 || strcmp(artifact_kind, "hf_tensor_pack") == 0) tier = "batch";
    else if (strcmp(verification, "verified") == 0) tier = "fast";
    n = snprintf(NULL, 0,
                 "{\n  \"artifact_id\": \"%s\",\n  \"budget_class\": \"%s\",\n  \"estimated_cost_usd\": %.2f,\n  \"operation\": \"%s\",\n  \"recommended_tier\": \"%s\"\n}",
                 artifact_id, cost < 0.1 ? "light" : "heavy", cost, operation, tier);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) { bf_json_free(doc); return 1; }
    snprintf(buf, (size_t)n + 1,
             "{\n  \"artifact_id\": \"%s\",\n  \"budget_class\": \"%s\",\n  \"estimated_cost_usd\": %.2f,\n  \"operation\": \"%s\",\n  \"recommended_tier\": \"%s\"\n}",
             artifact_id, cost < 0.1 ? "light" : "heavy", cost, operation, tier);
    *out_json = buf;
    bf_json_free(doc);
    return 0;
}

int bf_layer_finance_json(const char *root, const char *artifact_id, char **out_json) {
    char meter_path[PATH_MAX], ledger_path[PATH_MAX];
    sqlite3 *mdb = NULL, *ldb = NULL;
    double meter_sum = 0.0, ledger_sum = 0.0, price;
    int meter_count = 0, ledger_count = 0;
    char *buf;
    int n;
    if (!artifact_id || !out_json) return 1;
    if (bf_layer_state_db_path(root, "meter.db", meter_path, sizeof(meter_path)) != 0) return 1;
    if (bf_layer_state_db_path(root, "ledger.db", ledger_path, sizeof(ledger_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(meter_path, &mdb) != SQLITE_OK) return 1;
    if (bf_sqlite3_open_ro(ledger_path, &ldb) != SQLITE_OK) { sqlite3_close(mdb); return 1; }
    read_sum_double(mdb, "SELECT COUNT(*), COALESCE(SUM(estimated_cost), 0.0) FROM meter_events WHERE artifact_id=?", artifact_id, &meter_sum, &meter_count);
    read_sum_double(ldb, "SELECT COUNT(*), COALESCE(SUM(value_units), 0.0) FROM ledger_events WHERE artifact_id=?", artifact_id, &ledger_sum, &ledger_count);
    sqlite3_close(mdb);
    sqlite3_close(ldb);
    price = meter_sum * 3.0;
    n = snprintf(NULL, 0,
                 "{\n  \"artifact_id\": \"%s\",\n  \"estimated_cost_usd\": %.2f,\n  \"estimated_margin_usd\": %.2f,\n  \"implied_price_usd\": %.2f,\n  \"ledger_events\": %d,\n  \"ledger_value_units\": %.1f,\n  \"meter_events\": %d\n}",
                 artifact_id, meter_sum, price - meter_sum, price, ledger_count, ledger_sum, meter_count);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) return 1;
    snprintf(buf, (size_t)n + 1,
             "{\n  \"artifact_id\": \"%s\",\n  \"estimated_cost_usd\": %.2f,\n  \"estimated_margin_usd\": %.2f,\n  \"implied_price_usd\": %.2f,\n  \"ledger_events\": %d,\n  \"ledger_value_units\": %.1f,\n  \"meter_events\": %d\n}",
             artifact_id, meter_sum, price - meter_sum, price, ledger_count, ledger_sum, meter_count);
    *out_json = buf;
    return 0;
}

int bf_layer_pay_json(const char *artifact_id, const char *operation, char **out_json) {
    double cost;
    char *buf;
    int n;
    if (!artifact_id || !operation || !out_json) return 1;
    cost = bf_layer_estimated_cost(operation);
    n = snprintf(NULL, 0,
                 "{\n  \"artifact_id\": \"%s\",\n  \"operation\": \"%s\",\n  \"quote_cents\": %d,\n  \"quote_usd\": %.2f\n}",
                 artifact_id, operation, (int)(cost * 100.0 + 0.5), cost);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) return 1;
    snprintf(buf, (size_t)n + 1,
             "{\n  \"artifact_id\": \"%s\",\n  \"operation\": \"%s\",\n  \"quote_cents\": %d,\n  \"quote_usd\": %.2f\n}",
             artifact_id, operation, (int)(cost * 100.0 + 0.5), cost);
    *out_json = buf;
    return 0;
}

int bf_layer_moq_json(const char *artifact_json, char **out_json) {
    char err[128], artifact_id[256], artifact_kind[128], payload_hash[128], tags[512];
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
    char *buf;
    int n;
    if (!artifact_json || !out_json) return 1;
    doc = bf_json_parse_str(artifact_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    json_copy_str(doc, root, "artifact_id", artifact_id, sizeof(artifact_id));
    json_copy_str(doc, root, "artifact_kind", artifact_kind, sizeof(artifact_kind));
    json_copy_str(doc, root, "payload_hash", payload_hash, sizeof(payload_hash));
    json_array_join(doc, root, "compatibility_tags", tags, sizeof(tags));
    n = snprintf(NULL, 0,
                 "{\n  \"artifact_id\": \"%s\",\n  \"artifact_kind\": \"%s\",\n  \"compatibility_tags\": [%s],\n  \"payload_hash\": %s,\n  \"stream_mode\": \"%s\"\n}",
                 artifact_id, artifact_kind, tags[0] ? "\"" : "", payload_hash[0] ? "\"" : "null",
                 payload_hash[0] ? "payload-stream" : "manifest-stream");
    /* Build tags as quoted JSON array string inline. */
    {
        char tags_json[1024] = "";
        if (tags[0]) {
            const char *p = tags;
            size_t used = 0;
            while (*p && used + 4 < sizeof(tags_json)) {
                const char *comma = strstr(p, ", ");
                size_t len = comma ? (size_t)(comma - p) : strlen(p);
                int m = snprintf(tags_json + used, sizeof(tags_json) - used, "%s\"%.*s\"", used ? ", " : "", (int)len, p);
                if (m < 0 || (size_t)m >= sizeof(tags_json) - used) break;
                used += (size_t)m;
                if (!comma) break;
                p = comma + 2;
            }
        }
        n = snprintf(NULL, 0,
                     "{\n  \"artifact_id\": \"%s\",\n  \"artifact_kind\": \"%s\",\n  \"compatibility_tags\": [\n    %s\n  ],\n  \"payload_hash\": %s%s%s,\n  \"stream_mode\": \"%s\"\n}",
                     artifact_id, artifact_kind, tags_json, payload_hash[0] ? "\"" : "", payload_hash, payload_hash[0] ? "\"" : "null",
                     payload_hash[0] ? "payload-stream" : "manifest-stream");
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) { bf_json_free(doc); return 1; }
        snprintf(buf, (size_t)n + 1,
                 "{\n  \"artifact_id\": \"%s\",\n  \"artifact_kind\": \"%s\",\n  \"compatibility_tags\": [\n    %s\n  ],\n  \"payload_hash\": %s%s%s,\n  \"stream_mode\": \"%s\"\n}",
                 artifact_id, artifact_kind, tags_json, payload_hash[0] ? "\"" : "", payload_hash, payload_hash[0] ? "\"" : "null",
                 payload_hash[0] ? "payload-stream" : "manifest-stream");
    }
    *out_json = buf;
    bf_json_free(doc);
    return 0;
}

int bf_layer_rebuild_index(const char *root) {
    char src_path[PATH_MAX], dst_path[PATH_MAX];
    sqlite3 *src = NULL, *dst = NULL;
    sqlite3_stmt *st = NULL;
    if (bf_layer_state_db_path(root, "layers.db", src_path, sizeof(src_path)) != 0) return 1;
    if (bf_layer_state_db_path(root, "index.db", dst_path, sizeof(dst_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(src_path, &src) != SQLITE_OK) return 1;
    if (bf_sqlite3_open(dst_path, &dst) != SQLITE_OK) { sqlite3_close(src); return 1; }
    if (layer_init_index_schema(dst) != 0) goto fail;
    if (sqlite3_exec(dst, "DELETE FROM layer_index", NULL, NULL, NULL) != SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(src,
        "SELECT artifact_id, artifact_kind, source_model, source_recipe, source_collection, "
        "families_json, capabilities_json, workflow_steps_json, verification_status, "
        "materialization_status, lifecycle_status, payload_hash, manifest_hash, compatibility_tags_json "
        "FROM layer_artifacts ORDER BY artifact_id",
        -1, &st, NULL) != SQLITE_OK) goto fail;
    while (sqlite3_step(st) == SQLITE_ROW) {
        sqlite3_stmt *ins = NULL;
        if (sqlite3_prepare_v2(dst,
            "INSERT OR REPLACE INTO layer_index("
            "artifact_id, artifact_kind, source_model, source_recipe, source_collection, "
            "families_json, capabilities_json, workflow_steps_json, verification_status, "
            "materialization_status, lifecycle_status, payload_hash, manifest_hash, compatibility_tags_json"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            -1, &ins, NULL) != SQLITE_OK) {
            goto fail;
        }
        for (int i = 0; i < 14; i++) {
            const unsigned char *txt = sqlite3_column_text(st, i);
            sqlite3_bind_text(ins, i + 1, txt ? (const char *)txt : "", -1, SQLITE_TRANSIENT);
        }
        sqlite3_step(ins);
        sqlite3_finalize(ins);
    }
    sqlite3_finalize(st);
    sqlite3_close(src);
    sqlite3_close(dst);
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    if (src) sqlite3_close(src);
    if (dst) sqlite3_close(dst);
    return 1;
}

int bf_layer_query_json(const char *root,
                        const char *family,
                        const char *workflow,
                        const char *source,
                        const char *status,
                        const char *kind,
                        int bridge_required,
                        char **out_json) {
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char *buf = NULL;
    size_t len = 0, cap = 0;
    int first = 1;
    if (!out_json) return 1;
    *out_json = NULL;
    if (bf_layer_state_db_path(root, "index.db", db_path, sizeof(db_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db, "SELECT * FROM layer_index ORDER BY artifact_id", -1, &st, NULL) != SQLITE_OK) goto fail;
    if (json_appendf(&buf, &len, &cap, "[") != 0) goto fail;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *artifact_id = (const char *)sqlite3_column_text(st, 0);
        const char *artifact_kind = (const char *)sqlite3_column_text(st, 1);
        const char *source_model = (const char *)sqlite3_column_text(st, 2);
        const char *source_recipe = (const char *)sqlite3_column_text(st, 3);
        const char *source_collection = (const char *)sqlite3_column_text(st, 4);
        const char *families_json = (const char *)sqlite3_column_text(st, 5);
        const char *capabilities_json = (const char *)sqlite3_column_text(st, 6);
        const char *workflow_steps_json = (const char *)sqlite3_column_text(st, 7);
        const char *verification_status = (const char *)sqlite3_column_text(st, 8);
        const char *materialization_status = (const char *)sqlite3_column_text(st, 9);
        const char *lifecycle_status = (const char *)sqlite3_column_text(st, 10);
        const char *payload_hash = (const char *)sqlite3_column_text(st, 11);
        const char *manifest_hash = (const char *)sqlite3_column_text(st, 12);
        const char *compatibility_tags_json = (const char *)sqlite3_column_text(st, 13);
        int keep = 1;
        char err[128];
        bf_json_doc_t *fdoc = NULL, *wdoc = NULL, *tdoc = NULL;
        const bf_json_node_t *froot = NULL, *wroot = NULL, *troot = NULL;
        if (family && family[0]) {
            fdoc = bf_json_parse_str(families_json ? families_json : "[]", err, sizeof(err));
            froot = fdoc ? bf_json_root(fdoc) : NULL;
            keep = froot ? json_array_contains(fdoc, froot, "", family) : 0;
            if (!keep && fdoc) {
                const bf_json_node_t *arr = bf_json_root(fdoc);
                keep = 0;
                for (const bf_json_node_t *child = bf_json_child_first(fdoc, arr); child; child = bf_json_child_next(fdoc, child)) {
                    char item[256];
                    if (bf_json_get_str_copy(child, item, sizeof(item)) > 0 && strcmp(item, family) == 0) {
                        keep = 1; break;
                    }
                }
            }
        }
        if (keep && workflow && workflow[0]) {
            wdoc = bf_json_parse_str(workflow_steps_json ? workflow_steps_json : "[]", err, sizeof(err));
            wroot = wdoc ? bf_json_root(wdoc) : NULL;
            keep = 0;
            if (wroot) {
                for (const bf_json_node_t *child = bf_json_child_first(wdoc, wroot); child; child = bf_json_child_next(wdoc, child)) {
                    char item[256];
                    if (bf_json_get_str_copy(child, item, sizeof(item)) > 0 && strcmp(item, workflow) == 0) {
                        keep = 1; break;
                    }
                }
            }
        }
        if (keep && source && source[0]) {
            const char *sm = source_model ? source_model : "";
            const char *sc = source_collection ? source_collection : "";
            keep = strstr(sm, source) != NULL || strstr(sc, source) != NULL;
        }
        if (keep && status && status[0]) {
            keep = (verification_status && strcmp(status, verification_status) == 0) ||
                   (materialization_status && strcmp(status, materialization_status) == 0) ||
                   (lifecycle_status && strcmp(status, lifecycle_status) == 0);
        }
        if (keep && kind && kind[0]) {
            keep = artifact_kind && strcmp(kind, artifact_kind) == 0;
        }
        if (keep && bridge_required) {
            tdoc = bf_json_parse_str(compatibility_tags_json ? compatibility_tags_json : "[]", err, sizeof(err));
            troot = tdoc ? bf_json_root(tdoc) : NULL;
            keep = 0;
            if (troot) {
                for (const bf_json_node_t *child = bf_json_child_first(tdoc, troot); child; child = bf_json_child_next(tdoc, child)) {
                    char item[256];
                    if (bf_json_get_str_copy(child, item, sizeof(item)) > 0 && strcmp(item, "bridge-required") == 0) {
                        keep = 1; break;
                    }
                }
            }
        }
        if (fdoc) bf_json_free(fdoc);
        if (wdoc) bf_json_free(wdoc);
        if (tdoc) bf_json_free(tdoc);
        if (!keep) continue;
        {
            char *q0 = json_quote_sql(artifact_id ? artifact_id : "");
            char *q1 = json_quote_sql(artifact_kind ? artifact_kind : "");
            char *q2 = json_quote_sql(source_model ? source_model : "");
            char *q3 = json_quote_sql(source_recipe ? source_recipe : "");
            char *q4 = json_quote_sql(source_collection ? source_collection : "");
            char *q8 = json_quote_sql(verification_status ? verification_status : "");
            char *q9 = json_quote_sql(materialization_status ? materialization_status : "");
            char *q10 = json_quote_sql(lifecycle_status ? lifecycle_status : "");
            char *q11 = payload_hash && payload_hash[0] ? json_quote_sql(payload_hash) : strdup("null");
            char *q12 = manifest_hash && manifest_hash[0] ? json_quote_sql(manifest_hash) : strdup("null");
            if (!q0 || !q1 || !q2 || !q3 || !q4 || !q8 || !q9 || !q10 || !q11 || !q12) goto fail;
            if (json_appendf(&buf, &len, &cap,
                "%s{"
                "\"artifact_id\":%s,"
                "\"artifact_kind\":%s,"
                "\"source_model\":%s,"
                "\"source_recipe\":%s,"
                "\"source_collection\":%s,"
                "\"families_json\":%s,"
                "\"capabilities_json\":%s,"
                "\"workflow_steps_json\":%s,"
                "\"verification_status\":%s,"
                "\"materialization_status\":%s,"
                "\"lifecycle_status\":%s,"
                "\"payload_hash\":%s,"
                "\"manifest_hash\":%s,"
                "\"compatibility_tags_json\":%s"
                "}",
                first ? "" : ",",
                q0, q1, q2, q3, q4,
                families_json ? families_json : "[]",
                capabilities_json ? capabilities_json : "[]",
                workflow_steps_json ? workflow_steps_json : "[]",
                q8, q9, q10, q11, q12,
                compatibility_tags_json ? compatibility_tags_json : "[]") != 0) {
                free(q0); free(q1); free(q2); free(q3); free(q4); free(q8); free(q9); free(q10); free(q11); free(q12);
                goto fail;
            }
            free(q0); free(q1); free(q2); free(q3); free(q4); free(q8); free(q9); free(q10); free(q11); free(q12);
        }
        first = 0;
    }
    if (json_appendf(&buf, &len, &cap, "]") != 0) goto fail;
    sqlite3_finalize(st);
    sqlite3_close(db);
    *out_json = buf;
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(buf);
    return 1;
}

int bf_layer_rebuild_graph(const char *root) {
    char src_path[PATH_MAX], dst_path[PATH_MAX];
    sqlite3 *src = NULL, *dst = NULL;
    sqlite3_stmt *st = NULL;
    if (bf_layer_state_db_path(root, "layers.db", src_path, sizeof(src_path)) != 0) return 1;
    if (bf_layer_state_db_path(root, "graph.db", dst_path, sizeof(dst_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(src_path, &src) != SQLITE_OK) return 1;
    if (bf_sqlite3_open(dst_path, &dst) != SQLITE_OK) { sqlite3_close(src); return 1; }
    if (layer_init_graph_schema(dst) != 0) goto fail;
    if (sqlite3_exec(dst, "DELETE FROM atoms; DELETE FROM operators; DELETE FROM realizations; DELETE FROM edges;", NULL, NULL, NULL) != SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(src, "SELECT artifact_json FROM layer_artifacts ORDER BY artifact_id", -1, &st, NULL) != SQLITE_OK) goto fail;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *artifact_json = (const char *)sqlite3_column_text(st, 0);
        char err[128], artifact_id[256] = "", manifest_hash[128] = "", tensor_index_hash[128] = "";
        char payload_hash[128] = "", artifact_kind[128] = "", source_model[256] = "", source_recipe[256] = "", verification_status[64] = "", materialization_status[64] = "";
        bf_json_doc_t *doc = bf_json_parse_str(artifact_json ? artifact_json : "{}", err, sizeof(err));
        const bf_json_node_t *root_json;
        if (!doc) continue;
        root_json = bf_json_root(doc);
        json_copy_str(doc, root_json, "artifact_id", artifact_id, sizeof(artifact_id));
        json_copy_str(doc, root_json, "manifest_hash", manifest_hash, sizeof(manifest_hash));
        json_copy_str(doc, root_json, "tensor_index_hash", tensor_index_hash, sizeof(tensor_index_hash));
        json_copy_str(doc, root_json, "payload_hash", payload_hash, sizeof(payload_hash));
        json_copy_str(doc, root_json, "artifact_kind", artifact_kind, sizeof(artifact_kind));
        json_copy_str(doc, root_json, "source_model", source_model, sizeof(source_model));
        json_copy_str(doc, root_json, "source_recipe", source_recipe, sizeof(source_recipe));
        json_copy_str(doc, root_json, "verification_status", verification_status, sizeof(verification_status));
        json_copy_str(doc, root_json, "materialization_status", materialization_status, sizeof(materialization_status));
        {
            char manifest_atom[512], tensor_atom[512], payload_real[512], import_op[512], verify_op[512];
            sqlite3_stmt *ins = NULL;
            snprintf(manifest_atom, sizeof(manifest_atom), "atom:layer_manifest:%s", artifact_id);
            snprintf(tensor_atom, sizeof(tensor_atom), "atom:tensor_index:%s", artifact_id);
            snprintf(payload_real, sizeof(payload_real), "realization:%s", artifact_id);
            snprintf(import_op, sizeof(import_op), "operator:import_recipe:%s", artifact_id);
            snprintf(verify_op, sizeof(verify_op), "operator:verify_tensor_surface:%s", artifact_id);
            sqlite3_prepare_v2(dst, "INSERT OR REPLACE INTO atoms(atom_id,artifact_id,atom_kind,payload_hash,payload_json) VALUES(?,?,?,?,?)", -1, &ins, NULL);
            sqlite3_bind_text(ins,1,manifest_atom,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,2,artifact_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,3,"layer_manifest",-1,SQLITE_STATIC);
            sqlite3_bind_text(ins,4,manifest_hash,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,5,"{}",-1,SQLITE_STATIC);
            sqlite3_step(ins); sqlite3_finalize(ins);
            sqlite3_prepare_v2(dst, "INSERT OR REPLACE INTO atoms(atom_id,artifact_id,atom_kind,payload_hash,payload_json) VALUES(?,?,?,?,?)", -1, &ins, NULL);
            sqlite3_bind_text(ins,1,tensor_atom,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,2,artifact_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,3,"tensor_index",-1,SQLITE_STATIC);
            sqlite3_bind_text(ins,4,tensor_index_hash,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,5,"{}",-1,SQLITE_STATIC);
            sqlite3_step(ins); sqlite3_finalize(ins);
            sqlite3_prepare_v2(dst, "INSERT OR REPLACE INTO operators(operator_id,artifact_id,operator_kind,payload_json) VALUES(?,?,?,?)", -1, &ins, NULL);
            sqlite3_bind_text(ins,1,import_op,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,2,artifact_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,3,"import_recipe",-1,SQLITE_STATIC);
            sqlite3_bind_text(ins,4,"{}",-1,SQLITE_STATIC);
            sqlite3_step(ins); sqlite3_finalize(ins);
            sqlite3_prepare_v2(dst, "INSERT OR REPLACE INTO operators(operator_id,artifact_id,operator_kind,payload_json) VALUES(?,?,?,?)", -1, &ins, NULL);
            sqlite3_bind_text(ins,1,verify_op,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,2,artifact_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,3,"verify_tensor_surface",-1,SQLITE_STATIC);
            sqlite3_bind_text(ins,4,"{}",-1,SQLITE_STATIC);
            sqlite3_step(ins); sqlite3_finalize(ins);
            sqlite3_prepare_v2(dst, "INSERT OR REPLACE INTO realizations(realization_id,artifact_id,realization_kind,payload_hash,payload_json) VALUES(?,?,?,?,?)", -1, &ins, NULL);
            sqlite3_bind_text(ins,1,payload_real,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,2,artifact_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(ins,3,artifact_kind,-1,SQLITE_TRANSIENT);
            if (payload_hash[0]) sqlite3_bind_text(ins,4,payload_hash,-1,SQLITE_TRANSIENT); else sqlite3_bind_null(ins,4);
            sqlite3_bind_text(ins,5,"{}",-1,SQLITE_STATIC);
            sqlite3_step(ins); sqlite3_finalize(ins);
            sqlite3_exec(dst, "BEGIN", NULL, NULL, NULL);
            sqlite3_stmt *edge = NULL;
            sqlite3_prepare_v2(dst, "INSERT OR REPLACE INTO edges(src_id,rel,dst_id,meta_json) VALUES(?,?,?,?)", -1, &edge, NULL);
#define EDGE(S,R,D) do { sqlite3_reset(edge); sqlite3_clear_bindings(edge); sqlite3_bind_text(edge,1,(S),-1,SQLITE_TRANSIENT); sqlite3_bind_text(edge,2,(R),-1,SQLITE_TRANSIENT); sqlite3_bind_text(edge,3,(D),-1,SQLITE_TRANSIENT); sqlite3_bind_text(edge,4,"{}",-1,SQLITE_STATIC); sqlite3_step(edge);} while(0)
            EDGE(import_op, "emits", manifest_atom);
            EDGE(manifest_atom, "describes", payload_real);
            EDGE(tensor_atom, "indexes", payload_real);
            EDGE(verify_op, "verifies", tensor_atom);
            if (source_model[0]) {
                char model_id[512]; snprintf(model_id, sizeof(model_id), "model:%s", source_model);
                EDGE(payload_real, "derived_from_model", model_id);
            }
            if (source_recipe[0]) {
                char recipe_id[512]; snprintf(recipe_id, sizeof(recipe_id), "recipe:%s", source_recipe);
                EDGE(recipe_id, "emits_layer", payload_real);
            }
            {
                const bf_json_node_t *arr = bf_json_obj_get(doc, root_json, "families");
                for (const bf_json_node_t *child = arr ? bf_json_child_first(doc, arr) : NULL;
                     child; child = bf_json_child_next(doc, child)) {
                    char item[256], fam_id[512];
                    if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
                    snprintf(fam_id, sizeof(fam_id), "family:%s", item);
                    EDGE(payload_real, "classified_as_family", fam_id);
                }
            }
            {
                const bf_json_node_t *arr = bf_json_obj_get(doc, root_json, "capabilities");
                for (const bf_json_node_t *child = arr ? bf_json_child_first(doc, arr) : NULL;
                     child; child = bf_json_child_next(doc, child)) {
                    char item[256], cap_id[512];
                    if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
                    snprintf(cap_id, sizeof(cap_id), "capability:%s", item);
                    EDGE(payload_real, "supports_capability", cap_id);
                }
            }
            {
                const bf_json_node_t *arr = bf_json_obj_get(doc, root_json, "workflow_steps");
                for (const bf_json_node_t *child = arr ? bf_json_child_first(doc, arr) : NULL;
                     child; child = bf_json_child_next(doc, child)) {
                    char item[256], step_id[512];
                    if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
                    snprintf(step_id, sizeof(step_id), "workflow_step:%s", item);
                    EDGE(payload_real, "attached_to_workflow_step", step_id);
                }
            }
#undef EDGE
            sqlite3_finalize(edge);
            sqlite3_exec(dst, "COMMIT", NULL, NULL, NULL);
            (void)verification_status;
            (void)materialization_status;
        }
        bf_json_free(doc);
    }
    sqlite3_finalize(st);
    sqlite3_close(src);
    sqlite3_close(dst);
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    if (src) sqlite3_close(src);
    if (dst) sqlite3_close(dst);
    return 1;
}

int bf_layer_graph_edges_json(const char *root, const char *artifact_id, char **out_json) {
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char *buf = NULL;
    size_t len = 0, cap = 0;
    int first = 1;
    char pattern[512];
    if (!artifact_id || !out_json) return 1;
    *out_json = NULL;
    if (bf_layer_state_db_path(root, "graph.db", db_path, sizeof(db_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 1;
    snprintf(pattern, sizeof(pattern), "%%%s%%", artifact_id);
    if (sqlite3_prepare_v2(db, "SELECT src_id, rel, dst_id, meta_json FROM edges WHERE src_id LIKE ? OR dst_id LIKE ? ORDER BY rel, src_id, dst_id", -1, &st, NULL) != SQLITE_OK) goto fail;
    sqlite3_bind_text(st, 1, pattern, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, pattern, -1, SQLITE_TRANSIENT);
    if (json_appendf(&buf, &len, &cap, "[") != 0) goto fail;
    while (sqlite3_step(st) == SQLITE_ROW) {
        char *qsrc = json_quote_sql((const char *)sqlite3_column_text(st, 0));
        char *qrel = json_quote_sql((const char *)sqlite3_column_text(st, 1));
        char *qdst = json_quote_sql((const char *)sqlite3_column_text(st, 2));
        const char *meta = (const char *)sqlite3_column_text(st, 3);
        if (!qsrc || !qrel || !qdst) { free(qsrc); free(qrel); free(qdst); goto fail; }
        if (json_appendf(&buf, &len, &cap, "%s{\"src_id\":%s,\"rel\":%s,\"dst_id\":%s,\"meta_json\":%s}",
            first ? "" : ",", qsrc, qrel, qdst, meta ? meta : "{}") != 0) {
            free(qsrc); free(qrel); free(qdst); goto fail;
        }
        free(qsrc); free(qrel); free(qdst);
        first = 0;
    }
    if (json_appendf(&buf, &len, &cap, "]") != 0) goto fail;
    sqlite3_finalize(st);
    sqlite3_close(db);
    *out_json = buf;
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(buf);
    return 1;
}

int bf_layer_graph_plan_json(const char *root, const char *plan_path, char **out_json) {
    char graph_path[PATH_MAX], digest[65], operator_id[256], atom_id[256], realization_id[256], plan_artifact_id[256];
    char err[128], stitch_status[64] = "unknown", plan_kind[64] = "layer_dag_plan";
    char *plan_json = NULL, *validated_json = NULL, *buf = NULL;
    sqlite3 *db = NULL;
    sqlite3_stmt *ins = NULL, *edge = NULL;
    bf_json_doc_t *rawdoc = NULL, *doc = NULL;
    const bf_json_node_t *root_json = NULL, *bridges = NULL, *equiv = NULL, *candidates = NULL;
    size_t len = 0, cap = 0;
    double confidence = 0.0;
    int valid = 0;
    const char *stage = "start";

    if (!plan_path || !out_json) return 1;
    *out_json = NULL;
    stage = "read-plan";
    plan_json = bf_read_file_full(plan_path);
    if (!plan_json) return 1;
    stage = "parse-raw-plan";
    rawdoc = bf_json_parse_str(plan_json, err, sizeof(err));
    if (!rawdoc) goto fail;
    json_copy_str(rawdoc, bf_json_root(rawdoc), "plan_kind", plan_kind, sizeof(plan_kind));
    if (strcmp(plan_kind, "bridge_resolution_plan") == 0) {
        stage = "clone-bridge-plan";
        validated_json = strdup(plan_json);
        if (!validated_json) goto fail;
        doc = rawdoc;
        rawdoc = NULL;
    } else {
        stage = "validate-stitch-plan";
        if (bf_layer_stitch_validate_json(plan_json, &validated_json) != 0 || !validated_json) goto fail;
        stage = "parse-validated-plan";
        doc = bf_json_parse_str(validated_json, err, sizeof(err));
        if (!doc) goto fail;
    }
    root_json = bf_json_root(doc);
    json_copy_str(doc, root_json, "stitch_status", stitch_status, sizeof(stitch_status));
    json_copy_str(doc, root_json, "plan_kind", plan_kind, sizeof(plan_kind));
    {
        const bf_json_node_t *conf = bf_json_obj_get(doc, root_json, "confidence");
        const bf_json_node_t *validn = bf_json_obj_get(doc, root_json, "valid");
        if (conf) confidence = bf_json_get_double(conf);
        if (validn) valid = bf_json_get_bool(validn);
    }
    bf_sha256_hex((const uint8_t *)plan_json, strlen(plan_json), digest);
    snprintf(plan_artifact_id, sizeof(plan_artifact_id), "stitch_plan:%.*s", 40, digest);
    snprintf(atom_id, sizeof(atom_id), "atom:stitch_plan_manifest:%.*s", 40, digest);
    snprintf(operator_id, sizeof(operator_id), "operator:compose_layers:%.*s", 40, digest);
    snprintf(realization_id, sizeof(realization_id), "realization:%s", plan_artifact_id);

    stage = "resolve-db-path";
    if (bf_layer_state_db_path(root, "graph.db", graph_path, sizeof(graph_path)) != 0) goto fail;
    stage = "open-graph-db";
    if (bf_sqlite3_open(graph_path, &db) != SQLITE_OK) goto fail;
    stage = "init-graph-schema";
    if (layer_init_graph_schema(db) != 0) goto fail;

    stage = "insert-atom";
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO atoms(atom_id,artifact_id,atom_kind,payload_hash,payload_json) VALUES(?,?,?,?,?)", -1, &ins, NULL) != SQLITE_OK) goto fail;
    sqlite3_bind_text(ins, 1, atom_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 2, plan_artifact_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 3, "stitch_plan_manifest", -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 4, digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 5, plan_json, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(ins) != SQLITE_DONE) goto fail;
    sqlite3_finalize(ins);
    ins = NULL;

    stage = "insert-operator";
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO operators(operator_id,artifact_id,operator_kind,payload_json) VALUES(?,?,?,?)", -1, &ins, NULL) != SQLITE_OK) goto fail;
    sqlite3_bind_text(ins, 1, operator_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 2, plan_artifact_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 3, "compose_layers", -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 4, validated_json, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(ins) != SQLITE_DONE) goto fail;
    sqlite3_finalize(ins);
    ins = NULL;

    stage = "insert-realization";
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO realizations(realization_id,artifact_id,realization_kind,payload_hash,payload_json) VALUES(?,?,?,?,?)", -1, &ins, NULL) != SQLITE_OK) goto fail;
    sqlite3_bind_text(ins, 1, realization_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 2, plan_artifact_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 3, plan_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 4, digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(ins, 5, validated_json, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(ins) != SQLITE_DONE) goto fail;
    sqlite3_finalize(ins);
    ins = NULL;

    stage = "prepare-edges";
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO edges(src_id,rel,dst_id,meta_json) VALUES(?,?,?,?)", -1, &edge, NULL) != SQLITE_OK) goto fail;
#define PLAN_EDGE(S,R,D,M) do { sqlite3_reset(edge); sqlite3_clear_bindings(edge); sqlite3_bind_text(edge,1,(S),-1,SQLITE_TRANSIENT); sqlite3_bind_text(edge,2,(R),-1,SQLITE_TRANSIENT); sqlite3_bind_text(edge,3,(D),-1,SQLITE_TRANSIENT); sqlite3_bind_text(edge,4,(M),-1,SQLITE_TRANSIENT); if (sqlite3_step(edge) != SQLITE_DONE) goto fail; } while(0)
    PLAN_EDGE(operator_id, "emits", atom_id, "{}");
    PLAN_EDGE(atom_id, "describes", realization_id, "{}");
    {
        const bf_json_node_t *components = bf_json_obj_get(doc, root_json, "components");
        if (!components) {
            const bf_json_node_t *base_plan = bf_json_obj_get(doc, root_json, "base_plan");
            if (base_plan) components = bf_json_obj_get(doc, base_plan, "components");
        }
        for (const bf_json_node_t *child = components ? bf_json_child_first(doc, components) : NULL;
             child; child = bf_json_child_next(doc, child)) {
            char component[256], comp_real[512];
            if (bf_json_get_str_copy(child, component, sizeof(component)) <= 0) continue;
            snprintf(comp_real, sizeof(comp_real), "realization:%s", component);
            PLAN_EDGE(realization_id, "depends_on", comp_real, "{}");
        }
    }
    equiv = bf_json_obj_get(doc, root_json, "equivalent_edges");
    for (const bf_json_node_t *child = equiv ? bf_json_child_first(doc, equiv) : NULL;
         child; child = bf_json_child_next(doc, child)) {
        char family[256], fam_id[512], *meta = NULL;
        if (child->type != BF_JSON_OBJECT) continue;
        json_copy_str(doc, child, "family", family, sizeof(family));
        snprintf(fam_id, sizeof(fam_id), "family:%s", family);
        meta = strdup("{\"edge_type\":\"equivalent\"}");
        if (!meta) goto fail;
        PLAN_EDGE(realization_id, "equivalent_family", fam_id, meta);
        free(meta);
    }
    bridges = bf_json_obj_get(doc, root_json, "bridge_edges");
    for (const bf_json_node_t *child = bridges ? bf_json_child_first(doc, bridges) : NULL;
         child; child = bf_json_child_next(doc, child)) {
        char sf[256] = "", tf[256] = "", rel[128] = "", dir[64] = "", req[128] = "";
        char fam_src[512], fam_dst[512], bridge_id[512];
        char *qrel = NULL, *qdir = NULL, *qreq = NULL, *meta = NULL;
        if (child->type != BF_JSON_OBJECT) continue;
        json_copy_str(doc, child, "source_family", sf, sizeof(sf));
        json_copy_str(doc, child, "target_family", tf, sizeof(tf));
        json_copy_str(doc, child, "relationship", rel, sizeof(rel));
        json_copy_str(doc, child, "directionality", dir, sizeof(dir));
        json_copy_str(doc, child, "required_bridge", req, sizeof(req));
        snprintf(fam_src, sizeof(fam_src), "family:%s", sf);
        snprintf(fam_dst, sizeof(fam_dst), "family:%s", tf);
        snprintf(bridge_id, sizeof(bridge_id), "bridge:%s", req);
        qrel = json_quote_sql(rel);
        qdir = json_quote_sql(dir);
        qreq = json_quote_sql(req);
        if (!qrel || !qdir || !qreq) {
            free(qrel); free(qdir); free(qreq);
            goto fail;
        }
        if (json_appendf(&meta, &len, &cap,
                         "{\"edge_type\":\"complementary_bridge\",\"relationship\":%s,\"directionality\":%s,\"required_bridge\":%s}",
                         qrel, qdir, qreq) != 0) {
            free(qrel); free(qdir); free(qreq);
            free(meta);
            goto fail;
        }
        free(qrel); free(qdir); free(qreq);
        PLAN_EDGE(realization_id, "requires_bridge", bridge_id, meta ? meta : "{}");
        PLAN_EDGE(fam_src, "complementary_to", fam_dst, meta ? meta : "{}");
        free(meta);
    }
    candidates = bf_json_obj_get(doc, root_json, "bridge_candidates");
    for (const bf_json_node_t *child = candidates ? bf_json_child_first(doc, candidates) : NULL;
         child; child = bf_json_child_next(doc, child)) {
        char cid[256] = "", bridge_family[256] = "";
        char *qbridge = NULL, *meta = NULL;
        if (child->type != BF_JSON_OBJECT) continue;
        json_copy_str(doc, child, "candidate_id", cid, sizeof(cid));
        json_copy_str(doc, child, "bridge_family", bridge_family, sizeof(bridge_family));
        qbridge = json_quote_sql(bridge_family);
        if (!qbridge) goto fail;
        if (json_appendf(&meta, &len, &cap,
                         "{\"edge_type\":\"bridge_candidate\",\"bridge_family\":%s}", qbridge) != 0) {
            free(qbridge);
            free(meta);
            goto fail;
        }
        free(qbridge);
        PLAN_EDGE(realization_id, "resolves_with_candidate", cid, meta ? meta : "{}");
        free(meta);
    }
#undef PLAN_EDGE
    sqlite3_finalize(edge);
    edge = NULL;
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
    db = NULL;

    if (json_appendf(&buf, &len, &cap,
                     "{\n  \"plan_artifact_id\": \"%s\",\n  \"plan_hash\": \"%s\",\n  \"plan_kind\": \"%s\",\n  \"stitch_status\": \"%s\",\n  \"confidence\": %.2f,\n  \"valid\": %s,\n  \"graph_nodes\": {\n    \"atom_id\": \"%s\",\n    \"operator_id\": \"%s\",\n    \"realization_id\": \"%s\"\n  }\n}",
                     plan_artifact_id, digest, plan_kind, stitch_status, confidence,
                     valid ? "true" : "false", atom_id, operator_id, realization_id) != 0) goto fail;
    *out_json = buf;
    free(plan_json);
    free(validated_json);
    bf_json_free(doc);
    if (rawdoc) bf_json_free(rawdoc);
    return 0;
fail:
    if (db) fprintf(stderr, "bonfyre-graph plan fail at %s: %s\n", stage, sqlite3_errmsg(db));
    else fprintf(stderr, "bonfyre-graph plan fail at %s\n", stage);
    if (edge) sqlite3_finalize(edge);
    if (ins) sqlite3_finalize(ins);
    if (db) sqlite3_close(db);
    free(plan_json);
    free(validated_json);
    if (doc) bf_json_free(doc);
    if (rawdoc) bf_json_free(rawdoc);
    free(buf);
    return 1;
}

static int layer_collect_items(const bf_json_doc_t *doc, const bf_json_node_t *arr,
                               char items[][256], int max_items) {
    int n = 0;
    for (const bf_json_node_t *child = arr ? bf_json_child_first(doc, arr) : NULL;
         child && n < max_items; child = bf_json_child_next(doc, child)) {
        if (bf_json_get_str_copy(child, items[n], sizeof(items[n])) > 0) n++;
    }
    return n;
}

typedef struct {
    const char *family_a;
    const char *family_b;
    const char *relationship_a_to_b;
    const char *relationship_b_to_a;
    const char *bridge_kind;
    const char *directionality;
    double confidence_base;
} bf_layer_family_relation_t;

static const bf_layer_family_relation_t BF_LAYER_FAMILY_RELATIONS[] = {
    { "T_MOE_ROUTER",       "T_MOE_EXPERT",     "routes_to",            "routed_by",            "T_ROUTER_EXPERT_BINDING", "directed",      0.30 },
    { "T_PROJECTOR_BRIDGE", "T_VISION_PATCH",   "projects_from",        "feeds_projector",      "projector_bridge",         "directed",      0.25 },
    { "T_PROJECTOR_BRIDGE", "T_SHARED_QK",      "projects_into",        "accepts_projector",    "projector_bridge",         "directed",      0.25 },
    { "T_EMBED_POOL",       "T_RETRIEVAL_HEAD", "pools_for",            "consumes_pool",        "T_EMBED_RETRIEVAL_BINDING","directed",      0.25 },
    { "T_POLICY_ROUTE",     "T_SAFETY_HEAD",    "routes_to",            "enforced_by",          "T_POLICY_SAFETY_BINDING",  "directed",      0.25 },
    { "T_AUDIO_MODEL",      "T_MODAL_FUSION",   "feeds_fusion",         "fuses_audio_stream",   "T_AUDIO_FUSION_BINDING",   "directed",      0.25 },
    { "T_DIFFUSION_UNET",   "T_TEXT_ENCODER",   "conditioned_by",       "conditions_unet",      "T_DIFFUSION_TEXT_BINDING", "directed",      0.25 },
    { "T_KV_CACHE",         "T_SHARED_QK",      "caches_for",           "reads_from_cache",     "cache_layout_bridge",      "directed",      0.20 },
    { "T_LATENCY_ROUTE",    "T_MOE_ROUTER",     "constrains",           "optimized_by",         "T_LATENCY_ROUTER_BINDING", "bidirectional", 0.20 },
    { "T_CLIP_IMAGE",        "T_CLIP_TEXT",       "aligned_with",        "aligned_with",        "T_CLIP_ALIGNMENT_BINDING",             "bidirectional", 0.80 },
    { "T_VIDEO_DIFFUSION",   "T_TEXT_ENCODER",    "conditioned_by",      "conditions",          "T_VIDEO_TEXT_CONDITIONING_BINDING",    "directed",      0.60 },
    { "T_DENSE_VISION",      "T_VISION_GROUNDING","grounds_features_for","grounded_by",         "T_DENSE_VISION_GROUNDING_BINDING",     "directed",      0.65 },
    { "T_SEGMENTATION_MASK", "T_VISION_GROUNDING","localizes_grounding", "localized_by",        "T_SEGMENTATION_GROUNDING_BINDING",     "directed",      0.65 },
    { "T_AUDIO_MODEL",       "T_TTS_DECODER",     "decodes_to_speech",   "decodes_from_audio",  "T_TTS_AUDIO_BINDING",                  "directed",      0.65 },
    { "T_AUDIO_MODEL",       "T_ASR_HEAD",        "transcribed_by",      "transcribes_audio",   "T_ASR_AUDIO_BINDING",                  "directed",      0.65 },
    { "T_STATE_SPACE",       "T_SEQUENCE_OUTPUT", "emits_sequence",      "emitted_by",          "T_STATE_SEQUENCE_BINDING",             "directed",      0.70 },
    { "T_LATENT_SPACE",      "T_DIFFUSION_UNET",  "denoised_by",         "denoises_latent",     "T_LATENT_DIFFUSION_BINDING",           "directed",      0.65 },
    { "T_SPATIAL_FIELD",     "T_RENDER_MODEL",    "rendered_by",         "renders_field",       "T_SPATIAL_RENDER_BINDING",             "directed",      0.70 },
    { "T_GRAPH_STRUCTURE",   "T_PLANNER",         "plans_over",          "planned_from_graph",  "T_GRAPH_PLANNER_BINDING",              "directed",      0.70 },
    { "T_AUDIO_GENERATOR", "T_SAMPLE_OUTPUT",   "samples_audio",       "sampled_from_audio_gen", "T_AUDIO_SAMPLE_BINDING",        "directed", 0.70 },
    { "T_SAMPLE_OUTPUT",   "T_LATENT_SPACE",    "projects_to_latent",  "latent_from_sample",     "T_SAMPLE_LATENT_BINDING",       "directed", 0.65 },
    { "T_DIFFUSION_UNET",  "T_VIDEO_OUTPUT",    "emits_video",         "video_from_diffusion",   "T_DIFFUSION_VIDEO_BINDING",     "directed", 0.70 },
    { "T_VISION_GROUNDING", "T_OBJECT_DETECTOR", "guides_detection", "detected_from_grounding", "T_GROUNDING_DETECTION_BINDING", "directed", 0.70 },
    /* CLIP / contrastive alignment */
    { "T_DENSE_FEATURE",      "T_RETRIEVAL_HEAD",   "feeds_retrieval",      "retrieves_from_features", "T_FEATURE_RETRIEVAL_BINDING",       "directed",      0.65 },
    { "T_ZERO_SHOT_HEAD",     "T_CLIP_IMAGE",       "scores_image",         "scored_by_head",          "T_ZERO_SHOT_IMAGE_BINDING",         "directed",      0.65 },
    { "T_ZERO_SHOT_HEAD",     "T_CLIP_TEXT",        "scores_text",          "scored_by_head",          "T_ZERO_SHOT_TEXT_BINDING",          "directed",      0.65 },

    /* Vision / grounding / geometry */
    { "T_VISION_PATCH",       "T_DENSE_VISION",     "densifies_into",       "derived_from_patches",     "T_PATCH_DENSE_VISION_BINDING",      "directed",      0.65 },
    { "T_VISION_PATCH",       "T_DEPTH_FIELD",      "estimates_depth",      "estimated_from_vision",    "T_VISION_DEPTH_BINDING",            "directed",      0.65 },
    { "T_DEPTH_FIELD",        "T_SPATIAL_FIELD",    "lifts_to_space",       "lifted_from_depth",        "T_DEPTH_SPATIAL_BINDING",           "directed",      0.70 },
    { "T_POSE",               "T_PHYSICS_MODEL",    "initializes_physics",  "conditioned_by_pose",      "T_POSE_PHYSICS_BINDING",            "directed",      0.65 },
    { "T_SCENE_GRAPH",        "T_REASONING",        "grounds_reasoning",    "reasoned_over_scene",      "T_SCENE_REASON_BINDING",            "directed",      0.70 },

    /* Audio */
    { "T_AUDIO_ENCODER",      "T_ASR_HEAD",         "feeds_asr",            "decodes_encoder",          "T_AUDIO_ASR_HEAD_BINDING",          "directed",      0.75 },
    { "T_TEXT_ENCODER",       "T_TTS_DECODER",      "conditions_speech",    "speaks_text",              "T_TEXT_TTS_BINDING",                "directed",      0.70 },
    { "T_AUDIO_MODEL",        "T_AUDIO_GENERATOR",  "conditions_audio_gen", "generates_from_audio",      "T_AUDIO_GENERATOR_BINDING",         "directed",      0.65 },
    { "T_AUDIO_MODEL",        "T_AUDIO_OUTPUT",     "emits_audio",          "emitted_by_audio_model",    "T_AUDIO_OUTPUT_BINDING",            "directed",      0.70 },

    /* Sequence / memory / routing */
    { "T_STATE_SPACE",        "T_SEQUENCE_MODEL",   "implements_sequence",  "implemented_by_state",      "T_STATE_SEQUENCE_MODEL_BINDING",    "directed",      0.70 },
    { "T_SEQUENCE_MODEL",     "T_SEQUENCE_OUTPUT",  "emits_sequence",       "emitted_by_sequence_model", "T_SEQUENCE_OUTPUT_BINDING",         "directed",      0.70 },
    { "T_STREAM_MEMORY",      "T_STATE_SPACE",      "feeds_state",          "uses_stream_memory",        "T_STREAM_STATE_BINDING",            "directed",      0.65 },
    { "T_LONG_RANGE_ATTENTION","T_STATE_SPACE",     "augments_state",       "augmented_by_attention",    "T_ATTENTION_STATE_BINDING",         "bidirectional", 0.60 },

    /* Latent / generative */
    { "T_LATENT_SPACE",       "T_VAE_DECODER",      "decoded_by",           "decodes_latent",            "T_LATENT_VAE_BINDING",              "directed",      0.75 },
    { "T_LATENT_SPACE",       "T_IMAGE_GENERATOR",  "generates_image",      "generated_from_latent",     "T_LATENT_IMAGE_BINDING",            "directed",      0.70 },
    { "T_DIFFUSION_UNET",     "T_VAE_DECODER",      "decoded_by",           "decodes_denoised_latent",   "T_DIFFUSION_VAE_BINDING",           "directed",      0.70 },
    { "T_DIFFUSION_CONTROL",  "T_DIFFUSION_UNET",   "controls_denoising",   "controlled_by_condition",   "T_CONTROL_DIFFUSION_BINDING",       "directed",      0.75 },
    { "T_CONDITIONING",       "T_DIFFUSION_UNET",   "conditions",           "conditioned_by",            "T_CONDITION_DIFFUSION_BINDING",     "directed",      0.70 },

    /* Video / temporal */
    { "T_TEMPORAL_BLOCK",     "T_VIDEO_DIFFUSION",  "feeds_video",          "uses_temporal_context",     "T_TEMPORAL_VIDEO_BINDING",          "directed",      0.65 },
    { "T_VIDEO_DIFFUSION",    "T_VIDEO_OUTPUT",     "emits_video",          "emitted_by_video_model",    "T_VIDEO_OUTPUT_BINDING",            "directed",      0.70 },
    { "T_MOTION_MODEL",       "T_PLANNER",          "predicts_for_plan",    "plans_from_motion",         "T_MOTION_PLANNER_BINDING",          "directed",      0.65 },
    { "T_TRACKING",           "T_CONTROL",          "feeds_control",        "controlled_from_tracking",  "T_TRACKING_CONTROL_BINDING",        "directed",      0.65 },

    /* Retrieval / memory */
    { "T_TOKEN_EMBED",        "T_EMBED_POOL",       "pools_tokens",         "pooled_from_tokens",        "T_TOKEN_POOL_BINDING",              "directed",      0.70 },
    { "T_EMBED_POOL",         "T_VECTOR_NORMALIZE", "normalizes_pool",      "normalizes_embeddings",     "T_EMBED_NORMALIZE_BINDING",         "directed",      0.70 },
    { "T_VECTOR_NORMALIZE",   "T_RETRIEVAL_HEAD",   "feeds_retrieval",      "retrieves_normalized",      "T_NORMALIZED_RETRIEVAL_BINDING",    "directed",      0.70 },
    { "T_LATE_INTERACTION",   "T_RETRIEVAL_HEAD",   "reranks_for",          "uses_late_interaction",     "T_LATE_INTERACTION_BINDING",        "directed",      0.75 },
    { "T_DENSE_RETRIEVAL_PROJ","T_RETRIEVAL_HEAD",  "projects_for_search",  "searches_projection",       "T_DENSE_RETRIEVAL_BINDING",         "directed",      0.75 },

    /* Graph / planning / execution */
    { "T_GRAPH_ENCODER",      "T_GRAPH_STRUCTURE",  "encodes_graph",        "encoded_by",                "T_GRAPH_ENCODER_BINDING",           "directed",      0.70 },
    { "T_GRAPH_STRUCTURE",    "T_REASONING",        "supports_reasoning",   "reasons_over_graph",        "T_GRAPH_REASONING_BINDING",         "directed",      0.70 },
    { "T_REASONING",          "T_PLANNER",          "feeds_planner",        "plans_from_reasoning",      "T_REASON_PLANNER_BINDING",          "directed",      0.70 },
    { "T_PLANNER",            "T_EXECUTION",        "emits_execution",      "executes_plan",             "T_PLAN_EXECUTION_BINDING",          "directed",      0.75 },
    { "T_CONSTRAINT_SOLVER",  "T_PLANNER",          "constrains_plan",      "planned_under_constraints", "T_CONSTRAINT_PLANNER_BINDING",       "directed",      0.75 },

    /* Biology / scientific */
    { "T_PROTEIN_MODEL",      "T_STRUCTURE_PREDICTOR","predicts_structure", "uses_protein_model",        "T_PROTEIN_STRUCTURE_BINDING",       "directed",      0.75 },
    { "T_STRUCTURE_PREDICTOR","T_STRUCTURE_OUTPUT", "emits_structure",      "emitted_by_predictor",      "T_STRUCTURE_OUTPUT_BINDING",        "directed",      0.75 },
    { "T_GENOME_ENCODER",     "T_GENOME_UNET",      "conditions_genome",    "uses_genome_encoding",      "T_GENOME_CONDITIONING_BINDING",     "directed",      0.65 },
    { "T_GENOME_UNET",        "T_EXPRESSION_HEAD",  "predicts_expression",  "reads_genome_unet",         "T_GENOME_EXPRESSION_BINDING",       "directed",      0.65 },

    /* Systems/runtime */
    { "T_SYSTEM_MODEL",       "T_OPTIMIZER",        "optimizes_system",     "optimized_by_system_model", "T_SYSTEM_OPTIMIZER_BINDING",        "directed",      0.70 },
    { "T_OPTIMIZER",          "T_EXECUTION_PLAN",   "emits_plan",           "planned_by_optimizer",      "T_OPTIMIZER_EXECUTION_BINDING",     "directed",      0.70 },
    { "T_LATENCY_ROUTE",      "T_PLANNER",          "constrains_plan",      "planned_for_latency",       "T_LATENCY_PLANNER_BINDING",         "directed",      0.65 },
    { "T_LOWBIT_MATRIX",      "T_LAYER_PACK",       "packs_into",           "contains_lowbit_matrix",    "T_LOWBIT_PACK_BINDING",             "directed",      0.65 },
        /* Multimodal / projector / language */
    { "T_VISION_PATCH",       "T_PROJECTOR_BRIDGE", "feeds_projector",      "projects_from_vision",     "T_VISION_PROJECTOR_BINDING",        "directed",      0.70 },
    { "T_PROJECTOR_BRIDGE",   "T_TEXT_ENCODER",     "projects_to_text",     "accepts_projected_tokens", "T_PROJECTOR_TEXT_BINDING",          "directed",      0.70 },
    { "T_IMAGE_TOKEN_INSERT", "T_TEXT_ENCODER",     "inserts_image_tokens", "accepts_image_tokens",     "T_IMAGE_TOKEN_TEXT_BINDING",        "directed",      0.70 },
    { "T_TOKENIZER_ALIGN",    "T_TOKEN_EMBED",      "aligns_token_space",   "aligned_by_tokenizer",     "T_TOKENIZER_EMBED_BINDING",         "directed",      0.70 },
    { "T_SHARED_EMBED",       "T_TOKEN_EMBED",      "shares_embedding",     "shares_with",              "T_SHARED_EMBED_BINDING",            "bidirectional", 0.75 },

    /* Transformer internals */
    { "T_Q_PROJ",             "T_K_PROJ",           "pairs_attention",      "pairs_attention",          "T_QK_ATTENTION_BINDING",            "bidirectional", 0.80 },
    { "T_Q_PROJ",             "T_V_PROJ",           "attends_to_values",    "attended_by_query",        "T_QV_ATTENTION_BINDING",            "directed",      0.70 },
    { "T_K_PROJ",             "T_V_PROJ",           "indexes_values",       "indexed_by_keys",          "T_KV_ATTENTION_BINDING",            "directed",      0.70 },
    { "T_O_PROJ",             "T_SHARED_QK",        "projects_attention",   "receives_attention_out",   "T_ATTENTION_OUTPUT_BINDING",        "directed",      0.70 },
    { "T_QK_ROTARY",          "T_SHARED_QK",        "rotates_qk",           "uses_rotary_qk",           "T_ROTARY_QK_BINDING",               "directed",      0.75 },
    { "T_LOCAL_ATTENTION",    "T_SHARED_QK",        "windows_attention",    "windowed_by_local_attn",   "T_LOCAL_ATTENTION_BINDING",         "directed",      0.65 },
    { "T_RESIDUAL_STREAM",    "T_PARALLEL_RESIDUAL","feeds_residual",       "uses_residual_stream",     "T_RESIDUAL_PARALLEL_BINDING",       "directed",      0.70 },

    /* MLP / FFN / MoE */
    { "T_MLP_GATE",           "T_MLP_UP",           "gates_up_projection",  "gated_by",                 "T_MLP_GATE_UP_BINDING",             "directed",      0.75 },
    { "T_MLP_UP",             "T_MLP_DOWN",         "projects_down",        "receives_up_projection",   "T_MLP_UP_DOWN_BINDING",             "directed",      0.75 },
    { "T_EXPERT_FFN_GATE",    "T_EXPERT_FFN_UP",    "gates_expert",         "gated_by_expert_gate",     "T_EXPERT_GATE_UP_BINDING",          "directed",      0.75 },
    { "T_EXPERT_FFN_UP",      "T_EXPERT_FFN_DOWN",  "projects_expert_down", "receives_expert_up",       "T_EXPERT_UP_DOWN_BINDING",          "directed",      0.75 },
    { "T_MOE_TOPK_SELECT",    "T_MOE_EXPERT",       "selects_expert",       "selected_by_topk",         "T_TOPK_EXPERT_BINDING",             "directed",      0.80 },
    { "T_MOE_ROUTER",         "T_MOE_TOPK_SELECT",  "scores_topk",          "selected_from_router",     "T_ROUTER_TOPK_BINDING",             "directed",      0.80 },

    /* Compression / packing / runtime */
    { "T_BLOCKWISE_LAYOUT",   "T_LOWBIT_MATRIX",    "layouts_blocks",       "packed_by_layout",         "T_BLOCKWISE_LOWBIT_BINDING",        "directed",      0.70 },
    { "T_SCALE_OFFSET",       "T_LOWBIT_MATRIX",    "dequantizes_matrix",   "uses_scale_offset",        "T_SCALE_LOWBIT_BINDING",            "directed",      0.75 },
    { "T_LAYER_PACK",         "T_LOWBIT_MATRIX",    "contains_lowbit",      "packed_in_layer",          "T_PACK_LOWBIT_BINDING",             "directed",      0.65 },
    { "T_LAYER_PACK",         "T_TOKEN_EMBED",      "contains_embedding",   "packed_in_layer",          "T_PACK_EMBED_BINDING",              "directed",      0.60 },
    { "T_LAYER_PACK",         "T_TEXT_ENCODER",     "contains_encoder",     "packed_in_layer",          "T_PACK_ENCODER_BINDING",            "directed",      0.60 },

    /* Policy / safety / routing */
    { "T_POLICY_CLASSIFIER",  "T_POLICY_ROUTE",     "classifies_policy",    "routes_policy_result",     "T_POLICY_CLASSIFIER_ROUTE_BINDING", "directed",      0.75 },
    { "T_SCORE_ROUTE",        "T_POLICY_ROUTE",     "scores_route",         "routes_from_score",        "T_SCORE_POLICY_ROUTE_BINDING",      "directed",      0.65 },
    { "T_SCORE_ROUTE",        "T_LATENCY_ROUTE",    "balances_latency",     "latency_scored_by",        "T_SCORE_LATENCY_BINDING",           "directed",      0.65 },
    { "T_EDGE_ROUTE",         "T_LATENCY_ROUTE",    "routes_edge",          "optimized_for_edge",       "T_EDGE_LATENCY_BINDING",            "directed",      0.70 },

    /* Function/tool calling */
    { "T_FUNCTION_SCHEMA",    "T_FUNCTION_CALL_HEAD","defines_tool_call",   "calls_defined_schema",     "T_SCHEMA_CALL_BINDING",             "directed",      0.80 },
    { "T_FUNCTION_CALL_HEAD", "T_STRUCTURED_OUTPUT_HEAD","emits_structured","structured_from_call",      "T_CALL_STRUCTURED_BINDING",         "directed",      0.75 },
    { "T_REASONING",          "T_FUNCTION_CALL_HEAD","chooses_tool",        "tool_chosen_by_reasoning", "T_REASON_TOOL_BINDING",             "directed",      0.70 },

    /* Audio / speech refinement */
    { "T_AUDIO_ENCODER",      "T_MODAL_FUSION",     "feeds_fusion",         "fuses_audio_encoder",      "T_AUDIO_ENCODER_FUSION_BINDING",    "directed",      0.75 },
    { "T_ASR_HEAD",           "T_TEXT_ENCODER",     "emits_text_tokens",    "encodes_asr_text",         "T_ASR_TEXT_BINDING",                "directed",      0.70 },
    { "T_TTS_ENCODER",        "T_TTS_DECODER",      "conditions_decoder",   "decoded_from_tts_encoder", "T_TTS_ENCODER_DECODER_BINDING",     "directed",      0.80 },
    { "T_TTS_DECODER",        "T_AUDIO_OUTPUT",     "emits_speech",         "speech_from_decoder",      "T_TTS_OUTPUT_BINDING",              "directed",      0.80 },
    { "T_SPEAKER_EMBED",      "T_TTS_DECODER",      "conditions_voice",     "voice_conditioned_by",     "T_SPEAKER_TTS_BINDING",             "directed",      0.75 },
    { "T_AUDIO_MODEL",        "T_SPEAKER_EMBED",    "extracts_speaker",     "speaker_from_audio",       "T_AUDIO_SPEAKER_BINDING",           "directed",      0.70 },

    /* Vision / OCR / document */
    { "T_VISION_PATCH",       "T_OCR_HEAD",         "feeds_ocr",            "reads_vision_patches",     "T_VISION_OCR_BINDING",              "directed",      0.70 },
    { "T_OCR_HEAD",           "T_TEXT_ENCODER",     "emits_text",           "encodes_ocr_text",         "T_OCR_TEXT_BINDING",                "directed",      0.70 },
    { "T_VISION_PATCH",       "T_OBJECT_DETECTOR",  "feeds_detection",      "detects_from_patches",     "T_VISION_DETECTION_BINDING",        "directed",      0.70 },
    { "T_OBJECT_DETECTOR",    "T_SCENE_GRAPH",      "builds_scene_graph",   "built_from_objects",       "T_OBJECT_SCENE_BINDING",            "directed",      0.75 },
    { "T_SEGMENTATION_MASK",  "T_OBJECT_DETECTOR",  "refines_objects",      "refined_by_masks",         "T_MASK_OBJECT_BINDING",             "directed",      0.65 },

    /* 3D / spatial / geometry */
    { "T_DEPTH_FIELD",        "T_RENDER_MODEL",     "conditions_render",    "renders_depth",            "T_DEPTH_RENDER_BINDING",            "directed",      0.70 },
    { "T_SPATIAL_FIELD",      "T_MESH_GENERATOR",   "meshes_field",         "generated_from_field",     "T_FIELD_MESH_BINDING",              "directed",      0.75 },
    { "T_MESH_GENERATOR",     "T_RENDER_MODEL",     "feeds_renderer",       "renders_mesh",             "T_MESH_RENDER_BINDING",             "directed",      0.75 },
    { "T_POINT_CLOUD",        "T_SPATIAL_FIELD",    "samples_space",        "field_from_points",        "T_POINT_FIELD_BINDING",             "directed",      0.70 },
    { "T_POSE",               "T_RENDER_MODEL",     "conditions_render",    "rendered_with_pose",       "T_POSE_RENDER_BINDING",             "directed",      0.65 },

    /* Diffusion / image generation */
    { "T_TEXT_ENCODER",       "T_DIFFUSION_UNET",   "conditions_unet",      "conditioned_by_text",      "T_TEXT_DIFFUSION_BINDING",          "directed",      0.75 },
    { "T_IMAGE_GENERATOR",    "T_IMAGE_OUTPUT",     "emits_image",          "image_from_generator",     "T_IMAGE_GENERATOR_OUTPUT_BINDING",  "directed",      0.75 },
    { "T_VAE_DECODER",        "T_IMAGE_OUTPUT",     "decodes_image",        "image_from_vae",           "T_VAE_IMAGE_BINDING",               "directed",      0.75 },
    { "T_LATENT_SPACE",       "T_DISCRETE_LATENT",  "quantizes_latent",     "discretized_from_latent",  "T_LATENT_DISCRETE_BINDING",         "directed",      0.70 },
    { "T_DISCRETE_LATENT",    "T_IMAGE_GENERATOR",  "generates_from_codes", "uses_discrete_codes",      "T_DISCRETE_IMAGE_BINDING",          "directed",      0.70 },
    { "T_GENERATOR",          "T_IMAGE_OUTPUT",     "emits_image",          "generated_image",          "T_GENERATOR_IMAGE_BINDING",         "directed",      0.70 },
    { "T_DISCRIMINATOR",      "T_GENERATOR",        "trains_generator",     "trained_against",          "T_GAN_TRAINING_BINDING",            "bidirectional", 0.60 },

    /* Sampling / density / energy */
    { "T_DENSITY_MODEL",      "T_SAMPLE_OUTPUT",    "samples_density",      "sampled_from_density",     "T_DENSITY_SAMPLE_BINDING",          "directed",      0.70 },
    { "T_ENERGY_MODEL",       "T_SAMPLER",          "scores_samples",       "samples_from_energy",      "T_ENERGY_SAMPLER_BINDING",          "directed",      0.70 },
    { "T_SAMPLER",            "T_SAMPLE_OUTPUT",    "emits_sample",         "sample_from_sampler",      "T_SAMPLER_OUTPUT_BINDING",          "directed",      0.70 },
    { "T_SCORER",             "T_SELECTION",        "scores_selection",     "selects_by_score",         "T_SCORER_SELECTION_BINDING",        "directed",      0.70 },

    /* Data / tabular / forecasting */
    { "T_TABULAR_MODEL",      "T_FEATURE_ENCODER",  "encodes_features",     "features_from_tabular",    "T_TABULAR_FEATURE_BINDING",         "directed",      0.70 },
    { "T_FEATURE_ENCODER",    "T_PREDICTION_HEAD",  "feeds_prediction",     "predicts_from_features",   "T_FEATURE_PREDICTION_BINDING",      "directed",      0.70 },
    { "T_TIME_SERIES",        "T_PREDICTOR",        "feeds_forecast",       "predicts_timeseries",      "T_TIMESERIES_PREDICTOR_BINDING",    "directed",      0.70 },
    { "T_ANOMALY_MODEL",      "T_ALERT",            "raises_alert",         "alert_from_anomaly",       "T_ANOMALY_ALERT_BINDING",           "directed",      0.70 },
    { "T_CAUSAL_MODEL",       "T_DECISION",         "supports_decision",    "decision_from_causality",  "T_CAUSAL_DECISION_BINDING",         "directed",      0.70 },
    { "T_DECISION",           "T_POLICY",           "emits_policy",         "policy_from_decision",     "T_DECISION_POLICY_BINDING",         "directed",      0.70 },

    /* Graph / knowledge / memory */
    { "T_KNOWLEDGE_GRAPH",    "T_GRAPH_STRUCTURE",  "materializes_graph",   "graph_from_kg",            "T_KG_GRAPH_BINDING",                "directed",      0.75 },
    { "T_GRAPH_STRUCTURE",    "T_EMBED_POOL",       "embeds_graph",         "graph_embedding_pool",     "T_GRAPH_EMBED_BINDING",             "directed",      0.70 },
    { "T_GRAPH_STRUCTURE",    "T_RETRIEVAL_HEAD",   "retrieves_graph",      "retrieval_over_graph",     "T_GRAPH_RETRIEVAL_BINDING",         "directed",      0.70 },
    { "T_RETRIEVAL_HEAD",     "T_REASONING",        "grounds_reasoning",    "reasoning_from_retrieval", "T_RETRIEVAL_REASON_BINDING",        "directed",      0.75 },
    { "T_MEMORY_STORE",       "T_RETRIEVAL_HEAD",   "serves_retrieval",     "retrieves_from_memory",    "T_MEMORY_RETRIEVAL_BINDING",        "directed",      0.75 },

    /* Execution / agents / tools */
    { "T_EXECUTION_PLAN",     "T_EXECUTION",        "executes_plan",        "execution_from_plan",      "T_EXECUTION_PLAN_BINDING",          "directed",      0.75 },
    { "T_ACTION",             "T_EXECUTION",        "executes_action",      "execution_from_action",    "T_ACTION_EXECUTION_BINDING",        "directed",      0.75 },
    { "T_ALERT",              "T_RESPONSE",         "triggers_response",    "response_from_alert",      "T_ALERT_RESPONSE_BINDING",          "directed",      0.70 },
    { "T_CONTROL",            "T_EXECUTION",        "controls_execution",   "execution_controlled_by",  "T_CONTROL_EXECUTION_BINDING",       "directed",      0.70 },
    { "T_PLANNER",            "T_ACTION",           "plans_action",         "action_from_plan",         "T_PLANNER_ACTION_BINDING",          "directed",      0.75 },

    /* Scientific / medical / genomics */
    { "T_VISION_MED",         "T_UNCERTAINTY_CALIB","calibrates_medical",   "medical_uncertainty_from", "T_MED_UNCERTAINTY_BINDING",         "directed",      0.70 },
    { "T_UNCERTAINTY_CALIB",  "T_PREDICT_HEAD",     "calibrates_prediction","prediction_calibrated_by", "T_UNCERTAINTY_PREDICT_BINDING",      "directed",      0.70 },
    { "T_PROTEIN_MODEL",      "T_EMBED_POOL",       "embeds_protein",       "protein_embedding_pool",   "T_PROTEIN_EMBED_BINDING",           "directed",      0.70 },
    { "T_STRUCTURE_OUTPUT",   "T_SPATIAL_FIELD",    "maps_to_spatial",      "spatial_from_structure",   "T_STRUCTURE_SPATIAL_BINDING",       "directed",      0.65 },
    { "T_GENOME_ENCODER",     "T_EXPRESSION_HEAD",  "feeds_expression",     "expression_from_genome",   "T_GENOME_EXPRESSION_HEAD_BINDING",  "directed",      0.70 },
    { "T_EXPRESSION_HEAD",    "T_PREDICT_HEAD",     "feeds_prediction",     "predicts_expression",      "T_EXPRESSION_PREDICT_BINDING",      "directed",      0.65 },

    /* Modal fusion */
    { "T_VISION_PATCH",       "T_MODAL_FUSION",     "feeds_fusion",         "fuses_vision",             "T_VISION_FUSION_BINDING",           "directed",      0.70 },
    { "T_TEXT_ENCODER",       "T_MODAL_FUSION",     "feeds_fusion",         "fuses_text",               "T_TEXT_FUSION_BINDING",             "directed",      0.70 },
    { "T_MODAL_FUSION",       "T_REASONING",        "feeds_reasoning",      "reasoning_from_fusion",    "T_FUSION_REASON_BINDING",           "directed",      0.70 },
    { "T_MODAL_FUSION",       "T_GENERATION",       "feeds_generation",     "generation_from_fusion",   "T_FUSION_GENERATION_BINDING",       "directed",      0.70 },
    { "T_MULTIMODAL",         "T_MODAL_FUSION",     "implements_fusion",    "implemented_by_multimodal", "T_MULTIMODAL_FUSION_BINDING",       "directed",      0.70 },

    /* Output heads */
    { "T_TEXT_ENCODER",       "T_LM_HEAD",          "feeds_lm_head",        "decoded_from_encoder",     "T_TEXT_LM_HEAD_BINDING",            "directed",      0.75 },
    { "T_LM_HEAD",            "T_TEXT_OUTPUT",      "emits_text",           "text_from_lm_head",        "T_LM_TEXT_OUTPUT_BINDING",          "directed",      0.75 },
    { "T_PREDICTION_HEAD",    "T_OUTPUT",           "emits_output",         "output_from_prediction",   "T_PREDICTION_OUTPUT_BINDING",       "directed",      0.70 },
    { "T_STRUCTURED_OUTPUT_HEAD","T_OUTPUT",        "emits_structured",     "output_from_structured",   "T_STRUCTURED_OUTPUT_BINDING",       "directed",      0.70 },
    { "T_RETRIEVAL_HEAD",     "T_OUTPUT",           "emits_results",        "output_from_retrieval",    "T_RETRIEVAL_OUTPUT_BINDING",        "directed",      0.70 },
    { NULL, NULL, NULL, NULL, NULL, NULL, 0.0 }
};

int bf_layer_family_relations_json(const char *family_filter, char **out_json) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    int count = 0;
    char *qfilter = NULL;
    if (!out_json) return 1;
    *out_json = NULL;
    qfilter = json_quote_sql(family_filter ? family_filter : "");
    if (!qfilter) return 1;
    if (json_appendf(&buf, &len, &cap, "{\n  \"family_filter\": %s,\n  \"relations\": [", qfilter) != 0) {
        free(qfilter);
        free(buf);
        return 1;
    }
    free(qfilter);
    for (int i = 0; BF_LAYER_FAMILY_RELATIONS[i].family_a; i++) {
        const bf_layer_family_relation_t *rel = &BF_LAYER_FAMILY_RELATIONS[i];
        char *qa = NULL, *qb = NULL, *qrab = NULL, *qrba = NULL, *qbridge = NULL, *qdir = NULL;
        if (family_filter && family_filter[0] &&
            strcmp(family_filter, rel->family_a) != 0 &&
            strcmp(family_filter, rel->family_b) != 0 &&
            strcmp(family_filter, rel->bridge_kind) != 0) {
            continue;
        }
        qa = json_quote_sql(rel->family_a);
        qb = json_quote_sql(rel->family_b);
        qrab = json_quote_sql(rel->relationship_a_to_b);
        qrba = json_quote_sql(rel->relationship_b_to_a);
        qbridge = json_quote_sql(rel->bridge_kind);
        qdir = json_quote_sql(rel->directionality);
        if (!qa || !qb || !qrab || !qrba || !qbridge || !qdir) {
            free(qa); free(qb); free(qrab); free(qrba); free(qbridge); free(qdir); free(buf);
            return 1;
        }
        if (json_appendf(&buf, &len, &cap,
                         "%s\n    {\"family_a\":%s,\"family_b\":%s,\"relationship_a_to_b\":%s,"
                         "\"relationship_b_to_a\":%s,\"required_bridge\":%s,\"directionality\":%s,\"confidence_base\":%.2f}",
                         count ? "," : "", qa, qb, qrab, qrba, qbridge, qdir, rel->confidence_base) != 0) {
            free(qa); free(qb); free(qrab); free(qrba); free(qbridge); free(qdir); free(buf);
            return 1;
        }
        free(qa); free(qb); free(qrab); free(qrba); free(qbridge); free(qdir);
        count++;
    }
    if (json_appendf(&buf, &len, &cap, "\n  ],\n  \"count\": %d\n}\n", count) != 0) {
        free(buf);
        return 1;
    }
    *out_json = buf;
    return 0;
}

int bf_layer_bridge_query_json(const char *root, const char *bridge_family, char **out_json) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    int first = 1;
    (void)root;
    if (!out_json) return 1;
    *out_json = NULL;
    if (json_appendf(&buf, &len, &cap, "[") != 0) goto fail;
    for (int i = 0; BF_LAYER_FAMILY_RELATIONS[i].family_a; i++) {
        const bf_layer_family_relation_t *rel = &BF_LAYER_FAMILY_RELATIONS[i];
        char *qbridge = NULL, *qa = NULL, *qb = NULL, *qrel = NULL, *qdir = NULL;
        if (bridge_family && bridge_family[0] && strcmp(bridge_family, rel->bridge_kind) != 0) continue;
        qbridge = json_quote_sql(rel->bridge_kind);
        qa = json_quote_sql(rel->family_a);
        qb = json_quote_sql(rel->family_b);
        qrel = json_quote_sql(rel->relationship_a_to_b);
        qdir = json_quote_sql(rel->directionality);
        if (!qbridge || !qa || !qb || !qrel || !qdir) {
            free(qbridge); free(qa); free(qb); free(qrel); free(qdir);
            goto fail;
        }
        if (json_appendf(&buf, &len, &cap,
                         "%s{\"bridge_family\":%s,\"artifact_kind\":\"adapter_bridge\",\"source_family\":%s,\"target_family\":%s,\"relationship\":%s,\"directionality\":%s,\"confidence_base\":%.2f,\"resolution_mode\":\"metadata-first\"}",
                         first ? "" : ",", qbridge, qa, qb, qrel, qdir, rel->confidence_base) != 0) {
            free(qbridge); free(qa); free(qb); free(qrel); free(qdir);
            goto fail;
        }
        free(qbridge); free(qa); free(qb); free(qrel); free(qdir);
        first = 0;
    }
    if (json_appendf(&buf, &len, &cap, "]") != 0) goto fail;
    *out_json = buf;
    return 0;
fail:
    free(buf);
    return 1;
}

int bf_layer_compat_json(const char *root, const char *layer_a, const char *layer_b, char **out_json) {
    char *json_a = NULL, *json_b = NULL, *buf = NULL;
    char err[128];
    bf_json_doc_t *doc_a = NULL, *doc_b = NULL;
    const bf_json_node_t *root_a, *root_b;
    char fam_a[32][256], fam_b[32][256], steps_a[32][256], steps_b[32][256], fmt_a[16][256], fmt_b[16][256];
    int nfam_a, nfam_b, nsteps_a, nsteps_b, nfmt_a, nfmt_b;
    char verification_a[64] = "", verification_b[64] = "", kind_a[128] = "", kind_b[128] = "", src_coll_a[256] = "", src_coll_b[256] = "";
    char *match_json = NULL, *complement_json = NULL, *mismatch_json = NULL, *bridge_json = NULL, *reason_json = NULL;
    size_t l1 = 0, c1 = 0, lc = 0, cc = 0, l2 = 0, c2 = 0, l3 = 0, c3 = 0, l4 = 0, c4 = 0;
    double confidence = 0.2;
    int has_matching = 0, has_complementary = 0, has_bridge = 0, have_shape_a = 0, have_shape_b = 0, have_dtype_a = 0, have_dtype_b = 0;
    const char *status = "incompatible";
    if (!layer_a || !layer_b || !out_json) return 1;
    *out_json = NULL;
    if (bf_layer_load_json(root, layer_a, &json_a) != 0) return 1;
    if (bf_layer_load_json(root, layer_b, &json_b) != 0) { free(json_a); return 1; }
    doc_a = bf_json_parse_str(json_a, err, sizeof(err));
    doc_b = bf_json_parse_str(json_b, err, sizeof(err));
    if (!doc_a || !doc_b) goto fail;
    root_a = bf_json_root(doc_a);
    root_b = bf_json_root(doc_b);
    nfam_a = layer_collect_items(doc_a, bf_json_obj_get(doc_a, root_a, "families"), fam_a, 32);
    nfam_b = layer_collect_items(doc_b, bf_json_obj_get(doc_b, root_b, "families"), fam_b, 32);
    nsteps_a = layer_collect_items(doc_a, bf_json_obj_get(doc_a, root_a, "workflow_steps"), steps_a, 32);
    nsteps_b = layer_collect_items(doc_b, bf_json_obj_get(doc_b, root_b, "workflow_steps"), steps_b, 32);
    {
        const bf_json_node_t *rr_a = bf_json_obj_get(doc_a, root_a, "runtime_requirements");
        const bf_json_node_t *rr_b = bf_json_obj_get(doc_b, root_b, "runtime_requirements");
        nfmt_a = layer_collect_items(doc_a, rr_a ? bf_json_obj_get(doc_a, rr_a, "formats") : NULL, fmt_a, 16);
        nfmt_b = layer_collect_items(doc_b, rr_b ? bf_json_obj_get(doc_b, rr_b, "formats") : NULL, fmt_b, 16);
    }
    json_copy_str(doc_a, root_a, "verification_status", verification_a, sizeof(verification_a));
    json_copy_str(doc_b, root_b, "verification_status", verification_b, sizeof(verification_b));
    json_copy_str(doc_a, root_a, "artifact_kind", kind_a, sizeof(kind_a));
    json_copy_str(doc_b, root_b, "artifact_kind", kind_b, sizeof(kind_b));
    json_copy_str(doc_a, root_a, "source_collection", src_coll_a, sizeof(src_coll_a));
    json_copy_str(doc_b, root_b, "source_collection", src_coll_b, sizeof(src_coll_b));
    have_shape_a = bf_json_obj_get(doc_a, root_a, "shape_constraints") && bf_json_child_first(doc_a, bf_json_obj_get(doc_a, root_a, "shape_constraints"));
    have_shape_b = bf_json_obj_get(doc_b, root_b, "shape_constraints") && bf_json_child_first(doc_b, bf_json_obj_get(doc_b, root_b, "shape_constraints"));
    have_dtype_a = bf_json_obj_get(doc_a, root_a, "dtype_constraints") && bf_json_child_first(doc_a, bf_json_obj_get(doc_a, root_a, "dtype_constraints"));
    have_dtype_b = bf_json_obj_get(doc_b, root_b, "dtype_constraints") && bf_json_child_first(doc_b, bf_json_obj_get(doc_b, root_b, "dtype_constraints"));
    json_appendf(&match_json, &l1, &c1, "[");
    json_appendf(&complement_json, &lc, &cc, "[");
    json_appendf(&mismatch_json, &l2, &c2, "[");
    json_appendf(&bridge_json, &l3, &c3, "[");
    json_appendf(&reason_json, &l4, &c4, "[");
    for (int i = 0, first = 1; i < nfam_a; i++) {
        for (int j = 0; j < nfam_b; j++) {
            if (strcmp(fam_a[i], fam_b[j]) == 0) {
                char *q = json_quote_sql(fam_a[i]);
                if (!q) goto fail;
                json_appendf(&match_json, &l1, &c1, "%s%s", first ? "" : ",", q);
                free(q);
                first = 0;
                has_matching = 1;
            }
        }
    }
    json_appendf(&match_json, &l1, &c1, "]");
    {
        int first_comp = 1;
        for (int i = 0; i < nfam_a; i++) {
            for (int j = 0; j < nfam_b; j++) {
                for (int r = 0; BF_LAYER_FAMILY_RELATIONS[r].family_a; r++) {
                    const bf_layer_family_relation_t *rel = &BF_LAYER_FAMILY_RELATIONS[r];
                    int forward = strcmp(fam_a[i], rel->family_a) == 0 && strcmp(fam_b[j], rel->family_b) == 0;
                    int reverse = strcmp(fam_a[i], rel->family_b) == 0 && strcmp(fam_b[j], rel->family_a) == 0;
                    if (!forward && !reverse) continue;
                    {
                        char *qa = json_quote_sql(fam_a[i]);
                        char *qb = json_quote_sql(fam_b[j]);
                        const char *relationship = forward ? rel->relationship_a_to_b : rel->relationship_b_to_a;
                        char *qr = json_quote_sql(relationship);
                        char *qbri = json_quote_sql(rel->bridge_kind);
                        char *qdir = json_quote_sql(rel->directionality);
                        if (!qa || !qb || !qr || !qbri || !qdir) {
                            free(qa); free(qb); free(qr); free(qbri); free(qdir);
                            goto fail;
                        }
                        json_appendf(&complement_json, &lc, &cc,
                                     "%s[%s,%s,{\"relationship\":%s,\"directionality\":%s,\"required_bridge\":%s}]",
                                     first_comp ? "" : ",", qa, qb, qr, qdir, qbri);
                        free(qa); free(qb); free(qr); free(qdir);
                        if (!has_bridge || strstr(bridge_json ? bridge_json : "", rel->bridge_kind) == NULL) {
                            json_appendf(&bridge_json, &l3, &c3, "%s%s", l3 > 1 ? "," : "", qbri);
                            has_bridge = 1;
                        }
                        free(qbri);
                        has_complementary = 1;
                        confidence += rel->confidence_base;
                        {
                            char reason[256];
                            snprintf(reason, sizeof(reason), "complementary family relationship: %s", relationship);
                            {
                                char *qreason = json_quote_sql(reason);
                                if (!qreason) goto fail;
                                json_appendf(&reason_json, &l4, &c4, "%s%s", l4 > 1 ? "," : "", qreason);
                                free(qreason);
                            }
                        }
                        first_comp = 0;
                    }
                }
            }
        }
    }
    json_appendf(&complement_json, &lc, &cc, "]");
    if (has_matching) {
        confidence += 0.35;
        json_appendf(&reason_json, &l4, &c4, "%s\"matching T_* families\"", l4 > 1 ? "," : "");
    } else if (!has_complementary) {
        json_appendf(&mismatch_json, &l2, &c2, "%s\"no matching T_* family\"", l2 > 1 ? "," : "");
    }
    {
        int overlap = 0;
        for (int i = 0; i < nsteps_a && !overlap; i++) {
            for (int j = 0; j < nsteps_b; j++) {
                if (strcmp(steps_a[i], steps_b[j]) == 0) { overlap = 1; break; }
            }
        }
        if (overlap) {
            confidence += 0.15;
            json_appendf(&reason_json, &l4, &c4, "%s\"workflow step compatibility\"", l4 > 1 ? "," : "");
        } else {
            json_appendf(&mismatch_json, &l2, &c2, "%s\"workflow step is compatible only by semantic family, not exact overlap\"", l2 > 1 ? "," : "");
        }
    }
    if (have_shape_a && have_shape_b) confidence += 0.1;
    else json_appendf(&mismatch_json, &l2, &c2, "%s\"shape constraints unknown\"", l2 > 1 ? "," : "");
    if (have_dtype_a && have_dtype_b) confidence += 0.1;
    else json_appendf(&mismatch_json, &l2, &c2, "%s\"dtype constraints unknown\"", l2 > 1 ? "," : "");
    if (strncmp(kind_a, "hf_", 3) == 0 && strcmp(kind_b, "onnx_slice") == 0 && strcmp(verification_a, "verified") != 0) {
        json_appendf(&mismatch_json, &l2, &c2, "%s\"HF tensor names not verified for ONNX binding\"", l2 > 1 ? "," : "");
    }
    if (strncmp(kind_b, "hf_", 3) == 0 && strcmp(kind_a, "onnx_slice") == 0 && strcmp(verification_b, "verified") != 0) {
        json_appendf(&mismatch_json, &l2, &c2, "%s\"HF tensor names not verified for ONNX binding\"", l2 > 1 ? "," : "");
    }
    {
        int formats_equal = (nfmt_a == nfmt_b);
        if (formats_equal) {
            for (int i = 0; i < nfmt_a; i++) {
                if (strcmp(fmt_a[i], fmt_b[i]) != 0) { formats_equal = 0; break; }
            }
        }
        if (!formats_equal) {
            int fmt_has_onnx = 0, coll_has_quant = 0, need_projector = 0, need_cache = 0;
            for (int i = 0; i < nfmt_a; i++) if (strcmp(fmt_a[i], "onnx") == 0) fmt_has_onnx = 1;
            for (int i = 0; i < nfmt_b; i++) if (strcmp(fmt_b[i], "onnx") == 0) fmt_has_onnx = 1;
            if (fmt_has_onnx && (strncmp(kind_a, "hf_", 3) == 0 || strncmp(kind_b, "hf_", 3) == 0)) {
                const char *bridge = strncmp(kind_a, "hf_", 3) == 0 ? "hf_to_onnx_adapter" : "onnx_to_hf_adapter";
                char *q = json_quote_sql(bridge); if (!q) goto fail;
                json_appendf(&bridge_json, &l3, &c3, "%s%s", l3 > 1 ? "," : "", q); free(q); has_bridge = 1;
            }
            if (strstr(src_coll_a, "quant") || strstr(src_coll_b, "quant")) coll_has_quant = 1;
            for (int i = 0; i < nfam_a; i++) if (strcmp(fam_a[i], "T_PROJECTOR_BRIDGE") == 0) need_projector = 1;
            for (int i = 0; i < nfam_b; i++) if (strcmp(fam_b[i], "T_PROJECTOR_BRIDGE") == 0) need_projector = 1;
            for (int i = 0; i < nfam_a; i++) if (strcmp(fam_a[i], "T_CACHE_LAYOUT") == 0 || strcmp(fam_a[i], "T_ATTENTION_LATENT_CACHE") == 0 || strcmp(fam_a[i], "T_KV_MEMORY_SHARD") == 0) need_cache = 1;
            for (int i = 0; i < nfam_b; i++) if (strcmp(fam_b[i], "T_CACHE_LAYOUT") == 0 || strcmp(fam_b[i], "T_ATTENTION_LATENT_CACHE") == 0 || strcmp(fam_b[i], "T_KV_MEMORY_SHARD") == 0) need_cache = 1;
            if (coll_has_quant) { json_appendf(&bridge_json, &l3, &c3, "%s\"quant_dequant_bridge\"", l3 > 1 ? "," : ""); has_bridge = 1; }
            if (need_projector) { json_appendf(&bridge_json, &l3, &c3, "%s\"projector_bridge\"", l3 > 1 ? "," : ""); has_bridge = 1; }
            if (need_cache) { json_appendf(&bridge_json, &l3, &c3, "%s\"cache_layout_bridge\"", l3 > 1 ? "," : ""); has_bridge = 1; }
            json_appendf(&mismatch_json, &l2, &c2, "%s\"runtime formats differ\"", l2 > 1 ? "," : "");
        }
    }
    json_appendf(&mismatch_json, &l2, &c2, "]");
    json_appendf(&bridge_json, &l3, &c3, "]");
    json_appendf(&reason_json, &l4, &c4, "]");
    if ((has_matching || has_complementary) && !has_bridge) status = "compatible";
    else if ((has_matching || has_complementary) && has_bridge) status = "compatible_with_bridge";
    else status = "incompatible";
    {
        int n = snprintf(NULL, 0,
            "{\n  \"status\": \"%s\",\n  \"confidence\": %.2f,\n  \"matching_families\": %s,\n  \"complementary_families\": %s,\n  \"mismatched_constraints\": %s,\n  \"required_bridges\": %s,\n  \"reasons\": %s\n}",
            status, confidence > 1.0 ? 1.0 : confidence, match_json, complement_json, mismatch_json, bridge_json, reason_json);
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) goto fail;
        snprintf(buf, (size_t)n + 1,
            "{\n  \"status\": \"%s\",\n  \"confidence\": %.2f,\n  \"matching_families\": %s,\n  \"complementary_families\": %s,\n  \"mismatched_constraints\": %s,\n  \"required_bridges\": %s,\n  \"reasons\": %s\n}",
            status, confidence > 1.0 ? 1.0 : confidence, match_json, complement_json, mismatch_json, bridge_json, reason_json);
    }
    *out_json = buf;
    free(json_a); free(json_b);
    free(match_json); free(complement_json); free(mismatch_json); free(bridge_json); free(reason_json);
    bf_json_free(doc_a); bf_json_free(doc_b);
    return 0;
fail:
    free(json_a); free(json_b);
    free(match_json); free(complement_json); free(mismatch_json); free(bridge_json); free(reason_json);
    free(buf);
    if (doc_a) bf_json_free(doc_a);
    if (doc_b) bf_json_free(doc_b);
    return 1;
}

int bf_layer_compose_json(const char *root, const char *layer_a, const char *layer_b, int dry_run, char **out_json) {
    char *compat_json = NULL, *json_a = NULL, *json_b = NULL, *buf = NULL;
    char err[128], status[64] = "";
    bf_json_doc_t *cdoc = NULL, *adoc = NULL, *bdoc = NULL;
    const bf_json_node_t *croot, *aroot, *broot;
    char id_a[256] = "", id_b[256] = "", name_a[256] = "", name_b[256] = "", model_a[256] = "", model_b[256] = "";
    char digest[65], composite_id[256];
    if (!out_json) return 1;
    *out_json = NULL;
    if (bf_layer_compat_json(root, layer_a, layer_b, &compat_json) != 0) return 1;
    if (bf_layer_load_json(root, layer_a, &json_a) != 0) goto fail;
    if (bf_layer_load_json(root, layer_b, &json_b) != 0) goto fail;
    cdoc = bf_json_parse_str(compat_json, err, sizeof(err));
    adoc = bf_json_parse_str(json_a, err, sizeof(err));
    bdoc = bf_json_parse_str(json_b, err, sizeof(err));
    if (!cdoc || !adoc || !bdoc) goto fail;
    croot = bf_json_root(cdoc); aroot = bf_json_root(adoc); broot = bf_json_root(bdoc);
    json_copy_str(adoc, aroot, "artifact_id", id_a, sizeof(id_a));
    json_copy_str(bdoc, broot, "artifact_id", id_b, sizeof(id_b));
    json_copy_str(adoc, aroot, "name", name_a, sizeof(name_a));
    json_copy_str(bdoc, broot, "name", name_b, sizeof(name_b));
    json_copy_str(adoc, aroot, "source_model", model_a, sizeof(model_a));
    json_copy_str(bdoc, broot, "source_model", model_b, sizeof(model_b));
    json_copy_str(cdoc, croot, "status", status, sizeof(status));
    bf_sha256_hex((const uint8_t *)compat_json, strlen(compat_json), digest);
    snprintf(composite_id, sizeof(composite_id), "virtual_composite:%.*s", 40, digest);
    {
        int n = snprintf(NULL, 0,
            "{\n"
            "  \"compatibility\": %s,\n"
            "  \"artifact\": {\n"
            "    \"schema_version\": \"layer_artifact.v1\",\n"
            "    \"artifact_id\": \"%s\",\n"
            "    \"artifact_kind\": \"virtual_composite\",\n"
            "    \"name\": \"%s + %s\",\n"
            "    \"source_model\": \"%s%s%s\",\n"
            "    \"materialization_status\": \"%s\",\n"
            "    \"verification_status\": \"unverified\",\n"
            "    \"lifecycle_status\": \"materialization_planned\",\n"
            "    \"virtual_composite\": {\n"
            "      \"component_artifact_ids\": [\"%s\", \"%s\"],\n"
            "      \"composition_plan\": {\n"
            "        \"status\": \"%s\",\n"
            "        \"materialization_order\": [\"%s\", \"%s\"]\n"
            "      },\n"
            "      \"dry_run\": %s\n"
            "    }\n"
            "  }\n"
            "}",
            compat_json,
            composite_id,
            name_a[0] ? name_a : id_a,
            name_b[0] ? name_b : id_b,
            model_a,
            (model_a[0] && model_b[0] && strcmp(model_a, model_b) != 0) ? " | " : "",
            (model_b[0] && strcmp(model_a, model_b) != 0) ? model_b : "",
            dry_run ? "virtual" : "indexed",
            id_a, id_b,
            status[0] ? status : "unknown",
            id_a, id_b,
            dry_run ? "true" : "false");
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) goto fail;
        snprintf(buf, (size_t)n + 1,
            "{\n"
            "  \"compatibility\": %s,\n"
            "  \"artifact\": {\n"
            "    \"schema_version\": \"layer_artifact.v1\",\n"
            "    \"artifact_id\": \"%s\",\n"
            "    \"artifact_kind\": \"virtual_composite\",\n"
            "    \"name\": \"%s + %s\",\n"
            "    \"source_model\": \"%s%s%s\",\n"
            "    \"materialization_status\": \"%s\",\n"
            "    \"verification_status\": \"unverified\",\n"
            "    \"lifecycle_status\": \"materialization_planned\",\n"
            "    \"virtual_composite\": {\n"
            "      \"component_artifact_ids\": [\"%s\", \"%s\"],\n"
            "      \"composition_plan\": {\n"
            "        \"status\": \"%s\",\n"
            "        \"materialization_order\": [\"%s\", \"%s\"]\n"
            "      },\n"
            "      \"dry_run\": %s\n"
            "    }\n"
            "  }\n"
            "}",
            compat_json,
            composite_id,
            name_a[0] ? name_a : id_a,
            name_b[0] ? name_b : id_b,
            model_a,
            (model_a[0] && model_b[0] && strcmp(model_a, model_b) != 0) ? " | " : "",
            (model_b[0] && strcmp(model_a, model_b) != 0) ? model_b : "",
            dry_run ? "virtual" : "indexed",
            id_a, id_b,
            status[0] ? status : "unknown",
            id_a, id_b,
            dry_run ? "true" : "false");
    }
    *out_json = buf;
    free(compat_json); free(json_a); free(json_b);
    bf_json_free(cdoc); bf_json_free(adoc); bf_json_free(bdoc);
    return 0;
fail:
    free(compat_json); free(json_a); free(json_b); free(buf);
    if (cdoc) bf_json_free(cdoc); if (adoc) bf_json_free(adoc); if (bdoc) bf_json_free(bdoc);
    return 1;
}

int bf_layer_queue_job_json(const char *root, const char *queue_cmd, const char *artifact_id, int priority, char **out_json) {
    char db_path[PATH_MAX], created_at[32], digest[65], payload[512];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char *buf = NULL;
    if (!queue_cmd || !artifact_id || !out_json) return 1;
    *out_json = NULL;
    if (bf_layer_state_db_path(root, "queue.db", db_path, sizeof(db_path)) != 0) return 1;
    if (bf_sqlite3_open(db_path, &db) != SQLITE_OK) return 1;
    if (layer_init_queue_schema(db) != 0) goto fail;
    utc_now_iso(created_at);
    snprintf(payload, sizeof(payload),
             "{\"job_type\":\"layer.%s\",\"artifact_id\":\"%s\",\"allow_download\":false,\"priority\":%d}",
             strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd,
             artifact_id, priority);
    bf_sha256_hex((const uint8_t *)payload, strlen(payload), digest);
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO jobs(job_id, job_type, artifact_id, payload_json, status, priority, created_at) VALUES(?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) goto fail;
    {
        char job_type[128];
        snprintf(job_type, sizeof(job_type), "layer.%s", strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd);
        sqlite3_bind_text(st,1,digest,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,job_type,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,artifact_id,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,4,payload,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,5,"queued",-1,SQLITE_STATIC);
        sqlite3_bind_int(st,6,priority);
        sqlite3_bind_text(st,7,created_at,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_DONE) goto fail;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    {
        int n = snprintf(NULL,0,"{\n  \"job_type\": \"layer.%s\",\n  \"artifact_id\": \"%s\",\n  \"allow_download\": false,\n  \"priority\": %d,\n  \"job_id\": \"%s\"\n}",
            strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, artifact_id, priority, digest);
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) return 1;
        snprintf(buf,(size_t)n + 1,"{\n  \"job_type\": \"layer.%s\",\n  \"artifact_id\": \"%s\",\n  \"allow_download\": false,\n  \"priority\": %d,\n  \"job_id\": \"%s\"\n}",
            strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, artifact_id, priority, digest);
    }
    *out_json = buf;
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(buf);
    return 1;
}

int bf_layer_queue_plan_json(const char *root, const char *queue_cmd, const char *plan_path, int priority, char **out_json) {
    char db_path[PATH_MAX], created_at[32], digest[65], plan_digest[65], stitch_status[64] = "unknown", plan_kind[64] = "layer_dag_plan";
    char *plan_json = NULL, *validated_json = NULL, *payload = NULL, *buf = NULL;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root_json = NULL, *bridges = NULL;
    size_t lp = 0, cp = 0;
    double confidence = 0.0;

    if (!queue_cmd || !plan_path || !out_json) return 1;
    *out_json = NULL;
    plan_json = bf_read_file_full(plan_path);
    if (!plan_json) return 1;
    if (bf_layer_stitch_validate_json(plan_json, &validated_json) != 0 || !validated_json) goto fail;
    {
        char err[128];
        doc = bf_json_parse_str(validated_json, err, sizeof(err));
        if (!doc) goto fail;
        root_json = bf_json_root(doc);
        json_copy_str(doc, root_json, "stitch_status", stitch_status, sizeof(stitch_status));
        json_copy_str(doc, root_json, "plan_kind", plan_kind, sizeof(plan_kind));
        {
            const bf_json_node_t *conf = bf_json_obj_get(doc, root_json, "confidence");
            if (conf) confidence = bf_json_get_double(conf);
        }
        bridges = bf_json_obj_get(doc, root_json, "unresolved_bridges");
    }
    bf_sha256_hex((const uint8_t *)plan_json, strlen(plan_json), plan_digest);
    if (json_appendf(&payload, &lp, &cp,
                     "{\"job_type\":\"layer.%s\",\"artifact_id\":\"stitch_plan:%.*s\",\"plan_path\":",
                     strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, 40, plan_digest) != 0) goto fail;
    {
        char *qpath = json_quote_sql(plan_path);
        if (!qpath) goto fail;
        if (json_appendf(&payload, &lp, &cp,
                         "%s,\"plan_hash\":\"%s\",\"plan_kind\":\"%s\",\"stitch_status\":\"%s\",\"confidence\":%.2f,\"allow_download\":false,\"priority\":%d,\"bridge_requirements\":",
                         qpath, plan_digest, plan_kind, stitch_status, confidence, priority) != 0) {
            free(qpath);
            goto fail;
        }
        free(qpath);
    }
    if (bridges && bridges->type == BF_JSON_ARRAY) {
        int first = 1;
        if (json_appendf(&payload, &lp, &cp, "[") != 0) goto fail;
        for (const bf_json_node_t *child = bf_json_child_first(doc, bridges); child; child = bf_json_child_next(doc, child)) {
            char item[256];
            char *qitem = NULL;
            if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
            qitem = json_quote_sql(item);
            if (!qitem) goto fail;
            if (json_appendf(&payload, &lp, &cp, "%s%s", first ? "" : ",", qitem) != 0) {
                free(qitem);
                goto fail;
            }
            free(qitem);
            first = 0;
        }
        if (json_appendf(&payload, &lp, &cp, "]}") != 0) goto fail;
    } else {
        if (json_appendf(&payload, &lp, &cp, "[]}") != 0) goto fail;
    }

    bf_sha256_hex((const uint8_t *)payload, strlen(payload), digest);
    if (bf_layer_state_db_path(root, "queue.db", db_path, sizeof(db_path)) != 0) goto fail;
    if (bf_sqlite3_open(db_path, &db) != SQLITE_OK) goto fail;
    if (layer_init_queue_schema(db) != 0) goto fail;
    utc_now_iso(created_at);
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO jobs(job_id, job_type, artifact_id, payload_json, status, priority, created_at) VALUES(?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) goto fail;
    {
        char job_type[128], plan_artifact_id[256];
        snprintf(job_type, sizeof(job_type), "layer.%s", strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd);
        snprintf(plan_artifact_id, sizeof(plan_artifact_id), "stitch_plan:%.*s", 40, plan_digest);
        sqlite3_bind_text(st,1,digest,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,job_type,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,plan_artifact_id,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,4,payload,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,5,"queued",-1,SQLITE_STATIC);
        sqlite3_bind_int(st,6,priority);
        sqlite3_bind_text(st,7,created_at,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_DONE) goto fail;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    db = NULL;
    {
        int n = snprintf(NULL,0,
            "{\n  \"job_type\": \"layer.%s\",\n  \"artifact_id\": \"stitch_plan:%.*s\",\n  \"plan_path\": \"%s\",\n  \"stitch_status\": \"%s\",\n  \"allow_download\": false,\n  \"priority\": %d,\n  \"job_id\": \"%s\"\n}",
            strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, 40, plan_digest, plan_path, stitch_status, priority, digest);
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) goto fail;
        snprintf(buf,(size_t)n + 1,
            "{\n  \"job_type\": \"layer.%s\",\n  \"artifact_id\": \"stitch_plan:%.*s\",\n  \"plan_path\": \"%s\",\n  \"stitch_status\": \"%s\",\n  \"allow_download\": false,\n  \"priority\": %d,\n  \"job_id\": \"%s\"\n}",
            strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, 40, plan_digest, plan_path, stitch_status, priority, digest);
    }
    *out_json = buf;
    free(plan_json);
    free(validated_json);
    free(payload);
    if (doc) bf_json_free(doc);
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(plan_json);
    free(validated_json);
    free(payload);
    if (doc) bf_json_free(doc);
    free(buf);
    return 1;
}

int bf_layer_queue_bridge_plan_json(const char *root, const char *queue_cmd, const char *plan_path, int priority, char **out_json) {
    char db_path[PATH_MAX], created_at[32], digest[65], plan_digest[65], status[64] = "unknown";
    char *plan_json = NULL, *payload = NULL, *buf = NULL;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root_json = NULL, *candidates = NULL;
    size_t lp = 0, cp = 0;
    double confidence = 0.0;
    const char *stage = "start";

    if (!queue_cmd || !plan_path || !out_json) return 1;
    *out_json = NULL;
    stage = "read-plan";
    plan_json = bf_read_file_full(plan_path);
    if (!plan_json) return 1;
    {
        char err[128];
        stage = "parse-plan";
        doc = bf_json_parse_str(plan_json, err, sizeof(err));
        if (!doc) goto fail;
        root_json = bf_json_root(doc);
        candidates = bf_json_obj_get(doc, root_json, "bridge_candidates");
        json_copy_str(doc, root_json, "bridge_resolution_status", status, sizeof(status));
        {
            const bf_json_node_t *conf = bf_json_obj_get(doc, root_json, "confidence");
            if (conf) confidence = bf_json_get_double(conf);
        }
    }
    stage = "build-payload";
    bf_sha256_hex((const uint8_t *)plan_json, strlen(plan_json), plan_digest);
    if (json_appendf(&payload, &lp, &cp,
                     "{\"job_type\":\"layer.%s\",\"artifact_id\":\"bridge_resolution_plan:%.*s\",\"plan_path\":",
                     strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, 40, plan_digest) != 0) goto fail;
    {
        char *qpath = json_quote_sql(plan_path);
        if (!qpath) goto fail;
        if (json_appendf(&payload, &lp, &cp,
                         "%s,\"plan_hash\":\"%s\",\"plan_kind\":\"bridge_resolution_plan\",\"bridge_resolution_status\":\"%s\",\"confidence\":%.2f,\"allow_download\":false,\"priority\":%d,\"bridge_candidates\":[",
                         qpath, plan_digest, status, confidence, priority) != 0) {
            free(qpath);
            goto fail;
        }
        free(qpath);
    }
    if (candidates && candidates->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(doc, candidates); child; child = bf_json_child_next(doc, child)) {
            char cid[256] = "", bridge_family[256] = "";
            char *qcid = NULL, *qbf = NULL;
            if (child->type != BF_JSON_OBJECT) continue;
            json_copy_str(doc, child, "candidate_id", cid, sizeof(cid));
            json_copy_str(doc, child, "bridge_family", bridge_family, sizeof(bridge_family));
            qcid = json_quote_sql(cid);
            qbf = json_quote_sql(bridge_family);
            if (!qcid || !qbf) { free(qcid); free(qbf); goto fail; }
            if (json_appendf(&payload, &lp, &cp, "%s{\"candidate_id\":%s,\"bridge_family\":%s}", first ? "" : ",", qcid, qbf) != 0) {
                free(qcid); free(qbf); goto fail;
            }
            free(qcid); free(qbf);
            first = 0;
        }
    }
    if (json_appendf(&payload, &lp, &cp, "]}") != 0) goto fail;

    bf_sha256_hex((const uint8_t *)payload, strlen(payload), digest);
    stage = "resolve-db-path";
    if (bf_layer_state_db_path(root, "queue.db", db_path, sizeof(db_path)) != 0) goto fail;
    stage = "open-db";
    if (bf_sqlite3_open(db_path, &db) != SQLITE_OK) goto fail;
    stage = "init-schema";
    if (layer_init_queue_schema(db) != 0) goto fail;
    utc_now_iso(created_at);
    stage = "prepare-insert";
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO jobs(job_id, job_type, artifact_id, payload_json, status, priority, created_at) VALUES(?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) goto fail;
    {
        char job_type[128], artifact_id[256];
        snprintf(job_type, sizeof(job_type), "layer.%s", strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd);
        snprintf(artifact_id, sizeof(artifact_id), "bridge_resolution_plan:%.*s", 40, plan_digest);
        sqlite3_bind_text(st,1,digest,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,job_type,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,artifact_id,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,4,payload,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,5,"queued",-1,SQLITE_STATIC);
        sqlite3_bind_int(st,6,priority);
        sqlite3_bind_text(st,7,created_at,-1,SQLITE_TRANSIENT);
        stage = "step-insert";
        if (sqlite3_step(st) != SQLITE_DONE) goto fail;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    db = NULL;
    {
        int n = snprintf(NULL,0,
            "{\n  \"job_type\": \"layer.%s\",\n  \"artifact_id\": \"bridge_resolution_plan:%.*s\",\n  \"plan_path\": \"%s\",\n  \"bridge_resolution_status\": \"%s\",\n  \"allow_download\": false,\n  \"priority\": %d,\n  \"job_id\": \"%s\"\n}",
            strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, 40, plan_digest, plan_path, status, priority, digest);
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) goto fail;
        snprintf(buf,(size_t)n + 1,
            "{\n  \"job_type\": \"layer.%s\",\n  \"artifact_id\": \"bridge_resolution_plan:%.*s\",\n  \"plan_path\": \"%s\",\n  \"bridge_resolution_status\": \"%s\",\n  \"allow_download\": false,\n  \"priority\": %d,\n  \"job_id\": \"%s\"\n}",
            strstr(queue_cmd, "layer-") == queue_cmd ? queue_cmd + 6 : queue_cmd, 40, plan_digest, plan_path, status, priority, digest);
    }
    *out_json = buf;
    free(plan_json);
    free(payload);
    if (doc) bf_json_free(doc);
    return 0;
fail:
    if (db) fprintf(stderr, "bonfyre-layer-bridge queue fail at %s: %s\n", stage, sqlite3_errmsg(db));
    else fprintf(stderr, "bonfyre-layer-bridge queue fail at %s\n", stage);
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    free(plan_json);
    free(payload);
    if (doc) bf_json_free(doc);
    free(buf);
    return 1;
}

int bf_layer_stitch_plan_json(const char *root, const char *layer_a, const char *layer_b, char **out_json) {
    char *compat_json = NULL, *base_plan = NULL, *validated_json = NULL, *buf = NULL;
    char *equiv_json = NULL, *bridge_edges_json = NULL, *incompat_json = NULL, *unknown_json = NULL, *unresolved_json = NULL, *mat_json = NULL;
    char err[128], stitch_status[64] = "unknown";
    bf_json_doc_t *vdoc = NULL;
    const bf_json_node_t *vroot = NULL, *conf = NULL, *eq = NULL, *bridges = NULL, *unresolved = NULL, *mat = NULL, *validn = NULL, *incompat = NULL, *unknown = NULL;
    double confidence = 0.0;
    int valid = 0;
    size_t le = 0, ce = 0, lb = 0, cb = 0, li = 0, ci = 0, lu = 0, cu = 0, lur = 0, cur = 0, lm = 0, cm = 0;
    if (!out_json) return 1;
    *out_json = NULL;
    if (bf_layer_compat_json(root, layer_a, layer_b, &compat_json) != 0) return 1;
    {
        int n = snprintf(NULL, 0,
            "{\n  \"plan_kind\": \"layer_dag_plan\",\n  \"components\": [\"%s\", \"%s\"],\n  \"compatibility\": %s,\n  \"materialization_order\": [\"%s\", \"%s\"],\n  \"bridge_requirements\": %s\n}",
            layer_a, layer_b, compat_json, layer_a, layer_b,
            strstr(compat_json, "\"required_bridges\":") ? strstr(compat_json, "\"required_bridges\":") + strlen("\"required_bridges\": ") : "[]");
        const char *bridges = strstr(compat_json, "\"required_bridges\":");
        char bridge_json[2048] = "[]";
        if (bridges) {
            const char *start = strchr(bridges, '[');
            const char *end = start ? strchr(start, ']') : NULL;
            if (start && end && (size_t)(end - start + 1) < sizeof(bridge_json)) {
                memcpy(bridge_json, start, (size_t)(end - start + 1));
                bridge_json[end - start + 1] = '\0';
            }
        }
        n = snprintf(NULL, 0,
            "{\n  \"plan_kind\": \"layer_dag_plan\",\n  \"components\": [\"%s\", \"%s\"],\n  \"compatibility\": %s,\n  \"materialization_order\": [\"%s\", \"%s\"],\n  \"bridge_requirements\": %s\n}",
            layer_a, layer_b, compat_json, layer_a, layer_b, bridge_json);
        base_plan = (char *)malloc((size_t)n + 1);
        if (!base_plan) { free(compat_json); return 1; }
        snprintf(base_plan, (size_t)n + 1,
            "{\n  \"plan_kind\": \"layer_dag_plan\",\n  \"components\": [\"%s\", \"%s\"],\n  \"compatibility\": %s,\n  \"materialization_order\": [\"%s\", \"%s\"],\n  \"bridge_requirements\": %s\n}",
            layer_a, layer_b, compat_json, layer_a, layer_b, bridge_json);
    }
    if (bf_layer_stitch_validate_json(base_plan, &validated_json) != 0 && !validated_json) goto fail;
    vdoc = bf_json_parse_str(validated_json, err, sizeof(err));
    if (!vdoc) goto fail;
    vroot = bf_json_root(vdoc);
    json_copy_str(vdoc, vroot, "stitch_status", stitch_status, sizeof(stitch_status));
    conf = bf_json_obj_get(vdoc, vroot, "confidence");
    if (conf) confidence = bf_json_get_double(conf);
    validn = bf_json_obj_get(vdoc, vroot, "valid");
    if (validn) valid = bf_json_get_bool(validn);
    eq = bf_json_obj_get(vdoc, vroot, "equivalent_edges");
    bridges = bf_json_obj_get(vdoc, vroot, "bridge_edges");
    incompat = bf_json_obj_get(vdoc, vroot, "incompatible_edges");
    unknown = bf_json_obj_get(vdoc, vroot, "unknown_edges");
    unresolved = bf_json_obj_get(vdoc, vroot, "unresolved_bridges");
    mat = bf_json_obj_get(vdoc, vroot, "materialization_order");
    json_appendf(&equiv_json, &le, &ce, "[");
    if (eq && eq->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(vdoc, eq); child; child = bf_json_child_next(vdoc, child)) {
            char family[256] = "";
            char *qfamily = NULL;
            if (child->type != BF_JSON_OBJECT) continue;
            json_copy_str(vdoc, child, "family", family, sizeof(family));
            qfamily = json_quote_sql(family);
            if (!qfamily) goto fail;
            if (json_appendf(&equiv_json, &le, &ce, "%s{\"edge_type\":\"equivalent\",\"family\":%s}", first ? "" : ",", qfamily) != 0) {
                free(qfamily); goto fail;
            }
            free(qfamily);
            first = 0;
        }
    }
    json_appendf(&equiv_json, &le, &ce, "]");
    json_appendf(&bridge_edges_json, &lb, &cb, "[");
    if (bridges && bridges->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(vdoc, bridges); child; child = bf_json_child_next(vdoc, child)) {
            char sf[256] = "", tf[256] = "", rel[128] = "", dir[64] = "", req[128] = "";
            char *qsf = NULL, *qtf = NULL, *qrel = NULL, *qdir = NULL, *qreq = NULL;
            if (child->type != BF_JSON_OBJECT) continue;
            json_copy_str(vdoc, child, "source_family", sf, sizeof(sf));
            json_copy_str(vdoc, child, "target_family", tf, sizeof(tf));
            json_copy_str(vdoc, child, "relationship", rel, sizeof(rel));
            json_copy_str(vdoc, child, "directionality", dir, sizeof(dir));
            json_copy_str(vdoc, child, "required_bridge", req, sizeof(req));
            qsf = json_quote_sql(sf); qtf = json_quote_sql(tf); qrel = json_quote_sql(rel); qdir = json_quote_sql(dir); qreq = json_quote_sql(req);
            if (!qsf || !qtf || !qrel || !qdir || !qreq) { free(qsf); free(qtf); free(qrel); free(qdir); free(qreq); goto fail; }
            if (json_appendf(&bridge_edges_json, &lb, &cb, "%s{\"edge_type\":\"complementary_bridge\",\"source_family\":%s,\"target_family\":%s,\"relationship\":%s,\"directionality\":%s,\"required_bridge\":%s}",
                             first ? "" : ",", qsf, qtf, qrel, qdir, qreq) != 0) {
                free(qsf); free(qtf); free(qrel); free(qdir); free(qreq); goto fail;
            }
            free(qsf); free(qtf); free(qrel); free(qdir); free(qreq);
            first = 0;
        }
    }
    json_appendf(&bridge_edges_json, &lb, &cb, "]");
    json_appendf(&incompat_json, &li, &ci, "[");
    if (incompat && incompat->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(vdoc, incompat); child; child = bf_json_child_next(vdoc, child)) {
            char reason[256] = "";
            char *qreason = NULL;
            if (child->type != BF_JSON_OBJECT) continue;
            json_copy_str(vdoc, child, "reason", reason, sizeof(reason));
            qreason = json_quote_sql(reason);
            if (!qreason) goto fail;
            if (json_appendf(&incompat_json, &li, &ci, "%s{\"edge_type\":\"incompatible\",\"reason\":%s}", first ? "" : ",", qreason) != 0) {
                free(qreason); goto fail;
            }
            free(qreason);
            first = 0;
        }
    }
    json_appendf(&incompat_json, &li, &ci, "]");
    json_appendf(&unknown_json, &lu, &cu, "[");
    if (unknown && unknown->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(vdoc, unknown); child; child = bf_json_child_next(vdoc, child)) {
            char reason[256] = "";
            char *qreason = NULL;
            if (child->type != BF_JSON_OBJECT) continue;
            json_copy_str(vdoc, child, "reason", reason, sizeof(reason));
            qreason = json_quote_sql(reason);
            if (!qreason) goto fail;
            if (json_appendf(&unknown_json, &lu, &cu, "%s{\"edge_type\":\"unknown\",\"reason\":%s}", first ? "" : ",", qreason) != 0) {
                free(qreason); goto fail;
            }
            free(qreason);
            first = 0;
        }
    }
    json_appendf(&unknown_json, &lu, &cu, "]");
    json_appendf(&unresolved_json, &lur, &cur, "[");
    if (unresolved && unresolved->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(vdoc, unresolved); child; child = bf_json_child_next(vdoc, child)) {
            char item[256] = "";
            char *qitem = NULL;
            if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
            qitem = json_quote_sql(item);
            if (!qitem) goto fail;
            if (json_appendf(&unresolved_json, &lur, &cur, "%s%s", first ? "" : ",", qitem) != 0) {
                free(qitem); goto fail;
            }
            free(qitem);
            first = 0;
        }
    }
    json_appendf(&unresolved_json, &lur, &cur, "]");
    json_appendf(&mat_json, &lm, &cm, "[");
    if (mat && mat->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(vdoc, mat); child; child = bf_json_child_next(vdoc, child)) {
            char item[256] = "";
            char *qitem = NULL;
            if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
            qitem = json_quote_sql(item);
            if (!qitem) goto fail;
            if (json_appendf(&mat_json, &lm, &cm, "%s%s", first ? "" : ",", qitem) != 0) {
                free(qitem); goto fail;
            }
            free(qitem);
            first = 0;
        }
    }
    json_appendf(&mat_json, &lm, &cm, "]");
    {
        int n = snprintf(NULL, 0,
            "{\n  \"plan_kind\": \"layer_dag_plan\",\n  \"valid\": %s,\n  \"stitch_status\": \"%s\",\n  \"confidence\": %.2f,\n  \"components\": [\"%s\", \"%s\"],\n  \"compatibility\": %s,\n  \"equivalent_edges\": %s,\n  \"bridge_edges\": %s,\n  \"incompatible_edges\": %s,\n  \"unknown_edges\": %s,\n  \"unresolved_bridges\": %s,\n  \"materialization_order\": %s,\n  \"bridge_requirements\": %s\n}",
            valid ? "true" : "false", stitch_status, confidence,
            layer_a, layer_b, compat_json, equiv_json, bridge_edges_json, incompat_json, unknown_json, unresolved_json, mat_json, unresolved_json);
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) goto fail;
        snprintf(buf, (size_t)n + 1,
            "{\n  \"plan_kind\": \"layer_dag_plan\",\n  \"valid\": %s,\n  \"stitch_status\": \"%s\",\n  \"confidence\": %.2f,\n  \"components\": [\"%s\", \"%s\"],\n  \"compatibility\": %s,\n  \"equivalent_edges\": %s,\n  \"bridge_edges\": %s,\n  \"incompatible_edges\": %s,\n  \"unknown_edges\": %s,\n  \"unresolved_bridges\": %s,\n  \"materialization_order\": %s,\n  \"bridge_requirements\": %s\n}",
            valid ? "true" : "false", stitch_status, confidence,
            layer_a, layer_b, compat_json, equiv_json, bridge_edges_json, incompat_json, unknown_json, unresolved_json, mat_json, unresolved_json);
    }
    free(compat_json);
    free(base_plan);
    free(validated_json);
    bf_json_free(vdoc);
    free(equiv_json); free(bridge_edges_json); free(incompat_json); free(unknown_json); free(unresolved_json); free(mat_json);
    *out_json = buf;
    return 0;
fail:
    free(compat_json);
    free(base_plan);
    free(validated_json);
    if (vdoc) bf_json_free(vdoc);
    free(equiv_json); free(bridge_edges_json); free(incompat_json); free(unknown_json); free(unresolved_json); free(mat_json);
    free(buf);
    return 1;
}

int bf_layer_stitch_validate_json(const char *plan_json, char **out_json) {
    char err[128], status[64] = "unknown", pkbuf[64] = "unknown";
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root = NULL, *compat = NULL, *artifact = NULL, *vcomp = NULL, *cplan = NULL;
    const bf_json_node_t *matching = NULL, *complementary = NULL, *required = NULL, *mat_order = NULL, *pk = NULL;
    char *buf = NULL, *equiv_json = NULL, *bridge_edges_json = NULL, *incompat_json = NULL, *unknown_json = NULL, *unresolved_json = NULL, *mat_json = NULL;
    size_t le = 0, ce = 0, lb = 0, cb = 0, li = 0, ci = 0, lu = 0, cu = 0, lur = 0, cur = 0, lm = 0, cm = 0;
    const char *stitch_status = "invalid";
    double confidence = 0.0;
    int valid = 0;

    if (!plan_json || !out_json) return 1;
    *out_json = NULL;
    doc = bf_json_parse_str(plan_json, err, sizeof(err));
    if (!doc) return 1;
    root = bf_json_root(doc);
    artifact = bf_json_obj_get(doc, root, "artifact");
    compat = bf_json_obj_get(doc, root, "compatibility");
    pk = bf_json_obj_get(doc, root, "plan_kind");
    if (pk) json_copy_str(doc, root, "plan_kind", pkbuf, sizeof(pkbuf));
    else if (artifact && json_string_equals(doc, artifact, "artifact_kind", "virtual_composite")) snprintf(pkbuf, sizeof(pkbuf), "virtual_composite");
    else if (json_string_equals(doc, root, "artifact_kind", "virtual_composite")) snprintf(pkbuf, sizeof(pkbuf), "virtual_composite");

    if (artifact) {
        vcomp = bf_json_obj_get(doc, artifact, "virtual_composite");
        cplan = vcomp ? bf_json_obj_get(doc, vcomp, "composition_plan") : NULL;
    } else if (json_string_equals(doc, root, "artifact_kind", "virtual_composite")) {
        vcomp = bf_json_obj_get(doc, root, "virtual_composite");
        cplan = vcomp ? bf_json_obj_get(doc, vcomp, "composition_plan") : NULL;
    }
    if (!compat && cplan) compat = bf_json_obj_get(doc, cplan, "compatibility");
    if (!compat) goto fail;

    json_copy_str(doc, compat, "status", status, sizeof(status));
    {
        const bf_json_node_t *conf = bf_json_obj_get(doc, compat, "confidence");
        if (conf) confidence = bf_json_get_double(conf);
    }
    matching = bf_json_obj_get(doc, compat, "matching_families");
    complementary = bf_json_obj_get(doc, compat, "complementary_families");
    required = bf_json_obj_get(doc, compat, "required_bridges");
    mat_order = bf_json_obj_get(doc, root, "materialization_order");
    if (!mat_order && cplan) mat_order = bf_json_obj_get(doc, cplan, "materialization_order");

    json_appendf(&equiv_json, &le, &ce, "[");
    if (matching && matching->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(doc, matching); child; child = bf_json_child_next(doc, child)) {
            char fam[256];
            char *qfam;
            if (bf_json_get_str_copy(child, fam, sizeof(fam)) <= 0) continue;
            qfam = json_quote_sql(fam);
            if (!qfam) goto fail;
            if (json_appendf(&equiv_json, &le, &ce, "%s{\"edge_type\":\"equivalent\",\"family\":%s}", first ? "" : ",", qfam) != 0) {
                free(qfam); goto fail;
            }
            free(qfam);
            first = 0;
        }
    }
    json_appendf(&equiv_json, &le, &ce, "]");

    json_appendf(&bridge_edges_json, &lb, &cb, "[");
    if (complementary && complementary->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(doc, complementary); child; child = bf_json_child_next(doc, child)) {
            const bf_json_node_t *src = bf_json_arr_get(doc, child, 0);
            const bf_json_node_t *dst = bf_json_arr_get(doc, child, 1);
            const bf_json_node_t *meta = bf_json_arr_get(doc, child, 2);
            char sf[256], tf[256], relationship[128] = "", directionality[64] = "", req_bridge[128] = "";
            char *qsf, *qtf, *qrel, *qdir, *qbridge;
            if (!src || !dst || bf_json_get_str_copy(src, sf, sizeof(sf)) <= 0 || bf_json_get_str_copy(dst, tf, sizeof(tf)) <= 0) continue;
            if (meta) {
                json_copy_str(doc, meta, "relationship", relationship, sizeof(relationship));
                json_copy_str(doc, meta, "directionality", directionality, sizeof(directionality));
                json_copy_str(doc, meta, "required_bridge", req_bridge, sizeof(req_bridge));
            }
            qsf = json_quote_sql(sf); qtf = json_quote_sql(tf);
            qrel = json_quote_sql(relationship); qdir = json_quote_sql(directionality); qbridge = json_quote_sql(req_bridge);
            if (!qsf || !qtf || !qrel || !qdir || !qbridge) {
                free(qsf); free(qtf); free(qrel); free(qdir); free(qbridge);
                goto fail;
            }
            if (json_appendf(&bridge_edges_json, &lb, &cb,
                             "%s{\"edge_type\":\"complementary_bridge\",\"source_family\":%s,\"target_family\":%s,\"relationship\":%s,\"directionality\":%s,\"required_bridge\":%s}",
                             first ? "" : ",", qsf, qtf, qrel, qdir, qbridge) != 0) {
                free(qsf); free(qtf); free(qrel); free(qdir); free(qbridge);
                goto fail;
            }
            free(qsf); free(qtf); free(qrel); free(qdir); free(qbridge);
            first = 0;
        }
    }
    json_appendf(&bridge_edges_json, &lb, &cb, "]");

    json_appendf(&unresolved_json, &lur, &cur, "[");
    if (required && required->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(doc, required); child; child = bf_json_child_next(doc, child)) {
            char bridge[256];
            char *qbridge;
            if (bf_json_get_str_copy(child, bridge, sizeof(bridge)) <= 0) continue;
            qbridge = json_quote_sql(bridge);
            if (!qbridge) goto fail;
            if (json_appendf(&unresolved_json, &lur, &cur, "%s%s", first ? "" : ",", qbridge) != 0) {
                free(qbridge); goto fail;
            }
            free(qbridge);
            first = 0;
        }
    }
    json_appendf(&unresolved_json, &lur, &cur, "]");

    json_appendf(&mat_json, &lm, &cm, "[");
    if (mat_order && mat_order->type == BF_JSON_ARRAY) {
        int first = 1;
        for (const bf_json_node_t *child = bf_json_child_first(doc, mat_order); child; child = bf_json_child_next(doc, child)) {
            char item[256];
            char *qitem;
            if (bf_json_get_str_copy(child, item, sizeof(item)) <= 0) continue;
            qitem = json_quote_sql(item);
            if (!qitem) goto fail;
            if (json_appendf(&mat_json, &lm, &cm, "%s%s", first ? "" : ",", qitem) != 0) {
                free(qitem); goto fail;
            }
            free(qitem);
            first = 0;
        }
    }
    json_appendf(&mat_json, &lm, &cm, "]");

    json_appendf(&incompat_json, &li, &ci, "[");
    json_appendf(&unknown_json, &lu, &cu, "[");
    if (strcmp(status, "incompatible") == 0) {
        json_appendf(&incompat_json, &li, &ci, "{\"edge_type\":\"incompatible\",\"reason\":\"compatibility status incompatible\"}");
        stitch_status = "invalid";
    } else if (strcmp(status, "unknown") == 0) {
        json_appendf(&unknown_json, &lu, &cu, "{\"edge_type\":\"unknown\",\"reason\":\"compatibility status unknown\"}");
        stitch_status = "unknown";
    } else if (required && required->type == BF_JSON_ARRAY && bf_json_child_first(doc, required)) {
        stitch_status = "bridge_required";
        valid = 1;
    } else if (strcmp(status, "compatible") == 0) {
        stitch_status = "valid";
        valid = 1;
    } else if (strcmp(status, "compatible_with_bridge") == 0) {
        stitch_status = "bridge_required";
        valid = 1;
    }
    json_appendf(&incompat_json, &li, &ci, "]");
    json_appendf(&unknown_json, &lu, &cu, "]");

    {
        int n = snprintf(NULL, 0,
                         "{\n  \"valid\": %s,\n  \"plan_kind\": \"%s\",\n  \"stitch_status\": \"%s\",\n  \"confidence\": %.2f,\n  \"equivalent_edges\": %s,\n  \"bridge_edges\": %s,\n  \"incompatible_edges\": %s,\n  \"unknown_edges\": %s,\n  \"unresolved_bridges\": %s,\n  \"materialization_order\": %s\n}",
                         valid ? "true" : "false", pkbuf, stitch_status, confidence,
                         equiv_json, bridge_edges_json, incompat_json, unknown_json, unresolved_json, mat_json);
        buf = (char *)malloc((size_t)n + 1);
        if (!buf) goto fail;
        snprintf(buf, (size_t)n + 1,
                 "{\n  \"valid\": %s,\n  \"plan_kind\": \"%s\",\n  \"stitch_status\": \"%s\",\n  \"confidence\": %.2f,\n  \"equivalent_edges\": %s,\n  \"bridge_edges\": %s,\n  \"incompatible_edges\": %s,\n  \"unknown_edges\": %s,\n  \"unresolved_bridges\": %s,\n  \"materialization_order\": %s\n}",
                 valid ? "true" : "false", pkbuf, stitch_status, confidence,
                 equiv_json, bridge_edges_json, incompat_json, unknown_json, unresolved_json, mat_json);
    }
    *out_json = buf;
    bf_json_free(doc);
    free(equiv_json); free(bridge_edges_json); free(incompat_json); free(unknown_json); free(unresolved_json); free(mat_json);
    return valid ? 0 : 1;
fail:
    if (doc) bf_json_free(doc);
    free(buf);
    free(equiv_json); free(bridge_edges_json); free(incompat_json); free(unknown_json); free(unresolved_json); free(mat_json);
    return 1;
}

int bf_layer_stitch_validate_file(const char *plan_path, char **out_json) {
    FILE *fp;
    long sz;
    char *buf;
    int rc;
    if (!plan_path || !out_json) return 1;
    fp = fopen(plan_path, "rb");
    if (!fp) return 1;
    fseek(fp, 0, SEEK_END);
    sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (sz < 0) { fclose(fp); return 1; }
    buf = (char *)malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return 1; }
    fread(buf, 1, (size_t)sz, fp);
    buf[sz] = '\0';
    fclose(fp);
    rc = bf_layer_stitch_validate_json(buf, out_json);
    free(buf);
    return rc;
}

int bf_layer_stitch_resolve_bridges_json(const char *root, const char *plan_path, char **out_json) {
    char *plan_json = NULL, *validated_json = NULL, *buf = NULL;
    char err[128], stitch_status[64] = "unknown";
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root_json = NULL, *unresolved = NULL, *bridge_edges = NULL;
    size_t len = 0, cap = 0;
    int first = 1, candidate_count = 0, missing_count = 0, valid = 0;
    double confidence = 0.0;
    (void)root;

    if (!plan_path || !out_json) return 1;
    *out_json = NULL;
    plan_json = bf_read_file_full(plan_path);
    if (!plan_json) return 1;
    if (bf_layer_stitch_validate_json(plan_json, &validated_json) != 0 || !validated_json) goto fail;
    doc = bf_json_parse_str(validated_json, err, sizeof(err));
    if (!doc) goto fail;
    root_json = bf_json_root(doc);
    unresolved = bf_json_obj_get(doc, root_json, "unresolved_bridges");
    bridge_edges = bf_json_obj_get(doc, root_json, "bridge_edges");
    json_copy_str(doc, root_json, "stitch_status", stitch_status, sizeof(stitch_status));
    {
        const bf_json_node_t *conf = bf_json_obj_get(doc, root_json, "confidence");
        const bf_json_node_t *validn = bf_json_obj_get(doc, root_json, "valid");
        if (conf) confidence = bf_json_get_double(conf);
        if (validn) valid = bf_json_get_bool(validn);
    }
    if (json_appendf(&buf, &len, &cap,
                     "{\n  \"plan_kind\": \"bridge_resolution_plan\",\n  \"valid\": %s,\n  \"stitch_status\": \"%s\",\n  \"confidence\": %.2f,\n  \"base_plan\": %s,\n  \"bridge_candidates\": [",
                     valid ? "true" : "false", stitch_status, confidence, plan_json) != 0) goto fail;
    for (const bf_json_node_t *child = unresolved ? bf_json_child_first(doc, unresolved) : NULL;
         child; child = bf_json_child_next(doc, child)) {
        char bridge_kind[256] = "";
        int found = 0;
        if (bf_json_get_str_copy(child, bridge_kind, sizeof(bridge_kind)) <= 0) continue;
        for (const bf_json_node_t *edge = bridge_edges ? bf_json_child_first(doc, bridge_edges) : NULL;
             edge; edge = bf_json_child_next(doc, edge)) {
            char sf[256] = "", tf[256] = "", rel[128] = "", dir[64] = "", req[256] = "", digest[65], candidate_id[256];
            char *qbridge = NULL, *qsf = NULL, *qtf = NULL, *qrel = NULL, *qdir = NULL;
            double base = 0.25;
            if (edge->type != BF_JSON_OBJECT) continue;
            json_copy_str(doc, edge, "required_bridge", req, sizeof(req));
            if (strcmp(req, bridge_kind) != 0) continue;
            json_copy_str(doc, edge, "source_family", sf, sizeof(sf));
            json_copy_str(doc, edge, "target_family", tf, sizeof(tf));
            json_copy_str(doc, edge, "relationship", rel, sizeof(rel));
            json_copy_str(doc, edge, "directionality", dir, sizeof(dir));
            for (int r = 0; BF_LAYER_FAMILY_RELATIONS[r].family_a; r++) {
                const bf_layer_family_relation_t *rr = &BF_LAYER_FAMILY_RELATIONS[r];
                if (strcmp(rr->bridge_kind, bridge_kind) == 0 &&
                    strcmp(rr->family_a, sf) == 0 &&
                    strcmp(rr->family_b, tf) == 0) {
                    base = rr->confidence_base;
                    break;
                }
            }
            {
                char seed[1024];
                snprintf(seed, sizeof(seed), "%s|%s|%s|%s", bridge_kind, sf, tf, rel);
                bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
            }
            snprintf(candidate_id, sizeof(candidate_id), "bridge_artifact:%.*s", 40, digest);
            qbridge = json_quote_sql(bridge_kind);
            qsf = json_quote_sql(sf);
            qtf = json_quote_sql(tf);
            qrel = json_quote_sql(rel);
            qdir = json_quote_sql(dir);
            if (!qbridge || !qsf || !qtf || !qrel || !qdir) {
                free(qbridge); free(qsf); free(qtf); free(qrel); free(qdir);
                goto fail;
            }
            if (json_appendf(&buf, &len, &cap,
                             "%s{\"candidate_id\":\"%s\",\"bridge_family\":%s,\"artifact_kind\":\"adapter_bridge\",\"source_family\":%s,\"target_family\":%s,\"relationship\":%s,\"directionality\":%s,\"confidence\":%.2f,\"materialization_status\":\"virtual\",\"resolution_mode\":\"metadata-first\"}",
                             first ? "" : ",", candidate_id, qbridge, qsf, qtf, qrel, qdir, base) != 0) {
                free(qbridge); free(qsf); free(qtf); free(qrel); free(qdir);
                goto fail;
            }
            free(qbridge); free(qsf); free(qtf); free(qrel); free(qdir);
            first = 0;
            found = 1;
            candidate_count++;
        }
        if (!found) missing_count++;
    }
    if (json_appendf(&buf, &len, &cap, "],\n  \"missing_bridges\": [") != 0) goto fail;
    first = 1;
    for (const bf_json_node_t *child = unresolved ? bf_json_child_first(doc, unresolved) : NULL;
         child; child = bf_json_child_next(doc, child)) {
        char bridge_kind[256] = "";
        int found = 0;
        char *qbridge = NULL;
        if (bf_json_get_str_copy(child, bridge_kind, sizeof(bridge_kind)) <= 0) continue;
        for (const bf_json_node_t *edge = bridge_edges ? bf_json_child_first(doc, bridge_edges) : NULL;
             edge; edge = bf_json_child_next(doc, edge)) {
            char req[256] = "";
            json_copy_str(doc, edge, "required_bridge", req, sizeof(req));
            if (strcmp(req, bridge_kind) == 0) { found = 1; break; }
        }
        if (found) continue;
        qbridge = json_quote_sql(bridge_kind);
        if (!qbridge) goto fail;
        if (json_appendf(&buf, &len, &cap, "%s%s", first ? "" : ",", qbridge) != 0) {
            free(qbridge);
            goto fail;
        }
        free(qbridge);
        first = 0;
    }
    if (json_appendf(&buf, &len, &cap,
                     "],\n  \"bridge_resolution_status\": \"%s\",\n  \"candidate_count\": %d\n}",
                     missing_count == 0 ? "candidates_found" : "missing_candidates", candidate_count) != 0) goto fail;
    *out_json = buf;
    free(plan_json);
    free(validated_json);
    bf_json_free(doc);
    return 0;
fail:
    free(plan_json);
    free(validated_json);
    if (doc) bf_json_free(doc);
    free(buf);
    return 1;
}

int bf_layer_stitch_composite_json(const char *virtual_composite_id, const char *out_dir, char **out_json) {
    char *buf;
    int n;
    if (!virtual_composite_id || !out_dir || !out_json) return 1;
    *out_json = NULL;
    n = snprintf(NULL, 0,
                 "{\n  \"virtual_composite_id\": \"%s\",\n  \"out_dir\": \"%s\",\n  \"status\": \"validated\"\n}",
                 virtual_composite_id, out_dir);
    buf = (char *)malloc((size_t)n + 1);
    if (!buf) return 1;
    snprintf(buf, (size_t)n + 1,
             "{\n  \"virtual_composite_id\": \"%s\",\n  \"out_dir\": \"%s\",\n  \"status\": \"validated\"\n}",
             virtual_composite_id, out_dir);
    *out_json = buf;
    return 0;
}
