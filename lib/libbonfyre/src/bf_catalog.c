#include "bonfyre.h"
#include "bf_json.h"

#include <dirent.h>
#include <fnmatch.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define BF_CATALOG_DB_SUBPATH "/.local/share/bonfyre/catalog.db"
#define BF_CAPABILITY_DB_SUBPATH "/.local/share/bonfyre/capability.db"
#define BF_CAPABILITY_DB_FALLBACK_FILE "capability.sqlite3"
#define BF_MODEL_DB_SUBPATH "/.local/share/bonfyre/models.db"
#define BF_LAYER_DB_SUBPATH "/.local/share/bonfyre/layers.db"

static const char *BF_CATALOG_SCHEMA =
    "CREATE TABLE IF NOT EXISTS catalog_nodes ("
    "  node_id TEXT PRIMARY KEY,"
    "  kind TEXT NOT NULL,"
    "  external_id TEXT NOT NULL,"
    "  name TEXT,"
    "  category TEXT,"
    "  summary TEXT,"
    "  source_path TEXT,"
    "  source_hash TEXT,"
    "  json_data TEXT,"
    "  updated_at INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_catalog_kind_external ON catalog_nodes(kind, external_id);"
    "CREATE TABLE IF NOT EXISTS catalog_edges ("
    "  src_node_id TEXT NOT NULL,"
    "  rel TEXT NOT NULL,"
    "  dst_node_id TEXT NOT NULL,"
    "  meta TEXT,"
    "  PRIMARY KEY(src_node_id, rel, dst_node_id)"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_catalog_edges_rel_src ON catalog_edges(rel, src_node_id);"
    "CREATE INDEX IF NOT EXISTS idx_catalog_edges_rel_dst ON catalog_edges(rel, dst_node_id);"
    "CREATE VIRTUAL TABLE IF NOT EXISTS catalog_fts USING fts5("
    "  node_id, kind, external_id, name, category, summary,"
    "  content='catalog_nodes', content_rowid='rowid'"
    ");";

typedef struct {
    const char *family_id;
    const char *related;
    const char *workflow_examples;
} bf_catalog_family_seed_t;

static const bf_catalog_family_seed_t BF_CATALOG_FAMILY_SEEDS[] = {
    {
        "T_CASEOPS",
        "T_WHITESPACE_PACK, T_MARKER_STREAM",
        "A3|s03|Transcript Cleanup|BonfyreTranscribe\nA3|s05|Canonicalization|BonfyreCanon"
    },
    {
        "T_WHITESPACE_PACK",
        "T_CASEOPS, T_BYTE_REMAP",
        "A3|s03|Transcript Cleanup|BonfyreTranscribe\nA1|s03|Brief Generation|BonfyreBrief"
    },
    {
        "T_MARKER_STREAM",
        "T_CASEOPS, T_BYTE_REMAP",
        "A3|s01|VAD Segmentation|BonfyreSpeechLoop\nA3|s06|Graph Construction|BonfyreGraph"
    },
    {
        "T_BYTE_REMAP",
        "T_WHITESPACE_PACK, T_MARKER_STREAM",
        "A3|s06|Graph Construction|BonfyreGraph\nA1|s03|Brief Generation|BonfyreBrief"
    },
    {
        "T_RECUR_LATE",
        "T_RECUR_EARLY, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_RECUR_EARLY",
        "T_RECUR_LATE, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_RECUR_PROGRESSIVE",
        "T_RECUR_LATE, T_RECUR_EARLY",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_SHARED_QK",
        "T_PARALLEL_RESIDUAL, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    {
        "T_PARALLEL_RESIDUAL",
        "T_SHARED_QK, T_RECUR_PROGRESSIVE",
        "A1|s03|Brief Generation|BonfyreBrief\nA3|s11|Brief Generation|BonfyreBrief"
    },
    { NULL, NULL, NULL }
};

typedef struct {
    const char *kind;
    const char *src;
    const char *step_name_contains;
    const char *capability_id;
    const char *reason;
} bf_catalog_capability_rule_t;

static const bf_catalog_capability_rule_t BF_CAPABILITY_TAGGING_RULES[] = {
    {"operator", "BonfyreTranscribe", "Cleanup|Clean", "clean", "Transcribe cleanup stages normalize text post-ASR"},
    {"operator", "BonfyreTranscribe", NULL, "transcribe", "Transcribe stages emit text from audio"},
    {"operator", "BonfyreSpeechLoop", NULL, "segment", "Speech loop performs streaming ASR/VAD segmentation"},
    {"operator", "BonfyreSegment", NULL, "segment", "Segment performs speaker/VAD segmentation"},
    {"operator", "BonfyreBrief", NULL, "brief", "Brief emits structured brief artifacts"},
    {"operator", "BonfyreCodeBrief", NULL, "brief", "Code brief specializes brief generation"},
    {"operator", "BonfyreProof", NULL, "proof", "Proof verifies or bundles evidence"},
    {"operator", "BonfyreOffer", NULL, "offer", "Offer composes offer documents"},
    {"operator", "BonfyreNarrate", NULL, "narrate", "Narrate emits speech/narration output"},
    {"operator", "BonfyrePack", NULL, "pack", "Pack packages artifact families"},
    {"operator", "BonfyrePackage", NULL, "pack", "Package variants map to pack capability"},
    {"operator", "BonfyreDistribute", NULL, "distribute", "Distribute publishes artifacts"},
    {"operator", "BonfyreEmbed", NULL, "embed", "Embed creates vector embeddings"},
    {"operator", "BonfyreModel", NULL, "model", "Model manages model lifecycle"},
    {"operator", "BonfyreLayer", NULL, "layer", "Layer operates on layer artifacts"},
    {"operator", "BonfyreSLI", NULL, "sli", "SLI performs structured layer inference"},
    {"operator", "BonfyreFPQ", NULL, "fpq", "FPQ performs precision/compression routing"},
    {"operator", "BonfyreFPQx", NULL, "fpq", "FPQx extends FPQ routes"},
    {"operator", "BonfyreRecipe", NULL, "recipe", "Recipe manages recipe registry and execution metadata"},
    {"operator", "BonfyreRun", NULL, "run", "Run executes recipes"},
    {"step_name", NULL, "transcrib", "transcribe", "Text match on workflow step name"},
    {"step_name", NULL, "clean", "clean", "Text match on workflow step name"},
    {"step_name", NULL, "paragraph", "paragraph", "Text match on workflow step name"},
    {"step_name", NULL, "brief|summary", "brief", "Text match on workflow step name"},
    {"step_name", NULL, "proof|verif", "proof", "Text match on workflow step name"},
    {"step_name", NULL, "offer|quote", "offer", "Text match on workflow step name"},
    {"step_name", NULL, "narrat|tts|speech", "narrate", "Text match on workflow step name"},
    {"step_name", NULL, "pack|final", "pack", "Text match on workflow step name"},
    {"step_name", NULL, "distribut|deliver", "distribute", "Text match on workflow step name"},
    {"step_name", NULL, "embed", "embed", "Text match on workflow step name"},
    {"step_name", NULL, "segment", "segment", "Text match on workflow step name"},
    {"step_name", NULL, "model|train", "model", "Text match on workflow step name"},
    {"step_name", NULL, "layer", "layer", "Text match on workflow step name"},
    {"step_name", NULL, "sli|lattice", "sli", "Text match on workflow step name"},
    {"step_name", NULL, "fpq|quant", "fpq", "Text match on workflow step name"},
    {NULL, NULL, NULL, NULL, NULL}
};

typedef struct {
    const char *step;
    const char *kind;
    const char *target;
    const char *effect;
} bf_catalog_projection_rule_t;

static const bf_catalog_projection_rule_t BF_CATALOG_PROJECTION_RULES[] = {
    {"delete family", "delete_scope", "family:*", "clear existing family projection before rebuild"},
    {"delete workflow", "delete_scope", "workflow:*", "clear existing workflow projection before rebuild"},
    {"delete workflow-step", "delete_scope", "workflow-step:*", "clear existing workflow-step projection before rebuild"},
    {"delete capability", "delete_scope", "capability:*", "clear existing capability projection before rebuild"},
    {"delete model", "delete_scope", "model:*", "clear existing model projection before rebuild"},
    {"delete model-source", "delete_scope", "model-source:*", "clear existing model-source projection before rebuild"},
    {"delete layer", "delete_scope", "layer:*", "clear existing layer projection before rebuild"},
    {"delete recipe", "delete_scope", "recipe:*", "clear existing recipe projection before rebuild"},
    {"sync family index", "sync", "catalog_nodes(kind=family)", "seed family node projection from family index"},
    {"sync capability registry", "sync", "catalog_nodes(kind=capability)", "project capability registry into catalog"},
    {"sync model registry", "sync", "catalog_nodes(kind=model|model_source)", "project model metadata into catalog"},
    {"sync layer registry", "sync", "catalog_nodes(kind=layer)", "project layer registry into catalog"},
    {"sync docs recipes", "sync", "catalog_nodes(kind=workflow)", "project docs/recipes json workflows"},
    {"sync json recipes", "sync", "catalog_nodes(kind=recipe)", "project json recipes into catalog"},
    {"sync yaml google", "sync", "catalog_nodes(kind=recipe)", "project google yaml recipes into catalog"},
    {"sync yaml topology", "sync", "catalog_nodes(kind=recipe)", "project topology yaml recipes into catalog"},
    {"sync yaml cross_fusion", "sync", "catalog_nodes(kind=recipe)", "project cross_fusion yaml recipes into catalog"},
    {"sync surfaces and transports", "sync", "catalog_nodes(kind=surface|transport)", "project operator-contract-bindings.tsv surface/transport families into catalog"},
    {"seed family relationships", "derive", "catalog_edges(rel=specialized_by_family|related_family)", "seed family relationships into graph"},
    {"derive capability-family relationships", "derive", "catalog_edges(rel=related_family|supports_capability)", "derive capability-family links from workflow-step/family evidence"},
    {"rebuild fts", "index", "catalog_fts", "refresh full-text search projection"},
    {NULL, NULL, NULL, NULL}
};

static int bf_is_dir(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int bf_is_file(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static void bf_append_json_string(char *dst, size_t sz, const char *src) {
    size_t len = strlen(dst);
    const unsigned char *p = (const unsigned char *)(src ? src : "");
    if (len + 2 >= sz) return;
    dst[len++] = '"';
    while (*p && len + 8 < sz) {
        unsigned char c = *p++;
        switch (c) {
            case '\\': dst[len++] = '\\'; dst[len++] = '\\'; break;
            case '"':  dst[len++] = '\\'; dst[len++] = '"'; break;
            case '\n': dst[len++] = '\\'; dst[len++] = 'n'; break;
            case '\r': dst[len++] = '\\'; dst[len++] = 'r'; break;
            case '\t': dst[len++] = '\\'; dst[len++] = 't'; break;
            default:
                if (c < 0x20) {
                    len += (size_t)snprintf(dst + len, sz - len, "\\u%04x", (unsigned)c);
                } else {
                    dst[len++] = (char)c;
                }
                break;
        }
    }
    if (len < sz - 1) dst[len++] = '"';
    dst[len] = '\0';
}

static void join_path(char *buf, size_t sz, const char *a, const char *b) {
    if (!a || !a[0]) { snprintf(buf, sz, "%s", b ? b : ""); return; }
    if (!b || !b[0]) { snprintf(buf, sz, "%s", a); return; }
    snprintf(buf, sz, "%s/%s", a, b);
}

static int load_json_doc(const char *path, char **json_out, bf_json_doc_t **doc_out) {
    char err[256];
    char *json = bf_read_file(path, NULL);
    bf_json_doc_t *doc;
    if (!json) return 0;
    doc = bf_json_parse_str(json, err, sizeof(err));
    if (!doc) {
        free(json);
        return 0;
    }
    *json_out = json;
    *doc_out = doc;
    return 1;
}

static char *load_text_file(const char *path) {
    return bf_read_file(path, NULL);
}

static void node_str_copy(const bf_json_node_t *n, char *out, size_t out_sz) {
    if (!out || out_sz == 0) return;
    out[0] = '\0';
    if (!n) return;
    bf_json_get_str_copy(n, out, out_sz);
}

static int begin_tx(sqlite3 *db) {
    return sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) == SQLITE_OK ? 0 : 1;
}

static void end_tx(sqlite3 *db, int ok) {
    sqlite3_exec(db, ok ? "COMMIT" : "ROLLBACK", NULL, NULL, NULL);
}

static int upsert_node(sqlite3 *db,
                       const char *node_id,
                       const char *kind,
                       const char *external_id,
                       const char *name,
                       const char *category,
                       const char *summary,
                       const char *source_path,
                       const char *source_hash,
                       const char *json_data) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO catalog_nodes(node_id,kind,external_id,name,category,summary,source_path,source_hash,json_data,updated_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(node_id) DO UPDATE SET "
        "kind=excluded.kind, external_id=excluded.external_id, name=excluded.name, "
        "category=excluded.category, summary=excluded.summary, source_path=excluded.source_path, "
        "source_hash=excluded.source_hash, json_data=excluded.json_data, updated_at=excluded.updated_at",
        -1, &st, NULL) != SQLITE_OK) return 1;

    sqlite3_bind_text(st, 1, node_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, external_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, name ? name : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, category ? category : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 6, summary ? summary : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7, source_path ? source_path : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 8, source_hash ? source_hash : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 9, json_data ? json_data : "", -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 10, (sqlite3_int64)time(NULL));
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

static int upsert_edge(sqlite3 *db, const char *src, const char *rel, const char *dst, const char *meta) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO catalog_edges(src_node_id, rel, dst_node_id, meta) VALUES(?,?,?,?) "
        "ON CONFLICT(src_node_id, rel, dst_node_id) DO UPDATE SET meta=excluded.meta",
        -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, src, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, rel, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, dst, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, meta ? meta : "", -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

static void rebuild_fts(sqlite3 *db) {
    sqlite3_exec(db, "INSERT INTO catalog_fts(catalog_fts) VALUES('rebuild')", NULL, NULL, NULL);
}

static void trim_token(char *s) {
    char *start = s;
    char *end;
    if (!s || !s[0]) return;
    while (*start && (*start == ' ' || *start == '\t' || *start == '\n')) start++;
    if (start != s) memmove(s, start, strlen(start) + 1);
    end = s + strlen(s);
    while (end > s && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n')) end--;
    *end = '\0';
}

static int yaml_line_key_value(const char *line, const char *key, char *out, size_t out_sz) {
    size_t key_len;
    const char *p;
    size_t len;
    if (!line || !key || !out || out_sz == 0) return 0;
    out[0] = '\0';
    while (*line == ' ' || *line == '\t') line++;
    key_len = strlen(key);
    if (strncmp(line, key, key_len) != 0) return 0;
    p = line + key_len;
    while (*p == ' ' || *p == '\t') p++;
    if (*p != ':') return 0;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    len = strlen(p);
    while (len > 0 && (p[len - 1] == '\r' || p[len - 1] == '\n')) len--;
    if (len >= out_sz) len = out_sz - 1;
    memcpy(out, p, len);
    out[len] = '\0';
    trim_token(out);
    if (out[0] == '"' || out[0] == '\'') {
        size_t out_len = strlen(out);
        if (out_len >= 2 && out[out_len - 1] == out[0]) {
            memmove(out, out + 1, out_len - 2);
            out[out_len - 2] = '\0';
        }
    }
    return out[0] != '\0';
}

static int extract_yaml_scalar(const char *text, const char *key, char *out, size_t out_sz) {
    const char *line = text;
    if (!text || !key || !out || out_sz == 0) return 0;
    out[0] = '\0';
    while (*line) {
        const char *next = strchr(line, '\n');
        char buf[2048];
        size_t len = next ? (size_t)(next - line) : strlen(line);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, line, len);
        buf[len] = '\0';
        if (yaml_line_key_value(buf, key, out, out_sz)) return 1;
        if (!next) break;
        line = next + 1;
    }
    return 0;
}

static int collect_yaml_list_preview(const char *text, const char *key, char *out, size_t out_sz) {
    const char *line = text;
    int in_block = 0;
    size_t used = 0;
    int count = 0;
    char key_pattern[128];
    if (!text || !key || !out || out_sz == 0) return 0;
    out[0] = '\0';
    snprintf(key_pattern, sizeof(key_pattern), "%s:", key);
    while (*line) {
        const char *next = strchr(line, '\n');
        char buf[2048];
        char item[512];
        const char *trimmed;
        size_t len = next ? (size_t)(next - line) : strlen(line);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, line, len);
        buf[len] = '\0';
        trimmed = buf;
        while (*trimmed == ' ' || *trimmed == '\t') trimmed++;
        if (!in_block) {
            if (strncmp(trimmed, key_pattern, strlen(key_pattern)) == 0) in_block = 1;
        } else {
            if (*trimmed == '-' && (trimmed[1] == ' ' || trimmed[1] == '\t')) {
                snprintf(item, sizeof(item), "%s", trimmed + 1);
                trim_token(item);
                if (count > 0 && used + 2 < out_sz) {
                    out[used++] = ',';
                    out[used++] = ' ';
                    out[used] = '\0';
                }
                len = strlen(item);
                if (used + len >= out_sz) len = out_sz - used - 1;
                memcpy(out + used, item, len);
                used += len;
                out[used] = '\0';
                count++;
            } else if (*trimmed && *trimmed != '#') {
                break;
            }
        }
        if (!next) break;
        line = next + 1;
    }
    return count;
}

static int collect_yaml_list_items(const char *text, const char *key,
                                   char items[][256], int max_items) {
    const char *line = text;
    int in_block = 0;
    int count = 0;
    char key_pattern[128];
    if (!text || !key || !items || max_items <= 0) return 0;
    snprintf(key_pattern, sizeof(key_pattern), "%s:", key);
    while (*line) {
        const char *next = strchr(line, '\n');
        char buf[2048];
        char item[512];
        const char *trimmed;
        size_t len = next ? (size_t)(next - line) : strlen(line);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, line, len);
        buf[len] = '\0';
        trimmed = buf;
        while (*trimmed == ' ' || *trimmed == '\t') trimmed++;
        if (!in_block) {
            if (strncmp(trimmed, key_pattern, strlen(key_pattern)) == 0) in_block = 1;
        } else {
            if (*trimmed == '-' && (trimmed[1] == ' ' || trimmed[1] == '\t')) {
                size_t vlen;
                snprintf(item, sizeof(item), "%s", trimmed + 1);
                trim_token(item);
                if (count < max_items && item[0]) {
                    vlen = strlen(item);
                    if (vlen >= 256) vlen = 255;
                    memcpy(items[count], item, vlen);
                    items[count][vlen] = '\0';
                    count++;
                }
            } else if (*trimmed && *trimmed != '#') {
                break;
            }
        }
        if (!next) break;
        line = next + 1;
    }
    return count;
}

static int is_yaml_path(const char *name) {
    size_t len;
    if (!name) return 0;
    len = strlen(name);
    return (len > 5 && strcmp(name + len - 5, ".yaml") == 0)
        || (len > 4 && strcmp(name + len - 4, ".yml") == 0);
}

static int line_indent_width(const char *line) {
    int width = 0;
    if (!line) return 0;
    while (*line == ' ') {
        width++;
        line++;
    }
    return width;
}

static int collect_yaml_mapping_values(const char *text, const char *key,
                                       char items[][128], int max_items) {
    const char *line = text;
    int in_block = 0;
    int block_indent = 0;
    int count = 0;
    char key_pattern[128];
    if (!text || !key || !items || max_items <= 0) return 0;
    snprintf(key_pattern, sizeof(key_pattern), "%s:", key);
    while (*line) {
        const char *next = strchr(line, '\n');
        char buf[2048];
        char item[256];
        const char *trimmed;
        int indent;
        size_t len = next ? (size_t)(next - line) : strlen(line);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, line, len);
        buf[len] = '\0';
        indent = line_indent_width(buf);
        trimmed = buf + indent;
        if (!in_block) {
            if (strncmp(trimmed, key_pattern, strlen(key_pattern)) == 0) {
                in_block = 1;
                block_indent = indent;
            }
        } else {
            if (*trimmed == '\0' || *trimmed == '#') {
                /* keep scanning */
            } else if (indent <= block_indent) {
                break;
            } else {
                const char *colon = strchr(trimmed, ':');
                if (colon) {
                    size_t vlen;
                    snprintf(item, sizeof(item), "%s", colon + 1);
                    trim_token(item);
                    if (item[0] == '"' || item[0] == '\'') {
                        size_t item_len = strlen(item);
                        if (item_len >= 2 && item[item_len - 1] == item[0]) {
                            memmove(item, item + 1, item_len - 2);
                            item[item_len - 2] = '\0';
                        }
                    }
                    if (item[0] && count < max_items) {
                        vlen = strlen(item);
                        if (vlen >= 128) vlen = 127;
                        memcpy(items[count], item, vlen);
                        items[count][vlen] = '\0';
                        count++;
                    }
                }
            }
        }
        if (!next) break;
        line = next + 1;
    }
    return count;
}

static int collect_yaml_mapping_keys(const char *text, const char *key,
                                     char items[][128], int max_items) {
    const char *line = text;
    int in_block = 0;
    int block_indent = 0;
    int count = 0;
    char key_pattern[128];
    if (!text || !key || !items || max_items <= 0) return 0;
    snprintf(key_pattern, sizeof(key_pattern), "%s:", key);
    while (*line) {
        const char *next = strchr(line, '\n');
        char buf[2048];
        char item[256];
        const char *trimmed;
        const char *colon;
        int indent;
        size_t len = next ? (size_t)(next - line) : strlen(line);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, line, len);
        buf[len] = '\0';
        indent = line_indent_width(buf);
        trimmed = buf + indent;
        if (!in_block) {
            if (strncmp(trimmed, key_pattern, strlen(key_pattern)) == 0) {
                in_block = 1;
                block_indent = indent;
            }
        } else {
            if (*trimmed == '\0' || *trimmed == '#') {
                /* keep scanning */
            } else if (indent <= block_indent) {
                break;
            } else {
                colon = strchr(trimmed, ':');
                if (colon) {
                    size_t klen = (size_t)(colon - trimmed);
                    if (klen >= sizeof(item)) klen = sizeof(item) - 1;
                    memcpy(item, trimmed, klen);
                    item[klen] = '\0';
                    trim_token(item);
                    if (item[0] == '"' || item[0] == '\'') {
                        size_t item_len = strlen(item);
                        if (item_len >= 2 && item[item_len - 1] == item[0]) {
                            memmove(item, item + 1, item_len - 2);
                            item[item_len - 2] = '\0';
                        }
                    }
                    if (item[0] && count < max_items) {
                        snprintf(items[count], 128, "%s", item);
                        count++;
                    }
                }
            }
        }
        if (!next) break;
        line = next + 1;
    }
    return count;
}

static int extract_json_string_after(const char *text, const char *key, char *out, size_t out_sz) {
    char needle[128];
    const char *p;
    const char *q;
    size_t len;
    if (!text || !key || !out || out_sz == 0) return 0;
    out[0] = '\0';
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = strstr(text, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && *p != ':') p++;
    if (*p != ':') return 0;
    p++;
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (*p != '"') return 0;
    p++;
    q = p;
    while (*q && !(*q == '"' && q[-1] != '\\')) q++;
    len = (size_t)(q - p);
    if (len >= out_sz) len = out_sz - 1;
    memcpy(out, p, len);
    out[len] = '\0';
    return out[0] != '\0';
}

static void default_capability_db_path(char *buf, size_t sz) {
    const char *env = getenv("BONFYRE_CAPABILITY_DB");
    const char *home = getenv("HOME");
    if (env && env[0]) {
        if (bf_is_dir(env)) snprintf(buf, sz, "%s/%s", env, BF_CAPABILITY_DB_FALLBACK_FILE);
        else snprintf(buf, sz, "%s", env);
        return;
    }
    if (!home) home = "/tmp";
    snprintf(buf, sz, "%s%s", home, BF_CAPABILITY_DB_SUBPATH);
    if (bf_is_dir(buf)) {
        char nested[PATH_MAX];
        snprintf(nested, sizeof(nested), "%s/%s", buf, BF_CAPABILITY_DB_FALLBACK_FILE);
        snprintf(buf, sz, "%s", nested);
    }
}

static void default_model_db_path(char *buf, size_t sz) {
    const char *env = getenv("BONFYRE_MODEL_DB");
    const char *home = getenv("HOME");
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    if (!home) home = "/tmp";
    snprintf(buf, sz, "%s%s", home, BF_MODEL_DB_SUBPATH);
}

static void default_layer_db_path(char *buf, size_t sz) {
    const char *env = getenv("BONFYRE_LAYER_DB");
    const char *home = getenv("HOME");
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    if (!home) home = "/tmp";
    snprintf(buf, sz, "%s%s", home, BF_LAYER_DB_SUBPATH);
}

static int text_contains_ci(const char *text, const char *needle) {
    size_t nlen;
    if (!text || !needle || !needle[0]) return 0;
    nlen = strlen(needle);
    for (; *text; text++) {
        if (strncasecmp(text, needle, nlen) == 0) return 1;
    }
    return 0;
}

static const char *infer_step_capability_id(const char *op, const char *step_name) {
    if (op && strcmp(op, "BonfyreTranscribe") == 0) {
        if (step_name && (strstr(step_name, "Cleanup") || strstr(step_name, "Clean"))) return "clean";
        return "transcribe";
    }
    if (op && (strcmp(op, "BonfyreSpeechLoop") == 0 || strcmp(op, "BonfyreSegment") == 0)) return "segment";
    if (op && (strcmp(op, "BonfyreBrief") == 0 || strcmp(op, "BonfyreCodeBrief") == 0)) return "brief";
    if (op && strcmp(op, "BonfyreProof") == 0) return "proof";
    if (op && strcmp(op, "BonfyreOffer") == 0) return "offer";
    if (op && strcmp(op, "BonfyreNarrate") == 0) return "narrate";
    if (op && (strcmp(op, "BonfyrePack") == 0 || strcmp(op, "BonfyrePackage") == 0)) return "pack";
    if (op && strcmp(op, "BonfyreDistribute") == 0) return "distribute";
    if (op && strcmp(op, "BonfyreEmbed") == 0) return "embed";
    if (op && strcmp(op, "BonfyreModel") == 0) return "model";
    if (op && strcmp(op, "BonfyreLayer") == 0) return "layer";
    if (op && strcmp(op, "BonfyreSLI") == 0) return "sli";
    if (op && (strcmp(op, "BonfyreFPQ") == 0 || strcmp(op, "BonfyreFPQx") == 0)) return "fpq";
    if (op && strcmp(op, "BonfyreRecipe") == 0) return "recipe";
    if (op && strcmp(op, "BonfyreRun") == 0) return "run";

    if (!step_name || !step_name[0]) return NULL;
    if (text_contains_ci(step_name, "transcrib")) return "transcribe";
    if (text_contains_ci(step_name, "clean")) return "clean";
    if (text_contains_ci(step_name, "paragraph")) return "paragraph";
    if (text_contains_ci(step_name, "brief") || text_contains_ci(step_name, "summary")) return "brief";
    if (text_contains_ci(step_name, "proof") || text_contains_ci(step_name, "verif")) return "proof";
    if (text_contains_ci(step_name, "offer") || text_contains_ci(step_name, "quote")) return "offer";
    if (text_contains_ci(step_name, "narrat") || text_contains_ci(step_name, "tts") || text_contains_ci(step_name, "speech")) return "narrate";
    if (text_contains_ci(step_name, "pack") || text_contains_ci(step_name, "final")) return "pack";
    if (text_contains_ci(step_name, "distribut") || text_contains_ci(step_name, "deliver")) return "distribute";
    if (text_contains_ci(step_name, "embed")) return "embed";
    if (text_contains_ci(step_name, "segment")) return "segment";
    if (text_contains_ci(step_name, "model") || text_contains_ci(step_name, "train")) return "model";
    if (text_contains_ci(step_name, "layer")) return "layer";
    if (text_contains_ci(step_name, "sli") || text_contains_ci(step_name, "lattice")) return "sli";
    if (text_contains_ci(step_name, "fpq") || text_contains_ci(step_name, "quant")) return "fpq";
    return NULL;
}

int bf_catalog_capability_tagging_rules_json(const char *filter, char **out_json) {
    size_t cap = 16384, len = 0;
    char *buf;
    int first = 1;
    int count = 0;
    const char *f = (filter && filter[0]) ? filter : NULL;
    if (!out_json) return 1;
    *out_json = NULL;
    buf = (char *)malloc(cap);
    if (!buf) return 1;
    len += (size_t)snprintf(buf + len, cap - len, "{");
    len += (size_t)snprintf(buf + len, cap - len, "\"filter\":");
    if (f) {
        bf_append_json_string(buf + len, cap - len, f);
        len = strlen(buf);
    } else {
        len += (size_t)snprintf(buf + len, cap - len, "null");
    }
    len += (size_t)snprintf(buf + len, cap - len, ",\"rules\":[");
    for (const bf_catalog_capability_rule_t *r = BF_CAPABILITY_TAGGING_RULES; r->kind; r++) {
        if (f && !text_contains_ci(r->kind, f) &&
            !(r->src && text_contains_ci(r->src, f)) &&
            !(r->step_name_contains && text_contains_ci(r->step_name_contains, f)) &&
            !(r->capability_id && text_contains_ci(r->capability_id, f)) &&
            !(r->reason && text_contains_ci(r->reason, f))) {
            continue;
        }
        if (!first) len += (size_t)snprintf(buf + len, cap - len, ",");
        first = 0;
        count++;
        len += (size_t)snprintf(buf + len, cap - len, "{");
        len += (size_t)snprintf(buf + len, cap - len, "\"kind\":");
        bf_append_json_string(buf + len, cap - len, r->kind ? r->kind : "");
        len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, ",\"source_operator\":");
        if (r->src) { bf_append_json_string(buf + len, cap - len, r->src); len = strlen(buf); }
        else len += (size_t)snprintf(buf + len, cap - len, "null");
        len += (size_t)snprintf(buf + len, cap - len, ",\"step_name_contains\":");
        if (r->step_name_contains) { bf_append_json_string(buf + len, cap - len, r->step_name_contains); len = strlen(buf); }
        else len += (size_t)snprintf(buf + len, cap - len, "null");
        len += (size_t)snprintf(buf + len, cap - len, ",\"capability_id\":");
        bf_append_json_string(buf + len, cap - len, r->capability_id ? r->capability_id : "");
        len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, ",\"reason\":");
        bf_append_json_string(buf + len, cap - len, r->reason ? r->reason : "");
        len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, "}");
    }
    len += (size_t)snprintf(buf + len, cap - len, "],\"count\":%d}", count);
    *out_json = buf;
    return 0;
}

int bf_catalog_projection_rules_json(char **out_json) {
    size_t cap = 16384, len = 0;
    char *buf;
    int count = 0;
    if (!out_json) return 1;
    *out_json = NULL;
    buf = (char *)malloc(cap);
    if (!buf) return 1;
    len += (size_t)snprintf(buf + len, cap - len, "{\"rules\":[");
    for (const bf_catalog_projection_rule_t *r = BF_CATALOG_PROJECTION_RULES; r->step; r++) {
        if (count) len += (size_t)snprintf(buf + len, cap - len, ",");
        len += (size_t)snprintf(buf + len, cap - len, "{");
        len += (size_t)snprintf(buf + len, cap - len, "\"step\":");
        bf_append_json_string(buf + len, cap - len, r->step); len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, ",\"kind\":");
        bf_append_json_string(buf + len, cap - len, r->kind); len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, ",\"target\":");
        bf_append_json_string(buf + len, cap - len, r->target); len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, ",\"effect\":");
        bf_append_json_string(buf + len, cap - len, r->effect); len = strlen(buf);
        len += (size_t)snprintf(buf + len, cap - len, "}");
        count++;
    }
    len += (size_t)snprintf(buf + len, cap - len, "],\"count\":%d}", count);
    *out_json = buf;
    return 0;
}

static int delete_scope(sqlite3 *db, const char *prefix) {
    sqlite3_stmt *st = NULL;
    char likepat[128];
    snprintf(likepat, sizeof(likepat), "%s%%", prefix);
    sqlite3_prepare_v2(db, "DELETE FROM catalog_edges WHERE src_node_id LIKE ? OR dst_node_id LIKE ?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, likepat, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, likepat, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_prepare_v2(db, "DELETE FROM catalog_nodes WHERE node_id LIKE ?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, likepat, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

static const char *category_display_name(const char *key) {
    if (strcmp(key, "transform_families") == 0) return "Transform Families";
    if (strcmp(key, "model_structure_families") == 0) return "Model Structure Families";
    if (strcmp(key, "hybrid_families") == 0) return "Hybrid Families";
    if (strcmp(key, "ttt_families") == 0) return "TTT Families";
    if (strcmp(key, "quant_pack_families") == 0) return "Quant / Pack Families";
    if (strcmp(key, "pipeline_families") == 0) return "Pipeline Families";
    if (strcmp(key, "control_families") == 0) return "Control Families";
    if (strcmp(key, "meta_recipe_families") == 0) return "Meta Families";
    return "Families";
}

static const char *family_category_summary(const char *key) {
    if (strcmp(key, "transform_families") == 0) return "Step-local transform specialization.";
    if (strcmp(key, "model_structure_families") == 0) return "Model and layer structural specialization.";
    if (strcmp(key, "hybrid_families") == 0) return "Hybrid retrieval and sidecar specialization.";
    if (strcmp(key, "ttt_families") == 0) return "Tuning-time specialization.";
    if (strcmp(key, "quant_pack_families") == 0) return "Quantization and packed-artifact specialization.";
    if (strcmp(key, "pipeline_families") == 0) return "Pipeline-shape specialization.";
    if (strcmp(key, "control_families") == 0) return "Control-plane specialization.";
    if (strcmp(key, "meta_recipe_families") == 0) return "Meta-level generator and evaluator specialization.";
    return "Bonfyre specialization family.";
}

static int sync_family_index(sqlite3 *db, const char *repo_root) {
    static const char *keys[] = {
        "transform_families", "model_structure_families", "hybrid_families", "ttt_families",
        "quant_pack_families", "pipeline_families", "control_families", "meta_recipe_families", NULL
    };
    char path[PATH_MAX];
    char *json = NULL;
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root;

    join_path(path, sizeof(path), repo_root, "recipes/families/family_index.json");
    if (!load_json_doc(path, &json, &doc)) return 1;
    root = bf_json_root(doc);
    if (!root || root->type != BF_JSON_OBJECT) { bf_json_free(doc); free(json); return 1; }

    {
        const char *p = json;
        int inserted = 0;
        while ((p = strstr(p, "\"family\"")) != NULL) {
            char fam_id[128];
            char category[128];
            char summary[512];
            char node_id[160];
            category[0] = '\0';
            summary[0] = '\0';
            if (!extract_json_string_after(p, "family", fam_id, sizeof(fam_id))) {
                p += 8;
                continue;
            }
            extract_json_string_after(p, "category", category, sizeof(category));
            extract_json_string_after(p, "notes", summary, sizeof(summary));
            if (!category[0]) snprintf(category, sizeof(category), "%s", "HF Tensor Families");
            if (!summary[0]) snprintf(summary, sizeof(summary), "%s", "Bonfyre tensor family.");
            snprintf(node_id, sizeof(node_id), "family:%s", fam_id);
            upsert_node(db, node_id, "family", fam_id, fam_id, category, summary, path, "", "");
            inserted++;
            p += 8;
        }
        if (inserted > 0) {
            bf_json_free(doc);
            free(json);
            return 0;
        }
    }

    for (int i = 0; keys[i]; i++) {
        const bf_json_node_t *arr = bf_json_obj_get(doc, root, keys[i]);
        if (!arr || arr->type != BF_JSON_ARRAY) continue;
        for (const bf_json_node_t *child = bf_json_child_first(doc, arr); child; child = bf_json_child_next(doc, child)) {
            char fam_id[128];
            char node_id[160];
            node_str_copy(child, fam_id, sizeof(fam_id));
            if (!fam_id[0]) continue;
            snprintf(node_id, sizeof(node_id), "family:%s", fam_id);
            upsert_node(db, node_id, "family", fam_id, fam_id, category_display_name(keys[i]),
                        family_category_summary(keys[i]), path, "", "");
        }
    }

    bf_json_free(doc);
    free(json);
    return 0;
}

static int sync_json_dir(sqlite3 *db, const char *repo_root, const char *rel_dir, const char *kind) {
    char dirpath[PATH_MAX];
    DIR *dp;
    struct dirent *de;

    join_path(dirpath, sizeof(dirpath), repo_root, rel_dir);
    dp = opendir(dirpath);
    if (!dp) return 1;

    while ((de = readdir(dp)) != NULL) {
        char path[PATH_MAX];
        char *json = NULL;
        bf_json_doc_t *doc = NULL;
        const bf_json_node_t *root;
        const bf_json_node_t *idn;
        const bf_json_node_t *namen;
        const bf_json_node_t *descn;
        const bf_json_node_t *catn;
        char ext_id[128], name[256], summary[1024], category[128], node_id[192], hash[65];
        size_t len = strlen(de->d_name);

        if (len < 6 || strcmp(de->d_name + len - 5, ".json") != 0) continue;
        join_path(path, sizeof(path), dirpath, de->d_name);
        if (!load_json_doc(path, &json, &doc)) continue;
        root = bf_json_root(doc);
        if (!root || root->type != BF_JSON_OBJECT) { bf_json_free(doc); free(json); continue; }

        if (strcmp(kind, "recipe") == 0) {
            idn = bf_json_obj_get(doc, root, "code");
            if (!idn) idn = bf_json_obj_get(doc, root, "recipe_id");
            if (!idn) idn = bf_json_obj_get(doc, root, "id");
            if (!idn) idn = bf_json_obj_get(doc, root, "recipe");
        } else {
            idn = bf_json_obj_get(doc, root, "recipe_id");
            if (!idn) idn = bf_json_obj_get(doc, root, "id");
            if (!idn) idn = bf_json_obj_get(doc, root, "code");
            if (!idn) idn = bf_json_obj_get(doc, root, "recipe");
        }
        namen = bf_json_obj_get(doc, root, "name");
        descn = bf_json_obj_get(doc, root, "description");
        catn = bf_json_obj_get(doc, root, "category");

        node_str_copy(idn, ext_id, sizeof(ext_id));
        node_str_copy(namen, name, sizeof(name));
        node_str_copy(descn, summary, sizeof(summary));
        node_str_copy(catn, category, sizeof(category));
        if (!ext_id[0]) { bf_json_free(doc); free(json); continue; }

        bf_sha256_hex((const uint8_t *)json, strlen(json), hash);
        snprintf(node_id, sizeof(node_id), "%s:%s", kind, ext_id);
        upsert_node(db, node_id, kind, ext_id, name, category, summary, path, hash, json);

        if (strcmp(kind, "workflow") == 0) {
            const bf_json_node_t *stages = bf_json_obj_get(doc, root, "stages");
            const bf_json_node_t *models = bf_json_obj_get(doc, root, "models");
            if (stages && stages->type == BF_JSON_ARRAY) {
                for (const bf_json_node_t *child = bf_json_child_first(doc, stages); child; child = bf_json_child_next(doc, child)) {
                    const bf_json_node_t *sid = bf_json_obj_get(doc, child, "id");
                    const bf_json_node_t *sname = bf_json_obj_get(doc, child, "name");
                    const bf_json_node_t *sop = bf_json_obj_get(doc, child, "operator");
                    char step_id[128], step_name[256], step_op[256], step_node_id[256], step_external_id[256];
                    node_str_copy(sid, step_id, sizeof(step_id));
                    node_str_copy(sname, step_name, sizeof(step_name));
                    node_str_copy(sop, step_op, sizeof(step_op));
                    if (!step_id[0]) continue;
                    snprintf(step_node_id, sizeof(step_node_id), "workflow-step:%s:%s", ext_id, step_id);
                    snprintf(step_external_id, sizeof(step_external_id), "%s:%s", ext_id, step_id);
                    upsert_node(db, step_node_id, "workflow_step", step_external_id, step_name, step_op, step_name, path, hash, "");
                    upsert_edge(db, node_id, "has_step", step_node_id, "");
                    {
                        const char *cap_id = infer_step_capability_id(step_op, step_name);
                        if (cap_id && cap_id[0]) {
                            char capability_node_id[160];
                            snprintf(capability_node_id, sizeof(capability_node_id), "capability:%s", cap_id);
                            upsert_edge(db, step_node_id, "implements_capability", capability_node_id, "");
                            upsert_edge(db, capability_node_id, "seen_in_workflow_step", step_node_id, "");
                        }
                    }
                }
            }
            if (models && models->type == BF_JSON_ARRAY) {
                for (const bf_json_node_t *child = bf_json_child_first(doc, models); child; child = bf_json_child_next(doc, child)) {
                    const bf_json_node_t *mid = bf_json_obj_get(doc, child, "id");
                    const bf_json_node_t *mname = bf_json_obj_get(doc, child, "name");
                    char model_id[128], model_node_id[192];
                    node_str_copy(mid ? mid : mname, model_id, sizeof(model_id));
                    if (!model_id[0]) continue;
                    snprintf(model_node_id, sizeof(model_node_id), "model:%s", model_id);
                    upsert_edge(db, node_id, "uses_model", model_node_id, "");
                }
            }
        }

        bf_json_free(doc);
        free(json);
    }

    closedir(dp);
    return 0;
}

static int sync_yaml_dir(sqlite3 *db, const char *repo_root, const char *rel_dir, const char *kind) {
    char dirpath[PATH_MAX];
    DIR *dp;
    struct dirent *de;

    join_path(dirpath, sizeof(dirpath), repo_root, rel_dir);
    dp = opendir(dirpath);
    if (!dp) return 1;

    while ((de = readdir(dp)) != NULL) {
        char path[PATH_MAX];
        char *yaml = NULL;
        char ext_id[128];
        char name[256];
        char category[128];
        char summary[1024];
        char source_model[256];
        char source_collection[128];
        char capabilities[256];
        char hash[65];
        char node_id[192];
        size_t name_len;
        char family_values[128][128];
        char family_patterns[128][128];
        char pull_items[256][256];
        char workflow_step_ids[64][128];
        char workflow_step_names[64][128];
        int family_count = 0;
        int pull_count = 0;
        int workflow_step_count = 0;

        if (!is_yaml_path(de->d_name)) continue;
        join_path(path, sizeof(path), dirpath, de->d_name);
        yaml = load_text_file(path);
        if (!yaml) continue;

        ext_id[0] = '\0';
        name[0] = '\0';
        category[0] = '\0';
        summary[0] = '\0';
        source_model[0] = '\0';
        source_collection[0] = '\0';
        capabilities[0] = '\0';

        extract_yaml_scalar(yaml, "recipe", ext_id, sizeof(ext_id));
        extract_yaml_scalar(yaml, "name", name, sizeof(name));
        extract_yaml_scalar(yaml, "source_model", source_model, sizeof(source_model));
        extract_yaml_scalar(yaml, "source_collection", source_collection, sizeof(source_collection));
        collect_yaml_list_preview(yaml, "capabilities", capabilities, sizeof(capabilities));
        if (!ext_id[0]) {
            snprintf(ext_id, sizeof(ext_id), "%s", de->d_name);
            name_len = strlen(ext_id);
            if (name_len > 5 && strcmp(ext_id + name_len - 5, ".yaml") == 0) ext_id[name_len - 5] = '\0';
            else if (name_len > 4 && strcmp(ext_id + name_len - 4, ".yml") == 0) ext_id[name_len - 4] = '\0';
        }
        if (!name[0]) snprintf(name, sizeof(name), "%s", ext_id);
        if (source_collection[0]) snprintf(category, sizeof(category), "%s", source_collection);
        else snprintf(category, sizeof(category), "%s", rel_dir);
        if (source_model[0] && capabilities[0]) {
            snprintf(summary, sizeof(summary), "source_model=%s; capabilities=%s", source_model, capabilities);
        } else if (source_model[0]) {
            snprintf(summary, sizeof(summary), "source_model=%s", source_model);
        } else if (capabilities[0]) {
            snprintf(summary, sizeof(summary), "capabilities=%s", capabilities);
        }

        bf_sha256_hex((const uint8_t *)yaml, strlen(yaml), hash);
        snprintf(node_id, sizeof(node_id), "%s:%s", kind, ext_id);
        upsert_node(db, node_id, kind, ext_id, name, category, summary, path, hash, yaml);

        if (strcmp(kind, "recipe") == 0) {
            char model_node_id[256];
            char model_json[1024];
            char main_family[128];

            main_family[0] = '\0';
            extract_yaml_scalar(yaml, "family", main_family, sizeof(main_family));
            family_count = collect_yaml_mapping_values(yaml, "bonfyre_families", family_values, 128);
            collect_yaml_mapping_keys(yaml, "bonfyre_families", family_patterns, 128);
            if (main_family[0] && family_count < 128) {
                snprintf(family_values[family_count], sizeof(family_values[family_count]), "%s", main_family);
                family_patterns[family_count][0] = '\0';
                family_count++;
            }
            if (source_model[0]) {
                snprintf(model_node_id, sizeof(model_node_id), "model:%s", source_model);
                snprintf(model_json, sizeof(model_json),
                         "{\"id\":\"%s\",\"source_collection\":\"%s\",\"source_kind\":\"hf_recipe_yaml\"}",
                         source_model,
                         source_collection[0] ? source_collection : category);
                upsert_node(db, model_node_id, "model", source_model, source_model,
                            source_collection[0] ? source_collection : "hf-model",
                            "Model inferred from HF recipe YAML.", path, "", model_json);
                upsert_edge(db, node_id, "uses_model", model_node_id, "source_model");
            }

            for (int i = 0; i < family_count; i++) {
                char family_node_id[160];
                if (!family_values[i][0]) continue;
                snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_values[i]);
                upsert_edge(db, node_id, "implements_family", family_node_id, path);
                upsert_edge(db, family_node_id, "has_recipe", node_id, path);
                if (source_model[0]) {
                    char model_node_id[256];
                    snprintf(model_node_id, sizeof(model_node_id), "model:%s", source_model);
                    upsert_edge(db, family_node_id, "has_model", model_node_id, "derived_from_hf_recipe");
                    upsert_edge(db, model_node_id, "member_of_family", family_node_id, "derived_from_hf_recipe");
                }
            }

            pull_count = collect_yaml_list_items(yaml, "pull", pull_items, 256);
            for (int i = 0; i < pull_count; i++) {
                char layer_node_id[192];
                char layer_ext_id[192];
                char layer_name[320];
                char layer_summary[512];
                char layer_json[2048];
                char pull_hash[65];
                int matched_families = 0;

                if (!pull_items[i][0]) continue;
                bf_sha256_hex((const uint8_t *)pull_items[i], strlen(pull_items[i]), pull_hash);
                snprintf(layer_ext_id, sizeof(layer_ext_id), "%s:%s", ext_id, pull_hash);
                snprintf(layer_node_id, sizeof(layer_node_id), "layer:%s", layer_ext_id);
                snprintf(layer_name, sizeof(layer_name), "%s :: %s", ext_id, pull_items[i]);
                snprintf(layer_summary, sizeof(layer_summary),
                         "HF tensor surface from recipe %s%s%s",
                         ext_id,
                         source_model[0] ? " for " : "",
                         source_model[0] ? source_model : "");
                snprintf(layer_json, sizeof(layer_json),
                         "{\"id\":\"%s\",\"artifact_type\":\"hf_tensor_surface\","
                         "\"source_recipe\":\"%s\",\"source_model\":\"%s\","
                         "\"layer_spec_type\":\"tensor_glob\",\"layer_spec_value\":\"%s\","
                         "\"format\":\"safetensors\",\"source_collection\":\"%s\"}",
                         layer_ext_id,
                         ext_id,
                         source_model,
                         pull_items[i],
                         source_collection[0] ? source_collection : category);

                upsert_node(db, layer_node_id, "layer", layer_ext_id, layer_name,
                            "hf_tensor_surface", layer_summary, path, hash, layer_json);
                upsert_edge(db, node_id, "has_layer_artifact", layer_node_id, "derived_from_hf_recipe");
                upsert_edge(db, layer_node_id, "derived_from_recipe", node_id, "derived_from_hf_recipe");
                if (source_model[0]) {
                    char model_node_id[256];
                    snprintf(model_node_id, sizeof(model_node_id), "model:%s", source_model);
                    upsert_edge(db, layer_node_id, "derived_from_model", model_node_id, "derived_from_hf_recipe");
                    upsert_edge(db, model_node_id, "has_layer_artifact", layer_node_id, "derived_from_hf_recipe");
                }

                for (int j = 0; j < family_count; j++) {
                    char family_node_id[160];
                    int family_matches = 0;
                    if (!family_values[j][0]) continue;
                    if (family_patterns[j][0]) {
                        if (strcmp(family_patterns[j], pull_items[i]) == 0 ||
                            fnmatch(family_patterns[j], pull_items[i], 0) == 0 ||
                            strstr(pull_items[i], family_patterns[j]) != NULL ||
                            strstr(family_patterns[j], pull_items[i]) != NULL) {
                            family_matches = 1;
                        }
                    } else {
                        family_matches = 1;
                    }
                    if (!family_matches) continue;
                    snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_values[j]);
                    upsert_edge(db, family_node_id, "has_layer_artifact", layer_node_id, "derived_from_hf_recipe");
                    upsert_edge(db, layer_node_id, "classified_as_family", family_node_id, family_patterns[j]);
                    matched_families++;
                }

                if (matched_families == 0) {
                    for (int j = 0; j < family_count; j++) {
                        char family_node_id[160];
                        if (!family_values[j][0]) continue;
                        snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_values[j]);
                        upsert_edge(db, family_node_id, "has_layer_artifact", layer_node_id, "fallback_from_hf_recipe");
                        upsert_edge(db, layer_node_id, "classified_as_family", family_node_id, "fallback_from_hf_recipe");
                    }
                }
            }

            workflow_step_count = collect_yaml_mapping_keys(yaml, "workflow_steps", workflow_step_ids, 64);
            collect_yaml_mapping_values(yaml, "workflow_steps", workflow_step_names, 64);
            for (int i = 0; i < workflow_step_count && i < 64; i++) {
                char step_node_id[256];
                char step_external_id[256];
                snprintf(step_node_id, sizeof(step_node_id), "workflow-step:%s:%s", ext_id, workflow_step_ids[i]);
                snprintf(step_external_id, sizeof(step_external_id), "%s:%s", ext_id, workflow_step_ids[i]);
                upsert_node(db, step_node_id, "workflow_step", step_external_id,
                            workflow_step_names[i][0] ? workflow_step_names[i] : workflow_step_ids[i],
                            "hf_recipe_yaml_step",
                            workflow_step_names[i][0] ? workflow_step_names[i] : "HF recipe workflow step",
                            path, hash, "");
                upsert_edge(db, node_id, "has_step", step_node_id, "derived_from_hf_recipe");
                for (int j = 0; j < family_count; j++) {
                    char family_node_id[160];
                    if (!family_values[j][0]) continue;
                    snprintf(family_node_id, sizeof(family_node_id), "family:%s", family_values[j]);
                    upsert_edge(db, family_node_id, "investigate_in_workflow_step", step_node_id, "derived_from_hf_recipe");
                    upsert_edge(db, step_node_id, "specialized_by_family", family_node_id, "derived_from_hf_recipe");
                }
            }
        }
        free(yaml);
    }

    closedir(dp);
    return 0;
}

static int sync_capability_registry(sqlite3 *db) {
    sqlite3 *cap_db = NULL;
    sqlite3_stmt *st = NULL;
    char path[PATH_MAX];

    default_capability_db_path(path, sizeof(path));
    if (!bf_is_file(path)) return 0;
    if (bf_sqlite3_open_ro(path, &cap_db) != SQLITE_OK) {
        sqlite3_close(cap_db);
        return 0;
    }
    if (sqlite3_prepare_v2(cap_db,
        "SELECT id,name,description,binary,command,model_id,hardware_tier,cost_estimate,"
        "latency_tier,stage_class,artifact_out,keywords,source "
        "FROM capabilities ORDER BY id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(cap_db);
        return 0;
    }

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *description = (const char *)sqlite3_column_text(st, 2);
        const char *binary = (const char *)sqlite3_column_text(st, 3);
        const char *command = (const char *)sqlite3_column_text(st, 4);
        const char *model_id = (const char *)sqlite3_column_text(st, 5);
        const char *hardware = (const char *)sqlite3_column_text(st, 6);
        double cost = sqlite3_column_double(st, 7);
        const char *latency = (const char *)sqlite3_column_text(st, 8);
        const char *stage = (const char *)sqlite3_column_text(st, 9);
        const char *artifact = (const char *)sqlite3_column_text(st, 10);
        const char *keywords = (const char *)sqlite3_column_text(st, 11);
        const char *source = (const char *)sqlite3_column_text(st, 12);
        char node_id[160];
        char json[2048];

        if (!id || !id[0]) continue;
        snprintf(node_id, sizeof(node_id), "capability:%s", id);
        snprintf(json, sizeof(json),
                 "{\"id\":\"%s\",\"binary\":\"%s\",\"command\":\"%s\",\"model_id\":\"%s\","
                 "\"hardware_tier\":\"%s\",\"cost_estimate\":%.4f,\"latency_tier\":\"%s\","
                 "\"stage_class\":\"%s\",\"artifact_out\":\"%s\",\"keywords\":\"%s\",\"source\":\"%s\"}",
                 id,
                 binary ? binary : "",
                 command ? command : "",
                 model_id ? model_id : "",
                 hardware ? hardware : "",
                 cost,
                 latency ? latency : "",
                 stage ? stage : "",
                 artifact ? artifact : "",
                 keywords ? keywords : "",
                 source ? source : "");
        upsert_node(db, node_id, "capability", id, name ? name : id, stage ? stage : "capability",
                    description ? description : "", path, "", json);

        if (model_id && model_id[0] && strcmp(model_id, "-") != 0) {
            char model_node_id[192];
            snprintf(model_node_id, sizeof(model_node_id), "model:%s", model_id);
            upsert_edge(db, node_id, "uses_model", model_node_id, "");
            upsert_edge(db, model_node_id, "powers_capability", node_id, "");
        }
    }

    sqlite3_finalize(st);
    sqlite3_close(cap_db);
    return 0;
}

/* Projects the fabric's own operator-contract-bindings.tsv (operator_id,
 * contract_id, family, ...) into catalog_nodes as kind=transport (family
 * contains "transport") and kind=surface (contract_id contains "surface").
 * These are real, already-bound operators (command.sync/wire/moq for
 * transport, command.surface for surface) -- not invented categories. */
static int sync_surfaces_and_transports(sqlite3 *db, const char *repo_root) {
    char path[PATH_MAX];
    FILE *file;
    char line[4096];

    snprintf(path, sizeof(path), "%s/estate/operator-contract-bindings.tsv", repo_root);
    file = fopen(path, "r");
    if (!file) return 0;
    if (!fgets(line, sizeof(line), file)) { fclose(file); return 0; }

    while (fgets(line, sizeof(line), file)) {
        char *fields[3];
        char *cursor = line;
        int field = 0;
        while (field < 3 && cursor) {
            fields[field++] = cursor;
            cursor = strchr(cursor, '\t');
            if (cursor) *cursor++ = '\0';
        }
        if (field != 3) continue;
        const char *operator_id = fields[0];
        const char *contract_id = fields[1];
        const char *family = fields[2];
        char node_id[224];
        char json[384];

        if (strstr(family, "transport")) {
            snprintf(node_id, sizeof(node_id), "transport:%s", operator_id);
            snprintf(json, sizeof(json), "{\"operator_id\":\"%s\",\"contract_id\":\"%s\",\"family\":\"%s\"}",
                     operator_id, contract_id, family);
            upsert_node(db, node_id, "transport", operator_id, operator_id, family,
                        "Wire-protocol transport operator", path, "", json);
        }
        if (strstr(contract_id, "surface")) {
            snprintf(node_id, sizeof(node_id), "surface:%s", operator_id);
            snprintf(json, sizeof(json), "{\"operator_id\":\"%s\",\"contract_id\":\"%s\",\"family\":\"%s\"}",
                     operator_id, contract_id, family);
            upsert_node(db, node_id, "surface", operator_id, operator_id, family,
                        "UI/document surface operator", path, "", json);
        }
    }
    fclose(file);
    return 0;
}

static int sync_model_registry(sqlite3 *db) {
    sqlite3 *model_db = NULL;
    sqlite3_stmt *st = NULL;
    char path[PATH_MAX];

    default_model_db_path(path, sizeof(path));
    if (!bf_is_file(path)) return 0;
    if (bf_sqlite3_open_ro(path, &model_db) != SQLITE_OK) {
        sqlite3_close(model_db);
        return 0;
    }
    if (sqlite3_prepare_v2(model_db,
        "SELECT id,name,description,format,sha256,size_mb,fpq_sha256,fpq_size_mb,"
        "transform_family,geometry,geometry_condition,layer_frag_spec,mean_f1 "
        "FROM models ORDER BY id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(model_db);
        return 0;
    }

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *description = (const char *)sqlite3_column_text(st, 2);
        const char *format = (const char *)sqlite3_column_text(st, 3);
        const char *sha256 = (const char *)sqlite3_column_text(st, 4);
        double size_mb = sqlite3_column_double(st, 5);
        const char *fpq_sha256 = (const char *)sqlite3_column_text(st, 6);
        double fpq_size_mb = sqlite3_column_double(st, 7);
        const char *family = (const char *)sqlite3_column_text(st, 8);
        const char *geometry = (const char *)sqlite3_column_text(st, 9);
        const char *condition = (const char *)sqlite3_column_text(st, 10);
        const char *layer_frag = (const char *)sqlite3_column_text(st, 11);
        double mean_f1 = sqlite3_column_double(st, 12);
        char node_id[192];
        char json[2048];

        if (!id || !id[0]) continue;
        snprintf(node_id, sizeof(node_id), "model:%s", id);
        snprintf(json, sizeof(json),
                 "{\"id\":\"%s\",\"format\":\"%s\",\"sha256\":\"%s\",\"size_mb\":%.4f,"
                 "\"fpq_sha256\":\"%s\",\"fpq_size_mb\":%.4f,\"transform_family\":\"%s\","
                 "\"geometry\":\"%s\",\"geometry_condition\":\"%s\",\"layer_frag_spec\":\"%s\","
                 "\"mean_f1\":%.4f}",
                 id,
                 format ? format : "",
                 sha256 ? sha256 : "",
                 size_mb,
                 fpq_sha256 ? fpq_sha256 : "",
                 fpq_size_mb,
                 family ? family : "",
                 geometry ? geometry : "",
                 condition ? condition : "",
                 layer_frag ? layer_frag : "",
                 mean_f1);
        upsert_node(db, node_id, "model", id, name ? name : id, family ? family : "model",
                    description ? description : "", path, "", json);

        if (family && family[0]) {
            char family_node_id[160];
            snprintf(family_node_id, sizeof(family_node_id), "family:%s", family);
            upsert_edge(db, node_id, "member_of_family", family_node_id, "");
            upsert_edge(db, family_node_id, "has_model", node_id, "");
        }
    }
    sqlite3_finalize(st);

    if (sqlite3_prepare_v2(model_db,
        "SELECT model_id, url, priority FROM sources ORDER BY model_id, priority, url",
        -1, &st, NULL) == SQLITE_OK) {
        int ordinal = 0;
        char current_model[128] = "";
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *model_id = (const char *)sqlite3_column_text(st, 0);
            const char *url = (const char *)sqlite3_column_text(st, 1);
            int priority = sqlite3_column_int(st, 2);
            char model_node_id[192];
            char source_node_id[256];
            char source_ext_id[256];
            char source_json[1024];

            if (!model_id || !model_id[0] || !url || !url[0]) continue;
            if (strcmp(current_model, model_id) != 0) {
                snprintf(current_model, sizeof(current_model), "%s", model_id);
                ordinal = 0;
            }
            ordinal++;
            snprintf(model_node_id, sizeof(model_node_id), "model:%s", model_id);
            snprintf(source_node_id, sizeof(source_node_id), "model-source:%s:%04d", model_id, ordinal);
            snprintf(source_ext_id, sizeof(source_ext_id), "%s:%04d", model_id, ordinal);
            snprintf(source_json, sizeof(source_json),
                     "{\"model_id\":\"%s\",\"url\":\"%s\",\"priority\":%d}",
                     model_id, url, priority);
            upsert_node(db, source_node_id, "model_source", source_ext_id, url, "source",
                        "Model pull source", path, "", source_json);
            upsert_edge(db, model_node_id, "has_source", source_node_id, "");
            upsert_edge(db, source_node_id, "sources_model", model_node_id, "");
        }
        sqlite3_finalize(st);
    }

    if (sqlite3_prepare_v2(model_db,
        "SELECT recipe_code, model_id, role FROM recipe_models",
        -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *recipe_code = (const char *)sqlite3_column_text(st, 0);
            const char *model_id = (const char *)sqlite3_column_text(st, 1);
            const char *role = (const char *)sqlite3_column_text(st, 2);
            char recipe_node_id[160];
            char model_node_id[192];
            if (!recipe_code || !model_id) continue;
            snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_code);
            snprintf(model_node_id, sizeof(model_node_id), "model:%s", model_id);
            upsert_edge(db, recipe_node_id, "uses_model", model_node_id, role ? role : "");
        }
        sqlite3_finalize(st);
    }

    sqlite3_close(model_db);
    return 0;
}

static int sync_layer_registry(sqlite3 *db) {
    sqlite3 *layer_db = NULL;
    sqlite3_stmt *st = NULL;
    char path[PATH_MAX];

    default_layer_db_path(path, sizeof(path));
    if (!bf_is_file(path)) return 0;
    if (bf_sqlite3_open_ro(path, &layer_db) != SQLITE_OK) {
        sqlite3_close(layer_db);
        return 0;
    }
    if (sqlite3_prepare_v2(layer_db,
        "SELECT id,name,artifact_type,source_model,layer_spec_type,layer_spec_value,"
        "node_start,node_end,n_nodes,n_params,sha256,format,artifact_path,onnx_path,created_at "
        "FROM layers ORDER BY id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(layer_db);
        return 0;
    }

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *artifact_type = (const char *)sqlite3_column_text(st, 2);
        const char *source_model = (const char *)sqlite3_column_text(st, 3);
        const char *spec_type = (const char *)sqlite3_column_text(st, 4);
        const char *spec_value = (const char *)sqlite3_column_text(st, 5);
        int node_start = sqlite3_column_int(st, 6);
        int node_end = sqlite3_column_int(st, 7);
        int n_nodes = sqlite3_column_int(st, 8);
        sqlite3_int64 n_params = sqlite3_column_int64(st, 9);
        const char *sha256 = (const char *)sqlite3_column_text(st, 10);
        const char *format = (const char *)sqlite3_column_text(st, 11);
        const char *artifact_path = (const char *)sqlite3_column_text(st, 12);
        const char *onnx_path = (const char *)sqlite3_column_text(st, 13);
        const char *created_at = (const char *)sqlite3_column_text(st, 14);
        char node_id[192];
        char json[2048];

        if (!id || !id[0]) continue;
        snprintf(node_id, sizeof(node_id), "layer:%s", id);
        snprintf(json, sizeof(json),
                 "{\"id\":\"%s\",\"artifact_type\":\"%s\",\"source_model\":\"%s\","
                 "\"layer_spec_type\":\"%s\",\"layer_spec_value\":\"%s\",\"node_start\":%d,"
                 "\"node_end\":%d,\"n_nodes\":%d,\"n_params\":%lld,\"sha256\":\"%s\","
                 "\"format\":\"%s\",\"artifact_path\":\"%s\",\"onnx_path\":\"%s\",\"created_at\":\"%s\"}",
                 id,
                 artifact_type ? artifact_type : "",
                 source_model ? source_model : "",
                 spec_type ? spec_type : "",
                 spec_value ? spec_value : "",
                 node_start,
                 node_end,
                 n_nodes,
                 (long long)n_params,
                 sha256 ? sha256 : "",
                 format ? format : "",
                 artifact_path ? artifact_path : "",
                 onnx_path ? onnx_path : "",
                 created_at ? created_at : "");
        upsert_node(db, node_id, "layer", id, name ? name : id, artifact_type ? artifact_type : "layer",
                    spec_type ? spec_type : "", artifact_path ? artifact_path : path, sha256 ? sha256 : "", json);

        if (source_model && source_model[0]) {
            char model_node_id[256];
            snprintf(model_node_id, sizeof(model_node_id), "model:%s", source_model);
            upsert_edge(db, node_id, "derived_from_model", model_node_id, "");
            upsert_edge(db, model_node_id, "has_layer_artifact", node_id, "");
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(layer_db);
    return 0;
}

static int seed_family_relationships(sqlite3 *db) {
    for (const bf_catalog_family_seed_t *seed = BF_CATALOG_FAMILY_SEEDS; seed->family_id; seed++) {
        char family_node_id[160];
        snprintf(family_node_id, sizeof(family_node_id), "family:%s", seed->family_id);

        if (seed->related && seed->related[0]) {
            char related_copy[256];
            char *save = NULL;
            snprintf(related_copy, sizeof(related_copy), "%s", seed->related);
            for (char *tok = strtok_r(related_copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
                char related_id[128];
                char related_node_id[160];
                snprintf(related_id, sizeof(related_id), "%s", tok);
                trim_token(related_id);
                if (!related_id[0]) continue;
                snprintf(related_node_id, sizeof(related_node_id), "family:%s", related_id);
                upsert_edge(db, family_node_id, "related_family", related_node_id, "");
            }
        }

        if (seed->workflow_examples && seed->workflow_examples[0]) {
            char examples_copy[1024];
            char *save_lines = NULL;
            snprintf(examples_copy, sizeof(examples_copy), "%s", seed->workflow_examples);
            for (char *line = strtok_r(examples_copy, "\n", &save_lines);
                 line;
                 line = strtok_r(NULL, "\n", &save_lines)) {
                char *save_fields = NULL;
                char *workflow_id = strtok_r(line, "|", &save_fields);
                char *step_id = strtok_r(NULL, "|", &save_fields);
                char *step_name = strtok_r(NULL, "|", &save_fields);
                char *operator = strtok_r(NULL, "|", &save_fields);
                char step_node_id[256];
                char meta[512];
                if (!workflow_id || !step_id) continue;
                trim_token(workflow_id);
                trim_token(step_id);
                if (!workflow_id[0] || !step_id[0]) continue;
                snprintf(step_node_id, sizeof(step_node_id), "workflow-step:%s:%s", workflow_id, step_id);
                snprintf(meta, sizeof(meta),
                         "workflow=%s;step=%s;name=%s;operator=%s",
                         workflow_id,
                         step_id,
                         step_name ? step_name : "",
                         operator ? operator : "");
                upsert_edge(db, family_node_id, "investigate_in_workflow_step", step_node_id, meta);
                upsert_edge(db, step_node_id, "specialized_by_family", family_node_id, meta);
            }
        }
    }

    return 0;
}

static int derive_capability_family_relationships(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT DISTINCT ce.src_node_id, fe.dst_node_id "
        "FROM catalog_edges ce "
        "JOIN catalog_edges fe ON fe.src_node_id = ce.dst_node_id "
        "WHERE ce.rel = 'seen_in_workflow_step' "
        "  AND fe.rel = 'specialized_by_family'",
        -1, &st, NULL) != SQLITE_OK) {
        return 1;
    }

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *capability_node_id = (const char *)sqlite3_column_text(st, 0);
        const char *family_node_id = (const char *)sqlite3_column_text(st, 1);
        if (!capability_node_id || !family_node_id) continue;
        upsert_edge(db, capability_node_id, "related_family", family_node_id, "derived");
        upsert_edge(db, family_node_id, "supports_capability", capability_node_id, "derived");
    }

    sqlite3_finalize(st);
    return 0;
}

void bf_catalog_default_db_path(char *buf, size_t sz) {
    const char *env = getenv("BONFYRE_CATALOG_DB");
    const char *home = getenv("HOME");
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    if (!home) home = "/tmp";
    snprintf(buf, sz, "%s%s", home, BF_CATALOG_DB_SUBPATH);
}

int bf_catalog_find_repo_root(char *buf, size_t sz) {
    char cwd[PATH_MAX];
    char candidate[PATH_MAX];
    char *slash;
    const char *env = getenv("BONFYRE_REPO_ROOT");

    if (env && bf_is_dir(env)) { snprintf(buf, sz, "%s", env); return 1; }
    if (!getcwd(cwd, sizeof(cwd))) return 0;
    snprintf(candidate, sizeof(candidate), "%s", cwd);

    for (;;) {
        char recipes_dir[PATH_MAX];
        char docs_dir[PATH_MAX];
        join_path(recipes_dir, sizeof(recipes_dir), candidate, "recipes");
        join_path(docs_dir, sizeof(docs_dir), candidate, "docs/recipes");
        if (bf_is_dir(recipes_dir) && bf_is_dir(docs_dir)) {
            snprintf(buf, sz, "%s", candidate);
            return 1;
        }
        slash = strrchr(candidate, '/');
        if (!slash || slash == candidate) break;
        *slash = '\0';
    }
    return 0;
}

int bf_catalog_sync_repo(const char *db_path, const char *repo_root) {
    sqlite3 *db = NULL;
    char resolved_db[PATH_MAX];
    char db_dir[PATH_MAX];
    char *slash;
    int ok = 0;
    int debug = getenv("BONFYRE_CATALOG_DEBUG") ? 1 : 0;

    if (!repo_root || !repo_root[0]) return 1;
    if (db_path && db_path[0]) snprintf(resolved_db, sizeof(resolved_db), "%s", db_path);
    else bf_catalog_default_db_path(resolved_db, sizeof(resolved_db));

    snprintf(db_dir, sizeof(db_dir), "%s", resolved_db);
    slash = strrchr(db_dir, '/');
    if (slash) {
        *slash = '\0';
        if (bf_ensure_dir(db_dir) != 0) return 1;
    }

    if (bf_sqlite3_open(resolved_db, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    if (sqlite3_exec(db, BF_CATALOG_SCHEMA, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    if (begin_tx(db) != 0) {
        sqlite3_close(db);
        return 1;
    }

    ok = 1;
#define BF_SYNC_STEP(label, expr) \
    do { \
        int rc_step = (expr); \
        if (debug) fprintf(stderr, "catalog-sync: %s -> %s\n", (label), rc_step == 0 ? "ok" : "fail"); \
        if (rc_step != 0) ok = 0; \
    } while (0)

    BF_SYNC_STEP("delete family", delete_scope(db, "family:"));
    if (ok) BF_SYNC_STEP("delete workflow", delete_scope(db, "workflow:"));
    if (ok) BF_SYNC_STEP("delete workflow-step", delete_scope(db, "workflow-step:"));
    if (ok) BF_SYNC_STEP("delete capability", delete_scope(db, "capability:"));
    if (ok) BF_SYNC_STEP("delete model", delete_scope(db, "model:"));
    if (ok) BF_SYNC_STEP("delete model-source", delete_scope(db, "model-source:"));
    if (ok) BF_SYNC_STEP("delete layer", delete_scope(db, "layer:"));
    if (ok) BF_SYNC_STEP("delete recipe", delete_scope(db, "recipe:"));
    if (ok) BF_SYNC_STEP("delete transport", delete_scope(db, "transport:"));
    if (ok) BF_SYNC_STEP("delete surface", delete_scope(db, "surface:"));
    if (ok) BF_SYNC_STEP("sync family index", sync_family_index(db, repo_root));
    if (ok) BF_SYNC_STEP("sync capability registry", sync_capability_registry(db));
    if (ok) BF_SYNC_STEP("sync model registry", sync_model_registry(db));
    if (ok) BF_SYNC_STEP("sync layer registry", sync_layer_registry(db));
    if (ok) BF_SYNC_STEP("sync docs recipes", sync_json_dir(db, repo_root, "docs/recipes", "workflow"));
    if (ok) BF_SYNC_STEP("sync json recipes", sync_json_dir(db, repo_root, "recipes", "recipe"));
    if (ok) BF_SYNC_STEP("sync yaml google", sync_yaml_dir(db, repo_root, "recipes/google", "recipe"));
    if (ok) BF_SYNC_STEP("sync yaml topology", sync_yaml_dir(db, repo_root, "recipes/topology", "recipe"));
    if (ok) BF_SYNC_STEP("sync yaml cross_fusion", sync_yaml_dir(db, repo_root, "recipes/cross_fusion", "recipe"));
    if (ok) BF_SYNC_STEP("sync surfaces and transports", sync_surfaces_and_transports(db, repo_root));
    if (ok) BF_SYNC_STEP("seed family relationships", seed_family_relationships(db));
    if (ok) BF_SYNC_STEP("derive capability-family relationships", derive_capability_family_relationships(db));

#undef BF_SYNC_STEP

    if (ok) rebuild_fts(db);
    end_tx(db, ok);
    sqlite3_close(db);
    return ok ? 0 : 1;
}

int bf_catalog_sync_default(const char *db_path) {
    char repo_root[PATH_MAX];
    if (!bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) return 1;
    return bf_catalog_sync_repo(db_path, repo_root);
}

int bf_catalog_record_run_manifest(const char *db_path, const char *manifest_path) {
    sqlite3 *db = NULL;
    bf_json_doc_t *doc = NULL;
    char *json = NULL;
    char resolved_db[PATH_MAX];
    char repo_root[PATH_MAX];
    const bf_json_node_t *root;
    const bf_json_node_t *stages;
    char recipe_id[128] = "";
    char status[64] = "";
    char kind[128] = "";
    char hash[65] = "";
    long started_at = 0;
    char node_id[256];
    int ok = 0;

    if (!manifest_path || !manifest_path[0]) return 1;
    if (!load_json_doc(manifest_path, &json, &doc)) return 1;
    root = bf_json_root(doc);
    if (!root || root->type != BF_JSON_OBJECT) goto cleanup;

    if (db_path && db_path[0]) snprintf(resolved_db, sizeof(resolved_db), "%s", db_path);
    else bf_catalog_default_db_path(resolved_db, sizeof(resolved_db));
    if (bf_catalog_find_repo_root(repo_root, sizeof(repo_root)))
        bf_catalog_sync_repo(resolved_db, repo_root);

    if (bf_sqlite3_open(resolved_db, &db) != SQLITE_OK) goto cleanup;
    if (sqlite3_exec(db, BF_CATALOG_SCHEMA, NULL, NULL, NULL) != SQLITE_OK) goto cleanup;
    if (begin_tx(db) != 0) goto cleanup;

    node_str_copy(bf_json_obj_get(doc, root, "recipe_id"), recipe_id, sizeof(recipe_id));
    node_str_copy(bf_json_obj_get(doc, root, "status"), status, sizeof(status));
    node_str_copy(bf_json_obj_get(doc, root, "kind"), kind, sizeof(kind));
    {
        int started = 0;
        if (bf_json_int(json, "started_at", &started)) started_at = started;
    }
    bf_sha256_hex((const uint8_t *)json, strlen(json), hash);
    snprintf(node_id, sizeof(node_id), "run:%s:%ld", recipe_id[0] ? recipe_id : "unknown", started_at);
    upsert_node(db, node_id, "run_manifest",
                node_id + 4,
                recipe_id[0] ? recipe_id : "run",
                status[0] ? status : "run",
                kind[0] ? kind : "Bonfyre run manifest",
                manifest_path, hash, json);
    if (recipe_id[0]) {
        char recipe_node_id[160];
        snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_id);
        upsert_edge(db, node_id, "runs_recipe", recipe_node_id, status);
        upsert_edge(db, recipe_node_id, "has_run_manifest", node_id, status);
    }

    stages = bf_json_obj_get(doc, root, "stages");
    if (stages && stages->type == BF_JSON_ARRAY) {
        for (const bf_json_node_t *child = bf_json_child_first(doc, stages); child; child = bf_json_child_next(doc, child)) {
            char stage_id[128] = "";
            char stage_name[256] = "";
            char op[256] = "";
            char stage_node_id[320];
            const char *cap_id;
            node_str_copy(bf_json_obj_get(doc, child, "id"), stage_id, sizeof(stage_id));
            node_str_copy(bf_json_obj_get(doc, child, "name"), stage_name, sizeof(stage_name));
            node_str_copy(bf_json_obj_get(doc, child, "operator"), op, sizeof(op));
            if (!stage_id[0]) continue;
            snprintf(stage_node_id, sizeof(stage_node_id), "%s:%s", node_id, stage_id);
            upsert_node(db, stage_node_id, "run_stage",
                        stage_node_id + 4,
                        stage_name[0] ? stage_name : stage_id,
                        op[0] ? op : "stage",
                        stage_name[0] ? stage_name : "run stage",
                        manifest_path, hash, "");
            upsert_edge(db, node_id, "has_run_stage", stage_node_id, "");
            cap_id = infer_step_capability_id(op, stage_name[0] ? stage_name : stage_id);
            if ((!cap_id || !cap_id[0]) && stage_id[0]) {
                if (strcmp(stage_id, "embed") == 0) cap_id = "embed";
                else if (strcmp(stage_id, "train") == 0) cap_id = "model";
                else if (strcmp(stage_id, "finalize") == 0) cap_id = "pack";
                else if (strcmp(stage_id, "segment") == 0) cap_id = "segment";
                else if (strcmp(stage_id, "transcribe") == 0) cap_id = "transcribe";
                else if (strcmp(stage_id, "clean") == 0) cap_id = "clean";
                else if (strcmp(stage_id, "brief") == 0) cap_id = "brief";
                else if (strcmp(stage_id, "proof") == 0) cap_id = "proof";
            }
            if (cap_id && cap_id[0]) {
                char capability_node_id[160];
                snprintf(capability_node_id, sizeof(capability_node_id), "capability:%s", cap_id);
                upsert_edge(db, stage_node_id, "implements_capability", capability_node_id, "");
                upsert_edge(db, capability_node_id, "seen_in_run_stage", stage_node_id, "");
            }
        }
    }

    rebuild_fts(db);
    end_tx(db, 1);
    ok = 1;

cleanup:
    if (!ok && db) end_tx(db, 0);
    if (db) sqlite3_close(db);
    if (doc) bf_json_free(doc);
    free(json);
    return ok ? 0 : 1;
}
