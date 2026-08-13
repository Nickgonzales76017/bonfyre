/*
 * bonfyre-layer — Layer-aware ONNX model inspection and extraction (C port)
 *
 * Part of the Bonfyre layer-aware infrastructure (Track B).
 * Operates on ONNX protobuf directly — no Python or onnx library required.
 *
 * Usage:
 *   bonfyre-layer inspect      <model.onnx>
 *   bonfyre-layer layers       <model.onnx>
 *   bonfyre-layer pull-layer   <model.onnx> --range START:END --out DIR
 *   bonfyre-layer pull-head    <model.onnx> --name PREFIX --out DIR
 *   bonfyre-layer pack-transform <name> <part1.onnx> [part2.onnx ...] --out DIR
 *   bonfyre-layer schema
 *   bonfyre-layer --help
 *
 * Layer Artifact (written to DIR/artifact.json):
 *   type, source_model, layer_spec, node_range, n_params,
 *   sha256, format = "onnx"
 *
 * Protobuf fields (ONNX subset used):
 *   ModelProto:  graph(7), ir_version(1), opset_import(8)
 *   GraphProto:  node(1), initializer(5), value_info(13), input(11), output(12)
 *   NodeProto:   input(1), output(2), name(3), op_type(4), attribute(5)
 *   TensorProto: dims(1), data_type(2), float_data(4), name(5), raw_data(9)
 */
#include <bonfyre.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <ctype.h>

#define VERSION "1.0.0"
#define MAX_NODES 8192
#define MAX_NAME  512
#define MAX_PATH  4096
#define LAYER_DB_ENV "BONFYRE_LAYER_DB"
#define LAYER_DB_SUBPATH "/.local/share/bonfyre/layers.db"

static const char *layeros_binary(void) {
    const char *env = getenv("BONFYRE_LAYEROS_BINARY");
    return (env && env[0]) ? env : "layeros/bin/bonfyre-layeros";
}

static int delegate_layeros(char *const argv[]) {
    pid_t pid = fork();
    int st = 0;
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror(argv[0]);
        _exit(127);
    }
    waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * Minimal protobuf primitives (read + write)
 * ═══════════════════════════════════════════════════════════════════ */

#define WT_VARINT 0
#define WT_64BIT  1
#define WT_LEN    2
#define WT_32BIT  5

static uint64_t pb_read_varint(const uint8_t *buf, size_t len, size_t *pos) {
    uint64_t val = 0; int shift = 0;
    while (*pos < len) {
        uint8_t b = buf[(*pos)++];
        val |= (uint64_t)(b & 0x7F) << shift;
        shift += 7;
        if (!(b & 0x80)) break;
    }
    return val;
}

static void pb_skip(const uint8_t *buf, size_t len, size_t *pos, int wtype) {
    switch (wtype) {
    case WT_VARINT: pb_read_varint(buf, len, pos); break;
    case WT_64BIT:  *pos += 8; break;
    case WT_LEN: { uint64_t sz = pb_read_varint(buf, len, pos); *pos += (size_t)sz; break; }
    case WT_32BIT:  *pos += 4; break;
    default: *pos = len;
    }
}

/* Dynamic write buffer */
typedef struct { uint8_t *data; size_t len; size_t cap; } WBuf;

static int wbuf_grow(WBuf *b, size_t need) {
    if (b->len + need <= b->cap) return 1;
    size_t nc = (b->cap * 2 > b->len + need) ? b->cap * 2 : b->len + need + 256;
    uint8_t *p = (uint8_t *)realloc(b->data, nc);
    if (!p) return 0;
    b->data = p; b->cap = nc;
    return 1;
}

static void wbuf_init(WBuf *b) { b->data=NULL; b->len=0; b->cap=0; }
static void wbuf_free(WBuf *b) { free(b->data); wbuf_init(b); }

static void wbuf_write_varint(WBuf *b, uint64_t v) {
    uint8_t tmp[10]; int n=0;
    do { tmp[n++] = (uint8_t)((v & 0x7F) | (v > 127 ? 0x80 : 0)); v >>= 7; } while(v);
    if (!wbuf_grow(b, (size_t)n)) return;
    memcpy(b->data + b->len, tmp, (size_t)n); b->len += (size_t)n;
}

static void wbuf_write_tag(WBuf *b, int field, int wtype) {
    wbuf_write_varint(b, (uint64_t)(field << 3 | wtype));
}

static void wbuf_write_bytes(WBuf *b, int field, const uint8_t *data, size_t n) {
    wbuf_write_tag(b, field, WT_LEN);
    wbuf_write_varint(b, (uint64_t)n);
    if (!wbuf_grow(b, n)) return;
    memcpy(b->data + b->len, data, n); b->len += n;
}

static void wbuf_write_string(WBuf *b, int field, const char *s) {
    wbuf_write_bytes(b, field, (const uint8_t *)s, strlen(s));
}

static void wbuf_write_i64(WBuf *b, int field, int64_t v) {
    wbuf_write_tag(b, field, WT_VARINT);
    wbuf_write_varint(b, (uint64_t)v);
}

/* Write file from WBuf */
static int wbuf_save(const WBuf *b, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) { perror(path); return 1; }
    int ok = (fwrite(b->data, 1, b->len, f) == b->len);
    fclose(f);
    return ok ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * ONNX graph reader — captures nodes + initializers with raw bytes
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    char     name[MAX_NAME];
    char     op_type[64];
    char     inputs[8][MAX_NAME];
    int      n_inputs;
    char     outputs[4][MAX_NAME];
    int      n_outputs;
    /* raw bytes of this NodeProto (for verbatim copy to new model) */
    uint8_t *raw;
    size_t   raw_len;
} BfLayerNode;

typedef struct {
    char    name[MAX_NAME];
    size_t  n_elements;
    /* raw bytes of this TensorProto */
    uint8_t *raw;
    size_t   raw_len;
} BfInitTensor;

typedef struct {
    BfLayerNode   *nodes;
    size_t         n_nodes;
    BfInitTensor  *inits;
    size_t         n_inits;
    int64_t        ir_version;
    int64_t        opset_version;
    /* raw ir_version & opset_import bytes (for copy to new model) */
    uint8_t        opset_raw[64];
    size_t         opset_raw_len;
} BfLayerGraph;

static void bf_graph_free(BfLayerGraph *g) {
    for (size_t i = 0; i < g->n_nodes; i++) free(g->nodes[i].raw);
    free(g->nodes);
    for (size_t i = 0; i < g->n_inits; i++) free(g->inits[i].raw);
    free(g->inits);
}

static void parse_node(const uint8_t *buf, size_t len, BfLayerNode *nd) {
    nd->n_inputs = 0; nd->n_outputs = 0;
    nd->name[0] = '\0'; nd->op_type[0] = '\0';
    size_t pos = 0;
    while (pos < len) {
        uint64_t tag = pb_read_varint(buf, len, &pos);
        int field = (int)(tag >> 3), wtype = (int)(tag & 7);
        if (field == 1 && wtype == WT_LEN) { /* input */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            if (nd->n_inputs < 8) {
                size_t cp = sz < MAX_NAME-1 ? (size_t)sz : MAX_NAME-1;
                memcpy(nd->inputs[nd->n_inputs], buf+pos, cp);
                nd->inputs[nd->n_inputs][cp] = '\0';
                nd->n_inputs++;
            }
            pos += (size_t)sz;
        } else if (field == 2 && wtype == WT_LEN) { /* output */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            if (nd->n_outputs < 4) {
                size_t cp = sz < MAX_NAME-1 ? (size_t)sz : MAX_NAME-1;
                memcpy(nd->outputs[nd->n_outputs], buf+pos, cp);
                nd->outputs[nd->n_outputs][cp] = '\0';
                nd->n_outputs++;
            }
            pos += (size_t)sz;
        } else if (field == 3 && wtype == WT_LEN) { /* name */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            size_t cp = sz < MAX_NAME-1 ? (size_t)sz : MAX_NAME-1;
            memcpy(nd->name, buf+pos, cp); nd->name[cp] = '\0';
            pos += (size_t)sz;
        } else if (field == 4 && wtype == WT_LEN) { /* op_type */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            size_t cp = sz < 63 ? (size_t)sz : 63;
            memcpy(nd->op_type, buf+pos, cp); nd->op_type[cp] = '\0';
            pos += (size_t)sz;
        } else {
            pb_skip(buf, len, &pos, wtype);
        }
    }
}

static size_t parse_tensor_nelems(const uint8_t *buf, size_t len) {
    size_t pos = 0, n = 1; int got = 0;
    while (pos < len) {
        uint64_t tag = pb_read_varint(buf, len, &pos);
        int field = (int)(tag >> 3), wtype = (int)(tag & 7);
        if (field == 1 && wtype == WT_LEN) { /* dims — packed */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            size_t end = pos + (size_t)sz; n = 1; got = 1;
            while (pos < end) { int64_t d = (int64_t)pb_read_varint(buf, end, &pos); if(d>0) n *= (size_t)d; }
            pos = end;
        } else if (field == 1 && wtype == WT_VARINT) { /* dims — unpacked */
            int64_t d = (int64_t)pb_read_varint(buf, len, &pos);
            if (!got) { n = 1; got = 1; } if (d > 0) n *= (size_t)d;
        } else { pb_skip(buf, len, &pos, wtype); }
    }
    return got ? n : 0;
}

static void parse_init_name(const uint8_t *buf, size_t len, char *name, size_t nsz) {
    size_t pos = 0; name[0] = '\0';
    while (pos < len) {
        uint64_t tag = pb_read_varint(buf, len, &pos);
        int field = (int)(tag >> 3), wtype = (int)(tag & 7);
        if (field == 5 && wtype == WT_LEN) {
            uint64_t sz = pb_read_varint(buf, len, &pos);
            size_t cp = sz < nsz-1 ? (size_t)sz : nsz-1;
            memcpy(name, buf+pos, cp); name[cp] = '\0';
            return;
        } else { pb_skip(buf, len, &pos, wtype); }
    }
}

static void parse_graph(const uint8_t *buf, size_t len, BfLayerGraph *g) {
    g->nodes  = (BfLayerNode *)calloc(MAX_NODES, sizeof(BfLayerNode));
    g->inits  = (BfInitTensor *)calloc(MAX_NODES, sizeof(BfInitTensor));
    g->n_nodes = 0; g->n_inits = 0;

    size_t pos = 0;
    while (pos < len) {
        uint64_t tag = pb_read_varint(buf, len, &pos);
        int field = (int)(tag >> 3), wtype = (int)(tag & 7);
        if (field == 1 && wtype == WT_LEN) { /* node */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            if (g->n_nodes < MAX_NODES) {
                BfLayerNode *nd = &g->nodes[g->n_nodes++];
                nd->raw = (uint8_t *)malloc((size_t)sz);
                if (nd->raw) { memcpy(nd->raw, buf+pos, (size_t)sz); nd->raw_len = (size_t)sz; }
                parse_node(buf+pos, (size_t)sz, nd);
                /* fill default name */
                if (!nd->name[0]) snprintf(nd->name, sizeof(nd->name), "%s_%zu", nd->op_type, g->n_nodes-1);
            }
            pos += (size_t)sz;
        } else if (field == 5 && wtype == WT_LEN) { /* initializer */
            uint64_t sz = pb_read_varint(buf, len, &pos);
            if (g->n_inits < MAX_NODES) {
                BfInitTensor *it = &g->inits[g->n_inits++];
                it->raw = (uint8_t *)malloc((size_t)sz);
                if (it->raw) { memcpy(it->raw, buf+pos, (size_t)sz); it->raw_len = (size_t)sz; }
                it->n_elements = parse_tensor_nelems(buf+pos, (size_t)sz);
                parse_init_name(buf+pos, (size_t)sz, it->name, sizeof(it->name));
            }
            pos += (size_t)sz;
        } else {
            pb_skip(buf, len, &pos, wtype);
        }
    }
}

static int read_model_file(const char *path, uint8_t **out, size_t *sz) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return 1; }
    fseek(f, 0, SEEK_END); long fsz = ftell(f); rewind(f);
    if (fsz <= 0) { fclose(f); return 1; }
    *sz = (size_t)fsz;
    *out = (uint8_t *)malloc(*sz);
    if (!*out) { fclose(f); return 1; }
    if (fread(*out, 1, *sz, f) != *sz) { free(*out); fclose(f); return 1; }
    fclose(f);
    return 0;
}

static int load_graph(const char *path, BfLayerGraph *g) {
    uint8_t *raw = NULL; size_t rawsz = 0;
    if (read_model_file(path, &raw, &rawsz)) return 1;

    g->ir_version = 0; g->opset_version = 0;
    g->opset_raw_len = 0;
    g->nodes = NULL; g->n_nodes = 0;
    g->inits = NULL; g->n_inits = 0;

    size_t pos = 0;
    while (pos < rawsz) {
        size_t tag_pos = pos;
        uint64_t tag = pb_read_varint(raw, rawsz, &pos);
        int field = (int)(tag >> 3), wtype = (int)(tag & 7);
        if (field == 1 && wtype == WT_VARINT) { /* ir_version */
            g->ir_version = (int64_t)pb_read_varint(raw, rawsz, &pos);
        } else if (field == 7 && wtype == WT_LEN) { /* graph */
            uint64_t sz = pb_read_varint(raw, rawsz, &pos);
            parse_graph(raw + pos, (size_t)sz, g);
            pos += (size_t)sz;
        } else if (field == 8 && wtype == WT_LEN) { /* opset_import */
            uint64_t sz = pb_read_varint(raw, rawsz, &pos);
            /* capture raw opset blob for forwarding */
            size_t blob_sz = (size_t)(pos - tag_pos) - 1 + (size_t)sz;
            (void)blob_sz;
            /* parse out version */
            size_t op_pos = 0;
            while (op_pos < (size_t)sz) {
                uint64_t t2 = pb_read_varint(raw+pos, (size_t)sz, &op_pos);
                int f2 = (int)(t2 >> 3), w2 = (int)(t2 & 7);
                if (f2 == 2 && w2 == WT_VARINT)
                    g->opset_version = (int64_t)pb_read_varint(raw+pos, (size_t)sz, &op_pos);
                else pb_skip(raw+pos, (size_t)sz, &op_pos, w2);
            }
            pos += (size_t)sz;
        } else {
            pb_skip(raw, rawsz, &pos, wtype);
        }
    }
    free(raw);
    return (g->nodes != NULL) ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * SHA-256 (for artifact.json hash)
 * ═══════════════════════════════════════════════════════════════════ */

static void sha256_file_hex(const char *path, char *out64) {
    /* Use bf_sha256_file from libbonfyre if available, else "unknown" */
    out64[0] = '\0';
    FILE *f = fopen(path, "rb");
    if (!f) { strcpy(out64, "unknown"); return; }
    /* Simple SHA-256 via OpenSSL CLI or system sha256sum if available,
     * otherwise delegate to libbonfyre's bf_sha256_hex */
    fclose(f);

    /* Use bf_sha256_hex if libbonfyre exposes it */
    FILE *cmd = NULL;
    char buf[256];
    char cmdbuf[MAX_PATH + 64];
    /* macOS: shasum -a 256; Linux: sha256sum */
    snprintf(cmdbuf, sizeof(cmdbuf), "shasum -a 256 '%s' 2>/dev/null || sha256sum '%s' 2>/dev/null", path, path);
    cmd = popen(cmdbuf, "r");
    if (cmd && fgets(buf, sizeof(buf), cmd)) {
        /* output: "<hash>  filename" */
        size_t i = 0;
        while (i < 64 && buf[i] && buf[i] != ' ' && buf[i] != '\t' && buf[i] != '\n') {
            out64[i] = buf[i]; i++;
        }
        out64[i] = '\0';
    } else {
        strcpy(out64, "unknown");
    }
    if (cmd) pclose(cmd);
}

/* ═══════════════════════════════════════════════════════════════════
 * mkdir -p helper
 * ═══════════════════════════════════════════════════════════════════ */

static void mkdirp(const char *path) {
    char tmp[MAX_PATH];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
    }
    mkdir(tmp, 0755);
}

static void get_layer_db_path(const char *root, char *buf, size_t sz) {
    const char *env = getenv(LAYER_DB_ENV);
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    if (bf_layer_state_db_path(root, "layers.db", buf, sz) != 0 && sz > 0) {
        buf[0] = '\0';
    }
}

static sqlite3 *open_layer_db(const char *root, char *path_buf, size_t path_buf_sz) {
    sqlite3 *db = NULL;
    char dir[MAX_PATH];
    char *slash;
    get_layer_db_path(root, path_buf, path_buf_sz);
    if (!path_buf[0]) return NULL;
    snprintf(dir, sizeof(dir), "%s", path_buf);
    slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        bf_ensure_dir(dir);
    }
    if (bf_sqlite3_open(path_buf, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS layers ("
        "id TEXT PRIMARY KEY,"
        "name TEXT,"
        "artifact_type TEXT NOT NULL,"
        "source_model TEXT,"
        "layer_spec_type TEXT,"
        "layer_spec_value TEXT,"
        "node_start INTEGER,"
        "node_end INTEGER,"
        "n_nodes INTEGER,"
        "n_params INTEGER,"
        "sha256 TEXT,"
        "format TEXT,"
        "artifact_path TEXT,"
        "onnx_path TEXT,"
        "created_at TEXT NOT NULL"
        ");",
        NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE INDEX IF NOT EXISTS idx_layers_kind ON layers(artifact_kind);", NULL, NULL, NULL);
    return db;
}

static int sqlite_table_exists(sqlite3 *db, const char *table_name) {
    sqlite3_stmt *st = NULL;
    int exists = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, table_name, -1, SQLITE_TRANSIENT);
    exists = (sqlite3_step(st) == SQLITE_ROW);
    sqlite3_finalize(st);
    return exists;
}

static void json_extract_field(const char *json, const char *key, char *out, size_t out_sz) {
    if (!bf_json_str(json, key, out, out_sz)) out[0] = '\0';
}

typedef struct {
    char id[192];
    char name[256];
    char artifact_kind[64];
    char schema_version[64];
    char source_model[512];
    char source_recipe[256];
    char source_collection[256];
    char source_file[MAX_PATH];
    char generated_by[128];
    char created_at[64];
    char verified_at[64];
    char layer_spec_type[128];
    char layer_spec_value[256];
    char format[64];
    char artifact_path[MAX_PATH];
    char onnx_path[MAX_PATH];
    char sha256[128];
    char families_text[1024];
    char capabilities_text[1024];
    char workflow_steps_text[1024];
    char shape_constraints_text[1024];
    char dtype_constraints_text[512];
    char compatibility_tags_text[1024];
    char materialization_status[64];
    char verification_status[64];
    int node_start;
    int node_end;
    int n_nodes;
    int n_params;
    char artifact_json[12288];
} LayerArtifactRecord;

static int has_column(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *st = NULL;
    char sql[256];
    int found = 0;
    snprintf(sql, sizeof(sql), "PRAGMA table_info(%s);", table);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 1);
        if (name && strcmp(name, column) == 0) {
            found = 1;
            break;
        }
    }
    sqlite3_finalize(st);
    return found;
}

static void ensure_layer_column(sqlite3 *db, const char *column, const char *decl) {
    char sql[256];
    if (has_column(db, "layers", column)) return;
    snprintf(sql, sizeof(sql), "ALTER TABLE layers ADD COLUMN %s %s;", column, decl);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
}

static void prepare_layer_db_schema(sqlite3 *db) {
    ensure_layer_column(db, "schema_version", "TEXT");
    ensure_layer_column(db, "artifact_kind", "TEXT");
    ensure_layer_column(db, "source_recipe", "TEXT");
    ensure_layer_column(db, "source_collection", "TEXT");
    ensure_layer_column(db, "source_file", "TEXT");
    ensure_layer_column(db, "generated_by", "TEXT");
    ensure_layer_column(db, "verified_at", "TEXT");
    ensure_layer_column(db, "families_text", "TEXT");
    ensure_layer_column(db, "capabilities_text", "TEXT");
    ensure_layer_column(db, "workflow_steps_text", "TEXT");
    ensure_layer_column(db, "shape_constraints_text", "TEXT");
    ensure_layer_column(db, "dtype_constraints_text", "TEXT");
    ensure_layer_column(db, "compatibility_tags_text", "TEXT");
    ensure_layer_column(db, "materialization_status", "TEXT");
    ensure_layer_column(db, "verification_status", "TEXT");
    ensure_layer_column(db, "artifact_json", "TEXT");
}

static void trim_ws(char *s) {
    char *start = s;
    char *end;
    if (!s || !s[0]) return;
    while (*start && isspace((unsigned char)*start)) start++;
    if (start != s) memmove(s, start, strlen(start) + 1);
    end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) end--;
    *end = '\0';
}

static int csv_has_token(const char *csv, const char *token) {
    char copy[2048];
    char *save = NULL;
    if (!csv || !csv[0] || !token || !token[0]) return 0;
    snprintf(copy, sizeof(copy), "%s", csv);
    for (char *part = strtok_r(copy, ",", &save); part; part = strtok_r(NULL, ",", &save)) {
        trim_ws(part);
        if (strcmp(part, token) == 0) return 1;
    }
    return 0;
}

static void csv_add_unique(char *csv, size_t csv_sz, const char *token) {
    size_t len;
    if (!csv || csv_sz == 0 || !token || !token[0]) return;
    if (csv_has_token(csv, token)) return;
    len = strlen(csv);
    if (len > 0 && len + 2 < csv_sz) {
        csv[len++] = ',';
        csv[len++] = ' ';
        csv[len] = '\0';
    }
    strncat(csv, token, csv_sz - strlen(csv) - 1);
}

static void json_escape_copy(const char *src, char *dst, size_t dst_sz) {
    size_t used = 0;
    if (!dst || dst_sz == 0) return;
    dst[0] = '\0';
    if (!src) return;
    while (*src && used + 2 < dst_sz) {
        if (*src == '"' || *src == '\\') {
            if (used + 2 >= dst_sz) break;
            dst[used++] = '\\';
            dst[used++] = *src++;
        } else if (*src == '\n' || *src == '\r') {
            if (used + 2 >= dst_sz) break;
            dst[used++] = '\\';
            dst[used++] = 'n';
            src++;
        } else {
            dst[used++] = *src++;
        }
    }
    dst[used] = '\0';
}

static void csv_to_json_array(const char *csv, char *out, size_t out_sz) {
    char copy[4096];
    char *save = NULL;
    int first = 1;
    if (!out || out_sz == 0) return;
    out[0] = '\0';
    strncat(out, "[", out_sz - 1);
    if (csv && csv[0]) {
        snprintf(copy, sizeof(copy), "%s", csv);
        for (char *part = strtok_r(copy, ",", &save); part; part = strtok_r(NULL, ",", &save)) {
            char esc[768];
            trim_ws(part);
            if (!part[0]) continue;
            json_escape_copy(part, esc, sizeof(esc));
            if (!first) strncat(out, ",", out_sz - strlen(out) - 1);
            strncat(out, "\"", out_sz - strlen(out) - 1);
            strncat(out, esc, out_sz - strlen(out) - 1);
            strncat(out, "\"", out_sz - strlen(out) - 1);
            first = 0;
        }
    }
    strncat(out, "]", out_sz - strlen(out) - 1);
}

static void build_layer_artifact_json(LayerArtifactRecord *rec) {
    char families_json[3072], capabilities_json[3072], steps_json[3072];
    char shapes_json[3072], dtypes_json[2048], tags_json[3072];
    char esc_id[384], esc_name[512], esc_kind[128], esc_model[1024], esc_recipe[512];
    char esc_collection[512], esc_file[2048], esc_generated[256], esc_created[128];
    char esc_verified[128], esc_spec_type[256], esc_spec_value[512], esc_format[128];
    char esc_artifact_path[2048], esc_onnx_path[2048], esc_sha[256];
    const char *ext_key = rec->artifact_kind[0] ? rec->artifact_kind : "virtual_composite";
    char verified_literal[160];
    char binding_kind[64];
    csv_to_json_array(rec->families_text, families_json, sizeof(families_json));
    csv_to_json_array(rec->capabilities_text, capabilities_json, sizeof(capabilities_json));
    csv_to_json_array(rec->workflow_steps_text, steps_json, sizeof(steps_json));
    csv_to_json_array(rec->shape_constraints_text, shapes_json, sizeof(shapes_json));
    csv_to_json_array(rec->dtype_constraints_text, dtypes_json, sizeof(dtypes_json));
    csv_to_json_array(rec->compatibility_tags_text, tags_json, sizeof(tags_json));
    json_escape_copy(rec->id, esc_id, sizeof(esc_id));
    json_escape_copy(rec->name, esc_name, sizeof(esc_name));
    json_escape_copy(rec->artifact_kind, esc_kind, sizeof(esc_kind));
    json_escape_copy(rec->source_model, esc_model, sizeof(esc_model));
    json_escape_copy(rec->source_recipe, esc_recipe, sizeof(esc_recipe));
    json_escape_copy(rec->source_collection, esc_collection, sizeof(esc_collection));
    json_escape_copy(rec->source_file, esc_file, sizeof(esc_file));
    json_escape_copy(rec->generated_by, esc_generated, sizeof(esc_generated));
    json_escape_copy(rec->created_at, esc_created, sizeof(esc_created));
    json_escape_copy(rec->verified_at, esc_verified, sizeof(esc_verified));
    json_escape_copy(rec->layer_spec_type, esc_spec_type, sizeof(esc_spec_type));
    json_escape_copy(rec->layer_spec_value, esc_spec_value, sizeof(esc_spec_value));
    json_escape_copy(rec->format, esc_format, sizeof(esc_format));
    json_escape_copy(rec->artifact_path, esc_artifact_path, sizeof(esc_artifact_path));
    json_escape_copy(rec->onnx_path, esc_onnx_path, sizeof(esc_onnx_path));
    json_escape_copy(rec->sha256, esc_sha, sizeof(esc_sha));
    snprintf(binding_kind, sizeof(binding_kind), "%s",
             strcmp(rec->artifact_kind, "onnx_slice") == 0 ? "graph_node_range" : "tensor_glob");
    if (rec->verified_at[0]) snprintf(verified_literal, sizeof(verified_literal), "\"%s\"", esc_verified);
    else snprintf(verified_literal, sizeof(verified_literal), "null");
    snprintf(rec->artifact_json, sizeof(rec->artifact_json),
             "{"
             "\"schema_version\":\"%s\","
             "\"artifact_id\":\"%s\","
             "\"artifact_kind\":\"%s\","
             "\"name\":\"%s\","
             "\"families\":%s,"
             "\"capabilities\":%s,"
             "\"workflow_steps\":%s,"
             "\"input_signature\":{\"semantic_roles\":[\"unknown\"],\"shape_refs\":%s,\"dtype_refs\":%s},"
             "\"output_signature\":{\"semantic_roles\":[\"unknown\"],\"shape_refs\":%s,\"dtype_refs\":%s},"
             "\"tensor_bindings\":[{\"binding_kind\":\"%s\",\"pattern\":\"%s\",\"verified\":%s}],"
             "\"shape_constraints\":%s,"
             "\"dtype_constraints\":%s,"
             "\"runtime_requirements\":{\"formats\":[\"%s\"],\"bridges_allowed\":true},"
             "\"materialization_status\":\"%s\","
             "\"verification_status\":\"%s\","
             "\"compatibility_tags\":%s,"
             "\"provenance\":{\"source_recipe\":\"%s\",\"source_model\":\"%s\","
             "\"source_collection\":\"%s\",\"source_file\":\"%s\",\"generated_by\":\"%s\","
             "\"created_at\":\"%s\",\"verified_at\":%s},"
             "\"%s\":{\"source_model\":\"%s\",\"layer_spec_type\":\"%s\",\"layer_spec_value\":\"%s\","
             "\"format\":\"%s\",\"artifact_path\":\"%s\",\"onnx_path\":\"%s\","
             "\"node_range\":[%d,%d],\"n_nodes\":%d,\"n_params\":%d,\"sha256\":\"%s\"}"
             "}",
             rec->schema_version[0] ? rec->schema_version : "layer_artifact.v1",
             esc_id,
             esc_kind,
             esc_name,
             families_json,
             capabilities_json,
             steps_json,
             shapes_json,
             dtypes_json,
             shapes_json,
             dtypes_json,
             binding_kind,
             esc_spec_value,
             strcmp(rec->verification_status, "verified") == 0 ? "true" : "false",
             shapes_json,
             dtypes_json,
             esc_format[0] ? esc_format : "metadata",
             rec->materialization_status[0] ? rec->materialization_status : "indexed",
             rec->verification_status[0] ? rec->verification_status : "unverified",
             tags_json,
             esc_recipe,
             esc_model,
             esc_collection,
             esc_file,
             esc_generated,
             esc_created,
             verified_literal,
             ext_key,
             esc_model,
             esc_spec_type,
             esc_spec_value,
             esc_format,
             esc_artifact_path,
             esc_onnx_path,
             rec->node_start,
             rec->node_end,
             rec->n_nodes,
             rec->n_params,
             esc_sha);
}

static int upsert_layer_artifact(sqlite3 *db, LayerArtifactRecord *rec) {
    sqlite3_stmt *st = NULL;
    prepare_layer_db_schema(db);
    build_layer_artifact_json(rec);
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO layers("
        "id,name,artifact_type,source_model,layer_spec_type,layer_spec_value,node_start,node_end,"
        "n_nodes,n_params,sha256,format,artifact_path,onnx_path,created_at,"
        "schema_version,artifact_kind,source_recipe,source_collection,source_file,generated_by,verified_at,"
        "families_text,capabilities_text,workflow_steps_text,shape_constraints_text,dtype_constraints_text,"
        "compatibility_tags_text,materialization_status,verification_status,artifact_json)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, rec->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, rec->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, rec->artifact_kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, rec->source_model, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, rec->layer_spec_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 6, rec->layer_spec_value, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 7, rec->node_start);
    sqlite3_bind_int(st, 8, rec->node_end);
    sqlite3_bind_int(st, 9, rec->n_nodes);
    sqlite3_bind_int(st, 10, rec->n_params);
    sqlite3_bind_text(st, 11, rec->sha256, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 12, rec->format, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 13, rec->artifact_path, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 14, rec->onnx_path, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 15, rec->created_at, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 16, rec->schema_version, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 17, rec->artifact_kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 18, rec->source_recipe, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 19, rec->source_collection, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 20, rec->source_file, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 21, rec->generated_by, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 22, rec->verified_at, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 23, rec->families_text, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 24, rec->capabilities_text, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 25, rec->workflow_steps_text, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 26, rec->shape_constraints_text, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 27, rec->dtype_constraints_text, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 28, rec->compatibility_tags_text, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 29, rec->materialization_status, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 30, rec->verification_status, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 31, rec->artifact_json, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

static void init_layer_artifact(LayerArtifactRecord *rec) {
    memset(rec, 0, sizeof(*rec));
    snprintf(rec->schema_version, sizeof(rec->schema_version), "layer_artifact.v1");
    snprintf(rec->materialization_status, sizeof(rec->materialization_status), "indexed");
    snprintf(rec->verification_status, sizeof(rec->verification_status), "unverified");
}

static void collect_yaml_list_items_from_text(const char *text, const char *key, char *out_csv, size_t out_sz) {
    const char *line = text;
    int in_block = 0;
    char key_pattern[128];
    if (!text || !out_csv || out_sz == 0) return;
    out_csv[0] = '\0';
    snprintf(key_pattern, sizeof(key_pattern), "%s:", key);
    while (*line) {
        const char *next = strchr(line, '\n');
        char buf[1024];
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
                char item[256];
                snprintf(item, sizeof(item), "%s", trimmed + 1);
                trim_ws(item);
                csv_add_unique(out_csv, out_sz, item);
            } else if (*trimmed && *trimmed != '#') {
                break;
            }
        }
        if (!next) break;
        line = next + 1;
    }
}

static void collect_families_from_catalog(sqlite3 *catalog, const char *layer_node_id, char *out_csv, size_t out_sz) {
    sqlite3_stmt *st = NULL;
    out_csv[0] = '\0';
    if (sqlite3_prepare_v2(catalog,
        "SELECT dst_node_id FROM catalog_edges WHERE src_node_id=? AND rel='classified_as_family'",
        -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, layer_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *dst = (const char *)sqlite3_column_text(st, 0);
        if (dst && strncmp(dst, "family:", 7) == 0) csv_add_unique(out_csv, out_sz, dst + 7);
    }
    sqlite3_finalize(st);
}

static void collect_steps_from_recipe(sqlite3 *catalog, const char *recipe_id, char *out_csv, size_t out_sz) {
    sqlite3_stmt *st = NULL;
    char recipe_node_id[320];
    out_csv[0] = '\0';
    if (!recipe_id || !recipe_id[0]) return;
    snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_id);
    if (sqlite3_prepare_v2(catalog,
        "SELECT external_id FROM catalog_nodes WHERE node_id IN ("
        "SELECT dst_node_id FROM catalog_edges WHERE src_node_id=? AND rel='has_step'"
        ")",
        -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, recipe_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *step = (const char *)sqlite3_column_text(st, 0);
        if (step) csv_add_unique(out_csv, out_sz, step);
    }
    sqlite3_finalize(st);
}

static void collect_capabilities_from_recipe(sqlite3 *catalog, const char *recipe_id, char *out_csv, size_t out_sz) {
    sqlite3_stmt *st = NULL;
    char recipe_node_id[320];
    const char *yaml = NULL;
    out_csv[0] = '\0';
    if (!recipe_id || !recipe_id[0]) return;
    snprintf(recipe_node_id, sizeof(recipe_node_id), "recipe:%s", recipe_id);
    if (sqlite3_prepare_v2(catalog,
        "SELECT json_data FROM catalog_nodes WHERE node_id=?",
        -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, recipe_node_id, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) yaml = (const char *)sqlite3_column_text(st, 0);
    if (yaml) collect_yaml_list_items_from_text(yaml, "capabilities", out_csv, out_sz);
    sqlite3_finalize(st);
}

static void migrate_legacy_layer_artifacts(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT id,name,artifact_type,source_model,layer_spec_type,layer_spec_value,node_start,node_end,"
        "n_nodes,n_params,sha256,format,artifact_path,onnx_path,created_at,artifact_kind,artifact_json "
        "FROM layers ORDER BY id",
        -1, &st, NULL) != SQLITE_OK) return;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *artifact_kind = (const char *)sqlite3_column_text(st, 15);
        const char *artifact_json = (const char *)sqlite3_column_text(st, 16);
        LayerArtifactRecord rec;
        if (artifact_kind && artifact_kind[0] && artifact_json && artifact_json[0]) continue;
        init_layer_artifact(&rec);
        snprintf(rec.id, sizeof(rec.id), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
        snprintf(rec.name, sizeof(rec.name), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : rec.id);
        snprintf(rec.artifact_kind, sizeof(rec.artifact_kind), "onnx_slice");
        snprintf(rec.source_model, sizeof(rec.source_model), "%s", sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
        snprintf(rec.layer_spec_type, sizeof(rec.layer_spec_type), "%s", sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "graph_node_range");
        snprintf(rec.layer_spec_value, sizeof(rec.layer_spec_value), "%s", sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "");
        rec.node_start = sqlite3_column_int(st, 6);
        rec.node_end = sqlite3_column_int(st, 7);
        rec.n_nodes = sqlite3_column_int(st, 8);
        rec.n_params = sqlite3_column_int(st, 9);
        snprintf(rec.sha256, sizeof(rec.sha256), "%s", sqlite3_column_text(st, 10) ? (const char *)sqlite3_column_text(st, 10) : "");
        snprintf(rec.format, sizeof(rec.format), "%s", sqlite3_column_text(st, 11) ? (const char *)sqlite3_column_text(st, 11) : "onnx");
        snprintf(rec.artifact_path, sizeof(rec.artifact_path), "%s", sqlite3_column_text(st, 12) ? (const char *)sqlite3_column_text(st, 12) : "");
        snprintf(rec.onnx_path, sizeof(rec.onnx_path), "%s", sqlite3_column_text(st, 13) ? (const char *)sqlite3_column_text(st, 13) : "");
        snprintf(rec.created_at, sizeof(rec.created_at), "%s", sqlite3_column_text(st, 14) ? (const char *)sqlite3_column_text(st, 14) : "");
        snprintf(rec.source_file, sizeof(rec.source_file), "%s", rec.artifact_path[0] ? rec.artifact_path : rec.onnx_path);
        snprintf(rec.generated_by, sizeof(rec.generated_by), "bonfyre-layer");
        snprintf(rec.materialization_status, sizeof(rec.materialization_status), "materialized");
        snprintf(rec.verification_status, sizeof(rec.verification_status), "verified");
        csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), "onnx");
        csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), "legacy");
        upsert_layer_artifact(db, &rec);
    }
    sqlite3_finalize(st);
}

static void sync_hf_catalog_layers_to_registry(sqlite3 *db) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char catalog_db[MAX_PATH];
    bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
    if (bf_sqlite3_open_ro(catalog_db, &catalog) != SQLITE_OK) {
        sqlite3_close(catalog);
        return;
    }
    if (sqlite3_prepare_v2(catalog,
        "SELECT node_id, external_id, name, category, source_path, json_data "
        "FROM catalog_nodes WHERE kind='layer' AND category='hf_tensor_surface' ORDER BY external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(catalog);
        return;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        LayerArtifactRecord rec;
        const char *node_id = (const char *)sqlite3_column_text(st, 0);
        const char *external_id = (const char *)sqlite3_column_text(st, 1);
        const char *name = (const char *)sqlite3_column_text(st, 2);
        const char *source_path = (const char *)sqlite3_column_text(st, 4);
        const char *json = (const char *)sqlite3_column_text(st, 5);
        init_layer_artifact(&rec);
        snprintf(rec.id, sizeof(rec.id), "%s", external_id ? external_id : "");
        snprintf(rec.name, sizeof(rec.name), "%s", name ? name : rec.id);
        snprintf(rec.artifact_kind, sizeof(rec.artifact_kind), "hf_tensor_surface");
        snprintf(rec.format, sizeof(rec.format), "safetensors");
        snprintf(rec.source_file, sizeof(rec.source_file), "%s", source_path ? source_path : "");
        snprintf(rec.generated_by, sizeof(rec.generated_by), "bf_catalog_sync_hf_recipe");
        bf_iso_timestamp(rec.created_at, sizeof(rec.created_at));
        snprintf(rec.materialization_status, sizeof(rec.materialization_status), "indexed");
        snprintf(rec.verification_status, sizeof(rec.verification_status), "unverified");
        if (json && json[0]) {
            bf_json_str(json, "source_model", rec.source_model, sizeof(rec.source_model));
            bf_json_str(json, "source_recipe", rec.source_recipe, sizeof(rec.source_recipe));
            bf_json_str(json, "source_collection", rec.source_collection, sizeof(rec.source_collection));
            bf_json_str(json, "layer_spec_type", rec.layer_spec_type, sizeof(rec.layer_spec_type));
            bf_json_str(json, "layer_spec_value", rec.layer_spec_value, sizeof(rec.layer_spec_value));
        }
        collect_families_from_catalog(catalog, node_id ? node_id : "", rec.families_text, sizeof(rec.families_text));
        collect_steps_from_recipe(catalog, rec.source_recipe, rec.workflow_steps_text, sizeof(rec.workflow_steps_text));
        collect_capabilities_from_recipe(catalog, rec.source_recipe, rec.capabilities_text, sizeof(rec.capabilities_text));
        csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), "hf");
        csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), "tensor-surface");
        if (rec.source_collection[0]) csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), rec.source_collection);
        upsert_layer_artifact(db, &rec);
    }
    sqlite3_finalize(st);
    sqlite3_close(catalog);
}

static void prepare_layer_registry(sqlite3 *db) {
    prepare_layer_db_schema(db);
    migrate_legacy_layer_artifacts(db);
    sync_hf_catalog_layers_to_registry(db);
}

static int load_layer_artifact(sqlite3 *db, const char *id, LayerArtifactRecord *rec) {
    sqlite3_stmt *st = NULL;
    init_layer_artifact(rec);
    if (sqlite3_prepare_v2(db,
        "SELECT id,name,artifact_kind,source_model,source_recipe,source_collection,source_file,generated_by,"
        "created_at,verified_at,layer_spec_type,layer_spec_value,format,artifact_path,onnx_path,sha256,"
        "families_text,capabilities_text,workflow_steps_text,shape_constraints_text,dtype_constraints_text,"
        "compatibility_tags_text,materialization_status,verification_status,node_start,node_end,n_nodes,"
        "n_params,artifact_json "
        "FROM layers WHERE id=? OR name=? LIMIT 1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, id, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return 0;
    }
    snprintf(rec->id, sizeof(rec->id), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
    snprintf(rec->name, sizeof(rec->name), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : rec->id);
    snprintf(rec->artifact_kind, sizeof(rec->artifact_kind), "%s", sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "");
    snprintf(rec->source_model, sizeof(rec->source_model), "%s", sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
    snprintf(rec->source_recipe, sizeof(rec->source_recipe), "%s", sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "");
    snprintf(rec->source_collection, sizeof(rec->source_collection), "%s", sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "");
    snprintf(rec->source_file, sizeof(rec->source_file), "%s", sqlite3_column_text(st, 6) ? (const char *)sqlite3_column_text(st, 6) : "");
    snprintf(rec->generated_by, sizeof(rec->generated_by), "%s", sqlite3_column_text(st, 7) ? (const char *)sqlite3_column_text(st, 7) : "");
    snprintf(rec->created_at, sizeof(rec->created_at), "%s", sqlite3_column_text(st, 8) ? (const char *)sqlite3_column_text(st, 8) : "");
    snprintf(rec->verified_at, sizeof(rec->verified_at), "%s", sqlite3_column_text(st, 9) ? (const char *)sqlite3_column_text(st, 9) : "");
    snprintf(rec->layer_spec_type, sizeof(rec->layer_spec_type), "%s", sqlite3_column_text(st, 10) ? (const char *)sqlite3_column_text(st, 10) : "");
    snprintf(rec->layer_spec_value, sizeof(rec->layer_spec_value), "%s", sqlite3_column_text(st, 11) ? (const char *)sqlite3_column_text(st, 11) : "");
    snprintf(rec->format, sizeof(rec->format), "%s", sqlite3_column_text(st, 12) ? (const char *)sqlite3_column_text(st, 12) : "");
    snprintf(rec->artifact_path, sizeof(rec->artifact_path), "%s", sqlite3_column_text(st, 13) ? (const char *)sqlite3_column_text(st, 13) : "");
    snprintf(rec->onnx_path, sizeof(rec->onnx_path), "%s", sqlite3_column_text(st, 14) ? (const char *)sqlite3_column_text(st, 14) : "");
    snprintf(rec->sha256, sizeof(rec->sha256), "%s", sqlite3_column_text(st, 15) ? (const char *)sqlite3_column_text(st, 15) : "");
    snprintf(rec->families_text, sizeof(rec->families_text), "%s", sqlite3_column_text(st, 16) ? (const char *)sqlite3_column_text(st, 16) : "");
    snprintf(rec->capabilities_text, sizeof(rec->capabilities_text), "%s", sqlite3_column_text(st, 17) ? (const char *)sqlite3_column_text(st, 17) : "");
    snprintf(rec->workflow_steps_text, sizeof(rec->workflow_steps_text), "%s", sqlite3_column_text(st, 18) ? (const char *)sqlite3_column_text(st, 18) : "");
    snprintf(rec->shape_constraints_text, sizeof(rec->shape_constraints_text), "%s", sqlite3_column_text(st, 19) ? (const char *)sqlite3_column_text(st, 19) : "");
    snprintf(rec->dtype_constraints_text, sizeof(rec->dtype_constraints_text), "%s", sqlite3_column_text(st, 20) ? (const char *)sqlite3_column_text(st, 20) : "");
    snprintf(rec->compatibility_tags_text, sizeof(rec->compatibility_tags_text), "%s", sqlite3_column_text(st, 21) ? (const char *)sqlite3_column_text(st, 21) : "");
    snprintf(rec->materialization_status, sizeof(rec->materialization_status), "%s", sqlite3_column_text(st, 22) ? (const char *)sqlite3_column_text(st, 22) : "");
    snprintf(rec->verification_status, sizeof(rec->verification_status), "%s", sqlite3_column_text(st, 23) ? (const char *)sqlite3_column_text(st, 23) : "");
    rec->node_start = sqlite3_column_int(st, 24);
    rec->node_end = sqlite3_column_int(st, 25);
    rec->n_nodes = sqlite3_column_int(st, 26);
    rec->n_params = sqlite3_column_int(st, 27);
    snprintf(rec->artifact_json, sizeof(rec->artifact_json), "%s", sqlite3_column_text(st, 28) ? (const char *)sqlite3_column_text(st, 28) : "");
    sqlite3_finalize(st);
    return 1;
}

static void register_layer_artifact(const char *artifact_path, const char *onnx_path) {
    size_t len = 0;
    char *json = bf_read_file(artifact_path, &len);
    char db_path[MAX_PATH];
    LayerArtifactRecord rec;
    if (!json) return;

    init_layer_artifact(&rec);
    json_extract_field(json, "source_model", rec.source_model, sizeof(rec.source_model));
    json_extract_field(json, "created_at", rec.created_at, sizeof(rec.created_at));
    json_extract_field(json, "sha256", rec.sha256, sizeof(rec.sha256));
    json_extract_field(json, "format", rec.format, sizeof(rec.format));
    json_extract_field(json, "name", rec.name, sizeof(rec.name));
    bf_json_int(json, "n_nodes", &rec.n_nodes);
    bf_json_int(json, "n_params", &rec.n_params);

    const char *layer_spec = strstr(json, "\"layer_spec\"");
    if (layer_spec) {
        if (!bf_json_str(layer_spec, "type", rec.layer_spec_type, sizeof(rec.layer_spec_type))) rec.layer_spec_type[0] = '\0';
        if (!bf_json_str(layer_spec, "value", rec.layer_spec_value, sizeof(rec.layer_spec_value))) rec.layer_spec_value[0] = '\0';
    }
    const char *node_range = strstr(json, "\"node_range\"");
    if (node_range) sscanf(node_range, "%*[^[][%d, %d]", &rec.node_start, &rec.node_end);

    if (!rec.name[0]) snprintf(rec.name, sizeof(rec.name), "%s", rec.layer_spec_value[0] ? rec.layer_spec_value : "onnx_slice");
    if (!rec.created_at[0]) bf_iso_timestamp(rec.created_at, sizeof(rec.created_at));
    if (!rec.format[0]) snprintf(rec.format, sizeof(rec.format), "%s", "onnx");
    snprintf(rec.id, sizeof(rec.id), "%s:%s:%s",
             "onnx_slice",
             rec.layer_spec_type[0] ? rec.layer_spec_type : "artifact",
             rec.sha256[0] ? rec.sha256 : rec.created_at);
    snprintf(rec.artifact_kind, sizeof(rec.artifact_kind), "onnx_slice");
    snprintf(rec.artifact_path, sizeof(rec.artifact_path), "%s", artifact_path);
    snprintf(rec.onnx_path, sizeof(rec.onnx_path), "%s", onnx_path ? onnx_path : "");
    snprintf(rec.source_file, sizeof(rec.source_file), "%s", artifact_path);
    snprintf(rec.generated_by, sizeof(rec.generated_by), "bonfyre-layer");
    snprintf(rec.materialization_status, sizeof(rec.materialization_status), "materialized");
    snprintf(rec.verification_status, sizeof(rec.verification_status), "verified");
    snprintf(rec.verified_at, sizeof(rec.verified_at), "%s", rec.created_at);
    csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), "onnx");
    csv_add_unique(rec.compatibility_tags_text, sizeof(rec.compatibility_tags_text), "slice");

    sqlite3 *db = open_layer_db(NULL, db_path, sizeof(db_path));
    if (!db) {
        free(json);
        return;
    }
    prepare_layer_registry(db);
    upsert_layer_artifact(db, &rec);
    sqlite3_close(db);

    {
        char catalog_db[MAX_PATH];
        bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
        bf_catalog_sync_default(catalog_db);
    }
    free(json);
}

static int cmd_registry(const char *root) {
    char db_path[MAX_PATH];
    sqlite3 *db = open_layer_db(root, db_path, sizeof(db_path));
    if (!db) {
        fprintf(stderr, "bonfyre-layer: failed to open registry\n");
        return 1;
    }
    printf("bonfyre-layer registry\n");
    printf("db    %s\n\n", db_path);
    if (sqlite_table_exists(db, "layer_artifacts")) {
        sqlite3_stmt *st = NULL;
        sqlite3_prepare_v2(db,
            "SELECT artifact_id,artifact_kind,materialization_status,verification_status "
            "FROM layer_artifacts ORDER BY artifact_id ASC",
            -1, &st, NULL);
        printf("%-72s %-20s %-18s %s\n",
               "ID", "TYPE", "MATERIALIZATION", "VERIFICATION");
        while (sqlite3_step(st) == SQLITE_ROW) {
            printf("%-72s %-20s %-18s %s\n",
                   sqlite3_column_text(st,0) ? (const char*)sqlite3_column_text(st,0) : "",
                   sqlite3_column_text(st,1) ? (const char*)sqlite3_column_text(st,1) : "",
                   sqlite3_column_text(st,2) ? (const char*)sqlite3_column_text(st,2) : "",
                   sqlite3_column_text(st,3) ? (const char*)sqlite3_column_text(st,3) : "");
        }
        sqlite3_finalize(st);
    } else {
        prepare_layer_registry(db);
        sqlite3_stmt *st = NULL;
        sqlite3_prepare_v2(db,
            "SELECT id,artifact_kind,layer_spec_type,layer_spec_value,n_nodes,n_params "
            "FROM layers ORDER BY created_at DESC, id ASC",
            -1, &st, NULL);
        printf("%-40s %-18s %-14s %-18s %7s %9s\n",
               "ID", "TYPE", "SPEC TYPE", "SPEC", "NODES", "PARAMS");
        while (sqlite3_step(st) == SQLITE_ROW) {
            printf("%-40s %-18s %-14s %-18s %7d %9d\n",
                   sqlite3_column_text(st,0) ? (const char*)sqlite3_column_text(st,0) : "",
                   sqlite3_column_text(st,1) ? (const char*)sqlite3_column_text(st,1) : "",
                   sqlite3_column_text(st,2) ? (const char*)sqlite3_column_text(st,2) : "",
                   sqlite3_column_text(st,3) ? (const char*)sqlite3_column_text(st,3) : "",
                   sqlite3_column_int(st,4),
                   sqlite3_column_int(st,5));
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Write extracted ONNX model (node slice + required initializers)
 * ═══════════════════════════════════════════════════════════════════ */

static int write_onnx_slice(const char *path,
                             const BfLayerGraph *g,
                             size_t node_start, size_t node_end) {
    /* Collect names consumed by selected nodes and not produced by them */
    /* Mark all outputs produced by selected nodes */
    char produced[MAX_NODES][MAX_NAME]; size_t n_produced = 0;
    for (size_t i = node_start; i < node_end && i < g->n_nodes; i++) {
        for (int o = 0; o < g->nodes[i].n_outputs; o++) {
            if (g->nodes[i].outputs[o][0] && n_produced < MAX_NODES)
                strncpy(produced[n_produced++], g->nodes[i].outputs[o], MAX_NAME-1);
        }
    }
    /* Collect initializers needed */
    WBuf graph_buf; wbuf_init(&graph_buf);
    /* nodes */
    for (size_t i = node_start; i < node_end && i < g->n_nodes; i++) {
        const BfLayerNode *nd = &g->nodes[i];
        if (nd->raw && nd->raw_len)
            wbuf_write_bytes(&graph_buf, 1, nd->raw, nd->raw_len);
    }
    /* initializers that are consumed but not produced internally */
    for (size_t i = node_start; i < node_end && i < g->n_nodes; i++) {
        const BfLayerNode *nd = &g->nodes[i];
        for (int inp = 0; inp < nd->n_inputs; inp++) {
            const char *iname = nd->inputs[inp];
            if (!iname[0]) continue;
            /* Check if it's a graph initializer */
            int already_produced = 0;
            for (size_t p = 0; p < n_produced && !already_produced; p++)
                if (strcmp(produced[p], iname) == 0) already_produced = 1;
            if (already_produced) continue;
            for (size_t ki = 0; ki < g->n_inits; ki++) {
                if (strcmp(g->inits[ki].name, iname) == 0 && g->inits[ki].raw) {
                    wbuf_write_bytes(&graph_buf, 5, g->inits[ki].raw, g->inits[ki].raw_len);
                    break;
                }
            }
        }
    }
    /* Wrap graph in ModelProto */
    WBuf model_buf; wbuf_init(&model_buf);
    wbuf_write_i64(&model_buf, 1, 8); /* ir_version = 8 */
    /* opset_import { version=17 } */
    WBuf opset_buf; wbuf_init(&opset_buf);
    wbuf_write_string(&opset_buf, 1, ""); /* domain = "" */
    wbuf_write_i64(&opset_buf, 2, 17);   /* version = 17 */
    wbuf_write_bytes(&model_buf, 8, opset_buf.data, opset_buf.len);
    wbuf_free(&opset_buf);
    wbuf_write_bytes(&model_buf, 7, graph_buf.data, graph_buf.len);
    wbuf_free(&graph_buf);

    int rc = wbuf_save(&model_buf, path);
    wbuf_free(&model_buf);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 * Write artifact.json
 * ═══════════════════════════════════════════════════════════════════ */

static void write_artifact(const char *artifact_path, const char *onnx_path,
                            const char *artifact_type,
                            const char *source_model, const char *layer_spec_type,
                            size_t node_start, size_t node_end, size_t n_nodes,
                            const char *name_or_range, size_t n_params) {
    char sha[65]; sha256_file_hex(onnx_path, sha);
    char ts[32]; bf_iso_timestamp(ts, sizeof(ts));

    FILE *f = fopen(artifact_path, "w");
    if (!f) { perror(artifact_path); return; }

    fprintf(f,
        "{\n"
        "  \"type\": \"%s\",\n"
        "  \"version\": \"1.0.0\",\n"
        "  \"source_model\": \"%s\",\n"
        "  \"layer_spec\": {\n"
        "    \"type\": \"%s\",\n"
        "    \"value\": \"%s\"\n"
        "  },\n"
        "  \"node_range\": [%zu, %zu],\n"
        "  \"n_nodes\": %zu,\n"
        "  \"n_params\": %zu,\n"
        "  \"sha256\": \"%s\",\n"
        "  \"fpq_sha256\": null,\n"
        "  \"format\": \"onnx\",\n"
        "  \"created_at\": \"%s\"\n"
        "}\n",
        artifact_type, source_model,
        layer_spec_type, name_or_range,
        node_start, node_end,
        n_nodes, n_params, sha, ts);
    fclose(f);
}

/* Count params in selected node range (sum up initializer n_elements for used inits) */
static size_t count_range_params(const BfLayerGraph *g, size_t start, size_t end) {
    size_t total = 0;
    /* For each initializer name consumed by nodes[start:end], sum elements */
    for (size_t i = start; i < end && i < g->n_nodes; i++) {
        for (int inp = 0; inp < g->nodes[i].n_inputs; inp++) {
            const char *iname = g->nodes[i].inputs[inp];
            for (size_t ki = 0; ki < g->n_inits; ki++) {
                if (strcmp(g->inits[ki].name, iname) == 0) {
                    total += g->inits[ki].n_elements;
                    break;
                }
            }
        }
    }
    return total;
}

/* ═══════════════════════════════════════════════════════════════════
 * Commands
 * ═══════════════════════════════════════════════════════════════════ */

static int cmd_inspect(const char *model_path) {
    BfLayerGraph g;
    if (load_graph(model_path, &g)) {
        fprintf(stderr, "bonfyre-layer: failed to load %s\n", model_path);
        return 1;
    }
    /* Count total params */
    size_t total_params = 0;
    for (size_t i = 0; i < g.n_inits; i++) total_params += g.inits[i].n_elements;

    /* Op distribution */
    char ops[64][32]; int op_counts[64]; int n_ops = 0;
    for (size_t i = 0; i < g.n_nodes; i++) {
        int found = 0;
        for (int j = 0; j < n_ops; j++) {
            if (strcmp(ops[j], g.nodes[i].op_type) == 0) { op_counts[j]++; found = 1; break; }
        }
        if (!found && n_ops < 64) {
            strncpy(ops[n_ops], g.nodes[i].op_type, 31);
            op_counts[n_ops] = 1; n_ops++;
        }
    }

    printf("Model:      %s\n", model_path);
    printf("  IR ver:   %lld\n", (long long)g.ir_version);
    printf("  Opset:    %lld\n", (long long)g.opset_version);
    printf("  Nodes:    %zu\n", g.n_nodes);
    printf("  Inits:    %zu\n", g.n_inits);
    printf("  Params:   %zu\n\n", total_params);
    printf("Op distribution:\n");
    for (int i = 0; i < n_ops; i++)
        printf("  %-20s %d\n", ops[i], op_counts[i]);
    printf("\nLayer pull range: 0:%zu  (all nodes)\n", g.n_nodes);
    printf("  Example: bonfyre-layer pull-layer %s --range 0:%zu --out /tmp/layer\n",
           model_path, g.n_nodes / 2);

    bf_graph_free(&g);
    return 0;
}

static int cmd_layers(const char *model_path) {
    BfLayerGraph g;
    if (load_graph(model_path, &g)) return 1;

    printf("%-5s %-18s %-35s %s\n", "IDX", "OP", "NAME", "OUTPUTS");
    printf("%-5s %-18s %-35s %s\n", "---", "--", "----", "-------");
    for (size_t i = 0; i < g.n_nodes; i++) {
        const BfLayerNode *nd = &g.nodes[i];
        char out_str[128] = "";
        for (int o = 0; o < nd->n_outputs && o < 2; o++) {
            if (o > 0) strncat(out_str, ", ", sizeof(out_str)-1-strlen(out_str));
            strncat(out_str, nd->outputs[o], sizeof(out_str)-1-strlen(out_str));
        }
        if (nd->n_outputs > 2) strncat(out_str, " ...", sizeof(out_str)-1-strlen(out_str));
        printf("%-5zu %-18.18s %-35.35s %s\n", i, nd->op_type, nd->name, out_str);
    }
    bf_graph_free(&g);
    return 0;
}

static int cmd_pull_layer(const char *model_path, const char *range_str,
                           const char *out_dir) {
    BfLayerGraph g;
    if (load_graph(model_path, &g)) return 1;

    size_t start = 0, end = 0;
    if (sscanf(range_str, "%zu:%zu", &start, &end) != 2) {
        fprintf(stderr, "bonfyre-layer: --range must be START:END, got: %s\n", range_str);
        bf_graph_free(&g); return 1;
    }
    if (end > g.n_nodes || start >= end) {
        fprintf(stderr, "bonfyre-layer: range %zu:%zu out of bounds (0:%zu)\n",
                start, end, g.n_nodes);
        bf_graph_free(&g); return 1;
    }

    mkdirp(out_dir);
    char onnx_path[MAX_PATH], artifact_path[MAX_PATH];
    snprintf(onnx_path,     sizeof(onnx_path),     "%s/layer_fragment.onnx", out_dir);
    snprintf(artifact_path, sizeof(artifact_path), "%s/artifact.json",       out_dir);

    if (write_onnx_slice(onnx_path, &g, start, end)) {
        fprintf(stderr, "bonfyre-layer: failed to write %s\n", onnx_path);
        bf_graph_free(&g); return 1;
    }

    size_t n_params = count_range_params(&g, start, end);
    write_artifact(artifact_path, onnx_path, "layer_fragment",
                   model_path, "layer_range",
                   start, end, end - start,
                   range_str, n_params);
    register_layer_artifact(artifact_path, onnx_path);

    printf("[bonfyre-layer] pulled layer range %s\n", range_str);
    printf("  nodes:    %zu\n", end - start);
    printf("  params:   %zu\n", n_params);
    printf("  onnx:     %s\n", onnx_path);
    printf("  artifact: %s\n\n", artifact_path);
    printf("  Next: python3 scripts/finalize_artifact.py %s\n", out_dir);
    printf("        bonfyre-model add %s\n", artifact_path);

    bf_graph_free(&g);
    return 0;
}

static int cmd_pull_head(const char *model_path, const char *name_prefix,
                          const char *out_dir) {
    BfLayerGraph g;
    if (load_graph(model_path, &g)) return 1;

    /* Find nodes matching name prefix or op_type prefix */
    size_t first = SIZE_MAX, last = 0; int found = 0;
    for (size_t i = 0; i < g.n_nodes; i++) {
        int match = (strncmp(g.nodes[i].name, name_prefix, strlen(name_prefix)) == 0 ||
                     strncmp(g.nodes[i].op_type, name_prefix, strlen(name_prefix)) == 0);
        if (match) {
            if (i < first) first = i;
            if (i > last)  last  = i;
            found++;
        }
    }
    if (!found) {
        fprintf(stderr, "bonfyre-layer: no nodes matching '%s'\n", name_prefix);
        printf("Available nodes (first 20):\n");
        for (size_t i = 0; i < g.n_nodes && i < 20; i++)
            printf("  [%3zu] %-16s %s\n", i, g.nodes[i].op_type, g.nodes[i].name);
        bf_graph_free(&g); return 1;
    }

    char range_str[64];
    snprintf(range_str, sizeof(range_str), "%zu:%zu", first, last + 1);
    printf("[bonfyre-layer] matched %d nodes for '%s' → range %s\n",
           found, name_prefix, range_str);

    bf_graph_free(&g);
    return cmd_pull_layer(model_path, range_str, out_dir);
}

static int cmd_pack_transform(const char *name, char **parts, int n_parts,
                               const char *out_dir) {
    if (n_parts == 0) {
        fprintf(stderr, "bonfyre-layer pack-transform: no parts specified\n");
        return 1;
    }
    mkdirp(out_dir);

    /* Load all parts and concatenate their node+initializer raw bytes */
    WBuf graph_buf; wbuf_init(&graph_buf);
    size_t total_params = 0, total_nodes = 0;

    for (int pi = 0; pi < n_parts; pi++) {
        BfLayerGraph g;
        if (load_graph(parts[pi], &g)) {
            fprintf(stderr, "bonfyre-layer: failed to load part %s\n", parts[pi]);
            wbuf_free(&graph_buf); return 1;
        }
        /* Append all nodes */
        for (size_t i = 0; i < g.n_nodes; i++) {
            if (g.nodes[i].raw)
                wbuf_write_bytes(&graph_buf, 1, g.nodes[i].raw, g.nodes[i].raw_len);
        }
        /* Append all initializers */
        for (size_t i = 0; i < g.n_inits; i++) {
            total_params += g.inits[i].n_elements;
            if (g.inits[i].raw)
                wbuf_write_bytes(&graph_buf, 5, g.inits[i].raw, g.inits[i].raw_len);
        }
        total_nodes += g.n_nodes;
        printf("[bonfyre-layer] part %d: %s (%zu nodes)\n", pi+1, parts[pi], g.n_nodes);
        bf_graph_free(&g);
    }

    /* Wrap in ModelProto */
    WBuf model_buf; wbuf_init(&model_buf);
    wbuf_write_i64(&model_buf, 1, 8);
    WBuf opset_buf; wbuf_init(&opset_buf);
    wbuf_write_string(&opset_buf, 1, "");
    wbuf_write_i64(&opset_buf, 2, 17);
    wbuf_write_bytes(&model_buf, 8, opset_buf.data, opset_buf.len);
    wbuf_free(&opset_buf);
    wbuf_write_bytes(&model_buf, 7, graph_buf.data, graph_buf.len);
    wbuf_free(&graph_buf);

    char onnx_path[MAX_PATH], artifact_path[MAX_PATH];
    snprintf(onnx_path,     sizeof(onnx_path),     "%s/transform.onnx", out_dir);
    snprintf(artifact_path, sizeof(artifact_path), "%s/artifact.json",  out_dir);

    if (wbuf_save(&model_buf, onnx_path)) {
        wbuf_free(&model_buf); return 1;
    }
    wbuf_free(&model_buf);

    char sha[65]; sha256_file_hex(onnx_path, sha);
    char ts[32];  bf_iso_timestamp(ts, sizeof(ts));
    FILE *af = fopen(artifact_path, "w");
    if (af) {
        fprintf(af,
            "{\n"
            "  \"type\": \"transform_fragment\",\n"
            "  \"version\": \"1.0.0\",\n"
            "  \"name\": \"%s\",\n"
            "  \"n_parts\": %d,\n"
            "  \"n_nodes\": %zu,\n"
            "  \"n_params\": %zu,\n"
            "  \"sha256\": \"%s\",\n"
            "  \"fpq_sha256\": null,\n"
            "  \"format\": \"onnx\",\n"
            "  \"created_at\": \"%s\"\n"
            "}\n",
            name, n_parts, total_nodes, total_params, sha, ts);
        fclose(af);
    }
    register_layer_artifact(artifact_path, onnx_path);

    printf("[bonfyre-layer] packed transform '%s'\n", name);
    printf("  parts:    %d\n", n_parts);
    printf("  nodes:    %zu\n", total_nodes);
    printf("  params:   %zu\n", total_params);
    printf("  sha256:   %.16s…\n", sha);
    printf("  onnx:     %s\n", onnx_path);
    printf("  artifact: %s\n\n", artifact_path);
    printf("  Next: python3 scripts/finalize_artifact.py %s\n", out_dir);
    printf("        bonfyre-model add %s\n", artifact_path);
    return 0;
}

static void csv_intersection(const char *a, const char *b, char *out, size_t out_sz) {
    char copy[4096];
    char *save = NULL;
    out[0] = '\0';
    if (!a || !a[0] || !b || !b[0]) return;
    snprintf(copy, sizeof(copy), "%s", a);
    for (char *part = strtok_r(copy, ",", &save); part; part = strtok_r(NULL, ",", &save)) {
        trim_ws(part);
        if (part[0] && csv_has_token(b, part)) csv_add_unique(out, out_sz, part);
    }
}

static int csv_count(const char *csv) {
    char copy[4096];
    char *save = NULL;
    int count = 0;
    if (!csv || !csv[0]) return 0;
    snprintf(copy, sizeof(copy), "%s", csv);
    for (char *part = strtok_r(copy, ",", &save); part; part = strtok_r(NULL, ",", &save)) {
        trim_ws(part);
        if (part[0]) count++;
    }
    return count;
}

static void suggest_bridge_kinds(const LayerArtifactRecord *a, const LayerArtifactRecord *b,
                                 char *out, size_t out_sz) {
    out[0] = '\0';
    if ((strcmp(a->artifact_kind, "hf_tensor_surface") == 0 || strcmp(a->artifact_kind, "hf_tensor_pack") == 0) &&
        strcmp(b->artifact_kind, "onnx_slice") == 0) {
        csv_add_unique(out, out_sz, "hf_to_onnx_adapter");
    }
    if (strcmp(a->artifact_kind, "onnx_slice") == 0 &&
        (strcmp(b->artifact_kind, "hf_tensor_surface") == 0 || strcmp(b->artifact_kind, "hf_tensor_pack") == 0)) {
        csv_add_unique(out, out_sz, "onnx_to_hf_adapter");
    }
    if (csv_has_token(a->families_text, "T_PROJECTOR_BRIDGE") || csv_has_token(b->families_text, "T_PROJECTOR_BRIDGE")) {
        csv_add_unique(out, out_sz, "projector_bridge");
    }
    if (strstr(a->compatibility_tags_text, "quant") || strstr(b->compatibility_tags_text, "quant")) {
        csv_add_unique(out, out_sz, "quant_dequant_bridge");
    }
    if (strstr(a->compatibility_tags_text, "cache") || strstr(b->compatibility_tags_text, "cache")) {
        csv_add_unique(out, out_sz, "cache_layout_bridge");
    }
}

static void compute_compatibility(const LayerArtifactRecord *a, const LayerArtifactRecord *b,
                                  char *status, size_t status_sz,
                                  double *confidence,
                                  char *matching_families, size_t matching_sz,
                                  char *mismatches, size_t mismatch_sz,
                                  char *bridges, size_t bridges_sz,
                                  char *reasons, size_t reasons_sz) {
    int shared_family_count;
    int shared_step_count;
    int shape_known = (a->shape_constraints_text[0] && b->shape_constraints_text[0]);
    int dtype_known = (a->dtype_constraints_text[0] && b->dtype_constraints_text[0]);
    int role_known_a = a->layer_spec_value[0] != '\0';
    int role_known_b = b->layer_spec_value[0] != '\0';
    status[0] = mismatches[0] = bridges[0] = reasons[0] = matching_families[0] = '\0';
    *confidence = 0.25;
    csv_intersection(a->families_text, b->families_text, matching_families, matching_sz);
    shared_family_count = csv_count(matching_families);
    {
        char shared_steps[1024];
        csv_intersection(a->workflow_steps_text, b->workflow_steps_text, shared_steps, sizeof(shared_steps));
        shared_step_count = csv_count(shared_steps);
    }
    if (shared_family_count > 0) {
        *confidence += 0.35;
        csv_add_unique(reasons, reasons_sz, "matching T_* families");
    } else {
        csv_add_unique(mismatches, mismatch_sz, "no matching T_* families");
    }
    if (shared_step_count > 0) {
        *confidence += 0.15;
        csv_add_unique(reasons, reasons_sz, "workflow steps overlap");
    } else {
        csv_add_unique(mismatches, mismatch_sz, "workflow steps do not overlap");
    }
    if (shape_known) *confidence += 0.10;
    else csv_add_unique(mismatches, mismatch_sz, "shape constraints unknown or incomplete");
    if (dtype_known) *confidence += 0.10;
    else csv_add_unique(mismatches, mismatch_sz, "dtype constraints unknown or incomplete");
    if (role_known_a && role_known_b) *confidence += 0.10;
    else csv_add_unique(mismatches, mismatch_sz, "semantic input/output role unknown");

    if ((strcmp(a->artifact_kind, "hf_tensor_surface") == 0 || strcmp(a->artifact_kind, "hf_tensor_pack") == 0) &&
        strcmp(b->artifact_kind, "onnx_slice") == 0) {
        if (strcmp(a->verification_status, "verified") != 0) csv_add_unique(mismatches, mismatch_sz, "HF tensor names not verified");
    }
    if (strcmp(a->format, b->format) != 0) {
        suggest_bridge_kinds(a, b, bridges, bridges_sz);
        csv_add_unique(mismatches, mismatch_sz, "runtime format differs");
    }

    if (shared_family_count > 0 && (!bridges[0]) && (shape_known || (!a->shape_constraints_text[0] && !b->shape_constraints_text[0]))) {
        snprintf(status, status_sz, "compatible");
    } else if (shared_family_count > 0 && bridges[0]) {
        snprintf(status, status_sz, "compatible_with_bridge");
    } else if (!shared_family_count && !shape_known && !dtype_known) {
        snprintf(status, status_sz, "unknown");
    } else {
        snprintf(status, status_sz, "incompatible");
    }
    if (*confidence > 1.0) *confidence = 1.0;
}

static int open_prepared_layer_db(const char *root, sqlite3 **db_out, char *path_buf, size_t path_buf_sz) {
    sqlite3 *db = open_layer_db(root, path_buf, path_buf_sz);
    if (!db) return 0;
    prepare_layer_registry(db);
    *db_out = db;
    return 1;
}

static void extract_required_bridges_json_array(const char *compat_json, char *out, size_t out_sz) {
    const char *p;
    const char *start;
    const char *end;
    if (!out || out_sz == 0) return;
    out[0] = '\0';
    if (!compat_json) return;
    p = strstr(compat_json, "\"required_bridges\"");
    if (!p) return;
    start = strchr(p, '[');
    if (!start) return;
    end = strchr(start, ']');
    if (!end) return;
    if ((size_t)(end - start + 1) >= out_sz) return;
    memcpy(out, start, (size_t)(end - start + 1));
    out[end - start + 1] = '\0';
}

static int cmd_compat(const char *root, const char *layer_a, const char *layer_b) {
    char *json = NULL;
    if (bf_layer_compat_json(root, layer_a, layer_b, &json) != 0) {
        fprintf(stderr, "bonfyre-layer compat: unknown layer id\n");
        return 1;
    }
    printf("%s\n", json);
    free(json);
    return 0;
}

static int cmd_bridge_suggest(const char *root, const char *layer_a, const char *layer_b) {
    char *json = NULL;
    char bridges[1024];
    if (bf_layer_compat_json(root, layer_a, layer_b, &json) != 0) {
        fprintf(stderr, "bonfyre-layer bridge: unknown layer id\n");
        return 1;
    }
    extract_required_bridges_json_array(json, bridges, sizeof(bridges));
    printf("layer_a   %s\n", layer_a);
    printf("layer_b   %s\n", layer_b);
    printf("suggested %s\n", bridges[0] ? bridges : "[]");
    free(json);
    return 0;
}

static int cmd_compose(const char *root, const char *layer_a, const char *layer_b, int dry_run) {
    char *json = NULL;
    if (bf_layer_compose_json(root, layer_a, layer_b, dry_run, &json) != 0) {
        fprintf(stderr, "bonfyre-layer compose: unknown layer id\n");
        return 1;
    }
    printf("%s\n", json);
    free(json);
    return 0;
}

static int cmd_graph(const char *root, const char *families_csv) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[MAX_PATH];
    char families_copy[1024];
    char *save = NULL;
    if (!open_prepared_layer_db(root, &db, db_path, sizeof(db_path))) return 1;
    if (sqlite3_prepare_v2(db,
        "SELECT id,artifact_kind,families_text,workflow_steps_text,capabilities_text,source_model "
        "FROM layers ORDER BY artifact_kind, id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    printf("bonfyre-layer graph\n");
    printf("families %s\n\n", families_csv);
    printf("%-40s %-18s %-30s %-26s %s\n", "ID", "KIND", "FAMILIES", "WORKFLOW", "MODEL");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *kind = (const char *)sqlite3_column_text(st, 1);
        const char *families = (const char *)sqlite3_column_text(st, 2);
        int matched = 0;
        snprintf(families_copy, sizeof(families_copy), "%s", families_csv);
        for (char *fam = strtok_r(families_copy, ",", &save); fam; fam = strtok_r(NULL, ",", &save)) {
            trim_ws(fam);
            if (csv_has_token(families, fam)) { matched = 1; break; }
        }
        save = NULL;
        if (!matched) continue;
        printf("%-40.40s %-18.18s %-30.30s %-26.26s %s\n",
               id ? id : "",
               kind ? kind : "",
               families ? families : "",
               sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "",
               sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "");
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

static const char *SCHEMA_JSON =
    "{\n"
    "  \"$schema\": \"bonfyre-layer-target-v1\",\n"
    "  \"description\": \"Recipe target schema for layer-aware Bonfyre transforms.\",\n"
    "  \"variants\": {\n"
    "    \"full_model\":   { \"example\": { \"model\": \"hf://org/repo/model.onnx\" } },\n"
    "    \"layer_range\":  { \"example\": { \"model\": \"hf://org/repo\","
                            " \"target\": { \"type\": \"layer_range\", \"range\": \"0:3\" } } },\n"
    "    \"head\":         { \"example\": { \"model\": \"hf://org/repo\","
                            " \"target\": { \"type\": \"head\", \"name\": \"classifier\" } } },\n"
    "    \"slice\":        { \"example\": { \"model\": \"hf://org/repo\","
                            " \"target\": { \"type\": \"slice\", \"spec\": \"encoder.0-3,proj.out\" } } },\n"
    "    \"fragment\":     { \"example\": { \"model\": \"bonfyre://transform/name\","
                            " \"target\": { \"type\": \"fragment\" } } }\n"
    "  }\n"
    "}\n";

/* ═══════════════════════════════════════════════════════════════════
 * CLI dispatch
 * ═══════════════════════════════════════════════════════════════════ */

static void usage(void) {
    fprintf(stderr,
        "bonfyre-layer v" VERSION " — Shared LayerArtifact registry and ONNX inspection\n\n"
        "Usage:\n"
        "  bonfyre-layer inspect      <model.onnx>\n"
        "  bonfyre-layer layers       <model.onnx>\n"
        "  bonfyre-layer pull-layer   <model.onnx> --range START:END --out DIR\n"
        "  bonfyre-layer pull-head    <model.onnx> --name PREFIX --out DIR\n"
        "  bonfyre-layer pack-transform <name> <p1.onnx> [p2.onnx ...] --out DIR\n"
        "  bonfyre-layer registry     [--root DIR]\n"
        "  bonfyre-layer import-recipe <recipe.yaml> [--root DIR]\n"
        "  bonfyre-layer hash         <layer-manifest|layer-payload|layer-tree> <artifact_id> [--root DIR]\n"
        "  bonfyre-layer compat       <layer_a> <layer_b> [--root DIR]\n"
        "  bonfyre-layer compose      <layer_a> <layer_b> --dry-run [--root DIR]\n"
        "  bonfyre-layer bridge       <layer_a> <layer_b> --suggest [--root DIR]\n"
        "  bonfyre-layer ontology     [--family T_FAMILY]\n"
        "  bonfyre-layer graph        --families T_Q_PROJ,T_K_PROJ [--root DIR]\n"
        "  bonfyre-layer schema\n"
    );
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }
    const char *cmd = argv[1];
    const char *root = bf_arg_value(argc, argv, "--root");

    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) { usage(); return 0; }
    if (strcmp(cmd, "--version") == 0) {
        printf("bonfyre-layer v" VERSION "\n"); return 0;
    }

    if (strcmp(cmd, "schema") == 0) { puts(SCHEMA_JSON); return 0; }
    if (strcmp(cmd, "registry") == 0) return cmd_registry(root);
    if (strcmp(cmd, "import-recipe") == 0) {
        char *child[8];
        int n = 0;
        if (argc < 3) { fprintf(stderr, "usage: bonfyre-layer import-recipe <recipe.yaml> [--root DIR]\n"); return 1; }
        child[n++] = (char *)layeros_binary();
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--root") == 0 && n < 6) {
                child[n++] = argv[i];
                child[n++] = argv[i + 1];
                break;
            }
        }
        child[n++] = "import-recipe";
        for (int i = 2; i < argc && n < 7; i++) {
            if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) { i++; continue; }
            child[n++] = argv[i];
        }
        child[n] = NULL;
        return delegate_layeros(child);
    }
    if (strcmp(cmd, "hash") == 0) {
        char *child[8];
        int n = 0;
        if (argc < 4) { fprintf(stderr, "usage: bonfyre-layer hash <layer-manifest|layer-payload|layer-tree> <artifact_id> [--root DIR]\n"); return 1; }
        child[n++] = (char *)layeros_binary();
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--root") == 0 && n < 6) {
                child[n++] = argv[i];
                child[n++] = argv[i + 1];
                break;
            }
        }
        child[n++] = "hash";
        for (int i = 2; i < argc && n < 7; i++) {
            if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) { i++; continue; }
            child[n++] = argv[i];
        }
        child[n] = NULL;
        return delegate_layeros(child);
    }
    if (strcmp(cmd, "compat") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: bonfyre-layer compat <layer_a> <layer_b> [--root DIR]\n"); return 1; }
        return cmd_compat(root, argv[2], argv[3]);
    }
    if (strcmp(cmd, "compose") == 0) {
        if (argc < 4) { fprintf(stderr, "usage: bonfyre-layer compose <layer_a> <layer_b> --dry-run [--root DIR]\n"); return 1; }
        return cmd_compose(root, argv[2], argv[3], bf_arg_has(argc, argv, "--dry-run"));
    }
    if (strcmp(cmd, "bridge") == 0) {
        if (argc < 4 || !bf_arg_has(argc, argv, "--suggest")) {
            fprintf(stderr, "usage: bonfyre-layer bridge <layer_a> <layer_b> --suggest [--root DIR]\n");
            return 1;
        }
        return cmd_bridge_suggest(root, argv[2], argv[3]);
    }
    if (strcmp(cmd, "ontology") == 0) {
        const char *family = bf_arg_value(argc, argv, "--family");
        char *json = NULL;
        if (bf_layer_family_relations_json(family, &json) != 0 || !json) {
            fprintf(stderr, "bonfyre-layer ontology: failed to export family relations\n");
            free(json);
            return 1;
        }
        puts(json);
        free(json);
        return 0;
    }
    if (strcmp(cmd, "graph") == 0) {
        const char *families = bf_arg_value(argc, argv, "--families");
        if (!families) { fprintf(stderr, "usage: bonfyre-layer graph --families T_Q_PROJ,T_K_PROJ [--root DIR]\n"); return 1; }
        return cmd_graph(root, families);
    }

    if (strcmp(cmd, "inspect") == 0) {
        if (argc < 3) { fprintf(stderr,"usage: bonfyre-layer inspect <model.onnx>\n"); return 1; }
        return cmd_inspect(argv[2]);
    }

    if (strcmp(cmd, "layers") == 0) {
        if (argc < 3) { fprintf(stderr,"usage: bonfyre-layer layers <model.onnx>\n"); return 1; }
        return cmd_layers(argv[2]);
    }

    if (strcmp(cmd, "pull-layer") == 0) {
        if (argc < 3) { fprintf(stderr,"usage: bonfyre-layer pull-layer <model.onnx> --range S:E --out DIR\n"); return 1; }
        const char *model = argv[2];
        const char *range = bf_arg_value(argc, argv, "--range");
        const char *out   = bf_arg_value(argc, argv, "--out");
        if (!range || !out) {
            fprintf(stderr,"bonfyre-layer pull-layer: --range and --out required\n"); return 1;
        }
        return cmd_pull_layer(model, range, out);
    }

    if (strcmp(cmd, "pull-head") == 0) {
        if (argc < 3) { fprintf(stderr,"usage: bonfyre-layer pull-head <model.onnx> --name PREFIX --out DIR\n"); return 1; }
        const char *model = argv[2];
        const char *name  = bf_arg_value(argc, argv, "--name");
        const char *out   = bf_arg_value(argc, argv, "--out");
        if (!name || !out) {
            fprintf(stderr,"bonfyre-layer pull-head: --name and --out required\n"); return 1;
        }
        return cmd_pull_head(model, name, out);
    }

    if (strcmp(cmd, "pack-transform") == 0) {
        if (argc < 4) {
            fprintf(stderr,"usage: bonfyre-layer pack-transform <name> <p1.onnx> ... --out DIR\n");
            return 1;
        }
        const char *name  = argv[2];
        const char *out   = bf_arg_value(argc, argv, "--out");
        if (!out) { fprintf(stderr,"bonfyre-layer pack-transform: --out required\n"); return 1; }
        /* Collect parts: argv[3..] except --out and its value */
        char *parts[64]; int n_parts = 0;
        for (int i = 3; i < argc && n_parts < 64; i++) {
            if (strcmp(argv[i], "--out") == 0) { i++; continue; }
            if (argv[i][0] == '-') continue;
            parts[n_parts++] = argv[i];
        }
        return cmd_pack_transform(name, parts, n_parts, out);
    }

    fprintf(stderr, "bonfyre-layer: unknown command '%s'\n", cmd);
    usage(); return 1;
}
