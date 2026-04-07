/*
 * BonfyreCMS — stripped binary CMS.
 *
 * Replaces Strapi with a single C11 binary (~300KB).
 * Dynamic content types from JSON schemas, unified relation table,
 * namespace-scoped pseudo-instances, embedded HTTP+WebSocket,
 * lifecycle hooks via fork/exec, token auth via BonfyreGate model.
 *
 * Storage: single SQLite file (bonfyre_cms.db).
 * HTTP: minimal embedded server (POSIX sockets).
 * Dependencies: SQLite3, libpthread.
 *
 * Usage:
 *   bonfyre-cms serve [--port 8800] [--db FILE] [--schemas DIR]
 *   bonfyre-cms schema list [--db FILE]
 *   bonfyre-cms schema apply <schema.json> [--db FILE]
 *   bonfyre-cms schema migrate [--db FILE] [--schemas DIR]
 *   bonfyre-cms entry create <type> [--ns NAMESPACE] '<json>'
 *   bonfyre-cms entry list <type> [--ns NAMESPACE] [--status STATUS] [--limit N]
 *   bonfyre-cms entry get <type> <id> [--populate RELS]
 *   bonfyre-cms entry update <type> <id> '<json>'
 *   bonfyre-cms entry delete <type> <id>
 *   bonfyre-cms token issue [--scope NAMESPACE] [--actions ACTIONS]
 *   bonfyre-cms token list [--db FILE]
 */
#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>
#include <sqlite3.h>
#include "ephemeral.h"
#include "canonical.h"
#include "lt_egraph.h"
#include "tensor_ops.h"
#include "incremental.h"
#include "synthesis.h"
#include "ann_index.h"
#include "delta_sync.h"
#include "type_check.h"
#include "meta_gen.h"
#include "columnar.h"
#include "hybrid_reduce.h"
#include "provenance.h"
#include "transfer.h"
#include "compact_bindings.h"
#include "bench_metrics.h"
#include <bonfyre.h>

#define VERSION "0.1.0"
#define MAX_BODY   (1024 * 1024)   /* 1 MB max request body */
#define MAX_FIELDS 64
#define MAX_FIELD_NAME 128
#define MAX_FIELD_VAL  8192
#define MAX_TIERS  32
#define MAX_PATH_SEGS 16
#define DEFAULT_PORT 8800
#define DEFAULT_DB "bonfyre_cms.db"
#define DEFAULT_SCHEMAS "./content-types"

/* ================================================================
 * Utilities
 * ================================================================ */

static volatile int g_running = 1;
static int g_defer_indexing = 0;   /* when 1, entry_create skips family_refresh_entry */

static void handle_signal(int sig) { (void)sig; g_running = 0; }

void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static char *read_file_full(const char *path, long *out_size) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    buf[rd] = '\0';
    fclose(f);
    if (out_size) *out_size = (long)rd;
    return buf;
}

static int arg_has(int argc, char **argv, const char *flag) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], flag) == 0) return 1;
    }
    return 0;
}

static char *dup_cstr(const char *s) {
    size_t len;
    char *out;
    if (!s) return NULL;
    len = strlen(s);
    out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, s, len + 1);
    return out;
}

static char *resolve_schemas_dir(const char *argv0, const char *requested) {
    struct stat st;
    char cwd_resolved[PATH_MAX];
    char resolved[PATH_MAX];
    char exec_dir[PATH_MAX];
    char candidate[PATH_MAX];
    ssize_t n;
    char *slash;

    const char *rel = requested;

    if (!requested) return dup_cstr(DEFAULT_SCHEMAS);
    if (strncmp(rel, "./", 2) == 0) rel += 2;
    if (requested[0] == '/' || stat(requested, &st) == 0) return dup_cstr(requested);

    if (getcwd(cwd_resolved, sizeof(cwd_resolved))) {
        snprintf(candidate, sizeof(candidate), "%s/%s", cwd_resolved, rel);
        if (stat(candidate, &st) == 0) return dup_cstr(candidate);
    }

    n = readlink("/proc/self/exe", resolved, sizeof(resolved) - 1);
    if (n > 0) {
        resolved[n] = '\0';
        slash = strrchr(resolved, '/');
        if (slash) {
            *slash = '\0';
            snprintf(candidate, sizeof(candidate), "%s/%s", resolved, rel);
            if (stat(candidate, &st) == 0) return dup_cstr(candidate);
            snprintf(candidate, sizeof(candidate), "%s/../%s", resolved, rel);
            if (realpath(candidate, resolved) && stat(resolved, &st) == 0) return dup_cstr(resolved);
        }
    }

    if (realpath(argv0, exec_dir)) {
        slash = strrchr(exec_dir, '/');
        if (slash) {
            *slash = '\0';
            snprintf(candidate, sizeof(candidate), "%s/%s", exec_dir, rel);
            if (realpath(candidate, resolved) && stat(resolved, &st) == 0) return dup_cstr(resolved);
            snprintf(candidate, sizeof(candidate), "%s/../%s", exec_dir, rel);
            if (realpath(candidate, resolved) && stat(resolved, &st) == 0) return dup_cstr(resolved);
            snprintf(candidate, sizeof(candidate), "%s/../../10-Code/BonfyreCMS/%s", exec_dir, rel);
            if (realpath(candidate, resolved) && stat(resolved, &st) == 0) return dup_cstr(resolved);
        }
    }

    return dup_cstr(requested);
}

/* Simple FNV-1a 64-bit hash */
unsigned long long fnv1a64(const void *data, size_t len) {
    unsigned long long h = 0xcbf29ce484222325ULL;
    const unsigned char *p = data;
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned long long)p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

static double elapsed_ms_between(const struct timespec *start,
                                 const struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000.0 +
           (end->tv_nsec - start->tv_nsec) / 1e6;
}

static sqlite3_int64 stmt_value_bytes(sqlite3_stmt *stmt, int col) {
    switch (sqlite3_column_type(stmt, col)) {
        case SQLITE_INTEGER:
            return (sqlite3_int64)sizeof(sqlite3_int64);
        case SQLITE_FLOAT:
            return (sqlite3_int64)sizeof(double);
        case SQLITE_TEXT:
        case SQLITE_BLOB:
            return (sqlite3_int64)sqlite3_column_bytes(stmt, col);
        default:
            return 0;
    }
}

/* ================================================================
 * Minimal JSON helpers (inline, no deps)
 * ================================================================ */

/* Extract a string value for a given key from flat JSON.
   Returns 1 on success, 0 if not found. */
static int json_str(const char *json, const char *key, char *out, size_t sz) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (*p != '"') return 0;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i < sz - 1) {
        if (*p == '\\' && *(p+1)) { p++; }
        out[i++] = *p++;
    }
    out[i] = '\0';
    return 1;
}

/* Extract an integer value */
static int json_int(const char *json, const char *key, int *out) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (!isdigit((unsigned char)*p) && *p != '-') return 0;
    *out = atoi(p);
    return 1;
}

/* Extract a double value */
static int json_double(const char *json, const char *key, double *out) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    char *end = NULL;
    *out = strtod(p, &end);
    return end != p;
}

/* Extract a boolean value */
static int json_bool(const char *json, const char *key, int *out) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (strncmp(p, "true", 4) == 0) { *out = 1; return 1; }
    if (strncmp(p, "false", 5) == 0) { *out = 0; return 1; }
    return 0;
}

static const char *skip_json_string_end(const char *p) {
    if (!p || *p != '"') return NULL;
    p++;
    while (*p) {
        if (*p == '\\' && *(p + 1)) {
            p += 2;
            continue;
        }
        if (*p == '"') return p + 1;
        p++;
    }
    return NULL;
}

static const char *skip_json_compound_end(const char *p, char open, char close) {
    int depth = 1;
    if (!p || *p != open) return NULL;
    p++;
    while (*p && depth > 0) {
        if (*p == '"') {
            p = skip_json_string_end(p);
            if (!p) return NULL;
            continue;
        }
        if (*p == open) depth++;
        else if (*p == close) depth--;
        p++;
    }
    return depth == 0 ? p : NULL;
}

/* Find the start/end span of any JSON value for a key.
   Returns pointer to the first byte of the value and sets *end to one-past-end. */
static const char *json_value(const char *json, const char *key, const char **end) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (!*p) return NULL;

    if (*p == '"') {
        const char *q = skip_json_string_end(p);
        if (!q) return NULL;
        if (end) *end = q;
        return p;
    }
    if (*p == '{') {
        const char *q = skip_json_compound_end(p, '{', '}');
        if (!q) return NULL;
        if (end) *end = q;
        return p;
    }
    if (*p == '[') {
        const char *q = skip_json_compound_end(p, '[', ']');
        if (!q) return NULL;
        if (end) *end = q;
        return p;
    }
    if (strncmp(p, "true", 4) == 0) {
        if (end) *end = p + 4;
        return p;
    }
    if (strncmp(p, "false", 5) == 0) {
        if (end) *end = p + 5;
        return p;
    }
    if (strncmp(p, "null", 4) == 0) {
        if (end) *end = p + 4;
        return p;
    }

    char *q = NULL;
    strtod(p, &q);
    if (q != p) {
        if (end) *end = q;
        return p;
    }
    return NULL;
}

/* Find the start/end of a JSON object value for a key (for nested objects/arrays).
   Returns pointer to opening brace/bracket and sets *end to closing. */
static const char *json_object(const char *json, const char *key, const char **end) {
    const char *start = json_value(json, key, end);
    if (!start || (*start != '{' && *start != '[')) return NULL;
    return start;
}

static char *dup_json_value(const char *json, const char *key) {
    const char *end = NULL;
    const char *start = json_value(json, key, &end);
    size_t len;
    char *copy;
    if (!start || !end || end < start) return NULL;
    len = (size_t)(end - start);
    copy = malloc(len + 1);
    if (!copy) return NULL;
    memcpy(copy, start, len);
    copy[len] = '\0';
    return copy;
}

/* ================================================================
 * Schema representation (in-memory)
 * ================================================================ */

typedef enum {
    FT_STRING = 0,
    FT_TEXT,        /* richtext / long text */
    FT_INTEGER,
    FT_DECIMAL,
    FT_BOOLEAN,
    FT_DATETIME,
    FT_ENUMERATION,
    FT_UID,
    FT_JSON,
    FT_RELATION,
    FT_COMPONENT,
} FieldType;

typedef struct {
    char name[MAX_FIELD_NAME];
    FieldType type;
    int required;
    int repeatable;       /* for components */
    char relation_kind[32]; /* oneToOne, oneToMany, manyToOne, manyToMany */
    char target[128];       /* relation target content type or component name */
    char enum_values[512];  /* comma-separated for enumerations */
    char default_val[256];
    char target_field[128]; /* for uid: which field to derive from */
} FieldDef;

typedef struct {
    char name[128];
    char display_name[128];
    char description[256];
    FieldDef fields[MAX_FIELDS];
    int field_count;
} ContentType;

static const char *field_type_name(FieldType ft) {
    switch (ft) {
        case FT_STRING: return "string";
        case FT_TEXT: return "text";
        case FT_INTEGER: return "integer";
        case FT_DECIMAL: return "decimal";
        case FT_BOOLEAN: return "boolean";
        case FT_DATETIME: return "datetime";
        case FT_ENUMERATION: return "enumeration";
        case FT_UID: return "uid";
        case FT_JSON: return "json";
        case FT_RELATION: return "relation";
        case FT_COMPONENT: return "component";
    }
    return "string";
}

static FieldType parse_field_type(const char *s) {
    if (strcmp(s, "string") == 0) return FT_STRING;
    if (strcmp(s, "text") == 0 || strcmp(s, "richtext") == 0) return FT_TEXT;
    if (strcmp(s, "integer") == 0) return FT_INTEGER;
    if (strcmp(s, "decimal") == 0) return FT_DECIMAL;
    if (strcmp(s, "boolean") == 0) return FT_BOOLEAN;
    if (strcmp(s, "datetime") == 0) return FT_DATETIME;
    if (strcmp(s, "enumeration") == 0) return FT_ENUMERATION;
    if (strcmp(s, "uid") == 0) return FT_UID;
    if (strcmp(s, "json") == 0) return FT_JSON;
    if (strcmp(s, "relation") == 0) return FT_RELATION;
    if (strcmp(s, "component") == 0) return FT_COMPONENT;
    return FT_STRING;
}

static const char *sql_type_for(FieldType ft) {
    switch (ft) {
        case FT_STRING:      return "TEXT";
        case FT_TEXT:         return "TEXT";
        case FT_INTEGER:     return "INTEGER";
        case FT_DECIMAL:     return "REAL";
        case FT_BOOLEAN:     return "INTEGER";
        case FT_DATETIME:    return "TEXT";
        case FT_ENUMERATION: return "TEXT";
        case FT_UID:         return "TEXT UNIQUE";
        case FT_JSON:        return "TEXT";
        case FT_RELATION:    return NULL; /* handled via _relations */
        case FT_COMPONENT:   return NULL; /* handled via _relations */
    }
    return "TEXT";
}

/* Parse attributes from the JSON schema.
   Schema format:
   {
     "name": "service_offer",
     "displayName": "Service Offer",
     "attributes": {
       "title": {"type": "string", "required": true},
       ...
     }
   }
*/
static int parse_schema(const char *json, ContentType *ct) {
    memset(ct, 0, sizeof(*ct));
    json_str(json, "name", ct->name, sizeof(ct->name));
    json_str(json, "displayName", ct->display_name, sizeof(ct->display_name));
    json_str(json, "description", ct->description, sizeof(ct->description));

    if (ct->name[0] == '\0') return -1;

    /* Find "attributes" object */
    const char *attr_end = NULL;
    const char *attrs = json_object(json, "attributes", &attr_end);
    if (!attrs) return -1;

    /* Walk attributes — find each key-value pair */
    const char *p = attrs + 1; /* skip '{' */
    while (p < attr_end && ct->field_count < MAX_FIELDS) {
        /* skip whitespace */
        while (p < attr_end && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t' || *p == ',')) p++;
        if (p >= attr_end || *p == '}') break;

        /* expect field name in quotes */
        if (*p != '"') break;
        p++;
        FieldDef *fd = &ct->fields[ct->field_count];
        memset(fd, 0, sizeof(*fd));
        size_t ni = 0;
        while (*p && *p != '"' && ni < sizeof(fd->name) - 1) fd->name[ni++] = *p++;
        fd->name[ni] = '\0';
        if (*p == '"') p++;

        /* skip to value object */
        while (p < attr_end && *p != '{') p++;
        if (p >= attr_end) break;

        /* find end of this field's object */
        const char *fobj_start = p;
        int depth = 1; p++;
        while (p < attr_end && depth > 0) {
            if (*p == '"') { p++; while (*p && *p != '"') { if (*p == '\\') p++; p++; } }
            else if (*p == '{') depth++;
            else if (*p == '}') depth--;
            if (*p) p++;
        }

        /* extract field attributes from the sub-object */
        size_t fobj_len = (size_t)(p - fobj_start);
        char *fobj = malloc(fobj_len + 1);
        memcpy(fobj, fobj_start, fobj_len);
        fobj[fobj_len] = '\0';

        char type_str[64] = {0};
        json_str(fobj, "type", type_str, sizeof(type_str));
        fd->type = parse_field_type(type_str);

        json_bool(fobj, "required", &fd->required);
        json_bool(fobj, "repeatable", &fd->repeatable);
        json_str(fobj, "relation", fd->relation_kind, sizeof(fd->relation_kind));
        json_str(fobj, "target", fd->target, sizeof(fd->target));
        json_str(fobj, "component", fd->target, sizeof(fd->target));
        json_str(fobj, "default", fd->default_val, sizeof(fd->default_val));
        json_str(fobj, "targetField", fd->target_field, sizeof(fd->target_field));

        /* enum values */
        const char *ev_end = NULL;
        const char *ev = json_object(fobj, "enum", &ev_end);
        if (ev) {
            size_t evi = 0;
            const char *ep = ev + 1;
            while (ep < ev_end) {
                while (ep < ev_end && *ep != '"') ep++;
                if (ep >= ev_end) break;
                ep++;
                if (evi > 0 && evi < sizeof(fd->enum_values) - 1) fd->enum_values[evi++] = ',';
                while (*ep && *ep != '"' && evi < sizeof(fd->enum_values) - 1) fd->enum_values[evi++] = *ep++;
                if (*ep == '"') ep++;
            }
            fd->enum_values[evi] = '\0';
        }

        free(fobj);
        ct->field_count++;
    }

    return 0;
}

/* ================================================================
 * SQLite bootstrap + dynamic table management
 * ================================================================ */

static sqlite3 *g_db = NULL;
static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;

int db_exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[db] %s: %s\n", sql, err ? err : "unknown");
        if (err) sqlite3_free(err);
        return -1;
    }
    return 0;
}

static int bootstrap_meta_schema(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _content_types ("
        "  name TEXT PRIMARY KEY,"
        "  schema_json TEXT NOT NULL,"
        "  created_at TEXT NOT NULL,"
        "  updated_at TEXT NOT NULL"
        ");",

        "CREATE TABLE IF NOT EXISTS _relations ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  source_type TEXT NOT NULL,"
        "  source_id INTEGER NOT NULL,"
        "  target_type TEXT NOT NULL,"
        "  target_id INTEGER NOT NULL,"
        "  relation TEXT NOT NULL,"
        "  kind TEXT NOT NULL,"
        "  position INTEGER DEFAULT 0,"
        "  UNIQUE(source_type, source_id, target_type, target_id, relation)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_rel_source ON _relations(source_type, source_id);",
        "CREATE INDEX IF NOT EXISTS idx_rel_target ON _relations(target_type, target_id);",

        "CREATE TABLE IF NOT EXISTS _hooks ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type TEXT NOT NULL,"
        "  event TEXT NOT NULL,"
        "  command TEXT NOT NULL,"
        "  enabled INTEGER DEFAULT 1"
        ");",

        "CREATE TABLE IF NOT EXISTS _permissions ("
        "  token_hash TEXT NOT NULL,"
        "  content_type TEXT NOT NULL,"
        "  action TEXT NOT NULL,"
        "  namespace_scope TEXT DEFAULT 'root',"
        "  PRIMARY KEY(token_hash, content_type, action)"
        ");",

        "CREATE TABLE IF NOT EXISTS _tokens ("
        "  token_hash TEXT PRIMARY KEY,"
        "  label TEXT,"
        "  scope TEXT DEFAULT 'root',"
        "  created_at TEXT NOT NULL,"
        "  expires_at TEXT,"
        "  active INTEGER DEFAULT 1"
        ");",

        NULL
    };

    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* Create or migrate (add columns only) a content type table */
static int create_content_table(sqlite3 *db, const ContentType *ct) {
    char sql[8192];
    int off = snprintf(sql, sizeof(sql),
        "CREATE TABLE IF NOT EXISTS \"%s\" ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  namespace TEXT DEFAULT 'root' NOT NULL,"
        "  created_at TEXT NOT NULL,"
        "  updated_at TEXT NOT NULL,"
        "  published_at TEXT,"
        "  status TEXT DEFAULT 'draft'",
        ct->name);

    /* Built-in column names to skip in user-defined attributes */
    static const char *builtin_cols[] = {
        "id", "namespace", "created_at", "updated_at", "published_at", "status", NULL
    };

    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        const char *sqlt = sql_type_for(fd->type);
        if (!sqlt) continue; /* relations/components: handled via _relations */
        /* Skip if name collides with built-in column */
        int skip = 0;
        for (int b = 0; builtin_cols[b]; b++) {
            if (strcmp(fd->name, builtin_cols[b]) == 0) { skip = 1; break; }
        }
        if (skip) continue;
        off += snprintf(sql + off, sizeof(sql) - (size_t)off,
            ",\n  \"%s\" %s", fd->name, sqlt);
        if (fd->default_val[0]) {
            off += snprintf(sql + off, sizeof(sql) - (size_t)off,
                " DEFAULT '%s'", fd->default_val);
        }
    }

    off += snprintf(sql + off, sizeof(sql) - (size_t)off, "\n);");
    if (db_exec(db, sql) != 0) return -1;

    /* Create namespace index */
    snprintf(sql, sizeof(sql),
        "CREATE INDEX IF NOT EXISTS idx_%s_ns ON \"%s\"(namespace);", ct->name, ct->name);
    db_exec(db, sql);

    /* Create slug index if there's a uid field */
    for (int i = 0; i < ct->field_count; i++) {
        if (ct->fields[i].type == FT_UID) {
            snprintf(sql, sizeof(sql),
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_%s ON \"%s\"(\"%s\");",
                ct->name, ct->fields[i].name, ct->name, ct->fields[i].name);
            db_exec(db, sql);
        }
    }

    /* Migration: add columns that don't exist yet */
    sqlite3_stmt *pragma;
    snprintf(sql, sizeof(sql), "PRAGMA table_info(\"%s\")", ct->name);
    if (sqlite3_prepare_v2(db, sql, -1, &pragma, NULL) == SQLITE_OK) {
        /* Collect existing column names */
        char existing[MAX_FIELDS][MAX_FIELD_NAME];
        int existing_count = 0;
        while (sqlite3_step(pragma) == SQLITE_ROW && existing_count < MAX_FIELDS) {
            const char *col = (const char *)sqlite3_column_text(pragma, 1);
            if (col) {
                strncpy(existing[existing_count], col, MAX_FIELD_NAME - 1);
                existing[existing_count][MAX_FIELD_NAME - 1] = '\0';
                existing_count++;
            }
        }
        sqlite3_finalize(pragma);

        /* Add missing columns */
        for (int i = 0; i < ct->field_count; i++) {
            const FieldDef *fd = &ct->fields[i];
            const char *sqlt = sql_type_for(fd->type);
            if (!sqlt) continue;
            int found = 0;
            for (int j = 0; j < existing_count; j++) {
                if (strcmp(existing[j], fd->name) == 0) { found = 1; break; }
            }
            if (!found) {
                snprintf(sql, sizeof(sql),
                    "ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s",
                    ct->name, fd->name, sqlt);
                if (fd->default_val[0]) {
                    size_t l = strlen(sql);
                    snprintf(sql + l, sizeof(sql) - l, " DEFAULT '%s'", fd->default_val);
                }
                db_exec(db, sql);
                fprintf(stderr, "[migrate] added column %s.%s (%s)\n",
                    ct->name, fd->name, field_type_name(fd->type));
            }
        }
    }

    return 0;
}

/* Register the schema in _content_types */
static int register_schema(sqlite3 *db, const ContentType *ct, const char *raw_json) {
    char ts[64]; iso_timestamp(ts, sizeof(ts));
    sqlite3_stmt *stmt;
    const char *sql =
        "INSERT INTO _content_types (name, schema_json, created_at, updated_at) "
        "VALUES (?1, ?2, ?3, ?3) "
        "ON CONFLICT(name) DO UPDATE SET schema_json=?2, updated_at=?3";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, ct->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, raw_json, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, ts, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* ================================================================
 * Schema loading from directory
 * ================================================================ */

static ContentType g_types[128];
static int g_type_count = 0;

static ContentType *find_type(const char *name) {
    for (int i = 0; i < g_type_count; i++) {
        if (strcmp(g_types[i].name, name) == 0) return &g_types[i];
    }
    return NULL;
}

static int load_schema_file(sqlite3 *db, const char *path) {
    long sz = 0;
    char *json = read_file_full(path, &sz);
    if (!json) { fprintf(stderr, "[schema] cannot read: %s\n", path); return -1; }

    ContentType ct;
    if (parse_schema(json, &ct) != 0) {
        fprintf(stderr, "[schema] parse error: %s\n", path);
        free(json);
        return -1;
    }

    if (create_content_table(db, &ct) != 0) {
        free(json);
        return -1;
    }

    if (register_schema(db, &ct, json) != 0) {
        free(json);
        return -1;
    }

    /* Store in memory */
    if (g_type_count < 128) {
        g_types[g_type_count++] = ct;
    }

    fprintf(stderr, "[schema] loaded: %s (%d fields)\n", ct.name, ct.field_count);
    free(json);
    return 0;
}

static int load_schemas_from_dir(sqlite3 *db, const char *dir) {
    DIR *d = opendir(dir);
    if (!d) { fprintf(stderr, "[schema] cannot open dir: %s\n", dir); return -1; }
    struct dirent *ent;
    int count = 0;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        size_t nlen = strlen(ent->d_name);
        if (nlen < 5 || strcmp(ent->d_name + nlen - 5, ".json") != 0) continue;
        /* skip components subdirectory files loaded separately */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        struct stat st;
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode)) {
            if (load_schema_file(db, path) == 0) count++;
        }
    }
    closedir(d);

    /* Also load components/ subdirectory */
    char comp_dir[PATH_MAX];
    snprintf(comp_dir, sizeof(comp_dir), "%s/components", dir);
    DIR *cd = opendir(comp_dir);
    if (cd) {
        while ((ent = readdir(cd)) != NULL) {
            if (ent->d_name[0] == '.') continue;
            size_t nlen = strlen(ent->d_name);
            if (nlen < 5 || strcmp(ent->d_name + nlen - 5, ".json") != 0) continue;
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s", comp_dir, ent->d_name);
            if (load_schema_file(db, path) == 0) count++;
        }
        closedir(cd);
    }

    fprintf(stderr, "[schema] loaded %d content types from %s\n", count, dir);
    return count;
}

/* ================================================================
 * CRUD operations
 * ================================================================ */

/* Build INSERT statement from field values in a JSON body */
static int entry_create(sqlite3 *db, const char *type_name, const char *ns,
                        const char *body_json, long long *out_id)
{
    ContentType *ct = find_type(type_name);
    if (!ct) { fprintf(stderr, "[crud] unknown type: %s\n", type_name); return -1; }

    char sql[8192];
    char cols[4096] = "namespace, created_at, updated_at, status";
    char vals[4096] = "?1, ?2, ?2, ?3";
    int bind_idx = 4;
    int field_binds[MAX_FIELDS];
    int field_bind_count = 0;

    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;

        /* Check if field is present in body JSON */
        char test[MAX_FIELD_VAL];
        int has_str = json_str(body_json, fd->name, test, sizeof(test));
        int has_int = 0;
        int ival = 0;
        if (!has_str) has_int = json_int(body_json, fd->name, &ival);
        double dval = 0;
        int has_dbl = 0;
        int has_bool = 0;
        int bval = 0;
        int has_json = 0;
        if (!has_str && !has_int) has_dbl = json_double(body_json, fd->name, &dval);
        if (!has_str && !has_int && !has_dbl) has_bool = json_bool(body_json, fd->name, &bval);
        if (!has_str && !has_int && !has_dbl && !has_bool && fd->type == FT_JSON)
            has_json = (json_value(body_json, fd->name, NULL) != NULL);

        (void)bval;
        if (has_str || has_int || has_dbl || has_bool || has_json) {
            size_t cl = strlen(cols), vl = strlen(vals);
            snprintf(cols + cl, sizeof(cols) - cl, ", \"%s\"", fd->name);
            snprintf(vals + vl, sizeof(vals) - vl, ", ?%d", bind_idx);
            field_binds[field_bind_count++] = bind_idx;
            bind_idx++;
        }
    }

    snprintf(sql, sizeof(sql), "INSERT INTO \"%s\" (%s) VALUES (%s)", type_name, cols, vals);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] prepare insert: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    char ts[64]; iso_timestamp(ts, sizeof(ts));
    sqlite3_bind_text(stmt, 1, ns, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, ts, -1, SQLITE_STATIC);

    /* Default status */
    char status[64] = "draft";
    json_str(body_json, "status", status, sizeof(status));
    sqlite3_bind_text(stmt, 3, status, -1, SQLITE_STATIC);

    /* Bind field values */
    int bi = 0;
    for (int i = 0; i < ct->field_count && bi < field_bind_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;

        char sval[MAX_FIELD_VAL];
        int ival = 0;
        double dval = 0;
        int has_str = json_str(body_json, fd->name, sval, sizeof(sval));
        int has_int = 0;
        int has_dbl = 0;
        int has_bool = 0;
        int bval = 0;
        char *json_raw = NULL;
        if (!has_str) has_int = json_int(body_json, fd->name, &ival);
        if (!has_str && !has_int) has_dbl = json_double(body_json, fd->name, &dval);
        if (!has_str && !has_int && !has_dbl) has_bool = json_bool(body_json, fd->name, &bval);
        if (!has_str && !has_int && !has_dbl && !has_bool && fd->type == FT_JSON)
            json_raw = dup_json_value(body_json, fd->name);

        if (!has_str && !has_int && !has_dbl && !has_bool && !json_raw) continue;

        int pidx = field_binds[bi++];
        switch (fd->type) {
            case FT_INTEGER:
                sqlite3_bind_int(stmt, pidx, has_int ? ival : atoi(sval));
                break;
            case FT_BOOLEAN:
                sqlite3_bind_int(stmt, pidx, has_bool ? bval : (has_int ? (ival != 0) : (atoi(sval) != 0)));
                break;
            case FT_DECIMAL:
                if (has_dbl) sqlite3_bind_double(stmt, pidx, dval);
                else if (has_int) sqlite3_bind_double(stmt, pidx, (double)ival);
                else sqlite3_bind_double(stmt, pidx, atof(sval));
                break;
            case FT_JSON:
                sqlite3_bind_text(stmt, pidx, json_raw, -1, SQLITE_TRANSIENT);
                break;
            default:
                sqlite3_bind_text(stmt, pidx, sval, -1, SQLITE_TRANSIENT);
                break;
        }
        free(json_raw);
    }

    /* Wrap the insert + post-hooks in a savepoint so the full write-path
       commits as a single fsync when no outer transaction is active. */
    sqlite3_exec(db, "SAVEPOINT entry_create_sp", NULL, NULL, NULL);

    int rc = sqlite3_step(stmt);
    long long rowid = sqlite3_last_insert_rowid(db);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "[crud] insert failed: %s\n", sqlite3_errmsg(db));
        sqlite3_exec(db, "ROLLBACK TO entry_create_sp", NULL, NULL, NULL);
        sqlite3_exec(db, "RELEASE entry_create_sp", NULL, NULL, NULL);
        return -1;
    }

    if (out_id) *out_id = rowid;

    /* Log create op to BonfyreGraph op log */
    ops_append(db, OP_CREATE, type_name, rowid, ns, body_json, "system", NULL);

    /* Incremental family maintenance keeps writes out of full-table rediscovery.
       Skipped when g_defer_indexing is set; caller must run family_discover later. */
    if (!g_defer_indexing) {
        if (family_refresh_entry(db, type_name, (int)rowid) != 0) {
            fprintf(stderr, "[crud] family refresh failed after create for %s:%lld\n", type_name, rowid);
        }
    }

    /* Fire lifecycle hooks */
    char id_str[32]; snprintf(id_str, sizeof(id_str), "%lld", rowid);
    sqlite3_stmt *hook_stmt;
    if (sqlite3_prepare_v2(db,
        "SELECT command FROM _hooks WHERE content_type=?1 AND event='after_create' AND enabled=1",
        -1, &hook_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(hook_stmt, 1, type_name, -1, SQLITE_STATIC);
        while (sqlite3_step(hook_stmt) == SQLITE_ROW) {
            const char *cmd = (const char *)sqlite3_column_text(hook_stmt, 0);
            if (cmd) {
                pid_t pid = fork();
                if (pid == 0) {
                    execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
                    _exit(127);
                } else if (pid > 0) {
                    int st; waitpid(pid, &st, 0);
                }
            }
        }
        sqlite3_finalize(hook_stmt);
    }

    sqlite3_exec(db, "RELEASE entry_create_sp", NULL, NULL, NULL);
    return 0;
}

/* List entries with optional namespace + status filter */
static int entry_list(sqlite3 *db, const char *type_name, const char *ns,
                      const char *status, int limit, FILE *out)
{
    ContentType *ct = find_type(type_name);
    int bench_on = bench_metrics_is_enabled();
    struct timespec bench_ts0, bench_ts1;
    if (!ct) { fprintf(stderr, "[crud] unknown type: %s\n", type_name); return -1; }

    if (bench_on) clock_gettime(CLOCK_MONOTONIC, &bench_ts0);

    char sql[4096];
    int off = snprintf(sql, sizeof(sql),
        "SELECT id, namespace, created_at, updated_at, published_at, status");
    for (int i = 0; i < ct->field_count; i++) {
        if (ct->fields[i].type == FT_RELATION || ct->fields[i].type == FT_COMPONENT) continue;
        off += snprintf(sql + off, sizeof(sql) - (size_t)off, ", \"%s\"", ct->fields[i].name);
    }
    off += snprintf(sql + off, sizeof(sql) - (size_t)off, " FROM \"%s\" WHERE 1=1", type_name);
    if (ns && ns[0]) off += snprintf(sql + off, sizeof(sql) - (size_t)off, " AND namespace LIKE '%s%%'", ns);
    if (status && status[0]) off += snprintf(sql + off, sizeof(sql) - (size_t)off, " AND status='%s'", status);
    off += snprintf(sql + off, sizeof(sql) - (size_t)off, " ORDER BY id DESC LIMIT %d", limit > 0 ? limit : 25);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] list: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    fprintf(out, "{\"data\":[");
    int row = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (row > 0) fprintf(out, ",");
        fprintf(out, "\n  {\"id\":%d", sqlite3_column_int(stmt, 0));
        fprintf(out, ",\"attributes\":{");
        fprintf(out, "\"namespace\":\"%s\"", sqlite3_column_text(stmt, 1) ? (const char*)sqlite3_column_text(stmt, 1) : "root");
        fprintf(out, ",\"created_at\":\"%s\"", sqlite3_column_text(stmt, 2) ? (const char*)sqlite3_column_text(stmt, 2) : "");
        fprintf(out, ",\"updated_at\":\"%s\"", sqlite3_column_text(stmt, 3) ? (const char*)sqlite3_column_text(stmt, 3) : "");
        const char *pub = (const char *)sqlite3_column_text(stmt, 4);
        if (pub) fprintf(out, ",\"published_at\":\"%s\"", pub);
        else fprintf(out, ",\"published_at\":null");
        fprintf(out, ",\"status\":\"%s\"", sqlite3_column_text(stmt, 5) ? (const char*)sqlite3_column_text(stmt, 5) : "draft");

        int col = 6;
        for (int i = 0; i < ct->field_count; i++) {
            const FieldDef *fd = &ct->fields[i];
            if (fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;

            switch (fd->type) {
                case FT_INTEGER:
                    fprintf(out, ",\"%s\":%d", fd->name, sqlite3_column_int(stmt, col));
                    break;
                case FT_BOOLEAN:
                    fprintf(out, ",\"%s\":%s", fd->name,
                            sqlite3_column_int(stmt, col) ? "true" : "false");
                    break;
                case FT_DECIMAL:
                    fprintf(out, ",\"%s\":%.4f", fd->name, sqlite3_column_double(stmt, col));
                    break;
                case FT_JSON: {
                    const char *v = (const char *)sqlite3_column_text(stmt, col);
                    if (v) fprintf(out, ",\"%s\":%s", fd->name, v);
                    else fprintf(out, ",\"%s\":null", fd->name);
                    break;
                }
                default: {
                    const char *v = (const char *)sqlite3_column_text(stmt, col);
                    if (v) {
                        fprintf(out, ",\"%s\":\"", fd->name);
                        /* Escape JSON string */
                        for (const char *c = v; *c; c++) {
                            if (*c == '"') fprintf(out, "\\\"");
                            else if (*c == '\\') fprintf(out, "\\\\");
                            else if (*c == '\n') fprintf(out, "\\n");
                            else if (*c == '\r') fprintf(out, "\\r");
                            else if (*c == '\t') fprintf(out, "\\t");
                            else fputc(*c, out);
                        }
                        fprintf(out, "\"");
                    } else {
                        fprintf(out, ",\"%s\":null", fd->name);
                    }
                    break;
                }
            }
            col++;
        }
        fprintf(out, "}}");
        row++;
    }
    fprintf(out, "\n],\"meta\":{\"total\":%d}}\n", row);
    if (bench_on) {
        sqlite3_int64 vm_steps = sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 0);
        clock_gettime(CLOCK_MONOTONIC, &bench_ts1);
        bench_metrics_record_point_list(elapsed_ms_between(&bench_ts0, &bench_ts1), vm_steps);
    }
    sqlite3_finalize(stmt);
    return 0;
}

/* Get a single entry by ID */
static int entry_get(sqlite3 *db, const char *type_name, int entry_id, FILE *out) {
    ContentType *ct = find_type(type_name);
    int bench_on = bench_metrics_is_enabled();
    struct timespec bench_ts0, bench_ts1;
    sqlite3_int64 bytes_touched = 0;
    if (!ct) { fprintf(stderr, "[crud] unknown type: %s\n", type_name); return -1; }

    if (bench_on) clock_gettime(CLOCK_MONOTONIC, &bench_ts0);

    char sql[4096];
    int off = snprintf(sql, sizeof(sql),
        "SELECT id, namespace, created_at, updated_at, published_at, status");
    for (int i = 0; i < ct->field_count; i++) {
        if (ct->fields[i].type == FT_RELATION || ct->fields[i].type == FT_COMPONENT) continue;
        off += snprintf(sql + off, sizeof(sql) - (size_t)off, ", \"%s\"", ct->fields[i].name);
    }
    off += snprintf(sql + off, sizeof(sql) - (size_t)off, " FROM \"%s\" WHERE id=?1", type_name);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] get: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    sqlite3_bind_int(stmt, 1, entry_id);

    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        fprintf(out, "{\"data\":null,\"error\":\"not found\"}\n");
        if (bench_on) {
            clock_gettime(CLOCK_MONOTONIC, &bench_ts1);
            bench_metrics_record_point_get(elapsed_ms_between(&bench_ts0, &bench_ts1), bytes_touched);
        }
        sqlite3_finalize(stmt);
        return -1;
    }

    if (bench_on) {
        int ncols = sqlite3_column_count(stmt);
        for (int c = 0; c < ncols; c++) bytes_touched += stmt_value_bytes(stmt, c);
    }

    fprintf(out, "{\"data\":{\"id\":%d,\"attributes\":{", sqlite3_column_int(stmt, 0));
    fprintf(out, "\"namespace\":\"%s\"", sqlite3_column_text(stmt, 1) ? (const char*)sqlite3_column_text(stmt, 1) : "root");
    fprintf(out, ",\"created_at\":\"%s\"", sqlite3_column_text(stmt, 2) ? (const char*)sqlite3_column_text(stmt, 2) : "");
    fprintf(out, ",\"updated_at\":\"%s\"", sqlite3_column_text(stmt, 3) ? (const char*)sqlite3_column_text(stmt, 3) : "");
    const char *pub = (const char *)sqlite3_column_text(stmt, 4);
    if (pub) fprintf(out, ",\"published_at\":\"%s\"", pub);
    else fprintf(out, ",\"published_at\":null");
    fprintf(out, ",\"status\":\"%s\"", sqlite3_column_text(stmt, 5) ? (const char*)sqlite3_column_text(stmt, 5) : "draft");

    int col = 6;
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;
        switch (fd->type) {
            case FT_INTEGER:
                fprintf(out, ",\"%s\":%d", fd->name, sqlite3_column_int(stmt, col));
                break;
            case FT_BOOLEAN:
                fprintf(out, ",\"%s\":%s", fd->name,
                        sqlite3_column_int(stmt, col) ? "true" : "false");
                break;
            case FT_DECIMAL:
                fprintf(out, ",\"%s\":%.4f", fd->name, sqlite3_column_double(stmt, col));
                break;
            case FT_JSON: {
                const char *v = (const char *)sqlite3_column_text(stmt, col);
                if (v) fprintf(out, ",\"%s\":%s", fd->name, v);
                else fprintf(out, ",\"%s\":null", fd->name);
                break;
            }
            default: {
                const char *v = (const char *)sqlite3_column_text(stmt, col);
                if (v) {
                    fprintf(out, ",\"%s\":\"", fd->name);
                    for (const char *c = v; *c; c++) {
                        if (*c == '"') fprintf(out, "\\\"");
                        else if (*c == '\\') fprintf(out, "\\\\");
                        else if (*c == '\n') fprintf(out, "\\n");
                        else fputc(*c, out);
                    }
                    fprintf(out, "\"");
                } else {
                    fprintf(out, ",\"%s\":null", fd->name);
                }
                break;
            }
        }
        col++;
    }

    /* Populate relations */
    sqlite3_stmt *rel_stmt;
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type != FT_RELATION && fd->type != FT_COMPONENT) continue;

        if (sqlite3_prepare_v2(db,
            "SELECT target_type, target_id, position FROM _relations "
            "WHERE source_type=?1 AND source_id=?2 AND relation=?3 "
            "ORDER BY position",
            -1, &rel_stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(rel_stmt, 1, type_name, -1, SQLITE_STATIC);
            sqlite3_bind_int(rel_stmt, 2, entry_id);
            sqlite3_bind_text(rel_stmt, 3, fd->name, -1, SQLITE_STATIC);

            /* Check if one-to-one or arrays */
            int is_many = (strcmp(fd->relation_kind, "oneToMany") == 0 ||
                           strcmp(fd->relation_kind, "manyToMany") == 0 ||
                           fd->repeatable);

            if (is_many) fprintf(out, ",\"%s\":[", fd->name);
            int rcount = 0;
            while (sqlite3_step(rel_stmt) == SQLITE_ROW) {
                if (bench_on) {
                    bytes_touched += stmt_value_bytes(rel_stmt, 0);
                    bytes_touched += stmt_value_bytes(rel_stmt, 1);
                    bytes_touched += stmt_value_bytes(rel_stmt, 2);
                }
                if (rcount > 0 && is_many) fprintf(out, ",");
                int tid = sqlite3_column_int(rel_stmt, 1);
                if (!is_many && rcount == 0) fprintf(out, ",\"%s\":", fd->name);
                fprintf(out, "{\"id\":%d}", tid);
                rcount++;
            }
            if (is_many) fprintf(out, "]");
            else if (rcount == 0) fprintf(out, ",\"%s\":null", fd->name);
            sqlite3_finalize(rel_stmt);
        }
    }

    fprintf(out, "}}}\n");
    if (bench_on) {
        clock_gettime(CLOCK_MONOTONIC, &bench_ts1);
        bench_metrics_record_point_get(elapsed_ms_between(&bench_ts0, &bench_ts1), bytes_touched);
    }
    sqlite3_finalize(stmt);
    return 0;
}

/* Update entry fields */
static int entry_update(sqlite3 *db, const char *type_name, int entry_id,
                        const char *body_json)
{
    ContentType *ct = find_type(type_name);
    if (!ct) return -1;

    char sql[8192];
    char ts[64]; iso_timestamp(ts, sizeof(ts));
    int off = snprintf(sql, sizeof(sql), "UPDATE \"%s\" SET updated_at='%s'", type_name, ts);

    /* Build SET clause for present fields */
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;
        char sval[MAX_FIELD_VAL];
        int ival = 0;
        double dval = 0.0;
        int bval = 0;
        char *json_raw = NULL;
        int has_str = json_str(body_json, fd->name, sval, sizeof(sval));
        int has_int = 0;
        int has_dbl = 0;
        int has_bool = 0;
        if (!has_str) has_int = json_int(body_json, fd->name, &ival);
        if (!has_str && !has_int) has_dbl = json_double(body_json, fd->name, &dval);
        if (!has_str && !has_int && !has_dbl) has_bool = json_bool(body_json, fd->name, &bval);
        if (!has_str && !has_int && !has_dbl && !has_bool && fd->type == FT_JSON)
            json_raw = dup_json_value(body_json, fd->name);
        if (has_str) {
            /* Escape single quotes for SQL */
            char escaped[MAX_FIELD_VAL * 2];
            size_t ei = 0;
            for (size_t si = 0; sval[si] && ei < sizeof(escaped) - 2; si++) {
                if (sval[si] == '\'') escaped[ei++] = '\'';
                escaped[ei++] = sval[si];
            }
            escaped[ei] = '\0';
            off += snprintf(sql + off, sizeof(sql) - (size_t)off,
                ", \"%s\"='%s'", fd->name, escaped);
        } else if (fd->type == FT_INTEGER && has_int) {
            off += snprintf(sql + off, sizeof(sql) - (size_t)off,
                ", \"%s\"=%d", fd->name, ival);
        } else if (fd->type == FT_BOOLEAN && (has_bool || has_int)) {
            off += snprintf(sql + off, sizeof(sql) - (size_t)off,
                ", \"%s\"=%d", fd->name, has_bool ? bval : (ival != 0));
        } else if (fd->type == FT_DECIMAL && (has_dbl || has_int)) {
            off += snprintf(sql + off, sizeof(sql) - (size_t)off,
                ", \"%s\"=%.6f", fd->name, has_dbl ? dval : (double)ival);
        } else if (fd->type == FT_JSON && json_raw) {
            char escaped[MAX_FIELD_VAL * 2];
            size_t ei = 0;
            for (size_t si = 0; json_raw[si] && ei < sizeof(escaped) - 2; si++) {
                if (json_raw[si] == '\'') escaped[ei++] = '\'';
                escaped[ei++] = json_raw[si];
            }
            escaped[ei] = '\0';
            off += snprintf(sql + off, sizeof(sql) - (size_t)off,
                ", \"%s\"='%s'", fd->name, escaped);
        }
        free(json_raw);
    }

    /* Handle status + published_at */
    char new_status[64];
    if (json_str(body_json, "status", new_status, sizeof(new_status))) {
        off += snprintf(sql + off, sizeof(sql) - (size_t)off, ", status='%s'", new_status);
        if (strcmp(new_status, "published") == 0) {
            off += snprintf(sql + off, sizeof(sql) - (size_t)off, ", published_at='%s'", ts);
        }
    }

    off += snprintf(sql + off, sizeof(sql) - (size_t)off, " WHERE id=%d", entry_id);

    sqlite3_exec(db, "SAVEPOINT entry_update_sp", NULL, NULL, NULL);
    int rc = db_exec(db, sql);

    /* Log update op to BonfyreGraph op log */
    if (rc == 0) {
        ops_append(db, OP_UPDATE, type_name, (long long)entry_id, "root", body_json, "system", NULL);
        if (!g_defer_indexing) {
            /* Value-only update: rebind without re-hashing family (structure doesn't change) */
            if (family_rebind_entry(db, type_name, entry_id) != 0) {
                fprintf(stderr, "[crud] family rebind failed after update for %s:%d\n", type_name, entry_id);
            }
        }
    } else {
        sqlite3_exec(db, "ROLLBACK TO entry_update_sp", NULL, NULL, NULL);
    }
    sqlite3_exec(db, "RELEASE entry_update_sp", NULL, NULL, NULL);
    return rc;
}

/* Delete entry + its relations */
static int entry_delete(sqlite3 *db, const char *type_name, int entry_id) {
    if (family_remove_entry(db, type_name, entry_id) != 0) {
        fprintf(stderr, "[crud] family removal failed before delete for %s:%d\n", type_name, entry_id);
    }

    char sql[512];
    snprintf(sql, sizeof(sql), "DELETE FROM \"%s\" WHERE id=%d", type_name, entry_id);
    if (db_exec(db, sql) != 0) return -1;

    /* Log delete op to BonfyreGraph op log */
    ops_append(db, OP_DELETE, type_name, (long long)entry_id, "root", "{}", "system", NULL);

    snprintf(sql, sizeof(sql),
        "DELETE FROM _relations WHERE (source_type='%s' AND source_id=%d) "
        "OR (target_type='%s' AND target_id=%d)",
        type_name, entry_id, type_name, entry_id);
    return db_exec(db, sql);
}

/* ================================================================
 * Token management
 * ================================================================ */

static int token_issue(sqlite3 *db, const char *scope, const char *actions,
                       const char *label)
{
    char ts[64]; iso_timestamp(ts, sizeof(ts));

    /* Generate token from timestamp + scope + random-ish */
    char raw[256];
    snprintf(raw, sizeof(raw), "bfcms_%s_%s_%ld", scope, ts, (long)getpid());
    unsigned long long h = fnv1a64(raw, strlen(raw));
    char token[128];
    snprintf(token, sizeof(token), "bfcms_%llx", h);

    /* Hash the token for storage */
    unsigned long long th = fnv1a64(token, strlen(token));
    char token_hash[64];
    snprintf(token_hash, sizeof(token_hash), "%llx", th);

    /* Store token */
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO _tokens (token_hash, label, scope, created_at, active) "
        "VALUES (?1, ?2, ?3, ?4, 1)",
        -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, token_hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, label ? label : "cli", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, scope, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, ts, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    /* Store permissions — for each registered content type */
    const char *act = actions ? actions : "find,findOne,create,update,delete";
    for (int i = 0; i < g_type_count; i++) {
        /* Parse comma-separated actions */
        char acts_copy[512];
        strncpy(acts_copy, act, sizeof(acts_copy) - 1);
        acts_copy[sizeof(acts_copy) - 1] = '\0';
        char *saveptr = NULL;
        char *a = strtok_r(acts_copy, ",", &saveptr);
        while (a) {
            while (*a == ' ') a++;
            sqlite3_stmt *ps;
            if (sqlite3_prepare_v2(db,
                "INSERT OR IGNORE INTO _permissions (token_hash, content_type, action, namespace_scope) "
                "VALUES (?1, ?2, ?3, ?4)",
                -1, &ps, NULL) == SQLITE_OK) {
                sqlite3_bind_text(ps, 1, token_hash, -1, SQLITE_STATIC);
                sqlite3_bind_text(ps, 2, g_types[i].name, -1, SQLITE_STATIC);
                sqlite3_bind_text(ps, 3, a, -1, SQLITE_STATIC);
                sqlite3_bind_text(ps, 4, scope, -1, SQLITE_STATIC);
                sqlite3_step(ps);
                sqlite3_finalize(ps);
            }
            a = strtok_r(NULL, ",", &saveptr);
        }
    }

    printf("{\n  \"token\": \"%s\",\n  \"scope\": \"%s\",\n  \"actions\": \"%s\"\n}\n",
        token, scope, act);
    return 0;
}

/* ================================================================
 * Embedded HTTP server (minimal POSIX sockets)
 * ================================================================ */

typedef struct {
    char method[16];
    char path[2048];
    char query[2048];
    char body[MAX_BODY];
    int body_len;
    char auth_token[256];
    int content_length;
} HttpRequest;

typedef struct {
    int fd;
    char *buf;
    size_t buf_len;
    int status;
    char content_type[64];
} HttpResponse;

static void http_resp_init(HttpResponse *r, int fd) {
    memset(r, 0, sizeof(*r));
    r->fd = fd;
    r->status = 200;
    r->buf = NULL;
    r->buf_len = 0;
    strcpy(r->content_type, "application/json");
}

static void http_resp_free(HttpResponse *r) {
    free(r->buf);
    r->buf = NULL;
    r->buf_len = 0;
}

static void http_resp_send(HttpResponse *r) {
    const char *status_text = "OK";
    if (r->status == 201) status_text = "Created";
    else if (r->status == 204) status_text = "No Content";
    else if (r->status == 400) status_text = "Bad Request";
    else if (r->status == 401) status_text = "Unauthorized";
    else if (r->status == 404) status_text = "Not Found";
    else if (r->status == 405) status_text = "Method Not Allowed";
    else if (r->status == 500) status_text = "Internal Server Error";

    char header[1024];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Authorization, Content-Type\r\n"
        "Connection: close\r\n"
        "\r\n",
        r->status, status_text, r->content_type, r->buf_len);

    write(r->fd, header, (size_t)hlen);
    if (r->buf_len > 0 && r->buf) write(r->fd, r->buf, r->buf_len);
}

static void http_resp_json(HttpResponse *r, int status, const char *fmt, ...) {
    r->status = status;
    free(r->buf);
    char tmp[8192];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    r->buf_len = (size_t)(n > 0 ? n : 0);
    r->buf = malloc(r->buf_len + 1);
    memcpy(r->buf, tmp, r->buf_len + 1);
}

static int parse_http_request(int fd, HttpRequest *req) {
    memset(req, 0, sizeof(*req));

    /* Read headers into a small stack buffer (max 8KB headers) */
    char hdr[8192];
    ssize_t total = 0;

    while (total < (ssize_t)sizeof(hdr) - 1) {
        ssize_t n = read(fd, hdr + total, sizeof(hdr) - (size_t)total - 1);
        if (n <= 0) break;
        total += n;
        hdr[total] = '\0';
        if (strstr(hdr, "\r\n\r\n")) break;
    }
    if (total <= 0) return -1;
    hdr[total] = '\0';

    /* Find header/body boundary BEFORE we modify the buffer */
    char *body_marker = strstr(hdr, "\r\n\r\n");
    size_t header_end = body_marker ? (size_t)(body_marker - hdr) : (size_t)total;

    /* Parse request line */
    char *line_end = strstr(hdr, "\r\n");
    if (!line_end) return -1;
    *line_end = '\0';

    sscanf(hdr, "%15s %2047s", req->method, req->path);

    /* Split path and query string */
    char *qmark = strchr(req->path, '?');
    if (qmark) {
        *qmark = '\0';
        strncpy(req->query, qmark + 1, sizeof(req->query) - 1);
    }

    /* Parse headers */
    char *hp = line_end + 2;
    while (hp < hdr + header_end) {
        char *he = strstr(hp, "\r\n");
        if (!he || he == hp) break;
        *he = '\0';
        if (strncasecmp(hp, "Content-Length:", 15) == 0) {
            req->content_length = atoi(hp + 15);
        }
        if (strncasecmp(hp, "Authorization:", 14) == 0) {
            const char *av = hp + 14;
            while (*av == ' ') av++;
            if (strncasecmp(av, "Bearer ", 7) == 0) av += 7;
            strncpy(req->auth_token, av, sizeof(req->auth_token) - 1);
        }
        hp = he + 2;
    }

    /* Extract body using pre-saved marker */
    if (body_marker) {
        char *body_start = body_marker + 4;
        size_t body_in_hdr = (size_t)(total - (body_start - hdr));

        if (body_in_hdr > 0) {
            size_t copy = body_in_hdr < MAX_BODY ? body_in_hdr : MAX_BODY;
            memcpy(req->body, body_start, copy);
            req->body_len = (int)copy;
        }

        /* Read remaining body if needed */
        if (req->content_length > 0 && (size_t)req->body_len < (size_t)req->content_length) {
            while ((size_t)req->body_len < (size_t)req->content_length &&
                   (size_t)req->body_len < MAX_BODY) {
                ssize_t n = read(fd, req->body + req->body_len,
                    (size_t)req->content_length - (size_t)req->body_len);
                if (n <= 0) break;
                req->body_len += (int)n;
            }
        }
        req->body[req->body_len < MAX_BODY ? req->body_len : MAX_BODY - 1] = '\0';
    }

    return 0;
}

/* Parse path segments: /v1/api/{namespace}/{collection}[/{id}]
   Returns segment count. */
static int parse_path_segments(const char *path, char segs[][256], int max_segs) {
    int count = 0;
    const char *p = path;
    while (*p == '/') p++;
    while (*p && count < max_segs) {
        size_t i = 0;
        while (*p && *p != '/' && i < 255) segs[count][i++] = *p++;
        segs[count][i] = '\0';
        count++;
        while (*p == '/') p++;
    }
    return count;
}

/* ---- Token auth enforcement ---- */

/* Map HTTP method to Strapi-style action name */
static const char *method_to_action(const char *method, int has_id) {
    if (strcmp(method, "GET") == 0) return has_id ? "findOne" : "find";
    if (strcmp(method, "POST") == 0) return "create";
    if (strcmp(method, "PUT") == 0) return "update";
    if (strcmp(method, "DELETE") == 0) return "delete";
    return NULL;
}

/*
 * Verify bearer token against _tokens + _permissions tables.
 * Returns 0 on success, -1 if unauthorized.
 * When no tokens exist in the database, all requests are allowed (open mode).
 */
static int check_auth(sqlite3 *db, const char *bearer, const char *content_type,
                      const char *action, const char *ns)
{
    /* Open mode: if no tokens have been issued, skip auth entirely.
     * This lets new installs work without needing token setup first. */
    {
        sqlite3_stmt *cnt;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM _tokens", -1, &cnt, NULL) == SQLITE_OK) {
            if (sqlite3_step(cnt) == SQLITE_ROW && sqlite3_column_int(cnt, 0) == 0) {
                sqlite3_finalize(cnt);
                return 0;  /* no tokens → open mode */
            }
            sqlite3_finalize(cnt);
        }
    }

    /* If tokens exist but request has no bearer, reject */
    if (!bearer || bearer[0] == '\0') return -1;

    /* Hash the bearer token */
    unsigned long long th = fnv1a64(bearer, strlen(bearer));
    char token_hash[64];
    snprintf(token_hash, sizeof(token_hash), "%llx", th);

    /* Check token exists and is active */
    sqlite3_stmt *ts;
    if (sqlite3_prepare_v2(db,
        "SELECT scope FROM _tokens WHERE token_hash = ?1 AND active = 1",
        -1, &ts, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(ts, 1, token_hash, -1, SQLITE_STATIC);
    if (sqlite3_step(ts) != SQLITE_ROW) {
        sqlite3_finalize(ts);
        return -1;  /* token not found or inactive */
    }
    const char *token_scope = (const char *)sqlite3_column_text(ts, 0);

    /* Scope check: token scoped to "root" can access everything;
     * otherwise token scope must match the request namespace */
    if (token_scope && strcmp(token_scope, "root") != 0 &&
        ns && strcmp(ns, "root") != 0 && strcmp(token_scope, ns) != 0) {
        sqlite3_finalize(ts);
        return -1;  /* namespace mismatch */
    }
    sqlite3_finalize(ts);

    /* Check permission for this content type + action */
    sqlite3_stmt *ps;
    if (sqlite3_prepare_v2(db,
        "SELECT 1 FROM _permissions WHERE token_hash = ?1 AND content_type = ?2 AND action = ?3",
        -1, &ps, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(ps, 1, token_hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(ps, 2, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(ps, 3, action, -1, SQLITE_STATIC);
    int ok = (sqlite3_step(ps) == SQLITE_ROW);
    sqlite3_finalize(ps);

    return ok ? 0 : -1;
}

/* Route API requests */
static void handle_api_request(sqlite3 *db, HttpRequest *req, HttpResponse *resp) {
    /* Parse: /v1/api/{namespace}/{collection}[/{id}] */
    char segs[MAX_PATH_SEGS][256];
    memset(segs, 0, sizeof(segs));
    int nseg = parse_path_segments(req->path, segs, MAX_PATH_SEGS);

    /* CORS preflight */
    if (strcmp(req->method, "OPTIONS") == 0) {
        http_resp_json(resp, 204, "");
        return;
    }

    /* /v1/api/... */
    if (nseg < 3 || strcmp(segs[0], "v1") != 0 || strcmp(segs[1], "api") != 0) {
        http_resp_json(resp, 404, "{\"error\":\"not found\"}");
        return;
    }

    /* segments: [v1, api, namespace_or_collection, ...] */
    const char *ns = "root";
    const char *collection = NULL;
    int entry_id = -1;

    if (nseg == 3) {
        /* /v1/api/{collection} — default namespace */
        collection = segs[2];
    } else if (nseg == 4) {
        /* Could be /v1/api/{namespace}/{collection} or /v1/api/{collection}/{id} */
        /* If segs[3] is numeric, it's an ID */
        int is_num = 1;
        for (const char *c = segs[3]; *c; c++) { if (!isdigit((unsigned char)*c)) { is_num = 0; break; } }
        if (is_num) {
            collection = segs[2];
            entry_id = atoi(segs[3]);
        } else {
            ns = segs[2];
            collection = segs[3];
        }
    } else if (nseg >= 5) {
        ns = segs[2];
        collection = segs[3];
        entry_id = atoi(segs[4]);
    }

    if (!collection || !find_type(collection)) {
        http_resp_json(resp, 404, "{\"error\":\"unknown content type: %s\"}", collection ? collection : "(null)");
        return;
    }

    /* Auth check */
    const char *action = method_to_action(req->method, entry_id > 0);
    if (action && check_auth(db, req->auth_token, collection, action, ns) != 0) {
        http_resp_json(resp, 401, "{\"error\":\"unauthorized\"}");
        return;
    }

    /* Route by method */
    if (strcmp(req->method, "GET") == 0) {
        if (entry_id > 0) {
            /* GET single */
            free(resp->buf); resp->buf = NULL; resp->buf_len = 0;
            FILE *mem = open_memstream(&resp->buf, &resp->buf_len);
            if (!mem) { http_resp_json(resp, 500, "{\"error\":\"memstream\"}"); return; }
            int rc = entry_get(db, collection, entry_id, mem);
            fclose(mem);
            resp->status = rc == 0 ? 200 : 404;
        } else {
            /* GET list */
            /* Parse query params for status filter */
            char status_filter[64] = {0};
            char *sf = strstr(req->query, "status=");
            if (sf) {
                sf += 7;
                size_t si = 0;
                while (*sf && *sf != '&' && si < sizeof(status_filter) - 1)
                    status_filter[si++] = *sf++;
                status_filter[si] = '\0';
            }
            int limit = 25;
            char *lf = strstr(req->query, "pageSize=");
            if (lf) limit = atoi(lf + 9);

            free(resp->buf); resp->buf = NULL; resp->buf_len = 0;
            FILE *mem = open_memstream(&resp->buf, &resp->buf_len);
            if (!mem) { http_resp_json(resp, 500, "{\"error\":\"memstream\"}"); return; }
            entry_list(db, collection, ns, status_filter, limit, mem);
            fclose(mem);
            resp->status = 200;
        }
    } else if (strcmp(req->method, "POST") == 0) {
        if (req->body_len == 0) {
            http_resp_json(resp, 400, "{\"error\":\"empty body\"}");
            return;
        }
        long long new_id = 0;
        int rc = entry_create(db, collection, ns, req->body, &new_id);
        if (rc == 0) {
            http_resp_json(resp, 201, "{\"data\":{\"id\":%lld},\"meta\":{\"created\":true}}", new_id);
        } else {
            http_resp_json(resp, 500, "{\"error\":\"create failed\"}");
        }
    } else if (strcmp(req->method, "PUT") == 0) {
        if (entry_id <= 0) {
            http_resp_json(resp, 400, "{\"error\":\"id required\"}");
            return;
        }
        int rc = entry_update(db, collection, entry_id, req->body);
        if (rc == 0) {
            http_resp_json(resp, 200, "{\"data\":{\"id\":%d},\"meta\":{\"updated\":true}}", entry_id);
        } else {
            http_resp_json(resp, 500, "{\"error\":\"update failed\"}");
        }
    } else if (strcmp(req->method, "DELETE") == 0) {
        if (entry_id <= 0) {
            http_resp_json(resp, 400, "{\"error\":\"id required\"}");
            return;
        }
        int rc = entry_delete(db, collection, entry_id);
        if (rc == 0) {
            http_resp_json(resp, 200, "{\"data\":{\"id\":%d},\"meta\":{\"deleted\":true}}", entry_id);
        } else {
            http_resp_json(resp, 500, "{\"error\":\"delete failed\"}");
        }
    } else {
        http_resp_json(resp, 405, "{\"error\":\"method not allowed\"}");
    }
}

/* Health + schema introspection endpoints */
static void handle_meta_request(sqlite3 *db, HttpRequest *req, HttpResponse *resp) {
    (void)db;
    char segs[MAX_PATH_SEGS][256];
    memset(segs, 0, sizeof(segs));
    int nseg = parse_path_segments(req->path, segs, MAX_PATH_SEGS);

    if (nseg >= 2 && strcmp(segs[0], "v1") == 0 && strcmp(segs[1], "health") == 0) {
        http_resp_json(resp, 200,
            "{\"status\":\"ok\",\"version\":\"%s\",\"types\":%d}", VERSION, g_type_count);
        return;
    }

    if (nseg >= 2 && strcmp(segs[0], "v1") == 0 && strcmp(segs[1], "schemas") == 0) {
        free(resp->buf); resp->buf = NULL; resp->buf_len = 0;
        FILE *mem = open_memstream(&resp->buf, &resp->buf_len);
        if (!mem) { http_resp_json(resp, 500, "{\"error\":\"memstream\"}"); return; }
        fprintf(mem, "{\"data\":[");
        for (int i = 0; i < g_type_count; i++) {
            if (i > 0) fprintf(mem, ",");
            fprintf(mem, "\n  {\"name\":\"%s\",\"displayName\":\"%s\",\"fields\":%d}",
                g_types[i].name, g_types[i].display_name, g_types[i].field_count);
        }
        fprintf(mem, "\n]}\n");
        fclose(mem);
        resp->status = 200;
        return;
    }
}

/* Connection handler — called per thread */
static void *connection_handler(void *arg) {
    int fd = *(int *)arg;
    free(arg);

    HttpRequest *req = malloc(sizeof(HttpRequest));
    if (!req) { close(fd); return NULL; }
    if (parse_http_request(fd, req) != 0) {
        free(req);
        close(fd);
        return NULL;
    }

    HttpResponse resp;
    http_resp_init(&resp, fd);

    /* Route: /v1/health, /v1/schemas, or /v1/api/... */
    pthread_mutex_lock(&g_db_mutex);
    if (strncmp(req->path, "/v1/api/", 8) == 0 || strcmp(req->path, "/v1/api") == 0) {
        handle_api_request(g_db, req, &resp);
    } else if (strncmp(req->path, "/v1/health", 10) == 0 ||
               strncmp(req->path, "/v1/schemas", 11) == 0) {
        handle_meta_request(g_db, req, &resp);
    } else {
        http_resp_json(&resp, 404, "{\"error\":\"not found\"}");
    }
    pthread_mutex_unlock(&g_db_mutex);

    http_resp_send(&resp);
    http_resp_free(&resp);
    free(req);
    close(fd);
    return NULL;
}

static int start_server(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return -1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(server_fd); return -1;
    }
    if (listen(server_fd, 64) < 0) {
        perror("listen"); close(server_fd); return -1;
    }

    fprintf(stderr, "[serve] BonfyreCMS v%s listening on http://0.0.0.0:%d\n", VERSION, port);
    fprintf(stderr, "[serve] content types: %d\n", g_type_count);

    /* Startup orphan check: warn about entries missing family membership */
    for (int i = 0; i < g_type_count; i++) {
        int orph = family_count_orphans(g_db, g_types[i].name);
        if (orph > 0)
            fprintf(stderr, "[serve] WARNING: %d orphan entries in '%s' — run 'discover %s' to repair\n",
                    orph, g_types[i].name, g_types[i].name);
    }

    fprintf(stderr, "[serve] endpoints:\n");
    fprintf(stderr, "  GET  /v1/health\n");
    fprintf(stderr, "  GET  /v1/schemas\n");
    for (int i = 0; i < g_type_count; i++) {
        fprintf(stderr, "  CRUD /v1/api/{ns}/%s[/{id}]\n", g_types[i].name);
    }

    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (!g_running) break;
            continue;
        }

        int *pfd = malloc(sizeof(int));
        *pfd = client_fd;
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
        pthread_create(&tid, &attr, connection_handler, pfd);
        pthread_attr_destroy(&attr);
    }

    close(server_fd);
    return 0;
}

/* ================================================================
 * CLI commands
 * ================================================================ */

static void print_usage(void) {
    printf(
        "BonfyreCMS v%s — stripped binary CMS\n"
        "\n"
        "Usage:\n"
        "  bonfyre-cms serve [--port PORT] [--db FILE] [--schemas DIR]\n"
        "  bonfyre-cms schema list [--db FILE]\n"
        "  bonfyre-cms schema apply <schema.json> [--db FILE]\n"
        "  bonfyre-cms schema migrate [--db FILE] [--schemas DIR]\n"
        "  bonfyre-cms entry create <type> [--ns NS] '<json>'\n"
        "  bonfyre-cms entry list <type> [--ns NS] [--status STATUS] [--limit N]\n"
        "  bonfyre-cms entry get <type> <id>\n"
        "  bonfyre-cms entry update <type> <id> '<json>'\n"
        "  bonfyre-cms entry delete <type> <id>\n"
        "  bonfyre-cms token issue [--scope NS] [--actions ACTS] [--label LABEL]\n"
        "  bonfyre-cms token list\n"
        "  bonfyre-cms ann build <type>\n"
        "  bonfyre-cms ann knn <type> '<json>' [k]\n"
        "  bonfyre-cms ann qknn <type> '<json>' [k]\n"
        "  bonfyre-cms ann qknn-entry <type> <id> [k]\n"
        "  bonfyre-cms ann qbench <type> [queries] [k]\n"
        "  bonfyre-cms ann stats <type>\n"
        "  bonfyre-cms bench-real <type> [--query-field F --query-op OP --query-value V]\n"
        "\n"
        "Options:\n"
        "  --port PORT     HTTP port (default: 8800)\n"
        "  --db FILE       SQLite database file (default: bonfyre_cms.db)\n"
        "  --schemas DIR   Content-type JSON directory (default: ./content-types)\n"
        "  --ns NAMESPACE  Namespace scope (default: root)\n"
        "\n",
        VERSION
    );
}

static const char *arg_opt(int argc, char **argv, const char *flag, const char *def) {
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    }
    return def;
}

static int arg_int(int argc, char **argv, const char *flag, int def) {
    const char *v = arg_opt(argc, argv, flag, NULL);
    return v ? atoi(v) : def;
}

static int arg_pos_int_or_default(int argc, char **argv, int idx, int def) {
    if (idx >= argc) return def;
    if (!argv[idx] || argv[idx][0] == '-') return def;
    return atoi(argv[idx]);
}

static void fprint_json_string(FILE *out, const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    fputc('"', out);
    while (p && *p) {
        switch (*p) {
            case '\\': fputs("\\\\", out); break;
            case '"': fputs("\\\"", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default: fputc((int)*p, out); break;
        }
        p++;
    }
    fputc('"', out);
}

static void fprint_json_string_or_null(FILE *out, const char *s) {
    if (!s || !s[0]) fputs("null", out);
    else fprint_json_string(out, s);
}

static double avg_i64(sqlite3_int64 total, int count) {
    return count > 0 ? (double)total / (double)count : 0.0;
}

static int content_type_row_count(sqlite3 *db, const char *type_name) {
    sqlite3_stmt *stmt = NULL;
    char *sql = NULL;
    int count = 0;

    if (!db || !type_name) return 0;
    sql = sqlite3_mprintf("SELECT COUNT(*) FROM \"%w\"", type_name);
    if (!sql) return 0;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    sqlite3_free(sql);
    return count;
}

static int load_content_type_ids(sqlite3 *db, const char *type_name, int limit,
                                 int **out_ids, int *out_count) {
    sqlite3_stmt *stmt = NULL;
    char *sql = NULL;
    int *ids = NULL;
    int cap = 0;
    int count = 0;
    int rc = -1;

    if (!db || !type_name || !out_ids || !out_count) return -1;
    *out_ids = NULL;
    *out_count = 0;

    if (limit <= 0) limit = 64;
    sql = sqlite3_mprintf("SELECT id FROM \"%w\" ORDER BY id LIMIT %d", type_name, limit);
    if (!sql) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) goto cleanup;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int *next;
        if (count >= cap) {
            cap = cap ? cap * 2 : 16;
            next = realloc(ids, (size_t)cap * sizeof(*ids));
            if (!next) goto cleanup;
            ids = next;
        }
        ids[count++] = sqlite3_column_int(stmt, 0);
    }

    *out_ids = ids;
    *out_count = count;
    ids = NULL;
    rc = 0;

cleanup:
    if (stmt) sqlite3_finalize(stmt);
    sqlite3_free(sql);
    free(ids);
    return rc;
}

static int run_existing_content_bench(sqlite3 *db, const char *type_name,
                                      const char *query_field, const char *query_op,
                                      const char *query_value, const char *agg_field,
                                      const char *partial_fields,
                                      const char *symbolic_field,
                                      const char *symbolic_value,
                                      int sample_limit, int list_runs, int query_runs) {
    int *ids = NULL;
    int id_count = 0;
    int total_rows = 0;
    int point_get_failures = 0;
    int point_list_failures = 0;
    int reduce_failures = 0;
    int partial_failures = 0;
    int hybrid_failures = 0;
    int symbolic_failures = 0;
    int family_repack_failures = 0;
    int family_count = 0;
    int ann_indexed = 0;
    int ann_queries = 0;
    int ann_self_hits = 0;
    int ann_returned_total = 0;
    double elapsed = 0.0;
    double point_get_p95_ms = 0.0;
    double point_get_bytes_touched_avg = 0.0;
    double point_list_p95_ms = 0.0;
    double point_list_vm_steps_avg = 0.0;
    double reduce_full_cell_reads_avg = 0.0;
    double reduce_partial_cell_reads_avg = 0.0;
    double tensor_filter_result_count_avg = 0.0;
    double tensor_filter_fullscan_steps_avg = 0.0;
    double tensor_agg_vm_steps_avg = 0.0;
    double hybrid_memo_hit_pct = 0.0;
    double hybrid_strategy_partial_pct = 0.0;
    double ann_exact_candidate_count_avg = 0.0;
    double ann_quant_rerank_shortlist_avg = 0.0;
    double symbolic_update_bindings_rebuilt_bytes_avg = 0.0;
    double family_repack_bytes_written_avg = 0.0;
    FILE *bench_sink = NULL;
    char *ann_quant_json = NULL;
    char *agg_json = NULL;
    int agg_enabled = (agg_field && agg_field[0]) ? 1 : 0;
    int query_enabled = (query_field && query_field[0] && query_op && query_op[0] && query_value) ? 1 : 0;
    int symbolic_enabled = (symbolic_field && symbolic_field[0] && symbolic_value) ? 1 : 0;
    struct timespec ts0, ts1;
    const BenchMetrics *bench_stats;

    if (!db || !type_name) return 1;

    total_rows = content_type_row_count(db, type_name);
    if (total_rows <= 0) {
        fprintf(stderr, "[bench-real] no rows found for type: %s\n", type_name);
        return 1;
    }

    if (sample_limit <= 0) sample_limit = total_rows < 64 ? total_rows : 64;
    if (list_runs <= 0) list_runs = 25;
    if (query_runs <= 0) query_runs = 25;

    if (load_content_type_ids(db, type_name, sample_limit, &ids, &id_count) != 0 || id_count <= 0) {
        fprintf(stderr, "[bench-real] could not load sample ids for: %s\n", type_name);
        free(ids);
        return 1;
    }

    bench_sink = fopen("/dev/null", "w");
    if (!bench_sink) bench_sink = tmpfile();
    if (!bench_sink) {
        fprintf(stderr, "[bench-real] could not open benchmark sink\n");
        free(ids);
        return 1;
    }

    bench_metrics_enable(1);
    bench_metrics_reset();

    printf("{\n\"type\":\"%s\",\n\"entries\":%d,\n", type_name, total_rows);

    bench_metrics_reset();
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    for (int i = 0; i < id_count; i++) {
        if (entry_get(db, type_name, ids[i], bench_sink) != 0) point_get_failures++;
    }
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    elapsed = elapsed_ms_between(&ts0, &ts1);
    bench_stats = bench_metrics_current();
    point_get_p95_ms = bench_latency_metric_p95(&bench_stats->point_get);
    point_get_bytes_touched_avg = avg_i64(bench_stats->point_get.total_bytes, bench_stats->point_get.count);
    printf("\"point_get_ms\":%.3f,\n", elapsed);

    bench_metrics_reset();
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    for (int r = 0; r < list_runs; r++) {
        if (entry_list(db, type_name, NULL, NULL, 25, bench_sink) != 0) point_list_failures++;
    }
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    elapsed = elapsed_ms_between(&ts0, &ts1);
    bench_stats = bench_metrics_current();
    point_list_p95_ms = bench_latency_metric_p95(&bench_stats->point_list);
    point_list_vm_steps_avg = avg_i64(bench_stats->point_list.total_steps, bench_stats->point_list.count);
    printf("\"point_list_ms\":%.3f,\n", elapsed);

    clock_gettime(CLOCK_MONOTONIC, &ts0);
    family_count = family_discover(db, type_name);
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    printf("\"family_discover_ms\":%.3f,\n\"families\":%d,\n",
           elapsed_ms_between(&ts0, &ts1), family_count);

    clock_gettime(CLOCK_MONOTONIC, &ts0);
    lt_compact(db, type_name);
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    printf("\"egraph_compact_ms\":%.3f,\n", elapsed_ms_between(&ts0, &ts1));

    clock_gettime(CLOCK_MONOTONIC, &ts0);
    tensor_abstract(db, type_name);
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    printf("\"tensor_abstract_ms\":%.3f,\n", elapsed_ms_between(&ts0, &ts1));

    bench_metrics_reset();
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    for (int i = 0; i < id_count; i++) {
        char *json = NULL;
        if (tensor_reduce(db, type_name, ids[i], REDUCE_FULL, NULL, NULL, &json) != 0 || !json)
            reduce_failures++;
        free(json);
    }
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    elapsed = elapsed_ms_between(&ts0, &ts1);
    bench_stats = bench_metrics_current();
    reduce_full_cell_reads_avg = avg_i64(bench_stats->reduce_full_cell_reads.total,
                                         bench_stats->reduce_full_cell_reads.calls);
    printf("\"beta_reduce_ms\":%.3f,\n", elapsed);

    if (!partial_fields || !partial_fields[0]) partial_fields = query_field;
    bench_metrics_reset();
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    for (int i = 0; i < id_count; i++) {
        char *json = NULL;
        if (!partial_fields ||
            tensor_reduce(db, type_name, ids[i], REDUCE_PARTIAL, partial_fields, NULL, &json) != 0 ||
            !json) partial_failures++;
        free(json);
    }
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    elapsed = elapsed_ms_between(&ts0, &ts1);
    bench_stats = bench_metrics_current();
    reduce_partial_cell_reads_avg = avg_i64(bench_stats->reduce_partial_cell_reads.total,
                                            bench_stats->reduce_partial_cell_reads.calls);
    printf("\"partial_apply_ms\":%.3f,\n", elapsed);

    bench_metrics_reset();
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < id_count; i++) {
            char *json = NULL;
            if (tensor_reduce(db, type_name, ids[i], REDUCE_AUTO, NULL, NULL, &json) != 0 || !json)
                hybrid_failures++;
            free(json);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    elapsed = elapsed_ms_between(&ts0, &ts1);
    bench_stats = bench_metrics_current();
    hybrid_memo_hit_pct = bench_stats->hybrid_auto.calls > 0
        ? 100.0 * (double)bench_stats->hybrid_auto.memo_hits / (double)bench_stats->hybrid_auto.calls
        : 0.0;
    hybrid_strategy_partial_pct = bench_stats->hybrid_auto.recommended_total > 0
        ? 100.0 * (double)bench_stats->hybrid_auto.recommended_partial / (double)bench_stats->hybrid_auto.recommended_total
        : 0.0;
    printf("\"hybrid_auto_ms\":%.3f,\n", elapsed);

    if (query_enabled) {
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < query_runs; r++) {
            char *json = NULL;
            tensor_query(db, type_name, query_field, query_op, query_value, &json);
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = elapsed_ms_between(&ts0, &ts1);
        bench_stats = bench_metrics_current();
        tensor_filter_result_count_avg = bench_stats->tensor_filter.calls > 0
            ? (double)bench_stats->tensor_filter.result_total / (double)bench_stats->tensor_filter.calls
            : 0.0;
        tensor_filter_fullscan_steps_avg = bench_stats->tensor_filter.calls > 0
            ? (double)bench_stats->tensor_filter.fullscan_steps_total / (double)bench_stats->tensor_filter.calls
            : 0.0;
        printf("\"tensor_query_ms\":%.3f,\n", elapsed);
    } else {
        printf("\"tensor_query_ms\":null,\n");
    }

    if (agg_enabled) {
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < query_runs; r++) {
            free(agg_json);
            agg_json = NULL;
            tensor_aggregate(db, type_name, agg_field, "sum", &agg_json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = elapsed_ms_between(&ts0, &ts1);
        bench_stats = bench_metrics_current();
        tensor_agg_vm_steps_avg = avg_i64(bench_stats->tensor_agg_vm_steps.total,
                                          bench_stats->tensor_agg_vm_steps.calls);
        printf("\"tensor_agg_ms\":%.3f,\n", elapsed);
    } else {
        printf("\"tensor_agg_ms\":null,\n");
    }

    if (symbolic_enabled) {
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 0; i < id_count; i++) {
            if (tensor_update(db, type_name, ids[i], symbolic_field, symbolic_value) != 0)
                symbolic_failures++;
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = elapsed_ms_between(&ts0, &ts1);
        bench_stats = bench_metrics_current();
        symbolic_update_bindings_rebuilt_bytes_avg =
            avg_i64(bench_stats->symbolic_update.bindings_rebuilt_bytes_total,
                    bench_stats->symbolic_update.calls);
        printf("\"symbolic_update_ms\":%.3f,\n", elapsed);
    } else {
        printf("\"symbolic_update_ms\":null,\n");
    }

    clock_gettime(CLOCK_MONOTONIC, &ts0);
    ann_indexed = ann_build_index(db, type_name);
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    printf("\"ann_build_ms\":%.3f,\n\"ann_indexed\":%d,\n",
           elapsed_ms_between(&ts0, &ts1), ann_indexed);

    if (ann_indexed > 0) {
        ann_queries = id_count < 25 ? id_count : 25;
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 0; i < ann_queries; i++) {
            char *knn_json = NULL;
            int got = ann_knn_by_entry(db, type_name, ids[i], 10, &knn_json);
            ann_returned_total += got;
            if (knn_json) {
                char needle[32];
                snprintf(needle, sizeof(needle), "\"target_id\":%d", ids[i]);
                if (strstr(knn_json, needle)) ann_self_hits++;
            }
            free(knn_json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = elapsed_ms_between(&ts0, &ts1);
        bench_stats = bench_metrics_current();
        ann_exact_candidate_count_avg = avg_i64(bench_stats->ann_exact.candidate_count_total,
                                                bench_stats->ann_exact.calls);
        printf("\"ann_knn_ms\":%.3f,\n\"ann_self_recall_pct\":%.1f,\n\"ann_avg_k_returned\":%.1f,\n",
               elapsed,
               ann_queries > 0 ? (100.0 * (double)ann_self_hits / (double)ann_queries) : 0.0,
               ann_queries > 0 ? (double)ann_returned_total / (double)ann_queries : 0.0);

        bench_metrics_reset();
        if (ann_quant_bench(db, type_name, ann_queries, 10, &ann_quant_json) == 0 && ann_quant_json) {
            bench_stats = bench_metrics_current();
            ann_quant_rerank_shortlist_avg =
                avg_i64(bench_stats->ann_quant.rerank_shortlist_total, bench_stats->ann_quant.calls);
            printf("\"ann_quant_bench\":%s,\n", ann_quant_json);
        } else {
            printf("\"ann_quant_bench\":null,\n");
        }
    } else {
        printf("\"ann_knn_ms\":null,\n\"ann_self_recall_pct\":0.0,\n\"ann_avg_k_returned\":0.0,\n"
               "\"ann_quant_bench\":null,\n");
    }

    bench_metrics_reset();
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    if (compact_pack_content_type(db, type_name) != 0) family_repack_failures++;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    elapsed = elapsed_ms_between(&ts0, &ts1);
    bench_stats = bench_metrics_current();
    family_repack_bytes_written_avg = bench_stats->family_repack.families > 0
        ? (double)bench_stats->family_repack.bytes_written_total / (double)bench_stats->family_repack.families
        : 0.0;
    printf("\"family_repack_ms\":%.3f,\n", elapsed);

    printf("\"query_bench\":{\"point_get\":{\"p95_ms\":%.3f,\"bytes_touched_avg\":%.1f},"
           "\"point_list\":{\"p95_ms\":%.3f,\"sqlite_vm_steps_avg\":%.1f},"
           "\"reduce_full\":{\"cell_reads_avg\":%.1f},"
           "\"reduce_partial\":{\"cell_reads_avg\":%.1f},"
           "\"tensor_filter\":{\"result_count_avg\":%.1f,\"fullscan_steps_avg\":%.1f},"
           "\"tensor_agg\":{\"sqlite_vm_steps_avg\":%.1f},"
           "\"hybrid_auto\":{\"memo_hit_pct\":%.1f,\"strategy_partial_pct\":%.1f},"
           "\"ann_exact\":{\"candidate_count_avg\":%.1f},"
           "\"ann_quant\":{\"rerank_shortlist_avg\":%.1f}},\n"
           "\"write_bench\":{\"symbolic_update\":{\"bindings_rebuilt_bytes_avg\":%.1f},"
           "\"family_repack\":{\"bytes_written_avg\":%.1f}},\n",
           point_get_p95_ms, point_get_bytes_touched_avg,
           point_list_p95_ms, point_list_vm_steps_avg,
           reduce_full_cell_reads_avg,
           reduce_partial_cell_reads_avg,
           tensor_filter_result_count_avg, tensor_filter_fullscan_steps_avg,
           tensor_agg_vm_steps_avg,
           hybrid_memo_hit_pct, hybrid_strategy_partial_pct,
           ann_exact_candidate_count_avg,
           ann_quant_rerank_shortlist_avg,
           symbolic_update_bindings_rebuilt_bytes_avg,
           family_repack_bytes_written_avg);

    printf("\"bench_config\":{\"sample_limit\":%d,\"list_runs\":%d,\"query_runs\":%d,"
           "\"query_field\":", sample_limit, list_runs, query_runs);
    fprint_json_string_or_null(stdout, query_enabled ? query_field : NULL);
    printf(",\"query_op\":");
    fprint_json_string_or_null(stdout, query_enabled ? query_op : NULL);
    printf(",\"query_value\":");
    fprint_json_string_or_null(stdout, query_enabled ? query_value : NULL);
    printf(",\"agg_field\":");
    fprint_json_string_or_null(stdout, agg_enabled ? agg_field : NULL);
    printf(",\"partial_fields\":");
    fprint_json_string_or_null(stdout, partial_fields);
    printf(",\"symbolic_field\":");
    fprint_json_string_or_null(stdout, symbolic_enabled ? symbolic_field : NULL);
    printf(",\"symbolic_value\":");
    fprint_json_string_or_null(stdout, symbolic_enabled ? symbolic_value : NULL);
    printf("},\n");

    printf("\"point_get_failures\":%d,\n\"point_list_failures\":%d,\n"
           "\"reduce_failures\":%d,\n\"partial_failures\":%d,\n"
           "\"hybrid_failures\":%d,\n\"symbolic_failures\":%d,\n"
           "\"family_repack_failures\":%d\n}\n",
           point_get_failures, point_list_failures,
           reduce_failures, partial_failures,
           hybrid_failures, symbolic_failures,
           family_repack_failures);

    free(agg_json);
    free(ann_quant_json);
    fclose(bench_sink);
    bench_metrics_enable(0);
    bench_metrics_cleanup();
    free(ids);
    return 0;
}

int main(int argc, char **argv) {
    const char *requested_db;
    const char *requested_schemas;
    const int is_bench = (argc >= 2 &&
        (strcmp(argv[1], "bench") == 0 || strcmp(argv[1], "bench-real") == 0));
    const int has_explicit_db = arg_has(argc, argv, "--db");
    char *db_path_owned = NULL;
    char *schemas_dir_owned = NULL;
    char bench_db_template[] = "/tmp/bonfyre_cms_bench_XXXXXX.db";
    int bench_fd = -1;
    int remove_db_on_exit = 0;
    const char *db_path;
    const char *schemas_dir;

    if (argc < 2) { print_usage(); return 0; }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    requested_db = arg_opt(argc, argv, "--db", DEFAULT_DB);
    requested_schemas = arg_opt(argc, argv, "--schemas", DEFAULT_SCHEMAS);

    if (is_bench && !has_explicit_db) {
        bench_fd = mkstemps(bench_db_template, 3);
        if (bench_fd < 0) {
            fprintf(stderr, "[fatal] cannot create bench db: %s\n", strerror(errno));
            return 1;
        }
        close(bench_fd);
        db_path_owned = dup_cstr(bench_db_template);
        if (!db_path_owned) {
            unlink(bench_db_template);
            fprintf(stderr, "[fatal] cannot allocate bench db path\n");
            return 1;
        }
        remove_db_on_exit = 1;
    } else {
        db_path_owned = dup_cstr(requested_db);
        if (!db_path_owned) {
            fprintf(stderr, "[fatal] cannot allocate db path\n");
            return 1;
        }
    }

    schemas_dir_owned = resolve_schemas_dir(argv[0], requested_schemas);
    if (!schemas_dir_owned) {
        free(db_path_owned);
        fprintf(stderr, "[fatal] cannot allocate schemas path\n");
        return 1;
    }

    db_path = db_path_owned;
    schemas_dir = schemas_dir_owned;

    /* Open database */
    sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    if (sqlite3_open(db_path, &g_db) != SQLITE_OK) {
        fprintf(stderr, "[fatal] cannot open db: %s\n", db_path);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 1;
    }
    sqlite3_busy_timeout(g_db, 5000);

    /* Use lower-overhead settings for self-contained benchmarks, WAL for normal runtime. */
    if (is_bench) {
        db_exec(g_db, "PRAGMA journal_mode=MEMORY;");
        db_exec(g_db, "PRAGMA synchronous=OFF;");
        db_exec(g_db, "PRAGMA temp_store=MEMORY;");
        db_exec(g_db, "PRAGMA cache_size=-32768;");
    } else {
        db_exec(g_db, "PRAGMA journal_mode=WAL;");
        db_exec(g_db, "PRAGMA synchronous=NORMAL;");
    }

    /* Bootstrap meta schema */
    if (bootstrap_meta_schema(g_db) != 0) {
        fprintf(stderr, "[fatal] cannot bootstrap meta schema\n");
        sqlite3_close(g_db);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 1;
    }

    /* Bootstrap BonfyreGraph modules */
    if (ops_bootstrap(g_db) != 0) {
        fprintf(stderr, "[fatal] cannot bootstrap ops table\n");
        sqlite3_close(g_db);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 1;
    }
    if (families_bootstrap(g_db) != 0) {
        fprintf(stderr, "[fatal] cannot bootstrap families tables\n");
        sqlite3_close(g_db);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 1;
    }
    compact_bindings_bootstrap(g_db);
    if (egraph_bootstrap(g_db) != 0) {
        fprintf(stderr, "[fatal] cannot bootstrap egraph tables\n");
        sqlite3_close(g_db);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 1;
    }
    if (tensor_ops_bootstrap(g_db) != 0) {
        fprintf(stderr, "[fatal] cannot bootstrap tensor ops tables\n");
        sqlite3_close(g_db);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 1;
    }

    /* Bootstrap Upgrade modules */
    incr_bootstrap(g_db);
    synthesis_bootstrap(g_db);
    ann_bootstrap(g_db);
    delta_bootstrap(g_db);
    tc_bootstrap(g_db);
    meta_bootstrap(g_db);
    col_bootstrap(g_db);
    hybrid_bootstrap(g_db);
    prov_bootstrap(g_db);
    transfer_bootstrap(g_db);

    /* === serve === */
    if (strcmp(argv[1], "serve") == 0) {
        int port = arg_int(argc, argv, "--port", DEFAULT_PORT);
        load_schemas_from_dir(g_db, schemas_dir);
        int rc = start_server(port);
        sqlite3_close(g_db);
        return rc;
    }

    /* === schema === */
    if (strcmp(argv[1], "schema") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms schema <list|apply|migrate>\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "list") == 0) {
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(g_db, "SELECT name, updated_at FROM _content_types ORDER BY name", -1, &stmt, NULL) == SQLITE_OK) {
                printf("%-30s %s\n", "NAME", "UPDATED");
                printf("%-30s %s\n", "----", "-------");
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    printf("%-30s %s\n",
                        (const char *)sqlite3_column_text(stmt, 0),
                        (const char *)sqlite3_column_text(stmt, 1));
                }
                sqlite3_finalize(stmt);
            }
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "apply") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms schema apply <file.json>\n"); sqlite3_close(g_db); return 1; }
            int rc = load_schema_file(g_db, argv[3]);
            sqlite3_close(g_db);
            return rc;
        }

        if (strcmp(argv[2], "migrate") == 0) {
            int n = load_schemas_from_dir(g_db, schemas_dir);
            printf("migrated %d schemas\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown schema command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === entry === */
    if (strcmp(argv[1], "entry") == 0) {
        /* Load schemas first */
        load_schemas_from_dir(g_db, schemas_dir);

        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms entry <create|list|get|update|delete>\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "create") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms entry create <type> '<json>'\n"); sqlite3_close(g_db); return 1; }
            const char *type = argv[3];
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            /* Find the JSON body — last non-flag argument */
            const char *body = NULL;
            for (int i = 4; i < argc; i++) {
                if (argv[i][0] != '-') { body = argv[i]; break; }
                if (argv[i][0] == '-' && i + 1 < argc) i++; /* skip flag value */
            }
            if (!body) { fprintf(stderr, "Missing JSON body\n"); sqlite3_close(g_db); return 1; }
            long long new_id = 0;
            int rc = entry_create(g_db, type, ns, body, &new_id);
            if (rc == 0) printf("{\"id\":%lld,\"created\":true}\n", new_id);
            sqlite3_close(g_db);
            return rc;
        }

        if (strcmp(argv[2], "list") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms entry list <type>\n"); sqlite3_close(g_db); return 1; }
            const char *type = argv[3];
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            const char *status = arg_opt(argc, argv, "--status", NULL);
            int limit = arg_int(argc, argv, "--limit", 25);
            entry_list(g_db, type, ns, status, limit, stdout);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "get") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms entry get <type> <id>\n"); sqlite3_close(g_db); return 1; }
            entry_get(g_db, argv[3], atoi(argv[4]), stdout);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "update") == 0) {
            if (argc < 6) { fprintf(stderr, "Usage: bonfyre-cms entry update <type> <id> '<json>'\n"); sqlite3_close(g_db); return 1; }
            const char *body = NULL;
            for (int i = 5; i < argc; i++) {
                if (argv[i][0] != '-') { body = argv[i]; break; }
                if (argv[i][0] == '-' && i + 1 < argc) i++;
            }
            if (!body) { fprintf(stderr, "Missing JSON body\n"); sqlite3_close(g_db); return 1; }
            int rc = entry_update(g_db, argv[3], atoi(argv[4]), body);
            if (rc == 0) printf("{\"id\":%d,\"updated\":true}\n", atoi(argv[4]));
            sqlite3_close(g_db);
            return rc;
        }

        if (strcmp(argv[2], "delete") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms entry delete <type> <id>\n"); sqlite3_close(g_db); return 1; }
            int rc = entry_delete(g_db, argv[3], atoi(argv[4]));
            if (rc == 0) printf("{\"id\":%d,\"deleted\":true}\n", atoi(argv[4]));
            sqlite3_close(g_db);
            return rc;
        }

        fprintf(stderr, "Unknown entry command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === ops (BonfyreGraph Flagship I) === */
    if (strcmp(argv[1], "ops") == 0) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms ops <history|reconstruct|count|head> <type> <id>\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "history") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops history <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *json = NULL;
            int n = ops_history(g_db, argv[3], atoll(argv[4]), &json);
            if (json) { printf("%s\n", json); free(json); }
            printf("# %d ops\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "reconstruct") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops reconstruct <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *json = NULL;
            int rc = ops_reconstruct(g_db, argv[3], atoll(argv[4]), &json);
            if (rc == 0 && json) { printf("%s\n", json); free(json); }
            else printf("{\"error\":\"not found or deleted\"}\n");
            sqlite3_close(g_db);
            return rc;
        }

        if (strcmp(argv[2], "count") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops count <type> <id>\n"); sqlite3_close(g_db); return 1; }
            int n = ops_count(g_db, argv[3], atoll(argv[4]));
            printf("{\"ops\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "head") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops head <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char hash[17] = {0};
            int rc = ops_head(g_db, argv[3], atoll(argv[4]), hash);
            if (rc == 0) printf("{\"head\":\"%s\"}\n", hash);
            else printf("{\"head\":null}\n");
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown ops command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === family (BonfyreGraph Flagship II) === */
    if (strcmp(argv[1], "family") == 0) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms family <discover> <type>\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "discover") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms family discover <type>\n"); sqlite3_close(g_db); return 1; }
            int n = family_discover(g_db, argv[3]);
            printf("{\"families\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown family command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === egraph (BonfyreGraph Flagship III) === */
    if (strcmp(argv[1], "egraph") == 0) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms egraph <extract|reprs|compact|stats> <type> [id]\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "extract") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms egraph extract <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *data = NULL;
            const char *pref = arg_opt(argc, argv, "--repr", NULL);
            int rc = egraph_extract(g_db, argv[3], atoi(argv[4]), pref, &data);
            if (rc == 0 && data) { printf("%s\n", data); free(data); }
            else printf("{\"error\":\"no representation found\"}\n");
            sqlite3_close(g_db);
            return rc == 0 ? 0 : 1;
        }

        if (strcmp(argv[2], "reprs") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms egraph reprs <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *json = NULL;
            int n = egraph_list_reprs(g_db, argv[3], atoi(argv[4]), &json);
            if (json) { printf("%s\n", json); free(json); }
            printf("# %d representations\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "compact") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms egraph compact <type>\n"); sqlite3_close(g_db); return 1; }
            int n = lt_compact(g_db, argv[3]);
            printf("{\"families_compacted\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "stats") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms egraph stats <type>\n"); sqlite3_close(g_db); return 1; }
            int fc = 0, mc = 0, rc = 0;
            lt_compact_stats(g_db, argv[3], &fc, &mc, &rc);
            printf("{\"families\":%d,\"members\":%d,\"representations\":%d}\n", fc, mc, rc);
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown egraph command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === tensor (Lambda Calculus compressed-domain ops) === */
    if (strcmp(argv[1], "tensor") == 0) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms tensor <abstract|reduce|query|update|agg|project|eigen|discover> ...\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "abstract") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms tensor abstract <type>\n"); sqlite3_close(g_db); return 1; }
            int n = tensor_abstract(g_db, argv[3]);
            printf("{\"families_abstracted\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "reduce") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms tensor reduce <type> <id> [--fields F] [--strategy full|partial|symbolic] [--value V]\n"); sqlite3_close(g_db); return 1; }
            const char *strat_str = arg_opt(argc, argv, "--strategy", "full");
            int strat = REDUCE_FULL;
            if (strcmp(strat_str, "partial") == 0) strat = REDUCE_PARTIAL;
            else if (strcmp(strat_str, "symbolic") == 0) strat = REDUCE_SYMBOLIC;
            const char *fields = arg_opt(argc, argv, "--fields", NULL);
            const char *value = arg_opt(argc, argv, "--value", NULL);
            char *result = NULL;
            int rc = tensor_reduce(g_db, argv[3], atoi(argv[4]), strat, fields, value, &result);
            if (rc == 0 && result) { printf("%s\n", result); free(result); }
            else printf("{\"error\":\"reduction failed\"}\n");
            sqlite3_close(g_db);
            return rc;
        }

        if (strcmp(argv[2], "query") == 0) {
            if (argc < 7) { fprintf(stderr, "Usage: bonfyre-cms tensor query <type> <field> <op> <value>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            int n = tensor_query(g_db, argv[3], argv[4], argv[5], argv[6], &result);
            if (result) { printf("%s\n", result); free(result); }
            printf("# %d matches\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "update") == 0) {
            if (argc < 7) { fprintf(stderr, "Usage: bonfyre-cms tensor update <type> <id> <field> <value>\n"); sqlite3_close(g_db); return 1; }
            int rc = tensor_update(g_db, argv[3], atoi(argv[4]), argv[5], argv[6]);
            if (rc == 0) printf("{\"updated\":true,\"field\":\"%s\"}\n", argv[5]);
            else printf("{\"error\":\"update failed\"}\n");
            sqlite3_close(g_db);
            return rc;
        }

        if (strcmp(argv[2], "agg") == 0) {
            if (argc < 6) { fprintf(stderr, "Usage: bonfyre-cms tensor agg <type> <field> <sum|avg|min|max|count>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            tensor_aggregate(g_db, argv[3], argv[4], argv[5], &result);
            if (result) { printf("%s\n", result); free(result); }
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "project") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms tensor project <type> <fields,...>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            int n = tensor_project(g_db, argv[3], argv[4], &result);
            if (result) { printf("%s\n", result); free(result); }
            printf("# %d entries\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "eigen") == 0) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms tensor eigen <type>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            int n = tensor_eigen_stats(g_db, argv[3], &result);
            if (result) { printf("%s\n", result); free(result); }
            printf("# %d eigenvalue entries\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "discover") == 0) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms tensor discover <type> <family_id>\n"); sqlite3_close(g_db); return 1; }
            int n = tensor_discover(g_db, argv[3], atoi(argv[4]));
            printf("{\"refinements\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown tensor command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === token === */
    if (strcmp(argv[1], "token") == 0) {
        load_schemas_from_dir(g_db, schemas_dir);

        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms token <issue|list>\n"); sqlite3_close(g_db); return 1; }

        if (strcmp(argv[2], "issue") == 0) {
            const char *scope = arg_opt(argc, argv, "--scope", "root");
            const char *actions = arg_opt(argc, argv, "--actions", NULL);
            const char *label = arg_opt(argc, argv, "--label", "cli");
            token_issue(g_db, scope, actions, label);
            sqlite3_close(g_db);
            return 0;
        }

        if (strcmp(argv[2], "list") == 0) {
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(g_db,
                "SELECT token_hash, label, scope, created_at, active FROM _tokens ORDER BY created_at DESC",
                -1, &stmt, NULL) == SQLITE_OK) {
                printf("%-20s %-15s %-25s %-25s %s\n", "HASH", "LABEL", "SCOPE", "CREATED", "ACTIVE");
                printf("%-20s %-15s %-25s %-25s %s\n", "----", "-----", "-----", "-------", "------");
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    printf("%-20s %-15s %-25s %-25s %d\n",
                        (const char *)sqlite3_column_text(stmt, 0),
                        (const char *)sqlite3_column_text(stmt, 1),
                        (const char *)sqlite3_column_text(stmt, 2),
                        (const char *)sqlite3_column_text(stmt, 3),
                        sqlite3_column_int(stmt, 4));
                }
                sqlite3_finalize(stmt);
            }
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown token command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    if (strcmp(argv[1], "bench-real") == 0) {
        const char *type_name;
        const char *query_field;
        const char *query_op;
        const char *query_value;
        const char *agg_field;
        const char *partial_fields;
        const char *symbolic_field;
        const char *symbolic_value;
        int sample_limit;
        int list_runs;
        int query_runs;
        int rc;

        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) {
            fprintf(stderr, "Usage: bonfyre-cms bench-real <type> [--query-field F --query-op OP --query-value V]\n");
            sqlite3_close(g_db);
            return 1;
        }
        type_name = argv[2];
        query_field = arg_opt(argc, argv, "--query-field", NULL);
        query_op = arg_opt(argc, argv, "--query-op", ">=");
        query_value = arg_opt(argc, argv, "--query-value", NULL);
        agg_field = arg_opt(argc, argv, "--agg-field", NULL);
        partial_fields = arg_opt(argc, argv, "--partial-fields", NULL);
        symbolic_field = arg_opt(argc, argv, "--symbolic-field", NULL);
        symbolic_value = arg_opt(argc, argv, "--symbolic-value", NULL);
        sample_limit = arg_int(argc, argv, "--sample-limit", 64);
        list_runs = arg_int(argc, argv, "--list-runs", 25);
        query_runs = arg_int(argc, argv, "--query-runs", 25);
        rc = run_existing_content_bench(g_db, type_name,
                                        query_field, query_op, query_value,
                                        agg_field, partial_fields,
                                        symbolic_field, symbolic_value,
                                        sample_limit, list_runs, query_runs);
        sqlite3_close(g_db);
        return rc;
    }

    /* === bench === */
    if (strcmp(argv[1], "bench") == 0) {
        int loaded = load_schemas_from_dir(g_db, schemas_dir);
        int N = arg_int(argc, argv, "--scale", 100);
        int create_failures = 0;
        int update_failures = 0;
        int reconstruct_failures = 0;
        int extract_failures = 0;
        int reduce_failures = 0;
        int partial_failures = 0;
        int symbolic_failures = 0;
        int point_get_failures = 0;
        int point_list_failures = 0;
        int hybrid_failures = 0;
        int family_repack_failures = 0;
        struct rusage usage;
        long max_rss_kb = 0;
        FILE *bench_sink = NULL;
        double point_get_p95_ms = 0.0;
        double point_get_bytes_touched_avg = 0.0;
        double point_list_p95_ms = 0.0;
        double point_list_vm_steps_avg = 0.0;
        double reduce_full_cell_reads_avg = 0.0;
        double reduce_partial_cell_reads_avg = 0.0;
        double tensor_filter_result_count_avg = 0.0;
        double tensor_filter_fullscan_steps_avg = 0.0;
        double tensor_agg_vm_steps_avg = 0.0;
        double hybrid_memo_hit_pct = 0.0;
        double hybrid_strategy_partial_pct = 0.0;
        double ann_exact_candidate_count_avg = 0.0;
        double ann_quant_rerank_shortlist_avg = 0.0;
        double symbolic_update_bindings_rebuilt_bytes_avg = 0.0;
        double family_repack_bytes_written_avg = 0.0;

        if (loaded <= 0) {
            fprintf(stderr, "[fatal] bench could not load schemas from: %s\n", schemas_dir);
            sqlite3_close(g_db);
            free(schemas_dir_owned);
            free(db_path_owned);
            if (remove_db_on_exit) unlink(db_path);
            return 1;
        }

        bench_sink = fopen("/dev/null", "w");
        if (!bench_sink) bench_sink = tmpfile();
        if (!bench_sink) {
            fprintf(stderr, "[fatal] bench could not open sink for query benchmarks\n");
            sqlite3_close(g_db);
            free(schemas_dir_owned);
            free(db_path_owned);
            if (remove_db_on_exit) unlink(db_path);
            return 1;
        }
        bench_metrics_enable(1);
        bench_metrics_reset();

        printf("{\n\"scale\":%d,\n", N);

        struct timespec ts0, ts1;
        double elapsed;

        /* Phase 1: bulk entry create */
        sqlite3_exec(g_db, "BEGIN", NULL, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char body[1024];
            const char *verts[] = {"dog_walking","group_fitness","produce_coop","car_detailing",
                "mobile_barber","childcare_pool","massage_wellness","tutoring","tool_library",
                "laundry","cooking_class","pet_grooming","dry_cleaning","coworking","language_exchange"};
            snprintf(body, sizeof(body),
                "{\"title\":\"Offer %d\",\"slug\":\"bench-%d\",\"vertical\":\"%s\","
                "\"base_price\":%d,\"min_threshold\":%d,\"current_stake_count\":%d,"
                "\"offer_status\":\"open\"}",
                i, i, verts[i % 15], 10 + (i * 7 % 90), 3 + (i % 20), i % 10);
            long long new_id;
            if (entry_create(g_db, "service_offer", "root", body, &new_id) != 0) create_failures++;
        }
        sqlite3_exec(g_db, "COMMIT", NULL, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"create_ms\":%.3f,\n\"create_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 2: bulk entry update */
        sqlite3_exec(g_db, "BEGIN", NULL, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char upd[128];
            snprintf(upd, sizeof(upd), "{\"current_stake_count\":%d}", i % 20 + 1);
            if (entry_update(g_db, "service_offer", i, upd) != 0) update_failures++;
        }
        sqlite3_exec(g_db, "COMMIT", NULL, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"update_ms\":%.3f,\n\"update_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 3: point lookup by id */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            if (entry_get(g_db, "service_offer", i, bench_sink) != 0) point_get_failures++;
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = elapsed_ms_between(&ts0, &ts1);
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            point_get_p95_ms = bench_latency_metric_p95(&bench_stats->point_get);
            point_get_bytes_touched_avg = bench_stats->point_get.count > 0
                ? (double)bench_stats->point_get.total_bytes / (double)bench_stats->point_get.count
                : 0.0;
        }
        printf("\"point_get_ms\":%.3f,\n\"point_get_per_us\":%.2f,\n",
               elapsed, N > 0 ? (elapsed / N) * 1000 : 0.0);

        /* Phase 4: list endpoint scan */
        {
            int point_list_runs = N < 100 ? N : 100;
            bench_metrics_reset();
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            for (int r = 0; r < point_list_runs; r++) {
                if (entry_list(g_db, "service_offer", NULL, NULL, 25, bench_sink) != 0)
                    point_list_failures++;
            }
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            elapsed = elapsed_ms_between(&ts0, &ts1);
            {
                const BenchMetrics *bench_stats = bench_metrics_current();
                point_list_p95_ms = bench_latency_metric_p95(&bench_stats->point_list);
                point_list_vm_steps_avg = bench_stats->point_list.count > 0
                    ? (double)bench_stats->point_list.total_steps / (double)bench_stats->point_list.count
                    : 0.0;
            }
            printf("\"point_list_ms\":%.3f,\n\"point_list_per_us\":%.2f,\n",
                   elapsed, point_list_runs > 0 ? (elapsed / point_list_runs) * 1000 : 0.0);
        }

        /* Phase 5: op log reconstruction */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char *json = NULL;
            if (ops_reconstruct(g_db, "service_offer", i, &json) != 0 || !json) reconstruct_failures++;
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"reconstruct_ms\":%.3f,\n\"reconstruct_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 4: family discovery */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        int fams = family_discover(g_db, "service_offer");
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"family_discover_ms\":%.3f,\n\"families\":%d,\n", elapsed, fams);

        /* Phase 5: e-graph compaction */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        int compacted = lt_compact(g_db, "service_offer");
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"egraph_compact_ms\":%.3f,\n\"compacted\":%d,\n", elapsed, compacted);

        /* Phase 6: e-graph extract */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char *data = NULL;
            if (egraph_extract(g_db, "service_offer", i, REPR_ROW, &data) != 0 || !data) extract_failures++;
            free(data);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"egraph_extract_ms\":%.3f,\n\"extract_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 7: tensor abstract (λ-abstraction) */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        int abstracted = tensor_abstract(g_db, "service_offer");
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"tensor_abstract_ms\":%.3f,\n\"families_abstracted\":%d,\n", elapsed, abstracted);

        /* Phase 10: tensor reduce — full β-reduction */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char *json = NULL;
            if (tensor_reduce(g_db, "service_offer", i, REDUCE_FULL, NULL, NULL, &json) != 0 || !json) reduce_failures++;
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            reduce_full_cell_reads_avg = bench_stats->reduce_full_cell_reads.calls > 0
                ? (double)bench_stats->reduce_full_cell_reads.total / (double)bench_stats->reduce_full_cell_reads.calls
                : 0.0;
        }
        printf("\"beta_reduce_ms\":%.3f,\n\"beta_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 11: tensor reduce — partial application */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char *json = NULL;
            if (tensor_reduce(g_db, "service_offer", i, REDUCE_PARTIAL, "title,base_price", NULL, &json) != 0 || !json) partial_failures++;
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            reduce_partial_cell_reads_avg = bench_stats->reduce_partial_cell_reads.calls > 0
                ? (double)bench_stats->reduce_partial_cell_reads.total / (double)bench_stats->reduce_partial_cell_reads.calls
                : 0.0;
        }
        printf("\"partial_apply_ms\":%.3f,\n\"partial_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 12: hybrid auto reduction */
        {
            int hybrid_queries = N < 64 ? N : 64;
            bench_metrics_reset();
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            for (int pass = 0; pass < 2; pass++) {
                for (int i = 1; i <= hybrid_queries; i++) {
                    char *json = NULL;
                    if (tensor_reduce(g_db, "service_offer", i, REDUCE_AUTO, NULL, NULL, &json) != 0 || !json)
                        hybrid_failures++;
                    free(json);
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            elapsed = elapsed_ms_between(&ts0, &ts1);
            {
                const BenchMetrics *bench_stats = bench_metrics_current();
                hybrid_memo_hit_pct = bench_stats->hybrid_auto.calls > 0
                    ? 100.0 * (double)bench_stats->hybrid_auto.memo_hits / (double)bench_stats->hybrid_auto.calls
                    : 0.0;
                hybrid_strategy_partial_pct = bench_stats->hybrid_auto.recommended_total > 0
                    ? 100.0 * (double)bench_stats->hybrid_auto.recommended_partial / (double)bench_stats->hybrid_auto.recommended_total
                    : 0.0;
            }
            printf("\"hybrid_auto_ms\":%.3f,\n\"hybrid_auto_per_us\":%.2f,\n",
                   elapsed,
                   hybrid_queries > 0 ? (elapsed / (double)(hybrid_queries * 2)) * 1000 : 0.0);
        }

        /* Phase 13: compressed-domain query */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < 100; r++) {
            char *json = NULL;
            tensor_query(g_db, "service_offer", "base_price", ">=", "50", &json);
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            tensor_filter_result_count_avg = bench_stats->tensor_filter.calls > 0
                ? (double)bench_stats->tensor_filter.result_total / (double)bench_stats->tensor_filter.calls
                : 0.0;
            tensor_filter_fullscan_steps_avg = bench_stats->tensor_filter.calls > 0
                ? (double)bench_stats->tensor_filter.fullscan_steps_total / (double)bench_stats->tensor_filter.calls
                : 0.0;
        }
        printf("\"tensor_query_100x_ms\":%.3f,\n\"query_per_us\":%.2f,\n", elapsed, (elapsed / 100) * 1000);

        /* Phase 14: columnar aggregates */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < 100; r++) {
            char *json = NULL;
            tensor_aggregate(g_db, "service_offer", "base_price", "sum", &json);
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            tensor_agg_vm_steps_avg = bench_stats->tensor_agg_vm_steps.calls > 0
                ? (double)bench_stats->tensor_agg_vm_steps.total / (double)bench_stats->tensor_agg_vm_steps.calls
                : 0.0;
        }
        printf("\"tensor_sum_100x_ms\":%.3f,\n\"sum_per_us\":%.2f,\n", elapsed, (elapsed / 100) * 1000);

        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < 100; r++) {
            char *json = NULL;
            tensor_aggregate(g_db, "service_offer", "base_price", "avg", &json);
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"tensor_avg_100x_ms\":%.3f,\n\"avg_per_us\":%.2f,\n", elapsed, (elapsed / 100) * 1000);

        /* Phase 12: tensor project */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        {
            char *json = NULL;
            tensor_project(g_db, "service_offer", "title,base_price", &json);
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"tensor_project_ms\":%.3f,\n", elapsed);

        /* Phase 16: tensor symbolic update (no decompression) */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            char val[32];
            snprintf(val, sizeof(val), "%d", 15 + (i * 3 % 50));
            if (tensor_update(g_db, "service_offer", i, "base_price", val) != 0) symbolic_failures++;
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            symbolic_update_bindings_rebuilt_bytes_avg = bench_stats->symbolic_update.calls > 0
                ? (double)bench_stats->symbolic_update.bindings_rebuilt_bytes_total / (double)bench_stats->symbolic_update.calls
                : 0.0;
        }
        printf("\"symbolic_update_ms\":%.3f,\n\"symbolic_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 14: eigenvalue profiling */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        {
            char *json = NULL;
            tensor_eigen_stats(g_db, "service_offer", &json);
            free(json);
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"eigen_profile_ms\":%.3f,\n", elapsed);

        /* Phase 15: discover */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        {
            sqlite3_stmt *fq;
            if (sqlite3_prepare_v2(g_db,
                "SELECT id FROM _families WHERE content_type='service_offer'",
                -1, &fq, NULL) == SQLITE_OK) {
                while (sqlite3_step(fq) == SQLITE_ROW) {
                    tensor_discover(g_db, "service_offer", sqlite3_column_int(fq, 0));
                }
                sqlite3_finalize(fq);
            }
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"discover_ms\":%.3f,\n", elapsed);

        /* Phase 16: baseline SQL queries for honest comparison */
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < 100; r++) {
            sqlite3_stmt *sq;
            if (sqlite3_prepare_v2(g_db,
                "SELECT id, base_price FROM service_offer WHERE base_price >= 50",
                -1, &sq, NULL) == SQLITE_OK) {
                while (sqlite3_step(sq) == SQLITE_ROW) { (void)sqlite3_column_int(sq, 0); }
                sqlite3_finalize(sq);
            }
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"baseline_sql_query_100x_ms\":%.3f,\n\"baseline_query_per_us\":%.2f,\n",
               elapsed, (elapsed / 100) * 1000);

        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int r = 0; r < 100; r++) {
            sqlite3_stmt *sq;
            if (sqlite3_prepare_v2(g_db,
                "SELECT SUM(base_price) FROM service_offer",
                -1, &sq, NULL) == SQLITE_OK) {
                if (sqlite3_step(sq) == SQLITE_ROW) { (void)sqlite3_column_double(sq, 0); }
                sqlite3_finalize(sq);
            }
        }
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"baseline_sql_sum_100x_ms\":%.3f,\n\"baseline_sum_per_us\":%.2f,\n",
               elapsed, (elapsed / 100) * 1000);

        /* Phase 17: raw JSON baseline create (INSERT only, no hooks) */
        {
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            sqlite3_exec(g_db, "CREATE TABLE IF NOT EXISTS _baseline_raw (id INTEGER PRIMARY KEY, data TEXT)", NULL, NULL, NULL);
            sqlite3_exec(g_db, "BEGIN", NULL, NULL, NULL);
            for (int i = 1; i <= N; i++) {
                char body[1024];
                snprintf(body, sizeof(body),
                    "{\"title\":\"Offer %d\",\"slug\":\"bench-%d\",\"base_price\":%d}",
                    i, i, 10 + (i * 7 % 90));
                sqlite3_stmt *ins;
                if (sqlite3_prepare_v2(g_db,
                    "INSERT INTO _baseline_raw (data) VALUES (?1)",
                    -1, &ins, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(ins, 1, body, -1, SQLITE_STATIC);
                    sqlite3_step(ins);
                    sqlite3_finalize(ins);
                }
            }
            sqlite3_exec(g_db, "COMMIT", NULL, NULL, NULL);
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
            printf("\"baseline_raw_insert_ms\":%.3f,\n\"baseline_raw_per_us\":%.2f,\n",
                   elapsed, (elapsed / N) * 1000);
            sqlite3_exec(g_db, "DROP TABLE IF EXISTS _baseline_raw", NULL, NULL, NULL);
        }

        /* Phase 18: deferred-mode create — INSERT + op log only, then batch discover */
        {
            /* Create a temporary table to hold deferred entries */
            sqlite3_exec(g_db, "CREATE TABLE IF NOT EXISTS _deferred_bench "
                "(id INTEGER PRIMARY KEY, title TEXT, slug TEXT, vertical TEXT, "
                "base_price INTEGER, min_threshold INTEGER, current_stake_count INTEGER, "
                "offer_status TEXT, namespace TEXT, created_at TEXT, updated_at TEXT, status TEXT)",
                NULL, NULL, NULL);
            int deferred_create_failures = 0;

            g_defer_indexing = 1;
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            sqlite3_exec(g_db, "BEGIN", NULL, NULL, NULL);
            for (int i = 1; i <= N; i++) {
                char body[1024];
                const char *verts[] = {"dog_walking","group_fitness","produce_coop","car_detailing",
                    "mobile_barber","childcare_pool","massage_wellness","tutoring","tool_library",
                    "laundry","cooking_class","pet_grooming","dry_cleaning","coworking","language_exchange"};
                snprintf(body, sizeof(body),
                    "{\"title\":\"Deferred %d\",\"slug\":\"def-%d\",\"vertical\":\"%s\","
                    "\"base_price\":%d,\"min_threshold\":%d,\"current_stake_count\":%d,"
                    "\"offer_status\":\"open\"}",
                    i, i, verts[i % 15], 10 + (i * 7 % 90), 3 + (i % 20), i % 10);
                long long new_id;
                if (entry_create(g_db, "service_offer", "root", body, &new_id) != 0) deferred_create_failures++;
            }
            sqlite3_exec(g_db, "COMMIT", NULL, NULL, NULL);
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            g_defer_indexing = 0;
            double deferred_ingest = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;

            /* Now batch-index via family_discover */
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            family_discover(g_db, "service_offer");
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            double deferred_reindex = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;

            printf("\"deferred_create_ms\":%.3f,\n\"deferred_create_per_us\":%.2f,\n"
                   "\"deferred_reindex_ms\":%.3f,\n\"deferred_total_ms\":%.3f,\n"
                   "\"deferred_create_failures\":%d,\n",
                   deferred_ingest, (deferred_ingest / N) * 1000,
                   deferred_reindex, deferred_ingest + deferred_reindex,
                   deferred_create_failures);
            sqlite3_exec(g_db, "DROP TABLE IF EXISTS _deferred_bench", NULL, NULL, NULL);
        }

        /* Phase 19: ANN recall — compare ANN knn results against brute-force ordering */
        {
            /* Re-abstract after deferred phase may have changed family IDs */
            tensor_abstract(g_db, "service_offer");

            /* Build full ANN index */
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            int ann_indexed = ann_build_index(g_db, "service_offer");
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
            printf("\"ann_build_ms\":%.3f,\n\"ann_indexed\":%d,\n", elapsed, ann_indexed);

            /* Query for k=10 against a sample entry */
            int ann_queries = N < 50 ? N : 50;
            int recall_hits = 0, recall_total = 0;
            int knn_returned_total = 0;
            bench_metrics_reset();
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            for (int qi = 1; qi <= ann_queries; qi++) {
                char *knn_json = NULL;
                int k = 10;
                /* Use stored vector directly — avoids JSON→bindings dimensional mismatch */
                int got = ann_knn_by_entry(g_db, "service_offer", qi, k, &knn_json);
                knn_returned_total += got;
                if (knn_json && got > 0) {
                    /* Count how many of the top-k include the source entry (self-recall) */
                    char needle[32];
                    snprintf(needle, sizeof(needle), "\"target_id\":%d", qi);
                    if (strstr(knn_json, needle)) recall_hits++;
                    recall_total++;
                }
                free(knn_json);
            }
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
            double recall_pct = recall_total > 0 ? (100.0 * recall_hits / recall_total) : 0.0;
            double avg_returned = ann_queries > 0 ? (double)knn_returned_total / ann_queries : 0.0;
            {
                const BenchMetrics *bench_stats = bench_metrics_current();
                ann_exact_candidate_count_avg = bench_stats->ann_exact.calls > 0
                    ? (double)bench_stats->ann_exact.candidate_count_total / (double)bench_stats->ann_exact.calls
                    : 0.0;
            }
            printf("\"ann_knn_queries\":%d,\n\"ann_knn_ms\":%.3f,\n"
                   "\"ann_knn_per_us\":%.2f,\n\"ann_self_recall_pct\":%.1f,\n"
                   "\"ann_avg_k_returned\":%.1f,\n",
                   ann_queries, elapsed, (elapsed / ann_queries) * 1000, recall_pct,
                   avg_returned);

            {
                char *quant_json = NULL;
                bench_metrics_reset();
                if (ann_quant_bench(g_db, "service_offer", ann_queries, 10, &quant_json) == 0 && quant_json) {
                    const BenchMetrics *bench_stats = bench_metrics_current();
                    ann_quant_rerank_shortlist_avg = bench_stats->ann_quant.calls > 0
                        ? (double)bench_stats->ann_quant.rerank_shortlist_total / (double)bench_stats->ann_quant.calls
                        : 0.0;
                    printf("\"ann_quant_bench\":%s,\n", quant_json);
                    free(quant_json);
                }
            }
        }

        /* Phase 20: compression ratio — raw JSON vs gen+bindings */
        {
            long long raw_json_bytes = 0;
            sqlite3_stmt *rq;
            /* Sum lengths of all columns serialized as JSON for service_offer */
            char count_sql[256];
            snprintf(count_sql, sizeof(count_sql),
                "SELECT COUNT(*) FROM service_offer");
            int n_entries = 0;
            if (sqlite3_prepare_v2(g_db, count_sql, -1, &rq, NULL) == SQLITE_OK) {
                if (sqlite3_step(rq) == SQLITE_ROW) n_entries = sqlite3_column_int(rq, 0);
                sqlite3_finalize(rq);
            }
            /* Estimate: pull each row, serialize, sum byte lengths */
            if (sqlite3_prepare_v2(g_db,
                "SELECT * FROM service_offer", -1, &rq, NULL) == SQLITE_OK) {
                int ncols = sqlite3_column_count(rq);
                while (sqlite3_step(rq) == SQLITE_ROW) {
                    for (int c = 0; c < ncols; c++) {
                        const unsigned char *txt = sqlite3_column_text(rq, c);
                        if (txt) raw_json_bytes += (long long)strlen((const char *)txt);
                        raw_json_bytes += 4; /* overhead for key+punctuation estimate */
                    }
                }
                sqlite3_finalize(rq);
            }
            /* gen_bytes and bind_bytes are computed later, so compute them here too */
            long long g_b = 0, b_b = 0;
            if (sqlite3_prepare_v2(g_db,
                "SELECT COALESCE(SUM(LENGTH(generator)),0) FROM _families WHERE content_type='service_offer'",
                -1, &rq, NULL) == SQLITE_OK) {
                if (sqlite3_step(rq) == SQLITE_ROW) g_b = sqlite3_column_int64(rq, 0);
                sqlite3_finalize(rq);
            }
            if (sqlite3_prepare_v2(g_db,
                "SELECT COALESCE(SUM(LENGTH(bindings)),0) FROM _family_members WHERE target_type='service_offer'",
                -1, &rq, NULL) == SQLITE_OK) {
                if (sqlite3_step(rq) == SQLITE_ROW) b_b = sqlite3_column_int64(rq, 0);
                sqlite3_finalize(rq);
            }
            long long tensor_total = g_b + b_b;
            double ratio = raw_json_bytes > 0 ? (double)tensor_total / (double)raw_json_bytes : 0.0;
            printf("\"raw_json_bytes\":%lld,\n\"tensor_bytes\":%lld,\n"
                   "\"compression_ratio\":%.4f,\n\"entries_counted\":%d,\n",
                   raw_json_bytes, tensor_total, ratio, n_entries);
        }

        /* Phase 21: stored family repack */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        if (compact_pack_content_type(g_db, "service_offer") != 0) family_repack_failures++;
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = elapsed_ms_between(&ts0, &ts1);
        {
            const BenchMetrics *bench_stats = bench_metrics_current();
            family_repack_bytes_written_avg = bench_stats->family_repack.families > 0
                ? (double)bench_stats->family_repack.bytes_written_total / (double)bench_stats->family_repack.families
                : 0.0;
        }
        printf("\"family_repack_ms\":%.3f,\n", elapsed);

        /* Phase 22: Compact binary bindings — measure Layer 1+2 (binary) and Layer 3 (delta) sizes */
        {
            long long json_bind_bytes = 0;
            long long packed_bytes = 0;
            long long delta_bytes = 0;
            long long packed_v2_bytes = 0;
            long long delta_v2_bytes = 0;
            int packed_count = 0;
            int delta_count = 0;
            int packed_v2_count = 0;
            int delta_v2_count = 0;
            long long delta_ref_bytes = 0;
            long long delta_v2_ref_bytes = 0;
            int delta_ref_count = 0;
            int delta_v2_ref_count = 0;
            sqlite3_stmt *fam_q;

            /* Walk families independently so each family gets its own primed reference binding. */
            if (sqlite3_prepare_v2(g_db,
                "SELECT id FROM _families WHERE content_type='service_offer' ORDER BY id",
                -1, &fam_q, NULL) == SQLITE_OK) {
                while (sqlite3_step(fam_q) == SQLITE_ROW) {
                    int family_id = sqlite3_column_int(fam_q, 0);
                    sqlite3_stmt *mem_q = NULL;
                    char *ref_binding_json = NULL;
                    unsigned char *ref_packed = NULL;
                    unsigned char *ref_packed_v2 = NULL;
                    size_t ref_packed_len = 0;
                    size_t ref_packed_v2_len = 0;

                    if (sqlite3_prepare_v2(g_db,
                        "SELECT bindings FROM _family_members "
                        "WHERE target_type='service_offer' AND family_id=?1 "
                        "ORDER BY target_id LIMIT 1",
                        -1, &mem_q, NULL) == SQLITE_OK) {
                        sqlite3_bind_int(mem_q, 1, family_id);
                        if (sqlite3_step(mem_q) == SQLITE_ROW) {
                            const char *rb = (const char *)sqlite3_column_text(mem_q, 0);
                            if (rb) {
                                ref_binding_json = strdup(rb);
                                compact_encode(rb, &ref_packed, &ref_packed_len);
                                compact_encode_v2(rb, &ref_packed_v2, &ref_packed_v2_len);
                            }
                        }
                        sqlite3_finalize(mem_q);
                        mem_q = NULL;
                    }

                    if (ref_packed && ref_packed_len > 0) {
                        delta_ref_bytes += (long long)ref_packed_len;
                        delta_ref_count++;
                    }
                    if (ref_packed_v2 && ref_packed_v2_len > 0) {
                        delta_v2_ref_bytes += (long long)ref_packed_v2_len;
                        delta_v2_ref_count++;
                    }

                    if (sqlite3_prepare_v2(g_db,
                        "SELECT bindings FROM _family_members "
                        "WHERE target_type='service_offer' AND family_id=?1 "
                        "ORDER BY target_id",
                        -1, &mem_q, NULL) == SQLITE_OK) {
                        sqlite3_bind_int(mem_q, 1, family_id);
                        while (sqlite3_step(mem_q) == SQLITE_ROW) {
                            const char *bj = (const char *)sqlite3_column_text(mem_q, 0);
                            if (!bj) continue;

                            json_bind_bytes += (long long)strlen(bj);

                            /* Layer 1+2: binary packing */
                            {
                                int pm = compact_measure(bj);
                                if (pm > 0) {
                                    packed_bytes += pm;
                                    packed_count++;
                                }
                                pm = compact_measure_v2(bj);
                                if (pm > 0) {
                                    packed_v2_bytes += pm;
                                    packed_v2_count++;
                                }
                            }

                            /* Layer 3: family-primed delta from the family reference binding */
                            if (ref_binding_json && ref_packed && ref_packed_len > 0) {
                                int dm = compact_delta_measure(ref_binding_json, bj);
                                if (dm > 0) {
                                    delta_bytes += dm;
                                    delta_count++;
                                }
                            }
                            if (ref_binding_json && ref_packed_v2 && ref_packed_v2_len > 0) {
                                int dm = compact_delta_measure_v2(ref_binding_json, bj);
                                if (dm > 0) {
                                    delta_v2_bytes += dm;
                                    delta_v2_count++;
                                }
                            }
                        }
                        sqlite3_finalize(mem_q);
                    }

                    free(ref_binding_json);
                    free(ref_packed);
                    free(ref_packed_v2);
                }
                sqlite3_finalize(fam_q);
            }

            /* Add reference binding overhead (stored once per family) */
            long long delta_total = delta_bytes + delta_ref_bytes;
            long long delta_v2_total = delta_v2_bytes + delta_v2_ref_bytes;

            double packed_ratio = json_bind_bytes > 0
                ? (double)packed_bytes / (double)json_bind_bytes : 0.0;
            double delta_ratio  = json_bind_bytes > 0
                ? (double)delta_total / (double)json_bind_bytes : 0.0;
            double packed_v2_ratio = json_bind_bytes > 0
                ? (double)packed_v2_bytes / (double)json_bind_bytes : 0.0;
            double delta_v2_ratio  = json_bind_bytes > 0
                ? (double)delta_v2_total / (double)json_bind_bytes : 0.0;

            /* Compression ratios relative to raw_json_bytes from Phase 20 */
            /* Re-fetch for combined ratio: gen + packed bindings */
            long long gen_b = 0;
            sqlite3_stmt *gq;
            if (sqlite3_prepare_v2(g_db,
                "SELECT COALESCE(SUM(LENGTH(generator)),0) FROM _families WHERE content_type='service_offer'",
                -1, &gq, NULL) == SQLITE_OK) {
                if (sqlite3_step(gq) == SQLITE_ROW) gen_b = sqlite3_column_int64(gq, 0);
                sqlite3_finalize(gq);
            }

            printf("\"compact_json_bind_bytes\":%lld,\n"
                   "\"compact_packed_bytes\":%lld,\n"
                   "\"compact_packed_ratio\":%.4f,\n"
                   "\"compact_delta_bytes\":%lld,\n"
                   "\"compact_delta_ref_bytes\":%lld,\n"
                   "\"compact_delta_ref_count\":%d,\n"
                   "\"compact_delta_total_bytes\":%lld,\n"
                   "\"compact_delta_ratio\":%.4f,\n"
                   "\"compact_gen_plus_packed\":%lld,\n"
                   "\"compact_gen_plus_delta\":%lld,\n"
                   "\"compact_packed_count\":%d,\n"
                   "\"compact_delta_count\":%d,\n"
                   "\"compact_v2_packed_bytes\":%lld,\n"
                   "\"compact_v2_packed_ratio\":%.4f,\n"
                   "\"compact_v2_delta_bytes\":%lld,\n"
                   "\"compact_v2_delta_ref_bytes\":%lld,\n"
                   "\"compact_v2_delta_ref_count\":%d,\n"
                   "\"compact_v2_delta_total_bytes\":%lld,\n"
                   "\"compact_v2_delta_ratio\":%.4f,\n"
                   "\"compact_v2_gen_plus_packed\":%lld,\n"
                   "\"compact_v2_gen_plus_delta\":%lld,\n"
                   "\"compact_v2_packed_count\":%d,\n"
                   "\"compact_v2_delta_count\":%d,\n",
                   json_bind_bytes,
                   packed_bytes, packed_ratio,
                   delta_bytes, delta_ref_bytes, delta_ref_count, delta_total, delta_ratio,
                   gen_b + packed_bytes,
                   gen_b + delta_total,
                   packed_count, delta_count,
                   packed_v2_bytes, packed_v2_ratio,
                   delta_v2_bytes, delta_v2_ref_bytes, delta_v2_ref_count, delta_v2_total, delta_v2_ratio,
                   gen_b + packed_v2_bytes,
                   gen_b + delta_v2_total,
                   packed_v2_count, delta_v2_count);
        }

        /* Phase 23: V2 compact bindings — small values + back-refs + sparse delta */
        {
            long long v2_packed_bytes = 0;
            long long v2_delta_bytes = 0;
            int v2_packed_count = 0;
            int v2_delta_count = 0;
            long long v2_delta_ref_bytes = 0;
            sqlite3_stmt *fam_q;

            if (sqlite3_prepare_v2(g_db,
                "SELECT id FROM _families WHERE content_type='service_offer' ORDER BY id",
                -1, &fam_q, NULL) == SQLITE_OK) {
                while (sqlite3_step(fam_q) == SQLITE_ROW) {
                    int family_id = sqlite3_column_int(fam_q, 0);
                    sqlite3_stmt *mem_q = NULL;
                    char *ref_binding_json = NULL;
                    unsigned char *ref_packed_v2 = NULL;
                    size_t ref_packed_v2_len = 0;

                    if (sqlite3_prepare_v2(g_db,
                        "SELECT bindings FROM _family_members "
                        "WHERE target_type='service_offer' AND family_id=?1 "
                        "ORDER BY target_id LIMIT 1",
                        -1, &mem_q, NULL) == SQLITE_OK) {
                        sqlite3_bind_int(mem_q, 1, family_id);
                        if (sqlite3_step(mem_q) == SQLITE_ROW) {
                            const char *rb = (const char *)sqlite3_column_text(mem_q, 0);
                            if (rb) {
                                ref_binding_json = strdup(rb);
                                compact_encode_v2(rb, &ref_packed_v2, &ref_packed_v2_len);
                            }
                        }
                        sqlite3_finalize(mem_q);
                        mem_q = NULL;
                    }

                    if (ref_packed_v2 && ref_packed_v2_len > 0)
                        v2_delta_ref_bytes += (long long)ref_packed_v2_len;

                    if (sqlite3_prepare_v2(g_db,
                        "SELECT bindings FROM _family_members "
                        "WHERE target_type='service_offer' AND family_id=?1 "
                        "ORDER BY target_id",
                        -1, &mem_q, NULL) == SQLITE_OK) {
                        sqlite3_bind_int(mem_q, 1, family_id);
                        while (sqlite3_step(mem_q) == SQLITE_ROW) {
                            const char *bj = (const char *)sqlite3_column_text(mem_q, 0);
                            if (!bj) continue;

                            int pm = compact_measure_v2(bj);
                            if (pm > 0) { v2_packed_bytes += pm; v2_packed_count++; }

                            if (ref_binding_json && ref_packed_v2 && ref_packed_v2_len > 0) {
                                int dm = compact_delta_measure_v2(ref_binding_json, bj);
                                if (dm > 0) { v2_delta_bytes += dm; v2_delta_count++; }
                            }
                        }
                        sqlite3_finalize(mem_q);
                    }

                    free(ref_binding_json);
                    free(ref_packed_v2);
                }
                sqlite3_finalize(fam_q);
            }

            long long v2_delta_total = v2_delta_bytes + v2_delta_ref_bytes;

            printf("\"v2_packed_bytes\":%lld,\n"
                   "\"v2_packed_count\":%d,\n"
                   "\"v2_delta_bytes\":%lld,\n"
                   "\"v2_delta_ref_bytes\":%lld,\n"
                   "\"v2_delta_total_bytes\":%lld,\n"
                   "\"v2_delta_count\":%d,\n",
                   v2_packed_bytes, v2_packed_count,
                   v2_delta_bytes, v2_delta_ref_bytes, v2_delta_total,
                   v2_delta_count);
        }

        /* Phase 21: gzip baseline — compress all rows, then measure random-access decompress+parse */
        {
            /* Collect all row JSON into a single blob */
            size_t blob_cap = 65536, blob_len = 0;
            char *blob = malloc(blob_cap);
            sqlite3_stmt *rq;
            if (sqlite3_prepare_v2(g_db, "SELECT * FROM service_offer", -1, &rq, NULL) == SQLITE_OK) {
                int ncols = sqlite3_column_count(rq);
                while (sqlite3_step(rq) == SQLITE_ROW) {
                    /* Serialize row as JSON line */
                    while (blob_len + 2048 >= blob_cap) { blob_cap *= 2; blob = realloc(blob, blob_cap); }
                    blob[blob_len++] = '{';
                    for (int c = 0; c < ncols; c++) {
                        const char *cn = sqlite3_column_name(rq, c);
                        const unsigned char *cv = sqlite3_column_text(rq, c);
                        if (c > 0) blob[blob_len++] = ',';
                        int wrote = snprintf(blob + blob_len, blob_cap - blob_len,
                            "\"%s\":\"%s\"", cn, cv ? (const char *)cv : "null");
                        blob_len += (size_t)wrote;
                    }
                    blob[blob_len++] = '}'; blob[blob_len++] = '\n';
                }
                sqlite3_finalize(rq);
            }

            /* Compress with zlib (deflate) */
            uLongf comp_len = compressBound((uLong)blob_len);
            char *compressed = malloc(comp_len);
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            compress2((Bytef *)compressed, &comp_len, (const Bytef *)blob, (uLong)blob_len, Z_DEFAULT_COMPRESSION);
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            double gzip_compress_ms = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;

            /* Decompress */
            uLongf decomp_len = (uLongf)blob_len;
            char *decompressed = malloc(decomp_len + 1);
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            uncompress((Bytef *)decompressed, &decomp_len, (const Bytef *)compressed, comp_len);
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            double gzip_decompress_ms = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;

            /* Simulate random access: decompress + scan for Nth line (simulates parse) */
            clock_gettime(CLOCK_MONOTONIC, &ts0);
            for (int i = 0; i < 100; i++) {
                /* Decompress entire blob for each access (worst case) */
                uLongf dl = (uLongf)blob_len;
                uncompress((Bytef *)decompressed, &dl, (const Bytef *)compressed, comp_len);
                /* Scan to Nth line */
                int target = i % (N > 0 ? N : 1);
                char *p = decompressed;
                for (int l = 0; l < target && p < decompressed + (size_t)dl; l++) {
                    while (*p && *p != '\n') p++;
                    if (*p == '\n') p++;
                }
            }
            clock_gettime(CLOCK_MONOTONIC, &ts1);
            double gzip_random_access_ms = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;

            double gzip_ratio = blob_len > 0 ? (double)comp_len / (double)blob_len : 0.0;
            printf("\"gzip_raw_bytes\":%zu,\n\"gzip_compressed_bytes\":%lu,\n"
                   "\"gzip_ratio\":%.4f,\n\"gzip_compress_ms\":%.3f,\n"
                   "\"gzip_decompress_ms\":%.3f,\n"
                   "\"gzip_random_100x_ms\":%.3f,\n\"gzip_random_per_us\":%.2f,\n",
                   blob_len, (unsigned long)comp_len, gzip_ratio,
                   gzip_compress_ms, gzip_decompress_ms,
                   gzip_random_access_ms, (gzip_random_access_ms / 100) * 1000);

            free(blob);
            free(compressed);
            free(decompressed);
        }

        /* Phase 24: V2 interned compact bindings — family-wide string dedup */
        {
            long long int_packed_bytes = 0;
            long long int_delta_bytes = 0;
            long long int_delta_ref_bytes = 0;
            long long int_strtab_bytes = 0;
            int int_packed_count = 0;
            int int_delta_count = 0;
            sqlite3_stmt *fam_q;

            if (sqlite3_prepare_v2(g_db,
                "SELECT id FROM _families WHERE content_type='service_offer' ORDER BY id",
                -1, &fam_q, NULL) == SQLITE_OK) {
                while (sqlite3_step(fam_q) == SQLITE_ROW) {
                    int family_id = sqlite3_column_int(fam_q, 0);

                    /* Pass 1: build family string table from ALL members */
                    FamilyStringTable strtab;
                    family_strtab_init(&strtab);
                    {
                        sqlite3_stmt *iq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id",
                            -1, &iq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(iq, 1, family_id);
                            while (sqlite3_step(iq) == SQLITE_ROW) {
                                const char *bj = (const char *)sqlite3_column_text(iq, 0);
                                if (bj) family_strtab_ingest(&strtab, bj);
                            }
                            sqlite3_finalize(iq);
                        }
                    }
                    family_strtab_finalize(&strtab);
                    int hdr_sz = family_strtab_header_size(&strtab);
                    int_strtab_bytes += hdr_sz;

                    /* Encode reference member with interned encoder */
                    char *ref_binding_json = NULL;
                    unsigned char *ref_packed_int = NULL;
                    size_t ref_packed_int_len = 0;
                    {
                        sqlite3_stmt *mq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id LIMIT 1",
                            -1, &mq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(mq, 1, family_id);
                            if (sqlite3_step(mq) == SQLITE_ROW) {
                                const char *rb = (const char *)sqlite3_column_text(mq, 0);
                                if (rb) {
                                    ref_binding_json = strdup(rb);
                                    compact_encode_v2_interned(rb, &strtab, &ref_packed_int, &ref_packed_int_len);
                                }
                            }
                            sqlite3_finalize(mq);
                        }
                    }

                    if (ref_packed_int && ref_packed_int_len > 0)
                        int_delta_ref_bytes += (long long)ref_packed_int_len;

                    /* Pass 2: measure packed + delta for all members */
                    {
                        sqlite3_stmt *mq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id",
                            -1, &mq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(mq, 1, family_id);
                            while (sqlite3_step(mq) == SQLITE_ROW) {
                                const char *bj = (const char *)sqlite3_column_text(mq, 0);
                                if (!bj) continue;

                                int pm = compact_measure_v2_interned(bj, &strtab);
                                if (pm > 0) { int_packed_bytes += pm; int_packed_count++; }

                                if (ref_binding_json && ref_packed_int && ref_packed_int_len > 0) {
                                    int dm = compact_delta_measure_v2_interned(ref_binding_json, bj, &strtab);
                                    if (dm > 0) { int_delta_bytes += dm; int_delta_count++; }
                                }
                            }
                            sqlite3_finalize(mq);
                        }
                    }

                    free(ref_binding_json);
                    free(ref_packed_int);
                    family_strtab_free(&strtab);
                }
                sqlite3_finalize(fam_q);
            }

            long long int_delta_total = int_delta_bytes + int_delta_ref_bytes + int_strtab_bytes;

            printf("\"interned_packed_bytes\":%lld,\n"
                   "\"interned_packed_count\":%d,\n"
                   "\"interned_delta_bytes\":%lld,\n"
                   "\"interned_delta_ref_bytes\":%lld,\n"
                   "\"interned_strtab_bytes\":%lld,\n"
                   "\"interned_delta_total_bytes\":%lld,\n"
                   "\"interned_delta_count\":%d,\n",
                   int_packed_bytes, int_packed_count,
                   int_delta_bytes, int_delta_ref_bytes, int_strtab_bytes,
                   int_delta_total, int_delta_count);
        }

        /* Phase 25: V2 Huffman — per-position canonical Huffman coding */
        {
            long long huff_packed_bytes = 0;
            long long huff_delta_bytes = 0;
            long long huff_delta_ref_bytes = 0;
            long long huff_header_bytes = 0;
            int huff_packed_count = 0;
            int huff_delta_count = 0;
            sqlite3_stmt *fam_q;

            if (sqlite3_prepare_v2(g_db,
                "SELECT id FROM _families WHERE content_type='service_offer' ORDER BY id",
                -1, &fam_q, NULL) == SQLITE_OK) {
                while (sqlite3_step(fam_q) == SQLITE_ROW) {
                    int family_id = sqlite3_column_int(fam_q, 0);

                    /* Pass 1: build Huffman table from ALL members */
                    FamilyHuffTable htab;
                    family_huff_init(&htab);
                    {
                        sqlite3_stmt *iq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id",
                            -1, &iq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(iq, 1, family_id);
                            while (sqlite3_step(iq) == SQLITE_ROW) {
                                const char *bj = (const char *)sqlite3_column_text(iq, 0);
                                if (bj) family_huff_ingest(&htab, bj);
                            }
                            sqlite3_finalize(iq);
                        }
                    }
                    family_huff_finalize(&htab);
                    int hdr_sz = family_huff_header_size(&htab);
                    huff_header_bytes += hdr_sz;

                    /* Get reference member */
                    char *ref_binding_json = NULL;
                    {
                        sqlite3_stmt *mq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id LIMIT 1",
                            -1, &mq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(mq, 1, family_id);
                            if (sqlite3_step(mq) == SQLITE_ROW) {
                                const char *rb = (const char *)sqlite3_column_text(mq, 0);
                                if (rb) ref_binding_json = strdup(rb);
                            }
                            sqlite3_finalize(mq);
                        }
                    }

                    if (ref_binding_json) {
                        int rsz = compact_measure_v2_huffman(ref_binding_json, &htab);
                        if (rsz > 0) huff_delta_ref_bytes += rsz;
                    }

                    /* Pass 2: measure packed + delta for all members */
                    {
                        sqlite3_stmt *mq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id",
                            -1, &mq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(mq, 1, family_id);
                            while (sqlite3_step(mq) == SQLITE_ROW) {
                                const char *bj = (const char *)sqlite3_column_text(mq, 0);
                                if (!bj) continue;

                                int pm = compact_measure_v2_huffman(bj, &htab);
                                if (pm > 0) { huff_packed_bytes += pm; huff_packed_count++; }

                                if (ref_binding_json) {
                                    int dm = compact_delta_measure_v2_huffman(ref_binding_json, bj, &htab);
                                    if (dm > 0) { huff_delta_bytes += dm; huff_delta_count++; }
                                }
                            }
                            sqlite3_finalize(mq);
                        }
                    }

                    free(ref_binding_json);
                    family_huff_free(&htab);
                }
                sqlite3_finalize(fam_q);
            }

            long long huff_delta_total = huff_delta_bytes + huff_delta_ref_bytes + huff_header_bytes;

            printf("\"huff_packed_bytes\":%lld,\n"
                   "\"huff_packed_count\":%d,\n"
                   "\"huff_delta_bytes\":%lld,\n"
                   "\"huff_delta_ref_bytes\":%lld,\n"
                   "\"huff_header_bytes\":%lld,\n"
                   "\"huff_delta_total_bytes\":%lld,\n"
                   "\"huff_delta_count\":%d,\n",
                   huff_packed_bytes, huff_packed_count,
                   huff_delta_bytes, huff_delta_ref_bytes, huff_header_bytes,
                   huff_delta_total, huff_delta_count);
        }

        /* Phase 26: Hybrid Arithmetic-Huffman — fractional-bit entropy coding */
        {
            long long arith_packed_bytes = 0;
            long long arith_delta_bytes = 0;
            long long arith_delta_ref_bytes = 0;
            long long arith_header_bytes = 0;
            int arith_packed_count = 0;
            int arith_delta_count = 0;
            sqlite3_stmt *fam_q;

            if (sqlite3_prepare_v2(g_db,
                "SELECT id FROM _families WHERE content_type='service_offer' ORDER BY id",
                -1, &fam_q, NULL) == SQLITE_OK) {
                while (sqlite3_step(fam_q) == SQLITE_ROW) {
                    int family_id = sqlite3_column_int(fam_q, 0);

                    FamilyHuffTable htab;
                    family_huff_init(&htab);
                    {
                        sqlite3_stmt *iq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id",
                            -1, &iq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(iq, 1, family_id);
                            while (sqlite3_step(iq) == SQLITE_ROW) {
                                const char *bj = (const char *)sqlite3_column_text(iq, 0);
                                if (bj) family_huff_ingest(&htab, bj);
                            }
                            sqlite3_finalize(iq);
                        }
                    }
                    family_huff_finalize(&htab);
                    arith_header_bytes += family_huff_header_size(&htab);

                    char *ref_binding_json = NULL;
                    {
                        sqlite3_stmt *mq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id LIMIT 1",
                            -1, &mq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(mq, 1, family_id);
                            if (sqlite3_step(mq) == SQLITE_ROW) {
                                const char *rb = (const char *)sqlite3_column_text(mq, 0);
                                if (rb) ref_binding_json = strdup(rb);
                            }
                            sqlite3_finalize(mq);
                        }
                    }

                    if (ref_binding_json) {
                        int rsz = compact_measure_v2_arithmetic(ref_binding_json, &htab);
                        if (rsz > 0) arith_delta_ref_bytes += rsz;
                    }

                    {
                        sqlite3_stmt *mq;
                        if (sqlite3_prepare_v2(g_db,
                            "SELECT bindings FROM _family_members "
                            "WHERE target_type='service_offer' AND family_id=?1 "
                            "ORDER BY target_id",
                            -1, &mq, NULL) == SQLITE_OK) {
                            sqlite3_bind_int(mq, 1, family_id);
                            while (sqlite3_step(mq) == SQLITE_ROW) {
                                const char *bj = (const char *)sqlite3_column_text(mq, 0);
                                if (!bj) continue;

                                int pm = compact_measure_v2_arithmetic(bj, &htab);
                                if (pm > 0) { arith_packed_bytes += pm; arith_packed_count++; }

                                if (ref_binding_json) {
                                    int dm = compact_delta_measure_v2_arithmetic(ref_binding_json, bj, &htab);
                                    if (dm > 0) { arith_delta_bytes += dm; arith_delta_count++; }
                                }
                            }
                            sqlite3_finalize(mq);
                        }
                    }

                    free(ref_binding_json);
                    family_huff_free(&htab);
                }
                sqlite3_finalize(fam_q);
            }

            long long arith_delta_total = arith_delta_bytes + arith_delta_ref_bytes + arith_header_bytes;

            printf("\"arith_packed_bytes\":%lld,\n"
                   "\"arith_packed_count\":%d,\n"
                   "\"arith_delta_bytes\":%lld,\n"
                   "\"arith_delta_ref_bytes\":%lld,\n"
                   "\"arith_header_bytes\":%lld,\n"
                   "\"arith_delta_total_bytes\":%lld,\n"
                   "\"arith_delta_count\":%d,\n",
                   arith_packed_bytes, arith_packed_count,
                   arith_delta_bytes, arith_delta_ref_bytes, arith_header_bytes,
                   arith_delta_total, arith_delta_count);
        }

        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            max_rss_kb = usage.ru_maxrss;
#ifdef __APPLE__
            max_rss_kb /= 1024;
#endif
        }

        /* Storage metrics */
        int ops_total = 0, cells_total = 0, eigen_total = 0, reprs_total = 0, members_total = 0;
        int family_count_total = 0;
        long long gen_bytes = 0, bind_bytes = 0;
        {
            sqlite3_stmt *q;
            if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM _ops", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) ops_total = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM _tensor_cells", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) cells_total = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM _tensor_eigen", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) eigen_total = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM _equivalences", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) reprs_total = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM _family_members", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) members_total = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM _families WHERE content_type='service_offer'", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) family_count_total = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db,
                "SELECT COALESCE(SUM(LENGTH(generator)),0) FROM _families WHERE content_type='service_offer'",
                -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) gen_bytes = sqlite3_column_int64(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db,
                "SELECT COALESCE(SUM(LENGTH(bindings)),0) FROM _family_members WHERE target_type='service_offer'",
                -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) bind_bytes = sqlite3_column_int64(q, 0);
                sqlite3_finalize(q);
            }
        }
        printf("\"ops_total\":%d,\n\"tensor_cells\":%d,\n\"eigen_rows\":%d,\n"
               "\"egraph_reprs\":%d,\n\"family_members\":%d,\n\"family_count\":%d,\n"
               "\"generator_bytes\":%lld,\n\"binding_bytes\":%lld,\n",
               ops_total, cells_total, eigen_total, reprs_total, members_total,
               family_count_total, gen_bytes, bind_bytes);

        /* Database page-level size via PRAGMA */
        {
            int page_count = 0, page_size = 0;
            sqlite3_stmt *q;
            if (sqlite3_prepare_v2(g_db, "PRAGMA page_count", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) page_count = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            if (sqlite3_prepare_v2(g_db, "PRAGMA page_size", -1, &q, NULL) == SQLITE_OK) {
                if (sqlite3_step(q) == SQLITE_ROW) page_size = sqlite3_column_int(q, 0);
                sqlite3_finalize(q);
            }
            long long db_bytes = (long long)page_count * page_size;
            printf("\"db_bytes\":%lld,\n\"db_pages\":%d,\n\"page_size\":%d,\n",
                   db_bytes, page_count, page_size);
            /* Content vs metadata overhead ratio */
            long long content_bytes_est = (long long)N * 200; /* ~200B raw JSON per entry */
            double overhead_ratio = content_bytes_est > 0 ? (double)db_bytes / (double)content_bytes_est : 0.0;
            printf("\"content_bytes_est\":%lld,\n\"storage_overhead_x\":%.1f,\n",
                   content_bytes_est, overhead_ratio);
        }

        /* Orphan detection: entries without family membership */
        {
            int orphans = family_count_orphans(g_db, "service_offer");
            printf("\"orphan_entries\":%d,\n", orphans);
        }

        printf("\"query_bench\":{\"point_get\":{\"p95_ms\":%.3f,\"bytes_touched_avg\":%.1f},"
               "\"point_list\":{\"p95_ms\":%.3f,\"sqlite_vm_steps_avg\":%.1f},"
               "\"reduce_full\":{\"cell_reads_avg\":%.1f},"
               "\"reduce_partial\":{\"cell_reads_avg\":%.1f},"
               "\"tensor_filter\":{\"result_count_avg\":%.1f,\"fullscan_steps_avg\":%.1f},"
               "\"tensor_agg\":{\"sqlite_vm_steps_avg\":%.1f},"
               "\"hybrid_auto\":{\"memo_hit_pct\":%.1f,\"strategy_partial_pct\":%.1f},"
               "\"ann_exact\":{\"candidate_count_avg\":%.1f},"
               "\"ann_quant\":{\"rerank_shortlist_avg\":%.1f}},\n"
               "\"write_bench\":{\"symbolic_update\":{\"bindings_rebuilt_bytes_avg\":%.1f},"
               "\"family_repack\":{\"bytes_written_avg\":%.1f}},\n",
               point_get_p95_ms, point_get_bytes_touched_avg,
               point_list_p95_ms, point_list_vm_steps_avg,
               reduce_full_cell_reads_avg,
               reduce_partial_cell_reads_avg,
               tensor_filter_result_count_avg, tensor_filter_fullscan_steps_avg,
               tensor_agg_vm_steps_avg,
               hybrid_memo_hit_pct, hybrid_strategy_partial_pct,
               ann_exact_candidate_count_avg,
               ann_quant_rerank_shortlist_avg,
               symbolic_update_bindings_rebuilt_bytes_avg,
               family_repack_bytes_written_avg);

        printf("\"create_failures\":%d,\n\"update_failures\":%d,\n"
               "\"point_get_failures\":%d,\n\"point_list_failures\":%d,\n"
               "\"reconstruct_failures\":%d,\n\"extract_failures\":%d,\n"
               "\"reduce_failures\":%d,\n\"partial_failures\":%d,\n"
               "\"hybrid_failures\":%d,\n\"symbolic_failures\":%d,\n"
               "\"family_repack_failures\":%d,\n\"max_rss_kb\":%ld\n}\n",
               create_failures, update_failures,
               point_get_failures, point_list_failures,
               reconstruct_failures, extract_failures,
               reduce_failures, partial_failures,
               hybrid_failures, symbolic_failures,
               family_repack_failures, max_rss_kb);

        bench_metrics_enable(0);
        bench_metrics_cleanup();
        fclose(bench_sink);
        sqlite3_close(g_db);
        free(schemas_dir_owned);
        free(db_path_owned);
        if (remove_db_on_exit) unlink(db_path);
        return 0;
    }

    /* === incr (Upgrade I: Incremental Lambda) === */
    if (strcmp(argv[1], "incr") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms incr <compact|stats> <content_type|family_id>\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "compact") == 0 && argc >= 4) {
            incr_compact_online(g_db, argv[3], 0.01, 1.5);
            printf("{\"status\":\"ok\",\"action\":\"incr_compact\",\"content_type\":\"%s\"}\n", argv[3]);
        } else if (strcmp(argv[2], "stats") == 0 && argc >= 4) {
            char *json = NULL;
            if (incr_eigen_stats(g_db, atoi(argv[3]), &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === synth (Upgrade II: Generator Synthesis) === */
    if (strcmp(argv[1], "synth") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms synth <propose|candidates> <content_type>\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "propose") == 0 && argc >= 4) {
            int n = synthesis_propose(g_db, argv[3]);
            printf("{\"proposed\":%d}\n", n);
        } else if (strcmp(argv[2], "candidates") == 0 && argc >= 4) {
            char *json = NULL;
            int limit = argc >= 5 ? atoi(argv[4]) : 10;
            if (synthesis_candidates(g_db, argv[3], limit, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === ann (Upgrade III: ANN Index) === */
    if (strcmp(argv[1], "ann") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms ann <build|knn|qknn|qknn-entry|qbench|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "build") == 0 && argc >= 4) {
            int n = ann_build_index(g_db, argv[3]);
            printf("{\"indexed\":%d}\n", n);
        } else if (strcmp(argv[2], "knn") == 0 && argc >= 5) {
            int k = arg_pos_int_or_default(argc, argv, 5, 5);
            char *json = NULL;
            if (ann_knn_query(g_db, argv[3], argv[4], k, &json) >= 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "qknn") == 0 && argc >= 5) {
            int k = arg_pos_int_or_default(argc, argv, 5, 5);
            char *json = NULL;
            if (ann_knn_query_quantized(g_db, argv[3], argv[4], k, &json) >= 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "qknn-entry") == 0 && argc >= 5) {
            int k = arg_pos_int_or_default(argc, argv, 5, 5);
            char *json = NULL;
            if (ann_knn_by_entry_quantized(g_db, argv[3], atoi(argv[4]), k, &json) >= 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "qbench") == 0 && argc >= 4) {
            int queries = arg_pos_int_or_default(argc, argv, 4, 50);
            int k = arg_pos_int_or_default(argc, argv, 5, 10);
            char *json = NULL;
            if (ann_quant_bench(g_db, argv[3], queries, k, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "stats") == 0 && argc >= 4) {
            char *json = NULL;
            if (ann_stats(g_db, argv[3], &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === delta (Upgrade IV: Delta Sync) === */
    if (strcmp(argv[1], "delta") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms delta <export|checkpoint|stats> <ns>\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "export") == 0 && argc >= 4) {
            char *json = NULL;
            if (delta_export_json(g_db, argv[3], NULL, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "checkpoint") == 0 && argc >= 5) {
            delta_checkpoint(g_db, argv[3], argv[4], "");
            printf("{\"status\":\"ok\"}\n");
        }
        sqlite3_close(g_db); return 0;
    }

    /* === typecheck (Upgrade V: Typed Generators) === */
    if (strcmp(argv[1], "typecheck") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms typecheck <infer|verify|constraints> <family_id>\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "infer") == 0 && argc >= 4) {
            int n = tc_infer(g_db, atoi(argv[3]));
            printf("{\"inferred\":%d}\n", n);
        } else if (strcmp(argv[2], "infer-all") == 0 && argc >= 4) {
            int n = tc_infer_all(g_db, argv[3]);
            printf("{\"inferred\":%d}\n", n);
        } else if (strcmp(argv[2], "verify") == 0 && argc >= 4) {
            int valid = tc_verify_family(g_db, atoi(argv[3]));
            printf("{\"valid\":%s}\n", valid ? "true" : "false");
        } else if (strcmp(argv[2], "constraints") == 0 && argc >= 4) {
            char *json = NULL;
            if (tc_constraints_json(g_db, atoi(argv[3]), &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === meta (Upgrade VI: Meta-Generators) === */
    if (strcmp(argv[1], "meta") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms meta <discover|compact|stats> <content_type>\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "discover") == 0 && argc >= 4) {
            int n = meta_discover(g_db, argv[3]);
            printf("{\"meta_families\":%d}\n", n);
        } else if (strcmp(argv[2], "compact") == 0 && argc >= 4) {
            double ratio = meta_compact(g_db, argv[3]);
            printf("{\"compression_improvement\":%.4f}\n", ratio);
        } else if (strcmp(argv[2], "stats") == 0 && argc >= 4) {
            char *json = NULL;
            if (meta_stats(g_db, argv[3], &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === columnar (Upgrade VII: Vectorized Columnar Engine) === */
    if (strcmp(argv[1], "columnar") == 0) {
        if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms columnar <stats|scan> <family_id> ...\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "stats") == 0) {
            ColBatch batch;
            if (col_materialize(g_db, argv[3], NULL, 0, &batch) == 0) {
                char *json = NULL;
                if (col_stats(&batch, &json) == 0 && json) {
                    printf("%s\n", json); free(json);
                }
                col_batch_free(&batch);
            }
        } else if (strcmp(argv[2], "scan") == 0 && argc >= 7) {
            ColBatch batch;
            if (col_materialize(g_db, argv[3], NULL, 0, &batch) == 0) {
                int col_idx = atoi(argv[4]);
                int *matches = malloc(batch.nrows * sizeof(int));
                if (matches) {
                    int n = col_scan(&batch, col_idx, argv[5], argv[6], NULL, matches);
                    printf("{\"matches\":%d}\n", n);
                    free(matches);
                }
                col_batch_free(&batch);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === hybrid (Upgrade VIII: Hybrid Reduction) === */
    if (strcmp(argv[1], "hybrid") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms hybrid <reduce|profile|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "reduce") == 0 && argc >= 5) {
            char *json = NULL;
            if (hybrid_reduce(g_db, argv[3], atoi(argv[4]), NULL, 0, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "profile") == 0 && argc >= 5) {
            char *json = NULL;
            if (hybrid_profile(g_db, argv[3], atoi(argv[4]), &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "stats") == 0) {
            char *json = NULL;
            if (hybrid_stats(g_db, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === prov (Upgrade IX: Provenance + Merkle Proofs) === */
    if (strcmp(argv[1], "prov") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms prov <build|manifest|verify|chain|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "build") == 0 && argc >= 4) {
            uint64_t root = 0;
            prov_build_tree(g_db, argv[3], &root);
            printf("{\"merkle_root\":\"%llx\"}\n", (unsigned long long)root);
        } else if (strcmp(argv[2], "manifest") == 0 && argc >= 4) {
            int64_t mid = 0;
            prov_manifest(g_db, argv[3], &mid);
            printf("{\"manifest_id\":%lld}\n", (long long)mid);
        } else if (strcmp(argv[2], "verify") == 0 && argc >= 4) {
            int valid = prov_verify_manifest(g_db, atoll(argv[3]));
            printf("{\"valid\":%s}\n", valid ? "true" : "false");
        } else if (strcmp(argv[2], "chain") == 0 && argc >= 4) {
            char *json = NULL;
            if (prov_chain(g_db, argv[3], &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (strcmp(argv[2], "stats") == 0) {
            char *json = NULL;
            if (prov_stats(g_db, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === transfer (Upgrade X: Cross-Family Transfer) === */
    if (strcmp(argv[1], "transfer") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms transfer <auto|register|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (strcmp(argv[2], "auto") == 0 && argc >= 4) {
            int rc = transfer_auto(g_db, argv[3]);
            printf("{\"transferred\":%s}\n", rc == 0 ? "true" : "false");
        } else if (strcmp(argv[2], "register") == 0 && argc >= 5) {
            int64_t tid = 0;
            transfer_register_template(g_db, argv[3], argv[4], &tid);
            printf("{\"template_id\":%lld}\n", (long long)tid);
        } else if (strcmp(argv[2], "accelerate") == 0 && argc >= 4) {
            int accel = transfer_accelerate(g_db, argv[3]);
            printf("{\"accelerated\":%s}\n", accel ? "true" : "false");
        } else if (strcmp(argv[2], "stats") == 0) {
            char *json = NULL;
            if (transfer_stats(g_db, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    print_usage();
    sqlite3_close(g_db);
    return 1;
}
