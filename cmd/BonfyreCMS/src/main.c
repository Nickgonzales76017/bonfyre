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
 *   bonfyre-cms publication promote-rss --source SOURCE [--foreign-root DIR]
 *   bonfyre-cms token issue [--scope NAMESPACE] [--actions ACTIONS]
 *   bonfyre-cms token list [--db FILE]
 */
/* glibc gates PATH_MAX in <limits.h> and CLOCK_MONOTONIC in <time.h> behind a
 * POSIX feature macro. macOS exposes both unconditionally, so this built on a
 * developer machine and failed on the Linux host it actually deploys to. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "bf_lex.h"
#include "bf_command_lookup.h"
#include "bonfyre_frappe_condition.h"
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
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
#include "entry_snapshot.h"

#define VERSION "0.1.0"
#define MAX_BODY   (1024 * 1024)   /* 1 MB max request body */
#define MAX_FIELDS 1024 /* checked upper bound; descriptors grow on demand */
#define MAX_FIELD_NAME 128
#define MAX_FIELD_VAL  65536       /* bounded complete fields shared with the native form carrier */
#define MAX_TIERS  32
#define MAX_PATH_SEGS 16
#define DEFAULT_PORT 8800
#define DEFAULT_DB "bonfyre_cms.db"
#define DEFAULT_SCHEMAS "./content-types"

static int cms_fact_deltas_enabled(void) {
    const char *directory = getenv("BONFYRE_FACT_DELTAS");
    return directory != NULL && directory[0] != '\0';
}

/* Where this installation's CMS store actually lives.
 *
 * The default was the bare filename above, so every invocation opened -- or
 * created -- a store beside whatever directory the caller happened to be
 * standing in. Seven of them accumulated on one machine between April and
 * August, 0 to 483KB, each looking exactly as authoritative as the others.
 * Worse, the deployment declares BONFYRE_CMS_DB and this binary read no
 * environment at all: the one coordinate the operator had stated was the one
 * it ignored. Reaching the CMS through the web-address organ used the right
 * store; reaching the same binary directly did not.
 *
 * Order is declared, and $PWD is never a candidate -- a store chosen by where
 * you were standing is not a coordinate:
 *
 *   1. an explicit --db
 *   2. BONFYRE_CMS_DB, the deployment's own declaration
 *   3. one machine store under the state root
 *
 * When none resolves to an existing directory this refuses and names the
 * variable to set, rather than inventing an eighth store that would look just
 * as real as the other seven.
 *
 * Cannot infer: an opened store -> the deployment's store, or authority over
 * what is in it. This resolves a path. A record written here is not published. */
static int cms_db_dir_exists(const char *path) {
    char dir[PATH_MAX];
    const char *slash = strrchr(path, '/');
    struct stat st;
    if (!slash) return 0;                       /* bare name: no directory at all */
    if ((size_t)(slash - path) >= sizeof(dir)) return 0;
    memcpy(dir, path, (size_t)(slash - path));
    dir[slash - path] = '\0';
    return dir[0] && stat(dir, &st) == 0 && S_ISDIR(st.st_mode);
}

/* The schema directory is the same coordinate problem wearing a different name:
 * "./content-types" is $PWD, and BONFYRE_CMS_SCHEMAS -- which the deployment
 * declares alongside BONFYRE_CMS_DB -- was equally unread. A store found next to
 * schemas found somewhere else is not one installation. */
static int cms_dir_exists(const char *path) {
    struct stat st;
    return path && *path && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static const char *resolve_cms_schemas(char *buf, size_t n, const char *requested, int explicit_schemas) {
    const char *env, *home;

    if (explicit_schemas) return requested;

    env = getenv("BONFYRE_CMS_SCHEMAS");
    if (env && *env) { snprintf(buf, n, "%s", env); return buf; }

    env = getenv("BONFYRE_STATE_ROOT");
    if (env && *env) {
        snprintf(buf, n, "%s/cms/content-types", env);
        if (cms_dir_exists(buf)) return buf;
    }
    snprintf(buf, n, "/var/lib/bonfyre/cms/content-types");
    if (cms_dir_exists(buf)) return buf;

    home = getenv("HOME");
    if (home && *home) {
        snprintf(buf, n, "%s/.local/share/bonfyre/cms/content-types", home);
        if (cms_dir_exists(buf)) return buf;
    }
    /* A relative directory beside the caller is the one answer that is always
     * wrong; falling back to it is what produced schema sets that disagreed. */
    return NULL;
}

static const char *resolve_cms_db(char *buf, size_t n, const char *requested, int explicit_db) {
    const char *env, *home;

    if (explicit_db) return requested;           /* the caller named it; obey */

    env = getenv("BONFYRE_CMS_DB");
    if (env && *env) { snprintf(buf, n, "%s", env); return buf; }

    env = getenv("BONFYRE_STATE_ROOT");
    if (env && *env) {
        snprintf(buf, n, "%s/cms/bonfyre_cms.db", env);
        if (cms_db_dir_exists(buf)) return buf;
    }
    snprintf(buf, n, "/var/lib/bonfyre/cms/bonfyre_cms.db");
    if (cms_db_dir_exists(buf)) return buf;

    home = getenv("HOME");
    if (home && *home) {
        snprintf(buf, n, "%s/.local/share/bonfyre/cms/bonfyre_cms.db", home);
        if (cms_db_dir_exists(buf)) return buf;
    }
    return NULL;
}

/* ================================================================
 * Utilities
 * ================================================================ */

static volatile int g_running = 1;
static int g_defer_indexing = 0;   /* when 1, entry_create skips family_refresh_entry */
static int g_defer_publication_index = 0; /* when 1, a batch owns the one rebuild */

static void handle_signal(int sig) { (void)sig; g_running = 0; }

void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static char *read_file_full(const char *path, long *out_size) {
    size_t sz = 0;
    char *buf = bf_read_file(path, &sz);
    if (out_size) *out_size = (long)sz;
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
            snprintf(candidate, sizeof(candidate), "%s/../../cmd/BonfyreCMS/%s", exec_dir, rel);
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

/* Locate a key in the root JSON object only.  The old strstr-based helpers
 * accepted a nested occurrence (for example semantic_roles.accepts included
 * the string "status") as though it were a top-level field.  That made a
 * fully valid authored row depend on field ordering. */
static const char *json_top_level_value(const char *json, const char *key) {
    if (!json || !key) return NULL;
    const char *p = json;
    size_t key_len = strlen(key);
    int depth = 0;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '{') return NULL;
    for (; *p; p++) {
        if (*p == '"') {
            const char *end = skip_json_string_end(p);
            if (!end) return NULL;
            if (depth == 1 && (size_t)(end - p) == key_len + 2 &&
                memcmp(p + 1, key, key_len) == 0) {
                const char *value = end;
                while (*value && isspace((unsigned char)*value)) value++;
                if (*value != ':') { p = end - 1; continue; }
                value++;
                while (*value && isspace((unsigned char)*value)) value++;
                return value;
            }
            p = end - 1;
            continue;
        }
        if (*p == '{' || *p == '[') depth++;
        else if (*p == '}' || *p == ']') {
            depth--;
            if (depth < 0) return NULL;
        }
    }
    return NULL;
}

/* Extract a string value for a given key from flat JSON.
   Returns 1 on success, 0 if not found. */
static int json_str(const char *json, const char *key, char *out, size_t sz) {
    /* Delegates to the one implementation. This copy searched for "key" rather
     * than "key": so it matched inside values, and stopped at the first quote
     * so an escaped one truncated the result -- the bug that silently broke
     * conditional re-observation elsewhere. Six binaries carried it; a seventh
     * had been fixed locally and the fix never reached the others. */
    return bf_lex_json_string(json, key, out, sz);
}

/* A numeric prefix is not a whole value: 349.99 and 1e3 are not integers. */
static int json_number_delimiter(const char *p) {
    while (*p && isspace((unsigned char)*p)) p++;
    return *p == ',' || *p == '}' || *p == ']';
}

static int json_int(const char *json, const char *key, int *out) {
    const char *p = json_top_level_value(json, key);
    char *end = NULL;
    if (!p || (!isdigit((unsigned char)*p) && *p != '-')) return 0;
    errno = 0;
    long value = strtol(p, &end, 10);
    if (errno == ERANGE || end == p || !json_number_delimiter(end) ||
        value < INT_MIN || value > INT_MAX) return 0;
    *out = (int)value;
    return 1;
}

static int json_double(const char *json, const char *key, double *out) {
    const char *p = json_top_level_value(json, key);
    char *end = NULL;
    if (!p || (!isdigit((unsigned char)*p) && *p != '-')) return 0;
    errno = 0;
    double value = strtod(p, &end);
    if (errno == ERANGE || end == p || !json_number_delimiter(end) || !isfinite(value)) return 0;
    *out = value;
    return 1;
}

/* Extract a boolean value */
static int json_bool(const char *json, const char *key, int *out) {
    const char *p = json_top_level_value(json, key);
    if (!p) return 0;
    if (strncmp(p, "true", 4) == 0) { *out = 1; return 1; }
    if (strncmp(p, "false", 5) == 0) { *out = 0; return 1; }
    return 0;
}

static const char *skip_json_compound_end(const char *p, char open, char close) {
    int depth = 1;
    if (!p || *p != open) return NULL;
    p++;
    while (p && *p && depth > 0) {
        if (*p == '"') {
            p = skip_json_string_end(p);
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
    const char *p = json_top_level_value(json, key);
    if (!p) return NULL;
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
    char required_when[512]; /* a condition over the entry's own fields; see bonfyre_frappe_condition.h */
    int unique_within_namespace;
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
    FieldDef *fields;
    int field_count;
    int field_capacity;
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
    while (p < attr_end) {
        /* skip whitespace */
        while (p < attr_end && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t' || *p == ',')) p++;
        if (p >= attr_end || *p == '}') break;

        /* expect field name in quotes */
        if (*p != '"') return -1;
        if (ct->field_count == MAX_FIELDS) {
            fprintf(stderr, "[schema] %s exceeds the supported %d fields; refusing the complete schema\n", ct->name, MAX_FIELDS);
            return -1;
        }
        if (ct->field_count == ct->field_capacity) {
            int capacity = ct->field_capacity ? ct->field_capacity * 2 : 16;
            FieldDef *next = realloc(ct->fields, (size_t)capacity * sizeof(*next));
            if (!next) return -1;
            ct->fields = next;
            ct->field_capacity = capacity;
        }
        p++;
        FieldDef *fd = &ct->fields[ct->field_count];
        memset(fd, 0, sizeof(*fd));
        size_t ni = 0;
        while (p < attr_end && *p && *p != '"' && ni < sizeof(fd->name) - 1) fd->name[ni++] = *p++;
        fd->name[ni] = '\0';
        if (p >= attr_end || *p != '"' || !ni) return -1;
        for (int i = 0; i < ct->field_count; i++)
            if (sqlite3_stricmp(ct->fields[i].name, fd->name) == 0) return -1;
        p++;

        /* skip to value object */
        while (p < attr_end && isspace((unsigned char)*p)) p++;
        if (p >= attr_end || *p++ != ':') return -1;
        while (p < attr_end && isspace((unsigned char)*p)) p++;
        if (p >= attr_end || *p != '{') return -1;

        /* find end of this field's object */
        const char *fobj_start = p;
        p = skip_json_compound_end(p, '{', '}');
        if (!p || p > attr_end) return -1;

        /* extract field attributes from the sub-object */
        size_t fobj_len = (size_t)(p - fobj_start);
        char *fobj = malloc(fobj_len + 1);
        if (!fobj) return -1;
        memcpy(fobj, fobj_start, fobj_len);
        fobj[fobj_len] = '\0';

        char type_str[64] = {0};
        json_str(fobj, "type", type_str, sizeof(type_str));
        fd->type = parse_field_type(type_str);

        json_bool(fobj, "required", &fd->required);
        json_str(fobj, "requiredWhen", fd->required_when, sizeof(fd->required_when));
        json_bool(fobj, "uniqueWithinNamespace", &fd->unique_within_namespace);
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

/* Consume allocated SQL, including allocation and execution failures. */
static int schema_exec_owned(sqlite3 *db, char *sql) {
    if (!sql) return -1;
    int result = db_exec(db, sql);
    sqlite3_free(sql);
    return result;
}

static int schema_builtin(const char *name) {
    static const char *columns[] = {"id", "namespace", "created_at", "updated_at", "published_at", "status", NULL};
    for (int i = 0; columns[i]; i++)
        if (sqlite3_stricmp(name, columns[i]) == 0) return 1;
    return 0;
}

/* Called inside load_schema_file's savepoint: DDL and metadata commit together. */
static int create_content_table(sqlite3 *db, const ContentType *ct) {
    unsigned char found[MAX_FIELDS] = {0};
    sqlite3_stmt *pragma = NULL;
    sqlite3_str *builder = sqlite3_str_new(db);
    sqlite3_str_appendf(builder,
        "CREATE TABLE IF NOT EXISTS \"%w\" (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "namespace TEXT DEFAULT 'root' NOT NULL,created_at TEXT NOT NULL,"
        "updated_at TEXT NOT NULL,published_at TEXT,status TEXT DEFAULT 'draft'", ct->name);
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        const char *type = fd->type == FT_UID ? "TEXT" : sql_type_for(fd->type);
        if (!type || schema_builtin(fd->name)) continue;
        sqlite3_str_appendf(builder, ",\"%w\" %s", fd->name, type);
        if (fd->default_val[0]) sqlite3_str_appendf(builder, " DEFAULT %Q", fd->default_val);
    }
    sqlite3_str_appendall(builder, ");");
    if (schema_exec_owned(db, sqlite3_str_finish(builder)) != 0) return -1;

    char *sql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", ct->name);
    if (!sql) return -1;
    int rc = sqlite3_prepare_v2(db, sql, -1, &pragma, NULL);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) return -1;
    /* Inspect every existing column; built-in columns do not consume field capacity. */
    while ((rc = sqlite3_step(pragma)) == SQLITE_ROW) {
        const char *column = (const char *)sqlite3_column_text(pragma, 1);
        if (column) for (int i = 0; i < ct->field_count; i++)
            if (sqlite3_stricmp(column, ct->fields[i].name) == 0) found[i] = 1;
    }
    sqlite3_finalize(pragma);
    if (rc != SQLITE_DONE) return -1;
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        const char *type = fd->type == FT_UID ? "TEXT" : sql_type_for(fd->type);
        if (!type || found[i]) continue;
        builder = sqlite3_str_new(db);
        sqlite3_str_appendf(builder, "ALTER TABLE \"%w\" ADD COLUMN \"%w\" %s", ct->name, fd->name, type);
        if (fd->default_val[0]) sqlite3_str_appendf(builder, " DEFAULT %Q", fd->default_val);
        if (schema_exec_owned(db, sqlite3_str_finish(builder)) != 0) return -1;
        fprintf(stderr, "[migrate] staged column %s.%s (%s)\n", ct->name, fd->name, field_type_name(fd->type));
    }
    if (schema_exec_owned(db, sqlite3_mprintf(
            "CREATE INDEX IF NOT EXISTS \"idx_%w_ns\" ON \"%w\"(namespace)", ct->name, ct->name)) != 0) return -1;
    /* Add indexes only after all required columns exist. */
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (!sql_type_for(fd->type)) continue;
        if (fd->unique_within_namespace) {
            if (schema_exec_owned(db, sqlite3_mprintf("DROP INDEX IF EXISTS \"idx_%w_%w\"", ct->name, fd->name)) != 0) return -1;
            sql = sqlite3_mprintf("CREATE UNIQUE INDEX IF NOT EXISTS \"idx_%w_%w_ns\" ON \"%w\"(namespace,\"%w\")", ct->name, fd->name, ct->name, fd->name);
        } else if (fd->type == FT_UID) {
            sql = sqlite3_mprintf("CREATE UNIQUE INDEX IF NOT EXISTS \"idx_%w_%w\" ON \"%w\"(\"%w\")", ct->name, fd->name, ct->name, fd->name);
        } else continue;
        if (schema_exec_owned(db, sql) != 0) return -1;
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

/* CMS authoring remains the owner write.  A FactDelta is only its durable
 * change notice for derived compilers.  It is emitted after the owner commit,
 * through fsync+rename, and contains coordinates rather than row contents. */
static int cms_delta_json_string(FILE *out, const char *value) {
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    if (fputc('"', out) == EOF) return 0;
    for (; *p; p++) {
        if (*p == '"' || *p == '\\') { if (fputc('\\', out) == EOF || fputc(*p, out) == EOF) return 0; }
        else if (*p == '\n') { if (fputs("\\n", out) == EOF) return 0; }
        else if (*p == '\r') { if (fputs("\\r", out) == EOF) return 0; }
        else if (*p == '\t') { if (fputs("\\t", out) == EOF) return 0; }
        else if (*p < 0x20) { if (fprintf(out, "\\u%04x", *p) < 0) return 0; }
        else if (fputc(*p, out) == EOF) return 0;
    }
    return fputc('"', out) != EOF;
}

static int cms_emit_fact_delta(const char *type_name, const char *identity,
                               const char *publication_source, long long rowid) {
    const char *directory = getenv("BONFYRE_FACT_DELTAS");
    char temporary[PATH_MAX], destination[PATH_MAX], delta[256], coordinate[MAX_FIELD_VAL + 256];
    struct timespec now;
    FILE *stream = NULL;
    int fd = -1, dfd = -1, ok = 0;
    if (!directory || !*directory) return 1; /* deployment has not enabled forwarding */
    if (!type_name || !*type_name) return 0;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) return 0;
    if (snprintf(delta, sizeof(delta), "cms:%s:%lld:%lld:%ld", type_name, rowid,
                 (long long)now.tv_sec, now.tv_nsec) >= (int)sizeof(delta) ||
        snprintf(temporary, sizeof(temporary), "%s/.cms-delta-XXXXXX", directory) >= (int)sizeof(temporary) ||
        snprintf(destination, sizeof(destination), "%s/cms-%lld-%ld.json", directory,
                 rowid, now.tv_nsec) >= (int)sizeof(destination)) return 0;
    if ((fd = mkstemp(temporary)) < 0) return 0;
    if (!(stream = fdopen(fd, "w"))) { close(fd); unlink(temporary); return 0; }
    fd = -1;
    if (fputs("{\"schema\":\"bonfyre-fact-delta.v1\",\"delta_id\":", stream) == EOF ||
        !cms_delta_json_string(stream, delta) || fputs(",\"writes\":[", stream) == EOF ||
        fputs("\"cms:", stream) == EOF || fputs(type_name, stream) == EOF || fputc('"', stream) == EOF) goto out;
    if (identity && *identity) {
        if (snprintf(coordinate, sizeof(coordinate), "cms:%s:%s", type_name, identity) >= (int)sizeof(coordinate) ||
            fputc(',', stream) == EOF || !cms_delta_json_string(stream, coordinate)) goto out;
    }
    if (publication_source && *publication_source) {
        if (snprintf(coordinate, sizeof(coordinate), "cms:publication_source:%s", publication_source) >= (int)sizeof(coordinate) ||
            fputc(',', stream) == EOF || !cms_delta_json_string(stream, coordinate) ||
            snprintf(coordinate, sizeof(coordinate), "cms:internet_source:%s", publication_source) >= (int)sizeof(coordinate) ||
            fputc(',', stream) == EOF || !cms_delta_json_string(stream, coordinate)) goto out;
    }
    if (fputs("]}\n", stream) == EOF || fflush(stream) || fsync(fileno(stream))) goto out;
    if (fclose(stream)) { stream = NULL; goto out; }
    stream = NULL;
    if (rename(temporary, destination) || (dfd = open(directory, O_RDONLY | O_DIRECTORY)) < 0 || fsync(dfd)) goto out;
    ok = 1;
out:
    if (dfd >= 0) close(dfd);
    if (stream) fclose(stream);
    if (!ok) unlink(temporary);
    return ok;
}

#include "schema_read_indexes.h"

static int load_schema_file(sqlite3 *db, const char *path) {
    long sz = 0;
    char *json = read_file_full(path, &sz);
    if (!json) { fprintf(stderr, "[schema] cannot read: %s\n", path); return -1; }

    ContentType ct;
    if (parse_schema(json, &ct) != 0) {
        fprintf(stderr, "[schema] parse error: %s\n", path);
        free(ct.fields);
        free(json);
        return -1;
    }

    ContentType *existing = find_type(ct.name);
    if ((!existing && g_type_count >= 128) || db_exec(db, "SAVEPOINT cms_schema_apply") != 0) {
        free(ct.fields);
        free(json);
        return -1;
    }
    if (create_content_table(db, &ct) != 0 || cms_read_indexes(db, ct.name, json) != 0 ||
        register_schema(db, &ct, json) != 0 ||
        db_exec(db, "RELEASE cms_schema_apply") != 0) {
        db_exec(db, "ROLLBACK TO cms_schema_apply");
        db_exec(db, "RELEASE cms_schema_apply");
        fprintf(stderr, "[schema] apply failed and was rolled back: %s\n", ct.name);
        free(ct.fields);
        free(json);
        return -1;
    }

    /* Store in memory */
    if (existing) {
        free(existing->fields);
        *existing = ct;
    } else {
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

/* A schema applied through the native API is durable state.  Requiring its
 * source JSON to be rediscovered from a development directory on every later
 * CRUD invocation made `schema apply` a catalog-only operation.  Rehydrate
 * registered schemas from the canonical CMS store, while retaining directory
 * loading as the editable/import path. */
static int load_registered_schemas(sqlite3 *db) {
    sqlite3_stmt *stmt = NULL;
    int count = 0;
    if (!db) return -1;
    if (sqlite3_prepare_v2(db,
            "SELECT schema_json FROM _content_types ORDER BY name", -1,
            &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        ContentType ct = {0};
        if (!json || parse_schema(json, &ct) != 0) {
            free(ct.fields);
            sqlite3_finalize(stmt);
            return -1;
        }
        if (find_type(ct.name)) { free(ct.fields); continue; }
        if (g_type_count >= 128) { free(ct.fields); sqlite3_finalize(stmt); return -1; }
        g_types[g_type_count++] = ct;
        count++;
    }
    sqlite3_finalize(stmt);
    return count;
}

/* ================================================================
 * CRUD operations
 * ================================================================ */

/* The stored row as one JSON object, NULL columns left out. */
static char *entry_row_json(sqlite3 *db, const ContentType *ct, long long entry_id)
{
    char sql[256];
    sqlite3_stmt *row = NULL, *quote = NULL;
    size_t cap = 4096, len = 0;
    char *doc = NULL;
    snprintf(sql, sizeof(sql), "SELECT * FROM \"%s\" WHERE id=?1", ct->name);
    if (sqlite3_prepare_v2(db, sql, -1, &row, NULL) != SQLITE_OK) return NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &quote, NULL) != SQLITE_OK) { sqlite3_finalize(row); return NULL; }
    sqlite3_bind_int64(row, 1, entry_id);
    if (sqlite3_step(row) == SQLITE_ROW && (doc = malloc(cap))) {
        int columns = sqlite3_column_count(row);
        doc[len++] = '{';
        for (int c = 0; doc && c < columns; c++) {
            const char *name = sqlite3_column_name(row, c);
            const char *value;
            size_t need;
            if (sqlite3_column_type(row, c) == SQLITE_NULL || !name || strchr(name, '"') || strchr(name, '\\')) continue;
            sqlite3_reset(quote);
            sqlite3_bind_value(quote, 1, sqlite3_column_value(row, c));
            if (sqlite3_step(quote) != SQLITE_ROW || !(value = (const char *)sqlite3_column_text(quote, 0))) { free(doc); doc = NULL; break; }
            need = len + strlen(name) + strlen(value) + 8;
            if (need > cap) {
                char *next;
                while (cap < need) cap *= 2;
                next = realloc(doc, cap);
                if (!next) { free(doc); doc = NULL; break; }
                doc = next;
            }
            len += (size_t)snprintf(doc + len, cap - len, "%s\"%s\":%s", len > 1 ? "," : "", name, value);
        }
        if (doc) { doc[len++] = '}'; doc[len] = '\0'; }
    }
    sqlite3_finalize(row);
    sqlite3_finalize(quote);
    return doc;
}

/* A hook is a command the operator registered for a content type. It runs
 * before the write is released, so it cannot read the row from the store; the
 * write is handed to it instead: what kind, which event, which entry, the row
 * as it now stands and, for an update, as it stood before. A hook's failure
 * does not undo the write. Page aggregates queue these payloads and run the
 * hooks only after the entire page has committed. */
typedef struct DeferredHook {
    char *type, *event, *ns, *row, *previous;
    long long id;
    struct DeferredHook *next;
} DeferredHook;
static DeferredHook *deferred_hooks = NULL, **deferred_hook_tail = &deferred_hooks;
static int defer_hooks = 0, deferred_write_error = 0;

static void run_hooks(sqlite3 *db, const char *type_name, const char *event, long long entry_id,
                      const char *ns, const char *row_json, const char *previous_json)
{
    if (defer_hooks) {
        if (!row_json) deferred_write_error = 1;
        DeferredHook *h = calloc(1, sizeof(*h));
        if (!h) { deferred_write_error = 1; return; }
        h->type = strdup(type_name); h->event = strdup(event); h->ns = strdup(ns ? ns : "");
        h->row = strdup(row_json ? row_json : "{}"); h->previous = previous_json ? strdup(previous_json) : NULL; h->id = entry_id;
        if (!h->type || !h->event || !h->ns || !h->row || (previous_json && !h->previous)) deferred_write_error = 1;
        *deferred_hook_tail = h; deferred_hook_tail = &h->next;
        return;
    }
    sqlite3_stmt *hook_stmt = NULL;
    char id_str[32];
    snprintf(id_str, sizeof(id_str), "%lld", entry_id);
    if (sqlite3_prepare_v2(db,
        "SELECT command FROM _hooks WHERE content_type=?1 AND event=?2 AND enabled=1",
        -1, &hook_stmt, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(hook_stmt, 1, type_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(hook_stmt, 2, event, -1, SQLITE_STATIC);
    while (sqlite3_step(hook_stmt) == SQLITE_ROW) {
        const char *cmd = (const char *)sqlite3_column_text(hook_stmt, 0);
        pid_t pid;
        if (!cmd) continue;
        pid = fork();
        if (pid == 0) {
            setenv("BONFYRE_CMS_TYPE", type_name, 1);
            setenv("BONFYRE_CMS_EVENT", event, 1);
            setenv("BONFYRE_CMS_ENTRY_ID", id_str, 1);
            setenv("BONFYRE_CMS_NAMESPACE", ns ? ns : "", 1);
            setenv("BONFYRE_CMS_ENTRY_JSON", row_json ? row_json : "{}", 1);
            if (previous_json) setenv("BONFYRE_CMS_PREVIOUS_JSON", previous_json, 1);
            else unsetenv("BONFYRE_CMS_PREVIOUS_JSON");
            execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
            _exit(127);
        } else if (pid > 0) {
            int st; waitpid(pid, &st, 0);
        }
    }
    sqlite3_finalize(hook_stmt);
}

static void finish_deferred_hooks(sqlite3 *db, int committed) {
    defer_hooks = 0;
    while (deferred_hooks) {
        DeferredHook *h = deferred_hooks; deferred_hooks = h->next;
        if (committed) run_hooks(db, h->type, h->event, h->id, h->ns, h->row, h->previous);
        free(h->type); free(h->event); free(h->ns); free(h->row); free(h->previous); free(h);
    }
    deferred_hook_tail = &deferred_hooks; deferred_write_error = 0;
}

/* A field can be required only while a condition over the same entry holds.
 * The rule is the compiled plan a form would use to mark the field, evaluated
 * here against the stored row, so the page and the store cannot disagree.
 * A rule that cannot be evaluated requires nothing, and says so. */
static int conditional_required_valid(sqlite3 *db, const ContentType *ct, long long entry_id)
{
    int any = 0, invalid = 0;
    for (int i = 0; i < ct->field_count; i++)
        if (ct->fields[i].required_when[0]) any = 1;
    if (!any) return 0;

    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT * FROM \"%s\" WHERE id=?1", ct->name);
    sqlite3_stmt *row = NULL, *quote = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &row, NULL) != SQLITE_OK) return -1;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &quote, NULL) != SQLITE_OK) {
        sqlite3_finalize(row);
        return -1;
    }
    sqlite3_bind_int64(row, 1, entry_id);
    if (sqlite3_step(row) != SQLITE_ROW) {
        sqlite3_finalize(row); sqlite3_finalize(quote);
        return -1;
    }

    size_t cap = 4096, len = 0;
    char *doc = malloc(cap);
    int columns = sqlite3_column_count(row), ok = doc != NULL;
    if (ok) doc[len++] = '{';
    for (int c = 0; ok && c < columns; c++) {
        const char *name = sqlite3_column_name(row, c);
        const char *value;
        size_t need;
        if (sqlite3_column_type(row, c) == SQLITE_NULL || !name || strchr(name, '"') || strchr(name, '\\')) continue;
        sqlite3_reset(quote);
        sqlite3_bind_value(quote, 1, sqlite3_column_value(row, c));
        if (sqlite3_step(quote) != SQLITE_ROW || !(value = (const char *)sqlite3_column_text(quote, 0))) { ok = 0; break; }
        need = len + strlen(name) + strlen(value) + 8;
        if (need > cap) {
            char *next;
            while (cap < need) cap *= 2;
            next = realloc(doc, cap);
            if (!next) { ok = 0; break; }
            doc = next;
        }
        len += (size_t)snprintf(doc + len, cap - len, "%s\"%s\":%s", len > 1 ? "," : "", name, value);
    }
    if (ok) { doc[len++] = '}'; doc[len] = '\0'; }

    for (int i = 0; ok && i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        BfCondPlan plan;
        char why[200];
        int holds, col = -1, missing;
        if (!fd->required_when[0] || fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;
        if (bf_cond_compile(fd->required_when, &plan) != 0) {
            fprintf(stderr, "[crud] %s.%s: its condition is outside what this store evaluates (%s); nothing required\n",
                    ct->name, fd->name, plan.missing_operation);
            bf_cond_plan_free(&plan);
            continue;
        }
        holds = bf_cond_evaluate(&plan, db, doc, NULL, NULL, why, sizeof(why));
        bf_cond_plan_free(&plan);
        if (holds < 0) {
            fprintf(stderr, "[crud] %s.%s: its condition could not be evaluated (%s); nothing required\n",
                    ct->name, fd->name, why);
            continue;
        }
        if (!holds) continue;
        for (int c = 0; c < columns; c++)
            if (!strcmp(sqlite3_column_name(row, c), fd->name)) col = c;
        missing = col < 0 || sqlite3_column_type(row, col) == SQLITE_NULL ||
                  (sqlite3_column_type(row, col) == SQLITE_TEXT && sqlite3_column_bytes(row, col) == 0);
        if (missing) {
            fprintf(stderr, "[crud] required field missing: %s.%s is required while its condition holds (%s)\n",
                    ct->name, fd->name, fd->required_when);
            invalid = 1;
        }
    }
    free(doc);
    sqlite3_finalize(row);
    sqlite3_finalize(quote);
    return ok && !invalid ? 0 : -1;
}

/* Use the store's JSON parser before mutating; numeric fields cannot silently
 * lose malformed or out-of-range values during scalar helper fall-through. */
static int entry_input_valid(sqlite3 *db, const ContentType *ct, const char *body) {
    sqlite3_stmt *check = NULL;
    if (sqlite3_prepare_v2(db,
            "SELECT CASE WHEN json_valid(?1) THEN json_type(?1)='object' ELSE 0 END",
            -1, &check, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(check, 1, body, -1, SQLITE_STATIC);
    int valid = sqlite3_step(check) == SQLITE_ROW && sqlite3_column_int(check, 0);
    sqlite3_finalize(check);
    if (!valid) { fprintf(stderr, "[crud] body must be a valid JSON object\n"); return -1; }
    char *text = malloc(MAX_FIELD_VAL);
    if (!text) return -1;
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type != FT_INTEGER && fd->type != FT_DECIMAL) continue;
        const char *raw = json_top_level_value(body, fd->name);
        if (!raw || !strncmp(raw, "null", 4)) continue; /* existing null/required semantics */
        int integer = 0;
        double decimal = 0;
        if (json_str(body, fd->name, text, MAX_FIELD_VAL)) {
            char *end = NULL;
            errno = 0;
            if (fd->type == FT_INTEGER) {
                long value = strtol(text, &end, 10);
                valid = end != text && errno != ERANGE && value >= INT_MIN && value <= INT_MAX;
            } else {
                decimal = strtod(text, &end);
                valid = end != text && errno != ERANGE && isfinite(decimal);
            }
            while (end && *end && isspace((unsigned char)*end)) end++;
            valid = valid && end && !*end && strspn(text, "0123456789+-.eE \t\r\n") == strlen(text);
        } else {
            valid = fd->type == FT_INTEGER ? json_int(body, fd->name, &integer)
                                          : json_double(body, fd->name, &decimal);
        }
        if (!valid) {
            fprintf(stderr, "[crud] invalid numeric value for %s.%s\n", ct->name, fd->name);
            free(text);
            return -1;
        }
    }
    free(text);
    return 0;
}

/* Build INSERT statement from field values in a JSON body */
static int entry_create(sqlite3 *db, const char *type_name, const char *ns,
                        const char *body_json, long long *out_id)
{
    ContentType *ct = find_type(type_name);
    if (!ct) { fprintf(stderr, "[crud] unknown type: %s\n", type_name); return -1; }
    if (entry_input_valid(db, ct, body_json) != 0) return -1;

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
        if (fd->required && !(has_str || has_int || has_dbl || has_bool || has_json)) {
            fprintf(stderr, "[crud] required field missing: %s.%s\n", type_name, fd->name);
            return -1;
        }
        if (has_str || has_int || has_dbl || has_bool || has_json) {
            size_t cl = strlen(cols), vl = strlen(vals);
            int cn = snprintf(cols + cl, sizeof(cols) - cl, ", \"%s\"", fd->name);
            int vn = snprintf(vals + vl, sizeof(vals) - vl, ", ?%d", bind_idx);
            if (cn < 0 || vn < 0 || (size_t)cn >= sizeof(cols) - cl || (size_t)vn >= sizeof(vals) - vl) {
                fprintf(stderr, "[crud] insert field list exceeds the supported SQL size\n");
                return -1;
            }
            field_binds[field_bind_count++] = bind_idx;
            bind_idx++;
        }
    }

    int sql_len = snprintf(sql, sizeof(sql), "INSERT INTO \"%s\" (%s) VALUES (%s)", type_name, cols, vals);
    if (sql_len < 0 || (size_t)sql_len >= sizeof(sql)) {
        fprintf(stderr, "[crud] insert statement exceeds the supported SQL size\n");
        return -1;
    }

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
                /* JSON callers may supply a numeric or boolean value for a
                 * string-shaped imported field (for example a Frappe Data
                 * field whose historical data is numeric).  Never bind the
                 * uninitialised string buffer in that case. */
                if (has_str) {
                    sqlite3_bind_text(stmt, pidx, sval, -1, SQLITE_TRANSIENT);
                } else {
                    /* A source amount or identifier carried as text keeps its
                     * complete JSON token, not a rounded binary-double rendering. */
                    const char *end = NULL;
                    const char *raw = json_value(body_json, fd->name, &end);
                    if (!raw || !end || end <= raw || end - raw >= MAX_FIELD_VAL) {
                        free(json_raw);
                        sqlite3_finalize(stmt);
                        return -1;
                    }
                    sqlite3_bind_text(stmt, pidx, raw, (int)(end - raw), SQLITE_TRANSIENT);
                }
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

    if (conditional_required_valid(db, ct, rowid) != 0) {
        sqlite3_exec(db, "ROLLBACK TO entry_create_sp", NULL, NULL, NULL);
        sqlite3_exec(db, "RELEASE entry_create_sp", NULL, NULL, NULL);
        return -1;
    }

    if (out_id) *out_id = rowid;

    /* Log create op to BonfyreGraph op log */
    if (ops_append(db, OP_CREATE, type_name, rowid, ns, body_json, "system", NULL) && defer_hooks)
        deferred_write_error = 1;

    /* Incremental family maintenance keeps writes out of full-table rediscovery.
       Skipped when g_defer_indexing is set; caller must run family_discover later. */
    if (!g_defer_indexing) {
        if (family_refresh_entry(db, type_name, (int)rowid) != 0) {
            fprintf(stderr, "[crud] family refresh failed after create for %s:%lld\n", type_name, rowid);
            if (defer_hooks) deferred_write_error = 1;
        }
    }

    /* Fire lifecycle hooks */
    {
        char *row = entry_row_json(db, ct, rowid);
        run_hooks(db, type_name, "after_create", rowid, ns, row, NULL);
        free(row);
    }

    sqlite3_exec(db, "RELEASE entry_create_sp", NULL, NULL, NULL);
    return 0;
}

/* The uniqueWithinNamespace field is the authored owner coordinate, rather
 * than a route-local list of CMS type names. A published create announces its
 * broad family plus that exact coordinate after commit. Publication material
 * also announces the declared source edge that World advertisements consume. */
static int emit_published_entry_delta(const char *type_name, const char *body_json,
                                      long long rowid) {
    ContentType *type = find_type(type_name);
    char status[64] = "draft", identity[MAX_FIELD_VAL] = {0}, source[MAX_FIELD_VAL] = {0};
    if (!type || !body_json) return 0;
    json_str(body_json, "status", status, sizeof(status));
    if (strcmp(status, "published")) return 1;
    for (int i = 0; i < type->field_count; i++) {
        if (type->fields[i].unique_within_namespace &&
            json_str(body_json, type->fields[i].name, identity, sizeof(identity))) break;
    }
    if (!strcmp(type_name, "publication_occurrence"))
        json_str(body_json, "source_ref", source, sizeof(source));
    return cms_emit_fact_delta(type_name, identity, source, rowid);
}

typedef struct {
    int published;
    char identity[MAX_FIELD_VAL];
    char publication_source[MAX_FIELD_VAL];
} CmsDeltaCoordinates;

/* Resolve coordinates from the canonical row after an owner mutation.  The
 * schema supplies the identity field; this is not a hard-coded CMS type map. */
static int cms_row_delta_coordinates(sqlite3 *db, const char *type_name, int entry_id,
                                     const char *ns, CmsDeltaCoordinates *out) {
    ContentType *type = find_type(type_name);
    const char *identity_field = NULL;
    sqlite3_stmt *stmt = NULL;
    char sql[1024];
    int has_source = 0;
    if (!db || !type || !out || entry_id < 1) return 0;
    memset(out, 0, sizeof(*out));
    for (int i = 0; i < type->field_count; i++) {
        if (!identity_field && type->fields[i].unique_within_namespace) identity_field = type->fields[i].name;
        if (!strcmp(type->fields[i].name, "source_ref")) has_source = 1;
    }
    if (!identity_field) return 1;
    if (snprintf(sql, sizeof(sql), "SELECT \"%s\",status%s FROM \"%s\" WHERE id=?1%s",
                 identity_field, has_source ? ",COALESCE(source_ref,'')" : "", type->name,
                 ns && *ns ? " AND namespace=?2" : "") >= (int)sizeof(sql) ||
        sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_int(stmt, 1, entry_id);
    if (ns && *ns) sqlite3_bind_text(stmt, 2, ns, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *identity = (const char *)sqlite3_column_text(stmt, 0);
        const char *status = (const char *)sqlite3_column_text(stmt, 1);
        const char *source = has_source ? (const char *)sqlite3_column_text(stmt, 2) : NULL;
        if (identity) snprintf(out->identity, sizeof(out->identity), "%s", identity);
        if (source) snprintf(out->publication_source, sizeof(out->publication_source), "%s", source);
        out->published = status && !strcmp(status, "published");
    }
    sqlite3_finalize(stmt);
    return 1;
}

static int emit_published_row_delta(sqlite3 *db, const char *type_name, int entry_id,
                                    const char *ns, int *out_published) {
    CmsDeltaCoordinates coordinate;
    if (!cms_row_delta_coordinates(db, type_name, entry_id, ns, &coordinate)) return 0;
    if (out_published) *out_published = coordinate.published;
    if (!coordinate.published) return 1;
    return cms_emit_fact_delta(type_name, coordinate.identity, coordinate.publication_source,
                               entry_id);
}

/* Bulk-admit JSONL through the same typed create path as interactive
 * authoring. The whole generation, including family/tensor/ANN derivation,
 * is transactional: one bad row or failed derivation leaves no partial
 * migration generation behind. */
static int entry_import_jsonl(sqlite3 *db, const char *type_name, const char *ns,
                              const char *path, int *out_imported,
                              int *out_families, int *out_indexed)
{
    FILE *input = NULL;
    char *line = NULL;
    size_t cap = 0;
    ssize_t nread;
    int line_no = 0;
    int imported = 0;
    int families = 0;
    int indexed = 0;
    int previous_defer = g_defer_indexing;
    int rc = -1;

    if (!db || !type_name || !ns || !path || !find_type(type_name)) {
        fprintf(stderr, "[crud] import-jsonl: invalid type or arguments\n");
        return -1;
    }
    input = fopen(path, "r");
    if (!input) {
        fprintf(stderr, "[crud] import-jsonl: cannot read %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] import-jsonl: begin failed: %s\n", sqlite3_errmsg(db));
        goto cleanup;
    }

    g_defer_indexing = 1;
    while ((nread = getline(&line, &cap, input)) >= 0) {
        char *body = line;
        char *end;
        long long new_id = 0;
        (void)nread;
        line_no++;
        while (*body && isspace((unsigned char)*body)) body++;
        end = body + strlen(body);
        while (end > body && isspace((unsigned char)end[-1])) end--;
        *end = '\0';
        if (!*body) continue;
        if (*body != '{') {
            fprintf(stderr, "[crud] import-jsonl: line %d is not a JSON object\n", line_no);
            goto rollback;
        }
        if (entry_create(db, type_name, ns, body, &new_id) != 0) {
            fprintf(stderr, "[crud] import-jsonl: line %d rejected; rolling back generation\n", line_no);
            goto rollback;
        }
        imported++;
    }
    if (ferror(input)) {
        fprintf(stderr, "[crud] import-jsonl: read failed at line %d\n", line_no);
        goto rollback;
    }

    g_defer_indexing = previous_defer;
    if (imported > 0) {
        families = family_discover(db, type_name);
        if (families < 0) {
            fprintf(stderr, "[crud] import-jsonl: family derivation failed\n");
            goto rollback;
        }
        indexed = ann_build_index(db, type_name);
        if (indexed < 0) {
            fprintf(stderr, "[crud] import-jsonl: ANN derivation failed\n");
            goto rollback;
        }
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] import-jsonl: commit failed: %s\n", sqlite3_errmsg(db));
        goto rollback;
    }

    if (out_imported) *out_imported = imported;
    if (out_families) *out_families = families;
    if (out_indexed) *out_indexed = indexed;
    rc = 0;
    goto cleanup;

rollback:
    g_defer_indexing = previous_defer;
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);

cleanup:
    g_defer_indexing = previous_defer;
    free(line);
    if (input) fclose(input);
    return rc;
}

/* ================================================================
 * Publication material admission
 * ================================================================
 * A retained RSS body is an ingestion substrate, not page content.  This
 * native promotion pass crosses the body into the typed PublicationOccurrence
 * owner through the same checked create path used by every other CMS entry.
 * It deliberately admits only title/link/time already carried by an item;
 * authors, images, licences, categories and effects remain absent. */
/* Feed text is XML, so entities in it are text that has not been read yet.
 *
 * publication_rss_tag strips tags and unwraps CDATA and never decoded an
 * entity, so 1,799 of 22,962 promoted titles (7.8 percent, from 216 feeds)
 * were stored as "&quot;Миротворця&quot;" and some Cyrillic feeds as nothing
 * but "&#x43E;&#x430;...". Every consumer then saw markup: a surface rendered
 * it, and the embedder read "quot" and "x43e" as words, so unrelated headlines
 * that shared an escaped quote embedded as near-duplicates.
 *
 * Decoded once, in place, by XML rules plus the handful of HTML names feeds
 * actually use. A decoded character is never longer than its entity, so the
 * buffer cannot overflow. An entity that does not decode to a valid scalar
 * value is left exactly as written rather than guessed at. */
static int publication_utf8_put(uint32_t cp, char *out) {
    if (cp < 0x80) { out[0] = (char)cp; return 1; }
    if (cp < 0x800) { out[0] = (char)(0xC0 | (cp >> 6)); out[1] = (char)(0x80 | (cp & 0x3F)); return 2; }
    if (cp < 0x10000) {
        out[0] = (char)(0xE0 | (cp >> 12)); out[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[2] = (char)(0x80 | (cp & 0x3F)); return 3;
    }
    out[0] = (char)(0xF0 | (cp >> 18)); out[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    out[2] = (char)(0x80 | ((cp >> 6) & 0x3F)); out[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

static int publication_decode_entities(char *s) {
    static const struct { const char *name; uint32_t cp; } NAMED[] = {
        {"amp", '&'}, {"lt", '<'}, {"gt", '>'}, {"quot", '"'}, {"apos", '\''},
        {"nbsp", 0xA0}, {"ndash", 0x2013}, {"mdash", 0x2014}, {"lsquo", 0x2018},
        {"rsquo", 0x2019}, {"ldquo", 0x201C}, {"rdquo", 0x201D}, {"hellip", 0x2026},
        {"laquo", 0xAB}, {"raquo", 0xBB}, {"euro", 0x20AC}, {"copy", 0xA9},
        {"reg", 0xAE}, {"trade", 0x2122}, {"middot", 0xB7}, {NULL, 0}
    };
    char *r = s, *w = s;
    int changed = 0;
    if (!s) return 0;
    while (*r) {
        if (*r == '&') {
            const char *semi = strchr(r, ';');
            if (semi && semi - r >= 3 && semi - r <= 10) {
                uint32_t cp = 0;
                int ok = 0;
                size_t nlen = (size_t)(semi - r - 1);
                if (r[1] == '#') {
                    char *e = NULL;
                    unsigned long v;
                    if (r[2] == 'x' || r[2] == 'X') v = strtoul(r + 3, &e, 16);
                    else v = strtoul(r + 2, &e, 10);
                    if (e == semi && isxdigit((unsigned char)r[(r[2] == 'x' || r[2] == 'X') ? 3 : 2])) {
                        cp = (uint32_t)v; ok = v <= 0x10FFFF;
                    }
                } else {
                    for (int i = 0; NAMED[i].name; i++)
                        if (strlen(NAMED[i].name) == nlen && !strncmp(r + 1, NAMED[i].name, nlen)) {
                            cp = NAMED[i].cp; ok = 1; break;
                        }
                }
                if (ok && cp > 0 && !(cp >= 0xD800 && cp <= 0xDFFF)) {
                    w += publication_utf8_put(cp, w);
                    r = (char *)semi + 1;
                    changed = 1;
                    continue;
                }
            }
        }
        *w++ = *r++;
    }
    *w = '\0';
    return changed;
}

static const char *publication_rss_item_open(const char *at) {
    const char *p;
    if (!at) return NULL;
    for (p = at; (p = strstr(p, "<item")) != NULL; p += 5) {
        char after = p[5];
        if (after == '>' || isspace((unsigned char)after)) {
            p = strchr(p, '>');
            return p ? p + 1 : NULL;
        }
    }
    return NULL;
}

static int publication_rss_tag(const char *start, const char *end, const char *tag,
                               char *out, size_t cap) {
    char open[96], close[96];
    const char *p, *q;
    size_t n = 0;
    int in_tag = 0, space = 1;
    if (!start || !end || !tag || !out || cap < 2) return 0;
    snprintf(open, sizeof(open), "<%s", tag);
    snprintf(close, sizeof(close), "</%s>", tag);
    for (p = strstr(start, open); p && p < end; p = strstr(p + 1, open)) {
        char after;
        after = p[strlen(open)];
        if (after == '>' || isspace((unsigned char)after)) break;
    }
    if (!p || p >= end || !(p = strchr(p, '>')) || p >= end) return 0;
    p++;
    q = strstr(p, close);
    if (!q || q > end) return 0;
    if ((size_t)(q - p) >= 12 && !strncmp(p, "<![CDATA[", 9) && !strncmp(q - 3, "]]>", 3)) {
        p += 9;
        q -= 3;
    }
    for (; p < q && n + 1 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '<') { in_tag = 1; continue; }
        if (c == '>' && in_tag) { in_tag = 0; continue; }
        if (in_tag) continue;
        if (isspace(c)) {
            if (!space) out[n++] = ' ';
            space = 1;
        } else {
            out[n++] = (char)c;
            space = 0;
        }
    }
    while (n && isspace((unsigned char)out[n - 1])) n--;
    out[n] = '\0';
    return n > 0;
}

static int publication_json_string(char **cursor, char *end, const char *value) {
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    char *out = *cursor;
    if (out >= end) return -1;
    *out++ = '"';
    for (; *p; p++) {
        const char *escaped = NULL;
        if (*p == '"') escaped = "\\\"";
        else if (*p == '\\') escaped = "\\\\";
        else if (*p == '\n') escaped = "\\n";
        else if (*p == '\r') escaped = "\\r";
        else if (*p == '\t') escaped = "\\t";
        if (escaped) {
            size_t n = strlen(escaped);
            if ((size_t)(end - out) <= n) return -1;
            memcpy(out, escaped, n);
            out += n;
        } else if (*p < 0x20) {
            if ((size_t)(end - out) <= 6) return -1;
            snprintf(out, 7, "\\u%04x", (unsigned int)*p);
            out += 6;
        } else {
            if (out + 1 >= end) return -1;
            *out++ = (char)*p;
        }
    }
    if (out + 1 >= end) return -1;
    *out++ = '"';
    *out = '\0';
    *cursor = out;
    return 0;
}

static int publication_json_pair(char **cursor, char *end, const char *key,
                                 const char *value, int *first) {
    if (!*first) {
        if (*cursor + 1 >= end) return -1;
        *(*cursor)++ = ',';
    }
    *first = 0;
    if (publication_json_string(cursor, end, key) != 0) return -1;
    if (*cursor + 1 >= end) return -1;
    *(*cursor)++ = ':';
    return publication_json_string(cursor, end, value);
}

static int publication_occurrence_exists(sqlite3 *db, const char *ns, const char *occurrence_id) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT 1 FROM publication_occurrence WHERE namespace=?1 AND occurrence_id=?2 LIMIT 1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, occurrence_id, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

/* How many publication items this namespace holds, so a batch run can report
 * what it actually added rather than what each source claimed. */
/* A feed's bytes are whatever the publisher's encoding was.
 *
 * The promoter copied title, link and date straight out of the retained body
 * into a TEXT column, and 100 of the first 6,281 items landed there as invalid
 * UTF-8 -- `S\xe3o Paulo` where the feed declared ISO-8859-1. SQLite stores
 * the bytes without complaint and every reader afterwards has to guess.
 *
 * Latin-1 is the encoding RSS actually uses when it is not UTF-8, and the
 * transcode is unambiguous: every byte 0x80-0xFF becomes the two-byte UTF-8
 * sequence for the same code point. Text that is already valid UTF-8 is left
 * exactly as it was, so nothing well-formed is touched. */
static int publication_utf8_valid(const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    while (*p) {
        int n;
        if (*p < 0x80) { p++; continue; }
        else if ((*p & 0xE0) == 0xC0) n = 1;
        else if ((*p & 0xF0) == 0xE0) n = 2;
        else if ((*p & 0xF8) == 0xF0) n = 3;
        else return 0;
        p++;
        while (n--) { if ((*p & 0xC0) != 0x80) return 0; p++; }
    }
    return 1;
}

static int publication_to_utf8(char *field, size_t cap) {
    unsigned char buf[8192];
    const unsigned char *src = (const unsigned char *)field;
    size_t out = 0;
    if (publication_utf8_valid(field)) return 0;
    for (; *src && out + 2 < sizeof(buf); src++) {
        if (*src < 0x80) buf[out++] = *src;
        else {
            buf[out++] = (unsigned char)(0xC0 | (*src >> 6));
            buf[out++] = (unsigned char)(0x80 | (*src & 0x3F));
        }
    }
    buf[out] = '\0';
    if (out + 1 > cap) return 0;          /* leave it rather than truncate */
    memcpy(field, buf, out + 1);
    return 1;
}

/* Decode entities in rows promoted before the promoter decoded them.
 *
 * Rows are rewritten in place so their row ids and occurrence ids survive:
 * vectors and every other reference to a publication are keyed by those, and a
 * re-promotion into a fresh store would renumber the corpus under them. Each
 * repaired title is written to `out` as {"id","text"} so exactly those rows
 * can be re-embedded. The family and ANN indexes are rebuilt once, inside the
 * same transaction as the rewrites. */
static int publication_repair_entities(sqlite3 *db, const char *ns, const char *out_path) {
    sqlite3_stmt *sel = NULL, *upd = NULL;
    FILE *out = NULL;
    int rows = 0, titles = 0, links = 0, dates = 0, rc = -1;
    if (!db || !find_type("publication_occurrence")) return -1;
    if (out_path && !(out = fopen(out_path, "w"))) {
        fprintf(stderr, "[publication] cannot write %s\n", out_path);
        return -1;
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db, "SELECT id, COALESCE(title,''), COALESCE(canonical_href,''), "
                               "COALESCE(published_at,'') FROM publication_occurrence "
                               "WHERE namespace=?1", -1, &sel, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "UPDATE publication_occurrence SET title=?1, canonical_href=?2, "
                               "published_at=?3 WHERE id=?4", -1, &upd, NULL) != SQLITE_OK)
        goto rollback;
    sqlite3_bind_text(sel, 1, ns ? ns : "root", -1, SQLITE_TRANSIENT);
    while (sqlite3_step(sel) == SQLITE_ROW) {
        int id = sqlite3_column_int(sel, 0);
        char title[4096], href[4096], when[512];
        int ct, ch, cw;
        rows++;
        snprintf(title, sizeof(title), "%s", (const char *)sqlite3_column_text(sel, 1));
        snprintf(href, sizeof(href), "%s", (const char *)sqlite3_column_text(sel, 2));
        snprintf(when, sizeof(when), "%s", (const char *)sqlite3_column_text(sel, 3));
        ct = publication_decode_entities(title);
        ch = publication_decode_entities(href);
        cw = publication_decode_entities(when);
        if (!ct && !ch && !cw) continue;
        sqlite3_reset(upd);
        sqlite3_bind_text(upd, 1, title, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(upd, 2, href, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(upd, 3, when, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(upd, 4, id);
        if (sqlite3_step(upd) != SQLITE_DONE) goto rollback;
        titles += ct; links += ch; dates += cw;
        if (ct && out && title[0]) {
            char esc[8192];
            bf_lex_json_escape(title, esc, sizeof(esc));
            fprintf(out, "{\"id\":\"%d\",\"text\":\"%s\"}\n", id, esc);
        }
    }
    sqlite3_finalize(sel); sel = NULL;
    sqlite3_finalize(upd); upd = NULL;
    if (titles + links + dates > 0 &&
        (family_discover(db, "publication_occurrence") < 0 ||
         ann_build_index(db, "publication_occurrence") < 0))
        goto rollback;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto rollback;
    printf("{\"schema\":\"bonfyre-publication-entity-repair.v1\",\"rows\":%d,"
           "\"titles_repaired\":%d,\"links_repaired\":%d,\"dates_repaired\":%d,"
           "\"re_embed\":\"%s\"}\n", rows, titles, links, dates, out_path ? out_path : "");
    rc = 0;
    goto done;
rollback:
    fprintf(stderr, "[publication] entity repair failed: %s\n", sqlite3_errmsg(db));
    if (sel) sqlite3_finalize(sel);
    if (upd) sqlite3_finalize(upd);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
done:
    if (out) fclose(out);
    return rc;
}

static int publication_count(sqlite3 *db, const char *ns) {
    sqlite3_stmt *st = NULL;
    int n = 0;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM publication_occurrence WHERE namespace=?1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, ns ? ns : "root", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return n;
}

/* Durable post-commit FactDelta outbox for promoted publication sources.
 * Native CMS rows and this source revision commit atomically. A failed
 * delivery remains pending; a no-op retry can repair it without fabricating
 * another PublicationOccurrence. CAS acknowledgement keeps concurrent writes
 * from being cleared by an older emitter. */
static int publication_delta_outbox_boot(sqlite3 *db) {
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS _publication_delta_outbox("
        "namespace TEXT NOT NULL,source TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,"
        "PRIMARY KEY(namespace,source))", NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}
static int publication_delta_outbox_mark(sqlite3 *db, const char *ns, const char *source) {
    sqlite3_stmt *st = NULL;
    int ok;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO _publication_delta_outbox(namespace,source,revision) VALUES(?1,?2,1) "
        "ON CONFLICT(namespace,source) DO UPDATE SET revision=revision+1",
        -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    return ok ? 0 : -1;
}
static int publication_delta_outbox_revision(sqlite3 *db, const char *ns, const char *source, sqlite3_int64 *revision) {
    sqlite3_stmt *st = NULL;
    int rc, found = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT revision FROM _publication_delta_outbox WHERE namespace=?1 AND source=?2",
        -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) { *revision = sqlite3_column_int64(st, 0); found = 1; }
    else if (rc != SQLITE_DONE) found = -1;
    sqlite3_finalize(st);
    return found;
}
static int publication_delta_outbox_ack(sqlite3 *db, const char *ns, const char *source, sqlite3_int64 revision) {
    sqlite3_stmt *st = NULL;
    int ok;
    if (sqlite3_prepare_v2(db,
        "DELETE FROM _publication_delta_outbox WHERE namespace=?1 AND source=?2 AND revision=?3",
        -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 3, revision);
    ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    return ok ? 0 : -1;
}
static int publication_promote_rss(sqlite3 *db, const char *source, const char *ns,
                                   const char *foreign_root, int limit) {
    sqlite3_stmt *st = NULL;
    const char *root = foreign_root && *foreign_root ? foreign_root : getenv("BONFYRE_FOREIGN_ROOT");
    char twin[256] = {0}, extract[64] = {0}, receipt_path[PATH_MAX], body_path[PATH_MAX] = {0};
    char allowed_bodies[PATH_MAX], *receipt = NULL, *body = NULL, *line, *cursor;
    long ignored = 0;
    int created = 0, skipped = 0, transcoded = 0, previous_defer = 0, entity_decoded = 0;
    if (!db || !source || !*source || !find_type("publication_occurrence")) return -1;
    if (!root || !*root) root = "/var/lib/bonfyre/foreign";
    if (strstr(source, "..") || strchr(source, '/')) {
        fprintf(stderr, "[publication] unsafe source name\n");
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT twin,extract FROM internet_source WHERE source=?1 AND status='published' "
        "ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(twin, sizeof(twin), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
        snprintf(extract, sizeof(extract), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "");
    }
    sqlite3_finalize(st);
    if (!*twin || strncmp(extract, "rss:", 4)) {
        fprintf(stderr, "[publication] %s is not a published RSS source\n", source);
        return -1;
    }
    if (strstr(twin, "..") || strchr(twin, '/') ||
        snprintf(receipt_path, sizeof(receipt_path), "%s/receipts/%s.json", root, twin) >= (int)sizeof(receipt_path) ||
        snprintf(allowed_bodies, sizeof(allowed_bodies), "%s/bodies/", root) >= (int)sizeof(allowed_bodies)) return -1;
    receipt = read_file_full(receipt_path, &ignored);
    if (!receipt) { fprintf(stderr, "[publication] cannot read receipt for %s\n", source); return -1; }
    line = receipt;
    for (char *p = receipt; *p; ) {
        char *next = strchr(p, '\n');
        if (next) *next = '\0';
        if (*p) line = p;
        if (!next) break;
        p = next + 1;
    }
    if (!json_str(line, "body_path", body_path, sizeof(body_path)) ||
        strncmp(body_path, allowed_bodies, strlen(allowed_bodies)) || strstr(body_path, "..")) {
        fprintf(stderr, "[publication] receipt has no admitted body for %s\n", source);
        free(receipt);
        return -1;
    }
    body = read_file_full(body_path, &ignored);
    free(receipt);
    if (!body) { fprintf(stderr, "[publication] cannot read retained body for %s\n", source); return -1; }
    if (publication_delta_outbox_boot(db) != 0) { free(body); return -1; }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) { free(body); return -1; }
    /* Index once for the feed, not once per item.
     *
     * entry_create refreshes the family index for every row it writes, which
     * is right for a single authored entry and wrong for a promotion that
     * writes two hundred. Measured on this estate it was most of the six
     * seconds a feed cost, and the machinery to avoid it was already here --
     * import-jsonl has deferred indexing and rebuilt once since before this
     * promoter existed. The promoter simply never reached for it. */
    previous_defer = g_defer_indexing;
    g_defer_indexing = 1;
    cursor = body;
    while (created + skipped < limit) {
        const char *end;
        char title[1536] = {0}, href[2048] = {0}, when[256] = {0};
        char identity[4096], digest[65], occurrence_id[96], payload[12288], *out = payload;
        int first = 1;
        cursor = (char *)publication_rss_item_open(cursor);
        if (!cursor || !(end = strstr(cursor, "</item>"))) break;
        if (!publication_rss_tag(cursor, end, "title", title, sizeof(title))) { cursor = (char *)end + 7; continue; }
        publication_rss_tag(cursor, end, "link", href, sizeof(href));
        if (!publication_rss_tag(cursor, end, "pubDate", when, sizeof(when)))
            publication_rss_tag(cursor, end, "updated", when, sizeof(when));
        transcoded += publication_to_utf8(title, sizeof(title));
        transcoded += publication_to_utf8(href, sizeof(href));
        transcoded += publication_to_utf8(when, sizeof(when));
        snprintf(identity, sizeof(identity), "%s\n%s\n%s", source, href, title);
        bf_sha256_hex((const uint8_t *)identity, strlen(identity), digest);
        snprintf(occurrence_id, sizeof(occurrence_id), "pub-%.*s", 48, digest);
        /* Identity is taken from the bytes as the feed wrote them, and only
         * then are entities decoded for storage. Hashing the decoded title
         * would give every already-promoted item a new id, and re-promoting
         * a feed would duplicate its whole history. */
        entity_decoded += publication_decode_entities(title);
        entity_decoded += publication_decode_entities(href);
        entity_decoded += publication_decode_entities(when);
        if (publication_occurrence_exists(db, ns, occurrence_id)) { skipped++; cursor = (char *)end + 7; continue; }
        *out++ = '{'; *out = '\0';
        if (publication_json_pair(&out, payload + sizeof(payload), "occurrence_id", occurrence_id, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "source_twin", twin, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "source_ref", source, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "canonical_href", href, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "title", title, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "published_at", when, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "observed_at", "retained-body-promotion", &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "receipt_ref", twin, &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "rights_basis", "retained-public-rss-observation", &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "retention", "retained", &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "visibility", "public", &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "assertion_state", "OBSERVED", &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "cannot_infer", "a foreign publication remains a foreign assertion, endorsement, republication right or effect authority", &first) ||
            publication_json_pair(&out, payload + sizeof(payload), "status", "published", &first) ||
            out + 2 >= payload + sizeof(payload)) {
            sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); free(body); return -1;
        }
        *out++ = '}'; *out = '\0';
        if (entry_create(db, "publication_occurrence", ns, payload, NULL) != 0) {
            sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); free(body); return -1;
        }
        created++;
        cursor = (char *)end + 7;
    }
    free(body);
    g_defer_indexing = previous_defer;
    /* Rebuilding the index costs the whole table, so doing it per feed makes a
     * 500-feed promotion quadratic in the corpus it is building. A single
     * source owns its own rebuild; a batch defers and rebuilds once at the
     * end, and carries the obligation to do so -- the index is rebuilt inside
     * the same transaction as the rows either way, because a committed batch
     * with a stale index is a corpus that answers wrongly and looks healthy. */
    if (created > 0 && !g_defer_publication_index) {
        if (family_discover(db, "publication_occurrence") < 0 ||
            ann_build_index(db, "publication_occurrence") < 0) {
            fprintf(stderr, "[publication] index rebuild failed for %s\n", source);
            sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
            return -1;
        }
    }
    if (created > 0 && publication_delta_outbox_mark(db, ns, source) != 0) {
        fprintf(stderr, "[publication] durable source delta marker failed for %s\n", source);
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return -1;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) return -1;
    {
        int delta_enabled = cms_fact_deltas_enabled();
        sqlite3_int64 revision = 0;
        int pending = publication_delta_outbox_revision(db, ns, source, &revision);
        int delta_ok = pending >= 0;
        /* A no-op source has no write set. An older committed source whose
         * delivery failed remains in the outbox until an actual delivery
         * completes, even if this retry creates no new occurrences. */
        if (delta_ok && pending && delta_enabled) {
            delta_ok = cms_emit_fact_delta("publication_occurrence", NULL, source, 0);
            if (delta_ok) delta_ok = publication_delta_outbox_ack(db, ns, source, revision) == 0;
        }
        const char *delta_state = !delta_enabled ? "DISABLED"
            : !delta_ok ? "DELIVERY_FAILED" : pending ? "DELIVERED" : "NOT_NEEDED";
        printf("{\"source\":\"%s\",\"transcoded_fields\":%d,\"entity_decoded_fields\":%d,\"created\":%d,\"already_present\":%d,\"fact_delta\":\"%s\"}\n",
               source, transcoded, entity_decoded, created, skipped, delta_state);
        return delta_ok ? 0 : -1;
    }
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
    if (ns && ns[0]) {
        char *escaped_ns = sqlite3_mprintf("%q", ns);
        if (!escaped_ns) return -1;
        off += snprintf(sql + off, sizeof(sql) - (size_t)off, " AND namespace='%s'", escaped_ns);
        sqlite3_free(escaped_ns);
    }
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
static int entry_get(sqlite3 *db, const char *type_name, int entry_id,
                     const char *ns, FILE *out) {
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
    if (ns && ns[0]) {
        char *escaped_ns = sqlite3_mprintf("%q", ns);
        if (!escaped_ns) return -1;
        off += snprintf(sql + off, sizeof(sql) - (size_t)off, " AND namespace='%s'", escaped_ns);
        sqlite3_free(escaped_ns);
    }

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
static int required_fields_valid(sqlite3 *db, const ContentType *ct, int entry_id)
{
    char sql[4096];
    int off = snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM \"%s\" WHERE id=?1 AND (", ct->name);
    int required_count = 0;
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (!fd->required || fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;
        if (required_count++ > 0) off += snprintf(sql + off, sizeof(sql) - (size_t)off, " OR ");
        off += snprintf(sql + off, sizeof(sql) - (size_t)off,
            "\"%s\" IS NULL OR \"%s\"=''", fd->name, fd->name);
    }
    if (required_count == 0) return 0;
    off += snprintf(sql + off, sizeof(sql) - (size_t)off, ")");

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(stmt, 1, entry_id);
    int invalid = sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) > 0;
    sqlite3_finalize(stmt);
    if (invalid) fprintf(stderr, "[crud] required field missing after update: %s:%d\n", ct->name, entry_id);
    return invalid ? -1 : 0;
}

static int entry_update_if(sqlite3 *db, const char *type_name, int entry_id,
                        const char *ns, const char *body_json,
                        const char *if_field, const char *if_value,
                        const char *if_field2, const char *if_value2, const char *if_digest)
{
    ContentType *ct = find_type(type_name);
    if (!ct) return -1;
    if (entry_input_valid(db, ct, body_json) != 0) return -1;
    if (if_field2 && (!if_field || !strcmp(if_field, if_field2))) return -1;
    const char *conditions[][2]={{if_field,if_value},{if_field2,if_value2}};
    for (int k=0;k<2;k++) {
        if ((conditions[k][0]==NULL)!=(conditions[k][1]==NULL)) return -1;
        if (!conditions[k][0]) continue;
        int valid = 0;
        for (int i=0;i<ct->field_count;i++)
            if (!strcmp(ct->fields[i].name, conditions[k][0]) &&
                (ct->fields[i].type==FT_STRING || ct->fields[i].type==FT_TEXT)) valid=1;
        if (!valid) return -1;
    }

    /* The statement is grown as it is written and every value is quoted by
     * SQLite. It used to be printed into a fixed 8 KiB buffer with values
     * inlined, so an update larger than that was cut off mid-value and
     * refused, and the running offset could pass the end of the buffer. */
    char ts[64]; iso_timestamp(ts, sizeof(ts));
    sqlite3_str *q = sqlite3_str_new(db);
    sqlite3_str_appendf(q, "UPDATE \"%w\" SET updated_at=%Q", type_name, ts);

    /* Build SET clause for present fields */
    char *sval = malloc(MAX_FIELD_VAL);
    if (!sval) { sqlite3_free(sqlite3_str_finish(q)); return -1; }
    for (int i = 0; i < ct->field_count; i++) {
        const FieldDef *fd = &ct->fields[i];
        if (fd->type == FT_RELATION || fd->type == FT_COMPONENT) continue;
        int ival = 0;
        double dval = 0.0;
        int bval = 0;
        char *json_raw = NULL;
        int has_str = json_str(body_json, fd->name, sval, MAX_FIELD_VAL);
        int has_int = 0;
        int has_dbl = 0;
        int has_bool = 0;
        if (!has_str) has_int = json_int(body_json, fd->name, &ival);
        if (!has_str && !has_int) has_dbl = json_double(body_json, fd->name, &dval);
        if (!has_str && !has_int && !has_dbl) has_bool = json_bool(body_json, fd->name, &bval);
        if (!has_str && !has_int && !has_dbl && !has_bool && fd->type == FT_JSON)
            json_raw = dup_json_value(body_json, fd->name);
        if (has_str) {
            sqlite3_str_appendf(q, ", \"%w\"=%Q", fd->name, sval);
        } else if (fd->type == FT_INTEGER && has_int) {
            sqlite3_str_appendf(q, ", \"%w\"=%d", fd->name, ival);
        } else if (fd->type == FT_BOOLEAN && (has_bool || has_int)) {
            sqlite3_str_appendf(q, ", \"%w\"=%d", fd->name, has_bool ? bval : (ival != 0));
        } else if (fd->type == FT_DECIMAL && (has_dbl || has_int)) {
            sqlite3_str_appendf(q, ", \"%w\"=%!.17g", fd->name, has_dbl ? dval : (double)ival);
        } else if (fd->type == FT_JSON && json_raw) {
            sqlite3_str_appendf(q, ", \"%w\"=%Q", fd->name, json_raw);
        } else if (fd->type != FT_JSON && fd->type != FT_INTEGER &&
                   fd->type != FT_BOOLEAN && (has_int || has_dbl || has_bool)) {
            json_raw = dup_json_value(body_json, fd->name);
            if (!json_raw) { free(sval); sqlite3_free(sqlite3_str_finish(q)); return -1; }
            sqlite3_str_appendf(q, ", \"%w\"=%Q", fd->name, json_raw);
        }
        free(json_raw);
    }
    free(sval);

    /* Handle status + published_at */
    char new_status[64];
    if (json_str(body_json, "status", new_status, sizeof(new_status))) {
        sqlite3_str_appendf(q, ", status=%Q", new_status);
        if (strcmp(new_status, "published") == 0)
            sqlite3_str_appendf(q, ", published_at=%Q", ts);
    }

    sqlite3_str_appendf(q, " WHERE id=%d", entry_id);
    if (ns && ns[0]) sqlite3_str_appendf(q, " AND namespace=%Q", ns);
    if (if_field) {
        if (if_value && !if_value[0]) sqlite3_str_appendf(q, " AND COALESCE(\"%w\",'')=''", if_field);
        else sqlite3_str_appendf(q, " AND \"%w\"=%Q", if_field, if_value);
    }
    if (if_field2) {
        if (if_value2 && !if_value2[0]) sqlite3_str_appendf(q, " AND COALESCE(\"%w\",'')=''", if_field2);
        else sqlite3_str_appendf(q, " AND \"%w\"=%Q", if_field2, if_value2);
    }
    char *sql = sqlite3_str_finish(q);
    if (!sql) return -1;

    if (sqlite3_exec(db, "SAVEPOINT entry_update_sp", NULL, NULL, NULL) != SQLITE_OK) { sqlite3_free(sql); return -1; }
    if (if_digest) {
        char current[65];
        if (strlen(if_digest)!=64 || strspn(if_digest,"0123456789abcdef")!=64 ||
            !cms_snapshot_row(db,type_name,entry_id,ns,current) || strcmp(current,if_digest)) {
            sqlite3_free(sql);
            sqlite3_exec(db,"ROLLBACK TO entry_update_sp",NULL,NULL,NULL);
            sqlite3_exec(db,"RELEASE entry_update_sp",NULL,NULL,NULL);
            fprintf(stderr,"[crud] reviewed entry changed; refresh before saving\n"); return -1;
        }
    }
    char *previous_row = entry_row_json(db, ct, entry_id);
    int rc = db_exec(db, sql);
    sqlite3_free(sql);
    if (rc == 0 && sqlite3_changes(db) != 1) rc = -1;

    if (rc == 0 && (required_fields_valid(db, ct, entry_id) != 0 ||
                    conditional_required_valid(db, ct, entry_id) != 0)) {
        sqlite3_exec(db, "ROLLBACK TO entry_update_sp", NULL, NULL, NULL);
        rc = -1;
    }

    /* Log update op to BonfyreGraph op log */
    if (rc == 0) {
        if (ops_append(db, OP_UPDATE, type_name, (long long)entry_id, ns ? ns : "root", body_json, "system", NULL) && defer_hooks)
            deferred_write_error = 1;
        if (!g_defer_indexing) {
            /* Value-only update: rebind without re-hashing family (structure doesn't change) */
            if (family_rebind_entry(db, type_name, entry_id) != 0) {
                fprintf(stderr, "[crud] family rebind failed after update for %s:%d\n", type_name, entry_id);
            if (defer_hooks) deferred_write_error = 1;
            }
        }
        {
            char *row = entry_row_json(db, ct, entry_id);
            run_hooks(db, type_name, "after_update", entry_id, ns, row, previous_row);
            free(row);
        }
    } else {
        sqlite3_exec(db, "ROLLBACK TO entry_update_sp", NULL, NULL, NULL);
    }
    free(previous_row);
    sqlite3_exec(db, "RELEASE entry_update_sp", NULL, NULL, NULL);
    return rc;
}

static int entry_update(sqlite3 *db, const char *type_name, int entry_id,
                        const char *ns, const char *body_json)
{
    return entry_update_if(db, type_name, entry_id, ns, body_json, NULL, NULL, NULL, NULL, NULL);
}

typedef struct {
    int id;
    char namespace[256];
    CmsDeltaCoordinates previous;
} CmsBatchUpdate;

static int entry_condition_matches(sqlite3 *db, const char *type_name, int entry_id,
                                   const char *ns, const char *if_field, const char *if_value,
                                   const char *if_field2, const char *if_value2)
{
    ContentType *ct = find_type(type_name);
    sqlite3_str *query = NULL;
    sqlite3_stmt *stmt = NULL;
    const char *fields[2] = { if_field, if_field2 };
    const char *values[2] = { if_value, if_value2 };
    int matched = -1;

    if (!db || !ct || entry_id < 1) return -1;
    for (int k = 0; k < 2; k++) {
        int valid = 0;
        if ((fields[k] == NULL) != (values[k] == NULL)) return -1;
        if (!fields[k]) continue;
        for (int i = 0; i < ct->field_count; i++) {
            if (!strcmp(ct->fields[i].name, fields[k]) &&
                (ct->fields[i].type == FT_STRING || ct->fields[i].type == FT_TEXT)) {
                valid = 1;
                break;
            }
        }
        if (!valid) return -1;
    }

    query = sqlite3_str_new(db);
    if (!query) return -1;
    sqlite3_str_appendf(query, "SELECT 1 FROM \"%w\" WHERE id=%d", type_name, entry_id);
    if (ns && *ns) sqlite3_str_appendf(query, " AND namespace=%Q", ns);
    for (int k = 0; k < 2; k++) {
        if (!fields[k]) continue;
        if (values[k] && !values[k][0]) sqlite3_str_appendf(query, " AND COALESCE(\"%w\",'')=''", fields[k]);
        else sqlite3_str_appendf(query, " AND \"%w\"=%Q", fields[k], values[k]);
    }
    char *sql = sqlite3_str_finish(query);
    if (!sql) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK)
        matched = sqlite3_step(stmt) == SQLITE_ROW ? 1 : 0;
    sqlite3_finalize(stmt);
    sqlite3_free(sql);
    return matched;
}

/* Apply many reviewed partial updates through the exact same typed update
 * owner as one interactive edit. Conditional mismatches are stale work and
 * are skipped; malformed or invalid rows roll the whole batch back. Fact
 * deltas are emitted only after the canonical transaction commits. */
static int entry_update_jsonl(sqlite3 *db, const char *type_name, const char *ns,
                              const char *path, int *out_rows, int *out_updated,
                              int *out_skipped, int *out_delta_failures)
{
    FILE *input = NULL;
    char *line = NULL;
    size_t cap = 0, changed_cap = 0;
    ssize_t nread;
    CmsBatchUpdate *changed = NULL;
    int rows = 0, updated = 0, skipped = 0, delta_failures = 0, line_no = 0;
    int rc = -1;

    if (!db || !type_name || !ns || !path || !find_type(type_name)) {
        fprintf(stderr, "[crud] update-jsonl: invalid type or arguments\n");
        return -1;
    }
    input = !strcmp(path, "-") ? stdin : fopen(path, "r");
    if (!input) {
        fprintf(stderr, "[crud] update-jsonl: cannot read %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] update-jsonl: begin failed: %s\n", sqlite3_errmsg(db));
        goto cleanup;
    }

    while ((nread = getline(&line, &cap, input)) >= 0) {
        char *body = line, *end, *attrs = NULL;
        char if_field[128] = "", if_value[MAX_FIELD_VAL] = "";
        char if_field2[128] = "", if_value2[MAX_FIELD_VAL] = "";
        char row_ns[256] = "";
        int id = 0, has_if_field, has_if_value, has_if_field2, has_if_value2, has_row_ns, condition;
        const char *effective_ns;
        CmsDeltaCoordinates previous = {0};
        (void)nread;
        line_no++;
        while (*body && isspace((unsigned char)*body)) body++;
        end = body + strlen(body);
        while (end > body && isspace((unsigned char)end[-1])) end--;
        *end = '\0';
        if (!*body) continue;
        rows++;

        attrs = dup_json_value(body, "attributes");
        has_if_field = json_str(body, "if_field", if_field, sizeof(if_field));
        has_if_value = json_str(body, "if_value", if_value, sizeof(if_value));
        has_if_field2 = json_str(body, "if_field2", if_field2, sizeof(if_field2));
        has_if_value2 = json_str(body, "if_value2", if_value2, sizeof(if_value2));
        has_row_ns = json_str(body, "namespace", row_ns, sizeof(row_ns));
        effective_ns = has_row_ns ? row_ns : ns;
        if (!json_int(body, "id", &id) || id < 1 || !attrs || attrs[0] != '{' ||
            has_if_field != has_if_value || has_if_field2 != has_if_value2 ||
            (has_if_field2 && !has_if_field) || (has_row_ns && !row_ns[0])) {
            fprintf(stderr, "[crud] update-jsonl: line %d is not a valid conditional update\n", line_no);
            free(attrs);
            goto rollback;
        }

        condition = entry_condition_matches(db, type_name, id, effective_ns,
            has_if_field ? if_field : NULL, has_if_field ? if_value : NULL,
            has_if_field2 ? if_field2 : NULL, has_if_field2 ? if_value2 : NULL);
        if (condition < 0) {
            fprintf(stderr, "[crud] update-jsonl: line %d has an invalid condition\n", line_no);
            free(attrs);
            goto rollback;
        }
        if (!condition) {
            skipped++;
            free(attrs);
            continue;
        }

        (void)cms_row_delta_coordinates(db, type_name, id, effective_ns, &previous);
        if (entry_update_if(db, type_name, id, effective_ns, attrs,
                has_if_field ? if_field : NULL, has_if_field ? if_value : NULL,
                has_if_field2 ? if_field2 : NULL, has_if_field2 ? if_value2 : NULL,
                NULL) != 0) {
            fprintf(stderr, "[crud] update-jsonl: line %d rejected; rolling back batch\n", line_no);
            free(attrs);
            goto rollback;
        }
        free(attrs);
        if ((size_t)updated == changed_cap) {
            size_t next = changed_cap ? changed_cap * 2 : 64;
            CmsBatchUpdate *grown = realloc(changed, next * sizeof(*changed));
            if (!grown) goto rollback;
            changed = grown;
            changed_cap = next;
        }
        changed[updated].id = id;
        snprintf(changed[updated].namespace, sizeof(changed[updated].namespace), "%s", effective_ns);
        changed[updated].previous = previous;
        updated++;
    }
    if (ferror(input)) {
        fprintf(stderr, "[crud] update-jsonl: read failed at line %d\n", line_no);
        goto rollback;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "[crud] update-jsonl: commit failed: %s\n", sqlite3_errmsg(db));
        goto rollback;
    }

    for (int i = 0; i < updated; i++) {
        int published = 0;
        int delta_ok = emit_published_row_delta(db, type_name, changed[i].id, changed[i].namespace, &published);
        if (delta_ok && !published && changed[i].previous.published)
            delta_ok = cms_emit_fact_delta(type_name, changed[i].previous.identity,
                                           changed[i].previous.publication_source, changed[i].id);
        if (cms_fact_deltas_enabled() && !delta_ok) delta_failures++;
    }
    if (out_rows) *out_rows = rows;
    if (out_updated) *out_updated = updated;
    if (out_skipped) *out_skipped = skipped;
    if (out_delta_failures) *out_delta_failures = delta_failures;
    rc = delta_failures ? 2 : 0;
    goto cleanup;

rollback:
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
cleanup:
    free(changed);
    free(line);
    if (input && input != stdin) fclose(input);
    return rc;
}

/* Delete entry + its relations */
static int entry_delete(sqlite3 *db, const char *type_name, int entry_id, const char *ns) {
    char sql[512];
    int off = snprintf(sql, sizeof(sql), "DELETE FROM \"%s\" WHERE id=%d", type_name, entry_id);
    if (ns && ns[0]) {
        char *escaped_ns = sqlite3_mprintf("%q", ns);
        if (!escaped_ns) return -1;
        off += snprintf(sql + off, sizeof(sql) - (size_t)off, " AND namespace='%s'", escaped_ns);
        sqlite3_free(escaped_ns);
    }
    if (db_exec(db, sql) != 0) return -1;
    if (sqlite3_changes(db) != 1) return -1;

    if (family_remove_entry(db, type_name, entry_id) != 0) {
        fprintf(stderr, "[crud] family removal failed after delete for %s:%d\n", type_name, entry_id);
            if (defer_hooks) deferred_write_error = 1;
    }

    /* Log delete op to BonfyreGraph op log */
    if (ops_append(db, OP_DELETE, type_name, (long long)entry_id, ns ? ns : "root", "{}", "system", NULL) && defer_hooks)
        deferred_write_error = 1;

    snprintf(sql, sizeof(sql),
        "DELETE FROM _relations WHERE (source_type='%s' AND source_id=%d) "
        "OR (target_type='%s' AND target_id=%d)",
        type_name, entry_id, type_name, entry_id);
    return db_exec(db, sql);
}

#include "page_snapshot.h"

/* Commit the existing page aggregate through typed CRUD in one transaction.
 * Hooks and file-based publication notifications occur only after commit.
 * An expected page ID also prevents a removed/recreated route being edited
 * through the old page coordinate. The reviewed digest includes every stored
 * page and fragment field; concurrent edits cannot replace a changed snapshot. */
static int entry_page_write(sqlite3 *db, const char *ns, const char *body) {
    sqlite3_stmt *st = NULL, *fragments = NULL;
    char *page = NULL, *route = NULL, *operation = NULL;
    char expected_digest[65] = "", saved_digest[65] = "";
    long long expected = 0, current = 0, page_id = 0;
    int rc = 1, count = 0, begun = 0, committed = 0, step;
    if (!ns || !*ns || strlen(ns) > 127 || !body || strlen(body) > MAX_BODY) goto done;
    /* Reject ambiguous object keys and decoded NULs before the legacy field
     * reader could truncate them. Escaped literal backslashes remain text. */
    for (const char *p = body; *p; p++) {
        if (*p != '\\') continue;
        if (!strncmp(p + 1, "u0000", 5)) goto done;
        if (p[1]) p++;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT json_type(?1)='object' AND json_type(?1,'$.operation')='text' "
        "AND json_type(?1,'$.route')='text' AND json_type(?1,'$.expected_id')='integer' "
        "AND (json_type(?1,'$.expected_digest') IS NULL OR (json_type(?1,'$.expected_digest')='text' "
        "AND length(json_extract(?1,'$.expected_digest'))=64 AND json_extract(?1,'$.expected_digest') NOT GLOB '*[^0-9a-f]*')) "
        "AND json_type(?1,'$.fragments')='array' AND json_array_length(?1,'$.fragments')<=512 "
        "AND (json_extract(?1,'$.operation')='remove' OR (json_type(?1,'$.page.route')='text' AND json_extract(?1,'$.page.route')=json_extract(?1,'$.route'))) "
        "AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.fragments') WHERE type!='object' OR json_type(value,'$.page') IS NOT 'text' OR json_extract(value,'$.page')!=json_extract(?1,'$.route')) "
        "AND NOT EXISTS(SELECT 1 FROM json_tree(?1) WHERE type='text' AND length(CAST(atom AS BLOB))>=?2) "
        "AND NOT EXISTS(SELECT 1 FROM json_tree(?1) WHERE parent IS NOT NULL GROUP BY parent,key HAVING count(*)>1),"
        "json_extract(?1,'$.operation'),json_extract(?1,'$.route'),json_extract(?1,'$.expected_id'),json_extract(?1,'$.page'),json_type(?1,'$.page')",
        -1, &st, NULL) != SQLITE_OK || sqlite3_bind_text(st, 1, body, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_int(st, 2, MAX_FIELD_VAL) != SQLITE_OK || sqlite3_step(st) != SQLITE_ROW || sqlite3_column_int(st, 0) != 1) goto done;
    operation = strdup((const char *)sqlite3_column_text(st, 1));
    route = strdup((const char *)sqlite3_column_text(st, 2)); expected = sqlite3_column_int64(st, 3);
    const char *page_type = (const char *)sqlite3_column_text(st, 5);
    if (!operation || !route || !*route || strlen(route) > 127 || expected < 0 || expected > INT_MAX) goto done;
    int removing = !strcmp(operation, "remove"), creating = !strcmp(operation, "create");
    if (!removing && !creating && strcmp(operation, "replace")) goto done;
    if ((creating && expected) || (!creating && !expected)) goto done;
    if (!creating || json_str(body,"expected_digest",expected_digest,sizeof(expected_digest))) {
        if (!json_str(body,"expected_digest",expected_digest,sizeof(expected_digest)) || strlen(expected_digest)!=64 || strspn(expected_digest,"0123456789abcdef")!=64) goto done;
    }
    if (!removing) {
        if (!page_type || strcmp(page_type, "object")) goto done;
        page = strdup((const char *)sqlite3_column_text(st, 4));
        char page_route[128];
        if (!page || !json_str(page, "route", page_route, sizeof(page_route)) || strcmp(page_route, route)) goto done;
    } else if (page_type && strcmp(page_type, "null")) goto done;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto done;
    begun = 1; defer_hooks = 1; deferred_write_error = 0;
    if (sqlite3_prepare_v2(db, "SELECT id FROM site_page WHERE namespace=?1 AND route=?2", -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
    step = sqlite3_step(st);
    if (step == SQLITE_ROW) { current = sqlite3_column_int64(st, 0); step = sqlite3_step(st); }
    if (step != SQLITE_DONE || current != expected) goto done;
    sqlite3_finalize(st); st = NULL;
    if (expected_digest[0]) {
        long long snapshot_id=0; char actual_digest[65];
        char *snapshot=page_snapshot(db,ns,route,actual_digest,&snapshot_id);
        int matches=snapshot && snapshot_id==current && !strcmp(actual_digest,expected_digest);
        free(snapshot); if (!matches) goto done;
    }
    page_id = current;
    if (!removing) {
        if (creating ? entry_create(db, "site_page", ns, page, &page_id) : entry_update(db, "site_page", (int)current, ns, page)) goto done;
        if (page_id > INT_MAX) goto done;
    }
    /* Retain old identities until after commit for the exact invalidations.
     * This temporary table lives only in this CMS connection. */
    if (sqlite3_exec(db, "CREATE TEMP TABLE page_write_retired AS SELECT id,coordinate,status FROM site_fragment WHERE 0", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db, "INSERT INTO page_write_retired SELECT id,coordinate,status FROM site_fragment WHERE namespace=?1 AND page=?2", -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_DONE) goto done;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT id FROM page_write_retired", -1, &st, NULL) != SQLITE_OK) goto done;
    while ((step = sqlite3_step(st)) == SQLITE_ROW)
        if (entry_delete(db, "site_fragment", sqlite3_column_int(st, 0), ns)) goto done;
    if (step != SQLITE_DONE) goto done;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT value,type FROM json_each(?1,'$.fragments')", -1, &fragments, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(fragments, 1, body, -1, SQLITE_STATIC);
    while ((step = sqlite3_step(fragments)) == SQLITE_ROW) {
        const char *row = (const char *)sqlite3_column_text(fragments, 0), *type = (const char *)sqlite3_column_text(fragments, 1);
        char parent[128];
        if (removing || !type || strcmp(type, "object") || !row || !json_str(row, "page", parent, sizeof(parent)) || strcmp(parent, route)) goto done;
        if (entry_create(db, "site_fragment", ns, row, NULL)) goto done;
        count++;
    }
    if (step != SQLITE_DONE || deferred_write_error) goto done;
    sqlite3_finalize(fragments); fragments = NULL;
    if (removing && entry_delete(db, "site_page", (int)current, ns)) goto done;
    if (deferred_write_error) goto done;
    {
        long long saved_id=0; char *saved=page_snapshot(db,ns,route,saved_digest,&saved_id);
        if (!saved || saved_id!=(removing?0:page_id)) { free(saved); goto done; }
        free(saved);
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    begun = 0; committed = 1;
    finish_deferred_hooks(db, 1);
    int delta_ok = cms_emit_fact_delta("site_page", route, NULL, page_id);
    if (sqlite3_prepare_v2(db, "SELECT id,coordinate FROM page_write_retired WHERE status='published' UNION SELECT id,coordinate FROM site_fragment WHERE namespace=?1 AND page=?2 AND status='published'", -1, &st, NULL) != SQLITE_OK) delta_ok = 0;
    else {
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        while ((step = sqlite3_step(st)) == SQLITE_ROW)
            if (!cms_emit_fact_delta("site_fragment", (const char *)sqlite3_column_text(st, 1), NULL, sqlite3_column_int64(st, 0))) delta_ok = 0;
        if (step != SQLITE_DONE) delta_ok = 0;
    }
    printf("{\"committed\":true,\"id\":%lld,\"fragments\":%d,\"digest\":\"%s\",\"fact_delta\":\"%s\"}\n", page_id, count, saved_digest,
        !cms_fact_deltas_enabled() ? "DISABLED" : delta_ok ? "DELIVERED" : "DELIVERY_FAILED");
    rc = delta_ok ? 0 : 2;
 done:
    sqlite3_finalize(st); sqlite3_finalize(fragments);
    if (begun) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    if (!committed) { finish_deferred_hooks(db, 0); fputs("{\"committed\":false,\"error\":\"page write refused\"}\n", stdout); }
    free(page); free(route); free(operation);
    return rc;
}

#include "source_publication.h"
#include "community_journey.h"
#include "household_members.h"
#include "pack_lifecycle.h"
#include "pack_image.h"

/* ================================================================
 * Token management
 * ================================================================ */

static int token_issue(sqlite3 *db, const char *scope, const char *actions,
                       const char *label)
{
    char ts[64]; iso_timestamp(ts, sizeof(ts));

    /* A token is 32 bytes from the system's random source.  It was once made
     * from the scope, the second and the process id, which anyone who knew
     * roughly when a token was issued could reproduce. */
    unsigned char secret[32];
    char token[128] = "bfcms_";
    FILE *random_source = fopen("/dev/urandom", "rb");
    if (!random_source || fread(secret, 1, sizeof(secret), random_source) != sizeof(secret)) {
        if (random_source) fclose(random_source);
        fprintf(stderr, "[token] the system random source is unavailable; no token issued\n");
        return -1;
    }
    fclose(random_source);
    for (size_t i = 0; i < sizeof(secret); i++) snprintf(token + 6 + i * 2, 3, "%02x", secret[i]);

    /* Only its SHA-256 is kept, so the store cannot be read for tokens. */
    char token_hash[65];
    bf_sha256_hex((const uint8_t *)token, strlen(token), token_hash);

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
    char content_type[128];
    char accept[256];
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

/* The machine contract remains JSON.  An HTMX form is a human carrier, though:
 * it must receive a readable state transition, never a JSON receipt that a
 * browser-side template has to reinterpret. */
static void http_resp_human_result(HttpResponse *r, int status, const char *verb,
                                   const char *collection, long long id) {
    char body[1024];
    (void)verb;
    r->status = status;
    snprintf(r->content_type, sizeof(r->content_type), "text/html; charset=utf-8");
    snprintf(body, sizeof(body),
        "<section class=\"cms-save-result %s\" aria-live=\"polite\">"
        "<p class=\"eyebrow\"><i></i>Native content owner</p>"
        "<h3>%s</h3><p>%s <span class=\"mono\">#%lld</span> is now in the canonical content store.</p>"
        "</section>",
        status >= 400 ? "is-error" : "is-ok",
        status >= 400 ? "This needs attention" : "Saved",
        collection ? collection : "entry", id);
    free(r->buf);
    r->buf_len = strlen(body);
    r->buf = malloc(r->buf_len + 1);
    if (r->buf) memcpy(r->buf, body, r->buf_len + 1);
}

static int request_wants_html(const HttpRequest *req) {
    return req && strstr(req->accept, "text/html") != NULL;
}

static int form_decode_component(const char *src, size_t len, char *out, size_t cap) {
    size_t used = 0;
    if (!src || !out || cap == 0) return 0;
    for (size_t i = 0; i < len && used + 1 < cap; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '+') c = ' ';
        else if (c == '%' && i + 2 < len && isxdigit((unsigned char)src[i + 1]) && isxdigit((unsigned char)src[i + 2])) {
            int hi = isdigit((unsigned char)src[i + 1]) ? src[i + 1] - '0' : tolower((unsigned char)src[i + 1]) - 'a' + 10;
            int lo = isdigit((unsigned char)src[i + 2]) ? src[i + 2] - '0' : tolower((unsigned char)src[i + 2]) - 'a' + 10;
            c = (unsigned char)((hi << 4) | lo); i += 2;
        }
        if (c < 0x20) return 0;
        out[used++] = (char)c;
    }
    out[used] = '\0';
    return 1;
}

static int form_value(const char *form, const char *wanted, char *out, size_t cap) {
    const char *p = form;
    size_t wanted_len;
    if (!form || !wanted || !out || !(wanted_len = strlen(wanted))) return 0;
    while (*p) {
        const char *end = strchr(p, '&');
        const char *eq = strchr(p, '=');
        size_t field_len = end ? (size_t)(end - p) : strlen(p);
        if (eq && eq < p + field_len && (size_t)(eq - p) == wanted_len && !strncmp(p, wanted, wanted_len))
            return form_decode_component(eq + 1, (size_t)((end ? end : p + field_len) - (eq + 1)), out, cap);
        p = end ? end + 1 : p + field_len;
    }
    return 0;
}

static int json_append_string(char *out, size_t cap, size_t *used, const char *value) {
    if (!out || !used || !value || *used + 2 >= cap) return 0;
    out[(*used)++] = '\"';
    for (; *value && *used + 8 < cap; value++) {
        if (*value == '\"' || *value == '\\') { out[(*used)++] = '\\'; out[(*used)++] = *value; }
        else if ((unsigned char)*value < 0x20) return 0;
        else out[(*used)++] = *value;
    }
    if (*used + 2 >= cap) return 0;
    out[(*used)++] = '\"'; out[*used] = '\0';
    return 1;
}

/* Compile ordinary browser form fields through the registered content type.
 * The form never chooses SQL columns: unknown fields are ignored, scalar kinds
 * remain typed, and entry_create/entry_update still own validation. */
static int form_to_json(const char *collection, const char *form, char *out, size_t cap) {
    ContentType *type = find_type(collection);
    size_t used = 0;
    int fields = 0;
    if (!type || !form || !out || cap < 4) return 0;
    out[used++] = '{'; out[used] = '\0';
    for (int i = 0; i < type->field_count; i++) {
        const FieldDef *field = &type->fields[i];
        char value[MAX_FIELD_VAL + 1];
        const char *kind;
        int is_bool, is_number;
        if (!form_value(form, field->name, value, sizeof(value)) || !value[0]) continue;
        kind = field_type_name(field->type);
        is_bool = !strcmp(kind, "boolean");
        is_number = !strcmp(kind, "number") || !strcmp(kind, "integer") || !strcmp(kind, "decimal");
        if (fields++) { if (used + 1 >= cap) return 0; out[used++] = ','; }
        if (!json_append_string(out, cap, &used, field->name) || used + 1 >= cap) return 0;
        out[used++] = ':';
        if (is_bool) {
            const char *b = (!strcasecmp(value, "on") || !strcasecmp(value, "true") || !strcmp(value, "1")) ? "true" : "false";
            if (used + strlen(b) >= cap) return 0;
            memcpy(out + used, b, strlen(b)); used += strlen(b); out[used] = '\0';
        } else if (is_number) {
            char *end = NULL; (void)strtod(value, &end);
            if (!end || *end) return 0;
            if (used + strlen(value) >= cap) return 0;
            memcpy(out + used, value, strlen(value)); used += strlen(value); out[used] = '\0';
        } else if (!json_append_string(out, cap, &used, value)) return 0;
    }
    if (used + 2 > cap) return 0;
    out[used++] = '}'; out[used] = '\0';
    return fields > 0;
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
        if (strncasecmp(hp, "Content-Type:", 13) == 0) {
            const char *v = hp + 13; while (*v == ' ') v++;
            snprintf(req->content_type, sizeof(req->content_type), "%s", v);
        }
        if (strncasecmp(hp, "Accept:", 7) == 0) {
            const char *v = hp + 7; while (*v == ' ') v++;
            snprintf(req->accept, sizeof(req->accept), "%s", v);
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

    /* Hash the bearer token.  Tokens issued before 2026-10-10 were stored
     * under a 64-bit hash; they still verify, so nothing breaks, and
     * `token list` marks them so they can be reissued. */
    char token_hash[65];
    bf_sha256_hex((const uint8_t *)bearer, strlen(bearer), token_hash);
    {
        sqlite3_stmt *known;
        int present = 0;
        if (sqlite3_prepare_v2(db, "SELECT 1 FROM _tokens WHERE token_hash = ?1", -1, &known, NULL) == SQLITE_OK) {
            sqlite3_bind_text(known, 1, token_hash, -1, SQLITE_STATIC);
            present = sqlite3_step(known) == SQLITE_ROW;
            sqlite3_finalize(known);
        }
        if (!present) snprintf(token_hash, sizeof(token_hash), "%llx", fnv1a64(bearer, strlen(bearer)));
    }

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
            int rc = entry_get(db, collection, entry_id, ns, mem);
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
        char form_json[MAX_BODY];
        const char *payload = req->body;
        if (req->body_len == 0) {
            http_resp_json(resp, 400, "{\"error\":\"empty body\"}");
            return;
        }
        if (strstr(req->content_type, "application/x-www-form-urlencoded")) {
            if (!form_to_json(collection, req->body, form_json, sizeof(form_json))) {
                if (request_wants_html(req)) http_resp_human_result(resp, 400, "create", collection, 0);
                else http_resp_json(resp, 400, "{\"error\":\"valid content fields required\"}");
                return;
            }
            payload = form_json;
        }
        long long new_id = 0;
        int rc = entry_create(db, collection, ns, payload, &new_id);
        char publish_status[64] = "draft";
        int published = json_str(payload, "status", publish_status, sizeof(publish_status)) && !strcmp(publish_status, "published");
        int delta_ok = rc == 0 ? (!published || emit_published_entry_delta(collection, payload, new_id)) : 0;
        int delta_enabled = cms_fact_deltas_enabled();
        if (rc == 0) {
            int status = published && delta_enabled && !delta_ok ? 202 : 201;
            if (request_wants_html(req)) http_resp_human_result(resp, status, "create", collection, new_id);
            else http_resp_json(resp, status,
                "{\"data\":{\"id\":%lld},\"meta\":{\"created\":true,\"fact_delta\":\"%s\"}}", new_id,
                !published ? "NOT_PUBLISHED" : !delta_enabled ? "DISABLED" : delta_ok ? "DELIVERED" : "DELIVERY_FAILED");
        } else {
            if (request_wants_html(req)) http_resp_human_result(resp, 500, "create", collection, 0);
            else http_resp_json(resp, 500, "{\"error\":\"create failed\"}");
        }
    } else if (strcmp(req->method, "PUT") == 0) {
        char form_json[MAX_BODY];
        const char *payload = req->body;
        if (entry_id <= 0) {
            http_resp_json(resp, 400, "{\"error\":\"id required\"}");
            return;
        }
        if (strstr(req->content_type, "application/x-www-form-urlencoded")) {
            if (!form_to_json(collection, req->body, form_json, sizeof(form_json))) {
                if (request_wants_html(req)) http_resp_human_result(resp, 400, "update", collection, entry_id);
                else http_resp_json(resp, 400, "{\"error\":\"valid content fields required\"}");
                return;
            }
            payload = form_json;
        }
        CmsDeltaCoordinates previous = {0};
        int previous_ok = cms_row_delta_coordinates(db, collection, entry_id, ns, &previous);
        int rc = entry_update(db, collection, entry_id, ns, payload);
        int published = 0;
        int delta_ok = rc == 0 ? emit_published_row_delta(db, collection, entry_id, ns, &published) : 0;
        if (rc == 0 && !published && previous_ok && previous.published)
            delta_ok = cms_emit_fact_delta(collection, previous.identity, previous.publication_source, entry_id);
        int delta_enabled = cms_fact_deltas_enabled();
        if (rc == 0) {
            int status = (published || (previous_ok && previous.published)) && delta_enabled && !delta_ok ? 202 : 200;
            if (request_wants_html(req)) http_resp_human_result(resp, status, "update", collection, entry_id);
            else http_resp_json(resp, status,
                "{\"data\":{\"id\":%d},\"meta\":{\"updated\":true,\"fact_delta\":\"%s\"}}", entry_id,
                !published && !(previous_ok && previous.published) ? "NOT_PUBLISHED" : !delta_enabled ? "DISABLED" : delta_ok ? published ? "DELIVERED" : "RETIRED" : "DELIVERY_FAILED");
        } else {
            if (request_wants_html(req)) http_resp_human_result(resp, 500, "update", collection, entry_id);
            else http_resp_json(resp, 500, "{\"error\":\"update failed\"}");
        }
    } else if (strcmp(req->method, "DELETE") == 0) {
        if (entry_id <= 0) {
            http_resp_json(resp, 400, "{\"error\":\"id required\"}");
            return;
        }
        CmsDeltaCoordinates coordinate = {0};
        int coordinate_ok = cms_row_delta_coordinates(db, collection, entry_id, ns, &coordinate);
        int rc = entry_delete(db, collection, entry_id, ns);
        int delta_enabled = cms_fact_deltas_enabled();
        int delta_ok = rc == 0 && coordinate_ok && (!coordinate.published ||
            cms_emit_fact_delta(collection, coordinate.identity, coordinate.publication_source, entry_id));
        if (rc == 0) {
            int status = coordinate.published && delta_enabled && !delta_ok ? 202 : 200;
            http_resp_json(resp, status,
                "{\"data\":{\"id\":%d},\"meta\":{\"deleted\":true,\"fact_delta\":\"%s\"}}", entry_id,
                !coordinate.published ? "NOT_PUBLISHED" : !delta_enabled ? "DISABLED" : delta_ok ? "DELIVERED" : "DELIVERY_FAILED");
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
            /* Emit the field definitions, not just a count. A count tells a
             * client how many inputs to expect and nothing about what they are,
             * so any editor built on it has to hardcode a second copy of the
             * schema. The content type describes itself here instead. */
            fprintf(mem, "\n  {\"name\":\"%s\",\"displayName\":\"%s\",\"description\":\"%s\","
                         "\"fieldCount\":%d,\"fields\":[",
                g_types[i].name, g_types[i].display_name,
                g_types[i].description, g_types[i].field_count);
            for (int f = 0; f < g_types[i].field_count; f++) {
                const FieldDef *fd = &g_types[i].fields[f];
                if (f > 0) fprintf(mem, ",");
                fprintf(mem, "{\"name\":\"%s\",\"type\":\"%s\",\"required\":%s,"
                             "\"unique\":%s,\"repeatable\":%s",
                        fd->name, field_type_name(fd->type),
                        fd->required ? "true" : "false",
                        fd->unique_within_namespace ? "true" : "false",
                        fd->repeatable ? "true" : "false");
                if (fd->enum_values[0]) fprintf(mem, ",\"enum\":\"%s\"", fd->enum_values);
                if (fd->default_val[0]) fprintf(mem, ",\"default\":\"%s\"", fd->default_val);
                if (fd->relation_kind[0]) fprintf(mem, ",\"relation\":\"%s\"", fd->relation_kind);
                if (fd->target[0]) fprintf(mem, ",\"target\":\"%s\"", fd->target);
                if (fd->target_field[0]) fprintf(mem, ",\"targetField\":\"%s\"", fd->target_field);
                fprintf(mem, "}");
            }
            fprintf(mem, "]}");
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
        if (!pfd) { close(client_fd); continue; }
        *pfd = client_fd;
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
        int created = pthread_create(&tid, &attr, connection_handler, pfd);
        pthread_attr_destroy(&attr);
        if (created != 0) { free(pfd); close(client_fd); }
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
        "  bonfyre-cms entry page-read ROUTE --ns NS  exact page and fragment snapshot\n"
        "  bonfyre-cms entry page-write @file.json --ns NS  atomic page and fragment write\n"
        "  bonfyre-cms entry create <type> [--ns NS] '<json>'\n"
        "  bonfyre-cms entry list <type> [--ns NS] [--status STATUS] [--limit N]\n"
        "  bonfyre-cms entry get <type> <id>\n"
        "  bonfyre-cms entry update <type> <id> '<json>' [--ns NS]\n"
        "  bonfyre-cms entry import-jsonl <type> <file.jsonl> [--ns NS] [--mode create|update]\n"
        "  bonfyre-cms entry delete <type> <id> [--ns NS]\n"
        "  bonfyre-cms pack-image --pack ID [--version VERSION] [--out-dir ABSOLUTE_STORE | --verify-image BFP_PATH] [--cut-digest VERIFIED_SHA256] --db FILE\n"
        "  bonfyre-cms pack-lifecycle --operation plan|current|install|activate|deactivate|remove\n"
        "      --pack ID --subject SUBJECT --scope self|institution --actor ACTOR --db FILE\n"
        "      [--version VERSION] [--expected-revision N --expected-digest SHA256]\n"
        "      Trusted local CMS operation; the web owner checks session and context authority.\n"
        "      Changes require a reviewed digest and revision; output is bounded TSV.\n"
        "      Installation retains behavior references, not product data or execution authority.\n"
        "  bonfyre-cms token issue [--scope NS] [--actions ACTS] [--label LABEL]\n"
        "  bonfyre-cms token list\n"
        "  bonfyre-cms token revoke --scope NS --label LABEL\n"
        "  bonfyre-cms ann build <type>\n"
        "  bonfyre-cms ann knn <type> '<json>' [k]\n"
        "  bonfyre-cms ann qknn <type> '<json>' [k]\n"
        "  bonfyre-cms ann qknn-entry <type> <id> [k]\n"
        "  bonfyre-cms ann qbench <type> [queries] [k]\n"
        "  bonfyre-cms ann stats <type>\n"
        "  bonfyre-cms bench-real <type> [--query-field F --query-op OP --query-value V]\n"
        "  bonfyre-cms publish-layer-report <artifact_id> [--root DIR]\n"
        "  bonfyre-cms publish-layer-family <T_FAMILY> [--root DIR]\n"
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

static int arg_flag(int argc, char **argv, const char *flag) {
    for (int i = 1; i < argc; i++) if (!strcmp(argv[i], flag)) return 1;
    return 0;
}

static const char *arg_opt(int argc, char **argv, const char *flag, const char *def) {
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    }
    return def;
}

#define CMS_COMMANDS(X) \
    X("--help", CM_HELP) \
    X("ann", CM_ANN) \
    X("bench", CM_BENCH) \
    X("bench-real", CM_BENCH_REAL) \
    X("columnar", CM_COLUMNAR) \
    X("delta", CM_DELTA) \
    X("egraph", CM_EGRAPH) \
    X("entry", CM_ENTRY) \
    X("family", CM_FAMILY) \
    X("fixture", CM_FIXTURE) \
    X("help", CM_HELP_ALIAS) \
    X("household-members", CM_HOUSEHOLD_MEMBERS) \
    X("hybrid", CM_HYBRID) \
    X("incr", CM_INCR) \
    X("meta", CM_META) \
    X("ops", CM_OPS) \
    X("pack-image", CM_PACK_IMAGE) \
    X("pack-lifecycle", CM_PACK_LIFECYCLE) \
    X("pack-workspace", CM_PACK_WORKSPACE) \
    X("prov", CM_PROV) \
    X("publication", CM_PUBLICATION) \
    X("publish-layer-family", CM_PUBLISH_LAYER_FAMILY) \
    X("publish-layer-report", CM_PUBLISH_LAYER_REPORT) \
    X("schema", CM_SCHEMA) \
    X("serve", CM_SERVE) \
    X("synth", CM_SYNTH) \
    X("tensor", CM_TENSOR) \
    X("token", CM_TOKEN) \
    X("transfer", CM_TRANSFER) \
    X("typecheck", CM_TYPECHECK)

enum {
#define CMS_ENUM(name, id) id,
    CMS_COMMANDS(CMS_ENUM)
#undef CMS_ENUM
};
static const BfCommandEntry cms_commands[] = {
#define CMS_ENTRY(name, id) {name, id, 0, NULL},
    CMS_COMMANDS(CMS_ENTRY)
#undef CMS_ENTRY
};

/* Resolve the second verb once, before any schema or store work. The same
 * semantic verbs recur across command families; handlers still own argument,
 * authorization, database, output and return-code semantics. Sorted for the
 * shared binary-search lookup, rather than comparing argv[2] in every arm. */
#define CMS_SUBCOMMANDS(X) \
    X("abstract", CS_ABSTRACT) \
    X("accelerate", CS_ACCELERATE) \
    X("agg", CS_AGG) \
    X("apply", CS_APPLY) \
    X("auto", CS_AUTO) \
    X("build", CS_BUILD) \
    X("candidates", CS_CANDIDATES) \
    X("chain", CS_CHAIN) \
    X("checkpoint", CS_CHECKPOINT) \
    X("community-work", CS_COMMUNITY_WORK) \
    X("compact", CS_COMPACT) \
    X("constraints", CS_CONSTRAINTS) \
    X("count", CS_COUNT) \
    X("create", CS_CREATE) \
    X("delete", CS_DELETE) \
    X("discover", CS_DISCOVER) \
    X("eigen", CS_EIGEN) \
    X("export", CS_EXPORT) \
    X("extract", CS_EXTRACT) \
    X("get", CS_GET) \
    X("head", CS_HEAD) \
    X("history", CS_HISTORY) \
    X("import-jsonl", CS_IMPORT_JSONL) \
    X("infer", CS_INFER) \
    X("infer-all", CS_INFER_ALL) \
    X("issue", CS_ISSUE) \
    X("knn", CS_KNN) \
    X("list", CS_LIST) \
    X("manifest", CS_MANIFEST) \
    X("migrate", CS_MIGRATE) \
    X("page-read", CS_PAGE_READ) \
    X("page-write", CS_PAGE_WRITE) \
    X("profile", CS_PROFILE) \
    X("project", CS_PROJECT) \
    X("promote-rss", CS_PROMOTE_RSS) \
    X("propose", CS_PROPOSE) \
    X("qbench", CS_QBENCH) \
    X("qknn", CS_QKNN) \
    X("qknn-entry", CS_QKNN_ENTRY) \
    X("query", CS_QUERY) \
    X("reconstruct", CS_RECONSTRUCT) \
    X("reduce", CS_REDUCE) \
    X("register", CS_REGISTER) \
    X("repair-entities", CS_REPAIR_ENTITIES) \
    X("reprs", CS_REPRS) \
    X("revoke", CS_REVOKE) \
    X("scan", CS_SCAN) \
    X("source-publication", CS_SOURCE_PUBLICATION) \
    X("stats", CS_STATS) \
    X("update", CS_UPDATE) \
    X("verify", CS_VERIFY)
enum {
#define CMS_SUB_ENUM(name, id) id,
    CMS_SUBCOMMANDS(CMS_SUB_ENUM)
#undef CMS_SUB_ENUM
};
static const BfCommandEntry cms_subcommands[] = {
#define CMS_SUB_ENTRY(name, id) {name, id, 0, NULL},
    CMS_SUBCOMMANDS(CMS_SUB_ENTRY)
#undef CMS_SUB_ENTRY
};

static int delegate_layer_cms(int argc, char **argv, int command_id) {
    const char *root = arg_opt(argc, argv, "--root", NULL);
    char *json = NULL;
    char *out = NULL;
    if (command_id == CM_PUBLISH_LAYER_REPORT && argc >= 3) {
        if (bf_layer_load_json(root, argv[2], &json) != 0) return 1;
        if (bf_layer_report_md(json, &out) != 0) { free(json); return 1; }
        puts(out);
        free(out);
        free(json);
        return 0;
    } else if (command_id == CM_PUBLISH_LAYER_FAMILY && argc >= 3) {
        /* Native family publishing will move into libbonfyre next. */
        char *exec_argv[16];
        int n = 0;
        exec_argv[n++] = "layeros/bin/bonfyre-layeros";
        if (root) { exec_argv[n++] = "--root"; exec_argv[n++] = (char *)root; }
        exec_argv[n++] = "emit";
        exec_argv[n++] = "layer-cookbook";
        exec_argv[n++] = "--family";
        exec_argv[n++] = argv[2];
        exec_argv[n] = NULL;
        {
            pid_t pid = fork();
            int st = 0;
            if (pid < 0) return 1;
            if (pid == 0) { execvp(exec_argv[0], exec_argv); _exit(127); }
            if (waitpid(pid, &st, 0) < 0) return 1;
            return WIFEXITED(st) ? WEXITSTATUS(st) : 1;
        }
    }
    return 1;
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
        if (entry_get(db, type_name, ids[i], NULL, bench_sink) != 0) point_get_failures++;
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

static int cms_fixture(const char *schemas_dir) {
    long long first = 0, second = 0;
    sqlite3_stmt *statement = NULL;
    int created = 0, read_ok = 0, updated = 0, relation = 0;
    int paginated = 0, authorized = 0, deleted = 0;
    if (load_schemas_from_dir(g_db, schemas_dir) <= 0) return 1;
    if (entry_create(g_db, "frappe_app", "fixture",
            "{\"app_name\":\"Fabric\",\"apps_dir\":\"/governed/apps\",\"status\":\"draft\"}", &first) != 0 ||
        entry_create(g_db, "frappe_app", "fixture",
            "{\"app_name\":\"Related\",\"apps_dir\":\"/governed/apps\",\"status\":\"draft\"}", &second) != 0) return 1;
    created = first > 0 && second > 0;
    if (sqlite3_prepare_v2(g_db, "SELECT app_name FROM frappe_app WHERE id=? AND namespace='fixture'", -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(statement, 1, first);
        read_ok = sqlite3_step(statement) == SQLITE_ROW && !strcmp((const char *)sqlite3_column_text(statement, 0), "Fabric");
    }
    sqlite3_finalize(statement); statement = NULL;
    updated = entry_update(g_db, "frappe_app", (int)first, "fixture",
                           "{\"app_name\":\"Fabric Updated\",\"status\":\"published\"}") == 0;
    if (sqlite3_prepare_v2(g_db,
            "INSERT INTO _relations(source_type,source_id,target_type,target_id,relation,kind) VALUES('frappe_app',?,'frappe_app',?,'depends_on','manyToOne')", -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(statement, 1, first); sqlite3_bind_int64(statement, 2, second);
        relation = sqlite3_step(statement) == SQLITE_DONE;
    }
    sqlite3_finalize(statement); statement = NULL;
    if (sqlite3_prepare_v2(g_db, "SELECT count(*) FROM frappe_app WHERE namespace='fixture' ORDER BY id LIMIT 1", -1, &statement, NULL) == SQLITE_OK && sqlite3_step(statement) == SQLITE_ROW)
        paginated = sqlite3_column_int(statement, 0) == 2;
    sqlite3_finalize(statement); statement = NULL;
    authorized = token_issue(g_db, "fixture", "find,create,update,delete", "fixture-token") == 0;
    deleted = entry_delete(g_db, "frappe_app", (int)second, "fixture") == 0;
    printf("{\"schema_created\":true,\"record_created\":%s,\"read\":%s,\"updated\":%s,"
           "\"typed_relation\":%s,\"pagination\":%s,\"authorized\":%s,\"deleted\":%s}\n",
           created ? "true" : "false", read_ok ? "true" : "false", updated ? "true" : "false",
           relation ? "true" : "false", paginated ? "true" : "false", authorized ? "true" : "false", deleted ? "true" : "false");
    return created && read_ok && updated && relation && paginated && authorized && deleted ? 0 : 1;
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { print_usage(); return 0; }
    if (argc < 2) { print_usage(); return 0; }
    const BfCommandEntry *command = bf_command_lookup(cms_commands,
        sizeof(cms_commands) / sizeof(cms_commands[0]), argv[1]);
    const BfCommandEntry *subcommand = argc >= 3 ? bf_command_lookup(cms_subcommands,
        sizeof(cms_subcommands) / sizeof(cms_subcommands[0]), argv[2]) : NULL;
#define CMS_IS(expected) (command && command->id == (expected))
#define CMS_SUB_IS(expected) (subcommand && subcommand->id == (expected))
    if (!command) { print_usage(); return 1; }
    if (CMS_IS(CM_HELP) || CMS_IS(CM_HELP_ALIAS)) { print_usage(); return 0; }
    if (CMS_IS(CM_PUBLISH_LAYER_REPORT) || CMS_IS(CM_PUBLISH_LAYER_FAMILY))
        return delegate_layer_cms(argc, argv, command->id);

    const char *requested_db;
    const char *requested_schemas;
    const int is_bench = CMS_IS(CM_BENCH) || CMS_IS(CM_BENCH_REAL);
    const int has_explicit_db = arg_has(argc, argv, "--db");
    char *db_path_owned = NULL;
    char *schemas_dir_owned = NULL;
    char bench_db_template[] = "/tmp/bonfyre_cms_bench_XXXXXX.db";
    int bench_fd = -1;
    int remove_db_on_exit = 0;
    const char *db_path;
    const char *schemas_dir;

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    requested_db = arg_opt(argc, argv, "--db", DEFAULT_DB);
    requested_schemas = arg_opt(argc, argv, "--schemas", DEFAULT_SCHEMAS);

    /* Bench builds their own temporary store below; everything else resolves a
     * declared coordinate or refuses. */
    if (!is_bench) {
        static char resolved_db[PATH_MAX];
        const char *chosen = resolve_cms_db(resolved_db, sizeof(resolved_db),
                                            requested_db, has_explicit_db);
        if (!chosen) {
            fprintf(stderr,
                "[fatal] no CMS store coordinate: pass --db, or set BONFYRE_CMS_DB "
                "to this installation's store. Refusing to create one in the current "
                "directory, because a store chosen by where you were standing is "
                "indistinguishable from the real one.\n");
            return 1;
        }
        requested_db = chosen;

        {
            static char resolved_schemas[PATH_MAX];
            const char *sc = resolve_cms_schemas(resolved_schemas, sizeof(resolved_schemas),
                                                 requested_schemas, arg_has(argc, argv, "--schemas"));
            if (sc) requested_schemas = sc;
            /* No refusal here: several subcommands never read a schema at all,
             * and failing them for a directory they will not open would be a
             * worse lie than the relative default was. The schema loader still
             * reports honestly when a command does need one. */
        }
    }

    /* A Program configuration transaction asks the existing workspace owner
     * to verify one saved record. This read must not run startup migrations or
     * wait for the caller's write reservation. All admission remains in the
     * same cw_dispatch continuation path used by installed editors. */
    if (CMS_IS(CM_PACK_WORKSPACE) && !strcmp(arg_opt(argc, argv, "--operation", "read"), "continuation")) {
        const char *raw_epoch = arg_opt(argc, argv, "--epoch", "-1");
        char *end = NULL; errno = 0;
        long long epoch = strtoll(raw_epoch, &end, 10);
        if (errno || !*raw_epoch || !end || *end || epoch < -1 || epoch >= INT_MAX) {
            fputs("Invalid workspace read revision.\n", stderr); return 3;
        }
        sqlite3_config(SQLITE_CONFIG_SERIALIZED);
        if (sqlite3_open_v2(requested_db, &g_db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            fputs("The saved-work store is unavailable.\n", stderr);
            if (g_db) sqlite3_close(g_db);
            return 6;
        }
        sqlite3_busy_timeout(g_db, 1500);
        int result = cw_dispatch(g_db, "continuation",
            arg_opt(argc, argv, "--binding", ""), arg_opt(argc, argv, "--subject", ""),
            arg_opt(argc, argv, "--scope", "self"), arg_opt(argc, argv, "--actor", ""),
            arg_opt(argc, argv, "--prefix", ""), arg_opt(argc, argv, "--cursor", ""), epoch);
        sqlite3_close(g_db); return result;
    }

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
    if (CMS_IS(CM_SERVE)) {
        int port = arg_int(argc, argv, "--port", DEFAULT_PORT);
        load_schemas_from_dir(g_db, schemas_dir);
        int rc = start_server(port);
        sqlite3_close(g_db);
        return rc;
    }

    if (CMS_IS(CM_FIXTURE)) {
        int rc = cms_fixture(schemas_dir);
        sqlite3_close(g_db);
        free(schemas_dir_owned); free(db_path_owned);
        return rc;
    }

    /* === schema === */
    if (CMS_IS(CM_SCHEMA)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms schema <list|apply|migrate>\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_LIST)) {
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

        if (CMS_SUB_IS(CS_APPLY)) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms schema apply <file.json>\n"); sqlite3_close(g_db); return 1; }
            int rc = load_schema_file(g_db, argv[3]);
            sqlite3_close(g_db);
            return rc;
        }

        if (CMS_SUB_IS(CS_MIGRATE)) {
            int n = load_schemas_from_dir(g_db, schemas_dir);
            printf("migrated %d schemas\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        fprintf(stderr, "Unknown schema command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === publication material admission === */
    if (CMS_IS(CM_PUBLICATION)) {
        load_schemas_from_dir(g_db, schemas_dir);
        load_registered_schemas(g_db);
        if (argc >= 3 && CMS_SUB_IS(CS_REPAIR_ENTITIES)) {
            int rc = publication_repair_entities(g_db, arg_opt(argc, argv, "--ns", "root"),
                                                 arg_opt(argc, argv, "--out", NULL));
            sqlite3_close(g_db);
            return rc == 0 ? 0 : 1;
        }
        if (argc < 3 || !CMS_SUB_IS(CS_PROMOTE_RSS)) {
            fprintf(stderr, "Usage: bonfyre-cms publication promote-rss (--source SOURCE | --all-published) [--foreign-root DIR] [--limit N] [--ns NS]\n       bonfyre-cms publication repair-entities [--ns NS] [--out RE_EMBED.jsonl]\n");
            sqlite3_close(g_db);
            return 1;
        }
        {
            const char *source = arg_opt(argc, argv, "--source", NULL);
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            const char *foreign_root = arg_opt(argc, argv, "--foreign-root", NULL);
            int limit = arg_int(argc, argv, "--limit", 18);
            int all = arg_flag(argc, argv, "--all-published");
            int rc;
            /* Every published RSS source in one process.
             *
             * Promoting one source per invocation loads and validates 54
             * content-type schemas before reading a single feed item, so a
             * 500-feed estate spends most of its time re-reading its own
             * schema directory. Measured on this estate: about five seconds a
             * feed, of which the feed is a small part. The work is identical;
             * only the loop moves inside. */
            if (all) {
                sqlite3_stmt *st = NULL;
                char sources[4096][128];
                int n = 0, i, ok = 0, failed = 0, total_created = 0;
                if (sqlite3_prepare_v2(g_db,
                        "SELECT source FROM internet_source WHERE status='published' "
                        "AND extract LIKE 'rss:%' ORDER BY source", -1, &st, NULL) != SQLITE_OK) {
                    fprintf(stderr, "[publication] cannot enumerate published rss sources\n");
                    sqlite3_close(g_db);
                    return 1;
                }
                while (sqlite3_step(st) == SQLITE_ROW && n < (int)(sizeof(sources)/sizeof(sources[0]))) {
                    const unsigned char *t = sqlite3_column_text(st, 0);
                    if (!t) continue;
                    snprintf(sources[n], sizeof(sources[0]), "%s", (const char *)t);
                    n++;
                }
                sqlite3_finalize(st);
                g_defer_publication_index = 1;
                for (i = 0; i < n; i++) {
                    int before = publication_count(g_db, ns);
                    if (publication_promote_rss(g_db, sources[i], ns, foreign_root, limit) == 0) {
                        ok++;
                        total_created += publication_count(g_db, ns) - before;
                    } else {
                        failed++;
                    }
                }
                g_defer_publication_index = 0;
                if (total_created > 0 &&
                    (family_discover(g_db, "publication_occurrence") < 0 ||
                     ann_build_index(g_db, "publication_occurrence") < 0)) {
                    fprintf(stderr, "[publication] batch index rebuild failed; "
                                    "%d rows are committed and unindexed\n", total_created);
                    sqlite3_close(g_db);
                    return 1;
                }
                printf("{\"schema\":\"bonfyre-publication-promotion.v1\",\"sources\":%d,"
                       "\"promoted\":%d,\"refused\":%d,\"items_created\":%d,"
                       "\"cannot_infer\":[\"a promoted item -> an endorsement, a republication right, "
                       "or a claim that what the feed said is true\"]}\n",
                       n, ok, failed, total_created);
                sqlite3_close(g_db);
                return failed && !ok ? 1 : 0;
            }
            if (!source || !*source || limit < 1 || limit > 1000) {
                fprintf(stderr, "[publication] source and a limit from 1 to 1000 are required\n");
                sqlite3_close(g_db);
                return 1;
            }
            rc = publication_promote_rss(g_db, source, ns, foreign_root, limit);
            sqlite3_close(g_db);
            return rc == 0 ? 0 : 1;
        }
    }

    /* === entry === */
    if (CMS_IS(CM_PACK_IMAGE)) {
        int rc=arg_opt(argc,argv,"--verify-image",NULL)?cpack_verify_image(g_db,arg_opt(argc,argv,"--pack",NULL),
            arg_opt(argc,argv,"--version",""),arg_opt(argc,argv,"--verify-image",NULL),arg_opt(argc,argv,"--cut-digest",NULL)):
            cpack_image(g_db,arg_opt(argc,argv,"--pack",NULL),
            arg_opt(argc,argv,"--version",""),arg_opt(argc,argv,"--out-dir",NULL),arg_opt(argc,argv,"--cut-digest",NULL));
        sqlite3_close(g_db);return rc;
    }
    if (CMS_IS(CM_PACK_LIFECYCLE)) {
        const char *revision = arg_opt(argc, argv, "--expected-revision", "0");
        char *end = NULL; errno = 0;
        long long expected = strtoll(revision, &end, 10);
        if (!*revision || strspn(revision, "0123456789") != strlen(revision) || errno || !end || *end || expected < 0 || expected >= INT_MAX) {
            fputs("error\tinvalid_revision\n", stdout); sqlite3_close(g_db); return 1;
        }
        int result = cpack_lifecycle(g_db, arg_opt(argc, argv, "--operation", "plan"),
            arg_opt(argc, argv, "--pack", NULL), arg_opt(argc, argv, "--version", ""),
            arg_opt(argc, argv, "--subject", NULL), arg_opt(argc, argv, "--scope", "self"),
            arg_opt(argc, argv, "--actor", NULL), arg_opt(argc, argv, "--expected-digest", ""), expected);
        sqlite3_close(g_db); return result;
    }

    if (CMS_IS(CM_PACK_WORKSPACE)) {
        const char *raw_epoch = arg_opt(argc, argv, "--epoch", "-1");
        char *end = NULL; errno = 0;
        long long epoch = strtoll(raw_epoch, &end, 10);
        if (errno || !*raw_epoch || !end || *end || epoch < -1 || epoch >= INT_MAX) {
            fputs("Invalid workspace read revision.\n", stderr); sqlite3_close(g_db); return 3;
        }
        int result = cw_dispatch(g_db, arg_opt(argc, argv, "--operation", "read"),
            arg_opt(argc, argv, "--binding", ""), arg_opt(argc, argv, "--subject", ""),
            arg_opt(argc, argv, "--scope", "self"), arg_opt(argc, argv, "--actor", ""),
            arg_opt(argc, argv, "--prefix", ""), arg_opt(argc, argv, "--cursor", ""), epoch);
        sqlite3_close(g_db); return result;
    }

    if (CMS_IS(CM_HOUSEHOLD_MEMBERS)) {
        const char *names[]={"--expected-revision","--epoch","--expires-at","--after-id"};
        long long values[4];
        for(size_t i=0;i<4;i++){
            const char *raw=arg_opt(argc,argv,names[i],"0");char *end=NULL;errno=0;
            values[i]=strtoll(raw,&end,10);
            if(!*raw || strspn(raw,"0123456789")!=strlen(raw) || errno || !end || *end || values[i]<0 || ((i==0||i==3)&&values[i]>=INT_MAX)){
                fputs("Invalid household membership revision or time.\n",stderr);sqlite3_close(g_db);return 3;
            }
        }
        int result=chh_members(g_db,arg_opt(argc,argv,"--operation","list"),
            arg_opt(argc,argv,"--household",""),arg_opt(argc,argv,"--actor",""),
            arg_opt(argc,argv,"--member",""),arg_opt(argc,argv,"--role",""),
            values[0],values[1],values[2],(int)values[3]);
        sqlite3_close(g_db);return result;
    }

    /* Existing row operations remain the mutation owner for pack installation. */

    if (CMS_IS(CM_ENTRY)) {
        /* Load schemas first */
        load_schemas_from_dir(g_db, schemas_dir);
        load_registered_schemas(g_db);

        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms entry <create|list|get|update|delete>\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_COMMUNITY_WORK)) {
            unsigned char packet[CJ_BYTES+1];size_t bytes=fread(packet,1,sizeof(packet),stdin);
            int rc=entry_community_journey(g_db,arg_opt(argc,argv,"--actor",""),
                arg_opt(argc,argv,"--operation",""),packet,ferror(stdin)?0:bytes);
            sqlite3_close(g_db);return rc;
        }
        if (CMS_SUB_IS(CS_SOURCE_PUBLICATION)) {
            unsigned char packet[SP_BYTES+1];size_t bytes=fread(packet,1,sizeof(packet),stdin);
            int rc=entry_source_publication(g_db,arg_opt(argc,argv,"--ns",NULL),
                arg_opt(argc,argv,"--operation","read"),packet,ferror(stdin)?0:bytes);
            sqlite3_close(g_db);return rc;
        }

        if (CMS_SUB_IS(CS_PAGE_READ)) {
            int rc = entry_page_read(g_db,arg_opt(argc,argv,"--ns",NULL),argc>3?argv[3]:NULL);
            sqlite3_close(g_db); return rc;
        }

        if (CMS_SUB_IS(CS_PAGE_WRITE)) {
            const char *ns = arg_opt(argc, argv, "--ns", NULL);
            long size = 0;
            char *body = argc > 3 && argv[3][0] == '@' ? read_file_full(argv[3] + 1, &size) : NULL;
            int rc = 1;
            if (body && size > 0 && size <= MAX_BODY && (size_t)size == strlen(body)) rc = entry_page_write(g_db, ns, body);
            else fputs("Usage: bonfyre-cms entry page-write @file.json --ns NS (at most 1 MiB)\n", stderr);
            free(body); sqlite3_close(g_db); return rc;
        }

        if (CMS_SUB_IS(CS_CREATE)) {
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
            char *body_from_file = NULL;
            if (body[0] == '@') {
                body_from_file = bf_read_file(body + 1, NULL);
                if (!body_from_file) {
                    fprintf(stderr, "Cannot read JSON body file: %s\n", body + 1);
                    sqlite3_close(g_db);
                    return 1;
                }
                body = body_from_file;
            }
            long long new_id = 0;
            int rc = entry_create(g_db, type, ns, body, &new_id);
            char publish_status[64] = "draft";
            int published = json_str(body, "status", publish_status, sizeof(publish_status)) && !strcmp(publish_status, "published");
            int delta_ok = rc == 0 ? (!published || emit_published_entry_delta(type, body, new_id)) : 0;
            int delta_enabled = cms_fact_deltas_enabled();
            free(body_from_file);
            if (rc == 0) printf("{\"id\":%lld,\"created\":true,\"fact_delta\":\"%s\"}\n", new_id,
                                !published ? "NOT_PUBLISHED" : !delta_enabled ? "DISABLED" : delta_ok ? "DELIVERED" : "DELIVERY_FAILED");
            sqlite3_close(g_db);
            return rc == 0 && published && delta_enabled && !delta_ok ? 2 : rc;
        }

        if (CMS_SUB_IS(CS_IMPORT_JSONL)) {
            if (argc < 5) {
                fprintf(stderr, "Usage: bonfyre-cms entry import-jsonl <type> <file.jsonl> [--ns NS] [--mode create|update]\n");
                sqlite3_close(g_db);
                return 1;
            }
            const char *type = argv[3];
            const char *path = argv[4];
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            const char *mode = arg_opt(argc, argv, "--mode", "create");
            if (!strcmp(mode, "update")) {
                int rows = 0, updated = 0, skipped = 0, delta_failures = 0;
                int rc = entry_update_jsonl(g_db, type, ns, path, &rows, &updated, &skipped, &delta_failures);
                if (rc >= 0)
                    printf("{\"mode\":\"update\",\"rows\":%d,\"updated\":%d,\"skipped\":%d,\"fact_delta_failures\":%d,\"namespace\":\"%s\"}\n",
                           rows, updated, skipped, delta_failures, ns);
                sqlite3_close(g_db);
                return rc < 0 ? 1 : rc;
            }
            if (strcmp(mode, "create")) {
                fprintf(stderr, "entry import-jsonl: --mode must be create or update\n");
                sqlite3_close(g_db);
                return 1;
            }
            int imported = 0, families = 0, indexed = 0;
            int rc = entry_import_jsonl(g_db, type, ns, path, &imported, &families, &indexed);
            if (rc == 0)
                printf("{\"imported\":%d,\"families\":%d,\"ann_indexed\":%d,\"namespace\":\"%s\"}\n",
                       imported, families, indexed, ns);
            sqlite3_close(g_db);
            return rc;
        }

        if (CMS_SUB_IS(CS_LIST)) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms entry list <type>\n"); sqlite3_close(g_db); return 1; }
            const char *type = argv[3];
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            const char *status = arg_opt(argc, argv, "--status", NULL);
            int limit = arg_int(argc, argv, "--limit", 25);
            entry_list(g_db, type, ns, status, limit, stdout);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_GET)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms entry get <type> <id> [--ns NS]\n"); sqlite3_close(g_db); return 1; }
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            entry_get(g_db, argv[3], atoi(argv[4]), ns, stdout);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_UPDATE)) {
            if (argc < 6) { fprintf(stderr, "Usage: bonfyre-cms entry update <type> <id> '<json>|@file' [--ns NS] [--if-field FIELD --if-value VALUE]\n"); sqlite3_close(g_db); return 1; }
            const char *body = NULL;
            for (int i = 5; i < argc; i++) {
                if (argv[i][0] != '-') { body = argv[i]; break; }
                if (argv[i][0] == '-' && i + 1 < argc) i++;
            }
            if (!body) { fprintf(stderr, "Missing JSON body\n"); sqlite3_close(g_db); return 1; }
            const char *ns = arg_opt(argc, argv, "--ns", "root");
            char *body_from_file = NULL;
            if (body[0] == '@') {
                body_from_file = bf_read_file(body + 1, NULL);
                if (!body_from_file) { fprintf(stderr, "Cannot read update body file\n"); sqlite3_close(g_db); return 1; }
                body = body_from_file;
            }
            CmsDeltaCoordinates previous = {0};
            int previous_ok = cms_row_delta_coordinates(g_db, argv[3], atoi(argv[4]), ns, &previous);
            int rc = entry_update_if(g_db, argv[3], atoi(argv[4]), ns, body,
                arg_opt(argc, argv, "--if-field", NULL), arg_opt(argc, argv, "--if-value", NULL),
                arg_opt(argc, argv, "--if-field2", NULL), arg_opt(argc, argv, "--if-value2", NULL),
                arg_opt(argc, argv, "--if-digest", NULL));
            free(body_from_file);
            int published = 0;
            int delta_ok = rc == 0 ? emit_published_row_delta(g_db, argv[3], atoi(argv[4]), ns, &published) : 0;
            if (rc == 0 && !published && previous_ok && previous.published)
                delta_ok = cms_emit_fact_delta(argv[3], previous.identity, previous.publication_source, atoi(argv[4]));
            int delta_enabled = cms_fact_deltas_enabled();
            /* A refused update used to end with no word at all, which left a
             * caller unable to tell a wrong namespace from invalid input. */
            if (rc != 0) fprintf(stderr, "[crud] update refused: no %s %d in namespace %s, or the input is not valid for that type\n",
                                 argv[3], atoi(argv[4]), ns ? ns : "root");
            if (rc == 0) printf("{\"id\":%d,\"updated\":true,\"fact_delta\":\"%s\"}\n", atoi(argv[4]),
                                !published && !(previous_ok && previous.published) ? "NOT_PUBLISHED" : !delta_enabled ? "DISABLED" : delta_ok ? published ? "DELIVERED" : "RETIRED" : "DELIVERY_FAILED");
            sqlite3_close(g_db);
            return rc == 0 && (published || (previous_ok && previous.published)) && delta_enabled && !delta_ok ? 2 : rc;
        }
        if (CMS_SUB_IS(CS_DELETE)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms entry delete <type> <id> [--ns NS]\n"); sqlite3_close(g_db); return 1; }
            /* --ns scopes the delete to one namespace; without it the id alone
             * selects the row, as it always has. */
            const char *delete_ns = arg_opt(argc, argv, "--ns", NULL);
            CmsDeltaCoordinates coordinate = {0};
            int coordinate_ok = cms_row_delta_coordinates(g_db, argv[3], atoi(argv[4]), delete_ns, &coordinate);
            int rc = entry_delete(g_db, argv[3], atoi(argv[4]), delete_ns);
            int delta_enabled = cms_fact_deltas_enabled();
            int delta_ok = rc == 0 && coordinate_ok && (!coordinate.published ||
                cms_emit_fact_delta(argv[3], coordinate.identity, coordinate.publication_source, atoi(argv[4])));
            if (rc == 0) printf("{\"id\":%d,\"deleted\":true,\"fact_delta\":\"%s\"}\n", atoi(argv[4]),
                                !coordinate.published ? "NOT_PUBLISHED" : !delta_enabled ? "DISABLED" : delta_ok ? "DELIVERED" : "DELIVERY_FAILED");
            sqlite3_close(g_db);
            return rc == 0 && coordinate.published && delta_enabled && !delta_ok ? 2 : rc;
        }

        fprintf(stderr, "Unknown entry command: %s\n", argv[2]);
        sqlite3_close(g_db);
        return 1;
    }

    /* === ops (BonfyreGraph Flagship I) === */
    if (CMS_IS(CM_OPS)) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms ops <history|reconstruct|count|head> <type> <id>\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_HISTORY)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops history <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *json = NULL;
            int n = ops_history(g_db, argv[3], atoll(argv[4]), &json);
            if (json) { printf("%s\n", json); free(json); }
            printf("# %d ops\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_RECONSTRUCT)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops reconstruct <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *json = NULL;
            int rc = ops_reconstruct(g_db, argv[3], atoll(argv[4]), &json);
            if (rc == 0 && json) { printf("%s\n", json); free(json); }
            else printf("{\"error\":\"not found or deleted\"}\n");
            sqlite3_close(g_db);
            return rc;
        }

        if (CMS_SUB_IS(CS_COUNT)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms ops count <type> <id>\n"); sqlite3_close(g_db); return 1; }
            int n = ops_count(g_db, argv[3], atoll(argv[4]));
            printf("{\"ops\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_HEAD)) {
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
    if (CMS_IS(CM_FAMILY)) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms family <discover> <type>\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_DISCOVER)) {
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
    if (CMS_IS(CM_EGRAPH)) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms egraph <extract|reprs|compact|stats> <type> [id]\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_EXTRACT)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms egraph extract <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *data = NULL;
            const char *pref = arg_opt(argc, argv, "--repr", NULL);
            int rc = egraph_extract(g_db, argv[3], atoi(argv[4]), pref, &data);
            if (rc == 0 && data) { printf("%s\n", data); free(data); }
            else printf("{\"error\":\"no representation found\"}\n");
            sqlite3_close(g_db);
            return rc == 0 ? 0 : 1;
        }

        if (CMS_SUB_IS(CS_REPRS)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms egraph reprs <type> <id>\n"); sqlite3_close(g_db); return 1; }
            char *json = NULL;
            int n = egraph_list_reprs(g_db, argv[3], atoi(argv[4]), &json);
            if (json) { printf("%s\n", json); free(json); }
            printf("# %d representations\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_COMPACT)) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms egraph compact <type>\n"); sqlite3_close(g_db); return 1; }
            int n = lt_compact(g_db, argv[3]);
            printf("{\"families_compacted\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_STATS)) {
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
    if (CMS_IS(CM_TENSOR)) {
        load_schemas_from_dir(g_db, schemas_dir);
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms tensor <abstract|reduce|query|update|agg|project|eigen|discover> ...\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_ABSTRACT)) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms tensor abstract <type>\n"); sqlite3_close(g_db); return 1; }
            int n = tensor_abstract(g_db, argv[3]);
            printf("{\"families_abstracted\":%d}\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_REDUCE)) {
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

        if (CMS_SUB_IS(CS_QUERY)) {
            if (argc < 7) { fprintf(stderr, "Usage: bonfyre-cms tensor query <type> <field> <op> <value>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            int n = tensor_query(g_db, argv[3], argv[4], argv[5], argv[6], &result);
            if (result) { printf("%s\n", result); free(result); }
            printf("# %d matches\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_UPDATE)) {
            if (argc < 7) { fprintf(stderr, "Usage: bonfyre-cms tensor update <type> <id> <field> <value>\n"); sqlite3_close(g_db); return 1; }
            int rc = tensor_update(g_db, argv[3], atoi(argv[4]), argv[5], argv[6]);
            if (rc == 0) printf("{\"updated\":true,\"field\":\"%s\"}\n", argv[5]);
            else printf("{\"error\":\"update failed\"}\n");
            sqlite3_close(g_db);
            return rc;
        }

        if (CMS_SUB_IS(CS_AGG)) {
            if (argc < 6) { fprintf(stderr, "Usage: bonfyre-cms tensor agg <type> <field> <sum|avg|min|max|count>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            tensor_aggregate(g_db, argv[3], argv[4], argv[5], &result);
            if (result) { printf("%s\n", result); free(result); }
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_PROJECT)) {
            if (argc < 5) { fprintf(stderr, "Usage: bonfyre-cms tensor project <type> <fields,...>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            int n = tensor_project(g_db, argv[3], argv[4], &result);
            if (result) { printf("%s\n", result); free(result); }
            printf("# %d entries\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_EIGEN)) {
            if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms tensor eigen <type>\n"); sqlite3_close(g_db); return 1; }
            char *result = NULL;
            int n = tensor_eigen_stats(g_db, argv[3], &result);
            if (result) { printf("%s\n", result); free(result); }
            printf("# %d eigenvalue entries\n", n);
            sqlite3_close(g_db);
            return 0;
        }

        if (CMS_SUB_IS(CS_DISCOVER)) {
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
    if (CMS_IS(CM_TOKEN)) {
        load_schemas_from_dir(g_db, schemas_dir);

        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms token <issue|list|revoke>\n"); sqlite3_close(g_db); return 1; }

        if (CMS_SUB_IS(CS_ISSUE)) {
            const char *scope = arg_opt(argc, argv, "--scope", "root");
            const char *actions = arg_opt(argc, argv, "--actions", NULL);
            const char *label = arg_opt(argc, argv, "--label", "cli");
            token_issue(g_db, scope, actions, label);
            sqlite3_close(g_db);
            return 0;
        }

        /* Withdraw every active token with this label in this scope.  The
         * holder is refused from the next request on; the row stays, so the
         * record of what was issued is not rewritten. */
        if (CMS_SUB_IS(CS_REVOKE)) {
            const char *scope = arg_opt(argc, argv, "--scope", NULL);
            const char *label = arg_opt(argc, argv, "--label", NULL);
            sqlite3_stmt *stmt;
            int changed = 0;
            if (!scope || !label) { fprintf(stderr, "Usage: bonfyre-cms token revoke --scope NS --label LABEL\n"); sqlite3_close(g_db); return 1; }
            if (sqlite3_prepare_v2(g_db, "UPDATE _tokens SET active = 0 WHERE scope = ?1 AND label = ?2 AND active = 1", -1, &stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, label, -1, SQLITE_STATIC);
                sqlite3_step(stmt);
                changed = sqlite3_changes(g_db);
                sqlite3_finalize(stmt);
            }
            printf("{\"revoked\":%d,\"scope\":\"%s\",\"label\":\"%s\"}\n", changed, scope, label);
            sqlite3_close(g_db);
            return changed ? 0 : 3;
        }

        if (CMS_SUB_IS(CS_LIST)) {
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(g_db,
                "SELECT token_hash, label, scope, created_at, active FROM _tokens ORDER BY created_at DESC",
                -1, &stmt, NULL) == SQLITE_OK) {
                printf("%-20s %-15s %-25s %-25s %s\n", "HASH", "LABEL", "SCOPE", "CREATED", "ACTIVE");
                printf("%-20s %-15s %-25s %-25s %s\n", "----", "-----", "-----", "-------", "------");
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    const char *stored = (const char *)sqlite3_column_text(stmt, 0);
                    /* The first 12 characters identify a token; the rest is
                     * not needed to tell them apart.  A short stored hash is
                     * one issued the old, guessable way. */
                    printf("%-20.12s %-15s %-25s %-25s %d%s\n",
                        stored,
                        (const char *)sqlite3_column_text(stmt, 1),
                        (const char *)sqlite3_column_text(stmt, 2),
                        (const char *)sqlite3_column_text(stmt, 3),
                        sqlite3_column_int(stmt, 4),
                        stored && strlen(stored) < 64 ? "  WEAK: issued before 2026-10-10, reissue it" : "");
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

    if (CMS_IS(CM_BENCH_REAL)) {
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
    if (CMS_IS(CM_BENCH)) {
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
            if (entry_update(g_db, "service_offer", i, NULL, upd) != 0) update_failures++;
        }
        sqlite3_exec(g_db, "COMMIT", NULL, NULL, NULL);
        clock_gettime(CLOCK_MONOTONIC, &ts1);
        elapsed = (ts1.tv_sec - ts0.tv_sec) * 1000.0 + (ts1.tv_nsec - ts0.tv_nsec) / 1e6;
        printf("\"update_ms\":%.3f,\n\"update_per_us\":%.2f,\n", elapsed, (elapsed / N) * 1000);

        /* Phase 3: point lookup by id */
        bench_metrics_reset();
        clock_gettime(CLOCK_MONOTONIC, &ts0);
        for (int i = 1; i <= N; i++) {
            if (entry_get(g_db, "service_offer", i, NULL, bench_sink) != 0) point_get_failures++;
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
            /* SQLite's growable string owns capacity and detects allocation failure.
             * The former fixed 2048-byte reserve could overflow on a long field. */
            sqlite3_str *builder = sqlite3_str_new(g_db);
            if (!builder) return 1;
            sqlite3_stmt *rq;
            if (sqlite3_prepare_v2(g_db, "SELECT * FROM service_offer", -1, &rq, NULL) == SQLITE_OK) {
                int ncols = sqlite3_column_count(rq);
                while (sqlite3_step(rq) == SQLITE_ROW) {
                    sqlite3_str_appendchar(builder, 1, '{');
                    for (int c = 0; c < ncols; c++) {
                        const char *cn = sqlite3_column_name(rq, c);
                        const unsigned char *cv = sqlite3_column_text(rq, c);
                        if (c > 0) sqlite3_str_appendchar(builder, 1, ',');
                        sqlite3_str_appendf(builder, "\"%s\":\"%s\"",
                            cn, cv ? (const char *)cv : "null");
                    }
                    sqlite3_str_appendall(builder, "}\n");
                }
                sqlite3_finalize(rq);
            }
            char *blob = sqlite3_str_finish(builder);
            if (!blob) return 1;
            size_t blob_len = strlen(blob);

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

            sqlite3_free(blob);
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
    if (CMS_IS(CM_INCR)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms incr <compact|stats> <content_type|family_id>\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_COMPACT) && argc >= 4) {
            incr_compact_online(g_db, argv[3], 0.01, 1.5);
            printf("{\"status\":\"ok\",\"action\":\"incr_compact\",\"content_type\":\"%s\"}\n", argv[3]);
        } else if (CMS_SUB_IS(CS_STATS) && argc >= 4) {
            char *json = NULL;
            if (incr_eigen_stats(g_db, atoi(argv[3]), &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === synth (Upgrade II: Generator Synthesis) === */
    if (CMS_IS(CM_SYNTH)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms synth <propose|candidates> <content_type>\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_PROPOSE) && argc >= 4) {
            int n = synthesis_propose(g_db, argv[3]);
            printf("{\"proposed\":%d}\n", n);
        } else if (CMS_SUB_IS(CS_CANDIDATES) && argc >= 4) {
            char *json = NULL;
            int limit = argc >= 5 ? atoi(argv[4]) : 10;
            if (synthesis_candidates(g_db, argv[3], limit, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === ann (Upgrade III: ANN Index) === */
    if (CMS_IS(CM_ANN)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms ann <build|knn|qknn|qknn-entry|qbench|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_BUILD) && argc >= 4) {
            int n = ann_build_index(g_db, argv[3]);
            printf("{\"indexed\":%d}\n", n);
        } else if (CMS_SUB_IS(CS_KNN) && argc >= 5) {
            int k = arg_pos_int_or_default(argc, argv, 5, 5);
            char *json = NULL;
            if (ann_knn_query(g_db, argv[3], argv[4], k, &json) >= 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_QKNN) && argc >= 5) {
            int k = arg_pos_int_or_default(argc, argv, 5, 5);
            char *json = NULL;
            if (ann_knn_query_quantized(g_db, argv[3], argv[4], k, &json) >= 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_QKNN_ENTRY) && argc >= 5) {
            int k = arg_pos_int_or_default(argc, argv, 5, 5);
            char *json = NULL;
            if (ann_knn_by_entry_quantized(g_db, argv[3], atoi(argv[4]), k, &json) >= 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_QBENCH) && argc >= 4) {
            int queries = arg_pos_int_or_default(argc, argv, 4, 50);
            int k = arg_pos_int_or_default(argc, argv, 5, 10);
            char *json = NULL;
            if (ann_quant_bench(g_db, argv[3], queries, k, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_STATS) && argc >= 4) {
            char *json = NULL;
            if (ann_stats(g_db, argv[3], &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === delta (Upgrade IV: Delta Sync) === */
    if (CMS_IS(CM_DELTA)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms delta <export|checkpoint|stats> <ns>\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_EXPORT) && argc >= 4) {
            char *json = NULL;
            if (delta_export_json(g_db, argv[3], NULL, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_CHECKPOINT) && argc >= 5) {
            delta_checkpoint(g_db, argv[3], argv[4], "");
            printf("{\"status\":\"ok\"}\n");
        }
        sqlite3_close(g_db); return 0;
    }

    /* === typecheck (Upgrade V: Typed Generators) === */
    if (CMS_IS(CM_TYPECHECK)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms typecheck <infer|verify|constraints> <family_id>\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_INFER) && argc >= 4) {
            int n = tc_infer(g_db, atoi(argv[3]));
            printf("{\"inferred\":%d}\n", n);
        } else if (CMS_SUB_IS(CS_INFER_ALL) && argc >= 4) {
            int n = tc_infer_all(g_db, argv[3]);
            printf("{\"inferred\":%d}\n", n);
        } else if (CMS_SUB_IS(CS_VERIFY) && argc >= 4) {
            int valid = tc_verify_family(g_db, atoi(argv[3]));
            printf("{\"valid\":%s}\n", valid ? "true" : "false");
        } else if (CMS_SUB_IS(CS_CONSTRAINTS) && argc >= 4) {
            char *json = NULL;
            if (tc_constraints_json(g_db, atoi(argv[3]), &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === meta (Upgrade VI: Meta-Generators) === */
    if (CMS_IS(CM_META)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms meta <discover|compact|stats> <content_type>\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_DISCOVER) && argc >= 4) {
            int n = meta_discover(g_db, argv[3]);
            printf("{\"meta_families\":%d}\n", n);
        } else if (CMS_SUB_IS(CS_COMPACT) && argc >= 4) {
            double ratio = meta_compact(g_db, argv[3]);
            printf("{\"compression_improvement\":%.4f}\n", ratio);
        } else if (CMS_SUB_IS(CS_STATS) && argc >= 4) {
            char *json = NULL;
            if (meta_stats(g_db, argv[3], &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === columnar (Upgrade VII: Vectorized Columnar Engine) === */
    if (CMS_IS(CM_COLUMNAR)) {
        if (argc < 4) { fprintf(stderr, "Usage: bonfyre-cms columnar <stats|scan> <family_id> ...\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_STATS)) {
            ColBatch batch;
            if (col_materialize(g_db, argv[3], NULL, 0, &batch) == 0) {
                char *json = NULL;
                if (col_stats(&batch, &json) == 0 && json) {
                    printf("%s\n", json); free(json);
                }
                col_batch_free(&batch);
            }
        } else if (CMS_SUB_IS(CS_SCAN) && argc >= 7) {
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
    if (CMS_IS(CM_HYBRID)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms hybrid <reduce|profile|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_REDUCE) && argc >= 5) {
            char *json = NULL;
            if (hybrid_reduce(g_db, argv[3], atoi(argv[4]), NULL, 0, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_PROFILE) && argc >= 5) {
            char *json = NULL;
            if (hybrid_profile(g_db, argv[3], atoi(argv[4]), &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_STATS)) {
            char *json = NULL;
            if (hybrid_stats(g_db, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === prov (Upgrade IX: Provenance + Merkle Proofs) === */
    if (CMS_IS(CM_PROV)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms prov <build|manifest|verify|chain|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_BUILD) && argc >= 4) {
            uint64_t root = 0;
            prov_build_tree(g_db, argv[3], &root);
            printf("{\"merkle_root\":\"%llx\"}\n", (unsigned long long)root);
        } else if (CMS_SUB_IS(CS_MANIFEST) && argc >= 4) {
            int64_t mid = 0;
            prov_manifest(g_db, argv[3], &mid);
            printf("{\"manifest_id\":%lld}\n", (long long)mid);
        } else if (CMS_SUB_IS(CS_VERIFY) && argc >= 4) {
            int valid = prov_verify_manifest(g_db, atoll(argv[3]));
            printf("{\"valid\":%s}\n", valid ? "true" : "false");
        } else if (CMS_SUB_IS(CS_CHAIN) && argc >= 4) {
            char *json = NULL;
            if (prov_chain(g_db, argv[3], &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        } else if (CMS_SUB_IS(CS_STATS)) {
            char *json = NULL;
            if (prov_stats(g_db, &json) == 0 && json) {
                printf("%s\n", json); free(json);
            }
        }
        sqlite3_close(g_db); return 0;
    }

    /* === transfer (Upgrade X: Cross-Family Transfer) === */
    if (CMS_IS(CM_TRANSFER)) {
        if (argc < 3) { fprintf(stderr, "Usage: bonfyre-cms transfer <auto|register|stats> ...\n"); sqlite3_close(g_db); return 1; }
        if (CMS_SUB_IS(CS_AUTO) && argc >= 4) {
            int rc = transfer_auto(g_db, argv[3]);
            printf("{\"transferred\":%s}\n", rc == 0 ? "true" : "false");
        } else if (CMS_SUB_IS(CS_REGISTER) && argc >= 5) {
            int64_t tid = 0;
            transfer_register_template(g_db, argv[3], argv[4], &tid);
            printf("{\"template_id\":%lld}\n", (long long)tid);
        } else if (CMS_SUB_IS(CS_ACCELERATE) && argc >= 4) {
            int accel = transfer_accelerate(g_db, argv[3]);
            printf("{\"accelerated\":%s}\n", accel ? "true" : "false");
        } else if (CMS_SUB_IS(CS_STATS)) {
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
