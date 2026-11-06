/*
 * bonfyre-bridge rules | schema-map | map-verify — transformation evidence.
 *
 *   rules       parse the casting rules an upstream tool documents into rule
 *               candidates.  Each rule keeps the exact byte range it came from
 *               in a body that still hashes to its ForeignTwin receipt (or, for
 *               a vendored upstream file in this repository, to the file's own
 *               digest, said as such).
 *   schema-map  read a SQLite source's real schema and apply an authored plan
 *               (renames, value maps, Frappe DocType targets) and the parsed
 *               rules to it: every column gets the rule that typed it or a
 *               named exception.  Optionally writes the rehearsal SQL.
 *   map-verify  read source and rehearsal target, apply the mapping to the
 *               source values, and compare row counts, per-column checksums,
 *               declared target types, constraint counts, carried objects and
 *               the plan's query probes.  --expect re-checks an earlier receipt
 *               and refuses with exit 4 when anything it measured has moved.
 *
 * Sources are opened read-only.  The rehearsal target is read only here; the
 * SQL that builds it is written to a file for the caller to apply to a copy.
 * PostgreSQL is read through psql so a probe runs with the engine's own
 * semantics, not a translator's.
 */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#include <ctype.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_jsonw.h>
#include <bonfyre_frappe.h>

#define MAX_RULES 128
#define MAX_COLS 96
#define MAX_TABS 32
#define MAX_EXC 128

/* ------------------------------------------------------------------ util */

static const char *topt(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static void lower(char *s) { for (; *s; s++) *s = (char)tolower((unsigned char)*s); }

static void trim(char *s) {
    size_t n = strlen(s);
    while (n && isspace((unsigned char)s[n - 1])) s[--n] = 0;
    size_t i = 0;
    while (s[i] && isspace((unsigned char)s[i])) i++;
    if (i) memmove(s, s + i, n - i + 1);
}

static int node_str(const bf_json_node_t *n, char *out, size_t sz) {
    out[0] = 0;
    if (!n || n->type != BF_JSON_STRING) return 0;
    int len = 0; const char *raw = bf_json_get_str(n, &len);
    size_t ul = 0; char *u = bf_json_unescape(raw, len, &ul);
    if (!u) return 0;
    snprintf(out, sz, "%.*s", (int)ul, u);
    free(u);
    return 1;
}

static int jcopy(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k, char *out, size_t sz) {
    const bf_json_node_t *n = o ? bf_json_obj_get(d, o, k) : NULL;
    out[0] = 0;
    if (!n || n->type != BF_JSON_STRING) return 0;
    int len = 0; const char *raw = bf_json_get_str(n, &len);
    size_t ul = 0; char *u = bf_json_unescape(raw, len, &ul);
    if (!u) return 0;
    snprintf(out, sz, "%.*s", (int)ul, u);
    free(u);
    return 1;
}

static int write_file(const char *path, const char *data, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    int ok = fwrite(data, 1, len, f) == len;
    return fclose(f) == 0 && ok;
}

/* A sealed receipt: the digest is the sha256 of the document as it would end
 * without the digest key, so anyone can recompute it from the file. */
static int write_sealed(const char *path, bf_jsonw *w, const char *key, char hex[65]) {
    if (w->oom || w->len < 2 || strncmp(w->buf + w->len - 2, "\n}", 2)) return 0;
    bf_sha256_hex((const uint8_t *)w->buf, w->len, hex);
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    fwrite(w->buf, 1, w->len - 2, f);
    fprintf(f, ",\n  \"%s\": \"%s\"\n}\n", key, hex);
    return fclose(f) == 0;
}

int bridge_verify_sealed(const char *buf, size_t len, const char *key, char hex[65]) {
    char marker[96];
    snprintf(marker, sizeof marker, ",\n  \"%s\": \"", key);
    const char *at = NULL;
    for (const char *p = buf; (p = strstr(p, marker)); p++) at = p;
    if (!at || (size_t)(at - buf) + strlen(marker) + 64 > len) return 0;
    char stated[65];
    memcpy(stated, at + strlen(marker), 64); stated[64] = 0;
    size_t head = (size_t)(at - buf);
    char *body = malloc(head + 3);
    if (!body) return 0;
    memcpy(body, buf, head); memcpy(body + head, "\n}", 2);
    bf_sha256_hex((const uint8_t *)body, head + 2, hex);
    free(body);
    return !strcmp(stated, hex);
}

/* --------------------------------------------------------------- engines */

typedef struct { int ncols, nrows, cap; char **cells; } Rows;

static void rows_free(Rows *r) {
    for (int i = 0; i < r->ncols * r->nrows; i++) free(r->cells[i]);
    free(r->cells); memset(r, 0, sizeof *r);
}

typedef struct { int pg; char path[1024]; char url[1024]; char display[512]; sqlite3 *db; } Eng;

static int eng_open(const char *given, Eng *e) {
    memset(e, 0, sizeof *e);
    const char *u = given;
    if (u && !strncmp(u, "env:", 4)) u = getenv(u + 4);
    if (!u || !*u) return 0;
    if (!strncmp(u, "sqlite:", 7)) {
        snprintf(e->path, sizeof e->path, "%s", u + 7);
        snprintf(e->display, sizeof e->display, "sqlite:%s", u + 7);
        return sqlite3_open_v2(e->path, &e->db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK;
    }
    if (!strncmp(u, "postgres://", 11) || !strncmp(u, "postgresql://", 13)) {
        e->pg = 1;
        snprintf(e->url, sizeof e->url, "%s", u);
        /* The display never carries a password. */
        const char *s = strstr(u, "://") + 3, *at = strchr(s, '@'), *colon = strchr(s, ':');
        if (at && colon && colon < at) snprintf(e->display, sizeof e->display, "postgres://%.*s@%s", (int)(colon - s), s, at + 1);
        else snprintf(e->display, sizeof e->display, "postgres://%s", s);
        return 1;
    }
    return 0;
}

static void eng_close(Eng *e) { if (e->db) sqlite3_close(e->db); e->db = NULL; }

static int sqlite_rows(sqlite3 *db, const char *sql, Rows *r) {
    sqlite3_stmt *st = NULL;
    memset(r, 0, sizeof *r);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "bridge: sqlite: %s\n", sqlite3_errmsg(db)); return 0;
    }
    r->ncols = sqlite3_column_count(st);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        for (int c = 0; c < r->ncols; c++) {
            if (r->nrows * r->ncols + c + 1 > r->cap) {
                int cap = r->cap ? r->cap * 2 : 256;
                char **cc = realloc(r->cells, (size_t)cap * sizeof *cc);
                if (!cc) { sqlite3_finalize(st); return 0; }
                r->cells = cc; r->cap = cap;
            }
            const unsigned char *t = sqlite3_column_type(st, c) == SQLITE_NULL ? NULL : sqlite3_column_text(st, c);
            r->cells[r->nrows * r->ncols + c] = t ? strdup((const char *)t) : NULL;
        }
        r->nrows++;
    }
    sqlite3_finalize(st);
    if (rc != SQLITE_DONE) { fprintf(stderr, "bridge: sqlite: %s\n", sqlite3_errmsg(db)); return 0; }
    return 1;
}

/* psql, unaligned, with control characters as separators and NULL marked. */
static int pg_rows(const Eng *e, const char *sql, Rows *r) {
    int out[2];
    memset(r, 0, sizeof *r);
    if (pipe(out)) return 0;
    pid_t pid = fork();
    if (pid < 0) return 0;
    if (pid == 0) {
        dup2(out[1], 1); close(out[0]); close(out[1]);
        setenv("PGTZ", "UTC", 1);
        execlp("psql", "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-F", "\x1f", "-R", "\x1e",
               "-P", "null=\x1d", "-d", e->url, "-c", sql, (char *)NULL);
        _exit(127);
    }
    close(out[1]);
    size_t used = 0, cap = 1 << 14;
    char *buf = malloc(cap);
    for (;;) {
        if (!buf) break;
        if (used + 4096 > cap) { cap *= 2; char *nb = realloc(buf, cap); if (!nb) { free(buf); buf = NULL; break; } buf = nb; }
        ssize_t n = read(out[0], buf + used, cap - used - 1);
        if (n <= 0) break;
        used += (size_t)n;
    }
    close(out[0]);
    int status = 0;
    waitpid(pid, &status, 0);
    if (!buf || !WIFEXITED(status) || WEXITSTATUS(status)) { free(buf); fprintf(stderr, "bridge: psql refused: %.200s\n", sql); return 0; }
    buf[used] = 0;
    while (used && buf[used - 1] == '\n') buf[--used] = 0;
    if (used && buf[used - 1] == '\x1e') buf[--used] = 0;
    if (!used) { free(buf); return 1; }
    /* Split records, then fields. */
    char *rec = buf;
    for (;;) {
        char *rend = strchr(rec, '\x1e');
        if (rend) *rend = 0;
        int nf = 1;
        for (char *p = rec; *p; p++) if (*p == '\x1f') nf++;
        if (!r->ncols) r->ncols = nf;
        if (nf != r->ncols) { free(buf); rows_free(r); fprintf(stderr, "bridge: a value carried a separator; refusing to guess\n"); return 0; }
        char *f = rec;
        for (int c = 0; c < nf; c++) {
            char *fend = strchr(f, '\x1f');
            if (fend) *fend = 0;
            if (r->nrows * r->ncols + c + 1 > r->cap) {
                int ncap = r->cap ? r->cap * 2 : 256;
                char **cc = realloc(r->cells, (size_t)ncap * sizeof *cc);
                if (!cc) { free(buf); return 0; }
                r->cells = cc; r->cap = ncap;
            }
            r->cells[r->nrows * r->ncols + c] = strcmp(f, "\x1d") ? strdup(f) : NULL;
            f = fend ? fend + 1 : f + strlen(f);
        }
        r->nrows++;
        if (!rend) break;
        rec = rend + 1;
        if (*rec == '\n') rec++;
    }
    free(buf);
    return 1;
}

static int eng_rows(const Eng *e, const char *sql, Rows *r) { return e->pg ? pg_rows(e, sql, r) : sqlite_rows(e->db, sql, r); }

static const char *cell(const Rows *r, int row, int col) { return r->cells[row * r->ncols + col]; }

static long long eng_count(const Eng *e, const char *sql) {
    Rows r; long long v = -1;
    if (eng_rows(e, sql, &r) && r.nrows && cell(&r, 0, 0)) v = atoll(cell(&r, 0, 0));
    rows_free(&r);
    return v;
}

/* ---------------------------------------------------------- canonical form */

enum { K_TEXT, K_INT, K_NUM, K_TS, K_BYTES, K_BOOL };
static const char *KIND[] = {"text", "integer", "number", "timestamp", "bytes", "boolean"};

static int kind_of(const char *type) {
    char t[128]; snprintf(t, sizeof t, "%s", type); lower(t);
    if (strstr(t, "bytea") || strstr(t, "blob")) return K_BYTES;
    if (strstr(t, "bool")) return K_BOOL;
    if (strstr(t, "timestamp") || strstr(t, "datetime") || !strncmp(t, "date", 4)) return K_TS;
    if (strstr(t, "int") || strstr(t, "serial")) return K_INT;
    if (strstr(t, "numeric") || strstr(t, "decimal") || strstr(t, "real") || strstr(t, "double") || strstr(t, "float")) return K_NUM;
    return K_TEXT;
}

static int numeric_text(const char *s) {
    if (!s || !*s) return 0;
    const char *p = s;
    if (*p == '-' || *p == '+') p++;
    int digits = 0, dots = 0;
    for (; *p; p++) { if (isdigit((unsigned char)*p)) digits++; else if (*p == '.') dots++; else return 0; }
    return digits && dots <= 1;
}

static void strip_fraction_zeros(char *s) {
    char *dot = strchr(s, '.');
    if (!dot) return;
    size_t n = strlen(s);
    while (n > (size_t)(dot - s) + 1 && s[n - 1] == '0') s[--n] = 0;
    if (s[n - 1] == '.') s[--n] = 0;
}

static char *canon(int kind, const char *v) {
    if (!v) return NULL;
    char *s = strdup(v);
    if (!s) return NULL;
    switch (kind) {
    case K_INT: case K_NUM:
        trim(s);
        if (s[0] == '+') memmove(s, s + 1, strlen(s));
        if (numeric_text(s)) {
            strip_fraction_zeros(s);
            if (s[0] == '.') { char *t = malloc(strlen(s) + 2); sprintf(t, "0%s", s); free(s); s = t; }
            if (!strcmp(s, "-0")) strcpy(s, "0");
        }
        break;
    case K_TS: {
        size_t n = strlen(s);
        if (n > 10 && s[10] == 'T') s[10] = ' ';
        if (n && s[n - 1] == 'Z') s[--n] = 0;
        if (n > 6 && !strcmp(s + n - 6, "+00:00")) s[n -= 6] = 0;
        if (n > 3 && !strcmp(s + n - 3, "+00")) s[n -= 3] = 0;
        if (n == 10) { char *t = malloc(20); snprintf(t, 20, "%s 00:00:00", s); free(s); s = t; }
        if (strchr(s, '.')) strip_fraction_zeros(s);
        break;
    }
    case K_BYTES: lower(s); break;
    case K_BOOL:
        if (!strcmp(s, "t") || !strcasecmp(s, "true") || !strcmp(s, "1")) strcpy(s, "1");
        else if (!strcmp(s, "f") || !strcasecmp(s, "false") || !strcmp(s, "0")) strcpy(s, "0");
        break;
    default: break;
    }
    return s;
}

/* One value into a digest, with its length so field boundaries cannot merge
 * and NULL distinct from every text. */
static void hash_val(BfSha256 *h, const char *v) {
    char pre[32];
    if (!v) { bf_sha256_update(h, (const uint8_t *)"N;", 2); return; }
    int n = snprintf(pre, sizeof pre, "%zu:", strlen(v));
    bf_sha256_update(h, (const uint8_t *)pre, (size_t)n);
    bf_sha256_update(h, (const uint8_t *)v, strlen(v));
    bf_sha256_update(h, (const uint8_t *)";", 1);
}

static void hash_hex(BfSha256 *h, char hex[65]) { uint8_t d[32]; bf_sha256_final(h, d); bf_sha256_digest_hex(d, hex); }

/* A SQLite read of a column; bytes come back as hex, NULL stays NULL. */
static const char *sl_expr(char *buf, size_t sz, const char *col, int bytes) {
    if (bytes) snprintf(buf, sz, "CASE WHEN \"%s\" IS NULL THEN NULL ELSE hex(\"%s\") END", col, col);
    else snprintf(buf, sz, "\"%s\"", col);
    return buf;
}

/* ================================================================= rules */

typedef struct {
    char key[64], target[96], using_[64], cond[160], note[160];
    int drop_typemod, line, duplicate;
    size_t off, len;
} Rule;

static void rules_usage(void) {
    fputs("usage: bonfyre-bridge rules --format pgloader-casting --receipt RECEIPT.json --out rules.json\n"
          "       bonfyre-bridge rules --format frappe-type-map --file database.py --const-file database.py --out rules.json\n", stderr);
}

static int line_of(const char *b, size_t off) { int n = 1; for (size_t i = 0; i < off; i++) if (b[i] == '\n') n++; return n; }

/* pgloader: lines "type X to Y [drop typemod] [using F]" inside the section
 * headed "Default ... Casting Rules", up to the next underlined heading. */
static int parse_pgloader(const char *b, size_t len, Rule *rules, int *n) {
    const char *sec = NULL;
    for (const char *p = b; p && (p = strstr(p, "Casting Rules")); p++) {
        const char *ls = p; while (ls > b && ls[-1] != '\n') ls--;
        if (!strncmp(ls, "Default ", 8)) { sec = strchr(p, '\n'); break; }
    }
    if (!sec) return 0;
    const char *end = b + len;
    for (const char *ln = sec + 1; ln < end;) {
        const char *le = memchr(ln, '\n', (size_t)(end - ln)); if (!le) le = end;
        /* the next heading: a line of '-' or '^' under a text line */
        if (le + 1 < end && (le[1] == '-' || le[1] == '^') && ln != le && !isspace((unsigned char)*ln)) {
            const char *u = le + 1; while (u < end && (*u == '-' || *u == '^')) u++;
            if (u < end && *u == '\n' && u - le > 4) break;
        }
        const char *p = ln; while (p < le && (*p == ' ' || *p == '\t')) p++;
        if (le - p > 8 && !strncmp(p, "type ", 5) && *n < MAX_RULES) {
            char text[512]; size_t tl = (size_t)(le - p); if (tl >= sizeof text) tl = sizeof text - 1;
            memcpy(text, p, tl); text[tl] = 0; trim(text);
            char *to = strstr(text, " to ");
            if (to) {
                Rule *r = &rules[*n]; memset(r, 0, sizeof *r);
                *to = 0;
                char *src = text + 5; trim(src);
                char *cond = strstr(src, " with "); if (!cond) cond = strstr(src, " when ");
                if (cond) { snprintf(r->cond, sizeof r->cond, "%s", cond + 1); *cond = 0; }
                snprintf(r->key, sizeof r->key, "%s", src); trim(r->key); lower(r->key);
                char *tgt = to + 4, *cut;
                if ((cut = strstr(tgt, " using "))) { snprintf(r->using_, sizeof r->using_, "%s", cut + 7); trim(r->using_); *cut = 0; }
                if ((cut = strstr(tgt, " when "))) { if (!r->cond[0]) snprintf(r->cond, sizeof r->cond, "%s", cut + 1); *cut = 0; }
                if ((cut = strstr(tgt, " drop typemod"))) { r->drop_typemod = 1; *cut = 0; }
                if ((cut = strstr(tgt, " keep typemod"))) *cut = 0;
                trim(tgt);
                snprintf(r->target, sizeof r->target, "%s", tgt);
                r->off = (size_t)(p - b); r->len = (size_t)(le - p);
                while (r->len && isspace((unsigned char)b[r->off + r->len - 1])) r->len--;
                r->line = line_of(b, r->off);
                for (int k = 0; k < *n; k++) if (!strcmp(rules[k].key, r->key) && !rules[k].cond[0] && !r->cond[0]) r->duplicate = 1;
                (*n)++;
            }
        }
        ln = le + 1;
    }
    return *n > 0;
}

/* Frappe: the entries of `self.type_map = {` in a database backend. */
static int parse_frappe(const char *b, size_t len, long varchar_len, Rule *rules, int *n) {
    const char *start = strstr(b, "self.type_map = {");
    if (!start) return 0;
    const char *end = b + len;
    for (const char *ln = strchr(start, '\n') + 1; ln && ln < end;) {
        const char *le = memchr(ln, '\n', (size_t)(end - ln)); if (!le) le = end;
        const char *p = ln; while (p < le && isspace((unsigned char)*p)) p++;
        if (p < le && *p == '}') break;
        char name[64], typ[32], mod[64];
        char text[256]; size_t tl = (size_t)(le - p); if (tl >= sizeof text) tl = sizeof text - 1;
        memcpy(text, p, tl); text[tl] = 0;
        if (sscanf(text, "\"%63[^\"]\": (\"%31[^\"]\", %63[^)])", name, typ, mod) == 3 && *n < MAX_RULES) {
            Rule *r = &rules[*n]; memset(r, 0, sizeof *r);
            snprintf(r->key, sizeof r->key, "%s", name);
            char m[64]; snprintf(m, sizeof m, "%s", mod); trim(m);
            if (m[0] == '"') { memmove(m, m + 1, strlen(m)); char *q = strchr(m, '"'); if (q) *q = 0; }
            else if (!strcmp(m, "self.VARCHAR_LEN")) {
                if (varchar_len <= 0) return -1;
                snprintf(m, sizeof m, "%ld", varchar_len);
                snprintf(r->note, sizeof r->note, "self.VARCHAR_LEN resolved from the const file");
            }
            if (m[0]) snprintf(r->target, sizeof r->target, "%s(%s)", typ, m);
            else snprintf(r->target, sizeof r->target, "%s", typ);
            r->off = (size_t)(p - b); r->len = (size_t)(le - p);
            while (r->len && (b[r->off + r->len - 1] == ',' || isspace((unsigned char)b[r->off + r->len - 1]))) r->len--;
            r->line = line_of(b, r->off);
            (*n)++;
        }
        ln = le + 1;
    }
    return *n > 0;
}

int bridge_rules_main(int argc, char **argv) {
    const char *format = topt(argc, argv, "--format"), *receipt = topt(argc, argv, "--receipt");
    const char *file = topt(argc, argv, "--file"), *constf = topt(argc, argv, "--const-file"), *out = topt(argc, argv, "--out");
    if (!format || !out) { rules_usage(); return 2; }
    static Rule rules[MAX_RULES]; int n = 0;
    char body_sha[65] = "", stated[65] = "", twin[128] = "", url[1024] = "", observed[40] = "", body_path[1024] = "";
    char const_sha[65] = "";
    long varchar_len = 0;
    char *body = NULL; size_t blen = 0;
    int foreign = !strcmp(format, "pgloader-casting");
    if (foreign) {
        if (!receipt) { rules_usage(); return 2; }
        size_t rl = 0; char err[160];
        char *rb = bf_read_file(receipt, &rl);
        bf_json_doc_t *rd = rb ? bf_json_parse(rb, rl, err, sizeof err) : NULL;
        if (!rd) { fprintf(stderr, "rules: receipt unreadable\n"); free(rb); return 3; }
        const bf_json_node_t *root = bf_json_root(rd);
        char policy[32];
        jcopy(rd, root, "policy_decision", policy, sizeof policy);
        const bf_json_node_t *hs = bf_json_obj_get(rd, root, "http_status");
        jcopy(rd, root, "content_sha256", stated, sizeof stated);
        jcopy(rd, root, "twin_id", twin, sizeof twin); jcopy(rd, root, "source_url", url, sizeof url);
        jcopy(rd, root, "observed_at", observed, sizeof observed); jcopy(rd, root, "body_path", body_path, sizeof body_path);
        const char *bp = topt(argc, argv, "--body"); if (bp) snprintf(body_path, sizeof body_path, "%s", bp);
        bf_json_free(rd); free(rb);
        if (strcmp(policy, "allow") || !hs || bf_json_get_int(hs) != 200) { fprintf(stderr, "rules: receipt is not an allowed HTTP 200 observation\n"); return 3; }
        body = bf_read_file(body_path, &blen);
        if (!body) { fprintf(stderr, "rules: retained body missing\n"); return 3; }
        bf_sha256_hex((const uint8_t *)body, blen, body_sha);
        if (strcmp(body_sha, stated)) { fprintf(stderr, "rules: retained body no longer hashes to its receipt\n"); free(body); return 3; }
        if (!parse_pgloader(body, blen, rules, &n)) { fprintf(stderr, "rules: no casting-rule section found\n"); free(body); return 5; }
    } else if (!strcmp(format, "frappe-type-map")) {
        if (!file || !constf) { rules_usage(); return 2; }
        body = bf_read_file(file, &blen);
        if (!body) { fprintf(stderr, "rules: cannot read %s\n", file); return 3; }
        bf_sha256_hex((const uint8_t *)body, blen, body_sha);
        size_t cl = 0; char *cb = bf_read_file(constf, &cl);
        if (!cb) { free(body); return 3; }
        bf_sha256_hex((const uint8_t *)cb, cl, const_sha);
        const char *v = strstr(cb, "VARCHAR_LEN = ");
        if (v) varchar_len = strtol(v + 14, NULL, 10);
        free(cb);
        int rc = parse_frappe(body, blen, varchar_len, rules, &n);
        if (rc <= 0) { fprintf(stderr, rc < 0 ? "rules: VARCHAR_LEN not found in the const file\n" : "rules: no type_map found\n"); free(body); return 5; }
        snprintf(url, sizeof url, "%s", file);
    } else { rules_usage(); return 2; }

    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.bridge.rules.v1");
    bf_jsonw_key(&w, "format"); bf_jsonw_str(&w, format);
    bf_jsonw_key(&w, "provenance"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, foreign ? "foreign-twin-observation" : "vendored-upstream-file");
    bf_jsonw_key(&w, "source"); bf_jsonw_str(&w, url);
    bf_jsonw_key(&w, "content_sha256"); bf_jsonw_str(&w, body_sha);
    if (foreign) {
        const char *rbn = strrchr(receipt, '/');
        bf_jsonw_key(&w, "twin_id"); bf_jsonw_str(&w, twin);
        bf_jsonw_key(&w, "receipt"); bf_jsonw_str(&w, rbn ? rbn + 1 : receipt);
        bf_jsonw_key(&w, "observed_at"); bf_jsonw_str(&w, observed);
    } else {
        bf_jsonw_key(&w, "const_file_sha256"); bf_jsonw_str(&w, const_sha);
        bf_jsonw_key(&w, "varchar_len"); bf_jsonw_int(&w, varchar_len);
        bf_jsonw_key(&w, "note"); bf_jsonw_str(&w, "a file vendored in this repository, read from disk; no ForeignTwin receipt exists for it");
    }
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "rules"); bf_jsonw_arr(&w);
    for (int i = 0; i < n; i++) {
        Rule *r = &rules[i];
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "id"); { char id[16]; snprintf(id, sizeof id, "r%03d", i + 1); bf_jsonw_str(&w, id); }
        bf_jsonw_key(&w, "match"); bf_jsonw_str(&w, r->key);
        bf_jsonw_key(&w, "target"); bf_jsonw_str(&w, r->target);
        bf_jsonw_key(&w, "drop_typemod"); bf_jsonw_bool(&w, r->drop_typemod);
        bf_jsonw_key(&w, "using"); if (r->using_[0]) bf_jsonw_str(&w, r->using_); else bf_jsonw_null(&w);
        bf_jsonw_key(&w, "condition"); if (r->cond[0]) bf_jsonw_str(&w, r->cond); else bf_jsonw_null(&w);
        bf_jsonw_key(&w, "duplicate"); bf_jsonw_bool(&w, r->duplicate);
        bf_jsonw_key(&w, "note"); if (r->note[0]) bf_jsonw_str(&w, r->note); else bf_jsonw_null(&w);
        bf_jsonw_key(&w, "line"); bf_jsonw_int(&w, r->line);
        bf_jsonw_key(&w, "byte_offset"); bf_jsonw_int(&w, (int64_t)r->off);
        bf_jsonw_key(&w, "byte_length"); bf_jsonw_int(&w, (int64_t)r->len);
        bf_jsonw_key(&w, "excerpt"); bf_jsonw_strn(&w, body + r->off, r->len);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "cannot_infer"); bf_jsonw_arr(&w);
    bf_jsonw_str(&w, "a documented casting rule -> the tool applying it to this database, or the cast preserving every value");
    bf_jsonw_str(&w, "a rule's 'using' function name -> what that function does; it is recorded, not executed");
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    char digest[65];
    if (!write_sealed(out, &w, "rules_digest", digest)) { fprintf(stderr, "rules: cannot write %s\n", out); return 2; }
    printf("{\"rules\":%d,\"content_sha256\":\"%s\",\"rules_digest\":\"%s\"}\n", n, body_sha, digest);
    bf_jsonw_free(&w); free(body);
    return 0;
}

/* ============================================================ schema-map */

typedef struct {
    char src[64], tgt[64], src_type[96], tgt_type[128];
    int kind, src_kind, notnull, pk, mapped;
    int rule_set, rule_idx; char rule_note[160];
    const bf_json_node_t *value_map; char prefix[64];
} Col;

typedef struct {
    char src[64], tgt[96], doctype[96];
    Col cols[MAX_COLS]; int ncols;
    char fk_col[16][64], fk_table[16][96], fk_ref[16][64]; int nfk;
    int unique_sets, checks;
    char uniq[8][256];
} Tab;

typedef struct { char kind[48], ref[160], detail[320]; } Exc;
static Exc g_exc[MAX_EXC]; static int g_nexc;
static void exc(const char *k, const char *ref, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
#include <stdarg.h>
static void exc(const char *k, const char *ref, const char *fmt, ...) {
    if (g_nexc >= MAX_EXC) return;
    Exc *e = &g_exc[g_nexc++];
    snprintf(e->kind, sizeof e->kind, "%s", k); snprintf(e->ref, sizeof e->ref, "%s", ref);
    va_list ap; va_start(ap, fmt); vsnprintf(e->detail, sizeof e->detail, fmt, ap); va_end(ap);
}

static void w_exc(bf_jsonw *w) {
    bf_jsonw_key(w, "exceptions"); bf_jsonw_arr(w);
    for (int i = 0; i < g_nexc; i++) {
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "kind"); bf_jsonw_str(w, g_exc[i].kind);
        bf_jsonw_key(w, "ref"); bf_jsonw_str(w, g_exc[i].ref);
        bf_jsonw_key(w, "detail"); bf_jsonw_str(w, g_exc[i].detail);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
}

static const char *frappe_type_name(int t) {
    switch (t) {
    case BF_TYPE_INT: return "Int"; case BF_TYPE_FLOAT: return "Float"; case BF_TYPE_CURRENCY: return "Currency";
    case BF_TYPE_DATE: return "Date"; case BF_TYPE_DATETIME: return "Datetime"; case BF_TYPE_TIME: return "Time";
    case BF_TYPE_TEXT: return "Text"; case BF_TYPE_LONG_TEXT: return "Long Text"; case BF_TYPE_SELECT: return "Select";
    case BF_TYPE_LINK: return "Link"; case BF_TYPE_DYNAMIC_LINK: return "Dynamic Link"; case BF_TYPE_ATTACH: return "Attach";
    case BF_TYPE_ATTACH_IMAGE: return "Attach Image"; case BF_TYPE_CHECK: return "Check"; case BF_TYPE_PASSWORD: return "Password";
    case BF_TYPE_DATA: return "Data"; case BF_TYPE_CODE: return "Code"; case BF_TYPE_COLOR: return "Color";
    case BF_TYPE_SMALL_TEXT: return "Small Text"; case BF_TYPE_TEXT_EDITOR: return "Text Editor"; case BF_TYPE_RATING: return "Rating";
    case BF_TYPE_READ_ONLY: return "Read Only"; case BF_TYPE_JSON: return "JSON"; case BF_TYPE_DURATION: return "Duration";
    default: return NULL;
    }
}

static const char *FRAPPE_STD[] = {"name", "owner", "creation", "modified", "modified_by", "docstatus", "idx", "parent", "parentfield", "parenttype", NULL};

static void sq(FILE *o, const char *v) {
    fputc('\'', o);
    for (; *v; v++) { if (*v == '\'') fputc('\'', o); fputc(*v, o); }
    fputc('\'', o);
}

static const char *map_value(const bf_json_doc_t *pd, const Col *c, const char *raw, char *buf, size_t sz, int *miss) {
    *miss = 0;
    if (!raw) return NULL;
    const char *v = raw;
    if (c->value_map) {
        int found = 0;
        for (const bf_json_node_t *k = bf_json_child_first(pd, c->value_map); k; k = bf_json_child_next(pd, k))
            if ((int)strlen(raw) == k->key_len && !strncmp(k->key, raw, (size_t)k->key_len)) {
                node_str(k, buf, sz); v = buf; found = 1; break;
            }
        if (!found) *miss = 1;
    }
    if (c->prefix[0]) {
        char tmp[512]; snprintf(tmp, sizeof tmp, "%s%s", c->prefix, v);
        snprintf(buf, sz, "%s", tmp); v = buf;
    }
    return v;
}

static void schema_usage(void) {
    fputs("usage: bonfyre-bridge schema-map --from sqlite:PATH --plan plan.json --rules rules.json --out mapping.json\n"
          "                                 [--frappe-schema schema-ir.json] [--emit-sql rehearsal.sql]\n", stderr);
}

int bridge_schema_map_main(int argc, char **argv) {
    const char *from = topt(argc, argv, "--from"), *plan = topt(argc, argv, "--plan"), *rulesp = topt(argc, argv, "--rules");
    const char *out = topt(argc, argv, "--out"), *frappe = topt(argc, argv, "--frappe-schema"), *emit = topt(argc, argv, "--emit-sql");
    if (!from || !plan || !rulesp || !out || strncmp(from, "sqlite:", 7)) { schema_usage(); return 2; }
    Eng src;
    if (!eng_open(from, &src)) { fprintf(stderr, "schema-map: cannot open source read-only\n"); return 3; }
    char err[160], src_sha[65], plan_sha[65], rules_sha[65], frappe_sha[65] = "";
    bf_sha256_file(src.path, src_sha);
    size_t pl = 0, rl = 0, fl = 0;
    char *pb = bf_read_file(plan, &pl), *rb = bf_read_file(rulesp, &rl), *fb = frappe ? bf_read_file(frappe, &fl) : NULL;
    bf_json_doc_t *pd = pb ? bf_json_parse(pb, pl, err, sizeof err) : NULL;
    bf_json_doc_t *rd = rb ? bf_json_parse(rb, rl, err, sizeof err) : NULL;
    bf_json_doc_t *fd = fb ? bf_json_parse(fb, fl, err, sizeof err) : NULL;
    if (!pd || !rd || (frappe && !fd)) { fprintf(stderr, "schema-map: plan, rules or Frappe schema unreadable\n"); return 3; }
    char rules_check[65];
    if (!bridge_verify_sealed(rb, rl, "rules_digest", rules_check)) { fprintf(stderr, "schema-map: rules file does not match its own digest\n"); return 3; }
    snprintf(rules_sha, sizeof rules_sha, "%s", rules_check);
    bf_sha256_hex((const uint8_t *)pb, pl, plan_sha);
    if (fb) bf_sha256_hex((const uint8_t *)fb, fl, frappe_sha);
    const bf_json_node_t *proot = bf_json_root(pd), *rroot = bf_json_root(rd);
    char pschema[64], plan_id[96], tengine[32], rfmt[48];
    jcopy(pd, proot, "schema", pschema, sizeof pschema); jcopy(pd, proot, "id", plan_id, sizeof plan_id);
    jcopy(pd, proot, "target_engine", tengine, sizeof tengine); jcopy(rd, rroot, "format", rfmt, sizeof rfmt);
    if (strcmp(pschema, "bonfyre.bridge.transform-plan.v1") || (strcmp(tengine, "postgres") && strcmp(tengine, "sqlite"))) {
        fprintf(stderr, "schema-map: plan schema or target engine refused\n"); return 3;
    }
    int frappe_rules = !strcmp(rfmt, "frappe-type-map");
    const bf_json_node_t *rules = bf_json_obj_get(rd, rroot, "rules");

    static Tab tabs[MAX_TABS]; int ntab = 0;
    for (const bf_json_node_t *pt = bf_json_child_first(pd, bf_json_obj_get(pd, proot, "tables")); pt && ntab < MAX_TABS; pt = bf_json_child_next(pd, pt)) {
        Tab *t = &tabs[ntab]; memset(t, 0, sizeof *t);
        jcopy(pd, pt, "source", t->src, sizeof t->src); jcopy(pd, pt, "target", t->tgt, sizeof t->tgt);
        jcopy(pd, pt, "frappe_doctype", t->doctype, sizeof t->doctype);
        if (!t->tgt[0]) snprintf(t->tgt, sizeof t->tgt, "%s", t->src);
        for (const char *p = t->src; *p; p++) if (!isalnum((unsigned char)*p) && *p != '_') { fprintf(stderr, "schema-map: table name refused\n"); return 3; }
        for (const char *p = t->tgt; *p; p++) if (!isalnum((unsigned char)*p) && *p != '_' && *p != ' ') { fprintf(stderr, "schema-map: target name refused\n"); return 3; }
        char sql[512];
        snprintf(sql, sizeof sql, "SELECT name, type, \"notnull\", pk FROM pragma_table_info('%s') ORDER BY cid", t->src);
        Rows r;
        if (!sqlite_rows(src.db, sql, &r) || !r.nrows) { exc("source-table-missing", t->src, "the plan names a table the source does not hold"); rows_free(&r); continue; }
        const bf_json_node_t *pcols = bf_json_obj_get(pd, pt, "columns");
        /* The doctype, when the target is a Frappe DocType. */
        const bf_json_node_t *dt = NULL;
        if (t->doctype[0] && strcmp(tengine, "mariadb"))
            exc("rehearsal-engine-differs", t->tgt, "ERPNext stores DocTypes in MariaDB; this plan rehearses on %s, so MariaDB collation and strict-mode behaviour are not exercised", tengine);
        if (t->doctype[0]) {
            if (!fd) { exc("frappe-schema-missing", t->doctype, "the plan targets a DocType but no Frappe SchemaIR was given"); }
            else {
                const bf_json_node_t *dts = bf_json_dotpath(fd, "schema.doctypes");
                for (const bf_json_node_t *x = bf_json_child_first(fd, dts); x; x = bf_json_child_next(fd, x)) {
                    char nm[96]; jcopy(fd, x, "name", nm, sizeof nm);
                    if (!strcmp(nm, t->doctype)) { dt = x; break; }
                }
                if (!dt) exc("frappe-doctype-missing", t->doctype, "the compiled SchemaIR has no DocType of this name");
            }
        }
        for (int i = 0; i < r.nrows && t->ncols < MAX_COLS; i++) {
            const char *name = cell(&r, i, 0), *type = cell(&r, i, 1) ? cell(&r, i, 1) : "";
            const bf_json_node_t *pc = NULL;
            if (pcols) {
                for (const bf_json_node_t *x = bf_json_child_first(pd, pcols); x; x = bf_json_child_next(pd, x)) {
                    char s[64]; jcopy(pd, x, "source", s, sizeof s);
                    if (!strcmp(s, name)) { pc = x; break; }
                }
                if (!pc) { exc("column-not-carried", t->src, "%s is not in the plan and does not move", name); continue; }
            }
            Col *c = &t->cols[t->ncols++]; memset(c, 0, sizeof *c);
            snprintf(c->src, sizeof c->src, "%s", name); snprintf(c->src_type, sizeof c->src_type, "%s", type);
            c->notnull = atoi(cell(&r, i, 2)); c->pk = atoi(cell(&r, i, 3));
            c->src_kind = kind_of(type);
            if (pc) {
                jcopy(pd, pc, "target", c->tgt, sizeof c->tgt);
                jcopy(pd, pc, "prefix", c->prefix, sizeof c->prefix);
                c->value_map = bf_json_obj_get(pd, pc, "value_map");
            }
            if (!c->tgt[0]) snprintf(c->tgt, sizeof c->tgt, "%s", name);
            /* The rule key: the declared base type, or the Frappe field type. */
            char key[96] = "", mod[64] = "";
            if (frappe_rules) {
                if (dt) {
                    int is_std = 0;
                    for (int k = 0; FRAPPE_STD[k]; k++) if (!strcmp(FRAPPE_STD[k], c->tgt)) is_std = 1;
                    if (is_std && !strcmp(c->tgt, "name")) { snprintf(key, sizeof key, "Data"); snprintf(c->rule_note, sizeof c->rule_note, "standard column name, typed with the Data width"); }
                    else if (is_std) exc("frappe-standard-column", c->tgt, "a Frappe standard column other than name is not typed by this map");
                    else {
                        const bf_json_node_t *field = NULL;
                        for (const bf_json_node_t *f = bf_json_child_first(fd, bf_json_obj_get(fd, dt, "fields")); f; f = bf_json_child_next(fd, f)) {
                            char fnm[96]; jcopy(fd, f, "name", fnm, sizeof fnm);
                            if (!strcmp(fnm, c->tgt)) { field = f; break; }
                        }
                        if (!field) exc("frappe-field-missing", c->tgt, "DocType %s has no field of this name", t->doctype);
                        else {
                            const char *tn = frappe_type_name((int)bf_json_get_int(bf_json_obj_get(fd, field, "type")));
                            if (tn) snprintf(key, sizeof key, "%s", tn);
                            else exc("frappe-type-unnamed", c->tgt, "the field's compiled type has no database column");
                            if (tn && !strcmp(tn, "Link")) {
                                char opt[96]; jcopy(fd, field, "options", opt, sizeof opt);
                                exc("frappe-link-unchecked", c->tgt, "links to %s; the rehearsal does not hold that DocType, so the link is not checked", opt);
                            }
                            if (tn && !strcmp(tn, "Select") && c->value_map) {
                                char opts[1024]; jcopy(fd, field, "options", opts, sizeof opts);
                                for (char *q = opts; (q = strstr(q, "\\n")); ) { *q = '\n'; memmove(q + 1, q + 2, strlen(q + 2) + 1); }
                                for (const bf_json_node_t *v = bf_json_child_first(pd, c->value_map); v; v = bf_json_child_next(pd, v)) {
                                    char val[128]; node_str(v, val, sizeof val);
                                    int ok = 0;
                                    for (char *o = opts, *nl; o && *o; o = nl ? nl + 1 : NULL) {
                                        nl = strchr(o, '\n');
                                        if ((size_t)(nl ? nl - o : (long)strlen(o)) == strlen(val) && !strncmp(o, val, strlen(val))) ok = 1;
                                        if (!nl) break;
                                    }
                                    if (!ok) exc("frappe-select-value-outside-options", c->tgt, "value map emits %s, which the Select field does not offer", val);
                                }
                            }
                        }
                    }
                }
            } else {
                snprintf(key, sizeof key, "%s", type); lower(key);
                char *paren = strchr(key, '(');
                if (paren) { snprintf(mod, sizeof mod, "%s", paren); *paren = 0; }
                trim(key);
                char *sp = strchr(key, ' '); if (sp && !strstr(key, "double")) *sp = 0;
            }
            c->rule_idx = -1;
            int idx = 0;
            for (const bf_json_node_t *rn = bf_json_child_first(rd, rules); rn && key[0]; rn = bf_json_child_next(rd, rn), idx++) {
                char m[64], cond[8]; jcopy(rd, rn, "match", m, sizeof m);
                const bf_json_node_t *cn = bf_json_obj_get(rd, rn, "condition");
                (void)cond;
                if (cn && cn->type != BF_JSON_NULL) continue;  /* conditional rules are not defaults */
                if (!strcmp(m, key)) {
                    char tgt[96]; jcopy(rd, rn, "target", tgt, sizeof tgt);
                    int drop = bf_json_get_bool(bf_json_obj_get(rd, rn, "drop_typemod"));
                    snprintf(c->tgt_type, sizeof c->tgt_type, "%s%s", tgt, drop || frappe_rules ? "" : mod);
                    c->rule_idx = idx; c->rule_set = 1; c->mapped = 1;
                    break;
                }
            }
            if (!c->mapped) {
                if (key[0]) exc("unmapped-type", c->src, "%s.%s is declared %s; no default rule in the cited source covers it, so it is carried as text", t->src, name, type[0] ? type : "(none)");
                snprintf(c->tgt_type, sizeof c->tgt_type, "text");
            }
            /* A value map writes text whatever the source held. */
            c->kind = kind_of(c->tgt_type);
        }
        rows_free(&r);
        /* Frappe: every required field must be filled by something. */
        if (dt) {
            for (const bf_json_node_t *f = bf_json_child_first(fd, bf_json_obj_get(fd, dt, "fields")); f; f = bf_json_child_next(fd, f)) {
                if (!bf_json_get_bool(bf_json_obj_get(fd, f, "required"))) continue;
                char fnm[96]; jcopy(fd, f, "name", fnm, sizeof fnm);
                int have = 0;
                for (int k = 0; k < t->ncols; k++) if (!strcmp(t->cols[k].tgt, fnm)) have = 1;
                if (!have) exc("frappe-required-uncovered", fnm, "DocType %s requires this field and nothing in the source fills it", t->doctype);
            }
        }
        /* Constraints on the source. */
        snprintf(sql, sizeof sql, "SELECT \"from\", \"table\", \"to\" FROM pragma_foreign_key_list('%s')", t->src);
        if (sqlite_rows(src.db, sql, &r)) {
            for (int i = 0; i < r.nrows && t->nfk < 16; i++) {
                snprintf(t->fk_col[t->nfk], 64, "%s", cell(&r, i, 0)); snprintf(t->fk_table[t->nfk], 96, "%s", cell(&r, i, 1));
                snprintf(t->fk_ref[t->nfk], 64, "%s", cell(&r, i, 2) ? cell(&r, i, 2) : "");
                t->nfk++;
            }
        }
        rows_free(&r);
        snprintf(sql, sizeof sql, "SELECT name FROM pragma_index_list('%s') WHERE origin='u'", t->src);
        if (sqlite_rows(src.db, sql, &r)) {
            for (int i = 0; i < r.nrows && t->unique_sets < 8; i++) {
                Rows ic; char q[256];
                snprintf(q, sizeof q, "SELECT name FROM pragma_index_info('%s') ORDER BY seqno", cell(&r, i, 0));
                if (sqlite_rows(src.db, q, &ic)) {
                    char *u = t->uniq[t->unique_sets]; u[0] = 0;
                    for (int k = 0; k < ic.nrows; k++) strncat(u, cell(&ic, k, 0), 200), strncat(u, ",", 2);
                    t->unique_sets++;
                }
                rows_free(&ic);
            }
        }
        rows_free(&r);
        snprintf(sql, sizeof sql, "SELECT sql FROM sqlite_master WHERE type='table' AND name='%s'", t->src);
        if (sqlite_rows(src.db, sql, &r) && r.nrows && cell(&r, 0, 0)) {
            char *ddl = strdup(cell(&r, 0, 0)); lower(ddl);
            for (char *p = ddl; (p = strstr(p, "check")); p += 5) if ((p == ddl || !isalnum((unsigned char)p[-1])) && !isalnum((unsigned char)p[5])) t->checks++;
            free(ddl);
            if (t->checks) exc("check-not-carried", t->src, "%d CHECK constraint%s on the source; the rehearsal SQL does not rewrite CHECK expressions", t->checks, t->checks == 1 ? "" : "s");
        }
        rows_free(&r);
        ntab++;
    }
    /* Objects the plan does not carry. */
    Rows ob;
    if (sqlite_rows(src.db, "SELECT type, name FROM sqlite_master WHERE type IN ('view','trigger') ORDER BY type, name", &ob))
        for (int i = 0; i < ob.nrows; i++) exc("not-carried", cell(&ob, i, 1), "source %s; no rule converts it, so the rehearsal does not have it", cell(&ob, i, 0));
    rows_free(&ob);
    long long n_tables = eng_count(&src, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
    long long n_views = eng_count(&src, "SELECT count(*) FROM sqlite_master WHERE type='view'");
    long long n_trig = eng_count(&src, "SELECT count(*) FROM sqlite_master WHERE type='trigger'");
    long long n_idx = eng_count(&src, "SELECT count(*) FROM sqlite_master WHERE type='index' AND sql IS NOT NULL");
    if (n_tables > ntab) exc("table-not-in-plan", "", "%lld source table%s not named by the plan", n_tables - ntab, n_tables - ntab == 1 ? " is" : "s are");

    /* mapping.json */
    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.bridge.schema-map.v1");
    bf_jsonw_key(&w, "plan_id"); bf_jsonw_str(&w, plan_id);
    bf_jsonw_key(&w, "plan_sha256"); bf_jsonw_str(&w, plan_sha);
    bf_jsonw_key(&w, "source"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "engine"); bf_jsonw_str(&w, "sqlite");
    bf_jsonw_key(&w, "file_sha256"); bf_jsonw_str(&w, src_sha);
    bf_jsonw_key(&w, "objects"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "tables"); bf_jsonw_int(&w, n_tables); bf_jsonw_key(&w, "views"); bf_jsonw_int(&w, n_views);
    bf_jsonw_key(&w, "triggers"); bf_jsonw_int(&w, n_trig); bf_jsonw_key(&w, "indexes"); bf_jsonw_int(&w, n_idx);
    bf_jsonw_obj_end(&w);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "target_engine"); bf_jsonw_str(&w, tengine);
    bf_jsonw_key(&w, "rules"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "format"); bf_jsonw_str(&w, rfmt);
    bf_jsonw_key(&w, "rules_digest"); bf_jsonw_str(&w, rules_sha);
    bf_jsonw_key(&w, "provenance"); bf_jsonw_node(&w, rd, bf_json_obj_get(rd, rroot, "provenance"), 0);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "frappe_schema_sha256"); if (frappe_sha[0]) bf_jsonw_str(&w, frappe_sha); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "tables"); bf_jsonw_arr(&w);
    int total_cols = 0, mapped_cols = 0;
    for (int i = 0; i < ntab; i++) {
        Tab *t = &tabs[i];
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "source"); bf_jsonw_str(&w, t->src);
        bf_jsonw_key(&w, "target"); bf_jsonw_str(&w, t->tgt);
        bf_jsonw_key(&w, "frappe_doctype"); if (t->doctype[0]) bf_jsonw_str(&w, t->doctype); else bf_jsonw_null(&w);
        bf_jsonw_key(&w, "key"); bf_jsonw_arr(&w);
        for (int k = 0; k < t->ncols; k++) if (t->cols[k].pk) bf_jsonw_str(&w, t->cols[k].src);
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "columns"); bf_jsonw_arr(&w);
        for (int k = 0; k < t->ncols; k++) {
            Col *c = &t->cols[k];
            total_cols++; mapped_cols += c->mapped;
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "source"); bf_jsonw_str(&w, c->src);
            bf_jsonw_key(&w, "target"); bf_jsonw_str(&w, c->tgt);
            bf_jsonw_key(&w, "source_type"); bf_jsonw_str(&w, c->src_type);
            bf_jsonw_key(&w, "target_type"); bf_jsonw_str(&w, c->tgt_type);
            bf_jsonw_key(&w, "compare_as"); bf_jsonw_str(&w, KIND[c->kind]);
            bf_jsonw_key(&w, "source_compare_as"); bf_jsonw_str(&w, KIND[c->src_kind]);
            bf_jsonw_key(&w, "not_null"); bf_jsonw_bool(&w, c->notnull);
            bf_jsonw_key(&w, "primary_key"); bf_jsonw_bool(&w, c->pk > 0);
            bf_jsonw_key(&w, "rule");
            if (c->rule_set) {
                const bf_json_node_t *rn = bf_json_arr_get(rd, rules, c->rule_idx);
                bf_jsonw_obj(&w);
                const char *ks[] = {"id", "match", "target", "using", "note", "line", "byte_offset", "byte_length", "excerpt", NULL};
                for (int q = 0; ks[q]; q++) { bf_jsonw_key(&w, ks[q]); bf_jsonw_node(&w, rd, bf_json_obj_get(rd, rn, ks[q]), 0); }
                if (c->rule_note[0]) { bf_jsonw_key(&w, "note"); bf_jsonw_str(&w, c->rule_note); }
                bf_jsonw_obj_end(&w);
            } else bf_jsonw_null(&w);
            bf_jsonw_key(&w, "transform");
            if (c->value_map || c->prefix[0]) {
                bf_jsonw_obj(&w);
                if (c->value_map) { bf_jsonw_key(&w, "value_map"); bf_jsonw_node(&w, pd, c->value_map, 1); }
                if (c->prefix[0]) { bf_jsonw_key(&w, "prefix"); bf_jsonw_str(&w, c->prefix); }
                bf_jsonw_obj_end(&w);
            } else bf_jsonw_null(&w);
            bf_jsonw_obj_end(&w);
        }
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "foreign_keys"); bf_jsonw_arr(&w);
        for (int k = 0; k < t->nfk; k++) {
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "column"); bf_jsonw_str(&w, t->fk_col[k]);
            bf_jsonw_key(&w, "references"); bf_jsonw_str(&w, t->fk_table[k]);
            bf_jsonw_key(&w, "ref_column"); bf_jsonw_str(&w, t->fk_ref[k]);
            bf_jsonw_obj_end(&w);
        }
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "unique_sets"); bf_jsonw_int(&w, t->unique_sets);
        bf_jsonw_key(&w, "checks"); bf_jsonw_int(&w, t->checks);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "columns_total"); bf_jsonw_int(&w, total_cols);
    bf_jsonw_key(&w, "columns_typed_by_rule"); bf_jsonw_int(&w, mapped_cols);
    bf_jsonw_key(&w, "probes"); { const bf_json_node_t *pr = bf_json_obj_get(pd, proot, "probes"); if (pr) bf_jsonw_node(&w, pd, pr, 0); else { bf_jsonw_arr(&w); bf_jsonw_arr_end(&w); } }
    w_exc(&w);
    bf_jsonw_key(&w, "cannot_infer"); bf_jsonw_arr(&w);
    bf_jsonw_str(&w, "a column typed by a cited rule -> every value converting without loss; map-verify measures that on a rehearsal");
    bf_jsonw_str(&w, "a mapping over this source -> the same mapping fitting another database of the same product");
    bf_jsonw_str(&w, "a Frappe DocType field match -> ERPNext controller validation, naming series or permissions accepting the record");
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    char digest[65];
    if (!write_sealed(out, &w, "mapping_digest", digest)) { fprintf(stderr, "schema-map: cannot write %s\n", out); return 2; }

    /* Rehearsal SQL, for the caller to apply to a throwaway target. */
    if (emit) {
        FILE *o = fopen(emit, "wb");
        if (!o) { fprintf(stderr, "schema-map: cannot write %s\n", emit); return 2; }
        int pg = !strcmp(tengine, "postgres");
        fprintf(o, "-- rehearsal SQL from plan %s, mapping %s\n", plan_id, digest);
        if (pg) fputs("SET TIME ZONE 'UTC';\n", o);
        fputs("BEGIN;\n", o);
        for (int i = 0; i < ntab; i++) {
            Tab *t = &tabs[i];
            fprintf(o, "CREATE TABLE \"%s\" (\n", t->tgt);
            int npk = 0;
            for (int k = 0; k < t->ncols; k++) {
                fprintf(o, "  \"%s\" %s%s,\n", t->cols[k].tgt, t->cols[k].tgt_type, t->cols[k].notnull || t->cols[k].pk ? " NOT NULL" : "");
                npk += t->cols[k].pk > 0;
            }
            for (int u = 0; u < t->unique_sets; u++) {
                fputs("  UNIQUE (", o);
                char tmp[256]; snprintf(tmp, sizeof tmp, "%s", t->uniq[u]);
                int first = 1;
                for (char *tok = strtok(tmp, ","); tok; tok = strtok(NULL, ",")) {
                    for (int k = 0; k < t->ncols; k++) if (!strcmp(t->cols[k].src, tok)) { fprintf(o, "%s\"%s\"", first ? "" : ", ", t->cols[k].tgt); first = 0; }
                }
                fputs("),\n", o);
            }
            if (!pg) for (int f = 0; f < t->nfk; f++) {
                const char *rt = t->fk_table[f];
                for (int j = 0; j < ntab; j++) if (!strcmp(tabs[j].src, rt)) rt = tabs[j].tgt;
                fprintf(o, "  FOREIGN KEY (\"%s\") REFERENCES \"%s\",\n", t->fk_col[f], rt);
            }
            fputs("  PRIMARY KEY (", o);
            for (int k = 0, first = 1; k < t->ncols; k++) if (t->cols[k].pk) { fprintf(o, "%s\"%s\"", first ? "" : ", ", t->cols[k].tgt); first = 0; }
            fputs(")\n);\n", o);
            if (!npk) exc("no-key", t->src, "no primary key; rows cannot be matched by key");
            /* Rows */
            char sql[4096]; size_t used = (size_t)snprintf(sql, sizeof sql, "SELECT ");
            for (int k = 0; k < t->ncols; k++)
                { char eb[256]; used += (size_t)snprintf(sql + used, sizeof sql - used, "%s%s", k ? ", " : "", sl_expr(eb, sizeof eb, t->cols[k].src, t->cols[k].src_kind == K_BYTES)); }
            snprintf(sql + used, sizeof sql - used, " FROM \"%s\"", t->src);
            Rows r;
            if (!sqlite_rows(src.db, sql, &r)) { fclose(o); return 3; }
            for (int i2 = 0; i2 < r.nrows; i2++) {
                fprintf(o, "INSERT INTO \"%s\" VALUES (", t->tgt);
                for (int k = 0; k < t->ncols; k++) {
                    Col *c = &t->cols[k];
                    char buf[512]; int miss;
                    const char *v = map_value(pd, c, cell(&r, i2, k), buf, sizeof buf, &miss);
                    if (k) fputs(", ", o);
                    if (!v) fputs("NULL", o);
                    else if (c->src_kind == K_BYTES && c->kind == K_BYTES) { if (pg) fprintf(o, "decode('%s','hex')", v); else fprintf(o, "X'%s'", v); }
                    else if ((c->kind == K_INT || c->kind == K_NUM) && numeric_text(v)) fputs(v, o);
                    else sq(o, v);
                }
                fputs(");\n", o);
            }
            rows_free(&r);
        }
        if (pg) for (int i = 0; i < ntab; i++) for (int f = 0; f < tabs[i].nfk; f++) {
            const char *rt = tabs[i].fk_table[f];
            for (int j = 0; j < ntab; j++) if (!strcmp(tabs[j].src, rt)) rt = tabs[j].tgt;
            fprintf(o, "ALTER TABLE \"%s\" ADD FOREIGN KEY (\"%s\") REFERENCES \"%s\";\n", tabs[i].tgt, tabs[i].fk_col[f], rt);
        }
        /* Plain indexes whose columns all moved. */
        Rows ix;
        if (sqlite_rows(src.db, "SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name", &ix)) {
            for (int i = 0; i < ix.nrows; i++) {
                Tab *t = NULL;
                for (int j = 0; j < ntab; j++) if (!strcmp(tabs[j].src, cell(&ix, i, 1))) t = &tabs[j];
                if (!t) continue;
                Rows ic; char q[256];
                snprintf(q, sizeof q, "SELECT name FROM pragma_index_info('%s') ORDER BY seqno", cell(&ix, i, 0));
                if (!sqlite_rows(src.db, q, &ic)) { rows_free(&ic); continue; }
                fprintf(o, "CREATE INDEX \"%s\" ON \"%s\" (", cell(&ix, i, 0), t->tgt);
                for (int k = 0; k < ic.nrows; k++) {
                    const char *tc = cell(&ic, k, 0);
                    for (int m = 0; m < t->ncols; m++) if (!strcmp(t->cols[m].src, tc)) tc = t->cols[m].tgt;
                    fprintf(o, "%s\"%s\"", k ? ", " : "", tc);
                }
                fputs(");\n", o);
                rows_free(&ic);
            }
        }
        rows_free(&ix);
        fputs("COMMIT;\n", o);
        fclose(o);
    }
    printf("{\"tables\":%d,\"columns\":%d,\"typed_by_rule\":%d,\"exceptions\":%d,\"mapping_digest\":\"%s\"}\n", ntab, total_cols, mapped_cols, g_nexc, digest);
    bf_jsonw_free(&w); eng_close(&src);
    bf_json_free(pd); bf_json_free(rd); if (fd) bf_json_free(fd);
    free(pb); free(rb); free(fb);
    return 0;
}

/* ============================================================ map-verify */

typedef struct { char *key; char **vals; } KRow;

static int krow_cmp(const void *a, const void *b) {
    const KRow *x = a, *y = b;
    return strcmp(x->key ? x->key : "", y->key ? y->key : "");
}

/* Normalised spelling of a declared type, so "timestamptz" and "timestamp
 * with time zone" compare as the same declaration. */
static void norm_type(const char *in, int pg, char *out, size_t sz) {
    char t[160]; snprintf(t, sizeof t, "%s", in); lower(t); trim(t);
    if (!pg) { snprintf(out, sz, "%s", t); return; }
    char base[96], mod[64] = ""; snprintf(base, sizeof base, "%s", t);
    char *p = strchr(base, '('); if (p) { snprintf(mod, sizeof mod, "%s", p); *p = 0; trim(base); }
    struct { const char *a, *b; } alias[] = {
        {"timestamptz", "timestamp with time zone"}, {"timestamp", "timestamp without time zone"},
        {"float", "double precision"}, {"float8", "double precision"}, {"double", "double precision"},
        {"float4", "real"}, {"int", "integer"}, {"int4", "integer"}, {"int8", "bigint"}, {"int2", "smallint"},
        {"bool", "boolean"}, {"varchar", "character varying"}, {"char", "character"}, {"decimal", "numeric"}, {NULL, NULL}};
    for (int i = 0; alias[i].a; i++) if (!strcmp(base, alias[i].a)) { snprintf(base, sizeof base, "%s", alias[i].b); break; }
    /* numeric(10, 2) and numeric(10,2) are one declaration. */
    char m2[64]; size_t j = 0; for (const char *q = mod; *q && j + 1 < sizeof m2; q++) if (*q != ' ') m2[j++] = *q; m2[j] = 0;
    snprintf(out, sz, "%s%s", base, m2);
}

typedef struct { long long nn, pkc, fk, uq, ck; } Cons;

static void cons_sqlite(sqlite3 *db, const char *table, Cons *c) {
    char sql[512]; Rows r; memset(c, 0, sizeof *c);
    snprintf(sql, sizeof sql, "SELECT sum(\"notnull\" AND pk=0), sum(pk>0) FROM pragma_table_info('%s')", table);
    if (sqlite_rows(db, sql, &r) && r.nrows) { c->nn = cell(&r, 0, 0) ? atoll(cell(&r, 0, 0)) : 0; c->pkc = cell(&r, 0, 1) ? atoll(cell(&r, 0, 1)) : 0; }
    rows_free(&r);
    snprintf(sql, sizeof sql, "SELECT count(*) FROM pragma_foreign_key_list('%s')", table);
    if (sqlite_rows(db, sql, &r) && r.nrows) c->fk = atoll(cell(&r, 0, 0)); rows_free(&r);
    snprintf(sql, sizeof sql, "SELECT count(*) FROM pragma_index_list('%s') WHERE origin='u'", table);
    if (sqlite_rows(db, sql, &r) && r.nrows) c->uq = atoll(cell(&r, 0, 0)); rows_free(&r);
    snprintf(sql, sizeof sql, "SELECT sql FROM sqlite_master WHERE type='table' AND name='%s'", table);
    if (sqlite_rows(db, sql, &r) && r.nrows && cell(&r, 0, 0)) {
        char *d = strdup(cell(&r, 0, 0)); lower(d);
        for (char *p = d; (p = strstr(p, "check")); p += 5) if ((p == d || !isalnum((unsigned char)p[-1])) && !isalnum((unsigned char)p[5])) c->ck++;
        free(d);
    }
    rows_free(&r);
}

static void cons_pg(const Eng *e, const char *table, Cons *c) {
    char sql[1024]; Rows r; memset(c, 0, sizeof *c);
    snprintf(sql, sizeof sql,
             "SELECT (SELECT count(*) FROM pg_attribute a WHERE a.attrelid='public.\"%s\"'::regclass AND a.attnum>0 AND NOT a.attisdropped AND a.attnotnull "
             "AND NOT EXISTS (SELECT 1 FROM pg_constraint p WHERE p.conrelid=a.attrelid AND p.contype='p' AND a.attnum=ANY(p.conkey))),"
             "(SELECT coalesce(sum(array_length(conkey,1)),0) FROM pg_constraint WHERE conrelid='public.\"%s\"'::regclass AND contype='p'),"
             "(SELECT count(*) FROM pg_constraint WHERE conrelid='public.\"%s\"'::regclass AND contype='f'),"
             "(SELECT count(*) FROM pg_constraint WHERE conrelid='public.\"%s\"'::regclass AND contype='u'),"
             "(SELECT count(*) FROM pg_constraint WHERE conrelid='public.\"%s\"'::regclass AND contype='c')",
             table, table, table, table, table);
    if (pg_rows(e, sql, &r) && r.nrows) {
        c->nn = atoll(cell(&r, 0, 0)); c->pkc = atoll(cell(&r, 0, 1)); c->fk = atoll(cell(&r, 0, 2));
        c->uq = atoll(cell(&r, 0, 3)); c->ck = atoll(cell(&r, 0, 4));
    }
    rows_free(&r);
}

static void w_cons(bf_jsonw *w, const Cons *c) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "not_null"); bf_jsonw_int(w, c->nn); bf_jsonw_key(w, "primary_key_columns"); bf_jsonw_int(w, c->pkc);
    bf_jsonw_key(w, "foreign_keys"); bf_jsonw_int(w, c->fk); bf_jsonw_key(w, "unique"); bf_jsonw_int(w, c->uq);
    bf_jsonw_key(w, "checks"); bf_jsonw_int(w, c->ck);
    bf_jsonw_obj_end(w);
}

static void verify_usage(void) {
    fputs("usage: bonfyre-bridge map-verify --from sqlite:PATH --to sqlite:PATH|postgres://…|env:NAME --mapping mapping.json --out diff.json\n"
          "                                 [--expect earlier-diff.json]\n", stderr);
}

int bridge_map_verify_main(int argc, char **argv) {
    const char *from = topt(argc, argv, "--from"), *to = topt(argc, argv, "--to"), *mp = topt(argc, argv, "--mapping");
    const char *out = topt(argc, argv, "--out"), *expect = topt(argc, argv, "--expect");
    if (!from || !to || !mp || !out) { verify_usage(); return 2; }
    Eng src, dst;
    if (!eng_open(from, &src) || src.pg) { fprintf(stderr, "map-verify: the source is a SQLite file opened read-only\n"); return 3; }
    if (!eng_open(to, &dst)) { fprintf(stderr, "map-verify: cannot open the rehearsal target read-only\n"); return 3; }
    size_t ml = 0; char err[160], mdig[65], src_sha[65];
    char *mb = bf_read_file(mp, &ml);
    if (!mb || !bridge_verify_sealed(mb, ml, "mapping_digest", mdig)) { fprintf(stderr, "map-verify: mapping does not match its own digest\n"); return 3; }
    bf_json_doc_t *md = bf_json_parse(mb, ml, err, sizeof err);
    if (!md) return 3;
    const bf_json_node_t *mroot = bf_json_root(md);
    char want_src_sha[65], plan_id[96];
    jcopy(md, bf_json_obj_get(md, mroot, "source"), "file_sha256", want_src_sha, sizeof want_src_sha);
    jcopy(md, mroot, "plan_id", plan_id, sizeof plan_id);
    bf_sha256_file(src.path, src_sha);
    if (strcmp(src_sha, want_src_sha)) exc("source-changed", src.display, "the source file no longer hashes to the one the mapping was made from");

    bf_jsonw m; bf_jsonw_init(&m, 0);   /* the measured part, digested */
    bf_jsonw w; bf_jsonw_init(&w, 2);
    int all_rows = 1, all_types = 1, all_cons = 1, all_probes = 1, all_objects = 1;
    long long src_rows_total = 0, dst_rows_total = 0;
    bf_jsonw_arr(&m);

    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.bridge.map-verify.v1");
    bf_jsonw_key(&w, "plan_id"); bf_jsonw_str(&w, plan_id);
    bf_jsonw_key(&w, "mapping_digest"); bf_jsonw_str(&w, mdig);
    bf_jsonw_key(&w, "source"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "display"); bf_jsonw_str(&w, src.display);
    bf_jsonw_key(&w, "file_sha256"); bf_jsonw_str(&w, src_sha);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "target"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "engine"); bf_jsonw_str(&w, dst.pg ? "postgres" : "sqlite");
    bf_jsonw_key(&w, "display"); bf_jsonw_str(&w, dst.display);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "tables"); bf_jsonw_arr(&w);

    for (const bf_json_node_t *t = bf_json_child_first(md, bf_json_obj_get(md, mroot, "tables")); t; t = bf_json_child_next(md, t)) {
        char ts[64], tt[96]; jcopy(md, t, "source", ts, sizeof ts); jcopy(md, t, "target", tt, sizeof tt);
        const bf_json_node_t *cols = bf_json_obj_get(md, t, "columns");
        int nc = 0; Col cs[MAX_COLS]; int keyidx[MAX_COLS], nk = 0;
        for (const bf_json_node_t *c = bf_json_child_first(md, cols); c && nc < MAX_COLS; c = bf_json_child_next(md, c)) {
            Col *x = &cs[nc]; memset(x, 0, sizeof *x);
            char ca[32], sca[32];
            jcopy(md, c, "source", x->src, sizeof x->src); jcopy(md, c, "target", x->tgt, sizeof x->tgt);
            jcopy(md, c, "source_type", x->src_type, sizeof x->src_type); jcopy(md, c, "target_type", x->tgt_type, sizeof x->tgt_type);
            jcopy(md, c, "compare_as", ca, sizeof ca); jcopy(md, c, "source_compare_as", sca, sizeof sca);
            for (int k = 0; k < 6; k++) { if (!strcmp(ca, KIND[k])) x->kind = k; if (!strcmp(sca, KIND[k])) x->src_kind = k; }
            x->pk = bf_json_get_bool(bf_json_obj_get(md, c, "primary_key"));
            const bf_json_node_t *tf = bf_json_obj_get(md, c, "transform");
            if (tf && tf->type != BF_JSON_NULL) { x->value_map = bf_json_obj_get(md, tf, "value_map"); jcopy(md, tf, "prefix", x->prefix, sizeof x->prefix); }
            if (x->pk) keyidx[nk++] = nc;
            nc++;
        }
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "source"); bf_jsonw_str(&w, ts);
        bf_jsonw_key(&w, "target"); bf_jsonw_str(&w, tt);
        if (!nk || !nc) {
            exc("no-key", ts, "rows cannot be matched without a primary key");
            bf_jsonw_key(&w, "state"); bf_jsonw_str(&w, "not_compared"); bf_jsonw_obj_end(&w);
            all_rows = 0; all_types = 0; all_cons = 0; continue;
        }
        /* Read both sides. */
        char sq1[4096], sq2[4096]; size_t u1 = (size_t)snprintf(sq1, sizeof sq1, "SELECT "), u2 = (size_t)snprintf(sq2, sizeof sq2, "SELECT ");
        for (int k = 0; k < nc; k++) {
            char eb[256];
            u1 += (size_t)snprintf(sq1 + u1, sizeof sq1 - u1, "%s%s", k ? ", " : "", sl_expr(eb, sizeof eb, cs[k].src, cs[k].src_kind == K_BYTES));
            if (dst.pg) u2 += (size_t)snprintf(sq2 + u2, sizeof sq2 - u2, "%s%s\"%s\"%s", k ? ", " : "", cs[k].kind == K_BYTES ? "encode(" : "", cs[k].tgt, cs[k].kind == K_BYTES ? ",'hex')" : "::text");
            else u2 += (size_t)snprintf(sq2 + u2, sizeof sq2 - u2, "%s%s", k ? ", " : "", sl_expr(eb, sizeof eb, cs[k].tgt, cs[k].kind == K_BYTES));
        }
        snprintf(sq1 + u1, sizeof sq1 - u1, " FROM \"%s\"", ts);
        snprintf(sq2 + u2, sizeof sq2 - u2, dst.pg ? " FROM public.\"%s\"" : " FROM \"%s\"", tt);
        Rows rs, rt;
        int okd = 0;
        if (!sqlite_rows(src.db, sq1, &rs)) { bf_jsonw_key(&w, "state"); bf_jsonw_str(&w, "source_unreadable"); bf_jsonw_obj_end(&w); all_rows = 0; all_types = 0; all_cons = 0; continue; }
        okd = eng_rows(&dst, sq2, &rt);
        if (!okd) {
            exc("target-table-missing", tt, "the rehearsal target could not be read for this table");
            bf_jsonw_key(&w, "state"); bf_jsonw_str(&w, "target_unreadable"); bf_jsonw_obj_end(&w);
            rows_free(&rs); all_rows = 0; all_types = 0; all_cons = 0; continue;
        }
        /* Raw source digest: what the source held, before any transform. */
        BfSha256 hraw; bf_sha256_init(&hraw);
        for (int i = 0; i < rs.nrows; i++) for (int k = 0; k < nc; k++) hash_val(&hraw, cell(&rs, i, k));
        char raw_hex[65]; hash_hex(&hraw, raw_hex);
        /* Expected (source, transformed, canonical) and actual (target, canonical) rows, keyed. */
        KRow *ex = calloc((size_t)rs.nrows + 1, sizeof *ex), *ac = calloc((size_t)rt.nrows + 1, sizeof *ac);
        int map_miss = 0;
        for (int side = 0; side < 2; side++) {
            Rows *R = side ? &rt : &rs; KRow *K = side ? ac : ex;
            for (int i = 0; i < R->nrows; i++) {
                K[i].vals = calloc((size_t)nc, sizeof(char *));
                for (int k = 0; k < nc; k++) {
                    const char *v = cell(R, i, k);
                    char buf[512]; int miss = 0;
                    if (!side) v = map_value(md, &cs[k], v, buf, sizeof buf, &miss);
                    if (miss) map_miss++;
                    K[i].vals[k] = canon(cs[k].kind, v);
                }
                char key[1024] = "";
                for (int q = 0; q < nk; q++) {
                    const char *kv = K[i].vals[keyidx[q]];
                    strncat(key, kv ? kv : "\x1dN", sizeof key - strlen(key) - 2); strncat(key, "\x1f", sizeof key - strlen(key) - 1);
                }
                K[i].key = strdup(key);
            }
        }
        if (map_miss) exc("value-map-miss", ts, "%d source value%s had no entry in the plan's value map and moved unchanged", map_miss, map_miss == 1 ? "" : "s");
        qsort(ex, (size_t)rs.nrows, sizeof *ex, krow_cmp);
        qsort(ac, (size_t)rt.nrows, sizeof *ac, krow_cmp);
        /* Column checksums over key order, and a merge join for differences. */
        BfSha256 he[MAX_COLS], ha[MAX_COLS], hte, hta;
        for (int k = 0; k < nc; k++) { bf_sha256_init(&he[k]); bf_sha256_init(&ha[k]); }
        bf_sha256_init(&hte); bf_sha256_init(&hta);
        for (int i = 0; i < rs.nrows; i++) for (int k = 0; k < nc; k++) { hash_val(&he[k], ex[i].key); hash_val(&he[k], ex[i].vals[k]); hash_val(&hte, ex[i].vals[k]); }
        for (int i = 0; i < rt.nrows; i++) for (int k = 0; k < nc; k++) { hash_val(&ha[k], ac[i].key); hash_val(&ha[k], ac[i].vals[k]); hash_val(&hta, ac[i].vals[k]); }
        int missing = 0, extra = 0, diffcount[MAX_COLS] = {0};
        char firstdiff[MAX_COLS][3][96]; memset(firstdiff, 0, sizeof firstdiff);
        char miss_keys[3][96] = {"", "", ""}, extra_keys[3][96] = {"", "", ""};
        for (int a = 0, b = 0; a < rs.nrows || b < rt.nrows;) {
            int c = a >= rs.nrows ? 1 : b >= rt.nrows ? -1 : strcmp(ex[a].key, ac[b].key);
            if (c < 0) { if (missing < 3) snprintf(miss_keys[missing], 96, "%s", ex[a].key); missing++; a++; continue; }
            if (c > 0) { if (extra < 3) snprintf(extra_keys[extra], 96, "%s", ac[b].key); extra++; b++; continue; }
            for (int k = 0; k < nc; k++) {
                const char *x = ex[a].vals[k], *y = ac[b].vals[k];
                if ((!x) != (!y) || (x && strcmp(x, y))) { if (diffcount[k] < 3) snprintf(firstdiff[k][diffcount[k]], 96, "%s", ex[a].key); diffcount[k]++; }
            }
            a++; b++;
        }
        /* Declared target types. */
        char actual[MAX_COLS][128]; memset(actual, 0, sizeof actual);
        Rows ty; char tq[1024];
        if (dst.pg) snprintf(tq, sizeof tq, "SELECT a.attname, format_type(a.atttypid,a.atttypmod) FROM pg_attribute a WHERE a.attrelid='public.\"%s\"'::regclass AND a.attnum>0 AND NOT a.attisdropped", tt);
        else snprintf(tq, sizeof tq, "SELECT name, type FROM pragma_table_info('%s')", tt);
        if (eng_rows(&dst, tq, &ty)) for (int i = 0; i < ty.nrows; i++) for (int k = 0; k < nc; k++)
            if (!strcmp(cell(&ty, i, 0), cs[k].tgt)) snprintf(actual[k], 128, "%s", cell(&ty, i, 1) ? cell(&ty, i, 1) : "");
        rows_free(&ty);
        Cons cso, cto;
        cons_sqlite(src.db, ts, &cso);
        if (dst.pg) cons_pg(&dst, tt, &cto); else cons_sqlite(dst.db, tt, &cto);

        int values_ok = 1, types_ok = 1;
        char eh[65], ah[65]; hash_hex(&hte, eh); hash_hex(&hta, ah);
        bf_jsonw_key(&w, "source_rows"); bf_jsonw_int(&w, rs.nrows);
        bf_jsonw_key(&w, "target_rows"); bf_jsonw_int(&w, rt.nrows);
        bf_jsonw_key(&w, "source_raw_sha256"); bf_jsonw_str(&w, raw_hex);
        bf_jsonw_key(&w, "expected_rows_sha256"); bf_jsonw_str(&w, eh);
        bf_jsonw_key(&w, "target_rows_sha256"); bf_jsonw_str(&w, ah);
        bf_jsonw_key(&w, "missing_in_target"); bf_jsonw_int(&w, missing);
        bf_jsonw_key(&w, "extra_in_target"); bf_jsonw_int(&w, extra);
        bf_jsonw_key(&w, "missing_keys"); bf_jsonw_arr(&w); for (int q = 0; q < missing && q < 3; q++) { char k2[96]; snprintf(k2, sizeof k2, "%s", miss_keys[q]); for (char *p = k2; *p; p++) if (*p == '\x1f') *p = '|'; bf_jsonw_str(&w, k2); } bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "extra_keys"); bf_jsonw_arr(&w); for (int q = 0; q < extra && q < 3; q++) { char k2[96]; snprintf(k2, sizeof k2, "%s", extra_keys[q]); for (char *p = k2; *p; p++) if (*p == '\x1f') *p = '|'; bf_jsonw_str(&w, k2); } bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "columns"); bf_jsonw_arr(&w);
        bf_jsonw_obj(&m); bf_jsonw_key(&m, "t"); bf_jsonw_str(&m, tt);
        bf_jsonw_key(&m, "raw"); bf_jsonw_str(&m, raw_hex); bf_jsonw_key(&m, "rows"); bf_jsonw_str(&m, ah);
        bf_jsonw_key(&m, "n"); bf_jsonw_int(&m, rt.nrows);
        bf_jsonw_key(&m, "c"); bf_jsonw_arr(&m);
        for (int k = 0; k < nc; k++) {
            char e1[65], a1[65], want[160], got[160];
            hash_hex(&he[k], e1); hash_hex(&ha[k], a1);
            norm_type(cs[k].tgt_type, dst.pg, want, sizeof want); norm_type(actual[k], dst.pg, got, sizeof got);
            int type_ok = actual[k][0] && !strcmp(want, got);
            if (!type_ok) { types_ok = 0; exc("type-differs", cs[k].tgt, "%s.%s: mapping says %s, rehearsal declares %s", tt, cs[k].tgt, cs[k].tgt_type, actual[k][0] ? actual[k] : "(absent)"); }
            if (diffcount[k]) values_ok = 0;
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "source"); bf_jsonw_str(&w, cs[k].src);
            bf_jsonw_key(&w, "target"); bf_jsonw_str(&w, cs[k].tgt);
            bf_jsonw_key(&w, "source_type"); bf_jsonw_str(&w, cs[k].src_type);
            bf_jsonw_key(&w, "expected_type"); bf_jsonw_str(&w, cs[k].tgt_type);
            bf_jsonw_key(&w, "target_type"); bf_jsonw_str(&w, actual[k]);
            bf_jsonw_key(&w, "type_matches"); bf_jsonw_bool(&w, type_ok);
            bf_jsonw_key(&w, "compare_as"); bf_jsonw_str(&w, KIND[cs[k].kind]);
            bf_jsonw_key(&w, "expected_sha256"); bf_jsonw_str(&w, e1);
            bf_jsonw_key(&w, "target_sha256"); bf_jsonw_str(&w, a1);
            bf_jsonw_key(&w, "values_match"); bf_jsonw_bool(&w, !strcmp(e1, a1));
            bf_jsonw_key(&w, "differing_rows"); bf_jsonw_int(&w, diffcount[k]);
            bf_jsonw_key(&w, "first_differing_keys"); bf_jsonw_arr(&w);
            for (int q = 0; q < diffcount[k] && q < 3; q++) { char k2[96]; snprintf(k2, sizeof k2, "%s", firstdiff[k][q]); for (char *p = k2; *p; p++) if (*p == '\x1f') *p = '|'; bf_jsonw_str(&w, k2); }
            bf_jsonw_arr_end(&w);
            bf_jsonw_obj_end(&w);
            bf_jsonw_obj(&m); bf_jsonw_key(&m, "c"); bf_jsonw_str(&m, cs[k].tgt); bf_jsonw_key(&m, "h"); bf_jsonw_str(&m, a1);
            bf_jsonw_key(&m, "ty"); bf_jsonw_str(&m, got); bf_jsonw_obj_end(&m);
        }
        bf_jsonw_arr_end(&w);
        bf_jsonw_arr_end(&m);
        int cons_ok = cso.nn == cto.nn && cso.pkc == cto.pkc && cso.fk == cto.fk && cso.uq == cto.uq && cso.ck == cto.ck;
        bf_jsonw_key(&w, "source_constraints"); w_cons(&w, &cso);
        bf_jsonw_key(&w, "target_constraints"); w_cons(&w, &cto);
        bf_jsonw_key(&m, "k"); w_cons(&m, &cto);
        bf_jsonw_obj_end(&m);
        if (!cons_ok) exc("constraint-count-differs", tt, "source not-null/pk/fk/unique/check %lld/%lld/%lld/%lld/%lld, rehearsal %lld/%lld/%lld/%lld/%lld",
                          cso.nn, cso.pkc, cso.fk, cso.uq, cso.ck, cto.nn, cto.pkc, cto.fk, cto.uq, cto.ck);
        int rows_ok = !missing && !extra && rs.nrows == rt.nrows && values_ok;
        bf_jsonw_key(&w, "constraints_match"); bf_jsonw_bool(&w, cons_ok);
        bf_jsonw_key(&w, "state"); bf_jsonw_str(&w, rs.nrows != rt.nrows || missing || extra ? "rows_differ" : !values_ok ? "values_differ" : "arrived");
        bf_jsonw_obj_end(&w);
        if (!rows_ok) all_rows = 0;
        if (!types_ok) all_types = 0;
        if (!cons_ok) all_cons = 0;
        src_rows_total += rs.nrows; dst_rows_total += rt.nrows;
        for (int i = 0; i < rs.nrows; i++) { for (int k = 0; k < nc; k++) free(ex[i].vals[k]); free(ex[i].vals); free(ex[i].key); }
        for (int i = 0; i < rt.nrows; i++) { for (int k = 0; k < nc; k++) free(ac[i].vals[k]); free(ac[i].vals); free(ac[i].key); }
        free(ex); free(ac); rows_free(&rs); rows_free(&rt);
    }
    bf_jsonw_arr_end(&w);

    /* Objects: views and triggers present on each side, by name. */
    Rows so, to_;
    const char *tobj = dst.pg
        ? "SELECT 'view', viewname FROM pg_views WHERE schemaname='public' UNION ALL "
          "SELECT 'trigger', t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
          "WHERE n.nspname='public' AND NOT t.tgisinternal ORDER BY 1,2"
        : "SELECT type, name FROM sqlite_master WHERE type IN ('view','trigger') ORDER BY 1,2";
    sqlite_rows(src.db, "SELECT type, name FROM sqlite_master WHERE type IN ('view','trigger') ORDER BY 1,2", &so);
    if (!eng_rows(&dst, tobj, &to_)) memset(&to_, 0, sizeof to_);
    bf_jsonw_key(&w, "objects"); bf_jsonw_arr(&w);
    for (int i = 0; i < so.nrows; i++) {
        int found = 0;
        for (int j = 0; j < to_.nrows; j++) if (!strcmp(cell(&so, i, 0), cell(&to_, j, 0)) && !strcmp(cell(&so, i, 1), cell(&to_, j, 1))) found = 1;
        if (!found) { all_objects = 0; exc("object-absent", cell(&so, i, 1), "source %s is not on the rehearsal target", cell(&so, i, 0)); }
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, cell(&so, i, 0));
        bf_jsonw_key(&w, "name"); bf_jsonw_str(&w, cell(&so, i, 1));
        bf_jsonw_key(&w, "on_target"); bf_jsonw_bool(&w, found);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj(&m); bf_jsonw_key(&m, "o"); bf_jsonw_int(&m, all_objects); bf_jsonw_obj_end(&m);
    rows_free(&so); rows_free(&to_);

    /* Probes: the same question asked of each engine in its own dialect. */
    bf_jsonw_key(&w, "probes"); bf_jsonw_arr(&w);
    for (const bf_json_node_t *p = bf_json_child_first(md, bf_json_obj_get(md, mroot, "probes")); p; p = bf_json_child_next(md, p)) {
        char id[64], purpose[256], s1[2048], s2[2048];
        jcopy(md, p, "id", id, sizeof id); jcopy(md, p, "purpose", purpose, sizeof purpose);
        jcopy(md, p, "source_sql", s1, sizeof s1); jcopy(md, p, "target_sql", s2, sizeof s2);
        Rows a, b; char ha_[65] = "", hb_[65] = ""; int ra = sqlite_rows(src.db, s1, &a), rb = eng_rows(&dst, s2, &b);
        char pa[200] = "", pb2[200] = "";
        for (int side = 0; side < 2; side++) {
            Rows *R = side ? &b : &a; if (!(side ? rb : ra)) continue;
            BfSha256 h; bf_sha256_init(&h);
            char *prev = side ? pb2 : pa;
            for (int i = 0; i < R->nrows; i++) for (int k = 0; k < R->ncols; k++) {
                const char *v = cell(R, i, k);
                char *cv = v && numeric_text(v) ? canon(K_NUM, v) : (v ? strdup(v) : NULL);
                hash_val(&h, cv);
                if (strlen(prev) < 160) { strncat(prev, cv ? cv : "NULL", 199 - strlen(prev)); strncat(prev, k + 1 < R->ncols ? "," : ";", 199 - strlen(prev)); }
                free(cv);
            }
            hash_hex(&h, side ? hb_ : ha_);
        }
        int same = ra && rb && !strcmp(ha_, hb_);
        if (!same) { all_probes = 0; exc("behavior-differs", id, "%s: source answered %s, rehearsal answered %s", purpose, ra ? pa : "(error)", rb ? pb2 : "(error)"); }
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "id"); bf_jsonw_str(&w, id);
        bf_jsonw_key(&w, "purpose"); bf_jsonw_str(&w, purpose);
        bf_jsonw_key(&w, "source_result"); bf_jsonw_str(&w, ra ? pa : "(error)");
        bf_jsonw_key(&w, "target_result"); bf_jsonw_str(&w, rb ? pb2 : "(error)");
        bf_jsonw_key(&w, "source_sha256"); bf_jsonw_str(&w, ha_);
        bf_jsonw_key(&w, "target_sha256"); bf_jsonw_str(&w, hb_);
        bf_jsonw_key(&w, "same"); bf_jsonw_bool(&w, same);
        bf_jsonw_obj_end(&w);
        bf_jsonw_obj(&m); bf_jsonw_key(&m, "p"); bf_jsonw_str(&m, id); bf_jsonw_key(&m, "a"); bf_jsonw_str(&m, ha_); bf_jsonw_key(&m, "b"); bf_jsonw_str(&m, hb_); bf_jsonw_obj_end(&m);
        if (ra) rows_free(&a); if (rb) rows_free(&b);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_arr_end(&m);
    char diff_digest[65];
    bf_sha256_hex((const uint8_t *)m.buf, m.len, diff_digest);

    /* The re-check: an earlier receipt over the same mapping must measure the same. */
    int drift = 0; char prior[65] = "";
    if (expect) {
        size_t el = 0; char *eb = bf_read_file(expect, &el);
        bf_json_doc_t *ed = eb ? bf_json_parse(eb, el, err, sizeof err) : NULL;
        char pm[65] = "";
        if (ed) { jcopy(ed, bf_json_root(ed), "diff_digest", prior, sizeof prior); jcopy(ed, bf_json_root(ed), "mapping_digest", pm, sizeof pm); }
        if (!ed || !prior[0]) { fprintf(stderr, "map-verify: --expect receipt unreadable\n"); return 3; }
        if (strcmp(pm, mdig)) { drift = 1; exc("recheck-mapping-changed", expect, "the earlier receipt was made over another mapping"); }
        if (strcmp(prior, diff_digest)) { drift = 1; exc("recheck-drift", expect, "what was measured has changed since the earlier receipt"); }
        bf_json_free(ed); free(eb);
    }
    int matched = all_rows && all_types && all_cons && all_probes && all_objects && strcmp(src_sha, want_src_sha) == 0;
    bf_jsonw_key(&w, "source_rows"); bf_jsonw_int(&w, src_rows_total);
    bf_jsonw_key(&w, "target_rows"); bf_jsonw_int(&w, dst_rows_total);
    bf_jsonw_key(&w, "verdict"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "rows_and_values_match"); bf_jsonw_bool(&w, all_rows);
    bf_jsonw_key(&w, "types_match"); bf_jsonw_bool(&w, all_types);
    bf_jsonw_key(&w, "constraints_match"); bf_jsonw_bool(&w, all_cons);
    bf_jsonw_key(&w, "objects_carried"); bf_jsonw_bool(&w, all_objects);
    bf_jsonw_key(&w, "behavior_matches"); bf_jsonw_bool(&w, all_probes);
    bf_jsonw_key(&w, "matched"); bf_jsonw_bool(&w, matched);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "recheck");
    if (expect) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "prior_diff_digest"); bf_jsonw_str(&w, prior);
        bf_jsonw_key(&w, "unchanged"); bf_jsonw_bool(&w, !drift);
        bf_jsonw_obj_end(&w);
    } else bf_jsonw_null(&w);
    w_exc(&w);
    bf_jsonw_key(&w, "effects"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "source_written"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "target_written"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "network"); bf_jsonw_bool(&w, dst.pg);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "cannot_infer"); bf_jsonw_arr(&w);
    bf_jsonw_str(&w, "matching checksums on a rehearsal -> the production cutover matching, or the source not changing after it was read");
    bf_jsonw_str(&w, "values compared after canonicalisation -> byte-identical storage; precision beyond the canonical text is not compared");
    bf_jsonw_str(&w, "probes that agree -> every query the application runs behaving the same");
    bf_jsonw_str(&w, "a rehearsal on a fixture -> any customer's database; it runs on the copy it names and nothing else");
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "diff_digest"); bf_jsonw_str(&w, diff_digest);
    bf_jsonw_obj_end(&w);
    if (w.oom || !write_file(out, w.buf, w.len)) { fprintf(stderr, "map-verify: cannot write %s\n", out); return 2; }
    printf("{\"matched\":%s,\"rows_and_values_match\":%s,\"types_match\":%s,\"constraints_match\":%s,\"objects_carried\":%s,\"behavior_matches\":%s,\"exceptions\":%d,\"diff_digest\":\"%s\"%s}\n",
           matched ? "true" : "false", all_rows ? "true" : "false", all_types ? "true" : "false", all_cons ? "true" : "false",
           all_objects ? "true" : "false", all_probes ? "true" : "false", g_nexc, diff_digest, expect ? (drift ? ",\"recheck\":\"drift\"" : ",\"recheck\":\"unchanged\"") : "");
    bf_jsonw_free(&w); bf_jsonw_free(&m); bf_json_free(md); free(mb);
    eng_close(&src); eng_close(&dst);
    if (drift) return 4;
    return matched ? 0 : 5;
}
