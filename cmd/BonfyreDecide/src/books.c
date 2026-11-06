/*
 * books.c — bonfyre books: bank and card lines booked to the client's own
 * accounts, on this machine. Port of books.mjs and cmd/BonfyreBooks/bonfyre-books.
 * Learned weights answer first; the local model takes only the lines they are
 * unsure of, and its answers are held for review.
 */
#include "dp.h"
#include "books.h"
#include "bonfyre.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <alloca.h>
#include <unistd.h>
#include <limits.h>

#define MAX_SAFE 9007199254740991.0

static const char *js_num(const bf_jv *v, char *buf) { bf_js_number(bf_jv_n(v, 0), buf); return buf; }
static double js_round(double x) { double r = floor(x); return x - r >= 0.5 ? r + 1 : r; }
static bf_re *RE(const char *p, const char *f) {
    char err[128]; bf_re *re = bf_re_compile(p, f, err, sizeof err);
    if (!re) { fprintf(stderr, "bonfyre books: bad pattern %s: %s\n", p, err); exit(70); }
    return re;
}
static int test(bf_re *re, const char *s) { return bf_re_exec(re, s, strlen(s), 0, NULL); }
static char *lower(const char *s) { char *o = strdup(s); for (char *p = o; *p; p++) if (*p >= 'A' && *p <= 'Z') *p += 32; return o; }
static char *upper(const char *s) { char *o = strdup(s); for (char *p = o; *p; p++) if (*p >= 'a' && *p <= 'z') *p -= 32; return o; }

static void fs_fail(const char *op, const char *path) {
    if (errno == EACCES) dp_fail("EACCES: permission denied, %s '%s'", op, path);
    if (errno == EISDIR) dp_fail("EISDIR: illegal operation on a directory, read");
    dp_fail("ENOENT: no such file or directory, %s '%s'", op, path);
}
static char *read_text(const char *path, size_t *len) {
    if (!path) { if (len) *len = 0; return strdup(""); }
    char *t = dp_read_file(path, len);
    if (!t) fs_fail("open", path);
    return t;
}

/* ── CSV ────────────────────────────────────────────────────────────────── */

typedef struct { Row *r; int n; } Table;

static int row_has_text(const Row *row) {
    for (int i = 0; i < row->n; i++) { char *t = dp_js_trim(row->c[i]); int any = *t != 0; free(t); if (any) return 1; }
    return 0;
}
static Table parse_csv(const char *text) {
    Table t = {0}; Row row = {0}; bf_re_buf field = {0}; int quoted = 0, cap = 0;
    bf_re_append(&field, "", 0);
#define PUSH_FIELD() do { row.c = realloc(row.c, sizeof *row.c * (size_t)(row.n + 1)); row.c[row.n++] = strdup(field.buf); field.len = 0; field.buf[0] = 0; } while (0)
#define PUSH_ROW() do { if (row_has_text(&row)) { if (t.n == cap) t.r = realloc(t.r, sizeof *t.r * (size_t)(cap = cap ? cap * 2 : 64)); t.r[t.n++] = row; } else { for (int k = 0; k < row.n; k++) free(row.c[k]); free(row.c); } row.c = NULL; row.n = 0; } while (0)
    for (size_t i = 0; text[i]; i++) {
        char c = text[i];
        if (quoted) {
            if (c == '"' && text[i + 1] == '"') { bf_re_append(&field, "\"", 1); i++; }
            else if (c == '"') quoted = 0;
            else bf_re_append(&field, &c, 1);
        } else if (c == '"') quoted = 1;
        else if (c == ',') PUSH_FIELD();
        else if (c == '\n' || c == '\r') {
            if (c == '\r' && text[i + 1] == '\n') i++;
            PUSH_FIELD(); PUSH_ROW();
        } else bf_re_append(&field, &c, 1);
    }
    if (quoted) dp_fail("the CSV ends inside a quoted field; the file is cut off or malformed");
    PUSH_FIELD(); PUSH_ROW();
    free(field.buf);
    return t;
}
static char *csv_cell(const char *v) {
    if (!strpbrk(v, "\",\n")) return strdup(v);
    bf_re_buf b = {0}; bf_re_append(&b, "\"", 1);
    for (const char *p = v; *p; p++) { if (*p == '"') bf_re_append(&b, "\"\"", 2); else bf_re_append(&b, p, 1); }
    bf_re_append(&b, "\"", 1);
    return b.buf;
}

/* ── amounts, in integer cents, read exactly ────────────────────────────── */

static int amount_null;   /* parse_amount's null */
static double parse_amount(const char *text) {
    static bf_re *mark, *usd, *paren, *sign, *dollar, *shape;
    if (!mark) {
        mark = RE("\\s*\\b(CR|DR)$", ""); usd = RE("^(USD|US\\$)\\s*|\\s*USD$", ""); paren = RE("^\\(.*\\)$", "");
        sign = RE("^[+-]", ""); dollar = RE("^\\$\\s*", "");
        shape = RE("^(\\d{1,3}(,\\d{3})+|\\d+)(\\.\\d{1,2})?$|^\\.\\d{1,2}$", "");
    }
    amount_null = 0;
    char *t = dp_js_trim(text ? text : ""), *s = upper(t); free(t);
    int negative = 0;
    if (!*s) { free(s); amount_null = 1; return 0; }
    bf_re_match m;
    if (bf_re_exec(mark, s, strlen(s), 0, &m)) {
        negative = s[m.start[1]] == 'D';
        s[m.start[0]] = 0; t = dp_js_trim(s); free(s); s = t;
    }
    t = bf_re_replace(usd, s, ""); free(s); s = t;
    if (test(paren, s)) { negative = !negative; size_t n = strlen(s); s[n - 1] = 0; t = dp_js_trim(s + 1); free(s); s = t; }
    size_t n = strlen(s);
    if (n && s[n - 1] == '-') { negative = !negative; s[n - 1] = 0; t = dp_js_trim(s); free(s); s = t; }
    if (test(sign, s)) { if (s[0] == '-') negative = !negative; t = dp_js_trim(s + 1); free(s); s = t; }
    t = bf_re_replace(dollar, s, ""); free(s); s = t;
    if (!test(shape, s)) { free(s); amount_null = 1; return 0; }
    unsigned __int128 whole = 0, frac = 0; int fd = 0, digits = 0, in_frac = 0;
    for (char *p = s; *p; p++) {
        if (*p == ',') continue;
        if (*p == '.') { in_frac = 1; continue; }
        if (in_frac) { frac = frac * 10 + (unsigned)(*p - '0'); fd++; }
        else { if (++digits > 30) { free(s); amount_null = 1; return 0; } whole = whole * 10 + (unsigned)(*p - '0'); }
    }
    if (fd == 1) frac *= 10;
    free(s);
    unsigned __int128 exact = whole * 100 + frac;
    if (exact > (unsigned __int128)9007199254740991ULL) { amount_null = 1; return 0; }
    double cents = (double)(unsigned long long)exact;
    return negative ? -cents : cents;
}

static char *format_cents(double cents, int is_null) {
    if (is_null) return strdup("");
    double a = fabs(cents);
    return dp_fmt("%s%.0f.%02d", cents < 0 ? "-" : "", floor(a / 100), (int)fmod(a, 100));
}

/* ── columns ────────────────────────────────────────────────────────────── */

const char *ROLE_NAMES[6] = {"date", "description", "debit", "credit", "amount", "category"};
static const char *ROLE_EXACT[6] = {
    "^(posted |posting |transaction )?date$", "^(description|memo|payee|name|details|narrative)$",
    "^(debit|withdrawal|withdrawals|money out|paid out)( amount)?$", "^(credit|deposit|deposits|money in|paid in)( amount)?$",
    "^(amount|transaction amount|amt)$", "^(category|account category)$"};
static const char *ROLE_LOOSE[6] = {
    "\\bdate\\b", "\\b(description|memo|payee|details)\\b", "\\b(debit|withdrawal|money out|paid out)\\b",
    "\\b(credit|deposit|money in|paid in)\\b", "\\bamount\\b", "^(account|.*\\bcategory\\b.*)$"};


static int role_index(const char *r) { for (int i = 0; i < 6; i++) if (!strcmp(ROLE_NAMES[i], r)) return i; return -1; }

static char *join_header(const Row *h) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (int i = 0; i < h->n; i++) { if (i) bf_re_append(&b, ", ", 2); bf_re_append(&b, h->c[i], strlen(h->c[i])); }
    return b.buf;
}

Cols resolve_columns(const Row *header, const Mapping *map) {
    Cols out; for (int i = 0; i < 6; i++) out.at[i] = -1;
    out.mapped_amount = 0;
    int n = header->n; char **names = malloc(sizeof *names * (size_t)(n + 1)); int *taken = calloc((size_t)n + 1, sizeof(int));
    for (int i = 0; i < n; i++) { char *t = dp_js_trim(header->c[i]); names[i] = lower(t); free(t); }
    for (int i = 0; i < n; i++) if (*names[i]) for (int j = 0; j < i; j++) if (!strcmp(names[i], names[j]))
        dp_fail("the header names \"%s\" twice, so which column holds it is unknown", names[i]);
    for (int i = 0; i < n; i++) if (!*names[i]) taken[i] = 1;
    int assigned_order[6], na = 0;
    for (int m = 0; m < map->n; m++) {
        int role = map->order[m];
        char *g = dp_js_trim(map->given[role]), *gl = lower(g);
        int index = -1;
        if (*g) for (int i = 0; i < n; i++) if (!strcmp(names[i], gl)) { index = i; break; }
        if (index < 0) { char *h = join_header(header); dp_fail("--columns names \"%s\" for %s, but the header has: %s", map->given[role], ROLE_NAMES[role], h); }
        for (int k = 0; k < na; k++) if (out.at[assigned_order[k]] == index)
            dp_fail("--columns gives \"%s\" to both %s and %s", map->given[role], ROLE_NAMES[assigned_order[k]], ROLE_NAMES[role]);
        out.at[role] = index; taken[index] = 1; assigned_order[na++] = role;
        if (role == 4) out.mapped_amount = 1;
        free(g); free(gl);
    }
    static bf_re *ex[6], *lo[6], *not_amount;
    if (!ex[0]) { for (int r = 0; r < 6; r++) { ex[r] = RE(ROLE_EXACT[r], ""); lo[r] = RE(ROLE_LOOSE[r], ""); } not_amount = RE("\\b(debit|credit|withdrawal|deposit|balance)\\b", ""); }
    for (int r = 0; r < 6; r++) {
        if (out.at[r] >= 0) continue;
        int *hits = malloc(sizeof(int) * (size_t)(n + 1)), nh = 0;
        for (int i = 0; i < n; i++) if (!taken[i] && test(ex[r], names[i])) hits[nh++] = i;
        if (!nh) for (int i = 0; i < n; i++) if (!taken[i] && test(lo[r], names[i]) && !(r == 4 && test(not_amount, names[i]))) hits[nh++] = i;
        if (r == 1 && nh > 1) {
            static const char *words[6] = {"description", "memo", "payee", "name", "details", "narrative"};
            int *rank = malloc(sizeof(int) * (size_t)nh), *ord = malloc(sizeof(int) * (size_t)nh);
            for (int k = 0; k < nh; k++) { rank[k] = -1; for (int w = 0; w < 6; w++) if (strstr(names[hits[k]], words[w])) { rank[k] = w; break; } ord[k] = k; }
            for (int a = 1; a < nh; a++) for (int b = a; b > 0 && rank[ord[b - 1]] > rank[ord[b]]; b--) { int t = ord[b]; ord[b] = ord[b - 1]; ord[b - 1] = t; }
            if (rank[ord[0]] >= 0 && rank[ord[0]] < rank[ord[1]]) { hits[0] = hits[ord[0]]; nh = 1; }
            free(rank); free(ord);
        }
        if (nh > 1) {
            bf_re_buf b = {0}; bf_re_append(&b, "", 0);
            for (int k = 0; k < nh; k++) { if (k) bf_re_append(&b, ", ", 2); bf_re_append(&b, header->c[hits[k]], strlen(header->c[hits[k]])); }
            dp_fail("more than one column could be the %s: %s; name one with --columns %s=<header>", ROLE_NAMES[r], b.buf, ROLE_NAMES[r]);
        }
        if (nh) { out.at[r] = hits[0]; taken[hits[0]] = 1; }
        free(hits);
    }
    if (out.at[1] < 0) dp_fail("the transactions file needs a description (or memo, payee, details) column");
    if (out.at[4] >= 0 && (out.at[2] >= 0 || out.at[3] >= 0) && !out.mapped_amount)
        dp_fail("both a signed amount (%s) and debit/credit columns are present; name the one to use with --columns", header->c[out.at[4]]);
    if (out.mapped_amount) { out.at[2] = -1; out.at[3] = -1; }
    for (int i = 0; i < n; i++) free(names[i]);
    free(names); free(taken);
    return out;
}

static Mapping parse_columns(const char *text) {
    Mapping m; memset(&m, 0, sizeof m);
    if (!text || !strcmp(text, DP_TRUE) || !*text) return m;
    /* String.split(',') keeps empty pieces; walk it by hand */
    const char *p = text;
    for (;;) {
        size_t len = strcspn(p, ",");
        char *pair = malloc(len + 1); memcpy(pair, p, len); pair[len] = 0;
        char *eq = strchr(pair, '=');
        if (!eq || eq == pair) dp_fail("--columns wants role=header pairs, got \"%s\"", pair);
        *eq = 0;
        char *role = dp_js_trim(pair), *value = dp_js_trim(eq + 1);
        int r = role_index(role);
        if (r < 0) dp_fail("--columns role must be one of date, description, debit, credit, amount, category");
        int seen = 0; for (int k = 0; k < m.n; k++) if (m.order[k] == r) seen = 1;
        if (!seen) m.order[m.n++] = r;
        free(m.given[r]); m.given[r] = value; m.role[r] = role;
        free(pair);
        if (!p[len]) break;
        p += len + 1;
    }
    return m;
}

/* ── transactions ───────────────────────────────────────────────────────── */

typedef struct {
    int line; char *date, *description, *amount_text, *truth;
    double cents; int cents_null, amount_unread;
} Txn;
typedef struct { Txn *t; int n; bf_jv *columns; } TxnFile;

static char *cell(const Row *r, const Cols *c, int role) {
    if (c->at[role] < 0) return strdup("");
    return dp_js_trim(c->at[role] < r->n ? r->c[c->at[role]] : "");
}

static TxnFile read_transaction_file(const char *text, const Mapping *map) {
    Table t = parse_csv(text);
    Row empty = {0};
    const Row *header = t.n ? &t.r[0] : &empty;
    Cols at = resolve_columns(header, map);
    TxnFile f = {calloc((size_t)(t.n ? t.n : 1), sizeof(Txn)), 0, bf_jv_obj()};
    /* columns: insertion order of the resolved roles (mapping first) */
    for (int k = 0; k < map->n; k++) { int r = map->order[k]; if (at.at[r] >= 0) { char *v = dp_js_trim(header->c[at.at[r]]); bf_jv_set(f.columns, ROLE_NAMES[r], bf_jv_str(v)); free(v); } }
    for (int r = 0; r < 6; r++) if (at.at[r] >= 0 && !bf_jv_has(f.columns, ROLE_NAMES[r])) { char *v = dp_js_trim(header->c[at.at[r]]); bf_jv_set(f.columns, ROLE_NAMES[r], bf_jv_str(v)); free(v); }
    int split = at.at[4] < 0 && (at.at[2] >= 0 || at.at[3] >= 0);
    for (int i = 1; i < t.n; i++) {
        const Row *r = &t.r[i]; Txn *x = &f.t[f.n++];
        x->line = i + 1;
        x->date = cell(r, &at, 0); x->description = cell(r, &at, 1);
        x->cents_null = 1; x->cents = 0; x->amount_text = strdup("");
        if (at.at[4] >= 0) {
            free(x->amount_text); x->amount_text = cell(r, &at, 4);
            x->cents = parse_amount(x->amount_text); x->cents_null = amount_null;
        } else if (split) {
            char *dt = cell(r, &at, 2), *ct = cell(r, &at, 3);
            double debit = 0, credit = 0; int dn = 0, cn = 0;
            if (*dt) { debit = parse_amount(dt); dn = amount_null; }
            if (*ct) { credit = parse_amount(ct); cn = amount_null; }
            free(x->amount_text);
            x->amount_text = *dt && *ct ? dp_fmt("debit %s credit %s", dt, ct) : *dt ? dp_fmt("debit %s", dt) : *ct ? dp_fmt("credit %s", ct) : strdup("");
            if ((!*dt && !*ct) || dn || cn || (debit != 0 && credit != 0)) x->cents_null = 1;
            else { x->cents = fabs(credit) - fabs(debit); x->cents_null = 0; }
            free(dt); free(ct);
        }
        x->amount_unread = x->cents_null && (at.at[4] >= 0 || split);
        char *truth = cell(r, &at, 5);
        if (*truth) x->truth = truth; else free(truth);
    }
    return f;
}

static bf_jv *txn_json(const Txn *x) {
    bf_jv *o = bf_jv_obj();
    bf_jv_set(o, "line", bf_jv_num(x->line));
    bf_jv_set(o, "date", bf_jv_str(x->date));
    bf_jv_set(o, "description", bf_jv_str(x->description));
    bf_jv_set(o, "amount_cents", x->cents_null ? bf_jv_null() : bf_jv_num(x->cents));
    bf_jv_set(o, "amount", x->cents_null ? bf_jv_null() : bf_jv_num(x->cents / 100));
    bf_jv_set(o, "amount_text", bf_jv_str(x->amount_text));
    bf_jv_set(o, "amount_known", bf_jv_bool(!x->cents_null));
    bf_jv_set(o, "amount_unread", bf_jv_bool(x->amount_unread));
    if (x->truth) bf_jv_set(o, "truth", bf_jv_str(x->truth));
    return o;
}

/* ── chart, slugs, tokens ───────────────────────────────────────────────── */

typedef struct { char *name, *description; } Account;
typedef struct { Account *a; int n; } Chart;

static Chart read_chart(const char *text) {
    Chart c = {0};
    const char *p = text;
    while (1) {
        size_t len = strcspn(p, "\n");
        char *line = malloc(len + 1); memcpy(line, p, len); line[len] = 0;
        if (len && line[len - 1] == '\r') line[len - 1] = 0;
        char *t = dp_js_trim(line); free(line);
        if (*t && *t != '#') {
            char *colon = strchr(t, ':');
            Account a;
            if (colon && colon > t) {
                *colon = 0; a.name = dp_js_trim(t); char *d = dp_js_trim(colon + 1);
                if (*d) a.description = d; else { free(d); a.description = strdup(a.name); }
            } else { a.name = dp_js_trim(t); a.description = strdup(a.name); }
            c.a = realloc(c.a, sizeof *c.a * (size_t)(c.n + 1)); c.a[c.n++] = a;
        }
        free(t);
        if (!p[len]) break;
        p += len + 1;
    }
    return c;
}

static char *slug(const char *name) {
    static bf_re *nonalnum, *edges;
    if (!nonalnum) { nonalnum = RE("[^a-z0-9]+", "g"); edges = RE("^-|-$", "g"); }
    char *l = lower(name), *a = bf_re_replace(nonalnum, l, "-"), *b = bf_re_replace(edges, a, "");
    free(l); free(a);
    if (strlen(b) > 60) b[60] = 0;
    if (!*b) { free(b); return strdup("account"); }
    return b;
}

typedef struct { char **t; int n; } Tokens;
static Tokens line_tokens(const char *description) {
    static bf_re *nonalpha, *ws;
    if (!nonalpha) { nonalpha = RE("[^a-z ]+", "g"); ws = RE("\\s+", "g"); }
    char *l = lower(description ? description : ""), *c = bf_re_replace(nonalpha, l, " "); free(l);
    char **words = NULL; int nw = 0;
    for (char *p = c; *p;) {
        while (*p == ' ' || (*p >= 9 && *p <= 13)) p++;
        if (!*p) break;
        char *e = p; while (*e && !(*e == ' ' || (*e >= 9 && *e <= 13))) e++;
        if (e - p > 1) { words = realloc(words, sizeof *words * (size_t)(nw + 1)); words[nw] = malloc((size_t)(e - p) + 1); memcpy(words[nw], p, (size_t)(e - p)); words[nw][e - p] = 0; nw++; }
        p = e;
    }
    free(c);
    Tokens out = {0};
#define ADD_TOKEN(s) do { char *_s = (s); int _dup = 0; for (int _k = 0; _k < out.n; _k++) if (!strcmp(out.t[_k], _s)) { _dup = 1; break; } \
    if (_dup) free(_s); else { out.t = realloc(out.t, sizeof *out.t * (size_t)(out.n + 1)); out.t[out.n++] = _s; } } while (0)
    for (int i = 0; i < nw; i++) ADD_TOKEN(dp_fmt("w:%s", words[i]));
    for (int i = 0; i + 1 < nw; i++) ADD_TOKEN(dp_fmt("b:%s_%s", words[i], words[i + 1]));
    bf_re_buf text = {0}; bf_re_append(&text, " ", 1);
    for (int i = 0; i < nw; i++) { if (i) bf_re_append(&text, " ", 1); bf_re_append(&text, words[i], strlen(words[i])); }
    bf_re_append(&text, " ", 1);
    for (size_t i = 0; i + 4 <= text.len; i++) ADD_TOKEN(dp_fmt("c:%.4s", text.buf + i));
    free(text.buf);
    for (int i = 0; i < nw; i++) free(words[i]);
    free(words);
    (void)ws;
    return out;
}
static void tokens_free(Tokens *t) { for (int i = 0; i < t->n; i++) free(t->t[i]); free(t->t); }

/* ── learning ───────────────────────────────────────────────────────────── */

typedef struct { char *key; double *w; } Weight;
typedef struct { Weight *w; int n, cap; int ncls; } Weights;   /* insertion order, plus a lookup below */

/* A hash index over Weights keys (learning touches tens of thousands). */
typedef struct { char **keys; int *idx; size_t cap; } KeyIndex;
static size_t hash_str(const char *s) { size_t h = 1469598103934665603ULL; for (; *s; s++) h = (h ^ (unsigned char)*s) * 1099511628211ULL; return h; }
static int ki_find(KeyIndex *k, const char *key) {
    if (!k->cap) return -1;
    for (size_t i = hash_str(key) & (k->cap - 1);; i = (i + 1) & (k->cap - 1)) { if (!k->keys[i]) return -1; if (!strcmp(k->keys[i], key)) return k->idx[i]; }
}
static void ki_put(KeyIndex *k, const char *key, int idx);
static void ki_grow(KeyIndex *k) {
    KeyIndex n = {calloc(k->cap ? k->cap * 2 : 4096, sizeof(char *)), calloc(k->cap ? k->cap * 2 : 4096, sizeof(int)), k->cap ? k->cap * 2 : 4096};
    for (size_t i = 0; i < k->cap; i++) if (k->keys[i]) ki_put(&n, k->keys[i], k->idx[i]);
    free(k->keys); free(k->idx); *k = n;
}
static size_t ki_count;
static void ki_put(KeyIndex *k, const char *key, int idx) {
    if (!k->cap || (ki_count + 1) * 2 > k->cap) { size_t c = ki_count; ki_count = 0; ki_grow(k); ki_count = c; }
    for (size_t i = hash_str(key) & (k->cap - 1);; i = (i + 1) & (k->cap - 1)) {
        if (!k->keys[i]) { k->keys[i] = (char *)key; k->idx[i] = idx; ki_count++; return; }
        if (!strcmp(k->keys[i], key)) { k->idx[i] = idx; return; }
    }
}

static void softmax(double *z, int n) {
    double max = z[0]; for (int i = 1; i < n; i++) if (z[i] > max) max = z[i];
    double sum = 0; for (int i = 0; i < n; i++) { z[i] = exp(z[i] - max); sum += z[i]; }
    for (int i = 0; i < n; i++) z[i] /= sum;
}

static int cmp_str(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

static bf_jv *learn_books(const char *history, const Mapping *map, int epochs) {
    TxnFile f = read_transaction_file(history, map);
    int nrows = 0; for (int i = 0; i < f.n; i++) if (f.t[i].truth) nrows++;
    if (nrows < 20) dp_fail("learning needs at least 20 categorised lines; the history has %d", nrows);
    Txn **rows = malloc(sizeof *rows * (size_t)nrows); int r = 0;
    for (int i = 0; i < f.n; i++) if (f.t[i].truth) rows[r++] = &f.t[i];
    char **accounts = malloc(sizeof *accounts * (size_t)nrows); int na = 0;
    for (int i = 0; i < nrows; i++) { int seen = 0; for (int k = 0; k < na; k++) if (!strcmp(accounts[k], rows[i]->truth)) seen = 1; if (!seen) accounts[na++] = rows[i]->truth; }
    qsort(accounts, (size_t)na, sizeof *accounts, cmp_str);
    Weights ws = {0}; ws.ncls = na; KeyIndex ki = {0}; ki_count = 0;
    double *bias = calloc((size_t)na, sizeof(double));
    Tokens *toks = malloc(sizeof *toks * (size_t)nrows); int *y = malloc(sizeof(int) * (size_t)nrows);
    for (int i = 0; i < nrows; i++) {
        toks[i] = line_tokens(rows[i]->description);
        for (int k = 0; k < na; k++) if (!strcmp(accounts[k], rows[i]->truth)) y[i] = k;
    }
    double *p = malloc(sizeof(double) * (size_t)na);
    int **idx = calloc((size_t)nrows, sizeof(int *));   /* each row's tokens as weight rows, once they exist */
    for (int epoch = 0; epoch < epochs; epoch++) {
        double rate = 0.3 / sqrt(epoch + 1);
        for (int i = 0; i < nrows; i++) {
            memcpy(p, bias, sizeof(double) * (size_t)na);
            if (idx[i]) for (int t = 0; t < toks[i].n; t++) { const double *w = ws.w[idx[i][t]].w; for (int c = 0; c < na; c++) p[c] += w[c]; }
            else for (int t = 0; t < toks[i].n; t++) { int at = ki_find(&ki, toks[i].t[t]); if (at >= 0) for (int c = 0; c < na; c++) p[c] += ws.w[at].w[c]; }
            softmax(p, na);
            if (!idx[i]) {   /* JS creates missing rows on the first class pass, in token order */
                idx[i] = malloc(sizeof(int) * (size_t)(toks[i].n ? toks[i].n : 1));
                for (int t = 0; t < toks[i].n; t++) {
                    int at = ki_find(&ki, toks[i].t[t]);
                    if (at < 0) {
                        if (ws.n == ws.cap) ws.w = realloc(ws.w, sizeof *ws.w * (size_t)(ws.cap = ws.cap ? ws.cap * 2 : 1024));
                        ws.w[ws.n].key = strdup(toks[i].t[t]); ws.w[ws.n].w = calloc((size_t)na, sizeof(double));
                        at = ws.n++; ki_put(&ki, ws.w[at].key, at);
                    }
                    idx[i][t] = at;
                }
            }
            for (int c = 0; c < na; c++) {
                double g = p[c] - (c == y[i]);
                bias[c] -= rate * g * 0.1;
                double step = rate * g;
                for (int t = 0; t < toks[i].n; t++) ws.w[idx[i][t]].w[c] -= step;
            }
        }
    }
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "schema", bf_jv_str("bonfyre.books.learned.v1"));
    bf_jv *acc = bf_jv_arr(); for (int k = 0; k < na; k++) bf_jv_push(acc, bf_jv_str(accounts[k]));
    bf_jv_set(out, "accounts", acc);
    bf_jv *b = bf_jv_arr(); for (int k = 0; k < na; k++) bf_jv_push(b, bf_jv_num(js_round(bias[k] * 1e4) / 1e4));
    bf_jv_set(out, "bias", b);
    int *order = malloc(sizeof(int) * (size_t)(ws.n ? ws.n : 1));
    for (int i = 0; i < ws.n; i++) order[i] = i;
    /* toSorted(([a], [b]) => a < b ? -1 : 1) over distinct keys: byte order */
    char **keys = malloc(sizeof *keys * (size_t)(ws.n ? ws.n : 1));
    for (int i = 0; i < ws.n; i++) keys[i] = ws.w[i].key;
    qsort(keys, (size_t)ws.n, sizeof *keys, cmp_str);
    bf_jv *kept = bf_jv_obj();
    for (int i = 0; i < ws.n; i++) {
        double *w = ws.w[ki_find(&ki, keys[i])].w; int any = 0;
        for (int c = 0; c < na; c++) if (fabs(w[c]) > 0.01) any = 1;
        if (!any) continue;
        bf_jv *row = bf_jv_arr(); for (int c = 0; c < na; c++) bf_jv_push(row, bf_jv_num(js_round(w[c] * 1e4) / 1e4));
        bf_jv_set(kept, keys[i], row);
    }
    bf_jv_set(out, "weights", kept);
    bf_jv *from = bf_jv_obj(); char h[65];
    bf_sha256_hex((const uint8_t *)history, strlen(history), h);
    bf_jv_set(from, "lines", bf_jv_num(nrows)); bf_jv_set(from, "sha256", bf_jv_str(h));
    bf_jv_set(out, "learned_from", from);
    bf_jv *ex = bf_jv_arr();
    for (int i = 0; i < nrows; i++) { bf_jv *pair = bf_jv_arr(); bf_jv_push(pair, bf_jv_str(rows[i]->description)); bf_jv_push(pair, bf_jv_str(rows[i]->truth)); bf_jv_push(ex, pair); }
    bf_jv_set(out, "examples", ex);
    free(order); free(keys); free(p); free(bias); free(y);
    return out;
}

/* ── the learned owner ──────────────────────────────────────────────────── */

typedef struct { bf_jv *model; KeyIndex ki; char **slugs; int nacc; } Learned;

static bf_jv *learned_evaluate(DpOwner *o, const bf_jv *state, const bf_jv *questions, const DpSchema *schema) {
    (void)schema;
    Learned *l = o->ctx;
    const bf_jv *bias = bf_jv_get(l->model, "bias"), *weights = bf_jv_get(l->model, "weights");
    int n = (int)bias->len;
    double *z = malloc(sizeof(double) * (size_t)(n ? n : 1));
    for (int c = 0; c < n; c++) z[c] = bias->items[c]->n;
    Tokens t = line_tokens(bf_jv_s(bf_jv_path(state, "transaction.description")));
    for (int i = 0; i < t.n; i++) {
        int at = ki_find(&l->ki, t.t[i]);
        if (at >= 0) { const bf_jv *w = weights->items[at]; for (int c = 0; c < n; c++) z[c] += w->items[c]->n; }
    }
    tokens_free(&t);
    softmax(z, n);
    const bf_jv *criteria = bf_jv_path(questions, "account.criteria");
    bf_jv *probs = bf_jv_obj();
    for (size_t k = 0; k < criteria->len; k++) {
        int idx = -1;
        for (int a = 0; a < l->nacc; a++) if (!strcmp(l->slugs[a], criteria->keys[k])) idx = a;   /* last slug wins, as a Map */
        bf_jv_set(probs, criteria->keys[k], bf_jv_num(idx >= 0 ? z[idx] : 0));
    }
    double sum = 0; for (size_t k = 0; k < probs->len; k++) sum += probs->items[k]->n;
    double outside = 1 - sum;
    if (bf_jv_has(probs, "none")) bf_jv_get(probs, "none")->n += fmax(0, outside);
    else { double total = 1 - outside; if (total == 0) total = 1; for (size_t k = 0; k < probs->len; k++) probs->items[k]->n /= total; }
    const char *choice = criteria->keys[0];
    for (size_t k = 0; k < criteria->len; k++) if (bf_jv_get(probs, criteria->keys[k])->n > bf_jv_get(probs, choice)->n) choice = criteria->keys[k];
    double conf = bf_jv_get(probs, choice)->n;
    bf_jv *account = bf_jv_obj();
    bf_jv_set(account, "type", bf_jv_str("choice"));
    bf_jv_set(account, "choice", bf_jv_str(choice));
    bf_jv_set(account, "confidence", bf_jv_num(conf));
    bf_jv_set(account, "probabilities", probs);
    bf_jv *review = bf_jv_obj();
    bf_jv_set(review, "type", bf_jv_str("noul"));
    bf_jv_set(review, "noul", bf_jv_num(1 - conf));
    bf_jv *answers = bf_jv_obj();
    bf_jv_set(answers, "account", account);
    bf_jv_set(answers, "needs_review", review);
    bf_jv *out = bf_jv_obj(); bf_jv_set(out, "answers", answers);
    free(z);
    return out;
}

static DpOwner *learned_owner(const char *bytes, size_t len) {
    char err[128];
    bf_jv *model = bf_jv_parse(bytes, len, err, sizeof err);
    if (!model) dp_fail("JSON Parse error: %s", err);
    const char *schema = bf_jv_s(bf_jv_get(model, "schema"));
    if (!schema || strcmp(schema, "bonfyre.books.learned.v1")) dp_fail("not a bonfyre.books.learned.v1 file");
    Learned *l = calloc(1, sizeof *l); l->model = model;
    const bf_jv *weights = bf_jv_get(model, "weights"), *accounts = bf_jv_get(model, "accounts");
    ki_count = 0;
    for (size_t i = 0; i < weights->len; i++) ki_put(&l->ki, weights->keys[i], (int)i);
    l->nacc = (int)accounts->len; l->slugs = malloc(sizeof *l->slugs * (accounts->len + 1));
    for (size_t i = 0; i < accounts->len; i++) l->slugs[i] = slug(accounts->items[i]->s);
    DpOwner *o = calloc(1, sizeof *o);
    o->model = strdup("books-learned");
    bf_sha256_hex((const uint8_t *)bytes, len, o->artifact_digest);
    o->deterministic = 1;
    o->question_semantics = "independent_evaluation"; o->confidence_semantics = "maximum_option_probability";
    o->evaluate = learned_evaluate; o->ctx = l;
    return o;
}

/* ── the most similar earlier lines ─────────────────────────────────────── */

typedef struct { KeyIndex ki; int **post; int *npost; int ntok; int *sizes; const bf_jv *examples; } History;
static History *history_index(const bf_jv *examples) {
    History *h = calloc(1, sizeof *h); h->examples = examples;
    size_t n = examples ? examples->len : 0;
    h->sizes = calloc(n + 1, sizeof(int));
    ki_count = 0;
    char **toknames = NULL; int cap = 0;
    for (size_t id = 0; id < n; id++) {
        Tokens t = line_tokens(bf_jv_s(bf_jv_at(examples->items[id], 0)));
        h->sizes[id] = t.n;
        for (int i = 0; i < t.n; i++) {
            int at = ki_find(&h->ki, t.t[i]);
            if (at < 0) {
                if (h->ntok == cap) { cap = cap ? cap * 2 : 1024; toknames = realloc(toknames, sizeof *toknames * (size_t)cap); h->post = realloc(h->post, sizeof *h->post * (size_t)cap); h->npost = realloc(h->npost, sizeof *h->npost * (size_t)cap); }
                at = h->ntok++; toknames[at] = strdup(t.t[i]); h->post[at] = NULL; h->npost[at] = 0;
                ki_put(&h->ki, toknames[at], at);
            }
            h->post[at] = realloc(h->post[at], sizeof(int) * (size_t)(h->npost[at] + 1));
            h->post[at][h->npost[at]++] = (int)id;
        }
        tokens_free(&t);
    }
    return h;
}
typedef struct { int id; double score; } Sim;
static int cmp_sim(const void *a, const void *b) {
    const Sim *x = a, *y = b;
    if (y->score != x->score) return y->score > x->score ? 1 : -1;
    return x->id - y->id;
}
static bf_jv *similar(History *h, const char *description, int k) {
    Tokens mine = line_tokens(description);
    int n = h->examples ? (int)h->examples->len : 0;
    int *shared = calloc((size_t)n + 1, sizeof(int)), *seen = malloc(sizeof(int) * (size_t)(n + 1)), ns = 0;
    for (int i = 0; i < mine.n; i++) {
        int at = ki_find(&h->ki, mine.t[i]);
        if (at < 0) continue;
        for (int j = 0; j < h->npost[at]; j++) { int id = h->post[at][j]; if (!shared[id]) seen[ns++] = id; shared[id]++; }
    }
    Sim *s = malloc(sizeof *s * (size_t)(ns ? ns : 1));
    for (int i = 0; i < ns; i++) {
        int id = seen[i], c = shared[id], d = mine.n + h->sizes[id] - c;
        s[i].id = id; s[i].score = (double)c / (d ? d : 1);
    }
    qsort(s, (size_t)ns, sizeof *s, cmp_sim);
    bf_jv *out = bf_jv_arr();
    for (int i = 0; i < ns && i < k; i++) {
        const bf_jv *ex = h->examples->items[s[i].id];
        char *line = dp_fmt("%s -> %s", bf_jv_s(bf_jv_at(ex, 0)), bf_jv_s(bf_jv_at(ex, 1)));
        bf_jv_push(out, bf_jv_str(line)); free(line);
    }
    free(shared); free(seen); free(s); tokens_free(&mine);
    return out;
}

/* ── the run ────────────────────────────────────────────────────────────── */

static bf_jv *chart_json(const Chart *c) {
    bf_jv *a = bf_jv_arr();
    for (int i = 0; i < c->n; i++) { bf_jv *o = bf_jv_obj(); bf_jv_set(o, "name", bf_jv_str(c->a[i].name)); bf_jv_set(o, "description", bf_jv_str(c->a[i].description)); bf_jv_push(a, o); }
    return a;
}

static bf_jv *ledger_cases(Txn **rows, int n, const Chart *chart, char **masks, History *hist, const bf_jv *policy) {
    double max = bf_jv_n(bf_jv_get(policy, "max_candidates"), 20);
    if (chart->n > max - 1) dp_fail("a chart of more than %g accounts must be split; this one has %d", max - 1, chart->n);
    bf_jv *frontier = bf_jv_arr();
    for (int i = 0; i < chart->n; i++) {
        char *s = slug(chart->a[i].name);
        bf_jv *f = bf_jv_obj();
        bf_jv_set(f, "id", bf_jv_str(s));
        char *d = !strcmp(chart->a[i].description, chart->a[i].name) ? strdup(chart->a[i].name) : dp_fmt("%s: %s", chart->a[i].name, chart->a[i].description);
        bf_jv_set(f, "description", bf_jv_str(d)); free(d);
        char *ref = dp_fmt("Books:account/%s", s);
        bf_jv_set(f, "owner_ref", bf_jv_str(ref)); free(ref); free(s);
        bf_jv_push(frontier, f);
    }
    bf_jv *cases = bf_jv_arr();
    for (int i = 0; i < n; i++) {
        const Txn *x = rows[i];
        bf_jv *in = bf_jv_obj();
        bf_jv_set(in, "program", bf_jv_str("ledger_category"));
        bf_jv_set(in, "owner", bf_jv_str("Books"));
        char *subject = dp_fmt("books/line/%d", x->line);
        bf_jv_set(in, "subject_id", bf_jv_str(subject)); free(subject);
        bf_jv_set(in, "mask_digest", bf_jv_str(masks[i]));
        bf_jv_set(in, "gate_state", bf_jv_str("ADMIT"));
        bf_jv_set(in, "information_policy_class", bf_jv_str("private"));
        bf_jv_set(in, "frontier", bf_jv_clone(frontier));
        bf_jv *state = bf_jv_obj(), *t = bf_jv_obj();
        bf_jv_set(t, "description", bf_jv_str(x->description));
        if (!x->cents_null) {
            char *amt = format_cents(x->cents, 0);
            bf_jv_set(t, "amount", bf_jv_str(amt)); free(amt);
            bf_jv_set(t, "direction", bf_jv_str(x->cents < 0 ? "money out" : "money in"));
        }
        bf_jv_set(state, "transaction", t);
        bf_jv_set(state, "examples", hist ? similar(hist, x->description, 5) : bf_jv_arr());
        bf_jv_set(in, "state", state);
        bf_jv_push(cases, dp_compile_case(in, policy));
        bf_jv_free(in);
    }
    bf_jv_free(frontier);
    return cases;
}


bf_jv *run_books(BooksRun *run) {
    TxnFile f = read_transaction_file(run->transactions_text, &run->map);
    DpOwner *learned = run->learned_bytes ? learned_owner(run->learned_bytes, run->learned_len) : NULL;
    Chart chart = {0};
    if (run->chart_text && *run->chart_text) chart = read_chart(run->chart_text);
    else if (learned) {
        const bf_jv *acc = bf_jv_get(((Learned *)learned->ctx)->model, "accounts");
        chart.n = (int)acc->len; chart.a = calloc(acc->len + 1, sizeof(Account));
        for (size_t i = 0; i < acc->len; i++) { chart.a[i].name = strdup(acc->items[i]->s); chart.a[i].description = strdup(acc->items[i]->s); }
    } else dp_fail("give a chart of accounts (--chart) or learned books (--learned)");
    const bf_jv *policy = dp_policy();
    char **slugs = malloc(sizeof *slugs * (size_t)(chart.n + 1));
    for (int a = 0; a < chart.n; a++) slugs[a] = slug(chart.a[a].name);
    bf_jv *scope = bf_jv_obj();
    bf_jv_set(scope, "chart", chart_json(&chart));
    bf_jv_set(scope, "learned", learned ? bf_jv_str(learned->artifact_digest) : bf_jv_null());
    bf_jv_set(scope, "columns", bf_jv_clone(f.columns));
    char **masks = malloc(sizeof *masks * (size_t)(f.n ? f.n : 1));
    Txn **all = malloc(sizeof *all * (size_t)(f.n ? f.n : 1));
    for (int i = 0; i < f.n; i++) {
        all[i] = &f.t[i];
        bf_jv *m = bf_jv_clone(scope), *line = bf_jv_obj();
        bf_jv_set(line, "date", bf_jv_str(f.t[i].date));
        bf_jv_set(line, "description", bf_jv_str(f.t[i].description));
        bf_jv_set(line, "amount_text", bf_jv_str(f.t[i].amount_text));
        bf_jv_set(m, "line", line);
        masks[i] = dp_digest_str(m); bf_jv_free(m);
    }
    double started = dp_now_ms();
    bf_jv **first = calloc((size_t)(f.n ? f.n : 1), sizeof *first);
    if (learned && f.n) {
        bf_jv *cases = ledger_cases(all, f.n, &chart, masks, NULL, policy);
        bf_jv *rec = dp_assess_cases(cases, learned, f.n, policy);
        for (int i = 0; i < f.n; i++) first[i] = bf_jv_clone(rec->items[i]);
        bf_jv_free(rec); bf_jv_free(cases);
    }
    int *unsure = malloc(sizeof(int) * (size_t)(f.n ? f.n : 1)), nu = 0;
    for (int i = 0; i < f.n; i++) if (!first[i] || bf_jv_path(first[i], "answers.account.confidence")->n < 0.7) unsure[nu++] = i;
    DpOwner *model = NULL; bf_jv *second = NULL; char model_error[2048] = "";
    if (nu) {
        DpCatch c;
        if (DP_TRY(&c)) {
            model = dp_local_acquire(run->local);
            const bf_jv *examples = learned ? bf_jv_get(((Learned *)learned->ctx)->model, "examples") : NULL;
            History *hist = history_index(examples);
            Txn **rows = malloc(sizeof *rows * (size_t)nu); char **um = malloc(sizeof *um * (size_t)nu);
            for (int k = 0; k < nu; k++) { rows[k] = &f.t[unsure[k]]; um[k] = masks[unsure[k]]; }
            bf_jv *cases = ledger_cases(rows, nu, &chart, um, hist, policy);
            second = dp_assess_cases(cases, model, nu, policy);
            bf_jv_free(cases);
            DP_END(&c);
        } else { snprintf(model_error, sizeof model_error, "%s", c.msg); bf_jv_free(second); second = NULL; }
    }
    bf_jv **final = calloc((size_t)(f.n ? f.n : 1), sizeof *final);
    int *by_model = calloc((size_t)(f.n ? f.n : 1), sizeof(int));
    for (int i = 0; i < f.n; i++) final[i] = first[i];
    for (int k = 0; k < nu && second; k++) if (bf_jv_at(second, (size_t)k)) { final[unsure[k]] = second->items[k]; by_model[unsure[k]] = 1; }
    bf_jv *lines = bf_jv_arr();
    for (int i = 0; i < f.n; i++) {
        const Txn *x = &f.t[i];
        const bf_jv *receipt = final[i], *answer = receipt ? bf_jv_path(receipt, "answers.account") : NULL;
        const char *by = receipt && !by_model[i] ? "learned" : receipt ? "local model" : "none";
        const char *suggested = "none";
        if (answer) {
            const char *choice = bf_jv_s(bf_jv_get(answer, "choice"));
            for (int a = 0; a < chart.n; a++) if (!strcmp(slugs[a], choice)) suggested = chart.a[a].name;
        }
        const bf_jv *selection = receipt ? bf_jv_path(receipt, "proposal.selection") : NULL;
        double conf = answer ? bf_jv_get(answer, "confidence")->n : 0;
        int held = !strcmp(by, "learned") && selection && !strcmp(bf_jv_s(bf_jv_get(selection, "disposition")), "review");
        int review = !answer || !strcmp(bf_jv_s(bf_jv_get(answer, "choice")), "none") || conf < 0.5 || !strcmp(by, "local model") ||
                     x->amount_unread || (!strcmp(by, "learned") && conf < 0.7) || held;
        bf_jv *line = txn_json(x);
        bf_jv_set(line, "account", bf_jv_str(review ? "" : suggested));
        bf_jv_set(line, "suggested", bf_jv_str(suggested));
        bf_jv_set(line, "confidence", answer ? bf_jv_num(conf) : bf_jv_null());
        bf_jv_set(line, "decided_by", bf_jv_str(by));
        bf_jv_set(line, "review", bf_jv_bool(review));
        if (x->amount_unread) bf_jv_set(line, "review_reason", bf_jv_str("amount not read"));
        else if (held && conf >= 0.7) { char *r = dp_fmt("selection: %s", bf_jv_s(bf_jv_get(selection, "reason"))); bf_jv_set(line, "review_reason", bf_jv_str(r)); free(r); }
        if (receipt) bf_jv_set(line, "receipt", bf_jv_clone(bf_jv_get(receipt, "response_digest")));
        bf_jv_push(lines, line);
    }
    /* counts and exact totals by account, in first-booked order */
    char **acc = malloc(sizeof *acc * (size_t)(f.n ? f.n : 1)); int *cnt = calloc((size_t)(f.n ? f.n : 1), sizeof(int));
    __int128 *tot = calloc((size_t)(f.n ? f.n : 1), sizeof(__int128)); int *has_tot = calloc((size_t)(f.n ? f.n : 1), sizeof(int)); int nacc = 0;
    for (size_t i = 0; i < lines->len; i++) {
        const char *a = bf_jv_s(bf_jv_get(lines->items[i], "account"));
        if (!*a) continue;
        int k = 0; while (k < nacc && strcmp(acc[k], a)) k++;
        if (k == nacc) acc[nacc++] = (char *)a;
        cnt[k]++;
        if (!f.t[i].cents_null) { tot[k] += (__int128)f.t[i].cents; has_tot[k] = 1; }
    }
    int unsafe = 0;
    for (int k = 0; k < nacc; k++) if (has_tot[k] && (tot[k] > (__int128)MAX_SAFE || tot[k] < -(__int128)MAX_SAFE)) unsafe = 1;
    int n_learned = 0, n_model = 0, n_review = 0, n_unread = 0, scored = 0, booked = 0, booked_ok = 0, suggested_ok = 0;
    for (size_t i = 0; i < lines->len; i++) {
        const bf_jv *l = lines->items[i];
        int rv = bf_jv_get(l, "review")->b;
        const char *by = bf_jv_s(bf_jv_get(l, "decided_by"));
        if (!strcmp(by, "learned") && !rv) n_learned++;
        if (!strcmp(by, "local model")) n_model++;
        if (rv) n_review++;
        if (bf_jv_get(l, "amount_unread")->b) n_unread++;
        const char *truth = bf_jv_s(bf_jv_get(l, "truth"));
        if (truth) {
            scored++;
            if (!rv) { booked++; if (!strcmp(bf_jv_s(bf_jv_get(l, "account")), truth)) booked_ok++; }
            if (!strcmp(bf_jv_s(bf_jv_get(l, "suggested")), truth)) suggested_ok++;
        }
    }
    bf_jv *report = bf_jv_obj();
    bf_jv_set(report, "schema", bf_jv_str("bonfyre.books.v3"));
    bf_jv_set(report, "lines", bf_jv_num((double)lines->len));
    bf_jv_set(report, "by_learned", bf_jv_num(n_learned));
    bf_jv_set(report, "suggested_by_model", bf_jv_num(n_model));
    bf_jv_set(report, "to_review", bf_jv_num(n_review));
    bf_jv_set(report, "amounts_unread", bf_jv_num(n_unread));
    if (learned) {
        bf_jv *l = bf_jv_obj();
        bf_jv_set(l, "artifact_sha256", bf_jv_str(learned->artifact_digest));
        bf_jv_set(l, "lines", bf_jv_clone(bf_jv_path(((Learned *)learned->ctx)->model, "learned_from.lines")));
        bf_jv_set(report, "learned", l);
    } else bf_jv_set(report, "learned", bf_jv_null());
    bf_jv_set(report, "model", model ? bf_jv_str(model->model) : bf_jv_null());
    bf_jv_set(report, "model_artifact_sha256", model ? bf_jv_str(model->artifact_digest) : bf_jv_null());
    if (*model_error) bf_jv_set(report, "model_error", bf_jv_str(model_error));
    bf_jv_set(report, "seconds", bf_jv_num(js_round(dp_now_ms() - started) / 1000));
    if (scored) {
        bf_jv *c = bf_jv_obj();
        bf_jv_set(c, "lines", bf_jv_num(scored)); bf_jv_set(c, "booked", bf_jv_num(booked));
        bf_jv_set(c, "booked_correct", bf_jv_num(booked_ok)); bf_jv_set(c, "suggested_correct_including_review", bf_jv_num(suggested_ok));
        bf_jv_set(report, "checked_against_given_accounts", c);
    }
    int *ord = malloc(sizeof(int) * (size_t)(nacc ? nacc : 1));
    for (int k = 0; k < nacc; k++) ord[k] = k;
    for (int a = 1; a < nacc; a++) for (int b = a; b > 0; b--) {
        int x = ord[b - 1], y = ord[b];
        int swap = cnt[y] > cnt[x] || (cnt[y] == cnt[x] && strcmp(acc[y], acc[x]) < 0);
        if (!swap) break;
        ord[b] = x; ord[b - 1] = y;
    }
    bf_jv *bba = bf_jv_arr();
    for (int k = 0; k < nacc; k++) { bf_jv *o = bf_jv_obj(); bf_jv_set(o, "account", bf_jv_str(acc[ord[k]])); bf_jv_set(o, "booked", bf_jv_num(cnt[ord[k]])); bf_jv_push(bba, o); }
    bf_jv_set(report, "booked_by_account", bba);
    int ntot = 0; int *tk = malloc(sizeof(int) * (size_t)(nacc ? nacc : 1));
    if (!unsafe) for (int k = 0; k < nacc; k++) if (has_tot[k]) tk[ntot++] = k;
    for (int a = 1; a < ntot; a++) for (int b = a; b > 0 && (double)tot[tk[b - 1]] > (double)tot[tk[b]]; b--) { int t = tk[b]; tk[b] = tk[b - 1]; tk[b - 1] = t; }
    bf_jv *totals = bf_jv_arr();
    for (int k = 0; k < ntot; k++) {
        bf_jv *o = bf_jv_obj(); char *s = format_cents((double)tot[tk[k]], 0);
        bf_jv_set(o, "account", bf_jv_str(acc[tk[k]])); bf_jv_set(o, "total", bf_jv_str(s)); free(s);
        bf_jv_push(totals, o);
    }
    bf_jv_set(report, "totals", totals);
    if (!ntot) bf_jv_set(report, "totals_unavailable", bf_jv_str(unsafe ? "a total is larger than can be added exactly, so there are line counts by account and no money totals"
                                                                   : "the file has no amounts, so there are line counts by account and no money totals"));
    bf_jv_set(report, "transactions", lines);
    bf_jv *recs = bf_jv_arr(); for (int i = 0; i < f.n; i++) bf_jv_push(recs, final[i] ? bf_jv_clone(final[i]) : bf_jv_null());
    bf_jv_set(report, "receipts", recs);
    bf_jv_set(report, "cannot_infer", bf_jv_str("a booked line -> a reviewed ledger; the client or their bookkeeper still owns the books, and a line sent to review is not booked"));
    bf_jv_free(scope);
    return report;
}

static char *books_csv(const bf_jv *report) {
    bf_re_buf b = {0};
    const char *head = "date,description,amount,amount_as_given,account,suggested,confidence,decided_by,needs_review";
    bf_re_append(&b, head, strlen(head));
    const bf_jv *tx = bf_jv_get(report, "transactions");
    for (size_t i = 0; i < tx->len; i++) {
        const bf_jv *r = tx->items[i];
        const bf_jv *cents = bf_jv_get(r, "amount_cents"), *conf = bf_jv_get(r, "confidence");
        char *cells[9];
        cells[0] = strdup(bf_jv_s(bf_jv_get(r, "date"))); cells[1] = strdup(bf_jv_s(bf_jv_get(r, "description")));
        cells[2] = format_cents(bf_jv_n(cents, 0), cents->type == BF_JV_NULL);
        cells[3] = strdup(bf_jv_s(bf_jv_get(r, "amount_text"))); cells[4] = strdup(bf_jv_s(bf_jv_get(r, "account")));
        cells[5] = strdup(bf_jv_s(bf_jv_get(r, "suggested")));
        cells[6] = conf->type == BF_JV_NULL ? strdup("") : dp_to_fixed(conf->n, 2);
        cells[7] = strdup(bf_jv_s(bf_jv_get(r, "decided_by"))); cells[8] = strdup(bf_jv_get(r, "review")->b ? "yes" : "no");
        bf_re_append(&b, "\n", 1);
        for (int c = 0; c < 9; c++) { char *q = csv_cell(cells[c]); if (c) bf_re_append(&b, ",", 1); bf_re_append(&b, q, strlen(q)); free(q); free(cells[c]); }
    }
    bf_re_append(&b, "\n", 1);
    return b.buf;
}


#include "usage.h"

static int ends_with_ci(const char *s, const char *suffix) {
    size_t n = strlen(s), m = strlen(suffix);
    return n >= m && !strcasecmp(s + n - m, suffix);
}
static const char *str_flag(const DpFlags *fl, const char *k) { const char *v = dp_flag(fl, k); return v && strcmp(v, DP_TRUE) ? v : NULL; }

int books_main(int argc, char **argv) {
    if (argc > 0 && (!strcmp(argv[0], "--help") || !strcmp(argv[0], "-h") || !strcmp(argv[0], "help"))) { fputs(BOOKS_USAGE, stdout); return 0; }
    DpFlags fl;
    if (argc > 0 && !strcmp(argv[0], "inspect")) {
        dp_flags(&fl, argc - 1, argv + 1, NULL);
        if (!str_flag(&fl, "workbook")) dp_fail("bonfyre books inspect needs --workbook FILE");
        bf_jv *r = books_inspect_workbook(str_flag(&fl, "workbook"));
        char *s = bf_jv_dump(r, 2, BF_JV_SORT_NONE, NULL); printf("%s\n", s); free(s);
        return 0;
    }
    if (argc > 0 && !strcmp(argv[0], "review")) {   /* a Books report (JSON) as the review workbook */
        dp_flags(&fl, argc - 1, argv + 1, NULL);
        const char *in = str_flag(&fl, "report"), *outp = str_flag(&fl, "out");
        if (!in || !outp) dp_fail("bonfyre books review needs --report FILE and --out FILE");
        size_t len; char *text = read_text(in, &len), err[128];
        bf_jv *report = bf_jv_parse(text, len, err, sizeof err);
        if (!report) dp_fail("Invalid Books report.");
        size_t n; char *x = books_review_workbook(report, &n);
        dp_write_file(outp, x, n);
        return 0;
    }
    if (argc > 0 && !strcmp(argv[0], "learn")) {
        dp_flags(&fl, argc - 1, argv + 1, NULL);
        if (!dp_flag(&fl, "history") || !dp_flag(&fl, "out")) dp_fail("bonfyre books learn needs --history FILE and --out FILE");
        Mapping map = parse_columns(dp_flag(&fl, "columns"));
        char *history = read_text(dp_flag(&fl, "history"), NULL);
        bf_jv *learned = learn_books(history, &map, 8);
        size_t n; char *s = bf_jv_dump(learned, 0, BF_JV_SORT_NONE, &n);
        char *withnl = dp_fmt("%s\n", s);
        dp_write_file(dp_flag(&fl, "out"), withnl, n + 1);
        char ln[32]; bf_js_number(bf_jv_path(learned, "learned_from.lines")->n, ln);
        printf("Learned %zu accounts from %s lines; written to %s.\n", bf_jv_get(learned, "accounts")->len, ln, dp_flag(&fl, "out"));
        return 0;
    }
    dp_flags(&fl, argc, argv, NULL);
    const char *tx = dp_flag(&fl, "transactions"), *chart = dp_flag(&fl, "chart"), *learned = dp_flag(&fl, "learned"), *out = dp_flag(&fl, "out");
    if (!tx || (!chart && !learned)) dp_fail("bonfyre books needs --transactions FILE and --learned FILE or --chart FILE (see bonfyre books --help)");
    if (out) {
        const char *inputs[3] = {tx, chart, learned};
        for (int i = 0; i < 3; i++) {
            if (!inputs[i]) continue;
            char ra[4096], rb[4096];
            int same = realpath(out, ra) && realpath(inputs[i], rb) && !strcmp(ra, rb);
            if (!same) {   /* resolve() does not need the file to exist */
                char *a = out[0] == '/' ? strdup(out) : dp_fmt("%s/%s", getcwd(ra, sizeof ra) ? ra : ".", out);
                char *b = inputs[i][0] == '/' ? strdup(inputs[i]) : dp_fmt("%s/%s", getcwd(rb, sizeof rb) ? rb : ".", inputs[i]);
                same = !strcmp(a, b); free(a); free(b);
            }
            struct stat sa, sb;
            if (stat(inputs[i], &sb)) fs_fail("stat", inputs[i]);
            if (same || (!stat(out, &sa) && sa.st_dev == sb.st_dev && sa.st_ino == sb.st_ino)) dp_fail("Choose an output file different from every input file.");
        }
    }
    DpLocal local;
    dp_local_init(&local, dp_flag(&fl, "model-endpoint"), dp_flag_is_true(&fl, "model"), str_flag(&fl, "model"));
    BooksRun run = {0};
    run.local = &local;
    bf_jv *report = NULL;
    DpCatch c; char failure[2048] = "";
    if (DP_TRY(&c)) {
        run.chart_text = read_text(chart && strcmp(chart, DP_TRUE) ? chart : NULL, NULL);
        if (learned) { if (!strcmp(learned, DP_TRUE)) dp_fail("The \"path\" argument must be of type string or an instance of Buffer or URL. Received type boolean (true)"); run.learned_bytes = read_text(learned, &run.learned_len); }
        run.map = parse_columns(dp_flag(&fl, "columns"));
        if (ends_with_ci(tx, ".xlsx")) report = books_run_workbook(tx, str_flag(&fl, "sheet"), str_flag(&fl, "header-row"), &run, run_books);
        else { run.transactions_text = read_text(strcmp(tx, DP_TRUE) ? tx : NULL, NULL); report = run_books(&run); }
        DP_END(&c);
    } else snprintf(failure, sizeof failure, "%s", c.msg);
    dp_local_close(&local);   /* finally */
    if (*failure) dp_fail("%s", failure);
    if (out) {
        if (ends_with_ci(out, ".xlsx")) { size_t n; char *x = books_review_workbook(report, &n); dp_write_file(out, x, n); }
        else if (strlen(out) >= 5 && !strcmp(out + strlen(out) - 5, ".json")) {
            size_t n; char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, &n); char *w = dp_fmt("%s\n", s); dp_write_file(out, w, n + 1);
        } else { char *s = books_csv(report); dp_write_file(out, s, strlen(s)); }
    }
    if (dp_flag(&fl, "json")) { char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, NULL); printf("%s\n", s); return 0; }
    const bf_jv *check = bf_jv_get(report, "checked_against_given_accounts");
#define JN(v) js_num(v, alloca(32))
    printf("%s lines: %s booked from what your books taught it, %s held for review", JN(bf_jv_get(report, "lines")),
           JN(bf_jv_get(report, "by_learned")), JN(bf_jv_get(report, "to_review")));
    if (bf_jv_n(bf_jv_get(report, "suggested_by_model"), 0)) printf(" (%s with a suggestion from the local model)", JN(bf_jv_get(report, "suggested_by_model")));
    printf(".\n");
    if (bf_jv_n(bf_jv_get(report, "amounts_unread"), 0)) printf("%s amounts did not read as money and are held as they were written.\n", JN(bf_jv_get(report, "amounts_unread")));
    if (bf_jv_s(bf_jv_get(report, "model_error"))) printf("The local model was not used: %s\n", bf_jv_s(bf_jv_get(report, "model_error")));
    if (check) printf("Against the given accounts: %s of %s booked lines are right; %s of %s suggestions are right, review included.\n",
                      JN(bf_jv_get(check, "booked_correct")), JN(bf_jv_get(check, "booked")),
                      JN(bf_jv_get(check, "suggested_correct_including_review")), JN(bf_jv_get(check, "lines")));
    printf("%ss on this machine", JN(bf_jv_get(report, "seconds")));
    if (bf_jv_s(bf_jv_get(report, "model"))) printf(" (local model %s)", bf_jv_s(bf_jv_get(report, "model")));
    printf(".");
    if (out) printf(" Written to %s.", out);
    printf("\n");
    return 0;
}
