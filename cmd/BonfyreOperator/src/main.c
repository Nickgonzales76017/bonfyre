/*
 * bonfyre-operator — read a household projection and say what its sources
 * support. C port of the former src/main.ts (a 63 MB `bun build --compile`
 * executable). Output is byte-identical to that program, with one correction:
 * the TS version appended evidence_ref/source_url into the record's own
 * source_refs array while listing it, so the same record printed and digested
 * differently depending on the operator. Here records are never mutated.
 * tests/parity.sh holds the TS oracle (with that one line fixed) to this.
 */
#include "bf_json.h"
#include "bf_jsonw.h"
#include "bonfyre.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define DEFAULT_STATE ".bonfyre/run8-r2c/20260823T032652Z-27525"

typedef struct {
    char *address, *kind, *line;
    const char *source_file;
    bf_json_doc_t *doc;
    const bf_json_node_t *root;
} Rec;

static Rec *book; static int nbook;
static bf_jsonw out;

static void die(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    fputs("bonfyre-operator: ", stderr); vfprintf(stderr, fmt, ap); fputc('\n', stderr);
    va_end(ap); exit(2);
}

static const bf_json_node_t *get(const Rec *r, const char *key) {
    if (!r->root || r->root->type != BF_JSON_OBJECT) return NULL;
    const bf_json_node_t *found = NULL;   /* JSON.parse keeps the last duplicate */
    size_t kl = strlen(key);
    for (const bf_json_node_t *c = bf_json_child_first(r->doc, r->root); c; c = bf_json_child_next(r->doc, c))
        if (!bf_json_rawcmp(c->key, c->key_len, key, (int)kl)) found = c;
    return found;
}
static char *str_of(const bf_json_node_t *n) {
    return n && n->type == BF_JSON_STRING ? bf_json_unescape(n->val.str.ptr, n->val.str.len, NULL) : NULL;
}
static int nodes_equal(const Rec *a, const bf_json_node_t *x, const Rec *b, const bf_json_node_t *y) {
    char h1[65], h2[65];
    bf_jsonw_digest(a->doc, x, h1); bf_jsonw_digest(b->doc, y, h2);
    return !strcmp(h1, h2);
}
static double num(const bf_json_node_t *n) { return n->type == BF_JSON_INT ? (double)n->val.i : n->val.d; }
static int is_num(const bf_json_node_t *n) { return n && (n->type == BF_JSON_INT || n->type == BF_JSON_DOUBLE); }

/* ── book ─────────────────────────────────────────────────────────────── */

static char *slurp(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb"); if (!f) return NULL;
    fseek(f, 0, SEEK_END); long n = ftell(f); fseek(f, 0, SEEK_SET);
    char *b = malloc((size_t)n + 1); size_t got = fread(b, 1, (size_t)n, f); fclose(f);
    b[got] = 0; if (len) *len = got; return b;
}

static int cmp_addr(const void *a, const void *b) { return strcmp(((const Rec *)a)->address, ((const Rec *)b)->address); }
static Rec *sorted_book;

static void load_book(const char *state) {
    static const char *files[2][2] = {{"food", "household/food/current-offers.jsonl"},
                                      {"housing", "household/housing/current-listings.jsonl"}};
    int cap = 256; book = malloc(sizeof *book * (size_t)cap);
    for (int f = 0; f < 2; f++) {
        const char *domain = files[f][0], *rel = files[f][1];
        char path[4096]; snprintf(path, sizeof path, "%s/%s", state, rel);
        size_t len; char *text = slurp(path, &len);
        if (!text) die("missing current household projection: %s", rel);
        int index = 0;
        for (char *line = text; line; index++) {
            char *nl = strchr(line, '\n'); if (nl) *nl = 0;
            size_t ll = strlen(line); if (ll && line[ll - 1] == '\r') line[--ll] = 0;
            char *next = nl ? nl + 1 : NULL;
            int blank = 1; for (size_t i = 0; i < ll; i++) if (!strchr(" \t\r\n\v\f", line[i])) { blank = 0; break; }
            if (!blank) {
                Rec r = {0};
                r.line = line; r.source_file = rel;
                r.doc = bf_json_parse(line, ll, NULL, 0);
                if (!r.doc) die("invalid JSON in %s:%d", rel, index + 1);
                r.root = bf_json_root(r.doc);
                const char *idkeys[3] = {"offer_id", "listing_id", "candidate_id"};
                const char *kinds[3] = {"offer", "listing", "observation"};
                char *id = NULL; const char *kind = NULL;
                for (int k = 0; k < 3 && !id; k++) {
                    if ((k == 0) != (f == 0)) continue;
                    char *v = str_of(get(&r, idkeys[k]));
                    if (v && *v) { id = v; kind = kinds[k]; } else free(v);
                }
                if (!id) die("source record has no supported stable identity for %s", domain);
                size_t al = strlen(domain) + strlen(kind) + strlen(id) + 16;
                r.address = malloc(al); snprintf(r.address, al, "household:%s:%s:%s", domain, kind, id);
                r.kind = malloc(al); snprintf(r.kind, al, "%s.%s", domain, kind);
                free(id);
                for (int i = 0; i < nbook; i++) if (!strcmp(book[i].address, r.address)) die("duplicate household address: %s", r.address);
                if (nbook == cap) book = realloc(book, sizeof *book * (size_t)(cap *= 2));
                book[nbook++] = r;
            }
            line = next;
        }
    }
    if (!nbook) die("no addressable household records");
    sorted_book = malloc(sizeof *book * (size_t)nbook);
    memcpy(sorted_book, book, sizeof *book * (size_t)nbook);
    qsort(sorted_book, (size_t)nbook, sizeof *book, cmp_addr);
}

static Rec *require_record(const char *address) {
    Rec key = {.address = (char *)address};
    Rec *r = bsearch(&key, sorted_book, (size_t)nbook, sizeof *book, cmp_addr);
    if (!r) die("unknown household address: %s", address);
    return r;
}

/* ── shared pieces ────────────────────────────────────────────────────── */

static int cmp_str(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
static int uniq_sorted(char **v, int n) {
    qsort(v, (size_t)n, sizeof *v, cmp_str);
    int k = 0;
    for (int i = 0; i < n; i++) if (!k || strcmp(v[k - 1], v[i])) v[k++] = v[i];
    return k;
}

static void program(const char *op, char **reads, int n) {
    char **ordered = malloc(sizeof *ordered * (size_t)(n ? n : 1));
    memcpy(ordered, reads, sizeof *ordered * (size_t)n);
    n = uniq_sorted(ordered, n);
    bf_jsonw c; bf_jsonw_init(&c, 0);
    bf_jsonw_arr(&c); for (int i = 0; i < n; i++) bf_jsonw_str(&c, ordered[i]); bf_jsonw_arr_end(&c);
    char h[65]; bf_sha256_hex((const uint8_t *)c.buf, c.len, h);
    char lower[32]; size_t i;
    for (i = 0; op[i] && i < sizeof lower - 1; i++) lower[i] = (char)(op[i] >= 'A' && op[i] <= 'Z' ? op[i] + 32 : op[i]);
    lower[i] = 0;
    char id[96]; snprintf(id, sizeof id, "household.%s.%.16s", lower, h);
    bf_jsonw_reset(&c);
    bf_jsonw_obj(&c);
    bf_jsonw_key(&c, "constraints"); bf_jsonw_arr(&c); bf_jsonw_arr_end(&c);
    bf_jsonw_key(&c, "id"); bf_jsonw_str(&c, id);
    bf_jsonw_key(&c, "operator"); bf_jsonw_str(&c, op);
    bf_jsonw_key(&c, "reads"); bf_jsonw_arr(&c); for (int j = 0; j < n; j++) bf_jsonw_str(&c, ordered[j]); bf_jsonw_arr_end(&c);
    bf_jsonw_key(&c, "writes"); bf_jsonw_arr(&c); bf_jsonw_arr_end(&c);
    bf_jsonw_obj_end(&c);
    char d[65]; bf_sha256_hex((const uint8_t *)c.buf, c.len, d);
    bf_jsonw_free(&c);

    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "program_id"); bf_jsonw_str(&out, id);
    bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, op);
    bf_jsonw_key(&out, "digest"); bf_jsonw_str(&out, d);
    bf_jsonw_key(&out, "reads"); bf_jsonw_arr(&out); for (int j = 0; j < n; j++) bf_jsonw_str(&out, ordered[j]); bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "admitted"); bf_jsonw_bool(&out, 1);
    bf_jsonw_key(&out, "hard_failures"); bf_jsonw_arr(&out); bf_jsonw_arr_end(&out);
    bf_jsonw_obj_end(&out);
    free(ordered);
}
static void program1(const char *op, const char *a) { char *r[1] = {(char *)a}; program(op, r, 1); }
static void program2(const char *op, const char *a, const char *b) { char *r[2] = {(char *)a, (char *)b}; program(op, r, 2); }

static void source_refs(const Rec *r) {
    char *v[4096]; int n = 0;
    const bf_json_node_t *refs = get(r, "source_refs");
    if (refs && refs->type == BF_JSON_ARRAY) {
        for (const bf_json_node_t *c = bf_json_child_first(r->doc, refs); c && n < 4094; c = bf_json_child_next(r->doc, c)) {
            char *s = str_of(c); if (s && *s) v[n++] = s; else free(s);
        }
    } else { char *s = str_of(refs); if (s && *s) v[n++] = s; else free(s); }
    const char *more[2] = {"evidence_ref", "source_url"};
    for (int i = 0; i < 2; i++) { char *s = str_of(get(r, more[i])); if (s && *s) v[n++] = s; else free(s); }
    n = uniq_sorted(v, n);
    bf_jsonw_arr(&out); for (int i = 0; i < n; i++) bf_jsonw_str(&out, v[i]); bf_jsonw_arr_end(&out);
}

static void digest_field(const Rec *r) { char h[65]; bf_jsonw_digest(r->doc, r->root, h); bf_jsonw_str(&out, h); }

static void summary(const Rec *r) {
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "address"); bf_jsonw_str(&out, r->address);
    bf_jsonw_key(&out, "kind"); bf_jsonw_str(&out, r->kind);
    bf_jsonw_key(&out, "content_sha256"); digest_field(r);
    bf_jsonw_key(&out, "source_file"); bf_jsonw_str(&out, r->source_file);
    bf_jsonw_key(&out, "source_refs"); source_refs(r);
    bf_jsonw_key(&out, "record"); bf_jsonw_node(&out, r->doc, r->root, 0);
    bf_jsonw_obj_end(&out);
}

/* String(value) for a JSON value, as `matches` compares it. Returns malloc'd text. */
static void cat(char **s, size_t *n, const char *t, size_t tl) {
    *s = realloc(*s, *n + tl + 1); memcpy(*s + *n, t, tl); *n += tl; (*s)[*n] = 0;
}
static void js_string_into(const bf_json_doc_t *d, const bf_json_node_t *v, char **s, size_t *n) {
    char b[32];
    switch (v->type) {
    case BF_JSON_STRING: { size_t l; char *t = bf_json_unescape(v->val.str.ptr, v->val.str.len, &l); cat(s, n, t, l); free(t); break; }
    case BF_JSON_INT: case BF_JSON_DOUBLE: cat(s, n, b, (size_t)bf_js_number(num(v), b)); break;
    case BF_JSON_BOOL: cat(s, n, v->val.b ? "true" : "false", v->val.b ? 4 : 5); break;
    case BF_JSON_NULL: cat(s, n, "null", 4); break;
    case BF_JSON_OBJECT: cat(s, n, "[object Object]", 15); break;
    case BF_JSON_ARRAY: {
        int first = 1;
        for (const bf_json_node_t *c = bf_json_child_first(d, v); c; c = bf_json_child_next(d, c)) {
            if (!first) cat(s, n, ",", 1);
            first = 0;
            if (c->type != BF_JSON_NULL) js_string_into(d, c, s, n);
        }
        break;
    }
    }
}

static int matches(const Rec *r, const char *predicate) {
    const char *eq = strchr(predicate, '=');
    size_t plen = strlen(predicate);
    if (!eq || eq == predicate || eq == predicate + plen - 1) die("predicate must be field=value: %s", predicate);
    char field[512]; size_t fl = (size_t)(eq - predicate); if (fl >= sizeof field) fl = sizeof field - 1;
    memcpy(field, predicate, fl); field[fl] = 0;
    const char *expected = eq + 1;
    if (strchr(field, '.')) die("only top-level field=value predicates are supported: %s", predicate);
    const bf_json_node_t *v = get(r, field);
    if (!v) return 0;
    if (v->type == BF_JSON_NULL) return !strcmp(expected, "null");
    if (v->type == BF_JSON_BOOL) return !strcasecmp(expected, v->val.b ? "true" : "false");
    char *s = calloc(1, 1); size_t n = 0;
    js_string_into(r->doc, v, &s, &n);
    int ok = n == strlen(expected) && !memcmp(s, expected, n);
    free(s);
    return ok;
}

/* ── diff and compare ─────────────────────────────────────────────────── */

typedef struct { char *key; const bf_json_node_t *node; } Kid;
static int kids_of(const bf_json_doc_t *d, const bf_json_node_t *o, Kid *k, int max) {
    int n = 0;
    for (const bf_json_node_t *c = bf_json_child_first(d, o); c && n < max; c = bf_json_child_next(d, c)) {
        char *key = bf_json_unescape(c->key, c->key_len, NULL);
        int dup = -1; for (int i = 0; i < n; i++) if (!strcmp(k[i].key, key)) dup = i;
        if (dup >= 0) { k[dup].node = c; free(key); } else { k[n].key = key; k[n].node = c; n++; }
    }
    return n;
}
static const bf_json_node_t *kid(Kid *k, int n, const char *key) {
    for (int i = 0; i < n; i++) if (!strcmp(k[i].key, key)) return k[i].node;
    return NULL;
}
static int is_obj(const bf_json_node_t *n) { return n && n->type == BF_JSON_OBJECT; }

static void field_diff(const Rec *a, const bf_json_node_t *l, const Rec *b, const bf_json_node_t *r, const char *path) {
    if (is_obj(l) && is_obj(r)) {
        Kid lk[1024], rk[1024]; int ln = kids_of(a->doc, l, lk, 1024), rn = kids_of(b->doc, r, rk, 1024);
        char *keys[2048]; int n = 0;
        for (int i = 0; i < ln; i++) keys[n++] = lk[i].key;
        for (int i = 0; i < rn; i++) keys[n++] = rk[i].key;
        n = uniq_sorted(keys, n);
        for (int i = 0; i < n; i++) {
            size_t pl = strlen(path) + strlen(keys[i]) + 2; char *sub = malloc(pl);
            snprintf(sub, pl, "%s.%s", path, keys[i]);
            const bf_json_node_t *x = kid(lk, ln, keys[i]), *y = kid(rk, rn, keys[i]);
            if (!x || !y) {
                bf_jsonw_obj(&out);
                bf_jsonw_key(&out, "path"); bf_jsonw_str(&out, sub);
                bf_jsonw_key(&out, "left"); if (x) bf_jsonw_node(&out, a->doc, x, 0); else bf_jsonw_null(&out);
                bf_jsonw_key(&out, "right"); if (y) bf_jsonw_node(&out, b->doc, y, 0); else bf_jsonw_null(&out);
                bf_jsonw_obj_end(&out);
            } else field_diff(a, x, b, y, sub);
            free(sub);
        }
        return;
    }
    if (nodes_equal(a, l, b, r)) return;
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "path"); bf_jsonw_str(&out, path);
    bf_jsonw_key(&out, "left"); bf_jsonw_node(&out, a->doc, l, 0);
    bf_jsonw_key(&out, "right"); bf_jsonw_node(&out, b->doc, r, 0);
    bf_jsonw_obj_end(&out);
}

static void compare(const Rec *l, const Rec *r) {
    Kid lk[1024], rk[1024];
    int ln = is_obj(l->root) ? kids_of(l->doc, l->root, lk, 1024) : 0;
    int rn = is_obj(r->root) ? kids_of(r->doc, r->root, rk, 1024) : 0;
    char *shared[1024]; int n = 0;
    for (int i = 0; i < ln; i++) if (kid(rk, rn, lk[i].key)) shared[n++] = lk[i].key;
    qsort(shared, (size_t)n, sizeof *shared, cmp_str);
    int eq[1024];
    for (int i = 0; i < n; i++) eq[i] = nodes_equal(l, kid(lk, ln, shared[i]), r, kid(rk, rn, shared[i]));

    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "COMPARE");
    bf_jsonw_key(&out, "program"); program2("COMPARE", l->address, r->address);
    bf_jsonw_key(&out, "left"); bf_jsonw_str(&out, l->address);
    bf_jsonw_key(&out, "right"); bf_jsonw_str(&out, r->address);
    bf_jsonw_key(&out, "same_kind"); bf_jsonw_bool(&out, !strcmp(l->kind, r->kind));
    bf_jsonw_key(&out, "equal_fields"); bf_jsonw_arr(&out);
    for (int i = 0; i < n; i++) if (eq[i]) bf_jsonw_str(&out, shared[i]);
    bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "different_fields"); bf_jsonw_arr(&out);
    for (int i = 0; i < n; i++) if (!eq[i]) bf_jsonw_str(&out, shared[i]);
    bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "numeric_deltas"); bf_jsonw_arr(&out);
    for (int i = 0; i < n; i++) {
        if (eq[i]) continue;
        const bf_json_node_t *a = kid(lk, ln, shared[i]), *b = kid(rk, rn, shared[i]);
        if (!is_num(a) || !is_num(b)) continue;
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "field"); bf_jsonw_str(&out, shared[i]);
        bf_jsonw_key(&out, "left"); bf_jsonw_num(&out, num(a));
        bf_jsonw_key(&out, "right"); bf_jsonw_num(&out, num(b));
        bf_jsonw_key(&out, "right_minus_left"); bf_jsonw_num(&out, num(b) - num(a));
        bf_jsonw_obj_end(&out);
    }
    bf_jsonw_arr_end(&out);
    bf_jsonw_obj_end(&out);
}

/* JS truthiness and strict equality for the two reconcile fields. */
static int truthy(const bf_json_node_t *n) {
    if (!n) return 0;
    switch (n->type) {
    case BF_JSON_NULL: return 0;
    case BF_JSON_BOOL: return n->val.b;
    case BF_JSON_INT: case BF_JSON_DOUBLE: return num(n) != 0 && num(n) == num(n);
    case BF_JSON_STRING: return n->val.str.len > 0;
    default: return 1;
    }
}
static int strict_eq(const Rec *a, const bf_json_node_t *x, const Rec *b, const bf_json_node_t *y) {
    if (!x || !y) return !x && !y;
    if (x->type == BF_JSON_OBJECT || x->type == BF_JSON_ARRAY || y->type == BF_JSON_OBJECT || y->type == BF_JSON_ARRAY)
        return a == b && x == y;
    if (is_num(x) && is_num(y)) return num(x) == num(y);
    if (x->type != y->type) return 0;
    return nodes_equal(a, x, b, y);
}

/* ── arguments ────────────────────────────────────────────────────────── */

static const char *option(int argc, char **argv, const char *name) {
    for (int i = 0; i < argc; i++) if (!strcmp(argv[i], name)) return i + 1 < argc ? argv[i + 1] : NULL;
    return NULL;
}
static int options(int argc, char **argv, const char *name, const char **vals) {
    int n = 0;
    for (int i = 0; i < argc; i++) if (!strcmp(argv[i], name) && i + 1 < argc && *argv[i + 1]) vals[n++] = argv[i + 1];
    return n;
}

static int mkdirs(char *path) {
    for (char *p = path + 1; *p; p++) if (*p == '/') { *p = 0; if (mkdir(path, 0777) && errno != EEXIST) return -1; *p = '/'; }
    return mkdir(path, 0777) && errno != EEXIST ? -1 : 0;
}

static const char HELP[] =
    "bonfyre-operator -- read a household projection and say what its sources support\n"
    "\n"
    "  bonfyre-operator select [--prefix P] [--kind K] [--where FIELD=VALUE]...\n"
    "  bonfyre-operator why ADDRESS [--field F]          the record, its source and digest\n"
    "  bonfyre-operator project ADDRESS --field F...     only the named fields\n"
    "  bonfyre-operator diff|compare|reconcile A B       how two records differ\n"
    "  bonfyre-operator prove ADDRESS --claim C          which fields were observed for a claim; never a proof\n"
    "  bonfyre-operator route                            which organ owns structured queries\n"
    "\n"
    "  --state DIR     the projection to read (default " DEFAULT_STATE ")\n"
    "  --output FILE   also write the JSON result to FILE\n";

static const char *const proof_claims[4] = {"availability", "dog_access", "effective_cost", "price"};
static const char *const proof_fields[4][5] = {
    {"availability_status", "availability_claim", "available_text"},
    {"pet_status", "dogs_stated_ok"},
    {"advertised_base_rent_monthly_usd", "advertised_monthly_rent_usd", "price_usd_month"},
    {"cash_price_usd", "advertised_base_rent_monthly_usd", "advertised_monthly_rent_usd", "price_usd_month"},
};

static void execute(const char *op, int argc, char **args, const char *state) {
    if (!strcmp(op, "route")) {
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "ROUTE");
        bf_jsonw_key(&out, "program"); program1("ROUTE", "command:BonfyreQuery");
        bf_jsonw_key(&out, "owner"); bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "identity"); bf_jsonw_str(&out, "BonfyreQuery");
        bf_jsonw_key(&out, "classification");
        bf_jsonw_str(&out, access("cmd/BonfyreQuery/bonfyre-query", F_OK) == 0 ? "built" : "source-only");
        bf_jsonw_key(&out, "provides"); bf_jsonw_str(&out, "BonfyreQuery");
        bf_jsonw_obj_end(&out);
        bf_jsonw_key(&out, "route_state"); bf_jsonw_str(&out, "eligible_owner_only");
        bf_jsonw_key(&out, "forbidden_inference"); bf_jsonw_str(&out, "eligible query owner -> proof, authority, or external effect");
        bf_jsonw_obj_end(&out);
        return;
    }
    load_book(state);
    if (!strcmp(op, "select")) {
        const char *prefix = option(argc, args, "--prefix"), *kind = option(argc, args, "--kind");
        const char *preds[256]; int np = options(argc, args, "--where", preds);
        Rec **hits = malloc(sizeof *hits * (size_t)nbook); int n = 0;
        for (int i = 0; i < nbook; i++) {
            Rec *r = &book[i];
            if (prefix && *prefix && strncmp(r->address, prefix, strlen(prefix))) continue;
            if (kind && *kind && strcmp(r->kind, kind)) continue;
            int ok = 1; for (int p = 0; p < np && ok; p++) ok = matches(r, preds[p]);
            if (ok) hits[n++] = r;
        }
        for (int i = 1; i < n; i++)   /* stable insertion sort, as Array.prototype.sort */
            for (int j = i; j > 0 && bf_js_locale_compare(hits[j - 1]->address, hits[j]->address) > 0; j--) {
                Rec *t = hits[j]; hits[j] = hits[j - 1]; hits[j - 1] = t;
            }
        char **reads = malloc(sizeof *reads * (size_t)(n ? n : 1));
        for (int i = 0; i < n; i++) reads[i] = hits[i]->address;
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "SELECT");
        bf_jsonw_key(&out, "program"); program("SELECT", reads, n);
        bf_jsonw_key(&out, "count"); bf_jsonw_num(&out, n);
        bf_jsonw_key(&out, "records"); bf_jsonw_arr(&out);
        for (int i = 0; i < n; i++) summary(hits[i]);
        bf_jsonw_arr_end(&out);
        bf_jsonw_obj_end(&out);
        return;
    }
    if (!strcmp(op, "diff") || !strcmp(op, "compare") || !strcmp(op, "reconcile")) {
        if (argc < 2) die("%s requires two addresses", op);
        Rec *l = require_record(args[0]), *r = require_record(args[1]);
        if (!strcmp(op, "diff")) {
            bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "DIFF");
            bf_jsonw_key(&out, "program"); program2("DIFF", l->address, r->address);
            bf_jsonw_key(&out, "left"); bf_jsonw_str(&out, l->address);
            bf_jsonw_key(&out, "right"); bf_jsonw_str(&out, r->address);
            bf_jsonw_key(&out, "changes"); bf_jsonw_arr(&out); field_diff(l, l->root, r, r->root, "$"); bf_jsonw_arr_end(&out);
            bf_jsonw_obj_end(&out);
            return;
        }
        if (!strcmp(op, "compare")) { compare(l, r); return; }
        const bf_json_node_t *lp = get(l, "property_id"), *rp = get(r, "property_id");
        int shared = truthy(lp) && strict_eq(l, lp, r, rp);
        int distinct = !strict_eq(l, get(l, "unit_identity"), r, get(r, "unit_identity"));
        const char *disposition = !strcmp(l->address, r->address) ? "identical_address"
                                : shared && distinct ? "coexisting_distinct_units" : "no_canonical_join";
        const char *reason = !strcmp(disposition, "identical_address") ? "Both operands resolve to the same address."
            : !strcmp(disposition, "coexisting_distinct_units") ? "Shared property identity with distinct unit identities is not a duplicate or a price conflict."
            : "No supported identity relation permits merging these source-bound records.";
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "RECONCILE");
        bf_jsonw_key(&out, "program"); program2("RECONCILE", l->address, r->address);
        bf_jsonw_key(&out, "left"); bf_jsonw_str(&out, l->address);
        bf_jsonw_key(&out, "right"); bf_jsonw_str(&out, r->address);
        bf_jsonw_key(&out, "disposition"); bf_jsonw_str(&out, disposition);
        bf_jsonw_key(&out, "merge_prohibited"); bf_jsonw_bool(&out, strcmp(disposition, "identical_address") != 0);
        bf_jsonw_key(&out, "reason"); bf_jsonw_str(&out, reason);
        bf_jsonw_key(&out, "comparison"); compare(l, r);
        bf_jsonw_obj_end(&out);
        return;
    }
    if (!strcmp(op, "why") || !strcmp(op, "prove") || !strcmp(op, "project")) {
        if (argc < 1 || !*args[0]) die("%s requires an address", op);
        Rec *r = require_record(args[0]);
        if (!strcmp(op, "why")) {
            const char *field = option(argc, args, "--field");
            if (field && *field && !get(r, field)) die("field is not present on %s: %s", r->address, field);
            bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "WHY");
            bf_jsonw_key(&out, "program"); program1("WHY", r->address);
            bf_jsonw_key(&out, "address"); bf_jsonw_str(&out, r->address);
            bf_jsonw_key(&out, "source_file"); bf_jsonw_str(&out, r->source_file);
            bf_jsonw_key(&out, "source_refs"); source_refs(r);
            bf_jsonw_key(&out, "content_sha256"); digest_field(r);
            bf_jsonw_key(&out, "evidence_state"); bf_jsonw_str(&out, "source_bound_observation");
            const bf_json_node_t *notes = get(r, "notes");
            bf_jsonw_key(&out, "notes");
            if (notes && notes->type != BF_JSON_NULL) bf_jsonw_node(&out, r->doc, notes, 0); else bf_jsonw_str(&out, "");
            if (field && *field) {
                bf_jsonw_key(&out, "field"); bf_jsonw_str(&out, field);
                bf_jsonw_key(&out, "value"); bf_jsonw_node(&out, r->doc, get(r, field), 0);
            }
            bf_jsonw_obj_end(&out);
            return;
        }
        if (!strcmp(op, "prove")) {
            const char *claim = option(argc, args, "--claim"); int c = -1;
            for (int i = 0; claim && i < 4; i++) if (!strcmp(claim, proof_claims[i])) c = i;
            if (c < 0) die("unknown proof claim");
            bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "PROVE");
            bf_jsonw_key(&out, "program"); program1("PROVE", r->address);
            bf_jsonw_key(&out, "address"); bf_jsonw_str(&out, r->address);
            bf_jsonw_key(&out, "claim"); bf_jsonw_str(&out, claim);
            bf_jsonw_key(&out, "proof_state"); bf_jsonw_str(&out, "not_proven");
            bf_jsonw_key(&out, "observed_fields"); bf_jsonw_arr(&out);
            for (int i = 0; i < 5 && proof_fields[c][i]; i++) {
                const bf_json_node_t *v = get(r, proof_fields[c][i]);
                if (v && v->type != BF_JSON_NULL) bf_jsonw_str(&out, proof_fields[c][i]);
            }
            bf_jsonw_arr_end(&out);
            bf_jsonw_key(&out, "source_refs"); source_refs(r);
            bf_jsonw_key(&out, "reason"); bf_jsonw_str(&out, "A source-bound projection can establish record identity and preserve an observation, but it does not independently prove current availability, eligibility, pet acceptance, or cost.");
            bf_jsonw_obj_end(&out);
            return;
        }
        const char *fields[256]; int nf = options(argc, args, "--field", fields);
        if (!nf) die("PROJECT requires at least one --field");
        for (int i = 0; i < nf; i++) if (!get(r, fields[i])) die("fields absent from %s: %s", r->address, fields[i]);
        /* Object.fromEntries: first position wins, array-index keys lead */
        const char *order[256]; int no = 0;
        for (int pass = 0; pass < 2; pass++)
            for (int i = 0; i < nf; i++) {
                const char *f = fields[i]; size_t fl = strlen(f); int idx = fl && fl <= 10 && !(fl > 1 && f[0] == '0');
                for (size_t k = 0; k < fl && idx; k++) idx = f[k] >= '0' && f[k] <= '9';
                if (idx != (pass == 0)) continue;
                int seen = 0; for (int k = 0; k < no; k++) if (!strcmp(order[k], f)) seen = 1;
                if (!seen) order[no++] = f;
            }
        if (no > 1) {   /* index keys ascend numerically */
            for (int i = 1; i < no; i++) {
                const char *f = order[i]; size_t fl = strlen(f); int idx = fl <= 10 && !(fl > 1 && f[0] == '0');
                for (size_t k = 0; k < fl && idx; k++) idx = f[k] >= '0' && f[k] <= '9';
                if (!idx) break;
                for (int j = i; j > 0 && strtoul(order[j - 1], NULL, 10) > strtoul(order[j], NULL, 10); j--) {
                    const char *t = order[j]; order[j] = order[j - 1]; order[j - 1] = t;
                }
            }
        }
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "operator"); bf_jsonw_str(&out, "PROJECT");
        bf_jsonw_key(&out, "program"); program1("PROJECT", r->address);
        bf_jsonw_key(&out, "address"); bf_jsonw_str(&out, r->address);
        bf_jsonw_key(&out, "projection"); bf_jsonw_obj(&out);
        for (int i = 0; i < no; i++) { bf_jsonw_key(&out, order[i]); bf_jsonw_node(&out, r->doc, get(r, order[i]), 0); }
        bf_jsonw_obj_end(&out);
        bf_jsonw_key(&out, "source_refs"); source_refs(r);
        bf_jsonw_obj_end(&out);
        return;
    }
    die("unsupported operator: %s", op);
}

int main(int argc, char **argv) {
    enum { OP_HELP };
    static const BfCommandEntry operator_help_command[] = {
        {"--help", OP_HELP, 0, NULL},
        {"-h", OP_HELP, 0, NULL},
        {"help", OP_HELP, 0, NULL},
    };
    const BfCommandEntry *help_command = argc <= 1 ? NULL : bf_command_lookup(operator_help_command,
        sizeof(operator_help_command) / sizeof(operator_help_command[0]), argv[1]);
    if (argc > 1 && help_command && help_command->id == OP_HELP) {
        fputs(HELP, stdout);
        return 0;
    }
    const char *state = DEFAULT_STATE, *output = NULL;
    char **pos = malloc(sizeof *pos * (size_t)argc); int np = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--state") && i + 1 < argc && *argv[i + 1]) state = argv[++i];
        else if (!strcmp(argv[i], "--output") && i + 1 < argc && *argv[i + 1]) output = argv[++i];
        else pos[np++] = argv[i];
    }
    if (!np) die("missing operator");
    bf_jsonw_init(&out, 2);
    execute(pos[0], np - 1, pos + 1, state);
    if (out.oom) die("out of memory");
    if (output) {
        char *dir = strdup(output), *slash = strrchr(dir, '/');
        if (slash && slash != dir) { *slash = 0; mkdirs(dir); }
        free(dir);
        FILE *f = fopen(output, "w");
        if (!f) die("cannot write %s: %s", output, strerror(errno));
        fwrite(out.buf, 1, out.len, f); fputc('\n', f); fclose(f);
    }
    fwrite(out.buf, 1, out.len, stdout); fputc('\n', stdout);
    return 0;
}
