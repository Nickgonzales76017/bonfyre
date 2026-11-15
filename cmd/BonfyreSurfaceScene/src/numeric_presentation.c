/* Numeric presentation, natively. See numeric_presentation.h. Byte-identical
 * to the browser reader (components/BonfyreServiceSite/authoring/
 * numeric-presentation.mjs) for normalization and addresses: both serialize
 * ["bonfyre.numeric-presentation.v1", style] in the declared order. */
#include "numeric_presentation.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "bonfyre.h"
#include "bf_json.h"
#include "bf_jsonw.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *np_stage = "numeric presentation";
static sqlite3 *np_txn_db;

__attribute__((noreturn)) static void np_fail(const char *message) {
    if (np_txn_db) sqlite3_exec(np_txn_db, "ROLLBACK", 0, 0, 0);
    fprintf(stderr, "%s: %s\n", np_stage, message);
    exit(1);
}

/* ---- JSON helpers -------------------------------------------------------- */

static int np_is_int(const bf_json_node_t *n, long *out) {
    if (!n) return 0;
    if (n->type == BF_JSON_INT) { *out = (long)n->val.i; return 1; }
    if (n->type == BF_JSON_DOUBLE) {
        double d = n->val.d;
        if (d == (double)(long)d && d > -1e15 && d < 1e15) { *out = (long)d; return 1; }
    }
    return 0;
}

static int np_str_eq(const bf_json_node_t *n, const char *s) {
    size_t len = strlen(s);
    return n && n->type == BF_JSON_STRING && (size_t)n->val.str.len == len &&
           !memcmp(n->val.str.ptr, s, len);
}

static char *np_str_dup(const bf_json_node_t *n) {
    if (!n || n->type != BF_JSON_STRING) return NULL;
    size_t len = 0;
    char *s = bf_json_unescape(n->val.str.ptr, n->val.str.len, &len);
    return s;
}

static int np_index(const char *const *names, int count, const char *key, int key_len) {
    for (int i = 0; i < count; i++)
        if ((int)strlen(names[i]) == key_len && !memcmp(names[i], key, (size_t)key_len)) return i;
    return -1;
}

static int np_prop_index(const char *key, int key_len) {
    for (int i = 0; i < BF_NP_PROPS; i++)
        if ((int)strlen(bf_np_props[i].name) == key_len && !memcmp(bf_np_props[i].name, key, (size_t)key_len)) return i;
    return -1;
}

/* Validate one property value; returns its stored form. */
static int np_value(int prop, const bf_json_node_t *v) {
    const BfNpProp *p = &bf_np_props[prop];
    if (p->range) {
        long x;
        if (!np_is_int(v, &x) || x < p->min || x > p->max) np_fail("numeric range");
        return (int)x;
    }
    if (!v || v->type != BF_JSON_STRING) np_fail("unknown enum");
    int i = np_index(p->values, p->nvalues, v->val.str.ptr, v->val.str.len);
    if (i < 0) np_fail("unknown enum");
    return i;
}

static void np_normalize(const bf_json_doc_t *doc, const bf_json_node_t *responsive, BfNpStyle *out) {
    memset(out, 0, sizeof(*out));
    if (!responsive) return;
    if (responsive->type != BF_JSON_OBJECT) np_fail("responsive must be plain");
    for (const bf_json_node_t *c = bf_json_child_first(doc, responsive); c; c = bf_json_child_next(doc, c))
        if (np_index(bf_np_viewport, BF_NP_VIEWPORTS, c->key, c->key_len) < 0) np_fail("unknown viewport");
    for (const bf_json_node_t *c = bf_json_child_first(doc, responsive); c; c = bf_json_child_next(doc, c)) {
        int vp = np_index(bf_np_viewport, BF_NP_VIEWPORTS, c->key, c->key_len);
        if (c->type != BF_JSON_OBJECT) np_fail("invalid viewport");
        BfNpView *view = &out->view[vp];
        memset(view, 0, sizeof(*view)); /* a repeated key keeps the last, as JSON.parse */
        for (const bf_json_node_t *k = bf_json_child_first(doc, c); k; k = bf_json_child_next(doc, k)) {
            int prop = np_prop_index(k->key, k->key_len);
            if (prop < 0) np_fail("unknown property");
            view->value[prop] = np_value(prop, k);
            view->has |= 1u << prop;
        }
    }
}

static void np_write_value(bf_jsonw *w, int prop, int value) {
    if (bf_np_props[prop].range) bf_jsonw_int(w, value);
    else bf_jsonw_str(w, bf_np_props[prop].values[value]);
}

static void np_write_style(bf_jsonw *w, const BfNpStyle *s) {
    bf_jsonw_obj(w);
    for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
        if (!s->view[vp].has) continue;
        bf_jsonw_key(w, bf_np_viewport[vp]);
        bf_jsonw_obj(w);
        for (int p = 0; p < BF_NP_PROPS; p++) {
            if (!(s->view[vp].has & (1u << p))) continue;
            bf_jsonw_key(w, bf_np_props[p].name);
            np_write_value(w, p, s->view[vp].value[p]);
        }
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_obj_end(w);
}

static char *np_style_json(const BfNpStyle *s) {
    bf_jsonw w; bf_jsonw_init(&w, 0);
    np_write_style(&w, s);
    if (w.oom) np_fail("out of memory");
    return w.buf;
}

static void np_address(const BfNpStyle *s, char hex[65]) {
    bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_arr(&w); bf_jsonw_str(&w, BF_NP_SCHEMA); np_write_style(&w, s); bf_jsonw_arr_end(&w);
    if (w.oom) np_fail("out of memory");
    bf_sha256_hex((const uint8_t *)w.buf, w.len, hex);
    bf_jsonw_free(&w);
}

static void np_sha(const char *s, size_t len, char hex[65]) { bf_sha256_hex((const uint8_t *)s, len, hex); }

static int np_hex64(const char *s, size_t len) {
    if (len != 64) return 0;
    for (size_t i = 0; i < 64; i++) if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return 0;
    return 1;
}

static int np_hex64_node(const bf_json_node_t *n) {
    return n && n->type == BF_JSON_STRING && np_hex64(n->val.str.ptr, (size_t)n->val.str.len);
}

/* ---- family: factor / expand -------------------------------------------- */

typedef struct { char *id; BfNpStyle style; char address[65]; } NpEntry;
typedef struct {
    char address[65];
    unsigned set[BF_NP_VIEWPORTS], unset[BF_NP_VIEWPORTS];
    int value[BF_NP_VIEWPORTS][BF_NP_PROPS];
} NpProgram;
typedef struct {
    BfNpStyle basis; char basis_address[65];
    NpProgram *programs; int nprograms;
    NpEntry *bindings; int nbindings;
} NpFamily;

static int np_id_ok(const char *s) {
    size_t n = strlen(s);
    if (n < 1 || n > 118) return 0;
    if (!((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= '0' && s[0] <= '9'))) return 0;
    for (size_t i = 1; i < n; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == ':' || c == '_' || c == '-')) return 0;
    }
    return 1;
}

static int np_text_cmp(int prop, int a, int b) {
    char x[64], y[64];
    if (bf_np_props[prop].range) { snprintf(x, sizeof x, "%d", a); snprintf(y, sizeof y, "%d", b); }
    else { snprintf(x, sizeof x, "\"%s\"", bf_np_props[prop].values[a]); snprintf(y, sizeof y, "\"%s\"", bf_np_props[prop].values[b]); }
    return bf_js_locale_compare(x, y);
}

static int np_program_cmp(const void *a, const void *b) {
    return strcmp(((const NpProgram *)a)->address, ((const NpProgram *)b)->address);
}

static void np_factor(NpEntry *entries, int n, NpFamily *f) {
    memset(f, 0, sizeof(*f));
    f->bindings = entries; f->nbindings = n;
    for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
        for (int p = 0; p < BF_NP_PROPS; p++) {
            int best = -1, best_count = 0;
            for (int i = 0; i < n; i++) {
                if (!(entries[i].style.view[vp].has & (1u << p))) continue;
                int v = entries[i].style.view[vp].value[p], count = 0, first = 1;
                for (int j = 0; j < n; j++) {
                    if (!(entries[j].style.view[vp].has & (1u << p)) || entries[j].style.view[vp].value[p] != v) continue;
                    if (j < i) { first = 0; break; }
                    count++;
                }
                if (!first || count < 2) continue;
                if (best < 0 || count > best_count || (count == best_count && np_text_cmp(p, v, best) < 0)) { best = v; best_count = count; }
            }
            if (best >= 0) { f->basis.view[vp].has |= 1u << p; f->basis.view[vp].value[p] = best; }
        }
    }
    np_address(&f->basis, f->basis_address);
    f->programs = calloc((size_t)n, sizeof(NpProgram));
    if (!f->programs) np_fail("out of memory");
    for (int i = 0; i < n; i++) {
        int seen = 0;
        for (int k = 0; k < f->nprograms; k++) if (!strcmp(f->programs[k].address, entries[i].address)) { seen = 1; break; }
        if (seen) continue;
        NpProgram *g = &f->programs[f->nprograms++];
        memcpy(g->address, entries[i].address, 65);
        for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
            const BfNpView *a = &f->basis.view[vp], *b = &entries[i].style.view[vp];
            for (int p = 0; p < BF_NP_PROPS; p++) {
                unsigned bit = 1u << p; int prev = (a->has & bit) != 0, next = (b->has & bit) != 0;
                if (prev && !next) g->unset[vp] |= bit;
                else if (next && (!prev || a->value[p] != b->value[p])) { g->set[vp] |= bit; g->value[vp][p] = b->value[p]; }
            }
        }
    }
    qsort(f->programs, (size_t)f->nprograms, sizeof(NpProgram), np_program_cmp);
}

static void np_write_family(bf_jsonw *w, const NpFamily *f) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "schema"); bf_jsonw_str(w, BF_NP_SCHEMA);
    bf_jsonw_key(w, "basis"); np_write_style(w, &f->basis);
    bf_jsonw_key(w, "basis_address"); bf_jsonw_str(w, f->basis_address);
    bf_jsonw_key(w, "programs"); bf_jsonw_arr(w);
    for (int k = 0; k < f->nprograms; k++) {
        const NpProgram *g = &f->programs[k];
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "address"); bf_jsonw_str(w, g->address);
        bf_jsonw_key(w, "delta"); bf_jsonw_obj(w);
        for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
            if (!(g->set[vp] | g->unset[vp])) continue;
            bf_jsonw_key(w, bf_np_viewport[vp]); bf_jsonw_obj(w);
            for (int p = 0; p < BF_NP_PROPS; p++) {
                unsigned bit = 1u << p;
                if (g->unset[vp] & bit) { bf_jsonw_key(w, bf_np_props[p].name); bf_jsonw_null(w); }
                else if (g->set[vp] & bit) { bf_jsonw_key(w, bf_np_props[p].name); np_write_value(w, p, g->value[vp][p]); }
            }
            bf_jsonw_obj_end(w);
        }
        bf_jsonw_obj_end(w);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "bindings"); bf_jsonw_arr(w);
    for (int i = 0; i < f->nbindings; i++) {
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "id"); bf_jsonw_str(w, f->bindings[i].id);
        bf_jsonw_key(w, "address"); bf_jsonw_str(w, f->bindings[i].address);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_obj_end(w);
}

/* Candidates [{id, responsive}] -> family. */
static void np_factor_json(const bf_json_doc_t *doc, const bf_json_node_t *arr, NpFamily *f) {
    if (!arr || arr->type != BF_JSON_ARRAY || arr->child_count < 1 || arr->child_count > BF_NP_MAX_FAMILY) np_fail("candidate count");
    int n = arr->child_count, i = 0;
    NpEntry *entries = calloc((size_t)n, sizeof(NpEntry));
    if (!entries) np_fail("out of memory");
    for (const bf_json_node_t *c = bf_json_child_first(doc, arr); c; c = bf_json_child_next(doc, c), i++) {
        const bf_json_node_t *id = c->type == BF_JSON_OBJECT ? bf_json_obj_get(doc, c, "id") : NULL;
        char *s = np_str_dup(id);
        if (!s || !np_id_ok(s)) np_fail("duplicate or invalid id");
        for (int j = 0; j < i; j++) if (!strcmp(entries[j].id, s)) np_fail("duplicate or invalid id");
        entries[i].id = s;
        const bf_json_node_t *r = bf_json_obj_get(doc, c, "responsive");
        np_normalize(doc, r && r->type != BF_JSON_NULL ? r : NULL, &entries[i].style);
        np_address(&entries[i].style, entries[i].address);
    }
    np_factor(entries, n, f);
}

/* Read a family written by np_write_family; verifies its basis address. */
static void np_family_read(const bf_json_doc_t *doc, const bf_json_node_t *node, NpFamily *f) {
    memset(f, 0, sizeof(*f));
    if (!node || node->type != BF_JSON_OBJECT || !np_str_eq(bf_json_obj_get(doc, node, "schema"), BF_NP_SCHEMA)) np_fail("invalid basis");
    np_normalize(doc, bf_json_obj_get(doc, node, "basis"), &f->basis);
    np_address(&f->basis, f->basis_address);
    if (!np_str_eq(bf_json_obj_get(doc, node, "basis_address"), f->basis_address)) np_fail("invalid basis");
    const bf_json_node_t *programs = bf_json_obj_get(doc, node, "programs"), *bindings = bf_json_obj_get(doc, node, "bindings");
    if (programs && programs->type == BF_JSON_ARRAY) {
        f->programs = calloc((size_t)programs->child_count + 1, sizeof(NpProgram));
        if (!f->programs) np_fail("out of memory");
        for (const bf_json_node_t *g = bf_json_child_first(doc, programs); g; g = bf_json_child_next(doc, g)) {
            NpProgram *p = &f->programs[f->nprograms];
            const bf_json_node_t *a = g->type == BF_JSON_OBJECT ? bf_json_obj_get(doc, g, "address") : NULL;
            if (!np_hex64_node(a)) continue;
            memcpy(p->address, a->val.str.ptr, 64); p->address[64] = 0;
            const bf_json_node_t *delta = bf_json_obj_get(doc, g, "delta");
            if (delta && delta->type == BF_JSON_OBJECT) {
                for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
                    const bf_json_node_t *d = bf_json_obj_get(doc, delta, bf_np_viewport[vp]);
                    if (!d || d->type != BF_JSON_OBJECT) continue;
                    for (const bf_json_node_t *k = bf_json_child_first(doc, d); k; k = bf_json_child_next(doc, k)) {
                        int prop = np_prop_index(k->key, k->key_len);
                        if (prop < 0) np_fail("invalid residual");
                        if (k->type == BF_JSON_NULL) { p->unset[vp] |= 1u << prop; p->set[vp] &= ~(1u << prop); }
                        else { p->set[vp] |= 1u << prop; p->unset[vp] &= ~(1u << prop); p->value[vp][prop] = np_value(prop, k); }
                    }
                }
            }
            f->nprograms++;
        }
    }
    if (bindings && bindings->type == BF_JSON_ARRAY) {
        f->bindings = calloc((size_t)bindings->child_count + 1, sizeof(NpEntry));
        if (!f->bindings) np_fail("out of memory");
        for (const bf_json_node_t *b = bf_json_child_first(doc, bindings); b; b = bf_json_child_next(doc, b)) {
            NpEntry *e = &f->bindings[f->nbindings++];
            const bf_json_node_t *a = b->type == BF_JSON_OBJECT ? bf_json_obj_get(doc, b, "address") : NULL;
            e->id = np_str_dup(b->type == BF_JSON_OBJECT ? bf_json_obj_get(doc, b, "id") : NULL);
            if (!e->id) e->id = strdup("");
            if (np_hex64_node(a)) { memcpy(e->address, a->val.str.ptr, 64); e->address[64] = 0; }
        }
    }
}

static void np_expand(const NpFamily *f, const char *address, BfNpStyle *out) {
    const NpProgram *g = NULL;
    for (int k = 0; k < f->nprograms; k++) if (!strcmp(f->programs[k].address, address)) { g = &f->programs[k]; break; }
    if (!g) np_fail("unknown variant");
    *out = f->basis;
    for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
        out->view[vp].has &= ~g->unset[vp];
        for (int p = 0; p < BF_NP_PROPS; p++)
            if (g->set[vp] & (1u << p)) { out->view[vp].has |= 1u << p; out->view[vp].value[p] = g->value[vp][p]; }
    }
    char hex[65]; np_address(out, hex);
    if (strcmp(hex, address)) np_fail("variant digest mismatch");
}

/* ---- HVM4 selection, egglog witness, CSS lowering ------------------------ */

typedef struct { int viewport, field, op, max; } NpPredicate;
typedef struct { char *program; int expected[BF_NP_SELECT_MAX]; int n; } NpLowered;

static void np_predicate(const bf_json_doc_t *doc, const bf_json_node_t *node, NpPredicate *p) {
    const char *saved = np_stage; np_stage = "numeric HVM";
    if (!node || node->type != BF_JSON_OBJECT) np_fail("invalid numeric predicate");
    const bf_json_node_t *vp = bf_json_obj_get(doc, node, "viewport"), *fd = bf_json_obj_get(doc, node, "field"),
                         *op = bf_json_obj_get(doc, node, "op"), *mx = bf_json_obj_get(doc, node, "max");
    p->viewport = vp && vp->type == BF_JSON_STRING ? np_index(bf_np_viewport, BF_NP_VIEWPORTS, vp->val.str.ptr, vp->val.str.len) : -1;
    p->field = -1;
    if (fd && fd->type == BF_JSON_STRING && np_index(bf_np_select_fields, BF_NP_SELECT_FIELDS, fd->val.str.ptr, fd->val.str.len) >= 0)
        p->field = np_prop_index(fd->val.str.ptr, fd->val.str.len);
    p->op = op ? (op->type == BF_JSON_STRING ? np_index(bf_np_select_ops, BF_NP_SELECT_OPS, op->val.str.ptr, op->val.str.len) : -1) : 1;
    long m;
    if (p->viewport < 0 || p->field < 0 || p->op < 0 || !np_is_int(mx, &m) || m < 0 || m > 64) np_fail("invalid numeric predicate");
    p->max = (int)m;
    np_stage = saved;
}

static int np_eval(int op, int v, int max) {
    switch (op) { case 0: return v < max; case 1: return v <= max; case 2: return v > max; case 3: return v >= max; default: return v == max; }
}

static void np_nested(char *out, size_t cap, const int *a, int n) {
    static const char labels[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    char term[2048], next[2048];
    snprintf(term, sizeof term, "%d", a[n - 1]);
    for (int i = n - 2; i >= 0; i--) { snprintf(next, sizeof next, "&%c{%d,%s}", labels[i], a[i], term); memcpy(term, next, sizeof term); }
    snprintf(out, cap, "%s", term);
}

static void np_lower_selection(const NpFamily *f, const NpPredicate *p, NpLowered *out) {
    const char *saved = np_stage; np_stage = "numeric HVM";
    if (f->nbindings < 1 || f->nbindings > BF_NP_SELECT_MAX) np_fail("candidate count exceeds bound");
    int values[BF_NP_SELECT_MAX], ids[BF_NP_SELECT_MAX];
    for (int i = 0; i < f->nbindings; i++) {
        BfNpStyle s; np_stage = saved; np_expand(f, f->bindings[i].address, &s); np_stage = "numeric HVM";
        const BfNpView *v = &s.view[p->viewport];
        if (!(v->has & (1u << p->field)) || v->value[p->field] < 0 || v->value[p->field] > 64) np_fail("candidate lacks an exact numeric value");
        values[i] = v->value[p->field]; ids[i] = i + 1;
        out->expected[i] = np_eval(p->op, values[i], p->max) ? i + 1 : 0;
    }
    out->n = f->nbindings;
    char a[2048], b[2048];
    np_nested(a, sizeof a, ids, out->n); np_nested(b, sizeof b, values, out->n);
    size_t cap = strlen(a) + strlen(b) + 128;
    out->program = malloc(cap);
    if (!out->program) np_fail("out of memory");
    snprintf(out->program, cap, "@keep = \xce\xbbid. \xce\xbbp. \xce\xbb{0: 0; \xce\xbbk. id}(p)\n@main = @keep(%s, (%s %s %d))\n", a, b, bf_np_select_ops[p->op], p->max);
    np_stage = saved;
}

/* Run argv with optional stdin bytes; capture up to cap bytes of stdout. */
static int np_run(char *const argv[], const char *input, size_t input_len, char **out, size_t cap, int timeout_s, char *err, size_t errcap) {
    int in[2] = {-1, -1}, op[2], ep[2];
    if (pipe(op) || pipe(ep) || (input && pipe(in))) return -1;
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        if (input) dup2(in[0], 0); else { int z = open("/dev/null", O_RDONLY); if (z >= 0) dup2(z, 0); }
        dup2(op[1], 1); dup2(ep[1], 2);
        for (int fd = 3; fd < 64; fd++) close(fd);
        execv(argv[0], argv);
        _exit(127);
    }
    close(op[1]); close(ep[1]);
    if (input) {
        close(in[0]);
        signal(SIGPIPE, SIG_IGN);
        size_t off = 0;
        while (off < input_len) { ssize_t w = write(in[1], input + off, input_len - off); if (w <= 0) break; off += (size_t)w; }
        close(in[1]);
    }
    size_t len = 0, elen = 0; char *buf = malloc(cap + 1); int fl;
    if (!buf) return -1;
    fl = fcntl(op[0], F_GETFL); fcntl(op[0], F_SETFL, fl | O_NONBLOCK);
    fl = fcntl(ep[0], F_GETFL); fcntl(ep[0], F_SETFL, fl | O_NONBLOCK);
    time_t start = time(NULL); int status = 0, done = 0;
    for (;;) {
        char tmp[4096]; ssize_t r;
        while ((r = read(op[0], tmp, sizeof tmp)) > 0) { size_t take = (size_t)r; if (len + take > cap) take = cap - len; memcpy(buf + len, tmp, take); len += take; }
        while ((r = read(ep[0], tmp, sizeof tmp)) > 0) { if (err && elen + 1 < errcap) { size_t take = (size_t)r; if (elen + take + 1 > errcap) take = errcap - elen - 1; memcpy(err + elen, tmp, take); elen += take; err[elen] = 0; } }
        if (done) break;
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid) { done = 1; continue; }
        if (time(NULL) - start > timeout_s) { kill(pid, SIGKILL); waitpid(pid, &status, 0); free(buf); close(op[0]); close(ep[0]); return -2; }
        usleep(2000);
    }
    close(op[0]); close(ep[0]);
    buf[len] = 0; *out = buf;
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128;
}

static char *np_run_program(const char *binary, const char *source, const char *arg, const char *extension) {
    struct stat st;
    char msg[256];
    if (!binary || binary[0] != '/' || stat(binary, &st)) { snprintf(msg, sizeof msg, "required native backend unavailable: %s", extension); np_fail(msg); }
    char folder[] = "/tmp/bonfyre-numeric-XXXXXX";
    if (!mkdtemp(folder)) np_fail("temporary folder");
    char path[256]; snprintf(path, sizeof path, "%s/candidate.%s", folder, extension);
    FILE *fp = fopen(path, "wb");
    if (!fp) np_fail("temporary file");
    fputs(source, fp); fclose(fp);
    char *argv[4] = {(char *)binary, path, (char *)arg, NULL};
    char *out = NULL, err[256] = {0};
    int rc = np_run(argv, NULL, 0, &out, 1 << 18, 12, err, sizeof err);
    unlink(path); rmdir(folder);
    if (rc != 0) { snprintf(msg, sizeof msg, "%s reduction failed: %.200s", extension, err); np_fail(msg); }
    return out;
}

typedef struct { char sha[65]; int observed[BF_NP_SELECT_MAX]; int n; } NpHvm;
typedef struct { char sha[65]; int values[BF_NP_SELECT_MAX * BF_NP_VIEWPORTS * BF_NP_PROPS]; int n; } NpEgg;

static void np_hvm(const NpLowered *lowered, const char *binary, NpHvm *h) {
    char arg[16]; snprintf(arg, sizeof arg, "-C%d", lowered->n + 4);
    char *out = np_run_program(binary, lowered->program, arg, "hvm");
    h->n = 0;
    int mismatch = 0;
    for (char *tok = strtok(out, " \t\r\n"); tok; tok = strtok(NULL, " \t\r\n")) {
        size_t k = 0; while (tok[k] >= '0' && tok[k] <= '9') k++;
        if (!k || tok[k]) continue;
        if (h->n >= BF_NP_SELECT_MAX) { mismatch = 1; break; }
        h->observed[h->n++] = atoi(tok);
    }
    if (mismatch || h->n != lowered->n) np_fail("HVM4 disagrees with exact numerical reference");
    for (int i = 0; i < h->n; i++) if (h->observed[i] != lowered->expected[i]) np_fail("HVM4 disagrees with exact numerical reference");
    np_sha(lowered->program, strlen(lowered->program), h->sha);
}

static int np_int_cmp(const void *a, const void *b) { return *(const int *)a - *(const int *)b; }

static void np_egglog(int *values, int n, const char *binary, NpEgg *e) {
    qsort(values, (size_t)n, sizeof(int), np_int_cmp);
    e->n = 0;
    for (int i = 0; i < n; i++) if (!e->n || e->values[e->n - 1] != values[i]) e->values[e->n++] = values[i];
    size_t cap = 512 + (size_t)e->n * 160; char *p = malloc(cap); size_t len = 0;
    if (!p) np_fail("out of memory");
    len += (size_t)snprintf(p + len, cap - len, "(datatype NumExpr (Num i64) (Add NumExpr NumExpr) (Mul NumExpr NumExpr))\n(rewrite (Add (Num 0) a) a)\n(rewrite (Add a (Num 0)) a)\n(rewrite (Mul (Num 1) a) a)");
    for (int i = 0; i < e->n; i++) len += (size_t)snprintf(p + len, cap - len, "\n(let v%d (Add (Num 0) (Num %d)))\n(let m%d (Mul (Num 1) (Num %d)))", i, e->values[i], i, e->values[i]);
    len += (size_t)snprintf(p + len, cap - len, "\n(run 3)");
    for (int i = 0; i < e->n; i++) len += (size_t)snprintf(p + len, cap - len, "\n(check (= v%d (Num %d)))\n(check (= m%d (Num %d)))", i, e->values[i], i, e->values[i]);
    len += (size_t)snprintf(p + len, cap - len, "\n");
    free(np_run_program(binary, p, NULL, "egg"));
    np_sha(p, len, e->sha);
    free(p);
}

static char *np_lower_css(const NpFamily *f, char (*addresses)[65], int n) {
    const char *saved = np_stage; np_stage = "numeric presentation compiler";
    if (n < 1 || n > BF_NP_SELECT_MAX) np_fail("CSS selection count");
    unsigned long long groups[BF_NP_VIEWPORTS] = {0}; /* gap is 0..64: a 65-bit set; 64 kept apart */
    int has64[BF_NP_VIEWPORTS] = {0};
    for (int i = 0; i < n; i++) {
        int dup = 0; for (int j = 0; j < i; j++) if (!strcmp(addresses[i], addresses[j])) dup = 1;
        if (dup) continue;
        BfNpStyle s; np_stage = saved; np_expand(f, addresses[i], &s); np_stage = "numeric presentation compiler";
        for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
            if (!(s.view[vp].has & (1u << BF_NP_GAP))) continue;
            int g = s.view[vp].value[BF_NP_GAP];
            if (g == 64) has64[vp] = 1; else groups[vp] |= 1ull << g;
        }
    }
    size_t cap = 256, len = 0; char *css = malloc(cap);
    if (!css) np_fail("out of memory");
    for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++) {
        if (!groups[vp] && !has64[vp]) continue;
        char line[256];
        for (int g = -1; g <= 65; g++) {
            int written;
            if (g == -1) written = snprintf(line, sizeof line, "@media %s {\n", bf_np_media[vp]);
            else if (g == 65) written = snprintf(line, sizeof line, "}\n");
            else if (g < 64 ? !(groups[vp] & (1ull << g)) : !has64[vp]) continue;
            else written = snprintf(line, sizeof line, " .authored-site [data-%s-gap=\"%d\"] { --ak-gap-%s: %dpx; }\n", bf_np_viewport[vp], g, bf_np_viewport[vp], g);
            if (len + (size_t)written + 2 > cap) { cap = (len + (size_t)written + 2) * 2; css = realloc(css, cap); if (!css) np_fail("out of memory"); }
            memcpy(css + len, line, (size_t)written); len += (size_t)written;
        }
    }
    /* JS joins lines with "\n" and appends one: an empty selection is "\n". */
    if (!len) css[len++] = '\n';
    css[len] = 0;
    np_stage = saved;
    return css;
}
typedef struct { const char *hvm, *egglog, *css, *fixture, *label, *lightning; } NpEngines;

/* The CSS candidate engine. A fixture is test-only and must say so; the
 * production engine is the pinned Lightning CSS command. */
static char *np_lightning(const char *css, const NpEngines *eng, const char **label_out) {
    const char *saved = np_stage; np_stage = "numeric presentation compiler";
    if (strlen(css) > 100000) np_fail("invalid CSS source");
    char *out = NULL, err[256] = {0}; int rc;
    if (eng->fixture) {
        if (!eng->label || strncmp(eng->label, "fixture:", 8)) np_fail("test transformer must be explicitly marked fixture");
        char *argv[2] = {(char *)eng->fixture, NULL};
        rc = np_run(argv, css, strlen(css), &out, 100001, 12, err, sizeof err);
        *label_out = eng->label;
    } else {
        const char *bin = eng->lightning;
        struct stat st;
        if (!bin || bin[0] != '/' || stat(bin, &st)) np_fail("Lightning CSS not installed: install a pinned lightningcss dependency in deploy/tailwind");
        char *vargv[3] = {(char *)bin, "--version", NULL}; char *version = NULL;
        if (np_run(vargv, NULL, 0, &version, 256, 12, NULL, 0) != 0 || !strstr(version, "1.33.0")) np_fail("Lightning CSS version is not pinned to 1.33.0");
        free(version);
        char *argv[6] = {(char *)bin, "--minify", "--targets", "chrome 90, firefox 90, safari 15", "/dev/stdin", NULL};
        rc = np_run(argv, css, strlen(css), &out, 100001, 12, err, sizeof err);
        *label_out = "lightningcss:1.33.0";
    }
    if (rc != 0 || !out || !*out) np_fail("CSS transformer returned no bytes");
    if (strlen(out) > 100000) np_fail("invalid optimized CSS");
    np_stage = saved;
    return out;
}

typedef struct {
    char (*selected)[65]; int nselected;
    char raw_sha[65], out_sha[65];
    char *css; const char *engine;
    NpHvm hvm; NpEgg egg;
} NpCompiled;

static int np_addr_cmp(const void *a, const void *b) { return strcmp((const char *)a, (const char *)b); }

static void np_compile(const NpFamily *f, const NpPredicate *p, const NpEngines *eng, NpCompiled *c) {
    memset(c, 0, sizeof(*c));
    NpLowered lowered; np_lower_selection(f, p, &lowered);
    np_stage = "numeric presentation compiler";
    np_hvm(&lowered, eng->hvm, &c->hvm);
    c->selected = calloc(BF_NP_SELECT_MAX, 65);
    if (!c->selected) np_fail("out of memory");
    int values[BF_NP_SELECT_MAX * BF_NP_VIEWPORTS * BF_NP_PROPS], nv = 0;
    for (int i = 0; i < lowered.n; i++) {
        if (!lowered.expected[i]) continue;
        memcpy(c->selected[c->nselected++], f->bindings[i].address, 65);
    }
    if (!c->nselected) np_fail("no matching presentation");
    for (int i = 0; i < c->nselected; i++) {
        BfNpStyle s; np_expand(f, c->selected[i], &s);
        for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++)
            for (int k = 0; k < BF_NP_PROPS; k++)
                if ((s.view[vp].has & (1u << k)) && bf_np_props[k].range) values[nv++] = s.view[vp].value[k];
    }
    np_egglog(values, nv, eng->egglog, &c->egg);
    char *raw = np_lower_css(f, c->selected, c->nselected);
    int blank = 1; for (const char *q = raw; *q; q++) if (*q != ' ' && *q != '\n' && *q != '\t' && *q != '\r') blank = 0;
    if (blank) np_fail("selection has no supported CSS numeric rules");
    np_sha(raw, strlen(raw), c->raw_sha);
    if (!eng->css || !strcmp(eng->css, "raw")) { c->css = raw; c->engine = "unoptimized-reference"; }
    else if (!strcmp(eng->css, "lightning")) { c->css = np_lightning(raw, eng, &c->engine); }
    else np_fail("unknown CSS engine");
    np_sha(c->css, strlen(c->css), c->out_sha);
    /* source_addresses: unique, sorted */
    qsort(c->selected, (size_t)c->nselected, 65, np_addr_cmp);
    int u = 0; for (int i = 0; i < c->nselected; i++) if (!u || strcmp(c->selected[u - 1], c->selected[i])) memmove(c->selected[u++], c->selected[i], 65);
    c->nselected = u;
}

static void np_write_compiled(bf_jsonw *w, const bf_json_doc_t *doc, const bf_json_node_t *predicate, const NpCompiled *c) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "schema"); bf_jsonw_str(w, "bonfyre.numeric-css-derivation.v1");
    bf_jsonw_key(w, "source_addresses"); bf_jsonw_arr(w); for (int i = 0; i < c->nselected; i++) bf_jsonw_str(w, c->selected[i]); bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "predicate"); bf_jsonw_node(w, doc, predicate, 0);
    bf_jsonw_key(w, "raw_sha256"); bf_jsonw_str(w, c->raw_sha);
    bf_jsonw_key(w, "output_sha256"); bf_jsonw_str(w, c->out_sha);
    bf_jsonw_key(w, "css"); bf_jsonw_str(w, c->css);
    bf_jsonw_key(w, "engine"); bf_jsonw_str(w, c->engine);
    bf_jsonw_key(w, "hvm"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "engine"); bf_jsonw_str(w, "hvm4");
    bf_jsonw_key(w, "program_sha256"); bf_jsonw_str(w, c->hvm.sha);
    bf_jsonw_key(w, "observed"); bf_jsonw_arr(w); for (int i = 0; i < c->hvm.n; i++) bf_jsonw_int(w, c->hvm.observed[i]); bf_jsonw_arr_end(w);
    bf_jsonw_obj_end(w);
    bf_jsonw_key(w, "equivalence"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "engine"); bf_jsonw_str(w, "egglog");
    bf_jsonw_key(w, "values"); bf_jsonw_arr(w); for (int i = 0; i < c->egg.n; i++) bf_jsonw_int(w, c->egg.values[i]); bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "proof_sha256"); bf_jsonw_str(w, c->egg.sha);
    bf_jsonw_obj_end(w);
    bf_jsonw_obj_end(w);
}

/* ---- derived content store ---------------------------------------------- */

static int np_safe(const char *s) {
    size_t n = s ? strlen(s) : 0;
    if (n < 1 || n >= 255) return 0;
    for (size_t i = 0; i < n; i++) if ((unsigned char)s[i] < 0x20) return 0;
    return 1;
}

static sqlite3 *np_store_open(const char *path) {
    np_stage = "numerical content store";
    if (!np_safe(path) || path[0] != '/') np_fail("absolute explicit store path required");
    sqlite3 *db = NULL;
    if (sqlite3_open(path, &db) != SQLITE_OK) np_fail("store open failed");
    sqlite3_busy_timeout(db, 5000);
    if (sqlite3_exec(db, "PRAGMA foreign_keys=ON;" BF_NP_STORE_DDL, 0, 0, 0) != SQLITE_OK) np_fail("store schema failed");
    return db;
}

static void np_store_verify(const char *digest, const char *body, BfNpStyle *out) {
    if (!np_hex64(digest, strlen(digest)) || strlen(body) > 8192) np_fail("invalid body envelope");
    char err[128];
    bf_json_doc_t *doc = bf_json_parse(body, strlen(body), err, sizeof err);
    if (!doc) np_fail("invalid stored body");
    np_normalize(doc, bf_json_root(doc), out);
    char *again = np_style_json(out), hex[65];
    np_address(out, hex);
    if (strcmp(again, body) || strcmp(hex, digest)) np_fail("content address does not match stored numerical values");
    free(again); bf_json_free(doc);
}

static int np_authoring_kind(const bf_json_node_t *k) {
    if (!k || k->type != BF_JSON_STRING) return 0;
    char s[128]; int n = k->val.str.len;
    if (n <= 0 || n >= (int)sizeof s) return 0;
    memcpy(s, k->val.str.ptr, (size_t)n); s[n] = 0;
    static const char *const profile[] = {"hero", "section", "card", "story", "quote", "metric", "action"};
    if (!strncmp(s, "profile-", 8)) { for (size_t i = 0; i < 7; i++) if (!strcmp(s + 8, profile[i])) return 1; return 0; }
    const char *rest = !strncmp(s, "service:", 8) || !strncmp(s, "ui:", 3) ? strchr(s, ':') + 1 : NULL;
    if (!rest || !(*rest >= 'a' && *rest <= 'z')) return 0;
    for (const char *q = rest; *q; q++) if (!((*q >= 'a' && *q <= 'z') || *q == '-')) return 0;
    return 1;
}

typedef struct { char *coordinate; BfNpStyle style; char digest[65]; } NpRef;
typedef struct { char *namespace_, *route, *revision; int references, inserted, reused, retired; } NpReceipt;

/* Parse semantic tags (object or JSON string); returns node in *doc_out. */
static const bf_json_node_t *np_tags(const bf_json_doc_t *doc, const bf_json_node_t *row, bf_json_doc_t **owned, char **owned_buf) {
    const bf_json_node_t *t = bf_json_obj_get(doc, row, "semantic_tags");
    *owned = NULL; *owned_buf = NULL;
    if (!t || t->type == BF_JSON_NULL) return NULL;
    if (t->type == BF_JSON_STRING) {
        if (!t->val.str.len) return NULL;
        size_t len; char err[64];
        *owned_buf = bf_json_unescape(t->val.str.ptr, t->val.str.len, &len);
        *owned = *owned_buf ? bf_json_parse(*owned_buf, len, err, sizeof err) : NULL;
        if (!*owned) np_fail("invalid CMS tags");
        return bf_json_root(*owned);
    }
    return t;
}

static void np_store_intern(sqlite3 *db, const bf_json_doc_t *doc, const bf_json_node_t *snapshot, NpReceipt *rc, NpRef **refs_out, int *nrefs_out) {
    np_stage = "native authoring";
    const bf_json_node_t *snap = snapshot && snapshot->type == BF_JSON_OBJECT ? bf_json_obj_get(doc, snapshot, "snapshot") : NULL;
    const bf_json_node_t *ns = snap ? bf_json_obj_get(doc, snap, "namespace") : NULL, *route = snap ? bf_json_obj_get(doc, snap, "route") : NULL,
                         *frags = snap ? bf_json_obj_get(doc, snap, "fragments") : NULL;
    if (!np_str_eq(bf_json_obj_get(doc, snapshot, "schema"), "bonfyre.page-snapshot.v1") || !np_hex64_node(bf_json_obj_get(doc, snapshot, "digest")) ||
        !ns || ns->type != BF_JSON_STRING || !ns->val.str.len || !route || route->type != BF_JSON_STRING || !frags || frags->type != BF_JSON_ARRAY)
        np_fail("complete native snapshot is required");
    rc->namespace_ = np_str_dup(ns); rc->route = np_str_dup(route);
    rc->revision = np_str_dup(bf_json_obj_get(doc, snapshot, "digest"));
    np_stage = "numerical content store";
    NpRef *refs = calloc((size_t)frags->child_count + 1, sizeof(NpRef)); int n = 0;
    if (!refs) np_fail("out of memory");
    for (const bf_json_node_t *f = bf_json_child_first(doc, frags); f; f = bf_json_child_next(doc, f)) {
        if (f->type != BF_JSON_OBJECT) continue;
        const bf_json_node_t *page = bf_json_obj_get(doc, f, "page");
        char *page_s = np_str_dup(page);
        int on_page = page_s && !strcmp(page_s, rc->route) && np_str_eq(bf_json_obj_get(doc, f, "slot"), "showcase");
        free(page_s);
        if (!on_page) continue;
        bf_json_doc_t *owned; char *owned_buf;
        const bf_json_node_t *tags = np_tags(doc, f, &owned, &owned_buf);
        const bf_json_doc_t *tdoc = owned ? owned : doc;
        const bf_json_node_t *v1 = tags && tags->type == BF_JSON_OBJECT ? bf_json_obj_get(tdoc, tags, "authoring_v1") : NULL;
        if (v1 && v1->type != BF_JSON_OBJECT) v1 = NULL;
        const bf_json_node_t *declared = v1 ? bf_json_obj_get(tdoc, v1, "numeric_presentation_digest") : NULL;
        if (declared && !np_hex64_node(declared)) np_fail("invalid CMS numerical digest");
        if (np_authoring_kind(bf_json_obj_get(doc, f, "kind"))) {
            NpRef *r = &refs[n];
            r->coordinate = np_str_dup(bf_json_obj_get(doc, f, "coordinate"));
            if (!r->coordinate) { np_stage = "native authoring"; np_fail("invalid retained block coordinate"); }
            for (int j = 0; j < n; j++) if (!strcmp(refs[j].coordinate, r->coordinate)) { np_stage = "native authoring"; np_fail("duplicate retained block coordinate"); }
            const bf_json_node_t *resp = v1 ? bf_json_obj_get(tdoc, v1, "responsive") : NULL;
            np_stage = "numeric presentation";
            np_normalize(tdoc, resp && resp->type != BF_JSON_NULL ? resp : NULL, &r->style);
            np_stage = "numerical content store";
            np_address(&r->style, r->digest);
            if (declared && (declared->val.str.len != 64 || memcmp(declared->val.str.ptr, r->digest, 64)))
                np_fail("CMS numerical digest does not match authored values");
            n++;
        }
        if (owned) bf_json_free(owned);
        free(owned_buf);
    }
    sqlite3_exec(db, "BEGIN IMMEDIATE", 0, 0, 0); np_txn_db = db;
    sqlite3_stmt *get, *put, *ref, *all, *del;
    sqlite3_prepare_v2(db, "SELECT body FROM _scene_numeric_body WHERE digest=?1", -1, &get, 0);
    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO _scene_numeric_body(digest,body) VALUES(?1,?2)", -1, &put, 0);
    sqlite3_prepare_v2(db, "INSERT INTO _scene_numeric_ref(namespace,route,coordinate,source_revision,digest) VALUES(?1,?2,?3,?4,?5) "
                           "ON CONFLICT(namespace,route,coordinate) DO UPDATE SET source_revision=excluded.source_revision,digest=excluded.digest", -1, &ref, 0);
    sqlite3_prepare_v2(db, "SELECT coordinate FROM _scene_numeric_ref WHERE namespace=?1 AND route=?2", -1, &all, 0);
    sqlite3_prepare_v2(db, "DELETE FROM _scene_numeric_ref WHERE namespace=?1 AND route=?2 AND coordinate=?3", -1, &del, 0);
    for (int i = 0; i < n; i++) {
        char *body = np_style_json(&refs[i].style);
        sqlite3_bind_text(get, 1, refs[i].digest, -1, SQLITE_STATIC);
        if (sqlite3_step(get) == SQLITE_ROW) {
            char *stored = strdup((const char *)sqlite3_column_text(get, 0));
            sqlite3_reset(get);
            BfNpStyle check; np_store_verify(refs[i].digest, stored, &check); free(stored);
            rc->reused++;
        } else {
            sqlite3_reset(get);
            sqlite3_bind_text(put, 1, refs[i].digest, -1, SQLITE_STATIC);
            sqlite3_bind_text(put, 2, body, -1, SQLITE_STATIC);
            if (sqlite3_step(put) != SQLITE_DONE) np_fail("body write failed");
            sqlite3_reset(put);
            rc->inserted++;
        }
        sqlite3_bind_text(ref, 1, rc->namespace_, -1, SQLITE_STATIC);
        sqlite3_bind_text(ref, 2, rc->route, -1, SQLITE_STATIC);
        sqlite3_bind_text(ref, 3, refs[i].coordinate, -1, SQLITE_STATIC);
        sqlite3_bind_text(ref, 4, rc->revision, -1, SQLITE_STATIC);
        sqlite3_bind_text(ref, 5, refs[i].digest, -1, SQLITE_STATIC);
        if (sqlite3_step(ref) != SQLITE_DONE) np_fail("reference write failed");
        sqlite3_reset(ref);
        free(body);
    }
    sqlite3_bind_text(all, 1, rc->namespace_, -1, SQLITE_STATIC);
    sqlite3_bind_text(all, 2, rc->route, -1, SQLITE_STATIC);
    char **stale = NULL; int nstale = 0;
    while (sqlite3_step(all) == SQLITE_ROW) {
        const char *coord = (const char *)sqlite3_column_text(all, 0); int keep = 0;
        for (int i = 0; i < n; i++) if (!strcmp(refs[i].coordinate, coord)) { keep = 1; break; }
        if (keep) continue;
        stale = realloc(stale, sizeof(char *) * (size_t)(nstale + 1));
        if (!stale) np_fail("out of memory");
        stale[nstale++] = strdup(coord);
    }
    for (int i = 0; i < nstale; i++) {
        sqlite3_bind_text(del, 1, rc->namespace_, -1, SQLITE_STATIC);
        sqlite3_bind_text(del, 2, rc->route, -1, SQLITE_STATIC);
        sqlite3_bind_text(del, 3, stale[i], -1, SQLITE_STATIC);
        sqlite3_step(del); sqlite3_reset(del); rc->retired++; free(stale[i]);
    }
    free(stale);
    sqlite3_finalize(get); sqlite3_finalize(put); sqlite3_finalize(ref); sqlite3_finalize(all); sqlite3_finalize(del);
    if (sqlite3_exec(db, "COMMIT", 0, 0, 0) != SQLITE_OK) np_fail("commit failed");
    np_txn_db = NULL;
    rc->references = n;
    if (refs_out) { *refs_out = refs; *nrefs_out = n; }
}

/* 1 with digest/revision/style, 0 when absent. */
static int np_store_resolve(sqlite3 *db, const char *ns, const char *route, const char *coord, char digest[65], char **revision, BfNpStyle *style) {
    np_stage = "numerical content store";
    if (!np_safe(ns) || !np_safe(route) || !np_safe(coord)) np_fail("invalid scoped lookup");
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db, "SELECT r.digest,b.body,r.source_revision FROM _scene_numeric_ref r JOIN _scene_numeric_body b ON b.digest=r.digest "
                           "WHERE r.namespace=?1 AND r.route=?2 AND r.coordinate=?3", -1, &st, 0);
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC); sqlite3_bind_text(st, 3, coord, -1, SQLITE_STATIC);
    int found = sqlite3_step(st) == SQLITE_ROW;
    if (found) {
        snprintf(digest, 65, "%s", (const char *)sqlite3_column_text(st, 0));
        char *body = strdup((const char *)sqlite3_column_text(st, 1));
        *revision = strdup((const char *)sqlite3_column_text(st, 2));
        sqlite3_finalize(st);
        np_store_verify(digest, body, style); free(body);
        return 1;
    }
    sqlite3_finalize(st);
    return 0;
}

static int np_count(sqlite3 *db, const char *sql) {
    sqlite3_stmt *st; int n = 0;
    sqlite3_prepare_v2(db, sql, -1, &st, 0);
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return n;
}

static void np_write_receipt(bf_jsonw *w, const NpReceipt *r) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "namespace"); bf_jsonw_str(w, r->namespace_);
    bf_jsonw_key(w, "route"); bf_jsonw_str(w, r->route);
    bf_jsonw_key(w, "source_revision"); bf_jsonw_str(w, r->revision);
    bf_jsonw_key(w, "references"); bf_jsonw_int(w, r->references);
    bf_jsonw_key(w, "inserted"); bf_jsonw_int(w, r->inserted);
    bf_jsonw_key(w, "reused"); bf_jsonw_int(w, r->reused);
    bf_jsonw_key(w, "retired"); bf_jsonw_int(w, r->retired);
    bf_jsonw_obj_end(w);
}

/* ---- the CMS cut: snapshots -> shared bodies -> selection -> CSS -------- */

typedef struct { char *ns, *route, *coordinate, *revision; BfNpStyle style; } NpCandidate;

static int np_cut(const char *db_path, const char *artifact_dir, const bf_json_doc_t *doc, const bf_json_node_t *input, NpEngines *eng, bf_jsonw *w) {
    np_stage = "numerical cut";
    const bf_json_node_t *snapshots = bf_json_obj_get(doc, input, "snapshots"), *predicate = bf_json_obj_get(doc, input, "predicate");
    if (!snapshots || snapshots->type != BF_JSON_ARRAY || snapshots->child_count < 1 || snapshots->child_count > BF_NP_SELECT_MAX)
        np_fail("CMS snapshot cohort must contain 1..12 revisions");
    if (!db_path || db_path[0] != '/') np_fail("scoped store path required");
    const char *css_mode = eng->css ? eng->css : "raw";
    if (artifact_dir && (artifact_dir[0] != '/' || strcmp(css_mode, "lightning"))) np_fail("only Lightning CSS candidates can be retained as files");
    sqlite3 *db = np_store_open(db_path);
    NpReceipt receipts[BF_NP_SELECT_MAX]; int nr = 0;
    NpCandidate cands[BF_NP_SELECT_MAX + 1]; int nc = 0;
    memset(receipts, 0, sizeof receipts);
    for (const bf_json_node_t *s = bf_json_child_first(doc, snapshots); s; s = bf_json_child_next(doc, s)) {
        NpReceipt *r = &receipts[nr++];
        np_store_intern(db, doc, s, r, NULL, NULL);
        np_stage = "numerical cut";
        const bf_json_node_t *frags = bf_json_obj_get(doc, bf_json_obj_get(doc, s, "snapshot"), "fragments");
        for (const bf_json_node_t *f = bf_json_child_first(doc, frags); f; f = bf_json_child_next(doc, f)) {
            char *coord = np_str_dup(f->type == BF_JSON_OBJECT ? bf_json_obj_get(doc, f, "coordinate") : NULL);
            if (!coord) continue;
            char digest[65], *revision = NULL; BfNpStyle style;
            if (!np_store_resolve(db, r->namespace_, r->route, coord, digest, &revision, &style)) { free(coord); continue; }
            np_stage = "numerical cut";
            if (strcmp(revision, r->revision)) np_fail("stale numerical reference");
            if (nc >= BF_NP_SELECT_MAX) np_fail("numeric cohort must have 1..12 components");
            cands[nc++] = (NpCandidate){r->namespace_, r->route, coord, r->revision, style};
            free(revision);
        }
    }
    if (!nc) np_fail("numeric cohort must have 1..12 components");
    NpEntry *entries = calloc((size_t)nc, sizeof(NpEntry));
    if (!entries) np_fail("out of memory");
    for (int i = 0; i < nc; i++) {
        char id[16]; snprintf(id, sizeof id, "v%d", i);
        entries[i].id = strdup(id); entries[i].style = cands[i].style; np_address(&entries[i].style, entries[i].address);
    }
    NpFamily family; np_factor(entries, nc, &family);
    NpPredicate pred; np_predicate(doc, predicate, &pred);
    NpCompiled c; np_compile(&family, &pred, eng, &c);
    np_stage = "numerical cut";
    if (artifact_dir && strcmp(c.engine, "lightningcss:1.33.0")) np_fail("only pinned Lightning CSS 1.33.0 may materialize an asset");
    int bodies = np_count(db, "SELECT COUNT(*) FROM _scene_numeric_body"), references = np_count(db, "SELECT COUNT(*) FROM _scene_numeric_ref");
    char artifact[128] = {0};
    if (artifact_dir) {
        snprintf(artifact, sizeof artifact, "numeric-%s.css", c.out_sha);
        char file[PATH_MAX]; snprintf(file, sizeof file, "%s/%s", artifact_dir, artifact);
        mkdir(artifact_dir, 0755);
        char existing[65];
        if (!access(file, F_OK)) {
            if (bf_sha256_file(file, existing) || strcmp(existing, c.out_sha)) np_fail("existing CSS body hash mismatch");
        } else {
            char temp[PATH_MAX + 32]; snprintf(temp, sizeof temp, "%s.%d.tmp", file, (int)getpid());
            int fd = open(temp, O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (fd < 0) np_fail("artifact write failed");
            size_t len = strlen(c.css);
            if (write(fd, c.css, len) != (ssize_t)len) np_fail("artifact write failed");
            close(fd);
            if (rename(temp, file)) np_fail("artifact write failed");
        }
    }
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "receipt"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "schema"); bf_jsonw_str(w, "bonfyre.numeric-presentation-cut.v1");
    bf_jsonw_key(w, "source_revisions"); bf_jsonw_arr(w); for (int i = 0; i < nr; i++) bf_jsonw_str(w, receipts[i].revision); bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "cache"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "bodies"); bf_jsonw_int(w, bodies);
    bf_jsonw_key(w, "references"); bf_jsonw_int(w, references);
    bf_jsonw_obj_end(w);
    bf_jsonw_key(w, "family_basis"); bf_jsonw_str(w, family.basis_address);
    bf_jsonw_key(w, "layout_sources"); bf_jsonw_arr(w); for (int i = 0; i < c.nselected; i++) bf_jsonw_str(w, c.selected[i]); bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "selected_consumers"); bf_jsonw_arr(w);
    for (int i = 0; i < nc; i++) {
        int chosen = 0;
        for (int k = 0; k < c.nselected; k++) if (!strcmp(c.selected[k], entries[i].address)) chosen = 1;
        if (!chosen) continue;
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "namespace"); bf_jsonw_str(w, cands[i].ns);
        bf_jsonw_key(w, "route"); bf_jsonw_str(w, cands[i].route);
        bf_jsonw_key(w, "coordinate"); bf_jsonw_str(w, cands[i].coordinate);
        bf_jsonw_key(w, "revision"); bf_jsonw_str(w, cands[i].revision);
        bf_jsonw_key(w, "style_digest"); bf_jsonw_str(w, entries[i].address);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "engine"); bf_jsonw_str(w, c.engine);
    bf_jsonw_key(w, "hvm"); bf_jsonw_str(w, c.hvm.sha);
    bf_jsonw_key(w, "egglog"); bf_jsonw_str(w, c.egg.sha);
    bf_jsonw_key(w, "raw_css_sha256"); bf_jsonw_str(w, c.raw_sha);
    bf_jsonw_key(w, "css_sha256"); bf_jsonw_str(w, c.out_sha);
    if (artifact_dir) { bf_jsonw_key(w, "artifact_name"); bf_jsonw_str(w, artifact); }
    bf_jsonw_obj_end(w);
    bf_jsonw_key(w, "css"); bf_jsonw_str(w, c.css);
    bf_jsonw_obj_end(w);
    sqlite3_close(db);
    return 0;
}

/* ---- CLI ----------------------------------------------------------------- */

static const char *np_opt(int argc, char **argv, const char *name) {
    for (int i = 1; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static char *np_stdin(size_t *len_out) {
    size_t cap = 65536, len = 0; char *buf = malloc(cap + 1);
    if (!buf) np_fail("out of memory");
    for (;;) {
        size_t r = fread(buf + len, 1, cap - len, stdin); len += r;
        if (len < cap) break;
        if (cap >= (size_t)16 << 20) np_fail("input too large");
        cap *= 2; buf = realloc(buf, cap + 1);
        if (!buf) np_fail("out of memory");
    }
    buf[len] = 0; *len_out = len;
    return buf;
}

static void np_default_engines(NpEngines *eng, int argc, char **argv, const char *self) {
    static char hvm[PATH_MAX], egg[PATH_MAX];
    eng->hvm = np_opt(argc, argv, "--hvm"); if (!eng->hvm) eng->hvm = getenv("BONFYRE_HVM4");
    if (!eng->hvm) {
        char exe[PATH_MAX];
        if (self && strchr(self, '/') && realpath(self, exe)) {
            char *slash = strrchr(exe, '/'); if (slash) *slash = 0;
            snprintf(hvm, sizeof hvm, "%s/../../vendor/hvm4/src/hvm4", exe);
            char resolved[PATH_MAX]; if (realpath(hvm, resolved)) snprintf(hvm, sizeof hvm, "%s", resolved);
            eng->hvm = hvm;
        }
    }
    eng->egglog = np_opt(argc, argv, "--egglog"); if (!eng->egglog) eng->egglog = getenv("BONFYRE_EGGLOG");
    if (!eng->egglog && getenv("HOME")) { snprintf(egg, sizeof egg, "%s/.bonfyre/substrates/v6.1/egglog/target/release/egglog", getenv("HOME")); eng->egglog = egg; }
    eng->css = np_opt(argc, argv, "--css");
    static char lightning[PATH_MAX];
    eng->lightning = getenv("BONFYRE_LIGHTNINGCSS");
    if (!eng->lightning && self && strchr(self, '/') && realpath(self, lightning)) {
        char *slash = strrchr(lightning, '/'); if (slash) *slash = 0;
        size_t used = strlen(lightning);
        snprintf(lightning + used, sizeof lightning - used, "/../../deploy/tailwind/node_modules/.bin/lightningcss");
        eng->lightning = lightning;
    }
    eng->fixture = np_opt(argc, argv, "--lightning-fixture");
    eng->label = np_opt(argc, argv, "--engine-label");
}

#define NP_VERBS(X) \
    X("address", NP_ADDRESS) X("compile", NP_COMPILE) X("counts", NP_COUNTS) X("css", NP_CSS) X("cut", NP_CUT) \
    X("expand", NP_EXPAND) X("factor", NP_FACTOR) X("intern", NP_INTERN) X("lightning", NP_LIGHTNING) X("normalize", NP_NORMALIZE) \
    X("resolve", NP_RESOLVE) X("select", NP_SELECT)

BF_COMMAND_TABLE(np_verbs, NP_VERBS);

int bf_numeric_main(int argc, char **argv, const char *self) {
    int verb = argc > 1 ? bf_command_id(np_verbs, BF_COMMAND_COUNT(np_verbs), argv[1], -1) : -1;
    if (verb < 0) {
        fprintf(stderr, "usage: bonfyre-surface-scene numeric <address|normalize|factor|expand --address A|select|css|compile|lightning|intern|resolve|counts|cut> [--db FILE] [--css raw|lightning] [--hvm BIN] [--egglog BIN] [--lightning-fixture BIN --engine-label fixture:NAME] [--artifact-dir DIR] < input.json\n");
        return 2;
    }
    const char *db_path = np_opt(argc, argv, "--db");
    NpEngines eng; np_default_engines(&eng, argc, argv, self);
    bf_jsonw w; bf_jsonw_init(&w, 0);
    if (verb == NP_COUNTS || verb == NP_RESOLVE) {
        sqlite3 *db = np_store_open(db_path);
        if (verb == NP_COUNTS) {
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "bodies"); bf_jsonw_int(&w, np_count(db, "SELECT COUNT(*) FROM _scene_numeric_body"));
            bf_jsonw_key(&w, "references"); bf_jsonw_int(&w, np_count(db, "SELECT COUNT(*) FROM _scene_numeric_ref"));
            bf_jsonw_obj_end(&w);
        } else {
            char digest[65], *revision = NULL; BfNpStyle style;
            if (!np_store_resolve(db, np_opt(argc, argv, "--namespace"), np_opt(argc, argv, "--route"), np_opt(argc, argv, "--coordinate"), digest, &revision, &style)) bf_jsonw_null(&w);
            else {
                bf_jsonw_obj(&w);
                bf_jsonw_key(&w, "digest"); bf_jsonw_str(&w, digest);
                bf_jsonw_key(&w, "source_revision"); bf_jsonw_str(&w, revision);
                bf_jsonw_key(&w, "value"); np_write_style(&w, &style);
                bf_jsonw_obj_end(&w);
            }
        }
        sqlite3_close(db);
    } else {
        size_t len; char *text = np_stdin(&len), err[128];
        bf_json_doc_t *doc = verb == NP_LIGHTNING ? NULL : bf_json_parse(text, len, err, sizeof err);
        if (!doc && verb != NP_LIGHTNING) np_fail("input is not JSON");
        const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
        NpFamily family; BfNpStyle style; char hex[65];
        switch (verb) {
        case NP_NORMALIZE: np_normalize(doc, root, &style); np_write_style(&w, &style); break;
        case NP_ADDRESS: np_normalize(doc, root, &style); np_address(&style, hex); bf_jsonw_str(&w, hex); break;
        case NP_FACTOR: np_factor_json(doc, root, &family); np_write_family(&w, &family); break;
        case NP_EXPAND: {
            const char *address = np_opt(argc, argv, "--address");
            np_family_read(doc, root, &family);
            np_expand(&family, address ? address : "", &style); np_write_style(&w, &style); break;
        }
        case NP_SELECT: {
            NpPredicate p; NpLowered lowered;
            np_family_read(doc, bf_json_obj_get(doc, root, "family"), &family);
            np_predicate(doc, bf_json_obj_get(doc, root, "predicate"), &p);
            np_lower_selection(&family, &p, &lowered);
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "program"); bf_jsonw_str(&w, lowered.program);
            bf_jsonw_key(&w, "expected"); bf_jsonw_arr(&w); for (int i = 0; i < lowered.n; i++) bf_jsonw_int(&w, lowered.expected[i]); bf_jsonw_arr_end(&w);
            bf_jsonw_key(&w, "bindings"); bf_jsonw_arr(&w); for (int i = 0; i < family.nbindings; i++) bf_jsonw_str(&w, family.bindings[i].id); bf_jsonw_arr_end(&w);
            bf_jsonw_key(&w, "candidate_count"); bf_jsonw_int(&w, family.nbindings);
            bf_jsonw_obj_end(&w);
            break;
        }
        case NP_CSS: {
            const bf_json_node_t *list = bf_json_obj_get(doc, root, "addresses");
            np_family_read(doc, bf_json_obj_get(doc, root, "family"), &family);
            np_stage = "numeric presentation compiler";
            if (!list || list->type != BF_JSON_ARRAY || list->child_count < 1 || list->child_count > BF_NP_SELECT_MAX) np_fail("CSS selection count");
            char (*addresses)[65] = calloc((size_t)list->child_count, 65); int n = 0;
            if (!addresses) np_fail("out of memory");
            for (const bf_json_node_t *a = bf_json_child_first(doc, list); a; a = bf_json_child_next(doc, a))
                if (np_hex64_node(a)) { memcpy(addresses[n], a->val.str.ptr, 64); addresses[n++][64] = 0; }
            np_stage = "numeric presentation";
            char *css = np_lower_css(&family, addresses, n);
            fputs(css, stdout);
            return 0;
        }
        case NP_COMPILE: {
            NpPredicate p; NpCompiled c;
            const bf_json_node_t *pred = bf_json_obj_get(doc, root, "predicate");
            np_family_read(doc, bf_json_obj_get(doc, root, "family"), &family);
            np_predicate(doc, pred, &p);
            np_compile(&family, &p, &eng, &c);
            np_write_compiled(&w, doc, pred, &c);
            break;
        }
        case NP_INTERN: {
            sqlite3 *db = np_store_open(db_path);
            NpReceipt r; memset(&r, 0, sizeof r);
            np_store_intern(db, doc, root, &r, NULL, NULL);
            np_write_receipt(&w, &r);
            sqlite3_close(db);
            break;
        }
        case NP_LIGHTNING: {
            /* Raw CSS on stdin: the reader stylesheet and numeric candidates
             * share this one pinned transform. */
            const char *label = NULL; char hex[65];
            char *css = np_lightning(text, &eng, &label);
            np_sha(css, strlen(css), hex);
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "css"); bf_jsonw_str(&w, css);
            bf_jsonw_key(&w, "engine"); bf_jsonw_str(&w, label);
            bf_jsonw_key(&w, "sha256"); bf_jsonw_str(&w, hex);
            bf_jsonw_obj_end(&w);
            break;
        }
        case NP_CUT: np_cut(db_path, np_opt(argc, argv, "--artifact-dir"), doc, root, &eng, &w); break;
        default: return 2;
        }
    }
    if (w.oom) np_fail("out of memory");
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout);
    return 0;
}
