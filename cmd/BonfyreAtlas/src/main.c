/*
 * bonfyre-atlas — the Architecture Atlas in C. YAFF is law; the index, the
 * organ catalogue, WIRING.md and the fs tree are generated views.
 *
 * Port of architecture/{atlas,atlas-core,atlas-index,atlas-query,wiring}.mjs.
 * Output (text, JSON, written views) is byte-identical to those modules
 * (tests/parity.sh). The field contract is architecture/atlas-fields.cue,
 * compiled in; if the CUE beside the declarations has changed since the
 * build, it is exported again at run time rather than trusted stale.
 *
 *   bonfyre-atlas validate|fields|loss|maturity|export|wiring|wiring-doc|audit
 *   bonfyre-atlas expand|view NAME
 *   bonfyre-atlas summary|get ID|search Q|cannot-infer ID    (text, from the index)
 *   bonfyre-atlas query OP [ARG]                             (JSON, as atlas-query.mjs)
 *   bonfyre-atlas fs [DIR]
 *   bonfyre-atlas witness-grade [--mutation receipt.json]
 *   bonfyre-atlas build-edges [--strict] [--json]
 */
#include "bf_jv.h"
#include "bf_jsonw.h"
#include "bonfyre.h"
#include "fields.h"

#include <dirent.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static char *dir;    /* the architecture directory */
static char *root;   /* its parent, the checkout */
static char *opens_dir, *opens_root;   /* where witnesses and sources must open (default: dir, root) */

static char *fmt(const char *f, ...) {
    va_list ap; va_start(ap, f); int n = vsnprintf(NULL, 0, f, ap); va_end(ap);
    char *s = malloc((size_t)n + 1); va_start(ap, f); vsnprintf(s, (size_t)n + 1, f, ap); va_end(ap);
    return s;
}
static char *slurp(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb"); if (!f) return NULL;
    size_t cap = 1 << 16, n = 0; char *b = malloc(cap);
    for (size_t got; (got = fread(b + n, 1, cap - n - 1, f)) > 0;) { n += got; if (n + 1 >= cap) b = realloc(b, cap *= 2); }
    fclose(f); b[n] = 0; if (len) *len = n; return b;
}
static int exists(const char *p) { struct stat st; return !stat(p, &st); }

/* ── field schema ───────────────────────────────────────────────────────── */

static bf_jv *schema;
static int in_list(const char *list, const char *key) {
    const bf_jv *a = bf_jv_get(schema, list);
    for (size_t i = 0; a && i < a->len; i++) if (!strcmp(a->items[i]->s, key)) return 1;
    return 0;
}
static const char *alias_of(const char *key) { return bf_jv_s(bf_jv_get(bf_jv_get(schema, "aliases"), key)); }

static void load_schema(void) {
    char *cue = fmt("%s/atlas-fields.cue", dir);
    size_t n; char *text = slurp(cue, &n);
    char hex[65] = "";
    if (text) bf_sha256_hex((const uint8_t *)text, n, hex);
    const char *json = (const char *)atlas_fields_json;
    char *fresh = NULL;
    if (text && strcmp(hex, atlas_fields_cue_sha256)) {   /* the CUE moved on since this build */
        char *cmd = fmt("cue export '%s' 2>&1", cue);
        FILE *p = popen(cmd, "r");
        size_t cap = 4096, len = 0; fresh = malloc(cap);
        for (size_t got; p && (got = fread(fresh + len, 1, cap - len - 1, p)) > 0;) { len += got; if (len + 1 >= cap) fresh = realloc(fresh, cap *= 2); }
        fresh[len] = 0;
        int rc = p ? pclose(p) : -1;
        if (rc) { fprintf(stderr, "Atlas field schema invalid: %s\n", fresh); exit(1); }
        json = fresh;
        free(cmd);
    }
    char err[128];
    schema = bf_jv_parse(json, strlen(json), err, sizeof err);
    if (!schema) { fprintf(stderr, "Atlas field schema invalid: %s\n", err); exit(1); }
    free(text); free(cue); free(fresh);
}

/* ── declarations ───────────────────────────────────────────────────────── */

typedef struct { char **v; int n, cap; } Strs;
static void push(Strs *s, char *v) { if (s->n == s->cap) s->v = realloc(s->v, sizeof *s->v * (size_t)(s->cap = s->cap ? s->cap * 2 : 16)); s->v[s->n++] = v; }
static int cmp_str(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

static void walk(const char *base, const char *rel, Strs *out) {
    char *path = *rel ? fmt("%s/%s", base, rel) : strdup(base);
    DIR *d = opendir(path);
    if (!d) { free(path); return; }
    for (struct dirent *e; (e = readdir(d));) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char *r = *rel ? fmt("%s/%s", rel, e->d_name) : strdup(e->d_name);
        unsigned char t = e->d_type;
        if (t == DT_UNKNOWN) { char *full = fmt("%s/%s", base, r); struct stat st; if (!lstat(full, &st)) t = S_ISDIR(st.st_mode) ? DT_DIR : S_ISREG(st.st_mode) ? DT_REG : DT_LNK; free(full); }
        if (t == DT_DIR) { walk(base, r, out); free(r); }
        else if (t == DT_REG && strlen(e->d_name) > 5 && !strcmp(e->d_name + strlen(e->d_name) - 5, ".yaff")) push(out, r);
        else free(r);
    }
    closedir(d); free(path);
}
static Strs yaff_files(void) { Strs s = {0}; walk(dir, "", &s); qsort(s.v, (size_t)s.n, sizeof *s.v, cmp_str); return s; }

typedef struct { char *kind, *ident, *source_file; bf_jv *scalars, *lists; } Block;
typedef struct { Block **b; int n, cap; char **ids; } Coll;   /* insertion order, last definition wins */
typedef struct { Coll arch, view, inter, pheno, edge; bf_jv *undeclared; Strs duplicates; Block **order; int norder; } Atlas;

static Coll *coll_of(Atlas *a, const char *kind) {
    if (!strcmp(kind, "architecture")) return &a->arch;
    if (!strcmp(kind, "view")) return &a->view;
    if (!strcmp(kind, "interaction")) return &a->inter;
    if (!strcmp(kind, "phenotype")) return &a->pheno;
    return &a->edge;
}
static Block *find(const Coll *c, const char *id) { for (int i = 0; i < c->n; i++) if (!strcmp(c->b[i]->ident, id)) return c->b[i]; return NULL; }
static void coll_put(Atlas *a, Coll *c, Block *b) {
    for (int i = 0; i < c->n; i++) if (!strcmp(c->b[i]->ident, b->ident)) {
        push(&a->duplicates, fmt("%s %s: duplicate id", b->kind, b->ident));
        c->b[i] = b; return;
    }
    if (c->n == c->cap) c->b = realloc(c->b, sizeof *c->b * (size_t)(c->cap = c->cap ? c->cap * 2 : 64));
    c->b[c->n++] = b;
}

static int js_ws(unsigned char c) { return c == ' ' || (c >= 9 && c <= 13); }
static char *trim(const char *s) {
    while (js_ws((unsigned char)*s)) s++;
    size_t n = strlen(s); while (n && js_ws((unsigned char)s[n - 1])) n--;
    char *o = malloc(n + 1); memcpy(o, s, n); o[n] = 0; return o;
}

static void parse_file(Atlas *a, const char *rel) {
    char *path = fmt("%s/%s", dir, rel);
    char *text = slurp(path, NULL); free(path);
    if (!text) return;
    const char *base = strrchr(rel, '/'); base = base ? base + 1 : rel;
    Block *cur = NULL; int header = 0;
    for (char *line = text; line;) {
        char *nl = strchr(line, '\n'); if (nl) *nl = 0;
        size_t ll = strlen(line); if (ll && line[ll - 1] == '\r') line[--ll] = 0;
        char *stripped = trim(line);
        if (*stripped && *stripped != '#') {
            char *sp = strchr(stripped, ' ');
            char *raw_key = sp ? strndup(stripped, (size_t)(sp - stripped)) : strdup(stripped);
            char *value = sp ? trim(sp + 1) : strdup("");
            const char *al = alias_of(raw_key); const char *key = al ? al : raw_key;
            if (js_ws((unsigned char)line[0])) {
                if (cur) {
                    if (in_list("list_fields", key)) {
                        bf_jv *l = bf_jv_get(cur->lists, key);
                        if (!l) { bf_jv_set(cur->lists, key, bf_jv_arr()); l = bf_jv_get(cur->lists, key); }
                        bf_jv_push(l, bf_jv_str(value));
                    } else {
                        if (!in_list("scalar_fields", key)) {
                            bf_jv *u = bf_jv_get(a->undeclared, key);
                            if (!u) { bf_jv_set(a->undeclared, key, bf_jv_arr()); u = bf_jv_get(a->undeclared, key); }
                            bf_jv_push(u, bf_jv_str(base));
                        }
                        bf_jv_set(cur->scalars, key, bf_jv_str(value));
                    }
                }
            } else if (!header && !strcmp(key, "atlas")) header = 1;
            else if (in_list("block_kinds", key)) {
                cur = calloc(1, sizeof *cur);
                cur->kind = strdup(key); cur->ident = strdup(value); cur->source_file = strdup(base);
                cur->scalars = bf_jv_obj(); cur->lists = bf_jv_obj();
                a->order = realloc(a->order, sizeof *a->order * (size_t)(a->norder + 1)); a->order[a->norder++] = cur;
                coll_put(a, coll_of(a, key), cur);
            } else cur = NULL;
            free(raw_key); free(value);
        }
        free(stripped);
        line = nl ? nl + 1 : NULL;
    }
    free(text);
}

static Atlas load_atlas(void) {
    Atlas a; memset(&a, 0, sizeof a);
    a.undeclared = bf_jv_obj();
    Strs files = yaff_files();
    for (int i = 0; i < files.n; i++) parse_file(&a, files.v[i]);
    return a;
}

static const char *field(const Block *b, const char *k, const char *fallback) { const char *v = b ? bf_jv_s(bf_jv_get(b->scalars, k)) : NULL; return v ? v : fallback; }
static const bf_jv *list(const Block *b, const char *k) { static bf_jv *empty; if (!empty) empty = bf_jv_arr(); const bf_jv *l = b ? bf_jv_get(b->lists, k) : NULL; return l ? l : empty; }
static int list_has(const bf_jv *l, const char *s) { for (size_t i = 0; i < l->len; i++) if (!strcmp(l->items[i]->s, s)) return 1; return 0; }

/* Block indices sorted by id (sortedEntries). */
static int *sorted_ids(const Coll *c) {
    int *o = malloc(sizeof(int) * (size_t)(c->n + 1));
    for (int i = 0; i < c->n; i++) o[i] = i;
    for (int i = 1; i < c->n; i++) for (int j = i; j > 0 && strcmp(c->b[o[j - 1]]->ident, c->b[o[j]]->ident) > 0; j--) { int t = o[j]; o[j] = o[j - 1]; o[j - 1] = t; }
    return o;
}
static Strs views_of(const Atlas *a, const char *id) {
    Strs s = {0};
    for (int i = 0; i < a->view.n; i++) if (list_has(list(a->view.b[i], "expands_to"), id)) push(&s, a->view.b[i]->ident);
    qsort(s.v, (size_t)s.n, sizeof *s.v, cmp_str);
    return s;
}

static int looks_like_path(const char *v) { size_t n = strlen(v); return strchr(v, '/') && !strchr(v, ' ') && !(n && v[n - 1] == '.'); }
/* The public repository cannot carry machine-local evidence: paths outside the
 * tree, under the home directory, or matched by the repository's own ignore
 * rules (client tenants, private packs, generated stores). With
 * BONFYRE_ATLAS_PUBLIC_CHECKOUT=1 (CI only) such a path that does not open is
 * counted and reported instead of refused; the estate machine, where the
 * pre-commit hook runs without that variable, still opens every one. A path
 * inside the tree that is not ignored must open everywhere. */
static Strs local_only;
static int public_checkout(void) { const char *v = getenv("BONFYRE_ATLAS_PUBLIC_CHECKOUT"); return v && !strcmp(v, "1"); }
static int declared_local(const char *p) {
    if (p[0] == '/' || p[0] == '~' || !strncmp(p, "../", 3)) return 1;
    for (const char *q = p; *q; q++) if (*q == '\'' || *q == '\\') return 0;
    char *cmd = fmt("git -C '%s' check-ignore -q --no-index -- '%s' 2>/dev/null", opens_root, p);
    int rc = system(cmd); free(cmd);
    return rc == 0;
}
static int opens_here(const char *p);
static int opens(const char *p) {
    if (opens_here(p)) return 1;
    if (!public_checkout() || !declared_local(p)) return 0;
    for (int i = 0; i < local_only.n; i++) if (!strcmp(local_only.v[i], p)) return 1;
    push(&local_only, strdup(p));
    return 1;
}
static int opens_here(const char *p) {
    const char *home = getenv("HOME");
    char *expanded = !strcmp(p, "~") ? strdup(home ? home : "") : !strncmp(p, "~/", 2) ? fmt("%s/%s", home ? home : "", p + 2) : strdup(p);
    int ok = exists(expanded);
    if (!ok && expanded[0] != '/') {
        char *a = fmt("%s/%s", opens_root, expanded), *b = fmt("%s/%s", opens_dir, expanded);
        ok = exists(a) || exists(b); free(a); free(b);
    }
    free(expanded);
    return ok;
}
static int ends_with(const char *s, const char *suffix) { size_t a = strlen(s), b = strlen(suffix); return a >= b && !strcmp(s + a - b, suffix); }

static int rank(const char *m) {
    const bf_jv *mat = bf_jv_get(schema, "maturity");
    for (size_t i = 0; m && i < mat->len; i++) if (!strcmp(mat->items[i]->s, m)) return (int)i;
    return -1;
}

/* ── constitution ───────────────────────────────────────────────────────── */

static Strs validate(const Atlas *a) {
    Strs e = {0};
    int measured = rank("measured");
    for (int i = 0; i < a->arch.n; i++) {
        const Block *b = a->arch.b[i]; const char *id = b->ident;
        static const char *required[5] = {"canonical_name", "family", "domain", "semantic_question", "maturity"};
        for (int k = 0; k < 5; k++) if (!*field(b, required[k], "")) push(&e, fmt("architecture %s: missing required field '%s'", id, required[k]));
        const char *m = field(b, "maturity", "");
        if (*m && rank(m) < 0) push(&e, fmt("architecture %s: unknown maturity '%s'", id, m));
        else if ((rank(m) < 0 ? 0 : rank(m)) >= measured) {
            const bf_jv *w = list(b, "witness");
            if (!w->len) push(&e, fmt("architecture %s: maturity '%s' claims a measurement but carries no witness -- maturity laundering", id, m));
            else {
                int live = 0, all_archived = 1;
                for (size_t k = 0; k < w->len; k++) {
                    const char *s = w->items[k]->s;
                    if (!ends_with(s, ".archive")) all_archived = 0;
                    if (!ends_with(s, ".archive") && (!looks_like_path(s) || opens(s))) live++;
                }
                for (size_t k = 0; k < w->len; k++) {
                    const char *s = w->items[k]->s;
                    if (looks_like_path(s) && !ends_with(s, ".archive") && !opens(s))
                        push(&e, fmt("architecture %s: witness '%s' names a file that does not exist -- a witness that cannot be opened is not one", id, s));
                }
                if (!live) push(&e, fmt("architecture %s: %s -- '%s' is not supported", id,
                    all_archived ? "every witness is archived; a retired witness cannot support a current claim" : "no witness can be opened or run today", m));
            }
        }
    }
    for (int i = 0; i < a->view.n; i++) {
        const bf_jv *c = list(a->view.b[i], "expands_to");
        if (c->len < 2) push(&e, fmt("view %s: must expand to >=2 architectures (is a rename, not a view)", a->view.b[i]->ident));
        for (size_t k = 0; k < c->len; k++) if (!find(&a->arch, c->items[k]->s)) push(&e, fmt("view %s: expands to unknown architecture '%s'", a->view.b[i]->ident, c->items[k]->s));
    }
    for (int i = 0; i < a->inter.n; i++) for (int k = 0; k < 2; k++) {
        const char *end = k ? "destination" : "source", *ref = field(a->inter.b[i], end, "");
        if (!*ref) push(&e, fmt("interaction %s: missing '%s'", a->inter.b[i]->ident, end));
        else if (!find(&a->arch, ref)) push(&e, fmt("interaction %s: %s '%s' is not a registered architecture", a->inter.b[i]->ident, end, ref));
    }
    for (int i = 0; i < a->pheno.n; i++) {
        const char *ref = field(a->pheno.b[i], "architecture", "");
        if (!*ref) push(&e, fmt("phenotype %s: missing 'architecture'", a->pheno.b[i]->ident));
        else if (!find(&a->arch, ref)) push(&e, fmt("phenotype %s: architecture '%s' is not registered", a->pheno.b[i]->ident, ref));
        if (!list(a->pheno.b[i], "produces")->len) push(&e, fmt("phenotype %s: must name what it produces", a->pheno.b[i]->ident));
    }
    /* fact -> owners, in first-seen order */
    bf_jv *owners = bf_jv_obj();
    for (int i = 0; i < a->arch.n; i++) {
        const bf_jv *o = list(a->arch.b[i], "owns");
        for (size_t k = 0; k < o->len; k++) {
            bf_jv *l = bf_jv_get(owners, o->items[k]->s);
            if (!l) { bf_jv_set(owners, o->items[k]->s, bf_jv_arr()); l = bf_jv_get(owners, o->items[k]->s); }
            bf_jv_push(l, bf_jv_str(a->arch.b[i]->ident));
        }
    }
    Strs facts = {0};
    for (size_t i = 0; i < owners->len; i++) push(&facts, owners->keys[i]);
    qsort(facts.v, (size_t)facts.n, sizeof *facts.v, cmp_str);
    for (int i = 0; i < facts.n; i++) {
        const bf_jv *l = bf_jv_get(owners, facts.v[i]);
        if (l->len <= 1) continue;
        Strs ids = {0}; for (size_t k = 0; k < l->len; k++) push(&ids, l->items[k]->s);
        qsort(ids.v, (size_t)ids.n, sizeof *ids.v, cmp_str);
        size_t tot = 1; for (int k = 0; k < ids.n; k++) tot += strlen(ids.v[k]) + 2;
        char *j = calloc(tot, 1); for (int k = 0; k < ids.n; k++) { if (k) strcat(j, ", "); strcat(j, ids.v[k]); }
        push(&e, fmt("WiringSpec: fact '%s' is owned by %zu architectures (%s) -- duplicate ownership", facts.v[i], l->len, j));
    }
    return e;
}

static void backing(const Atlas *a, Strs *e) {
    int *o = sorted_ids(&a->arch);
    for (int x = 0; x < a->arch.n; x++) {
        const Block *b = a->arch.b[o[x]]; const char *id = b->ident;
        const bf_jv *src = list(b, "source_path");
        int nsrc = 0, any_live = 0;
        for (size_t k = 0; k < src->len; k++) {
            const char *p = src->items[k]->s;
            if (!looks_like_path(p)) continue;
            nsrc++;
            if (!opens(p)) push(e, fmt("architecture %s: source_path '%s' names a file that does not open -- a claim resting on it rests on nothing", id, p));
            else if (!ends_with(p, ".archive")) any_live = 1;
        }
        if (nsrc && !any_live) {
            int said = 0; const bf_jv *gaps = list(b, "known_gap");
            static const char *words[6] = {"archiv", "retired", "absorb", "native owner", "superseded", "no longer"};
            for (size_t k = 0; k < gaps->len && !said; k++) {
                char *l = strdup(gaps->items[k]->s); for (char *q = l; *q; q++) if (*q >= 'A' && *q <= 'Z') *q += 32;
                for (int w = 0; w < 6; w++) if (strstr(l, words[w])) said = 1;
                free(l);
            }
            if (!said) push(e, fmt("architecture %s: every source is archived and no known_gap says so -- archived code is history, not an implementation", id));
        }
        const char *m = field(b, "maturity", "");
        if (!strcmp(m, "measured") || !strcmp(m, "proven")) {
            const bf_jv *w = list(b, "witness"); int openable = 0;
            for (size_t k = 0; k < w->len; k++) {
                const char *s = w->items[k]->s;
                if (ends_with(s, ".archive")) continue;
                char *first = strndup(s, strcspn(s, " \t\n\r\f\v"));
                if (!looks_like_path(first) || opens(first)) openable = 1;
                free(first);
            }
            if (w->len && !openable) push(e, fmt("architecture %s: maturity '%s' with no witness that opens", id, m));
        }
    }
    free(o);
}

/* ── loss ───────────────────────────────────────────────────────────────── */

static void print_sorted(const char *key, Strs *s) {
    if (!s->n) return;
    qsort(s->v, (size_t)s->n, sizeof *s->v, cmp_str);
    printf("%s (%d): ", key, s->n);
    for (int i = 0; i < s->n; i++) printf("%s%s", i ? ", " : "", s->v[i]);
    printf("\n");
}

static int cmd_loss(const Atlas *a) {
    int measured = rank("measured");
    Strs endpoints = {0}, phen = {0};
    for (int i = 0; i < a->inter.n; i++) { push(&endpoints, (char *)field(a->inter.b[i], "source", "")); push(&endpoints, (char *)field(a->inter.b[i], "destination", "")); }
    for (int i = 0; i < a->pheno.n; i++) push(&phen, (char *)field(a->pheno.b[i], "architecture", ""));
    Strs r[9] = {{0}};
    static const char *names[9] = {"no_cannot_infer", "no_interaction_contract", "no_lineage", "no_native_format", "unwitnessed", "superseded_but_present", "substrate_without_phenotype", "fact_orphan_produced", "fact_orphan_consumed"};
    bf_jv *producers = bf_jv_obj(), *consumers = bf_jv_obj();
    for (int i = 0; i < a->arch.n; i++) {
        const Block *b = a->arch.b[i]; char *id = b->ident;
        if (!list(b, "forbidden_inference")->len) push(&r[0], id);
        int wired = 0; for (int k = 0; k < endpoints.n; k++) if (!strcmp(endpoints.v[k], id)) wired = 1;
        if (!wired) push(&r[1], id);
        if (!list(b, "source_lineage")->len) push(&r[2], id);
        if (!list(b, "native_format")->len) push(&r[3], id);
        int rk = rank(field(b, "maturity", "")); if ((rk < 0 ? 0 : rk) >= measured && !list(b, "witness")->len) push(&r[4], id);
        if (*field(b, "superseded_by", "")) push(&r[5], id);
        int ph = 0; for (int k = 0; k < phen.n; k++) if (!strcmp(phen.v[k], id)) ph = 1;
        if (!strcmp(field(b, "family", ""), "substrate") && !ph) push(&r[6], id);
        static const char *pk[2] = {"owns", "publishes"}, *ck[2] = {"consumes", "subscribes"};
        for (int k = 0; k < 2; k++) { const bf_jv *l = list(b, pk[k]); for (size_t j = 0; j < l->len; j++) bf_jv_set(producers, l->items[j]->s, bf_jv_bool(1)); }
        for (int k = 0; k < 2; k++) { const bf_jv *l = list(b, ck[k]); for (size_t j = 0; j < l->len; j++) bf_jv_set(consumers, l->items[j]->s, bf_jv_bool(1)); }
    }
    for (size_t i = 0; i < producers->len; i++) if (!bf_jv_has(consumers, producers->keys[i])) push(&r[7], producers->keys[i]);
    for (size_t i = 0; i < consumers->len; i++) if (!bf_jv_has(producers, consumers->keys[i])) push(&r[8], consumers->keys[i]);
    int any = 0;
    for (int k = 0; k < 9; k++) { if (r[k].n) any = 1; print_sorted(names[k], &r[k]); }
    if (!any) printf("no architecture loss detected\n");
    return 0;
}

/* ── export ─────────────────────────────────────────────────────────────── */

/* sorted by String.prototype.localeCompare(..., 'en') */
static int cmp_locale(const void *a, const void *b) { return bf_js_locale_compare(*(char *const *)a, *(char *const *)b); }

static bf_jv *declaration_digest(void) {
    Strs files = yaff_files();
    qsort(files.v, (size_t)files.n, sizeof *files.v, cmp_locale);
    BfSha256 c; bf_sha256_init(&c);
    for (int i = 0; i < files.n; i++) {
        bf_sha256_update(&c, (const uint8_t *)files.v[i], strlen(files.v[i]));
        char *p = fmt("%s/%s", dir, files.v[i]); size_t n; char *t = slurp(p, &n); free(p);
        if (t) bf_sha256_update(&c, (const uint8_t *)t, n);
        free(t);
    }
    uint8_t h[32]; bf_sha256_final(&c, h); char hex[65]; bf_sha256_digest_hex(h, hex);
    bf_jv *o = bf_jv_obj();
    bf_jv_set(o, "declaration_files", bf_jv_num(files.n));
    bf_jv_set(o, "declaration_digest", bf_jv_str(hex));
    return o;
}

static bf_jv *strs_jv(const bf_jv *l) { return bf_jv_clone(l); }

static bf_jv *export_index(const Atlas *a) {
    bf_jv *digest = declaration_digest();
    time_t now = time(NULL); struct tm tm; gmtime_r(&now, &tm);
    char stamp[32]; strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    const char *fixed = getenv("BONFYRE_ATLAS_GENERATED_AT");   /* tests pin the stamp */
    bf_jv *prov = bf_jv_obj();
    bf_jv_set(prov, "generated_at", bf_jv_str(fixed ? fixed : stamp));
    bf_jv_set(prov, "declaration_files", bf_jv_clone(bf_jv_get(digest, "declaration_files")));
    bf_jv_set(prov, "declaration_digest", bf_jv_clone(bf_jv_get(digest, "declaration_digest")));
    bf_jv_set(prov, "cannot_infer", bf_jv_str("a matching digest -> a correct atlas; it says this index was built from these declaration bytes and nothing more"));
    bf_jv_free(digest);
    bf_jv *arch = bf_jv_obj();
    int *o = sorted_ids(&a->arch);
    for (int x = 0; x < a->arch.n; x++) {
        const Block *b = a->arch.b[o[x]];
        bf_jv *r = bf_jv_obj();
        static const char *sc[6] = {"canonical_name", "family", "domain", "layer", "semantic_question", "maturity"};
        for (int k = 0; k < 6; k++) bf_jv_set(r, sc[k], bf_jv_str(field(b, sc[k], "")));
        static const char *lk[][2] = {{"node_types", "node_types"}, {"edge_types", "edge_types"}, {"internal_calculi", "internal_calculus"},
            {"forbidden_inferences", "forbidden_inference"}, {"invariants", "invariant"}, {"source_paths", "source_path"}, {"witnesses", "witness"},
            {"commands", "command"}, {"native_formats", "native_format"}, {"known_gaps", "known_gap"}, {"owns", "owns"}, {"consumes", "consumes"},
            {"publishes", "publishes"}, {"subscribes", "subscribes"}, {"derived", "derived"}};
        for (size_t k = 0; k < sizeof lk / sizeof lk[0]; k++) bf_jv_set(r, lk[k][0], strs_jv(list(b, lk[k][1])));
        Strs v = views_of(a, b->ident); bf_jv *va = bf_jv_arr(); for (int k = 0; k < v.n; k++) bf_jv_push(va, bf_jv_str(v.v[k]));
        bf_jv_set(r, "views", va);
        bf_jv_set(r, "experience_chains", strs_jv(list(b, "experience_chain")));
        bf_jv_set(arch, b->ident, r);
    }
    free(o);
    bf_jv *views = bf_jv_obj(); o = sorted_ids(&a->view);
    for (int x = 0; x < a->view.n; x++) {
        const Block *b = a->view.b[o[x]]; bf_jv *r = bf_jv_obj();
        bf_jv_set(r, "expands_to", strs_jv(list(b, "expands_to")));
        bf_jv_set(r, "collapsed_count", bf_jv_num((double)list(b, "expands_to")->len));
        bf_jv_set(views, b->ident, r);
    }
    free(o);
    bf_jv *inter = bf_jv_obj(); o = sorted_ids(&a->inter);
    for (int x = 0; x < a->inter.n; x++) {
        const Block *b = a->inter.b[o[x]]; bf_jv *r = bf_jv_obj();
        bf_jv_set(r, "source", bf_jv_str(field(b, "source", ""))); bf_jv_set(r, "destination", bf_jv_str(field(b, "destination", "")));
        bf_jv_set(r, "relation", bf_jv_str(field(b, "relation", "")));
        bf_jv_set(r, "preserves", strs_jv(list(b, "preserves"))); bf_jv_set(r, "drops", strs_jv(list(b, "drops")));
        bf_jv_set(r, "forbidden_inferences", strs_jv(list(b, "forbidden_inference"))); bf_jv_set(r, "invalidates_on", strs_jv(list(b, "invalidates_on")));
        bf_jv_set(inter, b->ident, r);
    }
    free(o);
    bf_jv *phen = bf_jv_obj(); o = sorted_ids(&a->pheno);
    for (int x = 0; x < a->pheno.n; x++) {
        const Block *b = a->pheno.b[o[x]]; bf_jv *r = bf_jv_obj();
        bf_jv_set(r, "architecture", bf_jv_str(field(b, "architecture", "")));
        bf_jv_set(r, "consumes", strs_jv(list(b, "consumes"))); bf_jv_set(r, "produces", strs_jv(list(b, "produces")));
        static const char *sc[6] = {"deterministic", "incremental", "distributed", "supports_retraction", "proof_class", "effect_class"};
        for (int k = 0; k < 6; k++) bf_jv_set(r, sc[k], bf_jv_str(field(b, sc[k], "")));
        bf_jv_set(phen, b->ident, r);
    }
    free(o);
    bf_jv *edges = bf_jv_obj(); o = sorted_ids(&a->edge);
    for (int x = 0; x < a->edge.n; x++) {
        const Block *b = a->edge.b[o[x]]; bf_jv *r = bf_jv_obj();
        bf_jv_set(r, "meaning", bf_jv_str(field(b, "meaning", ""))); bf_jv_set(r, "specialized_by", strs_jv(list(b, "specialized_by")));
        bf_jv_set(edges, b->ident, r);
    }
    free(o);
    bf_jv *idx = bf_jv_obj();
    bf_jv_set(idx, "provenance", prov); bf_jv_set(idx, "architectures", arch); bf_jv_set(idx, "views", views);
    bf_jv_set(idx, "interactions", inter); bf_jv_set(idx, "phenotypes", phen); bf_jv_set(idx, "edge_classes", edges);
    return idx;
}

/* str.slice by UTF-16 units over UTF-8 text */
static size_t u16_to_byte(const char *s, size_t units) {
    size_t u = 0; const unsigned char *p = (const unsigned char *)s;
    while (*p && u < units) { int len = *p < 0x80 ? 1 : (*p >> 5) == 6 ? 2 : (*p >> 4) == 14 ? 3 : 4; u += len == 4 ? 2 : 1; p += len; }
    return (size_t)((const char *)p - s);
}
static size_t u16_len(const char *s) { size_t u = 0; for (const unsigned char *p = (const unsigned char *)s; *p;) { int len = *p < 0x80 ? 1 : (*p >> 5) == 6 ? 2 : (*p >> 4) == 14 ? 3 : 4; u += len == 4 ? 2 : 1; p += len; } return u; }

static int organ_rank(const char *m) { return !strcmp(m, "measured") || !strcmp(m, "proven") ? 3 : !strcmp(m, "implemented") ? 2 : !strcmp(m, "declared") || !strcmp(m, "architectural") ? 1 : 0; }
static bf_jv *organ_projection(const bf_jv *index) {
    bf_jv *organs = bf_jv_obj();
    const bf_jv *arch = bf_jv_get(index, "architectures");
    for (size_t i = 0; i < arch->len; i++) {
        const bf_jv *e = arch->items[i]; const bf_jv *sp = bf_jv_get(e, "source_paths");
        for (size_t k = 0; sp && k < sp->len; k++) {
            const char *p = sp->items[k]->s;
            if (strncmp(p, "cmd/", 4)) continue;
            const char *s = p + 4; size_t n = strcspn(s, "/");
            if (!n) continue;
            char *organ = strndup(s, n);
            const bf_jv *pub = bf_jv_get(e, "publishes");
            bf_jv *c = bf_jv_obj();
            bf_jv_set(c, "architecture", bf_jv_str(arch->keys[i]));
            const char *q = bf_jv_s(bf_jv_get(e, "semantic_question"));
            bf_jv_set(c, "description", bf_jv_str(q ? q : ""));
            static const char *f3[4] = {"layer", "family", "domain", "maturity"};
            for (int j = 0; j < 4; j++) { const char *v = bf_jv_s(bf_jv_get(e, f3[j])); bf_jv_set(c, f3[j], bf_jv_str(v ? v : "")); }
            bf_jv_set(c, "publishes", bf_jv_str(pub && pub->len ? pub->items[0]->s : ""));
            const bf_jv *prev = bf_jv_get(organs, organ);
            /* JS: rank[m] is undefined for other grades; undefined > x is false,
             * undefined === undefined is true; lengths are UTF-16 units */
            int rc = organ_rank(bf_jv_s(bf_jv_get(c, "maturity"))), rp = prev ? organ_rank(bf_jv_s(bf_jv_get(prev, "maturity"))) : 0;
            int better = !prev || (rc && rp && rc > rp) ||
                (rc == rp && u16_len(bf_jv_s(bf_jv_get(c, "description"))) > u16_len(bf_jv_s(bf_jv_get(prev, "description"))));
            if (better) bf_jv_set(organs, organ, c); else bf_jv_free(c);
            free(organ);
        }
    }
    return organs;
}

static void write_json(const char *name, const bf_jv *v) {
    char *path = fmt("%s/%s", dir, name);
    size_t n; char *s = bf_jv_dump(v, 2, BF_JV_SORT_JS, &n);
    FILE *f = fopen(path, "wb"); if (!f) { fprintf(stderr, "cannot write %s\n", path); exit(1); }
    fwrite(s, 1, n, f); fclose(f); free(s); free(path);
}

/* ── wiring ─────────────────────────────────────────────────────────────── */

typedef struct { Strs nodes; char **ea, **eb; int ne; } Graph;   /* nodes in first-seen order */
static int node_index(const Graph *g, const char *n) { for (int i = 0; i < g->nodes.n; i++) if (!strcmp(g->nodes.v[i], n)) return i; return -1; }
static void add_edge(Graph *g, char *a, char *b) {
    if (node_index(g, a) < 0) push(&g->nodes, a);
    if (node_index(g, b) < 0) push(&g->nodes, b);
    g->ea = realloc(g->ea, sizeof *g->ea * (size_t)(g->ne + 1)); g->eb = realloc(g->eb, sizeof *g->eb * (size_t)(g->ne + 1));
    g->ea[g->ne] = a; g->eb[g->ne] = b; g->ne++;
}
/* out(n) and in(n) as Sets: insertion order, unique */
static Strs out_of(const Graph *g, const char *n) { Strs s = {0}; for (int i = 0; i < g->ne; i++) if (!strcmp(g->ea[i], n)) { int d = 0; for (int k = 0; k < s.n; k++) if (!strcmp(s.v[k], g->eb[i])) d = 1; if (!d) push(&s, g->eb[i]); } return s; }
static Strs in_of(const Graph *g, const char *n) { Strs s = {0}; for (int i = 0; i < g->ne; i++) if (!strcmp(g->eb[i], n)) { int d = 0; for (int k = 0; k < s.n; k++) if (!strcmp(s.v[k], g->ea[i])) d = 1; if (!d) push(&s, g->ea[i]); } return s; }

typedef struct { Strs *g; int n; } Groups;
static Groups components(const Graph *g) {
    int N = g->nodes.n; char *seen = calloc((size_t)N + 1, 1);
    Strs finish = {0};
    for (int r = 0; r < N; r++) {
        if (seen[r]) continue;
        int cap = 64, sp = 0; int *st = malloc(sizeof(int) * (size_t)cap * 2);
        st[0] = r; st[1] = 0; sp = 1;
        while (sp) {
            sp--; int node = st[2 * sp], closing = st[2 * sp + 1];
            if (closing) { push(&finish, g->nodes.v[node]); continue; }
            if (seen[node]) continue;
            seen[node] = 1;
            if (sp + 1 >= cap) { cap *= 2; st = realloc(st, sizeof(int) * (size_t)cap * 2); }
            st[2 * sp] = node; st[2 * sp + 1] = 1; sp++;
            Strs o = out_of(g, g->nodes.v[node]);
            qsort(o.v, (size_t)o.n, sizeof *o.v, cmp_str);
            for (int k = o.n - 1; k >= 0; k--) {
                int c = node_index(g, o.v[k]);
                if (seen[c]) continue;
                if (sp + 1 >= cap) { cap *= 2; st = realloc(st, sizeof(int) * (size_t)cap * 2); }
                st[2 * sp] = c; st[2 * sp + 1] = 0; sp++;
            }
            free(o.v);
        }
        free(st);
    }
    char *assigned = calloc((size_t)N + 1, 1);
    Groups gr = {calloc((size_t)N + 1, sizeof(Strs)), 0};
    for (int f = finish.n - 1; f >= 0; f--) {
        int r = node_index(g, finish.v[f]);
        if (assigned[r]) continue;
        Strs group = {0}; int *st = malloc(sizeof(int) * (size_t)(N + 1)), sp = 0;
        st[sp++] = r; assigned[r] = 1;
        while (sp) {
            int node = st[--sp];
            push(&group, g->nodes.v[node]);
            Strs in = in_of(g, g->nodes.v[node]);
            for (int k = 0; k < in.n; k++) { int p = node_index(g, in.v[k]); if (!assigned[p]) { assigned[p] = 1; st[sp++] = p; } }
            free(in.v);
        }
        free(st);
        gr.g[gr.n++] = group;
    }
    free(seen); free(assigned);
    return gr;
}

static Strs sorted_copy(const Strs *s) { Strs o = {0}; for (int i = 0; i < s->n; i++) push(&o, s->v[i]); qsort(o.v, (size_t)o.n, sizeof *o.v, cmp_str); return o; }
/* sort groups by size, largest first; stable */
static void sort_by_len_desc(Strs *g, int n) {
    for (int i = 1; i < n; i++) for (int j = i; j > 0 && g[j - 1].n < g[j].n; j--) { Strs t = g[j]; g[j] = g[j - 1]; g[j - 1] = t; }
}

typedef struct { char *a, *b; } Edge;
static int cmp_edge(const void *x, const void *y) { const Edge *p = x, *q = y; int c = strcmp(p->a, q->a); return c ? c : strcmp(p->b, q->b); }

static Graph interaction_graph(const bf_jv *index, bf_jv **relations) {
    Graph g; memset(&g, 0, sizeof g);
    *relations = bf_jv_obj();
    const bf_jv *inter = bf_jv_get(index, "interactions");
    for (size_t i = 0; i < inter->len; i++) {
        const char *s = bf_jv_s(bf_jv_get(inter->items[i], "source")), *d = bf_jv_s(bf_jv_get(inter->items[i], "destination"));
        if (!s || !*s || !d || !*d) continue;
        add_edge(&g, (char *)s, (char *)d);
        char *key = fmt("%s%c%s", s, 1, d);
        const char *rel = bf_jv_s(bf_jv_get(inter->items[i], "relation"));
        bf_jv_set(*relations, key, bf_jv_str(rel ? rel : ""));
        free(key);
    }
    return g;
}

static Edge *one_way(const Graph *g, int *n_out) {
    Groups gr = components(g);
    int *grp = malloc(sizeof(int) * (size_t)(g->nodes.n + 1));
    for (int i = 0; i < gr.n; i++) for (int k = 0; k < gr.g[i].n; k++) grp[node_index(g, gr.g[i].v[k])] = i;
    Edge *e = malloc(sizeof *e * (size_t)(g->ne + 1)); int n = 0;
    for (int i = 0; i < g->ne; i++) {
        if (grp[node_index(g, g->ea[i])] == grp[node_index(g, g->eb[i])]) continue;
        int dup = 0; for (int k = 0; k < n; k++) if (!strcmp(e[k].a, g->ea[i]) && !strcmp(e[k].b, g->eb[i])) dup = 1;
        if (!dup) { e[n].a = g->ea[i]; e[n].b = g->eb[i]; n++; }
    }
    qsort(e, (size_t)n, sizeof *e, cmp_edge);
    *n_out = n;
    return e;
}

static int reach_count(const Graph *g, const char *start) {
    int N = g->nodes.n; char *seen = calloc((size_t)N + 1, 1); int *st = malloc(sizeof(int) * (size_t)(N + 2)), sp = 0, count = 0;
    st[sp++] = node_index(g, start);
    while (sp) {
        int node = st[--sp]; if (node < 0) continue;
        Strs o = out_of(g, g->nodes.v[node]);
        for (int k = 0; k < o.n; k++) { int c = node_index(g, o.v[k]); if (!seen[c]) { seen[c] = 1; count++; st[sp++] = c; } }
        free(o.v);
    }
    free(seen); free(st);
    return count;
}

typedef struct { bf_jv *owns, *consumes, *publishes, *subscribes, *producers, *consumers; Strs ids; } Facts;
static void add_set(bf_jv *map, const char *fact, const char *id) {
    bf_jv *s = bf_jv_get(map, fact);
    if (!s) { bf_jv_set(map, fact, bf_jv_arr()); s = bf_jv_get(map, fact); }
    for (size_t i = 0; i < s->len; i++) if (!strcmp(s->items[i]->s, id)) return;
    bf_jv_push(s, bf_jv_str(id));
}
static Facts load_facts(const bf_jv *index) {
    Facts f; memset(&f, 0, sizeof f);
    f.owns = bf_jv_obj(); f.consumes = bf_jv_obj(); f.publishes = bf_jv_obj(); f.subscribes = bf_jv_obj(); f.producers = bf_jv_obj(); f.consumers = bf_jv_obj();
    const bf_jv *arch = bf_jv_get(index, "architectures");
    for (size_t i = 0; i < arch->len; i++) {
        const bf_jv *r = arch->items[i]; const char *id = arch->keys[i];
        const bf_jv *o = bf_jv_get(r, "owns"), *c = bf_jv_get(r, "consumes"), *p = bf_jv_get(r, "publishes"), *s = bf_jv_get(r, "subscribes");
        if (!(o->len || c->len || p->len || s->len)) continue;
        push(&f.ids, (char *)id);
        bf_jv_set(f.owns, id, bf_jv_clone(o)); bf_jv_set(f.consumes, id, bf_jv_clone(c)); bf_jv_set(f.publishes, id, bf_jv_clone(p)); bf_jv_set(f.subscribes, id, bf_jv_clone(s));
        for (size_t k = 0; k < o->len; k++) add_set(f.producers, o->items[k]->s, id);
        for (size_t k = 0; k < p->len; k++) add_set(f.producers, p->items[k]->s, id);
        for (size_t k = 0; k < c->len; k++) add_set(f.consumers, c->items[k]->s, id);
        for (size_t k = 0; k < s->len; k++) add_set(f.consumers, s->items[k]->s, id);
    }
    return f;
}
static Graph fact_flow(const Facts *f) {
    Edge *e = NULL; int n = 0;
    for (size_t i = 0; i < f->producers->len; i++) {
        const bf_jv *ps = f->producers->items[i], *cs = bf_jv_get(f->consumers, f->producers->keys[i]);
        for (size_t a = 0; a < ps->len; a++) for (size_t b = 0; cs && b < cs->len; b++) {
            if (!strcmp(ps->items[a]->s, cs->items[b]->s)) continue;
            int dup = 0; for (int k = 0; k < n; k++) if (!strcmp(e[k].a, ps->items[a]->s) && !strcmp(e[k].b, cs->items[b]->s)) dup = 1;
            if (!dup) { e = realloc(e, sizeof *e * (size_t)(n + 1)); e[n].a = ps->items[a]->s; e[n].b = cs->items[b]->s; n++; }
        }
    }
    qsort(e, (size_t)n, sizeof *e, cmp_edge);
    Graph g; memset(&g, 0, sizeof g);
    for (int i = 0; i < n; i++) add_edge(&g, e[i].a, e[i].b);
    free(e);
    return g;
}
static Strs sorted_keys(const bf_jv *o) { Strs s = {0}; for (size_t i = 0; i < o->len; i++) push(&s, o->keys[i]); qsort(s.v, (size_t)s.n, sizeof *s.v, cmp_str); return s; }
static char *joined_sorted(const bf_jv *arr, const char *sep) {
    Strs s = {0}; for (size_t i = 0; i < arr->len; i++) push(&s, arr->items[i]->s);
    qsort(s.v, (size_t)s.n, sizeof *s.v, cmp_str);
    size_t tot = 1; for (int i = 0; i < s.n; i++) tot += strlen(s.v[i]) + strlen(sep);
    char *o = calloc(tot, 1); for (int i = 0; i < s.n; i++) { if (i) strcat(o, sep); strcat(o, s.v[i]); }
    free(s.v); return o;
}
static char *join_strs(const Strs *s, const char *sep) {
    size_t tot = 1; for (int i = 0; i < s->n; i++) tot += strlen(s->v[i]) + strlen(sep);
    char *o = calloc(tot, 1); for (int i = 0; i < s->n; i++) { if (i) strcat(o, sep); strcat(o, s->v[i]); }
    return o;
}

static Groups feedback_loops(const Graph *g) {
    Groups gr = components(g), out = {calloc((size_t)gr.n + (size_t)g->nodes.n + 1, sizeof(Strs)), 0};
    for (int i = 0; i < gr.n; i++) if (gr.g[i].n > 1) out.g[out.n++] = sorted_copy(&gr.g[i]);
    for (int i = 0; i < g->nodes.n; i++) {
        Strs o = out_of(g, g->nodes.v[i]); int self = 0;
        for (int k = 0; k < o.n; k++) if (!strcmp(o.v[k], g->nodes.v[i])) self = 1;
        if (self) { Strs one = {0}; push(&one, g->nodes.v[i]); out.g[out.n++] = one; }
        free(o.v);
    }
    sort_by_len_desc(out.g, out.n);
    return out;
}

static int cmd_wiring(const bf_jv *index) {
    bf_jv *rel; Graph g = interaction_graph(index, &rel);
    Groups loops = feedback_loops(&g);
    int nw; Edge *ow = one_way(&g, &nw);
    printf("wiring: %d wired architectures, %d interactions\n", g.nodes.n, g.ne);
    printf("feedback loops closed: %d | one-way flows: %d\n", loops.n, nw);
    for (int i = 0; i < loops.n; i++) { char *j = join_strs(&loops.g[i], " -> "); printf("  loop: %s ->\n", j); free(j); }
    Strs srcs = {0}, sinks = {0};
    for (int i = 0; i < g.nodes.n; i++) {
        Strs o = out_of(&g, g.nodes.v[i]), in = in_of(&g, g.nodes.v[i]);
        if (o.n && !in.n) push(&srcs, g.nodes.v[i]);
        if (in.n && !o.n) push(&sinks, g.nodes.v[i]);
        free(o.v); free(in.v);
    }
    qsort(srcs.v, (size_t)srcs.n, sizeof *srcs.v, cmp_str); qsort(sinks.v, (size_t)sinks.n, sizeof *sinks.v, cmp_str);
    char *a = join_strs(&srcs, ", "), *b = join_strs(&sinks, ", ");
    printf("pure sources (emit, nothing returns): %s\n", srcs.n ? a : "none");
    printf("pure sinks (dead ends): %s\n", sinks.n ? b : "none");
    printf("highest-value loops to close (by destination influence):\n");
    int *infl = malloc(sizeof(int) * (size_t)(nw + 1)), *ord = malloc(sizeof(int) * (size_t)(nw + 1));
    for (int i = 0; i < nw; i++) { infl[i] = reach_count(&g, ow[i].b); ord[i] = i; }
    for (int i = 1; i < nw; i++) for (int j = i; j > 0 && infl[ord[j - 1]] < infl[ord[j]]; j--) { int t = ord[j]; ord[j] = ord[j - 1]; ord[j - 1] = t; }
    for (int i = 0; i < nw && i < 8; i++) {
        const Edge *e = &ow[ord[i]];
        printf("  %s -> %s  [infl %d] -- close a path %s -> ... -> %s to make this a feedback loop\n", e->a, e->b, infl[ord[i]], e->b, e->a);
    }
    Facts f = load_facts(index); Graph flow = fact_flow(&f);
    Strs facts = {0};
    for (size_t i = 0; i < f.producers->len; i++) push(&facts, f.producers->keys[i]);
    for (size_t i = 0; i < f.consumers->len; i++) if (!bf_jv_has(f.producers, f.consumers->keys[i])) push(&facts, f.consumers->keys[i]);
    printf("\n-- WiringSpec fact flow: %d organs, %d facts --\n", f.ids.n, facts.n);
    printf("orphan consumers (organ expects a fact NOBODY provides -- wire these):\n");
    Strs ck = sorted_keys(f.consumers), pk = sorted_keys(f.producers);
    for (int i = 0; i < ck.n; i++) if (!bf_jv_has(f.producers, ck.v[i])) { char *j = joined_sorted(bf_jv_get(f.consumers, ck.v[i]), ", "); printf("  %s <- expected by %s\n", ck.v[i], j); free(j); }
    printf("orphan producers (published, nobody consumes -- terminal surface or unwired):\n");
    for (int i = 0; i < pk.n; i++) if (!bf_jv_has(f.consumers, pk.v[i])) { char *j = joined_sorted(bf_jv_get(f.producers, pk.v[i]), ", "); printf("  %s -> from %s\n", pk.v[i], j); free(j); }
    /* duplicate ownership */
    bf_jv *owners = bf_jv_obj();
    for (size_t i = 0; i < f.owns->len; i++) { const bf_jv *o = f.owns->items[i]; for (size_t k = 0; k < o->len; k++) { bf_jv *l = bf_jv_get(owners, o->items[k]->s); if (!l) { bf_jv_set(owners, o->items[k]->s, bf_jv_arr()); l = bf_jv_get(owners, o->items[k]->s); } int d = 0; for (size_t q = 0; q < l->len; q++) if (!strcmp(l->items[q]->s, f.owns->keys[i])) d = 1; if (!d) bf_jv_push(l, bf_jv_str(f.owns->keys[i])); } }
    Strs ok = sorted_keys(owners); int dups = 0;
    for (int i = 0; i < ok.n; i++) if (bf_jv_get(owners, ok.v[i])->len > 1) dups++;
    if (dups) {
        printf("DUPLICATE OWNERSHIP (two organs claim one fact):\n");
        for (int i = 0; i < ok.n; i++) if (bf_jv_get(owners, ok.v[i])->len > 1) { char *j = joined_sorted(bf_jv_get(owners, ok.v[i]), ", "); printf("  %s: %s\n", ok.v[i], j); free(j); }
    }
    printf("fact feedback loops (closed producer/consumer cycles):\n");
    Groups fl = components(&flow); Groups big = {calloc((size_t)fl.n + 1, sizeof(Strs)), 0};
    for (int i = 0; i < fl.n; i++) if (fl.g[i].n > 1) big.g[big.n++] = sorted_copy(&fl.g[i]);
    sort_by_len_desc(big.g, big.n);
    for (int i = 0; i < big.n; i++) { char *j = join_strs(&big.g[i], " <-> "); printf("  %s\n", j); free(j); }
    return 0;
}

static int is_native_path(const bf_jv *paths) {
    static const char *ext[7] = {".c", ".h", ".rs", ".cpp", ".cc", ".hpp", ".zig"};
    for (size_t i = 0; paths && i < paths->len; i++) {
        const char *p = paths->items[i]->s;
        for (int k = 0; k < 7; k++) if (ends_with(p, ext[k])) return 1;
        if (strstr(p, "/substrate/") || strstr(p, "substrates/")) return 1;
    }
    return 0;
}
static int is_python_path(const bf_jv *paths) {
    for (size_t i = 0; paths && i < paths->len; i++) { const char *p = paths->items[i]->s; if (!strncmp(p, "components/", 11) || ends_with(p, ".py")) return 1; }
    return 0;
}

static void audit_line(const char *label, Strs *items) {
    char count[16]; if (items->n) snprintf(count, sizeof count, "%d", items->n); else snprintf(count, sizeof count, "ok");
    char *j = join_strs(items, ", ");
    printf("  [%3s] %s: %s\n", count, label, items->n ? j : "-");
    free(j);
}

static int cmd_audit(const bf_jv *index) {
    Facts f = load_facts(index); Graph flow = fact_flow(&f);
    const bf_jv *arch = bf_jv_get(index, "architectures");
    bf_jv *derived = bf_jv_obj();
    for (size_t i = 0; i < arch->len; i++) { const bf_jv *d = bf_jv_get(arch->items[i], "derived"); for (size_t k = 0; d && k < d->len; k++) bf_jv_set(derived, d->items[k]->s, bf_jv_bool(1)); }
    Edge *po = NULL; int npo = 0;
    for (size_t i = 0; i < arch->len; i++) {
        const bf_jv *r = arch->items[i], *o = bf_jv_get(r, "owns"), *sp = bf_jv_get(r, "source_paths");
        if (!o->len || !is_python_path(sp) || is_native_path(sp)) continue;
        for (size_t k = 0; k < o->len; k++) if (!bf_jv_has(derived, o->items[k]->s)) { po = realloc(po, sizeof *po * (size_t)(npo + 1)); po[npo].a = o->items[k]->s; po[npo].b = arch->keys[i]; npo++; }
    }
    qsort(po, (size_t)npo, sizeof *po, cmp_edge);
    bf_jv *subscribed = bf_jv_obj();
    for (size_t i = 0; i < arch->len; i++) { const bf_jv *s = bf_jv_get(arch->items[i], "subscribes"); for (size_t k = 0; k < s->len; k++) bf_jv_set(subscribed, s->items[k]->s, bf_jv_bool(1)); }
    Strs nondiff = {0};
    for (size_t i = 0; i < f.consumers->len; i++) if (f.consumers->items[i]->len >= 2 && !bf_jv_has(subscribed, f.consumers->keys[i]) && bf_jv_has(f.producers, f.consumers->keys[i])) push(&nondiff, f.consumers->keys[i]);
    qsort(nondiff.v, (size_t)nondiff.n, sizeof *nondiff.v, cmp_str);
    Strs islands = {0}, only_p = {0}, only_c = {0}, gaps = {0};
    for (size_t i = 0; i < arch->len; i++) {
        const bf_jv *r = arch->items[i];
        bf_jv *prod = bf_jv_obj(), *cons = bf_jv_obj();
        static const char *pk[2] = {"owns", "publishes"}, *ck[2] = {"consumes", "subscribes"};
        for (int k = 0; k < 2; k++) { const bf_jv *l = bf_jv_get(r, pk[k]); for (size_t j = 0; j < l->len; j++) bf_jv_set(prod, l->items[j]->s, bf_jv_bool(1)); }
        for (int k = 0; k < 2; k++) { const bf_jv *l = bf_jv_get(r, ck[k]); for (size_t j = 0; j < l->len; j++) bf_jv_set(cons, l->items[j]->s, bf_jv_bool(1)); }
        const char *fam = bf_jv_s(bf_jv_get(r, "family"));
        if (fam && (!strcmp(fam, "apps") || !strcmp(fam, "surface") || !strcmp(fam, "fabric")) && prod->len) {
            int all = 1; for (size_t j = 0; j < prod->len; j++) if (bf_jv_has(f.consumers, prod->keys[j])) all = 0;
            if (all) push(&islands, arch->keys[i]);
        }
        if (prod->len && !cons->len) push(&only_p, arch->keys[i]);
        else if (cons->len && !prod->len) push(&only_c, arch->keys[i]);
        const char *m = bf_jv_s(bf_jv_get(r, "maturity"));
        if (m && (!strcmp(m, "measured") || !strcmp(m, "proven")) && !bf_jv_get(r, "witnesses")->len) push(&gaps, arch->keys[i]);
        bf_jv_free(prod); bf_jv_free(cons);
    }
    qsort(islands.v, (size_t)islands.n, sizeof(char *), cmp_str); qsort(only_p.v, (size_t)only_p.n, sizeof(char *), cmp_str);
    qsort(only_c.v, (size_t)only_c.n, sizeof(char *), cmp_str); qsort(gaps.v, (size_t)gaps.n, sizeof(char *), cmp_str);
    Groups fl = components(&flow); int core = 0;
    for (int i = 0; i < fl.n; i++) if (fl.g[i].n > 1 && fl.g[i].n > core) core = fl.g[i].n;
    int nfacts = (int)f.producers->len; for (size_t i = 0; i < f.consumers->len; i++) if (!bf_jv_has(f.producers, f.consumers->keys[i])) nfacts++;
    printf("audit: %d wired organs, %d facts, feedback core %d organs\n", f.ids.n, nfacts, core);
    Strs pyl = {0}; for (int i = 0; i < npo; i++) push(&pyl, fmt("%s(%s)", po[i].a, po[i].b));
    audit_line("P0/P7 python-only authority", &pyl);
    audit_line("P1 non-differential recompute", &nondiff);
    audit_line("P3/P5 surface islands", &islands);
    Strs oc = {0}, op = {0}, dup = {0};
    Strs ck = sorted_keys(f.consumers), pk = sorted_keys(f.producers);
    for (int i = 0; i < ck.n; i++) if (!bf_jv_has(f.producers, ck.v[i])) push(&oc, ck.v[i]);
    for (int i = 0; i < pk.n; i++) if (!bf_jv_has(f.consumers, pk.v[i])) push(&op, pk.v[i]);
    bf_jv *owners = bf_jv_obj();
    for (size_t i = 0; i < f.owns->len; i++) { const bf_jv *o = f.owns->items[i]; for (size_t k = 0; k < o->len; k++) { bf_jv *l = bf_jv_get(owners, o->items[k]->s); if (!l) { bf_jv_set(owners, o->items[k]->s, bf_jv_arr()); l = bf_jv_get(owners, o->items[k]->s); } int d = 0; for (size_t q = 0; q < l->len; q++) if (!strcmp(l->items[q]->s, f.owns->keys[i])) d = 1; if (!d) bf_jv_push(l, bf_jv_str(f.owns->keys[i])); } }
    Strs okk = sorted_keys(owners); for (int i = 0; i < okk.n; i++) if (bf_jv_get(owners, okk.v[i])->len > 1) push(&dup, okk.v[i]);
    audit_line("orphan consumers (nobody provides)", &oc);
    audit_line("orphan producers (nobody consumes)", &op);
    audit_line("duplicate ownership", &dup);
    audit_line("P6 proof gaps (measured w/o witness)", &gaps);
    audit_line("only-produce organs", &only_p);
    audit_line("only-consume organs", &only_c);
    return 0;
}

static char *wiring_markdown(const bf_jv *index) {
    bf_jv *rel; Graph g = interaction_graph(index, &rel);
    Groups loops = feedback_loops(&g);
    int nw; one_way(&g, &nw);
    Facts f = load_facts(index); Graph flow = fact_flow(&f);
    int nfacts = (int)f.producers->len; for (size_t i = 0; i < f.consumers->len; i++) if (!bf_jv_has(f.producers, f.consumers->keys[i])) nfacts++;
    bf_jv *owners = bf_jv_obj();
    for (size_t i = 0; i < f.owns->len; i++) { const bf_jv *o = f.owns->items[i]; for (size_t k = 0; k < o->len; k++) { bf_jv *l = bf_jv_get(owners, o->items[k]->s); if (!l) { bf_jv_set(owners, o->items[k]->s, bf_jv_arr()); l = bf_jv_get(owners, o->items[k]->s); } bf_jv_push(l, bf_jv_str(f.owns->keys[i])); } }
    int dups = 0; for (size_t i = 0; i < owners->len; i++) if (owners->items[i]->len > 1) dups++;
    size_t cap = 1 << 16; char *md = malloc(cap); size_t n = 0;
#define MD(...) do { char *_l = fmt(__VA_ARGS__); size_t _k = strlen(_l); if (n + _k + 2 > cap) md = realloc(md, cap = (cap + _k) * 2); memcpy(md + n, _l, _k); n += _k; md[n++] = '\n'; free(_l); } while (0)
    MD("# Wiring map (generated)"); MD("%s", "");
    MD("Generated by `bun architecture/atlas.mjs wiring-doc` from the atlas. Do");
    MD("not hand-edit. It shows how the estate wires into itself -- feedback loops");
    MD("(an organ whose effect returns as evidence) versus one-way flows."); MD("%s", "");
    MD("## Interaction graph"); MD("%s", "");
    MD("- %d interacting architectures, %d interactions", g.nodes.n, g.ne);
    MD("- feedback loops closed: %d; one-way flows: %d", loops.n, nw); MD("%s", "");
    MD("## WiringSpec fact flow"); MD("%s", "");
    MD("- %d organs carry a WiringSpec, over %d shared facts", f.ids.n, nfacts);
    MD("- duplicate ownership: %d (validate fails if > 0)", dups); MD("%s", "");
    MD("### Closed fact feedback loops"); MD("%s", "");
    Groups fl = components(&flow); Groups big = {calloc((size_t)fl.n + 1, sizeof(Strs)), 0};
    for (int i = 0; i < fl.n; i++) if (fl.g[i].n > 1) big.g[big.n++] = sorted_copy(&fl.g[i]);
    sort_by_len_desc(big.g, big.n);
    for (int i = 0; i < big.n; i++) { char *j = join_strs(&big.g[i], " ↔ "); MD("- **%d organs**: %s", big.g[i].n, j); free(j); }
    MD("%s", ""); MD("### Open gaps (the next wiring to build)"); MD("%s", "");
    Strs ck = sorted_keys(f.consumers), pk = sorted_keys(f.producers);
    int any_c = 0, any_p = 0;
    for (int i = 0; i < ck.n; i++) if (!bf_jv_has(f.producers, ck.v[i])) any_c = 1;
    for (int i = 0; i < pk.n; i++) if (!bf_jv_has(f.consumers, pk.v[i])) any_p = 1;
    if (any_c) {
        MD("Orphan consumers — an organ expects a fact nobody provides:");
        for (int i = 0; i < ck.n; i++) if (!bf_jv_has(f.producers, ck.v[i])) { char *j = joined_sorted(bf_jv_get(f.consumers, ck.v[i]), ", "); MD("- `%s` ← %s", ck.v[i], j); free(j); }
        MD("%s", "");
    }
    if (any_p) {
        MD("Orphan producers — published, nobody consumes (terminal surface or unwired):");
        for (int i = 0; i < pk.n; i++) if (!bf_jv_has(f.consumers, pk.v[i])) { char *j = joined_sorted(bf_jv_get(f.producers, pk.v[i]), ", "); MD("- `%s` → %s", pk.v[i], j); free(j); }
    }
    md[n] = 0;
    return md;
}

/* ── the index read path (atlas-query.mjs) ──────────────────────────────── */

static bf_jv *organ_projection(const bf_jv *index);
static bf_jv *load_index(int *checked) {
    char *path = fmt("%s/atlas.index.json", dir);
    size_t n; char *text = slurp(path, &n);
    bf_jv *index = NULL;
    *checked = 0;
    char *stamped = NULL;
    if (text) { index = bf_jv_parse(text, n, NULL, 0); const char *s = bf_jv_s(bf_jv_path(index, "provenance.declaration_digest")); stamped = s ? strdup(s) : NULL; }
    /* no declarations here (a deployed index): serve it dated, not verified */
    Strs here_files = yaff_files();
    if (index && !here_files.n) { free(text); free(path); free(stamped); return index; }
    bf_jv *actual = declaration_digest();
    if (index && stamped && !strcmp(stamped, bf_jv_s(bf_jv_get(actual, "declaration_digest")))) { *checked = 1; free(text); free(path); return index; }
    /* missing: export (index and organ catalogue) and read it as written;
     * stale: the same, and say why the answer is not the file that was there */
    load_schema();
    Atlas a = load_atlas();
    bf_jv *fresh = export_index(&a), *organs = organ_projection(fresh);
    write_json("atlas.index.json", fresh);
    write_json("atlas.organs.json", organs);
    bf_jv_sort_keys(fresh, BF_JV_SORT_JS);
    if (index) bf_jv_set(bf_jv_get(fresh, "provenance"), "rebuilt_because", bf_jv_str(stamped
        ? "the index disagreed with the declarations beside it and was regenerated rather than served"
        : "the index carried no provenance stamp, so it could not be shown current"));
    *checked = 1;
    free(text); free(path); free(stamped);
    return fresh;
}

static void source_values(const bf_jv *v, Strs *out) {
    if (!v) return;
    if (v->type == BF_JV_STR) push(out, v->s);
    else if (v->type == BF_JV_ARR || v->type == BF_JV_OBJ) for (size_t i = 0; i < v->len; i++) source_values(v->items[i], out);
}
static char *lower(const char *s) { char *o = strdup(s); for (char *p = o; *p; p++) if (*p >= 'A' && *p <= 'Z') *p += 32; return o; }
static int contains_ci(const char *hay, const char *needle) { char *l = lower(hay); int r = strstr(l, needle) != NULL; free(l); return r; }

static bf_jv *q_summary(const bf_jv *index) {
    const bf_jv *arch = bf_jv_get(index, "architectures"), *views = bf_jv_get(index, "views");
    bf_jv *fam = bf_jv_obj(), *mat = bf_jv_obj();
    for (size_t i = 0; i < arch->len; i++) {
        const char *f = bf_jv_s(bf_jv_get(arch->items[i], "family")), *m = bf_jv_s(bf_jv_get(arch->items[i], "maturity"));
        f = f && *f ? f : "?"; m = m && *m ? m : "?";
        bf_jv *x = bf_jv_get(fam, f); if (x) x->n++; else bf_jv_set(fam, f, bf_jv_num(1));
        x = bf_jv_get(mat, m); if (x) x->n++; else bf_jv_set(mat, m, bf_jv_num(1));
    }
    bf_jv_sort_keys(fam, BF_JV_SORT_JS); bf_jv_sort_keys(mat, BF_JV_SORT_JS);
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "architectures", bf_jv_num((double)arch->len));
    bf_jv_set(out, "views", bf_jv_num((double)(views ? views->len : 0)));
    bf_jv_set(out, "interactions", bf_jv_num((double)bf_jv_get(index, "interactions")->len));
    bf_jv_set(out, "families", fam); bf_jv_set(out, "maturity", mat);
    Strs vn = {0}; for (size_t i = 0; views && i < views->len; i++) push(&vn, views->keys[i]);
    qsort(vn.v, (size_t)vn.n, sizeof *vn.v, cmp_str);
    bf_jv *va = bf_jv_arr(); for (int i = 0; i < vn.n; i++) bf_jv_push(va, bf_jv_str(vn.v[i]));
    bf_jv_set(out, "view_names", va);
    return out;
}
static bf_jv *q_get(const bf_jv *index, const char *id) {
    const bf_jv *arch = bf_jv_get(index, "architectures"), *r = bf_jv_get(arch, id);
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "id", bf_jv_str(id));
    if (!r) {
        bf_jv_set(out, "architecture", bf_jv_null());
        Strs dym = {0}; char *l = lower(id);
        for (size_t i = 0; *id && i < arch->len; i++) if (contains_ci(arch->keys[i], l)) push(&dym, arch->keys[i]);
        free(l);
        if (dym.n > 8) dym.n = 8;
        qsort(dym.v, (size_t)dym.n, sizeof *dym.v, cmp_str);
        bf_jv *a = bf_jv_arr(); for (int i = 0; i < dym.n; i++) bf_jv_push(a, bf_jv_str(dym.v[i]));
        bf_jv_set(out, "did_you_mean", a);
    } else bf_jv_set(out, "architecture", bf_jv_clone(r));
    return out;
}
static bf_jv *q_cannot_infer(const bf_jv *index, const char *id) {
    const bf_jv *r = bf_jv_get(bf_jv_get(index, "architectures"), id), *fi = r ? bf_jv_get(r, "forbidden_inferences") : NULL;
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "id", bf_jv_str(id)); bf_jv_set(out, "exists", bf_jv_bool(r != NULL));
    bf_jv_set(out, "forbidden_inferences", fi ? bf_jv_clone(fi) : bf_jv_arr());
    return out;
}
static bf_jv *q_expand(const bf_jv *index, const char *view) {
    const bf_jv *c = bf_jv_path(index, "views"), *v = bf_jv_get(c, view), *kids = v ? bf_jv_get(v, "expands_to") : NULL, *arch = bf_jv_get(index, "architectures");
    bf_jv *out = bf_jv_obj(), *children = bf_jv_arr();
    bf_jv_set(out, "view", bf_jv_str(view));
    bf_jv_set(out, "expands_to", kids ? bf_jv_clone(kids) : bf_jv_arr());
    for (size_t i = 0; kids && i < kids->len; i++) {
        const bf_jv *r = bf_jv_get(arch, kids->items[i]->s); bf_jv *o = bf_jv_obj();
        bf_jv_set(o, "id", bf_jv_str(kids->items[i]->s));
        bf_jv_set(o, "canonical_name", r ? bf_jv_clone(bf_jv_get(r, "canonical_name")) : bf_jv_null());
        bf_jv_set(o, "maturity", r ? bf_jv_clone(bf_jv_get(r, "maturity")) : bf_jv_null());
        bf_jv_push(children, o);
    }
    bf_jv_set(out, "children", children);
    return out;
}
static bf_jv *q_search(const bf_jv *index, const char *query) {
    char *t = trim(query ? query : ""), *needle = lower(t); free(t);
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "query", bf_jv_str(query ? query : ""));
    if (!*needle) {
        bf_jv_set(out, "total", bf_jv_num(0)); bf_jv_set(out, "matches", bf_jv_arr()); bf_jv_set(out, "truncated", bf_jv_bool(0));
        bf_jv_set(out, "note", bf_jv_str("empty query matches nothing (an empty filter is not a filter)"));
        return out;
    }
    const bf_jv *arch = bf_jv_get(index, "architectures");
    bf_jv *hits = bf_jv_arr();
    for (size_t i = 0; i < arch->len; i++) {
        const char *id = arch->keys[i]; const bf_jv *r = arch->items[i];
        Strs vals = {0}; source_values(r, &vals);
        int hit = contains_ci(id, needle);
        for (int k = 0; !hit && k < vals.n; k++) if (contains_ci(vals.v[k], needle)) hit = 1;
        if (!hit) { free(vals.v); continue; }
        Strs fields = {0};
        for (size_t k = 0; k < r->len; k++) { Strs fv = {0}; source_values(r->items[k], &fv); for (int q = 0; q < fv.n; q++) if (contains_ci(fv.v[q], needle)) { push(&fields, r->keys[k]); break; } free(fv.v); }
        qsort(fields.v, (size_t)fields.n, sizeof *fields.v, cmp_str);
        bf_jv *h = bf_jv_obj(), *fa = bf_jv_arr();
        bf_jv_set(h, "id", bf_jv_str(id));
        const bf_jv *cn = bf_jv_get(r, "canonical_name"), *mt = bf_jv_get(r, "maturity");
        bf_jv_set(h, "canonical_name", cn ? bf_jv_clone(cn) : bf_jv_null());
        bf_jv_set(h, "maturity", mt ? bf_jv_clone(mt) : bf_jv_null());
        if (fields.n) for (int q = 0; q < fields.n; q++) bf_jv_push(fa, bf_jv_str(fields.v[q])); else bf_jv_push(fa, bf_jv_str("id"));
        bf_jv_set(h, "matched_fields", fa);
        char *snip = strdup("");
        for (int k = 0; k < vals.n; k++) {
            char *l = lower(vals.v[k]); char *at = strstr(l, needle);
            if (!at) { free(l); continue; }
            size_t at16 = u16_len(strndup(vals.v[k], (size_t)(at - l)));
            size_t start = at16 > 33 ? at16 - 33 : 0, total = u16_len(vals.v[k]);
            size_t b0 = u16_to_byte(vals.v[k], start), b1 = u16_to_byte(vals.v[k], start + 100);
            free(snip);
            snip = fmt("%s%.*s%s", start ? "..." : "", (int)(b1 - b0), vals.v[k] + b0, start + 100 < total ? "..." : "");
            free(l); break;
        }
        bf_jv_set(h, "snippet", bf_jv_str(snip)); free(snip);
        bf_jv_push(hits, h);
        free(vals.v); free(fields.v);
    }
    /* hits sorted by id */
    for (size_t i = 1; i < hits->len; i++) for (size_t j = i; j > 0 && strcmp(bf_jv_s(bf_jv_get(hits->items[j - 1], "id")), bf_jv_s(bf_jv_get(hits->items[j], "id"))) > 0; j--) { bf_jv *x = hits->items[j]; hits->items[j] = hits->items[j - 1]; hits->items[j - 1] = x; }
    bf_jv *matches = bf_jv_arr(); size_t used = 0; int over = 0;
    for (size_t i = 0; i < hits->len && i < 25; i++) {
        size_t sz; char *s = bf_jv_dump(hits->items[i], 0, BF_JV_SORT_NONE, &sz); free(s);
        if (matches->len && used + sz > 8192) { over = 1; break; }
        bf_jv_push(matches, bf_jv_clone(hits->items[i])); used += sz;
    }
    bf_jv_set(out, "total", bf_jv_num((double)hits->len));
    bf_jv_set(out, "matches", matches);
    bf_jv_set(out, "truncated", bf_jv_bool(over || hits->len > matches->len));
    return out;
}
static bf_jv *q_loss(const bf_jv *index) {
    const bf_jv *arch = bf_jv_get(index, "architectures"), *inter = bf_jv_get(index, "interactions");
    bf_jv *wired = bf_jv_obj();
    for (size_t i = 0; i < inter->len; i++) {
        const char *s = bf_jv_s(bf_jv_get(inter->items[i], "source")), *d = bf_jv_s(bf_jv_get(inter->items[i], "destination"));
        if (s) bf_jv_set(wired, s, bf_jv_bool(1)); if (d) bf_jv_set(wired, d, bf_jv_bool(1));
    }
    Strs a = {0}, b = {0}, c = {0};
    for (size_t i = 0; i < arch->len; i++) {
        if (!bf_jv_has(wired, arch->keys[i])) push(&a, arch->keys[i]);
        const bf_jv *fi = bf_jv_get(arch->items[i], "forbidden_inferences"); if (!fi || !fi->len) push(&b, arch->keys[i]);
        const char *m = bf_jv_s(bf_jv_get(arch->items[i], "maturity")); if (m && !strcmp(m, "architectural")) push(&c, arch->keys[i]);
    }
    Strs *all[3] = {&a, &b, &c}; static const char *names[3] = {"no_interaction_contract", "no_cannot_infer", "unbuilt"};
    bf_jv *out = bf_jv_obj();
    for (int k = 0; k < 3; k++) {
        qsort(all[k]->v, (size_t)all[k]->n, sizeof(char *), cmp_str);
        bf_jv *arr = bf_jv_arr(); for (int i = 0; i < all[k]->n; i++) bf_jv_push(arr, bf_jv_str(all[k]->v[i]));
        bf_jv_set(out, names[k], arr);
    }
    return out;
}

static bf_jv *with_provenance(const bf_jv *index, bf_jv *result, int checked) {
    const bf_jv *prov = bf_jv_get(index, "provenance");
    bf_jv *stamp = bf_jv_obj();
    if (!prov || !prov->len) {
        bf_jv_set(stamp, "verified_against_sources", bf_jv_bool(0));
        bf_jv_set(stamp, "note", bf_jv_str("this index carries no provenance stamp; it cannot be shown current and its age is unknown"));
    } else {
        const bf_jv *g = bf_jv_get(prov, "generated_at"), *df = bf_jv_get(prov, "declaration_files");
        const char *dd = bf_jv_s(bf_jv_get(prov, "declaration_digest"));
        bf_jv_set(stamp, "generated_at", g ? bf_jv_clone(g) : bf_jv_null());
        char short12[13] = ""; if (dd) snprintf(short12, sizeof short12, "%.12s", dd);
        bf_jv_set(stamp, "declaration_digest", bf_jv_str(short12));
        bf_jv_set(stamp, "declaration_files", df ? bf_jv_clone(df) : bf_jv_null());
        bf_jv_set(stamp, "verified_against_sources", bf_jv_bool(checked));
        bf_jv_set(stamp, "note", bf_jv_str(checked ? "checked against the declarations beside it" : "no declarations are present here, so this index is dated and not verified; a newer one may exist in the checkout"));
        const bf_jv *rb = bf_jv_get(prov, "rebuilt_because"); if (rb) bf_jv_set(stamp, "rebuilt_because", bf_jv_clone(rb));
    }
    bf_jv_set(result, "index", stamp);
    return result;
}

static int cmd_query(const char *op_in, const char *arg) {
    char op[32]; snprintf(op, sizeof op, "%s", op_in ? op_in : "");
    for (char *p = op; *p; p++) if (*p == '-') *p = '_';
    static const char *ops[6] = {"summary", "get", "search", "cannot_infer", "expand", "loss"};
    int ok = 0; for (int i = 0; i < 6; i++) if (!strcmp(op, ops[i])) ok = 1;
    if (!ok) { fprintf(stderr, "usage: bun architecture/atlas-query.mjs <summary|get|search|cannot_infer|expand|loss> [arg]\n"); return 2; }
    int checked; bf_jv *index = load_index(&checked);
    bf_jv *r = !strcmp(op, "summary") ? q_summary(index) : !strcmp(op, "get") ? q_get(index, arg) : !strcmp(op, "search") ? q_search(index, arg)
             : !strcmp(op, "cannot_infer") ? q_cannot_infer(index, arg) : !strcmp(op, "expand") ? q_expand(index, arg) : q_loss(index);
    r = with_provenance(index, r, checked);
    char *s = bf_jv_dump(r, 2, BF_JV_SORT_JS, NULL);
    printf("%s\n", s);
    return 0;
}

static int print_query(const char *cmd, const char *arg) {
    int checked; bf_jv *index = load_index(&checked);
    if (!strcmp(cmd, "summary")) {
        bf_jv *v = q_summary(index);
        printf("atlas: %g architectures, %g views, %g interactions\n", bf_jv_get(v, "architectures")->n, bf_jv_get(v, "views")->n, bf_jv_get(v, "interactions")->n);
        const bf_jv *m = bf_jv_get(v, "maturity"), *f = bf_jv_get(v, "families"), *vn = bf_jv_get(v, "view_names");
        printf("  maturity: "); for (size_t i = 0; i < m->len; i++) printf("%s%s:%g", i ? ", " : "", m->keys[i], m->items[i]->n); printf("\n");
        printf("  families: "); for (size_t i = 0; i < f->len; i++) printf("%s%s:%g", i ? ", " : "", f->keys[i], f->items[i]->n); printf("\n");
        printf("  views:    "); for (size_t i = 0; i < vn->len; i++) printf("%s%s", i ? ", " : "", vn->items[i]->s); printf("\n");
        return 0;
    }
    if (!strcmp(cmd, "cannot-infer")) {
        bf_jv *v = q_cannot_infer(index, arg);
        if (!bf_jv_get(v, "exists")->b) { printf("no such architecture: %s\n", arg); return 1; }
        const bf_jv *fi = bf_jv_get(v, "forbidden_inferences");
        for (size_t i = 0; i < fi->len; i++) printf("    %s may NOT be read to imply: %s\n", arg, fi->items[i]->s);
        if (!fi->len) printf("    %s states no forbidden inference (atlas loss)\n", arg);
        return 0;
    }
    if (!strcmp(cmd, "get")) {
        bf_jv *v = q_get(index, arg); const bf_jv *r = bf_jv_get(v, "architecture");
        if (!r || r->type == BF_JV_NULL) {
            printf("no such architecture: %s\n", arg);
            const bf_jv *d = bf_jv_get(v, "did_you_mean");
            if (d->len) { printf("  did you mean: "); for (size_t i = 0; i < d->len; i++) printf("%s%s", i ? ", " : "", d->items[i]->s); printf("\n"); }
            return 1;
        }
        printf("%s  %s  [%s] family=%s\n", arg, bf_jv_s(bf_jv_get(r, "canonical_name")), bf_jv_s(bf_jv_get(r, "maturity")), bf_jv_s(bf_jv_get(r, "family")));
        printf("  asks: %s\n", bf_jv_s(bf_jv_get(r, "semantic_question")));
        static const char *keys[5][2] = {{"source_paths", "source_path"}, {"witnesses", "witnesse"}, {"forbidden_inferences", "forbidden_inference"}, {"invariants", "invariant"}, {"known_gaps", "known_gap"}};
        for (int k = 0; k < 5; k++) { const bf_jv *l = bf_jv_get(r, keys[k][0]); for (size_t i = 0; l && i < l->len; i++) printf("  %s: %s\n", keys[k][1], l->items[i]->s); }
        return 0;
    }
    bf_jv *v = q_search(index, arg);
    printf("search '%s': %g match(es)%s\n", arg, bf_jv_get(v, "total")->n, bf_jv_get(v, "truncated")->b ? " (truncated)" : "");
    const bf_jv *m = bf_jv_get(v, "matches");
    for (size_t i = 0; i < m->len; i++) {
        const bf_jv *h = m->items[i], *mf = bf_jv_get(h, "matched_fields");
        printf("    %-34s [%s] via ", bf_jv_s(bf_jv_get(h, "id")), bf_jv_s(bf_jv_get(h, "maturity")) ? bf_jv_s(bf_jv_get(h, "maturity")) : "null");
        for (size_t k = 0; k < mf->len; k++) printf("%s%s", k ? "," : "", mf->items[k]->s);
        printf("\n");
    }
    return 0;
}

/* ── fields, maturity, expand, fs ───────────────────────────────────────── */

static int cmd_fields(const Atlas *a) {
    int n = (int)a->undeclared->len;
    if (!n && !a->duplicates.n) { printf("every field key in the atlas is one a renderer reads\n"); return 0; }
    int *ord = malloc(sizeof(int) * (size_t)(n + 1)); size_t total = 0;
    for (int i = 0; i < n; i++) { ord[i] = i; total += a->undeclared->items[i]->len; }
    for (int i = 1; i < n; i++) for (int j = i; j > 0 && a->undeclared->items[ord[j - 1]]->len < a->undeclared->items[ord[j]]->len; j--) { int t = ord[j]; ord[j] = ord[j - 1]; ord[j - 1] = t; }
    printf("%d field keys are stored and never read (%zu authored lines):\n", n, total);
    for (int i = 0; i < n; i++) {
        const char *key = a->undeclared->keys[ord[i]]; const bf_jv *files = a->undeclared->items[ord[i]];
        Strs u = {0}; for (size_t k = 0; k < files->len; k++) { int d = 0; for (int q = 0; q < u.n; q++) if (!strcmp(u.v[q], files->items[k]->s)) d = 1; if (!d) push(&u, files->items[k]->s); }
        qsort(u.v, (size_t)u.n, sizeof *u.v, cmp_str);
        printf("  %-32s %4zu  ", key, files->len);
        for (int k = 0; k < u.n && k < 3; k++) printf("%s%s", k ? ", " : "", u.v[k]);
        printf("%s\n", u.n > 3 ? "  ..." : "");
    }
    if (a->duplicates.n) {
        printf("\n%d duplicate block IDs currently resolve by last definition:\n", a->duplicates.n);
        for (int i = 0; i < a->duplicates.n; i++) printf("  %s\n", a->duplicates.v[i]);
    }
    printf("\nEach undeclared field needs a reader or a rename; duplicate identities need authored reconciliation. No inference is made for them.\n");
    return 0;
}

static int cmd_maturity(const Atlas *a) {
    bf_jv *roll = bf_jv_obj();
    for (int i = 0; i < a->arch.n; i++) {
        const char *f = field(a->arch.b[i], "family", "?"), *m = field(a->arch.b[i], "maturity", "?");
        bf_jv *fam = bf_jv_get(roll, f); if (!fam) { bf_jv_set(roll, f, bf_jv_obj()); fam = bf_jv_get(roll, f); }
        bf_jv *c = bf_jv_get(fam, m); if (c) c->n++; else bf_jv_set(fam, m, bf_jv_num(1));
    }
    Strs fams = sorted_keys(roll);
    for (int i = 0; i < fams.n; i++) {
        const bf_jv *v = bf_jv_get(roll, fams.v[i]);
        int *ord = malloc(sizeof(int) * (v->len + 1)); for (size_t k = 0; k < v->len; k++) ord[k] = (int)k;
        for (size_t k = 1; k < v->len; k++) for (size_t j = k; j > 0; j--) {
            int ra = rank(v->keys[ord[j - 1]]), rb = rank(v->keys[ord[j]]);
            if (ra < 0 || rb < 0 || !(rb - ra > 0)) break;   /* NaN compares as no move */
            int t = ord[j]; ord[j] = ord[j - 1]; ord[j - 1] = t;
        }
        printf("%-16s ", fams.v[i]);
        for (size_t k = 0; k < v->len; k++) printf("%s%s:%g", k ? ", " : "", v->keys[ord[k]], v->items[ord[k]]->n);
        printf("\n");
        free(ord);
    }
    return 0;
}

static int cmd_expand(const Atlas *a, const char *cmd, const char *arg) {
    const Block *v = find(&a->view, arg); const bf_jv *c = list(v, "expands_to");
    if (!c->len) { printf("no such view%s: %s\n", !strcmp(cmd, "expand") ? " (or empty)" : "", arg); return 1; }
    if (!strcmp(cmd, "expand")) {
        printf("%s expands to %zu:\n", arg, c->len);
        for (size_t i = 0; i < c->len; i++) {
            const Block *b = find(&a->arch, c->items[i]->s);
            char m[64]; snprintf(m, sizeof m, "[%s]", field(b, "maturity", "?"));
            printf("    %-32s %-34s %s\n", c->items[i]->s, field(b, "canonical_name", "(UNKNOWN)"), m);
        }
    } else {
        printf("┌"); for (int i = 0; i < 52; i++) printf("─"); printf("┐\n");
        printf("│ %-50s │\n", arg);
        printf("│ collapsed: %2zu architectures  ([expand] to zoom)%8s │\n", c->len, "");
        printf("└"); for (int i = 0; i < 52; i++) printf("─"); printf("┘\n");
        for (size_t i = 0; i < c->len; i++) printf("    └─ %s\n", field(find(&a->arch, c->items[i]->s), "canonical_name", c->items[i]->s));
    }
    return 0;
}

static void mkdirs(const char *p) { char *d = strdup(p); for (char *q = d + 1; *q; q++) if (*q == '/') { *q = 0; mkdir(d, 0777); *q = '/'; } mkdir(d, 0777); free(d); }
static void put_lines(const char *path, const bf_jv *l) {
    FILE *f = fopen(path, "wb"); if (!f) return;
    for (size_t i = 0; i < l->len; i++) fprintf(f, "%s%s", i ? "\n" : "", l->items[i]->s);
    fprintf(f, "\n"); fclose(f);
}
static int cmd_fs(const Atlas *a, const char *out) {
    char real[4096]; const char *o = out;
    if (realpath(out, real)) o = real;
    if (!strcmp(o, root) || !strcmp(o, "/") || !strcmp(o, dir)) { fprintf(stderr, "refusing broad Atlas fs target\n"); return 1; }
    char *graphs = fmt("%s/Graphs", out);
    char *rm = fmt("rm -rf '%s'", graphs); if (exists(graphs) && system(rm)) {} free(rm);
    int count = 0;
    for (int i = 0; i < a->arch.n; i++) {
        const Block *b = a->arch.b[i];
        const char *cn = field(b, "canonical_name", "");
        char *d = fmt("%s/%s/%s", graphs, field(b, "family", "unfiled"), *cn ? cn : b->ident);
        mkdirs(d);
        char *spec = fmt("%s/spec.md", d); FILE *f = fopen(spec, "wb");
        if (f) { fprintf(f, "# %s  [%s]\n\n- id: %s\n- domain: %s\n- layer: %s\n\n**Question:** %s\n\n%s\n", cn, field(b, "maturity", ""), b->ident,
                         field(b, "domain", ""), field(b, "layer", ""), field(b, "semantic_question", ""), field(b, "purpose", "")); fclose(f); }
        static const char *files[4][2] = {{"cannot-infer.txt", "forbidden_inference"}, {"witnesses.txt", "witness"}, {"sources.txt", "source_path"}, {"views.txt", NULL}};
        for (int k = 0; k < 4; k++) {
            char *p = fmt("%s/%s", d, files[k][0]);
            if (files[k][1]) put_lines(p, list(b, files[k][1]));
            else { Strs v = views_of(a, b->ident); bf_jv *arr = bf_jv_arr(); for (int q = 0; q < v.n; q++) bf_jv_push(arr, bf_jv_str(v.v[q])); put_lines(p, arr); bf_jv_free(arr); }
            free(p);
        }
        free(spec); free(d); count++;
    }
    printf("✓ wrote introspection tree to %s/Graphs (%d architectures) and %s/Queries/Architecture\n", out, count, out);
    return 0;
}

static bf_jv *block_json(const Block *b) {
    bf_jv *o = bf_jv_obj();
    bf_jv_set(o, "kind", bf_jv_str(b->kind)); bf_jv_set(o, "ident", bf_jv_str(b->ident));
    bf_jv_set(o, "scalars", bf_jv_clone(b->scalars)); bf_jv_set(o, "lists", bf_jv_clone(b->lists));
    bf_jv_set(o, "source_file", bf_jv_str(b->source_file));
    return o;
}

/* parse FILE...: each file's blocks as atlas-core parseFile returned them */
static int cmd_parse(int argc, char **argv) {
    bf_jv *out = bf_jv_obj(), *files = bf_jv_obj();
    Atlas a; memset(&a, 0, sizeof a); a.undeclared = bf_jv_obj();
    for (int i = 0; i < argc; i++) {
        Atlas one; memset(&one, 0, sizeof one); one.undeclared = a.undeclared;
        char real[4096]; const char *p = realpath(argv[i], real) ? real : argv[i];
        char *saved = dir; dir = strdup("");
        char *path = strdup(p); char *slash = strrchr(path, '/');
        if (slash) { *slash = 0; free(dir); dir = strdup(path); parse_file(&one, slash + 1); }
        else parse_file(&one, path);
        free(dir); dir = saved; free(path);
        bf_jv *blocks = bf_jv_arr();
        for (int k = 0; k < one.norder; k++) bf_jv_push(blocks, block_json(one.order[k]));   /* file order, duplicates kept */
        bf_jv_set(files, argv[i], blocks);
    }
    bf_jv_set(out, "files", files);
    bf_jv_set(out, "undeclared", a.undeclared);
    char *s = bf_jv_dump(out, 0, BF_JV_SORT_NONE, NULL); printf("%s\n", s);
    return 0;
}

#include "witness_grade.h"
#include "build_edges.h"
#include "contracts.h"

static char *atlas_dir_from_root(const char *candidate_root) {
    if (!candidate_root || !*candidate_root) return NULL;
    char *candidate = fmt("%s/architecture", candidate_root);
    if (!exists(candidate)) {
        free(candidate);
        return NULL;
    }
    char real[4096];
    if (realpath(candidate, real)) {
        free(candidate);
        return strdup(real);
    }
    return candidate;
}

static char *atlas_default_dir(const char *self) {
    char *candidate;
    const char *repo = getenv("BONFYRE_REPO_ROOT");
    const char *bonfyre_root = getenv("BONFYRE_ROOT");
    if ((candidate = atlas_dir_from_root(repo))) return candidate;
    if ((candidate = atlas_dir_from_root(bonfyre_root))) return candidate;

    char exe[4096];
    if (getcwd(exe, sizeof exe) && (candidate = atlas_dir_from_root(exe))) return candidate;
    if (self && realpath(self, exe)) {
        char *binary_dir = strdup(exe);
        char *slash = strrchr(binary_dir, '/');
        if (slash) *slash = 0;
        slash = strrchr(binary_dir, '/');
        if (slash) *slash = 0; /* cmd */
        slash = strrchr(binary_dir, '/');
        if (slash) *slash = 0; /* checkout */
        candidate = atlas_dir_from_root(binary_dir);
        free(binary_dir);
        if (candidate) return candidate;
    }

    return NULL;
}
int main(int argc, char **argv) {
    const char *self = argv[0];
    /* Prefer explicit estate coordinates, then checkout-relative executable
     * discovery. Cwd is only a compatibility fallback. */
    const char *env = getenv("BONFYRE_ATLAS_DIR");
    if (env && *env) dir = strdup(env);
    else dir = atlas_default_dir(self);
    if (!dir || !exists(dir)) {
        fprintf(stderr, "bonfyre-atlas: cannot locate architecture declarations; set BONFYRE_ATLAS_DIR or BONFYRE_REPO_ROOT\n");
        free(dir);
        return 1;
    }
    char real[4096]; if (realpath(dir, real)) { free(dir); dir = strdup(real); }
    root = strdup(dir); char *slash = strrchr(root, '/'); if (slash && slash != root) *slash = 0;
    opens_dir = dir; opens_root = root;
    int strict = 0;
    for (int i = 1; i < argc; i++) {   /* options anywhere: --strict-duplicates, --opens-from DIR */
        if (!strcmp(argv[i], "--strict-duplicates")) { strict = 1; memmove(argv + i, argv + i + 1, sizeof *argv * (size_t)(argc - i)); argc--; i--; }
        else if (!strcmp(argv[i], "--opens-from") && i + 1 < argc) {
            char r2[4096]; opens_dir = strdup(realpath(argv[i + 1], r2) ? r2 : argv[i + 1]);
            opens_root = strdup(opens_dir); char *sl = strrchr(opens_root, '/'); if (sl && sl != opens_root) *sl = 0;
            memmove(argv + i, argv + i + 2, sizeof *argv * (size_t)(argc - i - 1)); argc -= 2; i--;
        }
    }
    (void)self;
    const char *cmd = argc > 1 ? argv[1] : "validate", *arg = argc > 2 ? argv[2] : "";
    if (!strcmp(cmd, "compile-contracts")) return cmd_contracts(argc - 2, argv + 2);
    if (!strcmp(cmd, "query")) return cmd_query(arg, argc > 3 ? argv[3] : "");
    if (!strcmp(cmd, "schema")) { load_schema(); char *s = bf_jv_dump(schema, 0, BF_JV_SORT_NONE, NULL); printf("%s\n", s); return 0; }
    if (!strcmp(cmd, "digest")) { bf_jv *d = declaration_digest(); char *s = bf_jv_dump(d, 0, BF_JV_SORT_NONE, NULL); printf("%s\n", s); return 0; }
    if (!strcmp(cmd, "files")) { Strs f = yaff_files(); for (int i = 0; i < f.n; i++) printf("%s/%s\n", dir, f.v[i]); return 0; }
    if (!strcmp(cmd, "parse")) { load_schema(); return cmd_parse(argc - 2, argv + 2); }
    if (!strcmp(cmd, "summary") || !strcmp(cmd, "get") || !strcmp(cmd, "search") || !strcmp(cmd, "cannot-infer")) return print_query(cmd, arg);
    load_schema();
    Atlas a = load_atlas();
    if (!strcmp(cmd, "witness-grade")) return cmd_witness_grade(&a, argc - 2, argv + 2);
    if (!strcmp(cmd, "build-edges")) return cmd_build_edges(&a, argc - 2, argv + 2);
    if (!strcmp(cmd, "validate")) {
        Strs e = {0};
        if (strict) for (int i = 0; i < a.duplicates.n; i++) push(&e, a.duplicates.v[i]);
        Strs v = validate(&a); for (int i = 0; i < v.n; i++) push(&e, v.v[i]);
        backing(&a, &e);
        if (e.n) { printf("✗ atlas invalid: %d error(s)\n", e.n); for (int i = 0; i < e.n; i++) printf("    %s\n", e.v[i]); return 1; }
        printf("✓ atlas valid: %d architectures, %d views, %d interactions, %d phenotypes, %d edge-classes\n", a.arch.n, a.view.n, a.inter.n, a.pheno.n, a.edge.n);
        if (local_only.n) printf("  %d machine-local path(s) are not in this public checkout and were not opened here; the estate machine's pre-commit validation opens them\n", local_only.n);
        if (a.duplicates.n) printf("  note: %d duplicate block IDs are last-definition-wins; run atlas.mjs fields\n", a.duplicates.n);
        return 0;
    }
    if (!strcmp(cmd, "fields")) return cmd_fields(&a);
    if (!strcmp(cmd, "loss")) return cmd_loss(&a);
    if (!strcmp(cmd, "maturity")) return cmd_maturity(&a);
    if (!strcmp(cmd, "expand") || !strcmp(cmd, "view")) return cmd_expand(&a, cmd, arg);
    if (!strcmp(cmd, "export")) {
        bf_jv *index = export_index(&a), *organs = organ_projection(index);
        write_json("atlas.index.json", index);
        write_json("atlas.organs.json", organs);
        printf("✓ wrote architecture/atlas.index.json (%d architectures)\n", a.arch.n);
        printf("✓ wrote architecture/atlas.organs.json (%zu organs the capability registry can read)\n", organs->len);
        return 0;
    }
    if (!strcmp(cmd, "wiring") || !strcmp(cmd, "wiring-doc") || !strcmp(cmd, "audit")) {
        bf_jv *index = export_index(&a);
        if (!strcmp(cmd, "wiring")) return cmd_wiring(index);
        if (!strcmp(cmd, "audit")) return cmd_audit(index);
        char *md = wiring_markdown(index), *p = fmt("%s/WIRING.md", dir);
        FILE *f = fopen(p, "wb"); if (f) { fputs(md, f); fclose(f); }
        printf("wrote architecture/WIRING.md\n");
        return 0;
    }
    if (!strcmp(cmd, "fs")) { char *o = *arg ? strdup(arg) : fmt("%s/fs", dir); return cmd_fs(&a, o); }
    fprintf(stderr, "unknown command: %s\n", cmd);
    return 2;
}
