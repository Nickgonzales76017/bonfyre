/*
 * dp_core.c — the DecisionPlane: case compilation, typed answer decoding,
 * projections, selection, local inference and receipts. Port of
 * decision-plane.mjs, typed-decision.mjs and local-inference.mjs.
 */
#include "dp.h"
#include "policy.h"

#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>

DpCatch *dp_catch_top;

void dp_fail(const char *fmt, ...) {
    char msg[2048];
    va_list ap; va_start(ap, fmt); vsnprintf(msg, sizeof msg, fmt, ap); va_end(ap);
    if (dp_catch_top) {
        DpCatch *c = dp_catch_top;
        snprintf(c->msg, sizeof c->msg, "%s", msg);
        dp_catch_top = c->prev;
        longjmp(c->jb, 1);
    }
    fprintf(stderr, "%s\n", msg);
    exit(2);
}

char *dp_fmt(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap); va_end(ap);
    char *s = malloc((size_t)n + 1);
    va_start(ap, fmt); vsnprintf(s, (size_t)n + 1, fmt, ap); va_end(ap);
    return s;
}

double dp_now_ms(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec * 1000.0 + (double)t.tv_nsec / 1e6;
}

int dp_is_hex64(const char *s) {
    if (!s || strlen(s) != 64) return 0;
    for (int i = 0; i < 64; i++) if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return 0;
    return 1;
}

/* ── policy and digests ─────────────────────────────────────────────────── */

bf_jv *dp_policy(void) {
    static bf_jv *policy;
    if (!policy) {
        char err[128];
        policy = bf_jv_parse((const char *)dp_policy_json, sizeof dp_policy_json - 1, err, sizeof err);
        if (!policy) dp_fail("DecisionPlane CUE: %s", err);
    }
    return policy;
}

void dp_digest(const bf_jv *v, char hex[65]) { bf_jv_digest_mode(v, BF_JV_SORT_JS, hex); }
char *dp_digest_str(const bf_jv *v) { char *h = malloc(65); dp_digest(v, h); return h; }
bf_jv *dp_sorted_clone(const bf_jv *v) { bf_jv *c = bf_jv_clone(v); bf_jv_sort_keys(c, BF_JV_SORT_JS); return c; }

static size_t sorted_bytes(const bf_jv *v) { size_t n; char *s = bf_jv_dump(v, 0, BF_JV_SORT_JS, &n); free(s); return n; }
static size_t json_bytes(const bf_jv *v) { size_t n; char *s = bf_jv_dump(v, 0, BF_JV_SORT_NONE, &n); free(s); return n; }

static int is_obj(const bf_jv *v) { return bf_jv_is(v, BF_JV_OBJ); }
static int is_prob(const bf_jv *v) { return bf_jv_is(v, BF_JV_NUM) && isfinite(v->n) && v->n >= 0 && v->n <= 1; }
static int nonempty_str(const bf_jv *v) { return bf_jv_is(v, BF_JV_STR) && v->len > 0; }
static bf_jv *S(const char *s) { return bf_jv_str(s); }
static bf_jv *N(double n) { return bf_jv_num(n); }

/* ── typed decision algebra (typed-decision.mjs) ────────────────────────── */

#define TD(...) dp_fail("typed decision: " __VA_ARGS__)

DpSchema *dp_schema(const bf_jv *questions) {
    if (!is_obj(questions) || !questions->len || questions->len > 255) TD("question budget or empty schema");
    DpSchema *s = calloc(1, sizeof *s);
    s->f = calloc(questions->len, sizeof *s->f);
    int offset = 0;
    for (size_t i = 0; i < questions->len; i++) {
        const char *id = questions->keys[i]; const bf_jv *q = questions->items[i];
        if (!*id || !is_obj(q)) TD("invalid question");
        DpField *f = &s->f[s->n++];
        f->id = strdup(id); f->offset = offset;
        const char *type = bf_jv_s(bf_jv_get(q, "type"));
        const bf_jv *criteria = bf_jv_get(q, "criteria");
        if (type && !strcmp(type, "noul")) {
            f->nopt = 2; f->options = malloc(2 * sizeof *f->options);
            f->options[0] = strdup("true"); f->options[1] = strdup("false");
        } else if (type && !strcmp(type, "choice")) {
            if (!is_obj(criteria)) TD("%s: choice requires criteria", id);
            f->nopt = (int)criteria->len; f->options = malloc(sizeof *f->options * (criteria->len ? criteria->len : 1));
            for (size_t k = 0; k < criteria->len; k++) f->options[k] = strdup(criteria->keys[k]);
        } else if (type && !strcmp(type, "score")) {
            if (!bf_jv_is(criteria, BF_JV_ARR) || criteria->len < 2) TD("%s: score requires at least two ordered levels", id);
            f->nopt = (int)criteria->len; f->options = malloc(sizeof *f->options * criteria->len);
            for (size_t k = 0; k < criteria->len; k++) f->options[k] = dp_fmt("%zu", k);
        } else TD("%s: unknown question type", id);
        snprintf(f->type, sizeof f->type, "%s", type);
        if (!f->nopt || f->nopt > 255) TD("%s: option budget or empty option", id);
        for (int k = 0; k < f->nopt; k++) if (!*f->options[k]) TD("%s: option budget or empty option", id);
        offset += f->nopt;
    }
    s->width = offset;
    return s;
}

void dp_schema_free(DpSchema *s) {
    if (!s) return;
    for (int i = 0; i < s->n; i++) { for (int k = 0; k < s->f[i].nopt; k++) free(s->f[i].options[k]); free(s->f[i].options); free(s->f[i].id); }
    free(s->f); free(s);
}

static const DpField *field_of(const DpSchema *s, const char *id) {
    for (int i = 0; i < s->n; i++) if (!strcmp(s->f[i].id, id)) return &s->f[i];
    return NULL;
}
static int option_index(const DpField *f, const char *key) {
    for (int k = 0; key && k < f->nopt; k++) if (!strcmp(f->options[k], key)) return k;
    return -1;
}

static int cmp_desc(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (y > x) - (y < x);
}

DpDecoded dp_decode(DpSchema *s, const bf_jv *answers, int decimals) {
    if (decimals < 1 || decimals > 15) TD("invalid wire precision");
    if (!is_obj(answers) || (int)answers->len != s->n) TD("answer shape does not match the request");
    double unit = 0.5 * pow(10, -decimals);
    DpDecoded d = {s, calloc((size_t)(s->width ? s->width : 1), sizeof(double)), bf_jv_obj()};
    for (int i = 0; i < s->n; i++) {
        const DpField *f = &s->f[i];
        const bf_jv *a = bf_jv_get(answers, f->id);
        if (!is_obj(a) || !bf_jv_s(bf_jv_get(a, "type")) || strcmp(bf_jv_s(bf_jv_get(a, "type")), f->type)) TD("%s: wrong answer type", f->id);
        int n = f->nopt; double *v = d.columns + f->offset;
        if (!strcmp(f->type, "noul")) {
            const bf_jv *p = bf_jv_get(a, "noul");
            if (!is_prob(p)) TD("%s: invalid noul", f->id);
            v[0] = p->n; v[1] = 1 - p->n;
        } else {
            const bf_jv *probs = bf_jv_get(a, "probabilities");
            if (!is_obj(probs) || (int)probs->len != n) TD("%s: invalid probability distribution", f->id);
            for (int k = 0; k < n; k++) {
                const bf_jv *p = bf_jv_get(probs, f->options[k]);
                if (!is_prob(p)) TD("%s: invalid probability distribution", f->id);
                v[k] = p->n;
            }
            if (!is_prob(bf_jv_get(a, "confidence"))) TD("%s: invalid confidence", f->id);
        }
        double mass = 0; for (int k = 0; k < n; k++) mass += v[k];
        if (fabs(mass - 1) > unit * n + 1e-12) TD("%s: invalid probability distribution; probabilities do not sum to one", f->id);
        double peak = v[0]; for (int k = 1; k < n; k++) if (v[k] > peak) peak = v[k];
        int winner = 0; while (v[winner] != peak) winner++;
        double mean = 0; for (int k = 0; k < n; k++) mean += v[k] * k;
        const char *choice = bf_jv_s(bf_jv_get(a, "choice"));
        if (!strcmp(f->type, "choice")) {
            int c = option_index(f, choice);
            if (c < 0 || peak - v[c] > 2 * unit + 1e-12) TD("%s: choice is not a maximum-probability option", f->id);
        }
        if (!strcmp(f->type, "score")) {
            const bf_jv *sc = bf_jv_get(a, "score");
            if (!bf_jv_is(sc, BF_JV_NUM) || !isfinite(sc->n) || sc->n < 0 || sc->n > n - 1 ||
                fabs(sc->n - mean) > unit * (1 + n * (n - 1) / 2.0) + 1e-12)
                TD("%s: score disagrees with its probability-weighted expectation", f->id);
        }
        double *desc = malloc(sizeof *desc * (size_t)n); memcpy(desc, v, sizeof *desc * (size_t)n);
        qsort(desc, (size_t)n, sizeof *desc, cmp_desc);
        double concentration = n == 1 ? 1 : fmax(0, (peak - 1.0 / n) / (1 - 1.0 / n));
        double even = 0; for (int k = 0; k < n; k++) even += fabs(k - (n - 1) / 2.0); even /= n;
        double entropy = 0; for (int k = 0; k < n; k++) entropy += v[k] ? v[k] * log(v[k]) : 0;
        bf_jv *m = bf_jv_obj();
        bf_jv_set(m, "mass", N(mass));
        bf_jv_set(m, "maximum", N(peak));
        bf_jv_set(m, "winner", S(!strcmp(f->type, "choice") ? choice : f->options[winner]));
        bf_jv_set(m, "margin", N(peak - (n > 1 && desc[1] ? desc[1] : 0)));
        bf_jv_set(m, "expectation", N(mean));
        bf_jv_set(m, "entropy", N(-entropy));
        bf_jv_set(m, "concentration", N(concentration));
        if (!strcmp(f->type, "score")) {
            double spread = 0; for (int k = 0; k < n; k++) spread += v[k] * abs(k - winner);
            bf_jv_set(m, "ordinal_concentration", N(fmax(0, 1 - spread / even)));
        } else bf_jv_set(m, "ordinal_concentration", bf_jv_null());
        bf_jv_set(m, "provider_confidence", !strcmp(f->type, "noul") ? bf_jv_null() : N(bf_jv_get(a, "confidence")->n));
        bf_jv_set(d.moments, f->id, m);
        free(desc);
    }
    return d;
}

void dp_decoded_free(DpDecoded *d) { free(d->columns); bf_jv_free(d->moments); }

/* answersFromLogits is not needed: every Bonfyre local owner returns answers. */

typedef struct { char *name, *stat, *option; const DpField *f[8]; int nf; } DpProj;
typedef struct { DpProj *p; int n; } DpProjection;
typedef struct { char *question, *abstain; bf_jv *losses; int review_offset; double max_review; } DpSelection;
typedef struct { DpSchema *schema; DpProjection proj; DpSelection *sel; } DpPlan;

static DpProjection compile_projection(const DpSchema *s, const bf_jv *outputs) {
    DpProjection pr = {calloc(outputs && outputs->len ? outputs->len : 1, sizeof(DpProj)), 0};
    for (size_t i = 0; outputs && i < outputs->len; i++) {
        const char *name = outputs->keys[i]; const bf_jv *spec = outputs->items[i];
        DpProj *p = &pr.p[pr.n++];
        p->name = strdup(name);
        const bf_jv *qs = bf_jv_get(spec, "questions"), *q = bf_jv_get(spec, "question");
        const char *refs[8]; int nr = 0;
        if (bf_jv_is(qs, BF_JV_ARR)) for (size_t k = 0; k < qs->len && nr < 8; k++) refs[nr++] = bf_jv_s(qs->items[k]);
        else if (q) refs[nr++] = bf_jv_s(q);
        for (int k = 0; k < nr; k++) { p->f[k] = refs[k] ? field_of(s, refs[k]) : NULL; if (!p->f[k]) TD("%s: unknown question reference", name); }
        p->nf = nr;
        if (!nr) TD("%s: unknown question reference", name);
        const char *stat = bf_jv_s(bf_jv_get(spec, "stat"));
        p->stat = strdup(stat ? stat : "");
        const char *opt = bf_jv_s(bf_jv_get(spec, "option"));
        p->option = opt ? strdup(opt) : NULL;
        if (!strcmp(p->stat, "all_lower") || !strcmp(p->stat, "all_upper")) {
            for (int k = 0; k < nr; k++) {
                if (strcmp(p->f[k]->type, "noul")) TD("%s: conjunction needs distinct noul questions", name);
                for (int j = 0; j < k; j++) if (!strcmp(refs[j], refs[k])) TD("%s: conjunction needs distinct noul questions", name);
            }
        } else {
            if (nr != 1 || (strcmp(p->stat, "probability") && strcmp(p->stat, "winner") && strcmp(p->stat, "distribution") && strcmp(p->stat, "expectation")))
                TD("%s: invalid projection", name);
            if (!strcmp(p->stat, "expectation") && strcmp(p->f[0]->type, "score")) TD("%s: expectation needs score", name);
            if (!strcmp(p->stat, "probability") && option_index(p->f[0], p->option) < 0) TD("%s: unknown option", name);
        }
    }
    return pr;
}

static DpSelection *compile_selection(const DpSchema *s, const bf_jv *sel) {
    const char *question = bf_jv_s(bf_jv_get(sel, "question")), *abstain = bf_jv_s(bf_jv_get(sel, "abstain"));
    const char *review_q = bf_jv_s(bf_jv_get(sel, "review_question"));
    const bf_jv *ec = bf_jv_get(sel, "error_cost"), *rc = bf_jv_get(sel, "review_cost"), *mr = bf_jv_get(sel, "maximum_review_probability");
    const DpField *f = question ? field_of(s, question) : NULL, *rf = review_q ? field_of(s, review_q) : NULL;
    if (!f || strcmp(f->type, "choice") || option_index(f, abstain) < 0 || !rf || strcmp(rf->type, "noul") || !is_prob(mr) ||
        !bf_jv_is(ec, BF_JV_NUM) || !isfinite(ec->n) || ec->n <= 0 || !bf_jv_is(rc, BF_JV_NUM) || !isfinite(rc->n) ||
        rc->n < 0 || rc->n >= ec->n) TD("invalid authored selection costs or review question");
    DpSelection *out = calloc(1, sizeof *out);
    out->question = strdup(question); out->abstain = strdup(abstain);
    out->losses = bf_jv_obj();
    for (int a = 0; a < f->nopt; a++) {
        bf_jv *row = bf_jv_obj();
        for (int o = 0; o < f->nopt; o++)
            bf_jv_set(row, f->options[o], N(!strcmp(f->options[a], abstain) ? rc->n : a == o ? 0 : ec->n));
        bf_jv_set(out->losses, f->options[a], row);
    }
    out->review_offset = rf->offset; out->max_review = mr->n;
    return out;
}

/* The plan depends only on the questions' shape (ids, types, option keys)
 * and the program's outputs and selection, so it is cached by that shape, as
 * decision-plane.mjs caches programPlans. Cached plans are never freed. */
typedef struct { char *key; DpPlan plan; } PlanCache;
static PlanCache plan_cache[128]; static int nplan;
static char *plan_key(const bf_jv *questions, const bf_jv *program) {
    bf_re_buf b = {0}; char ptr[32];
    snprintf(ptr, sizeof ptr, "%p|", (const void *)program); bf_re_append(&b, ptr, strlen(ptr));
    for (size_t i = 0; questions && i < questions->len; i++) {
        const bf_jv *q = questions->items[i], *c = bf_jv_get(q, "criteria");
        const char *t = bf_jv_s(bf_jv_get(q, "type"));
        bf_re_append(&b, questions->keys[i], strlen(questions->keys[i])); bf_re_append(&b, "\x1f", 1);
        bf_re_append(&b, t ? t : "?", t ? strlen(t) : 1); bf_re_append(&b, "\x1f", 1);
        if (c && c->type == BF_JV_OBJ) for (size_t k = 0; k < c->len; k++) { bf_re_append(&b, c->keys[k], strlen(c->keys[k])); bf_re_append(&b, "\x1e", 1); }
        else if (c && c->type == BF_JV_ARR) { char n[16]; snprintf(n, sizeof n, "#%zu", c->len); bf_re_append(&b, n, strlen(n)); }
        else if (c) bf_re_append(&b, "!", 1);
        bf_re_append(&b, "\x1d", 1);
    }
    return b.buf;
}
static DpPlan build_plan(const bf_jv *questions, const bf_jv *program);
static DpPlan program_plan(const bf_jv *questions, const bf_jv *program) {
    char *key = plan_key(questions, program);
    for (int i = 0; i < nplan; i++) if (!strcmp(plan_cache[i].key, key)) { free(key); return plan_cache[i].plan; }
    DpPlan plan = build_plan(questions, program);
    if (nplan < 128) { plan_cache[nplan].key = key; plan_cache[nplan].plan = plan; nplan++; }
    else free(key);
    return plan;
}
static DpPlan build_plan(const bf_jv *questions, const bf_jv *program) {
    DpPlan plan = {0};
    plan.schema = dp_schema(questions);
    plan.proj = compile_projection(plan.schema, bf_jv_get(program, "outputs"));
    const bf_jv *sel = bf_jv_get(program, "selection");
    if (sel) plan.sel = compile_selection(plan.schema, sel);
    return plan;
}

static bf_jv *conjunction(const DpDecoded *d, const DpProj *p, int upper) {
    double sum = 0, min = INFINITY;
    for (int k = 0; k < p->nf; k++) { double v = d->columns[p->f[k]->offset]; sum += v; if (v < min) min = v; }
    return N(upper ? min : fmax(0, sum - p->nf + 1));
}

static void execute_projection(bf_jv *out, const DpProjection *pr, const DpDecoded *d) {
    for (int i = 0; i < pr->n; i++) {
        const DpProj *p = &pr->p[i]; const DpField *f = p->f[0];
        const double *v = d->columns + f->offset;
        bf_jv *value;
        if (!strcmp(p->stat, "probability")) value = N(v[option_index(f, p->option)]);
        else if (!strcmp(p->stat, "winner")) value = bf_jv_clone(bf_jv_get(bf_jv_get(d->moments, f->id), "winner"));
        else if (!strcmp(p->stat, "distribution")) { value = bf_jv_obj(); for (int k = 0; k < f->nopt; k++) bf_jv_set(value, f->options[k], N(v[k])); }
        else if (!strcmp(p->stat, "expectation")) value = bf_jv_clone(bf_jv_get(bf_jv_get(d->moments, f->id), "expectation"));
        else if (!strcmp(p->stat, "all_lower") || !strcmp(p->stat, "all_upper")) value = conjunction(d, p, p->stat[4] == 'u');
        else TD("uncompiled projection");
        bf_jv_set(out, p->name, value);
    }
}

static bf_jv *select_candidate(const DpDecoded *d, const DpSelection *sel) {
    const DpField *f = field_of(d->schema, sel->question);
    if (!f || !is_obj(sel->losses) || !sel->losses->len) TD("loss program requires a question and actions");
    bf_jv *risks = bf_jv_obj();
    for (size_t a = 0; a < sel->losses->len; a++) {
        const bf_jv *row = sel->losses->items[a];
        double risk = 0;
        for (int k = 0; k < f->nopt; k++) risk += d->columns[f->offset + k] * bf_jv_get(row, f->options[k])->n;
        if (!isfinite(risk)) TD("loss overflow");
        bf_jv_set(risks, sel->losses->keys[a], N(risk));
    }
    double min = INFINITY; for (size_t a = 0; a < risks->len; a++) if (risks->items[a]->n < min) min = risks->items[a]->n;
    bf_jv *minimizing = bf_jv_arr(); const char *only = NULL; int count = 0;
    for (size_t a = 0; a < risks->len; a++) if (risks->items[a]->n == min) { bf_jv_push(minimizing, S(risks->keys[a])); only = risks->keys[a]; count++; }
    int review = d->columns[sel->review_offset] >= sel->max_review, unique = count == 1;
    const char *candidate = !review && unique ? only : sel->abstain;
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "candidate", S(candidate));
    bf_jv_set(out, "disposition", S(!strcmp(candidate, sel->abstain) ? "review" : "candidate"));
    bf_jv_set(out, "reason", S(review ? "review_probability" : !unique ? "risk_tie" : !strcmp(candidate, sel->abstain) ? "abstention_cost" : "minimum_expected_loss"));
    bf_jv_set(out, "risks", risks);
    bf_jv_set(out, "minimizing_actions", minimizing);
    bf_jv_set(out, "minimum_expected_loss", N(min));
    bf_jv_set(out, "action", S("none"));
    return out;
}

/* ── case compilation (decision-plane.mjs) ──────────────────────────────── */

static bf_jv *exact_frontier(const bf_jv *frontier, const bf_jv *policy) {
    int max = (int)bf_jv_n(bf_jv_get(policy, "max_candidates"), 0);
    if (!bf_jv_is(frontier, BF_JV_ARR) || !frontier->len || (int)frontier->len > max)
        dp_fail("exact frontier must have 1..%d candidates", max);
    bf_jv *out = bf_jv_arr();
    for (size_t i = 0; i < frontier->len; i++) {
        const bf_jv *row = frontier->items[i];
        const bf_jv *id = bf_jv_get(row, "id"), *desc = bf_jv_get(row, "description"), *ref = bf_jv_get(row, "owner_ref");
        int dup = 0;
        for (size_t k = 0; nonempty_str(id) && k < out->len; k++) if (!strcmp(bf_jv_s(bf_jv_get(out->items[k], "id")), id->s)) dup = 1;
        if (!is_obj(row) || !nonempty_str(id) || !nonempty_str(desc) || !nonempty_str(ref) || dup)
            dp_fail("exact frontier has missing or duplicate candidate identity, owner, or description");
        bf_jv *r = bf_jv_obj();
        bf_jv_set(r, "id", S(id->s)); bf_jv_set(r, "description", S(desc->s)); bf_jv_set(r, "owner_ref", S(ref->s));
        bf_jv_push(out, r);
    }
    return out;
}

static char *js_trim_str(const char *s) { return dp_js_trim(s); }

typedef struct { const bf_jv *state; } FillCtx;
static void fill_one(bf_re_buf *out, const char *s, const bf_re_match *m, void *ctx) {
    char *path = bf_re_group(s, m, 1);
    const bf_jv *at = ((FillCtx *)ctx)->state;
    char part[256]; const char *p = path;
    while (at && *p) {
        size_t n = strcspn(p, "."); if (n >= sizeof part) n = sizeof part - 1;
        memcpy(part, p, n); part[n] = 0;
        at = is_obj(at) ? bf_jv_get(at, part) : NULL;
        p += n; if (*p == '.') p++;
    }
    char *trimmed = bf_jv_is(at, BF_JV_STR) ? js_trim_str(at->s) : NULL;
    if (!trimmed || !*trimmed) dp_fail("decision question names {{%s}}, which the state does not carry", path);
    bf_re_append(out, trimmed, strlen(trimmed));
    free(trimmed); free(path);
}
static bf_jv *fill_text(const bf_jv *text, const bf_jv *state) {
    static bf_re *re;
    if (!re) re = bf_re_compile("\\{\\{([a-z_]+(?:\\.[a-z_]+)*)\\}\\}", "g", NULL, 0);
    if (!bf_jv_is(text, BF_JV_STR)) return bf_jv_clone(text);
    FillCtx ctx = {state};
    char *out = bf_re_replace_fn(re, text->s, fill_one, &ctx);
    bf_jv *v = S(out); free(out); return v;
}
static void fill_questions(bf_jv *questions, const bf_jv *state) {
    for (size_t i = 0; i < questions->len; i++) {
        bf_jv *q = questions->items[i];
        if (!is_obj(q)) continue;
        bf_jv *ins = bf_jv_get(q, "instructions");
        if (ins) bf_jv_set(q, "instructions", fill_text(ins, state));
        else if (bf_jv_has(q, "instructions")) {}
        bf_jv *crit = bf_jv_get(q, "criteria");
        if (bf_jv_is(crit, BF_JV_ARR)) {
            bf_jv *c = bf_jv_arr(); for (size_t k = 0; k < crit->len; k++) bf_jv_push(c, fill_text(crit->items[k], state));
            bf_jv_set(q, "criteria", c);
        } else if (is_obj(crit)) {
            bf_jv *c = bf_jv_obj(); for (size_t k = 0; k < crit->len; k++) bf_jv_set(c, crit->keys[k], fill_text(crit->items[k], state));
            bf_jv_set(q, "criteria", c);
        }
    }
}

static int array_has(const bf_jv *arr, const char *s) {
    for (size_t i = 0; bf_jv_is(arr, BF_JV_ARR) && i < arr->len; i++) if (bf_jv_s(arr->items[i]) && !strcmp(arr->items[i]->s, s)) return 1;
    return 0;
}

static void candidate_criteria(bf_jv *questions, const bf_jv *frontier, int check_reserved) {
    for (size_t i = 0; i < questions->len; i++) {
        bf_jv *q = questions->items[i];
        const char *type = bf_jv_s(bf_jv_get(q, "type"));
        if (!type || strcmp(type, "choice") || bf_jv_get(q, "criteria")) continue;
        if (check_reserved) for (size_t k = 0; k < frontier->len; k++)
            if (!strcmp(bf_jv_s(bf_jv_get(frontier->items[k], "id")), "none")) dp_fail("reserved decision candidate: none");
        bf_jv *c = bf_jv_obj();
        for (size_t k = 0; k < frontier->len; k++)
            bf_jv_set(c, bf_jv_s(bf_jv_get(frontier->items[k], "id")), bf_jv_clone(bf_jv_get(frontier->items[k], "description")));
        bf_jv_set(c, "none", S("None of the listed candidates clearly fits."));
        bf_jv_set(q, "criteria", c);
    }
}

/* Policy programs never change in a process: digest each once. */
static void program_digest(const bf_jv *program, char out[65]) {
    static const bf_jv *keys[32]; static char hex[32][65]; static int n;
    for (int i = 0; i < n; i++) if (keys[i] == program) { memcpy(out, hex[i], 65); return; }
    dp_digest(program, out);
    if (n < 32) { keys[n] = program; memcpy(hex[n], out, 65); n++; }
}

__attribute__((unused)) static void plan_free(DpPlan *p) {
    dp_schema_free(p->schema);
    for (int i = 0; i < p->proj.n; i++) { free(p->proj.p[i].name); free(p->proj.p[i].stat); free(p->proj.p[i].option); }
    free(p->proj.p);
    if (p->sel) { free(p->sel->question); free(p->sel->abstain); bf_jv_free(p->sel->losses); free(p->sel); }
}

bf_jv *dp_compile_case(const bf_jv *input, const bf_jv *policy) {
    const char *pname = bf_jv_s(bf_jv_get(input, "program"));
    const bf_jv *program = pname ? bf_jv_get(bf_jv_get(policy, "programs"), pname) : NULL;
    if (!program) dp_fail("unknown decision program: %s", pname ? pname : "undefined");
    const char *owner = bf_jv_s(bf_jv_get(input, "owner")), *gate = bf_jv_s(bf_jv_get(input, "gate_state"));
    if (!owner || strcmp(owner, bf_jv_s(bf_jv_get(program, "owner"))) || !dp_is_hex64(bf_jv_s(bf_jv_get(input, "mask_digest"))) ||
        !gate || (strcmp(gate, "ADMIT") && strcmp(gate, "SHADOW")) || !nonempty_str(bf_jv_get(input, "subject_id")) ||
        !nonempty_str(bf_jv_get(input, "information_policy_class")))
        dp_fail("decision case requires exact owner, subject, mask digest, gate state, and information policy");
    bf_jv *frontier = exact_frontier(bf_jv_get(input, "frontier"), policy);
    const bf_jv *in_state = bf_jv_get(input, "state"), *fields = bf_jv_get(program, "state_fields");
    if (!is_obj(in_state)) dp_fail("decision state contains undeclared fields");
    for (size_t i = 0; i < in_state->len; i++) if (!array_has(fields, in_state->keys[i])) dp_fail("decision state contains undeclared fields");
    bf_jv *state = bf_jv_obj();
    for (size_t i = 0; i < fields->len; i++) {
        const char *field = fields->items[i]->s;
        if (!strcmp(field, "candidates") || !strcmp(field, "candidate_roles")) {
            bf_jv *c = bf_jv_arr();
            for (size_t k = 0; k < frontier->len; k++) {
                bf_jv *r = bf_jv_obj();
                bf_jv_set(r, "id", bf_jv_clone(bf_jv_get(frontier->items[k], "id")));
                bf_jv_set(r, "description", bf_jv_clone(bf_jv_get(frontier->items[k], "description")));
                bf_jv_push(c, r);
            }
            bf_jv_set(state, field, c);
        } else if (!bf_jv_has(in_state, field)) dp_fail("decision state missing %s", field);
        else bf_jv_set(state, field, bf_jv_clone(bf_jv_get(in_state, field)));
    }
    if (!strcmp(pname, "context_passage")) {
        const char *pid = bf_jv_s(bf_jv_path(state, "passage.id"));
        if (frontier->len != 1 || !pid || strcmp(pid, bf_jv_s(bf_jv_get(frontier->items[0], "id"))))
            dp_fail("context passage must name its single exact frontier member");
    }
    if (sorted_bytes(state) > (size_t)bf_jv_n(bf_jv_get(policy, "max_state_bytes"), 0))
        dp_fail("decision state exceeds authored byte budget; narrow the exact state first");
    bf_jv *questions = bf_jv_clone(bf_jv_get(program, "questions"));
    fill_questions(questions, state);
    candidate_criteria(questions, frontier, 1);
    program_plan(questions, program);   /* validates; cached by shape */
    bf_jv *contract = bf_jv_obj();
    bf_jv_set(contract, "schema", bf_jv_clone(bf_jv_get(policy, "schema")));
    bf_jv_set(contract, "program", S(pname));
    bf_jv_set(contract, "owner", S(owner));
    char h[65]; program_digest(program, h);
    bf_jv_set(contract, "program_digest", S(h));
    bf_jv_set(contract, "subject_id", bf_jv_clone(bf_jv_get(input, "subject_id")));
    bf_jv_set(contract, "mask_digest", bf_jv_clone(bf_jv_get(input, "mask_digest")));
    bf_jv_set(contract, "gate_state", S(gate));
    bf_jv_set(contract, "information_policy_class", bf_jv_clone(bf_jv_get(input, "information_policy_class")));
    bf_jv_set(contract, "frontier", frontier);
    {
        const char *k[3] = {"contract", "state", "questions"}; const bf_jv *v[3] = {contract, state, questions};
        bf_jv_digest_members(BF_JV_SORT_JS, 3, k, v, h);
    }
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "contract", contract);
    bf_jv_set(out, "state", state);
    bf_jv_set(out, "questions", questions);
    bf_jv_set(out, "case_digest", S(h));
    bf_jv_set(out, "cannot_infer", bf_jv_clone(bf_jv_get(policy, "forbidden_inference")));
    return out;
}

/* {...compiled.contract, state: compiled.state}: the case rebuilt from its contract. */
static bf_jv *recompile(const bf_jv *compiled, const bf_jv *policy) {
    bf_jv *input = bf_jv_clone(bf_jv_get(compiled, "contract"));
    bf_jv_set(input, "state", bf_jv_clone(bf_jv_get(compiled, "state")));
    bf_jv *c = dp_compile_case(input, policy);
    bf_jv_free(input);
    return c;
}

static bf_jv *decision_request(const bf_jv *compiled, const char *provider_name, const bf_jv *policy) {
    const bf_jv *provider = bf_jv_get(bf_jv_get(policy, "providers"), provider_name);
    if (!provider) dp_fail("unknown decision provider: %s", provider_name);
    const bf_jv *questions = bf_jv_get(compiled, "questions");
    if (questions->len > (size_t)bf_jv_n(bf_jv_get(provider, "max_questions"), 0)) dp_fail("decision program exceeds provider question budget");
    const char *pname = bf_jv_s(bf_jv_path(compiled, "contract.program"));
    const bf_jv *program = bf_jv_get(bf_jv_get(policy, "programs"), pname);
    const bf_jv *cstate = bf_jv_get(compiled, "state"), *pf = bf_jv_get(program, "prompt_fields");
    bf_jv *state;
    if (!strcmp(pname, "context_passage")) {
        state = bf_jv_obj();
        bf_jv_set(state, "purpose", bf_jv_clone(bf_jv_get(cstate, "purpose")));
        bf_jv *p = bf_jv_obj(); bf_jv_set(p, "text", bf_jv_clone(bf_jv_path(cstate, "passage.text")));
        bf_jv_set(state, "passage", p);
    } else if (bf_jv_is(pf, BF_JV_ARR)) {
        state = bf_jv_obj();
        for (size_t i = 0; i < pf->len; i++) {
            const bf_jv *v = bf_jv_get(cstate, pf->items[i]->s);
            if (v) bf_jv_set(state, pf->items[i]->s, bf_jv_clone(v));   /* undefined drops out of JSON */
        }
    } else state = bf_jv_clone(cstate);
    bf_jv *req = bf_jv_obj();
    bf_jv_set(req, "model", bf_jv_clone(bf_jv_get(provider, "model")));
    bf_jv_set(req, "state", state);
    bf_jv_set(req, "questions", bf_jv_clone(questions));
    return req;
}

/* ── local inference (local-inference.mjs) ──────────────────────────────── */

static bf_jv *snapshot_answers(const DpSchema *schema, const bf_jv *source, const bf_jv *questions) {
    bf_jv *captured = bf_jv_clone(source);
    DpDecoded d = dp_decode((DpSchema *)schema, captured, 4); dp_decoded_free(&d);
    bf_jv *out = bf_jv_obj();
    for (int i = 0; i < schema->n; i++) {
        const DpField *f = &schema->f[i];
        const bf_jv *a = bf_jv_get(captured, f->id);
        if (!strcmp(f->type, "score") && bf_jv_has(a, "legend")) {
            const bf_jv *legend = bf_jv_get(a, "legend"), *criteria = bf_jv_get(bf_jv_get(questions, f->id), "criteria");
            int ok = is_obj(legend) && legend->len == criteria->len;
            for (size_t k = 0; ok && k < criteria->len; k++) {
                char key[24]; snprintf(key, sizeof key, "%zu", k);
                ok = bf_jv_has(legend, key) && bf_jv_equal(bf_jv_get(legend, key), criteria->items[k]);
            }
            if (!ok) dp_fail("%s: score legend disagrees with the authored criteria", f->id);
        }
        bf_jv *r = bf_jv_obj();
        if (!strcmp(f->type, "noul")) { bf_jv_set(r, "type", S("noul")); bf_jv_set(r, "noul", bf_jv_clone(bf_jv_get(a, "noul"))); }
        else {
            bf_jv *probs = bf_jv_obj();
            for (int k = 0; k < f->nopt; k++) bf_jv_set(probs, f->options[k], bf_jv_clone(bf_jv_get(bf_jv_get(a, "probabilities"), f->options[k])));
            bf_jv_set(r, "type", S(f->type));
            if (!strcmp(f->type, "choice")) bf_jv_set(r, "choice", bf_jv_clone(bf_jv_get(a, "choice")));
            else bf_jv_set(r, "score", bf_jv_clone(bf_jv_get(a, "score")));
            bf_jv_set(r, "confidence", bf_jv_clone(bf_jv_get(a, "confidence")));
            bf_jv_set(r, "probabilities", probs);
        }
        bf_jv_set(out, f->id, r);
    }
    bf_jv_free(captured);
    return out;
}

static bf_jv *evaluate_local(DpOwner *local, const bf_jv *state, const bf_jv *questions, DpSchema *schema,
                             long max_entries, long max_bytes, const char *key) {
    if (max_entries < 0 || max_bytes < 0 || !dp_is_hex64(key)) dp_fail("invalid local inference residency budget or identity");
    int enabled = local->deterministic && max_entries > 0 && max_bytes > 0;
    if (enabled) {
        for (int i = 0; i < local->ncache; i++) if (!strcmp(local->cache[i].key, key)) {
            DpCacheEntry hit = local->cache[i];   /* move to most recent */
            memmove(local->cache + i, local->cache + i + 1, sizeof hit * (size_t)(local->ncache - i - 1));
            local->cache[local->ncache - 1] = hit;
            bf_jv *inference = bf_jv_obj();
            bf_jv_set(inference, "state", S("reused"));
            bf_jv_set(inference, "producer_ms", N(hit.producer_ms));
            bf_jv_set(inference, "executed", bf_jv_bool(0));
            bf_jv *out = bf_jv_obj();
            bf_jv_set(out, "answers", bf_jv_clone(hit.answers));
            bf_jv_set(out, "inference", inference);
            return out;
        }
    } else {
        for (int i = 0; i < local->ncache; i++) bf_jv_free(local->cache[i].answers);
        local->ncache = 0; local->cache_bytes = 0;
    }
    double started = dp_now_ms();
    bf_jv *value = local->evaluate(local, state, questions, schema);
    if (!is_obj(value) || !bf_jv_has(value, "answers"))
        dp_fail("local logits require an owned floating-point buffer matching the schema");
    if (value->len != 1) dp_fail("local typed result must contain only answers");
    bf_jv *owned = snapshot_answers(schema, bf_jv_get(value, "answers"), questions);
    bf_jv_free(value);
    bf_jv *answers = bf_jv_clone(owned);
    size_t retained = json_bytes(owned);
    double producer_ms = dp_now_ms() - started;
    if (enabled && retained <= (size_t)max_bytes) {
        for (int i = 0; i < local->ncache; i++) if (!strcmp(local->cache[i].key, key)) {
            local->cache_bytes -= local->cache[i].bytes; bf_jv_free(local->cache[i].answers);
            memmove(local->cache + i, local->cache + i + 1, sizeof *local->cache * (size_t)(local->ncache - i - 1));
            local->ncache--; break;
        }
        while (local->ncache >= max_entries || local->cache_bytes + retained > (size_t)max_bytes) {
            if (!local->ncache) break;
            local->cache_bytes -= local->cache[0].bytes; bf_jv_free(local->cache[0].answers);
            memmove(local->cache, local->cache + 1, sizeof *local->cache * (size_t)(local->ncache - 1));
            local->ncache--;
        }
        if (local->ncache == local->capcache) {
            local->capcache = local->capcache ? local->capcache * 2 : 16;
            local->cache = realloc(local->cache, sizeof *local->cache * (size_t)local->capcache);
        }
        DpCacheEntry *e = &local->cache[local->ncache++];
        snprintf(e->key, sizeof e->key, "%s", key);
        e->answers = owned; e->bytes = retained; e->producer_ms = producer_ms;
        local->cache_bytes += retained;
    } else bf_jv_free(owned);
    bf_jv *inference = bf_jv_obj();
    bf_jv_set(inference, "state", S(enabled ? "computed" : "uncached"));
    bf_jv_set(inference, "producer_ms", N(producer_ms));
    bf_jv_set(inference, "executed", bf_jv_bool(1));
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "answers", answers);
    bf_jv_set(out, "inference", inference);
    return out;
}

/* ── assessment ─────────────────────────────────────────────────────────── */

bf_jv *dp_proposal(const bf_jv *receipt, const bf_jv *policy) {
    const char *state = bf_jv_s(bf_jv_get(receipt, "state")), *action = bf_jv_s(bf_jv_get(receipt, "action"));
    if (!state || strcmp(state, "diagnostic") || !action || strcmp(action, "none")) dp_fail("only diagnostic receipts produce proposals");
    char h[65]; dp_digest(bf_jv_get(receipt, "answers"), h);
    if (!bf_jv_s(bf_jv_get(receipt, "answers_digest")) || strcmp(h, bf_jv_s(bf_jv_get(receipt, "answers_digest"))))
        dp_fail("decision answers changed since assessment");
    const char *pname = bf_jv_s(bf_jv_path(receipt, "contract.program"));
    const bf_jv *program = pname ? bf_jv_get(bf_jv_get(policy, "programs"), pname) : NULL;
    if (program) program_digest(program, h);
    if (!program || strcmp(h, bf_jv_s(bf_jv_path(receipt, "contract.program_digest")))) dp_fail("decision program changed since assessment");
    const bf_jv *frontier = bf_jv_path(receipt, "contract.frontier");
    bf_jv *questions = bf_jv_clone(bf_jv_get(program, "questions"));
    candidate_criteria(questions, frontier, 0);
    DpPlan plan = program_plan(questions, program);
    DpDecoded d = dp_decode(plan.schema, bf_jv_get(receipt, "answers"), (int)bf_jv_n(bf_jv_get(policy, "probability_decimals"), 4));
    bf_jv *out = bf_jv_obj();
    bf_jv_set(out, "subject_id", bf_jv_clone(bf_jv_path(receipt, "contract.subject_id")));
    const bf_jv *co = bf_jv_get(program, "candidate_output");
    if (co && !(co->type == BF_JV_BOOL && !co->b) && co->type != BF_JV_NULL)
        bf_jv_set(out, "candidate_id", bf_jv_clone(bf_jv_get(frontier->items[0], "id")));
    execute_projection(out, &plan.proj, &d);
    if (plan.sel) {
        bf_jv *selection = select_candidate(&d, plan.sel);
        bf_jv_set(out, "suggested_candidate", bf_jv_clone(bf_jv_get(selection, "candidate")));
        bf_jv_set(out, "selection", selection);
    }
    bf_jv_set(out, "uncertainty", bf_jv_clone(d.moments));
    bf_jv_set(out, "action", S("none"));
    dp_decoded_free(&d); bf_jv_free(questions);
    return out;
}

static bf_jv *assess(const bf_jv *compiled_in, DpOwner *local, const bf_jv *policy) {
    bf_jv *rebuilt = recompile(compiled_in, policy);
    char a[65], b[65];
    dp_digest(bf_jv_get(rebuilt, "questions"), a); dp_digest(bf_jv_get(compiled_in, "questions"), b);
    if (strcmp(bf_jv_s(bf_jv_get(rebuilt, "case_digest")), bf_jv_s(bf_jv_get(compiled_in, "case_digest"))) || strcmp(a, b))
        dp_fail("decision case changed after compilation");
    bf_jv *compiled = rebuilt;
    bf_jv_set(compiled, "state", dp_sorted_clone(bf_jv_get(compiled, "state")));
    const char *provider_name = bf_jv_s(bf_jv_get(policy, "default_request_provider"));
    const bf_jv *provider = bf_jv_get(bf_jv_get(policy, "providers"), provider_name);
    bf_jv *request = decision_request(compiled, provider_name, policy);
    char request_digest[65]; dp_digest(request, request_digest);
    bf_jv *base = bf_jv_obj();
    bf_jv_set(base, "schema", bf_jv_clone(bf_jv_get(policy, "schema")));
    bf_jv_set(base, "contract", bf_jv_clone(bf_jv_get(compiled, "contract")));
    bf_jv_set(base, "case_digest", bf_jv_clone(bf_jv_get(compiled, "case_digest")));
    bf_jv_set(base, "provider", S(provider_name));
    bf_jv_set(base, "request_provider", S(provider_name));
    bf_jv_set(base, "requested_model", bf_jv_clone(bf_jv_get(provider, "model")));
    bf_jv_set(base, "request_digest", S(request_digest));
    bf_jv_set(base, "cannot_infer", bf_jv_clone(bf_jv_get(policy, "forbidden_inference")));
    bf_jv_set(base, "action", S("none"));
    const char *confidence = local->confidence_semantics ? local->confidence_semantics : "maximum_option_probability";
    if (!local->evaluate || !local->model || !*local->model || !dp_is_hex64(local->artifact_digest) || !local->question_semantics ||
        (strcmp(local->question_semantics, "joint_schema") && strcmp(local->question_semantics, "independent_evaluation")) ||
        (strcmp(confidence, "maximum_option_probability") && strcmp(confidence, "vendor_distribution_statistic") && strcmp(confidence, "typesafe-uniform-adjusted")))
        dp_fail("local decision owner requires a pinned model artifact and question semantics");
    char local_digest[65];
    {
        bf_jv *m = S(local->model);
        const char *k[3] = {"model", "state", "questions"}; const bf_jv *v[3] = {m, bf_jv_get(request, "state"), bf_jv_get(request, "questions")};
        bf_jv_digest_members(BF_JV_SORT_JS, 3, k, v, local_digest);
        bf_jv_free(m);
    }
    bf_jv *receipt = base;   /* localBase */
    bf_jv_set(receipt, "provider", S("local"));
    bf_jv_set(receipt, "requested_model", S(local->model));
    bf_jv_set(receipt, "model", S(local->model));
    bf_jv_set(receipt, "model_identity_state", S("artifact_bound"));
    bf_jv_set(receipt, "model_artifact_digest", S(local->artifact_digest));
    bf_jv_set(receipt, "question_semantics", S(local->question_semantics));
    bf_jv_set(receipt, "confidence_semantics", S(confidence));
    bf_jv_set(receipt, "request_digest", S(local_digest));
    bf_jv *local_request = dp_sorted_clone(request);
    DpSchema *schema = dp_schema(bf_jv_get(local_request, "questions"));
    double started = dp_now_ms();
    const bf_jv *res = bf_jv_get(policy, "local_inference_residency");
    bf_jv *key = bf_jv_obj();
    bf_jv_set(key, "request", S(local_digest));
    bf_jv_set(key, "model", S(local->model));
    bf_jv_set(key, "artifact", S(local->artifact_digest));
    bf_jv_set(key, "semantics", S(local->question_semantics));
    bf_jv_set(key, "confidence", S(confidence));
    bf_jv_set(key, "owner", bf_jv_clone(bf_jv_path(compiled, "contract.owner")));
    bf_jv_set(key, "mask", bf_jv_clone(bf_jv_path(compiled, "contract.mask_digest")));
    bf_jv_set(key, "gate", bf_jv_clone(bf_jv_path(compiled, "contract.gate_state")));
    bf_jv_set(key, "information_policy", bf_jv_clone(bf_jv_path(compiled, "contract.information_policy_class")));
    bf_jv_set(key, "program", bf_jv_clone(bf_jv_path(compiled, "contract.program_digest")));
    char key_digest[65]; dp_digest(key, key_digest); bf_jv_free(key);
    bf_jv *result = evaluate_local(local, bf_jv_get(local_request, "state"), bf_jv_get(local_request, "questions"), schema,
                                   (long)bf_jv_n(bf_jv_get(res, "max_entries"), 0), (long)bf_jv_n(bf_jv_get(res, "max_logit_bytes"), 0), key_digest);
    const bf_jv *answers = bf_jv_get(result, "answers");
    char response_digest[65], answers_digest[65];
    {
        bf_jv *m = S(local->model), *a = S(local->artifact_digest);
        const char *k[3] = {"model", "artifact", "answers"}; const bf_jv *v[3] = {m, a, answers};
        bf_jv_digest_members(BF_JV_SORT_JS, 3, k, v, response_digest);
        bf_jv_free(m); bf_jv_free(a);
    }
    dp_digest(answers, answers_digest);
    bf_jv_set(receipt, "state", S("diagnostic"));
    bf_jv_set(receipt, "response_digest", S(response_digest));
    bf_jv_set(receipt, "latency_ms", N(round(dp_now_ms() - started)));
    bf_jv_set(receipt, "usage", bf_jv_null());
    bf_jv_set(receipt, "answers", bf_jv_clone(answers));
    bf_jv_set(receipt, "answers_digest", S(answers_digest));
    bf_jv_set(receipt, "inference", bf_jv_clone(bf_jv_get(result, "inference")));
    bf_jv_set(receipt, "proposal", dp_proposal(receipt, policy));
    bf_jv_free(result); dp_schema_free(schema); bf_jv_free(local_request); bf_jv_free(request); bf_jv_free(compiled);
    return receipt;
}

bf_jv *dp_assess_cases(bf_jv *cases, DpOwner *local, long max_calls, const bf_jv *policy) {
    if (!bf_jv_is(cases, BF_JV_ARR)) dp_fail("decision cases must be an array");
    if (1 > bf_jv_n(bf_jv_get(policy, "max_concurrency"), 0))
        dp_fail("decision concurrency must be 1..%g", bf_jv_n(bf_jv_get(policy, "max_concurrency"), 0));
    size_t n = cases->len;
    bf_jv **compiled = malloc(sizeof *compiled * (n ? n : 1));
    for (size_t i = 0; i < n; i++) {
        const bf_jv *item = cases->items[i];
        compiled[i] = bf_jv_get(item, "contract") ? recompile(item, policy) : dp_compile_case(item, policy);
        bf_jv_set(compiled[i], "state", dp_sorted_clone(bf_jv_get(compiled[i], "state")));
    }
    /* unique by case digest: first position, last item */
    int *uniq = malloc(sizeof *uniq * (n ? n : 1)); int nu = 0;
    for (size_t i = 0; i < n; i++) {
        const char *d = bf_jv_s(bf_jv_get(compiled[i], "case_digest")); int found = -1;
        for (int k = 0; k < nu; k++) if (!strcmp(bf_jv_s(bf_jv_get(compiled[uniq[k]], "case_digest")), d)) found = k;
        if (found >= 0) uniq[found] = (int)i; else uniq[nu++] = (int)i;
    }
    const char *provider_name = bf_jv_s(bf_jv_get(policy, "default_request_provider"));
    char (*gkeys)[65] = malloc(sizeof *gkeys * (size_t)(nu ? nu : 1));
    int *group_of = malloc(sizeof *group_of * (size_t)(nu ? nu : 1)), ngroups = 0;
    int *rep = malloc(sizeof *rep * (size_t)(nu ? nu : 1));
    for (int k = 0; k < nu; k++) {
        const bf_jv *item = compiled[uniq[k]];
        bf_jv *g = bf_jv_obj();
        bf_jv_set(g, "request", decision_request(item, provider_name, policy));
        bf_jv_set(g, "information_policy_class", bf_jv_clone(bf_jv_path(item, "contract.information_policy_class")));
        bf_jv_set(g, "gate_state", bf_jv_clone(bf_jv_path(item, "contract.gate_state")));
        bf_jv_set(g, "owner", bf_jv_clone(bf_jv_path(item, "contract.owner")));
        bf_jv_set(g, "mask_digest", bf_jv_clone(bf_jv_path(item, "contract.mask_digest")));
        bf_jv_set(g, "program_digest", bf_jv_clone(bf_jv_path(item, "contract.program_digest")));
        char h[65]; dp_digest(g, h); bf_jv_free(g);
        int found = -1;
        for (int j = 0; j < ngroups; j++) if (!strcmp(gkeys[j], h)) found = j;
        if (found < 0) { found = ngroups++; memcpy(gkeys[found], h, 65); rep[found] = uniq[k]; }
        group_of[k] = found;
    }
    if (max_calls < 1 || ngroups > max_calls) dp_fail("decision execution requires an explicit maximum provider-call budget");
    bf_jv **results = calloc((size_t)(nu ? nu : 1), sizeof *results);
    for (int g = 0; g < ngroups; g++) {
        bf_jv *receipt = assess(compiled[rep[g]], local, policy);
        for (int k = 0; k < nu; k++) {
            if (group_of[k] != g) continue;
            const bf_jv *item = compiled[uniq[k]];
            bf_jv *r = bf_jv_clone(receipt);
            bf_jv_set(r, "contract", bf_jv_clone(bf_jv_get(item, "contract")));
            bf_jv_set(r, "case_digest", bf_jv_clone(bf_jv_get(item, "case_digest")));
            bf_jv *proposal = dp_proposal(r, policy);
            bf_jv_set(r, "proposal", proposal);
            results[k] = r;
        }
        bf_jv_free(receipt);
    }
    bf_jv *receipts = bf_jv_arr();
    for (size_t i = 0; i < n; i++) {
        const char *d = bf_jv_s(bf_jv_get(compiled[i], "case_digest"));
        for (int k = 0; k < nu; k++) if (!strcmp(bf_jv_s(bf_jv_get(compiled[uniq[k]], "case_digest")), d)) { bf_jv_push(receipts, bf_jv_clone(results[k])); break; }
    }
    for (size_t i = 0; i < n; i++) bf_jv_free(compiled[i]);
    for (int k = 0; k < nu; k++) bf_jv_free(results[k]);
    free(compiled); free(uniq); free(gkeys); free(group_of); free(rep); free(results);
    return receipts;
}

/* ── hosted providers (decision-plane.mjs assessDecision, non-local) ─────── */

/* Port of the hosted branch of assessDecision. The bearer credential comes only
 * from the provider's credential_env and reaches curl through a 0600 config
 * file, never argv. BONFYRE_DECISION_CURL replaces the transport for tests, as
 * the JS tests replace fetch. */
bf_jv *dp_assess_hosted(const bf_jv *compiled_in, const char *provider_name, int execute,
                        const char *account_id, double timeout_ms, const bf_jv *policy) {
    const bf_jv *provider = bf_jv_get(bf_jv_get(policy, "providers"), provider_name);
    if (!provider || !strcmp(provider_name, "local")) dp_fail("unknown decision provider: %s", provider_name);
    bf_jv *rebuilt = recompile(compiled_in, policy);
    char a[65], b[65];
    dp_digest(bf_jv_get(rebuilt, "questions"), a); dp_digest(bf_jv_get(compiled_in, "questions"), b);
    if (strcmp(bf_jv_s(bf_jv_get(rebuilt, "case_digest")), bf_jv_s(bf_jv_get(compiled_in, "case_digest"))) || strcmp(a, b))
        dp_fail("decision case changed after compilation");
    bf_jv *compiled = rebuilt;
    bf_jv_set(compiled, "state", dp_sorted_clone(bf_jv_get(compiled, "state")));
    bf_jv *request = decision_request(compiled, provider_name, policy);
    char request_digest[65]; dp_digest(request, request_digest);
    bf_jv *base = bf_jv_obj();
    bf_jv_set(base, "schema", bf_jv_clone(bf_jv_get(policy, "schema")));
    bf_jv_set(base, "contract", bf_jv_clone(bf_jv_get(compiled, "contract")));
    bf_jv_set(base, "case_digest", bf_jv_clone(bf_jv_get(compiled, "case_digest")));
    bf_jv_set(base, "provider", S(provider_name));
    bf_jv_set(base, "request_provider", S(provider_name));
    bf_jv_set(base, "requested_model", bf_jv_clone(bf_jv_get(provider, "model")));
    bf_jv_set(base, "request_digest", S(request_digest));
    bf_jv_set(base, "cannot_infer", bf_jv_clone(bf_jv_get(policy, "forbidden_inference")));
    bf_jv_set(base, "action", S("none"));
    if (!execute) { bf_jv_set(base, "state", S("planned")); bf_jv_free(request); bf_jv_free(compiled); return base; }

    const char *ipc = bf_jv_s(bf_jv_path(compiled, "contract.information_policy_class"));
    const bf_jv *remote = bf_jv_get(policy, "remote_information_policy_classes"); int allowed = 0;
    for (size_t i = 0; remote && i < remote->len; i++) if (ipc && !strcmp(bf_jv_s(remote->items[i]), ipc)) allowed = 1;
    if (!allowed) dp_fail("information policy forbids a hosted decision model for this state");
    const char *cred_env = bf_jv_s(bf_jv_get(provider, "credential_env"));
    const char *key = cred_env ? getenv(cred_env) : NULL;
    if (!key || !*key) dp_fail("decision execution requires %s", cred_env ? cred_env : "a credential");
    for (const char *k = key; *k; k++) if (*k == '"' || *k == '\\' || *k == '\n' || *k == '\r') dp_fail("credential contains characters a header cannot carry");
    const char *endpoint = bf_jv_s(bf_jv_get(provider, "endpoint"));
    char *url;
    if (!strcmp(bf_jv_s(bf_jv_get(provider, "response_envelope")), "direct")) url = dp_fmt("%s", endpoint);
    else {
        const char *acct = account_id && *account_id ? account_id : getenv(bf_jv_s(bf_jv_get(provider, "account_env")));
        int ok = acct && strlen(acct) == 32;
        for (int i = 0; ok && i < 32; i++) ok = (acct[i] >= '0' && acct[i] <= '9') || ((acct[i] | 32) >= 'a' && (acct[i] | 32) <= 'f');
        if (!ok) dp_fail("decision execution requires %s", bf_jv_s(bf_jv_get(provider, "account_env")));
        const char *at = strstr(endpoint, "{account_id}");
        if (!at) dp_fail("provider endpoint names no account");
        url = dp_fmt("%.*s%s%s", (int)(at - endpoint), endpoint, acct, at + strlen("{account_id}"));
    }
    char dir[] = "/tmp/bonfyre-decide.XXXXXX";
    if (!mkdtemp(dir)) dp_fail("cannot create a request directory");
    char *body_path = dp_fmt("%s/request.json", dir), *cfg_path = dp_fmt("%s/curl.cfg", dir);
    size_t blen = 0; char *body = bf_jv_dump(request, 0, 1, &blen);
    dp_write_file(body_path, body, blen); free(body);
    {
        int fd = open(cfg_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd < 0) dp_fail("cannot write the request header");
        char *line = dp_fmt("header = \"Authorization: Bearer %s\"\n", key);
        if (write(fd, line, strlen(line)) != (ssize_t)strlen(line)) dp_fail("cannot write the request header");
        close(fd); free(line);
    }
    const char *curl = getenv("BONFYRE_DECISION_CURL"); if (!curl || !*curl) curl = "curl";
    char *cmd = dp_fmt("'%s' -sS --max-time %.0f -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' "
                       "--config '%s' --data-binary '@%s' -w '\\n%%{http_code}' '%s'", curl, timeout_ms / 1000.0 < 1 ? 1 : timeout_ms / 1000.0,
                       cfg_path, body_path, url);
    double started = dp_now_ms();
    FILE *p = popen(cmd, "r");
    size_t cap = 1 << 16, len = 0; char *out = malloc(cap);
    if (p) { size_t got; while ((got = fread(out + len, 1, cap - len - 1, p)) > 0) { len += got; if (len + 1 == cap) { if (cap > ((size_t)8 << 20)) break; cap *= 2; out = realloc(out, cap); } } }
    int status = p ? pclose(p) : -1;
    double latency = dp_now_ms() - started;
    unlink(cfg_path); unlink(body_path); rmdir(dir); free(cfg_path); free(body_path); free(cmd);
    out[len] = 0;
    char *nl = strrchr(out, '\n'); int http = nl ? atoi(nl + 1) : 0; if (nl) *nl = 0;
    if (status != 0 || !http) dp_fail("decision provider could not be reached");
    if (http < 200 || http > 299) dp_fail("decision provider HTTP %d", http);
    char err[200] = {0};
    bf_jv *envelope = bf_jv_parse(out, strlen(out), err, sizeof err); free(out);
    if (!envelope) dp_fail("decision provider returned invalid JSON: %s", err);
    const bf_jv *payload = !strcmp(bf_jv_s(bf_jv_get(provider, "response_envelope")), "direct") ? envelope
        : (bf_jv_get(envelope, "success") && bf_jv_get(envelope, "success")->type == BF_JV_BOOL && bf_jv_get(envelope, "success")->b ? bf_jv_get(envelope, "result") : NULL);
    if (!payload || !is_obj(payload)) dp_fail("decision provider returned an unsuccessful envelope");
    const char *model = bf_jv_s(bf_jv_get(payload, "model"));
    if (!model || !*model) dp_fail("decision response shape does not match the request");
    DpSchema *schema = dp_schema(bf_jv_get(request, "questions"));
    bf_jv *answers = snapshot_answers(schema, bf_jv_get(payload, "answers"), bf_jv_get(request, "questions"));
    dp_schema_free(schema);
    bf_jv *receipt = base;
    bf_jv_set(receipt, "state", S("diagnostic"));
    bf_jv_set(receipt, "model", S(model));
    char rd[65]; dp_digest(envelope, rd); bf_jv_set(receipt, "response_digest", S(rd));
    bf_jv_set(receipt, "model_identity_state", S(!strcmp(model, bf_jv_s(bf_jv_get(provider, "model"))) ? "mutable_alias" : "provider_reported_revision"));
    bf_jv_set(receipt, "question_semantics", bf_jv_clone(bf_jv_get(provider, "question_semantics")));
    bf_jv_set(receipt, "confidence_semantics", bf_jv_clone(bf_jv_get(provider, "confidence_semantics")));
    bf_jv_set(receipt, "latency_ms", bf_jv_num((double)(long long)(latency + 0.5)));
    const bf_jv *usage = bf_jv_get(payload, "usage");
    bf_jv_set(receipt, "usage", usage && usage->type != BF_JV_NULL ? bf_jv_clone(usage) : bf_jv_null());
    char ad[65]; dp_digest(answers, ad);
    bf_jv_set(receipt, "answers", answers);
    bf_jv_set(receipt, "answers_digest", S(ad));
    bf_jv_set(receipt, "proposal", dp_proposal(receipt, policy));
    bf_jv_free(envelope); bf_jv_free(request); bf_jv_free(compiled); free(url);
    return receipt;
}
