/* FormGraph -> AtomicForm compile and submit readiness.  See bf_form.h. */
#include "bf_form.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *s_or(const bf_jv *o, const char *k, const char *d) {
    const char *v = bf_jv_s(bf_jv_get(o, k)); return v ? v : d;
}

/* Booleans: true/false only; absent takes the default. */
static int b_or(const bf_jv *o, const char *k, int d) {
    bf_jv *v = bf_jv_get(o, k);
    if (!bf_jv_is(v, BF_JV_BOOL)) return d;
    char *t = bf_jv_dump(v, 0, 0, NULL); int r = t && !strcmp(t, "true"); free(t); return r;
}

static int nonempty(const char *s) { return s && *s; }

int bf_form_field_required(const bf_jv *spec, const bf_jv *values) {
    if (!b_or(spec, "base_required", 1)) return 0;
    bf_jv *cond = bf_jv_get(spec, "required_when");
    if (!cond) return 1;
    const char *field = s_or(cond, "field", ""), *equals = s_or(cond, "equals", NULL);
    const char *have = bf_jv_s(bf_jv_get(values, field));
    return equals && have && !strcmp(have, equals);
}

static bf_jv *str_array(const bf_jv *src) {
    bf_jv *out = bf_jv_arr();
    for (size_t i = 0; src && bf_jv_at(src, i); i++) { const char *s = bf_jv_s(bf_jv_at(src, i)); if (s) bf_jv_push(out, bf_jv_str(s)); }
    return out;
}

bf_jv *bf_form_compile(const bf_jv *fg, const bf_jv *values, const bf_jv *state) {
    if (!fg || !nonempty(s_or(fg, "form_id", NULL))) return NULL;
    bf_jv *a = bf_jv_obj(), *fields = bf_jv_arr(), *ev = bf_jv_arr();
    bf_jv_set(a, "form_id", bf_jv_str(s_or(fg, "form_id", "")));
    bf_jv_set(a, "title", bf_jv_str(s_or(fg, "title", "")));
    bf_jv *fs = bf_jv_get(fg, "fields");
    for (size_t i = 0; fs && bf_jv_at(fs, i); i++) {
        bf_jv *spec = bf_jv_at(fs, i), *f = bf_jv_obj();
        const char *name = s_or(spec, "name", NULL);
        if (!nonempty(name)) { bf_jv_free(f); bf_jv_free(fields); bf_jv_free(ev); bf_jv_free(a); return NULL; }
        bf_jv_set(f, "name", bf_jv_str(name));
        bf_jv_set(f, "kind", bf_jv_str(s_or(spec, "kind", "text")));
        bf_jv_set(f, "required", bf_jv_bool(bf_form_field_required(spec, values)));
        const char *v = bf_jv_s(bf_jv_get(values, name));
        bf_jv_set(f, "value", v ? bf_jv_str(v) : bf_jv_null());
        const char *actor = s_or(spec, "autofill_actor", "");
        bf_jv_set(f, "autofill_actor", *actor ? bf_jv_str(actor) : bf_jv_null());
        bf_jv_push(fields, f);
    }
    bf_jv *es = bf_jv_get(fg, "evidence"), *bound = bf_jv_get(state, "evidence");
    for (size_t i = 0; es && bf_jv_at(es, i); i++) {
        bf_jv *spec = bf_jv_at(es, i), *e = bf_jv_obj();
        const char *name = s_or(spec, "name", "");
        bf_jv_set(e, "name", bf_jv_str(name));
        bf_jv_set(e, "required", bf_jv_bool(b_or(spec, "required", 1)));
        bf_jv *b = bf_jv_get(bound, name);
        const char *dig = s_or(b, "artifact_digest", NULL), *twin = s_or(b, "from_twin", NULL);
        bf_jv_set(e, "artifact_digest", dig ? bf_jv_str(dig) : bf_jv_null());
        bf_jv_set(e, "from_twin", twin ? bf_jv_str(twin) : bf_jv_null());
        bf_jv_set(e, "match_kind", bf_jv_str(s_or(spec, "match_kind", "")));
        bf_jv_set(e, "match_name", str_array(bf_jv_get(spec, "match_name")));
        bf_jv_set(e, "match_tags", str_array(bf_jv_get(spec, "match_tags")));
        bf_jv_push(ev, e);
    }
    bf_jv_set(a, "fields", fields); bf_jv_set(a, "evidence", ev);
    bf_jv_set(a, "authority_required", str_array(bf_jv_get(fg, "authority_required")));
    bf_jv_set(a, "authorities_present", str_array(bf_jv_get(state, "authorities_present")));
    const char *gate = s_or(fg, "review_gate", "");
    bf_jv_set(a, "review_gate", *gate ? bf_jv_str(gate) : bf_jv_null());
    bf_jv_set(a, "review_passed", bf_jv_bool(b_or(state, "review_passed", 0)));
    return a;
}

int bf_form_autofill(bf_jv *atomic, sqlite3 *db) {
    sqlite3_stmt *st = NULL; int filled = 0;
    if (!atomic || !db) return -1;
    if (sqlite3_prepare_v2(db, "SELECT display_name, confidence FROM actor_nodes WHERE actor_id=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    bf_jv *fields = bf_jv_get(atomic, "fields");
    for (size_t i = 0; fields && bf_jv_at(fields, i); i++) {
        bf_jv *f = bf_jv_at(fields, i);
        const char *actor = bf_jv_s(bf_jv_get(f, "autofill_actor"));
        if (bf_jv_s(bf_jv_get(f, "value")) || !nonempty(actor)) continue;
        sqlite3_reset(st); sqlite3_bind_text(st, 1, actor, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *name = (const char *)sqlite3_column_text(st, 0), *conf = (const char *)sqlite3_column_text(st, 1);
            if (conf && !strcmp(conf, "verified") && nonempty(name)) { bf_jv_set(f, "value", bf_jv_str(name)); filled++; }
        }
    }
    sqlite3_finalize(st);
    return filled;
}

static void blocker(bf_jv *list, const char *fmt, const char *a, const char *b) {
    char buf[600]; snprintf(buf, sizeof buf, fmt, a, b ? b : ""); bf_jv_push(list, bf_jv_str(buf));
}

bf_jv *bf_form_submit_ready(const bf_jv *a) {
    if (!a) return NULL;
    bf_jv *out = bf_jv_obj(), *bl = bf_jv_arr();
    bf_jv *fields = bf_jv_get(a, "fields");
    for (size_t i = 0; fields && bf_jv_at(fields, i); i++) {
        bf_jv *f = bf_jv_at(fields, i); const char *v = bf_jv_s(bf_jv_get(f, "value"));
        if (b_or(f, "required", 1) && !nonempty(v)) {
            const char *actor = bf_jv_s(bf_jv_get(f, "autofill_actor"));
            char hint[300] = ""; if (nonempty(actor)) snprintf(hint, sizeof hint, " (autofill actor %s not verified)", actor);
            blocker(bl, "field '%s' is empty%s", bf_jv_s(bf_jv_get(f, "name")), hint);
        }
    }
    bf_jv *ev = bf_jv_get(a, "evidence");
    for (size_t i = 0; ev && bf_jv_at(ev, i); i++) {
        bf_jv *e = bf_jv_at(ev, i);
        if (b_or(e, "required", 1) && !nonempty(bf_jv_s(bf_jv_get(e, "artifact_digest"))) && !nonempty(bf_jv_s(bf_jv_get(e, "from_twin"))))
            blocker(bl, "evidence slot '%s' has no artifact%s", bf_jv_s(bf_jv_get(e, "name")), NULL);
    }
    bf_jv *need = bf_jv_get(a, "authority_required"), *have = bf_jv_get(a, "authorities_present");
    for (size_t i = 0; need && bf_jv_at(need, i); i++) {
        const char *x = bf_jv_s(bf_jv_at(need, i)); int found = 0;
        for (size_t j = 0; have && bf_jv_at(have, j); j++) if (x && !strcmp(x, bf_jv_s(bf_jv_at(have, j)))) found = 1;
        if (!found) blocker(bl, "authority '%s' not present%s", x, NULL);
    }
    const char *gate = bf_jv_s(bf_jv_get(a, "review_gate"));
    if (nonempty(gate) && !b_or(a, "review_passed", 0)) blocker(bl, "review gate '%s' not passed%s", gate, NULL);
    size_t n = 0; while (bf_jv_at(bl, n)) n++;
    bf_jv_set(out, "form_id", bf_jv_str(s_or(a, "form_id", "")));
    bf_jv_set(out, "ready", bf_jv_bool(n == 0));
    bf_jv_set(out, "blockers", bl);
    return out;
}

/* ---- EvidenceBindingGraph ---- */

static size_t arr_len(const bf_jv *a) { size_t n = 0; while (a && bf_jv_at(a, n)) n++; return n; }

static int lower_contains(const char *hay, const char *needle) {
    size_t n = strlen(needle);
    if (!n) return 1;
    for (const char *p = hay; *p; p++) {
        size_t i = 0;
        while (i < n && p[i] && (p[i] | 32) == (needle[i] | 32)) i++;
        if (i == n) return 1;
    }
    return 0;
}

/* Matchers satisfied (all must be), with the witness; -1 when not a match. */
static int slot_match(const bf_jv *slot, const bf_jv *cand, char *witness, size_t cap) {
    const char *kind = s_or(slot, "match_kind", "");
    bf_jv *names = bf_jv_get(slot, "match_name"), *tags = bf_jv_get(slot, "match_tags");
    int count = 0; size_t w = 0; witness[0] = 0;
    if (!*kind && !arr_len(names) && !arr_len(tags)) return -1;
    if (*kind) {
        if (strcmp(s_or(cand, "kind", ""), kind)) return -1;
        w += (size_t)snprintf(witness + w, w < cap ? cap - w : 0, "kind=%s", kind); count++;
    }
    const char *cname = s_or(cand, "name", "");
    for (size_t i = 0; i < arr_len(names); i++) {
        const char *tok = bf_jv_s(bf_jv_at(names, i));
        if (!tok || !lower_contains(cname, tok)) return -1;
        w += (size_t)snprintf(witness + w, w < cap ? cap - w : 0, "%sname~%s", w ? "; " : "", tok); count++;
    }
    bf_jv *ctags = bf_jv_get(cand, "tags");
    for (size_t i = 0; i < arr_len(tags); i++) {
        const char *tag = bf_jv_s(bf_jv_at(tags, i)); int found = 0;
        for (size_t j = 0; tag && j < arr_len(ctags); j++) if (!strcmp(tag, bf_jv_s(bf_jv_at(ctags, j)) ? bf_jv_s(bf_jv_at(ctags, j)) : "")) found = 1;
        if (!found) return -1;
        w += (size_t)snprintf(witness + w, w < cap ? cap - w : 0, "%stag=%s", w ? "; " : "", tag); count++;
    }
    return count;
}

static int constraint(const bf_jv *slot) {
    return (int)arr_len(bf_jv_get(slot, "match_name")) + (int)arr_len(bf_jv_get(slot, "match_tags")) + (*s_or(slot, "match_kind", "") ? 1 : 0);
}

bf_jv *bf_form_bind_evidence(bf_jv *atomic, const bf_jv *candidates) {
    bf_jv *slots = bf_jv_get(atomic, "evidence");
    if (!atomic || !slots) return NULL;
    size_t ns = arr_len(slots), nc = arr_len(candidates);
    size_t *order = calloc(ns ? ns : 1, sizeof *order); char *used = calloc(nc ? nc : 1, 1);
    if (!order || !used) { free(order); free(used); return NULL; }
    for (size_t i = 0; i < ns; i++) order[i] = i;
    for (size_t i = 1; i < ns; i++) { /* stable: most constrained first */
        size_t t = order[i], j = i;
        while (j > 0 && constraint(bf_jv_at(slots, order[j - 1])) < constraint(bf_jv_at(slots, t))) { order[j] = order[j - 1]; j--; }
        order[j] = t;
    }
    bf_jv *out = bf_jv_obj(), *bound = bf_jv_obj(), *unbound = bf_jv_arr();
    for (size_t k = 0; k < ns; k++) {
        bf_jv *slot = bf_jv_at(slots, order[k]);
        const char *sname = s_or(slot, "name", "");
        if (nonempty(bf_jv_s(bf_jv_get(slot, "artifact_digest"))) || nonempty(bf_jv_s(bf_jv_get(slot, "from_twin")))) continue; /* already held */
        int best = -1; size_t bi = 0; char bw[512] = "", w[512];
        for (size_t c = 0; c < nc; c++) {
            if (used[c]) continue;
            const bf_jv *cand = bf_jv_at(candidates, c);
            int sc = slot_match(slot, cand, w, sizeof w);
            if (sc < 0 || !nonempty(s_or(cand, "digest", NULL))) continue;
            if (sc > best || (sc == best && strcmp(s_or(cand, "digest", ""), s_or(bf_jv_at(candidates, bi), "digest", "")) < 0)) {
                best = sc; bi = c; snprintf(bw, sizeof bw, "%s", w);
            }
        }
        if (best < 0) { if (constraint(slot) || b_or(slot, "required", 1)) bf_jv_push(unbound, bf_jv_str(sname)); continue; }
        used[bi] = 1;
        const char *dig = s_or(bf_jv_at(candidates, bi), "digest", "");
        bf_jv_set(slot, "artifact_digest", bf_jv_str(dig));
        bf_jv_set(slot, "binding_witness", bf_jv_str(bw));
        bf_jv *b = bf_jv_obj(); bf_jv_set(b, "digest", bf_jv_str(dig)); bf_jv_set(b, "witness", bf_jv_str(bw));
        bf_jv_set(bound, sname, b);
    }
    free(order); free(used);
    bf_jv_set(out, "bound", bound); bf_jv_set(out, "unbound", unbound);
    return out;
}
