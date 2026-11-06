/*
 * monitor.c — bonfyre monitor: what the sources this estate already reads
 * said about one business. Port of monitor.mjs and cmd/BonfyreMonitor.
 * Exact candidates come from the item archive (a whole-word match inside the
 * window); the local model answers mention_review for each story group.
 */
#include "dp.h"
#include "usage.h"

#include <math.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#define ATTENTION_CUT 0.9
static const char *KIND_ORDER[7] = {"regulatory", "review", "news", "competitor", "event", "promotion", "other"};
static const char *KIND_HEADING[7] = {"Courts, permits and regulators", "Reviews", "News", "Competitors and the market", "Events", "Sales and deals", "Passing mentions"};

static double js_round(double x) { double r = floor(x); return x - r >= 0.5 ? r + 1 : r; }
static int kind_index(const char *k) { for (int i = 0; i < 7; i++) if (k && !strcmp(KIND_ORDER[i], k)) return i; return -1; }

static char *plain(const char *text) {
    static bf_re *tags, *ents, *ws;
    if (!tags) { tags = bf_re_compile("<[^>]+>", "g", 0, 0); ents = bf_re_compile("&[a-z#0-9]+;", "gi", 0, 0); ws = bf_re_compile("\\s+", "g", 0, 0); }
    char *a = bf_re_replace(tags, text ? text : "", " "), *b = bf_re_replace(ents, a, " "), *c = bf_re_replace(ws, b, " ");
    char *t = dp_js_trim(c);
    free(a); free(b); free(c);
    return t;
}
static char *lower(const char *s) { char *o = strdup(s); for (char *p = o; *p; p++) if (*p >= 'A' && *p <= 'Z') *p += 32; return o; }

/* String.prototype.slice(0, n) counted in UTF-16 units. */
static char *slice16(const char *s, size_t n) {
    size_t units = 0; const unsigned char *p = (const unsigned char *)s;
    while (*p && units < n) {
        int len = *p < 0x80 ? 1 : (*p >> 5) == 6 ? 2 : (*p >> 4) == 14 ? 3 : 4;
        int u = len == 4 ? 2 : 1;
        if (units + (size_t)u > n) break;
        units += (size_t)u; p += len;
    }
    size_t bytes = (size_t)((const char *)p - s);
    char *o = malloc(bytes + 1); memcpy(o, s, bytes); o[bytes] = 0;
    return o;
}

static char *iso_date(double at) {
    time_t t = (time_t)floor(at); struct tm tm; gmtime_r(&t, &tm);
    return dp_fmt("%04d-%02d-%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

static bf_jv *column(sqlite3_stmt *q, int i) {
    switch (sqlite3_column_type(q, i)) {
    case SQLITE_INTEGER: return bf_jv_num((double)sqlite3_column_int64(q, i));
    case SQLITE_FLOAT: return bf_jv_num(sqlite3_column_double(q, i));
    case SQLITE_NULL: return bf_jv_null();
    default: return bf_jv_str((const char *)sqlite3_column_text(q, i));
    }
}
/* `${value}` in a JS template */
static char *tmpl(const bf_jv *v) {
    if (!v) return strdup("undefined");
    char b[32];
    switch (v->type) {
    case BF_JV_STR: return strdup(v->s);
    case BF_JV_NUM: bf_js_number(v->n, b); return strdup(b);
    case BF_JV_NULL: return strdup("null");
    case BF_JV_BOOL: return strdup(v->b ? "true" : "false");
    default: return strdup("[object Object]");
    }
}
/* value || '' */
static bf_jv *or_empty(const bf_jv *v) {
    if (!v || v->type == BF_JV_NULL || (v->type == BF_JV_STR && !v->len) || (v->type == BF_JV_NUM && (v->n == 0 || v->n != v->n)) || (v->type == BF_JV_BOOL && !v->b)) return bf_jv_str("");
    return bf_jv_clone(v);
}

static char *escape_like(const char *s) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (; *s; s++) { if (*s == '\\' || *s == '%' || *s == '_') bf_re_append(&b, "\\", 1); bf_re_append(&b, s, 1); }
    return b.buf;
}
static char *escape_re(const char *s) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (; *s; s++) { if (strchr(".*+?^${}()|[]\\", *s)) bf_re_append(&b, "\\", 1); bf_re_append(&b, s, 1); }
    return b.buf;
}

typedef struct { bf_jv *rows; int truncated; long dropped; } Candidates;

static Candidates mention_candidates(sqlite3 *db, char **names, int nnames, double since_seconds, double now, long limit) {
    if (!nnames) dp_fail("watch at least one name");
    double since = floor(now - since_seconds); long scan = limit * 4;
    bf_jv *groups = bf_jv_obj();   /* identity -> group, insertion order */
    char **seen = NULL; int nseen = 0, truncated = 0;
    static bf_re *nonalnum;
    if (!nonalnum) nonalnum = bf_re_compile("[^a-z0-9]+", "g", 0, 0);
    for (int ni = 0; ni < nnames; ni++) {
        char *ln = lower(names[ni]), *el = escape_like(ln), *like = dp_fmt("%%%s%%", el);
        sqlite3_stmt *q;
        if (sqlite3_prepare_v2(db, "SELECT source, key, kind, family, title, body, href, publisher, at FROM item\n"
                                   "        WHERE at BETWEEN ? AND ? AND (lower(title) LIKE ? ESCAPE '\\' OR lower(body) LIKE ? ESCAPE '\\')\n"
                                   "        ORDER BY at DESC LIMIT ?", -1, &q, 0) != SQLITE_OK)
            dp_fail("%s", sqlite3_errmsg(db));
        sqlite3_bind_double(q, 1, since); sqlite3_bind_double(q, 2, ceil(now + 86400));
        sqlite3_bind_text(q, 3, like, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 4, like, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(q, 5, scan);
        char *er = escape_re(ln), *pat = dp_fmt("(^|[^a-z0-9])%s([^a-z0-9]|$)", er);
        bf_re *whole = bf_re_compile(pat, "", 0, 0);
        long found = 0;
        static const char *cols[9] = {"source", "key", "kind", "family", "title", "body", "href", "publisher", "at"};
        while (sqlite3_step(q) == SQLITE_ROW) {
            found++;
            bf_jv *row = bf_jv_obj();
            for (int c = 0; c < 9; c++) bf_jv_set(row, cols[c], column(q, c));
            char *src = tmpl(bf_jv_get(row, "source")), *key = tmpl(bf_jv_get(row, "key"));
            char *sk = dp_fmt("%s%c%s", src, 1, key);
            int dup = 0; for (int k = 0; k < nseen; k++) if (!strcmp(seen[k], sk)) dup = 1;
            char *title = tmpl(bf_jv_get(row, "title")), *pb = plain(bf_jv_s(bf_jv_get(row, "body")) ? bf_jv_s(bf_jv_get(row, "body")) : ""),
                 *both = dp_fmt("%s %s", title, pb), *lb = lower(both);
            if (!dup && whole && bf_re_test(whole, lb)) {
                seen = realloc(seen, sizeof *seen * (size_t)(nseen + 1)); seen[nseen++] = strdup(sk);
                const bf_jv *tv = bf_jv_get(row, "title");
                char *pt = plain(bf_jv_s(tv) ? tv->s : ""), *ptl = lower(pt), *h1 = bf_re_replace(nonalnum, ptl, " "), *headline = dp_js_trim(h1);
                char *identity = *headline ? strdup(headline) : dp_fmt("%s:%s", src, key);
                bf_jv *group = bf_jv_get(groups, identity);
                if (!group) {
                    group = bf_jv_clone(row);
                    bf_jv_set(group, "matched", bf_jv_str(names[ni]));
                    bf_jv_set(group, "observations", bf_jv_arr());
                    bf_jv_set(groups, identity, group);
                    group = bf_jv_get(groups, identity);
                }
                bf_jv *ob = bf_jv_obj();
                bf_jv_set(ob, "source", bf_jv_clone(bf_jv_get(row, "source")));
                bf_jv_set(ob, "key", bf_jv_clone(bf_jv_get(row, "key")));
                bf_jv_set(ob, "publisher", or_empty(bf_jv_get(row, "publisher")));
                bf_jv_set(ob, "href", or_empty(bf_jv_get(row, "href")));
                bf_jv_set(ob, "at", bf_jv_clone(bf_jv_get(row, "at")));
                bf_jv_push(bf_jv_get(group, "observations"), ob);
                free(pt); free(ptl); free(h1); free(headline); free(identity);
            }
            free(src); free(key); free(sk); free(title); free(pb); free(both); free(lb);
            bf_jv_free(row);
        }
        sqlite3_finalize(q);
        if (found == scan) truncated = 1;
        bf_re_free(whole);
        free(ln); free(el); free(like); free(er); free(pat);
    }
    /* toSorted((a, b) => b.at - a.at), stable */
    int n = (int)groups->len; int *ord = malloc(sizeof(int) * (size_t)(n ? n : 1));
    for (int i = 0; i < n; i++) ord[i] = i;
    for (int a = 1; a < n; a++) for (int b = a; b > 0; b--) {
        double x = bf_jv_n(bf_jv_get(groups->items[ord[b - 1]], "at"), 0), y = bf_jv_n(bf_jv_get(groups->items[ord[b]], "at"), 0);
        if (!(y - x > 0)) break;
        int t = ord[b]; ord[b] = ord[b - 1]; ord[b - 1] = t;
    }
    Candidates c = {bf_jv_arr(), truncated, n > limit ? n - limit : 0};
    for (int i = 0; i < n && i < limit; i++) bf_jv_push(c.rows, bf_jv_clone(groups->items[ord[i]]));
    bf_jv_free(groups); free(ord);
    return c;
}

static bf_jv *mention_case(const bf_jv *row, const bf_jv *watch, const char *archive, const bf_jv *policy) {
    const bf_jv *tv = bf_jv_get(row, "title"), *bv = bf_jv_get(row, "body"), *at = bf_jv_get(row, "at");
    char *pt = plain(bf_jv_s(tv) ? tv->s : ""), *pb = plain(bf_jv_s(bv) ? bv->s : "");
    bf_jv *item = bf_jv_obj();
    bf_jv_set(item, "title", bf_jv_str(slice16(pt, 300)));
    bf_jv_set(item, "excerpt", bf_jv_str(slice16(pb, 1200)));
    bf_jv_set(item, "publisher", or_empty(bf_jv_get(row, "publisher")));
    if (at && at->type == BF_JV_NUM && at->n) { char *d = iso_date(at->n); bf_jv_set(item, "published", bf_jv_str(d)); free(d); }
    else bf_jv_set(item, "published", bf_jv_str(""));
    char *src = tmpl(bf_jv_get(row, "source")), *key = tmpl(bf_jv_get(row, "key")), *full = dp_fmt("%s:%s", src, key), *id = slice16(full, 200);
    bf_jv *m = bf_jv_obj(), *pair = bf_jv_arr();
    bf_jv_set(m, "archive", bf_jv_str(archive));
    bf_jv_set(m, "watch", bf_jv_clone(watch));
    bf_jv_push(pair, bf_jv_clone(bf_jv_get(row, "source"))); bf_jv_push(pair, bf_jv_clone(bf_jv_get(row, "key")));
    bf_jv_set(m, "item", pair);
    bf_jv_set(m, "text", bf_jv_clone(item));
    char *mask = dp_digest_str(m); bf_jv_free(m);
    bf_jv *in = bf_jv_obj();
    bf_jv_set(in, "program", bf_jv_str("mention_review"));
    bf_jv_set(in, "owner", bf_jv_str("ItemArchive"));
    char *subject = dp_fmt("item/%s", id); bf_jv_set(in, "subject_id", bf_jv_str(subject));
    bf_jv_set(in, "mask_digest", bf_jv_str(mask));
    bf_jv_set(in, "gate_state", bf_jv_str("ADMIT"));
    bf_jv_set(in, "information_policy_class", bf_jv_str("public"));
    bf_jv *fr = bf_jv_arr(), *f = bf_jv_obj();
    bf_jv_set(f, "id", bf_jv_str(id));
    const char *title = bf_jv_s(bf_jv_get(item, "title"));
    bf_jv_set(f, "description", bf_jv_str(*title ? title : id));
    char *ref = dp_fmt("ItemArchive:%s", id); bf_jv_set(f, "owner_ref", bf_jv_str(ref));
    bf_jv_push(fr, f);
    bf_jv_set(in, "frontier", fr);
    bf_jv *state = bf_jv_obj(); bf_jv_set(state, "watch", bf_jv_clone(watch)); bf_jv_set(state, "item", item);
    bf_jv_set(in, "state", state);
    bf_jv *c = dp_compile_case(in, policy);
    bf_jv_free(in); free(pt); free(pb); free(src); free(key); free(full); free(id); free(mask); free(subject); free(ref);
    return c;
}

static double now_seconds(void) { struct timeval tv; gettimeofday(&tv, 0); return (double)((long long)tv.tv_sec * 1000 + tv.tv_usec / 1000) / 1000; }

static bf_jv *run_monitor(const char *archive, char **names, int nnames, const char *about, double days, long limit, DpLocal *local) {
    char *trimmed = dp_js_trim(about);
    if (!*trimmed) dp_fail("say what the business is with --about, so a namesake can be told apart");
    struct stat st;
    if (stat(archive, &st)) dp_fail("no item archive at %s; set BONFYRE_ITEM_ARCHIVE_DB", archive);
    sqlite3 *db;
    char *uri = dp_fmt("file:%s?mode=ro", archive);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, 0) != SQLITE_OK) dp_fail("%s", sqlite3_errmsg(db));
    free(uri);
    Candidates c = mention_candidates(db, names, nnames, days * 86400, now_seconds(), limit);
    sqlite3_close(db);
    bf_jv *watch = bf_jv_obj(), *aka = bf_jv_arr();
    bf_jv_set(watch, "name", bf_jv_str(names[0]));
    for (int i = 1; i < nnames; i++) bf_jv_push(aka, bf_jv_str(names[i]));
    bf_jv_set(watch, "also_known_as", aka);
    bf_jv_set(watch, "about", bf_jv_str(trimmed));
    const bf_jv *policy = dp_policy();
    bf_jv *cases = bf_jv_arr();
    for (size_t i = 0; i < c.rows->len; i++) bf_jv_push(cases, mention_case(c.rows->items[i], watch, archive, policy));
    double started = dp_now_ms();
    DpOwner *model = cases->len ? dp_local_acquire(local) : NULL;
    bf_jv *receipts = cases->len ? dp_assess_cases(cases, model, (long)cases->len, policy) : bf_jv_arr();
    size_t n = c.rows->len;
    bf_jv **mentions = malloc(sizeof *mentions * (n ? n : 1));
    long observations = 0;
    for (size_t i = 0; i < n; i++) {
        const bf_jv *row = c.rows->items[i], *answers = bf_jv_get(receipts->items[i], "answers");
        const bf_jv *obs = bf_jv_get(row, "observations");
        observations += (long)obs->len;
        bf_jv *m = bf_jv_obj();
        const bf_jv *tv = bf_jv_get(row, "title");
        char *pt = plain(bf_jv_s(tv) ? tv->s : "");
        bf_jv_set(m, "title", bf_jv_str(pt)); free(pt);
        bf_jv_set(m, "link", bf_jv_clone(bf_jv_get(row, "href")));
        bf_jv_set(m, "publisher", bf_jv_clone(bf_jv_get(row, "publisher")));
        const bf_jv *at = bf_jv_get(row, "at");
        if (at && at->type == BF_JV_NUM && at->n) { char *d = iso_date(at->n); bf_jv_set(m, "published", bf_jv_str(d)); free(d); } else bf_jv_set(m, "published", bf_jv_str(""));
        bf_jv_set(m, "matched", bf_jv_clone(bf_jv_get(row, "matched")));
        bf_jv_set(m, "observations", bf_jv_clone(obs));
        bf_jv *pubs = bf_jv_arr();
        for (size_t k = 0; k < obs->len; k++) {
            const char *p = bf_jv_s(bf_jv_get(obs->items[k], "publisher"));
            if (!p || !*p) continue;
            int dup = 0; for (size_t j = 0; j < pubs->len; j++) if (!strcmp(pubs->items[j]->s, p)) dup = 1;
            if (!dup) bf_jv_push(pubs, bf_jv_str(p));
        }
        bf_jv_set(m, "publishers", pubs);
        double tone = bf_jv_path(answers, "tone.score")->n;
        static const char *TONES[3] = {"negative", "neutral", "positive"};
        int ti = (int)js_round(tone);
        bf_jv_set(m, "about", bf_jv_clone(bf_jv_path(answers, "about_subject.probabilities.business")));
        bf_jv_set(m, "kind", bf_jv_clone(bf_jv_path(answers, "kind.choice")));
        bf_jv_set(m, "kind_confidence", bf_jv_clone(bf_jv_path(answers, "kind.confidence")));
        if (ti >= 0 && ti < 3) bf_jv_set(m, "tone", bf_jv_str(TONES[ti]));
        bf_jv_set(m, "tone_score", bf_jv_num(tone));
        bf_jv_set(m, "action", bf_jv_clone(bf_jv_path(answers, "action.choice")));
        bf_jv_set(m, "action_confidence", bf_jv_clone(bf_jv_path(answers, "action.confidence")));
        bf_jv_set(m, "attention", bf_jv_num(1 - bf_jv_path(answers, "action.probabilities.awareness")->n));
        bf_jv_set(m, "receipt", bf_jv_clone(bf_jv_get(receipts->items[i], "response_digest")));
        mentions[i] = m;
    }
    bf_jv *warnings = bf_jv_arr();
    if (c.truncated) { char *w = dp_fmt("the archive has more matches than the %ld newest read for one name; older ones in the window were not read (raise --limit or shorten --days)", limit * 4); bf_jv_push(warnings, bf_jv_str(w)); free(w); }
    if (c.dropped) { char *w = dp_fmt("%ld older stories past --limit %ld were not judged", c.dropped, limit); bf_jv_push(warnings, bf_jv_str(w)); free(w); }
    int about_n = 0, aside_n = 0, act_n = 0;
    for (size_t i = 0; i < n; i++) {
        double a = bf_jv_get(mentions[i], "about")->n, att = bf_jv_get(mentions[i], "attention")->n;
        if (a >= 0.5) about_n++; if (a < 0.5) aside_n++;
        if (a >= 0.5 && att >= ATTENTION_CUT) act_n++;
    }
    /* stable sort: about first, attention desc, kind order, tone asc */
    int *ord = malloc(sizeof(int) * (n ? n : 1));
    for (size_t i = 0; i < n; i++) ord[i] = (int)i;
    for (size_t a = 1; a < n; a++) for (size_t b = a; b > 0; b--) {
        const bf_jv *x = mentions[ord[b - 1]], *y = mentions[ord[b]];
        double c1 = (double)(bf_jv_get(x, "about")->n >= 0.5) - (double)(bf_jv_get(y, "about")->n >= 0.5);
        double r = c1 ? c1 : bf_jv_get(x, "attention")->n - bf_jv_get(y, "attention")->n;
        if (!r) r = kind_index(bf_jv_s(bf_jv_get(y, "kind"))) - kind_index(bf_jv_s(bf_jv_get(x, "kind")));
        if (!r) r = bf_jv_get(y, "tone_score")->n - bf_jv_get(x, "tone_score")->n;
        if (!(r < 0)) break;   /* comparator(x, y) > 0 means y goes first */
        int t = ord[b]; ord[b] = ord[b - 1]; ord[b - 1] = t;
    }
    bf_jv *report = bf_jv_obj();
    bf_jv_set(report, "schema", bf_jv_str("bonfyre.monitor.v2"));
    bf_jv_set(report, "watch", watch);
    bf_jv_set(report, "window_days", bf_jv_num(days));
    bf_jv_set(report, "archive", bf_jv_str(archive));
    bf_jv_set(report, "candidates", bf_jv_num((double)n));
    bf_jv_set(report, "observations", bf_jv_num((double)observations));
    bf_jv_set(report, "warnings", warnings);
    bf_jv_set(report, "about_the_business", bf_jv_num(about_n));
    bf_jv_set(report, "namesakes_set_aside", bf_jv_num(aside_n));
    bf_jv_set(report, "model", model ? bf_jv_str(model->model) : bf_jv_null());
    bf_jv_set(report, "model_artifact_sha256", model ? bf_jv_str(model->artifact_digest) : bf_jv_null());
    bf_jv_set(report, "seconds", bf_jv_num(js_round(dp_now_ms() - started) / 1000));
    bf_jv_set(report, "attention_cut", bf_jv_num(ATTENTION_CUT));
    bf_jv_set(report, "to_act_on", bf_jv_num(act_n));
    bf_jv *ms = bf_jv_arr(); for (size_t i = 0; i < n; i++) bf_jv_push(ms, mentions[ord[i]]);
    bf_jv_set(report, "mentions", ms);
    bf_jv_set(report, "receipts", receipts);
    bf_jv_set(report, "cannot_infer", bf_jv_str("a mention found in the sources this estate reads -> every mention that exists; an item judged about the business -> a verified fact about it; one story group -> one independent report"));
    free(ord); free(mentions); free(trimmed);
    return report;
}

static char *byline(const bf_jv *item) {
    const char *pub = bf_jv_s(bf_jv_get(item, "publisher"));
    char *p = tmpl(bf_jv_get(item, "publisher"));
    const bf_jv *pubs = bf_jv_get(item, "publishers");
    int others = 0;
    for (size_t i = 0; pubs && i < pubs->len; i++) if (!pub || strcmp(pubs->items[i]->s, pub)) others++;
    char *out = others ? dp_fmt("%s and %d more", p, others) : strdup(p);
    free(p);
    return out;
}

static char *digest_markdown(const bf_jv *report) {
    bf_re_buf b = {0};
#define LINE(...) do { char *_l = dp_fmt(__VA_ARGS__); bf_re_append(&b, _l, strlen(_l)); bf_re_append(&b, "\n", 1); free(_l); } while (0)
    const bf_jv *ms = bf_jv_get(report, "mentions");
    const char *name = bf_jv_s(bf_jv_path(report, "watch.name")), *about = bf_jv_s(bf_jv_path(report, "watch.about"));
    char days[32], cands[32]; bf_js_number(bf_jv_get(report, "window_days")->n, days); bf_js_number(bf_jv_get(report, "candidates")->n, cands);
    int kept = 0, aside = 0;
    for (size_t i = 0; i < ms->len; i++) { if (bf_jv_get(ms->items[i], "about")->n >= 0.5) kept++; else aside++; }
    LINE("# %s: the last %s days", name, days);
    LINE("%s", "");
    LINE("%d items about %s; %d set aside because \"%s\" meant something else there; %s matched the name.", kept, about, aside, name, cands);
    LINE("%s", "");
    double cut = bf_jv_get(report, "attention_cut")->n;
    int any = 0;
    for (size_t i = 0; i < ms->len; i++) { const bf_jv *m = ms->items[i]; if (bf_jv_get(m, "about")->n >= 0.5 && bf_jv_get(m, "attention")->n >= cut) any = 1; }
    if (any) {
        LINE("## Act on"); LINE("%s", "");
        for (size_t i = 0; i < ms->len; i++) {
            const bf_jv *m = ms->items[i];
            if (!(bf_jv_get(m, "about")->n >= 0.5 && bf_jv_get(m, "attention")->n >= cut)) continue;
            const char *a = bf_jv_s(bf_jv_get(m, "action"));
            const char *verb = !strcmp(a, "respond") ? "Answer" : !strcmp(a, "risk") ? "Risk" : !strcmp(a, "opportunity") ? "Opening" : !strcmp(a, "awareness") ? "Look" : "undefined";
            char *by = byline(m), *link = tmpl(bf_jv_get(m, "link"));
            const bf_jv *lv = bf_jv_get(m, "link");
            int has_link = lv && lv->type == BF_JV_STR && lv->len;
            LINE("- **%s:** %s (%s, %s)%s%s%s", verb, bf_jv_s(bf_jv_get(m, "title")), by, bf_jv_s(bf_jv_get(m, "published")),
                 has_link ? " [Open](" : "", has_link ? link : "", has_link ? ")" : "");
            free(by); free(link);
        }
        LINE("%s", "");
    }
    for (int k = 0; k < 7; k++) {
        int count = 0;
        for (size_t i = 0; i < ms->len; i++) { const bf_jv *m = ms->items[i]; if (bf_jv_get(m, "about")->n >= 0.5 && !strcmp(bf_jv_s(bf_jv_get(m, "kind")), KIND_ORDER[k]) && bf_jv_get(m, "attention")->n < cut) count++; }
        if (!count) continue;
        LINE("## %s", KIND_HEADING[k]); LINE("%s", "");
        for (size_t i = 0; i < ms->len; i++) {
            const bf_jv *m = ms->items[i];
            if (!(bf_jv_get(m, "about")->n >= 0.5 && !strcmp(bf_jv_s(bf_jv_get(m, "kind")), KIND_ORDER[k]) && bf_jv_get(m, "attention")->n < cut)) continue;
            const char *tone = bf_jv_s(bf_jv_get(m, "tone")), *title = bf_jv_s(bf_jv_get(m, "title"));
            char *t = tone && !strcmp(tone, "negative") ? dp_fmt("**%s**", title) : strdup(title);
            char *by = byline(m), *link = tmpl(bf_jv_get(m, "link"));
            const bf_jv *lv = bf_jv_get(m, "link");
            int has_link = lv && lv->type == BF_JV_STR && lv->len;
            LINE("- %s (%s, %s; %s)%s%s%s", t, by, bf_jv_s(bf_jv_get(m, "published")), tone ? tone : "undefined",
                 has_link ? " [Open](" : "", has_link ? link : "", has_link ? ")" : "");
            free(t); free(by); free(link);
        }
        LINE("%s", "");
    }
    if (aside) {
        LINE("## Set aside: \"%s\" meant something else", name); LINE("%s", "");
        for (size_t i = 0; i < ms->len; i++) {
            const bf_jv *m = ms->items[i];
            if (bf_jv_get(m, "about")->n >= 0.5) continue;
            char *p = tmpl(bf_jv_get(m, "publisher"));
            LINE("- %s (%s)", bf_jv_s(bf_jv_get(m, "title")), p); free(p);
        }
        LINE("%s", "");
    }
    const bf_jv *w = bf_jv_get(report, "warnings");
    for (size_t i = 0; w && i < w->len; i++) { LINE("Note: %s.", w->items[i]->s); LINE("%s", ""); }
    if (bf_jv_s(bf_jv_get(report, "model"))) {
        char *l = dp_fmt("Read on this machine by %s. Each judgement has a receipt in the JSON report (--out report.json).", bf_jv_s(bf_jv_get(report, "model")));
        bf_re_append(&b, l, strlen(l)); free(l);
    } else { const char *none = "Nothing in the archive matched, so no model was asked."; bf_re_append(&b, none, strlen(none)); }
    bf_re_append(&b, "\n", 1);
    return b.buf;
}

static void mkdirs_for(const char *path) {
    char *d = strdup(path), *slash = strrchr(d, '/');
    if (slash) { *slash = 0; for (char *p = d + 1; *p; p++) if (*p == '/') { *p = 0; mkdir(d, 0777); *p = '/'; } mkdir(d, 0777); }
    free(d);
}

static double js_number_arg(const char *v, double fallback) {
    if (!v || !*v || !strcmp(v, DP_TRUE)) return v && !strcmp(v, DP_TRUE) ? 1 : fallback;   /* Number(true) is 1 */
    char *end; char *t = dp_js_trim(v); double d = *t ? strtod(t, &end) : 0;
    if (*t && *end) d = NAN;
    free(t);
    return d;
}

int monitor_main(int argc, char **argv) {
    if (argc > 0 && (!strcmp(argv[0], "--help") || !strcmp(argv[0], "-h") || !strcmp(argv[0], "help"))) { fputs(MONITOR_USAGE, stdout); return 0; }
    static const char *const repeatable[] = {"watch", NULL};
    DpFlags fl; dp_flags(&fl, argc, argv, repeatable);
    char **names; int nn = dp_flag_list(&fl, "watch", &names);
    if (!nn) dp_fail("bonfyre monitor needs --watch NAME (see bonfyre monitor --help)");
    DpLocal local;
    dp_local_init(&local, dp_flag(&fl, "model-endpoint"), dp_flag_is_true(&fl, "model"), dp_flag(&fl, "model") && !dp_flag_is_true(&fl, "model") ? dp_flag(&fl, "model") : NULL);
    const char *about = dp_flag(&fl, "about");
    const char *archive_flag = dp_flag(&fl, "archive");
    char *archive = archive_flag && strcmp(archive_flag, DP_TRUE) && *archive_flag ? strdup(archive_flag)
                  : getenv("BONFYRE_ITEM_ARCHIVE_DB") && *getenv("BONFYRE_ITEM_ARCHIVE_DB") ? strdup(getenv("BONFYRE_ITEM_ARCHIVE_DB"))
                  : dp_fmt("%s/.bonfyre/estate-local/items.db", getenv("HOME") ? getenv("HOME") : "");
    bf_jv *report = NULL; DpCatch c; char failure[2048] = "";
    if (DP_TRY(&c)) {
        report = run_monitor(archive, names, nn, about && strcmp(about, DP_TRUE) ? about : "",
                             js_number_arg(dp_flag(&fl, "days"), 30), (long)js_number_arg(dp_flag(&fl, "limit"), 40), &local);
        DP_END(&c);
    } else snprintf(failure, sizeof failure, "%s", c.msg);
    dp_local_close(&local);
    if (*failure) dp_fail("%s", failure);
    const char *out = dp_flag(&fl, "out");
    if (out && strcmp(out, DP_TRUE)) {
        mkdirs_for(out);
        size_t n = strlen(out);
        if (n >= 3 && !strcmp(out + n - 3, ".md")) { char *md = digest_markdown(report); dp_write_file(out, md, strlen(md)); }
        else { size_t len; char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, &len); char *w = dp_fmt("%s\n", s); dp_write_file(out, w, len + 1); }
    }
    if (dp_flag(&fl, "json")) { char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, NULL); printf("%s\n", s); }
    else { char *md = digest_markdown(report); fputs(md, stdout); }
    return 0;
}
