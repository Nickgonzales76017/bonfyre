/*
 * dp_gguf.c — the local model owner (gguf-owner.mjs). It never generates
 * text: up to five options are lettered and asked in both orders, and the
 * letters' log-probabilities become the distribution; more options are scored
 * by name as forced continuations. The owner is bound to the exact checkpoint
 * the server reports and refuses an answer from any other.
 */
#include "dp.h"
#include "bonfyre.h"

#include <errno.h>
#include <math.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static const char LETTERS[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
#define LEASE_SECONDS 600
#define N_PROBS 64

/* ── checkpoint identity and digest ─────────────────────────────────────── */

static char *file_identity(const struct stat *st) {
#ifdef __APPLE__
    long long mt = (long long)st->st_mtimespec.tv_sec * 1000000000LL + st->st_mtimespec.tv_nsec;
    long long ct = (long long)st->st_ctimespec.tv_sec * 1000000000LL + st->st_ctimespec.tv_nsec;
#else
    long long mt = (long long)st->st_mtim.tv_sec * 1000000000LL + st->st_mtim.tv_nsec;
    long long ct = (long long)st->st_ctim.tv_sec * 1000000000LL + st->st_ctim.tv_nsec;
#endif
    return dp_fmt("%llu:%llu:%lld:%lld:%lld", (unsigned long long)st->st_dev, (unsigned long long)st->st_ino,
                  (long long)st->st_size, mt, ct);
}

static sqlite3 *digest_db(void) {
    static sqlite3 *db; static int tried;
    if (tried) return db;
    tried = 1;
    const char *home = getenv("BONFYRE_HOME");
    char *dir = home ? dp_fmt("%s/state", home) : dp_fmt("%s/.bonfyre/state", getenv("HOME") ? getenv("HOME") : ".");
    for (char *p = dir + 1; *p; p++) if (*p == '/') { *p = 0; mkdir(dir, 0777); *p = '/'; }
    mkdir(dir, 0777);
    char *path = dp_fmt("%s/checkpoints.sqlite", dir);
    if (sqlite3_open(path, &db) != SQLITE_OK ||
        sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS checkpoint_digest (path TEXT PRIMARY KEY, identity TEXT NOT NULL, sha256 TEXT NOT NULL, hashed_at TEXT NOT NULL)", 0, 0, 0) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        db = NULL;
    }
    free(dir); free(path);
    return db;
}

static char *checkpoint_digest(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) dp_fail("ENOENT: no such file or directory, open '%s'", path);
    struct stat st; fstat(fd, &st);
    if (!S_ISREG(st.st_mode)) { close(fd); dp_fail("%s is not a regular file", path); }
    char *identity = file_identity(&st), *digest = NULL;
    sqlite3 *db = digest_db(); sqlite3_stmt *q;
    if (db && sqlite3_prepare_v2(db, "SELECT sha256 FROM checkpoint_digest WHERE path = ? AND identity = ?", -1, &q, 0) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, path, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, identity, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(q) == SQLITE_ROW) digest = strdup((const char *)sqlite3_column_text(q, 0));
        sqlite3_finalize(q);
    }
    if (!digest) {
        BfSha256 ctx; bf_sha256_init(&ctx);
        size_t cap = 16 << 20; uint8_t *buf = malloc(cap);
        for (ssize_t n; (n = read(fd, buf, cap)) > 0;) bf_sha256_update(&ctx, buf, (size_t)n);
        free(buf);
        uint8_t h[32]; bf_sha256_final(&ctx, h);
        digest = malloc(65); bf_sha256_digest_hex(h, digest);
        struct stat after; fstat(fd, &after); char *again = file_identity(&after);
        int same = !strcmp(again, identity); free(again);
        if (!same) { close(fd); dp_fail("%s changed while it was being hashed", path); }
        if (db && sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO checkpoint_digest VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'))", -1, &q, 0) == SQLITE_OK) {
            sqlite3_bind_text(q, 1, path, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, identity, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(q, 3, digest, -1, SQLITE_TRANSIENT);
            sqlite3_step(q); sqlite3_finalize(q);
        }
    }
    close(fd); free(identity);
    return digest;
}

/* ── records as labelled lines ──────────────────────────────────────────── */

typedef struct { char **v; int n, cap; } Lines;
static void line_add(Lines *l, char *s) { if (l->n == l->cap) l->v = realloc(l->v, sizeof *l->v * (size_t)(l->cap = l->cap ? l->cap * 2 : 16)); l->v[l->n++] = s; }

static char *js_string(const bf_jv *v) {
    char b[32];
    switch (v->type) {
    case BF_JV_STR: return strdup(v->s);
    case BF_JV_NUM: bf_js_number(v->n, b); return strdup(b);
    case BF_JV_BOOL: return strdup(v->b ? "true" : "false");
    case BF_JV_NULL: return strdup("null");
    default: return strdup("[object Object]");
    }
}
static char *underscores_to_spaces(const char *k) { char *s = strdup(k); for (char *p = s; *p; p++) if (*p == '_') *p = ' '; return s; }

static void render(const bf_jv *v, const char *prefix, Lines *out) {
    if (!v || v->type == BF_JV_NULL || (v->type == BF_JV_STR && !v->len)) return;
    if (v->type == BF_JV_ARR) {
        int flat = 1;
        for (size_t i = 0; i < v->len; i++) if (v->items[i]->type == BF_JV_ARR || v->items[i]->type == BF_JV_OBJ || v->items[i]->type == BF_JV_NULL) flat = 0;
        if (flat) {
            if (!v->len) return;
            line_add(out, dp_fmt("%s:", prefix));
            for (size_t i = 0; i < v->len; i++) { char *s = js_string(v->items[i]); line_add(out, dp_fmt("- %s", s)); free(s); }
            return;
        }
        for (size_t i = 0; i < v->len; i++) { char *p = dp_fmt("%s %zu", prefix, i + 1); render(v->items[i], p, out); free(p); }
        return;
    }
    if (v->type == BF_JV_OBJ) {
        for (size_t i = 0; i < v->len; i++) {
            char *k = underscores_to_spaces(v->keys[i]);
            char *p = *prefix ? dp_fmt("%s %s", prefix, k) : strdup(k);
            render(v->items[i], p, out);
            free(p); free(k);
        }
        return;
    }
    char *s = js_string(v); line_add(out, dp_fmt("%s: %s", prefix, s)); free(s);
}
static char *render_record(const bf_jv *state) {
    Lines l = {0}; render(state, "", &l);
    size_t n = 0; for (int i = 0; i < l.n; i++) n += strlen(l.v[i]) + 1;
    char *s = malloc(n + 1); s[0] = 0;
    for (int i = 0; i < l.n; i++) { if (i) strcat(s, "\n"); strcat(s, l.v[i]); free(l.v[i]); }
    free(l.v);
    return s;
}

static bf_jv *content_free(const bf_jv *v) {
    if (v->type == BF_JV_ARR) { bf_jv *a = bf_jv_arr(); for (size_t i = 0; i < v->len; i++) bf_jv_push(a, content_free(v->items[i])); return a; }
    if (v->type == BF_JV_OBJ) { bf_jv *o = bf_jv_obj(); for (size_t i = 0; i < v->len; i++) bf_jv_set(o, v->keys[i], content_free(v->items[i])); return o; }
    return v->type == BF_JV_STR ? bf_jv_str("N/A") : bf_jv_clone(v);
}

/* optionsOf: [key, text] pairs */
typedef struct { char **key, **text; int n; } Options;
static Options options_of(const bf_jv *q) {
    Options o = {0};
    const char *type = bf_jv_s(bf_jv_get(q, "type"));
    const bf_jv *crit = bf_jv_get(q, "criteria");
    if (!strcmp(type, "noul")) {
        o.n = 2; o.key = malloc(2 * sizeof(char *)); o.text = malloc(2 * sizeof(char *));
        o.key[0] = strdup("true"); o.text[0] = strdup("yes"); o.key[1] = strdup("false"); o.text[1] = strdup("no");
    } else if (!strcmp(type, "choice")) {
        o.n = (int)crit->len; o.key = malloc(sizeof(char *) * (crit->len + 1)); o.text = malloc(sizeof(char *) * (crit->len + 1));
        for (size_t i = 0; i < crit->len; i++) { o.key[i] = strdup(crit->keys[i]); o.text[i] = js_string(crit->items[i]); }
    } else {
        o.n = (int)crit->len; o.key = malloc(sizeof(char *) * (crit->len + 1)); o.text = malloc(sizeof(char *) * (crit->len + 1));
        for (size_t i = 0; i < crit->len; i++) { o.key[i] = dp_fmt("%zu", i); o.text[i] = js_string(crit->items[i]); }
    }
    return o;
}
static void options_free(Options *o) { for (int i = 0; i < o->n; i++) { free(o->key[i]); free(o->text[i]); } free(o->key); free(o->text); }

static bf_jv *message(const char *role, char *content) {
    bf_jv *m = bf_jv_obj(); bf_jv_set(m, "role", bf_jv_str(role)); bf_jv_set(m, "content", bf_jv_str(content)); free(content); return m;
}

static bf_jv *question_prompt(const bf_jv *state, const char *id, const bf_jv *q, const int *order) {
    Options o = options_of(q);
    if (o.n > 26) dp_fail("%s: more options than letters", id);
    char *record = render_record(state);
    const char *ins = bf_jv_s(bf_jv_get(q, "instructions"));
    bf_re_buf b = {0};
    char *head = dp_fmt("%s\n\nQuestion: %s\n\n", record, ins && *ins ? ins : id);
    bf_re_append(&b, head, strlen(head)); free(head);
    for (int i = 0; i < o.n; i++) {
        int k = order ? order[i] : i;
        char *line = !strcmp(o.key[k], o.text[k]) ? dp_fmt("%s%c. %s", i ? "\n" : "", LETTERS[i], o.key[k])
                                                 : dp_fmt("%s%c. %s: %s", i ? "\n" : "", LETTERS[i], o.key[k], o.text[k]);
        bf_re_append(&b, line, strlen(line)); free(line);
    }
    bf_re_append(&b, "\n\nLetter:", 9);
    bf_jv *msgs = bf_jv_arr();
    bf_jv_push(msgs, message("system", strdup("You answer one multiple-choice question about the record you are given. Reply with the single letter of the best option and nothing else.")));
    bf_jv_push(msgs, message("user", b.buf));
    free(record); options_free(&o);
    return msgs;
}

static char *option_name(const char *key, const char *text) {
    if (!strcmp(key, "none")) return strdup("none");
    const char *colon = strchr(text, ':');
    if (colon) { char *head = malloc((size_t)(colon - text) + 1); memcpy(head, text, (size_t)(colon - text)); head[colon - text] = 0; char *t = dp_js_trim(head); free(head); return t; }
    return strdup(key);
}

static bf_jv *named_prompt(const bf_jv *state, const bf_jv *q) {
    Options o = options_of(q);
    char *record = render_record(state);
    const char *ins = bf_jv_s(bf_jv_get(q, "instructions"));
    bf_re_buf b = {0};
    char *head = dp_fmt("%s\n\nQuestion: %s\n\nOptions:\n", record, ins ? ins : "undefined");
    bf_re_append(&b, head, strlen(head)); free(head);
    for (int i = 0; i < o.n; i++) {
        char *name = option_name(o.key[i], o.text[i]);
        int bare = strchr(o.text[i], ':') || !strcmp(o.key[i], "none");
        char *line = bare ? dp_fmt("%s- %s", i ? "\n" : "", name) : dp_fmt("%s- %s (%s)", i ? "\n" : "", name, o.text[i]);
        bf_re_append(&b, line, strlen(line)); free(line); free(name);
    }
    bf_re_append(&b, "\n\nOption:", 9);
    bf_jv *msgs = bf_jv_arr();
    bf_jv_push(msgs, message("system", strdup("Reply with the exact name of one option from the list and nothing else.")));
    bf_jv_push(msgs, message("user", b.buf));
    free(record); options_free(&o);
    return msgs;
}

/* ── the owner ──────────────────────────────────────────────────────────── */

typedef struct { char *key; bf_jv *value; } Memo;
typedef struct {
    char *origin, *model_path, *pinned;
    long lease; double leased_at;
    Memo memo[128]; int nmemo;
} Gguf;

static bf_jv *post(Gguf *g, const char *path, bf_jv *body) {
    char *text = bf_jv_dump(body, 0, BF_JV_SORT_NONE, NULL); bf_jv_free(body);
    DpHttp h; int rc = dp_http(g->origin, "POST", path, text, 120000, &h); free(text);
    if (rc) dp_fail("%s", h.error);
    if (h.status < 200 || h.status > 299) {
        char cut[201]; snprintf(cut, sizeof cut, "%.200s", h.body ? h.body : "");
        int status = h.status; dp_http_free(&h);
        dp_fail("local model HTTP %d on %s: %s", status, path, cut);
    }
    bf_jv *v = bf_jv_parse(h.body, h.len, NULL, 0);
    dp_http_free(&h);
    if (!v) dp_fail("JSON Parse error: Unexpected EOF");
    return v;
}

static bf_jv *get_json(Gguf *g, const char *path) {
    DpHttp h;
    if (dp_http(g->origin, "GET", path, NULL, 120000, &h)) dp_fail("%s", h.error);
    bf_jv *v = bf_jv_parse(h.body, h.len, NULL, 0);
    dp_http_free(&h);
    if (!v) dp_fail("JSON Parse error: Unexpected EOF");
    return v;
}

static int lease_call(Gguf *g, const char *query, bf_jv **body) {
    char *path = dp_fmt("/bonfyre/lease?%s", query);
    DpHttp h; int rc = dp_http(g->origin, "POST", path, NULL, 120000, &h); free(path);
    if (rc) dp_fail("%s", h.error);
    *body = bf_jv_parse(h.body, h.len, NULL, 0);
    int status = h.status; dp_http_free(&h);
    return status;
}

static int acknowledged(const bf_jv *body, long id, const char *word) {
    const bf_jv *l = bf_jv_get(body, "lease"), *w = bf_jv_get(body, word);
    return bf_jv_is(l, BF_JV_NUM) && l->n == (double)id && bf_jv_is(w, BF_JV_BOOL) && w->b;
}

static void release(Gguf *g) {
    if (g->lease < 0) return;
    char q[64]; snprintf(q, sizeof q, "release=%ld", g->lease);
    bf_jv *body = NULL; int status = -1;
    DpCatch c;
    if (DP_TRY(&c)) { status = lease_call(g, q, &body); DP_END(&c); }
    if (status == 404 || (status >= 200 && status <= 299 && acknowledged(body, g->lease, "released"))) g->lease = -1;
    bf_jv_free(body);
    if (g->lease >= 0) dp_fail("the model front did not acknowledge releasing lease %ld", g->lease);
}

static void renew(Gguf *g) {
    if (g->lease < 0 || dp_now_ms() - g->leased_at < LEASE_SECONDS * 500.0) return;
    char q[96]; snprintf(q, sizeof q, "renew=%ld&ttl=%d", g->lease, LEASE_SECONDS);
    bf_jv *body = NULL; int status = lease_call(g, q, &body);
    if (status == 404) { g->lease = -1; bf_jv_free(body); return; }
    int ok = status >= 200 && status <= 299 && acknowledged(body, g->lease, "renewed");
    bf_jv_free(body);
    if (!ok) dp_fail("the model front did not acknowledge renewing lease %ld", g->lease);
    g->leased_at = dp_now_ms();
}

static int same_pinned(Gguf *g, const char *model, const bf_jv *name) {
    if (!name || name->type == BF_JV_NULL) return 1;
    char *s = js_string(name); size_t sl = strlen(s), pl = strlen(g->model_path);
    char *with = dp_fmt("%s.gguf", model); size_t wl = strlen(with);
    int ok = !strcmp(s, model) || (sl <= pl && !strcmp(g->model_path + pl - sl, s)) || (wl <= sl && !strcmp(s + sl - wl, with));
    free(s); free(with);
    return ok;
}

static void check_pinned(DpOwner *o) {
    Gguf *g = o->ctx;
    renew(g);
    struct stat st;
    if (stat(g->model_path, &st)) dp_fail("ENOENT: no such file or directory, stat '%s'", g->model_path);
    char *id = file_identity(&st); int same = !strcmp(id, g->pinned); free(id);
    if (!same) dp_fail("%s changed on disk after it was pinned", o->model);
    bf_jv *now = get_json(g, "/props");
    const char *mp = bf_jv_s(bf_jv_get(now, "model_path"));
    if (!mp || strcmp(mp, g->model_path)) { char *shown = bf_jv_get(now, "model_path") ? js_string(bf_jv_get(now, "model_path")) : strdup("undefined"); dp_fail("the server now serves %s, not the pinned %s", shown, o->model); }
    bf_jv_free(now);
}

static bf_jv *memo_get(Gguf *g, const char *key) {
    for (int i = 0; i < g->nmemo; i++) if (!strcmp(g->memo[i].key, key)) return g->memo[i].value;
    return NULL;
}
static bf_jv *memo_set(Gguf *g, const char *key, bf_jv *value) {
    if (g->nmemo == 128) { free(g->memo[0].key); bf_jv_free(g->memo[0].value); memmove(g->memo, g->memo + 1, sizeof g->memo[0] * 127); g->nmemo--; }
    g->memo[g->nmemo].key = strdup(key); g->memo[g->nmemo].value = value; g->nmemo++;
    return value;
}

typedef struct { double *p; double covered; } Dist;

static Dist letter_distribution(const bf_jv *top, int count) {
    Dist d = {calloc((size_t)count, sizeof(double)), 0};
    double floor = INFINITY;
    for (size_t i = 0; bf_jv_is(top, BF_JV_ARR) && i < top->len; i++) {
        const bf_jv *e = top->items[i];
        const bf_jv *tok = bf_jv_get(e, "token");
        char *t = tok && !(tok->type == BF_JV_STR && !tok->len) && !(tok->type == BF_JV_BOOL && !tok->b) && tok->type != BF_JV_NULL ? js_string(tok) : strdup("");
        char *tr = dp_js_trim(t); free(t);
        size_t n = strlen(tr); if (n && strchr(".):", tr[n - 1])) tr[--n] = 0;
        double lp = bf_jv_is(bf_jv_get(e, "logprob"), BF_JV_NUM) ? bf_jv_get(e, "logprob")->n : NAN;
        double ex = exp(lp);
        if (ex < floor || ex != ex) floor = ex != ex ? floor : ex;
        if (n == 1) {
            int c = tr[0] >= 'a' && tr[0] <= 'z' ? tr[0] - 32 : tr[0];
            const char *at = strchr(LETTERS, c);
            if (at && c && at - LETTERS < count) d.p[at - LETTERS] += ex;
        }
        free(tr);
    }
    for (int i = 0; i < count; i++) d.covered += d.p[i];
    if (!(d.covered > 0)) dp_fail("the model put no probability on any option letter");
    int zero = 0; for (int i = 0; i < count; i++) if (d.p[i] == 0) zero = 1;
    if (zero && !(floor < 1e-3)) dp_fail("an option letter was not scored");
    for (int i = 0; i < count; i++) d.p[i] /= d.covered;
    return d;
}

static Dist ask(DpOwner *o, bf_jv *messages, int count) {
    Gguf *g = o->ctx;
    bf_jv *body = bf_jv_obj();
    bf_jv_set(body, "messages", messages);
    bf_jv_set(body, "max_tokens", bf_jv_num(1));
    bf_jv_set(body, "temperature", bf_jv_num(0));
    bf_jv_set(body, "logprobs", bf_jv_bool(1));
    bf_jv_set(body, "top_logprobs", bf_jv_num(20));
    bf_jv *r = post(g, "/v1/chat/completions", body);
    if (!same_pinned(g, o->model, bf_jv_get(r, "model"))) { char *m = js_string(bf_jv_get(r, "model")); dp_fail("the server answered with %s, not the pinned %s", m, o->model); }
    const bf_jv *ch = bf_jv_at(bf_jv_get(r, "choices"), 0);
    const bf_jv *top = bf_jv_get(bf_jv_at(bf_jv_get(bf_jv_get(ch, "logprobs"), "content"), 0), "top_logprobs");
    Dist d = letter_distribution(top, count);
    bf_jv_free(r);
    return d;
}

static Dist read_question(DpOwner *o, const bf_jv *record, const char *id, const bf_jv *q, int count) {
    Dist out = {calloc((size_t)count, sizeof(double)), 0};
    int *fwd = malloc(sizeof(int) * (size_t)count), *bwd = malloc(sizeof(int) * (size_t)count);
    for (int i = 0; i < count; i++) { fwd[i] = i; bwd[i] = count - 1 - i; }
    int *orders[2] = {fwd, bwd};
    for (int r = 0; r < 2; r++) {
        Dist d = ask(o, question_prompt(record, id, q, orders[r]), count);
        for (int i = 0; i < count; i++) out.p[orders[r][i]] += d.p[i] / 2;
        out.covered += d.covered / 2;
        free(d.p);
    }
    free(fwd); free(bwd);
    return out;
}

/* Token sequences as a trie; one /completion call per distinct prefix. */
typedef struct TNode { long token; struct TNode **kids; int nkids; int *ends; int nends; } TNode;
static TNode *tnode(long token) { TNode *n = calloc(1, sizeof *n); n->token = token; return n; }

static void walk(Gguf *g, const char *model, const char *prompt, TNode *node, long *prefix, int depth, double logp, double *score) {
    bf_jv *body = bf_jv_obj(), *pr = bf_jv_arr();
    bf_jv_push(pr, bf_jv_str(prompt));
    for (int i = 0; i < depth; i++) bf_jv_push(pr, bf_jv_num((double)prefix[i]));
    bf_jv_set(body, "prompt", pr);
    bf_jv_set(body, "n_predict", bf_jv_num(1));
    bf_jv_set(body, "n_probs", bf_jv_num(N_PROBS));
    bf_jv_set(body, "temperature", bf_jv_num(0));
    bf_jv_set(body, "cache_prompt", bf_jv_bool(1));
    bf_jv_set(body, "post_sampling_probs", bf_jv_bool(0));
    bf_jv *r = post(g, "/completion", body);
    if (!same_pinned(g, model, bf_jv_get(r, "model"))) { char *m = js_string(bf_jv_get(r, "model")); dp_fail("the server answered with %s, not the pinned %s", m, model); }
    const bf_jv *first = bf_jv_at(bf_jv_get(r, "completion_probabilities"), 0);
    const bf_jv *entries = bf_jv_get(first, "top_logprobs");
    if (!entries || (entries->type == BF_JV_NULL)) entries = bf_jv_get(first, "probs");
    long *ids = NULL; double *lps = NULL; int n = 0;
    for (size_t i = 0; bf_jv_is(entries, BF_JV_ARR) && i < entries->len; i++) {
        const bf_jv *e = entries->items[i];
        const bf_jv *id = bf_jv_get(e, "id"); if (!id || id->type == BF_JV_NULL) id = bf_jv_get(e, "tok");
        const bf_jv *lp = bf_jv_get(e, "logprob"); double v = NAN;
        if (lp && lp->type != BF_JV_NULL) v = bf_jv_is(lp, BF_JV_NUM) ? lp->n : NAN;
        else { const bf_jv *p = bf_jv_get(e, "prob"); if (bf_jv_is(p, BF_JV_NUM) && p->n > 0) v = log(p->n); }
        if (!bf_jv_is(id, BF_JV_NUM) || id->n != floor(id->n) || !isfinite(id->n) || !isfinite(v)) continue;
        int at = -1; for (int k = 0; k < n; k++) if (ids[k] == (long)id->n) at = k;
        if (at < 0) { ids = realloc(ids, sizeof *ids * (size_t)(n + 1)); lps = realloc(lps, sizeof *lps * (size_t)(n + 1)); at = n++; ids[at] = (long)id->n; }
        lps[at] = v;
    }
    bf_jv_free(r);
    if (!n) dp_fail("the server returned no token probabilities");
    for (int c = 0; c < node->nkids; c++) {
        TNode *child = node->kids[c]; int at = -1;
        for (int k = 0; k < n; k++) if (ids[k] == child->token) at = k;
        if (at < 0) { char t[32]; bf_js_number((double)child->token, t); dp_fail("option token %s was not scored; widen n_probs or shorten the option names", t); }
        double here = logp + lps[at];
        for (int e = 0; e < child->nends; e++) score[child->ends[e]] = exp(here);
        if (child->nkids) { prefix[depth] = child->token; walk(g, model, prompt, child, prefix, depth + 1, here, score); }
    }
    free(ids); free(lps);
}

static bf_jv *tokens_of(Gguf *g, const char *text) {
    char *k2 = dp_fmt("tokens\x1f%s", text);
    bf_jv *hit = memo_get(g, k2);
    if (!hit) {
        bf_jv *body = bf_jv_obj(); bf_jv_set(body, "content", bf_jv_str(text)); bf_jv_set(body, "add_special", bf_jv_bool(0));
        bf_jv *r = post(g, "/tokenize", body);
        hit = memo_set(g, k2, bf_jv_clone(bf_jv_get(r, "tokens")));
        bf_jv_free(r);
    }
    free(k2);
    return hit;
}

static bf_jv *gguf_evaluate(DpOwner *o, const bf_jv *state, const bf_jv *questions, const DpSchema *schema) {
    (void)schema;
    Gguf *g = o->ctx;
    check_pinned(o);
    bf_jv *answers = bf_jv_obj();
    for (size_t qi = 0; qi < questions->len; qi++) {
        const char *id = questions->keys[qi]; const bf_jv *q = questions->items[qi];
        Options opt = options_of(q); int count = opt.n;
        const char *type = bf_jv_s(bf_jv_get(q, "type"));
        if (!strcmp(type, "choice") && count > 5) {
            bf_jv *messages = named_prompt(state, q);
            char *mk = bf_jv_dump(messages, 0, BF_JV_SORT_NONE, NULL), *tk = dp_fmt("template\x1f%s", mk); free(mk);
            bf_jv *tmpl = memo_get(g, tk);
            if (!tmpl) {
                bf_jv *body = bf_jv_obj(); bf_jv_set(body, "messages", bf_jv_clone(messages));
                bf_jv *r = post(g, "/apply-template", body);
                tmpl = memo_set(g, tk, bf_jv_clone(bf_jv_get(r, "prompt")));
                bf_jv_free(r);
            }
            free(tk); bf_jv_free(messages);
            const char *prompt = bf_jv_s(tmpl) ? tmpl->s : "";
            TNode *root = tnode(-1);
            bf_jv **seqs = malloc(sizeof *seqs * (size_t)count);
            for (int i = 0; i < count; i++) { char *name = option_name(opt.key[i], opt.text[i]); seqs[i] = tokens_of(g, name); free(name); }
            for (int i = 0; i < count; i++) for (int j = 0; j < count; j++) {   /* prefixFree */
                if (i == j || !bf_jv_is(seqs[i], BF_JV_ARR) || !bf_jv_is(seqs[j], BF_JV_ARR) || seqs[i]->len > seqs[j]->len) continue;
                int pre = 1; for (size_t t = 0; t < seqs[i]->len; t++) if (seqs[i]->items[t]->n != seqs[j]->items[t]->n) pre = 0;
                if (pre) dp_fail("%s: one option name extends another, so their scores would overlap", id);
            }
            int maxlen = 0;
            for (int i = 0; i < count; i++) {
                const bf_jv *seq = seqs[i];
                if (!bf_jv_is(seq, BF_JV_ARR) || !seq->len) dp_fail("an option name has no tokens");
                if ((int)seq->len > maxlen) maxlen = (int)seq->len;
                TNode *node = root;
                for (size_t t = 0; t < seq->len; t++) {
                    long tok = (long)seq->items[t]->n; TNode *child = NULL;
                    for (int c = 0; c < node->nkids; c++) if (node->kids[c]->token == tok) child = node->kids[c];
                    if (!child) { child = tnode(tok); node->kids = realloc(node->kids, sizeof *node->kids * (size_t)(node->nkids + 1)); node->kids[node->nkids++] = child; }
                    if (t == seq->len - 1) { child->ends = realloc(child->ends, sizeof *child->ends * (size_t)(child->nends + 1)); child->ends[child->nends++] = i; }
                    node = child;
                }
            }
            double *score = calloc((size_t)count, sizeof(double));
            long *prefix = calloc((size_t)maxlen + 1, sizeof(long));
            walk(g, o->model, prompt, root, prefix, 0, 0, score);
            double on = 0; for (int i = 0; i < count; i++) on += score[i];
            if (!(on > 0)) dp_fail("%s: the model put no probability on any option", id);
            int abstain = bf_jv_has(bf_jv_get(q, "criteria"), "none");
            bf_jv *probs = bf_jv_obj();
            for (int i = 0; i < count; i++)
                bf_jv_set(probs, opt.key[i], bf_jv_num(abstain ? score[i] + (!strcmp(opt.key[i], "none") ? fmax(0, 1 - on) : 0) : score[i] / on));
            const char *choice = opt.key[0];
            for (int i = 0; i < count; i++) if (bf_jv_get(probs, opt.key[i])->n > bf_jv_get(probs, choice)->n) choice = opt.key[i];
            bf_jv *a = bf_jv_obj();
            bf_jv_set(a, "type", bf_jv_str("choice"));
            bf_jv_set(a, "choice", bf_jv_str(choice));
            bf_jv_set(a, "confidence", bf_jv_num(bf_jv_get(probs, choice)->n));
            bf_jv_set(a, "probabilities", probs);
            bf_jv_set(answers, id, a);
            free(score); free(prefix); free(seqs);
            options_free(&opt);
            continue;
        }
        /* key = JSON.stringify([id, question, Object.keys(state)]) */
        bf_jv *kv = bf_jv_arr(), *keys = bf_jv_arr();
        bf_jv_push(kv, bf_jv_str(id)); bf_jv_push(kv, bf_jv_clone(q));
        for (size_t i = 0; i < state->len; i++) bf_jv_push(keys, bf_jv_str(state->keys[i]));
        bf_jv_push(kv, keys);
        char *kj = bf_jv_dump(kv, 0, BF_JV_SORT_NONE, NULL), *pk = dp_fmt("prior\x1f%s", kj); free(kj); bf_jv_free(kv);
        bf_jv *prior = memo_get(g, pk);
        if (!prior) {
            bf_jv *blank = content_free(state);
            Dist d = read_question(o, blank, id, q, count);
            bf_jv_free(blank);
            bf_jv *arr = bf_jv_arr(); for (int i = 0; i < count; i++) bf_jv_push(arr, bf_jv_num(d.p[i]));
            free(d.p);
            prior = memo_set(g, pk, arr);
        }
        free(pk);
        Dist d = read_question(o, state, id, q, count);
        double *adj = malloc(sizeof(double) * (size_t)count), total = 0;
        for (int i = 0; i < count; i++) { adj[i] = d.p[i] / (0.5 * prior->items[i]->n + 0.5 / count); total += adj[i]; }
        for (int i = 0; i < count; i++) adj[i] /= total;
        bf_jv *a = bf_jv_obj();
        if (!strcmp(type, "noul")) { bf_jv_set(a, "type", bf_jv_str("noul")); bf_jv_set(a, "noul", bf_jv_num(adj[0])); }
        else {
            double peak = adj[0]; for (int i = 1; i < count; i++) if (adj[i] > peak) peak = adj[i];
            int winner = 0; while (adj[winner] != peak) winner++;
            bf_jv *probs = bf_jv_obj(); for (int i = 0; i < count; i++) bf_jv_set(probs, opt.key[i], bf_jv_num(adj[i]));
            bf_jv_set(a, "type", bf_jv_str(type));
            bf_jv_set(a, "confidence", bf_jv_num(peak));
            bf_jv_set(a, "probabilities", probs);
            if (!strcmp(type, "choice")) bf_jv_set(a, "choice", bf_jv_str(opt.key[winner]));
            else { double s = 0; for (int i = 0; i < count; i++) s += adj[i] * i; bf_jv_set(a, "score", bf_jv_num(s)); }
        }
        bf_jv_set(answers, id, a);
        free(adj); free(d.p); options_free(&opt);
    }
    bf_jv *out = bf_jv_obj(); bf_jv_set(out, "answers", answers);
    return out;
}

static void gguf_close(DpOwner *o) { release(o->ctx); }

static char *encode_uri_component(const char *s) {
    bf_re_buf b = {0};
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || strchr("-_.!~*'()", *p)) bf_re_append(&b, (const char *)p, 1);
        else { char e[4]; snprintf(e, sizeof e, "%%%02X", *p); bf_re_append(&b, e, 3); }
    }
    if (!b.buf) bf_re_append(&b, "", 0);
    return b.buf;
}

DpOwner *dp_gguf_owner(const char *endpoint, const char *prefer) {
    char host[256] = "";
    const char *p = strstr(endpoint, "://"); p = p ? p + 3 : endpoint;
    if (*p == '[') { const char *e = strchr(p, ']'); snprintf(host, sizeof host, "%.*s", e ? (int)(e - p + 1) : 0, p); }
    else snprintf(host, sizeof host, "%.*s", (int)strcspn(p, ":/"), p);
    if (strcmp(host, "127.0.0.1") && strcmp(host, "localhost") && strcmp(host, "[::1]")) dp_fail("the GGUF owner must be on loopback");
    Gguf *g = calloc(1, sizeof *g);
    size_t ol = (size_t)(p - endpoint) + strcspn(p, "/");
    g->origin = malloc(ol + 1); memcpy(g->origin, endpoint, ol); g->origin[ol] = 0;
    g->lease = -1;
    char *pe = *prefer ? encode_uri_component(prefer) : NULL;
    char *q = pe ? dp_fmt("ttl=%d&prefer=%s", LEASE_SECONDS, pe) : dp_fmt("ttl=%d", LEASE_SECONDS);
    free(pe);
    bf_jv *body = NULL; int status = lease_call(g, q, &body); free(q);
    if (status >= 200 && status <= 299) {
        const bf_jv *l = bf_jv_get(body, "lease");
        if (!bf_jv_is(l, BF_JV_NUM) || l->n != floor(l->n) || l->n < 1 || l->n > 9007199254740991.0) dp_fail("the model front answered a lease request without a lease");
        g->lease = (long)l->n; g->leased_at = dp_now_ms();
    } else if (status != 404) {
        const char *m = bf_jv_s(bf_jv_path(body, "error.message"));
        if (m && *m) dp_fail("%s", m);
        dp_fail("the model front refused a lease (HTTP %d)", status);
    }
    bf_jv_free(body);
    DpCatch c; char *digest = NULL;
    if (DP_TRY(&c)) {
        bf_jv *props = get_json(g, "/props");
        const char *mp = bf_jv_s(bf_jv_get(props, "model_path"));
        struct stat st;
        if (!mp || stat(mp, &st)) dp_fail("the local model server did not name a readable checkpoint");
        g->model_path = strdup(mp); bf_jv_free(props);
        g->pinned = file_identity(&st);
        digest = checkpoint_digest(g->model_path);
        stat(g->model_path, &st); char *again = file_identity(&st);
        int same = !strcmp(again, g->pinned); free(again);
        if (!same) dp_fail("the checkpoint changed while it was being pinned");
        DP_END(&c);
    } else {
        char msg[2048]; snprintf(msg, sizeof msg, "%s", c.msg);
        DpCatch r; if (DP_TRY(&r)) { release(g); DP_END(&r); }
        dp_fail("%s", msg);
    }
    DpOwner *o = calloc(1, sizeof *o);
    const char *base = strrchr(g->model_path, '/'); base = base ? base + 1 : g->model_path;
    o->model = strdup(base);
    size_t ml = strlen(o->model); if (ml > 5 && !strcmp(o->model + ml - 5, ".gguf")) o->model[ml - 5] = 0;
    snprintf(o->artifact_digest, sizeof o->artifact_digest, "%s", digest); free(digest);
    o->deterministic = 0;
    o->question_semantics = "independent_evaluation";
    o->confidence_semantics = "maximum_option_probability";
    o->evaluate = gguf_evaluate; o->close = gguf_close; o->ctx = g;
    return o;
}

/* ── product-cli localOwner ─────────────────────────────────────────────── */

void dp_local_init(DpLocal *l, const char *endpoint_flag, int model_flag_true, const char *model_flag) {
    memset(l, 0, sizeof *l);
    const char *env = getenv("BONFYRE_LOCAL_MODEL"), *penv = getenv("BONFYRE_LOCAL_MODEL_PREFER");
    l->endpoint = strdup(endpoint_flag && *endpoint_flag && strcmp(endpoint_flag, DP_TRUE) ? endpoint_flag : env && *env ? env : "http://127.0.0.1:7000");
    l->prefer = strdup(model_flag_true ? "" : model_flag && *model_flag ? model_flag : penv && *penv ? penv : "");
}

DpOwner *dp_local_acquire(DpLocal *l) {
    if (l->owner) return l->owner;
    if (l->tried) dp_fail("%s", l->error);
    l->tried = 1;
    DpCatch c;
    if (DP_TRY(&c)) { l->owner = dp_gguf_owner(l->endpoint, l->prefer); DP_END(&c); return l->owner; }
    snprintf(l->error, sizeof l->error, "the local model at %s is not answering (%s). Start it with: bonfyre-moe serve --port 7000   (see: bonfyre model local)", l->endpoint, c.msg);
    dp_fail("%s", l->error);
}

void dp_local_close(DpLocal *l) { if (l->owner && l->owner->close) l->owner->close(l->owner); }
