/* bonfyre-broadcast -- produced audio programs composed from the estate.
 *
 *   engines  discovers who can speak here: neural speech runtimes whose model
 *            bodies the model estate holds and verifies, the system's
 *            installed voices, and speech providers whose key is present.
 *   plan     casts the program's roles onto discovered engines and writes the
 *            exact script from the item archive; every spoken line names the
 *            item it came from.
 *   render   voices each line through its cast engine (a neural load takes a
 *            memory grant from the admission ledger first, and each neural
 *            line is read back by whisper and kept only above the runtime's
 *            recall floor), cuts open-terms excerpts from Internet Archive
 *            pools with ffmpeg reading only the seconds it needs, ducks a bed
 *            under spoken idents, loudness-normalises, and writes program.m4a,
 *            Podcasting 2.0 chapters, a WebVTT transcript, credits and a
 *            manifest.
 *   publish  writes the manifest as a draft media_program row.
 *
 * Cannot infer: a spoken line repeats its item's headline and does not make
 * the story true; an excerpt's licence is what its source stated when
 * fetched, not a clearance; read-back recall measures intelligibility of the
 * words, not voice quality; a draft row is not a publication. */
#include <bonfyre.h>
#include <bf_admission.h>
#include "bf_jv.h"
#include "bf_jsonw.h"
#include "bf_command_lookup.h"
#include "bf_help.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <spawn.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include "waveform.h"
#include "media_asset.h"
#include "narration_intake.h"

extern char **environ;

/* ---------- utilities ---------- */

static int fail(const char *m) { fprintf(stderr, "bonfyre-broadcast: %s\n", m); return 1; }

static const char *opt(int argc, char **argv, const char *name, const char *def) {
    for (int i = 1; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return def;
}

static char *slurp(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb"); if (!f) return NULL;
    size_t cap = 65536, n = 0; char *b = malloc(cap);
    for (size_t r; b && (r = fread(b + n, 1, cap - n - 1, f)) > 0;) {
        n += r; if (cap - n < 2) { cap *= 2; char *t = realloc(b, cap); if (!t) { free(b); b = NULL; break; } b = t; }
    }
    fclose(f); if (b) b[n] = 0; if (len) *len = n; return b;
}

static int spit(const char *path, const char *s, size_t n) {
    FILE *f = fopen(path, "wb"); if (!f) return -1;
    int ok = fwrite(s, 1, n, f) == n; return (fclose(f) == 0 && ok) ? 0 : -1;
}

static bf_jv *load_json(const char *path) {
    size_t n; char *t = slurp(path, &n); if (!t) return NULL;
    char err[200] = ""; bf_jv *v = bf_jv_parse(t, n, err, sizeof err); free(t);
    if (!v) fprintf(stderr, "bonfyre-broadcast: %s: %s\n", path, err);
    return v;
}

static int write_json(const char *path, const bf_jv *v) {
    size_t n; char *js = bf_jv_dump(v, 1, 0, &n); int rc = js ? spit(path, js, n) : -1; free(js); return rc;
}

static const char *jstr(const bf_jv *o, const char *k) { const char *s = bf_jv_s(bf_jv_get(o, k)); return s ? s : ""; }
static double jnum(const bf_jv *o, const char *k, double d) { return bf_jv_n(bf_jv_get(o, k), d); }
static size_t jlen(const bf_jv *a) { size_t n = 0; while (a && bf_jv_at(a, n)) n++; return n; }

static void home_path(const char *in, char *out, size_t cap) {
    const char *h = getenv("HOME");
    if (in[0] == '~' && h) snprintf(out, cap, "%s%s", h, in + 1); else snprintf(out, cap, "%s", in);
}

/* Run argv; capture stdout when out is given (bounded); stderr to log. */
static int run(char *const argv[], char **out, size_t cap, const char *log) {
    posix_spawn_file_actions_t fa; posix_spawn_file_actions_init(&fa);
    int pfd[2] = {-1, -1};
    if (out && pipe(pfd) == 0) { posix_spawn_file_actions_adddup2(&fa, pfd[1], 1); posix_spawn_file_actions_addclose(&fa, pfd[0]); }
    else posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_addopen(&fa, 2, log ? log : "/dev/null", O_WRONLY | O_CREAT | O_APPEND, 0644);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    pid_t pid; int rc = posix_spawnp(&pid, argv[0], &fa, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    if (out && pfd[1] >= 0) close(pfd[1]);
    if (rc != 0) { if (pfd[0] >= 0) close(pfd[0]); return -1; }
    if (out && pfd[0] >= 0) {
        size_t n = 0; char *b = malloc(cap + 1); ssize_t r;
        while (b && n < cap && (r = read(pfd[0], b + n, cap - n)) > 0) n += (size_t)r;
        close(pfd[0]); if (b) b[n] = 0; *out = b;
    }
    int st = 0; while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static int on_path(const char *prog) {
    char *w = NULL; char cmd[300]; snprintf(cmd, sizeof cmd, "command -v %s", prog);
    char *a[] = {"sh", "-c", cmd, NULL}; int ok = run(a, &w, 512, NULL) == 0 && w && *w; free(w); return ok;
}

static uint64_t fnv(const char *s, uint64_t h) { for (; *s; s++) { h ^= (unsigned char)*s; h *= 1099511628211ULL; } return h; }

/* Speakable text: no control bytes or markup, single spaces. */
static void speakable(const char *in, char *out, size_t cap) {
    size_t n = 0; int sp = 0;
    for (const unsigned char *p = (const unsigned char *)in; *p && n + 2 < cap; p++) {
        unsigned char c = *p;
        if (c == '<') { while (*p && *p != '>') p++; if (!*p) break; c = ' '; }
        if (c < 32 || c == '|' || c == '*' || c == '#' || c == '_') c = ' ';
        if (c == ' ') { if (!sp && n) out[n++] = ' '; sp = 1; continue; }
        sp = 0; out[n++] = (char)c;
    }
    while (n && (out[n - 1] == ' ' || out[n - 1] == '.' || out[n - 1] == ',')) n--;
    out[n] = 0;
}

static const char *months[] = {"January","February","March","April","May","June","July","August","September","October","November","December"};

/* A title carrying its own full date ("...: October 18, 2026") is a
 * recurring column -- a puzzle, a digest -- not a story. */
static int recurring_column(const char *t) {
    for (int m = 0; m < 12; m++) {
        const char *p = strstr(t, months[m]);
        while (p) {
            const char *q = p + strlen(months[m]); int d = 0, y = 0;
            if (sscanf(q, " %d, %d", &d, &y) == 2 && d >= 1 && d <= 31 && y > 1900) return 1;
            p = strstr(q, months[m]);
        }
    }
    return 0;
}

/* Open terms: the rule account pages apply before they play anything. */
static int open_terms(const char *r) {
    char s[256]; size_t n = 0; if (!r) return 0;
    for (; *r && n + 1 < sizeof s; r++) s[n++] = (char)tolower((unsigned char)*r); s[n] = 0;
    if (!n || strstr(s, "-nc") || strstr(s, "-nd")) return 0;
    return strstr(s, "creativecommons.org/publicdomain/") || strstr(s, "creativecommons.org/licenses/publicdomain") ||
           strstr(s, "creativecommons.org/licenses/by/") || strstr(s, "creativecommons.org/licenses/by-sa/");
}

static bf_jv *find_id(const bf_jv *arr, const char *id) {
    for (size_t i = 0; arr && bf_jv_at(arr, i); i++) { bf_jv *e = bf_jv_at(arr, i); if (!strcmp(jstr(e, "id"), id)) return e; }
    return NULL;
}

/* ---------- engine discovery ---------- */

static int model_body(const char *id, char *path, size_t cap, char *why, size_t wcap) {
    char cache[900]; const char *c = getenv("BONFYRE_MODEL_CACHE");
    home_path(c && *c ? c : "~/.cache/bonfyre/models", cache, sizeof cache);
    snprintf(path, cap, "%s/%s.gguf", cache, id);
    if (access(path, R_OK)) { snprintf(why, wcap, "model %s has no cached body; bonfyre model fetch %s", id, id); return -1; }
    char *a[] = {"bonfyre", "model", "verify", (char *)id, NULL};
    if (run(a, NULL, 0, NULL)) { snprintf(why, wcap, "model %s body did not verify", id); return -1; }
    return 0;
}

static bf_jv *discover(const bf_jv *cfg) {
    bf_jv *out = bf_jv_arr();
    /* neural runtimes over model-estate bodies */
    bf_jv *rts = bf_jv_get(cfg, "runtimes");
    for (size_t i = 0; bf_jv_at(rts, i); i++) {
        bf_jv *rt = bf_jv_at(rts, i), *e = bf_jv_obj(); char m[1000], v[1000], why[300] = "";
        bf_jv_set(e, "id", bf_jv_str(jstr(rt, "id"))); bf_jv_set(e, "kind", bf_jv_str("neural"));
        bf_jv_set(e, "locale", bf_jv_str("en")); bf_jv_set(e, "licence", bf_jv_str(jstr(rt, "licence")));
        bf_jv_set(e, "min_recall", bf_jv_num(jnum(rt, "min_recall", 0.6)));
        int ok = on_path(jstr(rt, "runtime"));
        if (!ok) snprintf(why, sizeof why, "%s is not installed", jstr(rt, "runtime"));
        if (ok && model_body(jstr(rt, "model"), m, sizeof m, why, sizeof why)) ok = 0;
        if (ok && model_body(jstr(rt, "vocoder"), v, sizeof v, why, sizeof why)) ok = 0;
        if (ok) {
            struct stat a, b; stat(m, &a); stat(v, &b);
            bf_jv_set(e, "model_path", bf_jv_str(m)); bf_jv_set(e, "vocoder_path", bf_jv_str(v));
            /* weights resident plus working set */
            bf_jv_set(e, "memory_bytes", bf_jv_num((double)(a.st_size + b.st_size) * 1.35 + 268435456.0));
        }
        bf_jv_set(e, "ready", bf_jv_bool(ok)); if (!ok) bf_jv_set(e, "reason", bf_jv_str(why));
        bf_jv_push(out, e);
    }
    /* system voices, read live */
    char *list = NULL; char *a[] = {"say", "-v", "?", NULL};
    if (run(a, &list, 1 << 20, NULL) == 0 && list) {
        bf_jv *ex = bf_jv_get(cfg, "system_exclude");
        for (char *ln = strtok(list, "\n"); ln; ln = strtok(NULL, "\n")) {
            char *hash = strstr(ln, "# "); if (!hash) continue;
            char *loc = NULL; for (char *p = ln; p < hash; p++) if (p[0] == 'e' && p[1] == 'n' && p[2] == '_' && (p == ln || p[-1] == ' ')) { loc = p; break; }
            if (!loc) continue;
            char name[128]; size_t nl = (size_t)(loc - ln); while (nl && ln[nl - 1] == ' ') nl--;
            if (nl >= sizeof name) continue; memcpy(name, ln, nl); name[nl] = 0;
            char base[128]; snprintf(base, sizeof base, "%s", name); char *par = strstr(base, " ("); if (par) *par = 0;
            int skip = 0; for (size_t k = 0; bf_jv_at(ex, k); k++) if (!strcmp(bf_jv_s(bf_jv_at(ex, k)), base)) skip = 1;
            if (skip) continue;
            char locale[8]; snprintf(locale, sizeof locale, "%.5s", loc);
            char id[160]; snprintf(id, sizeof id, "system-%s", base); for (char *p = id; *p; p++) *p = *p == ' ' ? '-' : (char)tolower((unsigned char)*p);
            if (find_id(out, id)) continue;
            bf_jv *e = bf_jv_obj(); bf_jv_set(e, "id", bf_jv_str(id)); bf_jv_set(e, "kind", bf_jv_str("system"));
            bf_jv_set(e, "voice", bf_jv_str(name)); bf_jv_set(e, "locale", bf_jv_str(locale));
            bf_jv_set(e, "licence", bf_jv_str("operating system voice")); bf_jv_set(e, "ready", bf_jv_bool(1));
            bf_jv_push(out, e);
        }
    }
    free(list);
    /* providers */
    bf_jv *pv = bf_jv_get(cfg, "providers");
    for (size_t i = 0; bf_jv_at(pv, i); i++) {
        bf_jv *p = bf_jv_at(pv, i), *e = bf_jv_clone(p); const char *k = jstr(p, "key_env"); int ok = 1; char why[200] = "";
        bf_jv_set(e, "kind", bf_jv_str("provider")); bf_jv_set(e, "locale", bf_jv_str("en"));
        if (*k) { const char *v = getenv(k); if (!v || !*v) { ok = 0; snprintf(why, sizeof why, "%s is not set", k); } }
        else { char *b[] = {"curl", "-s", "-o", "/dev/null", "--max-time", "2", (char *)jstr(p, "url"), NULL};
               if (run(b, NULL, 0, NULL) == 7 || run(b, NULL, 0, NULL) == 28) { ok = 0; snprintf(why, sizeof why, "endpoint does not answer"); } }
        bf_jv_set(e, "ready", bf_jv_bool(ok)); if (!ok) bf_jv_set(e, "reason", bf_jv_str(why));
        bf_jv_push(out, e);
    }
    return out;
}

static int cmd_engines(int argc, char **argv) {
    const char *programs = opt(argc, argv, "--programs", NULL); if (!programs) return fail("engines --programs JSON");
    bf_jv *cfg = load_json(programs); if (!cfg) return fail("cannot read programs");
    bf_jv *e = discover(cfg); size_t n; char *js = bf_jv_dump(e, 1, 0, &n); fwrite(js, 1, n, stdout); fputc('\n', stdout);
    free(js); bf_jv_free(e); bf_jv_free(cfg); return 0;
}

static int is_true(const bf_jv *v) { char *d = bf_jv_dump(v, 0, 0, NULL); int t = d && !strcmp(d, "true"); free(d); return t; }

/* Roles in the clock's order of first use. */
static bf_jv *role_order(const bf_jv *prog) {
    bf_jv *order = bf_jv_arr(), *seen = bf_jv_obj(), *clock = bf_jv_get(prog, "clock");
    for (size_t i = 0; bf_jv_at(clock, i); i++) {
        bf_jv *s = bf_jv_at(clock, i), *vs = bf_jv_get(s, "voices");
        const char *one = jstr(s, "voice");
        if (*one && !bf_jv_has(seen, one)) { bf_jv_set(seen, one, bf_jv_bool(1)); bf_jv_push(order, bf_jv_str(one)); }
        for (size_t k = 0; bf_jv_at(vs, k); k++) {
            const char *v = bf_jv_s(bf_jv_at(vs, k));
            if (!bf_jv_has(seen, v)) { bf_jv_set(seen, v, bf_jv_bool(1)); bf_jv_push(order, bf_jv_str(v)); }
        }
    }
    const char *fb = jstr(prog, "fallback");
    if (*fb && !bf_jv_has(seen, fb)) bf_jv_push(order, bf_jv_str(fb));
    bf_jv_free(seen); return order;
}

/* Cast roles onto ready engines: the role's kind preference, then a system
 * voice in its locale, distinct engines while any remain. */
static bf_jv *cast_roles(const bf_jv *prog, const bf_jv *engines) {
    bf_jv *cast = bf_jv_obj(), *roles = bf_jv_get(prog, "cast"), *taken = bf_jv_obj(), *order = role_order(prog);
    for (size_t i = 0; bf_jv_at(order, i); i++) {
        const char *role = bf_jv_s(bf_jv_at(order, i)); bf_jv *want = bf_jv_get(roles, role);
        const char *loc = want && *jstr(want, "locale") ? jstr(want, "locale") : "en";
        bf_jv *prefer = want ? bf_jv_get(want, "prefer") : NULL; const char *pick = NULL;
        for (int reuse = 0; reuse < 2 && !pick; reuse++)
            for (size_t p = 0; bf_jv_at(prefer, p) && !pick; p++)
                for (size_t e = 0; bf_jv_at(engines, e) && !pick; e++) {
                    bf_jv *en = bf_jv_at(engines, e);
                    if (!is_true(bf_jv_get(en, "ready")) || strcmp(jstr(en, "kind"), bf_jv_s(bf_jv_at(prefer, p)))) continue;
                    if (!strcmp(jstr(en, "kind"), "system") && strncmp(jstr(en, "locale"), loc, strlen(loc))) continue;
                    if (!reuse && bf_jv_has(taken, jstr(en, "id"))) continue;
                    pick = jstr(en, "id");
                }
        for (size_t e = 0; bf_jv_at(engines, e) && !pick; e++) { /* no match: any unused system voice */
            bf_jv *en = bf_jv_at(engines, e);
            if (!strcmp(jstr(en, "kind"), "system") && is_true(bf_jv_get(en, "ready")) && !bf_jv_has(taken, jstr(en, "id"))) pick = jstr(en, "id");
        }
        if (pick) { bf_jv_set(cast, role, bf_jv_str(pick)); bf_jv_set(taken, pick, bf_jv_bool(1)); }
    }
    bf_jv_free(taken); bf_jv_free(order); return cast;
}

/* ---------- plan ---------- */

static bf_jv *line(const char *text, const char *role) {
    bf_jv *l = bf_jv_obj(); bf_jv_set(l, "text", bf_jv_str(text)); bf_jv_set(l, "role", bf_jv_str(role)); return l;
}

static int cmd_plan(int argc, char **argv) {
    const char *programs = opt(argc, argv, "--programs", NULL), *pid = opt(argc, argv, "--program", NULL);
    const char *items = opt(argc, argv, "--items", NULL), *outp = opt(argc, argv, "--out", NULL);
    const char *now_s = opt(argc, argv, "--now", NULL), *engines_p = opt(argc, argv, "--engines", NULL);
    if (!programs || !pid || !items || !outp) return fail("plan needs --programs --program --items --out [--engines JSON] [--now EPOCH]");
    time_t now = now_s ? (time_t)strtoll(now_s, NULL, 10) : time(NULL);
    bf_jv *cfg = load_json(programs); if (!cfg) return fail("cannot read programs");
    bf_jv *prog = find_id(bf_jv_get(cfg, "programs"), pid); if (!prog) return fail("no such program");
    bf_jv *engines = engines_p ? load_json(engines_p) : discover(cfg); if (!engines) return fail("no engines");
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(items, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return fail("cannot open item archive");
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "WITH fam AS (SELECT value f FROM json_each(?1)), cen AS (SELECT value s FROM json_each(?5)), depth AS (SELECT publisher p, count(*) n FROM item WHERE kind='story' AND (?1='[]' OR family IN (SELECT f FROM fam)) AND (?5='[]' OR source IN (SELECT s FROM cen)) GROUP BY publisher) "
            "SELECT source,key,title,COALESCE(publisher,''),at,COALESCE(href,''),COALESCE(depth.n,1) FROM item "
            "LEFT JOIN depth ON depth.p=item.publisher "
            "WHERE kind='story' AND (?1='[]' OR family IN (SELECT f FROM fam)) AND (?5='[]' OR source IN (SELECT s FROM cen)) AND at>0 AND at<=?2 AND at>=?3 AND length(title)>12 "
            "GROUP BY lower(title) ORDER BY at DESC LIMIT ?4", -1, &st, NULL) != SQLITE_OK) return fail(sqlite3_errmsg(db));

    bf_jv *plan = bf_jv_obj(), *segs = bf_jv_arr(), *skipped = bf_jv_arr();
    bf_jv_set(plan, "schema", bf_jv_str("bonfyre.broadcast-plan.v2"));
    bf_jv_set(plan, "program", bf_jv_str(pid));
    bf_jv_set(plan, "title", bf_jv_str(jstr(prog, "title")));
    bf_jv_set(plan, "station", bf_jv_str(jstr(prog, "station")));
    bf_jv_set(plan, "fallback", bf_jv_str(jstr(prog, "fallback")));
    char stamp[32]; struct tm tmv; gmtime_r(&now, &tmv); strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tmv);
    bf_jv_set(plan, "planned_at", bf_jv_str(stamp));
    bf_jv_set(plan, "cast", cast_roles(prog, engines));
    bf_jv_set(plan, "engines", bf_jv_clone(engines));
    bf_jv *clock = bf_jv_get(prog, "clock");
    for (size_t i = 0; bf_jv_at(clock, i); i++) {
        bf_jv *seg = bf_jv_at(clock, i);
        bf_jv *s = bf_jv_clone(seg); bf_jv_set(s, "index", bf_jv_num((double)i));
        if (!strcmp(jstr(seg, "kind"), "desk")) {
            int count = (int)jnum(seg, "count", 5), age = (int)jnum(seg, "max_age_days", 60);
            bf_jv *voices = bf_jv_get(seg, "voices"), *lines = bf_jv_arr(); size_t nv = jlen(voices);
            const char *v0 = nv ? bf_jv_s(bf_jv_at(voices, 0)) : jstr(prog, "fallback");
            if (*jstr(seg, "intro")) bf_jv_push(lines, line(jstr(seg, "intro"), v0));
            sqlite3_reset(st);
            char fams[600]; bf_jv *fl = bf_jv_get(seg, "families");
            if (fl) { char *d = bf_jv_dump(fl, 0, 0, NULL); snprintf(fams, sizeof fams, "%s", d ? d : "[]"); free(d); }
            else if (*jstr(seg, "family")) { bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_arr(&w); bf_jsonw_str(&w, jstr(seg, "family")); bf_jsonw_arr_end(&w);
                   snprintf(fams, sizeof fams, "%.*s", (int)w.len, w.buf); bf_jsonw_free(&w); }
            else snprintf(fams, sizeof fams, "[]");
            sqlite3_bind_text(st, 1, fams, -1, SQLITE_TRANSIENT);
            /* A desk may read one declared census (the schemas/control source rows). */
            char *cens = NULL; const char *cname = jstr(seg, "census");
            if (*cname) { bf_jv *cl = bf_jv_get(bf_jv_get(cfg, "censuses"), cname); cens = cl ? bf_jv_dump(cl, 0, 0, NULL) : NULL; }
            sqlite3_bind_text(st, 5, cens ? cens : "[]", -1, SQLITE_TRANSIENT); free(cens);
            sqlite3_bind_int64(st, 2, (sqlite3_int64)now);
            sqlite3_bind_int64(st, 3, (sqlite3_int64)now - (sqlite3_int64)age * 86400);
            sqlite3_bind_int(st, 4, count * 40);
            /* Score each candidate: recency, plus how deep a publisher runs in
             * this family, so established outlets lead and the tail still
             * reaches air.  Two lines per publisher per desk. */
            typedef struct { char src[200], key[200], title[600], pub[128], href[600]; time_t at; double score; } Cand;
            int ncand = 0, cap = count * 40; Cand *cs = calloc((size_t)cap, sizeof *cs);
            while (cs && ncand < cap && sqlite3_step(st) == SQLITE_ROW) {
                if (recurring_column((const char *)sqlite3_column_text(st, 2))) continue;
                Cand *c = &cs[ncand++];
                snprintf(c->src, sizeof c->src, "%s", sqlite3_column_text(st, 0)); snprintf(c->key, sizeof c->key, "%s", sqlite3_column_text(st, 1));
                speakable((const char *)sqlite3_column_text(st, 2), c->title, sizeof c->title);
                snprintf(c->pub, sizeof c->pub, "%s", sqlite3_column_text(st, 3)); snprintf(c->href, sizeof c->href, "%s", sqlite3_column_text(st, 5));
                c->at = (time_t)sqlite3_column_int64(st, 4);
                double age = (double)(now - c->at) / 86400.0, depth = (double)sqlite3_column_int64(st, 6);
                c->score = -age + 9.0 * log2(1.0 + depth);
            }
            for (int x = 1; x < ncand; x++) { Cand t = cs[x]; int y = x - 1; while (y >= 0 && cs[y].score < t.score) { cs[y + 1] = cs[y]; y--; } cs[y + 1] = t; }
            int took = 0; char pubs[64][128]; int pubn[64] = {0}, npub = 0;
            for (int x = 0; x < ncand && took < count; x++) {
                Cand *c = &cs[x]; int k; for (k = 0; k < npub && strcmp(pubs[k], c->pub); k++) {}
                if (k == npub && npub < 64) { snprintf(pubs[npub], sizeof pubs[0], "%s", c->pub); npub++; }
                if (k < 64 && pubn[k] >= 2) continue;
                if (k < 64) pubn[k]++;
                char said[900]; struct tm a; gmtime_r(&c->at, &a);
                snprintf(said, sizeof said, "%s. From %s, %s %d.", c->title, *c->pub ? c->pub : "its publisher", months[a.tm_mon], a.tm_mday);
                const char *v = nv ? bf_jv_s(bf_jv_at(voices, (size_t)took % nv)) : v0;
                bf_jv *l = line(said, v);
                bf_jv_set(l, "source", bf_jv_str(c->src)); bf_jv_set(l, "key", bf_jv_str(c->key));
                bf_jv_set(l, "publisher", bf_jv_str(c->pub)); bf_jv_set(l, "href", bf_jv_str(c->href));
                bf_jv_set(l, "at", bf_jv_num((double)c->at));
                bf_jv_push(lines, l); took++;
            }
            free(cs);
            if (!took) {
                bf_jv *k = bf_jv_obj(); bf_jv_set(k, "index", bf_jv_num((double)i)); bf_jv_set(k, "family", bf_jv_get(seg, "families") ? bf_jv_clone(bf_jv_get(seg, "families")) : bf_jv_str(jstr(seg, "family")));
                bf_jv_set(k, "reason", bf_jv_str("no item of this family inside the desk's age window"));
                bf_jv_push(skipped, k); bf_jv_free(lines); bf_jv_free(s); continue;
            }
            bf_jv_set(s, "lines", lines);
        } else if (*jstr(seg, "say")) {
            bf_jv *lines = bf_jv_arr();
            bf_jv_push(lines, line(jstr(seg, "say"), *jstr(seg, "voice") ? jstr(seg, "voice") : jstr(prog, "fallback")));
            bf_jv_set(s, "lines", lines);
        }
        bf_jv_push(segs, s);
    }
    sqlite3_finalize(st); sqlite3_close(db);
    bf_jv_set(plan, "segments", segs); bf_jv_set(plan, "skipped", skipped);
    int rc = write_json(outp, plan) ? fail("cannot write plan") : 0;
    if (!rc) {
        size_t n; char *c = bf_jv_dump(bf_jv_get(plan, "cast"), 0, 1, &n);
        printf("{\"plan\":\"%s\",\"segments\":%zu,\"cast\":%s}\n", outp, jlen(segs), c); free(c);
    }
    bf_jv_free(plan); bf_jv_free(engines); bf_jv_free(cfg); return rc;
}

/* ---------- render ---------- */

typedef struct {
    char dir[1024], log[1100], narrate[1024], admission[1024];
    bf_jv *cfg, *plan, *engines, *used, *fallbacks, *credits, *skipped, *readbacks;
    int piece; int64_t grant; const char *grant_for; int neural_refused;
} Render;

static double duration_of(const char *path) {
    char *out = NULL;
    char *a[] = {"ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", (char *)path, NULL};
    double d = 0; if (run(a, &out, 256, NULL) == 0 && out) d = atof(out); free(out); return d;
}

static int to_pcm(Render *r, const char *in, const char *out) {
    char *a[] = {"ffmpeg", "-v", "error", "-y", "-i", (char *)in, "-ar", "44100", "-ac", "2", "-c:a", "pcm_s16le", (char *)out, NULL};
    return run(a, NULL, 0, r->log);
}

static int silence(Render *r, const char *out, double secs) {
    char t[32]; snprintf(t, sizeof t, "%.2f", secs);
    char *a[] = {"ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo", "-t", t, "-c:a", "pcm_s16le", (char *)out, NULL};
    return run(a, NULL, 0, r->log);
}

/* A neural load asks the memory ledger first and holds one grant for the run. */
static int neural_admit(Render *r, const bf_jv *eng) {
    if (r->grant > 0) return 0;
    if (r->neural_refused) return -1;
    BfAdmissionGrants *g = NULL;
    if (bf_admission_open(r->admission, &g, stderr)) { r->neural_refused = 1; return -1; }
    BfAdmissionPolicy pol; bf_admission_policy_memory(&pol);
    BfResourceRequest req = {.plane = "broadcast", .kind = "speech-model", .estimated_bytes = (int64_t)jnum(eng, "memory_bytes", 9e8),
                             .volume = BF_ADMISSION_MEMORY_VOLUME, .detail = jstr(eng, "id")};
    BfAdmissionDecision d; int64_t id = 0;
    BfAdmissionVerdict v = bf_admission_request_grant(g, &req, &pol, bf_admission_memory_available(), NULL, &d, &id);
    bf_admission_close(g);
    if (v != BF_ADMISSION_ADMIT) {
        bf_jv *n = bf_jv_obj(); bf_jv_set(n, "engine", bf_jv_str(jstr(eng, "id")));
        bf_jv_set(n, "verdict", bf_jv_str(bf_admission_verdict_name(v))); bf_jv_set(n, "reason", bf_jv_str(d.reason));
        bf_jv_push(r->fallbacks, n); r->neural_refused = 1; return -1;
    }
    r->grant = id; r->grant_for = jstr(eng, "id"); return 0;
}

static void neural_release(Render *r) {
    if (r->grant <= 0) return; BfAdmissionGrants *g = NULL;
    if (!bf_admission_open(r->admission, &g, stderr)) { bf_admission_release_grant(g, r->grant, NULL); bf_admission_close(g); }
    r->grant = 0;
}

/* Word recall of what whisper hears against what was meant. */
static double recall(const char *want, const char *heard) {
    char w[64][40], h[256][40]; int nw = 0, nh = 0;
    for (int pass = 0; pass < 2; pass++) {
        const char *s = pass ? heard : want; char tok[40]; int t = 0;
        for (const char *p = s;; p++) {
            if (*p && (isalnum((unsigned char)*p))) { if (t < 39) tok[t++] = (char)tolower((unsigned char)*p); continue; }
            if (t) { tok[t] = 0; if (pass && nh < 256) strcpy(h[nh++], tok); else if (!pass && nw < 64) strcpy(w[nw++], tok); t = 0; }
            if (!*p) break;
        }
    }
    if (!nw) return 1; int hit = 0;
    for (int i = 0; i < nw; i++) for (int j = 0; j < nh; j++) if (!strcmp(w[i], h[j])) { hit++; break; }
    return (double)hit / nw;
}

static double read_back(Render *r, const char *wav, const char *text, char *heard, size_t cap) {
    bf_jv *models = bf_jv_get(r->cfg, "readback"); char m[1000] = "";
    for (size_t i = 0; bf_jv_at(models, i); i++) { home_path(bf_jv_s(bf_jv_at(models, i)), m, sizeof m); if (!access(m, R_OK)) break; m[0] = 0; }
    if (!*m || !on_path("whisper-cli")) { snprintf(heard, cap, "(no read-back model)"); return -1; }
    char k16[1200]; snprintf(k16, sizeof k16, "%s.16k.wav", wav);
    char *a[] = {"ffmpeg", "-v", "error", "-y", "-i", (char *)wav, "-ar", "16000", "-ac", "1", k16, NULL};
    if (run(a, NULL, 0, r->log)) return -1;
    char *out = NULL; char *b[] = {"whisper-cli", "-m", m, "-f", k16, "-nt", "-np", NULL};
    int rc = run(b, &out, 65536, r->log); unlink(k16);
    if (rc || !out) { free(out); return -1; }
    speakable(out, heard, cap); free(out);
    return recall(text, heard);
}

static int engine_speak(Render *r, const bf_jv *eng, const char *text, const char *wav, double *score) {
    char work[1200], txt[1300]; snprintf(work, sizeof work, "%s/work/tts-%d", r->dir, ++r->piece);
    mkdir(work, 0755); snprintf(txt, sizeof txt, "%s/line.txt", work);
    if (spit(txt, text, strlen(text)) < 0) return -1;
    const char *kind = jstr(eng, "kind"); *score = -1;
    if (!is_true(bf_jv_get(eng, "ready"))) return -2;
    if (!strcmp(kind, "system")) {
        char outd[1300], art[1400]; snprintf(outd, sizeof outd, "%s/out", work); snprintf(art, sizeof art, "%s/artifact.wav", outd);
        char *a[] = {r->narrate, txt, outd, "--voice", (char *)jstr(eng, "voice"), "--rate", "178", "--title", "line", NULL};
        if (run(a, NULL, 0, r->log) || access(art, R_OK)) return -1;
        return to_pcm(r, art, wav);
    }
    if (!strcmp(kind, "neural")) {
        if (neural_admit(r, eng)) return -2;
        char got[1300]; snprintf(got, sizeof got, "%s/speech.wav", work);
        char *a[] = {"llama-tts", "-m", (char *)jstr(eng, "model_path"), "-mv", (char *)jstr(eng, "vocoder_path"),
                     "-p", (char *)text, "-o", got, NULL};
        if (run(a, NULL, 0, r->log) || access(got, R_OK)) return -1;
        char heard[2000]; double s = read_back(r, got, text, heard, sizeof heard); *score = s;
        bf_jv *rb = bf_jv_obj(); bf_jv_set(rb, "engine", bf_jv_str(jstr(eng, "id"))); bf_jv_set(rb, "text", bf_jv_str(text));
        bf_jv_set(rb, "heard", bf_jv_str(heard)); bf_jv_set(rb, "recall", bf_jv_num(s));
        int kept = s >= jnum(eng, "min_recall", 0.6); bf_jv_set(rb, "kept", bf_jv_bool(kept)); bf_jv_push(r->readbacks, rb);
        if (!kept) return -3;
        return to_pcm(r, got, wav);
    }
    if (!strcmp(kind, "provider")) {
        const char *kenv = jstr(eng, "key_env"); const char *key = *kenv ? getenv(kenv) : NULL;
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "model"); bf_jsonw_str(&w, jstr(eng, "model"));
        bf_jsonw_key(&w, "voice"); bf_jsonw_str(&w, jstr(eng, "voice"));
        bf_jsonw_key(&w, "input"); bf_jsonw_str(&w, text);
        bf_jsonw_key(&w, "response_format"); bf_jsonw_str(&w, "wav");
        bf_jsonw_obj_end(&w);
        char body[1400], got[1400], auth[700] = "X-Bonfyre: speech", data[1500];
        snprintf(body, sizeof body, "%s/request.json", work); snprintf(got, sizeof got, "%s/speech.wav", work);
        spit(body, w.buf, w.len); bf_jsonw_free(&w); snprintf(data, sizeof data, "@%s", body);
        if (key) snprintf(auth, sizeof auth, "Authorization: Bearer %s", key);
        char *a[] = {"curl", "-sf", "--max-time", "90", "-H", "Content-Type: application/json", "-H", auth,
                     "--data-binary", data, "-o", got, (char *)jstr(eng, "url"), NULL};
        if (run(a, NULL, 0, r->log)) return -2;
        return to_pcm(r, got, wav);
    }
    return -2;
}

/* Speak a role's line; fall back to the program's fallback role and record why. */
static const char *speak(Render *r, const char *role, const char *text, const char *wav) {
    bf_jv *cast = bf_jv_get(r->plan, "cast");
    const char *fallback_role = jstr(r->plan, "fallback");
    const char *tries[3] = {jstr(cast, role), jstr(cast, fallback_role), NULL};
    for (int t = 0; t < 2; t++) {
        if (!*tries[t] || (t && !strcmp(tries[0], tries[1]))) continue;
        bf_jv *eng = find_id(r->engines, tries[t]); double score;
        int rc = eng ? engine_speak(r, eng, text, wav, &score) : -2;
        if (!rc) {
            if (t) { bf_jv *n = bf_jv_obj(); bf_jv_set(n, "role", bf_jv_str(role)); bf_jv_set(n, "wanted", bf_jv_str(tries[0]));
                     bf_jv_set(n, "spoke", bf_jv_str(tries[1])); bf_jv_push(r->fallbacks, n); }
            if (!bf_jv_has(r->used, tries[t])) bf_jv_set(r->used, tries[t], bf_jv_bool(1));
            return tries[t];
        }
    }
    return NULL;
}

static double length_secs(const char *s) {
    if (!s || !*s) return 0; if (!strchr(s, ':')) return atof(s);
    double t = 0; for (const char *p = s; *p;) { t = t * 60 + atof(p); p = strchr(p, ':'); if (!p) break; p++; } return t;
}

static int excerpt(Render *r, const char *pool_id, int seconds, uint64_t seed, int from_start, const char *wav, bf_jv **credit_out) {
    bf_jv *pool = find_id(bf_jv_get(r->cfg, "pools"), pool_id); if (!pool) return -1;
    char q[2400], rows[16]; snprintf(q, sizeof q, "q=%s", jstr(pool, "query")); snprintf(rows, sizeof rows, "rows=%d", (int)jnum(pool, "rows", 50));
    char *out = NULL;
    char *a[] = {"curl", "-sfG", "--max-time", "40", "https://archive.org/advancedsearch.php", "--data-urlencode", q, "-d", rows,
                 "-d", "output=json", "-d", "fl[]=identifier", "-d", "fl[]=title", "-d", "fl[]=creator", "-d", "fl[]=licenseurl",
                 "--data-urlencode", "sort[]=downloads desc", NULL};
    if (run(a, &out, 4 << 20, r->log) || !out) { free(out); return -1; }
    bf_jv *res = bf_jv_parse(out, strlen(out), NULL, 0); free(out);
    bf_jv *docs = bf_jv_path(res, "response.docs"); size_t n = jlen(docs);
    int rc = -1;
    for (size_t tries = 0; n && tries < 10 && rc; tries++) {
        bf_jv *d = bf_jv_at(docs, (size_t)((seed + tries * 7919) % n));
        const char *id = jstr(d, "identifier"), *lic = jstr(d, "licenseurl");
        if (!open_terms(lic) || bf_jv_has(r->used, id)) continue;
        char murl[600]; snprintf(murl, sizeof murl, "https://archive.org/metadata/%s/files", id);
        char *m = NULL; char *b[] = {"curl", "-sf", "--max-time", "40", murl, NULL};
        if (run(b, &m, 8 << 20, r->log) || !m) { free(m); continue; }
        bf_jv *files = bf_jv_parse(m, strlen(m), NULL, 0); free(m);
        bf_jv *list = bf_jv_get(files, "result"); const char *pick = NULL; double plen = 0; size_t cands = 0;
        for (size_t k = 0; bf_jv_at(list, k); k++) {
            bf_jv *f = bf_jv_at(list, k); double len = length_secs(jstr(f, "length"));
            if (strstr(jstr(f, "format"), "MP3") && len >= seconds + 8) cands++;
        }
        size_t want = cands ? (size_t)((seed >> 9) % cands) : 0;
        for (size_t k = 0, c = 0; bf_jv_at(list, k) && !pick; k++) {
            bf_jv *f = bf_jv_at(list, k); double len = length_secs(jstr(f, "length"));
            if (!strstr(jstr(f, "format"), "MP3") || len < seconds + 8) continue;
            if (c++ == (from_start ? 0 : want)) { pick = jstr(f, "name"); plen = len; }
        }
        if (pick) {
            char enc[900]; size_t e = 0;
            for (const unsigned char *p = (const unsigned char *)pick; *p && e + 4 < sizeof enc; p++)
                e += (size_t)(isalnum(*p) || strchr("-_.~/", *p) ? snprintf(enc + e, 2, "%c", *p) : snprintf(enc + e, 4, "%%%02X", *p));
            char furl[1400], ss[32], tt[32], fade[200];
            snprintf(furl, sizeof furl, "https://archive.org/download/%s/%s", id, enc);
            double off = from_start ? 0 : plen * (0.2 + (double)(seed % 37) / 100.0);
            if (off + seconds > plen - 2) off = plen - seconds - 2; if (off < 0) off = 0;
            snprintf(ss, sizeof ss, "%.1f", off); snprintf(tt, sizeof tt, "%d", seconds);
            snprintf(fade, sizeof fade, "afade=t=in:d=1.5,afade=t=out:st=%.1f:d=2.5", seconds > 4 ? seconds - 2.5 : 0.5);
            char *c[] = {"ffmpeg", "-v", "error", "-y", "-ss", ss, "-t", tt, "-i", furl, "-ar", "44100", "-ac", "2",
                         "-af", fade, "-c:a", "pcm_s16le", (char *)wav, NULL};
            if (run(c, NULL, 0, r->log) == 0 && duration_of(wav) > seconds * 0.6) {
                char hex[65] = "", page[600]; bf_sha256_file(wav, hex);
                snprintf(page, sizeof page, "https://archive.org/details/%s", id);
                bf_jv *cr = bf_jv_obj(), *cre = bf_jv_get(d, "creator");
                bf_jv_set(cr, "identifier", bf_jv_str(id)); bf_jv_set(cr, "title", bf_jv_str(jstr(d, "title")));
                bf_jv_set(cr, "creator", bf_jv_str(bf_jv_is(cre, BF_JV_ARR) ? bf_jv_s(bf_jv_at(cre, 0)) : jstr(d, "creator")));
                bf_jv_set(cr, "licence", bf_jv_str(lic)); bf_jv_set(cr, "file", bf_jv_str(pick));
                bf_jv_set(cr, "url", bf_jv_str(furl)); bf_jv_set(cr, "page", bf_jv_str(page));
                bf_jv_set(cr, "offset_seconds", bf_jv_num(off)); bf_jv_set(cr, "seconds", bf_jv_num(seconds));
                bf_jv_set(cr, "pool", bf_jv_str(pool_id)); bf_jv_set(cr, "excerpt_sha256", bf_jv_str(hex));
                bf_jv_set(r->used, id, bf_jv_bool(1)); *credit_out = cr; rc = 0;
            }
        }
        bf_jv_free(files);
    }
    bf_jv_free(res); return rc;
}

static int over_bed(Render *r, const char *speech, const char *bed, const char *out) {
    char *a[] = {"ffmpeg", "-v", "error", "-y", "-i", (char *)speech, "-i", (char *)bed, "-filter_complex",
        "[0:a]adelay=2500|2500,apad=pad_dur=3,asplit=2[v][sc];[1:a]volume=0.55[b];"
        "[b][sc]sidechaincompress=threshold=0.02:ratio=12:attack=20:release=600[d];[d][v]amix=inputs=2:duration=first:normalize=0[m]",
        "-map", "[m]", "-ar", "44100", "-ac", "2", "-c:a", "pcm_s16le", (char *)out, NULL};
    return run(a, NULL, 0, r->log);
}

typedef struct { char path[1200]; double secs; } Piece;

static void vtt_time(FILE *f, double t) { int h = (int)t / 3600, m = ((int)t / 60) % 60; fprintf(f, "%02d:%02d:%06.3f", h, m, t - 3600 * h - 60 * m); }

static int cmd_render(int argc, char **argv) {
    const char *planp = argc > 2 ? argv[2] : NULL, *programs = opt(argc, argv, "--programs", NULL), *outd = opt(argc, argv, "--out", NULL);
    if (!planp || !programs || !outd) return fail("render PLAN --programs JSON --out DIR [--narrate BIN] [--admission-db DB]");
    Render r = {0}; snprintf(r.dir, sizeof r.dir, "%s", outd);
    snprintf(r.narrate, sizeof r.narrate, "%s", opt(argc, argv, "--narrate", "bonfyre-narrate"));
    const char *adb = opt(argc, argv, "--admission-db", getenv("BONFYRE_ADMISSION_DB"));
    home_path(adb && *adb ? adb : "~/.bonfyre/admission.db", r.admission, sizeof r.admission);
    mkdir(r.dir, 0755); char work[1100]; snprintf(work, sizeof work, "%s/work", r.dir); mkdir(work, 0755);
    snprintf(r.log, sizeof r.log, "%s/render.log", r.dir);
    r.cfg = load_json(programs); r.plan = load_json(planp);
    if (!r.cfg || !r.plan) return fail("cannot read plan or programs");
    r.engines = bf_jv_get(r.plan, "engines");
    r.used = bf_jv_obj(); r.fallbacks = bf_jv_arr(); r.credits = bf_jv_arr(); r.skipped = bf_jv_arr(); r.readbacks = bf_jv_arr();
    uint64_t seed0 = fnv(jstr(r.plan, "program"), fnv(jstr(r.plan, "planned_at"), 1469598103934665603ULL));
    Piece *pieces = calloc(4096, sizeof *pieces); int np = 0;
    bf_jv *chapters = bf_jv_arr(), *cues = bf_jv_arr();
    double clock = 0; char gap[1200], seggap[1200];
    snprintf(gap, sizeof gap, "%s/gap.wav", work); silence(&r, gap, 0.55);
    snprintf(seggap, sizeof seggap, "%s/seggap.wav", work); silence(&r, seggap, 1.1);
    double gaps = duration_of(gap), seggaps = duration_of(seggap);
#define ADD(p, s) do { snprintf(pieces[np].path, sizeof pieces[np].path, "%s", p); pieces[np].secs = (s); clock += (s); np++; } while (0)
#define CUE(a, d, txt, eng, ln) do { bf_jv *cue = bf_jv_obj(); bf_jv_set(cue, "start", bf_jv_num(a)); bf_jv_set(cue, "end", bf_jv_num((a) + (d))); \
        bf_jv_set(cue, "text", bf_jv_str(txt)); bf_jv_set(cue, "engine", bf_jv_str(eng)); \
        if ((ln) && *jstr(ln, "source")) { bf_jv_set(cue, "source", bf_jv_str(jstr(ln, "source"))); bf_jv_set(cue, "key", bf_jv_str(jstr(ln, "key"))); \
                                          bf_jv_set(cue, "href", bf_jv_str(jstr(ln, "href"))); } \
        bf_jv_push(cues, cue); } while (0)
    bf_jv *segs = bf_jv_get(r.plan, "segments");
    for (size_t i = 0; bf_jv_at(segs, i) && np < 4000; i++) {
        bf_jv *s = bf_jv_at(segs, i); const char *kind = jstr(s, "kind");
        double start = clock; const char *first_url = ""; int spoke = 0;
        bf_jv *lines = bf_jv_get(s, "lines");
        int bedded = *jstr(s, "bed") && (!strcmp(kind, "ident") || !strcmp(kind, "outro"));
        for (size_t k = 0; bf_jv_at(lines, k); k++) {
            bf_jv *l = bf_jv_at(lines, k); char wav[1200]; snprintf(wav, sizeof wav, "%s/line-%04d.wav", work, ++r.piece);
            const char *eng = speak(&r, jstr(l, "role"), jstr(l, "text"), wav);
            if (!eng) { bf_jv *x = bf_jv_obj(); bf_jv_set(x, "segment", bf_jv_num((double)i)); bf_jv_set(x, "line", bf_jv_str(jstr(l, "text")));
                        bf_jv_set(x, "reason", bf_jv_str("no cast engine could speak this line")); bf_jv_push(r.skipped, x); continue; }
            double d = duration_of(wav), at = clock;
            if (bedded) {
                char bed[1200], mixed[1200]; bf_jv *cr = NULL;
                snprintf(bed, sizeof bed, "%s/bed-%04d.wav", work, r.piece); snprintf(mixed, sizeof mixed, "%s/ident-%04d.wav", work, r.piece);
                if (!excerpt(&r, jstr(s, "bed"), (int)(d + 7.5), seed0 + i * 131, 0, bed, &cr) && !over_bed(&r, wav, bed, mixed)) {
                    bf_jv_set(cr, "segment", bf_jv_num((double)i)); bf_jv_set(cr, "use", bf_jv_str("bed")); bf_jv_set(cr, "start", bf_jv_num(clock));
                    bf_jv_push(r.credits, cr); at = clock + 2.5; ADD(mixed, duration_of(mixed));
                } else { if (cr) bf_jv_free(cr); ADD(wav, d); }
            } else { ADD(wav, d); ADD(gap, gaps); }
            CUE(at, d, jstr(l, "text"), eng, l);
            if (*jstr(l, "href") && !*first_url) first_url = jstr(l, "href");
            spoke++;
        }
        if (!strcmp(kind, "music") || !strcmp(kind, "feature")) {
            char wav[1200]; bf_jv *cr = NULL; snprintf(wav, sizeof wav, "%s/excerpt-%04d.wav", work, ++r.piece);
            int secs = (int)jnum(s, "seconds", 60);
            int from_start = !strcmp(kind, "feature") && strstr(jstr(s, "pool"), "librivox");
            if (!excerpt(&r, jstr(s, "pool"), secs, seed0 + i * 977, from_start, wav, &cr)) {
                bf_jv_set(cr, "segment", bf_jv_num((double)i)); bf_jv_set(cr, "use", bf_jv_str(kind)); bf_jv_set(cr, "start", bf_jv_num(clock));
                if (!*first_url) first_url = bf_jv_s(bf_jv_get(cr, "page"));
                ADD(wav, duration_of(wav)); bf_jv_push(r.credits, cr);
                /* Say who it was and on what terms, straight after. */
                char said[800], clean[800]; const char *who = bf_jv_s(bf_jv_get(cr, "creator"));
                snprintf(said, sizeof said, "That was %s%s%s, %s", bf_jv_s(bf_jv_get(cr, "title")), who && *who ? ", by " : "", who && *who ? who : "",
                         strstr(bf_jv_s(bf_jv_get(cr, "licence")), "publicdomain") ? "in the public domain" : "shared under a Creative Commons licence");
                speakable(said, clean, sizeof clean); strncat(clean, ".", sizeof clean - strlen(clean) - 1);
                const char *role = *jstr(s, "voice") ? jstr(s, "voice") : jstr(r.plan, "fallback");
                char sw[1200]; snprintf(sw, sizeof sw, "%s/credit-%04d.wav", work, ++r.piece);
                const char *eng = speak(&r, role, clean, sw);
                if (eng) { double d = duration_of(sw); ADD(gap, gaps); CUE(clock, d, clean, eng, (bf_jv *)NULL); ADD(sw, d); }
                spoke++;
            } else {
                bf_jv *x = bf_jv_obj(); bf_jv_set(x, "segment", bf_jv_num((double)i));
                bf_jv_set(x, "reason", bf_jv_str("no open-terms recording in the pool could be cut")); bf_jv_push(r.skipped, x);
            }
        }
        if (spoke) {
            bf_jv *ch = bf_jv_obj(); bf_jv_set(ch, "startTime", bf_jv_num((double)(int64_t)(start * 10) / 10.0));
            bf_jv_set(ch, "title", bf_jv_str(jstr(s, "title"))); if (*first_url) bf_jv_set(ch, "url", bf_jv_str(first_url));
            bf_jv_push(chapters, ch); ADD(seggap, seggaps);
        }
    }
    neural_release(&r);
    if (!np) return fail("nothing could be rendered");
    char list[1200], joined[1200], prog[1200], p[1200];
    snprintf(list, sizeof list, "%s/pieces.txt", work); snprintf(joined, sizeof joined, "%s/joined.wav", work);
    snprintf(prog, sizeof prog, "%s/program.m4a", r.dir);
    FILE *lf = fopen(list, "w"); for (int k = 0; k < np; k++) fprintf(lf, "file '%s'\n", pieces[k].path); fclose(lf);
    char *c1[] = {"ffmpeg", "-v", "error", "-y", "-f", "concat", "-safe", "0", "-i", list, "-c", "copy", joined, NULL};
    char *c2[] = {"ffmpeg", "-v", "error", "-y", "-i", joined, "-af", "loudnorm=I=-16:TP=-1.5:LRA=11", "-ar", "44100",
                  "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", prog, NULL};
    if (run(c1, NULL, 0, r.log) || run(c2, NULL, 0, r.log)) return fail("mixdown failed (see render.log)");
    double total = duration_of(prog);
    char waveform_peaks[BF_AUDIO_WAVEFORM_BINS*4+1] = "";
    int waveform_valid = bf_waveform_from_audio(prog,total,waveform_peaks,sizeof(waveform_peaks));
    if (!waveform_valid) fprintf(stderr,"[broadcast] acoustic peaks unavailable; retaining the standard seek slider\n");
    bf_jv *chd = bf_jv_obj(); bf_jv_set(chd, "version", bf_jv_str("1.2.0")); bf_jv_set(chd, "chapters", chapters);
    snprintf(p, sizeof p, "%s/chapters.json", r.dir); write_json(p, chd);
    snprintf(p, sizeof p, "%s/transcript.vtt", r.dir); FILE *vf = fopen(p, "w"); fputs("WEBVTT\n\n", vf);
    for (size_t k = 0; bf_jv_at(cues, k); k++) {
        bf_jv *cu = bf_jv_at(cues, k); vtt_time(vf, jnum(cu, "start", 0)); fputs(" --> ", vf); vtt_time(vf, jnum(cu, "end", 0));
        fprintf(vf, "\n%s\n\n", jstr(cu, "text"));
    }
    fclose(vf);
    char hex[65] = ""; bf_sha256_file(prog, hex);
    bf_jv *m = bf_jv_obj(), *voiced = bf_jv_arr();
    for (size_t e = 0; bf_jv_at(r.engines, e); e++) { bf_jv *en = bf_jv_at(r.engines, e); if (bf_jv_has(r.used, jstr(en, "id"))) {
        bf_jv *v = bf_jv_obj(); bf_jv_set(v, "id", bf_jv_str(jstr(en, "id"))); bf_jv_set(v, "kind", bf_jv_str(jstr(en, "kind")));
        bf_jv_set(v, "licence", bf_jv_str(jstr(en, "licence"))); bf_jv_push(voiced, v); } }
    bf_jv_set(m, "schema", bf_jv_str("bonfyre.broadcast-render.v2"));
    bf_jv_set(m, "program", bf_jv_str(jstr(r.plan, "program"))); bf_jv_set(m, "title", bf_jv_str(jstr(r.plan, "title")));
    bf_jv_set(m, "station", bf_jv_str(jstr(r.plan, "station"))); bf_jv_set(m, "planned_at", bf_jv_str(jstr(r.plan, "planned_at")));
    bf_jv_set(m, "audio", bf_jv_str("program.m4a")); bf_jv_set(m, "audio_sha256", bf_jv_str(hex));
    if (waveform_valid) {
        bf_jv_set(m, "waveform_peaks", bf_jv_str(waveform_peaks));
        bf_jv_set(m, "waveform_audio_sha256", bf_jv_str(hex));
    }
    bf_jv_set(m, "duration_ms", bf_jv_num((double)(int64_t)(total * 1000)));
    bf_jv_set(m, "chapters", bf_jv_str("chapters.json")); bf_jv_set(m, "transcript", bf_jv_str("transcript.vtt"));
    bf_jv_set(m, "cast", bf_jv_clone(bf_jv_get(r.plan, "cast"))); bf_jv_set(m, "voices", voiced);
    bf_jv_set(m, "lines", bf_jv_clone(cues)); bf_jv_set(m, "credits", r.credits); bf_jv_set(m, "readbacks", r.readbacks);
    bf_jv_set(m, "fallbacks", r.fallbacks); bf_jv_set(m, "skipped", r.skipped);
    if (bf_jv_get(r.plan, "skipped")) bf_jv_set(m, "plan_skipped", bf_jv_clone(bf_jv_get(r.plan, "skipped")));
    bf_jv_set(m, "cannot_infer", bf_jv_str("a spoken line repeats its item's headline and does not make the story true; "
        "an excerpt's licence is what its source stated when fetched, not a clearance; read-back recall measures whether the words "
        "can be heard, not voice quality; voices are synthetic"));
    snprintf(p, sizeof p, "%s/manifest.json", r.dir); write_json(p, m);
    printf("{\"audio\":\"%s\",\"seconds\":%.1f,\"lines\":%zu,\"excerpts\":%zu,\"fallbacks\":%zu,\"readbacks\":%zu,\"sha256\":\"%s\"}\n",
           prog, total, jlen(cues), jlen(r.credits), jlen(r.fallbacks), jlen(r.readbacks), hex);
    bf_jv_free(m); bf_jv_free(chd); bf_jv_free(cues); bf_jv_free(r.plan); bf_jv_free(r.used); bf_jv_free(r.cfg); free(pieces);
    return 0;
}

#include "editorial_mix.h"
#include "narration_assembly.h"
/* ---------- publish (draft only) ---------- */

static int cmd_publish(int argc, char **argv) {
    const char *dir = argc > 2 ? argv[2] : NULL, *cms = opt(argc, argv, "--cms", NULL), *ns = opt(argc, argv, "--namespace", "root");
    if (!dir || !cms) return fail("publish DIR --cms DB [--namespace NS]");
    char p[1200]; snprintf(p, sizeof p, "%s/manifest.json", dir); bf_jv *m = load_json(p); if (!m) return fail("no manifest");
    snprintf(p, sizeof p, "%s/chapters.json", dir); char *chapters = slurp(p, NULL);
    snprintf(p, sizeof p, "%s/transcript.vtt", dir); char *vtt = slurp(p, NULL);
    char rp[1200]; if (!realpath(dir, rp)) return fail("bad dir");
    char asset[1400], tref[1400], pid[400], subject[300];
    snprintf(asset, sizeof asset, "file://%s/program.m4a", rp); snprintf(tref, sizeof tref, "file://%s/transcript.vtt", rp);
    const char *peaks=jstr(m,"waveform_peaks"),*declared=jstr(m,"waveform_audio_sha256");
    if (*peaks) {
        char actual[65]="", recording[1200];
        snprintf(recording,sizeof recording,"%s/program.m4a",rp);
        if (!bf_waveform_valid(peaks) || strlen(declared)!=64 ||
            strcmp(declared,jstr(m,"audio_sha256")) ||
            bf_sha256_file(recording,actual)!=0 || strcmp(actual,declared)) {
            fprintf(stderr,"bonfyre-broadcast: waveform does not match the rendered audio; refusing its draft\n");
            free(chapters);free(vtt);bf_jv_free(m);return 1;
        }
    } else if (*declared) {
        fprintf(stderr,"bonfyre-broadcast: missing waveform samples for declared digest\n");
        free(chapters);free(vtt);bf_jv_free(m);return 1;
    }
    const char *source_ns=opt(argc,argv,"--source-namespace",NULL);
    const char *source_expected=opt(argc,argv,"--source-sha256",NULL);
    const char *source_subject=opt(argc,argv,"--subject",NULL);
    const char *article_expected=opt(argc,argv,"--article-revision-sha256",NULL);
    int source_bound=source_ns||source_expected||article_expected;
    char source_sha[65]="";
    if(source_bound){
        if(!source_ns||!*source_ns||(!source_expected&&!article_expected)||
           (source_expected&&article_expected)||
           (source_expected&&strlen(source_expected)!=64)||
           (article_expected&&(!bf_editorial_hex(article_expected)||!source_subject||strncmp(source_subject,"article:",8)))||
           !source_subject||
           (strncmp(source_subject,"article:",8)&&strncmp(source_subject,"post:",5)))
            return fail("source-bound audio requires --subject, --source-namespace and exact --source-sha256");
        sqlite3 *origin=NULL;
        if(sqlite3_open_v2(cms,&origin,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){
            if(origin)sqlite3_close(origin);return fail("source CMS is unavailable");
        }
        BfNarrationSource material;char article_sha[65]={0};
        int verified=bf_narration_digest_column(origin)&&
          (article_expected?(bf_narration_article_revision(origin,source_ns,source_subject,"published",article_sha)&&
                             !strcmp(article_sha,article_expected)):
            (bf_narration_source(origin,source_ns,source_subject,&material)&&!strcmp(material.sha,source_expected)));
        if(verified)snprintf(source_sha,sizeof(source_sha),"%s",article_expected?article_sha:material.sha);
        sqlite3_close(origin);
        if(!verified)return fail("the source is unpublished, unbound, or changed; no media draft staged");
    }
    int editorial=!strcmp(jstr(m,"schema"),"bonfyre.editorial-audio.v1");
    bf_jv *music=bf_jv_at(bf_jv_get(m,"credits"),0);
    const char *music_credit=jstr(music,"creator"),*music_license=jstr(music,"licence"),
        *music_href=jstr(music,"source_url"),*music_sha=jstr(music,"audio_sha256");
    BfEditorialProfile music_profile;
    if(article_expected&&!editorial)return fail("article revision requires an editorial mix");
    if(editorial&&(!source_bound||
       !bf_editorial_hex(jstr(m,article_expected?"narration_article_revision_sha256":"narration_source_sha256"))||
       strcmp(jstr(m,article_expected?"narration_article_revision_sha256":"narration_source_sha256"),article_expected?article_expected:source_expected)||
       !bf_editorial_profile(jstr(m,"editorial_profile"),&music_profile)||
       !bf_editorial_rights(music_license,music_credit,music_href)||
       !bf_editorial_hex(music_sha)||
       jnum(m,"narration_start_ms",-1)<500||
       jnum(m,"narration_start_ms",-1)>10000)){
        bf_jv_free(m);free(chapters);free(vtt);
        return fail("editorial audio requires current native source revision and explicit, retained music rights");
    }
    /* Asset staging is explicitly requested and never publishes the draft.
     * Local produced bytes are not foreign-observation receipts. */
    const char *asset_root=opt(argc,argv,"--asset-root",NULL);
    if(!asset_root)asset_root=getenv("BONFYRE_MEDIA_ASSET_ROOT");
    if(asset_root){
        char actual[65]="",href[1400]="",src[1200];
        snprintf(src,sizeof src,"%s/program.m4a",rp);
        if(strlen(jstr(m,"audio_sha256"))!=64||
           !bf_media_asset_stage(asset_root,src,"m4a",actual,href,sizeof href)||
           strcmp(actual,jstr(m,"audio_sha256"))){
            fprintf(stderr,"bonfyre-broadcast: rendered audio digest or staging mismatch\n");
            free(chapters);free(vtt);bf_jv_free(m);return 1;
        }
        snprintf(asset,sizeof asset,"%s",href);
        snprintf(src,sizeof src,"%s/transcript.vtt",rp);
        if(!bf_media_asset_stage(asset_root,src,"vtt",actual,href,sizeof href)){
            fprintf(stderr,"bonfyre-broadcast: transcript staging failed\n");
            free(chapters);free(vtt);bf_jv_free(m);return 1;
        }
        snprintf(tref,sizeof tref,"%s",href);
    }
    snprintf(pid, sizeof pid, "broadcast:%s:%.10s:%.12s", jstr(m, "program"), jstr(m, "planned_at"), jstr(m, "audio_sha256"));
    snprintf(subject, sizeof subject, "program:%s", jstr(m, "program"));
    const char *requested=opt(argc,argv,"--subject",NULL);
    if(requested){
        size_t size=strlen(requested);
        if(size<6||size>=sizeof(subject)||
           (strncmp(requested,"article:",8)&&strncmp(requested,"program:",8)&&
            strncmp(requested,"post:",5))){
            free(chapters);free(vtt);bf_jv_free(m);return fail("explicit media subject is invalid");
        }
        for(size_t i=0;i<size;i++)if(!(isalnum((unsigned char)requested[i])||
            requested[i]==':'||requested[i]=='-'||requested[i]=='_'||requested[i]=='.')){
            free(chapters);free(vtt);bf_jv_free(m);return fail("unsafe media subject");
        }
        snprintf(subject,sizeof subject,"%s",requested);
    }
    sqlite3 *db; if (sqlite3_open(cms, &db) != SQLITE_OK) return fail("cannot open cms");
    sqlite3_stmt *st=NULL;
    if(source_bound){
        BfNarrationSource current;char article_sha[65]={0};
        if(!bf_narration_digest_column(db)||
           sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK||
           (article_expected?(!bf_narration_article_revision(db,source_ns,subject,"published",article_sha)||strcmp(article_sha,source_sha)):
            (!bf_narration_source(db,source_ns,subject,&current)||strcmp(current.sha,source_sha)))){
            sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);sqlite3_close(db);
            return fail("published source changed before audio draft commit");
        }
    }
    const char *sql_legacy =
"INSERT INTO media_program(namespace,created_at,updated_at,status,program_id,subject_ref,media_kind,asset_ref,"
        "duration_ms,transcript_ref,chapters,captions,publication_state,cannot_infer,waveform_peaks,waveform_audio_sha256) "
        "VALUES(?1,strftime('%Y-%m-%dT%H:%M:%SZ','now'),"
        "strftime('%Y-%m-%dT%H:%M:%SZ','now'),'draft',?2,?3,'audio',?4,?5,?6,?7,?8,'draft',?9,?10,?11) "
        "ON CONFLICT(namespace,program_id) DO UPDATE SET updated_at=excluded.updated_at,asset_ref=excluded.asset_ref,"
        "transcript_ref=excluded.transcript_ref,duration_ms=excluded.duration_ms,chapters=excluded.chapters,"
        "captions=excluded.captions,waveform_peaks=excluded.waveform_peaks,"
        "waveform_audio_sha256=excluded.waveform_audio_sha256 "
        "WHERE media_program.subject_ref=excluded.subject_ref AND media_program.status='draft' "
        "AND media_program.publication_state='draft'";
    const char *sql_reviewed=
      "INSERT INTO media_program(namespace,created_at,updated_at,status,program_id,subject_ref,media_kind,asset_ref,"
      "duration_ms,transcript_ref,chapters,captions,publication_state,cannot_infer,waveform_peaks,waveform_audio_sha256,narration_source_sha256) "
      "VALUES(?1,strftime('%Y-%m-%dT%H:%M:%SZ','now'),"
      "strftime('%Y-%m-%dT%H:%M:%SZ','now'),'draft',?2,?3,'audio',?4,?5,?6,?7,?8,'draft',?9,?10,?11,?12) "
      "ON CONFLICT(namespace,program_id) DO UPDATE SET updated_at=excluded.updated_at,asset_ref=excluded.asset_ref,"
      "transcript_ref=excluded.transcript_ref,duration_ms=excluded.duration_ms,chapters=excluded.chapters,"
      "captions=excluded.captions,waveform_peaks=excluded.waveform_peaks,"
      "waveform_audio_sha256=excluded.waveform_audio_sha256,narration_source_sha256=excluded.narration_source_sha256 "
      "WHERE media_program.subject_ref=excluded.subject_ref AND media_program.status='draft' "
      "AND media_program.publication_state='draft'";
    const char *sql_editorial=
      "INSERT INTO media_program(namespace,created_at,updated_at,status,program_id,subject_ref,media_kind,asset_ref,"
      "duration_ms,transcript_ref,chapters,captions,publication_state,cannot_infer,waveform_peaks,waveform_audio_sha256,"
      "narration_source_sha256,editorial_profile,music_credit,music_license,music_source_url,music_source_sha256,narration_start_ms) "
      "VALUES(?1,strftime('%Y-%m-%dT%H:%M:%SZ','now'),"
      "strftime('%Y-%m-%dT%H:%M:%SZ','now'),'draft',?2,?3,'audio',?4,?5,?6,?7,?8,'draft',"
      "?9,?10,?11,?12,?13,?14,?15,?16,?17,?18) "
      "ON CONFLICT(namespace,program_id) DO UPDATE SET updated_at=excluded.updated_at,"
      "asset_ref=excluded.asset_ref,transcript_ref=excluded.transcript_ref,"
      "duration_ms=excluded.duration_ms,chapters=excluded.chapters,captions=excluded.captions,"
      "waveform_peaks=excluded.waveform_peaks,waveform_audio_sha256=excluded.waveform_audio_sha256,"
      "narration_source_sha256=excluded.narration_source_sha256,editorial_profile=excluded.editorial_profile,"
      "music_credit=excluded.music_credit,music_license=excluded.music_license,"
      "music_source_url=excluded.music_source_url,music_source_sha256=excluded.music_source_sha256,"
      "narration_start_ms=excluded.narration_start_ms "
      "WHERE media_program.subject_ref=excluded.subject_ref AND media_program.status='draft' "
      "AND media_program.publication_state='draft'";
    const char *sql_editorial_v2=
      "INSERT INTO media_program(namespace,created_at,updated_at,status,program_id,subject_ref,media_kind,asset_ref,"
      "duration_ms,transcript_ref,chapters,captions,publication_state,cannot_infer,waveform_peaks,waveform_audio_sha256,"
      "narration_source_sha256,editorial_profile,music_credit,music_license,music_source_url,music_source_sha256,narration_start_ms,narration_article_revision_sha256) "
      "VALUES(?1,strftime('%Y-%m-%dT%H:%M:%SZ','now'),"
      "strftime('%Y-%m-%dT%H:%M:%SZ','now'),'draft',?2,?3,'audio',?4,?5,?6,?7,?8,'draft',"
      "?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19) "
      "ON CONFLICT(namespace,program_id) DO UPDATE SET updated_at=excluded.updated_at,"
      "asset_ref=excluded.asset_ref,transcript_ref=excluded.transcript_ref,"
      "duration_ms=excluded.duration_ms,chapters=excluded.chapters,captions=excluded.captions,"
      "waveform_peaks=excluded.waveform_peaks,waveform_audio_sha256=excluded.waveform_audio_sha256,"
      "narration_source_sha256=excluded.narration_source_sha256,editorial_profile=excluded.editorial_profile,"
      "music_credit=excluded.music_credit,music_license=excluded.music_license,"
      "music_source_url=excluded.music_source_url,music_source_sha256=excluded.music_source_sha256,"
      "narration_start_ms=excluded.narration_start_ms,narration_article_revision_sha256=excluded.narration_article_revision_sha256 "
      "WHERE media_program.subject_ref=excluded.subject_ref AND media_program.status='draft' "
      "AND media_program.publication_state='draft'";
    const char *sql=article_expected?sql_editorial_v2:editorial?sql_editorial:source_bound?sql_reviewed:sql_legacy;
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK){
        if(source_bound)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
        fprintf(stderr,"%s\n",sqlite3_errmsg(db));sqlite3_close(db);return fail("CMS media program schema is unavailable");
    }
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, pid, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, subject, -1, SQLITE_STATIC); sqlite3_bind_text(st, 4, asset, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 5, (sqlite3_int64)jnum(m, "duration_ms", 0)); sqlite3_bind_text(st, 6, tref, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7, chapters ? chapters : "", -1, SQLITE_STATIC); sqlite3_bind_text(st, 8, vtt ? vtt : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 9, jstr(m, "cannot_infer"), -1, SQLITE_STATIC);
    sqlite3_bind_text(st,10,*peaks?peaks:"",-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,11,*peaks?declared:"",-1,SQLITE_TRANSIENT);
    if(source_bound)sqlite3_bind_text(st,12,article_expected?"":source_sha,-1,SQLITE_TRANSIENT);
    if(editorial){
        sqlite3_bind_text(st,13,music_profile.name,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,14,music_credit,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,15,music_license,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,16,music_href,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,17,music_sha,-1,SQLITE_TRANSIENT);
        sqlite3_bind_int64(st,18,(sqlite3_int64)jnum(m,"narration_start_ms",0));
        if(article_expected)sqlite3_bind_text(st,19,source_sha,-1,SQLITE_TRANSIENT);
    }
    int rc=sqlite3_step(st)==SQLITE_DONE&&sqlite3_changes(db)==1?0:1;
    sqlite3_finalize(st);
    if(source_bound){
        if(rc||sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){
            rc=1;sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
        }
    }
    if(rc) fprintf(stderr,"bonfyre-broadcast: refusing to overwrite another subject or published media program: %s\n",sqlite3_errmsg(db));
    else printf("{\"program_id\":\"%s\",\"subject_ref\":\"%s\",\"status\":\"draft\",\"asset_ref\":\"%s\"}\n",pid,subject,asset);
    sqlite3_close(db); free(chapters); free(vtt); bf_jv_free(m); return rc;
}

#include "narration_worklist.h"

static void usage(void) {
    fputs("bonfyre-broadcast -- produced audio programs from the estate's sources\n\n"
          "  bonfyre-broadcast engines --programs JSON\n"
          "  bonfyre-broadcast profiles  (native editorial sound profiles)\n"
          "  bonfyre-broadcast intake --cms DB [--namespace NS] [--subject article:ID|post:ID --out SCRIPT] [--only STATE] [--after-namespace NS --after-subject REF] [--limit 200]\n"
          "  bonfyre-broadcast plan --programs JSON --program ID --items DB --out PLAN [--engines JSON] [--now EPOCH]\n"
          "  bonfyre-broadcast render PLAN --programs JSON --out DIR [--narrate BIN] [--admission-db DB]\n"
          "  bonfyre-broadcast assemble-narration --plan JSON --out WAV --transcript VTT [--source-dir DIR]\n"
          "  bonfyre-broadcast mix-article --voice FILE --voice-sha256 HEX --bed FILE --bed-sha256 HEX --bed-license ID --bed-credit CREDIT --bed-source-url URL --transcript VTT --profile PROFILE --program ID --out DIR [--source-sha256 HEX | --article-revision-sha256 HEX]\n"
          "  bonfyre-broadcast publish DIR --cms DB [--namespace NS] [--subject article:ID|post:ID] [--source-namespace NS --source-sha256 HEX | --source-namespace NS --article-revision-sha256 HEX] [--asset-root DIR]   (draft only)\n", stderr);
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    enum { C_ENGINES, C_PLAN, C_PUBLISH, C_RENDER, C_INTAKE, C_MIX_ARTICLE, C_PROFILES, C_ASSEMBLE };
    static const BfCommandEntry cmds[] = {{"assemble-narration", C_ASSEMBLE, 0, NULL}, {"engines", C_ENGINES, 0, NULL},
                                          {"intake", C_INTAKE, 0, NULL}, {"mix-article", C_MIX_ARTICLE, 0, NULL},
                                          {"plan", C_PLAN, 0, NULL}, {"profiles", C_PROFILES, 0, NULL},
                                          {"publish", C_PUBLISH, 0, NULL}, {"render", C_RENDER, 0, NULL}};
    const BfCommandEntry *c = argc < 2 ? NULL : bf_command_lookup(cmds, 8, argv[1]);
    if (!c) { usage(); return 2; }
    switch (c->id) {
    case C_ENGINES: return cmd_engines(argc, argv);
    case C_PLAN: return cmd_plan(argc, argv);
    case C_RENDER: return cmd_render(argc, argv);
    case C_INTAKE: return cmd_intake(argc, argv);
    case C_MIX_ARTICLE: return cmd_mix_article(argc,argv);
    case C_ASSEMBLE: return cmd_assemble_narration(argc,argv);
    case C_PROFILES: return cmd_editorial_profiles();
    default: return cmd_publish(argc, argv);
    }
}
