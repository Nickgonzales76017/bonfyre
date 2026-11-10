/*
 * bonfyre-vectors: run command vectors against a native program (PLAN.md W4).
 *
 * A JS test that only spawns a binary and checks what came back is data, not
 * code. Its cases live as JSON beside the organ (ORGAN/tests/vectors/NAME.json)
 * and this one runner replays them:
 *
 *   bonfyre-vectors [--root DIR] FILE.json...
 *
 * File:  {"schema":"bonfyre.command-vectors.v1", "binary":"bonfyre-x",
 *         "binary_env":"OVERRIDE_VAR", "replaces":[...], "cases":[CASE...]}
 * CASE:  {"name":..., "files":{NAME:CONTENT}, "steps":[STEP...]}
 *        CONTENT is a string or {"prefix":s,"repeat":s,"count":n,"suffix":s}.
 *        Each case gets a fresh temporary directory ${tmp}.
 * STEP:  "argv":[...], "program" (default the binary), "env":{}, "stdin":s,
 *        "write":{NAME:CONTENT} (before running), and checks:
 *        "exit":n  "exit_nonzero":true  "stdout":s  "stderr":s  "stdout_file":path
 *        "contains":[s]  "excludes":[s]          (stdout substrings)
 *        "json":v           stdout parses to v (deep, key order ignored)
 *        "json_file":path   stdout parses to the JSON held in that file
 *        "first_line":true  parse only the first stdout line for json checks
 *        "at":{path:v}      value at path deep-equals v
 *        "has":{path:v}     array at path has an element equal to v, or the
 *                           string at path contains v
 *        "len":{path:n}     "min_len":{path:n}
 *        "in_file":{path:[file,path]}  every element at path equals some
 *                           element of the array at path in JSON file
 *        "present":[f] "absent":[f]   files after the step
 *        "for_each_jsonl":file  repeat the step once per line of a JSONL file
 *                           (at least one); "${item.K}" substitutes field K and
 *                           "json":"${item}" means stdout equals the line.
 * Paths are dotted ("owners.0.id"); "*" maps over an array ("owners.*.id");
 * "?k=v" picks the first array element whose field k is the string v.
 * ${tmp}, ${home}, ${organ} (the directory holding tests/vectors/) and ${root}
 * substitute in argv, env, program, file names and paths.
 *
 * A passing vector set says the program answered these recorded inputs the
 * way the retired JS assertions required; it says nothing about other inputs.
 *
 * Repair replay (bonfyre.repair-replay.v1):
 *   bonfyre-vectors [--root DIR] --replay-repair ID --case TEXT --before BIN
 *                   --after BIN [--before-source TEXT] [--runs N] FILE.json
 * replays FILE N times (default 3) against each binary through the file's
 * binary_env.  The repair replayed only if every before run fails a check of
 * the case named by TEXT, every after run passes the whole file, and each
 * side's outcome signature (the FAIL locations, messages dropped) is the same
 * across its runs.  Exit 0 replayed, 1 not, 2 usage.  The record says the
 * vector separates these two binaries; it does not say the binaries differ
 * only by the repair, or that the repair is a product.
 */
#include "bf_jv.h"
#include "bf_jsonw.h"
#include <bonfyre.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct { char *p; size_t n, cap; } buf;

static void bput(buf *b, const char *s, size_t n) {
    if (b->n + n + 1 > b->cap) {
        b->cap = (b->n + n + 1) * 2;
        b->p = realloc(b->p, b->cap);
        if (!b->p) { perror("realloc"); exit(3); }
    }
    memcpy(b->p + b->n, s, n);
    b->n += n;
    b->p[b->n] = 0;
}

static char *slurp(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    buf b = {0};
    char tmp[65536];
    size_t n;
    while ((n = fread(tmp, 1, sizeof tmp, f)) > 0) bput(&b, tmp, n);
    fclose(f);
    if (!b.p) bput(&b, "", 0);
    if (len) *len = b.n;
    return b.p;
}

static struct { const char *tmp, *organ, *root, *home; const bf_jv *item; } ctx;

/* Substitute ${tmp} ${organ} ${root} ${item.K}. Returns malloc'd string. */
static char *subst(const char *s, size_t len, size_t *out_len) {
    buf b = {0};
    bput(&b, "", 0);
    for (size_t i = 0; i < len;) {
        if (s[i] == '$' && i + 1 < len && s[i + 1] == '{') {
            const char *end = memchr(s + i, '}', len - i);
            if (end) {
                size_t kl = (size_t)(end - (s + i + 2));
                char key[256];
                if (kl < sizeof key) {
                    memcpy(key, s + i + 2, kl);
                    key[kl] = 0;
                    const char *rep = NULL;
                    char num[64];
                    if (!strcmp(key, "tmp")) rep = ctx.tmp;
                    else if (!strcmp(key, "organ")) rep = ctx.organ;
                    else if (!strcmp(key, "root")) rep = ctx.root;
                    else if (!strcmp(key, "home")) rep = ctx.home;
                    else if (!strncmp(key, "item.", 5) && ctx.item) {
                        const bf_jv *v = bf_jv_get(ctx.item, key + 5);
                        if (v && v->type == BF_JV_STR) { bput(&b, v->s, v->len); i = (size_t)(end - s) + 1; continue; }
                        if (v && v->type == BF_JV_NUM) { snprintf(num, sizeof num, "%.17g", v->n); rep = num; }
                    }
                    if (rep) { bput(&b, rep, strlen(rep)); i = (size_t)(end - s) + 1; continue; }
                }
            }
        }
        bput(&b, s + i, 1);
        i++;
    }
    if (out_len) *out_len = b.n;
    return b.p;
}

static char *sub_str(const bf_jv *v) { return v && v->type == BF_JV_STR ? subst(v->s, v->len, NULL) : NULL; }

static int write_content(const char *dir, const char *name, const bf_jv *content, char *why, size_t wsz) {
    size_t nl;
    char *n = subst(name, strlen(name), &nl);
    char path[PATH_MAX];
    if (n[0] == '/') snprintf(path, sizeof path, "%s", n);
    else snprintf(path, sizeof path, "%s/%s", dir, n);
    free(n);
    for (char *p = path + 1; *p; p++) if (*p == '/') { *p = 0; mkdir(path, 0755); *p = '/'; }
    FILE *f = fopen(path, "wb");
    if (!f) { snprintf(why, wsz, "cannot write %s: %s", path, strerror(errno)); return 0; }
    if (content->type == BF_JV_STR) {
        size_t l;
        char *s = subst(content->s, content->len, &l);
        fwrite(s, 1, l, f);
        free(s);
    } else if (content->type == BF_JV_OBJ) {
        const bf_jv *pre = bf_jv_get(content, "prefix"), *rep = bf_jv_get(content, "repeat"), *suf = bf_jv_get(content, "suffix");
        long count = (long)bf_jv_n(bf_jv_get(content, "count"), 0);
        if (pre) fwrite(pre->s, 1, pre->len, f);
        for (long k = 0; rep && k < count; k++) fwrite(rep->s, 1, rep->len, f);
        if (suf) fwrite(suf->s, 1, suf->len, f);
    }
    fclose(f);
    return 1;
}

/* Value(s) at a dotted path; "*" maps. Returns an owned clone or NULL. */
static bf_jv *select_path(const bf_jv *v, const char *path) {
    if (!v) return NULL;
    if (!*path) return bf_jv_clone(v);
    const char *dot = strchr(path, '.');
    size_t sl = dot ? (size_t)(dot - path) : strlen(path);
    const char *rest = dot ? dot + 1 : "";
    char seg[256];
    if (sl >= sizeof seg) return NULL;
    memcpy(seg, path, sl);
    seg[sl] = 0;
    if (!strcmp(seg, "*")) {
        if (v->type != BF_JV_ARR) return NULL;
        bf_jv *out = bf_jv_arr();
        for (size_t i = 0; i < v->len; i++) {
            bf_jv *x = select_path(v->items[i], rest);
            if (!x) { bf_jv_free(out); return NULL; }
            bf_jv_push(out, x);
        }
        return out;
    }
    if (seg[0] == '?' && v->type == BF_JV_ARR) {
        char *eq = strchr(seg, '=');
        if (!eq) return NULL;
        *eq = 0;
        for (size_t i = 0; i < v->len; i++) {
            const bf_jv *f = v->items[i]->type == BF_JV_OBJ ? bf_jv_get(v->items[i], seg + 1) : NULL;
            if (f && f->type == BF_JV_STR && !strcmp(f->s, eq + 1)) return select_path(v->items[i], rest);
        }
        return NULL;
    }
    if (v->type == BF_JV_ARR) {
        char *e;
        long idx = strtol(seg, &e, 10);
        if (*e || idx < 0 || (size_t)idx >= v->len) return NULL;
        return select_path(v->items[idx], rest);
    }
    if (v->type == BF_JV_OBJ) return select_path(bf_jv_get(v, seg), rest);
    return NULL;
}

typedef struct { int code; char *out, *err; size_t outn, errn; } result;

static int run_step(char **argv, const bf_jv *env, const char *stdin_s, size_t stdin_n, result *r) {
    char op[PATH_MAX], ep[PATH_MAX], ip[PATH_MAX];
    snprintf(op, sizeof op, "%s/.stdout", ctx.tmp);
    snprintf(ep, sizeof ep, "%s/.stderr", ctx.tmp);
    snprintf(ip, sizeof ip, "%s/.stdin", ctx.tmp);
    if (stdin_s) { FILE *f = fopen(ip, "wb"); if (f) { fwrite(stdin_s, 1, stdin_n, f); fclose(f); } }
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) return 0;
    if (pid == 0) {
        int in = open(stdin_s ? ip : "/dev/null", O_RDONLY);
        int o = open(op, O_WRONLY | O_CREAT | O_TRUNC, 0644), e = open(ep, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        dup2(in, 0); dup2(o, 1); dup2(e, 2);
        if (chdir(ctx.tmp) != 0) _exit(126);
        for (size_t i = 0; env && i < env->len; i++) {
            char *val = sub_str(env->items[i]);
            setenv(env->keys[i], val ? val : "", 1);
        }
        alarm(60);
        execvp(argv[0], argv);
        fprintf(stderr, "bonfyre-vectors: exec %s: %s\n", argv[0], strerror(errno));
        _exit(127);
    }
    int st;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    r->code = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st);
    r->out = slurp(op, &r->outn);
    r->err = slurp(ep, &r->errn);
    unlink(op); unlink(ep); unlink(ip);
    return 1;
}

static long checks_passed, checks_failed;

#define FAIL(...) do { char m_[1024]; snprintf(m_, sizeof m_, __VA_ARGS__); \
    printf("  FAIL %s / step %d: %s\n", label, stepno, m_); checks_failed++; ok = 0; } while (0)
#define PASS() (checks_passed++)

static char *dump(const bf_jv *v) { return v ? bf_jv_dump(v, 0, 0, NULL) : strdup("(missing)"); }

static int check_step(const char *label, int stepno, const bf_jv *s, const result *r) {
    int ok = 1;
    const bf_jv *x;
    if ((x = bf_jv_get(s, "exit"))) {
        if (r->code == (int)x->n) PASS(); else FAIL("exit %d, want %d; stderr: %.300s", r->code, (int)x->n, r->err);
    }
    if (bf_jv_get(s, "exit_nonzero")) {
        if (r->code != 0) PASS(); else FAIL("exit 0, want a refusal; stdout: %.300s", r->out);
    }
    if ((x = bf_jv_get(s, "stdout"))) {
        size_t l; char *w = subst(x->s, x->len, &l);
        if (l == r->outn && !memcmp(w, r->out, l)) PASS(); else FAIL("stdout %.200s, want %.200s", r->out, w);
        free(w);
    }
    if ((x = bf_jv_get(s, "stderr"))) {
        size_t l; char *w = subst(x->s, x->len, &l);
        if (l == r->errn && !memcmp(w, r->err, l)) PASS(); else FAIL("stderr %.200s, want %.200s", r->err, w);
        free(w);
    }
    if ((x = bf_jv_get(s, "stdout_file"))) {
        char *p = sub_str(x); size_t l; char *w = slurp(p, &l);
        if (w && l == r->outn && !memcmp(w, r->out, l)) PASS(); else FAIL("stdout differs from %s", p);
        free(p); free(w);
    }
    if ((x = bf_jv_get(s, "contains")))
        for (size_t i = 0; i < x->len; i++) {
            char *w = sub_str(x->items[i]);
            if (strstr(r->out, w)) PASS(); else FAIL("stdout lacks %.200s", w);
            free(w);
        }
    if ((x = bf_jv_get(s, "excludes")))
        for (size_t i = 0; i < x->len; i++) {
            char *w = sub_str(x->items[i]);
            if (!strstr(r->out, w)) PASS(); else FAIL("stdout has %.200s", w);
            free(w);
        }
    int wants_json = bf_jv_has(s, "json") || bf_jv_has(s, "at") || bf_jv_has(s, "has") || bf_jv_has(s, "len") ||
                     bf_jv_has(s, "min_len") || bf_jv_has(s, "in_file") || bf_jv_has(s, "json_file");
    if (wants_json) {
        size_t n = r->outn;
        if (bf_jv_get(s, "first_line")) { char *nl = memchr(r->out, '\n', r->outn); if (nl) n = (size_t)(nl - r->out); }
        char err[256] = "";
        bf_jv *got = bf_jv_parse(r->out, n, err, sizeof err);
        if (!got) { FAIL("stdout is not JSON (%s): %.200s", err, r->out); return 0; }
        if ((x = bf_jv_get(s, "json"))) {
            if (x->type == BF_JV_STR && !strcmp(x->s, "${item}") && ctx.item) x = ctx.item;
            if (bf_jv_equal(got, x)) PASS(); else { char *a = dump(got), *b = dump(x); FAIL("json %.300s, want %.300s", a, b); free(a); free(b); }
        }
        if ((x = bf_jv_get(s, "json_file"))) {
            char *fp = sub_str(x); size_t fl; char *ft = slurp(fp, &fl);
            bf_jv *want = ft ? bf_jv_parse(ft, fl, err, sizeof err) : NULL;
            if (want && bf_jv_equal(got, want)) PASS(); else FAIL("stdout JSON differs from %s", fp);
            bf_jv_free(want); free(ft); free(fp);
        }
        if ((x = bf_jv_get(s, "at")))
            for (size_t i = 0; i < x->len; i++) {
                bf_jv *v = select_path(got, x->keys[i]);
                if (v && bf_jv_equal(v, x->items[i])) PASS();
                else { char *a = dump(v), *b = dump(x->items[i]); FAIL("%s = %.300s, want %.300s", x->keys[i], a, b); free(a); free(b); }
                bf_jv_free(v);
            }
        if ((x = bf_jv_get(s, "has")))
            for (size_t i = 0; i < x->len; i++) {
                bf_jv *v = select_path(got, x->keys[i]);
                int found = 0;
                const bf_jv *want = x->items[i];
                if (v && v->type == BF_JV_ARR) { for (size_t k = 0; k < v->len; k++) if (bf_jv_equal(v->items[k], want)) found = 1; }
                else if (v && v->type == BF_JV_STR && want->type == BF_JV_STR) found = strstr(v->s, want->s) != NULL;
                if (found) PASS(); else { char *b = dump(want); FAIL("%s lacks %.300s", x->keys[i], b); free(b); }
                bf_jv_free(v);
            }
        if ((x = bf_jv_get(s, "len")))
            for (size_t i = 0; i < x->len; i++) {
                bf_jv *v = select_path(got, x->keys[i]);
                if (v && (v->type == BF_JV_ARR || v->type == BF_JV_STR) && v->len == (size_t)x->items[i]->n) PASS();
                else FAIL("%s length %zu, want %d", x->keys[i], v ? v->len : 0, (int)x->items[i]->n);
                bf_jv_free(v);
            }
        if ((x = bf_jv_get(s, "min_len")))
            for (size_t i = 0; i < x->len; i++) {
                bf_jv *v = select_path(got, x->keys[i]);
                if (v && v->type == BF_JV_ARR && v->len >= (size_t)x->items[i]->n) PASS();
                else FAIL("%s length %zu, want >= %d", x->keys[i], v ? v->len : 0, (int)x->items[i]->n);
                bf_jv_free(v);
            }
        if ((x = bf_jv_get(s, "in_file")))
            for (size_t i = 0; i < x->len; i++) {
                bf_jv *v = select_path(got, x->keys[i]);
                char *fp = sub_str(bf_jv_at(x->items[i], 0));
                const bf_jv *pp = bf_jv_at(x->items[i], 1);
                size_t fl; char *ft = slurp(fp, &fl);
                bf_jv *doc = ft ? bf_jv_parse(ft, fl, err, sizeof err) : NULL;
                bf_jv *pool = doc ? select_path(doc, pp ? pp->s : "") : NULL;
                int all = v && v->type == BF_JV_ARR && pool && pool->type == BF_JV_ARR;
                for (size_t k = 0; all && k < v->len; k++) {
                    int f = 0;
                    for (size_t j = 0; j < pool->len && !f; j++) f = bf_jv_equal(v->items[k], pool->items[j]);
                    if (!f) all = 0;
                }
                if (all) PASS(); else FAIL("%s has an element not in %s %s", x->keys[i], fp, pp ? pp->s : "");
                bf_jv_free(v); bf_jv_free(pool); bf_jv_free(doc); free(ft); free(fp);
            }
        bf_jv_free(got);
    }
    struct stat sb;
    if ((x = bf_jv_get(s, "present")))
        for (size_t i = 0; i < x->len; i++) {
            char *p = sub_str(x->items[i]);
            if (stat(p, &sb) == 0) PASS(); else FAIL("%s is missing", p);
            free(p);
        }
    if ((x = bf_jv_get(s, "absent")))
        for (size_t i = 0; i < x->len; i++) {
            char *p = sub_str(x->items[i]);
            if (stat(p, &sb) != 0) PASS(); else FAIL("%s exists", p);
            free(p);
        }
    return ok;
}

static int do_step(const char *label, int stepno, const bf_jv *s, const char *binary) {
    const bf_jv *x;
    char why[512];
    if ((x = bf_jv_get(s, "write")))
        for (size_t i = 0; i < x->len; i++)
            if (!write_content(ctx.tmp, x->keys[i], x->items[i], why, sizeof why)) { printf("  FAIL %s: %s\n", label, why); checks_failed++; return 0; }
    const bf_jv *args = bf_jv_get(s, "argv");
    size_t n = args ? args->len : 0;
    char **argv = calloc(n + 2, sizeof *argv);
    argv[0] = bf_jv_get(s, "program") ? sub_str(bf_jv_get(s, "program")) : strdup(binary);
    for (size_t i = 0; i < n; i++) {
        const bf_jv *a = args->items[i];
        if (a->type == BF_JV_STR) argv[i + 1] = sub_str(a);
        else if (a->type == BF_JV_OBJ) {   /* {"repeat":s,"count":n} for long arguments */
            const bf_jv *rep = bf_jv_get(a, "repeat");
            long count = (long)bf_jv_n(bf_jv_get(a, "count"), 0);
            buf b = {0}; bput(&b, "", 0);
            for (long k = 0; rep && k < count; k++) bput(&b, rep->s, rep->len);
            argv[i + 1] = b.p;
        } else argv[i + 1] = dump(a);
    }
    const bf_jv *in = bf_jv_get(s, "stdin");
    result r = {0};
    int ok = 0;
    if (run_step(argv, bf_jv_get(s, "env"), in ? in->s : NULL, in ? in->len : 0, &r)) ok = check_step(label, stepno, s, &r);
    else { printf("  FAIL %s / step %d: fork failed\n", label, stepno); checks_failed++; }
    for (size_t i = 0; i <= n; i++) free(argv[i]);
    free(argv); free(r.out); free(r.err);
    return ok;
}

static int run_file(const char *file) {
    size_t len;
    char *text = slurp(file, &len), err[256] = "";
    if (!text) { printf("FAIL %s: unreadable\n", file); return 0; }
    bf_jv *doc = bf_jv_parse(text, len, err, sizeof err);
    free(text);
    if (!doc || !bf_jv_get(doc, "schema") || strcmp(bf_jv_s(bf_jv_get(doc, "schema")), "bonfyre.command-vectors.v1")) {
        printf("FAIL %s: not a bonfyre.command-vectors.v1 file %s\n", file, err);
        bf_jv_free(doc);
        return 0;
    }
    char organ[PATH_MAX], *rp = realpath(file, NULL);
    snprintf(organ, sizeof organ, "%s", rp ? rp : file);
    free(rp);
    for (int up = 0; up < 3; up++) { char *sl = strrchr(organ, '/'); if (sl) *sl = 0; }
    ctx.organ = organ;
    char binary[PATH_MAX];
    const char *over = bf_jv_get(doc, "binary_env") ? getenv(bf_jv_s(bf_jv_get(doc, "binary_env"))) : NULL;
    const char *bin = bf_jv_s(bf_jv_get(doc, "binary"));
    if (over && *over) snprintf(binary, sizeof binary, "%s", over);
    else if (bin && bin[0] == '/') snprintf(binary, sizeof binary, "%s", bin);
    else snprintf(binary, sizeof binary, "%s/%s", organ, bin ? bin : "");
    const bf_jv *cases = bf_jv_get(doc, "cases");
    int cases_ok = 0, total = cases ? (int)cases->len : 0;
    long before = checks_failed;
    for (int c = 0; c < total; c++) {
        const bf_jv *cs = cases->items[c];
        char label[512];
        snprintf(label, sizeof label, "%s", bf_jv_s(bf_jv_get(cs, "name")) ? bf_jv_s(bf_jv_get(cs, "name")) : "?");
        char tmpl[] = "/tmp/bonfyre-vectors-XXXXXX";
        char *tmp = mkdtemp(tmpl);
        if (!tmp) { perror("mkdtemp"); exit(3); }
        char real_tmp[PATH_MAX];
        ctx.tmp = realpath(tmp, real_tmp) ? real_tmp : tmp;
        int ok = 1;
        char why[512];
        const bf_jv *files = bf_jv_get(cs, "files");
        for (size_t i = 0; files && i < files->len; i++)
            if (!write_content(ctx.tmp, files->keys[i], files->items[i], why, sizeof why)) { printf("  FAIL %s: %s\n", label, why); checks_failed++; ok = 0; }
        const bf_jv *steps = bf_jv_get(cs, "steps");
        for (size_t k = 0; ok && steps && k < steps->len; k++) {
            const bf_jv *s = steps->items[k];
            const bf_jv *each = bf_jv_get(s, "for_each_jsonl");
            if (!each) { if (!do_step(label, (int)k + 1, s, binary)) ok = 0; continue; }
            char *p = sub_str(each);
            size_t fl; char *ft = slurp(p, &fl);
            int lines = 0;
            for (char *line = ft, *nl; ft && line < ft + fl; line = nl + 1) {
                nl = memchr(line, '\n', (size_t)(ft + fl - line));
                if (!nl) nl = ft + fl;
                if (nl == line) continue;
                bf_jv *item = bf_jv_parse(line, (size_t)(nl - line), err, sizeof err);
                if (!item) { printf("  FAIL %s: bad JSONL line in %s\n", label, p); checks_failed++; ok = 0; break; }
                ctx.item = item;
                if (!do_step(label, (int)k + 1, s, binary)) ok = 0;
                ctx.item = NULL;
                bf_jv_free(item);
                lines++;
            }
            if (lines > 0) PASS(); else { printf("  FAIL %s: %s has no records\n", label, p); checks_failed++; ok = 0; }
            free(p); free(ft);
        }
        char cmd[PATH_MAX + 16];
        snprintf(cmd, sizeof cmd, "rm -rf '%s'", ctx.tmp);
        if (system(cmd) != 0) fprintf(stderr, "bonfyre-vectors: could not remove %s\n", ctx.tmp);
        if (ok) cases_ok++;
    }
    printf("%s %s: %d/%d cases\n", checks_failed == before ? "PASS" : "FAIL", file, cases_ok, total);
    bf_jv_free(doc);
    return checks_failed == before;
}


/* ---- repair replay ---------------------------------------------------- */

typedef struct {
    int all_target_fail, all_pass, identical;
    char sig[65], out_sha[65], bin_sha[65];
    char *failing;   /* signature text of the first run (FAIL lines, location only) */
} side_result;

/* Normalized signature: each "FAIL label / step N" location, message dropped. */
static char *fail_signature(const char *text, const char *case_text, int *hits_case) {
    buf b = {0};
    bput(&b, "", 0);
    *hits_case = 0;
    for (const char *line = text; line && *line;) {
        const char *nl = strchr(line, '\n');
        size_t n = nl ? (size_t)(nl - line) : strlen(line);
        if (n > 7 && !strncmp(line, "  FAIL ", 7)) {
            const char *stop = NULL;
            const char *st = strstr(line, " / step ");
            if (st && st < line + n) { stop = memchr(st, ':', (size_t)(line + n - st)); }
            else stop = memchr(line + 7, ':', n - 7);
            size_t k = stop ? (size_t)(stop - line) : n;
            bput(&b, line + 2, k - 2);
            bput(&b, "\n", 1);
            char lab[1024];
            size_t ln = k - 7 < sizeof lab - 1 ? k - 7 : sizeof lab - 1;
            memcpy(lab, line + 7, ln); lab[ln] = 0;
            if (strstr(lab, case_text)) *hits_case = 1;
        }
        line = nl ? nl + 1 : NULL;
    }
    return b.p;
}

static int replay_side(const char *file, const char *env_name, const char *bin, int runs,
                       const char *case_text, int want_fail, side_result *r) {
    memset(r, 0, sizeof *r);
    r->all_target_fail = r->all_pass = r->identical = 1;
    if (bf_sha256_file(bin, r->bin_sha) != 0) { r->all_target_fail = r->all_pass = 0; return 0; }
    for (int i = 0; i < runs; i++) {
        char tpath[] = "/tmp/bonfyre-replay-XXXXXX";
        int fd = mkstemp(tpath);
        if (fd < 0) { perror("mkstemp"); exit(3); }
        fflush(stdout);
        int saved = dup(1);
        dup2(fd, 1);
        setenv(env_name, bin, 1);
        long pb = checks_passed, fb = checks_failed;
        int ok = run_file(file);
        fflush(stdout);
        dup2(saved, 1); close(saved); close(fd);
        checks_passed = pb; checks_failed = fb;
        size_t n; char *text = slurp(tpath, &n);
        unlink(tpath);
        int hit = 0;
        char *sig = fail_signature(text ? text : "", case_text, &hit);
        char sh[65];
        bf_sha256_hex((const uint8_t *)sig, strlen(sig), sh);
        if (i == 0) { memcpy(r->sig, sh, 65); r->failing = sig; }
        else { if (strcmp(sh, r->sig)) r->identical = 0; free(sig); }
        if (!(want_fail && !ok && hit)) r->all_target_fail = 0;
        if (!ok) r->all_pass = 0;
        free(text);
    }
    unsetenv(env_name);
    return 1;
}

static void put_side(bf_jsonw *w, const char *bin, const char *source, const side_result *r, int before) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "binary"); bf_jsonw_str(w, bin);
    bf_jsonw_key(w, "binary_sha256"); bf_jsonw_str(w, r->bin_sha);
    if (source) { bf_jsonw_key(w, "source"); bf_jsonw_str(w, source); }
    if (before) { bf_jsonw_key(w, "every_run_fails_case"); bf_jsonw_bool(w, r->all_target_fail); }
    else { bf_jsonw_key(w, "every_run_passes"); bf_jsonw_bool(w, r->all_pass); }
    bf_jsonw_key(w, "signatures_identical"); bf_jsonw_bool(w, r->identical);
    bf_jsonw_key(w, "signature_sha256"); bf_jsonw_str(w, r->sig);
    bf_jsonw_key(w, "failing_checks"); bf_jsonw_arr(w);
    for (const char *l = r->failing; l && *l;) {
        const char *nl = strchr(l, '\n');
        size_t n = nl ? (size_t)(nl - l) : strlen(l);
        if (n) bf_jsonw_strn(w, l, n);
        l = nl ? nl + 1 : NULL;
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_obj_end(w);
}

static int replay_repair(const char *id, const char *case_text, const char *before, const char *after,
                         const char *before_source, int runs, const char *file) {
    size_t len;
    char *text = slurp(file, &len), err[256] = "";
    bf_jv *doc = text ? bf_jv_parse(text, len, err, sizeof err) : NULL;
    const char *env_name = doc ? bf_jv_s(bf_jv_get(doc, "binary_env")) : NULL;
    if (!env_name || !*env_name) {
        fprintf(stderr, "bonfyre-vectors: %s names no binary_env; a repair cannot be replayed against two binaries\n", file);
        free(text); bf_jv_free(doc); return 2;
    }
    char vsha[65];
    bf_sha256_hex((const uint8_t *)text, len, vsha);
    char env_copy[256];
    snprintf(env_copy, sizeof env_copy, "%s", env_name);
    /* Steps run inside the case directory, so a relative binary would name nothing. */
    char before_abs[PATH_MAX], after_abs[PATH_MAX];
    if (!realpath(before, before_abs)) snprintf(before_abs, sizeof before_abs, "%s", before);
    if (!realpath(after, after_abs)) snprintf(after_abs, sizeof after_abs, "%s", after);
    before = before_abs; after = after_abs;
    side_result b, a;
    int okb = replay_side(file, env_copy, before, runs, case_text, 1, &b);
    int oka = replay_side(file, env_copy, after, runs, case_text, 0, &a);
    int replayed = okb && oka && b.all_target_fail && b.identical && a.all_pass && a.identical
                   && strcmp(b.bin_sha, a.bin_sha) != 0;
    bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.repair-replay.v1");
    bf_jsonw_key(&w, "repair_id"); bf_jsonw_str(&w, id);
    bf_jsonw_key(&w, "vector_file"); bf_jsonw_str(&w, file);
    bf_jsonw_key(&w, "vector_sha256"); bf_jsonw_str(&w, vsha);
    bf_jsonw_key(&w, "case"); bf_jsonw_str(&w, case_text);
    bf_jsonw_key(&w, "binary_env"); bf_jsonw_str(&w, env_copy);
    bf_jsonw_key(&w, "runs"); bf_jsonw_int(&w, runs);
    bf_jsonw_key(&w, "before"); put_side(&w, before, before_source, &b, 1);
    bf_jsonw_key(&w, "after"); put_side(&w, after, NULL, &a, 0);
    bf_jsonw_key(&w, "reasons"); bf_jsonw_arr(&w);
    if (!okb) bf_jsonw_str(&w, "before binary unreadable");
    if (!oka) bf_jsonw_str(&w, "after binary unreadable");
    if (okb && !b.all_target_fail) bf_jsonw_str(&w, "a before run did not fail the named case");
    if (okb && !b.identical) bf_jsonw_str(&w, "before runs disagree");
    if (oka && !a.all_pass) bf_jsonw_str(&w, "an after run did not pass the whole file");
    if (oka && !a.identical) bf_jsonw_str(&w, "after runs disagree");
    if (okb && oka && !strcmp(b.bin_sha, a.bin_sha)) bf_jsonw_str(&w, "before and after are the same bytes");
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "verdict"); bf_jsonw_str(&w, replayed ? "replayed" : "not-replayed");
    bf_jsonw_obj_end(&w);
    printf("%s\n", w.buf);
    bf_jsonw_free(&w);
    free(b.failing); free(a.failing); free(text); bf_jv_free(doc);
    return replayed ? 0 : 1;
}

int main(int argc, char **argv) {
    char cwd[PATH_MAX];
    ctx.root = getcwd(cwd, sizeof cwd);
    ctx.home = getenv("HOME") ? getenv("HOME") : "/";
    int i = 1, ok = 1, files = 0;
    static char root_abs[PATH_MAX];
    if (i + 1 < argc && !strcmp(argv[i], "--root")) { ctx.root = realpath(argv[i + 1], root_abs) ? root_abs : argv[i + 1]; i += 2; }
    if (i < argc && !strcmp(argv[i], "--replay-repair")) {
        const char *id = NULL, *cs = NULL, *bb = NULL, *ab = NULL, *src = NULL, *file = NULL;
        int runs = 3;
        for (; i < argc; i++) {
            if (!strcmp(argv[i], "--replay-repair") && i + 1 < argc) id = argv[++i];
            else if (!strcmp(argv[i], "--case") && i + 1 < argc) cs = argv[++i];
            else if (!strcmp(argv[i], "--before") && i + 1 < argc) bb = argv[++i];
            else if (!strcmp(argv[i], "--after") && i + 1 < argc) ab = argv[++i];
            else if (!strcmp(argv[i], "--before-source") && i + 1 < argc) src = argv[++i];
            else if (!strcmp(argv[i], "--runs") && i + 1 < argc) runs = atoi(argv[++i]);
            else if (!file && argv[i][0] != '-') file = argv[i];
            else { fprintf(stderr, "bonfyre-vectors: unexpected %s\n", argv[i]); return 2; }
        }
        if (!id || !cs || !*cs || !bb || !ab || !file || runs < 2 || runs > 20) {
            fprintf(stderr, "usage: bonfyre-vectors [--root DIR] --replay-repair ID --case TEXT --before BIN --after BIN [--before-source TEXT] [--runs 2..20] FILE.json\n");
            return 2;
        }
        return replay_repair(id, cs, bb, ab, src, runs, file);
    }
    if (i >= argc || !strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
        printf("usage: bonfyre-vectors [--root DIR] FILE.json...\nReplays command vectors (bonfyre.command-vectors.v1) against native programs.\n");
        return i >= argc ? 2 : 0;
    }
    for (; i < argc; i++, files++) if (!run_file(argv[i])) ok = 0;
    printf("vectors: %d file(s), %ld checks passed, %ld failed\n", files, checks_passed, checks_failed);
    return ok ? 0 : 1;
}
