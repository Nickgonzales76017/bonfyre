/* bonfyre-repository-fabric -- one verb surface over every git host Aurekai
 * operates or composes, so a caller never needs to know which forge answers.
 *
 *   bonfyre-repository-fabric connect  --provider P
 *   bonfyre-repository-fabric provision --provider P --org O --repo R [--private 0|1] [--description D]
 *   bonfyre-repository-fabric resolve  --provider P --org O --repo R
 *   bonfyre-repository-fabric view     --provider P --org O --repo R [--path PATH] [--ref REF]
 *   bonfyre-repository-fabric checkout --provider P --org O --repo R --dest DIR [--ref REF] [--filter JOSH_FILTER]
 *   bonfyre-repository-fabric change   --provider P --org O --repo R --path PATH --message M --content-file F [--branch B]
 *   bonfyre-repository-fabric observe  --provider P --org O --repo R [--limit N]
 *   bonfyre-repository-fabric service   --provider P --org O --repo R --action list|add [--url HOOK] [--events e1,e2]
 *
 * Today's real backend is Forgejo, self-hosted at BONFYRE_FORGEJO_API
 * (default http://127.0.0.1:3400), its credentials held exclusively by
 * BonfyreCredentials under the "forgejo" provider -- this command never
 * touches the sealed store directly, it shells out to bonfyre-credentials
 * for every header and never places a secret in argv, an env var passed to
 * a logged process, or a file wider than 0600. "josh" is registered as a
 * provider name for repository.connect: it answers whether the
 * josh-proxy/josh binaries exist yet, honestly, without fabricating
 * readiness that is not built. When checkout --filter is given a JOSH
 * filter spec (e.g. ":/subdir"), checkout clones through the real josh
 * binary instead of plain git, re-rooting the checkout at that subtree;
 * josh's own git fetch needs git >= 2.41, so a Homebrew git ahead of the
 * macOS system git is added to the child's PATH only when present.
 *
 * Every reply is one JSON envelope written with bf_jsonw: verb, provider,
 * ok, http_status, and (when the backend answered) data -- the backend's
 * own parsed JSON, passed through rather than re-described field by field,
 * so nothing here can silently drop or invent a field the forge returned.
 */
#define _XOPEN_SOURCE 700
#include <bf_json.h>
#include <bf_jsonw.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define OUT_CAP (4 * 1024 * 1024)

/* ------------------------------------------------------------- helpers */

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}
static int flag_true(const char *v, int def) {
    if (!v || !*v) return def;
    return !strcmp(v, "1") || !strcmp(v, "true") || !strcmp(v, "yes");
}
/* Org/repo/path/branch names that may reach a URL: no credentials, no
 * escapes, no whitespace. Repository paths may contain '/'. */
static int ident_ok(const char *s, int allow_slash) {
    size_t n = s ? strlen(s) : 0;
    if (!n || n > 400) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (isalnum(c) || c == '-' || c == '_' || c == '.') continue;
        if (allow_slash && c == '/') continue;
        return 0;
    }
    /* Reject URL dot-segment normalization before any provider request.
     * The rule applies equally to orgs, repository names and file paths. */
    size_t start = 0;
    for (size_t i = 0; i <= n; i++) {
        if (i < n && s[i] != '/') continue;
        size_t len = i - start;
        if (!len || (len == 1 && s[start] == '.') ||
            (len == 2 && s[start] == '.' && s[start + 1] == '.')) return 0;
        start = i + 1;
    }
    return 1;
}
/* A JOSH filter spec (e.g. ":/subdir", ":workspace=path.josh"): starts
 * with ':' and carries only the characters JOSH's own filter grammar uses. */
static int filter_ok(const char *s) {
    size_t n = s ? strlen(s) : 0;
    if (!n || n > 400 || s[0] != ':') return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (isalnum(c) || strchr("-_./:=,", c)) continue;
        return 0;
    }
    return 1;
}
/* The josh-cli crate's cargo output binary is named "josh", not "josh-cli". */
static int find_josh_cli(char *out, size_t cap) {
    const char *home = getenv("HOME");
    snprintf(out, cap, "%s/.bonfyre/vendor/josh-src/target/release/josh", home ? home : "");
    struct stat st;
    return stat(out, &st) == 0;
}
static void fail_json(const char *verb, const char *why) {
    bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, verb);
    bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "error"); bf_jsonw_str(&w, why);
    bf_jsonw_obj_end(&w);
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout);
    bf_jsonw_free(&w);
}

/* ------------------------------------------------------ credential shell */

typedef struct { char bin[PATH_MAX], db[PATH_MAX], key[PATH_MAX], providers[PATH_MAX]; } Creds;

static int creds_paths(Creds *c,const char *argv0) {
    const char *home = getenv("HOME");
    const char *bin = getenv("BONFYRE_CREDENTIALS_BINARY");
    const char *db = getenv("BONFYRE_CREDENTIALS_DB");
    const char *key = getenv("BONFYRE_CREDENTIALS_KEY");
    const char *providers = getenv("BONFYRE_CREDENTIALS_PROVIDERS");
    const char *root = getenv("BONFYRE_PROJECT_ROOT");
    struct stat st;
    if (!home || *home != '/') return 0;
    if (bin && *bin == '/') snprintf(c->bin, sizeof(c->bin), "%s", bin);
    else snprintf(c->bin, sizeof(c->bin), "%s/.bonfyre/bin/bonfyre-credentials", home);
    if (db && *db == '/') snprintf(c->db, sizeof(c->db), "%s", db);
    else snprintf(c->db, sizeof(c->db), "%s/.bonfyre/credentials/credentials.db", home);
    if (key && *key == '/') snprintf(c->key, sizeof(c->key), "%s", key);
    else snprintf(c->key, sizeof(c->key), "%s/.bonfyre/credentials/credentials.key", home);
    if (providers && *providers == '/') snprintf(c->providers, sizeof(c->providers), "%s", providers);
    else if (root && *root == '/') snprintf(c->providers, sizeof(c->providers), "%s/cmd/BonfyreCredentials/provider-credentials.json", root);
    else {
        /* A tool runner has a real checkout binary but no project-root env.
         * Locate its canonical provider descriptor from that executable;
         * fall back to the conventional local checkout for managed tools. */
        const char *marker = argv0&&*argv0=='/'?strstr(argv0,"/cmd/BonfyreRepositoryFabric/"):NULL;
        if(marker&&marker>argv0&&(size_t)(marker-argv0)<PATH_MAX/2)
            snprintf(c->providers,sizeof(c->providers),"%.*s/cmd/BonfyreCredentials/provider-credentials.json",(int)(marker-argv0),argv0);
        else snprintf(c->providers,sizeof(c->providers),"%s/Developer/Bonfyre/cmd/BonfyreCredentials/provider-credentials.json",home);
    }
    return stat(c->bin, &st) == 0 && stat(c->db, &st) == 0 && stat(c->key, &st) == 0 && stat(c->providers, &st) == 0;
}

/* Run bonfyre-credentials (argv[0] must be its real absolute path, as
 * resolved by creds_paths) and capture its stdout. The secret never
 * appears on this process's argv or in any file; it only ever travels
 * inside the child's own memory and this pipe. */
static int run_credentials(char *const full_argv[], char *out, size_t cap) {
    int outp[2]; pid_t pid;
    if (pipe(outp)) return -1;
    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        dup2(outp[1], 1); if (devnull >= 0) dup2(devnull, 2); close(outp[0]);
        execv(full_argv[0], full_argv);
        _exit(127);
    }
    close(outp[1]);
    size_t got = 0; ssize_t k;
    while (got + 1 < cap && (k = read(outp[0], out + got, cap - 1 - got)) > 0) got += (size_t)k;
    out[got] = 0; close(outp[0]);
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static int apply_header(const Creds *c, const char *provider, const char *url, char *auth, size_t cap) {
    char out[8192];
    char *argv_[] = {(char *)c->bin, (char *)"apply", (char *)"--db", (char *)c->db, (char *)"--key", (char *)c->key,
                     (char *)"--providers", (char *)c->providers, (char *)"--provider", (char *)provider,
                     (char *)"--url", (char *)url, NULL};
    /* run_credentials execs argv_[0] directly (a real path), so no PATH lookup is needed. */
    run_credentials(argv_, out, sizeof(out));
    char err[8];
    bf_json_doc_t *doc = bf_json_parse(out, strlen(out), err, sizeof(err));
    if (!doc) return 0;
    const bf_json_node_t *root = bf_json_root(doc), *headers = root ? bf_json_obj_get(doc, root, "headers") : NULL;
    int found = 0;
    /* Each header arrives as one "Name: value" string. */
    if (headers) for (const bf_json_node_t *h = bf_json_child_first(doc, headers); h; h = bf_json_child_next(doc, h)) {
        char line[1024];
        if (!bf_json_get_str_copy(h, line, sizeof(line))) continue;
        char *colon = strchr(line, ':');
        if (!colon) continue;
        *colon = 0; char *value = colon + 1; while (*value == ' ') value++;
        if (!strcasecmp(line, "Authorization")) { snprintf(auth, cap, "%s", value); found = 1; }
    }
    bf_json_free(doc);
    return found;
}

static int run_verify(const Creds *c, const char *provider, char *out, size_t cap) {
    char *argv_[] = {(char *)c->bin, (char *)"verify", (char *)"--db", (char *)c->db, (char *)"--key", (char *)c->key,
                     (char *)"--providers", (char *)c->providers, (char *)"--provider", (char *)provider, NULL};
    return run_credentials(argv_ + 0, out, cap) >= 0;
}

/* ------------------------------------------------------------ http verb */

static char *forgejo_api(void) {
    static char buf[256]; const char *v = getenv("BONFYRE_FORGEJO_API");
    snprintf(buf, sizeof(buf), "%s", (v && *v) ? v : "http://127.0.0.1:3400");
    return buf;
}

static char *tmp_body_file(const char *json, char *path_out, size_t cap) {
    const char *home = getenv("HOME"); char dir[PATH_MAX]; int fd;
    if (!home) return NULL;
    snprintf(dir, sizeof(dir), "%s/.bonfyre/tmp", home); mkdir(dir, 0700);
    snprintf(path_out, cap, "%s/.bonfyre/tmp/repofabric-XXXXXX", home);
    fd = mkstemp(path_out);
    if (fd < 0) return NULL;
    fchmod(fd, 0600);
    size_t n = strlen(json);
    ssize_t w = write(fd, json, n);
    close(fd);
    if (w != (ssize_t)n) { unlink(path_out); return NULL; }
    return path_out;
}

/* One HTTP call through curl, config delivered on stdin so the bearer
 * token never reaches argv. The body (if any) lives in a 0600 temp file,
 * unlinked right after the call. Reply body and HTTP status share one
 * pipe; a marker line separates them because write-out always runs last. */
static int http_call(const char *method, const char *url, const char *auth, const char *body_json,
                      char *out_body, size_t out_cap, int *status_out) {
    int in[2], outp[2]; pid_t pid; char bodypath[PATH_MAX] = "";
    *status_out = 0; out_body[0] = 0;
    if (body_json && !tmp_body_file(body_json, bodypath, sizeof(bodypath))) return -1;
    if (pipe(in) || pipe(outp)) return -1;
    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        dup2(in[0], 0); dup2(outp[1], 1);
        int devnull = open("/dev/null", O_WRONLY); if (devnull >= 0) dup2(devnull, 2);
        close(in[1]); close(outp[0]);
        execlp("curl", "curl", "--config", "-", NULL);
        _exit(127);
    }
    close(in[0]); close(outp[1]);
    FILE *cfg = fdopen(in[1], "w");
    fputs("silent\nmax-time = 30\nuser-agent = \"BonfyreProject/1.0\"\n", cfg);
    fprintf(cfg, "request = \"%s\"\n", method);
    fprintf(cfg, "url = \"%s\"\n", url);
    if (auth && *auth) fprintf(cfg, "header = \"Authorization: %s\"\n", auth);
    if (body_json) {
        fputs("header = \"Content-Type: application/json\"\n", cfg);
        fprintf(cfg, "data-binary = \"@%s\"\n", bodypath);
    }
    fputs("write-out = \"\\n__RF_STATUS__%{http_code}__\\n\"\n", cfg);
    fclose(cfg);
    size_t got = 0; ssize_t k; int complete = 0;
    while (got + 1 < out_cap) {
        k = read(outp[0], out_body + got, out_cap - 1 - got);
        if (!k) { complete = 1; break; }
        if (k < 0) { if (errno == EINTR) continue; break; }
        got += (size_t)k;
    }
    out_body[got] = 0; close(outp[0]);
    int st; waitpid(pid, &st, 0);
    if (*bodypath) { /* shred before unlink: this file held only a request body, never a secret, but we hold the line anyway */
        FILE *f = fopen(bodypath, "r+b");
        if (f) { fseek(f, 0, SEEK_END); long n = ftell(f); fseek(f, 0, SEEK_SET); for (long i = 0; i < n; i++) fputc(0, f); fclose(f); }
        unlink(bodypath);
    }
    if (!complete || !WIFEXITED(st) || WEXITSTATUS(st) != 0) return -1;
    /* A response body may contain this marker. Only the final complete
     * curl write-out trailer establishes the observed HTTP status. */
    char *marker = NULL, *scan = out_body;
    while ((scan = strstr(scan, "__RF_STATUS__")) != NULL) { marker = scan; scan++; }
    if (!marker || marker == out_body || marker[-1] != '\n') return -1;
    const char *code = marker + strlen("__RF_STATUS__");
    if (strlen(code) != 6 || !isdigit((unsigned char)code[0]) ||
        !isdigit((unsigned char)code[1]) || !isdigit((unsigned char)code[2]) ||
        strcmp(code + 3, "__\n")) return -1;
    int parsed = (code[0] - '0') * 100 + (code[1] - '0') * 10 + code[2] - '0';
    if (parsed < 100 || parsed > 599) return -1;
    char *nl = marker; while (nl > out_body && nl[-1] == '\n') nl--;
    *nl = 0; *status_out = parsed;
    return 0;
}

/* Parse a backend reply and re-emit it as this command's envelope. */
static void emit_envelope(const char *verb, const char *provider, int http_status, const char *raw_body) {
    bf_jsonw w; bf_jsonw_init(&w, 0);
    char err[8];
    bf_json_doc_t *doc = raw_body && *raw_body ? bf_json_parse(raw_body, strlen(raw_body), err, sizeof(err)) : NULL;
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, verb);
    bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider);
    bf_jsonw_key(&w, "http_status"); bf_jsonw_int(&w, http_status);
    bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, http_status >= 200 && http_status < 300);
    bf_jsonw_key(&w, "data");
    if (doc) bf_jsonw_node(&w, doc, bf_json_root(doc), 0);
    else if (raw_body && *raw_body) bf_jsonw_str(&w, raw_body);
    else bf_jsonw_null(&w);
    bf_jsonw_obj_end(&w);
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout);
    if (doc) bf_json_free(doc);
    bf_jsonw_free(&w);
}

/* ------------------------------------------------------------ base64 */

static void base64_encode(const unsigned char *in, size_t n, char *out, size_t cap) {
    static const char tbl[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < n && o + 4 < cap; i += 3) {
        unsigned v = in[i] << 16;
        if (i + 1 < n) v |= in[i + 1] << 8;
        if (i + 2 < n) v |= in[i + 2];
        out[o++] = tbl[(v >> 18) & 63]; out[o++] = tbl[(v >> 12) & 63];
        out[o++] = (i + 1 < n) ? tbl[(v >> 6) & 63] : '=';
        out[o++] = (i + 2 < n) ? tbl[v & 63] : '=';
    }
    out[o] = 0;
}

/* ------------------------------------------------------------ verbs */

static int do_connect(const Creds *c, const char *provider) {
    if (!strcmp(provider, "forgejo")) {
        char out[8192];
        if (!run_verify(c, provider, out, sizeof(out))) { fail_json("connect", "could not reach bonfyre-credentials"); return 1; }
        char err[8]; bf_json_doc_t *doc = bf_json_parse(out, strlen(out), err, sizeof(err));
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, "connect");
        bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider);
        if (doc) { bf_jsonw_key(&w, "data"); bf_jsonw_node(&w, doc, bf_json_root(doc), 0); bf_json_free(doc); }
        bf_jsonw_obj_end(&w);
        fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
        return 0;
    }
    if (!strcmp(provider, "josh")) {
        const char *home = getenv("HOME"); char proxy[PATH_MAX], cli[PATH_MAX]; struct stat st;
        snprintf(proxy, sizeof(proxy), "%s/.bonfyre/vendor/josh-src/target/release/josh-proxy", home ? home : "");
        int have_proxy = stat(proxy, &st) == 0, have_cli = find_josh_cli(cli, sizeof(cli));
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, "connect");
        bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, "josh");
        bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, have_proxy && have_cli);
        bf_jsonw_key(&w, "josh_proxy_built"); bf_jsonw_bool(&w, have_proxy);
        bf_jsonw_key(&w, "josh_cli_built"); bf_jsonw_bool(&w, have_cli);
        bf_jsonw_key(&w, "outcome"); bf_jsonw_str(&w, (have_proxy && have_cli) ? "ready" : "not_built");
        bf_jsonw_obj_end(&w);
        fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
        return (have_proxy && have_cli) ? 0 : 3;
    }
    fail_json("connect", "unknown provider"); return 2;
}

static int do_provision(const Creds *c, const char *provider, const char *org, const char *repo, int priv, const char *desc) {
    if (strcmp(provider, "forgejo")) { fail_json("provision", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0)) { fail_json("provision", "org/repo must be plain identifiers"); return 2; }
    char url[512], auth[512]; int status;
    char *out = malloc(OUT_CAP);
    if (!out) { fail_json("provision", "out of memory"); return 2; }

    snprintf(url, sizeof(url), "%s/api/v1/orgs/%s", forgejo_api(), org);
    if (!apply_header(c, provider, url, auth, sizeof(auth))) { free(out); fail_json("provision", "no sealed forgejo credential"); return 2; }
    http_call("GET", url, auth, NULL, out, OUT_CAP, &status);
    int org_created = 0;
    if (status == 404) {
        bf_jsonw bw; bf_jsonw_init(&bw, 0); bf_jsonw_obj(&bw);
        bf_jsonw_key(&bw, "username"); bf_jsonw_str(&bw, org);
        bf_jsonw_key(&bw, "visibility"); bf_jsonw_str(&bw, "private");
        bf_jsonw_obj_end(&bw);
        snprintf(url, sizeof(url), "%s/api/v1/orgs", forgejo_api());
        http_call("POST", url, auth, bw.buf, out, OUT_CAP, &status);
        bf_jsonw_free(&bw);
        org_created = (status >= 200 && status < 300);
        if (!org_created) { emit_envelope("provision", provider, status, out); free(out); return 1; }
    }
    snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s", forgejo_api(), org, repo);
    http_call("GET", url, auth, NULL, out, OUT_CAP, &status);
    int repo_created = 0;
    if (status == 404) {
        bf_jsonw bw; bf_jsonw_init(&bw, 0); bf_jsonw_obj(&bw);
        bf_jsonw_key(&bw, "name"); bf_jsonw_str(&bw, repo);
        bf_jsonw_key(&bw, "private"); bf_jsonw_bool(&bw, priv);
        bf_jsonw_key(&bw, "auto_init"); bf_jsonw_bool(&bw, 1);
        bf_jsonw_key(&bw, "description"); bf_jsonw_str(&bw, desc ? desc : "");
        bf_jsonw_obj_end(&bw);
        snprintf(url, sizeof(url), "%s/api/v1/orgs/%s/repos", forgejo_api(), org);
        http_call("POST", url, auth, bw.buf, out, OUT_CAP, &status);
        bf_jsonw_free(&bw);
        repo_created = (status >= 200 && status < 300);
    }
    bf_jsonw w; bf_jsonw_init(&w, 0);
    char err[8]; bf_json_doc_t *doc = bf_json_parse(out, strlen(out), err, sizeof(err));
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, "provision");
    bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider);
    bf_jsonw_key(&w, "org"); bf_jsonw_str(&w, org);
    bf_jsonw_key(&w, "repo"); bf_jsonw_str(&w, repo);
    bf_jsonw_key(&w, "org_created"); bf_jsonw_bool(&w, org_created);
    bf_jsonw_key(&w, "repo_created"); bf_jsonw_bool(&w, repo_created);
    bf_jsonw_key(&w, "http_status"); bf_jsonw_int(&w, status);
    bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, status >= 200 && status < 300);
    bf_jsonw_key(&w, "data");
    if (doc) { bf_jsonw_node(&w, doc, bf_json_root(doc), 0); bf_json_free(doc); } else bf_jsonw_null(&w);
    bf_jsonw_obj_end(&w);
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
    free(out);
    return (status >= 200 && status < 300) ? 0 : 1;
}

static int do_resolve(const Creds *c, const char *provider, const char *org, const char *repo) {
    if (strcmp(provider, "forgejo")) { fail_json("resolve", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0)) { fail_json("resolve", "org/repo must be plain identifiers"); return 2; }
    char url[512], auth[512]; int status;
    char *out = malloc(OUT_CAP);
    if (!out) { fail_json("resolve", "out of memory"); return 2; }
    snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s", forgejo_api(), org, repo);
    if (!apply_header(c, provider, url, auth, sizeof(auth))) { free(out); fail_json("resolve", "no sealed forgejo credential"); return 2; }
    http_call("GET", url, auth, NULL, out, OUT_CAP, &status);
    emit_envelope("resolve", provider, status, out);
    free(out);
    return (status == 200) ? 0 : 1;
}

static int do_view(const Creds *c, const char *provider, const char *org, const char *repo, const char *path, const char *ref) {
    if (strcmp(provider, "forgejo")) { fail_json("view", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0)) { fail_json("view", "org/repo must be plain identifiers"); return 2; }
    if (path && *path && !ident_ok(path, 1)) { fail_json("view", "path has characters that do not belong in a URL"); return 2; }
    if (ref && *ref && !ident_ok(ref, 0)) { fail_json("view", "ref has characters that do not belong in a URL"); return 2; }
    char url[768], auth[512]; int status;
    char *out = malloc(OUT_CAP);
    if (!out) { fail_json("view", "out of memory"); return 2; }
    if (ref && *ref) snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s/contents/%s?ref=%s", forgejo_api(), org, repo, path ? path : "", ref);
    else snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s/contents/%s", forgejo_api(), org, repo, path ? path : "");
    if (!apply_header(c, provider, url, auth, sizeof(auth))) { free(out); fail_json("view", "no sealed forgejo credential"); return 2; }
    http_call("GET", url, auth, NULL, out, OUT_CAP, &status);
    emit_envelope("view", provider, status, out);
    free(out);
    return (status == 200) ? 0 : 1;
}

#include "cut_receipt.h"
static int do_checkout(const Creds *c, const char *provider, const char *org, const char *repo, const char *dest, const char *ref, const char *filter, const char *receipt) {
    if (strcmp(provider, "forgejo")) { fail_json("checkout", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0) || !dest || !*dest) { fail_json("checkout", "org/repo/dest required"); return 2; }
    if (ref && *ref && !ident_ok(ref, 0)) { fail_json("checkout", "ref has characters that do not belong in a URL"); return 2; }
    char josh_path[PATH_MAX]; int use_josh = 0;
    if (filter && *filter) {
        if (!filter_ok(filter)) { fail_json("checkout", "filter must be a josh filter spec starting with ':'"); return 2; }
        if (!find_josh_cli(josh_path, sizeof(josh_path))) { fail_json("checkout", "josh is not built; run cargo build --release -p josh-cli first"); return 2; }
        use_josh = 1;
    }
    char probe[512], auth[512], out[8192]; int status;
    snprintf(probe, sizeof(probe), "%s/api/v1/repos/%s/%s", forgejo_api(), org, repo);
    if (!apply_header(c, provider, probe, auth, sizeof(auth))) { fail_json("checkout", "no sealed forgejo credential"); return 2; }
    const char *tok = strstr(auth, "token "); tok = tok ? tok + 6 : auth;

    const char *api = forgejo_api(); const char *host = api;
    if (!strncmp(host, "https://", 8)) host += 8; else if (!strncmp(host, "http://", 7)) host += 7;
    const char *scheme = !strncmp(api, "https://", 8) ? "https" : "http";

    const char *home = getenv("HOME"); char dir[PATH_MAX], credpath[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/.bonfyre/tmp", home ? home : "/tmp"); mkdir(dir, 0700);
    snprintf(credpath, sizeof(credpath), "%s/repofabric-git-XXXXXX", dir);
    int fd = mkstemp(credpath);
    if (fd < 0) { fail_json("checkout", "could not create ephemeral credential file"); return 2; }
    fchmod(fd, 0600);
    char credline[1024];
    int ln = snprintf(credline, sizeof(credline), "%s://bonfyre-admin:%s@%s\n", scheme, tok, host);
    if (ln <= 0 || (size_t)ln >= sizeof(credline)) {
        close(fd); unlink(credpath); memset(credline, 0, sizeof(credline)); memset(auth, 0, sizeof(auth));
        fail_json("checkout", "credential line exceeds bounded file input"); return 2;
    }
    ssize_t wr = write(fd, credline, (size_t)ln); close(fd);
    memset(credline, 0, sizeof(credline)); memset(auth, 0, sizeof(auth));
    if (wr != ln) { unlink(credpath); fail_json("checkout", "could not write ephemeral credential file"); return 2; }

    char clone_url[512], helper[1300];
    snprintf(clone_url, sizeof(clone_url), "%s://%s/%s/%s.git", scheme, host, org, repo);
    snprintf(helper, sizeof(helper), "credential.helper=store --file=%s", credpath);

    pid_t pid = fork(); int exit_code = -1;
    if (pid == 0) {
        int devnull = open("/dev/null", O_RDONLY); if (devnull >= 0) dup2(devnull, 0);
        if (use_josh) {
            char gitval[1024]; snprintf(gitval, sizeof(gitval), "store --file=%s", credpath);
            setenv("GIT_CONFIG_COUNT", "1", 1);
            setenv("GIT_CONFIG_KEY_0", "credential.helper", 1);
            setenv("GIT_CONFIG_VALUE_0", gitval, 1);
            struct stat gst;
            if (stat("/opt/homebrew/opt/git/bin/git", &gst) == 0) {
                const char *oldpath = getenv("PATH");
                char newpath[PATH_MAX * 2];
                snprintf(newpath, sizeof(newpath), "/opt/homebrew/opt/git/bin:%s", oldpath ? oldpath : "/usr/bin:/bin");
                setenv("PATH", newpath, 1);
            }
            if (ref && *ref) execl(josh_path, "josh", "clone", "-b", ref, clone_url, filter, dest, (char *)NULL);
            else execl(josh_path, "josh", "clone", clone_url, filter, dest, (char *)NULL);
        } else {
            if (ref && *ref) execlp("git", "git", "-c", "credential.helper=", "-c", helper, "clone", "--branch", ref, clone_url, dest, (char *)NULL);
            else execlp("git", "git", "-c", "credential.helper=", "-c", helper, "clone", clone_url, dest, (char *)NULL);
        }
        _exit(127);
    }
    int st = 0;
    if (pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st))
        exit_code = WEXITSTATUS(st);

    FILE *f = fopen(credpath, "r+b");
    if (f) { fseek(f, 0, SEEK_END); long n = ftell(f); fseek(f, 0, SEEK_SET); for (long i = 0; i < n; i++) fputc(0, f); fclose(f); }
    unlink(credpath);
    memset(auth, 0, sizeof(auth));
    (void)status; (void)out;

    char virtual_commit[65]="",virtual_tree[65]="",cut_digest[65]="";
    int cut_ok=exit_code==0&&rf_rev_parse(dest,"HEAD",virtual_commit)&&
        rf_rev_parse(dest,"HEAD^{tree}",virtual_tree)&&
        rf_cut_digest(provider,org,repo,ref,filter,virtual_commit,virtual_tree,cut_digest);
    if(exit_code==0&&!cut_ok){fail_json("checkout","cloned repository could not be pinned to an exact commit and tree");return 1;}
    if(cut_ok&&!rf_cut_receipt(receipt,provider,org,repo,ref,filter,virtual_commit,virtual_tree,cut_digest)){
        fail_json("checkout","immutable source-cut receipt could not be created");return 1;}
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, "checkout");
    bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider);
    bf_jsonw_key(&w, "org"); bf_jsonw_str(&w, org);
    bf_jsonw_key(&w, "repo"); bf_jsonw_str(&w, repo);
    bf_jsonw_key(&w, "dest"); bf_jsonw_str(&w, dest);
    bf_jsonw_key(&w, "mode"); bf_jsonw_str(&w, use_josh ? "josh" : "git");
    if (use_josh) { bf_jsonw_key(&w, "filter"); bf_jsonw_str(&w, filter); }
    if(cut_ok){
        bf_jsonw_key(&w,"virtual_commit");bf_jsonw_str(&w,virtual_commit);
        bf_jsonw_key(&w,"virtual_tree");bf_jsonw_str(&w,virtual_tree);
        bf_jsonw_key(&w,"source_cut_digest");bf_jsonw_str(&w,cut_digest);
        bf_jsonw_key(&w,"canonical_mapping");bf_jsonw_str(&w,use_josh?"unverified":"direct-git");
        if(ref&&*ref){bf_jsonw_key(&w,"requested_ref");bf_jsonw_str(&w,ref);}
        if(receipt){bf_jsonw_key(&w,"receipt");bf_jsonw_str(&w,receipt);}
    }
    bf_jsonw_key(&w, "exit_code"); bf_jsonw_int(&w, exit_code);
    bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, exit_code == 0);
    bf_jsonw_obj_end(&w);
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
    return exit_code == 0 ? 0 : 1;
}

static int do_change(const Creds *c, const char *provider, const char *org, const char *repo, const char *path,
                      const char *message, const char *content_file, const char *branch) {
    if (strcmp(provider, "forgejo")) { fail_json("change", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0) || !path || !*path || !message || !content_file) { fail_json("change", "org/repo/path/message/content-file required"); return 2; }
    if (!ident_ok(path, 1)) { fail_json("change", "path has characters that do not belong in a URL"); return 2; }
    if (branch && *branch && !ident_ok(branch, 0)) { fail_json("change", "branch has characters that do not belong in a URL"); return 2; }

    FILE *cf = fopen(content_file, "rb");
    if (!cf) { fail_json("change", "cannot read content-file"); return 2; }
    size_t content_cap = 1024 * 1024;
    unsigned char *content = malloc(content_cap + 1);
    char *b64 = malloc(((content_cap / 3) + 1) * 4 + 4);
    char *out = malloc(OUT_CAP);
    if (!content || !b64 || !out) { fclose(cf); free(content); free(b64); free(out); fail_json("change", "out of memory"); return 2; }
    size_t n = fread(content, 1, content_cap + 1, cf);
    int read_failed = ferror(cf);
    fclose(cf);
    if (read_failed || n > content_cap) {
        free(content); free(b64); free(out);
        fail_json("change", read_failed ? "cannot read complete content-file" : "content-file exceeds 1 MiB; refusing truncated write");
        return 2;
    }
    base64_encode(content, n, b64, ((content_cap / 3) + 1) * 4 + 4);
    free(content);

    char url[768], auth[512]; int status;
    char urlpath[768];
    if (branch && *branch) snprintf(urlpath, sizeof(urlpath), "%s/api/v1/repos/%s/%s/contents/%s?ref=%s", forgejo_api(), org, repo, path, branch);
    else snprintf(urlpath, sizeof(urlpath), "%s/api/v1/repos/%s/%s/contents/%s", forgejo_api(), org, repo, path);
    if (!apply_header(c, provider, urlpath, auth, sizeof(auth))) { free(b64); free(out); fail_json("change", "no sealed forgejo credential"); return 2; }
    http_call("GET", urlpath, auth, NULL, out, OUT_CAP, &status);
    if (status != 200 && status != 404) {
        free(b64); free(out);
        fail_json("change", "source preflight did not establish an existing or absent file; mutation refused");
        return 1;
    }
    char sha[128] = "";
    if (status == 200) {
        char err[8]; bf_json_doc_t *doc = bf_json_parse(out, strlen(out), err, sizeof(err));
        if (doc) { const bf_json_node_t *sh = bf_json_obj_get(doc, bf_json_root(doc), "sha"); if (sh) bf_json_get_str_copy(sh, sha, sizeof(sha)); bf_json_free(doc); }
        size_t sl = strlen(sha);
        if ((sl != 40 && sl != 64) || strspn(sha, "0123456789abcdef") != sl) {
            free(b64); free(out);
            fail_json("change", "existing file has no verified Git object SHA; mutation refused");
            return 1;
        }
    }
    bf_jsonw bw; bf_jsonw_init(&bw, 0); bf_jsonw_obj(&bw);
    bf_jsonw_key(&bw, "message"); bf_jsonw_str(&bw, message);
    bf_jsonw_key(&bw, "content"); bf_jsonw_str(&bw, b64);
    if (*sha) { bf_jsonw_key(&bw, "sha"); bf_jsonw_str(&bw, sha); }
    if (branch && *branch) { bf_jsonw_key(&bw, "branch"); bf_jsonw_str(&bw, branch); }
    bf_jsonw_obj_end(&bw);
    free(b64);

    snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s/contents/%s", forgejo_api(), org, repo, path);
    http_call(*sha ? "PUT" : "POST", url, auth, bw.buf, out, OUT_CAP, &status);
    bf_jsonw_free(&bw);
    emit_envelope("change", provider, status, out);
    free(out);
    return (status >= 200 && status < 300) ? 0 : 1;
}

static int do_observe(const Creds *c, const char *provider, const char *org, const char *repo, int limit) {
    if (strcmp(provider, "forgejo")) { fail_json("observe", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0)) { fail_json("observe", "org/repo must be plain identifiers"); return 2; }
    char url[512], auth[512]; int status;
    char *out = malloc(OUT_CAP);
    if (!out) { fail_json("observe", "out of memory"); return 2; }
    snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s/commits?limit=%d", forgejo_api(), org, repo, limit > 0 ? limit : 20);
    if (!apply_header(c, provider, url, auth, sizeof(auth))) { free(out); fail_json("observe", "no sealed forgejo credential"); return 2; }
    http_call("GET", url, auth, NULL, out, OUT_CAP, &status);
    emit_envelope("observe", provider, status, out);
    free(out);
    return (status == 200) ? 0 : 1;
}

static int do_service(const Creds *c, const char *provider, const char *org, const char *repo, const char *action,
                       const char *hook_url, const char *events_csv) {
    if (strcmp(provider, "forgejo")) { fail_json("service", "provider not implemented"); return 2; }
    if (!ident_ok(org, 0) || !ident_ok(repo, 0) || !action) { fail_json("service", "org/repo/action required"); return 2; }
    char url[512], auth[512]; int status;
    char *out = malloc(OUT_CAP);
    if (!out) { fail_json("service", "out of memory"); return 2; }
    snprintf(url, sizeof(url), "%s/api/v1/repos/%s/%s/hooks", forgejo_api(), org, repo);
    if (!apply_header(c, provider, url, auth, sizeof(auth))) { free(out); fail_json("service", "no sealed forgejo credential"); return 2; }
    if (!strcmp(action, "list")) {
        http_call("GET", url, auth, NULL, out, OUT_CAP, &status);
        emit_envelope("service", provider, status, out);
        free(out);
        return (status == 200) ? 0 : 1;
    }
    if (!strcmp(action, "add")) {
        if (!hook_url || strncmp(hook_url, "https://", 8)) { free(out); fail_json("service", "add requires --url with an https address"); return 2; }
        bf_jsonw bw; bf_jsonw_init(&bw, 0); bf_jsonw_obj(&bw);
        bf_jsonw_key(&bw, "type"); bf_jsonw_str(&bw, "gitea");
        bf_jsonw_key(&bw, "config"); bf_jsonw_obj(&bw);
        bf_jsonw_key(&bw, "url"); bf_jsonw_str(&bw, hook_url);
        bf_jsonw_key(&bw, "content_type"); bf_jsonw_str(&bw, "json");
        bf_jsonw_obj_end(&bw);
        bf_jsonw_key(&bw, "events"); bf_jsonw_arr(&bw);
        char buf[512]; snprintf(buf, sizeof(buf), "%s", events_csv && *events_csv ? events_csv : "push");
        for (char *tok = strtok(buf, ","); tok; tok = strtok(NULL, ",")) bf_jsonw_str(&bw, tok);
        bf_jsonw_arr_end(&bw);
        bf_jsonw_key(&bw, "active"); bf_jsonw_bool(&bw, 1);
        bf_jsonw_obj_end(&bw);
        http_call("POST", url, auth, bw.buf, out, OUT_CAP, &status);
        bf_jsonw_free(&bw);
        emit_envelope("service", provider, status, out);
        free(out);
        return (status >= 200 && status < 300) ? 0 : 1;
    }
    free(out);
    fail_json("service", "action must be list or add"); return 2;
}

/* ------------------------------------------------------------------ main */

static void usage_to(FILE *dest) {
    fputs(
        "usage: bonfyre-repository-fabric <verb> [--provider P] ...\n"
        "  connect  --provider P\n"
        "  provision --provider P --org O --repo R [--private 0|1] [--description D]\n"
        "  resolve  --provider P --org O --repo R\n"
        "  view     --provider P --org O --repo R [--path PATH] [--ref REF]\n"
        "  checkout --provider P --org O --repo R --dest DIR [--ref REF] [--filter JOSH_FILTER] [--receipt ABSOLUTE_YAFF_PATH]\n"
        "  change   --provider P --org O --repo R --path PATH --message M --content-file F [--branch B]\n"
        "  observe  --provider P --org O --repo R [--limit N]\n"
        "  service  --provider P --org O --repo R --action list|add [--url HOOK] [--events e1,e2]\n",
        dest);
}
static void usage(void) { usage_to(stderr); }

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 2; }
    const char *verb = argv[1];
    /* Help works before sealed credentials are opened or any network request. */
    if (!strcmp(verb,"--help") || !strcmp(verb,"-h") || !strcmp(verb,"help") ||
        (argc==3 && (!strcmp(argv[2],"--help") || !strcmp(argv[2],"-h") || !strcmp(argv[2],"help")))) {
        usage_to(stdout); return 0;
    }
    const char *provider = arg(argc, argv, "--provider"); if (!provider) provider = "forgejo";
    Creds c;
    if (!creds_paths(&c,argv[0])) { fail_json(verb, "bonfyre-credentials is not reachable in this environment"); return 2; }
    if (!strcmp(verb, "connect")) return do_connect(&c, provider);
    if (!strcmp(verb, "provision")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo"), *desc = arg(argc, argv, "--description");
        int priv = flag_true(arg(argc, argv, "--private"), 1);
        if (!org || !repo) { usage(); return 2; }
        return do_provision(&c, provider, org, repo, priv, desc);
    }
    if (!strcmp(verb, "resolve")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo");
        if (!org || !repo) { usage(); return 2; }
        return do_resolve(&c, provider, org, repo);
    }
    if (!strcmp(verb, "view")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo"), *path = arg(argc, argv, "--path"), *ref = arg(argc, argv, "--ref");
        if (!org || !repo) { usage(); return 2; }
        return do_view(&c, provider, org, repo, path, ref);
    }
    if (!strcmp(verb, "checkout")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo"), *dest = arg(argc, argv, "--dest"),
                   *ref = arg(argc, argv, "--ref"), *filter = arg(argc, argv, "--filter"), *receipt = arg(argc, argv, "--receipt");
        if (!org || !repo || !dest) { usage(); return 2; }
        return do_checkout(&c, provider, org, repo, dest, ref, filter, receipt);
    }
    if (!strcmp(verb, "change")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo"), *path = arg(argc, argv, "--path"),
                   *message = arg(argc, argv, "--message"), *content_file = arg(argc, argv, "--content-file"), *branch = arg(argc, argv, "--branch");
        if (!org || !repo || !path || !message || !content_file) { usage(); return 2; }
        return do_change(&c, provider, org, repo, path, message, content_file, branch);
    }
    if (!strcmp(verb, "observe")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo"), *limit_s = arg(argc, argv, "--limit");
        if (!org || !repo) { usage(); return 2; }
        return do_observe(&c, provider, org, repo, limit_s ? atoi(limit_s) : 20);
    }
    if (!strcmp(verb, "service")) {
        const char *org = arg(argc, argv, "--org"), *repo = arg(argc, argv, "--repo"), *action = arg(argc, argv, "--action"),
                   *hook_url = arg(argc, argv, "--url"), *events = arg(argc, argv, "--events");
        if (!org || !repo || !action) { usage(); return 2; }
        return do_service(&c, provider, org, repo, action, hook_url, events);
    }
    usage(); return 2;
}
