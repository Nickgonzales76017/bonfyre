/* drive.c: the two official Obsidian tools, run as child processes.
 *
 *   obsidian  the desktop app's CLI. It drives the running app and starts the
 *             app when it is not running, so a call is made only when the app
 *             is already up (or BONFYRE_OBSIDIAN_CLI_LAUNCH=1 says starting it
 *             is wanted). A server with no desktop never launches anything.
 *   ob        Obsidian Headless. Its login and per-vault sync state live under
 *             $HOME/.obsidian-headless, so every account runs it with its own
 *             HOME (<state>/accounts/<account>/home): one account's login and
 *             remote vault list never reach another's. OBSIDIAN_AUTH_TOKEN in
 *             this process's environment reaches ob only for the one account
 *             BONFYRE_OBSIDIAN_TOKEN_ACCOUNT names, and is removed otherwise.
 *
 * Nothing is run through a shell; argv is built here and passed to execv. A
 * password, email or MFA code is never put on argv: those prompts are left to
 * the person at a terminal, and in JSON mode ob refuses to prompt at all.
 */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#else
#define _GNU_SOURCE
#endif
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "obsidian.h"

#define RUN_CAP (4u << 20)

int ob_mkdirs(const char *path, int mode) {
    char tmp[OB_PATH]; struct stat st;
    if (snprintf(tmp, sizeof(tmp), "%s", path) >= (int)sizeof(tmp)) return -1;
    for (char *p = tmp + 1; *p; p++) if (*p == '/') { *p = 0; if (mkdir(tmp, (mode_t)mode) && errno != EEXIST) return -1; *p = '/'; }
    if (mkdir(tmp, (mode_t)mode) && errno != EEXIST) return -1;
    return stat(tmp, &st) == 0 && S_ISDIR(st.st_mode) ? 0 : -1;
}

int ob_which(const char *name, char out[OB_PATH]) {
    if (!name || !*name) return 0;
    if (strchr(name, '/')) { if (access(name, X_OK)) return 0; snprintf(out, OB_PATH, "%s", name); return 1; }
    const char *path = getenv("PATH");
    char buf[8192]; snprintf(buf, sizeof(buf), "%s", path ? path : "/usr/bin:/bin");
    for (char *save = NULL, *d = strtok_r(buf, ":", &save); d; d = strtok_r(NULL, ":", &save)) {
        char cand[OB_PATH]; struct stat st;
        if (snprintf(cand, sizeof(cand), "%s/%s", *d ? d : ".", name) >= (int)sizeof(cand)) continue;
        if (!access(cand, X_OK) && !stat(cand, &st) && S_ISREG(st.st_mode)) { snprintf(out, OB_PATH, "%s", cand); return 1; }
    }
    return 0;
}

static void drain(int fd, char **buf, size_t *len, size_t *cap, int *open_) {
    char tmp[8192]; ssize_t n = read(fd, tmp, sizeof(tmp));
    if (n <= 0) { if (n == 0 || (errno != EINTR && errno != EAGAIN)) *open_ = 0; return; }
    if (*len + (size_t)n + 1 > *cap) {
        if (*len + (size_t)n + 1 > RUN_CAP) return;
        size_t nc = *cap ? *cap * 2 : 16384; while (nc < *len + (size_t)n + 1) nc *= 2;
        char *b = realloc(*buf, nc); if (!b) return; *buf = b; *cap = nc;
    }
    memcpy(*buf + *len, tmp, (size_t)n); *len += (size_t)n; (*buf)[*len] = 0;
}

static double now_s(void) { struct timeval tv; gettimeofday(&tv, NULL); return (double)tv.tv_sec + tv.tv_usec / 1e6; }

static void child_env(const char *home, int pass_token) {
    if (home) setenv("HOME", home, 1);
    if (!pass_token) unsetenv("OBSIDIAN_AUTH_TOKEN");
}

int ob_run(char *const argv[], const char *home_override, int pass_token, int timeout_s, RunResult *r) {
    int out[2], err[2]; pid_t pid; size_t oc = 0, ec = 0;
    memset(r, 0, sizeof(*r)); r->status = -1;
    if (pipe(out) || pipe(err)) return 0;
    if ((pid = fork()) < 0) return 0;
    if (pid == 0) {
        int nul = open("/dev/null", O_RDONLY);
        setpgid(0, 0);
        if (nul >= 0) dup2(nul, 0);
        dup2(out[1], 1); dup2(err[1], 2);
        close(out[0]); close(err[0]); close(out[1]); close(err[1]);
        child_env(home_override, pass_token);
        execv(argv[0], argv);
        _exit(127);
    }
    r->spawned = 1;
    close(out[1]); close(err[1]);
    int oo = 1, eo = 1; double deadline = now_s() + (timeout_s > 0 ? timeout_s : 10);
    while (oo || eo) {
        struct pollfd p[2]; int np = 0, io = -1, ie = -1;
        if (oo) { io = np; p[np++] = (struct pollfd){out[0], POLLIN, 0}; }
        if (eo) { ie = np; p[np++] = (struct pollfd){err[0], POLLIN, 0}; }
        int left = (int)((deadline - now_s()) * 1000);
        if (left <= 0) { r->timed_out = 1; kill(-pid, SIGKILL); kill(pid, SIGKILL); break; }
        int k = poll(p, (nfds_t)np, left);
        if (k < 0 && errno == EINTR) continue;
        if (k <= 0) continue;
        if (io >= 0 && p[io].revents) drain(out[0], &r->out, &r->out_len, &oc, &oo);
        if (ie >= 0 && p[ie].revents) drain(err[0], &r->err, &r->err_len, &ec, &eo);
    }
    close(out[0]); close(err[0]);
    int st = 0;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    r->status = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
    if (!r->out) { r->out = calloc(1, 1); }
    if (!r->err) { r->err = calloc(1, 1); }
    return 1;
}

void ob_run_free(RunResult *r) { free(r->out); free(r->err); memset(r, 0, sizeof(*r)); }

int ob_app_running(const Cfg *c) {
    char pg[OB_PATH]; RunResult r; int up;
    if (c->app_running && *c->app_running) return atoi(c->app_running) != 0;
    if (!ob_which("pgrep", pg)) return 0;
    char *argv[] = {pg, "-x", "Obsidian", NULL};
    if (!ob_run(argv, NULL, 0, 5, &r)) return 0;
    up = r.status == 0; ob_run_free(&r);
    return up;
}

int ob_cli_path(const Cfg *c, char out[OB_PATH]) {
    if (c->cli && *c->cli) return ob_which(c->cli, out);
    if (ob_which("obsidian", out)) return 1;
    /* Bundled CLI can drive a running app without a global symlink.
       ob_cli_usable still refuses to start the app implicitly. */
    char bundled[OB_PATH];
    if (snprintf(bundled, sizeof(bundled), "%s/Contents/MacOS/obsidian-cli", c->app) >= (int)sizeof(bundled)) return 0;
    return ob_which(bundled, out);
}

int ob_cli_version_ok(const RunResult *r) {
    if (!r || r->status || r->timed_out || !r->out || r->out_len < 3) return 0;
    size_t i = (r->out[0] == 'v') ? 1 : 0, n = r->out_len;
    if (i == n || !isdigit((unsigned char)r->out[i])) return 0;
    while (i < n && isdigit((unsigned char)r->out[i])) ++i;
    if (i == n || r->out[i++] != '.' || i == n || !isdigit((unsigned char)r->out[i])) return 0;
    return 1;
}
int ob_cli_usable(const Cfg *c, char out[OB_PATH]) {
    if ((c->via && !strcmp(c->via, "native")) || !ob_cli_path(c, out)) return 0;
    if (!c->cli_launch && !ob_app_running(c)) return 0;
    RunResult r = {0}; char *argv[] = {out, "version", NULL};
    int ready = ob_run(argv, NULL, 0, c->timeout, &r) && ob_cli_version_ok(&r);
    if (r.spawned) ob_run_free(&r);
    return ready;
}

int ob_ob_path(const Cfg *c, char out[OB_PATH]) {
    if (c->ob && *c->ob) return ob_which(c->ob, out);
    if (ob_which("ob", out)) return 1;
    /* An interactive PATH may include vite-plus while an MCP daemon's does not. */
    char bundled[OB_PATH];
    if (snprintf(bundled, sizeof(bundled), "%s/.vite-plus/bin/ob", c->home) >= (int)sizeof(bundled)) return 0;
    return ob_which(bundled, out);
}

void ob_account_home(const Cfg *c, const char *account, char out[OB_PATH]) { snprintf(out, OB_PATH, "%s/accounts/%s/home", c->state, account); }

/* ------------------------------------------------------------ desktop registry */
static bf_json_doc_t *registry_doc(const Cfg *c, char **txt) {
    char p[OB_PATH], err[64];
    snprintf(p, sizeof(p), "%s/obsidian.json", c->app_support);
    if (!(*txt = ob_read_file(p, NULL, 8u << 20))) return NULL;
    return bf_json_parse_str(*txt, err, sizeof(err));
}

int ob_registry_lookup(const Cfg *c, const char *real_root, char id[64], int *open_) {
    char *txt = NULL; bf_json_doc_t *d = registry_doc(c, &txt); int found = 0;
    if (d) {
        const bf_json_node_t *r = bf_json_root(d), *vs = r ? bf_json_obj_get(d, r, "vaults") : NULL;
        for (const bf_json_node_t *v = vs ? bf_json_child_first(d, vs) : NULL; v && !found; v = bf_json_child_next(d, v)) {
            const bf_json_node_t *p, *o;
            char path[OB_PATH] = "", real[OB_PATH];
            if ((p = bf_json_obj_get(d, v, "path"))) bf_json_get_str_copy(p, path, sizeof(path));
            if (path[0] && realpath(path, real) && !strcmp(real, real_root)) {
                snprintf(id, 64, "%.*s", v->key_len < 63 ? v->key_len : 63, v->key ? v->key : "");
                if (open_) *open_ = (o = bf_json_obj_get(d, v, "open")) ? bf_json_get_bool(o) : 0;
                found = 1;
            }
        }
        bf_json_free(d);
    }
    free(txt);
    return found;
}

int ob_desktop_sync_plugin(const char *root, const char *config_dir) {
    char p[OB_PATH], err[64]; char *txt; bf_json_doc_t *d; int on = -1;
    snprintf(p, sizeof(p), "%s/%s/core-plugins.json", root, config_dir);
    if (!(txt = ob_read_file(p, NULL, 1u << 20))) return -1;
    if ((d = bf_json_parse_str(txt, err, sizeof(err)))) {
        const bf_json_node_t *r = bf_json_root(d), *s;
        on = 0;
        if (r && (s = bf_json_obj_get(d, r, "sync"))) on = bf_json_get_bool(s) ? 1 : 0;
        else if (r) for (const bf_json_node_t *e = bf_json_child_first(d, r); e; e = bf_json_child_next(d, e)) {
            char name[32] = ""; if (bf_json_get_str_copy(e, name, sizeof(name)) && !strcmp(name, "sync")) on = 1;
        }
        bf_json_free(d);
    }
    free(txt);
    return on;
}

void ob_registry_json(const Cfg *c, bf_jsonw *w) {
    char *txt = NULL; bf_json_doc_t *d = registry_doc(c, &txt);
    bf_jsonw_arr(w);
    if (d) {
        const bf_json_node_t *r = bf_json_root(d), *vs = r ? bf_json_obj_get(d, r, "vaults") : NULL;
        for (const bf_json_node_t *v = vs ? bf_json_child_first(d, vs) : NULL; v; v = bf_json_child_next(d, v)) {
            const bf_json_node_t *p, *o;
            char id[64] = "", path[OB_PATH] = "", real[OB_PATH];
            snprintf(id, sizeof(id), "%.*s", v->key_len < 63 ? v->key_len : 63, v->key ? v->key : "");
            if ((p = bf_json_obj_get(d, v, "path"))) bf_json_get_str_copy(p, path, sizeof(path));
            int exists = path[0] && realpath(path, real);
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "desktop_sync_plugin");
            int s = exists ? ob_desktop_sync_plugin(real, ".obsidian") : -1;
            if (s < 0) bf_jsonw_null(w); else bf_jsonw_bool(w, s);
            bf_jsonw_key(w, "exists"); bf_jsonw_bool(w, exists);
            bf_jsonw_key(w, "id"); bf_jsonw_str(w, id);
            bf_jsonw_key(w, "name"); { const char *b = strrchr(path, '/'); bf_jsonw_str(w, b ? b + 1 : path); }
            bf_jsonw_key(w, "open"); bf_jsonw_bool(w, (o = bf_json_obj_get(d, v, "open")) ? bf_json_get_bool(o) : 0);
            bf_jsonw_key(w, "path"); bf_jsonw_str(w, path);
            bf_jsonw_key(w, "real_path"); if (exists) bf_jsonw_str(w, real); else bf_jsonw_null(w);
            bf_jsonw_obj_end(w);
        }
        bf_json_free(d);
    }
    bf_jsonw_arr_end(w);
    free(txt);
}

/* ------------------------------------------------------------ guard */
/* Does an ob home hold a sync configuration for this real vault path? */
static int ob_home_has_vault(const char *home, const char *real_root) {
    char dir[OB_PATH]; DIR *d; struct dirent *de; int hit = 0;
    snprintf(dir, sizeof(dir), "%s/.obsidian-headless/sync", home);
    if (!(d = opendir(dir))) return 0;
    while (!hit && (de = readdir(d))) {
        char p[OB_PATH], err[64], vp[OB_PATH] = "", real[OB_PATH]; char *txt; bf_json_doc_t *doc;
        if (de->d_name[0] == '.') continue;
        snprintf(p, sizeof(p), "%s/%s/config.json", dir, de->d_name);
        if (!(txt = ob_read_file(p, NULL, 1u << 20))) continue;
        if ((doc = bf_json_parse_str(txt, err, sizeof(err)))) {
            const bf_json_node_t *r = bf_json_root(doc), *v = r ? bf_json_obj_get(doc, r, "vaultPath") : NULL;
            if (v && bf_json_get_str_copy(v, vp, sizeof(vp)) && realpath(vp, real) && !strcmp(real, real_root)) hit = 1;
            bf_json_free(doc);
        }
        free(txt);
    }
    closedir(d);
    return hit;
}

static int ci_has(const char *hay, const char *needle) {
    size_t n = strlen(needle);
    for (; hay && *hay; hay++) if (!strncasecmp(hay, needle, n)) return 1;
    return 0;
}

/* Obsidian: never run desktop Sync and Headless Sync on one device for one
 * vault. Refusal is the safe direction, so an unclear answer refuses. */
void ob_guard_headless(const Cfg *c, const Binding *b, Guard *g) {
    char cli[OB_PATH], id[64], acct_home[OB_PATH];
    memset(g, 0, sizeof(*g));
    if (strcmp(b->transport, "headless")) {
        g->refuse = 1; snprintf(g->code, sizeof(g->code), "transport");
        snprintf(g->reason, sizeof(g->reason), "this vault is bound with transport %s; headless sync runs only for a vault bound with transport headless", b->transport);
        return;
    }
    const char *dirs[2] = {b->config_dir, ".obsidian"};
    for (int i = 0; i < 2; i++) {
        if (i == 1 && !strcmp(dirs[0], dirs[1])) break;
        if (ob_desktop_sync_plugin(b->root, dirs[i]) == 1) {
            g->refuse = 1; snprintf(g->code, sizeof(g->code), "desktop-sync-enabled");
            snprintf(g->reason, sizeof(g->reason), "the Obsidian app has its Sync plugin turned on for this vault on this device, and Obsidian says desktop Sync and Headless Sync must never run on the same device for the same vault. Turn Sync off for this vault in the app, or sync it from another machine");
            snprintf(g->evidence, sizeof(g->evidence), "%s/core-plugins.json has sync enabled", dirs[i]);
            return;
        }
    }
    if (ob_registry_lookup(c, b->root, id, NULL) && ob_cli_usable(c, cli)) {
        char vault[96], *argv[] = {cli, vault, "sync:status", NULL}; RunResult r;
        snprintf(vault, sizeof(vault), "vault=%s", id);
        if (ob_run(argv, NULL, 0, c->timeout, &r)) {
            int off = r.status == 0 && (ci_has(r.out, "disabled") || ci_has(r.out, "not connected") || ci_has(r.out, "not set up") || ci_has(r.out, "not enabled") || ci_has(r.out, "sync is off"));
            if (r.status == 0 && !off) {
                g->refuse = 1; snprintf(g->code, sizeof(g->code), "desktop-sync-reported");
                snprintf(g->reason, sizeof(g->reason), "the running Obsidian app reports Sync for this vault, and desktop Sync and Headless Sync must never run on the same device for the same vault");
                snprintf(g->evidence, sizeof(g->evidence), "obsidian sync:status: %.900s", r.out);
                ob_run_free(&r); return;
            }
            ob_run_free(&r);
        }
    }
    ob_account_home(c, b->account, acct_home);
    {
        char dir[OB_PATH]; DIR *d; struct dirent *de;
        snprintf(dir, sizeof(dir), "%s/accounts", c->state);
        if ((d = opendir(dir))) {
            while ((de = readdir(d))) {
                char h[OB_PATH];
                if (de->d_name[0] == '.' || !strcmp(de->d_name, b->account)) continue;
                snprintf(h, sizeof(h), "%s/%s/home", dir, de->d_name);
                if (ob_home_has_vault(h, b->root)) {
                    closedir(d);
                    g->refuse = 1; snprintf(g->code, sizeof(g->code), "headless-other-account");
                    snprintf(g->reason, sizeof(g->reason), "another account's Headless Sync is already configured for this folder; two sync clients on one folder conflict");
                    return;
                }
            }
            closedir(d);
        }
    }
    if (c->home[0] && strcmp(c->home, acct_home) && ob_home_has_vault(c->home, b->root)) {
        g->refuse = 1; snprintf(g->code, sizeof(g->code), "headless-unscoped");
        snprintf(g->reason, sizeof(g->reason), "an Obsidian Headless setup outside any account (in %s/.obsidian-headless) already syncs this folder; unlink it there with `ob sync-unlink --path` before an account takes it over", c->home);
        return;
    }
    snprintf(g->code, sizeof(g->code), "clear");
    snprintf(g->reason, sizeof(g->reason), "no desktop Sync and no other headless client was found for this vault on this device");
}

/* ------------------------------------------------------------ supervisor */
static void label_part(const char *s, char *out, size_t cap) {
    size_t i = 0; for (; s[i] && i + 1 < cap; i++) out[i] = (isalnum((unsigned char)s[i]) || s[i] == '.' || s[i] == '-') ? s[i] : '-'; out[i] = 0;
}

void ob_sup_paths(const Cfg *c, const Binding *b, char pidf[OB_PATH], char logf[OB_PATH], char label[256], char plist[OB_PATH]) {
    char a[48], n[96], digest[65];
    snprintf(pidf, OB_PATH, "%s/accounts/%s/run/%s.pid", c->state, b->account, b->name);
    snprintf(logf, OB_PATH, "%s/accounts/%s/logs/%s.log", c->state, b->account, b->name);
    ob_sha256_mem(b->account, strlen(b->account), digest);
    label_part(b->account, a, sizeof(a)); label_part(b->name, n, sizeof(n));
    /* Subject digest prevents user:19/user-19 launchd label collisions. */
    snprintf(label, 256, "com.bonfyre.obsidian.%.40s-%.20s.%.90s", a, digest, n);
    snprintf(plist, OB_PATH, "%s/%s.plist", c->agents_dir, label);
}

static int launchd_mode(const Cfg *c) { return c->supervisor && !strcmp(c->supervisor, "launchd"); }
static int token_for(const Cfg *c, const char *account) { return c->token_account && *c->token_account && !strcmp(c->token_account, account); }

static long read_pid(const char *pidf) {
    char *t = ob_read_file(pidf, NULL, 64); long pid = t ? strtol(t, NULL, 10) : 0; free(t); return pid;
}

/* A live pid is ours only when its command line is ob's continuous sync for
 * this folder; a reused pid belongs to someone else and is never signalled. */
static int pid_is_ours(long pid, const char *root) {
    char ps[OB_PATH], spid[32]; RunResult r; int ours = 0;
    if (pid <= 1 || kill((pid_t)pid, 0)) return 0;
    if (!ob_which("ps", ps)) return 1;
    snprintf(spid, sizeof(spid), "%ld", pid);
    char *argv[] = {ps, "-o", "command=", "-p", spid, NULL};
    if (ob_run(argv, NULL, 0, 5, &r)) { ours = r.status == 0 && strstr(r.out, "--continuous") && strstr(r.out, root); ob_run_free(&r); }
    return ours;
}

int ob_sup_alive(const Cfg *c, const Binding *b, long *pid) {
    char pidf[OB_PATH], logf[OB_PATH], label[256], plist[OB_PATH];
    ob_sup_paths(c, b, pidf, logf, label, plist);
    *pid = 0;
    if (launchd_mode(c)) {
        char lc[OB_PATH], target[320]; RunResult r; int up = 0;
        if (!ob_which(c->launchctl, lc)) return 0;
        snprintf(target, sizeof(target), "gui/%d/%s", (int)getuid(), label);
        char *argv[] = {lc, "print", target, NULL};
        if (ob_run(argv, NULL, 0, 5, &r)) {
            up = r.status == 0;
            const char *p = up ? strstr(r.out, "pid = ") : NULL; if (p) *pid = strtol(p + 6, NULL, 10);
            ob_run_free(&r);
        }
        return up;
    }
    long p = read_pid(pidf);
    if (p && pid_is_ours(p, b->root)) { *pid = p; return 1; }
    return 0;
}

static void xml_esc(FILE *f, const char *s) {
    for (; *s; s++) {
        if (*s == '&') fputs("&amp;", f); else if (*s == '<') fputs("&lt;", f); else if (*s == '>') fputs("&gt;", f); else fputc(*s, f);
    }
}

int ob_sup_start(const Cfg *c, const Binding *b, bf_jsonw *w, char *why, size_t cap) {
    char obp[OB_PATH], pidf[OB_PATH], logf[OB_PATH], label[256], plist[OB_PATH], home[OB_PATH], dir[OB_PATH]; long pid;
    if (!ob_ob_path(c, obp)) { snprintf(why, cap, "Obsidian Headless (ob) is not installed: npm install -g obsidian-headless"); return EX_MISSING; }
    ob_sup_paths(c, b, pidf, logf, label, plist);
    ob_account_home(c, b->account, home);
    snprintf(dir, sizeof(dir), "%.*s", (int)(strrchr(pidf, '/') - pidf), pidf); ob_mkdirs(dir, 0700);
    snprintf(dir, sizeof(dir), "%.*s", (int)(strrchr(logf, '/') - logf), logf); ob_mkdirs(dir, 0700);
    ob_mkdirs(home, 0700);
    if (ob_sup_alive(c, b, &pid)) {
        bf_jsonw_key(w, "already_running"); bf_jsonw_bool(w, 1);
        bf_jsonw_key(w, "pid"); bf_jsonw_int(w, pid);
        bf_jsonw_key(w, "log"); bf_jsonw_str(w, logf);
        return EX_OK;
    }
    bf_jsonw_key(w, "already_running"); bf_jsonw_bool(w, 0);
    if (launchd_mode(c)) {
        char lc[OB_PATH], domain[64]; RunResult r; FILE *f;
        if (!ob_which(c->launchctl, lc)) { snprintf(why, cap, "launchctl not found"); return EX_MISSING; }
        snprintf(dir, sizeof(dir), "%s", c->agents_dir); ob_mkdirs(dir, 0755);
        if (!(f = fopen(plist, "w"))) { snprintf(why, cap, "cannot write %s: %s", plist, strerror(errno)); return EX_FAIL; }
        fputs("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n", f);
        fputs("<key>Label</key><string>", f); xml_esc(f, label); fputs("</string>\n<key>ProgramArguments</key><array>", f);
        const char *args[] = {obp, "sync", "--continuous", "--path", b->root};
        for (int i = 0; i < 5; i++) { fputs("<string>", f); xml_esc(f, args[i]); fputs("</string>", f); }
        fputs("</array>\n<key>EnvironmentVariables</key><dict><key>HOME</key><string>", f); xml_esc(f, home);
        fputs("</string><key>PATH</key><string>", f); xml_esc(f, getenv("PATH") ? getenv("PATH") : "/usr/bin:/bin");
        fputs("</string></dict>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n<key>StandardOutPath</key><string>", f); xml_esc(f, logf);
        fputs("</string><key>StandardErrorPath</key><string>", f); xml_esc(f, logf); fputs("</string>\n</dict></plist>\n", f);
        fclose(f);
        snprintf(domain, sizeof(domain), "gui/%d", (int)getuid());
        char *argv[] = {lc, "bootstrap", domain, plist, NULL};
        if (!ob_run(argv, NULL, 0, c->timeout, &r)) { snprintf(why, cap, "launchctl could not be run"); return EX_FAIL; }
        int st = r.status; char err[400]; snprintf(err, sizeof(err), "%.380s", r.err); ob_run_free(&r);
        if (st) { unlink(plist); snprintf(why, cap, "launchctl bootstrap failed (%d): %s", st, err); return EX_FAIL; }
        bf_jsonw_key(w, "supervisor"); bf_jsonw_str(w, "launchd");
        bf_jsonw_key(w, "label"); bf_jsonw_str(w, label);
        bf_jsonw_key(w, "plist"); bf_jsonw_str(w, plist);
        bf_jsonw_key(w, "log"); bf_jsonw_str(w, logf);
        return EX_OK;
    }
    pid_t child = fork();
    if (child < 0) { snprintf(why, cap, "fork: %s", strerror(errno)); return EX_FAIL; }
    if (child == 0) {
        int nul = open("/dev/null", O_RDONLY), lg = open(logf, O_WRONLY | O_CREAT | O_APPEND, 0600);
        setsid();
        if (nul >= 0) dup2(nul, 0);
        if (lg >= 0) { dup2(lg, 1); dup2(lg, 2); }
        child_env(home, token_for(c, b->account));
        char *argv[] = {obp, "sync", "--continuous", "--path", (char *)b->root, NULL};
        execv(obp, argv);
        _exit(127);
    }
    { char t[32]; int n = snprintf(t, sizeof(t), "%ld\n", (long)child); if (ob_atomic_write(pidf, t, (size_t)n, 0)) { snprintf(why, cap, "cannot write pidfile"); kill(child, SIGTERM); return EX_FAIL; } }
    struct timespec ts = {0, 500 * 1000 * 1000}; nanosleep(&ts, NULL);
    int st; pid_t done = waitpid(child, &st, WNOHANG);
    if (done == child) {
        unlink(pidf);
        snprintf(why, cap, "ob sync --continuous exited at once with status %d; see %s", WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st), logf);
        return EX_FAIL;
    }
    bf_jsonw_key(w, "supervisor"); bf_jsonw_str(w, "child");
    bf_jsonw_key(w, "pid"); bf_jsonw_int(w, child);
    bf_jsonw_key(w, "pidfile"); bf_jsonw_str(w, pidf);
    bf_jsonw_key(w, "log"); bf_jsonw_str(w, logf);
    return EX_OK;
}

int ob_sup_stop(const Cfg *c, const Binding *b, bf_jsonw *w, char *why, size_t cap) {
    char pidf[OB_PATH], logf[OB_PATH], label[256], plist[OB_PATH]; long pid;
    ob_sup_paths(c, b, pidf, logf, label, plist);
    if (launchd_mode(c)) {
        char lc[OB_PATH], target[320]; RunResult r; int was = ob_sup_alive(c, b, &pid);
        if (!ob_which(c->launchctl, lc)) { snprintf(why, cap, "launchctl not found"); return EX_MISSING; }
        if (was) {
            snprintf(target, sizeof(target), "gui/%d/%s", (int)getuid(), label);
            char *argv[] = {lc, "bootout", target, NULL};
            if (ob_run(argv, NULL, 0, c->timeout, &r)) ob_run_free(&r);
        }
        unlink(plist);
        bf_jsonw_key(w, "was_running"); bf_jsonw_bool(w, was);
        bf_jsonw_key(w, "supervisor"); bf_jsonw_str(w, "launchd");
        return EX_OK;
    }
    long p = read_pid(pidf);
    int ours = p && pid_is_ours(p, b->root);
    if (ours) {
        kill((pid_t)p, SIGTERM);
        for (int i = 0; i < 50 && !kill((pid_t)p, 0); i++) { struct timespec ts = {0, 100 * 1000 * 1000}; waitpid((pid_t)p, NULL, WNOHANG); nanosleep(&ts, NULL); }
        if (!kill((pid_t)p, 0)) kill((pid_t)p, SIGKILL);
    }
    if (p) unlink(pidf);
    bf_jsonw_key(w, "was_running"); bf_jsonw_bool(w, ours);
    bf_jsonw_key(w, "stale_pidfile"); bf_jsonw_bool(w, p && !ours);
    bf_jsonw_key(w, "supervisor"); bf_jsonw_str(w, "child");
    (void)why; (void)cap;
    return EX_OK;
}
