/* bonfyre-obsidian: Obsidian vaults bound to Bonfyre accounts.
 *
 * One organ over both official Obsidian tools:
 *   - the desktop app's CLI (`obsidian`), used for reads and writes when the
 *     app is already running on this machine, so edits land through the app's
 *     own index and editor state;
 *   - Obsidian Headless (`ob`), used for Obsidian Sync without the app, run
 *     per account with its own HOME so each account has its own login;
 *   - and its own native readers and writers over the vault's files, which
 *     answer when neither tool is there (a server, the app closed).
 *
 * Every verb but doctor takes --account. A vault folder is bound to exactly
 * one account; any verb naming a vault another account bound is refused the
 * same way as a vault nobody bound. A folder must resolve (after symlinks)
 * inside an allowed root. Headless sync is refused wherever desktop Sync is on
 * for the same vault, as Obsidian requires.
 *
 * Output: one JSON object on stdout for every run, success or not, written
 * with bf_jsonw. The contract (verbs, shapes, exit codes) is README.md.
 */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#else
#define _GNU_SOURCE
#endif
#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "obsidian.h"
#include "bonfyre.h"

#define MAX_CONTENT (1u << 20)

static int g_argc; static char **g_argv;
static char g_code[48], g_reason[1536], g_hand[1536], g_detail[2048];
static Cfg C;

static const char *opt(const char *name) {
    for (int i = 2; i + 1 < g_argc; i++) if (!strcmp(g_argv[i], name)) return g_argv[i + 1];
    return NULL;
}
static int flag(const char *name) { for (int i = 2; i < g_argc; i++) if (!strcmp(g_argv[i], name)) return 1; return 0; }

static int fail(int rc, const char *code, const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    snprintf(g_code, sizeof(g_code), "%s", code);
    vsnprintf(g_reason, sizeof(g_reason), fmt, ap);
    va_end(ap);
    return rc;
}

static void iso_now(char out[32]) { time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm); strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tm); }

static int valid_account(const char *a) {
    size_t n = a ? strlen(a) : 0;
    if (!n || n > 128 || a[0] == '.') return 0;
    const char *colon = strchr(a, ':');
    if (!colon) return strspn(a, "abcdefghijklmnopqrstuvwxyz0123456789-_.@+") == n;
    /* The canonical account user:19 must stay distinct from user-19. */
    if (colon != a + 4 || strncmp(a, "user:", 5) || !a[5] || a[5] == '0') return 0;
    for (const char *p = a + 5; *p; ++p) if (*p < '0' || *p > '9') return 0;
    return 1;
}
static int in_set(const char *v, const char *const *set) { for (; *set; set++) if (!strcmp(v, *set)) return 1; return 0; }
static const char *const TRANSPORTS[] = {"cli", "headless", "files", NULL};
static const char *const MODES[] = {"bidirectional", "pull-only", "mirror-remote", NULL};
static const char *const CONFLICTS[] = {"merge", "conflict", NULL};

/* ------------------------------------------------------------ configuration */
static const char *envd(const char *k, const char *d) { const char *v = getenv(k); return v && *v ? v : d; }

static void cfg_init(void) {
    static char db[OB_PATH], state[OB_PATH], roots[OB_PATH], app_support[OB_PATH], agents[OB_PATH];
    const char *h = getenv("HOME");
    snprintf(C.home, sizeof(C.home), "%s", h ? h : "");
    snprintf(db, sizeof(db), "%s/.bonfyre/state/obsidian.db", C.home);
    snprintf(state, sizeof(state), "%s/.bonfyre/state/obsidian", C.home);
    snprintf(roots, sizeof(roots), "%s/Documents", C.home);
    snprintf(app_support, sizeof(app_support), "%s/Library/Application Support/obsidian", C.home);
    snprintf(agents, sizeof(agents), "%s/Library/LaunchAgents", C.home);
    C.db = opt("--db") ? opt("--db") : envd("BONFYRE_OBSIDIAN_DB", db);
    C.state = envd("BONFYRE_OBSIDIAN_STATE", state);
    C.roots = opt("--roots") ? opt("--roots") : envd("BONFYRE_OBSIDIAN_ROOTS", roots);
    C.cli = envd("OBSIDIAN_CLI", NULL);
    C.ob = envd("BONFYRE_OBSIDIAN_OB", NULL);
    C.node = envd("BONFYRE_OBSIDIAN_NODE", "node");
    C.app = envd("BONFYRE_OBSIDIAN_APP", "/Applications/Obsidian.app");
    C.app_support = envd("BONFYRE_OBSIDIAN_APP_SUPPORT", app_support);
    C.supervisor = opt("--supervisor") ? opt("--supervisor") : envd("BONFYRE_OBSIDIAN_SUPERVISOR", "child");
    C.launchctl = envd("BONFYRE_OBSIDIAN_LAUNCHCTL", "/bin/launchctl");
    C.agents_dir = envd("BONFYRE_OBSIDIAN_LAUNCH_AGENTS", agents);
    C.app_running = envd("BONFYRE_OBSIDIAN_APP_RUNNING", NULL);
    C.token_account = envd("BONFYRE_OBSIDIAN_TOKEN_ACCOUNT", NULL);
    C.cli_launch = atoi(envd("BONFYRE_OBSIDIAN_CLI_LAUNCH", "0")) != 0;
    C.timeout = atoi(envd("BONFYRE_OBSIDIAN_TIMEOUT", "10")); if (C.timeout <= 0) C.timeout = 10;
    C.account = opt("--account");
    C.actor = opt("--actor") ? opt("--actor") : "operator-cli";
    C.via = opt("--via") ? opt("--via") : "auto";
}

static int sync_timeout(void) { int t = atoi(envd("BONFYRE_OBSIDIAN_SYNC_TIMEOUT", "900")); return t > 0 ? t : 900; }

static int root_allowed(const char *real) {
    char buf[8192]; snprintf(buf, sizeof(buf), "%s", C.roots);
    for (char *save = NULL, *r = strtok_r(buf, ":", &save); r; r = strtok_r(NULL, ":", &save)) {
        char rr[OB_PATH]; if (realpath(r, rr) && ob_inside(rr, real)) return 1;
    }
    return 0;
}

/* ------------------------------------------------------------ store */
static const char *SCHEMA =
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE IF NOT EXISTS binding(account TEXT NOT NULL, name TEXT NOT NULL, vault_id TEXT NOT NULL, local_path TEXT NOT NULL UNIQUE,"
    " transport TEXT NOT NULL, sync_mode TEXT NOT NULL, conflict_strategy TEXT NOT NULL, excluded_folders TEXT NOT NULL, device_name TEXT NOT NULL,"
    " remote_vault TEXT NOT NULL, config_dir TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(account, name));"
    "CREATE TABLE IF NOT EXISTS receipt(receipt_id TEXT PRIMARY KEY, at TEXT NOT NULL, account TEXT NOT NULL, actor TEXT NOT NULL, verb TEXT NOT NULL,"
    " binding TEXT NOT NULL, target TEXT NOT NULL, outcome TEXT NOT NULL, code TEXT NOT NULL, reason TEXT NOT NULL, via TEXT NOT NULL,"
    " sha_before TEXT NOT NULL, sha_after TEXT NOT NULL);"
    "CREATE INDEX IF NOT EXISTS receipt_account ON receipt(account, at);"
    "CREATE TABLE IF NOT EXISTS sync_run(run_id INTEGER PRIMARY KEY, receipt_id TEXT NOT NULL, account TEXT NOT NULL, binding TEXT NOT NULL, kind TEXT NOT NULL,"
    " argv TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, exit_code INTEGER NOT NULL, outcome TEXT NOT NULL, log_path TEXT NOT NULL, pid INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS snapshot(snapshot_id INTEGER PRIMARY KEY, account TEXT NOT NULL, binding TEXT NOT NULL, taken_at TEXT NOT NULL, files INTEGER NOT NULL,"
    " bytes INTEGER NOT NULL, manifest_digest TEXT NOT NULL, rehashed INTEGER NOT NULL, reused INTEGER NOT NULL, include_config INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS snapshot_file(snapshot_id INTEGER NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,"
    " PRIMARY KEY(snapshot_id, path));";

static sqlite3 *store_open(void) {
    sqlite3 *db = NULL; char dir[OB_PATH]; const char *s = strrchr(C.db, '/'); char *err = NULL;
    if (s && s != C.db) { snprintf(dir, sizeof(dir), "%.*s", (int)(s - C.db), C.db); ob_mkdirs(dir, 0700); }
    if (sqlite3_open_v2(C.db, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) { sqlite3_close(db); return NULL; }
    sqlite3_busy_timeout(db, 5000);
    if (sqlite3_exec(db, SCHEMA, NULL, NULL, &err) != SQLITE_OK) { fprintf(stderr, "obsidian: store: %s\n", err ? err : ""); sqlite3_free(err); sqlite3_close(db); return NULL; }
    return db;
}

static void col(sqlite3_stmt *st, int i, char *out, size_t cap) { const unsigned char *t = sqlite3_column_text(st, i); snprintf(out, cap, "%s", t ? (const char *)t : ""); }

#define BINDING_COLS "account,name,vault_id,local_path,transport,sync_mode,conflict_strategy,excluded_folders,device_name,remote_vault,config_dir,state,created_at,updated_at"
static void binding_row(sqlite3_stmt *st, Binding *b) {
    col(st, 0, b->account, sizeof(b->account)); col(st, 1, b->name, sizeof(b->name)); col(st, 2, b->vault_id, sizeof(b->vault_id));
    col(st, 3, b->root, sizeof(b->root)); col(st, 4, b->transport, sizeof(b->transport)); col(st, 5, b->sync_mode, sizeof(b->sync_mode));
    col(st, 6, b->conflict, sizeof(b->conflict)); col(st, 7, b->excluded, sizeof(b->excluded)); col(st, 8, b->device, sizeof(b->device));
    col(st, 9, b->remote, sizeof(b->remote)); col(st, 10, b->config_dir, sizeof(b->config_dir)); col(st, 11, b->state, sizeof(b->state));
    col(st, 12, b->created_at, sizeof(b->created_at)); col(st, 13, b->updated_at, sizeof(b->updated_at));
}

/* Another account's vault and a vault nobody bound get the same answer, so a
 * caller cannot probe which names other accounts use. */
static int binding_load(sqlite3 *db, const char *name, Binding *b) {
    sqlite3_stmt *st; int rc = EX_REFUSED;
    if (!name || !*name) return fail(EX_USAGE, "usage", "--name is required");
    if (sqlite3_prepare_v2(db, "SELECT " BINDING_COLS " FROM binding WHERE account=? AND name=?", -1, &st, NULL) != SQLITE_OK) return fail(EX_FAIL, "store", "%s", sqlite3_errmsg(db));
    sqlite3_bind_text(st, 1, C.account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, name, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) { binding_row(st, b); rc = EX_OK; }
    sqlite3_finalize(st);
    if (rc) return fail(EX_REFUSED, "not-bound", "no vault named %s is bound to account %s", name, C.account);
    struct stat sb;
    if (stat(b->root, &sb) || !S_ISDIR(sb.st_mode)) return fail(EX_NOTFOUND, "vault-missing", "the folder bound as %s (%s) is gone", name, b->root);
    return EX_OK;
}

static void binding_state(sqlite3 *db, const Binding *b, const char *state) {
    sqlite3_stmt *st; char at[32]; iso_now(at);
    if (sqlite3_prepare_v2(db, "UPDATE binding SET state=?, updated_at=? WHERE account=? AND name=?", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, state, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, at, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, b->account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 4, b->name, -1, SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
}

/* A receipt names what was done, by whom, to which note and with which bytes
 * before and after; never content, never a credential. */
static void receipt(sqlite3 *db, const char *verb, const char *binding, const char *target, const char *outcome, const char *code,
                    const char *reason, const char *via, const char *before, const char *after, char id[32]) {
    char at[32], seed[2048], hex[65]; struct timeval tv; sqlite3_stmt *st;
    iso_now(at); gettimeofday(&tv, NULL);
    snprintf(seed, sizeof(seed), "%s|%ld.%06ld|%d|%s|%s|%s|%s|%s", at, (long)tv.tv_sec, (long)tv.tv_usec, (int)getpid(), C.account ? C.account : "", verb, binding, target, outcome);
    ob_sha256_mem(seed, strlen(seed), hex);
    snprintf(id, 32, "obr-%.24s", hex);
    if (!db || sqlite3_prepare_v2(db, "INSERT INTO receipt VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return;
    const char *v[] = {id, at, C.account ? C.account : "", C.actor, verb, binding, target, outcome, code, reason, via, before ? before : "", after ? after : ""};
    for (int i = 0; i < 13; i++) sqlite3_bind_text(st, i + 1, v[i], -1, SQLITE_TRANSIENT);
    sqlite3_step(st); sqlite3_finalize(st);
}

static void put_binding(bf_jsonw *w, const Binding *b) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "account"); bf_jsonw_str(w, b->account);
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b->name);
    bf_jsonw_key(w, "vault_id"); bf_jsonw_str(w, b->vault_id);
    bf_jsonw_key(w, "local_path"); bf_jsonw_str(w, b->root);
    bf_jsonw_key(w, "transport"); bf_jsonw_str(w, b->transport);
    bf_jsonw_key(w, "sync_mode"); bf_jsonw_str(w, b->sync_mode);
    bf_jsonw_key(w, "conflict_strategy"); bf_jsonw_str(w, b->conflict);
    bf_jsonw_key(w, "excluded_folders"); bf_jsonw_arr(w);
    { char buf[1024]; snprintf(buf, sizeof(buf), "%s", b->excluded); for (char *s = NULL, *t = strtok_r(buf, ",", &s); t; t = strtok_r(NULL, ",", &s)) bf_jsonw_str(w, t); }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "device_name"); bf_jsonw_str(w, b->device);
    bf_jsonw_key(w, "remote_vault"); if (b->remote[0]) bf_jsonw_str(w, b->remote); else bf_jsonw_null(w);
    bf_jsonw_key(w, "config_dir"); bf_jsonw_str(w, b->config_dir);
    bf_jsonw_key(w, "state"); bf_jsonw_str(w, b->state);
    bf_jsonw_key(w, "created_at"); bf_jsonw_str(w, b->created_at);
    bf_jsonw_key(w, "updated_at"); bf_jsonw_str(w, b->updated_at);
    bf_jsonw_obj_end(w);
}

static void put_login(bf_jsonw *w, const char *account) {
    char home[OB_PATH], tok[OB_PATH]; struct stat st;
    ob_account_home(&C, account, home);
    snprintf(tok, sizeof(tok), "%s/.obsidian-headless/auth_token", home);
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "home"); bf_jsonw_str(w, home);
    bf_jsonw_key(w, "token_file_present"); bf_jsonw_bool(w, !stat(tok, &st) && st.st_size > 0);
    bf_jsonw_key(w, "env_token_applies"); bf_jsonw_bool(w, getenv("OBSIDIAN_AUTH_TOKEN") && C.token_account && !strcmp(C.token_account, account));
    bf_jsonw_key(w, "login_command"); { char cmd[OB_PATH + 64]; snprintf(cmd, sizeof(cmd), "HOME='%s' ob login", home); bf_jsonw_str(w, cmd); }
    bf_jsonw_obj_end(w);
}

static void put_guard(bf_jsonw *w, const Guard *g) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "decision"); bf_jsonw_str(w, g->refuse ? "refuse" : "allow");
    bf_jsonw_key(w, "code"); bf_jsonw_str(w, g->code);
    bf_jsonw_key(w, "reason"); bf_jsonw_str(w, g->reason);
    bf_jsonw_key(w, "evidence"); if (g->evidence[0]) bf_jsonw_str(w, g->evidence); else bf_jsonw_null(w);
    bf_jsonw_obj_end(w);
}

/* ------------------------------------------------------------ desktop CLI */
static int use_cli(const Binding *b, char cli[OB_PATH], int *forced) {
    *forced = !strcmp(C.via, "cli");
    if (!strcmp(C.via, "native")) return 0;
    if (!*forced && strcmp(b->transport, "cli")) return 0;
    return ob_cli_usable(&C, cli);
}

static void cli_vault_arg(const Binding *b, char *out, size_t cap) {
    if (strncmp(b->vault_id, "local-", 6)) snprintf(out, cap, "vault=%s", b->vault_id);
    else { const char *s = strrchr(b->root, '/'); snprintf(out, cap, "vault=%s", s ? s + 1 : b->root); }
}

/* Runs `obsidian vault=V <args...>`. args are NULL-terminated key=value or flags. */
static int cli_call(const char *cli, const Binding *b, const char *const *args, RunResult *r) {
    char vault[OB_PATH + 8]; char *argv[24]; int n = 0;
    cli_vault_arg(b, vault, sizeof(vault));
    argv[n++] = (char *)cli; argv[n++] = vault;
    for (; *args && n < 22; args++) argv[n++] = (char *)*args;
    argv[n] = NULL;
    if (!ob_run(argv, NULL, 0, C.timeout, r)) return 0;
    return r->status == 0 && !r->timed_out;
}

/* The CLI takes \n and \t inside a content= value for a newline and a tab. */
static char *cli_escape(const char *s, size_t n) {
    char *o = malloc(n * 2 + 1), *p = o; if (!o) return NULL;
    for (size_t i = 0; i < n; i++) { if (s[i] == '\n') { *p++ = '\\'; *p++ = 'n'; } else if (s[i] == '\t') { *p++ = '\\'; *p++ = 't'; } else *p++ = s[i]; }
    *p = 0; return o;
}

static void put_cli_result(bf_jsonw *w, const char *command, const RunResult *r) {
    char err[64]; bf_json_doc_t *d = r->out_len ? bf_json_parse(r->out, r->out_len, err, sizeof(err)) : NULL;
    bf_jsonw_key(w, "shape"); bf_jsonw_str(w, "obsidian-cli");
    bf_jsonw_key(w, "via"); bf_jsonw_str(w, "cli");
    bf_jsonw_key(w, "command"); bf_jsonw_str(w, command);
    if (d) { bf_jsonw_key(w, "result"); bf_jsonw_node(w, d, bf_json_root(d), 0); bf_json_free(d); }
    else { bf_jsonw_key(w, "text"); bf_jsonw_strn(w, r->out, r->out_len); }
}

/* ------------------------------------------------------------ verbs: setup */
static int need_account(void) {
    if (!C.account) return fail(EX_USAGE, "usage", "--account is required");
    if (!valid_account(C.account)) return fail(EX_USAGE, "usage", "--account requires a safe account key or user:<positive decimal id>");
    return EX_OK;
}

/* Hosted Headless Sync is optional; inspect bindings without creating state. */
static int account_has_headless_binding(void) {
    if (!C.account || !valid_account(C.account)) return 0;
    sqlite3 *db=NULL; sqlite3_stmt *st=NULL; int present=0;
    if (sqlite3_open_v2(C.db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,"SELECT 1 FROM binding WHERE account=? AND transport='headless' LIMIT 1",-1,&st,NULL)==SQLITE_OK) {
        sqlite3_bind_text(st,1,C.account,-1,SQLITE_STATIC);
        present=sqlite3_step(st)==SQLITE_ROW;
    }
    sqlite3_finalize(st); sqlite3_close(db); return present;
}
static int v_doctor(bf_jsonw *w) {
    char p[OB_PATH], plist[OB_PATH], ver[64] = "", cli[OB_PATH], obp[OB_PATH], node[OB_PATH]; int running = ob_app_running(&C), hand = 0;
    struct stat st; RunResult r;
    bf_jsonw_key(w, "app"); bf_jsonw_obj(w);
    snprintf(plist, sizeof(plist), "%s/Contents/Info.plist", C.app);
    char *txt = ob_read_file(plist, NULL, 1u << 20);
    if (txt) { char *k = strstr(txt, "<key>CFBundleShortVersionString</key>"); char *s = k ? strstr(k, "<string>") : NULL; if (s) sscanf(s + 8, "%63[^<]", ver); free(txt); }
    bf_jsonw_key(w, "path"); bf_jsonw_str(w, C.app);
    bf_jsonw_key(w, "installed"); bf_jsonw_bool(w, !stat(C.app, &st));
    bf_jsonw_key(w, "version"); if (ver[0]) bf_jsonw_str(w, ver); else bf_jsonw_null(w);
    bf_jsonw_key(w, "running"); bf_jsonw_bool(w, running);
    bf_jsonw_obj_end(w);

    int registered = C.cli && *C.cli ? ob_which(C.cli, p) : ob_which("obsidian", p);
    int available = ob_cli_path(&C, cli), cli_ready = 0;
    snprintf(p, sizeof(p), "%s/Contents/MacOS/obsidian-cli", C.app);
    bf_jsonw_key(w, "cli"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "registered"); bf_jsonw_bool(w, registered);
    bf_jsonw_key(w, "path"); if (available) bf_jsonw_str(w, cli); else bf_jsonw_null(w);
    bf_jsonw_key(w, "bundled"); if (!access(p, X_OK)) bf_jsonw_str(w, p); else bf_jsonw_null(w);
    bf_jsonw_key(w, "reachable");
    if (available && (running || C.cli_launch) && strcmp(C.via, "native")) {
        char *argv[] = {cli, "version", NULL};
        int ok = ob_run(argv, NULL, 0, C.timeout, &r) && ob_cli_version_ok(&r);
        cli_ready = ok;
        bf_jsonw_bool(w, ok);
        bf_jsonw_key(w, "version"); if (ok) { size_t n = r.out_len; while (n && isspace((unsigned char)r.out[n - 1])) n--; bf_jsonw_strn(w, r.out, n); } else bf_jsonw_null(w);
        if (r.spawned) ob_run_free(&r);
    } else bf_jsonw_null(w);
    bf_jsonw_obj_end(w);

    bf_jsonw_key(w, "headless"); bf_jsonw_obj(w);
    int has_ob = ob_ob_path(&C, obp), node_major = 0;
    bf_jsonw_key(w, "installed"); bf_jsonw_bool(w, has_ob);
    bf_jsonw_key(w, "path"); if (has_ob) bf_jsonw_str(w, obp); else bf_jsonw_null(w);
    bf_jsonw_key(w, "version");
    if (has_ob) { char *argv[] = {obp, "--version", NULL}; if (ob_run(argv, NULL, 0, C.timeout, &r) && !r.status) { size_t n = r.out_len; while (n && isspace((unsigned char)r.out[n - 1])) n--; bf_jsonw_strn(w, r.out, n); } else bf_jsonw_null(w); if (r.spawned) ob_run_free(&r); }
    else bf_jsonw_null(w);
    bf_jsonw_key(w, "node"); bf_jsonw_obj(w);
    if (ob_which(C.node, node)) {
        char *argv[] = {node, "--version", NULL};
        bf_jsonw_key(w, "path"); bf_jsonw_str(w, node);
        bf_jsonw_key(w, "version");
        if (ob_run(argv, NULL, 0, C.timeout, &r) && !r.status) { size_t n = r.out_len; while (n && isspace((unsigned char)r.out[n - 1])) n--; bf_jsonw_strn(w, r.out, n); node_major = atoi(r.out + (r.out[0] == 'v')); } else bf_jsonw_null(w);
        if (r.spawned) ob_run_free(&r);
    } else { bf_jsonw_key(w, "path"); bf_jsonw_null(w); bf_jsonw_key(w, "version"); bf_jsonw_null(w); }
    bf_jsonw_key(w, "meets_22"); bf_jsonw_bool(w, node_major >= 22);
    bf_jsonw_obj_end(w);
    { char tok[OB_PATH]; snprintf(tok, sizeof(tok), "%s/.obsidian-headless/auth_token", C.home);
      bf_jsonw_key(w, "unscoped_login_present"); bf_jsonw_bool(w, !stat(tok, &st) && st.st_size > 0); }
    bf_jsonw_key(w, "env_token_present"); bf_jsonw_bool(w, getenv("OBSIDIAN_AUTH_TOKEN") != NULL);
    bf_jsonw_key(w, "env_token_account"); if (C.token_account) bf_jsonw_str(w, C.token_account); else bf_jsonw_null(w);
    bf_jsonw_obj_end(w);

    if (C.account && valid_account(C.account)) { bf_jsonw_key(w, "account"); bf_jsonw_str(w, C.account); bf_jsonw_key(w, "login"); put_login(w, C.account); }
    bf_jsonw_key(w, "native"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "local_file_io"); bf_jsonw_bool(w, 1);
    bf_jsonw_key(w, "snapshots_and_diff"); bf_jsonw_bool(w, 1);
    bf_jsonw_key(w, "obsidian_subscription_required"); bf_jsonw_bool(w, 0);
    bf_jsonw_key(w, "cross_device_replication_active"); bf_jsonw_bool(w, 0);
    bf_jsonw_key(w, "cross_device_replication"); bf_jsonw_str(w, "not-wired");
    bf_jsonw_obj_end(w);
    bf_jsonw_key(w, "desktop_vaults"); ob_registry_json(&C, w);
    bf_jsonw_key(w, "store"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "db"); bf_jsonw_str(w, C.db);
    bf_jsonw_key(w, "exists"); bf_jsonw_bool(w, !stat(C.db, &st));
    bf_jsonw_key(w, "state"); bf_jsonw_str(w, C.state);
    bf_jsonw_key(w, "roots"); bf_jsonw_arr(w);
    { char buf[8192]; snprintf(buf, sizeof(buf), "%s", C.roots); for (char *s = NULL, *t = strtok_r(buf, ":", &s); t; t = strtok_r(NULL, ":", &s)) bf_jsonw_str(w, t); }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "supervisor"); bf_jsonw_str(w, C.supervisor);
    bf_jsonw_obj_end(w);

    /* What only a person can do, each as the exact step. */
    int hosted_binding=account_has_headless_binding();
    bf_jsonw_key(w, "by_hand"); bf_jsonw_arr(w);
    if (!available || !cli_ready) {
        bf_jsonw_obj(w); bf_jsonw_key(w, "why"); bf_jsonw_str(w, "the desktop CLI is not enabled, not available or not reachable; native file reading remains available");
        bf_jsonw_key(w, "do"); bf_jsonw_str(w, "In Obsidian open Settings > General and turn on Command line interface (it registers /usr/local/bin/obsidian)."); bf_jsonw_obj_end(w); hand++;
    }
    if (hosted_binding && !has_ob) {
        bf_jsonw_obj(w); bf_jsonw_key(w, "why"); bf_jsonw_str(w, "Obsidian Headless is not installed");
        bf_jsonw_key(w, "do"); bf_jsonw_str(w, "npm install -g obsidian-headless"); bf_jsonw_obj_end(w); hand++;
    } else if (hosted_binding && C.account && valid_account(C.account)) {
        char home[OB_PATH], tok[OB_PATH], cmd[OB_PATH + 64];
        ob_account_home(&C, C.account, home); snprintf(tok, sizeof(tok), "%s/.obsidian-headless/auth_token", home);
        if (stat(tok, &st) && !(getenv("OBSIDIAN_AUTH_TOKEN") && C.token_account && !strcmp(C.token_account, C.account))) {
            snprintf(cmd, sizeof(cmd), "mkdir -p '%s' && HOME='%s' ob login", home, home);
            bf_jsonw_obj(w); bf_jsonw_key(w, "why"); bf_jsonw_str(w, "this account has no Obsidian login for Headless Sync (an Obsidian Sync subscription is required)");
            bf_jsonw_key(w, "do"); bf_jsonw_str(w, cmd); bf_jsonw_obj_end(w); hand++;
        }
    }
    bf_jsonw_arr_end(w);
    (void)hand;
    return EX_OK;
}

static int v_vaults(sqlite3 *db, bf_jsonw *w) {
    sqlite3_stmt *st;
    bf_jsonw_key(w, "account"); bf_jsonw_str(w, C.account);
    bf_jsonw_key(w, "bindings"); bf_jsonw_arr(w);
    if (sqlite3_prepare_v2(db, "SELECT " BINDING_COLS " FROM binding WHERE account=? ORDER BY name", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, C.account, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) { Binding b; binding_row(st, &b); put_binding(w, &b); }
        sqlite3_finalize(st);
    }
    bf_jsonw_arr_end(w);
    /* This machine's desktop vaults, with whether each is bound here; who
     * bound another is not said. */
    bf_jsonw_key(w, "desktop_vaults"); {
        bf_jsonw tmp; bf_jsonw_init(&tmp, 0); ob_registry_json(&C, &tmp);
        char err[64]; bf_json_doc_t *d = bf_json_parse(tmp.buf, tmp.len, err, sizeof(err));
        bf_jsonw_arr(w);
        for (const bf_json_node_t *v = d ? bf_json_child_first(d, bf_json_root(d)) : NULL; v; v = bf_json_child_next(d, v)) {
            const bf_json_node_t *rp = bf_json_obj_get(d, v, "real_path"); char real[OB_PATH] = ""; int mine = 0, any = 0;
            if (rp) bf_json_get_str_copy(rp, real, sizeof(real));
            if (real[0] && sqlite3_prepare_v2(db, "SELECT account FROM binding WHERE local_path=?", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, real, -1, SQLITE_STATIC);
                if (sqlite3_step(st) == SQLITE_ROW) { any = 1; mine = !strcmp((const char *)sqlite3_column_text(st, 0), C.account); }
                sqlite3_finalize(st);
            }
            bf_jsonw_obj(w);
            for (const bf_json_node_t *f = bf_json_child_first(d, v); f; f = bf_json_child_next(d, f)) { bf_jsonw_keyn(w, f->key, (size_t)f->key_len); bf_jsonw_node(w, d, f, 0); }
            bf_jsonw_key(w, "bound_to_this_account"); bf_jsonw_bool(w, mine);
            bf_jsonw_key(w, "bound_elsewhere"); bf_jsonw_bool(w, any && !mine);
            bf_jsonw_key(w, "allowed_root"); bf_jsonw_bool(w, real[0] && root_allowed(real));
            bf_jsonw_obj_end(w);
        }
        bf_jsonw_arr_end(w);
        if (d) bf_json_free(d);
        bf_jsonw_free(&tmp);
    }
    return EX_OK;
}

static int v_bind(sqlite3 *db, bf_jsonw *w) {
    const char *name = opt("--name"), *path = opt("--path"), *transport = opt("--transport") ? opt("--transport") : "files";
    const char *mode = opt("--mode") ? opt("--mode") : "bidirectional", *conflict = opt("--conflict") ? opt("--conflict") : "merge";
    const char *excl = opt("--exclude") ? opt("--exclude") : "", *remote = opt("--remote-vault") ? opt("--remote-vault") : "";
    const char *cfgdir = opt("--config-dir") ? opt("--config-dir") : ".obsidian";
    char device[128], real[OB_PATH], vid[64] = "", at[32], rid[32]; struct stat sb; sqlite3_stmt *st; int open_ = 0;
    snprintf(device, sizeof(device), "%s", opt("--device-name") ? opt("--device-name") : "");
    if (!device[0]) snprintf(device, sizeof(device), "bonfyre-%s", C.account);
    if (!name || !path) return fail(EX_USAGE, "usage", "bind needs --name and --path");
    if (!ob_valid_name(name, 64)) return fail(EX_USAGE, "usage", "--name takes letters, digits, '.', '_' and '-' (up to 64, not dot- or dash-led)");
    if (!in_set(transport, TRANSPORTS)) return fail(EX_USAGE, "usage", "--transport is cli, headless or files");
    if (!in_set(mode, MODES)) return fail(EX_USAGE, "usage", "--mode is bidirectional, pull-only or mirror-remote");
    if (!in_set(conflict, CONFLICTS)) return fail(EX_USAGE, "usage", "--conflict is merge or conflict");
    if (cfgdir[0] != '.' || strchr(cfgdir, '/') || strlen(cfgdir) < 2 || strlen(cfgdir) > 63) return fail(EX_USAGE, "usage", "--config-dir is a dot-led folder name such as .obsidian");
    if (strlen(excl) >= 1000 || strlen(remote) >= 250 || strlen(device) >= 120) return fail(EX_USAGE, "usage", "an option value is too long");
    for (const char *s = device; *s; s++) if ((unsigned char)*s < 0x20) return fail(EX_USAGE, "usage", "--device-name holds a control character");
    if (!realpath(path, real) || stat(real, &sb) || !S_ISDIR(sb.st_mode)) return fail(EX_NOTFOUND, "no-folder", "%s is not a folder", path);
    if (!root_allowed(real)) { char rid2[32]; receipt(db, "bind", name, real, "refused", "outside-roots", "outside allowed roots", "native", "", "", rid2);
        return fail(EX_REFUSED, "outside-roots", "%s resolves to %s, which is outside the allowed roots (%s); set BONFYRE_OBSIDIAN_ROOTS to admit it", path, real, C.roots); }
    if (sqlite3_prepare_v2(db, "SELECT account,name,local_path FROM binding", -1, &st, NULL) != SQLITE_OK) return fail(EX_FAIL, "store", "%s", sqlite3_errmsg(db));
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *a = (const char *)sqlite3_column_text(st, 0), *n = (const char *)sqlite3_column_text(st, 1), *lp = (const char *)sqlite3_column_text(st, 2);
        int same_acct = !strcmp(a, C.account);
        if (same_acct && !strcmp(n, name)) { sqlite3_finalize(st); return fail(EX_REFUSED, "name-taken", "this account already has a vault named %s; unbind it first", name); }
        if (ob_inside(lp, real) || ob_inside(real, lp)) {
            int exact = !strcmp(lp, real);
            sqlite3_finalize(st);
            receipt(db, "bind", name, real, "refused", exact ? "already-bound" : "nested", "folder overlaps a bound vault", "native", "", "", rid);
            return fail(EX_REFUSED, exact ? "already-bound" : "nested", exact ? "this folder is already bound%s" : "this folder holds or sits inside a folder that is already bound%s",
                        same_acct ? " by this account" : "; a folder belongs to one account");
        }
    }
    sqlite3_finalize(st);
    if (!ob_registry_lookup(&C, real, vid, &open_)) { char hex[65]; ob_sha256_mem(real, strlen(real), hex); snprintf(vid, sizeof(vid), "local-%.16s", hex); }
    iso_now(at);
    if (sqlite3_prepare_v2(db, "INSERT INTO binding(" BINDING_COLS ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return fail(EX_FAIL, "store", "%s", sqlite3_errmsg(db));
    const char *v[] = {C.account, name, vid, real, transport, mode, conflict, excl, device, remote, cfgdir, "bound", at, at};
    for (int i = 0; i < 14; i++) sqlite3_bind_text(st, i + 1, v[i], -1, SQLITE_STATIC);
    int ok = sqlite3_step(st) == SQLITE_DONE; sqlite3_finalize(st);
    if (!ok) return fail(EX_REFUSED, "already-bound", "this folder is already bound");
    receipt(db, "bind", name, real, "ok", "bound", "", "native", "", "", rid);
    Binding b; binding_load(db, name, &b);
    bf_jsonw_key(w, "binding"); put_binding(w, &b);
    bf_jsonw_key(w, "desktop_registered"); bf_jsonw_bool(w, strncmp(vid, "local-", 6) != 0);
    int sp = ob_desktop_sync_plugin(real, cfgdir);
    bf_jsonw_key(w, "desktop_sync_plugin"); if (sp < 0) bf_jsonw_null(w); else bf_jsonw_bool(w, sp);
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    return EX_OK;
}

static int v_unbind(sqlite3 *db, bf_jsonw *w) {
    Binding b; long pid; char rid[32]; sqlite3_stmt *st; int rc;
    if ((rc = binding_load(db, opt("--name"), &b)) && rc != EX_NOTFOUND) return rc;
    if (!rc && ob_sup_alive(&C, &b, &pid)) return fail(EX_REFUSED, "syncing", "continuous sync is running for %s (pid %ld); run sync-stop first", b.name, pid);
    if (sqlite3_prepare_v2(db, "DELETE FROM snapshot_file WHERE snapshot_id IN (SELECT snapshot_id FROM snapshot WHERE account=? AND binding=?)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, C.account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, opt("--name"), -1, SQLITE_STATIC); sqlite3_step(st); sqlite3_finalize(st);
    }
    const char *sqls[] = {"DELETE FROM snapshot WHERE account=? AND binding=?", "DELETE FROM binding WHERE account=? AND name=?"};
    for (int i = 0; i < 2; i++) if (sqlite3_prepare_v2(db, sqls[i], -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, C.account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, opt("--name"), -1, SQLITE_STATIC); sqlite3_step(st); sqlite3_finalize(st);
    }
    receipt(db, "unbind", opt("--name"), "", "ok", "unbound", "", "native", "", "", rid);
    bf_jsonw_key(w, "unbound"); bf_jsonw_str(w, opt("--name"));
    bf_jsonw_key(w, "note"); bf_jsonw_str(w, "the vault's files and any Obsidian Headless setup for it are left as they are");
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    return EX_OK;
}

static void put_last_run(sqlite3 *db, bf_jsonw *w, const Binding *b) {
    sqlite3_stmt *st;
    bf_jsonw_key(w, "last_sync_run");
    if (sqlite3_prepare_v2(db, "SELECT run_id,kind,started_at,ended_at,exit_code,outcome,receipt_id FROM sync_run WHERE account=? AND binding=? ORDER BY run_id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, b->account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, b->name, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "run_id"); bf_jsonw_int(w, sqlite3_column_int64(st, 0));
            bf_jsonw_key(w, "kind"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 1));
            bf_jsonw_key(w, "started_at"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 2));
            bf_jsonw_key(w, "ended_at"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 3));
            bf_jsonw_key(w, "exit_code"); bf_jsonw_int(w, sqlite3_column_int64(st, 4));
            bf_jsonw_key(w, "outcome"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 5));
            bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 6));
            bf_jsonw_obj_end(w);
        } else bf_jsonw_null(w);
        sqlite3_finalize(st);
    } else bf_jsonw_null(w);
    bf_jsonw_key(w, "last_snapshot");
    if (sqlite3_prepare_v2(db, "SELECT snapshot_id,taken_at,files,bytes,manifest_digest FROM snapshot WHERE account=? AND binding=? ORDER BY snapshot_id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, b->account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, b->name, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "snapshot_id"); bf_jsonw_int(w, sqlite3_column_int64(st, 0));
            bf_jsonw_key(w, "taken_at"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 1));
            bf_jsonw_key(w, "files"); bf_jsonw_int(w, sqlite3_column_int64(st, 2));
            bf_jsonw_key(w, "bytes"); bf_jsonw_int(w, sqlite3_column_int64(st, 3));
            bf_jsonw_key(w, "manifest_digest"); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, 4));
            bf_jsonw_obj_end(w);
        } else bf_jsonw_null(w);
        sqlite3_finalize(st);
    } else bf_jsonw_null(w);
}

static void put_status_one(sqlite3 *db, bf_jsonw *w, const Binding *b) {
    long pid = 0; Guard g; char cli[OB_PATH];
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "binding"); put_binding(w, b);
    struct stat sb; bf_jsonw_key(w, "folder_present"); bf_jsonw_bool(w, !stat(b->root, &sb) && S_ISDIR(sb.st_mode));
    int sp = ob_desktop_sync_plugin(b->root, b->config_dir);
    bf_jsonw_key(w, "desktop_sync_plugin"); if (sp < 0) bf_jsonw_null(w); else bf_jsonw_bool(w, sp);
    bf_jsonw_key(w, "cli_usable"); bf_jsonw_bool(w, !strcmp(b->transport, "cli") && ob_cli_usable(&C, cli));
    if (!strcmp(b->transport, "headless")) {
        ob_guard_headless(&C, b, &g);
        bf_jsonw_key(w, "headless_guard"); put_guard(w, &g);
        int alive = ob_sup_alive(&C, b, &pid);
        bf_jsonw_key(w, "continuous_sync"); bf_jsonw_obj(w);
        bf_jsonw_key(w, "running"); bf_jsonw_bool(w, alive);
        bf_jsonw_key(w, "pid"); if (alive && pid) bf_jsonw_int(w, pid); else bf_jsonw_null(w);
        bf_jsonw_key(w, "supervisor"); bf_jsonw_str(w, C.supervisor);
        bf_jsonw_obj_end(w);
    }
    put_last_run(db, w, b);
    bf_jsonw_obj_end(w);
}

static int v_status(sqlite3 *db, bf_jsonw *w) {
    sqlite3_stmt *st; int rc;
    bf_jsonw_key(w, "account"); bf_jsonw_str(w, C.account);
    bf_jsonw_key(w, "login"); put_login(w, C.account);
    if (opt("--name")) {
        Binding b; if ((rc = binding_load(db, opt("--name"), &b))) return rc;
        bf_jsonw_key(w, "vaults"); bf_jsonw_arr(w); put_status_one(db, w, &b); bf_jsonw_arr_end(w);
        return EX_OK;
    }
    bf_jsonw_key(w, "vaults"); bf_jsonw_arr(w);
    if (sqlite3_prepare_v2(db, "SELECT " BINDING_COLS " FROM binding WHERE account=? ORDER BY name", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, C.account, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) { Binding b; binding_row(st, &b); put_status_one(db, w, &b); }
        sqlite3_finalize(st);
    }
    bf_jsonw_arr_end(w);
    return EX_OK;
}

/* ------------------------------------------------------------ verbs: reading */
static int v_read(sqlite3 *db, bf_jsonw *w) {
    Binding b; int rc, forced; char cli[OB_PATH], full[OB_PATH], why[512], hex[65]; const char *rel = opt("--path");
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    if ((rc = ob_resolve_read(b.root, rel, b.config_dir, full, why, sizeof(why)))) return fail(rc, rc == EX_REFUSED ? "path-refused" : "not-found", "%s", why);
    if (use_cli(&b, cli, &forced)) {
        char pa[OB_PATH + 8]; RunResult r; snprintf(pa, sizeof(pa), "path=%s", rel);
        const char *args[] = {"read", pa, NULL};
        if (cli_call(cli, &b, args, &r)) {
            ob_sha256_mem(r.out, r.out_len, hex);
            bf_jsonw_key(w, "via"); bf_jsonw_str(w, "cli");
            bf_jsonw_key(w, "path"); bf_jsonw_str(w, rel);
            bf_jsonw_key(w, "bytes"); bf_jsonw_int(w, (int64_t)r.out_len);
            bf_jsonw_key(w, "sha256"); bf_jsonw_str(w, hex);
            bf_jsonw_key(w, "content"); bf_jsonw_strn(w, r.out, r.out_len);
            ob_run_free(&r); return EX_OK;
        }
        if (r.spawned) ob_run_free(&r);
        if (forced) return fail(EX_FAIL, "cli-failed", "the desktop CLI did not answer for read");
    } else if (forced) return fail(EX_MISSING, "cli-unavailable", "the desktop CLI is not registered or the app is not running");
    size_t len; char *buf = ob_read_file(full, &len, 16u << 20);
    if (!buf) return fail(EX_FAIL, "read-failed", "%s cannot be read: %s", rel, strerror(errno));
    if (memchr(buf, 0, len)) { free(buf); return fail(EX_REFUSED, "not-text", "%s is not text", rel); }
    ob_sha256_mem(buf, len, hex);
    bf_jsonw_key(w, "via"); bf_jsonw_str(w, "native");
    bf_jsonw_key(w, "path"); bf_jsonw_str(w, rel);
    bf_jsonw_key(w, "bytes"); bf_jsonw_int(w, (int64_t)len);
    bf_jsonw_key(w, "sha256"); bf_jsonw_str(w, hex);
    bf_jsonw_key(w, "content"); bf_jsonw_strn(w, buf, len);
    free(buf);
    return EX_OK;
}

/* A read verb answers through the CLI when the binding asks for it and the
 * app is up, else natively. The answer's "shape" says which, and a CLI that
 * fails falls back to the native answer with the reason attached. */
static int reader(sqlite3 *db, bf_jsonw *w, const char *verb) {
    Binding b; int rc, forced; char cli[OB_PATH], a1[OB_PATH + 16] = "", fallback[256] = ""; const char *rel = opt("--path");
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    if (!strcmp(verb, "search") && (!opt("--query") || !*opt("--query"))) return fail(EX_USAGE, "usage", "search needs --query");
    if ((!strcmp(verb, "backlinks") || !strcmp(verb, "links")) && !rel) return fail(EX_USAGE, "usage", "%s needs --path", verb);
    if (rel) { char full[OB_PATH], why[512]; if ((rc = ob_resolve_read(b.root, rel, b.config_dir, full, why, sizeof(why)))) return fail(rc, rc == EX_REFUSED ? "path-refused" : "not-found", "%s", why); }
    if (use_cli(&b, cli, &forced)) {
        const char *cmd = verb; RunResult r;
        const char *args[6] = {0}; int n = 0;
        if (!strcmp(verb, "search")) { snprintf(a1, sizeof(a1), "query=%s", opt("--query")); args[n++] = "search"; args[n++] = a1; }
        else if (!strcmp(verb, "links") || !strcmp(verb, "backlinks")) { snprintf(a1, sizeof(a1), "path=%s", rel); args[n++] = verb; args[n++] = a1; }
        else { args[n++] = verb; if (rel) { snprintf(a1, sizeof(a1), "path=%s", rel); args[n++] = a1; } }
        args[n++] = "format=json"; args[n] = NULL;
        if (cli_call(cli, &b, args, &r)) { bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name); put_cli_result(w, cmd, &r); ob_run_free(&r); return EX_OK; }
        snprintf(fallback, sizeof(fallback), "the desktop CLI %s (status %d)", r.timed_out ? "timed out" : "failed", r.status);
        if (r.spawned) ob_run_free(&r);
        if (forced) return fail(EX_FAIL, "cli-failed", "%s", fallback);
    } else if (forced) return fail(EX_MISSING, "cli-unavailable", "the desktop CLI is not registered or the app is not running");
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name);
    if (fallback[0]) { bf_jsonw_key(w, "cli_fallback"); bf_jsonw_str(w, fallback); }
    bf_jsonw_key(w, "answer");
    int limit = opt("--limit") ? atoi(opt("--limit")) : 50; if (limit <= 0 || limit > 1000) limit = 50;
    if (!strcmp(verb, "search")) rc = ob_verb_search(w, b.root, &b, opt("--query"), opt("--prefix"), limit);
    else if (!strcmp(verb, "tasks")) {
        const char *s = opt("--status") ? opt("--status") : "all";
        if (strcmp(s, "all") && strcmp(s, "todo") && strcmp(s, "done") && strcmp(s, "other")) return fail(EX_USAGE, "usage", "--status is all, todo, done or other");
        rc = ob_verb_tasks(w, b.root, &b, s, rel);
    }
    else if (!strcmp(verb, "backlinks") || !strcmp(verb, "links") || !strcmp(verb, "unresolved")) rc = ob_verb_links(w, b.root, &b, verb, rel);
    else if (!strcmp(verb, "tags")) rc = ob_verb_tags(w, b.root, &b, rel);
    else rc = ob_verb_properties(w, b.root, &b, rel);
    if (rc) return fail(rc, "read-failed", "%s could not read the vault", verb);
    return EX_OK;
}

/* ------------------------------------------------------------ verbs: writing */
static char *content_arg(size_t *len) {
    if (opt("--content")) { const char *s = opt("--content"); *len = strlen(s); return strdup(s); }
    if (!flag("--stdin")) return NULL;
    size_t cap = 65536, n = 0; char *buf = malloc(cap);
    while (buf) {
        size_t r = fread(buf + n, 1, cap - n - 1, stdin); n += r;
        if (n > MAX_CONTENT) { free(buf); errno = EFBIG; return NULL; }
        if (r == 0) break;
        if (n + 1 == cap) { char *b2 = realloc(buf, cap * 2); if (!b2) { free(buf); return NULL; } buf = b2; cap *= 2; }
    }
    if (buf) { buf[n] = 0; *len = n; }
    return buf;
}

static void file_sha(const char *full, char hex[65]) { if (ob_sha256_file(full, hex)) hex[0] = 0; }

/* create | append | prepend | daily-append */
static int writer(sqlite3 *db, bf_jsonw *w, const char *verb) {
    Binding b; int rc, forced; char cli[OB_PATH], full[OB_PATH], why[512], rel[OB_PATH], before[65] = "", after[65] = "", rid[32]; size_t clen = 0; struct stat st;
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    int daily = !strcmp(verb, "daily-append");
    if (daily) { if (!ob_daily_rel(b.root, b.config_dir, opt("--date"), rel, sizeof(rel))) return fail(EX_USAGE, "usage", "--date is YYYY-MM-DD"); }
    else if (!opt("--path")) return fail(EX_USAGE, "usage", "%s needs --path", verb);
    else snprintf(rel, sizeof(rel), "%s", opt("--path"));
    if (!daily && !strcmp(verb, "create") && strlen(rel) > 3 && strcasecmp(rel + strlen(rel) - 3, ".md") && !strchr(strrchr(rel, '/') ? strrchr(rel, '/') : rel, '.')) strncat(rel, ".md", sizeof(rel) - strlen(rel) - 1);
    if ((rc = ob_resolve_write(b.root, rel, b.config_dir, 0, full, why, sizeof(why))) && rc != EX_NOTFOUND) {
        receipt(db, verb, b.name, rel, "refused", "path-refused", why, "native", "", "", rid);
        return fail(rc, "path-refused", "%s", why);
    }
    char *content = content_arg(&clen);
    if (!content) return fail(EX_USAGE, "usage", errno == EFBIG ? "content is larger than 1 MiB" : "%s needs --content TEXT or --stdin", verb);
    if (memchr(content, 0, clen)) { free(content); return fail(EX_REFUSED, "not-text", "content holds a NUL byte"); }
    int exists = !lstat(full, &st);
    if (exists) file_sha(full, before);
    if (!strcmp(verb, "create") && exists && !flag("--overwrite")) { free(content); receipt(db, verb, b.name, rel, "refused", "exists", "", "native", before, "", rid); return fail(EX_REFUSED, "exists", "%s already exists; pass --overwrite to replace it", rel); }
    if ((!strcmp(verb, "append") || !strcmp(verb, "prepend")) && !exists) { free(content); return fail(EX_NOTFOUND, "not-found", "no note at %s", rel); }

    const char *via = "native";
    if (use_cli(&b, cli, &forced)) {
        char pa[OB_PATH + 8], *esc = cli_escape(content, clen), *ca = NULL; RunResult r; const char *args[6] = {0}; int n = 0;
        if (esc && (ca = malloc(strlen(esc) + 16))) sprintf(ca, "content=%s", esc);
        snprintf(pa, sizeof(pa), "path=%s", rel);
        if (daily) { args[n++] = "daily:append"; }
        else { args[n++] = verb; args[n++] = pa; if (!strcmp(verb, "create") && flag("--overwrite")) args[n++] = "overwrite"; }
        args[n++] = ca ? ca : "content="; args[n] = NULL;
        int ok = ca && cli_call(cli, &b, args, &r);
        if (r.spawned) ob_run_free(&r);
        free(esc); free(ca);
        if (ok) via = "cli";
        else if (forced) { free(content); return fail(EX_FAIL, "cli-failed", "the desktop CLI did not complete %s", verb); }
    } else if (forced) { free(content); return fail(EX_MISSING, "cli-unavailable", "the desktop CLI is not registered or the app is not running"); }

    if (!strcmp(via, "native")) {
        if ((rc = ob_resolve_write(b.root, rel, b.config_dir, 1, full, why, sizeof(why)))) { free(content); return fail(rc, "path-refused", "%s", why); }
        char *out = NULL; size_t olen = 0;
        if (!exists) { olen = clen + (clen && content[clen - 1] != '\n'); out = malloc(olen + 1); if (out) { memcpy(out, content, clen); if (olen > clen) out[clen] = '\n'; } }
        else if (!strcmp(verb, "create")) { olen = clen; out = strdup(content); }
        else {
            size_t elen; char *old = ob_read_file(full, &elen, 16u << 20);
            if (!old) { free(content); return fail(EX_FAIL, "read-failed", "%s cannot be read", rel); }
            int nl = clen && content[clen - 1] != '\n';
            out = malloc(elen + clen + 3);
            if (out && !strcmp(verb, "prepend")) {
                /* After the frontmatter, as Obsidian's own prepend does. */
                size_t at = 0;
                if (elen >= 4 && !memcmp(old, "---\n", 4)) { const char *e = strstr(old + 4, "\n---\n"); if (e) at = (size_t)(e - old) + 5; else if ((e = strstr(old + 4, "\n---")) && !e[4]) at = elen; }
                memcpy(out, old, at); olen = at;
                if (at && old[at - 1] != '\n') out[olen++] = '\n';
                memcpy(out + olen, content, clen); olen += clen; if (nl) out[olen++] = '\n';
                memcpy(out + olen, old + at, elen - at); olen += elen - at;
            } else if (out) {
                memcpy(out, old, elen); olen = elen;
                if (elen && old[elen - 1] != '\n') out[olen++] = '\n';
                memcpy(out + olen, content, clen); olen += clen; if (nl) out[olen++] = '\n';
            }
            free(old);
        }
        if (!out) { free(content); return fail(EX_FAIL, "oom", "out of memory"); }
        int wr = ob_atomic_write(full, out, olen, !exists);
        free(out);
        if (wr) { free(content); receipt(db, verb, b.name, rel, "failed", "write-failed", strerror(errno), "native", before, "", rid); return fail(EX_FAIL, "write-failed", "%s: %s", rel, strerror(errno)); }
    }
    free(content);
    file_sha(full, after);
    receipt(db, verb, b.name, rel, "ok", exists ? "updated" : "created", "", via, before, after, rid);
    bf_jsonw_key(w, "via"); bf_jsonw_str(w, via);
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name);
    bf_jsonw_key(w, "path"); bf_jsonw_str(w, rel);
    bf_jsonw_key(w, "created"); bf_jsonw_bool(w, !exists);
    bf_jsonw_key(w, "sha_before"); if (before[0]) bf_jsonw_str(w, before); else bf_jsonw_null(w);
    bf_jsonw_key(w, "sha_after"); if (after[0]) bf_jsonw_str(w, after); else bf_jsonw_null(w);
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    return EX_OK;
}

static int v_daily_path(sqlite3 *db, bf_jsonw *w) {
    Binding b; int rc; char rel[OB_PATH], full[OB_PATH]; struct stat st;
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    if (!ob_daily_rel(b.root, b.config_dir, opt("--date"), rel, sizeof(rel))) return fail(EX_USAGE, "usage", "--date is YYYY-MM-DD");
    snprintf(full, sizeof(full), "%s/%s", b.root, rel);
    bf_jsonw_key(w, "path"); bf_jsonw_str(w, rel);
    bf_jsonw_key(w, "exists"); bf_jsonw_bool(w, !stat(full, &st));
    return EX_OK;
}

static int v_task(sqlite3 *db, bf_jsonw *w) {
    Binding b; int rc, forced; char cli[OB_PATH], full[OB_PATH], why[512], rel[OB_PATH], before[65] = "", after[65] = "", rid[32];
    const char *ref = opt("--ref"), *how = flag("--done") ? "done" : flag("--todo") ? "todo" : "toggle";
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    const char *colon = ref ? strrchr(ref, ':') : NULL; int line = colon ? atoi(colon + 1) : 0;
    if (!colon || line <= 0) return fail(EX_USAGE, "usage", "task needs --ref path:line");
    snprintf(rel, sizeof(rel), "%.*s", (int)(colon - ref), ref);
    if ((rc = ob_resolve_read(b.root, rel, b.config_dir, full, why, sizeof(why)))) return fail(rc, rc == EX_REFUSED ? "path-refused" : "not-found", "%s", why);
    if ((rc = ob_resolve_write(b.root, rel, b.config_dir, 0, full, why, sizeof(why)))) return fail(rc, "path-refused", "%s", why);
    const char *via = "native";
    if (use_cli(&b, cli, &forced)) {
        char ra[OB_PATH + 16]; RunResult r; snprintf(ra, sizeof(ra), "ref=%s:%d", rel, line);
        const char *args[] = {"task", ra, how, NULL};
        file_sha(full, before);
        int ok = cli_call(cli, &b, args, &r); if (r.spawned) ob_run_free(&r);
        if (ok) { via = "cli"; file_sha(full, after); }
        else if (forced) return fail(EX_FAIL, "cli-failed", "the desktop CLI did not complete task");
    } else if (forced) return fail(EX_MISSING, "cli-unavailable", "the desktop CLI is not registered or the app is not running");
    if (!strcmp(via, "native") && (rc = ob_task_set(full, line, how, why, sizeof(why), before, after))) {
        receipt(db, "task", b.name, ref, "refused", "not-a-task", why, "native", before, "", rid);
        return fail(rc, rc == EX_REFUSED ? "not-a-task" : "not-found", "%s", why);
    }
    receipt(db, "task", b.name, ref, "ok", how, "", via, before, after, rid);
    bf_jsonw_key(w, "via"); bf_jsonw_str(w, via);
    bf_jsonw_key(w, "ref"); bf_jsonw_str(w, ref);
    bf_jsonw_key(w, "set"); bf_jsonw_str(w, how);
    bf_jsonw_key(w, "sha_before"); bf_jsonw_str(w, before);
    bf_jsonw_key(w, "sha_after"); bf_jsonw_str(w, after);
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    return EX_OK;
}

/* ------------------------------------------------------------ verbs: headless sync */
static void log_append(const char *logf, const char *head, const RunResult *r) {
    FILE *f = fopen(logf, "a"); char at[32]; iso_now(at);
    if (!f) return;
    fprintf(f, "--- %s %s (exit %d%s)\n", at, head, r->status, r->timed_out ? ", timed out" : "");
    if (r->out_len) fwrite(r->out, 1, r->out_len, f);
    if (r->err_len) fwrite(r->err, 1, r->err_len, f);
    fclose(f);
}

static int64_t sync_run_row(sqlite3 *db, const Binding *b, const char *kind, const char *argv_s, const char *started, int exit_code, const char *outcome, const char *logf, long pid, const char *rid) {
    sqlite3_stmt *st; char ended[32]; iso_now(ended);
    if (sqlite3_prepare_v2(db, "INSERT INTO sync_run(receipt_id,account,binding,kind,argv,started_at,ended_at,exit_code,outcome,log_path,pid) VALUES(?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, rid, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, b->account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 3, b->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, kind, -1, SQLITE_STATIC); sqlite3_bind_text(st, 5, argv_s, -1, SQLITE_STATIC); sqlite3_bind_text(st, 6, started, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7, ended, -1, SQLITE_STATIC); sqlite3_bind_int(st, 8, exit_code); sqlite3_bind_text(st, 9, outcome, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 10, logf, -1, SQLITE_STATIC); sqlite3_bind_int64(st, 11, pid);
    sqlite3_step(st); sqlite3_finalize(st);
    return sqlite3_last_insert_rowid(db);
}

static int guarded(sqlite3 *db, bf_jsonw *w, const char *verb, Binding *b) {
    Guard g; char rid[32]; int rc;
    if ((rc = binding_load(db, opt("--name"), b))) return rc;
    ob_guard_headless(&C, b, &g);
    bf_jsonw_key(w, "guard"); put_guard(w, &g);
    if (g.refuse) {
        receipt(db, verb, b->name, b->root, "refused", g.code, g.reason, "headless", "", "", rid);
        return fail(EX_REFUSED, g.code, "%s", g.reason);
    }
    return EX_OK;
}

/* argv is joined for the run record; it never holds a secret because none is
 * ever placed on it. */
static void join_argv(char *const *argv, char *out, size_t cap) {
    size_t o = 0; out[0] = 0;
    for (int i = 0; argv[i] && o + 2 < cap; i++) o += (size_t)snprintf(out + o, cap - o, "%s%s", i ? " " : "", argv[i]);
}

static int run_ob(sqlite3 *db, bf_jsonw *w, const Binding *b, const char *kind, char **argv, int timeout, int *exit_code) {
    char home[OB_PATH], pidf[OB_PATH], logf[OB_PATH], label[256], plist[OB_PATH], started[32], argv_s[2048], rid[32], dir[OB_PATH]; RunResult r;
    ob_account_home(&C, b->account, home); ob_mkdirs(home, 0700);
    ob_sup_paths(&C, b, pidf, logf, label, plist);
    snprintf(dir, sizeof(dir), "%.*s", (int)(strrchr(logf, '/') - logf), logf); ob_mkdirs(dir, 0700);
    iso_now(started); join_argv(argv, argv_s, sizeof(argv_s));
    int pass = C.token_account && !strcmp(C.token_account, b->account);
    if (!ob_run(argv, home, pass, timeout, &r)) return fail(EX_FAIL, "spawn-failed", "ob could not be started");
    log_append(logf, argv_s, &r);
    *exit_code = r.timed_out ? 124 : r.status;
    const char *outcome = r.timed_out ? "timed-out" : r.status ? "failed" : "ok";
    receipt(db, kind, b->name, b->root, outcome, kind, r.status ? "see log" : "", "headless", "", "", rid);
    int64_t run_id = sync_run_row(db, b, kind, argv_s, started, *exit_code, outcome, logf, 0, rid);
    bf_jsonw_key(w, kind); bf_jsonw_obj(w);
    bf_jsonw_key(w, "run_id"); bf_jsonw_int(w, run_id);
    bf_jsonw_key(w, "argv"); bf_jsonw_str(w, argv_s);
    bf_jsonw_key(w, "exit_code"); bf_jsonw_int(w, *exit_code);
    bf_jsonw_key(w, "outcome"); bf_jsonw_str(w, outcome);
    { char err[64]; bf_json_doc_t *d = r.out_len ? bf_json_parse(r.out, r.out_len, err, sizeof(err)) : NULL;
      bf_jsonw_key(w, "output"); if (d) { bf_jsonw_node(w, d, bf_json_root(d), 0); bf_json_free(d); } else bf_jsonw_strn(w, r.out, r.out_len > 4000 ? 4000 : r.out_len); }
    bf_jsonw_key(w, "stderr"); bf_jsonw_strn(w, r.err, r.err_len > 2000 ? 2000 : r.err_len);
    bf_jsonw_key(w, "log"); bf_jsonw_str(w, logf);
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    bf_jsonw_obj_end(w);
    if (r.status) snprintf(g_detail, sizeof(g_detail), "%.1500s", r.err_len ? r.err : r.out);
    int timed_out = r.timed_out;
    ob_run_free(&r);
    return timed_out ? fail(EX_FAIL, "timed-out", "ob %s did not finish within %d seconds", kind, timeout) : EX_OK;
}

static int apply_config(sqlite3 *db, bf_jsonw *w, const Binding *b, const char *obp) {
    char *argv[20]; int n = 0, code = 0;
    argv[n++] = (char *)obp; argv[n++] = "sync-config"; argv[n++] = "--path"; argv[n++] = (char *)b->root;
    argv[n++] = "--mode"; argv[n++] = (char *)b->sync_mode; argv[n++] = "--conflict-strategy"; argv[n++] = (char *)b->conflict;
    argv[n++] = "--excluded-folders"; argv[n++] = (char *)b->excluded;
    argv[n++] = "--device-name"; argv[n++] = (char *)b->device;
    if (strcmp(b->config_dir, ".obsidian")) { argv[n++] = "--config-dir"; argv[n++] = (char *)b->config_dir; }
    argv[n++] = "--json"; argv[n] = NULL;
    int rc = run_ob(db, w, b, "sync-config", argv, C.timeout * 3, &code);
    if (rc) return rc;
    if (code) return fail(EX_FAIL, "ob-failed", "ob sync-config exited %d", code);
    return EX_OK;
}

static int v_sync(sqlite3 *db, bf_jsonw *w, const char *verb) {
    Binding b; int rc, code = 0; char obp[OB_PATH], why[512], home[OB_PATH], rid[32];
    if (!strcmp(verb, "sync-stop") || !strcmp(verb, "sync-status")) { if ((rc = binding_load(db, opt("--name"), &b))) return rc; }
    else if ((rc = guarded(db, w, verb, &b))) return rc;
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name);
    ob_account_home(&C, b.account, home);
    if (!strcmp(verb, "sync-stop")) {
        rc = ob_sup_stop(&C, &b, w, why, sizeof(why));
        if (rc) return fail(rc, "stop-failed", "%s", why);
        receipt(db, verb, b.name, b.root, "ok", "stopped", "", "headless", "", "", rid);
        if (!strcmp(b.state, "syncing")) binding_state(db, &b, "sync-configured");
        bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
        return EX_OK;
    }
    if (!ob_ob_path(&C, obp)) { snprintf(g_hand, sizeof(g_hand), "npm install -g obsidian-headless"); return fail(EX_MISSING, "ob-missing", "Obsidian Headless (ob) is not installed"); }
    if (!strcmp(verb, "sync-status")) {
        long pid = 0; int alive = ob_sup_alive(&C, &b, &pid);
        char *argv[] = {obp, "sync-status", "--path", b.root, "--json", NULL}; RunResult r;
        bf_jsonw_key(w, "continuous_sync"); bf_jsonw_obj(w);
        bf_jsonw_key(w, "running"); bf_jsonw_bool(w, alive);
        bf_jsonw_key(w, "pid"); if (alive && pid) bf_jsonw_int(w, pid); else bf_jsonw_null(w);
        bf_jsonw_obj_end(w);
        bf_jsonw_key(w, "login"); put_login(w, b.account);
        int pass = C.token_account && !strcmp(C.token_account, b.account);
        bf_jsonw_key(w, "ob");
        if (ob_run(argv, home, pass, C.timeout, &r)) {
            char err[64]; bf_json_doc_t *d = r.out_len ? bf_json_parse(r.out, r.out_len, err, sizeof(err)) : NULL;
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "exit_code"); bf_jsonw_int(w, r.status);
            bf_jsonw_key(w, "configured"); bf_jsonw_bool(w, r.status == 0);
            bf_jsonw_key(w, "status"); if (d) { bf_jsonw_node(w, d, bf_json_root(d), 0); bf_json_free(d); } else bf_jsonw_null(w);
            bf_jsonw_key(w, "message"); { size_t n = r.err_len; while (n && isspace((unsigned char)r.err[n - 1])) n--; bf_jsonw_strn(w, r.err, n > 1000 ? 1000 : n); }
            bf_jsonw_obj_end(w);
            ob_run_free(&r);
        } else bf_jsonw_null(w);
        put_last_run(db, w, &b);
        return EX_OK;
    }
    if (!strcmp(verb, "sync-setup")) {
        const char *remote = opt("--remote-vault") ? opt("--remote-vault") : b.remote;
        if (!remote || !*remote) return fail(EX_USAGE, "usage", "sync-setup needs --remote-vault (an id or name from `ob sync-list-remote`)");
        if (strlen(remote) >= sizeof(b.remote)) return fail(EX_USAGE, "usage", "--remote-vault is too long");
        char *argv[16]; int n = 0;
        argv[n++] = obp; argv[n++] = "sync-setup"; argv[n++] = "--vault"; argv[n++] = (char *)remote; argv[n++] = "--path"; argv[n++] = b.root;
        argv[n++] = "--device-name"; argv[n++] = b.device;
        if (strcmp(b.config_dir, ".obsidian")) { argv[n++] = "--config-dir"; argv[n++] = b.config_dir; }
        argv[n++] = "--json"; argv[n] = NULL;
        if ((rc = run_ob(db, w, &b, "sync-setup", argv, C.timeout * 6, &code))) return rc;
        if (code) {
            snprintf(g_hand, sizeof(g_hand), "HOME='%s' ob sync-setup --vault '%s' --path '%s' --device-name '%s'   (run by hand when the remote vault is end-to-end encrypted, so ob can ask for its password; or log in first: HOME='%s' ob login)", home, remote, b.root, b.device, home);
            return fail(EX_FAIL, "ob-failed", "ob sync-setup exited %d", code);
        }
        sqlite3_stmt *st;
        if (sqlite3_prepare_v2(db, "UPDATE binding SET remote_vault=? WHERE account=? AND name=?", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, remote, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, b.account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 3, b.name, -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st);
        }
        snprintf(b.remote, sizeof(b.remote), "%s", remote);
        if ((rc = apply_config(db, w, &b, obp))) return rc;
        binding_state(db, &b, "sync-configured");
        return EX_OK;
    }
    if (!strcmp(verb, "sync-config")) {
        const char *m = opt("--mode"), *cf = opt("--conflict"), *ex = opt("--exclude"), *dv = opt("--device-name");
        if (m && !in_set(m, MODES)) return fail(EX_USAGE, "usage", "--mode is bidirectional, pull-only or mirror-remote");
        if (cf && !in_set(cf, CONFLICTS)) return fail(EX_USAGE, "usage", "--conflict is merge or conflict");
        if ((ex && strlen(ex) >= sizeof(b.excluded)) || (dv && strlen(dv) >= 120)) return fail(EX_USAGE, "usage", "an option value is too long");
        if (m) snprintf(b.sync_mode, sizeof(b.sync_mode), "%s", m);
        if (cf) snprintf(b.conflict, sizeof(b.conflict), "%s", cf);
        if (ex) snprintf(b.excluded, sizeof(b.excluded), "%s", ex);
        if (dv) snprintf(b.device, sizeof(b.device), "%s", dv);
        sqlite3_stmt *st; char at[32]; iso_now(at);
        if (sqlite3_prepare_v2(db, "UPDATE binding SET sync_mode=?, conflict_strategy=?, excluded_folders=?, device_name=?, updated_at=? WHERE account=? AND name=?", -1, &st, NULL) == SQLITE_OK) {
            const char *v[] = {b.sync_mode, b.conflict, b.excluded, b.device, at, b.account, b.name};
            for (int i = 0; i < 7; i++) sqlite3_bind_text(st, i + 1, v[i], -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st);
        }
        return apply_config(db, w, &b, obp);
    }
    if (!strcmp(verb, "sync-once")) {
        long pid; if (ob_sup_alive(&C, &b, &pid)) return fail(EX_REFUSED, "syncing", "continuous sync is already running for %s (pid %ld)", b.name, pid);
        char *argv[] = {obp, "sync", "--path", b.root, NULL};
        if ((rc = run_ob(db, w, &b, "sync-once", argv, sync_timeout(), &code))) return rc;
        if (code) { snprintf(g_hand, sizeof(g_hand), "HOME='%s' ob login   (when the log says the account is not logged in)", home); return fail(EX_FAIL, "ob-failed", "ob sync exited %d", code); }
        return EX_OK;
    }
    /* sync-start */
    char started[32]; iso_now(started);
    bf_jsonw_key(w, "continuous_sync"); bf_jsonw_obj(w);
    rc = ob_sup_start(&C, &b, w, why, sizeof(why));
    bf_jsonw_obj_end(w);
    long pid = 0; ob_sup_alive(&C, &b, &pid);
    receipt(db, verb, b.name, b.root, rc ? "failed" : "ok", rc ? "start-failed" : "started", rc ? why : "", "headless", "", "", rid);
    { char pidf[OB_PATH], logf[OB_PATH], label[256], plist[OB_PATH]; ob_sup_paths(&C, &b, pidf, logf, label, plist);
      sync_run_row(db, &b, "sync-start", "ob sync --continuous", started, rc, rc ? "failed" : "started", logf, pid, rid); }
    if (rc) return fail(rc, "start-failed", "%s", why);
    binding_state(db, &b, "syncing");
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    return EX_OK;
}

static int v_guard(sqlite3 *db, bf_jsonw *w) {
    Binding b; Guard g; int rc;
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    ob_guard_headless(&C, &b, &g);
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name);
    bf_jsonw_key(w, "guard"); put_guard(w, &g);
    return EX_OK;
}

/* ------------------------------------------------------------ verbs: snapshots */
typedef struct { char *path; char sha[65]; int64_t size, mtime; } Prev;
static int prev_cmp(const void *a, const void *b) { return strcmp(((const Prev *)a)->path, ((const Prev *)b)->path); }

static int64_t latest_snapshot(sqlite3 *db, const Binding *b, int64_t before) {
    sqlite3_stmt *st; int64_t id = 0;
    if (sqlite3_prepare_v2(db, "SELECT snapshot_id FROM snapshot WHERE account=? AND binding=? AND snapshot_id<? ORDER BY snapshot_id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, b->account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, b->name, -1, SQLITE_STATIC); sqlite3_bind_int64(st, 3, before);
    if (sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return id;
}

static Prev *load_snapshot(sqlite3 *db, int64_t id, size_t *n) {
    sqlite3_stmt *st; size_t cap = 256; Prev *v = malloc(cap * sizeof(Prev)); *n = 0;
    if (!v || sqlite3_prepare_v2(db, "SELECT path,sha256,size,mtime_ns FROM snapshot_file WHERE snapshot_id=? ORDER BY path", -1, &st, NULL) != SQLITE_OK) return v;
    sqlite3_bind_int64(st, 1, id);
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (*n == cap) { cap *= 2; Prev *v2 = realloc(v, cap * sizeof(Prev)); if (!v2) break; v = v2; }
        Prev *p = &v[(*n)++];
        p->path = strdup((const char *)sqlite3_column_text(st, 0)); col(st, 1, p->sha, sizeof(p->sha));
        p->size = sqlite3_column_int64(st, 2); p->mtime = sqlite3_column_int64(st, 3);
    }
    sqlite3_finalize(st);
    qsort(v, *n, sizeof(Prev), prev_cmp);
    return v;
}
static void free_prev(Prev *v, size_t n) { for (size_t i = 0; i < n; i++) free(v[i].path); free(v); }

static int snapshot_owned(sqlite3 *db, const Binding *b, int64_t id) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM snapshot WHERE snapshot_id=? AND account=? AND binding=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_int64(st, 1, id); sqlite3_bind_text(st, 2, b->account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 3, b->name, -1, SQLITE_STATIC);
    ok = sqlite3_step(st) == SQLITE_ROW; sqlite3_finalize(st);
    return ok;
}

/* Walks two sorted manifests. Writes added/removed/changed arrays and counts. */
static void put_diff(bf_jsonw *w, const Prev *a, size_t na, const Prev *b, size_t nb, int list, int64_t counts[3]) {
    for (int pass = 0; pass < 3; pass++) {
        const char *key = pass == 0 ? "added" : pass == 1 ? "removed" : "changed";
        if (list) { bf_jsonw_key(w, key); bf_jsonw_arr(w); }
        size_t i = 0, j = 0;
        while (i < na || j < nb) {
            int c = i == na ? 1 : j == nb ? -1 : strcmp(a[i].path, b[j].path);
            if (c > 0) { if (pass == 0) { counts[0]++; if (list) { bf_jsonw_obj(w); bf_jsonw_key(w, "path"); bf_jsonw_str(w, b[j].path); bf_jsonw_key(w, "sha256"); bf_jsonw_str(w, b[j].sha); bf_jsonw_key(w, "size"); bf_jsonw_int(w, b[j].size); bf_jsonw_obj_end(w); } } j++; }
            else if (c < 0) { if (pass == 1) { counts[1]++; if (list) { bf_jsonw_obj(w); bf_jsonw_key(w, "path"); bf_jsonw_str(w, a[i].path); bf_jsonw_key(w, "sha256"); bf_jsonw_str(w, a[i].sha); bf_jsonw_key(w, "size"); bf_jsonw_int(w, a[i].size); bf_jsonw_obj_end(w); } } i++; }
            else {
                if (pass == 2 && strcmp(a[i].sha, b[j].sha)) { counts[2]++; if (list) { bf_jsonw_obj(w); bf_jsonw_key(w, "path"); bf_jsonw_str(w, a[i].path); bf_jsonw_key(w, "from_sha256"); bf_jsonw_str(w, a[i].sha); bf_jsonw_key(w, "to_sha256"); bf_jsonw_str(w, b[j].sha); bf_jsonw_key(w, "size"); bf_jsonw_int(w, b[j].size); bf_jsonw_obj_end(w); } }
                i++; j++;
            }
        }
        if (list) bf_jsonw_arr_end(w);
    }
}

static int v_snapshot(sqlite3 *db, bf_jsonw *w) {
    Binding b; int rc; FileList fl; size_t np = 0; int incl = flag("--include-config"); char at[32], rid[32];
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    if (!ob_walk(b.root, b.config_dir, b.excluded, incl ? WALK_INCLUDE_CFG : 0, &fl)) return fail(EX_FAIL, "walk-failed", "the vault could not be walked");
    int64_t prev_id = latest_snapshot(db, &b, INT64_MAX);
    Prev *prev = prev_id ? load_snapshot(db, prev_id, &np) : NULL;
    int64_t bytes = 0, rehashed = 0, reused = 0; BfSha256 man; uint8_t dg[32]; char mhex[65];
    bf_sha256_init(&man);
    Prev *cur = malloc((fl.n ? fl.n : 1) * sizeof(Prev));
    if (!cur) { ob_files_free(&fl); free_prev(prev, np); return fail(EX_FAIL, "oom", "out of memory"); }
    size_t nc = 0;
    for (size_t i = 0; i < fl.n; i++) {
        FileEnt *f = &fl.v[i]; Prev key = {f->path, "", 0, 0}, *hit = prev ? bsearch(&key, prev, np, sizeof(Prev), prev_cmp) : NULL;
        if (hit && hit->size == f->size && hit->mtime == f->mtime_ns) { snprintf(f->sha, sizeof(f->sha), "%s", hit->sha); reused++; }
        else { char full[OB_PATH]; snprintf(full, sizeof(full), "%s/%s", b.root, f->path); if (ob_sha256_file(full, f->sha)) continue; rehashed++; }
        bytes += f->size;
        char line[OB_PATH + 128]; int n = snprintf(line, sizeof(line), "%s%c%s%c%lld\n", f->path, 0, f->sha, 0, (long long)f->size);
        bf_sha256_update(&man, (const uint8_t *)line, (size_t)n);
        cur[nc].path = f->path; snprintf(cur[nc].sha, sizeof(cur[nc].sha), "%s", f->sha); cur[nc].size = f->size; cur[nc].mtime = f->mtime_ns; nc++;
    }
    bf_sha256_final(&man, dg);
    for (int i = 0; i < 32; i++) snprintf(mhex + i * 2, 3, "%02x", dg[i]);
    iso_now(at);
    sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
    sqlite3_stmt *st; int64_t sid = 0;
    if (sqlite3_prepare_v2(db, "INSERT INTO snapshot(account,binding,taken_at,files,bytes,manifest_digest,rehashed,reused,include_config) VALUES(?,?,?,?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, b.account, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, b.name, -1, SQLITE_STATIC); sqlite3_bind_text(st, 3, at, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 4, (int64_t)nc); sqlite3_bind_int64(st, 5, bytes); sqlite3_bind_text(st, 6, mhex, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 7, rehashed); sqlite3_bind_int64(st, 8, reused); sqlite3_bind_int(st, 9, incl);
        if (sqlite3_step(st) == SQLITE_DONE) sid = sqlite3_last_insert_rowid(db);
        sqlite3_finalize(st);
    }
    if (sid && sqlite3_prepare_v2(db, "INSERT INTO snapshot_file VALUES(?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
        for (size_t i = 0; i < nc; i++) {
            sqlite3_reset(st);
            sqlite3_bind_int64(st, 1, sid); sqlite3_bind_text(st, 2, cur[i].path, -1, SQLITE_STATIC); sqlite3_bind_text(st, 3, cur[i].sha, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 4, cur[i].size); sqlite3_bind_int64(st, 5, cur[i].mtime);
            if (sqlite3_step(st) != SQLITE_DONE) { sid = 0; break; }
        }
        sqlite3_finalize(st);
    }
    if (!sid) { sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); free(cur); ob_files_free(&fl); free_prev(prev, np); return fail(EX_FAIL, "store", "%s", sqlite3_errmsg(db)); }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    receipt(db, "snapshot", b.name, mhex, "ok", "snapshot", "", "native", "", "", rid);
    int64_t counts[3] = {0, 0, 0};
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name);
    bf_jsonw_key(w, "snapshot_id"); bf_jsonw_int(w, sid);
    bf_jsonw_key(w, "previous"); if (prev_id) bf_jsonw_int(w, prev_id); else bf_jsonw_null(w);
    bf_jsonw_key(w, "taken_at"); bf_jsonw_str(w, at);
    bf_jsonw_key(w, "files"); bf_jsonw_int(w, (int64_t)nc);
    bf_jsonw_key(w, "bytes"); bf_jsonw_int(w, bytes);
    bf_jsonw_key(w, "rehashed"); bf_jsonw_int(w, rehashed);
    bf_jsonw_key(w, "reused"); bf_jsonw_int(w, reused);
    bf_jsonw_key(w, "symlinks_skipped"); bf_jsonw_int(w, (int64_t)fl.symlinks);
    bf_jsonw_key(w, "include_config"); bf_jsonw_bool(w, incl);
    bf_jsonw_key(w, "manifest_digest"); bf_jsonw_str(w, mhex);
    if (prev_id) {
        put_diff(w, prev, np, cur, nc, 0, counts);
        bf_jsonw_key(w, "since_previous"); bf_jsonw_obj(w);
        bf_jsonw_key(w, "added"); bf_jsonw_int(w, counts[0]);
        bf_jsonw_key(w, "removed"); bf_jsonw_int(w, counts[1]);
        bf_jsonw_key(w, "changed"); bf_jsonw_int(w, counts[2]);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_key(w, "receipt"); bf_jsonw_str(w, rid);
    free(cur); ob_files_free(&fl); free_prev(prev, np);
    return EX_OK;
}

static int v_diff(sqlite3 *db, bf_jsonw *w) {
    Binding b; int rc; size_t na = 0, nb = 0; int64_t counts[3] = {0, 0, 0};
    if ((rc = binding_load(db, opt("--name"), &b))) return rc;
    int64_t to = opt("--to") ? atoll(opt("--to")) : latest_snapshot(db, &b, INT64_MAX);
    int64_t from = opt("--from") ? atoll(opt("--from")) : (to ? latest_snapshot(db, &b, to) : 0);
    if (!to || !from) return fail(EX_NOTFOUND, "no-snapshots", "diff needs two snapshots of %s; take one with snapshot", b.name);
    if (!snapshot_owned(db, &b, from) || !snapshot_owned(db, &b, to)) return fail(EX_REFUSED, "not-owned", "snapshot %lld or %lld is not one of this account's snapshots of %s", (long long)from, (long long)to, b.name);
    Prev *a = load_snapshot(db, from, &na), *c = load_snapshot(db, to, &nb);
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, b.name);
    bf_jsonw_key(w, "from"); bf_jsonw_int(w, from);
    bf_jsonw_key(w, "to"); bf_jsonw_int(w, to);
    put_diff(w, a, na, c, nb, 1, counts);
    bf_jsonw_key(w, "counts"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "added"); bf_jsonw_int(w, counts[0]);
    bf_jsonw_key(w, "removed"); bf_jsonw_int(w, counts[1]);
    bf_jsonw_key(w, "changed"); bf_jsonw_int(w, counts[2]);
    bf_jsonw_obj_end(w);
    free_prev(a, na); free_prev(c, nb);
    return EX_OK;
}
/* Compare two locally bound replicas without copying, merging, or contacting a
 * provider. A per-file SHA-256 is measured afresh: timestamp-only snapshots
 * can miss restored mtimes. Parent symlinks are refused. */
static int replica_hash(const Binding *b, FileList *files) {
    if (!ob_walk(b->root, b->config_dir, b->excluded, 0, files))
        return fail(EX_FAIL, "walk-failed", "cannot inspect vault %s", b->name);
    if (files->n > 4096) return fail(EX_REFUSED, "too-many-files",
                                     "replica plan is limited to 4096 files per vault");
    for (size_t i = 0; i < files->n; ++i) {
        char path[OB_PATH], why[256];
        int rc = ob_resolve_write(b->root, files->v[i].path, b->config_dir, 0,
                                  path, why, sizeof(why));
        if (rc != EX_OK) return fail(EX_REFUSED, "unsafe-file",
                                      "replica file refused: %s", files->v[i].path);
        if (ob_sha256_file(path, files->v[i].sha))
            return fail(EX_FAIL, "hash-failed", "cannot hash replica file %s", files->v[i].path);
    }
    return EX_OK;
}

static int replica_root_current(const Binding *b) {
    char real[OB_PATH];
    return realpath(b->root, real) && !strcmp(real, b->root) && root_allowed(real);
}

/* Two snapshots are an admissible comparison baseline only when both owners
 * retained the same paths, byte hashes and sizes. This does not prove a
 * device-level common ancestor; it is a user-selected historical witness. */
static int replica_snapshot_id(const char *arg, int64_t *id) {
    if (!arg || !*arg || *arg == '0') return 0;
    for (const char *p = arg; *p; ++p) if (*p < '0' || *p > '9') return 0;
    errno = 0; long long n = strtoll(arg, NULL, 10);
    if (errno || n <= 0) return 0;
    *id = (int64_t)n; return 1;
}
static int replica_snapshot_load(sqlite3 *db, const Binding *b, int64_t id,
                                 Prev **files, size_t *count) {
    sqlite3_stmt *st = NULL; int64_t expected = -1; int include_cfg = 1;
    if (!snapshot_owned(db, b, id)) return EX_REFUSED;
    if (sqlite3_prepare_v2(db,
            "SELECT files,include_config FROM snapshot WHERE snapshot_id=? AND account=? AND binding=?",
            -1, &st, NULL) != SQLITE_OK) return EX_FAIL;
    sqlite3_bind_int64(st, 1, id);
    sqlite3_bind_text(st, 2, b->account, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, b->name, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        expected = sqlite3_column_int64(st, 0);
        include_cfg = sqlite3_column_int(st, 1);
    }
    sqlite3_finalize(st);
    if (expected < 0 || expected > 4096 || include_cfg) return EX_REFUSED;
    *files = load_snapshot(db, id, count);
    if (!*files || (int64_t)*count != expected) return EX_FAIL;
    for (size_t i = 0; i < *count; ++i) {
        char why[128];
        if (!(*files)[i].path ||
            !ob_rel_ok((*files)[i].path, b->config_dir, why, sizeof(why)))
            return EX_REFUSED;
    }
    return EX_OK;
}
static const char *replica_sha(const FileEnt *file) { return file ? file->sha : NULL; }
static int replica_equal(const char *a, const char *b) {
    return (!a && !b) || (a && b && !strcmp(a, b));
}

static int v_replica_plan(sqlite3 *db, bf_jsonw *w) {
    const char *name = opt("--name"), *peer = opt("--peer");
    Binding a, b; FileList x = {0}, y = {0};
    int rc = EX_OK;
    int64_t counts[4] = {0}, baseline_counts[5] = {0};
    const char *from_arg = opt("--base-source"), *peer_arg = opt("--base-peer");
    int with_base = from_arg != NULL || peer_arg != NULL;
    int64_t from_id = 0, peer_id = 0;
    Prev *baseline_source = NULL, *baseline_peer = NULL;
    size_t baseline_n = 0, baseline_peer_n = 0;
    if (with_base && (!replica_snapshot_id(from_arg, &from_id) ||
                      !replica_snapshot_id(peer_arg, &peer_id)))
        return fail(EX_USAGE, "usage", "both --base-source and --base-peer must be positive snapshot ids");
    if (!peer || !*peer || !name || !*name)
        return fail(EX_USAGE, "usage", "replica-plan requires --name and --peer");
    if (!strcmp(name, peer))
        return fail(EX_USAGE, "usage", "replica-plan requires two distinct vaults");
    if ((rc = binding_load(db, name, &a)) || (rc = binding_load(db, peer, &b))) return rc;
    if (!replica_root_current(&a) || !replica_root_current(&b))
        return fail(EX_REFUSED, "root-changed", "bound replica root moved or is no longer allowed");
    if (ob_inside(a.root, b.root) || ob_inside(b.root, a.root))
        return fail(EX_REFUSED, "overlap", "replica roots overlap");
    if (strcmp(a.config_dir, b.config_dir) || strcmp(a.excluded, b.excluded))
        return fail(EX_REFUSED, "scope-mismatch", "replica exclusion and config scopes differ");
    if ((rc = replica_hash(&a, &x)) || (rc = replica_hash(&b, &y))) {
        free_prev(baseline_source, baseline_n);
    ob_files_free(&x); ob_files_free(&y); return rc;
    }
    if (with_base) {
        int ar = replica_snapshot_load(db, &a, from_id, &baseline_source, &baseline_n);
        int br = ar == EX_OK ? replica_snapshot_load(db, &b, peer_id, &baseline_peer, &baseline_peer_n) : EX_OK;
        int match = ar == EX_OK && br == EX_OK && baseline_n == baseline_peer_n;
        if (match) for (size_t z = 0; z < baseline_n; ++z) {
            if (strcmp(baseline_source[z].path, baseline_peer[z].path) ||
                strcmp(baseline_source[z].sha, baseline_peer[z].sha) ||
                baseline_source[z].size != baseline_peer[z].size) { match = 0; break; }
        }
        free_prev(baseline_source, baseline_n); baseline_source = NULL;
        if (match) {
            baseline_source = baseline_peer;
            baseline_peer = NULL;
        } else {
            free_prev(baseline_peer, baseline_peer_n);
            ob_files_free(&x); ob_files_free(&y);
            return fail(EX_REFUSED, "base-mismatch",
                        "snapshots must belong to each replica and have exactly matching content scope");
        }
    }
    bf_jsonw_key(w, "name"); bf_jsonw_str(w, a.name);
    bf_jsonw_key(w, "peer"); bf_jsonw_str(w, b.name);
    bf_jsonw_key(w, "mode"); bf_jsonw_str(w, "read-only");
    bf_jsonw_key(w, "transfer"); bf_jsonw_bool(w, 0);
    bf_jsonw_key(w, "merge_base");
    if (with_base) {
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "source_snapshot"); bf_jsonw_int(w, from_id);
        bf_jsonw_key(w, "peer_snapshot"); bf_jsonw_int(w, peer_id);
        bf_jsonw_key(w, "status"); bf_jsonw_str(w, "matching-recorded-manifests");
        bf_jsonw_obj_end(w);
    } else bf_jsonw_null(w);
    bf_jsonw_key(w, "conflict_policy"); bf_jsonw_str(w, "divergent files require ancestor-aware review");
    bf_jsonw_key(w, "source_symlinks_skipped"); bf_jsonw_int(w, (int64_t)x.symlinks);
    bf_jsonw_key(w, "peer_symlinks_skipped"); bf_jsonw_int(w, (int64_t)y.symlinks);
    bf_jsonw_key(w, "files"); bf_jsonw_arr(w);
    size_t i = 0, j = 0, k = 0, emitted = 0;
    while (i < x.n || j < y.n || (with_base && k < baseline_n)) {
        const char *path = NULL;
        if (i < x.n) path = x.v[i].path;
        if (j < y.n && (!path || strcmp(y.v[j].path, path) < 0)) path = y.v[j].path;
        if (with_base && k < baseline_n &&
            (!path || strcmp(baseline_source[k].path, path) < 0)) path = baseline_source[k].path;
        const FileEnt *left = i < x.n && !strcmp(x.v[i].path, path) ? &x.v[i++] : NULL;
        const FileEnt *right = j < y.n && !strcmp(y.v[j].path, path) ? &y.v[j++] : NULL;
        const Prev *old = with_base && k < baseline_n &&
                          !strcmp(baseline_source[k].path, path) ? &baseline_source[k++] : NULL;
        int cls = !left ? 1 : !right ? 0 : strcmp(left->sha, right->sha) ? 2 : 3;
        int base_cls = 0;
        if (with_base) {
            const char *l = replica_sha(left), *r = replica_sha(right);
            const char *base_sha = old ? old->sha : NULL;
            if (replica_equal(l, r))
                base_cls = replica_equal(l, base_sha) ? 0 : 3;
            else if (replica_equal(r, base_sha)) base_cls = 1;
            else if (replica_equal(l, base_sha)) base_cls = 2;
            else base_cls = 4;
            baseline_counts[base_cls]++;
        } else counts[cls]++;
        if (emitted < 256) {
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "path"); bf_jsonw_str(w, path);
            bf_jsonw_key(w, "status");
            if (with_base) bf_jsonw_str(w, base_cls == 0 ? "unchanged" :
                base_cls == 1 ? "source-changed" : base_cls == 2 ? "peer-changed" :
                base_cls == 3 ? "both-same" : "conflict");
            else bf_jsonw_str(w, cls == 0 ? "source-only" :
                cls == 1 ? "peer-only" : cls == 2 ? "divergent" : "identical");
            bf_jsonw_key(w, "source_sha256");
            if (left) bf_jsonw_str(w, left->sha); else bf_jsonw_null(w);
            bf_jsonw_key(w, "peer_sha256");
            if (right) bf_jsonw_str(w, right->sha); else bf_jsonw_null(w);
            if (with_base) {
                bf_jsonw_key(w, "base_sha256");
                if (old) bf_jsonw_str(w, old->sha); else bf_jsonw_null(w);
            }
            bf_jsonw_obj_end(w);
            emitted++;
        }
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "truncated"); bf_jsonw_bool(w, with_base ?
        (baseline_counts[0]+baseline_counts[1]+baseline_counts[2]+baseline_counts[3]+baseline_counts[4]) > 256 :
        (counts[0]+counts[1]+counts[2]+counts[3]) > 256);
    bf_jsonw_key(w, "counts"); bf_jsonw_obj(w);
    if (with_base) {
        bf_jsonw_key(w, "unchanged"); bf_jsonw_int(w, baseline_counts[0]);
        bf_jsonw_key(w, "source_changed"); bf_jsonw_int(w, baseline_counts[1]);
        bf_jsonw_key(w, "peer_changed"); bf_jsonw_int(w, baseline_counts[2]);
        bf_jsonw_key(w, "both_same"); bf_jsonw_int(w, baseline_counts[3]);
        bf_jsonw_key(w, "conflict"); bf_jsonw_int(w, baseline_counts[4]);
    } else {
        bf_jsonw_key(w, "source_only"); bf_jsonw_int(w, counts[0]);
        bf_jsonw_key(w, "peer_only"); bf_jsonw_int(w, counts[1]);
        bf_jsonw_key(w, "divergent"); bf_jsonw_int(w, counts[2]);
        bf_jsonw_key(w, "identical"); bf_jsonw_int(w, counts[3]);
    }
    bf_jsonw_obj_end(w);
    ob_files_free(&x); ob_files_free(&y);
    return EX_OK;
}

static int v_receipts(sqlite3 *db, bf_jsonw *w) {
    sqlite3_stmt *st; int limit = opt("--limit") ? atoi(opt("--limit")) : 50; if (limit <= 0 || limit > 1000) limit = 50;
    const char *name = opt("--name");
    if (sqlite3_prepare_v2(db, name ? "SELECT receipt_id,at,actor,verb,binding,target,outcome,code,reason,via,sha_before,sha_after FROM receipt WHERE account=? AND binding=? ORDER BY rowid DESC LIMIT ?"
                                    : "SELECT receipt_id,at,actor,verb,binding,target,outcome,code,reason,via,sha_before,sha_after FROM receipt WHERE account=? AND ?1=?1 ORDER BY rowid DESC LIMIT ?3", -1, &st, NULL) != SQLITE_OK)
        return fail(EX_FAIL, "store", "%s", sqlite3_errmsg(db));
    sqlite3_bind_text(st, 1, C.account, -1, SQLITE_STATIC); if (name) sqlite3_bind_text(st, 2, name, -1, SQLITE_STATIC); sqlite3_bind_int(st, 3, limit);
    static const char *const keys[] = {"receipt","at","actor","verb","binding","target","outcome","code","reason","via","sha_before","sha_after"};
    bf_jsonw_key(w, "receipts"); bf_jsonw_arr(w);
    while (sqlite3_step(st) == SQLITE_ROW) {
        bf_jsonw_obj(w);
        for (int i = 0; i < 12; i++) { bf_jsonw_key(w, keys[i]); bf_jsonw_str(w, (const char *)sqlite3_column_text(st, i)); }
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
    sqlite3_finalize(st);
    return EX_OK;
}

/* ------------------------------------------------------------ main */
static void usage(void) {
    fputs("usage: bonfyre-obsidian VERB [--account A] [options]   (contract: cmd/BonfyreObsidian/README.md)\n"
          "  doctor [--account A]\n"
          "  vaults | status [--name N] | receipts [--name N] [--limit K]           --account A\n"
          "  bind --name N --path DIR [--transport cli|headless|files] [--mode M] [--conflict S] [--exclude a,b]\n"
          "       [--device-name D] [--remote-vault V] [--config-dir .obsidian]\n"
          "  unbind --name N\n"
          "  read --name N --path NOTE                        [--via auto|cli|native]\n"
          "  search --name N --query Q [--prefix P] [--limit K]\n"
          "  tasks [--status all|todo|done|other] [--path NOTE] | tags [--path NOTE] | properties [--path NOTE]\n"
          "  backlinks --path NOTE | links --path NOTE | unresolved                  (all take --name N)\n"
          "  create|append|prepend --name N --path NOTE (--content T | --stdin) [--overwrite]\n"
          "  daily-append --name N [--date YYYY-MM-DD] (--content T | --stdin)  |  daily-path --name N [--date D]\n"
          "  task --name N --ref NOTE:LINE [--done|--todo|--toggle]\n"
          "  guard | sync-setup [--remote-vault V] | sync-config [...] | sync-once | sync-start | sync-stop | sync-status   --name N\n"
          "  snapshot --name N [--include-config] | diff --name N [--from ID] [--to ID]\n"
          "  replica-plan --name SOURCE --peer PEER [--account A]  [--base-source ID --base-peer ID] (read-only; no transfer)\n", stderr);
}

int main(int argc, char **argv) {
    bf_jsonw w; int rc = EX_OK; sqlite3 *db = NULL; const char *verb = argc > 1 ? argv[1] : "";
    g_argc = argc; g_argv = argv;
    umask(077);
    signal(SIGPIPE, SIG_IGN);
    if (argc < 2 || !strcmp(verb, "-h") || !strcmp(verb, "--help") || !strcmp(verb, "help")) { usage(); return argc < 2 ? EX_USAGE : EX_OK; }
    cfg_init();
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, verb);
    if (strcmp(C.via, "auto") && strcmp(C.via, "cli") && strcmp(C.via, "native")) rc = fail(EX_USAGE, "usage", "--via is auto, cli or native");
    else if (!strcmp(verb, "doctor")) {
        if (C.account && !valid_account(C.account)) rc = fail(EX_USAGE, "usage", "--account requires a safe account key or user:<positive decimal id>");
        else rc = v_doctor(&w);
    } else if ((rc = need_account()) == EX_OK) {
        if (!(db = store_open())) rc = fail(EX_FAIL, "store", "cannot open %s", C.db);
        else if (!strcmp(verb, "vaults")) rc = v_vaults(db, &w);
        else if (!strcmp(verb, "bind")) rc = v_bind(db, &w);
        else if (!strcmp(verb, "unbind")) rc = v_unbind(db, &w);
        else if (!strcmp(verb, "status")) rc = v_status(db, &w);
        else if (!strcmp(verb, "receipts")) rc = v_receipts(db, &w);
        else if (!strcmp(verb, "read")) rc = v_read(db, &w);
        else if (!strcmp(verb, "search") || !strcmp(verb, "tasks") || !strcmp(verb, "backlinks") || !strcmp(verb, "links")
                 || !strcmp(verb, "unresolved") || !strcmp(verb, "tags") || !strcmp(verb, "properties")) rc = reader(db, &w, verb);
        else if (!strcmp(verb, "create") || !strcmp(verb, "append") || !strcmp(verb, "prepend") || !strcmp(verb, "daily-append")) rc = writer(db, &w, verb);
        else if (!strcmp(verb, "daily-path")) rc = v_daily_path(db, &w);
        else if (!strcmp(verb, "task")) rc = v_task(db, &w);
        else if (!strcmp(verb, "guard")) rc = v_guard(db, &w);
        else if (!strncmp(verb, "sync-", 5) && (!strcmp(verb, "sync-setup") || !strcmp(verb, "sync-config") || !strcmp(verb, "sync-once")
                 || !strcmp(verb, "sync-start") || !strcmp(verb, "sync-stop") || !strcmp(verb, "sync-status"))) rc = v_sync(db, &w, verb);
        else if (!strcmp(verb, "snapshot")) rc = v_snapshot(db, &w);
        else if (!strcmp(verb, "replica-plan")) rc = v_replica_plan(db, &w);
        else if (!strcmp(verb, "diff")) rc = v_diff(db, &w);
        else { rc = fail(EX_USAGE, "usage", "unknown verb %s", verb); usage(); }
    }
    if (rc != EX_OK) {
        /* A refusal replaces whatever a verb began to write, except the guard
         * decision, which is the reason. */
        bf_jsonw_reset(&w);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "verb"); bf_jsonw_str(&w, verb);
        bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, 0);
        bf_jsonw_key(&w, "exit"); bf_jsonw_int(&w, rc);
        bf_jsonw_key(&w, "error"); bf_jsonw_str(&w, g_code[0] ? g_code : "failed");
        bf_jsonw_key(&w, "reason"); bf_jsonw_str(&w, g_reason);
        if (g_hand[0]) { bf_jsonw_key(&w, "by_hand"); bf_jsonw_str(&w, g_hand); }
        if (g_detail[0]) { bf_jsonw_key(&w, "detail"); bf_jsonw_str(&w, g_detail); }
    } else { bf_jsonw_key(&w, "ok"); bf_jsonw_bool(&w, 1); }
    bf_jsonw_obj_end(&w);
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout);
    bf_jsonw_free(&w);
    if (db) sqlite3_close(db);
    return rc;
}
