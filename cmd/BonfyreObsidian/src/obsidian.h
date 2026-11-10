/* bonfyre-obsidian: shared declarations.
 *
 * Three parts:
 *   main.c   configuration, the account-scoped store, receipts and the verbs
 *   vault.c  safe paths inside a vault, the native walk, Markdown reading
 *            (tasks, links, tags, properties, search), atomic writes, the
 *            daily-note path and content-addressed snapshots
 *   drive.c  running the two official tools (the desktop `obsidian` CLI and
 *            Obsidian Headless `ob`) with a deadline, the headless-sync guard
 *            and the continuous-sync supervisor (child or launchd)
 */
#ifndef BONFYRE_OBSIDIAN_H
#define BONFYRE_OBSIDIAN_H

#include <stddef.h>
#include <stdint.h>
#include <sqlite3.h>
#include "bf_jsonw.h"

#define OB_PATH 4096

/* Exit codes, the contract every caller reads (README.md). */
enum { EX_OK = 0, EX_FAIL = 1, EX_USAGE = 2, EX_REFUSED = 3, EX_NOTFOUND = 4, EX_MISSING = 5 };

typedef struct {
    const char *db;           /* --db | BONFYRE_OBSIDIAN_DB */
    const char *state;        /* BONFYRE_OBSIDIAN_STATE: per-account homes, logs, pidfiles */
    const char *roots;        /* BONFYRE_OBSIDIAN_ROOTS: ':'-separated allowed vault roots */
    const char *cli;          /* OBSIDIAN_CLI or `obsidian` on PATH */
    const char *ob;           /* BONFYRE_OBSIDIAN_OB or `ob` on PATH */
    const char *node;         /* BONFYRE_OBSIDIAN_NODE or `node` */
    const char *app;          /* BONFYRE_OBSIDIAN_APP: Obsidian.app */
    const char *app_support;  /* BONFYRE_OBSIDIAN_APP_SUPPORT: holds obsidian.json */
    const char *supervisor;   /* BONFYRE_OBSIDIAN_SUPERVISOR: child | launchd */
    const char *launchctl;    /* BONFYRE_OBSIDIAN_LAUNCHCTL */
    const char *agents_dir;   /* BONFYRE_OBSIDIAN_LAUNCH_AGENTS */
    const char *app_running;  /* BONFYRE_OBSIDIAN_APP_RUNNING: 1|0 overrides the process probe */
    const char *token_account;/* BONFYRE_OBSIDIAN_TOKEN_ACCOUNT: the one account OBSIDIAN_AUTH_TOKEN may reach */
    int cli_launch;           /* BONFYRE_OBSIDIAN_CLI_LAUNCH=1 lets a call start the desktop app */
    int timeout;              /* BONFYRE_OBSIDIAN_TIMEOUT seconds for one tool call */
    const char *account;      /* --account */
    const char *actor;        /* --actor */
    const char *via;          /* --via auto|cli|native */
    char home[OB_PATH];       /* $HOME */
} Cfg;

typedef struct {
    char account[160], name[96], vault_id[64], root[OB_PATH], transport[16];
    char sync_mode[24], conflict[16], excluded[1024], device[128], remote[256], config_dir[64], state[24];
    char created_at[32], updated_at[32];
} Binding;

/* ---- vault.c ---- */
int  ob_valid_name(const char *s, size_t max);
int  ob_rel_ok(const char *rel, const char *config_dir, char *why, size_t cap);
int  ob_resolve_read(const char *root, const char *rel, const char *config_dir, char out[OB_PATH], char *why, size_t cap);
int  ob_resolve_write(const char *root, const char *rel, const char *config_dir, int mkdirs, char out[OB_PATH], char *why, size_t cap);
int  ob_inside(const char *root, const char *path);
char *ob_read_file(const char *path, size_t *len, size_t max);
int  ob_atomic_write(const char *path, const char *data, size_t len, int exclusive);
int  ob_sha256_file(const char *path, char hex[65]);
void ob_sha256_mem(const void *data, size_t len, char hex[65]);

typedef struct { char *path; int64_t size, mtime_ns; char sha[65]; } FileEnt;
typedef struct { FileEnt *v; size_t n, cap; size_t symlinks, skipped; } FileList;
/* flags for ob_walk */
#define WALK_MD_ONLY      1
#define WALK_INCLUDE_CFG  2
int  ob_walk(const char *root, const char *config_dir, const char *excluded, int flags, FileList *out);
void ob_files_free(FileList *l);

int  ob_verb_search(bf_jsonw *w, const char *root, const Binding *b, const char *query, const char *prefix, int limit);
int  ob_verb_tasks(bf_jsonw *w, const char *root, const Binding *b, const char *status, const char *rel);
int  ob_verb_links(bf_jsonw *w, const char *root, const Binding *b, const char *mode, const char *rel);
int  ob_verb_tags(bf_jsonw *w, const char *root, const Binding *b, const char *rel);
int  ob_verb_properties(bf_jsonw *w, const char *root, const Binding *b, const char *rel);
int  ob_task_set(const char *file, int line, const char *how, char *why, size_t cap, char before[65], char after[65]);
int  ob_daily_rel(const char *root, const char *config_dir, const char *date, char *out, size_t cap);

/* ---- drive.c ---- */
typedef struct { int status; int timed_out; int spawned; char *out; size_t out_len; char *err; size_t err_len; } RunResult;
int  ob_run(char *const argv[], const char *home_override, int pass_token, int timeout_s, RunResult *r);
void ob_run_free(RunResult *r);
int  ob_which(const char *name, char out[OB_PATH]);
int  ob_app_running(const Cfg *c);
int  ob_cli_path(const Cfg *c, char out[OB_PATH]);
int  ob_cli_usable(const Cfg *c, char out[OB_PATH]);
int  ob_cli_version_ok(const RunResult *r);
int  ob_ob_path(const Cfg *c, char out[OB_PATH]);
void ob_account_home(const Cfg *c, const char *account, char out[OB_PATH]);
int  ob_mkdirs(const char *path, int mode);

typedef struct { int refuse; char code[48]; char reason[512]; char evidence[1024]; } Guard;
void ob_guard_headless(const Cfg *c, const Binding *b, Guard *g);
int  ob_desktop_sync_plugin(const char *root, const char *config_dir);
int  ob_registry_lookup(const Cfg *c, const char *real_root, char id[64], int *open);
void ob_registry_json(const Cfg *c, bf_jsonw *w);

int  ob_sup_start(const Cfg *c, const Binding *b, bf_jsonw *w, char *why, size_t cap);
int  ob_sup_stop(const Cfg *c, const Binding *b, bf_jsonw *w, char *why, size_t cap);
int  ob_sup_alive(const Cfg *c, const Binding *b, long *pid);
void ob_sup_paths(const Cfg *c, const Binding *b, char pidf[OB_PATH], char logf[OB_PATH], char label[256], char plist[OB_PATH]);

#endif
