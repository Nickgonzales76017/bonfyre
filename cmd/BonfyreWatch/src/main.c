#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <spawn.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#include <bonfyre.h>

extern char **environ;

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
    char root[PATH_MAX];
    char watch_dir[PATH_MAX];
    char pipeline[128];
    char out_dir[PATH_MAX];
    int interval_sec;
    int once;
    int dry_run;
    int recursive;
} WatchConfig;

typedef struct {
    char session_id[65];
    char path[PATH_MAX];
    long long size;
    long long mtime;
} FileEntry;

static void usage(void) {
    fprintf(stderr,
            "BonfyreWatch — filesystem reality bridge\n\n"
            "Usage:\n"
            "  bonfyre-watch <dir> --pipeline <name> [--out DIR] [--interval N] [--once] [--dry-run] [--root DIR]\n\n"
            "Examples:\n"
            "  bonfyre-watch ~/Downloads --pipeline transcript-family\n"
            "  bonfyre-watch uploads --pipeline pipeline --out outputs/watch\n");
}

static void json_escape(const char *s) {
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    putchar('"');
    while (*p) {
        unsigned char c = *p++;
        switch (c) {
            case '\\': fputs("\\\\", stdout); break;
            case '"': fputs("\\\"", stdout); break;
            case '\n': fputs("\\n", stdout); break;
            case '\r': fputs("\\r", stdout); break;
            case '\t': fputs("\\t", stdout); break;
            default:
                if (c < 0x20) printf("\\u%04x", (unsigned)c);
                else putchar((char)c);
                break;
        }
    }
    putchar('"');
}

static void now_iso8601(char out[32]) {
    time_t now = time(NULL);
    struct tm tmv;
    gmtime_r(&now, &tmv);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static void join_path(char *buf, size_t sz, const char *a, const char *b) {
    if (!a || !a[0]) snprintf(buf, sz, "%s", b ? b : "");
    else if (!b || !b[0]) snprintf(buf, sz, "%s", a);
    else if (a[strlen(a) - 1] == '/') snprintf(buf, sz, "%s%s", a, b);
    else snprintf(buf, sz, "%s/%s", a, b);
}

static const char *file_basename(const char *path) {
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

static void strip_extension(const char *name, char *out, size_t out_sz) {
    const char *dot;
    size_t n;
    snprintf(out, out_sz, "%s", name ? name : "");
    dot = strrchr(out, '.');
    if (dot && dot != out) {
        n = (size_t)(dot - out);
        if (n < out_sz) out[n] = '\0';
    }
}

static int is_regular_file(const char *path, struct stat *st_out) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    if (st_out) *st_out = st;
    return S_ISREG(st.st_mode);
}

static int should_skip_name(const char *name) {
    if (!name || !name[0]) return 1;
    if (name[0] == '.') return 1;
    if (strstr(name, ".part")) return 1;
    if (strstr(name, ".tmp")) return 1;
    if (strstr(name, ".crdownload")) return 1;
    return 0;
}

static int ensure_watch_schema(const char *root, char *db_path, size_t db_path_sz) {
    sqlite3 *db = NULL;
    char watch_dir[PATH_MAX];
    const char *schema =
        "CREATE TABLE IF NOT EXISTS watch_sessions("
        " session_id TEXT PRIMARY KEY,"
        " watch_dir TEXT, pipeline TEXT, out_dir TEXT, interval_sec INTEGER,"
        " dry_run INTEGER, recursive INTEGER, created_at TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS watch_events("
        " session_id TEXT, event_id TEXT PRIMARY KEY, path TEXT, size INTEGER, mtime INTEGER,"
        " status TEXT, output_dir TEXT, pipeline TEXT, run_ref TEXT, created_at TEXT"
        ");";
    if (bf_ensure_dir(root) != 0) return 1;
    join_path(watch_dir, sizeof(watch_dir), root, "watch");
    if (bf_ensure_dir(watch_dir) != 0) return 1;
    join_path(db_path, db_path_sz, watch_dir, "watch.db");
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, schema, NULL, NULL, NULL);
    sqlite3_close(db);
    return 0;
}

static int open_watch_db(const char *root, sqlite3 **db, char *db_path, size_t db_path_sz) {
    if (ensure_watch_schema(root, db_path, db_path_sz) != 0) return 1;
    if (sqlite3_open(db_path, db) != SQLITE_OK) {
        if (*db) sqlite3_close(*db);
        *db = NULL;
        return 1;
    }
    return 0;
}

static void seed_session_id(WatchConfig *cfg, char out[65]) {
    char seed[PATH_MAX * 2];
    char now[32];
    now_iso8601(now);
    snprintf(seed, sizeof(seed), "%s:%s:%s:%s:%d:%d", now, cfg->watch_dir, cfg->pipeline, cfg->out_dir, cfg->interval_sec, getpid());
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), out);
}

static int record_session(const WatchConfig *cfg, const char *session_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX], now[32];
    now_iso8601(now);
    if (open_watch_db(cfg->root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO watch_sessions(session_id,watch_dir,pipeline,out_dir,interval_sec,dry_run,recursive,created_at) VALUES(?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, session_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, cfg->watch_dir, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, cfg->pipeline, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, cfg->out_dir, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 5, cfg->interval_sec);
    sqlite3_bind_int(st, 6, cfg->dry_run);
    sqlite3_bind_int(st, 7, cfg->recursive);
    sqlite3_bind_text(st, 8, now, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

static int event_seen(const char *root, const char *session_id, const char *path, long long size, long long mtime) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int seen = 0;
    if (open_watch_db(root, &db, db_path, sizeof(db_path)) != 0) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT 1 FROM watch_events WHERE session_id=? AND path=? AND size=? AND mtime=? LIMIT 1",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, session_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 3, size);
        sqlite3_bind_int64(st, 4, mtime);
        seen = (sqlite3_step(st) == SQLITE_ROW);
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return seen;
}

static int record_event(const char *root, const char *session_id, const char *path,
                        long long size, long long mtime, const char *status,
                        const char *output_dir, const char *pipeline, const char *run_ref) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX], now[32], seed[PATH_MAX * 2], event_id[65];
    now_iso8601(now);
    snprintf(seed, sizeof(seed), "%s:%s:%lld:%lld:%s", session_id, path, size, mtime, status ? status : "");
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), event_id);
    if (open_watch_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO watch_events(session_id,event_id,path,size,mtime,status,output_dir,pipeline,run_ref,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, session_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, event_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, path, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 4, size);
    sqlite3_bind_int64(st, 5, mtime);
    sqlite3_bind_text(st, 6, status ? status : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7, output_dir ? output_dir : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 8, pipeline ? pipeline : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 9, run_ref ? run_ref : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st,10, now, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

static int resolve_binary(const char *binary, const char *sibling_dir, char *out, size_t out_sz) {
    const char *candidates[5];
    char exe_dir[PATH_MAX];
    uint32_t bsz = sizeof(exe_dir);
    exe_dir[0] = '\0';
#ifdef __APPLE__
    if (_NSGetExecutablePath(exe_dir, &bsz) == 0) {
        char *last = strrchr(exe_dir, '/');
        if (last) *last = '\0';
    } else {
        exe_dir[0] = '\0';
    }
#endif
    candidates[0] = binary;
    if (exe_dir[0]) {
        snprintf(out, out_sz, "%s/%s", exe_dir, binary);
        if (access(out, X_OK) == 0) return 0;
        snprintf(out, out_sz, "%s/../%s/%s", exe_dir, sibling_dir, binary);
        if (access(out, X_OK) == 0) return 0;
        snprintf(out, out_sz, "%s/../%s/build/%s", exe_dir, sibling_dir, binary);
        if (access(out, X_OK) == 0) return 0;
        snprintf(out, out_sz, "%s/../../cmd/%s/%s", exe_dir, sibling_dir, binary);
        if (access(out, X_OK) == 0) return 0;
    }
    if (access(binary, X_OK) == 0) { snprintf(out, out_sz, "%s", binary); return 0; }
    return 1;
}

static int spawn_and_wait(char *const argv[]) {
    pid_t pid;
    int status = 0;
    if (posix_spawnp(&pid, argv[0], NULL, NULL, argv, environ) != 0) return 1;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

static int trigger_pipeline(const WatchConfig *cfg, const char *input_path, const char *output_dir, char *run_ref, size_t run_ref_sz) {
    char binary[PATH_MAX];
    int rc;
    run_ref[0] = '\0';
    if (strcmp(cfg->pipeline, "transcript-family") == 0) {
        char *argvv[] = { NULL, (char *)input_path, (char *)output_dir, NULL };
        if (resolve_binary("bonfyre-transcript-family", "BonfyreTranscriptFamily", binary, sizeof(binary)) != 0) return 1;
        argvv[0] = binary;
        snprintf(run_ref, run_ref_sz, "binary:%s", binary);
        return spawn_and_wait(argvv);
    }
    if (strcmp(cfg->pipeline, "pipeline") == 0) {
        char *argvv[] = { NULL, "run", (char *)input_path, "--out", (char *)output_dir, NULL };
        if (resolve_binary("bonfyre-pipeline", "BonfyrePipeline", binary, sizeof(binary)) != 0) return 1;
        argvv[0] = binary;
        snprintf(run_ref, run_ref_sz, "binary:%s run", binary);
        return spawn_and_wait(argvv);
    }
    if (strcmp(cfg->pipeline, "transcribe") == 0) {
        char *argvv[] = { NULL, (char *)input_path, (char *)output_dir, NULL };
        if (resolve_binary("bonfyre-transcribe", "BonfyreTranscribe", binary, sizeof(binary)) != 0) return 1;
        argvv[0] = binary;
        snprintf(run_ref, run_ref_sz, "binary:%s", binary);
        return spawn_and_wait(argvv);
    }
    rc = 1;
    snprintf(run_ref, run_ref_sz, "unsupported:%s", cfg->pipeline);
    return rc;
}

static int process_file(const WatchConfig *cfg, const char *session_id, const char *path, const struct stat *st) {
    char base[PATH_MAX], outdir[PATH_MAX], run_ref[PATH_MAX];
    int rc = 0;
    strip_extension(file_basename(path), base, sizeof(base));
    join_path(outdir, sizeof(outdir), cfg->out_dir, base);
    if (bf_ensure_dir(cfg->out_dir) != 0) return 1;
    if (bf_ensure_dir(outdir) != 0) return 1;
    if (cfg->dry_run) {
        record_event(cfg->root, session_id, path, (long long)st->st_size, (long long)st->st_mtime, "planned", outdir, cfg->pipeline, "dry-run");
        printf("{\"event\":\"planned\",\"path\":"); json_escape(path);
        printf(",\"pipeline\":"); json_escape(cfg->pipeline);
        printf(",\"output_dir\":"); json_escape(outdir);
        printf("}\n");
        return 0;
    }
    rc = trigger_pipeline(cfg, path, outdir, run_ref, sizeof(run_ref));
    record_event(cfg->root, session_id, path, (long long)st->st_size, (long long)st->st_mtime,
                 rc == 0 ? "triggered" : "failed", outdir, cfg->pipeline, run_ref);
    printf("{\"event\":"); json_escape(rc == 0 ? "triggered" : "failed");
    printf(",\"path\":"); json_escape(path);
    printf(",\"pipeline\":"); json_escape(cfg->pipeline);
    printf(",\"output_dir\":"); json_escape(outdir);
    printf(",\"run_ref\":"); json_escape(run_ref);
    printf(",\"exit_code\":%d}\n", rc);
    fflush(stdout);
    return rc;
}

static int scan_dir_once(const WatchConfig *cfg, const char *session_id, int *new_count) {
    DIR *dir = opendir(cfg->watch_dir);
    struct dirent *ent;
    int failures = 0;
    *new_count = 0;
    if (!dir) return 1;
    while ((ent = readdir(dir)) != NULL) {
        char path[PATH_MAX];
        struct stat st;
        if (should_skip_name(ent->d_name)) continue;
        join_path(path, sizeof(path), cfg->watch_dir, ent->d_name);
        if (!is_regular_file(path, &st)) continue;
        if (event_seen(cfg->root, session_id, path, (long long)st.st_size, (long long)st.st_mtime)) continue;
        (*new_count)++;
        if (process_file(cfg, session_id, path, &st) != 0) failures++;
    }
    closedir(dir);
    return failures ? 1 : 0;
}

int main(int argc, char **argv) {
    WatchConfig cfg;
    char session_id[65];
    int new_count = 0;
    int first_pass = 1;
    if (argc < 2) { usage(); return 1; }
    memset(&cfg, 0, sizeof(cfg));
    snprintf(cfg.root, sizeof(cfg.root), "%s", "layeros/state");
    snprintf(cfg.pipeline, sizeof(cfg.pipeline), "%s", "transcript-family");
    cfg.interval_sec = 2;
    cfg.once = 0;
    cfg.dry_run = 0;
    cfg.recursive = 0;
    snprintf(cfg.watch_dir, sizeof(cfg.watch_dir), "%s", argv[1]);
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--pipeline") == 0 && i + 1 < argc) snprintf(cfg.pipeline, sizeof(cfg.pipeline), "%s", argv[++i]);
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) snprintf(cfg.out_dir, sizeof(cfg.out_dir), "%s", argv[++i]);
        else if (strcmp(argv[i], "--interval") == 0 && i + 1 < argc) cfg.interval_sec = atoi(argv[++i]);
        else if (strcmp(argv[i], "--once") == 0) cfg.once = 1;
        else if (strcmp(argv[i], "--dry-run") == 0) cfg.dry_run = 1;
        else if (strcmp(argv[i], "--recursive") == 0) cfg.recursive = 1;
        else if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) snprintf(cfg.root, sizeof(cfg.root), "%s", argv[++i]);
    }
    if (!cfg.watch_dir[0]) { usage(); return 1; }
    if (!cfg.out_dir[0]) snprintf(cfg.out_dir, sizeof(cfg.out_dir), "%s/watch-output", cfg.root);
    if (bf_ensure_dir(cfg.out_dir) != 0) {
        fprintf(stderr, "bonfyre-watch: cannot create output dir %s\n", cfg.out_dir);
        return 1;
    }
    seed_session_id(&cfg, session_id);
    record_session(&cfg, session_id);
    printf("{\"session_id\":"); json_escape(session_id);
    printf(",\"watch_dir\":"); json_escape(cfg.watch_dir);
    printf(",\"pipeline\":"); json_escape(cfg.pipeline);
    printf(",\"out_dir\":"); json_escape(cfg.out_dir);
    printf(",\"mode\":"); json_escape(cfg.dry_run ? "dry-run" : "live");
    printf(",\"interval_sec\":%d}\n", cfg.interval_sec);
    fflush(stdout);
    for (;;) {
        int rc = scan_dir_once(&cfg, session_id, &new_count);
        if (cfg.once) return rc;
        if (first_pass) first_pass = 0;
        sleep(cfg.interval_sec > 0 ? cfg.interval_sec : 2);
    }
}
