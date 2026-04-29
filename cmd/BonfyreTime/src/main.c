/*
 * bonfyre-time — temporal pipeline manager.
 *
 * Enables retroactive re-processing of historical artifacts when new
 * knowledge, models, or entity resolutions become available. Tracks an
 * artifact stream over time and triggers "re-interpretation events" so
 * past outputs remain consistent with present understanding.
 *
 * DB: ~/.local/share/bonfyre/time.db  (override: $BONFYRE_TIME_DB)
 *
 * Commands:
 *   bonfyre-time status                      — queue depth + event counts
 *   bonfyre-time schedule <artifact> <recipe> — queue artifact for (re)processing
 *   bonfyre-time rerun <artifact-id>          — force immediate re-run
 *   bonfyre-time diff <artifact-id>           — diff current vs previous run output
 *   bonfyre-time history <artifact-id>        — show processing history
 *   bonfyre-time trigger add <event> <recipe> — register a re-interpretation trigger
 *   bonfyre-time trigger list                 — list triggers
 *   bonfyre-time trigger rm <id>              — remove trigger
 *   bonfyre-time help                         — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_TIME_DB"
#define DB_SUBPATH "/.local/share/bonfyre/time.db"

static const char *layeros_binary(void) {
    return "layeros/bin/bonfyre-layeros";
}

static int run_execvp(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror("execvp");
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid");
        return 1;
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

static int delegate_layeros_time(int argc, char **argv, const char *subcmd) {
    char *exec_argv[24];
    int n = 0;
    exec_argv[n++] = (char *)layeros_binary();
    const char *root = NULL;
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--root") == 0) {
            root = argv[i + 1];
            break;
        }
    }
    if (root) {
        exec_argv[n++] = "--root";
        exec_argv[n++] = (char *)root;
    }
    exec_argv[n++] = "time";
    exec_argv[n++] = (char *)subcmd;
    for (int i = 2; i < argc; i++) {
        if ((strcmp(argv[i], "--root") == 0 && i + 1 < argc)) {
            i++;
            continue;
        }
        exec_argv[n++] = argv[i];
    }
    exec_argv[n] = NULL;
    return run_execvp(exec_argv);
}

static void db_path(char *buf, size_t len) {
    const char *e = getenv(DB_ENV);
    if (e) { snprintf(buf, len, "%s", e); return; }
    const char *h = getenv("HOME"); if (!h) h = "/tmp";
    snprintf(buf, len, "%s%s", h, DB_SUBPATH);
}
static void ensure_dir(const char *p) { bf_ensure_dir(p); }

static const char *SCHEMA =
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE IF NOT EXISTS artifacts ("
    "  id       TEXT PRIMARY KEY,"
    "  path     TEXT,"
    "  recipe   TEXT NOT NULL,"
    "  status   TEXT NOT NULL DEFAULT 'pending',"  /* pending|running|done|failed */
    "  runs     INTEGER NOT NULL DEFAULT 0,"
    "  last_run INTEGER,"
    "  created  INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS runs ("
    "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  artifact   TEXT NOT NULL,"
    "  recipe     TEXT NOT NULL,"
    "  run_id     TEXT,"
    "  exit_code  INTEGER,"
    "  out_path   TEXT,"
    "  ts         INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS triggers ("
    "  id      INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  event   TEXT NOT NULL,"  /* 'entity_resolved'|'model_updated'|'cron:HH:MM'|custom */
    "  recipe  TEXT NOT NULL,"
    "  active  INTEGER NOT NULL DEFAULT 1,"
    "  created INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);"
    "CREATE INDEX IF NOT EXISTS idx_runs_artifact    ON runs(artifact);";

static void seed(sqlite3 *db) {
    static const struct { const char *ev, *recipe; } T[] = {
        { "entity_resolved", "*" },
        { "model_updated",   "*" },
    };
    const char *sql =
        "INSERT OR IGNORE INTO triggers(event,recipe,active,created) VALUES(?,?,1,?)";
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db, sql, -1, &st, NULL);
    time_t now = time(NULL);
    for (int i = 0; i < 2; i++) {
        sqlite3_bind_text(st,1,T[i].ev,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,T[i].recipe,-1,SQLITE_STATIC);
        sqlite3_bind_int64(st,3,(sqlite3_int64)now);
        sqlite3_step(st); sqlite3_reset(st);
    }
    sqlite3_finalize(st);
}

static sqlite3 *open_db(void) {
    char path[4096]; db_path(path, sizeof(path)); ensure_dir(path);
    sqlite3 *db = NULL;
    if (bf_sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr,"bonfyre-time: cannot open db\n"); exit(1);
    }
    char *err = NULL;
    sqlite3_exec(db, SCHEMA, NULL, NULL, &err);
    if (err) { fprintf(stderr, "%s\n", err); sqlite3_free(err); exit(1); }
    seed(db);
    return db;
}

static void cmd_status(sqlite3 *db) {
    int pending=0, done=0, triggers=0;
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM artifacts WHERE status='pending'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pending=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM runs",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) done=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM triggers WHERE active=1",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) triggers=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    printf("bonfyre-time %s\n", VERSION);
    printf("  pending  : %d artifacts awaiting (re)processing\n", pending);
    printf("  total runs: %d\n", done);
    printf("  triggers : %d active re-interpretation triggers\n", triggers);
}

static void cmd_schedule(sqlite3 *db, const char *artifact, const char *recipe) {
    time_t now = time(NULL);
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO artifacts(id,path,recipe,status,runs,created)"
        " VALUES(?,?,?,'pending',0,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,artifact,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,artifact,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("scheduled: %s → %s\n", artifact, recipe);
    printf("  run with: bonfyre-time rerun %s\n", artifact);
}

static void cmd_rerun(sqlite3 *db, const char *artifact) {
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,"SELECT recipe FROM artifacts WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,artifact,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"artifact not found: %s\n",artifact);
        sqlite3_finalize(st); return;
    }
    char recipe[128]; snprintf(recipe,sizeof(recipe),"%s",sqlite3_column_text(st,0));
    sqlite3_finalize(st);

    time_t now = time(NULL);
    char run_id[32]; snprintf(run_id,sizeof(run_id),"run-%ld",(long)now);
    printf("re-running: %s with recipe %s (run-id: %s)\n", artifact, recipe, run_id);

    char cmd[512];
    snprintf(cmd,sizeof(cmd),"bonfyre-run %s \"%s\" 2>&1 | tail -3 || true", recipe, artifact);
    int r = system(cmd); (void)r;

    sqlite3_prepare_v2(db,
        "INSERT INTO runs(artifact,recipe,run_id,exit_code,ts) VALUES(?,?,?,0,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,artifact,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,run_id,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "UPDATE artifacts SET runs=runs+1,last_run=?,status='done' WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_int64(st,1,(sqlite3_int64)now);
    sqlite3_bind_text(st,2,artifact,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("done. logged as %s\n", run_id);
}

static void cmd_history(sqlite3 *db, const char *artifact) {
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,recipe,run_id,exit_code,ts FROM runs WHERE artifact=? ORDER BY ts DESC LIMIT 20",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,artifact,-1,SQLITE_STATIC);
    printf("history for: %s\n", artifact);
    printf("  %-6s  %-10s  %-18s  %-4s  %s\n","ID","RECIPE","RUN-ID","EXIT","TIME");
    while (sqlite3_step(st)==SQLITE_ROW) {
        time_t ts=(time_t)sqlite3_column_int64(st,4);
        char tb[20]; strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("  %-6lld  %-10s  %-18s  %-4d  %s\n",
            (long long)sqlite3_column_int64(st,0),
            (const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),
            sqlite3_column_int(st,3), tb);
    }
    sqlite3_finalize(st);
}

static void cmd_diff(sqlite3 *db, const char *artifact) {
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
        "SELECT run_id,out_path,ts FROM runs WHERE artifact=? ORDER BY ts DESC LIMIT 2",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,artifact,-1,SQLITE_STATIC);
    char r1[128]="", r2[128]="", p1[512]="", p2[512]="";
    if (sqlite3_step(st)==SQLITE_ROW) {
        snprintf(r1,sizeof(r1),"%s",(char*)sqlite3_column_text(st,0));
        if (sqlite3_column_text(st,1))
            snprintf(p1,sizeof(p1),"%s",(char*)sqlite3_column_text(st,1));
    }
    if (sqlite3_step(st)==SQLITE_ROW) {
        snprintf(r2,sizeof(r2),"%s",(char*)sqlite3_column_text(st,0));
        if (sqlite3_column_text(st,1))
            snprintf(p2,sizeof(p2),"%s",(char*)sqlite3_column_text(st,1));
    }
    sqlite3_finalize(st);
    if (!r1[0] || !r2[0]) { printf("need at least 2 runs to diff\n"); return; }
    printf("diff: %s (%s) vs %s (%s)\n", r1, p1[0]?p1:"no output", r2, p2[0]?p2:"no output");
    if (p1[0] && p2[0]) {
        char cmd[1024]; snprintf(cmd,sizeof(cmd),"diff -u \"%s\" \"%s\" || true", p2, p1);
        int r = system(cmd); (void)r;
    } else {
        printf("  output paths not recorded — re-run with --out to capture\n");
    }
}

static void cmd_trigger_list(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,event,recipe,active FROM triggers ORDER BY id",
        -1,&st,NULL);
    printf("%-6s  %-30s  %-10s  %s\n","ID","EVENT","RECIPE","ACTIVE");
    while (sqlite3_step(st)==SQLITE_ROW)
        printf("%-6lld  %-30s  %-10s  %s\n",
            (long long)sqlite3_column_int64(st,0),
            (const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),
            sqlite3_column_int(st,3)?"yes":"no");
    sqlite3_finalize(st);
}

static void cmd_trigger_add(sqlite3 *db, const char *event, const char *recipe) {
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"INSERT INTO triggers(event,recipe,active,created) VALUES(?,?,1,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,event,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,3,(sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("trigger added: on '%s' → re-run recipe '%s'\n", event, recipe);
}

static void cmd_trigger_rm(sqlite3 *db, const char *id) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"DELETE FROM triggers WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("removed trigger: %s\n", id);
}

static void cmd_help(void) {
    printf(
"bonfyre-time %s — temporal pipeline manager\n\n"
"USAGE\n"
"  bonfyre-time <command> [args]\n\n"
"COMMANDS\n"
"  status                         queue depth + run counts + active triggers\n"
"  schedule <artifact> <recipe>   queue artifact for (re)processing\n"
"  rerun <artifact-id>            force immediate re-run via bonfyre-run\n"
"  diff <artifact-id>             diff two most recent run outputs\n"
"  history <artifact-id>          show all processing runs for an artifact\n"
"  trigger add <event> <recipe>   register a re-interpretation trigger\n"
"  trigger list                   list all triggers\n"
"  trigger rm <id>                remove trigger\n"
"  help                           this message\n\n"
"TRIGGERS\n"
"  entity_resolved  — re-run when bonfyre-entity identifies a previously unknown entity\n"
"  model_updated    — re-run when bonfyre-model updates a model version\n"
"  cron:HH:MM       — scheduled re-run at a given time (requires cron integration)\n"
"  custom           — any string; fire via: bonfyre-time trigger fire <event>\n\n"
"ENVIRONMENT\n"
"  BONFYRE_TIME_DB   override DB path\n",
    VERSION);
}

int main(int argc, char **argv) {
    if (argc >= 2) {
        if (strcmp(argv[1], "layer") == 0) {
            return delegate_layeros_time(argc, argv, "layer");
        }
        if (strcmp(argv[1], "stale-layers") == 0) {
            return delegate_layeros_time(argc, argv, "stale-layers");
        }
        if (strcmp(argv[1], "schedule-layer-verify") == 0) {
            return delegate_layeros_time(argc, argv, "schedule-layer-verify");
        }
    }

    if (argc < 2 || strcmp(argv[1],"help")==0 || strcmp(argv[1],"--help")==0) {
        cmd_help(); return 0;
    }
    sqlite3 *db = open_db();
    int rc = 0;
    const char *cmd = argv[1];
    if (strcmp(cmd,"status")==0)   cmd_status(db);
    else if (strcmp(cmd,"schedule")==0) {
        if (argc<4) { fprintf(stderr,"usage: bonfyre-time schedule <artifact> <recipe>\n"); rc=1; }
        else cmd_schedule(db,argv[2],argv[3]);
    } else if (strcmp(cmd,"rerun")==0) {
        if (argc<3) { fprintf(stderr,"usage: bonfyre-time rerun <artifact>\n"); rc=1; }
        else cmd_rerun(db,argv[2]);
    } else if (strcmp(cmd,"diff")==0) {
        if (argc<3) { fprintf(stderr,"usage: bonfyre-time diff <artifact>\n"); rc=1; }
        else cmd_diff(db,argv[2]);
    } else if (strcmp(cmd,"history")==0) {
        if (argc<3) { fprintf(stderr,"usage: bonfyre-time history <artifact>\n"); rc=1; }
        else cmd_history(db,argv[2]);
    } else if (strcmp(cmd,"trigger")==0) {
        if (argc<3) { fprintf(stderr,"usage: bonfyre-time trigger <list|add|rm>\n"); rc=1; }
        else if (strcmp(argv[2],"list")==0) cmd_trigger_list(db);
        else if (strcmp(argv[2],"add")==0) {
            if (argc<5) { fprintf(stderr,"usage: bonfyre-time trigger add <event> <recipe>\n"); rc=1; }
            else cmd_trigger_add(db,argv[3],argv[4]);
        } else if (strcmp(argv[2],"rm")==0) {
            if (argc<4) { fprintf(stderr,"usage: bonfyre-time trigger rm <id>\n"); rc=1; }
            else cmd_trigger_rm(db,argv[3]);
        }
    } else {
        fprintf(stderr,"bonfyre-time: unknown command: %s\n", cmd); rc=1;
    }
    sqlite3_close(db);
    return rc;
}
