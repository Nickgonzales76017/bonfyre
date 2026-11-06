#define _POSIX_C_SOURCE 200809L

#include "bf_command_lookup.h"
#include "bf_jsonw.h"
#include "bonfyre.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define OPERATOR_STORE_COMMANDS(X) \
    X("archive", OPERATOR_STORE_ARCHIVE, 10) \
    X("backlog-audit", OPERATOR_STORE_BACKLOG_AUDIT, 0) \
    X("init", OPERATOR_STORE_INIT, 4) \
    X("inspect", OPERATOR_STORE_INSPECT, 4) \
    X("quality-record", OPERATOR_STORE_QUALITY_RECORD, 0) \
    X("reconcile-reverted", OPERATOR_STORE_RECONCILE_REVERTED, 10) \
    X("route", OPERATOR_STORE_ROUTE, 0)

enum {
#define OPERATOR_STORE_ENUM(name, id, mode) id,
    OPERATOR_STORE_COMMANDS(OPERATOR_STORE_ENUM)
#undef OPERATOR_STORE_ENUM
};

static const BfCommandEntry operator_store_commands[] = {
#define OPERATOR_STORE_ENTRY(name, id, mode) {name, id, mode, "--db"},
    OPERATOR_STORE_COMMANDS(OPERATOR_STORE_ENTRY)
#undef OPERATOR_STORE_ENTRY
};

static const char schema_version[] = "bonfyre.operator_store.v1";
static const char schema_sql[] =
    "CREATE TABLE IF NOT EXISTS goals ("
    "id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,"
    "priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,"
    "interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,"
    "last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS runs ("
    "id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,state TEXT NOT NULL,"
    "response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');"
    "CREATE TABLE IF NOT EXISTS actions ("
    "seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,tool TEXT NOT NULL,args_json TEXT NOT NULL,"
    "state TEXT NOT NULL,result_json TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS budgets (day TEXT PRIMARY KEY,cycles INTEGER NOT NULL DEFAULT 0,"
    "tool_calls INTEGER NOT NULL DEFAULT 0,input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS operator_response_usage ("
    "response_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,usage_json TEXT NOT NULL,recorded_at REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS operator_run_owners ("
    "run_id TEXT PRIMARY KEY,pid INTEGER NOT NULL,owner_host TEXT NOT NULL,owner_nonce TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS operator_provider_health ("
    "provider TEXT PRIMARY KEY,state TEXT NOT NULL,cause TEXT NOT NULL,"
    "observed_at REAL NOT NULL,retry_at REAL NOT NULL DEFAULT 0);"
    "CREATE INDEX IF NOT EXISTS operator_goals_due ON goals(state,priority DESC,updated_at,retry_at);"
    "CREATE INDEX IF NOT EXISTS operator_runs_goal_open ON runs(goal_id,completed_at,started_at);"
    "CREATE INDEX IF NOT EXISTS operator_runs_started ON runs(started_at);"
    "CREATE INDEX IF NOT EXISTS operator_actions_run ON actions(run_id,seq);";

static int emit_error(const char *message)
{
    bf_jsonw out;
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "error");
    bf_jsonw_str(&out, message);
    bf_jsonw_obj_end(&out);
    if (!out.oom) {
        fwrite(out.buf, 1, out.len, stdout);
        fputc('\n', stdout);
    }
    bf_jsonw_free(&out);
    return 2;
}

static int safe_database_path(const char *path)
{
    char parent[PATH_MAX];
    char resolved_parent[PATH_MAX];
    const char *slash;
    struct stat entry;
    struct stat directory;
    size_t length;

    if (!path || !(length = strlen(path)) || length >= PATH_MAX) return 0;
    slash = strrchr(path, '/');
    if (!slash) {
        memcpy(parent, ".", 2);
    } else if (slash == path) {
        memcpy(parent, "/", 2);
    } else {
        size_t parent_length = (size_t)(slash - path);
        if (parent_length >= sizeof(parent)) return 0;
        memcpy(parent, path, parent_length);
        parent[parent_length] = '\0';
    }
    if (!realpath(parent, resolved_parent) ||
        lstat(resolved_parent, &directory) != 0 ||
        !S_ISDIR(directory.st_mode) || S_ISLNK(directory.st_mode)) return 0;
    if (lstat(path, &entry) == 0)
        return S_ISREG(entry.st_mode) && !S_ISLNK(entry.st_mode);
    return errno == ENOENT;
}

static int apply_schema(sqlite3 *db)
{
    char *error = NULL;
    int rc;
    size_t length = strlen(schema_sql);
    char *sql = malloc(length + sizeof("BEGIN IMMEDIATE;COMMIT;"));
    if (!sql) return 0;
    snprintf(sql, length + sizeof("BEGIN IMMEDIATE;COMMIT;"), "BEGIN IMMEDIATE;%sCOMMIT;", schema_sql);
    rc = sqlite3_exec(db, sql, NULL, NULL, &error);
    sqlite3_free(error);
    free(sql);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return 0;
    }
    return 1;
}

static int initialize_store(const char *path)
{
    sqlite3 *db = NULL;
    bf_jsonw out;
    int ok;
    if (!safe_database_path(path)) return emit_error("operator store requires a regular database path under an existing local directory");
    if (bf_sqlite3_open_tier(path, &db, BF_SQLITE_DURABLE) != SQLITE_OK) return emit_error("operator store could not open at full durability");
    sqlite3_busy_timeout(db, 30000);
    ok = apply_schema(db);
    sqlite3_close(db);
    if (!ok) return emit_error("operator store schema initialization failed");
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "database");
    bf_jsonw_str(&out, path);
    bf_jsonw_key(&out, "durability");
    bf_jsonw_str(&out, "full");
    bf_jsonw_key(&out, "schema_version");
    bf_jsonw_str(&out, schema_version);
    bf_jsonw_obj_end(&out);
    if (!out.oom) {
        fwrite(out.buf, 1, out.len, stdout);
        fputc('\n', stdout);
    }
    bf_jsonw_free(&out);
    return 0;
}

static int inspect_store(const char *path)
{
    sqlite3 *db = NULL;
    sqlite3_stmt *statement = NULL;
    bf_jsonw out;
    int rc;
    if (!safe_database_path(path)) return emit_error("operator store requires a regular database path under an existing local directory");
    if (bf_sqlite3_open_ro(path, &db) != SQLITE_OK) return emit_error("operator store could not open read-only");
    sqlite3_busy_timeout(db, 3000);
    rc = sqlite3_prepare_v2(db,
        "SELECT "
        "(SELECT COUNT(*) FROM goals),"
        "(SELECT COUNT(*) FROM goals WHERE state='queued'),"
        "(SELECT COUNT(*) FROM goals WHERE state='blocked'),"
        "(SELECT COUNT(*) FROM runs),"
        "(SELECT COUNT(*) FROM runs WHERE completed_at IS NULL),"
        "(SELECT COUNT(*) FROM actions),"
        "(SELECT COUNT(*) FROM operator_provider_health)",
        -1, &statement, NULL);
    if (rc != SQLITE_OK || sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        sqlite3_close(db);
        return emit_error("operator store does not satisfy the declared schema");
    }
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "actions");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 5));
    bf_jsonw_key(&out, "blocked_goals");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 2));
    bf_jsonw_key(&out, "database");
    bf_jsonw_str(&out, path);
    bf_jsonw_key(&out, "goals");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 0));
    bf_jsonw_key(&out, "open_runs");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 4));
    bf_jsonw_key(&out, "provider_health_records");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 6));
    bf_jsonw_key(&out, "queued_goals");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 1));
    bf_jsonw_key(&out, "runs");
    bf_jsonw_int(&out, sqlite3_column_int64(statement, 3));
    bf_jsonw_key(&out, "schema_version");
    bf_jsonw_str(&out, schema_version);
    bf_jsonw_obj_end(&out);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    if (!out.oom) {
        fwrite(out.buf, 1, out.len, stdout);
        fputc('\n', stdout);
    }
    bf_jsonw_free(&out);
    return 0;
}
static int backlog_audit_store(int argc, char **argv)
{
    const char *path = NULL, *state = "queued";
    int limit = 100, offset = 0, total = 0, returned = 0, eligible = 0, native_archive = 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *s = NULL, *count = NULL;
    bf_jsonw out;
    int result = 2;
    int saw_state = 0, saw_limit = 0, saw_offset = 0;
    if (argc < 4 || argc > 10 || argc % 2 != 0)
        return emit_error("use backlog-audit --db PATH [--state queued|blocked|retry|resolved|superseded|in_progress] [--limit 1..200] [--offset 0..1000000]");
    for (int i = 2; i + 1 < argc; i += 2) {
        if (!strcmp(argv[i], "--db")) {
            if (path) return emit_error("backlog-audit duplicate --db");
            path = argv[i + 1];
        } else if (!strcmp(argv[i], "--state")) {
            if (saw_state++) return emit_error("backlog-audit duplicate --state");
            state = argv[i + 1];
        } else if (!strcmp(argv[i], "--limit")) {
            char *end = NULL;
            long n;
            if (saw_limit++) return emit_error("backlog-audit duplicate --limit");
            n = strtol(argv[i + 1], &end, 10);
            if (!argv[i + 1][0] || *end || n < 1 || n > 200)
                return emit_error("backlog-audit limit must be 1..200");
            limit = (int)n;
        } else if (!strcmp(argv[i], "--offset")) {
            char *end = NULL;
            long n;
            if (saw_offset++) return emit_error("backlog-audit duplicate --offset");
            n = strtol(argv[i + 1], &end, 10);
            if (!argv[i + 1][0] || *end || n < 0 || n > 1000000)
                return emit_error("backlog-audit offset must be 0..1000000");
            offset = (int)n;
        } else return emit_error("backlog-audit unknown argument");
    }
    if (!path || !*path || (strcmp(state, "queued") && strcmp(state, "blocked") &&
        strcmp(state, "retry") && strcmp(state, "resolved") &&
        strcmp(state, "superseded") && strcmp(state, "in_progress")))
        return emit_error("backlog-audit requires a database and known state");
    if (!safe_database_path(path) || bf_sqlite3_open_ro(path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return emit_error("backlog-audit requires an existing regular read-only database");
    }
    sqlite3_busy_timeout(db, 3000);
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM goals WHERE state=?1", -1, &count, NULL) != SQLITE_OK ||
        sqlite3_bind_text(count, 1, state, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(count) != SQLITE_ROW) {
        sqlite3_finalize(count); sqlite3_close(db);
        return emit_error("backlog-audit could not count existing goal state");
    }
    total = sqlite3_column_int(count, 0);
    sqlite3_finalize(count);
    if (sqlite3_prepare_v2(db,
        "SELECT g.id,g.title,g.priority,g.state,g.source,g.recurring,g.updated_at,substr(g.last_error,1,240),"
        "(SELECT COUNT(*) FROM runs r WHERE r.goal_id=g.id),"
        "(SELECT COUNT(*) FROM runs r WHERE r.goal_id=g.id AND r.completed_at IS NULL),g.dedupe_key,substr(g.details,1,800) "
        "FROM goals g WHERE g.state=?1 ORDER BY g.priority DESC,g.updated_at ASC,g.id ASC LIMIT ?2 OFFSET ?3",
        -1, &s, NULL) != SQLITE_OK ||
        sqlite3_bind_text(s, 1, state, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_int(s, 2, limit) != SQLITE_OK || sqlite3_bind_int(s, 3, offset) != SQLITE_OK) {
        sqlite3_finalize(s); sqlite3_close(db);
        return emit_error("backlog-audit could not read existing goal schema");
    }
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "schema_version"); bf_jsonw_str(&out, "bonfyre.operator_store.backlog_audit.v1");
    bf_jsonw_key(&out, "state"); bf_jsonw_str(&out, state);
    bf_jsonw_key(&out, "limit"); bf_jsonw_int(&out, limit);
    bf_jsonw_key(&out, "offset"); bf_jsonw_int(&out, offset);
    bf_jsonw_key(&out, "total_in_state"); bf_jsonw_int(&out, total);
    bf_jsonw_key(&out, "rows"); bf_jsonw_arr(&out);
    int step;
    while ((step = sqlite3_step(s)) == SQLITE_ROW) {
        long long runs = sqlite3_column_int64(s, 8);
        int candidate = !strcmp(state, "queued") && runs == 0 && !sqlite3_column_int(s, 5);
        const unsigned char *id = sqlite3_column_text(s, 0);
        const unsigned char *title = sqlite3_column_text(s, 1);
        const unsigned char *source = sqlite3_column_text(s, 4);
        const unsigned char *note = sqlite3_column_text(s, 7);
        int archive_review = id && strlen((const char *)id) == 32 &&
            strspn((const char *)id, "0123456789abcdef") == 32 &&
            !sqlite3_column_int(s, 5) && !sqlite3_column_int64(s, 9) &&
            (!strcmp(state, "queued") || !strcmp(state, "retry") || !strcmp(state, "blocked"));
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "id"); bf_jsonw_str(&out, id ? (const char *)id : "");
        bf_jsonw_key(&out, "dedupe_key"); bf_jsonw_str(&out, (const char *)sqlite3_column_text(s, 10));
        bf_jsonw_key(&out, "title"); bf_jsonw_str(&out, title ? (const char *)title : "");
        bf_jsonw_key(&out, "state"); bf_jsonw_str(&out, (const char *)sqlite3_column_text(s, 3));
        bf_jsonw_key(&out, "priority"); bf_jsonw_int(&out, sqlite3_column_int(s, 2));
        bf_jsonw_key(&out, "source"); bf_jsonw_str(&out, source ? (const char *)source : "");
        bf_jsonw_key(&out, "recurring"); bf_jsonw_bool(&out, sqlite3_column_int(s, 5) != 0);
        bf_jsonw_key(&out, "updated_at"); bf_jsonw_num(&out, sqlite3_column_double(s, 6));
        bf_jsonw_key(&out, "last_error_excerpt"); bf_jsonw_str(&out, note ? (const char *)note : "");
        bf_jsonw_key(&out, "details_excerpt"); bf_jsonw_str(&out, (const char *)sqlite3_column_text(s, 11));
        bf_jsonw_key(&out, "run_count"); bf_jsonw_int(&out, runs);
        bf_jsonw_key(&out, "open_runs"); bf_jsonw_int(&out, sqlite3_column_int64(s, 9));
        bf_jsonw_key(&out, "candidate_for_exact_review"); bf_jsonw_bool(&out, candidate);
        bf_jsonw_key(&out, "native_archive_review_candidate"); bf_jsonw_bool(&out, archive_review);
        bf_jsonw_obj_end(&out);
        returned++; if (candidate) eligible++;
        if (archive_review) native_archive++;
    }
    bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "returned"); bf_jsonw_int(&out, returned);
    bf_jsonw_key(&out, "has_more"); bf_jsonw_bool(&out, (long long)offset + returned < total);
    bf_jsonw_key(&out, "candidates_for_exact_review"); bf_jsonw_int(&out, eligible);
    bf_jsonw_key(&out, "native_archive_review_candidates"); bf_jsonw_int(&out, native_archive);
    bf_jsonw_key(&out, "read_only"); bf_jsonw_bool(&out, 1);
    bf_jsonw_key(&out, "cannot_infer"); bf_jsonw_str(&out,
        "A zero-run nonrecurring queued goal is only a review candidate; shared titles or sources do not prove equivalence or authorize supersession.");
    bf_jsonw_obj_end(&out);
    if (step == SQLITE_DONE && !out.oom) {
        fwrite(out.buf, 1, out.len, stdout); fputc('\n', stdout); result = 0;
    } else if (step != SQLITE_DONE) result = emit_error("backlog-audit read failed");
    else result = emit_error("backlog-audit memory exhausted");
    bf_jsonw_free(&out);
    sqlite3_finalize(s); sqlite3_close(db);
    return result;
}


typedef struct {
    const char *database;
    const char *stop;
    const char *reason;
    const char *goals[100];
    size_t goal_count;
} ArchiveRequest;

typedef struct {
    const char *database;
    const char *stop;
    const char *run;
    const char *summary;
} ReconcileRequest;

static int hex_goal_id(const char *value)
{
    size_t length;
    if (!value || (length = strlen(value)) != 32) return 0;
    for (size_t i = 0; i < length; i++) {
        char ch = value[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) return 0;
    }
    return 1;
}

static int archive_request(int argc, char **argv, ArchiveRequest *out)
{
    memset(out, 0, sizeof(*out));
    for (int i = 2; i + 1 < argc; i += 2) {
        const char *key = argv[i];
        const char *value = argv[i + 1];
        if (!*value) return 0;
        if (!strcmp(key, "--db")) {
            if (out->database) return 0;
            out->database = value;
        } else if (!strcmp(key, "--stop")) {
            if (out->stop) return 0;
            out->stop = value;
        } else if (!strcmp(key, "--reason")) {
            if (out->reason || strlen(value) > 500) return 0;
            out->reason = value;
        } else if (!strcmp(key, "--goal")) {
            if (!hex_goal_id(value) || out->goal_count == sizeof(out->goals) / sizeof(out->goals[0])) return 0;
            for (size_t n = 0; n < out->goal_count; n++) if (!strcmp(out->goals[n], value)) return 0;
            out->goals[out->goal_count++] = value;
        } else return 0;
    }
    return out->database && out->stop && out->reason && out->goal_count && argc == 8 + (int)(2 * out->goal_count);
}

static int archive_stop_path(const char *database, const char *stop)
{
    char database_parent[PATH_MAX], stop_parent[PATH_MAX], resolved_database[PATH_MAX], resolved_stop[PATH_MAX];
    const char *database_slash = database ? strrchr(database, '/') : NULL;
    const char *stop_slash = stop ? strrchr(stop, '/') : NULL;
    struct stat entry;
    size_t database_length, stop_length;
    if (!database_slash || !stop_slash || strcmp(stop_slash + 1, "STOP")) return 0;
    database_length = (size_t)(database_slash - database);
    stop_length = (size_t)(stop_slash - stop);
    if (!database_length || !stop_length || database_length >= sizeof(database_parent) || stop_length >= sizeof(stop_parent)) return 0;
    memcpy(database_parent, database, database_length); database_parent[database_length] = '\0';
    memcpy(stop_parent, stop, stop_length); stop_parent[stop_length] = '\0';
    if (!realpath(database_parent, resolved_database) || !realpath(stop_parent, resolved_stop) ||
        strcmp(resolved_database, resolved_stop) || lstat(stop, &entry) != 0 ||
        !S_ISREG(entry.st_mode) || S_ISLNK(entry.st_mode)) return 0;
    return 1;
}

static int run_identifier(const char *value)
{
    size_t length;
    if (!value || !(length = strlen(value)) || length > 160) return 0;
    for (size_t i = 0; i < length; i++) {
        char ch = value[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') ||
              (ch >= 'A' && ch <= 'Z') || ch == '-' || ch == '_' || ch == '.')) return 0;
    }
    return 1;
}

static int reconcile_request(int argc, char **argv, ReconcileRequest *out)
{
    memset(out, 0, sizeof(*out));
    for (int i = 2; i + 1 < argc; i += 2) {
        const char *key = argv[i];
        const char *value = argv[i + 1];
        if (!*value) return 0;
        if (!strcmp(key, "--db")) {
            if (out->database) return 0;
            out->database = value;
        } else if (!strcmp(key, "--stop")) {
            if (out->stop) return 0;
            out->stop = value;
        } else if (!strcmp(key, "--run")) {
            if (out->run || !run_identifier(value)) return 0;
            out->run = value;
        } else if (!strcmp(key, "--summary")) {
            if (out->summary || strlen(value) > 500) return 0;
            out->summary = value;
        } else return 0;
    }
    return out->database && out->stop && out->run && out->summary && argc == 10;
}

static int reconcile_reverted_store(const ReconcileRequest *request)
{
    sqlite3 *db = NULL;
    sqlite3_stmt *run_check = NULL;
    sqlite3_stmt *action_check = NULL;
    sqlite3_stmt *run_update = NULL;
    sqlite3_stmt *goal_update = NULL;
    bf_jsonw out;
    char goal_id[128];
    char note[4096];
    char *error = NULL;
    struct stat database_entry;
    int ok = 0;
    time_t now;
    if (!safe_database_path(request->database) ||
        lstat(request->database, &database_entry) != 0 ||
        !S_ISREG(database_entry.st_mode) || S_ISLNK(database_entry.st_mode) ||
        !archive_stop_path(request->database, request->stop))
        return emit_error("reconciliation requires the regular same-directory STOP and operator database paths");
    if (bf_sqlite3_open_tier(request->database, &db, BF_SQLITE_DURABLE) != SQLITE_OK)
        return emit_error("operator store could not open at full durability");
    sqlite3_busy_timeout(db, 30000);
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, &error) != SQLITE_OK) goto done;
    sqlite3_free(error); error = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT r.goal_id,g.state,r.state,(SELECT COUNT(*) FROM runs x WHERE x.goal_id=r.goal_id AND x.completed_at IS NULL) "
        "FROM runs r JOIN goals g ON g.id=r.goal_id WHERE r.id=?", -1, &run_check, NULL) != SQLITE_OK ||
        sqlite3_bind_text(run_check, 1, request->run, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(run_check) != SQLITE_ROW) goto done;
    {
        const char *found_goal = (const char *)sqlite3_column_text(run_check, 0);
        const char *goal_state = (const char *)sqlite3_column_text(run_check, 1);
        const char *run_state = (const char *)sqlite3_column_text(run_check, 2);
        if (!found_goal || strlen(found_goal) >= sizeof(goal_id) || !goal_state || !run_state ||
            strcmp(goal_state, "queued") || strcmp(run_state, "running") || sqlite3_column_int64(run_check, 3) != 1) goto done;
        snprintf(goal_id, sizeof(goal_id), "%s", found_goal);
    }
    sqlite3_finalize(run_check); run_check = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT SUM(state='reverted'),SUM(state IN ('pending','running','in_progress')) FROM actions WHERE run_id=?", -1,
        &action_check, NULL) != SQLITE_OK || sqlite3_bind_text(action_check, 1, request->run, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(action_check) != SQLITE_ROW || sqlite3_column_int64(action_check, 0) < 1 || sqlite3_column_int64(action_check, 1) != 0) goto done;
    sqlite3_finalize(action_check); action_check = NULL;
    if (snprintf(note, sizeof(note), "reconciled as blocked after recorded reverted action: %s", request->summary) >= (int)sizeof(note)) goto done;
    now = time(NULL);
    if (sqlite3_prepare_v2(db,
        "UPDATE runs SET completed_at=?,state='blocked',summary=CASE WHEN summary='' THEN ? ELSE substr(summary,1,99000)||char(10)||? END "
        "WHERE id=? AND completed_at IS NULL AND state='running'", -1, &run_update, NULL) != SQLITE_OK ||
        sqlite3_bind_double(run_update, 1, (double)now) != SQLITE_OK ||
        sqlite3_bind_text(run_update, 2, note, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(run_update, 3, note, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(run_update, 4, request->run, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(run_update) != SQLITE_DONE || sqlite3_changes(db) != 1) goto done;
    if (sqlite3_prepare_v2(db,
        "UPDATE goals SET state='blocked',retry_at=0,last_error=CASE WHEN last_error='' THEN ? ELSE substr(last_error,1,3400)||char(10)||? END,updated_at=? "
        "WHERE id=? AND state='queued'", -1, &goal_update, NULL) != SQLITE_OK ||
        sqlite3_bind_text(goal_update, 1, note, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(goal_update, 2, note, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_double(goal_update, 3, (double)now) != SQLITE_OK ||
        sqlite3_bind_text(goal_update, 4, goal_id, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(goal_update) != SQLITE_DONE || sqlite3_changes(db) != 1) goto done;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, &error) != SQLITE_OK) goto done;
    sqlite3_free(error); error = NULL;
    ok = 1;
done:
    sqlite3_free(error);
    sqlite3_finalize(run_check);
    sqlite3_finalize(action_check);
    sqlite3_finalize(run_update);
    sqlite3_finalize(goal_update);
    if (!ok && db) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_close(db);
    if (!ok) return emit_error("reconciliation refused: require one queued goal, one running run and a recorded reverted action");
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "database");
    bf_jsonw_str(&out, request->database);
    bf_jsonw_key(&out, "forbidden_inference");
    bf_jsonw_str(&out, "A STOP-gated reconciliation records that a run stopped after a reverted action; it retains all history and is not a successful rollout or completion.");
    bf_jsonw_key(&out, "goal_id");
    bf_jsonw_str(&out, goal_id);
    bf_jsonw_key(&out, "reconciled");
    bf_jsonw_bool(&out, 1);
    bf_jsonw_key(&out, "retained_history");
    bf_jsonw_bool(&out, 1);
    bf_jsonw_key(&out, "run_id");
    bf_jsonw_str(&out, request->run);
    bf_jsonw_key(&out, "schema_version");
    bf_jsonw_str(&out, "bonfyre.operator_store.reconcile.v1");
    bf_jsonw_key(&out, "state");
    bf_jsonw_str(&out, "blocked");
    bf_jsonw_key(&out, "stopped");
    bf_jsonw_bool(&out, 1);
    bf_jsonw_obj_end(&out);
    if (!out.oom) {
        fwrite(out.buf, 1, out.len, stdout);
        fputc('\n', stdout);
    }
    bf_jsonw_free(&out);
    return 0;
}

static int archive_store(const ArchiveRequest *request)
{
    sqlite3 *db = NULL;
    sqlite3_stmt *check = NULL;
    sqlite3_stmt *update = NULL;
    bf_jsonw out;
    char note[4096];
    char *error = NULL;
    int ok = 0;
    time_t now;
    struct stat database_entry;
    if (!safe_database_path(request->database) ||
        lstat(request->database, &database_entry) != 0 ||
        !S_ISREG(database_entry.st_mode) || S_ISLNK(database_entry.st_mode) ||
        !archive_stop_path(request->database, request->stop))
        return emit_error("archive requires the regular same-directory STOP and operator database paths");
    if (bf_sqlite3_open_tier(request->database, &db, BF_SQLITE_DURABLE) != SQLITE_OK)
        return emit_error("operator store could not open at full durability");
    sqlite3_busy_timeout(db, 30000);
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, &error) != SQLITE_OK) goto done;
    sqlite3_free(error); error = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT state,recurring,(SELECT COUNT(*) FROM runs WHERE goal_id=g.id AND completed_at IS NULL) "
        "FROM goals g WHERE id=?", -1, &check, NULL) != SQLITE_OK) goto done;
    for (size_t n = 0; n < request->goal_count; n++) {
        const char *state;
        sqlite3_reset(check); sqlite3_clear_bindings(check);
        if (sqlite3_bind_text(check, 1, request->goals[n], -1, SQLITE_STATIC) != SQLITE_OK || sqlite3_step(check) != SQLITE_ROW) goto done;
        state = (const char *)sqlite3_column_text(check, 0);
        if (!state || sqlite3_column_int(check, 1) || sqlite3_column_int64(check, 2) != 0 ||
            (strcmp(state, "queued") && strcmp(state, "retry") && strcmp(state, "blocked"))) goto done;
    }
    sqlite3_finalize(check); check = NULL;
    if (snprintf(note, sizeof(note), "superseded through STOP-gated native archival: %s", request->reason) >= (int)sizeof(note)) goto done;
    if (sqlite3_prepare_v2(db,
        "UPDATE goals SET state='superseded',retry_at=0,"
        "last_error=CASE WHEN last_error='' THEN ? ELSE substr(last_error,1,3400)||char(10)||? END,updated_at=? "
        "WHERE id=? AND state IN ('queued','retry','blocked')", -1, &update, NULL) != SQLITE_OK) goto done;
    now = time(NULL);
    for (size_t n = 0; n < request->goal_count; n++) {
        sqlite3_reset(update); sqlite3_clear_bindings(update);
        if (sqlite3_bind_text(update, 1, note, -1, SQLITE_STATIC) != SQLITE_OK ||
            sqlite3_bind_text(update, 2, note, -1, SQLITE_STATIC) != SQLITE_OK ||
            sqlite3_bind_double(update, 3, (double)now) != SQLITE_OK ||
            sqlite3_bind_text(update, 4, request->goals[n], -1, SQLITE_STATIC) != SQLITE_OK ||
            sqlite3_step(update) != SQLITE_DONE || sqlite3_changes(db) != 1) goto done;
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, &error) != SQLITE_OK) goto done;
    sqlite3_free(error); error = NULL;
    ok = 1;
done:
    sqlite3_free(error);
    sqlite3_finalize(check);
    sqlite3_finalize(update);
    if (!ok && db) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_close(db);
    if (!ok) return emit_error("archive refused: require exact non-recurring queued, retry, or blocked goals with no open run");
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "archived");
    bf_jsonw_int(&out, (int64_t)request->goal_count);
    bf_jsonw_key(&out, "database");
    bf_jsonw_str(&out, request->database);
    bf_jsonw_key(&out, "forbidden_inference");
    bf_jsonw_str(&out, "A STOP-gated archival removes exact inactive goals from scheduling while retaining their rows, runs, actions, receipts, evidence, and external state; it is not completion.");
    bf_jsonw_key(&out, "goal_ids");
    bf_jsonw_arr(&out);
    for (size_t n = 0; n < request->goal_count; n++) bf_jsonw_str(&out, request->goals[n]);
    bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "retained_history");
    bf_jsonw_bool(&out, 1);
    bf_jsonw_key(&out, "schema_version");
    bf_jsonw_str(&out, "bonfyre.operator_store.archive.v1");
    bf_jsonw_key(&out, "stopped");
    bf_jsonw_bool(&out, 1);
    bf_jsonw_obj_end(&out);
    if (!out.oom) {
        fwrite(out.buf, 1, out.len, stdout);
        fputc('\n', stdout);
    }
    bf_jsonw_free(&out);
    return 0;
}

/*
 * route: which tier may carry one queued goal, read from what is true now.
 *
 * Cheapest first. A deterministic read goes to a native owner and never to a
 * model. Anything else may use a local model only when that exact checkpoint
 * has a passing quality record for the task class and is resident, leased or
 * fits the memory spendable above the floor; then an agent executor the
 * policy admits by name while Fabric accepts tasks and the vendor is not
 * quarantined; then the paid provider under the operator's own admission
 * rules. Otherwise the goal is held, with the reason for every tier.
 *
 * The command only reads. It does not take a lease, load a model, call a
 * provider or touch the queue, and STOP holds every dispatch.
 */
#define ROUTE_MAX_INPUT (4 * 1024 * 1024)

typedef struct {
    const char *database, *stop, *goal, *task_class, *policy;
    const char *residency, *memory, *lease, *quality, *fabric, *paid_model;
    double now;
} RouteRequest;

typedef struct {
    const char *name;
    const char *path;
    char *text;
    char sha256[65];
    int state; /* 0 not given, 1 valid JSON, -1 given but unreadable or invalid */
} RouteInput;

static int route_request(int argc, char **argv, RouteRequest *out)
{
    memset(out, 0, sizeof(*out));
    out->now = (double)time(NULL);
    out->paid_model = "gpt-5";
    if ((argc - 2) % 2) return 0;
    for (int i = 2; i + 1 < argc; i += 2) {
        const char *key = argv[i], *value = argv[i + 1];
        const char **slot = NULL;
        if (!*value) return 0;
        if (!strcmp(key, "--db")) slot = &out->database;
        else if (!strcmp(key, "--stop")) slot = &out->stop;
        else if (!strcmp(key, "--goal")) slot = &out->goal;
        else if (!strcmp(key, "--class")) slot = &out->task_class;
        else if (!strcmp(key, "--policy")) slot = &out->policy;
        else if (!strcmp(key, "--residency")) slot = &out->residency;
        else if (!strcmp(key, "--memory")) slot = &out->memory;
        else if (!strcmp(key, "--lease")) slot = &out->lease;
        else if (!strcmp(key, "--quality")) slot = &out->quality;
        else if (!strcmp(key, "--fabric")) slot = &out->fabric;
        else if (!strcmp(key, "--paid-model")) { out->paid_model = value; continue; }
        else if (!strcmp(key, "--at")) {
            char *end = NULL;
            out->now = strtod(value, &end);
            if (!end || *end || out->now <= 0) return 0;
            continue;
        } else return 0;
        if (*slot) return 0;
        *slot = value;
    }
    return out->database && out->stop && hex_goal_id(out->goal) && out->task_class && out->policy;
}

static void route_input_load(sqlite3 *mem, RouteInput *input)
{
    struct stat entry;
    FILE *in;
    sqlite3_stmt *valid = NULL;
    size_t got;
    input->state = 0;
    input->text = NULL;
    input->sha256[0] = '\0';
    if (!input->path) return;
    input->state = -1;
    if (lstat(input->path, &entry) != 0 || !S_ISREG(entry.st_mode) || entry.st_size <= 0 || entry.st_size > ROUTE_MAX_INPUT) return;
    if (!(in = fopen(input->path, "rb"))) return;
    input->text = malloc((size_t)entry.st_size + 1);
    got = input->text ? fread(input->text, 1, (size_t)entry.st_size, in) : 0;
    fclose(in);
    if (!input->text || got != (size_t)entry.st_size) { free(input->text); input->text = NULL; return; }
    input->text[got] = '\0';
    bf_sha256_hex((const uint8_t *)input->text, got, input->sha256);
    if (sqlite3_prepare_v2(mem, "SELECT json_valid(?1) AND json_type(?1)='object'", -1, &valid, NULL) == SQLITE_OK &&
        sqlite3_bind_text(valid, 1, input->text, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(valid) == SQLITE_ROW && sqlite3_column_int(valid, 0)) input->state = 1;
    sqlite3_finalize(valid);
}

/* One scalar from a JSON document; NULL when absent. Caller frees. */
static char *route_json_text(sqlite3 *mem, const char *doc, const char *path)
{
    sqlite3_stmt *s = NULL;
    char *value = NULL;
    if (!doc) return NULL;
    if (sqlite3_prepare_v2(mem, "SELECT json_extract(?1,?2)", -1, &s, NULL) == SQLITE_OK &&
        sqlite3_bind_text(s, 1, doc, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) {
        const char *text = (const char *)sqlite3_column_text(s, 0);
        if (text) value = strdup(text);
    }
    sqlite3_finalize(s);
    return value;
}

static int64_t route_json_int(sqlite3 *mem, const char *doc, const char *path, int64_t fallback)
{
    sqlite3_stmt *s = NULL;
    int64_t value = fallback;
    if (!doc) return fallback;
    if (sqlite3_prepare_v2(mem, "SELECT json_extract(?1,?2),json_type(?1,?2)", -1, &s, NULL) == SQLITE_OK &&
        sqlite3_bind_text(s, 1, doc, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 1) != SQLITE_NULL) {
        const char *type = (const char *)sqlite3_column_text(s, 1);
        if (type && (!strcmp(type, "integer") || !strcmp(type, "true") || !strcmp(type, "false")))
            value = sqlite3_column_int64(s, 0);
    }
    sqlite3_finalize(s);
    return value;
}

static int route_json_has(sqlite3 *mem, const char *doc, const char *path, const char *member)
{
    sqlite3_stmt *s = NULL;
    int found = 0;
    if (!doc) return 0;
    if (sqlite3_prepare_v2(mem, "SELECT EXISTS(SELECT 1 FROM json_each(?1,?2) WHERE type='text' AND value=?3)", -1, &s, NULL) == SQLITE_OK &&
        sqlite3_bind_text(s, 1, doc, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(s, 2, path, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(s, 3, member, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(s) == SQLITE_ROW) found = sqlite3_column_int(s, 0);
    sqlite3_finalize(s);
    return found;
}

static const char *route_base(const char *path)
{
    const char *slash = path ? strrchr(path, '/') : NULL;
    return slash ? slash + 1 : (path ? path : "");
}

/* Quality is keyed by the exact checkpoint file name and task class. One
 * failed record vetoes; a record for another checkpoint never transfers. */
static const char *route_quality(sqlite3 *mem, const RouteInput *quality, const char *model, const char *task_class)
{
    sqlite3_stmt *s = NULL;
    const char *verdict = "quality_unmeasured";
    if (quality->state != 1) return verdict;
    if (sqlite3_prepare_v2(mem,
        "SELECT COALESCE(SUM(json_extract(value,'$.passed')=1 AND length(json_extract(value,'$.witness'))>0),0),"
        "COALESCE(SUM(json_type(value,'$.passed')='false'),0) FROM json_each(?1,'$.records') "
        "WHERE json_extract(value,'$.model')=?2 AND json_extract(value,'$.class')=?3", -1, &s, NULL) == SQLITE_OK &&
        sqlite3_bind_text(s, 1, quality->text, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(s, 2, model, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_text(s, 3, task_class, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(s) == SQLITE_ROW) {
        if (sqlite3_column_int64(s, 1) > 0) verdict = "quality_failed";
        else if (sqlite3_column_int64(s, 0) > 0) verdict = "quality_passed";
    }
    sqlite3_finalize(s);
    return verdict;
}

/* Mirrors the Bun owner's providerStatus(): no row and a historical terminal
 * quota failure in the queue reads as quarantined; an expired cooldown needs
 * a human-verified recheck, never silent reentry. */
static const char *route_provider_state(sqlite3 *db, const char *provider, double now)
{
    sqlite3_stmt *s = NULL;
    const char *state = "unverified";
    static char held[64];
    if (sqlite3_prepare_v2(db, "SELECT state,retry_at FROM operator_provider_health WHERE provider=?", -1, &s, NULL) == SQLITE_OK &&
        sqlite3_bind_text(s, 1, provider, -1, SQLITE_STATIC) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW) {
        double retry = sqlite3_column_double(s, 1);
        const char *recorded = (const char *)sqlite3_column_text(s, 0);
        snprintf(held, sizeof held, "%s", retry > 0 && retry <= now ? "recheck_required" : (recorded ? recorded : "unknown"));
        sqlite3_finalize(s);
        return held;
    }
    sqlite3_finalize(s);
    s = NULL;
    if (!strcmp(provider, "openai") && sqlite3_prepare_v2(db,
        "SELECT 1 FROM goals WHERE state IN ('blocked','retry') AND (last_error LIKE '%credit_balance_exhausted%' "
        "OR last_error LIKE '%insufficient_quota%' OR last_error LIKE '%billing_hard_limit%' OR last_error LIKE '%invalid_api_key%') LIMIT 1",
        -1, &s, NULL) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW) state = "quarantined";
    sqlite3_finalize(s);
    return state;
}

typedef struct {
    char model[160];
    int64_t min_bytes;
    const char *verdict;
} RouteCandidate;

typedef struct {
    const char *tier, *verdict;
    char reason[160];
    char model[160];
} RouteTier;

typedef struct {
    const RouteRequest *request;
    const RouteInput *inputs;
    size_t input_count;
    const char *goal_state, *goal_digest, *tier, *reason, *dispatch, *model;
    int stopped;
    RouteTier tiers[3];
    int tier_count;
    RouteCandidate candidates[32];
    int candidate_count;
    int64_t spendable;
} RouteDecision;

static void route_set(RouteTier *t, const char *tier, const char *verdict, const char *reason, const char *model)
{
    t->tier = tier;
    t->verdict = verdict;
    snprintf(t->reason, sizeof t->reason, "%s", reason ? reason : "");
    snprintf(t->model, sizeof t->model, "%s", model ? model : "");
}

static void route_emit(bf_jsonw *w, const RouteDecision *d, const char *witness)
{
    const char *tier = d->tier;
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "class"); bf_jsonw_str(w, d->request->task_class);
    bf_jsonw_key(w, "cost");
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "basis");
    if (!strcmp(tier, "native_owner")) bf_jsonw_str(w, "no provider request is made");
    else if (!strcmp(tier, "local_model")) bf_jsonw_str(w, "no provider request is made; the local cost is a memory grant the residency owner holds");
    else if (!strcmp(tier, "hold")) bf_jsonw_str(w, "nothing is dispatched");
    else if (!strcmp(tier, "agent_relay")) bf_jsonw_str(w, "vendor agent allowance; not metered by this decision and never estimated");
    else bf_jsonw_str(w, "known only after the response, from BonfyreMeter; never estimated here");
    bf_jsonw_key(w, "provider_spend_usd");
    if (!strcmp(tier, "agent_relay") || !strcmp(tier, "paid_provider")) bf_jsonw_null(w);
    else bf_jsonw_str(w, "0");
    bf_jsonw_obj_end(w);
    bf_jsonw_key(w, "dispatch"); bf_jsonw_str(w, d->dispatch);
    bf_jsonw_key(w, "forbidden_inference");
    bf_jsonw_str(w, "A route names the cheapest tier admissible from these readings at this moment; it is not a lease, a memory "
                 "reservation, vendor credit, spending authority, a quality result or permission to run while STOP is present.");
    bf_jsonw_key(w, "goal_id"); bf_jsonw_str(w, d->request->goal);
    bf_jsonw_key(w, "goal_sha256"); bf_jsonw_str(w, d->goal_digest);
    bf_jsonw_key(w, "goal_state"); bf_jsonw_str(w, d->goal_state);
    bf_jsonw_key(w, "inputs");
    bf_jsonw_obj(w);
    for (size_t i = 0; i < d->input_count; i++) {
        bf_jsonw_key(w, d->inputs[i].name);
        if (d->inputs[i].state == 0) bf_jsonw_null(w);
        else if (d->inputs[i].state == 1) bf_jsonw_str(w, d->inputs[i].sha256);
        else bf_jsonw_str(w, "invalid");
    }
    bf_jsonw_obj_end(w);
    bf_jsonw_key(w, "model");
    if (d->model && *d->model) bf_jsonw_str(w, d->model); else bf_jsonw_null(w);
    bf_jsonw_key(w, "reason"); bf_jsonw_str(w, d->reason ? d->reason : "");
    bf_jsonw_key(w, "routed_at"); bf_jsonw_num(w, d->request->now);
    bf_jsonw_key(w, "schema_version"); bf_jsonw_str(w, "bonfyre.operator_store.route.v1");
    bf_jsonw_key(w, "stopped"); bf_jsonw_bool(w, d->stopped);
    bf_jsonw_key(w, "tier"); bf_jsonw_str(w, tier);
    bf_jsonw_key(w, "tiers");
    bf_jsonw_arr(w);
    for (int i = 0; i < d->tier_count; i++) {
        const RouteTier *t = &d->tiers[i];
        bf_jsonw_obj(w);
        if (!strcmp(t->tier, "local_model")) {
            bf_jsonw_key(w, "candidates");
            bf_jsonw_arr(w);
            for (int c = 0; c < d->candidate_count; c++) {
                bf_jsonw_obj(w);
                bf_jsonw_key(w, "min_bytes"); bf_jsonw_int(w, d->candidates[c].min_bytes);
                bf_jsonw_key(w, "model"); bf_jsonw_str(w, d->candidates[c].model);
                bf_jsonw_key(w, "verdict"); bf_jsonw_str(w, d->candidates[c].verdict);
                bf_jsonw_obj_end(w);
            }
            bf_jsonw_arr_end(w);
        }
        bf_jsonw_key(w, "model");
        if (t->model[0]) bf_jsonw_str(w, t->model); else bf_jsonw_null(w);
        bf_jsonw_key(w, "reason"); bf_jsonw_str(w, t->reason);
        if (!strcmp(t->tier, "local_model")) {
            bf_jsonw_key(w, "spendable_bytes");
            if (d->spendable >= 0) bf_jsonw_int(w, d->spendable); else bf_jsonw_null(w);
        }
        bf_jsonw_key(w, "tier"); bf_jsonw_str(w, t->tier);
        bf_jsonw_key(w, "verdict"); bf_jsonw_str(w, t->verdict);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
    if (witness) { bf_jsonw_key(w, "witness_sha256"); bf_jsonw_str(w, witness); }
    bf_jsonw_obj_end(w);
}

/* Local tier: the strongest checkpoint (residency order) that is usable, has
 * a passing quality record for this exact class and file, and is leased,
 * resident or fits what is spendable above the memory floor. */
static void route_local(sqlite3 *mem, const RouteRequest *request, const RouteInput *residency, const RouteInput *memory,
                        const RouteInput *lease, const RouteInput *quality, RouteDecision *d, RouteTier *t)
{
    char *resident_path = NULL, *leased_path = NULL, *schema = NULL;
    char reason[96] = "no_chat_checkpoint", chosen[160] = "";
    sqlite3_stmt *each = NULL;
    int64_t available, floor_bytes;
    d->spendable = -1;
    if (residency->state != 1 || !(schema = route_json_text(mem, residency->text, "$.schema"))) {
        route_set(t, "local_model", "unavailable", residency->state == 0 ? "local_backend_unavailable" : "residency_input_invalid", NULL);
        return;
    }
    free(schema);
    if (lease->state == -1) { route_set(t, "local_model", "unavailable", "lease_input_invalid", NULL); return; }
    if (lease->state == 1) {
        if (route_json_int(mem, lease->text, "$.lease", 0) <= 0 || !(leased_path = route_json_text(mem, lease->text, "$.model_path"))) {
            route_set(t, "local_model", "unavailable", "lease_input_invalid", NULL);
            return;
        }
    }
    if (route_json_int(mem, residency->text, "$.resident", 0) == 1) resident_path = route_json_text(mem, residency->text, "$.model");
    available = route_json_int(mem, memory->text, "$.available_bytes", -1);
    floor_bytes = route_json_int(mem, memory->text, "$.protected_floor_bytes", -1);
    if (memory->state == 1 && available >= 0 && floor_bytes >= 0) {
        /* Memory already granted to other work on the ledger is not spendable,
         * or the router would say a checkpoint fits that the residency owner
         * then defers. Callers carry the open total as open_grant_bytes. */
        int64_t granted = route_json_int(mem, memory->text, "$.open_grant_bytes", 0);
        d->spendable = available - floor_bytes - (granted > 0 ? granted : 0);
    }
    if (sqlite3_prepare_v2(mem,
        "SELECT json_extract(value,'$.model'),json_extract(value,'$.usable'),json_extract(value,'$.min_bytes') "
        "FROM json_each(?1,'$.candidates') ORDER BY CAST(key AS INTEGER)", -1, &each, NULL) == SQLITE_OK &&
        sqlite3_bind_text(each, 1, residency->text, -1, SQLITE_STATIC) == SQLITE_OK) {
        while (sqlite3_step(each) == SQLITE_ROW && d->candidate_count < 32) {
            const char *model = (const char *)sqlite3_column_text(each, 0);
            RouteCandidate *c;
            const char *verdict;
            int64_t need = sqlite3_column_int64(each, 2);
            if (!model || !*model) continue;
            if (leased_path && strcmp(model, route_base(leased_path))) verdict = "not_leased";
            else if (sqlite3_column_int(each, 1) != 1) verdict = "model_unsupported";
            else if (strcmp((verdict = route_quality(mem, quality, model, request->task_class)), "quality_passed")) {}
            else if (chosen[0]) verdict = "weaker_than_selected";
            else if (leased_path) verdict = "selected_leased";
            else if (resident_path && !strcmp(model, route_base(resident_path))) verdict = "selected_resident";
            else if (d->spendable < 0) verdict = "resource_unmeasured";
            else if (need <= 0 || need > d->spendable) verdict = "resource_unavailable";
            else verdict = "selected_fits_memory";
            if (!strncmp(verdict, "selected", 8)) snprintf(chosen, sizeof chosen, "%s", model);
            else if (!chosen[0] && strcmp(verdict, "not_leased")) snprintf(reason, sizeof reason, "%s", verdict);
            c = &d->candidates[d->candidate_count++];
            snprintf(c->model, sizeof c->model, "%s", model);
            c->min_bytes = need;
            c->verdict = verdict;
        }
    }
    sqlite3_finalize(each);
    if (leased_path && !chosen[0] && !strcmp(reason, "no_chat_checkpoint")) snprintf(reason, sizeof reason, "leased_model_not_a_candidate");
    if (chosen[0])
        route_set(t, "local_model", "selected", leased_path ? "the leased checkpoint has a passing quality record for this class"
                  : "a checkpoint with a passing quality record for this class is resident or fits; lease it by name before generating", chosen);
    else route_set(t, "local_model", "unavailable", reason, NULL);
    if (chosen[0]) d->dispatch = leased_path ? "admitted" : "lease_required";
    free(resident_path);
    free(leased_path);
}

static int route_goal(const RouteRequest *request)
{
    sqlite3 *db = NULL, *mem = NULL;
    sqlite3_stmt *goal = NULL;
    RouteInput inputs[6] = {
        {"fabric", request->fabric, NULL, "", 0}, {"lease", request->lease, NULL, "", 0},
        {"memory", request->memory, NULL, "", 0}, {"policy", request->policy, NULL, "", 0},
        {"quality", request->quality, NULL, "", 0}, {"residency", request->residency, NULL, "", 0}};
    const RouteInput *fabric = &inputs[0], *lease = &inputs[1], *memory = &inputs[2], *policy = &inputs[3],
                     *quality = &inputs[4], *residency = &inputs[5];
    RouteDecision d;
    bf_jsonw out;
    struct stat entry;
    char goal_state[32] = "", goal_digest[65] = "", witness[65], gate[64] = "";
    int deterministic, rc = 2;
    int64_t attempts, open_runs, uncertain, max_attempts;
    double retry_at;

    memset(&d, 0, sizeof d);
    deterministic = !strcmp(request->task_class, "deterministic_read");
    if (!deterministic && strcmp(request->task_class, "short_analysis") && strcmp(request->task_class, "code_review") &&
        strcmp(request->task_class, "workflow"))
        return emit_error("route --class must be deterministic_read, short_analysis, code_review, or workflow");
    if (!safe_database_path(request->database) || lstat(request->database, &entry) != 0 || !S_ISREG(entry.st_mode))
        return emit_error("operator store requires a regular database path under an existing local directory");
    if (sqlite3_open(":memory:", &mem) != SQLITE_OK) { sqlite3_close(mem); return emit_error("route could not open its JSON reader"); }
    for (size_t i = 0; i < sizeof inputs / sizeof inputs[0]; i++) route_input_load(mem, &inputs[i]);
    if (policy->state != 1) { rc = emit_error("route --policy must be a readable JSON object"); goto done; }
    if (bf_sqlite3_open_ro(request->database, &db) != SQLITE_OK) { rc = emit_error("operator store could not open read-only"); goto done; }
    sqlite3_busy_timeout(db, 3000);
    if (sqlite3_prepare_v2(db,
        "SELECT g.state,g.attempts,g.retry_at,g.updated_at,"
        "(SELECT COUNT(*) FROM runs r WHERE r.goal_id=g.id AND r.completed_at IS NULL),"
        "(SELECT COUNT(*) FROM actions a JOIN runs r ON r.id=a.run_id WHERE r.goal_id=g.id AND "
        "a.state IN ('started','outcome_uncertain','pending','running','in_progress'))"
        " FROM goals g WHERE g.id=?", -1, &goal, NULL) != SQLITE_OK ||
        sqlite3_bind_text(goal, 1, request->goal, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(goal) != SQLITE_ROW) { rc = emit_error("route requires an existing goal in a store of the declared schema"); goto done; }
    snprintf(goal_state, sizeof goal_state, "%s", sqlite3_column_text(goal, 0) ? (const char *)sqlite3_column_text(goal, 0) : "");
    attempts = sqlite3_column_int64(goal, 1);
    retry_at = sqlite3_column_double(goal, 2);
    open_runs = sqlite3_column_int64(goal, 4);
    uncertain = sqlite3_column_int64(goal, 5);
    {
        char row[256];
        int n = snprintf(row, sizeof row, "%s|%s|%" PRId64 "|%.6f|%.6f", request->goal, goal_state, attempts, retry_at,
                         sqlite3_column_double(goal, 3));
        bf_sha256_hex((const uint8_t *)row, (size_t)(n < (int)sizeof row ? n : (int)sizeof row - 1), goal_digest);
    }
    max_attempts = route_json_int(mem, policy->text, "$.max_goal_attempts", 6);

    d.request = request;
    d.inputs = inputs;
    d.input_count = sizeof inputs / sizeof inputs[0];
    d.goal_state = goal_state;
    d.goal_digest = goal_digest;
    d.stopped = lstat(request->stop, &entry) == 0;
    d.tier = "hold";
    d.dispatch = "refused";
    d.spendable = -1;

    if (strcmp(goal_state, "queued") && strcmp(goal_state, "retry")) snprintf(gate, sizeof gate, "goal_not_schedulable");
    else if (uncertain > 0) snprintf(gate, sizeof gate, "uncertain_effect_requires_reconciliation");
    else if (open_runs > 0) snprintf(gate, sizeof gate, "open_run_requires_reconciliation");
    else if (attempts >= max_attempts) snprintf(gate, sizeof gate, "attempts_exhausted");
    else if (retry_at > request->now) snprintf(gate, sizeof gate, "not_due");

    if (gate[0]) {
        d.reason = gate;
    } else if (deterministic) {
        route_set(&d.tiers[d.tier_count++], "native_owner", "selected", "deterministic class", NULL);
        d.tier = "native_owner";
        d.reason = "deterministic reads, retrieval and context compilation go to native owners regardless of model readiness";
        d.dispatch = "admitted";
    } else {
        static const char *vendors[] = {"claude", "codex"};
        RouteTier *local = &d.tiers[d.tier_count++], *relay = &d.tiers[d.tier_count++], *paid = &d.tiers[d.tier_count++];
        const char *agent = NULL, *health, *paid_reason = NULL;
        char vendor_reason[96];
        route_local(mem, request, residency, memory, lease, quality, &d, local);

        if (fabric->state != 1) snprintf(vendor_reason, sizeof vendor_reason, "%s", fabric->state ? "fabric_input_invalid" : "fabric_status_unavailable");
        else if (route_json_int(mem, fabric->text, "$.admission.accepting_tasks", 0) != 1)
            snprintf(vendor_reason, sizeof vendor_reason, "fabric_admission_paused");
        else {
            snprintf(vendor_reason, sizeof vendor_reason, "no_agent_executor_admitted_by_policy");
            for (size_t v = 0; v < 2 && !agent; v++) {
                char path[64];
                if (!route_json_has(mem, policy->text, "$.approved_agent_executors", vendors[v])) continue;
                snprintf(path, sizeof path, "$.%s.installed", vendors[v]);
                if (route_json_int(mem, fabric->text, path, 0) != 1) { snprintf(vendor_reason, sizeof vendor_reason, "%s_not_installed", vendors[v]); continue; }
                health = route_provider_state(db, vendors[v], request->now);
                if (strcmp(health, "unverified")) { snprintf(vendor_reason, sizeof vendor_reason, "%s_%s", vendors[v], health); continue; }
                agent = vendors[v];
            }
        }
        health = route_provider_state(db, "openai", request->now);
        if (route_json_int(mem, policy->text, "$.allow_provider_requests", 0) != 1) paid_reason = "provider_requests_not_allowed_by_policy";
        else if (!route_json_has(mem, policy->text, "$.approved_provider_models", request->paid_model)) paid_reason = "model_not_approved_by_policy";
        else if (strcmp(health, "unverified")) paid_reason = !strcmp(health, "quarantined") ? "provider_quarantined" : "provider_recheck_required";

        if (!strcmp(local->verdict, "selected")) {
            d.tier = "local_model"; d.reason = local->reason; d.model = local->model;
        } else if (agent) {
            d.tier = "agent_relay"; d.model = agent; d.dispatch = "admitted";
            d.reason = "the policy admits this executor by name, Fabric accepts tasks and the vendor is not quarantined";
        } else if (!paid_reason) {
            d.tier = "paid_provider"; d.model = request->paid_model; d.dispatch = "admitted";
            d.reason = "operator policy admits this model and the provider is not quarantined";
        } else d.reason = "no tier is available now; the goal stays queued";

        if (!strcmp(d.tier, "agent_relay")) route_set(relay, "agent_relay", "selected", d.reason, agent);
        else if (agent) route_set(relay, "agent_relay", "eligible_not_needed", "a cheaper tier was selected", agent);
        else route_set(relay, "agent_relay", "unavailable", vendor_reason, NULL);
        if (!strcmp(d.tier, "paid_provider")) route_set(paid, "paid_provider", "selected", d.reason, request->paid_model);
        else route_set(paid, "paid_provider", paid_reason ? "unavailable" : "eligible_not_needed",
                       paid_reason ? paid_reason : "a cheaper tier was selected", request->paid_model);
    }
    if (d.stopped && strcmp(d.dispatch, "refused")) d.dispatch = "held_by_stop";

    bf_jsonw_init(&out, 0);
    route_emit(&out, &d, NULL);
    if (out.oom) { bf_jsonw_free(&out); rc = emit_error("route output exhausted memory"); goto done; }
    bf_sha256_hex((const uint8_t *)out.buf, out.len, witness);
    bf_jsonw_reset(&out);
    route_emit(&out, &d, witness);
    if (!out.oom) {
        fwrite(out.buf, 1, out.len, stdout);
        fputc('\n', stdout);
        rc = 0;
    }
    bf_jsonw_free(&out);
done:
    sqlite3_finalize(goal);
    sqlite3_close(db);
    sqlite3_close(mem);
    for (size_t i = 0; i < sizeof inputs / sizeof inputs[0]; i++) free(inputs[i].text);
    return rc;
}

/*
 * quality-record: the one writer of the quality file route --quality reads.
 *
 * The caller names an evidence file of deterministically scored items; this
 * owner recomputes the score and the verdict from those items and the
 * threshold the evaluation set declared, so a caller cannot assert a pass.
 * The witness is the SHA-256 of the evidence bytes. A record is appended,
 * never edited: a later failure vetoes an earlier pass in the router, and the
 * same evidence twice is a replay, not a second record.
 */
#define QUALITY_SCHEMA "bonfyre.operator_store.quality.v1"
#define QUALITY_EVIDENCE_SCHEMA "bonfyre.operator_store.quality_evidence.v1"

static int quality_write_file(const char *path, const char *text, size_t len)
{
    char tmp[PATH_MAX];
    FILE *out;
    int ok;
    if (snprintf(tmp, sizeof tmp, "%s.tmp.%ld", path, (long)getpid()) >= (int)sizeof tmp) return 0;
    if (!(out = fopen(tmp, "wx"))) return 0;
    ok = fwrite(text, 1, len, out) == len && fputc('\n', out) != EOF && fflush(out) == 0 && fsync(fileno(out)) == 0;
    if (fclose(out) != 0) ok = 0;
    if (!ok || rename(tmp, path) != 0) { unlink(tmp); return 0; }
    return 1;
}

static int quality_hex64(const char *s)
{
    if (!s || strlen(s) != 64) return 0;
    for (const char *p = s; *p; p++) if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f'))) return 0;
    return 1;
}

static int quality_record(int argc, char **argv)
{
    const char *file = NULL, *evidence_path = NULL, *message = NULL;
    double now = (double)time(NULL), threshold = -1, score;
    sqlite3 *mem = NULL;
    sqlite3_stmt *s = NULL;
    RouteInput evidence = {"evidence", NULL, NULL, "", 0}, quality = {"quality", NULL, NULL, "", 0};
    char *model = NULL, *task_class = NULL, *eval_set = NULL;
    const char *doc = NULL;
    char witness[72], resolved[PATH_MAX], file_digest[65];
    int64_t items = 0, items_passed = 0, invalid = -1, measured = 0, replay = 0;
    int passed, rc = 2, have_file;
    bf_jsonw rec, out;
    struct stat entry;

    memset(&rec, 0, sizeof rec);
    memset(&out, 0, sizeof out);
    if ((argc - 2) % 2) return emit_error("use quality-record --file QUALITY.json --evidence EVIDENCE.json [--at EPOCH]");
    for (int i = 2; i + 1 < argc; i += 2) {
        if (!argv[i + 1][0]) file = evidence_path = NULL, i = argc;
        else if (!strcmp(argv[i], "--file") && !file) file = argv[i + 1];
        else if (!strcmp(argv[i], "--evidence") && !evidence_path) evidence_path = argv[i + 1];
        else if (!strcmp(argv[i], "--at")) {
            char *end = NULL;
            now = strtod(argv[i + 1], &end);
            if (!end || *end || now <= 0) file = evidence_path = NULL, i = argc;
        } else file = evidence_path = NULL, i = argc;
    }
    if (!file || !evidence_path) return emit_error("use quality-record --file QUALITY.json --evidence EVIDENCE.json [--at EPOCH]");
    if (!safe_database_path(file)) return emit_error("quality file must be a regular path under an existing local directory");
    if (sqlite3_open(":memory:", &mem) != SQLITE_OK) { sqlite3_close(mem); return emit_error("quality-record could not open its JSON reader"); }

    evidence.path = evidence_path;
    route_input_load(mem, &evidence);
    if (evidence.state != 1 || !realpath(evidence_path, resolved)) { message = "evidence must be a regular JSON object file"; goto done; }
    if (sqlite3_prepare_v2(mem,
        "SELECT json_extract(?1,'$.schema')=?2, json_extract(?1,'$.model'), json_extract(?1,'$.class'),"
        " json_extract(?1,'$.eval_set_sha256'), CASE WHEN json_type(?1,'$.threshold') IN ('real','integer') THEN json_extract(?1,'$.threshold') END,"
        " json_type(?1,'$.measured')='true',"
        " (SELECT COUNT(*) FROM json_each(?1,'$.items')),"
        " (SELECT COUNT(*) FROM json_each(?1,'$.items') WHERE json_type(value,'$.passed')='true'),"
        " (SELECT COUNT(*) FROM json_each(?1,'$.items') WHERE json_type(value,'$.id')<>'text' OR json_type(value,'$.id') IS NULL"
        "   OR json_type(value,'$.passed') IS NULL OR json_type(value,'$.passed') NOT IN ('true','false'))",
        -1, &s, NULL) != SQLITE_OK ||
        sqlite3_bind_text(s, 1, evidence.text, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(s, 2, QUALITY_EVIDENCE_SCHEMA, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(s) != SQLITE_ROW || sqlite3_column_int(s, 0) != 1) { message = "evidence schema must be " QUALITY_EVIDENCE_SCHEMA; goto done; }
    if (sqlite3_column_type(s, 1) == SQLITE_TEXT) model = strdup((const char *)sqlite3_column_text(s, 1));
    if (sqlite3_column_type(s, 2) == SQLITE_TEXT) task_class = strdup((const char *)sqlite3_column_text(s, 2));
    if (sqlite3_column_type(s, 3) == SQLITE_TEXT) eval_set = strdup((const char *)sqlite3_column_text(s, 3));
    if (sqlite3_column_type(s, 4) != SQLITE_NULL) threshold = sqlite3_column_double(s, 4);
    measured = sqlite3_column_int64(s, 5);
    items = sqlite3_column_int64(s, 6);
    items_passed = sqlite3_column_int64(s, 7);
    invalid = sqlite3_column_int64(s, 8);
    sqlite3_finalize(s);
    s = NULL;
    if (!model || !*model || strchr(model, '/') || strlen(model) < 6 || strcmp(model + strlen(model) - 5, ".gguf"))
        { message = "evidence model must be an exact checkpoint file name ending in .gguf"; goto done; }
    if (!task_class || (strcmp(task_class, "short_analysis") && strcmp(task_class, "code_review") && strcmp(task_class, "workflow")))
        { message = "evidence class must be short_analysis, code_review, or workflow"; goto done; }
    if (!quality_hex64(eval_set)) { message = "evidence eval_set_sha256 must be 64 lower-case hexadecimal digits"; goto done; }
    if (!(threshold > 0 && threshold <= 1)) { message = "evidence threshold must be a number in (0, 1]"; goto done; }
    if (measured != 1) { message = "evidence must say measured: true; estimates are not quality records"; goto done; }
    if (items < 1 || invalid != 0) { message = "evidence items must be non-empty, each with a text id and a boolean passed"; goto done; }

    score = (double)items_passed / (double)items;
    passed = score >= threshold;
    snprintf(witness, sizeof witness, "sha256:%s", evidence.sha256);

    have_file = lstat(file, &entry) == 0;
    if (have_file) {
        quality.path = file;
        route_input_load(mem, &quality);
        if (quality.state != 1) { message = "existing quality file is not a readable JSON object"; goto done; }
        doc = quality.text;
    } else doc = "{\"schema\":\"" QUALITY_SCHEMA "\",\"records\":[]}";
    if (sqlite3_prepare_v2(mem,
        "SELECT json_extract(?1,'$.schema')=?3 AND json_type(?1,'$.records')='array',"
        " (SELECT COUNT(*) FROM json_each(?1,'$.records') WHERE json_extract(value,'$.witness')=?2)", -1, &s, NULL) != SQLITE_OK ||
        sqlite3_bind_text(s, 1, doc, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(s, 2, witness, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(s, 3, QUALITY_SCHEMA, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_step(s) != SQLITE_ROW || sqlite3_column_int(s, 0) != 1) { message = "existing quality file must be schema " QUALITY_SCHEMA " with a records array"; goto done; }
    replay = sqlite3_column_int64(s, 1);
    sqlite3_finalize(s);
    s = NULL;

    bf_jsonw_init(&rec, 0);
    bf_jsonw_obj(&rec);
    bf_jsonw_key(&rec, "class"); bf_jsonw_str(&rec, task_class);
    bf_jsonw_key(&rec, "eval_set_sha256"); bf_jsonw_str(&rec, eval_set);
    bf_jsonw_key(&rec, "evidence"); bf_jsonw_str(&rec, resolved);
    bf_jsonw_key(&rec, "items"); bf_jsonw_int(&rec, items);
    bf_jsonw_key(&rec, "items_passed"); bf_jsonw_int(&rec, items_passed);
    bf_jsonw_key(&rec, "model"); bf_jsonw_str(&rec, model);
    bf_jsonw_key(&rec, "passed"); bf_jsonw_bool(&rec, passed);
    bf_jsonw_key(&rec, "recorded_at"); bf_jsonw_num(&rec, now);
    bf_jsonw_key(&rec, "score"); bf_jsonw_num(&rec, score);
    bf_jsonw_key(&rec, "threshold"); bf_jsonw_num(&rec, threshold);
    bf_jsonw_key(&rec, "witness"); bf_jsonw_str(&rec, witness);
    bf_jsonw_obj_end(&rec);
    if (rec.oom) { message = "quality record exhausted memory"; goto done; }

    if (!replay) {
        const char *next;
        if (sqlite3_prepare_v2(mem, "SELECT json_insert(?1,'$.records[#]',json(?2))", -1, &s, NULL) != SQLITE_OK ||
            sqlite3_bind_text(s, 1, doc, -1, SQLITE_STATIC) != SQLITE_OK ||
            sqlite3_bind_text(s, 2, rec.buf, (int)rec.len, SQLITE_STATIC) != SQLITE_OK ||
            sqlite3_step(s) != SQLITE_ROW || !(next = (const char *)sqlite3_column_text(s, 0))) { message = "quality record could not be appended"; goto done; }
        if (!quality_write_file(file, next, strlen(next))) { message = "quality file could not be written atomically"; goto done; }
        bf_sha256_hex((const uint8_t *)next, strlen(next), file_digest);
        sqlite3_finalize(s);
        s = NULL;
    } else bf_sha256_hex((const uint8_t *)doc, strlen(doc), file_digest);

    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "cannot_infer");
    bf_jsonw_str(&out, "A quality record says this checkpoint scored this way on this fixed evaluation set; it is not a lease, a memory "
                 "reservation, permission to dispatch, or evidence about other checkpoints, classes or task distributions.");
    bf_jsonw_key(&out, "file"); bf_jsonw_str(&out, file);
    bf_jsonw_key(&out, "file_body_sha256"); bf_jsonw_str(&out, file_digest);
    bf_jsonw_key(&out, "recorded"); bf_jsonw_bool(&out, !replay);
    bf_jsonw_key(&out, "replayed"); bf_jsonw_bool(&out, replay > 0);
    bf_jsonw_key(&out, "passed"); bf_jsonw_bool(&out, passed);
    bf_jsonw_key(&out, "score"); bf_jsonw_num(&out, score);
    bf_jsonw_key(&out, "witness"); bf_jsonw_str(&out, witness);
    bf_jsonw_key(&out, "model"); bf_jsonw_str(&out, model);
    bf_jsonw_key(&out, "class"); bf_jsonw_str(&out, task_class);
    bf_jsonw_obj_end(&out);
    if (!out.oom) { fwrite(out.buf, 1, out.len, stdout); fputc('\n', stdout); rc = 0; }
done:
    sqlite3_finalize(s);
    sqlite3_close(mem);
    if (message) rc = emit_error(message);
    bf_jsonw_free(&rec);
    bf_jsonw_free(&out);
    free(evidence.text);
    free(quality.text);
    free(model);
    free(task_class);
    free(eval_set);
    return rc;
}

int main(int argc, char **argv)
{
    const BfCommandEntry *command;
    const char *database;
    ArchiveRequest archive;
    ReconcileRequest reconcile;
    RouteRequest route;
    if (argc < 2 || !(command = bf_command_lookup(operator_store_commands,
        sizeof(operator_store_commands) / sizeof(operator_store_commands[0]), argv[1])))
        return emit_error("use archive, backlog-audit, init, inspect, quality-record, reconcile-reverted, or route");
    if (command->id == OPERATOR_STORE_ARCHIVE) {
        if (!archive_request(argc, argv, &archive))
            return emit_error("use archive --db PATH --stop PATH --reason TEXT --goal HEX_ID [...]");
        return archive_store(&archive);
    }
    if (command->id == OPERATOR_STORE_RECONCILE_REVERTED) {
        if (!reconcile_request(argc, argv, &reconcile))
            return emit_error("use reconcile-reverted --db PATH --stop PATH --run ID --summary TEXT");
        return reconcile_reverted_store(&reconcile);
    }
    if (command->id == OPERATOR_STORE_ROUTE) {
        if (!route_request(argc, argv, &route))
            return emit_error("use route --db PATH --stop PATH --goal HEX_ID --class CLASS --policy FILE [--residency FILE] "
                              "[--memory FILE] [--lease FILE] [--quality FILE] [--fabric FILE] [--paid-model NAME] [--at EPOCH]");
        return route_goal(&route);
    }
    if (command->id == OPERATOR_STORE_QUALITY_RECORD) return quality_record(argc, argv);
    if (command->id == OPERATOR_STORE_BACKLOG_AUDIT) return backlog_audit_store(argc, argv);
    if (argc != command->mode || strcmp(argv[2], command->path_option) != 0 || !argv[3][0])
        return emit_error("use init or inspect with --db PATH");
    database = argv[3];
    switch (command->id) {
    case OPERATOR_STORE_INIT:
        return initialize_store(database);
    case OPERATOR_STORE_INSPECT:
        return inspect_store(database);
    }
    return emit_error("unknown operator store command");
}
