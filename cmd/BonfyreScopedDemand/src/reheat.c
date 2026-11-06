#include <sqlite3.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct {
    char subject[256];
    char scope[256];
} Closure;

typedef struct {
    char path[PATH_MAX];
} Pending;

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 1; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static char *read_all(const char *path) {
    FILE *f = fopen(path, "rb");
    char *body = NULL;
    long n;
    if (!f || fseek(f, 0, SEEK_END) || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET)) {
        if (f) fclose(f);
        return NULL;
    }
    body = malloc((size_t)n + 1);
    if (!body || fread(body, 1, (size_t)n, f) != (size_t)n) {
        free(body);
        fclose(f);
        return NULL;
    }
    body[n] = '\0';
    fclose(f);
    return body;
}

static int one_term(sqlite3 *db, const char *identity, const char *axis, char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT term FROM candidate_advertisement_term "
        "WHERE identity=?1 AND axis=?2 ORDER BY term LIMIT 1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, identity, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, axis, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        ok = out[0] != '\0';
    }
    sqlite3_finalize(st);
    return ok;
}

static int add_closure(Closure *closures, int *count, int cap,
                       const char *subject, const char *scope) {
    if (!subject || !*subject || !scope || !*scope) return 0;
    for (int i = 0; i < *count; i++)
        if (!strcmp(closures[i].subject, subject) && !strcmp(closures[i].scope, scope)) return 1;
    if (*count >= cap) return 0;
    snprintf(closures[*count].subject, sizeof(closures[*count].subject), "%s", subject);
    snprintf(closures[*count].scope, sizeof(closures[*count].scope), "%s", scope);
    (*count)++;
    return 1;
}

static int activation_closure(sqlite3 *db, const char *identity,
                              Closure *closures, int *count, int cap) {
    char subject[256] = {0}, scope[256] = {0};
    if (!one_term(db, identity, "subject", subject, sizeof(subject)) ||
        !one_term(db, identity, "scope", scope, sizeof(scope))) return 0;
    return add_closure(closures, count, cap, subject, scope);
}

static int demand_closures(sqlite3 *db, const char *identity,
                           Closure *closures, int *count, int cap) {
    char pack[256] = {0};
    sqlite3_stmt *st = NULL;
    int ok = 1;
    if (!one_term(db, identity, "pack", pack, sizeof(pack))) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT DISTINCT s.term,c.term FROM candidate_advertisement a "
        "JOIN candidate_advertisement_term p ON p.identity=a.identity AND p.axis='pack' AND p.term=?1 "
        "JOIN candidate_advertisement_term s ON s.identity=a.identity AND s.axis='subject' "
        "JOIN candidate_advertisement_term c ON c.identity=a.identity AND c.axis='scope' "
        "WHERE a.candidate_kind='pack' ORDER BY s.term,c.term",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, pack, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *subject = (const char *)sqlite3_column_text(st, 0);
        const char *scope = (const char *)sqlite3_column_text(st, 1);
        if (!add_closure(closures, count, cap, subject, scope)) { ok = 0; break; }
    }
    sqlite3_finalize(st);
    return ok;
}

static int collect_handoff(sqlite3 *db, const char *path,
                           Closure *closures, int *closure_count, int closure_cap) {
    char *body = read_all(path);
    sqlite3 *json = NULL;
    sqlite3_stmt *st = NULL;
    int ok = 0;
    const char *schema, *identity, *kind, *lowerer;
    if (!body || sqlite3_open(":memory:", &json) != SQLITE_OK) goto out;
    if (sqlite3_prepare_v2(json,
        "SELECT json_extract(?1,'$.schema'),json_extract(?1,'$.identity'),"
        "json_extract(?1,'$.candidate_kind'),json_extract(?1,'$.lowerer_ref') "
        "WHERE json_valid(?1)", -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, body, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) goto out;
    schema = (const char *)sqlite3_column_text(st, 0);
    identity = (const char *)sqlite3_column_text(st, 1);
    kind = (const char *)sqlite3_column_text(st, 2);
    lowerer = (const char *)sqlite3_column_text(st, 3);
    if (!schema || !identity || !kind || !lowerer || strcmp(schema, "bonfyre-lowering-handoff.v1")) goto out;
    if (!strcmp(lowerer, "PackGraph:activation") && !strcmp(kind, "pack"))
        ok = activation_closure(db, identity, closures, closure_count, closure_cap);
    else if (!strcmp(lowerer, "MaterialCompiler:pack_demand") && !strcmp(kind, "pack_demand"))
        ok = demand_closures(db, identity, closures, closure_count, closure_cap);
out:
    if (st) sqlite3_finalize(st);
    if (json) sqlite3_close(json);
    free(body);
    return ok;
}

static int scan_partition(sqlite3 *db, const char *root, const char *partition,
                          Pending *pending, int *pending_count, int limit,
                          Closure *closures, int *closure_count, int closure_cap) {
    char dirpath[PATH_MAX];
    DIR *dir;
    struct dirent *entry;
    if (snprintf(dirpath, sizeof(dirpath), "%s/%s", root, partition) >= (int)sizeof(dirpath)) return 0;
    dir = opendir(dirpath);
    if (!dir) return 1;
    while (*pending_count < limit && (entry = readdir(dir)) != NULL) {
        size_t n = strlen(entry->d_name);
        Pending *item;
        if (entry->d_name[0] == '.' || n < 6 || strcmp(entry->d_name + n - 5, ".json")) continue;
        item = &pending[*pending_count];
        if (snprintf(item->path, sizeof(item->path), "%s/%s", dirpath, entry->d_name) >= (int)sizeof(item->path) ||
            !collect_handoff(db, item->path, closures, closure_count, closure_cap)) {
            closedir(dir);
            return 0;
        }
        (*pending_count)++;
    }
    closedir(dir);
    return 1;
}

static int run_resolver(const char *resolver, const char *db, const char *subject,
                        const char *scope, const char *outdir, const char *condition_db) {
    pid_t pid = fork();
    int status = 0;
    if (pid < 0) return 0;
    if (pid == 0) {
        int fd = open("/dev/null", O_WRONLY);
        if (fd >= 0) { dup2(fd, STDOUT_FILENO); close(fd); }
        if (condition_db && *condition_db)
            execl(resolver, resolver, "dispatch", "--db", db, "--condition-db", condition_db,
                  "--subject", subject, "--scope", scope, "--dir", outdir, "--limit", "4096", (char *)NULL);
        else
            execl(resolver, resolver, "dispatch", "--db", db, "--subject", subject,
                  "--scope", scope, "--dir", outdir, "--limit", "4096", (char *)NULL);
        _exit(127);
    }
    return waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(int argc, char **argv) {
    const char *dbpath = arg(argc, argv, "--db");
    const char *root = arg(argc, argv, "--lowerer-dir");
    const char *outdir = arg(argc, argv, "--dir");
    const char *resolver = arg(argc, argv, "--resolver");
    const char *condition_db = arg(argc, argv, "--condition-db");
    const char *limit_raw = arg(argc, argv, "--limit");
    int limit = limit_raw ? atoi(limit_raw) : 256;
    sqlite3 *db = NULL;
    Pending pending[4096];
    Closure closures[4096];
    int pending_count = 0, closure_count = 0;
    if (!dbpath || !root || !outdir || !resolver || limit < 1 || limit > 4096) return 2;
    if (mkdir(outdir, 0750) != 0 && access(outdir, F_OK) != 0) return 1;
    if (sqlite3_open_v2(dbpath, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 1;
    }
    if (!scan_partition(db, root, "PackGraph-activation", pending, &pending_count, limit,
                        closures, &closure_count, 4096) ||
        !scan_partition(db, root, "MaterialCompiler-pack_demand", pending, &pending_count, limit,
                        closures, &closure_count, 4096)) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_close(db);
    for (int i = 0; i < closure_count; i++)
        if (!run_resolver(resolver, dbpath, closures[i].subject, closures[i].scope, outdir, condition_db)) return 1;
    for (int i = 0; i < pending_count; i++) {
        char done[PATH_MAX];
        if (snprintf(done, sizeof(done), "%s.handled", pending[i].path) >= (int)sizeof(done) ||
            rename(pending[i].path, done) != 0) return 1;
    }
    printf("{\"schema\":\"bonfyre-scoped-demand-reheat.v1\",\"handoffs_handled\":%d,"
           "\"closures_recomputed\":%d,\"truth\":\"dependency_cut_recomputed_no_authority_or_observation\"}\n",
           pending_count, closure_count);
    return 0;
}
