/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreQuery — local analytics engine over artifacts via DuckDB.
 *
 * Query artifact manifests, transcript families, compression stats
 * like a database. Run SQL over your own pipeline output.
 *
 * Usage:
 *   bonfyre-query scan <dir>                  → import all JSON artifacts into DuckDB
 *   bonfyre-query sql <db> "<query>"          → run SQL against artifact DB
 *   bonfyre-query stats <db>                  → summary stats (counts, sizes, types)
 *   bonfyre-query family <db> <family-key>    → all members of an artifact family
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <bonfyre.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }

static void iso_ts(char *buf, size_t sz) {
    time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return -1; }
    if (pid == 0) { execvp(argv[0], (char *const *)argv); _exit(127); }
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

/* Run a row-producing command and pass its JSON through, of any size.
 *
 * The DuckDB shell prints "[{]" for a query that returns no rows, which is not
 * JSON, so every caller that parsed the answer failed exactly when the answer
 * was "nothing". No rows is written as "[]"; anything else is passed on
 * untouched as it arrives.
 */
static int write_all(const char *buf, size_t n) {
    while (n > 0) {
        ssize_t w = write(STDOUT_FILENO, buf, n);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        buf += w; n -= (size_t)w;
    }
    return 0;
}

static int run_json_rows(const char *const argv[]) {
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }
    fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    close(pipefd[1]);
    char head[8]; size_t have = 0; int passing = 0, failed = 0;
    char buf[65536]; ssize_t rd;
    while ((rd = read(pipefd[0], buf, sizeof(buf))) != 0) {
        if (rd < 0) { if (errno == EINTR) continue; failed = 1; break; }
        size_t off = 0;
        if (!passing) {
            while (have < sizeof(head) && off < (size_t)rd) head[have++] = buf[off++];
            if (have < sizeof(head)) continue;        /* still too short to tell */
            passing = 1;
            if (write_all(head, have) != 0) { failed = 1; break; }
        }
        if (off < (size_t)rd && write_all(buf + off, (size_t)rd - off) != 0) { failed = 1; break; }
    }
    close(pipefd[0]);
    if (!passing && !failed) {
        size_t n = have;
        while (n > 0 && (head[n - 1] == '\n' || head[n - 1] == '\r')) n--;
        if (n == 3 && memcmp(head, "[{]", 3) == 0) failed = write_all("[]\n", 3) != 0;
        else if (have > 0) failed = write_all(head, have) != 0;
    }
    int st; waitpid(pid, &st, 0);
    if (failed) return -1;
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static int run_cmd_capture(const char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t total = 0;
    ssize_t rd;
    while ((rd = read(pipefd[0], out + total, out_sz - total - 1)) > 0) {
        total += (size_t)rd;
        if (total >= out_sz - 1) break;
    }
    close(pipefd[0]);
    out[total] = '\0';
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static int cmd_layers_native(int argc, char **argv) {
    const char *root = NULL, *family = NULL, *workflow = NULL, *source = NULL, *status = NULL, *kind = NULL;
    int bridge_required = 0;
    char *json = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) root = argv[++i];
        else if (strcmp(argv[i], "--family") == 0 && i + 1 < argc) family = argv[++i];
        else if (strcmp(argv[i], "--workflow") == 0 && i + 1 < argc) workflow = argv[++i];
        else if (strcmp(argv[i], "--source") == 0 && i + 1 < argc) source = argv[++i];
        else if (strcmp(argv[i], "--status") == 0 && i + 1 < argc) status = argv[++i];
        else if (strcmp(argv[i], "--kind") == 0 && i + 1 < argc) kind = argv[++i];
        else if (strcmp(argv[i], "--bridge-required") == 0) bridge_required = 1;
    }
    if (bf_layer_query_json(root, family, workflow, source, status, kind, bridge_required, &json) != 0) {
        fprintf(stderr, "bonfyre-query: layer query failed\n");
        return 1;
    }
    puts(json);
    free(json);
    return 0;
}

static int cmd_bridges_native(int argc, char **argv) {
    const char *root = NULL, *family = NULL;
    char *json = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) root = argv[++i];
        else if (strcmp(argv[i], "--family") == 0 && i + 1 < argc) family = argv[++i];
    }
    if (bf_layer_bridge_query_json(root, family, &json) != 0) {
        fprintf(stderr, "bonfyre-query: bridge query failed\n");
        return 1;
    }
    puts(json);
    free(json);
    return 0;
}

/* ── JSON artifact file finder ────────────────────────────────── */

#define MAX_FILES 4096
#define MAX_PATH_LEN PATH_MAX

static int find_json_files(const char *dir, char files[][MAX_PATH_LEN], int max) {
    int count = 0;
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *ent;
    while ((ent = readdir(d)) && count < max) {
        if (ent->d_name[0] == '.') continue;
        char fp[MAX_PATH_LEN];
        snprintf(fp, sizeof(fp), "%s/%s", dir, ent->d_name);
        struct stat st;
        if (stat(fp, &st) < 0) continue;
        if (S_ISDIR(st.st_mode)) {
            count += find_json_files(fp, files + count, max - count);
        } else if (S_ISREG(st.st_mode)) {
            const char *ext = strrchr(ent->d_name, '.');
            if (ext && strcmp(ext, ".json") == 0) {
                strncpy(files[count], fp, MAX_PATH_LEN - 1);
                files[count][MAX_PATH_LEN - 1] = '\0';
                count++;
            }
        }
    }
    closedir(d);
    return count;
}

/* BonfyreQuery is a front end over DuckDB: every analytic command below runs by
 * exec'ing `duckdb`. When that binary is absent execvp fails in the child, which
 * _exit(127)s, and the parent is left holding empty output -- which previously
 * read as an empty result set rather than a missing dependency. The organ then
 * advertised itself reachable while being unable to answer a single query.
 * Probe once and fail closed, naming what is missing. */
static int duckdb_present(void) {
    const char *probe[] = { "duckdb", "--version", NULL };
    char buf[128] = {0};
    return run_cmd_capture(probe, buf, sizeof(buf)) == 0 && buf[0] != '\0';
}

static int require_duckdb(void) {
    if (duckdb_present()) return 1;
    fprintf(stderr, "bonfyre-query: DuckDB backend not found on PATH. "
                    "This organ executes every query through the duckdb CLI; "
                    "install it before calling this command.\n");
    return 0;
}

/* ── commands ───────────────────────────────────────────────── */

static int cmd_scan(const char *dir, const char *db_path) {
    if (!require_duckdb()) return 3;
    fprintf(stderr, "[query] Scanning %s for JSON artifacts...\n", dir);

    static char files[MAX_FILES][MAX_PATH_LEN];
    int nfiles = find_json_files(dir, files, MAX_FILES);
    fprintf(stderr, "[query] Found %d JSON files\n", nfiles);
    if (nfiles == 0) return 0;

    /* Build DuckDB import SQL:
     * CREATE TABLE artifacts AS SELECT * FROM read_json_auto([list]) */
    char sql[65536];
    int off = snprintf(sql, sizeof(sql),
        "CREATE OR REPLACE TABLE artifacts AS "
        "SELECT * FROM read_json_auto([");
    for (int i = 0; i < nfiles && (size_t)off < sizeof(sql) - 256; i++) {
        off += snprintf(sql + off, sizeof(sql) - (size_t)off,
            "'%s'%s", files[i], (i < nfiles - 1) ? "," : "");
    }
    off += snprintf(sql + off, sizeof(sql) - (size_t)off,
        "], union_by_name=true, filename=true);");

    fprintf(stderr, "[query] Importing into %s\n", db_path);
    const char *argv[] = { "duckdb", db_path, sql, NULL };
    int rc = run_cmd(argv);
    if (rc != 0) {
        fprintf(stderr, "[query] DuckDB import failed (rc=%d)\n", rc);
        return rc;
    }

    /* count rows */
    char count_buf[256];
    const char *cnt_argv[] = { "duckdb", db_path,
        "SELECT count(*) || ' artifacts imported' FROM artifacts;", NULL };
    run_cmd_capture(cnt_argv, count_buf, sizeof(count_buf));
    fprintf(stderr, "[query] → %s: %s", db_path, count_buf);
    return 0;
}

static int cmd_sql(const char *db_path, const char *query) {
    if (!require_duckdb()) return 3;
    fprintf(stderr, "[query] Running SQL on %s\n", db_path);
    const char *argv[] = { "duckdb", "-json", db_path, query, NULL };
    return run_json_rows(argv);
}

/* Does the imported table actually carry this column?
 *
 * scan() imports with read_json_auto(union_by_name=true), so the column set is
 * whatever keys the artifact.json files happen to have -- it is not fixed. A
 * stats query that hardcodes a column name fails with a DuckDB binder error on
 * any corpus lacking it, which is how `stats` came to be unusable on a store
 * that `scan` had just populated successfully with 594 rows.
 */
static int has_column(const char *db_path, const char *column) {
    char q[512];
    snprintf(q, sizeof(q),
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_name='artifacts' AND column_name='%s';", column);
    char buf[128] = {0};
    const char *argv[] = { "duckdb", "-noheader", "-list", db_path, q, NULL };
    if (run_cmd_capture(argv, buf, sizeof(buf)) != 0) return 0;
    return atoi(buf) > 0;
}

static int cmd_stats(const char *db_path, const char *out_dir) {
    ensure_dir(out_dir);

    /* filename is always present: scan passes filename=true. type is not. */
    const int have_type = has_column(db_path, "type");
    const char *stats_sql = have_type
        ? "SELECT "
          "  count(*) as total_artifacts, "
          "  count(DISTINCT type) as unique_types, "
          "  count(DISTINCT filename) as source_files, "
          "  list(DISTINCT type) as types "
          "FROM artifacts;"
        : "SELECT "
          "  count(*) as total_artifacts, "
          "  NULL as unique_types, "
          "  count(DISTINCT filename) as source_files, "
          "  NULL as types "
          "FROM artifacts;";

    char result[16384];
    const char *argv[] = { "duckdb", "-json", db_path, stats_sql, NULL };
    int rc = run_cmd_capture(argv, result, sizeof(result));
    if (rc != 0) return rc;

    char out_path[PATH_MAX];
    snprintf(out_path, sizeof(out_path), "%s/stats.json", out_dir);
    FILE *f = fopen(out_path, "w");
    if (!f) { perror("fopen"); return 1; }
    char ts[64]; iso_ts(ts, sizeof(ts));
    fprintf(f, "{\n  \"type\": \"artifact-stats\",\n");
    fprintf(f, "  \"db\": \"%s\",\n", db_path);
    fprintf(f, "  \"timestamp\": \"%s\",\n", ts);
    fprintf(f, "  \"results\": %s\n}\n", result);
    fclose(f);

    fprintf(stderr, "[query] → %s\n", out_path);
    printf("%s", result);
    return 0;
}

static int cmd_family(const char *db_path, const char *family_key) {
    char query[1024];
    snprintf(query, sizeof(query),
        "SELECT * FROM artifacts WHERE "
        "family_key = '%s' OR "
        "source LIKE '%%%s%%' OR "
        "filename LIKE '%%%s%%' "
        "ORDER BY timestamp;",
        family_key, family_key, family_key);

    const char *argv[] = { "duckdb", "-json", db_path, query, NULL };
    return run_cmd(argv);
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-query — local artifact analytics (DuckDB)\n\n"
        "Usage:\n"
        "  bonfyre-query layers [--family T_*] [--workflow A3.s03] [--source SRC] [--status STATUS] [--kind KIND] [--root DIR]\n"
        "  bonfyre-query bridges [--family T_*] [--root DIR]\n"
        "  bonfyre-query scan <dir> [db-path]\n"
        "  bonfyre-query sql <db> \"<SQL>\"\n"
        "  bonfyre-query stats <db> [output-dir]\n"
        "  bonfyre-query family <db> <family-key>\n"
        "  bonfyre-query status\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { print_usage(); return 0; }
    if (argc < 2) {
        fprintf(stderr, "usage: bonfyre-query <status|sql|scan|layers|bridges|...>\n");
        return 2;
    }
    enum { QR_STATUS };
    static const BfCommandEntry query_commands[] = {
        {"status", QR_STATUS, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(query_commands,
        sizeof(query_commands) / sizeof(query_commands[0]), argv[1]);
    if (command && command->id == QR_STATUS) {
        /* Report the backend honestly: a hardcoded "ok" told the operator
         * surface this capability was reachable even with no DuckDB installed. */
        int ok = duckdb_present();
        printf("{\"binary\":\"bonfyre-query\",\"status\":\"%s\","
               "\"backend\":\"duckdb\",\"backend_present\":%s,"
               "\"version\":\"1.0.0\"}\n",
               ok ? "ok" : "degraded", ok ? "true" : "false");
        return ok ? 0 : 1;
    }

    const char *cmd = argv[1];

    if (strcmp(cmd, "layers") == 0) {
        return cmd_layers_native(argc, argv);
    }
    if (strcmp(cmd, "bridges") == 0) {
        return cmd_bridges_native(argc, argv);
    }

    if (argc < 3) { print_usage(); return 1; }

    if (strcmp(cmd, "scan") == 0) {
        const char *db = (argc > 3) ? argv[3] : "artifacts.duckdb";
        return cmd_scan(argv[2], db);
    } else if (strcmp(cmd, "sql") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_sql(argv[2], argv[3]);
    } else if (strcmp(cmd, "stats") == 0) {
        return cmd_stats(argv[2], (argc > 3) ? argv[3] : "output");
    } else if (strcmp(cmd, "family") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_family(argv[2], argv[3]);
    }

    print_usage();
    return 1;
}
