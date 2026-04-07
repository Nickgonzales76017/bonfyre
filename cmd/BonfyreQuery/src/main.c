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

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX]; size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    memcpy(tmp, path, len + 1);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') { tmp[i] = '\0'; mkdir(tmp, 0755); tmp[i] = '/'; }
    }
    mkdir(tmp, 0755);
    return 0;
}

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

/* ── commands ───────────────────────────────────────────────── */

static int cmd_scan(const char *dir, const char *db_path) {
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
    fprintf(stderr, "[query] Running SQL on %s\n", db_path);
    const char *argv[] = { "duckdb", "-json", db_path, query, NULL };
    return run_cmd(argv);
}

static int cmd_stats(const char *db_path, const char *out_dir) {
    ensure_dir(out_dir);

    const char *stats_sql =
        "SELECT "
        "  count(*) as total_artifacts, "
        "  count(DISTINCT type) as unique_types, "
        "  count(DISTINCT filename) as source_files, "
        "  list(DISTINCT type) as types "
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
        "  bonfyre-query scan <dir> [db-path]\n"
        "  bonfyre-query sql <db> \"<SQL>\"\n"
        "  bonfyre-query stats <db> [output-dir]\n"
        "  bonfyre-query family <db> <family-key>\n"
        "  bonfyre-query status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-query\",\"status\":\"ok\",\"version\":\"1.0.0\"}\n");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];

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
