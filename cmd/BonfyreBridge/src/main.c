#define _POSIX_C_SOURCE 200809L
/*
 * bonfyre-bridge — move records between systems and prove what arrived.
 *
 *   bonfyre-bridge db --from URL --to URL [--tables a,b] [--replace]
 *   bonfyre-bridge db-verify --from URL --to URL [--tables a,b]
 *
 * URL is postgres://user:pass@host:port/db, mysql://user:pass@host:port/db,
 * sqlite:/path/file.db, or env:NAME to read one of those from the environment
 * so a password is never on a command line.
 *
 * The moving is DuckDB's: its Postgres, MySQL and SQLite connectors read one
 * system and write the other, and they are maintained by people whose work
 * that is.  What is this organ's own is the receipt.  After a table is moved,
 * both sides are read back through the same reader and compared: row count,
 * a digest over every value of every row, and each column's type on each
 * side.  A table is reported as arrived only when rows and values agree.  A
 * type the target could not hold the same way is named, not smoothed over.
 *
 * The source is opened read-only.  Nothing is written to it.
 */

#include <ctype.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#include <fcntl.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_TABLES 512

int bridge_rules_main(int argc, char **argv);
int bridge_schema_map_main(int argc, char **argv);
int bridge_map_verify_main(int argc, char **argv);

typedef struct {
    char kind[16];          /* postgres | mysql | sqlite */
    char attach[2048];      /* the connector's own connection text */
    char display[512];      /* the same, with any password removed */
    char schema[128];       /* where tables live as the reader sees them */
} Endpoint;

static const char *opt(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static int flag(int argc, char **argv, const char *name) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], name)) return 1;
    return 0;
}

static void json_text(FILE *out, const char *text) {
    fputc('"', out);
    for (; text && *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') { fputc('\\', out); fputc(c, out); }
        else if (c == '\n') fputs("\\n", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

/* A value inside a single-quoted SQL string. */
static void sql_quote(char *out, size_t cap, const char *text) {
    size_t n = 0;
    for (; *text && n + 3 < cap; text++) { if (*text == '\'') out[n++] = '\''; out[n++] = *text; }
    out[n] = '\0';
}

static int ident_ok(const char *name) {
    if (!name || !*name || strlen(name) > 120) return 0;
    for (const char *p = name; *p; p++)
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '$') return 0;
    return 1;
}

static void percent_decode(char *text) {
    char *w = text;
    for (char *r = text; *r; r++) {
        if (*r == '%' && isxdigit((unsigned char)r[1]) && isxdigit((unsigned char)r[2])) {
            char hex[3] = {r[1], r[2], 0};
            *w++ = (char)strtol(hex, NULL, 16);
            r += 2;
        } else *w++ = *r;
    }
    *w = '\0';
}

static int parse_endpoint(const char *given, Endpoint *e) {
    char url[2048], quoted[2048];
    memset(e, 0, sizeof(*e));
    if (!given || !*given) return 1;
    if (!strncmp(given, "env:", 4)) {
        const char *value = getenv(given + 4);
        if (!value || !*value) { fprintf(stderr, "bridge: %s is not set\n", given + 4); return 1; }
        snprintf(url, sizeof(url), "%s", value);
    } else snprintf(url, sizeof(url), "%s", given);

    if (!strncmp(url, "sqlite:", 7)) {
        snprintf(e->kind, sizeof(e->kind), "sqlite");
        sql_quote(quoted, sizeof(quoted), url + 7);
        snprintf(e->attach, sizeof(e->attach), "%s", quoted);
        snprintf(e->display, sizeof(e->display), "sqlite:%s", url + 7);
        snprintf(e->schema, sizeof(e->schema), "main");
        return 0;
    }
    if (!strncmp(url, "postgres://", 11) || !strncmp(url, "postgresql://", 13) || !strncmp(url, "mysql://", 8)) {
        int is_mysql = !strncmp(url, "mysql://", 8);
        char *rest = strstr(url, "://") + 3, *at = strrchr(rest, '@'), *slash, *query;
        char user[256] = "", pass[512] = "", host[256] = "", port[16] = "", db[256] = "";
        char q_user[300], q_pass[600], q_host[300], q_db[300];
        if (at) {
            char *colon;
            *at = '\0';
            colon = strchr(rest, ':');
            if (colon) { *colon = '\0'; snprintf(pass, sizeof(pass), "%s", colon + 1); }
            snprintf(user, sizeof(user), "%s", rest);
            rest = at + 1;
        }
        slash = strchr(rest, '/');
        if (!slash) { fputs("bridge: a database URL names its database after the host\n", stderr); return 1; }
        *slash = '\0';
        snprintf(db, sizeof(db), "%s", slash + 1);
        query = strchr(db, '?');
        if (query) *query = '\0';
        { char *colon = strrchr(rest, ':'); if (colon) { *colon = '\0'; snprintf(port, sizeof(port), "%s", colon + 1); } }
        snprintf(host, sizeof(host), "%s", rest);
        percent_decode(user); percent_decode(pass); percent_decode(db);
        if (!host[0] || !db[0]) { fputs("bridge: a database URL needs a host and a database\n", stderr); return 1; }
        for (const char *p = port; *p; p++) if (!isdigit((unsigned char)*p)) { fputs("bridge: port is a number\n", stderr); return 1; }
        /* Both connectors take the same key=value text. */
        sql_quote(q_user, sizeof(q_user), user); sql_quote(q_pass, sizeof(q_pass), pass);
        sql_quote(q_host, sizeof(q_host), host); sql_quote(q_db, sizeof(q_db), db);
        snprintf(e->kind, sizeof(e->kind), is_mysql ? "mysql" : "postgres");
        snprintf(e->attach, sizeof(e->attach), "host=%s %s%s%s %s%s%s %s%s%s %s=%s", q_host,
                 port[0] ? "port=" : "", port, "",
                 user[0] ? "user=" : "", q_user, "",
                 pass[0] ? "password=" : "", q_pass, "",
                 is_mysql ? "database" : "dbname", q_db);
        snprintf(e->display, sizeof(e->display), "%s://%s%s%s%s%s/%s", is_mysql ? "mysql" : "postgres", user,
                 user[0] ? "@" : "", host, port[0] ? ":" : "", port, db);
        snprintf(e->schema, sizeof(e->schema), "%s", is_mysql ? db : "public");
        return 0;
    }
    fputs("bridge: a URL is postgres://, mysql://, sqlite:PATH or env:NAME\n", stderr);
    return 1;
}

/* Runs SQL through DuckDB and returns its JSON output.  The SQL carries the
 * connection text, so it travels in a private file, not on a command line. */
static char *duck(const char *sql, int *status) {
    char path[PATH_MAX] = "", *out = NULL;
    const char *tmp = getenv("TMPDIR");
    size_t used = 0, cap = 1 << 16;
    int fd, pipefd[2];
    pid_t pid;
    snprintf(path, sizeof(path), "%s/bonfyre-bridge.XXXXXX", tmp && *tmp ? tmp : "/tmp");
    fd = mkstemp(path);
    if (fd < 0) { *status = -1; return NULL; }
    if (write(fd, sql, strlen(sql)) < 0) { close(fd); unlink(path); *status = -1; return NULL; }
    close(fd);
    if (pipe(pipefd)) { unlink(path); *status = -1; return NULL; }
    pid = fork();
    if (pid == 0) {
        dup2(pipefd[1], 1); dup2(pipefd[1], 2);
        close(pipefd[0]); close(pipefd[1]);
        execlp("duckdb", "duckdb", "-json", "-bail", "-f", path, (char *)NULL);
        _exit(127);
    }
    close(pipefd[1]);
    out = malloc(cap);
    for (;;) {
        ssize_t n;
        if (used + 4096 > cap) { cap *= 2; out = realloc(out, cap); }
        if (!out) break;
        n = read(pipefd[0], out + used, cap - used - 1);
        if (n <= 0) break;
        used += (size_t)n;
    }
    close(pipefd[0]);
    if (out) out[used] = '\0';
    waitpid(pid, status, 0);
    *status = WIFEXITED(*status) ? WEXITSTATUS(*status) : -1;
    unlink(path);
    return out;
}

static void attach_sql(char *out, size_t cap, const Endpoint *from, const Endpoint *to, int read_only_target) {
    snprintf(out, cap,
             "INSTALL postgres; INSTALL mysql; INSTALL sqlite; LOAD postgres; LOAD mysql; LOAD sqlite;\n"
             "ATTACH '%s' AS src (TYPE %s, READ_ONLY);\nATTACH '%s' AS dst (TYPE %s%s);\n",
             from->attach, from->kind, to->attach, to->kind, read_only_target ? ", READ_ONLY" : "");
}

/* The last JSON array DuckDB printed, which is the last statement's rows. */
static const char *last_rows(const char *output) {
    const char *last = NULL;
    for (const char *p = output; (p = strstr(p, "[{")); p++) if (p == output || p[-1] == '\n') last = p;
    return last;
}

/* An error DuckDB printed, without any connection text it may have echoed. */
static void report_engine_error(const char *what, const char *output) {
    const char *line = output ? strstr(output, "Error") : NULL;
    char clean[400];
    size_t n = 0;
    if (!line) line = output ? output : "";
    for (; *line && *line != '\n' && n + 1 < sizeof(clean); line++) clean[n++] = *line;
    clean[n] = '\0';
    if (strstr(clean, "password=")) snprintf(clean, sizeof(clean), "the engine refused the connection");
    fprintf(stderr, "bridge: %s: %s\n", what, clean);
}

static int list_tables(const Endpoint *from, const Endpoint *to, const char *only, char tables[][128], int *count, int verify_only) {
    char sql[8192], attach[5000], schema[300];
    char *output;
    const char *rows;
    sqlite3 *mem = NULL;
    sqlite3_stmt *st = NULL;
    int status;
    attach_sql(attach, sizeof(attach), from, to, verify_only);
    sql_quote(schema, sizeof(schema), from->schema);
    snprintf(sql, sizeof(sql), "%sSELECT table_name FROM duckdb_tables() WHERE database_name='src' AND schema_name='%s' "
                               "AND NOT internal ORDER BY table_name;\n", attach, schema);
    output = duck(sql, &status);
    if (status || !output) { report_engine_error("could not read the source", output); free(output); return 1; }
    rows = last_rows(output);
    *count = 0;
    if (rows && sqlite3_open(":memory:", &mem) == SQLITE_OK &&
        sqlite3_prepare_v2(mem, "SELECT json_extract(value,'$.table_name') FROM json_each(?1)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, rows, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW && *count < MAX_TABLES) {
            const char *name = (const char *)sqlite3_column_text(st, 0);
            if (!name) continue;
            if (only) {
                /* Only the tables that were asked for. */
                size_t len = strlen(name);
                const char *hit = only;
                int wanted = 0;
                while ((hit = strstr(hit, name))) {
                    if ((hit == only || hit[-1] == ',') && (hit[len] == ',' || !hit[len])) { wanted = 1; break; }
                    hit += len;
                }
                if (!wanted) continue;
            }
            snprintf(tables[(*count)++], 128, "%s", name);
        }
    }
    if (st) sqlite3_finalize(st);
    if (mem) sqlite3_close(mem);
    free(output);
    return 0;
}

typedef struct { long long src_rows, dst_rows; char src_digest[64], dst_digest[64]; char types[4096]; int type_changes; } Readback;

/* Reads both sides through the one reader and compares them.
 *
 * The digest names every column and each side is read alone.  The test that
 * changes one value in the target is what showed an earlier form of this
 * comparison agreeing with itself; it stays in the suite. */
static int read_back(const Endpoint *from, const Endpoint *to, const char *table, Readback *r) {
    char attach[5000], src_schema[300], dst_schema[300];
    char *sql = NULL, *output = NULL, *expr = NULL;
    size_t sql_size = 0, expr_size = 0;
    FILE *sql_out, *expr_out;
    const char *rows;
    sqlite3 *mem = NULL;
    sqlite3_stmt *st = NULL;
    int status, ok = 0, columns = 0;
    size_t types_used = 0;
    memset(r, 0, sizeof(*r));
    attach_sql(attach, sizeof(attach), from, to, 1);
    sql_quote(src_schema, sizeof(src_schema), from->schema);
    sql_quote(dst_schema, sizeof(dst_schema), to->schema);

    /* The source's columns, and what each became on the target. */
    sql_out = open_memstream(&sql, &sql_size);
    if (!sql_out) return 1;
    fprintf(sql_out,
        "%sWITH s AS (SELECT column_name,data_type,column_index FROM duckdb_columns() "
        "WHERE database_name='src' AND schema_name='%s' AND table_name='%s'), "
        "d AS (SELECT column_name,data_type,column_index FROM duckdb_columns() "
        "WHERE database_name='dst' AND schema_name='%s' AND table_name='%s') "
        "SELECT coalesce(s.column_name,d.column_name) AS name, "
        "coalesce(s.data_type,'(absent)') AS source_type, "
        "coalesce(d.data_type,'(absent)') AS target_type "
        "FROM s FULL OUTER JOIN d ON s.column_name=d.column_name "
        "ORDER BY coalesce(s.column_index,d.column_index), name;\n",
        attach, src_schema, table, dst_schema, table);
    fclose(sql_out);
    output = duck(sql, &status);
    free(sql); sql = NULL;
    rows = output ? last_rows(output) : NULL;
    if (status || !rows || sqlite3_open(":memory:", &mem) != SQLITE_OK ||
        sqlite3_prepare_v2(mem, "SELECT json_extract(value,'$.name'), json_extract(value,'$.source_type'), "
                                "json_extract(value,'$.target_type') FROM json_each(?1)", -1, &st, NULL) != SQLITE_OK) {
        report_engine_error(table, output);
        goto done;
    }
    expr_out = open_memstream(&expr, &expr_size);
    if (!expr_out) goto done;
    sqlite3_bind_text(st, 1, rows, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0), *source_type = (const char *)sqlite3_column_text(st, 1);
        const char *target_type = (const char *)sqlite3_column_text(st, 2);
        if (!ident_ok(name)) { fclose(expr_out); fprintf(stderr, "bridge: %s has a column name this bridge will not write into a statement\n", table); goto done; }
        if (strcmp(source_type, target_type)) {
            r->type_changes++;
            if (types_used + strlen(name) + strlen(source_type) + strlen(target_type) + 10 < sizeof(r->types))
                types_used += (size_t)snprintf(r->types + types_used, sizeof(r->types) - types_used, "%s%s: %s -> %s",
                                              types_used ? "; " : "", name, source_type, target_type);
        }
        /* A target-only column is schema drift, not a source digest field. */
        if (!strcmp(source_type, "(absent)")) continue;
        /* Hash separate fields with native NULL handling. Joining text with
         * a separator merged distinct rows when a field contained it; a NUL
         * sentinel also made actual NUL text compare equal to SQL NULL. */
        fprintf(expr_out, "%sCAST(\"%s\" AS VARCHAR)", columns++ ? ", " : "", name);
    }
    fclose(expr_out);
    sqlite3_finalize(st); st = NULL;
    if (!columns) { fprintf(stderr, "bridge: %s has no columns to compare\n", table); goto done; }

    /* Each side is read by its own statement.  Asked for in one statement,
     * two tables of the same name came back as the same table. */
    for (int side = 0; side < 2; side++) {
        const char *base = side ? "dst" : "src", *schema = side ? to->schema : from->schema;
        free(output); output = NULL;
        free(sql); sql = NULL;
        sql_out = open_memstream(&sql, &sql_size);
        if (!sql_out) goto done;
        fprintf(sql_out,
            "%sSELECT count(*) AS n, CAST(coalesce(sum(CAST(hash(%s) AS HUGEINT)),0) AS VARCHAR) AS digest "
            "FROM %s.\"%s\".\"%s\";\n", attach, expr, base, schema, table);
        fclose(sql_out);
        output = duck(sql, &status);
        rows = output ? last_rows(output) : NULL;
        if (status || !rows) { report_engine_error(table, output); goto done; }
        if (sqlite3_prepare_v2(mem, "SELECT json_extract(value,'$.n'), json_extract(value,'$.digest') FROM json_each(?1)", -1, &st, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(st, 1, rows, -1, SQLITE_STATIC);
        if (sqlite3_step(st) != SQLITE_ROW) goto done;
        if (side) { r->dst_rows = sqlite3_column_int64(st, 0); snprintf(r->dst_digest, sizeof(r->dst_digest), "%s", (const char *)sqlite3_column_text(st, 1)); }
        else { r->src_rows = sqlite3_column_int64(st, 0); snprintf(r->src_digest, sizeof(r->src_digest), "%s", (const char *)sqlite3_column_text(st, 1)); }
        sqlite3_finalize(st); st = NULL;
    }
    ok = 1;
done:
    if (st) sqlite3_finalize(st);
    if (mem) sqlite3_close(mem);
    free(output); free(sql); free(expr);
    return ok ? 0 : 1;
}

static int bridge_db(int argc, char **argv, int verify_only) {
    Endpoint from, to;
    static char tables[MAX_TABLES][128];
    const char *only = opt(argc, argv, "--tables");
    int replace = flag(argc, argv, "--replace"), count = 0, arrived = 0, differ = 0, refused = 0, first = 1;
    long long total_rows = 0;
    if (parse_endpoint(opt(argc, argv, "--from"), &from) || parse_endpoint(opt(argc, argv, "--to"), &to)) {
        fputs("usage: bonfyre-bridge db --from URL --to URL [--tables a,b] [--replace]\n", stderr);
        return 2;
    }
    if (list_tables(&from, &to, only, tables, &count, verify_only)) return 3;
    if (!count) { fputs("bridge: the source holds no tables to move\n", stderr); return 4; }
    printf("{\"schema\":\"bonfyre.bridge.db.v1\",\"mode\":\"%s\",\"from\":", verify_only ? "verify" : "move");
    json_text(stdout, from.display);
    fputs(",\"to\":", stdout); json_text(stdout, to.display);
    fputs(",\"engine\":\"duckdb connectors\",\"tables\":[", stdout);
    for (int i = 0; i < count; i++) {
        Readback r;
        const char *state = "arrived";
        char reason[300] = "";
        int failed = 0;
        if (!ident_ok(tables[i])) { failed = 1; snprintf(reason, sizeof(reason), "table name has characters this bridge will not write into a statement"); }
        if (!failed && !verify_only) {
            char sql[12000], attach[5000];
            char *output;
            int status;
            attach_sql(attach, sizeof(attach), &from, &to, 0);
            snprintf(sql, sizeof(sql), "%s%s%s%s%s%sCREATE TABLE dst.\"%s\".\"%s\" AS SELECT * FROM src.\"%s\".\"%s\";\n", attach,
                     replace ? "DROP TABLE IF EXISTS dst.\"" : "", replace ? to.schema : "", replace ? "\".\"" : "",
                     replace ? tables[i] : "", replace ? "\";\n" : "", to.schema, tables[i], from.schema, tables[i]);
            output = duck(sql, &status);
            if (status) {
                failed = 1;
                snprintf(reason, sizeof(reason), "%s", output && strstr(output, "already exists") ?
                         "the target already holds a table of this name (use --replace to overwrite it)" : "the engine could not write this table to the target");
                if (!(output && strstr(output, "already exists"))) report_engine_error(tables[i], output);
            }
            free(output);
        }
        if (!failed && read_back(&from, &to, tables[i], &r)) { failed = 1; snprintf(reason, sizeof(reason), "the two sides could not be read back for comparison"); }
        if (!first) fputc(',', stdout);
        first = 0;
        fputs("{\"table\":", stdout); json_text(stdout, tables[i]);
        if (failed) {
            refused++;
            fputs(",\"state\":\"not_moved\",\"reason\":", stdout); json_text(stdout, reason);
            fputc('}', stdout);
            continue;
        }
        if (r.src_rows != r.dst_rows) state = "rows_differ";
        else if (strcmp(r.src_digest, r.dst_digest)) state = "values_differ";
        if (strcmp(state, "arrived")) differ++; else { arrived++; total_rows += r.dst_rows; }
        printf(",\"state\":\"%s\",\"source_rows\":%lld,\"target_rows\":%lld,\"values_match\":%s,\"type_changes\":",
               state, r.src_rows, r.dst_rows, strcmp(r.src_digest, r.dst_digest) ? "false" : "true");
        json_text(stdout, r.types);
        printf(",\"column_types_equal\":%s,\"column_type_changes\":%d", r.type_changes ? "false" : "true", r.type_changes);
        fputc('}', stdout);
    }
    printf("],\"arrived\":%d,\"differ\":%d,\"not_moved\":%d,\"rows_arrived\":%lld,"
           "\"verified\":%s,\"cannot_infer\":\"values agreeing as read back is not the target's constraints, indexes, "
           "keys or stored procedures having been carried; this moves rows and their types\"}\n",
           arrived, differ, refused, total_rows, differ || refused ? "false" : "true");
    return differ || refused ? 5 : 0;
}

static void usage(void) {
    fputs(
        "bonfyre bridge — move records between systems and prove what arrived\n\n"
        "  bonfyre bridge db --from URL --to URL [--tables a,b] [--replace]\n"
        "        copy every table (or the named ones), then read both sides back and compare\n"
        "  bonfyre bridge db-verify --from URL --to URL [--tables a,b]\n"
        "        compare two systems that should already hold the same tables; writes nothing\n\n"
        "  URL: postgres://user:pass@host:port/db   mysql://user:pass@host:port/db   sqlite:/path/file.db\n"
        "       env:NAME reads the URL from the environment, keeping a password off the command line\n\n"
        "  bonfyre bridge rules --format pgloader-casting --receipt RECEIPT.json --out rules.json\n"
        "  bonfyre bridge rules --format frappe-type-map --file database.py --const-file database.py --out rules.json\n"
        "        parse an upstream tool's documented type rules into cited rule candidates\n"
        "  bonfyre bridge schema-map --from sqlite:PATH --plan plan.json --rules rules.json --out mapping.json\n"
        "                            [--frappe-schema schema-ir.json] [--emit-sql rehearsal.sql]\n"
        "        type every source column by a cited rule or name the exception; write the rehearsal SQL\n"
        "  bonfyre bridge map-verify --from sqlite:PATH --to URL --mapping mapping.json --out diff.json [--expect diff.json]\n"
        "        rows, per-column checksums, types, constraints, objects and probes; --expect re-checks\n\n"
        "  A table has arrived when its row count and a digest over every value agree on both sides.\n"
        "  The source is opened read-only. Constraints, indexes and procedures are not carried.\n", stderr);
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) { usage(); return 2; }
    enum {
        BR_DB, BR_DB_VERIFY, BR_MAP_VERIFY, BR_RULES, BR_SCHEMA_MAP,
    };
    static const BfCommandEntry bridge_commands[] = {
        {"db", BR_DB, 0, NULL},
        {"db-verify", BR_DB_VERIFY, 0, NULL},
        {"map-verify", BR_MAP_VERIFY, 0, NULL},
        {"rules", BR_RULES, 0, NULL},
        {"schema-map", BR_SCHEMA_MAP, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(bridge_commands,
        sizeof(bridge_commands) / sizeof(bridge_commands[0]), argv[1]);
    if (command && command->id == BR_DB) return bridge_db(argc, argv, 0);
    if (command && command->id == BR_DB_VERIFY) return bridge_db(argc, argv, 1);
    if (command && command->id == BR_RULES) return bridge_rules_main(argc, argv);
    if (command && command->id == BR_SCHEMA_MAP) return bridge_schema_map_main(argc, argv);
    if (command && command->id == BR_MAP_VERIFY) return bridge_map_verify_main(argc, argv);
    usage();
    return 2;
}
