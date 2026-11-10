#include <sqlite3.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    char *condition, *subject, *scope, *state, *basis;
    sqlite3_int64 generation;
} Row;

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 1; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static char *dup_text(const unsigned char *text) {
    const char *s = (const char *)(text ? text : (const unsigned char *)"");
    size_t n = strlen(s) + 1;
    char *copy = malloc(n);
    if (copy) memcpy(copy, s, n);
    return copy;
}

static int ensure_dir(const char *path) {
    return path && *path && (mkdir(path, 0750) == 0 || errno == EEXIST);
}

static uint64_t hash_part(uint64_t h, const char *s) {
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        h ^= *p;
        h *= 1099511628211ULL;
    }
    h ^= 0xff;
    h *= 1099511628211ULL;
    return h;
}

static void json_text(FILE *f, const char *s) {
    fputc('"', f);
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        if (*p == '"' || *p == '\\') { fputc('\\', f); fputc(*p, f); }
        else if (*p < 32) fprintf(f, "\\u%04x", *p);
        else fputc(*p, f);
    }
    fputc('"', f);
}

static void row_free(Row *row) {
    if (!row) return;
    free(row->condition);
    free(row->subject);
    free(row->scope);
    free(row->state);
    free(row->basis);
    memset(row, 0, sizeof(*row));
}

static int write_delta(const char *dir, const Row *row) {
    uint64_t h = 1469598103934665603ULL;
    char generation[64], path[2048], temp[2080];
    FILE *f = NULL;
    snprintf(generation, sizeof(generation), "%lld", (long long)row->generation);
    h = hash_part(h, row->condition);
    h = hash_part(h, row->subject);
    h = hash_part(h, row->scope);
    h = hash_part(h, generation);
    if (snprintf(path, sizeof(path), "%s/%016llx.json", dir,
                 (unsigned long long)h) >= (int)sizeof(path) ||
        snprintf(temp, sizeof(temp), "%s.tmp", path) >= (int)sizeof(temp) ||
        !(f = fopen(temp, "w"))) return 0;
    fputs("{\"schema\":\"bonfyre-pack-condition-delta.v1\",\"condition_ref\":", f);
    json_text(f, row->condition);
    fputs(",\"subject\":", f); json_text(f, row->subject);
    fputs(",\"scope\":", f); json_text(f, row->scope);
    fputs(",\"state\":", f); json_text(f, row->state);
    fprintf(f, ",\"generation\":%lld,\"basis\":", (long long)row->generation);
    json_text(f, row->basis);
    fputs(",\"truth\":\"accepted_condition_change_notification_not_authority\"}\n", f);
    if (fclose(f) || rename(temp, path)) { remove(temp); return 0; }
    return 1;
}

int main(int argc, char **argv) {
    const char *dbpath = arg(argc, argv, "--db");
    const char *dir = arg(argc, argv, "--dir");
    const char *limit_raw = arg(argc, argv, "--limit");
    int limit = limit_raw ? atoi(limit_raw) : 256;
    sqlite3 *db = NULL;
    sqlite3_stmt *read = NULL, *mark = NULL;
    Row *rows = NULL;
    int count = 0, rc = 1, transaction = 0;
    if (!dbpath || !dir || limit < 1 || limit > 4096 || !ensure_dir(dir)) return 2;
    rows = calloc((size_t)limit, sizeof(*rows));
    if (!rows) return 1;
    if (sqlite3_open_v2(dbpath, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) goto out;
    if (sqlite3_prepare_v2(db,
        "SELECT condition_ref,subject,scope,state,generation,basis "
        "FROM pack_condition_outbox WHERE emitted=0 "
        "ORDER BY created_at,condition_ref,subject,scope,generation LIMIT ?1",
        -1, &read, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_int(read, 1, limit);
    while (count < limit && sqlite3_step(read) == SQLITE_ROW) {
        Row *row = &rows[count];
        row->condition = dup_text(sqlite3_column_text(read, 0));
        row->subject = dup_text(sqlite3_column_text(read, 1));
        row->scope = dup_text(sqlite3_column_text(read, 2));
        row->state = dup_text(sqlite3_column_text(read, 3));
        row->generation = sqlite3_column_int64(read, 4);
        row->basis = dup_text(sqlite3_column_text(read, 5));
        if (!row->condition || !row->subject || !row->scope || !row->state || !row->basis) goto out;
        count++;
    }
    sqlite3_finalize(read);
    read = NULL;
    if (sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) goto out;
    transaction = 1;
    if (sqlite3_prepare_v2(db,
        "UPDATE pack_condition_outbox SET emitted=1 "
        "WHERE condition_ref=?1 AND subject=?2 AND scope=?3 AND generation=?4 AND emitted=0",
        -1, &mark, NULL) != SQLITE_OK) goto out;
    for (int i = 0; i < count; i++) {
        Row *row = &rows[i];
        if (!write_delta(dir, row)) goto out;
        sqlite3_bind_text(mark, 1, row->condition, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(mark, 2, row->subject, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(mark, 3, row->scope, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(mark, 4, row->generation);
        if (sqlite3_step(mark) != SQLITE_DONE || sqlite3_changes(db) != 1) goto out;
        sqlite3_reset(mark);
        sqlite3_clear_bindings(mark);
    }
    sqlite3_finalize(mark);
    mark = NULL;
    if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto out;
    transaction = 0;
    printf("{\"schema\":\"bonfyre-pack-condition-outbox.v1\",\"emitted\":%d,"
           "\"truth\":\"accepted_condition_change_notification_not_authority\"}\n", count);
    rc = 0;
out:
    if (read) sqlite3_finalize(read);
    if (mark) sqlite3_finalize(mark);
    if (transaction && db) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    if (db) sqlite3_close(db);
    for (int i = 0; i < count; i++) row_free(&rows[i]);
    free(rows);
    return rc;
}
