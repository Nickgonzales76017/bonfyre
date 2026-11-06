#include <sqlite3.h>
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static int norm(const char *in, char *out, size_t cap) {
    size_t n = 0;
    int sep = 0;
    if (!in || !*in || cap < 2) return 0;
    for (const unsigned char *p = (const unsigned char *)in; *p; p++) {
        unsigned char c = (unsigned char)tolower(*p);
        if (isalnum(c) || c == '_') {
            if (n + 1 >= cap) return 0;
            out[n++] = (char)c;
            sep = 0;
        } else if (n && !sep) {
            if (n + 1 >= cap) return 0;
            out[n++] = '-';
            sep = 1;
        }
    }
    while (n && out[n - 1] == '-') n--;
    out[n] = '\0';
    return n > 0;
}

static void json_text(const char *s) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        if (*p == '"' || *p == '\\') { putchar('\\'); putchar(*p); }
        else if (*p < 32) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s\n", err ? err : sqlite3_errmsg(db));
        sqlite3_free(err);
        return 0;
    }
    return 1;
}

static int init_schema(sqlite3 *db) {
    return exec_sql(db,
        "PRAGMA journal_mode=WAL;"
        "CREATE TABLE IF NOT EXISTS pack_condition_state("
        "condition_ref TEXT NOT NULL,"
        "subject TEXT NOT NULL,"
        "scope TEXT NOT NULL,"
        "state TEXT NOT NULL CHECK(state IN ('satisfied','unsatisfied')),"
        "generation INTEGER NOT NULL CHECK(generation>=1),"
        "basis TEXT NOT NULL CHECK(length(basis)>0),"
        "updated_at INTEGER NOT NULL,"
        "PRIMARY KEY(condition_ref,subject,scope));"
        "CREATE INDEX IF NOT EXISTS pack_condition_subject_scope "
        "ON pack_condition_state(subject,scope,condition_ref);"
        "CREATE TABLE IF NOT EXISTS pack_condition_outbox("
        "condition_ref TEXT NOT NULL,"
        "subject TEXT NOT NULL,"
        "scope TEXT NOT NULL,"
        "state TEXT NOT NULL CHECK(state IN ('satisfied','unsatisfied')),"
        "generation INTEGER NOT NULL CHECK(generation>=1),"
        "basis TEXT NOT NULL CHECK(length(basis)>0),"
        "emitted INTEGER NOT NULL DEFAULT 0 CHECK(emitted IN (0,1)),"
        "created_at INTEGER NOT NULL,"
        "PRIMARY KEY(condition_ref,subject,scope,generation));"
        "CREATE INDEX IF NOT EXISTS pack_condition_outbox_pending ON pack_condition_outbox(emitted,created_at);");
}

static int parse_generation(const char *raw, sqlite3_int64 *out) {
    char *end = NULL;
    long long value;
    errno = 0;
    if (!raw || !*raw) return 0;
    value = strtoll(raw, &end, 10);
    if (errno || !end || *end || value < 1) return 0;
    *out = (sqlite3_int64)value;
    return 1;
}

static void emit_resolution(const char *condition, const char *subject, const char *scope,
                            const char *state, sqlite3_int64 generation, const char *basis) {
    fputs("{\"schema\":\"bonfyre-pack-condition-resolution.v1\",\"condition_ref\":", stdout);
    json_text(condition);
    fputs(",\"subject\":", stdout); json_text(subject);
    fputs(",\"scope\":", stdout); json_text(scope);
    fputs(",\"state\":", stdout); json_text(state);
    printf(",\"generation\":%lld,\"basis\":", (long long)generation);
    json_text(basis ? basis : "");
    fputs(",\"truth\":\"condition_truth_only_no_authority_or_observation\","
          "\"cannot_infer\":[\"condition satisfied -> authority\","
          "\"condition satisfied -> observation\","
          "\"condition satisfied -> pack installed\","
          "\"condition satisfied -> body retained\"]}\n", stdout);
}

static int check_condition(sqlite3 *db, const char *condition, const char *subject, const char *scope) {
    sqlite3_stmt *st = NULL;
    const char *state = "unknown", *basis = "";
    sqlite3_int64 generation = 0;
    int found = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT state,generation,basis FROM pack_condition_state "
        "WHERE condition_ref=?1 AND subject=?2 AND scope=?3",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, condition, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, scope, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        state = (const char *)sqlite3_column_text(st, 0);
        generation = sqlite3_column_int64(st, 1);
        basis = (const char *)sqlite3_column_text(st, 2);
        found = 1;
        emit_resolution(condition, subject, scope, state, generation, basis);
    }
    sqlite3_finalize(st);
    if (!found) emit_resolution(condition, subject, scope, "unknown", 0, "");
    return 1;
}

static int assert_condition(sqlite3 *db, const char *condition, const char *subject, const char *scope,
                            const char *state, sqlite3_int64 generation, const char *basis) {
    sqlite3_stmt *read = NULL, *write = NULL, *outbox = NULL;
    const char *old_state = NULL, *old_basis = NULL;
    sqlite3_int64 old_generation = 0;
    int exists = 0, mode = 0, ok = 0;
    if (!exec_sql(db, "BEGIN IMMEDIATE;")) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT state,generation,basis FROM pack_condition_state "
        "WHERE condition_ref=?1 AND subject=?2 AND scope=?3",
        -1, &read, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(read, 1, condition, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(read, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(read, 3, scope, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(read) == SQLITE_ROW) {
        old_state = (const char *)sqlite3_column_text(read, 0);
        old_generation = sqlite3_column_int64(read, 1);
        old_basis = (const char *)sqlite3_column_text(read, 2);
        exists = 1;
        if (generation < old_generation) mode = -1;
        else if (generation == old_generation) {
            mode = (!strcmp(state, old_state) && !strcmp(basis, old_basis)) ? 2 : -2;
        } else mode = 1;
    } else mode = 1;
    if (mode < 0) {
        sqlite3_finalize(read);
        read = NULL;
        exec_sql(db, "ROLLBACK;");
        fprintf(stderr, "%s generation for condition tuple\n", mode == -1 ? "stale" : "conflicting");
        return 0;
    }
    if (mode == 2) {
        sqlite3_finalize(read);
        read = NULL;
        if (!exec_sql(db, "COMMIT;")) return 0;
        printf("{\"schema\":\"bonfyre-pack-condition-assertion.v1\",\"state\":\"idempotent\","
               "\"generation\":%lld,\"truth\":\"condition_truth_only_no_authority_or_observation\"}\n",
               (long long)generation);
        return 1;
    }
    sqlite3_finalize(read);
    read = NULL;
    if (sqlite3_prepare_v2(db,
        exists ?
        "UPDATE pack_condition_state SET state=?4,generation=?5,basis=?6,updated_at=strftime('%s','now') "
        "WHERE condition_ref=?1 AND subject=?2 AND scope=?3" :
        "INSERT INTO pack_condition_state(condition_ref,subject,scope,state,generation,basis,updated_at) "
        "VALUES(?1,?2,?3,?4,?5,?6,strftime('%s','now'))",
        -1, &write, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(write, 1, condition, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(write, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(write, 3, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(write, 4, state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(write, 5, generation);
    sqlite3_bind_text(write, 6, basis, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(write) != SQLITE_DONE) goto out;
    sqlite3_finalize(write);
    write = NULL;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO pack_condition_outbox(condition_ref,subject,scope,state,generation,basis,created_at) "
        "VALUES(?1,?2,?3,?4,?5,?6,strftime('%s','now'))",
        -1, &outbox, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(outbox, 1, condition, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(outbox, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(outbox, 3, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(outbox, 4, state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(outbox, 5, generation);
    sqlite3_bind_text(outbox, 6, basis, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(outbox) != SQLITE_DONE) goto out;
    sqlite3_finalize(outbox);
    outbox = NULL;
    if (!exec_sql(db, "COMMIT;")) goto out;
    printf("{\"schema\":\"bonfyre-pack-condition-assertion.v1\",\"state\":\"accepted\","
           "\"generation\":%lld,\"truth\":\"condition_truth_only_no_authority_or_observation\"}\n",
           (long long)generation);
    ok = 1;
    goto out_no_rollback;
out:
    exec_sql(db, "ROLLBACK;");
out_no_rollback:
    if (read) sqlite3_finalize(read);
    if (write) sqlite3_finalize(write);
    if (outbox) sqlite3_finalize(outbox);
    return ok;
}

int main(int argc, char **argv) {
    const char *dbpath, *condition_raw, *subject_raw, *scope_raw;
    char condition[256], subject[256], scope[256];
    sqlite3 *db = NULL;
    int readonly = 0, ok = 0;
    enum {
        PC_ASSERT, PC_CHECK, PC_INIT,
    };
    static const BfCommandEntry pack_condition_commands[] = {
        {"assert", PC_ASSERT, 0, NULL},
        {"check", PC_CHECK, 0, NULL},
        {"init", PC_INIT, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(pack_condition_commands,
        sizeof(pack_condition_commands) / sizeof(pack_condition_commands[0]), argv[1]);
    if (!command) {
        fputs("usage: bonfyre-pack-condition init --db FILE | assert|check --db FILE --condition C --subject S --scope S [--state satisfied|unsatisfied --generation N --basis REF]\n", stderr);
        return 2;
    }
    dbpath = arg(argc, argv, "--db");
    if (!dbpath) return 2;
    if (command->id == PC_INIT) {
        if (sqlite3_open_v2(dbpath, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) goto done;
        ok = init_schema(db);
        if (ok) puts("{\"schema\":\"bonfyre-pack-condition-store.v1\",\"state\":\"ready\"}");
        goto done;
    }
    condition_raw = arg(argc, argv, "--condition");
    subject_raw = arg(argc, argv, "--subject");
    scope_raw = arg(argc, argv, "--scope");
    if (!norm(condition_raw, condition, sizeof(condition)) ||
        !norm(subject_raw, subject, sizeof(subject)) ||
        !norm(scope_raw, scope, sizeof(scope))) return 2;
    readonly = command->id == PC_CHECK;
    if (sqlite3_open_v2(dbpath, &db, readonly ? SQLITE_OPEN_READONLY : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE), NULL) != SQLITE_OK) goto done;
    if (!readonly && !init_schema(db)) goto done;
    if (readonly) {
        ok = check_condition(db, condition, subject, scope);
    } else {
        const char *state = arg(argc, argv, "--state");
        const char *generation_raw = arg(argc, argv, "--generation");
        const char *basis = arg(argc, argv, "--basis");
        sqlite3_int64 generation = 0;
        if (!state || (strcmp(state, "satisfied") && strcmp(state, "unsatisfied")) ||
            !parse_generation(generation_raw, &generation) || !basis || !*basis) {
            sqlite3_close(db);
            return 2;
        }
        ok = assert_condition(db, condition, subject, scope, state, generation, basis);
    }
done:
    if (db) sqlite3_close(db);
    return ok ? 0 : 1;
}
