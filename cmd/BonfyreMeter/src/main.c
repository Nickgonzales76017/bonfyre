#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
/*
 * BonfyreMeter — usage metering + billing events.
 *
 * Every evidenced physical attempt is a metered event. This binary:
 *   - Records every operation with timestamp, duration, bytes, operator
 *   - Aggregates usage per key/org/tier
 *   - Computes billing (per-artifact, per-byte, per-operation pricing)
 *   - Exports invoices
 *
 * Usage:
 *   bonfyre-meter record --key KEY_ID --op OPERATION --receipt REF \\
 *       --attempt-ref REF --realization-ref REF --at RFC3339 [--bytes N] [--duration MS] [--db meter.db]
 *   bonfyre-meter usage --key KEY_ID [--since DATE] [--db meter.db]
 *   bonfyre-meter invoice --key KEY_ID [--period YYYY-MM] [--db meter.db]
 *   bonfyre-meter top [--limit N] [--db meter.db]
 *
 * A rate is never implied by an operation name. A priced event names the
 * admitted provider rate that applied when its physical attempt occurred.
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
#include <sqlite3.h>
#include "bonfyre.h"
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

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

static int delegate_layeros_meter(int argc, char *argv[]) {
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
    exec_argv[n++] = "meter";
    exec_argv[n++] = argv[2];
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "--root") == 0 && i + 1 < argc) ||
            (strcmp(argv[i], "--db") == 0 && i + 1 < argc)) {
            i++;
            continue;
        }
        if (strcmp(argv[i], "--op") == 0) {
            exec_argv[n++] = "--operation";
            continue;
        }
        exec_argv[n++] = argv[i];
    }
    exec_argv[n] = NULL;
    return run_execvp(exec_argv);
}

/* ---------- SQLite direct (no fork) ---------- */

static int print_row_cb(void *data, int ncols, char **vals, char **names) {
    int *first = (int *)data;
    if (*first) {
        for (int i = 0; i < ncols; i++) printf("%-20s", names[i]);
        printf("\n");
        for (int i = 0; i < ncols; i++) printf("%-20s", "------------------");
        printf("\n");
        *first = 0;
    }
    for (int i = 0; i < ncols; i++) printf("%-20s", vals[i] ? vals[i] : "");
    printf("\n");
    return 0;
}

static int print_plain_cb(void *data, int ncols, char **vals, char **names) {
    (void)data; (void)names;
    for (int i = 0; i < ncols; i++) printf("%s", vals[i] ? vals[i] : "");
    printf("\n");
    return 0;
}

static sqlite3 *db_open(const char *path) {
    sqlite3 *db=NULL;
    /* Physical-attempt and admitted-rate receipts are not rebuildable caches.
     * Require WAL + synchronous=FULL, verified by the SQLite owner. */
    int rc=bf_sqlite3_open_tier(path,&db,BF_SQLITE_DURABLE);
    if (rc!=SQLITE_OK) {
        fprintf(stderr, "Meter store cannot open at full durability (SQLite %d)\n", rc);
        return NULL;
    }
    return db;
}

static void ensure_schema(sqlite3 *db) {
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS events ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  key_id TEXT NOT NULL,"
        "  op TEXT NOT NULL,"
        "  bytes INTEGER DEFAULT 0,"
        "  duration_ms INTEGER DEFAULT 0,"
        "  timestamp TEXT NOT NULL,"
        /* Unknown, not zero. A price this estate cannot evidence is an open
         * question, and a column defaulting to 0.0 answered it with "free" on
         * every row nobody had a rate for. */
        "  cost REAL,"
        "  provider TEXT NOT NULL DEFAULT '',"
        "  receipt_ref TEXT NOT NULL DEFAULT '',"
        "  physical_attempt_ref TEXT NOT NULL DEFAULT '',"
        "  realization_ref TEXT NOT NULL DEFAULT '',"
        "  rate_id INTEGER,"
        "  work_subject TEXT NOT NULL DEFAULT '',"
        "  work_scope TEXT NOT NULL DEFAULT '',"
        "  work_id TEXT NOT NULL DEFAULT '',"
        "  work_run_id TEXT NOT NULL DEFAULT ''"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);"
        "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);"
        /* A rate is admitted, with the evidence that admitted it and the date
         * it took effect. Usage priced without one stays unpriced. */
        "CREATE TABLE IF NOT EXISTS rate ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  provider TEXT NOT NULL,"
        "  op TEXT NOT NULL,"
        "  unit TEXT NOT NULL,"
        "  amount REAL NOT NULL,"
        "  currency TEXT NOT NULL DEFAULT 'USD',"
        "  effective_at TEXT NOT NULL,"
        "  evidence_ref TEXT NOT NULL,"
        "  observed_at TEXT NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_rate_lookup ON rate(provider,op,effective_at);",
        NULL, NULL, NULL);
    /* A store written before prices had to be evidenced keeps its rows; the
     * columns it lacks are added rather than the store being replaced. */
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN provider TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN receipt_ref TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN physical_attempt_ref TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN realization_ref TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN rate_id INTEGER", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN work_subject TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN work_scope TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN work_id TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE events ADD COLUMN work_run_id TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE INDEX IF NOT EXISTS idx_events_work ON events(work_subject,work_scope,work_id)",NULL,NULL,NULL);
    /* Existing stores receive these columns above. Create the partial unique
     * index afterwards so migration cannot skip the idempotency law simply
     * because a pre-contract events table already existed. */
    sqlite3_exec(db,
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_physical_attempt "
        "ON events(physical_attempt_ref) WHERE physical_attempt_ref <> ''",
        NULL, NULL, NULL);
}

/* What one unit of this op costs, if this estate has admitted a rate for it.
 *
 * What stood here priced usage by the first letter of the op string: anything
 * beginning with O cost five cents, anything beginning with D cost ten, and
 * everything else was free. Those numbers were not evidence of anything, and
 * the free case was the worse half -- an unpriced op reported as costing
 * nothing is a claim, and the wrong one.
 *
 * So a price now requires an admitted rate: a provider, a unit, an amount, the
 * date it took effect and the evidence that admitted it. Usage with no such
 * rate is metered exactly as before and left unpriced, which is the true state
 * of what this estate knows about it. */
static int rate_for(sqlite3 *db, const char *provider, const char *op, const char *at,
                    double *amount, char *unit, size_t unit_cap, long long *rate_id) {
    sqlite3_stmt *stmt = NULL;
    int found = 0;
    if (!db || !provider || !*provider || !op) return 0;
    if (sqlite3_prepare_v2(db,
            "SELECT id, amount, unit FROM rate WHERE provider=?1 AND op=?2 AND effective_at<=?3 "
            "ORDER BY effective_at DESC, id DESC LIMIT 1", -1, &stmt, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(stmt, 1, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, op, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, at, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *rate_id = sqlite3_column_int64(stmt, 0);
        *amount = sqlite3_column_double(stmt, 1);
        snprintf(unit, unit_cap, "%s", (const char *)sqlite3_column_text(stmt, 2));
        found = 1;
    }
    sqlite3_finalize(stmt);
    return found;
}

/* The priced amount for one metered event, given an admitted rate. A rate is
 * per call, per byte, per megabyte or per millisecond; anything else this
 * estate has not been taught to apply, and an unapplied rate leaves the event
 * unpriced rather than guessed. */
static int apply_rate(const char *unit, double amount, long bytes, int duration_ms, double *cost) {
    if (!strcmp(unit, "call") || !strcmp(unit, "credit") || !strcmp(unit, "query")) {
        *cost = amount; return 1;
    }
    if (!strcmp(unit, "byte")) { *cost = amount * (double)bytes; return 1; }
    if (!strcmp(unit, "megabyte")) { *cost = amount * ((double)bytes / (1024.0 * 1024.0)); return 1; }
    if (!strcmp(unit, "gigabyte")) { *cost = amount * ((double)bytes / (1024.0 * 1024.0 * 1024.0)); return 1; }
    if (!strcmp(unit, "millisecond")) { *cost = amount * (double)duration_ms; return 1; }
    if (!strcmp(unit, "second")) { *cost = amount * ((double)duration_ms / 1000.0); return 1; }
    return 0;
}

/* ---------- commands ---------- */

static int same_text(const unsigned char *actual, const char *expected) {
    return !strcmp((const char *)(actual ? actual : (const unsigned char *)""),
                   expected ? expected : "");
}

/* A link is only recorded when an existing, scoped Queue work/run identity
 * is independently present. The work index is opened read-only: Meter does
 * not create account work or infer a relationship from timestamps or names. */
static int meter_verify_work_link(const char *index, const char *subject, const char *scope,
                                  const char *work_id, const char *run_id) {
    sqlite3 *db=NULL; sqlite3_stmt *q=NULL; int found=0;
    if (!index || *index!='/' || !subject || !*subject || !scope || !*scope ||
        !work_id || !*work_id || !run_id || !*run_id) return 0;
    if (sqlite3_open_v2(index,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK) goto done;
    sqlite3_busy_timeout(db,1000);
    if (sqlite3_prepare_v2(db,
        "SELECT 1 FROM account_work w JOIN account_work_run r ON r.work_id=w.id "
        "WHERE w.id=?1 AND w.subject=?2 AND w.scope=?3 AND r.run_id=?4 LIMIT 1",
        -1,&q,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(q,1,work_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,2,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,3,scope,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,4,run_id,-1,SQLITE_TRANSIENT);
    found=sqlite3_step(q)==SQLITE_ROW;
done:
    sqlite3_finalize(q); if(db) sqlite3_close(db); return found;
}

static int cmd_record(const char *db_path, const char *key, const char *op, long bytes,
                      int duration_ms, const char *provider, const char *receipt_ref,
                      const char *physical_attempt_ref, const char *realization_ref,
                      const char *happened_at, const char *work_db, const char *subject, const char *scope, const char *work_id, const char *work_run_id) {
    sqlite3 *db = db_open(db_path);
    sqlite3_stmt *stmt = NULL, *existing = NULL;
    char ts[64], unit[32] = "";
    double amount = 0.0, cost = 0.0;
    long long rate_id = 0;
    int priced = 0;
    if (!db) return 1;
    ensure_schema(db);
    const int work_link=(work_db&&*work_db)||(subject&&*subject)||(scope&&*scope)||
                        (work_id&&*work_id)||(work_run_id&&*work_run_id);
    if(work_link && !meter_verify_work_link(work_db,subject,scope,work_id,work_run_id)){
        fprintf(stderr,"[meter] refused: work/run is not evidenced in the requested account scope\n");
        sqlite3_close(db);return 2;
    }
    if (!receipt_ref || !*receipt_ref || !physical_attempt_ref || !*physical_attempt_ref ||
        !realization_ref || !*realization_ref || !happened_at || !*happened_at) {
        fprintf(stderr, "[meter] a usage event requires --receipt, --attempt-ref, --realization-ref and --at; "
                        "without them it is not an evidenced physical attempt\n");
        sqlite3_close(db);
        return 2;
    }
    /* When the usage happened, not when it was written down. Backfilled usage
     * priced at today's rate would charge yesterday's work at today's price,
     * and a rate that took effect after the event must not reach back to it. */
    snprintf(ts, sizeof(ts), "%s", happened_at);

    /* A retry of the same observed physical attempt retains its original
     * event and rate. A different payload under that identity is conflict,
     * not another charge. */
    if (sqlite3_prepare_v2(db,
            "SELECT key_id, op, bytes, duration_ms, timestamp, provider, receipt_ref, realization_ref "
            " ,work_subject,work_scope,work_id,work_run_id FROM events WHERE physical_attempt_ref=?1 AND physical_attempt_ref<>''", -1, &existing, NULL) == SQLITE_OK) {
        sqlite3_bind_text(existing, 1, physical_attempt_ref, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(existing) == SQLITE_ROW) {
            int exact = same_text(sqlite3_column_text(existing, 0), key) &&
                        same_text(sqlite3_column_text(existing, 1), op) &&
                        sqlite3_column_int64(existing, 2) == bytes &&
                        sqlite3_column_int(existing, 3) == duration_ms &&
                        same_text(sqlite3_column_text(existing, 4), ts) &&
                        same_text(sqlite3_column_text(existing, 5), provider) &&
                        same_text(sqlite3_column_text(existing, 6), receipt_ref) &&
                        same_text(sqlite3_column_text(existing, 7), realization_ref) &&
                        same_text(sqlite3_column_text(existing, 8), work_link?subject:"") &&
                        same_text(sqlite3_column_text(existing, 9), work_link?scope:"") &&
                        same_text(sqlite3_column_text(existing, 10), work_link?work_id:"") &&
                        same_text(sqlite3_column_text(existing, 11), work_link?work_run_id:"");
            sqlite3_finalize(existing);
            sqlite3_close(db);
            if (exact) {
                fprintf(stderr, "[meter] replay retained physical attempt %s\n", physical_attempt_ref);
                return 0;
            }
            fprintf(stderr, "[meter] refused: physical attempt %s already names a different usage payload\n",
                    physical_attempt_ref);
            return 2;
        }
        sqlite3_finalize(existing);
    }
    if (rate_for(db, provider, op, ts, &amount, unit, sizeof(unit), &rate_id))
        priced = apply_rate(unit, amount, bytes, duration_ms, &cost);

    sqlite3_prepare_v2(db,
        "INSERT INTO events (key_id, op, bytes, duration_ms, timestamp, cost, provider, receipt_ref, "
        "physical_attempt_ref, realization_ref, rate_id, work_subject, work_scope, work_id, work_run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, op, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, bytes);
    sqlite3_bind_int(stmt, 4, duration_ms);
    sqlite3_bind_text(stmt, 5, ts, -1, SQLITE_TRANSIENT);
    if (priced) sqlite3_bind_double(stmt, 6, cost); else sqlite3_bind_null(stmt, 6);
    sqlite3_bind_text(stmt, 7, provider ? provider : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 8, receipt_ref ? receipt_ref : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 9, physical_attempt_ref, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 10, realization_ref, -1, SQLITE_TRANSIENT);
    if (priced) sqlite3_bind_int64(stmt, 11, rate_id); else sqlite3_bind_null(stmt, 11);
    sqlite3_bind_text(stmt, 12, work_link?subject:"", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 13, work_link?scope:"", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 14, work_link?work_id:"", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 15, work_link?work_run_id:"", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        fprintf(stderr, "[meter] refused: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 2;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    if (priced)
        fprintf(stderr, "[meter] recorded key=%s op=%s bytes=%ld priced %.6f per %s at rate %lld\n",
                key, op, bytes, cost, unit, rate_id);
    else
        fprintf(stderr, "[meter] recorded key=%s op=%s bytes=%ld -- unpriced: no admitted rate for %s/%s\n",
                key, op, bytes, provider && *provider ? provider : "(no provider)", op);
    return 0;
}

/* Admit a rate, with the evidence that admits it. Without this there is no
 * legitimate way for a price to enter the store at all. */
static int cmd_admit_rate(const char *db_path, const char *provider, const char *op,
                          const char *unit, double amount, const char *currency,
                          const char *effective_at, const char *evidence_ref) {
    sqlite3 *db = db_open(db_path);
    sqlite3_stmt *stmt = NULL;
    char ts[64];
    double probe = 0.0;
    if (!db) return 1;
    ensure_schema(db);
    if (!provider || !*provider || !op || !*op || !unit || !*unit || !evidence_ref || !*evidence_ref) {
        fprintf(stderr, "[meter] a rate needs a provider, an op, a unit and the evidence that admits it\n");
        sqlite3_close(db); return 2;
    }
    /* token and megatoken rates are applied only by the provisional token
     * valuation read, per model dimension; a recorded event never uses them. */
    if (!apply_rate(unit, 1.0, 1, 1, &probe) && strcmp(unit, "token") && strcmp(unit, "megatoken")) {
        fprintf(stderr, "[meter] this estate cannot apply a rate per '%s'; it knows call, credit, query, "
                        "byte, megabyte, gigabyte, millisecond, second, token and megatoken\n", unit);
        sqlite3_close(db); return 2;
    }
    iso_timestamp(ts, sizeof(ts));
    sqlite3_prepare_v2(db,
        "INSERT INTO rate (provider, op, unit, amount, currency, effective_at, evidence_ref, observed_at) "
        "VALUES (?,?,?,?,?,?,?,?);", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, op, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, unit, -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(stmt, 4, amount);
    sqlite3_bind_text(stmt, 5, currency && *currency ? currency : "USD", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, effective_at && *effective_at ? effective_at : ts, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, evidence_ref, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 8, ts, -1, SQLITE_TRANSIENT);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    fprintf(stderr, "[meter] admitted %s/%s at %.6f per %s, on the evidence of %s\n",
            provider, op, amount, unit, evidence_ref);
    return 0;
}

static int cmd_usage(const char *db_path, const char *key, const char *since) {
    sqlite3 *db = db_open(db_path);
    if (!db) return 1;
    ensure_schema(db);

    sqlite3_stmt *stmt;
    if (since && since[0]) {
        sqlite3_prepare_v2(db,
            "SELECT op, COUNT(*) as invocations, SUM(bytes) as total_bytes, "
            "COALESCE(printf('%.4f',SUM(cost)),'unpriced') || "
            "  (CASE WHEN SUM(cost IS NULL) > 0 THEN ' (' || SUM(cost IS NULL) || ' unpriced)' ELSE '' END) "
            "FROM events WHERE key_id=? AND timestamp >= ? GROUP BY op ORDER BY SUM(cost) DESC;",
            -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, since, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT op, COUNT(*) as invocations, SUM(bytes) as total_bytes, "
            "COALESCE(printf('%.4f',SUM(cost)),'unpriced') || "
            "  (CASE WHEN SUM(cost IS NULL) > 0 THEN ' (' || SUM(cost IS NULL) || ' unpriced)' ELSE '' END) "
            "FROM events WHERE key_id=? GROUP BY op ORDER BY SUM(cost) DESC;",
            -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    }

    printf("=== Usage for %s ===\n", key);
    int ncols = sqlite3_column_count(stmt);
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (first) {
            for (int i = 0; i < ncols; i++) printf("%-20s", sqlite3_column_name(stmt, i));
            printf("\n");
            for (int i = 0; i < ncols; i++) printf("%-20s", "------------------");
            printf("\n");
            first = 0;
        }
        for (int i = 0; i < ncols; i++) {
            const char *v = (const char *)sqlite3_column_text(stmt, i);
            printf("%-20s", v ? v : "");
        }
        printf("\n");
    }
    sqlite3_finalize(stmt);

    printf("\n=== Totals ===\n");
    first = 1;
    sqlite3_exec(db,
        /* A total over rows whose price is unknown is not a total. It says
         * what was priced, and says how much was not. */
        "SELECT COUNT(*), SUM(bytes), COALESCE(printf('%.4f',SUM(cost)),'0') || "
        "  ' priced from ' || SUM(cost IS NOT NULL) || ' of ' || COUNT(*) || ' events' FROM events;",
        print_row_cb, &first, NULL);

    sqlite3_close(db);
    return 0;
}

static int cmd_invoice(const char *db_path, const char *key, const char *period) {
    sqlite3 *db = db_open(db_path);
    if (!db) return 1;
    ensure_schema(db);
    char ts[64]; iso_timestamp(ts, sizeof(ts));

    /* Not an invoice. What stood here printed one, with a TOTAL, over usage
     * nobody had been billed for -- which is the one inference this family
     * forbids outright. This says what was used and what that use accrues at
     * the rates this estate has admitted, and says plainly that accrual is
     * neither a bill nor a payment. */
    printf("╔══════════════════════════════════════╗\n");
    printf("║  ACCRUED USE -- NOT A BILL           ║\n");
    printf("╠══════════════════════════════════════╣\n");
    printf("║ Key:      %-26s ║\n", key);
    printf("║ Period:   %-26s ║\n", period ? period : "all-time");
    printf("║ Stated:   %-26s ║\n", ts);
    printf("╠══════════════════════════════════════╣\n");

    sqlite3_stmt *stmt;
    if (period && period[0]) {
        sqlite3_prepare_v2(db,
            "SELECT op || ' x' || COUNT(*) || ' = ' || "
            "  CASE WHEN SUM(cost IS NOT NULL)=0 THEN 'unpriced' "
            "       ELSE '$' || printf('%.4f', SUM(cost)) || "
            "            (CASE WHEN SUM(cost IS NULL)>0 THEN ' +' || SUM(cost IS NULL) || ' unpriced' ELSE '' END) END "
            "FROM events WHERE key_id=? AND timestamp LIKE ? GROUP BY op;",
            -1, &stmt, NULL);
        char like[128];
        snprintf(like, sizeof(like), "%s%%", period);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, like, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT op || ' x' || COUNT(*) || ' = ' || "
            "  CASE WHEN SUM(cost IS NOT NULL)=0 THEN 'unpriced' "
            "       ELSE '$' || printf('%.4f', SUM(cost)) || "
            "            (CASE WHEN SUM(cost IS NULL)>0 THEN ' +' || SUM(cost IS NULL) || ' unpriced' ELSE '' END) END "
            "FROM events WHERE key_id=? GROUP BY op;",
            -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *v = (const char *)sqlite3_column_text(stmt, 0);
        if (v) printf("║ %-36s ║\n", v);
    }
    sqlite3_finalize(stmt);

    printf("╠══════════════════════════════════════╣\n");
    int dummy = 1;
    /* Each unit on its own line. A provider billing in credits and a provider
     * billing in dollars do not add up, and one number over both would be an
     * amount nobody owes in any currency. */
    sqlite3_exec(db,
        "SELECT '  accrued ' || printf('%.4f', SUM(e.cost)) || ' ' || "
        "  COALESCE((SELECT r.currency FROM rate r WHERE r.id = e.rate_id), '?') || "
        "  ' over ' || COUNT(*) || ' events' "
        "FROM events e WHERE e.cost IS NOT NULL "
        "GROUP BY (SELECT r.currency FROM rate r WHERE r.id = e.rate_id);",
        print_plain_cb, &dummy, NULL);
    sqlite3_exec(db,
        "SELECT '  ' || COUNT(*) || ' events carry no admitted rate and stay unpriced' "
        "FROM events WHERE cost IS NULL HAVING COUNT(*) > 0;",
        print_plain_cb, &dummy, NULL);
    printf("╚══════════════════════════════════════╝\n");
    printf("  Accrued at rates this estate admitted with evidence. It is not a bill,\n");
    printf("  not money billed, paid or booked, and not authority to spend.\n");
    printf("  Usage with no admitted rate is counted and left unpriced.\n");

    sqlite3_close(db);
    return 0;
}

static int cmd_top(const char *db_path, int limit) {
    sqlite3 *db = db_open(db_path);
    if (!db) return 1;
    ensure_schema(db);
    printf("=== Top %d Keys by Revenue ===\n", limit);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "SELECT key_id, COUNT(*) as ops, SUM(cost) as revenue FROM events "
        "GROUP BY key_id ORDER BY revenue DESC LIMIT ?;",
        -1, &stmt, NULL);
    sqlite3_bind_int(stmt, 1, limit);

    int ncols = sqlite3_column_count(stmt);
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (first) {
            for (int i = 0; i < ncols; i++) printf("%-20s", sqlite3_column_name(stmt, i));
            printf("\n");
            for (int i = 0; i < ncols; i++) printf("%-20s", "------------------");
            printf("\n");
            first = 0;
        }
        for (int i = 0; i < ncols; i++) {
            const char *v = (const char *)sqlite3_column_text(stmt, i);
            printf("%-20s", v ? v : "");
        }
        printf("\n");
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

#include "meter_packet.h"
#include "bf_csv.h"
#include "meter_tokens.h"
#include "meter_context.h"

/* Scoped economic readback: a Meter event counts only when its exact run is
 * still present in the owner-derived account Work index. The original rate
 * reference and effective date are retained; no historical usage is repriced. */
static int cmd_work_costs(const char *meter_path,const char *work_path,const char *subject,
                          const char *scope,const char *work_id) {
    sqlite3 *db=NULL; sqlite3_stmt *s=NULL; char *uri=NULL; int rc=1;
    if(!meter_path||!*meter_path||!work_path||*work_path!='/'||!subject||!*subject||
       !scope||!*scope||!work_id||!*work_id) return 2;
    if(sqlite3_open_v2(meter_path,&db,SQLITE_OPEN_READONLY|SQLITE_OPEN_URI,NULL)!=SQLITE_OK)goto done;
    uri=sqlite3_mprintf("file:%s?mode=ro",work_path);
    if(!uri||sqlite3_prepare_v2(db,"ATTACH DATABASE ? AS work",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,uri,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM work.account_work WHERE id=?1 AND subject=?2 AND scope=?3",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,work_id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,scope,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    sqlite3_finalize(s);s=NULL;
    const char *sql=
      "WITH matched AS (SELECT e.cost AS stored_cost,e.provider,e.op,e.timestamp,e.receipt_ref,"
      "e.physical_attempt_ref,e.realization_ref,r.id rate_id,r.provider rate_provider,r.op rate_op,"
      "r.currency,r.effective_at,r.evidence_ref FROM events e LEFT JOIN rate r ON r.id=e.rate_id "
      "WHERE e.work_subject=?1 AND e.work_scope=?2 AND e.work_id=?3 AND "
      "EXISTS(SELECT 1 FROM work.account_work_run wr WHERE wr.work_id=e.work_id AND wr.run_id=e.work_run_id)),"
      " accepted AS (SELECT CASE WHEN rate_id IS NOT NULL AND currency='USD' AND rate_provider=provider "
      "AND rate_op=op AND effective_at<=timestamp AND length(evidence_ref)>0 AND "
      "length(receipt_ref)>0 AND length(physical_attempt_ref)>0 AND length(realization_ref)>0 "
      "AND stored_cost>=0 THEN stored_cost END AS usd FROM matched) "
      "SELECT count(*),count(usd),sum(usd) FROM accepted";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,scope,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,work_id,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    long long linked=sqlite3_column_int64(s,0),priced=sqlite3_column_int64(s,1);
    printf("task\t%s\nlinked_attempts\t%lld\npriced_attempts\t%lld\nunpriced_attempts\t%lld\n",
           work_id,linked,priced,linked-priced);
    printf("cost_coverage\t%s\n",!linked?"no-attempts":!priced?"unpriced":
           linked==priced?"complete":"partial");
    if(priced)printf("meter_accrual_usd\t%.6f\n",sqlite3_column_double(s,2));
    else fputs("meter_accrual_usd\tunknown\n",stdout);
    fputs("allocation_status\tnot_evaluated\nmoney_status\tunknown_not_inferred\n",stdout);
    rc=0;
done:
    if(rc)fprintf(stderr,"[meter] scoped Workboard/Meter evidence unavailable or not admitted\n");
    sqlite3_finalize(s);sqlite3_free(uri);if(db)sqlite3_close(db);return rc;
}

/* ---------- main ---------- */

static void usage(void) {
    fprintf(stderr,
        "BonfyreMeter — usage metering & billing\n\n"
        "  bonfyre-meter record --key K --op OP --receipt REF --attempt-ref REF --realization-ref REF --at RFC3339 [--bytes N] [--duration MS]\n"
        "  bonfyre-meter usage --key K [--since DATE]\n"
        "  bonfyre-meter record ... --work-db ABS --subject ACCOUNT --scope SCOPE --work-ref TASK --run-ref RUN\n"
        "  bonfyre-meter work-costs --db METER --work-db WORK_INDEX --subject ACCOUNT --scope SCOPE --mission TASK\n"
        "  bonfyre-meter token-import --db METER --subject ACCOUNT --source RECONCILED_EVENTS_CSV\n"
        "  bonfyre-meter token-usage --db METER --subject ACCOUNT [--work-db WORK_INDEX --scope SCOPE]\n"
        "  bonfyre-meter work-tokens --db METER --work-db WORK_INDEX --subject ACCOUNT --scope SCOPE --mission TASK\n"
        "  bonfyre-meter context-compare --db METER --work-db WORK_INDEX --subject ACCOUNT --scope SCOPE --mission TASK\n"
        "      --baseline-run RUN --compact-run RUN --handoff CONTEXT_VERIFY.json\n"
        "      admitted token rates: admit-rate --provider P --op MODEL:uncached_input|cached_input|cache_write|output --unit token|megatoken\n"
        "  bonfyre-meter invoice --key K [--period YYYY-MM]\n"
        "  bonfyre-meter top [--limit N]\n"
        "  bonfyre-meter layer <artifact_id> [--op verify|materialize|compose|run] [--root DIR]\n"
        "  bonfyre-meter quote-packet --db ABS --provider ID --op OP --at UTC\n"
        "  bonfyre-meter event-packet --db ABS --key K --attempt-ref REF\n"
        "  bonfyre-meter record-packet --db ABS --key K --op OP --provider ID --bytes N --duration MS\n"
        "      --receipt REF --attempt-ref REF --realization-ref REF --at UTC\n"
    );
}

enum {
    MC_ADMIT_RATE, MC_CONTEXT_COMPARE, MC_EVENT_PACKET, MC_INVOICE, MC_LAYER,
    MC_QUOTE_PACKET, MC_RECORD, MC_RECORD_PACKET, MC_TOKEN_IMPORT, MC_TOKEN_USAGE,
    MC_TOP, MC_USAGE, MC_WORK_COSTS, MC_WORK_TOKENS,
};
static const BfCommandEntry meter_commands[] = {
    {"admit-rate", MC_ADMIT_RATE, 0, NULL},
    {"context-compare", MC_CONTEXT_COMPARE, 0, NULL},
    {"event-packet", MC_EVENT_PACKET, 0, NULL},
    {"invoice", MC_INVOICE, 0, NULL},
    {"layer", MC_LAYER, 0, NULL},
    {"quote-packet", MC_QUOTE_PACKET, 0, NULL},
    {"record", MC_RECORD, 0, NULL},
    {"record-packet", MC_RECORD_PACKET, 0, NULL},
    {"token-import", MC_TOKEN_IMPORT, 0, NULL},
    {"token-usage", MC_TOKEN_USAGE, 0, NULL},
    {"top", MC_TOP, 0, NULL},
    {"usage", MC_USAGE, 0, NULL},
    {"work-costs", MC_WORK_COSTS, 0, NULL},
    {"work-tokens", MC_WORK_TOKENS, 0, NULL},
};

int main(int argc, char *argv[]) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const BfCommandEntry *command = argc >= 2
        ? bf_command_lookup(meter_commands, sizeof(meter_commands) / sizeof(meter_commands[0]), argv[1])
        : NULL;
    if (command && (command->id == MC_QUOTE_PACKET || command->id == MC_RECORD_PACKET || command->id == MC_EVENT_PACKET))
        return meter_packet_command(argc, argv, command->id == MC_QUOTE_PACKET, command->id == MC_EVENT_PACKET);
    const char *db = NULL;
    static char db_resolved[4096];  /* glibc gates PATH_MAX behind a feature macro */
    for (int i = 1; i < argc - 1; i++)
        if (strcmp(argv[i], "--db") == 0) db = argv[i+1];
    /* Same bare-name default as the index organ carried, with the same effect:
     * a meter store per working directory. */
    db = bf_state_path(db_resolved, sizeof(db_resolved),
                       "BONFYRE_METER_DB", "state/bonfyre-meter.db", db);
    if (!db) {
        fprintf(stderr, "[fatal] no meter store coordinate: pass --db or set "
                        "BONFYRE_METER_DB. Refusing to create one in the current "
                        "directory.\n");
        return 1;
    }

    if (argc >= 3 && command && command->id == MC_LAYER) {
        return delegate_layeros_meter(argc, argv);
    }

    if (command && (command->id == MC_TOKEN_IMPORT || command->id == MC_TOKEN_USAGE || command->id == MC_WORK_TOKENS)) {
        const char *src=NULL,*index=NULL,*subject=NULL,*scope=NULL,*work=NULL;
        for(int i=2;i<argc-1;i++){
            if(!strcmp(argv[i],"--source"))src=argv[i+1];
            else if(!strcmp(argv[i],"--work-db"))index=argv[i+1];
            else if(!strcmp(argv[i],"--subject"))subject=argv[i+1];
            else if(!strcmp(argv[i],"--scope"))scope=argv[i+1];
            else if(!strcmp(argv[i],"--mission"))work=argv[i+1];
        }
        if(command->id == MC_TOKEN_IMPORT) return cmd_token_import(db,src,subject);
        if(command->id == MC_TOKEN_USAGE) return cmd_token_usage(db,subject,index,scope);
        return cmd_work_tokens(db,index,subject,scope,work);
    }
    if (command && command->id == MC_CONTEXT_COMPARE) {
        const char *index=NULL,*subject=NULL,*scope=NULL,*work=NULL,*base=NULL,*compact=NULL,*handoff=NULL;
        for(int i=2;i<argc-1;i++){
            if(!strcmp(argv[i],"--work-db"))index=argv[i+1];
            else if(!strcmp(argv[i],"--subject"))subject=argv[i+1];
            else if(!strcmp(argv[i],"--scope"))scope=argv[i+1];
            else if(!strcmp(argv[i],"--mission"))work=argv[i+1];
            else if(!strcmp(argv[i],"--baseline-run"))base=argv[i+1];
            else if(!strcmp(argv[i],"--compact-run"))compact=argv[i+1];
            else if(!strcmp(argv[i],"--handoff"))handoff=argv[i+1];
        }
        return cmd_context_compare(db,index,subject,scope,work,base,compact,handoff);
    }
    if (command && command->id == MC_WORK_COSTS) {
        const char *index=NULL,*subject=NULL,*scope=NULL,*work=NULL;
        for(int i=2;i<argc-1;i++){
            if(!strcmp(argv[i],"--work-db"))index=argv[i+1];
            else if(!strcmp(argv[i],"--subject"))subject=argv[i+1];
            else if(!strcmp(argv[i],"--scope"))scope=argv[i+1];
            else if(!strcmp(argv[i],"--mission"))work=argv[i+1];
        }
        return cmd_work_costs(db,index,subject,scope,work);
    }

    if (command && command->id == MC_RECORD) {
        const char *key = NULL, *op = NULL, *provider = "", *receipt = "", *happened = "";
        const char *attempt = "", *realization = ""; const char *work_db=NULL,*work_subject=NULL,*work_scope=NULL,*work_ref=NULL,*run_ref=NULL;
        long bytes = 0; int dur = 0;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--key") == 0) key = argv[i+1];
            if (strcmp(argv[i], "--op") == 0) op = argv[i+1];
            if (strcmp(argv[i], "--bytes") == 0) bytes = atol(argv[i+1]);
            /* --bytes-of measures the realization itself, so the figure is
             * the size of the thing produced and not one supplied by hand. */
            if (strcmp(argv[i], "--bytes-of") == 0) {
                struct stat measured;
                if (stat(argv[i+1], &measured) != 0) {
                    fprintf(stderr, "[meter] cannot measure %s\n", argv[i+1]);
                    return 2;
                }
                bytes = (long)measured.st_size;
            }
            if (strcmp(argv[i], "--duration") == 0) dur = atoi(argv[i+1]);
            if (strcmp(argv[i], "--provider") == 0) provider = argv[i+1];
            if (strcmp(argv[i], "--receipt") == 0) receipt = argv[i+1];
            if (strcmp(argv[i], "--attempt-ref") == 0) attempt = argv[i+1];
            if (strcmp(argv[i], "--realization-ref") == 0) realization = argv[i+1];
            if (strcmp(argv[i], "--at") == 0) happened = argv[i+1];
            if (strcmp(argv[i], "--work-db") == 0) work_db = argv[i+1];
            if (strcmp(argv[i], "--subject") == 0) work_subject = argv[i+1];
            if (strcmp(argv[i], "--scope") == 0) work_scope = argv[i+1];
            if (strcmp(argv[i], "--work-ref") == 0) work_ref = argv[i+1];
            if (strcmp(argv[i], "--run-ref") == 0) run_ref = argv[i+1];
        }
        if (!key || !op) { fprintf(stderr, "record requires --key and --op\n"); return 1; }
        return cmd_record(db, key, op, bytes, dur, provider, receipt, attempt, realization, happened,work_db,work_subject,work_scope,work_ref,run_ref);
    }
    if (command && command->id == MC_ADMIT_RATE) {
        const char *provider = NULL, *op = NULL, *unit = NULL, *currency = "USD";
        const char *effective = "", *evidence = NULL;
        double amount = 0.0;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--provider") == 0) provider = argv[i+1];
            if (strcmp(argv[i], "--op") == 0) op = argv[i+1];
            if (strcmp(argv[i], "--unit") == 0) unit = argv[i+1];
            if (strcmp(argv[i], "--amount") == 0) amount = atof(argv[i+1]);
            if (strcmp(argv[i], "--currency") == 0) currency = argv[i+1];
            if (strcmp(argv[i], "--effective-at") == 0) effective = argv[i+1];
            if (strcmp(argv[i], "--evidence") == 0) evidence = argv[i+1];
        }
        if (!provider || !op || !unit || !evidence) {
            fprintf(stderr, "admit-rate requires --provider --op --unit --amount --evidence\n");
            return 1;
        }
        return cmd_admit_rate(db, provider, op, unit, amount, currency, effective, evidence);
    }
    if (command && command->id == MC_USAGE) {
        const char *key = NULL, *since = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--key") == 0) key = argv[i+1];
            if (strcmp(argv[i], "--since") == 0) since = argv[i+1];
        }
        if (!key) { fprintf(stderr, "usage requires --key\n"); return 1; }
        return cmd_usage(db, key, since);
    }
    if (command && command->id == MC_INVOICE) {
        const char *key = NULL, *period = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--key") == 0) key = argv[i+1];
            if (strcmp(argv[i], "--period") == 0) period = argv[i+1];
        }
        if (!key) { fprintf(stderr, "invoice requires --key\n"); return 1; }
        return cmd_invoice(db, key, period);
    }
    if (command && command->id == MC_TOP) {
        int limit = 10;
        for (int i = 2; i < argc - 1; i++)
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        return cmd_top(db, limit);
    }

    usage();
    return 1;
}
