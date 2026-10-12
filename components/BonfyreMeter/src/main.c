/*
 * BonfyreMeter — usage metering + billing events.
 *
 * Every pipeline operation is a metered event. This binary:
 *   - Records every operation with timestamp, duration, bytes, operator
 *   - Aggregates usage per key/org/tier
 *   - Computes billing (per-artifact, per-byte, per-operation pricing)
 *   - Exports invoices
 *
 * Usage:
 *   bonfyre-meter record --key KEY_ID --op OPERATION --bytes N [--duration MS] [--db meter.db]
 *   bonfyre-meter usage --key KEY_ID [--since DATE] [--db meter.db]
 *   bonfyre-meter invoice --key KEY_ID [--period YYYY-MM] [--db meter.db]
 *   bonfyre-meter top [--limit N] [--db meter.db]
 *
 * Pricing model:
 *   Ingest:     $0.001/file
 *   Brief:      $0.01/brief
 *   Proof:      $0.02/proof
 *   Offer:      $0.05/offer
 *   Narrate:    $0.03/narration
 *   Pack:       $0.01/package
 *   Distribute: $0.10/distribution
 *   Emit:       $0.005/format
 *   Compress:   $0.0001/MB compressed
 *   Index:      $0.001/family indexed
 *   Hash:       free (infrastructure)
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
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
    sqlite3 *db;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "Cannot open db: %s\n", sqlite3_errmsg(db));
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
        "  cost REAL DEFAULT 0.0"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);"
        "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);",
        NULL, NULL, NULL);
}

static double op_cost(const char *op, long bytes) {
    if (strcmp(op, "Ingest") == 0) return 0.001;
    if (strcmp(op, "Brief") == 0) return 0.01;
    if (strcmp(op, "Proof") == 0) return 0.02;
    if (strcmp(op, "Offer") == 0) return 0.05;
    if (strcmp(op, "Narrate") == 0) return 0.03;
    if (strcmp(op, "Pack") == 0) return 0.01;
    if (strcmp(op, "Distribute") == 0) return 0.10;
    if (strcmp(op, "Emit") == 0) return 0.005;
    if (strcmp(op, "Compress") == 0) return 0.0001 * ((double)bytes / (1024.0 * 1024.0));
    if (strcmp(op, "Index") == 0) return 0.001;
    return 0.0; /* Hash, etc. — free */
}

/* ---------- commands ---------- */

static int cmd_record(const char *db_path, const char *key, const char *op, long bytes, int duration_ms) {
    sqlite3 *db = db_open(db_path);
    if (!db) return 1;
    ensure_schema(db);
    char ts[64]; iso_timestamp(ts, sizeof(ts));
    double cost = op_cost(op, bytes);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "INSERT INTO events (key_id, op, bytes, duration_ms, timestamp, cost) "
        "VALUES (?,?,?,?,?,?);", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, op, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, bytes);
    sqlite3_bind_int(stmt, 4, duration_ms);
    sqlite3_bind_text(stmt, 5, ts, -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(stmt, 6, cost);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    fprintf(stderr, "[meter] Recorded: key=%s op=%s bytes=%ld cost=$%.4f\n", key, op, bytes, cost);
    return 0;
}

static int cmd_usage(const char *db_path, const char *key, const char *since) {
    sqlite3 *db = db_open(db_path);
    if (!db) return 1;
    ensure_schema(db);

    sqlite3_stmt *stmt;
    if (since && since[0]) {
        sqlite3_prepare_v2(db,
            "SELECT op, COUNT(*) as invocations, SUM(bytes) as total_bytes, SUM(cost) as total_cost "
            "FROM events WHERE key_id=? AND timestamp >= ? GROUP BY op ORDER BY total_cost DESC;",
            -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, since, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT op, COUNT(*) as invocations, SUM(bytes) as total_bytes, SUM(cost) as total_cost "
            "FROM events WHERE key_id=? GROUP BY op ORDER BY total_cost DESC;",
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
        "SELECT COUNT(*) as total_ops, SUM(bytes) as total_bytes, SUM(cost) as total_cost FROM events;",
        print_row_cb, &first, NULL);

    sqlite3_close(db);
    return 0;
}

static int cmd_invoice(const char *db_path, const char *key, const char *period) {
    sqlite3 *db = db_open(db_path);
    if (!db) return 1;
    ensure_schema(db);
    char ts[64]; iso_timestamp(ts, sizeof(ts));

    printf("╔══════════════════════════════════════╗\n");
    printf("║         BONFYRE INVOICE              ║\n");
    printf("╠══════════════════════════════════════╣\n");
    printf("║ Key:      %-26s ║\n", key);
    printf("║ Period:   %-26s ║\n", period ? period : "all-time");
    printf("║ Generated: %-25s ║\n", ts);
    printf("╠══════════════════════════════════════╣\n");

    sqlite3_stmt *stmt;
    if (period && period[0]) {
        sqlite3_prepare_v2(db,
            "SELECT op || ' x' || COUNT(*) || ' = $' || printf('%.4f', SUM(cost)) "
            "FROM events WHERE key_id=? AND timestamp LIKE ? GROUP BY op;",
            -1, &stmt, NULL);
        char like[128];
        snprintf(like, sizeof(like), "%s%%", period);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, like, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT op || ' x' || COUNT(*) || ' = $' || printf('%.4f', SUM(cost)) "
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
    sqlite3_exec(db,
        "SELECT '  TOTAL: $' || printf('%.2f', SUM(cost)) FROM events;",
        print_plain_cb, &dummy, NULL);
    printf("╚══════════════════════════════════════╝\n");

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

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    const char *db = "bonfyre-meter.db";
    for (int i = 1; i < argc - 1; i++)
        if (strcmp(argv[i], "--db") == 0) db = argv[i+1];

    if (argc >= 2 && strcmp(argv[1], "record") == 0) {
        const char *key = NULL, *op = NULL;
        long bytes = 0; int dur = 0;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--key") == 0) key = argv[i+1];
            if (strcmp(argv[i], "--op") == 0) op = argv[i+1];
            if (strcmp(argv[i], "--bytes") == 0) bytes = atol(argv[i+1]);
            if (strcmp(argv[i], "--duration") == 0) dur = atoi(argv[i+1]);
        }
        if (!key || !op) { fprintf(stderr, "record requires --key and --op\n"); return 1; }
        return cmd_record(db, key, op, bytes, dur);
    }
    if (argc >= 2 && strcmp(argv[1], "usage") == 0) {
        const char *key = NULL, *since = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--key") == 0) key = argv[i+1];
            if (strcmp(argv[i], "--since") == 0) since = argv[i+1];
        }
        if (!key) { fprintf(stderr, "usage requires --key\n"); return 1; }
        return cmd_usage(db, key, since);
    }
    if (argc >= 2 && strcmp(argv[1], "invoice") == 0) {
        const char *key = NULL, *period = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--key") == 0) key = argv[i+1];
            if (strcmp(argv[i], "--period") == 0) period = argv[i+1];
        }
        if (!key) { fprintf(stderr, "invoice requires --key\n"); return 1; }
        return cmd_invoice(db, key, period);
    }
    if (argc >= 2 && strcmp(argv[1], "top") == 0) {
        int limit = 10;
        for (int i = 2; i < argc - 1; i++)
            if (strcmp(argv[i], "--limit") == 0) limit = atoi(argv[i+1]);
        return cmd_top(db, limit);
    }

    fprintf(stderr,
        "BonfyreMeter — usage metering & billing\n\n"
        "  bonfyre-meter record --key K --op OP [--bytes N] [--duration MS]\n"
        "  bonfyre-meter usage --key K [--since DATE]\n"
        "  bonfyre-meter invoice --key K [--period YYYY-MM]\n"
        "  bonfyre-meter top [--limit N]\n"
    );
    return 1;
}
