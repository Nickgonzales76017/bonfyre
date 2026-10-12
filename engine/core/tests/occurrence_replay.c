/* Replay driver: fold every pending occurrence in the given control-plane db
 * through the native projection, then print the resulting projection rows so a
 * harness can diff them against the Python reference over the SAME real data. */

#include "bf_occurrence.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: occurrence_replay <db_path> [now_iso]\n");
        return 2;
    }
    const char *db_path = argv[1];
    const char *now = argc > 2 ? argv[2] : "2026-08-16T00:00:00+00:00";

    BfOccurrenceSpine *spine = NULL;
    if (bf_occurrence_open(db_path, &spine, stderr) != 0) {
        return 1;
    }
    int64_t folded = bf_occurrence_project(spine, now);
    if (folded < 0) {
        fprintf(stderr, "replay: fold failed\n");
        bf_occurrence_close(spine);
        return 1;
    }
    fprintf(stderr, "native folded=%lld\n", (long long)folded);
    bf_occurrence_close(spine);

    /* Print projection rows deterministically for the diff. */
    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        return 1;
    }
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db,
                       "SELECT event_id,actor,event_kind,status FROM occurrence_projection"
                       " ORDER BY event_id",
                       -1, &stmt, NULL);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        printf("%lld|%s|%s|%s\n", (long long)sqlite3_column_int64(stmt, 0),
               sqlite3_column_text(stmt, 1), sqlite3_column_text(stmt, 2),
               sqlite3_column_text(stmt, 3));
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}
