/* PatternGraph.  See bf_pattern.h. */
#include "bf_pattern.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static const char *PATTERN_SCHEMA =
    "CREATE TABLE IF NOT EXISTS pattern_clusters(cluster_id TEXT PRIMARY KEY, statement TEXT NOT NULL,"
    " evidence_refs TEXT NOT NULL DEFAULT '[]', status TEXT NOT NULL DEFAULT 'observed');"
    "CREATE TABLE IF NOT EXISTS pattern_members(cluster_id TEXT NOT NULL REFERENCES pattern_clusters(cluster_id),"
    " trace_ref TEXT NOT NULL, PRIMARY KEY(cluster_id, trace_ref));"
    "CREATE TABLE IF NOT EXISTS candidate_primitives(candidate_id TEXT PRIMARY KEY,"
    " cluster_id TEXT NOT NULL REFERENCES pattern_clusters(cluster_id), statement TEXT NOT NULL,"
    " status TEXT NOT NULL DEFAULT 'candidate', proof_ref TEXT NOT NULL DEFAULT '');";

static int present(const char *s) { return s && *s; }

int bf_pattern_open(void *db) {
    return db && sqlite3_exec((sqlite3 *)db, PATTERN_SCHEMA, NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}

static int step_done(sqlite3_stmt *st) { int rc = sqlite3_step(st); sqlite3_finalize(st); return rc == SQLITE_DONE; }

BfPatternStatus bf_pattern_record_cluster(void *vdb, const char *id, const char *statement,
                                          const char *const *traces, size_t nt,
                                          const char *const *evidence, size_t ne, const char *status) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL;
    if (!db || !present(id) || !present(statement) || !nt || !ne) return BF_PATTERN_INVALID;
    const char *s = present(status) ? status : "observed";
    if (strcmp(s, "observed") && strcmp(s, "challenged")) return BF_PATTERN_STATUS;
    for (size_t i = 0; i < nt; i++) if (!present(traces[i])) return BF_PATTERN_INVALID;
    for (size_t i = 0; i < ne; i++) if (!present(evidence[i])) return BF_PATTERN_INVALID;
    if (bf_pattern_open(db) || sqlite3_exec(db, "SAVEPOINT pattern", NULL, NULL, NULL) != SQLITE_OK) return BF_PATTERN_STORAGE;
    int ok = 1;
    /* evidence_refs as a JSON array built by SQLite itself */
    char *refs = sqlite3_mprintf("[]");
    for (size_t i = 0; ok && i < ne; i++) {
        if (sqlite3_prepare_v2(db, "SELECT json_insert(?1, '$[#]', ?2)", -1, &st, NULL) != SQLITE_OK) { ok = 0; break; }
        sqlite3_bind_text(st, 1, refs, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, evidence[i], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { char *n = sqlite3_mprintf("%s", sqlite3_column_text(st, 0)); sqlite3_free(refs); refs = n; } else ok = 0;
        sqlite3_finalize(st);
    }
    if (ok && sqlite3_prepare_v2(db, "INSERT INTO pattern_clusters(cluster_id,statement,evidence_refs,status) VALUES(?1,?2,?3,?4) "
            "ON CONFLICT(cluster_id) DO UPDATE SET statement=excluded.statement,evidence_refs=excluded.evidence_refs,status=excluded.status",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, statement, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, refs, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, s, -1, SQLITE_TRANSIENT);
        ok = step_done(st);
    } else ok = 0;
    sqlite3_free(refs);
    for (size_t i = 0; ok && i < nt; i++) {
        if (sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO pattern_members(cluster_id,trace_ref) VALUES(?1,?2)", -1, &st, NULL) != SQLITE_OK) { ok = 0; break; }
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, traces[i], -1, SQLITE_TRANSIENT);
        ok = step_done(st);
    }
    sqlite3_exec(db, ok ? "RELEASE pattern" : "ROLLBACK TO pattern; RELEASE pattern", NULL, NULL, NULL);
    return ok ? BF_PATTERN_OK : BF_PATTERN_STORAGE;
}

BfPatternStatus bf_pattern_propose(void *vdb, const char *cand, const char *cluster, const char *statement) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL;
    if (!db || !present(cand) || !present(cluster) || !present(statement)) return BF_PATTERN_INVALID;
    if (bf_pattern_open(db)) return BF_PATTERN_STORAGE;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM pattern_clusters WHERE cluster_id=?1", -1, &st, NULL) != SQLITE_OK) return BF_PATTERN_STORAGE;
    sqlite3_bind_text(st, 1, cluster, -1, SQLITE_TRANSIENT);
    int known = sqlite3_step(st) == SQLITE_ROW; sqlite3_finalize(st);
    if (!known) return BF_PATTERN_UNKNOWN_CLUSTER;
    /* A re-proposal never lifts or resets a status set elsewhere. */
    if (sqlite3_prepare_v2(db, "INSERT INTO candidate_primitives(candidate_id,cluster_id,statement,status) VALUES(?1,?2,?3,'candidate') "
            "ON CONFLICT(candidate_id) DO UPDATE SET cluster_id=excluded.cluster_id,statement=excluded.statement", -1, &st, NULL) != SQLITE_OK)
        return BF_PATTERN_STORAGE;
    sqlite3_bind_text(st, 1, cand, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, cluster, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, statement, -1, SQLITE_TRANSIENT);
    return step_done(st) ? BF_PATTERN_OK : BF_PATTERN_STORAGE;
}

int bf_pattern_candidate_status(void *vdb, const char *cand, char *out, size_t cap) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL;
    if (!db || !present(cand) || !out || !cap || bf_pattern_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT status FROM candidate_primitives WHERE candidate_id=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, cand, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st), found = rc == SQLITE_ROW;
    if (found) snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0)); else out[0] = 0;
    sqlite3_finalize(st);
    return (found || rc == SQLITE_DONE) ? found : -1;
}

int64_t bf_pattern_cluster_traces(void *vdb, const char *cluster, BfPatternVisit visit, void *ctx) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL; int64_t n = 0;
    if (!db || !present(cluster) || bf_pattern_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT trace_ref FROM pattern_members WHERE cluster_id=?1 ORDER BY trace_ref", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, cluster, -1, SQLITE_TRANSIENT);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) { n++; if (visit && visit((const char *)sqlite3_column_text(st, 0), ctx)) { rc = SQLITE_DONE; break; } }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? n : -1;
}
