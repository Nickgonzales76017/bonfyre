/*
 * Native evidence relations. Shares the evidence_relations store with the Python
 * reference (evidence_graphs.py); relations are directional and kind-gated.
 */

#include "bf_evidence.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>

struct BfEvidence {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Only the evidence_relations table -- the shared store's other tables are
 * created by whichever peer touches them; this must match evidence_graphs.py. */
static const char *EVIDENCE_SCHEMA =
    "CREATE TABLE IF NOT EXISTS evidence_relations("
    "  evidence TEXT NOT NULL, kind TEXT NOT NULL, claim TEXT NOT NULL,"
    "  PRIMARY KEY(evidence, kind, claim)"
    ");";

static const char *EVIDENCE_KINDS[] = {
    "supports", "contradicts", "reproduces", "falsifies", NULL,
};

static int kind_declared(const char *kind) {
    for (size_t i = 0; EVIDENCE_KINDS[i] != NULL; i++) {
        if (strcmp(EVIDENCE_KINDS[i], kind) == 0) {
            return 1;
        }
    }
    return 0;
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *errmsg = NULL;
    if (sqlite3_exec(db, EVIDENCE_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "evidence: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    return 0;
}

int bf_evidence_open(const char *database_path, BfEvidence **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "evidence: cannot open %s: %s\n", database_path,
                    db ? sqlite3_errmsg(db) : "?");
        }
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) {
        sqlite3_close(db);
        return -1;
    }
    BfEvidence *graph = calloc(1, sizeof(*graph));
    if (!graph) {
        sqlite3_close(db);
        return -1;
    }
    graph->db = db;
    graph->owns_db = 1;
    graph->err = err;
    *out = graph;
    return 0;
}

int bf_evidence_open_database(void *sqlite_database, BfEvidence **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfEvidence *graph = calloc(1, sizeof(*graph));
    if (!graph) {
        return -1;
    }
    graph->db = db;
    graph->owns_db = 0;
    graph->err = err;
    *out = graph;
    return 0;
}

void bf_evidence_close(BfEvidence *graph) {
    if (!graph) {
        return;
    }
    if (graph->owns_db && graph->db) {
        sqlite3_close(graph->db);
    }
    free(graph);
}

BfEvidenceStatus bf_evidence_relate(BfEvidence *graph, const char *evidence,
                                    const char *kind, const char *claim) {
    if (!graph || !evidence || evidence[0] == '\0' || !claim || claim[0] == '\0' ||
        !kind || kind[0] == '\0') {
        return BF_EVIDENCE_INVALID;
    }
    if (!kind_declared(kind)) {
        return BF_EVIDENCE_INVALID_KIND;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            graph->db,
            "INSERT OR REPLACE INTO evidence_relations(evidence,kind,claim) VALUES(?,?,?)",
            -1, &stmt, NULL) != SQLITE_OK) {
        return BF_EVIDENCE_STORAGE_ERROR;
    }
    sqlite3_bind_text(stmt, 1, evidence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, claim, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? BF_EVIDENCE_OK : BF_EVIDENCE_STORAGE_ERROR;
}

BfEvidenceStatus bf_evidence_retract(BfEvidence *graph, const char *evidence,
                                     const char *kind, const char *claim) {
    if (!graph || !evidence || evidence[0] == '\0' || !claim || claim[0] == '\0' ||
        !kind || kind[0] == '\0') {
        return BF_EVIDENCE_INVALID;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            graph->db,
            "DELETE FROM evidence_relations WHERE evidence=? AND kind=? AND claim=?",
            -1, &stmt, NULL) != SQLITE_OK) {
        return BF_EVIDENCE_STORAGE_ERROR;
    }
    sqlite3_bind_text(stmt, 1, evidence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, claim, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? BF_EVIDENCE_OK : BF_EVIDENCE_STORAGE_ERROR;
}

int bf_evidence_supports(BfEvidence *graph, const char *evidence, const char *claim) {
    if (!graph || !evidence || !claim) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int result = -1;
    if (sqlite3_prepare_v2(
            graph->db,
            "SELECT 1 FROM evidence_relations WHERE evidence=? AND claim=? AND kind='supports'",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, evidence, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, claim, -1, SQLITE_STATIC);
        result = (sqlite3_step(stmt) == SQLITE_ROW) ? 1 : 0;
    }
    sqlite3_finalize(stmt);
    return result;
}

int64_t bf_evidence_supporters_count(BfEvidence *graph, const char *claim) {
    if (!graph || !claim) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int64_t n = -1;
    if (sqlite3_prepare_v2(
            graph->db,
            "SELECT COUNT(DISTINCT evidence) FROM evidence_relations"
            " WHERE claim=? AND kind='supports'",
            -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, claim, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            n = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return n;
}
