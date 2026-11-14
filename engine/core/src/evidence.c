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
    ");"
    "CREATE TABLE IF NOT EXISTS artifact_transforms("
    "  derived TEXT NOT NULL, operator TEXT NOT NULL, source TEXT NOT NULL,"
    "  PRIMARY KEY(derived, source));"
    "CREATE TABLE IF NOT EXISTS provenance_edges("
    "  artifact TEXT NOT NULL, role TEXT NOT NULL, node TEXT NOT NULL,"
    "  PRIMARY KEY(artifact, role, node));"
    "CREATE TABLE IF NOT EXISTS temporal_evidence(fact TEXT PRIMARY KEY, planes TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS claim_deps("
    "  claim TEXT NOT NULL, dep_kind TEXT NOT NULL, target TEXT NOT NULL,"
    "  PRIMARY KEY(claim, dep_kind, target));"
    "CREATE TABLE IF NOT EXISTS claim_state(claim TEXT PRIMARY KEY, proof_state TEXT NOT NULL);";

static const char *EVIDENCE_KINDS[] = {
    "supports", "contradicts", "reproduces", "falsifies", NULL,
};

static int present(const char *value) { return value && value[0] != '\0'; }

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

const char *bf_evidence_verdict_name(BfEvidenceVerdict verdict) {
    switch (verdict) {
        case BF_EVIDENCE_UNEVIDENCED: return "unevidenced";
        case BF_EVIDENCE_SUPPORTED: return "supported";
        case BF_EVIDENCE_REPRODUCED: return "reproduced";
        case BF_EVIDENCE_CONTESTED: return "contested";
        case BF_EVIDENCE_FALSIFIED: return "falsified";
    }
    return "unknown";
}

BfEvidenceStatus bf_evidence_verdict(BfEvidence *graph, const char *claim,
                                      BfEvidenceVerdict *out) {
    sqlite3_stmt *stmt = NULL;
    int supports = 0, contradicts = 0, reproduces = 0, falsifies = 0;
    if (!graph || !present(claim) || !out) return BF_EVIDENCE_INVALID;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT kind FROM evidence_relations WHERE claim=?", -1,
            &stmt, NULL) != SQLITE_OK) return BF_EVIDENCE_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, claim, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *kind = (const char *)sqlite3_column_text(stmt, 0);
        if (!kind) continue;
        if (strcmp(kind, "supports") == 0) supports = 1;
        else if (strcmp(kind, "contradicts") == 0) contradicts = 1;
        else if (strcmp(kind, "reproduces") == 0) reproduces = 1;
        else if (strcmp(kind, "falsifies") == 0) falsifies = 1;
    }
    if (sqlite3_errcode(graph->db) != SQLITE_OK && sqlite3_errcode(graph->db) != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        return BF_EVIDENCE_STORAGE_ERROR;
    }
    sqlite3_finalize(stmt);
    if (falsifies) *out = BF_EVIDENCE_FALSIFIED;
    else if (contradicts) *out = BF_EVIDENCE_CONTESTED;
    else if (reproduces) *out = BF_EVIDENCE_REPRODUCED;
    else if (supports) *out = BF_EVIDENCE_SUPPORTED;
    else *out = BF_EVIDENCE_UNEVIDENCED;
    return BF_EVIDENCE_OK;
}

int bf_evidence_stands(BfEvidence *graph, const char *claim) {
    BfEvidenceVerdict verdict;
    if (bf_evidence_verdict(graph, claim, &verdict) != BF_EVIDENCE_OK) return -1;
    return verdict == BF_EVIDENCE_SUPPORTED || verdict == BF_EVIDENCE_REPRODUCED;
}

/* ---- ArtifactGraph, ProvenanceGraph, TemporalEvidenceGraph, ClaimGraph ---- */

static BfEvidenceStatus exec3(BfEvidence *g, const char *sql, const char *a, const char *b, const char *c) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(g->db, sql, -1, &st, NULL) != SQLITE_OK) return BF_EVIDENCE_STORAGE_ERROR;
    sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT);
    if (b) sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT);
    if (c) sqlite3_bind_text(st, 3, c, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? BF_EVIDENCE_OK : BF_EVIDENCE_STORAGE_ERROR;
}

/* Rows of (col0, col1) for one key; visit each. Count or -1. */
static int64_t visit_pairs(BfEvidence *g, const char *sql, const char *key, BfEvidenceVisit visit, void *ctx) {
    sqlite3_stmt *st = NULL; int64_t n = 0;
    if (!g || !present(key) || sqlite3_prepare_v2(g->db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        n++;
        if (visit && visit((const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1), ctx)) { rc = SQLITE_DONE; break; }
    }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? n : -1;
}

BfEvidenceStatus bf_evidence_record_transform(BfEvidence *graph, const char *source,
                                              const char *operator_name, const char *derived) {
    if (!graph || !present(source) || !present(operator_name) || !present(derived)) return BF_EVIDENCE_INVALID;
    return exec3(graph, "INSERT OR REPLACE INTO artifact_transforms(derived,operator,source) VALUES(?1,?2,?3)",
                 derived, operator_name, source);
}

int64_t bf_evidence_lineage(BfEvidence *graph, const char *derived, BfEvidenceVisit visit, void *ctx) {
    if (!graph || !present(derived)) return -1;
    /* A recursive CTE with UNION visits each ancestor once even through a cycle. */
    sqlite3_stmt *st = NULL; int64_t n = 0;
    if (sqlite3_prepare_v2(graph->db,
            "WITH RECURSIVE up(a) AS (SELECT source FROM artifact_transforms WHERE derived=?1 "
            "UNION SELECT t.source FROM artifact_transforms t JOIN up ON t.derived=up.a) "
            "SELECT a, '' FROM up WHERE a<>?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, derived, -1, SQLITE_TRANSIENT);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        n++;
        if (visit && visit((const char *)sqlite3_column_text(st, 0), "", ctx)) { rc = SQLITE_DONE; break; }
    }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? n : -1;
}

BfEvidenceStatus bf_evidence_record_provenance(BfEvidence *graph, const char *artifact,
                                               const char *role, const char *node) {
    if (!graph || !present(artifact) || !present(role) || !present(node)) return BF_EVIDENCE_INVALID;
    return exec3(graph, "INSERT OR REPLACE INTO provenance_edges(artifact,role,node) VALUES(?1,?2,?3)", artifact, role, node);
}

int64_t bf_evidence_provenance_path(BfEvidence *graph, const char *artifact, BfEvidenceVisit visit, void *ctx) {
    return visit_pairs(graph, "SELECT role,node FROM provenance_edges WHERE artifact=?1 ORDER BY role,node", artifact, visit, ctx);
}

typedef struct { const char *const *bad; size_t n; int hit; } AdmitCtx;
static int admit_visit(const char *role, const char *node, void *ctx) {
    AdmitCtx *a = ctx; (void)role;
    for (size_t i = 0; i < a->n; i++) if (a->bad[i] && strcmp(a->bad[i], node) == 0) { a->hit = 1; return 1; }
    return 0;
}

int bf_evidence_path_admits(BfEvidence *graph, const char *artifact,
                            const char *const *untrusted, size_t untrusted_count) {
    AdmitCtx a = {untrusted, untrusted_count, 0};
    if (bf_evidence_provenance_path(graph, artifact, admit_visit, &a) < 0) return -1;
    return a.hit ? 0 : 1;
}

static const char *TIME_PLANES[] = {
    "event_time", "publication_time", "observation_time", "retrieval_time",
    "effective_time", "supersession_time", "correction_time", "expiration_time", NULL,
};

int bf_evidence_time_plane_known(const char *plane) {
    for (size_t i = 0; plane && TIME_PLANES[i]; i++) if (strcmp(TIME_PLANES[i], plane) == 0) return 1;
    return 0;
}

BfEvidenceStatus bf_evidence_record_time(BfEvidence *graph, const char *fact, const char *plane, const char *at) {
    if (!graph || !present(fact) || !present(plane) || !present(at)) return BF_EVIDENCE_INVALID;
    if (!bf_evidence_time_plane_known(plane)) return BF_EVIDENCE_INVALID_KIND;
    char path[64]; snprintf(path, sizeof path, "$.%s", plane);
    return exec3(graph,
        "INSERT INTO temporal_evidence(fact,planes) VALUES(?1, json_object(substr(?2,3), ?3)) "
        "ON CONFLICT(fact) DO UPDATE SET planes=json_set(planes, ?2, ?3)", fact, path, at);
}

int bf_evidence_time_of(BfEvidence *graph, const char *fact, const char *plane, char *out, size_t cap) {
    if (!graph || !present(fact) || !bf_evidence_time_plane_known(plane) || !out || !cap) return -1;
    sqlite3_stmt *st = NULL; char path[64]; snprintf(path, sizeof path, "$.%s", plane);
    if (sqlite3_prepare_v2(graph->db, "SELECT json_extract(planes, ?2) FROM temporal_evidence WHERE fact=?1",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, fact, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    int found = 0, rc = sqlite3_step(st);
    if (rc == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0)); found = 1;
    } else out[0] = '\0';
    sqlite3_finalize(st);
    return (rc == SQLITE_ROW || rc == SQLITE_DONE) ? found : -1;
}

int bf_evidence_is_stale(BfEvidence *graph, const char *fact, const char *at) {
    char exp[128];
    if (!present(at)) return -1;
    int got = bf_evidence_time_of(graph, fact, "expiration_time", exp, sizeof exp);
    if (got < 0) return -1;
    return got && strcmp(at, exp) > 0;
}

BfEvidenceStatus bf_evidence_add_claim(BfEvidence *graph, const char *claim,
                                       const char *const *depends_on, size_t depends_count,
                                       const char *const *counters, size_t counters_count,
                                       const char *proof_state) {
    if (!graph || !present(claim)) return BF_EVIDENCE_INVALID;
    for (size_t i = 0; i < depends_count; i++) if (!present(depends_on[i])) return BF_EVIDENCE_INVALID;
    for (size_t i = 0; i < counters_count; i++) if (!present(counters[i])) return BF_EVIDENCE_INVALID;
    if (sqlite3_exec(graph->db, "SAVEPOINT claim", NULL, NULL, NULL) != SQLITE_OK) return BF_EVIDENCE_STORAGE_ERROR;
    BfEvidenceStatus s = BF_EVIDENCE_OK;
    const char *sql = "INSERT OR REPLACE INTO claim_deps(claim,dep_kind,target) VALUES(?1,?2,?3)";
    for (size_t i = 0; s == BF_EVIDENCE_OK && i < depends_count; i++) s = exec3(graph, sql, claim, "depends_on", depends_on[i]);
    for (size_t i = 0; s == BF_EVIDENCE_OK && i < counters_count; i++) s = exec3(graph, sql, claim, "counter", counters[i]);
    if (s == BF_EVIDENCE_OK)
        s = exec3(graph, "INSERT OR REPLACE INTO claim_state(claim,proof_state) VALUES(?1,?2)", claim,
                  present(proof_state) ? proof_state : "open", NULL);
    sqlite3_exec(graph->db, s == BF_EVIDENCE_OK ? "RELEASE claim" : "ROLLBACK TO claim; RELEASE claim", NULL, NULL, NULL);
    return s;
}

int64_t bf_evidence_claim_dependencies(BfEvidence *graph, const char *claim, BfEvidenceVisit visit, void *ctx) {
    return visit_pairs(graph, "SELECT dep_kind,target FROM claim_deps WHERE claim=?1 ORDER BY dep_kind,target", claim, visit, ctx);
}

int bf_evidence_proof_state(BfEvidence *graph, const char *claim, char *out, size_t cap) {
    if (!graph || !present(claim) || !out || !cap) return -1;
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(graph->db, "SELECT proof_state FROM claim_state WHERE claim=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, claim, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st), found = 0;
    if (rc == SQLITE_ROW) { snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0)); found = 1; } else out[0] = '\0';
    sqlite3_finalize(st);
    return (rc == SQLITE_ROW || rc == SQLITE_DONE) ? found : -1;
}
