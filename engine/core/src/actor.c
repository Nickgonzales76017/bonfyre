/*
 * Native ActorGraph. Shares the actor_nodes / actor_edges store with the Python
 * reference (actors.py) and enforces the same epistemic gates: declared kinds,
 * a mandatory provenance, and a confidence tier that never collapses.
 */

#include "bf_actor.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfActorGraph {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Must match actors.py SCHEMA exactly. */
static const char *ACTOR_SCHEMA =
    "CREATE TABLE IF NOT EXISTS actor_nodes("
    "  actor_id TEXT PRIMARY KEY,"
    "  node_kind TEXT NOT NULL,"
    "  display_name TEXT NOT NULL,"
    "  role TEXT NOT NULL DEFAULT '',"
    "  org_id TEXT,"
    "  confidence TEXT NOT NULL DEFAULT 'asserted',"
    "  provenance TEXT NOT NULL,"
    "  detail TEXT NOT NULL DEFAULT '',"
    "  created_at TEXT NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS actor_edges("
    "  from_id TEXT NOT NULL REFERENCES actor_nodes(actor_id),"
    "  edge_kind TEXT NOT NULL,"
    "  to_id TEXT NOT NULL REFERENCES actor_nodes(actor_id),"
    "  confidence TEXT NOT NULL DEFAULT 'asserted',"
    "  provenance TEXT NOT NULL,"
    "  detail TEXT NOT NULL DEFAULT '',"
    "  created_at TEXT NOT NULL,"
    "  PRIMARY KEY(from_id, edge_kind, to_id)"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_actor_edges_kind ON actor_edges(edge_kind, to_id);";

static const char *NODE_KINDS[] = {
    "organization", "person", "publication", "program", NULL,
};
static const char *EDGE_KINDS[] = {
    "employs", "authority_over", "evidence_for", "funds",
    "publishes_in", "contact_of", "opportunity_unlock", NULL,
};
static const char *CONFIDENCE_LEVELS[] = {
    "verified", "asserted", "inferred", NULL,
};

static int in_set(const char *const *set, const char *v) {
    for (size_t i = 0; set[i] != NULL; i++) {
        if (strcmp(set[i], v) == 0) {
            return 1;
        }
    }
    return 0;
}

static void now_iso(char out[40]) {
    time_t t = time(NULL);
    struct tm utc;
#if defined(_WIN32)
    gmtime_s(&utc, &t);
#else
    gmtime_r(&t, &utc);
#endif
    if (strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc) == 0) {
        snprintf(out, 40, "1970-01-01T00:00:00+00:00");
    }
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *errmsg = NULL;
    if (sqlite3_exec(db, ACTOR_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "actor: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    return 0;
}

int bf_actor_open(const char *database_path, BfActorGraph **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "actor: cannot open %s: %s\n", database_path,
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
    BfActorGraph *graph = calloc(1, sizeof(*graph));
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

int bf_actor_open_database(void *sqlite_database, BfActorGraph **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfActorGraph *graph = calloc(1, sizeof(*graph));
    if (!graph) {
        return -1;
    }
    graph->db = db;
    graph->owns_db = 0;
    graph->err = err;
    *out = graph;
    return 0;
}

void bf_actor_close(BfActorGraph *graph) {
    if (!graph) {
        return;
    }
    if (graph->owns_db && graph->db) {
        sqlite3_close(graph->db);
    }
    free(graph);
}

BfActorStatus bf_actor_upsert(BfActorGraph *graph, const BfActorNodeSpec *spec, const char *now) {
    if (!graph || !spec) {
        return BF_ACTOR_INVALID;
    }
    if (!spec->actor_id || spec->actor_id[0] == '\0' ||
        !spec->node_kind || spec->node_kind[0] == '\0' ||
        !spec->display_name || spec->display_name[0] == '\0') {
        return BF_ACTOR_INVALID;
    }
    if (!in_set(NODE_KINDS, spec->node_kind)) {
        return BF_ACTOR_INVALID_KIND;
    }
    const char *confidence = (spec->confidence && spec->confidence[0]) ? spec->confidence : "asserted";
    if (!in_set(CONFIDENCE_LEVELS, confidence)) {
        return BF_ACTOR_INVALID_CONFIDENCE;
    }
    if (!spec->provenance || spec->provenance[0] == '\0') {
        return BF_ACTOR_MISSING_PROVENANCE;
    }

    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            graph->db,
            "INSERT INTO actor_nodes"
            "(actor_id,node_kind,display_name,role,org_id,confidence,provenance,detail,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(actor_id) DO UPDATE SET"
            "  display_name=excluded.display_name, role=excluded.role,"
            "  org_id=excluded.org_id, confidence=excluded.confidence,"
            "  provenance=excluded.provenance, detail=excluded.detail",
            -1, &stmt, NULL) != SQLITE_OK) {
        return BF_ACTOR_STORAGE_ERROR;
    }
    sqlite3_bind_text(stmt, 1, spec->actor_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->node_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, spec->display_name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, spec->role ? spec->role : "", -1, SQLITE_TRANSIENT);
    if (spec->org_id) {
        sqlite3_bind_text(stmt, 5, spec->org_id, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, 5);
    }
    sqlite3_bind_text(stmt, 6, confidence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, spec->provenance, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 8, spec->detail ? spec->detail : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 9, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? BF_ACTOR_OK : BF_ACTOR_STORAGE_ERROR;
}

BfActorStatus bf_actor_add_edge(BfActorGraph *graph, const BfActorEdgeSpec *spec, const char *now) {
    if (!graph || !spec) {
        return BF_ACTOR_INVALID;
    }
    if (!spec->from_id || spec->from_id[0] == '\0' ||
        !spec->to_id || spec->to_id[0] == '\0' ||
        !spec->edge_kind || spec->edge_kind[0] == '\0') {
        return BF_ACTOR_INVALID;
    }
    if (!in_set(EDGE_KINDS, spec->edge_kind)) {
        return BF_ACTOR_INVALID_KIND;
    }
    const char *confidence = (spec->confidence && spec->confidence[0]) ? spec->confidence : "asserted";
    if (!in_set(CONFIDENCE_LEVELS, confidence)) {
        return BF_ACTOR_INVALID_CONFIDENCE;
    }
    if (!spec->provenance || spec->provenance[0] == '\0') {
        return BF_ACTOR_MISSING_PROVENANCE;
    }

    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            graph->db,
            "INSERT INTO actor_edges"
            "(from_id,edge_kind,to_id,confidence,provenance,detail,created_at)"
            " VALUES(?,?,?,?,?,?,?)"
            " ON CONFLICT(from_id,edge_kind,to_id) DO UPDATE SET"
            "  confidence=excluded.confidence, provenance=excluded.provenance,"
            "  detail=excluded.detail",
            -1, &stmt, NULL) != SQLITE_OK) {
        return BF_ACTOR_STORAGE_ERROR;
    }
    sqlite3_bind_text(stmt, 1, spec->from_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->edge_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, spec->to_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, confidence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, spec->provenance, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, spec->detail ? spec->detail : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc == SQLITE_DONE) {
        return BF_ACTOR_OK;
    }
    if (rc == SQLITE_CONSTRAINT) {
        /* Foreign key: an endpoint actor does not exist. We cannot relate actors
         * we have never recorded. */
        return BF_ACTOR_UNKNOWN_ENDPOINT;
    }
    return BF_ACTOR_STORAGE_ERROR;
}

int64_t bf_actor_unverified_count(BfActorGraph *graph) {
    if (!graph) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int64_t n = -1;
    if (sqlite3_prepare_v2(graph->db,
                           "SELECT COUNT(*) FROM actor_nodes WHERE confidence!='verified'",
                           -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            n = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return n;
}

int64_t bf_actor_neighbour_count(BfActorGraph *graph, const char *actor_id, const char *edge_kind) {
    if (!graph || !actor_id) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql = edge_kind
        ? "SELECT COUNT(*) FROM actor_edges WHERE (from_id=? OR to_id=?) AND edge_kind=?"
        : "SELECT COUNT(*) FROM actor_edges WHERE from_id=? OR to_id=?";
    int64_t n = -1;
    if (sqlite3_prepare_v2(graph->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, actor_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, actor_id, -1, SQLITE_STATIC);
        if (edge_kind) {
            sqlite3_bind_text(stmt, 3, edge_kind, -1, SQLITE_STATIC);
        }
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            n = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return n;
}

const char *bf_actor_status_name(BfActorStatus status) {
    switch (status) {
        case BF_ACTOR_OK: return "ok";
        case BF_ACTOR_INVALID_KIND: return "invalid_kind";
        case BF_ACTOR_INVALID_CONFIDENCE: return "invalid_confidence";
        case BF_ACTOR_MISSING_PROVENANCE: return "missing_provenance";
        case BF_ACTOR_UNKNOWN_ENDPOINT: return "unknown_endpoint";
        case BF_ACTOR_INVALID: return "invalid";
        case BF_ACTOR_STORAGE_ERROR: return "storage_error";
    }
    return "unknown";
}
