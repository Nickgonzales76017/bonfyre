/* Money, non-cash value and mission cost relations.  See bf_economic_graph.h. */
#include "bf_economic_graph.h"

#include <sqlite3.h>
#include <string.h>

static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS money_edges(src TEXT NOT NULL, relation TEXT NOT NULL, dst TEXT NOT NULL, amount REAL,"
    " PRIMARY KEY(src, relation, dst));"
    "CREATE TABLE IF NOT EXISTS value_edges(src TEXT NOT NULL, kind TEXT NOT NULL, dst TEXT NOT NULL, PRIMARY KEY(src, kind, dst));"
    "CREATE TABLE IF NOT EXISTS mission_costs(mission TEXT NOT NULL, resource TEXT NOT NULL, amount REAL NOT NULL,"
    " booked INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(mission, resource));";

static const char *MONEY[] = {"funds", "pays", "owes", "grants", "subsidizes", "costs", "earns", NULL};
static const char *VALUE[] = {"unlocks", "prefills", "increases_eligibility", "strengthens_evidence", "decreases_cost",
    "decreases_time", "creates_capacity", "increases_distribution", "creates_relationship_option", "creates_learning_option", NULL};

static int present(const char *s) { return s && *s; }
static int in(const char *const *set, const char *v) { for (size_t i = 0; v && set[i]; i++) if (!strcmp(set[i], v)) return 1; return 0; }

int bf_econ_open(void *db) { return db && sqlite3_exec(db, SCHEMA, NULL, NULL, NULL) == SQLITE_OK ? 0 : -1; }

static BfEconStatus done(sqlite3_stmt *st) { int rc = sqlite3_step(st); sqlite3_finalize(st); return rc == SQLITE_DONE ? BF_ECON_OK : BF_ECON_STORAGE; }

BfEconStatus bf_econ_record_money(void *vdb, const char *src, const char *rel, const char *dst, double amount, int has) {
    sqlite3 *db = vdb; sqlite3_stmt *st;
    if (!db || !present(src) || !present(dst) || !present(rel)) return BF_ECON_INVALID;
    if (!in(MONEY, rel)) return BF_ECON_UNKNOWN_KIND;
    if (bf_econ_open(db) || sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO money_edges(src,relation,dst,amount) VALUES(?1,?2,?3,?4)", -1, &st, NULL) != SQLITE_OK) return BF_ECON_STORAGE;
    sqlite3_bind_text(st, 1, src, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, rel, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, dst, -1, SQLITE_TRANSIENT);
    if (has) sqlite3_bind_double(st, 4, amount); else sqlite3_bind_null(st, 4);
    return done(st);
}

int64_t bf_econ_money_out(void *vdb, const char *src, BfEconVisit visit, void *ctx) {
    sqlite3 *db = vdb; sqlite3_stmt *st; int64_t n = 0; int rc;
    if (!db || !present(src) || bf_econ_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT relation,dst,amount FROM money_edges WHERE src=?1 ORDER BY relation,dst", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, src, -1, SQLITE_TRANSIENT);
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        n++; int has = sqlite3_column_type(st, 2) != SQLITE_NULL;
        if (visit && visit((const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1), has ? sqlite3_column_double(st, 2) : 0, has, ctx)) { rc = SQLITE_DONE; break; }
    }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? n : -1;
}

BfEconStatus bf_econ_record_value(void *vdb, const char *src, const char *kind, const char *dst) {
    sqlite3 *db = vdb; sqlite3_stmt *st;
    if (!db || !present(src) || !present(dst) || !present(kind)) return BF_ECON_INVALID;
    if (!in(VALUE, kind)) return BF_ECON_UNKNOWN_KIND; /* value is not cash */
    if (bf_econ_open(db) || sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO value_edges(src,kind,dst) VALUES(?1,?2,?3)", -1, &st, NULL) != SQLITE_OK) return BF_ECON_STORAGE;
    sqlite3_bind_text(st, 1, src, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, kind, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 3, dst, -1, SQLITE_TRANSIENT);
    return done(st);
}

int64_t bf_econ_value_reachable(void *vdb, const char *src, BfEconVisit visit, void *ctx) {
    sqlite3 *db = vdb; sqlite3_stmt *st; int64_t n = 0; int rc;
    if (!db || !present(src) || bf_econ_open(db)) return -1;
    /* Each destination once (first edge that reached it), cycles terminate. */
    if (sqlite3_prepare_v2(db,
            "WITH RECURSIVE r(kind,dst,depth) AS (SELECT kind,dst,1 FROM value_edges WHERE src=?1 "
            "UNION SELECT v.kind,v.dst,r.depth+1 FROM value_edges v JOIN r ON v.src=r.dst WHERE r.depth<64) "
            "SELECT kind,dst FROM r WHERE dst<>?1 GROUP BY dst HAVING depth=MIN(depth) ORDER BY MIN(depth),dst",
            -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, src, -1, SQLITE_TRANSIENT);
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        n++;
        if (visit && visit((const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1), 0, 0, ctx)) { rc = SQLITE_DONE; break; }
    }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? n : -1;
}

BfEconStatus bf_econ_record_cost(void *vdb, const char *mission, const char *resource, double amount) {
    sqlite3 *db = vdb; sqlite3_stmt *st;
    if (!db || !present(mission) || !present(resource)) return BF_ECON_INVALID;
    if (bf_econ_open(db) || sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO mission_costs(mission,resource,amount,booked) VALUES(?1,?2,?3,0)", -1, &st, NULL) != SQLITE_OK) return BF_ECON_STORAGE;
    sqlite3_bind_text(st, 1, mission, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, resource, -1, SQLITE_TRANSIENT); sqlite3_bind_double(st, 3, amount);
    return done(st);
}

int bf_econ_mission_cost(void *vdb, const char *mission, double *out) {
    sqlite3 *db = vdb; sqlite3_stmt *st;
    if (!db || !present(mission) || !out || bf_econ_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(SUM(amount),0) FROM mission_costs WHERE mission=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, mission, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st); if (rc == SQLITE_ROW) *out = sqlite3_column_double(st, 0);
    sqlite3_finalize(st);
    return rc == SQLITE_ROW ? 0 : -1;
}

int bf_econ_cost_booked(void *vdb, const char *mission, const char *resource) {
    sqlite3 *db = vdb; sqlite3_stmt *st;
    if (!db || !present(mission) || !present(resource) || bf_econ_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT booked FROM mission_costs WHERE mission=?1 AND resource=?2", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, mission, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, resource, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st), b = rc == SQLITE_ROW && sqlite3_column_int(st, 0) == 1;
    sqlite3_finalize(st);
    return (rc == SQLITE_ROW || rc == SQLITE_DONE) ? b : -1;
}
