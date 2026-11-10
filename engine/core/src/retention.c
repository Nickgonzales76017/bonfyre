/*
 * Native owner for RetentionGC. See bf_retention.h and
 * schemas/control/retention.{yaff,cue} for the semantic and declaration
 * boundaries. This absorbed components/BonfyreControlPlane/retention_gc.py
 * and retention_holds.py: they remain archived reference/witness material,
 * not alternate stateful implementations. Keep new retention mutations here
 * so all callers share one SQLite truth and one tombstone rule.
 */
#include "bf_retention.h"

#include <sqlite3.h>

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

struct BfRetention {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

static const char *RETENTION_SCHEMA =
    "CREATE TABLE IF NOT EXISTS retained("
    "record_id TEXT PRIMARY KEY,occurrence TEXT NOT NULL,kind TEXT NOT NULL,"
    "tier TEXT NOT NULL,written_at INTEGER NOT NULL,body TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS retention_hold("
    "record_id TEXT NOT NULL,hold TEXT NOT NULL,holder TEXT NOT NULL,"
    "PRIMARY KEY(record_id,hold,holder));"
    "CREATE TABLE IF NOT EXISTS tombstone("
    "record_id TEXT PRIMARY KEY,occurrence TEXT NOT NULL,kind TEXT NOT NULL,"
    "tier TEXT NOT NULL,written_at INTEGER NOT NULL,removed_at INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS commitment_rests_on("
    "commitment_id TEXT NOT NULL,record_id TEXT NOT NULL,"
    "PRIMARY KEY(commitment_id,record_id));";

typedef struct TierRule { const char *kind; const char *tier; int64_t lifetime_ms; } TierRule;
/* A negative lifetime marks a tier at/above the cooling floor: time cannot remove it. */
static const TierRule TIER_RULES[] = {
    {"reaction", "flash", 5LL * 60 * 1000},
    {"presence", "flash", 5LL * 60 * 1000},
    {"chat", "chat", 6LL * 60 * 60 * 1000},
    {"utterance", "chat", 6LL * 60 * 60 * 1000},
    {"speaker", "chat", 6LL * 60 * 60 * 1000},
    {"topic", "moment", 3LL * 24 * 60 * 60 * 1000},
    {"entity", "moment", 3LL * 24 * 60 * 60 * 1000},
    {"place", "moment", 3LL * 24 * 60 * 60 * 1000},
    {"chapter", "moment", 3LL * 24 * 60 * 60 * 1000},
    {"clip_candidate", "moment", 3LL * 24 * 60 * 60 * 1000},
    {"cta", "social", 30LL * 24 * 60 * 60 * 1000},
    {"claim", "evidentiary", -1},
    {"sponsor_mention", "evidentiary", -1},
    {"commerce", "economic", -1},
    {NULL, NULL, 0}
};

static int present(const char *value) { return value != NULL && value[0] != '\0'; }

static int copy_text(char *out, size_t out_size, const char *value) {
    return present(value) && snprintf(out, out_size, "%s", value) < (int)out_size;
}

static const TierRule *rule_for_kind(const char *kind) {
    size_t index;
    if (!present(kind)) return NULL;
    for (index = 0; TIER_RULES[index].kind != NULL; index++) {
        if (strcmp(kind, TIER_RULES[index].kind) == 0) return &TIER_RULES[index];
    }
    return NULL;
}

static int known_hold(const char *hold) {
    return present(hold) && (!strcmp(hold, "open_obligation") ||
                             !strcmp(hold, "evidence_reference") ||
                             !strcmp(hold, "legal_hold") ||
                             !strcmp(hold, "user_saved"));
}

static BfRetentionStatus apply_schema(sqlite3 *db, FILE *err) {
    char *message = NULL;
    if (sqlite3_exec(db, RETENTION_SCHEMA, NULL, NULL, &message) != SQLITE_OK) {
        if (err) fprintf(err, "retention: schema failed: %s\n", message ? message : "?");
        sqlite3_free(message);
        return BF_RETENTION_STORAGE;
    }
    return BF_RETENTION_OK;
}

static BfRetentionStatus execute(BfRetention *retention, const char *sql) {
    char *message = NULL;
    if (sqlite3_exec(retention->db, sql, NULL, NULL, &message) != SQLITE_OK) {
        if (retention->err) fprintf(retention->err, "retention: %s\n", message ? message : "storage error");
        sqlite3_free(message);
        return BF_RETENTION_STORAGE;
    }
    return BF_RETENTION_OK;
}

const char *bf_retention_status_name(BfRetentionStatus status) {
    switch (status) {
    case BF_RETENTION_OK: return "ok";
    case BF_RETENTION_INVALID: return "invalid";
    case BF_RETENTION_NOT_FOUND: return "not_found";
    case BF_RETENTION_NOT_REMOVABLE: return "not_removable";
    case BF_RETENTION_STORAGE: return "storage";
    case BF_RETENTION_CAPACITY: return "capacity";
    }
    return "unknown";
}

int bf_retention_open(const char *database_path, BfRetention **out, FILE *err) {
    sqlite3 *db = NULL;
    BfRetention *retention;
    if (!present(database_path) || out == NULL || sqlite3_open(database_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return BF_RETENTION_STORAGE;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != BF_RETENTION_OK) { sqlite3_close(db); return BF_RETENTION_STORAGE; }
    retention = calloc(1, sizeof(*retention));
    if (!retention) { sqlite3_close(db); return BF_RETENTION_STORAGE; }
    retention->db = db; retention->owns_db = 1; retention->err = err; *out = retention;
    return BF_RETENTION_OK;
}

int bf_retention_open_database(void *sqlite_database, BfRetention **out, FILE *err) {
    BfRetention *retention;
    if (sqlite_database == NULL || out == NULL || apply_schema(sqlite_database, err) != BF_RETENTION_OK) return BF_RETENTION_STORAGE;
    retention = calloc(1, sizeof(*retention));
    if (!retention) return BF_RETENTION_STORAGE;
    retention->db = sqlite_database; retention->err = err; *out = retention;
    return BF_RETENTION_OK;
}

void bf_retention_close(BfRetention *retention) {
    if (!retention) return;
    if (retention->owns_db) sqlite3_close(retention->db);
    free(retention);
}

BfRetentionStatus bf_retention_retain(BfRetention *retention, const char *record_id,
                                      const char *occurrence, const char *kind,
                                      int64_t written_at_ms, const char *body) {
    const TierRule *rule = rule_for_kind(kind);
    sqlite3_stmt *stmt = NULL;
    if (!retention || !present(record_id) || !present(occurrence) || !rule) return BF_RETENTION_INVALID;
    if (sqlite3_prepare_v2(retention->db, "INSERT OR REPLACE INTO retained(record_id,occurrence,kind,tier,written_at,body) VALUES(?,?,?,?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, occurrence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, rule->tier, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 5, written_at_ms);
    sqlite3_bind_text(stmt, 6, body ? body : "", -1, SQLITE_TRANSIENT);
    BfRetentionStatus status = sqlite3_step(stmt) == SQLITE_DONE ? BF_RETENTION_OK : BF_RETENTION_STORAGE;
    sqlite3_finalize(stmt);
    return status;
}

BfRetentionStatus bf_retention_hold(BfRetention *retention, const char *record_id,
                                    const char *hold, const char *holder) {
    sqlite3_stmt *stmt = NULL;
    if (!retention || !present(record_id) || !known_hold(hold) || !present(holder)) return BF_RETENTION_INVALID;
    if (sqlite3_prepare_v2(retention->db, "INSERT OR REPLACE INTO retention_hold(record_id,hold,holder) VALUES(?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, hold, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, holder, -1, SQLITE_TRANSIENT);
    BfRetentionStatus status = sqlite3_step(stmt) == SQLITE_DONE ? BF_RETENTION_OK : BF_RETENTION_STORAGE;
    sqlite3_finalize(stmt);
    return status;
}

BfRetentionStatus bf_retention_release(BfRetention *retention, const char *record_id,
                                       const char *hold, const char *holder, int *out_released) {
    sqlite3_stmt *stmt = NULL;
    if (out_released) *out_released = 0;
    if (!retention || !present(record_id) || !known_hold(hold) || !present(holder)) return BF_RETENTION_INVALID;
    if (sqlite3_prepare_v2(retention->db, "DELETE FROM retention_hold WHERE record_id=? AND hold=? AND holder=?", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, hold, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, holder, -1, SQLITE_TRANSIENT);
    BfRetentionStatus status = sqlite3_step(stmt) == SQLITE_DONE ? BF_RETENTION_OK : BF_RETENTION_STORAGE;
    sqlite3_finalize(stmt);
    if (status == BF_RETENTION_OK && out_released) *out_released = sqlite3_changes(retention->db) > 0;
    return status;
}

static BfRetentionStatus append_holds(BfRetention *retention, const char *record_id,
                                      BfRetentionRefusal *out, int *out_any) {
    sqlite3_stmt *stmt = NULL;
    int first = 1;
    size_t reason_used = 0, holder_used = 0;
    *out_any = 0;
    if (sqlite3_prepare_v2(retention->db, "SELECT hold,holder FROM retention_hold WHERE record_id=? ORDER BY hold,holder", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *hold = (const char *)sqlite3_column_text(stmt, 0);
        const char *holder = (const char *)sqlite3_column_text(stmt, 1);
        int n = snprintf(out->reason + reason_used, sizeof(out->reason) - reason_used,
                         "%s%s by %s", first ? "held: " : ", ", hold, holder);
        if (n < 0 || (size_t)n >= sizeof(out->reason) - reason_used) { sqlite3_finalize(stmt); return BF_RETENTION_CAPACITY; }
        reason_used += (size_t)n;
        n = snprintf(out->holder + holder_used, sizeof(out->holder) - holder_used,
                     "%s%s", first ? "" : ", ", holder);
        if (n < 0 || (size_t)n >= sizeof(out->holder) - holder_used) { sqlite3_finalize(stmt); return BF_RETENTION_CAPACITY; }
        holder_used += (size_t)n; first = 0; *out_any = 1;
    }
    sqlite3_finalize(stmt);
    return BF_RETENTION_OK;
}

static BfRetentionStatus commitment_exists(BfRetention *retention, const char *commitment_id,
                                           int *out_exists) {
    sqlite3_stmt *stmt = NULL;
    *out_exists = 0;
    if (sqlite3_prepare_v2(retention->db, "SELECT 1 FROM commitments WHERE commitment_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    *out_exists = sqlite3_step(stmt) == SQLITE_ROW;
    sqlite3_finalize(stmt);
    return BF_RETENTION_OK;
}

BfRetentionStatus bf_retention_rests_on(BfRetention *retention, const char *commitment_id,
                                        const char *record_id) {
    sqlite3_stmt *stmt = NULL;
    int exists = 0;
    BfRetentionStatus status;
    if (!retention || !present(commitment_id) || !present(record_id)) return BF_RETENTION_INVALID;
    status = commitment_exists(retention, commitment_id, &exists);
    if (status != BF_RETENTION_OK) return status;
    if (!exists) return BF_RETENTION_NOT_FOUND;
    if (sqlite3_prepare_v2(retention->db, "INSERT OR REPLACE INTO commitment_rests_on(commitment_id,record_id) VALUES(?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, commitment_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, record_id, -1, SQLITE_TRANSIENT);
    status = sqlite3_step(stmt) == SQLITE_DONE ? BF_RETENTION_OK : BF_RETENTION_STORAGE;
    sqlite3_finalize(stmt);
    return status;
}

static int id_in(char values[][BF_RETENTION_ID_MAX], size_t count, const char *value) {
    size_t index;
    for (index = 0; index < count; index++) if (!strcmp(values[index], value)) return 1;
    return 0;
}

static BfRetentionStatus append_id(char values[][BF_RETENTION_ID_MAX], size_t *count, const char *value) {
    if (id_in(values, *count, value)) return BF_RETENTION_OK;
    if (*count == BF_RETENTION_SWEEP_MAX || !copy_text(values[*count], BF_RETENTION_ID_MAX, value)) return BF_RETENTION_CAPACITY;
    (*count)++;
    return BF_RETENTION_OK;
}

static BfRetentionStatus reconcile_kind(BfRetention *retention, const char *hold,
                                        const char *holder, const char *wanted_sql,
                                        BfRetentionReconciliation *out) {
    sqlite3_stmt *wanted_stmt = NULL, *current_stmt = NULL;
    char wanted[BF_RETENTION_SWEEP_MAX][BF_RETENTION_ID_MAX] = {{0}};
    char current[BF_RETENTION_SWEEP_MAX][BF_RETENTION_ID_MAX] = {{0}};
    size_t wanted_count = 0, current_count = 0, index;
    BfRetentionStatus status;
    if (sqlite3_prepare_v2(retention->db, wanted_sql, -1, &wanted_stmt, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(retention->db, "SELECT record_id FROM retention_hold WHERE hold=? AND holder=? ORDER BY record_id", -1, &current_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(wanted_stmt); sqlite3_finalize(current_stmt); return BF_RETENTION_STORAGE;
    }
    while (sqlite3_step(wanted_stmt) == SQLITE_ROW) {
        status = append_id(wanted, &wanted_count, (const char *)sqlite3_column_text(wanted_stmt, 0));
        if (status != BF_RETENTION_OK) goto done;
    }
    sqlite3_bind_text(current_stmt, 1, hold, -1, SQLITE_STATIC);
    sqlite3_bind_text(current_stmt, 2, holder, -1, SQLITE_STATIC);
    while (sqlite3_step(current_stmt) == SQLITE_ROW) {
        status = append_id(current, &current_count, (const char *)sqlite3_column_text(current_stmt, 0));
        if (status != BF_RETENTION_OK) goto done;
    }
    for (index = 0; index < wanted_count; index++) if (!id_in(current, current_count, wanted[index])) {
        if (out->placed_count == BF_RETENTION_SWEEP_MAX ||
            snprintf(out->placed[out->placed_count], sizeof(out->placed[0]), "%s:%s", hold, wanted[index]) >= (int)sizeof(out->placed[0])) { status = BF_RETENTION_CAPACITY; goto done; }
        status = bf_retention_hold(retention, wanted[index], hold, holder); if (status != BF_RETENTION_OK) goto done;
        out->placed_count++;
    }
    for (index = 0; index < current_count; index++) if (!id_in(wanted, wanted_count, current[index])) {
        int released = 0;
        if (out->released_count == BF_RETENTION_SWEEP_MAX ||
            snprintf(out->released[out->released_count], sizeof(out->released[0]), "%s:%s", hold, current[index]) >= (int)sizeof(out->released[0])) { status = BF_RETENTION_CAPACITY; goto done; }
        status = bf_retention_release(retention, current[index], hold, holder, &released); if (status != BF_RETENTION_OK || !released) { if (status == BF_RETENTION_OK) status = BF_RETENTION_STORAGE; goto done; }
        out->released_count++;
    }
    status = BF_RETENTION_OK;
done:
    sqlite3_finalize(wanted_stmt); sqlite3_finalize(current_stmt); return status;
}

BfRetentionStatus bf_retention_reconcile_holds(BfRetention *retention,
                                               BfRetentionReconciliation *out) {
    BfRetentionStatus status;
    /* Violation remains outstanding; only discharged and compensated are terminal. */
    static const char *OPEN_COMMITMENT_RECORDS =
        "SELECT DISTINCT r.record_id FROM commitment_rests_on r "
        "JOIN commitment_events e ON e.commitment_id=r.commitment_id "
        "WHERE e.seq=(SELECT MAX(e2.seq) FROM commitment_events e2 WHERE e2.commitment_id=r.commitment_id) "
        "AND e.state NOT IN ('discharged','compensated') ORDER BY r.record_id";
    static const char *EVIDENCE_RECORDS =
        "SELECT DISTINCT evidence FROM evidence_relations ORDER BY evidence";
    if (!retention || !out) return BF_RETENTION_INVALID;
    memset(out, 0, sizeof(*out));
    status = execute(retention, "BEGIN IMMEDIATE"); if (status != BF_RETENTION_OK) return status;
    status = reconcile_kind(retention, "open_obligation", "CommitmentGraph", OPEN_COMMITMENT_RECORDS, out);
    if (status == BF_RETENTION_OK) status = reconcile_kind(retention, "evidence_reference", "EvidenceGraph", EVIDENCE_RECORDS, out);
    if (status == BF_RETENTION_OK && execute(retention, "COMMIT") == BF_RETENTION_OK) return BF_RETENTION_OK;
    execute(retention, "ROLLBACK"); return status == BF_RETENTION_OK ? BF_RETENTION_STORAGE : status;
}

BfRetentionStatus bf_retention_removable(BfRetention *retention, const char *record_id,
                                         int64_t now_ms, BfRetentionDecision *out) {
    sqlite3_stmt *stmt = NULL;
    const char *kind;
    int64_t written_at;
    const TierRule *rule;
    int has_holds;
    if (!retention || !present(record_id) || !out) return BF_RETENTION_INVALID;
    memset(out, 0, sizeof(*out));
    if (!copy_text(out->refusal.record_id, sizeof(out->refusal.record_id), record_id)) return BF_RETENTION_CAPACITY;
    if (sqlite3_prepare_v2(retention->db, "SELECT kind,written_at FROM retained WHERE record_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt); snprintf(out->refusal.reason, sizeof(out->refusal.reason), "no such record"); return BF_RETENTION_NOT_FOUND;
    }
    kind = (const char *)sqlite3_column_text(stmt, 0); written_at = sqlite3_column_int64(stmt, 1);
    rule = rule_for_kind(kind);
    sqlite3_finalize(stmt);
    if (!rule) return BF_RETENTION_STORAGE;
    if (rule->lifetime_ms < 0) {
        snprintf(out->refusal.reason, sizeof(out->refusal.reason), "tier %s is at or above the cooling floor; time passing does not remove it", rule->tier);
        return BF_RETENTION_NOT_REMOVABLE;
    }
    if (now_ms - written_at < rule->lifetime_ms) {
        snprintf(out->refusal.reason, sizeof(out->refusal.reason), "tier %s lifetime has not elapsed", rule->tier);
        return BF_RETENTION_NOT_REMOVABLE;
    }
    BfRetentionStatus status = append_holds(retention, record_id, &out->refusal, &has_holds);
    if (status != BF_RETENTION_OK) return status;
    if (has_holds) return BF_RETENTION_NOT_REMOVABLE;
    out->may_remove = 1;
    snprintf(out->refusal.reason, sizeof(out->refusal.reason), "tier is below the floor, its lifetime elapsed, nothing holds it");
    return BF_RETENTION_OK;
}

BfRetentionStatus bf_retention_collect(BfRetention *retention, int64_t now_ms,
                                       const char *occurrence_or_null, BfRetentionSweep *out) {
    sqlite3_stmt *select = NULL, *read = NULL, *stone = NULL, *drop = NULL;
    char ids[BF_RETENTION_SWEEP_MAX][BF_RETENTION_ID_MAX];
    size_t id_count = 0, index;
    BfRetentionStatus status = BF_RETENTION_OK;
    if (!retention || !out) return BF_RETENTION_INVALID;
    memset(out, 0, sizeof(*out));
    status = execute(retention, "BEGIN IMMEDIATE"); if (status != BF_RETENTION_OK) return status;
    const char *sql = occurrence_or_null ? "SELECT record_id FROM retained WHERE occurrence=? ORDER BY record_id" : "SELECT record_id FROM retained ORDER BY record_id";
    if (sqlite3_prepare_v2(retention->db, sql, -1, &select, NULL) != SQLITE_OK) { status = BF_RETENTION_STORAGE; goto rollback; }
    if (occurrence_or_null) sqlite3_bind_text(select, 1, occurrence_or_null, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(select) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(select, 0);
        if (id_count == BF_RETENTION_SWEEP_MAX || !copy_text(ids[id_count], sizeof(ids[id_count]), id)) { status = BF_RETENTION_CAPACITY; goto rollback; }
        id_count++;
    }
    sqlite3_finalize(select); select = NULL;
    if (sqlite3_prepare_v2(retention->db, "SELECT occurrence,kind,tier,written_at FROM retained WHERE record_id=?", -1, &read, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(retention->db, "INSERT OR REPLACE INTO tombstone(record_id,occurrence,kind,tier,written_at,removed_at) VALUES(?,?,?,?,?,?)", -1, &stone, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(retention->db, "DELETE FROM retained WHERE record_id=?", -1, &drop, NULL) != SQLITE_OK) { status = BF_RETENTION_STORAGE; goto rollback; }
    for (index = 0; index < id_count; index++) {
        BfRetentionDecision decision;
        status = bf_retention_removable(retention, ids[index], now_ms, &decision);
        if (status == BF_RETENTION_NOT_FOUND || status == BF_RETENTION_NOT_REMOVABLE) {
            if (out->refused_count == BF_RETENTION_SWEEP_MAX) { status = BF_RETENTION_CAPACITY; goto rollback; }
            out->refused[out->refused_count++] = decision.refusal; status = BF_RETENTION_OK; continue;
        }
        if (status != BF_RETENTION_OK || !decision.may_remove) goto rollback;
        sqlite3_reset(read); sqlite3_clear_bindings(read); sqlite3_bind_text(read, 1, ids[index], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(read) != SQLITE_ROW) { status = BF_RETENTION_STORAGE; goto rollback; }
        sqlite3_reset(stone); sqlite3_clear_bindings(stone);
        sqlite3_bind_text(stone, 1, ids[index], -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stone, 2, (const char *)sqlite3_column_text(read, 0), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stone, 3, (const char *)sqlite3_column_text(read, 1), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stone, 4, (const char *)sqlite3_column_text(read, 2), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stone, 5, sqlite3_column_int64(read, 3)); sqlite3_bind_int64(stone, 6, now_ms);
        if (sqlite3_step(stone) != SQLITE_DONE) { status = BF_RETENTION_STORAGE; goto rollback; }
        sqlite3_reset(drop); sqlite3_clear_bindings(drop); sqlite3_bind_text(drop, 1, ids[index], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(drop) != SQLITE_DONE || out->removed_count == BF_RETENTION_SWEEP_MAX || !copy_text(out->removed[out->removed_count], sizeof(out->removed[0]), ids[index])) { status = BF_RETENTION_STORAGE; goto rollback; }
        out->removed_count++;
    }
    sqlite3_finalize(select); sqlite3_finalize(read); sqlite3_finalize(stone); sqlite3_finalize(drop);
    if (execute(retention, "COMMIT") != BF_RETENTION_OK) return BF_RETENTION_STORAGE;
    return BF_RETENTION_OK;
rollback:
    sqlite3_finalize(select); sqlite3_finalize(read); sqlite3_finalize(stone); sqlite3_finalize(drop); execute(retention, "ROLLBACK"); return status;
}

BfRetentionStatus bf_retention_body(BfRetention *retention, const char *record_id, char *out, size_t out_size, int *out_present) {
    sqlite3_stmt *stmt = NULL;
    if (out_present) *out_present = 0;
    if (!retention || !present(record_id) || !out || out_size == 0) return BF_RETENTION_INVALID;
    out[0] = '\0';
    if (sqlite3_prepare_v2(retention->db, "SELECT body FROM retained WHERE record_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *body = (const char *)sqlite3_column_text(stmt, 0);
        if (snprintf(out, out_size, "%s", body ? body : "") >= (int)out_size) { sqlite3_finalize(stmt); return BF_RETENTION_CAPACITY; }
        if (out_present) *out_present = 1;
    }
    sqlite3_finalize(stmt); return BF_RETENTION_OK;
}

int bf_retention_happened(BfRetention *retention, const char *record_id) {
    sqlite3_stmt *stmt = NULL; int found = 0;
    if (!retention || !present(record_id)) return 0;
    if (sqlite3_prepare_v2(retention->db, "SELECT 1 FROM retained WHERE record_id=? UNION ALL SELECT 1 FROM tombstone WHERE record_id=? LIMIT 1", -1, &stmt, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, record_id, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(stmt) == SQLITE_ROW; sqlite3_finalize(stmt); return found;
}

BfRetentionStatus bf_retention_tombstone(BfRetention *retention, const char *record_id, BfRetentionTombstone *out, int *out_present) {
    sqlite3_stmt *stmt = NULL;
    if (out_present) *out_present = 0;
    if (!retention || !present(record_id) || !out) return BF_RETENTION_INVALID;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(retention->db, "SELECT occurrence,kind,tier,written_at,removed_at FROM tombstone WHERE record_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_RETENTION_STORAGE;
    sqlite3_bind_text(stmt, 1, record_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        if (!copy_text(out->record_id, sizeof(out->record_id), record_id) || !copy_text(out->occurrence, sizeof(out->occurrence), (const char *)sqlite3_column_text(stmt, 0)) || !copy_text(out->kind, sizeof(out->kind), (const char *)sqlite3_column_text(stmt, 1)) || !copy_text(out->tier, sizeof(out->tier), (const char *)sqlite3_column_text(stmt, 2))) { sqlite3_finalize(stmt); return BF_RETENTION_CAPACITY; }
        out->written_at_ms = sqlite3_column_int64(stmt, 3); out->removed_at_ms = sqlite3_column_int64(stmt, 4); if (out_present) *out_present = 1;
    }
    sqlite3_finalize(stmt); return BF_RETENTION_OK;
}
