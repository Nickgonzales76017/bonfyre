/*
 * Native OccurrenceSpine. Writes the external_event_log store shared with the
 * Python reference (external_events.py), with a byte-identical observation
 * digest, so native and Python writers dedup against each other.
 */

#include "bf_occurrence.h"
#include "bonfyre.h" /* bf_sha256_hex */

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfOccurrenceSpine {
    sqlite3 *db;
    int owns_db; /* 1 if we opened it and must close it */
    FILE *err;
};

/* Must match external_events.py SCHEMA exactly -- same table, columns, indexes. */
static const char *OCCURRENCE_SCHEMA =
    "CREATE TABLE IF NOT EXISTS external_event_log("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  digest TEXT NOT NULL UNIQUE,"
    "  observed_at TEXT NOT NULL,"
    "  recorded_at TEXT NOT NULL,"
    "  source TEXT NOT NULL,"
    "  actor TEXT NOT NULL,"
    "  event_kind TEXT NOT NULL,"
    "  subject_ref TEXT NOT NULL DEFAULT '',"
    "  payload TEXT NOT NULL DEFAULT '{}',"
    "  evidence_ref TEXT NOT NULL DEFAULT '',"
    "  projected_at TEXT"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_external_event_unprojected"
    "  ON external_event_log(projected_at, id);"
    "CREATE INDEX IF NOT EXISTS idx_external_event_actor"
    "  ON external_event_log(actor, id);";

/* The declared occurrence kinds, mirroring EVENT_KINDS in external_events.py.
 * An unknown kind is rejected here rather than folded, matching the reference's
 * ValueError -- an unmapped kind must never enter the spine and be silently
 * dropped downstream. */
static const char *OCCURRENCE_KINDS[] = {
    "inbound_reply", "declined", "accepted", "acknowledged",
    "redirected", "outbound_sent", "state_changed", NULL,
};

static int kind_is_declared(const char *kind) {
    for (size_t i = 0; OCCURRENCE_KINDS[i] != NULL; i++) {
        if (strcmp(OCCURRENCE_KINDS[i], kind) == 0) {
            return 1;
        }
    }
    return 0;
}

void bf_occurrence_now_iso(char out[40]) {
    time_t t = time(NULL);
    struct tm utc;
#if defined(_WIN32)
    gmtime_s(&utc, &t);
#else
    gmtime_r(&t, &utc);
#endif
    /* Python-parity: datetime.isoformat() on a UTC, microsecond-zeroed value. */
    if (strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc) == 0) {
        snprintf(out, 40, "1970-01-01T00:00:00+00:00");
    }
}

void bf_occurrence_digest(const char *source, const char *actor, const char *event_kind,
                          const char *subject_ref, const char *observed_at,
                          char out_digest[33]) {
    char material[1024];
    char hex[65];
    snprintf(material, sizeof(material), "%s|%s|%s|%s|%s",
             source ? source : "", actor ? actor : "", event_kind ? event_kind : "",
             subject_ref ? subject_ref : "", observed_at ? observed_at : "");
    bf_sha256_hex((const uint8_t *)material, strlen(material), hex);
    memcpy(out_digest, hex, 32);
    out_digest[32] = '\0';
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *errmsg = NULL;
    if (sqlite3_exec(db, OCCURRENCE_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "occurrence: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    return 0;
}

int bf_occurrence_open(const char *database_path, BfOccurrenceSpine **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "occurrence: cannot open %s: %s\n", database_path,
                    db ? sqlite3_errmsg(db) : "?");
        }
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) {
        sqlite3_close(db);
        return -1;
    }
    BfOccurrenceSpine *spine = calloc(1, sizeof(*spine));
    if (!spine) {
        sqlite3_close(db);
        return -1;
    }
    spine->db = db;
    spine->owns_db = 1;
    spine->err = err;
    *out = spine;
    return 0;
}

int bf_occurrence_open_database(void *sqlite_database, BfOccurrenceSpine **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfOccurrenceSpine *spine = calloc(1, sizeof(*spine));
    if (!spine) {
        return -1;
    }
    spine->db = db;
    spine->owns_db = 0;
    spine->err = err;
    *out = spine;
    return 0;
}

void bf_occurrence_close(BfOccurrenceSpine *spine) {
    if (!spine) {
        return;
    }
    if (spine->owns_db && spine->db) {
        sqlite3_close(spine->db);
    }
    free(spine);
}

static int64_t lookup_existing(BfOccurrenceSpine *spine, const char *digest) {
    sqlite3_stmt *stmt = NULL;
    int64_t id = 0;
    if (sqlite3_prepare_v2(spine->db, "SELECT id FROM external_event_log WHERE digest=?",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            id = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return id;
}

BfOccurrenceStatus bf_occurrence_observe(BfOccurrenceSpine *spine,
                                         const BfOccurrenceSpec *spec, int64_t *out_id) {
    if (out_id) {
        *out_id = 0;
    }
    if (!spine || !spec) {
        return BF_OCCURRENCE_INVALID;
    }
    if (!spec->source || spec->source[0] == '\0' ||
        !spec->actor || spec->actor[0] == '\0' ||
        !spec->event_kind || spec->event_kind[0] == '\0' ||
        !spec->observed_at || spec->observed_at[0] == '\0') {
        return BF_OCCURRENCE_INVALID;
    }
    if (!kind_is_declared(spec->event_kind)) {
        return BF_OCCURRENCE_INVALID_KIND;
    }

    const char *subject = spec->subject_ref ? spec->subject_ref : "";
    const char *payload = spec->payload_json ? spec->payload_json : "{}";
    const char *evidence = spec->evidence_ref ? spec->evidence_ref : "";
    char recorded[40];
    const char *recorded_at = spec->recorded_at;
    if (!recorded_at || recorded_at[0] == '\0') {
        bf_occurrence_now_iso(recorded);
        recorded_at = recorded;
    }

    char digest[33];
    bf_occurrence_digest(spec->source, spec->actor, spec->event_kind, subject,
                         spec->observed_at, digest);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            spine->db,
            "INSERT INTO external_event_log"
            "(digest,observed_at,recorded_at,source,actor,event_kind,subject_ref,"
            " payload,evidence_ref) VALUES(?,?,?,?,?,?,?,?,?)",
            -1, &stmt, NULL) != SQLITE_OK) {
        if (spine->err) {
            fprintf(spine->err, "occurrence: prepare failed: %s\n", sqlite3_errmsg(spine->db));
        }
        return BF_OCCURRENCE_STORAGE_ERROR;
    }
    sqlite3_bind_text(stmt, 1, digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->observed_at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, recorded_at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, spec->source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, spec->actor, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, spec->event_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 8, payload, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 9, evidence, -1, SQLITE_TRANSIENT);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc == SQLITE_DONE) {
        if (out_id) {
            *out_id = sqlite3_last_insert_rowid(spine->db);
        }
        return BF_OCCURRENCE_OK;
    }
    if (rc == SQLITE_CONSTRAINT) {
        /* Same observation already on the spine -- return its id, no new row. */
        if (out_id) {
            *out_id = lookup_existing(spine, digest);
        }
        return BF_OCCURRENCE_DUPLICATE;
    }
    if (spine->err) {
        fprintf(spine->err, "occurrence: insert failed (%d): %s\n", rc,
                sqlite3_errmsg(spine->db));
    }
    return BF_OCCURRENCE_STORAGE_ERROR;
}

int64_t bf_occurrence_unprojected_count(BfOccurrenceSpine *spine) {
    if (!spine) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    int64_t n = -1;
    if (sqlite3_prepare_v2(spine->db,
                           "SELECT COUNT(*) FROM external_event_log WHERE projected_at IS NULL",
                           -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            n = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return n;
}

BfOccurrenceStatus bf_occurrence_mark_projected(BfOccurrenceSpine *spine, int64_t event_id,
                                                const char *now) {
    if (!spine) {
        return BF_OCCURRENCE_INVALID;
    }
    char stamp[40];
    if (!now || now[0] == '\0') {
        bf_occurrence_now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(spine->db,
                           "UPDATE external_event_log SET projected_at=? WHERE id=?",
                           -1, &stmt, NULL) != SQLITE_OK) {
        return BF_OCCURRENCE_STORAGE_ERROR;
    }
    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, event_id);
    int rc = sqlite3_step(stmt);
    int changed = sqlite3_changes(spine->db);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return BF_OCCURRENCE_STORAGE_ERROR;
    }
    return changed > 0 ? BF_OCCURRENCE_OK : BF_OCCURRENCE_INVALID;
}

const char *bf_occurrence_status_name(BfOccurrenceStatus status) {
    switch (status) {
        case BF_OCCURRENCE_OK: return "ok";
        case BF_OCCURRENCE_DUPLICATE: return "duplicate";
        case BF_OCCURRENCE_INVALID_KIND: return "invalid_kind";
        case BF_OCCURRENCE_INVALID: return "invalid";
        case BF_OCCURRENCE_STORAGE_ERROR: return "storage_error";
    }
    return "unknown";
}
