#include "bf_reservation.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct BfReservation { sqlite3 *db; int owns_db; };

static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS resource_reservations("
    " reservation_id TEXT PRIMARY KEY, resource_id TEXT NOT NULL, holder TEXT NOT NULL,"
    " purpose TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL,"
    " authority_ref TEXT NOT NULL DEFAULT '', evidence_ref TEXT NOT NULL DEFAULT '',"
    " state TEXT NOT NULL CHECK(state IN ('held','confirmed','released','cancelled','expired')),"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    "CREATE INDEX IF NOT EXISTS resource_reservations_window"
    " ON resource_reservations(resource_id,starts_at,ends_at,state);";

static void now_iso(char out[32]) {
    time_t value = time(NULL); struct tm utc;
#if defined(_WIN32)
    gmtime_s(&utc, &value);
#else
    gmtime_r(&value, &utc);
#endif
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &utc);
}

static int open_db(sqlite3 *db, int owns, BfReservation **out, FILE *err) {
    BfReservation *value;
    char *message = NULL;
    if (sqlite3_exec(db, SCHEMA, NULL, NULL, &message) != SQLITE_OK) {
        if (err) fprintf(err, "reservation: schema failed: %s\n", message ? message : "?");
        sqlite3_free(message); return -1;
    }
    value = calloc(1, sizeof(*value)); if (!value) return -1;
    value->db = db; value->owns_db = owns; *out = value; return 0;
}

int bf_reservation_open(const char *path, BfReservation **out, FILE *err) {
    sqlite3 *db = NULL;
    if (!path || !out || sqlite3_open(path, &db) != SQLITE_OK) { if (db) sqlite3_close(db); return -1; }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    if (open_db(db, 1, out, err) != 0) { sqlite3_close(db); return -1; }
    return 0;
}
int bf_reservation_open_database(void *raw, BfReservation **out, FILE *err) {
    if (!raw || !out) return -1; return open_db((sqlite3 *)raw, 0, out, err);
}
void bf_reservation_close(BfReservation *value) { if (!value) return; if (value->owns_db) sqlite3_close(value->db); free(value); }

const char *bf_reservation_state_name(BfReservationState state) {
    static const char *names[] = {"held","confirmed","released","cancelled","expired"};
    return state >= BF_RESERVATION_HELD && state <= BF_RESERVATION_EXPIRED ? names[state] : "unknown";
}

int bf_reservation_hold(BfReservation *value, const BfReservationSpec *spec, const char *at) {
    sqlite3_stmt *statement = NULL; char stamp[32]; int busy = 0; int rc;
    if (!value || !spec || !spec->reservation_id || !spec->resource_id || !spec->holder ||
        !spec->purpose || !spec->starts_at || !spec->ends_at || !spec->reservation_id[0] ||
        strcmp(spec->starts_at, spec->ends_at) >= 0) return -1;
    if (!at || !at[0]) { now_iso(stamp); at = stamp; }
    if (sqlite3_prepare_v2(value->db,
            "SELECT 1 FROM resource_reservations WHERE resource_id=? AND state IN ('held','confirmed') "
            "AND starts_at < ? AND ends_at > ? LIMIT 1", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, spec->resource_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, spec->ends_at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, spec->starts_at, -1, SQLITE_TRANSIENT);
    busy = sqlite3_step(statement) == SQLITE_ROW; sqlite3_finalize(statement);
    if (busy) return 0;
    if (sqlite3_prepare_v2(value->db,
            "INSERT INTO resource_reservations(reservation_id,resource_id,holder,purpose,starts_at,ends_at,authority_ref,evidence_ref,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?, 'held',?,?)",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, spec->reservation_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, spec->resource_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, spec->holder, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, spec->purpose, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, spec->starts_at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, spec->ends_at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, spec->authority_ref ? spec->authority_ref : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 8, spec->evidence_ref ? spec->evidence_ref : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, at, -1, SQLITE_TRANSIENT); sqlite3_bind_text(statement, 10, at, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(statement); sqlite3_finalize(statement); return rc == SQLITE_DONE ? 1 : -1;
}

int bf_reservation_transition(BfReservation *value, const char *id, BfReservationState state, const char *at) {
    sqlite3_stmt *statement = NULL; char stamp[32]; int rc;
    if (!value || !id || !id[0] || state < BF_RESERVATION_HELD || state > BF_RESERVATION_EXPIRED) return -1;
    if (!at || !at[0]) { now_iso(stamp); at = stamp; }
    if (sqlite3_prepare_v2(value->db,
            "UPDATE resource_reservations SET state=?,updated_at=? WHERE reservation_id=? AND state IN ('held','confirmed')",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, bf_reservation_state_name(state), -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 2, at, -1, SQLITE_TRANSIENT); sqlite3_bind_text(statement, 3, id, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(statement); sqlite3_finalize(statement); return rc == SQLITE_DONE ? sqlite3_changes(value->db) : -1;
}

int bf_reservation_covers(BfReservation *value, const char *resource, const char *start, const char *end) {
    sqlite3_stmt *statement = NULL; int answer = 0;
    if (!value || !resource || !start || !end || strcmp(start,end) >= 0) return -1;
    if (sqlite3_prepare_v2(value->db,
            "SELECT 1 FROM resource_reservations WHERE resource_id=? AND state IN ('held','confirmed') "
            "AND starts_at<=? AND ends_at>=? LIMIT 1", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, resource, -1, SQLITE_TRANSIENT); sqlite3_bind_text(statement, 2, start, -1, SQLITE_TRANSIENT); sqlite3_bind_text(statement, 3, end, -1, SQLITE_TRANSIENT);
    answer = sqlite3_step(statement) == SQLITE_ROW; sqlite3_finalize(statement); return answer;
}
