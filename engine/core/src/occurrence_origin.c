/* Native owner for the origin refinement on OccurrenceSpine.  It shares the
 * historical occurrence_origin table with the Python reference while giving
 * native clients the same refusal law and bounded provenance traversal. */
#define _DEFAULT_SOURCE /* strsep under strict -std=c11 on glibc */
#include "bf_occurrence_origin.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>

struct BfOccurrenceOrigin { sqlite3 *db; int owns_db; FILE *err; };

static const char *ORIGIN_SCHEMA =
    "CREATE TABLE IF NOT EXISTS occurrence_origin("
    "occurrence_id TEXT PRIMARY KEY,origin TEXT NOT NULL,subject TEXT NOT NULL,"
    "at TEXT NOT NULL DEFAULT '',source TEXT NOT NULL DEFAULT '',authority TEXT NOT NULL DEFAULT '',"
    "derived_from TEXT NOT NULL DEFAULT '');";

static int present(const char *value) { return value && value[0]; }

const char *bf_origin_kind_name(BfOriginKind kind) {
    switch (kind) {
    case BF_ORIGIN_OBSERVED: return "observed";
    case BF_ORIGIN_DERIVED: return "derived";
    case BF_ORIGIN_EFFECT: return "effect";
    }
    return NULL;
}

const char *bf_origin_status_name(BfOriginStatus status) {
    switch (status) {
    case BF_ORIGIN_OK: return "ok";
    case BF_ORIGIN_INVALID: return "invalid";
    case BF_ORIGIN_NOT_FOUND: return "not_found";
    case BF_ORIGIN_EXISTS: return "exists";
    case BF_ORIGIN_STORAGE: return "storage";
    case BF_ORIGIN_CAPACITY: return "capacity";
    }
    return "unknown";
}

static BfOriginStatus apply_schema(sqlite3 *db, FILE *err) {
    char *message = NULL;
    if (sqlite3_exec(db, ORIGIN_SCHEMA, NULL, NULL, &message) != SQLITE_OK) {
        if (err) fprintf(err, "occurrence-origin: schema failed: %s\n", message ? message : "?");
        sqlite3_free(message); return BF_ORIGIN_STORAGE;
    }
    return BF_ORIGIN_OK;
}

static BfOriginStatus copy_text(char *out, size_t out_size, const char *value) {
    size_t size;
    if (!out || !out_size) return BF_ORIGIN_INVALID;
    value = value ? value : ""; size = strlen(value);
    if (size >= out_size) return BF_ORIGIN_CAPACITY;
    memcpy(out, value, size + 1); return BF_ORIGIN_OK;
}

static BfOriginKind kind_from_name(const char *name) {
    BfOriginKind kind;
    for (kind = BF_ORIGIN_OBSERVED; kind <= BF_ORIGIN_EFFECT; kind++)
        if (strcmp(name, bf_origin_kind_name(kind)) == 0) return kind;
    return (BfOriginKind)-1;
}

int bf_occurrence_origin_open(const char *database_path, BfOccurrenceOrigin **out, FILE *err) {
    sqlite3 *db = NULL; BfOccurrenceOrigin *origin;
    if (!present(database_path) || !out || sqlite3_open(database_path, &db) != SQLITE_OK) { sqlite3_close(db); return BF_ORIGIN_STORAGE; }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != BF_ORIGIN_OK) { sqlite3_close(db); return BF_ORIGIN_STORAGE; }
    origin = calloc(1, sizeof(*origin)); if (!origin) { sqlite3_close(db); return BF_ORIGIN_STORAGE; }
    origin->db = db; origin->owns_db = 1; origin->err = err; *out = origin; return BF_ORIGIN_OK;
}

int bf_occurrence_origin_open_database(void *sqlite_database, BfOccurrenceOrigin **out, FILE *err) {
    BfOccurrenceOrigin *origin;
    if (!sqlite_database || !out || apply_schema(sqlite_database, err) != BF_ORIGIN_OK) return BF_ORIGIN_STORAGE;
    origin = calloc(1, sizeof(*origin)); if (!origin) return BF_ORIGIN_STORAGE;
    origin->db = sqlite_database; origin->err = err; *out = origin; return BF_ORIGIN_OK;
}

void bf_occurrence_origin_close(BfOccurrenceOrigin *origin) {
    if (!origin) return;
    if (origin->owns_db) sqlite3_close(origin->db);
    free(origin);
}

static BfOriginStatus join_inputs(const BfOccurrenceOriginSpec *spec, char out[BF_ORIGIN_INPUT_MAX * BF_ORIGIN_ID_MAX]) {
    size_t index, used = 0;
    out[0] = '\0';
    if (spec->derived_from_count == 0 || spec->derived_from_count > BF_ORIGIN_INPUT_MAX || !spec->derived_from) return BF_ORIGIN_INVALID;
    for (index = 0; index < spec->derived_from_count; index++) {
        size_t length;
        if (!present(spec->derived_from[index]) || strchr(spec->derived_from[index], ',')) return BF_ORIGIN_INVALID;
        length = strlen(spec->derived_from[index]);
        if (used + length + (index ? 1 : 0) >= BF_ORIGIN_INPUT_MAX * BF_ORIGIN_ID_MAX) return BF_ORIGIN_CAPACITY;
        if (index) out[used++] = ',';
        memcpy(out + used, spec->derived_from[index], length); used += length; out[used] = '\0';
    }
    return BF_ORIGIN_OK;
}

BfOriginStatus bf_occurrence_origin_record(BfOccurrenceOrigin *origin, const BfOccurrenceOriginSpec *spec) {
    sqlite3_stmt *stmt = NULL; char inputs[BF_ORIGIN_INPUT_MAX * BF_ORIGIN_ID_MAX] = "";
    const char *kind; BfOriginStatus status = BF_ORIGIN_OK;
    if (!origin || !spec || !present(spec->occurrence_id) || !present(spec->subject) || !(kind = bf_origin_kind_name(spec->origin))) return BF_ORIGIN_INVALID;
    if (spec->origin == BF_ORIGIN_OBSERVED && (!present(spec->source) || present(spec->authority))) return BF_ORIGIN_INVALID;
    if (spec->origin == BF_ORIGIN_DERIVED && (present(spec->authority) || (status = join_inputs(spec, inputs)) != BF_ORIGIN_OK)) return status == BF_ORIGIN_OK ? BF_ORIGIN_INVALID : status;
    if (spec->origin == BF_ORIGIN_EFFECT && !present(spec->authority)) return BF_ORIGIN_INVALID;
    if (sqlite3_prepare_v2(origin->db, "INSERT INTO occurrence_origin(occurrence_id,origin,subject,at,source,authority,derived_from) VALUES(?,?,?,?,?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_ORIGIN_STORAGE;
    sqlite3_bind_text(stmt, 1, spec->occurrence_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, spec->subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 4, spec->at ? spec->at : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, spec->source ? spec->source : "", -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 6, spec->authority ? spec->authority : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, inputs, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) { int duplicate = sqlite3_errcode(origin->db) == SQLITE_CONSTRAINT; sqlite3_finalize(stmt); return duplicate ? BF_ORIGIN_EXISTS : BF_ORIGIN_STORAGE; }
    sqlite3_finalize(stmt); return BF_ORIGIN_OK;
}

BfOriginStatus bf_occurrence_origin_get(BfOccurrenceOrigin *origin, const char *occurrence_id, BfOriginKind *out, int *out_found) {
    sqlite3_stmt *stmt = NULL;
    if (!origin || !present(occurrence_id) || !out || !out_found) return BF_ORIGIN_INVALID;
    *out_found = 0;
    if (sqlite3_prepare_v2(origin->db, "SELECT origin FROM occurrence_origin WHERE occurrence_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_ORIGIN_STORAGE;
    sqlite3_bind_text(stmt, 1, occurrence_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) { *out = kind_from_name((const char *)sqlite3_column_text(stmt, 0)); *out_found = 1; }
    sqlite3_finalize(stmt); return BF_ORIGIN_OK;
}

static BfOriginStatus append_id(BfOriginIds *out, const char *id) {
    if (out->count == BF_ORIGIN_INPUT_MAX) return BF_ORIGIN_CAPACITY;
    BfOriginStatus status = copy_text(out->ids[out->count], BF_ORIGIN_ID_MAX, id);
    if (status == BF_ORIGIN_OK) out->count++;
    return status;
}

BfOriginStatus bf_occurrence_origin_inputs(BfOccurrenceOrigin *origin, const char *occurrence_id, BfOriginIds *out) {
    sqlite3_stmt *stmt = NULL; char values[BF_ORIGIN_INPUT_MAX * BF_ORIGIN_ID_MAX]; char *cursor, *part;
    BfOriginStatus status = BF_ORIGIN_OK;
    if (!origin || !present(occurrence_id) || !out) return BF_ORIGIN_INVALID;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(origin->db, "SELECT derived_from FROM occurrence_origin WHERE occurrence_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_ORIGIN_STORAGE;
    sqlite3_bind_text(stmt, 1, occurrence_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return BF_ORIGIN_NOT_FOUND; }
    status = copy_text(values, sizeof(values), (const char *)sqlite3_column_text(stmt, 0)); sqlite3_finalize(stmt);
    if (status != BF_ORIGIN_OK || values[0] == '\0') return status;
    cursor = values;
    while ((part = strsep(&cursor, ",")) != NULL && status == BF_ORIGIN_OK) if (part[0]) status = append_id(out, part);
    return status;
}

int bf_occurrence_origin_caused_by_us(BfOccurrenceOrigin *origin, const char *occurrence_id) {
    BfOriginKind kind = BF_ORIGIN_OBSERVED; int found = 0;
    if (bf_occurrence_origin_get(origin, occurrence_id, &kind, &found) != BF_ORIGIN_OK) return -1;
    return found && kind == BF_ORIGIN_EFFECT;
}

BfOriginStatus bf_occurrence_origin_by_subject(BfOccurrenceOrigin *origin, const char *subject, BfOriginIds *observed, BfOriginIds *derived, BfOriginIds *effect) {
    sqlite3_stmt *stmt = NULL; BfOriginStatus status = BF_ORIGIN_OK;
    if (!origin || !present(subject) || !observed || !derived || !effect) return BF_ORIGIN_INVALID;
    memset(observed, 0, sizeof(*observed)); memset(derived, 0, sizeof(*derived)); memset(effect, 0, sizeof(*effect));
    if (sqlite3_prepare_v2(origin->db, "SELECT occurrence_id,origin FROM occurrence_origin WHERE subject=? ORDER BY occurrence_id", -1, &stmt, NULL) != SQLITE_OK) return BF_ORIGIN_STORAGE;
    sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW && status == BF_ORIGIN_OK) {
        const char *id = (const char *)sqlite3_column_text(stmt, 0); BfOriginKind kind = kind_from_name((const char *)sqlite3_column_text(stmt, 1));
        status = append_id(kind == BF_ORIGIN_OBSERVED ? observed : kind == BF_ORIGIN_DERIVED ? derived : effect, id);
    }
    sqlite3_finalize(stmt); return status;
}

static int contains(const BfOriginIds *ids, const char *id) {
    size_t index; for (index = 0; index < ids->count; index++) if (strcmp(ids->ids[index], id) == 0) return 1; return 0;
}

BfOriginStatus bf_occurrence_origin_provenance(BfOccurrenceOrigin *origin, const char *occurrence_id, BfOriginIds *out) {
    BfOriginIds frontier = {0}, seen = {0}; BfOriginStatus status = BF_ORIGIN_OK;
    size_t at = 0;
    if (!origin || !present(occurrence_id) || !out) return BF_ORIGIN_INVALID;
    memset(out, 0, sizeof(*out)); status = append_id(&frontier, occurrence_id); if (status != BF_ORIGIN_OK) return status;
    while (at < frontier.count) {
        BfOriginKind kind = BF_ORIGIN_OBSERVED; BfOriginIds inputs; int found = 0; const char *current = frontier.ids[at++];
        if (contains(&seen, current)) continue;
        status = append_id(&seen, current); if (status != BF_ORIGIN_OK) return status;
        status = bf_occurrence_origin_get(origin, current, &kind, &found); if (status != BF_ORIGIN_OK) return status;
        if (!found) continue;
        if (kind == BF_ORIGIN_OBSERVED) { if (!contains(out, current)) { status = append_id(out, current); if (status != BF_ORIGIN_OK) return status; } }
        else if (kind == BF_ORIGIN_DERIVED) {
            status = bf_occurrence_origin_inputs(origin, current, &inputs); if (status != BF_ORIGIN_OK) return status;
            for (size_t index = 0; index < inputs.count; index++) if (!contains(&frontier, inputs.ids[index])) { status = append_id(&frontier, inputs.ids[index]); if (status != BF_ORIGIN_OK) return status; }
        }
    }
    /* Python returns sorted roots; make C callers receive the same deterministic order. */
    for (size_t left = 0; left < out->count; left++) for (size_t right = left + 1; right < out->count; right++) if (strcmp(out->ids[left], out->ids[right]) > 0) { char swap[BF_ORIGIN_ID_MAX]; memcpy(swap, out->ids[left], sizeof(swap)); memcpy(out->ids[left], out->ids[right], sizeof(swap)); memcpy(out->ids[right], swap, sizeof(swap)); }
    return BF_ORIGIN_OK;
}
