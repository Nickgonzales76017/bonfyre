/* Native CommerceChain owner.  This uses the same commerce_progress relation
 * as the archived Python reference and closes the last Python-only commercial
 * state machine: a stage is an observed/receipted fact, never a conclusion. */
#include "bf_commerce.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>

struct BfCommerce { sqlite3 *db; int owns_db; FILE *err; };
typedef struct BfCommerceMeaning { const char *name; const char *owner; const char *does_not_establish; } BfCommerceMeaning;
static const BfCommerceMeaning MEANINGS[BF_COMMERCE_STAGE_COUNT] = {
    {"product", "ObjectContract / catalogue", "that anybody has one, sells one, or will part with one"},
    {"listing", "merchant or marketplace ForeignTwin", "that the thing exists, is in stock, or is theirs"},
    {"inventory", "merchant system of record", "that the count is current, or that any of it is reachable by this buyer"},
    {"availability_observation", "AvailabilityObservation, receipted", "that it will still be obtainable when this buyer acts"},
    {"offer", "MoneyGraph / merchant", "terms priced for this particular buyer"},
    {"quote", "ProtocolContract quote-accept", "a reservation; the resource is not held"},
    {"cart", "surface state", "intent to buy any of them"},
    {"checkout", "ActionAperture", "that anybody committed to anything"},
    {"commitment", "CommitmentGraph", "that money moved, or that the goods exist"},
    {"payment", "payment rail", "that funds settled, or that they will"},
    {"settlement", "MoneyGraph / settlement rail", "that anything was shipped"},
    {"fulfilment", "merchant / fulfilment provider", "that it arrived"},
    {"delivery", "carrier / EPCIS custody event", "that it was what they wanted, or that it is now theirs"},
    {"satisfaction", "the buyer, and only the buyer", "that the obligation is discharged in law or in the ledger"},
    {"ownership", "OwnershipTransfer / registry", "anything about how the buyer feels about it"}
};
static const char *COMMERCE_SCHEMA = "CREATE TABLE IF NOT EXISTS commerce_progress(exchange TEXT NOT NULL,stage TEXT NOT NULL,owner TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '',at TEXT NOT NULL DEFAULT '',PRIMARY KEY(exchange,stage));";
static int present(const char *value) { return value && value[0]; }
static int valid(BfCommerceStage stage) { return stage >= BF_COMMERCE_PRODUCT && stage < BF_COMMERCE_STAGE_COUNT; }

const char *bf_commerce_stage_name(BfCommerceStage stage) { return valid(stage) ? MEANINGS[stage].name : NULL; }
const char *bf_commerce_stage_owner(BfCommerceStage stage) { return valid(stage) ? MEANINGS[stage].owner : NULL; }
const char *bf_commerce_stage_does_not_establish(BfCommerceStage stage) { return valid(stage) ? MEANINGS[stage].does_not_establish : NULL; }
const char *bf_commerce_status_name(BfCommerceStatus status) { switch (status) { case BF_COMMERCE_OK: return "ok"; case BF_COMMERCE_INVALID: return "invalid"; case BF_COMMERCE_NOT_FOUND: return "not_found"; case BF_COMMERCE_STORAGE: return "storage"; case BF_COMMERCE_CAPACITY: return "capacity"; } return "unknown"; }
int bf_commerce_implies(BfCommerceStage earlier, BfCommerceStage later) { return valid(earlier) && valid(later) && earlier == later; }

static BfCommerceStatus apply_schema(sqlite3 *db, FILE *err) { char *message = NULL; if (sqlite3_exec(db, COMMERCE_SCHEMA, NULL, NULL, &message) != SQLITE_OK) { if (err) fprintf(err, "commerce: schema failed: %s\n", message ? message : "?"); sqlite3_free(message); return BF_COMMERCE_STORAGE; } return BF_COMMERCE_OK; }
int bf_commerce_open(const char *database_path, BfCommerce **out, FILE *err) { sqlite3 *db = NULL; BfCommerce *commerce; if (!present(database_path) || !out || sqlite3_open(database_path, &db) != SQLITE_OK) { sqlite3_close(db); return BF_COMMERCE_STORAGE; } sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL); if (apply_schema(db, err) != BF_COMMERCE_OK) { sqlite3_close(db); return BF_COMMERCE_STORAGE; } commerce = calloc(1, sizeof(*commerce)); if (!commerce) { sqlite3_close(db); return BF_COMMERCE_STORAGE; } commerce->db = db; commerce->owns_db = 1; commerce->err = err; *out = commerce; return BF_COMMERCE_OK; }
int bf_commerce_open_database(void *sqlite_database, BfCommerce **out, FILE *err) { BfCommerce *commerce; if (!sqlite_database || !out || apply_schema(sqlite_database, err) != BF_COMMERCE_OK) return BF_COMMERCE_STORAGE; commerce = calloc(1, sizeof(*commerce)); if (!commerce) return BF_COMMERCE_STORAGE; commerce->db = sqlite_database; commerce->err = err; *out = commerce; return BF_COMMERCE_OK; }
void bf_commerce_close(BfCommerce *commerce) { if (!commerce) return; if (commerce->owns_db) sqlite3_close(commerce->db); free(commerce); }

BfCommerceStatus bf_commerce_reached(BfCommerce *commerce, const char *exchange, BfCommerceStage stage, int *out) {
    sqlite3_stmt *stmt = NULL;
    if (!commerce || !present(exchange) || !valid(stage) || !out) return BF_COMMERCE_INVALID;
    if (sqlite3_prepare_v2(commerce->db, "SELECT 1 FROM commerce_progress WHERE exchange=? AND stage=?", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMERCE_STORAGE;
    sqlite3_bind_text(stmt, 1, exchange, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, bf_commerce_stage_name(stage), -1, SQLITE_STATIC);
    *out = sqlite3_step(stmt) == SQLITE_ROW; sqlite3_finalize(stmt); return BF_COMMERCE_OK;
}

BfCommerceStatus bf_commerce_record(BfCommerce *commerce, const char *exchange, BfCommerceStage stage, const char *owner, const char *detail, const char *at) {
    sqlite3_stmt *stmt = NULL; int prior = 0;
    if (!commerce || !present(exchange) || !valid(stage) || !present(owner)) return BF_COMMERCE_INVALID;
    if (stage > BF_COMMERCE_PRODUCT) { BfCommerceStatus status = bf_commerce_reached(commerce, exchange, (BfCommerceStage)(stage - 1), &prior); if (status != BF_COMMERCE_OK) return status; if (!prior) return BF_COMMERCE_NOT_FOUND; }
    if (sqlite3_prepare_v2(commerce->db, "INSERT OR REPLACE INTO commerce_progress(exchange,stage,owner,detail,at) VALUES(?,?,?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMERCE_STORAGE;
    sqlite3_bind_text(stmt, 1, exchange, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, bf_commerce_stage_name(stage), -1, SQLITE_STATIC); sqlite3_bind_text(stmt, 3, owner, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 4, detail ? detail : "", -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 5, at ? at : "", -1, SQLITE_TRANSIENT);
    int done = sqlite3_step(stmt) == SQLITE_DONE; sqlite3_finalize(stmt); return done ? BF_COMMERCE_OK : BF_COMMERCE_STORAGE;
}

BfCommerceStatus bf_commerce_furthest(BfCommerce *commerce, const char *exchange, BfCommerceStage *out, int *out_found) {
    sqlite3_stmt *stmt = NULL; BfCommerceStage stage;
    if (!commerce || !present(exchange) || !out || !out_found) return BF_COMMERCE_INVALID;
    *out_found = 0;
    if (sqlite3_prepare_v2(commerce->db, "SELECT stage FROM commerce_progress WHERE exchange=?", -1, &stmt, NULL) != SQLITE_OK) return BF_COMMERCE_STORAGE;
    sqlite3_bind_text(stmt, 1, exchange, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW) for (stage = BF_COMMERCE_PRODUCT; stage < BF_COMMERCE_STAGE_COUNT; stage++) if (strcmp((const char *)sqlite3_column_text(stmt, 0), bf_commerce_stage_name(stage)) == 0 && (!*out_found || stage > *out)) { *out = stage; *out_found = 1; }
    sqlite3_finalize(stmt); return BF_COMMERCE_OK;
}

BfCommerceStatus bf_commerce_outstanding(BfCommerce *commerce, const char *exchange, BfCommerceProgress *out) {
    BfCommerceStage stage; BfCommerceStatus status; int reached;
    if (!commerce || !present(exchange) || !out) return BF_COMMERCE_INVALID;
    memset(out, 0, sizeof(*out));
    for (stage = BF_COMMERCE_PRODUCT; stage < BF_COMMERCE_STAGE_COUNT; stage++) { status = bf_commerce_reached(commerce, exchange, stage, &reached); if (status != BF_COMMERCE_OK) return status; if (!reached) out->stages[out->count++] = stage; }
    return BF_COMMERCE_OK;
}
