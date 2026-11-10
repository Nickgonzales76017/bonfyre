#include "bf_retention.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "retention_test:%d: %s\n", __LINE__, #expr); return 1; \
} } while (0)

static int has_id(char values[][BF_RETENTION_ID_MAX], size_t count, const char *wanted) {
    size_t index;
    for (index = 0; index < count; index++) if (!strcmp(values[index], wanted)) return 1;
    return 0;
}

int main(void) {
    BfRetention *retention = NULL;
    BfRetentionSweep sweep;
    BfRetentionDecision decision;
    BfRetentionTombstone stone;
    BfRetentionReconciliation reconciliation;
    sqlite3 *db = NULL;
    char body[64];
    int present = 0, released = 0;

    CHECK(sqlite3_open(":memory:", &db) == SQLITE_OK);
    CHECK(bf_retention_open_database(db, &retention, stderr) == BF_RETENTION_OK);
    CHECK(sqlite3_exec(db,
        "CREATE TABLE commitments(commitment_id TEXT PRIMARY KEY);"
        "CREATE TABLE commitment_events(seq INTEGER PRIMARY KEY, commitment_id TEXT, state TEXT);"
        "CREATE TABLE evidence_relations(evidence TEXT,kind TEXT,claim TEXT, PRIMARY KEY(evidence,kind,claim));",
        NULL, NULL, NULL) == SQLITE_OK);
    CHECK(bf_retention_retain(retention, "reaction", "night", "reaction", 0, "heart") == BF_RETENTION_OK);
    CHECK(bf_retention_retain(retention, "chat", "night", "chat", 0, "trees") == BF_RETENTION_OK);
    CHECK(bf_retention_retain(retention, "claim", "night", "claim", 0, "Friday") == BF_RETENTION_OK);
    CHECK(bf_retention_retain(retention, "purchase", "night", "commerce", 0, "ticket") == BF_RETENTION_OK);
    CHECK(bf_retention_retain(retention, "unknown", "night", "not_declared", 0, "x") == BF_RETENTION_INVALID);

    /* Flash expires, while a hold wins over elapsed time and durable facts never expire. */
    CHECK(bf_retention_hold(retention, "chat", "evidence_reference", "EvidenceGraph") == BF_RETENTION_OK);
    CHECK(bf_retention_collect(retention, 60LL * 24 * 60 * 60 * 1000, "night", &sweep) == BF_RETENTION_OK);
    CHECK(has_id(sweep.removed, sweep.removed_count, "reaction"));
    CHECK(!has_id(sweep.removed, sweep.removed_count, "chat"));
    CHECK(!has_id(sweep.removed, sweep.removed_count, "claim"));
    CHECK(sweep.refused_count == 3);
    CHECK(bf_retention_body(retention, "reaction", body, sizeof(body), &present) == BF_RETENTION_OK && !present);
    CHECK(bf_retention_happened(retention, "reaction"));
    CHECK(bf_retention_tombstone(retention, "reaction", &stone, &present) == BF_RETENTION_OK && present);
    CHECK(!strcmp(stone.occurrence, "night") && !strcmp(stone.kind, "reaction") && !strcmp(stone.tier, "flash"));
    CHECK(bf_retention_removable(retention, "purchase", 100000000000LL, &decision) == BF_RETENTION_NOT_REMOVABLE);
    CHECK(strstr(decision.refusal.reason, "time passing does not remove it") != NULL);

    CHECK(bf_retention_release(retention, "chat", "evidence_reference", "EvidenceGraph", &released) == BF_RETENTION_OK && released);
    CHECK(bf_retention_collect(retention, 60LL * 24 * 60 * 60 * 1000, "night", &sweep) == BF_RETENTION_OK);
    CHECK(has_id(sweep.removed, sweep.removed_count, "chat"));
    CHECK(bf_retention_collect(retention, 60LL * 24 * 60 * 60 * 1000, "night", &sweep) == BF_RETENTION_OK && sweep.removed_count == 0);

    /* Reconcile only owns its two derived hold kinds. A legal hold survives it. */
    CHECK(bf_retention_retain(retention, "source", "night-2", "chat", 0, "source body") == BF_RETENTION_OK);
    CHECK(bf_retention_hold(retention, "source", "legal_hold", "counsel") == BF_RETENTION_OK);
    CHECK(bf_retention_reconcile_holds(retention, &reconciliation) == BF_RETENTION_OK);
    CHECK(reconciliation.placed_count == 0 && reconciliation.released_count == 0);
    CHECK(bf_retention_removable(retention, "source", 60LL * 24 * 60 * 60 * 1000, &decision) == BF_RETENTION_NOT_REMOVABLE);
    CHECK(strstr(decision.refusal.reason, "legal_hold by counsel") != NULL);

    /* The bridge derives and releases a commitment hold from the shared owner store. */
    CHECK(bf_retention_release(retention, "source", "legal_hold", "counsel", &released) == BF_RETENTION_OK && released);
    CHECK(sqlite3_exec(db,
        "INSERT INTO commitments VALUES('c1');"
        "INSERT INTO commitment_events VALUES(1,'c1','open');",
        NULL, NULL, NULL) == SQLITE_OK);
    CHECK(bf_retention_rests_on(retention, "c1", "source") == BF_RETENTION_OK);
    CHECK(bf_retention_reconcile_holds(retention, &reconciliation) == BF_RETENTION_OK);
    CHECK(reconciliation.placed_count == 1 && !strcmp(reconciliation.placed[0], "open_obligation:source"));
    CHECK(bf_retention_removable(retention, "source", 60LL * 24 * 60 * 60 * 1000, &decision) == BF_RETENTION_NOT_REMOVABLE);
    CHECK(strstr(decision.refusal.reason, "open_obligation by CommitmentGraph") != NULL);
    CHECK(sqlite3_exec(db, "INSERT INTO commitment_events VALUES(2,'c1','discharged');", NULL, NULL, NULL) == SQLITE_OK);
    CHECK(bf_retention_reconcile_holds(retention, &reconciliation) == BF_RETENTION_OK);
    CHECK(reconciliation.released_count == 1 && !strcmp(reconciliation.released[0], "open_obligation:source"));
    CHECK(bf_retention_retain(retention, "evidence", "night-2", "chat", 0, "receipt") == BF_RETENTION_OK);
    CHECK(sqlite3_exec(db, "INSERT INTO evidence_relations VALUES('evidence','supports','claim:1');", NULL, NULL, NULL) == SQLITE_OK);
    CHECK(bf_retention_reconcile_holds(retention, &reconciliation) == BF_RETENTION_OK);
    CHECK(reconciliation.placed_count == 1 && !strcmp(reconciliation.placed[0], "evidence_reference:evidence"));
    CHECK(sqlite3_exec(db, "DELETE FROM evidence_relations;", NULL, NULL, NULL) == SQLITE_OK);
    CHECK(bf_retention_reconcile_holds(retention, &reconciliation) == BF_RETENTION_OK);
    CHECK(reconciliation.released_count == 1 && !strcmp(reconciliation.released[0], "evidence_reference:evidence"));

    bf_retention_close(retention);
    sqlite3_close(db);
    puts("retention_test: ok");
    return 0;
}
