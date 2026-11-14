/* A pattern cluster may propose a candidate primitive and never promote it. */
#include "bf_pattern.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

int main(void) {
    sqlite3 *db; sqlite3_open(":memory:", &db);
    const char *traces[] = {"run:2", "run:1"}, *ev[] = {"receipt:a", "receipt:b"}, *empty[] = {""};
    char st[32];
    CHECK(bf_pattern_record_cluster(db, "c1", "retries succeed after backoff", traces, 2, ev, 2, NULL) == BF_PATTERN_OK, "record cluster");
    CHECK(bf_pattern_record_cluster(db, "c2", "x", traces, 2, ev, 2, "proven") == BF_PATTERN_STATUS, "a cluster cannot claim proven");
    CHECK(bf_pattern_record_cluster(db, "c2", "x", traces, 2, ev, 2, "promoted") == BF_PATTERN_STATUS, "a cluster cannot claim promoted");
    CHECK(bf_pattern_record_cluster(db, "c3", "x", traces, 2, NULL, 0, NULL) == BF_PATTERN_INVALID, "a cluster needs evidence");
    CHECK(bf_pattern_record_cluster(db, "c3", "x", NULL, 0, ev, 2, NULL) == BF_PATTERN_INVALID, "a cluster needs traces");
    CHECK(bf_pattern_record_cluster(db, "c3", "x", empty, 1, ev, 2, NULL) == BF_PATTERN_INVALID, "an empty trace is refused");
    CHECK(bf_pattern_cluster_traces(db, "c1", NULL, NULL) == 2, "members kept");
    CHECK(bf_pattern_propose(db, "p1", "nope", "x") == BF_PATTERN_UNKNOWN_CLUSTER, "proposal needs a known cluster");
    CHECK(bf_pattern_propose(db, "p1", "c1", "backoff-retry primitive") == BF_PATTERN_OK, "propose");
    CHECK(bf_pattern_candidate_status(db, "p1", st, sizeof st) == 1 && !strcmp(st, "candidate"), "proposed is only a candidate");
    /* proof and promotion happen elsewhere; a later proposal never resets them */
    sqlite3_exec(db, "UPDATE candidate_primitives SET status='proven', proof_ref='proof:x' WHERE candidate_id='p1'", 0, 0, 0);
    CHECK(bf_pattern_propose(db, "p1", "c1", "restated") == BF_PATTERN_OK, "re-propose");
    CHECK(bf_pattern_candidate_status(db, "p1", st, sizeof st) == 1 && !strcmp(st, "proven"), "re-proposal keeps the status set elsewhere");
    char ev_json[128] = ""; sqlite3_stmt *q;
    sqlite3_prepare_v2(db, "SELECT evidence_refs FROM pattern_clusters WHERE cluster_id='c1'", -1, &q, NULL);
    if (sqlite3_step(q) == SQLITE_ROW) snprintf(ev_json, sizeof ev_json, "%s", sqlite3_column_text(q, 0)); sqlite3_finalize(q);
    CHECK(!strcmp(ev_json, "[\"receipt:a\",\"receipt:b\"]"), "evidence refs stored as the reference stored them");
    sqlite3_close(db);
    if (failures) return 1;
    puts("pattern graph propose-not-promote: PASS");
    return 0;
}
