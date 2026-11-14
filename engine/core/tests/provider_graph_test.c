/* Bids rank by a polarity-aware vector; authority gates selection; parity is proven only. */
#include "bf_provider_graph.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

int main(void) {
    sqlite3 *db; sqlite3_open(":memory:", &db);
    /*                       cost quality latency trust authority locality loss avail */
    double cheap[] =        {1,   5,      1,      1,    1,        0,       0,   1};
    double pricey[] =       {9,   5,      9,      1,    1,        0,       0,   1};
    double unauthorized[] = {0,   9,      0,      1,    0,        0,       0,   1};
    CHECK(bf_provider_record_bid(db, "transcribe", "pricey", pricey, NULL) == 0, "bid");
    CHECK(bf_provider_record_bid(db, "transcribe", "cheap", cheap, NULL) == 0, "bid");
    CHECK(bf_provider_record_bid(db, "transcribe", "free-but-unauthorized", unauthorized, NULL) == 0, "bid");
    BfProviderRank r[8];
    int n = bf_provider_tournament(db, "transcribe", NULL, 1, r, 8);
    CHECK(n == 2, "a bid without authority is not selectable");
    CHECK(n >= 2 && !strcmp(r[0].provider, "cheap") && !strcmp(r[1].provider, "pricey"), "cost and latency rank as spent, not as gain");
    for (int i = 0; i < n; i++) free((void *)r[i].provider);
    n = bf_provider_tournament(db, "transcribe", NULL, 0, r, 8);
    CHECK(n == 3, "authority requirement is a policy choice the caller states");
    for (int i = 0; i < n; i++) free((void *)r[i].provider);
    double only_quality[] = {0, 1, 0, 0, 0, 0, 0, 0};
    double q_hi[] = {9, 8, 9, 1, 1, 0, 0, 1};
    bf_provider_record_bid(db, "transcribe", "quality", q_hi, NULL);
    n = bf_provider_tournament(db, "transcribe", only_quality, 1, r, 8);
    CHECK(n == 3 && !strcmp(r[0].provider, "quality"), "weights reorder without changing the stored vector");
    for (int i = 0; i < n; i++) free((void *)r[i].provider);
    sqlite3_stmt *q; char m[256] = "";
    sqlite3_prepare_v2(db, "SELECT metric FROM provider_bids WHERE provider='cheap'", -1, &q, NULL);
    if (sqlite3_step(q) == SQLITE_ROW) snprintf(m, sizeof m, "%s", sqlite3_column_text(q, 0)); sqlite3_finalize(q);
    CHECK(strstr(m, "\"semantic_loss\":0") && strstr(m, "\"cost\":1"), "the metric vector is kept intact");

    char loc[64];
    CHECK(bf_provider_record_placement(db, "model:whisper", "node:mac") == 0, "place");
    CHECK(bf_provider_placement_of(db, "model:whisper", loc, sizeof loc) == 1 && !strcmp(loc, "node:mac"), "placement read");
    CHECK(bf_provider_placement_of(db, "model:none", loc, sizeof loc) == 0, "unplaced");

    bf_provider_record_parity(db, "transcribe", "native", "python", 1);
    bf_provider_record_parity(db, "transcribe", "wasm", "native", 0);
    CHECK(bf_provider_parity_group(db, "transcribe", NULL, NULL) == 1, "a pair that only ran is not parity");
    bf_provider_record_parity(db, "transcribe", "python", "native", 0);
    CHECK(bf_provider_parity_group(db, "transcribe", NULL, NULL) == 0, "pairs are unordered; a later disproof replaces the proof");
    sqlite3_close(db);
    if (failures) return 1;
    puts("provider graph bids, placement, parity: PASS");
    return 0;
}
