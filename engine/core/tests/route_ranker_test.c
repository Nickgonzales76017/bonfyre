/* The ranker reorders survivors; weak signals stay zero. */
#include "bf_route_ranker.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

int main(void) {
    sqlite3 *db; sqlite3_open(":memory:", &db);
    BfRouteRecord rec[16]; BfRouteRanker r;
    CHECK(bf_route_records_from_work(db, rec, 16) == 0, "no work table, no records");
    sqlite3_exec(db, "CREATE TABLE work_items(id TEXT, priority INTEGER, target_plane TEXT, attempt INTEGER, state TEXT);"
        "INSERT INTO work_items VALUES('a',10,'plane:x',0,'satisfied'),('b',8,'plane:y',0,'effected'),"
        "('c',9,'',2,'failed'),('d',1,'',1,'failed'),('e',2,'plane:z',0,'satisfied'),('f',10,'',0,'failed');", 0, 0, 0);
    long n = bf_route_records_from_work(db, rec, 16);
    CHECK(n == 6, "records from real outcomes");
    bf_route_train(rec, (size_t)n, 0.05, &r);
    CHECK(r.w[1] == 1, "having a target plane predicts success");
    CHECK(r.w[2] == 1, "retry-free predicts success");
    BfRouteRecord weak[2] = {{{0.50, 0, 0}, 1}, {{0.52, 0, 0}, 0}};
    BfRouteRanker rw; bf_route_train(weak, 2, 0.05, &rw);
    CHECK(rw.w[0] == 0, "a weak signal collapses to zero");
    BfRouteCandidate c[3] = {{"zeta", {0.9, 0, 0}, 0}, {"alpha", {0.1, 1, 1}, 0}, {"beta", {0.1, 1, 1}, 0}};
    bf_route_rank(&r, c, 3);
    CHECK(!strcmp(c[0].id, "alpha") && !strcmp(c[1].id, "beta") && !strcmp(c[2].id, "zeta"), "best first, ties by id, nobody dropped");
    BfRouteRanker empty; bf_route_train(NULL, 0, 0.05, &empty);
    CHECK(empty.trained_on == 0 && !empty.w[0] && !empty.w[1] && !empty.w[2], "untrained ranker is neutral");
    sqlite3_close(db);
    if (failures) return 1;
    puts("ternary route ranker: PASS");
    return 0;
}
