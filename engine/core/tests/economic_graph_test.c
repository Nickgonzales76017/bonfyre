/* Money relations, non-cash value, and cost projections that are never booked. */
#include "bf_economic_graph.h"

#include <math.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

typedef struct { int n, any_amount; char last[64]; } Acc;
static int acc(const char *k, const char *d, double a, int has, void *c) { Acc *x = c; (void)k; (void)a; x->n++; x->any_amount |= has; snprintf(x->last, sizeof x->last, "%s", d); return 0; }

int main(void) {
    sqlite3 *db; sqlite3_open(":memory:", &db);
    CHECK(bf_econ_record_money(db, "foundation", "grants", "org", 50000, 1) == BF_ECON_OK, "money");
    CHECK(bf_econ_record_money(db, "org", "owes", "vendor", 0, 0) == BF_ECON_OK, "unpriced relation");
    CHECK(bf_econ_record_money(db, "org", "gifts", "x", 1, 1) == BF_ECON_UNKNOWN_KIND, "undeclared money relation refused");
    Acc a = {0};
    CHECK(bf_econ_money_out(db, "org", acc, &a) == 1 && a.any_amount == 0, "an unpriced relation carries no figure, not zero");

    CHECK(bf_econ_record_value(db, "permit", "unlocks", "lease") == BF_ECON_OK, "value");
    CHECK(bf_econ_record_value(db, "lease", "creates_capacity", "kitchen") == BF_ECON_OK, "value 2");
    CHECK(bf_econ_record_value(db, "kitchen", "unlocks", "permit") == BF_ECON_OK, "cycle");
    CHECK(bf_econ_record_value(db, "permit", "worth_dollars", "cash") == BF_ECON_UNKNOWN_KIND, "value is not cash");
    Acc v = {0};
    CHECK(bf_econ_value_reachable(db, "permit", acc, &v) == 2 && v.any_amount == 0, "value reachable transitively, finite, no amounts");

    double total = -1;
    bf_econ_record_cost(db, "mission:1", "compute", 12.5);
    bf_econ_record_cost(db, "mission:1", "people", 100);
    CHECK(bf_econ_mission_cost(db, "mission:1", &total) == 0 && fabs(total - 112.5) < 1e-9, "mission cost sums projections");
    CHECK(bf_econ_cost_booked(db, "mission:1", "compute") == 0, "a projection is never booked");
    CHECK(bf_econ_cost_booked(db, "mission:1", "none") == 0, "unknown is not booked");
    sqlite3_close(db);
    if (failures) return 1;
    puts("economic graph money, value, cost: PASS");
    return 0;
}
