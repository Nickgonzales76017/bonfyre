/* Relationships advance only by recorded transitions; a shadow institution
 * activates only through a real relationship. */
#include "bf_relationship.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

int main(void) {
    sqlite3 *db; sqlite3_open(":memory:", &db);
    char st[40], why[80];
    CHECK(bf_relationship_stage_at_least(db, "actor:ed", "acm", "engaged") == -2, "no relationship is unknown, not no");
    CHECK(bf_relationship_record(db, "r1", "actor:ed", "acm", NULL, "", "2026-11-01T00:00:00+00:00") == 0, "record");
    CHECK(bf_relationship_record(db, "r9", "actor:ed", "acm", "friendly", "", NULL) == -1, "undeclared stage refused");
    CHECK(bf_relationship_advance(db, "r1", "engaged", "reply thread", "2026-11-02T00:00:00+00:00", 0) == 1, "advance");
    CHECK(bf_relationship_advance(db, "r1", "contacted", "", NULL, 0) == 0, "backward refused");
    CHECK(bf_relationship_advance(db, "nope", "engaged", "", NULL, 0) == 0, "unknown relationship refused");
    CHECK(bf_relationship_stage_at_least(db, "actor:ed", "acm", "replied") == 1, "lower stages implied");
    CHECK(bf_relationship_stage_at_least(db, "actor:ed", "acm", "collaborated") == 0, "higher stages not implied");
    CHECK(bf_relationship_stage_at_least(db, "actor:ed", "bernstein", "discovered") == -2, "another profile is independent");
    CHECK(bf_relationship_transition_count(db, "r1") == 1, "transition recorded with evidence");
    CHECK(bf_relationship_advance(db, "r1", "contacted", "correction", NULL, 1) == 1, "deliberate correction allowed");

    bf_shadow_record(db, "inst:acm", "ACM", "actor:ed", "acm", "[\"council\"]", "[]", "public page", NULL);
    bf_shadow_conversion_stage(db, "inst:acm", st, sizeof st, why, sizeof why);
    CHECK(!strcmp(st, "observed"), "unmapped structure is only observed");
    bf_shadow_record(db, "inst:acm", "ACM", "actor:ed", "acm", "[\"council\",\"editors\",\"board\"]", "[\"form:cfp\"]", "public page", NULL);
    bf_shadow_conversion_stage(db, "inst:acm", st, sizeof st, why, sizeof why);
    CHECK(!strcmp(st, "shadow"), "a richer public picture with a regressed relationship is still shadow");
    bf_relationship_advance(db, "r1", "engaged", "re-engaged", NULL, 0);
    bf_shadow_conversion_stage(db, "inst:acm", st, sizeof st, why, sizeof why);
    CHECK(!strcmp(st, "relationship_established"), "engaged relationship establishes");
    bf_relationship_advance(db, "r1", "collaborated", "co-reviewed", NULL, 0);
    bf_shadow_conversion_stage(db, "inst:acm", st, sizeof st, why, sizeof why);
    CHECK(!strcmp(st, "active") && !strcmp(why, "relationship collaborated"), "collaboration activates");
    bf_shadow_conversion_stage(db, "inst:none", st, sizeof st, why, sizeof why);
    CHECK(!strcmp(st, "observed") && !strcmp(why, "not recorded"), "unrecorded institution");
    sqlite3_close(db);
    if (failures) return 1;
    puts("relationship trajectory and shadow institution conversion: PASS");
    return 0;
}
