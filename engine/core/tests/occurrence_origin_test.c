#include "bf_occurrence_origin.h"

#include <stdio.h>
#include <string.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "occurrence_origin_test:%d: %s\n", __LINE__, #condition); return 1; } } while (0)

static BfOccurrenceOriginSpec spec(const char *id, BfOriginKind kind, const char *subject) {
    BfOccurrenceOriginSpec value = {0}; value.occurrence_id = id; value.origin = kind; value.subject = subject; return value;
}

int main(void) {
    BfOccurrenceOrigin *origin = NULL; BfOccurrenceOriginSpec value; BfOriginIds roots, observed, derived, effect; BfOriginKind kind; int found;
    const char *d1_inputs[] = {"o1"}; const char *d2_inputs[] = {"d1", "o2"};
    CHECK(bf_occurrence_origin_open(":memory:", &origin, stderr) == BF_ORIGIN_OK);
    value = spec("o1", BF_ORIGIN_OBSERVED, "live:night"); value.source = "audio:mic";
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_OK);
    value = spec("o2", BF_ORIGIN_OBSERVED, "live:night"); value.source = "calendar:trees";
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_OK);
    value = spec("d1", BF_ORIGIN_DERIVED, "live:night"); value.derived_from = d1_inputs; value.derived_from_count = 1;
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_OK);
    value = spec("d2", BF_ORIGIN_DERIVED, "live:night"); value.derived_from = d2_inputs; value.derived_from_count = 2;
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_OK);
    value = spec("e1", BF_ORIGIN_EFFECT, "live:night"); value.authority = "publish:clip";
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_OK);
    CHECK(bf_occurrence_origin_get(origin, "d2", &kind, &found) == BF_ORIGIN_OK && found && kind == BF_ORIGIN_DERIVED);
    CHECK(bf_occurrence_origin_caused_by_us(origin, "o1") == 0 && bf_occurrence_origin_caused_by_us(origin, "e1") == 1);
    CHECK(bf_occurrence_origin_provenance(origin, "d2", &roots) == BF_ORIGIN_OK && roots.count == 2 && !strcmp(roots.ids[0], "o1") && !strcmp(roots.ids[1], "o2"));
    CHECK(bf_occurrence_origin_by_subject(origin, "live:night", &observed, &derived, &effect) == BF_ORIGIN_OK && observed.count == 2 && derived.count == 2 && effect.count == 1);
    value = spec("bad-observed", BF_ORIGIN_OBSERVED, "live:night"); value.authority = "observe:foreign";
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_INVALID);
    value = spec("bad-derived", BF_ORIGIN_DERIVED, "live:night");
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_INVALID);
    value = spec("bad-effect", BF_ORIGIN_EFFECT, "live:night");
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_INVALID);
    value = spec("o1", BF_ORIGIN_EFFECT, "live:night"); value.authority = "publish:clip";
    CHECK(bf_occurrence_origin_record(origin, &value) == BF_ORIGIN_EXISTS);
    bf_occurrence_origin_close(origin); puts("occurrence_origin_test: PASS"); return 0;
}
