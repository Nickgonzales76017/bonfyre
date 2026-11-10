#include "bf_commerce.h"

#include <stdio.h>
#include <string.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "commerce_test:%d: %s\n", __LINE__, #condition); return 1; } } while (0)

int main(void) {
    BfCommerce *commerce = NULL; BfCommerceStage stage; BfCommerceProgress outstanding; int reached, found;
    CHECK(bf_commerce_open(":memory:", &commerce, stderr) == BF_COMMERCE_OK);
    for (stage = BF_COMMERCE_PRODUCT; stage < BF_COMMERCE_STAGE_COUNT; stage++) {
        BfCommerceStage other;
        for (other = BF_COMMERCE_PRODUCT; other < BF_COMMERCE_STAGE_COUNT; other++) CHECK(bf_commerce_implies(stage, other) == (stage == other));
    }
    CHECK(bf_commerce_record(commerce, "ex:boots", BF_COMMERCE_DELIVERY, "carrier", "", "") == BF_COMMERCE_NOT_FOUND);
    for (stage = BF_COMMERCE_PRODUCT; stage <= BF_COMMERCE_COMMITMENT; stage++) CHECK(bf_commerce_record(commerce, "ex:boots", stage, bf_commerce_stage_owner(stage), "", "") == BF_COMMERCE_OK);
    CHECK(bf_commerce_reached(commerce, "ex:boots", BF_COMMERCE_COMMITMENT, &reached) == BF_COMMERCE_OK && reached);
    CHECK(bf_commerce_reached(commerce, "ex:boots", BF_COMMERCE_PAYMENT, &reached) == BF_COMMERCE_OK && !reached);
    CHECK(bf_commerce_furthest(commerce, "ex:boots", &stage, &found) == BF_COMMERCE_OK && found && stage == BF_COMMERCE_COMMITMENT);
    CHECK(bf_commerce_outstanding(commerce, "ex:boots", &outstanding) == BF_COMMERCE_OK && outstanding.count == 6 && outstanding.stages[0] == BF_COMMERCE_PAYMENT && outstanding.stages[5] == BF_COMMERCE_OWNERSHIP);
    CHECK(bf_commerce_record(commerce, "ex:bad", BF_COMMERCE_PRODUCT, "", "", "") == BF_COMMERCE_INVALID);
    CHECK(strstr(bf_commerce_stage_does_not_establish(BF_COMMERCE_DELIVERY), "now theirs"));
    bf_commerce_close(commerce); puts("commerce_test: PASS"); return 0;
}
