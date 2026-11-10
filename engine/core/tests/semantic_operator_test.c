#include "bf_semantic_operator.h"

#include <stdio.h>
#include <string.h>

static int fail(const char *message)
{
    fprintf(stderr, "FAIL: %s\n", message);
    return 1;
}

static BfHouseholdProjection housing(const char *listing_id, const char *unit)
{
    return (BfHouseholdProjection){
        .domain = "housing",
        .listing_id = listing_id,
        .source_file = "household/housing/current-listings.jsonl",
        .source_ref = "housing/source-receipts.jsonl#milo",
        .canonical_document = "{\"listing_id\":\"milo\"}",
        .observed_claims = BF_HOUSEHOLD_OBSERVED_AVAILABILITY |
                           BF_HOUSEHOLD_OBSERVED_DOG_ACCESS |
                           BF_HOUSEHOLD_OBSERVED_PRICE,
        .property_id = "property:milo",
        .unit_identity = unit,
    };
}

int main(void)
{
    BfHouseholdRecord left, right, food;
    BfSemanticProgram select_a, select_b;
    BfHouseholdProofVerdict proof;
    BfHouseholdReconciliationResult reconciliation;
    BfHouseholdProjection first = housing("milo-a", "floorplan:A");
    BfHouseholdProjection second = housing("milo-a1", "floorplan:A1");
    BfHouseholdProjection offer = {
        .domain = "food", .offer_id = "beans",
        .source_file = "household/food/current-offers.jsonl",
        .source_ref = "https://example.test/beans",
        .canonical_document = "{\"offer_id\":\"beans\",\"merchant\":\"Target\"}",
        .observed_claims = BF_HOUSEHOLD_OBSERVED_PRICE,
    };

    if (bf_household_projection_bind(&first, &left) != BF_HOUSEHOLD_OK ||
        bf_household_projection_bind(&second, &right) != BF_HOUSEHOLD_OK ||
        bf_household_projection_bind(&offer, &food) != BF_HOUSEHOLD_OK) return fail("bind source projections");
    if (strcmp(left.address, "household:housing:listing:milo-a") != 0 ||
        strcmp(food.address, "household:food:offer:beans") != 0 || strlen(left.content_sha256) != 64) {
        return fail("stable source addresses and content digest");
    }

    BfHouseholdRecord records_a[] = { left, right };
    BfHouseholdRecord records_b[] = { right, left };
    if (bf_semantic_program_lower(BF_SEMANTIC_SELECT, records_a, 2, &select_a) != BF_HOUSEHOLD_OK ||
        bf_semantic_program_lower(BF_SEMANTIC_SELECT, records_b, 2, &select_b) != BF_HOUSEHOLD_OK ||
        select_a.writes != 0 || !select_a.admitted || strcmp(select_a.digest, select_b.digest) != 0) {
        return fail("deterministic read-only lowering");
    }
    if (bf_household_reconcile(&left, &right, &reconciliation) != BF_HOUSEHOLD_OK ||
        reconciliation.disposition != BF_HOUSEHOLD_COEXISTING_DISTINCT_UNITS ||
        !reconciliation.merge_prohibited) return fail("distinct units must not merge");

    if (bf_household_prove(&left, BF_HOUSEHOLD_PROVE_AVAILABILITY, &proof) != BF_HOUSEHOLD_OK ||
        !proof.observed || proof.proven || !strstr(proof.reason, "does not independently prove")) {
        return fail("observed availability must remain below proof");
    }

    offer.source_ref = "";
    if (bf_household_projection_bind(&offer, &food) != BF_HOUSEHOLD_MISSING_SOURCE) {
        return fail("missing evidence must fail closed");
    }
    first.candidate_id = "also-an-identity";
    if (bf_household_projection_bind(&first, &left) != BF_HOUSEHOLD_UNSUPPORTED_IDENTITY) {
        return fail("ambiguous housing identity must fail closed");
    }
    if (bf_semantic_program_lower(BF_SEMANTIC_SELECT, records_a, 0, &select_a) != BF_HOUSEHOLD_INVALID ||
        bf_semantic_program_lower(BF_SEMANTIC_SELECT, records_a, 17, &select_a) != BF_HOUSEHOLD_TOO_MANY_READS) {
        return fail("invalid read closure must fail closed");
    }

    printf("semantic_operator_test: PASS\n");
    return 0;
}
