#include "bf_standing_address.h"

#include <stdio.h>
#include <string.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "standing_address_test:%d: %s\n", __LINE__, #condition); return 1; } } while (0)

int main(void) {
    BfStandingAddress address; BfStandingRanking ranking; BfInformationPolicy policy;
    BfOpportunity candidates[] = {{"live:neon-june", BF_ACTION_WATCH, BF_TERRITORY_LIVE, 0.9, "", 1}, {"event:dj", BF_ACTION_GO, BF_TERRITORY_LIVE, 0.7, "", 1}, {"offer:guest", BF_ACTION_JOIN, BF_TERRITORY_LIVE, 0.6, "", 0}, {"product:x", BF_ACTION_BUY, BF_TERRITORY_SHOP, 1.0, "", 1}};
    CHECK(bf_standing_address_init(&address, "person:nick", BF_TERRITORY_LIVE, NULL) == BF_STANDING_OK);
    CHECK(bf_standing_compile(&address, candidates, 4, &ranking) == BF_STANDING_OK && ranking.count == 3 && ranking.candidate_indices[0] == 0 && ranking.candidate_indices[2] == 2);
    for (int index = 0; index < 50; index++) CHECK(bf_standing_observe_attention(&address, "offer:guest", BF_ATTENTION_DWELLED, 1.0) == BF_STANDING_OK);
    CHECK(bf_standing_compile(&address, candidates, 4, &ranking) == BF_STANDING_OK && ranking.candidate_indices[2] == 2);
    CHECK(bf_standing_observe_attention(&address, "event:dj", (BfAttentionSignal)99, 1.0) == BF_STANDING_INVALID);
    CHECK(bf_standing_opportunity_validate(&(BfOpportunity){"offer:x", BF_ACTION_BUY, BF_TERRITORY_LIVE, 0, "", 1}) == BF_STANDING_INVALID);
    bf_information_policy_init(&policy);
    CHECK(bf_information_policy_disclose(&policy, "subject", bf_information_policy_audience_mask(BF_AUDIENCE_INSTITUTION), "not public") == BF_INFORMATION_POLICY_OK);
    CHECK(bf_standing_address_init(&address, "person:nick", BF_TERRITORY_LIVE, &policy) == BF_STANDING_OK);
    CHECK(bf_standing_compile(&address, candidates, 4, &ranking) == BF_STANDING_OK && ranking.count == 0);
    puts("standing_address_test: PASS"); return 0;
}
