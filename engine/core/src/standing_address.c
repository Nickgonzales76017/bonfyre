/* Native implementation of the cheap opportunity ranking tier. The public
 * identity remains StandingAddress while capability closure may choose a more
 * elaborate realization later; no ranking realization owns an obligation. */
#include "bf_standing_address.h"

#include <string.h>

static int present(const char *value) { return value && value[0]; }
static int valid_territory(BfTerritory territory) { return territory >= BF_TERRITORY_LIFE && territory <= BF_TERRITORY_WORK; }
static int valid_action(BfAction action) { return action >= BF_ACTION_WATCH && action <= BF_ACTION_ESCALATE; }
static int valid_signal(BfAttentionSignal signal) { return signal >= BF_ATTENTION_WATCHED && signal <= BF_ATTENTION_MUTED; }
static const char *const TERRITORIES[] = {"life", "live", "shop", "work"};
static const char *const ACTIONS[] = {"WATCH", "JOIN", "GO", "FOLLOW", "SAVE", "BOOK", "BUY", "CONTRIBUTE", "MEET", "APPLY", "HELP", "OFFER", "APPROVE", "CLAIM", "FUND", "CONTACT", "PLAN", "REMEMBER", "SHARE", "MOVE", "COMPARE", "REQUEST", "SELL", "DO", "REVIEW", "ASSIGN", "RESPOND", "ESCALATE"};
static const char *const SIGNALS[] = {"watched", "swiped_past", "reacted", "dwelled", "followed_back", "muted"};

static int action_in_territory(BfAction action, BfTerritory territory) {
    static const unsigned int masks[] = {
        (1u<<BF_ACTION_MEET)|(1u<<BF_ACTION_JOIN)|(1u<<BF_ACTION_GO)|(1u<<BF_ACTION_SAVE)|(1u<<BF_ACTION_HELP)|(1u<<BF_ACTION_FOLLOW)|(1u<<BF_ACTION_PLAN)|(1u<<BF_ACTION_REMEMBER)|(1u<<BF_ACTION_SHARE)|(1u<<BF_ACTION_MOVE)|(1u<<BF_ACTION_ASSIGN),
        (1u<<BF_ACTION_WATCH)|(1u<<BF_ACTION_JOIN)|(1u<<BF_ACTION_GO)|(1u<<BF_ACTION_FOLLOW)|(1u<<BF_ACTION_SAVE)|(1u<<BF_ACTION_SHARE),
        (1u<<BF_ACTION_BUY)|(1u<<BF_ACTION_OFFER)|(1u<<BF_ACTION_JOIN)|(1u<<BF_ACTION_SAVE)|(1u<<BF_ACTION_COMPARE)|(1u<<BF_ACTION_REQUEST)|(1u<<BF_ACTION_SELL)|(1u<<BF_ACTION_FUND)|(1u<<BF_ACTION_CONTRIBUTE),
        (1u<<BF_ACTION_APPLY)|(1u<<BF_ACTION_APPROVE)|(1u<<BF_ACTION_CLAIM)|(1u<<BF_ACTION_FUND)|(1u<<BF_ACTION_CONTACT)|(1u<<BF_ACTION_BOOK)|(1u<<BF_ACTION_DO)|(1u<<BF_ACTION_REVIEW)|(1u<<BF_ACTION_ASSIGN)|(1u<<BF_ACTION_RESPOND)|(1u<<BF_ACTION_ESCALATE)
    };
    return valid_territory(territory) && valid_action(action) && (masks[territory] & (1u << action));
}

const char *bf_standing_territory_name(BfTerritory territory) { return valid_territory(territory) ? TERRITORIES[territory] : NULL; }
const char *bf_standing_action_name(BfAction action) { return valid_action(action) ? ACTIONS[action] : NULL; }
const char *bf_standing_attention_signal_name(BfAttentionSignal signal) { return valid_signal(signal) ? SIGNALS[signal] : NULL; }
const char *bf_standing_status_name(BfStandingStatus status) { switch (status) { case BF_STANDING_OK: return "ok"; case BF_STANDING_INVALID: return "invalid"; case BF_STANDING_CAPACITY: return "capacity"; } return "unknown"; }

BfStandingStatus bf_standing_address_init(BfStandingAddress *address, const char *actor, BfTerritory territory, const BfInformationPolicy *policy) {
    size_t length;
    if (!address || !present(actor) || !valid_territory(territory)) return BF_STANDING_INVALID;
    length = strlen(actor); if (length >= sizeof(address->actor)) return BF_STANDING_CAPACITY;
    memset(address, 0, sizeof(*address)); memcpy(address->actor, actor, length + 1); address->territory = territory; address->policy = policy; return BF_STANDING_OK;
}

BfStandingStatus bf_standing_opportunity_validate(const BfOpportunity *opportunity) { if (!opportunity || !present(opportunity->subject) || !valid_action(opportunity->action) || !valid_territory(opportunity->territory) || !action_in_territory(opportunity->action, opportunity->territory)) return BF_STANDING_INVALID; return BF_STANDING_OK; }

BfStandingStatus bf_standing_observe_attention(BfStandingAddress *address, const char *subject, BfAttentionSignal signal, double weight) {
    size_t index, length;
    if (!address || !present(subject) || !valid_signal(signal)) return BF_STANDING_INVALID;
    for (index = 0; index < address->attention_count; index++) if (strcmp(address->attention[index].subject, subject) == 0) { address->attention[index].weight += weight; return BF_STANDING_OK; }
    if (address->attention_count == BF_STANDING_MAX_ATTENTION) return BF_STANDING_CAPACITY;
    length = strlen(subject); if (length >= sizeof(address->attention[0].subject)) return BF_STANDING_CAPACITY;
    memcpy(address->attention[address->attention_count].subject, subject, length + 1); address->attention[address->attention_count++].weight = weight; return BF_STANDING_OK;
}

static double attention_for(const BfStandingAddress *address, const char *subject) { for (size_t index = 0; index < address->attention_count; index++) if (strcmp(address->attention[index].subject, subject) == 0) return address->attention[index].weight; return 0.0; }
static BfDisclosureAudience audience_for(BfTerritory territory) { static const BfDisclosureAudience values[] = {BF_AUDIENCE_SELF, BF_AUDIENCE_PUBLIC, BF_AUDIENCE_MARKET, BF_AUDIENCE_INSTITUTION}; return values[territory]; }

BfStandingStatus bf_standing_compile(const BfStandingAddress *address, const BfOpportunity *candidates, size_t candidate_count, BfStandingRanking *out) {
    if (!address || !out || (candidate_count && !candidates) || candidate_count > BF_STANDING_MAX_OPPORTUNITIES) return BF_STANDING_INVALID;
    memset(out, 0, sizeof(*out));
    if (address->policy) { BfDisclosureDecision decision; if (bf_information_policy_field_decision(address->policy, "subject", audience_for(address->territory), &decision) != BF_INFORMATION_POLICY_OK || !decision.allowed) return BF_STANDING_OK; }
    for (size_t index = 0; index < candidate_count; index++) if (bf_standing_opportunity_validate(&candidates[index]) == BF_STANDING_OK && candidates[index].territory == address->territory) out->candidate_indices[out->count++] = index;
    for (size_t left = 0; left < out->count; left++) for (size_t right = left + 1; right < out->count; right++) {
        const BfOpportunity *a = &candidates[out->candidate_indices[left]], *b = &candidates[out->candidate_indices[right]];
        double score_a = a->relevance + attention_for(address, a->subject) * 0.1, score_b = b->relevance + attention_for(address, b->subject) * 0.1;
        if ((b->available > a->available) || (b->available == a->available && score_b > score_a)) { size_t swap = out->candidate_indices[left]; out->candidate_indices[left] = out->candidate_indices[right]; out->candidate_indices[right] = swap; }
    }
    return BF_STANDING_OK;
}
