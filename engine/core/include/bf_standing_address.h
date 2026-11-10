#ifndef BF_STANDING_ADDRESS_H
#define BF_STANDING_ADDRESS_H

/* Native StandingAddress: a recompiled ranking of addressed opportunities,
 * never a stored feed. Attention may reorder candidates only; it cannot create
 * an institutional action or lift unavailable world state over availability. */

#include "bf_information_policy.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_STANDING_TEXT_MAX 256
#define BF_STANDING_MAX_ATTENTION 256
#define BF_STANDING_MAX_OPPORTUNITIES 256

typedef enum BfStandingStatus { BF_STANDING_OK = 0, BF_STANDING_INVALID, BF_STANDING_CAPACITY } BfStandingStatus;
typedef enum BfTerritory { BF_TERRITORY_LIFE, BF_TERRITORY_LIVE, BF_TERRITORY_SHOP, BF_TERRITORY_WORK } BfTerritory;
typedef enum BfAction { BF_ACTION_WATCH, BF_ACTION_JOIN, BF_ACTION_GO, BF_ACTION_FOLLOW, BF_ACTION_SAVE, BF_ACTION_BOOK, BF_ACTION_BUY, BF_ACTION_CONTRIBUTE, BF_ACTION_MEET, BF_ACTION_APPLY, BF_ACTION_HELP, BF_ACTION_OFFER, BF_ACTION_APPROVE, BF_ACTION_CLAIM, BF_ACTION_FUND, BF_ACTION_CONTACT, BF_ACTION_PLAN, BF_ACTION_REMEMBER, BF_ACTION_SHARE, BF_ACTION_MOVE, BF_ACTION_COMPARE, BF_ACTION_REQUEST, BF_ACTION_SELL, BF_ACTION_DO, BF_ACTION_REVIEW, BF_ACTION_ASSIGN, BF_ACTION_RESPOND, BF_ACTION_ESCALATE } BfAction;
typedef enum BfAttentionSignal { BF_ATTENTION_WATCHED, BF_ATTENTION_SWIPED_PAST, BF_ATTENTION_REACTED, BF_ATTENTION_DWELLED, BF_ATTENTION_FOLLOWED_BACK, BF_ATTENTION_MUTED } BfAttentionSignal;

typedef struct BfOpportunity { const char *subject; BfAction action; BfTerritory territory; double relevance; const char *reason; int available; } BfOpportunity;
typedef struct BfStandingAddress { char actor[BF_STANDING_TEXT_MAX]; BfTerritory territory; const BfInformationPolicy *policy; struct { char subject[BF_STANDING_TEXT_MAX]; double weight; } attention[BF_STANDING_MAX_ATTENTION]; size_t attention_count; } BfStandingAddress;
typedef struct BfStandingRanking { size_t count; size_t candidate_indices[BF_STANDING_MAX_OPPORTUNITIES]; } BfStandingRanking;

BfStandingStatus bf_standing_address_init(BfStandingAddress *address, const char *actor, BfTerritory territory, const BfInformationPolicy *policy);
BfStandingStatus bf_standing_opportunity_validate(const BfOpportunity *opportunity);
BfStandingStatus bf_standing_observe_attention(BfStandingAddress *address, const char *subject, BfAttentionSignal signal, double weight);
BfStandingStatus bf_standing_compile(const BfStandingAddress *address, const BfOpportunity *candidates, size_t candidate_count, BfStandingRanking *out);
const char *bf_standing_territory_name(BfTerritory territory);
const char *bf_standing_action_name(BfAction action);
const char *bf_standing_attention_signal_name(BfAttentionSignal signal);
const char *bf_standing_status_name(BfStandingStatus status);

#ifdef __cplusplus
}
#endif

#endif
