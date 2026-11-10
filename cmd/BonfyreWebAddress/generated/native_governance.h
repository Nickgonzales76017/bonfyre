/* Generated from schemas/control/governance.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_GOVERNANCE_H
#define BONFYRE_NATIVE_GOVERNANCE_H
#include <stddef.h>

/* A verb carries its own refusal so no surface can render the word
 * without the sentence that bounds it. */
typedef struct { const char *name; const char *label; const char *cannot_infer; } BfGovernanceVerb;

/* `settles` is the only thing that may offer settlement, and the
 * institution's declared world still gates it. */
typedef struct { const char *backing; int settles; const char *cannot_infer; } BfGovernanceBacking;

static const BfGovernanceVerb bf_governance_verbs[] = {
  {"Proposal", "Something being asked", "a proposal is a question recorded, never a decision taken and never authority to act on it"},
  {"Decision", "What was settled", "a decision is what a group concluded, not evidence that anything was carried out"},
  {"Vote", "What people said", "a tally is what was said by those who said it, never the whole membership's view"},
  {"Discussion", "What is being argued", "argument is not consent, and volume is not support"},
  {"Role", "What someone does here", "a role names what a person may do in this community and nowhere else"},
  {"Delegation", "Who is speaking for whom", "delegation is revocable and is never transfer of the thing delegated"},
  {"Budget", "What there is to spend", "an available figure is what has been contributed less what has been committed; it is not money held, owed or promised"},
  {"FundingRequest", "What is being asked for", "a request is not a pledge, an award, or a claim that the money exists"},
  {"Milestone", "What would count as done", "a stated milestone is a declaration of intent and never evidence that it was reached"},
  {"Execution", "What was actually done", "execution recorded here is what this estate observed, not proof that an external effect completed"},
  {"Receipt", "What the record shows", "a receipt reference is a digest of something presented; it is not settlement and not proof that value moved"},
  {"Policy", "The rule that stands", "a published policy is what a group wrote down, never a guarantee of how it will be applied"},
  {"Member", "Who belongs", "membership is participation in this community and never global authority"},
  {"Council", "Who decides together", "a council decides what its community granted it and nothing beyond that grant"},
};
#define BF_GOVERNANCE_VERB_COUNT (sizeof(bf_governance_verbs) / sizeof(bf_governance_verbs[0]))

static const BfGovernanceBacking bf_governance_backings[] = {
  {"human_approval", 0, "a person approving is a person approving; nothing is executed by the approval alone"},
  {"bonfyre_native", 0, "a decision recorded in this estate binds nothing outside it"},
  {"database_vote", 0, "a stored tally is the votes cast here and carries no weight elsewhere"},
  {"snapshot", 0, "signalling is not execution; a passing signal moves nothing on its own"},
  {"aragon_osx", 1, "a permission granted on chain is still bounded by the world the institution declares"},
  {"openzeppelin_governor", 1, "a queued proposal is not an executed one, and a timelock is not a guarantee"},
  {"safe", 1, "a threshold reached is authority to submit, never confirmation that a transaction landed"},
  {"external", 0, "an outside system's result is that system's claim, retained here as an observation"},
};
#define BF_GOVERNANCE_BACKING_COUNT (sizeof(bf_governance_backings) / sizeof(bf_governance_backings[0]))
#endif
