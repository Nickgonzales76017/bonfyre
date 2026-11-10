/* Generated from schemas/control/discovery-set.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_DISCOVERY_SETS_H
#define BONFYRE_NATIVE_DISCOVERY_SETS_H
#include <stddef.h>
#include <string.h>

/* A criterion is bound to the table and column it reads, so the evaluator
 * cannot build SQL against a field nobody declared. */
typedef struct { const char *kind, *label, *over, *column, *how, *means, *cannot_infer; } BfCriterionKind;

/* `machine_may_set` is the human line: the criteria propose, and moving a
 * candidate to accepted or rejected is somebody's act. */
typedef struct { const char *verdict, *label; unsigned char machine_may_set; const char *means; } BfCandidateVerdict;

static const BfCriterionKind bf_criterion_kinds[] = {
  {"host_suffix", "Host ends with", "root", "host", "suffix", "the publisher's hostname ends with this, which is how a country, a sector or an institution type is usually visible", "a suffix is not jurisdiction, ownership or sector; .org is not a charity and .edu is not a school"},
  {"host_contains", "Host contains", "root", "host", "contains", "the publisher's hostname contains this string anywhere", "a word in a hostname is not a statement about what the publisher does"},
  {"name_contains", "Publisher name contains", "root", "name", "contains", "the name this estate recorded for the publisher contains this string", "the recorded name is what a source or an inventory said; it is not the organisation's legal or current name"},
  {"path_contains", "Address path contains", "channel", "exact_address", "contains", "a channel's own address contains this string, which is how a publishing habit is visible", "a path containing a word is not a claim about what that address returns"},
  {"protocol_is", "Speaks", "channel", "protocol", "equals", "a channel was recorded as speaking this protocol", "a recorded protocol is not a working endpoint, and not one this estate has read"},
  {"protocol_in", "Speaks any of", "channel", "protocol", "in_set", "a channel speaks one of several protocols, given as a comma-separated set", "the same as protocol_is, several times over"},
  {"state_is", "Standing is", "channel", "state", "equals", "a channel's standing in the ledger: admitted, eligible, candidate or waiting_credential", "admitted means the census reads it, not that it is lawful, licensed or available"},
  {"truth_level_is", "Known by", "channel", "truth_level", "equals", "how this estate came to know the address at all -- listed, observed in a body, derived from a protocol", "a derived or linked candidate has not been asked for and is not a source"},
  {"min_channels", "Has at least this many channels", "root", "channel_count", "at_least", "the publisher has at least this many channels of any standing in the ledger", "a channel count is what inventories and bodies named, never how much the publisher publishes"},
  {"publishes_plane", "Publishes a", "root", "plane", "plane", "the publisher has at least one channel whose address classifies as this plane: calendar, feed, document, data or page", "a classified plane is the address's shape, not a promise of what it contains"},
};
#define BF_CRITERION_KIND_COUNT (sizeof(bf_criterion_kinds) / sizeof(bf_criterion_kinds[0]))

static const BfCandidateVerdict bf_candidate_verdicts[] = {
  {"pending", "Proposed by the criteria", 1, "every criterion the set requires matched, and nobody has looked at it"},
  {"missed", "Did not match", 1, "at least one required criterion did not match, and which one is recorded"},
  {"accepted", "Accepted by a person", 0, "somebody named looked at it and said it belongs in this set"},
  {"rejected", "Rejected by a person", 0, "somebody named looked at it and said it does not belong, with the reason if they gave one"},
};
#define BF_CANDIDATE_VERDICT_COUNT (sizeof(bf_candidate_verdicts) / sizeof(bf_candidate_verdicts[0]))

static const BfCriterionKind *bf_criterion_kind(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_CRITERION_KIND_COUNT; i++)
        if (!strcmp(bf_criterion_kinds[i].kind, name)) return &bf_criterion_kinds[i];
    return NULL;
}

/* May a machine put a candidate in this verdict? A run that could write
 * `accepted` would be the estate accepting on somebody's behalf. */
static int bf_verdict_machine_may_set(const char *name) {
    size_t i;
    if (!name) return 0;
    for (i = 0; i < BF_CANDIDATE_VERDICT_COUNT; i++)
        if (!strcmp(bf_candidate_verdicts[i].verdict, name))
            return (int)bf_candidate_verdicts[i].machine_may_set;
    return 0;
}
#endif
