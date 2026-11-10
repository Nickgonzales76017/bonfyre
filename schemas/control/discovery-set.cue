// Criteria that run against the frontier, and what each one is allowed to mean.
//
// The estate's existing collections are frozen lists: a source_collection names
// its members by hand in a source_refs array, so "the sources about X" is a set
// somebody typed once and nothing can grow, shrink or be argued with. A working
// set is the opposite shape -- criteria that evaluate against candidates, a
// verdict per candidate, and a person who accepts or rejects what the criteria
// proposed.
//
// Every criterion here is bound to a column the frontier actually holds, because
// a criterion that cannot be evaluated is a wish. `over` says which table it
// reads, `column` says which field, and `how` says how. Nothing else is
// permitted, so a set cannot smuggle in a judgement the estate has no data for.
//
// The hard limit: a criterion is a filter over recorded facts. It is never
// semantic relevance, never a statement about quality, and matching one is not
// permission to fetch, read, contact or enrol anything.
package control

#CriterionKind: {
	kind: string & =~"^[a-z][a-z_]*$"
	label: string & != ""
	over: "root" | "channel"
	// The column this reads. Bound here so the evaluator cannot invent one.
	column: string & =~"^[a-z_]+$"
	how: "suffix" | "contains" | "equals" | "at_least" | "in_set" | "plane"
	// What a match does and does not say.
	means: string & != ""
	cannot_infer: string & != ""
}

nativeCriterionKinds: [...#CriterionKind] & [
	{kind: "host_suffix", label: "Host ends with", over: "root", column: "host", how: "suffix",
	 means: "the publisher's hostname ends with this, which is how a country, a sector or an institution type is usually visible",
	 cannot_infer: "a suffix is not jurisdiction, ownership or sector; .org is not a charity and .edu is not a school"},
	{kind: "host_contains", label: "Host contains", over: "root", column: "host", how: "contains",
	 means: "the publisher's hostname contains this string anywhere",
	 cannot_infer: "a word in a hostname is not a statement about what the publisher does"},
	{kind: "name_contains", label: "Publisher name contains", over: "root", column: "name", how: "contains",
	 means: "the name this estate recorded for the publisher contains this string",
	 cannot_infer: "the recorded name is what a source or an inventory said; it is not the organisation's legal or current name"},
	{kind: "path_contains", label: "Address path contains", over: "channel", column: "exact_address", how: "contains",
	 means: "a channel's own address contains this string, which is how a publishing habit is visible",
	 cannot_infer: "a path containing a word is not a claim about what that address returns"},
	{kind: "protocol_is", label: "Speaks", over: "channel", column: "protocol", how: "equals",
	 means: "a channel was recorded as speaking this protocol",
	 cannot_infer: "a recorded protocol is not a working endpoint, and not one this estate has read"},
	{kind: "protocol_in", label: "Speaks any of", over: "channel", column: "protocol", how: "in_set",
	 means: "a channel speaks one of several protocols, given as a comma-separated set",
	 cannot_infer: "the same as protocol_is, several times over"},
	{kind: "state_is", label: "Standing is", over: "channel", column: "state", how: "equals",
	 means: "a channel's standing in the ledger: admitted, eligible, candidate or waiting_credential",
	 cannot_infer: "admitted means the census reads it, not that it is lawful, licensed or available"},
	{kind: "truth_level_is", label: "Known by", over: "channel", column: "truth_level", how: "equals",
	 means: "how this estate came to know the address at all -- listed, observed in a body, derived from a protocol",
	 cannot_infer: "a derived or linked candidate has not been asked for and is not a source"},
	{kind: "min_channels", label: "Has at least this many channels", over: "root", column: "channel_count", how: "at_least",
	 means: "the publisher has at least this many channels of any standing in the ledger",
	 cannot_infer: "a channel count is what inventories and bodies named, never how much the publisher publishes"},
	{kind: "publishes_plane", label: "Publishes a", over: "root", column: "plane", how: "plane",
	 means: "the publisher has at least one channel whose address classifies as this plane: calendar, feed, document, data or page",
	 cannot_infer: "a classified plane is the address's shape, not a promise of what it contains"},
]

// A verdict on one candidate. Only a person moves a candidate out of pending:
// the criteria propose and the reachability stops there.
#CandidateVerdict: {
	verdict: string & =~"^[a-z_]+$"
	label: string & != ""
	// May a machine put a candidate here?
	machine_may_set: bool
	means: string & != ""
}

nativeCandidateVerdicts: [...#CandidateVerdict] & [
	{verdict: "pending", label: "Proposed by the criteria", machine_may_set: true,
	 means: "every criterion the set requires matched, and nobody has looked at it"},
	{verdict: "missed", label: "Did not match", machine_may_set: true,
	 means: "at least one required criterion did not match, and which one is recorded"},
	{verdict: "accepted", label: "Accepted by a person", machine_may_set: false,
	 means: "somebody named looked at it and said it belongs in this set"},
	{verdict: "rejected", label: "Rejected by a person", machine_may_set: false,
	 means: "somebody named looked at it and said it does not belong, with the reason if they gave one"},
]
