// What happens to observed material on its way to a destination, and which of
// those states is a delivery.
//
// The release projection already reported readiness per path: a declared
// extractor plus retained archive material reads ready_for_extraction. That is
// a true and useful thing to say, and it is one step away from a lie. "Ready"
// in an operator's eye becomes "done" within a week of shipping, and the field
// that would have contradicted it -- public_program_delivery_verified -- was a
// literal zero printed beside it with nothing behind the number.
//
// So the lifecycle is declared here, once, with the delivering states named as
// such. A state that does not set `delivered` cannot be reported as a delivery
// however a caller phrases the question, and the build asserts how many states
// deliver, so a tenth one cannot quietly join them.
//
// Nothing here performs a delivery. This is the vocabulary a receipt is
// checked against; the receipt is what makes a delivery true.
package control

#DestinationState: {
	state: string & =~"^[a-z][a-z_]*$"
	label: string & != ""
	// Reached only by a receipt that names what was materialized. Readiness,
	// admission and intent are all false here.
	delivered: bool
	// May still become delivered later. A terminal state may not.
	open: bool
	means: string & != ""
	cannot_infer: string & != ""
}

nativeDestinationStates: [...#DestinationState] & [
	{state: "not_selected", label: "Not selected", delivered: false, open: true,
	 means: "nobody has proposed this destination for this material",
	 cannot_infer: "an unselected destination is not a refused one, and not one this estate judged unsuitable"},
	{state: "selected", label: "Selected", delivered: false, open: true,
	 means: "an operator named this destination as an intent",
	 cannot_infer: "an intent is not an admission; nothing has checked whether it is allowed"},
	{state: "blocked", label: "Blocked", delivered: false, open: true,
	 means: "a binding this destination needs is absent, and the missing binding is named",
	 cannot_infer: "a blocked destination is not a permanent refusal; naming the binding is how it stops being blocked"},
	{state: "admissible", label: "Admissible", delivered: false, open: true,
	 means: "every binding is present and the rights, robots and economics checks passed",
	 cannot_infer: "admissible is not delivered, not published, and not permission that survives a change in the source"},
	{state: "queued", label: "Queued", delivered: false, open: true,
	 means: "admitted by a human and waiting for work to be done",
	 cannot_infer: "a queued release has produced nothing; no fragment exists yet"},
	{state: "materializing", label: "Materializing", delivered: false, open: true,
	 means: "work is in progress and partial output may exist",
	 cannot_infer: "partial output is not a release, and reading it is not reading a delivery"},
	{state: "delivered", label: "Delivered", delivered: true, open: false,
	 means: "a receipt names what was materialized, where, and from which source generation",
	 cannot_infer: "a delivery is not a licence to redistribute, and not a claim the destination accepted it as correct"},
	{state: "stale", label: "Stale", delivered: true, open: true,
	 means: "delivered, and the source has changed in a way that affects it since",
	 cannot_infer: "stale is not withdrawn: what was delivered is still there and its receipt still stands"},
	{state: "superseded", label: "Superseded", delivered: true, open: false,
	 means: "a later delivery replaced it, and both receipts remain readable",
	 cannot_infer: "superseded is not deleted, and the earlier receipt is not falsified by the later one"},
	{state: "failed", label: "Failed", delivered: false, open: true,
	 means: "work was attempted and did not produce a receipt, with the reason recorded",
	 cannot_infer: "a failed delivery says nothing about the source; it never changes what was observed"},
	{state: "withdrawn", label: "Withdrawn", delivered: false, open: false,
	 means: "an operator took it back, and the receipts of what had been delivered remain",
	 cannot_infer: "withdrawal is not erasure of the record that it once existed"},
]

// How a source change bears on a destination that already holds a delivery.
//
// Crawling on a timer treats every byte that moved as work to redo. Most bytes
// that move are a footer, a session id or a rotated banner. A consequence class
// is what lets one remote change recompile only what it actually touched.
#ChangeConsequence: {
	consequence: string & =~"^[a-z][a-z_]*$"
	label: string & != ""
	// Does a destination holding a delivery become stale because of this?
	invalidates: bool
	// Does a person have to look at it before anything else happens?
	needs_review: bool
	means: string & != ""
	cannot_infer: string & != ""
}

nativeChangeConsequences: [...#ChangeConsequence] & [
	{consequence: "cosmetic", label: "No republish", invalidates: false, needs_review: false,
	 means: "the source's bytes changed and nothing a destination carries changed with them",
	 cannot_infer: "cosmetic is a statement about the fields this estate extracts, never about the source's own intent"},
	{consequence: "material", label: "Republish", invalidates: true, needs_review: false,
	 means: "a field a destination carries changed: an amount, an identifier, an availability, a relationship",
	 cannot_infer: "a changed amount is not a current offer, and a changed availability is not verified availability"},
	{consequence: "structural", label: "Republish and re-extract", invalidates: true, needs_review: false,
	 means: "the shape changed -- a category hierarchy, a part-number scheme, a feed's own schema",
	 cannot_infer: "a structural change is not a new source; it is the same source arranged differently"},
	{consequence: "boundary", label: "Review", invalidates: true, needs_review: true,
	 means: "robots, a licence, a rights statement or an observe right changed",
	 cannot_infer: "a boundary change is a standing rule the source wrote for everyone, never a refusal aimed at this estate"},
	{consequence: "disappeared", label: "Review", invalidates: true, needs_review: true,
	 means: "material a destination carries is no longer present at the source",
	 cannot_infer: "absence now is not absence then: every earlier observation and its receipt survive this"},
]

// Which fields each destination actually carries.
//
// This decides whether a change invalidates a delivery, and it has to be
// declared rather than assumed. A price that moved is a real material change,
// and a delivered search fragment carries titles, links, publishers and dates
// and no amount at all — so staling it because a price moved would be the timer
// crawl wearing a state machine's clothes. A destination is invalidated only by
// a change to a field this table says it carries.
#DestinationCarries: {
	destination: string & =~"^[a-z][a-z_]*$"
	fields: [string & != "", ...string]
	means: string & != ""
}

nativeDestinationCarries: [...#DestinationCarries] & [
	{destination: "search", fields: ["title", "href", "publisher", "at", "kind"],
	 means: "what this estate retained, arranged so it can be found; it carries no amount and no availability"},
	{destination: "shop", fields: ["amount", "currency", "availability"],
	 means: "observed prices bound to a source coordinate, which is the only destination a price change bears on"},
	{destination: "spatial", fields: ["place", "geometry"],
	 means: "extracted locations, once an extractor is declared and material is retained"},
	{destination: "media", fields: ["media", "image"],
	 means: "extracted media references, once an extractor is declared"},
	{destination: "rights", fields: ["rights", "licence"],
	 means: "extracted rights statements, which no source in this estate has yet stated machine-readably"},
]
