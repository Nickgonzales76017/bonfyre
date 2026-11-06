package control

// How a native owner says it participated in one occurrence.
//
// The participation report declares seven axes and twenty-six rungs, and until
// this existed it recognized three document schemas feeding three axes. The
// other four axes -- capability, economic, round_trip, and every rung of
// discovery above `indexed` -- had no way for any owner to report into them.
// They were not empty because nothing had happened; they were empty because
// nothing could speak.
//
// That also made the braid a straight line by construction. One chain could
// report -- ExternalEstate, ForeignTwin, DomainProfile, ProtocolContract --
// so branch_points and rejoin_points were structurally zero no matter how much
// work the estate's other thirty owners did.
//
// The risk in opening this up is obvious and is the thing the atlas's first law
// forbids: if any document claiming an axis and a state is counted, then every
// owner can launder itself to any rung. So a participation row is not a claim.
// It carries the digest of what the owner read and the digest of what it
// produced, and a reader that cannot check both against the declared rungs of
// the declared axis refuses the row rather than counting it.

#ParticipationAxis:
	"discovery" |
	"observation" |
	"semantic" |
	"protocol" |
	"capability" |
	"economic" |
	"round_trip"

// The rungs each axis actually declares. A row naming a state that is not a
// rung of its own axis is refused -- that is the whole check, and it is why
// this list lives next to the schema rather than only inside the reader.
#AxisRungs: {
	discovery: ["indexed", "routable", "maintained"]
	observation: ["policy_excluded", "policy_indeterminate", "boundary_response",
		"source_observed", "source_retained", "repeat_observed"]
	semantic: ["interpreted", "profile_bound", "carrier_conformant"]
	protocol: ["contract_bound", "compiled", "enacted", "obligations_closed"]
	capability: ["qualified", "eligible", "authorized", "activated"]
	economic: ["quoted", "committed", "paid", "settled", "reconciled"]
	round_trip: ["projected", "observed", "faithful", "drifted", "unreachable"]
}

#Sha256: =~"^[0-9a-f]{64}$"

#OwnerParticipation: {
	schema: "bonfyre-owner-participation.v1"

	// Which occurrence this is about. Participation that cannot name an
	// occurrence is not participation in the estate.
	occurrence_id: =~"^external:"

	axis:  #ParticipationAxis
	state: string & !=""

	// Who did the work, and the transition it represents. `from_owner` and
	// `to_owner` are what make a braid a braid: they are the edge, and the axis
	// is only the label on it.
	owner:      string & !=""
	from_owner: string & !=""
	to_owner:   string & !=""

	// What the owner read, and what it produced. Both are required and both are
	// checked. An owner that cannot digest its own input and output did not do
	// work it can be held to.
	basis_sha256:    #Sha256
	evidence_sha256: #Sha256

	// Which binary emitted this, so a row can be traced to a program rather
	// than to a person's assertion about one.
	produced_by: string & !=""

	recorded_at: string & !=""

	// What this row is not. Every rung above the lowest on every axis needs
	// something this estate cannot grant itself: an observation needs the
	// network, an authorization needs an authority, a settlement needs money.
	cannot_infer?: [...string]
}

#OwnerParticipationSet: [...#OwnerParticipation]
