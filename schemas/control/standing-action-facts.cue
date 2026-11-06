package control

// What each standing action requires, acts on, and whether the machine may
// finish it.
//
// #StandingAction names twenty-eight things a person does at an opportunity and
// nothing consumes it. It is the only grammar in the estate that is entirely
// unread: no C, no other schema, no data.
//
// It is also the grammar where the fourth law lives. "Reachability stops at the
// human line" is stated everywhere and enforced by convention; here it can be
// mechanical. An action either completes in computation or it is a commitment a
// person makes, and saying which is what stops an opportunity engine from
// walking past a decision that was never anyone's to automate.
//
// Six of twenty-eight complete: WATCH, FOLLOW, SAVE, PLAN, REMEMBER, COMPARE.
// Everything a person would recognise as *doing* something -- buying, applying,
// approving, funding, escalating, sharing -- is a commitment. That ratio is the
// point rather than an accident of drafting.
//
// Cannot infer: a satisfiable requirement -> a permitted action. The relations
// say what the machine must be able to do for the action to be possible at all.
// AuthorityGraph still decides whether this actor may, and a commitment still
// needs the person.

#StandingActionFacts: {
	// What the machine must be able to do for this action to be reachable.
	requires: [#RelationKind, ...#RelationKind]
	// What the action is performed upon.
	acts_on: [#Phenotype, ...#Phenotype]
	// false when computation can carry it to completion; true when finishing it
	// is a person's commitment and no engine may close it on their behalf.
	commits: bool
}

standingActionFacts: [Name=string]: #StandingActionFacts

standingActionFacts: {
	// ---- reachable to completion: attention, memory and comparison
	WATCH:      {requires: ["OBSERVE", "SUBSCRIBE"], acts_on: ["MEDIA_ASSET", "EVENT", "LIVE_ENTITY"], commits: false}
	FOLLOW:     {requires: ["SUBSCRIBE", "NOTIFY"], acts_on: ["EVENT", "LIVE_ENTITY"], commits: false}
	SAVE:       {requires: ["TRANSFER"], acts_on: ["DOCUMENT", "MEDIA_ASSET", "DATASET"], commits: false}
	PLAN:       {requires: ["QUERY", "DESCRIBE"], acts_on: ["EVENT", "SPATIAL_RESOURCE"], commits: false}
	REMEMBER:   {requires: ["TRANSFER", "SYNCHRONIZE"], acts_on: ["DOCUMENT", "VERSIONED_RECORD"], commits: false}
	COMPARE:    {requires: ["QUERY", "FILTER"], acts_on: ["DATASET", "ECONOMIC_OFFER"], commits: false}

	// ---- money
	BUY:        {requires: ["QUOTE", "COMMIT", "PAY", "SETTLE"], acts_on: ["ECONOMIC_OFFER"], commits: true}
	SELL:       {requires: ["QUOTE", "PUBLISH", "SETTLE"], acts_on: ["ECONOMIC_OFFER"], commits: true}
	OFFER:      {requires: ["QUOTE", "PUBLISH"], acts_on: ["ECONOMIC_OFFER"], commits: true}
	FUND:       {requires: ["COMMIT", "PAY", "SETTLE"], acts_on: ["ECONOMIC_OFFER", "OBLIGATION"], commits: true}
	BOOK:       {requires: ["NEGOTIATE", "COMMIT"], acts_on: ["ECONOMIC_OFFER", "OBLIGATION"], commits: true}

	// ---- identity and belonging
	JOIN:       {requires: ["AUTHENTICATE", "AUTHORIZE"], acts_on: ["IDENTITY_CREDENTIAL"], commits: true}
	APPLY:      {requires: ["COMMIT", "TRANSFER"], acts_on: ["OBLIGATION", "IDENTITY_CREDENTIAL"], commits: true}
	CONTACT:    {requires: ["NOTIFY", "TRANSFER"], acts_on: ["IDENTITY_CREDENTIAL"], commits: true}
	MEET:       {requires: ["COMMIT"], acts_on: ["EVENT", "SPATIAL_RESOURCE"], commits: true}

	// ---- obligation
	CLAIM:      {requires: ["COMMIT", "PROVE"], acts_on: ["OBLIGATION"], commits: true}
	APPROVE:    {requires: ["AUTHORIZE"], acts_on: ["OBLIGATION"], commits: true}
	ASSIGN:     {requires: ["TASK", "AUTHORIZE"], acts_on: ["OBLIGATION"], commits: true}
	ESCALATE:   {requires: ["NOTIFY", "AUTHORIZE"], acts_on: ["OBLIGATION"], commits: true}
	RESPOND:    {requires: ["NOTIFY", "TRANSFER"], acts_on: ["EVENT", "OBLIGATION"], commits: true}
	REQUEST:    {requires: ["TASK", "NEGOTIATE"], acts_on: ["SERVICE_CAPABILITY"], commits: true}
	HELP:       {requires: ["TASK", "COMMIT"], acts_on: ["OBLIGATION"], commits: true}
	CONTRIBUTE: {requires: ["COMMIT", "TRANSFER"], acts_on: ["OBLIGATION", "DATASET"], commits: true}
	DO:         {requires: ["TASK", "COMMAND"], acts_on: ["SERVICE_CAPABILITY", "COMMANDABLE_RESOURCE"], commits: true}

	// ---- moving a thing in the world
	MOVE:       {requires: ["COMMAND", "TASK"], acts_on: ["SPATIAL_RESOURCE", "COMMANDABLE_RESOURCE", "SUPPLY_CHAIN_OBJECT"], commits: true}
	GO:         {requires: ["DISCOVER", "DESCRIBE"], acts_on: ["SPATIAL_RESOURCE", "EVENT"], commits: true}

	// ---- saying something publicly. SHARE and REVIEW commit because both
	// disclose, and a disclosure is not undone by deleting it.
	SHARE:      {requires: ["PUBLISH", "FEDERATE", "TRANSFER"], acts_on: ["DOCUMENT", "MEDIA_ASSET"], commits: true}
	REVIEW:     {requires: ["DESCRIBE", "PUBLISH"], acts_on: ["DOCUMENT", "ECONOMIC_OFFER"], commits: true}
}
