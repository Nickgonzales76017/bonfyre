package control

// Governance, as verbs a person already knows.
//
// The frontends worth mining -- Aragon's UI kit, Snapshot's proposal browsing,
// an OpenZeppelin Governor's lifecycle -- agree about the *grammar* and differ
// about the machinery. So the grammar is declared here and the machinery is an
// adapter underneath it, because most governance in this estate is not
// blockchain governance: a PTA vote, a moderator appointment, a household
// spending decision and a community funding proposal need no wallet, and
// making them carry one would be the protocol leaking into the vocabulary.
//
// The rule this follows from the rest of the estate: protocol implementation
// stays underneath; a reader sees normal words.
#GovernanceVerb: {
    // What a person is looking at.
    name: "Proposal" | "Decision" | "Vote" | "Discussion" | "Role" | "Delegation" |
          "Budget" | "FundingRequest" | "Milestone" | "Execution" | "Receipt" |
          "Policy" | "Member" | "Council"
    // The words a person sees. Never the adapter's name.
    label: string & != ""
    // What it cannot be read to mean. Every verb states one, because the
    // failure here is always the same shape: a record of an intention being
    // read as the thing itself having happened.
    cannot_infer: string & != ""
}

// What actually carries a verb. A backing is chosen per community and may
// differ per verb inside one community: a group can take its votes in a
// database and its treasury through a Safe without either fact reaching the
// page. `human_approval` is first because it is the honest default -- most
// decisions are made by a person who is allowed to make them.
#GovernanceBacking: {
    backing: "human_approval" | "bonfyre_native" | "database_vote" | "snapshot" |
             "aragon_osx" | "openzeppelin_governor" | "safe" | "external"
    // Whether this backing can produce an effect outside the estate. Only a
    // backing that can settle may be offered where settlement is claimed, and
    // the institution's declared world still gates it.
    settles: bool
    cannot_infer: string & != ""
}

// Governance that does not require a proposal for every act.
//
// Aragon's newer direction is the right one and it matches this estate's
// policy model: governance mostly declares the rules under which an action may
// execute without a vote, and a vote is what happens when an act falls outside
// them. "Create proposal, vote on everything" is not maturity, it is overhead.
#GovernanceRule: {
    // What the rule is about, in the grammar above.
    verb: #GovernanceVerb.name
    // Who may act without asking.
    who: string & != ""
    // The bound below which no decision is needed. An empty ceiling means the
    // rule permits nothing on its own and always defers.
    ceiling?: string
    // What must happen when the act is outside the bound.
    otherwise: "council_approval" | "member_vote" | "steward_threshold" | "refused"
    cannot_infer: string & != ""
}

nativeGovernanceGrammar: [...#GovernanceVerb] & [
    {name: "Proposal", label: "Something being asked",
     cannot_infer: "a proposal is a question recorded, never a decision taken and never authority to act on it"},
    {name: "Decision", label: "What was settled",
     cannot_infer: "a decision is what a group concluded, not evidence that anything was carried out"},
    {name: "Vote", label: "What people said",
     cannot_infer: "a tally is what was said by those who said it, never the whole membership's view"},
    {name: "Discussion", label: "What is being argued",
     cannot_infer: "argument is not consent, and volume is not support"},
    {name: "Role", label: "What someone does here",
     cannot_infer: "a role names what a person may do in this community and nowhere else"},
    {name: "Delegation", label: "Who is speaking for whom",
     cannot_infer: "delegation is revocable and is never transfer of the thing delegated"},
    {name: "Budget", label: "What there is to spend",
     cannot_infer: "an available figure is what has been contributed less what has been committed; it is not money held, owed or promised"},
    {name: "FundingRequest", label: "What is being asked for",
     cannot_infer: "a request is not a pledge, an award, or a claim that the money exists"},
    {name: "Milestone", label: "What would count as done",
     cannot_infer: "a stated milestone is a declaration of intent and never evidence that it was reached"},
    {name: "Execution", label: "What was actually done",
     cannot_infer: "execution recorded here is what this estate observed, not proof that an external effect completed"},
    {name: "Receipt", label: "What the record shows",
     cannot_infer: "a receipt reference is a digest of something presented; it is not settlement and not proof that value moved"},
    {name: "Policy", label: "The rule that stands",
     cannot_infer: "a published policy is what a group wrote down, never a guarantee of how it will be applied"},
    {name: "Member", label: "Who belongs",
     cannot_infer: "membership is participation in this community and never global authority"},
    {name: "Council", label: "Who decides together",
     cannot_infer: "a council decides what its community granted it and nothing beyond that grant"},
]

nativeGovernanceBackings: [...#GovernanceBacking] & [
    {backing: "human_approval", settles: false,
     cannot_infer: "a person approving is a person approving; nothing is executed by the approval alone"},
    {backing: "bonfyre_native", settles: false,
     cannot_infer: "a decision recorded in this estate binds nothing outside it"},
    {backing: "database_vote", settles: false,
     cannot_infer: "a stored tally is the votes cast here and carries no weight elsewhere"},
    {backing: "snapshot", settles: false,
     cannot_infer: "signalling is not execution; a passing signal moves nothing on its own"},
    {backing: "aragon_osx", settles: true,
     cannot_infer: "a permission granted on chain is still bounded by the world the institution declares"},
    {backing: "openzeppelin_governor", settles: true,
     cannot_infer: "a queued proposal is not an executed one, and a timelock is not a guarantee"},
    {backing: "safe", settles: true,
     cannot_infer: "a threshold reached is authority to submit, never confirmation that a transaction landed"},
    {backing: "external", settles: false,
     cannot_infer: "an outside system's result is that system's claim, retained here as an observation"},
]
