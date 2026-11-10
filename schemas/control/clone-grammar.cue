// Thirty mature product grammars, and the shared primitives underneath them.
//
// The tempting reading of a list like this is thirty applications. That is the
// failure mode: thirty surfaces joined by a navigation bar, each re-inventing a
// proposal, a release, a member and a run, and none of them reaching the organs
// that already hold those things. A Proposal from a crowdfunding pitch, a
// Proposal in a DAO and a Proposal at a parent association are one primitive
// under three vocabularies, and if they are built three times they can never be
// counted, compared or governed together.
//
// So this file declares the layer that makes thirty grammars cheaper than three
// applications: a set of interaction primitives, each bound to the Bonfyre organ
// that already owns it, and thirty grammars expressed as the primitives they
// need. What a grammar "explodes" is then computable rather than asserted -- how
// many of the thirty share a primitive, which primitives no organ owns yet, and
// which grammars rest entirely on ground that exists.
//
// Two rules keep it from becoming a wish list:
//
//   - a primitive names the architecture that owns it, and that id must exist in
//     the atlas. A primitive nothing owns says so, with the reason, and every
//     grammar depending on it inherits that gap.
//   - a grammar is provenance and vocabulary only. Naming one is not a claim its
//     authors know this estate exists, that any of their code runs here, or that
//     this estate reproduces their product.
package control

#InteractionPrimitive: {
	primitive: string & =~"^[a-z][a-z-]*$"
	label: string & != ""
	// The architecture in the atlas that already owns this. Empty means no organ
	// owns it yet, and `unowned_because` has to say what is missing.
	owner: string & =~"^([a-z][a-z0-9-]*)?$"
	unowned_because: string
	means: string & != ""
	cannot_infer: string & != ""
}

nativeInteractionPrimitives: [...#InteractionPrimitive] & [
	{primitive: "proposal", label: "Something being asked", owner: "governance-grammar", unowned_because: "",
	 means: "a question put to a body that can answer it, with the backing that would carry it if it were decided",
	 cannot_infer: "a proposal is never a decision, never authority to act on it, and never evidence anything was carried out"},
	{primitive: "decision", label: "What was settled", owner: "governance-grammar", unowned_because: "",
	 means: "the recorded outcome of a proposal, with what backed it",
	 cannot_infer: "a decision is not an execution; something still has to do the thing"},
	{primitive: "vote", label: "What people said", owner: "governance-grammar", unowned_because: "",
	 means: "one member's position at one time, replaceable while the question is open",
	 cannot_infer: "a tally is what was said by those who said it, and never authority to carry it out"},
	{primitive: "delegation", label: "Who speaks for whom", owner: "authority-graph", unowned_because: "",
	 means: "an authority held by one actor exercised by another, for a stated scope",
	 cannot_infer: "delegation never releases the original holder, and never widens what was held"},
	{primitive: "member", label: "Who belongs", owner: "actor-graph", unowned_because: "",
	 means: "a person or organisation in a relationship with a body, at some standing",
	 cannot_infer: "membership is not identity verification, and not a claim about anyone outside this estate"},
	{primitive: "role", label: "What someone does here", owner: "authority-graph", unowned_because: "",
	 means: "a named position carrying stated authority inside one body",
	 cannot_infer: "holding a role here is not holding it anywhere else, and not an employment relationship"},
	{primitive: "thread", label: "What is being argued", owner: "human-community-body", unowned_because: "",
	 means: "accepted human conversation, nested, with the moderation state that admitted it",
	 cannot_infer: "a published thread is not endorsement, and moderation state is not truth"},
	{primitive: "moderation", label: "What was admitted", owner: "human-community-body", unowned_because: "",
	 means: "the decision that let a contribution appear, and the rule it answered to",
	 cannot_infer: "admitting a post is not agreeing with it; refusing one is not a claim about its author"},
	{primitive: "collection", label: "Things gathered on purpose", owner: "source-frontier", unowned_because: "",
	 means: "a working set with criteria, accepted and rejected members, and what it is monitored for",
	 cannot_infer: "a filter is not semantic relevance, and membership of a collection is not a fact about the thing collected"},
	{primitive: "artifact", label: "A thing that was built", owner: "artifact-graph", unowned_because: "",
	 means: "a produced file, pack, binary or model shard, identified by what it contains",
	 cannot_infer: "an artifact existing is not a signature, a scan, a licence, or permission to run it"},
	{primitive: "release", label: "Something delivered somewhere", owner: "destination-release", unowned_because: "",
	 means: "material carried to a destination, in a lifecycle where only a receipt makes it a delivery",
	 cannot_infer: "delivered is not a reuse licence, and readiness is not delivery"},
	{primitive: "receipt", label: "What the record shows", owner: "receipt-envelope", unowned_because: "",
	 means: "the evidence that something happened, naming what and when",
	 cannot_infer: "a receipt proves an act, never that the act was correct or useful"},
	{primitive: "run", label: "Work that was done", owner: "work-graph", unowned_because: "",
	 means: "a durable execution with dependencies, state, and what it produced",
	 cannot_infer: "queue priority is not a durability guarantee, and a finished run is not a verified outcome"},
	{primitive: "schedule", label: "When something happens", owner: "source-scheduler", unowned_because: "",
	 means: "a cadence or a time something is due, derived from measurement where there is any",
	 cannot_infer: "a cadence is a reading plan, never permission to fetch, buy, contact or enrol"},
	{primitive: "budget", label: "What there is to spend", owner: "funding-closure", unowned_because: "",
	 means: "stated funds against stated costs, open for anyone to read",
	 cannot_infer: "an available figure is contributions less commitments, never money held or owed"},
	{primitive: "pledge", label: "What was promised", owner: "commitment-graph", unowned_because: "",
	 means: "somebody stating they will do or give something, on the rail that knows a promise is not a delivery",
	 cannot_infer: "a pledge is not a payment, not collectable by this estate, and not evidence value moved"},
	{primitive: "price", label: "What something costs", owner: "price-observatory", unowned_because: "",
	 means: "an amount a publisher stated about itself, with the document that admitted it",
	 cannot_infer: "an observed price is not a current offer, not available to this viewer, and not a quote this estate can honour"},
	{primitive: "order", label: "Something bought", owner: "commerce-chain", unowned_because: "",
	 means: "a header and its lines, moving through fulfilment states",
	 cannot_infer: "an order shape is not stock, not availability, and not a payment"},
	{primitive: "model", label: "A model and its variants", owner: "model-topology-graph", unowned_because: "",
	 means: "a family of weights and representations with measured behaviour",
	 cannot_infer: "low reconstruction error is not good generation, and a measured variant is not a licensed one"},
	{primitive: "capability", label: "Something the estate can do", owner: "capability-lattice", unowned_because: "",
	 means: "an operation with its inputs, its cost and the authority it answers to",
	 cannot_infer: "a capability existing is not authorisation to invoke it"},
	{primitive: "provider", label: "Who would do it", owner: "cmd-provider-frontier", unowned_because: "",
	 means: "an external party that could serve a capability, with what it charges where that is stated",
	 cannot_infer: "a listed provider is not an enrolled, credentialed or contracted one"},
	{primitive: "namespace", label: "Where a thing lives", owner: "bonfyre-fs", unowned_because: "",
	 means: "the universal path by which any addressable resource is located",
	 cannot_infer: "a path resolving is not permission to read what is there"},
	{primitive: "media", label: "Pictures and sound", owner: "media-library", unowned_because: "",
	 means: "retained images, audio and video with where they came from",
	 cannot_infer: "holding media is not a licence to publish it"},
	{primitive: "place", label: "Somewhere", owner: "spatial-observation", unowned_because: "",
	 means: "an observed location, at the precision it was observed",
	 cannot_infer: "a coordinate is not a route, an entrance, an address, or a right to be there"},
	{primitive: "publication", label: "Something published", owner: "publication-program", unowned_because: "",
	 means: "composed material at an address, from sources that remain named",
	 cannot_infer: "publishing material is not owning it, and a publication is not its sources' endorsement"},
	{primitive: "form", label: "Something filled in", owner: "form-graph", unowned_because: "",
	 means: "a structured question put to a person, and what they answered",
	 cannot_infer: "a completed form is not consent to anything not stated on it"},
	{primitive: "household", label: "The people who share a life", owner: "household-economic-circuit", unowned_because: "",
	 means: "a body composed for the people in it, whose record answers to them alone",
	 cannot_infer: "a household record is not a roster of anybody, and never readable by this estate's operators"},
	{primitive: "institution-plane", label: "How an institution can be reached", owner: "institution-planes", unowned_because: "",
	 means: "one surface of an institution, public or answering only to a relationship",
	 cannot_infer: "a granted plane is not reachable because it is named; it answers to a relationship a household holds"},
	{primitive: "lineage", label: "What came from what", owner: "evidence-binding", unowned_because: "",
	 means: "the chain from a derived thing back to the evidence it was derived from",
	 cannot_infer: "a derived field is not a new observation; it is one arrangement of its inputs and stale when they move"},
	{primitive: "version", label: "A branch of a large estate", owner: "estate-version", unowned_because: "",
	 means: "a named reading of stored material, comparable against another reading",
	 cannot_infer: "a snapshot records digests and not bytes: it says what changed and cannot give back what was there, and it cannot be merged with another"},
	{primitive: "message", label: "Something sent to somebody", owner: "held-message", unowned_because: "",
	 means: "a direct communication to a named person or group",
	 cannot_infer: "a composed message is not a sent one; nothing here contacts anybody"},
	{primitive: "signature", label: "Somebody agreeing in writing", owner: "assent-signature", unowned_because: "",
	 means: "a person's recorded assent to a specific document at a specific version",
	 cannot_infer: "an approval recorded in governance is not a signature, and neither is a click"},
	{primitive: "stream", label: "Something happening live", owner: "live-session", unowned_because: "",
	 means: "a broadcast in progress, with what is being shown and who is present",
	 cannot_infer: "a scheduled stream is not a live one, and a replay is not a broadcast"},
]

// A grammar is where an interaction vocabulary was learned. It is provenance in
// exactly the sense a page region records a reading rhythm: no donor code runs
// here, and naming one is not a claim its authors know this estate exists.
#CloneGrammar: {
	grammar: string & =~"^[a-z][a-z0-9-]*$"
	studied: string & != ""
	// What it makes legible, in the estate's own terms.
	explodes: string & != ""
	// The organ whose existing work this vocabulary would surface. Must be a
	// real architecture id.
	over: string & =~"^[a-z][a-z0-9-]*$"
	// The primitives this grammar needs. Ordering is not significance.
	needs: [string & != "", ...string]
	// Where the estate's existing work is already visible in this shape.
	wave: 1 | 2 | 3
	cannot_infer: string & != ""
}

nativeCloneGrammars: [...#CloneGrammar] & [
	{grammar: "model-commons", studied: "Hugging Face Hub", wave: 1, over: "model-topology-graph",
	 explodes: "the model estate as something a person can look at: variants, cards, measured behaviour, artifacts, what uses it and what it came from",
	 needs: ["model", "artifact", "lineage", "thread", "member", "collection", "receipt"],
	 cannot_infer: "a model page is not a licence to use the weights, and measured behaviour is not fitness for anyone's purpose"},
	{grammar: "model-execution", studied: "Replicate", wave: 1, over: "capability-lattice",
	 explodes: "a capability with an input box and a cost, so years of provider and economic machinery become one button",
	 needs: ["model", "capability", "provider", "price", "run", "receipt"],
	 cannot_infer: "an estimated cost is not a quote, and a run button is not authority to spend"},
	{grammar: "artifact-registry", studied: "Harbor", wave: 3, over: "artifact-graph",
	 explodes: "compiled packs, binaries, model shards and templates as a trusted registry with provenance and replication",
	 needs: ["artifact", "lineage", "receipt", "version", "signature"],
	 cannot_infer: "a registered artifact is not a scanned, signed or safe one"},
	{grammar: "object-versioning", studied: "lakeFS", wave: 1, over: "bonfyre-fs",
	 explodes: "branching an enormous object estate: compare, commit, roll back, inspect provenance of derived collections",
	 needs: ["namespace", "version", "lineage", "collection", "receipt"],
	 cannot_infer: "a catalogue generation is not a branch; nothing here can be reverted"},
	{grammar: "file-browser", studied: "Nextcloud and FileBrowser", wave: 1, over: "bonfyre-fs",
	 explodes: "the universal namespace with a human face: folders, previews, recents, activity, shares",
	 needs: ["namespace", "media", "member", "receipt"],
	 cannot_infer: "a path being browsable is not permission to read it; the namespace locates and authority gates"},
	{grammar: "media-estate", studied: "Immich", wave: 3, over: "media-library",
	 explodes: "retained media as browsable memory: by time, place, album and what is in the frame",
	 needs: ["media", "place", "collection", "namespace"],
	 cannot_infer: "recognising something in a picture is not identifying anybody, and holding media is not a licence"},
	{grammar: "estate-catalogue", studied: "OpenMetadata and DataHub", wave: 1, over: "evidence-binding",
	 explodes: "the estate as a catalogue: ownership, lineage, usage, classification and the conversation about each thing",
	 needs: ["lineage", "collection", "member", "thread", "receipt", "namespace"],
	 cannot_infer: "declared ownership is not authority over a thing, and lineage is not quality"},
	{grammar: "organ-catalogue", studied: "Backstage", wave: 3, over: "capability-lattice",
	 explodes: "what organs and capabilities exist, who owns them, whether they answer, and what depends on them",
	 needs: ["capability", "member", "run", "namespace", "lineage"],
	 cannot_infer: "a listed organ is not a healthy one, and an owner is not on call"},
	{grammar: "repository", studied: "the GitHub repository page", wave: 2, over: "artifact-graph",
	 explodes: "any composed thing read as a repository: overview, contents, history, contributors, releases, discussion",
	 needs: ["artifact", "version", "release", "thread", "member", "lineage"],
	 cannot_infer: "a history being shown is not a history being navigable; nothing here can be checked out"},
	{grammar: "pipeline-runs", studied: "GitHub Actions and Dagster", wave: 3, over: "work-graph",
	 explodes: "durable work with a face: the graph, each step's state, logs, artifacts, retries and standing schedules",
	 needs: ["run", "artifact", "schedule", "receipt", "capability"],
	 cannot_infer: "a green run is not a correct result, and a retry is not idempotence"},
	{grammar: "discovery-sets", studied: "Exa Websets", wave: 1, over: "source-frontier",
	 explodes: "the frontier as working sets: describe a cohort, state criteria, accept and reject candidates, keep it monitored",
	 needs: ["collection", "schedule", "member", "receipt", "capability"],
	 cannot_infer: "a collection filter is not semantic relevance, and monitoring is not permission to fetch"},
	{grammar: "source-inspection", studied: "Firecrawl", wave: 2, over: "foreign-twin",
	 explodes: "what a source actually is: its map, its structure, what may be retained, and what changed since last time",
	 needs: ["collection", "lineage", "receipt", "schedule", "release"],
	 cannot_infer: "reading a source's structure is not a reuse licence, and a boundary response is not a source that does not exist"},
	{grammar: "distribution", studied: "DistroKid", wave: 1, over: "destination-release",
	 explodes: "one source to many destinations with states a person can read, where delivered means a receipt exists",
	 needs: ["release", "receipt", "collection", "price"],
	 cannot_infer: "delivered is not published by the destination, and readiness is not delivery"},
	{grammar: "action-composition", studied: "Zapier and Make", wave: 3, over: "protocol-machine",
	 explodes: "when this happens do that, compiled onto the protocol and capability rails rather than a second runtime",
	 needs: ["capability", "run", "schedule", "receipt"],
	 cannot_infer: "a composed automation is not an authorised effect; each step still answers to its own authority"},
	{grammar: "visual-pipelines", studied: "n8n", wave: 3, over: "work-graph",
	 explodes: "the same composition for an operator who wants to see the machine: branches, transforms, test data",
	 needs: ["run", "capability", "artifact", "receipt"],
	 cannot_infer: "a pipeline that ran with test data is not one that may run with real data"},
	{grammar: "work-tracking", studied: "Plane and Linear", wave: 1, over: "work-graph",
	 explodes: "work people can see: issues, cycles, triage, assignment and state over the durable work graph",
	 needs: ["run", "member", "role", "schedule", "thread"],
	 cannot_infer: "an assignment is not a commitment anybody made, and a cycle is not a deadline"},
	{grammar: "scheduling", studied: "Cal.com", wave: 2, over: "source-scheduler",
	 explodes: "availability as a capability: office hours, volunteer slots, conferences, venue times",
	 needs: ["schedule", "member", "form", "capability", "message"],
	 cannot_infer: "an offered slot is not a booking, and a booking here contacts nobody"},
	{grammar: "relationship-workspace", studied: "Attio and Twenty", wave: 2, over: "actor-graph",
	 explodes: "people, organisations, communities and households with their interactions and what is next, over the real relationship graph",
	 needs: ["member", "role", "thread", "collection", "form"],
	 cannot_infer: "a recorded interaction is not a relationship either party would claim"},
	{grammar: "community-operations", studied: "Discord", wave: 2, over: "human-community-body",
	 explodes: "a community's private operating layer: rooms, roles, moderation, events and the actions a member may take",
	 needs: ["thread", "moderation", "member", "role", "schedule", "message"],
	 cannot_infer: "a public presence does not expose a private room's contents"},
	{grammar: "public-discussion", studied: "Reddit", wave: 2, over: "human-community-body",
	 explodes: "threaded public discussion with its moderation queue and rules, attached to a publication rather than being the product",
	 needs: ["thread", "moderation", "member", "publication"],
	 cannot_infer: "a reputation number is not standing anywhere else, and a mod decision is not a fact"},
	{grammar: "project-pitch", studied: "Kickstarter", wave: 1, over: "funding-frontier",
	 explodes: "how a community body is born: a pitch, a target, supporters, milestones and updates",
	 needs: ["proposal", "pledge", "budget", "member", "publication", "schedule"],
	 cannot_infer: "a stated target is not money raised, held or promised, and reaching one is a stage rather than an ending"},
	{grammar: "recurring-support", studied: "Patreon and Open Collective", wave: 2, over: "funding-closure",
	 explodes: "underwriting what a publication costs to run, with the budget and its receipts in the open",
	 needs: ["pledge", "budget", "member", "receipt", "price"],
	 cannot_infer: "stated recurring support is an intention; this estate bills nobody and collects nothing"},
	{grammar: "governance", studied: "Snapshot, Aragon and Safe", wave: 1, over: "governance-grammar",
	 explodes: "proposal, electorate, vote, council, delegation and treasury authorisation, with policies rather than a vote on everything",
	 needs: ["proposal", "decision", "vote", "delegation", "role", "member", "budget"],
	 cannot_infer: "a passed vote is not an execution, and a treasury authorisation is not a payment"},
	{grammar: "commerce", studied: "Shopify and Medusa", wave: 2, over: "commerce-chain",
	 explodes: "a generic commerce substrate: catalogue, variants, orders, fulfilment, promotions and channels",
	 needs: ["order", "price", "collection", "member", "release"],
	 cannot_infer: "a catalogue is not stock, and an order shape is not a payment"},
	{grammar: "live-selling", studied: "TikTok Shop and Twitch", wave: 2, over: "media-library",
	 explodes: "a live operator publication: what is on screen now, chat, guests, the schedule and a durable replay",
	 needs: ["stream", "media", "order", "thread", "schedule", "member"],
	 cannot_infer: "a stream recorded here is not a storefront on anyone else's marketplace"},
	{grammar: "publishing", studied: "Ghost and Substack", wave: 2, over: "publication-program",
	 explodes: "the author's side of a living publication: stories, newsletters, membership, archives",
	 needs: ["publication", "member", "thread", "media", "message"],
	 cannot_infer: "a subscriber list is not imported by this estate knowing the writer exists"},
	{grammar: "curation", studied: "Pinterest, Are.na and Raindrop", wave: 2, over: "source-frontier",
	 explodes: "collected material as boards and research trails, which is how people actually understand relationship density",
	 needs: ["collection", "media", "member", "namespace", "publication"],
	 cannot_infer: "collecting things together is not a claim they are related in the world"},
	{grammar: "household-habitat", studied: "Cozi and Home Assistant", wave: 2, over: "household-economic-circuit",
	 explodes: "a household's own calendar, lists, routines and bills, and the physical state of where it lives",
	 needs: ["household", "schedule", "form", "budget", "place", "member"],
	 cannot_infer: "a household edition is composed for the people in it and is not shared by existing"},
	{grammar: "school-institution", studied: "ParentSquare and Membership Toolkit", wave: 1, over: "institution-planes",
	 explodes: "the parent-facing institution: directories, volunteers, fundraising, forms, payments, events and governance",
	 needs: ["institution-plane", "household", "member", "role", "form", "budget", "schedule", "message"],
	 cannot_infer: "running a parent association here is not affiliation with any school or district, and no student record reaches it"},
	{grammar: "controlled-documents", studied: "Papermark, DocSend and Documenso", wave: 2, over: "form-graph",
	 explodes: "board material, proposals and funding packs with scoped sharing, approval and who actually opened them",
	 needs: ["form", "artifact", "signature", "member", "receipt", "version"],
	 cannot_infer: "a viewed document is not an agreed one, and an approval is not a signature"},
]
