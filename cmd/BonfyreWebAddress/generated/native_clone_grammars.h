/* Generated from schemas/control/clone-grammar.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_CLONE_GRAMMARS_H
#define BONFYRE_NATIVE_CLONE_GRAMMARS_H
#include <stddef.h>
#include <string.h>

/* A semantic cell several grammars share. `owner` is the architecture in
 * the atlas that already holds it; an empty owner means no organ does,
 * and `unowned_because` says what is missing. `shared_by` is counted from
 * the grammars rather than asserted here. */
typedef struct { const char *primitive, *label, *owner, *unowned_because,
                 *means, *cannot_infer; unsigned short shared_by; } BfInteractionPrimitive;

/* Provenance and vocabulary only: naming a grammar is not a claim its
 * authors know this estate exists, that their code runs here, or that this
 * estate reproduces their product. `needs` is a |-delimited list of
 * primitives; `unowned_needs` counts how many of them no organ owns. */
typedef struct { const char *grammar, *studied, *explodes, *over, *needs, *cannot_infer;
                 unsigned char wave; unsigned short need_count, unowned_needs; } BfCloneGrammar;

static const BfInteractionPrimitive bf_interaction_primitives[] = {
  {"proposal", "Something being asked", "governance-grammar", "", "a question put to a body that can answer it, with the backing that would carry it if it were decided", "a proposal is never a decision, never authority to act on it, and never evidence anything was carried out", 2},
  {"decision", "What was settled", "governance-grammar", "", "the recorded outcome of a proposal, with what backed it", "a decision is not an execution; something still has to do the thing", 1},
  {"vote", "What people said", "governance-grammar", "", "one member's position at one time, replaceable while the question is open", "a tally is what was said by those who said it, and never authority to carry it out", 1},
  {"delegation", "Who speaks for whom", "authority-graph", "", "an authority held by one actor exercised by another, for a stated scope", "delegation never releases the original holder, and never widens what was held", 1},
  {"member", "Who belongs", "actor-graph", "", "a person or organisation in a relationship with a body, at some standing", "membership is not identity verification, and not a claim about anyone outside this estate", 21},
  {"role", "What someone does here", "authority-graph", "", "a named position carrying stated authority inside one body", "holding a role here is not holding it anywhere else, and not an employment relationship", 5},
  {"thread", "What is being argued", "human-community-body", "", "accepted human conversation, nested, with the moderation state that admitted it", "a published thread is not endorsement, and moderation state is not truth", 9},
  {"moderation", "What was admitted", "human-community-body", "", "the decision that let a contribution appear, and the rule it answered to", "admitting a post is not agreeing with it; refusing one is not a claim about its author", 2},
  {"collection", "Things gathered on purpose", "source-frontier", "", "a working set with criteria, accepted and rejected members, and what it is monitored for", "a filter is not semantic relevance, and membership of a collection is not a fact about the thing collected", 10},
  {"artifact", "A thing that was built", "artifact-graph", "", "a produced file, pack, binary or model shard, identified by what it contains", "an artifact existing is not a signature, a scan, a licence, or permission to run it", 6},
  {"release", "Something delivered somewhere", "destination-release", "", "material carried to a destination, in a lifecycle where only a receipt makes it a delivery", "delivered is not a reuse licence, and readiness is not delivery", 4},
  {"receipt", "What the record shows", "receipt-envelope", "", "the evidence that something happened, naming what and when", "a receipt proves an act, never that the act was correct or useful", 14},
  {"run", "Work that was done", "work-graph", "", "a durable execution with dependencies, state, and what it produced", "queue priority is not a durability guarantee, and a finished run is not a verified outcome", 6},
  {"schedule", "When something happens", "source-scheduler", "", "a cadence or a time something is due, derived from measurement where there is any", "a cadence is a reading plan, never permission to fetch, buy, contact or enrol", 11},
  {"budget", "What there is to spend", "funding-closure", "", "stated funds against stated costs, open for anyone to read", "an available figure is contributions less commitments, never money held or owed", 5},
  {"pledge", "What was promised", "commitment-graph", "", "somebody stating they will do or give something, on the rail that knows a promise is not a delivery", "a pledge is not a payment, not collectable by this estate, and not evidence value moved", 2},
  {"price", "What something costs", "price-observatory", "", "an amount a publisher stated about itself, with the document that admitted it", "an observed price is not a current offer, not available to this viewer, and not a quote this estate can honour", 4},
  {"order", "Something bought", "commerce-chain", "", "a header and its lines, moving through fulfilment states", "an order shape is not stock, not availability, and not a payment", 2},
  {"model", "A model and its variants", "model-topology-graph", "", "a family of weights and representations with measured behaviour", "low reconstruction error is not good generation, and a measured variant is not a licensed one", 2},
  {"capability", "Something the estate can do", "capability-lattice", "", "an operation with its inputs, its cost and the authority it answers to", "a capability existing is not authorisation to invoke it", 7},
  {"provider", "Who would do it", "cmd-provider-frontier", "", "an external party that could serve a capability, with what it charges where that is stated", "a listed provider is not an enrolled, credentialed or contracted one", 1},
  {"namespace", "Where a thing lives", "bonfyre-fs", "", "the universal path by which any addressable resource is located", "a path resolving is not permission to read what is there", 6},
  {"media", "Pictures and sound", "media-library", "", "retained images, audio and video with where they came from", "holding media is not a licence to publish it", 5},
  {"place", "Somewhere", "spatial-observation", "", "an observed location, at the precision it was observed", "a coordinate is not a route, an entrance, an address, or a right to be there", 2},
  {"publication", "Something published", "publication-program", "", "composed material at an address, from sources that remain named", "publishing material is not owning it, and a publication is not its sources' endorsement", 4},
  {"form", "Something filled in", "form-graph", "", "a structured question put to a person, and what they answered", "a completed form is not consent to anything not stated on it", 5},
  {"household", "The people who share a life", "household-economic-circuit", "", "a body composed for the people in it, whose record answers to them alone", "a household record is not a roster of anybody, and never readable by this estate's operators", 2},
  {"institution-plane", "How an institution can be reached", "institution-planes", "", "one surface of an institution, public or answering only to a relationship", "a granted plane is not reachable because it is named; it answers to a relationship a household holds", 1},
  {"lineage", "What came from what", "evidence-binding", "", "the chain from a derived thing back to the evidence it was derived from", "a derived field is not a new observation; it is one arrangement of its inputs and stale when they move", 7},
  {"version", "A branch of a large estate", "estate-version", "", "a named reading of stored material, comparable against another reading", "a snapshot records digests and not bytes: it says what changed and cannot give back what was there, and it cannot be merged with another", 4},
  {"message", "Something sent to somebody", "held-message", "", "a direct communication to a named person or group", "a composed message is not a sent one; nothing here contacts anybody", 4},
  {"signature", "Somebody agreeing in writing", "assent-signature", "", "a person's recorded assent to a specific document at a specific version", "an approval recorded in governance is not a signature, and neither is a click", 2},
  {"stream", "Something happening live", "live-session", "", "a broadcast in progress, with what is being shown and who is present", "a scheduled stream is not a live one, and a replay is not a broadcast", 1},
};
#define BF_INTERACTION_PRIMITIVE_COUNT (sizeof(bf_interaction_primitives) / sizeof(bf_interaction_primitives[0]))

static const BfCloneGrammar bf_clone_grammars[] = {
  {"model-commons", "Hugging Face Hub", "the model estate as something a person can look at: variants, cards, measured behaviour, artifacts, what uses it and what it came from", "model-topology-graph", "|model|artifact|lineage|thread|member|collection|receipt|", "a model page is not a licence to use the weights, and measured behaviour is not fitness for anyone's purpose", 1, 7, 0},
  {"model-execution", "Replicate", "a capability with an input box and a cost, so years of provider and economic machinery become one button", "capability-lattice", "|model|capability|provider|price|run|receipt|", "an estimated cost is not a quote, and a run button is not authority to spend", 1, 6, 0},
  {"artifact-registry", "Harbor", "compiled packs, binaries, model shards and templates as a trusted registry with provenance and replication", "artifact-graph", "|artifact|lineage|receipt|version|signature|", "a registered artifact is not a scanned, signed or safe one", 3, 5, 0},
  {"object-versioning", "lakeFS", "branching an enormous object estate: compare, commit, roll back, inspect provenance of derived collections", "bonfyre-fs", "|namespace|version|lineage|collection|receipt|", "a catalogue generation is not a branch; nothing here can be reverted", 1, 5, 0},
  {"file-browser", "Nextcloud and FileBrowser", "the universal namespace with a human face: folders, previews, recents, activity, shares", "bonfyre-fs", "|namespace|media|member|receipt|", "a path being browsable is not permission to read it; the namespace locates and authority gates", 1, 4, 0},
  {"media-estate", "Immich", "retained media as browsable memory: by time, place, album and what is in the frame", "media-library", "|media|place|collection|namespace|", "recognising something in a picture is not identifying anybody, and holding media is not a licence", 3, 4, 0},
  {"estate-catalogue", "OpenMetadata and DataHub", "the estate as a catalogue: ownership, lineage, usage, classification and the conversation about each thing", "evidence-binding", "|lineage|collection|member|thread|receipt|namespace|", "declared ownership is not authority over a thing, and lineage is not quality", 1, 6, 0},
  {"organ-catalogue", "Backstage", "what organs and capabilities exist, who owns them, whether they answer, and what depends on them", "capability-lattice", "|capability|member|run|namespace|lineage|", "a listed organ is not a healthy one, and an owner is not on call", 3, 5, 0},
  {"repository", "the GitHub repository page", "any composed thing read as a repository: overview, contents, history, contributors, releases, discussion", "artifact-graph", "|artifact|version|release|thread|member|lineage|", "a history being shown is not a history being navigable; nothing here can be checked out", 2, 6, 0},
  {"pipeline-runs", "GitHub Actions and Dagster", "durable work with a face: the graph, each step's state, logs, artifacts, retries and standing schedules", "work-graph", "|run|artifact|schedule|receipt|capability|", "a green run is not a correct result, and a retry is not idempotence", 3, 5, 0},
  {"discovery-sets", "Exa Websets", "the frontier as working sets: describe a cohort, state criteria, accept and reject candidates, keep it monitored", "source-frontier", "|collection|schedule|member|receipt|capability|", "a collection filter is not semantic relevance, and monitoring is not permission to fetch", 1, 5, 0},
  {"source-inspection", "Firecrawl", "what a source actually is: its map, its structure, what may be retained, and what changed since last time", "foreign-twin", "|collection|lineage|receipt|schedule|release|", "reading a source's structure is not a reuse licence, and a boundary response is not a source that does not exist", 2, 5, 0},
  {"distribution", "DistroKid", "one source to many destinations with states a person can read, where delivered means a receipt exists", "destination-release", "|release|receipt|collection|price|", "delivered is not published by the destination, and readiness is not delivery", 1, 4, 0},
  {"action-composition", "Zapier and Make", "when this happens do that, compiled onto the protocol and capability rails rather than a second runtime", "protocol-machine", "|capability|run|schedule|receipt|", "a composed automation is not an authorised effect; each step still answers to its own authority", 3, 4, 0},
  {"visual-pipelines", "n8n", "the same composition for an operator who wants to see the machine: branches, transforms, test data", "work-graph", "|run|capability|artifact|receipt|", "a pipeline that ran with test data is not one that may run with real data", 3, 4, 0},
  {"work-tracking", "Plane and Linear", "work people can see: issues, cycles, triage, assignment and state over the durable work graph", "work-graph", "|run|member|role|schedule|thread|", "an assignment is not a commitment anybody made, and a cycle is not a deadline", 1, 5, 0},
  {"scheduling", "Cal.com", "availability as a capability: office hours, volunteer slots, conferences, venue times", "source-scheduler", "|schedule|member|form|capability|message|", "an offered slot is not a booking, and a booking here contacts nobody", 2, 5, 0},
  {"relationship-workspace", "Attio and Twenty", "people, organisations, communities and households with their interactions and what is next, over the real relationship graph", "actor-graph", "|member|role|thread|collection|form|", "a recorded interaction is not a relationship either party would claim", 2, 5, 0},
  {"community-operations", "Discord", "a community's private operating layer: rooms, roles, moderation, events and the actions a member may take", "human-community-body", "|thread|moderation|member|role|schedule|message|", "a public presence does not expose a private room's contents", 2, 6, 0},
  {"public-discussion", "Reddit", "threaded public discussion with its moderation queue and rules, attached to a publication rather than being the product", "human-community-body", "|thread|moderation|member|publication|", "a reputation number is not standing anywhere else, and a mod decision is not a fact", 2, 4, 0},
  {"project-pitch", "Kickstarter", "how a community body is born: a pitch, a target, supporters, milestones and updates", "funding-frontier", "|proposal|pledge|budget|member|publication|schedule|", "a stated target is not money raised, held or promised, and reaching one is a stage rather than an ending", 1, 6, 0},
  {"recurring-support", "Patreon and Open Collective", "underwriting what a publication costs to run, with the budget and its receipts in the open", "funding-closure", "|pledge|budget|member|receipt|price|", "stated recurring support is an intention; this estate bills nobody and collects nothing", 2, 5, 0},
  {"governance", "Snapshot, Aragon and Safe", "proposal, electorate, vote, council, delegation and treasury authorisation, with policies rather than a vote on everything", "governance-grammar", "|proposal|decision|vote|delegation|role|member|budget|", "a passed vote is not an execution, and a treasury authorisation is not a payment", 1, 7, 0},
  {"commerce", "Shopify and Medusa", "a generic commerce substrate: catalogue, variants, orders, fulfilment, promotions and channels", "commerce-chain", "|order|price|collection|member|release|", "a catalogue is not stock, and an order shape is not a payment", 2, 5, 0},
  {"live-selling", "TikTok Shop and Twitch", "a live operator publication: what is on screen now, chat, guests, the schedule and a durable replay", "media-library", "|stream|media|order|thread|schedule|member|", "a stream recorded here is not a storefront on anyone else's marketplace", 2, 6, 0},
  {"publishing", "Ghost and Substack", "the author's side of a living publication: stories, newsletters, membership, archives", "publication-program", "|publication|member|thread|media|message|", "a subscriber list is not imported by this estate knowing the writer exists", 2, 5, 0},
  {"curation", "Pinterest, Are.na and Raindrop", "collected material as boards and research trails, which is how people actually understand relationship density", "source-frontier", "|collection|media|member|namespace|publication|", "collecting things together is not a claim they are related in the world", 2, 5, 0},
  {"household-habitat", "Cozi and Home Assistant", "a household's own calendar, lists, routines and bills, and the physical state of where it lives", "household-economic-circuit", "|household|schedule|form|budget|place|member|", "a household edition is composed for the people in it and is not shared by existing", 2, 6, 0},
  {"school-institution", "ParentSquare and Membership Toolkit", "the parent-facing institution: directories, volunteers, fundraising, forms, payments, events and governance", "institution-planes", "|institution-plane|household|member|role|form|budget|schedule|message|", "running a parent association here is not affiliation with any school or district, and no student record reaches it", 1, 8, 0},
  {"controlled-documents", "Papermark, DocSend and Documenso", "board material, proposals and funding packs with scoped sharing, approval and who actually opened them", "form-graph", "|form|artifact|signature|member|receipt|version|", "a viewed document is not an agreed one, and an approval is not a signature", 2, 6, 0},
};
#define BF_CLONE_GRAMMAR_COUNT (sizeof(bf_clone_grammars) / sizeof(bf_clone_grammars[0]))

static const BfInteractionPrimitive *bf_interaction_primitive(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_INTERACTION_PRIMITIVE_COUNT; i++)
        if (!strcmp(bf_interaction_primitives[i].primitive, name))
            return &bf_interaction_primitives[i];
    return NULL;
}

static const BfCloneGrammar *bf_clone_grammar(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_CLONE_GRAMMAR_COUNT; i++)
        if (!strcmp(bf_clone_grammars[i].grammar, name))
            return &bf_clone_grammars[i];
    return NULL;
}

/* Does this grammar need this primitive? Whole segments only, so "media"
 * never matches "media-estate". */
static int bf_grammar_needs(const BfCloneGrammar *g, const char *primitive) {
    const char *p;
    if (!g || !primitive || !*primitive) return 0;
    p = g->needs;
    while ((p = strchr(p, '|')) != NULL) {
        const char *start = ++p;
        const char *end = strchr(start, '|');
        size_t len;
        if (!end) break;
        len = (size_t)(end - start);
        if (len && !strncmp(start, primitive, len) && !primitive[len]) return 1;
        p = end;
    }
    return 0;
}
#endif
