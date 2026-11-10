package control

// The people this estate is actually for, and the shape of what they run.
//
// These are not ten integrations. They are ten populations of the same
// underlying product: someone who already knows how to run a small piece of
// civilization online, whose capability is currently fragmented by whichever
// application they learned it in. The pitch is "bring the thing you already
// know how to operate", not "create an account and start posting".
//
// `has` is what the operator already possesses before this estate exists;
// `unlocks` is what stops being someone else's dependency. Neither is a claim
// that any named platform agrees, participates, or has been approached.
#OperatorArchetype: {
    archetype: string & =~"^[a-z][a-z0-9-]*$"
    label: string & != ""
    has: string & != ""
    unlocks: string & != ""
    cannot_infer: string & != ""
}

// A project is one thing with several faces, not several products. A facet is
// a way of reading the same object: the same effort is a campaign before it
// starts, a supporter relationship while it runs, an open ledger for what it
// costs, and a set of decisions throughout.
//
// `studied` records where the grammar was learned, in the same sense the page
// regions record a reading rhythm: provenance only. No donor code runs here,
// no donor vocabulary reaches a page, and naming a project is not a claim its
// authors know this estate exists.
#ProjectFacet: {
    facet: "campaign" | "recurring" | "ledger" | "bounty" | "recruiting" | "governance"
    label: string & != ""
    studied: string & != ""
    cannot_infer: string & != ""
}

nativeOperatorArchetypes: [...#OperatorArchetype] & [
    {archetype: "forum-moderator", label: "Community moderator",
     has: "a community, its taxonomy, and years of curatorial judgement",
     unlocks: "a publication they hold, with the sources and funding attached to it",
     cannot_infer: "operating a community here implies nothing about any other platform's terms, and moves no membership"},
    {archetype: "live-seller", label: "Live-commerce host",
     has: "a show, an audience, and the skill of selling in front of one",
     unlocks: "a durable shop and publication where one hour of work stops disappearing when the stream ends",
     cannot_infer: "a stream recorded here is not a storefront on anyone else's marketplace"},
    {archetype: "chat-admin", label: "Chat-server admin",
     has: "a tight community and the habits that hold it together",
     unlocks: "a public face for a private room, and discovery that does not depend on an invite",
     cannot_infer: "a public presence does not expose a private room's contents"},
    {archetype: "supporter-creator", label: "Supported creator",
     has: "people who already pay to keep the work going",
     unlocks: "funding attached to capabilities and publications rather than to posting frequency",
     cannot_infer: "support stated here is not a subscription and collects nothing"},
    {archetype: "newsletter-writer", label: "Newsletter writer",
     has: "an editorial voice and a list that trusts it",
     unlocks: "live data, actions and community around the story instead of under it",
     cannot_infer: "a list is not imported by this estate knowing the writer exists"},
    {archetype: "marketplace-seller", label: "Marketplace seller",
     has: "inventory, product knowledge, and a reputation somewhere else",
     unlocks: "media, community and several commerce rails around the same catalogue",
     cannot_infer: "listings shown here are observations of a market, not offers this estate can fulfil"},
    {archetype: "local-promoter", label: "Local promoter",
     has: "scene knowledge nobody has written down",
     unlocks: "tonight, tickets, food, transit and the afters composed as one night",
     cannot_infer: "an event appearing is not a ticket, a reservation, or a promise it will happen"},
    {archetype: "deal-curator", label: "Deal community admin",
     has: "price intelligence and a crowd that checks it",
     unlocks: "real comparison against observed inventory rather than screenshots of prices",
     cannot_infer: "an observed price is what a source published, never a quote this estate can honour"},
    {archetype: "forum-maintainer", label: "Forum maintainer",
     has: "twenty years of accumulated answers",
     unlocks: "that knowledge searchable, multimodal, and not one outage from gone",
     cannot_infer: "retaining public material is not ownership of it"},
    {archetype: "pta-officer", label: "PTA or booster officer",
     has: "a school community, a fundraising calendar, and a volunteer list held together by hand",
     unlocks: "one publication where the fundraiser, the sign-up, the vote and the books are the same object",
     cannot_infer: "running a parent association here is not affiliation with any school or district, and no student record reaches it"},
    {archetype: "team-manager", label: "Coach or team manager",
     has: "a roster, a schedule, and parents who need to know when the bus leaves",
     unlocks: "a season that composes itself from the fixtures, the venue and who is bringing what",
     cannot_infer: "a schedule shown here is what its source published, never a fixture this estate can change or guarantee"},
    {archetype: "room-parent", label: "Room parent or volunteer coordinator",
     has: "the knack of turning a need into ten people showing up",
     unlocks: "a need that states itself, fills itself, and closes when it is filled",
     cannot_infer: "a filled need is people who said they would come, not attendance"},
    {archetype: "household-organiser", label: "Household organiser",
     has: "everyone else's calendar in their head",
     unlocks: "one page where the school run, the bill, the permission slip and the practice are one day",
     cannot_infer: "a household edition is composed for the people in it and is not shared by existing"},
    {archetype: "neighbourhood-organiser", label: "Neighbourhood organiser",
     has: "a street, a group chat, and the civic calendar nobody else reads",
     unlocks: "the council agenda, the closure notice and the block party as one neighbourhood presence",
     cannot_infer: "publishing a public notice is not speaking for the body that issued it"},
    {archetype: "club-admin", label: "Club or society admin",
     has: "members, dues, a committee and a constitution in a shared document",
     unlocks: "membership, money and decisions carried in the open by the same publication",
     cannot_infer: "a club's governance here binds its own members and nobody else"},
    {archetype: "stream-community", label: "Streaming community",
     has: "a live audience and the people who moderate it",
     unlocks: "durable media, a shop, events and a publication between streams",
     cannot_infer: "a community here is not a migration of anyone's followers"},
]

// Which field names actually reach into somebody's household.
//
// The first version of this asked whether a fieldname contained "student",
// "parent", "guardian", "household", "family", "employee" or "member" anywhere
// in it, and got three different kinds of wrong answer. CRM Lead was counted as
// reaching into a household because it has no_of_employees, a dropdown for how
// big a company is. Frappe's own parent, parenttype and parentfield are the
// document a row belongs to and have nothing to do with anybody's parent. And
// "member" is club membership in every app that has it.
//
// So the names are declared, and a name matches only as a whole underscore-
// separated segment of a fieldname. The honest consequence is that almost
// nothing in these nine applications reaches into a household, which is the
// contract being true rather than the estate lacking a feature.
#HouseholdReach: {
    name: string & =~"^[a-z][a-z_]*$"
    why: string & != ""
}

nativeHouseholdReach: [...#HouseholdReach] & [
    {name: "student", why: "a student is a person in a household, and their record is the one a school is most careful with"},
    {name: "guardian", why: "a guardian is named as somebody's guardian and in no other sense"},
    {name: "pupil", why: "the same person as a student, under the word some institutions use"},
    {name: "caregiver", why: "somebody responsible for a person in a household"},
    {name: "household", why: "the household itself, named directly"},
    {name: "next_of_kin", why: "the person an institution contacts about somebody, which is a household relationship and nothing else"},
]

// Measured, and worth stating rather than quietly discovering later: across the
// nine applications compiled here, no record kind names any of these as a whole
// segment. LMS comes closest with show_students, a setting, and a member_type
// whose options include "Student" -- neither of which is a person's record. The
// household planes therefore have nothing in these applications reaching them,
// which is the same finding as the frontier's: no roster or student-record plane
// appears anywhere, and that is the contract being true.
//
// "dependent" was declared here first and had to be withdrawn: it matched
// erpnext's price_not_uom_dependent, a flag about units of measure.

// What an operator archetype can actually work with, stated rather than guessed.
//
// The compiled record kinds and the operator grammar were, until now, two
// published vocabularies with nothing between them: a PTA officer existed as an
// archetype, 1,152 DocTypes existed as layouts, and no declaration said which
// of them a PTA officer has any business operating. Matching them on the prose
// in `has` would be the estate inventing an affinity out of its own wording.
//
// So the affinity is declared here and bound to something measured: `needs` is
// a property the compiler actually reads off a DocType's own fields -- an
// amount field, a child table, a geolocation, a submit step, or a field naming
// somebody's household. An archetype is offered for a record kind only when
// that property is present in it.
//
// It is not a permission. Naming an archetype beside a record kind says who
// would recognise the thing, never that they may act on it, and never that this
// estate holds any record of that kind for anyone.
#RecordAffinity: {
    archetype: string & =~"^[a-z][a-z0-9-]*$"
    needs: "money" | "composition" | "place" | "submittable" | "household"
    because: string & != ""
    cannot_infer: string & != ""
}

nativeRecordAffinities: [...#RecordAffinity] & [
    {archetype: "pta-officer", needs: "money",
     because: "a parent association's whole year is fees, fundraisers and what was spent, so a record kind carrying amounts is the shape they already keep by hand",
     cannot_infer: "recognising the shape is not authority to hold a school's financial records, and no school ledger reaches this estate"},
    {archetype: "pta-officer", needs: "household",
     because: "the people a parent association serves are households, and a record kind naming a guardian is one they would already be careful with",
     cannot_infer: "no student record, roster or contact list reaches a parent association here; a plane naming a household answers only to that household"},
    {archetype: "room-parent", needs: "submittable",
     because: "a volunteer coordinator's work is asking and being answered, which is exactly a record somebody submits",
     cannot_infer: "a submitted record is somebody stating they will; it is not attendance and not a commitment discharged"},
    {archetype: "team-manager", needs: "place",
     because: "a season is fixtures somewhere, and a record kind naming a place is the part a manager spends the most time correcting",
     cannot_infer: "a place named here is what its source published, never a venue this estate can book or guarantee"},
    {archetype: "household-organiser", needs: "household",
     because: "a household organiser is already the person holding the calendar, the bills and the school run for the people in one",
     cannot_infer: "a household edition is composed for the people in it and is not shared by existing"},
    {archetype: "club-admin", needs: "submittable",
     because: "a club decides things, and a record somebody submits is where a decision starts rather than where it ends",
     cannot_infer: "a club's governance binds its own members and nobody else, and submitting is not deciding"},
    {archetype: "club-admin", needs: "money",
     because: "subscriptions, hire and equipment are the whole of a small club's administration",
     cannot_infer: "an amount in a record kind is not money this estate holds, collects or can be asked for"},
    {archetype: "neighbourhood-organiser", needs: "place",
     because: "a neighbourhood is a place before it is anything else, and a record kind that names one is already local",
     cannot_infer: "publishing a public notice is not speaking for the body that issued it"},
    {archetype: "marketplace-seller", needs: "money",
     because: "a seller's records are priced things, and a record kind carrying amounts is the ledger they keep whether or not anyone asked them to",
     cannot_infer: "an amount shown here is an observation of a market, never an offer this estate can fulfil"},
    {archetype: "marketplace-seller", needs: "composition",
     because: "an order is a header and its lines, which is precisely a record composed of rows of another kind",
     cannot_infer: "a composed record is a shape, not stock, availability, or an order anybody placed"},
    {archetype: "deal-curator", needs: "money",
     because: "a deal community is a published price and the argument about whether it is one",
     cannot_infer: "an observed price is what a source published, never a quote this estate can honour"},
    {archetype: "local-promoter", needs: "place",
     because: "a promoter's every record is a room on a night, and the place is the part that has to be right",
     cannot_infer: "an event appearing is not a ticket, a reservation, or a promise it will happen"},
    {archetype: "forum-maintainer", needs: "composition",
     because: "a thread is a post and its replies, which is a record made of rows of another kind",
     cannot_infer: "retaining public material is not ownership of it"},
    {archetype: "supporter-creator", needs: "money",
     because: "the relationship a supported creator holds is stated in amounts, whatever else it is",
     cannot_infer: "support stated here is not a subscription and collects nothing"},
    {archetype: "newsletter-writer", needs: "composition",
     because: "an issue is a set of items, and the shape of that is a record with rows inside it",
     cannot_infer: "a list is not imported by this estate knowing the writer exists"},
]

nativeProjectFacets: [...#ProjectFacet] & [
    {facet: "campaign", label: "What it is trying to reach",
     studied: "reward-tier crowdfunding, where a pitch states a target and a deadline",
     cannot_infer: "a stated target is not money raised, held or promised, and reaching one is a stage rather than an ending"},
    {facet: "recurring", label: "What keeps it running",
     studied: "recurring supporter platforms, where the relationship outlives the transaction",
     cannot_infer: "stated recurring support is an intention; this estate bills nobody and collects nothing"},
    {facet: "ledger", label: "What it costs, in the open",
     studied: "transparent collective finance, where the budget and its receipts are public",
     cannot_infer: "an available figure is contributions less commitments, never money held or owed"},
    {facet: "bounty", label: "Discrete work, priced",
     studied: "bounty boards, where a task carries a figure before anyone takes it",
     cannot_infer: "a stated bounty is not an award and not an offer capable of acceptance"},
    {facet: "recruiting", label: "The people it lacks",
     studied: "open-team recruiting, where a project names roles rather than posts jobs",
     cannot_infer: "a named role is not employment, not a contract, and not a claim anyone was engaged"},
    {facet: "governance", label: "How it decides",
     studied: "proposal-and-vote governance, and the newer rule-bounded kind where most acts need no vote",
     cannot_infer: "a tally is what was said by those who said it, and never authority to carry it out"},
]
