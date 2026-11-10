/* Generated from schemas/control/operators.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_OPERATORS_H
#define BONFYRE_NATIVE_OPERATORS_H
#include <stddef.h>

/* `has` is what an operator already possesses; `unlocks` is what stops
 * being someone else's dependency. Neither claims any named platform
 * agrees, participates, or has been approached. */
typedef struct { const char *archetype; const char *label; const char *has;
                 const char *unlocks; const char *cannot_infer; } BfOperatorArchetype;

/* `studied` is provenance in the same sense a page region records a
 * reading rhythm: no donor code runs and no donor vocabulary reaches a page. */
typedef struct { const char *facet; const char *label; const char *studied;
                 const char *cannot_infer; } BfProjectFacet;

/* `needs` is a property measured off a compiled DocType's own fields, so
 * an affinity is a declared pairing evidenced by the compiler rather than
 * a resemblance read out of prose. It is recognition, never permission. */
typedef struct { const char *archetype; const char *needs; const char *because;
                 const char *cannot_infer; } BfRecordAffinity;

/* A name matches only as a whole underscore-separated segment of a
 * fieldname: no_of_employees names a company's size, and Frappe's own
 * parent names the document a row belongs to. */
typedef struct { const char *name; const char *why; } BfHouseholdReach;

static const BfOperatorArchetype bf_operator_archetypes[] = {
  {"forum-moderator", "Community moderator", "a community, its taxonomy, and years of curatorial judgement", "a publication they hold, with the sources and funding attached to it", "operating a community here implies nothing about any other platform's terms, and moves no membership"},
  {"live-seller", "Live-commerce host", "a show, an audience, and the skill of selling in front of one", "a durable shop and publication where one hour of work stops disappearing when the stream ends", "a stream recorded here is not a storefront on anyone else's marketplace"},
  {"chat-admin", "Chat-server admin", "a tight community and the habits that hold it together", "a public face for a private room, and discovery that does not depend on an invite", "a public presence does not expose a private room's contents"},
  {"supporter-creator", "Supported creator", "people who already pay to keep the work going", "funding attached to capabilities and publications rather than to posting frequency", "support stated here is not a subscription and collects nothing"},
  {"newsletter-writer", "Newsletter writer", "an editorial voice and a list that trusts it", "live data, actions and community around the story instead of under it", "a list is not imported by this estate knowing the writer exists"},
  {"marketplace-seller", "Marketplace seller", "inventory, product knowledge, and a reputation somewhere else", "media, community and several commerce rails around the same catalogue", "listings shown here are observations of a market, not offers this estate can fulfil"},
  {"local-promoter", "Local promoter", "scene knowledge nobody has written down", "tonight, tickets, food, transit and the afters composed as one night", "an event appearing is not a ticket, a reservation, or a promise it will happen"},
  {"deal-curator", "Deal community admin", "price intelligence and a crowd that checks it", "real comparison against observed inventory rather than screenshots of prices", "an observed price is what a source published, never a quote this estate can honour"},
  {"forum-maintainer", "Forum maintainer", "twenty years of accumulated answers", "that knowledge searchable, multimodal, and not one outage from gone", "retaining public material is not ownership of it"},
  {"pta-officer", "PTA or booster officer", "a school community, a fundraising calendar, and a volunteer list held together by hand", "one publication where the fundraiser, the sign-up, the vote and the books are the same object", "running a parent association here is not affiliation with any school or district, and no student record reaches it"},
  {"team-manager", "Coach or team manager", "a roster, a schedule, and parents who need to know when the bus leaves", "a season that composes itself from the fixtures, the venue and who is bringing what", "a schedule shown here is what its source published, never a fixture this estate can change or guarantee"},
  {"room-parent", "Room parent or volunteer coordinator", "the knack of turning a need into ten people showing up", "a need that states itself, fills itself, and closes when it is filled", "a filled need is people who said they would come, not attendance"},
  {"household-organiser", "Household organiser", "everyone else's calendar in their head", "one page where the school run, the bill, the permission slip and the practice are one day", "a household edition is composed for the people in it and is not shared by existing"},
  {"neighbourhood-organiser", "Neighbourhood organiser", "a street, a group chat, and the civic calendar nobody else reads", "the council agenda, the closure notice and the block party as one neighbourhood presence", "publishing a public notice is not speaking for the body that issued it"},
  {"club-admin", "Club or society admin", "members, dues, a committee and a constitution in a shared document", "membership, money and decisions carried in the open by the same publication", "a club's governance here binds its own members and nobody else"},
  {"stream-community", "Streaming community", "a live audience and the people who moderate it", "durable media, a shop, events and a publication between streams", "a community here is not a migration of anyone's followers"},
};
#define BF_OPERATOR_ARCHETYPE_COUNT (sizeof(bf_operator_archetypes) / sizeof(bf_operator_archetypes[0]))

static const BfProjectFacet bf_project_facets[] = {
  {"campaign", "What it is trying to reach", "reward-tier crowdfunding, where a pitch states a target and a deadline", "a stated target is not money raised, held or promised, and reaching one is a stage rather than an ending"},
  {"recurring", "What keeps it running", "recurring supporter platforms, where the relationship outlives the transaction", "stated recurring support is an intention; this estate bills nobody and collects nothing"},
  {"ledger", "What it costs, in the open", "transparent collective finance, where the budget and its receipts are public", "an available figure is contributions less commitments, never money held or owed"},
  {"bounty", "Discrete work, priced", "bounty boards, where a task carries a figure before anyone takes it", "a stated bounty is not an award and not an offer capable of acceptance"},
  {"recruiting", "The people it lacks", "open-team recruiting, where a project names roles rather than posts jobs", "a named role is not employment, not a contract, and not a claim anyone was engaged"},
  {"governance", "How it decides", "proposal-and-vote governance, and the newer rule-bounded kind where most acts need no vote", "a tally is what was said by those who said it, and never authority to carry it out"},
};
#define BF_PROJECT_FACET_COUNT (sizeof(bf_project_facets) / sizeof(bf_project_facets[0]))

static const BfRecordAffinity bf_record_affinities[] = {
  {"pta-officer", "money", "a parent association's whole year is fees, fundraisers and what was spent, so a record kind carrying amounts is the shape they already keep by hand", "recognising the shape is not authority to hold a school's financial records, and no school ledger reaches this estate"},
  {"pta-officer", "household", "the people a parent association serves are households, and a record kind naming a guardian is one they would already be careful with", "no student record, roster or contact list reaches a parent association here; a plane naming a household answers only to that household"},
  {"room-parent", "submittable", "a volunteer coordinator's work is asking and being answered, which is exactly a record somebody submits", "a submitted record is somebody stating they will; it is not attendance and not a commitment discharged"},
  {"team-manager", "place", "a season is fixtures somewhere, and a record kind naming a place is the part a manager spends the most time correcting", "a place named here is what its source published, never a venue this estate can book or guarantee"},
  {"household-organiser", "household", "a household organiser is already the person holding the calendar, the bills and the school run for the people in one", "a household edition is composed for the people in it and is not shared by existing"},
  {"club-admin", "submittable", "a club decides things, and a record somebody submits is where a decision starts rather than where it ends", "a club's governance binds its own members and nobody else, and submitting is not deciding"},
  {"club-admin", "money", "subscriptions, hire and equipment are the whole of a small club's administration", "an amount in a record kind is not money this estate holds, collects or can be asked for"},
  {"neighbourhood-organiser", "place", "a neighbourhood is a place before it is anything else, and a record kind that names one is already local", "publishing a public notice is not speaking for the body that issued it"},
  {"marketplace-seller", "money", "a seller's records are priced things, and a record kind carrying amounts is the ledger they keep whether or not anyone asked them to", "an amount shown here is an observation of a market, never an offer this estate can fulfil"},
  {"marketplace-seller", "composition", "an order is a header and its lines, which is precisely a record composed of rows of another kind", "a composed record is a shape, not stock, availability, or an order anybody placed"},
  {"deal-curator", "money", "a deal community is a published price and the argument about whether it is one", "an observed price is what a source published, never a quote this estate can honour"},
  {"local-promoter", "place", "a promoter's every record is a room on a night, and the place is the part that has to be right", "an event appearing is not a ticket, a reservation, or a promise it will happen"},
  {"forum-maintainer", "composition", "a thread is a post and its replies, which is a record made of rows of another kind", "retaining public material is not ownership of it"},
  {"supporter-creator", "money", "the relationship a supported creator holds is stated in amounts, whatever else it is", "support stated here is not a subscription and collects nothing"},
  {"newsletter-writer", "composition", "an issue is a set of items, and the shape of that is a record with rows inside it", "a list is not imported by this estate knowing the writer exists"},
};
#define BF_RECORD_AFFINITY_COUNT (sizeof(bf_record_affinities) / sizeof(bf_record_affinities[0]))

static const BfHouseholdReach bf_household_reach[] = {
  {"student", "a student is a person in a household, and their record is the one a school is most careful with"},
  {"guardian", "a guardian is named as somebody's guardian and in no other sense"},
  {"pupil", "the same person as a student, under the word some institutions use"},
  {"caregiver", "somebody responsible for a person in a household"},
  {"household", "the household itself, named directly"},
  {"next_of_kin", "the person an institution contacts about somebody, which is a household relationship and nothing else"},
};
#define BF_HOUSEHOLD_REACH_COUNT (sizeof(bf_household_reach) / sizeof(bf_household_reach[0]))
#endif
