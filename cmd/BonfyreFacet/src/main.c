/* The facet compiler.
 *
 * A protocol integration is a facet, not a vertical adapter. Compiling facets
 * from domains degenerates into `if earthquake then FDSN`, which turns 739
 * protocols into 739 integrations and a taxonomy swamp. Compiling them from the
 * phenotype of the thing crossing the fabric turns the same 739 into a library
 * of typed transition grammars, and one object may carry several phenotypes at
 * once: a property is a spatial resource and a versioned record and an economic
 * offer simultaneously, and each opens different lawful facets.
 *
 * Nothing here is a new classifier. Phenotypes are derived from evidence the
 * corpus already carries and already witnesses -- candidate affordances, estate
 * roles, the spatial classification -- because a second opinion computed from
 * the same descriptions would be a second truth store with no extra evidence
 * behind it.
 *
 * What this produces is candidates. The whole point of the finite relation
 * vocabulary is that it makes the ladder visible:
 *
 *   candidate_match  ->  contract_compiled  ->  enacted
 *                    ->  obligations_closed ->  effect_authorized
 *
 * and collapsing any adjacent pair is the failure mode. This organ only ever
 * emits the first rung. Something else must witness the rest.
 */
#define _POSIX_C_SOURCE 200809L
#include <sqlite3.h>
#include "bonfyre.h"
#include "bf_command_lookup.h"
#include "protocol_derivation_bridge.h"
#include <ctype.h>
#include <stdint.h>
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf_lex.h"
/* ------------------------------------------------------------ vocabulary -- */

static const char *const RELATIONS[] = {
    "DISCOVER","IDENTIFY","AUTHENTICATE","AUTHORIZE","OBSERVE","SYNCHRONIZE",
    "DESCRIBE","QUERY","FILTER","SUBSCRIBE","NOTIFY","TRANSFER","STREAM",
    "PUBLISH","FEDERATE","TASK","COMMAND","NEGOTIATE","QUOTE","COMMIT","PAY",
    "SETTLE","PROVE","VERIFY","ROUND_TRIP"
};
#define RELATION_COUNT ((int)(sizeof(RELATIONS)/sizeof(*RELATIONS)))

static const char *const PHENOTYPES[] = {
    "VERSIONED_RECORD","EVENT","LIVE_ENTITY","MEASUREMENT_STREAM",
    "FIELD_COVERAGE","DATASET","MEDIA_ASSET","COMMANDABLE_RESOURCE",
    "ECONOMIC_OFFER","SUPPLY_CHAIN_OBJECT","IDENTITY_CREDENTIAL",
    "SERVICE_CAPABILITY","DOCUMENT","SPATIAL_RESOURCE","OBLIGATION"
};
#define PHENOTYPE_COUNT ((int)(sizeof(PHENOTYPES)/sizeof(*PHENOTYPES)))

enum { P_VERSIONED_RECORD, P_EVENT, P_LIVE_ENTITY, P_MEASUREMENT_STREAM,
       P_FIELD_COVERAGE, P_DATASET, P_MEDIA_ASSET, P_COMMANDABLE_RESOURCE,
       P_ECONOMIC_OFFER, P_SUPPLY_CHAIN_OBJECT, P_IDENTITY_CREDENTIAL,
       P_SERVICE_CAPABILITY, P_DOCUMENT, P_SPATIAL_RESOURCE, P_OBLIGATION };

enum { R_DISCOVER, R_IDENTIFY, R_AUTHENTICATE, R_AUTHORIZE, R_OBSERVE,
       R_SYNCHRONIZE, R_DESCRIBE, R_QUERY, R_FILTER, R_SUBSCRIBE, R_NOTIFY,
       R_TRANSFER, R_STREAM, R_PUBLISH, R_FEDERATE, R_TASK, R_COMMAND,
       R_NEGOTIATE, R_QUOTE, R_COMMIT, R_PAY, R_SETTLE, R_PROVE, R_VERIFY,
       R_ROUND_TRIP };

#define BIT(x) (1u << (x))

/* Which relations are meaningful for a phenotype.
 *
 * This is the architectural content of the compiler and it is deliberately not
 * derived from any protocol's own description: what it means to interact with a
 * live entity is a property of live entities, and a protocol that claims a
 * relation the phenotype cannot carry is claiming something about itself rather
 * than about the thing. Every phenotype can be discovered, described and
 * verified; beyond that they differ, and the differences are the reason the
 * table exists. */
static const uint32_t PHENOTYPE_RELATIONS[PHENOTYPE_COUNT] = {
    /* VERSIONED_RECORD     */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_FILTER)|
                               BIT(R_SYNCHRONIZE)|BIT(R_TRANSFER)|BIT(R_PROVE)|BIT(R_VERIFY)|
                               BIT(R_ROUND_TRIP)|BIT(R_IDENTIFY),
    /* EVENT                */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_OBSERVE)|BIT(R_QUERY)|
                               BIT(R_FILTER)|BIT(R_SUBSCRIBE)|BIT(R_NOTIFY)|BIT(R_PUBLISH)|
                               BIT(R_FEDERATE)|BIT(R_PROVE)|BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
    /* LIVE_ENTITY          */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_OBSERVE)|BIT(R_QUERY)|
                               BIT(R_SUBSCRIBE)|BIT(R_NOTIFY)|BIT(R_STREAM)|BIT(R_TRANSFER)|
                               BIT(R_IDENTIFY)|BIT(R_VERIFY),
    /* MEASUREMENT_STREAM   */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_OBSERVE)|BIT(R_QUERY)|
                               BIT(R_FILTER)|BIT(R_SUBSCRIBE)|BIT(R_NOTIFY)|BIT(R_STREAM)|
                               BIT(R_SYNCHRONIZE)|BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
    /* FIELD_COVERAGE       */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_FILTER)|
                               BIT(R_OBSERVE)|BIT(R_TRANSFER)|BIT(R_SYNCHRONIZE)|BIT(R_VERIFY),
    /* DATASET              */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_FILTER)|
                               BIT(R_SYNCHRONIZE)|BIT(R_TRANSFER)|BIT(R_PUBLISH)|BIT(R_PROVE)|
                               BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
    /* MEDIA_ASSET          */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_STREAM)|
                               BIT(R_TRANSFER)|BIT(R_PUBLISH)|BIT(R_FEDERATE)|BIT(R_PROVE)|
                               BIT(R_VERIFY),
    /* COMMANDABLE_RESOURCE */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_IDENTIFY)|
                               BIT(R_AUTHENTICATE)|BIT(R_AUTHORIZE)|BIT(R_OBSERVE)|
                               BIT(R_TASK)|BIT(R_COMMAND)|BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
    /* ECONOMIC_OFFER       */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_NEGOTIATE)|
                               BIT(R_QUOTE)|BIT(R_COMMIT)|BIT(R_PAY)|BIT(R_SETTLE)|
                               BIT(R_AUTHORIZE)|BIT(R_VERIFY),
    /* SUPPLY_CHAIN_OBJECT  */ BIT(R_DISCOVER)|BIT(R_IDENTIFY)|BIT(R_DESCRIBE)|BIT(R_OBSERVE)|
                               BIT(R_QUERY)|BIT(R_NOTIFY)|BIT(R_TRANSFER)|BIT(R_PROVE)|
                               BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
    /* IDENTITY_CREDENTIAL  */ BIT(R_DISCOVER)|BIT(R_IDENTIFY)|BIT(R_AUTHENTICATE)|
                               BIT(R_AUTHORIZE)|BIT(R_DESCRIBE)|BIT(R_PROVE)|BIT(R_VERIFY),
    /* SERVICE_CAPABILITY   */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_AUTHENTICATE)|
                               BIT(R_AUTHORIZE)|BIT(R_QUERY)|BIT(R_TASK)|BIT(R_NEGOTIATE)|
                               BIT(R_QUOTE)|BIT(R_COMMIT)|BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
    /* DOCUMENT             */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_TRANSFER)|
                               BIT(R_SYNCHRONIZE)|BIT(R_PUBLISH)|BIT(R_PROVE)|BIT(R_VERIFY),
    /* SPATIAL_RESOURCE     */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_QUERY)|BIT(R_FILTER)|
                               BIT(R_OBSERVE)|BIT(R_IDENTIFY)|BIT(R_TRANSFER)|BIT(R_VERIFY),
    /* OBLIGATION           */ BIT(R_DISCOVER)|BIT(R_DESCRIBE)|BIT(R_AUTHORIZE)|
                               BIT(R_NEGOTIATE)|BIT(R_COMMIT)|BIT(R_SETTLE)|BIT(R_PROVE)|
                               BIT(R_VERIFY)|BIT(R_ROUND_TRIP),
};

/* The estate's own affordance vocabulary, mapped to the relation vocabulary.
 *
 * These affordances are already computed and witnessed on every occurrence, so
 * a protocol's relations come from what the estate observed it offering rather
 * than from a fresh reading of its description. */
struct AffordanceRelation { const char *affordance; uint32_t relations; };
static const struct AffordanceRelation AFFORDANCE_RELATIONS[] = {
    {"DISCOVER",     BIT(R_DISCOVER)|BIT(R_DESCRIBE)},
    {"SEARCH",       BIT(R_QUERY)|BIT(R_FILTER)},
    {"COMPARE",      BIT(R_QUERY)|BIT(R_FILTER)},
    {"OBSERVE",      BIT(R_OBSERVE)},
    {"MEASURE",      BIT(R_OBSERVE)|BIT(R_VERIFY)},
    {"INSPECT",      BIT(R_OBSERVE)|BIT(R_VERIFY)},
    {"REPORT",       BIT(R_PUBLISH)|BIT(R_NOTIFY)},
    {"ALERT",        BIT(R_NOTIFY)|BIT(R_SUBSCRIBE)},
    {"PUBLISH",      BIT(R_PUBLISH)|BIT(R_FEDERATE)},
    {"COMMUNICATE",  BIT(R_NOTIFY)|BIT(R_TRANSFER)},
    {"STREAM",       BIT(R_STREAM)|BIT(R_SUBSCRIBE)},
    {"ROUTE",        BIT(R_QUERY)|BIT(R_TRANSFER)},
    {"LOCATE",       BIT(R_QUERY)|BIT(R_IDENTIFY)},
    {"IDENTIFY",     BIT(R_IDENTIFY)},
    {"AUTHENTICATE", BIT(R_AUTHENTICATE)},
    {"AUTHORIZE",    BIT(R_AUTHORIZE)},
    {"SIGN",         BIT(R_PROVE)|BIT(R_VERIFY)},
    {"CERTIFY",      BIT(R_PROVE)|BIT(R_VERIFY)},
    {"VERIFY",       BIT(R_VERIFY)|BIT(R_ROUND_TRIP)},
    {"CLASSIFY",     BIT(R_DESCRIBE)},
    {"TRANSLATE",    BIT(R_DESCRIBE)|BIT(R_TRANSFER)},
    {"MATCH",        BIT(R_QUERY)|BIT(R_FILTER)},
    {"SCHEDULE",     BIT(R_NEGOTIATE)|BIT(R_COMMIT)},
    {"NEGOTIATE",    BIT(R_NEGOTIATE)},
    {"QUOTE",        BIT(R_QUOTE)},
    {"PURCHASE",     BIT(R_COMMIT)|BIT(R_PAY)},
    {"PAY",          BIT(R_PAY)},
    {"SETTLE",       BIT(R_SETTLE)},
    {"CONTRACT",     BIT(R_NEGOTIATE)|BIT(R_COMMIT)},
    {"INSURE",       BIT(R_COMMIT)|BIT(R_SETTLE)},
    {"HIRE",         BIT(R_TASK)|BIT(R_COMMIT)},
    {"REPAIR",       BIT(R_TASK)|BIT(R_COMMAND)},
    {"CARE",         BIT(R_TASK)},
    {"OPTIMIZE",     BIT(R_TASK)|BIT(R_COMMAND)},
    {"SIMULATE",     BIT(R_QUERY)},
    {"PREDICT",      BIT(R_QUERY)},
    {"TEACH",        BIT(R_DESCRIBE)},
    {"FILE",         BIT(R_TRANSFER)|BIT(R_SYNCHRONIZE)},
    /* Declared by #ExternalAffordance and mapped to nothing until now. No
     * occurrence in the estate carries them yet, so nothing was losing
     * relations -- but a source that arrives tomorrow carrying SHIP would have
     * contributed nothing to its own facet set and no one would have been told.
     * A declared affordance that derives no relation is a silent hole, and the
     * witness beside this now refuses one. */
    {"SHIP",         BIT(R_TRANSFER)|BIT(R_TASK)|BIT(R_NOTIFY)},
    {"STORE",        BIT(R_TRANSFER)|BIT(R_SYNCHRONIZE)},
    {"CONTROL",      BIT(R_COMMAND)|BIT(R_AUTHORIZE)},
    {"RESERVE",      BIT(R_NEGOTIATE)|BIT(R_COMMIT)},
    {"FINANCE",      BIT(R_QUOTE)|BIT(R_COMMIT)|BIT(R_SETTLE)},
    {"NOTIFY",       BIT(R_NOTIFY)|BIT(R_SUBSCRIBE)},
};
#define AFFORDANCE_RELATION_COUNT ((int)(sizeof(AFFORDANCE_RELATIONS)/sizeof(*AFFORDANCE_RELATIONS)))

/* Which phenotypes an occurrence carries, from the same already-witnessed
 * evidence. An affordance says what may be done with the thing; a role says
 * what the thing is to this estate; the spatial classification says whether it
 * bears position. Together they say what shape it is. */
struct PhenotypeRule { const char *evidence; int phenotype; };
static const struct PhenotypeRule AFFORDANCE_PHENOTYPES[] = {
    /* Only affordances that say what the thing *is*. An affordance describing
     * how a source is reached says nothing about its shape, and mapping those
     * across was measured producing the swamp this compiler exists to avoid:
     * `AUTHENTICATE` made the USGS earthquake feed an IDENTITY_CREDENTIAL and
     * a commerce protocol then offered it IDENTIFY. Dropped for that reason,
     * each recorded here rather than silently: AUTHENTICATE and IDENTIFY (how
     * you reach a source, not what it holds), OBSERVE and SEARCH and COMPARE
     * (what you may do with almost anything), PUBLISH and COMMUNICATE and
     * TRANSLATE (properties of a carrier), MATCH and INSPECT and VERIFY (too
     * generic to name a shape). Those all remain relation evidence, where they
     * are true. */
    {"ALERT",        P_EVENT},
    {"REPORT",       P_EVENT},
    {"STREAM",       P_LIVE_ENTITY},
    {"MEASURE",      P_MEASUREMENT_STREAM},
    {"PREDICT",      P_FIELD_COVERAGE},
    {"SIMULATE",     P_FIELD_COVERAGE},
    {"SIGN",         P_IDENTITY_CREDENTIAL},
    {"CERTIFY",      P_IDENTITY_CREDENTIAL},
    {"FILE",         P_DOCUMENT},
    {"LOCATE",       P_SPATIAL_RESOURCE},
    {"ROUTE",        P_SPATIAL_RESOURCE},
    {"QUOTE",        P_ECONOMIC_OFFER},
    {"PAY",          P_ECONOMIC_OFFER},
    {"PURCHASE",     P_ECONOMIC_OFFER},
    {"SETTLE",       P_ECONOMIC_OFFER},
    {"NEGOTIATE",    P_ECONOMIC_OFFER},
    {"CONTRACT",     P_OBLIGATION},
    {"INSURE",       P_OBLIGATION},
    {"SCHEDULE",     P_OBLIGATION},
    {"HIRE",         P_SERVICE_CAPABILITY},
    {"CARE",         P_SERVICE_CAPABILITY},
    {"TEACH",        P_SERVICE_CAPABILITY},
    {"REPAIR",       P_COMMANDABLE_RESOURCE},
    {"OPTIMIZE",     P_COMMANDABLE_RESOURCE},
};
#define AFFORDANCE_PHENOTYPE_COUNT ((int)(sizeof(AFFORDANCE_PHENOTYPES)/sizeof(*AFFORDANCE_PHENOTYPES)))

/* Domain evidence, from the phenotype's own examples.
 *
 * Affordances and roles alone left five phenotypes nearly empty and one at
 * zero: the corpus tags ALERT and REPORT sparsely, so EVENT reached 132 of
 * 4,891 while the estate plainly carries earthquakes, concerts, claims,
 * outages, filings and transactions. Affordance evidence says what may be done
 * with a thing; domain evidence says what kind of thing a domain is mostly
 * made of. Both are needed and they are not the same strength, which is why
 * the derivation tier is reported per phenotype rather than flattened.
 *
 * The mapping is the phenotype definitions read backwards. A deed, a permit, a
 * policy, a law and an invoice are versioned records, so law, government,
 * housing, land and finance carry VERSIONED_RECORD. A bus, an aircraft, a
 * vessel and a courier are live entities, so vehicles, ships, spacecraft and
 * logistics carry LIVE_ENTITY. Nothing here is invented for the corpus.
 *
 * Domain evidence is weaker than shape evidence and says so. A legal API in
 * the `law` domain is a SERVICE_CAPABILITY that happens to be about documents,
 * and the tier is what lets a reader see that a phenotype came from the
 * neighbourhood rather than from the thing. */
static const struct PhenotypeRule DOMAIN_PHENOTYPES[] = {
    {"law",                P_VERSIONED_RECORD}, {"government",  P_VERSIONED_RECORD},
    {"housing",            P_VERSIONED_RECORD}, {"land",        P_VERSIONED_RECORD},
    {"finance",            P_VERSIONED_RECORD}, {"medicine",    P_VERSIONED_RECORD},

    {"weather",            P_EVENT},            {"sports",      P_EVENT},
    {"music",              P_EVENT},            {"natural-resources", P_EVENT},

    {"vehicles",           P_LIVE_ENTITY},      {"ships",       P_LIVE_ENTITY},
    {"spacecraft",         P_LIVE_ENTITY},      {"logistics",   P_LIVE_ENTITY},

    {"energy",             P_MEASUREMENT_STREAM}, {"machines",  P_MEASUREMENT_STREAM},
    {"science",            P_MEASUREMENT_STREAM},

    {"oceans",             P_FIELD_COVERAGE},   {"telecommunications", P_FIELD_COVERAGE},

    {"research",           P_DATASET},

    {"media",              P_MEDIA_ASSET},      {"publishing",  P_MEDIA_ASSET},
    {"film",               P_MEDIA_ASSET},      {"art",         P_MEDIA_ASSET},

    {"robots",             P_COMMANDABLE_RESOURCE}, {"factories", P_COMMANDABLE_RESOURCE},
    {"buildings",          P_COMMANDABLE_RESOURCE},

    {"retail",             P_ECONOMIC_OFFER},   {"trade",       P_ECONOMIC_OFFER},
    {"travel",             P_ECONOMIC_OFFER},

    {"manufacturing",      P_SUPPLY_CHAIN_OBJECT}, {"food",     P_SUPPLY_CHAIN_OBJECT},

    {"people",             P_IDENTITY_CREDENTIAL}, {"labor",    P_IDENTITY_CREDENTIAL},

    {"agents",             P_SERVICE_CAPABILITY},  {"models",   P_SERVICE_CAPABILITY},
    {"repair",             P_SERVICE_CAPABILITY},

    {"computers",          P_DOCUMENT},

    {"buildings",          P_SPATIAL_RESOURCE},
};
#define DOMAIN_PHENOTYPE_COUNT ((int)(sizeof(DOMAIN_PHENOTYPES)/sizeof(*DOMAIN_PHENOTYPES)))

/* Which protocol families a phenotype's relations are usually carried over.
 * Named families, not a routing vocabulary: these fill a facet's
 * carrier_candidates so it says which relation is possible *and over what*,
 * while AddressPlane still addresses affordances and never protocol names. */
static const char *const PHENOTYPE_CARRIERS[PHENOTYPE_COUNT] = {
    /* VERSIONED_RECORD     */ "Memento,ResourceSync,WebDAV,FHIR,Akoma Ntoso,XBRL",
    /* EVENT                */ "CloudEvents,Atom,RSS,ActivityPub,CAP,EDXL,iCalendar,WebSub",
    /* LIVE_ENTITY          */ "GTFS-RT,SIRI,FIXM,AIS,MOQT,MQTT",
    /* MEASUREMENT_STREAM   */ "SensorThings,WoT,MQTT,OPC UA,Arrow Flight,MOQT",
    /* FIELD_COVERAGE       */ "OGC EDR,WCS,STAC,Zarr,GeoParquet",
    /* DATASET              */ "DCAT,Dataspace Protocol,RO-Crate,STAC,OAI-PMH",
    /* MEDIA_ASSET          */ "C2PA,IPTC,HLS,DASH,WHIP,ActivityPub",
    /* COMMANDABLE_RESOURCE */ "WoT,OPC UA,Matter,OCPP,NETCONF,MCP,A2A",
    /* ECONOMIC_OFFER       */ "UCP,Beckn,AP2,x402,ISO 20022",
    /* SUPPLY_CHAIN_OBJECT  */ "GS1 Digital Link,EPCIS,Catena-X,UN/CEFACT",
    /* IDENTITY_CREDENTIAL  */ "OIDC,Verifiable Credentials,DID,SCIM,SPIFFE",
    /* SERVICE_CAPABILITY   */ "MCP,A2A,OpenAPI,Dataspace Protocol,x402",
    /* DOCUMENT             */ "Signposting,IIIF,RO-Crate,LegalRuleML,SWORD",
    /* SPATIAL_RESOURCE     */ "OGC API Features,CQL2,GeoSPARQL,STAC",
    /* OBLIGATION           */ "WorkGraph,CommitmentGraph,UBL,Peppol,UN/CEFACT",
};

/* What actually invalidates a facet on this phenotype.
 *
 * Density is only affordable if it is precise: more protocol projections must
 * not mean more recomputation. A bus moving invalidates its position, its
 * nearby queries and its live projections, and touches neither its route
 * identity nor its fare policy. A price change invalidates the offer and the
 * quote and not the product's supply-chain history. A licence change
 * invalidates onward distribution and rewrites none of the bytes already
 * observed. */
static const char *const PHENOTYPE_INVALIDATION[PHENOTYPE_COUNT] = {
    /* VERSIONED_RECORD     */ "record:new_version,record:effective_dates_changed",
    /* EVENT                */ "event:new_occurrence,event:retracted,event:time_changed",
    /* LIVE_ENTITY          */ "position:changed,trip:changed",
    /* MEASUREMENT_STREAM   */ "sample:appended,calibration:changed",
    /* FIELD_COVERAGE       */ "field:new_grid,field:uncertainty_changed",
    /* DATASET              */ "dataset:new_distribution,dataset:licence_changed",
    /* MEDIA_ASSET          */ "publication:updated,provenance:changed",
    /* COMMANDABLE_RESOURCE */ "capability:changed,availability:changed",
    /* ECONOMIC_OFFER       */ "price:changed,availability:changed,offer:withdrawn",
    /* SUPPLY_CHAIN_OBJECT  */ "custody:changed,location:changed",
    /* IDENTITY_CREDENTIAL  */ "credential:reissued,credential:revoked",
    /* SERVICE_CAPABILITY   */ "capability:changed,admission:changed",
    /* DOCUMENT             */ "document:new_version,rights:changed",
    /* SPATIAL_RESOURCE     */ "geometry:changed,tenure:changed",
    /* OBLIGATION           */ "obligation:discharged,obligation:amended",
};

static const struct PhenotypeRule ROLE_PHENOTYPES[] = {
    {"material_source",            P_DATASET},
    {"publication_carrier",        P_MEDIA_ASSET},
    {"media_service",              P_MEDIA_ASSET},
    {"external_service_candidate", P_SERVICE_CAPABILITY},
    {"action_adapter",             P_COMMANDABLE_RESOURCE},
    {"verification_tool",          P_VERSIONED_RECORD},
    {"reference_standard",         P_DOCUMENT},
    {"design_reference",           P_DOCUMENT},
    {"interaction_reference",      P_DOCUMENT},
};
#define ROLE_PHENOTYPE_COUNT ((int)(sizeof(ROLE_PHENOTYPES)/sizeof(*ROLE_PHENOTYPES)))

/* AddressPlane coordinates for a phenotype: affordances, never protocol names.
 * `supports_gtfs` becomes another taxonomy swamp, and a live bus and a private
 * physiological reading share `temporal:live` while InformationPolicy and
 * Authority split them into entirely different closures. */
static const char *const PHENOTYPE_AFFORDANCE[PHENOTYPE_COUNT][4] = {
    /* VERSIONED_RECORD     */ {"semantic:record","temporal:versioned","interaction:read","effect:none"},
    /* EVENT                */ {"semantic:event","temporal:historical","interaction:subscribe","effect:none"},
    /* LIVE_ENTITY          */ {"semantic:measurement","temporal:live","spatial:trajectory","interaction:subscribe"},
    /* MEASUREMENT_STREAM   */ {"semantic:measurement","temporal:live","interaction:subscribe","effect:none"},
    /* FIELD_COVERAGE       */ {"semantic:dataset","spatial:coverage","interaction:read","effect:none"},
    /* DATASET              */ {"semantic:dataset","temporal:versioned","interaction:read","effect:none"},
    /* MEDIA_ASSET          */ {"semantic:document","temporal:versioned","interaction:read","effect:none"},
    /* COMMANDABLE_RESOURCE */ {"semantic:capability","resource:physical","interaction:command","effect:physical"},
    /* ECONOMIC_OFFER       */ {"semantic:offer","interaction:commit","interaction:settle","effect:financial"},
    /* SUPPLY_CHAIN_OBJECT  */ {"semantic:record","spatial:trajectory","interaction:read","effect:reversible"},
    /* IDENTITY_CREDENTIAL  */ {"semantic:credential","privacy:restricted","interaction:read","effect:none"},
    /* SERVICE_CAPABILITY   */ {"semantic:capability","resource:human","interaction:commit","effect:reversible"},
    /* DOCUMENT             */ {"semantic:document","temporal:versioned","interaction:read","effect:none"},
    /* SPATIAL_RESOURCE     */ {"semantic:record","spatial:point","interaction:read","effect:none"},
    /* OBLIGATION           */ {"semantic:offer","interaction:commit","resource:human","effect:reversible"},
};

/* ------------------------------------------------------------------ util -- */

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_text(const char *s) {
    bf_lex_json_emit(stdout, s);
}

/* The carrier and invalidation lists are authored as one string per phenotype
 * because that is how they read; they are emitted as arrays because that is
 * what the contract declares. */
static void print_delimited(const char *list) {
    char buffer[512];
    int first = 1;
    putchar('[');
    if (list && *list) {
        snprintf(buffer, sizeof(buffer), "%s", list);
        for (char *token = strtok(buffer, ","); token; token = strtok(NULL, ",")) {
            if (!first) putchar(',');
            first = 0;
            json_text(token);
        }
    }
    putchar(']');
}

static int has_flag(int argc, char **argv, const char *flag) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], flag)) return 1;
    return 0;
}

static const char *arg(int argc, char **argv, const char *flag) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], flag)) return argv[i+1];
    return NULL;
}

struct Occurrence {
    char id[64];
    char name[160];
    char source_kind[32];
    uint16_t phenotypes;        /* union of both tiers */
    uint16_t shape_phenotypes;  /* derived from what the thing is */
    uint16_t domain_phenotypes; /* derived from the neighbourhood it sits in */
    uint32_t offers;        /* relations this occurrence's affordances offer */
    uint32_t meaningful;    /* relations meaningful for its phenotypes */
    int spatial_bearing;
    char domains[256];          /* topic domains, comma-delimited as observed */
};

static uint32_t phenotype_relations(uint16_t mask) {
    uint32_t out = 0;
    for (int p = 0; p < PHENOTYPE_COUNT; p++)
        if (mask & (1u << p)) out |= PHENOTYPE_RELATIONS[p];
    return out;
}

/* ------------------------------------------------------------ corpus read -- */

/* The corpus is JSONL and SQLite's JSON1 is already how every other organ here
 * reads it, so this stays consistent rather than hand-rolling a parser. */
static int load_corpus(const char *path, struct Occurrence **out, size_t *count) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL, *lst = NULL;
    FILE *fh = fopen(path, "rb");
    char *line = NULL;
    size_t cap = 0, n = 0, used = 0;
    ssize_t len;
    struct Occurrence *rows = NULL;

    if (!fh) { fprintf(stderr, "cannot read corpus %s\n", path); return 0; }
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) { fclose(fh); return 0; }
    if (sqlite3_prepare_v2(db,
        "SELECT json_extract(?1,'$.id'), json_extract(?1,'$.observed_name'),"
        "       json_extract(?1,'$.source_kind'),"
        "       json_extract(?1,'$.projection.spatial_candidate')", -1, &st, NULL) != SQLITE_OK)
        goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT value FROM json_each(?1, ?2) WHERE type='text'", -1, &lst, NULL) != SQLITE_OK)
        goto done;

    while ((len = getline(&line, &cap, fh)) > 0) {
        struct Occurrence row;
        if (len && line[len-1] == '\n') line[--len] = '\0';
        if (!len) continue;
        memset(&row, 0, sizeof(row));
        sqlite3_bind_text(st, 1, line, (int)len, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *a = (const char *)sqlite3_column_text(st, 0);
            const char *b = (const char *)sqlite3_column_text(st, 1);
            const char *c = (const char *)sqlite3_column_text(st, 2);
            const char *d = (const char *)sqlite3_column_text(st, 3);
            if (a) snprintf(row.id, sizeof(row.id), "%s", a);
            if (b) snprintf(row.name, sizeof(row.name), "%s", b);
            if (c) snprintf(row.source_kind, sizeof(row.source_kind), "%s", c);
            row.spatial_bearing = d && !strcmp(d, "bearing");
        }
        sqlite3_reset(st); sqlite3_clear_bindings(st);
        if (!row.id[0]) continue;

        /* Affordances give both what the thing offers and what shape it is. */
        sqlite3_bind_text(lst, 1, line, (int)len, SQLITE_TRANSIENT);
        sqlite3_bind_text(lst, 2, "$.projection.candidate_affordances", -1, SQLITE_STATIC);
        while (sqlite3_step(lst) == SQLITE_ROW) {
            const char *v = (const char *)sqlite3_column_text(lst, 0);
            if (!v) continue;
            for (int i = 0; i < AFFORDANCE_RELATION_COUNT; i++)
                if (!strcmp(v, AFFORDANCE_RELATIONS[i].affordance))
                    row.offers |= AFFORDANCE_RELATIONS[i].relations;
            for (int i = 0; i < AFFORDANCE_PHENOTYPE_COUNT; i++)
                if (!strcmp(v, AFFORDANCE_PHENOTYPES[i].evidence))
                    row.shape_phenotypes |= (uint16_t)(1u << AFFORDANCE_PHENOTYPES[i].phenotype);
        }
        sqlite3_reset(lst); sqlite3_clear_bindings(lst);

        sqlite3_bind_text(lst, 1, line, (int)len, SQLITE_TRANSIENT);
        sqlite3_bind_text(lst, 2, "$.projection.estate_roles", -1, SQLITE_STATIC);
        while (sqlite3_step(lst) == SQLITE_ROW) {
            const char *v = (const char *)sqlite3_column_text(lst, 0);
            if (!v) continue;
            for (int i = 0; i < ROLE_PHENOTYPE_COUNT; i++)
                if (!strcmp(v, ROLE_PHENOTYPES[i].evidence))
                    row.shape_phenotypes |= (uint16_t)(1u << ROLE_PHENOTYPES[i].phenotype);
        }
        sqlite3_reset(lst); sqlite3_clear_bindings(lst);

        /* Topic domains only. Carrier defaults and cross-domain reuse are held
         * apart in the corpus for exactly this reason: a generic MCP server is
         * useful to a weather compiler without being weather material. */
        sqlite3_bind_text(lst, 1, line, (int)len, SQLITE_TRANSIENT);
        sqlite3_bind_text(lst, 2, "$.projection.topic_domains", -1, SQLITE_STATIC);
        while (sqlite3_step(lst) == SQLITE_ROW) {
            const char *v = (const char *)sqlite3_column_text(lst, 0);
            if (!v) continue;
            for (int i = 0; i < DOMAIN_PHENOTYPE_COUNT; i++)
                if (!strcmp(v, DOMAIN_PHENOTYPES[i].evidence))
                    row.domain_phenotypes |= (uint16_t)(1u << DOMAIN_PHENOTYPES[i].phenotype);
            {   /* Kept verbatim so the estate can be reported by domain
                 * without a second pass over the corpus. */
                size_t have = strlen(row.domains);
                size_t need = strlen(v) + 2;
                if (have + need < sizeof(row.domains))
                    snprintf(row.domains + have, sizeof(row.domains) - have, "%s%s",
                             have ? "," : "", v);
            }
        }
        sqlite3_reset(lst); sqlite3_clear_bindings(lst);

        /* A source that bears position is a spatial resource; that is what the
         * classification means and it is the only place it is read here. */
        if (row.spatial_bearing) row.shape_phenotypes |= (uint16_t)(1u << P_SPATIAL_RESOURCE);
        row.phenotypes = (uint16_t)(row.shape_phenotypes | row.domain_phenotypes);
        row.meaningful = phenotype_relations(row.phenotypes);

        if (used == n) {
            size_t grown = n ? n * 2 : 1024;
            struct Occurrence *bigger = realloc(rows, grown * sizeof(*bigger));
            if (!bigger) { free(rows); rows = NULL; goto done; }
            rows = bigger; n = grown;
        }
        rows[used++] = row;
    }
    *out = rows; *count = used; rows = NULL;
done:
    free(line);
    if (st) sqlite3_finalize(st);
    if (lst) sqlite3_finalize(lst);
    sqlite3_close(db);
    fclose(fh);
    if (rows) free(rows);
    return *out != NULL;
}

/* -------------------------------------------------------- braid input ----- */

static void print_phenotypes(uint16_t mask) {
    int first = 1;
    putchar('[');
    for (int p = 0; p < PHENOTYPE_COUNT; p++)
        if (mask & (1u << p)) { if (!first) putchar(','); first = 0; json_text(PHENOTYPES[p]); }
    putchar(']');
}

static void print_relations(uint32_t mask) {
    int first = 1;
    putchar('[');
    for (int r = 0; r < RELATION_COUNT; r++)
        if (mask & BIT(r)) { if (!first) putchar(','); first = 0; json_text(RELATIONS[r]); }
    putchar(']');
}

/* Absent fields are printed absent, not empty. No actor context means no
 * actor-scoped facet may be compiled, rather than one compiled for a generic
 * actor who does not exist. */
static int cmd_braid_input(const struct Occurrence *rows, size_t count, const char *want) {
    for (size_t i = 0; i < count; i++) {
        if (want && strcmp(rows[i].id, want)) continue;
        printf("{\"schema\":\"bonfyre-occurrence-braid-input.v1\",\"occurrence_id\":");
        json_text(rows[i].id);
        printf(",\"subjects\":[],\"owner_receipts\":[],\"domain_profiles\":[]");
        printf(",\"phenotypes\":"); print_phenotypes(rows[i].phenotypes);
        printf(",\"phenotypes_from_shape\":"); print_phenotypes(rows[i].shape_phenotypes);
        printf(",\"phenotypes_from_domain\":"); print_phenotypes(rows[i].domain_phenotypes);
        printf(",\"exact_owner_refs\":[],\"representation_passports\":[]");
        printf(",\"observed_capabilities\":"); print_relations(rows[i].offers);
        printf(",\"protocol_descriptors\":[]");
        printf(",\"absent\":[\"valid_frontier\",\"knowledge_frontier\",\"rights_profile\","
               "\"information_policy\",\"world_demand\",\"actor_context\",\"authority_cut\"]");
        printf(",\"truth\":\"phenotypes_derived_from_witnessed_corpus_evidence_not_a_second_classification\"}\n");
        if (want) return 0;
    }
    if (want) { fprintf(stderr, "no such occurrence: %s\n", want); return 1; }
    return 0;
}

/* ------------------------------------------- relation-shaped contracts ---- */

/* A contract's shape follows its relation.
 *
 * Describer/compiler/reader is lawful for a read and wrong for everything that
 * changes something: COMMAND, TASK, COMMIT, PAY and SETTLE need a role that
 * bears authority, and a contract that omits it can be enacted by anyone who
 * can reach it. The whole point of a finite relation vocabulary is that the
 * shape is derivable, so it is derived here rather than fixed at three.
 *
 * `authority` on a role is a requirement the AuthorityGraph must satisfy at
 * enactment, not a grant. A role naming an authority it does not hold is
 * refused by bf_protocol_contract_authorize, which is why the refusal path is
 * witnessed alongside the success.
 */
struct RelationShape {
    int bears_authority;     /* the acting role must hold a permission */
    int opens_commitment;    /* something is owed once this runs */
    const char *actor_role;  /* the role that does the thing */
    const char *permission;  /* the permission that role must hold */
    const char *owed;        /* what is owed, when anything is */
};

static struct RelationShape relation_shape(int relation) {
    struct RelationShape shape = {0, 0, "reader", "", ""};
    switch (relation) {
        /* Reads. Nothing is owed and nothing is authorized beyond the read
         * itself, which InformationPolicy governs and this does not. */
        case R_DISCOVER: case R_DESCRIBE: case R_QUERY: case R_FILTER:
        case R_OBSERVE: case R_IDENTIFY: case R_SYNCHRONIZE:
            shape.actor_role = "reader";
            break;
        case R_AUTHENTICATE:
            shape.actor_role = "authenticator";
            break;
        case R_SUBSCRIBE: case R_NOTIFY: case R_STREAM:
            shape.actor_role = "subscriber";
            break;
        case R_PROVE: case R_VERIFY: case R_ROUND_TRIP:
            shape.actor_role = "verifier";
            break;
        /* Publication moves bytes outward under a rights profile, so the
         * publisher holds a permission even though nothing is owed after. */
        case R_PUBLISH: case R_FEDERATE: case R_TRANSFER:
            shape.bears_authority = 1;
            shape.actor_role = "publisher";
            shape.permission = "publish:projection";
            break;
        /* Negotiation and quoting bind nobody yet. */
        case R_NEGOTIATE: case R_QUOTE:
            shape.actor_role = "negotiator";
            break;
        /* Everything that changes the world outside this estate. */
        case R_AUTHORIZE:
            shape.bears_authority = 1;
            shape.actor_role = "authorizer";
            shape.permission = "authorize:capability";
            break;
        case R_TASK:
            shape.bears_authority = 1; shape.opens_commitment = 1;
            shape.actor_role = "delegator";
            shape.permission = "task:capability";
            shape.owed = "the delegated task";
            break;
        case R_COMMAND:
            shape.bears_authority = 1; shape.opens_commitment = 1;
            shape.actor_role = "commander";
            shape.permission = "command:resource";
            shape.owed = "the commanded change";
            break;
        case R_COMMIT:
            shape.bears_authority = 1; shape.opens_commitment = 1;
            shape.actor_role = "committer";
            shape.permission = "commit:offer";
            shape.owed = "the committed exchange";
            break;
        case R_PAY:
            shape.bears_authority = 1; shape.opens_commitment = 1;
            shape.actor_role = "payer";
            shape.permission = "pay:amount";
            shape.owed = "the quoted amount";
            break;
        case R_SETTLE:
            shape.bears_authority = 1; shape.opens_commitment = 1;
            shape.actor_role = "settler";
            shape.permission = "settle:obligation";
            shape.owed = "the settled position";
            break;
        default:
            shape.actor_role = "reader";
            break;
    }
    return shape;
}

/* Representations a phenotype is actually carried in. A facet that names a
 * relation and a carrier and no representation is still an incomplete answer:
 * the same field is a different cost as Zarr than as GeoTIFF. */
static const char *const PHENOTYPE_REPRESENTATIONS[PHENOTYPE_COUNT] = {
    /* VERSIONED_RECORD     */ "JSON-LD,XML,Parquet",
    /* EVENT                */ "JSON,JSON-LD,Atom XML,Avro",
    /* LIVE_ENTITY          */ "Protocol Buffers,JSON,Arrow",
    /* MEASUREMENT_STREAM   */ "Arrow,Parquet,miniSEED,NetCDF",
    /* FIELD_COVERAGE       */ "Zarr,GeoParquet,GeoTIFF,NetCDF",
    /* DATASET              */ "Parquet,Arrow,CSV,RO-Crate",
    /* MEDIA_ASSET          */ "HLS,DASH,JPEG,AVIF,WebM",
    /* COMMANDABLE_RESOURCE */ "JSON,CBOR,OPC UA Binary",
    /* ECONOMIC_OFFER       */ "JSON,ISO 20022 XML",
    /* SUPPLY_CHAIN_OBJECT  */ "EPCIS JSON-LD,GS1 XML",
    /* IDENTITY_CREDENTIAL  */ "JWT,SD-JWT,Verifiable Credential JSON-LD",
    /* SERVICE_CAPABILITY   */ "OpenAPI JSON,JSON-RPC",
    /* DOCUMENT             */ "PDF,XML,RO-Crate,IIIF JSON",
    /* SPATIAL_RESOURCE     */ "GeoJSON,GeoParquet,WKB",
    /* OBLIGATION           */ "UBL XML,JSON",
};

/* ----------------------------------------------------------- compilation -- */

/* A facet is (phenotype the occurrence carries) x (relation a protocol offers)
 * where the relation is meaningful for that phenotype. All three conditions
 * matter: drop the phenotype and this is domain matching; drop the protocol's
 * own offer and this invents capability; drop the meaningfulness table and a
 * payment protocol acquires a relation over a weather field. */
static int compile_facets(const struct Occurrence *rows, size_t count,
                          const char *want, int emit, long long *total) {
    long long facets = 0;
    int first = 1;
    const struct Occurrence *subject = NULL;

    /* Resolved before a byte is written. Opening the document first meant a
     * failed lookup emitted a well-formed but empty facet set, which a reader
     * cannot tell from an occurrence that genuinely has no facets. */
    for (size_t i = 0; i < count; i++)
        if (!strcmp(rows[i].id, want)) { subject = &rows[i]; break; }
    if (!subject) {
        fprintf(stderr, "no such occurrence: %s\n", want);
        return 1;
    }
    if (emit) {
        printf("{\"schema\":\"bonfyre-protocol-facet-set.v1\",\"occurrence_id\":");
        json_text(want);
        printf(",\"facets\":[");
    }

    for (size_t i = 0; i < count; i++) {
        const struct Occurrence *proto = &rows[i];
        uint32_t shared;
        if (strcmp(proto->source_kind, "machine_protocol")) continue;
        if (!strcmp(proto->id, subject->id)) continue;
        shared = proto->offers & subject->meaningful;
        if (!shared) continue;
        for (int r = 0; r < RELATION_COUNT; r++) {
            int via = -1;
            if (!(shared & BIT(r))) continue;
            /* Which phenotype admitted this relation. Shape evidence is
             * preferred over domain evidence, and the promoter resolves it the
             * same way: two different answers to "which phenotype admitted
             * this" would make the derivation tier meaningless, and a facet
             * that cannot name its phenotype is a domain match wearing a
             * phenotype label. */
            for (int p = 0; p < PHENOTYPE_COUNT; p++)
                if ((subject->shape_phenotypes & (1u << p)) && (PHENOTYPE_RELATIONS[p] & BIT(r))) { via = p; break; }
            if (via < 0)
                for (int p = 0; p < PHENOTYPE_COUNT; p++)
                    if ((subject->phenotypes & (1u << p)) && (PHENOTYPE_RELATIONS[p] & BIT(r))) { via = p; break; }
            facets++;
            if (!emit) continue;
            if (!first) putchar(',');
            first = 0;
            printf("{\"protocol_family\":"); json_text(proto->name);
            printf(",\"protocol_occurrence\":"); json_text(proto->id);
            printf(",\"relation_kind\":"); json_text(RELATIONS[r]);
            printf(",\"from_owner\":\"ExternalEstate\",\"to_owner\":\"ProtocolContract\"");
            printf(",\"consumes\":["); json_text(subject->id); printf("]");
            printf(",\"produces\":[\"ProtocolFacetCandidate\"]");
            printf(",\"evidence_requirements\":[\"ForeignTwin:receipt\",\"EvidenceGraph:provenance\"]");
            printf(",\"semantic_requirements\":[\"DomainProfile:bound\"]");
            {   /* A relation that changes something names the permission its
                 * acting role must hold. Absent means the relation reads. */
                struct RelationShape shape = relation_shape(r);
                if (shape.bears_authority) {
                    printf(",\"authority_requirement\":");
                    json_text(shape.permission);
                }
                printf(",\"information_policy_requirement\":\"InformationPolicy:audience_cut\"");
            }
            printf(",\"via_phenotype\":");
            json_text(via >= 0 ? PHENOTYPES[via] : "");
            printf(",\"phenotype_derivation\":");
            json_text(via < 0 ? ""
                      : (subject->shape_phenotypes & (1u << via)) ? "shape" : "domain");
            printf(",\"representation_requirements\":");
            print_delimited(via >= 0 ? PHENOTYPE_REPRESENTATIONS[via] : NULL);
            printf(",\"carrier_candidates\":");
            print_delimited(via >= 0 ? PHENOTYPE_CARRIERS[via] : NULL);
            /* Precision is what makes density affordable: more projections must
             * not mean more recomputation. */
            printf(",\"invalidated_by\":");
            {
                char both[640];
                snprintf(both, sizeof(both), "corpus:regenerated,foreign_twin:new_digest,%s",
                         via >= 0 ? PHENOTYPE_INVALIDATION[via] : "");
                print_delimited(both);
            }
            /* The first rung and only the first rung. */
            printf(",\"maturity\":\"candidate_match\"");
            printf(",\"defers_to\":[\"ForeignTwin\",\"DomainProfile\",\"InformationPolicy\",\"AuthorityGraph\"]}");
        }
    }
    if (emit)
        printf("],\"facet_count\":%lld,\"truth\":\"candidate_match_only_not_a_compiled_contract_"
               "an_enactment_closed_obligations_or_an_authorized_effect\"}\n", facets);
    if (total) *total = facets;
    return 0;
}

/* ---------------------------------------------------------------- estate -- */

/* Full depth across the estate: every occurrence against every protocol
 * occurrence. Counts only -- emitting 4,891 facet sets would be a corpus, not a
 * report, and the per-occurrence sets are available individually. */
static int cmd_estate(const struct Occurrence *rows, size_t count) {
    long long with_phenotype = 0, protocols = 0, total_facets = 0, reachable = 0;
    long long phenotype_hist[PHENOTYPE_COUNT] = {0};
    long long shape_hist[PHENOTYPE_COUNT] = {0};
    long long relation_hist[RELATION_COUNT] = {0};
    long long facet_max = 0;
    char facet_max_id[64] = {0};

    for (size_t i = 0; i < count; i++) {
        if (!strcmp(rows[i].source_kind, "machine_protocol")) protocols++;
        if (rows[i].phenotypes) with_phenotype++;
        for (int p = 0; p < PHENOTYPE_COUNT; p++) {
            if (rows[i].phenotypes & (1u << p)) phenotype_hist[p]++;
            if (rows[i].shape_phenotypes & (1u << p)) shape_hist[p]++;
        }
    }

    for (size_t i = 0; i < count; i++) {
        long long facets = 0;
        if (!rows[i].meaningful) continue;
        for (size_t k = 0; k < count; k++) {
            uint32_t shared;
            if (strcmp(rows[k].source_kind, "machine_protocol")) continue;
            if (i == k) continue;
            shared = rows[k].offers & rows[i].meaningful;
            if (!shared) continue;
            for (int r = 0; r < RELATION_COUNT; r++)
                if (shared & BIT(r)) { facets++; relation_hist[r]++; }
        }
        total_facets += facets;
        if (facets) reachable++;
        if (facets > facet_max) { facet_max = facets; snprintf(facet_max_id, sizeof(facet_max_id), "%s", rows[i].id); }
    }

    printf("{\"schema\":\"bonfyre-facet-estate-report.v1\"");
    printf(",\"occurrences\":%zu,\"protocol_occurrences\":%lld", count, protocols);
    printf(",\"occurrences_with_a_phenotype\":%lld", with_phenotype);
    printf(",\"occurrences_with_at_least_one_facet\":%lld", reachable);
    printf(",\"candidate_facets\":%lld", total_facets);
    printf(",\"densest_occurrence\":"); json_text(facet_max_id);
    printf(",\"densest_occurrence_facets\":%lld", facet_max);
    printf(",\"phenotype_distribution\":{");
    for (int p = 0; p < PHENOTYPE_COUNT; p++) {
        if (p) putchar(',');
        json_text(PHENOTYPES[p]); printf(":%lld", phenotype_hist[p]);
    }
    printf("},\"phenotype_from_shape_alone\":{");
    for (int p = 0; p < PHENOTYPE_COUNT; p++) {
        if (p) putchar(',');
        json_text(PHENOTYPES[p]); printf(":%lld", shape_hist[p]);
    }
    printf("},\"relation_distribution\":{");
    for (int r = 0; r < RELATION_COUNT; r++) {
        if (r) putchar(',');
        json_text(RELATIONS[r]); printf(":%lld", relation_hist[r]);
    }
    printf("},\"truth\":\"candidate_adjacency_across_the_estate_not_realized_participation\"");
    printf(",\"cannot_infer\":[\"a candidate facet -> a compiled contract\","
           "\"a compiled contract -> an enactment\","
           "\"an enactment -> closed obligations\","
           "\"closed obligations -> an authorized effect\","
           "\"a protocol offering a relation -> that protocol being reachable, admitted or lawful here\"]}\n");
    return 0;
}

/* -------------------------------------------------- realized density ------ */

/* Density measured as shape, not as length.
 *
 * The longest owner path rewards pipelines and penalises exactly the shape this
 * estate is trying to grow: a straight line of eight owners scores higher than
 * six branches that rejoin on one occurrence, and the second is the braid. So
 * the path is reported beside branching, rejoining, and the excess of
 * transitions over a bare spanning tree -- which is zero for any pipeline of
 * any length.
 *
 * Every field of the declared structure is emitted, including the ones nothing
 * yet supplies. A zero that is present is a gap someone can see; a field
 * omitted because no evidence exists for it is a gap that looks like a design
 * decision. The zeros here are the honest map of what is not yet measured.
 */
#define MAX_OWNERS 64

struct DensityAccumulator {
    char owners[MAX_OWNERS][48];
    int owner_count;
    uint64_t edges[MAX_OWNERS];      /* adjacency as bitsets over owner index */
    uint64_t incoming[MAX_OWNERS];
    int transitions;
    int evidence_edges, rights_edges, authority_edges;
    int semantic_profiles, exact_owner_queries;
    int representations, carriers;
    char representation_ids[MAX_OWNERS][160];
    int representation_id_count;
    char carrier_families[MAX_OWNERS][160];
    int carrier_family_count;
    int contracts_compiled, contracts_enacted, obligations_closed;
    int human_decisions, machine_decisions;
    int commitments, effects, compensations;
    int roundtrips, faithful, drifted;
};

static int owner_index(struct DensityAccumulator *a, const char *name) {
    if (!name || !*name) return -1;
    for (int i = 0; i < a->owner_count; i++)
        if (!strcmp(a->owners[i], name)) return i;
    if (a->owner_count >= MAX_OWNERS) return -1;
    snprintf(a->owners[a->owner_count], sizeof(a->owners[0]), "%s", name);
    return a->owner_count++;
}

static int density_named_once(char values[MAX_OWNERS][160], int *count, const char *value) {
    if (!value || !*value || !count) return 0;
    for (int i = 0; i < *count; i++)
        if (!strcmp(values[i], value)) return 0;
    if (*count >= MAX_OWNERS) return 0;
    snprintf(values[*count], sizeof(values[0]), "%s", value);
    (*count)++;
    return 1;
}

static int longest_path_from(const struct DensityAccumulator *a, int at, uint64_t seen) {
    int best = 0;
    for (int n = 0; n < a->owner_count; n++) {
        int depth;
        if (!(a->edges[at] & (1ull << n))) continue;
        if (seen & (1ull << n)) continue;   /* a cycle is not extra depth */
        depth = 1 + longest_path_from(a, n, seen | (1ull << n));
        if (depth > best) best = depth;
    }
    return best;
}

static int cmd_density(const char *evidence_path, const char *want) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    FILE *fh = fopen(evidence_path, "rb");
    char *line = NULL;
    size_t cap = 0;
    ssize_t len;
    struct DensityAccumulator a;
    int rc = 1, branch = 0, rejoin = 0, longest = 0, rows = 0;

    memset(&a, 0, sizeof(a));
    if (!fh) { fprintf(stderr, "cannot read evidence %s\n", evidence_path); return 1; }
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) { fclose(fh); return 1; }
    if (sqlite3_prepare_v2(db,
        "SELECT json_extract(?1,'$.occurrence_id'), json_extract(?1,'$.twin_id'),"
        "       json_extract(?1,'$.from_owner'), json_extract(?1,'$.to_owner'),"
        "       json_extract(?1,'$.axis'), json_extract(?1,'$.state'),"
        "       json_extract(?1,'$.observation_kind'), json_extract(?1,'$.schema'),"
        "       json_extract(?1,'$.representation_id'), json_extract(?1,'$.carrier_family')", -1, &st, NULL) != SQLITE_OK)
        goto done;

    while ((len = getline(&line, &cap, fh)) > 0) {
        const char *oid, *twin, *from, *to, *axis, *state, *observation, *schema, *representation, *carrier;
        if (len && line[len-1] == '\n') line[--len] = '\0';
        if (!len) continue;
        sqlite3_bind_text(st, 1, line, (int)len, SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_reset(st); sqlite3_clear_bindings(st); continue; }
        oid   = (const char *)sqlite3_column_text(st, 0);
        twin  = (const char *)sqlite3_column_text(st, 1);
        from  = (const char *)sqlite3_column_text(st, 2);
        to    = (const char *)sqlite3_column_text(st, 3);
        axis  = (const char *)sqlite3_column_text(st, 4);
        state = (const char *)sqlite3_column_text(st, 5);
        observation = (const char *)sqlite3_column_text(st, 6);
        schema = (const char *)sqlite3_column_text(st, 7);
        representation = (const char *)sqlite3_column_text(st, 8);
        carrier = (const char *)sqlite3_column_text(st, 9);
        if (!oid) oid = twin;   /* ForeignTwin receipts key on twin_id */
        /* A ForeignTwin receipt is the ExternalEstate -> ForeignTwin handoff and
         * never says so: the observer writes what it saw, not who handed the
         * occurrence to it. The participation reader synthesises this edge and
         * this one did not, so the same evidence measured as a four-edge graph
         * in one reader and a five-edge graph in the other, and the estate's
         * first hop was missing from every density report. */
        if (observation && (!from || !to)) {
            from = "ExternalEstate";
            to = "ForeignTwin";
            if (!axis) axis = "observation";
            if (!state) state = observation;
        }
        if (want && (!oid || strcmp(oid, want))) { sqlite3_reset(st); sqlite3_clear_bindings(st); continue; }
        if (oid) {
            rows++;
            if (from && to && strcmp(from, to)) {
                int f = owner_index(&a, from), t = owner_index(&a, to);
                if (f >= 0 && f < MAX_OWNERS && t >= 0 && t < MAX_OWNERS &&
                    !(a.edges[f] & (1ull << t))) {
                    a.edges[f] |= 1ull << t;
                    a.incoming[t] |= 1ull << f;
                    a.transitions++;
                }
            }
            if (axis && !strcmp(axis, "semantic")) a.semantic_profiles++;
            if (axis && !strcmp(axis, "observation")) a.evidence_edges++;
            if (schema && !strcmp(schema, "bonfyre-exact-owner-query-evidence.v1") && state && !strcmp(state, "queried")) a.exact_owner_queries++;
            if (schema && !strcmp(schema, "bonfyre-representation-use-evidence.v1") && state && !strcmp(state, "used") && representation &&
                density_named_once(a.representation_ids, &a.representation_id_count, representation)) a.representations++;
            if (schema && !strcmp(schema, "bonfyre-carrier-use-evidence.v1") && state && !strcmp(state, "used") && carrier &&
                density_named_once(a.carrier_families, &a.carrier_family_count, carrier)) a.carriers++;
            if (state && !strcmp(state, "compiled")) a.contracts_compiled++;
            if (state && !strcmp(state, "enacted")) a.contracts_enacted++;
            if (state && !strcmp(state, "obligations_closed")) a.obligations_closed++;
            if (state && !strcmp(state, "closed_faithful")) { a.roundtrips++; a.faithful++; }
            if (state && !strcmp(state, "drifted")) { a.roundtrips++; a.drifted++; }
        }
        sqlite3_reset(st); sqlite3_clear_bindings(st);
    }

    for (int i = 0; i < a.owner_count; i++) {
        int out = 0, in = 0, depth;
        for (int n = 0; n < a.owner_count; n++) {
            if (a.edges[i] & (1ull << n)) out++;
            if (a.incoming[i] & (1ull << n)) in++;
        }
        if (out > 1) branch++;
        if (in > 1) rejoin++;
        depth = longest_path_from(&a, i, 1ull << i);
        if (depth > longest) longest = depth;
    }

    printf("{\"schema\":\"bonfyre-realized-braid-density.v1\"");
    if (want) { printf(",\"occurrence_id\":"); json_text(want); }
    printf(",\"evidence_rows\":%d", rows);
    printf(",\"distinct_owners\":%d", a.owner_count);
    printf(",\"owner_transition_count\":%d", a.transitions);
    printf(",\"longest_owner_path\":%d", longest);
    printf(",\"branch_points\":%d", branch);
    printf(",\"rejoin_points\":%d", rejoin);
    /* Transitions beyond a bare spanning tree: how much of the shape is braid
     * rather than chain. Zero for a pipeline of any length. */
    printf(",\"braid_excess\":%d", a.transitions - (a.owner_count ? a.owner_count - 1 : 0) > 0
                                   ? a.transitions - (a.owner_count - 1) : 0);
    printf(",\"protocol_contracts_compiled\":%d", a.contracts_compiled);
    printf(",\"protocol_contracts_enacted\":%d", a.contracts_enacted);
    printf(",\"obligations_closed\":%d", a.obligations_closed);
    printf(",\"semantic_profiles_used\":%d", a.semantic_profiles);
    printf(",\"exact_owner_queries\":%d", a.exact_owner_queries);
    printf(",\"representations_used\":%d", a.representations);
    printf(",\"carrier_families_used\":%d", a.carriers);
    printf(",\"evidence_edges\":%d", a.evidence_edges);
    printf(",\"rights_edges\":%d", a.rights_edges);
    printf(",\"authority_edges\":%d", a.authority_edges);
    printf(",\"invalidation_fanout\":%d", 0);
    printf(",\"recompute_cut_size\":%d", 0);
    printf(",\"human_decisions\":%d", a.human_decisions);
    printf(",\"machine_decisions\":%d", a.machine_decisions);
    printf(",\"commitments\":%d", a.commitments);
    printf(",\"effects\":%d", a.effects);
    printf(",\"compensations\":%d", a.compensations);
    printf(",\"roundtrips\":%d", a.roundtrips);
    printf(",\"faithful_roundtrips\":%d", a.faithful);
    printf(",\"drifted_roundtrips\":%d", a.drifted);
    printf(",\"truth\":\"realized_shape_over_supplied_occurrence_keyed_evidence\"");
    printf(",\"cannot_infer\":[\"a zero -> that transition being impossible; it is a "
           "transition nothing has yet left evidence for\","
           "\"longest_owner_path -> density; a chain of eight is not denser than six "
           "branches that rejoin\"]}\n");
    rc = 0;
done:
    free(line);
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    fclose(fh);
    return rc;
}

/* ----------------------------------------------------- the next rung ------ */

/* candidate_match -> contract_compiled.
 *
 * The facet compiler emits candidates and cannot witness anything above the
 * first rung. This promotes one candidate, and the promotion is only real
 * because three things are checked rather than asserted:
 *
 *   the facet exists      the occurrence's phenotype admits the relation and
 *                         the protocol offers it; a promotion for a facet the
 *                         compiler would not emit is refused
 *   the source was seen   a ForeignTwin receipt for this exact occurrence,
 *                         hashing to its own recorded digest; a contract over
 *                         a source nobody observed is a contract about nothing
 *   the owner registered  bf_protocol_derivation_register is the real native
 *                         ProtocolContract owner, and the program count in the
 *                         receipt is what it returned, not a number chosen here
 *
 * The digest chain is what makes the rung unforgeable downstream: the protocol
 * binding carries the digest of the domain binding, which carries the digest of
 * the ForeignTwin receipt. Remove any link and the chain stops verifying.
 *
 * Cannot infer: a compiled contract is not an enactment, not closed
 * obligations, and not an authorized effect. It says a lawful interaction shape
 * exists for this occurrence over this relation, and nothing has run.
 */
static void slug(const char *in, char *out, size_t cap) {
    size_t j = 0;
    int dash = 0;
    for (size_t i = 0; in && in[i] && j + 1 < cap; i++) {
        unsigned char c = (unsigned char)tolower((unsigned char)in[i]);
        if (isalnum(c)) { out[j++] = (char)c; dash = 0; }
        else if (!dash && j) { out[j++] = '-'; dash = 1; }
    }
    while (j && out[j-1] == '-') j--;
    out[j] = '\0';
}

/* A canonical JSON digest over sorted keys, so two producers of the same fact
 * agree on its digest. Written narrowly: the caller supplies already-sorted
 * key/value pairs because a general canonicaliser here would be a second
 * opinion about what canonical means. */
static void digest_pairs(const char *const *keys, const char *const *values,
                         size_t n, char out[65]) {
    char body[4096];
    size_t w = 0;
    w += (size_t)snprintf(body + w, sizeof(body) - w, "{");
    for (size_t i = 0; i < n && w < sizeof(body); i++)
        w += (size_t)snprintf(body + w, sizeof(body) - w, "%s\"%s\":\"%s\"",
                              i ? "," : "", keys[i], values[i]);
    if (w < sizeof(body)) snprintf(body + w, sizeof(body) - w, "}");
    bf_sha256_hex((const uint8_t *)body, strlen(body), out);
}

/* Both the promoter and the enactor need the same contract, built the same
 * way. Two constructions of one contract is two contracts. */
static int build_contract(BfProtocolContract *contract, const struct Occurrence *subject,
                          const char *proto_name, int relation_index,
                          const char *occurrence, char *name_out, size_t name_cap) {
    char produced[96], read_back[96], proto_slug[128], relation_slug[48];
    const char *describer_requires[] = {"domain_profile", "source_url"};
    const char *describer_produces[1];
    const char *actor_requires[1];
    const char *actor_produces[1];
    const char *reader_requires[1];
    BfProtocolRoleSpec describer = {0}, actor_role = {0}, reader = {0};
    struct RelationShape shape = relation_shape(relation_index);
    (void)subject;

    slug(proto_name, proto_slug, sizeof(proto_slug));
    slug(RELATIONS[relation_index], relation_slug, sizeof(relation_slug));
    snprintf(name_out, name_cap, "%s-%s", proto_slug, relation_slug);
    snprintf(produced, sizeof(produced), "%s_descriptor", relation_slug);
    snprintf(read_back, sizeof(read_back), "%s_projection", relation_slug);
    describer_produces[0] = produced;
    actor_requires[0] = produced;
    actor_produces[0] = read_back;
    reader_requires[0] = read_back;

    describer.name = "describer"; describer.authority = "";
    describer.requires = describer_requires; describer.requires_count = 2;
    describer.produces = describer_produces; describer.produces_count = 1;
    actor_role.name = shape.actor_role;
    actor_role.authority = shape.bears_authority ? shape.permission : "";
    actor_role.requires = actor_requires; actor_role.requires_count = 1;
    actor_role.produces = actor_produces; actor_role.produces_count = 1;
    reader.name = "witness"; reader.authority = "";
    reader.requires = reader_requires; reader.requires_count = 1;

    bf_protocol_contract_init(contract, name_out);
    if (bf_protocol_contract_given(contract, "domain_profile") != BF_PROTOCOL_OK ||
        bf_protocol_contract_given(contract, "source_url") != BF_PROTOCOL_OK ||
        bf_protocol_contract_role(contract, &describer) != BF_PROTOCOL_OK ||
        bf_protocol_contract_role(contract, &actor_role) != BF_PROTOCOL_OK ||
        bf_protocol_contract_role(contract, &reader) != BF_PROTOCOL_OK) return 0;
    if (shape.opens_commitment) {
        BfProtocolCommitmentSpec owed = {0};
        owed.key = relation_slug;
        owed.committer = shape.actor_role;
        owed.beneficiary = "witness";
        owed.resource = shape.owed;
        owed.discharged_by = read_back;
        if (bf_protocol_contract_commit(contract, &owed) != BF_PROTOCOL_OK) return 0;
    }
    {
        char claim[256];
        snprintf(claim, sizeof(claim), "claim:%s:%s", relation_slug, occurrence);
        if (bf_protocol_contract_evidence_required(contract, claim) != BF_PROTOCOL_OK) return 0;
    }
    return 1;
}

/* ------------------------------------------------------- enact and close -- */

/* The three rungs above contract_compiled, each produced by the owner that
 * owns it rather than declared here.
 *
 *   effect_authorized   bf_protocol_contract_authorize says every
 *                       authority-bearing role holds its permission over this
 *                       subject. Without a grant it refuses and names the role
 *                       that lacks it, which is the path worth witnessing.
 *   enacted             bf_protocol_contract_enact_authorized opened the
 *                       obligations the relation creates.
 *   obligations_closed  bf_protocol_contract_close reports obligations
 *                       terminal AND required evidence standing. Both axes, and
 *                       it is reported unclosed when either is missing rather
 *                       than rounded up.
 *
 * Granting authority is an institutional act and this will not do it silently:
 * --grant-authority writes an AuthorityGraph grant and says so in the output.
 */
static int cmd_enact(const struct Occurrence *rows, size_t count, int argc, char **argv) {
    const char *want = arg(argc, argv, "--occurrence");
    const char *proto_id = arg(argc, argv, "--protocol");
    const char *relation = arg(argc, argv, "--relation");
    const char *db_path = arg(argc, argv, "--db");
    const char *actor = arg(argc, argv, "--actor");
    const char *subject_ref = arg(argc, argv, "--subject");
    const char *at = arg(argc, argv, "--at");
    const char *out_path = arg(argc, argv, "--out");
    int grant = has_flag(argc, argv, "--grant-authority");
    int evidence_stands = has_flag(argc, argv, "--evidence-stands");
    int discharge = has_flag(argc, argv, "--discharge");
    const struct Occurrence *subject = NULL, *proto = NULL;
    int relation_index = -1;
    BfProtocolContract contract;
    BfProtocolActor actors[3];
    BfProtocolAuthorityRefusal refusal;
    BfProtocolEnactment enactment;
    BfProtocolClosure closure;
    struct RelationShape shape;
    char contract_name[192], enactment_id[256];
    sqlite3 *db = NULL;
    int rc = 1;
    FILE *fh = NULL;

    if (!want || !proto_id || !relation || !db_path || !actor || !subject_ref || !at) {
        fprintf(stderr, "enact requires --occurrence --protocol --relation --db "
                        "--actor --subject --at\n");
        return 2;
    }
    for (int r = 0; r < RELATION_COUNT; r++)
        if (!strcmp(relation, RELATIONS[r])) relation_index = r;
    if (relation_index < 0) {
        fprintf(stderr, "--relation is not a declared relation kind\n");
        return 2;
    }
    for (size_t i = 0; i < count; i++) {
        if (!strcmp(rows[i].id, want)) subject = &rows[i];
        if (!strcmp(rows[i].id, proto_id)) proto = &rows[i];
    }
    if (!subject || !proto) { fprintf(stderr, "no such occurrence or protocol\n"); return 1; }
    if (!(proto->offers & BIT(relation_index)) || !(subject->meaningful & BIT(relation_index))) {
        fprintf(stderr, "that facet does not exist; nothing to enact\n");
        return 4;
    }
    shape = relation_shape(relation_index);
    if (!build_contract(&contract, subject, proto->name, relation_index, want,
                        contract_name, sizeof(contract_name))) {
        fprintf(stderr, "the contract shape was refused by its own owner\n");
        return 1;
    }
    snprintf(enactment_id, sizeof(enactment_id), "%s:%s", contract_name, want);

    actors[0].role = "describer";      actors[0].actor = actor;
    actors[1].role = shape.actor_role; actors[1].actor = actor;
    actors[2].role = "witness";        actors[2].actor = actor;

    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "cannot open %s\n", db_path);
        return 1;
    }

    /* Granting authority is an institutional act and this will not do it
     * silently. Without the flag the authorize step refuses and names the role
     * that lacks the permission, which is the path worth witnessing. */
    if (grant) {
        BfAuthority *authority = NULL;
        if (bf_authority_open_database(db, &authority, stderr) != 0) goto done;
        for (size_t index = 0; index < contract.role_count; index++) {
            BfAuthorityEdge edge = {0};
            if (!contract.roles[index].authority[0]) continue;
            edge.edge_id = contract.roles[index].name;
            edge.actor = actor;
            edge.permission = contract.roles[index].authority;
            edge.subject = subject_ref;
            if (bf_authority_grant(authority, &edge, NULL) != 0) {
                bf_authority_close(authority);
                fprintf(stderr, "the AuthorityGraph refused the grant\n");
                goto done;
            }
        }
        bf_authority_close(authority);
    }

    memset(&refusal, 0, sizeof(refusal));
    if (bf_protocol_contract_authorize(&contract, db, actors, 3, subject_ref, at,
                                       NULL, &refusal) != BF_PROTOCOL_OK) {
        printf("{\"contract\":\"%s\",\"occurrence_id\":\"%s\",\"state\":\"refused\","
               "\"refused_role\":\"%s\",\"missing_permission\":\"%s\","
               "\"truth\":\"authority_is_not_granted_by_asking_for_it\"}\n",
               contract_name, want, refusal.role, refusal.permission);
        rc = 5;
        goto done;
    }

    memset(&enactment, 0, sizeof(enactment));
    if (bf_protocol_contract_enact_authorized(&contract, db, db, enactment_id, actors, 3,
                                              subject_ref, at, NULL, at,
                                              &enactment) != BF_PROTOCOL_OK) {
        fprintf(stderr, "the ProtocolContract owner refused the enactment\n");
        goto done;
    }

    /* Evidence is a separate axis from obligations. A contract requiring a
     * claim nobody has evidenced closes on neither. */
    if (evidence_stands) {
        BfEvidence *evidence = NULL;
        char claim[256], receipt[256], relation_slug[48];
        slug(relation, relation_slug, sizeof(relation_slug));
        snprintf(claim, sizeof(claim), "claim:%s:%s", relation_slug, want);
        snprintf(receipt, sizeof(receipt), "receipt:%s", enactment_id);
        if (bf_evidence_open_database(db, &evidence, stderr) != 0) goto done;
        if (bf_evidence_relate(evidence, receipt, "supports", claim) != BF_EVIDENCE_OK) {
            bf_evidence_close(evidence);
            fprintf(stderr, "the EvidenceGraph refused the relation\n");
            goto done;
        }
        bf_evidence_close(evidence);
    }

    /* An obligation closes when it is discharged, and discharging is an act
     * someone performs. bf_commitment.h says it outright -- recording is never
     * delivery -- so enactment does not imply it and this flag is the only way
     * to reach obligations_closed. */
    if (discharge && enactment.opened_count) {
        BfCommitment *commitments = NULL;
        if (bf_commitment_open_database(db, &commitments, stderr) != 0) goto done;
        for (size_t index = 0; index < enactment.opened_count; index++) {
            if (bf_commitment_discharge(commitments, enactment.commitment_ids[index],
                                        actor, "discharged by the enacting actor",
                                        at) != BF_COMMITMENT_OK) {
                bf_commitment_close(commitments);
                fprintf(stderr, "the CommitmentGraph refused the discharge of %s\n",
                        enactment.commitment_ids[index]);
                goto done;
            }
        }
        bf_commitment_close(commitments);
    }

    memset(&closure, 0, sizeof(closure));
    if (bf_protocol_contract_close(&contract, db, db, enactment_id, &closure) != BF_PROTOCOL_OK) {
        fprintf(stderr, "the ProtocolContract owner could not close the exchange\n");
        goto done;
    }

    /* Evidence rows go to a file or nowhere. Emitting them on stdout beside
     * the summary put four JSON documents on one stream with nothing to tell
     * them apart, which is a stream no reader can parse. */
    fh = out_path ? fopen(out_path, "a") : NULL;
    if (out_path && !fh) { fprintf(stderr, "cannot append to %s\n", out_path); goto done; }
    if (fh && shape.bears_authority)
        fprintf(fh,
            "{\"schema\":\"bonfyre-protocol-enactment.v1\",\"occurrence_id\":\"%s\","
            "\"axis\":\"capability\",\"owner\":\"AuthorityGraph\",\"state\":\"authorized\","
            "\"from_owner\":\"ProtocolContract\",\"to_owner\":\"AuthorityGraph\","
            "\"contract\":\"%s\",\"subject\":\"%s\",\"actor\":\"%s\","
            "\"cannot_infer\":[\"an authorized role -> a performed effect\","
            "\"a grant -> the actor being the right actor\"]}\n",
            want, contract_name, subject_ref, actor);
    /* An enactment row is evidence and is held to the same bar as any other:
     * the digest of what this owner read and what it produced. Without them the
     * participation reader refuses the row, which is the correct outcome for a
     * document that merely asserts a state. */
    {
        char basis[65], evidence[65], scratch[1024];
        int dn = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f%s", want, contract_name, subject_ref);
        if (dn < 0) dn = 0;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)dn, basis);
        dn = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f" "enacted", enactment_id, at);
        if (dn < 0) dn = 0;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)dn, evidence);
        /* The evidence relation is a real hop and was never reported as one.
         * The closure below checks evidence_closed, so ProtocolContract hands to
         * EvidenceGraph and CommitmentGraph consumes what EvidenceGraph settled.
         * This is emitted here rather than beside the relate() call because the
         * output file is not open yet at that point -- written there it went to
         * a NULL handle, the hop vanished, and pointing CommitmentGraph at an
         * EvidenceGraph with no in-edge severed the chain instead of extending
         * it: the measured path got shorter, not longer. */
        if (fh && evidence_stands) {
            char ebasis[65], eevidence[65], escratch[1024], eclaim[256], erelation[48];
            int en;
            slug(relation, erelation, sizeof(erelation));
            snprintf(eclaim, sizeof(eclaim), "claim:%s:%s", erelation, want);
            en = snprintf(escratch, sizeof(escratch), "%s\x1f%s\x1f%s", want, contract_name, eclaim);
            if (en < 0) en = 0;
            bf_sha256_hex((const uint8_t *)escratch, (size_t)en, ebasis);
            en = snprintf(escratch, sizeof(escratch), "%s\x1f%s\x1f" "stands", enactment_id, at);
            if (en < 0) en = 0;
            bf_sha256_hex((const uint8_t *)escratch, (size_t)en, eevidence);
            fprintf(fh,
                "{\"schema\":\"bonfyre-protocol-enactment.v1\",\"occurrence_id\":\"%s\","
                "\"axis\":\"protocol\",\"owner\":\"EvidenceGraph\",\"state\":\"enacted\","
                "\"from_owner\":\"ProtocolContract\",\"to_owner\":\"EvidenceGraph\","
                "\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\",\"produced_by\":\"bonfyre-facet\","
                "\"contract\":\"%s\",\"claim\":\"%s\","
                "\"cannot_infer\":[\"a standing claim -> a true one\","
                "\"evidence relating a receipt to a claim -> the claim being adjudicated\"]}\n",
                want, ebasis, eevidence, contract_name, eclaim);
        }
        if (fh) fprintf(fh,
            "{\"schema\":\"bonfyre-protocol-enactment.v1\",\"occurrence_id\":\"%s\","
            "\"axis\":\"protocol\",\"owner\":\"ProtocolContract\",\"state\":\"enacted\","
            "\"from_owner\":\"AuthorityGraph\",\"to_owner\":\"ProtocolContract\","
            "\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\",\"produced_by\":\"bonfyre-facet\","
            "\"contract\":\"%s\",\"enactment_id\":\"%s\",\"obligations_opened\":%zu,"
            "\"cannot_infer\":[\"an enactment -> a discharged obligation\","
            "\"an enactment -> an effect outside this estate\"]}\n",
            want, basis, evidence, contract_name, enactment_id, enactment.opened_count);
    }
    {
        char cbasis[65], cevidence[65], cscratch[1024];
        int cn = snprintf(cscratch, sizeof(cscratch), "%s\x1f%s\x1f%s", want, contract_name, enactment_id);
        if (cn < 0) cn = 0;
        bf_sha256_hex((const uint8_t *)cscratch, (size_t)cn, cbasis);
        cn = snprintf(cscratch, sizeof(cscratch), "%s\x1f%s\x1f" "closed", enactment_id, at);
        if (cn < 0) cn = 0;
        bf_sha256_hex((const uint8_t *)cscratch, (size_t)cn, cevidence);
    if (fh) fprintf(fh,
        "{\"schema\":\"bonfyre-protocol-enactment.v1\",\"occurrence_id\":\"%s\","
        "\"axis\":\"protocol\",\"owner\":\"CommitmentGraph\",\"state\":\"%s\","
        "\"from_owner\":\"%s\",\"to_owner\":\"CommitmentGraph\","
        "\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\",\"produced_by\":\"bonfyre-facet\","
        "\"contract\":\"%s\",\"enactment_id\":\"%s\","
        "\"obligations_terminal\":%s,\"evidence_closed\":%s,\"outstanding\":%zu,"
        "\"unevidenced_claims\":%zu,"
        "\"cannot_infer\":[\"closed obligations -> a correct outcome\","
        "\"terminal obligations -> standing evidence; they are separate axes\"]}\n",
        want,
        (closure.obligations_terminal && closure.evidence_closed) ? "obligations_closed" : "outstanding",
        evidence_stands ? "EvidenceGraph" : "ProtocolContract",
        cbasis, cevidence, contract_name, enactment_id,
        closure.obligations_terminal ? "true" : "false",
        closure.evidence_closed ? "true" : "false",
        closure.outstanding.opened_count, closure.unevidenced_count);
    }
    if (fh) fclose(fh);

    printf("{\"contract\":\"%s\",\"occurrence_id\":\"%s\",\"relation_kind\":\"%s\","
           "\"authority_required\":%s,\"authorized\":true,\"obligations_opened\":%zu,"
           "\"obligations_terminal\":%s,\"evidence_closed\":%s,\"state\":\"%s\","
           "\"enacted_by\":\"bf_protocol_contract_enact_authorized\","
           "\"closed_by\":\"bf_protocol_contract_close\","
           "\"truth\":\"enacted_by_its_owner_not_an_effect_outside_this_estate\"}\n",
           contract_name, want, relation, shape.bears_authority ? "true" : "false",
           enactment.opened_count,
           closure.obligations_terminal ? "true" : "false",
           closure.evidence_closed ? "true" : "false",
           (closure.obligations_terminal && closure.evidence_closed) ? "obligations_closed" : "outstanding");
    rc = 0;
done:
    sqlite3_close(db);
    return rc;
}

static int cmd_contract(const struct Occurrence *rows, size_t count, int argc, char **argv) {
    const char *want = arg(argc, argv, "--occurrence");
    const char *proto_id = arg(argc, argv, "--protocol");
    const char *relation = arg(argc, argv, "--relation");
    const char *receipt_path = arg(argc, argv, "--receipt");
    const char *out_path = arg(argc, argv, "--out");
    const struct Occurrence *subject = NULL, *proto = NULL;
    int relation_index = -1, via = -1;
    char receipt_digest[65] = {0};
    char contract_name[192], proto_slug[128], relation_slug[48];
    BfProtocolContract contract;
    BfSemanticDerivationGraph graph;
    size_t added = 0;
    FILE *fh = NULL;

    if (!want || !proto_id || !relation || !receipt_path) {
        fprintf(stderr, "contract requires --occurrence --protocol --relation --receipt\n");
        return 2;
    }
    for (int r = 0; r < RELATION_COUNT; r++)
        if (!strcmp(relation, RELATIONS[r])) relation_index = r;
    if (relation_index < 0) {
        fprintf(stderr, "--relation must be one of the declared relation kinds\n");
        return 2;
    }
    for (size_t i = 0; i < count; i++) {
        if (!strcmp(rows[i].id, want)) subject = &rows[i];
        if (!strcmp(rows[i].id, proto_id)) proto = &rows[i];
    }
    if (!subject || !proto) { fprintf(stderr, "no such occurrence or protocol\n"); return 1; }
    if (strcmp(proto->source_kind, "machine_protocol")) {
        fprintf(stderr, "%s is not a protocol occurrence\n", proto_id);
        return 1;
    }

    /* The facet must be one this compiler would emit. Promoting a facet it
     * would not is inventing a relation and calling it compiled. */
    if (!(proto->offers & BIT(relation_index))) {
        fprintf(stderr, "%s does not offer %s; refusing to compile a contract for a "
                        "facet the compiler would not emit\n", proto_id, relation);
        return 4;
    }
    if (!(subject->meaningful & BIT(relation_index))) {
        fprintf(stderr, "no phenotype of %s admits %s; refusing\n", want, relation);
        return 4;
    }
    /* Prefer a phenotype the thing itself evidenced. Only fall to domain
     * evidence when no shape evidence admits the relation. */
    for (int p = 0; p < PHENOTYPE_COUNT; p++)
        if ((subject->shape_phenotypes & (1u << p)) && (PHENOTYPE_RELATIONS[p] & BIT(relation_index))) { via = p; break; }
    if (via < 0)
        for (int p = 0; p < PHENOTYPE_COUNT; p++)
            if ((subject->phenotypes & (1u << p)) && (PHENOTYPE_RELATIONS[p] & BIT(relation_index))) { via = p; break; }

    /* Domain evidence is a neighbourhood and not an observation: a legal API in
     * the `law` domain is a service capability that happens to be about
     * documents. A candidate facet may rest on it; a compiled contract may not,
     * because a contract asserts a lawful interaction shape for this source
     * rather than for sources like it. The caller may override and the override
     * is recorded on the binding, so the weaker basis stays visible. */
    if (via >= 0 && !(subject->shape_phenotypes & (1u << via)) &&
        !has_flag(argc, argv, "--accept-domain-evidence")) {
        fprintf(stderr, "%s admits %s only through domain evidence (%s), which is a "
                        "neighbourhood and not an observation of this source. Bind a "
                        "DomainProfile, or pass --accept-domain-evidence to record the "
                        "weaker basis explicitly.\n",
                want, relation, PHENOTYPES[via]);
        return 6;
    }

    /* A contract over a source nobody observed is a contract about nothing. */
    {
        sqlite3 *mem = NULL;
        sqlite3_stmt *st = NULL;
        char *doc = NULL;
        long size = 0;
        char body_path[1024] = {0}, expected[96] = {0}, twin[256] = {0};
        FILE *rf = fopen(receipt_path, "rb");
        if (!rf) { fprintf(stderr, "no receipt at %s\n", receipt_path); return 1; }
        fseek(rf, 0, SEEK_END); size = ftell(rf); fseek(rf, 0, SEEK_SET);
        if (size < 0 || !(doc = malloc((size_t)size + 1))) { fclose(rf); return 1; }
        if (fread(doc, 1, (size_t)size, rf) != (size_t)size) { free(doc); fclose(rf); return 1; }
        doc[size] = '\0'; fclose(rf);
        if (sqlite3_open(":memory:", &mem) != SQLITE_OK) { free(doc); return 1; }
        if (sqlite3_prepare_v2(mem,
            "SELECT json_extract(?1,'$.body_path'), json_extract(?1,'$.content_sha256'),"
            "       json_extract(?1,'$.twin_id')", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, doc, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *a = (const char *)sqlite3_column_text(st, 0);
                const char *b = (const char *)sqlite3_column_text(st, 1);
                const char *c = (const char *)sqlite3_column_text(st, 2);
                if (a) snprintf(body_path, sizeof(body_path), "%s", a);
                if (b) snprintf(expected, sizeof(expected), "%s", b);
                if (c) snprintf(twin, sizeof(twin), "%s", c);
            }
            sqlite3_finalize(st);
        }
        sqlite3_close(mem);
        free(doc);
        if (strcmp(twin, want)) {
            fprintf(stderr, "receipt observes %s and not %s; a contract may not borrow "
                            "another occurrence's observation\n", twin[0] ? twin : "(nothing)", want);
            return 4;
        }
        if (!body_path[0] || !expected[0] || bf_sha256_file(body_path, receipt_digest) != 0) {
            fprintf(stderr, "receipt names no verifiable body\n");
            return 4;
        }
        if (strcmp(receipt_digest, expected)) {
            fprintf(stderr, "receipted body no longer hashes to its receipt; nothing is "
                            "compiled from bytes that have drifted\n");
            return 3;
        }
    }

    /* The contract shape differs per relation, which is what makes this a
     * grammar rather than one specimen wearing different names. */
    slug(proto->name, proto_slug, sizeof(proto_slug));
    slug(relation, relation_slug, sizeof(relation_slug));
    snprintf(contract_name, sizeof(contract_name), "%s-%s", proto_slug, relation_slug);

    {
        char produced[96], read_back[96];
        const char *describer_requires[] = {"domain_profile", "source_url"};
        const char *describer_produces[1];
        const char *actor_requires[1];
        const char *actor_produces[1];
        const char *reader_requires[1];
        BfProtocolRoleSpec describer = {0}, actor_role = {0}, reader = {0};
        struct RelationShape shape = relation_shape(relation_index);
        snprintf(produced, sizeof(produced), "%s_descriptor", relation_slug);
        snprintf(read_back, sizeof(read_back), "%s_projection", relation_slug);
        describer_produces[0] = produced;
        actor_requires[0] = produced;
        actor_produces[0] = read_back;
        reader_requires[0] = read_back;

        describer.name = "describer"; describer.authority = "";
        describer.requires = describer_requires; describer.requires_count = 2;
        describer.produces = describer_produces; describer.produces_count = 1;
        /* The acting role is named and authority-bearing for exactly the
         * relations that change something. A contract for COMMAND with no
         * authority-bearing role can be enacted by anyone who can reach it. */
        actor_role.name = shape.actor_role;
        actor_role.authority = shape.bears_authority ? shape.permission : "";
        actor_role.requires = actor_requires; actor_role.requires_count = 1;
        actor_role.produces = actor_produces; actor_role.produces_count = 1;
        reader.name = "witness"; reader.authority = "";
        reader.requires = reader_requires; reader.requires_count = 1;

        bf_protocol_contract_init(&contract, contract_name);
        if (bf_protocol_contract_given(&contract, "domain_profile") != BF_PROTOCOL_OK ||
            bf_protocol_contract_given(&contract, "source_url") != BF_PROTOCOL_OK ||
            bf_protocol_contract_role(&contract, &describer) != BF_PROTOCOL_OK ||
            bf_protocol_contract_role(&contract, &actor_role) != BF_PROTOCOL_OK ||
            bf_protocol_contract_role(&contract, &reader) != BF_PROTOCOL_OK) {
            fprintf(stderr, "the contract shape was refused by its own owner\n");
            return 1;
        }
        /* Something is owed only where the relation creates an obligation, so
         * close() has something real to discharge rather than nothing. */
        if (shape.opens_commitment) {
            BfProtocolCommitmentSpec owed = {0};
            owed.key = relation_slug;
            owed.committer = shape.actor_role;
            owed.beneficiary = "witness";
            owed.resource = shape.owed;
            owed.discharged_by = read_back;
            if (bf_protocol_contract_commit(&contract, &owed) != BF_PROTOCOL_OK) {
                fprintf(stderr, "the obligation this relation opens was refused\n");
                return 1;
            }
        }
        {   /* A contract that requires no evidence closes on obligations
             * alone, which is how an exchange is declared finished without
             * anything standing behind it. */
            char claim[256];
            snprintf(claim, sizeof(claim), "claim:%s:%s", relation_slug, want);
            if (bf_protocol_contract_evidence_required(&contract, claim) != BF_PROTOCOL_OK) {
                fprintf(stderr, "the evidence this contract requires was refused\n");
                return 1;
            }
        }
    }

    /* The real native owner registers it, and the program count below is what
     * it returned. */
    bf_semantic_derivation_init(&graph);
    if (bf_protocol_derivation_register(&contract, want, &graph, &added)
        != BF_PROTOCOL_DERIVATION_BRIDGE_OK) {
        fprintf(stderr, "the native ProtocolContract owner refused the registration\n");
        return 1;
    }

    {
        char registration[65], semantic[65], domain_binding[65], ingest[65], facet_basis[65], facet_evidence[65], stamp[32];
        char programs[16];
        time_t recorded = time(NULL);
        struct tm recorded_tm;
        const char *fkeys[] = {"occurrence_id", "protocol_occurrence", "relation_kind", "via_phenotype"};
        const char *fvals[4];
        const char *ekeys[] = {"contract", "programs", "native_registration_receipt_sha256", "state"};
        const char *evals[4];
        gmtime_r(&recorded, &recorded_tm); strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%SZ", &recorded_tm);
        const char *rkeys[] = {"contract", "occurrence_id", "programs", "registered"};
        const char *rvals[4];
        const char *skeys[] = {"occurrence_id", "profile", "source_receipt", "via_phenotype"};
        const char *svals[4];
        const char *dkeys[] = {"occurrence_id", "owner", "semantic_record", "state"};
        const char *dvals[4];
        snprintf(programs, sizeof(programs), "%zu", added);

        rvals[0] = contract_name; rvals[1] = want; rvals[2] = programs; rvals[3] = "true";
        digest_pairs(rkeys, rvals, 4, registration);

        svals[0] = want; svals[1] = via >= 0 ? PHENOTYPES[via] : "";
        svals[2] = receipt_digest; svals[3] = via >= 0 ? PHENOTYPES[via] : "";
        digest_pairs(skeys, svals, 4, semantic);

        dvals[0] = want; dvals[1] = "DomainProfile"; dvals[2] = semantic; dvals[3] = "profile_bound";
        digest_pairs(dkeys, dvals, 4, domain_binding);

        digest_pairs(skeys, svals, 4, ingest);

        fvals[0] = want; fvals[1] = proto_id; fvals[2] = relation;
        fvals[3] = via >= 0 ? PHENOTYPES[via] : "";
        digest_pairs(fkeys, fvals, 4, facet_basis);
        evals[0] = contract_name; evals[1] = programs; evals[2] = registration;
        evals[3] = "contract_bound";
        digest_pairs(ekeys, evals, 4, facet_evidence);

        /* Same rule as enact: bindings go to a file or nowhere, so the summary
         * on stdout is always one parseable document. */
        fh = out_path ? fopen(out_path, "a") : NULL;
        if (out_path && !fh) { fprintf(stderr, "cannot append evidence to %s\n", out_path); return 1; }
        if (!fh) fh = fopen("/dev/null", "w");
        if (!fh) return 1;

        /* The semantic rung first. A protocol binding without a domain binding
         * it hashes is a contract with nothing under it. */
        fprintf(fh,
            "{\"schema\":\"bonfyre-domain-binding.v1\",\"occurrence_id\":\"%s\""
            ",\"axis\":\"semantic\",\"owner\":\"DomainProfile\",\"state\":\"profile_bound\""
            ",\"from_owner\":\"ForeignTwin\",\"to_owner\":\"DomainProfile\""
            ",\"binding_validated\":true,\"profile\":\"%s\",\"parser\":\"facet-phenotype-v1\""
            ",\"foreign_receipt_sha256\":\"%s\",\"semantic_record_sha256\":\"%s\""
            ",\"ingest_manifest_sha256\":\"%s\""
            ",\"via_phenotype\":\"%s\",\"phenotype_derivation\":\"%s\""
            ",\"cannot_infer\":[\"profile binding -> external standard conformance\""
            ",\"phenotype -> an observation of the thing it names\""
            ",\"retained bytes -> provider admission or callable capability\"]}\n",
            want, via >= 0 ? PHENOTYPES[via] : "", receipt_digest, semantic, ingest,
            via >= 0 ? PHENOTYPES[via] : "",
            (via >= 0 && (subject->shape_phenotypes & (1u << via))) ? "shape" : "domain");

        /* This is the second real parent of ProtocolContract. The observed
         * DomainProfile above is the first; contract_bound remains below compiled. */
        fprintf(fh, "{\"schema\":\"bonfyre-owner-participation.v1\",\"occurrence_id\":\"%s\",\"axis\":\"protocol\",\"state\":\"contract_bound\",\"owner\":\"ProtocolContract\",\"from_owner\":\"ProtocolFacetCompiler\",\"to_owner\":\"ProtocolContract\",\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\",\"produced_by\":\"bonfyre-facet\",\"recorded_at\":\"%s\",\"cannot_infer\":[\"contract_bound -> compiled, enacted, authorized, or effected\"]}\n", want, facet_basis, facet_evidence, stamp);

        fprintf(fh,
            "{\"schema\":\"bonfyre-protocol-binding.v1\",\"occurrence_id\":\"%s\""
            ",\"axis\":\"protocol\",\"owner\":\"ProtocolContract\",\"state\":\"compiled\""
            ",\"from_owner\":\"DomainProfile\",\"to_owner\":\"ProtocolContract\""
            ",\"binding_validated\":true,\"contract\":\"%s\",\"programs\":%zu"
            ",\"relation_kind\":\"%s\",\"protocol_occurrence\":\"%s\""
            ",\"semantic_record_sha256\":\"%s\",\"domain_binding_sha256\":\"%s\""
            ",\"native_registration_receipt_sha256\":\"%s\""
            ",\"cannot_infer\":[\"compiled contract -> conformance to the named standard\""
            ",\"compiled contract -> an enacted request\""
            ",\"compiled contract -> provider admission, authority or effect\"]}\n",
            want, contract_name, added, relation, proto_id, semantic, domain_binding, registration);

        fclose(fh);
        printf("{\"compiled\":\"%s\",\"occurrence_id\":\"%s\",\"relation_kind\":\"%s\""
               ",\"programs\":%zu,\"registered_by\":\"bf_protocol_derivation_register\""
               ",\"truth\":\"contract_compiled_not_enacted_not_closed_not_authorized\"}\n",
               contract_name, want, relation, added);
    }
    return 0;
}

static int sha256_string(const char *s) {
    if (!s || strlen(s) != 64) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++)
        if (!isxdigit(*p)) return 0;
    return 1;
}

/* One-time reconciliation for historical compiled bindings emitted before the
 * facet-parent edge became explicit. It copies the supplied evidence and adds
 * only a contract_bound edge whose exact protocol/relation is still admitted
 * by the current source-shape facet. No network or enactment occurs. */
static int cmd_reconcile_contract_joins(const struct Occurrence *rows, size_t count,
                                        const char *evidence_path, const char *out_path) {
    FILE *in = NULL, *out = NULL; sqlite3 *mem = NULL; sqlite3_stmt *parse = NULL;
    char *line = NULL; size_t cap = 0; ssize_t len; int repaired = 0, compiled = 0, refused = 0, ok = 0;
    if (!evidence_path || !out_path || !strcmp(evidence_path, out_path)) return 0;
    if (!(in=fopen(evidence_path,"r")) || !(out=fopen(out_path,"w")) || sqlite3_open(":memory:",&mem)!=SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(mem,
        "SELECT json_extract(?1,'$.schema'),json_extract(?1,'$.occurrence_id'),json_extract(?1,'$.state'),"
        "json_extract(?1,'$.relation_kind'),json_extract(?1,'$.protocol_occurrence'),json_extract(?1,'$.binding_validated'),"
        "json_extract(?1,'$.domain_binding_sha256'),json_extract(?1,'$.native_registration_receipt_sha256')",
        -1,&parse,NULL)!=SQLITE_OK) goto done;
    while ((len=getline(&line,&cap,in))>0) {
        fputs(line,out);
        sqlite3_bind_text(parse,1,line,(int)len,SQLITE_TRANSIENT);
        if (sqlite3_step(parse)==SQLITE_ROW) {
            const char *schema=(const char*)sqlite3_column_text(parse,0), *oid=(const char*)sqlite3_column_text(parse,1),
                       *state=(const char*)sqlite3_column_text(parse,2), *relation=(const char*)sqlite3_column_text(parse,3),
                       *proto_id=(const char*)sqlite3_column_text(parse,4), *domain_digest=(const char*)sqlite3_column_text(parse,6),
                       *registration=(const char*)sqlite3_column_text(parse,7);
            int validated=sqlite3_column_int(parse,5);
            if (schema&&oid&&state&&relation&&proto_id&&!strcmp(schema,"bonfyre-protocol-binding.v1")&&!strcmp(state,"compiled")) {
                const struct Occurrence *subject=NULL,*proto=NULL; int ri=-1,via=-1;
                char basis[65],evidence[65],stamp[32]; const char *keys[]={"occurrence_id","protocol_occurrence","relation_kind","via_phenotype"},*vals[4];
                time_t now=time(NULL); struct tm tm; compiled++;
                for(size_t i=0;i<count;i++){if(!strcmp(rows[i].id,oid))subject=&rows[i];if(!strcmp(rows[i].id,proto_id))proto=&rows[i];}
                for(int r=0;r<RELATION_COUNT;r++)if(!strcmp(relation,RELATIONS[r]))ri=r;
                if(!validated||!subject||!proto||ri<0||strcmp(proto->source_kind,"machine_protocol")||
                   !(proto->offers&BIT(ri))||!(subject->meaningful&BIT(ri))||!sha256_string(domain_digest)||!sha256_string(registration)){refused++;goto row_done;}
                for(int p=0;p<PHENOTYPE_COUNT;p++)if((subject->shape_phenotypes&(1u<<p))&&(PHENOTYPE_RELATIONS[p]&BIT(ri))){via=p;break;}
                if(via<0){refused++;goto row_done;}
                vals[0]=oid;vals[1]=proto_id;vals[2]=relation;vals[3]=PHENOTYPES[via];digest_pairs(keys,vals,4,basis);
                bf_sha256_hex((const uint8_t*)line,(size_t)len,evidence);gmtime_r(&now,&tm);strftime(stamp,sizeof(stamp),"%Y-%m-%dT%H:%M:%SZ",&tm);
                fprintf(out,"{\"schema\":\"bonfyre-owner-participation.v1\",\"occurrence_id\":\"%s\",\"axis\":\"protocol\",\"state\":\"contract_bound\",\"owner\":\"ProtocolContract\",\"from_owner\":\"ProtocolFacetCompiler\",\"to_owner\":\"ProtocolContract\",\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\",\"produced_by\":\"bonfyre-facet:reconcile-contract-joins\",\"recorded_at\":\"%s\",\"cannot_infer\":[\"reconciled historical dependency -> new execution, enactment, authority, or effect\"]}\n",oid,basis,evidence,stamp);
                repaired++;
            }
        }
row_done:
        sqlite3_reset(parse);sqlite3_clear_bindings(parse);
    }
    if(fflush(out)||ferror(out))goto done;
    printf("{\"schema\":\"bonfyre-contract-join-reconciliation.v1\",\"compiled_rows\":%d,\"repaired\":%d,\"refused\":%d,\"truth\":\"historical_dependency_reconciled_not_reexecuted\"}\n",compiled,repaired,refused);ok=1;
done:
    free(line);if(parse)sqlite3_finalize(parse);if(mem)sqlite3_close(mem);if(in)fclose(in);if(out)fclose(out);return ok?0:1;
}

/* ------------------------------------------------------ by domain --------- */

/* Acceptance across the whole machine rather than one nucleus.
 *
 * A compiler demonstrated on one earthquake feed is a compiler demonstrated on
 * one earthquake feed. Picking a favourable specimen per domain is the same
 * mistake with more steps, so this reports every domain the corpus carries:
 * how many occurrences it holds, how many phenotypes those occurrences reach,
 * how many candidate facets they compile, and how much of the relation
 * vocabulary is exercised. A domain that compiles nothing is visible here, and
 * that is the point of reporting all of them.
 */
static int domain_in(const char *list, const char *want) {
    const char *p = list;
    size_t n = strlen(want);
    while (p && *p) {
        const char *end = strchr(p, ',');
        size_t len = end ? (size_t)(end - p) : strlen(p);
        if (len == n && !strncmp(p, want, n)) return 1;
        p = end ? end + 1 : NULL;
    }
    return 0;
}

static int cmd_domains(const struct Occurrence *rows, size_t count) {
    char names[128][48];
    int name_count = 0;
    int first_out = 1;

    /* The domain list is whatever the corpus used, not a list maintained here. */
    for (size_t i = 0; i < count; i++) {
        const char *p = rows[i].domains;
        while (p && *p) {
            const char *end = strchr(p, ',');
            size_t len = end ? (size_t)(end - p) : strlen(p);
            int seen = 0;
            if (len && len < sizeof(names[0])) {
                for (int k = 0; k < name_count; k++)
                    if (strlen(names[k]) == len && !strncmp(names[k], p, len)) { seen = 1; break; }
                if (!seen && name_count < 128) {
                    memcpy(names[name_count], p, len);
                    names[name_count][len] = '\0';
                    name_count++;
                }
            }
            p = end ? end + 1 : NULL;
        }
    }

    printf("{\"schema\":\"bonfyre-facet-domain-report.v1\",\"domains\":[");
    for (int d = 0; d < name_count; d++) {
        long long occurrences = 0, facets = 0;
        uint16_t phenotypes = 0;
        uint32_t relations = 0;
        for (size_t i = 0; i < count; i++) {
            if (!domain_in(rows[i].domains, names[d])) continue;
            occurrences++;
            phenotypes |= rows[i].phenotypes;
            if (!rows[i].meaningful) continue;
            for (size_t k = 0; k < count; k++) {
                uint32_t shared;
                if (strcmp(rows[k].source_kind, "machine_protocol") || i == k) continue;
                shared = rows[k].offers & rows[i].meaningful;
                if (!shared) continue;
                relations |= shared;
                for (int r = 0; r < RELATION_COUNT; r++) if (shared & BIT(r)) facets++;
            }
        }
        if (!first_out) putchar(',');
        first_out = 0;
        printf("{\"domain\":"); json_text(names[d]);
        printf(",\"occurrences\":%lld,\"candidate_facets\":%lld", occurrences, facets);
        printf(",\"phenotypes_reached\":%d", __builtin_popcount(phenotypes));
        printf(",\"relations_exercised\":%d", __builtin_popcount(relations));
        printf(",\"phenotypes\":"); print_phenotypes(phenotypes);
        putchar('}');
    }
    printf("],\"domain_count\":%d", name_count);
    printf(",\"truth\":\"candidate_adjacency_by_domain_not_realized_participation\"");
    printf(",\"cannot_infer\":[\"a domain compiling facets -> that domain being integrated\","
           "\"a domain reaching many phenotypes -> those phenotypes being observed in it\"]}\n");
    return 0;
}

/* -------------------------------------------------- estate interpretation -- */

/* Occurrence-keyed evidence for every occurrence this compiler interpreted.
 *
 * 4,881 of 4,891 sit at braid depth zero, and the reason is not that nothing
 * has happened to them -- the compiler read every one and produced typed
 * phenotypes and candidate facets for it. What has not happened is observation:
 * nobody has fetched a body or written a receipt. Those are different facts and
 * the report has separate axes for them, so the honest move is to record the
 * interpretation that did happen and leave the observation axis unknown.
 *
 * This is deliberately the weakest rung on the semantic axis. `interpreted`
 * means this estate read a description and typed it. It is not `profile_bound`,
 * which needs a DomainProfile, and it is emphatically not an observation of the
 * source.
 *
 * Cannot infer: an interpretation is not an observation, not a receipt, and not
 * evidence that the source is reachable. An occurrence at depth 1 by this route
 * has still never been looked at.
 */

/* ------------------------------------------------- owner participation -- */

/* How this compiler reports that it participated in one occurrence.
 *
 * The participation report declares seven axes and twenty-six rungs and, until
 * the evidence contract existed, recognized three document schemas feeding
 * three axes. This compiler interpreted 4,880 occurrences every run and none of
 * it was counted, because there was no way for an owner that was not
 * ForeignTwin, DomainProfile or ProtocolContract to say anything at all.
 *
 * The two digests are the reason a row can be believed. `basis` is what this
 * owner actually read, canonically rendered, and `evidence` is what it
 * produced. A row without both is refused by the reader, so an owner cannot
 * write itself onto a rung it did not reach -- which is the difference between
 * reporting participation and laundering it. */
static void participation_digests(const struct Occurrence *o,
                                  char basis[65], char evidence[65]) {
    char buf[1024];
    int n;
    n = snprintf(buf, sizeof(buf), "%s\x1f%s\x1f%s\x1f%s\x1f%u\x1f%u",
                 o->id, o->name, o->source_kind, o->domains,
                 (unsigned)o->offers, (unsigned)o->shape_phenotypes);
    if (n < 0) n = 0;
    if ((size_t)n > sizeof(buf) - 1) n = (int)sizeof(buf) - 1;
    bf_sha256_hex((const uint8_t *)buf, (size_t)n, basis);
    n = snprintf(buf, sizeof(buf), "%s\x1f%u\x1f%u\x1finterpreted",
                 o->id, (unsigned)o->phenotypes, (unsigned)o->meaningful);
    if (n < 0) n = 0;
    if ((size_t)n > sizeof(buf) - 1) n = (int)sizeof(buf) - 1;
    bf_sha256_hex((const uint8_t *)buf, (size_t)n, evidence);
}

static void participation_now(char out[32]) {
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static int cmd_interpret(const struct Occurrence *rows, size_t count, const char *out_path) {
    FILE *fh = out_path ? fopen(out_path, "a") : stdout;
    long long written = 0, skipped = 0;
    char basis[65], evidence[65], stamp[32];
    if (!fh) { fprintf(stderr, "cannot append to %s\n", out_path); return 1; }
    participation_now(stamp);
    for (size_t i = 0; i < count; i++) {
        int first = 1;
        if (!rows[i].phenotypes) { skipped++; continue; }
        participation_digests(&rows[i], basis, evidence);
        fprintf(fh,
            "{\"schema\":\"bonfyre-owner-participation.v1\",\"occurrence_id\":\"%s\""
            ",\"axis\":\"semantic\",\"owner\":\"ProtocolFacetCompiler\",\"state\":\"interpreted\""
            ",\"from_owner\":\"ExternalEstate\",\"to_owner\":\"ProtocolFacetCompiler\""
            ",\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\""
            ",\"produced_by\":\"bonfyre-facet\",\"recorded_at\":\"%s\""
            ",\"phenotypes\":[", rows[i].id, basis, evidence, stamp);
        for (int p = 0; p < PHENOTYPE_COUNT; p++)
            if (rows[i].phenotypes & (1u << p)) {
                if (!first) fputc(',', fh);
                first = 0;
                fprintf(fh, "\"%s\"", PHENOTYPES[p]);
            }
        fprintf(fh, "],\"relations_meaningful\":%d", __builtin_popcount(rows[i].meaningful));
        fprintf(fh,
            ",\"truth\":\"a_description_was_typed_not_a_source_observed\""
            ",\"cannot_infer\":[\"an interpretation -> an observation, a receipt, or a "
            "reachable source\",\"a phenotype -> an observation of the thing it names\"]}\n");
        written++;
    }
    if (out_path) fclose(fh);
    fprintf(stderr, "interpreted %lld occurrences; %lld carried no phenotype and were "
                    "left alone\n", written, skipped);
    return 0;
}


/* ------------------------------------------------------ carrier conformance -- */

/* Does the representation this source actually served conform to a carrier its
 * phenotype expects?
 *
 * semantic/carrier_conformant sits above profile_bound on the ladder, and
 * profile_bound already requires a retained body. So this rung was unreachable
 * for as long as nothing had fetched anything, and deriving it from declared
 * metadata would have laundered a rung above one that needs real evidence. With
 * bodies in hand it is answerable from the bytes, and it costs nothing: the
 * request was already made.
 *
 * The check is deliberately narrow. A carrier is only claimed when the thing
 * detected in the bytes appears in this phenotype's own carrier list. Detecting
 * a carrier the table does not associate with the phenotype is reported and not
 * claimed, because the interesting case is not a source that failed -- it is a
 * vocabulary that is missing a carrier the estate keeps meeting.
 *
 * Cannot infer: conformance -> correctness. An Atom feed that parses as Atom is
 * an Atom feed. It is not true, current, or permitted to redistribute. */
struct CarrierProbe { const char *name; const char *needle; int xml_head; };

static const struct CarrierProbe CARRIER_PROBES[] = {
    { "Atom",    "<feed",          1 },
    { "OpenAPI", "\"openapi\"",    0 },
    { "OpenAPI", "\"swagger\"",    0 },
    { "STAC",    "\"stac_version\"", 0 },
    { "DCAT",    "dcat:",          0 },
    { "RO-Crate","ro-crate-metadata", 0 },
    { "IIIF",    "iiif.io/api",    0 },
    { "RSS",     "<rss",           1 },
    { "Sitemap", "<urlset",        1 },
};
#define CARRIER_PROBE_COUNT (int)(sizeof(CARRIER_PROBES)/sizeof(CARRIER_PROBES[0]))

/* Does any phenotype at all declare this carrier, whether or not this
 * occurrence was typed with it?
 *
 * This is the third derivation tier and the only one that rests on bytes. Shape
 * evidence reads what a thing is described as; domain evidence reads the
 * neighbourhood it sits in; both were available before anything had been
 * fetched, and both are guesses about a source rather than observations of one.
 *
 * The estate typed 502 syndication feeds as DATASET and MEDIA_ASSET and 49 of
 * them as EVENT, because a feed's description does not say "feed" -- its bytes
 * do. A carrier detected in a retained body is the source telling the estate
 * what it is, and that outranks what the catalogue assumed. */
static int carrier_declared_anywhere(const char *carrier, int *via) {
    for (int p = 0; p < PHENOTYPE_COUNT; p++) {
        const char *list = PHENOTYPE_CARRIERS[p];
        size_t n = strlen(carrier);
        for (const char *at = strstr(list, carrier); at; at = strstr(at + 1, carrier)) {
            char before = at == list ? ',' : at[-1];
            char after = at[n];
            if ((before == ',' || before == ' ') && (after == ',' || after == '\0')) {
                if (via) *via = p;
                return 1;
            }
        }
    }
    return 0;
}

static int carrier_expected(const struct Occurrence *o, const char *carrier, int *via) {
    for (int p = 0; p < PHENOTYPE_COUNT; p++) {
        if (!(o->phenotypes & (1u << p))) continue;
        {
            const char *list = PHENOTYPE_CARRIERS[p];
            size_t n = strlen(carrier);
            for (const char *at = strstr(list, carrier); at; at = strstr(at + 1, carrier)) {
                char before = at == list ? ',' : at[-1];
                char after = at[n];
                if ((before == ',' || before == ' ') && (after == ',' || after == '\0')) {
                    if (via) *via = p;
                    return 1;
                }
            }
        }
    }
    return 0;
}

/* Lowercase the leading window once; every probe reads the same buffer. */
/* Returns 1 carrier found, 0 nothing matched, -1 the body is not there.
 *
 * Those last two were one return value, and the difference between them is the
 * difference between "this source carries no carrier we can detect" and "we no
 * longer hold this source". A sweep that reports both as nothing-detected is
 * reporting an absence of evidence as evidence of absence, about 691 bodies
 * this estate's own receipts still claim to hold. */
static int carrier_detect(const char *body_path, char *out, size_t cap) {
    char window[8192];
    size_t got;
    FILE *fh = fopen(body_path, "rb");
    if (!fh) return -1;
    got = fread(window, 1, sizeof(window) - 1, fh);
    fclose(fh);
    window[got] = '\0';
    for (size_t i = 0; i < got; i++) window[i] = (char)tolower((unsigned char)window[i]);
    for (int i = 0; i < CARRIER_PROBE_COUNT; i++) {
        if (strstr(window, CARRIER_PROBES[i].needle)) {
            snprintf(out, cap, "%s", CARRIER_PROBES[i].name);
            return 1;
        }
    }
    return 0;
}

/* One receipt against the corpus.
 *
 * Split out of the command so a whole directory of receipts can be read
 * against a corpus parsed once. Conformance had only a single-receipt entry
 * point, and the estate has 3,316 receipts -- running it per receipt meant
 * parsing 4,891 occurrences 3,316 times, which is why it had been run on 521
 * bodies and never on the other 1,984. The gap in the evidence was a cost of
 * the interface, not a decision about the sources.
 *
 * Returns 0 conformant, 1 detected-but-undeclared, 2 nothing detected, 3 the
 * receipt could not be read or its subject is not in the corpus. Those are
 * different facts and the batch counts them separately. */
static int conformance_one(const struct Occurrence *rows, size_t count,
                           const char *receipt_path, FILE *conformant_fh,
                           FILE *undeclared_fh) {
    char doc[65536], twin[256] = {0}, body[1024] = {0}, carrier[64] = {0}, stamp[32];
    const struct Occurrence *subject = NULL;
    FILE *rf;
    size_t got;
    int via = -1, expected;

    if (!(rf = fopen(receipt_path, "rb"))) return 3;
    got = fread(doc, 1, sizeof(doc) - 1, rf);
    fclose(rf);
    doc[got] = '\0';
    {
        char *newest = NULL;
        for (char *line = strtok(doc, "\n"); line; line = strtok(NULL, "\n"))
            if (line[0] == '{') newest = line;
        if (!newest) return 3;
        if (!bf_lex_json_string(newest, "twin_id", twin, sizeof(twin)) ||
            !bf_lex_json_string(newest, "body_path", body, sizeof(body))) return 3;
    }
    for (size_t i = 0; i < count; i++)
        if (!strcmp(rows[i].id, twin)) subject = &rows[i];
    if (!subject) return 3;
    switch (carrier_detect(body, carrier, sizeof(carrier))) {
        case 1: break;
        case 0: return 2;    /* bytes here, no carrier among the nine probes */
        default: return 4;   /* the receipt claims a body this estate has lost */
    }
    expected = carrier_expected(subject, carrier, &via);
    if (!expected && carrier_declared_anywhere(carrier, &via)) expected = 2;

    participation_now(stamp);
    if (expected) {
        char basis[65], evidence[65], scratch[2048];
        int n = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f%s", twin, body, carrier);
        if (n < 0) n = 0;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)n, basis);
        n = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f" "carrier_conformant", twin, carrier);
        if (n < 0) n = 0;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)n, evidence);
        fprintf(conformant_fh,
            "{\"schema\":\"bonfyre-owner-participation.v1\",\"occurrence_id\":\"%s\""
            ",\"axis\":\"semantic\",\"state\":\"carrier_conformant\",\"owner\":\"CarrierConformance\""
            ",\"from_owner\":\"DomainProfile\",\"to_owner\":\"CarrierConformance\""
            ",\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\""
            ",\"produced_by\":\"bonfyre-facet\",\"recorded_at\":\"%s\""
            ",\"carrier\":\"%s\",\"via_phenotype\":\"%s\",\"phenotype_derivation\":\"%s\""
            ",\"cannot_infer\":[\"conformance -> correctness, currency or a right to redistribute\""
            ",\"an observed phenotype -> the catalogue being wrong about everything else it said\"]}\n",
            twin, basis, evidence, stamp, carrier, via >= 0 ? PHENOTYPES[via] : "",
            expected == 2 ? "observed" : "typed");
        return 0;
    }
    fprintf(undeclared_fh,
        "{\"schema\":\"bonfyre-carrier-undeclared.v1\",\"occurrence_id\":\"%s\""
        ",\"carrier_detected\":\"%s\",\"claimed\":false"
        ",\"truth\":\"the_bytes_carry_this_and_no_phenotype_of_this_occurrence_declares_it\"}\n",
        twin, carrier);
    return 1;
}

/* Every receipt in a directory, against a corpus parsed once. */
static int cmd_conformance_batch(const struct Occurrence *rows, size_t count,
                                 const char *dir, const char *out_path,
                                 const char *undeclared_path) {
    DIR *d = opendir(dir);
    struct dirent *ent;
    FILE *conf_fh, *und_fh;
    long conformant = 0, undeclared = 0, nothing = 0, unreadable = 0, seen = 0, body_gone = 0;
    if (!d) { fprintf(stderr, "no receipt directory at %s\n", dir); return 1; }
    conf_fh = out_path ? fopen(out_path, "a") : stdout;
    if (!conf_fh) { closedir(d); return 1; }
    und_fh = undeclared_path ? fopen(undeclared_path, "a") : conf_fh;
    if (!und_fh) { if (out_path) fclose(conf_fh); closedir(d); return 1; }
    while ((ent = readdir(d))) {
        char path[PATH_MAX];
        const char *dot = strrchr(ent->d_name, '.');
        if (!dot || strcmp(dot, ".json")) continue;
        if (snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name) >= (int)sizeof(path)) continue;
        seen++;
        switch (conformance_one(rows, count, path, conf_fh, und_fh)) {
            case 0: conformant++; break;
            case 1: undeclared++; break;
            case 2: nothing++; break;
            case 4: body_gone++; break;
            default: unreadable++; break;
        }
    }
    closedir(d);
    if (undeclared_path) fclose(und_fh);
    if (out_path) fclose(conf_fh);
    fprintf(stderr,
        "{\"schema\":\"bonfyre-carrier-conformance-sweep.v1\",\"receipts\":%ld"
        ",\"conformant\":%ld,\"detected_undeclared\":%ld,\"no_carrier_detected\":%ld"
        ",\"body_claimed_but_absent\":%ld,\"unreadable_or_not_in_corpus\":%ld"
        ",\"cannot_infer\":[\"no_carrier_detected -> the body carries none; the probe set is nine needles\""
        ",\"body_claimed_but_absent -> the source stopped serving it; the bytes were ours and are gone\""
        ",\"conformant -> correct, current, or redistributable\"]}\n",
        seen, conformant, undeclared, nothing, body_gone, unreadable);
    return 0;
}

static int cmd_conformance(const struct Occurrence *rows, size_t count, int argc, char **argv) {
    const char *receipt_path = arg(argc, argv, "--receipt");
    const char *receipt_dir = arg(argc, argv, "--receipts");
    const char *out_path = arg(argc, argv, "--out");
    const char *undeclared_path = arg(argc, argv, "--undeclared");
    char doc[65536], twin[256] = {0}, body[1024] = {0}, carrier[64] = {0}, stamp[32];
    const struct Occurrence *subject = NULL;
    FILE *rf, *fh;
    size_t got;
    int via = -1, expected;

    if (receipt_dir)
        return cmd_conformance_batch(rows, count, receipt_dir, out_path, undeclared_path);
    if (!receipt_path) { fprintf(stderr, "conformance requires --receipt or --receipts\n"); return 2; }
    if (!(rf = fopen(receipt_path, "rb"))) { fprintf(stderr, "no receipt at %s\n", receipt_path); return 1; }
    got = fread(doc, 1, sizeof(doc) - 1, rf);
    fclose(rf);
    doc[got] = '\0';
    {   /* Receipts are append-only logs, so the newest line describes the
         * representation this estate currently holds. The newest LINE, not the
         * last brace: a receipt contains a nested http_evidence object, and
         * seeking the last brace lands inside it, where twin_id is not a key at
         * all. That silently found nothing on all 3,316 receipts. */
        char *newest = NULL;
        for (char *line = strtok(doc, "\n"); line; line = strtok(NULL, "\n"))
            if (line[0] == '{') newest = line;
        if (!newest) return 1;
        if (!bf_lex_json_string(newest, "twin_id", twin, sizeof(twin)) ||
            !bf_lex_json_string(newest, "body_path", body, sizeof(body))) return 1;
    }
    for (size_t i = 0; i < count; i++)
        if (!strcmp(rows[i].id, twin)) subject = &rows[i];
    if (!subject) { fprintf(stderr, "%s is not in the corpus\n", twin); return 1; }
    if (!carrier_detect(body, carrier, sizeof(carrier))) return 1;
    expected = carrier_expected(subject, carrier, &via);
    if (!expected && carrier_declared_anywhere(carrier, &via)) expected = 2;  /* observed */

    participation_now(stamp);
    fh = out_path ? fopen(out_path, "a") : stdout;
    if (!fh) return 1;
    if (expected) {
        char basis[65], evidence[65], scratch[2048];
        int n = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f%s", twin, body, carrier);
        if (n < 0) n = 0;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)n, basis);
        n = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f" "carrier_conformant", twin, carrier);
        if (n < 0) n = 0;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)n, evidence);
        fprintf(fh,
            "{\"schema\":\"bonfyre-owner-participation.v1\",\"occurrence_id\":\"%s\""
            ",\"axis\":\"semantic\",\"state\":\"carrier_conformant\",\"owner\":\"CarrierConformance\""
            ",\"from_owner\":\"DomainProfile\",\"to_owner\":\"CarrierConformance\""
            ",\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\""
            ",\"produced_by\":\"bonfyre-facet\",\"recorded_at\":\"%s\""
            ",\"carrier\":\"%s\",\"via_phenotype\":\"%s\",\"phenotype_derivation\":\"%s\""
            ",\"cannot_infer\":[\"conformance -> correctness, currency or a right to redistribute\""
            ",\"an observed phenotype -> the catalogue being wrong about everything else it said\"]}\n",
            twin, basis, evidence, stamp, carrier, via >= 0 ? PHENOTYPES[via] : "",
            expected == 2 ? "observed" : "typed");
    } else {
        /* Reported, never claimed. A carrier the estate keeps meeting and never
         * declared is a gap in the vocabulary, not a failure of the source. */
        fprintf(fh,
            "{\"schema\":\"bonfyre-carrier-undeclared.v1\",\"occurrence_id\":\"%s\""
            ",\"carrier_detected\":\"%s\",\"claimed\":false"
            ",\"truth\":\"the_bytes_carry_this_and_no_phenotype_of_this_occurrence_declares_it\"}\n",
            twin, carrier);
    }
    if (out_path) fclose(fh);
    return expected ? 0 : 1;
}


/* Which protocols can actually serve a phenotype.
 *
 * Binding a grammar to phenotypes gives it relations, and relations are still
 * only a description. The ability is the protocol: 739 of them in this estate
 * each offer some subset of the twenty-five relations, and a grammar member is
 * executable exactly to the degree that some protocol offers what its phenotype
 * admits.
 *
 * `compile` already answers this for an occurrence and cannot answer it for a
 * grammar member, because an application kind is not an occurrence and never
 * will be. This takes the phenotypes directly, which is what a bound grammar
 * has, and reports the protocols that could carry each admitted relation.
 *
 * Cannot infer: a protocol offering a relation -> that protocol being
 * reachable, admitted or lawful here. This counts what exists in the estate's
 * protocol population; whether any of it may be enacted is the ladder's
 * question and the answer is usually no until a receipt says otherwise. */
static int cmd_reach(const struct Occurrence *rows, size_t count, int argc, char **argv) {
    const char *want = arg(argc, argv, "--phenotypes");
    int as_json = has_flag(argc, argv, "--json");
    uint32_t admitted = 0;
    int chosen[PHENOTYPE_COUNT], nchosen = 0;
    long long per_relation[RELATION_COUNT];
    long long protocols_any = 0, protocol_total = 0;

    if (!want) { fprintf(stderr, "reach requires --phenotypes NAME[,NAME...]\n"); return 2; }
    memset(per_relation, 0, sizeof(per_relation));
    {
        char buf[512], *tok;
        snprintf(buf, sizeof(buf), "%s", want);
        for (tok = strtok(buf, ","); tok; tok = strtok(NULL, ",")) {
            int found = -1;
            while (*tok == ' ') tok++;
            for (int p = 0; p < PHENOTYPE_COUNT; p++)
                if (!strcmp(tok, PHENOTYPES[p])) found = p;
            if (found < 0) { fprintf(stderr, "not a declared phenotype: %s\n", tok); return 2; }
            if (nchosen < PHENOTYPE_COUNT) chosen[nchosen++] = found;
            admitted |= PHENOTYPE_RELATIONS[found];
        }
    }
    for (size_t i = 0; i < count; i++) {
        if (strcmp(rows[i].source_kind, "machine_protocol")) continue;
        protocol_total++;
        if (rows[i].offers & admitted) protocols_any++;
        for (int r = 0; r < RELATION_COUNT; r++)
            if ((admitted & BIT(r)) && (rows[i].offers & BIT(r))) per_relation[r]++;
    }
    if (as_json) {
        int first = 1;
        printf("{\"schema\":\"bonfyre-phenotype-reach.v1\",\"phenotypes\":[");
        for (int i = 0; i < nchosen; i++)
            printf("%s\"%s\"", i ? "," : "", PHENOTYPES[chosen[i]]);
        printf("],\"protocol_population\":%lld,\"protocols_offering_any\":%lld,\"by_relation\":{",
               protocol_total, protocols_any);
        for (int r = 0; r < RELATION_COUNT; r++) {
            if (!(admitted & BIT(r))) continue;
            printf("%s\"%s\":%lld", first ? "" : ",", RELATIONS[r], per_relation[r]);
            first = 0;
        }
        printf("},\"truth\":\"protocols_that_offer_the_relation_not_protocols_anyone_may_use\","
               "\"cannot_infer\":[\"a protocol offering a relation -> it being reachable, admitted or lawful here\","
               "\"reach -> an enacted request; nothing here has been contracted\"]}\n");
        return 0;
    }
    printf("phenotypes: %s\n", want);
    printf("protocol population: %lld, offering at least one admitted relation: %lld\n",
           protocol_total, protocols_any);
    for (int r = 0; r < RELATION_COUNT; r++)
        if (admitted & BIT(r)) printf("  %-14s %lld protocols\n", RELATIONS[r], per_relation[r]);
    return 0;
}

/* ---------------------------------------------------------- observe plan -- */

/* What is worth observing, and in what order.
 *
 * The estate has 4,891 occurrences and nothing has looked at 4,881 of them.
 * Observing is ForeignTwin's job and choosing is this compiler's, so this emits
 * a plan and fetches nothing: twin id, url, and why it is worth a request.
 *
 * Ordering matters because a sweep is rude if it is arbitrary. Occurrences are
 * emitted grouped by host so a driver can rate-limit per host without sorting,
 * and the phenotype and facet count travel with each line so a partial sweep
 * spends its requests on the occurrences that would unlock the most.
 *
 * Cannot infer: a planned observation is not an observation, and a high facet
 * count is not a reason a source wants to be read. robots still decides, and
 * it decides at request time rather than here.
 */
static int host_of(const char *url, char *out, size_t cap) {
    const char *scheme = url ? strstr(url, "://") : NULL;
    const char *start, *end;
    size_t n;
    out[0] = '\0';
    if (!scheme) return 0;
    start = scheme + 3;
    end = start;
    while (*end && *end != '/' && *end != '?' && *end != '#') end++;
    n = (size_t)(end - start);
    if (!n || n >= cap) return 0;
    memcpy(out, start, n);
    out[n] = '\0';
    return 1;
}

struct PlanRow { char host[256]; char id[64]; char url[1024]; int facets; int phenotype; };

static int plan_cmp(const void *a, const void *b) {
    const struct PlanRow *x = a, *y = b;
    int c = strcmp(x->host, y->host);
    if (c) return c;
    /* Within a host, the occurrence that would unlock the most goes first. */
    if (x->facets != y->facets) return y->facets - x->facets;
    return strcmp(x->id, y->id);
}

static int cmd_observe_plan(const struct Occurrence *rows, size_t count,
                            const char *corpus_path, int limit) {
    struct PlanRow *plan = NULL;
    size_t n = 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    FILE *fh = fopen(corpus_path, "rb");
    char *line = NULL;
    size_t cap = 0;
    ssize_t len;

    if (!fh) { fprintf(stderr, "cannot read corpus\n"); return 1; }
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) { fclose(fh); return 1; }
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,'$.id'), json_extract(?1,'$.primary_url')",
                           -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); fclose(fh); return 1; }
    plan = calloc(count, sizeof(*plan));
    if (!plan) { sqlite3_finalize(st); sqlite3_close(db); fclose(fh); return 1; }

    while ((len = getline(&line, &cap, fh)) > 0) {
        char id[64] = "", url[1024] = "";
        if (len && line[len-1] == '\n') line[--len] = '\0';
        if (!len) continue;
        sqlite3_bind_text(st, 1, line, (int)len, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *a = (const char *)sqlite3_column_text(st, 0);
            const char *b = (const char *)sqlite3_column_text(st, 1);
            if (a) snprintf(id, sizeof(id), "%s", a);
            if (b) snprintf(url, sizeof(url), "%s", b);
        }
        sqlite3_reset(st); sqlite3_clear_bindings(st);
        if (!id[0] || strncmp(url, "http", 4)) continue;
        if (n >= count) break;
        if (!host_of(url, plan[n].host, sizeof(plan[n].host))) continue;
        snprintf(plan[n].id, sizeof(plan[n].id), "%s", id);
        snprintf(plan[n].url, sizeof(plan[n].url), "%s", url);
        plan[n].phenotype = -1;
        for (size_t k = 0; k < count; k++) {
            if (strcmp(rows[k].id, id)) continue;
            for (int p = 0; p < PHENOTYPE_COUNT; p++)
                if (rows[k].phenotypes & (1u << p)) { plan[n].phenotype = p; break; }
            for (size_t j = 0; j < count; j++) {
                uint32_t shared;
                if (strcmp(rows[j].source_kind, "machine_protocol") || j == k) continue;
                shared = rows[j].offers & rows[k].meaningful;
                plan[n].facets += __builtin_popcount(shared);
            }
            break;
        }
        n++;
    }
    free(line);
    sqlite3_finalize(st);
    sqlite3_close(db);
    fclose(fh);

    qsort(plan, n, sizeof(*plan), plan_cmp);
    printf("# twin_id\turl\thost\tcandidate_facets\tphenotype\n");
    {
        size_t emitted = 0;
        for (size_t i = 0; i < n; i++) {
            if (limit > 0 && emitted >= (size_t)limit) break;
            printf("%s\t%s\t%s\t%d\t%s\n", plan[i].id, plan[i].url, plan[i].host,
                   plan[i].facets,
                   plan[i].phenotype >= 0 ? PHENOTYPES[plan[i].phenotype] : "");
            emitted++;
        }
        fprintf(stderr, "planned %zu of %zu observable occurrences across hosts; "
                        "nothing was fetched and robots still decides at request time\n",
                emitted, n);
    }
    free(plan);
    return 0;
}

/* --------------------------------------------------------- vocabulary out -- */

/* The vocabulary, including which relations each phenotype admits.
 *
 * That table lived only as a multi-line C initialiser, so anything else that
 * wanted it had to parse C -- and a line-indexed parse of a table whose rows
 * wrap across lines silently attributes one phenotype's relations to another.
 * It reported that a Checkout admits VERIFY and not PAY, which is not what a
 * checkout does and is not what the table says. Emitting it is cheaper than
 * every reader getting it wrong differently. */
static int cmd_vocabulary(int argc, char **argv) {
    int as_json = has_flag(argc, argv, "--json");
    if (as_json) {
        fputs("{\"schema\":\"bonfyre-facet-vocabulary.v1\",\"relations\":[", stdout);
        for (int r = 0; r < RELATION_COUNT; r++)
            printf("%s\"%s\"", r ? "," : "", RELATIONS[r]);
        fputs("],\"phenotypes\":{", stdout);
        for (int p = 0; p < PHENOTYPE_COUNT; p++) {
            int first = 1;
            printf("%s\"%s\":{\"admits\":[", p ? "," : "", PHENOTYPES[p]);
            for (int r = 0; r < RELATION_COUNT; r++)
                if (PHENOTYPE_RELATIONS[p] & BIT(r)) {
                    printf("%s\"%s\"", first ? "" : ",", RELATIONS[r]);
                    first = 0;
                }
            fputs("],\"carriers\":\"", stdout);
            fputs(PHENOTYPE_CARRIERS[p], stdout);
            fputs("\"}", stdout);
        }
        fputs("},\"cannot_infer\":[\"an admitted relation -> a lawful one; admission is what the "
              "phenotype can carry and authority decides what may be done\"]}\n", stdout);
        return 0;
    }
    for (int r = 0; r < RELATION_COUNT; r++) printf("relation %s\n", RELATIONS[r]);
    for (int p = 0; p < PHENOTYPE_COUNT; p++) {
        printf("phenotype %s", PHENOTYPES[p]);
        for (int a = 0; a < 4; a++)
            if (PHENOTYPE_AFFORDANCE[p][a]) printf(" %s", PHENOTYPE_AFFORDANCE[p][a]);
        printf(" admits:");
        for (int r = 0; r < RELATION_COUNT; r++)
            if (PHENOTYPE_RELATIONS[p] & BIT(r)) printf(" %s", RELATIONS[r]);
        putchar('\n');
    }
    return 0;
}

/* ------------------------------------------------------------------ main -- */

static void usage(void) {
    fputs("bonfyre-facet <command> --corpus <observed-external-estate.jsonl>\n"
          "  vocabulary                 the finite relation and phenotype sets\n"
          "  braid-input [--occurrence] OccurrenceBraidInput for one or all\n"
          "  compile --occurrence <id>  candidate ProtocolFacetContracts\n"
          "  estate                     full depth across the whole corpus\n"
          "  domains                    density per domain the corpus carries\n"
          "  interpret [--out <jsonl>]  occurrence-keyed interpretation evidence\n"
          "  observe-plan [--limit N]   what is worth observing, host-grouped\n"
          "  contract --occurrence --protocol --relation --receipt [--out]\n"
          "                             promote one facet to contract_compiled\n"
          "  enact --occurrence --protocol --relation --db --actor --subject --at\n"
          "        [--grant-authority] [--evidence-stands] [--discharge] [--out]\n"
          "                             authorize, enact and close one contract\n"
          "  conformance --receipt R | --receipts DIR [--out J] [--undeclared J]\n"
          "                             conform to a carrier this phenotype declares\n"
          "  reach --phenotypes A[,B] [--json]  which protocols can serve a phenotype\n"
          "  density --evidence <jsonl> [--occurrence <id>]  realized braid shape\n"
          "  reconcile-contract-joins --evidence <jsonl> --out <jsonl>\n"
          "\n"
          "Everything emitted is candidate_match. A candidate facet is not a\n"
          "compiled contract, an enactment, closed obligations, or an effect.\n", stderr);
}

#define FACET_COMMANDS(X) \
    X("--help", FC_HELP) \
    X("braid-input", FC_BRAID_INPUT) \
    X("compile", FC_COMPILE) \
    X("conformance", FC_CONFORMANCE) \
    X("contract", FC_CONTRACT) \
    X("density", FC_DENSITY) \
    X("domains", FC_DOMAINS) \
    X("enact", FC_ENACT) \
    X("estate", FC_ESTATE) \
    X("help", FC_HELP_ALIAS) \
    X("interpret", FC_INTERPRET) \
    X("observe-plan", FC_OBSERVE_PLAN) \
    X("reach", FC_REACH) \
    X("reconcile-contract-joins", FC_RECONCILE_CONTRACT_JOINS) \
    X("vocabulary", FC_VOCABULARY)

enum FacetCommand {
#define FACET_ENUM(name, command_id) command_id,
    FACET_COMMANDS(FACET_ENUM)
#undef FACET_ENUM
};

static const BfCommandEntry facet_commands[] = {
#define FACET_ENTRY(name, command_id) {name, command_id, 0, NULL},
    FACET_COMMANDS(FACET_ENTRY)
#undef FACET_ENTRY
};

int main(int argc, char **argv) {
    struct Occurrence *rows = NULL;
    size_t count = 0;
    const char *corpus;
    const BfCommandEntry *command;
    int rc;

    if (argc < 2) { usage(); return 2; }
    command = bf_command_lookup(facet_commands,
                                sizeof(facet_commands) / sizeof(facet_commands[0]), argv[1]);
    if (!command) { usage(); return 2; }
    if (command->id == FC_HELP || command->id == FC_HELP_ALIAS) { usage(); return 0; }
    if (command->id == FC_VOCABULARY) return cmd_vocabulary(argc, argv);
    if (command->id == FC_DENSITY) {
        const char *evidence = arg(argc, argv, "--evidence");
        if (!evidence) { usage(); return 2; }
        return cmd_density(evidence, arg(argc, argv, "--occurrence"));
    }

    corpus = arg(argc, argv, "--corpus");
    if (!corpus) { usage(); return 2; }
    if (!load_corpus(corpus, &rows, &count) || !count) {
        fprintf(stderr, "corpus carried no occurrences this compiler can read\n");
        free(rows);
        return 1;
    }

    if (command->id == FC_BRAID_INPUT) rc = cmd_braid_input(rows, count, arg(argc, argv, "--occurrence"));
    else if (command->id == FC_ESTATE) rc = cmd_estate(rows, count);
    else if (command->id == FC_DOMAINS) rc = cmd_domains(rows, count);
    else if (command->id == FC_INTERPRET) rc = cmd_interpret(rows, count, arg(argc, argv, "--out"));
    else if (command->id == FC_OBSERVE_PLAN)
        rc = cmd_observe_plan(rows, count, corpus,
                              arg(argc, argv, "--limit") ? atoi(arg(argc, argv, "--limit")) : 0);
    else if (command->id == FC_RECONCILE_CONTRACT_JOINS)
        rc = cmd_reconcile_contract_joins(rows, count, arg(argc, argv, "--evidence"), arg(argc, argv, "--out"));
    else if (command->id == FC_CONTRACT) rc = cmd_contract(rows, count, argc, argv);
    else if (command->id == FC_CONFORMANCE) rc = cmd_conformance(rows, count, argc, argv);
    else if (command->id == FC_REACH) rc = cmd_reach(rows, count, argc, argv);
    else if (command->id == FC_ENACT) rc = cmd_enact(rows, count, argc, argv);
    else if (command->id == FC_COMPILE) {
        const char *want = arg(argc, argv, "--occurrence");
        if (!want) { fprintf(stderr, "compile requires --occurrence\n"); rc = 2; }
        else rc = compile_facets(rows, count, want, 1, NULL);
    }
    else { usage(); rc = 2; }

    free(rows);
    return rc;
}
