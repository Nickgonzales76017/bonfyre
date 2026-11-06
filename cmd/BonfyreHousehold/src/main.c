/* The household semantic operator, as something the estate can actually run.
 *
 * It binds a decoded food or housing projection to a stable address, lowers a
 * bounded read-only program over those records, and answers a proof query with
 * the one verdict that matters: a field may be reported as observed and can
 * never be reported as proven. Observation does not promote itself, however
 * many times it is repeated and however confident the source sounded.
 *
 * The organ enforced all of that and had no caller anywhere in the estate.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_semantic_operator.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void json_text(const char *s) {
    putchar('"');
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

static BfSemanticOperator operator_by_name(const char *name, int *ok) {
    struct { const char *name; BfSemanticOperator kind; } known[] = {
        {"select", BF_SEMANTIC_SELECT}, {"diff", BF_SEMANTIC_DIFF},
        {"compare", BF_SEMANTIC_COMPARE}, {"why", BF_SEMANTIC_WHY},
        {"prove", BF_SEMANTIC_PROVE}, {"project", BF_SEMANTIC_PROJECT},
        {"reconcile", BF_SEMANTIC_RECONCILE},
    };
    size_t i;
    *ok = 0;
    for (i = 0; i < sizeof(known) / sizeof(known[0]); i++)
        if (name && !strcmp(known[i].name, name)) { *ok = 1; return known[i].kind; }
    return BF_SEMANTIC_SELECT;
}

static BfHouseholdProofClaim claim_by_name(const char *name, int *ok) {
    struct { const char *name; BfHouseholdProofClaim claim; } known[] = {
        {"availability", BF_HOUSEHOLD_PROVE_AVAILABILITY},
        {"dog_access", BF_HOUSEHOLD_PROVE_DOG_ACCESS},
        {"effective_cost", BF_HOUSEHOLD_PROVE_EFFECTIVE_COST},
        {"price", BF_HOUSEHOLD_PROVE_PRICE},
    };
    size_t i;
    *ok = 0;
    for (i = 0; i < sizeof(known) / sizeof(known[0]); i++)
        if (name && !strcmp(known[i].name, name)) { *ok = 1; return known[i].claim; }
    return BF_HOUSEHOLD_PROVE_AVAILABILITY;
}

static const char *status_word(BfHouseholdStatus status) {
    return bf_household_status_name(status);
}

static int usage(void) {
    fputs("BonfyreHousehold -- observed, and never thereby proven\n\n"
          "  bonfyre-household bind --domain housing|food [--listing L | --candidate C | --offer O]\n"
          "        --source-file F --source-ref R --document JSON [--observed a,b,c]\n"
          "        [--operator select|diff|compare|why|prove|project|reconcile]\n"
          "        [--prove availability|dog_access|effective_cost|price]\n\n"
          "Use exactly one identity: listing or candidate for housing, offer for food.\n"
          "A lowered program is read-only. A proof query reports what was observed\n"
          "and reports proven as false, always.\n", stderr);
    return 2;
}

int main(int argc, char *argv[]) {
    BfHouseholdProjection projection;
    BfHouseholdRecord record;
    BfSemanticProgram program;
    BfHouseholdStatus status;
    const char *domain = NULL, *listing = NULL, *candidate = NULL, *offer = NULL;
    const char *source_file = NULL, *source_ref = NULL, *document = NULL;
    const char *observed = "", *operator_name = "select", *prove_name = NULL;
    uint32_t claims = 0;
    int i, ok = 0;
    BfSemanticOperator kind;

    if (argc < 2) return usage();
    enum { HH_BIND };
    static const BfCommandEntry household_commands[] = {
        {"bind", HH_BIND, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(household_commands,
        sizeof(household_commands) / sizeof(household_commands[0]), argv[1]);
    for (i = 2; i < argc - 1; i++) {
        if (!strcmp(argv[i], "--domain")) domain = argv[i + 1];
        else if (!strcmp(argv[i], "--listing")) listing = argv[i + 1];
        else if (!strcmp(argv[i], "--candidate")) candidate = argv[i + 1];
        else if (!strcmp(argv[i], "--offer")) offer = argv[i + 1];
        else if (!strcmp(argv[i], "--source-file")) source_file = argv[i + 1];
        else if (!strcmp(argv[i], "--source-ref")) source_ref = argv[i + 1];
        else if (!strcmp(argv[i], "--document")) document = argv[i + 1];
        else if (!strcmp(argv[i], "--observed")) observed = argv[i + 1];
        else if (!strcmp(argv[i], "--operator")) operator_name = argv[i + 1];
        else if (!strcmp(argv[i], "--prove")) prove_name = argv[i + 1];
    }
    if (!command || command->id != HH_BIND) return usage();
    if (strstr(observed, "availability")) claims |= BF_HOUSEHOLD_OBSERVED_AVAILABILITY;
    if (strstr(observed, "dog")) claims |= BF_HOUSEHOLD_OBSERVED_DOG_ACCESS;
    if (strstr(observed, "effective_cost")) claims |= BF_HOUSEHOLD_OBSERVED_EFFECTIVE_COST;
    if (strstr(observed, "price")) claims |= BF_HOUSEHOLD_OBSERVED_PRICE;

    memset(&projection, 0, sizeof(projection));
    projection.domain = domain;
    projection.offer_id = offer;
    projection.listing_id = listing;
    projection.candidate_id = candidate;
    projection.source_file = source_file;
    projection.source_ref = source_ref;
    projection.canonical_document = document;
    projection.observed_claims = claims;

    memset(&record, 0, sizeof(record));
    status = bf_household_projection_bind(&projection, &record);
    if (status != BF_HOUSEHOLD_OK) {
        /* Each refusal names a specific missing thing: an unsupported domain,
         * an identity the projection did not carry, or no source at all. */
        fputs("{\"bound\":false,\"refused\":", stdout);
        json_text(status_word(status));
        fputs("}\n", stdout);
        return 3;
    }

    kind = operator_by_name(operator_name, &ok);
    if (!ok) { fprintf(stderr, "no such operator: %s\n", operator_name); return 2; }
    memset(&program, 0, sizeof(program));
    status = bf_semantic_program_lower(kind, &record, 1, &program);

    fputs("{\"address\":", stdout); json_text(record.address);
    fputs(",\"kind\":", stdout); json_text(record.kind);
    fputs(",\"operator\":", stdout); json_text(bf_semantic_operator_name(kind));
    if (status == BF_HOUSEHOLD_OK) {
        printf(",\"lowered\":{\"reads\":%zu,\"writes\":%d,\"admitted\":%s,\"digest\":",
               program.read_count, program.writes, program.admitted ? "true" : "false");
        json_text(program.digest);
        putchar('}');
    } else {
        fputs(",\"lowered\":false,\"refused\":", stdout); json_text(status_word(status));
    }

    if (prove_name) {
        BfHouseholdProofVerdict verdict;
        BfHouseholdProofClaim claim = claim_by_name(prove_name, &ok);
        if (!ok) { fprintf(stderr, "no such claim: %s\n", prove_name); return 2; }
        memset(&verdict, 0, sizeof(verdict));
        status = bf_household_prove(&record, claim, &verdict);
        fputs(",\"proof\":{\"claim\":", stdout); json_text(prove_name);
        if (status == BF_HOUSEHOLD_OK) {
            printf(",\"observed\":%s,\"proven\":%s",
                   verdict.observed ? "true" : "false",
                   verdict.proven ? "true" : "false");
            fputs(",\"reason\":", stdout); json_text(verdict.reason);
        } else {
            fputs(",\"refused\":", stdout); json_text(status_word(status));
        }
        putchar('}');
    }
    fputs(",\"cannot_infer\":", stdout);
    json_text("an observed field is what a source said when this estate read it; it is never "
              "proven by having been observed, and a lowered program reads without writing "
              "anything anywhere");
    fputs("}\n", stdout);
    return 0;
}
