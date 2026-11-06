/* Generated from schemas/control/destination-release.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_DESTINATION_RELEASE_H
#define BONFYRE_NATIVE_DESTINATION_RELEASE_H
#include <stddef.h>
#include <string.h>

/* `delivered` is reached only by a receipt naming what was materialized.
 * Readiness, admission and intent all leave it false, which is the whole
 * reason this table is generated rather than remembered. */
typedef struct { const char *state; const char *label; unsigned char delivered, open;
                 const char *means; const char *cannot_infer; } BfDestinationState;

/* How a source change bears on a destination that already delivered.
 * `invalidates` is what makes one remote change recompile only what it
 * touched instead of everything on a timer. */
typedef struct { const char *consequence; const char *label;
                 unsigned char invalidates, needs_review;
                 const char *means; const char *cannot_infer; } BfChangeConsequence;

/* Which fields a destination carries, which is what decides whether a
 * change invalidates a delivery. A delivered search fragment holds no
 * amount, so a price that moved does not stale it. */
typedef struct { const char *destination; const char *fields; const char *means; } BfDestinationCarries;

static const BfDestinationState bf_destination_states[] = {
  {"not_selected", "Not selected", 0, 1, "nobody has proposed this destination for this material", "an unselected destination is not a refused one, and not one this estate judged unsuitable"},
  {"selected", "Selected", 0, 1, "an operator named this destination as an intent", "an intent is not an admission; nothing has checked whether it is allowed"},
  {"blocked", "Blocked", 0, 1, "a binding this destination needs is absent, and the missing binding is named", "a blocked destination is not a permanent refusal; naming the binding is how it stops being blocked"},
  {"admissible", "Admissible", 0, 1, "every binding is present and the rights, robots and economics checks passed", "admissible is not delivered, not published, and not permission that survives a change in the source"},
  {"queued", "Queued", 0, 1, "admitted by a human and waiting for work to be done", "a queued release has produced nothing; no fragment exists yet"},
  {"materializing", "Materializing", 0, 1, "work is in progress and partial output may exist", "partial output is not a release, and reading it is not reading a delivery"},
  {"delivered", "Delivered", 1, 0, "a receipt names what was materialized, where, and from which source generation", "a delivery is not a licence to redistribute, and not a claim the destination accepted it as correct"},
  {"stale", "Stale", 1, 1, "delivered, and the source has changed in a way that affects it since", "stale is not withdrawn: what was delivered is still there and its receipt still stands"},
  {"superseded", "Superseded", 1, 0, "a later delivery replaced it, and both receipts remain readable", "superseded is not deleted, and the earlier receipt is not falsified by the later one"},
  {"failed", "Failed", 0, 1, "work was attempted and did not produce a receipt, with the reason recorded", "a failed delivery says nothing about the source; it never changes what was observed"},
  {"withdrawn", "Withdrawn", 0, 0, "an operator took it back, and the receipts of what had been delivered remain", "withdrawal is not erasure of the record that it once existed"},
};
#define BF_DESTINATION_STATE_COUNT (sizeof(bf_destination_states) / sizeof(bf_destination_states[0]))

static const BfChangeConsequence bf_change_consequences[] = {
  {"cosmetic", "No republish", 0, 0, "the source's bytes changed and nothing a destination carries changed with them", "cosmetic is a statement about the fields this estate extracts, never about the source's own intent"},
  {"material", "Republish", 1, 0, "a field a destination carries changed: an amount, an identifier, an availability, a relationship", "a changed amount is not a current offer, and a changed availability is not verified availability"},
  {"structural", "Republish and re-extract", 1, 0, "the shape changed -- a category hierarchy, a part-number scheme, a feed's own schema", "a structural change is not a new source; it is the same source arranged differently"},
  {"boundary", "Review", 1, 1, "robots, a licence, a rights statement or an observe right changed", "a boundary change is a standing rule the source wrote for everyone, never a refusal aimed at this estate"},
  {"disappeared", "Review", 1, 1, "material a destination carries is no longer present at the source", "absence now is not absence then: every earlier observation and its receipt survive this"},
};
#define BF_CHANGE_CONSEQUENCE_COUNT (sizeof(bf_change_consequences) / sizeof(bf_change_consequences[0]))

static const BfDestinationCarries bf_destination_carries[] = {
  {"search", "|title|href|publisher|at|kind|", "what this estate retained, arranged so it can be found; it carries no amount and no availability"},
  {"shop", "|amount|currency|availability|", "observed prices bound to a source coordinate, which is the only destination a price change bears on"},
  {"spatial", "|place|geometry|", "extracted locations, once an extractor is declared and material is retained"},
  {"media", "|media|image|", "extracted media references, once an extractor is declared"},
  {"rights", "|rights|licence|", "extracted rights statements, which no source in this estate has yet stated machine-readably"},
};
#define BF_DESTINATION_CARRIES_COUNT (sizeof(bf_destination_carries) / sizeof(bf_destination_carries[0]))

/* The only way to ask whether a state is a delivery. A caller that
 * compares strings itself is a caller that will eventually compare the
 * wrong one. */
static const BfDestinationState *bf_destination_state(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_DESTINATION_STATE_COUNT; i++) {
        const char *s = bf_destination_states[i].state;
        size_t j = 0;
        while (s[j] && s[j] == name[j]) j++;
        if (!s[j] && !name[j]) return &bf_destination_states[i];
    }
    return NULL;
}

static int bf_destination_delivered(const char *name) {
    const BfDestinationState *s = bf_destination_state(name);
    return s ? (int)s->delivered : 0;
}

/* Does this destination carry this field? A change to a field it does not
 * carry cannot invalidate what it delivered. */
static int bf_destination_carries_field(const char *destination, const char *field) {
    size_t i;
    if (!destination || !field || !*field) return 0;
    for (i = 0; i < BF_DESTINATION_CARRIES_COUNT; i++) {
        const char *d = bf_destination_carries[i].destination;
        size_t j = 0;
        while (d[j] && d[j] == destination[j]) j++;
        if (d[j] || destination[j]) continue;
        {
            const char *list = bf_destination_carries[i].fields;
            const char *p = list;
            while ((p = strchr(p, '|')) != NULL) {
                const char *start = ++p;
                const char *end = strchr(start, '|');
                size_t len;
                if (!end) break;
                len = (size_t)(end - start);
                if (len && !strncmp(start, field, len) && !field[len]) return 1;
                p = end;
            }
        }
        return 0;
    }
    return 0;
}

static const BfChangeConsequence *bf_change_consequence(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_CHANGE_CONSEQUENCE_COUNT; i++) {
        const char *s = bf_change_consequences[i].consequence;
        size_t j = 0;
        while (s[j] && s[j] == name[j]) j++;
        if (!s[j] && !name[j]) return &bf_change_consequences[i];
    }
    return NULL;
}
#endif
