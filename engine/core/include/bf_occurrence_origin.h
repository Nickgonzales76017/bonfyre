#ifndef BF_OCCURRENCE_ORIGIN_H
#define BF_OCCURRENCE_ORIGIN_H

/* Origin is a separate fact from an occurrence's causal ordering.  It prevents
 * a watched or derived event being laundered into an act performed by Bonfyre. */

#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_ORIGIN_ID_MAX 256
#define BF_ORIGIN_INPUT_MAX 64

typedef struct BfOccurrenceOrigin BfOccurrenceOrigin;

typedef enum BfOriginStatus {
    BF_ORIGIN_OK = 0,
    BF_ORIGIN_INVALID,
    BF_ORIGIN_NOT_FOUND,
    BF_ORIGIN_EXISTS,
    BF_ORIGIN_STORAGE,
    BF_ORIGIN_CAPACITY
} BfOriginStatus;

typedef enum BfOriginKind {
    BF_ORIGIN_OBSERVED,
    BF_ORIGIN_DERIVED,
    BF_ORIGIN_EFFECT
} BfOriginKind;

typedef struct BfOccurrenceOriginSpec {
    const char *occurrence_id;
    BfOriginKind origin;
    const char *subject;
    const char *at;
    const char *source;
    const char *authority;
    /* Exact input identifiers for a derived occurrence; ignored otherwise. */
    const char *const *derived_from;
    size_t derived_from_count;
} BfOccurrenceOriginSpec;

typedef struct BfOriginIds {
    size_t count;
    char ids[BF_ORIGIN_INPUT_MAX][BF_ORIGIN_ID_MAX];
} BfOriginIds;

int bf_occurrence_origin_open(const char *database_path, BfOccurrenceOrigin **out, FILE *err);
int bf_occurrence_origin_open_database(void *sqlite_database, BfOccurrenceOrigin **out, FILE *err);
void bf_occurrence_origin_close(BfOccurrenceOrigin *origin);

/* Record once. OBSERVED requires source/no authority; DERIVED requires input(s)
 * /no authority; EFFECT requires authority. */
BfOriginStatus bf_occurrence_origin_record(BfOccurrenceOrigin *origin,
                                           const BfOccurrenceOriginSpec *spec);
BfOriginStatus bf_occurrence_origin_get(BfOccurrenceOrigin *origin, const char *occurrence_id,
                                        BfOriginKind *out, int *out_found);
BfOriginStatus bf_occurrence_origin_inputs(BfOccurrenceOrigin *origin, const char *occurrence_id,
                                           BfOriginIds *out);
int bf_occurrence_origin_caused_by_us(BfOccurrenceOrigin *origin, const char *occurrence_id);
BfOriginStatus bf_occurrence_origin_by_subject(BfOccurrenceOrigin *origin, const char *subject,
                                               BfOriginIds *observed, BfOriginIds *derived,
                                               BfOriginIds *effect);
/* Returns observed roots only. Effects are deliberately not reported as watched
 * material, and traversal is bounded to prevent an invalid cycle from looping. */
BfOriginStatus bf_occurrence_origin_provenance(BfOccurrenceOrigin *origin,
                                               const char *occurrence_id, BfOriginIds *out);

const char *bf_origin_kind_name(BfOriginKind kind);
const char *bf_origin_status_name(BfOriginStatus status);

#ifdef __cplusplus
}
#endif

#endif
