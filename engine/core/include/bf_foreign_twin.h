#ifndef BF_FOREIGN_TWIN_H
#define BF_FOREIGN_TWIN_H

/* ForeignTwin is an observation boundary. A retained/independently observed
 * remote representation can satisfy an observation predicate; it never grants
 * effect authority or turns the remote assertion into institutional truth. */

#include <stdio.h>

typedef struct BfForeignTwin BfForeignTwin;
typedef struct BfForeignTwinObservation {
    const char *receipt_id;
    const char *subject;
    const char *predicate;
    const char *source_url;
    const char *body_sha256;
    const char *observed_at;
    const char *observer;
} BfForeignTwinObservation;

int bf_foreign_twin_open(const char *database_path, BfForeignTwin **out, FILE *err);
int bf_foreign_twin_open_database(void *sqlite_database, BfForeignTwin **out, FILE *err);
void bf_foreign_twin_close(BfForeignTwin *twin);
/* Receipt IDs are idempotent only for byte-identical observation identity. */
int bf_foreign_twin_record(BfForeignTwin *twin, const BfForeignTwinObservation *observation);
/* 1 iff an exact subject+predicate observation remains in the ledger. */
int bf_foreign_twin_observed(BfForeignTwin *twin, const char *subject, const char *predicate);
/* Copy the newest receipt for an exact retained fact into out.  It is a
 * receipt lookup, never an assertion that the remote fact is institutional truth. */
int bf_foreign_twin_latest_receipt(BfForeignTwin *twin, const char *subject,
                                   const char *predicate, char *out, size_t out_size);

#endif
