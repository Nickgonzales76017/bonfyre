#ifndef BF_CORROBORATION_H
#define BF_CORROBORATION_H

#include <stdint.h>
#include <stdio.h>

/*
 * Native corroboration ledger -- the native peer of the Python reference
 * (10-Code/BonfyreControlPlane/verification.py). Writes the SAME
 * actor_corroborations table, so a native writer and the Python writer share one
 * ledger and count independence identically.
 *
 * Independence is by distinct source: recording the same (actor, source) twice
 * updates the evidence in place and does NOT raise the independent-source count,
 * so an actor cannot bootstrap its own verification. This ledger records
 * evidence; it never promotes asserted -> verified -- that crosses the human
 * line and stays with the Python/human path.
 */

typedef struct BfCorroboration BfCorroboration;

int bf_corroboration_open(const char *database_path, BfCorroboration **out, FILE *err);
int bf_corroboration_open_database(void *sqlite_database, BfCorroboration **out, FILE *err);
void bf_corroboration_close(BfCorroboration *ledger);

/*
 * Record one attestation. Returns 1 if this is a NEW independent source, 0 if the
 * (actor, source) pair already existed (evidence updated, count unchanged), or -1
 * on error. now may be NULL (=> now). evidence_ref / note may be NULL.
 */
int bf_corroboration_record(BfCorroboration *ledger, const char *actor_id,
                            const char *source, const char *evidence_ref,
                            const char *note, const char *now);

/* Count of distinct sources that have corroborated an actor. -1 on error. */
int64_t bf_corroboration_independent_sources(BfCorroboration *ledger, const char *actor_id);

#endif
