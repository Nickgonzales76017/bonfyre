#ifndef BF_EVIDENCE_H
#define BF_EVIDENCE_H

#include <stdint.h>
#include <stdio.h>

/*
 * Native evidence relations -- the native peer of the EvidenceGraph in the Python
 * reference (components/BonfyreControlPlane/evidence_graphs.py). Writes the SAME
 * evidence_relations store, so native and Python share one evidence graph.
 *
 * A relation (evidence --kind--> claim) is DIRECTIONAL: supports(A,B) never
 * implies supports(B,A). The kind is one of supports / contradicts / reproduces /
 * falsifies; an unknown kind is rejected, never coerced.
 */

typedef struct BfEvidence BfEvidence;

typedef enum BfEvidenceStatus {
    BF_EVIDENCE_OK = 0,
    BF_EVIDENCE_INVALID_KIND,  /* kind not one of the declared evidence kinds */
    BF_EVIDENCE_INVALID,       /* a required field is null or empty */
    BF_EVIDENCE_STORAGE_ERROR
} BfEvidenceStatus;

/* The current position of the evidence relation store about one claim.  This
 * is deliberately not a truth/proof verdict: the ProofFrontier owns that
 * stronger conclusion.  A later support can never rescue a contradiction or
 * falsification merely by being written later. */
typedef enum BfEvidenceVerdict {
    BF_EVIDENCE_UNEVIDENCED = 0,
    BF_EVIDENCE_SUPPORTED,
    BF_EVIDENCE_REPRODUCED,
    BF_EVIDENCE_CONTESTED,
    BF_EVIDENCE_FALSIFIED
} BfEvidenceVerdict;

int bf_evidence_open(const char *database_path, BfEvidence **out, FILE *err);
int bf_evidence_open_database(void *sqlite_database, BfEvidence **out, FILE *err);
void bf_evidence_close(BfEvidence *graph);

/* Record a directional evidence relation. kind must be declared. */
BfEvidenceStatus bf_evidence_relate(BfEvidence *graph, const char *evidence,
                                    const char *kind, const char *claim);

/*
 * Retract a relation (the -1): evidence that no longer stands stops supporting.
 * Returns OK whether or not a row existed (idempotent withdrawal), INVALID on a
 * malformed request. This is the retraction primitive the consequence plane needs.
 */
BfEvidenceStatus bf_evidence_retract(BfEvidence *graph, const char *evidence,
                                     const char *kind, const char *claim);

/* Does `evidence` SUPPORT `claim`? Directional. Returns 1 yes, 0 no, -1 error. */
int bf_evidence_supports(BfEvidence *graph, const char *evidence, const char *claim);

/* Count of distinct evidences that SUPPORT a claim. -1 on error. */
int64_t bf_evidence_supporters_count(BfEvidence *graph, const char *claim);

/* Derive the precedence-ordered verdict for a claim.  The result is a query
 * over the relation store, never a persisted second truth store. */
BfEvidenceStatus bf_evidence_verdict(BfEvidence *graph, const char *claim,
                                      BfEvidenceVerdict *out);

/* True only for supported/reproduced claims with no contradiction or
 * falsification.  "stands" must not be read as "proven". */
int bf_evidence_stands(BfEvidence *graph, const char *claim);
const char *bf_evidence_verdict_name(BfEvidenceVerdict verdict);

#endif
