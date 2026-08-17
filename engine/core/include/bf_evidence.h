#ifndef BF_EVIDENCE_H
#define BF_EVIDENCE_H

#include <stdint.h>
#include <stdio.h>

/*
 * Native evidence relations -- the native peer of the EvidenceGraph in the Python
 * reference (10-Code/BonfyreControlPlane/evidence_graphs.py). Writes the SAME
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

int bf_evidence_open(const char *database_path, BfEvidence **out, FILE *err);
int bf_evidence_open_database(void *sqlite_database, BfEvidence **out, FILE *err);
void bf_evidence_close(BfEvidence *graph);

/* Record a directional evidence relation. kind must be declared. */
BfEvidenceStatus bf_evidence_relate(BfEvidence *graph, const char *evidence,
                                    const char *kind, const char *claim);

/* Does `evidence` SUPPORT `claim`? Directional. Returns 1 yes, 0 no, -1 error. */
int bf_evidence_supports(BfEvidence *graph, const char *evidence, const char *claim);

/* Count of distinct evidences that SUPPORT a claim. -1 on error. */
int64_t bf_evidence_supporters_count(BfEvidence *graph, const char *claim);

#endif
