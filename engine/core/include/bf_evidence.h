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

/*
 * The other four relations of the evidence estate, distinct from evidence
 * support and never folded into it (the reference this ports is archived as
 * components/BonfyreControlPlane/evidence_graphs.py.archive; same tables).
 *
 * ArtifactGraph: transformation ancestry, source --operator--> derived.
 * ProvenanceGraph: the path an artifact took to exist; a clean path does not
 *   make the artifact support any claim.
 * TemporalEvidenceGraph: eight time planes per fact, kept apart, never
 *   collapsed into one timestamp and never deleted by staleness.
 * ClaimGraph: a claim's dependencies, counterclaims and proof state.
 */
typedef int (*BfEvidenceVisit)(const char *a, const char *b, void *ctx); /* nonzero stops */

BfEvidenceStatus bf_evidence_record_transform(BfEvidence *graph, const char *source,
                                              const char *operator_name, const char *derived);
/* Transitive ancestors of derived, each once, cycles tolerated. Count or -1. */
int64_t bf_evidence_lineage(BfEvidence *graph, const char *derived, BfEvidenceVisit visit, void *ctx);

BfEvidenceStatus bf_evidence_record_provenance(BfEvidence *graph, const char *artifact,
                                               const char *role, const char *node);
/* (role, node) pairs ordered by role. Count or -1. */
int64_t bf_evidence_provenance_path(BfEvidence *graph, const char *artifact, BfEvidenceVisit visit, void *ctx);
/* 1 when no node of the path is untrusted, 0 when one is, -1 on error. */
int bf_evidence_path_admits(BfEvidence *graph, const char *artifact,
                            const char *const *untrusted, size_t untrusted_count);

/* event_time, publication_time, observation_time, retrieval_time,
 * effective_time, supersession_time, correction_time, expiration_time. */
int bf_evidence_time_plane_known(const char *plane);
/* Set one plane of a fact; other planes are kept. An undeclared plane is
 * INVALID_KIND. */
BfEvidenceStatus bf_evidence_record_time(BfEvidence *graph, const char *fact,
                                         const char *plane, const char *at);
/* 1 and the value when set, 0 when unset, -1 on error. */
int bf_evidence_time_of(BfEvidence *graph, const char *fact, const char *plane, char *out, size_t cap);
/* Stale for a decision at `at` (ISO, compared as text): past expiration_time.
 * 1 stale, 0 not (or no expiration), -1 error.  The row is never removed. */
int bf_evidence_is_stale(BfEvidence *graph, const char *fact, const char *at);

/* Add dependencies and counterclaims and set the proof state (default "open"). */
BfEvidenceStatus bf_evidence_add_claim(BfEvidence *graph, const char *claim,
                                       const char *const *depends_on, size_t depends_count,
                                       const char *const *counters, size_t counters_count,
                                       const char *proof_state);
/* (dep_kind, target) pairs: dep_kind is "depends_on" or "counter". Count or -1. */
int64_t bf_evidence_claim_dependencies(BfEvidence *graph, const char *claim, BfEvidenceVisit visit, void *ctx);
/* 1 and the state when the claim is recorded, 0 when not, -1 on error. */
int bf_evidence_proof_state(BfEvidence *graph, const char *claim, char *out, size_t cap);

#endif
