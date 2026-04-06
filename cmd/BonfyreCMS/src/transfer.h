/*
 * BonfyreGraph — Upgrade X: Cross-Family Transfer Learning
 *
 * Measures structural similarity between generator families,
 * transfers eigenvalue priors across similar families, and
 * maintains a template library for accelerated discovery.
 *
 * Key accelerations:
 *   - New families matching a known template skip cold-start
 *   - Eigenvalue estimates are pre-seeded from similar families
 *   - Generator patterns are reused across content types
 */
#ifndef TRANSFER_H
#define TRANSFER_H

#include <sqlite3.h>
#include <stdint.h>

/* A transfer candidate: a similar family to borrow from */
typedef struct {
    char   family_id[128];
    char   generator[4096];
    double similarity;
    int    arity;
    int    member_count;
} TransferCandidate;

/* Bootstrap _transfer_templates and _transfer_log tables. */
int transfer_bootstrap(sqlite3 *db);

/* Compute structural distance between two generators.
   Returns 0.0 (identical) to 1.0 (completely different). */
double transfer_distance(const char *gen_a, const char *gen_b);

/* Find the top-k most similar families to a given family.
   Caller allocates out array of TransferCandidate[max_k].
   Returns actual count found. */
int transfer_candidates(sqlite3 *db, const char *family_id, int max_k,
                        TransferCandidate *out);

/* Transfer eigenvalue priors from a donor family to a recipient.
   Scales the donor's eigenvalues by similarity weight.
   Returns number of positions seeded. */
int transfer_eigen(sqlite3 *db, const char *donor_id,
                   const char *recipient_id, double weight);

/* Auto-transfer: find best donor for a family and apply.
   Returns 0 on success, -1 on failure or no suitable donor. */
int transfer_auto(sqlite3 *db, const char *family_id);

/* Store a generator pattern as a reusable template.
   Returns template_id. */
int transfer_register_template(sqlite3 *db, const char *family_id,
                               const char *name, int64_t *out_template_id);

/* Match a generator string against known templates.
   Returns best matching template_id, or -1 if none match.
   out_similarity receives the similarity score. */
int transfer_match_template(sqlite3 *db, const char *generator,
                            int64_t *out_template_id, double *out_similarity);

/* Accelerated family discovery using template matching.
   If a matching template exists, pre-seed eigenvalues and skip
   initial compaction. Returns 1 if accelerated, 0 if cold start. */
int transfer_accelerate(sqlite3 *db, const char *family_id);

/* Transfer statistics JSON. Caller frees out_json. */
int transfer_stats(sqlite3 *db, char **out_json);

#endif /* TRANSFER_H */
