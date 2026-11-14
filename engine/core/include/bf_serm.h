#ifndef BF_SERM_H
#define BF_SERM_H

/*
 * SERM: reduce measurements the estate already holds into a sufficiency and
 * honesty verdict.  It re-runs nothing.  A measurement contract names the
 * layers a capability demand needs; a layer is covered only by a
 * witness-backed measurement (a proven frontier layer, or a cooled measured
 * or proven invariant).  A measured/proven claim with no witness is a
 * laundered plane and fails honesty even when its layer looks covered.  Only
 * a sufficient and honest reduction cools a sufficiency invariant.
 * Ported from components/BonfyreControlPlane/serm.py.archive over the
 * proof frontier's own tables (bf_proof).
 */
#include <stddef.h>

#define BF_SERM_MAX 64

typedef struct {
    int sufficient, honest;
    size_t covered_n, missing_n, laundered_n;
    char covered[BF_SERM_MAX][128], missing[BF_SERM_MAX][128], laundered[BF_SERM_MAX][192];
    char verdict[256];
} BfSermResult;

/* 0 ok, -1 error. */
int bf_serm_reduce(void *sqlite_database, const char *subject_resource, const char *subject_profile,
                   const char *const *required_layers, size_t required_count, BfSermResult *out);
/* Cools serm.sufficiency.<contract_id> when the result earned it.
 * 1 recorded, 0 not earned, -1 error. */
int bf_serm_record_sufficiency(void *sqlite_database, const char *contract_id, const char *capability_demand,
                               const char *subject_resource, const char *subject_profile, const BfSermResult *result);

#endif
