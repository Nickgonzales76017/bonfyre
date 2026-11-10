#ifndef BF_RETENTION_H
#define BF_RETENTION_H

/*
 * RetentionGC's native authority boundary.
 *
 * Bodies are removable material.  Occurrence is institutional history.  The
 * two are deliberately stored separately: a sweep may remove a body only
 * after its lifetime elapsed and every hold released, and it always leaves a
 * tombstone.  This API owns the same SQLite tables as the former Python
 * reference so a staged migration does not create a second truth store.
 */

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_RETENTION_ID_MAX 256
#define BF_RETENTION_KIND_MAX 64
#define BF_RETENTION_TIER_MAX 32
#define BF_RETENTION_HOLDER_MAX 256
#define BF_RETENTION_REASON_MAX 512
#define BF_RETENTION_SWEEP_MAX 256

typedef struct BfRetention BfRetention;

typedef enum BfRetentionStatus {
    BF_RETENTION_OK = 0,
    BF_RETENTION_INVALID,
    BF_RETENTION_NOT_FOUND,
    BF_RETENTION_NOT_REMOVABLE,
    BF_RETENTION_STORAGE,
    BF_RETENTION_CAPACITY
} BfRetentionStatus;

typedef struct BfRetentionRefusal {
    char record_id[BF_RETENTION_ID_MAX];
    char reason[BF_RETENTION_REASON_MAX];
    char holder[BF_RETENTION_HOLDER_MAX];
} BfRetentionRefusal;

typedef struct BfRetentionDecision {
    int may_remove;
    BfRetentionRefusal refusal;
} BfRetentionDecision;

typedef struct BfRetentionTombstone {
    char record_id[BF_RETENTION_ID_MAX];
    char occurrence[BF_RETENTION_ID_MAX];
    char kind[BF_RETENTION_KIND_MAX];
    char tier[BF_RETENTION_TIER_MAX];
    int64_t written_at_ms;
    int64_t removed_at_ms;
} BfRetentionTombstone;

typedef struct BfRetentionSweep {
    size_t removed_count;
    char removed[BF_RETENTION_SWEEP_MAX][BF_RETENTION_ID_MAX];
    size_t refused_count;
    BfRetentionRefusal refused[BF_RETENTION_SWEEP_MAX];
} BfRetentionSweep;

typedef struct BfRetentionReconciliation {
    size_t placed_count;
    char placed[BF_RETENTION_SWEEP_MAX][BF_RETENTION_ID_MAX + BF_RETENTION_KIND_MAX];
    size_t released_count;
    char released[BF_RETENTION_SWEEP_MAX][BF_RETENTION_ID_MAX + BF_RETENTION_KIND_MAX];
} BfRetentionReconciliation;

int bf_retention_open(const char *database_path, BfRetention **out, FILE *err);
int bf_retention_open_database(void *sqlite_database, BfRetention **out, FILE *err);
void bf_retention_close(BfRetention *retention);

/* A kind carries its closed retention tier.  Callers cannot supply a tier. */
BfRetentionStatus bf_retention_retain(BfRetention *retention, const char *record_id,
                                      const char *occurrence, const char *kind,
                                      int64_t written_at_ms, const char *body);
BfRetentionStatus bf_retention_hold(BfRetention *retention, const char *record_id,
                                    const char *hold, const char *holder);
/* `out_released` is 1 only when this exact hold existed and was removed. */
BfRetentionStatus bf_retention_release(BfRetention *retention, const char *record_id,
                                       const char *hold, const char *holder,
                                       int *out_released);

/*
 * The cross-owner bridge is intentionally narrow: a commitment can name a
 * retained record it rests on, and reconciliation derives only the two holds
 * owned by CommitmentGraph and EvidenceGraph.  It never releases legal or
 * user holds, which belong to their respective external owners.
 */
BfRetentionStatus bf_retention_rests_on(BfRetention *retention, const char *commitment_id,
                                        const char *record_id);
BfRetentionStatus bf_retention_reconcile_holds(BfRetention *retention,
                                               BfRetentionReconciliation *out);

BfRetentionStatus bf_retention_removable(BfRetention *retention, const char *record_id,
                                         int64_t now_ms, BfRetentionDecision *out);
BfRetentionStatus bf_retention_collect(BfRetention *retention, int64_t now_ms,
                                       const char *occurrence_or_null,
                                       BfRetentionSweep *out);

/* A removed body is absent; the occurrence remains answerable via its tombstone. */
BfRetentionStatus bf_retention_body(BfRetention *retention, const char *record_id,
                                    char *out, size_t out_size, int *out_present);
int bf_retention_happened(BfRetention *retention, const char *record_id);
BfRetentionStatus bf_retention_tombstone(BfRetention *retention, const char *record_id,
                                         BfRetentionTombstone *out, int *out_present);

const char *bf_retention_status_name(BfRetentionStatus status);

#ifdef __cplusplus
}
#endif

#endif
