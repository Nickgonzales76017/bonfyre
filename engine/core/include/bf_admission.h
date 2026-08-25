#ifndef BF_ADMISSION_H
#define BF_ADMISSION_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

/*
 * Native resource-admission controller. Native peer of the Python reference
 * (10-Code/BonfyreControlPlane/resource_admission.py). Two parts:
 *
 *   1. A pure decision kernel, bf_admission_decide(), that returns one of three
 *      verdicts -- ADMIT / DEFER / REJECT. "never" (reject) and "not now"
 *      (defer) are kept distinct and never collapse: a request that cannot fit
 *      even if every outstanding grant were released is a reject, not a defer.
 *      committed_bytes is subtracted from free space so concurrent planes cannot
 *      each be admitted against the same bytes.
 *
 *   2. A grant ledger on the SHARED resource_grants table, so a native grant is
 *      counted by the Python committed_bytes() and vice versa.
 *
 * Run 6 had no reserve and lost planes to ENOSPC; the protected floor is space
 * no grant may ever consume, so SQLite and the supervisor can still write.
 */

typedef struct BfAdmissionGrants BfAdmissionGrants;

typedef enum BfAdmissionVerdict {
    BF_ADMISSION_ADMIT = 0,
    BF_ADMISSION_DEFER,
    BF_ADMISSION_REJECT
} BfAdmissionVerdict;

typedef struct BfAdmissionPolicy {
    int64_t protected_floor_bytes; /* never consumed; 0 => default 10 GiB */
    int64_t per_plane_quota_bytes; /* 0 => default 20 GiB */
    int64_t max_grant_bytes;       /* single-grant ceiling; 0 => default 40 GiB */
} BfAdmissionPolicy;

typedef struct BfResourceRequest {
    const char *plane;      /* required */
    const char *kind;       /* required */
    int64_t estimated_bytes;
    const char *volume;     /* optional (default "/") */
    const char *detail;     /* optional (default "") */
} BfResourceRequest;

typedef struct BfAdmissionDecision {
    BfAdmissionVerdict verdict;
    char reason[256];
    int64_t free_bytes;
    int64_t committed_bytes;
    int64_t spendable_bytes; /* clamped at 0 */
} BfAdmissionDecision;

/* Fill a policy with the reference defaults (10/20/40 GiB). */
void bf_admission_policy_defaults(BfAdmissionPolicy *policy);

/*
 * Pure admission decision. No I/O -- free_bytes / committed_bytes /
 * plane_committed_bytes are supplied by the caller, exactly as the Python
 * decide() takes them, so the verdict is deterministic and testable. Returns
 * the verdict (also stored in out->verdict).
 */
BfAdmissionVerdict bf_admission_decide(const BfResourceRequest *request,
                                       const BfAdmissionPolicy *policy,
                                       int64_t free_bytes, int64_t committed_bytes,
                                       int64_t plane_committed_bytes,
                                       BfAdmissionDecision *out);

/* Free bytes on the volume holding `path` (statvfs). -1 on failure. */
int64_t bf_admission_disk_free(const char *path);

int bf_admission_open(const char *database_path, BfAdmissionGrants **out, FILE *err);
int bf_admission_open_database(void *sqlite_database, BfAdmissionGrants **out, FILE *err);
void bf_admission_close(BfAdmissionGrants *grants);

/* Sum of unreleased grant estimates on a volume; plane NULL = all planes. */
int64_t bf_admission_committed_bytes(BfAdmissionGrants *grants, const char *volume,
                                     const char *plane);

/*
 * Decide against the current ledger and, when admitted, record the grant in the
 * same connection. free_bytes is supplied by the caller (probe it with
 * bf_admission_disk_free, or inject for tests). On ADMIT, *out_grant_id is the
 * new grant id; otherwise it is 0. now may be NULL (=> now). Returns the verdict.
 */
BfAdmissionVerdict bf_admission_request_grant(BfAdmissionGrants *grants,
                                              const BfResourceRequest *request,
                                              const BfAdmissionPolicy *policy,
                                              int64_t free_bytes, const char *now,
                                              BfAdmissionDecision *out_decision,
                                              int64_t *out_grant_id);

/* Release a grant (idempotent: only affects an unreleased grant). Returns 0 on
 * success (whether or not a row changed), -1 on storage error. */
int bf_admission_release_grant(BfAdmissionGrants *grants, int64_t grant_id, const char *now);

/* Release grants granted before (now - older_than_seconds). Returns count released, -1 on error. */
int64_t bf_admission_reap_expired(BfAdmissionGrants *grants, int64_t older_than_seconds,
                                  const char *now);

const char *bf_admission_verdict_name(BfAdmissionVerdict verdict);

#endif
