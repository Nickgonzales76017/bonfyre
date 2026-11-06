#ifndef BF_ADMISSION_H
#define BF_ADMISSION_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

/*
 * Native resource-admission controller. Native peer of the Python reference
 * (components/BonfyreControlPlane/resource_admission.py.archive). Two parts:
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

/* An explicitly absent limit. Distinct from 0, which means "unset, use the
 * default" and is what a zeroed struct carries.
 *
 * The two were the same value and the CLI accepted both: passing
 * --protected-floor-bytes 0 set the field to 0, resolve_policy read 0 as unset,
 * and the caller silently got the 10 GiB default it had just asked to remove.
 * A 2 KiB representation was then refused against 1 MB of simulated free space
 * with no indication that the flag had been discarded. A field that cannot
 * express "none" cannot be told it. */
#define BF_ADMISSION_LIMIT_NONE ((int64_t)-1)

typedef struct BfAdmissionPolicy {
    /* Each: 0 => unset, take the default. A negative value (BF_ADMISSION_LIMIT_NONE)
     * => this limit is explicitly absent. Positive => that many bytes. */
    int64_t protected_floor_bytes; /* never consumed; default 10 GiB */
    int64_t per_plane_quota_bytes; /* default 20 GiB */
    int64_t max_grant_bytes;       /* single-grant ceiling; default 40 GiB */
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

/*
 * Memory is a volume too. A grant on BF_ADMISSION_MEMORY_VOLUME is a claim on
 * unified RAM, made before a model is loaded and released when it is unloaded,
 * so two models cannot each be admitted against the same gigabytes. The disk
 * defaults (10 GiB floor) make no sense for 16 GiB of RAM; memory has its own.
 *
 * A grant is a claim on bytes not yet taken. Once its process is resident the
 * bytes are gone from available memory, so the owner settles the grant: it
 * stays open (the ledger still says who holds what) but stops counting as
 * committed. Counting it twice refused a 700 MB publish pass beside a model
 * that was already loaded and measured.
 */
#define BF_ADMISSION_MEMORY_VOLUME "memory"
int bf_admission_volume_is_memory(const char *volume);
void bf_admission_policy_memory(BfAdmissionPolicy *policy);
/* Available memory in bytes; 0 under critical pressure; -1 if unknown. */
int64_t bf_admission_memory_available(void);
/* bf_admission_memory_available for the memory volume, statvfs otherwise. */
int64_t bf_admission_free(const char *volume);

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

/* Mark an open grant realized: still held, no longer committed. 0 / -1. */
int bf_admission_settle_grant(BfAdmissionGrants *grants, int64_t grant_id);

/* Release every open grant a plane holds; a restarted owner reclaims what its
 * crashed predecessor left behind. Returns the count released, -1 on error. */
int64_t bf_admission_release_plane(BfAdmissionGrants *grants, const char *plane, const char *now);

/* What a GGUF checkpoint costs in memory, read from its own metadata rather
 * than from a constant measured on one model. KV per token is
 * 2 x layers x kv_heads x head_dim x element size; compute is dominated by the
 * logits buffer, ubatch x vocabulary x 4 bytes. kv_element_bytes is 2.0 for an
 * f16 cache and 1.0625 for q8_0. */
typedef struct BfModelShape {
    char architecture[64];
    int64_t file_bytes;
    int64_t layers;
    int64_t kv_heads;
    int64_t head_dim;
    int64_t vocabulary;
    int64_t trained_context;
    int64_t kv_bytes_per_token;   /* at the element size asked for */
    int64_t compute_bytes;
    int chat;                     /* carries tokenizer.chat_template: it can hold a conversation */
} BfModelShape;

/* 0 on success; -1 when the file is not a readable GGUF. */
int bf_admission_model_shape(const char *gguf_path, double kv_element_bytes, BfModelShape *out);
/* file + compute + context x KV; -1 when the shape cannot be read. */
int64_t bf_admission_model_bytes(const BfModelShape *shape, int64_t context);
/* Largest context (a multiple of 1024, between min and max and no larger than
 * the trained one) whose total fits in spendable bytes; 0 if even min does not. */
int64_t bf_admission_model_context(const BfModelShape *shape, int64_t spendable, int64_t min_context, int64_t max_context);

const char *bf_admission_verdict_name(BfAdmissionVerdict verdict);

#endif
