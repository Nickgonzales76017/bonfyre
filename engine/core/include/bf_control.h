#ifndef BF_CONTROL_H
#define BF_CONTROL_H

/*
 * Control-plane primitives absorbed from the Run 6 postmortem.
 *
 * These are the semantics the Python reference in 10-Code/BonfyreControlPlane
 * pinned down, ported to the kernel. The reference stays as a conformance
 * oracle; this is the authority. Specifications live under schemas/control as
 * YaFF, and the replay vectors under tests/conformance/control.
 *
 * Deliberately absent: a work lifecycle. engine/core already owns one
 * (bf_workgraph.h) with missions, lease tokens, effect prepare/commit/
 * compensate and crash reconciliation. Adding a second would be the duplication
 * this absorption exists to stop.
 *
 * Everything here is a pure fold or a pure decision over caller-supplied
 * observations. No storage, no clock, no allocation.
 */

#include <stdint.h>
#include <stddef.h>

/* ------------------------------------------------------------------ provider
 *
 * Run 6 stored provider health as a mutable row, so a transient failure
 * overwrote Codex's hard-capacity window and the guardian relaunched into an
 * exhausted provider. State is a fold over append-only observations; there is
 * no write that could shorten a hard window.
 */

typedef enum BfProviderEventKind {
    BF_PROVIDER_SUCCESS = 0,
    BF_PROVIDER_TRANSIENT_FAILURE,
    BF_PROVIDER_HARD_CAPACITY,
    BF_PROVIDER_MANUAL_PAUSE,
    BF_PROVIDER_MANUAL_RESUME
} BfProviderEventKind;

typedef enum BfProviderStatus {
    BF_PROVIDER_READY = 0,
    BF_PROVIDER_COOLING,
    BF_PROVIDER_CAPACITY_EXHAUSTED,
    BF_PROVIDER_PAUSED
} BfProviderStatus;

typedef struct BfProviderObservation {
    BfProviderEventKind kind;
    int64_t observed_at_ms;
    /* Explicit reset carried by the provider message. 0 when absent, in which
     * case a hard-capacity window defaults to 24h -- longer than any transient
     * backoff, so it can never be undercut by one. */
    int64_t reset_at_ms;
} BfProviderObservation;

typedef struct BfProviderState {
    BfProviderStatus status;
    int64_t circuit_until_ms; /* 0 when no circuit is open */
    int32_t consecutive_failures;
    int32_t hard_capacity_hits;
    int64_t last_success_at_ms;
} BfProviderState;

/* Observations may arrive in any order; the fold sorts by observed_at_ms. */
BfProviderState bf_provider_fold(const BfProviderObservation *observations,
                                 size_t count, int64_t now_ms);
int bf_provider_available(const BfProviderState *state, int64_t now_ms);
const char *bf_provider_status_name(BfProviderStatus status);

/* Recover the reset instant a provider stated. Handles the two real formats:
 *   codex   "... or try again at Aug 19th, 2026 10:53 PM."   (ordinal suffix)
 *   iso     "resets 2026-08-19T22:53:00Z"
 * Returns 0 when nothing parseable is present. The wall-clock form Claude uses
 * ("resets 7:20pm (America/Chicago)") needs a timezone database and is resolved
 * by the caller, not here. */
int64_t bf_provider_parse_reset(const char *text);

/* Classify provider stderr. Returns 1 for hard capacity, 0 for transient. */
int bf_provider_is_hard_capacity(const char *text);

/* ----------------------------------------------------------------- admission
 *
 * Run 6's terminal failure was ENOSPC: planes filled the volume until SQLite,
 * transcripts, builds and the supervisor died together. A bare free-space check
 * is not enough because five planes each observe the same free bytes, so
 * outstanding grants are subtracted from what the next caller may see.
 */

typedef enum BfAdmissionVerdict {
    BF_ADMISSION_ADMIT = 0,
    BF_ADMISSION_DEFER,  /* waiting could help */
    BF_ADMISSION_REJECT  /* waiting could not help */
} BfAdmissionVerdict;

typedef struct BfAdmissionPolicy {
    int64_t protected_floor_bytes;
    int64_t per_plane_quota_bytes;
    int64_t max_grant_bytes;
} BfAdmissionPolicy;

typedef struct BfAdmissionRequest {
    int64_t estimated_bytes;
    int64_t free_bytes;
    int64_t committed_bytes;       /* outstanding grants on this volume */
    int64_t plane_committed_bytes; /* outstanding grants held by this plane */
} BfAdmissionRequest;

typedef struct BfAdmissionDecision {
    BfAdmissionVerdict verdict;
    int64_t spendable_bytes;
    char reason[192];
} BfAdmissionDecision;

BfAdmissionDecision bf_admission_decide(const BfAdmissionRequest *request,
                                        const BfAdmissionPolicy *policy);
const char *bf_admission_verdict_name(BfAdmissionVerdict verdict);

/* ----------------------------------------------------------------- attention
 *
 * Proton never obtained transport and consumed frontier attention on every pass
 * regardless. Attention is a property of a work node, not a second scheduler:
 * it cools to watcher-only after repeated unchanged checks, and reheats only on
 * a named condition.
 */

typedef enum BfAttention {
    BF_ATTENTION_HOT = 0,
    BF_ATTENTION_WARM,
    BF_ATTENTION_COOL,
    BF_ATTENTION_COLD
} BfAttention;

#define BF_ATTENTION_COOL_AFTER_UNCHANGED 3

typedef struct BfAttentionState {
    BfAttention level;
    int32_t unchanged_checks;
    int has_reheat_condition; /* reheat_at or reheat_on is set */
} BfAttentionState;

/* Fold one observation. `changed` resets to hot. Cooling requires a reheat
 * condition -- cooling without one is just forgetting, so such a node stays
 * warm instead. */
BfAttentionState bf_attention_check(BfAttentionState state, int changed);
/* Whether the subject belongs in the next context cut. */
int bf_attention_in_context_cut(const BfAttentionState *state);
const char *bf_attention_name(BfAttention level);

/* ---------------------------------------------------------------- capability
 *
 * Run 4 verified 91 public command identities while a native inventory reported
 * 0 compiled tools, and later planes probed PATH for names that never resolved.
 * Identity and callability are different facts. The ladder is the one already
 * used by estate/catalog.yaff.
 */

typedef enum BfMaturity {
    BF_MATURITY_DEFINED = 0,
    BF_MATURITY_IMPLEMENTED,
    BF_MATURITY_BUILT,
    BF_MATURITY_INSTALLED,
    BF_MATURITY_RESOLVABLE,
    BF_MATURITY_VERSION_ALIGNED,
    BF_MATURITY_HEALTH_PROBED,
    BF_MATURITY_ACTIVATED,
    BF_MATURITY_WORKLOAD_PROVEN,
    BF_MATURITY_QUALITY_PROVEN,
    BF_MATURITY_PROMOTED
} BfMaturity;

/* At or above: something exists to invoke. */
#define BF_MATURITY_CALLABLE_FLOOR BF_MATURITY_RESOLVABLE
/* At or above: there is evidence it does its job. `--help` working is four
 * rungs below this. */
#define BF_MATURITY_PROVEN_FLOOR BF_MATURITY_WORKLOAD_PROVEN

int bf_capability_callable(BfMaturity maturity);
int bf_capability_proven(BfMaturity maturity);
/* A name that stops resolving falls back below the callable floor. */
BfMaturity bf_capability_apply_probe(BfMaturity maturity, int resolved);
const char *bf_maturity_name(BfMaturity maturity);
int bf_maturity_from_name(const char *name, BfMaturity *out);

#endif
