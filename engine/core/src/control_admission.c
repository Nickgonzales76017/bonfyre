/*
 * Resource admission, decided before work starts rather than during.
 *
 * Ported from 10-Code/BonfyreControlPlane/resource_admission.py. Conformance
 * vectors: tests/conformance/control/vectors/admission.
 */

#include "bf_control.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static void human_size(int64_t bytes, char *out, size_t out_size)
{
    static const char *const units[] = { "B", "KiB", "MiB", "GiB", "TiB" };
    double value = (double)bytes;
    size_t unit = 0;
    while ((value >= 1024.0 || value <= -1024.0) && unit + 1 < 5) {
        value /= 1024.0;
        unit++;
    }
    if (unit == 0) {
        snprintf(out, out_size, "%" PRId64 "B", bytes);
    } else {
        snprintf(out, out_size, "%.1f%s", value, units[unit]);
    }
}

BfAdmissionDecision bf_admission_decide(const BfAdmissionRequest *request,
                                        const BfAdmissionPolicy *policy)
{
    BfAdmissionDecision decision;
    memset(&decision, 0, sizeof(decision));

    if (request == NULL || policy == NULL) {
        decision.verdict = BF_ADMISSION_REJECT;
        snprintf(decision.reason, sizeof(decision.reason), "missing request or policy");
        return decision;
    }

    /* Subtracting outstanding grants is what stops concurrent planes from each
     * being admitted against the same free bytes. */
    const int64_t spendable =
        request->free_bytes - policy->protected_floor_bytes - request->committed_bytes;
    decision.spendable_bytes = spendable > 0 ? spendable : 0;

    char wanted[32];
    char available[32];
    char floor[32];
    human_size(request->estimated_bytes, wanted, sizeof(wanted));
    human_size(decision.spendable_bytes, available, sizeof(available));
    human_size(policy->protected_floor_bytes, floor, sizeof(floor));

    if (request->estimated_bytes <= 0) {
        decision.verdict = BF_ADMISSION_REJECT;
        snprintf(decision.reason, sizeof(decision.reason),
                 "request must carry a positive size estimate");
        return decision;
    }

    if (request->estimated_bytes > policy->max_grant_bytes) {
        char ceiling[32];
        human_size(policy->max_grant_bytes, ceiling, sizeof(ceiling));
        decision.verdict = BF_ADMISSION_REJECT;
        snprintf(decision.reason, sizeof(decision.reason),
                 "request of %s exceeds the %s single-grant ceiling", wanted, ceiling);
        return decision;
    }

    if (request->plane_committed_bytes + request->estimated_bytes >
        policy->per_plane_quota_bytes) {
        char quota[32];
        human_size(policy->per_plane_quota_bytes, quota, sizeof(quota));
        decision.verdict = BF_ADMISSION_DEFER;
        snprintf(decision.reason, sizeof(decision.reason),
                 "plane would exceed its %s quota", quota);
        return decision;
    }

    if (request->estimated_bytes > spendable) {
        /* Distinguish "not now" from "never": if the request could not fit even
         * with every outstanding grant released, waiting is pointless. */
        const int64_t drained = request->free_bytes - policy->protected_floor_bytes;
        if (request->estimated_bytes > drained) {
            decision.verdict = BF_ADMISSION_REJECT;
            snprintf(decision.reason, sizeof(decision.reason),
                     "%s cannot fit above the %s protected floor even if every "
                     "outstanding grant were released",
                     wanted, floor);
        } else {
            decision.verdict = BF_ADMISSION_DEFER;
            snprintf(decision.reason, sizeof(decision.reason),
                     "%s exceeds %s spendable", wanted, available);
        }
        return decision;
    }

    decision.verdict = BF_ADMISSION_ADMIT;
    snprintf(decision.reason, sizeof(decision.reason),
             "%s spendable above the protected floor", available);
    return decision;
}

const char *bf_admission_verdict_name(BfAdmissionVerdict verdict)
{
    switch (verdict) {
    case BF_ADMISSION_ADMIT: return "admit";
    case BF_ADMISSION_DEFER: return "defer";
    case BF_ADMISSION_REJECT: return "reject";
    }
    return "unknown";
}
