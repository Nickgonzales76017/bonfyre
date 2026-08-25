/*
 * Capability identity versus callability.
 *
 * Ported from 10-Code/BonfyreControlPlane/capability_catalog.py. The ladder is
 * the one estate/catalog.yaff already uses for operators.
 */

#include "bf_control.h"

#include <string.h>

static const char *const kMaturityNames[] = {
    "defined",         "implemented",     "built",
    "installed",       "resolvable",      "version_aligned",
    "health_probed",   "activated",       "workload_proven",
    "quality_proven",  "promoted"
};

static const size_t kMaturityCount =
    sizeof(kMaturityNames) / sizeof(kMaturityNames[0]);

int bf_capability_callable(BfMaturity maturity)
{
    return (int)maturity >= (int)BF_MATURITY_CALLABLE_FLOOR;
}

int bf_capability_proven(BfMaturity maturity)
{
    return (int)maturity >= (int)BF_MATURITY_PROVEN_FLOOR;
}

BfMaturity bf_capability_apply_probe(BfMaturity maturity, int resolved)
{
    if (resolved) {
        /* Promote only into the callable floor; a name that already claims a
         * higher rung keeps whatever evidence earned it. */
        return bf_capability_callable(maturity) ? maturity : BF_MATURITY_RESOLVABLE;
    }
    /* A name that stops resolving must stop claiming to be callable. Assuming
     * installation is permanent is how five planes ended up probing PATH. */
    return bf_capability_callable(maturity) ? BF_MATURITY_IMPLEMENTED : maturity;
}

const char *bf_maturity_name(BfMaturity maturity)
{
    size_t index = (size_t)maturity;
    if (index >= kMaturityCount) return "unknown";
    return kMaturityNames[index];
}

int bf_maturity_from_name(const char *name, BfMaturity *out)
{
    if (name == NULL || out == NULL) return -1;
    for (size_t index = 0; index < kMaturityCount; index++) {
        if (strcmp(name, kMaturityNames[index]) == 0) {
            *out = (BfMaturity)index;
            return 0;
        }
    }
    return -1;
}
