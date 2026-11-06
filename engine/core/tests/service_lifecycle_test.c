/* A usable ResourceActivation must be joined from three independent owners:
 * DemandBook eligibility, AuthorityGraph's activate grant, and a lawful service
 * binding. Neither authority nor a binding may be inferred from the other. */

#include "bf_activation.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *message) { fprintf(stderr, "FAIL: %s\n", message); return 1; }

int main(void) {
    char database[] = "/tmp/bf_service_lifecycle_XXXXXX";
    int fd = mkstemp(database);
    BfAuthority *authority = NULL;
    BfActivation *activation = NULL;
    BfServiceLifecycle *lifecycle = NULL;
    BfAuthorityEdge grant = {0};
    BfActivationCandidate candidate = {0};
    BfServiceBinding binding = {0};
    const char *actor = "person:operator";
    const char *resource = "external:service:search";
    const char *service = "service:search-provider";
    if (fd >= 0) close(fd);
    unlink(database);
    if (bf_authority_open(database, &authority, stderr) ||
        bf_activation_open(database, &activation, stderr) ||
        bf_service_lifecycle_open(database, &lifecycle, stderr)) return fail("open owners");
    candidate.resource_id = resource; candidate.mechanism = "service";
    candidate.qualified = 1; candidate.eligible = 1; candidate.activate_actor = actor;
    if (bf_activation_record(activation, &candidate, "2026-09-21T00:00:00+00:00")) return fail("candidate");
    if (bf_activation_reconcile(activation, authority, lifecycle, resource,
                                "2026-09-21T00:00:00+00:00") != BF_ACT_ELIGIBLE) return fail("eligibility is not authority");
    binding.resource_id = resource; binding.service_id = service; binding.actor = actor; binding.evidence = "receipt:bind";
    if (bf_service_lifecycle_bind(lifecycle, authority, &binding,
                                  "2026-09-21T00:00:00+00:00") != 0) return fail("bind must be refused without bind grant");
    grant.edge_id = "grant:bind"; grant.actor = actor; grant.permission = "bind"; grant.subject = service;
    if (bf_authority_grant(authority, &grant, NULL)) return fail("bind grant");
    if (bf_service_lifecycle_bind(lifecycle, authority, &binding,
                                  "2026-09-21T00:00:00+00:00") != 1) return fail("lawful bind");
    if (bf_activation_reconcile(activation, authority, lifecycle, resource,
                                "2026-09-21T00:00:00+00:00") != BF_ACT_ELIGIBLE) return fail("binding is not activate authority");
    grant.edge_id = "grant:activate"; grant.permission = "activate"; grant.subject = resource;
    if (bf_authority_grant(authority, &grant, NULL)) return fail("activate grant");
    if (bf_activation_reconcile(activation, authority, lifecycle, resource,
                                "2026-09-21T00:00:00+00:00") != BF_ACT_ACTIVATED) return fail("three owner join activates");
    if (bf_authority_revoke(authority, "grant:activate") != 1 ||
        bf_activation_reconcile(activation, authority, lifecycle, resource,
                                "2026-09-21T00:00:00+00:00") != BF_ACT_ELIGIBLE) return fail("revocation retracts activation");
    if (bf_service_lifecycle_release(lifecycle, resource, actor, "2026-09-21T00:01:00+00:00") != 1 ||
        bf_service_lifecycle_release(lifecycle, resource, actor, "2026-09-21T00:01:00+00:00") != 0) return fail("release idempotence");
    bf_service_lifecycle_close(lifecycle); bf_activation_close(activation); bf_authority_close(authority);
    unlink(database);
    puts("service_lifecycle_test: PASS");
    return 0;
}
