#ifndef BF_SERVICE_LIFECYCLE_H
#define BF_SERVICE_LIFECYCLE_H

#include <stdio.h>

#include "bf_authority.h"

/*
 * Native service-lifecycle binding owner.
 *
 * A binding says that a named actor has deliberately connected a resource to
 * a named provisionable service.  It does not assert that a remote service is
 * healthy, that it has performed work, or that the actor may activate the
 * resource.  Those are separate owners.  This owner enforces the one fact it
 * owns: a live binding requires an independently-held `bind` grant.
 */

typedef struct BfServiceLifecycle BfServiceLifecycle;

typedef struct BfServiceBinding {
    const char *resource_id; /* required */
    const char *service_id;  /* required */
    const char *actor;       /* required */
    const char *evidence;    /* required: receipt/reference for the binding */
} BfServiceBinding;

int bf_service_lifecycle_open(const char *database_path, BfServiceLifecycle **out,
                              FILE *err);
int bf_service_lifecycle_open_database(void *sqlite_database,
                                       BfServiceLifecycle **out, FILE *err);
void bf_service_lifecycle_close(BfServiceLifecycle *lifecycle);

/*
 * Record a binding only when AuthorityGraph says `actor` currently has `bind`
 * over `service_id` for the `service-binding` purpose.  No grant is created.
 */
int bf_service_lifecycle_bind(BfServiceLifecycle *lifecycle, BfAuthority *authority,
                              const BfServiceBinding *binding, const char *at_iso);

/* Release is idempotent.  It changes only this lifecycle's binding state. */
int bf_service_lifecycle_release(BfServiceLifecycle *lifecycle, const char *resource_id,
                                 const char *actor, const char *at_iso);

/* True only for a current bound row made by this exact actor. */
int bf_service_lifecycle_is_bound(BfServiceLifecycle *lifecycle,
                                  const char *resource_id, const char *actor);

#endif
