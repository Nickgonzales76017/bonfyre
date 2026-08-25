#ifndef BF_AUTHORITY_H
#define BF_AUTHORITY_H

#include <stdint.h>
#include <stdio.h>

/*
 * Native authority grants -- native peer of the grant/has_authority half of the
 * Python reference (10-Code/BonfyreControlPlane/authority.py). Writes the SAME
 * authority_edges store. (The bit-mask / CID forwarding in authority.py is a
 * separate AddressPlane concern; this owns the grants fact.)
 *
 * Authority is NEVER inferred: an actor holds a permission over a subject only
 * if a non-revoked edge matches actor+permission+subject, its time window
 * contains the moment, and -- when a purpose is asked -- the edge's purpose is
 * general (empty) or matches. Affiliation, similarity, provenance: none of them
 * grant authority.
 */

typedef struct BfAuthority BfAuthority;

typedef struct BfAuthorityEdge {
    const char *edge_id;     /* required */
    const char *actor;       /* required */
    const char *permission;  /* required */
    const char *subject;     /* required */
    const char *purpose;     /* optional (default "" = general) */
    const char *scope;       /* optional */
    const char *effective_from; /* optional ISO8601; NULL = no lower bound */
    const char *expires_at;  /* optional ISO8601; NULL = no upper bound */
    int delegable;
    const char *evidence;    /* optional */
    const char *originating_authority; /* optional */
} BfAuthorityEdge;

int bf_authority_open(const char *database_path, BfAuthority **out, FILE *err);
int bf_authority_open_database(void *sqlite_database, BfAuthority **out, FILE *err);
void bf_authority_close(BfAuthority *auth);

/* Record an authority edge (grant). Returns 0 on success, -1 on error. */
int bf_authority_grant(BfAuthority *auth, const BfAuthorityEdge *edge, const char *now);

/* Revoke an edge by id. Returns 1 if an edge was revoked, 0 if none, -1 error. */
int bf_authority_revoke(BfAuthority *auth, const char *edge_id);

/*
 * Does `actor` hold `permission` over `subject` at `at_iso` (NULL => now), for
 * `purpose` (NULL => any)? Returns 1 yes, 0 no, -1 error. True only from a
 * matching non-revoked edge whose window contains the moment.
 */
int bf_authority_has(BfAuthority *auth, const char *actor, const char *permission,
                     const char *subject, const char *at_iso, const char *purpose);

#endif
