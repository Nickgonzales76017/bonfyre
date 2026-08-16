#ifndef BF_ACTOR_H
#define BF_ACTOR_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

/*
 * Native ActorGraph -- people, organizations, publications, programs and the
 * typed relationships between them. Native peer of the Python reference
 * (10-Code/BonfyreControlPlane/actors.py): it writes the SAME actor_nodes /
 * actor_edges tables, so native and Python share one social/authority graph.
 *
 * The load-bearing invariant is epistemic honesty. Every node and edge carries
 * a mandatory provenance and a confidence tier that is never collapsed:
 *   verified  -- a human confirmed it
 *   asserted  -- someone said it (unconfirmed)
 *   inferred  -- a model concluded it, unchecked
 * "inferred" must never be read as "verified"; unverified() is what outreach
 * reads before acting on a relationship.
 */

typedef struct BfActorGraph BfActorGraph;

typedef enum BfActorStatus {
    BF_ACTOR_OK = 0,
    BF_ACTOR_INVALID_KIND,        /* node_kind / edge_kind not declared */
    BF_ACTOR_INVALID_CONFIDENCE,  /* confidence tier not declared */
    BF_ACTOR_MISSING_PROVENANCE,  /* provenance is mandatory */
    BF_ACTOR_UNKNOWN_ENDPOINT,    /* an edge references an actor that doesn't exist */
    BF_ACTOR_INVALID,             /* a required id field is null or empty */
    BF_ACTOR_STORAGE_ERROR
} BfActorStatus;

typedef struct BfActorNodeSpec {
    const char *actor_id;      /* required */
    const char *node_kind;     /* required: organization|person|publication|program */
    const char *display_name;  /* required */
    const char *role;          /* optional (default "") */
    const char *org_id;        /* optional (NULL allowed) */
    const char *confidence;    /* optional (default "asserted"): verified|asserted|inferred */
    const char *provenance;    /* required: how we know this */
    const char *detail;        /* optional (default "") */
} BfActorNodeSpec;

typedef struct BfActorEdgeSpec {
    const char *from_id;    /* required, must exist */
    const char *edge_kind;  /* required: employs|authority_over|evidence_for|funds|
                             *           publishes_in|contact_of|opportunity_unlock */
    const char *to_id;      /* required, must exist */
    const char *confidence; /* optional (default "asserted") */
    const char *provenance; /* required */
    const char *detail;     /* optional (default "") */
} BfActorEdgeSpec;

int bf_actor_open(const char *database_path, BfActorGraph **out, FILE *err);
int bf_actor_open_database(void *sqlite_database, BfActorGraph **out, FILE *err);
void bf_actor_close(BfActorGraph *graph);

/* Insert or update an actor by actor_id (upsert). now may be NULL (=> now). */
BfActorStatus bf_actor_upsert(BfActorGraph *graph, const BfActorNodeSpec *spec, const char *now);

/* Insert or update a typed edge, keyed (from_id, edge_kind, to_id). Both
 * endpoints must already exist, or BF_ACTOR_UNKNOWN_ENDPOINT. now may be NULL. */
BfActorStatus bf_actor_add_edge(BfActorGraph *graph, const BfActorEdgeSpec *spec, const char *now);

/* Count of actors a human has not confirmed (confidence != 'verified'). */
int64_t bf_actor_unverified_count(BfActorGraph *graph);

/* Count of edges touching actor_id (as from or to); edge_kind NULL = any kind. */
int64_t bf_actor_neighbour_count(BfActorGraph *graph, const char *actor_id, const char *edge_kind);

const char *bf_actor_status_name(BfActorStatus status);

#endif
