#ifndef BF_RELATIONSHIP_H
#define BF_RELATIONSHIP_H

/*
 * RelationshipGraph: where a real relationship sits on the trajectory
 * discovered < observed < contacted < replied < engaged < assigned < reviewed
 * < corrected < collaborated < committed < adopted < converted.  A stage is
 * reached only when recorded, with evidence; a move backward is refused unless
 * explicitly allowed; relationships under different profiles are independent.
 *
 * ShadowInstitutionGraph: the public picture of an institution before any
 * relationship or authority.  Its conversion stage (observed, shadow,
 * relationship_established, active) is decided against the RelationshipGraph,
 * never by a richer public picture.
 *
 * Ported from components/BonfyreControlPlane/{relationship,shadow_institution}.py.archive
 * (same tables).  now: ISO-8601 UTC, or NULL for the current time.
 */
#include <stddef.h>
#include <stdint.h>

int bf_relationship_open(void *sqlite_database);
int bf_relationship_stage_rank(const char *stage); /* -1 when undeclared */
/* 0 ok, -1 invalid or undeclared stage / storage. */
int bf_relationship_record(void *db, const char *rel_id, const char *actor, const char *profile,
                           const char *stage, const char *evidence, const char *now);
/* 1 moved, 0 refused (unknown relationship, or backward without allow_regress), -1 error. */
int bf_relationship_advance(void *db, const char *rel_id, const char *to_stage, const char *evidence,
                            const char *now, int allow_regress);
/* 1 reached, 0 not, -2 unknown (no relationship recorded: never assumed), -1 error. */
int bf_relationship_stage_at_least(void *db, const char *actor, const char *profile, const char *stage);
int64_t bf_relationship_transition_count(void *db, const char *rel_id);

/* attachments_json / hierarchy_json: JSON arrays. */
int bf_shadow_record(void *db, const char *inst_id, const char *name, const char *contact_actor,
                     const char *profile, const char *hierarchy_json, const char *attachments_json,
                     const char *evidence, const char *now);
/* Writes "observed" | "shadow" | "relationship_established" | "active" and a reason. 0 ok, -1 error. */
int bf_shadow_conversion_stage(void *db, const char *inst_id, char *stage, size_t stage_cap, char *detail, size_t detail_cap);

#endif
