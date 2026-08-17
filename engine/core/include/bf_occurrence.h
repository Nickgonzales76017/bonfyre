#ifndef BF_OCCURRENCE_H
#define BF_OCCURRENCE_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

/*
 * Native OccurrenceSpine -- external observations folded into an ordered,
 * append-only causal spine. This is the native peer of the Python reference in
 * 10-Code/BonfyreControlPlane/external_events.py: it writes the SAME
 * external_event_log table, with the SAME observation digest, so a native
 * writer and the Python writer dedup against each other on a shared store.
 *
 * Two clocks are kept structurally distinct and never collapse: observed_at is
 * event time (when the thing happened / was observed), recorded_at is ingest
 * time (when we noticed). The forbidden inference "retrieval time -> event
 * time" is enforced by separate columns, not convention.
 */

typedef struct BfOccurrenceSpine BfOccurrenceSpine;

typedef enum BfOccurrenceStatus {
    BF_OCCURRENCE_OK = 0,
    BF_OCCURRENCE_DUPLICATE,     /* this exact observation was already recorded */
    BF_OCCURRENCE_INVALID_KIND,  /* event_kind is not one of the declared kinds */
    BF_OCCURRENCE_INVALID,       /* a required field is null or empty */
    BF_OCCURRENCE_STORAGE_ERROR
} BfOccurrenceStatus;

typedef struct BfOccurrenceSpec {
    const char *source;       /* required: the watcher/channel that observed it */
    const char *actor;        /* required: who the observation is about */
    const char *event_kind;   /* required: one of the declared kinds */
    const char *subject_ref;  /* optional (default ""): what it concerns */
    const char *observed_at;  /* required: canonical ISO8601 event time */
    const char *payload_json; /* optional (default "{}") */
    const char *evidence_ref; /* optional (default "") */
    const char *recorded_at;  /* optional: ingest time; NULL => now */
} BfOccurrenceSpec;

/* Open (creating the external_event_log schema if absent). Returns 0 on success. */
int bf_occurrence_open(const char *database_path, BfOccurrenceSpine **out, FILE *err);
/* Adopt an already-open sqlite3* (not owned; not closed by bf_occurrence_close). */
int bf_occurrence_open_database(void *sqlite_database, BfOccurrenceSpine **out, FILE *err);
void bf_occurrence_close(BfOccurrenceSpine *spine);

/*
 * Record one observation. On BF_OCCURRENCE_OK, *out_id is the new row id. If the
 * same (source|actor|event_kind|subject_ref|observed_at) was already recorded,
 * returns BF_OCCURRENCE_DUPLICATE and sets *out_id to the existing row id -- no
 * second row is written. out_id may be NULL.
 */
BfOccurrenceStatus bf_occurrence_observe(BfOccurrenceSpine *spine,
                                         const BfOccurrenceSpec *spec, int64_t *out_id);

/*
 * The stable identity of an observation: the first 32 hex chars of
 * sha256("source|actor|event_kind|subject_ref|observed_at"). Byte-identical to
 * the Python reference's digest_for(), which is what makes cross-writer dedup
 * work. out_digest must hold 33 bytes (32 hex + NUL).
 */
void bf_occurrence_digest(const char *source, const char *actor, const char *event_kind,
                          const char *subject_ref, const char *observed_at,
                          char out_digest[33]);

/* Canonical UTC now in the Python-parity form YYYY-MM-DDTHH:MM:SS+00:00. */
void bf_occurrence_now_iso(char out[40]);

/*
 * Correct/retract a prior occurrence. Records `correction` as a new observation
 * and links it as superseding `original_event_id` -- the -1 retraction: the
 * original's stale projection is withdrawn and the correction's stands instead.
 * append-only and auditable (nothing is deleted; the supersession is recorded).
 * On success sets *out_correction_id to the correction's id. Returns OK, or
 * INVALID if the original does not exist / the correction is malformed.
 */
BfOccurrenceStatus bf_occurrence_correct(BfOccurrenceSpine *spine, int64_t original_event_id,
                                         const BfOccurrenceSpec *correction,
                                         const char *reason, int64_t *out_correction_id);

/* Has this occurrence been superseded by a correction? 1 / 0 / -1. */
int bf_occurrence_is_superseded(BfOccurrenceSpine *spine, int64_t event_id);

/* Count of occurrences not yet folded into downstream state (projected_at IS NULL). */
int64_t bf_occurrence_unprojected_count(BfOccurrenceSpine *spine);

/* Fold one occurrence out of the pending set. now may be NULL (=> now). */
BfOccurrenceStatus bf_occurrence_mark_projected(BfOccurrenceSpine *spine, int64_t event_id,
                                                const char *now);

/*
 * The campaign status one occurrence kind projects to (declined -> "declined",
 * inbound_reply -> "replied", outbound_sent -> "sent", ...). Returns NULL for an
 * undeclared kind. Every declared kind maps; bf_occurrence_projection_is_total()
 * verifies that, so a kind can never be silently dropped by the fold.
 */
const char *bf_occurrence_status_for_kind(const char *event_kind);
int bf_occurrence_projection_is_total(void);

/*
 * Fold every pending occurrence into the shared occurrence_projection table --
 * one durable (actor, kind, status) row per occurrence -- then mark it
 * projected. Re-runnable: a row is marked projected only after its projection
 * write, so a crash mid-fold replays rather than skips. Returns the count folded
 * (>=0), or -1 on storage error. now may be NULL.
 */
int64_t bf_occurrence_project(BfOccurrenceSpine *spine, const char *now);

const char *bf_occurrence_status_name(BfOccurrenceStatus status);

#endif
