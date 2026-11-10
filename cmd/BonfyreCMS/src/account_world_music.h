#ifndef BONFYRE_ACCOUNT_WORLD_MUSIC_H
#define BONFYRE_ACCOUNT_WORLD_MUSIC_H
#include <sqlite3.h>

/* account-world-music-1: artists, labels, releases and listening rooms on an
 * account space. Additions to the account-world owner, beside the sound
 * additions (account_world_audio.h), whose time marks and playback gate the
 * music reader reuses instead of keeping its own.
 *
 * Four identities stay apart, after MusicBrainz:
 *   work       the composition: title, ISWC, writers' credits
 *   recording  one performance: title, ISRC, performers' credits, and the
 *              PageComposition piece ("<source>/<24 hex key>") that is its
 *              audio; the audio file, its source and its terms are never
 *              stored here and are re-read from the source on every request
 *   release    a published product: type, date, label, catalogue number,
 *              cover piece, liner notes, release credits, and an edition_of
 *              link to the release it reissues
 *   track      a position (disc, number) on a release pointing at a recording
 * Rights are not a row: whether a recording plays is decided by its source's
 * stated terms on each read (world_web_open_terms).
 *
 * Everything is a draft until the space is released; release and withdrawal
 * copy or drop the public versions inside the owner's own transaction
 * (triggers on account_world_release). A release reaches readers only once
 * its owner marks it ready; any change to it, its tracks or its credits makes
 * it a draft again. Listening rooms are drafts until announced. Same contract
 * as account_world.h: the caller authenticates. Return 1 success, 0 refused or
 * conflict, -1 storage error. Owner mutations need the exact current space
 * revision and advance it. */
#define AWM_WORK_LIMIT 400
#define AWM_RECORDING_LIMIT 400
#define AWM_RELEASE_LIMIT 64
#define AWM_TRACK_LIMIT 200
#define AWM_CREDIT_LIMIT 2000
#define AWM_RELATION_LIMIT 64
#define AWM_ROOM_LIMIT 48
#define AWM_NOTES_LIMIT 4000
#define AWM_COLLECTION_LIMIT 2000

enum { AWM_PARTY, AWM_WORKS, AWM_RECORDINGS, AWM_RELEASES, AWM_TRACKS, AWM_CREDITS, AWM_RELATIONS, AWM_ROOMS };

int awm_initialize(sqlite3 *db);
/* kind artist | label. mbid "" or a MusicBrainz UUID. begun "" or YYYY. */
int awm_party_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                  const char *kind, const char *name, const char *mbid, const char *area, const char *begun,
                  int expected_revision);
/* iswc "" or T-123.456.789-0 (separators optional). */
int awm_work_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                 const char *work_id, const char *title, const char *iswc, const char *mbid, int expected_revision);
/* work_id "" or an existing work of the space; ref a piece reference;
 * isrc "" or CC-XXX-YY-NNNNN without separators (12 characters). */
int awm_recording_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                      const char *recording_id, const char *work_id, const char *title, const char *ref,
                      const char *isrc, const char *mbid, int expected_revision);
/* type album | ep | single | compilation | live | soundtrack | other.
 * released_on "" | YYYY | YYYY-MM | YYYY-MM-DD. label "" , a name, or
 * "space:<id>" for a label space in the same World. cover "" or a piece.
 * edition_of "" or another release of this space. Saving makes it a draft. */
int awm_release_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                    const char *release_id, const char *title, const char *type, const char *released_on,
                    const char *label, const char *catalog, const char *cover_ref, const char *notes,
                    const char *edition_of, const char *mbid, int expected_revision);
/* ready 1 needs at least one track. */
int awm_release_ready(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                      const char *release_id, int ready, int expected_revision);
/* disc 1..9, position 1..99; recording must exist. */
int awm_track_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                  const char *release_id, int disc, int position, const char *recording_id, int expected_revision);
int awm_track_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                     const char *release_id, int disc, int position, int expected_revision);
/* target work | recording | release. Roles are per target, so a writer can
 * only be credited on a work and a performer only on a recording:
 *  work:      composer lyricist songwriter arranger translator publisher
 *  recording: performer vocals instrument producer engineer mixing remixer conductor featured
 *  release:   artwork design photography liner-notes mastering executive-producer
 * detail: optional words such as "piano". href "" , https or "space:<id>". */
int awm_credit_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                   const char *target, const char *target_id, const char *role, const char *name,
                   const char *detail, const char *href, int expected_revision);
int awm_credit_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                      sqlite3_int64 credit_id, int expected_revision);
/* kind member-of | founded | signed-to | collaborated-with | split-from |
 * also-known-as | influenced-by. begun, ended "" or YYYY (ended >= begun). */
int awm_relation_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                     const char *kind, const char *name, const char *href, const char *begun, const char *ended,
                     int expected_revision);
int awm_relation_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                        sqlite3_int64 relation_id, int expected_revision);
/* Listening room: starts "YYYY-MM-DDTHH:MM" in UTC; target release |
 * recording of this space; ticket "" or https. Saving makes it a draft. */
int awm_room_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                 const char *room_id, const char *starts, const char *target, const char *target_id,
                 const char *note, const char *ticket, int expected_revision);
/* state announced | cancelled | draft */
int awm_room_state(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                   const char *room_id, const char *state, int expected_revision);
/* what: work | recording | release | room. A work a recording uses, a
 * recording on a track or a room, or a release another one reissues or a
 * room plays, is refused. A release goes with its tracks and its credits. */
int awm_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
               const char *what, const char *id, int expected_revision);
/* subject non-NULL: the owner's draft. subject NULL: the released public
 * version of a public space in a bound live World, holding only ready
 * releases and what they reach (their tracks, recordings, works and credits)
 * and announced or cancelled rooms on them. key narrows: the id for works,
 * recordings, releases and rooms; the release for tracks; "target:id" for
 * credits.
 *  AWM_PARTY:      kind, name, mbid, area, begun
 *  AWM_WORKS:      work_id, title, iswc, mbid
 *  AWM_RECORDINGS: recording_id, work_id, title, ref, isrc, mbid
 *  AWM_RELEASES:   release_id, title, type, released_on, label, catalog, cover_ref, notes, edition_of, mbid, ready
 *  AWM_TRACKS:     release_id, disc, position, recording_id
 *  AWM_CREDITS:    id, target, target_id, role, name, detail, href
 *  AWM_RELATIONS:  id, kind, name, href, begun, ended
 *  AWM_ROOMS:      room_id, starts, target, target_id, note, ticket, state */
int awm_rows(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
             int table, const char *key, sqlite3_stmt **rows);

/* Readers across spaces of one public World.
 * awm_label_releases: ready releases of other public spaces naming
 *   "space:<label_space>" as label: space_id, release_id, title, type, released_on, cover_ref.
 * awm_heard_on: where a recording's piece is heard in this World's public
 *   spaces: space_id, title, medium, how ("played" for a sound track mark,
 *   "holds" for an item), start_ms, mark ref (the episode piece, or ""). */
int awm_label_releases(sqlite3 *db, const char *ns, sqlite3_int64 world_id, const char *label_space, sqlite3_stmt **rows);
int awm_heard_on(sqlite3 *db, const char *ns, sqlite3_int64 world_id, const char *ref, sqlite3_stmt **rows);

/* Fan collections: any authenticated subject keeps public ready releases.
 * Collecting needs the release public now; the collection keeps the
 * reference and shows it as gone once the release leaves the public. */
int awm_collect(sqlite3 *db, const char *member, const char *ns, const char *space_id, const char *release_id);
int awm_uncollect(sqlite3 *db, const char *member, const char *ns, const char *space_id, const char *release_id);
/* 1 when collected, 0 when not */
int awm_collected(sqlite3 *db, const char *member, const char *ns, const char *space_id, const char *release_id);
/* the member's own: namespace, space_id, release_id, title, type, released_on, collected, public(1/0) */
int awm_collection(sqlite3 *db, const char *member, sqlite3_stmt **rows);
/* owner analytics on its own draft space: release_id, collectors */
int awm_collectors(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, const char *space_id,
                   sqlite3_stmt **rows);
#endif
