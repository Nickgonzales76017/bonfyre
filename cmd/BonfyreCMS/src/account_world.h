#ifndef BONFYRE_ACCOUNT_WORLD_H
#define BONFYRE_ACCOUNT_WORLD_H
#include <sqlite3.h>
#include <stddef.h>

/* Internal owner API: caller must authenticate subject and authorize namespace.
 * These functions cannot authenticate an argument or publish any material. */
#define AW_SPACE_LIMIT 32
#define AW_TEXT_LIMIT 256
int aw_initialize(sqlite3 *db);
int aw_bind(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id);
int aw_space_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, const char *title, const char *renderer,
                 const char *template_ref, const char *layout_ref, int expected_revision);
int aw_space_get(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, sqlite3_stmt **row);
int aw_space_list(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, sqlite3_stmt **rows);

/* account-world-2: what a space shows and when the public may see it.
 * An item names material by reference only: "<source>/<24 hex key>" for a
 * PageComposition item, or "space:<id>" for a portal to a sibling space.
 * The owner never copies or resolves material and never states its rights;
 * the reader re-resolves each reference against its source on every read. */
#define AW_ITEM_LIMIT 24
#define AW_NOTE_LIMIT 280
int aw_space_style(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                   const char *space_id, const char *medium, const char *character,
                   const char *lede, int expected_revision);
int aw_item_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                const char *space_id, int position, const char *ref, const char *role,
                const char *note, int expected_revision);
int aw_item_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                   const char *space_id, int position, int expected_revision);
/* Draft view: medium, character, lede, release state, released revision. */
int aw_space_detail(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, sqlite3_stmt **row);
/* Draft items: position, ref, role, note. */
int aw_items(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
             const char *space_id, sqlite3_stmt **rows);
/* Release copies the current draft (style and items) as the public version
 * at exactly expected_revision; withdraw removes that version from readers. */
int aw_space_release(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                     const char *space_id, int expected_revision);
int aw_space_withdraw(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                      const char *space_id);
/* Anonymous readers. They see only released versions in a bound World that
 * still carries its binding subject and namespace. The caller still owns
 * host, site and Matrix presence checks.
 * aw_public_space: title, renderer, medium, character, lede, state, world_id, revision.
 * aw_public_items: position, ref, role, note.
 * aw_public_spaces: space_id, title, medium, character, lede (public only). */
int aw_public_space(sqlite3 *db, const char *ns, const char *space_id, sqlite3_stmt **row);
int aw_public_items(sqlite3 *db, const char *ns, sqlite3_int64 world_id, const char *space_id, sqlite3_stmt **rows);
int aw_public_spaces(sqlite3 *db, const char *ns, sqlite3_stmt **rows);

/* account-world-cine: screenings, a film club and a watch diary on a space.
 * Every film is a PageComposition reference ("<source>/<24 hex key>"); the
 * owner never stores a media URL or a rights statement. A screening plays
 * only where its own source states public domain or CC0, and the reader
 * decides that on each request. Called by aw_initialize. */
#define AW_SCREENING_LIMIT 48
#define AW_CLUB_PAGE 20
int aw_cine_initialize(sqlite3 *db);
/* Owner: programme. starts "YYYY-MM-DDTHH:MM"; a new or edited screening is
 * a draft until announced; cancelling a draft deletes it, cancelling an
 * announced one keeps it on the programme marked cancelled. */
int aw_screening_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                     const char *space_id, const char *screening_id, const char *starts,
                     const char *venue, const char *ref, const char *note);
int aw_screening_announce(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                          const char *space_id, const char *screening_id);
int aw_screening_cancel(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                        const char *space_id, const char *screening_id);
/* screening_id, starts, venue, ref, note, state */
int aw_screenings(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                  const char *space_id, sqlite3_stmt **rows);
/* announced and cancelled screenings of a public space only */
int aw_public_screenings(sqlite3 *db, const char *ns, sqlite3_int64 world_id, const char *space_id, sqlite3_stmt **rows);

/* Club on a public space. Members are other authenticated subjects; the
 * World's own subject is the host. Picks are member proposals with one vote
 * per member; posts are member-only discussion the host can hide and the
 * author can retract. A removed member cannot rejoin. */
int aw_club_set(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                const char *space_id, int open);
int aw_club_join(sqlite3 *db, const char *member, const char *ns, const char *space_id);
int aw_club_leave(sqlite3 *db, const char *member, const char *ns, const char *space_id);
int aw_club_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                   const char *space_id, const char *member);
int aw_club_pick(sqlite3 *db, const char *member, const char *ns, const char *space_id,
                 const char *ref, const char *note);
int aw_club_vote(sqlite3 *db, const char *member, const char *ns, const char *space_id, sqlite3_int64 pick_id);
int aw_club_say(sqlite3 *db, const char *member, const char *ns, const char *space_id,
                const char *ref, const char *body);
int aw_club_retract(sqlite3 *db, const char *member, const char *ns, const char *space_id, sqlite3_int64 post_id);
/* kind "post" or "pick" */
int aw_club_hide(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, const char *kind, sqlite3_int64 item_id);
/* club state on a draft space for its owner: "open", "closed" or "" */
int aw_club_state(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                  const char *space_id, char out[8]);
/* Any authenticated viewer of a public space with an open club:
 * world_id, title, character, lede, viewer role (host, member, left, removed or ""). */
int aw_club_view(sqlite3 *db, const char *viewer, const char *ns, const char *space_id, sqlite3_stmt **row);
/* anonymous: 1 when a public space has an open club */
int aw_public_club(sqlite3 *db, const char *ns, const char *space_id);
/* Members and host only. picks: pick_id, ref, note, proposed_by, votes, state, voted.
 * posts newest first, older than before (0 = newest), at most AW_CLUB_PAGE+1:
 * post_id, author, ref, body, state. Members never see hidden rows. */
int aw_club_picks(sqlite3 *db, const char *viewer, const char *ns, const char *space_id, sqlite3_stmt **rows);
int aw_club_posts(sqlite3 *db, const char *viewer, const char *ns, const char *space_id,
                  sqlite3_int64 before, sqlite3_stmt **rows);

/* Watch diary of the World's subject. watched "YYYY-MM-DD", rating 0..10
 * (half stars; 0 = unrated), visibility "private" or "public". note_id 0
 * creates. A public note shows only beside a film in a public space. */
int aw_note_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                sqlite3_int64 note_id, const char *ref, const char *watched, int rating,
                int rewatch, const char *body, const char *visibility);
int aw_note_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id, sqlite3_int64 note_id);
/* note_id, ref, watched, rating, rewatch, body, visibility; newest watched
 * first, strictly before (before_day, before_id) when before_id > 0, at most
 * AW_CLUB_PAGE+1 rows. */
int aw_notes(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
             const char *before_day, sqlite3_int64 before_id, sqlite3_stmt **rows);
/* public notes on films released in this public space: ref, watched, rating, rewatch, body */
int aw_public_notes(sqlite3 *db, const char *ns, sqlite3_int64 world_id, const char *space_id, sqlite3_stmt **rows);
#endif
