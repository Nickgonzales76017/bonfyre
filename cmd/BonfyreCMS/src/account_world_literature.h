#ifndef BONFYRE_ACCOUNT_WORLD_LITERATURE_H
#define BONFYRE_ACCOUNT_WORLD_LITERATURE_H
#include <sqlite3.h>

/* account-world-literature-1: writing additions to the account-world owner.
 *
 * A space may hold works: poems, stories, essays, memoir, serials and
 * interactive stories. A work is the account's own writing or a public-domain
 * text with the https address it was taken from. Its passages keep their text
 * exactly as written (line breaks, indentation, blank lines between stanzas);
 * a serial chapter may carry the day it is released; an interactive story
 * declares its choices as edges between passages, and nothing in a passage is
 * ever run. Owner glosses (notes and footnotes) attach to a line of a poem or a
 * paragraph of prose. Every edit of a passage keeps the text it replaced, for
 * comparison. Each work has a stable key so another space of the same World
 * can show it as "works/<key>" without a copy.
 *
 * Draft and public copies follow account_world_audio.h: triggers on
 * account_world_release copy drafts at release and drop the public copy on
 * withdrawal. Readers keep their own place per work (awl_place_*), checked
 * against the public copy. The caller authenticates; nothing here states a
 * licence on anyone else's behalf, copies media or plays a recording. Return 1
 * success, 0 refused or conflict, -1 storage error. Owner mutations need the
 * exact current space revision and advance it. */
#define AWL_WORK_LIMIT 64
#define AWL_PASSAGE_LIMIT 400
#define AWL_CHOICE_LIMIT 9
#define AWL_GLOSS_LIMIT 600
#define AWL_BODY_LIMIT 60000
#define AWL_VERSION_KEEP 20
#define AWL_PATH_STEPS 64

enum { AWL_WORKS, AWL_PASSAGES, AWL_CHOICES, AWL_GLOSSES };

int awl_initialize(sqlite3 *db);
/* kind: poem | story | essay | memoir | serial | interactive.
 * rights: own | public-domain. licence: what the author states for their own
 * work ("All rights reserved", "CC BY 4.0", ...); for public-domain it must
 * begin "Public domain" and provenance must be the https page it came from.
 * original: "" or another work of this space this one translates.
 * recitation: "" or "<source>/<24 hex key>" naming a recording; whether it
 * plays is decided by its source on every read. position 1..999. */
int awl_work_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, const char *work, const char *kind, const char *title,
                 const char *author, const char *translator, const char *language,
                 const char *original, const char *collection, const char *rights,
                 const char *licence, const char *provenance, const char *provenance_label,
                 const char *recitation, int position, int expected_revision);
int awl_work_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, const char *work, int expected_revision);
/* body: exact text, "\n" line ends, tabs allowed, no other control
 * characters. release_on: "" or YYYY-MM-DD. start: 1 marks the first passage
 * of an interactive story (clearing any other). */
int awl_passage_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, const char *work, const char *passage, int number,
                    const char *title, const char *body, const char *release_on, int start,
                    int expected_revision);
/* Refused while another passage's choice leads to it. */
int awl_passage_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                       const char *space_id, const char *work, const char *passage, int expected_revision);
/* Interactive works only; both passages must exist. number 1..9. */
int awl_choice_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                   const char *space_id, const char *work, const char *passage, int number,
                   const char *label, const char *target, int expected_revision);
int awl_choice_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                      const char *space_id, const char *work, const char *passage, int number,
                      int expected_revision);
/* kind: note | footnote. unit: a line of a poem, a paragraph otherwise
 * (awl_units), 1-based and inside the passage as it stands. */
int awl_gloss_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                  const char *space_id, const char *work, const char *passage, int unit,
                  const char *kind, const char *body, int expected_revision);
int awl_gloss_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                     const char *space_id, sqlite3_int64 gloss_id, int expected_revision);

/* Lines of a poem (every "\n"-separated line, blank ones included) or
 * paragraphs of prose (runs of non-blank lines). */
int awl_units(const char *kind, const char *body);

/* Rows of one table. subject non-NULL: the owner's draft (binding checked).
 * subject NULL: the released public version, only while the space is public
 * in a bound, live World. work narrows to one work.
 *  AWL_WORKS:    work, key, kind, title, author, translator, language, original,
 *                collection, rights, licence, provenance, provenance_label,
 *                recitation, position
 *  AWL_PASSAGES: work, passage, number, title, body, release_on, start
 *  AWL_CHOICES:  work, passage, number, label, target
 *  AWL_GLOSSES:  id, work, passage, unit, kind, body */
int awl_rows(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
             const char *space_id, int table, const char *work, sqlite3_stmt **rows);
/* Owner only: saved_revision, body of earlier texts of one passage, newest first. */
int awl_versions(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, const char *work, const char *passage, sqlite3_stmt **rows);
/* Public: the home space and work slug of a released work key in this World. */
int awl_public_key(sqlite3 *db, const char *ns, sqlite3_int64 world_id, const char *key,
                   char space_out[65], char work_out[65]);

/* A reader's place. reader is any authenticated subject. The passage must be
 * public and released (release_on today or earlier, UTC); unit 0..units.
 * Interactive works need path: the passages read, joined by ".", from the
 * first passage to this one along declared choices; other works need "". */
int awl_place_put(sqlite3 *db, const char *reader, const char *ns, const char *space_id,
                  const char *work, const char *passage, int unit, const char *path);
int awl_place_clear(sqlite3 *db, const char *reader, const char *ns, const char *space_id, const char *work);
/* work, passage, unit, path, updated for works still public, newest first. */
int awl_places(sqlite3 *db, const char *reader, const char *ns, const char *space_id, sqlite3_stmt **rows);
#endif
