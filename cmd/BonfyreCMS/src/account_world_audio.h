#ifndef BONFYRE_ACCOUNT_WORLD_AUDIO_H
#define BONFYRE_ACCOUNT_WORLD_AUDIO_H
#include <sqlite3.h>

/* account-world-audio-1: sound additions to the account-world owner.
 *
 * A space may carry one programme record (a station or a show), a weekly
 * schedule, episode details for items it already holds, and time marks on
 * those episodes: tracks, chapters, transcript cues, clips, people and cited
 * sources. Everything stays a draft until the space is released; release and
 * withdrawal copy or drop the public versions inside the owner's own
 * transaction (triggers on account_world_release). Same contract as
 * account_world.h: the caller authenticates; nothing here states rights,
 * copies media or decides playback. Return 1 success, 0 refused/conflict,
 * -1 storage error. Every mutation needs the exact current space revision and
 * advances it. */
#define AWA_SLOT_LIMIT 48
#define AWA_MARK_LIMIT 600
#define AWA_VTT_LIMIT 65536

enum { AWA_SOUND, AWA_SLOTS, AWA_EPISODES, AWA_MARKS };

int awa_initialize(sqlite3 *db);
/* kind: station | show. timezone: an IANA name ("Area/City") or UTC.
 * stream_ref: "" or "<source>/<24 hex key>" naming the piece that is the live
 * stream; whether it plays is decided by its source on every read. */
int awa_sound_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                  const char *space_id, const char *kind, const char *timezone, const char *language,
                  const char *author, const char *category, int explicit_content,
                  const char *stream_ref, int expected_revision);
/* weekday 1 (Monday) .. 7; start_minute 0..1439 in the station timezone. */
int awa_slot_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, int slot, int weekday, int start_minute, int minutes,
                 const char *programme, const char *host, const char *ref, int expected_revision);
int awa_slot_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, int slot, int expected_revision);
/* ref must already be an item of the space. published: YYYY-MM-DD. */
int awa_episode_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, const char *ref, int season, int number,
                    const char *published, int expected_revision);
/* kind: track | chapter | cue | clip | person | source. end_ms is required
 * for cue and clip and must follow start_ms. href: "" , https URL (a
 * citation or profile, never played) or a "<source>/<key>" reference.
 * checked: a verified track, a reviewed cue or clip. */
int awa_mark_put(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                 const char *space_id, const char *ref, const char *kind, long long start_ms,
                 long long end_ms, const char *title, const char *credit, const char *href,
                 int checked, int expected_revision);
int awa_mark_check(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                   const char *space_id, sqlite3_int64 mark_id, int checked, int expected_revision);
int awa_mark_remove(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, sqlite3_int64 mark_id, int expected_revision);
/* WebVTT text -> unreviewed cue marks on one episode; "<v Name>" becomes the
 * cue's speaker. Existing unreviewed cues of that episode are replaced;
 * reviewed ones are kept. *imported receives the cue count. */
int awa_cues_import(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
                    const char *space_id, const char *ref, const char *vtt, int expected_revision,
                    int *imported);
/* Rows of one table. subject non-NULL: the owner's draft (binding checked).
 * subject NULL: the released public version, only while the space is public
 * in a bound, live World. ref narrows episodes and marks to one item.
 *  AWA_SOUND:    kind, timezone, language, author, category, explicit, stream_ref
 *  AWA_SLOTS:    slot, weekday, start_minute, minutes, programme, host, ref
 *  AWA_EPISODES: ref, season, number, published
 *  AWA_MARKS:    id, ref, kind, start_ms, end_ms, title, credit, href, checked */
int awa_rows(sqlite3 *db, const char *subject, const char *ns, sqlite3_int64 world_id,
             const char *space_id, int table, const char *ref, sqlite3_stmt **rows);
#endif
