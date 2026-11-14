#ifndef BONFYRE_CLUB_PROJECTION_H
#define BONFYRE_CLUB_PROJECTION_H
#include <sqlite3.h>
#include <stddef.h>
#include <stdio.h>
/* Interest clubs, events and the gold entitlement, read from the community
 * organs. Clubs are community bodies carrying an interest label; events are
 * community posts carrying a time and venue; RSVPs are reactions of kind
 * rsvp. Gold is membership in the gold circle with a future paid-through
 * date; purchase wiring stays out until a facilitator exists, so gold is
 * granted by the operator and enforced here. Missing tables or columns
 * read as empty, never as error. */
/* True when the actor holds active gold past now. -1 on misuse. */
int bf_gold_active(sqlite3 *db, const char *actor);
/* Remaining free quota, or -1 when gold (unlimited). 0 on success. */
int bf_quota_superlike(sqlite3 *db, const char *actor, int *left);
int bf_quota_plans(sqlite3 *db, const char *actor, int *left);
/* Interest-club directory with member counts and viewer state. */
int bf_clubs_page(FILE *out, sqlite3 *db, const char *viewer);
/* Upcoming events, optionally one club, with RSVP counts and viewer state. */
int bf_events_page(FILE *out, sqlite3 *db, const char *viewer, const char *club);
/* Notice center: new mutuals, inbound superlikes and RSVPs to the viewer's
 * events, newest first. Derived from retained edges only; nothing here sends
 * anything anywhere. A bare store renders empty. -1 on misuse. */
int bf_notices_page(FILE *out, sqlite3 *db, const char *viewer);
int bf_notices_json(FILE *out, sqlite3 *db, const char *viewer);
#endif
