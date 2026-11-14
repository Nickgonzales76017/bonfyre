#ifndef BONFYRE_AFFINITY_PROJECTION_H
#define BONFYRE_AFFINITY_PROJECTION_H
#include <sqlite3.h>
#include <stddef.h>
#include <stdio.h>
/* Directed person affinity reads: like/pass/superlike edges between account
 * subjects. Only the latest edge per ordered pair counts; withdrawn edges
 * read as never-existed. Mutuals are computed at read time, never stored.
 * Self-edges read as none. A store without the type reads as empty. */
int bf_affinity_status(sqlite3 *db, const char *actor, const char *target,
                       char *kind_out, size_t kind_cap);
int bf_affinity_mutual(sqlite3 *db, const char *a, const char *b);
/* True when either direction holds an active block. Returns -1 on misuse. */
int bf_affinity_blocked(sqlite3 *db, const char *a, const char *b);
/* True when the actor filed fewer than 5 published reports today (UTC).
 * A store without the report type has no quota. Returns -1 on misuse. */
int bf_report_quota_ok(sqlite3 *db, const char *actor);
/* Companion discovery over published member sites: excludes the viewer, the
 * viewer's active passes, and non-person subjects. viewer NULL/"" reads
 * anonymous (no exclusion, no states). territory "" matches all; needle ""
 * matches all; club "" matches all, otherwise only active members of that
 * community. sort "match" ranks a signed-in viewer's pool by match score
 * (ties keep subject order); anything else keeps subject order. page_size
 * clamps to 24. Returns rows shown, or -1 on misuse. */
int bf_companions_page(FILE *out, sqlite3 *db, const char *viewer,
                       const char *territory, const char *needle,
                       unsigned offset, unsigned page_size, const char *sort,
                       const char *view, const char *club);
int bf_companions_json(FILE *out, sqlite3 *db, const char *viewer,
                       const char *territory, const char *needle,
                       unsigned offset, unsigned page_size, const char *sort,
                       const char *view, const char *club);
/* Transparent match score: 2 per shared interest label (cap 10) plus 1 for
 * a community post in the last 30 days plus quiz agreement (cap 6).
 * shared_out receives up to 8 shared labels, comma-separated, with a quiz+N
 * token when quiz points apply. Anonymous, edgeless or blocked pairs
 * score 0. Returns 0 on success, -1 on misuse. A store without the
 * interest type scores 0. */
int bf_match_score(sqlite3 *db, const char *viewer, const char *target,
                   int *score, char *shared_out, size_t shared_cap);
/* The native quiz bank: original everyday-compatibility questions with
 * stable keys. Answers naming an unknown question or choice, or carrying
 * a weight outside 1..3, read as never-existed. */
typedef struct {
    const char *key;
    const char *prompt;
    const char *choice_keys[4];
    const char *choice_texts[4];
    unsigned choices;
} BfQuizQuestion;
extern const BfQuizQuestion bf_quiz_bank[];
extern const unsigned bf_quiz_bank_n;
/* Recent attention: latest inbound like/superlike edges with member names,
 * newest first, blocked pairs excluded. A store without the affinity type
 * renders empty. Returns rows shown, -1 on misuse. */
int bf_attention_page(FILE *out, sqlite3 *db, const char *viewer, unsigned limit);
int bf_attention_json(FILE *out, sqlite3 *db, const char *viewer, unsigned limit);
#endif
