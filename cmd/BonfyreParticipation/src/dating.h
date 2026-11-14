#ifndef BONFYRE_PARTICIPATION_DATING_H
#define BONFYRE_PARTICIPATION_DATING_H
#include <sqlite3.h>
#include <stdint.h>
#include "../../BonfyreWebAddress/src/dating_policy.h"
/* Subject always comes from Auth's verified actor closure; never from a form.
 * Public pages/posts cannot read this owner or imply dating participation. */
typedef struct {
    const char *subject,*eligibility_witness,*moderation_witness;
    unsigned age,min_age,max_age;
    int accepts_introductions;
} BfDatingEnrollment;
typedef struct {
    char subject[192];
    BfDatingChoice mine;
    unsigned my_choice_revision;
    unsigned shared_answers,agreement_percent;
    int introduction_for_me, introduction_sent, matched;
} BfDatingCandidate;
typedef struct {
    char key[80],activity[161],meeting_context[121];
    uint64_t estimated_cents;
    unsigned revision;
    int proposed_by_me,approved_by_me,approved_by_other;
} BfDatingPlan;
typedef struct {
    long long id, at;
    char sender[192], body[1001];
} BfDatingMessage;
int bf_dating_store_init(sqlite3 *db);
int bf_dating_eligible(sqlite3 *db,const char *authenticated_subject);
unsigned bf_dating_active_revision(sqlite3 *db,const char *authenticated_subject);
int bf_dating_preferences_read(sqlite3 *db,const char *authenticated_subject,
                               BfDatingAnswer answers[4],unsigned *revision);
/* Four private non-sensitive voluntary preferences; comparison only with
 * independently opted-in current participation epochs, never a diagnosis. */
int bf_dating_preferences(sqlite3 *db,const char *authenticated_subject,
                          const BfDatingAnswer answers[4],unsigned expected_participation_revision);
/* Separately requested by the authenticated person; never grants eligibility. */
int bf_dating_request(sqlite3 *db,const char *authenticated_subject,int enable,
                      unsigned expected_revision);
int bf_dating_request_status(sqlite3 *db,const char *authenticated_subject,
                             unsigned *revision); /* 0 none, 1 pending, 2 withdrawn */
int bf_dating_candidates(sqlite3 *db,const char *authenticated_subject,
                         BfDatingCandidate *out,size_t cap,size_t *count);
/* A conversation is a private Inbox material source, not a second identity. */
int bf_dating_send(sqlite3 *db,const char *authenticated_subject,
                   const char *other,const char *nonce,const char *body);
int bf_dating_messages(sqlite3 *db,const char *authenticated_subject,
                       const char *other,long long before_id,
                       BfDatingMessage *out,size_t cap,size_t *count);
int bf_dating_enroll(sqlite3 *db,const BfDatingEnrollment *enroll,unsigned expected_revision);
int bf_dating_withdraw(sqlite3 *db,const char *authenticated_subject,unsigned expected_revision);
int bf_dating_choice(sqlite3 *db,const char *authenticated_subject,const char *other,
                     BfDatingChoice choice,unsigned expected_revision);
int bf_dating_can_chat(sqlite3 *db,const char *a,const char *b);
int bf_dating_can_introduce(sqlite3 *db,const char *a,const char *b);
int bf_dating_propose_detail(sqlite3 *db,const char *actor,const char *partner,
                            const char *plan_key,const char *activity,
                            const char *meeting_context,uint64_t estimated_cents,
                            uint64_t budget_max_cents);
int bf_dating_plans(sqlite3 *db,const char *actor,const char *partner,
                    BfDatingPlan *out,size_t cap,size_t *count);
int bf_dating_propose(sqlite3 *db,const char *a,const char *b,const char *plan_key,
                      const uint64_t *costs,size_t count,uint64_t min,uint64_t max);
int bf_dating_approve(sqlite3 *db,const char *actor,const char *plan_key,
                      unsigned expected_revision,int *mutually_approved);
#endif
