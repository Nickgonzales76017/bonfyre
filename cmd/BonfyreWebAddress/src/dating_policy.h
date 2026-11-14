#ifndef BONFYRE_DATING_POLICY_H
#define BONFYRE_DATING_POLICY_H
/* Aurekai Dating: pure, source-independent decisions for the existing native
 * Auth/Profile/Participation owners. This is not a second user database.
 * A public person profile never implies dating eligibility or participation.
 * Persist consent and effects only through a separately admitted owner.
 * No code or question bank was copied from reference repositories. */
#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef enum {
    BF_DATING_NONE=0, BF_DATING_PASS=1, BF_DATING_INTEREST=2,
    BF_DATING_ACCEPT=3, BF_DATING_BLOCK=4
} BfDatingChoice;
typedef struct {
    const char *subject;              /* exact native person subject */
    unsigned age;                    /* verified age at time of decision */
    unsigned minimum_age,maximum_age; /* self-selected bounds */
    unsigned participation_revision;
    int adult_eligibility_verified;  /* native identity/eligibility witness */
    int active,explicit_opt_in,profile_approved;
    int accepts_introductions;       /* revocable, separate from matches */
} BfDatingMember;
typedef struct {
    unsigned question; /* non-sensitive, opt-in question identifier */
    int answer;        /* -1, 0 (unanswered), or +1 */
    unsigned importance;/* 1..5; no inference from a skipped question */
} BfDatingAnswer;
typedef struct {unsigned compared;unsigned agreement_percent;} BfDatingAgreement;
typedef enum {BF_DATING_PLAN_REFUSED=0,BF_DATING_PLAN_REVIEW=1,BF_DATING_PLAN_APPROVED=2} BfDatingPlanState;

static inline int bf_dating_subject(const char *s){
    if(!s||strncmp(s,"person:",7)||!s[7]||strlen(s)>191)return 0;
    for(const unsigned char *p=(const unsigned char*)s+7;*p;p++)
        if(!((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||
             (*p>='0'&&*p<='9')||*p=='-'||*p=='_'||*p=='.'))return 0;
    return 1;
}
static inline int bf_dating_ready(const BfDatingMember *m){
    return m && bf_dating_subject(m->subject) &&
        m->participation_revision>0 && m->active && m->explicit_opt_in &&
        m->adult_eligibility_verified && m->profile_approved &&
        m->age>=18 && m->age<=115 && m->minimum_age>=18 &&
        m->maximum_age<=115 && m->minimum_age<=m->maximum_age;
}
static inline int bf_dating_candidate(const BfDatingMember *a,const BfDatingMember *b,
                                       BfDatingChoice from_a,BfDatingChoice from_b){
    return bf_dating_ready(a)&&bf_dating_ready(b) &&
        strcmp(a->subject,b->subject) &&
        a->age>=b->minimum_age && a->age<=b->maximum_age &&
        b->age>=a->minimum_age && b->age<=a->maximum_age &&
        from_a!=BF_DATING_BLOCK && from_b!=BF_DATING_BLOCK &&
        from_a!=BF_DATING_PASS && from_b!=BF_DATING_PASS;
}
/* A like is not consent to contact. Both people must explicitly accept
 * before the shared Inbox may project a conversation. */
static inline int bf_dating_conversation(const BfDatingMember *a,const BfDatingMember *b,
                                         BfDatingChoice from_a,BfDatingChoice from_b){
    return bf_dating_candidate(a,b,from_a,from_b) &&
        from_a==BF_DATING_ACCEPT && from_b==BF_DATING_ACCEPT;
}
/* An introduction can create only a pending review, not an Inbox message or
 * a profile-disclosure capability. Limits and abuse checks remain with owner. */
static inline int bf_dating_pending_intro(const BfDatingMember *a,const BfDatingMember *b,
                                          BfDatingChoice from_a,BfDatingChoice from_b){
    return bf_dating_candidate(a,b,from_a,from_b) &&
        b->accepts_introductions && from_a==BF_DATING_INTEREST &&
        from_b==BF_DATING_NONE;
}
/* A deterministic ranking hint, never a claim about psychological truth.
 * Only equal opted-in question IDs are compared, no answer is imputed. */
static inline BfDatingAgreement bf_dating_shared_answers(
       const BfDatingAnswer *a,size_t na,const BfDatingAnswer *b,size_t nb){
    BfDatingAgreement out={0,0};uint64_t equal=0,total=0;
    if((na&&!a)||(nb&&!b)||na>256||nb>256)return out;
    for(size_t i=0;i<na;i++){
        if(!a[i].question||!a[i].answer||a[i].answer<-1||a[i].answer>1||
           !a[i].importance||a[i].importance>5)continue;
        /* Duplicate question records are invalid; only first is used. */
        int duplicate=0;
        for(size_t k=0;k<i;k++)if(a[k].question==a[i].question){duplicate=1;break;}
        if(duplicate)continue;
        for(size_t j=0;j<nb;j++)if(a[i].question==b[j].question){
            if(b[j].answer==-1||b[j].answer==1){
                unsigned weight=a[i].importance;
                if(b[j].importance>=1&&b[j].importance<=5)
                    weight+=b[j].importance;
                else break;
                total+=weight;equal+=a[i].answer==b[j].answer?weight:0;
                out.compared++;
            }
            break;
        }
    }
    if(total)out.agreement_percent=(unsigned)((100*equal+total/2)/total);
    return out;
}
/* Non-authoritative date-plan review. Cost values are estimates in cents.
 * Plan approval alone never spends funds, books a restaurant or contacts
 * a third party, and requires separate actor approval at the effect owner. */
static inline BfDatingPlanState bf_dating_plan_review(
        const uint64_t *estimated_cents,size_t count,uint64_t budget_min,
        uint64_t budget_max,int explicitly_approved){
    if(!estimated_cents||!count||count>32||budget_min>budget_max)return BF_DATING_PLAN_REFUSED;
    uint64_t total=0;
    for(size_t i=0;i<count;i++){
        if(estimated_cents[i]>UINT64_MAX-total)return BF_DATING_PLAN_REFUSED;
        total+=estimated_cents[i];
    }
    if(total<budget_min||total>budget_max)return BF_DATING_PLAN_REFUSED;
    return explicitly_approved?BF_DATING_PLAN_APPROVED:BF_DATING_PLAN_REVIEW;
}
#endif
