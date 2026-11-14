#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include "../src/dating_policy.h"
int main(void){
    BfDatingMember a={"person:alice",28,24,40,1,1,1,1,1,1};
    BfDatingMember b={"person:bob",31,24,37,2,1,1,1,1,1};
    assert(bf_dating_ready(&a));
    assert(bf_dating_candidate(&a,&b,BF_DATING_NONE,BF_DATING_NONE));
    assert(!bf_dating_conversation(&a,&b,BF_DATING_INTEREST,BF_DATING_NONE));
    assert(bf_dating_pending_intro(&a,&b,BF_DATING_INTEREST,BF_DATING_NONE));
    assert(!bf_dating_pending_intro(&a,&b,BF_DATING_ACCEPT,BF_DATING_NONE));
    assert(bf_dating_conversation(&a,&b,BF_DATING_ACCEPT,BF_DATING_ACCEPT));
    assert(!bf_dating_conversation(&a,&b,BF_DATING_ACCEPT,BF_DATING_BLOCK));
    assert(!bf_dating_candidate(&a,&b,BF_DATING_PASS,BF_DATING_ACCEPT));
    b.accepts_introductions=0;
    assert(!bf_dating_pending_intro(&a,&b,BF_DATING_INTEREST,BF_DATING_NONE));
    b.accepts_introductions=1;
    b.explicit_opt_in=0;
    assert(!bf_dating_candidate(&a,&b,BF_DATING_NONE,BF_DATING_NONE));
    b.explicit_opt_in=1;
    b.adult_eligibility_verified=0;
    assert(!bf_dating_ready(&b));
    b.adult_eligibility_verified=1;
    b.age=17;
    assert(!bf_dating_candidate(&a,&b,BF_DATING_ACCEPT,BF_DATING_ACCEPT));
    b.age=31;
    b.profile_approved=0;
    assert(!bf_dating_ready(&b));
    b.profile_approved=1;
    b.participation_revision=0;
    assert(!bf_dating_ready(&b));
    b.participation_revision=2;
    b.minimum_age=29;
    assert(!bf_dating_candidate(&a,&b,BF_DATING_ACCEPT,BF_DATING_ACCEPT));
    b.minimum_age=24;
    b.subject="person:alice";
    assert(!bf_dating_candidate(&a,&b,BF_DATING_ACCEPT,BF_DATING_ACCEPT));
    b.subject="person:bob";
    assert(!bf_dating_subject("person:evil/other"));
    assert(!bf_dating_subject("org:alice"));
    BfDatingAnswer x[]={{1,1,5},{2,-1,1},{3,0,5},{1,-1,5}};
    BfDatingAnswer y[]={{1,1,2},{2,1,4},{3,1,5},{4,1,5}};
    BfDatingAgreement score=bf_dating_shared_answers(x,4,y,4);
    assert(score.compared==2 && score.agreement_percent==58);
    assert(bf_dating_shared_answers(NULL,0,y,4).compared==0);
    assert(bf_dating_shared_answers(x,4,NULL,1).compared==0);
    uint64_t items[]={1800,2400};
    assert(bf_dating_plan_review(items,2,3000,5000,0)==BF_DATING_PLAN_REVIEW);
    assert(bf_dating_plan_review(items,2,3000,5000,1)==BF_DATING_PLAN_APPROVED);
    assert(bf_dating_plan_review(items,2,0,4100,1)==BF_DATING_PLAN_REFUSED);
    uint64_t overflow[]={UINT64_MAX,2};
    assert(bf_dating_plan_review(overflow,2,0,UINT64_MAX,1)==BF_DATING_PLAN_REFUSED);
    assert(bf_dating_plan_review(items,0,0,5000,1)==BF_DATING_PLAN_REFUSED);
    puts("dating policy: opt-in, adult eligibility, pair consent, block, intros, scoring, plans PASS");
    return 0;
}
