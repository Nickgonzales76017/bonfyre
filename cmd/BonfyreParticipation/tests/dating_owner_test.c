#include <assert.h>
#include <stdio.h>
#include "../src/dating.h"
int main(void){
 sqlite3 *db=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
 assert(bf_dating_store_init(db));
 BfDatingEnrollment a={"person:alice","identity:verified-a","media:approved-a",28,24,40,1};
 BfDatingEnrollment b={"person:bob","identity:verified-b","media:approved-b",31,24,37,1};
 unsigned request_revision=0;
 assert(bf_dating_request_status(db,a.subject,&request_revision)==0&&request_revision==0);
 assert(bf_dating_request(db,a.subject,1,0));
 assert(bf_dating_request(db,b.subject,1,0));
 assert(!bf_dating_request(db,a.subject,1,0));
 assert(bf_dating_request_status(db,a.subject,&request_revision)==1&&request_revision==1);
 assert(!bf_dating_enroll(db,&a,1));
 assert(!bf_dating_enroll(db,&a,0)); /* unverified strings are not evidence */
 assert(sqlite3_exec(db,"INSERT INTO dating_verification"
 "(subject,age,eligibility_witness,moderation_witness,issued_by,state,valid_until) VALUES"
 "('person:alice',28,'identity:verified-a','media:approved-a','BonfyreAuth','verified',"
 "CAST(strftime('%s','now') AS INTEGER)+3600),"
 "('person:bob',31,'identity:verified-b','media:approved-b','BonfyreAuth','verified',"
 "CAST(strftime('%s','now') AS INTEGER)+3600)",NULL,NULL,NULL)==SQLITE_OK);
 BfDatingEnrollment forged=a;forged.eligibility_witness="identity:forged";
 assert(!bf_dating_enroll(db,&forged,0));
 assert(bf_dating_enroll(db,&a,0));assert(bf_dating_enroll(db,&b,0));
 assert(!bf_dating_enroll(db,&a,0));
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(!bf_dating_choice(db,a.subject,a.subject,BF_DATING_ACCEPT,0));
 assert(bf_dating_choice(db,a.subject,b.subject,BF_DATING_INTEREST,0));
 assert(bf_dating_can_introduce(db,a.subject,b.subject));
 assert(!bf_dating_choice(db,a.subject,b.subject,BF_DATING_ACCEPT,0));
 assert(bf_dating_choice(db,a.subject,b.subject,BF_DATING_ACCEPT,1));
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(bf_dating_choice(db,b.subject,a.subject,BF_DATING_ACCEPT,0));
 assert(bf_dating_can_chat(db,a.subject,b.subject));
 /* A creative/civic block immediately closes dating without altering the
  * dating consent epoch or exposing the other context. */
 assert(sqlite3_exec(db,"INSERT INTO peer_block VALUES('person:alice','person:bob')",
    NULL,NULL,NULL)==SQLITE_OK);
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(!bf_dating_can_introduce(db,a.subject,b.subject));
 assert(sqlite3_exec(db,"DELETE FROM peer_block WHERE subject='person:alice'",
    NULL,NULL,NULL)==SQLITE_OK);
 assert(bf_dating_can_chat(db,a.subject,b.subject));
 BfDatingAnswer liked[4]={{1,1,5},{2,-1,2},{3,1,4},{4,0,2}};
 BfDatingAnswer similar[4]={{1,1,5},{2,1,2},{3,1,4},{4,0,2}};
 assert(!bf_dating_preferences(db,a.subject,liked,0));
 assert(bf_dating_preferences(db,a.subject,liked,1));
 assert(bf_dating_preferences(db,b.subject,similar,1));
 BfDatingAnswer bad[4]={{1,1,5},{2,99,2},{3,1,4},{4,0,2}};
 assert(!bf_dating_preferences(db,a.subject,bad,1));
 BfDatingCandidate candidates[3];size_t candidate_count=0;
 assert(bf_dating_candidates(db,a.subject,candidates,3,&candidate_count));
 assert(candidate_count==1&&candidates[0].matched);
 assert(candidates[0].shared_answers==3);
 assert(candidates[0].agreement_percent==82);
 BfDatingMessage messages[4];size_t message_count=0;
 assert(!bf_dating_send(db,a.subject,b.subject,"n1",""));
 assert(bf_dating_send(db,a.subject,b.subject,"n1","Good to meet you."));
 assert(bf_dating_send(db,a.subject,b.subject,"n1","Good to meet you.")); /* exact replay */
 assert(!bf_dating_send(db,a.subject,b.subject,"n1","Forged overwrite"));
 assert(bf_dating_messages(db,b.subject,a.subject,0,messages,4,&message_count));
 assert(message_count==1&&!strcmp(messages[0].body,"Good to meet you."));
 uint64_t costs[]={1200,1900};
 assert(!bf_dating_propose(db,a.subject,b.subject,"date-1",costs,2,0,3000));
 assert(bf_dating_propose(db,a.subject,b.subject,"date-1",costs,2,0,4000));
 int mutual=0;
 assert(!bf_dating_approve(db,"person:mallory","date-1",1,&mutual));
 assert(bf_dating_approve(db,a.subject,"date-1",1,&mutual)&&!mutual);
 assert(!bf_dating_approve(db,b.subject,"date-1",1,&mutual));
 assert(bf_dating_approve(db,b.subject,"date-1",2,&mutual)&&mutual);
 assert(!bf_dating_propose_detail(db,a.subject,b.subject,"reviewed-date",
    "Museum and coffee","Meet at a mutually chosen public place",3500,3000));
 assert(bf_dating_propose_detail(db,a.subject,b.subject,"reviewed-date",
    "Museum and coffee","Meet at a mutually chosen public place",3500,5000));
 assert(!bf_dating_propose_detail(db,a.subject,b.subject,"reviewed-date",
    "Overwrite approved date","",3500,5000));
 BfDatingPlan plans[4];size_t plan_count=0;
 assert(bf_dating_plans(db,a.subject,b.subject,plans,4,&plan_count));
 assert(plan_count==2);
 assert(!strcmp(plans[0].key,"reviewed-date"));
 assert(!strcmp(plans[0].activity,"Museum and coffee"));
 assert(plans[0].proposed_by_me&&plans[0].approved_by_me&&!plans[0].approved_by_other);
 assert(bf_dating_approve(db,b.subject,"reviewed-date",1,&mutual)&&mutual);
 assert(!bf_dating_approve(db,b.subject,"reviewed-date",2,&mutual));
 assert(bf_dating_plans(db,b.subject,a.subject,plans,4,&plan_count));
 assert(plan_count==2&&plans[0].approved_by_me&&plans[0].approved_by_other);
 /* Proof revocation immediately closes matches without deleting old history. */
 assert(sqlite3_exec(db,"UPDATE dating_verification SET state='revoked' "
    "WHERE subject='person:bob'",NULL,NULL,NULL)==SQLITE_OK);
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(!bf_dating_messages(db,a.subject,b.subject,0,messages,4,&message_count));
 assert(!bf_dating_send(db,a.subject,b.subject,"n2","No consent"));
 assert(!bf_dating_approve(db,a.subject,"date-1",3,&mutual));
 assert(sqlite3_exec(db,"UPDATE dating_verification SET state='verified' "
    "WHERE subject='person:bob'",NULL,NULL,NULL)==SQLITE_OK);
 assert(bf_dating_can_chat(db,a.subject,b.subject));
 /* A new participation epoch requires fresh mutual consent and a new plan. */
 assert(bf_dating_enroll(db,&a,1));
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(bf_dating_candidates(db,a.subject,candidates,3,&candidate_count));
 assert(candidate_count==1&&candidates[0].shared_answers==0); /* no old quiz reuse */
 assert(!bf_dating_preferences(db,a.subject,liked,1)); /* stale owner epoch */
 assert(bf_dating_choice(db,a.subject,b.subject,BF_DATING_ACCEPT,2));
 assert(bf_dating_choice(db,b.subject,a.subject,BF_DATING_ACCEPT,1));
 assert(bf_dating_can_chat(db,a.subject,b.subject));
 assert(!bf_dating_approve(db,a.subject,"date-1",3,&mutual));
 assert(bf_dating_plans(db,a.subject,b.subject,plans,4,&plan_count));
 assert(plan_count==0); /* old date details cannot follow new consent */
 assert(bf_dating_choice(db,b.subject,a.subject,BF_DATING_BLOCK,2));
 sqlite3_stmt *global_block=NULL;
 assert(sqlite3_prepare_v2(db,"SELECT count(*) FROM peer_block WHERE "
   "subject='person:bob' AND target='person:alice'",-1,&global_block,NULL)==SQLITE_OK);
 assert(sqlite3_step(global_block)==SQLITE_ROW&&sqlite3_column_int(global_block,0)==1);
 sqlite3_finalize(global_block);
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(!bf_dating_approve(db,a.subject,"date-1",3,&mutual));
 assert(bf_dating_withdraw(db,a.subject,2));
 assert(!bf_dating_withdraw(db,a.subject,2));
 assert(bf_dating_enroll(db,&a,3));
 assert(!bf_dating_can_chat(db,a.subject,b.subject)); /* block survives */
 assert(bf_dating_request(db,a.subject,0,1));
 assert(bf_dating_request_status(db,a.subject,&request_revision)==2&&request_revision==2);
 assert(!bf_dating_enroll(db,&a,5));
 a.age=17;assert(!bf_dating_enroll(db,&a,4));
 assert(sqlite3_exec(db,"UPDATE dating_verification SET valid_until=0 "
    "WHERE subject='person:alice'",NULL,NULL,NULL)==SQLITE_OK);
 assert(!bf_dating_can_chat(db,a.subject,b.subject));
 assert(!bf_dating_enroll(db,&a,4));
 sqlite3_close(db);
 puts("dating owner PASS: opt-in CAS, mutuality, safe plans, block, withdrawal");
 return 0;
}
