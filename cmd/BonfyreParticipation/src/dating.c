#include "dating.h"
#include <stdio.h>
#include <string.h>
#include <limits.h>
#define DATE_SQL(db,s) (sqlite3_exec(db,s,NULL,NULL,NULL)==SQLITE_OK)
static int tx(sqlite3 *db){return db&&sqlite3_get_autocommit(db)&&DATE_SQL(db,"BEGIN IMMEDIATE");}
static int finish(sqlite3 *db,int ok){if(ok&&DATE_SQL(db,"COMMIT"))return 1;DATE_SQL(db,"ROLLBACK");return 0;}
static int ref(const char *s){return s&&*s&&strlen(s)<160&&
 strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-")==strlen(s);}
static int key(const char *s){return s&&*s&&strlen(s)<80&&
 strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")==strlen(s);}
int bf_dating_store_init(sqlite3 *db){
 /* Specialized private relation owner, not a duplicate Auth/profile database.
  * CMS records can describe authored intents, but only this owner enacts them. */
 return db&&DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_request("
  "subject TEXT PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL,"
  "updated_at INTEGER NOT NULL,CHECK(state IN ('requested','withdrawn')))")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_verification("
  "subject TEXT PRIMARY KEY,age INTEGER NOT NULL,eligibility_witness TEXT NOT NULL,"
  "moderation_witness TEXT NOT NULL,issued_by TEXT NOT NULL,state TEXT NOT NULL,"
  "valid_until INTEGER NOT NULL)")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_person("
  "subject TEXT PRIMARY KEY,revision INTEGER NOT NULL,age INTEGER NOT NULL,"
  "min_age INTEGER NOT NULL,max_age INTEGER NOT NULL,active INTEGER NOT NULL,"
  "intro INTEGER NOT NULL,eligibility_witness TEXT NOT NULL,moderation_witness TEXT NOT NULL)")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_choice("
  "actor TEXT NOT NULL,target TEXT NOT NULL,choice INTEGER NOT NULL,revision INTEGER NOT NULL,"
  "actor_rev INTEGER NOT NULL,target_rev INTEGER NOT NULL,"
  "PRIMARY KEY(actor,target),CHECK(actor<>target),CHECK(choice BETWEEN 0 AND 4))")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_answer("
  "subject TEXT NOT NULL,question INTEGER NOT NULL,answer INTEGER NOT NULL,"
  "importance INTEGER NOT NULL,participant_rev INTEGER NOT NULL,"
  "PRIMARY KEY(subject,question),CHECK(question BETWEEN 1 AND 4),"
  "CHECK(answer IN (-1,1)),CHECK(importance BETWEEN 1 AND 5))")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_message("
  "id INTEGER PRIMARY KEY AUTOINCREMENT,sender TEXT NOT NULL,recipient TEXT NOT NULL,"
  "nonce TEXT NOT NULL,body TEXT NOT NULL,sent_at INTEGER NOT NULL,"
  "sender_rev INTEGER NOT NULL,recipient_rev INTEGER NOT NULL,"
  "UNIQUE(sender,nonce),CHECK(sender<>recipient))")&&
 DATE_SQL(db,"CREATE INDEX IF NOT EXISTS dating_message_lookup "
  "ON dating_message(recipient,sender,id)")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_plan_detail("
  "plan_key TEXT PRIMARY KEY,activity TEXT NOT NULL,meeting_context TEXT NOT NULL)")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS dating_plan("
  "plan_key TEXT PRIMARY KEY,proposer TEXT NOT NULL,partner TEXT NOT NULL,"
  "revision INTEGER NOT NULL,total_cents INTEGER NOT NULL,"
  "proposer_rev INTEGER NOT NULL,partner_rev INTEGER NOT NULL,"
  "yes_proposer INTEGER NOT NULL DEFAULT 0,yes_partner INTEGER NOT NULL DEFAULT 0)")&&
 DATE_SQL(db,"CREATE TABLE IF NOT EXISTS peer_block("
   "subject TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(subject,target),"
   "CHECK(subject<>target))")&&
 /* Carry existing dating blocks into the common, cross-context refusal.
  * The original dating_choice remains the dating consent/revision owner. */
 DATE_SQL(db,"INSERT OR IGNORE INTO peer_block(subject,target) "
   "SELECT actor,target FROM dating_choice WHERE choice=4");
}
/* Only Auth and the independent moderation owner can supply these witnesses.
 * No public profile form, CMS draft, or arbitrary ref string can enroll anyone.
 * Witness expiry/revocation is checked again on EVERY read and action. */
static int verified(sqlite3 *db,const char *subject,unsigned age,
                    const char *eligibility,const char *moderation){
 sqlite3_stmt *s=NULL;int ok=0;
 if(!db||!bf_dating_subject(subject)||!ref(eligibility)||!ref(moderation))return 0;
 if(sqlite3_prepare_v2(db,"SELECT 1 FROM dating_verification WHERE "
    "subject=?1 AND age=?2 AND eligibility_witness=?3 AND moderation_witness=?4 "
    "AND issued_by='BonfyreAuth' AND state='verified' "
    "AND valid_until>CAST(strftime('%s','now') AS INTEGER) LIMIT 1",
    -1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,2,(sqlite3_int64)age);
 sqlite3_bind_text(s,3,eligibility,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,4,moderation,-1,SQLITE_TRANSIENT);
 ok=sqlite3_step(s)==SQLITE_ROW;
 sqlite3_finalize(s);return ok;
}
/* Consent revision must be read independently of witness validity. Otherwise
 * an expired witness could make an existing row appear absent, letting a
 * creation-only request reset the account's withdrawn consent. */
static int request_state(sqlite3 *db,const char *subject,unsigned *revision){
 sqlite3_stmt *s=NULL;int outcome=-1;
 if(!db||!bf_dating_subject(subject)||!revision)return -1;
 if(sqlite3_prepare_v2(db,"SELECT revision,state FROM dating_request WHERE subject=?1",
    -1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 int state=sqlite3_step(s);
 if(state==SQLITE_DONE){outcome=0;*revision=0;}
 if(state==SQLITE_ROW){
  sqlite3_int64 n=sqlite3_column_int64(s,0);
  const char *value=(const char*)sqlite3_column_text(s,1);
  if(n>0&&n<=UINT_MAX&&value){
   *revision=(unsigned)n;
   if(!strcmp(value,"requested"))outcome=1;
   else if(!strcmp(value,"withdrawn"))outcome=2;
  }
 }
 sqlite3_finalize(s);return outcome;
}
int bf_dating_request_status(sqlite3 *db,const char *subject,unsigned *revision){
 int state=request_state(db,subject,revision);
 return state<0?-1:state;
}
int bf_dating_request(sqlite3 *db,const char *subject,int enable,unsigned expected){
 sqlite3_stmt *s=NULL;unsigned revision=0;int ok=0;
 if(!db||!bf_dating_subject(subject)||expected==UINT_MAX||!tx(db))return 0;
 int state=request_state(db,subject,&revision);
 if(state<0||revision!=expected||(enable&&state==1)||(!enable&&state!=1))goto done;
 if(state==0){
  if(!enable||sqlite3_prepare_v2(db,"INSERT INTO dating_request"
     "(subject,revision,state,updated_at) VALUES(?1,1,'requested',strftime('%s','now'))",
     -1,&s,NULL)!=SQLITE_OK)goto done;
  sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 }else{
  if(sqlite3_prepare_v2(db,"UPDATE dating_request SET revision=revision+1,"
     "state=?1,updated_at=strftime('%s','now') WHERE subject=?2 AND revision=?3",
     -1,&s,NULL)!=SQLITE_OK)goto done;
  sqlite3_bind_text(s,1,enable?"requested":"withdrawn",-1,SQLITE_STATIC);
  sqlite3_bind_text(s,2,subject,-1,SQLITE_TRANSIENT);
  sqlite3_bind_int64(s,3,expected);
 }
 ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;
 sqlite3_finalize(s);s=NULL;
 if(ok&&!enable){
  if(sqlite3_prepare_v2(db,"UPDATE dating_person SET active=0,intro=0,"
    "revision=revision+1 WHERE subject=?1 AND active=1",-1,&s,NULL)!=SQLITE_OK){ok=0;goto done;}
  sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
  ok=sqlite3_step(s)==SQLITE_DONE;
 }
done:sqlite3_finalize(s);return finish(db,ok);
}
static int current_revision(sqlite3 *db,const char *subject,unsigned *rev){
 sqlite3_stmt *s=NULL;int found=-1;
 if(sqlite3_prepare_v2(db,"SELECT revision FROM dating_person WHERE subject=?1",
    -1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 int state=sqlite3_step(s);
 if(state==SQLITE_DONE)found=0;
 else if(state==SQLITE_ROW){
   sqlite3_int64 n=sqlite3_column_int64(s,0);
   if(n>0&&n<=UINT_MAX){*rev=(unsigned)n;found=1;}
 }
 sqlite3_finalize(s);return found;
}
static int read_member(sqlite3 *db,const char *subject,BfDatingMember *out,unsigned *rev){
 sqlite3_stmt *s=NULL;int ok=0;unsigned r=0;
 if(!db||!out||!rev||!bf_dating_subject(subject))return 0;
 if(sqlite3_prepare_v2(db,"SELECT p.revision,p.age,p.min_age,p.max_age,p.active,p.intro,"
 "p.eligibility_witness,p.moderation_witness FROM dating_person p "
 "JOIN dating_request r ON r.subject=p.subject AND r.state='requested' "
 "JOIN dating_verification v ON v.subject=p.subject AND v.age=p.age AND "
 "v.eligibility_witness=p.eligibility_witness AND v.moderation_witness=p.moderation_witness "
 "AND v.issued_by='BonfyreAuth' AND v.state='verified' "
 "AND v.valid_until>CAST(strftime('%s','now') AS INTEGER) WHERE p.subject=?1",
 -1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)==SQLITE_ROW){
  sqlite3_int64 revision=sqlite3_column_int64(s,0);
  int age=sqlite3_column_int(s,1),min=sqlite3_column_int(s,2),max=sqlite3_column_int(s,3);
  const char *eligible=(const char*)sqlite3_column_text(s,6);
  const char *moderated=(const char*)sqlite3_column_text(s,7);
  if(revision>0&&revision<=UINT_MAX&&age>=18&&age<=115&&min>=18&&max<=115&&min<=max&&ref(eligible)&&ref(moderated)){
   r=(unsigned)revision;
   *out=(BfDatingMember){subject,(unsigned)age,(unsigned)min,(unsigned)max,r,1,
       sqlite3_column_int(s,4),sqlite3_column_int(s,4),1,sqlite3_column_int(s,5)};
   ok=1;
  }
 }
 sqlite3_finalize(s);if(ok)*rev=r;return ok;
}
int bf_dating_enroll(sqlite3 *db,const BfDatingEnrollment *e,unsigned expected){
 sqlite3_stmt *s=NULL;int ok=0;unsigned revision=0;
 /* Evidence refs must be issued/rechecked by Auth and moderation owners. An
  * account cannot mint them through this API. No activation from public copy. */
 if(!db||!e||!bf_dating_subject(e->subject)||!ref(e->eligibility_witness)||
 !ref(e->moderation_witness)||e->age<18||e->age>115||
 e->min_age<18||e->max_age>115||e->min_age>e->max_age||expected==UINT_MAX||!tx(db))return 0;
 int exists=current_revision(db,e->subject,&revision);
 unsigned request_revision=0;
 if(exists<0||(exists&&revision!=expected)||(!exists&&expected)||
    request_state(db,e->subject,&request_revision)!=1||
    !verified(db,e->subject,e->age,e->eligibility_witness,e->moderation_witness))goto done;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_person"
 "(subject,revision,age,min_age,max_age,active,intro,eligibility_witness,moderation_witness)"
 " VALUES(?1,?2,?3,?4,?5,1,?6,?7,?8)"
 " ON CONFLICT(subject) DO UPDATE SET revision=excluded.revision,"
 "age=excluded.age,min_age=excluded.min_age,max_age=excluded.max_age,active=1,"
 "intro=excluded.intro,eligibility_witness=excluded.eligibility_witness,"
 "moderation_witness=excluded.moderation_witness",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,e->subject,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,2,(sqlite3_int64)expected+1);
 sqlite3_bind_int(s,3,e->age);sqlite3_bind_int(s,4,e->min_age);sqlite3_bind_int(s,5,e->max_age);
 sqlite3_bind_int(s,6,e->accepts_introductions!=0);
 sqlite3_bind_text(s,7,e->eligibility_witness,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,8,e->moderation_witness,-1,SQLITE_TRANSIENT);
 ok=sqlite3_step(s)==SQLITE_DONE;
done:sqlite3_finalize(s);return finish(db,ok);
}
int bf_dating_withdraw(sqlite3 *db,const char *subject,unsigned expected){
 sqlite3_stmt *s=NULL;int ok=0;
 if(!bf_dating_subject(subject)||!expected||expected==UINT_MAX||!tx(db))return 0;
 if(sqlite3_prepare_v2(db,"UPDATE dating_person SET active=0,intro=0,revision=revision+1 "
 "WHERE subject=?1 AND revision=?2 AND active=1",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,2,expected);
 ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;
done:sqlite3_finalize(s);return finish(db,ok);
}
static BfDatingChoice choice_of(sqlite3 *db,const char *a,const char *b,unsigned ar,unsigned br){
 sqlite3_stmt *s=NULL;BfDatingChoice c=BF_DATING_BLOCK; /* DB failure: deny */
 if(sqlite3_prepare_v2(db,"SELECT choice,actor_rev,target_rev FROM dating_choice "
 "WHERE actor=?1 AND target=?2",-1,&s,NULL)!=SQLITE_OK)return c;
 sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
 c=BF_DATING_NONE;
 if(sqlite3_step(s)==SQLITE_ROW){
  int n=sqlite3_column_int(s,0);
  if(n==BF_DATING_BLOCK)c=BF_DATING_BLOCK; /* block persists after withdrawal */
  else if(sqlite3_column_int64(s,1)==(sqlite3_int64)ar &&
          sqlite3_column_int64(s,2)==(sqlite3_int64)br &&n>=0&&n<=BF_DATING_ACCEPT)
          c=(BfDatingChoice)n;
 }
 sqlite3_finalize(s);return c;
}

/* One revocation applies across dating, art, personal, civic and projects.
 * A missing/corrupt block table is never treated as permission to contact. */
static int peer_blocked_dating(sqlite3 *db,const char *a,const char *b){
 sqlite3_stmt *s=NULL;int denied=1;
 if(!db||sqlite3_prepare_v2(db,"SELECT 1 FROM peer_block WHERE "
    "(subject=?1 AND target=?2) OR (subject=?2 AND target=?1) LIMIT 1",
    -1,&s,NULL)!=SQLITE_OK)return 1;
 sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)==SQLITE_DONE)denied=0;
 sqlite3_finalize(s);return denied;
}
static int pair(sqlite3 *db,const char *a,const char *b,BfDatingMember *x,BfDatingMember *y,
 BfDatingChoice *ab,BfDatingChoice *ba,unsigned *ar,unsigned *br){
 if(!bf_dating_subject(a)||!bf_dating_subject(b)||!strcmp(a,b)||
 peer_blocked_dating(db,a,b)||!read_member(db,a,x,ar)||!read_member(db,b,y,br))return 0;
 *ab=choice_of(db,a,b,*ar,*br);*ba=choice_of(db,b,a,*br,*ar);return 1;
}
int bf_dating_choice(sqlite3 *db,const char *a,const char *b,BfDatingChoice requested,unsigned expected){
 sqlite3_stmt *s=NULL;int ok=0;unsigned ar=0,br=0;BfDatingMember x,y;BfDatingChoice ab,ba;
 if(!db||!bf_dating_subject(a)||!bf_dating_subject(b)||!strcmp(a,b)||
 requested<BF_DATING_NONE||requested>BF_DATING_BLOCK||expected==UINT_MAX||!tx(db))return 0;
 if(!pair(db,a,b,&x,&y,&ab,&ba,&ar,&br))goto done;
 if(requested!=BF_DATING_NONE&&requested!=BF_DATING_BLOCK&&
   (!bf_dating_candidate(&x,&y,ab,ba)||ab==BF_DATING_BLOCK))goto done;
 unsigned current=0;
 if(sqlite3_prepare_v2(db,"SELECT revision FROM dating_choice WHERE actor=?1 AND target=?2",
 -1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)==SQLITE_ROW){sqlite3_int64 n=sqlite3_column_int64(s,0);if(n>0&&n<=UINT_MAX)current=(unsigned)n;}
 sqlite3_finalize(s);s=NULL;
 if(current!=expected)goto done;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_choice(actor,target,choice,revision,actor_rev,target_rev)"
 " VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(actor,target) DO UPDATE SET choice=excluded.choice,"
 "revision=excluded.revision,actor_rev=excluded.actor_rev,target_rev=excluded.target_rev",
 -1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int(s,3,requested);sqlite3_bind_int64(s,4,(sqlite3_int64)expected+1);
 sqlite3_bind_int64(s,5,ar);sqlite3_bind_int64(s,6,br);
 ok=sqlite3_step(s)==SQLITE_DONE;
 sqlite3_finalize(s);s=NULL;
 if(ok&&requested==BF_DATING_BLOCK){
  if(sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO peer_block(subject,target) "
      "VALUES(?1,?2)",-1,&s,NULL)!=SQLITE_OK){ok=0;goto done;}
  sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);
  sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
  ok=sqlite3_step(s)==SQLITE_DONE;
 }
done:sqlite3_finalize(s);return finish(db,ok);
}
unsigned bf_dating_active_revision(sqlite3 *db,const char *subject){
 BfDatingMember member;unsigned revision=0;
 return db&&read_member(db,subject,&member,&revision)&&bf_dating_ready(&member)?revision:0;
}
int bf_dating_eligible(sqlite3 *db,const char *subject){
 return bf_dating_active_revision(db,subject)!=0;
}
int bf_dating_can_chat(sqlite3 *db,const char *a,const char *b){
 BfDatingMember x,y;BfDatingChoice ab,ba;unsigned ar,br;
 return db&&pair(db,a,b,&x,&y,&ab,&ba,&ar,&br)&&bf_dating_conversation(&x,&y,ab,ba);
}
int bf_dating_can_introduce(sqlite3 *db,const char *a,const char *b){
 BfDatingMember x,y;BfDatingChoice ab,ba;unsigned ar,br;
 return db&&pair(db,a,b,&x,&y,&ab,&ba,&ar,&br)&&bf_dating_pending_intro(&x,&y,ab,ba);
}
int bf_dating_propose(sqlite3 *db,const char *a,const char *b,const char *id,
 const uint64_t *costs,size_t count,uint64_t min,uint64_t max){
 sqlite3_stmt *s=NULL;int ok=0;uint64_t total=0;
 BfDatingMember ma,mb;unsigned ar=0,br=0;
 if(!db||!key(id)||min>INT64_MAX||max>INT64_MAX||
 bf_dating_plan_review(costs,count,min,max,0)!=BF_DATING_PLAN_REVIEW)return 0;
 for(size_t i=0;i<count;i++)total+=costs[i];
 if(total>INT64_MAX||!tx(db))return 0;
 if(!bf_dating_can_chat(db,a,b)||!read_member(db,a,&ma,&ar)||
    !read_member(db,b,&mb,&br))goto done;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_plan("
 "plan_key,proposer,partner,revision,total_cents,proposer_rev,partner_rev)"
 "VALUES(?1,?2,?3,1,?4,?5,?6)",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,a,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,3,b,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,4,(sqlite3_int64)total);
 sqlite3_bind_int64(s,5,ar);sqlite3_bind_int64(s,6,br);
 ok=sqlite3_step(s)==SQLITE_DONE;
done:sqlite3_finalize(s);return finish(db,ok);
}

static int dating_plan_text(const char *v,size_t max,int optional){
 if(!v||strlen(v)>max||(!optional&&!v[0]))return 0;
 for(const unsigned char *p=(const unsigned char*)v;*p;p++)
  if(*p<32||*p==127)return 0;
 return 1;
}
int bf_dating_propose_detail(sqlite3 *db,const char *actor,const char *partner,
                             const char *id,const char *activity,
                             const char *meeting_context,uint64_t estimated,uint64_t budget){
 sqlite3_stmt *s=NULL;int ok=0;BfDatingMember ma,mb;unsigned ar=0,br=0;
 if(!db||!key(id)||!dating_plan_text(activity,160,0)||
    !dating_plan_text(meeting_context,120,1)||
    estimated>INT64_MAX||budget>INT64_MAX||
    bf_dating_plan_review(&estimated,1,0,budget,0)!=BF_DATING_PLAN_REVIEW||!tx(db))return 0;
 if(!bf_dating_can_chat(db,actor,partner)||!read_member(db,actor,&ma,&ar)||
    !read_member(db,partner,&mb,&br))goto done;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_plan("
    "plan_key,proposer,partner,revision,total_cents,proposer_rev,partner_rev,yes_proposer)"
    "VALUES(?1,?2,?3,1,?4,?5,?6,1)",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,3,partner,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,4,(sqlite3_int64)estimated);
 sqlite3_bind_int64(s,5,ar);sqlite3_bind_int64(s,6,br);
 if(sqlite3_step(s)!=SQLITE_DONE)goto done;
 sqlite3_finalize(s);s=NULL;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_plan_detail(plan_key,activity,meeting_context)"
    "VALUES(?1,?2,?3)",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,activity,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,3,meeting_context,-1,SQLITE_TRANSIENT);
 ok=sqlite3_step(s)==SQLITE_DONE;
done:sqlite3_finalize(s);return finish(db,ok);
}
int bf_dating_plans(sqlite3 *db,const char *actor,const char *partner,
                    BfDatingPlan *out,size_t cap,size_t *count){
 sqlite3_stmt *s=NULL;unsigned ar=0,br=0;size_t n=0;BfDatingMember ma,mb;
 if(count)*count=0;
 if(!db||!count||(cap&&!out)||cap>12||!bf_dating_can_chat(db,actor,partner)||
    !read_member(db,actor,&ma,&ar)||!read_member(db,partner,&mb,&br))return 0;
 if(sqlite3_prepare_v2(db,"SELECT p.plan_key,COALESCE(d.activity,''),"
    "COALESCE(d.meeting_context,''),p.total_cents,p.revision,"
    "p.proposer,p.yes_proposer,p.yes_partner "
    "FROM dating_plan p LEFT JOIN dating_plan_detail d ON d.plan_key=p.plan_key "
    "WHERE ((p.proposer=?1 AND p.partner=?2 AND p.proposer_rev=?3 AND p.partner_rev=?4)"
    " OR (p.proposer=?2 AND p.partner=?1 AND p.proposer_rev=?4 AND p.partner_rev=?3))"
    "ORDER BY p.rowid DESC LIMIT ?5",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,partner,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,3,ar);sqlite3_bind_int64(s,4,br);
 sqlite3_bind_int64(s,5,(sqlite3_int64)cap);
 while(n<cap&&sqlite3_step(s)==SQLITE_ROW){
  const char *keyval=(const char*)sqlite3_column_text(s,0);
  const char *activity=(const char*)sqlite3_column_text(s,1);
  const char *context=(const char*)sqlite3_column_text(s,2);
  const char *proposer=(const char*)sqlite3_column_text(s,5);
  sqlite3_int64 cents=sqlite3_column_int64(s,3),rev=sqlite3_column_int64(s,4);
  if(!keyval||!activity||!context||!proposer||!key(keyval)||
     !dating_plan_text(activity,160,1)||!dating_plan_text(context,120,1)||
     cents<0||rev<1||rev>UINT_MAX)continue;
  BfDatingPlan *p=&out[n];memset(p,0,sizeof(*p));
  snprintf(p->key,sizeof(p->key),"%s",keyval);
  snprintf(p->activity,sizeof(p->activity),"%s",activity);
  snprintf(p->meeting_context,sizeof(p->meeting_context),"%s",context);
  p->estimated_cents=(uint64_t)cents;p->revision=(unsigned)rev;
  p->proposed_by_me=!strcmp(proposer,actor);
  p->approved_by_me=sqlite3_column_int(s,p->proposed_by_me?6:7)!=0;
  p->approved_by_other=sqlite3_column_int(s,p->proposed_by_me?7:6)!=0;
  n++;
 }
 sqlite3_finalize(s);*count=n;return 1;
}
int bf_dating_approve(sqlite3 *db,const char *actor,const char *id,unsigned expected,int *mutual){
 sqlite3_stmt *s=NULL;char a[192]={0},b[192]={0};int ok=0;
 unsigned ar=0,br=0;sqlite3_int64 saved_ar=0,saved_br=0;BfDatingMember ma,mb;
 if(mutual)*mutual=0;
 if(!db||!mutual||!bf_dating_subject(actor)||!key(id)||!expected||expected==UINT_MAX||!tx(db))return 0;
 if(sqlite3_prepare_v2(db,"SELECT proposer,partner,proposer_rev,partner_rev,yes_proposer,yes_partner "
 "FROM dating_plan WHERE plan_key=?1 AND revision=?2",
 -1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,2,expected);
 if(sqlite3_step(s)!=SQLITE_ROW)goto done;
 const char *p=(const char*)sqlite3_column_text(s,0),*q=(const char*)sqlite3_column_text(s,1);
 if(!p||!q||strlen(p)>=sizeof(a)||strlen(q)>=sizeof(b))goto done;
 snprintf(a,sizeof(a),"%s",p);snprintf(b,sizeof(b),"%s",q);
 saved_ar=sqlite3_column_int64(s,2);saved_br=sqlite3_column_int64(s,3);
 int already=sqlite3_column_int(s,!strcmp(actor,a)?4:5);
 sqlite3_finalize(s);s=NULL;
 if(already)goto done;
 /* A formerly approved plan must not revive after withdrawal/re-enrollment
  * even if both members establish an entirely new reciprocal match. */
 if((strcmp(actor,a)&&strcmp(actor,b))||!bf_dating_can_chat(db,a,b)||
    !read_member(db,a,&ma,&ar)||!read_member(db,b,&mb,&br)||
    saved_ar!=(sqlite3_int64)ar||saved_br!=(sqlite3_int64)br)goto done;
 if(sqlite3_prepare_v2(db,"UPDATE dating_plan SET revision=revision+1,"
 "yes_proposer=CASE WHEN proposer=?1 THEN 1 ELSE yes_proposer END,"
 "yes_partner=CASE WHEN partner=?1 THEN 1 ELSE yes_partner END "
 "WHERE plan_key=?2 AND revision=?3",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,id,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,3,expected);
 ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;
 sqlite3_finalize(s);s=NULL;if(!ok)goto done;
 if(sqlite3_prepare_v2(db,"SELECT yes_proposer AND yes_partner FROM dating_plan WHERE plan_key=?1",
 -1,&s,NULL)!=SQLITE_OK){ok=0;goto done;}
 sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)==SQLITE_ROW)*mutual=sqlite3_column_int(s,0);
done:sqlite3_finalize(s);if(!ok)*mutual=0;return finish(db,ok);
}


int bf_dating_preferences(sqlite3 *db,const char *actor,const BfDatingAnswer answers[4],
                          unsigned expected){
 sqlite3_stmt *s=NULL;int ok=0;unsigned rev=0;BfDatingMember member;
 if(!db||!bf_dating_subject(actor)||!answers||!expected||!tx(db))return 0;
 if(!read_member(db,actor,&member,&rev)||!bf_dating_ready(&member)||rev!=expected)goto done;
 for(unsigned i=0;i<4;i++){
  if(answers[i].question!=i+1||answers[i].answer<-1||answers[i].answer>1||
    !answers[i].importance||answers[i].importance>5)goto done;
 }
 if(sqlite3_prepare_v2(db,"DELETE FROM dating_answer WHERE subject=?1",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)!=SQLITE_DONE)goto done;
 sqlite3_finalize(s);s=NULL;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_answer"
   "(subject,question,answer,importance,participant_rev) VALUES(?1,?2,?3,?4,?5)",
   -1,&s,NULL)!=SQLITE_OK)goto done;
 for(unsigned i=0;i<4;i++){
  if(!answers[i].answer)continue;
  sqlite3_reset(s);sqlite3_clear_bindings(s);
  sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
  sqlite3_bind_int64(s,2,answers[i].question);
  sqlite3_bind_int(s,3,answers[i].answer);
  sqlite3_bind_int64(s,4,answers[i].importance);
  sqlite3_bind_int64(s,5,expected);
  if(sqlite3_step(s)!=SQLITE_DONE)goto done;
 }
 ok=1;
done:sqlite3_finalize(s);return finish(db,ok);
}
static size_t bf_dating_answers(sqlite3 *db,const char *actor,unsigned revision,
                                BfDatingAnswer out[4]){
 sqlite3_stmt *s=NULL;size_t used=0;
 if(sqlite3_prepare_v2(db,"SELECT question,answer,importance FROM dating_answer "
  "WHERE subject=?1 AND participant_rev=?2 ORDER BY question LIMIT 4",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,2,revision);
 while(used<4&&sqlite3_step(s)==SQLITE_ROW){
  const unsigned question=(unsigned)sqlite3_column_int(s,0);
  const int answer=sqlite3_column_int(s,1);
  const unsigned weight=(unsigned)sqlite3_column_int(s,2);
  if(question>=1&&question<=4&&(answer==-1||answer==1)&&weight>=1&&weight<=5)
    out[used++]=(BfDatingAnswer){question,answer,weight};
 }
 sqlite3_finalize(s);return used;
}
int bf_dating_preferences_read(sqlite3 *db,const char *actor,
                               BfDatingAnswer answers[4],unsigned *revision){
 if(revision)*revision=0;
 if(!db||!answers||!revision)return 0;
 unsigned current=bf_dating_active_revision(db,actor);
 if(!current)return 0;
 for(unsigned i=0;i<4;i++)answers[i]=(BfDatingAnswer){i+1,0,3};
 BfDatingAnswer saved[4];
 size_t n=bf_dating_answers(db,actor,current,saved);
 for(size_t i=0;i<n;i++)if(saved[i].question>=1&&saved[i].question<=4)
   answers[saved[i].question-1]=saved[i];
 *revision=current;return 1;
}
/* Private, current-consent candidate projection. Only the native owner reads
 * dating tables; a public /people or /profile view cannot enumerate them. */
int bf_dating_candidates(sqlite3 *db,const char *actor,BfDatingCandidate *out,size_t cap,size_t *count){
 sqlite3_stmt *s=NULL;unsigned ar=0,br=0;BfDatingMember a,b;size_t used=0;
 if(count)*count=0;
 if(!db||!count||(cap&&!out)||cap>24||!read_member(db,actor,&a,&ar)||!bf_dating_ready(&a))return 0;
 if(sqlite3_prepare_v2(db,"SELECT subject FROM dating_person WHERE active=1 "
    "AND subject<>?1 ORDER BY subject LIMIT 256",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 BfDatingAnswer self[4];size_t na=bf_dating_answers(db,actor,ar,self);
 while(used<cap&&sqlite3_step(s)==SQLITE_ROW){
  const char *target=(const char*)sqlite3_column_text(s,0);
  if(!target||peer_blocked_dating(db,actor,target)||!read_member(db,target,&b,&br))continue;
  BfDatingChoice mine=choice_of(db,actor,target,ar,br);
  BfDatingChoice theirs=choice_of(db,target,actor,br,ar);
  if(!bf_dating_candidate(&a,&b,mine,theirs))continue;
  BfDatingCandidate *row=&out[used];memset(row,0,sizeof(*row));
  snprintf(row->subject,sizeof(row->subject),"%s",target);
  row->mine=mine;
  row->matched=bf_dating_conversation(&a,&b,mine,theirs);
  row->introduction_for_me=bf_dating_pending_intro(&b,&a,theirs,mine);
  row->introduction_sent=bf_dating_pending_intro(&a,&b,mine,theirs);
  BfDatingAnswer other[4];size_t nb=bf_dating_answers(db,target,br,other);
  BfDatingAgreement score=bf_dating_shared_answers(self,na,other,nb);
  if(score.compared>=3){
   row->shared_answers=score.compared;row->agreement_percent=score.agreement_percent;
  }
  /* An action is always revision-checked; no unsolicited message rights. */
  sqlite3_stmt *rev=NULL;
  if(sqlite3_prepare_v2(db,"SELECT revision FROM dating_choice WHERE actor=?1 AND target=?2",
     -1,&rev,NULL)==SQLITE_OK){
   sqlite3_bind_text(rev,1,actor,-1,SQLITE_TRANSIENT);
   sqlite3_bind_text(rev,2,target,-1,SQLITE_TRANSIENT);
   if(sqlite3_step(rev)==SQLITE_ROW){
    sqlite3_int64 n=sqlite3_column_int64(rev,0);
    if(n>0&&n<UINT_MAX)row->my_choice_revision=(unsigned)n;
   }
  }
  sqlite3_finalize(rev);used++;
 }
 sqlite3_finalize(s);
 /* Rank genuine voluntarily shared preferences; ties preserve source order.
  * An unanswered preference never becomes an inferred similarity signal. */
 for(size_t i=1;i<used;i++)for(size_t j=i;j>0;j--){
  unsigned left=out[j-1].shared_answers?out[j-1].agreement_percent:0;
  unsigned right=out[j].shared_answers?out[j].agreement_percent:0;
  if(right<=left)break;
  BfDatingCandidate tmp=out[j];out[j]=out[j-1];out[j-1]=tmp;
 }
 *count=used;return 1;
}
static int dating_body(const char *body){
 if(!body||!body[0]||strlen(body)>1000)return 0;
 for(const unsigned char *p=(const unsigned char*)body;*p;p++)
  if((*p<32&&*p!=9&&*p!=10&&*p!=13)||*p==127)return 0;
 return 1;
}
int bf_dating_send(sqlite3 *db,const char *actor,const char *target,
                   const char *nonce,const char *body){
 sqlite3_stmt *s=NULL;int ok=0,rows=0;unsigned ar=0,br=0;
 if(!db||!bf_dating_subject(actor)||!bf_dating_subject(target)||
    !strcmp(actor,target)||!key(nonce)||!dating_body(body)||!tx(db))return 0;
 if(!bf_dating_can_chat(db,actor,target))goto done;
 if(current_revision(db,actor,&ar)!=1||current_revision(db,target,&br)!=1)goto done;
 /* A retry with this token can only acknowledge the exact original message. */
 if(sqlite3_prepare_v2(db,"SELECT recipient,body,sender_rev,recipient_rev FROM dating_message "
    "WHERE sender=?1 AND nonce=?2",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,nonce,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)==SQLITE_ROW){
  const char *to=(const char*)sqlite3_column_text(s,0);
  const char *txt=(const char*)sqlite3_column_text(s,1);
  ok=to&&txt&&!strcmp(to,target)&&!strcmp(txt,body)&&
     sqlite3_column_int64(s,2)==(sqlite3_int64)ar&&
     sqlite3_column_int64(s,3)==(sqlite3_int64)br;
  goto done;
 }
 sqlite3_finalize(s);s=NULL;
 if(sqlite3_prepare_v2(db,"SELECT count(*) FROM dating_message WHERE sender=?1 AND "
    "sent_at>CAST(strftime('%s','now') AS INTEGER)-60",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_int(s,0)>=4)goto done;
 sqlite3_finalize(s);s=NULL;
 if(sqlite3_prepare_v2(db,"SELECT count(*) FROM dating_message WHERE sender=?1 AND "
    "sent_at>CAST(strftime('%s','now') AS INTEGER)-86400",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_int(s,0)>=50)goto done;
 sqlite3_finalize(s);s=NULL;
 if(sqlite3_prepare_v2(db,"INSERT INTO dating_message"
    "(sender,recipient,nonce,body,sent_at,sender_rev,recipient_rev)"
    "VALUES(?1,?2,?3,?4,strftime('%s','now'),?5,?6)",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,target,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,3,nonce,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,4,body,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,5,ar);sqlite3_bind_int64(s,6,br);
 rows=sqlite3_step(s);ok=rows==SQLITE_DONE;
done:sqlite3_finalize(s);return finish(db,ok);
}
int bf_dating_messages(sqlite3 *db,const char *actor,const char *target,
                       long long before_id,BfDatingMessage *out,size_t cap,size_t *count){
 sqlite3_stmt *s=NULL;size_t n=0;unsigned ar=0,br=0;
 if(count)*count=0;
 if(!db||!count||(cap&&!out)||cap>50||before_id<0||!bf_dating_can_chat(db,actor,target)||
    current_revision(db,actor,&ar)!=1||current_revision(db,target,&br)!=1)return 0;
 if(sqlite3_prepare_v2(db,"SELECT id,sender,body,sent_at FROM dating_message "
    "WHERE ((sender=?1 AND recipient=?2 AND sender_rev=?3 AND recipient_rev=?4) OR "
    "(sender=?2 AND recipient=?1 AND sender_rev=?4 AND recipient_rev=?3)) "
    "AND (?5=0 OR id<?5) ORDER BY id DESC LIMIT ?6",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,target,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,3,ar);sqlite3_bind_int64(s,4,br);
 sqlite3_bind_int64(s,5,before_id);sqlite3_bind_int64(s,6,(sqlite3_int64)cap);
 while(n<cap&&sqlite3_step(s)==SQLITE_ROW){
  const char *from=(const char*)sqlite3_column_text(s,1);
  const char *body=(const char*)sqlite3_column_text(s,2);
  if(!from||!body||strlen(from)>=sizeof(out[n].sender)||
     strlen(body)>=sizeof(out[n].body))continue;
  out[n].id=sqlite3_column_int64(s,0);
  out[n].at=sqlite3_column_int64(s,3);
  snprintf(out[n].sender,sizeof(out[n].sender),"%s",from);
  snprintf(out[n].body,sizeof(out[n].body),"%s",body);n++;
 }
 sqlite3_finalize(s);*count=n;return 1;
}
