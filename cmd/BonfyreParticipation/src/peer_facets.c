#include "peer_facets.h"
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include "../generated/store_schema.h"  /* store DDL: schemas/storage/stores */
#define PEER_SQL(db,sql) (sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK)
static int peer_tx(sqlite3 *db){return db&&sqlite3_get_autocommit(db)&&PEER_SQL(db,"BEGIN IMMEDIATE");}
static int peer_finish(sqlite3 *db,int ok){
 if(ok&&PEER_SQL(db,"COMMIT"))return 1;
 PEER_SQL(db,"ROLLBACK");return 0;
}
static int peer_subject(const char *s){
 if(!s||strncmp(s,"person:",7)||!s[7]||strlen(s)>191)return 0;
 for(const unsigned char *p=(const unsigned char *)s+7;*p;p++){
  if(!((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||
       (*p>='0'&&*p<='9')||*p=='-'||*p=='_'||*p=='.'))return 0;
 }
 return 1;
}
static int peer_facet_key(const char *s){
 if(!s||!*s||strlen(s)>31)return 0;
 for(const unsigned char *p=(const unsigned char*)s;*p;p++)
  if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='-'))return 0;
 return 1;
}
int bf_peer_store_init(sqlite3 *db){
 /* Definitions are governed by the native owner. A new facet adds *no*
  * relationship, conversation, CMS publication or capability by itself. */
 return db&&PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_FACET)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_PARTICIPATION)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_PARTICIPATION_FEED)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_INTERESTS)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_CHOICE)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_MESSAGE)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_MESSAGE_THREAD)&&
 PEER_SQL(db,BF_STORE_PARTICIPATION_PEER_BLOCK_2)&&
 PEER_SQL(db,"INSERT OR IGNORE INTO peer_facet VALUES"
  "('personal','People & friendships','Meet other members through shared interests and introductions.',"
  "'life','BonfyreParticipation','introductions',1),"
  "('art','Art & making','Discover artists, exchange feedback, and find creative collaborators.',"
  "'life','BonfyreParticipation','introductions',1),"
  "('civic','Civic & community','Discuss issues and meet contributors without implying a vote or office.',"
  "'life','BonfyreParticipation','introductions',1),"
  "('projects','Projects & work','Connect around projects, practical skills, and possible contributions.',"
  "'work','BonfyreParticipation','introductions',1)");
}
static int facet_allowed(sqlite3 *db,const char *facet){
 sqlite3_stmt *s=NULL;int ok=0;
 if(!db||!peer_facet_key(facet))return 0;
 if(sqlite3_prepare_v2(db,"SELECT 1 FROM peer_facet WHERE id=?1 AND "
    "owner='BonfyreParticipation' AND scope='introductions' AND active=1",
    -1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,facet,-1,SQLITE_TRANSIENT);
 ok=sqlite3_step(s)==SQLITE_ROW;sqlite3_finalize(s);return ok;
}
int bf_peer_facets(sqlite3 *db,BfPeerFacet *out,size_t cap,size_t *count){
 sqlite3_stmt *s=NULL;size_t n=0;int result=1;
 if(count)*count=0;
 if(!db||!count||(cap&&!out)||cap>32)return 0;
 if(sqlite3_prepare_v2(db,"SELECT id,label,description,territory "
    "FROM peer_facet WHERE active=1 AND owner='BonfyreParticipation' "
    "AND scope='introductions' ORDER BY rowid LIMIT ?1",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_int64(s,1,(sqlite3_int64)cap);
 int step;
 while(n<cap&&(step=sqlite3_step(s))==SQLITE_ROW){
  const char *a=(const char*)sqlite3_column_text(s,0);
  const char *b=(const char*)sqlite3_column_text(s,1);
  const char *c=(const char*)sqlite3_column_text(s,2);
  const char *d=(const char*)sqlite3_column_text(s,3);
  if(!a||!b||!c||!d||strlen(a)>=sizeof(out[n].id)||
     strlen(b)>=sizeof(out[n].label)||strlen(c)>=sizeof(out[n].description)||
     strlen(d)>=sizeof(out[n].territory)||!peer_facet_key(a)){result=0;break;}
  snprintf(out[n].id,sizeof(out[n].id),"%s",a);
  snprintf(out[n].label,sizeof(out[n].label),"%s",b);
  snprintf(out[n].description,sizeof(out[n].description),"%s",c);
  snprintf(out[n].territory,sizeof(out[n].territory),"%s",d);n++;
 }
 sqlite3_finalize(s);
 if(result)*count=n;
 return result;
}
static int member(sqlite3 *db,const char *subject,const char *facet,unsigned *revision){
 sqlite3_stmt *s=NULL;int status=-1;
 if(revision)*revision=0;
 if(!peer_subject(subject)||!revision||!facet_allowed(db,facet))return -1;
 if(sqlite3_prepare_v2(db,"SELECT revision,state FROM peer_participation "
    "WHERE subject=?1 AND facet=?2",-1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,facet,-1,SQLITE_TRANSIENT);
 int step=sqlite3_step(s);
 if(step==SQLITE_DONE)status=0;
 else if(step==SQLITE_ROW){
  sqlite3_int64 value=sqlite3_column_int64(s,0);
  const char *state=(const char*)sqlite3_column_text(s,1);
  if(state&&value>0&&value<=UINT_MAX){
   *revision=(unsigned)value;
   if(!strcmp(state,"active"))status=1;
   if(!strcmp(state,"withdrawn"))status=2;
  }
 }
 sqlite3_finalize(s);return status;
}
int bf_peer_status(sqlite3 *db,const char *subject,const char *facet,unsigned *revision){
 return member(db,subject,facet,revision);
}
int bf_peer_join(sqlite3 *db,const char *subject,const char *facet,int enable,unsigned expected){
 unsigned revision=0;sqlite3_stmt *s=NULL;int ok=0;
 if(!peer_subject(subject)||!peer_facet_key(facet)||expected==UINT_MAX||!peer_tx(db))return 0;
 int previous=member(db,subject,facet,&revision);
 if(previous<0||revision!=expected||(enable&&previous==1)||
    (!enable&&previous!=1))goto done;
 if(previous==0){
  if(!enable||sqlite3_prepare_v2(db,"INSERT INTO peer_participation VALUES"
    "(?1,?2,1,'active')",-1,&s,NULL)!=SQLITE_OK)goto done;
  sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
  sqlite3_bind_text(s,2,facet,-1,SQLITE_TRANSIENT);
 }else{
  if(sqlite3_prepare_v2(db,"UPDATE peer_participation SET revision=revision+1,"
    "state=?1 WHERE subject=?2 AND facet=?3 AND revision=?4",
    -1,&s,NULL)!=SQLITE_OK)goto done;
  sqlite3_bind_text(s,1,enable?"active":"withdrawn",-1,SQLITE_STATIC);
  sqlite3_bind_text(s,2,subject,-1,SQLITE_TRANSIENT);
  sqlite3_bind_text(s,3,facet,-1,SQLITE_TRANSIENT);
  sqlite3_bind_int64(s,4,expected);
 }
 ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;
done:sqlite3_finalize(s);return peer_finish(db,ok);
}
static int peer_topic_has(const char *list,const char *topic){
 size_t len=strlen(topic);for(const char *p=list;*p;){const char *end=strchr(p,',');if(!end)end=p+strlen(p);
  if((size_t)(end-p)==len&&!memcmp(p,topic,len))return 1;p=*end?end+1:end;}return 0;
}
static int peer_topics_normalize(const char *input,char out[265]){
 if(!input||strlen(input)>512)return 0;out[0]=0;size_t used=0,count=0;
 for(const char *p=input;*p;){
  while(*p==' '||*p=='\t'||*p==',')p++;if(!*p)break;
  char token[33];size_t n=0;int space=0;
  while(*p&&*p!=','){unsigned char c=(unsigned char)*p++;
   if(c==' '||c=='\t'){space=n>0;continue;}
   if(c>='A'&&c<='Z')c=(unsigned char)(c-'A'+'a');
   if(!((c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'))return 0;
   if(space&&n&&token[n-1]!='-'&&c!='-'){if(n>=32)return 0;token[n++]='-';}space=0;
   if(n>=32)return 0;token[n++]=(char)c;
  }
  if(!n||token[0]=='-'||token[n-1]=='-')return 0;token[n]=0;
  if(++count>8||peer_topic_has(out,token)||used+n+(used?1:0)>=265)return 0;
  if(used)out[used++]=',';memcpy(out+used,token,n);used+=n;out[used]=0;
 }
 return 1;
}
int bf_peer_interests_read(sqlite3 *db,const char *subject,const char *facet,char topics[265],unsigned *revision){
 sqlite3_stmt *s=NULL;unsigned epoch=0;int ok=0;if(revision)*revision=0;if(topics)topics[0]=0;
 if(!topics||!revision||member(db,subject,facet,&epoch)!=1)return 0;
 if(sqlite3_prepare_v2(db,"SELECT participation_revision,revision,topics FROM peer_interests WHERE subject=?1 AND facet=?2",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,facet,-1,SQLITE_TRANSIENT);int step=sqlite3_step(s);
 if(step==SQLITE_DONE)ok=1;else if(step==SQLITE_ROW){
  sqlite3_int64 stored_epoch=sqlite3_column_int64(s,0),rev=sqlite3_column_int64(s,1);const char *value=(const char*)sqlite3_column_text(s,2);
  if(rev>0&&rev<=UINT_MAX&&value&&strlen(value)<265){*revision=(unsigned)rev;if(stored_epoch==epoch)snprintf(topics,265,"%s",value);ok=1;}
 }
 sqlite3_finalize(s);return ok;
}
int bf_peer_interests_set(sqlite3 *db,const char *subject,const char *facet,const char *input,unsigned participation_revision,unsigned expected){
 char normalized[265],prior[265];unsigned epoch=0,current=0;sqlite3_stmt *s=NULL;int ok=0;
 if(expected==UINT_MAX||!peer_topics_normalize(input,normalized)||!peer_tx(db))return 0;
 if(member(db,subject,facet,&epoch)!=1||epoch!=participation_revision||
    !bf_peer_interests_read(db,subject,facet,prior,&current)||current!=expected)goto done;
 const char *sql="INSERT INTO peer_interests(subject,facet,participation_revision,revision,topics) VALUES(?1,?2,?3,?4,?5) "
  "ON CONFLICT(subject,facet) DO UPDATE SET participation_revision=excluded.participation_revision,revision=excluded.revision,topics=excluded.topics WHERE peer_interests.revision=?6";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,facet,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,3,epoch);sqlite3_bind_int64(s,4,(sqlite3_int64)expected+1);
 sqlite3_bind_text(s,5,normalized,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(s,6,expected);
 ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;
 done:sqlite3_finalize(s);return peer_finish(db,ok);
}
static void peer_shared_topics(sqlite3 *db,const char *a,const char *b,const char *facet,char out[265]){
 char mine[265],theirs[265];unsigned x,y;out[0]=0;
 if(!bf_peer_interests_read(db,a,facet,mine,&x)||!bf_peer_interests_read(db,b,facet,theirs,&y))return;
 size_t used=0;for(const char *p=mine;*p;){const char *end=strchr(p,',');if(!end)end=p+strlen(p);size_t n=(size_t)(end-p);
  char topic[33];if(n>0&&n<sizeof(topic)){memcpy(topic,p,n);topic[n]=0;if(peer_topic_has(theirs,topic)&&used+n+(used?2:0)<265){
   if(used){out[used++]=',';out[used++]=' ';}memcpy(out+used,topic,n);used+=n;out[used]=0;}}
  p=*end?end+1:end;
 }
}
static int blocked(sqlite3 *db,const char *a,const char *b){
 sqlite3_stmt *s=NULL;int yes=1;
 if(sqlite3_prepare_v2(db,"SELECT 1 FROM peer_block WHERE "
    "(subject=?1 AND target=?2) OR (subject=?2 AND target=?1) LIMIT 1",
    -1,&s,NULL)!=SQLITE_OK)return 1;
 sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
 int step=sqlite3_step(s);
 if(step==SQLITE_DONE)yes=0;
 sqlite3_finalize(s);return yes;
}
static BfPeerChoice choice_state(sqlite3 *db,const char *a,const char *b,
                                const char *facet,unsigned ar,unsigned br,
                                unsigned *revision){
 sqlite3_stmt *s=NULL;BfPeerChoice kind=BF_PEER_BLOCK;
 if(revision)*revision=0;
 if(sqlite3_prepare_v2(db,"SELECT choice,revision,subject_rev,target_rev "
  "FROM peer_choice WHERE subject=?1 AND target=?2 AND facet=?3",
  -1,&s,NULL)!=SQLITE_OK)return kind;
 sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,3,facet,-1,SQLITE_TRANSIENT);
 int step=sqlite3_step(s);
 if(step==SQLITE_DONE)kind=BF_PEER_NONE;
 else if(step==SQLITE_ROW){
  int val=sqlite3_column_int(s,0);
  sqlite3_int64 rev=sqlite3_column_int64(s,1);
  if(revision&&rev>0&&rev<=UINT_MAX)*revision=(unsigned)rev;
  if(val>=0&&val<=4){
   if((unsigned)sqlite3_column_int(s,2)==ar&&
      (unsigned)sqlite3_column_int(s,3)==br)kind=(BfPeerChoice)val;
   else kind=BF_PEER_NONE; /* old consent epoch never grants permission */
  }
 }
 sqlite3_finalize(s);return kind;
}
int bf_peer_choice(sqlite3 *db,const char *actor,const char *facet,
                   const char *target,BfPeerChoice requested,unsigned expected){
 unsigned ar=0,br=0,current=0;sqlite3_stmt *s=NULL;int ok=0;
 if(!peer_subject(actor)||!peer_subject(target)||!strcmp(actor,target)||
    requested<BF_PEER_NONE||requested>BF_PEER_BLOCK||expected==UINT_MAX||!peer_tx(db))return 0;
 int own=member(db,actor,facet,&ar),other=member(db,target,facet,&br);
 if(own!=1||((requested!=BF_PEER_BLOCK)&&other!=1))goto done;
 BfPeerChoice mine=choice_state(db,actor,target,facet,ar,br,&current);
 if(current!=expected||blocked(db,actor,target))goto done;
 if(requested==BF_PEER_ACCEPT){
  BfPeerChoice theirs=choice_state(db,target,actor,facet,br,ar,NULL);
  if(theirs!=BF_PEER_INTEREST&&theirs!=BF_PEER_ACCEPT&&
    mine!=BF_PEER_INTEREST&&mine!=BF_PEER_ACCEPT)goto done;
 }
 if(requested==BF_PEER_BLOCK){
  if(sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO peer_block(subject,target) "
    "VALUES(?1,?2)",-1,&s,NULL)!=SQLITE_OK)goto done;
  sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
  sqlite3_bind_text(s,2,target,-1,SQLITE_TRANSIENT);
  if(sqlite3_step(s)!=SQLITE_DONE)goto done;
  sqlite3_finalize(s);s=NULL;
 }
 if(sqlite3_prepare_v2(db,"INSERT INTO peer_choice"
   "(subject,target,facet,choice,revision,subject_rev,target_rev)"
   "VALUES(?1,?2,?3,?4,?5,?6,?7)"
   " ON CONFLICT(subject,target,facet) DO UPDATE SET choice=excluded.choice,"
   "revision=excluded.revision,subject_rev=excluded.subject_rev,"
   "target_rev=excluded.target_rev",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,target,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,3,facet,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int(s,4,requested);
 sqlite3_bind_int64(s,5,(sqlite3_int64)expected+1);
 sqlite3_bind_int64(s,6,ar);sqlite3_bind_int64(s,7,br);
 ok=sqlite3_step(s)==SQLITE_DONE;
done:sqlite3_finalize(s);return peer_finish(db,ok);
}
int bf_peer_mutual(sqlite3 *db,const char *a,const char *b,const char *facet){
 unsigned ar=0,br=0;
 return peer_subject(a)&&peer_subject(b)&&strcmp(a,b)&&
   member(db,a,facet,&ar)==1&&member(db,b,facet,&br)==1&&
   !blocked(db,a,b)&&
   choice_state(db,a,b,facet,ar,br,NULL)==BF_PEER_ACCEPT&&
   choice_state(db,b,a,facet,br,ar,NULL)==BF_PEER_ACCEPT;
}
static int peer_thread(sqlite3 *db,const char *a,const char *b,const char *facet,
                       unsigned *ar,unsigned *br,unsigned *ac,unsigned *bc){
 if(!peer_subject(a)||!peer_subject(b)||!strcmp(a,b)||
    member(db,a,facet,ar)!=1||member(db,b,facet,br)!=1||blocked(db,a,b))return 0;
 return choice_state(db,a,b,facet,*ar,*br,ac)==BF_PEER_ACCEPT&&
        choice_state(db,b,a,facet,*br,*ar,bc)==BF_PEER_ACCEPT;
}
static int peer_message_body(const char *body){
 if(!body||!*body||strlen(body)>1000)return 0;
 for(const unsigned char *p=(const unsigned char*)body;*p;p++)
  if((*p<32&&*p!=9&&*p!=10&&*p!=13)||*p==127)return 0;
 return 1;
}
int bf_peer_send(sqlite3 *db,const char *actor,const char *facet,const char *target,const char *nonce,const char *body){
 unsigned ar=0,br=0,ac=0,bc=0;sqlite3_stmt *s=NULL;int ok=0;
 if(!nonce||strlen(nonce)!=32||strspn(nonce,"0123456789abcdef")!=32||!peer_message_body(body)||!peer_tx(db))return 0;
 if(!peer_thread(db,actor,target,facet,&ar,&br,&ac,&bc))goto done;
 if(sqlite3_prepare_v2(db,"SELECT recipient,facet,body,sender_epoch,recipient_epoch,sender_choice_revision,recipient_choice_revision FROM peer_message WHERE sender=?1 AND nonce=?2",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,nonce,-1,SQLITE_TRANSIENT);
 int step=sqlite3_step(s);if(step==SQLITE_ROW){
  const char *to=(const char*)sqlite3_column_text(s,0),*f=(const char*)sqlite3_column_text(s,1),*text=(const char*)sqlite3_column_text(s,2);
  ok=to&&f&&text&&!strcmp(to,target)&&!strcmp(f,facet)&&!strcmp(text,body)&&
    sqlite3_column_int64(s,3)==ar&&sqlite3_column_int64(s,4)==br&&sqlite3_column_int64(s,5)==ac&&sqlite3_column_int64(s,6)==bc;goto done;
 }if(step!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;
 if(sqlite3_prepare_v2(db,"SELECT sum(sent_at>CAST(strftime('%s','now') AS INTEGER)-60),count(*) FROM peer_message WHERE sender=?1 AND sent_at>CAST(strftime('%s','now') AS INTEGER)-86400",-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_int(s,0)>=4||sqlite3_column_int(s,1)>=50)goto done;
 sqlite3_finalize(s);s=NULL;
 const char *sql="INSERT INTO peer_message(facet,sender,recipient,nonce,body,sent_at,sender_epoch,recipient_epoch,sender_choice_revision,recipient_choice_revision) VALUES(?1,?2,?3,?4,?5,CAST(strftime('%s','now') AS INTEGER),?6,?7,?8,?9)";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,facet,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,actor,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,target,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,nonce,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,body,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,6,ar);sqlite3_bind_int64(s,7,br);sqlite3_bind_int64(s,8,ac);sqlite3_bind_int64(s,9,bc);
 ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;
 done:sqlite3_finalize(s);return peer_finish(db,ok);
}
int bf_peer_messages(sqlite3 *db,const char *actor,const char *facet,const char *target,BfPeerMessage *out,size_t cap,size_t *count){
 unsigned ar=0,br=0,ac=0,bc=0;sqlite3_stmt *s=NULL;size_t n=0;int ok=0,step;
 if(count)*count=0;if(!db||!count||(cap&&!out)||cap>50)return 0;
 if(sqlite3_exec(db,"SAVEPOINT peer_thread_read",NULL,NULL,NULL)!=SQLITE_OK)return 0;
 if(!peer_thread(db,actor,target,facet,&ar,&br,&ac,&bc))goto done;
 const char *sql="SELECT id,sender,body,sent_at FROM peer_message WHERE facet=?1 AND ((sender=?2 AND recipient=?3 AND sender_epoch=?4 AND recipient_epoch=?5 AND sender_choice_revision=?6 AND recipient_choice_revision=?7) OR (sender=?3 AND recipient=?2 AND sender_epoch=?5 AND recipient_epoch=?4 AND sender_choice_revision=?7 AND recipient_choice_revision=?6)) ORDER BY id DESC LIMIT ?8";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(s,1,facet,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,actor,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,target,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,4,ar);sqlite3_bind_int64(s,5,br);sqlite3_bind_int64(s,6,ac);sqlite3_bind_int64(s,7,bc);sqlite3_bind_int64(s,8,(sqlite3_int64)cap);
 while((step=sqlite3_step(s))==SQLITE_ROW){const char *from=(const char*)sqlite3_column_text(s,1),*body=(const char*)sqlite3_column_text(s,2);
  if(n>=cap||!from||!body||strlen(from)>=sizeof(out[n].sender)||strlen(body)>=sizeof(out[n].body))goto done;
  out[n].id=sqlite3_column_int64(s,0);out[n].at=sqlite3_column_int64(s,3);snprintf(out[n].sender,sizeof(out[n].sender),"%s",from);snprintf(out[n].body,sizeof(out[n].body),"%s",body);n++;}
 if(step==SQLITE_DONE)ok=1;
 done:sqlite3_finalize(s);if(sqlite3_exec(db,"RELEASE peer_thread_read",NULL,NULL,NULL)!=SQLITE_OK)ok=0;if(ok)*count=n;return ok;
}
int bf_peer_candidates(sqlite3 *db,const char *actor,const char *facet,
                       BfPeerCandidate *out,size_t cap,size_t *count){
 sqlite3_stmt *s=NULL;unsigned ar=0,br=0;size_t n=0;int ok=1;
 if(count)*count=0;
 if(!count||!db||(cap&&!out)||cap>24||member(db,actor,facet,&ar)!=1)return 0;
 /* Order the whole eligible set before taking the bounded page. Both choice
  * joins are constrained to the current participation epochs, so withdrawn
  * permissions cannot gain priority or hide a current introduction. */
 const char *query=
  "SELECT p.subject FROM peer_participation p "
  "LEFT JOIN peer_choice mine ON mine.subject=?2 AND mine.target=p.subject AND mine.facet=?1 AND mine.subject_rev=?3 AND mine.target_rev=p.revision "
  "LEFT JOIN peer_choice theirs ON theirs.subject=p.subject AND theirs.target=?2 AND theirs.facet=?1 AND theirs.subject_rev=p.revision AND theirs.target_rev=?3 "
  "WHERE p.facet=?1 AND p.state='active' AND p.subject<>?2 AND p.subject GLOB 'person:?*' AND length(p.subject)<=191 "
  "AND substr(p.subject,8) NOT GLOB '*[^a-zA-Z0-9_.-]*' AND COALESCE(mine.choice,0) NOT IN (1,4) AND COALESCE(theirs.choice,0) NOT IN (1,4) "
  "AND NOT EXISTS(SELECT 1 FROM peer_block b WHERE (b.subject=?2 AND b.target=p.subject) OR (b.subject=p.subject AND b.target=?2)) "
  "ORDER BY CASE WHEN theirs.choice IN (2,3) AND COALESCE(mine.choice,0)<>3 THEN 3 "
  "WHEN mine.choice=3 AND theirs.choice=3 THEN 2 WHEN mine.choice IN (2,3) AND COALESCE(theirs.choice,0)<>3 THEN 1 ELSE 0 END DESC,p.subject LIMIT ?4";
 if(sqlite3_prepare_v2(db,query,-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,facet,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_int64(s,3,ar);sqlite3_bind_int64(s,4,(sqlite3_int64)cap);
 int step;
 while(n<cap&&(step=sqlite3_step(s))==SQLITE_ROW){
  const char *target=(const char*)sqlite3_column_text(s,0);
  if(!target||!peer_subject(target)||blocked(db,actor,target)||
    member(db,target,facet,&br)!=1)continue;
  unsigned choice_revision=0;
  BfPeerChoice mine=choice_state(db,actor,target,facet,ar,br,&choice_revision);
  BfPeerChoice theirs=choice_state(db,target,actor,facet,br,ar,NULL);
  if(mine==BF_PEER_PASS||theirs==BF_PEER_PASS||
     mine==BF_PEER_BLOCK||theirs==BF_PEER_BLOCK)continue;
  memset(&out[n],0,sizeof(out[n]));
  snprintf(out[n].subject,sizeof(out[n].subject),"%s",target);
  out[n].mine=mine;out[n].my_choice_revision=choice_revision;
  out[n].incoming=(theirs==BF_PEER_INTEREST||theirs==BF_PEER_ACCEPT)&&mine!=BF_PEER_ACCEPT;
  out[n].outgoing=(mine==BF_PEER_INTEREST||mine==BF_PEER_ACCEPT)&&theirs!=BF_PEER_ACCEPT;
  out[n].matched=mine==BF_PEER_ACCEPT&&theirs==BF_PEER_ACCEPT;
  peer_shared_topics(db,actor,target,facet,out[n].shared_topics);
  n++;
 }
 if(n<cap){ /* A failed SQLite step is not proof of an empty feed. */
  step=sqlite3_errcode(db);
  if(step!=SQLITE_OK&&step!=SQLITE_DONE&&step!=SQLITE_ROW)ok=0;
 }
 sqlite3_finalize(s);
 if(!ok)return 0;
 /* The feed is ordered by decisions requiring attention, then established
  * relations, then other eligible members. No fabricated ranking score. */
 for(size_t i=1;i<n;i++)for(size_t j=i;j>0;j--){
  int a=out[j-1].incoming?3:out[j-1].matched?2:out[j-1].outgoing?1:0;
  int b=out[j].incoming?3:out[j].matched?2:out[j].outgoing?1:0;
  if(a>=b)break;
  BfPeerCandidate swap=out[j];out[j]=out[j-1];out[j-1]=swap;
 }
 *count=n;return 1;
}
