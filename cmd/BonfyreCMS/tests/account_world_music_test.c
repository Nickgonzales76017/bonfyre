/* account-world-music-1: works, recordings, releases, tracks, credits,
 * relations, listening rooms and fan collections, owner-level. */
#include "../src/account_world.h"
#include "../src/account_world_audio.h"
#include "../src/account_world_music.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define assert(x) do{if(!(x)){fprintf(stderr,"account world music: FAIL line %d: %s\n",__LINE__,#x);exit(1);}}while(0)
static const char *U="user:7",*N="site-a";
static sqlite3 *gdb;
static int rev=0;
static int R(void){sqlite3_stmt *s=NULL;int r=0;if(sqlite3_prepare_v2(gdb,"SELECT revision FROM account_world_space WHERE world_id=1 AND space_id='band'",-1,&s,NULL)==SQLITE_OK&&sqlite3_step(s)==SQLITE_ROW)r=sqlite3_column_int(s,0);sqlite3_finalize(s);return r;}
#define OK(call) do{int _r=(call);if(_r!=1){fprintf(stderr,"account world music: FAIL line %d: %d\n",__LINE__,_r);exit(1);}}while(0)
#define NO(call) do{int _r=(call);if(_r!=0){fprintf(stderr,"account world music: FAIL line %d: expected refusal, got %d\n",__LINE__,_r);exit(1);}}while(0)
static int count(sqlite3 *db,const char *subject,int table,const char *key){
    sqlite3_stmt *s=NULL;int n=0;if(awm_rows(db,subject,N,1,"band",table,key,&s)!=1)return -1;while(sqlite3_step(s)==SQLITE_ROW)n++;sqlite3_finalize(s);return n;
}
static int scalar(sqlite3 *db,const char *sql){sqlite3_stmt *s=NULL;assert(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK);assert(sqlite3_step(s)==SQLITE_ROW);int n=sqlite3_column_int(s,0);sqlite3_finalize(s);return n;}
int main(void){
 sqlite3 *db=NULL;sqlite3_stmt *s=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);gdb=db;(void)rev;
 assert(sqlite3_exec(db,"CREATE TABLE world(id INTEGER PRIMARY KEY,namespace TEXT,subject TEXT,status TEXT,world TEXT);"
                        "INSERT INTO world VALUES(1,'site-a','user:7','draft','a'),(2,'site-b','user:8','draft','b')",NULL,NULL,NULL)==SQLITE_OK);
 assert(awm_initialize(db)==1);assert(awm_initialize(db)==1);
 assert(aw_bind(db,U,N,1)==1);assert(aw_bind(db,"user:8","site-b",2)==1);
 const char *p1="fixture-music/0123456789abcdef01234567",*p2="fixture-music/89abcdef0123456789abcdef",*cover="fixture-music/aaaaaaaaaaaaaaaaaaaaaaaa";
 OK(aw_space_put(db,U,N,1,"band","The Levee Band","2d","t","l",0));          /* rev 1 */

 OK(aw_space_put(db,U,N,1,"label","River Records","2d","t","l",0));   /* the label space keeps its own revision */
 int lrev=1;

 /* artist identity */
 NO(awm_party_put(db,U,N,1,"band","band","The Levee Band","","","",R()));
 NO(awm_party_put(db,U,N,1,"band","artist","The Levee Band","not-a-uuid","","",R()));
 NO(awm_party_put(db,U,N,1,"band","artist","The Levee Band","","","19x9",R()));
 NO(awm_party_put(db,"user:8","site-b",1,"band","artist","Takeover","","","",R()));
 NO(awm_party_put(db,U,N,1,"band","artist","Stale","","","",R()+5));
 OK(awm_party_put(db,U,N,1,"band","artist","The Levee Band","0383dadf-2a4e-4d10-a46a-e9e041da8eb3","New Orleans","2019",R()));
 assert(awm_party_put(db,U,N,1,"label","label","River Records","","Memphis","2001",lrev)==1);lrev++;

 /* works and recordings stay separate identities */
 OK(awm_work_put(db,U,N,1,"band","levee-song","Levee Song","T-345.246.800-1","",R()));
 OK(awm_work_put(db,U,N,1,"band","floodgates","Floodgates","","",R()));
 NO(awm_work_put(db,U,N,1,"band","bad","Bad","T-12","",R()));
 NO(awm_work_put(db,U,N,1,"band","Upper","Bad","","",R()));
 NO(awm_recording_put(db,U,N,1,"band","levee-take","nowhere","Levee Song",p1,"","",R()));      /* missing work */
 NO(awm_recording_put(db,U,N,1,"band","levee-take","levee-song","Levee Song","https://example.test/a.mp3","","",R())); /* a URL is not a piece */
 NO(awm_recording_put(db,U,N,1,"band","levee-take","levee-song","Levee Song",p1,"usab12000001","",R()));                /* ISRC shape */
 OK(awm_recording_put(db,U,N,1,"band","levee-take","levee-song","Levee Song",p1,"USAB12000001","",R()));
 OK(awm_recording_put(db,U,N,1,"band","floodgates-live","floodgates","Floodgates (live)",p2,"","",R()));

 /* credits: roles belong to their identity */
 OK(awm_credit_put(db,U,N,1,"band","work","levee-song","composer","Avery Lane","","",R()));
 OK(awm_credit_put(db,U,N,1,"band","work","levee-song","lyricist","Jo Reyes","","",R()));
 NO(awm_credit_put(db,U,N,1,"band","work","levee-song","vocals","Jo Reyes","","",R()));          /* a work has no singer */
 NO(awm_credit_put(db,U,N,1,"band","recording","levee-take","composer","Avery Lane","","",R()));  /* a recording has no composer */
 OK(awm_credit_put(db,U,N,1,"band","recording","levee-take","vocals","Jo Reyes","","space:band",R()));
 OK(awm_credit_put(db,U,N,1,"band","recording","levee-take","instrument","Avery Lane","piano","",R()));
 NO(awm_credit_put(db,U,N,1,"band","recording","missing","vocals","X","","",R()));
 NO(awm_credit_put(db,U,N,1,"band","recording","levee-take","vocals","X","","javascript:alert(1)",R()));

 /* a release with two tracks, a cover and liner notes; an edition */
 NO(awm_release_put(db,U,N,1,"band","levee","Levee","lp","2024-05-01","space:label","RR-001",cover,"Notes","","",R()));
 NO(awm_release_put(db,U,N,1,"band","levee","Levee","album","2024-02-30","space:label","RR-001",cover,"Notes","","",R()));
 NO(awm_release_put(db,U,N,1,"band","levee","Levee","album","2024","space:Bad Label","RR-001",cover,"Notes","","",R()));
 NO(awm_release_put(db,U,N,1,"band","levee","Levee","album","2024","","",cover,"Notes","levee","",R()));   /* not an edition of itself */
 OK(awm_release_put(db,U,N,1,"band","levee","Levee","album","2024-05","space:label","RR-001",cover,"Recorded in one room.\nMixed by the river.","","",R()));
 OK(awm_track_put(db,U,N,1,"band","levee",1,1,"levee-take",R()));
 OK(awm_track_put(db,U,N,1,"band","levee",1,2,"floodgates-live",R()));
 NO(awm_track_put(db,U,N,1,"band","levee",1,100,"levee-take",R()));
 NO(awm_track_put(db,U,N,1,"band","levee",1,3,"no-recording",R()));
 OK(awm_credit_put(db,U,N,1,"band","release","levee","artwork","Mara Okafor","","https://example.test/mara",R()));
 NO(awm_credit_put(db,U,N,1,"band","release","levee","composer","Mara Okafor","","",R()));
 OK(awm_release_put(db,U,N,1,"band","levee-deluxe","Levee (deluxe)","album","2025","space:label","RR-001D","","","levee","",R()));
 OK(awm_release_put(db,U,N,1,"band","empty","Empty","single","","","","","","","",R()));
 NO(awm_release_put(db,U,N,1,"band","double","Double","album","","","","","","levee-deluxe","",R()));     /* editions point at originals */
 NO(awm_release_ready(db,U,N,1,"band","empty",1,R()));                                                  /* nothing to listen to */
 OK(awm_release_ready(db,U,N,1,"band","levee",1,R()));
 NO(awm_release_ready(db,U,N,1,"band","levee",1,R()));

 /* history and rooms */
 OK(awm_relation_put(db,U,N,1,"band","signed-to","River Records","space:label","2020","",R()));
 OK(awm_relation_put(db,U,N,1,"band","founded","Avery Lane","","2019","",R()));
 NO(awm_relation_put(db,U,N,1,"band","signed-to","X","","2020","2019",R()));
 NO(awm_relation_put(db,U,N,1,"band","owned-by","X","","","",R()));
 NO(awm_room_put(db,U,N,1,"band","party","2026-11-07T25:00","release","levee","","",R()));
 NO(awm_room_put(db,U,N,1,"band","party","2026-11-07T20:00","release","nope","","",R()));
 NO(awm_room_put(db,U,N,1,"band","party","2026-11-07T20:00","release","levee","","http://example.test/t",R()));
 OK(awm_room_put(db,U,N,1,"band","party","2026-11-07T20:00","release","levee","Bring headphones","https://example.test/tickets",R()));
 OK(awm_room_put(db,U,N,1,"band","single-room","2026-11-08T20:00","recording","levee-take","","",R()));
 OK(awm_room_put(db,U,N,1,"band","scrap","2026-11-09T20:00","recording","levee-take","","",R()));
 OK(awm_room_state(db,U,N,1,"band","party","announced",R()));
 NO(awm_room_state(db,U,N,1,"band","party","announced",R()));
 OK(awm_room_state(db,U,N,1,"band","single-room","announced",R()));
 OK(awm_room_state(db,U,N,1,"band","scrap","cancelled",R()));   /* a cancelled draft is gone */
 assert(count(db,U,AWM_ROOMS,"scrap")==0);

 /* removals are refused while something depends on them */
 NO(awm_remove(db,U,N,1,"band","work","levee-song",R()));
 NO(awm_remove(db,U,N,1,"band","recording","levee-take",R()));
 NO(awm_remove(db,U,N,1,"band","release","levee",R()));
 OK(awm_remove(db,U,N,1,"band","release","empty",R()));

 /* nothing is public before the space is */
 assert(count(db,NULL,AWM_RELEASES,NULL)==-1||count(db,NULL,AWM_RELEASES,NULL)==0);
 assert(awm_collect(db,"user:9",N,"band","levee")==0);
 OK(aw_item_put(db,U,N,1,"band",1,p1,"track","",R()));
 OK(aw_space_style(db,U,N,1,"band","music","almanac","Songs from the river",R()));
 OK(aw_space_release(db,U,N,1,"band",R()));
 assert(aw_item_put(db,U,N,1,"label",1,"space:band","artist","",lrev)==1);lrev++;
 assert(aw_space_style(db,U,N,1,"label","label","ticker","",lrev)==1);lrev++;
 assert(aw_space_release(db,U,N,1,"label",lrev)==1);
 assert(count(db,NULL,AWM_PARTY,NULL)==1);
 assert(count(db,NULL,AWM_RELEASES,NULL)==1);              /* the edition is not ready */
 assert(count(db,NULL,AWM_TRACKS,"levee")==2);
 assert(count(db,NULL,AWM_RECORDINGS,NULL)==2);
 assert(count(db,NULL,AWM_WORKS,NULL)==2);
 assert(count(db,NULL,AWM_CREDITS,"recording:levee-take")==2);
 assert(count(db,NULL,AWM_CREDITS,"release:levee")==1);
 assert(count(db,NULL,AWM_CREDITS,"release:levee-deluxe")==0);
 assert(count(db,NULL,AWM_ROOMS,NULL)==2);
 assert(count(db,NULL,AWM_RELATIONS,NULL)==2);
 assert(awm_label_releases(db,N,1,"label",&s)==1);
 assert(sqlite3_step(s)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(s,1),"levee"));assert(sqlite3_step(s)==SQLITE_DONE);sqlite3_finalize(s);

 /* a draft change reopens review and stays private until released */
 OK(awm_credit_put(db,U,N,1,"band","recording","levee-take","producer","Sam Hill","","",R()));
 assert(count(db,U,AWM_RELEASES,"levee")==1);
 assert(awm_rows(db,U,N,1,"band",AWM_RELEASES,"levee",&s)==1&&sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,10)==0);sqlite3_finalize(s);
 assert(count(db,NULL,AWM_CREDITS,"recording:levee-take")==2);

 /* fans */
 assert(awm_collect(db,"user:9",N,"band","levee")==1);
 assert(awm_collect(db,"user:9",N,"band","levee")==0);
 assert(awm_collect(db,"user:9",N,"band","levee-deluxe")==0);  /* not ready */
 assert(awm_collect(db,"user:9","site-b","band","levee")==0);   /* wrong site */
 assert(awm_collect(db,"user:10",N,"band","levee")==1);
 assert(awm_collected(db,"user:9",N,"band","levee")==1&&awm_collected(db,"user:11",N,"band","levee")==0);
 assert(awm_collectors(db,"user:8","site-b",1,"band",&s)==0&&!s);
 assert(awm_collectors(db,U,N,1,"band",&s)==1&&sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,1)==2);sqlite3_finalize(s);
 assert(awm_collection(db,"user:9",&s)==1&&sqlite3_step(s)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(s,3),"Levee")&&sqlite3_column_int(s,7)==1);sqlite3_finalize(s);

 /* the same recording heard on a radio show in the same World */
 OK(aw_space_put(db,U,N,1,"show","Night Shift","2d","t","l",0)); int srev=1;
 const char *ep="fixture-sound/cccccccccccccccccccccccc";
 assert(aw_item_put(db,U,N,1,"show",1,ep,"episode","",srev)==1);srev++;
 assert(awa_mark_put(db,U,N,1,"show",ep,"track",754000,0,"Levee Song","The Levee Band",p1,1,srev)==1);srev++;
 assert(awm_heard_on(db,N,1,p1,&s)==1&&sqlite3_step(s)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(s,3),"holds")&&sqlite3_step(s)==SQLITE_DONE);sqlite3_finalize(s); /* the show is not yet public */
 assert(aw_space_style(db,U,N,1,"show","radio","ticker","",srev)==1);srev++;
 assert(aw_space_release(db,U,N,1,"show",srev)==1);
 assert(awm_heard_on(db,N,1,p1,&s)==1&&sqlite3_step(s)==SQLITE_ROW&&sqlite3_step(s)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(s,0),"show")&&
        !strcmp((const char*)sqlite3_column_text(s,3),"played")&&sqlite3_column_int(s,4)==754000);sqlite3_finalize(s);

 /* withdrawal drops every public copy; collections keep the reference as gone */
 assert(aw_space_withdraw(db,U,N,1,"band")==1);
 assert(scalar(db,"SELECT count(*) FROM account_world_music_release WHERE released=1")==0);
 assert(scalar(db,"SELECT count(*) FROM account_world_music_credit WHERE released=1")==0);
 assert(count(db,NULL,AWM_RELEASES,NULL)==0);
 assert(awm_collection(db,"user:9",&s)==1&&sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,7)==0);sqlite3_finalize(s);
 assert(awm_collect(db,"user:11",N,"band","levee")==0);
 assert(awm_uncollect(db,"user:9",N,"band","levee")==1&&awm_uncollect(db,"user:9",N,"band","levee")==0);

 /* caps */
 for(int i=0;i<AWM_RELATION_LIMIT-2;i++){char nm[32];snprintf(nm,sizeof(nm),"Friend %d",i);OK(awm_relation_put(db,U,N,1,"band","collaborated-with",nm,"","","",R()));}
 NO(awm_relation_put(db,U,N,1,"band","collaborated-with","One too many","","","",R()));
 sqlite3_close(db);puts("account world music: ok");return 0;
}
