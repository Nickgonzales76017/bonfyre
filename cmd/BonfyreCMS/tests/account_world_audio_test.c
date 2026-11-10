#include "../src/account_world.h"
#include "../src/account_world_audio.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
static int count(sqlite3 *db,const char *sql){sqlite3_stmt *s=NULL;assert(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK);assert(sqlite3_step(s)==SQLITE_ROW);int n=sqlite3_column_int(s,0);sqlite3_finalize(s);return n;}
static int rows(sqlite3 *db,const char *subject,int table,const char *ref){sqlite3_stmt *s=NULL;int n=0;if(awa_rows(db,subject,"actor-19",1,"show",table,ref,&s)!=1)return -1;while(sqlite3_step(s)==SQLITE_ROW)n++;sqlite3_finalize(s);return n;}
int main(void){
 sqlite3 *db=NULL; assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
 assert(sqlite3_exec(db,"CREATE TABLE world(id INTEGER PRIMARY KEY,namespace TEXT,subject TEXT,status TEXT,world TEXT); INSERT INTO world VALUES(1,'actor-19','user:19','draft','first'),(2,'actor-20','user:20','draft','second')",NULL,NULL,NULL)==SQLITE_OK);
 assert(awa_initialize(db)==1); assert(awa_initialize(db)==1);
 assert(aw_bind(db,"user:19","actor-19",1)==1); assert(aw_bind(db,"user:20","actor-20",2)==1);
 const char *U="user:19",*N="actor-19";
 const char *ep1="fixture-sound/0123456789abcdef01234567",*ep2="fixture-sound/89abcdef0123456789abcdef",*other="fixture-sound/ffffffffffffffffffffffff";
 assert(aw_space_put(db,U,N,1,"show","The Levee Hour","2d","t","l",0)==1);                       /* rev 1 */
 assert(aw_space_style(db,U,N,1,"show","podcast","journal","A show",1)==1);                       /* rev 2 */
 assert(aw_item_put(db,U,N,1,"show",1,ep1,"episode","",2)==1);                                     /* rev 3 */
 assert(aw_item_put(db,U,N,1,"show",2,ep2,"episode","",3)==1);                                     /* rev 4 */
 /* programme record */
 assert(awa_sound_put(db,U,N,1,"show","radio","America/Chicago","en","Avery","Arts",0,"",4)==0);  /* kind is station|show */
 assert(awa_sound_put(db,U,N,1,"show","show","Chicago","en","Avery","Arts",0,"",4)==0);            /* zone needs Area/City */
 assert(awa_sound_put(db,U,N,1,"show","show","../etc/passwd","en","Avery","Arts",0,"",4)==0);
 assert(awa_sound_put(db,U,N,1,"show","show","America/Chicago","en","Avery","Arts",0,"https://x.test/live",4)==0); /* stream is a reference */
 assert(awa_sound_put(db,"user:20","actor-20",1,"show","show","America/Chicago","en","Avery","Arts",0,"",4)==0); /* another account */
 assert(awa_sound_put(db,U,N,1,"show","show","America/Chicago","en","Avery","Arts",0,"",3)==0);    /* stale */
 assert(awa_sound_put(db,U,N,1,"show","show","America/Chicago","en","Avery","Arts",0,"",4)==1);    /* rev 5 */
 /* schedule */
 assert(awa_slot_put(db,U,N,1,"show",1,8,0,60,"Night","","",5)==0);
 assert(awa_slot_put(db,U,N,1,"show",1,1,1440,60,"Night","","",5)==0);
 assert(awa_slot_put(db,U,N,1,"show",49,1,0,60,"Night","","",5)==0);
 assert(awa_slot_put(db,U,N,1,"show",1,1,1260,120,"Night Shift","Avery","",5)==1);                 /* rev 6 */
 assert(awa_slot_put(db,U,N,1,"show",2,3,600,60,"Morning","","space:other",6)==1);                 /* rev 7 */
 assert(awa_slot_remove(db,U,N,1,"show",2,7)==1);                                                  /* rev 8 */
 assert(awa_slot_remove(db,U,N,1,"show",2,8)==0);                                                  /* already gone: refused, revision kept */
 /* episodes only for items the space holds */
 assert(awa_episode_put(db,U,N,1,"show",other,1,1,"2026-10-01",8)==0);
 assert(awa_episode_put(db,U,N,1,"show",ep1,1,1,"2026-02-30",8)==0);
 assert(awa_episode_put(db,U,N,1,"show",ep1,1,1,"2026-10-01",8)==1);                               /* rev 9 */
 assert(awa_episode_put(db,U,N,1,"show",ep2,1,2,"2026-10-15",9)==1);                               /* rev 10 */
 /* marks */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"chapter",0,0,"Cold open","","",1,10)==1);                /* 11 */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"chapter",90000,0,"The river","","",1,11)==1);            /* 12 */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"track",30000,0,"Dawn chorus","Field recordist","",0,12)==1); /* 13, unverified */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"clip",90000,0,"x","","",1,13)==0);                       /* clips need an end */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"clip",90000,80000,"x","","",1,13)==0);                   /* end follows start */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"clip",90000,120000,"The levee at night","","",0,13)==1); /* 14 */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"source",95000,0,"Levee history","","javascript:alert(1)",1,14)==0);
 assert(awa_mark_put(db,U,N,1,"show",ep1,"source",95000,0,"Levee history","","https://example.test/levee",1,14)==1); /* 15 */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"person",0,0,"Avery","host","",1,15)==1);                 /* 16 */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"person",0,0,"Jordan","guest","https://example.test/jordan",1,16)==1); /* 17 */
 assert(awa_mark_put(db,U,N,1,"show",other,"chapter",0,0,"x","","",1,17)==0);                      /* not an item here */
 /* transcript import: unreviewed cues, voice tags become speakers */
 int imported=0;
 assert(awa_cues_import(db,U,N,1,"show",ep1,"not vtt",17,&imported)==0);
 const char *vtt="WEBVTT\n\n1\n00:00:01.000 --> 00:00:04.500\n<v Avery>Good evening from the levee.\n\n00:01:30.000 --> 00:01:35.250\n<v Jordan>The river carries <i>everything</i>\ndownstream.\n";
 assert(awa_cues_import(db,U,N,1,"show",ep1,vtt,17,&imported)==1&&imported==2);                    /* 18 */
 assert(count(db,"SELECT count(*) FROM account_world_mark WHERE kind='cue' AND checked=0 AND credit='Jordan' AND title='The river carries everything downstream.' AND start_ms=90000 AND end_ms=95250")==1);
 assert(awa_cues_import(db,U,N,1,"show",ep1,"WEBVTT\n\n00:00:05.000 --> 00:00:02.000\nbackwards\n",18,&imported)==0);
 sqlite3_int64 cue=0;{sqlite3_stmt *s=NULL;assert(sqlite3_prepare_v2(db,"SELECT id FROM account_world_mark WHERE kind='cue' AND start_ms=1000",-1,&s,NULL)==SQLITE_OK);assert(sqlite3_step(s)==SQLITE_ROW);cue=sqlite3_column_int64(s,0);sqlite3_finalize(s);}
 assert(awa_mark_check(db,U,N,1,"show",cue,1,18)==1);                                              /* 19 */
 assert(awa_mark_check(db,"user:20","actor-20",1,"show",cue,0,19)==0);
 /* re-import keeps reviewed cues and replaces the unreviewed ones */
 assert(awa_cues_import(db,U,N,1,"show",ep1,vtt,19,&imported)==1);                                 /* 20 */
 assert(count(db,"SELECT count(*) FROM account_world_mark WHERE kind='cue'")==2);
 assert(count(db,"SELECT count(*) FROM account_world_mark WHERE kind='cue' AND checked=1")==1);
 /* draft reads; nothing public before release */
 assert(rows(db,U,AWA_MARKS,ep1)==9); assert(rows(db,U,AWA_EPISODES,NULL)==2); assert(rows(db,U,AWA_SLOTS,NULL)==1);
 assert(rows(db,"user:20",AWA_MARKS,ep1)==-1);
 assert(rows(db,NULL,AWA_MARKS,ep1)==0); assert(rows(db,NULL,AWA_SOUND,NULL)==0);
 /* release copies the sound objects with the space, at the exact revision */
 assert(aw_space_release(db,U,N,1,"show",19)==0);
 assert(aw_space_release(db,U,N,1,"show",20)==1);
 assert(rows(db,NULL,AWA_SOUND,NULL)==1); assert(rows(db,NULL,AWA_SLOTS,NULL)==1); assert(rows(db,NULL,AWA_EPISODES,NULL)==2); assert(rows(db,NULL,AWA_MARKS,ep1)==9);
 /* a draft change stays private until the next release */
 assert(awa_mark_put(db,U,N,1,"show",ep1,"chapter",200000,0,"Late addition","","",1,20)==1);       /* 21 */
 assert(rows(db,NULL,AWA_MARKS,ep1)==9);
 assert(aw_space_release(db,U,N,1,"show",21)==1);
 assert(rows(db,NULL,AWA_MARKS,ep1)==10);
 /* correction: remove a mark, release again */
 sqlite3_int64 late=0;{sqlite3_stmt *s=NULL;assert(sqlite3_prepare_v2(db,"SELECT id FROM account_world_mark WHERE title='Late addition' AND released=0",-1,&s,NULL)==SQLITE_OK);assert(sqlite3_step(s)==SQLITE_ROW);late=sqlite3_column_int64(s,0);sqlite3_finalize(s);}
 assert(awa_mark_remove(db,U,N,1,"show",late,21)==1);                                              /* 22 */
 assert(aw_space_release(db,U,N,1,"show",22)==1);
 assert(rows(db,NULL,AWA_MARKS,ep1)==9);
 /* withdrawal drops every public copy; the draft stays */
 assert(aw_space_withdraw(db,U,N,1,"show")==1);
 assert(count(db,"SELECT (SELECT count(*) FROM account_world_mark WHERE released=1)+(SELECT count(*) FROM account_world_sound WHERE released=1)+(SELECT count(*) FROM account_world_slot WHERE released=1)+(SELECT count(*) FROM account_world_episode WHERE released=1)")==0);
 assert(rows(db,NULL,AWA_MARKS,ep1)==0); assert(rows(db,U,AWA_MARKS,ep1)==9);
 /* an ended World leaves the public address */
 assert(aw_space_release(db,U,N,1,"show",22)==1); assert(rows(db,NULL,AWA_SOUND,NULL)==1);
 assert(sqlite3_exec(db,"UPDATE world SET status='ended' WHERE id=1",NULL,NULL,NULL)==SQLITE_OK);
 assert(rows(db,NULL,AWA_SOUND,NULL)==0);
 assert(awa_mark_put(db,U,N,1,"show",ep1,"chapter",300000,0,"After the end","","",1,22)==0);       /* no writes to an ended World */
 /* capacity */
 assert(sqlite3_exec(db,"UPDATE world SET status='draft' WHERE id=1",NULL,NULL,NULL)==SQLITE_OK);
 int rev=22;for(int i=0;i<AWA_MARK_LIMIT-10;i++){char t[24];snprintf(t,sizeof(t),"c%d",i);assert(awa_mark_put(db,U,N,1,"show",ep2,"chapter",i*1000,0,t,"","",1,rev)==1);rev++;}
 assert(awa_mark_put(db,U,N,1,"show",ep2,"chapter",999000,0,"one too many","","",1,rev)==1);rev++;
 for(int i=0;i<9;i++){char t[24];snprintf(t,sizeof(t),"d%d",i);assert(awa_mark_put(db,U,N,1,"show",ep2,"chapter",900000+i,0,t,"","",1,rev)==1);rev++;}
 assert(awa_mark_put(db,U,N,1,"show",ep2,"chapter",950000,0,"over","","",1,rev)==0);
 printf("account world audio: ok\n");
 sqlite3_close(db); return 0;
}
