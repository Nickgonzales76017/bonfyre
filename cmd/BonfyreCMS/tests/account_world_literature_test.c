#include "../src/account_world.h"
#include "../src/account_world_literature.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
/* account-world-literature-1: works, exact text, serial dates, interactive
 * edges, glosses, versions, release, reuse by key and readers' places. */
static sqlite3 *db;
static int count(const char *sql){sqlite3_stmt *s=NULL;assert(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK);assert(sqlite3_step(s)==SQLITE_ROW);int n=sqlite3_column_int(s,0);sqlite3_finalize(s);return n;}
static int rev(const char *space){char q[200];snprintf(q,sizeof(q),"SELECT revision FROM account_world_space WHERE world_id=1 AND space_id='%s'",space);return count(q);}
static int rows(const char *subject,const char *space,int table,const char *work){sqlite3_stmt *s=NULL;int n=0;if(awl_rows(db,subject,"actor-19",1,space,table,work,&s)!=1)return -1;while(sqlite3_step(s)==SQLITE_ROW)n++;sqlite3_finalize(s);return n;}
#define U "user:19"
#define N "actor-19"
#define WORK(space,work,kind,rights,licence,prov,orig,lang) awl_work_put(db,U,N,1,space,work,kind,"Title " work,"Avery Lane","",lang,orig,"Night poems",rights,licence,prov,"",rec,1,rev(space))
static const char *rec="";
int main(void){
 assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
 assert(sqlite3_exec(db,"CREATE TABLE world(id INTEGER PRIMARY KEY,namespace TEXT,subject TEXT,status TEXT,world TEXT); INSERT INTO world VALUES(1,'actor-19','user:19','draft','first'),(2,'actor-20','user:20','draft','second')",NULL,NULL,NULL)==SQLITE_OK);
 assert(awl_initialize(db)==1); assert(awl_initialize(db)==1);
 assert(aw_bind(db,U,N,1)==1); assert(aw_bind(db,"user:20","actor-20",2)==1);
 assert(aw_space_put(db,U,N,1,"journal","The Night Journal","2d","t","l",0)==1);
 assert(aw_space_put(db,U,N,1,"room","Reading room","3d","t","l",0)==1);

 /* works: kinds, rights, provenance */
 assert(WORK("journal","river","sonnet","own","All rights reserved","","","en")==0);              /* unknown kind */
 assert(WORK("journal","chariot","poem","public-domain","Public domain","","","en")==0);         /* public domain needs where it came from */
 assert(WORK("journal","chariot","poem","public-domain","CC BY 4.0","https://en.wikisource.org/wiki/The_Chariot","","en")==0); /* and says public domain */
 assert(WORK("journal","chariot","poem","public-domain","Public domain","javascript:alert(1)","","en")==0);
 assert(WORK("journal","chariot","poem","public-domain","Public domain in the United States","https://en.wikisource.org/wiki/Poems_(Dickinson)/The_Chariot","","en")==1);
 assert(WORK("journal","river","poem","own","All rights reserved","","","en")==1);
 assert(WORK("journal","rio","poem","own","All rights reserved","","nowhere","es")==0);          /* translates a work of this space */
 assert(WORK("journal","rio","poem","own","All rights reserved","","river","es")==1);
 assert(WORK("journal","ferry","serial","own","CC BY 4.0","","","en")==1);
 assert(WORK("journal","door","interactive","own","CC BY-SA 4.0","","","en")==1);
 rec="bad ref"; assert(WORK("journal","river","poem","own","All rights reserved","","","en")==0); rec="fixture-sound/0123456789abcdef01234567";
 assert(WORK("journal","river","poem","own","All rights reserved","","","en")==1);
 assert(awl_work_put(db,"user:20","actor-20",1,"journal","x","poem","T","A","","en","","","own","All rights reserved","","","",1,rev("journal"))==0); /* another account */
 assert(awl_work_put(db,U,N,1,"journal","x","poem","T","A","","en","","","own","All rights reserved","","","",1,rev("journal")-1)==0);       /* stale */
 assert(rows(U,"journal",AWL_WORKS,NULL)==5);
 assert(count("SELECT count(DISTINCT key) FROM account_world_work WHERE released=0")==5);

 /* exact text: indentation, blank stanza lines and tabs survive */
 const char *poem="The river keeps its counsel,\n    folding light\n\tinto the reeds.\n\nAt the ferry\n        no one waits.";
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"","",  "",0,rev("journal"))==0);    /* empty */
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"","x\r\ny","",0,rev("journal"))==0); /* no carriage returns */
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"",poem,"",1,rev("journal"))==0);     /* only interactive works have a start */
 assert(awl_passage_put(db,U,N,1,"journal","nowork","text",1,"",poem,"",0,rev("journal"))==0);
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"",poem,"",0,rev("journal"))==1);
 assert(count("SELECT count(*) FROM account_world_passage WHERE work='river' AND body='The river keeps its counsel,\n    folding light\n\tinto the reeds.\n\nAt the ferry\n        no one waits.'")==1);
 assert(awl_units("poem",poem)==6 && awl_units("story","One.\nstill one.\n\n\nTwo.\n \nThree.")==3);
 /* glosses on lines that exist */
 assert(awl_gloss_put(db,U,N,1,"journal","river","text",7,"note","x",rev("journal"))==0);
 assert(awl_gloss_put(db,U,N,1,"journal","river","text",2,"aside","x",rev("journal"))==0);
 assert(awl_gloss_put(db,U,N,1,"journal","river","text",2,"footnote","Written on the Natchez ferry.",rev("journal"))==1);
 assert(awl_gloss_put(db,U,N,1,"journal","river","text",6,"note","The ferry stopped running in 1961.",rev("journal"))==1);
 /* versions: an edit keeps the text it replaced, and cannot strand a gloss */
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"","Too short.","",0,rev("journal"))==0);
 const char *poem2="The river keeps its counsel,\n    folding the light\n\tinto the reeds.\n\nAt the ferry\n        no one waits.";
 int before=rev("journal");
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"",poem2,"",0,before)==1);
 sqlite3_stmt *s=NULL; assert(awl_versions(db,U,N,1,"journal","river","text",&s)==1); assert(sqlite3_step(s)==SQLITE_ROW);
 assert(sqlite3_column_int(s,0)==before && !strcmp((const char*)sqlite3_column_text(s,1),poem)); sqlite3_finalize(s);
 assert(awl_versions(db,"user:20","actor-20",1,"journal","river","text",&s)==0);
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"",poem2,"",0,rev("journal"))==1);   /* unchanged text keeps no version */
 assert(count("SELECT count(*) FROM account_world_passage_version WHERE work='river'")==1);

 /* public-domain poem and a translation */
 const char *chariot="Because I could not stop for Death,\nHe kindly stopped for me;\nThe carriage held but just ourselves\nAnd Immortality.";
 assert(awl_passage_put(db,U,N,1,"journal","chariot","text",1,"",chariot,"",0,rev("journal"))==1);
 assert(awl_passage_put(db,U,N,1,"journal","rio","texto",1,"","El río guarda su consejo.","",0,rev("journal"))==1);

 /* serial: two chapters out, one scheduled */
 assert(awl_passage_put(db,U,N,1,"journal","ferry","one",1,"The landing","Chapter one.\n\nThe ferry came at dusk.","2026-02-30",0,rev("journal"))==0);
 assert(awl_passage_put(db,U,N,1,"journal","ferry","one",1,"The landing","Chapter one.\n\nThe ferry came at dusk.","2026-10-01",0,rev("journal"))==1);
 assert(awl_passage_put(db,U,N,1,"journal","ferry","two",2,"Crossing","Chapter two.\n\nNobody paid the fare.","2026-10-15",0,rev("journal"))==1);
 assert(awl_passage_put(db,U,N,1,"journal","ferry","three",3,"The far bank","Chapter three.","2999-01-01",0,rev("journal"))==1);

 /* interactive: declared edges only */
 assert(awl_passage_put(db,U,N,1,"journal","door","hall",1,"The hall","Two doors stand open.","",1,rev("journal"))==1);
 assert(awl_passage_put(db,U,N,1,"journal","door","garden",2,"The garden","Rain on the roses. The end.","",0,rev("journal"))==1);
 assert(awl_passage_put(db,U,N,1,"journal","door","cellar",3,"The cellar","Dark stairs. The end.","",0,rev("journal"))==1);
 assert(awl_passage_put(db,U,N,1,"journal","door","attic",4,"The attic","Nobody can reach this.","",0,rev("journal"))==1);
 assert(awl_choice_put(db,U,N,1,"journal","door","hall",1,"Go into the garden","nowhere",rev("journal"))==0);
 assert(awl_choice_put(db,U,N,1,"journal","ferry","one",1,"Skip","two",rev("journal"))==0);      /* only interactive works branch */
 assert(awl_choice_put(db,U,N,1,"journal","door","hall",10,"x","garden",rev("journal"))==0);
 assert(awl_choice_put(db,U,N,1,"journal","door","hall",1,"Go into the garden","garden",rev("journal"))==1);
 assert(awl_choice_put(db,U,N,1,"journal","door","hall",2,"Take the cellar stairs","cellar",rev("journal"))==1);
 assert(awl_passage_remove(db,U,N,1,"journal","door","garden",rev("journal"))==0);               /* a choice leads there */
 assert(awl_work_put(db,U,N,1,"journal","door","story","Title door","Avery Lane","","en","","","own","CC BY-SA 4.0","","","",1,rev("journal"))==0); /* has choices */
 assert(awl_passage_put(db,U,N,1,"journal","door","garden",2,"The garden","Rain on the roses. The end.","",1,rev("journal"))==1); /* moving the start */
 assert(count("SELECT count(*) FROM account_world_passage WHERE work='door' AND start=1 AND released=0")==1);
 assert(awl_passage_put(db,U,N,1,"journal","door","hall",1,"The hall","Two doors stand open.","",1,rev("journal"))==1);

 /* nothing is public before release */
 assert(rows(NULL,"journal",AWL_WORKS,NULL)==0);
 char sp[65],wk[65]; sqlite3_stmt *k=NULL; char key[25];
 assert(sqlite3_prepare_v2(db,"SELECT key FROM account_world_work WHERE work='river' AND released=0",-1,&k,NULL)==SQLITE_OK&&sqlite3_step(k)==SQLITE_ROW);
 snprintf(key,sizeof(key),"%s",(const char*)sqlite3_column_text(k,0)); sqlite3_finalize(k);
 assert(awl_public_key(db,N,1,key,sp,wk)==0);
 assert(awl_place_put(db,"user:30",N,"journal","river","text",1,"")==0);
 /* the foundation releases a styled space with at least one item: the poem placed on its own page by key */
 char ref[40]; snprintf(ref,sizeof(ref),"works/%s",key);
 assert(aw_space_style(db,U,N,1,"journal","poetry","journal","Poems and a serial",rev("journal"))==1);
 assert(aw_item_put(db,U,N,1,"journal",1,ref,"poem","",rev("journal"))==1);
 assert(aw_space_release(db,U,N,1,"journal",rev("journal"))==1);
 assert(rows(NULL,"journal",AWL_WORKS,NULL)==5 && rows(NULL,"journal",AWL_PASSAGES,NULL)==10 && rows(NULL,"journal",AWL_CHOICES,"door")==2 && rows(NULL,"journal",AWL_GLOSSES,"river")==2);
 assert(awl_public_key(db,N,1,key,sp,wk)==1 && !strcmp(sp,"journal") && !strcmp(wk,"river"));
 assert(count("SELECT count(*) FROM account_world_work a JOIN account_world_work b ON a.key=b.key AND a.released=0 AND b.released=1")==5);
 /* the same work shown in a second space by key, without a copy */
 assert(aw_item_put(db,U,N,1,"room",1,ref,"poem","",rev("room"))==1);
 assert(count("SELECT count(*) FROM account_world_passage WHERE space_id='room'")==0);

 /* private edits stay private until released again */
 assert(awl_passage_put(db,U,N,1,"journal","river","text",1,"","Draft only.","",0,rev("journal"))==0); /* glosses on lines 2 and 6 */
 assert(awl_gloss_put(db,U,N,1,"journal","chariot","text",1,"footnote","First published 1890 as The Chariot.",rev("journal"))==1);
 assert(rows(NULL,"journal",AWL_GLOSSES,"chariot")==0 && rows(U,"journal",AWL_GLOSSES,"chariot")==1);

 /* readers' places */
 assert(awl_place_put(db,"user:30",N,"journal","river","text",6,"")==1);
 assert(awl_place_put(db,"user:30",N,"journal","river","text",7,"")==0);                          /* past the last line */
 assert(awl_place_put(db,"user:30",N,"journal","river","text",2,"text")==0);                      /* only branching works have paths */
 assert(awl_place_put(db,"user:30",N,"journal","ferry","two",1,"")==1);
 assert(awl_place_put(db,"user:30",N,"journal","ferry","three",0,"")==0);                         /* not out yet */
 assert(awl_place_put(db,"user:30",N,"journal","door","garden",1,"")==0);                         /* needs the path */
 assert(awl_place_put(db,"user:30",N,"journal","door","garden",1,"garden")==0);                   /* must start at the first passage */
 assert(awl_place_put(db,"user:30",N,"journal","door","attic",1,"hall.attic")==0);                /* no such edge */
 assert(awl_place_put(db,"user:30",N,"journal","door","cellar",1,"hall.garden")==0);              /* path ends elsewhere */
 assert(awl_place_put(db,"user:30",N,"journal","door","garden",1,"hall.garden")==1);
 assert(awl_place_put(db,"user:31",N,"journal","door","hall",0,"hall")==1);
 sqlite3_stmt *p=NULL; int n=0; assert(awl_places(db,"user:30",N,"journal",&p)==1);
 while(sqlite3_step(p)==SQLITE_ROW){n++; if(!strcmp((const char*)sqlite3_column_text(p,0),"door"))assert(!strcmp((const char*)sqlite3_column_text(p,3),"hall.garden")&&sqlite3_column_int(p,2)==1);}
 sqlite3_finalize(p); assert(n==3);
 assert(awl_place_clear(db,"user:30",N,"journal","ferry")==1 && awl_place_clear(db,"user:30",N,"journal","ferry")==0);

 /* removing a work drops its passages, choices, glosses and versions */
 assert(awl_work_remove(db,U,N,1,"journal","river",rev("journal"))==1);
 assert(count("SELECT count(*) FROM account_world_passage WHERE work='river' AND released=0")==0 && count("SELECT count(*) FROM account_world_passage_version WHERE work='river'")==0);
 assert(count("SELECT count(*) FROM account_world_work WHERE work='rio' AND released=0 AND original=''")==1);
 assert(rows(NULL,"journal",AWL_PASSAGES,"river")==1);                                            /* still public until released again */

 /* withdrawal drops every public copy; places stop resolving */
 assert(aw_space_withdraw(db,U,N,1,"journal")==1);
 assert(rows(NULL,"journal",AWL_WORKS,NULL)==0 && count("SELECT count(*) FROM account_world_passage WHERE released=1")==0);
 assert(awl_public_key(db,N,1,key,sp,wk)==0);
 assert(awl_places(db,"user:30",N,"journal",&p)==0);
 assert(awl_place_put(db,"user:30",N,"journal","door","hall",0,"hall")==0);
 /* an ended World leaves the public copy unreachable */
 assert(aw_space_release(db,U,N,1,"journal",rev("journal"))==1);
 assert(rows(NULL,"journal",AWL_WORKS,NULL)==4);
 assert(sqlite3_exec(db,"UPDATE world SET status='archived' WHERE id=1",NULL,NULL,NULL)==SQLITE_OK);
 assert(rows(NULL,"journal",AWL_WORKS,NULL)==0);
 sqlite3_close(db);
 puts("account world literature: ok");
 return 0;
}
