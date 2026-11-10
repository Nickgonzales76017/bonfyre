/* account-world-cine: screenings, club and diary on a space, owner-level.
 * Two accounts each bind a World; a third subject joins a club. */
#include "../src/account_world.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
static int rows(sqlite3_stmt *s){int n=0;while(sqlite3_step(s)==SQLITE_ROW)n++;sqlite3_finalize(s);return n;}
int main(void) {
 sqlite3 *db=NULL;sqlite3_stmt *row=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
 assert(sqlite3_exec(db,"CREATE TABLE world(id INTEGER PRIMARY KEY,namespace TEXT,subject TEXT,status TEXT,world TEXT);"
                        "INSERT INTO world VALUES(1,'site-a','user:1','draft','a'),(2,'site-b','user:2','draft','b')",NULL,NULL,NULL)==SQLITE_OK);
 assert(aw_initialize(db)==1);assert(aw_initialize(db)==1);
 assert(aw_bind(db,"user:1","site-a",1)==1);assert(aw_bind(db,"user:2","site-b",2)==1);
 const char *film="fixture-films/0123456789abcdef01234567",*held="fixture-films/89abcdef0123456789abcdef";
 assert(aw_space_put(db,"user:1","site-a",1,"cinema","Cinema","2d","t","l",0)==1);
 assert(aw_space_put(db,"user:2","site-b",2,"cinema","Other cinema","2d","t","l",0)==1);

 /* programme: drafts, validation, isolation, announce and cancel */
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","matinee","2026-11-07T14:00","Main room",film,"Bring a friend")==1);
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","bad-date","2026-02-30T14:00","Main room",film,"")==0);
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","bad-time","2026-11-07T24:00","Main room",film,"")==0);
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","url","2026-11-07T14:00","Main room","https://example.test/x.webm","")==0);
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","portal","2026-11-07T14:00","Main room","space:cinema","")==0);
 assert(aw_screening_put(db,"user:1","site-a",1,"nowhere","x","2026-11-07T14:00","Main room",film,"")==0);
 assert(aw_screening_put(db,"user:2","site-b",1,"cinema","takeover","2026-11-07T14:00","x",film,"")==0);
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","late","2026-11-14T21:30","Roof",held,"")==1);
 assert(aw_screenings(db,"user:2","site-b",1,"cinema",&row)==0&&!row);
 assert(aw_screenings(db,"user:1","site-a",1,"cinema",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,0),"matinee")&&!strcmp((const char*)sqlite3_column_text(row,5),"draft"));
 sqlite3_finalize(row);
 assert(aw_screening_announce(db,"user:1","site-a",1,"cinema","matinee")==1);
 assert(aw_screening_announce(db,"user:1","site-a",1,"cinema","matinee")==0);
 assert(aw_screening_announce(db,"user:1","site-a",1,"cinema","late")==1);
 assert(aw_screening_cancel(db,"user:1","site-a",1,"cinema","late")==1);   /* announced -> cancelled */
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","late","2026-11-15T21:30","Roof",held,"")==0); /* cancelled stays */
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","gone","2026-11-20T19:00","Hall",film,"")==1);
 assert(aw_screening_cancel(db,"user:1","site-a",1,"cinema","gone")==1);   /* draft -> deleted */
 assert(aw_screening_cancel(db,"user:1","site-a",1,"cinema","gone")==0);
 for(int i=0;i<46;i++){char id[16];snprintf(id,sizeof(id),"s-%d",i);assert(aw_screening_put(db,"user:1","site-a",1,"cinema",id,"2027-01-01T10:00","Hall",film,"")==1);}
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","overflow","2027-01-01T10:00","Hall",film,"")==0);
 assert(aw_screening_put(db,"user:1","site-a",1,"cinema","s-0","2027-01-02T10:00","Hall",film,"")==1); /* edits within the cap */

 /* nothing public until the space is released */
 assert(aw_public_screenings(db,"site-a",1,"cinema",&row)==1&&rows(row)==0);
 assert(aw_space_style(db,"user:1","site-a",1,"cinema","film","almanac","Saturday films",1)==1);
 assert(aw_item_put(db,"user:1","site-a",1,"cinema",1,film,"feature","",2)==1);
 assert(aw_space_release(db,"user:1","site-a",1,"cinema",3)==1);
 assert(aw_public_screenings(db,"site-a",1,"cinema",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,0),"matinee"));
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,5),"cancelled"));
 assert(sqlite3_step(row)==SQLITE_DONE);sqlite3_finalize(row);   /* drafts never show */
 assert(aw_public_screenings(db,"site-b",1,"cinema",&row)==1&&rows(row)==0);

 /* club: closed until opened, members only, host moderates */
 char state[8];
 assert(aw_club_state(db,"user:1","site-a",1,"cinema",state)==1&&!*state);
 assert(aw_club_join(db,"user:3","site-a","cinema")==0);
 assert(aw_club_set(db,"user:2","site-b",1,"cinema",1)==0);
 assert(aw_club_set(db,"user:1","site-a",1,"cinema",1)==1);
 assert(aw_club_set(db,"user:1","site-a",1,"cinema",1)==0);
 assert(aw_club_state(db,"user:1","site-a",1,"cinema",state)==1&&!strcmp(state,"open"));
 assert(aw_club_view(db,"user:3","site-a","cinema",&row)==1&&!strcmp((const char*)sqlite3_column_text(row,4),""));sqlite3_finalize(row);
 assert(aw_club_posts(db,"user:3","site-a","cinema",0,&row)==0&&!row);   /* not a member yet */
 assert(aw_club_say(db,"user:3","site-a","cinema","","Hello")==0);
 assert(aw_club_join(db,"user:3","site-a","cinema")==1);
 assert(aw_club_join(db,"user:3","site-a","cinema")==0);
 assert(aw_club_join(db,"user:1","site-a","cinema")==0);                  /* the host is not a joiner */
 assert(aw_club_join(db,"user:4","site-a","cinema")==1);
 assert(aw_club_view(db,"user:1","site-a","cinema",&row)==1&&!strcmp((const char*)sqlite3_column_text(row,4),"host"));sqlite3_finalize(row);
 assert(aw_club_pick(db,"user:3","site-a","cinema",film,"For the matinee")==1);
 assert(aw_club_pick(db,"user:4","site-a","cinema",film,"")==0);           /* already proposed */
 assert(aw_club_pick(db,"user:4","site-a","cinema",held,"")==1);
 assert(aw_club_pick(db,"user:4","site-a","cinema","space:cinema","")==0);
 assert(aw_club_vote(db,"user:3","site-a","cinema",1)==1);
 assert(aw_club_vote(db,"user:3","site-a","cinema",1)==0);                 /* one vote each */
 assert(aw_club_vote(db,"user:4","site-a","cinema",1)==1);
 assert(aw_club_vote(db,"user:1","site-a","cinema",2)==1);
 assert(aw_club_vote(db,"user:9","site-a","cinema",1)==0);                 /* not a member */
 assert(aw_club_picks(db,"user:3","site-a","cinema",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&sqlite3_column_int(row,0)==1&&sqlite3_column_int(row,4)==2&&sqlite3_column_int(row,6)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&sqlite3_column_int(row,4)==1);sqlite3_finalize(row);
 assert(aw_club_say(db,"user:3","site-a","cinema",film,"The barn scene still works.")==1);
 assert(aw_club_say(db,"user:4","site-a","cinema","","Spam spam")==1);
 assert(aw_club_say(db,"user:4","site-a","cinema","","   ")==0);
 assert(aw_club_say(db,"user:4","site-a","cinema","https://x.test","link")==0);
 assert(aw_club_retract(db,"user:3","site-a","cinema",2)==0);              /* not the author */
 assert(aw_club_hide(db,"user:3","site-a",1,"cinema","post",2)==0);        /* members cannot moderate */
 assert(aw_club_hide(db,"user:1","site-a",1,"cinema","post",2)==1);
 assert(aw_club_posts(db,"user:3","site-a","cinema",0,&row)==1&&rows(row)==1);
 assert(aw_club_posts(db,"user:1","site-a","cinema",0,&row)==1&&rows(row)==2); /* host sees what it hid */
 assert(aw_club_retract(db,"user:3","site-a","cinema",1)==1);
 assert(aw_club_posts(db,"user:4","site-a","cinema",0,&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,4),"retracted")&&!*sqlite3_column_text(row,3));sqlite3_finalize(row);
 for(int i=0;i<25;i++)assert(aw_club_say(db,"user:3","site-a","cinema","","Page filler")==1);
 assert(aw_club_posts(db,"user:3","site-a","cinema",0,&row)==1&&rows(row)==AW_CLUB_PAGE+1);
 assert(aw_club_posts(db,"user:3","site-a","cinema",10,&row)==1&&rows(row)==8);    /* ids 1..9 minus hidden 2 */
 assert(aw_club_remove(db,"user:1","site-a",1,"cinema","user:4")==1);
 assert(aw_club_remove(db,"user:1","site-a",1,"cinema","user:1")==0);
 assert(aw_club_join(db,"user:4","site-a","cinema")==0);                  /* removed stays out */
 assert(aw_club_picks(db,"user:3","site-a","cinema",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&sqlite3_column_int(row,4)==1);sqlite3_finalize(row); /* user:4's vote no longer counts */
 assert(aw_club_leave(db,"user:3","site-a","cinema")==1);
 assert(aw_club_posts(db,"user:3","site-a","cinema",0,&row)==0);
 assert(aw_club_join(db,"user:3","site-a","cinema")==1);                  /* leaving is not removal */
 assert(aw_club_set(db,"user:1","site-a",1,"cinema",0)==1);
 assert(aw_club_view(db,"user:3","site-a","cinema",&row)==0);             /* a closed club is not shown */
 assert(aw_club_set(db,"user:1","site-a",1,"cinema",1)==1);

 /* diary: private by default, public notes only beside released films */
 assert(aw_note_put(db,"user:1","site-a",1,0,film,"2026-10-30",9,1,"Better on the third watch.","public")==1);
 assert(aw_note_put(db,"user:1","site-a",1,0,film,"2026-10-01",7,0,"Private thoughts.","private")==1);
 assert(aw_note_put(db,"user:1","site-a",1,0,held,"2026-10-29",6,0,"Not in this space.","public")==1);
 assert(aw_note_put(db,"user:1","site-a",1,0,film,"2026-13-01",7,0,"","private")==0);
 assert(aw_note_put(db,"user:1","site-a",1,0,film,"2026-10-01",11,0,"","private")==0);
 assert(aw_note_put(db,"user:1","site-a",1,0,film,"2026-10-01",5,0,"","friends")==0);
 assert(aw_note_put(db,"user:2","site-b",1,0,film,"2026-10-01",5,0,"x","public")==0);
 assert(aw_note_put(db,"user:2","site-b",1,1,film,"2026-10-01",5,0,"edit","public")==0); /* a note of another World */
 assert(aw_public_notes(db,"site-a",1,"cinema",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,4),"Better on the third watch.")&&sqlite3_column_int(row,3)==1);
 assert(sqlite3_step(row)==SQLITE_DONE);sqlite3_finalize(row);
 assert(aw_notes(db,"user:2","site-b",1,"",0,&row)==0);
 for(int i=0;i<30;i++)assert(aw_note_put(db,"user:1","site-a",1,0,film,"2026-09-01",0,0,"","private")==1);
 assert(aw_notes(db,"user:1","site-a",1,"",0,&row)==1);
 int n=0;char day[11]="";sqlite3_int64 last=0;
 while(sqlite3_step(row)==SQLITE_ROW){if(++n==AW_CLUB_PAGE){snprintf(day,sizeof(day),"%s",(const char*)sqlite3_column_text(row,2));last=sqlite3_column_int64(row,0);}}
 sqlite3_finalize(row);assert(n==AW_CLUB_PAGE+1);
 assert(aw_notes(db,"user:1","site-a",1,day,last,&row)==1&&rows(row)==33-AW_CLUB_PAGE);
 assert(aw_note_put(db,"user:1","site-a",1,1,film,"2026-10-30",9,1,"Now private.","private")==1);
 assert(aw_public_notes(db,"site-a",1,"cinema",&row)==1&&rows(row)==0);
 assert(aw_note_remove(db,"user:1","site-a",1,1)==1);assert(aw_note_remove(db,"user:1","site-a",1,1)==0);

 /* withdrawal and an ended World take the programme and club off the address */
 assert(aw_space_withdraw(db,"user:1","site-a",1,"cinema")==1);
 assert(aw_public_screenings(db,"site-a",1,"cinema",&row)==1&&rows(row)==0);
 assert(aw_club_view(db,"user:3","site-a","cinema",&row)==0);
 assert(aw_club_say(db,"user:3","site-a","cinema","","after")==0);
 sqlite3_close(db);puts("account world cinema: ok");return 0;
}
