#include "../src/account_world.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
int main(void) {
 sqlite3 *db=NULL; sqlite3_stmt *row=NULL; assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
 assert(sqlite3_exec(db,"CREATE TABLE world(id INTEGER PRIMARY KEY,namespace TEXT,subject TEXT,status TEXT,world TEXT); INSERT INTO world VALUES(1,'actor-19','user:19','draft','first'),(2,'actor-20','user:20','draft','second'),(3,'actor-19','user:19','published','third')",NULL,NULL,NULL)==SQLITE_OK);
 assert(aw_initialize(db)==1); assert(aw_bind(db,"user:20","actor-20",1)==0);assert(aw_bind(db,"user:19","actor-19",3)==0);
 assert(aw_bind(db,"user:19","actor-19",1)==1);assert(aw_bind(db,"user:19","actor-19",1)==1);assert(aw_bind(db,"user:20","actor-20",2)==1);
 const char *ids[]={"gallery","room","archive"};const char *renders[]={"2d","3d","2d"};
 for(int i=0;i<3;i++)assert(aw_space_put(db,"user:19","actor-19",1,ids[i],ids[i],renders[i],"template:one","layout:one",0)==1);
 assert(aw_space_put(db,"user:20","actor-20",1,"room","takeover","2d","t","l",1)==0);
 assert(aw_space_get(db,"user:20","actor-20",1,"room",&row)==0&&row==NULL);
 assert(aw_space_get(db,"user:19","actor-19",1,"room",&row)==1);assert(!strcmp((const char*)sqlite3_column_text(row,1),"3d"));assert(sqlite3_column_int(row,4)==1);sqlite3_finalize(row);
 assert(aw_space_put(db,"user:19","actor-19",1,"room","New room","2d","template:two","layout:two",1)==1);
 assert(aw_space_put(db,"user:19","actor-19",1,"room","Stale","2d","t","l",1)==0);
 assert(aw_space_put(db,"user:19","actor-19",1,"room","Invalid","script","t","l",2)==0);
 for(int i=3;i<32;i++){char name[32];snprintf(name,sizeof(name),"space-%d",i);assert(aw_space_put(db,"user:19","actor-19",1,name,name,"2d","t","l",0)==1);}
 assert(aw_space_put(db,"user:19","actor-19",1,"overflow","overflow","2d","t","l",0)==0);
 assert(sqlite3_exec(db,"UPDATE world SET subject='user:20' WHERE id=1",NULL,NULL,NULL)==SQLITE_CONSTRAINT);
 assert(sqlite3_exec(db,"DELETE FROM world WHERE id=1",NULL,NULL,NULL)==SQLITE_CONSTRAINT);
 assert(aw_space_get(db,"user:19","actor-19",1,"room",&row)==1);sqlite3_finalize(row);
 /* account-world-2: style, items by reference, release and withdrawal. */
 const char *art="cleveland-artworks/0123456789abcdef01234567";
 assert(aw_space_style(db,"user:19","actor-19",1,"room","radio","zine","Night programme",1)==0); /* stale: room is at 2 */
 assert(aw_space_style(db,"user:19","actor-19",1,"room","Radio!","zine","x",2)==0);              /* medium must be a slug */
 assert(aw_space_style(db,"user:20","actor-20",1,"room","radio","zine","x",2)==0);               /* another account */
 assert(aw_space_style(db,"user:19","actor-19",1,"room","radio","zine","Night programme",2)==1);
 assert(aw_space_release(db,"user:19","actor-19",1,"room",3)==0);                                /* nothing in it yet */
 assert(aw_item_put(db,"user:19","actor-19",1,"room",1,"https://example.test/x","track","",3)==0); /* a URL is not a reference */
 assert(aw_item_put(db,"user:19","actor-19",1,"room",1,"space:room","portal","",3)==0);          /* no portal to itself */
 assert(aw_item_put(db,"user:19","actor-19",1,"room",1,"space:missing","portal","",3)==0);       /* no portal to nowhere */
 assert(aw_item_put(db,"user:19","actor-19",1,"room",25,art,"cover","",3)==0);
 assert(aw_item_put(db,"user:19","actor-19",1,"room",1,art,"cover","Hung by the door",3)==1);
 assert(aw_item_put(db,"user:19","actor-19",1,"room",2,"space:gallery","portal","",4)==1);
 assert(aw_items(db,"user:19","actor-19",1,"room",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,1),art));
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,1),"space:gallery"));
 assert(sqlite3_step(row)==SQLITE_DONE);sqlite3_finalize(row);
 assert(aw_public_space(db,"actor-19","room",&row)==0);                                          /* nothing public before release */
 assert(aw_space_release(db,"user:19","actor-19",1,"room",4)==0);                                 /* stale */
 assert(aw_space_release(db,"user:20","actor-20",1,"room",5)==0);
 assert(aw_space_release(db,"user:19","actor-19",1,"room",5)==1);
 assert(aw_public_space(db,"actor-19","room",&row)==1);
 assert(!strcmp((const char*)sqlite3_column_text(row,2),"radio")&&!strcmp((const char*)sqlite3_column_text(row,3),"zine")&&!strcmp((const char*)sqlite3_column_text(row,5),"public"));
 sqlite3_finalize(row);
 assert(aw_public_space(db,"actor-20","room",&row)==0);                                          /* another namespace sees nothing */
 /* A draft edit after release stays private until the next release. */
 assert(aw_item_put(db,"user:19","actor-19",1,"room",1,art,"cover","Changed caption",5)==1);
 assert(aw_public_items(db,"actor-19",1,"room",&row)==1);
 assert(sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,3),"Hung by the door"));sqlite3_finalize(row);
 assert(aw_space_detail(db,"user:19","actor-19",1,"room",&row)==1&&sqlite3_column_int(row,4)==5&&!strcmp((const char*)sqlite3_column_text(row,3),"public"));sqlite3_finalize(row);
 assert(aw_public_spaces(db,"actor-19",&row)==1&&sqlite3_step(row)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(row,0),"room")&&sqlite3_step(row)==SQLITE_DONE);sqlite3_finalize(row);
 /* A second World on the same site cannot claim the same public address. */
 assert(sqlite3_exec(db,"INSERT INTO world VALUES(4,'actor-19','user:19','draft','fourth')",NULL,NULL,NULL)==SQLITE_OK);
 assert(aw_bind(db,"user:19","actor-19",4)==1&&aw_space_put(db,"user:19","actor-19",4,"room","Twin","2d","t","l",0)==1);
 assert(aw_space_style(db,"user:19","actor-19",4,"room","radio","journal","",1)==1&&aw_item_put(db,"user:19","actor-19",4,"room",1,art,"cover","",2)==1);
 assert(aw_space_release(db,"user:19","actor-19",4,"room",3)==0);
 assert(aw_space_withdraw(db,"user:20","actor-20",1,"room")==0);
 assert(aw_space_withdraw(db,"user:19","actor-19",1,"room")==1);
 assert(aw_space_withdraw(db,"user:19","actor-19",1,"room")==0);
 assert(aw_public_items(db,"actor-19",1,"room",&row)==1&&sqlite3_step(row)==SQLITE_DONE);sqlite3_finalize(row);
 assert(aw_public_space(db,"actor-19","room",&row)==1&&!strcmp((const char*)sqlite3_column_text(row,5),"withdrawn"));sqlite3_finalize(row);
 assert(aw_space_release(db,"user:19","actor-19",4,"room",3)==1);                                 /* the address is free again */
 assert(aw_public_space(db,"actor-19","room",&row)==1&&!strcmp((const char*)sqlite3_column_text(row,3),"journal"));sqlite3_finalize(row);
 /* Ending the World takes every space under it off the public address. */
 assert(sqlite3_exec(db,"UPDATE world SET status='ended' WHERE id=4",NULL,NULL,NULL)==SQLITE_OK);
 assert(aw_public_space(db,"actor-19","room",&row)==1&&!strcmp((const char*)sqlite3_column_text(row,5),"withdrawn"));sqlite3_finalize(row);
 assert(aw_public_spaces(db,"actor-19",&row)==1&&sqlite3_step(row)==SQLITE_DONE);sqlite3_finalize(row);
 sqlite3_close(db);puts("account world: ok");return 0;
}
