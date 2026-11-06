#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "../src/front_recipe_query.h"
static int checks;
static void check(int ok,const char *why){checks++;if(!ok){fprintf(stderr,"recipe pick FAIL: %s\n",why);exit(1);}}
static void exec(sqlite3 *db,const char *text){check(sqlite3_exec(db,text,NULL,NULL,NULL)==SQLITE_OK,sqlite3_errmsg(db));}
static int pick(sqlite3 *db,const char *sql,const char *match,int offset,char *out,size_t cap,int *hits){
    sqlite3_stmt *s=NULL;int prior=0,high=0,rc;
    check(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK,"prepare selection");
    sqlite3_bind_text(s,1,match,-1,SQLITE_STATIC);sqlite3_bind_int(s,2,offset);
    sqlite3_db_status(db,SQLITE_DBSTATUS_CACHE_HIT,&prior,&high,1);
    rc=sqlite3_step(s);check(rc==SQLITE_ROW||rc==SQLITE_DONE,"selection completed");out[0]=0;
    if(rc==SQLITE_ROW)snprintf(out,cap,"%s|%s",sqlite3_column_text(s,0),sqlite3_column_text(s,1));
    sqlite3_db_status(db,SQLITE_DBSTATUS_CACHE_HIT,hits,&high,1);sqlite3_finalize(s);return rc;
}
int main(void){
    sqlite3 *db=NULL;check(sqlite3_open(":memory:",&db)==SQLITE_OK,"fixture");
    exec(db,"CREATE TABLE record(seq INTEGER PRIMARY KEY,record_key TEXT UNIQUE,title TEXT NOT NULL,keywords TEXT NOT NULL,payload BLOB);"
        "CREATE VIRTUAL TABLE record_search USING fts5(title,keywords,content='record',content_rowid='seq');"
        "WITH RECURSIVE n(i) AS(VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<12000)"
        "INSERT INTO record SELECT i*3,'opaque-'||i,CASE WHEN i%7=0 THEN 'Pumpkin cake' ELSE 'Pumpkin soup '||i END,'season',zeroblob(4096) FROM n;"
        "INSERT INTO record_search(record_search) VALUES('rebuild');");
    const char *old="SELECT r.record_key,r.title FROM record_search s JOIN record r ON r.seq=s.rowid WHERE record_search MATCH ?1 ORDER BY s.rowid LIMIT 1 OFFSET ?2";
    const char *match="title : (pumpkin) NOT title : (cake)";char a[256],b[256];int ah=0,bh=0;
    for(int offset=0;offset<12500;offset+=137){int ar=pick(db,old,match,offset,a,sizeof(a),&ah),br=pick(db,BF_FRONT_RECIPE_PICK_SQL,match,offset,b,sizeof(b),&bh);check(ar==br&&!strcmp(a,b),"exact key/title/ordinal and exclusions retained");}
    pick(db,old,match,9000,a,sizeof(a),&ah);pick(db,BF_FRONT_RECIPE_PICK_SQL,match,9000,b,sizeof(b),&bh);
    check(!strcmp(a,b)&&a[0],"late ordinal matches");check(ah>bh*20,"skipped record pages not read");int fast=bh;
    check(pick(db,BF_FRONT_RECIPE_PICK_SQL,"title : absent",0,b,sizeof(b),&bh)==SQLITE_DONE,"empty search stays empty");
    exec(db,"DELETE FROM record WHERE seq=3");
    check(pick(db,BF_FRONT_RECIPE_PICK_SQL,match,0,b,sizeof(b),&bh)==SQLITE_DONE,"missing selected record yields no invented link");
    sqlite3_close(db);printf("Recipe late materialization: %d assertions; cache hits %d -> %d: PASS\n",checks,ah,fast);return 0;
}
