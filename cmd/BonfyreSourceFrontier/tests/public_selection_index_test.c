#define main source_frontier_entry
#include "../src/main.c"
#undef main
static int checks;
static void check(int ok,const char *why){checks++;if(!ok){fprintf(stderr,"public selection index FAIL: %s\n",why);exit(1);}}
static int value(sqlite3 *db,const char *sql){sqlite3_stmt *st=NULL;check(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK,"query");check(sqlite3_step(st)==SQLITE_ROW,"row");int v=sqlite3_column_int(st,0);sqlite3_finalize(st);return v;}
int main(void){
    char path[]="/tmp/frontier-read-index-XXXXXX";int fd=mkstemp(path);check(fd>=0,"temporary path");close(fd);
    sqlite3 *db=open_db(path,1);check(db!=NULL,"legacy schema opens");
    check(value(db,"SELECT count(*) FROM sqlite_master WHERE name='channel_public_selection'")==0,"no index on absent legacy column");
    check(exec_sql(db,"ALTER TABLE channel ADD COLUMN carries TEXT DEFAULT '';INSERT INTO channel(channel_id,family_id,root_id,address,exact_address,state,material,census_source) VALUES('one','family','root','https://example.test/feed','https://example.test/feed','admitted','news','source')"),"measured carriage fixture");
    sqlite3_close(db);db=open_db(path,1);check(db!=NULL,"owner installs read index");
    check(value(db,"SELECT count(*) FROM pragma_index_info('channel_public_selection')")==9,"complete covering shape");
    check(value(db,"SELECT count(*) FROM channel WHERE state='admitted' AND census_source='source'")==1,"channel admission unchanged");
    int version=value(db,"PRAGMA schema_version");sqlite3_close(db);db=open_db(path,1);
    check(db&&value(db,"PRAGMA schema_version")==version,"idempotent migration");
    sqlite3_stmt *st=NULL;check(sqlite3_prepare_v2(db,"EXPLAIN QUERY PLAN SELECT census_source,root_id,protocol FROM channel WHERE state='admitted' AND material='news' ORDER BY first_seen,state,rowid",-1,&st,NULL)==SQLITE_OK,"plan");
    int covering=0;while(sqlite3_step(st)==SQLITE_ROW){const char *text=(const char*)sqlite3_column_text(st,3);if(text&&strstr(text,"COVERING INDEX channel_public_selection"))covering=1;}sqlite3_finalize(st);check(covering,"public selection avoids channel bodies");
    sqlite3_close(db);unlink(path);printf("SourceFrontier public read index: %d assertions: PASS\n",checks);return 0;
}
