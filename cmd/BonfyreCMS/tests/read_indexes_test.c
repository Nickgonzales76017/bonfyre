/* Exercise the real native CMS schema owner in a disposable store. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sqlite3.h>
static int checks;
static char dir[512], path[600], schema_path[600];
static const char *binary;
static void check(int ok, const char *why) {
    checks++; if (!ok) { fprintf(stderr,"read indexes: FAIL: %s\n",why); exit(1); }
}
static int apply(const char *extra) {
    FILE *f=fopen(schema_path,"w"); check(f!=NULL,"schema file");
    fprintf(f,"{\"name\":\"occurrence\",\"attributes\":{\"source_ref\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},%s\"displayName\":\"Fixture\"}",extra);
    check(!fclose(f),"schema written");
    pid_t pid=fork(); check(pid>=0,"fork");
    if (!pid) { int fd=open("/dev/null",O_WRONLY); dup2(fd,1); dup2(fd,2); close(fd);
        execl(binary,binary,"schema","apply",schema_path,"--db",path,"--schemas",dir,(char*)NULL); _exit(127); }
    int status=0; check(waitpid(pid,&status,0)==pid,"wait");
    return WIFEXITED(status)?WEXITSTATUS(status):128;
}
static void sql(sqlite3 *db,const char *text) { check(sqlite3_exec(db,text,NULL,NULL,NULL)==SQLITE_OK,sqlite3_errmsg(db)); }
static int number(sqlite3 *db,const char *text) {
    sqlite3_stmt *s=NULL; check(sqlite3_prepare_v2(db,text,-1,&s,NULL)==SQLITE_OK,"prepare");
    check(sqlite3_step(s)==SQLITE_ROW,"number row"); int n=sqlite3_column_int(s,0); sqlite3_finalize(s);return n;
}
static int query(sqlite3 *db,char *out,size_t cap) {
    sqlite3_stmt *s=NULL;size_t used=0;int rc;
    check(sqlite3_prepare_v2(db,"SELECT id,title,namespace FROM occurrence WHERE source_ref='source-7' AND status='published' ORDER BY published_at DESC,id DESC LIMIT 18",-1,&s,NULL)==SQLITE_OK,"page query");
    while((rc=sqlite3_step(s))==SQLITE_ROW){int n=snprintf(out+used,cap-used,"%d|%s|%s\n",sqlite3_column_int(s,0),sqlite3_column_text(s,1),sqlite3_column_text(s,2));check(n>0&&(size_t)n<cap-used,"query buffer");used+=(size_t)n;}
    check(rc==SQLITE_DONE,"page complete");int steps=sqlite3_stmt_status(s,SQLITE_STMTSTATUS_VM_STEP,0);sqlite3_finalize(s);return steps;
}
int main(int argc,char **argv) {
    check(argc==2,"native CMS binary argument");binary=argv[1];
    snprintf(dir,sizeof(dir),"/tmp/cms-read-indexes-XXXXXX");check(mkdtemp(dir)!=NULL,"fixture directory");
    snprintf(path,sizeof(path),"%s/cms.db",dir);snprintf(schema_path,sizeof(schema_path),"%s/schema.json",dir);
    check(apply("")==0,"legacy schema accepted");sqlite3 *db=NULL;check(sqlite3_open(path,&db)==SQLITE_OK,"fixture database");
    sql(db,"WITH RECURSIVE n(i) AS(VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<65000) INSERT INTO occurrence(namespace,created_at,updated_at,published_at,status,source_ref,title) SELECT CASE WHEN i%601<300 THEN 'one' ELSE 'two' END,'2026-10-27','2026-10-27',printf('%08d',i),'published','source-'||(i%600),'Item '||i FROM n");
    char before[4096]={0},after[4096]={0};int slow=query(db,before,sizeof(before));
    const char *indexes="\"readIndexes\":{\"public_source\":[\"source_ref\",\"status\",\"published_at\",\"id\"]},";
    check(apply(indexes)==0,"declared index applied");int fast=query(db,after,sizeof(after));
    check(!strcmp(before,after),"ordered rows and namespaces preserved");check(fast*20<slow,"at least twenty-fold fewer SQLite VM steps");
    check(number(db,"SELECT count(*) FROM occurrence")==65000,"no rows lost");
    check(number(db,"SELECT [unique] FROM pragma_index_list('occurrence') WHERE name='idx_occurrence_read_public_source'")==0,"read index is not identity constraint");
    int version=number(db,"PRAGMA schema_version");check(apply(indexes)==0,"replay succeeds");check(number(db,"PRAGMA schema_version")==version,"replay does not rebuild index");
    const char *bad[]={"\"readIndexes\":[],","\"readIndexes\":{\"bad-name\":[\"id\"]},","\"readIndexes\":{\"bad\":[]},","\"readIndexes\":{\"bad\":[\"missing\"]},","\"readIndexes\":{\"bad\":[\"id\",\"ID\"]},","\"readIndexes\":{\"bad\":[7]},","\"readIndexes\":{\"bad\":[\"id DESC\"]},","\"readIndexes\":{\"dup\":[\"id\"],\"DUP\":[\"title\"]},","\"readIndexes\":{\"public_source\":[\"title\"]},","\"readIndexes\":{\"nine\":[\"id\",\"namespace\",\"created_at\",\"updated_at\",\"published_at\",\"status\",\"source_ref\",\"title\",\"other\"]},"};
    for(size_t i=0;i<sizeof(bad)/sizeof(bad[0]);i++){check(apply(bad[i])!=0,"malformed or conflicting declaration refused");check(number(db,"PRAGMA schema_version")==version,"failed schema rolls back DDL");check(number(db,"SELECT count(*) FROM occurrence")==65000,"refusal retains rows");}
    sql(db,"CREATE TRIGGER fail_schema BEFORE UPDATE ON _content_types BEGIN SELECT RAISE(ABORT,'fixture metadata failure');END");
    check(apply("\"readIndexes\":{\"rollback\":[\"title\"]},")!=0,"metadata failure refused");
    check(number(db,"SELECT count(*) FROM sqlite_master WHERE name='idx_occurrence_read_rollback'")==0,"index rolled back with metadata");
    sql(db,"DROP TRIGGER fail_schema");check(apply("")==0,"omitted indexes compatible");
    check(number(db,"SELECT count(*) FROM sqlite_master WHERE name='idx_occurrence_read_public_source'")==1,"omission never drops an index");
    sqlite3_close(db);unlink(schema_path);unlink(path);char side[650];snprintf(side,sizeof(side),"%s-wal",path);unlink(side);snprintf(side,sizeof(side),"%s-shm",path);unlink(side);rmdir(dir);
    printf("CMS read indexes: %d assertions; VM steps %d -> %d: PASS\n",checks,slow,fast);return 0;
}
