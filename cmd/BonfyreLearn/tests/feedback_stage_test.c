/* Exercise the real CLI and SQLite owner in a disposable store. No model,
 * production feedback, threshold or account state is read or changed. */
#define _POSIX_C_SOURCE 200809L
#include <unistd.h>
#include <math.h>
#define main learn_entry
#include "../src/main.c"
#undef main

static int assertions;
static void check(int ok,const char *message){
    assertions++;
    if(!ok){fprintf(stderr,"learn stage: FAIL: %s\n",message);exit(1);}
}
static char *invoke(int argc,char **argv,int expected){
    FILE *out=tmpfile();check(out!=NULL,"capture output");
    fflush(stdout);int saved=dup(1);check(saved>=0,"retain stdout");
    check(dup2(fileno(out),1)>=0,"capture stdout");
    int rc=learn_entry(argc,argv);fflush(stdout);
    check(dup2(saved,1)>=0,"restore stdout");close(saved);
    check(rc==expected,"CLI returned expected status");
    check(fseek(out,0,SEEK_END)==0,"seek output");long n=ftell(out);
    check(n>=0,"measure output");rewind(out);
    char *body=calloc((size_t)n+1,1);check(body!=NULL,"allocate output");
    check(fread(body,1,(size_t)n,out)==(size_t)n,"read output");fclose(out);return body;
}
static double scalar(sqlite3 *db,const char *sql){
    sqlite3_stmt *s=NULL;check(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK,"prepare query");
    check(sqlite3_step(s)==SQLITE_ROW,"read query");double v=sqlite3_column_double(s,0);sqlite3_finalize(s);return v;
}
int main(void){
    char path[]="/tmp/bonfyre-learn-stage-XXXXXX";int fd=mkstemp(path);
    check(fd>=0,"create isolated store");close(fd);
    char *good[]={"learn","--db",path,"feedback","qa-looking-opaque-run","0.9","good","--stage","transcribe"};
    char *body=invoke(9,good,0);check(strstr(body,"stage=transcribe")!=NULL,"explicit stage reported");free(body);
    sqlite3 *db=NULL;check(sqlite3_open(path,&db)==SQLITE_OK,"inspect isolated store");
    check(scalar(db,"SELECT count(*) FROM feedback WHERE run_id='qa-looking-opaque-run' AND stage='transcribe' AND label='good'")==1,"run identity retained, no stage guessed from run");
    check(sqlite3_exec(db,"INSERT INTO feedback(run_id,stage,score,ts) VALUES('legacy',NULL,0,strftime('%s','now'))",NULL,NULL,NULL)==SQLITE_OK,"retain legacy unassigned feedback");
    char *status[]={"learn","--db",path,"status"};body=invoke(4,status,0);
    char *row=strstr(body,"transcribe");check(row!=NULL,"stage in status");
    char *end=strchr(row,'\n');if(end)*end=0;check(strstr(row,"0.900")!=NULL,"status includes stage feedback average");free(body);
    char *tune[]={"learn","--db",path,"tune","transcribe"};body=invoke(5,tune,0);
    check(strstr(body,"samples=1 avg=0.900")!=NULL,"tuning consumes assigned feedback only");free(body);
    check(fabs(scalar(db,"SELECT quality FROM thresholds WHERE stage='transcribe'")-0.77)<0.00001,"target threshold changes");
    check(fabs(scalar(db,"SELECT quality FROM thresholds WHERE stage='qa'")-0.75)<0.00001,"other stage unchanged");
    check(scalar(db,"SELECT count(*) FROM feedback WHERE run_id='legacy' AND stage IS NULL")==1,"legacy row is not guessed or rewritten");
    char *history[]={"learn","--db",path,"history","transcribe"};body=invoke(5,history,0);
    check(strstr(body,"qa-looking-opaque-run")!=NULL&&!strstr(body,"legacy"),"history uses exact stage");free(body);
    char *missing[]={"learn","--db",path,"feedback","transcribe-looks-assigned","0.8"};body=invoke(6,missing,2);free(body);
    char *bad[]={"learn","--db",path,"feedback","opaque","bad","--stage","transcribe"};
    const char *scores[]={"bad","","nan","inf","-0.1","1.1","0.2x"};
    for(size_t i=0;i<sizeof(scores)/sizeof(scores[0]);i++){bad[5]=(char *)scores[i];body=invoke(8,bad,2);free(body);}
    bad[5]="0.8";bad[7]="unregistered";body=invoke(8,bad,1);check(!strstr(body,"feedback:"),"unknown stage prints no success");free(body);
    bad[7]="";body=invoke(8,bad,2);free(body);
    char *duplicate[]={"learn","--db",path,"feedback","opaque","0.8","--stage","qa","--stage","transcribe"};body=invoke(10,duplicate,2);free(body);
    check(scalar(db,"SELECT count(*) FROM feedback")==2,"invalid inputs stored no feedback");
    check(sqlite3_exec(db,"CREATE TRIGGER refuse_feedback BEFORE INSERT ON feedback WHEN NEW.run_id='write-fail' BEGIN SELECT RAISE(ABORT,'fixture refusal'); END",NULL,NULL,NULL)==SQLITE_OK,"create write refusal");
    bad[4]="write-fail";bad[7]="transcribe";body=invoke(8,bad,1);check(!strstr(body,"feedback:"),"failed write prints no success");free(body);
    check(scalar(db,"SELECT count(*) FROM feedback")==2,"refused write changes no feedback");
    char *second[]={"learn","--db",path,"feedback","second-run","0.2","--stage","tag"};body=invoke(8,second,0);free(body);
    check(scalar(db,"SELECT count(*) FROM feedback WHERE stage='tag' AND label IS NULL")==1,"label optional and second stage isolated");
    sqlite3_close(db);unlink(path);char extra[256];snprintf(extra,sizeof(extra),"%s-wal",path);unlink(extra);snprintf(extra,sizeof(extra),"%s-shm",path);unlink(extra);
    printf("learn feedback-stage lifecycle: %d assertions: PASS\n",assertions);return 0;
}
