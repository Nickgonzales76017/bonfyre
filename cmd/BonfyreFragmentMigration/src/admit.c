#define _POSIX_C_SOURCE 200809L
#include "admit.h"
#include "util.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int field(sqlite3 *db,const char *json,const char *path,char *out,size_t cap){
    sqlite3_stmt *st=NULL;int ok=0;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(?1,?2)",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,json,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,path,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_type(st,0)==SQLITE_TEXT){snprintf(out,cap,"%s",(const char *)sqlite3_column_text(st,0));ok=1;}
    sqlite3_finalize(st);return ok;
}
static int exists(sqlite3 *db,const char *ns,const char *coordinate){
    sqlite3_stmt *st=NULL;int found=0;
    if(!db||sqlite3_prepare_v2(db,"SELECT 1 FROM site_fragment WHERE namespace=?1 AND coordinate=?2 LIMIT 1",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,coordinate,-1,SQLITE_TRANSIENT);found=sqlite3_step(st)==SQLITE_ROW;sqlite3_finalize(st);return found;
}
static int seen_add(sqlite3 *db,const char *coordinate){
    sqlite3_stmt *st=NULL;int ok=0;
    if(sqlite3_prepare_v2(db,"INSERT INTO seen(coordinate) VALUES(?1)",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,coordinate,-1,SQLITE_TRANSIENT);ok=sqlite3_step(st)==SQLITE_DONE;sqlite3_finalize(st);return ok;
}
static int apply_schema(const char *cms,const char *db,const char *schemas){
    char *schema=bf_fm_path_join(schemas,"site_fragment.json");int rc;
    if(!schema)return -1;
    char *const argv[]={(char *)cms,"schema","apply",schema,"--db",(char *)db,"--schemas",(char *)schemas,NULL};
    rc=bf_fm_run(argv);free(schema);return rc;
}
static int import_missing(const char *cms,const char *file,const char *db,const char *schemas,const char *ns){
    char *const argv[]={(char *)cms,"entry","import-jsonl","site_fragment",(char *)file,"--ns",(char *)ns,"--db",(char *)db,"--schemas",(char *)schemas,NULL};
    return bf_fm_run(argv);
}
int bf_fm_admit(const char *drafts,const char *db_path,const char *schemas,const char *namespace_name,const char *cms_bin,int apply){
    sqlite3 *store=NULL,*scratch=NULL;FILE *in=NULL,*missing=NULL;char *line=NULL;size_t cap=0;ssize_t nread;
    char temp[]="/tmp/bonfyre-fragment-migration-XXXXXX";int fd=-1,requested=0,skipped=0,pending=0,rc=2;
    if(!drafts||!db_path||!schemas||!namespace_name||!cms_bin||!bf_fm_safe_ident(namespace_name)){fprintf(stderr,"fragment-migration: invalid admission arguments\n");return 2;}
    if(apply&&apply_schema(cms_bin,db_path,schemas)!=0){fprintf(stderr,"fragment-migration: CMS schema admission failed\n");return 1;}
    if(sqlite3_open(":memory:",&scratch)!=SQLITE_OK||sqlite3_exec(scratch,"CREATE TABLE seen(coordinate TEXT PRIMARY KEY)",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_open_v2(db_path,&store,SQLITE_OPEN_READWRITE,NULL)==SQLITE_OK)sqlite3_exec(store,"PRAGMA query_only=ON",NULL,NULL,NULL);else{if(store)sqlite3_close(store);store=NULL;}
    if(!(in=fopen(drafts,"r"))||(fd=mkstemp(temp))<0||!(missing=fdopen(fd,"w")))goto done;fd=-1;
    while((nread=getline(&line,&cap,in))>=0){
        char coordinate[512]="",status[64]="";char *body=line,*end;(void)nread;
        while(*body&&(*body==' '||*body=='\t'||*body=='\r'||*body=='\n'))body++;end=body+strlen(body);while(end>body&&(end[-1]==' '||end[-1]=='\t'||end[-1]=='\r'||end[-1]=='\n'))end--;*end='\0';if(!*body)continue;requested++;
        if(!field(scratch,body,"$.coordinate",coordinate,sizeof(coordinate))||!field(scratch,body,"$.status",status,sizeof(status))||!bf_fm_safe_ident(coordinate)||strcmp(status,"draft")){fprintf(stderr,"fragment-migration: row %d is not a valid draft coordinate\n",requested);goto done;}
        if(!seen_add(scratch,coordinate)){fprintf(stderr,"fragment-migration: duplicate coordinate in input: %s\n",coordinate);goto done;}
        if(exists(store,namespace_name,coordinate)){skipped++;continue;}
        fputs(body,missing);fputc('\n',missing);pending++;
    }
    if(ferror(in)||fflush(missing)||fclose(missing)){missing=NULL;goto done;}missing=NULL;
    if(!apply){printf("{\"schema\":\"bonfyre.surface.fragment-admission-preview.v2\",\"apply\":false,\"requested\":%d,\"create\":%d,\"skip_existing\":%d,\"namespace\":\"%s\",\"publication_effect\":false}\n",requested,pending,skipped,namespace_name);rc=0;goto done;}
    if(pending&&import_missing(cms_bin,temp,db_path,schemas,namespace_name)!=0){fprintf(stderr,"fragment-migration: CMS transactional import refused; no partial generation admitted\n");rc=1;goto done;}
    printf("{\"schema\":\"bonfyre.surface.fragment-admission.v2\",\"apply\":true,\"requested\":%d,\"created\":%d,\"skipped_existing\":%d,\"namespace\":\"%s\",\"write_owner\":\"BonfyreCMS\",\"publication_effect\":false,\"forbidden_inference\":\"draft admission is not publication; existing authored coordinates are never overwritten\"}\n",requested,pending,skipped,namespace_name);rc=0;
done:
    if(missing)fclose(missing);else if(fd>=0)close(fd);if(in)fclose(in);free(line);if(store)sqlite3_close(store);if(scratch)sqlite3_close(scratch);unlink(temp);return rc;
}
