#define _POSIX_C_SOURCE 200809L
#include <sqlite3.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include "source_constellations.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *arg(int argc,char **argv,const char *name){for(int i=2;i+1<argc;i++)if(!strcmp(argv[i],name))return argv[i+1];return NULL;}

static void host_of(const char *url,char *out,size_t cap){
    const char *p=strstr(url?url:"","://"),*e; size_t n;
    p=p?p+3:(url?url:""); e=p;
    while(*e&&*e!='/'&&*e!='?'&&*e!='#'&&*e!=':')e++;
    if((size_t)(e-p)>=4&&!strncasecmp(p,"www.",4))p+=4;
    n=(size_t)(e-p); if(n>=cap)n=cap-1;
    for(size_t i=0;i<n;i++)out[i]=(char)tolower((unsigned char)p[i]);
    out[n]='\0';
}

static int schema(sqlite3 *db){
    const char *q=
      "CREATE TABLE estate(id TEXT PRIMARY KEY,name TEXT,url TEXT,host TEXT,kind TEXT);"
      "CREATE INDEX estate_url ON estate(url);CREATE INDEX estate_host ON estate(host);CREATE INDEX estate_name ON estate(name);"
      "CREATE TABLE showcase(id TEXT PRIMARY KEY,label TEXT,authority TEXT,url TEXT,host TEXT,carrier TEXT,auth TEXT,protocols TEXT,worlds INTEGER);";
    return sqlite3_exec(db,q,NULL,NULL,NULL)==SQLITE_OK;
}

static int load_estate(sqlite3 *db,const char *path,int *rows){
    FILE *f=fopen(path,"rb"); char *line=NULL; size_t cap=0; ssize_t got;
    sqlite3_stmt *parse=NULL,*ins=NULL; int ok=0,n=0;
    if(!f)return 0;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(?1,'$.id'),json_extract(?1,'$.observed_name'),json_extract(?1,'$.primary_url'),json_extract(?1,'$.source_kind') WHERE json_valid(?1)",-1,&parse,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(db,"INSERT OR REPLACE INTO estate(id,name,url,host,kind) VALUES(?1,?2,?3,?4,?5)",-1,&ins,NULL)!=SQLITE_OK)goto done;
    while((got=getline(&line,&cap,f))>=0){
        char host[512]={0}; const char *id,*name,*url,*kind;
        sqlite3_bind_text(parse,1,line,(int)got,SQLITE_TRANSIENT);
        if(sqlite3_step(parse)!=SQLITE_ROW||!sqlite3_column_text(parse,0)||!sqlite3_column_text(parse,2)){sqlite3_reset(parse);sqlite3_clear_bindings(parse);continue;}
        id=(const char*)sqlite3_column_text(parse,0); name=(const char*)sqlite3_column_text(parse,1); url=(const char*)sqlite3_column_text(parse,2); kind=(const char*)sqlite3_column_text(parse,3); host_of(url,host,sizeof(host));
        sqlite3_bind_text(ins,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(ins,2,name?name:"",-1,SQLITE_TRANSIENT);sqlite3_bind_text(ins,3,url,-1,SQLITE_TRANSIENT);sqlite3_bind_text(ins,4,host,-1,SQLITE_TRANSIENT);sqlite3_bind_text(ins,5,kind?kind:"",-1,SQLITE_TRANSIENT);
        if(sqlite3_step(ins)!=SQLITE_DONE)goto done;
        n++;sqlite3_reset(ins);sqlite3_clear_bindings(ins);sqlite3_reset(parse);sqlite3_clear_bindings(parse);
    }
    *rows=n;ok=1;
done:
    free(line);if(f)fclose(f);sqlite3_finalize(parse);sqlite3_finalize(ins);return ok;
}

static int load_showcase(sqlite3 *db){
    sqlite3_stmt *q=NULL,*ins=NULL; int ok=0;
    const char *sql="SELECT json_extract(s.value,'$.id'),max(json_extract(s.value,'$.label')),max(json_extract(s.value,'$.authority')),max(json_extract(s.value,'$.url')),max(json_extract(s.value,'$.carrier')),max(json_extract(s.value,'$.auth')),max(json_extract(s.value,'$.protocols')),count(DISTINCT json_extract(w.value,'$.id')) FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s GROUP BY 1 ORDER BY 1";
    if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)return 0;
    if(sqlite3_prepare_v2(db,"INSERT INTO showcase(id,label,authority,url,host,carrier,auth,protocols,worlds) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",-1,&ins,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(q,1,bf_source_constellations_json,-1,SQLITE_STATIC);
    while(sqlite3_step(q)==SQLITE_ROW){
        char host[512]={0}; const char *url=(const char*)sqlite3_column_text(q,3);
        host_of(url,host,sizeof(host));
        for(int i=0;i<4;i++)sqlite3_bind_text(ins,i+1,(const char*)sqlite3_column_text(q,i),-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(ins,5,host,-1,SQLITE_TRANSIENT);
        for(int i=4;i<7;i++)sqlite3_bind_text(ins,i+2,(const char*)sqlite3_column_text(q,i),-1,SQLITE_TRANSIENT);
        sqlite3_bind_int(ins,9,sqlite3_column_int(q,7));
        if(sqlite3_step(ins)!=SQLITE_DONE)goto done;
        sqlite3_reset(ins);sqlite3_clear_bindings(ins);
    }
    ok=1;
done:
    sqlite3_finalize(q);sqlite3_finalize(ins);return ok;
}

static const char *crosswalk_sql(void){
    return
      "SELECT json_object('schema','bonfyre-showcase-crosswalk.v1','source',s.id,'label',s.label,'authority',s.authority,'url',s.url,'host',s.host,'showcase_worlds',s.worlds,'source_relation',CASE WHEN EXISTS(SELECT 1 FROM estate e WHERE e.url=s.url) THEN 'exact_source_reuse' WHEN EXISTS(SELECT 1 FROM estate e WHERE e.host=s.host) THEN 'same_host_family_new_endpoint' ELSE 'new_source_candidate' END,'exact_source_matches',json(COALESCE((SELECT json_group_array(json_object('id',e.id,'name',e.name,'url',e.url)) FROM estate e WHERE e.url=s.url),'[]')),'host_family_matches',json(COALESCE((SELECT json_group_array(json_object('id',e.id,'name',e.name,'url',e.url)) FROM estate e WHERE e.host=s.host AND e.url<>s.url),'[]')),'semantic_reuse',json(COALESCE((SELECT json_group_array(json_object('declared',p.value,'estate_id',e.id,'estate_name',e.name,'kind',e.kind)) FROM json_each(s.protocols) p JOIN estate e ON lower(e.name)=lower(p.value)),'[]')),'next_owner',CASE WHEN EXISTS(SELECT 1 FROM estate e WHERE e.url=s.url) THEN 'ExternalEstate' ELSE 'ForeignTwin' END,'truth','crosswalk_only_not_observation_or_admission') FROM showcase s ORDER BY s.id";
}

static int emit(sqlite3 *db,const char *path){
    FILE *f=fopen(path,"wb");sqlite3_stmt *q=NULL;int ok=0,n=0;
    if(!f)return 0;
    if(sqlite3_prepare_v2(db,crosswalk_sql(),-1,&q,NULL)!=SQLITE_OK)goto done;
    while(sqlite3_step(q)==SQLITE_ROW){const char *j=(const char*)sqlite3_column_text(q,0);if(!j||fprintf(f,"%s\n",j)<0)goto done;n++;}
    if(fflush(f)||fclose(f)){f=NULL;goto done;}f=NULL;
    printf("{\"rows\":%d,\"output\":\"%s\",\"truth\":\"crosswalk_only_not_observation_or_admission\"}\n",n,path);ok=1;
done:
    if(f)fclose(f);sqlite3_finalize(q);return ok;
}

static int audit(sqlite3 *db,int estate_rows){
    sqlite3_stmt *q=NULL;int unique=0,exact=0,host=0,newn=0,semantic_sources=0,semantic_bindings=0,shared=0,krdart_collision=0;
    const char *sql="SELECT count(*),sum(EXISTS(SELECT 1 FROM estate e WHERE e.url=s.url)),sum(NOT EXISTS(SELECT 1 FROM estate e WHERE e.url=s.url) AND EXISTS(SELECT 1 FROM estate e WHERE e.host=s.host)),sum(NOT EXISTS(SELECT 1 FROM estate e WHERE e.url=s.url) AND NOT EXISTS(SELECT 1 FROM estate e WHERE e.host=s.host)),sum(EXISTS(SELECT 1 FROM json_each(s.protocols) p JOIN estate e ON lower(e.name)=lower(p.value))),sum((SELECT count(*) FROM json_each(s.protocols) p JOIN estate e ON lower(e.name)=lower(p.value))),sum(s.worlds>1),sum(EXISTS(SELECT 1 FROM estate e WHERE e.name='KR-DART Events' AND (e.url=s.url OR e.host=s.host))) FROM showcase s";
    if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)return 0;
    if(sqlite3_step(q)!=SQLITE_ROW){sqlite3_finalize(q);return 0;}
    unique=sqlite3_column_int(q,0);exact=sqlite3_column_int(q,1);host=sqlite3_column_int(q,2);newn=sqlite3_column_int(q,3);semantic_sources=sqlite3_column_int(q,4);semantic_bindings=sqlite3_column_int(q,5);shared=sqlite3_column_int(q,6);krdart_collision=sqlite3_column_int(q,7);sqlite3_finalize(q);
    printf("{\"schema\":\"bonfyre-showcase-crosswalk-audit.v1\",\"estate_rows\":%d,\"showcase_sources\":%d,\"exact_source_reuse\":%d,\"same_host_family\":%d,\"new_source_candidates\":%d,\"semantic_reuse_sources\":%d,\"semantic_reuse_bindings\":%d,\"shared_showcase_sources\":%d,\"kr_dart_false_collisions\":%d,\"truth\":\"identity_and_semantic_crosswalk_only\"}\n",estate_rows,unique,exact,host,newn,semantic_sources,semantic_bindings,shared,krdart_collision);
    return unique==17&&exact+host+newn==unique&&krdart_collision==0;
}

int main(int argc,char **argv){
    const char *estate=arg(argc,argv,"--estate"),*out=arg(argc,argv,"--out");sqlite3 *db=NULL;int rows=0,ok=0;
    if(argc<2||!estate||sqlite3_open(":memory:",&db)!=SQLITE_OK)return 2;
    if(!schema(db)||!load_estate(db,estate,&rows)||!load_showcase(db))goto done;
    enum{SC_AUDIT,SC_EMIT};
    static const BfCommandEntry crosswalk_commands[]={{"audit",SC_AUDIT,0,NULL},{"emit",SC_EMIT,0,NULL},};
    const BfCommandEntry*command=bf_command_lookup(crosswalk_commands,sizeof(crosswalk_commands)/sizeof(crosswalk_commands[0]),argv[1]);
    if(command&&command->id==SC_AUDIT)ok=audit(db,rows);
    else if(command&&command->id==SC_EMIT&&out)ok=emit(db,out);
    else fprintf(stderr,"usage: bonfyre-showcase-crosswalk audit --estate FILE | emit --estate FILE --out FILE\n");
done:
    if(db)sqlite3_close(db);
    return ok?0:1;
}
