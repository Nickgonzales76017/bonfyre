#ifndef BONFYRE_CMS_PAGE_COMPOSER_H
#define BONFYRE_CMS_PAGE_COMPOSER_H
#include "cms_page_composer_markup.h"
#include "cms_advanced_composer_markup.h"
#include "../../BonfyreBroadcast/src/narration_intake.h"
/* A browser carrier for the existing namespace-scoped CMS page aggregate. */
static void cms_composer_reply(int code,const char *type,const char *body) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nX-Frame-Options: SAMEORIGIN\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
        code,code==200?"OK":code==403?"Forbidden":code==409?"Conflict":code==503?"Service Unavailable":"Bad Request",type,strlen(body),body);
}
static int serve_cms_composer(const char *method,const char *target,const char *query,const char *body,
                             const char *bearer,const char *auth_db,const char *origin,const char *host) {
    int advanced=!strcmp(target,"/cms/compose/advanced"),revision=!strcmp(target,"/cms/compose/narration-revision");
    int home=!strcmp(target,"/cms/compose"),page=!strcmp(target,"/cms/compose/page"),listing=!strcmp(target,"/cms/compose/pages");
    if(!home&&!page&&!listing&&!advanced&&!revision)return 0;
    int writing=page&&!strcmp(method,"POST");
    if((!writing&&strcmp(method,"GET"))||(writing&&(!body||!*body||strlen(body)>220000))) {
        cms_composer_reply(400,"application/json","{\"error\":\"Use a complete page request within the 220 KB save limit.\"}");return 1;
    }
    char ns[192]={0},site[128]={0},subject[160]={0};
    int site_arg=bf_lex_form_query(query?query:"","site",site,sizeof(site));
    if(site_arg<0||(site_arg==1&&!*site)){cms_composer_reply(400,"application/json","{\"error\":\"Invalid profile site.\"}");return 1;}
    if(site_arg==0&&(!session_valid(bearer,auth_db)||!session_authorized(bearer,auth_db,"write")||
       !cms_namespace_for(bearer,auth_db,ns,sizeof(ns))||strlen(ns)>127)) {
        cms_composer_reply(403,"application/json","{\"error\":\"This CMS workspace requires a current write grant.\"}");return 1;
    }
    if(writing) {
        char http[300],https[300];snprintf(http,sizeof(http),"http://%s",host?host:"");snprintf(https,sizeof(https),"https://%s",host?host:"");
        if(!origin||(!*origin)||(strcmp(origin,http)&&strcmp(origin,https))) {
            cms_composer_reply(403,"application/json","{\"error\":\"Save from this workspace origin.\"}");return 1;
        }
    }
    const char *bin=getenv("BONFYRE_CMS_BINARY"),*dbpath=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");
    sqlite3 *db=cms_open_ro();
    if(!db||!bin||!*bin||!dbpath||!*dbpath||!schemas||!*schemas||
       !cms_type_registered(db,"site_page")||!cms_type_registered(db,"site_fragment")) {
        if(db)sqlite3_close(db);
        cms_composer_reply(503,"application/json","{\"error\":\"The page composition owner is not configured.\"}");return 1;
    }
    if(site_arg<0||(site_arg==1&&(!*site||!world_web_admit(bearer,auth_db,db,site,writing,subject)))){
        sqlite3_close(db);cms_composer_reply(403,"application/json","{\"error\":\"Profile site unavailable.\"}");return 1;
    }
    if(site_arg==1)snprintf(ns,sizeof(ns),"%s",site);
    if(home){sqlite3_close(db);cms_composer_reply(200,"text/html; charset=utf-8",cms_page_composer_markup);return 1;}
    if(advanced){sqlite3_close(db);cms_composer_reply(200,"text/html; charset=utf-8",cms_advanced_composer_markup);return 1;}
    if(listing){
        sqlite3_stmt *st=NULL;const char *result=NULL;
        if(sqlite3_prepare_v2(db,"SELECT json_object('pages',json_group_array(json_object('route',route,'title',title,'status',status)),'has_more',(SELECT count(*)>512 FROM site_page WHERE namespace=?1)) FROM (SELECT route,title,status FROM site_page WHERE namespace=?1 ORDER BY route LIMIT 512)",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_bytes(st,0)<1024*1024)result=(const char*)sqlite3_column_text(st,0);
        }
        if(result)cms_composer_reply(200,"application/json",result);
        else cms_composer_reply(503,"application/json","{\"error\":\"The page list is unavailable or exceeds its bound.\"}");
        sqlite3_finalize(st);sqlite3_close(db);return 1;
    }
    char route[128]={0},path[]="/tmp/bonfyre-page-compose-XXXXXX",arg[sizeof(path)+1];
    if(!writing&&(bf_lex_form_query(query?query:"","route",route,sizeof(route))!=1||route[0]!='/')){
        sqlite3_close(db);cms_composer_reply(400,"application/json","{\"error\":\"Choose a local page address.\"}");return 1;
    }
    if(revision){
        sqlite3_stmt *st=NULL;char ref[192]={0},state[16]={0},digest[65]={0};int ok=0;
        if(sqlite3_exec(db,"SAVEPOINT composer_narration_revision",NULL,NULL,NULL)==SQLITE_OK){
            const char *sql="SELECT media_subject_ref,status FROM site_page WHERE namespace=?1 AND route=?2 LIMIT 2";
            if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){
                sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
                if(sqlite3_step(st)==SQLITE_ROW){
                    const char *r=(const char*)sqlite3_column_text(st,0),*v=(const char*)sqlite3_column_text(st,1);
                    if(r&&v&&strlen(r)<sizeof(ref)&&strlen(v)<sizeof(state)){
                        snprintf(ref,sizeof(ref),"%s",r);snprintf(state,sizeof(state),"%s",v);
                        if(sqlite3_step(st)==SQLITE_DONE)ok=1;
                    }
                }
            }
            sqlite3_finalize(st);st=NULL;
            if(ok)ok=bf_narration_article_revision(db,ns,ref,state,digest);
            if(sqlite3_exec(db,"RELEASE composer_narration_revision",NULL,NULL,NULL)!=SQLITE_OK)ok=0;
        }
        if(site_arg==1){char checked[160];if(!world_web_admit(bearer,auth_db,db,site,0,checked)||strcmp(subject,checked))ok=0;}
        if(ok){
            bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
            bf_jsonw_key(&w,"schema");bf_jsonw_str(&w,"bonfyre.article-revision.v2");
            bf_jsonw_key(&w,"namespace");bf_jsonw_str(&w,ns);
            bf_jsonw_key(&w,"route");bf_jsonw_str(&w,route);
            bf_jsonw_key(&w,"subject");bf_jsonw_str(&w,ref);
            bf_jsonw_key(&w,"status");bf_jsonw_str(&w,state);
            bf_jsonw_key(&w,"sha256");bf_jsonw_str(&w,digest);bf_jsonw_obj_end(&w);
            if(!w.oom&&w.buf)cms_composer_reply(200,"application/json",w.buf);
            else cms_composer_reply(503,"application/json","{\"error\":\"The article revision is unavailable.\"}");
            bf_jsonw_free(&w);
        }else cms_composer_reply(409,"application/json","{\"error\":\"The article revision is unavailable or changed.\"}");
        sqlite3_close(db);return 1;
    }
    int fd=-1;
    if(writing){
        fd=mkstemp(path);
        if(fd<0){sqlite3_close(db);cms_composer_reply(503,"application/json","{\"error\":\"The save could not be prepared.\"}");return 1;}
        size_t used=0,length=strlen(body);
        while(used<length){ssize_t n=write(fd,body+used,length-used);if(n<0&&errno==EINTR)continue;if(n<=0)break;used+=(size_t)n;}
        int closed=close(fd);fd=-1;
        if(used!=length||closed){unlink(path);sqlite3_close(db);cms_composer_reply(503,"application/json","{\"error\":\"The complete save could not be prepared.\"}");return 1;}
        snprintf(arg,sizeof(arg),"@%s",path);
    }
    size_t cap=1024*1024+2048;char *output=calloc(1,cap),diagnostic[4096]={0};
    if(!output){if(writing)unlink(path);sqlite3_close(db);cms_composer_reply(503,"application/json","{\"error\":\"The page could not be opened.\"}");return 1;}
    char *argv[]={(char*)bin,"entry",writing?"page-write":"page-read",writing?arg:route,"--ns",ns,"--db",(char*)dbpath,"--schemas",(char*)schemas,NULL};
    if(site_arg==1&&writing){
        sqlite3_stmt *draft=NULL;int allowed=0;
        const char *sql="SELECT json_valid(?1) AND json_extract(?1,'$.operation') IN ('create','replace') AND json_extract(?1,'$.page.status')='draft' AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.fragments') WHERE json_extract(value,'$.status') IS NOT 'draft') AND NOT EXISTS(SELECT 1 FROM site_page WHERE namespace=?2 AND route=json_extract(?1,'$.route') AND status='published') AND NOT EXISTS(SELECT 1 FROM site_fragment WHERE namespace=?2 AND page=json_extract(?1,'$.route') AND status='published')";
        if(sqlite3_prepare_v2(db,sql,-1,&draft,NULL)==SQLITE_OK){sqlite3_bind_text(draft,1,body,-1,SQLITE_STATIC);sqlite3_bind_text(draft,2,site,-1,SQLITE_STATIC);allowed=sqlite3_step(draft)==SQLITE_ROW&&sqlite3_column_int(draft,0);}
        sqlite3_finalize(draft);if(!allowed){unlink(path);free(output);sqlite3_close(db);cms_composer_reply(409,"application/json","{\"error\":\"This editor saves unpublished pages only.\"}");return 1;}
    }
    if(site_arg==1){char checked[160];
        if(!world_web_admit(bearer,auth_db,db,site,writing,checked)||strcmp(subject,checked)){
            if(writing)unlink(path);free(output);sqlite3_close(db);cms_composer_reply(403,"application/json","{\"error\":\"Profile site unavailable.\"}");return 1;
        }
    }
    int rc=auth_call_split(argv,NULL,output,cap,diagnostic,sizeof(diagnostic));
    if(writing)unlink(path);
    sqlite3_stmt *check=NULL;int valid=0,committed=0;
    if(strlen(output)<cap-1&&sqlite3_prepare_v2(db,"SELECT json_valid(?1)",-1,&check,NULL)==SQLITE_OK){sqlite3_bind_text(check,1,output,-1,SQLITE_STATIC);valid=sqlite3_step(check)==SQLITE_ROW&&sqlite3_column_int(check,0);}
    sqlite3_finalize(check);check=NULL;
    if(valid&&writing&&sqlite3_prepare_v2(db,"SELECT json_extract(?1,'$.committed')=1",-1,&check,NULL)==SQLITE_OK){sqlite3_bind_text(check,1,output,-1,SQLITE_STATIC);committed=sqlite3_step(check)==SQLITE_ROW&&sqlite3_column_int(check,0);}
    sqlite3_finalize(check);sqlite3_close(db);
    if(valid&&((!writing&&rc==0)||(writing&&committed)))cms_composer_reply(200,"application/json",output);
    else cms_composer_reply(writing?409:503,"application/json",writing?"{\"error\":\"The page changed or its fields were refused. Keep the draft and reopen the saved version.\"}":"{\"error\":\"The complete page snapshot is unavailable.\"}");
    free(output);return 1;
}
#endif
