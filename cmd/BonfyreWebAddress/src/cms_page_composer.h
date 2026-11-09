#ifndef BONFYRE_CMS_PAGE_COMPOSER_H
#define BONFYRE_CMS_PAGE_COMPOSER_H
#include "cms_page_composer_markup.h"
#include "cms_advanced_composer_markup.h"
#include "../../BonfyreSurfaceScene/native/authoring_carrier.h"
/* A browser carrier for the existing namespace-scoped CMS page aggregate. */
static void cms_composer_reply(int code,const char *type,const char *body) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nX-Frame-Options: SAMEORIGIN\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
        code,code==200?"OK":code==403?"Forbidden":code==409?"Conflict":code==503?"Service Unavailable":"Bad Request",type,strlen(body),body);
}
/* Account-bound native C preview: an exact typed draft source projection.
 * No publication, no external assets or provider calls, and no temporary file
 * copies. The same renderer creates the offline preview and this HTTP view.
 */
static int cms_preview_hex(int value){
 if(value>='0'&&value<='9')return value-'0';
 if(value>='a'&&value<='f')return value-'a'+10;
 if(value>='A'&&value<='F')return value-'A'+10;
 return -1;
}
static void cms_composer_native_preview(const char *body,const char *ns){
 size_t n=body?strlen(body):0;
 if(n<8||strncmp(body,"BFAU2:",6)||(n-6)%2||n>220000){
  cms_composer_reply(400,"application/json","{\"error\":\"Provide a complete bounded native authoring carrier.\"}");return;
 }
 size_t count=(n-6)/2;
 unsigned char *bytes=(unsigned char*)malloc(count?count:1);
 if(!bytes){cms_composer_reply(503,"application/json","{\"error\":\"Preview could not be prepared.\"}");return;}
 int valid=1;
 for(size_t i=0;i<count;i++){
  int hi=cms_preview_hex((unsigned char)body[6+2*i]);
  int lo=cms_preview_hex((unsigned char)body[7+2*i]);
  if(hi<0||lo<0){valid=0;break;}
  bytes[i]=(unsigned char)((hi<<4)|lo);
 }
 BfWireScene scene={0};
 if(valid){
  FILE *input=fmemopen(bytes,count,"rb");
  valid=input&&bf_wire_read(input,&scene);
  if(input)fclose(input);
 }
 free(bytes);
 if(!valid){
  bf_wire_clear(&scene);
  cms_composer_reply(400,"application/json","{\"error\":\"Native authoring carrier rejected by C validation.\"}");return;
 }
 if(strcmp(scene.site,ns)){
  bf_wire_clear(&scene);
  cms_composer_reply(403,"application/json","{\"error\":\"This draft belongs to another account.\"}");return;
 }
 char *html=NULL;size_t used=0;
 FILE *out=open_memstream(&html,&used);
 valid=out&&bf_wire_render(out,&scene);
 if(out&&fclose(out))valid=0;
 bf_wire_clear(&scene);
 if(!valid||!html||used>1024*1024){
  free(html);
  cms_composer_reply(400,"application/json","{\"error\":\"Unsupported editorial elements or oversized native preview.\"}");return;
 }
 cms_composer_reply(200,"text/html; charset=utf-8",html);
 free(html);
}
static int serve_cms_composer(const char *method,const char *target,const char *query,const char *body,
                             const char *bearer,const char *auth_db,const char *origin,const char *host) {
    int advanced=!strcmp(target,"/cms/compose/advanced");
    int home=!strcmp(target,"/cms/compose"),page=!strcmp(target,"/cms/compose/page"),listing=!strcmp(target,"/cms/compose/pages");
    int preview=!strcmp(target,"/cms/compose/preview");
    if(!home&&!page&&!listing&&!advanced&&!preview)return 0;
    int writing=page&&!strcmp(method,"POST");
    int posting=(page||preview)&&!strcmp(method,"POST");
    if((!posting&&strcmp(method,"GET"))||(preview&&!posting)||
       (posting&&(!body||!*body||strlen(body)>220000))) {
        cms_composer_reply(400,"application/json","{\"error\":\"Use a complete page request within the 220 KB save limit.\"}");return 1;
    }
    char ns[192]={0};
    if(!session_valid(bearer,auth_db)||!session_authorized(bearer,auth_db,"write")||
       !cms_namespace_for(bearer,auth_db,ns,sizeof(ns))||strlen(ns)>127) {
        cms_composer_reply(403,"application/json","{\"error\":\"This CMS workspace requires a current write grant.\"}");return 1;
    }
    if(posting) {
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
    if(home){sqlite3_close(db);cms_composer_reply(200,"text/html; charset=utf-8",cms_page_composer_markup);return 1;}
    if(advanced){sqlite3_close(db);cms_composer_reply(200,"text/html; charset=utf-8",cms_advanced_composer_markup);return 1;}
    if(preview){sqlite3_close(db);cms_composer_native_preview(body,ns);return 1;}
    if(listing){
        sqlite3_stmt *st=NULL;const char *result=NULL;
        if(sqlite3_prepare_v2(db,"SELECT json_object('pages',json_group_array(json_object('route',route,'title',title)),'has_more',(SELECT count(*)>512 FROM site_page WHERE namespace=?1)) FROM (SELECT route,title FROM site_page WHERE namespace=?1 ORDER BY route LIMIT 512)",-1,&st,NULL)==SQLITE_OK){
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
