#ifndef BONFYRE_APPLICATION_WORKSPACE_H
#define BONFYRE_APPLICATION_WORKSPACE_H
#include <poll.h>
/* Serve the existing specialized release and delegate candidate mutations to
 * CMS. A URL or binding row does not substitute for a current native grant. */
#define AW_PACKET (512u * 1024u)
#include "workspace_package.h"
static int workspace_package_available(const char *release,const char *entry){
    char path[1024];size_t size=0;if(!entry || !*entry || entry[strlen(entry)-1]!='/' || snprintf(path,sizeof(path),"%sindex.html",entry)>=(int)sizeof(path))return 0;
    unsigned char *body=aw_package(release,path,&size);int ok=body && strstr((char *)body,"bonfyre-workspace-store");free(body);return ok;
}
static int workspace_plan_available(const BfPackResolution *plan){
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int ok=0,step=SQLITE_DONE,count=0;
    if(!db)return 0;
    for(size_t n=0;n<plan->count;n++){
        if(sqlite3_prepare_v2(db,"SELECT preferred_sources,fields FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace' AND status='published'",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,plan->nodes[n].pack,-1,SQLITE_STATIC);
        while((step=sqlite3_step(s))==SQLITE_ROW){const char *release=(const char *)sqlite3_column_text(s,0),*entry=(const char *)sqlite3_column_text(s,1);if(++count>16 || !workspace_package_available(release,entry))goto done;}
        sqlite3_finalize(s);s=NULL;if(step!=SQLITE_DONE)goto done;
    }
    ok=1;
 done:sqlite3_finalize(s);sqlite3_close(db);return ok;
}
static void workspace_editor_links(long long installation){
    sqlite3 *db=cms_open_ro();sqlite3_stmt *q=NULL;
    if(db&&sqlite3_prepare_v2(db,"SELECT w.binding_id,w.entry,w.release FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id WHERE w.installation_id=?1 AND a.status='active' ORDER BY w.entry",-1,&q,NULL)==SQLITE_OK){
        sqlite3_bind_int64(q,1,installation);while(sqlite3_step(q)==SQLITE_ROW){const char *id=(const char *)sqlite3_column_text(q,0),*entry=(const char *)sqlite3_column_text(q,1),*release=(const char *)sqlite3_column_text(q,2);
            if(!id||strlen(id)!=43||strncmp(id,"ws-",3)||strspn(id+3,"abcdef0123456789")!=40||!workspace_package_available(release,entry))continue;
            fputs("<p><a data-pack-editor href=\"/apps/",stdout);html(id);fputs("/",stdout);html(entry);fputs("\">Open ",stdout);html(!strcmp(entry,"catalogue/")?"catalogue":!strcmp(entry,"product-room/")?"room editor":entry);fputs("</a></p>",stdout);
        }
    }
    sqlite3_finalize(q);if(db)sqlite3_close(db);
}
static const char *aw_mime(const char *path){
    const char *dot=strrchr(path,'.');if(!dot)return "text/plain; charset=utf-8";
    if(!strcmp(dot,".html"))return "text/html; charset=utf-8";if(!strcmp(dot,".js")||!strcmp(dot,".mjs"))return "text/javascript; charset=utf-8";
    if(!strcmp(dot,".css"))return "text/css; charset=utf-8";if(!strcmp(dot,".wasm"))return "application/wasm";if(!strcmp(dot,".glb"))return "model/gltf-binary";
    if(!strcmp(dot,".png"))return "image/png";if(!strcmp(dot,".jpg")||!strcmp(dot,".jpeg"))return "image/jpeg";if(!strcmp(dot,".svg"))return "image/svg+xml";if(!strcmp(dot,".webp"))return "image/webp";
    if(!strcmp(dot,".woff2"))return "font/woff2";return "application/octet-stream";
}
static void aw_response_with_referrer(int code,const char *mime,const void *body,size_t size,int public_material,const char *referrer){
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nCache-Control: %s\r\nReferrer-Policy: %s\r\nX-Content-Type-Options: nosniff\r\nContent-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; img-src 'self' data: blob: https:; font-src 'self' https://fonts.gstatic.com; connect-src 'self'; worker-src 'self' blob:; media-src 'self' blob:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",code,code==200?"OK":code==409?"Conflict":code==403?"Forbidden":code==404?"Not Found":"Unavailable",mime,public_material==1?"public, max-age=31536000, immutable":public_material==2?"public, no-cache":"private, no-store",referrer,size);
    if(size)fwrite(body,1,size,stdout);
}
static void aw_response(int code,const char *mime,const void *body,size_t size,int public_material){aw_response_with_referrer(code,mime,body,size,public_material,"no-referrer");}
static void aw_error(int code,const char *text){aw_response(code,"text/plain; charset=utf-8",text,strlen(text),0);}
static int aw_query(const char *query,char *operation,char *prefix,char *cursor,char *epoch){
    unsigned seen=0;snprintf(operation,24,"read");prefix[0]=cursor[0]=0;snprintf(epoch,24,"-1");
    if(query && strlen(query)>4096)return 0;
    for(const char *p=query?query:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[24];
        if(!eq || !psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;char *out=NULL;size_t cap=0;unsigned bit=0;
        if(!strcmp(name,"operation")){out=operation;cap=24;bit=1;}else if(!strcmp(name,"prefix")){out=prefix;cap=512;bit=2;}else if(!strcmp(name,"cursor")){out=cursor;cap=512;bit=4;}else if(!strcmp(name,"epoch")){out=epoch;cap=24;bit=8;}else return 0;
        if(seen&bit || !psetup_decode(eq+1,(size_t)(end-eq-1),out,cap))return 0;seen|=bit;p=*end?end+1:end;
    }
    return !strcmp(operation,"read") || !strcmp(operation,"get") || !strcmp(operation,"context") || !strcmp(operation,"write") || !strcmp(operation,"blob-put") || !strcmp(operation,"blob-get");
}
static int aw_header(const char *raw,const char *name,char *out,size_t cap){
    out[0]=0;size_t length=strlen(name);int found=0;
    for(const char *p=raw?strchr(raw,'\n'):NULL;p && *p;p=strchr(p,'\n')){
        p++;if(!strncasecmp(p,name,length)&&p[length]==':'){
            if(found++)return 0;const char *value=p+length+1;while(*value==' '||*value=='\t')value++;const char *end=value+strcspn(value,"\r\n");while(end>value&&(end[-1]==' '||end[-1]=='\t'))end--;
            if((size_t)(end-value)>=cap)return 0;memcpy(out,value,(size_t)(end-value));out[end-value]=0;
        }
    }
    return found==1;
}
static int aw_native_result(char *const args[],const void *body,size_t bytes,unsigned char **out,size_t *size,int failure_body){
    FILE *input=tmpfile(),*output=tmpfile(),*errors=tmpfile();pid_t pid=-1;int status=0,rc=6;*out=NULL;*size=0;
    if(!input||!output||!errors)goto done;
    if(bytes && fwrite(body,1,bytes,input)!=bytes)goto done;rewind(input);fflush(NULL);
    pid=fork();if(pid<0)goto done;
    if(!pid){dup2(fileno(input),STDIN_FILENO);dup2(fileno(output),STDOUT_FILENO);dup2(fileno(errors),STDERR_FILENO);alarm(20);execv(args[0],args);_exit(127);}
    for(int n=0;n<2200;n++){pid_t result=waitpid(pid,&status,WNOHANG);if(result==pid){pid=-1;break;}if(result<0&&errno!=EINTR)goto done;usleep(10000);}
    if(pid>0){kill(pid,SIGKILL);waitpid(pid,&status,0);pid=-1;goto done;}
    if(!WIFEXITED(status))goto done;rc=WEXITSTATUS(status);if(rc&&!failure_body)goto done;
    if(fseek(output,0,SEEK_END))goto failed;long length=ftell(output);if(length<0||length>(long)AW_PACKET)goto failed;rewind(output);
    *out=malloc((size_t)length+1);if(!*out || fread(*out,1,(size_t)length,output)!=(size_t)length)goto failed;*size=(size_t)length;goto done;
 failed:free(*out);*out=NULL;*size=0;rc=6;
 done:if(pid>0){kill(pid,SIGKILL);waitpid(pid,&status,0);}if(input)fclose(input);if(output)fclose(output);if(errors)fclose(errors);return rc;
}
/* Existing workspace callers retain success-only response semantics. Auth's
 * typed refusals may request the bounded failure body without merging stderr. */
static int aw_native(char *const args[],const void *body,size_t bytes,unsigned char **out,size_t *size){return aw_native_result(args,body,bytes,out,size,0);}
static unsigned char *aw_replace(unsigned char *body,size_t *bytes,const char *needle,const char *value){
    size_t count=0,a=strlen(needle),b=strlen(value);for(char *p=(char *)body;(p=strstr(p,needle));p+=a)count++;
    if(!count)return body;size_t total=*bytes+count*b-count*a;if(total>AW_FILE){free(body);return NULL;}
    unsigned char *next=malloc(total+1);if(!next){free(body);return NULL;}char *p=(char *)body,*q=(char *)next,*found;
    while((found=strstr(p,needle))){size_t n=(size_t)(found-p);memcpy(q,p,n);q+=n;memcpy(q,value,b);q+=b;p=found+a;}strcpy(q,p);free(body);*bytes=total;return next;
}
#include "application_continuation.h"
#include "account_inbox_web.h"
#include "narration_workspace.h"
#include "publication_workspace.h"
#include "publication_retained.h"
#include "activity_workspace.h"
#include "connections_workspace.h"
#include "program_workspace.h"
#include "procurement_approval_status.h"
#include "procurement_downstream_status.h"
#include "procurement_enactment.h"
#include "campaign_workspace.h"
#include "relationship_observation.h"
#include "connect_github_app.h"
static int serve_application_workspace(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    if(serve_connections_workspace(method,target,query,body,raw,host,bearer,auth_db))return 1;
    if(serve_program_workspace(method,target,query,body,raw,host,bearer,auth_db))return 1;
    if(serve_activity_workspace(method,target,query,body,raw,host,bearer,auth_db))return 1;
    if(connect_github_app(method,target,query,host,bearer,auth_db))return 1;
    if(ac_work_board_act(method,target,body,raw,host,bearer,auth_db))return 1;
    if(ac_work_outputs(method,target,query,bearer,auth_db))return 1;
    if(ac_work_board(method,target,query,bearer,auth_db))return 1;
    if(ac_relationship(method,target,query,bearer,auth_db))return 1;
    if(ac_feed_use(method,target,query,bearer,auth_db))return 1;
    if(ac_index(method,target,query,bearer,auth_db))return 1;
    if(strncmp(target,"/apps/",6))return 0;
    const char *assets[]={"program-workspace.mjs","program-service-continuations.mjs","program-service-policy.mjs","program-source-work.mjs","program-workspace.css","workspace-store.mjs","workspace-msgpack.mjs","workspace-value.mjs","workspace-continuation.mjs","workspace-members.mjs","interaction-collection.mjs","record-comparison.mjs","workspace-adoption-review.mjs","source-publication.mjs","source-publication.css","source-desk.mjs","source-desk.css","workspace-destinations.mjs","web-workbench.mjs","web-workbench.css","aurekai-activity.mjs","aurekai-activity-source.mjs","aurekai-horde.mjs","aurekai-inbox-source.mjs","aurekai-activity.css","aurekai-model.mjs","aurekai-model.css","aurekai-push.mjs","aurekai-push-sw.mjs","provider-contract.mjs","extension/foreign-sign-in.mjs","extension/foreign-sign-in.css","publication-research.mjs","publication-picker.mjs","reference-picker.mjs","account-choices.mjs","account-choices.css","invitation-choice.mjs"};
    for(size_t n=0;n<sizeof(assets)/sizeof(assets[0]);n++)if(!strcmp(target+6,assets[n])){
        if(strcmp(method,"GET")){aw_error(405,"Use GET for editor code.\n");return 1;}
        const char *root=getenv("BONFYRE_WORKSPACE_CLIENT");size_t bytes=0;unsigned char *file=root?aw_read_file(root,assets[n],256u*1024u,&bytes):NULL;
        /* Editor code is the same bytes for every reader: revalidated, so a deploy is
         * seen at once, and answered 304 by its digest instead of resent each view. */
        if(file){aw_response(200,aw_mime(assets[n]),file,bytes,2);free(file);}else aw_error(503,"The workspace client release is not connected.\n");return 1;
    }
    if(!strncmp(target+6,"material/",9)){
        const char *key=target+15,*slash=strchr(key,'/');char release[65];size_t bytes=0;unsigned char *file=NULL;
        if(strcmp(method,"GET") || !slash || slash-key!=64 || strncmp(slash+1,"assets/",7)){aw_error(404,"Public source asset not found.\n");return 1;}
        memcpy(release,key,64);release[64]=0;file=aw_package(release,slash+1,&bytes);
        if(file){aw_response(200,aw_mime(slash+1),file,bytes,1);free(file);}else aw_error(404,"The retained source asset is unavailable.\n");return 1;
    }
    const char *start=target+6,*slash=strchr(start,'/');char id[64],subject[160],scope[24],pack[96],family[64],release[65],entry[128],context[96],actor[160],checked_subject[160];const char *checked_scope=NULL;
    if(!slash || slash-start!=43 || strncmp(start,"ws-",3)){aw_error(404,"Workspace not found.\n");return 1;}memcpy(id,start,43);id[43]=0;
    if(!aw_hex(id+3,40)){aw_error(404,"Workspace not found.\n");return 1;}
    int found=0,lookup_code=SQLITE_CANTOPEN;
    /* A failed read is not evidence of an absent installation. Reopen once for
     * a transient WAL shared-memory open failure; never change database mode,
     * repair files, or weaken current account checks to make a read succeed. */
    for(int attempt=0;attempt<2;attempt++){
        sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;found=0;
        if(!db){lookup_code=SQLITE_CANTOPEN;break;}
        lookup_code=sqlite3_prepare_v2(db,"SELECT i.installed_for,a.scope,i.pack,w.release,w.entry,w.family FROM _application_workspace w JOIN pack_installation i ON i.id=w.installation_id JOIN pack_activation a ON a.activated_for=i.installed_for AND a.pack=i.pack AND a.namespace=i.namespace JOIN application_binding b ON b.id=w.cms_id AND b.namespace=i.namespace WHERE w.binding_id=?1 AND b.binding_id=?1 AND b.status='active' AND i.status='published' AND a.status='published'",-1,&s,NULL);
        if(lookup_code==SQLITE_OK)lookup_code=sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);
        if(lookup_code==SQLITE_OK){
            lookup_code=sqlite3_step(s);
            if(lookup_code==SQLITE_ROW){
                int valid=bfpg_text(s,0,subject,sizeof(subject))&&bfpg_text(s,1,scope,sizeof(scope))&&bfpg_text(s,2,pack,sizeof(pack))&&bfpg_text(s,3,release,sizeof(release))&&bfpg_text(s,4,entry,sizeof(entry))&&bfpg_text(s,5,family,sizeof(family));
                lookup_code=sqlite3_step(s);found=valid&&lookup_code==SQLITE_DONE;
                if(lookup_code==SQLITE_ROW)lookup_code=SQLITE_DONE; /* ambiguous binding: refuse */
            }
        }
        int retry=lookup_code!=SQLITE_DONE&&sqlite3_extended_errcode(db)==SQLITE_IOERR_SHMOPEN&&attempt==0;
        sqlite3_finalize(s);sqlite3_close(db);
        if(!retry)break;usleep(25000);
    }
    if(lookup_code!=SQLITE_DONE){fprintf(stderr,"workspace lookup unavailable: sqlite=%d\n",lookup_code);aw_error(503,"Workspace storage could not be read. Try again; no configuration or saved work was changed.\n");return 1;}
    if(!found){aw_error(404,"Workspace is unavailable or inactive. Review its configuration.\n");return 1;}
    if(strlen(subject)>=sizeof(context) || !bfpg_token(subject,sizeof(subject)) || !bfpg_token(pack,sizeof(pack))){aw_error(409,"The workspace binding is invalid.\n");return 1;}
    snprintf(context,sizeof(context),"%s",!strcmp(scope,"self")?"self":subject);
    int post=!strcmp(method,"POST"),permission=psetup_context(bearer,auth_db,context,post?2:0,actor,sizeof(actor),checked_subject,sizeof(checked_subject),&checked_scope);
    if(permission!=1||strcmp(subject,checked_subject)||strcmp(scope,checked_scope?checked_scope:"")){
        if(!permission && !post && strcmp(slash+1,"_records")){
            if(strlen(target)+(query&&*query?strlen(query)+1:0)>=BF_AUTH_NEXT_MAX){aw_error(400,"The editor return address is too long.\n");return 1;}
            fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);url_query_value(target);if(query&&*query){fputs("%3F",stdout);url_query_value(query);}fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);
        }else aw_error(403,"Sign in with current access to this destination. Your open edits have not been saved.\n");return 1;
    }
    if(!strcmp(slash+1,"_relationship-observation"))return ro_serve(method,query,body,raw,host,id,subject,scope,actor,context,pack,family,release,entry);
    if(!strcmp(slash+1,"_campaign-link")){
        if(strcmp(pack,"experience.source-campaign.workspace") || strcmp(entry,"campaigns/") || !ac_verified(id,subject,scope,pack,release,entry,actor)){
            aw_error(409,"Open a current campaign workspace to build a link.\n");return 1;
        }
        char back[128];
        if(snprintf(back,sizeof(back),"/apps/%s/campaigns/",id)>=(int)sizeof(back)){
            aw_error(409,"The campaign workspace address is unavailable.\n");return 1;
        }
        return cwl_serve(method,query,body,raw,host,back);
    }
    if(!strcmp(slash+1,"_records")){
        char operation[24],prefix[512],cursor[512],epoch[24],content_type[128],origin[512],expected[512],length[32];
        if(!aw_query(query,operation,prefix,cursor,epoch)){aw_error(400,"Invalid workspace operation or duplicate argument.\n");return 1;}
        int writing=!strcmp(operation,"write")||!strcmp(operation,"blob-put");size_t bytes=0;
        if(writing!=post || (!post && strcmp(method,"GET"))){aw_error(405,"Use the operation's declared method.\n");return 1;}
        if(post){
            snprintf(expected,sizeof(expected),"%s://%s",(host && (!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10)))?"http":"https",host?host:"");
            if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)||!aw_header(raw,"Content-Type",content_type,sizeof(content_type))||strcmp(content_type,"application/msgpack")||!aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strspn(length,"0123456789")!=strlen(length)){aw_error(403,"Submit this edit from its native workspace.\n");return 1;}
            errno=0;unsigned long long parsed=strtoull(length,NULL,10);if(errno||parsed>AW_PACKET){aw_error(413,"The native draft packet exceeds its bound.\n");return 1;}bytes=(size_t)parsed;
        }
        const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
        char *const argv[]={(char *)bin,"pack-workspace","--operation",operation,"--binding",id,"--subject",subject,"--scope",scope,"--actor",actor,"--prefix",prefix,"--cursor",cursor,"--epoch",epoch,"--db",(char *)cms,NULL};
        unsigned char *output=NULL;size_t size=0;int rc=aw_native(argv,body,bytes,&output,&size);
        if(!rc)aw_response(200,"application/msgpack",output,size,0);
        else aw_error(rc==4?409:rc==3?400:rc==5?413:503,rc==4?"A draft changed in another tab or this workspace changed. Nothing was overwritten.\n":"The native owner did not confirm this save. Keep the open edit and retry after resolving the error.\n");
        free(output);return 1;
    }
    if(!strcmp(slash+1,"_approval_status")){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"Review this workspace before reconciling an approval request.\n");return 1;}
        return pas_status(method,query,id,subject,scope,actor,auth_db);
    }
    if(!strcmp(slash+1,"_procurement_downstream")){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"Review this workspace before reconciling downstream procurement state.\n");return 1;}
        return pds_status(method,query,id,subject,scope,actor);
    }
    if(!strcmp(slash+1,"_procurement_enactment")){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"Review this workspace before enacting supplier acceptance.\n");return 1;}
        return pde_enact(method,query,body,raw,host,id,subject,scope,actor,auth_db);
    }
    if(!strcmp(slash+1,"_procurement_enactment_readiness")){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"Review this workspace before checking procurement enactment readiness.\n");return 1;}
        return pde_readiness(method,query,id,subject,scope,actor,auth_db);
    }
    if(!strcmp(slash+1,"_retained_inputs")){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"Review this workspace before selecting retained source material.\n");return 1;}
        return pr_inputs(method,query,host);
    }
    if(!strcmp(slash+1,"_publication_inputs")){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"Review this workspace before choosing its inputs.\n");return 1;}
        return pw_inputs(method,query);
    }
    if(!strcmp(slash+1,"_narrate")){
        if(strcmp(entry,"audio/") || !ac_verified(id,subject,scope,pack,release,entry,actor)){
            aw_error(409,"Open a current audio workspace before rendering.\n");return 1;
        }
        if(query && *query){aw_error(400,"Narration settings belong in the reviewed request.\n");return 1;}
        return nw_render(method,body,raw,host);
    }
    if(strcmp(method,"GET")){aw_error(405,"Use GET to open the editor.\n");return 1;}
    if(!ac_verified(id,subject,scope,pack,release,entry,actor)){aw_error(409,"The installed source or editor binding changed. Review this configuration before opening its editor.\n");return 1;}
    if(ac_views(slash+1,query,id,subject,scope,pack,release,actor,context))return 1;
    if(ac_work(slash+1,query,id,subject,scope,pack,family,release,entry,actor,context))return 1;
    char path[1024];const char *relative=slash+1;if(!*relative)relative=entry;
    if(snprintf(path,sizeof(path),"%s%s",relative,relative[strlen(relative)-1]=='/'?"index.html":"")>=(int)sizeof(path)){aw_error(404,"Editor path not found.\n");return 1;}
    size_t bytes=0;unsigned char *file=aw_package(release,path,&bytes);if(!file){aw_error(503,"The reviewed editor release is unavailable. Drafts remain in the native store.\n");return 1;}
    const char *mime=aw_mime(path);
    if(!strncmp(mime,"text/",5)){
        if(memchr(file,0,bytes)){free(file);aw_error(503,"The editor text release is invalid.\n");return 1;}
        const char *tokens[]={"__WORKSPACE__","__DESTINATION__","__PACK__","__CONTEXT__","__RELEASE__"};const char *values[]={id,subject,pack,context,release};
        for(size_t n=0;n<5&&file;n++)file=aw_replace(file,&bytes,tokens[n],values[n]);
    }
    if(file && !strncmp(mime,"text/html",9) && strstr((char *)file,"__WORKSPACE_NAV__")){
        size_t nav_bytes=0;unsigned char *nav=ac_navigation(id,context,pack,&nav_bytes);
        if(nav){file=aw_replace(file,&bytes,"__WORKSPACE_NAV__",(char *)nav);free(nav);}else{free(file);file=NULL;}
    }
    if(file){aw_response(200,mime,file,bytes,0);free(file);}else aw_error(503,"The editor projection failed. Drafts were not changed.\n");return 1;
}
#endif
