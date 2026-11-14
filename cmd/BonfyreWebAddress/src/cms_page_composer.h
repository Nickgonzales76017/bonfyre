#ifndef BONFYRE_CMS_PAGE_COMPOSER_H
#define BONFYRE_CMS_PAGE_COMPOSER_H
#include "cms_page_composer_markup.h"
#include "cms_page_composer_visual.h"
#include "article_revision.h"
/* A browser carrier for the existing namespace-scoped CMS page aggregate. */
static void cms_composer_reply(int code,const char *type,const char *body) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nX-Frame-Options: SAMEORIGIN\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
        code,code==200?"OK":code==403?"Forbidden":code==409?"Conflict":code==503?"Service Unavailable":"Bad Request",type,strlen(body),body);
}
static int serve_cms_composer(const char *method,const char *target,const char *query,const char *body,
                             const char *bearer,const char *auth_db,const char *origin,const char *host) {
    int home=!strcmp(target,"/cms/compose"),page=!strcmp(target,"/cms/compose/page"),listing=!strcmp(target,"/cms/compose/pages"),visual=!strcmp(target,"/cms/compose/advanced"),revision=!strcmp(target,"/cms/compose/narration-revision"),revising=!strcmp(target,"/cms/compose/revise"),publishing=!strcmp(target,"/cms/compose/publish"),exporting=!strcmp(target,"/cms/compose/export");
    if(!home&&!page&&!listing&&!visual&&!revision&&!revising&&!publishing&&!exporting)return 0;
    int writing=(page||publishing||revising)&&!strcmp(method,"POST");
    int write_grant=session_authorized(bearer,auth_db,"write");
    int self_site_scope=!write_grant&&session_authorized(bearer,auth_db,"self");
    if(((publishing||revising)&&!writing)||(!writing&&strcmp(method,"GET"))||(writing&&(!body||!*body||strlen(body)>220000))) {
        cms_composer_reply(400,"application/json","{\"error\":\"Use a complete page request within the 220 KB save limit.\"}");return 1;
    }
    char ns[192]={0},requested_site[128]={0};
    int site_arg=bf_lex_form_query(query?query:"","site",requested_site,sizeof(requested_site));
    if(site_arg<0||(site_arg==1&&!requested_site[0])){
        cms_composer_reply(400,"application/json","{\"error\":\"Invalid profile site.\"}");return 1;
    }
    if((home||visual)&&!strcmp(method,"GET")&&(!session_valid(bearer,auth_db)||(!write_grant&&!self_site_scope))) {
        ak_app_http(403,1);ak_app_open("Compose a page","workbench","route-compose");
        fputs("<div class=\"ak-wb-strip\"><span>Studio</span><span>Composer</span><span class=\"ak-live\">Signed out</span></div>"
              "<section class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\"><a href=\"/studio\">Studio</a> &middot; Page composition</p>"
              "<h1>Shape a page people can use.</h1><p class=\"ak-lede\">The composer opens the pages of your own address. Sign in with an account that has a site, then open a page or start one.</p>"
              "<div class=\"ak-actions\"><a class=\"ak-btn is-primary\" href=\"/login?next=/cms/compose\">Sign in</a><a class=\"ak-btn\" href=\"/studio\">Back to the studio</a></div></div></section>",stdout);
        ak_app_close();return 1;
    }
    if(!session_valid(bearer,auth_db)||(!write_grant&&!self_site_scope)||
       (site_arg==0&&(!cms_namespace_for(bearer,auth_db,ns,sizeof(ns))||strlen(ns)>127))) {
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
    /* A profile opens its own site namespace: ?site=<site-id> resolves only
     * to a published member site the bearer owns, exactly the ownership
     * proof a profile mutation demands. Anything else keeps the actor
     * workspace; no sharing model is implied. */
    int exact_owned_site=0;char owned_actor[192]={0};
    {
        const char *site=requested_site;
        if(site_arg==1) {
            char actor[192]="";
            StudioViewer who={0};
            PhSite owned;
            if(!session_identity(bearer,auth_db,&who)||who.id<1||
               !ph_actor(who.id,auth_db,actor)||!ph_site(db,NULL,site,&owned)||
               strcmp(owned.actor,actor)) {
                sqlite3_close(db);
                cms_composer_reply(403,"application/json","{\"error\":\"This profile site is not available to this account.\"}");return 1;
            }
            snprintf(ns,sizeof(ns),"%s",site);
            exact_owned_site=1;snprintf(owned_actor,sizeof(owned_actor),"%s",actor);
        }
    }
    /* A copy of the whole site: every page's exact snapshot, read through
     * the same page-read owner the composer opens, in one file. Each page
     * keeps its own digest, so a later restore can be checked page by page. */
    if(exporting&&(exact_owned_site||write_grant)&&!strcmp(method,"GET")){
        sqlite3_stmt *st=NULL;bf_jsonw w;int pages=0,refused=0;
        char routes[256][128];int nr=0;
        if(sqlite3_prepare_v2(db,"SELECT route FROM site_page WHERE namespace=?1 ORDER BY route LIMIT 256",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);
            while(sqlite3_step(st)==SQLITE_ROW&&nr<256){const char *r=(const char*)sqlite3_column_text(st,0);if(r&&strlen(r)<128)snprintf(routes[nr++],128,"%s",r);}
        }
        sqlite3_finalize(st);sqlite3_close(db);
        bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
        bf_jsonw_key(&w,"schema");bf_jsonw_str(&w,"bonfyre.site-export.v1");
        bf_jsonw_key(&w,"namespace");bf_jsonw_str(&w,ns);
        {char when[32];time_t now=time(NULL);struct tm tmv;gmtime_r(&now,&tmv);strftime(when,sizeof(when),"%Y-%m-%dT%H:%M:%SZ",&tmv);bf_jsonw_key(&w,"exported_at");bf_jsonw_str(&w,when);}
        bf_jsonw_key(&w,"pages");bf_jsonw_arr(&w);
        size_t cap=1024*1024+2048;char *output=calloc(1,cap),diagnostic[2048];
        char unread[256][128];int nu=0;
        for(int i=0;output&&i<nr;i++){
            memset(output,0,cap);
            char *argv[]={(char*)bin,"entry","page-read",routes[i],"--ns",ns,"--db",(char*)dbpath,"--schemas",(char*)schemas,NULL};
            int rc=auth_call_split(argv,NULL,output,cap,diagnostic,sizeof(diagnostic));
            bf_json_doc_t *doc=rc==0&&strlen(output)<cap-1?bf_json_parse(output,strlen(output),NULL,0):NULL;
            if(doc){bf_jsonw_node(&w,doc,bf_json_root(doc),0);bf_json_free(doc);pages++;}
            else{snprintf(unread[nu++],128,"%s",routes[i]);refused++;}
        }
        free(output);
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w,"unread_routes");bf_jsonw_arr(&w);for(int i=0;i<nu;i++)bf_jsonw_str(&w,unread[i]);bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w,"page_count");bf_jsonw_int(&w,pages);
        bf_jsonw_obj_end(&w);
        if(w.oom||!w.buf){bf_jsonw_free(&w);cms_composer_reply(503,"application/json","{\"error\":\"The copy could not be assembled.\"}");return 1;}
        printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Disposition: attachment; filename=\"site-copy.json\"\r\n"
               "Cache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nX-Content-Type-Options: nosniff\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",strlen(w.buf));
        fputs(w.buf,stdout);bf_jsonw_free(&w);(void)refused;return 1;
    }
    if(self_site_scope&&!exact_owned_site&&(home||visual)&&!strcmp(method,"GET")){
        sqlite3_close(db);ak_app_http(200,1);ak_app_open("Compose a page","workbench","route-compose");
        fputs("<div class=\"ak-wb-strip\"><span>Studio</span><span>Composer</span><span class=\"ak-live\">No address yet</span></div>"
              "<section class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\"><a href=\"/studio\">Studio</a> &middot; Page composition</p>"
              "<h1>Shape a page people can use.</h1><p class=\"ak-lede\">The composer writes the pages of an address you own. This account has none bound yet; claim one from your profile, then come back to compose.</p>"
              "<div class=\"ak-actions\"><a class=\"ak-btn is-primary\" href=\"/profile\">Claim an address</a><a class=\"ak-btn\" href=\"/studio\">Back to the studio</a></div></div></section>",stdout);
        ak_app_close();return 1;
    }
    if(self_site_scope&&!exact_owned_site){sqlite3_close(db);cms_composer_reply(403,"application/json","{\"error\":\"A current, owned profile site is required. \"}");return 1;}
    /* Both editors are pages of the one application: the estate bar, one
     * <main>, the footer and the player. Their layout is composition.css. */
    if(home){sqlite3_close(db);ak_app_http(200,1);ak_app_open("Compose a page","workbench","route-compose");fputs(cms_page_composer_markup,stdout);ak_app_close();return 1;}
    if(visual){sqlite3_close(db);ak_app_http(200,1);ak_app_open("Visual composition","workbench","route-compose route-compose-visual");fputs(cms_page_composer_visual_markup,stdout);ak_app_close();return 1;}
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
    if((!writing||publishing||revising)&&(bf_lex_form_query(query?query:"","route",route,sizeof(route))!=1||route[0]!='/')){
        sqlite3_close(db);cms_composer_reply(400,"application/json","{\"error\":\"Choose a local page address.\"}");return 1;
    }

    if(publishing){
        if(!exact_owned_site||(!write_grant&&!self_site_scope)||!owned_actor[0]){
            sqlite3_close(db);cms_composer_reply(403,"application/json","{\"error\":\"This site needs its current publishing owner.\"}");return 1;
        }
        if(!body||strlen(body)>2048){
            sqlite3_close(db);cms_composer_reply(400,"application/json","{\"error\":\"Review one article revision at a time.\"}");return 1;
        }
        sqlite3_stmt *review=NULL;char digest[65]={0};long long expected=0;int reviewed=0;
        const char *sql=
          "SELECT json_extract(?1,'$.expected_id'),json_extract(?1,'$.expected_digest') "
          "WHERE json_valid(?1) AND json_type(?1)='object' AND json_type(?1,'$.expected_id')='integer' "
          "AND json_type(?1,'$.expected_digest')='text' AND json_type(?1,'$.rights_confirmed')='true' "
          "AND NOT EXISTS(SELECT 1 FROM json_tree(?1) WHERE parent IS NOT NULL GROUP BY parent,key HAVING count(*)>1)";
        if(sqlite3_prepare_v2(db,sql,-1,&review,NULL)==SQLITE_OK){
            sqlite3_bind_text(review,1,body,-1,SQLITE_STATIC);
            if(sqlite3_step(review)==SQLITE_ROW){
                expected=sqlite3_column_int64(review,0);
                const char *v=(const char*)sqlite3_column_text(review,1);
                if(v&&strlen(v)==64&&strspn(v,"0123456789abcdef")==64){
                    snprintf(digest,sizeof(digest),"%s",v);reviewed=expected>0;
                }
            }
        }
        sqlite3_finalize(review);
        if(!reviewed){
            sqlite3_close(db);cms_composer_reply(400,"application/json","{\"error\":\"An exact saved revision and explicit publication review are required.\"}");return 1;
        }
        char id_arg[32],output[2048]={0},diagnostic[512]={0};
        snprintf(id_arg,sizeof(id_arg),"%lld",expected);
        char *argv[]={(char*)bin,"entry","page-release",route,digest,
            "--ns",ns,"--expected-id",id_arg,"--actor",owned_actor,
            "--db",(char*)dbpath,"--schemas",(char*)schemas,NULL};
        int result=auth_call_split(argv,NULL,output,sizeof(output),diagnostic,sizeof(diagnostic));
        sqlite3_stmt *confirm=NULL;int committed=0;
        if(sqlite3_prepare_v2(db,"SELECT json_extract(?1,'$.committed')=1 AND json_extract(?1,'$.id')=?2 WHERE json_valid(?1)",-1,&confirm,NULL)==SQLITE_OK){
            sqlite3_bind_text(confirm,1,output,-1,SQLITE_STATIC);
            sqlite3_bind_int64(confirm,2,expected);
            committed=sqlite3_step(confirm)==SQLITE_ROW&&sqlite3_column_int(confirm,0);
        }
        sqlite3_finalize(confirm);sqlite3_close(db);
        if(committed){
            /* A notification error is not a rollback. The native receipt
             * identifies it explicitly so the public indexing owner can retry. */
            (void)result;cms_composer_reply(200,"application/json",output);
        }else cms_composer_reply(409,"application/json","{\"error\":\"The article changed, was already released, or failed its publication checks. Reopen the current revision.\"}");
        return 1;
    }
    if(revision){
        sqlite3_stmt *st=NULL;char ref[192]={0},state[16]={0},digest[65]={0};int ok=0;
        if(sqlite3_exec(db,"SAVEPOINT composer_narration_revision",NULL,NULL,NULL)==SQLITE_OK){
            if(sqlite3_prepare_v2(db,"SELECT media_subject_ref,status FROM site_page WHERE namespace=?1 AND route=?2 LIMIT 2",-1,&st,NULL)==SQLITE_OK){
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
        if(!session_valid(bearer,auth_db)||(!session_authorized(bearer,auth_db,"write")&&
           (!exact_owned_site||!session_authorized(bearer,auth_db,"self"))))ok=0;
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
    /* Site-owner reviewed revision: no route, credit or retained source transfer. */
    if((page||revising)&&writing){
        sqlite3_stmt *guard=NULL;int admitted=0;
        const char *sql=revising?
          "SELECT 1 FROM site_page p WHERE p.namespace=?1 AND p.route=?2 AND p.status='published' "
          "AND json_valid(?3) AND json_extract(?3,'$.operation')='replace' "
          "AND json_extract(?3,'$.route')=?2 AND json_extract(?3,'$.page.route')=?2 "
          "AND json_extract(?3,'$.expected_id')=p.id "
          "AND json_extract(?3,'$.page.status')='published' "
          "AND json_type(?3,'$.rights_confirmed')='true' "
          "AND json_type(?3,'$.revision_confirmed')='true' "
          "AND COALESCE(json_extract(?3,'$.page.author_credit'),'')=COALESCE(p.author_credit,'') "
          "AND COALESCE(json_extract(?3,'$.page.author_subject_ref'),'')=COALESCE(p.author_subject_ref,'') "
          "AND COALESCE(json_extract(?3,'$.page.editorial_kind'),'')=COALESCE(p.editorial_kind,'') "
          "AND COALESCE(json_extract(?3,'$.page.media_subject_ref'),'')=COALESCE(p.media_subject_ref,'') "
          "AND COALESCE(json_extract(?3,'$.page.editorial_published_on'),'')=COALESCE(p.editorial_published_on,'') "
          "AND json_type(?3,'$.fragments')='array' "
          "AND NOT EXISTS(SELECT 1 FROM json_each(?3,'$.fragments') WHERE json_extract(value,'$.status')!='published') "
          "AND NOT EXISTS(SELECT 1 FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route "
          "AND f.status='published' AND NOT EXISTS(SELECT 1 FROM json_each(?3,'$.fragments') j "
          "WHERE json_extract(j.value,'$.coordinate')=f.coordinate AND "
          "COALESCE(json_extract(j.value,'$.source_ref'),'')=COALESCE(f.source_ref,'') AND "
          "COALESCE(json_extract(j.value,'$.source_digest'),'')=COALESCE(f.source_digest,''))) LIMIT 1":
          "SELECT 1 WHERE json_valid(?3) AND json_extract(?3,'$.page.status')='draft' "
          "AND NOT EXISTS(SELECT 1 FROM site_page WHERE namespace=?1 AND route=json_extract(?3,'$.route') AND status='published')";
        if((!revising||exact_owned_site)&&
            sqlite3_prepare_v2(db,sql,-1,&guard,NULL)==SQLITE_OK){
            sqlite3_bind_text(guard,1,ns,-1,SQLITE_STATIC);
            sqlite3_bind_text(guard,2,route,-1,SQLITE_STATIC);
            sqlite3_bind_text(guard,3,body,-1,SQLITE_STATIC);
            admitted=sqlite3_step(guard)==SQLITE_ROW;
        }
        sqlite3_finalize(guard);
        if(!admitted){sqlite3_close(db);cms_composer_reply(revising?409:403,"application/json",
            "{\"error\":\"Use an exact owned revision and preserve the credited public source.\"}");return 1;}
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
