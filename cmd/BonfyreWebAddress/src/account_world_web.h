#ifndef BONFYRE_ACCOUNT_WORLD_WEB_H
#define BONFYRE_ACCOUNT_WORLD_WEB_H
#include "../../BonfyreCMS/src/account_world.h"
#include "bf_jsonw.h"

/* Account spaces.
 *
 * /workspace/world   private, site-scoped API (JSON, or a 303 back to the studio)
 * /workspace/worlds  the owner's studio for one site, in the profile's dress
 * /worlds, /worlds/X the public spaces a member released on their own host
 *
 * Auth owns the principal-to-person closure; Matrix and SitePresentation own
 * the site; the CMS owner holds spaces, items by reference and releases. The
 * reader re-resolves every item through PageComposition on each request and
 * only plays or shows media whose own source states open terms. Nothing here
 * copies material, states rights, or publishes the World row itself. */
static int world_web_admit(const char *bearer,const char *auth_db,sqlite3 *db,
                           const char *site_id,int write,char subject[160]) {
    char actor[160],checked[160],person[192];const char *scope=NULL;
    StudioViewer viewer={0};PhSite site;
    if(psetup_context(bearer,auth_db,"self",write?2:0,actor,sizeof(actor),checked,sizeof(checked),&scope)!=1 ||
       !session_identity(bearer,auth_db,&viewer) || viewer.id<1 ||
       !ph_actor(viewer.id,auth_db,person) || !ph_site(db,NULL,site_id,&site) || strcmp(person,site.actor))return 0;
    snprintf(subject,160,"%s",checked);return 1;
}
static int world_web_number(const char *s,sqlite3_int64 *value,int allow_zero){
    char *end=NULL;if(!s||!*s||strlen(s)>19||strspn(s,"0123456789")!=strlen(s))return 0;
    errno=0;long long n=strtoll(s,&end,10);if(errno||!end||*end||n<(!allow_zero)||n>INT_MAX)return 0;
    *value=(sqlite3_int64)n;return 1;
}
/* A look is one of the CUE-compiled World characters; world.css owns how it
 * dresses. The resilience fallback is not offered as a choice. */
static const char *world_web_look(const char *character){
    if(!character||!*character||!strcmp(character,"default"))return NULL;
    for(size_t i=0;i<BF_WORLD_MATERIAL_RENDERER_COUNT;i++)if(!strcmp(character,bf_world_material_renderers[i].character)){
        const char *c=character;
        return !strcmp(c,"zine")?"Flyer wall":!strcmp(c,"scrapbook")?"Scrapbook":!strcmp(c,"dossier")?"Case file":
               !strcmp(c,"ticker")?"Ticker":!strcmp(c,"almanac")?"Almanac":!strcmp(c,"journal")?"Journal":c;
    }
    return NULL;
}
static void world_web_words(const char *slug){ /* "record-label" -> "record label" */
    char out[40];size_t n=0;for(;slug&&*slug&&n+1<sizeof(out);slug++)out[n++]=*slug=='-'?' ':*slug;out[n]=0;html(out);
}

enum{WF_SITE,WF_WORLD,WF_OP,WF_SPACE,WF_TITLE,WF_RENDERER,WF_TEMPLATE,WF_LAYOUT,WF_REVISION,
     WF_MEDIUM,WF_CHARACTER,WF_LEDE,WF_POSITION,WF_REF,WF_ROLE,WF_NOTE,WF_RETURN,WF_COUNT};
#define WB(x) (1u<<(x))
typedef struct{const char *op;int post;unsigned required,optional;} WorldOp;
static const WorldOp world_ops[]={
    {"create",1,0,0},
    {"bind",1,WB(WF_WORLD),0},
    {"put",1,WB(WF_WORLD)|WB(WF_SPACE)|WB(WF_TITLE)|WB(WF_RENDERER)|WB(WF_TEMPLATE)|WB(WF_LAYOUT)|WB(WF_REVISION),0},
    {"style",1,WB(WF_WORLD)|WB(WF_SPACE)|WB(WF_MEDIUM)|WB(WF_CHARACTER)|WB(WF_REVISION),WB(WF_LEDE)},
    {"item",1,WB(WF_WORLD)|WB(WF_SPACE)|WB(WF_POSITION)|WB(WF_REF)|WB(WF_ROLE)|WB(WF_REVISION),WB(WF_NOTE)},
    {"unitem",1,WB(WF_WORLD)|WB(WF_SPACE)|WB(WF_POSITION)|WB(WF_REVISION),0},
    {"release",1,WB(WF_WORLD)|WB(WF_SPACE)|WB(WF_REVISION),0},
    {"withdraw",1,WB(WF_WORLD)|WB(WF_SPACE),0},
    {"read",0,WB(WF_WORLD)|WB(WF_SPACE),0},
    {"list",0,WB(WF_WORLD),0},
    {"items",0,WB(WF_WORLD)|WB(WF_SPACE),0},
};
static void world_web_studio_location(const char *site,sqlite3_int64 world,const char *space){
    char location[400];int n=snprintf(location,sizeof(location),"/workspace/worlds?site=%s",site);
    if(world>0)n+=snprintf(location+n,sizeof(location)-(size_t)n,"&world=%lld",(long long)world);
    if(space&&*space)snprintf(location+n,sizeof(location)-(size_t)n,"&space=%s",space);
    page_redirect(location);
}
/* A new draft World row on this site, bound to the account at once. The CMS
 * owner creates it; its sources stay "none" because a space names its own
 * material by reference. */
static int world_web_create(sqlite3 *db,const char *subject,const char *site,sqlite3_int64 *out){
    char slug[40],nonce[33],payload[1024];ph_nonce(nonce);snprintf(slug,sizeof(slug),"spaces-%.12s",nonce);
    bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
    const char *k[]={"world","title","character","lede","subject","sources","cannot_infer","status"};
    const char *v[]={slug,"Spaces","journal","Spaces kept on this site.",subject,"none",
                     "A space names material by reference; it does not own, license or publish that material.","draft"};
    for(int i=0;i<8;i++){bf_jsonw_key(&w,k[i]);bf_jsonw_str(&w,v[i]);}bf_jsonw_obj_end(&w);
    int ok=!w.oom&&w.len<sizeof(payload);if(ok){memcpy(payload,w.buf,w.len);payload[w.len]=0;}bf_jsonw_free(&w);
    if(!ok||!ph_cms("create","world",NULL,payload,site,NULL,NULL,NULL,NULL))return -1;
    sqlite3_stmt *st=NULL;*out=0;
    if(sqlite3_prepare_v2(db,"SELECT id FROM world WHERE namespace=?1 AND world=?2 AND subject=?3 AND status='draft'",-1,&st,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(st,1,site,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,slug,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,subject,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW)*out=sqlite3_column_int64(st,0);sqlite3_finalize(st);
    return *out>0?aw_bind(db,subject,site,*out):-1;
}
static int world_web_api(const char *method,const char *query,const char *body,const char *raw,const char *host,
                         const char *bearer,const char *auth_db){
    int post=!strcmp(method,"POST"),get=!strcmp(method,"GET");
    if(!post&&!get){aw_error(405,"Use GET or POST.\n");return 1;}
    if(post&&(!ph_origin(raw,host)||(query&&*query))){aw_error(403,"Submit from this workspace.\n");return 1;}
    char v[WF_COUNT][608];memset(v,0,sizeof(v));
    static const char *names[WF_COUNT]={"site","world","operation","space","title","renderer","template","layout","revision",
                                         "medium","character","lede","position","ref","role","note","return"};
    static const size_t caps[WF_COUNT]={128,24,24,65,257,8,257,257,24,33,33,601,8,161,33,281,8};
    PhField fields[WF_COUNT];for(int i=0;i<WF_COUNT;i++){fields[i].key=names[i];fields[i].value=v[i];fields[i].cap=caps[i];fields[i].multiline=i==WF_LEDE||i==WF_NOTE;}
    const WorldOp *op=NULL;
    if(!ph_fields(post?body:query,fields,WF_COUNT,8192)||!*v[WF_SITE]){aw_error(400,"Invalid workspace request.\n");return 1;}
    for(size_t i=0;i<sizeof(world_ops)/sizeof(world_ops[0]);i++)if(!strcmp(v[WF_OP],world_ops[i].op)&&world_ops[i].post==post)op=&world_ops[i];
    if(!op){aw_error(400,"Invalid workspace request.\n");return 1;}
    unsigned allowed=op->required|op->optional|WB(WF_SITE)|WB(WF_OP)|(post?WB(WF_RETURN):0);
    for(int i=0;i<WF_COUNT;i++)if((*v[i]&&!(allowed&WB(i)))||(!*v[i]&&(op->required&WB(i)))){aw_error(400,"Invalid workspace coordinates.\n");return 1;}
    int studio=!strcmp(v[WF_RETURN],"studio");if(*v[WF_RETURN]&&!studio){aw_error(400,"Invalid workspace coordinates.\n");return 1;}
    sqlite3_int64 world_id=0,rev=0,position=0;
    if((*v[WF_WORLD]&&!world_web_number(v[WF_WORLD],&world_id,0))||(*v[WF_REVISION]&&!world_web_number(v[WF_REVISION],&rev,1))||
       (*v[WF_POSITION]&&(!world_web_number(v[WF_POSITION],&position,0)||position>AW_ITEM_LIMIT))||
       (*v[WF_CHARACTER]&&!world_web_look(v[WF_CHARACTER]))){aw_error(400,"Invalid workspace coordinates.\n");return 1;}
    const char *path=getenv("BONFYRE_CMS_DB");sqlite3 *db=NULL;char subject[160],again[160];const char *site=v[WF_SITE],*space=v[WF_SPACE];
    if(!path||!*path||!(db=post?cms_open_rw():cms_open_ro())){if(db)sqlite3_close(db);aw_error(503,"Workspace unavailable.\n");return 1;}
    sqlite3_busy_timeout(db,2000);
    if(!world_web_admit(bearer,auth_db,db,site,post,subject)){sqlite3_close(db);aw_error(404,"Workspace unavailable.\n");return 1;}
    if(post&&aw_initialize(db)!=1){sqlite3_close(db);aw_error(503,"Workspace unavailable.\n");return 1;}
    if(!world_web_admit(bearer,auth_db,db,site,post,again)||strcmp(subject,again)){sqlite3_close(db);aw_error(404,"Workspace unavailable.\n");return 1;}
    sqlite3_stmt *row=NULL,*items=NULL;int result;const char *o=op->op;
    if(!strcmp(o,"create"))result=world_web_create(db,subject,site,&world_id);
    else if(!strcmp(o,"bind"))result=aw_bind(db,subject,site,world_id);
    else if(!strcmp(o,"put"))result=aw_space_put(db,subject,site,world_id,space,v[WF_TITLE],v[WF_RENDERER],v[WF_TEMPLATE],v[WF_LAYOUT],(int)rev);
    else if(!strcmp(o,"style"))result=aw_space_style(db,subject,site,world_id,space,v[WF_MEDIUM],v[WF_CHARACTER],v[WF_LEDE],(int)rev);
    else if(!strcmp(o,"item"))result=aw_item_put(db,subject,site,world_id,space,(int)position,v[WF_REF],v[WF_ROLE],v[WF_NOTE],(int)rev);
    else if(!strcmp(o,"unitem"))result=aw_item_remove(db,subject,site,world_id,space,(int)position,(int)rev);
    else if(!strcmp(o,"release"))result=aw_space_release(db,subject,site,world_id,space,(int)rev);
    else if(!strcmp(o,"withdraw"))result=aw_space_withdraw(db,subject,site,world_id,space);
    else if(!strcmp(o,"list"))result=aw_space_list(db,subject,site,world_id,&row);
    else if(!strcmp(o,"items")){result=aw_space_detail(db,subject,site,world_id,space,&row);if(result==1)result=aw_items(db,subject,site,world_id,space,&items);}
    else result=aw_space_get(db,subject,site,world_id,space,&row);
    if(result!=1){sqlite3_finalize(row);sqlite3_finalize(items);sqlite3_close(db);aw_error(result<0?503:post&&strcmp(o,"bind")?409:404,"Workspace request could not be completed.\n");return 1;}
    if(post&&studio){sqlite3_close(db);world_web_studio_location(site,world_id,!strcmp(o,"bind")||!strcmp(o,"create")?"":space);return 1;}
    bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"state");bf_jsonw_str(&w,post?"saved":"draft");
    if(!strcmp(o,"create")){bf_jsonw_key(&w,"world");bf_jsonw_int(&w,world_id);}
    int step=SQLITE_DONE;
    if(!strcmp(o,"list")){
        bf_jsonw_key(&w,"spaces");bf_jsonw_arr(&w);
        int count=0;
        while(row&&count<AW_SPACE_LIMIT&&(step=sqlite3_step(row))==SQLITE_ROW){
            bf_jsonw_obj(&w);const char *keys[]={"space","title","renderer","template","layout"};
            for(int i=0;i<5;i++){bf_jsonw_key(&w,keys[i]);bf_jsonw_str(&w,(const char*)sqlite3_column_text(row,i));}
            bf_jsonw_key(&w,"revision");bf_jsonw_int(&w,sqlite3_column_int64(row,5));bf_jsonw_obj_end(&w);count++;
        }
        if(row&&count==AW_SPACE_LIMIT)step=sqlite3_step(row);
        bf_jsonw_arr_end(&w);
    }else if(!strcmp(o,"items")){
        const char *keys[]={"medium","character","lede","release"};for(int i=0;i<4;i++){bf_jsonw_key(&w,keys[i]);bf_jsonw_str(&w,(const char*)sqlite3_column_text(row,i));}
        bf_jsonw_key(&w,"released_revision");bf_jsonw_int(&w,sqlite3_column_int64(row,4));
        bf_jsonw_key(&w,"items");bf_jsonw_arr(&w);
        while((step=sqlite3_step(items))==SQLITE_ROW){
            bf_jsonw_obj(&w);bf_jsonw_key(&w,"position");bf_jsonw_int(&w,sqlite3_column_int64(items,0));
            const char *ik[]={"ref","role","note"};for(int i=0;i<3;i++){bf_jsonw_key(&w,ik[i]);bf_jsonw_str(&w,(const char*)sqlite3_column_text(items,i+1));}
            bf_jsonw_obj_end(&w);
        }
        bf_jsonw_arr_end(&w);
    }else if(row){const char *keys[]={"title","renderer","template","layout"};for(int i=0;i<4;i++){bf_jsonw_key(&w,keys[i]);bf_jsonw_str(&w,(const char*)sqlite3_column_text(row,i));}bf_jsonw_key(&w,"revision");bf_jsonw_int(&w,sqlite3_column_int64(row,4));}
    bf_jsonw_obj_end(&w);sqlite3_finalize(row);sqlite3_finalize(items);sqlite3_close(db);
    if(step!=SQLITE_DONE||w.oom)aw_error(503,"Workspace unavailable.\n");else aw_response(200,"application/json",w.buf,w.len,0);bf_jsonw_free(&w);return 1;
}

#include "account_world_cinema_web.h"
#include "account_world_audio_web.h"
#include "account_world_literature_web.h"
#include "account_world_room.h"
/* ------------------------------------------------------------------ studio */
static void world_web_hidden_base(const char *site,sqlite3_int64 world,const char *space,const char *op,long long revision){
    char n[24];ph_hidden("site",site);snprintf(n,sizeof(n),"%lld",(long long)world);ph_hidden("world",n);ph_hidden("operation",op);
    if(space&&*space)ph_hidden("space",space);if(revision>=0){snprintf(n,sizeof(n),"%lld",revision);ph_hidden("revision",n);}ph_hidden("return","studio");
}
#include "account_world_picker.h"
static void world_web_space_href(const PhSite *site,const char *space){fputs("https://",stdout);html(site->host);fputs("/worlds/",stdout);html(space);}

static int serve_world_studio(const char *method,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(method,"GET")){aw_error(405,"Open your spaces with GET.\n");return 1;}
    char site_id[128]="",world[24]="",space[65]="",source[96]="",q[96]="";PhField params[]={{"site",site_id,sizeof(site_id),0},{"world",world,sizeof(world),0},{"space",space,sizeof(space),0},{"source",source,sizeof(source),0},{"q",q,sizeof(q),0}};
    sqlite3_int64 world_id=0;
    if(!ph_fields(query,params,5,1536)||!*site_id||(*world&&!world_web_number(world,&world_id,0))||(*space&&!*world)||(*source&&(!*space||!aw_picker_source(source)))||(*q&&!*space)){aw_error(400,"Choose one of your sites.\n");return 1;}
    sqlite3 *db=cms_open_ro();char subject[160];StudioViewer who={0};char actor[192]="";
    if(!db||!world_web_admit(bearer,auth_db,db,site_id,0,subject)||!session_identity(bearer,auth_db,&who)||!ph_actor(who.id,auth_db,actor)){if(db)sqlite3_close(db);aw_error(404,"These spaces are not available.\n");return 1;}
    PhSite site,sites[16];int count=ph_sites(db,actor,sites);ph_site(db,NULL,site_id,&site);
    /* Without a World named, use the one already bound to this site. */
    if(!world_id){sqlite3_stmt *st=NULL;if(sqlite3_prepare_v2(db,"SELECT b.world_id FROM account_world_binding b JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject AND w.status='draft' WHERE b.namespace=?1 AND b.subject=?2 ORDER BY b.world_id LIMIT 1",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,site_id,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,subject,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW)world_id=sqlite3_column_int64(st,0);}sqlite3_finalize(st);}
    ph_begin(db,&site,site.name,1,"worlds",actor);ph_nav(sites,count,&site,"worlds",1);
    ui_open_a(stdout,"div","class","aw-studio","data-world-studio",(const char *)NULL);
    ui_open(stdout,"div",NULL); ui_open(stdout,"h2",NULL); fputs("Your spaces",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Each space is its own page on ",stdout); ui_open(stdout,"strong",NULL);html(site.host);
    ui_close(stdout,"strong"); fputs(": a station, a label, a reading room, a screening room, or whatever you make of it. Pieces stay where they are published. A space only points to them.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div");
    sqlite3_stmt *list=NULL;
    if(!world_id||aw_space_list(db,subject,site_id,world_id,&list)!=1){
        ui_open(stdout,"section",NULL); ui_open(stdout,"h3",NULL); fputs("Start your spaces",stdout); ui_close(stdout,"h3"); ui_open(stdout,"p",NULL); fputs("Nothing is public until you choose to publish a space.",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"form","method","post","action","/workspace/world");
        ph_hidden("site",site_id);ph_hidden("operation","create");ph_hidden("return","studio");
        ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Start spaces on this site",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form"); ui_close(stdout,"section"); ui_close(stdout,"div");
        sqlite3_finalize(list);ph_end();sqlite3_close(db);return 1;
    }
    char ids[AW_SPACE_LIMIT][65];long long revs[AW_SPACE_LIMIT];int n=0;
    ui_open_a(stdout,"nav","class","aw-spaces","aria-label","Your spaces");
    while(n<AW_SPACE_LIMIT&&sqlite3_step(list)==SQLITE_ROW){
        const char *id=(const char*)sqlite3_column_text(list,0),*title=(const char*)sqlite3_column_text(list,1);
        snprintf(ids[n],65,"%s",id?id:"");revs[n]=sqlite3_column_int64(list,5);
        fputs("<a href=\"/workspace/worlds?site=",stdout);url_query_value(site_id);printf("&amp;world=%lld&amp;space=",(long long)world_id);url_query_value(ids[n]);fputs("\"",stdout);
        if(!strcmp(ids[n],space))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(title);ui_open(stdout,"small",NULL);html(ids[n]);ui_close(stdout,"small"); ui_close(stdout,"a");n++;
    }
    sqlite3_finalize(list);ui_close(stdout,"nav");
    long long revision=-1;for(int i=0;i<n;i++)if(!strcmp(ids[i],space))revision=revs[i];
    if(*space&&revision<0){ui_text(stdout, "p", NULL, "That space is not on this site.");*space=0;}
    if(*space){
        sqlite3_stmt *row=NULL,*items=NULL;char medium[33]="",character[33]="",lede[601]="",state[16]="";long long released=0;
        char title[257]="",renderer[8]="";
        if(aw_space_get(db,subject,site_id,world_id,space,&row)==1){snprintf(title,sizeof(title),"%s",(const char*)sqlite3_column_text(row,0));snprintf(renderer,sizeof(renderer),"%s",(const char*)sqlite3_column_text(row,1));}
        sqlite3_finalize(row);row=NULL;
        if(aw_space_detail(db,subject,site_id,world_id,space,&row)==1){
            snprintf(medium,sizeof(medium),"%s",(const char*)sqlite3_column_text(row,0));snprintf(character,sizeof(character),"%s",(const char*)sqlite3_column_text(row,1));
            snprintf(lede,sizeof(lede),"%s",(const char*)sqlite3_column_text(row,2));snprintf(state,sizeof(state),"%s",(const char*)sqlite3_column_text(row,3));released=sqlite3_column_int64(row,4);
        }
        sqlite3_finalize(row);
        ui_open_a(stdout,"section",UI_DYN,"data-world-space","",(space)); ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"h3","style","margin:0");html(title);fputs("</h3><span class=\"aw-state\" data-world-state=\"",stdout);
        const char *shown=!strcmp(state,"public")?(released==revision?"public":"changed"):!strcmp(state,"withdrawn")?"withdrawn":"draft";html(shown);fputs("\">",stdout);
        fputs(!strcmp(shown,"public")?"Public":!strcmp(shown,"changed")?"Public, with unpublished changes":!strcmp(shown,"withdrawn")?"Taken down":"Draft",stdout);ui_close(stdout,"span");
        if(!strcmp(state,"public")){fputs("<a class=\"ph-button\" href=\"",stdout);world_web_space_href(&site,space);fputs("\">View public page</a>",stdout);}
        ui_close(stdout,"div");
        /* look */
        ui_open_a(stdout,"form","method","post","action","/workspace/world","data-world-form","style");world_web_hidden_base(site_id,world_id,space,"style",revision);
        ui_open(stdout,"div","aw-row"); ui_open(stdout,"label",NULL); fputs("What kind of space",stdout); ui_open(stdout,"small",NULL); fputs("Radio, label, podcast, newspaper, poetry, cinema, or your own word",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"input","name","medium","required",(const char *)NULL,"maxlength","32","pattern","[a-z0-9][a-z0-9-]*","list","aw-media",UI_DYN,"value","",(medium)); ui_open_a(stdout,"datalist","id","aw-media"); ui_open_a(stdout,"option","value","radio"); ui_open_a(stdout,"option","value","music"); ui_open_a(stdout,"option","value","podcast"); ui_open_a(stdout,"option","value","news"); ui_open_a(stdout,"option","value","writing"); ui_open_a(stdout,"option","value","poetry"); ui_open_a(stdout,"option","value","film"); ui_open_a(stdout,"option","value","gallery"); ui_open_a(stdout,"option","value","archive"); ui_open_a(stdout,"option","value","mixed"); ui_close(stdout,"datalist"); ui_close(stdout,"label");
        ui_open(stdout,"label",NULL); fputs("Look",stdout); ui_open(stdout,"small",NULL); fputs("How the page is set",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"select","name","character");
        for(size_t i=0;i<BF_WORLD_MATERIAL_RENDERER_COUNT;i++){const char *c=bf_world_material_renderers[i].character,*label=world_web_look(c);if(!label)continue;fputs("<option value=\"",stdout);html(c);fputs("\"",stdout);if(!strcmp(c,character))fputs(" selected",stdout);fputs(">",stdout);html(label);ui_close(stdout,"option");}
        ui_close(stdout,"select"); ui_close(stdout,"label"); ui_close(stdout,"div"); ui_open(stdout,"label",NULL); fputs("Introduction",stdout); ui_open(stdout,"small",NULL); fputs("Shown under the title",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"textarea","name","lede","rows","3","maxlength","600");html(lede);ui_close(stdout,"textarea"); ui_close(stdout,"label");
        ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Save the look",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");
        /* items */
        ui_open(stdout,"h3",NULL); fputs("In this space",stdout); ui_close(stdout,"h3"); ui_open_a(stdout,"ol","class","aw-items","data-world-items",(const char *)NULL);
        int next=1,have=0;
        if(aw_items(db,subject,site_id,world_id,space,&items)==1)while(sqlite3_step(items)==SQLITE_ROW){
            int pos=sqlite3_column_int(items,0);const char *ref=(const char*)sqlite3_column_text(items,1),*role=(const char*)sqlite3_column_text(items,2),*note=(const char*)sqlite3_column_text(items,3);
            if(pos>=next)next=pos+1;have++;
            printf("<li data-world-item=\"%d\"><div><strong>%d. ",pos,pos);world_web_words(role);ui_close(stdout,"strong"); ui_open(stdout,"br",NULL); ui_open(stdout,"code",NULL);html(ref);ui_close(stdout,"code");
            if(note&&*note){ui_open(stdout,"br",NULL);html(note);}ui_close(stdout,"div"); ui_open_a(stdout,"form","method","post","action","/workspace/world");
            world_web_hidden_base(site_id,world_id,space,"unitem",revision);char p[8];snprintf(p,sizeof(p),"%d",pos);ph_hidden("position",p);
            ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Remove",stdout); ui_close(stdout,"button"); ui_close(stdout,"form"); ui_close(stdout,"li");
        }
        sqlite3_finalize(items);if(!have)ui_text(stdout, "li", NULL, "Nothing here yet.");ui_close(stdout,"ol");
        if(next<=AW_ITEM_LIMIT){
            ui_open_a(stdout,"form","method","post","action","/workspace/world","data-world-form","item");world_web_hidden_base(site_id,world_id,space,"item",revision);
            char p[8];snprintf(p,sizeof(p),"%d",next);ph_hidden("position",p);
            ui_open(stdout,"div","aw-row"); ui_open(stdout,"label",NULL); fputs("Piece",stdout); ui_open(stdout,"small",NULL); fputs("The address after /o/ on a piece&rsquo;s page, or a space below",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"input","name","ref","required",(const char *)NULL,"maxlength","160","list","aw-portals"); ui_open_a(stdout,"datalist","id","aw-portals");
            for(int i=0;i<n;i++)if(strcmp(ids[i],space)){ui_open_a(stdout,"option",UI_DYN,"value","space:",(ids[i]));}
            ui_close(stdout,"datalist"); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("Its part here",stdout); ui_open(stdout,"small",NULL); fputs("Track, episode, poem, story, doorway&hellip;",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"input","name","role","required",(const char *)NULL,"maxlength","32","pattern","[a-z0-9][a-z0-9-]*"); ui_close(stdout,"label"); ui_close(stdout,"div");
            ui_open(stdout,"label",NULL); fputs("Your note",stdout); ui_open(stdout,"small",NULL); fputs("Optional. Shown with the piece",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"textarea","name","note","rows","2","maxlength","280"); ui_close(stdout,"textarea"); ui_close(stdout,"label"); ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Add to this space",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form");
        }
        aw_picker_emit(db,site_id,world_id,space,revision,next,who.id,source,q);
        /* release */
        ui_open(stdout,"div","aw-actions");
        if(strcmp(shown,"public")){ui_open_a(stdout,"form","method","post","action","/workspace/world","data-world-form","release");world_web_hidden_base(site_id,world_id,space,"release",revision);
            ui_open_a(stdout,"button","type","submit");fputs(!strcmp(shown,"changed")?"Publish changes":"Publish this space",stdout);ui_close(stdout,"button"); ui_close(stdout,"form");}
        if(!strcmp(state,"public")){ui_open_a(stdout,"form","method","post","action","/workspace/world","data-world-form","withdraw");world_web_hidden_base(site_id,world_id,space,"withdraw",-1);ui_open_a(stdout,"button","type","submit","class","aw-quiet"); fputs("Take it down",stdout); ui_close(stdout,"button"); ui_close(stdout,"form");}
        ui_close(stdout,"div");
        world_cine_studio(db,subject,site_id,world_id,space);
        world_audio_studio(db,subject,site_id,world_id,space,revision,state);
        world_lit_studio(db,subject,site_id,world_id,space,revision);
        /* title and room */
        ui_open_a(stdout,"form","method","post","action","/workspace/world","data-world-form","put");world_web_hidden_base(site_id,world_id,space,"put",revision);
        ph_hidden("template","template:world-character");ph_hidden("layout","layout:sequence");
        fputs("<div class=\"aw-row\"><label>Title<input name=\"title\" required maxlength=\"256\" value=\"",stdout);html(title);fputs("\"></label><label>Shape<select name=\"renderer\"><option value=\"2d\"",stdout);
        if(!strcmp(renderer,"2d"))fputs(" selected",stdout);fputs(">A page</option><option value=\"3d\"",stdout);if(!strcmp(renderer,"3d"))fputs(" selected",stdout);
        fputs(">A three-dimensional room with a readable page</option></select></label></div><div class=\"aw-actions\"><button type=\"submit\" class=\"aw-quiet\">Rename</button></div></form></section>",stdout);
    }
    /* new space */
    if(n<AW_SPACE_LIMIT){
        ui_open(stdout,"section",NULL); ui_open(stdout,"h3",NULL); fputs("New space",stdout); ui_close(stdout,"h3"); ui_open_a(stdout,"form","method","post","action","/workspace/world","data-world-form","new");world_web_hidden_base(site_id,world_id,"","put",0);
        ph_hidden("template","template:world-character");ph_hidden("layout","layout:sequence");
        ui_open(stdout,"div","aw-row"); ui_open(stdout,"label",NULL); fputs("Title",stdout); ui_open_a(stdout,"input","name","title","required",(const char *)NULL,"maxlength","256"); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("Address",stdout); ui_open(stdout,"small",NULL); fputs("Lowercase, used in the link",stdout); ui_close(stdout,"small"); ui_open_a(stdout,"input","name","space","required",(const char *)NULL,"maxlength","64","pattern","[a-z0-9][a-z0-9-]*"); ui_close(stdout,"label"); ui_open(stdout,"label",NULL); fputs("Shape",stdout); ui_open_a(stdout,"select","name","renderer"); ui_open_a(stdout,"option","value","2d"); fputs("A page",stdout); ui_close(stdout,"option"); ui_open_a(stdout,"option","value","3d"); fputs("A three-dimensional room with a readable page",stdout); ui_close(stdout,"option"); ui_close(stdout,"select"); ui_close(stdout,"label"); ui_close(stdout,"div"); ui_open(stdout,"div","aw-actions"); ui_open_a(stdout,"button","type","submit"); fputs("Make the space",stdout); ui_close(stdout,"button"); ui_close(stdout,"div"); ui_close(stdout,"form"); ui_close(stdout,"section");
    }
    ui_close(stdout,"div");ph_end();sqlite3_close(db);return 1;
}

/* ------------------------------------------------------------------ public */
/* Media plays or shows here only under terms its own source states as open:
 * public domain, CC0, CC BY or CC BY-SA. Everything else is a link to where
 * it is published. */
static int world_web_open_terms(const char *r){
    char s[128];size_t n=0;if(!r)return 0;while(*r==' ')r++;for(;*r&&n+1<sizeof(s);r++)s[n++]=(char)tolower((unsigned char)*r);s[n]=0;
    if(!n||strstr(s,"-nc")||strstr(s," nc")||strstr(s,"-nd")||strstr(s," nd")||strstr(s,"by-nc")||strstr(s,"by-nd"))return 0;
    return !strncmp(s,"cc0",3)||!strncmp(s,"public domain",13)||!strncmp(s,"cc by ",6)||!strcmp(s,"cc by")||!strncmp(s,"cc by-sa",8)||
           strstr(s,"creativecommons.org/publicdomain/")!=NULL||strstr(s,"creativecommons.org/licenses/by/")!=NULL||strstr(s,"creativecommons.org/licenses/by-sa/")!=NULL;
}
static int world_web_https(const char *u){return u&&!strncmp(u,"https://",8)&&public_http_url(u)&&!strpbrk(u," \"'<>\\");}
static void world_web_public_head(int code,const char *title,const char *site_name,const char *lede){
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: public, no-cache\r\nVary: Host\r\n"
           "X-Content-Type-Options: nosniff\r\nReferrer-Policy: strict-origin-when-cross-origin\r\nConnection: close\r\n\r\n",code,code==200?"OK":"Gone");
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>",stdout);
    html(title);fputs(" · ",stdout);html(site_name);ui_close(stdout,"title");
    if(lede&&*lede){ui_open_a(stdout,"meta","name","description",UI_DYN,"content","",(lede));}
    fputs("<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Archivo+Black&amp;family=DM+Mono:wght@400;500&amp;family=DM+Sans:opsz,wght@9..40,400;9..40,600&amp;"
          "family=Instrument+Serif:ital@0;1&amp;family=Newsreader:opsz,wght@6..72,400;6..72,600&amp;family=Spline+Sans+Mono:wght@400;600&amp;display=swap\">"
          "<link rel=\"stylesheet\" href=\"/assets/world.css\"></head>",stdout);
}
static void world_web_bar(const PhSite *site,const char *medium,int index){
    ui_open_a(stdout,"nav","class","bar","aria-label","Where you are"); ui_open_a(stdout,"a","class","home","href","/");html(site->name);ui_close(stdout,"a");
    if(medium&&*medium){ui_open(stdout,"span","ch");world_web_words(medium);ui_close(stdout,"span");}
    if(!index)do{ ui_open(stdout,"span","gr"); ui_open_a(stdout,"a","href","/worlds"); fputs("All spaces",stdout); ui_close(stdout,"a"); ui_close(stdout,"span"); }while(0);ui_close(stdout,"nav");
}
static const char *world_web_kind(int kind){
    return kind==BF_PAGE_MATERIAL_AUDIO?"audio":kind==BF_PAGE_MATERIAL_VIDEO?"video":kind==BF_PAGE_MATERIAL_PHOTO?"photo":
           kind==BF_PAGE_MATERIAL_EVENT?"event":kind==BF_PAGE_MATERIAL_PLACE?"place":"text";
}
static void world_web_piece(const PageItem *it,int position,const char *ref,const char *role,const char *note){
    int open=world_web_open_terms(it->rights),kind=it->kind;
    const char *media=kind==BF_PAGE_MATERIAL_AUDIO||kind==BF_PAGE_MATERIAL_VIDEO?it->asset:kind==BF_PAGE_MATERIAL_PHOTO?it->image:"";
    int shown=open&&world_web_https(media);
    printf("<article class=\"aw-piece\" id=\"piece-%d\" data-kind=\"%s\" data-world-piece=\"",position,world_web_kind(kind));html(ref);fputs("\" data-shown=\"",stdout);fputs(shown?"media":"link",stdout);fputs("\">",stdout);
    if(shown&&kind==BF_PAGE_MATERIAL_PHOTO){ui_open(stdout,"figure",NULL); ui_open_a(stdout,"img","loading","lazy","decoding","async",UI_DYN,"src","",(media),UI_DYN,"alt","",(it->title)); ui_close(stdout,"figure");}
    else if(shown&&kind==BF_PAGE_MATERIAL_VIDEO){fputs("<figure><video controls preload=\"none\" playsinline",stdout);if(world_web_https(it->image)&&page_image_url_safe(it->image)){fputs(" poster=\"",stdout);html(it->image);fputs("\"",stdout);}fputs(" src=\"",stdout);html(media);fputs("\"><a href=\"",stdout);html(media);fputs("\">Open the film file</a></video></figure>",stdout);}
    ui_open(stdout,"div","aw-words"); ui_open(stdout,"p","aw-role");world_web_words(role);ui_close(stdout,"p"); ui_open(stdout,"h2",NULL); ui_open_a(stdout,"a",UI_DYN,"href","/o/",(ref));html(it->title);ui_close(stdout,"a"); ui_close(stdout,"h2");
    if(it->by[0]){ui_text(stdout, "p", "by", it->by);}
    if(shown&&kind==BF_PAGE_MATERIAL_AUDIO){ui_open_a(stdout,"audio","controls",(const char *)NULL,"preload","none",UI_DYN,"src","",(media)); ui_close(stdout,"audio");}
    if(note&&*note){ui_text(stdout, "p", "aw-note", note);}
    if(it->body[0]){ui_text(stdout, "p", "aw-body", it->body);}
    ui_open(stdout,"p","aw-terms");
    if(it->rights[0]){if(world_web_https(it->rights_href)){ui_open_a(stdout,"a","rel","license noopener",UI_DYN,"href","",(it->rights_href));html(it->rights);ui_close(stdout,"a");}else html(it->rights);fputs(" · ",stdout);}
    else if(media&&*media)fputs("No open terms stated, so it is linked rather than shown · ",stdout);
    if(public_http_url(it->href)){ui_open_a(stdout,"a","rel","noopener",UI_DYN,"href","",(it->href)); fputs("Where it is published",stdout); ui_close(stdout,"a");}else{fputs("From ",stdout);html(it->source);}
    if(world_web_https(it->transcript_href)){fputs(" · <a rel=\"noopener\" href=\"",stdout);html(it->transcript_href);fputs("\">Transcript</a>",stdout);}
    ui_close(stdout,"p"); ui_close(stdout,"div"); ui_close(stdout,"article");
}
static int world_web_resolve(sqlite3 *db,const char *ref,PageMaterial *material,char loaded[96],PageItem *out){
    const char *slash=strchr(ref,'/');char source[96];const PageItem *found=NULL;
    if(!slash||(size_t)(slash-ref)>=sizeof(source))return 0;memcpy(source,ref,(size_t)(slash-ref));source[slash-ref]=0;
    /* one read per source, however many of its pieces the space names */
    if(strcmp(loaded,source)){for(int k=0;k<BF_PAGE_MATERIAL_COUNT;k++)material->stream[k].count=material->stream[k].cursor=0;page_collect_stories(db,source,material);snprintf(loaded,96,"%s",source);}
    const PageStream *mix=&material->stream[BF_PAGE_MATERIAL_MIX];
    for(int k=0;k<mix->count;k++)if(!strcmp(mix->item[k].key,slash+1)&&!strcmp(mix->item[k].source,source)){found=&mix->item[k];break;}
    if(!found)return 0;*out=*found;return 1;
}
/* index: the /worlds address itself; otherwise target is under /worlds/. */
static int serve_world_public(const char *method,const char *target,const char *host,int index){
    const char *space=index?"":target+8;
    if(!index&&(!*space||strlen(space)>64||strspn(space,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(space)))return 0;
    char normalized[256];if(!lower_host(host,normalized)||host_is_territory_apex(host))return 0;
    sqlite3 *db=cms_open_ro();PhSite site;
    if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}
    if(strcmp(method,"GET")&&strcmp(method,"HEAD")){sqlite3_close(db);aw_error(405,"Open spaces with GET.\n");return 1;}
    sqlite3_stmt *row=NULL;
    if(index){
        if(aw_public_spaces(db,site.id,&row)!=1){sqlite3_finalize(row);sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","No spaces have been published on this site.\n");return 1;}
        int count=0;
        while(sqlite3_step(row)==SQLITE_ROW){
            if(!count++){world_web_public_head(200,"Spaces",site.name,"");
                ui_open_a(stdout,"body","class","shelf aw-shelf","data-world-spaces",(const char *)NULL); ui_open(stdout,"main","wrap");world_web_bar(&site,"",1);
                ui_open(stdout,"header","hero"); ui_open(stdout,"h1",NULL); fputs("Spaces",stdout); ui_close(stdout,"h1"); ui_open(stdout,"p","lede"); fputs("What ",stdout);html(site.name);fputs(" keeps on this site. Each one opens in its own look.</p></header><div class=\"rack\">",stdout);}
            const char *id=(const char*)sqlite3_column_text(row,0),*title=(const char*)sqlite3_column_text(row,1),*medium=(const char*)sqlite3_column_text(row,2),*character=(const char*)sqlite3_column_text(row,3);
            ui_open_a(stdout,"a",UI_DYN,"class","cover c-",(world_web_look(character)?character:"journal"),UI_DYN,"href","/worlds/",(id)); ui_open(stdout,"div","t");html(title);ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_open(stdout,"div","m");world_web_words(medium);ui_close(stdout,"div"); ui_close(stdout,"div"); ui_close(stdout,"a");
        }
        sqlite3_finalize(row);sqlite3_close(db);
        if(!count){protocol_reply(404,"text/plain; charset=utf-8","No spaces have been published on this site.\n");return 1;}
        ui_close(stdout,"div"); ui_close(stdout,"main"); ui_close(stdout,"body"); ui_close(stdout,"html");return 1;
    }
    if(aw_public_space(db,site.id,space,&row)!=1){sqlite3_close(db);protocol_reply(404,"text/plain; charset=utf-8","There is no space at this address.\n");return 1;}
    char title[257],renderer[8],medium[33],character[33],lede[601],state[16];sqlite3_int64 world_id=sqlite3_column_int64(row,6);
    char *dst[]={title,renderer,medium,character,lede,state};size_t cap[]={sizeof(title),sizeof(renderer),sizeof(medium),sizeof(character),sizeof(lede),sizeof(state)};
    for(int i=0;i<6;i++){const char *s=(const char*)sqlite3_column_text(row,i);snprintf(dst[i],cap[i],"%s",s?s:"");}
    sqlite3_finalize(row);row=NULL;
    if(strcmp(state,"public")){sqlite3_close(db);protocol_reply(410,"text/plain; charset=utf-8","Its owner has taken this space down.\n");return 1;}
    if(!world_web_look(character))snprintf(character,sizeof(character),"journal");
    typedef struct{int position;char ref[161],role[33],note[281];} WorldRow;
    WorldRow rows[AW_ITEM_LIMIT];int n=0;
    if(aw_public_items(db,site.id,world_id,space,&row)==1)while(n<AW_ITEM_LIMIT&&sqlite3_step(row)==SQLITE_ROW){
        rows[n].position=sqlite3_column_int(row,0);const char *r=(const char*)sqlite3_column_text(row,1),*ro=(const char*)sqlite3_column_text(row,2),*no=(const char*)sqlite3_column_text(row,3);
        snprintf(rows[n].ref,sizeof(rows[n].ref),"%s",r?r:"");snprintf(rows[n].role,sizeof(rows[n].role),"%s",ro?ro:"");snprintf(rows[n].note,sizeof(rows[n].note),"%s",no?no:"");n++;
    }
    sqlite3_finalize(row);row=NULL;
    world_web_public_head(200,title,site.name,lede);
    fputs("<body class=\"",stdout);html(character);fputs(" aw-space\" data-world-look=\"",stdout);html(character);fputs("\" data-medium=\"",stdout);html(medium);fputs("\" data-renderer=\"",stdout);html(renderer);
    fputs("\"><main class=\"wrap\" data-world-space=\"",stdout);html(space);fputs("\">",stdout);world_web_bar(&site,medium,0);
    ui_open(stdout,"header","hero"); ui_open(stdout,"h1",NULL);html(title);ui_close(stdout,"h1");if(*lede){ui_text(stdout, "p", "lede", lede);}ui_close(stdout,"header");
    if(!strcmp(renderer,"3d"))world_room_begin();
    world_audio_station(db,&site,world_id,space);
    ui_open_a(stdout,"section","id","world-room-readable","class","aw-pieces","aria-label","In this space");
    PageMaterial *material=calloc(1,sizeof(*material));PageItem *item=calloc(1,sizeof(*item));char loaded[96]="";
    for(int i=0;i<n;i++){
        if(!strncmp(rows[i].ref,"space:",6)){
            sqlite3_stmt *p=NULL;
            if(aw_public_space(db,site.id,rows[i].ref+6,&p)==1&&!strcmp((const char*)sqlite3_column_text(p,5),"public")){
                printf("<article class=\"aw-piece aw-portal\" id=\"piece-%d\" data-kind=\"portal\"><div class=\"aw-words\"><p class=\"aw-role\">",rows[i].position);world_web_words(rows[i].role);
                ui_close(stdout,"p"); ui_open(stdout,"h2",NULL); ui_open_a(stdout,"a",UI_DYN,"href","/worlds/",(rows[i].ref+6));html((const char*)sqlite3_column_text(p,0));ui_close(stdout,"a"); ui_close(stdout,"h2");
                const char *pl=(const char*)sqlite3_column_text(p,4);if(rows[i].note[0]){ui_text(stdout, "p", "aw-note", rows[i].note);}else if(pl&&*pl){ui_text(stdout, "p", "aw-body", pl);}
                ui_close(stdout,"div"); ui_close(stdout,"article");
            }
            sqlite3_finalize(p);continue;
        }
        if(!strncmp(rows[i].ref,"works/",6)){world_lit_piece(db,&site,world_id,rows[i].position,rows[i].ref,rows[i].role,rows[i].note);continue;}
        if(material&&item&&world_web_resolve(db,rows[i].ref,material,loaded,item)){world_web_piece(item,rows[i].position,rows[i].ref,rows[i].role,rows[i].note);world_audio_episode(db,&site,world_id,space,rows[i].position,rows[i].ref,item);}
        else printf("<article class=\"aw-piece aw-gone\" id=\"piece-%d\" data-kind=\"gone\"><div class=\"aw-words\"><p class=\"aw-body\">This piece is no longer available from where it was published.</p></div></article>",rows[i].position);
    }
    free(material);free(item);ui_close(stdout,"section");
    if(!strcmp(renderer,"3d"))world_room_end();
    world_lit_public(db,&site,world_id,space);
    world_cine_public(db,&site,world_id,space);
    world_audio_script();
    if(aw_public_spaces(db,site.id,&row)==1){
        int shown=0;
        while(sqlite3_step(row)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(row,0);if(!id||!strcmp(id,space))continue;
            if(!shown++)do{ ui_open_a(stdout,"nav","class","aw-elsewhere","aria-label","Other spaces here"); ui_open(stdout,"h2","region-label"); fputs("Also here",stdout); ui_close(stdout,"h2"); ui_open(stdout,"ul",NULL); }while(0);
            ui_open(stdout,"li",NULL); ui_open_a(stdout,"a",UI_DYN,"href","/worlds/",(id));html((const char*)sqlite3_column_text(row,1));ui_close(stdout,"a"); fputs(" ",stdout); ui_open(stdout,"span",NULL);world_web_words((const char*)sqlite3_column_text(row,2));ui_close(stdout,"span"); ui_close(stdout,"li");}
        if(shown)do{ ui_close(stdout,"ul"); ui_close(stdout,"nav"); }while(0);
    }
    sqlite3_finalize(row);sqlite3_close(db);
    ui_text(stdout,"footer","src","Every piece here is shown from where it is published, under the terms its source states."); ui_close(stdout,"main"); ui_close(stdout,"body"); ui_close(stdout,"html");
    return 1;
}

static int serve_account_world_workspace(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *body=r->body; const char *raw=r->raw; const char *host=r->host; const char *bearer=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;
    int world_page=bf_request_is(r,"world_page");
    if(bf_request_is(r,"world"))return world_web_api(method,query,body,raw,host,bearer,auth_db);
    if(bf_request_is(r,"literature")&&world_lit_api(method,query,body,raw,host,bearer,auth_db))return 1;
    if(bf_request_is(r,"reading")&&serve_world_reading(method,query,bearer,auth_db))return 1;
    if(world_page&&strstr(target+8,"/read/")&&serve_world_lit_public(method,target,query,host))return 1;
    if(bf_request_is(r,"film"))return world_cine_api(method,query,body,raw,host,bearer,auth_db);
    if(bf_request_is(r,"club"))return serve_world_club(method,query,bearer,auth_db);
    if(bf_request_is(r,"studio"))return serve_world_studio(method,query,bearer,auth_db);
    if(bf_request_is(r,"audio"))return world_audio_api(method,query,body,raw,host,bearer,auth_db);
    if(world_page&&strchr(target+8,'/')&&serve_world_audio_public(method,target,host))return 1;
    if(world_page||bf_request_is(r,"worlds"))return serve_world_public(method,target,host,!world_page||!target[8]);
    return 0;
}
#endif
