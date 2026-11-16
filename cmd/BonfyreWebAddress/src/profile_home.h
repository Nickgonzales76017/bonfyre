#ifndef BONFYRE_PROFILE_HOME_H
#define BONFYRE_PROFILE_HOME_H
#include "ui_tree.h"
#include "../generated/native_estate_hosts.h"
#include "web_store.h"
#include "../../BonfyreCMS/src/account_world.h"
#include "site_authored_document.h"
#include "affinity_projection.h"
#include "route_presentation.h"
#include "form_tree.h"
#include "bf_matrix.h"
#include "profile_journal_batch.h"
/* One profile presentation. Auth owns the reader; Matrix owns the address;
 * CMS owns site-specific pages/posts and the independent private saved library. */
typedef struct {char id[128],host[256],actor[192],territory[24],name[161],bio[6001];long about_id;} PhSite;
#include "profile_publication_placement.h"
typedef struct {const char *key;char *value;size_t cap;int multiline;} PhField;
static int ph_claim_pending(sqlite3 *db,const char *actor);
static int ph_decode(const char *restrict p,size_t len,char *restrict out,size_t cap,int multiline){size_t n=0;for(size_t i=0;i<len;i++){unsigned char c=p[i];if(c=='%'){if(i+2>=len||psetup_hex(p[i+1])<0||psetup_hex(p[i+2])<0)return 0;c=(unsigned char)(psetup_hex(p[i+1])*16+psetup_hex(p[i+2]));i+=2;}else if(c=='+')c=' ';if(!c||c==127||(c<32&&(!multiline||(c!='\n'&&c!='\r'&&c!='\t')))||n+1>=cap)return 0;out[n++]=(char)c;}out[n]=0;return 1;}
static int ph_fields(const char *input,PhField *fields,size_t count,size_t limit){unsigned seen=0;if(!input||!*input)return 1;if(strlen(input)>limit)return 0;for(const char *p=input;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[32];if(!eq||!ph_decode(p,(size_t)(eq-p),key,sizeof(key),0))return 0;size_t i;for(i=0;i<count&&strcmp(key,fields[i].key);i++){}if(i==count||(seen&(1u<<i))||!ph_decode(eq+1,(size_t)(end-eq-1),fields[i].value,fields[i].cap,fields[i].multiline))return 0;seen|=1u<<i;p=*end?end+1:end;if(!*p&&*end)return 0;}return 1;}
static int ph_header(const char *raw,const char *name,char *out,size_t cap){int found=0;size_t key=strlen(name);for(const char *p=raw?strstr(raw,"\n"):NULL;p&&*++p&&*p!='\r'&&*p!='\n';p=strchr(p,'\n')){if(!strncasecmp(p,name,key)&&p[key]==':'){if(found++)return 0;const char *v=p+key+1;while(*v==' '||*v=='\t')v++;const char *end=strchr(v,'\r');if(!end)end=strchr(v,'\n');if(!end||(size_t)(end-v)>=cap)return 0;memcpy(out,v,(size_t)(end-v));out[end-v]=0;}}return found==1;}
static int ph_origin(const char *raw,const char *host){char origin[400],expected[400],type[100];if(!ph_header(raw,"Origin",origin,sizeof(origin))||!ph_header(raw,"Content-Type",type,sizeof(type))||strcmp(type,"application/x-www-form-urlencoded"))return 0;snprintf(expected,sizeof(expected),"%s://%s",host&&(!strncmp(host,"127.0.0.1",9)||!strncmp(host,"localhost",9))?"http":"https",host?host:"");return !strcmp(origin,expected);}
static void ph_error(int code,const char *message){protocol_reply(code,"text/plain; charset=utf-8",message);}
static int ph_actor(long uid,const char *auth_db,char out[192]){sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int count=0;out[0]=0;if(uid<1||!auth_db||bf_store_open(auth_db, &db, SQLITE_OPEN_READONLY, &bf_store_policy_auth)!=SQLITE_OK)goto done;if(sqlite3_prepare_v2(db,"SELECT actor_subject FROM actor_closure_bindings WHERE user_id=?1 AND state='active' LIMIT 2",-1,&st,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(st,1,uid);while(sqlite3_step(st)==SQLITE_ROW){const char *s=(const char*)sqlite3_column_text(st,0);if(++count!=1||!s||strncmp(s,"person:",7)||strlen(s)>=192||strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:._-")!=strlen(s)){count=0;break;}snprintf(out,192,"%s",s);}done:sqlite3_finalize(st);if(db)sqlite3_close(db);if(count!=1)out[0]=0;return count==1;}
static int ph_matrix(const PhSite *site){const char *path=getenv("BONFYRE_MATRIX_DB");sqlite3 *db=NULL;sqlite3_stmt *st=NULL;char institution[224],normalized[256];int ok=0;if(!path||!lower_host(site->host,normalized)||strcmp(site->host,normalized))return 0;snprintf(institution,sizeof(institution),"inst:%s",site->actor);for(size_t i=5;institution[i];i++)if(institution[i]==':'||institution[i]=='.')institution[i]='-';if(bf_store_open(path, &db, SQLITE_OPEN_READONLY, &bf_store_policy_matrix)!=SQLITE_OK)goto done;
 if(sqlite3_prepare_v2(db,"SELECT 1 FROM matrix_domains d WHERE d.hostname=?1 AND d.institution_id=?2 AND d.territory=?3 AND d.default_scope='public' AND d.default_world='LIVE' AND d.default_representation='HTML' AND EXISTS(SELECT 1 FROM matrix_presences p WHERE p.subject_id=?4 AND p.territory=d.territory AND p.scope='public' AND p.world='LIVE' AND p.representation='HTML' AND p.active=1)",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(st,1,site->host,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,institution,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,site->territory,-1,SQLITE_STATIC);sqlite3_bind_text(st,4,site->actor,-1,SQLITE_STATIC);ok=sqlite3_step(st)==SQLITE_ROW;done:sqlite3_finalize(st);if(db)sqlite3_close(db);return ok;}
static int ph_site(sqlite3 *db,const char *host,const char *id,PhSite *site){sqlite3_stmt *st=NULL;int count=0;memset(site,0,sizeof(*site));if(!db||sqlite3_prepare_v2(db,"SELECT site_id,host,subject_address,territory FROM site_presentation WHERE namespace='root' AND status='published' AND profile='member-profile-v1' AND ((?1<>'' AND host=?1) OR (?2<>'' AND site_id=?2)) LIMIT 2",-1,&st,NULL)!=SQLITE_OK)return 0;sqlite3_bind_text(st,1,host?host:"",-1,SQLITE_STATIC);sqlite3_bind_text(st,2,id?id:"",-1,SQLITE_STATIC);while(sqlite3_step(st)==SQLITE_ROW){if(++count!=1)break;char *out[]={site->id,site->host,site->actor,site->territory};size_t cap[]={sizeof(site->id),sizeof(site->host),sizeof(site->actor),sizeof(site->territory)};for(int i=0;i<4;i++){const char *s=(const char*)sqlite3_column_text(st,i);if(!s||strlen(s)>=cap[i]){count=0;break;}snprintf(out[i],cap[i],"%s",s);}if(!count)break;}sqlite3_finalize(st);
 if(count!=1||strncmp(site->actor,"person:",7)||!studio_safe_slug_arg(site->id)||!ph_matrix(site))return 0;
 const char *handle=site->actor+7;snprintf(site->name,sizeof(site->name),"%s",handle);if(site->name[0]>='a'&&site->name[0]<='z')site->name[0]-='a'-'A';
 if(sqlite3_prepare_v2(db,"SELECT id,heading,COALESCE(lede,'') FROM site_page WHERE namespace=?1 AND route='/' AND status='published' LIMIT 2",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW){site->about_id=sqlite3_column_int64(st,0);const char *name=(const char*)sqlite3_column_text(st,1),*bio=(const char*)sqlite3_column_text(st,2);if(!name||strlen(name)>=sizeof(site->name)||!bio||strlen(bio)>=sizeof(site->bio)){sqlite3_finalize(st);return 0;}snprintf(site->name,sizeof(site->name),"%s",name);snprintf(site->bio,sizeof(site->bio),"%s",bio);if(sqlite3_step(st)==SQLITE_ROW){sqlite3_finalize(st);return 0;}}sqlite3_finalize(st);}return 1;}
static int ph_sites(sqlite3 *db,const char *actor,PhSite sites[16]){sqlite3_stmt *st=NULL;char ids[17][128];int n=0,valid=0;if(!actor||!*actor||!db||sqlite3_prepare_v2(db,"SELECT site_id FROM site_presentation WHERE namespace='root' AND status='published' AND profile='member-profile-v1' AND subject_address=?1 ORDER BY territory,host LIMIT 17",-1,&st,NULL)!=SQLITE_OK)return 0;sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);while(n<17&&sqlite3_step(st)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(st,0);if(!id||strlen(id)>=128){sqlite3_finalize(st);return 0;}snprintf(ids[n++],128,"%s",id);}sqlite3_finalize(st);if(n>16)return 0;for(int i=0;i<n;i++){PhSite s;if(ph_site(db,NULL,ids[i],&s)&&!strcmp(s.actor,actor))sites[valid++]=s;}return valid;}
static void ph_hash(const PhSite *site,char out[65]){char value[6600];int n=snprintf(value,sizeof(value),"%s\n%ld\n%zu:%s%zu:%s",site->id,site->about_id,strlen(site->name),site->name,strlen(site->bio),site->bio);if(n<0||n>=(int)sizeof(value)){out[0]=0;return;}bf_sha256_hex((const unsigned char*)value,(size_t)n,out);}
static void ph_hidden(const char *key,const char *value){fputs("<input type=\"hidden\" name=\"",stdout);html(key);fputs("\" value=\"",stdout);html(value?value:"");fputs("\">",stdout);}
static void ph_url(const PhSite *site,const char *tab,int private_view){if(!private_view&&site&&!site->actor[0]){fputs("#",stdout);url_query_value(tab);return;}if(private_view){fputs("/profile?site=",stdout);url_query_value(site?site->id:"");fputs("&amp;tab=",stdout);}else fputs("/?tab=",stdout);url_query_value(tab);}
static const char *ph_label(const char *t){return !strcmp(t,"work")?"Work":!strcmp(t,"life")?"Life":!strcmp(t,"live")?"Live":!strcmp(t,"shop")?"Shop":"Profile";}
/* The profile's look lives in /assets/aurekai-native.css under [data-profile-page];
 * these helpers only state the real counts the hero reports. */
typedef struct {int articles,subjects,sites;char latest[16];} PhStats;
static void ph_stats(sqlite3 *db,const PhSite *site,PhStats *s){memset(s,0,sizeof(*s));sqlite3_stmt *st=NULL;if(!db||!site||!site->actor[0])return;
 if(sqlite3_prepare_v2(db,"SELECT count(*),count(DISTINCT NULLIF(COALESCE(p.eyebrow,''),'')),COALESCE(max(p.editorial_published_on),''),(SELECT count(*) FROM site_presentation x WHERE x.namespace='root' AND x.status='published' AND x.profile='member-profile-v1' AND x.subject_address=?1) FROM site_page p JOIN site_presentation s ON s.site_id=p.namespace AND s.namespace='root' AND s.status='published' AND s.profile='member-profile-v1' AND s.subject_address=?1 WHERE p.status='published' AND p.route LIKE '/blog/_%'",-1,&st,NULL)!=SQLITE_OK)return;
 sqlite3_bind_text(st,1,site->actor,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW){s->articles=sqlite3_column_int(st,0);s->subjects=sqlite3_column_int(st,1);const char *d=(const char*)sqlite3_column_text(st,2);if(d&&strlen(d)==10)snprintf(s->latest,sizeof(s->latest),"%s",d);s->sites=sqlite3_column_int(st,3);}sqlite3_finalize(st);}
static void ph_short_date(const char *iso){static const char *m[]={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};int y,mo,d;if(!iso||sscanf(iso,"%4d-%2d-%2d",&y,&mo,&d)!=3||mo<1||mo>12){fputs("—",stdout);return;}printf("%s %d, %d",m[mo-1],d,y);}
static const char *ph_territory_line(const char *t){return !strcmp(t,"work")?"Jobs, tools and business writing":!strcmp(t,"life")?"Stories, places and everyday life":!strcmp(t,"live")?"What is on, to watch and hear":!strcmp(t,"shop")?"Things made and offered":"Public profile";}
static void ph_address(const PhSite *site,int carry){
 if(carry&&account_request_territory[0]&&strcmp(site->territory,account_request_territory)&&(!strcmp(account_request_territory,"work")||!strcmp(account_request_territory,"life")||!strcmp(account_request_territory,"live")||!strcmp(account_request_territory,"shop"))){char next[300];snprintf(next,sizeof(next),"https://%s/",site->host);fputs("https://aurekai.",stdout);html(account_request_territory);fputs("/session/switch?to=",stdout);url_query_value(site->territory);fputs("&amp;next=",stdout);url_query_value(next);}else{fputs("https://",stdout);html(site->host);fputc('/',stdout);}
}
#include "profile_cover_projection.h"
#include "profile_journal_styles.h"
static void ph_begin(sqlite3 *db,const PhSite *site,const char *name,int private_view,const char *tab,const char *actor){char pattern[65]={0},tmpl[65]={0};bf_presentation_resolve(NULL,NULL,NULL,site?site->id:NULL,NULL,actor,pattern,tmpl);fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>",stdout);html(name);fputs(" · Aurekai</title>",stdout);if(private_view){ui_start(stdout,"meta");ui_attr(stdout,"name","robots");ui_attr(stdout,"content","noindex,nofollow");ui_gt(stdout);}ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","https://fonts.googleapis.com/css2?family=Literata:opsz,wght@7..72,400;7..72,500&amp;display=swap");ui_gt(stdout);ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","/assets/aurekai-native.css?v=4");ui_gt(stdout);ui_start(stdout,"link");ui_attr(stdout,"rel","stylesheet");ui_attr(stdout,"href","/assets/authoring-reader.css");ui_gt(stdout);ph_journal_styles();if(tmpl[0]&&db)bf_site_presentation_emit_scoped(stdout,db,tmpl,"body[data-bf-template]");ui_close(stdout,"head");ui_start(stdout,"body");ui_attr(stdout,"class","ak-app ph-app");if(tmpl[0]){fputs(" data-bf-template=\"",stdout);html(tmpl);fputs("\"",stdout);}fputs(">" BF_ESTATE_BAR "<main class=\"ph-shell\" data-profile-page data-profile-template=\"",stdout);fputs(site&&!site->actor[0]?"organization-subprofile-v1":"member-v1",stdout);fputs("\" data-profile-territory=\"",stdout);html(site?site->territory:"");fputs("\" data-profile-site=\"",stdout);html(site?site->id:"");fputs("\" data-profile-view=\"",stdout);html(private_view?"self":"public");fputs("\">",stdout);
 PhStats stats;ph_stats(db,site,&stats);
 ui_open(stdout,"section","ph-hero");ph_cover_emit(site);ui_open(stdout,"div","ph-hero-in");ui_open(stdout,"div","ph-hero-main");ui_open(stdout,"p","ph-status");ui_open(stdout,"span","ph-live");fputs(private_view?"Your profile":site&&!site->actor[0]?"Credited profile":"Public profile",stdout);ui_close(stdout,"span");if(site&&site->actor[0]){ui_open(stdout,"span",NULL);ui_copy(stdout,"Address ");ui_open(stdout,"code",NULL);html(site->actor);ui_close(stdout,"code");ui_close(stdout,"span");}if(site){ui_open(stdout,"span",NULL);html(ph_label(site->territory));fputs(" · ",stdout);html(ph_territory_line(site->territory));ui_close(stdout,"span");}ui_close(stdout,"p");ui_open(stdout,"div","ph-title");ui_open(stdout,"h1",NULL);html(name);ui_close(stdout,"h1");if(site&&site->host[0]&&site->actor[0]){ui_start(stdout,"a");ui_attr(stdout,"class","ph-handle");ui_attr_open(stdout,"href");ph_address(site,0);fputs("\">@",stdout);html(site->host);ui_close(stdout,"a");}ui_close(stdout,"div");
 if(site&&site->bio[0]){ui_text(stdout, "p", "ph-bio", site->bio);}else if(private_view)ui_text(stdout, "p", "ph-bio", "Choose a site to manage what it publishes.");
 if(site&&site->actor[0]){ui_open(stdout,"dl","ph-stats");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Published articles");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%d",stats.articles);ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Subjects");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%d",stats.subjects);ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Sites");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%d",stats.sites);ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Latest");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);ph_short_date(stats.latest);ui_close(stdout,"dd");ui_close(stdout,"div");ui_close(stdout,"dl");}
 ui_close(stdout,"div");ui_start(stdout,"nav");ui_attr(stdout,"class","ph-actions");ui_attr(stdout,"aria-label","Profile actions");ui_gt(stdout);
 if(private_view&&site){ui_start(stdout,"a");ui_attr(stdout,"class","ph-primary");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/compose/advanced?site=");url_query_value(site->id);fputs("&amp;new=article\">Write an article</a><a href=\"",stdout);ph_url(site,"posts",1);fputs("\">Review drafts</a><a href=\"/workspace/worlds?site=",stdout);url_query_value(site->id);fputs("\">World studio</a><a href=\"",stdout);ph_url(site,"edit",1);fputs("\">Edit this profile</a><a data-profile-preview href=\"",stdout);ph_address(site,1);fputs("\">View public site</a>",stdout);ph_cover_remove_form(site);}else if(private_view&&ph_claim_pending(db,actor)){ui_start(stdout,"a");ui_attr(stdout,"class","ph-primary");ui_attr(stdout,"href","/profile?tab=sites");ui_gt(stdout);ui_copy(stdout,"Your address is waiting for review");ui_close(stdout,"a");}else if(private_view){ui_start(stdout,"a");ui_attr(stdout,"class","ph-primary");ui_attr(stdout,"href","/profile?tab=sites");ui_gt(stdout);ui_copy(stdout,"Set up your profile");ui_close(stdout,"a");}
 else if(site&&site->actor[0]){ui_start(stdout,"a");ui_attr(stdout,"class","ph-primary");ui_attr_open(stdout,"href");ph_url(site,"posts",0);fputs("\">Read everything published</a><a href=\"/companions\">Find companions</a><a href=\"",stdout);ph_url(site,"about",0);fputs("\">About ",stdout);html(name);ui_close(stdout,"a");}
 ui_close(stdout,"nav");ui_close(stdout,"div");ui_close(stdout,"section");(void)tab;}
static void ph_tab_icon(const char *tab){
    const char *glyph=!strcmp(tab,"posts")?BF_ICON_FILE_TEXT:!strcmp(tab,"saved")?BF_ICON_BOOKMARK:!strcmp(tab,"about")?BF_ICON_USER:!strcmp(tab,"sites")?BF_ICON_WORLD:!strcmp(tab,"organizations")?BF_ICON_BRIEFCASE:!strcmp(tab,"communities")?BF_ICON_GROUP:!strcmp(tab,"activity")?BF_ICON_ACTIVITY:!strcmp(tab,"gallery")?BF_ICON_PHOTO:!strcmp(tab,"connections")?BF_ICON_GROUP:ak_app_tab(tab)?ak_app_tab(tab)->icon:BF_ICON_BOARD;
    ui_start(stdout,"span");ui_attr(stdout,"class","ph-tab-icon");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);fputs(glyph,stdout);ui_close(stdout,"span");
}
static void ph_nav(PhSite *sites,int n,const PhSite *site,const char *tab,int private_view){ui_start(stdout,"nav");ui_attr(stdout,"class","ph-tabs");ui_attr(stdout,"aria-label","Profile sections");ui_gt(stdout);const char *keys[]={"overview","gallery","posts","saved","about","sites","communities","activity","organizations","dating","connections"},*labels[]={"Overview","Gallery","Posts","Saved","About","My sites","Communities","My activity","Organizations","Dating","Connect"};for(int i=0;i<11;i++){if((private_view&&i==8)||(!private_view&&(i==3||i==5||i==7||i==9||i==10)))continue;ui_start(stdout,"a");ui_attr_open(stdout,"href");ph_url(site,keys[i],private_view);fputs("\"",stdout);if(!strcmp(tab,keys[i]))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);ph_tab_icon(keys[i]);html(labels[i]);ui_close(stdout,"a");}fputs("<a href=\"/companions\"><span class=\"ph-tab-icon\" aria-hidden=\"true\">" BF_ICON_HEART "</span>Companions</a></nav><div class=\"ph-layout\"><aside class=\"ph-rail\"><nav class=\"ph-sites\" aria-label=\"",stdout);fputs(private_view?"Your profile sites":"Published profile sites",stdout);fputs("\"><p class=\"ph-eyebrow\">",stdout);fputs(private_view?"Your sites":"Also on Aurekai",stdout);ui_close(stdout,"p");ui_open(stdout,"h2",NULL);fputs(private_view?"Where you publish":"Their sites",stdout);ui_close(stdout,"h2");for(int i=0;i<n;i++){ui_start(stdout,"a");ui_attr_open(stdout,"href");if(private_view)ph_url(&sites[i],"overview",1);else if(!sites[i].actor[0]&&!strncmp(sites[i].id,"credited-",9))fputs("#overview",stdout);else{fputs("https://",stdout);html(sites[i].host);fputc('/',stdout);}fputs("\"",stdout);if(site&&!strcmp(site->id,sites[i].id))fputs(" aria-current=\"page\"",stdout);fputs("><span class=\"ph-site-icon\" aria-hidden=\"true\">",stdout);fputs(!strcmp(sites[i].territory,"life")?BF_ICON_HEART:!strcmp(sites[i].territory,"live")?BF_ICON_PLAY:!strcmp(sites[i].territory,"shop")?BF_ICON_SHOP:BF_ICON_BRIEFCASE,stdout);ui_close(stdout,"span");ui_open(stdout,"span","ph-site-copy");ui_open(stdout,"strong",NULL);html(ph_label(sites[i].territory));ui_close(stdout,"strong");ui_open(stdout,"code",NULL);html(sites[i].host);ui_close(stdout,"code");ui_open(stdout,"small",NULL);html(ph_territory_line(sites[i].territory));ui_close(stdout,"small");ui_close(stdout,"span");if(site&&!strcmp(site->id,sites[i].id))ui_text(stdout, "em", NULL, "Here");ui_close(stdout,"a");}if(private_view){fputs("<a href=\"/account\"><span class=\"ph-site-icon\" aria-hidden=\"true\">" BF_ICON_USER "</span><span class=\"ph-site-copy\"><strong>Account settings</strong><small>Connections and privacy</small></span></a>",stdout);}ui_close(stdout,"nav");ui_close(stdout,"aside");ui_open(stdout,"section","ph-panel");}
static void ph_end(void){fputs("</section></div></main>" BF_ESTATE_FOOT "</body></html>",stdout);}
#include "profile_public_collections.h"
/* Public spaces are a released AccountWorld projection on this exact profile site.
 * The profile never copies their material or exposes unreleased draft records. */
static void ph_worlds_teaser(sqlite3 *db,const PhSite *site){
    if(!db||!site)return;
    sqlite3_stmt *rows=NULL;
    if(aw_public_spaces(db,site->id,&rows)!=1){sqlite3_finalize(rows);return;}
    int shown=0;
    while(shown<6&&sqlite3_step(rows)==SQLITE_ROW){
        const char *id=(const char*)sqlite3_column_text(rows,0);
        const char *title=(const char*)sqlite3_column_text(rows,1);
        const char *medium=(const char*)sqlite3_column_text(rows,2);
        const char *look=(const char*)sqlite3_column_text(rows,3);
        const char *lede=(const char*)sqlite3_column_text(rows,4);
        if(!id||!*id||!title||!*title)continue;
        if(!shown)fputs("<section class=\"ph-worlds\" data-profile-worlds><header class=\"ph-worlds-head\"><h2>Published spaces</h2><span class=\"ph-world-mark\" aria-hidden=\"true\">" BF_ICON_WORLD "</span></header><div class=\"ph-worlds-grid\">",stdout);
        ui_start(stdout,"a");ui_attr(stdout,"class","ph-world-card");ui_attr_open(stdout,"data-look");html(look?look:"");
        fputs("\" href=\"https://",stdout);html(site->host);fputs("/worlds/",stdout);html(id);
        fputs("\"><small>",stdout);html(medium?medium:"Space");ui_close(stdout,"small");ui_open(stdout,"strong",NULL);html(title);ui_close(stdout,"strong");
        if(lede&&*lede){ui_text(stdout, "p", NULL, lede);}
        ui_close(stdout,"a");shown++;
    }
    sqlite3_finalize(rows);
    if(shown){ui_close(stdout,"div");ui_start(stdout,"a");ui_attr(stdout,"class","ph-worlds-all");ui_attr_open(stdout,"href");ui_copy(stdout,"https://");html(site->host);fputs("/worlds\">Browse all spaces</a></section>",stdout);}
}
/* Profile paths present installed native work without copying its source records. */
typedef struct {const char *title,*href,*glyph,*description;} PhJourneyLink;
static void ph_journey(const PhSite *site){
    static const PhJourneyLink paths[][3]={
        {{"Sources","/sources",BF_ICON_RSS,"Read published sources"},{"Recipes","/recipes",BF_ICON_BOOK,"Search and plan from retained recipes"},{"Feeds","/channels",BF_ICON_ACTIVITY,"Explore publishers and live channels"}},
        {{"Profile","/profile",BF_ICON_USER,"Your own account and sites"},{"Collections","/profile?tab=saved",BF_ICON_BOOKMARK,"Private saved sources and collections"},{"Operating roles",BF_ORIGIN_WORK "/roles",BF_ICON_BRIEFCASE,"Choose your scoped working posture"}},
        {{"Posts",NULL,BF_ICON_FILE_TEXT,"Write, review and publish"},{"Worlds",NULL,BF_ICON_WORLD,"Build spaces with retained references"},{"Programs","/programs?context=self",BF_ICON_BOARD,"Continue work in installed programs"}}
    };
    static const char *stages[]={"discover","keep","create"};
    static const char *labels[]={"Discover","Keep","Create"};
    static const char *kickers[]={"Sources · Recipes · Feeds","Profile · Collections","Posts · Worlds · Programs"};
    static const char *glyphs[]={BF_ICON_COMPASS,BF_ICON_BOOKMARK,BF_ICON_SPARKLES};
    ui_start(stdout,"section");ui_attr(stdout,"class","ph-pathways");ui_flag(stdout,"data-profile-pathways");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your source-to-creation journey");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Discover retained material, keep it in your own account, then create with your words and review before publication.");ui_close(stdout,"p");ui_close(stdout,"header");ui_open(stdout,"div","ph-pathway-grid");
    for(int stage=0;stage<3;stage++){
        ui_start(stdout,"section");ui_attr(stdout,"class","ph-pathway");ui_attr_open(stdout,"data-pathway-stage");html(stages[stage]);fputs("\"><div class=\"ph-pathway-head\">",stdout);
        fputs(glyphs[stage],stdout);ui_open(stdout,"h3",NULL);html(labels[stage]);ui_close(stdout,"h3");ui_close(stdout,"div");ui_open(stdout,"p","ph-pathway-kicker");html(kickers[stage]);ui_close(stdout,"p");ui_start(stdout,"nav");ui_attr(stdout,"class","ph-pathway-items");ui_attr_open(stdout,"aria-label");html(labels[stage]);fputs(" destinations\">",stdout);
        for(int j=0;j<3;j++){const PhJourneyLink *path=&paths[stage][j];if(!path->title)continue;
            ui_start(stdout,"a");ui_attr_open(stdout,"data-pathway-link");html(path->title);fputs("\" href=\"",stdout);
            if(stage==2&&j==0){if(site)ph_url(site,"compose",1);else fputs("/profile?tab=sites",stdout);}
            else if(stage==2&&j==1){if(site){fputs("/workspace/worlds?site=",stdout);url_query_value(site->id);}else fputs("/profile?tab=sites",stdout);}
            else html(path->href);
            fputs("\">",stdout);fputs(path->glyph,stdout);ui_open(stdout,"span",NULL);ui_open(stdout,"strong",NULL);html(path->title);ui_close(stdout,"strong");ui_open(stdout,"small",NULL);html(path->description);ui_close(stdout,"small");ui_close(stdout,"span");ui_close(stdout,"a");
        }
        ui_close(stdout,"nav");ui_close(stdout,"section");
    }
    ui_close(stdout,"div");ui_open(stdout,"p","ph-pathway-principle");ui_copy(stdout,"One retained source identity, multiple account-owned continuations, explicit publication review. Saving is private; posting is a separate decision.");ui_close(stdout,"p");ui_close(stdout,"section");
}
/* The private overview lists only sites already bound to this actor. Published
 * post counts use the same owner, visibility and moderation predicates as the
 * public profile; no account work is copied into a public page. */
static void ph_territory_icon(const char *territory){
    const char *glyph=!strcmp(territory,"life")?BF_ICON_HEART:!strcmp(territory,"live")?BF_ICON_PLAY:!strcmp(territory,"shop")?BF_ICON_SHOP:BF_ICON_BRIEFCASE;
    ui_start(stdout,"span");ui_attr(stdout,"class","ph-realm-icon");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);fputs(glyph,stdout);ui_close(stdout,"span");
}
static void ph_private_sites(sqlite3 *db,const PhSite *sites,int count){
    if(!db||count<1)return;
    sqlite3_stmt *q=NULL;
    const char *sql="SELECT count(*) FROM community_post WHERE namespace=?1 AND community=?1 AND author_subject=?2 AND visibility='public' AND moderation_state='accepted' AND status='published'";
    int available=sqlite3_prepare_v2(db,sql,-1,&q,NULL)==SQLITE_OK;
    ui_start(stdout,"section");ui_flag(stdout,"data-profile-site-overview");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your public sites");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Only published site and post records appear here.");ui_close(stdout,"p");ui_open(stdout,"div","ph-realm-grid");
    const char *territories[]={"life","live","shop","work"};
    for(size_t t=0;t<4;t++){
        const char *territory=territories[t];const PhSite *s=NULL;int matches=0;
        for(int i=0;i<count;i++)if(!strcmp(sites[i].territory,territory)){s=&sites[i];matches++;}
        if(matches!=1){
            ui_start(stdout,"article");ui_attr(stdout,"class","ph-realm ph-realm-unbound");ui_attr_open(stdout,"data-profile-unbound");html(territory);
            fputs("\"><div class=\"ph-realm-head\">",stdout);ph_territory_icon(territory);ui_open(stdout,"div",NULL);ui_open(stdout,"h3",NULL);html(ph_label(territory));
            ui_close(stdout,"h3");ui_open(stdout,"small",NULL);ui_copy(stdout,ph_claim_pending(db,sites->actor)?"Waiting for review":"Site setup needed");ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"div");ui_open(stdout,"p",NULL);ui_copy(stdout,"No published profile is bound to this account in this territory.");ui_close(stdout,"p");if(ph_claim_pending(db,sites->actor)){ui_open(stdout,"p",NULL);ui_copy(stdout,"Your address is waiting for review.");ui_close(stdout,"p");}else{ui_open(stdout,"nav","ph-realm-actions");ui_start(stdout,"a");ui_attr(stdout,"href","/profile?tab=sites");ui_gt(stdout);ui_copy(stdout,"Set up site");ui_close(stdout,"a");ui_close(stdout,"nav");}ui_close(stdout,"article");
            continue;
        }
        ui_start(stdout,"article");ui_attr(stdout,"class","ph-realm");ui_attr_open(stdout,"data-profile-territory");html(territory);
        fputs("\"><div class=\"ph-realm-head\">",stdout);ph_territory_icon(territory);ui_open(stdout,"div",NULL);ui_open(stdout,"h3",NULL);html(ph_label(territory));
        ui_close(stdout,"h3");ui_open(stdout,"small",NULL);html(s->host);ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"strong",NULL);html(s->name);
        ui_close(stdout,"strong");ui_open(stdout,"p","ph-realm-bio");
        if(s->bio[0])html(s->bio);else fputs("An introduction has not been published yet.",stdout);
        ui_close(stdout,"p");ui_close(stdout,"div");
        if(available){sqlite3_reset(q);sqlite3_clear_bindings(q);sqlite3_bind_text(q,1,s->id,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,s->actor,-1,SQLITE_STATIC);
            if(sqlite3_step(q)==SQLITE_ROW){long long n=sqlite3_column_int64(q,0);ui_open(stdout,"p",NULL);fprintf(stdout,"%lld",n);ui_copy(stdout," published post");ui_copy(stdout,n==1?"":"s");ui_close(stdout,"p");}}
        else ui_text(stdout, "p", NULL, "Published-post count unavailable");
        ui_start(stdout,"nav");ui_attr(stdout,"class","ph-realm-actions");ui_attr(stdout,"aria-label","Site actions");ui_gt(stdout);ui_start(stdout,"a");ui_attr_open(stdout,"href");ph_url(s,"overview",1);
        fputs("\">Manage site</a><a href=\"",stdout);ph_url(s,"edit",1);fputs("\">Edit introduction</a><a data-profile-edit-pages href=\"/cms/compose/advanced?site=",stdout);url_query_value(s->id);fputs("\">Edit pages</a><a href=\"",stdout);
        ph_address(s,1);fputs("\">View public site</a></nav></article>",stdout);
    }
    ui_close(stdout,"div");ui_close(stdout,"section");sqlite3_finalize(q);
}
/* A person's private profile is useful before they publish a site.
 * Read only the same account-work index that owns the Activity/Inbox view;
 * private work is never represented as public profile content. */
static void ph_private_work(long uid){
    char subject[64];
    const char *path=getenv("BONFYRE_ACCOUNT_WORK_DB");
    sqlite3 *db=NULL; sqlite3_stmt *q=NULL;
    long long total=0,attention=0,waiting=0;
    int shown=0;
    snprintf(subject,sizeof(subject),"user:%ld",uid);
    ui_start(stdout,"section");ui_attr(stdout,"class","ph-private-work");ui_flag(stdout,"data-private-work");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your work in Aurekai");ui_close(stdout,"h2");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Private account work is separate from what you publish on a profile site. Recorded states are not live worker heartbeats.");ui_close(stdout,"p");ui_start(stdout,"nav");ui_attr(stdout,"class","ph-actions");ui_attr(stdout,"aria-label","Continue your work");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"href","/inbox?context=self");ui_gt(stdout);ui_copy(stdout,"Inbox and decisions");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/work-board?context=self");ui_gt(stdout);ui_copy(stdout,"Work board");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/apps?context=self");ui_gt(stdout);ui_copy(stdout,"Installed tools");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/saved");ui_gt(stdout);ui_copy(stdout,"Saved material");ui_close(stdout,"a");ui_close(stdout,"nav");
    if(!path||*path!='/')goto unavailable;
    if(bf_store_open(path, &db, SQLITE_OPEN_READONLY, &bf_store_policy_account_work)!=SQLITE_OK)goto unavailable;
    sqlite3_busy_timeout(db,500);
    if(sqlite3_prepare_v2(db,
      "SELECT count(*),"
      "sum(CASE WHEN state IN('blocked','retry_wait','dead_letter') THEN 1 ELSE 0 END),"
      "sum(CASE WHEN state IN('ready','defined','running') THEN 1 ELSE 0 END) "
      "FROM account_work WHERE subject=?1 AND scope='self' AND state NOT IN('complete','cancelled')",
      -1,&q,NULL)!=SQLITE_OK)goto unavailable;
    sqlite3_bind_text(q,1,subject,-1,SQLITE_STATIC);
    if(sqlite3_step(q)!=SQLITE_ROW)goto unavailable;
    total=sqlite3_column_int64(q,0);
    attention=sqlite3_column_int64(q,1);
    waiting=sqlite3_column_int64(q,2);
    sqlite3_finalize(q);q=NULL;
    if(total<0||attention<0||waiting<0)goto unavailable;
    ui_start(stdout,"p");ui_attr(stdout,"class","ph-work-counts");ui_attr_open(stdout,"data-work-total");fprintf(stdout,"%lld",total);ui_attr_close(stdout);ui_attr_open(stdout,"data-work-attention");fprintf(stdout,"%lld",attention);ui_attr_close(stdout);ui_gt(stdout);ui_open(stdout,"strong",NULL);fprintf(stdout,"%lld",total);ui_close(stdout,"strong");ui_copy(stdout," open records · ");ui_open(stdout,"strong",NULL);fprintf(stdout,"%lld",attention);ui_close(stdout,"strong");ui_copy(stdout," held or requiring recovery · ");ui_open(stdout,"strong",NULL);fprintf(stdout,"%lld",waiting);ui_close(stdout,"strong");ui_copy(stdout," ready or owner-reported in progress");ui_close(stdout,"p");
    if(sqlite3_prepare_v2(db,
      "SELECT id,title,state FROM account_work WHERE subject=?1 AND scope='self' "
      "AND state IN('blocked','retry_wait','dead_letter','running','ready') "
      "ORDER BY updated_ms DESC,id LIMIT 5",
      -1,&q,NULL)==SQLITE_OK){
        sqlite3_bind_text(q,1,subject,-1,SQLITE_STATIC);
        ui_open(stdout,"ol","ph-work-items");
        while(sqlite3_step(q)==SQLITE_ROW){
            const char *id=(const char*)sqlite3_column_text(q,0);
            const char *title=(const char*)sqlite3_column_text(q,1);
            const char *state=(const char*)sqlite3_column_text(q,2);
            if(!id||!title||!state||!ac_word(id))continue;
            ui_open(stdout,"li",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=self&amp;task=");
            url_query_value(id);
            fputs("\">",stdout);html(title);ui_close(stdout,"a");ui_copy(stdout," ");ui_open(stdout,"small",NULL);html(state);ui_close(stdout,"small");ui_close(stdout,"li");shown++;
        }
        ui_close(stdout,"ol");
    }
    if(!total)ui_text(stdout, "p", NULL, "No active work is indexed for this account. This does not imply that the remote service was checked.");
    if(total>shown){ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"href","/work-board?context=self");ui_gt(stdout);ui_copy(stdout,"See the complete account work history");ui_close(stdout,"a");ui_close(stdout,"p");}
    goto done;
unavailable:
    fputs("<p role=\"alert\" data-work-unavailable>The native Workboard index is unavailable on this service. Your work has not been verified empty. <a href=\"/work-board?context=self\">Open the Workboard</a>.</p>",stdout);
done:
    sqlite3_finalize(q);if(db)sqlite3_close(db);
    ui_close(stdout,"section");
}
static int ph_cms(const char *action,const char *type,const char *id,const char *payload,const char *ns,const char *f1,const char *v1,const char *f2,const char *v2){const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");if(!db||!schemas)return 0;char *args[28];int n=0;args[n++]=(char*)bin;args[n++]="entry";args[n++]=(char*)action;args[n++]=(char*)type;if(id)args[n++]=(char*)id;args[n++]=(char*)payload;args[n++]="--ns";args[n++]=(char*)ns;args[n++]="--db";args[n++]=(char*)db;args[n++]="--schemas";args[n++]=(char*)schemas;if(f1){args[n++]="--if-field";args[n++]=(char*)f1;args[n++]="--if-value";args[n++]=(char*)v1;}if(f2){args[n++]="--if-field2";args[n++]=(char*)f2;args[n++]="--if-value2";args[n++]=(char*)v2;}args[n]=NULL;char output[8192];return auth_call(args,NULL,output,sizeof(output))==0;}
static int ph_cms_cover_remove(const char *id,const char *ns,const char *digest){
 const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");
 if(!db||!schemas)return 0;
 char *args[]={(char*)bin,"entry","update","site_fragment",(char*)id,"{\"status\":\"archived\"}","--ns",(char*)ns,"--db",(char*)db,"--schemas",(char*)schemas,"--if-digest",(char*)digest,NULL};
 char output[8192];return auth_call(args,NULL,output,sizeof(output))==0;
}
/* A new account claims its own address once: one handle, one person subject,
 * one presence per territory, and one site draft per territory waiting for
 * the same review that publishes any site. Auth owns the binding, Matrix owns
 * the address, CMS owns the drafts; this handler only carries one reviewed
 * form across the three owners, and every step re-reads before it writes so
 * a retry or a rival claim can never half-apply. */
static int ph_claim_handle(const char *s){size_t n=s?strlen(s):0;if(n<2||n>40)return 0;if(!((s[0]>='a'&&s[0]<='z')||(s[0]>='0'&&s[0]<='9')))return 0;for(size_t i=1;i<n;i++)if(!((s[i]>='a'&&s[i]<='z')||(s[i]>='0'&&s[i]<='9')||s[i]=='-'))return 0;return 1;}
static void ph_claim_facts(const char *handle,char digest[65],char evidence[64],char actor[192],char institution[224]){char material[128];int n=snprintf(material,sizeof(material),"profile-claim-v1\n%s",handle);if(n<0||n>=(int)sizeof(material)){digest[0]=0;return;}bf_sha256_hex((const unsigned char*)material,(size_t)n,digest);snprintf(evidence,64,"claim:profile:%s",handle);snprintf(actor,192,"person:%s",handle);snprintf(institution,224,"inst:person-%s",handle);}
static int ph_claim_row(sqlite3 *db,const char *sql,const char *a,const char *b){sqlite3_stmt *st=NULL;int found=0;if(!db||sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return -1;sqlite3_bind_text(st,1,a?a:"",-1,SQLITE_STATIC);if(b)sqlite3_bind_text(st,2,b,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW)found=1;sqlite3_finalize(st);return found;}
static int ph_claim_binding_free(const char *auth_db,const char *actor,long self){sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int rows=0,mine=0,ok=-1;if(!auth_db||bf_store_open(auth_db,&db,SQLITE_OPEN_READONLY,&bf_store_policy_auth)!=SQLITE_OK)return -1;if(sqlite3_prepare_v2(db,"SELECT user_id FROM actor_closure_bindings WHERE actor_subject=?1 AND state='active' LIMIT 3",-1,&st,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);while(sqlite3_step(st)==SQLITE_ROW){long uid=sqlite3_column_int64(st,0);rows++;if(uid==self)mine++;}ok=rows==mine;done:sqlite3_finalize(st);if(db)sqlite3_close(db);return ok;}
static int ph_claim_pending(sqlite3 *db,const char *actor){sqlite3_stmt *st=NULL;int n=0;if(!db||!actor||!*actor)return 0;if(sqlite3_prepare_v2(db,"SELECT count(*) FROM site_presentation WHERE namespace='root' AND profile='member-profile-v1' AND subject_address=?1 AND status='draft'",-1,&st,NULL)!=SQLITE_OK)return 0;sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW)n=sqlite3_column_int(st,0);sqlite3_finalize(st);return n>0;}
static int ph_claim_matrix_free(BfMatrix *m,const char *handle,const char *actor,const char *institution,const char *digest){static const char *territories[]={"life","live","shop","work"};BfMatrixSubjectRecord record;BfMatrixDomainResolution resolved;int exists=bf_matrix_subject_exists(m,actor);if(exists<0)return -1;if(exists>0){if(bf_matrix_subject_read(m,actor,&record)!=1||strcmp(record.subject_kind,"Person")||strcmp(record.identity_digest,digest))return 0;}for(size_t i=0;i<4;i++){char host[256];if((size_t)snprintf(host,sizeof(host),"%s.aurekai.%s",handle,territories[i])>=sizeof(host))return 0;int found=bf_matrix_domain_resolve(m,host,&resolved);if(found<0)return -1;if(found>0&&!strcmp(resolved.matched_hostname,host)&&(!resolved.institution_id[0]||strcmp(resolved.institution_id,institution)||strcmp(resolved.territory,territories[i])))return 0;}return 1;}
static int ph_claim_cms_free(sqlite3 *db,const char *actor,const char *handle){static const char *territories[]={"life","live","shop","work"};for(size_t i=0;i<4;i++){char host[256],site[128];int row;if((size_t)snprintf(host,sizeof(host),"%s.aurekai.%s",handle,territories[i])>=sizeof(host))return 0;if((size_t)snprintf(site,sizeof(site),"%s-%s",handle,territories[i])>=sizeof(site))return 0;row=ph_claim_row(db,"SELECT 1 FROM site_presentation WHERE host=?1 AND subject_address<>?2 LIMIT 1",host,actor);if(row!=0)return row<0?-1:0;row=ph_claim_row(db,"SELECT 1 FROM site_presentation WHERE namespace='root' AND site_id=?1 AND subject_address<>?2 LIMIT 1",site,actor);if(row!=0)return row<0?-1:0;}return 1;}
static int ph_claim_matrix_bind(BfMatrix *m,const char *handle,const char *actor,const char *institution,const char *digest,const char *evidence){static const char *territories[]={"life","live","shop","work"};BfMatrixSubjectSpec subject={actor,"Person",digest,evidence};BfMatrixInstitutionSpec owner={institution,"Person",BF_MATRIX_LIVE,evidence};BfMatrixStatus st=bf_matrix_subject_bind(m,&subject);if(st==BF_MATRIX_IDENTITY_CONFLICT)return 409;if(st!=BF_MATRIX_OK)return 503;st=bf_matrix_institution_bind(m,&owner);if(st==BF_MATRIX_IDENTITY_CONFLICT)return 409;if(st!=BF_MATRIX_OK)return 503;for(size_t i=0;i<4;i++){char host[256],presence[256];PhSite probe;BfMatrixDomainSpec domain;BfMatrixPresenceSpec pspec;if((size_t)snprintf(host,sizeof(host),"%s.aurekai.%s",handle,territories[i])>=sizeof(host)||(size_t)snprintf(presence,sizeof(presence),"presence:%s:%s",handle,territories[i])>=sizeof(presence))return 503;domain=(BfMatrixDomainSpec){host,institution,territories[i],BF_MATRIX_PUBLIC,BF_MATRIX_LIVE,"HTML",NULL,evidence};if(bf_matrix_domain_bind(m,&domain)!=BF_MATRIX_OK)return 503;pspec=(BfMatrixPresenceSpec){presence,actor,NULL,territories[i],BF_MATRIX_PUBLIC,BF_MATRIX_LIVE,"HTML",evidence};st=bf_matrix_presence_bind(m,&pspec);if(st!=BF_MATRIX_OK&&st!=BF_MATRIX_DUPLICATE_PRESENCE)return 503;memset(&probe,0,sizeof(probe));snprintf(probe.host,sizeof(probe.host),"%s",host);snprintf(probe.actor,sizeof(probe.actor),"%s",actor);snprintf(probe.territory,sizeof(probe.territory),"%s",territories[i]);if(!ph_matrix(&probe))return 503;}return 303;}
static int ph_claim_cms_draft(sqlite3 *db,const char *handle,const char *actor){static const char *territories[]={"life","live","shop","work"};char name[41];size_t hn=strlen(handle);if(hn>40)return 503;snprintf(name,sizeof(name),"%s",handle);if(name[0]>='a'&&name[0]<='z')name[0]-='a'-'A';for(size_t i=0;i<4;i++){char host[256],site[128];bf_jsonw w;if((size_t)snprintf(host,sizeof(host),"%s.aurekai.%s",handle,territories[i])>=sizeof(host)||(size_t)snprintf(site,sizeof(site),"%s-%s",handle,territories[i])>=sizeof(site))return 503;if(ph_claim_row(db,"SELECT 1 FROM site_presentation WHERE namespace='root' AND site_id=?1 LIMIT 1",site,NULL)<0)return 503;if(!ph_claim_row(db,"SELECT 1 FROM site_presentation WHERE namespace='root' AND site_id=?1 LIMIT 1",site,NULL)){bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"site_id");bf_jsonw_str(&w,site);bf_jsonw_key(&w,"host");bf_jsonw_str(&w,host);bf_jsonw_key(&w,"subject_address");bf_jsonw_str(&w,actor);bf_jsonw_key(&w,"profile");bf_jsonw_str(&w,"member-profile-v1");bf_jsonw_key(&w,"territory");bf_jsonw_str(&w,territories[i]);bf_jsonw_key(&w,"navigation");bf_jsonw_arr(&w);bf_jsonw_arr_end(&w);bf_jsonw_key(&w,"status");bf_jsonw_str(&w,"draft");bf_jsonw_obj_end(&w);if(w.oom||!w.buf||!ph_cms("create","site_presentation",NULL,w.buf,"root",NULL,NULL,NULL,NULL)){bf_jsonw_free(&w);return 503;}bf_jsonw_free(&w);if(!ph_claim_row(db,"SELECT 1 FROM site_presentation WHERE namespace='root' AND site_id=?1 LIMIT 1",site,NULL))return 503;}if(ph_claim_row(db,"SELECT 1 FROM site_page WHERE namespace=?1 AND route='/' LIMIT 1",site,NULL)<0)return 503;if(!ph_claim_row(db,"SELECT 1 FROM site_page WHERE namespace=?1 AND route='/' LIMIT 1",site,NULL)){bf_jsonw_init(&w,0);bf_jsonw_obj(&w);bf_jsonw_key(&w,"route");bf_jsonw_str(&w,"/");bf_jsonw_key(&w,"title");bf_jsonw_str(&w,name);bf_jsonw_key(&w,"heading");bf_jsonw_str(&w,name);bf_jsonw_key(&w,"status");bf_jsonw_str(&w,"draft");bf_jsonw_obj_end(&w);if(w.oom||!w.buf||!ph_cms("create","site_page",NULL,w.buf,site,NULL,NULL,NULL,NULL)){bf_jsonw_free(&w);return 503;}bf_jsonw_free(&w);if(!ph_claim_row(db,"SELECT 1 FROM site_page WHERE namespace=?1 AND route='/' LIMIT 1",site,NULL))return 503;}}return 303;}
static int ph_claim(const char *body,const char *raw,const char *host,const StudioViewer *who,const char *bound,sqlite3 *db,const BfRequest *r){char handle[41]="";PhField fields[]={{"handle",handle,sizeof(handle),0}};char digest[65]="",evidence[64]="",actor[192]="",institution[224]="",current[192]="";if(!ph_origin(raw,host)||!ph_fields(body,fields,1,40000)){ph_error(403,"Review and submit this form from its own profile page.\n");return 1;}if(!ph_claim_handle(handle)){ph_error(400,"Use 2 to 40 lowercase letters, digits and dashes, starting with a letter or digit.\n");return 1;}ph_claim_facts(handle,digest,evidence,actor,institution);if(!digest[0]||!r->m||!db){ph_error(503,"The address owner is unavailable.\n");return 1;}if(bound&&*bound&&strcmp(bound,actor)){ph_error(409,"This account already claimed its address. One account holds one address.\n");return 1;}int slot=ph_claim_binding_free(r->auth_db,actor,who->id);if(slot<0){ph_error(503,"The address owner is unavailable.\n");return 1;}if(!slot){ph_error(409,"That handle is bound to another account. Try another spelling.\n");return 1;}slot=ph_claim_cms_free(db,actor,handle);if(slot<0){ph_error(503,"The address owner is unavailable.\n");return 1;}if(!slot){ph_error(409,"That address is already in use on a site. Try another spelling.\n");return 1;}slot=ph_claim_matrix_free(r->m,handle,actor,institution,digest);if(slot<0){ph_error(503,"The address owner is unavailable.\n");return 1;}if(!slot){ph_error(409,"That address is reserved in the directory. Try another spelling.\n");return 1;}if(!bound||!*bound){char uid[24],output[1024];if((size_t)snprintf(uid,sizeof(uid),"%ld",who->id)>=sizeof(uid)){ph_error(503,"The address owner is unavailable.\n");return 1;}char *const argv[]={(char*)auth_binary(),"--db",(char*)r->auth_db,"actor-bind","--user-id",uid,"--actor",actor,NULL};if(auth_call(argv,NULL,output,sizeof(output))!=0){ph_error(503,"The address owner is unavailable.\n");return 1;}if(!ph_actor(who->id,r->auth_db,current)||strcmp(current,actor)){ph_error(503,"The address claim was not confirmed. Reload the profile before retrying.\n");return 1;}if(ph_claim_binding_free(r->auth_db,actor,who->id)!=1){char *const revoke[]={(char*)auth_binary(),"--db",(char*)r->auth_db,"actor-revoke","--user-id",uid,NULL};char ignored[256];auth_call(revoke,NULL,ignored,sizeof(ignored));ph_error(409,"That handle was just claimed by someone else. Try another spelling.\n");return 1;}}int code=ph_claim_matrix_bind(r->m,handle,actor,institution,digest,evidence);if(code!=303){ph_error(code,code==409?"That address conflicts in the directory. Try another spelling.\n":"The address claim was not confirmed. Reload the profile before retrying.\n");return 1;}code=ph_claim_cms_draft(db,handle,actor);if(code!=303){ph_error(code,"The address claim was not confirmed. Reload the profile before retrying.\n");return 1;}page_redirect("/profile?tab=sites");return 1;}
static void ph_claim_form(void){const FtInput in[]={{.label="Your handle",.type="text",.name="handle",.placeholder="e.g. riverstone",.maxlength="40",.pattern="[a-z0-9][a-z0-9-]{1,39}",.required=1}};const FtSubmit go[]={{.label="Claim your address"}};const FtForm form={.id="profile-claim",.method="post",.action="/profile/claim",.label="Claim your address",.hook="profile-claim",.hook_value="form",.inputs=in,.ninputs=1,.submits=go,.nsubmits=1};ft_form(stdout,&form);}
static void ph_redirect(const PhSite *site,const char *tab){char location[300];snprintf(location,sizeof(location),"/profile?site=%s&tab=%s",site->id,tab);page_redirect(location);}
static void ph_nonce(char out[33]){unsigned char data[16];sqlite3_randomness(16,data);for(int i=0;i<16;i++)snprintf(out+i*2,3,"%02x",data[i]);}
static int ph_digits(const char *s){return s&&*s&&strlen(s)<20&&strspn(s,"0123456789")==strlen(s);}
static int ph_link(const char *s){if(!s||!*s)return 1;if(strlen(s)>1400||!public_http_url(s)||strpbrk(s,"\r\n\t <>\"\\"))return 0;const char *host=strstr(s,"://")+3,*end=strchr(host,'/');if(!end)end=host+strlen(host);return !memchr(host,'@',(size_t)(end-host));}
/* Resolve one published archive coordinate; do not copy its retained excerpt into a post. */
static int ph_retained_link(sqlite3 *cms,const char *ref,char title[768],char href[1401]){
 const char *slash=ref?strchr(ref,'/'):NULL;
 if(!cms||!slash||slash==ref||strchr(slash+1,'/')||(size_t)(slash-ref)>96||strlen(slash+1)!=24)return 0;
 char source[97],key[25],family[64]={0};size_t n=(size_t)(slash-ref);
 memcpy(source,ref,n);source[n]=0;snprintf(key,sizeof(key),"%s",slash+1);
 if(strspn(source,"abcdefghijklmnopqrstuvwxyz0123456789-_.")!=n||strspn(key,"0123456789abcdef")!=24||!a2a_public_released(cms,source))return 0;
 sqlite3 *archive=page_archive_open(0);if(!archive)return 0;
 PageItem *item=calloc(1,sizeof(*item));
 int ok=item&&page_archive_item(archive,source,key,item,family,sizeof(family));
 sqlite3_close(archive);
 if(ok)ok=item->title[0]&&item->href[0]&&public_http_url(item->href);
 if(ok){snprintf(title,768,"%.*s",160,item->title);
   int written=snprintf(href,1401,BF_ORIGIN_WORK "/o/%s/%s",source,key);
   ok=written>0&&written<1401&&ph_link(href);}
 free(item);return ok;
}
static int ph_saved(sqlite3 *db,long uid,const char *id,char title[768],char href[1401]){sqlite3_stmt *st=NULL;char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);if(!ph_digits(id)||sqlite3_prepare_v2(db,"SELECT title,href FROM saved_item WHERE id=?1 AND actor=?2 AND status='published'",-1,&st,NULL)!=SQLITE_OK)return 0;sqlite3_bind_int64(st,1,strtoll(id,NULL,10));sqlite3_bind_text(st,2,actor,-1,SQLITE_STATIC);int ok=sqlite3_step(st)==SQLITE_ROW;if(ok){const char *a=(const char*)sqlite3_column_text(st,0),*b=(const char*)sqlite3_column_text(st,1);ok=a&&strlen(a)<768&&b&&strlen(b)<1401&&ph_link(b)&&*b;if(ok){snprintf(title,768,"%s",a);snprintf(href,1401,"%s",b);}}sqlite3_finalize(st);return ok;}
static void ph_saved_list(sqlite3 *db,long uid,const PhSite *site,const char *q,long long cursor,int take){char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);sqlite3_stmt *st=NULL;fputs("<h2>Saved for you</h2><p class=\"ph-privacy\">Private across your account. Saving something never puts it on a public site. You choose a site and review a post before sharing a link.</p><form class=\"ph-search\" method=\"get\" action=\"/profile\">",stdout);ph_hidden("site",site?site->id:"");ph_hidden("tab","saved");fputs("<input type=\"search\" name=\"q\" aria-label=\"Search your saved material\" placeholder=\"Search saved titles, publishers or collections\" value=\"",stdout);html(q);fputs("\"><button type=\"submit\">Search saved</button></form><div class=\"ph-board\">",stdout);
 const char *sql="SELECT id,title,href,image,publisher,summary,saved_at,COALESCE(collection,''),item_ref FROM saved_item WHERE actor=?1 AND status='published' AND (?2=0 OR id<?2) AND (?3='' OR instr(lower(COALESCE(title,'')||' '||COALESCE(publisher,'')||' '||COALESCE(collection,'')),lower(?3))>0) ORDER BY id DESC LIMIT ?4";int count=0,more=0;long long last=0;
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);sqlite3_bind_int64(st,2,cursor);sqlite3_bind_text(st,3,q,-1,SQLITE_STATIC);sqlite3_bind_int(st,4,take+1);while(sqlite3_step(st)==SQLITE_ROW){if(count==take){more=1;break;}count++;last=sqlite3_column_int64(st,0);const char *title=(const char*)sqlite3_column_text(st,1),*href=(const char*)sqlite3_column_text(st,2),*image=(const char*)sqlite3_column_text(st,3),*pub=(const char*)sqlite3_column_text(st,4),*summary=(const char*)sqlite3_column_text(st,5),*saved=(const char*)sqlite3_column_text(st,6),*folder=(const char*)sqlite3_column_text(st,7);ui_start(stdout,"article");ui_attr(stdout,"class","ph-saved");ui_flag(stdout,"data-private-saved");ui_gt(stdout);ui_open(stdout,"div",NULL);ui_open(stdout,"small",NULL);html(folder&&*folder?folder:"Saved");ui_close(stdout,"small");ui_open(stdout,"h3",NULL);html(title);ui_close(stdout,"h3");if(image&&page_image_url_safe(image)){ui_start(stdout,"img");ui_attr(stdout,"loading","lazy");ui_attr(stdout,"decoding","async");ui_attr(stdout,"referrerpolicy","no-referrer");ui_attr(stdout,"alt","");ui_attr_open(stdout,"src");html(image);fputs("\">",stdout);}ui_open(stdout,"p",NULL);html(summary?summary:"");ui_close(stdout,"p");ui_open(stdout,"small",NULL);html(pub?pub:"");ui_close(stdout,"small");ui_start(stdout,"nav");ui_attr(stdout,"aria-label","Saved item actions");ui_gt(stdout);if(href&&*href&&ph_link(href)){ui_start(stdout,"a");ui_attr_open(stdout,"href");html(href);fputs("\" rel=\"noopener noreferrer\" target=\"_blank\">Open original \u2197</a>",stdout);if(site){ui_start(stdout,"a");ui_attr_open(stdout,"href");ph_url(site,"compose",1);printf("&amp;item=%lld\">Share a link here</a>",last);}}ui_close(stdout,"nav");ui_close(stdout,"div");ui_close(stdout,"article");(void)saved;}}sqlite3_finalize(st);ui_close(stdout,"div");if(!count){ui_open(stdout,"div","ph-empty");ui_open(stdout,"p",NULL);ui_copy(stdout,"No saved material matches this view.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/discover");ui_gt(stdout);ui_copy(stdout,"Explore and save something");ui_close(stdout,"a");ui_close(stdout,"div");}if(more){ui_start(stdout,"a");ui_attr(stdout,"class","ph-next");ui_attr_open(stdout,"href");ph_url(site,"saved",1);printf("&amp;cursor=%lld&amp;q=",last);url_query_value(q);fputs("\">More saved material</a>",stdout);}}
static void ph_work_examples(sqlite3 *db,const PhSite *site){
 sqlite3_stmt *st=NULL;int count=0;
 /* program_template binds a release to the reusable Program it starts; a store
  * not yet migrated reads as unbound rather than hiding released work. */
 if(!db||!site||(sqlite3_prepare_v2(db,"SELECT example_id,kicker,headline,dek,made_with,COALESCE(made_with_href,''),picture,picture_alt,pack,observed_at,COALESCE(program_template,'') FROM work_example WHERE namespace='root' AND status='published' AND COALESCE(site,'')=?1 ORDER BY position,id LIMIT 24",-1,&st,NULL)!=SQLITE_OK&&
    sqlite3_prepare_v2(db,"SELECT example_id,kicker,headline,dek,made_with,COALESCE(made_with_href,''),picture,picture_alt,pack,observed_at,'' FROM work_example WHERE namespace='root' AND status='published' AND COALESCE(site,'')=?1 ORDER BY position,id LIMIT 24",-1,&st,NULL)!=SQLITE_OK))return;
 sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *id=(const char*)sqlite3_column_text(st,0),*kicker=(const char*)sqlite3_column_text(st,1),*headline=(const char*)sqlite3_column_text(st,2),*dek=(const char*)sqlite3_column_text(st,3),*made_with=(const char*)sqlite3_column_text(st,4),*href=(const char*)sqlite3_column_text(st,5),*picture=(const char*)sqlite3_column_text(st,6),*alt=(const char*)sqlite3_column_text(st,7),*pack=(const char*)sqlite3_column_text(st,8),*published=(const char*)sqlite3_column_text(st,9),*program=(const char*)sqlite3_column_text(st,10);
  int bound=program&&program_template_token(program);
  if(!id||!headline||!*headline||!pack||!*pack||strlen(pack)>95||strspn(pack,"abcdefghijklmnopqrstuvwxyz0123456789.-_")!=strlen(pack))continue;
  if(!count++){ui_start(stdout,"section");ui_attr(stdout,"class","ph-work-list");ui_flag(stdout,"data-profile-work");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Released work");ui_close(stdout,"h2");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Work here is released by this profile. Starting one opens a program in your own account. Nothing from this profile is copied into it.");ui_close(stdout,"p");}
  ui_open(stdout,"article","ph-work");ui_open(stdout,"div",NULL);
  if(picture&&page_image_url_safe(picture)){ui_start(stdout,"img");ui_attr(stdout,"loading","lazy");ui_attr(stdout,"decoding","async");ui_attr_open(stdout,"src");html(picture);fputs("\" alt=\"",stdout);html(alt?alt:"");fputs("\">",stdout);}
  ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ph-kicker");html(kicker&&*kicker?kicker:"Released work");ui_close(stdout,"p");ui_open(stdout,"h3",NULL);html(headline);ui_close(stdout,"h3");if(dek&&*dek){ui_text(stdout, "p", NULL, dek);}if(made_with&&*made_with){ui_open(stdout,"p","ph-work-with");ui_copy(stdout,"Built with ");if(href&&public_http_url(href)){ui_start(stdout,"a");ui_attr(stdout,"rel","noopener noreferrer");ui_attr_open(stdout,"href");html(href);fputs("\">",stdout);html(made_with);ui_close(stdout,"a");}else html(made_with);fputs(".</p>",stdout);}if(published&&*published){ui_text(stdout, "time", NULL, published);}ui_start(stdout,"p");ui_flag(stdout,"data-work-program");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"class","ph-button");ui_attr_open(stdout,"href");ui_copy(stdout,"/programs?context=self");if(bound){fputs("&amp;template=",stdout);url_query_value(program);}fputs(bound?"\">Start this as a program</a> ":"\">Open your programs</a> ",stdout);ui_start(stdout,"a");ui_attr(stdout,"class","ph-work-with");ui_attr_open(stdout,"href");ui_copy(stdout,"/packs/setup?pack=");url_query_value(pack);fputs("&amp;context=self\">Set up the tool on its own</a></p></div></article>",stdout);
 }
 sqlite3_finalize(st);if(count)ui_close(stdout,"section");
}
/* One published site has journal pages and short community posts. */
static int ph_journal_articles(sqlite3 *db,const PhSite *site,int owner){
 sqlite3_stmt *st=NULL;int count=0;
 const char *own_sql="SELECT route,title,COALESCE(lede,''),COALESCE(eyebrow,''),p.status,0 "
 "FROM site_page p WHERE p.namespace=?1 AND p.route LIKE '/blog/%' "
 "AND ((?2=1 AND p.status IN ('draft','published')) OR (?2=0 AND p.status='published')) "
 "AND EXISTS(SELECT 1 FROM site_fragment f WHERE f.namespace=p.namespace "
 "AND f.page=p.route AND f.slot='showcase' "
 "AND f.status=p.status "
 "AND (p.status='draft' OR f.audience='public') "
 "AND (f.kind LIKE 'profile-%' OR f.kind LIKE 'ui:%' OR f.kind LIKE 'service:%') "
 "AND CASE WHEN json_valid(f.projection_contract) THEN COALESCE(json_extract(f.projection_contract,'$.html'),0)=1 ELSE 0 END "
 "AND CASE WHEN json_valid(f.semantic_tags) THEN COALESCE(json_extract(f.semantic_tags,'$.authoring_v1.version'),0)=1 ELSE 0 END) "
 "ORDER BY p.route LIMIT 128";
 /* Published placements are candidates until independently verified by the
  * exact two-scope source-and-destination admission and compiled digest gate. */
 const char *placement_sql="SELECT route,title,COALESCE(lede,''),COALESCE(eyebrow,''),p.status,0 AS placed "
 "FROM site_page p WHERE p.namespace=?1 AND p.route LIKE '/blog/%' "
 "AND ((?2=1 AND p.status IN ('draft','published')) OR (?2=0 AND p.status='published')) "
 "AND EXISTS(SELECT 1 FROM site_fragment f WHERE f.namespace=p.namespace "
 "AND f.page=p.route AND f.slot='showcase' "
 "AND f.status=p.status "
 "AND (p.status='draft' OR f.audience='public') "
 "AND (f.kind LIKE 'profile-%' OR f.kind LIKE 'ui:%' OR f.kind LIKE 'service:%') "
 "AND CASE WHEN json_valid(f.projection_contract) THEN COALESCE(json_extract(f.projection_contract,'$.html'),0)=1 ELSE 0 END "
 "AND CASE WHEN json_valid(f.semantic_tags) THEN COALESCE(json_extract(f.semantic_tags,'$.authoring_v1.version'),0)=1 ELSE 0 END) "
 "UNION ALL SELECT d.destination_route,p.title,COALESCE(p.lede,''),COALESCE(p.eyebrow,''),p.status,1 AS placed "
 "FROM site_publication_placement d JOIN site_publication_placement offer "
 "ON offer.namespace=d.origin_site AND offer.placement_id=d.placement_id AND offer.status='published' "
 "AND offer.origin_page_digest=d.origin_page_digest AND offer.review_request_ref=d.review_request_ref "
 "JOIN site_page p ON p.namespace=d.origin_site AND p.route=d.origin_route AND p.status='published' "
 "WHERE d.namespace=?1 AND d.destination_site=?1 AND d.destination_subject=?3 "
 "AND d.status='published' AND d.destination_route LIKE '/blog/%' "
 "AND NOT EXISTS(SELECT 1 FROM site_page own WHERE own.namespace=?1 AND own.route=d.destination_route) "
 "ORDER BY 1 LIMIT 128";
 if(!db||!site)return 0;
 if(sqlite3_prepare_v2(db,placement_sql,-1,&st,NULL)!=SQLITE_OK){
  sqlite3_finalize(st);st=NULL;
  if(sqlite3_prepare_v2(db,own_sql,-1,&st,NULL)!=SQLITE_OK)return 0;
 }
 sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);
 sqlite3_bind_int(st,2,owner?1:0);
 sqlite3_bind_text(st,3,site->actor,-1,SQLITE_TRANSIENT);
 while(sqlite3_step(st)==SQLITE_ROW){const char *route=(const char*)sqlite3_column_text(st,0),*title=(const char*)sqlite3_column_text(st,1),*lede=(const char*)sqlite3_column_text(st,2),*category=(const char*)sqlite3_column_text(st,3),*state=(const char*)sqlite3_column_text(st,4);int draft=state&&strcmp(state,"published")!=0,placed=sqlite3_column_int(st,5);
  PhPublicationPlacement approved={0};if(placed&&!ph_placement_resolve(db,site,route,&approved,NULL))continue;
  if(!route||!title||!*title||strncmp(route,"/blog/",6))continue;
  const char *slug=route+6;size_t len=strlen(slug);if(len&&slug[len-1]=='/')len--;
  if(!len||len>96||!((slug[0]>='a'&&slug[0]<='z')||(slug[0]>='0'&&slug[0]<='9')))continue;
  int safe=1;for(size_t i=1;i<len;i++)if(!((slug[i]>='a'&&slug[i]<='z')||(slug[i]>='0'&&slug[i]<='9')||slug[i]=='-')){safe=0;break;}
  if(!safe||(slug[len]&&slug[len]!='/'))continue;
  if(!count){
   ui_start(stdout,"section");ui_attr(stdout,"class","ph-journal");ui_flag(stdout,"data-profile-journal");ui_gt(stdout);ui_open(stdout,"div","ph-journal-heading");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ph-journal-eyebrow");
   fputs(owner?"Your journal":"Writing / Published work",stdout);
   ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Journal");ui_close(stdout,"h2");ui_close(stdout,"div");ui_open(stdout,"p",NULL);
   fputs(owner?"Composed articles on this site, published and in review. Unreleased writing stays private.":"Ideas and research written for this site.",stdout);
   ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div","ph-journal-lead");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ph-journal-eyebrow");
   fputs(owner?"Continue composing":"Start reading",stdout);ui_close(stdout,"span");ui_open(stdout,"h3",NULL);
   html(title);
   ui_close(stdout,"h3");ui_open(stdout,"p",NULL);if(lede)html(lede);
   ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"class","ph-journal-read");ui_attr_open(stdout,"href");
   if(owner&&draft){fputs("/cms/compose/advanced?site=",stdout);url_query_value(site->id);fputs("&amp;route=",stdout);url_query_value(route);}else fwrite(route,1,6+len,stdout);
   if(!(owner&&draft))fputc('/',stdout);fputs("\">",stdout);fputs(owner&&draft?"Continue editing article":"Read the featured article",stdout);
   ui_close(stdout,"a");ui_close(stdout,"div");ui_open(stdout,"div","ph-journal-lead-aside");ui_open(stdout,"span","ph-journal-eyebrow");
   fputs(owner?"Private editorial desk":"A living publication",stdout);
   ui_close(stdout,"span");ui_open(stdout,"p",NULL);
   fputs(owner?"Edit a draft, arrange a page, or read a published edition in the same journal.":"Every article published on this site, newest first. Open one to read it or have it read aloud.",stdout);
   ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/blog/");ui_gt(stdout);ui_copy(stdout,"Published journal");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");
   fputs("<div class=\"ph-journal-tools\"><details class=\"ph-journal-search\"><summary>" BF_ICON_SEARCH "<span>Search articles</span></summary><label>Search titles and topics<input type=\"search\" data-journal-search placeholder=\"Search articles\" autocomplete=\"off\"></label></details><div class=\"ph-journal-filters\" role=\"group\" aria-label=\"Filter by subject\"><button type=\"button\" data-journal-category=\"\" aria-pressed=\"true\">All subjects</button>",stdout);
   sqlite3_stmt *cats=NULL;
   if(sqlite3_prepare_v2(db,"SELECT DISTINCT COALESCE(eyebrow,'') FROM site_page WHERE namespace=?1 AND ((?2=1 AND status IN ('draft','published')) OR (?2=0 AND status='published')) AND route LIKE '/blog/%' AND COALESCE(eyebrow,'')<>'' ORDER BY 1",-1,&cats,NULL)==SQLITE_OK){
    sqlite3_bind_text(cats,1,site->id,-1,SQLITE_STATIC);
    sqlite3_bind_int(cats,2,owner?1:0);
    while(sqlite3_step(cats)==SQLITE_ROW){const char *c=(const char*)sqlite3_column_text(cats,0);if(!c||strlen(c)>120)continue;
     fputs("<button type=\"button\" data-journal-category=\"",stdout);html(c);fputs("\" aria-pressed=\"false\">",stdout);html(c);fputs("</button>",stdout);}
   }sqlite3_finalize(cats);
   ui_close(stdout,"div");ui_start(stdout,"p");ui_flag(stdout,"data-journal-count");ui_attr(stdout,"aria-live","polite");ui_gt(stdout);ui_close(stdout,"p");ui_close(stdout,"div");
   if(owner) {
    ui_start(stdout,"div");ui_attr(stdout,"class","ph-journal-batch");ui_flag(stdout,"data-journal-batch");ui_attr_open(stdout,"data-site");html(site->id);
    fputs("\"><div class=\"ph-journal-batch-actions\"><button type=\"button\" data-journal-select-all>Select visible drafts</button><button type=\"button\" data-journal-clear>Clear selection</button></div><label class=\"ph-journal-batch-rights\"><input type=\"checkbox\" data-journal-rights> I have reviewed each selected article and hold its publication rights.</label><button type=\"button\" data-journal-publish disabled>Publish selected articles</button><output data-journal-status role=\"status\" aria-live=\"polite\">Select drafts to review.</output></div>",stdout);
   }
   ui_open(stdout,"div","ph-journal-grid");
  }
  ui_start(stdout,"article");ui_attr(stdout,"class","ph-journal-card");ui_flag(stdout,"data-article");ui_attr_open(stdout,"data-publication-state");fputs(owner&&draft?"draft":"published",stdout);fputs("\" data-category=\"",stdout);html(category?category:"");
  fputs("\" data-search=\"",stdout);html(title);fputc(' ',stdout);if(lede)html(lede);fputc(' ',stdout);if(category)html(category);
  fputs("\" data-journal-route=\"",stdout);html(route);fputs("\">",stdout);
  if(owner&&draft) {fputs("<label class=\"ph-journal-card-select\"><input type=\"checkbox\" data-journal-select aria-label=\"Select ",stdout);html(title);fputs(" for publication review\"><span>Select for review</span></label>",stdout);}
  ui_start(stdout,"a");ui_attr_open(stdout,"href");if(owner&&draft){fputs("/cms/compose/advanced?site=",stdout);url_query_value(site->id);fputs("&amp;route=",stdout);url_query_value(route);}else fwrite(route,1,6+len,stdout);if(!(owner&&draft))fputc('/',stdout);fputs("\"><div class=\"ph-journal-card-top\"><span class=\"ph-journal-card-kind\">",stdout);
  html(category&&*category?category:"Journal");
  ui_close(stdout,"span");ui_open(stdout,"span","ph-journal-index");if(owner&&draft)fputs("DRAFT · ",stdout);printf("%02d",count+1);
  ui_close(stdout,"span");ui_close(stdout,"div");ui_open(stdout,"h3",NULL);html(title);
  ui_close(stdout,"h3");ui_open(stdout,"p",NULL);if(lede)html(lede);
  ui_close(stdout,"p");ui_open(stdout,"div","ph-journal-card-bottom");ui_open(stdout,"span",NULL);fputs(owner&&draft?"Private draft":(category&&*category?category:"Article"),stdout);
  ui_close(stdout,"span");ui_open(stdout,"strong",NULL);fputs(owner&&draft?"Continue editing":"Read article",stdout);fputs(" <span aria-hidden=\"true\">&#8599;</span></strong></div></a>",stdout);
  if(placed){ui_start(stdout,"p");ui_attr(stdout,"class","ph-journal-publisher");ui_flag(stdout,"data-publisher-credit");ui_gt(stdout);ui_copy(stdout,"Published by ");html(approved.author_credit);fputs(" · <a href=\"https://",stdout);html(approved.origin_host);html(approved.origin_route);fputs("\" rel=\"external noopener noreferrer\">Original article</a></p>",stdout);}
  if(owner&&!draft&&!placed){ui_start(stdout,"a");ui_attr(stdout,"class","ph-journal-edit");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/compose/advanced?site=");url_query_value(site->id);
   fputs("&amp;route=",stdout);url_query_value(route);fputs("\">Edit and arrange this published page</a>",stdout);}
  ui_close(stdout,"article");
  count++;
 }
 sqlite3_finalize(st);
 if(count){
  ui_close(stdout,"div");ui_start(stdout,"p");ui_attr(stdout,"class","ph-journal-empty");ui_flag(stdout,"hidden");ui_gt(stdout);ui_copy(stdout,"No stories match those filters.");ui_close(stdout,"p");ui_close(stdout,"section");
  fputs("<script>(()=>{for(const root of document.querySelectorAll(\"[data-profile-journal]\")){const cards=[...root.querySelectorAll(\"[data-article]\")],search=root.querySelector(\"[data-journal-search]\"),categories=[...root.querySelectorAll(\"[data-journal-category]\")],count=root.querySelector(\"[data-journal-count]\"),empty=root.querySelector(\".ph-journal-empty\");if(!search||!categories.length||!count||!empty)continue;const filterBar=root.querySelector(\".ph-journal-filters\");if(filterBar){for(const card of cards){const label=card.dataset.category;if(!label||label.length>120||categories.some(b=>b.dataset.journalCategory===label))continue;const b=document.createElement(\"button\");b.type=\"button\";b.dataset.journalCategory=label;b.setAttribute(\"aria-pressed\",\"false\");b.textContent=label;filterBar.append(b);categories.push(b);}}let category='';function apply(){const q=search.value.trim().toLocaleLowerCase();let n=0;for(const card of cards){const ok=!card.dataset.released&&(!category||card.dataset.category===category)&&(!q||card.dataset.search.toLocaleLowerCase().includes(q));card.hidden=!ok;if(ok)n++;}count.textContent=\"Showing \"+n+\" of \"+cards.filter(card=>!card.dataset.released).length+\" articles\";empty.hidden=n!==0;}search.addEventListener(\"input\",apply);for(const b of categories)b.addEventListener(\"click\",()=>{category=b.dataset.journalCategory;for(const x of categories)x.setAttribute(\"aria-pressed\",String(x===b));apply();});root.addEventListener(\"journalchange\",apply);apply();}})();</script>",stdout);
  if(owner)ph_journal_batch_script();
 }
 return count;
}
/* Private authoring queue: only exact site-owned, nonreleased CMS pages. */
static int ph_review_pages(sqlite3 *db,const PhSite *site){
 if(!db||!site)return 0;
 sqlite3_stmt *st=NULL;int n=0;
 const char *sql=
  "SELECT p.route,p.title,p.status,COALESCE(p.eyebrow,''),""(SELECT count(*) FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route),""(SELECT count(*) FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route AND f.status='published') ""FROM site_page p WHERE p.namespace=?1 AND "
  "((p.status NOT IN ('published','archived')) OR "
  "(p.status='published' AND EXISTS(SELECT 1 FROM site_fragment f WHERE "
  "f.namespace=p.namespace AND f.page=p.route AND f.status NOT IN ('published','archived')))) "
  "ORDER BY p.route LIMIT 100";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *route=(const char*)sqlite3_column_text(st,0),*title=(const char*)sqlite3_column_text(st,1),*status=(const char*)sqlite3_column_text(st,2);const char *subject=(const char*)sqlite3_column_text(st,3);const int sections=sqlite3_column_int(st,4),released=sqlite3_column_int(st,5);
  if(!route||*route!='/'||strlen(route)>127||!title||!*title)continue;
  if(!n++){ui_start(stdout,"section");ui_attr(stdout,"class","ph-review");ui_flag(stdout,"data-review-queue");ui_gt(stdout);ui_open(stdout,"header","ph-review-head");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ph-review-kicker");ui_copy(stdout,"Private editorial desk");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Pages awaiting your review");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Finish composing and reviewing these sections. Unreleased changes remain private.");ui_close(stdout,"p");ui_close(stdout,"div");ui_start(stdout,"a");ui_attr(stdout,"class","ph-review-new");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/compose/advanced?site=");url_query_value(site->id);fputs("&amp;new=article\">Write another article</a></header><div class=\"ph-review-grid\">",stdout);}
  ui_open(stdout,"article","ph-review-card");ui_open(stdout,"div","ph-review-meta");ui_open(stdout,"span",NULL);html(subject&&*subject?subject:"Site page");ui_close(stdout,"span");ui_open(stdout,"span",NULL);html(status&&!strcmp(status,"published")?"Section changes held":"Draft — not public");ui_close(stdout,"span");ui_close(stdout,"div");ui_open(stdout,"h3",NULL);html(title);ui_close(stdout,"h3");ui_open(stdout,"p","ph-review-count");printf("%d composed %s · %d released",sections,sections==1?"section":"sections",released);ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"class","ph-review-open");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/compose/advanced?site=");
  url_query_value(site->id);fputs("&amp;route=",stdout);url_query_value(route);fputs("\">Continue editing <span aria-hidden=\"true\">&#8599;</span></a></article>",stdout);
 }
 sqlite3_finalize(st);
 if(n){ui_close(stdout,"div");ui_open(stdout,"p","ph-review-summary");printf("%d %s awaiting composition and review",n,n==1?"page":"pages");ui_close(stdout,"p");ui_close(stdout,"section");}
 return n;
}
/* The member feed: every released, renderable journal page on any of this
 * person's published sites, newest first. A Life home therefore shows the
 * writing kept on the Work site instead of claiming nothing was published. */
typedef struct {char *route,*title,*lede,*subject,*date,*host,*by;} PhRow;
static int ph_feed(sqlite3 *db,const PhSite *site,int limit,const char *current_host,int owner){
 if(!db||!site)return 0;sqlite3_stmt *st=NULL;PhRow *rows=NULL;int n=0,total=0,cap=0;
 const char *sql="SELECT p.route,p.title,COALESCE(p.lede,''),COALESCE(p.eyebrow,''),COALESCE(p.editorial_published_on,''),s.host,COALESCE(p.author_credit,'') FROM site_page p JOIN site_presentation s ON s.site_id=p.namespace AND s.namespace='root' AND s.status='published' AND s.profile='member-profile-v1' AND ((?1<>'' AND s.subject_address=?1) OR (?1='' AND s.site_id=?2)) WHERE p.status='published' AND p.route LIKE '/blog/_%' AND EXISTS(SELECT 1 FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route AND f.slot='showcase' AND f.audience='public' AND f.status='published' AND (f.kind LIKE 'profile-%' OR f.kind LIKE 'ui:%' OR f.kind LIKE 'service:%') AND CASE WHEN json_valid(f.projection_contract) THEN COALESCE(json_extract(f.projection_contract,'$.html'),0)=1 ELSE 0 END) ORDER BY p.editorial_published_on DESC,p.route LIMIT 400";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(st,1,site->actor,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,site->id,-1,SQLITE_STATIC);
 while(sqlite3_step(st)==SQLITE_ROW){const char *route=(const char*)sqlite3_column_text(st,0),*title=(const char*)sqlite3_column_text(st,1),*host=(const char*)sqlite3_column_text(st,5);
  if(!route||strlen(route)>127||strspn(route+6,"abcdefghijklmnopqrstuvwxyz0123456789-/")!=strlen(route+6)||!title||!*title||!host||strlen(host)>200)continue;
  total++;if(n>=limit)continue;if(n==cap){cap=cap?cap*2:32;PhRow *grown=realloc(rows,(size_t)cap*sizeof(*rows));if(!grown)break;rows=grown;}
  PhRow *r=&rows[n++];r->route=strdup(route);r->title=strdup(title);r->lede=strdup((const char*)sqlite3_column_text(st,2));r->subject=strdup((const char*)sqlite3_column_text(st,3));r->date=strdup((const char*)sqlite3_column_text(st,4));r->host=strdup(host);r->by=strdup((const char*)sqlite3_column_text(st,6));}
 sqlite3_finalize(st);
 if(!n){free(rows);return 0;}
 ui_start(stdout,"section");ui_attr(stdout,"class","ph-feed");ui_flag(stdout,"data-profile-feed");ui_gt(stdout);ui_open(stdout,"header","ph-feed-head");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ph-eyebrow");ui_copy(stdout,"Published writing");ui_close(stdout,"p");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Writing and work");ui_close(stdout,"h2");ui_close(stdout,"div");ui_open(stdout,"p","ph-feed-note");printf("%d %s",total,total==1?"article":"articles");fputs("</p></header><div class=\"ph-chips\" role=\"group\" aria-label=\"Filter by subject\"><button type=\"button\" data-subject=\"\" aria-pressed=\"true\">All <span>",stdout);printf("%d",n);fputs("</span></button>",stdout);
 for(int i=0;i<n;i++){const char *s=rows[i].subject;if(!*s)continue;int seen=0,c=0;for(int j=0;j<i;j++)if(!strcmp(rows[j].subject,s)){seen=1;break;}if(seen)continue;for(int j=i;j<n;j++)if(!strcmp(rows[j].subject,s))c++;fputs("<button type=\"button\" aria-pressed=\"false\" data-subject=\"",stdout);html(s);fputs("\">",stdout);html(s);fputs(" <span>",stdout);printf("%d",c);fputs("</span></button>",stdout);}
 ui_close(stdout,"div");ui_open(stdout,"p","ph-callout");ui_copy(stdout,"Only released pages appear here. Drafts, private saves and account history stay off this page.");ui_close(stdout,"p");ui_open(stdout,"ol","ph-rows");
 for(int i=0;i<n;i++){PhRow *r=&rows[i];int here=current_host&&!strcmp(current_host,r->host);ui_start(stdout,"li");ui_attr(stdout,"class","ph-row");ui_attr_open(stdout,"data-subject");html(r->subject);fputs("\"><span class=\"ph-row-n\">",stdout);printf("%02d",i+1);ui_close(stdout,"span");ui_open(stdout,"div","ph-row-body");ui_open(stdout,"p","ph-row-meta");ui_open(stdout,"b",NULL);html(*r->subject?r->subject:"Journal");ui_close(stdout,"b");ui_open(stdout,"span",NULL);html(r->host);ui_close(stdout,"span");ui_start(stdout,"time");ui_attr_open(stdout,"datetime");html(r->date);fputs("\">",stdout);ph_short_date(r->date);ui_close(stdout,"time");ui_close(stdout,"p");ui_open(stdout,"h3",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");if(!here){fputs("https://",stdout);html(r->host);}html(r->route);fputs("\">",stdout);html(r->title);ui_close(stdout,"a");ui_close(stdout,"h3");if(*r->lede){ui_text(stdout, "p", "ph-row-lede", r->lede);}ui_open(stdout,"p","ph-row-tags");ui_open(stdout,"span","ph-tag");ui_copy(stdout,"Article");ui_close(stdout,"span");if(*r->by){ui_open(stdout,"span",NULL);ui_copy(stdout,"By ");html(r->by);ui_close(stdout,"span");}ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"li");
  free(r->route);free(r->title);free(r->lede);free(r->subject);free(r->date);free(r->host);free(r->by);}
 free(rows);ui_close(stdout,"ol");
 if(total>n){ui_start(stdout,"a");ui_attr(stdout,"class","ph-more");ui_attr_open(stdout,"href");ph_url(site,"posts",owner);fputs("\">Explore all ",stdout);printf("%d",total);fputs(" articles <span aria-hidden=\"true\">&rarr;</span></a>",stdout);}
 fputs("<p class=\"ph-chip-empty\" hidden>No articles on this page match that subject.</p></section><script>(()=>{for(const root of document.querySelectorAll('[data-profile-feed]')){const bs=[...root.querySelectorAll('.ph-chips button')],rows=[...root.querySelectorAll('.ph-row')],empty=root.querySelector('.ph-chip-empty');for(const b of bs)b.addEventListener('click',()=>{const s=b.dataset.subject;let shown=0;for(const x of bs)x.setAttribute('aria-pressed',String(x===b));for(const r of rows){const ok=!s||r.dataset.subject===s;r.hidden=!ok;if(ok)shown++;}empty.hidden=shown!==0;});}})();</script>",stdout);
 return total;}
static void ph_posts(sqlite3 *db,const PhSite *site,int owner,long long cursor,int limit){sqlite3_stmt *st=NULL;int count=0,more=0;long long last=0;if(!site){ui_open(stdout,"div","ph-empty");ui_open(stdout,"p",NULL);ui_copy(stdout,"Choose one of your sites to see what you have published there.");ui_close(stdout,"p");ui_close(stdout,"div");return;}int awaiting=owner?ph_review_pages(db,site):0;int composed=0;if(limit>6){composed=ph_journal_articles(db,site,owner);}ph_work_examples(db,site);int articles=composed?composed:ph_feed(db,site,limit<=6?8:400,site->host,owner);
 const char *sql="SELECT id,title,body,COALESCE(subject_address,''),published_at,post FROM community_post WHERE namespace=?1 AND community=?1 AND author_subject=?2 AND visibility='public' AND moderation_state='accepted' AND status='published' AND (?3=0 OR id<?3) ORDER BY id DESC LIMIT ?4";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,site->actor,-1,SQLITE_STATIC);sqlite3_bind_int64(st,3,cursor);sqlite3_bind_int(st,4,limit+1);while(sqlite3_step(st)==SQLITE_ROW){if(count==limit){more=1;break;}count++;last=sqlite3_column_int64(st,0);ui_start(stdout,"article");ui_attr(stdout,"class","ph-post");ui_flag(stdout,"data-public-post");ui_gt(stdout);ui_open(stdout,"time",NULL);html((const char*)sqlite3_column_text(st,4));ui_close(stdout,"time");ui_open(stdout,"h3",NULL);html((const char*)sqlite3_column_text(st,1));ui_close(stdout,"h3");ui_open(stdout,"p",NULL);html((const char*)sqlite3_column_text(st,2));ui_close(stdout,"p");const char *href=(const char*)sqlite3_column_text(st,3);if(href&&*href&&ph_link(href)){ui_start(stdout,"a");ui_attr_open(stdout,"href");html(href);fputs("\" rel=\"noopener noreferrer\">Read the linked source \u2197</a>",stdout);}if(owner){fputs("<details><summary>Manage this post</summary><p>Taking it off this site does not remove private saved material or posts on your other sites.</p><form action=\"/profile/withdraw\" method=\"post\">",stdout);ph_hidden("site",site->id);char id[32];snprintf(id,sizeof(id),"%lld",last);ph_hidden("id",id);ph_hidden("expected",(const char*)sqlite3_column_text(st,5));fputs("<button type=\"submit\">Remove from this site</button></form></details>",stdout);}ui_close(stdout,"article");}}sqlite3_finalize(st);if(!count&&!articles&&!composed){ui_open(stdout,"div","ph-empty");ui_open(stdout,"h2",NULL);fputs(owner&&awaiting?"Not yet released":owner?"Make this space yours":"Nothing published here yet",stdout);ui_close(stdout,"h2");ui_open(stdout,"p",NULL);fputs(owner&&awaiting?"Your private editorial desk above has articles awaiting review. Publish a reviewed edition to make it appear here.":owner?"Write a post, or choose a saved link and add your own introduction. Your other sites stay unchanged.":"This page will show what its owner chooses to publish here.",stdout);ui_close(stdout,"p");ui_close(stdout,"div");}if(more){ui_start(stdout,"a");ui_attr(stdout,"class","ph-next");ui_attr_open(stdout,"href");ph_url(site,"posts",owner);printf("&amp;cursor=%lld\">Earlier posts</a>",last);}}
static void ph_compose(sqlite3 *db,long uid,const PhSite *site,const char *item,const char *ref){char title[768]="",href[1401]="",nonce[33];ph_nonce(nonce);if(item&&*item&&ref&&*ref){ui_text(stdout, "p", NULL, "Choose one source at a time.");return;}if(item&&*item&&!ph_saved(db,uid,item,title,href)){ui_text(stdout, "p", NULL, "This saved source is no longer available to your account.");return;}if(ref&&*ref&&!ph_retained_link(db,ref,title,href)){ui_text(stdout, "p", NULL, "This retained item is not available from a released source. Open the current source before writing.");return;}ui_open(stdout,"h2",NULL);ui_copy(stdout,"Write for this site");ui_close(stdout,"h2");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Your post will appear only on ");html(site->host);fputs(". Other sites and private saves stay separate.</p><form class=\"ph-form\" action=\"/profile/preview\" method=\"post\">",stdout);ph_hidden("site",site->id);ph_hidden("nonce",nonce);if(item&&*item)ph_hidden("item",item);if(ref&&*ref)ph_hidden("ref",ref);fputs("<label>Post title<input name=\"title\" required maxlength=\"160\" value=\"",stdout);char short_title[161];snprintf(short_title,sizeof(short_title),"%.*s",160,title);html(short_title);fputs("\"></label><label>Your words<textarea name=\"text\" required maxlength=\"6000\" rows=\"8\" placeholder=\"Add your own introduction, update, or story.\"></textarea></label>",stdout);if((item&&*item)||(ref&&*ref)){ui_open(stdout,"p",NULL);ui_copy(stdout,"Linked source: ");html(href);ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Only the exact source link is shared. No private excerpt or retained body is copied into this public post.");ui_close(stdout,"p");ph_hidden("href",href);}else fputs("<label>Public link (optional)<input type=\"url\" name=\"href\" maxlength=\"1400\" placeholder=\"https://\"></label>",stdout);fputs("<button type=\"submit\">Review public post</button></form>",stdout);}
static int ph_interest_label(const char *in,char *out,size_t cap){
 size_t n=0;int space=1;
 if(!in||!out||cap<2)return 0;
 for(const unsigned char *p=(const unsigned char *)in;*p;p++){
  unsigned char c=*p;
  if(c>='A'&&c<='Z')c+='a'-'A';
  if((c>='a'&&c<='z')||(c>='0'&&c<='9')){if(n+1>=cap)return 0;out[n++]=c;space=0;}
  else if(c==' '||c=='\t'){if(!space&&n){if(n+1>=cap)return 0;out[n++]=' ';space=1;}}
  else return 0;
 }
 if(space&&n)n--;
 out[n]=0;
 return n>=2&&n<=40&&out[0]>='a'&&out[0]<='z';
}
static int ph_mutate(const char *body,const char *raw,const char *host,const StudioViewer *who,const char *actor,sqlite3 *db,const BfRequest *route){char siteid[128]="",title[161]="",text[6001]="",href[1401]="",nonce[33]="",expected[200]="",item[24]="",ref[132]="",id[24]="",label[64]="",question[16]="",choice[8]="",qweight[4]="",template[65]="";PhField fields[]={{"site",siteid,sizeof(siteid),0},{"title",title,sizeof(title),0},{"text",text,sizeof(text),1},{"href",href,sizeof(href),0},{"nonce",nonce,sizeof(nonce),0},{"expected",expected,sizeof(expected),0},{"item",item,sizeof(item),0},{"ref",ref,sizeof(ref),0},{"id",id,sizeof(id),0},{"label",label,sizeof(label),0},{"question",question,sizeof(question),0},{"choice",choice,sizeof(choice),0},{"weight",qweight,sizeof(qweight),0},{"template",template,sizeof(template),0}};
 if(!ph_origin(raw,host)||!ph_fields(body,fields,14,40000)){ph_error(403,"Review and submit this form from its own profile page.\n");return 1;}PhSite site;if(!ph_site(db,NULL,siteid,&site)||strcmp(site.actor,actor)){ph_error(404,"This profile site is not available to this account.\n");return 1;}
 char payload[56000],output[65],post[200];size_t used=0;
 if(bf_request_is(route,"profile_cover_remove")){
  if(!ph_digits(id)||strlen(expected)!=64||strspn(expected,"0123456789abcdef")!=64){ph_error(400,"Reload the cover before removing it.\n");return 1;}
  errno=0;char *end=NULL;long rowid=strtol(id,&end,10);if(errno||!end||*end||rowid<1||rowid>INT_MAX){ph_error(400,"Choose a cover on this site.\n");return 1;}
  sqlite3_stmt *st=NULL;char coordinate[160],actual[65];int valid=0;
  snprintf(coordinate,sizeof(coordinate),"profile-cover-%s",site.id);
  if(sqlite3_prepare_v2(db,"SELECT 1 FROM site_fragment WHERE id=?1 AND namespace=?2 AND coordinate=?3 AND kind='profile-cover' AND status='published'",-1,&st,NULL)==SQLITE_OK){
   sqlite3_bind_int(st,1,(int)rowid);sqlite3_bind_text(st,2,site.id,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,coordinate,-1,SQLITE_STATIC);valid=sqlite3_step(st)==SQLITE_ROW;
  }
  sqlite3_finalize(st);st=NULL;
  if(!valid||!cms_snapshot_row(db,"site_fragment",(int)rowid,site.id,actual)||strcmp(actual,expected)){ph_error(409,"This cover changed. Reload the profile before removing it.\n");return 1;}
  if(!ph_cms_cover_remove(id,site.id,expected)){ph_error(409,"The cover could not be removed. Reload the profile before retrying.\n");return 1;}
  int removed=0;if(sqlite3_prepare_v2(db,"SELECT status FROM site_fragment WHERE id=?1 AND namespace=?2 AND coordinate=?3",-1,&st,NULL)==SQLITE_OK){
   sqlite3_bind_int(st,1,(int)rowid);sqlite3_bind_text(st,2,site.id,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,coordinate,-1,SQLITE_STATIC);
   if(sqlite3_step(st)==SQLITE_ROW){const char *state=(const char*)sqlite3_column_text(st,0);removed=state&&!strcmp(state,"archived");}
  }
  sqlite3_finalize(st);if(!removed){ph_error(503,"Cover removal was not confirmed. Reload the profile.\n");return 1;}
  ph_redirect(&site,"overview");return 1;
 }
 if(bf_request_is(route,"profile_edit")){if(!studio_safe_title(title)||strlen(text)>6000){ph_error(400,"Use a profile name and introduction within the displayed limits.\n");return 1;}ph_hash(&site,output);if(strcmp(output,expected)){ph_error(409,"This profile changed. Reload its current name and introduction before editing.\n");return 1;}used=1;payload[0]='{';page_json_field(payload,sizeof(payload),&used,"heading",title,0);page_json_field(payload,sizeof(payload),&used,"lede",text,site.about_id?1:0);if(!site.about_id){page_json_field(payload,sizeof(payload),&used,"route","/",0);page_json_field(payload,sizeof(payload),&used,"title",title,0);page_json_field(payload,sizeof(payload),&used,"status","published",1);}payload[used++]='}';payload[used]=0;char record[32];snprintf(record,sizeof(record),"%ld",site.about_id);int ok=ph_cms(site.about_id?"update":"create","site_page",site.about_id?record:NULL,payload,site.id,site.about_id?"heading":NULL,site.name,site.about_id?"lede":NULL,site.bio);PhSite current;if(!ph_site(db,NULL,site.id,&current)||strcmp(current.name,title)||strcmp(current.bio,text)){ph_error(ok?503:409,"The profile change was not confirmed. Reload this site before resubmitting.\n");return 1;}ph_redirect(&site,"about");return 1;}
 if(bf_request_is(route,"profile_withdraw")){if(!ph_digits(id)||!*expected){ph_error(400,"Choose an exact post from this site.\n");return 1;}sqlite3_stmt *st=NULL;int valid=0;if(sqlite3_prepare_v2(db,"SELECT 1 FROM community_post WHERE id=?1 AND namespace=?2 AND community=?2 AND author_subject=?3 AND post=?4 AND status IN ('published','archived')",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_int64(st,1,strtoll(id,NULL,10));sqlite3_bind_text(st,2,site.id,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,actor,-1,SQLITE_STATIC);sqlite3_bind_text(st,4,expected,-1,SQLITE_STATIC);valid=sqlite3_step(st)==SQLITE_ROW;}sqlite3_finalize(st);if(!valid){ph_error(404,"That post is not part of this profile.\n");return 1;}ph_cms("update","community_post",id,"{\"status\":\"archived\"}",site.id,"community",site.id,"post",expected);st=NULL;int gone=0;if(sqlite3_prepare_v2(db,"SELECT status FROM community_post WHERE id=?1 AND namespace=?2",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_int64(st,1,strtoll(id,NULL,10));sqlite3_bind_text(st,2,site.id,-1,SQLITE_STATIC);gone=sqlite3_step(st)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(st,0),"archived");}sqlite3_finalize(st);if(!gone){ph_error(503,"Withdrawal is not confirmed. Read the current post before retrying.\n");return 1;}ph_redirect(&site,"posts");return 1;}
 if(bf_request_is(route,"profile_interests_add")||bf_request_is(route,"profile_interests_remove")){
  char clean[41]="",iid[32]="",date[40]="";
  if(!ph_interest_label(label,clean,sizeof(clean))){ph_error(400,"Use 2 to 40 letters, digits and spaces starting with a letter.\n");return 1;}
  {
   /* Deterministic per (actor,label): re-adding an interest is idempotent
    * and removal needs no separate identity. */
   char material[256], hex[65];
   int mlen = snprintf(material, sizeof(material), "%s\n%s", actor, clean);
   if (mlen < 0 || (size_t)mlen >= sizeof(material)) {
    ph_error(400, "Use a shorter interest label.\n");
    return 1;
   }
   bf_sha256_hex((const unsigned char *)material, (size_t)mlen, hex);
   snprintf(iid, sizeof(iid), "int-%.16s", hex);
  }
  sqlite3_stmt *st=NULL;long long rowid=0;int count=0;
  if(sqlite3_prepare_v2(db,"SELECT id,status FROM person_interest WHERE namespace='root' AND interest=?1 LIMIT 1",-1,&st,NULL)!=SQLITE_OK){ph_error(503,"The interest owner is unavailable.\n");return 1;}
  sqlite3_bind_text(st,1,iid,-1,SQLITE_STATIC);
  if(sqlite3_step(st)==SQLITE_ROW){rowid=sqlite3_column_int64(st,0);}
  sqlite3_finalize(st);
  if(bf_request_is(route,"profile_interests_remove")){
   if(!rowid){ph_redirect(&site,"edit");return 1;}
   char record[32];snprintf(record,sizeof(record),"%lld",rowid);
   ph_cms("update","person_interest",record,"{\"status\":\"archived\"}","root","interest",iid,"status","published");
   ph_redirect(&site,"edit");return 1;
  }
  if(rowid){ph_redirect(&site,"edit");return 1;}
  if(sqlite3_prepare_v2(db,"SELECT count(*) FROM person_interest WHERE namespace='root' AND actor_subject=?1 AND status='published'",-1,&st,NULL)!=SQLITE_OK){ph_error(503,"The interest owner is unavailable.\n");return 1;}
  sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);
  if(sqlite3_step(st)==SQLITE_ROW)count=sqlite3_column_int(st,0);
  sqlite3_finalize(st);
  if(count>=24){ph_error(400,"Keep at most 24 interests; remove one before adding.\n");return 1;}
  used=1;payload[0]='{';iso_utc_now(date,sizeof(date));
  page_json_field(payload,sizeof(payload),&used,"interest",iid,0);
  page_json_field(payload,sizeof(payload),&used,"actor_subject",actor,0);
  page_json_field(payload,sizeof(payload),&used,"label",clean,0);
  page_json_field(payload,sizeof(payload),&used,"weight","1",0);
  page_json_field(payload,sizeof(payload),&used,"created_at",date,0);
  page_json_field(payload,sizeof(payload),&used,"cannot_infer","A stated interest label. Overlap never implies availability, compatibility or consent.",0);
  page_json_field(payload,sizeof(payload),&used,"status","published",1);
  payload[used++]='}';payload[used]=0;
  ph_cms("create","person_interest",NULL,payload,"root",NULL,NULL,NULL,NULL);
  ph_redirect(&site,"edit");return 1;
 }
 if(bf_request_is(route,"profile_quiz_answer")||bf_request_is(route,"profile_quiz_clear")){
  /* Re-answering supersedes: reads take the latest published row, so an
   * answer is a new row, never an edit. Clearing archives every published
   * row for the question so no older answer resurfaces. */
  const BfQuizQuestion *q=NULL;
  unsigned qi=0,ci=0;
  char date[40]="";
  for(qi=0;qi<bf_quiz_bank_n;qi++)
   if(!strcmp(question,bf_quiz_bank[qi].key)){q=&bf_quiz_bank[qi];break;}
  if(!q){ph_error(400,"Answer one of the listed quiz questions.\n");return 1;}
  if(bf_request_is(route,"profile_quiz_clear")){
   sqlite3_stmt *st=NULL;
   if(sqlite3_prepare_v2(db,"SELECT id,answer FROM person_quiz_answer WHERE namespace='root' AND actor_subject=?1 AND question=?2 AND status='published'",-1,&st,NULL)!=SQLITE_OK){ph_error(503,"The quiz owner is unavailable.\n");return 1;}
   sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);
   sqlite3_bind_text(st,2,question,-1,SQLITE_STATIC);
   while(sqlite3_step(st)==SQLITE_ROW){
    char record[32];const char *aid=(const char*)sqlite3_column_text(st,1);
    snprintf(record,sizeof(record),"%lld",sqlite3_column_int64(st,0));
    if(aid&&*aid)ph_cms("update","person_quiz_answer",record,"{\"status\":\"archived\"}","root","answer",aid,"status","published");
   }
   sqlite3_finalize(st);
   ph_redirect(&site,"edit");return 1;
  }
  for(ci=0;ci<q->choices;ci++)
   if(q->choice_keys[ci]&&!strcmp(choice,q->choice_keys[ci]))break;
  if(ci>=q->choices||(strcmp(qweight,"1")&&strcmp(qweight,"2")&&strcmp(qweight,"3"))){ph_error(400,"Pick one of the listed answers and an importance from 1 to 3.\n");return 1;}
  {
   char hex[33],qid[24];
   ph_nonce(hex);
   snprintf(qid,sizeof(qid),"qa-%.16s",hex);
   used=1;payload[0]='{';iso_utc_now(date,sizeof(date));
   page_json_field(payload,sizeof(payload),&used,"answer",qid,0);
   page_json_field(payload,sizeof(payload),&used,"actor_subject",actor,0);
   page_json_field(payload,sizeof(payload),&used,"question",question,0);
   page_json_field(payload,sizeof(payload),&used,"choice",choice,0);
   page_json_field(payload,sizeof(payload),&used,"weight",qweight,0);
   page_json_field(payload,sizeof(payload),&used,"created_at",date,0);
   page_json_field(payload,sizeof(payload),&used,"cannot_infer","A stated quiz answer. Agreement never implies availability, compatibility or consent.",0);
   page_json_field(payload,sizeof(payload),&used,"status","published",1);
   payload[used++]='}';payload[used]=0;
   ph_cms("create","person_quiz_answer",NULL,payload,"root",NULL,NULL,NULL,NULL);
  }
  ph_redirect(&site,"edit");return 1;
 }
 if(bf_request_is(route,"profile_theme")){
  /* Site design is a profile-scope row pointing at a published template.
   * Latest published row wins, so choosing is always a create. */
  sqlite3_stmt *st=NULL;int known=0;
  char hex[33],spid[24],date[40]="";
  if(!bf_route_presentation_safe(template)){ph_error(400,"Choose one of the listed designs.\n");return 1;}
  if(sqlite3_prepare_v2(db,"SELECT 1 FROM presentation_template WHERE template=?1 AND status='published' LIMIT 1",-1,&st,NULL)!=SQLITE_OK){ph_error(503,"The design catalogue is unavailable.\n");return 1;}
  sqlite3_bind_text(st,1,template,-1,SQLITE_STATIC);
  known=sqlite3_step(st)==SQLITE_ROW;
  sqlite3_finalize(st);
  if(!known){ph_error(400,"Choose one of the listed designs.\n");return 1;}
  ph_nonce(hex);
  snprintf(spid,sizeof(spid),"sp-%.16s",hex);
  used=1;payload[0]='{';iso_utc_now(date,sizeof(date));
  page_json_field(payload,sizeof(payload),&used,"scope_presentation",spid,0);
  page_json_field(payload,sizeof(payload),&used,"scope","profile",0);
  page_json_field(payload,sizeof(payload),&used,"ref",site.id,0);
  page_json_field(payload,sizeof(payload),&used,"template",template,0);
  page_json_field(payload,sizeof(payload),&used,"created_at",date,0);
  page_json_field(payload,sizeof(payload),&used,"cannot_infer","A chosen site design. It changes presentation only.",0);
  page_json_field(payload,sizeof(payload),&used,"status","published",1);
  payload[used++]='}';payload[used]=0;
  ph_cms("create","scope_presentation",NULL,payload,"root",NULL,NULL,NULL,NULL);
  ph_redirect(&site,"edit");return 1;
 }
 if(!bf_request_is(route,"profile_preview")&&!bf_request_is(route,"profile_publish")){ph_error(404,"Unknown profile operation.\n");return 1;}
 if(!studio_safe_title(title)||!text[0]||strlen(nonce)!=32||strspn(nonce,"0123456789abcdef")!=32||!ph_link(href)){ph_error(400,"Review the post title, text and public link before publishing.\n");return 1;}if(*item){char original_title[768],original_href[1401];if(!ph_saved(db,who->id,item,original_title,original_href)||strcmp(original_href,href)){ph_error(409,"Your saved source changed. Review its current link before publishing.\n");return 1;}}if(*ref){char source_title[768],source_href[1401];if(*item||!ph_retained_link(db,ref,source_title,source_href)||strcmp(source_href,href)){ph_error(409,"The retained source changed or is no longer public. Reopen it before sharing.\n");return 1;}}
 if(bf_request_is(route,"profile_preview")){ph_begin(db,&site,site.name,1,"compose",actor);PhSite sites[16];int n=ph_sites(db,actor,sites);ph_nav(sites,n,&site,"posts",1);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Review before publishing");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"This post will be public on ");ui_open(stdout,"strong",NULL);html(site.host);ui_close(stdout,"strong");ui_copy(stdout," only.");ui_close(stdout,"p");ui_open(stdout,"article","ph-review");ui_open(stdout,"h3",NULL);html(title);ui_close(stdout,"h3");ui_open(stdout,"p",NULL);html(text);ui_close(stdout,"p");ui_open(stdout,"p",NULL);html(href);fputs("</p></article><form action=\"/profile/publish\" method=\"post\">",stdout);ph_hidden("site",site.id);ph_hidden("title",title);ph_hidden("text",text);ph_hidden("href",href);ph_hidden("nonce",nonce);if(*item)ph_hidden("item",item);if(*ref)ph_hidden("ref",ref);fputs("<button type=\"submit\">Publish on this site</button></form><p><a href=\"",stdout);ph_url(&site,"compose",1);fputs("\">Start a different post</a></p>",stdout);ph_end();return 1;}
 snprintf(post,sizeof(post),"profile:%s:%s",site.id,nonce);sqlite3_stmt *st=NULL;int existing=0,match=0;const char *find="SELECT title,body,COALESCE(subject_address,''),status,author_subject,community FROM community_post WHERE namespace=?1 AND post=?2 LIMIT 2";
 for(int pass=0;pass<2;pass++){if(sqlite3_prepare_v2(db,find,-1,&st,NULL)!=SQLITE_OK){ph_error(503,"The native post owner is unavailable.\n");return 1;}sqlite3_bind_text(st,1,site.id,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,post,-1,SQLITE_STATIC);if(sqlite3_step(st)==SQLITE_ROW){existing=1;match=!strcmp((const char*)sqlite3_column_text(st,0),title)&&!strcmp((const char*)sqlite3_column_text(st,1),text)&&!strcmp((const char*)sqlite3_column_text(st,2),href)&&!strcmp((const char*)sqlite3_column_text(st,3),"published")&&!strcmp((const char*)sqlite3_column_text(st,4),actor)&&!strcmp((const char*)sqlite3_column_text(st,5),site.id);if(sqlite3_step(st)==SQLITE_ROW)match=0;}sqlite3_finalize(st);st=NULL;if(existing)break;if(pass)break;
 used=1;payload[0]='{';char date[40];iso_utc_now(date,sizeof(date));const char *keys[]={"post","community","author_subject","title","body","subject_address","published_at","visibility","moderation_state","cannot_infer","status"};const char *values[]={post,site.id,actor,title,text,href,date,"public","accepted","An explicitly published profile post. Saving a source did not publish it or grant rights to its underlying contents.","published"};for(int i=0;i<11;i++)page_json_field(payload,sizeof(payload),&used,keys[i],values[i],i==10);payload[used++]='}';payload[used]=0;ph_cms("create","community_post",NULL,payload,site.id,NULL,NULL,NULL,NULL);}
 if(!existing||!match){ph_error(existing?409:503,existing?"This publish identity already belongs to a different or withdrawn post.\n":"The publication is not confirmed. Keep the reviewed request and read this site's posts before repeating it.\n");return 1;}ph_redirect(&site,"posts");return 1;}
#include "profile_gallery.h"
#include "profile_dating_web.h"
/* A member's journal article at /blog/<slug>/ on their own host. The site
 * scope is the profile apex's: a published member site with Matrix
 * presence. The article is a released site_page plus its showcase/public
 * fragments, composed by the shared native projector -- the same units
 * that render the CMS release, with journal prose through the reviewed
 * Markdown stage. Style is the mature journal sheets plus the
 * authored-reader sheet for component sections; no new style system.
 * Related links are the newest published siblings, a deterministic
 * fallback until a recommender exists. Cannot infer: a served article ->
 * the intake journal was receipted, current, or rights-cleared. */
static int ph_blog_slug(const char *target,char *slug,size_t cap){
 const char *s=target+6;size_t len=strlen(s);
 if(len&&s[len-1]=='/')len--;
 if(!len||len>96||len>=cap)return 0;
 if(!((s[0]>='a'&&s[0]<='z')||(s[0]>='0'&&s[0]<='9')))return 0;
 for(size_t i=1;i<len;i++)
  if(!((s[i]>='a'&&s[i]<='z')||(s[i]>='0'&&s[i]<='9')||s[i]=='-'))return 0;
 memcpy(slug,s,len);slug[len]=0;return 1;
}
typedef struct {char title[221],heading[221],lede[5001],eyebrow[121],published[11],author[160];} PhArticle;
static int ph_page_has_date(sqlite3 *db){
 sqlite3_stmt *st=NULL;int found=0;
 if(sqlite3_prepare_v2(db,"SELECT 1 FROM pragma_table_info('site_page') WHERE name='editorial_published_on' LIMIT 1",-1,&st,NULL)!=SQLITE_OK)return 0;
 found=sqlite3_step(st)==SQLITE_ROW;sqlite3_finalize(st);return found;
}
static int ph_article(sqlite3 *db,const char *ns,const char *route,PhArticle *a,int dated){
 sqlite3_stmt *st=NULL;int count=0;memset(a,0,sizeof(*a));
 const char *sql=dated?
  "SELECT title,heading,COALESCE(lede,''),COALESCE(eyebrow,''),COALESCE(editorial_published_on,''),COALESCE(author_credit,'') FROM site_page WHERE namespace=?1 AND route=?2 AND status='published' LIMIT 2":
  "SELECT title,heading,COALESCE(lede,''),COALESCE(eyebrow,''),'',COALESCE(author_credit,'') FROM site_page WHERE namespace=?1 AND route=?2 AND status='published' LIMIT 2";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
 char *out[]={a->title,a->heading,a->lede,a->eyebrow,a->published,a->author};
 size_t cap[]={sizeof(a->title),sizeof(a->heading),sizeof(a->lede),sizeof(a->eyebrow),sizeof(a->published),sizeof(a->author)};
 while(sqlite3_step(st)==SQLITE_ROW){
  if(++count!=1)break;
  for(int i=0;i<6;i++){const char *s=(const char*)sqlite3_column_text(st,i);
   if(!s||strlen(s)>=cap[i]){count=0;break;}
   snprintf(out[i],cap[i],"%s",s);}
  if(!count)break;}
 sqlite3_finalize(st);
 return count==1&&a->title[0]&&a->heading[0];
}
static long ph_read_mins(sqlite3 *db,const char *ns,const char *route){
 sqlite3_stmt *st=NULL;long long chars=0;
 if(sqlite3_prepare_v2(db,"SELECT COALESCE(sum(length(COALESCE(heading,''))+length(COALESCE(body,''))),0) FROM site_fragment WHERE namespace=?1 AND page=?2 AND slot='showcase' AND audience='public' AND status='published'",-1,&st,NULL)!=SQLITE_OK)return 1;
 sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
 if(sqlite3_step(st)==SQLITE_ROW)chars=sqlite3_column_int64(st,0);
 sqlite3_finalize(st);
 if(chars<0)chars=0;
 long long mins=(chars/5+100)/200;
 return mins<1?1:(long)mins;
}
static int ph_date_long(const char *iso,char *out,size_t cap){
 static const char *months[]={"January","February","March","April","May","June","July","August","September","October","November","December"};
 static const int days[]={31,28,31,30,31,30,31,31,30,31,30,31};
 int y=0,m=0,d=0;
 if(!iso||strlen(iso)!=10||iso[4]!='-'||iso[7]!='-'||sscanf(iso,"%4d-%2d-%2d",&y,&m,&d)!=3)return 0;
 if(y<1900||y>2100||m<1||m>12||d<1)return 0;
 int leap=(m==2&&(y%4==0&&(y%100!=0||y%400==0)));
 if(d>days[m-1]+leap)return 0;
 int n=snprintf(out,cap,"%s %d, %d",months[m-1],d,y);
 return n>0&&(size_t)n<cap;
}
/* Keep reading: the newest other articles, a plain ruled list. */
static void ph_blog_related(sqlite3 *db,const char *ns,const char *route,int dated){
 sqlite3_stmt *st=NULL;
 const char *sql=dated?
  "SELECT route,title,COALESCE(eyebrow,'') FROM site_page WHERE namespace=?1 AND route LIKE '/blog/%' AND route<>?2 AND status='published' ORDER BY editorial_published_on IS NULL,editorial_published_on DESC,route LIMIT 4":
  "SELECT route,title,COALESCE(eyebrow,'') FROM site_page WHERE namespace=?1 AND route LIKE '/blog/%' AND route<>?2 AND status='published' ORDER BY route LIMIT 4";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return;
 sqlite3_bind_text(st,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,route,-1,SQLITE_STATIC);
 int count=0;
 while(count<4&&sqlite3_step(st)==SQLITE_ROW){
  const char *r=(const char*)sqlite3_column_text(st,0),*t=(const char*)sqlite3_column_text(st,1),*c=(const char*)sqlite3_column_text(st,2);
  if(!r||!t||!*t||strlen(r)>=128||strlen(t)>=221)continue;
  if(strncmp(r,"/blog/",6)||strstr(r,"..")||strpbrk(r," \t\r\n\"<>\\"))continue;
  if(!count){ui_start(stdout,"nav");ui_attr(stdout,"class","ak-zen-next");ui_attr(stdout,"aria-labelledby","zen-next-h");ui_gt(stdout);ui_start(stdout,"h2");ui_attr(stdout,"id","zen-next-h");ui_gt(stdout);ui_copy(stdout,"Keep reading");ui_close(stdout,"h2");ui_open(stdout,"ol",NULL);}
  ui_open(stdout,"li",NULL);ui_open(stdout,"span",NULL);html(c&&*c?c:"Journal");ui_close(stdout,"span");ui_start(stdout,"a");ui_attr_open(stdout,"href");html(r);fputs("\">",stdout);html(t);ui_close(stdout,"a");ui_close(stdout,"li");count++;}
 sqlite3_finalize(st);
 if(count){ui_close(stdout,"ol");ui_start(stdout,"a");ui_attr(stdout,"class","fd-open");ui_attr(stdout,"href","/blog/");ui_gt(stdout);ui_copy(stdout,"All articles ");ui_start(stdout,"span");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);ui_copy(stdout,"&rsaquo;");ui_close(stdout,"span");ui_close(stdout,"a");ui_close(stdout,"nav");}
}
static void ph_shell_open(const char *title,const char *description,const char *canonical,const char *body_class);
static void ph_shell_close(void);
/* The article in a quiet single column: a reading bar (back, subject, size,
 * read aloud), the title and byline, the composed sections, then a short
 * list of what to read next. */
static int ph_blog(sqlite3 *db,const PhSite *site,const char *slug,const char *host){
 char route[128];if(snprintf(route,sizeof(route),"/blog/%s/",slug)>=(int)sizeof(route))return 0;
 if(!db||!site||!host){ph_error(503,"The journal store is unavailable.\n");return 1;}
 PhArticle art;int dated=ph_page_has_date(db);
 PhPublicationPlacement placement={0};int placed=0;char *body=NULL;
 const char *origin_site=site->id,*origin_route=route;
 if(!ph_article(db,origin_site,origin_route,&art,dated)){
  if(!ph_placement_resolve(db,site,route,&placement,&body)){ph_error(404,"This journal article is not published on this site.\n");return 1;}
  placed=1;origin_site=placement.origin_site;origin_route=placement.origin_route;
  if(!ph_article(db,origin_site,origin_route,&art,dated)){free(body);ph_error(404,"This journal article is no longer published by its source.\n");return 1;}
 }
 if(!body)body=malloc(BF_SITE_AUTHORED_BUFFER_CAP);
 if(!body){ph_error(503,"The journal store is unavailable.\n");return 1;}
 int blocks=0,projected=placed?1:bf_site_authored_document_project(db,origin_site,origin_route,body,BF_SITE_AUTHORED_BUFFER_CAP,&blocks);
 if(projected!=1){free(body);
  if(!projected){ph_error(404,"This journal article is not published on this site.\n");return 1;}
  ph_error(500,"This journal article cannot be composed safely.\n");return 1;}
 long mins=ph_read_mins(db,origin_site,origin_route);
 char datelong[32]="";int hasdate=art.published[0]&&ph_date_long(art.published,datelong,sizeof(datelong));
 char canonical[352];snprintf(canonical,sizeof(canonical),"https://%s/blog/%s/",host,slug);
 const char *by=art.author[0]?art.author:placed?placement.author_credit:site->name;
 ph_shell_open(art.title,art.lede,canonical,"ak-journal-article");
 ui_open(stdout,"div","ak-zen-bar");ui_start(stdout,"a");ui_attr(stdout,"class","ak-zen-back");ui_attr(stdout,"href","/blog/");ui_gt(stdout);ui_start(stdout,"span");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);ui_copy(stdout,"&larr;");ui_close(stdout,"span");ui_copy(stdout," Journal");ui_close(stdout,"a");ui_open(stdout,"p",NULL);
 html(art.eyebrow[0]?art.eyebrow:"Journal");printf(" &middot; <span>%ld min read</span></p><div class=\"ak-zen-tools\" role=\"group\" aria-label=\"Reading\">",mins);
 fputs("<button type=\"button\" data-zen-size=\"-1\" aria-label=\"Smaller text\">A&minus;</button><button type=\"button\" data-zen-size=\"1\" aria-label=\"Larger text\">A+</button>"
       "<button type=\"button\" class=\"ak-zen-listen\" data-ak-track data-ak-default data-narrate=\".ak-zen-body\" data-title=\"",stdout);html(art.title);
 fputs("\" data-by=\"",stdout);html(by);fputs("\">",stdout);page_glyph("narration");fputs("<span>Read aloud</span></button></div></div>",stdout);
 ui_open(stdout,"article","ak-zen");ui_open(stdout,"header","ak-zen-head");ui_open(stdout,"p","ak-zen-kicker");ui_open(stdout,"span",NULL);html(art.eyebrow[0]?art.eyebrow:"Journal");ui_close(stdout,"span");
 if(hasdate){ui_text(stdout, "span", NULL, datelong);}
 ui_close(stdout,"p");ui_open(stdout,"div","ak-zen-text");ui_open(stdout,"h1",NULL);html(art.title);ui_close(stdout,"h1");
 if(art.lede[0]){ui_text(stdout, "p", "ak-zen-lede", art.lede);}
 ui_close(stdout,"div");ui_open(stdout,"p","ak-zen-by");ui_start(stdout,"span");ui_attr(stdout,"class","ak-zen-initials");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);
 {char initials[8]="";int n=0;for(const char *p=by;*p&&n<2;p++)if((p==by||p[-1]==' ')&&isalpha((unsigned char)*p))initials[n++]=(char)toupper((unsigned char)*p);initials[n]=0;html(initials);}
 ui_close(stdout,"span");ui_open(stdout,"span",NULL);html(by);ui_close(stdout,"span");
 if(hasdate){ui_open(stdout,"span","ak-zen-meta");html(datelong);printf(" &middot; %ld min read</span>",mins);}
 if(placed){ui_close(stdout,"p");ui_start(stdout,"p");ui_attr(stdout,"class","ak-zen-origin");ui_flag(stdout,"data-publisher-origin");ui_gt(stdout);ui_copy(stdout,"Originally published by ");html(placement.author_credit);fputs(" at <a rel=\"external noopener noreferrer\" href=\"https://",stdout);html(placement.origin_host);html(placement.origin_route);fputs("\">the publisher's original article</a></p>",stdout);}
 else ui_close(stdout,"p");
 ui_close(stdout,"header");ui_open(stdout,"div","ak-zen-body ak-zen-text");
 fwrite(body,1,strlen(body),stdout);ui_close(stdout,"div");
 free(body);
 ph_blog_related(db,site->id,route,dated);
 fputs("</article><script>(function(){var a=document.querySelector('.ak-zen'),s=1;try{s=+localStorage.getItem('ak:zen-size')||1}catch(e){}"
       "function set(){a.style.setProperty('--zen-scale',s)}set();document.querySelectorAll('[data-zen-size]').forEach(function(b){b.addEventListener('click',function(){"
       "s=Math.min(1.3,Math.max(.85,s+(+b.dataset.zenSize)*.075));set();try{localStorage.setItem('ak:zen-size',s)}catch(e){}})})})();</script>",stdout);
 ph_shell_close();
 return 1;
}
#include "profile_journal_index.h"
static void ph_interests_edit(sqlite3 *db,const PhSite *site){
 sqlite3_stmt *st=NULL;int count=0;
 ui_open(stdout,"h2",NULL);ui_copy(stdout,"Interests");ui_close(stdout,"h2");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Up to 24 labels. Shared interests rank discovery and nothing else; they never gatekeep who can see this profile.");ui_close(stdout,"p");ui_open(stdout,"ul","ph-interests");
 if(db&&sqlite3_prepare_v2(db,"SELECT label FROM person_interest WHERE namespace='root' AND actor_subject=?1 AND status='published' ORDER BY label LIMIT 24",-1,&st,NULL)==SQLITE_OK){
  sqlite3_bind_text(st,1,site->actor,-1,SQLITE_STATIC);
  while(sqlite3_step(st)==SQLITE_ROW){
   const char *label=(const char*)sqlite3_column_text(st,0);
   if(!label||!*label||strlen(label)>40)continue;
   count++;ui_open(stdout,"li",NULL);html(label);
   {const FtField hidden[]={{"site",site->id},{"label",label}};
    const FtSubmit go[]={{.label="Remove",.tone="quiet"}};
    const FtForm f={.id="ph-interest-remove",.action="/profile/interests/remove",.hook="interest-remove",.hidden=hidden,.nhidden=2,.submits=go,.nsubmits=1};
    ft_form(stdout,&f);}
   ui_close(stdout,"li");
  }
 }
 sqlite3_finalize(st);
 ui_close(stdout,"ul");
 {const FtField hidden[]={{"site",site->id}};
  static const FtInput in[]={{.label="Add an interest",.name="label",.maxlength="40",.required=1,.placeholder="road cycling"}};
  const FtForm f={.id="ph-interest-add",.action="/profile/interests/add",.label="Add interest",.hook="interest-add",.hidden=hidden,.nhidden=1,.inputs=in,.ninputs=1};
  ft_form(stdout,&f);}
 (void)count;
}
static void ph_quiz_edit(sqlite3 *db,const PhSite *site){
 /* Questions render as shared components: a card per question holding the
  * answer action, with the two choice groups inside the action's own form. */
 static const char *const wlabel[]={"matters a little","matters","matters a lot"};
 static const char *const wv[]={"1","2","3"};
 static const char *const wl[]={"Matters a little","Matters","Matters a lot"};
 BfAuthoringTree *tree=calloc(1,sizeof(*tree));
 unsigned qi=0;int ok=0;
 if(!db||!site||!tree)goto done;
 ok=ft_card(tree,"quiz",NULL,0,"Compatibility quiz","Eight original questions. Agreed answers rank discovery by the lower stated importance, capped at +6; interests stay the primary signal.",NULL,0);
 if(!ok)goto done;
 for(qi=0;qi<bf_quiz_bank_n;qi++){
  const BfQuizQuestion *q=&bf_quiz_bank[qi];
  char have[8]="",havew[4]="",wlabelled[24]="",body[700]="";
  char id[32],child[40];unsigned ci=0,wi=0,hi=0;
  const char *havetext=have;sqlite3_stmt *st=NULL;
  if(sqlite3_prepare_v2(db,"SELECT choice,weight FROM person_quiz_answer WHERE namespace='root' AND actor_subject=?1 AND question=?2 AND status='published' AND id=(SELECT MAX(id) FROM person_quiz_answer WHERE namespace='root' AND actor_subject=?1 AND question=?2 AND status='published')",-1,&st,NULL)!=SQLITE_OK)goto done;
  sqlite3_bind_text(st,1,site->actor,-1,SQLITE_STATIC);
  sqlite3_bind_text(st,2,q->key,-1,SQLITE_STATIC);
  if(sqlite3_step(st)==SQLITE_ROW){
   const char *c=(const char*)sqlite3_column_text(st,0),*ww=(const char*)sqlite3_column_text(st,1);
   if(c)snprintf(have,sizeof(have),"%s",c);
   if(ww)snprintf(havew,sizeof(havew),"%s",ww);
  }
  sqlite3_finalize(st);st=NULL;
  if(havew[0]>='1'&&havew[0]<='3'&&!havew[1])snprintf(wlabelled,sizeof(wlabelled),"%s",wlabel[havew[0]-'1']);
  for(hi=0;hi<q->choices;hi++)
   if(q->choice_keys[hi]&&!strcmp(have,q->choice_keys[hi])){havetext=q->choice_texts[hi];break;}
  if(have[0])snprintf(body,sizeof(body),"Now answering %s%s%s%s",havetext,wlabelled[0]?" (":"",wlabelled,wlabelled[0]?")":"");
  snprintf(id,sizeof(id),"quiz-%u",qi);
  if(!ft_card(tree,id,NULL,(int)qi+1,q->prompt,body,NULL,0))goto done;
  snprintf(child,sizeof(child),"quiz-%u-save",qi);
  {
   FtField fields[2];
   fields[0].name="site";fields[0].value=site->id;
   fields[1].name="question";fields[1].value=q->key;
   if(!ft_action(tree,child,id,0,"Save answer","post","/profile/quiz/answer",fields,2))goto done;
  }
  {
   char pick[40];FtOption opts[4];snprintf(pick,sizeof(pick),"quiz-%u-pick",qi);
   for(ci=0;ci<q->choices;ci++){opts[ci].value=q->choice_keys[ci];opts[ci].label=q->choice_texts[ci];opts[ci].checked=!strcmp(have,q->choice_keys[ci])||(!have[0]&&ci==0);}
   if(!ft_choice(tree,pick,child,0,"Answer","choice",opts,q->choices))goto done;
  }
  {
   char imp[40];FtOption wopts[3];snprintf(imp,sizeof(imp),"quiz-%u-weight",qi);
   for(wi=0;wi<3;wi++){wopts[wi].value=wv[wi];wopts[wi].label=wl[wi];wopts[wi].checked=!strcmp(havew,wv[wi])||(!havew[0]&&wi==1);}
   if(!ft_choice(tree,imp,child,1,"Importance","weight",wopts,3))goto done;
  }
  if(have[0]){
   char clear[40];FtField cfields[2];snprintf(clear,sizeof(clear),"quiz-%u-clear",qi);
   cfields[0].name="site";cfields[0].value=site->id;
   cfields[1].name="question";cfields[1].value=q->key;
   if(!ft_action(tree,clear,id,1,"Clear this answer","post","/profile/quiz/clear",cfields,2))goto done;
  }
 }
 ok=1;
done:
 if(tree){if(ok)ft_render(tree,stdout);else{bf_authoring_tree_clear(tree);free(tree);}}
}
static void ph_theme_edit(sqlite3 *db,const PhSite *site){
 /* Catalogue by family: one choice group per family inside one save form. */
 BfAuthoringTree *tree=calloc(1,sizeof(*tree));
 sqlite3_stmt *st=NULL,*cur=NULL;
 char current[65]="",family[65]="",gid[32]="";
 char oval[200][65],olabel[200][128];
 FtField fields[1];FtOption opts[200];unsigned nopts=0,fam=0;int ok=0;
 if(!db||!site||!tree)goto done;
 if(sqlite3_prepare_v2(db,"SELECT template FROM scope_presentation WHERE namespace='root' AND scope='profile' AND ref=?1 AND status='published' ORDER BY id DESC LIMIT 1",-1,&cur,NULL)==SQLITE_OK){
  sqlite3_bind_text(cur,1,site->id,-1,SQLITE_STATIC);
  if(sqlite3_step(cur)==SQLITE_ROW){const char *t=(const char*)sqlite3_column_text(cur,0);if(t)snprintf(current,sizeof(current),"%s",t);}
 }
 sqlite3_finalize(cur);
 ok=ft_card(tree,"theme",NULL,0,"Site design","One design dresses this whole site. Choosing saves a new selection; the newest one wins.",NULL,0);
 if(!ok)goto done;
 fields[0].name="site";fields[0].value=site->id;
 if(!ft_action(tree,"theme-save","theme",0,"Save design","post","/profile/theme",fields,1))goto done;
 ok=0;
 if(sqlite3_prepare_v2(db,"SELECT template,COALESCE(family,''),COALESCE(reference_name,'') FROM presentation_template WHERE namespace='root' AND status='published' ORDER BY family,template LIMIT 200",-1,&st,NULL)!=SQLITE_OK)goto done;
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *t=(const char*)sqlite3_column_text(st,0),*f=(const char*)sqlite3_column_text(st,1),*n=(const char*)sqlite3_column_text(st,2);
  if(!t||!*t)continue;
  if(strcmp(f?family:"",family)){
   if(nopts){snprintf(gid,sizeof(gid),"theme-f%u",fam++);if(!ft_choice(tree,gid,"theme-save",(int)fam-1,family[0]?family:"Design","template",opts,nopts))goto done;nopts=0;}
   snprintf(family,sizeof(family),"%s",f?f:"");
  }
  if(nopts>=200)continue;
  snprintf(oval[nopts],sizeof(oval[nopts]),"%s",t);
  snprintf(olabel[nopts],sizeof(olabel[nopts]),"%s",(n&&*n)?n:t);
  opts[nopts].value=oval[nopts];opts[nopts].label=olabel[nopts];opts[nopts].checked=!strcmp(current,t);nopts++;
 }
 sqlite3_finalize(st);st=NULL;
 if(nopts){snprintf(gid,sizeof(gid),"theme-f%u",fam++);if(!ft_choice(tree,gid,"theme-save",(int)fam-1,family[0]?family:"Design","template",opts,nopts))goto done;}
 ok=1;
done:
 if(st)sqlite3_finalize(st);
 if(tree){if(ok)ft_render(tree,stdout);else{bf_authoring_tree_clear(tree);free(tree);}}
}
static void ph_interests_public(sqlite3 *db,const PhSite *site,const char *viewer){
 sqlite3_stmt *st=NULL,*mine=NULL;int shown=0;
 char own[24][41];int nown=0;
 if(!db||!site)return;
 if(viewer&&*viewer&&sqlite3_prepare_v2(db,"SELECT label FROM person_interest WHERE namespace='root' AND actor_subject=?1 AND status='published' LIMIT 24",-1,&mine,NULL)==SQLITE_OK){
  sqlite3_bind_text(mine,1,viewer,-1,SQLITE_STATIC);
  while(nown<24&&sqlite3_step(mine)==SQLITE_ROW){
   const char *label=(const char*)sqlite3_column_text(mine,0);
   if(!label||!*label||strlen(label)>40)continue;
   snprintf(own[nown++],41,"%s",label);
  }
 }
 sqlite3_finalize(mine);
 if(sqlite3_prepare_v2(db,"SELECT label FROM person_interest WHERE namespace='root' AND actor_subject=?1 AND status='published' ORDER BY label LIMIT 24",-1,&st,NULL)!=SQLITE_OK)return;
 sqlite3_bind_text(st,1,site->actor,-1,SQLITE_STATIC);
 ui_start(stdout,"section");ui_attr(stdout,"class","ph-interests-public");ui_flag(stdout,"data-profile-interests");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Interests");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *label=(const char*)sqlite3_column_text(st,0);
  int shared=0;
  if(!label||!*label||strlen(label)>40)continue;
  for(int i=0;i<nown;i++)if(!strcmp(own[i],label)){shared=1;break;}
  shown++;
  ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/companions?q=");url_query_value(label);
  if(shared)fputs("\" data-shared=\"1\">",stdout);else fputs("\">",stdout);
  html(label);ui_close(stdout,"a");ui_copy(stdout," ");
 }
 sqlite3_finalize(st);
 if(!shown)fputs("No interests stated yet.",stdout);
 ui_close(stdout,"p");ui_close(stdout,"section");
}
#include "profile_connections_web.h"
static int serve_profile_home(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *body=r->body; const char *raw=r->raw; const char *host=r->host; const char *bearer=r->bearer; const char *auth_db=r->auth_db; const char *accept=r->accept;
    (void)method;(void)target;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;char blogslug[97];int connections_change=r->as&&!strncmp(r->as,"connections_",12),dating_change=r->as&&!strncmp(r->as,"dating_",7),private_view=bf_request_is(r,"profile"),mutation=connections_change||dating_change||(r->as&&!strncmp(r->as,"profile_",8)),apex=bf_request_is(r,"apex"),blog=bf_request_is(r,"post")&&ph_blog_slug(target,blogslug,sizeof(blogslug)),bloghome=bf_request_is(r,"journal");if(!private_view&&!mutation&&!apex&&!blog&&!bloghome)return 0;if(apex&&host_is_territory_apex(host))return 0;if(!wants_html(accept)&&!mutation&&!((apex||blog||bloghome||private_view)&&!wants_json(accept)&&(!accept||!*accept||strstr(accept,"*/*"))))return 0;
 char normalized[256];if(!lower_host(host,normalized))return 0;sqlite3 *db=cms_open_ro();PhSite site={0},sites[16];int count=0,have_site=0;StudioViewer who={0};char actor[192]="";int signed_in=session_identity(bearer,auth_db,&who)&&who.id>0;if(signed_in)ph_actor(who.id,auth_db,actor);
 if(apex){if(!ph_site(db,normalized,NULL,&site)){sqlite3_stmt *claim=NULL;int declared=0;if(db&&sqlite3_prepare_v2(db,"SELECT 1 FROM site_presentation WHERE namespace='root' AND host=?1 AND profile='member-profile-v1' LIMIT 1",-1,&claim,NULL)==SQLITE_OK){sqlite3_bind_text(claim,1,normalized,-1,SQLITE_STATIC);declared=sqlite3_step(claim)==SQLITE_ROW;}sqlite3_finalize(claim);if(db)sqlite3_close(db);if(declared){ph_error(404,"This profile site is not currently published.\n");return 1;}return 0;}have_site=1;}
 if(bloghome){if(strcmp(method,"GET")){ph_error(405,"Open the journal with GET.\n");if(db)sqlite3_close(db);return 1;}if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}int served=ph_blog_index(db,&site,normalized,query);sqlite3_close(db);return served;}
 if(blog){if(strcmp(method,"GET")){ph_error(405,"Open journal articles with GET.\n");if(db)sqlite3_close(db);return 1;}if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}int served=ph_blog(db,&site,blogslug,normalized);sqlite3_close(db);return served;}
 if(private_view||mutation){if(!signed_in){if(db)sqlite3_close(db);if(mutation)ph_error(401,"Sign in before changing a profile.\n");else if(!pj_profile_login_redirect(query))ph_error(400,"Choose a valid profile section before signing in.\n");return 1;}if(!db){ph_error(503,"The profile owner is unavailable.\n");return 1;}}
 if(mutation){if(strcmp(method,"POST")){ph_error(405,"Profile changes require a reviewed form submission.\n");sqlite3_close(db);return 1;}int result=bf_request_is(r,"profile_claim")?ph_claim(body,raw,host,&who,actor,db,r):
 connections_change?ph_connections_mutate(body,raw,host,actor,r):
 dating_change?ph_dating_mutate(body,raw,host,actor,r):
 ph_art_target(target)?ph_art_mutate(target,body,raw,host,actor,db):ph_mutate(body,raw,host,&who,actor,db,r);sqlite3_close(db);return result;}
 if(strcmp(method,"GET")){ph_error(405,"Open profiles with GET.\n");if(db)sqlite3_close(db);return 1;}
 char tab[24]="overview",selected[128]="",q[160]="",cursor[24]="",item[24]="",ref[132]="",kind[16]="",art[128]="",fx[8]="",fy[8]="",view[20]="discover",peer[192]="",before[24]="",facet[32]="";PhField params[]={{"tab",tab,sizeof(tab),0},{"site",selected,sizeof(selected),0},{"q",q,sizeof(q),0},{"cursor",cursor,sizeof(cursor),0},{"item",item,sizeof(item),0},{"ref",ref,sizeof(ref),0},{"kind",kind,sizeof(kind),0},{"art",art,sizeof(art),0},{"fx",fx,sizeof(fx),0},{"fy",fy,sizeof(fy),0},{"view",view,sizeof(view),0},{"peer",peer,sizeof(peer),0},{"before",before,sizeof(before),0},{"facet",facet,sizeof(facet),0}};
 if(!ph_fields(query,params,14,2048)||(*facet&&(strcmp(tab,"connections")||strlen(facet)>31||strspn(facet,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(facet)))||((!ak_app_tab(tab))&&(*peer||strcmp(view,"discover")))||(ak_app_tab(tab)&&(!ak_app_tab_view(ak_app_tab(tab),view)||(*peer&&(strcmp(view,ak_app_tab(tab)->peer_view)||!bf_dating_subject(peer)))))||(*before&&(!ak_app_tab(tab)||strcmp(view,ak_app_tab(tab)->peer_view)||!*peer||!ph_digits(before)))||((*art||*fx||*fy)&&strcmp(tab,"artwork"))||(*cursor&&!ph_digits(cursor))||(*item&&!ph_digits(item))||(*ref&&(strcmp(tab,"compose")||*item))||(*kind&&(!pj_kind(kind)||strcmp(tab,"activity")))||(!private_view&&(*selected||*item||*ref||*q||*kind||*art||!strcmp(tab,"artwork")||(!strcmp(tab,"saved")||!strcmp(tab,"sites")||!strcmp(tab,"edit")||!strcmp(tab,"compose")||!strcmp(tab,"activity")||!strcmp(tab,"connections")||ak_app_tab(tab))))){ph_error(400,"Choose a section on this exact profile.\n");if(db)sqlite3_close(db);return 1;}
 long long dating_before=0;
 if(*before){errno=0;char *end=NULL;dating_before=strtoll(before,&end,10);
  if(errno||!end||*end||dating_before<=0){ph_error(400,"Choose a valid message page.\n");if(db)sqlite3_close(db);return 1;}}
 const char *allowed[]={"overview","posts","saved","about","sites","edit","compose","communities","activity","gallery","artwork","organizations","dating","connections"};int valid=0;for(size_t i=0;i<14;i++)if(!strcmp(tab,allowed[i]))valid=1;if(!valid){ph_error(404,"This profile section does not exist.\n");if(db)sqlite3_close(db);return 1;}
 if(private_view){count=ph_sites(db,actor,sites);if(*selected){for(int i=0;i<count;i++)if(!strcmp(sites[i].id,selected)){site=sites[i];have_site=1;}if(!have_site){ph_error(404,"This site is not part of your profile.\n");sqlite3_close(db);return 1;}}else{for(int i=0;i<count;i++)if(!strcmp(sites[i].host,normalized)){site=sites[i];have_site=1;break;}if(!have_site)for(int i=0;i<count;i++)if(!strcmp(sites[i].territory,territory_of(host))){if(have_site){have_site=0;break;}site=sites[i];have_site=1;}}}
 else count=ph_sites(db,site.actor,sites);
 const char *name=have_site?site.name:who.name[0]?who.name:"Your profile";ph_begin(db,have_site?&site:NULL,name,private_view,tab,actor);ph_nav(sites,count,have_site?&site:NULL,tab,private_view);
 /* overview: the feed leads; worlds and interests follow it (see the overview branch below). */
 if(private_view&&!strcmp(tab,"overview")){ph_private_work(who.id);ph_journey(have_site?&site:NULL);ph_private_sites(db,sites,count);}
 if(!private_view&&signed_in&&actor[0]&&!strcmp(actor,site.actor)){ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"You are viewing the public page. Private saves are not shown here. ");ui_start(stdout,"a");ui_attr_open(stdout,"href");ph_url(&site,"overview",1);fputs("\">Open your profile</a></p>",stdout);}
 if(!private_view&&signed_in&&actor[0]&&strcmp(actor,site.actor)&&have_site){ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Visiting as ");html(actor+7);fputs(". <a href=\"/companions/report?target=",stdout);url_query_value(site.actor);fputs("\">Report this profile</a></p>",stdout);}
 if(private_view&&!strcmp(tab,"connections"))ph_connections_view(db,actor,*facet?facet:"personal");
 else if(private_view&&ak_app_tab(tab))ph_dating_view(db,actor,view,peer,dating_before);
 else if(!strcmp(tab,"activity")&&private_view){ph_private_work(who.id);pj_profile_list(db,who.id,have_site?site.id:"",pj_kind(kind),q,*cursor?strtoll(cursor,NULL,10):0);}
 else if(!strcmp(tab,"saved"))ph_saved_list(db,who.id,have_site?&site:NULL,q,*cursor?strtoll(cursor,NULL,10):0,24);
 else if(!strcmp(tab,"communities")){if(private_view)cjw_profile(db,who.id,q,cursor);else ph_public_communities(db,&site,auth_db);}
 else if(!strcmp(tab,"organizations")&&!private_view)ph_public_organizations();
 else if(!strcmp(tab,"sites")){if(count)ph_private_sites(db,sites,count);else if(actor[0]&&ph_claim_pending(db,actor)){ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your public sites");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your address ");html(actor+7);ui_copy(stdout," is waiting for review. Your saved material stays private until your pages open.");ui_close(stdout,"p");}else {ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your public sites");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"No published profile site is currently bound to this account. Claim one address and it opens in every territory once reviewed. Your saved material remains private.");ui_close(stdout,"p");ph_claim_form();}}
 else if(!strcmp(tab,"edit")&&have_site){char digest[65];ph_hash(&site,digest);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Edit this public profile");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Changes apply only to ");ui_open(stdout,"strong",NULL);html(site.host);fputs("</strong>.</p><form class=\"ph-form\" method=\"post\" action=\"/profile/edit\">",stdout);ph_hidden("site",site.id);ph_hidden("expected",digest);fputs("<label>Display name<input name=\"title\" maxlength=\"160\" required value=\"",stdout);html(site.name);fputs("\"></label><label>Introduction<textarea name=\"text\" maxlength=\"6000\" rows=\"7\">",stdout);html(site.bio);fputs("</textarea></label><button type=\"submit\">Publish profile changes</button></form>",stdout);ph_interests_edit(db,&site);ph_quiz_edit(db,&site);ph_theme_edit(db,&site);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Site pages");ui_close(stdout,"h2");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Compose this site's pages and sections in the shared page composer, opening this profile's own workspace.");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"class","ph-button");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/compose/advanced?site=");html(site.id);fputs("\">Open visual page editor</a></p>",stdout);}
 else if(!strcmp(tab,"compose")&&have_site)ph_compose(db,who.id,&site,item,ref);
 else if(!strcmp(tab,"artwork")&&private_view)ph_artwork_view(db,sites,count,have_site?&site:NULL,art,fx,fy);
 else if(!strcmp(tab,"gallery")){if(have_site)ph_gallery_emit(db,&site,private_view,PH_GALLERY_MAX,1);else {ui_open(stdout,"div","ph-empty");ui_open(stdout,"p",NULL);ui_copy(stdout,"Choose one of your sites to see its gallery.");ui_close(stdout,"p");ui_close(stdout,"div");}}
 else if(!strcmp(tab,"about")){ui_open(stdout,"h2",NULL);ui_copy(stdout,"About");ui_close(stdout,"h2");ui_open(stdout,"div","ph-post");ui_open(stdout,"p",NULL);html(have_site&&site.bio[0]?site.bio:private_view?"Add an introduction for this site from Edit this profile.":"No introduction has been published on this site.");ui_close(stdout,"p");ui_close(stdout,"div");}
 else{if(private_view){ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"This is your private profile view. ");if(have_site){fputs("You are managing <strong>",stdout);html(site.host);ui_close(stdout,"strong");ui_copy(stdout,". ");}fputs("Published posts below belong to this site; saved material belongs to your private account.</p>",stdout);}ph_posts(db,have_site?&site:NULL,private_view,*cursor?strtoll(cursor,NULL,10):0,!strcmp(tab,"overview")?6:24);if(have_site&&!strcmp(tab,"overview")){ph_gallery_emit(db,&site,private_view,7,0);ph_worlds_teaser(db,&site);ph_interests_public(db,&site,signed_in&&actor[0]?actor:NULL);}if(private_view&&!strcmp(tab,"overview"))ph_saved_list(db,who.id,have_site?&site:NULL,"",0,6);}
 ph_end();if(db)sqlite3_close(db);return 1;}
#endif
