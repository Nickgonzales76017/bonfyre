#ifndef BONFYRE_PROFILE_HOME_H
#define BONFYRE_PROFILE_HOME_H
#include "../../BonfyreCMS/src/account_world.h"
#include "site_authored_document.h"
#include "affinity_projection.h"
#include "route_presentation.h"
#include "form_tree.h"
#include "profile_journal_batch.h"
/* One profile presentation. Auth owns the reader; Matrix owns the address;
 * CMS owns site-specific pages/posts and the independent private saved library. */
typedef struct {char id[128],host[256],actor[192],territory[24],name[161],bio[6001];long about_id;} PhSite;
#include "profile_publication_placement.h"
typedef struct {const char *key;char *value;size_t cap;int multiline;} PhField;
static int ph_decode(const char *restrict p,size_t len,char *restrict out,size_t cap,int multiline){size_t n=0;for(size_t i=0;i<len;i++){unsigned char c=p[i];if(c=='%'){if(i+2>=len||psetup_hex(p[i+1])<0||psetup_hex(p[i+2])<0)return 0;c=(unsigned char)(psetup_hex(p[i+1])*16+psetup_hex(p[i+2]));i+=2;}else if(c=='+')c=' ';if(!c||c==127||(c<32&&(!multiline||(c!='\n'&&c!='\r'&&c!='\t')))||n+1>=cap)return 0;out[n++]=(char)c;}out[n]=0;return 1;}
static int ph_fields(const char *input,PhField *fields,size_t count,size_t limit){unsigned seen=0;if(!input||!*input)return 1;if(strlen(input)>limit)return 0;for(const char *p=input;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[32];if(!eq||!ph_decode(p,(size_t)(eq-p),key,sizeof(key),0))return 0;size_t i;for(i=0;i<count&&strcmp(key,fields[i].key);i++){}if(i==count||(seen&(1u<<i))||!ph_decode(eq+1,(size_t)(end-eq-1),fields[i].value,fields[i].cap,fields[i].multiline))return 0;seen|=1u<<i;p=*end?end+1:end;if(!*p&&*end)return 0;}return 1;}
static int ph_header(const char *raw,const char *name,char *out,size_t cap){int found=0;size_t key=strlen(name);for(const char *p=raw?strstr(raw,"\n"):NULL;p&&*++p&&*p!='\r'&&*p!='\n';p=strchr(p,'\n')){if(!strncasecmp(p,name,key)&&p[key]==':'){if(found++)return 0;const char *v=p+key+1;while(*v==' '||*v=='\t')v++;const char *end=strchr(v,'\r');if(!end)end=strchr(v,'\n');if(!end||(size_t)(end-v)>=cap)return 0;memcpy(out,v,(size_t)(end-v));out[end-v]=0;}}return found==1;}
static int ph_origin(const char *raw,const char *host){char origin[400],expected[400],type[100];if(!ph_header(raw,"Origin",origin,sizeof(origin))||!ph_header(raw,"Content-Type",type,sizeof(type))||strcmp(type,"application/x-www-form-urlencoded"))return 0;snprintf(expected,sizeof(expected),"%s://%s",host&&(!strncmp(host,"127.0.0.1",9)||!strncmp(host,"localhost",9))?"http":"https",host?host:"");return !strcmp(origin,expected);}
static void ph_error(int code,const char *message){protocol_reply(code,"text/plain; charset=utf-8",message);}
static int ph_actor(long uid,const char *auth_db,char out[192]){sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int count=0;out[0]=0;if(uid<1||!auth_db||sqlite3_open_v2(auth_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;if(sqlite3_prepare_v2(db,"SELECT actor_subject FROM actor_closure_bindings WHERE user_id=?1 AND state='active' LIMIT 2",-1,&st,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(st,1,uid);while(sqlite3_step(st)==SQLITE_ROW){const char *s=(const char*)sqlite3_column_text(st,0);if(++count!=1||!s||strncmp(s,"person:",7)||strlen(s)>=192||strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:._-")!=strlen(s)){count=0;break;}snprintf(out,192,"%s",s);}done:sqlite3_finalize(st);if(db)sqlite3_close(db);if(count!=1)out[0]=0;return count==1;}
static int ph_matrix(const PhSite *site){const char *path=getenv("BONFYRE_MATRIX_DB");sqlite3 *db=NULL;sqlite3_stmt *st=NULL;char institution[224],normalized[256];int ok=0;if(!path||!lower_host(site->host,normalized)||strcmp(site->host,normalized))return 0;snprintf(institution,sizeof(institution),"inst:%s",site->actor);for(size_t i=5;institution[i];i++)if(institution[i]==':'||institution[i]=='.')institution[i]='-';if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
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
static void ph_begin(sqlite3 *db,const PhSite *site,const char *name,int private_view,const char *tab,const char *actor){char pattern[65]={0},tmpl[65]={0};bf_presentation_resolve(NULL,NULL,NULL,site?site->id:NULL,NULL,actor,pattern,tmpl);fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>",stdout);html(name);fputs(" · Aurekai</title>",stdout);if(private_view)fputs("<meta name=\"robots\" content=\"noindex,nofollow\">",stdout);fputs("<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:opsz,wght@7..72,400;7..72,500&amp;display=swap\"><link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=4\"><link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\">",stdout);ph_journal_styles();if(tmpl[0]&&db)bf_site_presentation_emit_scoped(stdout,db,tmpl,"body[data-bf-template]");fputs("</head><body class=\"ak-app ph-app\"",stdout);if(tmpl[0]){fputs(" data-bf-template=\"",stdout);html(tmpl);fputs("\"",stdout);}fputs(">" BF_ESTATE_BAR "<main class=\"ph-shell\" data-profile-page data-profile-template=\"",stdout);fputs(site&&!site->actor[0]?"organization-subprofile-v1":"member-v1",stdout);fputs("\" data-profile-territory=\"",stdout);html(site?site->territory:"");fputs("\" data-profile-site=\"",stdout);html(site?site->id:"");fputs("\" data-profile-view=\"",stdout);html(private_view?"self":"public");fputs("\">",stdout);
 PhStats stats;ph_stats(db,site,&stats);
 fputs("<section class=\"ph-hero\">",stdout);ph_cover_emit(site);fputs("<div class=\"ph-hero-in\"><div class=\"ph-hero-main\"><p class=\"ph-status\"><span class=\"ph-live\">",stdout);fputs(private_view?"Your profile":site&&!site->actor[0]?"Credited profile":"Public profile",stdout);fputs("</span>",stdout);if(site&&site->actor[0]){fputs("<span>Address <code>",stdout);html(site->actor);fputs("</code></span>",stdout);}if(site){fputs("<span>",stdout);html(ph_label(site->territory));fputs(" · ",stdout);html(ph_territory_line(site->territory));fputs("</span>",stdout);}fputs("</p><div class=\"ph-title\"><h1>",stdout);html(name);fputs("</h1>",stdout);if(site&&site->host[0]&&site->actor[0]){fputs("<a class=\"ph-handle\" href=\"",stdout);ph_address(site,0);fputs("\">@",stdout);html(site->host);fputs("</a>",stdout);}fputs("</div>",stdout);
 if(site&&site->bio[0]){fputs("<p class=\"ph-bio\">",stdout);html(site->bio);fputs("</p>",stdout);}else if(private_view)fputs("<p class=\"ph-bio\">Choose a site to manage what it publishes.</p>",stdout);
 if(site&&site->actor[0]){fputs("<dl class=\"ph-stats\"><div><dt>Published articles</dt><dd>",stdout);printf("%d",stats.articles);fputs("</dd></div><div><dt>Subjects</dt><dd>",stdout);printf("%d",stats.subjects);fputs("</dd></div><div><dt>Sites</dt><dd>",stdout);printf("%d",stats.sites);fputs("</dd></div><div><dt>Latest</dt><dd>",stdout);ph_short_date(stats.latest);fputs("</dd></div></dl>",stdout);}
 fputs("</div><nav class=\"ph-actions\" aria-label=\"Profile actions\">",stdout);
 if(private_view&&site){fputs("<a class=\"ph-primary\" href=\"/cms/compose/advanced?site=",stdout);url_query_value(site->id);fputs("&amp;new=article\">Write an article</a><a href=\"",stdout);ph_url(site,"posts",1);fputs("\">Review drafts</a><a href=\"/workspace/worlds?site=",stdout);url_query_value(site->id);fputs("\">World studio</a><a href=\"",stdout);ph_url(site,"edit",1);fputs("\">Edit this profile</a><a data-profile-preview href=\"",stdout);ph_address(site,1);fputs("\">View public site</a>",stdout);ph_cover_remove_form(site);}else if(private_view)fputs("<a class=\"ph-primary\" href=\"/account#presence\">Set up your profile</a>",stdout);
 else if(site&&site->actor[0]){fputs("<a class=\"ph-primary\" href=\"",stdout);ph_url(site,"posts",0);fputs("\">Read everything published</a><a href=\"/companions\">Find companions</a><a href=\"",stdout);ph_url(site,"about",0);fputs("\">About ",stdout);html(name);fputs("</a>",stdout);}
 fputs("</nav></div></section>",stdout);(void)tab;}
static void ph_tab_icon(const char *tab){
    const char *glyph=!strcmp(tab,"posts")?BF_ICON_FILE_TEXT:!strcmp(tab,"saved")?BF_ICON_BOOKMARK:!strcmp(tab,"about")?BF_ICON_USER:!strcmp(tab,"sites")?BF_ICON_WORLD:!strcmp(tab,"organizations")?BF_ICON_BRIEFCASE:!strcmp(tab,"communities")?BF_ICON_GROUP:!strcmp(tab,"activity")?BF_ICON_ACTIVITY:!strcmp(tab,"gallery")?BF_ICON_PHOTO:ak_app_tab(tab)?ak_app_tab(tab)->icon:BF_ICON_BOARD;
    fputs("<span class=\"ph-tab-icon\" aria-hidden=\"true\">",stdout);fputs(glyph,stdout);fputs("</span>",stdout);
}
static void ph_nav(PhSite *sites,int n,const PhSite *site,const char *tab,int private_view){fputs("<nav class=\"ph-tabs\" aria-label=\"Profile sections\">",stdout);const char *keys[]={"overview","gallery","posts","saved","about","sites","communities","activity","organizations","dating"},*labels[]={"Overview","Gallery","Posts","Saved","About","My sites","Communities","My activity","Organizations","Dating"};for(int i=0;i<10;i++){if((private_view&&i==8)||(!private_view&&(i==3||i==5||i==7||i==9)))continue;fputs("<a href=\"",stdout);ph_url(site,keys[i],private_view);fputs("\"",stdout);if(!strcmp(tab,keys[i]))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);ph_tab_icon(keys[i]);html(labels[i]);fputs("</a>",stdout);}fputs("<a href=\"/companions\"><span class=\"ph-tab-icon\" aria-hidden=\"true\">" BF_ICON_HEART "</span>Companions</a></nav><div class=\"ph-layout\"><aside class=\"ph-rail\"><nav class=\"ph-sites\" aria-label=\"",stdout);fputs(private_view?"Your profile sites":"Published profile sites",stdout);fputs("\"><p class=\"ph-eyebrow\">",stdout);fputs(private_view?"Your sites":"Also on Aurekai",stdout);fputs("</p><h2>",stdout);fputs(private_view?"Where you publish":"Their sites",stdout);fputs("</h2>",stdout);for(int i=0;i<n;i++){fputs("<a href=\"",stdout);if(private_view)ph_url(&sites[i],"overview",1);else if(!sites[i].actor[0]&&!strncmp(sites[i].id,"credited-",9))fputs("#overview",stdout);else{fputs("https://",stdout);html(sites[i].host);fputc('/',stdout);}fputs("\"",stdout);if(site&&!strcmp(site->id,sites[i].id))fputs(" aria-current=\"page\"",stdout);fputs("><span class=\"ph-site-icon\" aria-hidden=\"true\">",stdout);fputs(!strcmp(sites[i].territory,"life")?BF_ICON_HEART:!strcmp(sites[i].territory,"live")?BF_ICON_PLAY:!strcmp(sites[i].territory,"shop")?BF_ICON_SHOP:BF_ICON_BRIEFCASE,stdout);fputs("</span><span class=\"ph-site-copy\"><strong>",stdout);html(ph_label(sites[i].territory));fputs("</strong><code>",stdout);html(sites[i].host);fputs("</code><small>",stdout);html(ph_territory_line(sites[i].territory));fputs("</small></span>",stdout);if(site&&!strcmp(site->id,sites[i].id))fputs("<em>Here</em>",stdout);fputs("</a>",stdout);}if(private_view){fputs("<a href=\"/account\"><span class=\"ph-site-icon\" aria-hidden=\"true\">" BF_ICON_USER "</span><span class=\"ph-site-copy\"><strong>Account settings</strong><small>Connections and privacy</small></span></a>",stdout);}fputs("</nav></aside><section class=\"ph-panel\">",stdout);}
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
        fputs("<a class=\"ph-world-card\" data-look=\"",stdout);html(look?look:"");
        fputs("\" href=\"https://",stdout);html(site->host);fputs("/worlds/",stdout);html(id);
        fputs("\"><small>",stdout);html(medium?medium:"Space");fputs("</small><strong>",stdout);html(title);fputs("</strong>",stdout);
        if(lede&&*lede){fputs("<p>",stdout);html(lede);fputs("</p>",stdout);}
        fputs("</a>",stdout);shown++;
    }
    sqlite3_finalize(rows);
    if(shown){fputs("</div><a class=\"ph-worlds-all\" href=\"https://",stdout);html(site->host);fputs("/worlds\">Browse all spaces</a></section>",stdout);}
}
/* Profile paths present installed native work without copying its source records. */
typedef struct {const char *title,*href,*glyph,*description;} PhJourneyLink;
static void ph_journey(const PhSite *site){
    static const PhJourneyLink paths[][3]={
        {{"Sources","/sources",BF_ICON_RSS,"Read published sources"},{"Recipes","/recipes",BF_ICON_BOOK,"Search and plan from retained recipes"},{"Feeds","/channels",BF_ICON_ACTIVITY,"Explore publishers and live channels"}},
        {{"Profile","/profile",BF_ICON_USER,"Your own account and sites"},{"Collections","/profile?tab=saved",BF_ICON_BOOKMARK,"Private saved sources and collections"},{"Operating roles","https://aurekai.work/roles",BF_ICON_BRIEFCASE,"Choose your scoped working posture"}},
        {{"Posts",NULL,BF_ICON_FILE_TEXT,"Write, review and publish"},{"Worlds",NULL,BF_ICON_WORLD,"Build spaces with retained references"},{"Programs","/programs?context=self",BF_ICON_BOARD,"Continue work in installed programs"}}
    };
    static const char *stages[]={"discover","keep","create"};
    static const char *labels[]={"Discover","Keep","Create"};
    static const char *kickers[]={"Sources · Recipes · Feeds","Profile · Collections","Posts · Worlds · Programs"};
    static const char *glyphs[]={BF_ICON_COMPASS,BF_ICON_BOOKMARK,BF_ICON_SPARKLES};
    fputs("<section class=\"ph-pathways\" data-profile-pathways><header><h2>Your source-to-creation journey</h2>"
          "<p>Discover retained material, keep it in your own account, then create with your words and review before publication.</p></header><div class=\"ph-pathway-grid\">",stdout);
    for(int stage=0;stage<3;stage++){
        fputs("<section class=\"ph-pathway\" data-pathway-stage=\"",stdout);html(stages[stage]);fputs("\"><div class=\"ph-pathway-head\">",stdout);
        fputs(glyphs[stage],stdout);fputs("<h3>",stdout);html(labels[stage]);fputs("</h3></div><p class=\"ph-pathway-kicker\">",stdout);html(kickers[stage]);fputs("</p><nav class=\"ph-pathway-items\" aria-label=\"",stdout);html(labels[stage]);fputs(" destinations\">",stdout);
        for(int j=0;j<3;j++){const PhJourneyLink *path=&paths[stage][j];if(!path->title)continue;
            fputs("<a data-pathway-link=\"",stdout);html(path->title);fputs("\" href=\"",stdout);
            if(stage==2&&j==0){if(site)ph_url(site,"compose",1);else fputs("/profile?tab=sites",stdout);}
            else if(stage==2&&j==1){if(site){fputs("/workspace/worlds?site=",stdout);url_query_value(site->id);}else fputs("/profile?tab=sites",stdout);}
            else html(path->href);
            fputs("\">",stdout);fputs(path->glyph,stdout);fputs("<span><strong>",stdout);html(path->title);fputs("</strong><small>",stdout);html(path->description);fputs("</small></span></a>",stdout);
        }
        fputs("</nav></section>",stdout);
    }
    fputs("</div><p class=\"ph-pathway-principle\">One retained source identity, multiple account-owned continuations, explicit publication review. Saving is private; posting is a separate decision.</p></section>",stdout);
}
/* The private overview lists only sites already bound to this actor. Published
 * post counts use the same owner, visibility and moderation predicates as the
 * public profile; no account work is copied into a public page. */
static void ph_territory_icon(const char *territory){
    const char *glyph=!strcmp(territory,"life")?BF_ICON_HEART:!strcmp(territory,"live")?BF_ICON_PLAY:!strcmp(territory,"shop")?BF_ICON_SHOP:BF_ICON_BRIEFCASE;
    fputs("<span class=\"ph-realm-icon\" aria-hidden=\"true\">",stdout);fputs(glyph,stdout);fputs("</span>",stdout);
}
static void ph_private_sites(sqlite3 *db,const PhSite *sites,int count){
    if(!db||count<1)return;
    sqlite3_stmt *q=NULL;
    const char *sql="SELECT count(*) FROM community_post WHERE namespace=?1 AND community=?1 AND author_subject=?2 AND visibility='public' AND moderation_state='accepted' AND status='published'";
    int available=sqlite3_prepare_v2(db,sql,-1,&q,NULL)==SQLITE_OK;
    fputs("<section data-profile-site-overview><h2>Your public sites</h2><p>Only published site and post records appear here.</p><div class=\"ph-realm-grid\">",stdout);
    const char *territories[]={"life","live","shop","work"};
    for(size_t t=0;t<4;t++){
        const char *territory=territories[t];const PhSite *s=NULL;int matches=0;
        for(int i=0;i<count;i++)if(!strcmp(sites[i].territory,territory)){s=&sites[i];matches++;}
        if(matches!=1){
            fputs("<article class=\"ph-realm ph-realm-unbound\" data-profile-unbound=\"",stdout);html(territory);
            fputs("\"><div class=\"ph-realm-head\">",stdout);ph_territory_icon(territory);fputs("<div><h3>",stdout);html(ph_label(territory));
            fputs("</h3><small>Site setup needed</small></div></div><p>No published profile is bound to this account in this territory.</p><nav class=\"ph-realm-actions\"><a href=\"/account#presence\">Set up site</a></nav></article>",stdout);
            continue;
        }
        fputs("<article class=\"ph-realm\" data-profile-territory=\"",stdout);html(territory);
        fputs("\"><div class=\"ph-realm-head\">",stdout);ph_territory_icon(territory);fputs("<div><h3>",stdout);html(ph_label(territory));
        fputs("</h3><small>",stdout);html(s->host);fputs("</small></div></div><div><strong>",stdout);html(s->name);
        fputs("</strong><p class=\"ph-realm-bio\">",stdout);
        if(s->bio[0])html(s->bio);else fputs("An introduction has not been published yet.",stdout);
        fputs("</p></div>",stdout);
        if(available){sqlite3_reset(q);sqlite3_clear_bindings(q);sqlite3_bind_text(q,1,s->id,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,s->actor,-1,SQLITE_STATIC);
            if(sqlite3_step(q)==SQLITE_ROW){long long n=sqlite3_column_int64(q,0);printf("<p>%lld published post%s</p>",n,n==1?"":"s");}}
        else fputs("<p>Published-post count unavailable</p>",stdout);
        fputs("<nav class=\"ph-realm-actions\" aria-label=\"Site actions\"><a href=\"",stdout);ph_url(s,"overview",1);
        fputs("\">Manage site</a><a href=\"",stdout);ph_url(s,"edit",1);fputs("\">Edit introduction</a><a href=\"",stdout);
        ph_address(s,1);fputs("\">View public site</a></nav></article>",stdout);
    }
    fputs("</div></section>",stdout);sqlite3_finalize(q);
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
    fputs("<section class=\"ph-private-work\" data-private-work><h2>Your work in Aurekai</h2><p class=\"ph-privacy\">Private account work is separate from what you publish on a profile site. Recorded states are not live worker heartbeats.</p><nav class=\"ph-actions\" aria-label=\"Continue your work\"><a href=\"/inbox?context=self\">Inbox and decisions</a><a href=\"/work-board?context=self\">Work board</a><a href=\"/apps?context=self\">Installed tools</a><a href=\"/saved\">Saved material</a></nav>",stdout);
    if(!path||*path!='/')goto unavailable;
    if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto unavailable;
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
    printf("<p class=\"ph-work-counts\" data-work-total=\"%lld\" data-work-attention=\"%lld\"><strong>%lld</strong> open records · <strong>%lld</strong> held or requiring recovery · <strong>%lld</strong> ready or owner-reported in progress</p>",total,attention,total,attention,waiting);
    if(sqlite3_prepare_v2(db,
      "SELECT id,title,state FROM account_work WHERE subject=?1 AND scope='self' "
      "AND state IN('blocked','retry_wait','dead_letter','running','ready') "
      "ORDER BY updated_ms DESC,id LIMIT 5",
      -1,&q,NULL)==SQLITE_OK){
        sqlite3_bind_text(q,1,subject,-1,SQLITE_STATIC);
        fputs("<ol class=\"ph-work-items\">",stdout);
        while(sqlite3_step(q)==SQLITE_ROW){
            const char *id=(const char*)sqlite3_column_text(q,0);
            const char *title=(const char*)sqlite3_column_text(q,1);
            const char *state=(const char*)sqlite3_column_text(q,2);
            if(!id||!title||!state||!ac_word(id))continue;
            fputs("<li><a href=\"/work-board?context=self&amp;task=",stdout);
            url_query_value(id);
            fputs("\">",stdout);html(title);fputs("</a> <small>",stdout);html(state);fputs("</small></li>",stdout);shown++;
        }
        fputs("</ol>",stdout);
    }
    if(!total)fputs("<p>No active work is indexed for this account. This does not imply that the remote service was checked.</p>",stdout);
    if(total>shown)fputs("<p><a href=\"/work-board?context=self\">See the complete account work history</a></p>",stdout);
    goto done;
unavailable:
    fputs("<p role=\"alert\" data-work-unavailable>The native Workboard index is unavailable on this service. Your work has not been verified empty. <a href=\"/work-board?context=self\">Open the Workboard</a>.</p>",stdout);
done:
    sqlite3_finalize(q);if(db)sqlite3_close(db);
    fputs("</section>",stdout);
}
static int ph_cms(const char *action,const char *type,const char *id,const char *payload,const char *ns,const char *f1,const char *v1,const char *f2,const char *v2){const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");if(!db||!schemas)return 0;char *args[28];int n=0;args[n++]=(char*)bin;args[n++]="entry";args[n++]=(char*)action;args[n++]=(char*)type;if(id)args[n++]=(char*)id;args[n++]=(char*)payload;args[n++]="--ns";args[n++]=(char*)ns;args[n++]="--db";args[n++]=(char*)db;args[n++]="--schemas";args[n++]=(char*)schemas;if(f1){args[n++]="--if-field";args[n++]=(char*)f1;args[n++]="--if-value";args[n++]=(char*)v1;}if(f2){args[n++]="--if-field2";args[n++]=(char*)f2;args[n++]="--if-value2";args[n++]=(char*)v2;}args[n]=NULL;char output[8192];return auth_call(args,NULL,output,sizeof(output))==0;}
static int ph_cms_cover_remove(const char *id,const char *ns,const char *digest){
 const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");
 if(!db||!schemas)return 0;
 char *args[]={(char*)bin,"entry","update","site_fragment",(char*)id,"{\"status\":\"archived\"}","--ns",(char*)ns,"--db",(char*)db,"--schemas",(char*)schemas,"--if-digest",(char*)digest,NULL};
 char output[8192];return auth_call(args,NULL,output,sizeof(output))==0;
}
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
   int written=snprintf(href,1401,"https://aurekai.work/o/%s/%s",source,key);
   ok=written>0&&written<1401&&ph_link(href);}
 free(item);return ok;
}
static int ph_saved(sqlite3 *db,long uid,const char *id,char title[768],char href[1401]){sqlite3_stmt *st=NULL;char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);if(!ph_digits(id)||sqlite3_prepare_v2(db,"SELECT title,href FROM saved_item WHERE id=?1 AND actor=?2 AND status='published'",-1,&st,NULL)!=SQLITE_OK)return 0;sqlite3_bind_int64(st,1,strtoll(id,NULL,10));sqlite3_bind_text(st,2,actor,-1,SQLITE_STATIC);int ok=sqlite3_step(st)==SQLITE_ROW;if(ok){const char *a=(const char*)sqlite3_column_text(st,0),*b=(const char*)sqlite3_column_text(st,1);ok=a&&strlen(a)<768&&b&&strlen(b)<1401&&ph_link(b)&&*b;if(ok){snprintf(title,768,"%s",a);snprintf(href,1401,"%s",b);}}sqlite3_finalize(st);return ok;}
static void ph_saved_list(sqlite3 *db,long uid,const PhSite *site,const char *q,long long cursor,int take){char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);sqlite3_stmt *st=NULL;fputs("<h2>Saved for you</h2><p class=\"ph-privacy\">Private across your account. Saving something never puts it on a public site. You choose a site and review a post before sharing a link.</p><form class=\"ph-search\" method=\"get\" action=\"/profile\">",stdout);ph_hidden("site",site?site->id:"");ph_hidden("tab","saved");fputs("<input type=\"search\" name=\"q\" aria-label=\"Search your saved material\" placeholder=\"Search saved titles, publishers or collections\" value=\"",stdout);html(q);fputs("\"><button type=\"submit\">Search saved</button></form><div class=\"ph-board\">",stdout);
 const char *sql="SELECT id,title,href,image,publisher,summary,saved_at,COALESCE(collection,''),item_ref FROM saved_item WHERE actor=?1 AND status='published' AND (?2=0 OR id<?2) AND (?3='' OR instr(lower(COALESCE(title,'')||' '||COALESCE(publisher,'')||' '||COALESCE(collection,'')),lower(?3))>0) ORDER BY id DESC LIMIT ?4";int count=0,more=0;long long last=0;
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);sqlite3_bind_int64(st,2,cursor);sqlite3_bind_text(st,3,q,-1,SQLITE_STATIC);sqlite3_bind_int(st,4,take+1);while(sqlite3_step(st)==SQLITE_ROW){if(count==take){more=1;break;}count++;last=sqlite3_column_int64(st,0);const char *title=(const char*)sqlite3_column_text(st,1),*href=(const char*)sqlite3_column_text(st,2),*image=(const char*)sqlite3_column_text(st,3),*pub=(const char*)sqlite3_column_text(st,4),*summary=(const char*)sqlite3_column_text(st,5),*saved=(const char*)sqlite3_column_text(st,6),*folder=(const char*)sqlite3_column_text(st,7);fputs("<article class=\"ph-saved\" data-private-saved><div><small>",stdout);html(folder&&*folder?folder:"Saved");fputs("</small><h3>",stdout);html(title);fputs("</h3>",stdout);if(image&&page_image_url_safe(image)){fputs("<img loading=\"lazy\" decoding=\"async\" referrerpolicy=\"no-referrer\" alt=\"\" src=\"",stdout);html(image);fputs("\">",stdout);}fputs("<p>",stdout);html(summary?summary:"");fputs("</p><small>",stdout);html(pub?pub:"");fputs("</small><nav aria-label=\"Saved item actions\">",stdout);if(href&&*href&&ph_link(href)){fputs("<a href=\"",stdout);html(href);fputs("\" rel=\"noopener noreferrer\" target=\"_blank\">Open original \u2197</a>",stdout);if(site){fputs("<a href=\"",stdout);ph_url(site,"compose",1);printf("&amp;item=%lld\">Share a link here</a>",last);}}fputs("</nav></div></article>",stdout);(void)saved;}}sqlite3_finalize(st);fputs("</div>",stdout);if(!count)fputs("<div class=\"ph-empty\"><p>No saved material matches this view.</p><a href=\"/discover\">Explore and save something</a></div>",stdout);if(more){fputs("<a class=\"ph-next\" href=\"",stdout);ph_url(site,"saved",1);printf("&amp;cursor=%lld&amp;q=",last);url_query_value(q);fputs("\">More saved material</a>",stdout);}}
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
  if(!count++)fputs("<section class=\"ph-work-list\" data-profile-work><h2>Released work</h2><p class=\"ph-privacy\">Work here is released by this profile. Starting one opens a program in your own account. Nothing from this profile is copied into it.</p>",stdout);
  fputs("<article class=\"ph-work\"><div>",stdout);
  if(picture&&page_image_url_safe(picture)){fputs("<img loading=\"lazy\" decoding=\"async\" src=\"",stdout);html(picture);fputs("\" alt=\"",stdout);html(alt?alt:"");fputs("\">",stdout);}
  fputs("</div><div><p class=\"ph-kicker\">",stdout);html(kicker&&*kicker?kicker:"Released work");fputs("</p><h3>",stdout);html(headline);fputs("</h3>",stdout);if(dek&&*dek){fputs("<p>",stdout);html(dek);fputs("</p>",stdout);}if(made_with&&*made_with){fputs("<p class=\"ph-work-with\">Built with ",stdout);if(href&&public_http_url(href)){fputs("<a rel=\"noopener noreferrer\" href=\"",stdout);html(href);fputs("\">",stdout);html(made_with);fputs("</a>",stdout);}else html(made_with);fputs(".</p>",stdout);}if(published&&*published){fputs("<time>",stdout);html(published);fputs("</time>",stdout);}fputs("<p data-work-program><a class=\"ph-button\" href=\"/programs?context=self",stdout);if(bound){fputs("&amp;template=",stdout);url_query_value(program);}fputs(bound?"\">Start this as a program</a> ":"\">Open your programs</a> ",stdout);fputs("<a class=\"ph-work-with\" href=\"/packs/setup?pack=",stdout);url_query_value(pack);fputs("&amp;context=self\">Set up the tool on its own</a></p></div></article>",stdout);
 }
 sqlite3_finalize(st);if(count)fputs("</section>",stdout);
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
   fputs("<section class=\"ph-journal\" data-profile-journal><div class=\"ph-journal-heading\"><div><span class=\"ph-journal-eyebrow\">",stdout);
   fputs(owner?"Your journal":"Writing / Published work",stdout);
   fputs("</span><h2>Journal</h2></div><p>",stdout);
   fputs(owner?"Composed articles on this site, published and in review. Unreleased writing stays private.":"Ideas and research written for this site.",stdout);
   fputs("</p></div><div class=\"ph-journal-lead\"><div><span class=\"ph-journal-eyebrow\">",stdout);
   fputs(owner?"Continue composing":"Start reading",stdout);fputs("</span><h3>",stdout);
   html(title);
   fputs("</h3><p>",stdout);if(lede)html(lede);
   fputs("</p><a class=\"ph-journal-read\" href=\"",stdout);
   if(owner&&draft){fputs("/cms/compose/advanced?site=",stdout);url_query_value(site->id);fputs("&amp;route=",stdout);url_query_value(route);}else fwrite(route,1,6+len,stdout);
   if(!(owner&&draft))fputc('/',stdout);fputs("\">",stdout);fputs(owner&&draft?"Continue editing article":"Read the featured article",stdout);
   fputs("</a></div><div class=\"ph-journal-lead-aside\"><span class=\"ph-journal-eyebrow\">",stdout);
   fputs(owner?"Private editorial desk":"A living publication",stdout);
   fputs("</span><p>",stdout);
   fputs(owner?"Edit a draft, arrange a page, or read a published edition in the same journal.":"Every article published on this site, newest first. Open one to read it or have it read aloud.",stdout);
   fputs("</p><a href=\"/blog/\">Published journal</a></div></div>",stdout);
   fputs("<div class=\"ph-journal-tools\"><details class=\"ph-journal-search\"><summary>" BF_ICON_SEARCH "<span>Search articles</span></summary><label>Search titles and topics<input type=\"search\" data-journal-search placeholder=\"Search articles\" autocomplete=\"off\"></label></details><div class=\"ph-journal-filters\" role=\"group\" aria-label=\"Filter by subject\"><button type=\"button\" data-journal-category=\"\" aria-pressed=\"true\">All subjects</button>",stdout);
   sqlite3_stmt *cats=NULL;
   if(sqlite3_prepare_v2(db,"SELECT DISTINCT COALESCE(eyebrow,'') FROM site_page WHERE namespace=?1 AND ((?2=1 AND status IN ('draft','published')) OR (?2=0 AND status='published')) AND route LIKE '/blog/%' AND COALESCE(eyebrow,'')<>'' ORDER BY 1",-1,&cats,NULL)==SQLITE_OK){
    sqlite3_bind_text(cats,1,site->id,-1,SQLITE_STATIC);
    sqlite3_bind_int(cats,2,owner?1:0);
    while(sqlite3_step(cats)==SQLITE_ROW){const char *c=(const char*)sqlite3_column_text(cats,0);if(!c||strlen(c)>120)continue;
     fputs("<button type=\"button\" data-journal-category=\"",stdout);html(c);fputs("\" aria-pressed=\"false\">",stdout);html(c);fputs("</button>",stdout);}
   }sqlite3_finalize(cats);
   fputs("</div><p data-journal-count aria-live=\"polite\"></p></div>",stdout);
   if(owner) {
    fputs("<div class=\"ph-journal-batch\" data-journal-batch data-site=\"",stdout);html(site->id);
    fputs("\"><div class=\"ph-journal-batch-actions\"><button type=\"button\" data-journal-select-all>Select visible drafts</button><button type=\"button\" data-journal-clear>Clear selection</button></div><label class=\"ph-journal-batch-rights\"><input type=\"checkbox\" data-journal-rights> I have reviewed each selected article and hold its publication rights.</label><button type=\"button\" data-journal-publish disabled>Publish selected articles</button><output data-journal-status role=\"status\" aria-live=\"polite\">Select drafts to review.</output></div>",stdout);
   }
   fputs("<div class=\"ph-journal-grid\">",stdout);
  }
  fputs("<article class=\"ph-journal-card\" data-article data-publication-state=\"",stdout);fputs(owner&&draft?"draft":"published",stdout);fputs("\" data-category=\"",stdout);html(category?category:"");
  fputs("\" data-search=\"",stdout);html(title);fputc(' ',stdout);if(lede)html(lede);fputc(' ',stdout);if(category)html(category);
  fputs("\" data-journal-route=\"",stdout);html(route);fputs("\">",stdout);
  if(owner&&draft) {fputs("<label class=\"ph-journal-card-select\"><input type=\"checkbox\" data-journal-select aria-label=\"Select ",stdout);html(title);fputs(" for publication review\"><span>Select for review</span></label>",stdout);}
  fputs("<a href=\"",stdout);if(owner&&draft){fputs("/cms/compose/advanced?site=",stdout);url_query_value(site->id);fputs("&amp;route=",stdout);url_query_value(route);}else fwrite(route,1,6+len,stdout);if(!(owner&&draft))fputc('/',stdout);fputs("\"><div class=\"ph-journal-card-top\"><span class=\"ph-journal-card-kind\">",stdout);
  html(category&&*category?category:"Journal");
  fputs("</span><span class=\"ph-journal-index\">",stdout);if(owner&&draft)fputs("DRAFT · ",stdout);printf("%02d",count+1);
  fputs("</span></div><h3>",stdout);html(title);
  fputs("</h3><p>",stdout);if(lede)html(lede);
  fputs("</p><div class=\"ph-journal-card-bottom\"><span>",stdout);fputs(owner&&draft?"Private draft":(category&&*category?category:"Article"),stdout);
  fputs("</span><strong>",stdout);fputs(owner&&draft?"Continue editing":"Read article",stdout);fputs(" <span aria-hidden=\"true\">&#8599;</span></strong></div></a>",stdout);
  if(placed){fputs("<p class=\"ph-journal-publisher\" data-publisher-credit>Published by ",stdout);html(approved.author_credit);fputs(" · <a href=\"https://",stdout);html(approved.origin_host);html(approved.origin_route);fputs("\" rel=\"external noopener noreferrer\">Original article</a></p>",stdout);}
  if(owner&&!draft&&!placed){fputs("<a class=\"ph-journal-edit\" href=\"/cms/compose/advanced?site=",stdout);url_query_value(site->id);
   fputs("&amp;route=",stdout);url_query_value(route);fputs("\">Edit and arrange this published page</a>",stdout);}
  fputs("</article>",stdout);
  count++;
 }
 sqlite3_finalize(st);
 if(count){
  fputs("</div><p class=\"ph-journal-empty\" hidden>No stories match those filters.</p></section>",stdout);
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
  if(!n++){fputs("<section class=\"ph-review\" data-review-queue><header class=\"ph-review-head\"><div><span class=\"ph-review-kicker\">Private editorial desk</span><h2>Pages awaiting your review</h2><p>Finish composing and reviewing these sections. Unreleased changes remain private.</p></div><a class=\"ph-review-new\" href=\"/cms/compose/advanced?site=",stdout);url_query_value(site->id);fputs("&amp;new=article\">Write another article</a></header><div class=\"ph-review-grid\">",stdout);}
  fputs("<article class=\"ph-review-card\"><div class=\"ph-review-meta\"><span>",stdout);html(subject&&*subject?subject:"Site page");fputs("</span><span>",stdout);html(status&&!strcmp(status,"published")?"Section changes held":"Draft — not public");fputs("</span></div><h3>",stdout);html(title);fputs("</h3><p class=\"ph-review-count\">",stdout);printf("%d composed %s · %d released",sections,sections==1?"section":"sections",released);fputs("</p><a class=\"ph-review-open\" href=\"/cms/compose/advanced?site=",stdout);
  url_query_value(site->id);fputs("&amp;route=",stdout);url_query_value(route);fputs("\">Continue editing <span aria-hidden=\"true\">&#8599;</span></a></article>",stdout);
 }
 sqlite3_finalize(st);
 if(n){fputs("</div><p class=\"ph-review-summary\">",stdout);printf("%d %s awaiting composition and review",n,n==1?"page":"pages");fputs("</p></section>",stdout);}
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
 fputs("<section class=\"ph-feed\" data-profile-feed><header class=\"ph-feed-head\"><div><p class=\"ph-eyebrow\">Published writing</p><h2>Writing and work</h2></div><p class=\"ph-feed-note\">",stdout);printf("%d %s",total,total==1?"article":"articles");fputs("</p></header><div class=\"ph-chips\" role=\"group\" aria-label=\"Filter by subject\"><button type=\"button\" data-subject=\"\" aria-pressed=\"true\">All <span>",stdout);printf("%d",n);fputs("</span></button>",stdout);
 for(int i=0;i<n;i++){const char *s=rows[i].subject;if(!*s)continue;int seen=0,c=0;for(int j=0;j<i;j++)if(!strcmp(rows[j].subject,s)){seen=1;break;}if(seen)continue;for(int j=i;j<n;j++)if(!strcmp(rows[j].subject,s))c++;fputs("<button type=\"button\" aria-pressed=\"false\" data-subject=\"",stdout);html(s);fputs("\">",stdout);html(s);fputs(" <span>",stdout);printf("%d",c);fputs("</span></button>",stdout);}
 fputs("</div><p class=\"ph-callout\">Only released pages appear here. Drafts, private saves and account history stay off this page.</p><ol class=\"ph-rows\">",stdout);
 for(int i=0;i<n;i++){PhRow *r=&rows[i];int here=current_host&&!strcmp(current_host,r->host);fputs("<li class=\"ph-row\" data-subject=\"",stdout);html(r->subject);fputs("\"><span class=\"ph-row-n\">",stdout);printf("%02d",i+1);fputs("</span><div class=\"ph-row-body\"><p class=\"ph-row-meta\"><b>",stdout);html(*r->subject?r->subject:"Journal");fputs("</b><span>",stdout);html(r->host);fputs("</span><time datetime=\"",stdout);html(r->date);fputs("\">",stdout);ph_short_date(r->date);fputs("</time></p><h3><a href=\"",stdout);if(!here){fputs("https://",stdout);html(r->host);}html(r->route);fputs("\">",stdout);html(r->title);fputs("</a></h3>",stdout);if(*r->lede){fputs("<p class=\"ph-row-lede\">",stdout);html(r->lede);fputs("</p>",stdout);}fputs("<p class=\"ph-row-tags\"><span class=\"ph-tag\">Article</span>",stdout);if(*r->by){fputs("<span>By ",stdout);html(r->by);fputs("</span>",stdout);}fputs("</p></div></li>",stdout);
  free(r->route);free(r->title);free(r->lede);free(r->subject);free(r->date);free(r->host);free(r->by);}
 free(rows);fputs("</ol>",stdout);
 if(total>n){fputs("<a class=\"ph-more\" href=\"",stdout);ph_url(site,"posts",owner);fputs("\">Explore all ",stdout);printf("%d",total);fputs(" articles <span aria-hidden=\"true\">&rarr;</span></a>",stdout);}
 fputs("<p class=\"ph-chip-empty\" hidden>No articles on this page match that subject.</p></section><script>(()=>{for(const root of document.querySelectorAll('[data-profile-feed]')){const bs=[...root.querySelectorAll('.ph-chips button')],rows=[...root.querySelectorAll('.ph-row')],empty=root.querySelector('.ph-chip-empty');for(const b of bs)b.addEventListener('click',()=>{const s=b.dataset.subject;let shown=0;for(const x of bs)x.setAttribute('aria-pressed',String(x===b));for(const r of rows){const ok=!s||r.dataset.subject===s;r.hidden=!ok;if(ok)shown++;}empty.hidden=shown!==0;});}})();</script>",stdout);
 return total;}
static void ph_posts(sqlite3 *db,const PhSite *site,int owner,long long cursor,int limit){sqlite3_stmt *st=NULL;int count=0,more=0;long long last=0;if(!site){fputs("<div class=\"ph-empty\"><p>Choose one of your sites to see what you have published there.</p></div>",stdout);return;}int awaiting=owner?ph_review_pages(db,site):0;int composed=0;if(limit>6){composed=ph_journal_articles(db,site,owner);}ph_work_examples(db,site);int articles=composed?composed:ph_feed(db,site,limit<=6?8:400,site->host,owner);
 const char *sql="SELECT id,title,body,COALESCE(subject_address,''),published_at,post FROM community_post WHERE namespace=?1 AND community=?1 AND author_subject=?2 AND visibility='public' AND moderation_state='accepted' AND status='published' AND (?3=0 OR id<?3) ORDER BY id DESC LIMIT ?4";
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);sqlite3_bind_text(st,2,site->actor,-1,SQLITE_STATIC);sqlite3_bind_int64(st,3,cursor);sqlite3_bind_int(st,4,limit+1);while(sqlite3_step(st)==SQLITE_ROW){if(count==limit){more=1;break;}count++;last=sqlite3_column_int64(st,0);fputs("<article class=\"ph-post\" data-public-post><time>",stdout);html((const char*)sqlite3_column_text(st,4));fputs("</time><h3>",stdout);html((const char*)sqlite3_column_text(st,1));fputs("</h3><p>",stdout);html((const char*)sqlite3_column_text(st,2));fputs("</p>",stdout);const char *href=(const char*)sqlite3_column_text(st,3);if(href&&*href&&ph_link(href)){fputs("<a href=\"",stdout);html(href);fputs("\" rel=\"noopener noreferrer\">Read the linked source \u2197</a>",stdout);}if(owner){fputs("<details><summary>Manage this post</summary><p>Taking it off this site does not remove private saved material or posts on your other sites.</p><form action=\"/profile/withdraw\" method=\"post\">",stdout);ph_hidden("site",site->id);char id[32];snprintf(id,sizeof(id),"%lld",last);ph_hidden("id",id);ph_hidden("expected",(const char*)sqlite3_column_text(st,5));fputs("<button type=\"submit\">Remove from this site</button></form></details>",stdout);}fputs("</article>",stdout);}}sqlite3_finalize(st);if(!count&&!articles&&!composed){fputs("<div class=\"ph-empty\"><h2>",stdout);fputs(owner&&awaiting?"Not yet released":owner?"Make this space yours":"Nothing published here yet",stdout);fputs("</h2><p>",stdout);fputs(owner&&awaiting?"Your private editorial desk above has articles awaiting review. Publish a reviewed edition to make it appear here.":owner?"Write a post, or choose a saved link and add your own introduction. Your other sites stay unchanged.":"This page will show what its owner chooses to publish here.",stdout);fputs("</p></div>",stdout);}if(more){fputs("<a class=\"ph-next\" href=\"",stdout);ph_url(site,"posts",owner);printf("&amp;cursor=%lld\">Earlier posts</a>",last);}}
static void ph_compose(sqlite3 *db,long uid,const PhSite *site,const char *item,const char *ref){char title[768]="",href[1401]="",nonce[33];ph_nonce(nonce);if(item&&*item&&ref&&*ref){fputs("<p>Choose one source at a time.</p>",stdout);return;}if(item&&*item&&!ph_saved(db,uid,item,title,href)){fputs("<p>This saved source is no longer available to your account.</p>",stdout);return;}if(ref&&*ref&&!ph_retained_link(db,ref,title,href)){fputs("<p>This retained item is not available from a released source. Open the current source before writing.</p>",stdout);return;}fputs("<h2>Write for this site</h2><p class=\"ph-privacy\">Your post will appear only on ",stdout);html(site->host);fputs(". Other sites and private saves stay separate.</p><form class=\"ph-form\" action=\"/profile/preview\" method=\"post\">",stdout);ph_hidden("site",site->id);ph_hidden("nonce",nonce);if(item&&*item)ph_hidden("item",item);if(ref&&*ref)ph_hidden("ref",ref);fputs("<label>Post title<input name=\"title\" required maxlength=\"160\" value=\"",stdout);char short_title[161];snprintf(short_title,sizeof(short_title),"%.*s",160,title);html(short_title);fputs("\"></label><label>Your words<textarea name=\"text\" required maxlength=\"6000\" rows=\"8\" placeholder=\"Add your own introduction, update, or story.\"></textarea></label>",stdout);if((item&&*item)||(ref&&*ref)){fputs("<p>Linked source: ",stdout);html(href);fputs("</p><p>Only the exact source link is shared. No private excerpt or retained body is copied into this public post.</p>",stdout);ph_hidden("href",href);}else fputs("<label>Public link (optional)<input type=\"url\" name=\"href\" maxlength=\"1400\" placeholder=\"https://\"></label>",stdout);fputs("<button type=\"submit\">Review public post</button></form>",stdout);}
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
static int ph_mutate(const char *target,const char *body,const char *raw,const char *host,const StudioViewer *who,const char *actor,sqlite3 *db){char siteid[128]="",title[161]="",text[6001]="",href[1401]="",nonce[33]="",expected[200]="",item[24]="",ref[132]="",id[24]="",label[64]="",question[16]="",choice[8]="",qweight[4]="",template[65]="";PhField fields[]={{"site",siteid,sizeof(siteid),0},{"title",title,sizeof(title),0},{"text",text,sizeof(text),1},{"href",href,sizeof(href),0},{"nonce",nonce,sizeof(nonce),0},{"expected",expected,sizeof(expected),0},{"item",item,sizeof(item),0},{"ref",ref,sizeof(ref),0},{"id",id,sizeof(id),0},{"label",label,sizeof(label),0},{"question",question,sizeof(question),0},{"choice",choice,sizeof(choice),0},{"weight",qweight,sizeof(qweight),0},{"template",template,sizeof(template),0}};
 if(!ph_origin(raw,host)||!ph_fields(body,fields,14,40000)){ph_error(403,"Review and submit this form from its own profile page.\n");return 1;}PhSite site;if(!ph_site(db,NULL,siteid,&site)||strcmp(site.actor,actor)){ph_error(404,"This profile site is not available to this account.\n");return 1;}
 char payload[56000],output[65],post[200];size_t used=0;
 if(!strcmp(target,"/profile/cover/remove")){
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
 if(!strcmp(target,"/profile/edit")){if(!studio_safe_title(title)||strlen(text)>6000){ph_error(400,"Use a profile name and introduction within the displayed limits.\n");return 1;}ph_hash(&site,output);if(strcmp(output,expected)){ph_error(409,"This profile changed. Reload its current name and introduction before editing.\n");return 1;}used=1;payload[0]='{';page_json_field(payload,sizeof(payload),&used,"heading",title,0);page_json_field(payload,sizeof(payload),&used,"lede",text,site.about_id?1:0);if(!site.about_id){page_json_field(payload,sizeof(payload),&used,"route","/",0);page_json_field(payload,sizeof(payload),&used,"title",title,0);page_json_field(payload,sizeof(payload),&used,"status","published",1);}payload[used++]='}';payload[used]=0;char record[32];snprintf(record,sizeof(record),"%ld",site.about_id);int ok=ph_cms(site.about_id?"update":"create","site_page",site.about_id?record:NULL,payload,site.id,site.about_id?"heading":NULL,site.name,site.about_id?"lede":NULL,site.bio);PhSite current;if(!ph_site(db,NULL,site.id,&current)||strcmp(current.name,title)||strcmp(current.bio,text)){ph_error(ok?503:409,"The profile change was not confirmed. Reload this site before resubmitting.\n");return 1;}ph_redirect(&site,"about");return 1;}
 if(!strcmp(target,"/profile/withdraw")){if(!ph_digits(id)||!*expected){ph_error(400,"Choose an exact post from this site.\n");return 1;}sqlite3_stmt *st=NULL;int valid=0;if(sqlite3_prepare_v2(db,"SELECT 1 FROM community_post WHERE id=?1 AND namespace=?2 AND community=?2 AND author_subject=?3 AND post=?4 AND status IN ('published','archived')",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_int64(st,1,strtoll(id,NULL,10));sqlite3_bind_text(st,2,site.id,-1,SQLITE_STATIC);sqlite3_bind_text(st,3,actor,-1,SQLITE_STATIC);sqlite3_bind_text(st,4,expected,-1,SQLITE_STATIC);valid=sqlite3_step(st)==SQLITE_ROW;}sqlite3_finalize(st);if(!valid){ph_error(404,"That post is not part of this profile.\n");return 1;}ph_cms("update","community_post",id,"{\"status\":\"archived\"}",site.id,"community",site.id,"post",expected);st=NULL;int gone=0;if(sqlite3_prepare_v2(db,"SELECT status FROM community_post WHERE id=?1 AND namespace=?2",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_int64(st,1,strtoll(id,NULL,10));sqlite3_bind_text(st,2,site.id,-1,SQLITE_STATIC);gone=sqlite3_step(st)==SQLITE_ROW&&!strcmp((const char*)sqlite3_column_text(st,0),"archived");}sqlite3_finalize(st);if(!gone){ph_error(503,"Withdrawal is not confirmed. Read the current post before retrying.\n");return 1;}ph_redirect(&site,"posts");return 1;}
 if(!strcmp(target,"/profile/interests/add")||!strcmp(target,"/profile/interests/remove")){
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
  if(!strcmp(target,"/profile/interests/remove")){
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
 if(!strcmp(target,"/profile/quiz/answer")||!strcmp(target,"/profile/quiz/clear")){
  /* Re-answering supersedes: reads take the latest published row, so an
   * answer is a new row, never an edit. Clearing archives every published
   * row for the question so no older answer resurfaces. */
  const BfQuizQuestion *q=NULL;
  unsigned qi=0,ci=0;
  char date[40]="";
  for(qi=0;qi<bf_quiz_bank_n;qi++)
   if(!strcmp(question,bf_quiz_bank[qi].key)){q=&bf_quiz_bank[qi];break;}
  if(!q){ph_error(400,"Answer one of the listed quiz questions.\n");return 1;}
  if(!strcmp(target,"/profile/quiz/clear")){
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
 if(!strcmp(target,"/profile/theme")){
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
 if(strcmp(target,"/profile/preview")&&strcmp(target,"/profile/publish")){ph_error(404,"Unknown profile operation.\n");return 1;}
 if(!studio_safe_title(title)||!text[0]||strlen(nonce)!=32||strspn(nonce,"0123456789abcdef")!=32||!ph_link(href)){ph_error(400,"Review the post title, text and public link before publishing.\n");return 1;}if(*item){char original_title[768],original_href[1401];if(!ph_saved(db,who->id,item,original_title,original_href)||strcmp(original_href,href)){ph_error(409,"Your saved source changed. Review its current link before publishing.\n");return 1;}}if(*ref){char source_title[768],source_href[1401];if(*item||!ph_retained_link(db,ref,source_title,source_href)||strcmp(source_href,href)){ph_error(409,"The retained source changed or is no longer public. Reopen it before sharing.\n");return 1;}}
 if(!strcmp(target,"/profile/preview")){ph_begin(db,&site,site.name,1,"compose",actor);PhSite sites[16];int n=ph_sites(db,actor,sites);ph_nav(sites,n,&site,"posts",1);fputs("<h2>Review before publishing</h2><p>This post will be public on <strong>",stdout);html(site.host);fputs("</strong> only.</p><article class=\"ph-review\"><h3>",stdout);html(title);fputs("</h3><p>",stdout);html(text);fputs("</p><p>",stdout);html(href);fputs("</p></article><form action=\"/profile/publish\" method=\"post\">",stdout);ph_hidden("site",site.id);ph_hidden("title",title);ph_hidden("text",text);ph_hidden("href",href);ph_hidden("nonce",nonce);if(*item)ph_hidden("item",item);if(*ref)ph_hidden("ref",ref);fputs("<button type=\"submit\">Publish on this site</button></form><p><a href=\"",stdout);ph_url(&site,"compose",1);fputs("\">Start a different post</a></p>",stdout);ph_end();return 1;}
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
 if(strncmp(target,"/blog/",6))return 0;
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
  if(!count)fputs("<nav class=\"ak-zen-next\" aria-labelledby=\"zen-next-h\"><h2 id=\"zen-next-h\">Keep reading</h2><ol>",stdout);
  fputs("<li><span>",stdout);html(c&&*c?c:"Journal");fputs("</span><a href=\"",stdout);html(r);fputs("\">",stdout);html(t);fputs("</a></li>",stdout);count++;}
 sqlite3_finalize(st);
 if(count)fputs("</ol><a class=\"fd-open\" href=\"/blog/\">All articles <span aria-hidden=\"true\">&rsaquo;</span></a></nav>",stdout);
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
 fputs("<div class=\"ak-zen-bar\"><a class=\"ak-zen-back\" href=\"/blog/\"><span aria-hidden=\"true\">&larr;</span> Journal</a><p>",stdout);
 html(art.eyebrow[0]?art.eyebrow:"Journal");printf(" &middot; <span>%ld min read</span></p><div class=\"ak-zen-tools\" role=\"group\" aria-label=\"Reading\">",mins);
 fputs("<button type=\"button\" data-zen-size=\"-1\" aria-label=\"Smaller text\">A&minus;</button><button type=\"button\" data-zen-size=\"1\" aria-label=\"Larger text\">A+</button>"
       "<button type=\"button\" class=\"ak-zen-listen\" data-ak-track data-ak-default data-narrate=\".ak-zen-body\" data-title=\"",stdout);html(art.title);
 fputs("\" data-by=\"",stdout);html(by);fputs("\">",stdout);page_glyph("narration");fputs("<span>Read aloud</span></button></div></div>",stdout);
 fputs("<article class=\"ak-zen\"><header class=\"ak-zen-head\"><p class=\"ak-zen-kicker\"><span>",stdout);html(art.eyebrow[0]?art.eyebrow:"Journal");fputs("</span>",stdout);
 if(hasdate){fputs("<span>",stdout);html(datelong);fputs("</span>",stdout);}
 fputs("</p><div class=\"ak-zen-text\"><h1>",stdout);html(art.title);fputs("</h1>",stdout);
 if(art.lede[0]){fputs("<p class=\"ak-zen-lede\">",stdout);html(art.lede);fputs("</p>",stdout);}
 fputs("</div><p class=\"ak-zen-by\"><span class=\"ak-zen-initials\" aria-hidden=\"true\">",stdout);
 {char initials[8]="";int n=0;for(const char *p=by;*p&&n<2;p++)if((p==by||p[-1]==' ')&&isalpha((unsigned char)*p))initials[n++]=(char)toupper((unsigned char)*p);initials[n]=0;html(initials);}
 fputs("</span><span>",stdout);html(by);fputs("</span>",stdout);
 if(hasdate){fputs("<span class=\"ak-zen-meta\">",stdout);html(datelong);printf(" &middot; %ld min read</span>",mins);}
 if(placed){fputs("</p><p class=\"ak-zen-origin\" data-publisher-origin>Originally published by ",stdout);html(placement.author_credit);fputs(" at <a rel=\"external noopener noreferrer\" href=\"https://",stdout);html(placement.origin_host);html(placement.origin_route);fputs("\">the publisher's original article</a></p>",stdout);}
 else fputs("</p>",stdout);
 fputs("</header><div class=\"ak-zen-body ak-zen-text\">",stdout);
 fwrite(body,1,strlen(body),stdout);fputs("</div>",stdout);
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
 fputs("<h2>Interests</h2><p class=\"ph-privacy\">Up to 24 labels. Shared interests rank discovery and nothing else; they never gatekeep who can see this profile.</p><ul class=\"ph-interests\">",stdout);
 if(db&&sqlite3_prepare_v2(db,"SELECT label FROM person_interest WHERE namespace='root' AND actor_subject=?1 AND status='published' ORDER BY label LIMIT 24",-1,&st,NULL)==SQLITE_OK){
  sqlite3_bind_text(st,1,site->actor,-1,SQLITE_STATIC);
  while(sqlite3_step(st)==SQLITE_ROW){
   const char *label=(const char*)sqlite3_column_text(st,0);
   if(!label||!*label||strlen(label)>40)continue;
   count++;fputs("<li>",stdout);html(label);
   fputs("<form method=\"post\" action=\"/profile/interests/remove\" style=\"display:inline\">",stdout);
   ph_hidden("site",site->id);ph_hidden("label",label);
   fputs("<button type=\"submit\" aria-label=\"Remove ",stdout);html(label);fputs("\">Remove</button></form></li>",stdout);
  }
 }
 sqlite3_finalize(st);
 fputs("</ul><form class=\"ph-form\" method=\"post\" action=\"/profile/interests/add\">",stdout);
 ph_hidden("site",site->id);
 fputs("<label>Add an interest<input name=\"label\" maxlength=\"40\" required placeholder=\"road cycling\"></label><button type=\"submit\">Add interest</button></form>",stdout);
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
 fputs("<section class=\"ph-interests-public\" data-profile-interests><h2>Interests</h2><p>",stdout);
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *label=(const char*)sqlite3_column_text(st,0);
  int shared=0;
  if(!label||!*label||strlen(label)>40)continue;
  for(int i=0;i<nown;i++)if(!strcmp(own[i],label)){shared=1;break;}
  shown++;
  fputs("<a href=\"/companions?q=",stdout);url_query_value(label);
  if(shared)fputs("\" data-shared=\"1\">",stdout);else fputs("\">",stdout);
  html(label);fputs("</a> ",stdout);
 }
 sqlite3_finalize(st);
 if(!shown)fputs("No interests stated yet.",stdout);
 fputs("</p></section>",stdout);
}
static int serve_profile_home(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db,const char *accept){char blogslug[97];int private_view=!strcmp(target,"/profile")||!strcmp(target,"/profile/"),mutation=!strncmp(target,"/profile/",9)&&!private_view,apex=!strcmp(target,"/"),blog=ph_blog_slug(target,blogslug,sizeof(blogslug)),bloghome=!strcmp(target,"/blog")||!strcmp(target,"/blog/");if(!private_view&&!mutation&&!apex&&!blog&&!bloghome)return 0;if(apex&&host_is_territory_apex(host))return 0;if(!wants_html(accept)&&!mutation&&!((apex||blog||bloghome)&&!wants_json(accept)&&(!accept||!*accept||strstr(accept,"*/*"))))return 0;
 char normalized[256];if(!lower_host(host,normalized))return 0;sqlite3 *db=cms_open_ro();PhSite site={0},sites[16];int count=0,have_site=0;StudioViewer who={0};char actor[192]="";int signed_in=session_identity(bearer,auth_db,&who)&&who.id>0;if(signed_in)ph_actor(who.id,auth_db,actor);
 if(apex){if(!ph_site(db,normalized,NULL,&site)){sqlite3_stmt *claim=NULL;int declared=0;if(db&&sqlite3_prepare_v2(db,"SELECT 1 FROM site_presentation WHERE namespace='root' AND host=?1 AND profile='member-profile-v1' LIMIT 1",-1,&claim,NULL)==SQLITE_OK){sqlite3_bind_text(claim,1,normalized,-1,SQLITE_STATIC);declared=sqlite3_step(claim)==SQLITE_ROW;}sqlite3_finalize(claim);if(db)sqlite3_close(db);if(declared){ph_error(404,"This profile site is not currently published.\n");return 1;}return 0;}have_site=1;}
 if(bloghome){if(strcmp(method,"GET")){ph_error(405,"Open the journal with GET.\n");if(db)sqlite3_close(db);return 1;}if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}int served=ph_blog_index(db,&site,normalized,query);sqlite3_close(db);return served;}
 if(blog){if(strcmp(method,"GET")){ph_error(405,"Open journal articles with GET.\n");if(db)sqlite3_close(db);return 1;}if(!db||!ph_site(db,normalized,NULL,&site)){if(db)sqlite3_close(db);return 0;}int served=ph_blog(db,&site,blogslug,normalized);sqlite3_close(db);return served;}
 if(private_view||mutation){if(!signed_in){if(db)sqlite3_close(db);if(mutation)ph_error(401,"Sign in before changing a profile.\n");else if(!pj_profile_login_redirect(query))ph_error(400,"Choose a valid profile section before signing in.\n");return 1;}if(!db){ph_error(503,"The profile owner is unavailable.\n");return 1;}}
 if(mutation){if(strcmp(method,"POST")){ph_error(405,"Profile changes require a reviewed form submission.\n");sqlite3_close(db);return 1;}int result=!strncmp(target,"/profile/dating/",16)?ph_dating_mutate(target,body,raw,host,actor):
 ph_art_target(target)?ph_art_mutate(target,body,raw,host,actor,db):ph_mutate(target,body,raw,host,&who,actor,db);sqlite3_close(db);return result;}
 if(strcmp(method,"GET")){ph_error(405,"Open profiles with GET.\n");if(db)sqlite3_close(db);return 1;}
 char tab[24]="overview",selected[128]="",q[160]="",cursor[24]="",item[24]="",ref[132]="",kind[16]="",art[128]="",fx[8]="",fy[8]="",view[20]="discover",peer[192]="",before[24]="";PhField params[]={{"tab",tab,sizeof(tab),0},{"site",selected,sizeof(selected),0},{"q",q,sizeof(q),0},{"cursor",cursor,sizeof(cursor),0},{"item",item,sizeof(item),0},{"ref",ref,sizeof(ref),0},{"kind",kind,sizeof(kind),0},{"art",art,sizeof(art),0},{"fx",fx,sizeof(fx),0},{"fy",fy,sizeof(fy),0},{"view",view,sizeof(view),0},{"peer",peer,sizeof(peer),0},{"before",before,sizeof(before),0}};
 if(!ph_fields(query,params,13,2048)||((!ak_app_tab(tab))&&(*peer||strcmp(view,"discover")))||(ak_app_tab(tab)&&(!ak_app_tab_view(ak_app_tab(tab),view)||(*peer&&(strcmp(view,ak_app_tab(tab)->peer_view)||!bf_dating_subject(peer)))))||(*before&&(!ak_app_tab(tab)||strcmp(view,ak_app_tab(tab)->peer_view)||!*peer||!ph_digits(before)))||((*art||*fx||*fy)&&strcmp(tab,"artwork"))||(*cursor&&!ph_digits(cursor))||(*item&&!ph_digits(item))||(*ref&&(strcmp(tab,"compose")||*item))||(*kind&&(!pj_kind(kind)||strcmp(tab,"activity")))||(!private_view&&(*selected||*item||*ref||*q||*kind||*art||!strcmp(tab,"artwork")||(!strcmp(tab,"saved")||!strcmp(tab,"sites")||!strcmp(tab,"edit")||!strcmp(tab,"compose")||!strcmp(tab,"activity")||ak_app_tab(tab))))){ph_error(400,"Choose a section on this exact profile.\n");if(db)sqlite3_close(db);return 1;}
 long long dating_before=0;
 if(*before){errno=0;char *end=NULL;dating_before=strtoll(before,&end,10);
  if(errno||!end||*end||dating_before<=0){ph_error(400,"Choose a valid message page.\n");if(db)sqlite3_close(db);return 1;}}
 const char *allowed[]={"overview","posts","saved","about","sites","edit","compose","communities","activity","gallery","artwork","organizations","dating"};int valid=0;for(size_t i=0;i<13;i++)if(!strcmp(tab,allowed[i]))valid=1;if(!valid){ph_error(404,"This profile section does not exist.\n");if(db)sqlite3_close(db);return 1;}
 if(private_view){count=ph_sites(db,actor,sites);if(*selected){for(int i=0;i<count;i++)if(!strcmp(sites[i].id,selected)){site=sites[i];have_site=1;}if(!have_site){ph_error(404,"This site is not part of your profile.\n");sqlite3_close(db);return 1;}}else{for(int i=0;i<count;i++)if(!strcmp(sites[i].host,normalized)){site=sites[i];have_site=1;break;}if(!have_site)for(int i=0;i<count;i++)if(!strcmp(sites[i].territory,territory_of(host))){if(have_site){have_site=0;break;}site=sites[i];have_site=1;}}}
 else count=ph_sites(db,site.actor,sites);
 const char *name=have_site?site.name:who.name[0]?who.name:"Your profile";ph_begin(db,have_site?&site:NULL,name,private_view,tab,actor);ph_nav(sites,count,have_site?&site:NULL,tab,private_view);
 /* overview: the feed leads; worlds and interests follow it (see the overview branch below). */
 if(private_view&&!strcmp(tab,"overview")){ph_private_work(who.id);ph_journey(have_site?&site:NULL);ph_private_sites(db,sites,count);}
 if(!private_view&&signed_in&&actor[0]&&!strcmp(actor,site.actor)){fputs("<p class=\"ph-privacy\">You are viewing the public page. Private saves are not shown here. <a href=\"",stdout);ph_url(&site,"overview",1);fputs("\">Open your profile</a></p>",stdout);}
 if(!private_view&&signed_in&&actor[0]&&strcmp(actor,site.actor)&&have_site){fputs("<p class=\"ph-privacy\">Visiting as ",stdout);html(actor+7);fputs(". <a href=\"/companions/report?target=",stdout);url_query_value(site.actor);fputs("\">Report this profile</a></p>",stdout);}
 if(private_view&&ak_app_tab(tab))ph_dating_view(db,actor,view,peer,dating_before);
 else if(!strcmp(tab,"activity")&&private_view){ph_private_work(who.id);pj_profile_list(db,who.id,have_site?site.id:"",pj_kind(kind),q,*cursor?strtoll(cursor,NULL,10):0);}
 else if(!strcmp(tab,"saved"))ph_saved_list(db,who.id,have_site?&site:NULL,q,*cursor?strtoll(cursor,NULL,10):0,24);
 else if(!strcmp(tab,"communities")){if(private_view)cjw_profile(db,who.id,q,cursor);else ph_public_communities(db,&site,auth_db);}
 else if(!strcmp(tab,"organizations")&&!private_view)ph_public_organizations();
 else if(!strcmp(tab,"sites")){if(count)ph_private_sites(db,sites,count);else fputs("<h2>Your public sites</h2><p>No published profile site is currently bound to this account. Your saved material remains private. <a href=\"/account#presence\">Set up a public site</a>.</p>",stdout);}
 else if(!strcmp(tab,"edit")&&have_site){char digest[65];ph_hash(&site,digest);fputs("<h2>Edit this public profile</h2><p>Changes apply only to <strong>",stdout);html(site.host);fputs("</strong>.</p><form class=\"ph-form\" method=\"post\" action=\"/profile/edit\">",stdout);ph_hidden("site",site.id);ph_hidden("expected",digest);fputs("<label>Display name<input name=\"title\" maxlength=\"160\" required value=\"",stdout);html(site.name);fputs("\"></label><label>Introduction<textarea name=\"text\" maxlength=\"6000\" rows=\"7\">",stdout);html(site.bio);fputs("</textarea></label><button type=\"submit\">Publish profile changes</button></form>",stdout);ph_interests_edit(db,&site);ph_quiz_edit(db,&site);ph_theme_edit(db,&site);fputs("<h2>Site pages</h2><p class=\"ph-privacy\">Compose this site's pages and sections in the shared page composer, opening this profile's own workspace.</p><p><a class=\"ph-button\" href=\"/cms/compose/advanced?site=",stdout);html(site.id);fputs("\">Open visual page editor</a></p>",stdout);}
 else if(!strcmp(tab,"compose")&&have_site)ph_compose(db,who.id,&site,item,ref);
 else if(!strcmp(tab,"artwork")&&private_view)ph_artwork_view(db,sites,count,have_site?&site:NULL,art,fx,fy);
 else if(!strcmp(tab,"gallery")){if(have_site)ph_gallery_emit(db,&site,private_view,PH_GALLERY_MAX,1);else fputs("<div class=\"ph-empty\"><p>Choose one of your sites to see its gallery.</p></div>",stdout);}
 else if(!strcmp(tab,"about")){fputs("<h2>About</h2><div class=\"ph-post\"><p>",stdout);html(have_site&&site.bio[0]?site.bio:private_view?"Add an introduction for this site from Edit this profile.":"No introduction has been published on this site.");fputs("</p></div>",stdout);}
 else{if(private_view){fputs("<p class=\"ph-privacy\">This is your private profile view. ",stdout);if(have_site){fputs("You are managing <strong>",stdout);html(site.host);fputs("</strong>. ",stdout);}fputs("Published posts below belong to this site; saved material belongs to your private account.</p>",stdout);}ph_posts(db,have_site?&site:NULL,private_view,*cursor?strtoll(cursor,NULL,10):0,!strcmp(tab,"overview")?6:24);if(have_site&&!strcmp(tab,"overview")){ph_gallery_emit(db,&site,private_view,7,0);ph_worlds_teaser(db,&site);ph_interests_public(db,&site,signed_in&&actor[0]?actor:NULL);}if(private_view&&!strcmp(tab,"overview"))ph_saved_list(db,who.id,have_site?&site:NULL,"",0,6);}
 ph_end();if(db)sqlite3_close(db);return 1;}
#endif
