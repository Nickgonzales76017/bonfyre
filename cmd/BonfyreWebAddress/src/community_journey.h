#ifndef BONFYRE_COMMUNITY_JOURNEY_WEB_H
#define BONFYRE_COMMUNITY_JOURNEY_WEB_H
#include "ui_tree.h"
#include "form_tree.h"
#include "../generated/native_estate_hosts.h"
#include "../../BonfyreCMS/src/community_read.h"
/* Community UI over existing records; account identity comes only from Auth. */
typedef struct {char operation[24],community[97],target[97],expected[65],request[33],title[161],text[6001],interest[48],when[20],where[161];} CjwForm;
static void cjw_href(const char *key,const char *tab,const char *post){
 if(tab&&!strcmp(tab,"thread")&&post&&cjr_key(key)&&cjr_key(post)&&!strchr(post,'.')){
  fputs("/community/",stdout);html(key);fputs("/thread/",stdout);html(post);fputc('/',stdout);return;
 }
 fputs("/community?community=",stdout);url_query_value(key);if(tab&&*tab){fputs("&amp;tab=",stdout);url_query_value(tab);}if(post&&*post){fputs("&amp;post=",stdout);url_query_value(post);}
}
static void cjw_back(const char *key,const char *tab,const char *post){
 if(tab&&!strcmp(tab,"thread")&&cjr_key(key)&&cjr_key(post)&&!strchr(post,'.')){
  char link[512];snprintf(link,sizeof(link),"/community/%s/thread/%s/",key,post);page_redirect(link);return;
 }
 char location[512];snprintf(location,sizeof(location),"/community?community=%s&tab=%s%s%s",key,tab&&*tab?tab:"conversations",post&&*post?"&post=":"",post&&*post?post:"");page_redirect(location);
}
static void cjw_signin(const char *path){
 char location[2048];const char *hex="0123456789ABCDEF";size_t n=(size_t)snprintf(location,sizeof(location),"%s?next=",account_signin_path?account_signin_path:"/login");
 for(const unsigned char *p=(const unsigned char*)path;*p;p++){if(n+4>=sizeof(location)){ph_error(400,"The return address is too long.\n");return;}if(isalnum(*p)||strchr("-._~",*p))location[n++]=(char)*p;else{location[n++]='%';location[n++]=hex[*p>>4];location[n++]=hex[*p&15];}}location[n]=0;page_redirect(location);
}
static void cjw_excerpt(const char *text){
 size_t size=strlen(text),n=size>600?600:size;char excerpt[601];
 if(n<size)while(n&&(((unsigned char)text[n]&0xc0)==0x80))n--;
 memcpy(excerpt,text,n);excerpt[n]=0;html(excerpt);if(n<size)ui_copy(stdout,"…");
}
/* Search representation belongs to the published community conversation,
 * not to a generated copy or a second search-only content store. */
typedef struct {
    sqlite3 *db;
    const CjrCommunity *community;
    const CjrPost *post;
    const char *root;
} CjwSearchThread;

static void cjw_schema_text(const char *value) {
    fputc('"', stdout);
    for (const unsigned char *at=(const unsigned char*)(value?value:""); *at; ++at) {
        unsigned char ch=*at;
        if (ch=='"' || ch=='\\') { fputc('\\',stdout); fputc(ch,stdout); }
        else if (ch<32 || ch=='<' || ch=='>' || ch=='&')
            printf("\\u%04x", (unsigned)ch);
        else fputc(ch,stdout);
    }
    fputc('"',stdout);
}
static int cjw_schema_date(const char *date) {
    return date && strlen(date)>=10 && strlen(date)<64 && date[4]=='-' &&
           date[7]=='-' && strspn(date,"0123456789-:TZ+. ")==strlen(date);
}
static void cjw_search_thread_head(void *context) {
    CjwSearchThread *c=(CjwSearchThread*)context;
    const CjrPost *post=c->post;
    const char *root=c->root&&*c->root?c->root:post->key;
    char url[512],desc[281],author[100];
    size_t size=strlen(post->body), take=size<280?size:280;
    if (!cjr_key(c->community->key)||!cjr_key(root)||!cjr_visible(post)) return;
    while (take && (unsigned char)post->body[take] >= 0x80 &&
           ((unsigned char)post->body[take]&0xc0)==0x80) --take;
    memcpy(desc,post->body,take);desc[take]=0;
    snprintf(url,sizeof(url),BF_ORIGIN_LIFE "/community/%s/thread/%s/",
             c->community->key,root);
    snprintf(author,sizeof(author),"Member %s",
             cjr_actor(post->author)?post->author+5:"unknown");
    ui_start(stdout,"link");ui_attr(stdout,"rel","canonical");ui_attr_open(stdout,"href");html(url);
    ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"meta");ui_attr(stdout,"name","description");ui_attr_open(stdout,"content");html(desc);
    ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"meta");ui_attr(stdout,"property","og:type");ui_attr(stdout,"content","article");ui_gt(stdout);
    ui_start(stdout,"meta");ui_attr(stdout,"property","og:site_name");ui_attr(stdout,"content","Aurekai Life");ui_gt(stdout);
    ui_start(stdout,"meta");ui_attr(stdout,"property","og:url");ui_attr_open(stdout,"content");html(url);
    ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"meta");ui_attr(stdout,"property","og:title");ui_attr_open(stdout,"content");
    html(post->title[0]?post->title:c->community->title);
    ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"meta");ui_attr(stdout,"property","og:description");ui_attr_open(stdout,"content");
    html(desc);ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"meta");ui_attr(stdout,"name","twitter:card");ui_attr(stdout,"content","summary");ui_gt(stdout);
    /* Replies have the original discussion as their canonical; only a
     * visible root publishes DiscussionForumPosting with real human copy. */
    if (*post->parent || !cjw_schema_date(post->at) || !cjr_actor(post->author)) return;
    ui_start(stdout,"script");ui_attr(stdout,"type","application/ld+json");ui_gt(stdout);
    ui_lit(stdout,"{\"@context\":\"https://schema.org\",\"@type\":\"DiscussionForumPosting\","
          "\"mainEntityOfPage\":");cjw_schema_text(url);
    ui_lit(stdout,",\"url\":");cjw_schema_text(url);
    ui_lit(stdout,",\"headline\":");
    cjw_schema_text(post->title[0]?post->title:c->community->title);
    ui_lit(stdout,",\"text\":");cjw_schema_text(post->body);
    ui_lit(stdout,",\"author\":{\"@type\":\"Person\",\"name\":");
    cjw_schema_text(author);ui_lit(stdout,"},\"datePublished\":");
    cjw_schema_text(post->at);
    sqlite3_stmt *st=NULL;
    if (sqlite3_prepare_v2(c->db,
         "SELECT post FROM community_post WHERE namespace='root' "
         "AND community=?1 AND reply_to=?2 AND visibility='public' "
         "AND status='published' AND julianday(published_at)<=julianday('now') AND moderation_state IN ('accepted','visible','published') "
         "ORDER BY id ASC LIMIT 20",-1,&st,NULL)==SQLITE_OK) {
        sqlite3_bind_text(st,1,c->community->key,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,post->key,-1,SQLITE_TRANSIENT);
        int shown=0;
        while(sqlite3_step(st)==SQLITE_ROW) {
            const char *key=(const char*)sqlite3_column_text(st,0);
            CjrPost reply; char reply_author[100];
            if (!key||cjr_post(c->db,c->community->key,key,&reply)!=1||
                !cjr_visible(&reply)||!cjr_actor(reply.author)||
                !cjw_schema_date(reply.at))continue;
            if (!shown++)ui_lit(stdout,",\"comment\":[");
            else fputc(',',stdout);
            snprintf(reply_author,sizeof(reply_author),"Member %s",reply.author+5);
            ui_lit(stdout,"{\"@type\":\"Comment\",\"text\":");
            cjw_schema_text(reply.body);
            ui_lit(stdout,",\"datePublished\":");cjw_schema_text(reply.at);
            ui_lit(stdout,",\"author\":{\"@type\":\"Person\",\"name\":");
            cjw_schema_text(reply_author);ui_lit(stdout,"}}");
        }
        if(shown)fputc(']',stdout);
    }
    sqlite3_finalize(st);
    ui_lit(stdout,"}");ui_close(stdout,"script");
}
/* One exact release predicate for dynamic discussion discovery. The same
 * published/moderated human roots feed both the URL set and static export. */
/* The native CMS read owner supplies the exact public conversation cohort. */
#define CJW_SEARCH_PUBLIC_FROM CJR_PUBLIC_DISCUSSION_FROM
#define CJW_SEARCH_PER_SITEMAP 400
#define CJW_SEARCH_MAX_SITEMAPS 2000
static int cjw_search_public_root(sqlite3 *db,const char *community,const char *root) {
    if(!db||!cjr_key(community)||!cjr_key(root)||!strcmp(community,".")||
       !strcmp(community,"..")||!strcmp(root,".")||!strcmp(root,".."))return 0;
    sqlite3_stmt *st=NULL;
    if(sqlite3_prepare_v2(db,
       "SELECT 1 " CJW_SEARCH_PUBLIC_FROM
       " AND p.community=?1 AND p.post=?2 LIMIT 1",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,community,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,root,-1,SQLITE_TRANSIENT);
    int valid=sqlite3_step(st)==SQLITE_ROW;
    sqlite3_finalize(st);
    return valid;
}
static int cjw_search_current_date(sqlite3 *db,const CjrPost *post) {
    if(!db||!post||!cjr_actor(post->author)||!cjw_schema_date(post->at))return 0;
    sqlite3_stmt *st=NULL;
    if(sqlite3_prepare_v2(db,
        "SELECT julianday(?1) IS NOT NULL AND julianday(?1)<=julianday('now')",
        -1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,post->at,-1,SQLITE_TRANSIENT);
    int valid=sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_int(st,0)==1;
    sqlite3_finalize(st);
    return valid;
}
static int cjw_search_sitemap_pages(sqlite3 *db) {
    sqlite3_stmt *st=NULL;
    sqlite3_int64 count=0;
    if(!db || sqlite3_prepare_v2(db,
       "SELECT COUNT(*) " CJW_SEARCH_PUBLIC_FROM,-1,&st,NULL)!=SQLITE_OK)
       return -1;
    int step=sqlite3_step(st);
    if(step==SQLITE_ROW)count=sqlite3_column_int64(st,0);
    sqlite3_finalize(st);
    if(step!=SQLITE_ROW || count<0 ||
       count>(sqlite3_int64)CJW_SEARCH_PER_SITEMAP*CJW_SEARCH_MAX_SITEMAPS)
       return -1;
    return (int)((count+CJW_SEARCH_PER_SITEMAP-1)/CJW_SEARCH_PER_SITEMAP);
}
static void cjw_begin_search(const char *title,const char *key,const char *description,int code,
                             CjwSearchThread *thread,int public_response){
 printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",code,code==200?"OK":code==409?"Conflict":code==403?"Forbidden":"Unavailable",
        public_response&&code==200?"public, max-age=60, stale-while-revalidate=300":"private, no-store");
 if(thread)ak_app_open_with_head(title,"directory","route-community",cjw_search_thread_head,thread);
 else ak_app_open(title,"directory","route-community");
 ui_start(stdout,"div");ui_attr(stdout,"class","cj-shell");ui_flag(stdout,"data-community-journey");ui_attr_open(stdout,"data-community");html(key?key:"");ui_attr_close(stdout);ui_gt(stdout);ui_start(stdout,"nav");ui_attr(stdout,"class","cj-breadcrumbs");ui_attr(stdout,"aria-label","Breadcrumb");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"href","/communities");ui_gt(stdout);ui_copy(stdout,"Communities");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/guilds");ui_gt(stdout);ui_copy(stdout,"Guilds");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/profile?tab=communities");ui_gt(stdout);ui_copy(stdout,"My communities");ui_close(stdout,"a");ui_close(stdout,"nav");ui_open(stdout,"header","ak-wb-hero is-solo");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"People and shared interests");ui_close(stdout,"span");ui_open(stdout,"h1",NULL);html(title);ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");html(description?description:"");ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"header");
}
static void cjw_begin(const char *title,const char *key,const char *description,int code){
 cjw_begin_search(title,key,description,code,NULL,0);
}
static void cjw_end(void){ui_close(stdout,"div");ak_app_close();}
static void cjw_form_fields(const CjwForm *p){ph_hidden("operation",p->operation);ph_hidden("community",p->community);ph_hidden("target",p->target);ph_hidden("expected",p->expected);ph_hidden("request",p->request);ph_hidden("title",p->title);ph_hidden("text",p->text);ph_hidden("interest",p->interest);ph_hidden("when",p->when);ph_hidden("where",p->where);}
static void cjw_action(const char *op,const char *key,const char *target,const char *expected,const char *label){
 const FtField hidden[]={{"operation",op},{"community",key},{"target",target?target:""},{"expected",expected?expected:""}};
 const FtSubmit go[]={{.label=label,.hook="community-action",.hook_value=op}};
 const FtForm f={.id="cj-action",.action="/community/action",.hidden=hidden,.nhidden=4,.submits=go,.nsubmits=1};
 ft_form(stdout,&f);
}
static void cjw_name(const char *actor,const char *viewer,const char *owner){if(viewer&&*viewer&&!strcmp(actor,viewer))html("You");else if(owner&&*owner&&!strcmp(actor,owner))html("Community steward");else{ui_copy(stdout,"Member ");html(cjr_actor(actor)?actor+5:"unknown");}}
static int cjw_member_state(sqlite3 *db,const CjrCommunity *c,const char *actor,CjrMember *m){if(!actor||!*actor){memset(m,0,sizeof(*m));snprintf(m->digest,sizeof(m->digest),"absent");return 0;}return cjr_member(db,c->key,actor,m);}
static void cjw_member_actions(const CjrCommunity *c,const char *actor,const CjrMember *m){
 ui_open(stdout,"div","cj-actions");
 if(!*actor){ui_start(stdout,"a");ui_attr(stdout,"class","cj-button");ui_attr_open(stdout,"href");cjw_href(c->key,"join",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Sign in to take part");ui_close(stdout,"a");}
 else if(!strcmp(c->owner,actor)||m->active){ui_start(stdout,"a");ui_attr(stdout,"class","cj-button");ui_attr_open(stdout,"href");cjw_href(c->key,"write",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Start a conversation");ui_close(stdout,"a");}
 else if(!m->id||!strcmp(m->state,"withdrawn")){ui_start(stdout,"a");ui_attr(stdout,"class","cj-button");ui_attr_open(stdout,"href");cjw_href(c->key,"join",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Request to join");ui_close(stdout,"a");}
 else{ui_start(stdout,"p");ui_attr_open(stdout,"data-membership-state");html(m->state);ui_attr_close(stdout);ui_gt(stdout);html(!strcmp(m->state,"requested")?"Your request is awaiting the steward.":!strcmp(m->state,"declined")?"Your request was declined.":!strcmp(m->state,"revoked")?"Your participation has ended.":"Your membership is not currently active.");ui_close(stdout,"p");}
 ui_close(stdout,"div");
 if(*actor&&strcmp(c->owner,actor)&&m->id&&!strcmp(m->state,"requested")){ui_open(stdout,"div","cj-leave");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Withdraw my request");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your contributions keep their own history. This does not change any other community.");ui_close(stdout,"p");cjw_action("leave",c->key,"",m->digest,"Withdraw request");ui_close(stdout,"div");}
 if(*actor&&strcmp(c->owner,actor)&&m->id&&!strcmp(m->state,"active")){ui_open(stdout,"div","cj-leave");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Leave this community");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your contributions keep their own history. This does not change any other community.");ui_close(stdout,"p");cjw_action("leave",c->key,"",m->digest,"Confirm leaving");ui_close(stdout,"div");}
}
/* A community does not become a guild merely because its title says guild. */
static int cjw_is_guild(sqlite3 *db,const char *key){
    sqlite3_stmt *s=NULL;int result=0;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM community_body WHERE namespace=\'root\' AND community=?1 AND governance_scope=\'local-guild\' AND status=\'published\' AND visibility=\'public\' LIMIT 1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,key,-1,SQLITE_TRANSIENT);result=sqlite3_step(s)==SQLITE_ROW;
    }sqlite3_finalize(s);return result;
}
#include "community_programs.h"
static void cjw_tabs(const CjrCommunity *c,const char *tab,int owner,int guild,int participant){
 const char *keys[]={"conversations","about","members","settings"},*names[]={"Conversations","About","People and requests","Edit community"};ui_start(stdout,"nav");ui_attr(stdout,"class","cj-tabs");ui_attr(stdout,"aria-label","Community sections");ui_gt(stdout);for(int i=0;i<4;i++){if(i>=2&&!owner)continue;ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,keys[i],NULL);ui_attr_close(stdout);if(!strcmp(tab,keys[i])||(!i&&!strcmp(tab,"thread")))ui_attr(stdout,"aria-current","page");ui_gt(stdout);html(names[i]);ui_close(stdout,"a");}if(guild&&participant){ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,"programs",NULL);ui_attr_close(stdout);if(!strcmp(tab,"programs"))ui_attr(stdout,"aria-current","page");ui_gt(stdout);ui_copy(stdout,"Programmes and work");ui_close(stdout,"a");}ui_close(stdout,"nav");
}
static int cjw_parse(const char *body,CjwForm *p){
 memset(p,0,sizeof(*p));PhField f[]={{"operation",p->operation,sizeof(p->operation),0},{"community",p->community,sizeof(p->community),0},{"target",p->target,sizeof(p->target),0},{"expected",p->expected,sizeof(p->expected),0},{"request",p->request,sizeof(p->request),0},{"title",p->title,sizeof(p->title),0},{"text",p->text,sizeof(p->text),1},{"interest",p->interest,sizeof(p->interest),0},{"when",p->when,sizeof(p->when),0},{"where",p->where,sizeof(p->where),0}};if(!ph_fields(body,f,10,40000)||!*p->operation)return 0;
 /* A conversation can be a plan: when (local time from the form) and where. */
 if(*p->when){const char *e=p->when;if(strlen(e)!=16||e[4]!='-'||e[7]!='-'||e[10]!='T'||e[13]!=':')return 0;}
 /* What a new community is about, in the words members use for their own
  * interests: folded the same way, so a community about "Sci-Fi" is the club
  * for everyone who said sci-fi. */
 if(*p->interest){char clean[48];if(!ph_interest_label(p->interest,clean,sizeof(clean)))return 0;snprintf(p->interest,sizeof(p->interest),"%s",clean);}return 1;
}
static void cjw_editor(const CjrCommunity *c,const CjrPost *post,const char *operation){
 char nonce[33];ph_nonce(nonce);int guild=!strcmp(operation,"create-guild"),creating=!strcmp(operation,"create")||guild,replying=!strcmp(operation,"reply"),editing=!strcmp(operation,"edit"),settings=!strcmp(operation,"edit-community");
 ui_open(stdout,"h2",NULL);html(guild?"Start a public guild":creating?"Start a public community":replying?"Reply in this conversation":editing?"Edit your contribution":settings?"Edit this community":"Start a conversation");ui_close(stdout,"h2");ui_open(stdout,"p","cj-notice");html(guild?"Guilds offer steward-reviewed local participation and conversation. No apprenticeship, credential, equipment use or payment is granted.":creating?"Give people a clear purpose and describe how you want to take part together. You will review the public page before creating it.":"This contribution will be public in this community. It will not publish on your profile sites or on an external platform.");ui_close(stdout,"p");
 if(replying&&post){ui_text(stdout, "blockquote", "cj-review", post->body);}
 ui_start(stdout,"form");ui_attr(stdout,"class","cj-form");ui_attr(stdout,"action","/community/preview");ui_attr(stdout,"method","post");ui_gt(stdout);ph_hidden("operation",operation);ph_hidden("community",c?c->key:"");ph_hidden("target",post?post->key:"");ph_hidden("expected",post?post->digest:settings&&c?c->digest:"");ph_hidden("request",nonce);
 if(!replying){ui_open(stdout,"label",NULL);html(guild?"Guild name":creating||settings?"Community name":"Conversation title");ui_start(stdout,"input");ui_attr(stdout,"name","title");ui_flag(stdout,"required");ui_attr(stdout,"maxlength","160");ui_attr_open(stdout,"value");html(editing&&post?post->title:settings&&c?c->title:"");ui_attr_close(stdout);ui_gt(stdout);ui_close(stdout,"label");}else ph_hidden("title","");
 if(creating&&!guild){ui_open(stdout,"label",NULL);ui_copy(stdout,"What is it about? One interest, like sci-fi or Dallas food (optional)");ui_start(stdout,"input");ui_attr(stdout,"name","interest");ui_attr(stdout,"maxlength","40");ui_gt(stdout);ui_close(stdout,"label");}
 if(!strcmp(operation,"post")){ui_open(stdout,"label",NULL);ui_copy(stdout,"When, if it is something to meet for (optional)");ui_start(stdout,"input");ui_attr(stdout,"name","when");ui_attr(stdout,"type","datetime-local");ui_gt(stdout);ui_close(stdout,"label");ui_open(stdout,"label",NULL);ui_copy(stdout,"Where (optional)");ui_start(stdout,"input");ui_attr(stdout,"name","where");ui_attr(stdout,"maxlength","160");ui_gt(stdout);ui_close(stdout,"label");}
 ui_open(stdout,"label",NULL);html(creating||settings?"Purpose and community guidelines":"Your contribution");ui_start(stdout,"textarea");ui_attr(stdout,"name","text");ui_attr(stdout,"rows","9");ui_flag(stdout,"required");ui_attr(stdout,"maxlength","6000");ui_gt(stdout);html(editing&&post?post->body:settings&&c?c->description:"");ui_close(stdout,"textarea");ui_close(stdout,"label");ui_start(stdout,"button");ui_attr(stdout,"type","submit");ui_gt(stdout);ui_copy(stdout,"Review before publishing");ui_close(stdout,"button");ui_close(stdout,"form");
}
static int cjw_reviewable(sqlite3 *db,const CjwForm *p,const char *actor){
 if((!strcmp(p->operation,"create")||!strcmp(p->operation,"create-guild")))return !*p->community&&!*p->target&&!*p->expected&&strlen(p->request)==32&&strspn(p->request,"0123456789abcdef")==32&&studio_safe_title(p->title)&&*p->text;
 CjrCommunity c;CjrMember m;if(cjr_community(db,p->community,&c)!=1||!cjr_public(&c)||cjr_member(db,c.key,actor,&m)<0)return 0;int owner=!strcmp(c.owner,actor);
 if(!strcmp(p->operation,"edit-community"))return owner&&!strcmp(p->expected,c.digest)&&studio_safe_title(p->title)&&*p->text;
 if(!owner&&!m.active)return 0;
 if(!strcmp(p->operation,"post"))return !*p->target&&studio_safe_title(p->title)&&*p->text&&strlen(p->request)==32&&strspn(p->request,"0123456789abcdef")==32;
 CjrPost post;if(cjr_post(db,c.key,p->target,&post)!=1||!cjr_visible(&post)||strcmp(p->expected,post.digest)||!*p->text)return 0;
 if(!strcmp(p->operation,"edit"))return !strcmp(post.author,actor)&&(*post.parent||studio_safe_title(p->title));
 if(!strcmp(p->operation,"reply")){char root[97];int depth=cjr_thread(db,c.key,&post,root,1);return depth>0&&depth<9&&strlen(p->request)==32&&strspn(p->request,"0123456789abcdef")==32;}
 return 0;
}
static void cjw_preview(const CjwForm *p,const char *name){
 cjw_begin("Review your public contribution",p->community,name,200);ui_open(stdout,"p","cj-notice");ui_copy(stdout,"This is public community content. Your private saved material and independently published profile sites stay unchanged.");ui_close(stdout,"p");ui_open(stdout,"article","cj-review");ui_open(stdout,"h2",NULL);html(p->title);ui_close(stdout,"h2");ui_open(stdout,"p",NULL);html(p->text);ui_close(stdout,"p");ui_close(stdout,"article");ui_start(stdout,"form");ui_attr(stdout,"method","post");ui_attr(stdout,"action","/community/action");ui_gt(stdout);cjw_form_fields(p);ui_start(stdout,"button");ui_attr(stdout,"type","submit");ui_flag(stdout,"data-community-publish");ui_gt(stdout);html(!strcmp(p->operation,"create-guild")?"Create this guild":!strcmp(p->operation,"create")?"Create this community":"Publish this contribution");ui_close(stdout,"button");ui_close(stdout,"form");ui_open(stdout,"p",NULL);ui_copy(stdout,"You can return to the previous form to revise your words.");ui_close(stdout,"p");cjw_end();
}
static int cjw_native_string(bf_mp_reader_t *r,char *out,size_t cap){bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR||v.v.raw.len>=cap||memchr(v.v.raw.ptr,0,v.v.raw.len))return 0;memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;return 1;}
static int cjw_commit(const CjwForm *p,const char *actor){
 const char *bin=getenv("BONFYRE_CMS_BINARY"),*path=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");if(!bin||!path||!schemas){ph_error(503,"The community owner is unavailable.\n");return 1;}
 bf_mp_writer_t w;bf_mp_writer_init(&w,1024);int ok=!bf_mp_pack_array(&w,10)&&!bf_mp_pack_uint(&w,1);const char *parts[]={p->community,p->target,p->expected,p->request,p->title,p->text,p->interest,p->when,p->where};for(int i=0;i<9&&ok;i++)ok=!bf_mp_pack_str_cstr(&w,parts[i]);if(!ok||w.len>16384){bf_mp_writer_free(&w);ph_error(413,"This contribution exceeds its supported size.\n");return 1;}
 char *args[]={(char*)bin,"entry","community-work","--actor",(char*)actor,"--operation",(char*)p->operation,"--db",(char*)path,"--schemas",(char*)schemas,NULL};unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(args,w.buf,w.len,&output,&size,1);bf_mp_writer_free(&w);
 char state[32]="",community[97]="",target[97]="",digest[65]="",message[512]="",delta[32]="";bf_mp_reader_t r;bf_mp_value_t v;bf_mp_reader_init(&r,output,size);ok=output&&size<=4096&&bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count==7;
 if(ok){bf_mp_type_t t=bf_mp_read_next(&r,&v);ok=(t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1);}char *values[]={state,community,target,digest,message,delta};size_t caps[]={sizeof(state),sizeof(community),sizeof(target),sizeof(digest),sizeof(message),sizeof(delta)};for(int i=0;i<6&&ok;i++)ok=cjw_native_string(&r,values[i],caps[i]);ok=ok&&r.pos==r.len;free(output);
 if(ok&&!rc&&(!strcmp(state,"committed")||!strcmp(state,"unchanged"))&&cjr_key(community)){
  if(!strcmp(p->operation,"post")||!strcmp(p->operation,"reply")||!strcmp(p->operation,"edit")||!strcmp(p->operation,"remove")||!strcmp(p->operation,"hide")){cjw_back(community,"thread",target);return 1;}
  cjw_back(community,(!strcmp(p->operation,"accept")||!strcmp(p->operation,"decline")||!strcmp(p->operation,"revoke")||!strcmp(p->operation,"transfer"))?"members":"conversations",NULL);return 1;
 }
 int code=!ok?503:!strcmp(state,"forbidden")?403:!strcmp(state,"not_found")?404:!strcmp(state,"conflict")?409:!strcmp(state,"invalid")?400:503;
 cjw_begin(ok?"This change needs your attention":"Check the current community",p->community,"",code);ui_start(stdout,"p");ui_attr(stdout,"class","cj-notice");ui_attr(stdout,"role","alert");ui_gt(stdout);html(ok?message:"The reply was interrupted. Read the current community before resending the same reviewed request. No retry was sent automatically.");ui_close(stdout,"p");
 if(cjr_key(p->community)){ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(p->community,"conversations",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open the current community");ui_close(stdout,"a");ui_close(stdout,"p");}else {ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"href","/profile?tab=communities");ui_gt(stdout);ui_copy(stdout,"Check your communities");ui_close(stdout,"a");ui_close(stdout,"p");}
 if(!ok){ui_open(stdout,"div","cj-retry");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Keep this exact request");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"First check whether the change is already visible. This form retains its original identity and will not overwrite newer work.");ui_close(stdout,"p");ui_start(stdout,"form");ui_attr(stdout,"method","post");ui_attr(stdout,"action","/community/action");ui_gt(stdout);cjw_form_fields(p);ui_start(stdout,"button");ui_attr(stdout,"type","submit");ui_gt(stdout);ui_copy(stdout,"Retry this exact reviewed request");ui_close(stdout,"button");ui_close(stdout,"form");ui_close(stdout,"div");}else if(*p->text){ui_open(stdout,"div","cj-words");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Your submitted words");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"pre","cj-review");html(p->text);ui_close(stdout,"pre");ui_close(stdout,"div");}cjw_end();return 1;
}
static void cjw_card(sqlite3 *db,const CjrCommunity *c,const CjrPost *p,const char *actor,int full,int can_post){
 ui_start(stdout,"article");ui_attr(stdout,"class","cj-thread");ui_attr_open(stdout,"data-community-post");html(p->key);ui_attr_close(stdout);ui_gt(stdout);ui_open(stdout,"div","cj-meta");cjw_name(p->author,actor,c->owner);ui_copy(stdout," · ");ui_open(stdout,"time",NULL);html(p->at);ui_close(stdout,"time");ui_close(stdout,"div");
 if(cjr_visible(p)&&cjw_search_current_date(db,p)){if(*p->title){ui_open(stdout,"h2",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,"thread",p->key);ui_attr_close(stdout);ui_gt(stdout);html(p->title);ui_close(stdout,"a");ui_close(stdout,"h2");}ui_open(stdout,"p",NULL);if(full)html(p->body);else cjw_excerpt(p->body);ui_close(stdout,"p");}else ui_text(stdout, "p", "cj-removed", "This contribution is no longer public.");
 ui_start(stdout,"nav");ui_attr(stdout,"aria-label","Conversation actions");ui_gt(stdout);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,"thread",p->key);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open conversation");ui_close(stdout,"a");char root[97];int depth=cjr_thread(db,c->key,p,root,1);
 if(can_post&&depth>0&&depth<9){ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,"reply",p->key);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Reply");ui_close(stdout,"a");}if(*actor&&!strcmp(p->author,actor)&&cjr_visible(p)&&can_post){ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,"edit",p->key);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Edit my contribution");ui_close(stdout,"a");}ui_close(stdout,"nav");
 if(*actor&&cjr_visible(p)&&!strcmp(p->author,actor)){ui_open(stdout,"div","cj-moderate");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Remove my contribution");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Its text will stop being public. Other people's replies remain their own contributions.");ui_close(stdout,"p");cjw_action("remove",c->key,p->key,p->digest,"Confirm removal");ui_close(stdout,"div");}
 if(*actor&&cjr_visible(p)&&strcmp(p->author,actor)&&!strcmp(c->owner,actor)){ui_open(stdout,"div","cj-moderate");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Moderate this contribution");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Its text will stop being public. Other people's replies remain their own contributions.");ui_close(stdout,"p");cjw_action("hide",c->key,p->key,p->digest,"Hide this contribution");ui_close(stdout,"div");}
 if(*actor&&cjr_visible(p)&&strcmp(p->author,actor)&&can_post){ui_open(stdout,"div","cj-flag");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Flag for the steward");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"It hides from you now and waits in the steward queue. A flag is not a verdict and never implies guilt.");ui_close(stdout,"p");
  {const FtField hidden[]={{"operation","flag"},{"community",c->key},{"target",p->key},{"expected",p->digest}};
   static const FtOption reasons[]={{"spam","Spam or scam",0},{"harassment","Harassment or abuse",0},{"fake","False or misleading",0},{"minor-safety","Minor safety",0},{"other","Something else",0}};
   const FtInput in[]={{.label="Reason",.type="select",.name="title",.options=reasons,.noptions=5},{.label="Details for the steward (optional)",.name="text",.maxlength="600"}};
   const FtSubmit go[]={{.label="Send flag",.hook="community-action",.hook_value="flag"}};
   const FtForm f={.id="cj-flag",.action="/community/action",.hidden=hidden,.nhidden=4,.inputs=in,.ninputs=2,.submits=go,.nsubmits=1};
   ft_form(stdout,&f);}
  ui_close(stdout,"div");}ui_close(stdout,"article");
}
static int cjw_posts(sqlite3 *db,const CjrCommunity *c,const char *parent,const char *actor,int can_post,long long cursor){
 sqlite3_stmt *s=NULL,*t=NULL;int flagged=0;
 if(sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name='community_flag'",-1,&t,NULL)==SQLITE_OK){flagged=sqlite3_step(t)==SQLITE_ROW;sqlite3_finalize(t);}
 const char *base="SELECT post,id FROM community_post WHERE namespace='root' AND community=?1 AND COALESCE(reply_to,'')=?2 AND visibility='public' AND status IN ('published','archived') AND moderation_state IN ('accepted','visible','published','withdrawn','hidden') AND (status='archived' OR (julianday(published_at) IS NOT NULL AND julianday(published_at)<=julianday('now'))) AND (?3=0 OR id<?3)";
 char sql[2048];snprintf(sql,sizeof(sql),"%s%s ORDER BY id DESC LIMIT 25",base,(flagged&&actor&&*actor)?" AND NOT EXISTS (SELECT 1 FROM community_flag f WHERE f.namespace='root' AND f.community=?1 AND f.post=community_post.post AND f.actor_subject=?4 AND f.status='published')":"");
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,c->key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,parent?parent:"",-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor);if(flagged&&actor&&*actor)sqlite3_bind_text(s,4,actor,-1,SQLITE_STATIC);int count=0,more=0,step;long long last=0;
 while((step=sqlite3_step(s))==SQLITE_ROW){if(count==24){more=1;break;}CjrPost p;const char *key=(const char*)sqlite3_column_text(s,0);if(cjr_post(db,c->key,key,&p)!=1)continue;count++;last=sqlite3_column_int64(s,1);cjw_card(db,c,&p,actor,parent&&*parent,can_post);}sqlite3_finalize(s);
 if(!count)ui_text(stdout, "div", "cj-notice", "No conversations in this view yet. An active member can start one.");
 if(more){ui_start(stdout,"a");ui_attr(stdout,"class","cj-next");ui_attr_open(stdout,"href");cjw_href(c->key,parent&&*parent?"thread":"conversations",parent);ui_copy(stdout,parent&&*parent?"?":"&amp;");ui_copy(stdout,"cursor=");printf("%lld",last);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Earlier contributions");ui_close(stdout,"a");}return step==SQLITE_DONE||step==SQLITE_ROW;
}
/* One count over the community owner's own tables; -1 when unreadable. */
static long long cjw_count(sqlite3 *db,const char *sql,const char *key,int guilds){
 sqlite3_stmt *s=NULL;long long n=-1;if(!db||sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,CJR_POLICY,-1,SQLITE_STATIC);if(sqlite3_bind_parameter_count(s)>=2)sqlite3_bind_text(s,2,key?key:"",-1,SQLITE_STATIC);if(sqlite3_bind_parameter_count(s)>=3)sqlite3_bind_int(s,3,guilds);
 if(sqlite3_step(s)==SQLITE_ROW)n=sqlite3_column_int64(s,0);sqlite3_finalize(s);return n;
}
static void cjw_stat(const char *label,long long n,const char *note){ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);html(label);ui_close(stdout,"dt");ui_start(stdout,"dd");if(n>0)ui_attr(stdout,"class","is-on");ui_gt(stdout);printf("%lld",n<0?0:n);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);html(note);ui_close(stdout,"small");ui_close(stdout,"div");}
/* The guild and community registry: the Stitch guild directory over the
 * community owner's published public bodies, grouped by their own interest
 * label. Counts are read, never estimated; an empty registry says so. */
static void cjw_directory(sqlite3 *db,const char *q,long long cursor,int guilds){
 const char *route=guilds?"/guilds":"/communities";
 printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n");
 ak_app_open(guilds?"Guilds":"Communities","directory","route-community route-guilds");
 static const char scope[]=" FROM community_body b WHERE b.namespace='root' AND b.status='published' AND b.visibility='public' AND b.moderation_policy=?1 AND (?2='' OR 1) AND (?3=0 OR b.governance_scope='local-guild') AND length(b.created_by_subject) BETWEEN 6 AND 23 AND b.created_by_subject GLOB 'user:[1-9]*' AND substr(b.created_by_subject,6) NOT GLOB '*[^0-9]*'";
 char sql[2048];
 snprintf(sql,sizeof(sql),"SELECT COUNT(*)%s",scope);long long bodies=cjw_count(db,sql,"",guilds);
 snprintf(sql,sizeof(sql),"SELECT COUNT(DISTINCT m.member_subject) FROM community_membership m WHERE m.namespace='root' AND m.status='published' AND m.state='active' AND (COALESCE(m.expires_at,'')='' OR julianday(m.expires_at)>julianday('now')) AND m.community IN (SELECT b.community%s)",scope);long long members=cjw_count(db,sql,"",guilds);
 snprintf(sql,sizeof(sql),"SELECT COUNT(*) " CJW_SEARCH_PUBLIC_FROM " AND p.community IN (SELECT b.community%s)",scope);long long posts=cjw_count(db,sql,"",guilds);
 snprintf(sql,sizeof(sql),"SELECT COUNT(DISTINCT NULLIF(b.interest_label,'')) %s",scope);long long crafts=cjw_count(db,sql,"",guilds);
 ui_open(stdout,"div","ak-wb-strip");ui_open(stdout,"span",NULL);fputs(guilds?"Guild registry":"Community registry",stdout);ui_close(stdout,"span");ui_open(stdout,"span",NULL);ui_open(stdout,"b",NULL);fprintf(stdout,"%lld",bodies<0?0:bodies);ui_close(stdout,"b");ui_copy(stdout," public");ui_close(stdout,"span");ui_open(stdout,"span",NULL);ui_open(stdout,"b",NULL);fprintf(stdout,"%lld",members<0?0:members);ui_close(stdout,"b");ui_copy(stdout," active members");ui_close(stdout,"span");ui_open(stdout,"span","ak-live");ui_copy(stdout,"Read from the community records");ui_close(stdout,"span");ui_close(stdout,"div");
 ui_open(stdout,"header","ak-wb-hero is-solo");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");fputs(guilds?"Local guilds &middot; steward-reviewed":"Communities &middot; people and shared interests",stdout);ui_close(stdout,"span");ui_open(stdout,"h1",NULL);
 fputs(guilds?"Guilds: crafts kept by the people who practise them":"Find your people",stdout);ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");
 fputs(guilds?"Each guild is a public body with a steward who reviews who takes part. Read their conversations, ask to join, or start one around a craft you keep.":"Local communities with public conversations. Read along, ask to join, or start a shared space around something you care about.",stdout);
 ui_close(stdout,"p");ui_open(stdout,"div","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn is-primary");ui_attr_open(stdout,"href");fputs(guilds?"/guild/new\">Start a guild":"/community/new\">Start a community",stdout);ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn is-quiet");ui_attr(stdout,"href","/profile?tab=communities");ui_gt(stdout);ui_copy(stdout,"My communities");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"header");
 ui_open(stdout,"dl","ak-stats is-row");cjw_stat(guilds?"Public guilds":"Public communities",bodies,"published and open to read");cjw_stat("Active members",members,"accepted by a steward");cjw_stat("Conversations",posts,"public contributions");cjw_stat("Interests",crafts,"named by their stewards");ui_close(stdout,"dl");
 ui_start(stdout,"nav");ui_attr(stdout,"class","ak-chips");ui_attr(stdout,"aria-label","Registry");ui_gt(stdout);
 ui_start(stdout,"a");ui_attr(stdout,"class","ak-chip");ui_attr(stdout,"href","/communities");if(!guilds)ui_attr(stdout,"aria-current","page");ui_gt(stdout);ui_copy(stdout,"All communities");ui_close(stdout,"a");
 ui_start(stdout,"a");ui_attr(stdout,"class","ak-chip");ui_attr(stdout,"href","/guilds");if(guilds)ui_attr(stdout,"aria-current","page");ui_gt(stdout);ui_copy(stdout,"Guilds");ui_close(stdout,"a");
 {sqlite3_stmt *s=NULL;snprintf(sql,sizeof(sql),"SELECT b.interest_label,COUNT(*)%s AND COALESCE(b.interest_label,'')<>'' GROUP BY b.interest_label ORDER BY COUNT(*) DESC LIMIT 8",scope);
  if(db&&sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,CJR_POLICY,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,"",-1,SQLITE_STATIC);sqlite3_bind_int(s,3,guilds);while(sqlite3_step(s)==SQLITE_ROW){const char *l=(const char*)sqlite3_column_text(s,0);ui_start(stdout,"a");ui_attr(stdout,"class","ak-chip");ui_attr_open(stdout,"href");ui_copy(stdout,route);ui_copy(stdout,"?q=");url_query_value(l);ui_attr_close(stdout);if(q&&*q&&!strcmp(q,l))ui_attr(stdout,"aria-current","page");ui_gt(stdout);html(l);ui_copy(stdout," ");ui_open(stdout,"b",NULL);printf("%d",sqlite3_column_int(s,1));ui_close(stdout,"b");ui_close(stdout,"a");}}sqlite3_finalize(s);}
 ui_close(stdout,"nav");
 ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Registry");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,guilds?"Chambers of craft":"Communities");ui_close(stdout,"h2");ui_close(stdout,"div");
 {const FtInput find[]={{.label="Find by name or interest",.type="search",.name="q",.value=q,.placeholder="Find by name or interest"}};
  const FtForm f={.id="cj-search",.method="get",.action=route,.label="Find",.hook="community-search",.inputs=find,.ninputs=1};
  ft_form(stdout,&f);}
 ui_close(stdout,"header");
 sqlite3_stmt *s=NULL;int count=0,more=0;long long last=0;char group[161]="\x01";
 const char *list=
   "SELECT b.community,b.id,COALESCE(b.interest_label,''),b.governance_scope,"
   "(SELECT COUNT(*) FROM community_membership m WHERE m.namespace='root' AND m.community=b.community AND m.status='published' AND m.state='active' AND (COALESCE(m.expires_at,'')='' OR julianday(m.expires_at)>julianday('now'))),"
   "(SELECT COUNT(*) " CJW_SEARCH_PUBLIC_FROM " AND p.community=b.community) "
   "FROM community_body b WHERE b.namespace='root' AND b.status='published' AND b.visibility='public' AND b.moderation_policy=?1 "
   "AND length(b.created_by_subject) BETWEEN 6 AND 23 "
   "AND b.created_by_subject GLOB 'user:[1-9]*' AND substr(b.created_by_subject,6) NOT GLOB '*[^0-9]*' "
   "AND (?4=0 OR b.governance_scope='local-guild') "
   "AND (?2='' OR instr(lower(b.title||' '||COALESCE(b.description,'')||' '||COALESCE(b.interest_label,'')),lower(?2))>0) "
   "AND (?3=0 OR b.id<?3) ORDER BY COALESCE(b.interest_label,'')='',b.interest_label,b.id DESC LIMIT 25";
 if(db&&sqlite3_prepare_v2(db,list,-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,CJR_POLICY,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,q,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor);sqlite3_bind_int(s,4,guilds);
  while(sqlite3_step(s)==SQLITE_ROW){if(count==24){more=1;break;}CjrCommunity c;if(cjr_community(db,(const char*)sqlite3_column_text(s,0),&c)!=1||!cjr_public(&c))continue;
   const char *label=(const char*)sqlite3_column_text(s,2),*gov=(const char*)sqlite3_column_text(s,3);
   if(strcmp(group,label)){if(count)ui_close(stdout,"div");snprintf(group,sizeof(group),"%s",label);ui_open(stdout,"h3","cj-chamber");html(*label?label:guilds?"Other crafts":"Other interests");ui_close(stdout,"h3");ui_open(stdout,"div","ak-grid is-3");}
   count++;last=sqlite3_column_int64(s,1);
   ui_start(stdout,"article");ui_attr(stdout,"class","ak-panel cj-listing");ui_flag(stdout,"data-community-listing");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_start(stdout,"span");ui_attr_open(stdout,"class");ui_copy(stdout,"ak-tag");if(gov&&!strcmp(gov,"local-guild"))ui_copy(stdout," is-on");ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,gov&&!strcmp(gov,"local-guild")?"Guild":"Community");ui_close(stdout,"span");ui_close(stdout,"header");ui_open(stdout,"h3",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c.key,"conversations",NULL);ui_attr_close(stdout);ui_gt(stdout);html(c.title);ui_close(stdout,"a");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);cjw_excerpt(c.description);
   ui_close(stdout,"p");ui_open(stdout,"dl","ak-spec");ui_open(stdout,"dt",NULL);ui_copy(stdout,"Active members");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%d",sqlite3_column_int(s,4));ui_close(stdout,"dd");ui_open(stdout,"dt",NULL);ui_copy(stdout,"Conversations");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%d",sqlite3_column_int(s,5));ui_close(stdout,"dd");ui_close(stdout,"dl");ui_open(stdout,"div","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");cjw_href(c.key,"conversations",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Read");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn is-quiet");ui_attr_open(stdout,"href");cjw_href(c.key,"join",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Ask to join");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"article");}
  if(count)ui_close(stdout,"div");}
 sqlite3_finalize(s);
 if(!count){ui_open(stdout,"p","ak-empty");ui_copy(stdout,guilds?"No public guild is registered yet. A guild appears here once its steward publishes it.":"No public community matches this view. Try another interest or start one.");ui_close(stdout,"p");}
 if(more){ui_open(stdout,"p","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,route);ui_copy(stdout,"?cursor=");fprintf(stdout,"%lld",last);ui_copy(stdout,"&amp;q=");url_query_value(q);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"More");ui_close(stdout,"a");ui_close(stdout,"p");}
 ui_close(stdout,"section");ui_open(stdout,"section","ak-band");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");fputs(guilds?"Start a guild":"Start a community",stdout);ui_close(stdout,"span");ui_open(stdout,"h2",NULL);
 fputs(guilds?"Keep a craft? Open your bench to others.":"Something worth gathering around?",stdout);ui_close(stdout,"h2");ui_open(stdout,"p",NULL);
 fputs(guilds?"A guild is public, with a steward who accepts members. Nothing here grants credentials, equipment use or payment.":"A community is public, with a steward who accepts members and can hide a contribution.",stdout);
 ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,guilds?"/guild/new":"/community/new");ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,guilds?"Start a guild":"Start a community");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"section");
 ak_app_close();
}
static void cjw_members(sqlite3 *db,const CjrCommunity *c,long long cursor){
 ui_open(stdout,"h2",NULL);ui_copy(stdout,"People and requests");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"These decisions apply only to this community. They do not grant access to your account, other communities, integrations or money.");ui_close(stdout,"p");sqlite3_stmt *s=NULL;int count=0,more=0;long long last=0;
 if(sqlite3_prepare_v2(db,"SELECT member_subject,id FROM community_membership WHERE namespace='root' AND community=?1 AND status='published' AND member_subject<>?2 AND (?3=0 OR id<?3) ORDER BY id DESC LIMIT 25",-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,c->key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,c->owner,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor);while(sqlite3_step(s)==SQLITE_ROW){if(count==24){more=1;break;}CjrMember m;if(cjr_member(db,c->key,(const char*)sqlite3_column_text(s,0),&m)!=1)continue;count++;last=sqlite3_column_int64(s,1);ui_start(stdout,"article");ui_attr(stdout,"class","cj-member");ui_flag(stdout,"data-community-member");ui_gt(stdout);ui_open(stdout,"h3",NULL);cjw_name(m.actor,c->owner,c->owner);ui_close(stdout,"h3");ui_open(stdout,"p",NULL);html(m.state);if(!m.active&&!strcmp(m.state,"active"))ui_copy(stdout," · access expired");ui_close(stdout,"p");if(!strcmp(m.state,"requested")){cjw_action("accept",c->key,m.actor,m.digest,"Accept request");cjw_action("decline",c->key,m.actor,m.digest,"Decline request");}else if(m.active){ui_open(stdout,"div","cj-revoke");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"End this participation");ui_close(stdout,"strong");ui_close(stdout,"p");cjw_action("revoke",c->key,m.actor,m.digest,"Confirm removal from community");ui_close(stdout,"div");ui_open(stdout,"div","cj-transfer");ui_open(stdout,"p",NULL);ui_open(stdout,"strong",NULL);ui_copy(stdout,"Pass stewardship");ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"They become the steward and you stay on as a member. This cannot be undone except by them passing it back.");ui_close(stdout,"p");cjw_action("transfer",c->key,m.actor,c->digest,"Make steward");ui_close(stdout,"div");}ui_close(stdout,"article");}}sqlite3_finalize(s);if(!count)ui_text(stdout, "p", NULL, "No requests or other memberships in this view.");if(more){ui_start(stdout,"a");ui_attr(stdout,"class","cj-next");ui_attr_open(stdout,"href");cjw_href(c->key,"members",NULL);ui_copy(stdout,"&amp;cursor=");printf("%lld",last);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Earlier requests");ui_close(stdout,"a");}
 ui_open(stdout,"h2",NULL);ui_copy(stdout,"Flags awaiting review");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Member safety flags on contributions. A flag hides the post from its reporter only; nothing else changes without your decision.");ui_close(stdout,"p");
 {sqlite3_stmt *f=NULL;int fcount=0;
  if(sqlite3_prepare_v2(db,"SELECT f.post,f.reason,f.actor_subject,f.created_at,p.title FROM community_flag f JOIN community_post p ON p.namespace='root' AND p.community=f.community AND p.post=f.post WHERE f.namespace='root' AND f.community=?1 AND f.status='published' ORDER BY f.id DESC LIMIT 25",-1,&f,NULL)==SQLITE_OK){
   sqlite3_bind_text(f,1,c->key,-1,SQLITE_STATIC);
   while(sqlite3_step(f)==SQLITE_ROW&&fcount<25){fcount++;
    ui_start(stdout,"article");ui_attr(stdout,"class","cj-flag-report");ui_flag(stdout,"data-community-flag");ui_gt(stdout);
    ui_open(stdout,"h3",NULL);html((const char*)sqlite3_column_text(f,4));ui_close(stdout,"h3");
    ui_open(stdout,"p",NULL);ui_copy(stdout,"Reason: ");html((const char*)sqlite3_column_text(f,1));ui_lit(stdout," · Reported by ");cjw_name((const char*)sqlite3_column_text(f,2),c->owner,c->owner);ui_lit(stdout," · ");html((const char*)sqlite3_column_text(f,3));ui_close(stdout,"p");
    ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c->key,"thread",(const char*)sqlite3_column_text(f,0));ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open contribution");ui_close(stdout,"a");
    ui_close(stdout,"article");}
   sqlite3_finalize(f);}
  if(!fcount)ui_text(stdout, "p", NULL, "No flags waiting. Member reports will appear here with their reason.");}
}
static void cjw_profile(sqlite3 *db,long uid,const char *q,const char *cursor){
 char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);sqlite3_stmt *s=NULL;int count=0,more=0;long long last=0;
 ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your communities");ui_close(stdout,"h2");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Requests and participation are private to this profile view. Community conversations are public where you contribute; they are not automatically copied to your sites.");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"class","ph-button");ui_attr(stdout,"href","/communities");ui_gt(stdout);ui_copy(stdout,"Find communities");ui_close(stdout,"a");ui_copy(stdout," ");ui_start(stdout,"a");ui_attr(stdout,"class","ph-button");ui_attr(stdout,"href","/community/new");ui_gt(stdout);ui_copy(stdout,"Start a community");ui_close(stdout,"a");ui_close(stdout,"p");
 const char *sql="SELECT c.community,m.id FROM community_membership m JOIN community_body c ON c.community=m.community AND c.namespace='root' WHERE m.namespace='root' AND m.member_subject=?1 AND m.status='published' AND c.status='published' AND c.visibility='public' AND c.moderation_policy=?2 AND (?3=0 OR m.id<?3) AND (?4='' OR instr(lower(c.title),lower(?4))>0) ORDER BY m.id DESC LIMIT 25";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,actor,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,CJR_POLICY,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor&&*cursor?strtoll(cursor,NULL,10):0);sqlite3_bind_text(s,4,q?q:"",-1,SQLITE_STATIC);while(sqlite3_step(s)==SQLITE_ROW){if(count==24){more=1;break;}CjrCommunity c;CjrMember m;if(cjr_community(db,(const char*)sqlite3_column_text(s,0),&c)!=1||!cjr_public(&c)||cjr_member(db,c.key,actor,&m)!=1)continue;count++;last=sqlite3_column_int64(s,1);ui_start(stdout,"article");ui_attr(stdout,"class","ph-post");ui_flag(stdout,"data-private-community");ui_gt(stdout);ui_open(stdout,"h3",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c.key,"conversations",NULL);ui_attr_close(stdout);ui_gt(stdout);html(c.title);ui_close(stdout,"a");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);html(!strcmp(c.owner,actor)?"You steward this community.":m.active?"You can take part.":!strcmp(m.state,"requested")?"Your request is awaiting review.":!strcmp(m.state,"withdrawn")?"You left this community.":!strcmp(m.state,"declined")?"Your request was declined.":"Your participation is not active.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c.key,"conversations",NULL);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open conversations");ui_close(stdout,"a");ui_close(stdout,"article");}}sqlite3_finalize(s);if(!count){ui_open(stdout,"div","ph-empty");ui_open(stdout,"p",NULL);ui_copy(stdout,"Find a community to take part, or start one around a shared interest.");ui_close(stdout,"p");ui_close(stdout,"div");}if(more){ui_start(stdout,"a");ui_attr(stdout,"class","ph-next");ui_attr_open(stdout,"href");ui_copy(stdout,"/profile?tab=communities&amp;cursor=");fprintf(stdout,"%lld",last);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"More of your communities");ui_close(stdout,"a");}
}
/* /community with nothing chosen: the neighbourhood exchange. Every row is a
 * published, public community record or a public post in one; the page writes
 * nothing and each control opens the community owner's own forms. */
static long long cjx_count(sqlite3 *db,const char *sql){sqlite3_stmt *s=NULL;long long n=0;if(db&&sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK&&sqlite3_step(s)==SQLITE_ROW)n=sqlite3_column_int64(s,0);sqlite3_finalize(s);return n;}
static const char *cjx_col(sqlite3_stmt *s,int i){const char *t=(const char*)sqlite3_column_text(s,i);return t?t:"";}
static void cjx_excerpt(const char *text,size_t cap){size_t n=strlen(text);if(n<=cap){html(text);return;}char b[400];if(cap>=sizeof b)cap=sizeof b-1;memcpy(b,text,cap);b[cap]=0;char *sp=strrchr(b,' ');if(sp&&sp>b+cap/2)*sp=0;html(b);ui_copy(stdout,"…");}
static void cjw_exchange(sqlite3 *db,int signed_in){
 const char *pub="FROM community_body b WHERE b.namespace='root' AND b.status='published' AND b.visibility='public' "
                 "AND b.moderation_policy='" CJR_POLICY "' AND length(b.created_by_subject) BETWEEN 6 AND 23 "
                 "AND b.created_by_subject GLOB 'user:[1-9]*' AND substr(b.created_by_subject,6) NOT GLOB '*[^0-9]*'";char sql[1400];
 snprintf(sql,sizeof sql,"SELECT COUNT(*) %s",pub);long long bodies=cjx_count(db,sql);
 long long posts=cjx_count(db,"SELECT COUNT(*) " CJW_SEARCH_PUBLIC_FROM);
 snprintf(sql,sizeof sql,"SELECT COUNT(DISTINCT m.member_subject) FROM community_membership m WHERE m.namespace='root' AND m.status='published' AND m.state='active' AND m.community IN (SELECT b.community %s)",pub);long long members=cjx_count(db,sql);
 long long events=cjx_count(db,"SELECT COUNT(*) " CJW_SEARCH_PUBLIC_FROM " AND COALESCE(p.event_at,'')<>''");
 fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: ",stdout);
 fputs(signed_in?"private, no-store":"public, max-age=60, stale-while-revalidate=300",stdout);
 fputs("\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Bonfyre-Surface-Program: native-source-workspace\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",stdout);
 ak_app_open("Near you","feed","sx-page route-community");
 ui_open(stdout,"div","sx cx");ui_start(stdout,"nav");ui_attr(stdout,"class","sx-crumbs");ui_attr(stdout,"aria-label","Breadcrumb");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"href","/");ui_gt(stdout);ui_copy(stdout,"Aurekai");ui_close(stdout,"a");ui_open(stdout,"span",NULL);ui_copy(stdout,"/");ui_close(stdout,"span");ui_open(stdout,"span",NULL);ui_copy(stdout,"Community");ui_close(stdout,"span");ui_open(stdout,"em",NULL);ui_copy(stdout,"local circulation");ui_close(stdout,"em");ui_close(stdout,"nav");ui_open(stdout,"header","sx-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"p","sx-kicker");ui_copy(stdout,"The people around you");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Selling, lending and swapping ");ui_open(stdout,"em",NULL);ui_copy(stdout,"near you");ui_close(stdout,"em");ui_close(stdout,"h1");ui_open(stdout,"p",NULL);ui_copy(stdout,"Shops, studios and neighbours post what they make, lend and need inside their own communities. Every post below belongs to a community with a steward, so you always know who you are dealing with.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"aside","sx-panel cx-radius");ui_open(stdout,"p","sx-kicker");ui_copy(stdout,"In circulation");ui_close(stdout,"p");ui_open(stdout,"p","cx-big");printf("%lld",posts);
 ui_open(stdout,"small",NULL);ui_copy(stdout,posts==1?"public post":"public posts");ui_close(stdout,"small");ui_close(stdout,"p");ui_open(stdout,"p",NULL);printf("%lld %s · %lld active member%s · %lld dated gathering%s",bodies,bodies==1?"community":"communities",members,members==1?"":"s",events,events==1?"":"s");ui_close(stdout,"p");ui_close(stdout,"aside");ui_close(stdout,"header");
 /* Each community is a circle you can read; the chips open its room. */
 ui_start(stdout,"nav");ui_attr(stdout,"class","sx-chips");ui_attr(stdout,"aria-label","Communities");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"class","sx-chip");ui_attr(stdout,"href","/community");ui_attr(stdout,"aria-current","true");ui_gt(stdout);ui_copy(stdout,"Everyone ");ui_open(stdout,"span",NULL);printf("%lld",posts);ui_close(stdout,"span");ui_close(stdout,"a");
 {sqlite3_stmt *s=NULL;const char *top="SELECT c.community,c.title,COUNT(p.id) n " CJW_SEARCH_PUBLIC_FROM " GROUP BY c.community ORDER BY n DESC,c.title LIMIT 10";
  if(sqlite3_prepare_v2(db,top,-1,&s,NULL)==SQLITE_OK){while(sqlite3_step(s)==SQLITE_ROW){ui_start(stdout,"a");ui_attr(stdout,"class","sx-chip");ui_attr_open(stdout,"href");ui_copy(stdout,"/community?community=");url_query_value(cjx_col(s,0));ui_attr_close(stdout);ui_gt(stdout);html(cjx_col(s,1));ui_copy(stdout," ");ui_open(stdout,"span",NULL);printf("%d",sqlite3_column_int(s,2));ui_close(stdout,"span");ui_close(stdout,"a");}sqlite3_finalize(s);}}
 ui_close(stdout,"nav");
 /* The register: what people have put out, newest first. */
 ui_start(stdout,"section");ui_attr(stdout,"class","sx-catalogue");ui_attr(stdout,"aria-labelledby","cx-reg");ui_gt(stdout);ui_open(stdout,"header","sx-section-head");ui_open(stdout,"div",NULL);ui_open(stdout,"p","sx-kicker");ui_copy(stdout,"Open register");ui_close(stdout,"p");ui_start(stdout,"h2");ui_attr(stdout,"id","cx-reg");ui_gt(stdout);ui_copy(stdout,"What people are offering and asking for");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Newest first. Open a post to reply in its community.");ui_close(stdout,"p");ui_close(stdout,"div");ui_start(stdout,"a");ui_attr(stdout,"class","sx-btn");ui_attr(stdout,"href","/communities");ui_gt(stdout);ui_copy(stdout,"All communities");ui_close(stdout,"a");ui_close(stdout,"header");ui_open(stdout,"div","sx-grid cx-grid");
 {sqlite3_stmt *s=NULL;int k=0;
  if(sqlite3_prepare_v2(db,
     "SELECT p.post,p.community,p.title,p.body,COALESCE(p.event_at,''),COALESCE(p.venue,''),"
     "c.title,COALESCE(p.published_at,p.created_at),COALESCE(c.interest_label,'') "
     CJW_SEARCH_PUBLIC_FROM
     " ORDER BY p.published_at DESC,p.id DESC LIMIT 12",-1,&s,NULL)==SQLITE_OK){
   while(sqlite3_step(s)==SQLITE_ROW){k++;const char *ev=cjx_col(s,4),*venue=cjx_col(s,5);
    ui_open(stdout,"article","sx-card cx-lot");ui_open(stdout,"header",NULL);ui_open(stdout,"span","sx-host");html(cjx_col(s,6));ui_close(stdout,"span");ui_start(stdout,"span");ui_attr_open(stdout,"class");ui_copy(stdout,"sx-badge");if(*ev)ui_copy(stdout," is-on");ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,*ev?"Gathering":"Post");ui_close(stdout,"span");ui_close(stdout,"header");ui_open(stdout,"h3",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");
    cjw_href(cjx_col(s,1),"thread",cjx_col(s,0));ui_attr_close(stdout);ui_gt(stdout);html(cjx_col(s,2));ui_close(stdout,"a");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);cjx_excerpt(cjx_col(s,3),190);ui_close(stdout,"p");ui_open(stdout,"footer",NULL);ui_open(stdout,"span",NULL);
    if(*ev){html(ev);if(*venue){ui_copy(stdout," · ");html(venue);}}else if(*cjx_col(s,8))html(cjx_col(s,8));else{const char *at=cjx_col(s,7);fwrite(at,1,strlen(at)>10?10:strlen(at),stdout);}
    ui_close(stdout,"span");ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(cjx_col(s,1),"thread",cjx_col(s,0));ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open and reply");ui_close(stdout,"a");ui_close(stdout,"footer");ui_close(stdout,"article");}
   sqlite3_finalize(s);}
  if(!k)ui_text(stdout, "p", "sx-empty", "Nothing has been posted in a public community yet. Start one and put the first thing out.");}
 ui_close(stdout,"div");ui_close(stdout,"section");
 /* The circles themselves: the ways trade happens here. */
 ui_start(stdout,"section");ui_attr(stdout,"class","cx-modes");ui_attr(stdout,"aria-labelledby","cx-modes");ui_gt(stdout);ui_open(stdout,"div","cx-modes-copy");ui_open(stdout,"p","sx-kicker");ui_copy(stdout,"Circles");ui_close(stdout,"p");ui_start(stdout,"h2");ui_attr(stdout,"id","cx-modes");ui_gt(stdout);ui_copy(stdout,"Where neighbours trade");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Each circle is a community with its own steward and its own rules for who may post. Join the ones you live near or care about.");ui_close(stdout,"p");
 ui_open(stdout,"p","sx-panel cx-zero");ui_open(stdout,"b",NULL);ui_copy(stdout,"No commission.");ui_close(stdout,"b");ui_copy(stdout," Aurekai takes nothing from a sale, loan or swap arranged in a community.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"ol","cx-mode-list");
 {sqlite3_stmt *s=NULL;int k=0;snprintf(sql,sizeof sql,"SELECT b.community,b.title,b.description,COALESCE(NULLIF(b.interest_label,''),b.governance_scope,''),(SELECT COUNT(*) FROM community_membership m WHERE m.namespace='root' AND m.community=b.community AND m.status='published' AND m.state='active') %s ORDER BY b.title LIMIT 8",pub);
  if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){while(sqlite3_step(s)==SQLITE_ROW){k++;ui_open(stdout,"li",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/community?community=");url_query_value(cjx_col(s,0));ui_attr_close(stdout);ui_gt(stdout);ui_open(stdout,"span","cx-n");printf("%02d",k);ui_close(stdout,"span");ui_open(stdout,"small",NULL);html(cjx_col(s,3));ui_close(stdout,"small");ui_open(stdout,"b",NULL);html(cjx_col(s,1));ui_close(stdout,"b");ui_open(stdout,"span",NULL);cjx_excerpt(cjx_col(s,2),120);ui_close(stdout,"span");ui_open(stdout,"em",NULL);fprintf(stdout,"%d",sqlite3_column_int(s,4));ui_copy(stdout," active ");ui_copy(stdout,sqlite3_column_int(s,4)==1?"member":"members");ui_close(stdout,"em");ui_close(stdout,"a");ui_close(stdout,"li");}sqlite3_finalize(s);}
  if(!k)ui_text(stdout, "li", "sx-empty", "No public community yet.");}
 ui_close(stdout,"ol");ui_close(stdout,"section");
 ui_start(stdout,"section");ui_attr(stdout,"class","cx-trust");ui_attr(stdout,"aria-labelledby","cx-trust");ui_gt(stdout);ui_open(stdout,"div",NULL);ui_open(stdout,"p","sx-kicker");ui_copy(stdout,"Accountability");ui_close(stdout,"p");ui_start(stdout,"h2");ui_attr(stdout,"id","cx-trust");ui_gt(stdout);ui_copy(stdout,"Kept honest by people, not surveillance");ui_close(stdout,"h2");ui_open(stdout,"ul","sx-checks cx-checks");ui_open(stdout,"li",NULL);ui_open(stdout,"b",NULL);ui_copy(stdout,"Stewards admit members.");ui_close(stdout,"b");ui_copy(stdout," Only people a community has accepted can post in it.");ui_close(stdout,"li");ui_open(stdout,"li",NULL);ui_open(stdout,"b",NULL);ui_copy(stdout,"Posts stay where they were made.");ui_close(stdout,"b");ui_copy(stdout," Nothing here is copied to your profile or sold on.");ui_close(stdout,"li");ui_open(stdout,"li",NULL);ui_open(stdout,"b",NULL);ui_copy(stdout,"Disagreements go to the circle.");ui_close(stdout,"b");ui_copy(stdout," A community's steward settles disputes in its own conversation.");ui_close(stdout,"li");ui_close(stdout,"ul");ui_close(stdout,"div");
 ui_open(stdout,"dl","sx-stats cx-stats");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Communities");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%lld",bodies);ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Active members");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%lld",members);ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Public posts");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%lld",posts);ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Gatherings");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%lld",events);ui_close(stdout,"dd");ui_close(stdout,"div");ui_close(stdout,"dl");ui_close(stdout,"section");
 ui_open(stdout,"section","cx-cta");ui_open(stdout,"div",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Put something out, or ask for it");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Post inside a community you belong to, or start one for your street, studio or shop.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"nav","sx-actions");
 ui_start(stdout,"a");ui_attr(stdout,"class","sx-btn cx-dark");ui_attr(stdout,"href",signed_in?"/profile?tab=communities":"/login?next=%2Fcommunity");ui_gt(stdout);ui_copy(stdout,signed_in?"Post in your communities":"Sign in to post");ui_close(stdout,"a");
 ui_start(stdout,"a");ui_attr(stdout,"class","sx-btn");ui_attr(stdout,"href","/community/new");ui_gt(stdout);ui_copy(stdout,"Start a community");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"class","sx-btn");ui_attr(stdout,"href","/guilds");ui_gt(stdout);ui_copy(stdout,"Browse guilds");ui_close(stdout,"a");ui_close(stdout,"nav");ui_close(stdout,"section");ui_close(stdout,"div");
 ak_app_close();
}
static int serve_community_journey(const BfRequest *r) {
    const char *method=r->method; const char *path=r->target; const char *query=r->query; const char *body=r->body; const char *raw=r->raw; const char *host=r->host; const char *bearer=r->bearer; const char *auth_db=r->auth_db; const char *accept=r->accept;
    (void)method;(void)path;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
 char path_community[97]={0}, path_post[97]={0}; int permalink=0;
 if(bf_request_is(r,"permalink")){
  const char *first=path+11, *break_at=strstr(first,"/thread/");
  if(break_at&&break_at>first&&break_at[8]){
   size_t c=(size_t)(break_at-first),n=strlen(break_at+8);if(n&&break_at[8+n-1]=='/')n--;
   if(c<sizeof(path_community)&&n<sizeof(path_post)){
    memcpy(path_community,first,c); path_community[c]=0;
    memcpy(path_post,break_at+8,n);path_post[n]=0;
    permalink=cjr_key(path_community)&&cjr_key(path_post);
   }
  }
 }
 int guild_directory=bf_request_is(r,"guild_directory"),guild_create=bf_request_is(r,"guild_create"),directory=bf_request_is(r,"directory"),room=bf_request_is(r,"room"),creating=bf_request_is(r,"creating"),preview=bf_request_is(r,"preview"),action=bf_request_is(r,"action"),legacy=bf_request_is(r,"legacy");if(!guild_directory&&!guild_create&&!directory&&!room&&!creating&&!preview&&!action&&!legacy&&!permalink)return 0;
 if(!preview&&!action&&!legacy&&!wants_html(accept)&&!((creating||guild_create)&&!wants_json(accept)&&(!accept||!*accept||strstr(accept,"*/*"))))return 0;
 sqlite3 *db=cms_open_ro();if(!db){ph_error(503,"The community owner is unavailable.\n");return 1;}
 if(legacy){char key[200]="";CjrCommunity c;if(body&&query_value(body,"id",key,sizeof(key))&&cjr_community(db,key,&c)==1&&cjr_local(&c)){sqlite3_close(db);if(strcmp(method,"POST")){ph_error(405,"Use the community's participation form.\n");return 1;}cjw_back(key,"join",NULL);return 1;}sqlite3_close(db);return 0;}
 StudioViewer who={0};char actor[64]="";int signed_in=session_identity(bearer,auth_db,&who)&&who.id>0;if(signed_in)snprintf(actor,sizeof(actor),"user:%ld",who.id);
 if(preview||action){if(strcmp(method,"POST")){sqlite3_close(db);ph_error(405,"Use the reviewed community form.\n");return 1;}if(!signed_in){sqlite3_close(db);ph_error(401,"Sign in before changing a community.\n");return 1;}CjwForm p;if(!ph_origin(raw,host)||!cjw_parse(body,&p)){sqlite3_close(db);ph_error(403,"Submit one bounded form from this community page.\n");return 1;}if(preview){if(!cjw_reviewable(db,&p,actor)){sqlite3_close(db);ph_error(409,"The selected contribution or membership changed. Reopen it before reviewing.\n");return 1;}CjrCommunity c;const char *name="New public community";if(*p.community&&cjr_community(db,p.community,&c)==1)name=c.title;cjw_preview(&p,name);sqlite3_close(db);return 1;}sqlite3_close(db);return cjw_commit(&p,actor);}
 if(strcmp(method,"GET")){sqlite3_close(db);ph_error(405,"Open communities with GET.\n");return 1;}
 char key[97]="",tab[24]="conversations",postkey[97]="",q[161]="",cursor[24]="";PhField fields[]={{"community",key,sizeof(key),0},{"tab",tab,sizeof(tab),0},{"post",postkey,sizeof(postkey),0},{"q",q,sizeof(q),0},{"cursor",cursor,sizeof(cursor),0}};
 if(!ph_fields(query,fields,5,2048)||(*cursor&&!ph_digits(cursor))||(*key&&!cjr_key(key))||(*postkey&&!cjr_key(postkey))){sqlite3_close(db);ph_error(400,"Choose one exact community and conversation.\n");return 1;}errno=0;long long offset=*cursor?strtoll(cursor,NULL,10):0;if(errno||offset<0){sqlite3_close(db);ph_error(400,"The page cursor is invalid.\n");return 1;}
 if(permalink){
  if(*key||*postkey||*q||strcmp(tab,"conversations")){sqlite3_close(db);ph_error(400,"The discussion address is exact.\n");return 1;}
  snprintf(key,sizeof(key),"%s",path_community);
  snprintf(postkey,sizeof(postkey),"%s",path_post);
  snprintf(tab,sizeof(tab),"thread");
 }
 if(directory||guild_directory){if(*key||*postkey||strcmp(tab,"conversations")){sqlite3_close(db);ph_error(400,"Search the community directory by name or interest.\n");return 1;}cjw_directory(db,q,offset,guild_directory);sqlite3_close(db);return 1;}
 if(creating||guild_create){if(*key||*postkey||*q||*cursor||strcmp(tab,"conversations")){sqlite3_close(db);ph_error(400,"Open the new community form without another community selection.\n");return 1;}if(!signed_in){sqlite3_close(db);cjw_signin(guild_create?"/guild/new":"/community/new");return 1;}cjw_begin(guild_create?"Start a guild":"Start a community","","A shared public space with conversations and community-scoped participation.",200);cjw_editor(NULL,NULL,guild_create?"create-guild":"create");cjw_end();sqlite3_close(db);return 1;}
 if(!*key){if(room&&!*postkey&&!*q&&!*cursor&&!strcmp(tab,"conversations")){cjw_exchange(db,signed_in);sqlite3_close(db);return 1;}sqlite3_close(db);return 0;}CjrCommunity c;int found=cjr_community(db,key,&c);if(found==0||(found==1&&!cjr_local(&c))){sqlite3_close(db);return 0;}if(found!=1||!cjr_public(&c)){sqlite3_close(db);ph_error(404,"This community is not currently public.\n");return 1;}
 int guild=cjw_is_guild(db,key);const char *tabs[]={"conversations","about","members","settings","join","write","thread","reply","edit","programs"};int valid=0;for(int i=0;i<10;i++)if(!strcmp(tab,tabs[i]))valid=1;if((!guild&&!strcmp(tab,"programs"))||!valid||*q){sqlite3_close(db);ph_error(400,"Choose a section of this community.\n");return 1;}
 int restricted=!strcmp(tab,"members")||!strcmp(tab,"settings")||!strcmp(tab,"join")||!strcmp(tab,"write")||!strcmp(tab,"reply")||!strcmp(tab,"edit")||!strcmp(tab,"programs");if(restricted&&!signed_in){char next[512];snprintf(next,sizeof(next),"/community?community=%s&tab=%s%s%s",key,tab,*postkey?"&post=":"",postkey);sqlite3_close(db);cjw_signin(next);return 1;}
 CjrMember member;if(cjw_member_state(db,&c,actor,&member)<0){sqlite3_close(db);ph_error(503,"Current membership could not be read.\n");return 1;}int owner=*actor&&!strcmp(c.owner,actor),can_post=owner||member.active;
 if((!strcmp(tab,"members")||!strcmp(tab,"settings"))&&!owner){sqlite3_close(db);ph_error(403,"Only this community's steward can open this section.\n");return 1;}if((!strcmp(tab,"write")||!strcmp(tab,"reply")||!strcmp(tab,"edit")||!strcmp(tab,"programs"))&&!can_post){sqlite3_close(db);ph_error(403,"Your community membership must be active to contribute.\n");return 1;}
 CjrPost selected;memset(&selected,0,sizeof(selected));int needs_post=!strcmp(tab,"thread")||!strcmp(tab,"reply")||!strcmp(tab,"edit");char rootkey[97]="";if(needs_post&&(cjr_post(db,key,postkey,&selected)!=1||strcmp(selected.visibility,"public")||!cjr_thread(db,key,&selected,rootkey,0))){sqlite3_close(db);ph_error(404,"This conversation is not available in this community.\n");return 1;}
 if(needs_post&&cjr_visible(&selected)&&!cjw_search_current_date(db,&selected)&&
    !owner&&strcmp(actor,selected.author)){
  sqlite3_close(db);ph_error(404,"This contribution is not published yet.\n");return 1;
 }
 if(!strcmp(tab,"edit")&&(!cjr_visible(&selected)||strcmp(selected.author,actor))){sqlite3_close(db);ph_error(403,"You can edit only your own visible contribution.\n");return 1;}if(!strcmp(tab,"reply")&&(!cjr_visible(&selected)||cjr_thread(db,key,&selected,rootkey,1)>=9||!cjr_thread(db,key,&selected,rootkey,1))){sqlite3_close(db);ph_error(409,"This conversation no longer accepts this reply.\n");return 1;}
 int indexable=!strcmp(tab,"thread")&&cjr_visible(&selected)&&cjw_search_current_date(db,&selected)&&cjr_thread(db,key,&selected,rootkey,1)>0&&cjw_search_public_root(db,key,rootkey);
 if(permalink&&!indexable){sqlite3_close(db);ph_error(404,"This discussion is no longer public.\n");return 1;}
 CjwSearchThread search={db,&c,&selected,rootkey};
 if(indexable)cjw_begin_search(selected.title[0]?selected.title:c.title,c.key,c.description,200,&search,!signed_in);
 else cjw_begin(c.title,c.key,c.description,200);
 cjw_member_actions(&c,actor,&member);cjw_tabs(&c,tab,owner,guild,can_post);
 if(!strcmp(tab,"programs"))cjw_programmes(db,&c,owner);else if(!strcmp(tab,"members"))cjw_members(db,&c,offset);
 else if(!strcmp(tab,"settings"))cjw_editor(&c,NULL,"edit-community");
 else if(!strcmp(tab,"write"))cjw_editor(&c,NULL,"post");
 else if(!strcmp(tab,"reply")||!strcmp(tab,"edit"))cjw_editor(&c,&selected,tab);
 else if(!strcmp(tab,"join")){ui_open(stdout,"h2",NULL);ui_copy(stdout,"Take part in this community");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your request goes to this community's steward. It does not join an external network, sign you up for marketing, or publish anything on your profile sites.");ui_close(stdout,"p");if(!owner&&(!member.id||!strcmp(member.state,"withdrawn")))cjw_action("request",c.key,"",member.digest,"Send my join request");}
 else if(!strcmp(tab,"about")){ui_open(stdout,"h2",NULL);ui_copy(stdout,"Community purpose");ui_close(stdout,"h2");ui_open(stdout,"p","cj-review");html(c.description);ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Conversations are public. Participation requires a local request accepted by this community's steward. Authors can edit or remove their own words, and the steward can hide a contribution.");ui_close(stdout,"p");}
 else{ui_open(stdout,"div","cj-content");ui_open(stdout,"section",NULL);if(needs_post){if(*selected.parent){ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c.key,"thread",selected.parent);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Back to the parent contribution");ui_close(stdout,"a");ui_close(stdout,"p");if(strcmp(rootkey,selected.parent)){ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(c.key,"thread",rootkey);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Conversation start");ui_close(stdout,"a");ui_close(stdout,"p");}}cjw_card(db,&c,&selected,actor,1,can_post);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Replies");ui_close(stdout,"h2");ui_open(stdout,"div","cj-replies");cjw_posts(db,&c,selected.key,actor,can_post,offset);ui_close(stdout,"div");}else cjw_posts(db,&c,"",actor,can_post,offset);ui_close(stdout,"section");ui_open(stdout,"aside",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Your place here");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);html(owner?"You steward this community. Manage only its requests and conversations.":can_post?"You can start a conversation or reply. Your contributions remain yours to edit.":"Read freely. Request to join when you want to contribute.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/profile?tab=communities");ui_gt(stdout);ui_copy(stdout,"Return through your profile");ui_close(stdout,"a");ui_close(stdout,"aside");ui_close(stdout,"div");}
 cjw_end();sqlite3_close(db);return 1;
}
#endif
