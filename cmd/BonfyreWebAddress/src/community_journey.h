#ifndef BONFYRE_COMMUNITY_JOURNEY_WEB_H
#define BONFYRE_COMMUNITY_JOURNEY_WEB_H
#include "../../BonfyreCMS/src/community_read.h"
/* Community UI over existing records; account identity comes only from Auth. */
typedef struct {char operation[24],community[97],target[97],expected[65],request[33],title[161],text[6001];} CjwForm;
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
 memcpy(excerpt,text,n);excerpt[n]=0;html(excerpt);if(n<size)fputs("…",stdout);
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
    snprintf(url,sizeof(url),"https://aurekai.life/community/%s/thread/%s/",
             c->community->key,root);
    snprintf(author,sizeof(author),"Member %s",
             cjr_actor(post->author)?post->author+5:"unknown");
    fputs("<link rel=\"canonical\" href=\"",stdout);html(url);
    fputs("\"><meta name=\"description\" content=\"",stdout);html(desc);
    fputs("\"><meta property=\"og:type\" content=\"article\">"
          "<meta property=\"og:site_name\" content=\"Aurekai Life\">"
          "<meta property=\"og:url\" content=\"",stdout);html(url);
    fputs("\"><meta property=\"og:title\" content=\"",stdout);
    html(post->title[0]?post->title:c->community->title);
    fputs("\"><meta property=\"og:description\" content=\"",stdout);
    html(desc);fputs("\"><meta name=\"twitter:card\" content=\"summary\">",stdout);
    /* Replies have the original discussion as their canonical; only a
     * visible root publishes DiscussionForumPosting with real human copy. */
    if (*post->parent || !cjw_schema_date(post->at) || !cjr_actor(post->author)) return;
    fputs("<script type=\"application/ld+json\">"
          "{\"@context\":\"https://schema.org\",\"@type\":\"DiscussionForumPosting\","
          "\"mainEntityOfPage\":",stdout);cjw_schema_text(url);
    fputs(",\"url\":",stdout);cjw_schema_text(url);
    fputs(",\"headline\":",stdout);
    cjw_schema_text(post->title[0]?post->title:c->community->title);
    fputs(",\"text\":",stdout);cjw_schema_text(post->body);
    fputs(",\"author\":{\"@type\":\"Person\",\"name\":",stdout);
    cjw_schema_text(author);fputs("},\"datePublished\":",stdout);
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
            if (!shown++)fputs(",\"comment\":[",stdout);
            else fputc(',',stdout);
            snprintf(reply_author,sizeof(reply_author),"Member %s",reply.author+5);
            fputs("{\"@type\":\"Comment\",\"text\":",stdout);
            cjw_schema_text(reply.body);
            fputs(",\"datePublished\":",stdout);cjw_schema_text(reply.at);
            fputs(",\"author\":{\"@type\":\"Person\",\"name\":",stdout);
            cjw_schema_text(reply_author);fputs("}}",stdout);
        }
        if(shown)fputc(']',stdout);
    }
    sqlite3_finalize(st);
    fputs("}</script>",stdout);
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
 fputs("<div class=\"cj-shell\" data-community-journey data-community=\"",stdout);html(key?key:"");fputs("\"><nav class=\"cj-breadcrumbs\" aria-label=\"Breadcrumb\"><a href=\"/communities\">Communities</a><a href=\"/guilds\">Guilds</a><a href=\"/profile?tab=communities\">My communities</a></nav><header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">People and shared interests</span><h1>",stdout);html(title);fputs("</h1><p class=\"ak-lede\">",stdout);html(description?description:"");fputs("</p></div></header>",stdout);
}
static void cjw_begin(const char *title,const char *key,const char *description,int code){
 cjw_begin_search(title,key,description,code,NULL,0);
}
static void cjw_end(void){fputs("</div>",stdout);ak_app_close();}
static void cjw_form_fields(const CjwForm *p){ph_hidden("operation",p->operation);ph_hidden("community",p->community);ph_hidden("target",p->target);ph_hidden("expected",p->expected);ph_hidden("request",p->request);ph_hidden("title",p->title);ph_hidden("text",p->text);}
static void cjw_action(const char *op,const char *key,const char *target,const char *expected,const char *label){
 fputs("<form method=\"post\" action=\"/community/action\">",stdout);ph_hidden("operation",op);ph_hidden("community",key);ph_hidden("target",target?target:"");ph_hidden("expected",expected?expected:"");fputs("<button type=\"submit\" data-community-action=\"",stdout);html(op);fputs("\">",stdout);html(label);fputs("</button></form>",stdout);
}
static void cjw_name(const char *actor,const char *viewer,const char *owner){if(viewer&&*viewer&&!strcmp(actor,viewer))html("You");else if(owner&&*owner&&!strcmp(actor,owner))html("Community steward");else{fputs("Member ",stdout);html(cjr_actor(actor)?actor+5:"unknown");}}
static int cjw_member_state(sqlite3 *db,const CjrCommunity *c,const char *actor,CjrMember *m){if(!actor||!*actor){memset(m,0,sizeof(*m));snprintf(m->digest,sizeof(m->digest),"absent");return 0;}return cjr_member(db,c->key,actor,m);}
static void cjw_member_actions(const CjrCommunity *c,const char *actor,const CjrMember *m){
 fputs("<div class=\"cj-actions\">",stdout);
 if(!*actor){fputs("<a class=\"cj-button\" href=\"",stdout);cjw_href(c->key,"join",NULL);fputs("\">Sign in to take part</a>",stdout);}
 else if(!strcmp(c->owner,actor)||m->active){fputs("<a class=\"cj-button\" href=\"",stdout);cjw_href(c->key,"write",NULL);fputs("\">Start a conversation</a>",stdout);}
 else if(!m->id||!strcmp(m->state,"withdrawn")){fputs("<a class=\"cj-button\" href=\"",stdout);cjw_href(c->key,"join",NULL);fputs("\">Request to join</a>",stdout);}
 else{fputs("<p data-membership-state=\"",stdout);html(m->state);fputs("\">",stdout);html(!strcmp(m->state,"requested")?"Your request is awaiting the steward.":!strcmp(m->state,"declined")?"Your request was declined.":!strcmp(m->state,"revoked")?"Your participation has ended.":"Your membership is not currently active.");fputs("</p>",stdout);}
 fputs("</div>",stdout);
 if(*actor&&strcmp(c->owner,actor)&&m->id&&(!strcmp(m->state,"requested")||!strcmp(m->state,"active"))){fputs("<details><summary>",stdout);html(!strcmp(m->state,"requested")?"Withdraw my request":"Leave this community");fputs("</summary><p>Your contributions keep their own history. This does not change any other community.</p>",stdout);cjw_action("leave",c->key,"",m->digest,!strcmp(m->state,"requested")?"Withdraw request":"Confirm leaving");fputs("</details>",stdout);}
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
 const char *keys[]={"conversations","about","members","settings"},*names[]={"Conversations","About","People and requests","Edit community"};fputs("<nav class=\"cj-tabs\" aria-label=\"Community sections\">",stdout);for(int i=0;i<4;i++){if(i>=2&&!owner)continue;fputs("<a href=\"",stdout);cjw_href(c->key,keys[i],NULL);fputs("\"",stdout);if(!strcmp(tab,keys[i])||(!i&&!strcmp(tab,"thread")))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(names[i]);fputs("</a>",stdout);}if(guild&&participant){fputs("<a href=\"",stdout);cjw_href(c->key,"programs",NULL);fputs("\"",stdout);if(!strcmp(tab,"programs"))fputs(" aria-current=\"page\"",stdout);fputs(">Programmes and work</a>",stdout);}fputs("</nav>",stdout);
}
static int cjw_parse(const char *body,CjwForm *p){
 memset(p,0,sizeof(*p));PhField f[]={{"operation",p->operation,sizeof(p->operation),0},{"community",p->community,sizeof(p->community),0},{"target",p->target,sizeof(p->target),0},{"expected",p->expected,sizeof(p->expected),0},{"request",p->request,sizeof(p->request),0},{"title",p->title,sizeof(p->title),0},{"text",p->text,sizeof(p->text),1}};return ph_fields(body,f,7,40000)&&*p->operation;
}
static void cjw_editor(const CjrCommunity *c,const CjrPost *post,const char *operation){
 char nonce[33];ph_nonce(nonce);int guild=!strcmp(operation,"create-guild"),creating=!strcmp(operation,"create")||guild,replying=!strcmp(operation,"reply"),editing=!strcmp(operation,"edit"),settings=!strcmp(operation,"edit-community");
 fputs("<h2>",stdout);html(guild?"Start a public guild":creating?"Start a public community":replying?"Reply in this conversation":editing?"Edit your contribution":settings?"Edit this community":"Start a conversation");fputs("</h2><p class=\"cj-notice\">",stdout);html(guild?"Guilds offer steward-reviewed local participation and conversation. No apprenticeship, credential, equipment use or payment is granted.":creating?"Give people a clear purpose and describe how you want to take part together. You will review the public page before creating it.":"This contribution will be public in this community. It will not publish on your profile sites or on an external platform.");fputs("</p>",stdout);
 if(replying&&post){fputs("<blockquote class=\"cj-review\">",stdout);html(post->body);fputs("</blockquote>",stdout);}
 fputs("<form class=\"cj-form\" action=\"/community/preview\" method=\"post\">",stdout);ph_hidden("operation",operation);ph_hidden("community",c?c->key:"");ph_hidden("target",post?post->key:"");ph_hidden("expected",post?post->digest:settings&&c?c->digest:"");ph_hidden("request",nonce);
 if(!replying){fputs("<label>",stdout);html(guild?"Guild name":creating||settings?"Community name":"Conversation title");fputs("<input name=\"title\" required maxlength=\"160\" value=\"",stdout);html(editing&&post?post->title:settings&&c?c->title:"");fputs("\"></label>",stdout);}else ph_hidden("title","");
 fputs("<label>",stdout);html(creating||settings?"Purpose and community guidelines":"Your contribution");fputs("<textarea name=\"text\" rows=\"9\" required maxlength=\"6000\">",stdout);html(editing&&post?post->body:settings&&c?c->description:"");fputs("</textarea></label><button type=\"submit\">Review before publishing</button></form>",stdout);
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
 cjw_begin("Review your public contribution",p->community,name,200);fputs("<p class=\"cj-notice\">This is public community content. Your private saved material and independently published profile sites stay unchanged.</p><article class=\"cj-review\"><h2>",stdout);html(p->title);fputs("</h2><p>",stdout);html(p->text);fputs("</p></article><form method=\"post\" action=\"/community/action\">",stdout);cjw_form_fields(p);fputs("<button type=\"submit\" data-community-publish>",stdout);html(!strcmp(p->operation,"create-guild")?"Create this guild":!strcmp(p->operation,"create")?"Create this community":"Publish this contribution");fputs("</button></form><p>You can return to the previous form to revise your words.</p>",stdout);cjw_end();
}
static int cjw_native_string(bf_mp_reader_t *r,char *out,size_t cap){bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR||v.v.raw.len>=cap||memchr(v.v.raw.ptr,0,v.v.raw.len))return 0;memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;return 1;}
static int cjw_commit(const CjwForm *p,const char *actor){
 const char *bin=getenv("BONFYRE_CMS_BINARY"),*path=getenv("BONFYRE_CMS_DB"),*schemas=getenv("BONFYRE_CMS_SCHEMAS");if(!bin||!path||!schemas){ph_error(503,"The community owner is unavailable.\n");return 1;}
 bf_mp_writer_t w;bf_mp_writer_init(&w,1024);int ok=!bf_mp_pack_array(&w,7)&&!bf_mp_pack_uint(&w,1);const char *parts[]={p->community,p->target,p->expected,p->request,p->title,p->text};for(int i=0;i<6&&ok;i++)ok=!bf_mp_pack_str_cstr(&w,parts[i]);if(!ok||w.len>16384){bf_mp_writer_free(&w);ph_error(413,"This contribution exceeds its supported size.\n");return 1;}
 char *args[]={(char*)bin,"entry","community-work","--actor",(char*)actor,"--operation",(char*)p->operation,"--db",(char*)path,"--schemas",(char*)schemas,NULL};unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(args,w.buf,w.len,&output,&size,1);bf_mp_writer_free(&w);
 char state[32]="",community[97]="",target[97]="",digest[65]="",message[512]="",delta[32]="";bf_mp_reader_t r;bf_mp_value_t v;bf_mp_reader_init(&r,output,size);ok=output&&size<=4096&&bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count==7;
 if(ok){bf_mp_type_t t=bf_mp_read_next(&r,&v);ok=(t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1);}char *values[]={state,community,target,digest,message,delta};size_t caps[]={sizeof(state),sizeof(community),sizeof(target),sizeof(digest),sizeof(message),sizeof(delta)};for(int i=0;i<6&&ok;i++)ok=cjw_native_string(&r,values[i],caps[i]);ok=ok&&r.pos==r.len;free(output);
 if(ok&&!rc&&(!strcmp(state,"committed")||!strcmp(state,"unchanged"))&&cjr_key(community)){
  if(!strcmp(p->operation,"post")||!strcmp(p->operation,"reply")||!strcmp(p->operation,"edit")||!strcmp(p->operation,"remove")||!strcmp(p->operation,"hide")){cjw_back(community,"thread",target);return 1;}
  cjw_back(community,(!strcmp(p->operation,"accept")||!strcmp(p->operation,"decline")||!strcmp(p->operation,"revoke"))?"members":"conversations",NULL);return 1;
 }
 int code=!ok?503:!strcmp(state,"forbidden")?403:!strcmp(state,"not_found")?404:!strcmp(state,"conflict")?409:!strcmp(state,"invalid")?400:503;
 cjw_begin(ok?"This change needs your attention":"Check the current community",p->community,"",code);fputs("<p class=\"cj-notice\" role=\"alert\">",stdout);html(ok?message:"The reply was interrupted. Read the current community before resending the same reviewed request. No retry was sent automatically.");fputs("</p>",stdout);
 if(cjr_key(p->community)){fputs("<p><a href=\"",stdout);cjw_href(p->community,"conversations",NULL);fputs("\">Open the current community</a></p>",stdout);}else fputs("<p><a href=\"/profile?tab=communities\">Check your communities</a></p>",stdout);
 if(!ok){fputs("<details><summary>Keep this exact request</summary><p>First check whether the change is already visible. This form retains its original identity and will not overwrite newer work.</p><form method=\"post\" action=\"/community/action\">",stdout);cjw_form_fields(p);fputs("<button type=\"submit\">Retry this exact reviewed request</button></form></details>",stdout);}else if(*p->text){fputs("<details open><summary>Your submitted words</summary><pre class=\"cj-review\">",stdout);html(p->text);fputs("</pre></details>",stdout);}cjw_end();return 1;
}
static void cjw_card(sqlite3 *db,const CjrCommunity *c,const CjrPost *p,const char *actor,int full,int can_post){
 fputs("<article class=\"cj-thread\" data-community-post=\"",stdout);html(p->key);fputs("\"><div class=\"cj-meta\">",stdout);cjw_name(p->author,actor,c->owner);fputs(" · <time>",stdout);html(p->at);fputs("</time></div>",stdout);
 if(cjr_visible(p)&&cjw_search_current_date(db,p)){if(*p->title){fputs("<h2><a href=\"",stdout);cjw_href(c->key,"thread",p->key);fputs("\">",stdout);html(p->title);fputs("</a></h2>",stdout);}fputs("<p>",stdout);if(full)html(p->body);else cjw_excerpt(p->body);fputs("</p>",stdout);}else fputs("<p class=\"cj-removed\">This contribution is no longer public.</p>",stdout);
 fputs("<nav aria-label=\"Conversation actions\"><a href=\"",stdout);cjw_href(c->key,"thread",p->key);fputs("\">Open conversation</a>",stdout);char root[97];int depth=cjr_thread(db,c->key,p,root,1);
 if(can_post&&depth>0&&depth<9){fputs("<a href=\"",stdout);cjw_href(c->key,"reply",p->key);fputs("\">Reply</a>",stdout);}if(*actor&&!strcmp(p->author,actor)&&cjr_visible(p)&&can_post){fputs("<a href=\"",stdout);cjw_href(c->key,"edit",p->key);fputs("\">Edit my contribution</a>",stdout);}fputs("</nav>",stdout);
 if(*actor&&cjr_visible(p)&&(!strcmp(p->author,actor)||!strcmp(c->owner,actor))){int own=!strcmp(p->author,actor);fputs("<details><summary>",stdout);html(own?"Remove my contribution":"Moderate this contribution");fputs("</summary><p>Its text will stop being public. Other people's replies remain their own contributions.</p>",stdout);cjw_action(own?"remove":"hide",c->key,p->key,p->digest,own?"Confirm removal":"Hide this contribution");fputs("</details>",stdout);}fputs("</article>",stdout);
}
static int cjw_posts(sqlite3 *db,const CjrCommunity *c,const char *parent,const char *actor,int can_post,long long cursor){
 sqlite3_stmt *s=NULL;const char *sql="SELECT post,id FROM community_post WHERE namespace='root' AND community=?1 AND COALESCE(reply_to,'')=?2 AND visibility='public' AND status IN ('published','archived') AND moderation_state IN ('accepted','visible','published','withdrawn','hidden') AND (status='archived' OR (julianday(published_at) IS NOT NULL AND julianday(published_at)<=julianday('now'))) AND (?3=0 OR id<?3) ORDER BY id DESC LIMIT 25";if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,c->key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,parent?parent:"",-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor);int count=0,more=0,step;long long last=0;
 while((step=sqlite3_step(s))==SQLITE_ROW){if(count==24){more=1;break;}CjrPost p;const char *key=(const char*)sqlite3_column_text(s,0);if(cjr_post(db,c->key,key,&p)!=1)continue;count++;last=sqlite3_column_int64(s,1);cjw_card(db,c,&p,actor,parent&&*parent,can_post);}sqlite3_finalize(s);
 if(!count)fputs("<div class=\"cj-notice\">No conversations in this view yet. An active member can start one.</div>",stdout);
 if(more){fputs("<a class=\"cj-next\" href=\"",stdout);cjw_href(c->key,parent&&*parent?"thread":"conversations",parent);printf("%scursor=%lld\">Earlier contributions</a>",parent&&*parent?"?":"&amp;",last);}return step==SQLITE_DONE||step==SQLITE_ROW;
}
/* One count over the community owner's own tables; -1 when unreadable. */
static long long cjw_count(sqlite3 *db,const char *sql,const char *key,int guilds){
 sqlite3_stmt *s=NULL;long long n=-1;if(!db||sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,CJR_POLICY,-1,SQLITE_STATIC);if(sqlite3_bind_parameter_count(s)>=2)sqlite3_bind_text(s,2,key?key:"",-1,SQLITE_STATIC);if(sqlite3_bind_parameter_count(s)>=3)sqlite3_bind_int(s,3,guilds);
 if(sqlite3_step(s)==SQLITE_ROW)n=sqlite3_column_int64(s,0);sqlite3_finalize(s);return n;
}
static void cjw_stat(const char *label,long long n,const char *note){fputs("<div><dt>",stdout);html(label);fputs("</dt><dd",stdout);if(n>0)fputs(" class=\"is-on\"",stdout);printf(">%lld</dd><small>",n<0?0:n);html(note);fputs("</small></div>",stdout);}
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
 fputs("<div class=\"ak-wb-strip\"><span>",stdout);fputs(guilds?"Guild registry":"Community registry",stdout);printf("</span><span><b>%lld</b> public</span><span><b>%lld</b> active members</span><span class=\"ak-live\">Read from the community records</span></div>",bodies<0?0:bodies,members<0?0:members);
 fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">",stdout);fputs(guilds?"Local guilds &middot; steward-reviewed":"Communities &middot; people and shared interests",stdout);fputs("</span><h1>",stdout);
 fputs(guilds?"Guilds: crafts kept by the people who practise them":"Find your people",stdout);fputs("</h1><p class=\"ak-lede\">",stdout);
 fputs(guilds?"Each guild is a public body with a steward who reviews who takes part. Read their conversations, ask to join, or start one around a craft you keep.":"Local communities with public conversations. Read along, ask to join, or start a shared space around something you care about.",stdout);
 fputs("</p><div class=\"ak-actions\"><a class=\"ak-btn is-primary\" href=\"",stdout);fputs(guilds?"/guild/new\">Start a guild":"/community/new\">Start a community",stdout);fputs("</a><a class=\"ak-btn is-quiet\" href=\"/profile?tab=communities\">My communities</a></div></div></header>",stdout);
 fputs("<dl class=\"ak-stats is-row\">",stdout);cjw_stat(guilds?"Public guilds":"Public communities",bodies,"published and open to read");cjw_stat("Active members",members,"accepted by a steward");cjw_stat("Conversations",posts,"public contributions");cjw_stat("Interests",crafts,"named by their stewards");fputs("</dl>",stdout);
 printf("<nav class=\"ak-chips\" aria-label=\"Registry\"><a class=\"ak-chip\" href=\"/communities\"%s>All communities</a><a class=\"ak-chip\" href=\"/guilds\"%s>Guilds</a>",guilds?"":" aria-current=\"page\"",guilds?" aria-current=\"page\"":"");
 {sqlite3_stmt *s=NULL;snprintf(sql,sizeof(sql),"SELECT b.interest_label,COUNT(*)%s AND COALESCE(b.interest_label,'')<>'' GROUP BY b.interest_label ORDER BY COUNT(*) DESC LIMIT 8",scope);
  if(db&&sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,CJR_POLICY,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,"",-1,SQLITE_STATIC);sqlite3_bind_int(s,3,guilds);while(sqlite3_step(s)==SQLITE_ROW){const char *l=(const char*)sqlite3_column_text(s,0);printf("<a class=\"ak-chip\" href=\"%s?q=",route);url_query_value(l);fputs("\"",stdout);if(q&&*q&&!strcmp(q,l))fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(l);printf(" <b>%d</b></a>",sqlite3_column_int(s,1));}}sqlite3_finalize(s);}
 fputs("</nav>",stdout);
 printf("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Registry</span><h2>%s</h2></div><form class=\"cj-search\" method=\"get\" action=\"%s\" role=\"search\"><input type=\"search\" name=\"q\" aria-label=\"Find by name or interest\" placeholder=\"Find by name or interest\" value=\"",guilds?"Chambers of craft":"Communities",route);html(q);fputs("\"><button type=\"submit\">Find</button></form></header>",stdout);
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
   if(strcmp(group,label)){if(count)fputs("</div>",stdout);snprintf(group,sizeof(group),"%s",label);fputs("<h3 class=\"cj-chamber\">",stdout);html(*label?label:guilds?"Other crafts":"Other interests");fputs("</h3><div class=\"ak-grid is-3\">",stdout);}
   count++;last=sqlite3_column_int64(s,1);
   fputs("<article class=\"ak-panel cj-listing\" data-community-listing><header><span class=\"ak-tag",stdout);if(gov&&!strcmp(gov,"local-guild"))fputs(" is-on\">Guild",stdout);else fputs("\">Community",stdout);fputs("</span></header><h3><a href=\"",stdout);cjw_href(c.key,"conversations",NULL);fputs("\">",stdout);html(c.title);fputs("</a></h3><p>",stdout);cjw_excerpt(c.description);
   printf("</p><dl class=\"ak-spec\"><dt>Active members</dt><dd>%d</dd><dt>Conversations</dt><dd>%d</dd></dl><div class=\"ak-actions\"><a class=\"ak-btn\" href=\"",sqlite3_column_int(s,4),sqlite3_column_int(s,5));cjw_href(c.key,"conversations",NULL);fputs("\">Read</a><a class=\"ak-btn is-quiet\" href=\"",stdout);cjw_href(c.key,"join",NULL);fputs("\">Ask to join</a></div></article>",stdout);}
  if(count)fputs("</div>",stdout);}
 sqlite3_finalize(s);
 if(!count)fputs(guilds?"<p class=\"ak-empty\">No public guild is registered yet. A guild appears here once its steward publishes it.</p>":"<p class=\"ak-empty\">No public community matches this view. Try another interest or start one.</p>",stdout);
 if(more){printf("<p class=\"ak-actions\"><a class=\"ak-btn\" href=\"%s?cursor=%lld&amp;q=",route,last);url_query_value(q);fputs("\">More</a></p>",stdout);}
 fputs("</section><section class=\"ak-band\"><div><span class=\"ak-kicker\">",stdout);fputs(guilds?"Start a guild":"Start a community",stdout);fputs("</span><h2>",stdout);
 fputs(guilds?"Keep a craft? Open your bench to others.":"Something worth gathering around?",stdout);fputs("</h2><p>",stdout);
 fputs(guilds?"A guild is public, with a steward who accepts members. Nothing here grants credentials, equipment use or payment.":"A community is public, with a steward who accepts members and can hide a contribution.",stdout);
 fputs("</p></div><div class=\"ak-actions\"><a class=\"ak-btn\" href=\"",stdout);fputs(guilds?"/guild/new\">Start a guild":"/community/new\">Start a community",stdout);fputs("</a></div></section>",stdout);
 ak_app_close();
}
static void cjw_members(sqlite3 *db,const CjrCommunity *c,long long cursor){
 fputs("<h2>People and requests</h2><p>These decisions apply only to this community. They do not grant access to your account, other communities, integrations or money.</p>",stdout);sqlite3_stmt *s=NULL;int count=0,more=0;long long last=0;
 if(sqlite3_prepare_v2(db,"SELECT member_subject,id FROM community_membership WHERE namespace='root' AND community=?1 AND status='published' AND member_subject<>?2 AND (?3=0 OR id<?3) ORDER BY id DESC LIMIT 25",-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,c->key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,c->owner,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor);while(sqlite3_step(s)==SQLITE_ROW){if(count==24){more=1;break;}CjrMember m;if(cjr_member(db,c->key,(const char*)sqlite3_column_text(s,0),&m)!=1)continue;count++;last=sqlite3_column_int64(s,1);fputs("<article class=\"cj-member\" data-community-member><h3>",stdout);cjw_name(m.actor,c->owner,c->owner);fputs("</h3><p>",stdout);html(m.state);if(!m.active&&!strcmp(m.state,"active"))fputs(" · access expired",stdout);fputs("</p>",stdout);if(!strcmp(m.state,"requested")){cjw_action("accept",c->key,m.actor,m.digest,"Accept request");cjw_action("decline",c->key,m.actor,m.digest,"Decline request");}else if(m.active){fputs("<details><summary>End this participation</summary>",stdout);cjw_action("revoke",c->key,m.actor,m.digest,"Confirm removal from community");fputs("</details>",stdout);}fputs("</article>",stdout);}}sqlite3_finalize(s);if(!count)fputs("<p>No requests or other memberships in this view.</p>",stdout);if(more){fputs("<a class=\"cj-next\" href=\"",stdout);cjw_href(c->key,"members",NULL);printf("&amp;cursor=%lld\">Earlier requests</a>",last);}
}
static void cjw_profile(sqlite3 *db,long uid,const char *q,const char *cursor){
 char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);sqlite3_stmt *s=NULL;int count=0,more=0;long long last=0;
 fputs("<h2>Your communities</h2><p class=\"ph-privacy\">Requests and participation are private to this profile view. Community conversations are public where you contribute; they are not automatically copied to your sites.</p><p><a class=\"ph-button\" href=\"/communities\">Find communities</a> <a class=\"ph-button\" href=\"/community/new\">Start a community</a></p>",stdout);
 const char *sql="SELECT c.community,m.id FROM community_membership m JOIN community_body c ON c.community=m.community AND c.namespace='root' WHERE m.namespace='root' AND m.member_subject=?1 AND m.status='published' AND c.status='published' AND c.visibility='public' AND c.moderation_policy=?2 AND (?3=0 OR m.id<?3) AND (?4='' OR instr(lower(c.title),lower(?4))>0) ORDER BY m.id DESC LIMIT 25";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,actor,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,CJR_POLICY,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,cursor&&*cursor?strtoll(cursor,NULL,10):0);sqlite3_bind_text(s,4,q?q:"",-1,SQLITE_STATIC);while(sqlite3_step(s)==SQLITE_ROW){if(count==24){more=1;break;}CjrCommunity c;CjrMember m;if(cjr_community(db,(const char*)sqlite3_column_text(s,0),&c)!=1||!cjr_public(&c)||cjr_member(db,c.key,actor,&m)!=1)continue;count++;last=sqlite3_column_int64(s,1);fputs("<article class=\"ph-post\" data-private-community><h3><a href=\"",stdout);cjw_href(c.key,"conversations",NULL);fputs("\">",stdout);html(c.title);fputs("</a></h3><p>",stdout);html(!strcmp(c.owner,actor)?"You steward this community.":m.active?"You can take part.":!strcmp(m.state,"requested")?"Your request is awaiting review.":!strcmp(m.state,"withdrawn")?"You left this community.":!strcmp(m.state,"declined")?"Your request was declined.":"Your participation is not active.");fputs("</p><a href=\"",stdout);cjw_href(c.key,"conversations",NULL);fputs("\">Open conversations</a></article>",stdout);}}sqlite3_finalize(s);if(!count)fputs("<div class=\"ph-empty\"><p>Find a community to take part, or start one around a shared interest.</p></div>",stdout);if(more)printf("<a class=\"ph-next\" href=\"/profile?tab=communities&amp;cursor=%lld\">More of your communities</a>",last);
}
/* /community with nothing chosen: the neighbourhood exchange. Every row is a
 * published, public community record or a public post in one; the page writes
 * nothing and each control opens the community owner's own forms. */
static long long cjx_count(sqlite3 *db,const char *sql){sqlite3_stmt *s=NULL;long long n=0;if(db&&sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK&&sqlite3_step(s)==SQLITE_ROW)n=sqlite3_column_int64(s,0);sqlite3_finalize(s);return n;}
static const char *cjx_col(sqlite3_stmt *s,int i){const char *t=(const char*)sqlite3_column_text(s,i);return t?t:"";}
static void cjx_excerpt(const char *text,size_t cap){size_t n=strlen(text);if(n<=cap){html(text);return;}char b[400];if(cap>=sizeof b)cap=sizeof b-1;memcpy(b,text,cap);b[cap]=0;char *sp=strrchr(b,' ');if(sp&&sp>b+cap/2)*sp=0;html(b);fputs("…",stdout);}
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
 fputs("<div class=\"sx cx\"><nav class=\"sx-crumbs\" aria-label=\"Breadcrumb\"><a href=\"/\">Aurekai</a><span>/</span><span>Community</span><em>local circulation</em></nav>"
       "<header class=\"sx-hero\"><div><p class=\"sx-kicker\">The people around you</p><h1>Selling, lending and swapping <em>near you</em></h1>"
       "<p>Shops, studios and neighbours post what they make, lend and need inside their own communities. Every post below belongs to a community with a steward, so you always know who you are dealing with.</p></div>"
       "<aside class=\"sx-panel cx-radius\"><p class=\"sx-kicker\">In circulation</p><p class=\"cx-big\">",stdout);printf("%lld",posts);
 fputs("<small>public posts</small></p><p>",stdout);printf("%lld communities · %lld active members · %lld dated gatherings",bodies,members,events);fputs("</p></aside></header>",stdout);
 /* Each community is a circle you can read; the chips open its room. */
 fputs("<nav class=\"sx-chips\" aria-label=\"Communities\"><a class=\"sx-chip\" href=\"/community\" aria-current=\"true\">Everyone <span>",stdout);printf("%lld",posts);fputs("</span></a>",stdout);
 {sqlite3_stmt *s=NULL;const char *top="SELECT c.community,c.title,COUNT(p.id) n " CJW_SEARCH_PUBLIC_FROM " GROUP BY c.community ORDER BY n DESC,c.title LIMIT 10";
  if(sqlite3_prepare_v2(db,top,-1,&s,NULL)==SQLITE_OK){while(sqlite3_step(s)==SQLITE_ROW){fputs("<a class=\"sx-chip\" href=\"/community?community=",stdout);url_query_value(cjx_col(s,0));fputs("\">",stdout);html(cjx_col(s,1));printf(" <span>%d</span></a>",sqlite3_column_int(s,2));}sqlite3_finalize(s);}}
 fputs("</nav>",stdout);
 /* The register: what people have put out, newest first. */
 fputs("<section class=\"sx-catalogue\" aria-labelledby=\"cx-reg\"><header class=\"sx-section-head\"><div><p class=\"sx-kicker\">Open register</p><h2 id=\"cx-reg\">What people are offering and asking for</h2><p>Newest first. Open a post to reply in its community.</p></div><a class=\"sx-btn\" href=\"/communities\">All communities</a></header><div class=\"sx-grid cx-grid\">",stdout);
 {sqlite3_stmt *s=NULL;int k=0;
  if(sqlite3_prepare_v2(db,
     "SELECT p.post,p.community,p.title,p.body,COALESCE(p.event_at,''),COALESCE(p.venue,''),"
     "c.title,COALESCE(p.published_at,p.created_at),COALESCE(c.interest_label,'') "
     CJW_SEARCH_PUBLIC_FROM
     " ORDER BY p.published_at DESC,p.id DESC LIMIT 12",-1,&s,NULL)==SQLITE_OK){
   while(sqlite3_step(s)==SQLITE_ROW){k++;const char *ev=cjx_col(s,4),*venue=cjx_col(s,5);
    fputs("<article class=\"sx-card cx-lot\"><header><span class=\"sx-host\">",stdout);html(cjx_col(s,6));fputs("</span><span class=\"sx-badge",stdout);fputs(*ev?" is-on\">Gathering":"\">Post",stdout);fputs("</span></header><h3><a href=\"",stdout);
    cjw_href(cjx_col(s,1),"thread",cjx_col(s,0));fputs("\">",stdout);html(cjx_col(s,2));fputs("</a></h3><p>",stdout);cjx_excerpt(cjx_col(s,3),190);fputs("</p><footer><span>",stdout);
    if(*ev){html(ev);if(*venue){fputs(" · ",stdout);html(venue);}}else if(*cjx_col(s,8))html(cjx_col(s,8));else{const char *at=cjx_col(s,7);fwrite(at,1,strlen(at)>10?10:strlen(at),stdout);}
    fputs("</span><a href=\"",stdout);cjw_href(cjx_col(s,1),"thread",cjx_col(s,0));fputs("\">Open and reply</a></footer></article>",stdout);}
   sqlite3_finalize(s);}
  if(!k)fputs("<p class=\"sx-empty\">Nothing has been posted in a public community yet. Start one and put the first thing out.</p>",stdout);}
 fputs("</div></section>",stdout);
 /* The circles themselves: the ways trade happens here. */
 fputs("<section class=\"cx-modes\" aria-labelledby=\"cx-modes\"><div class=\"cx-modes-copy\"><p class=\"sx-kicker\">Circles</p><h2 id=\"cx-modes\">Where neighbours trade</h2><p>Each circle is a community with its own steward and its own rules for who may post. Join the ones you live near or care about.</p>"
       "<p class=\"sx-panel cx-zero\"><b>No commission.</b> Aurekai takes nothing from a sale, loan or swap arranged in a community.</p></div><ol class=\"cx-mode-list\">",stdout);
 {sqlite3_stmt *s=NULL;int k=0;snprintf(sql,sizeof sql,"SELECT b.community,b.title,b.description,COALESCE(NULLIF(b.interest_label,''),b.governance_scope,''),(SELECT COUNT(*) FROM community_membership m WHERE m.namespace='root' AND m.community=b.community AND m.status='published' AND m.state='active') %s ORDER BY b.title LIMIT 8",pub);
  if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){while(sqlite3_step(s)==SQLITE_ROW){k++;printf("<li><a href=\"/community?community=");url_query_value(cjx_col(s,0));printf("\"><span class=\"cx-n\">%02d</span><small>",k);html(cjx_col(s,3));fputs("</small><b>",stdout);html(cjx_col(s,1));fputs("</b><span>",stdout);cjx_excerpt(cjx_col(s,2),120);printf("</span><em>%d active %s</em></a></li>",sqlite3_column_int(s,4),sqlite3_column_int(s,4)==1?"member":"members");}sqlite3_finalize(s);}
  if(!k)fputs("<li class=\"sx-empty\">No public community yet.</li>",stdout);}
 fputs("</ol></section>",stdout);
 fputs("<section class=\"cx-trust\" aria-labelledby=\"cx-trust\"><div><p class=\"sx-kicker\">Accountability</p><h2 id=\"cx-trust\">Kept honest by people, not surveillance</h2>"
       "<ul class=\"sx-checks cx-checks\"><li><b>Stewards admit members.</b> Only people a community has accepted can post in it.</li><li><b>Posts stay where they were made.</b> Nothing here is copied to your profile or sold on.</li><li><b>Disagreements go to the circle.</b> A community's steward settles disputes in its own conversation.</li></ul></div>",stdout);
 printf("<dl class=\"sx-stats cx-stats\"><div><dt>Communities</dt><dd>%lld</dd></div><div><dt>Active members</dt><dd>%lld</dd></div><div><dt>Public posts</dt><dd>%lld</dd></div><div><dt>Gatherings</dt><dd>%lld</dd></div></dl></section>",bodies,members,posts,events);
 fputs("<section class=\"cx-cta\"><div><h2>Put something out, or ask for it</h2><p>Post inside a community you belong to, or start one for your street, studio or shop.</p></div><nav class=\"sx-actions\">",stdout);
 fputs(signed_in?"<a class=\"sx-btn cx-dark\" href=\"/profile?tab=communities\">Post in your communities</a>":"<a class=\"sx-btn cx-dark\" href=\"/login?next=%2Fcommunity\">Sign in to post</a>",stdout);
 fputs("<a class=\"sx-btn\" href=\"/community/new\">Start a community</a><a class=\"sx-btn\" href=\"/guilds\">Browse guilds</a></nav></section></div>",stdout);
 ak_app_close();
}
static int serve_community_journey(const char *method,const char *path,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db,const char *accept){
 char path_community[97]={0}, path_post[97]={0}; int permalink=0;
 if(path&&!strncmp(path,"/community/",11)){
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
 int guild_directory=!strcmp(path,"/guilds")||!strcmp(path,"/guilds/"),guild_create=!strcmp(path,"/guild/new"),directory=!strcmp(path,"/communities")||!strcmp(path,"/communities/"),room=!strcmp(path,"/community")||!strcmp(path,"/community/"),creating=!strcmp(path,"/community/new"),preview=!strcmp(path,"/community/preview"),action=!strcmp(path,"/community/action"),legacy=!strcmp(path,"/govern/community-join");if(!guild_directory&&!guild_create&&!directory&&!room&&!creating&&!preview&&!action&&!legacy&&!permalink)return 0;
 if(!preview&&!action&&!legacy&&!wants_html(accept))return 0;
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
 else if(!strcmp(tab,"join")){fputs("<h2>Take part in this community</h2><p>Your request goes to this community's steward. It does not join an external network, sign you up for marketing, or publish anything on your profile sites.</p>",stdout);if(!owner&&(!member.id||!strcmp(member.state,"withdrawn")))cjw_action("request",c.key,"",member.digest,"Send my join request");}
 else if(!strcmp(tab,"about")){fputs("<h2>Community purpose</h2><p class=\"cj-review\">",stdout);html(c.description);fputs("</p><p>Conversations are public. Participation requires a local request accepted by this community's steward. Authors can edit or remove their own words, and the steward can hide a contribution.</p>",stdout);}
 else{fputs("<div class=\"cj-content\"><section>",stdout);if(needs_post){if(*selected.parent){fputs("<p><a href=\"",stdout);cjw_href(c.key,"thread",selected.parent);fputs("\">Back to the parent contribution</a></p>",stdout);if(strcmp(rootkey,selected.parent)){fputs("<p><a href=\"",stdout);cjw_href(c.key,"thread",rootkey);fputs("\">Conversation start</a></p>",stdout);}}cjw_card(db,&c,&selected,actor,1,can_post);fputs("<h2>Replies</h2><div class=\"cj-replies\">",stdout);cjw_posts(db,&c,selected.key,actor,can_post,offset);fputs("</div>",stdout);}else cjw_posts(db,&c,"",actor,can_post,offset);fputs("</section><aside><h2>Your place here</h2><p>",stdout);html(owner?"You steward this community. Manage only its requests and conversations.":can_post?"You can start a conversation or reply. Your contributions remain yours to edit.":"Read freely. Request to join when you want to contribute.");fputs("</p><a href=\"/profile?tab=communities\">Return through your profile</a></aside></div>",stdout);}
 cjw_end();sqlite3_close(db);return 1;
}
#endif
