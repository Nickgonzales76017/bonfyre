#ifndef BONFYRE_APPLICATION_CONTINUATION_H
#define BONFYRE_APPLICATION_CONTINUATION_H
#include "account_navigation_markup.h"
#include "bf_msgpack.h"
/* Read-only projections of existing ApplicationBinding and CMS record owners.
 * CONTINUATIONS.tsv is part of the exact retained editor release, not a store
 * or a second application registry. No file bytes are hydrated for this view. */
#define AC_VIEWS 16
#define AC_ROWS 128
#define AC_URL 4096

/* The public design is a continuation hint, never an actor or authority. */
static int ac_design_query(const char *query,char *rest,size_t cap,char *design,size_t dcap){
    size_t used=0;int found=0;rest[0]=design[0]=0;
    if(query&&strlen(query)>4096)return 0;
    for(const char *p=query?query:"";*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[24];
        if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
        if(!strcmp(key,"design")){char ns[128],slug[72];if(found++||!psetup_decode(eq+1,(size_t)(end-eq-1),design,dcap)||!production_reference(design,ns,slug))return 0;}
        else{size_t n=(size_t)(end-p);if(used+n+2>cap)return 0;if(used)rest[used++]='&';memcpy(rest+used,p,n);used+=n;rest[used]=0;}
        p=*end?end+1:end;
    }
    return 1;
}

typedef struct { char id[64],label[160],entry[128],prefix[512],title[160],query[1024]; } AcView;
typedef struct { char key[512]; long long revision; const uint8_t *body; size_t bytes; } AcRow;
typedef struct { long long epoch; int more; char cursor[512]; size_t count; AcRow rows[AC_ROWS]; } AcPage;

static int ac_copy(const uint8_t *restrict p,size_t n,char *restrict out,size_t cap) {
    if(!p || n>=cap || memchr(p,0,n))return 0;
    for(size_t i=0;i<n;i++)if(p[i]<32 || p[i]==127)return 0;
    memcpy(out,p,n);out[n]=0;return 1;
}
static int ac_word(const char *s) { return s && *s && strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")==strlen(s); }
static int ac_selector(const char *s) {
    if(!s || !*s)return 0;
    if(!strcmp(s,"$key"))return 1;
    if(*s=='@')return s[1]!=0;
    if(strlen(s)>127 || *s=='.' || s[strlen(s)-1]=='.' || strstr(s,".."))return 0;
    return strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")==strlen(s);
}
static int ac_queries(const char *text) {
    char copy[1024],seen[8][64];size_t count=0;
    if(!text || strlen(text)>=sizeof(copy))return 0;
    if(!strcmp(text,"-"))return 1;
    snprintf(copy,sizeof(copy),"%s",text);
    for(char *p=copy;*p;){char *end=strchr(p,'&');if(end)*end=0;char *eq=strchr(p,'=');
        if(!eq || count==8)return 0;*eq++=0;
        if(strlen(p)>=64 || !ac_word(p) || !ac_selector(eq))return 0;
        for(size_t i=0;i<count;i++)if(!strcmp(seen[i],p))return 0;
        snprintf(seen[count++],64,"%s",p);if(!end)break;p=end+1;if(!*p)return 0;
    }
    return count>0;
}
static int ac_profiles(const char *release,AcView *views,size_t *count) {
    size_t bytes=0;*count=0;unsigned char *file=aw_package(release,"CONTINUATIONS.tsv",&bytes);
    if(!file)return 0;
    int ok=0;if(bytes>32768 || memchr(file,0,bytes))goto done;
    char *save=NULL,*line=strtok_r((char *)file,"\n",&save);
    if(!line || strcmp(line,"view\tlabel\tentry\tprefix\ttitle\tquery"))goto done;
    while((line=strtok_r(NULL,"\n",&save))){
        if(*count==AC_VIEWS)goto done;AcView *v=&views[*count];char *parts[6]={line};
        for(int n=1;n<6;n++){parts[n]=strchr(parts[n-1],'\t');if(!parts[n])goto done;*parts[n]++=0;}
        if(strchr(parts[5],'\t'))goto done;
#define AC_FIELD(f,n) if(!ac_copy((const uint8_t *)parts[n],strlen(parts[n]),v->f,sizeof(v->f)))goto done
        AC_FIELD(id,0);AC_FIELD(label,1);AC_FIELD(entry,2);AC_FIELD(prefix,3);AC_FIELD(title,4);AC_FIELD(query,5);
#undef AC_FIELD
        if(!ac_word(v->id) || !*v->label || !aw_path(v->entry) || v->entry[strlen(v->entry)-1]!='/' || !*v->prefix || !ac_selector(v->title) || !ac_queries(v->query) || !workspace_package_available(release,v->entry))goto done;
        for(size_t n=0;n<*count;n++)if(!strcmp(views[n].id,v->id))goto done;
        (*count)++;
    }
    ok=*count>0;
 done:free(file);if(!ok)*count=0;return ok;
}
/* Bound traversal independently of the MessagePack library's generic skip. */
static int ac_skip(bf_mp_reader_t *r,unsigned depth,size_t *nodes) {
    if(depth>32 || ++*nodes>50000)return 0;
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_ARRAY || t==BF_MP_MAP){uint64_t n=(uint64_t)v.v.count*(t==BF_MP_MAP?2:1);if(n>50000)return 0;while(n--)if(!ac_skip(r,depth+1,nodes))return 0;return 1;}
    return t!=BF_MP_EXT && t!=BF_MP_ERROR && t!=BF_MP_EOF;
}
static int ac_text(bf_mp_reader_t *r,char *out,size_t cap) { bf_mp_value_t v;return bf_mp_read_next(r,&v)==BF_MP_STR && ac_copy(v.v.raw.ptr,v.v.raw.len,out,cap); }
static int ac_uint(bf_mp_reader_t *r,long long *out) {
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_UINT && v.v.u<INT_MAX){*out=(long long)v.v.u;return 1;}
    if(t==BF_MP_INT && v.v.i>=0 && v.v.i<INT_MAX){*out=v.v.i;return 1;}return 0;
}
static int ac_array(bf_mp_reader_t *r,uint32_t count) { bf_mp_value_t v;return bf_mp_read_next(r,&v)==BF_MP_ARRAY && v.v.count==count; }
static int ac_page(const uint8_t *body,size_t bytes,AcPage *out) {
    memset(out,0,sizeof(*out));bf_mp_reader_t r;bf_mp_reader_init(&r,body,bytes);bf_mp_value_t v;long long version;
    if(!ac_array(&r,5) || !ac_uint(&r,&version) || version!=1 || !ac_uint(&r,&out->epoch) || bf_mp_read_next(&r,&v)!=BF_MP_BOOL)return 0;
    out->more=v.v.b;
    if(!ac_text(&r,out->cursor,sizeof(out->cursor)) || bf_mp_read_next(&r,&v)!=BF_MP_ARRAY || v.v.count>AC_ROWS)return 0;
    out->count=v.v.count;
    for(size_t n=0;n<out->count;n++){
        AcRow *row=&out->rows[n];if(!ac_array(&r,3) || !ac_text(&r,row->key,sizeof(row->key)) || !*row->key || !ac_uint(&r,&row->revision) || row->revision<1)return 0;
        bf_mp_type_t t=bf_mp_read_next(&r,&v);if(t!=BF_MP_BIN && t!=BF_MP_NIL)return 0;
        if(t==BF_MP_BIN){if(v.v.raw.len>256u*1024u)return 0;row->body=v.v.raw.ptr;row->bytes=v.v.raw.len;}
        if(n && strcmp(out->rows[n-1].key,row->key)>=0)return 0;
    }
    if(out->more && !out->count)return 0;
    return r.pos==r.len && (!out->count || !strcmp(out->cursor,out->rows[out->count-1].key));
}
static int ac_plain(const AcRow *row,bf_mp_reader_t *plain) {
    bf_mp_reader_t r;bf_mp_reader_init(&r,row->body,row->bytes);long long version;size_t nodes=0;
    if(!row->body || !ac_array(&r,3) || !ac_uint(&r,&version) || (version!=2 && version!=3 && version!=4))return 0;
    *plain=r;if(!ac_skip(&r,0,&nodes))return 0;plain->len=r.pos;
    bf_mp_value_t v;if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY || v.v.count>64)return 0;
    for(uint32_t n=0;n<v.v.count;n++)if(!ac_skip(&r,0,&nodes))return 0;
    return r.pos==r.len;
}
static int ac_field(bf_mp_reader_t r,const char *path,char *out,size_t cap) {
    bf_mp_value_t v;const char *dot=strchr(path,'.');size_t length=dot?(size_t)(dot-path):strlen(path),nodes=0;
    if(!length || bf_mp_read_next(&r,&v)!=BF_MP_MAP || v.v.count>4096)return 0;
    uint32_t count=v.v.count;int found=0;char result[1024]="";
    for(uint32_t n=0;n<count;n++){
        if(bf_mp_read_next(&r,&v)!=BF_MP_STR)return 0;
        if(v.v.raw.len==length && !memcmp(v.v.raw.ptr,path,length)){
            if(found++)return 0;bf_mp_reader_t value=r;
            if(dot){if(!ac_field(value,dot+1,result,sizeof(result)))return 0;}
            else if(!ac_text(&value,result,sizeof(result)))return 0;
        }
        if(!ac_skip(&r,0,&nodes))return 0;
    }
    return found==1 && ac_copy((const uint8_t *)result,strlen(result),out,cap);
}
static int ac_value(const AcRow *row,const char *selector,char *out,size_t cap) {
    if(!strcmp(selector,"$key"))return ac_copy((const uint8_t *)row->key,strlen(row->key),out,cap);
    if(*selector=='@')return ac_copy((const uint8_t *)selector+1,strlen(selector+1),out,cap);
    bf_mp_reader_t plain;return ac_plain(row,&plain) && ac_field(plain,selector,out,cap);
}
static int ac_encoded(const char *text,char *out,size_t cap) {
    static const char digits[]="0123456789ABCDEF";size_t n=0;
    for(const unsigned char *p=(const unsigned char *)text;*p;p++){
        int safe=(*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||strchr("-._~",*p);
        if(n+(safe?1:3)>=cap)return 0;if(safe)out[n++]=(char)*p;else{out[n++]='%';out[n++]=digits[*p>>4];out[n++]=digits[*p&15];}
    }
    out[n]=0;return 1;
}
static int ac_destination(const AcView *v,const AcRow *row,const char *id,char *url,size_t cap) {
    if(!row->body || (v->prefix[0]=='='?strcmp(row->key,v->prefix+1):strncmp(row->key,v->prefix,strlen(v->prefix))))return 0;
    bf_mp_reader_t plain;if(!ac_plain(row,&plain))return 0;
    int n=snprintf(url,cap,"/apps/%s/%s",id,v->entry);if(n<0 || (size_t)n>=cap)return 0;
    if(!strcmp(v->query,"-"))return 1;
    char copy[1024];snprintf(copy,sizeof(copy),"%s",v->query);int first=1;
    for(char *p=copy;*p;){char *end=strchr(p,'&');if(end)*end=0;char *eq=strchr(p,'=');if(!eq)return 0;*eq++=0;
        char value[1024],encoded[3072];if(!ac_value(row,eq,value,sizeof(value)) || !*value || !ac_encoded(value,encoded,sizeof(encoded)))return 0;
        size_t used=strlen(url);n=snprintf(url+used,cap-used,"%c%s=%s",first?'?':'&',p,encoded);if(n<0 || (size_t)n>=cap-used)return 0;
        first=0;if(!end)break;p=end+1;
    }
    return 1;
}
#include "application_context.h"
static int ac_verified(const char *id,const char *subject,const char *scope,const char *pack,const char *release,const char *entry,const char *actor) {
    AcContext resolved;
    return ac_context(id,subject,scope,actor,&resolved) && !strcmp(resolved.pack,pack) && !strcmp(resolved.release,release) && !strcmp(resolved.entry,entry);
}
static unsigned char *ac_navigation(const char *id,const char *context,const char *pack,size_t *bytes) {
    char ctx[320],pk[320];if(!ac_encoded(context,ctx,sizeof(ctx)) || !ac_encoded(pack,pk,sizeof(pk)))return NULL;
    const char *kind=!strcmp(context,"self")?"Personal work":!strncmp(context,"household:",10)?"Household":"Organization";
    char label[640];size_t used=(size_t)snprintf(label,sizeof(label),"%s",kind);
    const char *name=strchr(context,':');if(name){name++;label[used++]=':';label[used++]=' ';for(;*name;name++){
        const char *escaped=*name=='&'?"&amp;":*name=='<'?"&lt;":*name=='>'?"&gt;":*name=='\"'?"&quot;":NULL;
        if(escaped){size_t n=strlen(escaped);if(used+n>=sizeof(label))return NULL;memcpy(label+used,escaped,n);used+=n;}
        else{if(used+1>=sizeof(label))return NULL;label[used++]=*name;}
    }}label[used]=0;
    char campaign[256]="";
    if(!strcmp(pack,"experience.source-campaign.workspace")){
        int n=snprintf(campaign,sizeof(campaign),"<a data-campaign-link-builder href=\"/apps/%s/_campaign-link\" target=\"_blank\" rel=\"noopener\">Build campaign link (new tab)</a>",id);
        if(n<0 || n>=(int)sizeof(campaign))return NULL;
    }
    const char *activity=BF_AUREKAI_ACTIVITY_ASSETS;size_t activity_bytes=strlen(activity);
    char *out=malloc(4096+activity_bytes);if(!out)return NULL;
    int size=snprintf(out,4096,"<style>[data-native-workspace]{display:flex;flex-wrap:wrap;gap:12px;align-items:center;padding:10px 16px;font:14px system-ui;box-sizing:border-box;overflow-wrap:anywhere}[data-native-workspace] a{color:inherit;display:inline-flex;align-items:center;min-height:44px}[data-native-workspace] strong{max-width:100%%}</style><nav data-native-workspace data-activity-context=\"%s\" aria-label=\"Workspace\"><strong>%s</strong><a href=\"/apps?context=%s\">Your work</a><a href=\"/programs?context=%s\">Programs</a><a href=\"/apps/%s/_work\">Saved work</a><a href=\"/packs/setup?pack=%s&amp;context=%s\">Configuration</a>%s</nav>",ctx,label,ctx,ctx,id,pk,ctx,campaign);
    if(size<0 || size>=4096){free(out);return NULL;}memcpy(out+size,activity,activity_bytes+1);*bytes=(size_t)size+activity_bytes;return (unsigned char *)out;
}
static void ac_work_link(const char *id,const AcView *v,const AcRow *row) {
    fputs("/apps/",stdout);html(id);fputs("/_open?view=",stdout);url_query_value(v->id);fputs("&amp;key=",stdout);url_query_value(row->key);printf("&amp;revision=%lld",row->revision);
}
static int ac_decode(const char *restrict s,size_t len,char *restrict out,size_t cap) {
    size_t n=0;
    for(size_t i=0;i<len;i++){
        unsigned char c=(unsigned char)s[i];
        if(c=='%'){if(i+2>=len || psetup_hex(s[i+1])<0 || psetup_hex(s[i+2])<0)return 0;c=(unsigned char)(psetup_hex(s[i+1])*16+psetup_hex(s[i+2]));i+=2;}
        else if(c=='+')c=' ';
        if(c<32 || c==127 || n+1>=cap)return 0;out[n++]=(char)c;
    }
    out[n]=0;return 1;
}
#include "application_views.h"
static int ac_collection_declared(const char *pack,const char *family,const char *prefix){
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int ok=0;
    if(!db||!pack||!*pack||!family||!*family||!prefix||!*prefix)goto done;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace-record' AND needs=?2 AND fields=?3 AND status='published' LIMIT 1",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,pack,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,family,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,prefix,-1,SQLITE_STATIC);ok=sqlite3_step(s)==SQLITE_ROW;
 done:sqlite3_finalize(s);if(db)sqlite3_close(db);return ok;
}
static void ac_collection_label(const char *prefix,char *out,size_t cap){
    size_t n=0;int word=1;if(!out||!cap)return;out[0]=0;
    for(const unsigned char *p=(const unsigned char *)prefix;p&&*p&&n+1<cap;p++){
        unsigned char c=*p;if(c==':'||c=='-'||c=='_'){if(n&&out[n-1]!=' ')out[n++]=' ';word=1;continue;}
        out[n++]=(char)(word&&c>='a'&&c<='z'?c-'a'+'A':c);word=0;
    }
    while(n&&out[n-1]==' ')n--;out[n]=0;
}
/* A declared record family may contain narrower or exact-key continuations.
 * Keep those real views reachable, but never invent one for supporting rows. */
static int ac_collection_has_view(const AcView *views,size_t count,const char *prefix){
    if(!prefix||!*prefix)return 0;
    size_t length=strlen(prefix);
    for(size_t i=0;i<count;i++){
        const char *candidate=views[i].prefix;
        if(*candidate=='='){if(!strncmp(candidate+1,prefix,length))return 1;}
        else if(!strncmp(candidate,prefix,length)||!strncmp(prefix,candidate,strlen(candidate)))return 1;
    }
    return 0;
}
static void ac_collection_owner(const char *binding,const char *entry){
    fputs("<a data-work-owner href=\"/apps/",stdout);html(binding);fputs("/",stdout);html(entry);fputs("\">Open the owning editor</a>",stdout);
}
static void ac_installed_collections(const char *binding,const char *pack,const char *family,const char *release,const char *entry){
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;char prefixes[32][128];size_t count=0,view_count=0;AcView views[AC_VIEWS];
    if(!db||!binding||!pack||!family||!ac_profiles(release,views,&view_count)||!workspace_package_available(release,entry))goto done;
    if(sqlite3_prepare_v2(db,"SELECT fields FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace-record' AND needs=?2 AND status='published' ORDER BY id LIMIT 33",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,pack,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,family,-1,SQLITE_STATIC);
    while(sqlite3_step(s)==SQLITE_ROW){const char *prefix=(const char *)sqlite3_column_text(s,0);if(!prefix||!*prefix||strlen(prefix)>=sizeof(prefixes[0])||count>=32)continue;int seen=0;for(size_t i=0;i<count;i++)if(!strcmp(prefixes[i],prefix)){seen=1;break;}if(!seen)snprintf(prefixes[count++],sizeof(prefixes[0]),"%s",prefix);}
    if(!count)goto done;
    fputs("<p class=\"pack-family\">Workspace family <code>",stdout);html(family);fputs("</code></p>",stdout);
    fputs("<details class=\"pack-collections\"><summary>Browse ",stdout);printf("%zu",count);fputs(" collection",stdout);if(count!=1)fputc('s',stdout);fputs(" <span aria-hidden=\"true\">↘</span></summary><nav class=\"pack-collection-list\" aria-label=\"Collections in this workspace\">",stdout);
    for(size_t i=0;i<count;i++){
        char label[160];int mapped=ac_collection_has_view(views,view_count,prefixes[i]);ac_collection_label(prefixes[i],label,sizeof(label));
        fputs("<a data-work-collection href=\"/apps/",stdout);html(binding);
        if(mapped){fputs("/_work?prefix=",stdout);url_query_value(prefixes[i]);}else{fputs("/",stdout);html(entry);}
        fputs("\" data-collection-kind=\"",stdout);fputs(mapped?"saved-work":"supporting",stdout);fputs("\">",stdout);page_glyph(mapped?"list":"apps");fputs("<span>",stdout);html(label);fputs("</span><code>",stdout);html(prefixes[i]);
        fputs(mapped?"</code></a>":"</code><small>Open owning editor</small></a>",stdout);
    }
    fputs("</nav></details>",stdout);
 done:sqlite3_finalize(s);if(db)sqlite3_close(db);
}
#include "work_board.h"
#include "work_provenance.h"
/* POST /work-board/act: the person records where their own provider task
 * stands. Done resolves it; the waiting reasons keep it open with a dated note;
 * Not now lowers it. Same-origin form, current session, listed task only. */
static int ac_work_board_act(const char *method,const char *target,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    if(strcmp(target,"/work-board/act"))return 0;
    if(strcmp(method,"POST")){aw_error(405,"Use the work board to record this.\n");return 1;}
    char origin[300],http[300],https[300],ctype[96],context[96]="",task[128]="",action[16]="",back[8]="",actor[160],subject[160],operator_db[PATH_MAX];const char *scope=NULL;unsigned seen=0;
    snprintf(http,sizeof(http),"http://%s",host?host:"");snprintf(https,sizeof(https),"https://%s",host?host:"");
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||(strcmp(origin,http)&&strcmp(origin,https))){aw_error(403,"Record this from the work board on this site.\n");return 1;}
    if(!aw_header(raw,"Content-Type",ctype,sizeof(ctype))||strncmp(ctype,"application/x-www-form-urlencoded",33)||!body||strlen(body)>1024){aw_error(400,"Send one work board form.\n");return 1;}
    for(const char *p=body;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[16],*out=NULL;size_t cap=0;unsigned bit=0;
        if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key))){aw_error(400,"Send one work board form.\n");return 1;}
        if(!strcmp(key,"context")){out=context;cap=sizeof(context);bit=1;}else if(!strcmp(key,"task")){out=task;cap=sizeof(task);bit=2;}else if(!strcmp(key,"action")){out=action;cap=sizeof(action);bit=4;}else if(!strcmp(key,"back")){out=back;cap=sizeof(back);bit=8;}
        if(!out||(seen&bit)||!ac_decode(eq+1,(size_t)(end-eq-1),out,cap)){aw_error(400,"Send one work board form.\n");return 1;}seen|=bit;p=*end?end+1:end;}
    if((seen&7)!=7||((seen&8)&&strcmp(back,"inbox"))||!ac_word(task)||(strcmp(action,"done")&&strcmp(action,"email")&&strcmp(action,"review")&&strcmp(action,"later")&&strcmp(action,"approve"))){aw_error(400,"Choose one task and one of done, email, review or later.\n");return 1;}
    if(psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1){aw_error(403,"Sign in with current access to this account.\n");return 1;}
    if(!ac_work_listed(task,subject,scope?scope:"")||!ac_work_operator_path(operator_db,sizeof(operator_db))){aw_error(404,"That task is not on this account's board.\n");return 1;}
    sqlite3 *db=NULL;sqlite3_stmt *s=NULL;int ok=0;time_t now=time(NULL);struct tm tm;char day[32]="",note[160];
    if(gmtime_r(&now,&tm))strftime(day,sizeof(day),"%a %d %b %Y",&tm);
    if(!strcmp(action,"approve")){
        /* A release approval is the person's click, written where the release
         * watcher reads it. The watcher, not this request, runs the activation
         * and reports back on the same task. */
        char approvals[PATH_MAX],path[PATH_MAX],release[96]="",line[512];sqlite3 *odb=NULL;sqlite3_stmt *q=NULL;int found=0;
        const char *idx=getenv("BONFYRE_ACCOUNT_WORK_DB"),*slash=idx?strrchr(idx,'/'):NULL;
        if(!slash||snprintf(approvals,sizeof(approvals),"%.*s/approvals",(int)(slash-idx),idx)>=(int)sizeof(approvals)){aw_error(503,"Approvals are not configured here.\n");return 1;}
        if(sqlite3_open_v2(operator_db,&odb,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK&&sqlite3_prepare_v2(odb,"SELECT substr(dedupe_key,18) FROM goals WHERE id=?1 AND source='release-approval' AND dedupe_key LIKE 'release-approval:%' AND state NOT IN('resolved','superseded')",-1,&q,NULL)==SQLITE_OK){
            sqlite3_bind_text(q,1,task+17,-1,SQLITE_STATIC);if(sqlite3_step(q)==SQLITE_ROW){snprintf(release,sizeof(release),"%s",(const char*)sqlite3_column_text(q,0));found=1;}}
        sqlite3_finalize(q);sqlite3_close(odb);
        if(!found||strspn(release,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")!=strlen(release)){aw_error(409,"That is not an open release approval.\n");return 1;}
        mkdir(approvals,0700);
        if(snprintf(path,sizeof(path),"%s/%s.approved",approvals,release)>=(int)sizeof(path)){aw_error(400,"Release name too long.\n");return 1;}
        int fd=open(path,O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0600);
        if(fd<0){aw_error(409,"This release is already approved.\n");return 1;}
        int len=snprintf(line,sizeof(line),"release\t%s\ngoal\t%s\nactor\t%s\napproved_at\t%lld\n",release,task+17,actor,(long long)time(NULL));
        int wrote=len>0&&write(fd,line,(size_t)len)==len;close(fd);if(!wrote){unlink(path);aw_error(503,"The approval could not be saved.\n");return 1;}
    }
    snprintf(note,sizeof(note),!strcmp(action,"approve")?"Approved by you %s; the release is being put live":!strcmp(action,"email")?"Waiting on an email or phone check (recorded %s)":!strcmp(action,"review")?"Waiting on the provider's review or sales reply (recorded %s)":!strcmp(action,"later")?"Set aside for now (recorded %s)":"Set up (recorded %s)",day);
    if(sqlite3_open_v2(operator_db,&db,SQLITE_OPEN_READWRITE,NULL)==SQLITE_OK){sqlite3_busy_timeout(db,3000);
        if(sqlite3_prepare_v2(db,"UPDATE goals SET state=CASE WHEN ?2='done' THEN 'resolved' ELSE state END,last_error=?3,"
            "priority=CASE WHEN ?2='later' THEN MAX(priority-30,1) ELSE priority END,updated_at=?4 WHERE id=?1 AND (source='provider-estate' OR (source='release-approval' AND ?2 IN('approve','later'))) AND state NOT IN('resolved','superseded')",-1,&s,NULL)==SQLITE_OK){
            sqlite3_bind_text(s,1,task+17,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,action,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,note,-1,SQLITE_STATIC);sqlite3_bind_double(s,4,(double)now);
            ok=sqlite3_step(s)==SQLITE_DONE&&sqlite3_changes(db)==1;}
        sqlite3_finalize(s);}
    sqlite3_close(db);
    if(!ok){aw_error(409,"This task is already settled or could not be updated. Reopen the board.\n");return 1;}
    /* The inbox acts on the same goal and returns the person to the inbox. */
    if(*back){fputs("HTTP/1.1 303 See Other\r\nLocation: /inbox?context=",stdout);url_query_value(context);fputs("&done=",stdout);url_query_value(action);fputs("#",stdout);url_query_value(task);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}
    fputs("HTTP/1.1 303 See Other\r\nLocation: /work-board?context=",stdout);url_query_value(context);fputs("&task=",stdout);url_query_value(task);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);
    return 1;
}
/* Goal text with its https addresses made into links that open the provider's
 * own page in a new tab. Everything else is escaped text. */
static void ac_work_linked_text(const char *text){
    char chunk[2048];
    for(const char *p=text;*p;){
        const char *u=strstr(p,"https://");
        if(!u){html(p);return;}
        size_t lead=(size_t)(u-p);while(lead){size_t n=lead<sizeof(chunk)-1?lead:sizeof(chunk)-1;memcpy(chunk,p,n);chunk[n]=0;html(chunk);p+=n;lead-=n;}
        size_t n=strcspn(u," \t\r\n<>\"'()");while(n>8&&strchr(".,;:",u[n-1]))n--;
        if(n<=8||n>=sizeof(chunk)){memcpy(chunk,u,8);chunk[8]=0;html(chunk);p=u+8;continue;}
        memcpy(chunk,u,n);chunk[n]=0;
        /* NAME={email} marks a signup page that reads the address from its own
         * link. The address itself lives only in the person's browser. */
        char *slot=strstr(chunk,"={email}");
        if(slot&&!slot[8]){char *sep=slot;while(sep>chunk&&*sep!='?'&&*sep!='&')sep--;
            if(sep>chunk&&slot-sep>1&&slot-sep<32){char param[32];size_t pl=(size_t)(slot-sep-1);memcpy(param,sep+1,pl);param[pl]=0;*sep=0;
                fputs("<a rel=\"noopener noreferrer\" target=\"_blank\" data-prefill-param=\"",stdout);html(param);fputs("\" data-prefill-base=\"",stdout);html(chunk);fputs("\" href=\"",stdout);html(chunk);fputs("\">",stdout);html(chunk);fputs("</a> <small data-prefill-note>(fills your sign-up email)</small>",stdout);p=u+n;continue;}}
        fputs("<a rel=\"noopener noreferrer\" target=\"_blank\" href=\"",stdout);html(chunk);fputs("\">",stdout);html(chunk);fputs("</a>",stdout);p=u+n;
    }
}
/* Keep long diagnostic records available without turning the task into a wall
 * of logs. The full source remains escaped and readable on explicit expansion. */
static void ac_work_note(const char *text){
    if(!text||!*text)return;
    if(strlen(text)>320||strchr(text,'\n')){
        fputs("<details class=\"work-note\"><summary>View full recorded update</summary><div class=\"work-note-body\">",stdout);
        ac_work_linked_text(text);fputs("</div></details>",stdout);
    }else{fputs("<p class=\"work-focus-error\">",stdout);ac_work_linked_text(text);fputs("</p>",stdout);}
}
/* The selected task told in full: what was asked, where it stands and what its
 * runs said. Only an assistant goal this account's work index already lists is
 * read, so a guessed id shows nothing that the right rail would not. */
static void ac_work_focus(const char *context,const char *task,const char *subject,const char *scope){
    char operator_db[PATH_MAX];const char *index=getenv("BONFYRE_ACCOUNT_WORK_DB");sqlite3 *db=NULL;sqlite3_stmt *s=NULL;int listed=0;
    if(!task||strncmp(task,"account-operator-",17)||!index||*index!='/'||!ac_work_operator_path(operator_db,sizeof(operator_db)))return;
    if(sqlite3_open_v2(index,&db,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK){sqlite3_busy_timeout(db,1000);
        if(sqlite3_prepare_v2(db,"SELECT 1 FROM account_work WHERE id=?1 AND subject=?2 AND scope=?3",-1,&s,NULL)==SQLITE_OK){
            sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,scope,-1,SQLITE_STATIC);listed=sqlite3_step(s)==SQLITE_ROW;}
        sqlite3_finalize(s);s=NULL;}
    sqlite3_close(db);db=NULL;if(!listed)return;
    if(sqlite3_open_v2(operator_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){sqlite3_close(db);return;}sqlite3_busy_timeout(db,1000);
    if(sqlite3_prepare_v2(db,"SELECT title,details,state,attempts,last_error,created_at,updated_at,source FROM goals WHERE id=?1",-1,&s,NULL)!=SQLITE_OK){sqlite3_close(db);return;}
    sqlite3_bind_text(s,1,task+17,-1,SQLITE_STATIC);
    if(sqlite3_step(s)==SQLITE_ROW){
        const char *title=(const char*)sqlite3_column_text(s,0),*details=(const char*)sqlite3_column_text(s,1),*state=(const char*)sqlite3_column_text(s,2),*error=(const char*)sqlite3_column_text(s,4);
        const char *source=(const char*)sqlite3_column_text(s,7);
        int is_release=source&&!strcmp(source,"release-approval"),finished=state&&!strcmp(state,"resolved");
        int activated=is_release&&finished&&error&&!strncmp(error,"Live since ",11);
        int attempts=sqlite3_column_int(s,3);
        fputs("<section class=\"work-focus\" data-work-focus aria-labelledby=\"work-focus-title\"><p class=\"work-focus-kicker\">",stdout);html(ac_work_state_words(state?state:""));
        fputs("</p><h2 id=\"work-focus-title\">",stdout);
        if(is_release&&finished){fputs(activated?"Release live: ":"Release finished: ",stdout);html(title&&!strncmp(title,"Approve release ",16)?title+16:title?title:"");}
        else html(title?title:"");
        fputs("</h2><p class=\"work-focus-dates\">Asked ",stdout);ac_work_when(sqlite3_column_double(s,5));
        fputs(" · last changed ",stdout);ac_work_when(sqlite3_column_double(s,6));if(attempts>0)printf(" · tried %d time%s",attempts,attempts==1?"":"s");fputs("</p>",stdout);
        if(details&&*details){fputs(is_release&&finished?"<details><summary>Original release request</summary><p class=\"work-focus-details\">":"<details><summary>What it asks for</summary><p class=\"work-focus-details\">",stdout);ac_work_linked_text(details);fputs("</p></details>",stdout);}
        int person=source&&ac_work_person_source(source);
        if(error&&*error){fputs(activated?"<h3>Activation receipt</h3>":person?"<h3>Where it stands</h3>":state&&(!strcmp(state,"blocked")||!strcmp(state,"retry"))?"<h3>Why it stopped last time</h3>":"<h3>Latest update</h3>",stdout);ac_work_note(error);}
        if(person&&state&&strcmp(state,"resolved")&&strcmp(state,"superseded")){
            static const char *const acts[][2]={{"done","Done: account is set up"},{"email","Waiting on email or phone"},{"review","Waiting on the provider (review or sales)"},{"later","Not now"}};
            static const char *const release_acts[][2]={{"approve","Approve: put this release live"},{"later","Not now"}};
            int release=!strcmp(source,"release-approval");const char *const (*set)[2]=release?release_acts:acts;size_t count=release?2:4;
            fputs("<div class=\"work-focus-acts\" role=\"group\" aria-label=\"Record where this stands\">",stdout);
            for(size_t a=0;a<count;a++){fputs("<form method=\"post\" action=\"/work-board/act\"><input type=\"hidden\" name=\"context\" value=\"",stdout);html(context);fputs("\"><input type=\"hidden\" name=\"task\" value=\"",stdout);html(task);fputs("\"><input type=\"hidden\" name=\"action\" value=\"",stdout);html(set[a][0]);fputs("\"><button type=\"submit\">",stdout);html(set[a][1]);fputs("</button></form>",stdout);}
            fputs("</div>",stdout);
        }
        sqlite3_finalize(s);s=NULL;
        if(sqlite3_prepare_v2(db,"SELECT state,summary,started_at FROM runs WHERE goal_id=?1 ORDER BY started_at DESC LIMIT 5",-1,&s,NULL)==SQLITE_OK){
            int any=0;sqlite3_bind_text(s,1,task+17,-1,SQLITE_STATIC);
            while(sqlite3_step(s)==SQLITE_ROW){const char *rs=(const char*)sqlite3_column_text(s,0),*sum=(const char*)sqlite3_column_text(s,1);
                if(!any){fputs("<h3>Recent attempts</h3><ol class=\"work-focus-runs\">",stdout);any=1;}
                fputs("<li><span>",stdout);ac_work_when(sqlite3_column_double(s,2));fputs(" · ",stdout);html(rs&&!strcmp(rs,"resolved")?"finished":rs&&!strcmp(rs,"failed")?"failed":rs&&!strcmp(rs,"running")?"running":"waiting");fputs("</span>",stdout);
                if(sum&&*sum)ac_work_note(sum);fputs("</li>",stdout);}
            if(any)fputs("</ol>",stdout);}
        fputs("</section>",stdout);
    }
    sqlite3_finalize(s);sqlite3_close(db);
}
static int ac_work_board(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(target,"/work-board"))return 0;if(strcmp(method,"GET")){aw_error(405,"Use GET to open the account work board.\n");return 1;}
    char context[96],task[128],actor[160],destination[160],back[800];const char *scope=NULL;if(!ac_work_board_query(query,context,task)){aw_error(400,"Choose one account work-board destination and optional exact task.\n");return 1;}
    int access=psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),destination,sizeof(destination),&scope);if(access==0){snprintf(back,sizeof(back),"/work-board?%s",query);fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);url_query_value(back);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}if(access!=1){aw_error(403,"This account cannot open that work board.\n");return 1;}
    psetup_start(200,"Account work board");
    fputs("<style>.account-work-board{margin-top:24px;max-width:1100px}.work-board-hero{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:24px;align-items:end}.work-board-title{display:flex;align-items:center;gap:12px}.work-board-title .ak-icon{width:34px;height:34px}.work-board-nav{display:flex;flex-wrap:wrap;gap:8px}.work-board-nav a{display:inline-flex;align-items:center;gap:7px;border:1px solid currentColor;border-radius:999px;padding:9px 13px;text-decoration:none}.work-board-picker{margin-top:18px}"
          ".work-focus{margin:28px 0 8px;padding:4px 0 18px 20px;border-left:4px solid currentColor;max-width:820px}.work-focus h2{font-size:clamp(1.6rem,3vw,2.3rem);margin:4px 0 6px}.work-focus h3{font-size:.95rem;margin:18px 0 4px}.work-focus-kicker{text-transform:uppercase;letter-spacing:.08em;font-size:.75rem;margin:0}.work-focus-dates{opacity:.75;margin:0;font-size:.88rem}.work-focus-details{white-space:pre-line;line-height:1.55;margin:0}.work-focus-error{margin:0;overflow-wrap:anywhere}.work-note{margin:8px 0;padding:10px 13px;border:1px solid color-mix(in srgb,currentColor 20%,transparent);border-radius:12px}.work-note summary{cursor:pointer;font-weight:600}.work-note-body{white-space:pre-wrap;overflow-wrap:anywhere;margin-top:10px}.work-focus-runs{margin:0;padding-left:18px}.work-focus-runs li{margin:8px 0}.work-focus-runs p{margin:2px 0;opacity:.85;white-space:pre-line}.work-focus-steps{margin:0;padding-left:18px;columns:2 260px;font-size:.9rem}.work-prefill{display:block;margin:12px 0;font-size:.9rem}.work-prefill input{font:inherit;padding:7px 10px;border:1px solid currentColor;border-radius:8px;background:none;color:inherit;min-width:min(320px,100%)}.work-focus-acts{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px}.work-focus-acts form{margin:0}.work-focus-acts button{font:inherit;border:1px solid currentColor;background:none;color:inherit;border-radius:999px;padding:8px 14px;cursor:pointer}.work-focus-acts form:first-child button{background:#173d33;border-color:#173d33;color:#f5f7ee}"
          ""
          ".work-board-group{margin-top:26px}.work-board-group h2{display:flex;align-items:center;gap:8px;font-size:1.15rem;margin:0 0 4px;padding-bottom:6px;border-bottom:2px solid currentColor}.work-board-group h2 .ak-icon{width:18px;height:18px}.work-board-group h2 small{font-weight:400;opacity:.7}.work-board-list{list-style:none;margin:0;padding:0}.work-board-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:4px 18px;padding:11px 0;border-bottom:1px solid color-mix(in srgb,currentColor 18%,transparent)}.work-board-row[data-focus=\"true\"]{background:color-mix(in srgb,currentColor 7%,transparent);padding-inline:10px}.work-board-row a{font-weight:600;text-decoration:none;overflow-wrap:anywhere}.work-board-row a:hover{text-decoration:underline}.work-board-row p{margin:0;font-size:.85rem;opacity:.78;grid-column:1}.work-board-row time{font-size:.8rem;opacity:.7;white-space:nowrap;grid-column:2;grid-row:1}.work-board-empty{opacity:.65;font-size:.9rem;padding:10px 0}.work-board-note{margin:2px 0 6px;font-size:.88rem;opacity:.8}.work-board-more{margin-top:22px;font:inherit;border:1px solid currentColor;background:none;color:inherit;border-radius:999px;padding:9px 16px;cursor:pointer}.work-board-more[hidden]{display:none}"
          ".account-work-board{max-width:1260px}.work-board-hero{grid-template-columns:minmax(0,1fr);padding:clamp(26px,4vw,52px);border-radius:28px;background:radial-gradient(ellipse at 90% 0%,#4b7250,transparent 60%),#173d33;color:#f5f7ee}.work-board-hero .front-arrival-kicker{color:#b5df32}.work-board-hero h1{color:#f5f7ee}.work-board-hero p{max-width:62ch;color:#d5e0cc}.work-board-nav a{border-color:#e6edcb66;color:#f5f7ee;min-height:44px}.work-board-nav a:hover,.work-board-nav a:focus-visible{background:#b5df32;border-color:#b5df32;color:#111913}.work-board-nav .ak-icon{width:17px;height:17px}.account-work-board [data-board-status]{margin:22px 0 10px;font-size:.9rem}.account-work-board [data-board-collection]{margin-top:12px}.account-work-board>.work-board-group{padding:16px 20px;border:1px solid color-mix(in srgb,currentColor 20%,transparent);border-radius:16px}.account-work-board>.work-board-group>summary{cursor:pointer;font-weight:650;min-height:44px;display:flex;align-items:center}.work-focus{background:color-mix(in srgb,currentColor 4%,transparent);border-radius:0 14px 14px 0;padding:18px 20px}.work-inspect-mark{margin-bottom:8px}"
          ".work-inspect-mark{display:inline-grid;place-items:center;width:40px;height:40px;border-radius:12px;background:color-mix(in srgb,currentColor 9%,transparent)}.work-inspect-mark .ak-icon{width:20px;height:20px}.work-board-family{list-style:none;margin:9px 0 15px;padding:0}.work-board-family-head{display:flex;gap:8px;align-items:center;font-size:.86rem;font-weight:650;padding:8px 0 6px;border-bottom:1px solid color-mix(in srgb,currentColor 16%,transparent)}.work-board-family-head .ak-icon{width:17px;height:17px}.work-board-family-head small{font-weight:400;opacity:.7}.work-board-family-list{list-style:none;margin:0;padding:0}.work-board-density{margin-top:12px;padding:8px 12px;border:1px solid currentColor;border-radius:9px;background:none;color:inherit;font:inherit;cursor:pointer}.account-work-board[data-density=compact] .work-board-row{padding-block:4px;gap:1px 10px}.account-work-board[data-density=compact] .work-board-row p{font-size:.78rem}.work-board-stream{list-style:none;margin:5px 0 6px}.work-board-stream details{border:1px solid color-mix(in srgb,currentColor 14%,transparent);border-radius:10px;padding:0 10px}.work-board-stream summary{cursor:pointer;display:flex;align-items:center;justify-content:space-between;gap:10px;min-height:40px;font-size:.84rem;font-weight:600}.work-board-stream summary small{font-size:.75rem;font-weight:400;opacity:.7;white-space:nowrap}.work-board-stream-list{list-style:none;margin:0;padding:0 5px 8px 16px}.account-work-board[data-density=compact] .work-board-stream summary{min-height:30px}@media(max-width:1000px){.work-board-hero{grid-template-columns:1fr}}@media(max-width:620px){.pack-workspace{padding-inline:14px}.work-board-row{grid-template-columns:1fr}.work-board-row time{grid-column:1;grid-row:auto}}</style>",stdout);
    /* The board still lists every task; only the person-waiting ones are
     * grouped as needing the person. Other stopped work is the agents'. */
    AcPersonGoal *people=calloc(AC_PERSON_MAX,sizeof(*people));int person_count=people?ac_person_goals(destination,scope?scope:"",people,AC_PERSON_MAX):-1;
    fputs("<section class=\"account-work-board\" data-work-board data-context=\"",stdout);html(context);fputs("\" data-task=\"",stdout);html(task);fputs("\" data-person-tasks=\"",stdout);
    for(int i=0;i<person_count;i++){if(i)fputs(" ",stdout);html(people[i].id);}if(people){ac_person_free(people,person_count>0?(size_t)person_count:0);free(people);}fputs("\"><header class=\"work-board-hero\"><div><p class=\"front-arrival-kicker\">Account work</p><h1 class=\"work-board-title\">",stdout);page_glyph("board");fputs("<span>Work board</span></h1><p>Everything the assistant, your agents and the site checks are doing for this account: what needs you, what is running, what is waiting and what finished. It is the same list as the activity rail, laid out in full.</p></div><nav class=\"work-board-nav\" aria-label=\"Work continuations\"><a href=\"/apps?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("apps");fputs("Installed work</a><a href=\"/programs?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("play");fputs("Programs</a><a href=\"/work-board/outputs?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("receipt");fputs("Outputs</a><a href=\"/s/models/all?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("apps");fputs("Model Commons</a><a href=\"/board\">",stdout);page_glyph("board");fputs("Public boards</a></nav></header>",stdout);
    ac_work_board_picker(context,task,bearer,auth_db);
    fputs("<button type=\"button\" class=\"work-board-density\" data-board-density-toggle aria-pressed=\"false\">Compact rows</button>",stdout);
    ac_work_focus(context,task,destination,scope?scope:"");
    ac_work_provenance(context,task,destination,scope?scope:"");
    fputs("<p data-board-status role=\"status\" aria-live=\"polite\">Reading this account's work…</p>",stdout);
    fputs("<template data-board-icon=\"Needs you\">",stdout);page_glyph("warning");fputs("</template><template data-board-icon=\"Running\">",stdout);page_glyph("activity");fputs("</template><template data-board-icon=\"Stopped\">",stdout);page_glyph("robot");fputs("</template><template data-board-icon=\"Waiting\">",stdout);page_glyph("clock");fputs("</template><template data-board-icon=\"Finished\">",stdout);page_glyph("check");fputs("</template><div data-board-collection></div><details class=\"work-board-group\"><summary>Browse grouped work</summary><section class=\"work-board-group\" data-board-phase=\"attention\"><h2>",stdout);page_glyph("warning");fputs("Needs you <small></small></h2><p class=\"work-board-note\">Approvals, sign-ins and account steps only you can take. <a href=\"/inbox?context=",stdout);url_query_value(context);fputs("\">Act on them in your inbox</a>.</p><ol class=\"work-board-list\"></ol></section>",stdout);
    fputs("<template data-board-family-icon=\"Connections\">",stdout);page_glyph("link");fputs("</template>",stdout);fputs("<template data-board-family-icon=\"Sources\">",stdout);page_glyph("book");fputs("</template>",stdout);fputs("<template data-board-family-icon=\"Engineering\">",stdout);page_glyph("tool");fputs("</template>",stdout);fputs("<template data-board-family-icon=\"Identity\">",stdout);page_glyph("group");fputs("</template>",stdout);fputs("<template data-board-family-icon=\"Worlds\">",stdout);page_glyph("world");fputs("</template>",stdout);fputs("<template data-board-family-icon=\"Commerce\">",stdout);page_glyph("coins");fputs("</template>",stdout);fputs("<template data-board-family-icon=\"Other\">",stdout);page_glyph("board");fputs("</template>",stdout);
    fputs("<section class=\"work-board-group\" data-board-phase=\"active\"><h2>",stdout);page_glyph("activity");fputs("Running <small></small></h2><ol class=\"work-board-list\"></ol></section>",stdout);
    fputs("<section class=\"work-board-group\" data-board-phase=\"stopped\"><h2>",stdout);page_glyph("robot");fputs("Agent work that stopped <small></small></h2><p class=\"work-board-note\">An agent stopped on these. None of them is waiting on an approval or sign-in from you.</p><ol class=\"work-board-list\"></ol></section>",stdout);
    fputs("<section class=\"work-board-group\" data-board-phase=\"pending\"><h2>",stdout);page_glyph("clock");fputs("Waiting <small></small></h2><ol class=\"work-board-list\"></ol></section>",stdout);
    fputs("<section class=\"work-board-group\" data-board-phase=\"completed\"><h2>",stdout);page_glyph("check");fputs("Finished <small></small></h2><ol class=\"work-board-list\"></ol></section></details><button type=\"button\" class=\"work-board-more\" data-board-more hidden>Show older work</button><noscript><p>The work board needs JavaScript to read the current task list. <a href=\"/apps?context=",stdout);url_query_value(context);fputs("\">Your installed work</a> remains available.</p></noscript></section>",stdout);
    fputs("<script type=\"module\">import{readTasks,workTheme,workStream}from'/apps/aurekai-activity-source.mjs';import{mountCollection}from'/apps/interaction-collection.mjs';"
          "const root=document.querySelector('[data-work-board]'),status=root.querySelector('[data-board-status]'),more=root.querySelector('[data-board-more]'),focusBox=root.querySelector('[data-work-focus]'),context=root.dataset.context,focus=root.dataset.task;"
          "const groups=Object.fromEntries([...root.querySelectorAll('[data-board-phase]')].map(n=>[n.dataset.boardPhase,{box:n,list:n.querySelector('ol'),count:n.querySelector('small'),n:0}]));"
          "{const links=[...root.querySelectorAll('a[data-prefill-param]')];if(links.length){const key='aurekai.signup-email';let saved='';try{saved=localStorage.getItem(key)||''}catch{}const f=document.createElement('label');f.className='work-prefill';f.textContent='Email you sign up with ';const i=document.createElement('input');i.type='email';i.autocomplete='email';i.value=saved;i.placeholder='you@your-company.com';f.append(i);const note=document.createElement('small');note.textContent=' Kept in this browser only. Links marked to fill it will carry it.';f.append(note);links[0].closest('p').before(f);"
          "const apply=()=>{const v=i.value.trim(),ok=/^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$/.test(v);for(const a of links){const u=new URL(a.dataset.prefillBase);if(ok)u.searchParams.set(a.dataset.prefillParam,v);a.href=u.href}};"
          "i.addEventListener('input',()=>{try{localStorage.setItem(key,i.value.trim())}catch{}apply()});apply()}}"
          "const lifetime=new AbortController();addEventListener('pagehide',()=>lifetime.abort(),{once:true});let bound;"
          "const admit=snap=>{if(lifetime.signal.aborted)throw Error('Board closed');const key=JSON.stringify([snap.binding.actor,snap.binding.subject,snap.binding.scope]);if(bound&&bound!==key)throw Error('Account changed');bound=key;return snap};"
          "const words=v=>{const t=String(v||'').replace(/[_.:]+/g,' ').replace(/\\s+--\\s+/g,': ').replace(/\\s+/g,' ').trim();return t?t[0].toUpperCase()+t.slice(1):''};"
          "const plural=(n,one,many)=>n+' '+(n===1?one:many);"
          "const fmt=new Intl.DateTimeFormat(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});"
          "const ago=ms=>{const d=Date.now()-ms;if(!(ms>0))return'';if(d<9e4)return'just now';if(d<36e5)return Math.round(d/6e4)+' min ago';if(d<864e5)return Math.round(d/36e5)+' h ago';if(d<6048e5)return Math.round(d/864e5)+' days ago';return fmt.format(ms)};"
          "const origin=t=>/^account-operator-/.test(t.id)?'Assistant':/^account-fabric-/.test(t.id)?'Agent work':/^account-workgraph-/.test(t.id)?'Pipeline':'Work';"
          "const stateWords={ready:'waiting its turn',blocked:'stopped until someone looks',retry_wait:'will try again',running:'running',complete:'finished',cancelled:'cancelled',dead_letter:'failed',defined:'not started',cancel_requested:'stopping'};"
          "const familyFor=workTheme;"
          "const seen=new Set(),collectionRows=[],labels={attention:'Needs you',active:'Running',stopped:'Stopped',pending:'Waiting',completed:'Finished'};const collection=mountCollection(root.querySelector('[data-board-collection]'),{label:'Work',selection:false,renderRowIcon:r=>root.querySelector('template[data-board-family-icon=\"'+r.family+'\"]')?.content.cloneNode(true),renderInspector:(r,host,{element})=>{const t=r.task,p=element('p',line(t)),icon=root.querySelector('template[data-board-family-icon=\"'+r.family+'\"]');if(icon){const mark=element('span');mark.className='work-inspect-mark';mark.append(icon.content.cloneNode(true));host.append(mark)}host.append(element('p','Work family: '+r.family));host.append(p);if(t.continuation){const dest=element('a','Open receiving workspace');dest.href=t.continuation;host.append(dest)}else host.append(element('p','No receiving workspace declared for this task.'));if(t.usage){const u=t.usage;host.append(element('p',u.reportedRuns?'Tokens: '+u.inputTokens.toLocaleString()+' input · '+u.outputTokens.toLocaleString()+' output ('+u.reportedRuns+' of '+u.runs+' runs)':'Tokens not reported'));if(u.pricedRuns)host.append(element('p','Estimated cost: $'+(u.estimatedMicroUsd/1000000).toFixed(2)+' for '+u.pricedRuns+' of '+u.runs+' runs (not an invoice)'));}const a=element('a','Open full task');a.href='/work-board?context='+encodeURIComponent(context)+'&task='+encodeURIComponent(t.id);host.append(a);}});lifetime.signal.addEventListener('abort',()=>collection.destroy(),{once:true});"
          "const workerWords=ws=>{const m=new Map();for(const w of ws){const k=words(w.workerId)+' '+(w.state==='completed'?'finished':w.state);m.set(k,(m.get(k)||0)+1)}return[...m].map(([k,n])=>n>1?k+' ×'+n:k).join(', ')};"
          "const line=t=>{const bits=[origin(t),t.phase==='active'&&!['running','cancel_requested'].includes(t.state)?'in progress':stateWords[t.state]||words(t.state)];if(t.workers.length)bits.push(workerWords(t.workers));if(t.counts.nodes>1)bits.push(plural(t.counts.nodes,'step','steps'));return bits.join(' · ')};"
          "const familyLists=new Map();const familyList=(g,phase,family)=>{const key=phase+'|'+family;let item=familyLists.get(key);if(!item){const outer=document.createElement('li'),heading=document.createElement('div'),name=document.createElement('span'),count=document.createElement('small'),list=document.createElement('ol');outer.className='work-board-family';heading.className='work-board-family-head';list.className='work-board-family-list';name.textContent=family;const icon=root.querySelector('template[data-board-family-icon=\"'+family+'\"]');if(icon)heading.append(icon.content.cloneNode(true));heading.append(name,count);outer.append(heading,list);g.list.append(outer);item={list,count,n:0};familyLists.set(key,item)}item.count.textContent='('+ ++item.n +')';return item.list};"
          "const streamLists=new Map();const streamList=(parent,phase,family,stream,selected)=>{const key=phase+'|'+family+'|'+stream;let row=streamLists.get(key);if(!row){const li=document.createElement('li'),details=document.createElement('details'),summary=document.createElement('summary'),title=document.createElement('span'),count=document.createElement('small'),list=document.createElement('ol');li.className='work-board-stream';li.dataset.workstream=stream;title.textContent=stream;list.className='work-board-stream-list';summary.append(title,count);details.append(summary,list);li.append(details);parent.append(li);row={list,count,n:0,details};streamLists.set(key,row)}row.count.textContent='('+ ++row.n+' loaded)';if(selected)row.details.open=true;return row.list};"
          "const person=new Set((root.dataset.personTasks||'').split(' ').filter(Boolean));const render=t=>{if(seen.has(t.id))return;seen.add(t.id);const family=familyFor(t),phase=t.phase==='cancelled'?'completed':person.has(t.id)&&t.phase!=='completed'?'attention':t.phase==='attention'?'stopped':t.phase,g=groups[phase]||groups.pending,li=document.createElement('li');li.className='work-board-row';li.dataset.taskId=t.id;li.dataset.family=family;if(t.id===focus)li.dataset.focus='true';"
          "const a=document.createElement('a');a.href='/work-board?context='+encodeURIComponent(context)+'&task='+encodeURIComponent(t.id);const displayTitle=t.phase==='completed'&&/^Approve release activity-public-[A-Za-z0-9-]+$/.test(t.title)?'Release completed: '+t.title.slice(16):t.title;a.textContent=displayTitle;const p=document.createElement('p');p.textContent=line(t);const when=document.createElement('time');if(t.updated>0){when.dateTime=new Date(t.updated).toISOString();when.textContent=ago(t.updated)}"
          "li.append(a,when,p);if(t.continuation){const c=document.createElement('p'),o=document.createElement('a');o.href=t.continuation;o.textContent='Open its workspace';c.append(o);li.append(c)}const familyRows=familyList(g,phase,family),stream=workStream(t);(stream?streamList(familyRows,phase,family,stream,t.id===focus):familyRows).append(li);g.n++;if(t.continuation)continuations++;if(collectionRows.length<1000)collectionRows.push({id:t.id,title:displayTitle,version:t.version,subtitle:family+' · '+line(t),family,category:labels[phase]||'Waiting',status:labels[phase]||'Waiting',at:t.updated,task:t});return li};"
          "const counts=()=>{for(const g of Object.values(groups)){g.count.textContent=g.n?'('+g.n+')':'';const empty=g.list.querySelector('.work-board-empty');if(!g.n&&!empty){const li=document.createElement('li');li.className='work-board-empty';li.textContent='Nothing here right now.';g.list.append(li)}else if(g.n&&empty)empty.remove()}};"
          "const steps=t=>{if(!focusBox||!t||!t.nodes.length)return;const real=t.nodes.filter(n=>n.parentId!==null||t.nodes.length===1);if(!real.length)return;const byId=new Map(t.nodes.map(n=>[n.id,n]));const depth=n=>{let p=n.parentId,d=0;const seen=new Set([n.id]);while(p&&byId.has(p)&&!seen.has(p)&&d<4){seen.add(p);d++;p=byId.get(p).parentId}return Math.max(0,d-1)};const h=document.createElement('h3');h.textContent='Recorded task hierarchy ('+t.nodes.length+' of '+t.counts.nodes+')';const ol=document.createElement('ol');ol.className='work-focus-steps';for(const n of real){const li=document.createElement('li');li.style.paddingInlineStart=(depth(n)*14)+'px';li.textContent=words(n.operatorId||n.family||'step')+' · '+(stateWords[n.state]||words(n.state));const fam=familyFor({title:n.family||n.operatorId||'',nodes:[]}),icon=root.querySelector('template[data-board-family-icon=\"'+fam+'\"]');if(icon)li.prepend(icon.content.cloneNode(true));ol.append(li)}focusBox.append(h,ol)};"
          "let cursor=0,total=0,continuations=0;"
          "const page=async()=>{const snap=admit(await readTasks(context,{cursor,signal:lifetime.signal}));for(const t of snap.tasks)render(t);collection.refresh(collectionRows);total+=snap.tasks.length;cursor=snap.more?snap.cursor:0;more.hidden=!snap.more;counts();status.textContent=total>1000?'Search covers the first 1,000 loaded tasks; grouped work includes all '+total+'.':snap.more?'Showing the '+total+' most recent tasks.':'Showing all '+total+' tasks.';status.textContent+=' · '+continuations+' of '+total+' loaded tasks declare a receiving continuation.'};"
          "more.addEventListener('click',async()=>{more.disabled=true;try{await page()}catch(e){status.textContent='Older work could not be read. Try again.'}more.disabled=false});"
          "const density=root.querySelector('[data-board-density-toggle]');density?.addEventListener('click',()=>{const compact=root.dataset.density!=='compact';root.dataset.density=compact?'compact':'regular';density.setAttribute('aria-pressed',String(compact));density.textContent=compact?'Comfortable rows':'Compact rows'});"
          "const loading=new AbortController();addEventListener('pagehide',()=>loading.abort(),{once:true});import('/apps/aurekai-horde.mjs').then(m=>m.horde(status,loading.signal),()=>{});"
          "try{const selected=focus?readTasks(context,{taskId:focus,signal:lifetime.signal}).then(admit).catch(()=>null):null;await page();loading.abort();for(let more=1;more<4&&cursor;more++)await page();if(selected){const snap=await selected,t=snap&&snap.tasks.find(x=>x.id===focus);if(t){render(t);collection.refresh(collectionRows);collection.inspect(t.id);counts();steps(t);}}}"
          "catch(error){loading.abort();status.textContent='Account work could not be read just now. The activity rail keeps the same list; refresh to try again.';console.error(error)}</script>",stdout);
    psetup_end();return 1;
}
static int ac_feed_ref(const char *ref,char source[128],char key[25]){
    const char *slash=ref?strchr(ref,'/'):NULL;size_t n;if(!slash||strchr(slash+1,'/')||(n=(size_t)(slash-ref))<1||n>=128||strlen(slash+1)!=24)return 0;
    memcpy(source,ref,n);source[n]=0;snprintf(key,25,"%s",slash+1);return strspn(source,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==n&&strspn(key,"0123456789abcdef")==24;
}
static int ac_feed_item(const char *ref,PageItem *item,char family[64],char source_kind[64]){
    char source[128],key[25];sqlite3 *db=NULL,*archive=NULL;sqlite3_stmt *s=NULL;PageMaterial *material=NULL;int found=0;
    if(!ac_feed_ref(ref,source,key)||!(db=cms_open_ro()))goto done;family[0]=source_kind[0]=0;
    if(sqlite3_prepare_v2(db,"SELECT COALESCE(family,''),COALESCE(kind,'') FROM internet_source WHERE namespace='root' AND source=?1 AND status='published' ORDER BY id DESC LIMIT 1",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,source,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_ROW)goto done;snprintf(family,64,"%s",(const char*)sqlite3_column_text(s,0));snprintf(source_kind,64,"%s",(const char*)sqlite3_column_text(s,1));sqlite3_finalize(s);s=NULL;
    if((material=calloc(1,sizeof(*material)))){page_collect_stories(db,source,material);for(int kind=0;kind<BF_PAGE_MATERIAL_COUNT&&!found;kind++)for(int i=0;i<material->stream[kind].count;i++){PageItem *one=&material->stream[kind].item[i];if(!strcmp(one->source,source)&&!strcmp(one->key,key)){*item=*one;found=1;break;}}}
    if(!found&&(archive=page_archive_open(0)))found=page_archive_item(archive,source,key,item,family,64);
 done:sqlite3_finalize(s);if(archive)sqlite3_close(archive);free(material);if(db)sqlite3_close(db);return found;
}
static int ac_feed_query(const char *query,char context[96],char item[160]){
    unsigned seen=0;context[0]=item[0]=0;if(!query||!*query||strlen(query)>1024)return 0;
    for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[16],*out=NULL;size_t cap=0;unsigned bit=0;if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;if(!strcmp(name,"context")){out=context;cap=96;bit=1;}else if(!strcmp(name,"item")){out=item;cap=160;bit=2;}else return 0;if((seen&bit)||!ac_decode(eq+1,(size_t)(end-eq-1),out,cap)||!*out)return 0;seen|=bit;p=*end?end+1:end;}
    char source[128],key[25];return seen==3&&ac_feed_ref(item,source,key);
}
static void ac_feed_picker(const char *context,const char *item,const char *bearer,const char *auth_db){
    PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;int result=psetup_destinations(bearer,auth_db,rows,&count);if(result<0){fputs("<p role=\"alert\">The complete destination list is unavailable.</p>",stdout);return;}
    fputs("<form class=\"feed-use-picker\" method=\"get\" action=\"/feed-use\"><label>Use this item in <select name=\"context\">",stdout);if(!result)fputs("<option value=\"self\">My personal workspace</option>",stdout);for(size_t i=0;i<count;i++){fputs("<option value=\"",stdout);html(rows[i].context);fputs(!strcmp(rows[i].context,context)?"\" selected>":"\">",stdout);html(rows[i].label);fputs("</option>",stdout);}fputs("</select></label>",stdout);psetup_hidden("item",item);fputs(" <button>Change workspace</button></form>",stdout);
}
static int ac_feed_use(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(target,"/feed-use"))return 0;if(strcmp(method,"GET")){aw_error(405,"Use GET to review feed handoffs.\n");return 1;}char context[96],ref[160],actor[160],destination[160],back[1300],family[64],source_kind[64],source[128],key[25];const char *scope=NULL;PageItem item;
    if(!ac_feed_query(query,context,ref)||!ac_feed_ref(ref,source,key)){aw_error(400,"Choose one destination and exact retained feed item.\n");return 1;}int access=psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),destination,sizeof(destination),&scope);if(access==0){snprintf(back,sizeof(back),"/feed-use?%s",query);fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);url_query_value(back);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}if(access!=1){aw_error(403,"This account cannot receive that feed item.\n");return 1;}if(!ac_feed_item(ref,&item,family,source_kind)){aw_error(404,"That retained feed item is unavailable.\n");return 1;}
    psetup_start(200,"Use this feed item");fputs("<style>.feed-use-head{max-width:850px}.feed-use-source{margin:22px 0;padding:18px;border-left:4px solid currentColor}.feed-use-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:16px;margin-top:24px}.feed-use-card{border:1px solid currentColor;border-radius:14px;padding:18px;min-width:0}.feed-use-card h2{display:flex;align-items:center;gap:9px;margin-top:0}.feed-use-card button,.feed-use-card input,.feed-use-card textarea{font:inherit}.feed-use-card input,.feed-use-card textarea{width:100%;box-sizing:border-box;padding:9px;margin:5px 0 10px}.feed-use-card textarea{min-height:150px;resize:vertical}.feed-use-card audio{width:100%;margin-top:10px}.feed-use-result{font-size:.85rem;min-height:1.4em}</style>",stdout);
    fputs("<section data-feed-use data-context=\"",stdout);html(context);fputs("\" data-item=\"",stdout);html(ref);fputs("\" data-source=\"",stdout);html(source);fputs("\" data-key=\"",stdout);html(key);fputs("\" data-family=\"",stdout);html(family);fputs("\" data-kind=\"",stdout);html(source_kind);fputs("\"><header class=\"feed-use-head\"><p class=\"front-arrival-kicker\">One source, several lawful continuations</p><h1>Use this feed item</h1><p>These destinations come from installed app intake declarations. Opening this page writes nothing; each action below is reviewed separately.</p></header>",stdout);
    fputs("<article class=\"feed-use-source\"><h2>",stdout);html(item.title);fputs("</h2><p><code>",stdout);html(family);fputs("</code> · <code>",stdout);html(source_kind);fputs("</code> · source <code>",stdout);html(source);fputs("</code></p>",stdout);if(item.body[0]){fputs("<p>",stdout);html(item.body);fputs("</p>",stdout);}if(public_http_url(item.href)){fputs("<p><a rel=\"nofollow\" href=\"",stdout);html(item.href);fputs("\">Open the publisher item</a></p>",stdout);}fputs("</article>",stdout);ac_feed_picker(context,ref,bearer,auth_db);
    fputs("<span hidden data-feed-title>",stdout);html(item.title);fputs("</span><span hidden data-feed-body>",stdout);html(item.body);fputs("</span><span hidden data-feed-href>",stdout);html(item.href);fputs("</span><div class=\"feed-use-grid\">",stdout);
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int shown=0;if(db&&sqlite3_prepare_v2(db,"SELECT w.binding_id,i.pack,w.family,w.entry,w.release,COALESCE(p.canonical_name,i.pack),b.status='active',COALESCE(json_extract(d.produces,'$.mode'),''),COALESCE(json_extract(d.produces,'$.prefix'),'') FROM _application_workspace w JOIN pack_installation i ON i.id=w.installation_id JOIN application_binding b ON b.id=w.cms_id AND b.binding_id=w.binding_id AND b.namespace=i.namespace JOIN capability_program d ON d.namespace='root' AND d.application_kind=w.family AND d.action='feed-intake' AND d.status='published' AND COALESCE(json_extract(d.capability_requirements,'$.pack'),'')=i.pack LEFT JOIN pack_artifact p ON p.namespace='root' AND p.pack=i.pack WHERE i.installed_for=?1 AND i.status='published' AND b.status<>'withdrawn' AND EXISTS(SELECT 1 FROM json_each(d.consumes) j WHERE j.value=?2 OR j.value=?3 OR j.value=?4) ORDER BY 6,d.program_id LIMIT 64",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,destination,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,family,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,source_kind,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,item.kind==BF_PAGE_MATERIAL_PUBLICATION?"publication":"__none__",-1,SQLITE_STATIC);while(sqlite3_step(s)==SQLITE_ROW){char binding[64],pack[96],wf[64],entry[128],release[65],label[256],mode[64],prefix[128],research[1024]="";if(!bfpg_text(s,0,binding,sizeof(binding))||!bfpg_text(s,1,pack,sizeof(pack))||!bfpg_text(s,2,wf,sizeof(wf))||!bfpg_text(s,3,entry,sizeof(entry))||!bfpg_text(s,4,release,sizeof(release))||!bfpg_text(s,5,label,sizeof(label))||!bfpg_text(s,7,mode,sizeof(mode))||!bfpg_text(s,8,prefix,sizeof(prefix)))break;int active=sqlite3_column_int(s,6);if(!active||!ac_collection_declared(pack,wf,prefix)||!ac_verified(binding,destination,scope,pack,release,entry,actor)||!workspace_package_available(release,entry))continue;if(strcmp(mode,"relationship")&&strcmp(mode,"source-library")&&strcmp(mode,"narration")&&strcmp(mode,"research"))continue;if(!strcmp(mode,"source-library")&&!public_http_url(item.href))continue;if(!strcmp(mode,"research")){char twin[256],digest[65];if(!pr_source(db,source,twin,sizeof(twin))||!pr_item_fingerprint(&item,source,twin,digest))continue;snprintf(research,sizeof(research),"/sources?source=%s&item=%s&snapshot=%s&return=%%2Fo%%2F%s%%2F%s&context=%s&binding=%s",source,key,digest,source,key,context,binding);}
            shown++;fputs("<article class=\"feed-use-card\" data-feed-target data-mode=\"",stdout);html(mode);fputs("\" data-binding=\"",stdout);html(binding);fputs("\" data-prefix=\"",stdout);html(prefix);fputs("\"><h2>",stdout);page_glyph(mode);html(label);fputs("</h2>",stdout);if(!strcmp(mode,"relationship")){fputs("<p>Review the retained source and save an unresolved relationship observation.</p><a data-feed-save href=\"/apps/",stdout);html(binding);fputs("/_relationship-observation?item=",stdout);url_query_value(ref);fputs("\">Review relationship observation</a>",stdout);}else if(!strcmp(mode,"source-library"))fputs("<p>Retain the publisher address in this account's source library with the exact feed coordinate attached.</p><button type=\"button\" data-feed-save>Save source material</button>",stdout);else if(!strcmp(mode,"research")){fputs("<p>Carry this exact retained excerpt into the reviewed research flow. Research rereads this source fingerprint and the selected account workspace before any draft can be saved.</p><p><a data-feed-research href=\"",stdout);html(research);fputs("\">Prepare reviewed research</a></p>",stdout);}else{fputs("<p>Render a private WAV from the retained title and summary. Review the script and voice before rendering; no audio session is created automatically.</p><label>Voice<input data-feed-voice value=\"system\" maxlength=\"80\"></label><label>Rate<input data-feed-rate type=\"number\" min=\"80\" max=\"300\" value=\"180\"></label><label>Script<textarea data-feed-script>",stdout);html(item.title);if(item.body[0]){fputs("\n\n",stdout);html(item.body);}fputs("</textarea></label><button type=\"button\" data-feed-narrate>Render narration</button><audio controls hidden data-feed-audio></audio><a hidden data-feed-download download=\"feed-narration.wav\">Download WAV</a>",stdout);}fputs("<p class=\"feed-use-result\" data-feed-result role=\"status\" aria-live=\"polite\"></p>",stdout);if(!strcmp(mode,"relationship")){fputs("<p><a href=\"/relationships?context=",stdout);url_query_value(context);fputs("\">Open relationship CRM</a></p>",stdout);}else if(!strcmp(mode,"source-library")){fputs("<p><a href=\"/apps/",stdout);html(binding);fputs("/sources/\">Open source library</a></p>",stdout);}else if(!strcmp(mode,"research"))fputs("<p><small>No research record is created from this card alone.</small></p>",stdout);else{fputs("<p><a href=\"/apps/",stdout);html(binding);fputs("/audio/\">Open Audio Studio</a></p>",stdout);}fputs("</article>",stdout);}
    }
    sqlite3_finalize(s);if(db)sqlite3_close(db);fputs("</div>",stdout);if(!shown)fputs("<p class=\"pack-warning\">No installed app currently declares an intake compatible with this item.</p>",stdout);fputs("<noscript><p>Downstream save and narration actions need JavaScript. The source and installed-work links above remain available.</p></noscript></section>",stdout);
    fputs("<script type=\"module\">import{openDestinationStore}from'/apps/workspace-store.mjs';import{digest}from'/apps/workspace-value.mjs';import{encode}from'/apps/workspace-msgpack.mjs';const root=document.querySelector('[data-feed-use]'),enc=new TextEncoder(),context=root.dataset.context,itemRef=root.dataset.item,source=root.dataset.source,key=root.dataset.key,family=root.dataset.family,kind=root.dataset.kind,title=root.querySelector('[data-feed-title]').textContent,body=root.querySelector('[data-feed-body]').textContent,href=root.querySelector('[data-feed-href]').textContent,publicPath='/o/'+source+'/'+key,hash=s=>digest(enc.encode(s)),set=(card,msg)=>card.querySelector('[data-feed-result]').textContent=msg;for(const card of root.querySelectorAll('[data-feed-target]')){const binding=card.dataset.binding,mode=card.dataset.mode,prefix=card.dataset.prefix;if(mode==='research'||mode==='relationship')continue;if(mode==='narration'){card.querySelector('[data-feed-narrate]').addEventListener('click',async()=>{const button=card.querySelector('[data-feed-narrate]'),voice=card.querySelector('[data-feed-voice]').value.trim(),rate=Number(card.querySelector('[data-feed-rate]').value),script=card.querySelector('[data-feed-script]').value.trim();button.disabled=true;set(card,'Rendering reviewed narration…');try{const response=await fetch('/apps/'+binding+'/_narrate',{method:'POST',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{'Content-Type':'application/msgpack'},body:encode([1,script,voice,rate])});if(!response.ok)throw Error((await response.text()).slice(0,400)||'Narration was refused.');if(response.headers.get('content-type')?.split(';')[0]!=='audio/wav')throw Error('Narration returned an unexpected format.');const blob=await response.blob(),url=URL.createObjectURL(blob),audio=card.querySelector('[data-feed-audio]'),download=card.querySelector('[data-feed-download]');if(audio.dataset.url)URL.revokeObjectURL(audio.dataset.url);audio.dataset.url=url;audio.src=url;audio.hidden=false;download.href=url;download.hidden=false;set(card,'WAV rendered from this retained item. Review it before reuse or distribution.')}catch(e){set(card,e.message)}finally{button.disabled=false}});continue}card.querySelector('[data-feed-save]').addEventListener('click',async()=>{const button=card.querySelector('[data-feed-save]');button.disabled=true;set(card,'Checking the destination record…');try{const store=openDestinationStore('/apps/'+binding+'/_records');const address=new URL(href);if(!['http:','https:'].includes(address.protocol)||address.username||address.password)throw Error('This item has no ordinary publisher address to save.');const id=await hash(address.href),recordKey=prefix+id,existing=await store.readVersion(recordKey),provenance={schema:'bonfyre.feed-handoff.v1',itemRef,publicPath,source,family,kind};if(existing){const prior=existing.value;if(existing.deleted||prior?.schema!=='bonfyre.source-material.v1'||prior.kind!=='link'||prior.url!==address.href||prior.id!==id||prior.state!=='active'||!Number.isSafeInteger(existing.revision)||existing.revision<1||prior.feedOrigins&&!Array.isArray(prior.feedOrigins))throw Error('This source changed. Review its current record in the Source Library.');const origins=[prior.provenance,...(prior.feedOrigins||[])].filter(Boolean);if(origins.some(p=>p.itemRef===itemRef)){set(card,'This feed item is already retained in the source library.');return}if(origins.length>=24)throw Error('This source holds 24 feed references. Start a separate source review.');await store.saveLocal(recordKey,{...prior,feedOrigins:[...(prior.feedOrigins||[]),provenance]},existing.revision);set(card,'Saved another feed item to this source.');return}const value={schema:'bonfyre.source-material.v1',id,kind:'link',state:'active',title,collection:'Feed handoffs',tags:[family,source].filter(Boolean),notes:'Saved from a retained Aurekai feed item.',url:address.href,contentSHA256:id,createdAt:new Date().toISOString(),history:[],provenance};await store.saveLocal(recordKey,value,0);set(card,'Saved to the source library with feed provenance.')}catch(e){set(card,e.message)}finally{button.disabled=false}})}</script>",stdout);
    psetup_end();return 1;
}
static int ac_relationship_actor_exists(const char *subject,const char *who,const char *source){
    sqlite3 *db=NULL,*cms=NULL;sqlite3_stmt *s=NULL;int ok=0;
    if(subject&&*subject){
        if(strncmp(subject,"person:",7)&&strncmp(subject,"institution:",12))return 0;
        cms=cms_open_ro();if(!cms)goto done;
        if(sqlite3_prepare_v2(cms,"SELECT 1 FROM site_presentation WHERE namespace='root' AND status='published' AND subject_address=?1 LIMIT 1",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,subject,-1,SQLITE_STATIC);ok=sqlite3_step(s)==SQLITE_ROW;goto done;
    }
    if(!who||!*who||!source||!*source)goto done;
    {const char *path=getenv("BONFYRE_ITEM_ARCHIVE_DB");if(!path||!*path||sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;sqlite3_busy_timeout(db,1000);}
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM item WHERE by=?1 AND source=?2 LIMIT 1",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,who,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,source,-1,SQLITE_STATIC);ok=sqlite3_step(s)==SQLITE_ROW;
 done:sqlite3_finalize(s);if(cms)sqlite3_close(cms);if(db)sqlite3_close(db);return ok;
}
static void ac_relationship_picker(const char *context,const char *subject,const char *who,const char *source,const char *bearer,const char *auth_db){
    PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;int result=psetup_destinations(bearer,auth_db,rows,&count);
    if(result<0){fputs("<p role=\"alert\">The complete destination list is unavailable.</p>",stdout);return;}
    fputs("<form class=\"relationship-destination\" method=\"get\" action=\"/relationships\"><label>Relationship workspace <select name=\"context\">",stdout);
    if(!result)fputs("<option value=\"self\">My personal workspace</option>",stdout);
    for(size_t i=0;i<count;i++){fputs("<option value=\"",stdout);html(rows[i].context);fputs(!strcmp(rows[i].context,context)?"\" selected>":"\">",stdout);html(rows[i].label);fputs("</option>",stdout);}
    fputs("</select></label>",stdout);if(subject&&*subject)psetup_hidden("subject",subject);else if(who&&*who&&source&&*source){psetup_hidden("who",who);psetup_hidden("source",source);}fputs(" <button>Change workspace</button></form>",stdout);
}
static int ro_declared(const char *pack,const char *family,const char *source_family,const char *kind,char prefix[128]);
static void ac_relationship_sources(const char *binding,const char *pack,const char *family,const char *who,const char *source) {
    if(!who||!*who||!source||!*source)return;
    sqlite3 *db=page_archive_open(0);sqlite3_stmt *q=NULL;int shown=0;
    if(db&&sqlite3_prepare_v2(db,"SELECT key FROM item WHERE by=?1 AND source=?2 ORDER BY at DESC,key LIMIT 8",-1,&q,NULL)==SQLITE_OK){
        sqlite3_bind_text(q,1,who,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,source,-1,SQLITE_STATIC);
        while(sqlite3_step(q)==SQLITE_ROW){char ref[160],sf[64],kind[64],prefix[128];PageItem item;const char *key=(const char*)sqlite3_column_text(q,0);
            if(!key||snprintf(ref,sizeof(ref),"%s/%s",source,key)>=(int)sizeof(ref)||!ac_feed_item(ref,&item,sf,kind)||strcmp(item.by,who)||!ro_declared(pack,family,sf,kind,prefix))continue;
            if(!shown++)fputs("<section data-relationship-sources><h4>Review a credited source item</h4><p>Select an excerpt to review before saving this credit as an unresolved observation.</p><ul>",stdout);
            fputs("<li><a data-relationship-review href=\"/apps/",stdout);html(binding);fputs("/_relationship-observation?item=",stdout);url_query_value(ref);fputs("\">",stdout);html(item.title);fputs("</a></li>",stdout);
        }
    }
    if(shown)fputs("</ul></section>",stdout);sqlite3_finalize(q);if(db)sqlite3_close(db);
}
static int ac_relationship(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(target,"/relationships"))return 0;if(strcmp(method,"GET")){aw_error(405,"Use GET to inspect relationship work.\n");return 1;}
    char context[96]="",subject_ref[192]="",who[512]="",source[2048]="";unsigned seen=0;if(!query||!*query||strlen(query)>4096)goto invalid;
    for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[24],*out=NULL;size_t cap=0;unsigned bit=0;if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))goto invalid;
#define AC_REL_ARG(k,f,b) if(!strcmp(name,k)){out=f;cap=sizeof(f);bit=b;}
        AC_REL_ARG("context",context,1)else AC_REL_ARG("subject",subject_ref,2)else AC_REL_ARG("who",who,4)else AC_REL_ARG("source",source,8)
#undef AC_REL_ARG
        if(!out||(seen&bit)||!ac_decode(eq+1,(size_t)(end-eq-1),out,cap))goto invalid;seen|=bit;p=*end?end+1:end;
    }
    if(!(seen&1)||!((seen==1)||((seen&2)&&!(seen&12))||((seen&12)==12&&!(seen&2))))goto invalid;
    if(seen!=1&&!ac_relationship_actor_exists(subject_ref,who,source)){aw_error(404,"That actor coordinate is not retained as a published subject or source-bound observation.\n");return 1;}
    char actor[160],destination[160],back[4300];const char *scope=NULL;int access=psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),destination,sizeof(destination),&scope);
    if(access==0){snprintf(back,sizeof(back),"/relationships?%s",query);fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);url_query_value(back);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}
    if(access!=1){aw_error(403,"This account cannot open that relationship workspace.\n");return 1;}
    psetup_start(200,"Relationship workspace");
    fputs("<div class=\"relationship-surface\" data-relationship-surface=\"crm\">"
          "<header class=\"relationship-hero\"><p class=\"relationship-eyebrow\">Aurekai / Connections</p>"
          "<h1>Relationships</h1><p class=\"relationship-intro\">Review your own source-backed relationships, installed CRM workspaces and account-owned continuations. Unresolved credits remain observations until you review them.</p>"
          "<nav class=\"relationship-actions\" aria-label=\"Relationship navigation\">"
          "<a href=\"https://aurekai.work/people\">Explore people</a>"
          "<a href=\"/organizations\">Organizations</a></nav></header>"
          "<section class=\"relationship-context\" aria-label=\"Relationship context\">",stdout);
    if(*subject_ref){fputs("<p data-relationship-actor>Published subject address <code>",stdout);html(subject_ref);fputs("</code>. This is a subject coordinate, not a claim that a CRM record already exists.</p>",stdout);}else if(*who){fputs("<p data-relationship-actor>Source-bound observation <strong>",stdout);html(who);fputs("</strong> credited by <code>",stdout);html(source);fputs("</code>. This does not assert that equal names identify one person.</p><p><a href=\"/people?who=",stdout);url_query_value(who);fputs("&amp;source=",stdout);url_query_value(source);fputs("\">Open the public observation</a></p>",stdout);}else fputs("<p data-relationship-account>Account-scoped relationship CRM. Feed observations remain unresolved until you review and connect them to a known actor.</p>",stdout);
    fputs("<p>Opening this view does not create a contact, merge identities, or grant access to an external CRM.</p></section>",stdout);
    ac_relationship_picker(context,subject_ref,who,source,bearer,auth_db);
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int shown=0,step=SQLITE_DONE;if(db&&sqlite3_prepare_v2(db,
      "SELECT w.binding_id,i.pack,w.family,w.entry,w.release,COALESCE(p.canonical_name,i.pack),b.status='active','crm',COALESCE(json_extract(d.produces,'$.prefix'),'') FROM _application_workspace w JOIN pack_installation i ON i.id=w.installation_id JOIN application_binding b ON b.id=w.cms_id AND b.binding_id=w.binding_id AND b.namespace=i.namespace JOIN capability_program d ON d.namespace='root' AND d.application_kind=w.family AND d.action='feed-intake' AND d.status='published' AND COALESCE(json_extract(d.capability_requirements,'$.pack'),'')=i.pack AND COALESCE(json_extract(d.produces,'$.mode'),'')='relationship' LEFT JOIN pack_artifact p ON p.namespace='root' AND p.pack=i.pack WHERE i.installed_for=?1 AND i.status='published' AND b.status<>'withdrawn' ORDER BY 6,i.pack LIMIT 33",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,destination,-1,SQLITE_STATIC);while((step=sqlite3_step(s))==SQLITE_ROW){char binding[64],pack[96],family[64],entry[128],release[65],label[256],grammar[64],prefix[128];
            if(!bfpg_text(s,0,binding,sizeof(binding))||!bfpg_text(s,1,pack,sizeof(pack))||!bfpg_text(s,2,family,sizeof(family))||!bfpg_text(s,3,entry,sizeof(entry))||!bfpg_text(s,4,release,sizeof(release))||!bfpg_text(s,5,label,sizeof(label))||!bfpg_text(s,7,grammar,sizeof(grammar))||!bfpg_text(s,8,prefix,sizeof(prefix)))break;
            int ready=sqlite3_column_int(s,6)&&ac_collection_declared(pack,family,prefix)&&ac_verified(binding,destination,scope,pack,release,entry,actor)&&workspace_package_available(release,entry);if(!ready)continue;
            if(!shown++)fputs("<section class=\"relationship-bindings\"><h2>Installed relationship workspaces</h2><div class=\"pack-cards\">",stdout);
            fputs("<article class=\"pack-card\" data-relationship-workspace data-binding=\"",stdout);html(binding);fputs("\" data-prefix=\"",stdout);html(prefix);fputs("\" data-who=\"",stdout);html(who);fputs("\" data-source=\"",stdout);html(source);fputs("\"><h3>",stdout);html(label);fputs("</h3><p class=\"relationship-workspace-intro\">Your installed workspace for reviewing source-linked observations.</p><p><a data-relationship-collection href=\"/apps/",stdout);html(binding);fputs("/_work?prefix=",stdout);url_query_value(prefix);fputs("\">Open relationship collection</a></p><div data-relationship-records><p>Loading relationship observations…</p></div>",stdout);
            ac_relationship_sources(binding,pack,family,who,source);
            if(ac_word(grammar)){fputs("<p><a data-relationship-grammar href=\"/s/apps/",stdout);html(grammar);fputs("\">",stdout);html(grammar);fputs(" interaction grammar</a> <small>Compiled semantics, not this account's CRM instance.</small></p>",stdout);}fputs("</article>",stdout);
        }
    }
    sqlite3_finalize(s);if(db)sqlite3_close(db);if(shown){fputs("</div></section><script type=\"module\">import{openDestinationStore}from'/apps/workspace-store.mjs';import{readWorkspacePacket}from'/apps/workspace-destinations.mjs';let finance='';try{const context=new URL(location.href).searchParams.get('context'),scope=context==='self'?'self':context?.split(':')[0];if(context&&scope){const packet=await readWorkspacePacket('/apps/_bindings?'+new URLSearchParams({context,pack:'experience.finance-workbooks.workspace'}));if(Array.isArray(packet)&&packet.length===4&&packet[0]===1&&packet[2]===scope&&Array.isArray(packet[3])){const row=packet[3].find(r=>Array.isArray(r)&&/^ws-[a-f0-9]{40}$/.test(r[0])&&r[1]==='experience.finance-workbooks.workspace'&&r[5]===true);if(row){const manifest=await readWorkspacePacket('/apps/'+row[0]+'/CONTINUATIONS.tsv',{type:'text/plain',limit:32768});if(manifest.includes('crm-finance')&&manifest.includes('finance/crm/')&&manifest.includes('crm-finance:'))finance=row[0]}}}}catch{}const lifetime=new AbortController();addEventListener('pagehide',()=>{lifetime.abort();for(const out of document.querySelectorAll('[data-relationship-records]'))out.replaceChildren()},{once:true});const esc=v=>String(v==null?'':v);for(const root of document.querySelectorAll('[data-relationship-workspace]')){const out=root.querySelector('[data-relationship-records]');try{const store=openDestinationStore('/apps/'+root.dataset.binding+'/_records',{signal:lifetime.signal}),rows=[];let cursor='',epoch=-1,more=true;for(let n=0;n<80&&more;n++){const page=await store.readMetadata(root.dataset.prefix,{cursor,epoch});rows.push(...page.rows);cursor=page.cursor;epoch=page.epoch;more=page.more}if(more)throw Error('Collection exceeds this view');out.replaceChildren();if(!rows.length){const p=document.createElement('p');p.textContent='No saved relationship observations yet.';out.append(p);continue}const list=document.createElement('ul');for(const row of rows){const v=row.value?.state?.relationship;if(v?.schema!=='bonfyre.relationship.v1'||root.dataset.who&&(v.source?.authorName!==root.dataset.who||v.source?.source!==root.dataset.source))continue;const li=document.createElement('li'),strong=document.createElement('strong'),meta=document.createElement('small');strong.textContent=esc(v.label)||'Untitled relationship';meta.textContent=[esc(v.opportunity?.kind),esc(v.opportunity?.stage),v.actor?.state==='unresolved'?'identity unresolved':esc(v.actor?.subject)].filter(Boolean).join(' · ');li.append(strong,document.createElement('br'),meta);if(v.source?.publicPath){const a=document.createElement('a');a.href=v.source.publicPath;a.textContent='Open source item';li.append(document.createElement('br'),a)}if(finance&&/^opportunity:(?:feed|actor):[a-f0-9]{64}$/.test(row.key)&&Number.isSafeInteger(row.revision)&&row.revision>0){const link=document.createElement('a');link.dataset.crmFinance='';link.textContent='Prepare Finance review';link.href='/apps/'+finance+'/finance/crm/?'+new URLSearchParams({crm:root.dataset.binding,record:row.key,revision:String(row.revision)});li.append(document.createElement('br'),link)}list.append(li)}if(list.childElementCount)out.append(list);else{const p=document.createElement('p');p.textContent='No supported relationship observations in this collection.';out.append(p)}}catch(error){out.textContent='Relationship records could not be read. Open the collection to retry.'}}</script>",stdout);}else{fputs("<section><h2>No relationship workspace is installed here</h2><p><a href=\"/packs/setup?context=",stdout);url_query_value(context);fputs("\">Review compatible tools for this destination</a></p></section>",stdout);}fputs("</div>",stdout);psetup_end();return 1;
 invalid:aw_error(400,"Choose one destination, optionally with one published subject or one exact name/source observation.\n");return 1;
}
static int ac_work(const char *relative,const char *query,const char *id,const char *subject,const char *scope,const char *pack,const char *family,const char *release,const char *entry,const char *actor,const char *context) {
    int opening=!strcmp(relative,"_open");if(!opening && strcmp(relative,"_work"))return 0;
    char key[512]="",prefix[128]="",view[64]="",cursor[512]="",epoch[24]="-1",revision[24]="",pinned[65]="",digest[65]="";unsigned seen=0;
    if(query && strlen(query)>4096){aw_error(400,"The continuation address is too long.\n");return 1;}
    for(const char *p=query?query:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[24];char *out=NULL;size_t cap=0;unsigned bit=0;
        if(!eq || !psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))goto invalid;
#define AC_ARG(k,f,b) if(!strcmp(name,k)){out=f;cap=sizeof(f);bit=b;}
        AC_ARG("key",key,1)else AC_ARG("view",view,2)else AC_ARG("cursor",cursor,4)else AC_ARG("epoch",epoch,8)else AC_ARG("revision",revision,16)else AC_ARG("release",pinned,32)else AC_ARG("digest",digest,64)else AC_ARG("prefix",prefix,128)
#undef AC_ARG
        if(!out || seen&bit || !ac_decode(eq+1,(size_t)(end-eq-1),out,cap))goto invalid;seen|=bit;p=*end?end+1:end;
    }
    if(opening){if((seen&128) || ((seen!=19 && seen!=115) || !*key || !ac_word(view) || !*revision || strlen(revision)>10 || strspn(revision,"0123456789")!=strlen(revision) || strtoll(revision,NULL,10)<1 || strtoll(revision,NULL,10)>=INT_MAX))goto invalid;}else{unsigned page_seen=seen&~128u;if(page_seen && (page_seen!=12 || !*epoch || strlen(epoch)>10 || strspn(epoch,"0123456789")!=strlen(epoch) || strtoll(epoch,NULL,10)>=INT_MAX))goto invalid;if((seen&128) && (!*prefix || !ac_collection_declared(pack,family,prefix)))goto invalid;}
    if(seen==115 && (!aw_hex(pinned,64) || !aw_hex(digest,64)))goto invalid;
    AcView views[AC_VIEWS];size_t count=0;if(!ac_profiles(release,views,&count)){aw_error(409,"This release has no verified saved-work views. Open its editor from Your work.\n");return 1;}
    if(!opening&&*prefix&&!ac_collection_has_view(views,count,prefix)){
        if(!ac_verified(id,subject,scope,pack,release,entry,actor)||!workspace_package_available(release,entry)){aw_error(409,"Reopen this collection from the current workspace.\n");return 1;}
        psetup_start(200,"Supporting records");size_t nav_bytes=0;unsigned char *nav=ac_navigation(id,context,pack,&nav_bytes);if(nav){fwrite(nav,1,nav_bytes,stdout);free(nav);}
        char label[160];ac_collection_label(prefix,label,sizeof(label));fputs("<section data-work-supporting><h1>",stdout);html(label);fputs("</h1><p>Record family <code>",stdout);html(prefix);fputs("</code></p><p>These records support the workspace's tools. Open the owning editor to work with them.</p><p>",stdout);ac_collection_owner(id,entry);fputs("</p></section>",stdout);psetup_end();return 1;
    }
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const args[]={(char *)bin,"pack-workspace","--operation",opening?"get":"read","--binding",(char *)id,"--subject",(char *)subject,"--scope",(char *)scope,"--actor",(char *)actor,"--prefix",opening?key:prefix,"--cursor",cursor,"--epoch",epoch,"--db",(char *)db,NULL};
    unsigned char *body=NULL;size_t bytes=0;AcPage page;int rc=aw_native(args,NULL,0,&body,&bytes);
    if(rc || !ac_page(body,bytes,&page)){free(body);aw_error(rc==4?409:503,"The saved-work view changed or could not be read. Refresh Your work; no record was changed.\n");return 1;}
    if(opening){
        char url[AC_URL];const AcView *selected=NULL;for(size_t n=0;n<count;n++)if(!strcmp(views[n].id,view))selected=&views[n];
        if(!selected || page.more || page.count!=1 || strcmp(page.rows[0].key,key) || !page.rows[0].body){free(body);aw_error(404,"The selected work is unavailable in this view.\n");return 1;}
        if(page.rows[0].revision!=strtoll(revision,NULL,10)){free(body);aw_error(409,"This work has a newer saved version. Refresh Saved work before continuing.\n");return 1;}
        if(seen==115){char actual[65];bf_sha256_hex(page.rows[0].body,page.rows[0].bytes,actual);if(strcmp(actual,digest)){free(body);aw_error(409,"The selected saved content changed. Refresh before continuing.\n");return 1;}}
        if(!ac_destination(selected,&page.rows[0],id,url,sizeof(url))){free(body);aw_error(409,"This saved record does not satisfy the editor's continuation contract.\n");return 1;}
        if(seen==115 && strcmp(pinned,release)){
            /* Preserve the reviewed record identity. A changed editor is not
             * permission to open another revision, bypass access or migrate it. */
            psetup_start(409,"Continue your saved work");
            size_t nav_bytes=0;unsigned char *nav=ac_navigation(id,context,pack,&nav_bytes);
            if(nav){fwrite(nav,1,nav_bytes,stdout);free(nav);}
            char title[1024]="";ac_value(&page.rows[0],selected->title,title,sizeof(title));
            fputs("<section data-work-release-review><h1>This editor has been updated</h1><p>Your saved work is unchanged. The link you followed names an earlier editor version.</p><h2>",stdout);
            html(*title?title:selected->label);printf("</h2><p>Saved version %lld. Open this same record in the current editor.</p><p><a data-work-current href=\"",page.rows[0].revision);
            ac_work_link(id,selected,&page.rows[0]);printf("&amp;release=%s&amp;digest=%s",release,digest);
            fputs("\">Open your saved work</a></p><p><a data-work-saved href=\"/apps/",stdout);html(id);
            fputs("/_work\">See all saved work</a></p><p>Nothing was copied, changed or reinstalled by opening this page.</p></section>",stdout);
            psetup_end();free(body);return 1;
        }
        printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nCache-Control: private, no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",url);free(body);return 1;
    }
    psetup_start(200,"Saved work");size_t nav_bytes=0;unsigned char *nav=ac_navigation(id,context,pack,&nav_bytes);if(nav){fwrite(nav,1,nav_bytes,stdout);free(nav);}
    fputs("<h1>Continue your saved work</h1><p>Open the saved item in its original editor.</p>",stdout);if(*prefix){char label[160];ac_collection_label(prefix,label,sizeof(label));fputs("<p data-work-collection-scope>Collection: <strong>",stdout);html(label);fputs("</strong> <code>",stdout);html(prefix);fputs("</code></p>",stdout);}fputs("<div class=\"pack-cards\">",stdout);
    size_t shown=0,unmapped=0;
    for(size_t n=0;n<page.count;n++){size_t matched=0;for(size_t j=0;j<count;j++){
        char url[AC_URL],title[1024];if(!ac_destination(&views[j],&page.rows[n],id,url,sizeof(url)))continue;
        if(!ac_value(&page.rows[n],views[j].title,title,sizeof(title)) || !*title)snprintf(title,sizeof(title),"%s",views[j].label);
        matched++;shown++;fputs("<article class=\"pack-panel\" data-saved-work><p>",stdout);html(views[j].label);fputs("</p><h2>",stdout);html(title);printf("</h2><p>Saved version %lld</p><a data-work-continue href=\"",page.rows[n].revision);ac_work_link(id,&views[j],&page.rows[n]);fputs("\">Continue editing</a>",stdout);
        fputs("<p><a data-work-views href=\"/apps/",stdout);html(id);fputs("/_views?key=",stdout);url_query_value(page.rows[n].key);printf("&amp;revision=%lld&amp;release=%s",page.rows[n].revision,release);fputs("\">Related views</a></p></article>",stdout);
    }if(!matched)unmapped++;}
    fputs("</div>",stdout);if(!shown)fputs("<p>No saved items to open on this page.</p>",stdout);
    if(unmapped)fputs("<p>Supporting records stay in their owning editors; this view does not turn them into separate projects.</p>",stdout);
    if(page.more){fputs("<p><a data-work-next href=\"/apps/",stdout);html(id);fputs("/_work?",stdout);if(*prefix){fputs("prefix=",stdout);url_query_value(prefix);fputs("&amp;",stdout);}fputs("cursor=",stdout);url_query_value(page.cursor);printf("&amp;epoch=%lld\">More saved work</a></p>",page.epoch);}
    psetup_end();free(body);return 1;
 invalid:aw_error(400,"Invalid or duplicated continuation coordinates.\n");return 1;
}
/* A retained editor release already declares its real working views. Expose
 * only current, package-verified views after destination admission; this is
 * a navigation projection, not another installation or entitlement owner. */
static void ac_installed_views(const char *binding,const char *release){
    AcView views[AC_VIEWS];size_t count=0;
    if(!ac_profiles(release,views,&count))return;
    fputs("<p><a data-work-saved href=\"/apps/",stdout);html(binding);fputs("/_work\">Continue saved work</a></p>",stdout);
    if(count<2)return;
    fputs("<details class=\"pack-subtools\"><summary>Explore ",stdout);printf("%zu",count);
    fputs(" working tools <span aria-hidden=\"true\">↘</span></summary><nav class=\"pack-subtool-list\" aria-label=\"Editors in this installation\">",stdout);
    for(size_t i=0;i<count;i++){
        fputs("<a data-instrument-entry href=\"/apps/",stdout);html(binding);fputs("/",stdout);html(views[i].entry);
        fputs("\"><span>",stdout);html(views[i].label);fputs("</span><span aria-hidden=\"true\">↗</span></a>",stdout);
    }
    fputs("</nav></details>",stdout);
}
static int ac_index(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db) {
    if(!strcmp(target,"/apps/_destinations")){
        if(strcmp(method,"GET")){aw_error(405,"Use GET to read current destinations.\n");return 1;}
        if(query&&*query){aw_error(400,"Destination discovery uses the signed-in account without caller-selected coordinates.\n");return 1;}
        PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;int access=psetup_destinations(bearer,auth_db,rows,&count);
        if(access!=1){aw_error(access==0?403:503,access==0?"Sign in to read current destinations.\n":"The complete destination list is unavailable or exceeds its bound.\n");return 1;}
        bf_mp_writer_t out;bf_mp_writer_init(&out,4096);
        int ok=!bf_mp_pack_array(&out,2)&&!bf_mp_pack_uint(&out,1)&&!bf_mp_pack_array(&out,(uint32_t)count);
        for(size_t i=0;i<count&&ok;i++)ok=!bf_mp_pack_array(&out,3)&&!bf_mp_pack_str_cstr(&out,rows[i].context)&&!bf_mp_pack_str_cstr(&out,rows[i].label)&&!bf_mp_pack_bool(&out,rows[i].can_write);
        if(ok&&out.len<=AW_PACKET)aw_response(200,"application/msgpack",out.buf,out.len,0);else aw_error(503,"The destination projection is incomplete.\n");
        bf_mp_writer_free(&out);return 1;
    }
    int machine=!strcmp(target,"/apps/_bindings");
    int cards=!strcmp(target,"/apps/_cards");
    int production=!strcmp(target,"/cms/productions");
    if(!machine && !cards && !production && strcmp(target,"/apps") && strcmp(target,"/apps/"))return 0;
    if(strcmp(method,"GET")){aw_error(405,"Use GET to open Your work.\n");return 1;}
    PackSetupInput in;char actor[160],subject[160];const char *scope=NULL;
    char design[208]="",setup_query[4097];
    if(production){if(!ac_design_query(query,setup_query,sizeof(setup_query),design,sizeof(design))){aw_error(400,"Choose one exact published design.\n");return 1;}query=setup_query;}
    if(!psetup_input(query,&in) || *in.operation || *in.digest || *in.revision || *in.shared || *in.repr || *in.version){aw_error(400,"Choose a destination for Your work.\n");return 1;}
    int access=psetup_context(bearer,auth_db,in.context,0,actor,sizeof(actor),subject,sizeof(subject),&scope);
    if(access==0){if(machine){aw_error(403,"Sign in to read this destination.\n");return 1;}fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);char back[768];snprintf(back,sizeof(back),"%s?context=%s%s%s",production?"/cms/productions":"/apps",in.context,*design?"&design=":"",design);url_query_value(back);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}
    if(access!=1){aw_error(403,"This account cannot open that destination.\n");return 1;}
    typedef struct {char id[64],pack[96],family[64],entry[128],release[65],label[256];int active;} AcBinding;
    AcBinding rows[65];size_t count=0;sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int ok=0,step=SQLITE_DONE;
    if(!db){aw_error(503,"Your workspace owner is unavailable.\n");return 1;}
    if(sqlite3_prepare_v2(db,"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_application_workspace'",-1,&s,NULL)==SQLITE_OK && sqlite3_step(s)==SQLITE_ROW && sqlite3_column_int(s,0)==0)ok=1;
    sqlite3_finalize(s);s=NULL;
    const char *sql="SELECT w.binding_id,i.pack,w.entry,w.release,COALESCE(p.canonical_name,i.pack),b.status='active',w.family FROM _application_workspace w JOIN pack_installation i ON i.id=w.installation_id JOIN application_binding b ON b.id=w.cms_id AND b.binding_id=w.binding_id AND b.namespace=i.namespace LEFT JOIN pack_artifact p ON p.pack=i.pack AND p.namespace='root' WHERE i.installed_for=?1 AND i.status='published' AND b.status<>'withdrawn' AND (?2='' OR i.pack=?2) ORDER BY 5,i.pack,w.entry LIMIT 65";
    if(!ok && sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,in.pack,-1,SQLITE_STATIC);
        while((step=sqlite3_step(s))==SQLITE_ROW && count<65){AcBinding *r=&rows[count];
            if(!bfpg_text(s,0,r->id,sizeof(r->id)) || !bfpg_text(s,1,r->pack,sizeof(r->pack)) || !bfpg_text(s,2,r->entry,sizeof(r->entry)) || !bfpg_text(s,3,r->release,sizeof(r->release)) || !bfpg_text(s,4,r->label,sizeof(r->label)) || !bfpg_text(s,6,r->family,sizeof(r->family)))break;
            r->active=sqlite3_column_int(s,5);count++;
        }
        ok=step==SQLITE_DONE && count<65;
    }
    sqlite3_finalize(s);sqlite3_close(db);
    if(!ok){aw_error(503,"The installed-workspace view could not be read completely. Narrow it to one configuration.\n");return 1;}
    if(production){
        char routes[65][768];size_t choices=0;
        for(size_t n=0;n<count;n++){
            AcBinding *r=&rows[n];AcView views[AC_VIEWS];size_t size=0;
            if(strlen(r->id)!=43||strncmp(r->id,"ws-",3)||!aw_hex(r->id+3,40)||!aw_path(r->entry)||!r->active||
               !ac_verified(r->id,subject,scope,r->pack,r->release,r->entry,actor)||!ac_profiles(r->release,views,&size))continue;
            for(size_t v=0;v<size;v++)if(!strcmp(views[v].id,"production")){
                if(choices>=65||snprintf(routes[choices],sizeof(routes[choices]),"/apps/%s/%s%s%s",r->id,views[v].entry,*design?"?design=":"",design)>=(int)sizeof(routes[choices])){aw_error(503,"The production entry cannot be represented safely.\n");return 1;}
                choices++;
            }
        }
        if(choices==1){printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",routes[0]);return 1;}
        psetup_start(200,"Production studio");
        fputs("<h1>Make the work, not another form.</h1><p>Arrange working instruments, bring in your sources, and prepare the next piece of work. The layout, material and receiving editors stay connected.</p>",stdout);
        psetup_destination_picker_with(&in,bearer,auth_db,"/cms/productions","Production belonging to",*design?"design":NULL,design);
        if(choices){fputs("<section><h2>Choose an installed production editor</h2>",stdout);for(size_t n=0;n<choices;n++){fputs("<p><a data-production-open href=\"",stdout);html(routes[n]);fputs("\">Open production studio</a></p>",stdout);}fputs("</section>",stdout);}
        else{
            fputs("<section><h2>Prepare this destination</h2><p>Research reading, a product story map, observed process traces and SQLite rehearsal can be arranged in one editable canvas. Sources remain separate records; reviewed saves keep their exact revisions together.</p><p><a data-production-setup href=\"/packs/setup?pack=experience.connected-instruments.workspace&amp;context=",stdout);
            url_query_value(in.context);if(*design){fputs("&amp;design=",stdout);url_query_value(design);}
            fputs("\">Review and enable Production studio</a></p><p>Enabling an editor does not grant organization membership, publish your source material or act against an external account.</p></section>",stdout);
        }
        if(*design){fputs("<p>After enabling the editor, return here to open this exact design. <a data-design-resume href=\"/cms/productions?context=",stdout);html(in.context);fputs("&amp;design=",stdout);html(design);fputs("\">Continue the selected design →</a></p>",stdout);}
        fputs("<p><a href=\"/cms/\">Content records and page administration</a> · <a href=\"/apps?context=",stdout);url_query_value(in.context);fputs("\">All installed work</a></p>",stdout);
        psetup_end();return 1;
    }
    if(cards){
        fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",stdout);
        fputs("<section class=\"pack-workspace account-tool-shelf\" data-account-tools><p>Installed for this destination</p><h2>Your tools</h2><p>Open the editors already enabled here, or continue their saved work without returning to setup.</p>",stdout);
        pack_presentation_style();pack_presentation_search();sqlite3 *presentation=cms_open_ro();fputs("<div class=\"pack-cards\">",stdout);
        for(size_t n=0;n<count;n++){AcBinding *r=&rows[n];if(strlen(r->id)!=43||strncmp(r->id,"ws-",3)||!aw_hex(r->id+3,40)||!aw_path(r->entry))continue;
            int ready=r->active&&ac_verified(r->id,subject,scope,r->pack,r->release,r->entry,actor)&&workspace_package_available(r->release,r->entry);
            pack_presentation_open(presentation,r->pack,r->label,NULL,ready?"Ready to open":"Review configuration",1);
            if(ready){fputs("<p><a data-work-editor href=\"/apps/",stdout);html(r->id);fputs("/",stdout);html(r->entry);fputs("\">Open editor</a></p>",stdout);
                ac_installed_views(r->id,r->release);ac_installed_collections(r->id,r->pack,r->family,r->release,r->entry);}
            else fputs("<p>Review configuration before opening this editor. Saved work has not been changed.</p>",stdout);
            fputs("<a href=\"/packs/setup?pack=",stdout);url_query_value(r->pack);fputs("&amp;context=",stdout);url_query_value(in.context);fputs("\">Configuration</a>",stdout);pack_presentation_close();
        }
        if(presentation)sqlite3_close(presentation);fputs("</div>",stdout);if(!count)fputs("<p>No editors are enabled here yet.</p>",stdout);fputs("</section>",stdout);return 1;
    }
    if(machine){
        /* Same admitted destination and installed bindings as the human index.
         * This projection is private and grants no CMS or execution authority. */
        bf_mp_writer_t out;bf_mp_writer_init(&out,4096);
        ok=!bf_mp_pack_array(&out,4) && !bf_mp_pack_uint(&out,1) && !bf_mp_pack_str_cstr(&out,subject) && !bf_mp_pack_str_cstr(&out,scope) && !bf_mp_pack_array(&out,(uint32_t)count);
        for(size_t n=0;n<count && ok;n++){
            AcBinding *r=&rows[n];
            if(strlen(r->id)!=43 || strncmp(r->id,"ws-",3) || !aw_hex(r->id+3,40) || !aw_path(r->entry)){ok=0;break;}
            int ready=r->active && ac_verified(r->id,subject,scope,r->pack,r->release,r->entry,actor) && workspace_package_available(r->release,r->entry);
            const char *fields[]={r->id,r->pack,r->entry,r->release,r->label};ok=!bf_mp_pack_array(&out,6);
            for(size_t k=0;k<5 && ok;k++)ok=!bf_mp_pack_str_cstr(&out,fields[k]);
            if(ok)ok=!bf_mp_pack_bool(&out,ready);
        }
        if(ok && out.len<=AW_PACKET)aw_response(200,"application/msgpack",out.buf,out.len,0);else aw_error(503,"The installed-workspace projection is incomplete.\n");
        bf_mp_writer_free(&out);return 1;
    }
    psetup_start(200,"Your work");fputs("<h1>Your work</h1><p>Continue in the tools enabled for this destination.</p>",stdout);psetup_destination_picker(&in,bearer,auth_db,"/apps","Work belonging to");
    fputs("<p><a data-programs-open href=\"/programs?context=",stdout);url_query_value(in.context);fputs("\">Continue a program or prepare an institutional plan</a></p>",stdout);
    pack_presentation_search();sqlite3 *presentation=cms_open_ro();
    fputs("<div class=\"pack-cards\">",stdout);
    for(size_t n=0;n<count;n++){AcBinding *r=&rows[n];if(strlen(r->id)!=43 || strncmp(r->id,"ws-",3) || !aw_hex(r->id+3,40) || !aw_path(r->entry))continue;
        int ready=r->active && ac_verified(r->id,subject,scope,r->pack,r->release,r->entry,actor) && workspace_package_available(r->release,r->entry);
        pack_presentation_open(presentation,r->pack,r->label,NULL,ready?"Ready to open":"Review configuration",1);
        if(ready){fputs("<p><a data-work-editor href=\"/apps/",stdout);html(r->id);fputs("/",stdout);html(r->entry);fputs("\">Open editor</a></p>",stdout);
            ac_installed_views(r->id,r->release);ac_installed_collections(r->id,r->pack,r->family,r->release,r->entry);}
        else fputs("<p>Review configuration before opening this editor. Saved work has not been changed.</p>",stdout);
        fputs("<a href=\"/packs/setup?pack=",stdout);url_query_value(r->pack);fputs("&amp;context=",stdout);url_query_value(in.context);fputs("\">Configuration</a>",stdout);pack_presentation_close();
    }
    if(presentation)sqlite3_close(presentation);
    fputs("</div>",stdout);if(!count)fputs("<p>No editors are enabled here yet.</p>",stdout);
    fputs("<p><a data-work-setup href=\"/packs/setup?context=",stdout);url_query_value(in.context);fputs("\">Choose tools for this destination</a></p>",stdout);psetup_end();return 1;
}
#endif
