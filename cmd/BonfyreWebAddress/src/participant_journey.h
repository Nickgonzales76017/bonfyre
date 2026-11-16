#ifndef BONFYRE_PARTICIPANT_JOURNEY_H
#define BONFYRE_PARTICIPANT_JOURNEY_H
#include "ui_tree.h"
#include "form_tree.h"
/* HTML and HTTP adapters around the existing native participation relations.
 * No client-side account database and no implicit external participation. */
#include <strings.h>
static sqlite3 *cms_open_rw(void);
typedef struct {const char *name;char *out;size_t cap;} PjField;
static int pj_hex(unsigned char c){return c>='0'&&c<='9'?c-'0':c>='a'&&c<='f'?c-'a'+10:c>='A'&&c<='F'?c-'A'+10:-1;}
static int pj_decode(const char *restrict s,size_t len,char *restrict out,size_t cap){
    size_t n=0;
    for(size_t i=0;i<len;i++){
        unsigned char c=(unsigned char)s[i];
        if(c=='%'){if(i+2>=len||pj_hex(s[i+1])<0||pj_hex(s[i+2])<0)return 0;c=(unsigned char)(pj_hex(s[i+1])*16+pj_hex(s[i+2]));i+=2;}
        else if(c=='+')c=' ';
        if(c<32||c==127||n+1>=cap)return 0;
        out[n++]=(char)c;
    }
    out[n]=0;return 1;
}
static int pj_fields(const char *s,PjField *f,size_t count,size_t limit){
    unsigned seen=0;if(!s||!*s)return 1;if(count>16||strlen(s)>limit)return 0;
    for(const char *p=s;*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);
        const char *equal=memchr(p,'=',(size_t)(end-p));char name[24];
        if(!equal||!pj_decode(p,(size_t)(equal-p),name,sizeof(name)))return 0;
        size_t i;for(i=0;i<count&&strcmp(name,f[i].name);i++){}
        if(i==count||(seen&(1u<<i))||!pj_decode(equal+1,(size_t)(end-equal-1),f[i].out,f[i].cap))return 0;
        seen|=1u<<i;p=*end?end+1:end;if(!*p&&*end)return 0;
    }
    return 1;
}
static int pj_encode(const char *s,char *out,size_t cap){
    static const char digits[]="0123456789ABCDEF";size_t n=0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++){
        int plain=(*p>='A'&&*p<='Z')||(*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||strchr("-._~",*p);
        size_t take=plain?1:3;if(n+take>=cap)return 0;
        if(plain)out[n++]=(char)*p;else{out[n++]='%';out[n++]=digits[*p>>4];out[n++]=digits[*p&15];}
    }
    out[n]=0;return 1;
}
/* Preserve the exact selected profile section through the existing SSO
 * handoff. This validates a destination, not an effect to replay after login. */
static int pj_profile_login_redirect(const char *query){
    char tab[24]="overview",site[128]="",q[160]="",cursor[24]="",item[24]="",kind[16]="",view[20]="discover",peer[192]="",facet[32]="";
    PjField fields[]={{"tab",tab,sizeof(tab)},{"site",site,sizeof(site)},
      {"q",q,sizeof(q)},{"cursor",cursor,sizeof(cursor)},{"item",item,sizeof(item)},{"kind",kind,sizeof(kind)},{"view",view,sizeof(view)},{"peer",peer,sizeof(peer)},{"facet",facet,sizeof(facet)}};
    if(!pj_fields(query,fields,9,2048))return 0;
    const char *allowed[]={"overview","posts","saved","about","sites","edit","compose","activity","communities","dating","connections"};
    int found=0;for(size_t i=0;i<sizeof(allowed)/sizeof(allowed[0]);i++)if(!strcmp(tab,allowed[i]))found=1;
    if(!found||(*site&&!pj_key(site,sizeof(site)))||(*kind&&(!pj_kind(kind)||strcmp(tab,"activity")))||
 (*facet&&(strcmp(tab,"connections")||strlen(facet)>31||
  strspn(facet,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(facet)))||
 (!ak_app_tab(tab)&&(*peer||strcmp(view,"discover")))||
 (ak_app_tab(tab)&&!ak_app_tab_view(ak_app_tab(tab),view))||
 (*peer&&(!ak_app_tab(tab)||strcmp(view,ak_app_tab(tab)->peer_view)||strncmp(peer,"person:",7)||
 strlen(peer)>191||strspn(peer,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:._-")!=strlen(peer))))return 0;
    const char *numbers[]={cursor,item};
    for(size_t i=0;i<2;i++)if(*numbers[i]){
        if(strlen(numbers[i])>19||strspn(numbers[i],"0123456789")!=strlen(numbers[i]))return 0;
        errno=0;char *end;long long value=strtoll(numbers[i],&end,10);if(errno||*end||value<0)return 0;
    }
    char next[2100],encoded[6400],location[6500];
    int n=snprintf(next,sizeof(next),"/profile%s%s",query&&*query?"?":"",query?query:"");
    if(n<0||(size_t)n>=sizeof(next)||!pj_encode(next,encoded,sizeof(encoded)))return 0;
    n=snprintf(location,sizeof(location),"/session/continue?next=%s",encoded);
    if(n<0||(size_t)n>=sizeof(location))return 0;
    page_redirect(location);return 1;
}
static int pj_one_header(const char *raw,const char *name,char *out,size_t cap){
    size_t len=strlen(name);int count=0;out[0]=0;
    const char *p=raw?strchr(raw,'\n'):NULL;
    while(p&&*++p&&*p!='\r'&&*p!='\n'){
        const char *end=strchr(p,'\n');if(!end)end=p+strlen(p);
        if((size_t)(end-p)>len&&!strncasecmp(p,name,len)&&p[len]==':'){
            if(++count!=1)return 0;
            const char *v=p+len+1,*last=end;while(v<end&&(*v==' '||*v=='\t'))v++;
            if(last>v&&last[-1]=='\r')last--;
            if((size_t)(last-v)>=cap||memchr(v,'\r',(size_t)(last-v)))return 0;
            memcpy(out,v,(size_t)(last-v));out[last-v]=0;
        }
        if(!*end)break;
        p=end;
    }
    return count==1;
}
static int pj_same_origin(const char *raw,const char *host){
    char origin[320],type[100],expected[320];
    if(!host||!*host||strlen(host)>255||strpbrk(host,"/\\@\r\n\t "))return 0;
    int loopback=!strcmp(host,"localhost")||!strncmp(host,"localhost:",10)||
        !strcmp(host,"127.0.0.1")||!strncmp(host,"127.0.0.1:",10);
    snprintf(expected,sizeof(expected),"%s://%s",loopback?"http":"https",host);
    return pj_one_header(raw,"Origin",origin,sizeof(origin))&&!strcmp(origin,expected)&&
        pj_one_header(raw,"Content-Type",type,sizeof(type))&&!strcmp(type,"application/x-www-form-urlencoded");
}
static int pj_local_path(const char *s){
    if(!s||s[0]!='/'||!s[1]||s[1]=='/'||strlen(s)>=600)return 0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++)if(*p<=32||*p==127||*p=='\\')return 0;
    /* No percent-encoded path separator/control before the query: the same
     * string must have the same local-path interpretation at every hop. */
    size_t path=strcspn(s,"?#");if(memchr(s,'%',path))return 0;
    return 1;
}
static int pj_route(PjKind kind,const char *id,char out[768]){
    char encoded[600];if(!pj_kind_name(kind)[0]||!pj_key(id,200)||!pj_encode(id,encoded,sizeof(encoded)))return 0;
    int n=snprintf(out,768,"/participation?kind=%s&id=%s",pj_kind_name(kind),encoded);return n>0&&n<768;
}
static int pj_login_url(const char *next,char out[2600]){
    char encoded[2400];if(!pj_local_path(next)||!pj_encode(next,encoded,sizeof(encoded)))return 0;
    int n=snprintf(out,2600,"/session/continue?next=%s",encoded);return n>0&&n<2600;
}
static int pj_external_link(const char *s){
    if(!s||!public_http_url(s)||strlen(s)>2048)return 0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++)if(*p<=32||*p==127||strchr("\\<>\"",*p))return 0;
    const char *host=strstr(s,"://");if(!host)return 0;host+=3;
    size_t n=strcspn(host,"/?#");return n&&!memchr(host,'@',n);
}
static void pj_document(const PjRecord *r){
    char path[768],login[2600];pj_route(r->kind,r->target,path);
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nX-Robots-Tag: noindex, nofollow\r\nConnection: close\r\n\r\n",stdout);
    ak_app_open(r->title,"directory","route-participation");
    ui_start(stdout,"div");ui_attr(stdout,"class","pj-page");ui_flag(stdout,"data-participant-journey");ui_gt(stdout);ui_start(stdout,"nav");ui_attr(stdout,"class","pj-breadcrumb");ui_attr(stdout,"aria-label","Breadcrumb");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"href","/discover");ui_gt(stdout);ui_copy(stdout,"Discover");ui_close(stdout,"a");ui_start(stdout,"span");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);ui_copy(stdout,"/");ui_close(stdout,"span");ui_start(stdout,"a");ui_attr(stdout,"href","/profile?tab=activity");ui_gt(stdout);ui_copy(stdout,"My activity");ui_close(stdout,"a");ui_close(stdout,"nav");ui_open(stdout,"p","pj-kicker");
    html(r->kind==PJ_COMMUNITY?"Community participation":"Your interests");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);html(r->title);ui_close(stdout,"h1");ui_open(stdout,"div","pj-layout");ui_start(stdout,"section");ui_attr(stdout,"aria-label","About this activity");ui_gt(stdout);
    if(r->visible){
        ui_open(stdout,"p","pj-source");html(r->description);ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"class","pj-source-link");ui_attr_open(stdout,"href");ui_copy(stdout,"/s/");
        fputs(r->kind==PJ_COMMUNITY?"community/":"publication_occurrence/",stdout);url_query_value(r->target);
        fputs("\">Open the full ",stdout);fputs(r->kind==PJ_COMMUNITY?"community":"source",stdout);fputs(" page</a>",stdout);
        if(pj_external_link(r->href)){ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"class","pj-source-link");ui_attr(stdout,"target","_blank");ui_attr(stdout,"rel","noopener noreferrer");ui_attr_open(stdout,"href");html(r->href);fputs("\">Visit the original publisher ↗</a></p>",stdout);}
    }else ui_text(stdout, "p", "pj-notice", "This source is no longer public. Your private record remains available; it does not reveal the withdrawn content.");
    ui_open(stdout,"p","pj-notice");
    html(r->kind==PJ_COMMUNITY?"Your request is kept in Aurekai. It does not join or contact a community on another site.":"Keep this in your private activity. Registration and subscriptions on the original site remain separate.");
    ui_close(stdout,"p");ui_close(stdout,"section");ui_start(stdout,"aside");ui_attr(stdout,"class","pj-state");ui_attr(stdout,"aria-label","Your participation");ui_gt(stdout);ui_open(stdout,"h2",NULL);html(pj_state_label(r));ui_close(stdout,"h2");
    if(!r->actor[0]){
        if(pj_login_url(path,login)){ui_open(stdout,"p",NULL);ui_copy(stdout,"Sign in to keep this choice in your account. You will return to this same page before anything changes.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"class","pj-button");ui_flag(stdout,"data-participant-signin");ui_attr_open(stdout,"href");html(login);fputs("\">Sign in to continue</a>",stdout);}
    }else{
        if(r->present){ui_open(stdout,"p",NULL);ui_copy(stdout,"Kept for you");ui_open(stdout,"time",NULL);html(r->began);ui_close(stdout,"time");ui_close(stdout,"p");}
        if(r->kind==PJ_COMMUNITY&&!strcmp(r->state,"requested")&&!strcmp(r->status,"published"))ui_text(stdout, "p", NULL, "Your request is saved in Aurekai. Membership has not been accepted yet.");
        if(r->kind==PJ_COMMUNITY&&!strcmp(r->state,"active"))fputs(r->expired?"<p>This membership's expiry has passed. Its community manages renewal.</p>":"<p>Your membership is active. Open the community to continue.</p>",stdout);
        if(r->managed&&r->visible){
            ui_open(stdout,"p",NULL);ui_copy(stdout,"Manage your participation from the community itself.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"class","pj-button");ui_attr_open(stdout,"href");ui_copy(stdout,"/community?community=");
            url_query_value(r->target);fputs("\">Open community</a>",stdout);
        }
        const char *actions[]={"follow","unfollow","request","withdraw"};
        const char *labels[]={"Follow in my activity","Stop following","Request participation","Withdraw my request"};
        for(size_t i=0;i<4;i++)if(pj_action_offered(r,actions[i])){
            const FtField hidden[]={{"kind",pj_kind_name(r->kind)},{"id",r->target},{"action",actions[i]},{"expected",r->expected}};
            const FtForm f={.id="pj-change",.action="/participation/change",.label=labels[i],.hidden=hidden,.nhidden=4};
            ft_form(stdout,&f);
        }
        ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/profile?tab=activity&amp;kind=");html(pj_kind_name(r->kind));fputs("\">Return to my activity</a></p>",stdout);
    }
    ui_close(stdout,"aside");ui_close(stdout,"div");ui_close(stdout,"div");ak_app_close();
}
static void pj_error(int code){
    protocol_reply(code,"text/plain; charset=utf-8",code==409?
      "This participation changed or has conflicting records. Reopen the exact activity before choosing an action.\n":
      code==404?"This activity is not available to the current account.\n":
      code==503?"The participation owner did not confirm this request. Reopen the saved activity before retrying.\n":
      "Review the exact activity and submit its own form.\n");
}
/* Existing form consumers call this branch; no second relation writer remains. */
static int pj_legacy(const char *method,const BfRequest *route,const char *body,const char *raw,
                     const char *host,const char *bearer,const char *auth_db){
    PjKind kind=bf_request_is(route,"community_join")?PJ_COMMUNITY:
      bf_request_is(route,"occurrence_follow")?PJ_OCCURRENCE:0;
    if(!kind)return 0;
    if(strcmp(method,"POST")){pj_error(405);return 1;}
    char id[200]="",back[600]="",actor[64]="",path[768],login[2600];
    PjField f[]={{"id",id,sizeof(id)},{"return",back,sizeof(back)}};
    if(!pj_fields(body,f,2,2048)||!pj_key(id,sizeof(id))){pj_error(400);return 1;}
    if(!pj_same_origin(raw,host)){pj_error(403);return 1;}
    pj_route(kind,id,path);
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){
        if(pj_login_url(path,login))page_redirect(login);else pj_error(400);return 1;
    }
    sqlite3 *db=cms_open_rw();if(!db){pj_error(503);return 1;}
    PjRecord r;int code=pj_change(db,kind,actor,id,kind==PJ_COMMUNITY?"request":"follow",NULL,&r);
    sqlite3_close(db);if(code!=200){pj_error(code);return 1;}
    page_redirect(pj_local_path(back)?back:path);return 1;
}
static int serve_participant_journey(const BfRequest *rq) {
    const char *method=rq->method; const char *target=rq->target; const char *query=rq->query; const char *body=rq->body; const char *raw=rq->raw; const char *host=rq->host; const char *bearer=rq->bearer; const char *auth_db=rq->auth_db; const char *accept=rq->accept;
    (void)method;(void)target;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
    int mutation=bf_request_is(rq,"change");
    if(strcmp(method,mutation?"POST":"GET")){pj_error(405);return 1;}
    if(!mutation&&!wants_html(accept)){protocol_reply(406,"text/plain; charset=utf-8","Open the participant page as HTML.\n");return 1;}
    char kind[16]="",id[200]="",action[16]="",expected[65]="",actor[64]="",path[768],login[2600];
    PjField f[]={{"kind",kind,sizeof(kind)},{"id",id,sizeof(id)},{"action",action,sizeof(action)},{"expected",expected,sizeof(expected)}};
    if(!pj_fields(mutation?body:query,f,mutation?4:2,2048)||!pj_kind(kind)||!pj_key(id,200)||
       (mutation&&(!action[0]||!expected[0]))){pj_error(400);return 1;}
    if(mutation&&!pj_same_origin(raw,host)){pj_error(403);return 1;}
    int signed_in=page_session_actor(bearer,auth_db,actor,sizeof(actor));
    pj_route(pj_kind(kind),id,path);
    if(mutation&&!signed_in){if(pj_login_url(path,login))page_redirect(login);else pj_error(400);return 1;}
    sqlite3 *db=mutation?cms_open_rw():cms_open_ro();if(!db){pj_error(503);return 1;}
    PjRecord r;int code=mutation?pj_change(db,pj_kind(kind),actor,id,action,expected,&r):pj_load(db,pj_kind(kind),actor,id,&r);
    sqlite3_close(db);if(code!=200){pj_error(code);return 1;}
    if(mutation)page_redirect(path);else pj_document(&r);return 1;
}
static void pj_profile_url(const char *site,PjKind kind,const char *q,sqlite3_int64 cursor){
    fputs("/profile?tab=activity&amp;kind=",stdout);html(pj_kind_name(kind));
    if(site&&*site){fputs("&amp;site=",stdout);url_query_value(site);}
    if(q&&*q){fputs("&amp;q=",stdout);url_query_value(q);}
    if(cursor)printf("&amp;cursor=%lld",(long long)cursor);
}
/* Private read projection: the root participation actor is the authenticated
 * user, independent from the selected public profile/site's person identity. */
static void pj_profile_list(sqlite3 *db,long uid,const char *site,PjKind kind,const char *q,sqlite3_int64 cursor){
    if(!db||uid<1){ui_text(stdout, "p", NULL, "Your private activity could not be read.");return;}
    if(!pj_kind_name(kind)[0])kind=PJ_OCCURRENCE;
    char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);
    ui_start(stdout,"section");ui_attr(stdout,"class","pj-activity");ui_flag(stdout,"data-private-participation");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"My activity");ui_close(stdout,"h2");ui_open(stdout,"p","pj-notice");ui_copy(stdout,"Only you can see this activity. Switching profile sites does not publish it.");ui_close(stdout,"p");ui_start(stdout,"nav");ui_attr(stdout,"class","pj-filter");ui_attr(stdout,"aria-label","Activity type");ui_gt(stdout);
    for(int i=1;i<=2;i++){ui_start(stdout,"a");ui_attr_open(stdout,"href");pj_profile_url(site,(PjKind)i,"",0);fputs("\"",stdout);if(i==(int)kind)fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(i==1?"Following":"Community requests");ui_close(stdout,"a");}
    ui_close(stdout,"nav");
    {const FtField hidden[]={{"tab","activity"},{"kind",pj_kind_name(kind)},{site&&*site?"site":NULL,site}};
     const FtInput find[]={{.label="Search my activity",.type="search",.name="q",.maxlength="159",.value=q?q:"",.placeholder="Search current public titles"}};
     const FtForm f={.id="pj-search",.method="get",.action="/profile",.label="Search activity",.hidden=hidden,.nhidden=3,.inputs=find,.ninputs=1};
     ft_form(stdout,&f);}
    const char *sql=kind==PJ_COMMUNITY?
      "SELECT m.id,m.community FROM community_membership m WHERE m.namespace='root' AND m.member_subject=?1 "
      "AND (?2=0 OR m.id<?2) AND (?3='' OR EXISTS(SELECT 1 FROM community_body c WHERE c.namespace='root' "
      "AND c.community=m.community AND c.status='published' AND c.visibility IN ('public','public_observed') "
      "AND instr(lower(c.title),lower(?3))>0)) ORDER BY m.id DESC LIMIT 25":
      "SELECT m.id,m.occurrence_id FROM occurrence_follow m WHERE m.namespace='root' AND m.follower=?1 "
      "AND (?2=0 OR m.id<?2) AND (?3='' OR EXISTS(SELECT 1 FROM publication_occurrence c WHERE c.namespace='root' "
      "AND c.occurrence_id=m.occurrence_id AND c.status='published' AND c.visibility='public' "
      "AND lower(c.assertion_state)='observed' AND instr(lower(c.title),lower(?3))>0)) ORDER BY m.id DESC LIMIT 25";
    sqlite3_stmt *s=NULL;int count=0,step=SQLITE_DONE,more=0;sqlite3_int64 last=0;
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){ui_open(stdout,"p",NULL);ui_copy(stdout,"The activity owner is unavailable. This is not an empty activity list.");ui_close(stdout,"p");ui_close(stdout,"section");return;}
    sqlite3_bind_text(s,1,actor,-1,SQLITE_STATIC);sqlite3_bind_int64(s,2,cursor);sqlite3_bind_text(s,3,q?q:"",-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        if(count==24){more=1;break;}last=sqlite3_column_int64(s,0);count++;
        char id[200];PjRecord r;int code=pj_column(s,1,id,sizeof(id))?pj_load(db,kind,actor,id,&r):503;
        if(code!=200){ui_open(stdout,"article","pj-row");ui_open(stdout,"div",NULL);ui_open(stdout,"h3",NULL);ui_copy(stdout,"Activity needs review");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"A retained identity could not be resolved safely. No records were changed.");ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"article");continue;}
        char path[768];if(!pj_route(kind,id,path))continue;
        ui_open(stdout,"article","pj-row");ui_open(stdout,"div",NULL);ui_open(stdout,"h3",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");html(path);fputs("\">",stdout);html(r.title);ui_close(stdout,"a");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);
        html(r.visible?(kind==PJ_COMMUNITY?"Your saved request and its current membership status.":"Open the original material or manage your follow."):"The source is no longer public. Your private relationship can still be reviewed.");
        ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"aside",NULL);ui_open(stdout,"span","pj-status");html(pj_state_label(&r));ui_close(stdout,"span");ui_start(stdout,"a");ui_attr_open(stdout,"href");html(path);fputs("\">Open this activity →</a></aside></article>",stdout);
    }
    sqlite3_finalize(s);
    if(step!=SQLITE_DONE&&step!=SQLITE_ROW)ui_text(stdout, "p", NULL, "The activity page could not be read completely. Reload before treating it as a complete list.");
    else if(!count){ui_open(stdout,"div","pj-empty");ui_open(stdout,"h3",NULL);ui_copy(stdout,"No matching activity");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Follow a public source or request participation from its community page. Return here to see its current state.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/discover");ui_gt(stdout);ui_copy(stdout,"Explore public material");ui_close(stdout,"a");ui_close(stdout,"div");}
    if(more){ui_start(stdout,"a");ui_attr(stdout,"class","pj-more");ui_attr_open(stdout,"href");pj_profile_url(site,kind,q,last);fputs("\">Earlier activity</a>",stdout);}
    ui_close(stdout,"section");
}
#endif
