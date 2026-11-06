#ifndef BONFYRE_PARTICIPANT_JOURNEY_H
#define BONFYRE_PARTICIPANT_JOURNEY_H
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
    char tab[24]="overview",site[128]="",q[160]="",cursor[24]="",item[24]="",kind[16]="";
    PjField fields[]={{"tab",tab,sizeof(tab)},{"site",site,sizeof(site)},
      {"q",q,sizeof(q)},{"cursor",cursor,sizeof(cursor)},{"item",item,sizeof(item)},{"kind",kind,sizeof(kind)}};
    if(!pj_fields(query,fields,6,2048))return 0;
    const char *allowed[]={"overview","posts","saved","about","sites","edit","compose","activity","communities"};
    int found=0;for(size_t i=0;i<sizeof(allowed)/sizeof(allowed[0]);i++)if(!strcmp(tab,allowed[i]))found=1;
    if(!found||(*site&&!pj_key(site,sizeof(site)))||(*kind&&(!pj_kind(kind)||strcmp(tab,"activity"))))return 0;
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
static void pj_hidden(const char *key,const char *value){
    fputs("<input type=\"hidden\" name=\"",stdout);html(key);fputs("\" value=\"",stdout);html(value);fputs("\">",stdout);
}
static int pj_external_link(const char *s){
    if(!s||!public_http_url(s)||strlen(s)>2048)return 0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++)if(*p<=32||*p==127||strchr("\\<>\"",*p))return 0;
    const char *host=strstr(s,"://");if(!host)return 0;host+=3;
    size_t n=strcspn(host,"/?#");return n&&!memchr(host,'@',n);
}
static void pj_styles(void){
    fputs("<style>"
      ".pj-page{max-width:1260px;margin:0 auto;padding:30px clamp(16px,4vw,64px) 70px;color:#203b2a;font:15px/1.6 system-ui}.pj-page *,.pj-activity *{box-sizing:border-box}.pj-breadcrumb{display:flex;gap:12px;flex-wrap:wrap;font-size:13px;margin-bottom:26px}.pj-page a,.pj-activity a{color:#285838}.pj-page h1{font:400 clamp(36px,4vw,62px)/1.08 Georgia,serif;max-width:24ch;letter-spacing:-.025em;overflow-wrap:anywhere}.pj-page h2{font:400 27px/1.2 Georgia,serif}.pj-kicker{font:600 11px ui-monospace,monospace;letter-spacing:.1em;text-transform:uppercase;color:#627550}.pj-layout{display:grid;grid-template-columns:minmax(0,1fr) 330px;gap:32px}.pj-source{white-space:pre-wrap;overflow-wrap:anywhere;font:18px/1.8 Georgia,serif}.pj-source-link{display:inline-flex;margin:18px 0;min-height:44px;align-items:center}.pj-state{align-self:start;border:1px solid #bdcead;border-radius:18px;padding:24px;background:#f1f6e7;position:sticky;top:20px}.pj-state h2{font-size:26px;margin-top:0}.pj-state p{font-size:14px}.pj-state button,.pj-button{min-height:44px;display:inline-flex;align-items:center;justify-content:center;padding:11px 17px;border:1px solid #244c33;border-radius:10px;background:#2b5038;color:#fffef5!important;font:600 13px/1.5 system-ui;text-decoration:none}.pj-state button{cursor:pointer;width:100%}.pj-state time{display:block;font:12px/1.7 ui-monospace,monospace;overflow-wrap:anywhere}.pj-notice{padding:15px 18px;background:#edf3e3;border:1px solid #c9d7b9;border-radius:12px;font-size:13px}.pj-activity .pj-filter{display:flex;gap:8px;flex-wrap:wrap;margin:18px 0}.pj-filter a{padding:9px 16px;border:1px solid #c3d1b4;border-radius:999px;text-decoration:none;min-height:44px}.pj-filter a[aria-current=page]{background:#e4eed7;font-weight:650}.pj-activity .pj-row{display:grid;grid-template-columns:minmax(0,1fr) 200px;gap:22px;padding:22px 8px;border-bottom:1px solid #d4dec9}.pj-row h3{font:400 24px/1.25 Georgia,serif;margin:0 0 10px}.pj-row p{margin:5px 0;white-space:normal;font-size:14px}.pj-status{font:600 12px/1.4 system-ui;display:inline-block;border:1px solid #c3d1b4;border-radius:8px;padding:8px 12px;background:#f1f5e8}.pj-row aside{align-self:center}.pj-row aside a{display:block;font-size:13px;margin-top:12px;min-height:36px}.pj-activity form{display:flex;gap:10px;margin:20px 0}.pj-activity input{min-width:0;flex:1}.pj-activity .pj-empty{padding:28px;background:#f0f5e7;border:1px dashed #bacaa8;border-radius:15px}.pj-more{display:inline-flex;align-items:center;min-height:44px;margin-top:20px;padding:10px 17px;border:1px solid #bccfa9;border-radius:999px}.pj-page :focus-visible,.pj-activity :focus-visible{outline:3px solid #ac842e;outline-offset:3px}"
      "@media(max-width:850px){.pj-layout{grid-template-columns:minmax(0,1fr)}.pj-state{position:static}.pj-row aside{align-self:start}}@media(max-width:600px){.pj-activity .pj-row{grid-template-columns:minmax(0,1fr);gap:12px}.pj-page h1{font-size:38px}.pj-activity form{flex-wrap:wrap}.pj-activity input{flex-basis:100%}.pj-state{padding:20px}}@media(prefers-reduced-motion:reduce){.pj-page *,.pj-activity *{scroll-behavior:auto!important;transition:none!important;animation:none!important}}"
      "</style>",stdout);
}
static void pj_document(const PjRecord *r){
    char path[768],login[2600];pj_route(r->kind,r->target,path);
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"robots\" content=\"noindex,nofollow\"><title>",stdout);
    html(r->title);fputs(" · Aurekai</title><link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=2\">",stdout);pj_styles();
    fputs("</head><body>" BF_ESTATE_BAR_CORE "<main class=\"pj-page\" data-participant-journey><nav class=\"pj-breadcrumb\" aria-label=\"Breadcrumb\"><a href=\"/discover\">Discover</a><span aria-hidden=\"true\">/</span><a href=\"/profile?tab=activity\">My activity</a></nav><p class=\"pj-kicker\">",stdout);
    html(r->kind==PJ_COMMUNITY?"Community participation":"Your interests");fputs("</p><h1>",stdout);html(r->title);fputs("</h1><div class=\"pj-layout\"><section aria-label=\"About this activity\">",stdout);
    if(r->visible){
        fputs("<p class=\"pj-source\">",stdout);html(r->description);fputs("</p><a class=\"pj-source-link\" href=\"/s/",stdout);
        fputs(r->kind==PJ_COMMUNITY?"community/":"publication_occurrence/",stdout);url_query_value(r->target);
        fputs("\">Open the full ",stdout);fputs(r->kind==PJ_COMMUNITY?"community":"source",stdout);fputs(" page</a>",stdout);
        if(pj_external_link(r->href)){fputs("<p><a class=\"pj-source-link\" target=\"_blank\" rel=\"noopener noreferrer\" href=\"",stdout);html(r->href);fputs("\">Visit the original publisher ↗</a></p>",stdout);}
    }else fputs("<p class=\"pj-notice\">This source is no longer public. Your private record remains available; it does not reveal the withdrawn content.</p>",stdout);
    fputs("<p class=\"pj-notice\">",stdout);
    html(r->kind==PJ_COMMUNITY?"Your request is kept in Aurekai. It does not join or contact a community on another site.":"Keep this in your private activity. Registration and subscriptions on the original site remain separate.");
    fputs("</p></section><aside class=\"pj-state\" aria-label=\"Your participation\"><h2>",stdout);html(pj_state_label(r));fputs("</h2>",stdout);
    if(!r->actor[0]){
        if(pj_login_url(path,login)){fputs("<p>Sign in to keep this choice in your account. You will return to this same page before anything changes.</p><a class=\"pj-button\" data-participant-signin href=\"",stdout);html(login);fputs("\">Sign in to continue</a>",stdout);}
    }else{
        if(r->present){fputs("<p>Kept for you<time>",stdout);html(r->began);fputs("</time></p>",stdout);}
        if(r->kind==PJ_COMMUNITY&&!strcmp(r->state,"requested")&&!strcmp(r->status,"published"))fputs("<p>Your request is saved in Aurekai. Membership has not been accepted yet.</p>",stdout);
        if(r->kind==PJ_COMMUNITY&&!strcmp(r->state,"active"))fputs(r->expired?"<p>This membership's expiry has passed. Its community manages renewal.</p>":"<p>Your membership is active. Open the community to continue.</p>",stdout);
        if(r->managed&&r->visible){
            fputs("<p>Manage your participation from the community itself.</p><a class=\"pj-button\" href=\"/community?community=",stdout);
            url_query_value(r->target);fputs("\">Open community</a>",stdout);
        }
        const char *actions[]={"follow","unfollow","request","withdraw"};
        const char *labels[]={"Follow in my activity","Stop following","Request participation","Withdraw my request"};
        for(size_t i=0;i<4;i++)if(pj_action_offered(r,actions[i])){
            fputs("<form method=\"post\" action=\"/participation/change\">",stdout);
            pj_hidden("kind",pj_kind_name(r->kind));pj_hidden("id",r->target);pj_hidden("action",actions[i]);pj_hidden("expected",r->expected);
            fputs("<button type=\"submit\">",stdout);html(labels[i]);fputs("</button></form>",stdout);
        }
        fputs("<p><a href=\"/profile?tab=activity&amp;kind=",stdout);html(pj_kind_name(r->kind));fputs("\">Return to my activity</a></p>",stdout);
    }
    fputs("</aside></div></main></body></html>",stdout);
}
static void pj_error(int code){
    protocol_reply(code,"text/plain; charset=utf-8",code==409?
      "This participation changed or has conflicting records. Reopen the exact activity before choosing an action.\n":
      code==404?"This activity is not available to the current account.\n":
      code==503?"The participation owner did not confirm this request. Reopen the saved activity before retrying.\n":
      "Review the exact activity and submit its own form.\n");
}
/* Existing form consumers call this branch; no second relation writer remains. */
static int pj_legacy(const char *method,const char *target,const char *body,const char *raw,
                     const char *host,const char *bearer,const char *auth_db){
    PjKind kind=target&&!strcmp(target,"/govern/community-join")?PJ_COMMUNITY:
      target&&!strcmp(target,"/govern/occurrence-follow")?PJ_OCCURRENCE:0;
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
static int serve_participant_journey(const char *method,const char *target,const char *query,
    const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db,const char *accept){
    int mutation=target&&!strcmp(target,"/participation/change");
    if(!target||(!mutation&&strcmp(target,"/participation")))return 0;
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
    if(!db||uid<1){fputs("<p>Your private activity could not be read.</p>",stdout);return;}
    if(!pj_kind_name(kind)[0])kind=PJ_OCCURRENCE;
    char actor[64];snprintf(actor,sizeof(actor),"user:%ld",uid);
    pj_styles();fputs("<section class=\"pj-activity\" data-private-participation><h2>My activity</h2><p class=\"pj-notice\">Only you can see this activity. Switching profile sites does not publish it.</p><nav class=\"pj-filter\" aria-label=\"Activity type\">",stdout);
    for(int i=1;i<=2;i++){fputs("<a href=\"",stdout);pj_profile_url(site,(PjKind)i,"",0);fputs("\"",stdout);if(i==(int)kind)fputs(" aria-current=\"page\"",stdout);fputs(">",stdout);html(i==1?"Following":"Community requests");fputs("</a>",stdout);}
    fputs("</nav><form method=\"get\" action=\"/profile\">",stdout);pj_hidden("tab","activity");pj_hidden("kind",pj_kind_name(kind));if(site&&*site)pj_hidden("site",site);
    fputs("<input type=\"search\" name=\"q\" aria-label=\"Search my activity\" placeholder=\"Search current public titles\" maxlength=\"159\" value=\"",stdout);html(q?q:"");fputs("\"><button type=\"submit\">Search activity</button></form>",stdout);
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
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){fputs("<p>The activity owner is unavailable. This is not an empty activity list.</p></section>",stdout);return;}
    sqlite3_bind_text(s,1,actor,-1,SQLITE_STATIC);sqlite3_bind_int64(s,2,cursor);sqlite3_bind_text(s,3,q?q:"",-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        if(count==24){more=1;break;}last=sqlite3_column_int64(s,0);count++;
        char id[200];PjRecord r;int code=pj_column(s,1,id,sizeof(id))?pj_load(db,kind,actor,id,&r):503;
        if(code!=200){fputs("<article class=\"pj-row\"><div><h3>Activity needs review</h3><p>A retained identity could not be resolved safely. No records were changed.</p></div></article>",stdout);continue;}
        char path[768];if(!pj_route(kind,id,path))continue;
        fputs("<article class=\"pj-row\"><div><h3><a href=\"",stdout);html(path);fputs("\">",stdout);html(r.title);fputs("</a></h3><p>",stdout);
        html(r.visible?(kind==PJ_COMMUNITY?"Your saved request and its current membership status.":"Open the original material or manage your follow."):"The source is no longer public. Your private relationship can still be reviewed.");
        fputs("</p></div><aside><span class=\"pj-status\">",stdout);html(pj_state_label(&r));fputs("</span><a href=\"",stdout);html(path);fputs("\">Open this activity →</a></aside></article>",stdout);
    }
    sqlite3_finalize(s);
    if(step!=SQLITE_DONE&&step!=SQLITE_ROW)fputs("<p>The activity page could not be read completely. Reload before treating it as a complete list.</p>",stdout);
    else if(!count)fputs("<div class=\"pj-empty\"><h3>No matching activity</h3><p>Follow a public source or request participation from its community page. Return here to see its current state.</p><a href=\"/discover\">Explore public material</a></div>",stdout);
    if(more){fputs("<a class=\"pj-more\" href=\"",stdout);pj_profile_url(site,kind,q,last);fputs("\">Earlier activity</a>",stdout);}
    fputs("</section>",stdout);
}
#endif
