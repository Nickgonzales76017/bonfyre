#ifndef BONFYRE_ACCOUNT_INBOX_WEB_H
#define BONFYRE_ACCOUNT_INBOX_WEB_H
/* GET /inbox for a signed-in person: what waits on them, apart from what the
 * agents are doing. A signed-out reader, or a query this page does not own,
 * falls through to the public /inbox surface unchanged.
 *
 * Needs you is read from the operator store as it is now (release approvals,
 * provider steps, the provider checklist), limited to goals this account's
 * work index lists, plus organization invitations addressed to the person's
 * email. Agent work is read from the same account work index the activity
 * rail and work board use. Every action is an existing owner's: Approve posts
 * the work board's own /work-board/act; sign-ins open the provider's page or
 * the existing connect flow; organization steps open /organizations. This
 * page decides nothing, starts nothing and owns no work. */

#define AI_AGENT_MAX 8
#define AI_CREDIT_ATTENTION "OpenAI API credits are exhausted. Add credits before retrying this work."
typedef struct {char id[128],title[161],worker[161],attention[241],state[24];long long updated;} AiWork;

static int ai_query(const char *query,char context[96],char done[16]){
    unsigned seen=0;snprintf(context,96,"self");done[0]=0;if(!query||!*query)return 1;if(strlen(query)>256)return 0;
    for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[16],ignored[96],*out=NULL;size_t cap=0;unsigned bit=0;
        if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
        if(!strcmp(key,"context")){out=context;cap=96;bit=1;}else if(!strcmp(key,"done")){out=done;cap=16;bit=2;}
        else if(!strcmp(key,"utm_source")||!strcmp(key,"utm_medium")||!strcmp(key,"utm_campaign")||!strcmp(key,"utm_content")||!strcmp(key,"utm_term")){out=ignored;cap=sizeof(ignored);}
        else return 0;
        /* Tracking metadata is validated but never becomes account authority. */
        if((seen&bit)||!ac_decode(eq+1,(size_t)(end-eq-1),out,cap)||!*out)return 0;seen|=bit;p=*end?end+1:end;}
    return !*done||ac_word(done);
}
static void ai_ago(long long ms){
    long long d=(long long)time(NULL)*1000-ms;if(ms<=0)return;
    if(d<90000)fputs("just now",stdout);else if(d<3600000)printf("%lld min ago",d/60000);else if(d<86400000)printf("%lld h ago",d/3600000);
    else if(d<604800000)printf("%lld day%s ago",d/86400000,d/86400000==1?"":"s");else ac_work_when((double)ms/1000.0);
}
/* A link on one of Aurekai's own hosts stays on the host the person is using. */
static void ai_href(const char *url){
    static const char *const own[]={"https://aurekai.work/","https://aurekai.life/","https://aurekai.live/","https://aurekai.shop/"};
    for(size_t i=0;i<4;i++)if(!strncmp(url,own[i],strlen(own[i]))){html(url+strlen(own[i])-1);return;}html(url);
}
static int ai_external(const char *url){return strncmp(url,"https://aurekai.",16)!=0;}
/* One line of a goal's text: the first line starting with prefix, or the
 * first line when prefix is NULL. Copied without its trailing newline. */
static int ai_line(const char *text,const char *prefix,char *out,size_t cap){
    for(const char *p=text;p&&*p;){const char *end=strchr(p,'\n');size_t n=end?(size_t)(end-p):strlen(p);
        if(n&&(!prefix||!strncmp(p,prefix,strlen(prefix)))){if(n>=cap)n=cap-1;memcpy(out,p,n);out[n]=0;return 1;}
        p=end?end+1:NULL;}
    return 0;
}
/* The first sentence of an owner's message, for a one-line reason. */
static void ai_sentence(const char *text,size_t cap){
    char line[320];size_t n=strcspn(text,".;\n");if(n>=sizeof(line))n=sizeof(line)-1;if(n>cap)n=cap;memcpy(line,text,n);line[n]=0;html(line);if(text[n]=='.'||text[n]==';')fputs(".",stdout);
}
static void ai_act(const char *context,const char *task,const char *action,const char *label,int primary){
    fputs("<form method=\"post\" action=\"/work-board/act\">",stdout);psetup_hidden("context",context);psetup_hidden("task",task);psetup_hidden("action",action);psetup_hidden("back","inbox");
    fputs(primary?"<button type=\"submit\" class=\"ak-inbox-primary\" data-inbox-act=\"":"<button type=\"submit\" data-inbox-act=\"",stdout);html(action);fputs("\">",stdout);html(label);fputs("</button></form>",stdout);
}
static void ai_meta(const char *who,const char *kind,long long ms){
    fputs("<p class=\"ak-inbox-meta\"><span class=\"ak-inbox-who\" data-who=\"you\">",stdout);html(who);fputs("</span><span>",stdout);html(kind);fputs("</span>",stdout);
    if(ms>0){fputs("<time>",stdout);ai_ago(ms);fputs("</time>",stdout);}fputs("</p>",stdout);
}
static int ai_approved(const char *release){
    const char *idx=getenv("BONFYRE_ACCOUNT_WORK_DB"),*slash=idx?strrchr(idx,'/'):NULL;char path[PATH_MAX];
    if(!slash||!*release||snprintf(path,sizeof(path),"%.*s/approvals/%s.approved",(int)(slash-idx),idx,release)>=(int)sizeof(path))return 0;
    return access(path,F_OK)==0;
}
static void ai_release(const char *context,const AcPersonGoal *g){
    char changes[700];int approved=ai_approved(g->release);
    fputs("<li class=\"ak-inbox-item\" data-needs=\"you\" data-inbox-kind=\"release\" id=\"",stdout);html(g->id);fputs("\"><span class=\"ak-inbox-mark\" aria-hidden=\"true\">",stdout);page_glyph("check");fputs("</span><div class=\"ak-inbox-body\">",stdout);
    ai_meta(approved?"Approved by you":"Needs you","Release to approve",(long long)(g->at*1000));
    fputs("<h3>",stdout);html(g->title);fputs("</h3>",stdout);
    if(ai_line(g->details,"What it changes:",changes,sizeof(changes))||ai_line(g->details,NULL,changes,sizeof(changes))){fputs("<p class=\"ak-inbox-text\">",stdout);html(changes);fputs("</p>",stdout);}
    if(*g->error&&!approved){fputs("<p class=\"ak-inbox-warn\"><strong>Last try:</strong> ",stdout);ai_sentence(g->error,220);fputs("</p>",stdout);}
    fputs("<div class=\"ak-inbox-acts\">",stdout);
    if(approved)fputs("<p class=\"ak-inbox-status\" role=\"status\">Going live now. This updates when the release reports back.</p>",stdout);
    else{ai_act(context,g->id,"approve","Approve and put live",1);ai_act(context,g->id,"later","Not now",0);}
    fputs("<a href=\"/work-board?context=",stdout);url_query_value(context);fputs("&amp;task=",stdout);url_query_value(g->id);fputs("\">Read the full release note</a></div></div></li>",stdout);
}
static void ai_provider(const char *context,const AcPersonGoal *g){
    char first[400],url[400]="";const char *u=strstr(g->details,"https://");
    if(u){size_t n=strcspn(u," \t\r\n<>\"'()");while(n>8&&strchr(".,;:",u[n-1]))n--;if(n<sizeof(url)){memcpy(url,u,n);url[n]=0;}}
    fputs("<li class=\"ak-inbox-item\" data-needs=\"you\" data-inbox-kind=\"provider\" id=\"",stdout);html(g->id);fputs("\"><span class=\"ak-inbox-mark\" aria-hidden=\"true\">",stdout);page_glyph("login");fputs("</span><div class=\"ak-inbox-body\">",stdout);
    ai_meta("Needs you","Provider account step",(long long)(g->at*1000));
    fputs("<h3>",stdout);html(g->title);fputs("</h3>",stdout);
    if(ai_line(g->details,NULL,first,sizeof(first))){fputs("<p class=\"ak-inbox-text\">",stdout);html(first);fputs("</p>",stdout);}
    if(*g->error){fputs("<p class=\"ak-inbox-note\">",stdout);html(g->error);fputs("</p>",stdout);}
    fputs("<div class=\"ak-inbox-acts\">",stdout);
    if(*url){fputs("<a class=\"ak-inbox-signin\" data-inbox-signin href=\"",stdout);ai_href(url);fputs(ai_external(url)?"\" target=\"_blank\" rel=\"noopener noreferrer\">Sign in and make the key</a>":"\">Connect</a>",stdout);}
    ai_act(context,g->id,"done","Done, it's set up",!*url);ai_act(context,g->id,"email","Waiting on email or phone",0);ai_act(context,g->id,"review","Waiting on the provider",0);ai_act(context,g->id,"later","Not now",0);
    fputs("</div></div></li>",stdout);
}
/* The provider checklist: one goal whose "Not connected yet" lines are each a
 * sign-in only the account holder can make. */
static void ai_checklist(const char *context,const AcPersonGoal *g){
    const char *section=strstr(g->details,"\nNot connected yet");if(!section&&!strncmp(g->details,"Not connected yet",17))section=g->details;
    fputs("<li class=\"ak-inbox-item\" data-needs=\"you\" data-inbox-kind=\"provider-checklist\" id=\"",stdout);html(g->id);fputs("\"><span class=\"ak-inbox-mark\" aria-hidden=\"true\">",stdout);page_glyph("group");fputs("</span><div class=\"ak-inbox-body\">",stdout);
    ai_meta("Needs you","Customer accounts at providers",(long long)(g->at*1000));
    fputs("<h3>",stdout);html(g->title);fputs("</h3><p class=\"ak-inbox-text\">Each provider below needs you to sign in once and make a key. After that, organizations can open and close their own accounts there.</p>",stdout);
    if(section){
        const char *p=strchr(section+1,'\n');unsigned shown=0,total=0;
        for(const char *q=p;q&&*q;){q++;const char *end=strchr(q,'\n');size_t n=end?(size_t)(end-q):strlen(q);if(!n)break;{char probe[600];if(n<sizeof(probe)){memcpy(probe,q,n);probe[n]=0;if(strstr(probe,": https://"))total++;}}q=end;}
        fputs("<ul class=\"ak-inbox-signins\" data-inbox-signins>",stdout);
        for(const char *q=p;q&&*q;){
            q++;const char *end=strchr(q,'\n');size_t n=end?(size_t)(end-q):strlen(q);if(!n)break;char line[600];if(n>=sizeof(line)){q=end;continue;}
            memcpy(line,q,n);line[n]=0;q=end;char *link=strstr(line,": https://");if(!link)continue;*link=0;link+=2;
            char *gives=strstr(line," (gives customers ");if(gives){*gives=0;gives+=18;size_t m=strlen(gives);if(m&&gives[m-1]==')')gives[m-1]=0;}
            if(shown==6&&total>6){printf("</ul><details class=\"ak-inbox-more\"><summary>%u more providers to sign in to</summary><ul class=\"ak-inbox-signins\">",total-6);}
            fputs("<li><span><strong>",stdout);html(line);fputs("</strong>",stdout);if(gives){fputs("<small>Customers get: ",stdout);html(gives);fputs("</small>",stdout);}
            fputs("</span><a data-inbox-signin href=\"",stdout);ai_href(link);fputs(ai_external(link)?"\" target=\"_blank\" rel=\"noopener noreferrer\">Sign in</a></li>":"\">Connect</a></li>",stdout);shown++;
        }
        fputs(shown>6?"</ul></details>":"</ul>",stdout);
    }
    fputs("<div class=\"ak-inbox-acts\"><a href=\"/connect/github-app\">Connect GitHub</a><a href=\"/apps/connections?context=",stdout);url_query_value(context);fputs("\">Your connections</a><a href=\"/work-board?context=",stdout);url_query_value(context);fputs("&amp;task=",stdout);url_query_value(g->id);fputs("\">Full checklist</a></div></div></li>",stdout);
}
static int ai_column(sqlite3 *db,const char *table,const char *column){
    sqlite3_stmt *s=NULL;char *query=sqlite3_mprintf("PRAGMA table_info(%Q)",table);int found=0;if(!query)return 0;
    if(sqlite3_prepare_v2(db,query,-1,&s,NULL)==SQLITE_OK)while(sqlite3_step(s)==SQLITE_ROW){const char *name=(const char*)sqlite3_column_text(s,1);if(name&&!strcmp(name,column)){found=1;break;}}
    sqlite3_free(query);sqlite3_finalize(s);return found;
}
static int ai_credit_attention(const char *state,const char *attention){return state&&attention&&(!strcmp(state,"retry_wait")||!strcmp(state,"blocked"))&&!strcmp(attention,AI_CREDIT_ATTENTION);}
/* Agent work for this account, newest first. A classified execution-credit
 * hold belongs in Needs you, but it has no account-side retry or close action. */
static int ai_agent_work(const char *subject,const char *scope,AiWork *rows,size_t cap,int *running,int *stopped,int *waiting,const AcPersonGoal *people,int person_count,AiWork *halted,size_t halt_cap,int *halted_n,AiWork *credit,size_t credit_cap,int *credit_n){
    const char *index=getenv("BONFYRE_ACCOUNT_WORK_DB");sqlite3 *db=NULL;sqlite3_stmt *s=NULL;size_t n=0;*running=*stopped=*waiting=*halted_n=*credit_n=0;
    if(!index||*index!='/')return -1;
    if(sqlite3_open_v2(index,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){sqlite3_close(db);return -1;}sqlite3_busy_timeout(db,1000);
    const char *attention=ai_column(db,"account_work","attention")?"COALESCE(w.attention,'')":"''";char query[640];
    int query_length=snprintf(query,sizeof(query),"SELECT w.id,w.title,w.state,w.updated_ms,COALESCE((SELECT worker_id FROM account_work_run r WHERE r.work_id=w.id ORDER BY r.updated_ms DESC LIMIT 1),w.source),%s "
        "FROM account_work w WHERE w.subject=?1 AND w.scope=?2 AND w.state IN('running','retry_wait','blocked','dead_letter','ready','defined') ORDER BY w.updated_ms DESC",attention);
    if(query_length<0||(size_t)query_length>=sizeof(query)){sqlite3_close(db);return -1;}
    if(sqlite3_prepare_v2(db,query,-1,&s,NULL)!=SQLITE_OK){sqlite3_close(db);return -1;}
    sqlite3_bind_text(s,1,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,scope,-1,SQLITE_STATIC);
    while(sqlite3_step(s)==SQLITE_ROW){
        const char *id=(const char*)sqlite3_column_text(s,0),*state=(const char*)sqlite3_column_text(s,2),*reason=(const char*)sqlite3_column_text(s,5);int person=0;if(!id||!state)continue;
        for(int i=0;i<person_count;i++)if(!strcmp(people[i].id,id)){person=1;break;}if(person)continue;
        int live=!strcmp(state,"running")||!strcmp(state,"retry_wait"),halt=!strcmp(state,"blocked")||!strcmp(state,"dead_letter");
        AiWork *row=NULL;if(ai_credit_attention(state,reason)){if((size_t)*credit_n<credit_cap)row=&credit[(*credit_n)++];}else if(live){(*running)++;if(n<cap)row=&rows[n++];}else if(halt){(*stopped)++;if((size_t)*halted_n<halt_cap)row=&halted[(*halted_n)++];}else{(*waiting)++;}
        if(!row)continue;memset(row,0,sizeof(*row));
        snprintf(row->id,sizeof(row->id),"%s",id);snprintf(row->title,sizeof(row->title),"%s",sqlite3_column_text(s,1)?(const char*)sqlite3_column_text(s,1):"");snprintf(row->state,sizeof(row->state),"%s",state);
        row->updated=sqlite3_column_int64(s,3);snprintf(row->worker,sizeof(row->worker),"%s",sqlite3_column_text(s,4)?(const char*)sqlite3_column_text(s,4):"");if(ai_credit_attention(state,reason))snprintf(row->attention,sizeof(row->attention),"%s",AI_CREDIT_ATTENTION);
    }
    sqlite3_finalize(s);sqlite3_close(db);return (int)n;
}
static const char *ai_origin(const char *id){return !strncmp(id,"account-operator-",17)?"Assistant":!strncmp(id,"account-fabric-",15)?"Agent":!strncmp(id,"account-workgraph-",18)?"Pipeline":"Work";}
static void ai_execution_credit(const char *context,const AiWork *w){
    fputs("<li class=\"ak-inbox-item\" data-needs=\"you\" data-inbox-kind=\"execution-credit\" id=\"",stdout);html(w->id);fputs("\"><span class=\"ak-inbox-mark\" aria-hidden=\"true\">",stdout);page_glyph("warning");fputs("</span><div class=\"ak-inbox-body\">",stdout);
    ai_meta("Needs you","Execution credit",w->updated);fputs("<h3>",stdout);html(*w->title?w->title:"Execution needs credit");fputs("</h3><p class=\"ak-inbox-warn\">",stdout);html(w->attention);fputs("</p><div class=\"ak-inbox-acts\"><a class=\"ak-inbox-signin\" data-inbox-credit href=\"https://platform.openai.com/settings/organization/billing/\" target=\"_blank\" rel=\"noopener noreferrer\">Add OpenAI API credits</a><a href=\"/work-board?context=",stdout);url_query_value(context);fputs("&amp;task=",stdout);url_query_value(w->id);fputs("\">View work</a></div></div></li>",stdout);
}
static void ai_agent_row(const char *context,const AiWork *w,int halted){
    fputs("<li class=\"ak-inbox-agent\" data-needs=\"agent\" data-state=\"",stdout);html(w->state);fputs("\"><span class=\"ak-inbox-pulse\" aria-hidden=\"true\"></span><div><p class=\"ak-inbox-meta\"><span class=\"ak-inbox-who\" data-who=\"",stdout);
    fputs(halted?"stopped\">Agent stopped":!strcmp(w->state,"retry_wait")?"agent\">Agent retrying":"agent\">Agent working",stdout);fputs("</span><span>",stdout);
    html(*w->worker&&strcmp(w->worker,"fabric")&&strcmp(w->worker,"workgraph")?w->worker:ai_origin(w->id));fputs("</span>",stdout);if(w->updated>0){fputs("<time>",stdout);ai_ago(w->updated);fputs("</time>",stdout);}
    fputs("</p><a href=\"/work-board?context=",stdout);url_query_value(context);fputs("&amp;task=",stdout);url_query_value(w->id);fputs("\">",stdout);html(*w->title?w->title:"Untitled work");fputs("</a></div></li>",stdout);
}
typedef struct {char institution[96],label[161],role[16],expires[32];int pending;} AiOrg;
static int ai_table(sqlite3 *db,const char *name){sqlite3_stmt *q=NULL;int found=0;if(sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,name,-1,SQLITE_STATIC);found=sqlite3_step(q)==SQLITE_ROW;}sqlite3_finalize(q);return found;}
/* Invitations addressed to this person's email, and organizations they run
 * with the invitations still open there. Read-only over the Auth store. */
static void ai_orgs(const char *auth_db,long user,AiOrg *invites,int *ni,AiOrg *admin,int *na,int *member_n){
    sqlite3 *db=NULL;sqlite3_stmt *q=NULL;char now[32];time_t t=time(NULL);struct tm tm;*ni=*na=*member_n=0;
    if(!auth_db||!gmtime_r(&t,&tm)||sqlite3_open_v2(auth_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){sqlite3_close(db);return;}
    strftime(now,sizeof(now),"%Y-%m-%dT%H:%M:%SZ",&tm);sqlite3_busy_timeout(db,1000);
    int has_invites=ai_table(db,"institution_invitations"),has_origins=ai_table(db,"institution_origins");
    if(has_invites&&sqlite3_prepare_v2(db,has_origins?"SELECT i.institution,COALESCE(o.label,i.institution),i.role,i.expires_at FROM institution_invitations i JOIN users u ON lower(u.email)=lower(i.email) LEFT JOIN institution_origins o ON o.institution=i.institution "
        "WHERE u.id=?1 AND i.state='pending' AND i.expires_at>?2 AND NOT EXISTS(SELECT 1 FROM institution_grants g WHERE g.user_id=u.id AND g.institution=i.institution AND g.active=1) ORDER BY i.expires_at LIMIT 10"
        :"SELECT i.institution,i.institution,i.role,i.expires_at FROM institution_invitations i JOIN users u ON lower(u.email)=lower(i.email) WHERE u.id=?1 AND i.state='pending' AND i.expires_at>?2 ORDER BY i.expires_at LIMIT 10",-1,&q,NULL)==SQLITE_OK){
        sqlite3_bind_int64(q,1,user);sqlite3_bind_text(q,2,now,-1,SQLITE_STATIC);
        while(*ni<10&&sqlite3_step(q)==SQLITE_ROW){AiOrg *o=&invites[(*ni)++];memset(o,0,sizeof(*o));for(int c=0;c<4;c++){const char *v=(const char*)sqlite3_column_text(q,c);char *dst=c==0?o->institution:c==1?o->label:c==2?o->role:o->expires;size_t cap=c==0?sizeof(o->institution):c==1?sizeof(o->label):c==2?sizeof(o->role):sizeof(o->expires);snprintf(dst,cap,"%s",v?v:"");}}
    }
    sqlite3_finalize(q);q=NULL;
    if(sqlite3_prepare_v2(db,"SELECT g.institution,g.role FROM institution_grants g WHERE g.user_id=?1 AND g.active=1 AND (g.expires_at IS NULL OR g.expires_at='' OR g.expires_at>=?2) ORDER BY g.role='admin' DESC,g.institution LIMIT 60",-1,&q,NULL)==SQLITE_OK){
        sqlite3_bind_int64(q,1,user);sqlite3_bind_text(q,2,now,-1,SQLITE_STATIC);
        while(sqlite3_step(q)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(q,0),*role=(const char*)sqlite3_column_text(q,1);if(!id||!role)continue;
            if(strcmp(role,"admin")||*na>=12){(*member_n)++;continue;}AiOrg *o=&admin[(*na)++];memset(o,0,sizeof(*o));snprintf(o->institution,sizeof(o->institution),"%s",id);snprintf(o->label,sizeof(o->label),"%s",id);snprintf(o->role,sizeof(o->role),"admin");}
    }
    sqlite3_finalize(q);q=NULL;
    for(int i=0;i<*na;i++){
        if(has_origins&&sqlite3_prepare_v2(db,"SELECT label FROM institution_origins WHERE institution=?1",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,admin[i].institution,-1,SQLITE_STATIC);if(sqlite3_step(q)==SQLITE_ROW&&sqlite3_column_text(q,0))snprintf(admin[i].label,sizeof(admin[i].label),"%s",sqlite3_column_text(q,0));}
        sqlite3_finalize(q);q=NULL;
        if(has_invites&&sqlite3_prepare_v2(db,"SELECT count(*) FROM institution_invitations WHERE institution=?1 AND state='pending' AND expires_at>?2",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,admin[i].institution,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,now,-1,SQLITE_STATIC);if(sqlite3_step(q)==SQLITE_ROW)admin[i].pending=sqlite3_column_int(q,0);}
        sqlite3_finalize(q);q=NULL;
    }
    sqlite3_close(db);
}
static void ai_style(void){
    fputs("<style id=\"account-inbox\">"
    ".ak-inbox{--ib-night:#111913;--ib-forest:#173d33;--ib-lime:#b5df32;--ib-signal:#ef6547;--ib-ink:#203023;--ib-muted:#5d6b58;--ib-rule:#2030231f;--ib-paper:#e6edcb;color:var(--ib-ink);font-family:'DM Sans',system-ui,sans-serif}"
    ".pack-workspace:has(>.ak-inbox){padding-top:24px}"
    ".ak-inbox-mast{margin:0;border-radius:32px;padding:clamp(32px,5vw,64px) clamp(22px,4vw,56px) 30px;background:radial-gradient(ellipse at 88% 0%,#4b7250,transparent 62%),radial-gradient(ellipse at 0% 100%,#173d33,transparent 70%),var(--ib-night);color:var(--ib-paper);display:grid;grid-template-columns:minmax(0,1fr) auto;gap:22px 40px;align-items:end}"
    ".ak-inbox-kicker{font:500 11px/1.4 'DM Mono',monospace;letter-spacing:.16em;text-transform:uppercase;color:var(--ib-lime);margin:0 0 14px;display:flex;align-items:center;gap:10px}.ak-inbox-kicker .ak-icon{width:18px;height:18px}"
    ".pack-workspace .ak-inbox-mast h1{font:400 clamp(44px,6.4vw,92px)/.95 'Instrument Serif',Georgia,serif;letter-spacing:-.045em;max-width:none;margin:0 0 16px;color:#f5f7ee}.ak-inbox-mast h1 em{font-style:normal;color:var(--ib-lime)}"
    ".ak-inbox-mast>div>p:last-child{max-width:60ch;margin:0;font-size:17px;line-height:1.55;color:#c9d4bd}"
    ".ak-inbox-nav{display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end}.ak-inbox-nav a{display:inline-flex;align-items:center;gap:7px;min-height:44px;padding:0 16px;border-radius:999px;border:1px solid #e6edcb55;color:var(--ib-paper);text-decoration:none;font-size:14px;transition:background .2s,color .2s}.ak-inbox-nav a:hover,.ak-inbox-nav a:focus-visible{background:var(--ib-lime);color:var(--ib-night);border-color:var(--ib-lime)}.ak-inbox-nav .ak-icon{width:16px;height:16px}"
    ".ak-inbox-tally{grid-column:1/-1;display:flex;flex-wrap:wrap;gap:0;margin:14px 0 0;border-top:1px solid #e6edcb2b;padding-top:18px}.ak-inbox-tally a{display:grid;gap:2px;padding:0 28px 0 0;margin-right:28px;border-right:1px solid #e6edcb2b;color:inherit;text-decoration:none}.ak-inbox-tally a:last-child{border:0}.ak-inbox-tally strong{font:400 34px/1 'Instrument Serif',Georgia,serif;color:#f5f7ee}.ak-inbox-tally a[data-tally=you] strong{color:var(--ib-signal)}.ak-inbox-tally a[data-tally=agents] strong{color:var(--ib-lime)}.ak-inbox-tally span{font:500 10px/1.4 'DM Mono',monospace;letter-spacing:.12em;text-transform:uppercase;color:#a9b89d}"
    ".ak-inbox-receipt{margin:22px 0 0;padding:14px 18px;border-left:4px solid var(--ib-lime);background:#e9f1e4;color:#2b4a31;font-size:15px}"
    ".ak-inbox-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(300px,380px);gap:clamp(28px,4vw,64px);margin-top:38px;align-items:start}"
    ".ak-inbox-group{margin:0 0 46px}.ak-inbox-group>h2{display:flex;align-items:baseline;gap:12px;font:400 clamp(30px,3.2vw,42px)/1 'Instrument Serif',Georgia,serif!important;letter-spacing:-.03em;margin:0 0 6px;color:#14251b}.ak-inbox-group>h2 small{font:500 11px 'DM Mono',monospace;letter-spacing:.12em;text-transform:uppercase;color:var(--ib-muted)}"
    ".ak-inbox-group>p{margin:0 0 14px;color:var(--ib-muted);max-width:62ch;line-height:1.55}"
    ".ak-inbox-list{list-style:none;margin:0;padding:0;border-top:2px solid var(--ib-ink)}"
    ".ak-inbox-item{display:grid;grid-template-columns:44px minmax(0,1fr);gap:16px;padding:22px 0;border-bottom:1px solid var(--ib-rule);scroll-margin-top:90px}.ak-inbox-item:target{background:linear-gradient(90deg,#ef654712,transparent 70%)}"
    ".ak-inbox-mark{display:grid;place-items:center;width:44px;height:44px;border-radius:50%;background:var(--ib-signal);color:#fff}.ak-inbox-mark .ak-icon{width:20px;height:20px}"
    ".ak-inbox-meta{display:flex;flex-wrap:wrap;align-items:center;gap:6px 12px;margin:0 0 6px;font:500 11px/1.4 'DM Mono',monospace;letter-spacing:.06em;text-transform:uppercase;color:var(--ib-muted)}"
    ".ak-inbox-who{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:4px 10px;font-weight:600;letter-spacing:.08em}.ak-inbox-who[data-who=you]{background:var(--ib-signal);color:#fff}.ak-inbox-who[data-who=agent]{background:var(--ib-lime);color:var(--ib-night)}.ak-inbox-who[data-who=stopped]{background:#e6edcb26;color:#e6edcb;border:1px solid #e6edcb44}"
    ".ak-inbox-item h3{font:600 clamp(19px,1.8vw,23px)/1.2 'DM Sans',system-ui,sans-serif;letter-spacing:-.02em;margin:0 0 6px;overflow-wrap:anywhere;color:#14251b}"
    ".ak-inbox-text{margin:0 0 8px;line-height:1.55;color:#3c4b39;display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;overflow:hidden;overflow-wrap:anywhere}"
    ".ak-inbox-warn{margin:0 0 8px;padding-left:12px;border-left:3px solid var(--ib-signal);color:#7a2d1c;font-size:14px;line-height:1.5;overflow-wrap:anywhere}.ak-inbox-note{margin:0 0 8px;font-size:14px;color:var(--ib-muted)}"
    ".ak-inbox-acts{display:flex;flex-wrap:wrap;align-items:center;gap:8px 10px;margin-top:12px}.ak-inbox-acts form{margin:0}"
    ".pack-workspace .ak-inbox-acts button,.ak-inbox-acts>a{display:inline-flex;align-items:center;min-height:44px;padding:0 18px;border-radius:999px;border:1px solid #20302355;background:transparent;color:var(--ib-ink);font:500 14px 'DM Sans',system-ui,sans-serif;text-decoration:none;cursor:pointer;transition:background .2s,color .2s,border-color .2s}"
    ".pack-workspace .ak-inbox-acts button:hover,.ak-inbox-acts>a:hover{border-color:var(--ib-ink);background:#20302310}"
    ".pack-workspace .ak-inbox-acts .ak-inbox-primary,.ak-inbox-acts .ak-inbox-signin{background:var(--ib-night);color:var(--ib-lime);border-color:var(--ib-night);font-weight:600}.pack-workspace .ak-inbox-acts .ak-inbox-primary:hover,.ak-inbox-acts .ak-inbox-signin:hover{background:var(--ib-forest);color:#d8f27a}"
    ".ak-inbox-status{margin:0;padding:10px 16px;border-radius:999px;background:#e9f1e4;color:#2b4a31;font-weight:600;font-size:14px}"
    ".ak-inbox-signins{list-style:none;margin:10px 0 0;padding:0;columns:2 280px;column-gap:28px}.ak-inbox-signins li{break-inside:avoid;display:flex;align-items:center;justify-content:space-between;gap:12px;padding:9px 0;border-bottom:1px solid var(--ib-rule)}.ak-inbox-signins span{display:grid;gap:2px;min-width:0}.ak-inbox-signins small{font-size:12px;color:var(--ib-muted);overflow-wrap:anywhere}"
    ".ak-inbox-signins a{flex:none;display:inline-flex;align-items:center;min-height:36px;padding:0 14px;border-radius:999px;background:var(--ib-night);color:var(--ib-lime);text-decoration:none;font-size:13px;font-weight:600}.ak-inbox-signins a:hover{background:var(--ib-forest)}"
    ".ak-inbox-more{margin-top:6px}.ak-inbox-more summary{cursor:pointer;min-height:44px;display:flex;align-items:center;font-weight:600;color:#2b4a31}"
    ".ak-inbox-subhead{font:500 11px 'DM Mono',monospace;letter-spacing:.14em;text-transform:uppercase;color:var(--ib-muted);margin:30px 0 8px}"
    ".ak-inbox-empty{padding:20px 0;border-bottom:1px solid var(--ib-rule);color:var(--ib-muted);font-size:16px}"
    ".ak-inbox-orgs{list-style:none;margin:0;padding:0;border-top:2px solid var(--ib-ink)}.ak-inbox-orgs>li{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px 18px;padding:16px 0;border-bottom:1px solid var(--ib-rule)}.ak-inbox-orgs strong{font-size:18px}.ak-inbox-orgs small{display:block;color:var(--ib-muted);font-size:13px;margin-top:2px}.ak-inbox-orgs .ak-inbox-acts{margin:0}"
    ".ak-inbox-side{position:sticky;top:24px;margin:0;padding:28px 26px 30px;background:radial-gradient(ellipse at 100% 0%,#4b725099,transparent 60%),var(--ib-forest);color:var(--ib-paper);border-radius:28px}"
    ".ak-inbox-side h2{font:400 30px/1 'Instrument Serif',Georgia,serif!important;letter-spacing:-.02em;margin:0 0 6px;color:#f5f7ee}.ak-inbox-side>p{margin:0 0 14px;font-size:14px;line-height:1.5;color:#bccab0}.ak-inbox-side h3{font:500 11px 'DM Mono',monospace;letter-spacing:.14em;text-transform:uppercase;color:#a9b89d;margin:26px 0 6px}"
    ".ak-inbox-side ol{list-style:none;margin:0;padding:0}.ak-inbox-agent{display:grid;grid-template-columns:14px minmax(0,1fr);gap:10px;padding:12px 0;border-top:1px solid #e6edcb24}.ak-inbox-agent .ak-inbox-meta{color:#a9b89d;margin-bottom:4px;font-size:10px}.ak-inbox-agent a{color:#f5f7ee;text-decoration:none;font-size:15px;line-height:1.35;overflow-wrap:anywhere}.ak-inbox-agent a:hover{text-decoration:underline;text-underline-offset:3px}"
    ".ak-inbox-pulse{width:10px;height:10px;border-radius:50%;background:var(--ib-lime);margin-top:5px;box-shadow:0 0 0 0 #b5df3266;animation:ak-inbox-pulse 1.8s ease-out infinite}.ak-inbox-agent[data-state=blocked] .ak-inbox-pulse,.ak-inbox-agent[data-state=dead_letter] .ak-inbox-pulse{background:#e6edcb55;animation:none}"
    ".ak-inbox-side .ak-inbox-empty{color:#bccab0;border-color:#e6edcb24;font-size:14px}.ak-inbox-side-link{display:inline-flex;align-items:center;min-height:44px;margin-top:16px;padding:0 16px;border-radius:999px;border:1px solid #e6edcb55;color:var(--ib-paper);text-decoration:none;font-size:14px}.ak-inbox-side-link:hover{background:var(--ib-lime);color:var(--ib-night)}"
    "@keyframes ak-inbox-pulse{70%{box-shadow:0 0 0 8px #b5df3200}100%{box-shadow:0 0 0 0 #b5df3200}}"
    "@media(max-width:1000px){.ak-inbox-mast{grid-template-columns:1fr}.ak-inbox-nav{justify-content:flex-start}.ak-inbox-grid{grid-template-columns:1fr}.ak-inbox-side{position:static}}"
    "@media(max-width:620px){.pack-workspace:has(>.ak-inbox){padding-inline:16px}.ak-inbox-mast{margin-inline:-16px;border-radius:0 0 28px 28px;padding-inline:16px}.ak-inbox-tally a{padding-right:16px;margin-right:16px}.ak-inbox-tally strong{font-size:28px}.ak-inbox-item{grid-template-columns:1fr;gap:10px}.ak-inbox-mark{width:36px;height:36px}.ak-inbox-side{margin-inline:-16px;border-radius:0;padding-inline:16px}.ak-inbox-acts>*,.ak-inbox-acts form button{flex:1 1 auto;justify-content:center}.ak-inbox-acts form{flex:1 1 auto;display:flex}}"
    "@media(prefers-reduced-motion:reduce){.ak-inbox-pulse{animation:none}}"
    "</style>",stdout);
}
/* Read-only rail projection of the same person-work and invitation owners as
 * /inbox. It never converts an actionable goal into a recipient notification. */
static int ai_pack_need(bf_mp_writer_t *w,const char *id,const char *kind,const char *title,long long updated){
    return !bf_mp_pack_map(w,4)&&!bf_mp_pack_kv_str(w,"id",id)&&
        !bf_mp_pack_kv_str(w,"kind",kind)&&!bf_mp_pack_kv_str(w,"title",title)&&
        !bf_mp_pack_str_cstr(w,"updated")&&!bf_mp_pack_uint(w,updated>0?(uint64_t)updated:0);
}
static int serve_account_needs_summary(const char *method,const char *query,const char *bearer,const char *auth_db){
    char context[96],done[16],actor[160],subject[160];const char *scope=NULL;
    if(strcmp(method,"GET")||!ai_query(query,context,done)||*done){aw_error(400,"Choose one current account Inbox destination.\n");return 1;}
    if(psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1){
        aw_error(403,"Current account access is required to read Inbox actions.\n");return 1;
    }
    AcPersonGoal *people=calloc(AC_PERSON_MAX,sizeof(*people));
    AiWork *agents=calloc(AI_AGENT_MAX,sizeof(*agents)),*halted=calloc(4,sizeof(*halted)),*credit=calloc(AI_AGENT_MAX,sizeof(*credit));
    AiOrg *invites=calloc(10,sizeof(*invites)),*admin=calloc(12,sizeof(*admin));
    if(!people||!agents||!halted||!credit||!invites||!admin){
        free(people);free(agents);free(halted);free(credit);free(invites);free(admin);
        aw_error(503,"The account Inbox sources could not be prepared.\n");return 1;
    }
    int running=0,stopped=0,waiting=0,halted_n=0,credit_n=0,ni=0,na=0,members=0;
    int person_count=ac_person_goals(subject,scope?scope:"",people,AC_PERSON_MAX);
    int agent_count=ai_agent_work(subject,scope?scope:"",agents,AI_AGENT_MAX,&running,&stopped,&waiting,
        people,person_count>0?person_count:0,halted,4,&halted_n,credit,AI_AGENT_MAX,&credit_n);
    ai_orgs(auth_db,strtol(actor+5,NULL,10),invites,&ni,admin,&na,&members);
    if(person_count<0||agent_count<0){aw_error(503,"The account Inbox actions could not be confirmed from native owners.\n");goto done;}
    bf_mp_writer_t w;bf_mp_writer_init(&w,65536);
    int ok=!bf_mp_pack_array(&w,3)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_bool(&w,1)&&
        !bf_mp_pack_map(&w,3)&&!bf_mp_pack_kv_str(&w,"context",context)&&
        !bf_mp_pack_str_cstr(&w,"binding")&&!bf_mp_pack_map(&w,3)&&
        !bf_mp_pack_kv_str(&w,"actor",actor)&&!bf_mp_pack_kv_str(&w,"subject",subject)&&
        !bf_mp_pack_kv_str(&w,"scope",scope?scope:"")&&
        !bf_mp_pack_str_cstr(&w,"items")&&!bf_mp_pack_array(&w,(uint32_t)(person_count+credit_n+ni));
    for(int i=0;i<person_count&&ok;i++){
        const char *kind=!strcmp(people[i].source,"release-approval")?"release":
            !strcmp(people[i].source,"provider-estate-checklist")?"provider-checklist":"provider";
        ok=ai_pack_need(&w,people[i].id,kind,people[i].title,(long long)(people[i].at*1000.0));
    }
    for(int i=0;i<credit_n&&ok;i++)ok=ai_pack_need(&w,credit[i].id,"execution-credit",credit[i].title,credit[i].updated);
    for(int i=0;i<ni&&ok;i++){
        char id[128],title[192];snprintf(id,sizeof(id),"invitation:%s",invites[i].institution);
        snprintf(title,sizeof(title),"Join %s",invites[i].label);
        ok=ai_pack_need(&w,id,"invitation",title,0);
    }
    if(ok&&w.len<=65536)aw_response(200,"application/msgpack",w.buf,w.len,0);
    else aw_error(503,"The account Inbox actions could not be encoded.\n");
    bf_mp_writer_free(&w);
done:
    ac_person_free(people,person_count>0?(size_t)person_count:0);
    free(people);free(agents);free(halted);free(credit);free(invites);free(admin);return 1;
}
static int serve_account_inbox(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db){
    if(!strcmp(target,"/inbox/_summary"))return serve_account_needs_summary(method,query,bearer,auth_db);
    if(strcmp(target,"/inbox")&&strcmp(target,"/inbox/"))return 0;
    char context[96],done[16],actor[160],subject[160];const char *scope=NULL;
    /* A query this page does not own, or no session, is the public surface's. */
    if(strcmp(method,"GET")||!ai_query(query,context,done))return 0;
    int access=psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),subject,sizeof(subject),&scope);
    if(access==0)return 0;if(access!=1){aw_error(403,"This account cannot open that inbox.\n");return 1;}
    long user=strtol(actor+5,NULL,10);
    AcPersonGoal *people=calloc(AC_PERSON_MAX,sizeof(*people));AiWork *agents=calloc(AI_AGENT_MAX,sizeof(*agents)),*halted=calloc(4,sizeof(*halted)),*credit=calloc(AI_AGENT_MAX,sizeof(*credit));AiOrg *invites=calloc(10,sizeof(*invites)),*admin=calloc(12,sizeof(*admin));
    if(!people||!agents||!halted||!credit||!invites||!admin){free(people);free(agents);free(halted);free(credit);free(invites);free(admin);aw_error(503,"The inbox could not be prepared.\n");return 1;}
    int person_count=ac_person_goals(subject,scope?scope:"",people,AC_PERSON_MAX),running=0,stopped=0,waiting=0,halted_n=0,credit_n=0,ni=0,na=0,members=0;
    int agent_count=ai_agent_work(subject,scope?scope:"",agents,AI_AGENT_MAX,&running,&stopped,&waiting,people,person_count>0?person_count:0,halted,4,&halted_n,credit,AI_AGENT_MAX,&credit_n);
    ai_orgs(auth_db,user,invites,&ni,admin,&na,&members);
    int needs=(person_count>0?person_count:0)+credit_n+ni,direct=credit_n,checklists=0;for(int i=0;i<person_count;i++){if(!strcmp(people[i].source,"provider-estate-checklist"))checklists++;else direct++;}
    char label[160]="My personal workspace";{PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;if(psetup_destinations(bearer,auth_db,rows,&count)>0)for(size_t i=0;i<count;i++)if(!strcmp(rows[i].context,context)){snprintf(label,sizeof(label),"%s",rows[i].label);break;}}
    psetup_start(200,"Inbox");ai_style();
    fputs("<section class=\"ak-inbox\" data-account-inbox data-context=\"",stdout);html(context);printf("\" data-needs-count=\"%d\" data-agent-count=\"%d\"><header class=\"ak-inbox-mast\"><div><p class=\"ak-inbox-kicker\">",needs,running);page_glyph("bell");fputs("Inbox · ",stdout);html(label);fputs("</p><h1>",stdout);
    if(needs)printf("<em>%d</em> %s you",needs,needs==1?"thing needs":"things need");else if(person_count<0||agent_count<0)fputs("Inbox needs a source connection",stdout);else fputs("Nothing needs you right now",stdout);
    fputs("</h1><p>Approvals, sign-ins and account steps only you can take are here, each with its one next step. Everything the agents are doing stays on the right, so you can tell the two apart at a glance.</p></div><nav class=\"ak-inbox-nav\" aria-label=\"Related pages\"><a href=\"/work-board?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("board");fputs("Work board</a><a href=\"/organizations\">",stdout);page_glyph("group");fputs("Organizations</a><a href=\"/apps/connections?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("link");fputs("Connections</a></nav>",stdout);
    printf("<p class=\"ak-inbox-tally\"><a href=\"#needs-you\" data-tally=\"you\"><strong>%d</strong><span>Needs you</span></a><a href=\"#agents\" data-tally=\"agents\"><strong>%d</strong><span>Agents working</span></a><a href=\"#agents-stopped\" data-tally=\"stopped\"><strong>%d</strong><span>Agent work stopped</span></a><a href=\"/work-board?context=",needs,running,stopped);url_query_value(context);printf("\" data-tally=\"waiting\"><strong>%d</strong><span>Waiting its turn</span></a></p></header>",waiting);
    if(person_count<0||agent_count<0)fputs("<p role=\"alert\" data-inbox-source-unavailable>The account Inbox cannot read all native work owners on this service. It has not verified that your work is empty. <a href=\"/work-board?context=self\">Check the Workboard</a> and its account connection.</p>",stdout);
    if(*done){fputs("<p class=\"ak-inbox-receipt\" role=\"status\" data-inbox-receipt>",stdout);
        fputs(!strcmp(done,"approve")?"Approved. The release is being put live; its item below shows how that goes.":!strcmp(done,"later")?"Set aside. It stays on the work board and comes back when you want it.":!strcmp(done,"done")?"Marked as set up. Thank you.":"Recorded. The item keeps where it stands.",stdout);fputs("</p>",stdout);}
    fputs("<div class=\"ak-inbox-grid\"><div class=\"ak-inbox-main\"><section class=\"ak-inbox-group\" id=\"needs-you\" data-inbox-group=\"needs-you\"><h2>Needs you",stdout);if(direct)printf(" <small>%d</small>",direct);fputs("</h2><ol class=\"ak-inbox-list\">",stdout);
    for(int i=0;i<credit_n;i++)ai_execution_credit(context,&credit[i]);
    for(int i=0;i<person_count;i++){if(!strcmp(people[i].source,"release-approval"))ai_release(context,&people[i]);else if(!strcmp(people[i].source,"provider-estate"))ai_provider(context,&people[i]);}
    if(!direct){fputs("<li class=\"ak-inbox-empty\">",stdout);fputs(person_count<0?"Approvals are not connected on this site.":(ni||checklists)?"No approvals are waiting. Your sub-account steps are just below.":"No approvals or sign-ins are waiting on you.",stdout);fputs("</li>",stdout);}
    fputs("</ol></section><section class=\"ak-inbox-group\" id=\"sub-accounts\" data-inbox-group=\"sub-accounts\"><h2>Sub-accounts",stdout);if(ni+checklists)printf(" <small>%d need you</small>",ni+checklists);
    fputs("</h2><p>Organizations you belong to or run, invitations waiting for you, and the provider accounts your organizations can open.</p><ol class=\"ak-inbox-list\">",stdout);
    for(int i=0;i<ni;i++){
        fputs("<li class=\"ak-inbox-item\" data-needs=\"you\" data-inbox-kind=\"invitation\"><span class=\"ak-inbox-mark\" aria-hidden=\"true\">",stdout);page_glyph("user");fputs("</span><div class=\"ak-inbox-body\">",stdout);ai_meta("Needs you","Invitation",0);
        fputs("<h3>Join ",stdout);html(invites[i].label);fputs("</h3><p class=\"ak-inbox-text\">You were invited with ",stdout);html(invites[i].role);fputs(" access. Open the private link you were sent, or paste its code. It expires ",stdout);{static const char *const months[]={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};int y=0,mo=0,d=0;if(sscanf(invites[i].expires,"%4d-%2d-%2d",&y,&mo,&d)==3&&mo>=1&&mo<=12)printf("%d %s %d",d,months[mo-1],y);else html(invites[i].expires);}fputs(".</p><div class=\"ak-inbox-acts\"><a class=\"ak-inbox-signin\" href=\"/organizations/join\">Open your invitation</a></div></div></li>",stdout);
    }
    for(int i=0;i<person_count;i++)if(!strcmp(people[i].source,"provider-estate-checklist"))ai_checklist(context,&people[i]);
    fputs("</ol>",stdout);
    if(na){fputs("<h3 class=\"ak-inbox-subhead\">Organizations you run</h3><ul class=\"ak-inbox-orgs\" data-inbox-orgs>",stdout);
        for(int i=0;i<na;i++){fputs("<li><div><strong>",stdout);html(admin[i].label);fputs("</strong><small>",stdout);
            if(admin[i].pending)printf("%d invitation%s not accepted yet",admin[i].pending,admin[i].pending==1?"":"s");else fputs("Nobody is waiting on an invitation",stdout);
            fputs("</small></div><div class=\"ak-inbox-acts\"><a href=\"/organizations?organization=",stdout);url_query_value(admin[i].institution);fputs("\">People and provider accounts</a><a href=\"/apps?context=institution%3A",stdout);url_query_value(admin[i].institution);fputs("\">Shared work</a></div></li>",stdout);}
        fputs("</ul>",stdout);}
    fputs("<div class=\"ak-inbox-acts\"><a href=\"/organizations\">",stdout);
    if(members)printf("All your organizations (%d more)",members);else fputs(na?"All your organizations":"Start or join an organization",stdout);fputs("</a></div></section></div>",stdout);
    fputs("<aside class=\"ak-inbox-side\" id=\"agents\" data-inbox-group=\"agents\" aria-labelledby=\"ak-inbox-agents\"><h2 id=\"ak-inbox-agents\">Agents working</h2><p>The assistant, your agents and site checks. Nothing here is waiting on you.</p><ol>",stdout);
    for(int i=0;i<agent_count;i++)ai_agent_row(context,&agents[i],0);
    if(agent_count<=0){fputs("<li class=\"ak-inbox-empty\">",stdout);fputs(agent_count<0?"Agent work is not connected on this site.":"No agent is working right now.",stdout);fputs("</li>",stdout);}
    if(running>agent_count&&agent_count>0)printf("<li class=\"ak-inbox-empty\">%d more on the work board.</li>",running-agent_count);
    fputs("</ol><h3 id=\"agents-stopped\">Agent work that stopped",stdout);if(stopped)printf(" · %d",stopped);fputs("</h3><ol data-inbox-group=\"agents-stopped\">",stdout);
    for(int i=0;i<halted_n;i++)ai_agent_row(context,&halted[i],1);if(!halted_n)fputs("<li class=\"ak-inbox-empty\">None.</li>",stdout);
    fputs("</ol><a class=\"ak-inbox-side-link\" href=\"/work-board?context=",stdout);url_query_value(context);fputs("\">Everything on the work board</a></aside></div></section>",stdout);
    psetup_end();
    ac_person_free(people,person_count>0?(size_t)person_count:0);free(people);free(agents);free(halted);free(credit);free(invites);free(admin);return 1;
}
#endif
