#ifndef BONFYRE_ACCOUNT_INBOX_WEB_H
#define BONFYRE_ACCOUNT_INBOX_WEB_H
#include "form_tree.h"
#include "web_store.h"
#include "../generated/native_estate_hosts.h"
#include "account_inbox_dating.h"
#include "../../BonfyreQueue/src/account_legacy_api.h"
#include "../../BonfyreParticipation/src/dating.h"
#include "../../BonfyreParticipation/src/peer_facets.h"
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
#define AI_CREDIT_ATTENTION BONFYRE_LEGACY_API_ATTENTION
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
    static const char *const own[]={BF_ORIGIN_WORK "/",BF_ORIGIN_LIFE "/",BF_ORIGIN_LIVE "/",BF_ORIGIN_SHOP "/"};
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
    const FtField hidden[]={{"context",context},{"task",task},{"action",action},{"back","inbox"}};
    const FtSubmit go[]={{.label=label,.tone=primary?NULL:"quiet",.hook="inbox-act",.hook_value=action}};
    const FtForm f={.id="inbox-act",.action="/work-board/act",.hidden=hidden,.nhidden=4,.submits=go,.nsubmits=1};
    ft_form(stdout,&f);
}
static void ai_meta(const char *who,const char *kind,long long ms){
    ui_open(stdout,"p","ak-inbox-meta"); ui_open_a(stdout,"span","class","ak-inbox-who","data-who","you");html(who);ui_close(stdout,"span"); ui_open(stdout,"span",NULL);html(kind);ui_close(stdout,"span");
    if(ms>0){ui_open(stdout,"time",NULL);ai_ago(ms);ui_close(stdout,"time");}ui_close(stdout,"p");
}
static int ai_approved(const char *release){
    const char *idx=getenv("BONFYRE_ACCOUNT_WORK_DB"),*slash=idx?strrchr(idx,'/'):NULL;char path[PATH_MAX];
    if(!slash||!*release||snprintf(path,sizeof(path),"%.*s/approvals/%s.approved",(int)(slash-idx),idx,release)>=(int)sizeof(path))return 0;
    return access(path,F_OK)==0;
}
static void ai_release(const char *context,const AcPersonGoal *g){
    char changes[700];int approved=ai_approved(g->release);
    ui_open_a(stdout,"li","class","ak-inbox-item","data-needs","you","data-inbox-kind","release",UI_DYN,"id","",(g->id)); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");page_glyph("check");ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body");
    ai_meta(approved?"Approved by you":"Needs you","Release to approve",(long long)(g->at*1000));
    ui_text(stdout, "h3", NULL, g->title);
    if(ai_line(g->details,"What it changes:",changes,sizeof(changes))||ai_line(g->details,NULL,changes,sizeof(changes))){ui_text(stdout, "p", "ak-inbox-text", changes);}
    if(*g->error&&!approved){ui_open(stdout,"p","ak-inbox-warn"); ui_open(stdout,"strong",NULL); fputs("Last try:",stdout); ui_close(stdout,"strong"); fputs(" ",stdout);ai_sentence(g->error,220);ui_close(stdout,"p");}
    ui_open(stdout,"div","ak-inbox-acts");
    if(approved)do{ ui_open_a(stdout,"p","class","ak-inbox-status","role","status"); fputs("Going live now. This updates when the release reports back.",stdout); ui_close(stdout,"p"); }while(0);
    else{ai_act(context,g->id,"approve","Approve and put live",1);ai_act(context,g->id,"later","Not now",0);}
    ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_copy(stdout,"&amp;task=");url_query_value(g->id);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Read the full release note");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"li");
}
static void ai_provider(const char *context,const AcPersonGoal *g){
    char first[400],url[400]="";const char *u=strstr(g->details,"https://");
    if(u){size_t n=strcspn(u," \t\r\n<>\"'()");while(n>8&&strchr(".,;:",u[n-1]))n--;if(n<sizeof(url)){memcpy(url,u,n);url[n]=0;}}
    ui_open_a(stdout,"li","class","ak-inbox-item","data-needs","you","data-inbox-kind","provider",UI_DYN,"id","",(g->id)); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");page_glyph("login");ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body");
    ai_meta("Needs you","Provider account step",(long long)(g->at*1000));
    ui_text(stdout, "h3", NULL, g->title);
    if(ai_line(g->details,NULL,first,sizeof(first))){ui_text(stdout, "p", "ak-inbox-text", first);}
    if(*g->error){ui_text(stdout, "p", "ak-inbox-note", g->error);}
    ui_open(stdout,"div","ak-inbox-acts");
    if(*url){ui_start(stdout,"a");ui_attr(stdout,"class","ak-inbox-signin");ui_flag(stdout,"data-inbox-signin");ui_attr_open(stdout,"href");ai_href(url);if(ai_external(url)){ui_attr_close(stdout);ui_attr(stdout,"target","_blank");ui_attr(stdout,"rel","noopener noreferrer");ui_gt(stdout);ui_copy(stdout,"Sign in and make the key");}else{ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Connect");}ui_close(stdout,"a");}
    ai_act(context,g->id,"done","Done, it's set up",!*url);ai_act(context,g->id,"email","Waiting on email or phone",0);ai_act(context,g->id,"review","Waiting on the provider",0);ai_act(context,g->id,"later","Not now",0);
    ui_close(stdout,"div"); ui_close(stdout,"div"); ui_close(stdout,"li");
}
/* The provider checklist: one goal whose "Not connected yet" lines are each a
 * sign-in only the account holder can make. */
static void ai_checklist(const char *context,const AcPersonGoal *g){
    const char *section=strstr(g->details,"\nNot connected yet");if(!section&&!strncmp(g->details,"Not connected yet",17))section=g->details;
    ui_open_a(stdout,"li","class","ak-inbox-item","data-needs","you","data-inbox-kind","provider-checklist",UI_DYN,"id","",(g->id)); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");page_glyph("group");ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body");
    ai_meta("Needs you","Customer accounts at providers",(long long)(g->at*1000));
    ui_open(stdout,"h3",NULL);html(g->title);ui_close(stdout,"h3"); ui_text(stdout,"p","ak-inbox-text","Each provider below needs you to sign in once and make a key. After that, organizations can open and close their own accounts there.");
    if(section){
        const char *p=strchr(section+1,'\n');unsigned shown=0,total=0;
        for(const char *q=p;q&&*q;){q++;const char *end=strchr(q,'\n');size_t n=end?(size_t)(end-q):strlen(q);if(!n)break;{char probe[600];if(n<sizeof(probe)){memcpy(probe,q,n);probe[n]=0;if(strstr(probe,": https://"))total++;}}q=end;}
        ui_open_a(stdout,"ul","class","ak-inbox-signins","data-inbox-signins",(const char *)NULL);
        for(const char *q=p;q&&*q;){
            q++;const char *end=strchr(q,'\n');size_t n=end?(size_t)(end-q):strlen(q);if(!n)break;char line[600];if(n>=sizeof(line)){q=end;continue;}
            memcpy(line,q,n);line[n]=0;q=end;char *link=strstr(line,": https://");if(!link)continue;*link=0;link+=2;
            char *gives=strstr(line," (gives customers ");if(gives){*gives=0;gives+=18;size_t m=strlen(gives);if(m&&gives[m-1]==')')gives[m-1]=0;}
            if(shown==6&&total>6){ui_close(stdout,"ul");ui_open_a(stdout,"details","class","ak-inbox-more");ui_open(stdout,"summary",NULL);printf("%u more providers to sign in to",total-6);ui_close(stdout,"summary");ui_open(stdout,"ul","ak-inbox-signins");}
            ui_open(stdout,"li",NULL); ui_open(stdout,"span",NULL); ui_open(stdout,"strong",NULL);html(line);ui_close(stdout,"strong");if(gives){ui_open(stdout,"small",NULL); fputs("Customers get: ",stdout);html(gives);ui_close(stdout,"small");}
            ui_close(stdout,"span");ui_start(stdout,"a");ui_flag(stdout,"data-inbox-signin");ui_attr_open(stdout,"href");ai_href(link);if(ai_external(link)){ui_attr_close(stdout);ui_attr(stdout,"target","_blank");ui_attr(stdout,"rel","noopener noreferrer");ui_gt(stdout);ui_copy(stdout,"Sign in");}else{ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Connect");}ui_close(stdout,"a");ui_close(stdout,"li");shown++;
        }
        fputs(shown>6?"</ul></details>":"</ul>",stdout);
    }
    ui_open(stdout,"div","ak-inbox-acts");ui_open_a(stdout,"a","href","/connect/github-app");ui_copy(stdout,"Connect GitHub");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/apps/connections?context=");url_query_value(context);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Your connections");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_copy(stdout,"&amp;task=");url_query_value(g->id);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Full checklist");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"li");
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
    if(bf_store_open(index, &db, SQLITE_OPEN_READONLY, &bf_store_policy_account_work)!=SQLITE_OK){sqlite3_close(db);return -1;}sqlite3_busy_timeout(db,1000);
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
    ui_open_a(stdout,"li","class","ak-inbox-item","data-needs","you","data-inbox-kind","legacy-api-hold",UI_DYN,"id","",(w->id)); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");page_glyph("warning");ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body");
    ai_meta("Needs review","Discontinued API",w->updated);ui_open(stdout,"h3",NULL);html(*w->title?w->title:"Stopped legacy API work");ui_close(stdout,"h3"); ui_open(stdout,"p","ak-inbox-warn");html(w->attention);ui_close(stdout,"p");ui_open(stdout,"div","ak-inbox-acts");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/programs?context=");url_query_value(context);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open native programs");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_copy(stdout,"&amp;task=");url_query_value(w->id);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Review retained work");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"li");
}
static void ai_agent_row(const char *context,const AiWork *w,int halted){
    ui_start(stdout,"li");ui_attr(stdout,"class","ak-inbox-agent");ui_attr(stdout,"data-needs","agent");ui_attr_open(stdout,"data-state");html(w->state);ui_attr_close(stdout);ui_gt(stdout);ui_open_a(stdout,"span","class","ak-inbox-pulse","aria-hidden","true");ui_close(stdout,"span");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ak-inbox-meta");ui_start(stdout,"span");ui_attr(stdout,"class","ak-inbox-who");ui_attr_open(stdout,"data-who");
    fputs(halted?"stopped\">Agent stopped":!strcmp(w->state,"retry_wait")?"agent\">Agent retrying":"agent\">Agent working",stdout);ui_close(stdout,"span"); ui_open(stdout,"span",NULL);
    html(*w->worker&&strcmp(w->worker,"fabric")&&strcmp(w->worker,"workgraph")?w->worker:ai_origin(w->id));ui_close(stdout,"span");if(w->updated>0){ui_open(stdout,"time",NULL);ai_ago(w->updated);ui_close(stdout,"time");}
    ui_close(stdout,"p");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_copy(stdout,"&amp;task=");url_query_value(w->id);ui_attr_close(stdout);ui_gt(stdout);html(*w->title?w->title:"Untitled work");ui_close(stdout,"a"); ui_close(stdout,"div"); ui_close(stdout,"li");
}
typedef struct {char institution[96],label[161],role[16],expires[32];int pending;} AiOrg;
static int ai_table(sqlite3 *db,const char *name){sqlite3_stmt *q=NULL;int found=0;if(sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,name,-1,SQLITE_STATIC);found=sqlite3_step(q)==SQLITE_ROW;}sqlite3_finalize(q);return found;}
/* Invitations addressed to this person's email, and organizations they run
 * with the invitations still open there. Read-only over the Auth store. */
static void ai_orgs(const char *auth_db,long user,AiOrg *invites,int *ni,AiOrg *admin,int *na,int *member_n){
    sqlite3 *db=NULL;sqlite3_stmt *q=NULL;char now[32];time_t t=time(NULL);struct tm tm;*ni=*na=*member_n=0;
    if(!auth_db||!gmtime_r(&t,&tm)||bf_store_open(auth_db, &db, SQLITE_OPEN_READONLY, &bf_store_policy_auth)!=SQLITE_OK){sqlite3_close(db);return;}
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
    int dating_pending=scope&&!strcmp(scope,"self")?ai_dating_pending(auth_db,strtol(actor+5,NULL,10)):0;
    if(person_count<0||agent_count<0){aw_error(503,"The account Inbox actions could not be confirmed from native owners.\n");goto done;}
    bf_mp_writer_t w;bf_mp_writer_init(&w,65536);
    int ok=!bf_mp_pack_array(&w,3)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_bool(&w,1)&&
        !bf_mp_pack_map(&w,3)&&!bf_mp_pack_kv_str(&w,"context",context)&&
        !bf_mp_pack_str_cstr(&w,"binding")&&!bf_mp_pack_map(&w,3)&&
        !bf_mp_pack_kv_str(&w,"actor",actor)&&!bf_mp_pack_kv_str(&w,"subject",subject)&&
        !bf_mp_pack_kv_str(&w,"scope",scope?scope:"")&&
        !bf_mp_pack_str_cstr(&w,"items")&&!bf_mp_pack_array(&w,(uint32_t)(person_count+credit_n+ni+(dating_pending>0?1:0)));
    for(int i=0;i<person_count&&ok;i++){
        const char *kind=!strcmp(people[i].source,"release-approval")?"release":
            !strcmp(people[i].source,"provider-estate-checklist")?"provider-checklist":"provider";
        ok=ai_pack_need(&w,people[i].id,kind,people[i].title,(long long)(people[i].at*1000.0));
    }
    for(int i=0;i<credit_n&&ok;i++)ok=ai_pack_need(&w,credit[i].id,"legacy-api-hold",credit[i].title,credit[i].updated);
    for(int i=0;i<ni&&ok;i++){
        char id[128],title[192];snprintf(id,sizeof(id),"invitation:%s",invites[i].institution);
        snprintf(title,sizeof(title),"Join %s",invites[i].label);
        ok=ai_pack_need(&w,id,"invitation",title,0);
    }
    if(dating_pending>0&&ok)ok=ai_pack_need(&w,"dating:introductions","dating-introduction","Review current dating introductions",0);
    if(ok&&w.len<=65536)aw_response(200,"application/msgpack",w.buf,w.len,0);
    else aw_error(503,"The account Inbox actions could not be encoded.\n");
    bf_mp_writer_free(&w);
done:
    ac_person_free(people,person_count>0?(size_t)person_count:0);
    free(people);free(agents);free(halted);free(credit);free(invites);free(admin);return 1;
}
/* Without a session the inbox still has a place in the one app: it says what
 * arrives here and opens sign-in to the same address. Nothing is read. */
static void ai_native_head(void *unused){(void)unused;ui_open_a(stdout,"link","rel","stylesheet","href","/assets/aurekai-native.css?v=4");}
static void ai_signed_out(const char *context){
 fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Bonfyre-Surface-Program: native-source-workspace\r\nConnection: close\r\n\r\n",stdout);
 ak_app_open_with_head("Inbox","feed","sx-page route-inbox",ai_native_head,NULL);
 ui_open(stdout,"div","sx");ui_open_a(stdout,"nav","class","sx-crumbs","aria-label","Breadcrumb");ui_open_a(stdout,"a","href","/");ui_copy(stdout,"Aurekai");ui_close(stdout,"a");ui_open(stdout,"span",NULL);ui_copy(stdout,"/");ui_close(stdout,"span");ui_open(stdout,"span",NULL);ui_copy(stdout,"Inbox");ui_close(stdout,"span");ui_open(stdout,"em",NULL);ui_copy(stdout,"signed out");ui_close(stdout,"em");ui_close(stdout,"nav");
 ui_open(stdout,"header","sx-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"p","sx-kicker");ui_copy(stdout,"What needs you");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Inbox");ui_close(stdout,"h1");ui_open(stdout,"p",NULL);ui_copy(stdout,"Approvals, invitations, introductions and the work your tools are waiting on, for your own account and the organisations you belong to.");ui_close(stdout,"p");ui_close(stdout,"div");
 ui_open(stdout,"nav","sx-actions");ui_start(stdout,"a");ui_attr(stdout,"class","sx-btn sx-primary");ui_attr_open(stdout,"href");ui_copy(stdout,"/login?next=");
 {char next[200];snprintf(next,sizeof next,"/inbox?context=%s",context&&*context?context:"self");url_query_value(next);}
 fputs("\">Sign in to open it</a><a class=\"sx-btn\" href=\"/signup\">Make an account</a></nav></header>"
       "<div class=\"sx-split sx-three ai-out\"><section class=\"sx-register\" aria-labelledby=\"ai-q\"><h2 id=\"ai-q\">Arrives here <span>once you are signed in</span></h2><ol>"
       "<li><span class=\"sx-dispatch\"><small>Needs you</small><b>Approvals and sign-in steps</b><span>Things only you can confirm, from your tools and connected accounts.</span></span></li>"
       "<li><span class=\"sx-dispatch\"><small>Organisations</small><b>Invitations</b><span>Workspaces and institutions that asked you to join.</span></span></li>"
       "<li><span class=\"sx-dispatch\"><small>Running</small><b>Work in progress</b><span>Agents and jobs working for you, and the ones that stopped.</span></span></li></ol></section>"
       "<article class=\"sx-reading\"><p class=\"sx-kicker\">Private</p><h2>Your inbox is yours alone</h2><div class=\"sx-body\"><p>Nothing in it is shown to anyone else, and nothing in it is acted on until you choose. Sign in and it opens on what is waiting.</p></div>"
       "<footer class=\"sx-reading-actions\"><a class=\"sx-btn sx-primary\" href=\"/login?next=%2Finbox\">Sign in</a><a class=\"sx-btn\" href=\"/channels\">Read the living feeds meanwhile</a></footer></article>"
       "<aside class=\"sx-rail\"><section class=\"sx-panel\"><p class=\"sx-kicker\">Public meanwhile</p><ul class=\"sx-also\"><li><a href=\"/community\">What neighbours are offering</a></li><li><a href=\"/sources\">Sources Aurekai keeps</a></li><li><a href=\"/connections\">Connections</a></li></ul></section></aside></div>",stdout);
 ui_open_a(stdout,"section","class","sx-catalogue ai-programs","aria-labelledby","ai-ask"); ui_open(stdout,"header","sx-section-head"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","sx-kicker"); fputs("Open to everyone",stdout); ui_close(stdout,"p"); ui_open_a(stdout,"h2","id","ai-ask"); fputs("What you can ask for here",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Each request names the authority it needs; it is checked again when you send it.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_close(stdout,"header");
 surface_emit_capability_programs(NULL);
 ui_close(stdout,"section"); ui_close(stdout,"div");
 ak_app_close();
}

/* Dating conversations are an account-scoped source of the existing Inbox.
 * Never turn public /people or a caller-provided subject into dating consent.
 * The private Participation owner rechecks both members for each projection. */
static int ai_dating_subject(const char *auth_db,long user,char subject[192]){
 sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int found=0;
 subject[0]=0;
 if(user<1||!auth_db||bf_store_open(auth_db, &db, SQLITE_OPEN_READONLY, &bf_store_policy_auth)!=SQLITE_OK)goto done;
 if(sqlite3_prepare_v2(db,
 "SELECT actor_subject FROM actor_closure_bindings "
 "WHERE user_id=?1 AND state='active' LIMIT 2",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_int64(st,1,user);
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *v=(const char*)sqlite3_column_text(st,0);
  if(++found!=1||!bf_dating_subject(v)) {found=0;break;}
  snprintf(subject,192,"%s",v);
 }
done:
 sqlite3_finalize(st);if(db)sqlite3_close(db);
 if(found!=1)subject[0]=0;return found==1;
}
static void ai_dating_inbox(const char *auth_db,long uid,const char *context){
 const char *path=getenv("BONFYRE_PARTICIPATION_DB");
 if(!path||!*path)path=getenv("BONFYRE_DATING_DB");
 char subject[192];sqlite3 *db=NULL;BfDatingCandidate candidates[24];size_t count=0;
 if(strcmp(context,"self")||!path||*path!='/'||strlen(path)>4096||
    !ai_dating_subject(auth_db,uid,subject))return;
 if(bf_store_open(path, &db, SQLITE_OPEN_READONLY, &bf_store_policy_participation)!=SQLITE_OK){if(db)sqlite3_close(db);return;}
 sqlite3_busy_timeout(db,1000);
 if(!bf_dating_eligible(db,subject)||
    !bf_dating_candidates(db,subject,candidates,24,&count)){sqlite3_close(db);return;}
 /* Pending introductions are counted once, under Needs you (ai_dating_pending);
  * this group carries only accepted conversations and date plans. */
 int conversations=0,plans_pending=0;
 for(size_t i=0;i<count;i++){
  if(candidates[i].matched){
   conversations++;
   BfDatingPlan plans[8];size_t nplans=0;
   if(bf_dating_plans(db,subject,candidates[i].subject,plans,8,&nplans))
     for(size_t k=0;k<nplans;k++)
       if(!plans[k].approved_by_me&&plans[k].approved_by_other)plans_pending++;
  }
 }
 sqlite3_close(db);
 if(!conversations&&!plans_pending)return;
 ui_open_a(stdout,"section","class","ak-inbox-group","id","dating-conversations","data-inbox-group","dating","data-private-dating",(const char *)NULL); ui_open(stdout,"h2",NULL); fputs("Personal conversations",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Accepted conversations and date plans from your privately approved Life participation. These are never projected into public posts or organization work.",stdout); ui_close(stdout,"p"); ui_open(stdout,"ol","ak-inbox-list");
 if(plans_pending){
  ui_open_a(stdout,"li","class","ak-inbox-item","data-inbox-kind","dating-plan","data-needs","you"); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");
  page_glyph("calendar");
  ui_close(stdout,"span");ui_open(stdout,"div","ak-inbox-body");ui_open(stdout,"p","ak-inbox-meta");ui_copy(stdout,"Your approval");ui_close(stdout,"p");ui_open(stdout,"h3",NULL);printf("%d proposed date plan%s",plans_pending,plans_pending==1?"":"s");ui_close(stdout,"h3");ui_open(stdout,"p","ak-inbox-text");ui_copy(stdout,"Review the activity and budget before agreeing. No booking or payment is implied.");ui_close(stdout,"p");ui_open(stdout,"div","ak-inbox-acts");ui_open_a(stdout,"a","href","/profile?tab=dating");ui_copy(stdout,"Review date plans");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"li");
 }
 if(conversations){
  ui_open_a(stdout,"li","class","ak-inbox-item","data-inbox-kind","dating-conversation"); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");
  page_glyph("relationship");
  ui_close(stdout,"span");ui_open(stdout,"div","ak-inbox-body");ui_open(stdout,"p","ak-inbox-meta");ui_copy(stdout,"Mutually accepted");ui_close(stdout,"p");ui_open(stdout,"h3",NULL);printf("%d private conversation%s",conversations,conversations==1?"":"s");ui_close(stdout,"h3");ui_open(stdout,"p","ak-inbox-text");ui_copy(stdout,"Read and reply within your authenticated Life profile.");ui_close(stdout,"p");ui_open(stdout,"div","ak-inbox-acts");ui_open_a(stdout,"a","href","/profile?tab=dating");ui_copy(stdout,"Open conversations");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"li");
 }
 ui_close(stdout,"ol"); ui_close(stdout,"section");
}
/* The private Inbox presents context-specific peer decisions from the same
 * Participation owner. An introduction is not a relationship, vote, role,
 * content license, notification delivery, or the person's public presence. */
static void ai_peer_inbox(const char *auth_db,long uid,const char *context){
 const char *path=getenv("BONFYRE_PARTICIPATION_DB");
 if(!path||!*path)path=getenv("BONFYRE_DATING_DB");
 char actor[192];sqlite3 *db=NULL;
 if(strcmp(context,"self")||!path||*path!='/'||strlen(path)>4096||
    !ai_dating_subject(auth_db,uid,actor))return;
 if(bf_store_open(path, &db, SQLITE_OPEN_READONLY, &bf_store_policy_participation)!=SQLITE_OK){
  if(db)sqlite3_close(db);return;
 }
 sqlite3_busy_timeout(db,1000);
 BfPeerFacet facets[16];size_t nf=0;int opened=0;
 if(!bf_peer_facets(db,facets,16,&nf)){sqlite3_close(db);return;}
 for(size_t f=0;f<nf;f++){
  unsigned revision=0;
  if(bf_peer_status(db,actor,facets[f].id,&revision)!=1)continue;
  BfPeerCandidate peers[24];size_t count=0;
  if(!bf_peer_candidates(db,actor,facets[f].id,peers,24,&count))continue;
  unsigned incoming=0,connected=0;
  for(size_t i=0;i<count;i++){
   if(peers[i].incoming)incoming++;
   if(peers[i].matched)connected++;
  }
  if(!incoming&&!connected)continue;
  if(!opened++){
   ui_open_a(stdout,"section","class","ak-inbox-group","id","member-connections","data-inbox-group","member-connections","data-private-peer-facets",(const char *)NULL); ui_open(stdout,"h2",NULL); fputs("Member connections",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("Separate introductions for personal, creative, civic and project contexts. These are decisions you made, not inferred relationships.",stdout); ui_close(stdout,"p"); ui_open(stdout,"ol","ak-inbox-list");
  }
  ui_open_a(stdout,"li","class","ak-inbox-item","data-inbox-kind","context-introduction"); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");
  page_glyph("relationship");
  ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body"); ui_open(stdout,"p","ak-inbox-meta");
  html(facets[f].label);
  if(incoming){
   printf(" · Needs your decision</p><h3>%u private introduction%s",incoming,incoming==1?"":"s");
   ui_close(stdout,"h3");
  }else{
   ui_close(stdout,"p");ui_open(stdout,"h3",NULL);printf("%u mutually accepted connection%s",connected,connected==1?"":"s");
   ui_close(stdout,"h3");
  }
  ui_open(stdout,"p","ak-inbox-text");ui_copy(stdout,"Participation is limited to the chosen context. ");
  ui_copy(stdout,"No civic authority or publication permission is implied.");ui_close(stdout,"p");
  ui_open(stdout,"div","ak-inbox-acts");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/profile?tab=connections&amp;facet=");
  url_query_value(facets[f].id);
  ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Review this context"); ui_close(stdout,"a"); ui_close(stdout,"div"); ui_close(stdout,"div"); ui_close(stdout,"li");
 }
 if(opened)do{ ui_close(stdout,"ol"); ui_close(stdout,"section"); }while(0);
 sqlite3_close(db);
}
static int serve_account_inbox(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *bearer=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)query;(void)bearer;(void)auth_db;
    if(bf_request_is(r,"summary"))return serve_account_needs_summary(method,query,bearer,auth_db);
    char context[96],done[16],actor[160],subject[160];const char *scope=NULL;
    /* A query this page does not own, or no session, is the public surface's. */
    if(strcmp(method,"GET")||!ai_query(query,context,done))return 0;
    int access=psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),subject,sizeof(subject),&scope);
    if(access==0){ai_signed_out(context);return 1;}if(access!=1){aw_error(403,"This account cannot open that inbox.\n");return 1;}
    long user=strtol(actor+5,NULL,10);
    AcPersonGoal *people=calloc(AC_PERSON_MAX,sizeof(*people));AiWork *agents=calloc(AI_AGENT_MAX,sizeof(*agents)),*halted=calloc(4,sizeof(*halted)),*credit=calloc(AI_AGENT_MAX,sizeof(*credit));AiOrg *invites=calloc(10,sizeof(*invites)),*admin=calloc(12,sizeof(*admin));
    if(!people||!agents||!halted||!credit||!invites||!admin){free(people);free(agents);free(halted);free(credit);free(invites);free(admin);aw_error(503,"The inbox could not be prepared.\n");return 1;}
    int person_count=ac_person_goals(subject,scope?scope:"",people,AC_PERSON_MAX),running=0,stopped=0,waiting=0,halted_n=0,credit_n=0,ni=0,na=0,members=0;
    int agent_count=ai_agent_work(subject,scope?scope:"",agents,AI_AGENT_MAX,&running,&stopped,&waiting,people,person_count>0?person_count:0,halted,4,&halted_n,credit,AI_AGENT_MAX,&credit_n);
    ai_orgs(auth_db,user,invites,&ni,admin,&na,&members);
    int dating_pending=scope&&!strcmp(scope,"self")?ai_dating_pending(auth_db,user):0;int needs=(person_count>0?person_count:0)+credit_n+ni+(dating_pending>0?1:0),direct=credit_n+(dating_pending>0?1:0),checklists=0;for(int i=0;i<person_count;i++){if(!strcmp(people[i].source,"provider-estate-checklist"))checklists++;else direct++;}
    char label[160]="My personal workspace";{PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;if(psetup_destinations(bearer,auth_db,rows,&count)>0)for(size_t i=0;i<count;i++)if(!strcmp(rows[i].context,context)){snprintf(label,sizeof(label),"%s",rows[i].label);break;}}
    psetup_open_with_style(200,"Inbox","pack-workspace","/assets/account-inbox.css");
    ui_start(stdout,"section");ui_attr(stdout,"class","ak-inbox");ui_flag(stdout,"data-account-inbox");ui_attr_open(stdout,"data-context");html(context);printf("\" data-needs-count=\"%d\" data-agent-count=\"%d\">",needs,running);ui_open(stdout,"header","ak-inbox-mast");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ak-inbox-kicker");page_glyph("bell");fputs("Inbox · ",stdout);html(label);ui_close(stdout,"p"); ui_open(stdout,"h1",NULL);
    if(needs){ui_open(stdout,"em",NULL);printf("%d",needs);ui_close(stdout,"em");fputs(needs==1?" thing needs you":" things need you",stdout);}else if(person_count<0||agent_count<0)fputs("Inbox needs a source connection",stdout);else fputs("Nothing needs you right now",stdout);
    ui_close(stdout,"h1");ui_open(stdout,"p",NULL);ui_copy(stdout,"Approvals, sign-ins and account steps only you can take are here, each with its one next step. Everything the agents are doing stays on the right, so you can tell the two apart at a glance.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open_a(stdout,"nav","class","ak-inbox-nav","aria-label","Related pages");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_attr_close(stdout);ui_gt(stdout);page_glyph("board");ui_copy(stdout,"Work board");ui_close(stdout,"a");ui_open_a(stdout,"a","href","/organizations");page_glyph("group");ui_copy(stdout,"Organizations");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/apps/connections?context=");url_query_value(context);ui_attr_close(stdout);ui_gt(stdout);page_glyph("link");ui_copy(stdout,"Connections");ui_close(stdout,"a");ui_close(stdout,"nav");
    ui_open(stdout,"p","ak-inbox-tally");ui_open_a(stdout,"a","href","#needs-you","data-tally","you");ui_open(stdout,"strong",NULL);printf("%d",needs);ui_close(stdout,"strong");ui_open(stdout,"span",NULL);ui_copy(stdout,"Needs you");ui_close(stdout,"span");ui_close(stdout,"a");ui_open_a(stdout,"a","href","#agents","data-tally","agents");ui_open(stdout,"strong",NULL);printf("%d",running);ui_close(stdout,"strong");ui_open(stdout,"span",NULL);ui_copy(stdout,"Agents working");ui_close(stdout,"span");ui_close(stdout,"a");ui_open_a(stdout,"a","href","#agents-stopped","data-tally","stopped");ui_open(stdout,"strong",NULL);printf("%d",stopped);ui_close(stdout,"strong");ui_open(stdout,"span",NULL);ui_copy(stdout,"Agent work stopped");ui_close(stdout,"span");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_attr_close(stdout);ui_attr(stdout,"data-tally","waiting");ui_gt(stdout);ui_open(stdout,"strong",NULL);printf("%d",waiting);ui_close(stdout,"strong");ui_open(stdout,"span",NULL);ui_copy(stdout,"Waiting its turn");ui_close(stdout,"span");ui_close(stdout,"a");ui_close(stdout,"p");ui_close(stdout,"header");
    if(person_count<0||agent_count<0)do{ ui_open_a(stdout,"p","role","alert","data-inbox-source-unavailable",(const char *)NULL); fputs("The account Inbox cannot read all native work owners on this service. It has not verified that your work is empty. ",stdout); ui_open_a(stdout,"a","href","/work-board?context=self"); fputs("Check the Workboard",stdout); ui_close(stdout,"a"); fputs(" and its account connection.",stdout); ui_close(stdout,"p"); }while(0);
    if(*done){ui_open_a(stdout,"p","class","ak-inbox-receipt","role","status","data-inbox-receipt",(const char *)NULL);
        fputs(!strcmp(done,"approve")?"Approved. The release is being put live; its item below shows how that goes.":!strcmp(done,"later")?"Set aside. It stays on the work board and comes back when you want it.":!strcmp(done,"done")?"Marked as set up. Thank you.":"Recorded. The item keeps where it stands.",stdout);ui_close(stdout,"p");}
    ui_open(stdout,"div","ak-inbox-grid"); ui_open(stdout,"div","ak-inbox-main"); ui_open_a(stdout,"section","class","ak-inbox-group","id","needs-you","data-inbox-group","needs-you"); ui_open(stdout,"h2",NULL); fputs("Needs you",stdout);if(direct)printf(" <small>%d</small>",direct);ui_close(stdout,"h2"); ui_open(stdout,"ol","ak-inbox-list");
    for(int i=0;i<credit_n;i++)ai_execution_credit(context,&credit[i]);
    for(int i=0;i<person_count;i++){if(!strcmp(people[i].source,"release-approval"))ai_release(context,&people[i]);else if(!strcmp(people[i].source,"provider-estate"))ai_provider(context,&people[i]);}
    if(dating_pending>0){
      ui_open_a(stdout,"li","class","ak-inbox-item","id","dating:introductions","data-needs","you","data-inbox-kind","dating-introduction"); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");page_glyph("user");
      ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body");ai_meta("Needs you","Private introduction",0);
      ui_open(stdout,"h3",NULL);printf("%d %s to review",dating_pending,dating_pending==1?"introduction":"introductions");
      ui_close(stdout,"h3");ui_open(stdout,"p","ak-inbox-text");ui_copy(stdout,"Someone has requested an introduction. Nothing becomes a conversation without your current, explicit agreement. Names and messages stay inside your private Life account.");ui_close(stdout,"p");ui_open(stdout,"div","ak-inbox-acts");ui_start(stdout,"a");ui_attr(stdout,"class","ak-inbox-signin");ui_attr(stdout,"href","/profile?tab=dating&amp;view=matches");ui_gt(stdout);ui_copy(stdout,"Review introductions");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"li");
    }
    if(!direct){ui_open(stdout,"li","ak-inbox-empty");fputs(person_count<0?"Approvals are not connected on this site.":(ni||checklists)?"No approvals are waiting. Your sub-account steps are just below.":"No approvals or sign-ins are waiting on you.",stdout);ui_close(stdout,"li");}
    ui_close(stdout,"ol"); ui_close(stdout,"section");
    ai_dating_inbox(auth_db,user,context);
    ai_peer_inbox(auth_db,user,context);
    ui_open_a(stdout,"section","class","ak-inbox-group","id","sub-accounts","data-inbox-group","sub-accounts"); ui_open(stdout,"h2",NULL); fputs("Sub-accounts",stdout);if(ni+checklists)printf(" <small>%d need you</small>",ni+checklists);
    ui_close(stdout,"h2"); ui_text(stdout,"p",NULL,"Organizations you belong to or run, invitations waiting for you, and the provider accounts your organizations can open."); ui_open(stdout,"ol","ak-inbox-list");
    for(int i=0;i<ni;i++){
        ui_open_a(stdout,"li","class","ak-inbox-item","data-needs","you","data-inbox-kind","invitation"); ui_open_a(stdout,"span","class","ak-inbox-mark","aria-hidden","true");page_glyph("user");ui_close(stdout,"span"); ui_open(stdout,"div","ak-inbox-body");ai_meta("Needs you","Invitation",0);
        ui_open(stdout,"h3",NULL); fputs("Join ",stdout);html(invites[i].label);ui_close(stdout,"h3"); ui_open(stdout,"p","ak-inbox-text"); fputs("You were invited with ",stdout);html(invites[i].role);fputs(" access. Open the private link you were sent, or paste its code. It expires ",stdout);{static const char *const months[]={"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};int y=0,mo=0,d=0;if(sscanf(invites[i].expires,"%4d-%2d-%2d",&y,&mo,&d)==3&&mo>=1&&mo<=12)printf("%d %s %d",d,months[mo-1],y);else html(invites[i].expires);}fputs(".</p><div class=\"ak-inbox-acts\"><a class=\"ak-inbox-signin\" href=\"/organizations/join\">Open your invitation</a></div></div></li>",stdout);
    }
    for(int i=0;i<person_count;i++)if(!strcmp(people[i].source,"provider-estate-checklist"))ai_checklist(context,&people[i]);
    ui_close(stdout,"ol");
    if(na){ui_open(stdout,"h3","ak-inbox-subhead"); fputs("Organizations you run",stdout); ui_close(stdout,"h3"); ui_open_a(stdout,"ul","class","ak-inbox-orgs","data-inbox-orgs",(const char *)NULL);
        for(int i=0;i<na;i++){ui_open(stdout,"li",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"strong",NULL);html(admin[i].label);ui_close(stdout,"strong"); ui_open(stdout,"small",NULL);
            if(admin[i].pending)printf("%d invitation%s not accepted yet",admin[i].pending,admin[i].pending==1?"":"s");else fputs("Nobody is waiting on an invitation",stdout);
            ui_close(stdout,"small");ui_close(stdout,"div");ui_open(stdout,"div","ak-inbox-acts");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/organizations?organization=");url_query_value(admin[i].institution);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"People and provider accounts");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/apps?context=institution%3A");url_query_value(admin[i].institution);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Shared work");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"li");}
        ui_close(stdout,"ul");}
    ui_open(stdout,"div","ak-inbox-acts"); ui_open_a(stdout,"a","href","/organizations");
    if(members)printf("All your organizations (%d more)",members);else fputs(na?"All your organizations":"Start or join an organization",stdout);ui_close(stdout,"a"); ui_close(stdout,"div"); ui_close(stdout,"section"); ui_close(stdout,"div");
    ui_open_a(stdout,"aside","class","ak-inbox-side","id","agents","data-inbox-group","agents","aria-labelledby","ak-inbox-agents"); ui_open_a(stdout,"h2","id","ak-inbox-agents"); fputs("Agents working",stdout); ui_close(stdout,"h2"); ui_open(stdout,"p",NULL); fputs("The assistant, your agents and site checks. Nothing here is waiting on you.",stdout); ui_close(stdout,"p"); ui_open(stdout,"ol",NULL);
    for(int i=0;i<agent_count;i++)ai_agent_row(context,&agents[i],0);
    if(agent_count<=0){ui_open(stdout,"li","ak-inbox-empty");fputs(agent_count<0?"Agent work is not connected on this site.":"No agent is working right now.",stdout);ui_close(stdout,"li");}
    if(running>agent_count&&agent_count>0){ui_open(stdout,"li","ak-inbox-empty");printf("%d more on the work board.",running-agent_count);ui_close(stdout,"li");}
    ui_close(stdout,"ol"); ui_open_a(stdout,"h3","id","agents-stopped"); fputs("Agent work that stopped",stdout);if(stopped)printf(" · %d",stopped);ui_close(stdout,"h3"); ui_open_a(stdout,"ol","data-inbox-group","agents-stopped");
    for(int i=0;i<halted_n;i++)ai_agent_row(context,&halted[i],1);if(!halted_n)ui_text(stdout, "li", "ak-inbox-empty", "None.");
    ui_close(stdout,"ol");ui_start(stdout,"a");ui_attr(stdout,"class","ak-inbox-side-link");ui_attr_open(stdout,"href");ui_copy(stdout,"/work-board?context=");url_query_value(context);ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Everything on the work board");ui_close(stdout,"a");ui_close(stdout,"aside");ui_close(stdout,"div");ui_close(stdout,"section");
    psetup_end();
    ac_person_free(people,person_count>0?(size_t)person_count:0);free(people);free(agents);free(halted);free(credit);free(invites);free(admin);return 1;
}
#endif
