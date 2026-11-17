#ifndef BONFYRE_WORK_ORDERS_H
#define BONFYRE_WORK_ORDERS_H
/* Work orders between named accounts, each party's own sign-off on their
 * terms and milestones, disputes with a named arbiter, and role vouches.
 *
 * Records are CMS rows written through the CMS owner (ph_cms). Every write
 * here is one signed-in person's explicit submit from this site, about their
 * own decision: an opener stating terms, a named party accepting them or
 * approving a milestone, an arbiter writing a decision, a voucher vouching or
 * revoking. Nothing auto-commits. No money is held, moved or owed: a release
 * "approved by N of M" is a count of people's recorded decisions, and stated
 * compensation is a written term. No message is sent to anyone. */

static int wo_host(const char *host){return host&&(!strcmp(host,"aurekai.work")||!strcmp(host,"aurekai.life"));}
static int wo_ref_ok(const char *s,const char *prefix){
    size_t n=strlen(prefix);
    return s&&!strncmp(s,prefix,n)&&strlen(s)==n+24&&strspn(s+n,"0123456789abcdef")==24;
}
static void wo_now(char out[32]){time_t t=time(NULL);struct tm tm;gmtime_r(&t,&tm);strftime(out,32,"%Y-%m-%dT%H:%M:%SZ",&tm);}
static void wo_code(const char *prefix,const char *id){
    const char *dash=id?strchr(id,'-'):NULL;fputs(prefix,stdout);
    if(!dash)return;for(int i=1;i<=6&&dash[i];i++)fputc(toupper((unsigned char)dash[i]),stdout);
}
static void wo_day(const char *iso){char d[11];if(!iso||strlen(iso)<10){html(iso?iso:"");return;}memcpy(d,iso,10);d[10]=0;html(d);}
static void wo_name(const char *auth_db,const char *subject){char n[129];cine_name(auth_db,subject,n);html(n);}
static const char *wo_col(sqlite3_stmt *s,int i){const char *v=(const char*)sqlite3_column_text(s,i);return v?v:"";}
static void wo_reply(int code,const char *text){protocol_reply(code,"text/plain; charset=utf-8",text);}
static void wo_see(const char *prefix,const char *id){
    printf("HTTP/1.1 303 See Other\r\nLocation: %s%s\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",prefix,id?id:"");
}
/* Text a person typed, bounded; no control bytes other than newlines. */
static int wo_text(const char *body,const char *key,char *out,size_t cap,int required){
    out[0]=0;if(!query_value(body,key,out,cap)){if(required)return 0;out[0]=0;return 1;}
    if(strlen(out)>=cap-1)return 0;
    for(const unsigned char *p=(const unsigned char*)out;*p;p++)if(*p<0x20&&*p!='\n'&&*p!='\r'&&*p!='\t')return 0;
    return !required||out[strspn(out," \t\r\n")];
}
/* A guarded submit: signed in, from this site, a bounded form body. */
static int wo_post_actor(const char *method,const char *raw,const char *host,const char *body,
                         const char *bearer,const char *auth_db,char actor[64]){
    char origin[300]="",https[300],http[300];
    if(strcmp(method,"POST")){wo_reply(405,"This is a form submission.\n");return 0;}
    if(!wo_host(host)){wo_reply(404,"Work orders are not kept at this address.\n");return 0;}
    if(!page_session_actor(bearer,auth_db,actor,64)){wo_reply(401,"Sign in first: everything recorded here carries the name of the person who did it.\n");return 0;}
    snprintf(https,sizeof(https),"https://%s",host);snprintf(http,sizeof(http),"http://%s",host);
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||(strcmp(origin,https)&&strcmp(origin,http))){
        wo_reply(403,"This change must be submitted from this site.\n");return 0;}
    if(!body||strlen(body)>24000){wo_reply(400,"The form is missing or too long.\n");return 0;}
    return 1;
}
/* JSON payloads for the CMS owner. */
typedef struct{char buf[48000];size_t used;} WoJson;
static void wo_json_begin(WoJson *j){j->buf[0]='{';j->buf[1]=0;j->used=1;}
static void wo_json(WoJson *j,const char *k,const char *v){page_json_field(j->buf,sizeof(j->buf),&j->used,k,v,0);}
static const char *wo_json_end(WoJson *j,const char *status){
    if(status)page_json_field(j->buf,sizeof(j->buf),&j->used,"status",status,1);
    else if(j->used>1&&j->buf[j->used-1]==',')j->used--;
    if(j->used+2<sizeof(j->buf)){j->buf[j->used++]='}';j->buf[j->used]=0;}
    return j->buf;
}
static long long wo_row(sqlite3 *db,const char *table,const char *field,const char *id){
    char sql[200];sqlite3_stmt *s=NULL;long long row=0;
    snprintf(sql,sizeof(sql),"SELECT id FROM %s WHERE namespace='root' AND %s=?1 AND status='published' LIMIT 1",table,field);
    if(db&&cms_type_registered(db,table)&&sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW)row=sqlite3_column_int64(s,0);}
    sqlite3_finalize(s);return row;
}

typedef struct{char id[40],title[256],scope[6100],on_behalf[100],opened_by[64],terms[6100],currency[24],state[16],opened_at[32];} WoOrder;
static int wo_load(sqlite3 *db,const char *id,WoOrder *o){
    sqlite3_stmt *s=NULL;int found=0;memset(o,0,sizeof(*o));
    if(!db||!cms_type_registered(db,"work_order")||sqlite3_prepare_v2(db,
       "SELECT work_order_id,title,scope,on_behalf_of,opened_by,stated_terms,currency,state,opened_at FROM work_order "
       "WHERE namespace='root' AND status='published' AND work_order_id=?1 LIMIT 1",-1,&s,NULL)!=SQLITE_OK){sqlite3_finalize(s);return 0;}
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)==SQLITE_ROW){found=1;
        snprintf(o->id,sizeof(o->id),"%s",wo_col(s,0));snprintf(o->title,sizeof(o->title),"%s",wo_col(s,1));
        snprintf(o->scope,sizeof(o->scope),"%s",wo_col(s,2));snprintf(o->on_behalf,sizeof(o->on_behalf),"%s",wo_col(s,3));
        snprintf(o->opened_by,sizeof(o->opened_by),"%s",wo_col(s,4));snprintf(o->terms,sizeof(o->terms),"%s",wo_col(s,5));
        snprintf(o->currency,sizeof(o->currency),"%s",wo_col(s,6));snprintf(o->state,sizeof(o->state),"%s",wo_col(s,7));
        snprintf(o->opened_at,sizeof(o->opened_at),"%s",wo_col(s,8));}
    sqlite3_finalize(s);return found;
}
static long long wo_count(sqlite3 *db,const char *sql,const char *a,const char *b,const char *c){
    sqlite3_stmt *s=NULL;long long n=0;
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){
        if(a)sqlite3_bind_text(s,1,a,-1,SQLITE_TRANSIENT);if(b)sqlite3_bind_text(s,2,b,-1,SQLITE_TRANSIENT);if(c)sqlite3_bind_text(s,3,c,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(s)==SQLITE_ROW)n=sqlite3_column_int64(s,0);}
    sqlite3_finalize(s);return n;
}
#define WO_PARTY_SQL "SELECT count(*) FROM work_order_party WHERE namespace='root' AND status='published' AND work_order_id=?1"
static int wo_is_party(sqlite3 *db,const char *wo,const char *subject){
    return wo_count(db,WO_PARTY_SQL " AND party_subject=?2",wo,subject,NULL)>0;
}
static long long wo_parties(sqlite3 *db,const char *wo){return wo_count(db,WO_PARTY_SQL,wo,NULL,NULL);}
/* Approvals by people who are parties now; a decision is never inherited. */
static long long wo_approvals(sqlite3 *db,const char *wo,const char *milestone){
    return wo_count(db,"SELECT count(DISTINCT s.signer) FROM work_order_signoff s JOIN work_order_party p "
        "ON p.namespace=s.namespace AND p.work_order_id=s.work_order_id AND p.party_subject=s.signer AND p.status='published' "
        "WHERE s.namespace='root' AND s.status='published' AND s.work_order_id=?1 AND s.milestone_id=?2 AND s.decision='approve'",wo,milestone,NULL);
}
/* Once anyone other than the opener has accepted the terms, the terms are
 * what they accepted: no party or milestone may be added under them. */
static int wo_terms_fixed(sqlite3 *db,const WoOrder *o){
    return wo_count(db,"SELECT count(*) FROM work_order_signoff WHERE namespace='root' AND status='published' "
        "AND work_order_id=?1 AND milestone_id='terms' AND decision='approve' AND signer<>?2",o->id,o->opened_by,NULL)>0;
}
static int wo_is_arbiter(sqlite3 *db,const char *wo,const char *subject){
    return cms_type_registered(db,"work_dispute")&&wo_count(db,"SELECT count(*) FROM work_dispute WHERE namespace='root' AND status='published' AND work_order_id=?1 AND arbiter=?2",wo,subject,NULL)>0;
}
static int wo_milestone_of(sqlite3 *db,const char *wo,const char *milestone){
    return !strcmp(milestone,"terms")||wo_count(db,"SELECT count(*) FROM work_order_milestone WHERE namespace='root' AND status='published' AND work_order_id=?1 AND milestone_id=?2",wo,milestone,NULL)>0;
}
static int wo_types(sqlite3 *db){
    static const char *types[]={"work_order","work_order_party","work_order_milestone","work_order_signoff","work_dispute","dispute_statement"};
    for(size_t i=0;i<sizeof(types)/sizeof(*types);i++)if(!cms_type_registered(db,types[i]))return 0;
    return 1;
}
static void wo_signer_strip(sqlite3 *db,const char *wo,const char *milestone,long long m){
    long long n=wo_approvals(db,wo,milestone);
    ui_start(stdout,"span");ui_attr(stdout,"class","wo-sig");ui_attr(stdout,"role","img");ui_attr_open(stdout,"aria-label");printf("Approved by %lld of %lld",n,m);ui_attr_close(stdout);ui_gt(stdout);
    for(long long i=0;i<m&&i<12;i++)fputs(i<n?"<i class=\"is-on\"></i>":"<i></i>",stdout);
    ui_close(stdout,"span");ui_open(stdout,"b","wo-count");printf("Approved by %lld of %lld",n,m);ui_close(stdout,"b");
}
/* Each party's own decision on one stage, as they recorded it. */
static void wo_decisions(sqlite3 *db,const char *auth_db,const char *wo,const char *milestone){
    sqlite3_stmt *s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT p.party_subject,p.part,COALESCE(s.decision,''),COALESCE(s.signed_at,''),COALESCE(s.note,'') "
        "FROM work_order_party p LEFT JOIN work_order_signoff s ON s.namespace=p.namespace AND s.work_order_id=p.work_order_id "
        "AND s.signer=p.party_subject AND s.milestone_id=?2 AND s.status='published' "
        "WHERE p.namespace='root' AND p.status='published' AND p.work_order_id=?1 ORDER BY p.named_at,p.id LIMIT 40",-1,&s,NULL)!=SQLITE_OK){sqlite3_finalize(s);return;}
    sqlite3_bind_text(s,1,wo,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,milestone,-1,SQLITE_TRANSIENT);
    ui_open(stdout,"ul","wo-signers");
    while(sqlite3_step(s)==SQLITE_ROW){
        const char *d=wo_col(s,2);
        ui_open(stdout,"li",NULL); ui_open(stdout,"span",NULL); ui_open(stdout,"strong",NULL);wo_name(auth_db,wo_col(s,0));ui_close(stdout,"strong"); ui_open(stdout,"small",NULL);html(wo_col(s,1));
        ui_close(stdout,"small");ui_close(stdout,"span");ui_start(stdout,"span");ui_attr_open(stdout,"class");fputs(!strcmp(d,"approve")?"is-on":!strcmp(d,"withhold")?"is-held":"is-wait",stdout);ui_attr_close(stdout);ui_gt(stdout);
        fputs(!strcmp(d,"approve")?"Approved":!strcmp(d,"withhold")?"Withheld":"Waiting",stdout);
        if(*d){ui_open(stdout,"small",NULL);wo_day(wo_col(s,3));ui_close(stdout,"small");}
        ui_close(stdout,"span");
        if(*wo_col(s,4)){ui_text(stdout, "q", NULL, wo_col(s,4));}
        ui_close(stdout,"li");
    }
    ui_close(stdout,"ul");sqlite3_finalize(s);
}
static void wo_signoff_form(const char *wo,const char *milestone,const char *label,const char *approve_label){
    BfAuthoringTree *t=calloc(1,sizeof(*t));if(!t)return;
    FtField hidden[2]={{"wo",wo},{"milestone",milestone}};
    FtOption options[2]={{"approve",approve_label,1},{"withhold","Withhold for now",0}};
    char id[80];snprintf(id,sizeof(id),"so-%.60s",milestone);
    int ok=ft_action(t,id,NULL,0,label,"post","/work-orders/signoff",hidden,2)&&
           ft_choice(t,"so-decision",id,0,"Your decision","decision",options,2)&&
           ft_field(t,"so-note",id,1,"Note (optional)","text","note","","",0,"2","600",0,"");
    if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}
}
static void wo_strip(const char *here){
    ui_open_a(stdout,"div","class","ak-wb-strip","data-work-orders",(const char *)NULL); ui_open_a(stdout,"a","href","/work-board"); fputs("Your work",stdout); ui_close(stdout,"a"); ui_open(stdout,"span",NULL); fputs("/",stdout); ui_close(stdout,"span"); ui_open_a(stdout,"a","href","/work-orders"); fputs("Work orders",stdout); ui_close(stdout,"a");
    if(here){ui_text(stdout,"span",NULL,"/"); ui_open(stdout,"b",NULL);html(here);ui_close(stdout,"b");}
    if(!here||strcmp(here,"Vouches"))do{ ui_open_a(stdout,"a","href","/vouches"); fputs("Vouches",stdout); ui_close(stdout,"a"); }while(0);ui_open_a(stdout,"a","href","/roles"); fputs("Roles",stdout); ui_close(stdout,"a"); ui_open(stdout,"span","ak-live"); fputs("Recorded decisions, no money moved",stdout); ui_close(stdout,"span"); ui_close(stdout,"div");
}
static void wo_foot(void){
    ui_open(stdout,"p","ak-packs-foot"); ui_text(stdout,"span","ak-live","Nothing here holds, moves or pays money, and nothing is sent to anyone. Each record is one person's own submitted decision."); ui_close(stdout,"p");
    psetup_end();
}

/* ------------------------------------------------------------ /work-orders */
static void wo_compose_form(const char *bearer,const char *auth_db){
    PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;
    int result=psetup_destinations(bearer,auth_db,rows,&count);
    FtOption options[PSETUP_DESTINATIONS_MAX];unsigned n=0;
    if(result<=0){options[n].value="self";options[n].label="Myself";options[n].checked=1;n++;}
    for(size_t i=0;i<count&&n<PSETUP_DESTINATIONS_MAX;i++){
        if(strcmp(rows[i].context,"self")&&!rows[i].can_write)continue;
        options[n].value=rows[i].context;options[n].label=strcmp(rows[i].context,"self")?rows[i].label:"Myself";options[n].checked=n==0;n++;
    }
    BfAuthoringTree *t=calloc(1,sizeof(*t));if(!t)return;
    int ok=ft_action(t,"wo-compose",NULL,0,"Open this work order","post","/work-orders/open",NULL,0)&&
           ft_field(t,"wo-title","wo-compose",0,"What is the work","text","title","","Engine bore and hull inspection",0,"1","200",1,"")&&
           ft_field(t,"wo-scope","wo-compose",1,"Scope","text","scope","","What is to be done, by whom, to what standard",1,"4","6000",1,"")&&
           ft_field(t,"wo-terms","wo-compose",2,"Terms you are stating","text","terms","","How each milestone is approved, what happens if someone withholds",1,"4","6000",1,"")&&
           ft_field(t,"wo-currency","wo-compose",3,"Currency the amounts are written in","text","currency","USD","USD",0,"1","12",1,"")&&
           ft_choice(t,"wo-behalf","wo-compose",4,"Opened on behalf of","context",options,n);
    if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}
}
static int wo_index(const char *host,const char *bearer,const char *auth_db){
    char actor[64];
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){cjw_signin("/work-orders");return 1;}
    sqlite3 *db=cms_open_ro();
    if(!db||!wo_types(db)){if(db)sqlite3_close(db);wo_reply(503,"The work order records are not available here yet.\n");return 1;}
    (void)host;
    long long open=wo_count(db,"SELECT count(DISTINCT w.work_order_id) FROM work_order w JOIN work_order_party p ON p.namespace=w.namespace AND p.work_order_id=w.work_order_id AND p.status='published' "
        "WHERE w.namespace='root' AND w.status='published' AND w.state='open' AND p.party_subject=?1",actor,NULL,NULL);
    long long waiting=wo_count(db,"SELECT count(*) FROM work_order_milestone m JOIN work_order w ON w.namespace=m.namespace AND w.work_order_id=m.work_order_id AND w.status='published' AND w.state='open' "
        "JOIN work_order_party p ON p.namespace=m.namespace AND p.work_order_id=m.work_order_id AND p.status='published' AND p.party_subject=?1 "
        "WHERE m.namespace='root' AND m.status='published' AND NOT EXISTS(SELECT 1 FROM work_order_signoff s WHERE s.namespace=m.namespace AND s.work_order_id=m.work_order_id "
        "AND s.milestone_id=m.milestone_id AND s.signer=?1 AND s.status='published')",actor,NULL,NULL);
    long long disputes=wo_count(db,"SELECT count(*) FROM work_dispute d WHERE d.namespace='root' AND d.status='published' AND d.state='open' AND (d.arbiter=?1 OR EXISTS(SELECT 1 FROM work_order_party p "
        "WHERE p.namespace=d.namespace AND p.work_order_id=d.work_order_id AND p.party_subject=?1 AND p.status='published'))",actor,NULL,NULL);
    long long terms=wo_count(db,"SELECT count(*) FROM work_order_party p WHERE p.namespace='root' AND p.status='published' AND p.compensation_stated<>'' "
        "AND EXISTS(SELECT 1 FROM work_order_party me WHERE me.namespace=p.namespace AND me.work_order_id=p.work_order_id AND me.party_subject=?1 AND me.status='published')",actor,NULL,NULL);
    psetup_open(200,"Work orders","ak-wb work-orders");
    wo_strip(NULL);
    ui_open(stdout,"header","ak-wb-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","ak-kicker"); fputs("Work between people",stdout); ui_close(stdout,"p"); ui_open(stdout,"h1",NULL); fputs("Work orders, sign-offs and disputes",stdout); ui_close(stdout,"h1"); ui_open(stdout,"p","ak-lede"); fputs("Open a piece of work, name the people doing it, stage it into milestones and state the terms. Each party records their own acceptance and their own approval of each milestone. A disagreement goes to an arbiter the parties can see.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"aside","ak-panel is-raised"); ui_open(stdout,"p","ak-kicker"); fputs("What is recorded here",stdout); ui_close(stdout,"p"); ui_open(stdout,"p",NULL); fputs("Decisions, in the words and names of the people who made them. A milestone shown as approved by 3 of 3 means three people said yes. It is not a payment, no funds are held for it, and nobody is told anything on your behalf.",stdout); ui_close(stdout,"p"); ui_close(stdout,"aside"); ui_close(stdout,"header");
    ui_open(stdout,"dl","ak-stats is-row");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Open work orders");ui_close(stdout,"dt");ui_start(stdout,"dd");printf(" class=\"%s\">%lld",open?"is-on":"",open);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"you are a party to");ui_close(stdout,"small");ui_close(stdout,"div");
    ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Waiting on you");ui_close(stdout,"dt");ui_start(stdout,"dd");printf(" class=\"%s\">%lld",waiting?"is-on":"",waiting);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"milestones you have not decided");ui_close(stdout,"small");ui_close(stdout,"div");
    ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Open disputes");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%lld",disputes);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"as a party or arbiter");ui_close(stdout,"small");ui_close(stdout,"div");
    ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Stated compensation");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%lld",terms);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"terms recorded, none paid here");ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"dl");
    /* The ledger: every work order this person is part of, newest first. */
    ui_open_a(stdout,"section","class","ak-sec","data-work-order-ledger",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Release ledger",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL); fputs("Your work orders",stdout); ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_open(stdout,"p",NULL); fputs("Each row shows the next milestone still waiting and how many of its parties have approved it.",stdout); ui_close(stdout,"p"); ui_close(stdout,"header"); ui_open(stdout,"div","wo-ledger");
    sqlite3_stmt *s=NULL;int found=0;
    if(sqlite3_prepare_v2(db,"SELECT w.work_order_id,w.title,w.state,w.currency,w.opened_at,"
        "(SELECT group_concat(q.party_subject,' ') FROM (SELECT party_subject FROM work_order_party x WHERE x.namespace=w.namespace AND x.work_order_id=w.work_order_id AND x.status='published' ORDER BY x.named_at LIMIT 6) q),"
        "(SELECT m.milestone_id||char(9)||m.title||char(9)||COALESCE(m.amount_stated,'') FROM work_order_milestone m WHERE m.namespace=w.namespace AND m.work_order_id=w.work_order_id AND m.status='published' "
        "AND (SELECT count(DISTINCT s.signer) FROM work_order_signoff s JOIN work_order_party pp ON pp.namespace=s.namespace AND pp.work_order_id=s.work_order_id AND pp.party_subject=s.signer AND pp.status='published' "
        "WHERE s.namespace=m.namespace AND s.work_order_id=m.work_order_id AND s.milestone_id=m.milestone_id AND s.decision='approve' AND s.status='published')"
        "<(SELECT count(*) FROM work_order_party y WHERE y.namespace=w.namespace AND y.work_order_id=w.work_order_id AND y.status='published') "
        "ORDER BY CAST(m.sequence AS INTEGER),m.id LIMIT 1),"
        "(SELECT count(*) FROM work_order_milestone z WHERE z.namespace=w.namespace AND z.work_order_id=w.work_order_id AND z.status='published') "
        "FROM work_order w WHERE w.namespace='root' AND w.status='published' AND (EXISTS(SELECT 1 FROM work_order_party p WHERE p.namespace=w.namespace AND p.work_order_id=w.work_order_id AND p.party_subject=?1 AND p.status='published') "
        "OR EXISTS(SELECT 1 FROM work_dispute d WHERE d.namespace=w.namespace AND d.work_order_id=w.work_order_id AND d.arbiter=?1 AND d.status='published')) "
        "ORDER BY w.opened_at DESC,w.id DESC LIMIT 40",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){
            const char *id=wo_col(s,0);if(!wo_ref_ok(id,"wo-"))continue;found++;
            long long m=wo_parties(db,id);
            ui_open_a(stdout,"article","class","wo-row","data-work-order",(const char *)NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"p","ak-kicker");wo_code("WO-",id);
            fputs(" <span class=\"ak-tag",stdout);fputs(!strcmp(wo_col(s,2),"open")?" is-on\">Open":"\">Closed",stdout);ui_close(stdout,"span"); ui_close(stdout,"p"); ui_open(stdout,"h3",NULL); ui_open_a(stdout,"a",UI_DYN,"href","/work-orders/",(id));html(wo_col(s,1));ui_close(stdout,"a"); ui_close(stdout,"h3"); ui_open(stdout,"small",NULL);
            char parties[600];snprintf(parties,sizeof(parties),"%s",wo_col(s,5));int k=0;
            for(char *save=NULL,*p=strtok_r(parties," ",&save);p;p=strtok_r(NULL," ",&save)){if(k++)fputs(", ",stdout);wo_name(auth_db,p);}
            ui_close(stdout,"small"); ui_close(stdout,"div");
            char next[600];snprintf(next,sizeof(next),"%s",wo_col(s,6));
            char *tab1=strchr(next,'\t'),*tab2=tab1?strchr(tab1+1,'\t'):NULL;
            if(tab1&&tab2){*tab1=0;*tab2=0;
                ui_open(stdout,"div",NULL); ui_text(stdout,"span","ak-kicker","Next milestone"); ui_open(stdout,"p",NULL);html(tab1+1);
                if(tab2[1]){ui_open(stdout,"br",NULL); ui_open(stdout,"small",NULL);html(tab2+1);fputs(" ",stdout);html(wo_col(s,3));fputs(" stated</small>",stdout);}
                ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL);wo_signer_strip(db,id,next,m);ui_close(stdout,"div");
            }else{
                ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Milestones");ui_close(stdout,"span");ui_open(stdout,"p",NULL);fputs(sqlite3_column_int64(s,7)?"Every milestone approved by all parties":"No milestones staged yet",stdout);ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"b","wo-count");printf("%lld %s",m,m==1?"party":"parties");ui_close(stdout,"b");ui_close(stdout,"div");
            }
            ui_open_a(stdout,"a","class","ak-btn",UI_DYN,"href","/work-orders/",(id)); fputs("Open",stdout); ui_close(stdout,"a"); ui_close(stdout,"article");
        }
    }
    sqlite3_finalize(s);s=NULL;
    if(!found)ui_text(stdout, "p", "ak-empty", "You are not a party to any work order yet. Open one below, or ask the person opening one to name you on it.");
    ui_close(stdout,"div"); ui_close(stdout,"section");
    /* Stated compensation across the work orders this person is part of. */
    ui_open_a(stdout,"section","class","ak-sec","data-compensation-ledger",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Compensation terms",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL); fputs("What each party is stated to receive",stdout); ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_open(stdout,"p",NULL); fputs("These are the amounts and shares written on each work order. Recording them pays nobody; payment happens, if it does, between the people involved and outside this page.",stdout); ui_close(stdout,"p"); ui_close(stdout,"header"); ui_open(stdout,"div","ak-panel ak-scroll"); ui_open(stdout,"table","ak-table"); ui_open(stdout,"thead",NULL); ui_open(stdout,"tr",NULL); ui_open(stdout,"th",NULL); fputs("Party",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Work order",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Part",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Stated compensation",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Basis",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Terms accepted",stdout); ui_close(stdout,"th"); ui_close(stdout,"tr"); ui_close(stdout,"thead"); ui_open(stdout,"tbody",NULL);
    found=0;
    if(sqlite3_prepare_v2(db,"SELECT p.party_subject,p.work_order_id,w.title,p.part,COALESCE(p.compensation_stated,''),COALESCE(p.compensation_basis,''),w.currency,"
        "EXISTS(SELECT 1 FROM work_order_signoff s WHERE s.namespace=p.namespace AND s.work_order_id=p.work_order_id AND s.milestone_id='terms' AND s.signer=p.party_subject AND s.decision='approve' AND s.status='published') "
        "FROM work_order_party p JOIN work_order w ON w.namespace=p.namespace AND w.work_order_id=p.work_order_id AND w.status='published' "
        "WHERE p.namespace='root' AND p.status='published' AND (COALESCE(p.compensation_stated,'')<>'' OR COALESCE(p.compensation_basis,'')<>'') "
        "AND EXISTS(SELECT 1 FROM work_order_party me WHERE me.namespace=p.namespace AND me.work_order_id=p.work_order_id AND me.party_subject=?1 AND me.status='published') "
        "ORDER BY w.opened_at DESC,p.named_at LIMIT 60",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){found++;
            ui_open_a(stdout,"tr","data-compensation-term",(const char *)NULL); ui_open(stdout,"td",NULL); ui_open(stdout,"strong",NULL);wo_name(auth_db,wo_col(s,0));ui_close(stdout,"strong"); ui_close(stdout,"td"); ui_open(stdout,"td",NULL); ui_open_a(stdout,"a",UI_DYN,"href","/work-orders/",(wo_col(s,1)));html(wo_col(s,2));ui_close(stdout,"a"); ui_close(stdout,"td"); ui_open(stdout,"td",NULL);html(wo_col(s,3));ui_close(stdout,"td"); ui_open(stdout,"td",NULL);
            if(*wo_col(s,4)){html(wo_col(s,4));fputs(" ",stdout);html(wo_col(s,6));}else fputs("&mdash;",stdout);
            ui_close(stdout,"td"); ui_open(stdout,"td",NULL);html(wo_col(s,5));ui_close(stdout,"td"); ui_open(stdout,"td",NULL);
            fputs(sqlite3_column_int(s,7)?"<span class=\"ak-tag is-on\">Accepted</span>":"<span class=\"ak-tag\">Not yet</span>",stdout);ui_close(stdout,"td"); ui_close(stdout,"tr");
        }
    }
    sqlite3_finalize(s);s=NULL;
    ui_close(stdout,"tbody"); ui_close(stdout,"table");
    if(!found)ui_text(stdout, "p", "ak-empty", "No compensation has been stated on your work orders. The opener states it when naming each party.");
    ui_close(stdout,"div"); ui_close(stdout,"section");
    /* Disputes this person can see. */
    ui_open_a(stdout,"section","class","ak-sec","data-dispute-docket",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Disputes",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL); fputs("Open and decided disputes",stdout); ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_open(stdout,"p",NULL); fputs("A party can raise a dispute on any milestone and name an arbiter who is not a party. The arbiter's decision is written, not enforced.",stdout); ui_close(stdout,"p"); ui_close(stdout,"header"); ui_open(stdout,"div","ak-panel"); ui_open(stdout,"ol","ak-rows");
    found=0;
    if(sqlite3_prepare_v2(db,"SELECT d.dispute_id,w.title,d.state,d.opened_at,d.arbiter FROM work_dispute d JOIN work_order w ON w.namespace=d.namespace AND w.work_order_id=d.work_order_id AND w.status='published' "
        "WHERE d.namespace='root' AND d.status='published' AND (d.arbiter=?1 OR EXISTS(SELECT 1 FROM work_order_party p WHERE p.namespace=d.namespace AND p.work_order_id=d.work_order_id AND p.party_subject=?1 AND p.status='published')) "
        "ORDER BY d.state='open' DESC,d.opened_at DESC LIMIT 30",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){found++;
            ui_open_a(stdout,"li","data-dispute",(const char *)NULL); ui_open(stdout,"strong",NULL);wo_code("D-",wo_col(s,0));fputs(" \xc2\xb7 ",stdout);html(wo_col(s,1));ui_close(stdout,"strong"); ui_open(stdout,"small",NULL);
            fputs(!strcmp(wo_col(s,2),"decided")?"Decided":"Open",stdout);fputs(" \xc2\xb7 arbiter ",stdout);wo_name(auth_db,wo_col(s,4));fputs(" \xc2\xb7 opened ",stdout);wo_day(wo_col(s,3));
            ui_close(stdout,"small"); ui_open_a(stdout,"a","class","ak-go",UI_DYN,"href","/disputes/",(wo_col(s,0))); fputs("Open hearing",stdout); ui_close(stdout,"a"); ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ol");if(!found)ui_text(stdout, "p", "ak-empty", "No disputes involve you.");ui_close(stdout,"div"); ui_close(stdout,"section");
    ui_open_a(stdout,"section","class","ak-sec","data-work-order-compose",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Compose",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL); fputs("Open a work order",stdout); ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_open(stdout,"p",NULL); fputs("You state the work and the terms. On the next page you name each party and stage the milestones. Nobody is told; share the page with the people you name.",stdout); ui_close(stdout,"p"); ui_close(stdout,"header"); ui_open(stdout,"div","ak-ask");
    wo_compose_form(bearer,auth_db);
    ui_close(stdout,"div"); ui_close(stdout,"section");
    sqlite3_close(db);wo_foot();return 1;
}

/* --------------------------------------------------- /work-orders/<id> */
static void wo_party_form(sqlite3 *db,const char *wo){
    BfAuthoringTree *t=calloc(1,sizeof(*t));if(!t)return;
    FtField hidden[1]={{"wo",wo}};
    FtOption roles[25];char labels[25][160],values[25][96];unsigned n=0;
    roles[n].value="";roles[n].label="No registry role";roles[n].checked=1;n++;
    sqlite3_stmt *s=NULL;
    if(cms_type_registered(db,"role_program")&&sqlite3_prepare_v2(db,"SELECT role_id,label FROM role_program WHERE namespace='root' AND status='published' ORDER BY label LIMIT 24",-1,&s,NULL)==SQLITE_OK)
        while(n<25&&sqlite3_step(s)==SQLITE_ROW){snprintf(values[n],96,"%s",wo_col(s,0));snprintf(labels[n],160,"%s",wo_col(s,1));roles[n].value=values[n];roles[n].label=labels[n];roles[n].checked=0;n++;}
    sqlite3_finalize(s);
    int ok=ft_action(t,"wo-party",NULL,0,"Name this party","post","/work-orders/party",hidden,1)&&
           ft_field(t,"wp-email","wo-party",0,"Their account email","email","email","","name@example.org",0,"1","300",1,"email")&&
           ft_field(t,"wp-part","wo-party",1,"Part they play","text","part","","Fabricator, courier, steward",0,"1","60",1,"")&&
           ft_field(t,"wp-comp","wo-party",2,"Stated compensation (amount)","text","compensation","","2400",0,"1","24",0,"decimal")&&
           ft_field(t,"wp-basis","wo-party",3,"Basis","text","basis","","Per milestone approved; share of the total",0,"1","600",0,"")&&
           ft_choice(t,"wp-role","wo-party",4,"Registry role (optional)","role",roles,n);
    if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}
}
static void wo_milestone_form(const char *wo){
    BfAuthoringTree *t=calloc(1,sizeof(*t));if(!t)return;
    FtField hidden[1]={{"wo",wo}};
    int ok=ft_action(t,"wo-milestone",NULL,0,"Stage this milestone","post","/work-orders/milestone",hidden,1)&&
           ft_field(t,"wm-title","wo-milestone",0,"Milestone","text","title","","Bore and hull inspection",0,"1","200",1,"")&&
           ft_field(t,"wm-scope","wo-milestone",1,"What counts as done","text","scope","","The evidence each party will look at before approving",1,"3","6000",1,"")&&
           ft_field(t,"wm-amount","wo-milestone",2,"Amount stated","text","amount","","2400",0,"1","24",0,"decimal")&&
           ft_field(t,"wm-due","wo-milestone",3,"Due on","date","due","","",0,"1","10",0,"");
    if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}
}
static void wo_dispute_form(sqlite3 *db,const char *wo){
    BfAuthoringTree *t=calloc(1,sizeof(*t));if(!t)return;
    FtField hidden[1]={{"wo",wo}};
    FtOption opts[31];char labels[31][220],values[31][40];unsigned n=0;
    opts[n].value="terms";opts[n].label="The terms as a whole";opts[n].checked=1;n++;
    sqlite3_stmt *s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT milestone_id,title FROM work_order_milestone WHERE namespace='root' AND status='published' AND work_order_id=?1 ORDER BY CAST(sequence AS INTEGER),id LIMIT 30",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,wo,-1,SQLITE_TRANSIENT);
        while(n<31&&sqlite3_step(s)==SQLITE_ROW){snprintf(values[n],40,"%s",wo_col(s,0));snprintf(labels[n],220,"%s",wo_col(s,1));opts[n].value=values[n];opts[n].label=labels[n];opts[n].checked=0;n++;}
    }
    sqlite3_finalize(s);
    int ok=ft_action(t,"wo-dispute",NULL,0,"Raise this dispute","post","/disputes/open",hidden,1)&&
           ft_choice(t,"wd-on","wo-dispute",0,"What it is about","milestone",opts,n)&&
           ft_field(t,"wd-arbiter","wo-dispute",1,"Arbiter's account email (not a party)","email","arbiter","","arbiter@example.org",0,"1","300",1,"email")&&
           ft_field(t,"wd-claim","wo-dispute",2,"Your claim","text","claim","","What happened and what you are asking for",1,"4","6000",1,"");
    if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}
}
static int wo_detail(const char *id,const char *bearer,const char *auth_db){
    char actor[64];
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){char back[80];snprintf(back,sizeof(back),"/work-orders/%s",id);cjw_signin(back);return 1;}
    sqlite3 *db=cms_open_ro();WoOrder o;
    if(!db||!wo_types(db)){if(db)sqlite3_close(db);wo_reply(503,"The work order records are not available here yet.\n");return 1;}
    if(!wo_load(db,id,&o)||!(wo_is_party(db,id,actor)||wo_is_arbiter(db,id,actor))){sqlite3_close(db);wo_reply(404,"This work order does not exist or you are not part of it.\n");return 1;}
    int party=wo_is_party(db,id,actor),opener=!strcmp(actor,o.opened_by),fixed=wo_terms_fixed(db,&o),open=!strcmp(o.state,"open");
    long long m=wo_parties(db,id);
    long long accepted=wo_approvals(db,id,"terms");
    double total=0;long long stages=0;sqlite3_stmt *s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT count(*),COALESCE(sum(CAST(REPLACE(amount_stated,',','') AS REAL)),0) FROM work_order_milestone WHERE namespace='root' AND status='published' AND work_order_id=?1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW){stages=sqlite3_column_int64(s,0);total=sqlite3_column_double(s,1);}}
    sqlite3_finalize(s);s=NULL;
    psetup_open(200,o.title,"ak-wb work-orders work-order");
    char code[16];snprintf(code,sizeof(code),"WO-%.6s",o.id+3);for(char *p=code;*p;p++)*p=(char)toupper((unsigned char)*p);
    wo_strip(code);
    ui_open(stdout,"header","ak-wb-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","ak-kicker"); fputs("Work order ",stdout);html(code);fputs(" \xc2\xb7 ",stdout);
    fputs(open?(fixed?"Terms accepted":"Being composed"):"Closed",stdout);
    ui_close(stdout,"p"); ui_open(stdout,"h1",NULL);html(o.title);ui_close(stdout,"h1"); ui_open(stdout,"p","ak-lede");html(o.scope);ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open_a(stdout,"aside","class","ak-panel is-raised wo-vault","data-release-summary",(const char *)NULL); ui_open(stdout,"p","ak-kicker"); fputs("Stated across milestones",stdout); ui_close(stdout,"p"); ui_open(stdout,"p","wo-sum");
    printf("%.2f <small>",total);html(o.currency);ui_close(stdout,"small"); ui_close(stdout,"p"); ui_open(stdout,"dl","ak-spec"); ui_open(stdout,"dt",NULL); fputs("Opened by",stdout); ui_close(stdout,"dt"); ui_open(stdout,"dd",NULL);wo_name(auth_db,o.opened_by);
    ui_close(stdout,"dd"); ui_text(stdout,"dt",NULL,"On behalf of"); ui_open(stdout,"dd",NULL);if(!strcmp(o.on_behalf,"self"))fputs("Themselves",stdout);else html(o.on_behalf);
    ui_close(stdout,"dd");ui_open(stdout,"dt",NULL);ui_copy(stdout,"Milestones");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%lld",stages);ui_close(stdout,"dd");ui_open(stdout,"dt",NULL);ui_copy(stdout,"Terms accepted");ui_close(stdout,"dt");ui_open_a(stdout,"dd","class","is-on");printf("%lld of %lld",accepted,m);ui_close(stdout,"dd");ui_open(stdout,"dt",NULL);ui_copy(stdout,"Opened");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);wo_day(o.opened_at);
    ui_close(stdout,"dd"); ui_close(stdout,"dl"); ui_text(stdout,"p","wo-note","A stated amount is a written term. No funds are held for this work order and none move when a milestone is approved."); ui_close(stdout,"aside"); ui_close(stdout,"header");
    ui_open(stdout,"div","wo-split"); ui_open_a(stdout,"section","class","wo-stages","data-milestones",(const char *)NULL); ui_open(stdout,"header","wo-head"); ui_open(stdout,"span","ak-kicker"); fputs("Staged milestones",stdout); ui_close(stdout,"span");
    ui_open(stdout,"span","ak-tag");printf("%lld staged",stages);ui_close(stdout,"span");ui_close(stdout,"header");
    int found=0;
    if(sqlite3_prepare_v2(db,"SELECT milestone_id,sequence,title,scope,COALESCE(amount_stated,''),COALESCE(due_on,'') FROM work_order_milestone "
        "WHERE namespace='root' AND status='published' AND work_order_id=?1 ORDER BY CAST(sequence AS INTEGER),id LIMIT 30",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){found++;
            const char *mid=wo_col(s,0);long long n=wo_approvals(db,id,mid);
            ui_open_a(stdout,"article","class","ak-panel wo-stage","data-milestone",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"p","ak-kicker"); fputs("Milestone ",stdout);html(wo_col(s,1));
            fputs(n>=m&&m>0?" <span class=\"ak-tag is-on\">Approved by all</span>":"",stdout);ui_close(stdout,"p");
            if(*wo_col(s,4)){ui_open(stdout,"b","wo-amt");html(wo_col(s,4));fputs(" ",stdout);html(o.currency);ui_open(stdout,"small",NULL); fputs("stated",stdout); ui_close(stdout,"small"); ui_close(stdout,"b");}
            ui_close(stdout,"header"); ui_open(stdout,"h3",NULL);html(wo_col(s,2));ui_close(stdout,"h3"); ui_open(stdout,"p",NULL);html(wo_col(s,3));ui_close(stdout,"p");
            if(*wo_col(s,5)){ui_open(stdout,"p","wo-due"); fputs("Due ",stdout);html(wo_col(s,5));ui_close(stdout,"p");}
            ui_open_a(stdout,"div","class","wo-release","data-release-ledger",(const char *)NULL); ui_open(stdout,"div","wo-quorum"); ui_open(stdout,"span","ak-kicker"); fputs("Release sign-off",stdout); ui_close(stdout,"span");wo_signer_strip(db,id,mid,m);ui_close(stdout,"div");
            wo_decisions(db,auth_db,id,mid);
            if(party&&open)wo_signoff_form(id,mid,"Record your decision","Approve this milestone's release");
            ui_close(stdout,"div"); ui_close(stdout,"article");
        }
    }
    sqlite3_finalize(s);s=NULL;
    if(!found)ui_text(stdout, "p", "ak-empty", "No milestones are staged yet. The opener stages them before the parties accept the terms.");
    if(opener&&open&&!fixed){ui_open(stdout,"div","ak-ask wo-add"); ui_text(stdout,"p","ak-kicker","Add a milestone");wo_milestone_form(id);ui_close(stdout,"div");}
    else if(opener&&fixed)ui_text(stdout, "p", "ak-empty", "A party has accepted these terms, so the milestones are fixed as they accepted them.");
    ui_close(stdout,"section"); ui_open(stdout,"aside","wo-side"); ui_open_a(stdout,"section","class","ak-panel","data-parties",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"h2",NULL); fputs("Parties and stated terms",stdout); ui_close(stdout,"h2"); ui_close(stdout,"header");
    ui_open(stdout,"ul","wo-parties");
    if(sqlite3_prepare_v2(db,"SELECT p.party_subject,p.part,COALESCE(p.compensation_stated,''),COALESCE(p.compensation_basis,''),COALESCE(r.label,''),"
        "EXISTS(SELECT 1 FROM work_order_signoff x WHERE x.namespace=p.namespace AND x.work_order_id=p.work_order_id AND x.milestone_id='terms' AND x.signer=p.party_subject AND x.decision='approve' AND x.status='published') "
        "FROM work_order_party p LEFT JOIN role_program r ON r.namespace=p.namespace AND r.role_id=p.role_program AND r.status='published' "
        "WHERE p.namespace='root' AND p.status='published' AND p.work_order_id=?1 ORDER BY p.named_at,p.id LIMIT 40",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){
            ui_open_a(stdout,"li","data-party",(const char *)NULL); ui_open(stdout,"span",NULL); ui_open(stdout,"strong",NULL);wo_name(auth_db,wo_col(s,0));ui_close(stdout,"strong"); ui_open(stdout,"small",NULL);html(wo_col(s,1));
            if(*wo_col(s,4)){fputs(" \xc2\xb7 ",stdout);html(wo_col(s,4));}
            if(*wo_col(s,3)){ui_open(stdout,"br",NULL);html(wo_col(s,3));}
            ui_close(stdout,"small"); ui_close(stdout,"span"); ui_open(stdout,"span",NULL);
            if(*wo_col(s,2)){ui_open(stdout,"b",NULL);html(wo_col(s,2));fputs(" ",stdout);html(o.currency);ui_close(stdout,"b");}
            fputs(sqlite3_column_int(s,5)?"<em class=\"is-on\">Accepted</em>":"<em>Not accepted</em>",stdout);ui_close(stdout,"span"); ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);s=NULL;
    ui_close(stdout,"ul"); ui_text(stdout,"h3",NULL,"Terms as stated"); ui_open(stdout,"p","wo-terms");html(o.terms);ui_close(stdout,"p");
    if(party&&open){ui_open(stdout,"div","wo-accept");wo_signoff_form(id,"terms","Record your answer to these terms","Accept these terms");ui_close(stdout,"div");}
    ui_close(stdout,"section");
    if(opener&&open&&!fixed){ui_open_a(stdout,"section","class","ak-ask","data-name-party",(const char *)NULL); ui_open(stdout,"p","ak-kicker"); fputs("Name a party",stdout); ui_close(stdout,"p"); ui_open(stdout,"p",NULL); fputs("They must already have an account. Nobody is invited or told; they see this work order once named.",stdout); ui_close(stdout,"p");wo_party_form(db,id);ui_close(stdout,"section");}
    ui_open_a(stdout,"section","class","ak-panel","data-disputes",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"h2",NULL); fputs("Disputes",stdout); ui_close(stdout,"h2"); ui_close(stdout,"header"); ui_open(stdout,"ol","ak-rows");
    found=0;
    if(sqlite3_prepare_v2(db,"SELECT dispute_id,state,arbiter,opened_at FROM work_dispute WHERE namespace='root' AND status='published' AND work_order_id=?1 ORDER BY opened_at DESC LIMIT 20",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){found++;
            ui_open_a(stdout,"li","data-dispute",(const char *)NULL); ui_open(stdout,"strong",NULL);wo_code("D-",wo_col(s,0));fputs(" \xc2\xb7 ",stdout);fputs(!strcmp(wo_col(s,1),"decided")?"Decided":"Open",stdout);
            ui_close(stdout,"strong"); ui_open(stdout,"small",NULL); fputs("Arbiter ",stdout);wo_name(auth_db,wo_col(s,2));fputs(" \xc2\xb7 ",stdout);wo_day(wo_col(s,3));
            ui_close(stdout,"small"); ui_open_a(stdout,"a","class","ak-go",UI_DYN,"href","/disputes/",(wo_col(s,0))); fputs("Open hearing",stdout); ui_close(stdout,"a"); ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ol");if(!found)ui_text(stdout, "p", "ak-empty", "No dispute has been raised.");
    if(party&&open){ui_open(stdout,"details","wo-raise"); ui_text(stdout,"summary","ak-btn","Raise a dispute");wo_dispute_form(db,id);ui_close(stdout,"details");}
    ui_close(stdout,"section"); ui_close(stdout,"aside"); ui_close(stdout,"div");
    sqlite3_close(db);wo_foot();return 1;
}

/* ------------------------------------------------------- /disputes/<id> */
static int wo_dispute_page(const char *id,const char *bearer,const char *auth_db){
    char actor[64];
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){char back[80];snprintf(back,sizeof(back),"/disputes/%s",id);cjw_signin(back);return 1;}
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;
    if(!db||!wo_types(db)){if(db)sqlite3_close(db);wo_reply(503,"The dispute records are not available here yet.\n");return 1;}
    char wo[40]="",milestone[40]="",raised[64]="",arbiter[64]="",claim[6100]="",state[16]="",decision[6100]="",decided[32]="",opened[32]="";
    if(sqlite3_prepare_v2(db,"SELECT work_order_id,milestone_id,raised_by,arbiter,claim,state,COALESCE(decision,''),COALESCE(decided_at,''),opened_at FROM work_dispute "
        "WHERE namespace='root' AND status='published' AND dispute_id=?1 LIMIT 1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(s)==SQLITE_ROW){snprintf(wo,40,"%s",wo_col(s,0));snprintf(milestone,40,"%s",wo_col(s,1));snprintf(raised,64,"%s",wo_col(s,2));snprintf(arbiter,64,"%s",wo_col(s,3));
            snprintf(claim,sizeof(claim),"%s",wo_col(s,4));snprintf(state,16,"%s",wo_col(s,5));snprintf(decision,sizeof(decision),"%s",wo_col(s,6));snprintf(decided,32,"%s",wo_col(s,7));snprintf(opened,32,"%s",wo_col(s,8));}
    }
    sqlite3_finalize(s);s=NULL;
    WoOrder o;int is_arbiter=!strcmp(actor,arbiter),party=*wo&&wo_is_party(db,wo,actor);
    if(!*wo||!wo_load(db,wo,&o)||!(party||is_arbiter)){sqlite3_close(db);wo_reply(404,"This dispute does not exist or you are not part of it.\n");return 1;}
    char about[256]="The terms as a whole";
    if(strcmp(milestone,"terms")&&sqlite3_prepare_v2(db,"SELECT title FROM work_order_milestone WHERE namespace='root' AND status='published' AND milestone_id=?1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,milestone,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)==SQLITE_ROW)snprintf(about,sizeof(about),"%s",wo_col(s,0));}
    sqlite3_finalize(s);s=NULL;
    int open=!strcmp(state,"open");
    long long responses=wo_count(db,"SELECT count(*) FROM dispute_statement WHERE namespace='root' AND status='published' AND dispute_id=?1",id,NULL,NULL);
    char code[16];snprintf(code,sizeof(code),"D-%.6s",id+3);for(char *p=code;*p;p++)*p=(char)toupper((unsigned char)*p);
    psetup_open(200,about,"ak-wb work-orders hearing");
    wo_strip(code);
    ui_open(stdout,"header","ak-panel wo-docket"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","ak-kicker"); fputs("Dispute ",stdout);html(code);fputs(" \xc2\xb7 <a href=\"/work-orders/",stdout);html(wo);fputs("\">",stdout);html(o.title);
    ui_close(stdout,"a"); fputs(" \xc2\xb7 ",stdout); ui_open(stdout,"span","ak-live");fputs(open?"Awaiting the arbiter":"Decided",stdout);ui_close(stdout,"span"); ui_close(stdout,"p"); ui_open(stdout,"h1",NULL);html(about);ui_close(stdout,"h1"); ui_open(stdout,"p",NULL); fputs("Raised by ",stdout);
    wo_name(auth_db,raised);fputs(" on ",stdout);wo_day(opened);fputs(".</p></div><dl class=\"ak-stats\"><div><dt>Arbiter</dt><dd class=\"is-on\">",stdout);wo_name(auth_db,arbiter);
    ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"named by the party who raised it");ui_close(stdout,"small");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Responses");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%lld",responses);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"written into this hearing");ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"dl");ui_close(stdout,"header");
    ui_open(stdout,"div","wo-split"); ui_open_a(stdout,"section","class","ak-panel wo-feed","data-hearing-feed",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"h2",NULL); fputs("Claim and responses",stdout); ui_close(stdout,"h2"); ui_close(stdout,"header"); ui_open(stdout,"ol","wo-statements"); ui_open(stdout,"li","is-claim"); ui_open(stdout,"p","ak-kicker"); fputs("Claim \xc2\xb7 ",stdout);
    wo_name(auth_db,raised);fputs(" \xc2\xb7 ",stdout);wo_day(opened);ui_close(stdout,"p"); ui_open(stdout,"blockquote",NULL);html(claim);ui_close(stdout,"blockquote"); ui_close(stdout,"li");
    if(sqlite3_prepare_v2(db,"SELECT author,body,stated_at FROM dispute_statement WHERE namespace='root' AND status='published' AND dispute_id=?1 ORDER BY stated_at,id LIMIT 200",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){
            ui_open_a(stdout,"li","data-statement",(const char *)NULL); ui_open(stdout,"p","ak-kicker");fputs(!strcmp(wo_col(s,0),arbiter)?"Arbiter \xc2\xb7 ":"",stdout);wo_name(auth_db,wo_col(s,0));fputs(" \xc2\xb7 ",stdout);wo_day(wo_col(s,2));
            ui_close(stdout,"p"); ui_open(stdout,"blockquote",NULL);html(wo_col(s,1));ui_close(stdout,"blockquote"); ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);s=NULL;
    ui_close(stdout,"ol");
    if(open){
        BfAuthoringTree *t=calloc(1,sizeof(*t));
        if(t){FtField hidden[1]={{"dispute",id}};
            int ok=ft_action(t,"ds-say",NULL,0,"Add your response","post","/disputes/statement",hidden,1)&&
                   ft_field(t,"ds-body","ds-say",0,"Your response","text","body","","What you saw, what you hold, what you are asking for",1,"3","6000",1,"");
            if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}}
    }
    ui_close(stdout,"section"); ui_open(stdout,"aside","wo-side"); ui_open_a(stdout,"section","class","ak-panel","data-hearing-panel",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"h2",NULL); fputs("Who is in this hearing",stdout); ui_close(stdout,"h2"); ui_close(stdout,"header"); ui_open(stdout,"ul","wo-parties");
    ui_open(stdout,"li",NULL); ui_open(stdout,"span",NULL); ui_open(stdout,"strong",NULL);wo_name(auth_db,arbiter);ui_close(stdout,"strong"); ui_text(stdout,"small",NULL,"Arbiter"); ui_close(stdout,"span"); ui_open(stdout,"span",NULL);
    fputs(open?"<em>Deciding</em>":"<em class=\"is-on\">Decision written</em>",stdout);ui_close(stdout,"span"); ui_close(stdout,"li");
    if(sqlite3_prepare_v2(db,"SELECT p.party_subject,p.part,EXISTS(SELECT 1 FROM dispute_statement x WHERE x.namespace=p.namespace AND x.dispute_id=?2 AND x.author=p.party_subject AND x.status='published') "
        "FROM work_order_party p WHERE p.namespace='root' AND p.status='published' AND p.work_order_id=?1 ORDER BY p.named_at LIMIT 40",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,wo,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,id,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){
            ui_open(stdout,"li",NULL); ui_open(stdout,"span",NULL); ui_open(stdout,"strong",NULL);wo_name(auth_db,wo_col(s,0));ui_close(stdout,"strong"); ui_open(stdout,"small",NULL);html(wo_col(s,1));
            fputs(!strcmp(wo_col(s,0),raised)?" \xc2\xb7 raised it":"",stdout);ui_close(stdout,"small"); ui_close(stdout,"span"); ui_open(stdout,"span",NULL);
            fputs(!strcmp(wo_col(s,0),raised)||sqlite3_column_int(s,2)?"<em class=\"is-on\">Heard</em>":"<em>No response yet</em>",stdout);ui_close(stdout,"span"); ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ul"); ui_text(stdout,"p","wo-note","The arbiter is a person the raising party named. They are not a party to the work order, and they hold no power over anyone's account or money."); ui_close(stdout,"section"); ui_close(stdout,"aside"); ui_close(stdout,"div");
    ui_open_a(stdout,"section","class","ak-sec wo-decision","data-decision",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Decision",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL);
    fputs(open?"The arbiter's decision":"What the arbiter decided",stdout);ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_text(stdout,"p",NULL,"A decision is the arbiter's written view. It changes no sign-off and moves no money. The parties record anything that follows from it themselves."); ui_close(stdout,"header");
    if(!open){ui_open(stdout,"div","ak-panel is-raised"); ui_open(stdout,"p","ak-kicker");wo_name(auth_db,arbiter);fputs(" \xc2\xb7 ",stdout);wo_day(decided);ui_close(stdout,"p"); ui_open(stdout,"blockquote","wo-ruling");html(decision);ui_close(stdout,"blockquote"); ui_close(stdout,"div");}
    else if(is_arbiter){
        ui_open(stdout,"div","ak-ask");
        BfAuthoringTree *t=calloc(1,sizeof(*t));
        if(t){FtField hidden[1]={{"dispute",id}};FtOption confirm[1]={{"yes","I have read the claim and the responses, and this is my decision",0}};
            int ok=ft_action(t,"ds-decide",NULL,0,"Record my decision","post","/disputes/decide",hidden,1)&&
                   ft_field(t,"ds-decision","ds-decide",0,"Your decision","text","decision","","What you decide and why, in words the parties can act on",1,"5","6000",1,"")&&
                   ft_choice(t,"ds-confirm","ds-decide",1,"Confirm","confirm",confirm,1);
            if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}}
        ui_close(stdout,"div");
    }else ui_text(stdout, "p", "ak-empty", "The arbiter has not written a decision yet.");
    ui_close(stdout,"section");
    sqlite3_close(db);wo_foot();return 1;
}

/* --------------------------------------------------------------- /vouches */
static void wo_vouch_form(sqlite3 *db,const char *subject){
    BfAuthoringTree *t=calloc(1,sizeof(*t));if(!t)return;
    FtOption roles[24];char labels[24][160],values[24][96];unsigned n=0;sqlite3_stmt *s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT role_id,label FROM role_program WHERE namespace='root' AND status='published' ORDER BY label LIMIT 24",-1,&s,NULL)==SQLITE_OK)
        while(n<24&&sqlite3_step(s)==SQLITE_ROW){snprintf(values[n],96,"%s",wo_col(s,0));snprintf(labels[n],160,"%s",wo_col(s,1));roles[n].value=values[n];roles[n].label=labels[n];roles[n].checked=n==0;n++;}
    sqlite3_finalize(s);
    if(!n){bf_authoring_tree_clear(t);free(t);ui_text(stdout, "p", "ak-empty", "No operating roles are published, so there is nothing to vouch for yet.");return;}
    FtField hidden[1]={{"subject",subject?subject:""}};
    FtOption confirm[1]={{"yes","I know this person's work in this role, and I can be asked about this vouch",0}};
    int ok=ft_action(t,"vo-give",NULL,0,"Record my vouch","post","/vouches/give",hidden,subject?1:0)&&
           (subject?1:ft_field(t,"vo-email","vo-give",0,"Their account email","email","email","","name@example.org",0,"1","300",1,"email"))&&
           ft_choice(t,"vo-role","vo-give",1,"Role","role",roles,n)&&
           ft_field(t,"vo-scope","vo-give",2,"Where it applies","text","scope","global","global, community:river-valley",0,"1","95",1,"")&&
           ft_field(t,"vo-say","vo-give",3,"What you have seen of their work","text","statement","","",1,"3","2000",1,"")&&
           ft_choice(t,"vo-confirm","vo-give",4,"Confirm","confirm",confirm,1);
    if(ok)ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}
}
static int wo_vouches(const char *query,const char *bearer,const char *auth_db){
    char actor[64],subject[64]="";
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){cjw_signin("/vouches");return 1;}
    if(query&&*query){query_value(query,"for",subject,sizeof(subject));if(*subject&&(!bhh_actor(subject)||!strcmp(subject,actor))){wo_reply(400,"Choose another member's account to vouch for.\n");return 1;}}
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;
    if(!db||!cms_type_registered(db,"role_vouch")||!cms_type_registered(db,"role_program")){if(db)sqlite3_close(db);wo_reply(503,"Vouches are not available here yet.\n");return 1;}
    long long standing=wo_count(db,"SELECT count(*) FROM role_vouch WHERE namespace='root' AND status='published' AND state='standing'",NULL,NULL,NULL);
    long long given=wo_count(db,"SELECT count(*) FROM role_vouch WHERE namespace='root' AND status='published' AND state='standing' AND voucher=?1",actor,NULL,NULL);
    long long received=wo_count(db,"SELECT count(*) FROM role_vouch WHERE namespace='root' AND status='published' AND state='standing' AND vouched_subject=?1",actor,NULL,NULL);
    long long revoked=wo_count(db,"SELECT count(*) FROM role_vouch WHERE namespace='root' AND status='published' AND state='revoked'",NULL,NULL,NULL);
    psetup_open(200,"Vouches","ak-wb work-orders vouches");
    wo_strip("Vouches");
    ui_open(stdout,"header","ak-wb-hero"); ui_open(stdout,"div",NULL); ui_open(stdout,"p","ak-kicker"); fputs("Peer vouching",stdout); ui_close(stdout,"p"); ui_open(stdout,"h1",NULL); fputs("Vouch for someone in a role",stdout); ui_close(stdout,"h1"); ui_open(stdout,"p","ak-lede"); fputs("Say, in your own name, that you know someone's work in one of the published operating roles. You can take a vouch back at any time.",stdout); ui_close(stdout,"p"); ui_close(stdout,"div"); ui_open(stdout,"aside","ak-panel is-raised"); ui_open(stdout,"p","ak-kicker"); fputs("What a vouch is",stdout); ui_close(stdout,"p"); ui_open(stdout,"p",NULL); fputs("A personal statement. It appoints nobody, grants nothing and certifies nothing. A number of vouches is not an election, and every operation still checks its own authority.",stdout); ui_close(stdout,"p"); ui_close(stdout,"aside"); ui_close(stdout,"header");
    ui_open(stdout,"dl","ak-stats is-row");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Standing vouches");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%lld",standing);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"across all members");ui_close(stdout,"small");ui_close(stdout,"div");
    ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"You have given");ui_close(stdout,"dt");ui_start(stdout,"dd");printf(" class=\"%s\">%lld",given?"is-on":"",given);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"still standing");ui_close(stdout,"small");ui_close(stdout,"div");
    ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"You have received");ui_close(stdout,"dt");ui_start(stdout,"dd");printf(" class=\"%s\">%lld",received?"is-on":"",received);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"still standing");ui_close(stdout,"small");ui_close(stdout,"div");
    ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Taken back");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);printf("%lld",revoked);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"revoked by their voucher");ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"dl");
    ui_open_a(stdout,"section","class","ak-sec","data-vouch-nominations",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Who is vouched for",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL); fputs("Standing vouches by person and role",stdout); ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_open(stdout,"p",NULL); fputs("Grouped by the person vouched for and the role. Add your own if you know their work.",stdout); ui_close(stdout,"p"); ui_close(stdout,"header"); ui_open(stdout,"div","wo-ledger");
    int found=0;
    if(sqlite3_prepare_v2(db,"SELECT v.vouched_subject,v.role_program,COALESCE(r.label,v.role_program),v.context_scope,count(*),max(v.vouched_at),"
        "sum(v.voucher=?1) FROM role_vouch v LEFT JOIN role_program r ON r.namespace=v.namespace AND r.role_id=v.role_program AND r.status='published' "
        "WHERE v.namespace='root' AND v.status='published' AND v.state='standing' GROUP BY v.vouched_subject,v.role_program,v.context_scope ORDER BY max(v.vouched_at) DESC LIMIT 40",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){found++;
            const char *who=wo_col(s,0);long long n=sqlite3_column_int64(s,4);
            ui_open_a(stdout,"article","class","wo-row","data-vouch-group",(const char *)NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"p","ak-kicker");html(wo_col(s,3));ui_close(stdout,"p"); ui_open(stdout,"h3",NULL);wo_name(auth_db,who);
            ui_close(stdout,"h3"); ui_open(stdout,"small",NULL);html(wo_col(s,2));ui_close(stdout,"small"); ui_close(stdout,"div");
            ui_open(stdout,"div",NULL);ui_open(stdout,"b","wo-count");printf("%lld %s",n,n==1?"vouch":"vouches");ui_close(stdout,"b");ui_open(stdout,"small",NULL);ui_copy(stdout,"latest ");wo_day(wo_col(s,5));ui_close(stdout,"small"); ui_close(stdout,"div"); ui_open(stdout,"div",NULL); ui_close(stdout,"div");
            if(sqlite3_column_int(s,6))ui_text(stdout, "span", "ak-tag is-on", "You vouched");
            else if(strcmp(who,actor)){ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,"/vouches?for=");url_query_value(who);ui_copy(stdout,"#vouch");ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Vouch");ui_close(stdout,"a");}
            else ui_text(stdout, "span", "ak-tag", "You");
            ui_close(stdout,"article");
        }
    }
    sqlite3_finalize(s);s=NULL;
    if(!found)ui_text(stdout, "p", "ak-empty", "Nobody has vouched for anyone yet.");
    ui_close(stdout,"div"); ui_close(stdout,"section"); ui_open_a(stdout,"section","class","ak-sec","id","vouch","data-vouch-give",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Vouch",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL);
    if(*subject){fputs("Vouch for ",stdout);wo_name(auth_db,subject);}else fputs("Vouch for a member",stdout);
    ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_text(stdout,"p",NULL,"Name them by the email on their account. They are not notified; the vouch appears here under your name."); ui_close(stdout,"header"); ui_open(stdout,"div","ak-ask");
    wo_vouch_form(db,*subject?subject:NULL);
    ui_close(stdout,"div"); ui_close(stdout,"section"); ui_open_a(stdout,"section","class","ak-sec","data-vouch-ledger",(const char *)NULL); ui_open(stdout,"header",NULL); ui_open(stdout,"div",NULL); ui_open(stdout,"span","ak-kicker"); fputs("Ledger",stdout); ui_close(stdout,"span"); ui_open(stdout,"h2",NULL); fputs("Recent vouches",stdout); ui_close(stdout,"h2"); ui_close(stdout,"div"); ui_close(stdout,"header"); ui_open(stdout,"div","ak-panel ak-scroll"); ui_open(stdout,"table","ak-table"); ui_open(stdout,"thead",NULL); ui_open(stdout,"tr",NULL); ui_open(stdout,"th",NULL); fputs("Date",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Voucher",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("For",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Role",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("Scope",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); fputs("State",stdout); ui_close(stdout,"th"); ui_open(stdout,"th",NULL); ui_close(stdout,"th"); ui_close(stdout,"tr"); ui_close(stdout,"thead"); ui_open(stdout,"tbody",NULL);
    found=0;
    if(sqlite3_prepare_v2(db,"SELECT v.vouch_id,v.voucher,v.vouched_subject,COALESCE(r.label,v.role_program),v.context_scope,v.state,v.vouched_at,v.statement "
        "FROM role_vouch v LEFT JOIN role_program r ON r.namespace=v.namespace AND r.role_id=v.role_program AND r.status='published' "
        "WHERE v.namespace='root' AND v.status='published' ORDER BY v.vouched_at DESC,v.id DESC LIMIT 50",-1,&s,NULL)==SQLITE_OK){
        while(sqlite3_step(s)==SQLITE_ROW){found++;
            int mine=!strcmp(wo_col(s,1),actor),standing_row=!strcmp(wo_col(s,5),"standing");
            ui_open_a(stdout,"tr","data-vouch",(const char *)NULL); ui_open(stdout,"td",NULL);wo_day(wo_col(s,6));ui_close(stdout,"td"); ui_open(stdout,"td",NULL);wo_name(auth_db,wo_col(s,1));ui_close(stdout,"td"); ui_open(stdout,"td",NULL); ui_open(stdout,"strong",NULL);wo_name(auth_db,wo_col(s,2));
            ui_close(stdout,"strong"); ui_open(stdout,"br",NULL); ui_open(stdout,"small",NULL);html(wo_col(s,7));ui_close(stdout,"small"); ui_close(stdout,"td"); ui_open(stdout,"td",NULL);html(wo_col(s,3));ui_close(stdout,"td"); ui_open(stdout,"td",NULL); ui_open(stdout,"code",NULL);html(wo_col(s,4));ui_close(stdout,"code"); ui_close(stdout,"td"); ui_open(stdout,"td",NULL);
            fputs(standing_row?"<span class=\"ak-tag is-on\">Standing</span>":"<span class=\"ak-tag\">Revoked</span>",stdout);ui_close(stdout,"td"); ui_open(stdout,"td",NULL);
            if(mine&&standing_row){
                BfAuthoringTree *t=calloc(1,sizeof(*t));
                if(t){char fid[48];snprintf(fid,sizeof(fid),"vr-%.40s",wo_col(s,0));FtField hidden[1]={{"vouch",wo_col(s,0)}};
                    if(ft_action(t,fid,NULL,0,"Take back","post","/vouches/revoke",hidden,1))ft_render(t,stdout);else{bf_authoring_tree_clear(t);free(t);}}
            }
            ui_close(stdout,"td"); ui_close(stdout,"tr");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"tbody"); ui_close(stdout,"table");if(!found)ui_text(stdout, "p", "ak-empty", "No vouches recorded yet.");ui_close(stdout,"div"); ui_close(stdout,"section");
    sqlite3_close(db);wo_foot();return 1;
}

/* ------------------------------------------------------------- the writes */
static int wo_write(const char *type,const char *row,WoJson *j,const char *f1,const char *v1){
    return ph_cms(row?"update":"create",type,row,wo_json_end(j,row?NULL:"published"),"root",f1,v1,NULL,NULL);
}
static int wo_account(const char *auth_db,const char *email,const char *subject,char out[64]){
    if(email&&*email)return strlen(email)<300&&strchr(email,'@')&&hhm_account(auth_db,email,NULL,out,64,1);
    return subject&&bhh_actor(subject)&&hhm_account(auth_db,NULL,subject,out,64,1);
}
static int wo_amount_ok(const char *a){return !*a||(strlen(a)<24&&strspn(a,"0123456789.,")==strlen(a)&&strspn(a,".,")<strlen(a));}
static int serve_work_order_writes(const char *method,const char *target,const char *body,const char *raw,
                                   const char *host,const char *bearer,const char *auth_db){
    static const char *routes[]={"/work-orders/open","/work-orders/party","/work-orders/milestone","/work-orders/signoff",
                                 "/disputes/open","/disputes/statement","/disputes/decide","/vouches/give","/vouches/revoke"};
    int which=-1;for(int i=0;i<9;i++)if(!strcmp(target,routes[i]))which=i;
    if(which<0)return 0;
    char actor[64],now[32],ref[40],key[400];
    if(!wo_post_actor(method,raw,host,body,bearer,auth_db,actor))return 1;
    wo_now(now);
    sqlite3 *db=cms_open_ro();
    if(!db||!wo_types(db)||!cms_type_registered(db,"role_vouch")){if(db)sqlite3_close(db);wo_reply(503,"These records are not available here yet; nothing was saved.\n");return 1;}
    WoJson *j=malloc(sizeof(WoJson));if(!j){sqlite3_close(db);wo_reply(503,"Nothing was saved.\n");return 1;}
    wo_json_begin(j);
    char wo[40]="";WoOrder o;int done=0;
#define WO_FAIL(code,msg) do{wo_reply(code,msg);goto out;}while(0)
    if(which<=3||which==4){
        if(which>0){
            if(!query_value(body,"wo",wo,sizeof(wo))||!wo_ref_ok(wo,"wo-")||!wo_load(db,wo,&o))WO_FAIL(404,"That work order does not exist.\n");
            if(!wo_is_party(db,wo,actor))WO_FAIL(403,"Only the people named on this work order can record anything on it.\n");
            if(strcmp(o.state,"open"))WO_FAIL(409,"This work order is closed.\n");
        }
    }
    if(which==0){ /* open */
        char title[256],scope[6100],terms[6100],currency[16],context[96]="self",cactor[160],csubject[160];const char *cscope=NULL;
        if(!wo_text(body,"title",title,sizeof(title),1)||strlen(title)>200||!wo_text(body,"scope",scope,sizeof(scope),1)||strlen(scope)>6000||
           !wo_text(body,"terms",terms,sizeof(terms),1)||strlen(terms)>6000||!wo_text(body,"currency",currency,sizeof(currency),1)||
           strspn(currency,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")!=strlen(currency))
            WO_FAIL(400,"Give the work a title, a scope, the terms you are stating and a currency written in letters.\n");
        query_value(body,"context",context,sizeof(context));
        if(strcmp(context,"self")&&psetup_context(bearer,auth_db,context,2,cactor,sizeof(cactor),csubject,sizeof(csubject),&cscope)!=1)
            WO_FAIL(403,"You can open a work order for yourself, or for an organisation or household where you currently hold write access.\n");
        snprintf(key,sizeof(key),"%s|%s",now,title);govern_ref(ref,sizeof(ref),"work-order",actor,key);snprintf(wo,sizeof(wo),"wo-%s",ref);
        wo_json(j,"work_order_id",wo);wo_json(j,"title",title);wo_json(j,"scope",scope);wo_json(j,"on_behalf_of",context);wo_json(j,"opened_by",actor);
        wo_json(j,"stated_terms",terms);wo_json(j,"currency",currency);wo_json(j,"state","open");wo_json(j,"opened_at",now);
        wo_json(j,"cannot_infer","a work order is what its opener proposed: it is not a contract until each named party accepts it, it holds no money, and it authorises nobody to act for anyone else");
        if(!wo_write("work_order",NULL,j,NULL,NULL)||!wo_row(db,"work_order","work_order_id",wo))WO_FAIL(503,"The work order was not recorded. Nothing was saved.\n");
        govern_ref(ref,sizeof(ref),"party",wo,actor);char pid[40];snprintf(pid,sizeof(pid),"wp-%s",ref);
        wo_json_begin(j);wo_json(j,"party_id",pid);wo_json(j,"work_order_id",wo);wo_json(j,"party_subject",actor);wo_json(j,"part","Opener");wo_json(j,"role_program","");
        wo_json(j,"compensation_stated","");wo_json(j,"compensation_basis","");wo_json(j,"named_by",actor);wo_json(j,"named_at",now);
        wo_json(j,"cannot_infer","being named on a work order is not agreeing to it; stated compensation is a written term, never a payment or balance");
        if(!wo_write("work_order_party",NULL,j,NULL,NULL))WO_FAIL(503,"The work order was recorded but you were not named on it. Reload before retrying.\n");
        wo_see("/work-orders/",wo);done=1;
    }else if(which==1||which==2){ /* party, milestone: the opener composes until the terms are accepted */
        if(strcmp(actor,o.opened_by))WO_FAIL(403,"Only the person who opened this work order can change its parties or milestones.\n");
        if(wo_terms_fixed(db,&o))WO_FAIL(409,"A party has accepted these terms, so they can no longer change.\n");
        if(which==1){
            char email[320],part[80],comp[32],basis[700],role[100]="",who[64];
            if(!wo_text(body,"email",email,sizeof(email),1)||!wo_text(body,"part",part,sizeof(part),1)||strlen(part)>60||
               !wo_text(body,"compensation",comp,sizeof(comp),0)||!wo_amount_ok(comp)||!wo_text(body,"basis",basis,sizeof(basis),0)||strlen(basis)>600)
                WO_FAIL(400,"Name the party by account email and the part they play; a stated amount is digits only.\n");
            query_value(body,"role",role,sizeof(role));
            if(*role&&!govern_published_id(db,"role_program","role_id",role))WO_FAIL(400,"That role is not in the published registry.\n");
            if(!wo_account(auth_db,email,NULL,who))WO_FAIL(404,"No account uses that email. Nothing was sent: this page never invites anyone.\n");
            if(wo_is_party(db,wo,who))WO_FAIL(409,"That person is already named on this work order.\n");
            govern_ref(ref,sizeof(ref),"party",wo,who);char pid[40];snprintf(pid,sizeof(pid),"wp-%s",ref);
            wo_json(j,"party_id",pid);wo_json(j,"work_order_id",wo);wo_json(j,"party_subject",who);wo_json(j,"part",part);wo_json(j,"role_program",role);
            wo_json(j,"compensation_stated",comp);wo_json(j,"compensation_basis",basis);wo_json(j,"named_by",actor);wo_json(j,"named_at",now);
            wo_json(j,"cannot_infer","being named on a work order is not agreeing to it; stated compensation is a written term, never a payment or balance");
            if(!wo_write("work_order_party",NULL,j,NULL,NULL)||!wo_is_party(db,wo,who))WO_FAIL(503,"The party was not recorded.\n");
        }else{
            char title[256],scope[6100],amount[32],due[16];
            if(!wo_text(body,"title",title,sizeof(title),1)||strlen(title)>200||!wo_text(body,"scope",scope,sizeof(scope),1)||strlen(scope)>6000||
               !wo_text(body,"amount",amount,sizeof(amount),0)||!wo_amount_ok(amount)||!wo_text(body,"due",due,sizeof(due),0)||
               (*due&&(strlen(due)!=10||due[4]!='-'||due[7]!='-'||strspn(due,"0123456789-")!=10)))
                WO_FAIL(400,"Give the milestone a title and what counts as done; amounts are digits and dates are YYYY-MM-DD.\n");
            char seq[16];snprintf(seq,sizeof(seq),"%02lld",wo_count(db,"SELECT count(*) FROM work_order_milestone WHERE namespace='root' AND status='published' AND work_order_id=?1",wo,NULL,NULL)+1);
            if(atoi(seq)>30)WO_FAIL(409,"A work order holds at most 30 milestones.\n");
            snprintf(key,sizeof(key),"%s|%s",now,title);govern_ref(ref,sizeof(ref),"milestone",wo,key);char mid[40];snprintf(mid,sizeof(mid),"wm-%s",ref);
            wo_json(j,"milestone_id",mid);wo_json(j,"work_order_id",wo);wo_json(j,"sequence",seq);wo_json(j,"title",title);wo_json(j,"scope",scope);
            wo_json(j,"amount_stated",amount);wo_json(j,"due_on",due);
            wo_json(j,"cannot_infer","a milestone is a written stage; its amount is a stated term, not money held, and it is no evidence the work was done");
            if(!wo_write("work_order_milestone",NULL,j,NULL,NULL)||!wo_row(db,"work_order_milestone","milestone_id",mid))WO_FAIL(503,"The milestone was not recorded.\n");
        }
        wo_see("/work-orders/",wo);done=1;
    }else if(which==3){ /* signoff: one decision per party per stage */
        char milestone[40],decision[16],note[700];
        if(!query_value(body,"milestone",milestone,sizeof(milestone))||(strcmp(milestone,"terms")&&!wo_ref_ok(milestone,"wm-"))||!wo_milestone_of(db,wo,milestone)||
           !query_value(body,"decision",decision,sizeof(decision))||(strcmp(decision,"approve")&&strcmp(decision,"withhold"))||
           !wo_text(body,"note",note,sizeof(note),0)||strlen(note)>600)
            WO_FAIL(400,"Choose a stage of this work order and approve or withhold.\n");
        snprintf(key,sizeof(key),"%s|%s",wo,milestone);govern_ref(ref,sizeof(ref),"signoff",key,actor);char sid[40];snprintf(sid,sizeof(sid),"so-%s",ref);
        long long row=wo_row(db,"work_order_signoff","signoff_id",sid);char rowid[24];snprintf(rowid,sizeof(rowid),"%lld",row);
        if(!row){wo_json(j,"signoff_id",sid);wo_json(j,"work_order_id",wo);wo_json(j,"milestone_id",milestone);wo_json(j,"signer",actor);
            wo_json(j,"cannot_infer","one party's own decision on one stage; a count of approvals moves no funds, settles nothing and is never a payment instruction");}
        wo_json(j,"decision",decision);wo_json(j,"note",note);wo_json(j,"signed_at",now);
        if(!wo_write("work_order_signoff",row?rowid:NULL,j,row?"signer":NULL,row?actor:NULL))WO_FAIL(503,"Your decision was not recorded.\n");
        sqlite3_stmt *s=NULL;int saved=0;
        if(sqlite3_prepare_v2(db,"SELECT decision FROM work_order_signoff WHERE namespace='root' AND status='published' AND signoff_id=?1",-1,&s,NULL)==SQLITE_OK){
            sqlite3_bind_text(s,1,sid,-1,SQLITE_STATIC);saved=sqlite3_step(s)==SQLITE_ROW&&!strcmp(wo_col(s,0),decision);}
        sqlite3_finalize(s);
        if(!saved)WO_FAIL(503,"Your decision was not confirmed. Reload before retrying.\n");
        wo_see("/work-orders/",wo);done=1;
    }else if(which==4){ /* a party raises a dispute and names an arbiter */
        char milestone[40]="terms",email[320],claim[6100],arbiter[64];
        query_value(body,"milestone",milestone,sizeof(milestone));
        if((strcmp(milestone,"terms")&&!wo_ref_ok(milestone,"wm-"))||!wo_milestone_of(db,wo,milestone)||!wo_text(body,"arbiter",email,sizeof(email),1)||
           !wo_text(body,"claim",claim,sizeof(claim),1)||strlen(claim)>6000)
            WO_FAIL(400,"Say what the dispute is about, name an arbiter by account email and write your claim.\n");
        if(!wo_account(auth_db,email,NULL,arbiter))WO_FAIL(404,"No account uses that arbiter email. Nothing was sent.\n");
        if(wo_is_party(db,wo,arbiter))WO_FAIL(409,"The arbiter cannot be a party to this work order.\n");
        snprintf(key,sizeof(key),"%s|%s",now,milestone);govern_ref(ref,sizeof(ref),"dispute",wo,key);char did[40];snprintf(did,sizeof(did),"dp-%s",ref);
        wo_json(j,"dispute_id",did);wo_json(j,"work_order_id",wo);wo_json(j,"milestone_id",milestone);wo_json(j,"raised_by",actor);wo_json(j,"arbiter",arbiter);
        wo_json(j,"claim",claim);wo_json(j,"state","open");wo_json(j,"decision","");wo_json(j,"decided_at","");wo_json(j,"opened_at",now);
        wo_json(j,"cannot_infer","a dispute records a claim and the arbiter its raiser named; a decision is one person's written view, not enforcement, a court finding or a movement of money");
        if(!wo_write("work_dispute",NULL,j,NULL,NULL)||!wo_row(db,"work_dispute","dispute_id",did))WO_FAIL(503,"The dispute was not recorded.\n");
        wo_see("/disputes/",did);done=1;
    }else if(which==5||which==6){ /* statements and the arbiter's decision */
        char did[40],arbiter[64]="",state[16]="";sqlite3_stmt *s=NULL;long long row=0;
        if(!query_value(body,"dispute",did,sizeof(did))||!wo_ref_ok(did,"dp-"))WO_FAIL(404,"That dispute does not exist.\n");
        if(sqlite3_prepare_v2(db,"SELECT id,work_order_id,arbiter,state FROM work_dispute WHERE namespace='root' AND status='published' AND dispute_id=?1",-1,&s,NULL)==SQLITE_OK){
            sqlite3_bind_text(s,1,did,-1,SQLITE_STATIC);
            if(sqlite3_step(s)==SQLITE_ROW){row=sqlite3_column_int64(s,0);snprintf(wo,sizeof(wo),"%s",wo_col(s,1));snprintf(arbiter,sizeof(arbiter),"%s",wo_col(s,2));snprintf(state,sizeof(state),"%s",wo_col(s,3));}}
        sqlite3_finalize(s);
        if(!row)WO_FAIL(404,"That dispute does not exist.\n");
        if(strcmp(state,"open"))WO_FAIL(409,"This dispute has been decided.\n");
        if(which==5){
            char text[6100];
            if(strcmp(actor,arbiter)&&!wo_is_party(db,wo,actor))WO_FAIL(403,"Only the parties and the arbiter can write into this hearing.\n");
            if(!wo_text(body,"body",text,sizeof(text),1)||strlen(text)>6000)WO_FAIL(400,"Write your response.\n");
            snprintf(key,sizeof(key),"%s|%s",now,actor);govern_ref(ref,sizeof(ref),"statement",did,key);char sid[40];snprintf(sid,sizeof(sid),"st-%s",ref);
            wo_json(j,"statement_id",sid);wo_json(j,"dispute_id",did);wo_json(j,"author",actor);wo_json(j,"body",text);wo_json(j,"stated_at",now);
            wo_json(j,"cannot_infer","a statement is what its author said; this estate has not verified it and it is not a finding");
            if(!wo_write("dispute_statement",NULL,j,NULL,NULL)||!wo_row(db,"dispute_statement","statement_id",sid))WO_FAIL(503,"Your response was not recorded.\n");
        }else{
            char text[6100],confirm[8]="";
            if(strcmp(actor,arbiter))WO_FAIL(403,"Only the named arbiter can write the decision.\n");
            query_value(body,"confirm",confirm,sizeof(confirm));
            if(strcmp(confirm,"yes")||!wo_text(body,"decision",text,sizeof(text),1)||strlen(text)>6000)WO_FAIL(400,"Write the decision and confirm it is yours.\n");
            char rowid[24];snprintf(rowid,sizeof(rowid),"%lld",row);
            wo_json(j,"state","decided");wo_json(j,"decision",text);wo_json(j,"decided_at",now);
            ph_cms("update","work_dispute",rowid,wo_json_end(j,NULL),"root","arbiter",actor,"state","open");
            if(wo_count(db,"SELECT count(*) FROM work_dispute WHERE namespace='root' AND dispute_id=?1 AND state='decided' AND arbiter=?2",did,actor,NULL)!=1)
                WO_FAIL(503,"The decision was not confirmed. Reload the hearing before retrying.\n");
        }
        wo_see("/disputes/",did);done=1;
    }else if(which==7){ /* vouch */
        char email[320]="",subject[64]="",who[64],role[100],scope[100],statement[2100],confirm[8]="";
        query_value(body,"email",email,sizeof(email));query_value(body,"subject",subject,sizeof(subject));query_value(body,"confirm",confirm,sizeof(confirm));
        if(!query_value(body,"role",role,sizeof(role))||!*role||!query_value(body,"scope",scope,sizeof(scope))||!*scope||strlen(scope)>95||
           strspn(scope,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:.")!=strlen(scope)||
           !wo_text(body,"statement",statement,sizeof(statement),1)||strlen(statement)>2000||strcmp(confirm,"yes"))
            WO_FAIL(400,"Choose a role and scope, say what you have seen of their work and confirm the vouch is yours.\n");
        if(!govern_published_id(db,"role_program","role_id",role))WO_FAIL(400,"That role is not in the published registry.\n");
        if(!wo_account(auth_db,*email?email:NULL,*email?NULL:subject,who))WO_FAIL(404,"No account matches. Nothing was sent.\n");
        if(!strcmp(who,actor))WO_FAIL(409,"You cannot vouch for yourself.\n");
        snprintf(key,sizeof(key),"%s|%s|%s",who,role,scope);govern_ref(ref,sizeof(ref),"vouch",actor,key);char vid[40];snprintf(vid,sizeof(vid),"vo-%s",ref);
        long long row=wo_row(db,"role_vouch","vouch_id",vid);char rowid[24];snprintf(rowid,sizeof(rowid),"%lld",row);
        if(!row){wo_json(j,"vouch_id",vid);wo_json(j,"voucher",actor);wo_json(j,"vouched_subject",who);wo_json(j,"role_program",role);wo_json(j,"context_scope",scope);
            wo_json(j,"cannot_infer","a vouch is one person's revocable statement; it is not an appointment, grant, certification, employment or permission to act, and a count of vouches is not an election");}
        wo_json(j,"statement",statement);wo_json(j,"state","standing");wo_json(j,"vouched_at",now);wo_json(j,"revoked_at","");
        if(!wo_write("role_vouch",row?rowid:NULL,j,row?"voucher":NULL,row?actor:NULL)||
           wo_count(db,"SELECT count(*) FROM role_vouch WHERE namespace='root' AND vouch_id=?1 AND state='standing' AND voucher=?2",vid,actor,NULL)!=1)
            WO_FAIL(503,"Your vouch was not recorded.\n");
        wo_see("/vouches","");done=1;
    }else{ /* revoke your own vouch */
        char vid[40];
        if(!query_value(body,"vouch",vid,sizeof(vid))||!wo_ref_ok(vid,"vo-"))WO_FAIL(404,"That vouch does not exist.\n");
        long long row=wo_count(db,"SELECT id FROM role_vouch WHERE namespace='root' AND status='published' AND vouch_id=?1 AND voucher=?2",vid,actor,NULL);
        if(!row)WO_FAIL(404,"That vouch is not yours or does not exist.\n");
        char rowid[24];snprintf(rowid,sizeof(rowid),"%lld",row);
        wo_json(j,"state","revoked");wo_json(j,"revoked_at",now);
        ph_cms("update","role_vouch",rowid,wo_json_end(j,NULL),"root","voucher",actor,NULL,NULL);
        if(wo_count(db,"SELECT count(*) FROM role_vouch WHERE namespace='root' AND vouch_id=?1 AND state='revoked'",vid,NULL,NULL)!=1)WO_FAIL(503,"The vouch was not taken back. Reload before retrying.\n");
        wo_see("/vouches","");done=1;
    }
#undef WO_FAIL
 out:
    (void)done;free(j);sqlite3_close(db);return 1;
}

/* ------------------------------------------------------------ the reads */
static int serve_work_orders(const BfRequest *r) {
    const char *method=r->method,*target=r->target,*query=r->query,*body=r->body,*raw=r->raw,*host=r->host,*bearer=r->bearer,*auth_db=r->auth_db,*accept=r->accept;
    (void)method;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
    if(serve_work_order_writes(method,target,body,raw,host,bearer,auth_db))return 1;
    if(!wo_host(host)){wo_reply(404,"Work orders are not kept at this address.\n");return 1;}
    if(strcmp(method,"GET")){wo_reply(405,"Read this page with GET.\n");return 1;}
    if(bf_request_is(r,"index"))return wo_index(host,bearer,auth_db);
    if(bf_request_is(r,"vouches"))return wo_vouches(query,bearer,auth_db);
    if(bf_request_is(r,"order")&&wo_ref_ok(target+13,"wo-"))return wo_detail(target+13,bearer,auth_db);
    if(bf_request_is(r,"dispute")&&wo_ref_ok(target+10,"dp-"))return wo_dispute_page(target+10,bearer,auth_db);
    wo_reply(404,"No such work order, dispute or vouch page.\n");return 1;
}
#endif
