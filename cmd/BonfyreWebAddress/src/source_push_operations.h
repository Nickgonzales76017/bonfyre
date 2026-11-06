#ifndef BONFYRE_SOURCE_PUSH_OPERATIONS_H
#define BONFYRE_SOURCE_PUSH_OPERATIONS_H
/* Institution-operator view over the existing WebSub carrier. This file does
 * not change a lease, request a subscription, accept a delivery, or fetch any
 * foreign material. The named native WebSub and ForeignTwin owners do that. */
static void spo_domain(const char *url,char out[192]) {
    out[0]=0;
    if(!url)return;
    const char *p=strstr(url,"://");
    if(!p||((size_t)(p-url)!=4&&(size_t)(p-url)!=5))return;
    p+=3;
    const char *end=p;
    while(*end&&*end!='/'&&*end!='?'&&*end!='#')end++;
    size_t n=(size_t)(end-p);
    if(!n||n>=192||memchr(p,'@',n))return;
    for(size_t i=0;i<n;i++){
        unsigned char c=(unsigned char)p[i];
        if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||
             (c>='0'&&c<='9')||c=='.'||c=='-'||c==':'||
             c=='['||c==']'))return;
    }
    memcpy(out,p,n);out[n]=0;
}
static int spo_state_ok(const char *filter) {
    static const char *states[]={"all","active","pending","expired","dirty","rejected","unsubscribed","failed"};
    for(size_t i=0;i<sizeof(states)/sizeof(states[0]);i++)
        if(!strcmp(states[i],filter))return 1;
    return 0;
}
static void spo_filters(const char *q,const char *state) {
    fputs("<form method=\"get\" action=\"/sources/push\" class=\"spo-filters\">"
          "<label>Search known topic or hub<input type=\"search\" name=\"q\" maxlength=\"80\" value=\"",stdout);
    html(q);
    fputs("\" placeholder=\"Publisher or host\"></label><label>Transport state"
          "<select name=\"state\">",stdout);
    static const char *states[]={"all","active","pending","expired","dirty","rejected","unsubscribed","failed"};
    static const char *labels[]={"All","Active","Awaiting challenge","Lease expired","Source changed","Rejected deliveries","Unsubscribed","Failed"};
    for(size_t i=0;i<sizeof(states)/sizeof(states[0]);i++){
        fputs("<option value=\"",stdout);html(states[i]);fputs("\"",stdout);
        if(!strcmp(state,states[i]))fputs(" selected",stdout);
        fputs(">",stdout);html(labels[i]);fputs("</option>",stdout);
    }
    fputs("</select></label><button type=\"submit\">Filter subscriptions</button></form>",stdout);
}
static int serve_source_push_operations(const char *method,const char *target,
                                        const char *query,const char *host,
                                        const char *bearer,const char *auth_db) {
    if(strcmp(target,"/sources/push")&&strcmp(target,"/sources/push/"))return 0;
    if(strcmp(method,"GET")){aw_error(405,"Read this operator console with GET.\n");return 1;}
    if(!host||strcmp(host,"aurekai.work")){aw_error(404,"This operator console is not on this host.\n");return 1;}
    char actor[160],subject[160];const char *scope=NULL;
    if(psetup_context(bearer,auth_db,"institution:bonfyre",1,
                     actor,sizeof(actor),subject,sizeof(subject),&scope)!=1||
       strcmp(scope,"institution")||strcmp(subject,"institution:bonfyre")){
        aw_error(403,"An active Bonfyre institution administrator is required.\n");return 1;
    }
    char q[81]="",state[24]="all",pageno[12]="";
    if(query){
        if(strlen(query)>2048){aw_error(400,"Bound the source filter.\n");return 1;}
        query_value(query,"q",q,sizeof(q));query_value(query,"state",state,sizeof(state));
        query_value(query,"page",pageno,sizeof(pageno));
    }
    if(!*state)snprintf(state,sizeof(state),"all");
    if(!spo_state_ok(state)||(*pageno&&(strspn(pageno,"0123456789")!=strlen(pageno)||strlen(pageno)>4))){
        aw_error(400,"Choose a supported filter and page.\n");return 1;
    }
    unsigned page=*pageno?(unsigned)strtoul(pageno,NULL,10):0;
    if(page>500){aw_error(400,"Page exceeds the bounded operator view.\n");return 1;}
    sqlite3 *db=websub_open(0);
    if(!db){aw_error(503,"The WebSub carrier store is not connected.\n");return 1;}
    sqlite3_stmt *st=NULL;
    const char *sql=
        "SELECT s.topic,s.hub,s.state,COALESCE(s.lease_expires,0),"
        "COALESCE(s.verified_at,''),COALESCE(s.last_delivery_at,''),"
        "COALESCE(s.deliveries,0),COALESCE(s.rejected,0),"
        "COALESCE(d.state,'none'),COALESCE(d.attempts,0),"
        "CASE WHEN COALESCE(s.authority_receipt,'')<>'' THEN 1 ELSE 0 END "
        "FROM subscriptions s LEFT JOIN source_dirty d ON d.token=s.token "
        "WHERE (?1='' OR instr(lower(s.topic),lower(?1))>0 OR instr(lower(s.hub),lower(?1))>0) "
        "AND (?2='all' OR s.state=?2 "
        "OR (?2='expired' AND s.state='active' AND s.lease_expires<=strftime('%s','now')) "
        "OR (?2='dirty' AND d.state IN('pending','running')) "
        "OR (?2='rejected' AND s.rejected>0)) "
        "ORDER BY CASE WHEN s.state='active' THEN 0 ELSE 1 END,"
        "s.lease_expires ASC,s.topic ASC LIMIT 26 OFFSET ?3";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK){
        sqlite3_close(db);aw_error(503,"WebSub carrier schema is not ready.\n");return 1;
    }
    sqlite3_bind_text(st,1,q,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,state,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,3,(int)page*25);
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
          "Cache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\n"
          "Referrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\n"
          "Connection: close\r\n\r\n<!doctype html><html lang=\"en\"><head>"
          "<meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<title>WebSub delivery operations · Aurekai Work</title>"
          "<link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=2\">"
          "<style>"
          ".spo{max-width:1250px;margin:20px auto 70px;padding:0 clamp(14px,4vw,50px)}"
          ".spo h1{font:400 clamp(30px,5vw,58px)/1.08 Georgia,serif;margin:13px 0}"
          ".spo p{line-height:1.5;max-width:75ch}.spo small{font:12px/1.5 system-ui}"
          ".spo-filters{display:flex;flex-wrap:wrap;align-items:end;gap:10px;margin:24px 0}"
          ".spo-filters label{display:grid;gap:5px;flex:1;min-width:min(100%,190px)}"
          ".spo-filters input,.spo-filters select,.spo-filters button{font:14px system-ui;min-height:44px;padding:9px;border-radius:8px}"
          ".spo-list{list-style:none;margin:0;padding:0;display:grid;gap:12px}"
          ".spo-list details{border:1px solid var(--w-rule,#46564d);border-radius:13px;overflow:hidden;background:var(--w-panel,#17221a)}"
          ".spo-list summary{cursor:pointer;padding:16px;display:flex;gap:12px;flex-wrap:wrap;justify-content:space-between;align-items:center}"
          ".spo-list summary strong{overflow-wrap:anywhere}.spo-list dl{padding:0 16px 16px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px}"
          ".spo-list dt{font:600 12px system-ui}.spo-list dd{margin:4px 0 0;overflow-wrap:anywhere}"
          ".spo-steps{border-block:1px solid var(--w-rule,#46564d);padding:15px 0;margin:24px 0}"
          ".spo-paging{display:flex;gap:20px;margin-top:20px}"
          ".spo :is(button,input,select,a,summary):focus-visible{outline:2px solid var(--w-accent,#d0fc4f);outline-offset:3px}"
          "@media(max-width:390px){.spo-list dl{grid-template-columns:1fr}.spo-filters{display:grid}.spo-filters label{min-width:0}}"
          "</style></head><body><main class=\"spo\" data-source-push-operations>"
          "<nav aria-label=\"Breadcrumb\"><a href=\"/\">Work</a> / <a href=\"/sources\">Sources</a> / Push delivery</nav>"
          "<header><h1>WebSub delivery operations</h1>"
          "<p>Inspect the native transport. A challenge confirms that the hub knows the topic; "
          "an HMAC check admits a callback; a dirty source requests separate ForeignTwin observation. "
          "None of these alone establishes a new fact or publishes material.</p></header>",stdout);
    spo_filters(q,state);
    fputs("<ol class=\"spo-list\" data-source-push-list>",stdout);
    int n=0;time_t now=time(NULL);int rc;
    while((rc=sqlite3_step(st))==SQLITE_ROW&&n<25){
        const char *topic=(const char*)sqlite3_column_text(st,0);
        const char *hub=(const char*)sqlite3_column_text(st,1);
        const char *rawstate=(const char*)sqlite3_column_text(st,2);
        sqlite3_int64 lease=sqlite3_column_int64(st,3);
        const char *verified=(const char*)sqlite3_column_text(st,4);
        const char *delivered=(const char*)sqlite3_column_text(st,5);
        sqlite3_int64 deliveries=sqlite3_column_int64(st,6),rejected=sqlite3_column_int64(st,7);
        const char *dirty=(const char*)sqlite3_column_text(st,8);
        int attempts=sqlite3_column_int(st,9),receipt=sqlite3_column_int(st,10);
        char origin[192],hubname[192],digest[65];
        spo_domain(topic,origin);spo_domain(hub,hubname);
        bf_sha256_hex((const uint8_t*)(topic?topic:""),topic?strlen(topic):0,digest);
        const char *standing=!strcmp(rawstate?rawstate:"","active")&&lease<=now?
            "expired":(rawstate?rawstate:"unknown");
        n++;
        fputs("<li data-source-push-row data-state=\"",stdout);html(standing);
        fputs("\"><details><summary><strong>",stdout);
        html(*origin?origin:"Source address withheld");fputs(" <small>topic ",stdout);
        digest[12]=0;html(digest);fputs("</small></strong><span>",stdout);html(standing);
        fputs("</span></summary><dl><div><dt>Hub</dt><dd>",stdout);
        html(*hubname?hubname:"Address withheld");fputs("</dd></div><div><dt>Lease</dt><dd>",stdout);
        if(lease>0){printf("%lld seconds %s",(long long)(lease-(sqlite3_int64)now),lease<=now?"past expiry":"remaining");}
        else fputs("Not verified",stdout);
        fputs("</dd></div><div><dt>Challenge accepted</dt><dd>",stdout);
        html(verified&&*verified?verified:"Not observed");fputs("</dd></div><div><dt>Accepted deliveries</dt><dd>",stdout);
        printf("%lld",(long long)deliveries);
        fputs("</dd></div><div><dt>Rejected callbacks</dt><dd>",stdout);
        printf("%lld",(long long)rejected);
        fputs("</dd></div><div><dt>Last accepted delivery</dt><dd>",stdout);
        html(delivered&&*delivered?delivered:"Never");fputs("</dd></div><div><dt>Separate source observation</dt><dd>",stdout);
        html(dirty&&*dirty?dirty:"none");printf(" · %d attempts",attempts);
        fputs("</dd></div><div><dt>Observation authority</dt><dd>",stdout);
        fputs(receipt?"Stored receipt present (not revalidated here)":"No retained authority receipt",stdout);
        fputs("</dd></div></dl></details></li>",stdout);
    }
    int more=rc==SQLITE_ROW;
    sqlite3_finalize(st);sqlite3_close(db);
    if(!n)fputs("<li>No matching subscriptions in the retained native carrier.</li>",stdout);
    fputs("</ol><nav class=\"spo-paging\" aria-label=\"Subscription pages\">",stdout);
    if(page){fputs("<a href=\"/sources/push?page=",stdout);printf("%u",page-1);
        fputs("&amp;state=",stdout);html(state);fputs("&amp;q=",stdout);url_query_value(q);
        fputs("\">Previous subscriptions</a>",stdout);}
    if(more&&page<500){fputs("<a href=\"/sources/push?page=",stdout);printf("%u",page+1);
        fputs("&amp;state=",stdout);html(state);fputs("&amp;q=",stdout);url_query_value(q);
        fputs("\">More subscriptions</a>",stdout);}
    fputs("</nav><section class=\"spo-steps\"><h2>From a feed to retained knowledge</h2>"
          "<p>Discovery → subscribed with a separate authority receipt → hub challenge → "
          "HMAC-authenticated change notice → SourceDirty queue → receipted ForeignTwin read. "
          "If an upstream host is silent, that is not evidence that no articles were published.</p>"
          "<p>Subscription and renewal are explicit operations in the native WebSub owner; "
          "this inspector has no write controls.</p>"
          "<a href=\"/sources\">Return to Source Library</a></section></main></body></html>",stdout);
    return 1;
}
#endif
