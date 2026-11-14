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
/* The material-demand ledger and the downstream FactDelta obligation are
 * separate native WebSub stores. An unbound demand must not disappear merely
 * because no subscription was ever established. All three views are read-only.
 * The UI reveals source host plus stable hash only, never URLs or identity. */
static void spo_source_label(const char *topic) {
    char domain[192],digest[65];
    spo_domain(topic,domain);
    bf_sha256_hex((const uint8_t*)(topic?topic:""),topic?strlen(topic):0,digest);
    digest[12]=0;
    fputs("<strong>",stdout);
    html(*domain?domain:"Source address withheld");
    fputs("</strong> <small>source ",stdout);
    html(digest);
    fputs("</small>",stdout);
}
/* Three independent bounded pagination coordinates, one per native ledger. */
static int spo_parse_page(const char *raw,unsigned *out){
    if(!raw||!*raw){*out=0;return 1;}
    if(strlen(raw)>4||strspn(raw,"0123456789")!=strlen(raw))return 0;
    unsigned n=(unsigned)strtoul(raw,NULL,10);
    if(n>500)return 0;
    *out=n;return 1;
}
static void spo_page_links(const char *q,const char *state,unsigned page,
                           unsigned dpage,unsigned fpage,int view,int more){
    unsigned at=view==0?page:view==1?dpage:fpage;
    if(at>=500)more=0; /* Each ledger is independently bounded. */
    if(!at&&!more)return;
    const char *anchor=view==0?"subscriptions":view==1?"material-demands":"fact-handoffs";
    const char *labels[]={"subscription","material demand","FactDelta handoff"};
    fputs("<nav class=\"spo-paging\" aria-label=\"",stdout);html(labels[view]);
    fputs(" pages\">",stdout);
    for(int dir=-1;dir<=1;dir+=2){
        if((dir<0&&!at)||(dir>0&&!more))continue;
        unsigned step=dir<0?at-1:at+1;
        unsigned sp=view==0?step:page,dp=view==1?step:dpage,fp=view==2?step:fpage;
        fputs("<a href=\"/sources/push?state=",stdout);url_query_value(state);
        fputs("&amp;q=",stdout);url_query_value(q);
        fputs("&amp;page=",stdout);printf("%u",sp);
        fputs("&amp;dpage=",stdout);printf("%u",dp);
        fputs("&amp;fpage=",stdout);printf("%u",fp);
        fputs("#",stdout);html(anchor);fputs("\">",stdout);
        fputs(dir<0?"Previous ":"More ",stdout);html(labels[view]);
        fputs("s</a>",stdout);
    }
    fputs("</nav>",stdout);
}
static void spo_material_demands(sqlite3 *db,const char *q,const char *state,unsigned page,unsigned dpage,unsigned fpage) {
    sqlite3_stmt *st=NULL;int n=0,step=0;
    fputs("<section id=\"material-demands\" data-source-material-demands>"
          "<h2>MaterialCompiler observation demands</h2>"
          "<p>These are retained requests for new source observations. They do not "
          "require an active hub subscription. A request does not prove that any "
          "foreign content has been read or admitted.</p>",stdout);
    const char *sql=
        "SELECT d.topic,COUNT(*),MAX(d.last_requested_at),"
        "COALESCE((SELECT s.state FROM source_dirty s WHERE s.topic=d.topic "
        " ORDER BY s.dirty_at DESC LIMIT 1),'not queued'),"
        "COALESCE((SELECT s.attempts FROM source_dirty s WHERE s.topic=d.topic "
        " ORDER BY s.dirty_at DESC LIMIT 1),0) "
        "FROM source_dirty_demand d "
        "WHERE (?1='' OR instr(lower(d.topic),lower(?1))>0) "
        "GROUP BY d.topic ORDER BY MAX(d.last_requested_at) DESC,d.topic "
        "LIMIT 26 OFFSET ?2";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK){
        sqlite3_finalize(st);
        fputs("<p role=\"status\">The material-demand ledger is not yet available in this carrier.</p></section>",stdout);
        return;
    }
    sqlite3_bind_text(st,1,q,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,2,(int)dpage*25);
    fputs("<ol class=\"spo-ledger\">",stdout);
    while((step=sqlite3_step(st))==SQLITE_ROW && n<25){
        const char *topic=(const char*)sqlite3_column_text(st,0);
        const char *status=(const char*)sqlite3_column_text(st,3);
        if(!topic||!*topic)continue;
        n++;
        fputs("<li data-material-demand>",stdout);
        spo_source_label(topic);
        fputs("<dl><div><dt>Distinct requests</dt><dd>",stdout);
        printf("%lld",(long long)sqlite3_column_int64(st,1));
        fputs("</dd></div><div><dt>Latest request</dt><dd>",stdout);
        printf("%lld",(long long)sqlite3_column_int64(st,2));
        fputs("</dd></div><div><dt>Observation queue</dt><dd>",stdout);
        html(status?status:"not queued");
        fputs("</dd></div><div><dt>Observation attempts</dt><dd>",stdout);
        printf("%d",sqlite3_column_int(st,4));
        fputs("</dd></div></dl></li>",stdout);
    }
    if(!n)fputs("<li>No material observation demands match this view.</li>",stdout);
    fputs("</ol>",stdout);
    spo_page_links(q,state,page,dpage,fpage,1,step==SQLITE_ROW);
    fputs("</section>",stdout);
    sqlite3_finalize(st);
}
static void spo_fact_handoffs(sqlite3 *db,const char *q,const char *state,unsigned page,unsigned dpage,unsigned fpage) {
    sqlite3_stmt *st=NULL;int n=0,step=0;
    fputs("<section id=\"fact-handoffs\" data-source-fact-handoffs>"
          "<h2>ForeignTwin to FactDelta handoffs</h2>"
          "<p>Each retained row is a downstream notification obligation after a "
          "ForeignTwin observation. It is not proof of publication, delivery, "
          "fact verification, or completion by the receiving owner.</p>",stdout);
    const char *sql=
        "SELECT topic,observed_at,attempts FROM foreign_delta "
        "WHERE (?1='' OR instr(lower(topic),lower(?1))>0) "
        "ORDER BY observed_at DESC,topic LIMIT 26 OFFSET ?2";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK){
        sqlite3_finalize(st);
        fputs("<p role=\"status\">The FactDelta handoff ledger is not yet available in this carrier.</p></section>",stdout);
        return;
    }
    sqlite3_bind_text(st,1,q,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,2,(int)fpage*25);
    fputs("<ol class=\"spo-ledger\">",stdout);
    while((step=sqlite3_step(st))==SQLITE_ROW && n<25){
        const char *topic=(const char*)sqlite3_column_text(st,0);
        if(!topic||!*topic)continue;
        n++;
        fputs("<li data-fact-handoff>",stdout);spo_source_label(topic);
        fputs("<dl><div><dt>Recorded observation time</dt><dd>",stdout);
        printf("%lld",(long long)sqlite3_column_int64(st,1));
        fputs("</dd></div><div><dt>Handoff attempts</dt><dd>",stdout);
        printf("%d",sqlite3_column_int(st,2));
        fputs("</dd></div><div><dt>Standing</dt><dd>Pending downstream notification</dd></div></dl></li>",stdout);
    }
    if(!n)fputs("<li>No pending native FactDelta handoffs match this view.</li>",stdout);
    fputs("</ol>",stdout);
    spo_page_links(q,state,page,dpage,fpage,2,step==SQLITE_ROW);
    fputs("</section>",stdout);
    sqlite3_finalize(st);
}
/* A refused or unavailable console still opens on the one shell, in the
 * console's own layout, saying which owner decided. */
static void spo_refusal(int code,const char *title,const char *why){
    ak_app_http(code,1);ak_app_open("WebSub delivery","connections","route-push");
    fputs("<div class=\"ak-wb-strip\"><span>Sources</span><span>Push delivery</span><span class=\"ak-live is-off\">",stdout);html(title);
    fputs("</span></div><section class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\"><a href=\"/sources\">Sources</a> &middot; WebSub</p>"
          "<h1>Push delivery, leases and signatures</h1><p class=\"ak-lede\">",stdout);html(why);
    fputs("</p><div class=\"ak-actions\"><a class=\"ak-btn\" href=\"/sources\">Back to sources</a></div></div></section>",stdout);
    ak_app_close();
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
        spo_refusal(403,"Administrators only","This console is for an active Bonfyre institution administrator. Sign in with that account to inspect leases, challenges and signatures.");return 1;
    }
    char q[81]="",state[24]="all",pageno[12]="",dno[12]="",fno[12]="";
    if(query){
        if(strlen(query)>2048){aw_error(400,"Bound the source filter.\n");return 1;}
        query_value(query,"q",q,sizeof(q));query_value(query,"state",state,sizeof(state));
        query_value(query,"page",pageno,sizeof(pageno));
        query_value(query,"dpage",dno,sizeof(dno));query_value(query,"fpage",fno,sizeof(fno));
    }
    if(!*state)snprintf(state,sizeof(state),"all");
    if(!spo_state_ok(state)){
        aw_error(400,"Choose a supported transport state.\n");return 1;
    }
    unsigned page=0,dpage=0,fpage=0;
    if(!spo_parse_page(pageno,&page)||!spo_parse_page(dno,&dpage)||!spo_parse_page(fno,&fpage)){
        aw_error(400,"Choose supported bounded page coordinates.\n");return 1;
    }
    sqlite3 *db=websub_open(0);
    if(!db){spo_refusal(503,"Store not connected","The WebSub carrier store is not connected on this server, so there are no leases or deliveries to show.");return 1;}
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
        sqlite3_close(db);spo_refusal(503,"Store not ready","The WebSub carrier store has no subscription table yet.");return 1;
    }
    sqlite3_bind_text(st,1,q,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,state,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,3,(int)page*25);
    long long n_total=0,n_active=0,n_expired=0,n_rejected=0,n_dirty=0,n_deliv=0;
    {   sqlite3_stmt *c=NULL;
        if(sqlite3_prepare_v2(db,"SELECT count(*),sum(s.state='active' AND s.lease_expires>strftime('%s','now')),"
            "sum(s.state='active' AND COALESCE(s.lease_expires,0)<=strftime('%s','now')),sum(COALESCE(s.rejected,0)>0),"
            "sum(COALESCE(d.state,'') IN('pending','running')),sum(COALESCE(s.deliveries,0)) "
            "FROM subscriptions s LEFT JOIN source_dirty d ON d.token=s.token",-1,&c,NULL)==SQLITE_OK&&sqlite3_step(c)==SQLITE_ROW){
            n_total=sqlite3_column_int64(c,0);n_active=sqlite3_column_int64(c,1);n_expired=sqlite3_column_int64(c,2);
            n_rejected=sqlite3_column_int64(c,3);n_dirty=sqlite3_column_int64(c,4);n_deliv=sqlite3_column_int64(c,5);}
        sqlite3_finalize(c);
    }
    ak_app_http(200,1);ak_app_open("WebSub delivery","connections","route-push");
    printf("<div class=\"ak-wb-strip\"><span>WebSub</span><span>HMAC-SHA256 callbacks</span><span>Subscriptions <b>%lld</b></span>"
           "<span class=\"ak-live%s\">%s</span></div>",n_total,n_expired?" is-off":"",n_expired?"Leases past expiry":"Leases current");
    fputs("<section class=\"ak-wb-hero is-solo\" data-source-push-operations><div><p class=\"ak-kicker\"><a href=\"/sources\">Sources</a> &middot; Push delivery</p>"
          "<h1>Lease renewal and signature console</h1>"
          "<p class=\"ak-lede\">Every feed that pushes to us: its hub, how long its lease has left, whether the hub answered the challenge, "
          "and how many signed callbacks were accepted or refused. A challenge proves the hub knows the topic; a valid signature admits a callback. "
          "Neither publishes anything.</p></div></section>",stdout);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Active leases</dt><dd class=\"is-on\">%lld<small style=\"display:inline\"> / %lld</small></dd><small>%lld past expiry</small></div>"
           "<div><dt>Accepted deliveries</dt><dd>%lld</dd><small>signed callbacks admitted</small></div>"
           "<div><dt>Refused callbacks</dt><dd>%lld</dd><small>subscriptions with a signature mismatch</small></div>"
           "<div><dt>Sources changed</dt><dd>%lld</dd><small>waiting for a separate read</small></div></dl>",
           n_active,n_total,n_expired,n_deliv,n_rejected,n_dirty);
    fputs("<div class=\"spo-layout\"><div class=\"spo-main\">",stdout);
    fputs("<nav class=\"spo-rail\" aria-label=\"Native source pipeline\"><a href=\"#subscriptions\">Subscriptions</a><a href=\"#material-demands\">Material observation demands</a><a href=\"#fact-handoffs\">FactDelta handoffs</a></nav>",stdout);
    spo_filters(q,state);
    fputs("<section id=\"subscriptions\"><h2>Hub subscriptions and delivery</h2>",stdout);
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
    sqlite3_finalize(st);
    if(!n)fputs("<li>No matching subscriptions in the retained native carrier.</li>",stdout);
    fputs("</ol>",stdout);
    spo_page_links(q,state,page,dpage,fpage,0,more);
    fputs("</section>",stdout);
    spo_material_demands(db,q,state,page,dpage,fpage);
    spo_fact_handoffs(db,q,state,page,dpage,fpage);
    sqlite3_close(db);
    fputs("</div><aside class=\"spo-side\"><section class=\"ak-panel\" id=\"setup\"><header><h2>Add a push feed</h2><span class=\"ak-tag is-on\">WebSub 0.4</span></header>"
          "<p>Paste a feed address. It is reviewed first: the feed's own <code>rel=\"hub\"</code> link names its hub, and robots.txt is respected. "
          "Subscribing, renewing and unsubscribing are explicit operations of the WebSub owner; this page never sends them.</p>",stdout);
    {   BfAuthoringTree *tree=calloc(1,sizeof(*tree));
        int ok=tree&&ft_action(tree,"push-topic",NULL,0,"Review this feed","get","/studio/",NULL,0)&&
               ft_field(tree,"push-topic-url","push-topic",0,"Topic address","url","foreign_url","","https://example.org/feed.xml",0,NULL,"1200",1,"url");
        if(tree){if(ok)ft_render(tree,stdout);else{bf_authoring_tree_clear(tree);free(tree);}}
    }
    fputs("</section><section class=\"ak-console\" aria-label=\"Handshake path\"><b>handshake path</b><br>"
          "&gt; discover the hub from the feed<br>&gt; subscribe with a separate authority receipt<br>"
          "&gt; hub GET hub.mode=subscribe &amp; hub.challenge<br>&gt; echo the challenge, 200 OK<br>"
          "&gt; POST callback with X-Hub-Signature sha256=&hellip;<br>&gt; constant-time HMAC compare<br>"
          "&gt; source marked changed, read separately<br><br>A silent hub is not evidence that nothing was published.</section></aside></div>"
          "<section class=\"ak-band\"><div><p class=\"ak-kicker\">Where it goes next</p><h2>Changed sources are read, receipted, then known.</h2>"
          "<p>A delivery only marks a source as changed. The read that follows is its own receipted observation.</p></div>"
          "<div class=\"ak-actions\"><a class=\"ak-btn\" href=\"/sources\">Source library</a></div></section>",stdout);
    ak_app_close();
    return 1;
}
#endif
