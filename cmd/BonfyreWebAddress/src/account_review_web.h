#ifndef BONFYRE_ACCOUNT_REVIEW_WEB_H
#define BONFYRE_ACCOUNT_REVIEW_WEB_H
/* One read-only, session-scoped review surface. Native owners retain every
 * publication, approval, moderation, payment and delivery decision. */
static int cms_namespace_for(const char *,const char *,char *,size_t);
static int cms_type_visible_in_closure(const char *,const char *,const char *);
typedef struct {char type[96],title[224],status[40],route[128];} BfReviewRow;
static int bf_review_column(sqlite3 *db,const char *table,const char *field){
    char *sql=sqlite3_mprintf("PRAGMA table_info(\"%w\")",table);
    sqlite3_stmt *q=NULL;int exists=0;
    if(sql&&sqlite3_prepare_v2(db,sql,-1,&q,NULL)==SQLITE_OK)
        while(sqlite3_step(q)==SQLITE_ROW){
            const char *name=(const char*)sqlite3_column_text(q,1);
            if(name&&!strcmp(name,field)){exists=1;break;}}
    sqlite3_finalize(q);sqlite3_free(sql);return exists;
}
static const char *bf_review_title(sqlite3 *db,const char *table){
    static const char *const columns[]={"title","heading","business_name","name","subject_line","label","route","site_id","message_id","coordinate"};
    for(size_t i=0;i<sizeof(columns)/sizeof(columns[0]);i++)
        if(bf_review_column(db,table,columns[i]))return columns[i];
    return "id";
}
static int bf_review_error(int status,const char *body){
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: private, no-store\r\nConnection: close\r\n\r\n%s\n",
        status,status==403?"Forbidden":status==400?"Bad Request":"Service Unavailable",body);
    return 1;
}
static int serve_account_review(const char *method,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(method,"GET")||(query&&*query))
        return bf_review_error(400,"Open the current review desk without parameters.");
    StudioViewer who={0};char namespace[192]={0};
    if(!session_identity(bearer,auth_db,&who)||who.id<1||
       !cms_namespace_for(bearer,auth_db,namespace,sizeof(namespace)))
        return bf_review_error(403,"Sign in to review your own account.");
    sqlite3 *db=cms_open_ro();BfReviewRow *rows=calloc(120,sizeof(*rows));
    if(!db||!rows){if(db)sqlite3_close(db);free(rows);
        return bf_review_error(503,"The native review owner is unavailable.");}
    sqlite3_busy_timeout(db,1200);
    sqlite3_stmt *registry=NULL;size_t shown=0;int total=0,draft_total=0,type_total=0;
    if(sqlite3_prepare_v2(db,"SELECT name FROM _content_types ORDER BY name",-1,&registry,NULL)!=SQLITE_OK){
        sqlite3_close(db);free(rows);return bf_review_error(503,"The native CMS type registry is unavailable.");}
    while(sqlite3_step(registry)==SQLITE_ROW){
        const char *type=(const char*)sqlite3_column_text(registry,0);
        if(!type||strlen(type)>80||!studio_safe_ident(type)||
           !cms_type_visible_in_closure(bearer,auth_db,type)||
           !bf_review_column(db,type,"namespace")||!bf_review_column(db,type,"id"))continue;
        const char *status=bf_review_column(db,type,"status")?"status":
            bf_review_column(db,type,"review_state")?"review_state":
            bf_review_column(db,type,"state")?"state":NULL;
        int named_draft=strlen(type)>6&&!strcmp(type+strlen(type)-6,"_draft");
        if(!status&&!named_draft)continue;
        char *filter=status?sqlite3_mprintf(" AND \"%w\" IN ('draft','held','withheld','review','pending','pending_review','needs_review','review_required','awaiting_review')",status):
            sqlite3_mprintf("");
        char *count=status?sqlite3_mprintf("SELECT count(*),sum(CASE WHEN \"%w\"='draft' THEN 1 ELSE 0 END) FROM \"%w\" WHERE namespace=?1%s",status,type,filter):
            sqlite3_mprintf("SELECT count(*),count(*) FROM \"%w\" WHERE namespace=?1",type);
        sqlite3_stmt *c=NULL;int n=0,d=0;
        if(count&&sqlite3_prepare_v2(db,count,-1,&c,NULL)==SQLITE_OK){
            sqlite3_bind_text(c,1,namespace,-1,SQLITE_STATIC);
            if(sqlite3_step(c)==SQLITE_ROW){n=sqlite3_column_int(c,0);d=sqlite3_column_int(c,1);}}
        sqlite3_finalize(c);sqlite3_free(count);
        if(n>0){total+=n;draft_total+=d;type_total++;}
        if(n>0&&shown<120){
            const char *label=bf_review_title(db,type);
            char *name=!strcmp(label,"id")?sqlite3_mprintf("CAST(id AS TEXT)"):
                sqlite3_mprintf("COALESCE(NULLIF(CAST(\"%w\" AS TEXT),''),CAST(id AS TEXT))",label);
            char *state=status?sqlite3_mprintf("\"%w\"",status):sqlite3_mprintf("'draft'");
            char *sql=sqlite3_mprintf("SELECT %s,%s,%s FROM \"%w\" WHERE namespace=?1%s ORDER BY id DESC LIMIT %d",
                state,name,bf_review_column(db,type,"route")?"COALESCE(route,'')":"''",type,filter,(int)(120-shown));
            sqlite3_stmt *q=NULL;
            if(sql&&sqlite3_prepare_v2(db,sql,-1,&q,NULL)==SQLITE_OK){
                sqlite3_bind_text(q,1,namespace,-1,SQLITE_STATIC);
                while(shown<120&&sqlite3_step(q)==SQLITE_ROW){
                    BfReviewRow *item=&rows[shown++];
                    const char *s=(const char*)sqlite3_column_text(q,0),*t=(const char*)sqlite3_column_text(q,1);
                    const char *route=(const char*)sqlite3_column_text(q,2);
                    snprintf(item->type,sizeof(item->type),"%s",type);
                    snprintf(item->status,sizeof(item->status),"%s",s?s:"draft");
                    snprintf(item->title,sizeof(item->title),"%s",t?t:"Untitled");
                    snprintf(item->route,sizeof(item->route),"%s",route?route:"");
                }}
            sqlite3_finalize(q);sqlite3_free(sql);sqlite3_free(name);sqlite3_free(state);
        }
        sqlite3_free(filter);
    }
    sqlite3_finalize(registry);sqlite3_close(db);
    ak_app_http(200,1);ak_app_open("Review work","workbench","route-account-review");
    fputs("<div data-account-review><section class=\"br-hero\"><div><p class=\"br-eyebrow\">Your account / Review and drafts</p><h1>Work ready for<br>another look.</h1>"
        "<p>See the drafts and review holds available in this CMS destination. Resume in the existing editor; nothing here approves a release, publishes an article, sends a message or spends money.</p></div><div class=\"br-total\"><strong>",stdout);
    printf("%d",total);
    fputs("</strong><span>accessible CMS draft and review records</span></div></section><nav class=\"br-links\" aria-label=\"Continue reviewing\">"
        "<a href=\"/inbox?context=self\">Decisions waiting on you</a><a href=\"/cms/compose/advanced\">Review authored pages</a><a href=\"/cms/\">All content drafts</a><a href=\"/apps?context=self\">Saved work in installed editors</a></nav>"
        "<section><div class=\"br-heading\"><div><p class=\"br-eyebrow br-eyebrow-quiet\">Native CMS</p><h2>Drafts and held work</h2><p>",stdout);
    printf("%d drafts · %d other review states · %d registered types",draft_total,total-draft_total,type_total);
    fputs("</p></div><label>Find work <input id=\"br-find\" type=\"search\" aria-label=\"Search reviews\" placeholder=\"Title, state or type\"></label></div><ol class=\"br-list\">",stdout);
    for(size_t i=0;i<shown;i++){
        BfReviewRow *item=&rows[i];
        fputs("<li class=\"br-row\" data-review-row><div><small>",stdout);html(item->type);
        fputs(" · ",stdout);html(item->status);fputs("</small><h3>",stdout);
        html(item->title);fputs("</h3></div><a href=\"",stdout);
        if(!strcmp(item->type,"site_page")&&item->route[0]=='/'){
            fputs("/cms/compose/advanced?route=",stdout);url_query_value(item->route);
        }else if(!strcmp(item->type,"site_fragment"))fputs("/cms/compose/advanced",stdout);
        else fputs("/cms/",stdout);
        fputs("\">Open in editor</a></li>",stdout);
    }
    fputs("</ol>",stdout);
    if(!shown)fputs("<p class=\"br-note\">No draft records were found in this account's accessible CMS namespace. Another publisher or installed editor may still have work waiting.</p>",stdout);
    if((int)shown<total){fputs("<p class=\"br-note\">Showing ",stdout);printf("%zu of %d",shown,total);fputs(" records; open Content Studio for the rest.</p>",stdout);}
    fputs("<p id=\"br-empty\" class=\"br-note\" hidden>No matching records in this view.</p></section><section class=\"br-footer br-note\"><h2>One place to continue, not a new approval system</h2>"
        "<p>Inbox owns human approval requests; Content Studio owns CMS drafts; installed editors own their saved work. Material held by another publication remains with that publisher until its release owner admits it.</p></section></div>"
        "<script>(()=>{const q=document.getElementById('br-find'),rows=[...document.querySelectorAll('[data-review-row]')],empty=document.getElementById('br-empty');q.addEventListener('input',()=>{const s=q.value.trim().toLowerCase();let count=0;for(const row of rows){row.hidden=!row.textContent.toLowerCase().includes(s);if(!row.hidden)count++;}empty.hidden=count!==0||!s;});})();</script>",stdout);
    ak_app_close();
    free(rows);return 1;
}
#endif
