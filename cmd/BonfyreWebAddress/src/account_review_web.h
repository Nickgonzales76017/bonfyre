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
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nContent-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'\r\nConnection: close\r\n\r\n",stdout);
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Review work · Aurekai</title><style>"
        "*{box-sizing:border-box}body{margin:0;background:#f7f7f0;color:#193729;font:15px/1.55 system-ui,sans-serif}a{color:inherit}a:focus-visible,input:focus-visible{outline:3px solid #b18c39;outline-offset:3px}"
        ".br-nav{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:18px;background:white;border-bottom:1px solid #c6d3c3;padding:18px clamp(16px,4vw,60px)}.br-nav>a{font:600 23px Georgia,serif;text-decoration:none}"
        ".br-nav nav{display:flex;gap:8px;flex-wrap:wrap}.br-nav nav a{text-decoration:none;padding:8px 13px;border-radius:40px}.br-nav nav [aria-current]{background:#e5eddd;font-weight:700}"
        "main{max-width:1480px;margin:auto;padding:28px clamp(16px,4vw,60px) 90px}.br-hero{display:grid;grid-template-columns:minmax(0,1fr) 225px;gap:25px;padding:clamp(30px,5vw,70px);background:#183e30;color:#f1f7e9;border-radius:24px}"
        ".br-hero h1{font:400 clamp(44px,6vw,84px)/1 Georgia,serif;letter-spacing:-.05em;margin:16px 0 24px}.br-hero p{max-width:65ch}.br-eyebrow{font:600 11px monospace;letter-spacing:.13em;text-transform:uppercase;color:#b9d8a6}"
        ".br-total{border-left:1px solid #6c927b;padding-left:26px;align-self:end}.br-total strong{display:block;font:400 85px/1 Georgia,serif}.br-total span{color:#d5e7ca}"
        ".br-links{display:flex;gap:10px;flex-wrap:wrap;margin:23px 0 40px}.br-links a{padding:12px 18px;border:1px solid #b1c8b1;background:#fff;border-radius:50px;text-decoration:none;font-weight:650}"
        ".br-links a:hover,.br-row>a:hover{background:#e7f0df}.br-heading{display:flex;align-items:end;justify-content:space-between;flex-wrap:wrap;gap:18px;border-bottom:2px solid #244d35;padding-bottom:18px}"
        ".br-heading h2{font:400 clamp(29px,4vw,47px)/1.1 Georgia,serif;margin:7px 0}.br-heading input{min-width:min(310px,70vw);padding:13px 15px;border:1px solid #9aaf9c;border-radius:9px;font:inherit}"
        ".br-list{margin:0;padding:0;list-style:none}.br-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:20px;align-items:center;border-bottom:1px solid #c7d7c6;padding:23px 0}"
        ".br-row small{font:600 11px monospace;letter-spacing:.05em;text-transform:uppercase;color:#637d67}.br-row h3{font:400 clamp(21px,2.3vw,31px)/1.16 Georgia,serif;margin:7px 0;overflow-wrap:anywhere}"
        ".br-row>a{padding:12px 18px;border:1px solid #9eb59e;border-radius:50px;text-decoration:none;font-weight:650}.br-note{max-width:75ch;color:#55705c;padding:23px 0}.br-footer{border-top:1px solid #c7d7c6;margin-top:40px}"
        "@media(max-width:760px){.br-hero{grid-template-columns:1fr}.br-total{border:0;border-top:1px solid #6c927b;padding:15px 0 0}.br-row{grid-template-columns:1fr}.br-row>a{justify-self:start}}"
        "</style></head><body><header class=\"br-nav\"><a href=\"/\">Aurekai</a><nav aria-label=\"Account\"><a href=\"/inbox?context=self\">Inbox</a><a href=\"/account/review\" aria-current=\"page\">Review</a><a href=\"/cms/\">Content Studio</a><a href=\"/apps?context=self\">Your work</a><a href=\"/profile\">Profile</a></nav></header>"
        "<main data-account-review><section class=\"br-hero\"><div><p class=\"br-eyebrow\">Your account / Review and drafts</p><h1>Work ready for<br>another look.</h1>"
        "<p>See the drafts and review holds available in this CMS destination. Resume in the existing editor; nothing here approves a release, publishes an article, sends a message or spends money.</p></div><div class=\"br-total\"><strong>",stdout);
    printf("%d",total);
    fputs("</strong><span>accessible CMS draft and review records</span></div></section><nav class=\"br-links\" aria-label=\"Continue reviewing\">"
        "<a href=\"/inbox?context=self\">Decisions waiting on you</a><a href=\"/cms/compose/advanced\">Review authored pages</a><a href=\"/cms/\">All content drafts</a><a href=\"/apps?context=self\">Saved work in installed editors</a></nav>"
        "<section><div class=\"br-heading\"><div><p class=\"br-eyebrow\" style=\"color:#607965\">Native CMS</p><h2>Drafts and held work</h2><p>",stdout);
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
    fputs("<p id=\"br-empty\" class=\"br-note\" hidden>No matching records in this view.</p></section><footer class=\"br-footer br-note\"><h2>One place to continue, not a new approval system</h2>"
        "<p>Inbox owns human approval requests; Content Studio owns CMS drafts; installed editors own their saved work. Material held by another publication remains with that publisher until its release owner admits it.</p></footer></main>"
        "<script>(()=>{const q=document.getElementById('br-find'),rows=[...document.querySelectorAll('[data-review-row]')],empty=document.getElementById('br-empty');q.addEventListener('input',()=>{const s=q.value.trim().toLowerCase();let count=0;for(const row of rows){row.hidden=!row.textContent.toLowerCase().includes(s);if(!row.hidden)count++;}empty.hidden=count!==0||!s;});})();</script></body></html>",stdout);
    free(rows);return 1;
}
#endif
