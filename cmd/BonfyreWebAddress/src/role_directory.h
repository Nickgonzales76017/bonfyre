#ifndef BONFYRE_ROLE_DIRECTORY_H
#define BONFYRE_ROLE_DIRECTORY_H
/* A human-operated role registry over native RoleProgram / RoleAssignment.
 * These are published postures, not new grants, jobs, employment, invitations
 * or bypasses around effect-specific admission. */
static void role_dir_terms(sqlite3 *db,const char *title,const char *raw,size_t limit){
    sqlite3_stmt *stmt=NULL;int count=0;
    fputs("<section class=\"role-terms\"><h3>",stdout);html(title);fputs("</h3>",stdout);
    if(!raw||!*raw||sqlite3_prepare_v2(db,
       "SELECT value FROM json_each(?1) WHERE type='text' LIMIT ?2",-1,&stmt,NULL)!=SQLITE_OK){
        sqlite3_finalize(stmt);fputs("<p>No published entries in this group.</p></section>",stdout);return;
    }
    sqlite3_bind_text(stmt,1,raw,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt,2,(int)limit);
    while(sqlite3_step(stmt)==SQLITE_ROW){
        const char *value=(const char*)sqlite3_column_text(stmt,0);
        if(!value||!*value)continue;
        if(!count++)fputs("<ul>",stdout);
        fputs("<li>",stdout);html(value);fputs("</li>",stdout);
    }
    sqlite3_finalize(stmt);
    if(count)fputs("</ul>",stdout);else fputs("<p>No published entries in this group.</p>",stdout);
    fputs("</section>",stdout);
}
static void role_dir_head(const char *title){
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
          "Cache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\n"
          "Referrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\n"
          "Connection: close\r\n\r\n<!doctype html><html lang=\"en\"><head>"
          "<meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<title>",stdout);html(title);fputs(" · Aurekai</title>"
          "<link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=2\">"
          "<style>.role-registry{margin:0 auto;max-width:1290px;padding:clamp(20px,4vw,48px)}"
          ".role-registry *{box-sizing:border-box}.role-registry h1{font:400 clamp(31px,5vw,56px)/1.08 Georgia,serif;margin:14px 0}"
          ".role-registry>p{max-width:78ch;font:14px/1.6 system-ui}.role-registry h2{font:400 clamp(23px,3vw,34px)/1.2 Georgia,serif;margin:24px 0 13px}"
          ".role-registry .role-cards{list-style:none;margin:12px 0;padding:0;display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,290px),1fr));gap:15px}"
          ".role-registry .role-card{padding:20px;border:1px solid var(--w-rule,#87978a);border-radius:17px;background:var(--w-panel,#182720);display:flex;flex-direction:column;gap:13px;min-width:0}"
          ".role-registry .role-card h3{font:400 23px/1.18 Georgia,serif;margin:0}.role-registry .role-card p{margin:4px 0;font:13px/1.5 system-ui;overflow-wrap:anywhere}"
          ".role-registry .role-search{display:flex;gap:12px;flex-wrap:wrap;align-items:end;margin:24px 0}.role-registry label{display:grid;gap:6px;font:600 12px/1.35 system-ui;flex:1;min-width:180px}"
          ".role-registry :is(input,button){min-height:44px;padding:9px 13px;font:14px/1.4 system-ui;border-radius:9px}"
          ".role-registry input{min-width:0;width:100%;border:1px solid var(--w-rule,#87978a);background:var(--w-ground,#0e1711);color:inherit}"
          ".role-registry button{border:1px solid #9cc491;background:#c7ec9a;color:#1b3928;cursor:pointer;font-weight:700}"
          ".role-registry .role-terms{border-top:1px solid var(--w-rule,#87978a);padding-top:12px}.role-registry .role-terms h3{font:600 12px/1.4 system-ui;margin:3px 0 9px}"
          ".role-registry .role-terms ul{margin:0;padding:0 0 0 18px}.role-registry .role-terms li{margin:5px 0;font:13px/1.4 system-ui;overflow-wrap:anywhere}"
          ".role-registry .role-terms p{font:13px/1.5 system-ui;color:var(--w-muted,#bdc9b9)}"
          ".role-registry .role-active{list-style:none;margin:0;padding:0;display:grid;gap:8px}.role-registry .role-active li{border:1px solid var(--w-rule,#87978a);border-radius:11px;padding:13px 16px;display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}"
          ".role-registry .role-active small{font:12px/1.4 system-ui}.role-registry .role-proof{border-left:3px solid #789b76;padding:10px 15px;margin:18px 0;background:var(--w-panel,#182720)}"
          ".role-registry a{color:inherit;text-underline-offset:3px}.role-registry :is(a,button,input):focus-visible{outline:3px solid #d5f6ab;outline-offset:3px}"
          ".role-registry .role-pages{display:flex;gap:24px;margin:24px 0}.role-registry .role-detail{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px}"
          "@media(max-width:390px){.role-registry{padding:17px}.role-registry .role-search{display:grid}.role-registry label{min-width:0}}"
          "</style></head><body><main class=\"role-registry\" data-role-directory><nav aria-label=\"Breadcrumb\">"
          "<a href=\"/\">Aurekai</a> / <a href=\"/subjects\">Subjects</a> / Operating roles</nav>"
          "<header><h1>",stdout);html(title);
    fputs("</h1><p>Explore published operating postures. Choosing a posture changes only your own scoped role assignment. "
          "Every provider, organizational, financial and publication action independently checks current authority.</p></header>",stdout);
}
static void role_dir_foot(void){
    fputs("<footer><p>No role assignment creates employment, certification, membership, grants, payment authority, or permission to act.</p>"
          "<a href=\"/roles\">Back to roles</a></footer></main></body></html>",stdout);
}
/* Ending a personal posture touches only the signed-in actor's exact
 * assignment. It does not revoke an institutional grant or finish work. */
static int serve_role_leave(const char *method,const char *target,const char *body,
                           const char *raw,const char *host,const char *bearer,const char *auth_db){
    if(!target||strcmp(target,"/roles/leave"))return 0;
    if(!host||(strcmp(host,"aurekai.life")&&strcmp(host,"aurekai.work"))){
        protocol_reply(404,"text/plain; charset=utf-8","Role posture not on this host.\n");return 1;
    }
    if(strcmp(method,"POST")){
        protocol_reply(405,"text/plain; charset=utf-8","Submit the reviewed role action with POST.\n");return 1;
    }
    char actor[64]="",origin[300]="",expected[300],other[300];
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){
        protocol_reply(401,"text/plain; charset=utf-8","Sign in before changing your operating posture.\n");return 1;
    }
    snprintf(expected,sizeof(expected),"https://%s",host);
    snprintf(other,sizeof(other),"http://%s",host);
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||
       (strcmp(origin,expected)&&strcmp(origin,other))){
        protocol_reply(403,"text/plain; charset=utf-8","The role change must originate on this site.\n");return 1;
    }
    char assignment[289]="";
    if(!body||strlen(body)>2048||!query_value(body,"assignment",assignment,sizeof(assignment))||
       !*assignment||
       strspn(assignment,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:." )!=strlen(assignment)){
        protocol_reply(400,"text/plain; charset=utf-8","Choose an exact existing role assignment.\n");return 1;
    }
    sqlite3 *db=cms_open_rw();
    if(!db||!cms_type_registered(db,"role_assignment")){
        if(db)sqlite3_close(db);
        protocol_reply(503,"text/plain; charset=utf-8","Role assignments are unavailable; no state changed.\n");return 1;
    }
    sqlite3_stmt *st=NULL;int changed=0,recognized=0;
    const char *update=
      "UPDATE role_assignment SET state='ended',updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') "
      "WHERE namespace='root' AND status='published' AND state='assumed' "
      "AND actor_subject=?1 AND assignment_id=?2";
    if(sqlite3_prepare_v2(db,update,-1,&st,NULL)!=SQLITE_OK){
        sqlite3_finalize(st);sqlite3_close(db);
        protocol_reply(503,"text/plain; charset=utf-8","The role owner cannot save this change.\n");return 1;
    }
    sqlite3_bind_text(st,1,actor,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,assignment,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_DONE)changed=sqlite3_changes(db)>0;
    else {sqlite3_finalize(st);sqlite3_close(db);
        protocol_reply(503,"text/plain; charset=utf-8","The role change did not complete; reopen it before retrying.\n");return 1;}
    sqlite3_finalize(st);st=NULL;
    if(!changed&&sqlite3_prepare_v2(db,
       "SELECT 1 FROM role_assignment WHERE namespace='root' AND status='published' "
       "AND actor_subject=?1 AND assignment_id=?2 AND state='ended' LIMIT 1",
       -1,&st,NULL)==SQLITE_OK){
       sqlite3_bind_text(st,1,actor,-1,SQLITE_TRANSIENT);
       sqlite3_bind_text(st,2,assignment,-1,SQLITE_TRANSIENT);
       recognized=sqlite3_step(st)==SQLITE_ROW;
    }
    sqlite3_finalize(st);sqlite3_close(db);
    if(!changed&&!recognized){
        protocol_reply(404,"text/plain; charset=utf-8","This operating posture is not yours or is unavailable.\n");return 1;
    }
    fputs("HTTP/1.1 303 See Other\r\nLocation: /roles\r\nCache-Control: private, no-store\r\n"
          "Content-Length: 0\r\nConnection: close\r\n\r\n",stdout);
    return 1;
}
static int role_dir_detail(sqlite3 *db,const char *actor,const char *assignment){
    sqlite3_stmt *s=NULL;
    const char *sql=
      "SELECT a.assignment_id,a.context_scope,p.label,p.role_id,p.work_queues,"
      "p.exact_operations,p.unresolved_operations,p.authority_requirements,"
      "p.handoff_contracts,p.lenses,p.cannot_infer "
      "FROM role_assignment a JOIN role_program p "
      "ON a.namespace=p.namespace AND a.role_program=p.role_id "
      "WHERE a.namespace='root' AND a.actor_subject=?1 AND a.assignment_id=?2 "
      "AND a.status='published' AND a.state='assumed' AND p.status='published' LIMIT 1";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){sqlite3_finalize(s);return -1;}
    sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,assignment,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);return 0;}
    const char *label=(const char*)sqlite3_column_text(s,2);
    role_dir_head(label&&*label?label:"My operating role");
    fputs("<p class=\"role-proof\">Current assignment for this signed-in actor · Scope: <strong>",stdout);
    html((const char*)sqlite3_column_text(s,1));
    fputs("</strong>. This is an operating posture, not a capability authorization.</p>"
          "<section class=\"role-detail\" data-role-posture>",stdout);
    static const char *names[]={"Work queues","Exact native operations","Operations not yet bound",
      "Authority still required","Handoff contracts","Available lenses"};
    for(int i=0;i<6;i++)role_dir_terms(db,names[i],(const char*)sqlite3_column_text(s,i+4),12);
    fputs("</section><p><a href=\"/roles/workspace?assignment=",stdout);
    url_query_value((const char*)sqlite3_column_text(s,0));
    fputs("\">Read the exact private owner projection</a></p>",stdout);
    fputs("<form class=\"role-end\" action=\"/roles/leave\" method=\"post\">",stdout);
    fputs("<input type=\"hidden\" name=\"assignment\" value=\"",stdout);
    html((const char*)sqlite3_column_text(s,0));
    fputs("\"><p>Ending this posture does not revoke a separate grant, remove work history or end any employment.</p>"
          "<button type=\"submit\">End this operating posture</button></form>",stdout);
    sqlite3_finalize(s);
    role_dir_foot();return 1;
}
static void role_dir_assignments(sqlite3 *db,const char *actor){
    sqlite3_stmt *s=NULL;
    fputs("<section data-role-assignments><h2>My current operating postures</h2>",stdout);
    const char *sql=
      "SELECT a.assignment_id,a.context_scope,p.label "
      "FROM role_assignment a JOIN role_program p "
      "ON a.namespace=p.namespace AND a.role_program=p.role_id "
      "WHERE a.namespace='root' AND a.actor_subject=?1 AND a.status='published' "
      "AND a.state='assumed' AND p.status='published' "
      "ORDER BY a.assumed_at DESC,a.id DESC LIMIT 31";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);fputs("<p>Assignments cannot be read currently.</p></section>",stdout);return;
    }
    sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
    int found=0,step=0;
    fputs("<ol class=\"role-active\">",stdout);
    while((step=sqlite3_step(s))==SQLITE_ROW&&found<30){
        const char *assignment=(const char*)sqlite3_column_text(s,0),
                   *scope=(const char*)sqlite3_column_text(s,1),
                   *label=(const char*)sqlite3_column_text(s,2);
        if(!assignment||!*assignment||!label||!*label)continue;
        found++;fputs("<li data-role-assignment><span><strong>",stdout);html(label);
        fputs("</strong> <small>",stdout);html(scope?scope:"");
        fputs("</small></span><a href=\"/roles?assignment=",stdout);url_query_value(assignment);
        fputs("\">Open work posture</a></li>",stdout);
    }
    sqlite3_finalize(s);fputs("</ol>",stdout);
    if(!found)fputs("<p>No operating postures are recorded for this account yet.</p>",stdout);
    if(found==30||step==SQLITE_ROW)fputs("<p>More current assignments exist beyond this bounded view.</p>",stdout);
    fputs("</section>",stdout);
}
static int serve_role_directory(const char *method,const char *target,const char *query,
                                const char *host,const char *bearer,const char *auth_db){
    if(!target||strcmp(target,"/roles"))return 0;
    if(!host||(strcmp(host,"aurekai.life")&&strcmp(host,"aurekai.work"))){
        protocol_reply(404,"text/plain; charset=utf-8","Operating roles are not published at this address.\n");return 1;
    }
    if(strcmp(method,"GET")){protocol_reply(405,"text/plain; charset=utf-8","Read role programmes with GET.\n");return 1;}
    char actor[64]="";
    if(!page_session_actor(bearer,auth_db,actor,sizeof(actor))){
        protocol_reply(401,"text/plain; charset=utf-8","Sign in to use personal operating roles.\n");return 1;
    }
    char q[97]="",page_text[6]="",assignment[289]="",scope[96]="";
    if(query&&strlen(query)>2048){protocol_reply(400,"text/plain; charset=utf-8","Bound the role search.\n");return 1;}
    if(query){
        query_value(query,"q",q,sizeof(q));
        query_value(query,"page",page_text,sizeof(page_text));
        query_value(query,"assignment",assignment,sizeof(assignment));
        query_value(query,"scope",scope,sizeof(scope));
    }
    if((page_text[0]&&(strlen(page_text)>4||strspn(page_text,"0123456789")!=strlen(page_text)))||
       (assignment[0]&&(q[0]||page_text[0]||scope[0]))||
       (assignment[0]&&strspn(assignment,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:.")!=strlen(assignment) )||
       (scope[0]&&strspn(scope,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:.")!=strlen(scope))){
        protocol_reply(400,"text/plain; charset=utf-8","Choose one role view and a bounded page.\n");return 1;
    }
    if(!scope[0])snprintf(scope,sizeof(scope),"global");
    unsigned page=page_text[0]?(unsigned)strtoul(page_text,NULL,10):0;
    if(page>400){protocol_reply(400,"text/plain; charset=utf-8","Role page exceeds the supported range.\n");return 1;}
    sqlite3 *db=cms_open_ro();
    if(!db||!cms_type_registered(db,"role_program")||!cms_type_registered(db,"role_assignment")){
        if(db)sqlite3_close(db);
        protocol_reply(503,"text/plain; charset=utf-8","The native role registry is unavailable.\n");return 1;
    }
    if(assignment[0]){
        int rc=role_dir_detail(db,actor,assignment);sqlite3_close(db);
        if(rc<=0)protocol_reply(rc<0?503:404,"text/plain; charset=utf-8",
            "This role posture is not available to the signed-in actor.\n");
        return 1;
    }
    sqlite3_stmt *s=NULL;
    const char *sql="SELECT role_id,label,work_queues,exact_operations,"
      "unresolved_operations,authority_requirements,cannot_infer "
      "FROM role_program WHERE namespace='root' AND status='published' "
      "AND (?1='' OR instr(lower(role_id||' '||label),lower(?1))>0) "
      "ORDER BY label ASC,role_id ASC LIMIT 25 OFFSET ?2";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);sqlite3_close(db);
        protocol_reply(503,"text/plain; charset=utf-8","Role registry cannot be read completely.\n");return 1;
    }
    sqlite3_bind_text(s,1,q,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(s,2,(int)page*24);
    role_dir_head("Operating roles");
    role_dir_assignments(db,actor);
    fputs("<section data-role-catalogue><h2>Published role registry</h2>"
          "<p>Each entry is a compiled set of interests, queues and possible operations. "
          "Only operations admitted by their actual owner can execute.</p>"
          "<form class=\"role-search\" action=\"/roles\" method=\"get\">"
          "<label>Find an operating role<input name=\"q\" type=\"search\" maxlength=\"96\" value=\"",stdout);
    html(q);fputs("\" placeholder=\"Role, craft or operating subject\"></label><input type=\"hidden\" name=\"scope\" value=\"",stdout);
    html(scope);fputs("\"><button type=\"submit\">Search roles</button></form>"
          "<ol class=\"role-cards\">",stdout);
    int found=0,step=0;
    while((step=sqlite3_step(s))==SQLITE_ROW && found<24){
        const char *id=(const char*)sqlite3_column_text(s,0),
                   *label=(const char*)sqlite3_column_text(s,1),
                   *queues=(const char*)sqlite3_column_text(s,2),
                   *exact=(const char*)sqlite3_column_text(s,3),
                   *unresolved=(const char*)sqlite3_column_text(s,4),
                   *authority=(const char*)sqlite3_column_text(s,5);
        if(!id||!*id||!label||!*label||
           strspn(id,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(id))continue;
        found++;
        fputs("<li class=\"role-card\" data-role-program><h3>",stdout);html(label);
        fputs("</h3><p><small>Operating role · ",stdout);html(id);
        fputs("</small></p>",stdout);
        role_dir_terms(db,"Work queues",queues,5);
        role_dir_terms(db,"Exact operations",exact,5);
        role_dir_terms(db,"Operations not yet connected",unresolved,5);
        role_dir_terms(db,"Authority required, not granted",authority,5);
        fputs("<form method=\"post\" action=\"/roles/assume\"><input type=\"hidden\" name=\"role\" value=\"",stdout);
        html(id);fputs("\"><input type=\"hidden\" name=\"return\" value=\"/roles\">"
          "<label>Apply in scope<input name=\"scope\" maxlength=\"95\" required pattern=\"[A-Za-z0-9._:-]+\" "
          "value=\"",stdout);
        html(scope);fputs("\" aria-label=\"Role context scope\"></label>"
          "<button type=\"submit\">Use this operating posture</button></form></li>",stdout);
    }
    sqlite3_finalize(s);
    fputs("</ol>",stdout);
    if(!found)fputs("<p>No published roles match this search. No role is inferred.</p>",stdout);
    fputs("<nav class=\"role-pages\" aria-label=\"Role registry pages\">",stdout);
    if(page){fputs("<a rel=\"prev\" href=\"/roles?page=",stdout);printf("%u",page-1);
        fputs("&amp;q=",stdout);url_query_value(q);fputs("&amp;scope=",stdout);url_query_value(scope);fputs("\">Previous roles</a>",stdout);}
    if((found==24||step==SQLITE_ROW)&&page<400){
        fputs("<a rel=\"next\" href=\"/roles?page=",stdout);printf("%u",page+1);
        fputs("&amp;q=",stdout);url_query_value(q);fputs("&amp;scope=",stdout);url_query_value(scope);fputs("\">More roles</a>",stdout);
    }
    fputs("</nav></section>",stdout);role_dir_foot();sqlite3_close(db);return 1;
}
#endif
