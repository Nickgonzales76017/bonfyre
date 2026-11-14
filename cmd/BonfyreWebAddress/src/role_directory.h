#ifndef BONFYRE_ROLE_DIRECTORY_H
#define BONFYRE_ROLE_DIRECTORY_H
/* A human-operated role registry over native RoleProgram / RoleAssignment.
 * These are published postures, not new grants, jobs, employment, invitations
 * or bypasses around effect-specific admission. */
static void role_dir_terms(sqlite3 *db,const char *title,const char *raw,size_t limit){
    sqlite3_stmt *stmt=NULL;int count=0;
    fputs("<section class=\"ak-panel role-terms\"><h3>",stdout);html(title);fputs("</h3>",stdout);
    if(!raw||!*raw||sqlite3_prepare_v2(db,
       "SELECT value FROM json_each(?1) WHERE type='text' LIMIT ?2",-1,&stmt,NULL)!=SQLITE_OK){
        sqlite3_finalize(stmt);fputs("<p class=\"ak-empty\">None published.</p></section>",stdout);return;
    }
    sqlite3_bind_text(stmt,1,raw,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt,2,(int)limit);
    while(sqlite3_step(stmt)==SQLITE_ROW){
        const char *value=(const char*)sqlite3_column_text(stmt,0);
        if(!value||!*value)continue;
        if(!count++)fputs("<ul class=\"ak-rows\">",stdout);
        fputs("<li>",stdout);html(value);fputs("</li>",stdout);
    }
    sqlite3_finalize(stmt);
    if(count)fputs("</ul>",stdout);else fputs("<p class=\"ak-empty\">None published.</p>",stdout);
    fputs("</section>",stdout);
}
static void role_dir_inline(sqlite3 *db,const char *raw,int limit){
    sqlite3_stmt *st=NULL;int n=0;
    if(!raw||!*raw||sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1) WHERE type='text' LIMIT ?2",-1,&st,NULL)!=SQLITE_OK){sqlite3_finalize(st);return;}
    sqlite3_bind_text(st,1,raw,-1,SQLITE_TRANSIENT);sqlite3_bind_int(st,2,limit);
    while(sqlite3_step(st)==SQLITE_ROW){const char *v=(const char*)sqlite3_column_text(st,0);if(!v||!*v)continue;fputs(n++?", ":"<br><small>",stdout);html(v);}
    if(n)fputs("</small>",stdout);sqlite3_finalize(st);
}
/* Stitch: the operator roles directory/registry, on the one shell. */
static void role_dir_head(const char *title){
    psetup_open(200,title,"ak-wb role-registry");
    fputs("<div class=\"ak-wb-strip\" data-role-directory><a href=\"/apps\">Your work</a><span>/</span><b>Operating roles</b><a href=\"/vouches\">Vouch for someone in a role</a><a href=\"/work-orders\">Work orders</a><span class=\"ak-live\">Registry</span></div>",stdout);
}
static void role_dir_hero(const char *title,const char *lede){
    fputs("<header class=\"ak-wb-hero\"><div><p class=\"ak-kicker\">Operating roles</p><h1>",stdout);html(title);
    fputs("</h1><p class=\"ak-lede\">",stdout);html(lede);
    fputs("</p></div><aside class=\"ak-panel is-raised\"><p class=\"ak-kicker\">What a role is</p><p>A role is a posture you take on: the queues you watch and the operations you expect to run. "
          "It grants nothing. Every provider, organisation, money and publishing step still checks your current authority on its own.</p></aside></header>",stdout);
}
static void role_dir_foot(void){
    fputs("<p class=\"ak-packs-foot\"><span class=\"ak-live\">No role creates employment, certification, membership, grants, payment authority or permission to act.</span></p>",stdout);
    psetup_end();
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
    role_dir_hero(label&&*label?label:"My operating role","Your current posture in this scope. It tells the estate what you are watching and what you expect to run; it is not an authorisation.");
    fputs("<dl class=\"ak-stats is-row\"><div><dt>Scope</dt><dd class=\"is-on\">",stdout);
    html((const char*)sqlite3_column_text(s,1));
    fputs("</dd><small>where this posture applies</small></div><div><dt>Role</dt><dd>",stdout);html((const char*)sqlite3_column_text(s,3));
    fputs("</dd><small>published role programme</small></div></dl>"
          "<section class=\"ak-grid is-3 role-detail\" data-role-posture>",stdout);
    static const char *names[]={"Work queues","Exact native operations","Operations not yet bound",
      "Authority still required","Handoff contracts","Available lenses"};
    for(int i=0;i<6;i++)role_dir_terms(db,names[i],(const char*)sqlite3_column_text(s,i+4),12);
    fputs("</section><section class=\"ak-band\"><div><p class=\"ak-kicker\">Your posture</p><h2>Open the work or end the posture</h2><p>Ending it does not revoke a separate grant, remove work history or end any employment.</p></div><div class=\"ak-actions\"><a class=\"ak-btn\" href=\"/roles/workspace?assignment=",stdout);
    url_query_value((const char*)sqlite3_column_text(s,0));
    fputs("\">Open the private work view</a>",stdout);
    fputs("<form class=\"role-end\" action=\"/roles/leave\" method=\"post\">",stdout);
    fputs("<input type=\"hidden\" name=\"assignment\" value=\"",stdout);
    html((const char*)sqlite3_column_text(s,0));
    fputs("\"><button class=\"ak-btn is-quiet\" type=\"submit\">End this posture</button></form></div></section>",stdout);
    sqlite3_finalize(s);
    role_dir_foot();return 1;
}
static void role_dir_assignments(sqlite3 *db,const char *actor){
    sqlite3_stmt *s=NULL;
    fputs("<section class=\"ak-sec\" data-role-assignments><header><div><span class=\"ak-kicker\">Yours</span><h2>My current postures</h2></div></header><div class=\"ak-panel\">",stdout);
    const char *sql=
      "SELECT a.assignment_id,a.context_scope,p.label "
      "FROM role_assignment a JOIN role_program p "
      "ON a.namespace=p.namespace AND a.role_program=p.role_id "
      "WHERE a.namespace='root' AND a.actor_subject=?1 AND a.status='published' "
      "AND a.state='assumed' AND p.status='published' "
      "ORDER BY a.assumed_at DESC,a.id DESC LIMIT 31";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);fputs("<p class=\"ak-empty\">Your postures cannot be read right now.</p></div></section>",stdout);return;
    }
    sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
    int found=0,step=0;
    fputs("<ol class=\"ak-rows\">",stdout);
    while((step=sqlite3_step(s))==SQLITE_ROW&&found<30){
        const char *assignment=(const char*)sqlite3_column_text(s,0),
                   *scope=(const char*)sqlite3_column_text(s,1),
                   *label=(const char*)sqlite3_column_text(s,2);
        if(!assignment||!*assignment||!label||!*label)continue;
        found++;fputs("<li data-role-assignment><strong>",stdout);html(label);
        fputs("</strong><small>",stdout);html(scope?scope:"");
        fputs("</small><a class=\"ak-go\" href=\"/roles?assignment=",stdout);url_query_value(assignment);
        fputs("\">Open posture</a></li>",stdout);
    }
    sqlite3_finalize(s);fputs("</ol>",stdout);
    if(!found)fputs("<p class=\"ak-empty\">You have not taken on a posture yet. Choose one from the registry below.</p>",stdout);
    if(found==30||step==SQLITE_ROW)fputs("<p class=\"ak-empty\">More postures exist than this view shows.</p>",stdout);
    fputs("</div></section>",stdout);
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
    const char *sql="SELECT role_id,label,work_queues,"
      "CASE WHEN json_valid(exact_operations) THEN json_array_length(exact_operations) ELSE 0 END,"
      "CASE WHEN json_valid(unresolved_operations) THEN json_array_length(unresolved_operations) ELSE 0 END,"
      "authority_requirements,cannot_infer "
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
    long long roles=0,mine=0;{sqlite3_stmt *c=NULL;
        if(sqlite3_prepare_v2(db,"SELECT (SELECT count(*) FROM role_program WHERE namespace='root' AND status='published'),(SELECT count(*) FROM role_assignment WHERE namespace='root' AND status='published' AND state='assumed' AND actor_subject=?1)",-1,&c,NULL)==SQLITE_OK){
            sqlite3_bind_text(c,1,actor,-1,SQLITE_TRANSIENT);if(sqlite3_step(c)==SQLITE_ROW){roles=sqlite3_column_int64(c,0);mine=sqlite3_column_int64(c,1);}}
        sqlite3_finalize(c);}
    role_dir_hero("Operating roles","The published registry of postures you can take on across Aurekai: the queues each one watches, the operations it can run today, and the authority it still needs from someone else.");
    printf("<dl class=\"ak-stats is-row\"><div><dt>Published roles</dt><dd>%lld</dd><small>in this registry</small></div><div><dt>Your postures</dt><dd class=\"%s\">%lld</dd><small>currently taken on</small></div><div><dt>Scope</dt><dd class=\"is-on\">",roles,mine?"is-on":"",mine);
    html(scope);fputs("</dd><small>where a new posture applies</small></div></dl>",stdout);
    role_dir_assignments(db,actor);
    fputs("<section class=\"ak-sec\" data-role-catalogue><header><div><span class=\"ak-kicker\">Registry</span><h2>Published roles</h2></div>"
          "<p>Only operations admitted by their actual owner can run. Taking on a posture changes your own assignment and nothing else.</p></header>"
          "<form class=\"ak-packs-filter role-search\" action=\"/roles\" method=\"get\" role=\"search\">"
          "<label class=\"ak-packs-find\"><span class=\"ak-visually-hidden\">Find a role</span><input name=\"q\" type=\"search\" maxlength=\"96\" value=\"",stdout);
    html(q);fputs("\" placeholder=\"Filter by role, craft or subject\xe2\x80\xa6\"></label><label><span>Apply in scope</span><input name=\"scope\" maxlength=\"95\" required pattern=\"[A-Za-z0-9._:-]+\" value=\"",stdout);
    html(scope);fputs("\" aria-label=\"Role context scope\"></label><button class=\"ak-btn\" type=\"submit\">Search roles</button></form>"
          "<div class=\"ak-panel ak-scroll\"><table class=\"ak-table role-table\"><thead><tr><th>Code</th><th>Role and what it watches</th><th>Runs today</th><th>Not yet connected</th><th>Authority it needs</th><th>Take it on</th></tr></thead><tbody>",stdout);
    int found=0,step=0;
    while((step=sqlite3_step(s))==SQLITE_ROW && found<24){
        const char *id=(const char*)sqlite3_column_text(s,0),
                   *label=(const char*)sqlite3_column_text(s,1),
                   *queues=(const char*)sqlite3_column_text(s,2),
                   *authority=(const char*)sqlite3_column_text(s,5);
        if(!id||!*id||!label||!*label||
           strspn(id,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(id))continue;
        found++;
        fputs("<tr data-role-program><td><code>",stdout);html(id);fputs("</code></td><td><strong>",stdout);html(label);fputs("</strong>",stdout);
        role_dir_inline(db,queues,3);
        printf("</td><td>%d operations</td><td>%d</td><td>",sqlite3_column_int(s,3),sqlite3_column_int(s,4));
        role_dir_inline(db,authority,3);
        fputs("</td><td><form method=\"post\" action=\"/roles/assume\"><input type=\"hidden\" name=\"role\" value=\"",stdout);
        html(id);fputs("\"><input type=\"hidden\" name=\"return\" value=\"/roles\"><input type=\"hidden\" name=\"scope\" value=\"",stdout);
        html(scope);fputs("\"><button class=\"ak-btn\" type=\"submit\">Take on</button></form></td></tr>",stdout);
    }
    sqlite3_finalize(s);
    fputs("</tbody></table>",stdout);
    if(!found)fputs(q[0]?"<p class=\"ak-empty\">No published role matches this search. No role is inferred.</p>":"<p class=\"ak-empty\">No roles are published in this registry yet. When a role programme is published it appears here with the queues it watches and the authority it needs.</p>",stdout);
    fputs("</div><nav class=\"ak-actions role-pages\" aria-label=\"Role registry pages\">",stdout);
    if(page){fputs("<a class=\"ak-btn\" rel=\"prev\" href=\"/roles?page=",stdout);printf("%u",page-1);
        fputs("&amp;q=",stdout);url_query_value(q);fputs("&amp;scope=",stdout);url_query_value(scope);fputs("\">Previous roles</a>",stdout);}
    if((found==24||step==SQLITE_ROW)&&page<400){
        fputs("<a class=\"ak-btn\" rel=\"next\" href=\"/roles?page=",stdout);printf("%u",page+1);
        fputs("&amp;q=",stdout);url_query_value(q);fputs("&amp;scope=",stdout);url_query_value(scope);fputs("\">More roles</a>",stdout);
    }
    fputs("</nav></section>",stdout);role_dir_foot();sqlite3_close(db);return 1;
}
#endif
