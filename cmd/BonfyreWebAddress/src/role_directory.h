#ifndef BONFYRE_ROLE_DIRECTORY_H
#define BONFYRE_ROLE_DIRECTORY_H
#include "ui_tree.h"
#include "form_tree.h"
/* A human-operated role registry over native RoleProgram / RoleAssignment.
 * These are published postures, not new grants, jobs, employment, invitations
 * or bypasses around effect-specific admission. */
static void role_dir_terms(sqlite3 *db,const char *title,const char *raw,size_t limit){
    sqlite3_stmt *stmt=NULL;int count=0;
    ui_open(stdout,"section","ak-panel role-terms");ui_open(stdout,"h3",NULL);html(title);ui_close(stdout,"h3");
    if(!raw||!*raw||sqlite3_prepare_v2(db,
       "SELECT value FROM json_each(?1) WHERE type='text' LIMIT ?2",-1,&stmt,NULL)!=SQLITE_OK){
        sqlite3_finalize(stmt);ui_open(stdout,"p","ak-empty");ui_copy(stdout,"None published.");ui_close(stdout,"p");ui_close(stdout,"section");return;
    }
    sqlite3_bind_text(stmt,1,raw,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt,2,(int)limit);
    while(sqlite3_step(stmt)==SQLITE_ROW){
        const char *value=(const char*)sqlite3_column_text(stmt,0);
        if(!value||!*value)continue;
        if(!count++)ui_open(stdout,"ul","ak-rows");
        ui_text(stdout, "li", NULL, value);
    }
    sqlite3_finalize(stmt);
    if(count)ui_close(stdout,"ul");else ui_text(stdout, "p", "ak-empty", "None published.");
    ui_close(stdout,"section");
}
static void role_dir_inline(sqlite3 *db,const char *raw,int limit){
    sqlite3_stmt *st=NULL;int n=0;
    if(!raw||!*raw||sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1) WHERE type='text' LIMIT ?2",-1,&st,NULL)!=SQLITE_OK){sqlite3_finalize(st);return;}
    sqlite3_bind_text(st,1,raw,-1,SQLITE_TRANSIENT);sqlite3_bind_int(st,2,limit);
    while(sqlite3_step(st)==SQLITE_ROW){const char *v=(const char*)sqlite3_column_text(st,0);if(!v||!*v)continue;fputs(n++?", ":"<br><small>",stdout);html(v);}
    if(n)ui_close(stdout,"small");sqlite3_finalize(st);
}
/* Stitch: the operator roles directory/registry, on the one shell. */
static void role_dir_head(const char *title){
    psetup_open(200,title,"ak-wb role-registry");
    ui_start(stdout,"div");ui_attr(stdout,"class","ak-wb-strip");ui_flag(stdout,"data-role-directory");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"href","/apps");ui_gt(stdout);ui_copy(stdout,"Your work");ui_close(stdout,"a");ui_open(stdout,"span",NULL);ui_copy(stdout,"/");ui_close(stdout,"span");ui_open(stdout,"b",NULL);ui_copy(stdout,"Operating roles");ui_close(stdout,"b");ui_start(stdout,"a");ui_attr(stdout,"href","/vouches");ui_gt(stdout);ui_copy(stdout,"Vouch for someone in a role");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/work-orders");ui_gt(stdout);ui_copy(stdout,"Work orders");ui_close(stdout,"a");ui_open(stdout,"span","ak-live");ui_copy(stdout,"Registry");ui_close(stdout,"span");ui_close(stdout,"div");
}
static void role_dir_hero(const char *title,const char *lede){
    ui_open(stdout,"header","ak-wb-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ak-kicker");ui_copy(stdout,"Operating roles");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);html(title);
    ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");html(lede);
    ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"aside","ak-panel is-raised");ui_open(stdout,"p","ak-kicker");ui_copy(stdout,"What a role is");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"A role is a posture you take on: the queues you watch and the operations you expect to run. It grants nothing. Every provider, organisation, money and publishing step still checks your current authority on its own.");ui_close(stdout,"p");ui_close(stdout,"aside");ui_close(stdout,"header");
}
static void role_dir_foot(void){
    ui_open(stdout,"p","ak-packs-foot");ui_open(stdout,"span","ak-live");ui_copy(stdout,"No role creates employment, certification, membership, grants, payment authority or permission to act.");ui_close(stdout,"span");ui_close(stdout,"p");
    psetup_end();
}
/* Ending a personal posture touches only the signed-in actor's exact
 * assignment. It does not revoke an institutional grant or finish work. */
static int serve_role_leave(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *body=r->body; const char *raw=r->raw; const char *host=r->host; const char *bearer=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;
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
    ui_open(stdout,"dl","ak-stats is-row");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Scope");ui_close(stdout,"dt");ui_open(stdout,"dd","is-on");
    html((const char*)sqlite3_column_text(s,1));
    ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"where this posture applies");ui_close(stdout,"small");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Role");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);html((const char*)sqlite3_column_text(s,3));
    ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"published role programme");ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"dl");ui_start(stdout,"section");ui_attr(stdout,"class","ak-grid is-3 role-detail");ui_flag(stdout,"data-role-posture");ui_gt(stdout);
    static const char *names[]={"Work queues","Exact native operations","Operations not yet bound",
      "Authority still required","Handoff contracts","Available lenses"};
    for(int i=0;i<6;i++)role_dir_terms(db,names[i],(const char*)sqlite3_column_text(s,i+4),12);
    ui_close(stdout,"section");ui_open(stdout,"section","ak-band");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ak-kicker");ui_copy(stdout,"Your posture");ui_close(stdout,"p");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Open the work or end the posture");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Ending it does not revoke a separate grant, remove work history or end any employment.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,"/roles/workspace?assignment=");
    url_query_value((const char*)sqlite3_column_text(s,0));
    fputs("\">Open the private work view</a>",stdout);
    {const FtField hidden[]={{"assignment",(const char*)sqlite3_column_text(s,0)}};
     const FtForm leave={.id="role-end",.action="/roles/leave",.label="End this posture",.hook="role-end",.hidden=hidden,.nhidden=1};
     ft_form(stdout,&leave);}
    ui_close(stdout,"div");ui_close(stdout,"section");
    sqlite3_finalize(s);
    role_dir_foot();return 1;
}
static void role_dir_assignments(sqlite3 *db,const char *actor){
    sqlite3_stmt *s=NULL;
    ui_start(stdout,"section");ui_attr(stdout,"class","ak-sec");ui_flag(stdout,"data-role-assignments");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Yours");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"My current postures");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"div","ak-panel");
    const char *sql=
      "SELECT a.assignment_id,a.context_scope,p.label "
      "FROM role_assignment a JOIN role_program p "
      "ON a.namespace=p.namespace AND a.role_program=p.role_id "
      "WHERE a.namespace='root' AND a.actor_subject=?1 AND a.status='published' "
      "AND a.state='assumed' AND p.status='published' "
      "ORDER BY a.assumed_at DESC,a.id DESC LIMIT 31";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);ui_open(stdout,"p","ak-empty");ui_copy(stdout,"Your postures cannot be read right now.");ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"section");return;
    }
    sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
    int found=0,step=0;
    ui_open(stdout,"ol","ak-rows");
    while((step=sqlite3_step(s))==SQLITE_ROW&&found<30){
        const char *assignment=(const char*)sqlite3_column_text(s,0),
                   *scope=(const char*)sqlite3_column_text(s,1),
                   *label=(const char*)sqlite3_column_text(s,2);
        if(!assignment||!*assignment||!label||!*label)continue;
        found++;ui_start(stdout,"li");ui_flag(stdout,"data-role-assignment");ui_gt(stdout);ui_open(stdout,"strong",NULL);html(label);
        ui_close(stdout,"strong");ui_open(stdout,"small",NULL);html(scope?scope:"");
        ui_close(stdout,"small");ui_start(stdout,"a");ui_attr(stdout,"class","ak-go");ui_attr_open(stdout,"href");ui_copy(stdout,"/roles?assignment=");url_query_value(assignment);
        fputs("\">Open posture</a></li>",stdout);
    }
    sqlite3_finalize(s);ui_close(stdout,"ol");
    if(!found)ui_text(stdout, "p", "ak-empty", "You have not taken on a posture yet. Choose one from the registry below.");
    if(found==30||step==SQLITE_ROW)ui_text(stdout, "p", "ak-empty", "More postures exist than this view shows.");
    ui_close(stdout,"div");ui_close(stdout,"section");
}
static int serve_role_directory(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *host=r->host; const char *bearer=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)query;(void)host;(void)bearer;(void)auth_db;
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
    ui_open(stdout,"dl","ak-stats is-row");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Published roles");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%lld",roles);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"in this registry");ui_close(stdout,"small");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Your postures");ui_close(stdout,"dt");ui_start(stdout,"dd");ui_attr_open(stdout,"class");ui_copy(stdout,mine?"is-on":"");ui_attr_close(stdout);ui_gt(stdout);fprintf(stdout,"%lld",mine);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"currently taken on");ui_close(stdout,"small");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Scope");ui_close(stdout,"dt");ui_open(stdout,"dd","is-on");
    html(scope);ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"where a new posture applies");ui_close(stdout,"small");ui_close(stdout,"div");ui_close(stdout,"dl");
    role_dir_assignments(db,actor);
    ui_start(stdout,"section");ui_attr(stdout,"class","ak-sec");ui_flag(stdout,"data-role-catalogue");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Registry");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Published roles");ui_close(stdout,"h2");ui_close(stdout,"div");ui_open(stdout,"p",NULL);ui_copy(stdout,"Only operations admitted by their actual owner can run. Taking on a posture changes your own assignment and nothing else.");ui_close(stdout,"p");ui_close(stdout,"header");
    {const FtInput find[]={{.label="Find a role",.type="search",.name="q",.maxlength="96",.value=q,.placeholder="Filter by role, craft or subject\xe2\x80\xa6"},
                           {.label="Apply in scope",.name="scope",.maxlength="95",.required=1,.pattern="[A-Za-z0-9._:-]+",.value=scope}};
     const FtForm search={.id="role-search",.method="get",.action="/roles",.label="Search roles",.hook="role-search",.inputs=find,.ninputs=2};
     ft_form(stdout,&search);}
    ui_open(stdout,"div","ak-panel ak-scroll");ui_open(stdout,"table","ak-table role-table");ui_open(stdout,"thead",NULL);ui_open(stdout,"tr",NULL);ui_open(stdout,"th",NULL);ui_copy(stdout,"Code");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Role and what it watches");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Runs today");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Not yet connected");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Authority it needs");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Take it on");ui_close(stdout,"th");ui_close(stdout,"tr");ui_close(stdout,"thead");ui_open(stdout,"tbody",NULL);
    int found=0,step=0;
    while((step=sqlite3_step(s))==SQLITE_ROW && found<24){
        const char *id=(const char*)sqlite3_column_text(s,0),
                   *label=(const char*)sqlite3_column_text(s,1),
                   *queues=(const char*)sqlite3_column_text(s,2),
                   *authority=(const char*)sqlite3_column_text(s,5);
        if(!id||!*id||!label||!*label||
           strspn(id,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(id))continue;
        found++;
        ui_start(stdout,"tr");ui_flag(stdout,"data-role-program");ui_gt(stdout);ui_open(stdout,"td",NULL);ui_open(stdout,"code",NULL);html(id);ui_close(stdout,"code");ui_close(stdout,"td");ui_open(stdout,"td",NULL);ui_open(stdout,"strong",NULL);html(label);ui_close(stdout,"strong");
        role_dir_inline(db,queues,3);
        ui_close(stdout,"td");ui_open(stdout,"td",NULL);fprintf(stdout,"%d",sqlite3_column_int(s,3));ui_copy(stdout," operations");ui_close(stdout,"td");ui_open(stdout,"td",NULL);fprintf(stdout,"%d",sqlite3_column_int(s,4));ui_close(stdout,"td");ui_open(stdout,"td",NULL);
        role_dir_inline(db,authority,3);
        ui_close(stdout,"td");ui_open(stdout,"td",NULL);
        {char fid[140];snprintf(fid,sizeof(fid),"role-take-%s",id);
         const FtField hidden[]={{"role",id},{"return","/roles"},{"scope",scope}};
         const FtForm take={.id=fid,.action="/roles/assume",.label="Take on",.hidden=hidden,.nhidden=3};
         ft_form(stdout,&take);}
        ui_close(stdout,"td");ui_close(stdout,"tr");
    }
    sqlite3_finalize(s);
    ui_close(stdout,"tbody");ui_close(stdout,"table");
    if(!found)fputs(q[0]?"<p class=\"ak-empty\">No published role matches this search. No role is inferred.</p>":"<p class=\"ak-empty\">No roles are published in this registry yet. When a role programme is published it appears here with the queues it watches and the authority it needs.</p>",stdout);
    ui_close(stdout,"div");ui_start(stdout,"nav");ui_attr(stdout,"class","ak-actions role-pages");ui_attr(stdout,"aria-label","Role registry pages");ui_gt(stdout);
    if(page){ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr(stdout,"rel","prev");ui_attr_open(stdout,"href");ui_copy(stdout,"/roles?page=");printf("%u",page-1);
        fputs("&amp;q=",stdout);url_query_value(q);fputs("&amp;scope=",stdout);url_query_value(scope);fputs("\">Previous roles</a>",stdout);}
    if((found==24||step==SQLITE_ROW)&&page<400){
        ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr(stdout,"rel","next");ui_attr_open(stdout,"href");ui_copy(stdout,"/roles?page=");printf("%u",page+1);
        fputs("&amp;q=",stdout);url_query_value(q);fputs("&amp;scope=",stdout);url_query_value(scope);fputs("\">More roles</a>",stdout);
    }
    ui_close(stdout,"nav");ui_close(stdout,"section");role_dir_foot();sqlite3_close(db);return 1;
}
#endif
