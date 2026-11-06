#ifndef BONFYRE_COMMUNITY_PROGRAMS_H
#define BONFYRE_COMMUNITY_PROGRAMS_H
/* An account's guild is not a ProgrammeFabric owner. Read the real programmes
 * only when they explicitly name this community's canonical subject. The
 * community owner supplies membership admission; ProgrammeFabric retains all
 * work, evidence and financial authority. No inferred enrollment or award. */
static int cjw_programme_schema(sqlite3 *db) {
    sqlite3_stmt *s=NULL;int count=0;
    if(sqlite3_prepare_v2(db,
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN"
        " ('program_instance','program_template','program_requirement')",
        -1,&s,NULL)==SQLITE_OK && sqlite3_step(s)==SQLITE_ROW)
        count=sqlite3_column_int(s,0);
    sqlite3_finalize(s);
    return count==3;
}
/* Explicit note references must resolve within the same guild. Their
 * existence is evidence to read, not proof of requirement satisfaction. */
static int cjw_programme_note(sqlite3 *db,const CjrCommunity *guild,const char *ref){
    if(!db||!guild||!ref||strncmp(ref,"community_post:",15))return 0;
    const char *key=ref+15;CjrPost note;
    if(!cjr_key(key)||cjr_post(db,guild->key,key,&note)!=1||!cjr_visible(&note))return 0;
    char root[97];if(!cjr_thread(db,guild->key,&note,root,1))return 0;
    fputs("<p data-requirement-source-note><a href=\"",stdout);
    cjw_href(guild->key,"thread",note.key);
    fputs("\">Open the published workshop note</a> · This source is visible, but completion is not verified.</p>",stdout);
    return 1;
}
static void cjw_programme_requirements(sqlite3 *db,const CjrCommunity *guild,const char *instance,int steward) {
    sqlite3_stmt *s=NULL;
    const char *sql=
      "SELECT requirement_id,kind,statement,state,COALESCE(satisfied_by,''),"
      "COALESCE(evidence_ref,''),COALESCE(deadline,'') "
      "FROM program_requirement WHERE namespace='root' AND instance_id=?1 "
      "AND status='published' ORDER BY CASE WHEN state IN ('open','pending','blocked') THEN 0 ELSE 1 END,"
      "id DESC LIMIT 31";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);
        fputs("<p role=\"status\">The programme requirement read is currently unavailable.</p>",stdout);
        return;
    }
    sqlite3_bind_text(s,1,instance,-1,SQLITE_TRANSIENT);
    int count=0,step=0;
    fputs("<ul class=\"cj-program-reqs\" data-program-requirements>",stdout);
    while((step=sqlite3_step(s))==SQLITE_ROW && count<30){
        const char *id=(const char*)sqlite3_column_text(s,0),
                   *kind=(const char*)sqlite3_column_text(s,1),
                   *statement=(const char*)sqlite3_column_text(s,2),
                   *state=(const char*)sqlite3_column_text(s,3),
                   *satisfied=(const char*)sqlite3_column_text(s,4),
                   *evidence=(const char*)sqlite3_column_text(s,5),
                   *deadline=(const char*)sqlite3_column_text(s,6);
        if(!id||!*id||!statement||!*statement||!state||!*state)continue;
        count++;
        fputs("<li data-program-requirement><div><strong>",stdout);html(statement);
        fputs("</strong><p><small>",stdout);html(kind?kind:"Requirement");
        fputs(" · ",stdout);html(state);
        if(deadline&&*deadline){fputs(" · Due ",stdout);html(deadline);}
        fputs("</small></p></div>",stdout);
        if(steward&&((satisfied&&*satisfied)||(evidence&&*evidence))){
            fputs("<p class=\"cj-program-evidence\">Retained evidence reference is ",stdout);
            fputs(evidence&&*evidence?"present":"not supplied",stdout);
            fputs(". This view does not independently verify completion.</p>",stdout);
        }
        if(evidence&&*evidence)cjw_programme_note(db,guild,evidence);
        fputs("</li>",stdout);
    }
    sqlite3_finalize(s);
    if(!count)fputs("<li>No published programme requirements in this scope.</li>",stdout);
    fputs("</ul>",stdout);
    if(count==30||step==SQLITE_ROW)
        fputs("<p>More requirements exist. Use the owning programme workspace for the full record.</p>",stdout);
}
static void cjw_programmes(sqlite3 *db,const CjrCommunity *community,int steward){
    if(!db||!community)return;
    fputs("<section data-community-programmes aria-label=\"Guild programme commitments\">"
          "<h2>Guild programmes and obligations</h2>"
          "<p>The programmes shown here explicitly name this guild as their owner. "
          "A shared programme does not create an apprenticeship, school placement, equipment permission, award, "
          "funding approval or payment entitlement. Requests and evidence remain with their original owners.</p>",stdout);
    if(!cjw_programme_schema(db)){
        fputs("<p role=\"status\">The programme records are not available in this site context. "
              "No programmes or requirements are inferred.</p></section>",stdout);
        return;
    }
    char subject[140];
    if(snprintf(subject,sizeof(subject),"community_body:%s",community->key)>=(int)sizeof(subject)){
        fputs("</section>",stdout);return;
    }
    sqlite3_stmt *s=NULL;
    const char *sql=
      "SELECT i.instance_id,i.template_id,t.name,t.archetype,COALESCE(t.description,''),"
      "i.opened_at,COALESCE(i.site_id,'') "
      "FROM program_instance i JOIN program_template t "
      "ON t.namespace=i.namespace AND t.template_id=i.template_id "
      "WHERE i.namespace='root' AND i.status='published' AND t.status='published' "
      "AND i.owner_subject=?1 ORDER BY i.opened_at DESC,i.id DESC LIMIT 21";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        fputs("<p role=\"status\">The programme owner could not be read completely.</p></section>",stdout);
        sqlite3_finalize(s);return;
    }
    sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
    int count=0,step=0;
    fputs("<style>"
          ".cj-program-list{display:grid;gap:16px;list-style:none;padding:0;margin:20px 0}"
          ".cj-program-list>li{border:1px solid #b9ccb6;border-radius:14px;padding:clamp(14px,3vw,24px);background:#fbfcf4}"
          ".cj-program-list h3{font:400 clamp(21px,2.6vw,29px)/1.2 Georgia,serif;margin:0 0 10px}"
          ".cj-program-meta{font:12px/1.5 system-ui;color:#526950}"
          ".cj-program-reqs{list-style:none;display:grid;gap:8px;padding:0;margin:12px 0}"
          ".cj-program-reqs li{padding:12px;border-left:3px solid #9cb894;background:#f0f6e9}"
          ".cj-program-reqs strong{font:600 13px/1.4 system-ui}"
          ".cj-program-evidence{font:12px/1.5 system-ui;opacity:.85}"
          "@media(max-width:390px){.cj-program-list>li{padding:14px}}"
          "</style><ol class=\"cj-program-list\" data-program-instance-list>",stdout);
    while((step=sqlite3_step(s))==SQLITE_ROW && count<20){
        const char *instance=(const char*)sqlite3_column_text(s,0),
                   *template_id=(const char*)sqlite3_column_text(s,1),
                   *name=(const char*)sqlite3_column_text(s,2),
                   *archetype=(const char*)sqlite3_column_text(s,3),
                   *description=(const char*)sqlite3_column_text(s,4),
                   *opened=(const char*)sqlite3_column_text(s,5),
                   *site=(const char*)sqlite3_column_text(s,6);
        if(!instance||!*instance||!template_id||!*template_id||!name||!*name)continue;
        count++;
        fputs("<li data-program-instance><h3>",stdout);html(name);
        fputs("</h3><p class=\"cj-program-meta\">",stdout);
        html(archetype?archetype:"Programme");fputs(" · Opened ",stdout);
        html(opened?opened:"Date unavailable");fputs("</p>",stdout);
        if(description&&*description){fputs("<p>",stdout);cjw_excerpt(description);fputs("</p>",stdout);}
        fputs("<details><summary>Declared work and requirements</summary>",stdout);
        cjw_programme_requirements(db,community,instance,steward);
        fputs("</details>",stdout);
        if(steward){
            fputs("<p><a href=\"/programs?context=self&amp;template=",stdout);
            url_query_value(template_id);
            fputs("\">Open the programme owner</a> · ",stdout);
            fputs("Programme edits require separate workspace authority.</p>",stdout);
        }
        (void)site;
        fputs("</li>",stdout);
    }
    sqlite3_finalize(s);
    if(!count)fputs("<li>No programmes are explicitly bound to this guild yet.</li>",stdout);
    fputs("</ol>",stdout);
    if(count>=20||step==SQLITE_ROW)
        fputs("<p>Additional programmes exist in the owning programme workspace.</p>",stdout);
    fputs("<p><a href=\"",stdout);cjw_href(community->key,"write",NULL);
    fputs("\">Document a workshop observation</a>. A member's reviewed contribution "
          "does not, by itself, satisfy a formal programme requirement.</p></section>",stdout);
}
#endif
