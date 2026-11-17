#ifndef BONFYRE_COMMUNITY_PROGRAMS_H
#define BONFYRE_COMMUNITY_PROGRAMS_H
#include "ui_tree.h"
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
    ui_start(stdout,"p");ui_flag(stdout,"data-requirement-source-note");ui_gt(stdout);ui_start(stdout,"a");ui_attr_open(stdout,"href");
    cjw_href(guild->key,"thread",note.key);
    ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open the published workshop note");ui_close(stdout,"a");ui_copy(stdout," · This source is visible, but completion is not verified.");ui_close(stdout,"p");
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
        ui_start(stdout,"p");ui_attr(stdout,"role","status");ui_gt(stdout);ui_copy(stdout,"The programme requirement read is currently unavailable.");ui_close(stdout,"p");
        return;
    }
    sqlite3_bind_text(s,1,instance,-1,SQLITE_TRANSIENT);
    int count=0,step=0;
    ui_start(stdout,"ul");ui_attr(stdout,"class","cj-program-reqs");ui_flag(stdout,"data-program-requirements");ui_gt(stdout);
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
        ui_start(stdout,"li");ui_flag(stdout,"data-program-requirement");ui_gt(stdout);ui_open(stdout,"div",NULL);ui_open(stdout,"strong",NULL);html(statement);
        ui_close(stdout,"strong");ui_open(stdout,"p",NULL);ui_open(stdout,"small",NULL);html(kind?kind:"Requirement");
        ui_copy(stdout," · ");html(state);
        if(deadline&&*deadline){ui_copy(stdout," · Due ");html(deadline);}
        ui_close(stdout,"small");ui_close(stdout,"p");ui_close(stdout,"div");
        if(steward&&((satisfied&&*satisfied)||(evidence&&*evidence))){
            ui_open(stdout,"p","cj-program-evidence");ui_copy(stdout,"Retained evidence reference is ");
            ui_copy(stdout,evidence&&*evidence?"present":"not supplied");
            ui_copy(stdout,". This view does not independently verify completion.");ui_close(stdout,"p");
        }
        if(evidence&&*evidence)cjw_programme_note(db,guild,evidence);
        ui_close(stdout,"li");
    }
    sqlite3_finalize(s);
    if(!count)ui_text(stdout, "li", NULL, "No published programme requirements in this scope.");
    ui_close(stdout,"ul");
    if(count==30||step==SQLITE_ROW)
        ui_text(stdout, "p", NULL, "More requirements exist. Use the owning programme workspace for the full record.");
}
static void cjw_programmes(sqlite3 *db,const CjrCommunity *community,int steward){
    if(!db||!community)return;
    ui_start(stdout,"section");ui_flag(stdout,"data-community-programmes");ui_attr(stdout,"aria-label","Guild programme commitments");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Guild programmes and obligations");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"The programmes shown here explicitly name this guild as their owner. A shared programme does not create an apprenticeship, school placement, equipment permission, award, funding approval or payment entitlement. Requests and evidence remain with their original owners.");ui_close(stdout,"p");
    if(!cjw_programme_schema(db)){
        ui_start(stdout,"p");ui_attr(stdout,"role","status");ui_gt(stdout);ui_copy(stdout,"The programme records are not available in this site context. No programmes or requirements are inferred.");ui_close(stdout,"p");ui_close(stdout,"section");
        return;
    }
    char subject[140];
    if(snprintf(subject,sizeof(subject),"community_body:%s",community->key)>=(int)sizeof(subject)){
        ui_close(stdout,"section");return;
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
        ui_start(stdout,"p");ui_attr(stdout,"role","status");ui_gt(stdout);ui_copy(stdout,"The programme owner could not be read completely.");ui_close(stdout,"p");ui_close(stdout,"section");
        sqlite3_finalize(s);return;
    }
    sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
    int count=0,step=0;
    ui_start(stdout,"ol");ui_attr(stdout,"class","cj-program-list");ui_flag(stdout,"data-program-instance-list");ui_gt(stdout);
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
        ui_start(stdout,"li");ui_flag(stdout,"data-program-instance");ui_gt(stdout);ui_open(stdout,"h3",NULL);html(name);
        ui_close(stdout,"h3");ui_open(stdout,"p","cj-program-meta");
        html(archetype?archetype:"Programme");ui_copy(stdout," · Opened ");
        html(opened?opened:"Date unavailable");ui_close(stdout,"p");
        if(description&&*description){ui_open(stdout,"p",NULL);cjw_excerpt(description);ui_close(stdout,"p");}
        ui_section_open(stdout,NULL,"Declared work and requirements");
        cjw_programme_requirements(db,community,instance,steward);
        ui_section_close(stdout);
        if(steward){
            ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/programs?context=self&amp;template=");
            url_query_value(template_id);
            ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Open the programme owner");ui_close(stdout,"a");ui_copy(stdout," · ");
            ui_copy(stdout,"Programme edits require separate workspace authority.");ui_close(stdout,"p");
        }
        (void)site;
        ui_close(stdout,"li");
    }
    sqlite3_finalize(s);
    if(!count)ui_text(stdout, "li", NULL, "No programmes are explicitly bound to this guild yet.");
    ui_close(stdout,"ol");
    if(count>=20||step==SQLITE_ROW)
        ui_text(stdout, "p", NULL, "Additional programmes exist in the owning programme workspace.");
    char role_scope[140];snprintf(role_scope,sizeof(role_scope),"community:%s",community->key);
    ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_flag(stdout,"data-guild-role-bridge");ui_attr_open(stdout,"href");ui_copy(stdout,"/roles?scope=");url_query_value(role_scope);
    ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Choose a role for this guild");ui_close(stdout,"a");ui_copy(stdout,". Role postures organize your work; they do not enroll members, grant authority or execute guild operations.");ui_close(stdout,"p");
    ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");cjw_href(community->key,"write",NULL);
    ui_attr_close(stdout);ui_gt(stdout);ui_copy(stdout,"Document a workshop observation");ui_close(stdout,"a");ui_copy(stdout,". A member's reviewed contribution "
          "does not, by itself, satisfy a formal programme requirement.");ui_close(stdout,"p");ui_close(stdout,"section");
}
#endif
