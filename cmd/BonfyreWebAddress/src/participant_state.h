#ifndef BONFYRE_PARTICIPANT_STATE_H
#define BONFYRE_PARTICIPANT_STATE_H
/* State transitions within the existing CommunityMembership/OccurrenceFollow
 * owner. No new tables, role grants, provider actions or publication records.
 * bf_sha256_hex is supplied by the existing native shared library. */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>

typedef enum { PJ_OCCURRENCE = 1, PJ_COMMUNITY = 2 } PjKind;
typedef struct {
    PjKind kind;
    sqlite3_int64 row;
    int visible, present, expired, managed;
    char target[200], actor[64], key[288];
    char title[1024], description[8192], href[2049];
    char status[32], state[32], role[64], authority[256];
    char created[80], updated[80], began[80], expires[80];
    char expected[65];
} PjRecord;

static int pj_key(const char *s, size_t cap) {
    return s && *s && strlen(s) < cap &&
        strspn(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:") == strlen(s);
}
static int pj_actor(const char *s) {
    char normalized[64], *end; long long id;
    if (!s || strncmp(s,"user:",5) || !s[5] || strlen(s)>=64) return 0;
    errno=0; id=strtoll(s+5,&end,10);
    if (errno || *end || id<1) return 0;
    snprintf(normalized,sizeof(normalized),"user:%lld",id);
    return !strcmp(normalized,s);
}
static const char *pj_kind_name(PjKind k) {
    return k==PJ_COMMUNITY?"community":k==PJ_OCCURRENCE?"occurrence":"";
}
static PjKind pj_kind(const char *s) {
    return s&&!strcmp(s,"community")?PJ_COMMUNITY:
           s&&!strcmp(s,"occurrence")?PJ_OCCURRENCE:0;
}
static int pj_column(sqlite3_stmt *s,int at,char *out,size_t cap) {
    const unsigned char *v=sqlite3_column_text(s,at);
    int n=sqlite3_column_bytes(s,at);
    if (!v) { out[0]=0; return 1; }
    if (n<0 || (size_t)n>=cap || memchr(v,0,(size_t)n)) return 0;
    memcpy(out,v,(size_t)n); out[n]=0; return 1;
}
static void pj_digest(PjRecord *r) {
    char body[2400]; size_t used=0;
    const char *v[]={pj_kind_name(r->kind),r->actor,r->target,r->key,
        r->status,r->state,r->role,r->authority,r->created,r->updated,r->began,r->expires};
    int n=snprintf(body,sizeof(body),"participant-state-v1:%lld:",(long long)r->row);
    r->expected[0]=0; if(n<0||(size_t)n>=sizeof(body))return;used=(size_t)n;
    for(size_t i=0;i<sizeof(v)/sizeof(v[0]);i++){
        n=snprintf(body+used,sizeof(body)-used,"%zu:%s",strlen(v[i]),v[i]);
        if(n<0||(size_t)n>=sizeof(body)-used)return;
        used+=(size_t)n;
    }
    bf_sha256_hex((const unsigned char*)body,used,r->expected);
}
/* 200: accessible source or own retained relation. Hidden source content never
 * enters the result. Duplicate root identities fail closed instead of merging. */
static int pj_load(sqlite3 *db,PjKind kind,const char *actor,const char *id,PjRecord *out) {
    sqlite3_stmt *s=NULL;int step,rows=0,ok=1;
    if(!db||!out||!pj_key(id,200)||(actor&&*actor&&!pj_actor(actor))||!pj_kind_name(kind)[0])return 400;
    memset(out,0,sizeof(*out));out->kind=kind;
    snprintf(out->target,sizeof(out->target),"%s",id);
    snprintf(out->actor,sizeof(out->actor),"%s",actor?actor:"");
    const char *source=kind==PJ_COMMUNITY?
      "SELECT title,COALESCE(description,''),'' FROM community_body "
      "WHERE namespace='root' AND community=?1 AND status='published' "
      "AND visibility IN ('public','public_observed') LIMIT 2":
      "SELECT title,COALESCE(summary,''),COALESCE(canonical_href,'') FROM publication_occurrence "
      "WHERE namespace='root' AND occurrence_id=?1 AND status='published' "
      "AND visibility='public' AND lower(assertion_state)='observed' LIMIT 2";
    if(sqlite3_prepare_v2(db,source,-1,&s,NULL)!=SQLITE_OK)return 503;
    sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        if(++rows>1){sqlite3_finalize(s);return 409;}
        ok=pj_column(s,0,out->title,sizeof(out->title))&&
           pj_column(s,1,out->description,sizeof(out->description))&&
           pj_column(s,2,out->href,sizeof(out->href));
        if(!ok){sqlite3_finalize(s);return 503;}
    }
    sqlite3_finalize(s);s=NULL;if(step!=SQLITE_DONE)return 503;
    out->visible=rows==1;
    if(!out->visible)snprintf(out->title,sizeof(out->title),"Source no longer public");
    /* A declared community policy or owner belongs to its own native
     * lifecycle. This legacy-observation writer must not become a second
     * membership API for the new community-work owner. Read inside the same
     * transaction as mutations so a newly installed policy cannot race it. */
    if(kind==PJ_COMMUNITY){
        const char *owned="SELECT 1 FROM community_body WHERE namespace='root' AND community=?1 "
          "AND (COALESCE(moderation_policy,'')<>'' OR COALESCE(created_by_subject,'')<>'') LIMIT 1";
        if(sqlite3_prepare_v2(db,owned,-1,&s,NULL)!=SQLITE_OK)return 503;
        sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);step=sqlite3_step(s);
        out->managed=step==SQLITE_ROW;sqlite3_finalize(s);s=NULL;
        if(step!=SQLITE_ROW&&step!=SQLITE_DONE)return 503;
    }
    if(actor&&*actor){
        const char *relation=kind==PJ_COMMUNITY?
          "SELECT id,membership,status,state,role,COALESCE(authority_ref,''),"
          "COALESCE(created_at,''),COALESCE(updated_at,''),COALESCE(joined_at,''),COALESCE(expires_at,''),"
          "CASE WHEN COALESCE(expires_at,'')='' THEN 0 WHEN julianday(expires_at)>julianday('now') THEN 0 ELSE 1 END "
          "FROM community_membership WHERE namespace='root' AND community=?1 AND member_subject=?2 LIMIT 2":
          "SELECT id,follow_id,status,'','','',COALESCE(created_at,''),COALESCE(updated_at,''),"
          "COALESCE(followed_at,''),'',0 FROM occurrence_follow "
          "WHERE namespace='root' AND occurrence_id=?1 AND follower=?2 LIMIT 2";
        if(sqlite3_prepare_v2(db,relation,-1,&s,NULL)!=SQLITE_OK)return 503;
        sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,actor,-1,SQLITE_STATIC);
        rows=0;
        while((step=sqlite3_step(s))==SQLITE_ROW){
            if(++rows>1){sqlite3_finalize(s);return 409;}
            out->row=sqlite3_column_int64(s,0);
            char *values[]={out->key,out->status,out->state,out->role,out->authority,
                            out->created,out->updated,out->began,out->expires};
            const size_t caps[]={sizeof(out->key),sizeof(out->status),sizeof(out->state),sizeof(out->role),
                sizeof(out->authority),sizeof(out->created),sizeof(out->updated),sizeof(out->began),sizeof(out->expires)};
            for(size_t i=0;i<sizeof(caps)/sizeof(caps[0]);i++)if(!pj_column(s,(int)i+1,values[i],caps[i]))ok=0;
            out->expired=sqlite3_column_int(s,10);if(!ok||out->row<1){sqlite3_finalize(s);return 503;}
        }
        sqlite3_finalize(s);if(step!=SQLITE_DONE)return 503;out->present=rows==1;
    }
    pj_digest(out);if(!out->expected[0])return 503;
    return out->visible||out->present?200:404;
}
static const char *pj_state_label(const PjRecord *r) {
    if(!r->present)return r->kind==PJ_COMMUNITY?"Not requested":"Not following";
    if(r->kind==PJ_OCCURRENCE)return !strcmp(r->status,"published")?"Following locally":
        !strcmp(r->status,"archived")?"No longer following":"Needs review";
    if(!strcmp(r->state,"withdrawn"))return "Request withdrawn";
    if(!strcmp(r->state,"active")&&!strcmp(r->status,"published"))return r->expired?"Membership expired":"Membership active";
    if(!strcmp(r->state,"requested")&&!strcmp(r->status,"published"))return r->expired?"Request expired":"Request saved";
    if(!strcmp(r->state,"rejected"))return "Request declined";
    if(!strcmp(r->state,"suspended"))return "Membership suspended";
    if(!strcmp(r->state,"revoked"))return "Membership revoked";
    if(!strcmp(r->state,"left"))return "Left community";
    return "Membership needs review";
}
static int pj_action_offered(const PjRecord *r,const char *action) {
    if(!r||!pj_actor(r->actor)||!action||r->managed)return 0;
    if(r->kind==PJ_OCCURRENCE){
        if(!strcmp(action,"follow"))return r->visible&&(!r->present||!strcmp(r->status,"archived"));
        if(!strcmp(action,"unfollow"))return r->present&&!strcmp(r->status,"published");
    }else{
        if(!strcmp(action,"request"))return r->visible&&(!r->present||
          (!strcmp(r->status,"archived")&&!strcmp(r->state,"withdrawn")&&!strcmp(r->role,"member")&&!r->authority[0]&&!r->expires[0]));
        if(!strcmp(action,"withdraw"))return r->present&&!strcmp(r->status,"published")&&
          !strcmp(r->state,"requested")&&!strcmp(r->role,"member");
    }
    return 0;
}
/* Preserve the legacy deterministic relation identity. Actor is server-derived. */
static void pj_relation_key(PjKind kind,const char *id,const char *actor,char out[288]) {
    char input[768],digest[65];
    int n=snprintf(input,sizeof(input),"%s|%s|%s",kind==PJ_COMMUNITY?"community-join":"occurrence-follow",id,actor);
    if(n<0||n>=(int)sizeof(input)){out[0]=0;return;}
    bf_sha256_hex((const unsigned char*)input,(size_t)n,digest);
    snprintf(out,288,"%s-%.24s",kind==PJ_COMMUNITY?"member":"follow",digest);
}
/* Millisecond CAS clock advances even when rapid opposite edits occur in one
 * wall-clock tick. It prevents an old preimage becoming valid again (ABA). */
static int pj_next_time(sqlite3 *db,const char *previous,char out[80]) {
    sqlite3_stmt *s=NULL;int ok=0;
    const char *sql="SELECT CASE WHEN ?1<>'' AND julianday(?1) IS NULL THEN NULL ELSE "
      "strftime('%Y-%m-%dT%H:%M:%fZ',CASE WHEN julianday(?1)>=julianday('now') "
      "THEN julianday(?1)+(0.002/86400.0) ELSE julianday('now') END) END";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,previous,-1,SQLITE_STATIC);
        if(sqlite3_step(s)==SQLITE_ROW)ok=pj_column(s,0,out,80)&&out[0]&&strcmp(out,previous);
    }
    sqlite3_finalize(s);return ok;
}
/* expected==NULL is the old creation-only form. A replay can observe an
 * accepted relation, but can never reset its state or revive a withdrawn one. */
static int pj_change(sqlite3 *db,PjKind kind,const char *actor,const char *id,
                     const char *action,const char *expected,PjRecord *out) {
    PjRecord current;sqlite3_stmt *s=NULL;int code,done=0,rc;char when[80],key[288];
    if(!db||!pj_actor(actor)||!pj_key(id,200)||!action||!out||
       (expected&&(strlen(expected)!=64||strspn(expected,"0123456789abcdef")!=64)))return 400;
    if((kind==PJ_OCCURRENCE&&strcmp(action,"follow")&&strcmp(action,"unfollow"))||
       (kind==PJ_COMMUNITY&&strcmp(action,"request")&&strcmp(action,"withdraw"))||!pj_kind_name(kind)[0])return 400;
    if(!sqlite3_get_autocommit(db)||sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return 503;
    code=pj_load(db,kind,actor,id,&current);if(code!=200)goto rollback;
    if(current.managed){code=409;goto rollback;}
    int enabling=!strcmp(action,"follow")||!strcmp(action,"request");
    if(enabling&&!current.visible){code=404;goto rollback;}
    if(current.present){
        if(kind==PJ_OCCURRENCE)done=(!strcmp(action,"follow")&&!strcmp(current.status,"published"))||
              (!strcmp(action,"unfollow")&&!strcmp(current.status,"archived"));
        else done=(!strcmp(action,"request")&&!strcmp(current.status,"published")&&
                    (!strcmp(current.state,"requested")||!strcmp(current.state,"active")))||
                  (!strcmp(action,"withdraw")&&!strcmp(current.status,"archived")&&!strcmp(current.state,"withdrawn"));
    }
    if(done){*out=current;goto commit;}
    if(expected){if(strcmp(expected,current.expected)){code=409;goto rollback;}}
    else if(current.present||!enabling){code=409;goto rollback;}
    if(!pj_action_offered(&current,action)){code=409;goto rollback;}
    if(!pj_next_time(db,current.updated,when)){code=503;goto rollback;}
    if(!current.present){
        pj_relation_key(kind,id,actor,key);if(!key[0]){code=503;goto rollback;}
        const char *sql=kind==PJ_COMMUNITY?
          "INSERT INTO community_membership(namespace,created_at,updated_at,published_at,status,membership,community,member_subject,role,state,joined_at,cannot_infer) "
          "VALUES('root',?1,?1,?1,'published',?2,?3,?4,'member','requested',?1,?5)":
          "INSERT INTO occurrence_follow(namespace,created_at,updated_at,published_at,status,follow_id,occurrence_id,follower,followed_at,cannot_infer) "
          "VALUES('root',?1,?1,?1,'published',?2,?3,?4,?1,?5)";
        if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){code=503;goto rollback;}
        sqlite3_bind_text(s,1,when,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,key,-1,SQLITE_STATIC);
        sqlite3_bind_text(s,3,id,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,actor,-1,SQLITE_STATIC);
        sqlite3_bind_text(s,5,kind==PJ_COMMUNITY?
          "A private local participation request, not acceptance, an external account membership or an authority grant.":
          "A private local follow, not an RSVP, registration, attendance, delivered notification or external subscription.",-1,SQLITE_STATIC);
    }else{
        const char *sql=kind==PJ_COMMUNITY?
          "UPDATE community_membership SET status=?1,state=?2,updated_at=?3,joined_at=CASE WHEN ?2='requested' THEN ?3 ELSE joined_at END "
          "WHERE id=?4 AND namespace='root' AND member_subject=?5 AND community=?6 AND membership=?7 AND COALESCE(updated_at,'')=?8":
          "UPDATE occurrence_follow SET status=?1,updated_at=?3,followed_at=CASE WHEN ?1='published' THEN ?3 ELSE followed_at END "
          "WHERE id=?4 AND namespace='root' AND follower=?5 AND occurrence_id=?6 AND follow_id=?7 AND COALESCE(updated_at,'')=?8";
        if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){code=503;goto rollback;}
        sqlite3_bind_text(s,1,enabling?"published":"archived",-1,SQLITE_STATIC);
        if(kind==PJ_COMMUNITY)sqlite3_bind_text(s,2,enabling?"requested":"withdrawn",-1,SQLITE_STATIC);
        sqlite3_bind_text(s,3,when,-1,SQLITE_STATIC);sqlite3_bind_int64(s,4,current.row);
        sqlite3_bind_text(s,5,actor,-1,SQLITE_STATIC);sqlite3_bind_text(s,6,id,-1,SQLITE_STATIC);
        sqlite3_bind_text(s,7,current.key,-1,SQLITE_STATIC);sqlite3_bind_text(s,8,current.updated,-1,SQLITE_STATIC);
    }
    rc=sqlite3_step(s);sqlite3_finalize(s);s=NULL;
    if(rc!=SQLITE_DONE){code=503;goto rollback;}
    if(sqlite3_changes(db)!=1){code=409;goto rollback;}
    code=pj_load(db,kind,actor,id,out);if(code!=200)goto rollback;
commit:
    if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)==SQLITE_OK)return 200;
    code=503;
rollback:
    sqlite3_finalize(s);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return code;
}
#endif
