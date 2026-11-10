#ifndef BONFYRE_HOUSEHOLD_ACCESS_H
#define BONFYRE_HOUSEHOLD_ACCESS_H
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
/* Read projection of CMS household authority. A similarly named institution
 * grant is never a household grant. No names, cookies or caller roles are trusted. */
typedef struct { sqlite3_int64 id, epoch; char steward[64], role[8]; } BfHouseholdAccess;
static int bhh_slug(const char *id) {
    return id && *id && strlen(id)<=63 && strspn(id,"abcdefghijklmnopqrstuvwxyz0123456789-")==strlen(id);
}
static int bhh_actor(const char *actor) {
    if(!actor || strncmp(actor,"user:",5) || !actor[5] || strlen(actor)>15 || actor[5]=='0' || strspn(actor+5,"0123456789")!=strlen(actor+5)) return 0;
    return strtoll(actor+5,NULL,10)<=INT_MAX;
}
static int bhh_rank(const char *role) {
    return !role?-1:!strcmp(role,"none")?0:!strcmp(role,"read")?1:!strcmp(role,"write")?2:!strcmp(role,"admin")?3:-1;
}
static int bhh_context(sqlite3 *db,const char *id,const char *actor,BfHouseholdAccess *out) {
    sqlite3_stmt *s=NULL;int valid=0;
    memset(out,0,sizeof(*out));
    if(!db || !bhh_slug(id) || !bhh_actor(actor))return 0;
    if(sqlite3_prepare_v2(db,"SELECT id,steward FROM household WHERE namespace='root' AND household_id=?1 AND status='published'",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    const char *steward=(const char *)sqlite3_column_text(s,1);
    if(!bhh_actor(steward))goto done;
    out->id=sqlite3_column_int64(s,0);snprintf(out->steward,sizeof(out->steward),"%s",steward);
    if(sqlite3_step(s)!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;valid=1;
    if(sqlite3_prepare_v2(db,"SELECT epoch FROM _household_authority WHERE household_row=?1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_int64(s,1,out->id);if(sqlite3_step(s)==SQLITE_ROW)out->epoch=sqlite3_column_int64(s,0);
    }
    sqlite3_finalize(s);s=NULL;
    if(!strcmp(actor,out->steward)){snprintf(out->role,sizeof(out->role),"admin");goto done;}
    if(!out->epoch)goto done;
    if(sqlite3_prepare_v2(db,"SELECT role FROM _household_membership WHERE household_row=?1 AND member=?2 AND authority_epoch=?3 AND (expires_at=0 OR expires_at>CAST(strftime('%s','now') AS INTEGER))",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_int64(s,1,out->id);sqlite3_bind_text(s,2,actor,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,out->epoch);
    if(sqlite3_step(s)==SQLITE_ROW){const char *role=(const char *)sqlite3_column_text(s,0);if(bhh_rank(role)>0)snprintf(out->role,sizeof(out->role),"%s",role);}
 done:sqlite3_finalize(s);return valid;
}
#endif
