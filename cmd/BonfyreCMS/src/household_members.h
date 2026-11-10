#ifndef BONFYRE_CMS_HOUSEHOLD_MEMBERS_H
#define BONFYRE_CMS_HOUSEHOLD_MEMBERS_H
#include "household_access.h"
#include <time.h>
/* Membership state belongs to the same CMS transaction as the household.
 * The HTTP boundary supplies an authenticated user principal. Local CLI actor
 * arguments are not authentication and must never be exposed as browser input. */
static int chh_schema(sqlite3 *db) {
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS _household_authority(household_row INTEGER PRIMARY KEY,epoch INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS _household_membership(household_row INTEGER NOT NULL,member TEXT NOT NULL,role TEXT NOT NULL,revision INTEGER NOT NULL,authority_epoch INTEGER NOT NULL,expires_at INTEGER NOT NULL,granted_by TEXT NOT NULL,PRIMARY KEY(household_row,member));"
        "INSERT OR IGNORE INTO _household_authority SELECT id,1 FROM household;"
        "CREATE TRIGGER IF NOT EXISTS _household_authority_changed AFTER UPDATE OF steward,status,household_id,namespace ON household WHEN OLD.steward IS NOT NEW.steward OR OLD.status IS NOT NEW.status OR OLD.household_id IS NOT NEW.household_id OR OLD.namespace IS NOT NEW.namespace BEGIN INSERT INTO _household_authority VALUES(NEW.id,1) ON CONFLICT(household_row) DO UPDATE SET epoch=epoch+1; END;"
        "CREATE TRIGGER IF NOT EXISTS _household_authority_deleted AFTER DELETE ON household BEGIN UPDATE _household_authority SET epoch=epoch+1 WHERE household_row=OLD.id; END;"
        "CREATE TRIGGER IF NOT EXISTS _household_authority_inserted AFTER INSERT ON household BEGIN INSERT INTO _household_authority VALUES(NEW.id,1) ON CONFLICT(household_row) DO UPDATE SET epoch=epoch+1; END;",
        NULL,NULL,NULL)==SQLITE_OK;
}
static int chh_members(sqlite3 *db,const char *operation,const char *household,const char *actor,
                       const char *member,const char *role,long long expected,long long epoch,long long expires,int after) {
    int writing=operation&&!strcmp(operation,"set"),rc=3,begun=0,step;sqlite3_stmt *s=NULL;BfHouseholdAccess a;
    if(!operation||(!writing&&strcmp(operation,"list"))||!bhh_slug(household)||!bhh_actor(actor)||expected<0||expected>=INT_MAX||epoch<0||after<0||expires<0)goto done;
    if(sqlite3_exec(db,writing?"BEGIN IMMEDIATE":"BEGIN",NULL,NULL,NULL)!=SQLITE_OK){rc=6;goto done;}begun=1;
    if(!bhh_context(db,household,actor,&a)||strcmp(actor,a.steward)){rc=4;goto done;}
    if(!writing){
        printf("schema\tbonfyre.household.members.v1\nhousehold\t%s\nepoch\t%lld\nsteward\t%s\n",household,(long long)a.epoch,a.steward);
        if(!a.epoch){puts("next\t0");rc=0;goto done;}
        if(sqlite3_prepare_v2(db,"SELECT member,role,revision,expires_at,authority_epoch=?2 AND (expires_at=0 OR expires_at>CAST(strftime('%s','now') AS INTEGER)) FROM _household_membership WHERE household_row=?1 AND CAST(substr(member,6) AS INTEGER)>?3 ORDER BY CAST(substr(member,6) AS INTEGER) LIMIT 51",-1,&s,NULL)!=SQLITE_OK){rc=6;goto done;}
        sqlite3_bind_int64(s,1,a.id);sqlite3_bind_int64(s,2,a.epoch);sqlite3_bind_int(s,3,after);int count=0,last=0,more=0;
        while((step=sqlite3_step(s))==SQLITE_ROW){
            if(count==50){more=1;break;}const char *m=(const char *)sqlite3_column_text(s,0),*r=(const char *)sqlite3_column_text(s,1);
            if(!bhh_actor(m)||bhh_rank(r)<0){rc=6;goto done;}
            printf("member\t%s\t%s\t%lld\t%lld\t%d\n",m,r,(long long)sqlite3_column_int64(s,2),(long long)sqlite3_column_int64(s,3),sqlite3_column_int(s,4)&&bhh_rank(r)>0);
            last=atoi(m+5);count++;
        }
        if(step!=SQLITE_DONE&&!more){rc=6;goto done;}printf("next\t%d\n",more?last:0);rc=0;goto done;
    }
    if(!bhh_actor(member)||!strcmp(member,actor)||bhh_rank(role)<0)goto done;
    if(a.epoch!=epoch){rc=4;goto done;}
    if(strcmp(role,"none") && expires && expires<=(long long)time(NULL))goto done;
    if(!chh_schema(db)||!bhh_context(db,household,actor,&a)){rc=6;goto done;}
    if(sqlite3_prepare_v2(db,"SELECT revision FROM _household_membership WHERE household_row=?1 AND member=?2",-1,&s,NULL)!=SQLITE_OK){rc=6;goto done;}
    sqlite3_bind_int64(s,1,a.id);sqlite3_bind_text(s,2,member,-1,SQLITE_STATIC);step=sqlite3_step(s);
    long long current=step==SQLITE_ROW?sqlite3_column_int64(s,0):0;
    if(step!=SQLITE_ROW&&step!=SQLITE_DONE){rc=6;goto done;}
    sqlite3_finalize(s);s=NULL;if(current!=expected||current>=INT_MAX-1){rc=4;goto done;}
    if(sqlite3_prepare_v2(db,"INSERT INTO _household_membership VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(household_row,member) DO UPDATE SET role=excluded.role,revision=excluded.revision,authority_epoch=excluded.authority_epoch,expires_at=excluded.expires_at,granted_by=excluded.granted_by",-1,&s,NULL)!=SQLITE_OK){rc=6;goto done;}
    sqlite3_bind_int64(s,1,a.id);sqlite3_bind_text(s,2,member,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,role,-1,SQLITE_STATIC);sqlite3_bind_int64(s,4,current+1);sqlite3_bind_int64(s,5,a.epoch);sqlite3_bind_int64(s,6,expires);sqlite3_bind_text(s,7,actor,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_DONE){rc=6;goto done;}sqlite3_finalize(s);s=NULL;
    if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){rc=6;goto done;}begun=0;
    printf("schema\tbonfyre.household.members.v1\nhousehold\t%s\nepoch\t%lld\nmember\t%s\t%s\t%lld\n",household,(long long)a.epoch,member,role,current+1);rc=0;
 done:sqlite3_finalize(s);if(begun)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    if(rc)fputs(rc==4?"Household authority or the reviewed membership changed. Refresh before changing access.\n":rc==3?"Choose a valid existing member, access level and reviewed revision.\n":"The household owner could not confirm this operation.\n",stderr);
    return rc;
}
#endif
