#ifndef BONFYRE_ACCOUNT_INBOX_DATING_H
#define BONFYRE_ACCOUNT_INBOX_DATING_H
#include "../../BonfyreParticipation/src/dating.h"
/* A single account-scoped Inbox signal, not a second dating directory.
 * Read the active exact person binding from Auth. Native Participation
 * independently checks eligibility, consent revisions and cross-context
 * blocks before a pending introduction can appear. This projection carries
 * only a count; no peer ID, answer, message or match is sent to the rail. */
static int ai_dating_pending(const char *auth_db,long user){
 if(!auth_db||!*auth_db||user<1)return 0;
 sqlite3 *auth=NULL,*date=NULL;sqlite3_stmt *st=NULL;
 char actor[192]="";int rows=0,pending=0;
 if(sqlite3_open_v2(auth_db,&auth,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
 if(sqlite3_prepare_v2(auth,"SELECT actor_subject FROM actor_closure_bindings "
    "WHERE user_id=?1 AND state='active' LIMIT 2",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_int64(st,1,user);
 while(sqlite3_step(st)==SQLITE_ROW){
  const char *value=(const char*)sqlite3_column_text(st,0);
  if(++rows!=1||!bf_dating_subject(value)||strlen(value)>=sizeof(actor)){rows=0;break;}
  snprintf(actor,sizeof(actor),"%s",value);
 }
 sqlite3_finalize(st);st=NULL;
 if(rows!=1)goto done;
 const char *path=getenv("BONFYRE_PARTICIPATION_DB");
 if(!path||!*path)path=getenv("BONFYRE_DATING_DB");
 if(!path||*path!='/'||strlen(path)>4096)goto done;
 if(sqlite3_open_v2(path,&date,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
 sqlite3_busy_timeout(date,1200);
 BfDatingCandidate candidates[24];size_t count=0;
 if(!bf_dating_candidates(date,actor,candidates,24,&count))goto done;
 for(size_t i=0;i<count;i++)
  if(candidates[i].introduction_for_me&&!candidates[i].matched)pending++;
done:
 sqlite3_finalize(st);if(auth)sqlite3_close(auth);if(date)sqlite3_close(date);
 return pending;
}
#endif
