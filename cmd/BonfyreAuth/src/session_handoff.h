#ifndef BONFYRE_SESSION_HANDOFF_H
#define BONFYRE_SESSION_HANDOFF_H
/* Short-lived, one-use cross-territory login transfer owned by BonfyreAuth.
 * Existing browser sessions never appear in redirects. */
static int handoff_hex(const char *s){if(!s||strlen(s)!=64)return 0;for(int i=0;i<64;i++)if(!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f')))return 0;return 1;}
static int handoff_dest(const char *s){return s&&(!strcmp(s,"life")||!strcmp(s,"live")||!strcmp(s,"shop")||!strcmp(s,"work"));}
static void handoff_hash(const char *s,char out[65]){uint8_t digest[32];sha256(s,strlen(s),digest);hex_str(digest,32,out);}
static void handoff_expires(char out[32],int seconds){time_t t=time(NULL)+seconds;struct tm tm;gmtime_r(&t,&tm);strftime(out,32,"%Y-%m-%dT%H:%M:%SZ",&tm);}
static int handoff_read_code(char out[65]){char line[128]={0};if(!fgets(line,sizeof(line),stdin))return 0;size_t n=strcspn(line,"\r\n");if(n!=64)return 0;line[n]=0;if(!handoff_hex(line))return 0;memcpy(out,line,65);return 1;}
static int cmd_handoff_issue(sqlite3 *db,const char *dest,const char *state){
 sqlite3_stmt *st=NULL;char bearer[MAX_TOKEN]={0},code[65]={0},code_hash[65],state_hash[65];
 char now[64],expires[32];uint8_t entropy[32];int origin=0,ok=0;
 if(!handoff_dest(dest)||!handoff_hex(state)||stdin_token(bearer,sizeof(bearer)))return 1;
 iso_now(now,sizeof(now));handoff_expires(expires,90);
 if(sqlite3_prepare_v2(db,"SELECT s.id FROM sessions s JOIN users u ON u.id=s.user_id "
    "WHERE s.token=?1 AND s.active=1 AND u.active=1 AND s.expires_at>?2 LIMIT 1",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(st,1,bearer,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,now,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)==SQLITE_ROW)origin=sqlite3_column_int(st,0);sqlite3_finalize(st);st=NULL;memset(bearer,0,sizeof(bearer));
 if(origin<1||!secure_random(entropy,sizeof(entropy)))goto done;
 hex_str(entropy,32,code);memset(entropy,0,sizeof(entropy));
 handoff_hash(code,code_hash);handoff_hash(state,state_hash);
 if(sqlite3_prepare_v2(db,"INSERT INTO session_handoffs (code_hash,origin_session,destination,state_hash,created_at,expires_at) VALUES(?1,?2,?3,?4,?5,?6)",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(st,1,code_hash,-1,SQLITE_TRANSIENT);sqlite3_bind_int(st,2,origin);sqlite3_bind_text(st,3,dest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,4,state_hash,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,5,now,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,6,expires,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)==SQLITE_DONE)ok=1;
done:
 if(st)sqlite3_finalize(st);
 if(ok)printf("{\"code\":\"%s\",\"expires_in\":90}\n",code);
 memset(code,0,sizeof(code));return ok?0:1;
}
static int cmd_handoff_consume(sqlite3 *db,const char *dest,const char *state){
 sqlite3_stmt *st=NULL;char code[65]={0},code_hash[65],state_hash[65];
 char now[64],expiry[64],token[MAX_TOKEN]={0},scope[24]={0};int uid=0,ok=0;
 if(!handoff_dest(dest)||!handoff_hex(state)||!handoff_read_code(code))return 1;
 handoff_hash(code,code_hash);handoff_hash(state,state_hash);memset(code,0,sizeof(code));
 iso_now(now,sizeof(now));iso_future(expiry,sizeof(expiry),SESSION_HOURS);
 if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return 1;
 if(sqlite3_prepare_v2(db,"SELECT s.user_id,s.scope,s.expires_at FROM session_handoffs h "
   "JOIN sessions s ON s.id=h.origin_session JOIN users u ON u.id=s.user_id "
   "WHERE h.code_hash=?1 AND h.destination=?2 AND h.state_hash=?3 "
   "AND h.consumed_at IS NULL AND h.expires_at>?4 AND s.active=1 AND s.expires_at>?4 AND u.active=1 LIMIT 1",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(st,1,code_hash,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,dest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,3,state_hash,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,4,now,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)==SQLITE_ROW){uid=sqlite3_column_int(st,0);const char *v=(const char*)sqlite3_column_text(st,1);if(v&&strlen(v)<sizeof(scope))snprintf(scope,sizeof(scope),"%s",v);const char *until=(const char*)sqlite3_column_text(st,2);if(!until||strlen(until)>=sizeof(expiry))uid=0;else if(strcmp(until,expiry)<0)snprintf(expiry,sizeof(expiry),"%s",until);}
 sqlite3_finalize(st);st=NULL;if(uid<1||!scope[0])goto done;
 if(sqlite3_prepare_v2(db,"UPDATE session_handoffs SET consumed_at=?2 WHERE code_hash=?1 AND consumed_at IS NULL",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_text(st,1,code_hash,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,now,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)!=SQLITE_DONE||sqlite3_changes(db)!=1)goto done;sqlite3_finalize(st);st=NULL;
 generate_token(token,sizeof(token));
 if(sqlite3_prepare_v2(db,"INSERT INTO sessions (user_id,token,created_at,expires_at,scope) VALUES(?1,?2,?3,?4,?5)",-1,&st,NULL)!=SQLITE_OK)goto done;
 sqlite3_bind_int(st,1,uid);sqlite3_bind_text(st,2,token,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,3,now,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,4,expiry,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,5,scope,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(st)!=SQLITE_DONE)goto done;sqlite3_finalize(st);st=NULL;
 if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto done;ok=1;
done:
 if(st)sqlite3_finalize(st);if(!ok)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
 if(ok)printf("{\"session\":{\"token\":\"%s\",\"expires_at\":\"%s\",\"scope\":\"%s\"},\"handoff_consumed\":true}\n",token,expiry,scope);
 memset(token,0,sizeof(token));return ok?0:1;
}
#endif
