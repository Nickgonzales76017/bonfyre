#ifndef BONFYRE_ORGANIZATION_WORKSPACE_H
#define BONFYRE_ORGANIZATION_WORKSPACE_H
#include "bf_msgpack.h"
#include "bf_utf8.h"
#include <errno.h>
#include <limits.h>
/* Auth owns this explicit admission genesis, not a legal organization registry.
 * Request [1, session, operation, string_arguments]; response [1,status,message,rows,cursor].
 * A new realm is derived from the authenticated creator and creation nonce.
 * Existing named institutions can never be claimed through this operation. */
#define ORG_MAX_INPUT 8192
#define ORG_PAGE 50
static const char *organization_schema =
 "CREATE TABLE IF NOT EXISTS institution_origins("
 "institution TEXT PRIMARY KEY,creator INTEGER NOT NULL,request_digest TEXT NOT NULL,"
 "label TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(creator,request_digest),"
 "FOREIGN KEY(creator) REFERENCES users(id));"
 "CREATE TABLE IF NOT EXISTS institution_invitations("
 "digest TEXT PRIMARY KEY,institution TEXT NOT NULL,issuer INTEGER NOT NULL,email TEXT NOT NULL,"
 "role TEXT NOT NULL CHECK(role IN('read','write','admin')),created_at TEXT NOT NULL,expires_at TEXT NOT NULL,"
 "grant_expires_at TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','accepted','revoked')),"
 "revision INTEGER NOT NULL DEFAULT 1,accepted_by INTEGER,request_hours INTEGER NOT NULL,"
 "FOREIGN KEY(institution) REFERENCES institution_origins(institution));"
 "CREATE INDEX IF NOT EXISTS institution_invitations_scope ON institution_invitations(institution,digest);";
static int org_text(bf_mp_reader_t *r,char *out,size_t cap){
 bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR||v.v.raw.len>=cap)return 0;
 for(size_t i=0;i<v.v.raw.len;i++)if(v.v.raw.ptr[i]<32||v.v.raw.ptr[i]==127)return 0;
 if(!bf_utf8_valid((const char *)v.v.raw.ptr,v.v.raw.len,NULL))return 0;
 memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;return 1;
}
static int org_array(bf_mp_reader_t *r,unsigned count){bf_mp_value_t v;return bf_mp_read_next(r,&v)==BF_MP_ARRAY&&v.v.count==count;}
static int org_nonce(const char *s){return s&&strlen(s)==64&&strspn(s,"0123456789abcdef")==64;}
static int org_number(const char *s,long min,long max,long *out){char *end=NULL;errno=0;long n=strtol(s,&end,10);if(!*s||strspn(s,"0123456789")!=strlen(s)||errno||*end||n<min||n>max)return 0;*out=n;return 1;}
static void org_digest(const char *s,char out[65]){bf_sha256_hex((const uint8_t *)s,strlen(s),out);}
static int org_response(sqlite3 *db,int status,const char *message,bf_mp_writer_t *rows,unsigned count,const char *cursor){
 bf_mp_writer_t out;bf_mp_writer_init(&out,1024);
 int bad=bf_mp_pack_array(&out,5)||bf_mp_pack_uint(&out,1)||bf_mp_pack_uint(&out,(uint64_t)status)||bf_mp_pack_str_cstr(&out,message?message:"")||bf_mp_pack_array(&out,count);
 /* Copy already encoded complete row values only after allocation succeeds. */
 if(!bad&&rows&&rows->len){size_t total=out.len+rows->len;if(total>131072)bad=1;else{unsigned char *p=realloc(out.buf,total+64);if(!p)bad=1;else{out.buf=p;out.cap=total+64;memcpy(out.buf+out.len,rows->buf,rows->len);out.len=total;}}}
 if(!bad)bad=bf_mp_pack_str_cstr(&out,cursor?cursor:"");
 if(bad){sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);bf_mp_writer_free(&out);return 1;}
 if(status>=400)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
 else if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){bf_mp_writer_free(&out);return org_response(db,503,"The native transaction did not complete.",NULL,0,"");}
 int ok=fwrite(out.buf,1,out.len,stdout)==out.len&&fflush(stdout)==0;bf_mp_writer_free(&out);return ok&&status<400?0:1;
}
#define ORG_FAIL(code,text) return org_response(db,code,text,NULL,0,"")
static int org_exists(sqlite3 *db,const char *id,char label[161]){
 sqlite3_stmt *q=NULL;int ok=0;if(sqlite3_prepare_v2(db,"SELECT label FROM institution_origins WHERE institution=?",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);if(sqlite3_step(q)==SQLITE_ROW){const char *s=(const char *)sqlite3_column_text(q,0);if(s&&strlen(s)<161){strcpy(label,s);ok=sqlite3_step(q)==SQLITE_DONE;}}}sqlite3_finalize(q);return ok;
}
static int org_member_row(bf_mp_writer_t *w,const char *id,const char *label,const char *role,const char *expiry){return bf_mp_pack_array(w,4)||bf_mp_pack_str_cstr(w,id)||bf_mp_pack_str_cstr(w,label)||bf_mp_pack_str_cstr(w,role)||bf_mp_pack_str_cstr(w,expiry?expiry:"");}
static int org_create(sqlite3 *db,const char *token,int caller,const char *label,const char *nonce){
 if(!*label||strlen(label)>160||label[0]==' '||label[strlen(label)-1]==' '||!org_nonce(nonce))ORG_FAIL(400,"Choose an organization name and a fresh creation request.");
 if(!auth_authorized(db,token,"self")&&!auth_authorized(db,token,"write"))ORG_FAIL(403,"This session cannot create a workspace.");
 char request[65],source[160],digest[65],id[48],now[64];org_digest(nonce,request);snprintf(source,sizeof(source),"organization-workspace-v1:%d:%s",caller,nonce);org_digest(source,digest);snprintf(id,sizeof(id),"org-%.32s",digest);iso_now(now,sizeof(now));
 sqlite3_stmt *q=NULL;if(sqlite3_prepare_v2(db,"SELECT institution,label FROM institution_origins WHERE creator=? AND request_digest=?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"The organization owner is unavailable.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,request,-1,SQLITE_STATIC);int step=sqlite3_step(q),existing=step==SQLITE_ROW;
 if(existing){int matches=!strcmp((const char *)sqlite3_column_text(q,0),id)&&!strcmp((const char *)sqlite3_column_text(q,1),label);sqlite3_finalize(q);if(!matches)ORG_FAIL(409,"This creation request was already used for different details.");if(!auth_institution_authorized(db,token,id,"admin"))ORG_FAIL(403,"Replaying creation cannot restore removed administrator access.");}
 else {sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(503,"Could not inspect the creation request.");
  if(sqlite3_prepare_v2(db,"SELECT count(*) FROM institution_origins WHERE creator=?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not check workspace allowance.");sqlite3_bind_int(q,1,caller);step=sqlite3_step(q);int allowed=step==SQLITE_ROW&&sqlite3_column_int(q,0)<32;sqlite3_finalize(q);if(!allowed)ORG_FAIL(429,"The account's organization creation allowance has been reached.");
  if(sqlite3_prepare_v2(db,"SELECT 1 FROM institution_grants WHERE institution=? LIMIT 1",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not check the new destination.");sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE||institution_parent(id))ORG_FAIL(409,"The proposed destination cannot be newly allocated.");
  if(sqlite3_prepare_v2(db,"INSERT INTO institution_origins VALUES(?,?,?,?,?)",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not create the organization.");sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);sqlite3_bind_int(q,2,caller);sqlite3_bind_text(q,3,request,-1,SQLITE_STATIC);sqlite3_bind_text(q,4,label,-1,SQLITE_STATIC);sqlite3_bind_text(q,5,now,-1,SQLITE_STATIC);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(409,"The organization creation conflicted.");
  if(sqlite3_prepare_v2(db,"INSERT INTO institution_grants(user_id,institution,role,granted_at,active) VALUES(?,?,'admin',?,1)",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not establish initial workspace administration.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,id,-1,SQLITE_STATIC);sqlite3_bind_text(q,3,now,-1,SQLITE_STATIC);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(503,"Initial workspace administration was not saved.");
 }
 bf_mp_writer_t rows;bf_mp_writer_init(&rows,512);int bad=org_member_row(&rows,id,label,"admin","");int rc=bad?org_response(db,503,"Could not prepare the result.",NULL,0,""):org_response(db,existing?200:201,"Organization workspace ready. It is not a legal-entity verification or a payment account.",&rows,1,"");bf_mp_writer_free(&rows);return rc;
}
static int org_list(sqlite3 *db,int caller,const char *after){
 if(strlen(after)>96)ORG_FAIL(400,"Invalid organization cursor.");sqlite3_stmt *q=NULL;char now[64],cursor[97]="";iso_now(now,sizeof(now));
 const char *sql="SELECT g.institution,COALESCE(o.label,g.institution),g.role,COALESCE(g.expires_at,'') FROM institution_grants g LEFT JOIN institution_origins o ON o.institution=g.institution WHERE g.user_id=? AND g.active=1 AND (g.expires_at IS NULL OR g.expires_at='' OR g.expires_at>=?) AND g.institution>? ORDER BY g.institution LIMIT 51";
 if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not read current organizations.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,now,-1,SQLITE_STATIC);sqlite3_bind_text(q,3,after,-1,SQLITE_STATIC);
 bf_mp_writer_t rows;bf_mp_writer_init(&rows,2048);unsigned count=0;int bad=0,step,more=0;char last[97]="";
 while((step=sqlite3_step(q))==SQLITE_ROW){if(count==ORG_PAGE){more=1;break;}const char *id=(const char *)sqlite3_column_text(q,0);if(!id||strlen(id)>96){bad=1;break;}snprintf(last,sizeof(last),"%s",id);if(org_member_row(&rows,id,(const char *)sqlite3_column_text(q,1),(const char *)sqlite3_column_text(q,2),(const char *)sqlite3_column_text(q,3))){bad=1;break;}count++;}
 sqlite3_finalize(q);if(more)strcpy(cursor,last);int rc=bad||(!more&&step!=SQLITE_DONE)?org_response(db,503,"The organization listing was interrupted.",NULL,0,""):org_response(db,200,"",&rows,count,cursor);bf_mp_writer_free(&rows);return rc;
}
static int org_invite(sqlite3 *db,const char *token,int caller,char args[6][321]){
 const char *id=args[0],*email=args[1],*role=args[2],*secret=args[4];long hours;char label[161],expiry[64],grant_expiry[64],now[64],digest[65];
 if(!org_exists(db,id,label)||!auth_institution_authorized(db,token,id,"admin"))ORG_FAIL(403,"Current administration of this user-created organization is required.");
 if(!*email||!strchr(email,'@')||strpbrk(email," <>\"\\")||institution_role_rank(role)<0||!org_number(args[3],1,168,&hours)||!org_nonce(secret))ORG_FAIL(400,"Choose a recipient, access level and invitation expiry.");
 if(!member_admin_expiry(db,caller,id,grant_expiry,sizeof(grant_expiry)))ORG_FAIL(403,"Administrator access expired.");
 iso_now(now,sizeof(now));iso_future(expiry,sizeof(expiry),(int)hours);if(*grant_expiry&&strcmp(grant_expiry,expiry)<0)strcpy(expiry,grant_expiry);org_digest(secret,digest);
 sqlite3_stmt *q=NULL;if(sqlite3_prepare_v2(db,"SELECT institution,issuer,email,role,state,expires_at,request_hours FROM institution_invitations WHERE digest=?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not review invitations.");sqlite3_bind_text(q,1,digest,-1,SQLITE_STATIC);int step=sqlite3_step(q),replay=step==SQLITE_ROW;
 if(replay){int same=!strcmp((const char *)sqlite3_column_text(q,0),id)&&sqlite3_column_int(q,1)==caller&&sqlite3_column_int(q,6)==hours&&!strcmp((const char *)sqlite3_column_text(q,2),email)&&!strcmp((const char *)sqlite3_column_text(q,3),role)&&!strcmp((const char *)sqlite3_column_text(q,4),"pending")&&strcmp((const char *)sqlite3_column_text(q,5),now)>0;if(same)snprintf(expiry,sizeof(expiry),"%s",sqlite3_column_text(q,5));sqlite3_finalize(q);if(!same)ORG_FAIL(409,"This invitation request is no longer available. Prepare a new one.");}
 else{sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(503,"Could not review invitations.");
  if(sqlite3_prepare_v2(db,"SELECT 1 FROM institution_grants g JOIN users u ON u.id=g.user_id WHERE u.email=? AND g.institution=? AND g.active=1",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not review recipient access.");sqlite3_bind_text(q,1,email,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,id,-1,SQLITE_STATIC);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(409,"This account already has recorded access. Review its membership instead.");
  if(sqlite3_prepare_v2(db,"SELECT count(*) FROM institution_invitations WHERE institution=? AND state='pending' AND expires_at>?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not check invitation allowance.");sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,now,-1,SQLITE_STATIC);int allowed=sqlite3_step(q)==SQLITE_ROW&&sqlite3_column_int(q,0)<100;sqlite3_finalize(q);if(!allowed)ORG_FAIL(429,"Review outstanding invitations before creating more.");
  if(sqlite3_prepare_v2(db,"INSERT INTO institution_invitations(digest,institution,issuer,email,role,created_at,expires_at,grant_expires_at,state,request_hours) VALUES(?,?,?,?,?,?,?,?,'pending',?)",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not save the invitation.");sqlite3_bind_text(q,1,digest,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,id,-1,SQLITE_STATIC);sqlite3_bind_int(q,3,caller);sqlite3_bind_text(q,4,email,-1,SQLITE_STATIC);sqlite3_bind_text(q,5,role,-1,SQLITE_STATIC);sqlite3_bind_text(q,6,now,-1,SQLITE_STATIC);sqlite3_bind_text(q,7,expiry,-1,SQLITE_STATIC);sqlite3_bind_text(q,8,grant_expiry,-1,SQLITE_STATIC);sqlite3_bind_int(q,9,(int)hours);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(409,"Invitation changed before it could be saved.");
 }
 bf_mp_writer_t rows;bf_mp_writer_init(&rows,512);int bad=org_member_row(&rows,id,label,role,expiry)||org_member_row(&rows,digest,email,secret,grant_expiry);int rc=bad?org_response(db,503,"Could not prepare the invitation result.",NULL,0,""):org_response(db,replay?200:201,"Share this invitation with its intended recipient. No email has been sent.",&rows,2,"");bf_mp_writer_free(&rows);return rc;
}
static int org_join(sqlite3 *db,int caller,const char *secret,int accept){
 if(!org_nonce(secret))ORG_FAIL(400,"Invalid invitation.");char digest[65],id[97],email[321],role[8],expiry[64],cap[64],now[64],label[161];int issuer,accepted_by,state=0;org_digest(secret,digest);iso_now(now,sizeof(now));sqlite3_stmt *q=NULL;
 if(sqlite3_prepare_v2(db,"SELECT i.institution,i.email,i.role,i.expires_at,i.grant_expires_at,i.issuer,i.state,COALESCE(i.accepted_by,0) FROM institution_invitations i JOIN users u ON u.email=i.email AND u.id=? WHERE i.digest=?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not read the invitation.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,digest,-1,SQLITE_STATIC);if(sqlite3_step(q)!=SQLITE_ROW){sqlite3_finalize(q);ORG_FAIL(403,"Use the intended account and the complete invitation link.");}
 snprintf(id,sizeof(id),"%s",sqlite3_column_text(q,0));snprintf(email,sizeof(email),"%s",sqlite3_column_text(q,1));snprintf(role,sizeof(role),"%s",sqlite3_column_text(q,2));snprintf(expiry,sizeof(expiry),"%s",sqlite3_column_text(q,3));snprintf(cap,sizeof(cap),"%s",sqlite3_column_text(q,4));issuer=sqlite3_column_int(q,5);const char *status=(const char *)sqlite3_column_text(q,6);state=!strcmp(status,"pending")?1:!strcmp(status,"accepted")?2:0;accepted_by=sqlite3_column_int(q,7);sqlite3_finalize(q);
 if(!state||!org_exists(db,id,label))ORG_FAIL(403,"This invitation is unavailable.");
 if(state==1){char current_expiry[64];if(strcmp(now,expiry)>=0||!member_admin_expiry(db,issuer,id,current_expiry,sizeof(current_expiry)))ORG_FAIL(403,"The invitation expired or its issuer no longer has administrator access.");
  if(*current_expiry&&(!*cap||strcmp(current_expiry,cap)<0))snprintf(cap,sizeof(cap),"%s",current_expiry);
  if(sqlite3_prepare_v2(db,"SELECT active FROM users WHERE id=?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not check the invitation issuer.");sqlite3_bind_int(q,1,issuer);int active=sqlite3_step(q)==SQLITE_ROW&&sqlite3_column_int(q,0);sqlite3_finalize(q);if(!active)ORG_FAIL(403,"The invitation issuer is no longer active.");
 }
 if(sqlite3_prepare_v2(db,"SELECT role,active,COALESCE(expires_at,'') FROM institution_grants WHERE user_id=? AND institution=?",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not check your current access.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,id,-1,SQLITE_STATIC);int step=sqlite3_step(q),has=step==SQLITE_ROW,live=has&&sqlite3_column_int(q,1)&&(!*sqlite3_column_text(q,2)||strcmp((const char *)sqlite3_column_text(q,2),now)>0),same=live&&!strcmp((const char *)sqlite3_column_text(q,0),role);sqlite3_finalize(q);
 if(step!=SQLITE_DONE&&!has)ORG_FAIL(503,"Could not inspect current membership.");if(state==2&&(accepted_by!=caller||!same))ORG_FAIL(409,"The invitation was already used. It cannot restore or change membership.");if(state==1&&has)ORG_FAIL(409,"Your organization access changed. Ask an administrator to review it directly.");
 if(accept&&state==1){
  if(sqlite3_prepare_v2(db,"INSERT INTO institution_grants(user_id,institution,role,granted_at,expires_at,active) VALUES(?,?,?,?,?,1)",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not accept membership.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,id,-1,SQLITE_STATIC);sqlite3_bind_text(q,3,role,-1,SQLITE_STATIC);sqlite3_bind_text(q,4,now,-1,SQLITE_STATIC);if(*cap)sqlite3_bind_text(q,5,cap,-1,SQLITE_STATIC);else sqlite3_bind_null(q,5);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE)ORG_FAIL(409,"Membership changed while accepting.");
  if(sqlite3_prepare_v2(db,"UPDATE institution_invitations SET state='accepted',revision=revision+1,accepted_by=? WHERE digest=? AND state='pending'",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not retain invitation acceptance.");sqlite3_bind_int(q,1,caller);sqlite3_bind_text(q,2,digest,-1,SQLITE_STATIC);step=sqlite3_step(q);sqlite3_finalize(q);if(step!=SQLITE_DONE||sqlite3_changes(db)!=1)ORG_FAIL(409,"Invitation changed during acceptance.");
 }
 bf_mp_writer_t rows;bf_mp_writer_init(&rows,512);int bad=org_member_row(&rows,id,label,role,cap);int rc=bad?org_response(db,503,"Could not prepare membership result.",NULL,0,""):org_response(db,200,accept?"Invitation accepted. Your personal work has not been shared.":"Review this organization and access level before accepting.",&rows,1,"");bf_mp_writer_free(&rows);return rc;
}
static int org_invitations(sqlite3 *db,const char *token,const char *id,const char *after){
 char label[161];if(!org_exists(db,id,label)||!auth_institution_authorized(db,token,id,"admin"))ORG_FAIL(403,"Administrator access to this organization is required.");if(*after&&!org_nonce(after))ORG_FAIL(400,"Invalid invitation cursor.");sqlite3_stmt *q=NULL;
 if(sqlite3_prepare_v2(db,"SELECT digest,email,role,state,expires_at,revision FROM institution_invitations WHERE institution=? AND digest>? ORDER BY digest LIMIT 51",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not read invitations.");sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,after,-1,SQLITE_STATIC);
 bf_mp_writer_t rows;bf_mp_writer_init(&rows,2048);unsigned count=0;int step,bad=0,more=0;char cursor[65]="",last[65]="";
 while((step=sqlite3_step(q))==SQLITE_ROW){if(count==ORG_PAGE){more=1;break;}snprintf(last,sizeof(last),"%s",sqlite3_column_text(q,0));bad=bf_mp_pack_array(&rows,6);for(int i=0;i<5&&!bad;i++)bad=bf_mp_pack_str_cstr(&rows,(const char *)sqlite3_column_text(q,i));if(!bad)bad=bf_mp_pack_uint(&rows,(uint64_t)sqlite3_column_int64(q,5));if(bad)break;count++;}sqlite3_finalize(q);if(more)strcpy(cursor,last);int rc=bad||(!more&&step!=SQLITE_DONE)?org_response(db,503,"Invitation listing interrupted.",NULL,0,""):org_response(db,200,"",&rows,count,cursor);bf_mp_writer_free(&rows);return rc;
}
static int org_revoke(sqlite3 *db,const char *token,char args[6][321]){
 char label[161];long revision;if(!org_exists(db,args[0],label)||!auth_institution_authorized(db,token,args[0],"admin"))ORG_FAIL(403,"Administrator access is required.");if(!org_nonce(args[1])||!org_number(args[2],1,INT_MAX-1,&revision))ORG_FAIL(400,"Invalid reviewed invitation.");sqlite3_stmt *q=NULL;
 if(sqlite3_prepare_v2(db,"UPDATE institution_invitations SET state='revoked',revision=revision+1 WHERE institution=? AND digest=? AND revision=? AND state='pending'",-1,&q,NULL)!=SQLITE_OK)ORG_FAIL(503,"Could not revoke the invitation.");sqlite3_bind_text(q,1,args[0],-1,SQLITE_STATIC);sqlite3_bind_text(q,2,args[1],-1,SQLITE_STATIC);sqlite3_bind_int64(q,3,revision);int step=sqlite3_step(q),changed=sqlite3_changes(db);sqlite3_finalize(q);if(step!=SQLITE_DONE||changed!=1)ORG_FAIL(409,"The invitation changed. Review its current state.");return org_response(db,200,"Invitation revoked. Existing memberships are managed separately.",NULL,0,"");
}
static int cmd_organization_workspace(sqlite3 *db){
 unsigned char input[ORG_MAX_INPUT+1];size_t bytes=fread(input,1,sizeof(input),stdin);if(ferror(stdin)||bytes>ORG_MAX_INPUT)ORG_FAIL(413,"Organization request is too large.");
 bf_mp_reader_t r;bf_mp_reader_init(&r,input,bytes);bf_mp_value_t v;char token[MAX_TOKEN],operation[24],args[6][321];memset(args,0,sizeof(args));
 if(!org_array(&r,4)||bf_mp_read_next(&r,&v)!=BF_MP_UINT||v.v.u!=1||!org_text(&r,token,sizeof(token))||!org_text(&r,operation,sizeof(operation))||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count>6)ORG_FAIL(400,"Invalid organization request.");unsigned count=v.v.count;
 for(unsigned i=0;i<count;i++)if(!org_text(&r,args[i],sizeof(args[i])))ORG_FAIL(400,"Invalid organization field.");if(r.pos!=r.len)ORG_FAIL(400,"Unexpected organization request data.");
 int write=!strcmp(operation,"create")||!strcmp(operation,"invite")||!strcmp(operation,"accept")||!strcmp(operation,"revoke"),caller=0;
 sqlite3_busy_timeout(db,3000);if(sqlite3_exec(db,write?"BEGIN IMMEDIATE":"BEGIN",NULL,NULL,NULL)!=SQLITE_OK)ORG_FAIL(503,"Another operation is updating access. Retry the same reviewed request.");
 if(!session_user_id(db,token,&caller))ORG_FAIL(401,"Sign in to manage organizations.");
 if(write&&!auth_authorized(db,token,"self")&&!auth_authorized(db,token,"write"))ORG_FAIL(403,"This session cannot change organizations.");
 if(!strcmp(operation,"create")&&count==2)return org_create(db,token,caller,args[0],args[1]);
 if(!strcmp(operation,"list")&&count==1)return org_list(db,caller,args[0]);
 if(!strcmp(operation,"invite")&&count==5)return org_invite(db,token,caller,args);
 if((!strcmp(operation,"review")||!strcmp(operation,"accept"))&&count==1)return org_join(db,caller,args[0],write);
 if(!strcmp(operation,"invitations")&&count==2)return org_invitations(db,token,args[0],args[1]);
 if(!strcmp(operation,"revoke")&&count==3)return org_revoke(db,token,args);
 ORG_FAIL(400,"Choose a supported organization operation with its exact fields.");
}
#undef ORG_FAIL
#endif
