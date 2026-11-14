#ifndef BONFYRE_COMMUNITY_READ_H
#define BONFYRE_COMMUNITY_READ_H
/* Shared projections of the existing CMS community records. No shadow store. */
#define CJR_POLICY "aurekai-member-conversation-v1"
/* Canonical public-root predicate for discovery, readers, export and XML.
 * A visible post must be a real member conversation and have a routable URL. */
#define CJR_PUBLIC_DISCUSSION_FROM \
 "FROM community_post p JOIN community_body c ON c.namespace='root' " \
 "AND c.community=p.community WHERE p.namespace='root' " \
 "AND p.status='published' AND p.visibility='public' " \
 "AND julianday(p.published_at) IS NOT NULL AND julianday(p.published_at)<=julianday('now') " \
 "AND p.moderation_state IN ('accepted','visible','published') " \
 "AND COALESCE(p.reply_to,'')='' " \
 "AND p.post NOT IN ('.','..') AND p.community NOT IN ('.','..') " \
 "AND length(p.post) BETWEEN 1 AND 96 AND instr(p.post,'.')=0 " \
 "AND p.post NOT GLOB '*[^A-Za-z0-9._:-]*' " \
 "AND length(p.community) BETWEEN 1 AND 96 AND instr(p.community,'..')=0 " \
 "AND p.community NOT GLOB '*[^A-Za-z0-9._:-]*' " \
 "AND length(p.author_subject) BETWEEN 6 AND 23 " \
 "AND p.author_subject GLOB 'user:[1-9]*' AND substr(p.author_subject,6) NOT GLOB '*[^0-9]*' " \
 "AND c.status='published' AND c.visibility='public' " \
 "AND length(c.created_by_subject) BETWEEN 6 AND 23 " \
 "AND c.created_by_subject GLOB 'user:[1-9]*' AND substr(c.created_by_subject,6) NOT GLOB '*[^0-9]*' " \
 "AND c.moderation_policy='" CJR_POLICY "'"
typedef struct {long long id;char key[97],title[161],description[6001],owner[64],status[24],visibility[24],policy[64],digest[65];} CjrCommunity;
typedef struct {long long id;char key[97],actor[64],role[32],state[32],status[24],expires[64],digest[65];int active;} CjrMember;
typedef struct {long long id;char key[97],author[64],parent[97],title[161],body[6001],status[24],moderation[24],visibility[24],at[64],digest[65];} CjrPost;
static int cjr_key(const char *s){return s&&*s&&strlen(s)<97&&strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-")==strlen(s);}
static int cjr_actor(const char *s){return s&&!strncmp(s,"user:",5)&&s[5]>='1'&&s[5]<='9'&&strlen(s)<24&&strspn(s+5,"0123456789")==strlen(s+5);}
static int cjr_column(sqlite3_stmt *s,int i,char *out,size_t cap){
 const unsigned char *v=sqlite3_column_text(s,i);int n=sqlite3_column_bytes(s,i);
 if(!v||n<0||(size_t)n>=cap||memchr(v,0,(size_t)n))return 0;
 memcpy(out,v,(size_t)n);out[n]=0;return 1;
}
static void cjr_digest(long long id,const char *const *parts,size_t count,char out[65]){
 sqlite3_str *s=sqlite3_str_new(NULL);sqlite3_str_appendf(s,"%lld;",id);
 for(size_t i=0;i<count;i++)sqlite3_str_appendf(s,"%llu:%s",(unsigned long long)strlen(parts[i]),parts[i]);
 char *text=sqlite3_str_finish(s);out[0]=0;if(text){bf_sha256_hex((const uint8_t*)text,strlen(text),out);sqlite3_free(text);}
}
/* -1 is a failed/ambiguous read, 0 absent or owned elsewhere, 1 exact record. */
static int cjr_community(sqlite3 *db,const char *key,CjrCommunity *out){
 sqlite3_stmt *s=NULL;int found=0;memset(out,0,sizeof(*out));if(!cjr_key(key))return 0;
 const char *sql="SELECT id,community,title,COALESCE(description,''),COALESCE(created_by_subject,''),status,visibility,COALESCE(moderation_policy,'') FROM community_body WHERE namespace='root' AND community=?1 LIMIT 2";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,key,-1,SQLITE_STATIC);int step=sqlite3_step(s);
 if(step==SQLITE_ROW){out->id=sqlite3_column_int64(s,0);char *fields[]={out->key,out->title,out->description,out->owner,out->status,out->visibility,out->policy};size_t caps[]={sizeof(out->key),sizeof(out->title),sizeof(out->description),sizeof(out->owner),sizeof(out->status),sizeof(out->visibility),sizeof(out->policy)};found=1;
  for(int i=0;i<7;i++)if(!cjr_column(s,i+1,fields[i],caps[i]))found=-1;
  if(sqlite3_step(s)!=SQLITE_DONE)found=-1;
  if(found==1){const char *parts[]={out->key,out->title,out->description,out->owner,out->status,out->visibility,out->policy};cjr_digest(out->id,parts,7,out->digest);if(!*out->digest)found=-1;}
 }else if(step!=SQLITE_DONE)found=-1;sqlite3_finalize(s);return found;
}
static int cjr_local(const CjrCommunity *c){return c->id>0&&!strcmp(c->policy,CJR_POLICY)&&cjr_actor(c->owner);}
static int cjr_public(const CjrCommunity *c){return cjr_local(c)&&!strcmp(c->status,"published")&&!strcmp(c->visibility,"public");}
static int cjr_member(sqlite3 *db,const char *community,const char *actor,CjrMember *out){
 sqlite3_stmt *s=NULL;int found=0;memset(out,0,sizeof(*out));snprintf(out->digest,sizeof(out->digest),"absent");if(!cjr_actor(actor))return 0;
 const char *sql="SELECT id,membership,member_subject,role,state,status,COALESCE(expires_at,''),CASE WHEN status='published' AND state='active' AND (COALESCE(expires_at,'')='' OR julianday(expires_at)>julianday('now')) THEN 1 ELSE 0 END FROM community_membership WHERE namespace='root' AND community=?1 AND member_subject=?2 LIMIT 2";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,community,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,actor,-1,SQLITE_STATIC);int step=sqlite3_step(s);
 if(step==SQLITE_ROW){out->id=sqlite3_column_int64(s,0);char *fields[]={out->key,out->actor,out->role,out->state,out->status,out->expires};size_t caps[]={sizeof(out->key),sizeof(out->actor),sizeof(out->role),sizeof(out->state),sizeof(out->status),sizeof(out->expires)};found=1;
  for(int i=0;i<6;i++)if(!cjr_column(s,i+1,fields[i],caps[i]))found=-1;out->active=sqlite3_column_int(s,7);
  if(sqlite3_step(s)!=SQLITE_DONE)found=-1;
  if(found==1){const char *parts[]={out->key,out->actor,out->role,out->state,out->status,out->expires};cjr_digest(out->id,parts,6,out->digest);if(!*out->digest)found=-1;}
 }else if(step!=SQLITE_DONE)found=-1;sqlite3_finalize(s);return found;
}
static int cjr_post(sqlite3 *db,const char *community,const char *key,CjrPost *out){
 sqlite3_stmt *s=NULL;int found=0;memset(out,0,sizeof(*out));if(!cjr_key(key))return 0;
 const char *sql="SELECT id,post,author_subject,COALESCE(reply_to,''),COALESCE(title,''),body,status,moderation_state,visibility,published_at FROM community_post WHERE namespace='root' AND community=?1 AND post=?2 LIMIT 2";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return -1;
 sqlite3_bind_text(s,1,community,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,key,-1,SQLITE_STATIC);int step=sqlite3_step(s);
 if(step==SQLITE_ROW){out->id=sqlite3_column_int64(s,0);char *fields[]={out->key,out->author,out->parent,out->title,out->body,out->status,out->moderation,out->visibility,out->at};size_t caps[]={sizeof(out->key),sizeof(out->author),sizeof(out->parent),sizeof(out->title),sizeof(out->body),sizeof(out->status),sizeof(out->moderation),sizeof(out->visibility),sizeof(out->at)};found=1;
  for(int i=0;i<9;i++)if(!cjr_column(s,i+1,fields[i],caps[i]))found=-1;
  if(sqlite3_step(s)!=SQLITE_DONE)found=-1;
  if(found==1){const char *parts[]={out->key,out->author,out->parent,out->title,out->body,out->status,out->moderation,out->visibility,out->at};cjr_digest(out->id,parts,9,out->digest);if(!*out->digest)found=-1;}
 }else if(step!=SQLITE_DONE)found=-1;sqlite3_finalize(s);return found;
}
static int cjr_visible(const CjrPost *p){return !strcmp(p->visibility,"public")&&!strcmp(p->status,"published")&&(!strcmp(p->moderation,"accepted")||!strcmp(p->moderation,"visible")||!strcmp(p->moderation,"published"));}
static int cjr_thread(sqlite3 *db,const char *community,const CjrPost *start,char root[97],int require_visible){
 CjrPost current=*start;char seen[9][97];int n=0;
 for(;;){if(n>=9||strcmp(current.visibility,"public")||(strcmp(current.status,"published")&&strcmp(current.status,"archived"))||(!cjr_visible(&current)&&strcmp(current.moderation,"withdrawn")&&strcmp(current.moderation,"hidden"))||(require_visible&&!cjr_visible(&current)))return 0;
  for(int i=0;i<n;i++)if(!strcmp(seen[i],current.key))return 0;snprintf(seen[n++],97,"%s",current.key);
  if(!*current.parent){snprintf(root,97,"%s",current.key);return n;}
  char parent[97];snprintf(parent,sizeof(parent),"%s",current.parent);if(cjr_post(db,community,parent,&current)!=1)return 0;
 }
}
#endif
