#ifndef BONFYRE_COMMUNITY_JOURNEY_H
#define BONFYRE_COMMUNITY_JOURNEY_H
#include "community_read.h"
/* Native effects over CommunityBody, CommunityMembership and CommunityPost.
 * CLI callers are trusted gateways; only the gateway resolves the acting account. */
#define CJ_BYTES 16384
typedef struct {char community[97],target[97],expected[65],request[65],title[161],body[6001];} CjInput;
static int cj_string(bf_mp_reader_t *r,char *out,size_t cap){
 bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR||v.v.raw.len>=cap||memchr(v.v.raw.ptr,0,v.v.raw.len))return 0;
 for(uint32_t i=0;i<v.v.raw.len;i++)if((v.v.raw.ptr[i]<32&&v.v.raw.ptr[i]!='\n'&&v.v.raw.ptr[i]!='\r'&&v.v.raw.ptr[i]!='\t')||v.v.raw.ptr[i]==127)return 0;
 memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;return 1;
}
static int cj_decode(const void *data,size_t size,CjInput *p){
 bf_mp_reader_t r;bf_mp_value_t v;memset(p,0,sizeof(*p));bf_mp_reader_init(&r,data,size);
 if(!size||size>CJ_BYTES||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=7)return 0;
 bf_mp_type_t t=bf_mp_read_next(&r,&v);if(!((t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1)))return 0;
 char *parts[]={p->community,p->target,p->expected,p->request,p->title,p->body};size_t caps[]={sizeof(p->community),sizeof(p->target),sizeof(p->expected),sizeof(p->request),sizeof(p->title),sizeof(p->body)};
 for(int i=0;i<6;i++)if(!cj_string(&r,parts[i],caps[i]))return 0;return r.pos==r.len;
}
static int cj_words(const char *s){return s&&*s&&strspn(s," \r\n\t")<strlen(s);}
static int cj_nonce(const char *s){return strlen(s)==32&&strspn(s,"0123456789abcdef")==32;}
static void cj_identity(const char *prefix,const char *community,const char *actor,const char *nonce,char out[97]){
 const char *parts[]={prefix,community,actor,nonce};char digest[65];cjr_digest(0,parts,4,digest);snprintf(out,97,"%s-%s",prefix,digest);
}
static char *cj_payload(sqlite3 *db,const char *sql,const char *const *values,int count){
 sqlite3_stmt *s=NULL;char *text=NULL;if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return NULL;
 for(int i=0;i<count;i++)sqlite3_bind_text(s,i+1,values[i],-1,SQLITE_STATIC);
 if(sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_type(s,0)==SQLITE_TEXT)text=strdup((const char*)sqlite3_column_text(s,0));sqlite3_finalize(s);return text;
}
static int cj_write(sqlite3 *db,const char *type,long long *id,const char *payload){
 if(!payload||*id>INT_MAX)return 0;return *id?entry_update(db,type,(int)*id,"root",payload)==0:entry_create(db,type,"root",payload,id)==0;
}
static int cj_reply(const char *state,const char *community,const char *target,const char *digest,const char *message,const char *delta){
 bf_mp_writer_t w;bf_mp_writer_init(&w,512);int ok=!bf_mp_pack_array(&w,7)&&!bf_mp_pack_uint(&w,1);const char *parts[]={state,community,target,digest,message,delta};for(int i=0;i<6&&ok;i++)ok=!bf_mp_pack_str_cstr(&w,parts[i]);if(ok)ok=fwrite(w.buf,1,w.len,stdout)==w.len;bf_mp_writer_free(&w);return ok;
}
static int entry_community_journey(sqlite3 *db,const char *actor,const char *operation,const void *data,size_t size){
 CjInput p;CjrCommunity c={0};CjrMember member={0},selected={0};CjrPost post={0},parent={0};
 const char *state="invalid",*message="Review this operation in its community.",*delta="NOT_REQUESTED",*changed_type=NULL;char key[97]="",target[97]="",digest[65]="",now[64],root[97]="";char *payload=NULL,*membership_payload=NULL;long long changed_id=0,membership_id=0;int begun=0,committed=0,result=4,changed=0;
 if(!cjr_actor(actor)||!operation||!cj_decode(data,size,&p)||!find_type("community_body")||!find_type("community_membership")||!find_type("community_post"))goto done;
 int creating_guild=!strcmp(operation,"create-guild"),creating=!strcmp(operation,"create")||creating_guild,joining=!strcmp(operation,"request"),leaving=!strcmp(operation,"leave"),accepting=!strcmp(operation,"accept"),declining=!strcmp(operation,"decline"),revoking=!strcmp(operation,"revoke"),posting=!strcmp(operation,"post"),replying=!strcmp(operation,"reply"),editing=!strcmp(operation,"edit"),removing=!strcmp(operation,"remove"),hiding=!strcmp(operation,"hide"),editing_community=!strcmp(operation,"edit-community");
 if(!creating&&!joining&&!leaving&&!accepting&&!declining&&!revoking&&!posting&&!replying&&!editing&&!removing&&!hiding&&!editing_community)goto done;
 if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK){state="unavailable";message="The community owner is busy. No change was confirmed.";goto done;}begun=1;iso_timestamp(now,sizeof(now));
 if(creating){
  if(*p.community||*p.target||*p.expected||!cj_nonce(p.request)||!cj_words(p.title)||!cj_words(p.body))goto done;
  cj_identity(creating_guild?"guild":"community","",actor,p.request,key);int exists=cjr_community(db,key,&c);if(exists<0){state="unavailable";goto done;}
  if(exists){if(cjr_public(&c)&&!strcmp(c.owner,actor)&&!strcmp(c.title,p.title)&&!strcmp(c.description,p.body)){state="unchanged";message="This community is already created.";snprintf(digest,sizeof(digest),"%s",c.digest);result=0;goto finish;}state="conflict";message="This creation request already belongs to a different or withdrawn community.";goto done;}
  sqlite3_stmt *q=NULL;int capacity=0;if(sqlite3_prepare_v2(db,"SELECT count(*)<100 FROM community_body WHERE namespace='root' AND created_by_subject=?1",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,actor,-1,SQLITE_STATIC);capacity=sqlite3_step(q)==SQLITE_ROW&&sqlite3_column_int(q,0);}sqlite3_finalize(q);if(!capacity){state="capacity";message="Review your existing communities before creating another.";goto done;}
  char subject[128],membership[97];snprintf(subject,sizeof(subject),"community_body:%s",key);cj_identity("member",key,actor,"",membership);
  const char *values[]={key,subject,p.title,p.body,actor,CJR_POLICY,creating_guild?"local-guild":"local-community"};payload=cj_payload(db,"SELECT json_object('community',?1,'subject_address',?2,'title',?3,'description',?4,'visibility','public','created_by_subject',?5,'governance_scope',?7,'moderation_policy',?6,'cannot_infer','A local Aurekai community; it does not create an external account, provider membership, payment or programme enrollment.','status','published')",values,7);
  const char *mv[]={membership,key,actor,now};membership_payload=cj_payload(db,"SELECT json_object('membership',?1,'community',?2,'member_subject',?3,'role','steward','state','active','joined_at',?4,'cannot_infer','Local community participation only; no infrastructure, provider or financial authority.','status','published')",mv,4);
  defer_hooks=1;deferred_write_error=0;if(!cj_write(db,"community_body",&changed_id,payload)||!cj_write(db,"community_membership",&membership_id,membership_payload)||deferred_write_error){state="unavailable";goto done;}changed_type="community_body";changed=1;
  if(cjr_community(db,key,&c)!=1){state="unavailable";goto done;}snprintf(digest,sizeof(digest),"%s",c.digest);message="Your public community is ready. People can request to join.";goto commit;
 }
 if(!cjr_key(p.community))goto done;snprintf(key,sizeof(key),"%s",p.community);
 if(cjr_community(db,key,&c)!=1||!cjr_public(&c)){state="not_found";message="This is not an active local community.";goto done;}int owner=!strcmp(c.owner,actor);
 if(cjr_member(db,key,actor,&member)<0){state="unavailable";goto done;}
 if(joining||leaving||accepting||declining||revoking){
  const char *person=(joining||leaving)?actor:p.target;if(!cjr_actor(person)){state="invalid";goto done;}
  if((accepting||declining||revoking)&&!owner){state="forbidden";message="Only this community's steward can manage its requests.";goto done;}
  if(cjr_member(db,key,person,&selected)<0){state="unavailable";goto done;}snprintf(target,sizeof(target),"%s",person);
  if(!strcmp(c.owner,person)){if(joining){state="unchanged";message="You already manage this community.";result=0;goto finish;}state="forbidden";message="The steward cannot remove their own founding membership.";goto done;}
  const char *next=joining?"requested":leaving?"withdrawn":accepting?"active":declining?"declined":"revoked";
  if((joining&&(selected.active||(!strcmp(selected.state,"requested")&&!strcmp(selected.status,"published"))))||(selected.id&&!strcmp(selected.state,next)&&!strcmp(selected.status,"published"))){state="unchanged";message="Your current community state is already saved.";snprintf(digest,sizeof(digest),"%s",selected.digest);result=0;goto finish;}
  if(strcmp(p.expected,selected.digest)){state="conflict";message="This membership changed. Reload before deciding.";goto done;}
  if((joining&&selected.id&&strcmp(selected.state,"withdrawn"))||(accepting&&strcmp(selected.state,"requested"))||(declining&&strcmp(selected.state,"requested"))||(revoking&&!selected.active)||(leaving&&(!selected.id||(strcmp(selected.state,"requested")&&strcmp(selected.state,"active"))))){state="forbidden";message="This membership does not support that change.";goto done;}
  char membership[97];cj_identity("member",key,person,"",membership);const char *values[]={membership,key,person,next,now};
  payload=selected.id?cj_payload(db,"SELECT json_object('state',?4,'role','member','status','published','expires_at','')",values,5):cj_payload(db,"SELECT json_object('membership',?1,'community',?2,'member_subject',?3,'role','member','state',?4,'joined_at',?5,'expires_at','','cannot_infer','Local participation or a request; no external membership, payment or programme enrollment.','status','published')",values,5);
  changed_type="community_membership";changed_id=selected.id;defer_hooks=1;deferred_write_error=0;if(!cj_write(db,changed_type,&changed_id,payload)||deferred_write_error||cjr_member(db,key,person,&selected)!=1){state="unavailable";goto done;}changed=1;snprintf(digest,sizeof(digest),"%s",selected.digest);message=joining?"Your request is with the community steward.":leaving?"You have left this community. Existing conversations are retained.":accepting?"This person can now take part.":declining?"The request was declined.":"This member can no longer contribute here.";goto commit;
 }
 if(editing_community){
  if(!owner){state="forbidden";message="Only this community's steward can change its introduction.";goto done;}
  if(!cj_words(p.title)||!cj_words(p.body))goto done;
  if(!strcmp(c.title,p.title)&&!strcmp(c.description,p.body)){state="unchanged";message="The introduction already matches.";snprintf(digest,sizeof(digest),"%s",c.digest);result=0;goto finish;}
  if(strcmp(p.expected,c.digest)){state="conflict";message="The community introduction changed. Reload before editing.";goto done;}
  const char *v[]={p.title,p.body};payload=cj_payload(db,"SELECT json_object('title',?1,'description',?2)",v,2);changed_type="community_body";changed_id=c.id;defer_hooks=1;deferred_write_error=0;if(!cj_write(db,changed_type,&changed_id,payload)||deferred_write_error||cjr_community(db,key,&c)!=1){state="unavailable";goto done;}changed=1;snprintf(digest,sizeof(digest),"%s",c.digest);message="The community introduction was updated.";goto commit;
 }
 if(posting||replying){
  if(!owner&&!member.active){state="forbidden";message="Your membership must be active before contributing.";goto done;}
  if(!cj_nonce(p.request)||!cj_words(p.body)||(posting&&(!cj_words(p.title)||*p.target))||(replying&&!cjr_key(p.target)))goto done;
  char post_key[97];cj_identity("post",key,actor,p.request,post_key);int exists=cjr_post(db,key,post_key,&post);if(exists<0){state="unavailable";goto done;}
  if(exists){if(cjr_visible(&post)&&!strcmp(post.author,actor)&&!strcmp(post.title,p.title)&&!strcmp(post.body,p.body)&&!strcmp(post.parent,p.target)){state="unchanged";message="This contribution is already published.";snprintf(target,sizeof(target),"%s",post.key);snprintf(digest,sizeof(digest),"%s",post.digest);result=0;goto finish;}state="conflict";message="This publishing request already belongs to different or removed content.";goto done;}
  if(replying&&(cjr_post(db,key,p.target,&parent)!=1||cjr_thread(db,key,&parent,root,1)<1||cjr_thread(db,key,&parent,root,1)>=9||strcmp(parent.digest,p.expected))){state="conflict";message="The conversation changed or no longer accepts this reply. Reopen it before replying.";goto done;}
  const char *v[]={post_key,key,actor,p.title,p.body,p.target,now};payload=cj_payload(db,"SELECT json_object('post',?1,'community',?2,'author_subject',?3,'title',?4,'body',?5,'reply_to',?6,'published_at',?7,'visibility','public','moderation_state','accepted','cannot_infer','A local public contribution; no external publication, endorsement or operational commitment.','status','published')",v,7);
  changed_type="community_post";defer_hooks=1;deferred_write_error=0;if(!cj_write(db,changed_type,&changed_id,payload)||deferred_write_error||cjr_post(db,key,post_key,&post)!=1){state="unavailable";goto done;}changed=1;snprintf(target,sizeof(target),"%s",post.key);snprintf(digest,sizeof(digest),"%s",post.digest);message="Your contribution is published in this community.";goto commit;
 }
 if(cjr_post(db,key,p.target,&post)!=1||strcmp(post.visibility,"public")){state="not_found";message="This contribution is not in this community.";goto done;}snprintf(target,sizeof(target),"%s",post.key);
 if((hiding&&!owner)||((editing||removing)&&strcmp(post.author,actor))){state="forbidden";message="You cannot change this contribution.";goto done;}
 if(editing){if((!owner&&!member.active)||!cjr_visible(&post)){state="forbidden";message="Only active contributors can edit visible contributions.";goto done;}if(!cj_words(p.body)||(!*post.parent&&!cj_words(p.title)))goto done;if(!strcmp(post.title,p.title)&&!strcmp(post.body,p.body)){state="unchanged";message="This contribution already matches.";snprintf(digest,sizeof(digest),"%s",post.digest);result=0;goto finish;}}
 else if(!strcmp(post.status,"archived")){state="unchanged";message="This contribution is already removed.";snprintf(digest,sizeof(digest),"%s",post.digest);result=0;goto finish;}
 if(strcmp(p.expected,post.digest)){state="conflict";message="This contribution changed. Reload before editing or removing it.";goto done;}
 if(editing){const char *v[]={p.title,p.body};payload=cj_payload(db,"SELECT json_object('title',?1,'body',?2)",v,2);}else payload=strdup(hiding?"{\"status\":\"archived\",\"moderation_state\":\"hidden\"}":"{\"status\":\"archived\",\"moderation_state\":\"withdrawn\"}");
 changed_type="community_post";changed_id=post.id;defer_hooks=1;deferred_write_error=0;if(!cj_write(db,changed_type,&changed_id,payload)||deferred_write_error||cjr_post(db,key,p.target,&post)!=1){state="unavailable";goto done;}changed=1;snprintf(digest,sizeof(digest),"%s",post.digest);message=editing?"Your edit was saved.":"The text is no longer public. Replies keep their own authorship.";
commit:
 state="committed";result=0;
finish:
 if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){state="unavailable";result=6;goto done;}begun=0;committed=1;
 finish_deferred_hooks(db,1);if(changed){const char *coordinate=!strcmp(changed_type,"community_post")?post.key:!strcmp(changed_type,"community_membership")?selected.key:key;int ok=cms_emit_fact_delta(changed_type,coordinate,NULL,changed_id);if(membership_id){char membership[97];cj_identity("member",key,actor,"",membership);ok=cms_emit_fact_delta("community_membership",membership,NULL,membership_id)&&ok;}delta=!cms_fact_deltas_enabled()?"DISABLED":ok?"DELIVERED":"DELIVERY_FAILED";}else delta="UNCHANGED";
done:
 if(begun)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);if(!committed)finish_deferred_hooks(db,0);
 if(!cj_reply(state,key,target,digest,message,delta))result=6;free(payload);free(membership_payload);return result;
}
#endif
