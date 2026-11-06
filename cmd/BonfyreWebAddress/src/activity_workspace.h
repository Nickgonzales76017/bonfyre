#ifndef BONFYRE_ACTIVITY_WORKSPACE_H
#define BONFYRE_ACTIVITY_WORKSPACE_H
/* Fabric owns execution and recipient state. Resolve the current destination
 * on every call; browser coordinates never establish a principal or grant. */
typedef struct {char context[128],operation[24],task[129];unsigned seen;} AkActivityQuery;
static int ak_activity_query(const char *query,AkActivityQuery *q){
 memset(q,0,sizeof(*q));if(!query||strlen(query)>2048)return 0;
 for(const char *p=query;*p;){
  const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));
  char name[24],*out=NULL;size_t cap=0;unsigned bit=0;
  if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;
  if(!strcmp(name,"context")){out=q->context;cap=sizeof(q->context);bit=1;}
  else if(!strcmp(name,"operation")){out=q->operation;cap=sizeof(q->operation);bit=2;}
  else if(!strcmp(name,"task")){out=q->task;cap=sizeof(q->task);bit=4;}else return 0;
  if((q->seen&bit)||!psetup_decode(eq+1,(size_t)(end-eq-1),out,cap)||!*out)return 0;
  q->seen|=bit;p=*end?end+1:end;if(*end&&!*p)return 0;
 }
 if((q->seen&3)!=3)return 0;
 if(q->task[0]&&(strspn(q->task,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")!=strlen(q->task)||!isalnum((unsigned char)q->task[0])))return 0;
 return 1;
}
static int serve_activity_workspace(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
 if(strcmp(target,"/apps/_fabric"))return 0;
 AkActivityQuery q;int writing=!strcmp(method,"POST");
 if(!ak_activity_query(query,&q)){
  aw_error(400,"Choose an exact account work operation.\n");return 1;
 }
 int model_write=!strcmp(q.operation,"model-review")||!strcmp(q.operation,"model-submit")||!strcmp(q.operation,"model-cancel")||!strcmp(q.operation,"model-reconcile")||!strcmp(q.operation,"model-verify-transfer");
 int post_operation=model_write||!strcmp(q.operation,"notification")||!strcmp(q.operation,"push-subscribe")||!strcmp(q.operation,"push-unsubscribe")||!strcmp(q.operation,"push-ack")||!strcmp(q.operation,"push-resolve");
 int get_operation=(!strcmp(q.operation,"read"))||(!strcmp(q.operation,"model-read")&&q.seen==7)||
                   ((!strcmp(q.operation,"push-status")||!strcmp(q.operation,"model-capabilities"))&&q.seen==3);
 if(writing?(!post_operation||q.seen!=3):(strcmp(method,"GET")||!get_operation)){
  aw_error(400,"Use this account operation's declared method and coordinates.\n");return 1;
 }
 char actor[160],subject[160];const char *scope=NULL;
 /* Model execution needs current destination write access. Recipient actions
  * need read membership and cannot start or cancel a task. Native owners also
  * recheck expected account bindings and current background authority. */
 if(psetup_context(bearer,auth_db,q.context,model_write?2:0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1){
  aw_error(403,"Sign in with current access to this destination.\n");return 1;
 }
 size_t bytes=0;
 if(writing){
  char origin[300],http[300],https[300],content[80],length[32];
  snprintf(http,sizeof(http),"http://%s",host?host:"");snprintf(https,sizeof(https),"https://%s",host?host:"");
  if(!aw_header(raw,"Origin",origin,sizeof(origin))||(strcmp(origin,http)&&strcmp(origin,https))){aw_error(403,"Use the current workspace for this account action.\n");return 1;}
  if(!body||!aw_header(raw,"Content-Type",content,sizeof(content))||strcmp(content,"application/msgpack")||
     !aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>5||strspn(length,"0123456789")!=strlen(length)||
     (bytes=(size_t)strtoul(length,NULL,10))==0||bytes>65536){aw_error(400,"Submit a bounded native account work packet.\n");return 1;}
 }
 const char *bun=getenv("BONFYRE_BUN_BINARY"),*owner=getenv("BONFYRE_ACTIVITY_OWNER");
 if(!bun||*bun!='/'||!owner||*owner!='/'){aw_error(503,"Activity and Inbox are temporarily unavailable. Retained work is unchanged.\n");return 1;}
 char *argv[]={(char*)bun,"--no-env-file","--no-install",(char*)owner,"--operation",q.operation,"--actor",actor,"--subject",subject,"--scope",(char*)scope,"--task",q.task,NULL};
 unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(argv,writing?body:NULL,bytes,&output,&size,1),valid=0;bf_mp_reader_t r;bf_mp_value_t v;
 if(rc==0&&output&&size&&size<=AW_PACKET){
  bf_mp_reader_init(&r,output,size);valid=bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count==3;
  if(valid){bf_mp_type_t t=bf_mp_read_next(&r,&v);valid=(t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1);}
  if(valid)valid=bf_mp_read_next(&r,&v)==BF_MP_BOOL;
  if(valid)valid=bf_mp_skip(&r)==0&&r.pos==r.len;
 }
 if(valid)aw_response(200,"application/msgpack",output,size,0);
 else aw_error(503,"Activity could not be read completely. Reopen the same work before repeating an action.\n");
 free(output);return 1;
}
#endif
