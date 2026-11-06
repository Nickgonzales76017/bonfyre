#ifndef BONFYRE_CONNECTIONS_WORKSPACE_H
#define BONFYRE_CONNECTIONS_WORKSPACE_H
/* Auth decides the destination; the enrollment owner decides its configured
 * lane, reviewed work and declared credential. No client identity comes from
 * a hidden input or a provider's redirect. */
typedef struct{char context[96],identity[16],operation[20],work[32],revision[24],challenge[65];unsigned seen;} CwQuery;
static int cw_query(const char *text,CwQuery *q){
 memset(q,0,sizeof(*q));if(!text||strlen(text)>1024)return 0;
 for(const char *p=text;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[24],*dest=NULL;size_t cap=0;unsigned bit=0;
  if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
  if(!strcmp(key,"context")){dest=q->context;cap=sizeof(q->context);bit=1;}else if(!strcmp(key,"identity")){dest=q->identity;cap=sizeof(q->identity);bit=2;}
  else if(!strcmp(key,"operation")){dest=q->operation;cap=sizeof(q->operation);bit=4;}else if(!strcmp(key,"work")){dest=q->work;cap=sizeof(q->work);bit=8;}
  else if(!strcmp(key,"revision")){dest=q->revision;cap=sizeof(q->revision);bit=16;}else if(!strcmp(key,"challenge")){dest=q->challenge;cap=sizeof(q->challenge);bit=32;}else return 0;
  if((q->seen&bit)||!psetup_decode(eq+1,(size_t)(end-eq-1),dest,cap)||!*dest)return 0;q->seen|=bit;p=*end?end+1:end;if(*end&&!*p)return 0;
 }
 return 1;
}
static int cw_packet(const unsigned char *data,size_t size){bf_mp_reader_t r;bf_mp_value_t v;if(!data||!size||size>AW_PACKET)return 0;bf_mp_reader_init(&r,data,size);if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3)return 0;bf_mp_type_t t=bf_mp_read_next(&r,&v);return ((t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1))&&bf_mp_read_next(&r,&v)==BF_MP_BOOL&&bf_mp_skip(&r)==0&&r.pos==r.len;}
/* The catalog files and admitted account are operator configuration, never
 * supplied by a browser. Only the native owner's reduced app view may pass. */
static int cw_catalog_packet(const unsigned char *bytes,size_t size){
 bf_json_doc_t *d=bf_json_parse((const char*)bytes,size,NULL,0);if(!d)return 0;
 const bf_json_node_t *r=bf_json_root(d),*schema=r?bf_json_obj_get(d,r,"schema"):NULL,*rows=r?bf_json_obj_get(d,r,"entries"):NULL,*offset=r?bf_json_obj_get(d,r,"offset"):NULL,*next=r?bf_json_obj_get(d,r,"next_offset"):NULL,*boundary=r?bf_json_obj_get(d,r,"boundary"):NULL;char text[4097];
 int ok=r&&r->type==BF_JSON_OBJECT&&r->child_count==5&&schema&&schema->type==BF_JSON_STRING&&bf_json_get_str_copy(schema,text,sizeof(text))>0&&!strcmp(text,"bonfyre.connection-catalog.v1")&&rows&&rows->type==BF_JSON_ARRAY&&rows->child_count<=50&&offset&&offset->type==BF_JSON_INT&&offset->val.i>=0&&next&&(next->type==BF_JSON_NULL||(next->type==BF_JSON_INT&&next->val.i>offset->val.i))&&boundary&&boundary->type==BF_JSON_STRING;
 for(int i=0;ok&&i<rows->child_count;i++){
  const bf_json_node_t *row=bf_json_arr_get(d,rows,i),*ecosystem=row?bf_json_obj_get(d,row,"ecosystem"):NULL,*name=row?bf_json_obj_get(d,row,"name"):NULL,*link=row?bf_json_obj_get(d,row,"safe_link"):NULL,*status=row?bf_json_obj_get(d,row,"status"):NULL;
  ok=row&&row->type==BF_JSON_OBJECT&&row->child_count==4&&ecosystem&&ecosystem->type==BF_JSON_STRING&&name&&name->type==BF_JSON_STRING&&link&&(link->type==BF_JSON_NULL||(link->type==BF_JSON_STRING&&bf_json_get_str_copy(link,text,sizeof(text))>8&&!strncmp(text,"https://",8)))&&status&&status->type==BF_JSON_STRING&&bf_json_get_str_copy(status,text,sizeof(text))>0&&!strcmp(text,"unverified_source_candidate");
 }
 bf_json_free(d);return ok;
}
static int cw_catalog_decode(const char *restrict s,size_t len,char *restrict out,size_t cap){size_t n=0;for(size_t i=0;i<len;i++){unsigned char c=(unsigned char)s[i];if(c=='%'){if(i+2>=len||psetup_hex(s[i+1])<0||psetup_hex(s[i+2])<0)return 0;c=(unsigned char)(psetup_hex(s[i+1])*16+psetup_hex(s[i+2]));i+=2;}else if(c=='+')c=' ';if(c<32||c==127||n+1>=cap)return 0;out[n++]=(char)c;}out[n]=0;return 1;}
static int cw_catalog(const char *method,const char *query,const char *bearer,const char *auth_db){
 if(strcmp(method,"GET")){aw_error(405,"Use GET to browse connection sources.\n");return 1;}
 char offset[8]="0",ecosystem[129]="",search[129]="",repr[4]="";unsigned seen=0;
 for(const char *p=query?query:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[16],*out=NULL;size_t cap=0;unsigned bit=0;
  if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))goto bad;
  if(!strcmp(key,"offset")){out=offset;cap=sizeof(offset);bit=1;}else if(!strcmp(key,"ecosystem")){out=ecosystem;cap=sizeof(ecosystem);bit=2;}else if(!strcmp(key,"search")){out=search;cap=sizeof(search);bit=4;}else if(!strcmp(key,"repr")){out=repr;cap=sizeof(repr);bit=8;}else goto bad;
  if((seen&bit)||!cw_catalog_decode(eq+1,(size_t)(end-eq-1),out,cap)||!*out)goto bad;seen|=bit;p=*end?end+1:end;if(*end&&!*p)goto bad;
 }
 if(!*offset||strspn(offset,"0123456789")!=strlen(offset)||strtoul(offset,NULL,10)>1000000||(*repr&&strcmp(repr,"API")))goto bad;
 char actor[160],subject[160];const char *scope=NULL,*admitted=getenv("BONFYRE_CONNECTION_CATALOG_SUBJECT");
 if(psetup_context(bearer,auth_db,"self",0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1||!admitted||strcmp(subject,admitted)||strcmp(actor,subject)||strcmp(scope,"self")){aw_error(403,"Sign in to your connection sources.\n");return 1;}
 const char *bin=getenv("BONFYRE_CONNECTION_CATALOG_BINARY"),*facets=getenv("BONFYRE_CONNECTION_CATALOG_FACETS"),*entries=getenv("BONFYRE_CONNECTION_CATALOG_ENTRIES");
 if(!bin||*bin!='/'||access(bin,X_OK)||!facets||*facets!='/'||!entries||*entries!='/'){aw_error(503,"Connection sources are unavailable.\n");return 1;}
 char *args[18];int n=0;args[n++]=(char*)bin;args[n++]="list";args[n++]="--facets";args[n++]=(char*)facets;args[n++]="--entries";args[n++]=(char*)entries;args[n++]="--view";args[n++]="app";args[n++]="--limit";args[n++]="50";args[n++]="--offset";args[n++]=offset;if(*ecosystem){args[n++]="--ecosystem";args[n++]=ecosystem;}if(*search){args[n++]="--search";args[n++]=search;}args[n]=NULL;
 unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(args,NULL,0,&output,&size,0);if(!rc&&output&&cw_catalog_packet(output,size))aw_response(200,"application/json; charset=utf-8",output,size,0);else aw_error(503,"Connection sources could not be read.\n");free(output);return 1;
bad:aw_error(400,"Choose a valid source page and ecosystem.\n");return 1;
}
static int serve_connections_workspace(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
 if(!strcmp(target,"/apps/_connection_catalog"))return cw_catalog(method,query,bearer,auth_db);
 const char *assets[]={"authentication-capability-surface.mjs","provider-auth-capability.mjs","connection-source.mjs","connection-studio.mjs","connection-studio.css","connection-lab.mjs"};
 for(size_t i=0;i<sizeof(assets)/sizeof(assets[0]);i++)if(!strncmp(target,"/apps/",6)&&!strcmp(target+6,assets[i])){
  if(strcmp(method,"GET")){aw_error(405,"Use GET for workspace code.\n");return 1;}size_t bytes=0;const char *root=getenv("BONFYRE_WORKSPACE_CLIENT");unsigned char *file=root?aw_read_file(root,assets[i],256u*1024u,&bytes):NULL;
  if(file){aw_response(200,aw_mime(assets[i]),file,bytes,0);free(file);}else aw_error(503,"This workspace release is unavailable.\n");return 1;
 }
 int page=!strcmp(target,"/apps/connections");if(!page&&strcmp(target,"/apps/_connections"))return 0;
 CwQuery q;int post=!strcmp(method,"POST");
 if(!cw_query(query&&*query?query:"context=self&identity=gmail",&q)){aw_error(400,"Use one exact connection destination and identity.\n");return 1;}
 if(!(q.seen&1))snprintf(q.context,sizeof(q.context),"self");if(!(q.seen&2))snprintf(q.identity,sizeof(q.identity),"gmail");
 if(strcmp(q.identity,"gmail")&&strcmp(q.identity,"proton")){aw_error(400,"Choose an assigned provider identity.\n");return 1;}
 char actor[160],subject[160];const char *scope=NULL;int permission=psetup_context(bearer,auth_db,q.context,post?2:0,actor,sizeof(actor),subject,sizeof(subject),&scope);
 if(permission!=1){if(page&&!post){fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=%2Fapps%2Fconnections%3Fcontext%3D",stdout);url_query_value(q.context);fputs("%26identity%3D",stdout);url_query_value(q.identity);if(q.work[0]){fputs("%26work%3D",stdout);url_query_value(q.work);}fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);}else aw_error(403,"Sign in with current access to this connection destination.\n");return 1;}
 if(page){
  if(strcmp(method,"GET")||(q.seen&~11u)){aw_error(400,"Open the exact saved connection setup.\n");return 1;}
  sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;char binding[64]={0};int found=0;
  if(db&&sqlite3_prepare_v2(db,"SELECT w.binding_id FROM _application_workspace w JOIN pack_installation i ON i.id=w.installation_id JOIN application_binding b ON b.id=w.cms_id JOIN pack_activation a ON a.pack=i.pack AND a.activated_for=i.installed_for AND a.namespace=i.namespace WHERE i.installed_for=? AND a.scope=? AND i.pack='experience.connection-studio.workspace' AND b.status='active' AND i.status='published' AND a.status='published'",-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,scope,-1,SQLITE_STATIC);found=sqlite3_step(s)==SQLITE_ROW&&bfpg_text(s,0,binding,sizeof(binding))&&strlen(binding)==43&&!strncmp(binding,"ws-",3)&&aw_hex(binding+3,40)&&sqlite3_step(s)==SQLITE_DONE;}
  sqlite3_finalize(s);if(db)sqlite3_close(db);
  if(!found){aw_error(404,"Connection Studio has not been installed for this destination.\n");return 1;}
  printf("HTTP/1.1 303 See Other\r\nLocation: /apps/%s/connections/?context=",binding);url_query_value(q.context);fputs("&identity=",stdout);url_query_value(q.identity);if(q.work[0]){fputs("&work=",stdout);url_query_value(q.work);}fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;
 }
 int list=!strcmp(q.operation,"list"),get=!strcmp(q.operation,"get"),provider=!strcmp(q.operation,"provider"),change=!strcmp(q.operation,"change");
 if((!post&&strcmp(method,"GET"))||change!=post||(!list&&!get&&!provider&&!change)||(q.seen&7u)!=7u||(list&&q.seen!=7)||(get&&q.seen!=15)||(provider&&q.seen!=63)||(change&&q.seen!=7)){aw_error(400,"Use one declared connection operation and its exact coordinates.\n");return 1;}
 char origin[300],expected[300],content[80],length[24];size_t size=0;
 if(!host||!*host||strlen(host)>250||strcspn(host,"\r\n/\\ ")!=strlen(host)){aw_error(400,"Invalid workspace origin.\n");return 1;}
 int local=!strcmp(host,"localhost")||!strcmp(host,"127.0.0.1")||!strncmp(host,"localhost:",10)||!strncmp(host,"127.0.0.1:",10);snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host);
 if(post&&(!body||!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)||!aw_header(raw,"Content-Type",content,sizeof(content))||strcmp(content,"application/msgpack")||!aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>5||strspn(length,"0123456789")!=strlen(length)||(size=(size_t)strtoul(length,NULL,10))==0||size>65536)){aw_error(403,"Submit this bounded change from the current account workspace.\n");return 1;}
 if(provider&&(!aw_hex(q.challenge,64)||!*q.revision||strlen(q.revision)>15||strspn(q.revision,"0123456789")!=strlen(q.revision))){aw_error(400,"Use the exact reviewed provider continuation.\n");return 1;}
 const char *bun=getenv("BONFYRE_BUN_BINARY"),*owner=getenv("BONFYRE_CONNECTION_OWNER");if(!bun||*bun!='/'||!owner||*owner!='/'||access(bun,X_OK)||access(owner,R_OK)){aw_error(503,"The account connection owner is not connected to this release.\n");return 1;}
 char *args[28];int n=0;args[n++]=(char*)bun;args[n++]="--no-env-file";args[n++]="--no-install";args[n++]=(char*)owner;args[n++]="--operation";args[n++]=q.operation;args[n++]="--actor";args[n++]=actor;args[n++]="--subject";args[n++]=subject;args[n++]="--scope";args[n++]=(char*)scope;args[n++]="--identity";args[n++]=q.identity;
 if(q.work[0]){args[n++]="--work";args[n++]=q.work;}if(provider){args[n++]="--revision";args[n++]=q.revision;args[n++]="--challenge";args[n++]=q.challenge;args[n++]="--origin";args[n++]=expected;}args[n]=NULL;
 unsigned char *output=NULL;size_t bytes=0;int rc=aw_native_result(args,post?body:NULL,size,&output,&bytes,1);
 if(!rc&&cw_packet(output,bytes))aw_response(200,"application/msgpack",output,bytes,0);else aw_error(503,post?"The connection change is unconfirmed. Reload the selected setup before another change.\n":"The current connection setup could not be read.\n");free(output);return 1;
}
#endif
