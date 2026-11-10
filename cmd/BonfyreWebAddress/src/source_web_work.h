#ifndef BONFYRE_SOURCE_WEB_WORK_H
#define BONFYRE_SOURCE_WEB_WORK_H
/* SourceFrontier owns plans, provider admission, retained replies and credentials.
 * The HTTP boundary resolves the current actor and destination for every call. */
typedef struct {char context[128],operation[24],job[65],cursor[101],request[101],revision[16],challenge[65];unsigned seen;} SwQuery;
static int sw_query(const char *query,SwQuery *q){
 memset(q,0,sizeof(*q));if(!query||strlen(query)>4096)return 0;
 for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[24],*out=NULL;size_t cap=0;unsigned bit=0;
  if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;
  if(!strcmp(name,"context")){out=q->context;cap=sizeof(q->context);bit=1;}else if(!strcmp(name,"operation")){out=q->operation;cap=sizeof(q->operation);bit=2;}
  else if(!strcmp(name,"job")){out=q->job;cap=sizeof(q->job);bit=4;}else if(!strcmp(name,"cursor")){out=q->cursor;cap=sizeof(q->cursor);bit=8;}
  else if(!strcmp(name,"request")){out=q->request;cap=sizeof(q->request);bit=16;}
  else if(!strcmp(name,"revision")){out=q->revision;cap=sizeof(q->revision);bit=32;}
  else if(!strcmp(name,"challenge")){out=q->challenge;cap=sizeof(q->challenge);bit=64;}else return 0;
  if((q->seen&bit)||!psetup_decode(eq+1,(size_t)(end-eq-1),out,cap)||!*out)return 0;q->seen|=bit;p=*end?end+1:end;if(*end&&!(*p))return 0;
 }
 return (q->seen&3)==3;
}
static int sw_operation(const SwQuery *q,int writing){
 if(writing)return q->seen==3&&(!strcmp(q->operation,"plan")||!strcmp(q->operation,"start")||!strcmp(q->operation,"decision")||!strcmp(q->operation,"archive")||!strcmp(q->operation,"restore")||!strcmp(q->operation,"connect")||!strcmp(q->operation,"disconnect"));
 if(!strcmp(q->operation,"status"))return q->seen==3;
 if(!strcmp(q->operation,"list"))return q->seen==3||(q->seen==11&&strlen(q->cursor)<=15&&strspn(q->cursor,"0123456789")==strlen(q->cursor));
 if(!strcmp(q->operation,"get"))return q->seen==7&&aw_hex(q->job,64);
 if(!strcmp(q->operation,"provider"))return q->seen==103&&aw_hex(q->job,64)&&aw_hex(q->challenge,64)&&q->revision[0]>='1'&&q->revision[0]<='9'&&strspn(q->revision,"0123456789")==strlen(q->revision);
 if(!strcmp(q->operation,"item")||!strcmp(q->operation,"compare"))return q->seen==15&&aw_hex(q->job,64)&&aw_hex(q->cursor,64);
 if(!strcmp(q->operation,"recover"))return q->seen==19&&strlen(q->request)>=8&&strspn(q->request,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.:-")==strlen(q->request);
 return 0;
}
static void sw_page(void){
 const char *head="<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Web research · Aurekai</title><link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=2\"><link rel=\"stylesheet\" href=\"/apps/web-workbench.css\"><link rel=\"stylesheet\" href=\"/apps/extension/foreign-sign-in.css\"></head><body>";
 const char *tail="<main data-web-workbench class=\"web-workbench\"><nav aria-label=\"Breadcrumb\"><a href=\"/\">Aurekai</a><span aria-hidden=\"true\"> / </span><a href=\"/sources\">Links and sources</a><span aria-hidden=\"true\"> / </span><span>Web research</span></nav><header class=\"ww-heading\"><div><p class=\"ww-eyebrow\">Read, review, retain</p><h1>Bring the web into your work.</h1><p>Search with Exa or Firecrawl, read exact pages, and keep the source material you want to work with.</p></div><a href=\"/apps?context=self\">Your work</a></header><p data-web-status role=\"status\" aria-live=\"polite\">Opening your research workspace…</p><div data-web-content></div><noscript><p>Web research needs JavaScript. <a href=\"/sources\">Browse known sources</a> or open <a href=\"/apps?context=self\">your work</a>.</p></noscript></main><script type=\"module\" src=\"/apps/web-workbench.mjs\"></script></body></html>";
 fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Host, Cookie, Authorization\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",stdout);fputs(head,stdout);fputs(BF_ESTATE_BAR,stdout);fputs(tail,stdout);
}
static int serve_source_web_work(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
 if(!strcmp(target,"/sources/web")||!strcmp(target,"/sources/web/")){if(strcmp(method,"GET"))aw_error(405,"Use GET to open web research.\n");else sw_page();return 1;}
 if(strcmp(target,"/sources/_web"))return 0;
 int writing=!strcmp(method,"POST");SwQuery q;
 if((!writing&&strcmp(method,"GET"))||!sw_query(query,&q)||!sw_operation(&q,writing)){aw_error(400,"Use one declared web-research operation and its exact coordinates.\n");return 1;}
 char provider_origin[64]="";
 if(!strcmp(q.operation,"provider")){
  if(!host||(strcmp(host,"aurekai.work")&&strcmp(host,"aurekai.life")&&strcmp(host,"aurekai.live")&&strcmp(host,"aurekai.shop"))){aw_error(403,"Open this provider step from its Aurekai workspace.\n");return 1;}
  snprintf(provider_origin,sizeof(provider_origin),"https://%s",host);
 }
 char actor[160],subject[160];const char *scope=NULL;int administration=!strcmp(q.operation,"connect")||!strcmp(q.operation,"disconnect");
 if(psetup_context(bearer,auth_db,q.context,writing?(administration?1:2):0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1){aw_error(403,"Sign in with current access to this research destination.\n");return 1;}
 int can_administer=administration;
 if(!writing){char admin_actor[160],admin_subject[160];const char *admin_scope=NULL;
  can_administer=psetup_context(bearer,auth_db,q.context,1,admin_actor,sizeof(admin_actor),admin_subject,sizeof(admin_subject),&admin_scope)==1&&
   !strcmp(actor,admin_actor)&&!strcmp(subject,admin_subject)&&admin_scope&&!strcmp(scope,admin_scope);
 }
 size_t bytes=0;
 if(writing){char origin[300],http[300],https[300],content[80],length[32];snprintf(http,sizeof(http),"http://%s",host?host:"");snprintf(https,sizeof(https),"https://%s",host?host:"");
  if(!aw_header(raw,"Origin",origin,sizeof(origin))||(strcmp(origin,http)&&strcmp(origin,https))){aw_error(403,"Submit the review from the current workspace origin.\n");return 1;}
  if(!body||!aw_header(raw,"Content-Type",content,sizeof(content))||strcmp(content,"application/msgpack")||!aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>5||strspn(length,"0123456789")!=strlen(length)||(bytes=(size_t)strtoul(length,NULL,10))==0||bytes>65536){aw_error(400,"Submit a bounded native web-research packet.\n");return 1;}
 }
 const char *bun=getenv("BONFYRE_BUN_BINARY"),*owner=getenv("BONFYRE_WEB_WORK_OWNER"),*db=getenv("BONFYRE_SOURCE_FRONTIER_DB");
 if(!bun||*bun!='/'||!owner||*owner!='/'||!db||*db!='/'){aw_error(503,"The retained web-research owner is not connected. Saved source-library work remains available.\n");return 1;}
 char *argv[]={(char*)bun,"--no-env-file","--no-install",(char*)owner,"--operation",q.operation,"--db",(char*)db,"--subject",subject,"--scope",(char*)scope,"--actor",actor,"--job",q.job,"--cursor",q.cursor,"--request",q.request,"--revision",q.revision,"--challenge",q.challenge,"--origin",provider_origin,"--can-administer",can_administer?"1":"0",NULL};
 unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(argv,writing?body:NULL,bytes,&output,&size,1),valid=0;bf_mp_reader_t r;bf_mp_value_t v;
 if(rc==0&&output&&size&&size<=AW_PACKET){bf_mp_reader_init(&r,output,size);valid=bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count==3;
  if(valid){bf_mp_type_t t=bf_mp_read_next(&r,&v);valid=(t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1);}
  if(valid)valid=bf_mp_read_next(&r,&v)==BF_MP_BOOL;if(valid)valid=bf_mp_skip(&r)==0&&r.pos==r.len;
 }
 if(valid)aw_response(200,"application/msgpack",output,size,0);else aw_error(503,"The web-research reply was interrupted. Read the same saved run or request before taking another action.\n");
 free(output);return 1;
}
#endif
