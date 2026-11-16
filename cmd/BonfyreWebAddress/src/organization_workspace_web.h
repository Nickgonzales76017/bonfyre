#ifndef BONFYRE_ORGANIZATION_WORKSPACE_WEB_H
#define BONFYRE_ORGANIZATION_WORKSPACE_WEB_H
#include "ui_tree.h"
#include "form_tree.h"
#include "web_store.h"
#include "bf_command_lookup.h"
/* Ordinary forms call Auth's existing admission owner. Cookies, member grants,
 * packages, editor records and provider authority keep their existing owners. */
typedef struct {char organization[97],name[161],nonce[65],email[321],role[8],hours[4],secret[65],operation[24],digest[65],revision[24],expiry[64],expected[8],after[97],pack[97],provider[24],mode[8];} OgForm;
#include "provider_accounts_web.h"
typedef struct {char value[6][321];unsigned count;} OgRow;
typedef struct {int status;char message[512],cursor[97];unsigned count;OgRow rows[50];} OgResult;
static int og_token(const char *s){return strlen(s)==64&&strspn(s,"0123456789abcdef")==64;}
static int og_random(char out[65]){unsigned char b[32];int fd=open("/dev/urandom",O_RDONLY);if(fd<0)return 0;size_t done=0;while(done<sizeof(b)){ssize_t n=read(fd,b+done,sizeof(b)-done);if(n<0&&errno==EINTR)continue;if(n<=0){close(fd);return 0;}done+=(size_t)n;}close(fd);static const char hex[]="0123456789abcdef";for(size_t i=0;i<32;i++){out[2*i]=hex[b[i]>>4];out[2*i+1]=hex[b[i]&15];}out[64]=0;return 1;}
/* Names are human text, not the identifier grammar used by PackSetup. The
 * native Auth decoder validates UTF-8; controls and NUL never reach it. */
static int og_human(const char *restrict s,size_t length,char *restrict out,size_t cap){size_t n=0;for(size_t i=0;i<length;i++){unsigned char c=(unsigned char)s[i];if(c=='%'){if(i+2>=length||psetup_hex(s[i+1])<0||psetup_hex(s[i+2])<0)return 0;c=(unsigned char)(psetup_hex(s[i+1])*16+psetup_hex(s[i+2]));i+=2;}else if(c=='+')c=' ';if(c<32||c==127||n+1>=cap)return 0;out[n++]=(char)c;}out[n]=0;return 1;}
static int og_form(const char *input,OgForm *out){
 memset(out,0,sizeof(*out));if(input&&strlen(input)>4096)return 0;unsigned seen=0;
 for(const char *p=input?input:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[24];if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;char *dest=NULL;size_t cap=0;unsigned bit=0;
#define OG_FIELD(n,b) if(!strcmp(key,#n)){dest=out->n;cap=sizeof(out->n);bit=b;}
 OG_FIELD(organization,1) else OG_FIELD(name,2) else OG_FIELD(nonce,4) else OG_FIELD(email,8) else OG_FIELD(role,16) else OG_FIELD(hours,32) else OG_FIELD(secret,64) else OG_FIELD(operation,128) else OG_FIELD(digest,256) else OG_FIELD(revision,512) else OG_FIELD(expiry,1024) else OG_FIELD(expected,2048) else OG_FIELD(after,4096) else OG_FIELD(pack,8192) else OG_FIELD(provider,16384) else OG_FIELD(mode,32768)
#undef OG_FIELD
 if(!dest||(seen&bit)||!((bit==2||bit==8)?og_human(eq+1,(size_t)(end-eq-1),dest,cap):psetup_decode(eq+1,(size_t)(end-eq-1),dest,cap)))return 0;seen|=bit;p=*end?end+1:end;}
 return (!*out->organization||bfpg_token(out->organization,64))&&(!*out->pack||bfpg_token(out->pack,96));
}
/* The shared bar and footer carry inline scripts; this page admits scripts only
 * by nonce, so they are printed with the page nonce rather than loosening CSP. */
static const char *og_page_nonce="";
static void og_nonced(const char *markup,const char *nonce){for(const char *p=markup;*p;){const char *at=strstr(p,"<script>");if(!at){fputs(p,stdout);break;}fwrite(p,1,(size_t)(at-p),stdout);printf("<script nonce=\"%s\">",nonce);p=at+8;}}
static void og_start_inner(int status,const char *title,const char *nonce,int clear_invitation){
 /* Keep same-origin form provenance while withholding referrers externally.
  * Invitation secrets remain in fragments or POST bodies, never query URLs. */
 printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store, no-transform\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nContent-Security-Policy: default-src 'none'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; script-src 'nonce-%s' 'strict-dynamic'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'\r\nConnection: close\r\n%s\r\n",status,status<400?"OK":status==403?"Forbidden":status==409?"Conflict":"Unavailable",nonce,clear_invitation?"Set-Cookie: aurekai_org_invite=; Path=/organizations/join; Max-Age=0; HttpOnly; SameSite=Lax; Secure\r\n":"");
 fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>",stdout);html(title);fputs(" \u00B7 Aurekai</title><link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;1,7..72,400&amp;display=swap\"><link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=4\"></head><body class=\"ak-app og-app\">",stdout);
 og_page_nonce=nonce;og_nonced(BF_ESTATE_BAR,nonce);
 ui_start(stdout,"main");ui_attr(stdout,"class","og-shell");ui_flag(stdout,"data-org-page");ui_gt(stdout);ui_start(stdout,"nav");ui_attr(stdout,"class","og-tabs");ui_attr(stdout,"aria-label","Aurekai work");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"href","/apps?context=self");ui_gt(stdout);ui_copy(stdout,"Your work");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/organizations");ui_attr(stdout,"aria-current","page");ui_gt(stdout);ui_copy(stdout,"Organizations");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/organizations/join");ui_gt(stdout);ui_copy(stdout,"Open an invitation");ui_close(stdout,"a");ui_close(stdout,"nav");ui_open(stdout,"p","og-kicker");ui_copy(stdout,"Workspaces and organizations");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);html(title);ui_close(stdout,"h1");
}
static void og_start(int status,const char *title,const char *nonce){og_start_inner(status,title,nonce,0);printf("<script type=\"module\" nonce=\"%s\" src=\"/apps/workspace-members.mjs\"></script>",nonce);}
static void og_end(void){ui_open(stdout,"p","og-note");ui_copy(stdout,"Organization access does not share personal records, establish legal identity or authorize payments.");ui_close(stdout,"p");ui_close(stdout,"main");og_nonced(BF_ESTATE_FOOT "</body></html>",og_page_nonce);}
static void og_notice(int status,const char *title,const char *message){char nonce[65];if(!og_random(nonce)){aw_error(503,"Could not prepare a secure page.\n");return;}og_start(status,title,nonce);ui_start(stdout,"p");ui_attr(stdout,"role","alert");ui_gt(stdout);html(message);ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/organizations");ui_gt(stdout);ui_copy(stdout,"Return to organizations");ui_close(stdout,"a");og_end();}
static int og_call(const char *db,const char *token,const char *op,const char **args,unsigned count,OgResult *out){
 memset(out,0,sizeof(*out));bf_mp_writer_t w;bf_mp_writer_init(&w,2048);int bad=bf_mp_pack_array(&w,4)||bf_mp_pack_uint(&w,1)||bf_mp_pack_str_cstr(&w,token?token:"")||bf_mp_pack_str_cstr(&w,op)||bf_mp_pack_array(&w,count);for(unsigned i=0;i<count&&!bad;i++)bad=bf_mp_pack_str_cstr(&w,args[i]);
 const char *bin=auth_binary();char *const argv[]={(char *)bin,"--db",(char *)db,"organization-stdin",NULL};unsigned char *body=NULL;size_t bytes=0;int rc=bad?-1:aw_native_result(argv,w.buf,w.len,&body,&bytes,1);bf_mp_writer_free(&w);if(rc<0||bytes>131072){free(body);return 0;}
 bf_mp_reader_t r;bf_mp_reader_init(&r,body,bytes);long long version,status;bf_mp_value_t v;int ok=ac_array(&r,5)&&ac_uint(&r,&version)&&version==1&&ac_uint(&r,&status)&&status>=200&&status<=599&&ac_text(&r,out->message,sizeof(out->message))&&bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count<=50;
 if(ok){out->status=(int)status;out->count=v.v.count;for(unsigned n=0;n<out->count&&ok;n++){if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count>6){ok=0;break;}OgRow *row=&out->rows[n];row->count=v.v.count;for(unsigned c=0;c<row->count&&ok;c++){bf_mp_reader_t value=r;if(bf_mp_read_next(&value,&v)==BF_MP_UINT&&v.v.u<INT_MAX){snprintf(row->value[c],321,"%llu",(unsigned long long)v.v.u);r=value;}else ok=ac_text(&r,row->value[c],321);}}if(ok)ok=ac_text(&r,out->cursor,sizeof(out->cursor))&&r.pos==r.len&&((rc==0)==(status<400));}free(body);return ok;
}
static void og_org_link(const char *id,const char *path){fputs(path,stdout);fputs(strchr(path,'?')?"&amp;":"?",stdout);fputs("context=institution%3A",stdout);url_query_value(id);}
static void og_open_links(const char *id,const char *pack){ui_open(stdout,"p","actions");ui_start(stdout,"a");ui_flag(stdout,"data-org-work");ui_attr_open(stdout,"href");og_org_link(id,"/apps");fputs("\">Open shared work</a><a data-org-setup href=\"/packs/setup?context=institution%3A",stdout);url_query_value(id);if(pack&&*pack){fputs("&amp;pack=",stdout);url_query_value(pack);}fputs("\">Choose and configure applications</a><a href=\"/organizations?organization=",stdout);url_query_value(id);fputs("\">People and invitations</a></p>",stdout);}
/* One Auth-owned child listing; never grant child login or silently install apps. */
static void og_subaccounts(const char *parent,const char *token,const char *auth_db){
 const char *args[]={parent,""};OgResult result;
 ui_start(stdout,"section");ui_flag(stdout,"data-org-subaccounts");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Subaccounts");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Separate destinations for publications, teams or clients. Creating one does not share private data, admit members, install applications or publish content.");ui_close(stdout,"p");
 if(!og_call(auth_db,token,"subaccounts",args,2,&result)||result.status!=200){ui_start(stdout,"p");ui_attr(stdout,"role","alert");ui_gt(stdout);ui_copy(stdout,"The child workspace list is unavailable.");ui_close(stdout,"p");ui_close(stdout,"section");return;}
 if(!result.count)ui_text(stdout, "p", NULL, "No subaccounts yet.");
 for(unsigned i=0;i<result.count;i++){OgRow *row=&result.rows[i];if(row->count!=4)continue;
  ui_start(stdout,"article");ui_attr_open(stdout,"data-org-subaccount");html(row->value[0]);fputs("\"><h3>",stdout);html(row->value[1]);ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Subaccount ");ui_open(stdout,"code",NULL);html(row->value[0]);ui_close(stdout,"code");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Parent organization: ");ui_open(stdout,"code",NULL);html(parent);ui_close(stdout,"code");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_copy(stdout,"Separate application bindings and editorial permissions must be explicitly admitted.");ui_close(stdout,"p");ui_close(stdout,"article");
 }
 if(*result.cursor)ui_text(stdout, "p", NULL, "Additional child destinations exist. Use the native paginated organization owner to inspect them.");
 ui_close(stdout,"section");
}
static void og_subaccount_create_form(const char *parent,const char *nonce){
 ui_start(stdout,"section");ui_flag(stdout,"data-org-subaccount-creation");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Create a subaccount");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Start a distinct publication, client or team under this organization. Access and publishing remain separately governed.");ui_close(stdout,"p");
 const FtField hidden[]={{"operation","subaccount-create"},{"organization",parent},{"nonce",nonce}};
 static const FtInput in[]={{.label="Subaccount name",.name="name",.maxlength="160",.required=1}};
 const FtForm f={.id="og-subaccount",.action="/organizations",.label="Create subaccount",.hook="org-subaccount-create",.hidden=hidden,.nhidden=3,.inputs=in,.ninputs=1};
 ft_form(stdout,&f);
 ui_close(stdout,"section");
}
static void og_new_form(const char *nonce,const char *pack){ui_open(stdout,"section",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Create an organization workspace");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Name the business, team or collective you are starting here. Its private work stays separate from your personal workspace. This does not claim an existing company's identity.");ui_close(stdout,"p");
 const FtField hidden[]={{"operation","create"},{"nonce",nonce},{"pack",pack}};
 static const FtInput in[]={{.label="Organization name",.name="name",.maxlength="160",.required=1,.autocomplete="organization"}};
 const FtForm f={.id="og-create",.action="/organizations",.label="Create organization workspace",.hook="org-create",.hidden=hidden,.nhidden=3,.inputs=in,.ninputs=1};
 ft_form(stdout,&f);ui_close(stdout,"section");}
static int og_origin(const char *raw,const char *host){char origin[512],type[128],expected[512];int local=host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10));snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host?host:"");return aw_header(raw,"Origin",origin,sizeof(origin))&&!strcmp(origin,expected)&&aw_header(raw,"Content-Type",type,sizeof(type))&&!strcmp(type,"application/x-www-form-urlencoded");}
static int og_pending(const char *raw,char secret[65]){char cookies[8192];if(!aw_header(raw,"Cookie",cookies,sizeof(cookies)))return 0;int found=0;for(char *p=cookies;*p;){while(*p==' ')p++;char *end=strchr(p,';');if(!end)end=p+strlen(p);const char *key="aurekai_org_invite=";size_t n=strlen(key);if((size_t)(end-p)>=n&&!strncmp(p,key,n)){if(found++||(size_t)(end-p)!=n+64)return 0;memcpy(secret,p+n,64);secret[64]=0;}p=*end?end+1:end;}return found==1&&og_token(secret);}
static void og_members(const char *id,const char *token,const char *auth_db){
 if(!session_institution_authorized(token,auth_db,id,"admin"))return;
 sqlite3 *db=NULL;sqlite3_stmt *q=NULL;if(bf_store_open(auth_db, &db, SQLITE_OPEN_READONLY, &bf_store_policy_auth)!=SQLITE_OK)goto done;
 const char *sql="SELECT u.id,u.name,u.email,g.role,COALESCE(g.expires_at,''),g.active FROM institution_grants g JOIN users u ON u.id=g.user_id WHERE g.institution=? ORDER BY u.id LIMIT 101";
 if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);StudioViewer me;if(!session_identity(token,auth_db,&me))goto done;
 ui_open(stdout,"section",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"People and access");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Read permits inspection, write permits editing, and admin permits configuration and access management. Changes use the existing Auth owner and recheck the reviewed role and expiry.");ui_close(stdout,"p");ui_open(stdout,"div","scroll");ui_open(stdout,"table",NULL);ui_open(stdout,"thead",NULL);ui_open(stdout,"tr",NULL);ui_open(stdout,"th",NULL);ui_copy(stdout,"Person");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Access");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Reviewed change");ui_close(stdout,"th");ui_close(stdout,"tr");ui_close(stdout,"thead");ui_open(stdout,"tbody",NULL);
 int step,count=0;while((step=sqlite3_step(q))==SQLITE_ROW){if(count++==100){ui_open(stdout,"tr",NULL);ui_start(stdout,"td");ui_attr(stdout,"colspan","3");ui_gt(stdout);ui_copy(stdout,"Showing the first 100 members; use the native membership owner for the full paginated list.");ui_close(stdout,"td");ui_close(stdout,"tr");break;}const char *email=(const char *)sqlite3_column_text(q,2),*role=(const char *)sqlite3_column_text(q,3),*expiry=(const char *)sqlite3_column_text(q,4);int active=sqlite3_column_int(q,5);ui_start(stdout,"tr");ui_flag(stdout,"data-org-member");ui_gt(stdout);ui_open(stdout,"td",NULL);html((const char *)sqlite3_column_text(q,1));ui_open(stdout,"small",NULL);html(email);ui_close(stdout,"small");ui_close(stdout,"td");ui_open(stdout,"td",NULL);html(active?role:"Removed");if(*expiry){ui_open(stdout,"small",NULL);ui_copy(stdout,"Expires ");html(expiry);ui_close(stdout,"small");}ui_close(stdout,"td");ui_open(stdout,"td",NULL);
 if(sqlite3_column_int64(q,0)!=me.id){const FtField hidden[]={{"operation","member"},{"organization",id},{"email",email},{"expected",active?role:"none"},{"expiry",active?expiry:""}};
  FtOption roles[]={{"none","none",0},{"read","read",0},{"write","write",0},{"admin","admin",0}};
  for(unsigned n=0;n<4;n++)roles[n].checked=!strcmp(roles[n].value,active?role:"none");
  const FtInput in[]={{.label="Access",.type="select",.name="role",.options=roles,.noptions=4}};
  const FtForm f={.id="og-member",.action="/organizations",.label="Apply reviewed access",.hook="org-member-form",.hidden=hidden,.nhidden=5,.inputs=in,.ninputs=1};
  ft_form(stdout,&f);}else fputs("Ask another administrator to change your own access.",stdout);ui_close(stdout,"td");ui_close(stdout,"tr");}
 ui_close(stdout,"tbody");ui_close(stdout,"table");ui_close(stdout,"div");ui_close(stdout,"section");
 done:sqlite3_finalize(q);if(db)sqlite3_close(db);
}
/* The operations this owner takes, resolved once (strcmp order). */
#define OG_OPS_LIST(X) \
    X("create", OGO_CREATE) \
    X("invite", OGO_INVITE) \
    X("revoke", OGO_REVOKE) \
    X("subaccount-create", OGO_SUBACCOUNT_CREATE)
BF_COMMAND_TABLE(og_ops, OG_OPS_LIST);
static int serve_organization_workspace(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *body=r->body; const char *raw=r->raw; const char *host=r->host; const char *token=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)query;(void)body;(void)raw;(void)host;(void)token;(void)auth_db;
 int join=bf_request_is(r,"join");int post=!strcmp(method,"POST");if(!post&&strcmp(method,"GET")){og_notice(405,"Method unavailable","Open this page or submit one of its reviewed forms.");return 1;}
 OgForm form;if(!og_form(post?body:query,&form)){og_notice(400,"Request not understood","Review the current page. Repeated or unsupported fields were refused.");return 1;}if(post&&!og_origin(raw,host)){og_notice(403,"Change refused","Submit the form from this exact secure workspace.");return 1;}
 char nonce[65];if(!og_random(nonce)){og_notice(503,"Try again","Could not prepare a secure request.");return 1;}StudioViewer viewer;int signed_in=session_identity(token,auth_db,&viewer)&&viewer.id>0;
 if(join){
  if(!*form.secret)og_pending(raw,form.secret);
  if(!signed_in&&post&&og_token(form.secret)&&!strcmp(form.operation,"review")){
   int local=host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10));printf("HTTP/1.1 303 See Other\r\nLocation: /login?next=%%2Forganizations%%2Fjoin\r\nSet-Cookie: aurekai_org_invite=%s; Path=/organizations/join; Max-Age=900; HttpOnly; SameSite=Lax%s\r\nCache-Control: private, no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",form.secret,local?"":"; Secure");return 1;
  }
  if(signed_in&&og_token(form.secret)){
   if(post&&strcmp(form.operation,"review")&&strcmp(form.operation,"accept")){og_notice(400,"Invalid invitation action","Review the invitation before accepting.");return 1;}
   const char *args[]={form.secret};OgResult result;if(!og_call(auth_db,token,post&&!strcmp(form.operation,"accept")?"accept":"review",args,1,&result)){og_notice(503,"Invitation unavailable","The native invitation owner did not return a complete response.");return 1;}if(result.status>=400){og_notice(result.status,"Invitation not accepted",result.message);return 1;}
   if(result.count!=1||result.rows[0].count!=4){og_notice(503,"Invitation unavailable","The native review was incomplete.");return 1;}OgRow *row=&result.rows[0];og_start_inner(200,post&&!strcmp(form.operation,"accept")?"Organization joined":"Review your invitation",nonce,post&&!strcmp(form.operation,"accept"));ui_open(stdout,"h2",NULL);html(row->value[1]);ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Access: ");ui_open(stdout,"strong",NULL);html(row->value[2]);ui_close(stdout,"strong");ui_close(stdout,"p");ui_open(stdout,"p",NULL);html(result.message);ui_close(stdout,"p");
   if(post&&!strcmp(form.operation,"accept"))og_open_links(row->value[0],"");else{const FtField hidden[]={{"operation","accept"},{"secret",form.secret}};
    const FtForm f={.id="og-accept",.action="/organizations/join",.label="Accept organization invitation",.hook="org-accept",.hidden=hidden,.nhidden=2};
    ft_form(stdout,&f);}og_end();return 1;
  }
  og_start(200,"Open an organization invitation",nonce);fputs("<p>The invitation must be accepted by its intended account. It does not copy personal work or sign you up for payments.</p><form method=\"post\" action=\"/organizations/join\" id=\"org-invite-review\">",stdout);psetup_hidden("operation","review");fputs("<label>Invitation code<input name=\"secret\" id=\"org-invite-code\" pattern=\"[a-f0-9]{64}\" minlength=\"64\" maxlength=\"64\" required autocomplete=\"off\"></label><button>Review invitation</button></form><script nonce=\"",stdout);html(nonce);fputs("\">(()=>{const code=location.hash.slice(1);if(/^[a-f0-9]{64}$/.test(code)){document.getElementById('org-invite-code').value=code;history.replaceState(null,'',location.pathname);document.getElementById('org-invite-review').requestSubmit();}})();</script>",stdout);og_end();return 1;
 }
 if(!signed_in){if(post){og_notice(401,"Sign in first","No organization or invitation was created.");return 1;}og_start(200,"Build your organization",nonce);ui_open(stdout,"p",NULL);ui_copy(stdout,"Keep using public pages and tools without an organization. Sign in when you want a shared destination you can configure and invite people into.");ui_close(stdout,"p");ui_start(stdout,"a");ui_flag(stdout,"data-org-signin");ui_attr_open(stdout,"href");ui_copy(stdout,"/login?next=%2Forganizations");if(*form.pack){fputs("%3Fpack%3D",stdout);url_query_value(form.pack);}fputs("\">Sign in or create an account</a>",stdout);og_end();return 1;}
 if(post&&!session_self_authoring(token,auth_db)){og_notice(403,"Read-only session","This session cannot change organization access.");return 1;}
 if(post&&!strcmp(form.operation,"member")){
  if(!*form.organization||!session_institution_authorized(token,auth_db,form.organization,"admin")){og_notice(403,"Access change refused","Current administration of the selected organization is required.");return 1;}
  const char *bin=auth_binary();char output[2048],diagnostic[512],secretline[256];snprintf(secretline,sizeof(secretline),"%s\n",token);char *const args[]={(char *)bin,"--db",(char *)auth_db,"institution-member-set-stdin","--institution",form.organization,"--email",form.email,"--role",form.role,"--expected-role",form.expected,"--expected-expiry",form.expiry,NULL};int rc=auth_call_split(args,secretline,output,sizeof(output),diagnostic,sizeof(diagnostic));
  if(rc){og_notice(409,"Access was not changed","Current authority or the reviewed membership changed. Reload the people list before trying again.");return 1;}
  printf("HTTP/1.1 303 See Other\r\nLocation: /organizations?organization=%s\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",form.organization);return 1;
 }
 if(post&&!strcmp(form.operation,"provider-access")){
  char customer[41],kind[160],access[1024];const PaProvider *pp=pa_find(form.provider);int wire=!strcmp(form.mode,"wire");
  if(!*form.organization||!session_institution_authorized(token,auth_db,form.organization,"admin")){og_notice(403,"Access refused","Current administration of the selected organization is required.");return 1;}
  if(strcmp(form.mode,"reveal")&&strcmp(form.mode,"scoped")&&!wire){og_notice(400,"Choose how to receive access","Use one of the access buttons on the organization page.");return 1;}
  if(!pa_mode_on(form.mode)){og_notice(403,"This way of handing over access is turned off","Ask the workspace owner, or use another access button.");return 1;}
  if(!pp||!pa_has_access(pp->slug)||!pa_customer(form.organization,customer)||!pa_access(pp->slug,customer,form.mode,kind,sizeof(kind),access,sizeof(access))){og_notice(409,"No access to hand over",wire?"Open this organization's Vercel project first, then try again.":"Open the account first, or this service has no login to give yet.");return 1;}
  og_start(200,"Your organization's access",nonce);ui_start(stdout,"p");ui_flag(stdout,"data-provider-granted");ui_gt(stdout);html(pp->name);fputs(": ",stdout);html(kind);fputs(".</p>",stdout);
  if(wire){ui_start(stdout,"p");ui_flag(stdout,"data-provider-wired");ui_gt(stdout);ui_copy(stdout,"It is now in this organization's Vercel project as the encrypted variable ");ui_open(stdout,"code",NULL);html(access);ui_close(stdout,"code");ui_copy(stdout,". Nobody was shown it, and Aurekai keeps no copy.");ui_close(stdout,"p");}
  else{ui_open(stdout,"p",NULL);ui_copy(stdout,"Copy it now. Aurekai does not keep a copy, and this page is not saved.");ui_close(stdout,"p");ui_start(stdout,"pre");ui_flag(stdout,"data-provider-secret");ui_gt(stdout);html(access);ui_close(stdout,"pre");}
  memset(access,0,sizeof(access));ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/organizations?organization=");url_query_value(form.organization);fputs("\">Back to the organization</a>",stdout);og_end();return 1;
 }
 if(post&&(!strcmp(form.operation,"provider-open")||!strcmp(form.operation,"provider-close"))){
  char customer[41],id[128];const PaProvider *pp=pa_find(form.provider);
  if(!*form.organization||!session_institution_authorized(token,auth_db,form.organization,"admin")){og_notice(403,"Provider account refused","Current administration of the selected organization is required.");return 1;}
  if(!pp||!pa_customer(form.organization,customer)){og_notice(400,"Unknown service","Choose a service listed on the organization page.");return 1;}
  int opening=!strcmp(form.operation,"provider-open"),state=pa_run(opening?"open":"close",pp->slug,customer,id,sizeof(id));
  if(state==-2){og_notice(429,"No room for another account","Aurekai's account at this service is full for now. Nothing was opened; try again later or ask the workspace owner.");return 1;}
  if(state<0||state!=opening){og_notice(502,opening?"Account not opened":"Account not closed","The service did not confirm the change. Reload the organization page to see what it holds now.");return 1;}
  printf("HTTP/1.1 303 See Other\r\nLocation: /organizations?organization=%s\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",form.organization);return 1;
 }
 OgResult result;int have_result=0;
 if(post){const char *args[5];unsigned count=0;const char *op=form.operation;
    int opid=bf_command_id(og_ops,BF_COMMAND_COUNT(og_ops),op,-1);
  if(opid==OGO_CREATE){args[0]=form.name;args[1]=form.nonce;count=2;}
  else if(opid==OGO_SUBACCOUNT_CREATE){if(!*form.organization||!session_institution_authorized(token,auth_db,form.organization,"admin")){og_notice(403,"Child workspace refused","Current parent organization administration is required.");return 1;}args[0]=form.organization;args[1]=form.name;args[2]=form.nonce;count=3;}
  else if(opid==OGO_INVITE){args[0]=form.organization;args[1]=form.email;args[2]=form.role;args[3]=form.hours;args[4]=form.secret;count=5;}
  else if(opid==OGO_REVOKE){args[0]=form.organization;args[1]=form.digest;args[2]=form.revision;count=3;}
  else{og_notice(400,"Unknown operation","Use one of the current reviewed forms.");return 1;}
  if(!og_call(auth_db,token,op,args,count,&result)){og_notice(503,"Organization owner unavailable","No completed response was received. Retry the same reviewed request before creating another.");return 1;}if(result.status>=400){og_notice(result.status,"Change not completed",result.message);return 1;}have_result=1;if(opid==OGO_CREATE&&result.count==1)snprintf(form.organization,sizeof(form.organization),"%s",result.rows[0].value[0]);
 }
 if(*form.organization&&!session_institution_authorized(token,auth_db,form.organization,"read")){og_notice(403,"Organization unavailable","This account has no current access to that destination.");return 1;}
 og_start(200,*form.organization?"Organization workspace":"Your organizations",nonce);
 if(have_result){ui_start(stdout,"p");ui_attr(stdout,"class","result");ui_attr(stdout,"role","status");ui_gt(stdout);html(result.message);ui_close(stdout,"p");if(!strcmp(form.operation,"invite")&&result.count==2&&result.rows[1].count==4){ui_open(stdout,"p",NULL);ui_copy(stdout,"Send this private link to ");html(form.email);fputs(". It expires at ",stdout);html(result.rows[0].value[3]);fputs(".</p><a class=\"invite-link\" data-org-invitation href=\"/organizations/join#",stdout);html(result.rows[1].value[2]);fputs("\">Open or copy the private invitation link</a>",stdout);}}
 if(*form.organization){
  sqlite3 *names=NULL;sqlite3_stmt *name=NULL;
  if(bf_store_open(auth_db, &names, SQLITE_OPEN_READONLY, &bf_store_policy_auth)==SQLITE_OK&&sqlite3_prepare_v2(names,"SELECT label FROM institution_origins WHERE institution=?",-1,&name,NULL)==SQLITE_OK){sqlite3_bind_text(name,1,form.organization,-1,SQLITE_STATIC);if(sqlite3_step(name)==SQLITE_ROW){ui_start(stdout,"h2");ui_flag(stdout,"data-org-name");ui_gt(stdout);html((const char *)sqlite3_column_text(name,0));ui_close(stdout,"h2");}}sqlite3_finalize(name);if(names)sqlite3_close(names);
  ui_open(stdout,"p",NULL);ui_copy(stdout,"Destination: ");ui_open(stdout,"code",NULL);html(form.organization);ui_close(stdout,"code");ui_close(stdout,"p");og_open_links(form.organization,form.pack);og_members(form.organization,token,auth_db);og_subaccounts(form.organization,token,auth_db);
  if(session_institution_authorized(token,auth_db,form.organization,"admin")){
   og_subaccount_create_form(form.organization,nonce);
   pa_section(form.organization,nonce);
   ui_open(stdout,"section",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Invite someone");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Choose a specific account email and access level. The recipient must sign in and accept the private invitation. Nothing is emailed automatically.");ui_close(stdout,"p");
   {const FtField hidden[]={{"operation","invite"},{"organization",form.organization},{"secret",nonce}};
    static const FtOption roles[]={{"read","Read shared work",0},{"write","Edit shared work",0},{"admin","Manage the workspace and people",0}};
    static const FtOption hours[]={{"24","In 24 hours",0},{"72","In 3 days",0},{"168","In 7 days",0}};
    static const FtInput in[]={{.label="Recipient email",.type="email",.name="email",.maxlength="300",.required=1},
                               {.label="Access",.type="select",.name="role",.options=roles,.noptions=3},
                               {.label="Invitation expires",.type="select",.name="hours",.options=hours,.noptions=3}};
    const FtForm f={.id="og-invite",.action="/organizations",.label="Prepare private invitation",.hook="org-invite",.hidden=hidden,.nhidden=3,.inputs=in,.ninputs=3};
    ft_form(stdout,&f);}
   ui_close(stdout,"section");
   const char *args[]={form.organization,form.after};OgResult invites;if(og_call(auth_db,token,"invitations",args,2,&invites)&&invites.status==200){ui_open(stdout,"section",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Invitations");ui_close(stdout,"h2");if(!invites.count)ui_text(stdout, "p", NULL, "No invitations have been created for this organization.");for(unsigned n=0;n<invites.count;n++){OgRow *row=&invites.rows[n];if(row->count!=6)continue;ui_open(stdout,"article",NULL);ui_open(stdout,"p",NULL);html(row->value[1]);fputs(" \u00B7 ",stdout);html(row->value[2]);fputs(" \u00B7 ",stdout);html(row->value[3]);fputs(" \u00B7 expires ",stdout);html(row->value[4]);ui_close(stdout,"p");if(!strcmp(row->value[3],"pending")){const FtField hidden[]={{"operation","revoke"},{"organization",form.organization},{"digest",row->value[0]},{"revision",row->value[5]}};
  const FtForm f={.id="og-revoke",.action="/organizations",.label="Revoke this invitation",.hook="org-revoke",.hidden=hidden,.nhidden=4};
  ft_form(stdout,&f);}ui_close(stdout,"article");}if(*invites.cursor){ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/organizations?organization=");url_query_value(form.organization);fputs("&amp;after=",stdout);url_query_value(invites.cursor);fputs("\">Next invitations</a>",stdout);}ui_close(stdout,"section");}
  }
 }else{
  const char *args[]={form.after};OgResult list;if(og_call(auth_db,token,"list",args,1,&list)&&list.status==200){ui_open(stdout,"div","org-grid");for(unsigned n=0;n<list.count;n++){OgRow *row=&list.rows[n];if(row->count!=4)continue;ui_start(stdout,"section");ui_attr_open(stdout,"data-organization");html(row->value[0]);fputs("\" data-org-role=\"",stdout);html(row->value[2]);fputs("\"><h2>",stdout);html(row->value[1]);ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your access: ");html(row->value[2]);ui_close(stdout,"p");og_open_links(row->value[0],form.pack);ui_close(stdout,"section");}ui_close(stdout,"div");if(!list.count)ui_text(stdout, "p", NULL, "You do not have a direct organization membership yet. Your personal workspace remains available.");if(*list.cursor){ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/organizations?after=");url_query_value(list.cursor);fputs("\">Next organizations</a>",stdout);}}else {ui_start(stdout,"p");ui_attr(stdout,"role","alert");ui_gt(stdout);ui_copy(stdout,"Your organization listing is unavailable. Reload before making another workspace.");ui_close(stdout,"p");}og_new_form(nonce,form.pack);
 }
 og_end();return 1;
}
#endif
