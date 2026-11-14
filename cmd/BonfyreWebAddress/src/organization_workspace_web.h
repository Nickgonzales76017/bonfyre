#ifndef BONFYRE_ORGANIZATION_WORKSPACE_WEB_H
#define BONFYRE_ORGANIZATION_WORKSPACE_WEB_H
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
 fputs("<main class=\"og-shell\" data-org-page><nav class=\"og-tabs\" aria-label=\"Aurekai work\"><a href=\"/apps?context=self\">Your work</a><a href=\"/organizations\" aria-current=\"page\">Organizations</a><a href=\"/organizations/join\">Open an invitation</a></nav><p class=\"og-kicker\">Workspaces and organizations</p><h1>",stdout);html(title);fputs("</h1>",stdout);
}
static void og_start(int status,const char *title,const char *nonce){og_start_inner(status,title,nonce,0);printf("<script type=\"module\" nonce=\"%s\" src=\"/apps/workspace-members.mjs\"></script>",nonce);}
static void og_end(void){fputs("<p class=\"og-note\">Organization access does not share personal records, establish legal identity or authorize payments.</p></main>",stdout);og_nonced(BF_ESTATE_FOOT "</body></html>",og_page_nonce);}
static void og_notice(int status,const char *title,const char *message){char nonce[65];if(!og_random(nonce)){aw_error(503,"Could not prepare a secure page.\n");return;}og_start(status,title,nonce);fputs("<p role=\"alert\">",stdout);html(message);fputs("</p><a href=\"/organizations\">Return to organizations</a>",stdout);og_end();}
static int og_call(const char *db,const char *token,const char *op,const char **args,unsigned count,OgResult *out){
 memset(out,0,sizeof(*out));bf_mp_writer_t w;bf_mp_writer_init(&w,2048);int bad=bf_mp_pack_array(&w,4)||bf_mp_pack_uint(&w,1)||bf_mp_pack_str_cstr(&w,token?token:"")||bf_mp_pack_str_cstr(&w,op)||bf_mp_pack_array(&w,count);for(unsigned i=0;i<count&&!bad;i++)bad=bf_mp_pack_str_cstr(&w,args[i]);
 const char *bin=auth_binary();char *const argv[]={(char *)bin,"--db",(char *)db,"organization-stdin",NULL};unsigned char *body=NULL;size_t bytes=0;int rc=bad?-1:aw_native_result(argv,w.buf,w.len,&body,&bytes,1);bf_mp_writer_free(&w);if(rc<0||bytes>131072){free(body);return 0;}
 bf_mp_reader_t r;bf_mp_reader_init(&r,body,bytes);long long version,status;bf_mp_value_t v;int ok=ac_array(&r,5)&&ac_uint(&r,&version)&&version==1&&ac_uint(&r,&status)&&status>=200&&status<=599&&ac_text(&r,out->message,sizeof(out->message))&&bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count<=50;
 if(ok){out->status=(int)status;out->count=v.v.count;for(unsigned n=0;n<out->count&&ok;n++){if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count>6){ok=0;break;}OgRow *row=&out->rows[n];row->count=v.v.count;for(unsigned c=0;c<row->count&&ok;c++){bf_mp_reader_t value=r;if(bf_mp_read_next(&value,&v)==BF_MP_UINT&&v.v.u<INT_MAX){snprintf(row->value[c],321,"%llu",(unsigned long long)v.v.u);r=value;}else ok=ac_text(&r,row->value[c],321);}}if(ok)ok=ac_text(&r,out->cursor,sizeof(out->cursor))&&r.pos==r.len&&((rc==0)==(status<400));}free(body);return ok;
}
static void og_org_link(const char *id,const char *path){fputs(path,stdout);fputs(strchr(path,'?')?"&amp;":"?",stdout);fputs("context=institution%3A",stdout);url_query_value(id);}
static void og_open_links(const char *id,const char *pack){fputs("<p class=\"actions\"><a data-org-work href=\"",stdout);og_org_link(id,"/apps");fputs("\">Open shared work</a><a data-org-setup href=\"/packs/setup?context=institution%3A",stdout);url_query_value(id);if(pack&&*pack){fputs("&amp;pack=",stdout);url_query_value(pack);}fputs("\">Choose and configure applications</a><a href=\"/organizations?organization=",stdout);url_query_value(id);fputs("\">People and invitations</a></p>",stdout);}
/* One Auth-owned child listing; never grant child login or silently install apps. */
static void og_subaccounts(const char *parent,const char *token,const char *auth_db){
 const char *args[]={parent,""};OgResult result;
 fputs("<section data-org-subaccounts><h2>Subaccounts</h2><p>Separate destinations for publications, teams or clients. Creating one does not share private data, admit members, install applications or publish content.</p>",stdout);
 if(!og_call(auth_db,token,"subaccounts",args,2,&result)||result.status!=200){fputs("<p role=\"alert\">The child workspace list is unavailable.</p></section>",stdout);return;}
 if(!result.count)fputs("<p>No subaccounts yet.</p>",stdout);
 for(unsigned i=0;i<result.count;i++){OgRow *row=&result.rows[i];if(row->count!=4)continue;
  fputs("<article data-org-subaccount=\"",stdout);html(row->value[0]);fputs("\"><h3>",stdout);html(row->value[1]);fputs("</h3><p>Subaccount <code>",stdout);html(row->value[0]);fputs("</code></p><p>Parent organization: <code>",stdout);html(parent);fputs("</code></p><p>Separate application bindings and editorial permissions must be explicitly admitted.</p></article>",stdout);
 }
 if(*result.cursor)fputs("<p>Additional child destinations exist. Use the native paginated organization owner to inspect them.</p>",stdout);
 fputs("</section>",stdout);
}
static void og_subaccount_create_form(const char *parent,const char *nonce){
 fputs("<section data-org-subaccount-creation><h2>Create a subaccount</h2><p>Start a distinct publication, client or team under this organization. Access and publishing remain separately governed.</p><form method=\"post\" action=\"/organizations\" data-org-subaccount-create>",stdout);
 psetup_hidden("operation","subaccount-create");psetup_hidden("organization",parent);psetup_hidden("nonce",nonce);
 fputs("<label>Subaccount name<input name=\"name\" maxlength=\"160\" required></label><button>Create subaccount</button></form></section>",stdout);
}
static void og_new_form(const char *nonce,const char *pack){fputs("<section><h2>Create an organization workspace</h2><p>Name the business, team or collective you are starting here. Its private work stays separate from your personal workspace. This does not claim an existing company's identity.</p><form method=\"post\" action=\"/organizations\" data-org-create>",stdout);psetup_hidden("operation","create");psetup_hidden("nonce",nonce);psetup_hidden("pack",pack);fputs("<label>Organization name<input name=\"name\" maxlength=\"160\" required autocomplete=\"organization\"></label><button>Create organization workspace</button></form></section>",stdout);}
static int og_origin(const char *raw,const char *host){char origin[512],type[128],expected[512];int local=host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10));snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host?host:"");return aw_header(raw,"Origin",origin,sizeof(origin))&&!strcmp(origin,expected)&&aw_header(raw,"Content-Type",type,sizeof(type))&&!strcmp(type,"application/x-www-form-urlencoded");}
static int og_pending(const char *raw,char secret[65]){char cookies[8192];if(!aw_header(raw,"Cookie",cookies,sizeof(cookies)))return 0;int found=0;for(char *p=cookies;*p;){while(*p==' ')p++;char *end=strchr(p,';');if(!end)end=p+strlen(p);const char *key="aurekai_org_invite=";size_t n=strlen(key);if((size_t)(end-p)>=n&&!strncmp(p,key,n)){if(found++||(size_t)(end-p)!=n+64)return 0;memcpy(secret,p+n,64);secret[64]=0;}p=*end?end+1:end;}return found==1&&og_token(secret);}
static void og_members(const char *id,const char *token,const char *auth_db){
 if(!session_institution_authorized(token,auth_db,id,"admin"))return;
 sqlite3 *db=NULL;sqlite3_stmt *q=NULL;if(sqlite3_open_v2(auth_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
 const char *sql="SELECT u.id,u.name,u.email,g.role,COALESCE(g.expires_at,''),g.active FROM institution_grants g JOIN users u ON u.id=g.user_id WHERE g.institution=? ORDER BY u.id LIMIT 101";
 if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);StudioViewer me;if(!session_identity(token,auth_db,&me))goto done;
 fputs("<section><h2>People and access</h2><p>Read permits inspection, write permits editing, and admin permits configuration and access management. Changes use the existing Auth owner and recheck the reviewed role and expiry.</p><div class=\"scroll\"><table><thead><tr><th>Person</th><th>Access</th><th>Reviewed change</th></tr></thead><tbody>",stdout);
 int step,count=0;while((step=sqlite3_step(q))==SQLITE_ROW){if(count++==100){fputs("<tr><td colspan=\"3\">Showing the first 100 members; use the native membership owner for the full paginated list.</td></tr>",stdout);break;}const char *email=(const char *)sqlite3_column_text(q,2),*role=(const char *)sqlite3_column_text(q,3),*expiry=(const char *)sqlite3_column_text(q,4);int active=sqlite3_column_int(q,5);fputs("<tr data-org-member><td>",stdout);html((const char *)sqlite3_column_text(q,1));fputs("<small>",stdout);html(email);fputs("</small></td><td>",stdout);html(active?role:"Removed");if(*expiry){fputs("<small>Expires ",stdout);html(expiry);fputs("</small>",stdout);}fputs("</td><td>",stdout);
 if(sqlite3_column_int64(q,0)!=me.id){fputs("<form method=\"post\" action=\"/organizations\" data-org-member-form>",stdout);psetup_hidden("operation","member");psetup_hidden("organization",id);psetup_hidden("email",email);psetup_hidden("expected",active?role:"none");psetup_hidden("expiry",active?expiry:"");fputs("<label>Access<select name=\"role\">",stdout);const char *roles[]={"none","read","write","admin"};for(unsigned n=0;n<4;n++){fputs("<option value=\"",stdout);html(roles[n]);if(!strcmp(roles[n],active?role:"none"))fputs("\" selected>",stdout);else fputs("\">",stdout);html(roles[n]);fputs("</option>",stdout);}fputs("</select></label><button>Apply reviewed access</button></form>",stdout);}else fputs("Ask another administrator to change your own access.",stdout);fputs("</td></tr>",stdout);}
 fputs("</tbody></table></div></section>",stdout);
 done:sqlite3_finalize(q);if(db)sqlite3_close(db);
}
static int serve_organization_workspace(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *token,const char *auth_db){
 int join=!strcmp(target,"/organizations/join");if(!join&&strcmp(target,"/organizations"))return 0;int post=!strcmp(method,"POST");if(!post&&strcmp(method,"GET")){og_notice(405,"Method unavailable","Open this page or submit one of its reviewed forms.");return 1;}
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
   if(result.count!=1||result.rows[0].count!=4){og_notice(503,"Invitation unavailable","The native review was incomplete.");return 1;}OgRow *row=&result.rows[0];og_start_inner(200,post&&!strcmp(form.operation,"accept")?"Organization joined":"Review your invitation",nonce,post&&!strcmp(form.operation,"accept"));fputs("<h2>",stdout);html(row->value[1]);fputs("</h2><p>Access: <strong>",stdout);html(row->value[2]);fputs("</strong></p><p>",stdout);html(result.message);fputs("</p>",stdout);
   if(post&&!strcmp(form.operation,"accept"))og_open_links(row->value[0],"");else{fputs("<form method=\"post\" action=\"/organizations/join\" data-org-accept>",stdout);psetup_hidden("operation","accept");psetup_hidden("secret",form.secret);fputs("<button>Accept organization invitation</button></form>",stdout);}og_end();return 1;
  }
  og_start(200,"Open an organization invitation",nonce);fputs("<p>The invitation must be accepted by its intended account. It does not copy personal work or sign you up for payments.</p><form method=\"post\" action=\"/organizations/join\" id=\"org-invite-review\">",stdout);psetup_hidden("operation","review");fputs("<label>Invitation code<input name=\"secret\" id=\"org-invite-code\" pattern=\"[a-f0-9]{64}\" minlength=\"64\" maxlength=\"64\" required autocomplete=\"off\"></label><button>Review invitation</button></form><script nonce=\"",stdout);html(nonce);fputs("\">(()=>{const code=location.hash.slice(1);if(/^[a-f0-9]{64}$/.test(code)){document.getElementById('org-invite-code').value=code;history.replaceState(null,'',location.pathname);document.getElementById('org-invite-review').requestSubmit();}})();</script>",stdout);og_end();return 1;
 }
 if(!signed_in){if(post){og_notice(401,"Sign in first","No organization or invitation was created.");return 1;}og_start(200,"Build your organization",nonce);fputs("<p>Keep using public pages and tools without an organization. Sign in when you want a shared destination you can configure and invite people into.</p><a data-org-signin href=\"/login?next=%2Forganizations",stdout);if(*form.pack){fputs("%3Fpack%3D",stdout);url_query_value(form.pack);}fputs("\">Sign in or create an account</a>",stdout);og_end();return 1;}
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
  og_start(200,"Your organization's access",nonce);fputs("<p data-provider-granted>",stdout);html(pp->name);fputs(": ",stdout);html(kind);fputs(".</p>",stdout);
  if(wire){fputs("<p data-provider-wired>It is now in this organization's Vercel project as the encrypted variable <code>",stdout);html(access);fputs("</code>. Nobody was shown it, and Aurekai keeps no copy.</p>",stdout);}
  else{fputs("<p>Copy it now. Aurekai does not keep a copy, and this page is not saved.</p><pre data-provider-secret>",stdout);html(access);fputs("</pre>",stdout);}
  memset(access,0,sizeof(access));fputs("<a href=\"/organizations?organization=",stdout);url_query_value(form.organization);fputs("\">Back to the organization</a>",stdout);og_end();return 1;
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
  if(!strcmp(op,"create")){args[0]=form.name;args[1]=form.nonce;count=2;}
  else if(!strcmp(op,"subaccount-create")){if(!*form.organization||!session_institution_authorized(token,auth_db,form.organization,"admin")){og_notice(403,"Child workspace refused","Current parent organization administration is required.");return 1;}args[0]=form.organization;args[1]=form.name;args[2]=form.nonce;count=3;}
  else if(!strcmp(op,"invite")){args[0]=form.organization;args[1]=form.email;args[2]=form.role;args[3]=form.hours;args[4]=form.secret;count=5;}
  else if(!strcmp(op,"revoke")){args[0]=form.organization;args[1]=form.digest;args[2]=form.revision;count=3;}
  else{og_notice(400,"Unknown operation","Use one of the current reviewed forms.");return 1;}
  if(!og_call(auth_db,token,op,args,count,&result)){og_notice(503,"Organization owner unavailable","No completed response was received. Retry the same reviewed request before creating another.");return 1;}if(result.status>=400){og_notice(result.status,"Change not completed",result.message);return 1;}have_result=1;if(!strcmp(op,"create")&&result.count==1)snprintf(form.organization,sizeof(form.organization),"%s",result.rows[0].value[0]);
 }
 if(*form.organization&&!session_institution_authorized(token,auth_db,form.organization,"read")){og_notice(403,"Organization unavailable","This account has no current access to that destination.");return 1;}
 og_start(200,*form.organization?"Organization workspace":"Your organizations",nonce);
 if(have_result){fputs("<p class=\"result\" role=\"status\">",stdout);html(result.message);fputs("</p>",stdout);if(!strcmp(form.operation,"invite")&&result.count==2&&result.rows[1].count==4){fputs("<p>Send this private link to ",stdout);html(form.email);fputs(". It expires at ",stdout);html(result.rows[0].value[3]);fputs(".</p><a class=\"invite-link\" data-org-invitation href=\"/organizations/join#",stdout);html(result.rows[1].value[2]);fputs("\">Open or copy the private invitation link</a>",stdout);}}
 if(*form.organization){
  sqlite3 *names=NULL;sqlite3_stmt *name=NULL;
  if(sqlite3_open_v2(auth_db,&names,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK&&sqlite3_prepare_v2(names,"SELECT label FROM institution_origins WHERE institution=?",-1,&name,NULL)==SQLITE_OK){sqlite3_bind_text(name,1,form.organization,-1,SQLITE_STATIC);if(sqlite3_step(name)==SQLITE_ROW){fputs("<h2 data-org-name>",stdout);html((const char *)sqlite3_column_text(name,0));fputs("</h2>",stdout);}}sqlite3_finalize(name);if(names)sqlite3_close(names);
  fputs("<p>Destination: <code>",stdout);html(form.organization);fputs("</code></p>",stdout);og_open_links(form.organization,form.pack);og_members(form.organization,token,auth_db);og_subaccounts(form.organization,token,auth_db);
  if(session_institution_authorized(token,auth_db,form.organization,"admin")){
   og_subaccount_create_form(form.organization,nonce);
   pa_section(form.organization,nonce);
   fputs("<section><h2>Invite someone</h2><p>Choose a specific account email and access level. The recipient must sign in and accept the private invitation. Nothing is emailed automatically.</p><form method=\"post\" action=\"/organizations\" data-org-invite>",stdout);psetup_hidden("operation","invite");psetup_hidden("organization",form.organization);psetup_hidden("secret",nonce);fputs("<label>Recipient email<input name=\"email\" type=\"email\" maxlength=\"300\" required></label><label>Access<select name=\"role\"><option value=\"read\">Read shared work</option><option value=\"write\">Edit shared work</option><option value=\"admin\">Manage the workspace and people</option></select></label><label>Invitation expires<select name=\"hours\"><option value=\"24\">In 24 hours</option><option value=\"72\">In 3 days</option><option value=\"168\">In 7 days</option></select></label><button>Prepare private invitation</button></form></section>",stdout);
   const char *args[]={form.organization,form.after};OgResult invites;if(og_call(auth_db,token,"invitations",args,2,&invites)&&invites.status==200){fputs("<section><h2>Invitations</h2>",stdout);if(!invites.count)fputs("<p>No invitations have been created for this organization.</p>",stdout);for(unsigned n=0;n<invites.count;n++){OgRow *row=&invites.rows[n];if(row->count!=6)continue;fputs("<article><p>",stdout);html(row->value[1]);fputs(" \u00B7 ",stdout);html(row->value[2]);fputs(" \u00B7 ",stdout);html(row->value[3]);fputs(" \u00B7 expires ",stdout);html(row->value[4]);fputs("</p>",stdout);if(!strcmp(row->value[3],"pending")){fputs("<form method=\"post\" action=\"/organizations\" data-org-revoke>",stdout);psetup_hidden("operation","revoke");psetup_hidden("organization",form.organization);psetup_hidden("digest",row->value[0]);psetup_hidden("revision",row->value[5]);fputs("<button>Revoke this invitation</button></form>",stdout);}fputs("</article>",stdout);}if(*invites.cursor){fputs("<a href=\"/organizations?organization=",stdout);url_query_value(form.organization);fputs("&amp;after=",stdout);url_query_value(invites.cursor);fputs("\">Next invitations</a>",stdout);}fputs("</section>",stdout);}
  }
 }else{
  const char *args[]={form.after};OgResult list;if(og_call(auth_db,token,"list",args,1,&list)&&list.status==200){fputs("<div class=\"org-grid\">",stdout);for(unsigned n=0;n<list.count;n++){OgRow *row=&list.rows[n];if(row->count!=4)continue;fputs("<section data-organization=\"",stdout);html(row->value[0]);fputs("\" data-org-role=\"",stdout);html(row->value[2]);fputs("\"><h2>",stdout);html(row->value[1]);fputs("</h2><p>Your access: ",stdout);html(row->value[2]);fputs("</p>",stdout);og_open_links(row->value[0],form.pack);fputs("</section>",stdout);}fputs("</div>",stdout);if(!list.count)fputs("<p>You do not have a direct organization membership yet. Your personal workspace remains available.</p>",stdout);if(*list.cursor){fputs("<a href=\"/organizations?after=",stdout);url_query_value(list.cursor);fputs("\">Next organizations</a>",stdout);}}else fputs("<p role=\"alert\">Your organization listing is unavailable. Reload before making another workspace.</p>",stdout);og_new_form(nonce,form.pack);
 }
 og_end();return 1;
}
#endif
