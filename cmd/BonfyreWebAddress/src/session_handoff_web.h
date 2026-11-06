#ifndef BONFYRE_SESSION_HANDOFF_WEB_H
#define BONFYRE_SESSION_HANDOFF_WEB_H
/* One-use native Auth transfer. The receiving territory owns a host-only
 * nonce; no bearer or password travels through browser redirects. */
static const char *sso_host_for(const char *t){if(!t)return NULL;if(!strcmp(t,"work"))return "aurekai.work";if(!strcmp(t,"life"))return "aurekai.life";if(!strcmp(t,"live"))return "aurekai.live";if(!strcmp(t,"shop"))return "aurekai.shop";return NULL;}
static const char *sso_territory(const char *h){if(!h)return NULL;if(!strcmp(h,"aurekai.work"))return "work";if(!strcmp(h,"aurekai.life"))return "life";if(!strcmp(h,"aurekai.live"))return "live";if(!strcmp(h,"aurekai.shop"))return "shop";return NULL;}
static int sso_return_address(const char *candidate,const char *host,char *out,size_t cap){
 if(bf_auth_return_address(candidate,host,out,cap))return 1;
 const char *territory=sso_territory(host),*matrix=getenv("BONFYRE_MATRIX_DB");
 if(!candidate||strncmp(candidate,"https://",8)||!territory||!matrix||strlen(candidate)>=cap)return 0;
 const char *path=strchr(candidate+8,'/');char domain[256],normalized[256],suffix[48];
 size_t n=path?(size_t)(path-candidate-8):strlen(candidate+8);
 if(!n||n>=sizeof(domain))return 0;memcpy(domain,candidate+8,n);domain[n]=0;
 snprintf(suffix,sizeof(suffix),".aurekai.%s",territory);size_t tail=strlen(suffix);
 if(n<=tail||strcmp(domain+n-tail,suffix)||!lower_host(domain,normalized)||strcmp(domain,normalized)||
    strspn(domain,"abcdefghijklmnopqrstuvwxyz0123456789-.")!=n||!bf_auth_local_path(path?path:"/"))return 0;
 sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int ok=0;
 if(sqlite3_open_v2(matrix,&db,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK&&sqlite3_prepare_v2(db,
    "SELECT 1 FROM matrix_domains WHERE hostname=?1 AND territory=?2 LIMIT 1",-1,&st,NULL)==SQLITE_OK){
  sqlite3_bind_text(st,1,domain,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,territory,-1,SQLITE_TRANSIENT);ok=sqlite3_step(st)==SQLITE_ROW;
 }sqlite3_finalize(st);if(db)sqlite3_close(db);if(ok)snprintf(out,cap,"%s",candidate);return ok;
}
static int sso_hex(const char *s,size_t n){if(!s||strlen(s)!=n)return 0;for(size_t i=0;i<n;i++)if(!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f')))return 0;return 1;}
static int sso_random(char out[65]){unsigned char bytes[32];FILE *f=fopen("/dev/urandom","rb");if(!f)return 0;size_t n=fread(bytes,1,sizeof(bytes),f);fclose(f);if(n!=sizeof(bytes))return 0;static const char hex[]="0123456789abcdef";for(size_t i=0;i<32;i++){out[2*i]=hex[bytes[i]>>4];out[2*i+1]=hex[bytes[i]&15];}out[64]=0;memset(bytes,0,sizeof(bytes));return 1;}
static int sso_cookie(const char *raw,const char *name,char *out,size_t cap){if(!raw||!name||!out||cap<2)return 0;const char *p=strstr(raw,"\nCookie:");if(!p)p=strstr(raw,"\ncookie:");if(!p)return 0;p+=8;const char *end=strchr(p,'\n');if(!end)end=p+strlen(p);size_t n=strlen(name);out[0]=0;while(p<end){while(p<end&&(*p==' '||*p=='\t'||*p==';'))p++;const char *stop=p;while(stop<end&&*stop!=';'&&*stop!='\r')stop++;if((size_t)(stop-p)>n+1&&!memcmp(p,name,n)&&p[n]=='='){const char *v=p+n+1;size_t m=(size_t)(stop-v);if(m>=cap)return 0;memcpy(out,v,m);out[m]=0;return 1;}p=stop<end?stop+1:end;}return 0;}
static int sso_hex_path(const char *restrict s,char *restrict out,size_t cap){static const char h[]="0123456789abcdef";size_t n=strlen(s);if(n>512||2*n+1>cap)return 0;for(size_t i=0;i<n;i++){unsigned char c=(unsigned char)s[i];out[2*i]=h[c>>4];out[2*i+1]=h[c&15];}out[2*n]=0;return 1;}
static int sso_decode_path(const char *restrict s,char *restrict out,size_t cap){size_t n=strlen(s);if(!n||(n&1)||n>1024||n/2>=cap)return 0;for(size_t i=0;i<n;i+=2){unsigned a=s[i]>='0'&&s[i]<='9'?(unsigned)(s[i]-'0'):s[i]>='a'&&s[i]<='f'?(unsigned)(s[i]-'a'+10):255;unsigned b=s[i+1]>='0'&&s[i+1]<='9'?(unsigned)(s[i+1]-'0'):s[i+1]>='a'&&s[i+1]<='f'?(unsigned)(s[i+1]-'a'+10):255;if(a>15||b>15||!((a<<4)|b))return 0;out[i/2]=(char)((a<<4)|b);}out[n/2]=0;return 1;}
static void sso_redirect(const char *location){printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",location);}
static int serve_session_handoff(const char *method,const char *target,const char *bearer,const char *db,const char *host,const char *query){
 if(strcmp(target,"/session/switch")&&strcmp(target,"/session/continue")&&strcmp(target,"/session/authorize")&&strcmp(target,"/session/complete"))return 0;
 const char *territory=sso_territory(host);char state[65]={0},next[BF_AUTH_NEXT_MAX]={0},source[16]={0};
 if(strcmp(method,"GET")||!territory){html_error(400,"Unsupported sign-in route","Use a controlled Aurekai territory and its browser sign-in flow.");return 1;}
 if(!strcmp(target,"/session/switch")){
  char dest[16]={0},requested[BF_AUTH_NEXT_MAX]={0};
  if(bf_lex_form_query(query,"to",dest,sizeof(dest))!=1||!sso_host_for(dest)||
     bf_lex_form_query(query,"next",requested,sizeof(requested))!=1||
     !sso_return_address(requested,sso_host_for(dest),next,sizeof(next))){
   html_error(400,"Invalid territory destination","Choose one Aurekai territory and a valid return address.");return 1;
  }
  if(!strcmp(dest,territory)||!session_valid(bearer,db)){
   if(!strncmp(next,"https://",8))sso_redirect(next);
   else{char address[BF_AUTH_NEXT_MAX+64];snprintf(address,sizeof(address),"https://%s%s",sso_host_for(dest),next);sso_redirect(address);}
   return 1;
  }
  printf("HTTP/1.1 303 See Other\r\nLocation: https://%s/session/continue?source=%s&next=",sso_host_for(dest),territory);
  bf_auth_query(stdout,next);fputs("\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;
 }
 if(!strcmp(target,"/session/continue")){
  int has_source=bf_lex_form_query(query,"source",source,sizeof(source));
  if(has_source<0||(has_source&&(!sso_host_for(source)||!strcmp(source,territory)))){
   html_error(400,"Invalid sign-in source","Start from a different Aurekai territory.");return 1;
  }
  if(!has_source)snprintf(source,sizeof(source),"work");
  char requested[BF_AUTH_NEXT_MAX]={0};int supplied=bf_lex_form_query(query,"next",requested,sizeof(requested));
  if(supplied<0||(supplied&&!sso_return_address(requested,host,next,sizeof(next)))){html_error(400,"Invalid return address","Choose a current Aurekai address.");return 1;}
  if(!supplied)snprintf(next,sizeof(next),"/account");
  if(!has_source&&!strcmp(territory,"work")){
   if(session_valid(bearer,db)){sso_redirect(next);return 1;}
   fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);bf_auth_query(stdout,next);
   fputs("\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;
  }
  if(session_valid(bearer,db)){sso_redirect(next);return 1;}
  char encoded[BF_AUTH_NEXT_MAX*2+2]={0};
  if(!sso_random(state)||!sso_hex_path(next,encoded,sizeof(encoded))){html_error(503,"Sign-in handoff unavailable","A secure sign-in nonce could not be prepared.");return 1;}
  printf("HTTP/1.1 303 See Other\r\nLocation: https://%s/session/authorize?dest=%s&state=%s\r\n"
   "Set-Cookie: __Host-bonfyre_sso_state=%s; Path=/; Max-Age=180; Secure; HttpOnly; SameSite=Lax\r\n"
   "Set-Cookie: __Host-bonfyre_sso_next=%s; Path=/; Max-Age=180; Secure; HttpOnly; SameSite=Lax\r\n"
   "Cache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",sso_host_for(source),territory,state,state,encoded);return 1;
 }
 char dest[16]={0},code[65]={0},result[4096]={0};
 if(!strcmp(target,"/session/authorize")){
  if(bf_lex_form_query(query,"dest",dest,sizeof(dest))!=1||!sso_host_for(dest)||!strcmp(dest,territory)||
     bf_lex_form_query(query,"state",state,sizeof(state))!=1||!sso_hex(state,64)){html_error(400,"Invalid handoff","The destination and nonce must identify a controlled territory.");return 1;}
  if(!session_valid(bearer,db)){
   fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);
   char return_path[200];snprintf(return_path,sizeof(return_path),"/session/authorize?dest=%s&state=%s",dest,state);bf_auth_query(stdout,return_path);
   fputs("\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;
  }
  const char *bin=auth_binary();
  char *const argv[]={(char*)bin,"--db",(char*)db,"handoff-issue-stdin","--dest",dest,"--state",state,NULL};
  if(auth_call(argv,bearer,result,sizeof(result))!=0||!studio_json_field(result,"code",code,sizeof(code))||!sso_hex(code,64)){html_error(503,"Sign-in handoff refused","The authenticated session could not issue a transfer.");return 1;}
  printf("HTTP/1.1 303 See Other\r\nLocation: https://%s/session/complete?state=%s&code=%s\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",sso_host_for(dest),state,code);
  memset(code,0,sizeof(code));return 1;
 }
 if(bf_lex_form_query(query,"state",state,sizeof(state))!=1||!sso_hex(state,64)||
    bf_lex_form_query(query,"code",code,sizeof(code))!=1||!sso_hex(code,64)){html_error(400,"Invalid sign-in return","A valid one-time handoff is required.");return 1;}
 char cookie_state[80]={0},path_hex[BF_AUTH_NEXT_MAX*2+2]={0},decoded[BF_AUTH_NEXT_MAX]={0};
 if(!sso_cookie(page_request_raw,"__Host-bonfyre_sso_state",cookie_state,sizeof(cookie_state))||strcmp(cookie_state,state)||
    !sso_cookie(page_request_raw,"__Host-bonfyre_sso_next",path_hex,sizeof(path_hex))||!sso_decode_path(path_hex,decoded,sizeof(decoded))){
   html_error(401,"Sign-in return not recognized","This browser did not initiate the matching transfer.");return 1;
 }
 if(!sso_return_address(decoded,host,next,sizeof(next))){html_error(400,"Return address changed","The receiving address is no longer available.");return 1;}
 /* Do not replace another account that signed in during this ceremony. */
 if(session_valid(bearer,db)){sso_redirect(next);return 1;}
 const char *bin=auth_binary();
 char *const argv[]={(char*)bin,"--db",(char*)db,"handoff-consume-stdin","--dest",(char*)territory,"--state",state,NULL};
 if(auth_call(argv,code,result,sizeof(result))!=0){html_error(401,"Sign-in transfer expired","Return and initiate a new one-time sign-in transfer.");return 1;}
 char token[128]={0},domain[64]={0};
 if(!auth_response_token(result,token,sizeof(token))||!auth_cookie_domain(host,domain,sizeof(domain))){
  html_error(503,"Sign-in return unavailable","The receiving territory could not establish its session.");return 1;
 }
 printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\n"
  "Set-Cookie: bonfyre_session=%s; Domain=%s; Path=/; Max-Age=86400; Secure; HttpOnly; SameSite=Lax\r\n"
  "Set-Cookie: __Host-bonfyre_sso_state=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Lax\r\n"
  "Set-Cookie: __Host-bonfyre_sso_next=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Lax\r\n"
  "Cache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",next,token,domain);
 memset(token,0,sizeof(token));memset(code,0,sizeof(code));return 1;
}
#endif
