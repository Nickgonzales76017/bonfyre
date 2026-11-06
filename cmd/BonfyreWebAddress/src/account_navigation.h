#ifndef BONFYRE_ACCOUNT_NAVIGATION_H
#define BONFYRE_ACCOUNT_NAVIGATION_H
/* Auth owns principal -> actor binding; Matrix owns the public profile address.
 * Never infer one from a name or email, and never invent an address when the
 * explicit native bindings are missing or ambiguous. */
static char account_request_territory[24]="";
static int account_public_profile_at(long user_id,const char *auth_db,const char *territory,char *out,size_t cap) {
    sqlite3 *adb=NULL,*mdb=NULL;sqlite3_stmt *s=NULL;
    char actor[192]={0},institution[224]={0},host[256]={0},normalized[256];
    const char *matrix_db=getenv("BONFYRE_MATRIX_DB");int ok=0,rows=0;
    if(out&&cap)out[0]=0;
    if(user_id<1||!auth_db||!matrix_db||!out||cap<16)return 0;
    if(sqlite3_open_v2(auth_db,&adb,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(adb,
        "SELECT actor_subject FROM actor_closure_bindings WHERE user_id=?1 AND state='active' LIMIT 2",
        -1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_int64(s,1,user_id);
    while(sqlite3_step(s)==SQLITE_ROW){
        const char *value=(const char *)sqlite3_column_text(s,0);
        if(++rows>1||!value||!value[0]||strlen(value)>=sizeof(actor)||
           strspn(value,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:._-")!=strlen(value))goto done;
        snprintf(actor,sizeof(actor),"%s",value);
    }
    sqlite3_finalize(s);s=NULL;if(rows!=1)goto done;
    if(snprintf(institution,sizeof(institution),"inst:%s",actor)>=(int)sizeof(institution))goto done;
    for(size_t i=5;institution[i];i++)if(institution[i]==':'||institution[i]=='.')institution[i]='-';
    if(sqlite3_open_v2(matrix_db,&mdb,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(mdb,
        "SELECT d.hostname FROM matrix_domains d WHERE d.institution_id=?1 AND d.territory=?3 "
        "AND EXISTS(SELECT 1 FROM matrix_presences p WHERE p.subject_id=?2 AND p.territory=?3 "
        "AND p.scope='public' AND p.world='LIVE' AND p.representation='HTML' AND p.active=1) "
        "ORDER BY d.hostname LIMIT 2",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,institution,-1,SQLITE_STATIC);
    sqlite3_bind_text(s,2,actor,-1,SQLITE_STATIC);
    sqlite3_bind_text(s,3,territory,-1,SQLITE_STATIC);
    rows=0;
    while(sqlite3_step(s)==SQLITE_ROW){
        const char *value=(const char *)sqlite3_column_text(s,0);
        if(++rows>1||!value||!lower_host(value,normalized)||strcmp(value,normalized))goto done;
        snprintf(host,sizeof(host),"%s",normalized);
    }
    if(rows==1&&snprintf(out,cap,"https://%s/",host)<(int)cap)ok=1;
done:
    sqlite3_finalize(s);if(adb)sqlite3_close(adb);if(mdb)sqlite3_close(mdb);
    if(!ok&&out&&cap)out[0]=0;return ok;
}
static int account_public_profile(long user_id,const char *auth_db,char *out,size_t cap){
    if(strcmp(account_request_territory,"work")&&strcmp(account_request_territory,"life")&&strcmp(account_request_territory,"live")&&strcmp(account_request_territory,"shop")){if(out&&cap)out[0]=0;return 0;}
    return account_public_profile_at(user_id,auth_db,account_request_territory,out,cap);
}
/* Native session presentation. No account facts, grants or sessions are cached. */
static void account_navigation_emit(const char *bearer,const char *auth_db) {
    StudioViewer who={0};
    int signed_in=session_identity(bearer,auth_db,&who) && who.id>0;
    fputs("<div class=\"ak-session\" data-session-state=\"",stdout);
    fputs(signed_in?"signed-in":"anonymous",stdout);fputs("\">",stdout);
    if(!signed_in) {
        fputs("<a class=\"ak-account\" id=\"session-link\" href=\"",stdout);fputs(account_signin_path,stdout);fputs("?next=",stdout);bf_auth_query(stdout,account_return_path);fputs("\">" BF_ICON_LOGIN "<span>Sign in</span></a>",stdout);
    } else {
        char profile[512]={0};
        account_public_profile(who.id,auth_db,profile,sizeof(profile));
        fputs("<a class=\"ak-account\" data-profile-home href=\"/profile\">" BF_ICON_USER "<span>Profile</span></a>",stdout);
        fputs("<a class=\"ak-account\" data-your-work href=\"/apps?context=self\">" BF_ICON_BRIEFCASE "<span>Your work</span></a><details class=\"ak-user\"><summary aria-label=\"Your account menu\"><span class=\"ak-avatar\" aria-hidden=\"true\">",stdout);
        /* A decorative initial is text, never an image URL or an identity claim. */
        char initial[2]={'A',0};if(who.name[0]>='A'&&who.name[0]<='Z')initial[0]=who.name[0];else if(who.name[0]>='a'&&who.name[0]<='z')initial[0]=who.name[0]-'a'+'A';html(initial);
        fputs("</span><span class=\"ak-user-name\">",stdout);html(*who.name?who.name:"Your account");
        fputs("</span><span aria-hidden=\"true\">⌄</span></summary><nav class=\"ak-user-links\" aria-label=\"Your account\"><a data-profile-home-menu href=\"/profile\">" BF_ICON_USER "Your profile</a>",stdout);
        if(profile[0]){fputs("<a data-public-profile data-public-profile-menu href=\"",stdout);html(profile);fputs("\">" BF_ICON_WORLD "View public profile</a>",stdout);}
        fputs("<a data-account-profile href=\"/profile?tab=sites\">" BF_ICON_WORLD_WWW "Your profile sites</a><a href=\"/account\">" BF_ICON_LINK "Account &amp; connections</a><a href=\"/apps?context=self\">" BF_ICON_APPS "Your tools &amp; saved work</a><a href=\"/saved\">" BF_ICON_BOOKMARK "Saved material</a><a href=\"/organizations\">" BF_ICON_GROUP "Organizations &amp; people</a><a href=\"/packs/setup?context=self\">" BF_ICON_SPARKLES "Explore tools</a><a href=\"/account#logout\">" BF_ICON_LOGOUT "Sign out</a></nav></details>",stdout);
    }
    fputs("<style>.ak-user:not([open])>.ak-user-links{display:none!important}.ak-user-links{box-sizing:border-box;max-height:calc(100vh - 100px);overflow-y:auto;overscroll-behavior:contain}</style>",stdout);
    fputs("<style>@media(max-width:760px){.ak-session>a[data-your-work],.ak-session>a[data-profile-home]{display:none}}</style>",stdout);
    fputs("</div>",stdout);
}
/* Cached public shells request only this private, bounded native fragment.
 * It does not return tokens, email, account IDs or hidden destination counts. */
static int serve_account_navigation(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db) {
    if(strcmp(target,"/account/navigation"))return 0;
    if(strcmp(method,"GET") || (query&&*query)) {
        fputs("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nCache-Control: private, no-store\r\nConnection: close\r\n\r\nUse the current account navigation without parameters.\n",stdout);return 1;
    }
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",stdout);
    account_navigation_emit(bearer,auth_db);return 1;
}
/* The setup and installed-work shell is already private. Fill its session slot
 * before sending bytes, so ordinary navigation also works without JavaScript. */
static void account_navigation_chrome(const char *format,const char *links,const char *bearer,const char *auth_db) {
    int size=snprintf(NULL,0,format,links);
    if(size<0||size>256*1024)return;
    char *body=malloc((size_t)size+1);if(!body)return;
    snprintf(body,(size_t)size+1,format,links);
    const char *start="<!--BF_SESSION_BEGIN-->",*end="<!--BF_SESSION_END-->";
    char *a=strstr(body,start),*b=a?strstr(a,end):NULL;
    if(a&&b){fwrite(body,1,(size_t)(a-body),stdout);fputs(start,stdout);account_navigation_emit(bearer,auth_db);fputs(b,stdout);}else fputs(body,stdout);
    free(body);
}
#endif
