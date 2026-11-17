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
    if(bf_store_open(auth_db, &adb, SQLITE_OPEN_READONLY, &bf_store_policy_auth)!=SQLITE_OK)goto done;
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
    if(bf_store_open(matrix_db, &mdb, SQLITE_OPEN_READONLY, &bf_store_policy_matrix)!=SQLITE_OK)goto done;
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
    ui_start(stdout,"div");ui_attr(stdout,"class","ak-session");ui_attr_open(stdout,"data-session-state");
    fputs(signed_in?"signed-in":"anonymous",stdout);ui_attr_close(stdout);ui_gt(stdout);
    if(!signed_in) {
        ui_start(stdout,"a");ui_attr(stdout,"class","ak-account");ui_attr(stdout,"id","session-link");ui_attr_open(stdout,"href");fputs(account_signin_path,stdout);fputs("?next=",stdout);bf_auth_query(stdout,account_return_path);ui_attr_close(stdout);ui_gt(stdout);fputs(BF_ICON_LOGIN "<span>Sign in</span></a>",stdout);
    } else {
        char profile[512]={0};
        account_public_profile(who.id,auth_db,profile,sizeof(profile));
        ui_open_a(stdout,"a","class","ak-account","data-profile-home",(const char *)NULL,"href","/profile");fputs(BF_ICON_USER "<span>Profile</span></a>",stdout);
        ui_open_a(stdout,"a","class","ak-account","data-your-work",(const char *)NULL,"href","/apps?context=self");fputs(BF_ICON_BRIEFCASE "<span>Your work</span></a>",stdout);ui_open_a(stdout,"a","class","ak-account","data-account-review",(const char *)NULL,"href","/account/review");fputs(BF_ICON_BOARD "<span>Review</span></a>",stdout);ui_open_a(stdout,"details","class","ak-user");ui_open_a(stdout,"summary","aria-label","Your account menu");ui_open_a(stdout,"span","class","ak-avatar","aria-hidden","true");
        /* A decorative initial is text, never an image URL or an identity claim. */
        char initial[2]={'A',0};if(who.name[0]>='A'&&who.name[0]<='Z')initial[0]=who.name[0];else if(who.name[0]>='a'&&who.name[0]<='z')initial[0]=who.name[0]-'a'+'A';html(initial);
        ui_close(stdout,"span"); ui_open(stdout,"span","ak-user-name");html(*who.name?who.name:"Your account");
        ui_close(stdout,"span");ui_open_a(stdout,"span","aria-hidden","true");ui_copy(stdout,"⌄");ui_close(stdout,"span");ui_close(stdout,"summary");ui_open_a(stdout,"nav","class","ak-user-links","aria-label","Your account");ui_open_a(stdout,"a","data-profile-home-menu",(const char *)NULL,"href","/profile");fputs(BF_ICON_USER "Your profile</a>",stdout);
        if(profile[0]){ui_start(stdout,"a");ui_flag(stdout,"data-public-profile");ui_flag(stdout,"data-public-profile-menu");ui_attr_open(stdout,"href");html(profile);ui_attr_close(stdout);ui_gt(stdout);fputs(BF_ICON_WORLD "View public profile</a>",stdout);}
        ui_start(stdout,"a");ui_flag(stdout,"data-account-profile");ui_attr(stdout,"href","/profile?tab=sites");ui_gt(stdout);fputs(BF_ICON_WORLD_WWW "Your profile sites</a>",stdout);
        ui_start(stdout,"a");ui_attr(stdout,"href","/account/review");ui_gt(stdout);fputs(BF_ICON_BOARD "Review drafts and held work</a>",stdout);
        ui_start(stdout,"a");ui_attr(stdout,"href","/account");ui_gt(stdout);fputs(BF_ICON_LINK,stdout);ui_copy(stdout,"Account &amp; connections");ui_close(stdout,"a");
        ui_start(stdout,"a");ui_attr(stdout,"href","/apps?context=self");ui_gt(stdout);fputs(BF_ICON_APPS,stdout);ui_copy(stdout,"Your tools &amp; saved work");ui_close(stdout,"a");
        ui_start(stdout,"a");ui_attr(stdout,"href","/saved");ui_gt(stdout);fputs(BF_ICON_BOOKMARK "Saved material</a>",stdout);
        ui_start(stdout,"a");ui_attr(stdout,"href","/organizations");ui_gt(stdout);fputs(BF_ICON_GROUP,stdout);ui_copy(stdout,"Organizations &amp; people");ui_close(stdout,"a");
        ui_start(stdout,"a");ui_attr(stdout,"href","/packs/setup?context=self");ui_gt(stdout);fputs(BF_ICON_SPARKLES "Explore tools</a>",stdout);
        ui_start(stdout,"a");ui_attr(stdout,"href","/account#logout");ui_gt(stdout);fputs(BF_ICON_LOGOUT "Sign out</a>",stdout);
        ui_close(stdout,"nav");ui_close(stdout,"details");
    }
    ui_close(stdout,"div");
}
#include "account_review_web.h"
#include "web_store.h"
/* Cached public shells request only this private, bounded native fragment.
 * It does not return tokens, email, account IDs or hidden destination counts. */
static int serve_account_navigation(const BfRequest *r) {
    const char *method=r->method; const char *target=r->target; const char *query=r->query; const char *bearer=r->bearer; const char *auth_db=r->auth_db;
    (void)method;(void)target;(void)query;(void)bearer;(void)auth_db;
    if(bf_request_is(r,"review"))return serve_account_review(method,query,bearer,auth_db);
    if(strcmp(method,"GET") || (query&&*query)) {
        fputs("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nCache-Control: private, no-store\r\nConnection: close\r\n\r\nUse the current account navigation without parameters.\n",stdout);return 1;
    }
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nVary: Cookie, Authorization\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",stdout);
    account_navigation_emit(bearer,auth_db);return 1;
}
#endif
