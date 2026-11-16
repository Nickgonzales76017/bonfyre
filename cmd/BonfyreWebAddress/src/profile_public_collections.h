#ifndef BONFYRE_PROFILE_PUBLIC_COLLECTIONS_H
#define BONFYRE_PROFILE_PUBLIC_COLLECTIONS_H
#include "ui_tree.h"
/* Public profile continuations reuse Auth's exact actor binding and the native
 * CMS community reader. Private membership and institution grants never project. */
#include "../../BonfyreCMS/src/community_read.h"
#include "web_store.h"

static int ph_public_actor_user(const char *actor,const char *auth_db,char out[32]) {
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int count=0;long long id=0;
    out[0]=0;
    if(!actor||strncmp(actor,"person:",7)||!auth_db||!*auth_db)return 0;
    if(bf_store_open(auth_db, &db, SQLITE_OPEN_READONLY, &bf_store_policy_auth)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(db,"SELECT user_id FROM actor_closure_bindings WHERE actor_subject=?1 AND state='active' LIMIT 2",-1,&st,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(st,1,actor,-1,SQLITE_STATIC);
    while(sqlite3_step(st)==SQLITE_ROW){count++;id=sqlite3_column_int64(st,0);}
    if(count==1&&id>0)snprintf(out,32,"user:%lld",id);
done:
    sqlite3_finalize(st);if(db)sqlite3_close(db);return out[0]!=0;
}

static void ph_public_communities(sqlite3 *db,const PhSite *site,const char *auth_db) {
    ui_start(stdout,"section");ui_flag(stdout,"data-profile-communities");ui_gt(stdout);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Communities");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Public communities this person has explicitly created. Private participation and invitations are not published here.");ui_close(stdout,"p");
    char author[32]={0};sqlite3_stmt *st=NULL;int shown=0;
    if(db&&site&&ph_public_actor_user(site->actor,auth_db,author)&&
       sqlite3_prepare_v2(db,"SELECT community FROM community_body WHERE namespace='root' AND status='published' AND visibility='public' AND moderation_policy=?1 AND created_by_subject=?2 ORDER BY id DESC LIMIT 24",-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,CJR_POLICY,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,author,-1,SQLITE_STATIC);
        while(sqlite3_step(st)==SQLITE_ROW){
            const char *key=(const char*)sqlite3_column_text(st,0);CjrCommunity c;
            if(!key||cjr_community(db,key,&c)!=1||!cjr_public(&c)||strcmp(c.owner,author))continue;
            if(!shown++){ui_start(stdout,"div");ui_flag(stdout,"data-public-community-list");ui_gt(stdout);}
            ui_start(stdout,"article");ui_attr(stdout,"class","ph-post");ui_flag(stdout,"data-public-community");ui_gt(stdout);ui_open(stdout,"h3",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/community?community=");
            url_query_value(c.key);fputs("\">",stdout);html(c.title);ui_close(stdout,"a");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);html(c.description);ui_close(stdout,"p");ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/community?community=");url_query_value(c.key);fputs("\">Open public conversations</a></article>",stdout);
        }
    }
    sqlite3_finalize(st);
    if(shown)ui_close(stdout,"div");
    else {ui_start(stdout,"div");ui_attr(stdout,"class","ph-empty");ui_flag(stdout,"data-public-communities-empty");ui_gt(stdout);ui_open(stdout,"p",NULL);ui_copy(stdout,"No public communities are declared for this profile.");ui_close(stdout,"p");ui_close(stdout,"div");}
    ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"href","/communities");ui_gt(stdout);ui_copy(stdout,"Browse public communities");ui_close(stdout,"a");ui_close(stdout,"p");ui_close(stdout,"section");
}

static void ph_public_organizations(void) {
    /* Auth institution grants, invitations and workspace memberships are
     * private access state, not a published person-to-organization relation. */
    fputs("<section data-profile-organizations><h2>Organizations</h2><div class=\"ph-empty\"><p>No public organization affiliations are declared for this profile. Private organization access is not a public endorsement or membership claim.</p></div><p><a href=\"/organisations\">Explore published organizations and their sources</a></p></section>",stdout);
}
#endif
