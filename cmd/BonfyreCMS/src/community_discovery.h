#ifndef BONFYRE_COMMUNITY_DISCOVERY_H
#define BONFYRE_COMMUNITY_DISCOVERY_H
#include "community_read.h"
/* Public discovery uses the same exact identities and visibility predicates
 * as the conversation owner. It neither reads memberships nor admits speech. */
#define CJR_DISCOVERY_LIMIT 3
#define CJR_DISCOVERY_SCAN 24
typedef struct { CjrCommunity community; CjrPost post; } CjrDiscoveryItem;
typedef struct { size_t count; CjrDiscoveryItem items[CJR_DISCOVERY_LIMIT]; } CjrDiscovery;
static int cjr_discover(sqlite3 *db,CjrDiscovery *out) {
    sqlite3_stmt *s=NULL;int step=SQLITE_DONE,ok=1;
    memset(out,0,sizeof(*out));
    const char *sql="SELECT p.community,p.post FROM community_post p JOIN community_body c "
        "ON c.community=p.community AND c.namespace='root' "
        "WHERE p.namespace='root' AND p.status='published' AND p.visibility='public' "
        "AND p.moderation_state IN ('accepted','visible','published') AND COALESCE(p.reply_to,'')='' "
        "AND c.status='published' AND c.visibility='public' AND c.moderation_policy=?1 "
        "AND julianday(p.published_at)<=julianday('now') "
        "ORDER BY julianday(p.published_at) DESC,p.id DESC LIMIT ?2";
    if(!db||sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(s,1,CJR_POLICY,-1,SQLITE_STATIC);
    sqlite3_bind_int(s,2,CJR_DISCOVERY_SCAN);
    while((step=sqlite3_step(s))==SQLITE_ROW) {
        char community[97],post[97],root[97];CjrDiscoveryItem item;
        if(!cjr_column(s,0,community,sizeof(community))||!cjr_column(s,1,post,sizeof(post)))continue;
        int seen=0;for(size_t n=0;n<out->count;n++)if(!strcmp(out->items[n].community.key,community))seen=1;
        if(seen)continue;
        if(cjr_community(db,community,&item.community)!=1||!cjr_public(&item.community)||
           cjr_post(db,community,post,&item.post)!=1||!cjr_visible(&item.post)||
           *item.post.parent||cjr_thread(db,community,&item.post,root,1)!=1)continue;
        out->items[out->count++]=item;
        if(out->count==CJR_DISCOVERY_LIMIT)break;
    }
    if(step!=SQLITE_ROW&&step!=SQLITE_DONE)ok=0;
    sqlite3_finalize(s);if(!ok)memset(out,0,sizeof(*out));return ok;
}
#endif
