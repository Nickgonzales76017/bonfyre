#ifndef BONFYRE_FRONT_COMMUNITY_H
#define BONFYRE_FRONT_COMMUNITY_H
#include "../../BonfyreCMS/src/community_discovery.h"
/* A shared public entrance. These conversations are not classified as
 * belonging to a territory and never contain the visitor's private work. */
static int front_community_route(const char *host,const char *route) {
    return host&&route&&!strcmp(route,"/")&&(!strcmp(host,"aurekai.life")||
        !strcmp(host,"aurekai.live")||!strcmp(host,"aurekai.work")||!strcmp(host,"aurekai.shop"));
}
static void front_community_revision(char revision[65]) {
    sqlite3 *db=cms_open_ro();CjrDiscovery items;int available=cjr_discover(db,&items);
    const char *parts[1+CJR_DISCOVERY_LIMIT*2];size_t count=0;
    parts[count++]=available?"available":"unavailable";
    for(size_t n=0;n<items.count;n++){parts[count++]=items.items[n].community.digest;parts[count++]=items.items[n].post.digest;}
    cjr_digest(0,parts,count,revision);if(db)sqlite3_close(db);
}
/* A public conversation belongs to its community, not to a territory or
 * the reader's account. Both the home and its cache use cjr_discover, so a
 * withdrawn contribution can never survive through a secondary selection. */

/* Native front-page circulation reads the same released community cohort
 * as the revision cache and indexed discussion owner. No imported HTML. */
static void front_community_emit(sqlite3 *db) {
    CjrDiscovery found;
    if(!db||!cjr_discover(db,&found)||!found.count)return;
    fputs("<section class=\"community-front\" data-community-front "
          "aria-labelledby=\"community-front-title\"><div class=\"community-front-head\">"
          "<div><span class=\"community-front-kicker\">Community conversations</span>"
          "<h2 id=\"community-front-title\">What people are discussing</h2>"
          "<p>Real questions, shared work and replies from Aurekai communities.</p></div>"
          "<a href=\"https://aurekai.life/communities\">Explore communities</a>"
          "</div><div class=\"community-front-grid\">",stdout);
    for(size_t i=0;i<found.count;i++) {
        const CjrDiscoveryItem *item=&found.items[i];
        const CjrCommunity *community=&item->community;
        const CjrPost *post=&item->post;
        if(!cjr_key(community->key)||!cjr_key(post->key)||
           !cjr_public(community)||!cjr_visible(post))continue;
        char excerpt[361];size_t take=strlen(post->body);
        if(take>=sizeof(excerpt))take=sizeof(excerpt)-1;
        while(take&&((unsigned char)post->body[take]&0xc0)==0x80)take--;
        memcpy(excerpt,post->body,take);excerpt[take]=0;
        fputs("<article class=\"community-front-card\" data-community-discovery=\"",stdout);
        html(community->key);
        fputs("\"><p class=\"community-from\">",stdout);html(community->title);
        fputs("</p><h3>",stdout);html(post->title[0]?post->title:community->title);
        fputs("</h3><p class=\"community-excerpt\">",stdout);html(excerpt);
        fputs("</p><a data-community-thread href=\"https://aurekai.life/community/",stdout);
        html(community->key);fputs("/thread/",stdout);html(post->key);
        fputs("/\">Read the conversation</a></article>",stdout);
    }
    fputs("</div></section>",stdout);
}
#endif
