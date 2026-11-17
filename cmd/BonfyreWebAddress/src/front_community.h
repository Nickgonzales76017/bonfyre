#ifndef BONFYRE_FRONT_COMMUNITY_H
#define BONFYRE_FRONT_COMMUNITY_H
#include "../generated/native_estate_hosts.h"
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
    ui_start(stdout,"section");ui_attr(stdout,"class","community-front");ui_flag(stdout,"data-community-front");
    ui_attr(stdout,"aria-labelledby","community-front-title");ui_gt(stdout);
    ui_open(stdout,"div","community-front-head");ui_open(stdout,"div",NULL);
    ui_open(stdout,"span","community-front-kicker");ui_copy(stdout,"Community conversations");ui_close(stdout,"span");
    ui_start(stdout,"h2");ui_attr(stdout,"id","community-front-title");ui_gt(stdout);ui_copy(stdout,"What people are discussing");ui_close(stdout,"h2");
    ui_open(stdout,"p",NULL);ui_copy(stdout,"Real questions, shared work and replies from Aurekai communities.");ui_close(stdout,"p");ui_close(stdout,"div");
    ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,BF_ORIGIN_LIFE);ui_copy(stdout,"/communities");ui_attr_close(stdout);ui_gt(stdout);
    ui_copy(stdout,"Explore communities");ui_close(stdout,"a");ui_close(stdout,"div");ui_open(stdout,"div","community-front-grid");
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
        (ui_open_a(stdout, "article", "class", "community-front-card", "data-community-discovery", community->key), ui_open(stdout, "p", "community-from"));html(community->title);
        (ui_close(stdout, "p"), ui_open(stdout, "h3", NULL));html(post->title[0]?post->title:community->title);
        (ui_close(stdout, "h3"), ui_open(stdout, "p", "community-excerpt"));html(excerpt);
        ui_close(stdout,"p");ui_start(stdout,"a");ui_flag(stdout,"data-community-thread");ui_attr_open(stdout,"href");ui_copy(stdout,BF_ORIGIN_LIFE);ui_copy(stdout,"/community/");
        html(community->key);ui_lit(stdout, "/thread/");html(post->key);
        (ui_lit(stdout, "/\">Read the conversation"), ui_close(stdout, "a"), ui_close(stdout, "article"));
    }
    ui_close_n(stdout, "div", "section", NULL);
}
#endif
