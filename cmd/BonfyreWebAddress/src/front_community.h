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
static void front_community_styles(void) {
    fputs("<style id=\"native-community-front\">"
      ".community-front{margin:clamp(44px,6vw,86px) 0;border-top:1px solid var(--w-rule,#778477);padding-top:clamp(24px,4vw,46px)}"
      ".community-front *{box-sizing:border-box}"
      ".community-front-head{display:flex;align-items:end;justify-content:space-between;flex-wrap:wrap;gap:16px;margin-bottom:25px}"
      ".community-front-kicker{font:700 .71rem/1.5 var(--bf-face-body,system-ui);letter-spacing:.12em;text-transform:uppercase;color:var(--w-accent,#b5df32)}"
      ".community-front h2{font:400 clamp(2.2rem,4vw,4.4rem)/1.08 var(--bf-face-display,Georgia,serif);letter-spacing:-.04em;margin:9px 0}"
      ".community-front-head p{max-width:58ch;line-height:1.6;color:var(--w-muted,#99a89a)}"
      ".community-front-head>a{font:700 .83rem/1.4 var(--bf-face-body,system-ui);padding:12px 0;color:inherit;text-underline-offset:5px}"
      ".community-front-grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:clamp(12px,1.8vw,22px)}"
      ".community-front-card{grid-column:span 4;display:flex;flex-direction:column;gap:12px;min-width:0;padding:clamp(20px,2vw,28px);border:1px solid var(--w-rule,#778477);background:var(--w-panel,transparent)}"
      ".community-front-card:first-child{grid-column:span 5}.community-front-card:nth-child(2){grid-column:span 4}.community-front-card:last-child{grid-column:span 3}"
      ".community-front-card h3{font:400 clamp(1.45rem,2.1vw,2.25rem)/1.16 var(--bf-face-display,Georgia,serif);letter-spacing:-.025em;margin:0;overflow-wrap:anywhere}"
      ".community-front-card p{margin:0;color:var(--w-muted,#99a89a);line-height:1.57;overflow-wrap:anywhere}"
      ".community-front-card .community-from{font:650 .75rem/1.45 var(--bf-face-body,system-ui);color:var(--w-accent,#b5df32);letter-spacing:.025em}"
      ".community-front-card .community-excerpt{flex:1;max-width:72ch}"
      ".community-front-card>a{align-self:flex-start;color:inherit;font:700 .84rem/1.5 var(--bf-face-body,system-ui);text-underline-offset:5px;margin-top:12px}"
      ".community-front-card a:focus-visible,.community-front-head a:focus-visible{outline:3px solid var(--w-accent,#b5df32);outline-offset:5px}"
      "@media(max-width:1000px){.community-front-card:first-child,.community-front-card:nth-child(2),.community-front-card:last-child{grid-column:span 6}}"
      "@media(max-width:670px){.community-front-grid{grid-template-columns:1fr}.community-front-card:first-child,.community-front-card:nth-child(2),.community-front-card:last-child{grid-column:1}}"
      "</style>",stdout);
}
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
