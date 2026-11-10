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
static int front_community(sqlite3 *db) {
    CjrDiscovery items;
    if(!db||!cms_type_registered(db,"community_body")||!cms_type_registered(db,"community_post"))return 0;
    int available=cjr_discover(db,&items);
    /* A front page that announces it has nothing to show is a dead end in the
     * middle of the reading; with no public starter the section stays out and
     * the directory is still one tap away in the bar. */
    if(available&&!items.count)return 0;
    fputs("<section class=\"community-front\" data-community-front aria-labelledby=\"community-front-title\"><header><div><p class=\"community-front-kicker\">Across Aurekai</p><h2 id=\"community-front-title\">",stdout);
    front_section_glyph("group");
    fputs("Conversations to take part in</h2><p>Read what people are working through together. Open a conversation to meet its community.</p></div><nav aria-label=\"Community discovery\"><a href=\"/communities\">Find a community</a><a href=\"/community/new\">Start a community</a></nav></header>",stdout);
    if(!available)fputs("<p role=\"status\">Conversations could not be loaded. Open the community directory to try again.</p>",stdout);
    else {
        fputs("<div class=\"community-front-threads\">",stdout);
        for(size_t n=0;n<items.count;n++) {
            const CjrDiscoveryItem *item=&items.items[n];
            fputs("<article data-community-discovery=\"",stdout);html(item->community.key);
            fputs("\"><p class=\"community-front-group\"><a href=\"/community?community=",stdout);url_query_value(item->community.key);
            fputs("\">",stdout);html(item->community.title);fputs("</a></p><h3><a data-community-thread href=\"/community?community=",stdout);
            url_query_value(item->community.key);fputs("&amp;tab=thread&amp;post=",stdout);url_query_value(item->post.key);
            fputs("\">",stdout);html(*item->post.title?item->post.title:"Open this conversation");fputs("</a></h3><p class=\"community-front-excerpt\">",stdout);
            size_t size=strlen(item->post.body),length=size>280?280:size;char excerpt[281];
            if(length<size)while(length&&(((unsigned char)item->post.body[length]&0xc0)==0x80))length--;
            memcpy(excerpt,item->post.body,length);excerpt[length]=0;html(excerpt);if(length<size)fputs("…",stdout);
            char date[64];struct tm tm;time_t at=page_time(item->post.at);
            snprintf(date,sizeof(date),"%s",item->post.at);
            if(at>0&&gmtime_r(&at,&tm))strftime(date,sizeof(date),"%b %e, %Y",&tm);
            fputs("</p><p class=\"community-front-date\">Published <time datetime=\"",stdout);html(item->post.at);
            fputs("\">",stdout);html(date);fputs("</time></p></article>",stdout);
        }
        fputs("</div>",stdout);
    }
    fputs("</section><style>"
        ".community-front{scroll-margin-top:110px;margin:clamp(28px,5vw,72px) 0;padding:clamp(20px,4vw,46px);background:var(--w-panel,#f5f2e7);color:var(--w-ink,#24362b);border-block:1px solid var(--w-rule,#c8d0bd);box-sizing:border-box}"
        ".community-front *{box-sizing:border-box}.community-front>header{display:flex;justify-content:space-between;align-items:end;gap:24px;flex-wrap:wrap}.community-front>header>div{max-width:65ch}"
        ".community-front-kicker{font:600 11px/1.5 ui-monospace,monospace;letter-spacing:.1em;text-transform:uppercase}.community-front h2{font:400 clamp(30px,4vw,54px)/1.08 Georgia,serif;letter-spacing:-.035em;margin:8px 0 16px;max-width:22ch}"
        ".community-front p{line-height:1.6}.community-front a{color:inherit;text-underline-offset:4px}.community-front nav{display:flex;gap:12px;flex-wrap:wrap}.community-front nav a{display:inline-flex;align-items:center;min-height:44px;padding:10px 15px;border:1px solid currentColor;border-radius:999px;text-decoration:none;font:600 13px/1.4 system-ui}"
        ".community-front-threads{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:clamp(20px,3vw,40px);margin-top:32px}.community-front article{min-width:0;padding-top:20px;border-top:2px solid currentColor;overflow-wrap:anywhere}"
        ".community-front-group{font:600 12px/1.5 system-ui}.community-front h3{font:400 clamp(24px,2.5vw,34px)/1.15 Georgia,serif;margin:14px 0}.community-front h3 a{text-decoration:none}.community-front h3 a:hover{text-decoration:underline}"
        ".community-front-excerpt{font-size:15px;white-space:pre-wrap}.community-front-date{font:12px/1.5 system-ui;opacity:.75}.community-front :focus-visible{outline:3px solid currentColor;outline-offset:5px}"
        "@media(max-width:760px){.community-front-threads{grid-template-columns:minmax(0,1fr)}.community-front{padding:22px 18px}.community-front h2{max-width:none}}"
        "</style>",stdout);
    return 1;
}
#endif
