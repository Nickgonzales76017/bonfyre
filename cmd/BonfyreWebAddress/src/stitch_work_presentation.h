#ifndef BONFYRE_STITCH_WORK_PRESENTATION_H
#define BONFYRE_STITCH_WORK_PRESENTATION_H
/* Google Stitch 2026-11-04 is presentation research, not a route, data or
 * authority owner. Route identity and native surface material are unchanged.
 * These exact Work routes reuse the 14-pattern SurfaceScene visual grammar. */
#include <stdio.h>
#include <string.h>

typedef struct { const char *route; const char *pattern; } BfStitchWorkRoute;
#include "../generated/native_experience_routes.h"
static int bf_stitch_work_has_query_key(const char *query, const char *key) {
    size_t n = strlen(key);
    for (const char *p = query; p && *p; ) {
        if (!strncmp(p, key, n) && p[n] == '=') return 1;
        p = strchr(p, '&'); if (p) p++;
    }
    return 0;
}
/* Canonical page owners choose the route and retain all rights. This is
 * presentation only on the four exact Aurekai apexes; no new action aperture. */
static const char *bf_stitch_apex_pattern(const char *apex,const char *route,const char *query) {
    const BfStitchWorkRoute *routes=NULL; size_t count=0;
    if(!apex||!route)return NULL;
    if(!strcmp(apex,"work")){routes=bf_stitch_work_routes;count=sizeof(bf_stitch_work_routes)/sizeof(*routes);}
    else if(!strcmp(apex,"life")){routes=bf_stitch_life_routes;count=sizeof(bf_stitch_life_routes)/sizeof(*routes);}
    else if(!strcmp(apex,"live")){routes=bf_stitch_live_routes;count=sizeof(bf_stitch_live_routes)/sizeof(*routes);}
    else if(!strcmp(apex,"shop")){routes=bf_stitch_shop_routes;count=sizeof(bf_stitch_shop_routes)/sizeof(*routes);}
    for(size_t i=0;i<count;i++){
        if(strcmp(routes[i].route,route))continue;
        if(!strcmp(route,"/people")&&(bf_stitch_work_has_query_key(query,"who")||
            bf_stitch_work_has_query_key(query,"subject")))return "detail";
        return routes[i].pattern;
    }
    return NULL;
}
static const char *bf_stitch_work_pattern(const char *host,const char *route,const char *query){
    if(!host||strncmp(host,"aurekai.",8))return NULL;
    return bf_stitch_apex_pattern(host+8,route,query);
}
static const char *bf_stitch_work_native_pattern(const char *territory,const char *route){
    return bf_stitch_apex_pattern(territory,route,NULL);
}
static void bf_stitch_work_emit_body_attribute(FILE *out, const char *pattern) {
    if (pattern) fprintf(out, " data-aurekai-pattern=\"%s\"", pattern);
}
static void bf_stitch_work_emit_head(FILE *out, const char *pattern) {
    if (!pattern) return;
    fprintf(out, "<meta name=\"aurekai:composition\" content=\"%s\">", pattern);
    /* Original HTML remains native and readable without scripts. These are
     * progressive rhythm/style changes, not a second browser renderer. */
    fputs("<style id=\"aurekai-work-composition\">"
        "body[data-aurekai-pattern]{--ak-comp-accent:var(--w-accent,#d0fc4f);"
        "--ak-comp-line:var(--w-rule,rgba(140,160,130,.24));"
        "--ak-comp-panel:var(--w-panel,rgba(125,145,115,.09));"
        "--ak-comp-gap:clamp(20px,3vw,44px)}"
        "body[data-aurekai-pattern] :is(.wrap,.bf-native-layout){"
        "width:min(calc(100% - clamp(24px,5vw,80px)),1440px);max-width:1440px;margin-inline:auto}"
        "body[data-aurekai-pattern] :is(.wrap>.hero,.bf-native-masthead){"
        "padding-block:clamp(28px,5vw,76px);border-bottom:1px solid var(--ak-comp-line)}"
        "body[data-aurekai-pattern] :is(.wrap>.hero h1,.bf-native-masthead h1){"
        "font-size:clamp(2.3rem,5vw,5.4rem);letter-spacing:-.055em;line-height:1.02;text-wrap:balance}"
        "body[data-aurekai-pattern] :is(.wrap>.region,.bf-native-slot){"
        "padding-block:var(--ak-comp-gap);border-bottom:1px solid var(--ak-comp-line)}"
        "body[data-aurekai-pattern] :is(a,button,input,select,textarea):focus-visible{"
        "outline:2px solid var(--ak-comp-accent);outline-offset:3px}"
        "body[data-aurekai-pattern=directory] :is(.people-members,.people-grid){"
        "display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr));gap:16px}"
        "body[data-aurekai-pattern=directory] .people-members>li{"
        "list-style:none;padding:20px;border:1px solid var(--ak-comp-line);"
        "background:var(--ak-comp-panel);border-radius:14px}"
        "body[data-aurekai-pattern=reader] :is(p,li){line-height:1.75}"
        "body[data-aurekai-pattern=reader] .wrap{max-width:1100px}"
        "body[data-aurekai-pattern=workbench] .bf-native-layout{max-width:1640px}"
        "body[data-aurekai-pattern=connections] :is(.source-desk,.bf-native-layout){max-width:1480px}"
        "body[data-aurekai-pattern=market] :is(.tile,.offer,.bf-native-slot){"
        "border-color:var(--ak-comp-line)}"
        "body[data-aurekai-pattern=spatial] :is(svg,canvas){max-width:100%}"
        "@media(max-width:650px){body[data-aurekai-pattern] :is(.wrap,.bf-native-layout){"
        "width:calc(100% - 20px)}"
        "body[data-aurekai-pattern] :is(.wrap>.hero h1,.bf-native-masthead h1){"
        "font-size:clamp(2rem,11vw,3.2rem)}}"
        "@media(prefers-reduced-motion:reduce){"
        "body[data-aurekai-pattern] *{transition:none!important}}"
        "</style>",out);
}
/* Reuse a complete CMS-authored HTML document without changing its records,
 * aperture or route owner. Machine clients still take their original path.
 * All styled human responses are uncached; no authenticated pane is shared. */
static inline int bf_stitch_work_emit_authored(const char *host, const char *route,
                                       const char *query, const char *body) {
    const char *pattern = bf_stitch_work_pattern(host, route, query);
    if (!pattern || !body) return 0;
    const char *head = strstr(body, "</head>");
    const char *open = head ? strstr(head + 7, "<body") : NULL;
    const char *close = open ? strchr(open, '>') : NULL;
    if (!head || !open || !close) return 0;
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
          "Cache-Control: private, no-store\r\nVary: Host, Accept, Cookie\r\n"
          "Connection: close\r\n\r\n", stdout);
    fwrite(body, 1, (size_t)(head - body), stdout);
    bf_stitch_work_emit_head(stdout, pattern);
    fwrite(head, 1, (size_t)(close - head), stdout);
    bf_stitch_work_emit_body_attribute(stdout, pattern);
    fputs(close, stdout);
    return 1;
}
#endif
