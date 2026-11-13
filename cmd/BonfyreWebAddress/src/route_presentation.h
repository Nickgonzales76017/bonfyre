#ifndef BONFYRE_ROUTE_PRESENTATION_H
#define BONFYRE_ROUTE_PRESENTATION_H
/* Which composition pattern (and optional presentation_template) a public
 * route wears on a tenant-owned territory. Authored as route_presentation rows in
 * the CMS, so a design system arrives as data and every surface that reads the
 * CMS (pages, MCP, API) sees the same choice. Presentation only: the canonical
 * page owner keeps the route, its material and every action. No row, no
 * change: the page renders exactly as its owner wrote it. */
#include <stdio.h>
#include <string.h>
#include <sqlite3.h>
#include "site_presentation.h"
#include "tenant_host_binding.h"

static sqlite3 *cms_open_ro(void);

static inline int bf_route_presentation_has_query_key(const char *query, const char *key) {
    size_t n = key ? strlen(key) : 0;
    if (!n) return 0;
    for (const char *p = query; p && *p; ) {
        if (!strncmp(p, key, n) && p[n] == '=') return 1;
        p = strchr(p, '&'); if (p) p++;
    }
    return 0;
}

static inline int bf_route_presentation_safe(const char *v) {
    if (!v || !*v || strlen(v) > 64) return 0;
    for (const unsigned char *c = (const unsigned char *)v; *c; c++)
        if (!((*c >= 'a' && *c <= 'z') || (*c >= '0' && *c <= '9') || *c == '-' || *c == '_' || *c == ':')) return 0;
    return 1;
}

/* Fills pattern (and template when the row names one); 1 when a row applies. */
static inline int bf_route_presentation_lookup(const char *territory, const char *route, const char *query,
                                        char pattern[65], char template_name[65]) {
    sqlite3 *db; sqlite3_stmt *st = NULL; int found = 0;
    pattern[0] = template_name[0] = '\0';
    if (!territory || !route || (strcmp(territory, "work") && strcmp(territory, "life") &&
        strcmp(territory, "live") && strcmp(territory, "shop"))) return 0;
    if (!(db = cms_open_ro())) return 0;
    if (sqlite3_prepare_v2(db,
          "SELECT pattern,COALESCE(template,''),COALESCE(when_query_key,'') FROM route_presentation "
          "WHERE status='published' AND territory=?1 AND "
          "(route=?2 OR (route<>'/' AND substr(?2,1,length(route)+1)=route||'/')) "
          "ORDER BY length(route) DESC,(COALESCE(when_query_key,'')<>'') DESC,id DESC", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, territory, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        while (!found && sqlite3_step(st) == SQLITE_ROW) {
            const char *p = (const char *)sqlite3_column_text(st, 0);
            const char *t = (const char *)sqlite3_column_text(st, 1);
            const char *k = (const char *)sqlite3_column_text(st, 2);
            if (k && *k && !bf_route_presentation_has_query_key(query, k)) continue;
            if (!bf_route_presentation_safe(p)) continue;
            snprintf(pattern, 65, "%s", p);
            if (bf_route_presentation_safe(t)) snprintf(template_name, 65, "%s", t);
            found = 1;
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return found;
}

/* Only the declared territory front doors and native account authorities
 * inherit a territory presentation. Arbitrary creator/publisher subdomains
 * retain their own independently published subject compositions. */
static inline const char *bf_route_presentation_territory(const char *host) {
    int state=bf_tenant_hosts_status();
    if(state==1) return bf_tenant_territory_for_host(host);
    /* Invalid explicit configuration never silently switches to legacy. */
    if(state<0 || !host) return NULL;
    /* Release compatibility only, moved out with Aurekai-owned deployment. */
    if (!strcmp(host,"aurekai.work") || !strcmp(host,"signin.aurekai.work")) return "work";
    if (!strcmp(host,"aurekai.life") || !strcmp(host,"auth.aurekai.life")) return "life";
    if (!strcmp(host,"aurekai.live") || !strcmp(host,"auth.aurekai.live")) return "live";
    if (!strcmp(host,"aurekai.shop") || !strcmp(host,"auth.aurekai.shop")) return "shop";
    return NULL;
}
static inline const char *bf_route_presentation_pattern(const char *host, const char *route, const char *query) {
    static char pattern[65], template_name[65];
    const char *territory = bf_route_presentation_territory(host);
    if (!territory) return NULL;
    return bf_route_presentation_lookup(territory, route, query, pattern, template_name) ? pattern : NULL;
}
static inline const char *bf_route_presentation_native_pattern(const char *territory, const char *route) {
    static char pattern[65], template_name[65];
    return bf_route_presentation_lookup(territory, route, NULL, pattern, template_name) ? pattern : NULL;
}
static inline void bf_route_presentation_emit_body_attribute(FILE *out, const char *pattern) {
    if (bf_route_presentation_safe(pattern)) fprintf(out, " data-aurekai-pattern=\"%s\"", pattern);
}
/* Marks the body with the pattern and, when the row names a published
 * template, with data-bf-template so world.css maps the template tokens. */
static inline void bf_route_presentation_emit_body_attribute_for(FILE *out, const char *territory, const char *route,
                                                                 const char *query, const char *pattern) {
    char p[65], t[65];
    bf_route_presentation_emit_body_attribute(out, pattern);
    if (bf_route_presentation_safe(pattern) && territory && route &&
        bf_route_presentation_lookup(territory, route, query, p, t) && t[0])
        fprintf(out, " data-bf-template=\"%s\"", t);
}
/* The pattern rhythm lives in a cached static sheet; a template named by the
 * row adds its tokens through the same emitter member sites use. */
static inline void bf_route_presentation_emit_head_for(FILE *out, const char *territory, const char *route,
                                                const char *query, const char *pattern) {
    char p[65], t[65];
    if (!bf_route_presentation_safe(pattern)) return;
    fprintf(out, "<meta name=\"aurekai:composition\" content=\"%s\">"
                 "<link rel=\"stylesheet\" href=\"/assets/composition.css\">", pattern);
    if (territory && route && bf_route_presentation_lookup(territory, route, query, p, t) && t[0]) {
        sqlite3 *db = cms_open_ro();
        if (db) { bf_site_presentation_emit_scoped(out, db, t, "body[data-aurekai-pattern]"); sqlite3_close(db); }
    }
}
static inline void bf_route_presentation_emit_head(FILE *out, const char *pattern) {
    bf_route_presentation_emit_head_for(out, NULL, NULL, NULL, pattern);
}
/* Reuse a complete CMS-authored HTML document without changing its records,
 * aperture or route owner. Machine clients still take their original path. */
static inline int bf_route_presentation_emit_authored(const char *host, const char *route,
                                                      const char *query, const char *body) {
    const char *pattern = bf_route_presentation_pattern(host, route, query);
    if (!pattern || !body) return 0;
    const char *head = strstr(body, "</head>");
    const char *open = head ? strstr(head + 7, "<body") : NULL;
    const char *close = open ? strchr(open, '>') : NULL;
    if (!head || !open || !close) return 0;
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
          "Cache-Control: private, no-store\r\nVary: Host, Accept, Cookie\r\n"
          "Connection: close\r\n\r\n", stdout);
    fwrite(body, 1, (size_t)(head - body), stdout);
    bf_route_presentation_emit_head_for(stdout, bf_route_presentation_territory(host), route, query, pattern);
    fwrite(head, 1, (size_t)(close - head), stdout);
    bf_route_presentation_emit_body_attribute_for(stdout, bf_route_presentation_territory(host), route, query, pattern);
    fputs(close, stdout);
    return 1;
}
#endif
