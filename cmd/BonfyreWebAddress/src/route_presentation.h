#ifndef BONFYRE_ROUTE_PRESENTATION_H
#define BONFYRE_ROUTE_PRESENTATION_H
/* Which composition pattern (and optional presentation_template) a public
 * route wears on one Aurekai territory. Authored as route_presentation rows in
 * the CMS, so a design system arrives as data and every surface that reads the
 * CMS (pages, MCP, API) sees the same choice. Presentation only: the canonical
 * page owner keeps the route, its material and every action. No row, no
 * change: the page renders exactly as its owner wrote it. */
#include <stdio.h>
#include <string.h>
#include <sqlite3.h>
#include "site_presentation.h"

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
/* Shared read-only CMS snapshot for a route and its design provenance. */
static inline int bf_route_presentation_lookup_db(sqlite3 *db, const char *territory, const char *route, const char *query,
                                        char pattern[65], char template_name[65], sqlite3_int64 *row_id, char owner[8]) {
    sqlite3_stmt *st = NULL; int found = 0;
    if (row_id) *row_id = 0;
    if (owner) owner[0] = '\0';
    pattern[0] = template_name[0] = '\0';
    if (!db || !territory || !route || (strcmp(territory, "work") && strcmp(territory, "life") &&
        strcmp(territory, "live") && strcmp(territory, "shop"))) return 0;
    if (sqlite3_prepare_v2(db,
          "SELECT id,namespace,pattern,COALESCE(template,''),COALESCE(when_query_key,'') FROM route_presentation "
          "WHERE status='published' AND namespace IN ('aurekai','root') AND territory=?1 AND "
          "(route=?2 OR (route<>'/' AND substr(?2,1,length(route)+1)=route||'/')) "
          "ORDER BY length(route) DESC,(COALESCE(when_query_key,'')<>'') DESC,"
          "CASE namespace WHEN 'aurekai' THEN 0 ELSE 1 END,id DESC", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, territory, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        while (!found && sqlite3_step(st) == SQLITE_ROW) {
            const char *ns = (const char *)sqlite3_column_text(st, 1);
            const char *p = (const char *)sqlite3_column_text(st, 2);
            const char *t = (const char *)sqlite3_column_text(st, 3);
            const char *k = (const char *)sqlite3_column_text(st, 4);
            if (k && *k && !bf_route_presentation_has_query_key(query, k)) continue;
            if (!bf_route_presentation_safe(p)) continue;
            snprintf(pattern, 65, "%s", p);
            if (bf_route_presentation_safe(t)) snprintf(template_name, 65, "%s", t);
            if (row_id) *row_id = sqlite3_column_int64(st, 0);
            if (owner) snprintf(owner, 8, "%s", ns);
            found = 1;
        }
        sqlite3_finalize(st);
    }
    return found;
}
static inline int bf_route_presentation_lookup_with_id(const char *territory, const char *route, const char *query,
                                        char pattern[65], char template_name[65], sqlite3_int64 *row_id, char owner[8]) {
    pattern[0] = template_name[0] = '\0';
    if(row_id) *row_id=0;
    if(owner) owner[0]='\0';
    if(!territory || !route ||
       (strcmp(territory,"work") && strcmp(territory,"life") &&
        strcmp(territory,"live") && strcmp(territory,"shop"))) return 0;
    sqlite3 *db=cms_open_ro();
    if(!db) return 0;
    int matched=bf_route_presentation_lookup_db(db,territory,route,query,pattern,template_name,row_id,owner);
    sqlite3_close(db);
    return matched;
}
static inline int bf_route_presentation_lookup(const char *territory, const char *route, const char *query,
                                        char pattern[65], char template_name[65]) {
    return bf_route_presentation_lookup_with_id(territory, route, query, pattern, template_name, NULL, NULL);
}

/* Only the declared territory front doors and native account authorities
 * inherit a territory presentation. Arbitrary creator/publisher subdomains
 * retain their own independently published subject compositions. */
/* Account, profile and organization template scopes. Latest published row
 * per (scope, ref) wins; unknown scopes and unsafe names read as absent.
 * A store without the type reads as no scopes. */
static inline int bf_scope_presentation_lookup_owned(const char *scope, const char *ref,
                                                     const char *namespace, char tmpl[65]) {
    sqlite3 *db; sqlite3_stmt *st = NULL; int found = 0;
    tmpl[0] = '\0';
    if (!scope || !ref || !*ref || !namespace || !*namespace || strlen(namespace)>127 ||
        (strcmp(scope, "account") && strcmp(scope, "profile") && strcmp(scope, "org")))
        return 0;
    for(const unsigned char *p=(const unsigned char*)namespace;*p;p++)
        if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='_'||*p=='-'))
            return 0;
    if (!(db = cms_open_ro())) return 0;
    /* A profile/account override is owned copy, not a global latest-ID
     * contest. The requested namespace is an exact CMS owner coordinate. */
    if (sqlite3_prepare_v2(db,
          "SELECT template FROM scope_presentation WHERE status='published'"
          " AND namespace=?3 AND scope=?1 AND ref=?2"
          " ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, scope, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, ref, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 3, namespace, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *t = (const char *)sqlite3_column_text(st, 0);
            if (bf_route_presentation_safe(t)) { snprintf(tmpl, 65, "%s", t); found = 1; }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return found;
}
static inline int bf_scope_presentation_lookup(const char *scope,const char *ref,char tmpl[65]) {
    return bf_scope_presentation_lookup_owned(scope,ref,"root",tmpl);
}
/* Full template resolution: route row, then the visited profile site, then
 * the visited organization, then the viewing account's default. Owner
 * specificity wins; the account row applies only where owners are silent.
 * Pattern comes from the route row alone. Returns 1 when a pattern matched;
 * tmpl may still be empty (no template anywhere). */
static inline int bf_presentation_resolve(const char *territory, const char *route,
                                           const char *query, const char *site_id,
                                           const char *org_ref, const char *actor,
                                           char pattern[65], char tmpl[65]) {
    int matched = 0;
    pattern[0] = tmpl[0] = '\0';
    if (territory && route)
        matched = bf_route_presentation_lookup(territory, route, query, pattern, tmpl);
    if (!tmpl[0] && site_id && *site_id)
        bf_scope_presentation_lookup("profile", site_id, tmpl);
    if (!tmpl[0] && org_ref && *org_ref)
        bf_scope_presentation_lookup("org", org_ref, tmpl);
    if (!tmpl[0] && actor && *actor)
        bf_scope_presentation_lookup("account", actor, tmpl);
    return matched;
}
static inline const char *bf_route_presentation_territory(const char *host) {
    if (!host) return NULL;
    if (!strcmp(host,"aurekai.work") || !strcmp(host,"signin.aurekai.work")) return "work";
    if (!strcmp(host,"aurekai.life") || !strcmp(host,"auth.aurekai.life")) return "life";
    if (!strcmp(host,"aurekai.live") || !strcmp(host,"auth.aurekai.live")) return "live";
    if (!strcmp(host,"aurekai.shop") || !strcmp(host,"auth.aurekai.shop")) return "shop";
    return NULL;
}
static inline const char *bf_route_presentation_pattern(const char *host, const char *route, const char *query) {
    static _Thread_local char pattern[65], template_name[65];
    const char *territory = bf_route_presentation_territory(host);
    if (!territory) return NULL;
    return bf_route_presentation_lookup(territory, route, query, pattern, template_name) ? pattern : NULL;
}
static inline const char *bf_route_presentation_native_pattern(const char *territory, const char *route) {
    static _Thread_local char pattern[65], template_name[65];
    return bf_route_presentation_lookup(territory, route, NULL, pattern, template_name) ? pattern : NULL;
}
static inline void bf_route_presentation_emit_body_attribute(FILE *out, const char *pattern) {
    if (bf_route_presentation_safe(pattern)) fprintf(out, " data-aurekai-pattern=\"%s\"", pattern);
}
/* Marks the body with the pattern and, when the row names a published
 * template, with data-bf-template so world.css maps the template tokens. */
static inline int bf_route_presentation_emit_reference(FILE *out, sqlite3 *db, sqlite3_int64 row_id, char density[12]);
static inline void bf_route_presentation_emit_body_attribute_for(FILE *out, const char *territory, const char *route,
                                                                 const char *query, const char *pattern) {
    char p[65], t[65], owner[8]={0}, density[12]={0}; sqlite3_int64 row_id=0;
    bf_route_presentation_emit_body_attribute(out, pattern);
    if (!bf_route_presentation_safe(pattern) || !territory || !route) return;
    sqlite3 *db=cms_open_ro();
    if (!db) return;
    if (bf_route_presentation_lookup_db(db,territory,route,query,p,t,&row_id,owner) &&
        !strcmp(pattern,p)) {
        if (t[0]) fprintf(out," data-bf-template=\"%s\"",t);
        bf_route_presentation_emit_reference(NULL,db,row_id,density);
        if (density[0]) fprintf(out," data-aurekai-density=\"%s\"",density);
    }
    sqlite3_close(db);
}
/* Source-pinned reference selection is a statement of visual lineage, not
 * screenshot parity. Only explicit Aurekai design rows with an exact retained
 * HTML digest and reviewed family/variant status may mark rendered routes.
 * The published template and compiled PCSS still own all presentation rules. */
static inline int bf_route_presentation_emit_reference(FILE *out, sqlite3 *db, sqlite3_int64 row_id, char density[12]) {
    sqlite3_stmt *st=NULL; int matched=0;
    if (density) density[0]='\0';
    const char *sql=
      "SELECT d.fingerprint_id,r.design_html_sha256,d.density FROM route_presentation r "
      "JOIN design_fingerprint d ON d.fingerprint_id=r.design_fingerprint "
      "AND d.namespace=r.design_namespace "
      "WHERE r.id=?1 AND r.status='published' AND r.design_namespace='aurekai' "
      "AND d.status IN ('reviewed_family','reviewed_variant') "
      "AND d.reference_source='stitch-20261104' AND d.renderer_affinity=r.pattern "
      "AND r.design_html_sha256=json_extract(d.visual_vector,'$.html_sha256') "
      "AND length(r.design_html_sha256)=64";
    if (!row_id || sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_int64(st,1,row_id);
    if (sqlite3_step(st)==SQLITE_ROW) {
        const char *id=(const char *)sqlite3_column_text(st,0);
        const char *sha=(const char *)sqlite3_column_text(st,1);
        int safe=id && !strncmp(id,"reference:stitch-",17) && strlen(id)<=190 && sha;
        for(const unsigned char *p=(const unsigned char *)id;safe && *p;p++)
            if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='_'||*p=='-'||*p==':')) safe=0;
        for(const unsigned char *p=(const unsigned char *)sha;safe && *p;p++)
            if(!((*p>='a'&&*p<='f')||(*p>='0'&&*p<='9'))) safe=0;
        if(safe) {
            matched=1;
            const char *v=(const char *)sqlite3_column_text(st,2);
            if (density && v &&
                (!strcmp(v,"tight") || !strcmp(v,"airy") || !strcmp(v,"standard")))
                snprintf(density,12,"%s",v);
            if(out) fprintf(out,"<meta name=\"aurekai:design-reference\" content=\"%s\">"
                               "<meta name=\"aurekai:reference-sha256\" content=\"%s\">",id,sha);
        }
    }
    sqlite3_finalize(st);
    return matched;
}
/* The pattern rhythm lives in a cached static sheet; a template named by the
 * row adds its tokens through the same emitter member sites use. */
static inline void bf_route_presentation_emit_head_for(FILE *out, const char *territory, const char *route,
                                                const char *query, const char *pattern) {
    char p[65], t[65], owner[8]={0}; sqlite3_int64 row_id=0;
    if (!bf_route_presentation_safe(pattern)) return;
    fprintf(out, "<meta name=\"aurekai:composition\" content=\"%s\">"
                 "<link rel=\"stylesheet\" href=\"/assets/composition.css\">",pattern);
    if (!territory || !route) return;
    sqlite3 *db=cms_open_ro();
    if (!db) return;
    if (bf_route_presentation_lookup_db(db,territory,route,query,p,t,&row_id,owner) &&
        !strcmp(p,pattern)) {
        if (t[0] && (strcmp(owner,"aurekai") ||
            !bf_site_presentation_emit_scoped_owned(out,db,t,"body[data-aurekai-pattern]","aurekai")))
            bf_site_presentation_emit_scoped(out,db,t,"body[data-aurekai-pattern]");
        bf_route_presentation_emit_reference(out,db,row_id,NULL);
    }
    sqlite3_close(db);
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
