/* route_presentation: a route's composition pattern and template are CMS rows.
 * Fixture store on disk (the reader opens and closes it per lookup). */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "../src/route_presentation.h"

static char store[64];
static sqlite3 *cms_open_ro(void) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(store, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(db); return NULL; }
    return db;
}

static int fail(const char *why) { fprintf(stderr, "route presentation: FAIL %s\n", why); unlink(store); return 1; }

int main(void) {
    sqlite3 *db = NULL;
    char *out = NULL; size_t len = 0; FILE *mem;
    const char *p;
    snprintf(store, sizeof(store), "/tmp/bf-route-presentation-%d.db", (int)getpid());
    unlink(store);
    if (sqlite3_open(store, &db) != SQLITE_OK || sqlite3_exec(db,
        "CREATE TABLE route_presentation(id INTEGER PRIMARY KEY,territory TEXT,route TEXT,when_query_key TEXT,pattern TEXT,template TEXT,status TEXT);"
        "CREATE TABLE presentation_template(id INTEGER PRIMARY KEY,template TEXT,status TEXT,layout_tokens TEXT,hero_mode TEXT,navigation_mode TEXT,density TEXT,card_mode TEXT,typography_mode TEXT);"
        "INSERT INTO route_presentation(territory,route,when_query_key,pattern,template,status) VALUES"
        "('work','/people',NULL,'directory',NULL,'published'),"
        "('work','/people','who','detail',NULL,'published'),"
        "('work','/s',NULL,'detail','night-ledger','published'),"
        "('work','/s/prices',NULL,'reader',NULL,'published'),"
        "('work','/apps',NULL,'workbench',NULL,'published'),"
        "('work','/relationships',NULL,'workbench','night-ledger','published'),"
        "('life','/','',  'discover','night-ledger','published'),"
        "('shop','/market',NULL,'market',NULL,'draft'),"
        "('live','/bad',NULL,'x\" onload=\"y',NULL,'published');"
        "INSERT INTO presentation_template(template,status,layout_tokens,hero_mode,navigation_mode,density,card_mode,typography_mode) VALUES"
        "('night-ledger','published','{\"ground\":\"#0f1511\",\"accent\":\"#d0fc4f\"}','lead','header','standard','soft-panel','mixed');",
        NULL, NULL, NULL) != SQLITE_OK) return fail("fixture");
    if (sqlite3_exec(db,
        "INSERT INTO route_presentation(territory,route,when_query_key,pattern,template,status)"
        " VALUES ('research','/study',NULL,'reader','night-ledger','published')",
        NULL,NULL,NULL)!=SQLITE_OK)return fail("research fixture");
    sqlite3_close(db);

    if (!(p = bf_route_presentation_pattern("aurekai.work", "/people", "page=2")) || strcmp(p, "directory")) return fail("plain row");
    if (!(p = bf_route_presentation_pattern("aurekai.work", "/people", "who=Alex")) || strcmp(p, "detail")) return fail("query-key row wins when its key is present");
    if (bf_route_presentation_pattern("aurekai.shop", "/market", NULL)) return fail("draft row must not apply");
    if (bf_route_presentation_pattern("aurekai.live", "/bad", NULL)) return fail("unsafe pattern must not apply");
    if (bf_route_presentation_pattern("example.com", "/people", NULL)) return fail("only Aurekai apexes");
    if (bf_route_presentation_pattern("aurekai.work", "/missing", NULL)) return fail("no row, no change");
    if (!(p = bf_route_presentation_pattern("aurekai.work", "/s/prices/observed", NULL)) || strcmp(p, "reader")) return fail("longest route family wins");
    if (!(p = bf_route_presentation_pattern("aurekai.work", "/s/other", NULL)) || strcmp(p, "detail")) return fail("nested source route inherits detail");
    if (!(p = bf_route_presentation_pattern("signin.aurekai.work", "/apps/ws-123/_work", NULL)) || strcmp(p, "workbench")) return fail("signed-in app inherits Work territory");
    if (!(p = bf_route_presentation_pattern("signin.aurekai.work", "/relationships", "context=self")) || strcmp(p, "workbench")) return fail("signed-in relationship workspace");
    if (!(p = bf_route_presentation_pattern("auth.aurekai.life", "/", NULL)) || strcmp(p, "discover")) return fail("account authority inherits Life territory");
    if (bf_route_presentation_pattern("nick.aurekai.work", "/apps/ws-123/_work", NULL)) return fail("subject domain must retain its own surface");
    if (bf_route_presentation_pattern("bad.signin.aurekai.work", "/relationships", NULL)) return fail("untrusted subdomain may not borrow account presentation");
    if (bf_route_presentation_pattern("aurekai.work", "/silly", NULL) || bf_route_presentation_pattern("aurekai.work", "/applications", NULL)) return fail("prefix must stop on segment");
    if (bf_route_presentation_pattern("auth.aurekai.life", "/missing", NULL)) return fail("root must not cover undeclared paths");
    if (!(p = bf_route_presentation_native_pattern("life", "/")) || strcmp(p, "discover")) return fail("native territory lookup");

    mem = open_memstream(&out, &len);
    bf_route_presentation_emit_head_for(mem, "life", "/", NULL, "discover");
    fclose(mem);
    if (!out || !strstr(out, "<link rel=\"stylesheet\" href=\"/assets/composition.css\">") ||
        !strstr(out, "body[data-aurekai-pattern]{--bf-ground:#0f1511;--bf-accent:#d0fc4f;}")) { free(out); return fail("head carries sheet and the row's template tokens"); }
    free(out); out = NULL; len = 0;
    mem = open_memstream(&out, &len);
    bf_route_presentation_emit_body_attribute(mem, "x\"y");
    fclose(mem);
    if (len) { free(out); return fail("unsafe attribute value"); }
    free(out); out=NULL; len=0;

    /* One engine may serve a different operator's explicit domains and
     * presentation vocabulary, not merely its historical four public hosts. */
    char tenant_map[]="/tmp/bf-route-tenant-XXXXXX";
    int fd=mkstemp(tenant_map);
    if(fd<0)return fail("tenant map tempfile");
    FILE *hostmap=fdopen(fd,"w");
    if(!hostmap)return fail("tenant map fdopen");
    fputs("work demo.example.net login.demo.example.net\n"
          "life people.example.net auth.people.example.net\n"
          "research research.example.net auth.research.example.net\n",hostmap);
    if(fclose(hostmap))return fail("tenant map flush");
    if(bf_tenant_hosts_load(tenant_map)!=1)return fail("tenant map admission");
    if (!(p = bf_route_presentation_pattern("demo.example.net","/people",NULL)) ||
        strcmp(p,"directory"))return fail("another operator's work front door");
    if (!(p = bf_route_presentation_pattern("login.demo.example.net","/people",NULL)) ||
        strcmp(p,"directory"))return fail("matching account origin inherits work");
    if (!(p = bf_route_presentation_pattern("research.example.net","/study",NULL)) ||
        strcmp(p,"reader"))return fail("new tenant territory is not limited to four Aurekai names");
    if (bf_route_presentation_pattern("aurekai.work","/people",NULL))
        return fail("configured tenant must not inherit Aurekai's site");
    if (bf_route_presentation_pattern("bad.login.demo.example.net","/people",NULL))
        return fail("untrusted account subdomain");
    mem=open_memstream(&out,&len);
    bf_route_presentation_emit_head_for(mem,"research","/study",NULL,"reader");
    bf_route_presentation_emit_body_attribute(mem,"reader");
    fclose(mem);
    if(!out || !strstr(out,"name=\"bonfyre:composition\"") ||
       !strstr(out,"body[data-bf-pattern]{--bf-ground:#0f1511;--bf-accent:#d0fc4f;}") ||
       !strstr(out,"data-bf-pattern=\"reader\"") ||
       strstr(out,"data-aurekai-pattern"))return fail("brand-neutral publication composition");
    free(out);
    if(bf_tenant_hosts_load("/missing/tenant-map")!=-1)return fail("invalid configuration not refused");
    if(bf_route_presentation_pattern("demo.example.net","/people",NULL))
        return fail("invalid configuration must fail closed");
    unlink(tenant_map);
    unlink(store);
    puts("route presentation: PASS");
    return 0;
}
