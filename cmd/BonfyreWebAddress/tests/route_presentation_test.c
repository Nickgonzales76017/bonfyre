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
        "('life','/','',  'discover','night-ledger','published'),"
        "('shop','/market',NULL,'market',NULL,'draft'),"
        "('live','/bad',NULL,'x\" onload=\"y',NULL,'published');"
        "INSERT INTO presentation_template(template,status,layout_tokens,hero_mode,navigation_mode,density,card_mode,typography_mode) VALUES"
        "('night-ledger','published','{\"ground\":\"#0f1511\",\"accent\":\"#d0fc4f\"}','lead','header','standard','soft-panel','mixed');",
        NULL, NULL, NULL) != SQLITE_OK) return fail("fixture");
    sqlite3_close(db);

    if (!(p = bf_route_presentation_pattern("aurekai.work", "/people", "page=2")) || strcmp(p, "directory")) return fail("plain row");
    if (!(p = bf_route_presentation_pattern("aurekai.work", "/people", "who=Alex")) || strcmp(p, "detail")) return fail("query-key row wins when its key is present");
    if (bf_route_presentation_pattern("aurekai.shop", "/market", NULL)) return fail("draft row must not apply");
    if (bf_route_presentation_pattern("aurekai.live", "/bad", NULL)) return fail("unsafe pattern must not apply");
    if (bf_route_presentation_pattern("example.com", "/people", NULL)) return fail("only Aurekai apexes");
    if (bf_route_presentation_pattern("aurekai.work", "/missing", NULL)) return fail("no row, no change");
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
    free(out);
    unlink(store);
    puts("route presentation: PASS");
    return 0;
}
