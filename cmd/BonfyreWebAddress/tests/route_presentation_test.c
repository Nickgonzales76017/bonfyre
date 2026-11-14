/* route_presentation: a route's composition pattern and template are CMS rows.
 * Fixture store on disk (the reader opens and closes it per lookup). */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include <pthread.h>
#include "../src/route_presentation.h"

static char store[64];
static int cms_opens;
static sqlite3 *cms_open_ro(void) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(store, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(db); return NULL; }
    cms_opens++;
    return db;
}

static int fail(const char *why) { fprintf(stderr, "route presentation: FAIL %s\n", why); unlink(store); return 1; }
static pthread_mutex_t pattern_lock=PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t pattern_signal=PTHREAD_COND_INITIALIZER;
static int pattern_ready,pattern_release,pattern_valid;
static void *threaded_pattern_probe(void *arg) {
    (void)arg;
    const char *observed=bf_route_presentation_pattern("aurekai.work","/connect",NULL);
    pthread_mutex_lock(&pattern_lock);
    pattern_ready=1; pthread_cond_signal(&pattern_signal);
    while (!pattern_release) pthread_cond_wait(&pattern_signal,&pattern_lock);
    pattern_valid=observed && !strcmp(observed,"connections");
    pthread_mutex_unlock(&pattern_lock);
    return NULL;
}


int main(void) {
    sqlite3 *db = NULL;
    char *out = NULL; size_t len = 0; FILE *mem;
    const char *p;
    snprintf(store, sizeof(store), "/tmp/bf-route-presentation-%d.db", (int)getpid());
    unlink(store);
    if (sqlite3_open(store, &db) != SQLITE_OK || sqlite3_exec(db,
        "CREATE TABLE route_presentation(id INTEGER PRIMARY KEY,namespace TEXT NOT NULL DEFAULT 'root',territory TEXT,route TEXT,when_query_key TEXT,pattern TEXT,template TEXT,status TEXT,design_fingerprint TEXT,design_namespace TEXT,design_html_sha256 TEXT);"
        "CREATE TABLE presentation_template(id INTEGER PRIMARY KEY,namespace TEXT NOT NULL DEFAULT 'root',template TEXT,status TEXT,layout_tokens TEXT,hero_mode TEXT,navigation_mode TEXT,density TEXT,card_mode TEXT,typography_mode TEXT);"
        "CREATE TABLE design_fingerprint(id INTEGER PRIMARY KEY,namespace TEXT,fingerprint_id TEXT,status TEXT,visual_vector TEXT,reference_source TEXT,renderer_affinity TEXT,density TEXT);"
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
        "('night-ledger','published','{\"ground\":\"#0f1511\",\"accent\":\"#d0fc4f\"}','lead','header','standard','soft-panel','mixed');"
        "INSERT INTO route_presentation(namespace,territory,route,pattern,template,status) VALUES"
        "('root','work','/connect','directory','night-ledger','published'),"
        "('aurekai','work','/connect','connections','night-ledger','published');"
        "INSERT INTO presentation_template(namespace,template,status,layout_tokens,hero_mode,navigation_mode,density,card_mode,typography_mode) VALUES"
        "('aurekai','night-ledger','published','{\"ground\":\"#202020\",\"accent\":\"#d0fc4f\"}','lead','header','standard','soft-panel','mixed'),"
        "('site-a','site-dress','published','{\"ground\":\"#565656\"}','lead','header','standard','soft-panel','mixed');"
        "INSERT INTO design_fingerprint(namespace,fingerprint_id,status,visual_vector,reference_source,renderer_affinity,density) VALUES"
        "('aurekai','reference:stitch-20261104:fixture_discover','reviewed_family','{\"html_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}','stitch-20261104','discover','standard'),"
        "('aurekai','reference:stitch-20261104:fixture_connections','reviewed_family','{\"html_sha256\":\"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\"}','stitch-20261104','connections','airy');"
        "UPDATE route_presentation SET design_fingerprint='reference:stitch-20261104:fixture_connections',design_namespace='aurekai',design_html_sha256='cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' WHERE namespace='aurekai' AND route='/connect';"
        "UPDATE route_presentation SET design_fingerprint='reference:stitch-20261104:fixture_discover',design_namespace='aurekai',design_html_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE territory='life' AND route='/';"
        "CREATE TABLE scope_presentation(id INTEGER PRIMARY KEY,namespace TEXT DEFAULT 'root',scope_presentation TEXT,scope TEXT,ref TEXT,template TEXT,status TEXT);"
        "INSERT INTO scope_presentation(scope_presentation,scope,ref,template,status) VALUES"
        "('sp1','profile','site-a','site-dress','published'),"
        "('sp2','org','org-acme','org-dress','published'),"
        "('sp3','account','person:a','my-night','published'),"
        "('sp4','profile','site-a','newer-dress','published'),"
        "('sp5','profile','site-b','x\"y','published'),"
        "('sp6','account','person:b','draft-dress','draft');"
        "INSERT INTO scope_presentation(namespace,scope_presentation,scope,ref,template,status) VALUES"
        "('aurekai','sp7','profile','site-a','foreign-dress','published'),"
        "('aurekai','sp8','account','person:a','foreign-account','published'),"
        "('site-a','sp9','profile','site-a','site-dress','published');",
        NULL, NULL, NULL) != SQLITE_OK) return fail("fixture");
    if (sqlite3_exec(db,
        "INSERT INTO route_presentation(territory,route,when_query_key,pattern,template,status)"
        " VALUES ('research','/study',NULL,'reader','night-ledger','published')",
        NULL,NULL,NULL)!=SQLITE_OK)return fail("research fixture");
    sqlite3_close(db);

    if (!(p = bf_route_presentation_pattern("aurekai.work", "/people", "page=2")) || strcmp(p, "directory")) return fail("plain row");
    if (!(p = bf_route_presentation_pattern("aurekai.work", "/connect", NULL)) || strcmp(p, "connections"))
        return fail("company-owned route supersedes obsolete root route");
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
    /* Deterministic two-thread overlap: global static return buffers would
     * be overwritten by the second request while the first is still using it. */
    {
        pthread_t worker;
        if (pthread_create(&worker,NULL,threaded_pattern_probe,NULL)) return fail("thread creation");
        pthread_mutex_lock(&pattern_lock);
        while (!pattern_ready) pthread_cond_wait(&pattern_signal,&pattern_lock);
        pthread_mutex_unlock(&pattern_lock);
        const char *main_pattern=bf_route_presentation_pattern("aurekai.work","/people","who=Alex");
        const int main_valid=main_pattern && !strcmp(main_pattern,"detail");
        pthread_mutex_lock(&pattern_lock);
        pattern_release=1; pthread_cond_signal(&pattern_signal);
        pthread_mutex_unlock(&pattern_lock);
        if (pthread_join(worker,NULL) || !pattern_valid || !main_valid)
            return fail("parallel requests must not corrupt another worker's route pattern");
    }

    {
        char pattern[65], tmpl[65];
        if (!bf_presentation_resolve("work", "/people", NULL, "site-a", "org-acme", "person:a", pattern, tmpl) ||
            strcmp(pattern, "directory") || strcmp(tmpl, "newer-dress")) return fail("profile wins below route, latest row wins");
        if (!bf_presentation_resolve("work", "/s", NULL, "site-a", "org-acme", "person:a", pattern, tmpl) ||
            strcmp(tmpl, "night-ledger")) return fail("route template beats every scope");
        if (bf_presentation_resolve("work", "/missing", NULL, NULL, "org-acme", "person:a", pattern, tmpl) ||
            strcmp(tmpl, "org-dress")) return fail("org beats account without a route row");
        if (bf_presentation_resolve(NULL, NULL, NULL, NULL, NULL, "person:a", pattern, tmpl) ||
            strcmp(tmpl, "my-night")) return fail("account is the fallback");
        if (bf_presentation_resolve(NULL, NULL, NULL, "site-b", NULL, NULL, pattern, tmpl) || tmpl[0])
            return fail("unsafe template reads as absent");
        if (bf_presentation_resolve(NULL, NULL, NULL, NULL, NULL, "person:b", pattern, tmpl) || tmpl[0])
            return fail("draft scope reads as absent");
        if (bf_scope_presentation_lookup("bogus", "x", tmpl) || tmpl[0]) return fail("unknown scope refused");
        if (bf_scope_presentation_lookup("profile", "", tmpl) || tmpl[0]) return fail("empty ref refused");
        if (!bf_scope_presentation_lookup("profile","site-a",tmpl) || strcmp(tmpl,"newer-dress"))
            return fail("another namespace cannot override a root profile");
        if (!bf_scope_presentation_lookup("account","person:a",tmpl) || strcmp(tmpl,"my-night"))
            return fail("another namespace cannot override a root account");
        if (!bf_scope_presentation_lookup_owned("profile","site-a","aurekai",tmpl) ||
            strcmp(tmpl,"foreign-dress")) return fail("explicit CMS owner retrieves only its own profile");
        if (bf_scope_presentation_lookup_owned("profile","site-a","aurekai;drop",tmpl) || tmpl[0])
            return fail("invalid CMS owner is refused");
        if (!bf_scope_presentation_lookup_owned("profile","site-a","site-a",tmpl) || strcmp(tmpl,"site-dress"))
            return fail("member site's own published presentation wins for its exact namespace");
        if (bf_scope_presentation_lookup_owned("profile","site-a","site-b",tmpl) || tmpl[0])
            return fail("unrelated site cannot borrow presentation");
    }

    if (sqlite3_open_v2(store,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK) return fail("open scoped profile template");
    mem = open_memstream(&out,&len);
    int site_css=bf_site_presentation_emit_scoped_owned(mem,db,"site-dress","body[data-bf-template]","site-a");
    fclose(mem);sqlite3_close(db);db=NULL;
    if (!site_css || !strstr(out,"--bf-ground:#565656") || strstr(out,"--bf-ground:#202020")) {
        free(out); return fail("profile site uses its own bounded native template tokens");
    }
    free(out);out=NULL;len=0;
    int before=cms_opens;
    mem = open_memstream(&out,&len);
    bf_route_presentation_emit_head_for(mem,"work","/connect",NULL,"connections");
    fclose(mem);
    if (cms_opens != before+1) { free(out); return fail("head joins route and reviewed design through one CMS reader"); }
    if (!strstr(out,"body[data-aurekai-pattern]{--bf-ground:#202020;--bf-accent:#d0fc4f;}") ||
        strstr(out,"body[data-aurekai-pattern]{--bf-ground:#0f1511;--bf-accent:#d0fc4f;}")) {
        free(out); return fail("company-owned template supersedes root without token islands");
    }
    free(out); out=NULL; len=0;
    before=cms_opens;
    mem = open_memstream(&out, &len);
    bf_route_presentation_emit_body_attribute_for(mem,"work","/connect",NULL,"connections");
    fclose(mem);
    if (cms_opens != before+1) { free(out); return fail("body joins route and reviewed density through one CMS reader"); }
    if (!strstr(out," data-aurekai-density=\"airy\"") ||
        !strstr(out," data-bf-template=\"night-ledger\"")) {
        free(out); return fail("verified company fingerprint drives authored density and template");
    }
    free(out); out=NULL; len=0;
    mem = open_memstream(&out, &len);
    bf_route_presentation_emit_head_for(mem, "life", "/", NULL, "discover");
    fclose(mem);
    if (!out || !strstr(out, "<link rel=\"stylesheet\" href=\"/assets/composition.css\">") ||
        !strstr(out, "body[data-aurekai-pattern]{--bf-ground:#0f1511;--bf-accent:#d0fc4f;}") ||
        !strstr(out, "<meta name=\"aurekai:design-reference\" content=\"reference:stitch-20261104:fixture_discover\">") ||
        !strstr(out, "<meta name=\"aurekai:reference-sha256\" content=\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\">")) { free(out); return fail("head joins exact reviewed reference and native template"); }
    free(out); out = NULL; len = 0;
    if (sqlite3_open(store,&db)!=SQLITE_OK || sqlite3_exec(db,
            "UPDATE route_presentation SET design_html_sha256='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' WHERE territory='life' AND route='/'",
            NULL,NULL,NULL)!=SQLITE_OK) return fail("revision-mismatch fixture");
    sqlite3_close(db);
    mem = open_memstream(&out,&len);
    bf_route_presentation_emit_head_for(mem,"life","/",NULL,"discover");
    fclose(mem);
    if (strstr(out,"aurekai:design-reference")) { free(out); return fail("unmatched source revision cannot attach"); }
    free(out); out = NULL; len = 0;
    if (sqlite3_open(store,&db)!=SQLITE_OK || sqlite3_exec(db,
            "UPDATE design_fingerprint SET renderer_affinity='profile' WHERE fingerprint_id='reference:stitch-20261104:fixture_connections'",
            NULL,NULL,NULL)!=SQLITE_OK) return fail("reference-type mismatch fixture");
    sqlite3_close(db);
    mem = open_memstream(&out,&len);
    bf_route_presentation_emit_body_attribute_for(mem,"work","/connect",NULL,"connections");
    fclose(mem);
    if (strstr(out,"data-aurekai-density")) {
        free(out); return fail("mismatched renderer family cannot control spacing");
    }
    free(out); out=NULL; len=0;
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
    fputs("work demo.example.net login.demo.example.net root\n"
          "life people.example.net auth.people.example.net root\n"
          "research research.example.net auth.research.example.net root\n",hostmap);
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
