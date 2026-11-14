/* Release gate: two institutions can reuse identical route, territory and
 * template names without borrowing one another's editorial or CSS records. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "../src/route_presentation.h"
#include "../src/territory_home.h"

static char store[128];
static sqlite3 *cms_open_ro(void) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(store, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return NULL;
    }
    return db;
}
static int fail(const char *why) {
    fprintf(stderr,"tenant presentation namespace: FAIL %s\n",why);
    return 1;
}
static int write_map(char path[128], const char *row) {
    int fd;
    FILE *f;
    snprintf(path,128,"/tmp/bf-cms-scope-XXXXXX");
    fd=mkstemp(path);
    if(fd<0)return 0;
    f=fdopen(fd,"w");
    if(!f){close(fd);return 0;}
    fprintf(f,"%s\n",row);
    return fclose(f)==0;
}
static const char *home =
    "<!doctype html><html><head></head>"
    "<body><b id=\"territory\">PUBLIC</b>"
    "<span id=\"eyebrow\">generic</span>"
    "<h1 id=\"title\">generic</h1>"
    "<p id=\"sub\">generic</p>"
    "<span id=\"orbit-label\">generic</span>"
    "<h3 id=\"look-title\">generic</h3>"
    "<p id=\"look-copy\">generic</p></body></html>";
static char *owned(const char *s) {
    size_t n = strlen(s)+1;
    char *copy=malloc(n);
    if(copy)memcpy(copy,s,n);
    return copy;
}
static int check_owner(sqlite3 *db, const char *host, const char *ns,
                       const char *pattern, const char *headline, const char *accent) {
    char *body=NULL,*css=NULL;
    size_t css_len=0;
    FILE *memory;
    const char *resolved;
    if(!bf_tenant_cms_namespace("work") ||
       strcmp(bf_tenant_cms_namespace("work"),ns)) {
        fprintf(stderr,"scope mismatch expected=%s observed=%s\n",ns,
                bf_tenant_cms_namespace("work")?bf_tenant_cms_namespace("work"):"(none)");
        return 0;
    }
    resolved=bf_route_presentation_pattern(host,"/shared",NULL);
    if(!resolved || strcmp(resolved,pattern)){
        fprintf(stderr,"route mismatch expected=%s observed=%s\n",pattern,resolved?resolved:"(none)");
        return 0;
    }
    memory=open_memstream(&css,&css_len);
    if(!memory)return 0;
    bf_route_presentation_emit_head_for(memory,"work","/shared",NULL,pattern);
    fclose(memory);
    if(!css || !strstr(css,accent)) {
        fprintf(stderr,"CSS template mismatch expected=%s content=%s\n",accent,css?css:"(none)");
        free(css);return 0;
    }
    free(css);
    body=owned(home);
    if(!body)return 0;
    if(!bf_apply_territory_home_presentation_owned(db,&body,"work",ns) ||
       !strstr(body,headline) || !strstr(body,accent)) {
        fprintf(stderr,"home mismatch expected=%s %s body=%s\n",headline,accent,body);
        free(body);return 0;
    }
    free(body);
    return 1;
}
int main(void) {
    sqlite3 *db=NULL;
    char alpha[128]="", beta[128]="", empty[128]="", unsafe[128]="", unowned[128]="";
    int fd;
    const char *sql =
      "CREATE TABLE route_presentation (id INTEGER PRIMARY KEY,namespace TEXT,"
         "territory TEXT,route TEXT,when_query_key TEXT,pattern TEXT,template TEXT,status TEXT);"
      "CREATE TABLE territory_presentation (id INTEGER PRIMARY KEY,namespace TEXT,"
         "territory TEXT,eyebrow TEXT,head TEXT,lede TEXT,question TEXT,character TEXT,"
         "cannot_infer TEXT,template TEXT,status TEXT);"
      "CREATE TABLE presentation_template (id INTEGER PRIMARY KEY,namespace TEXT,"
         "template TEXT,status TEXT,layout_tokens TEXT,hero_mode TEXT,navigation_mode TEXT,"
         "density TEXT,card_mode TEXT,typography_mode TEXT);"
      "INSERT INTO route_presentation(namespace,territory,route,pattern,template,status) VALUES"
         "('tenant_alpha','work','/shared','alpha-mode','shared-theme','published'),"
         "('tenant_beta','work','/shared','beta-mode','shared-theme','published');"
      "INSERT INTO territory_presentation(namespace,territory,eyebrow,head,lede,question,"
         "character,cannot_infer,template,status) VALUES"
         "('tenant_alpha','work','Alpha eyebrow','Alpha publication','Alpha lede','Alpha Q','A','A','shared-theme','published'),"
         "('tenant_beta','work','Beta eyebrow','Beta publication','Beta lede','Beta Q','B','B','shared-theme','published');"
      "INSERT INTO presentation_template(namespace,template,status,layout_tokens,hero_mode,"
         "navigation_mode,density,card_mode,typography_mode) VALUES"
         "('tenant_alpha','shared-theme','published','{\"accent\":\"#00aa11\"}',"
           "'lead','header','standard','soft-panel','mixed'),"
         "('tenant_beta','shared-theme','published','{\"accent\":\"#bb2200\"}',"
           "'lead','header','standard','soft-panel','mixed');";
    snprintf(store,sizeof(store),"/tmp/bf-owner-data-XXXXXX");
    fd=mkstemp(store);
    if(fd<0)return fail("CMS tempfile");
    close(fd);
    if(sqlite3_open(store,&db)!=SQLITE_OK || sqlite3_exec(db,sql,NULL,NULL,NULL)!=SQLITE_OK)
        return fail("CMS fixtures");
    if(!write_map(alpha,"work alpha.example.net login.alpha.example.net tenant_alpha") ||
       !write_map(beta,"work beta.example.net login.beta.example.net tenant_beta") ||
       !write_map(empty,"work none.example.net login.none.example.net tenant_gamma") ||
       !write_map(unsafe,"work unsafe.example.net login.unsafe.example.net bad'owner") ||
       !write_map(unowned,"work ghost.example.net login.ghost.example.net"))
       return fail("map files");
    if(bf_tenant_hosts_load(alpha)!=1 ||
       !check_owner(db,"alpha.example.net","tenant_alpha","alpha-mode","Alpha publication","#00aa11"))
       return fail("alpha account leaked or was not selected");
    if(bf_route_presentation_pattern("beta.example.net","/shared",NULL))
       return fail("unconfigured beta host inherited alpha owner");
    if(bf_tenant_hosts_load(beta)!=1 ||
       !check_owner(db,"beta.example.net","tenant_beta","beta-mode","Beta publication","#bb2200"))
       return fail("beta account leaked or was not selected");
    if(bf_route_presentation_pattern("alpha.example.net","/shared",NULL))
       return fail("retired alpha host retained access to beta");
    if(bf_tenant_hosts_load(empty)!=1)return fail("missing-content scoped map invalid");
    if(bf_route_presentation_pattern("none.example.net","/shared",NULL))
       return fail("missing tenant namespace borrowed another record");
    char *unused=owned(home);
    if(!unused)return fail("fallback allocation");
    if(!bf_apply_territory_home_presentation_owned(db,&unused,"work","tenant_gamma") ||
       strstr(unused,"Alpha publication") || strstr(unused,"Beta publication") ||
       !strstr(unused,"<body class=\"work\">"))return fail("CMS fallback leaked business content");
    free(unused);
    if(bf_tenant_hosts_load(unowned)!=1)return fail("compatibility host map should parse");
    if(bf_route_presentation_pattern("ghost.example.net","/shared",NULL))
       return fail("unowned configured host borrowed a CMS row");
    if(bf_tenant_hosts_load(unsafe)!=-1)return fail("unsafe namespace was admitted");
    if(bf_route_presentation_pattern("unsafe.example.net","/shared",NULL))
       return fail("malformed configured map failed open");
    sqlite3_close(db);
    unlink(store);unlink(alpha);unlink(beta);unlink(empty);unlink(unsafe);unlink(unowned);
    puts("TENANT_CMS_PRESENTATION_ISOLATION_PASS: hosts, rows, templates, home, no data borrowing");
    return 0;
}
