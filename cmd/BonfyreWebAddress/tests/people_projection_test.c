/* A printed byline is source evidence, not an identity resolver.  This keeps
 * the public directory from silently collapsing two publishers' Alexes. */
#include "people_projection.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void fail(const char *message) {
    fprintf(stderr, "people projection test: %s\n", message);
    exit(1);
}

static char *read_rendered(FILE *out) {
    long n; char *body;
    fflush(out); fseek(out, 0, SEEK_END); n = ftell(out); rewind(out);
    body = calloc((size_t)n + 1, 1);
    if (!body || fread(body, 1, (size_t)n, out) != (size_t)n) fail("could not read rendered page");
    fclose(out); return body;
}

static int occurrences(const char *body, const char *needle) {
    int count = 0;
    size_t n = strlen(needle);
    const char *p = body;
    while ((p = strstr(p, needle))) {
        count++;
        p += n;
    }
    return count;
}


static char *render_index(const char *db) {
    FILE *out = tmpfile();
    if (!out) fail("could not create output");
    if (!bf_people_index(out, db, "/people", "", 0, 60)) fail("index was empty");
    return read_rendered(out);
}

static char *render_person(const char *db, const char *who, const char *source) {
    FILE *out = tmpfile();
    if (!out) fail("could not create output");
    if (!bf_person_page(out, db, who, source, "/people")) fail("person surface was empty");
    return read_rendered(out);
}

/* Count SQLite work, not wall time: this must pass under a loaded host too. */
static unsigned people_query_ticks;
static int people_query_metering;
static int people_query_budget(void *unused) {
    (void)unused;
    return ++people_query_ticks > 4000;
}
static int people_query_open(const char *path, sqlite3 **db, int flags,
                             const char *vfs) {
    int rc = sqlite3_open_v2(path, db, flags, vfs);
    if (rc == SQLITE_OK && people_query_metering)
        sqlite3_progress_handler(*db, 1000, people_query_budget, NULL);
    return rc;
}
/* Compile the real projection with a connection-local meter, including on macOS. */
#define sqlite3_open_v2 people_query_open
#include "../src/people_projection.c"
#include "../src/item_credit_indexes.h"
#undef sqlite3_open_v2
/* More than two thousand published people, each rendered once despite multiple
 * territory hosts. The archive can disappear without taking CMS people down. */
static void people_member_paging(const char *archive,const char *cms) {
    sqlite3 *db=NULL;
    const char *seed =
      "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2048)"
      " INSERT INTO site_presentation"
      " SELECT printf('member%04d.aurekai.work',i),printf('person:member%04d',i),"
      " 'published','root','member-profile-v1' FROM n;"
      "INSERT INTO site_presentation VALUES"
      "('member0030.aurekai.life','person:member0030','published','root','member-profile-v1'),"
      "('member0030.aurekai.live','person:member0030','published','root','member-profile-v1'),"
      "('member0030.aurekai.shop','person:member0030','published','root','member-profile-v1');";
    if(sqlite3_open(cms,&db)!=SQLITE_OK||sqlite3_exec(db,seed,NULL,NULL,NULL)!=SQLITE_OK)
        fail("published member cohort fixture");
    sqlite3_close(db);
    FILE *out=tmpfile();if(!out)fail("member page output");
    if(!bf_people_index(out,archive,"/people","",30,30))fail("member page not rendered");
    char *page=read_rendered(out);
    if(occurrences(page,"data-person-subject='person:member0030'")!=1||
       !strstr(page,"member0030.aurekai.work")||
       !strstr(page,"member0030.aurekai.life")||
       !strstr(page,"member0030.aurekai.live")||
       !strstr(page,"member0030.aurekai.shop")||
       strstr(page,"data-person-subject='person:member0029'")||
       !strstr(page,"More people")||!strstr(page,"Previous people"))
        fail("member pagination split one person across territory sites");
    free(page);
    out=tmpfile();if(!out)fail("large member page output");
    if(!bf_people_index(out,archive,"/people","",2040,30))fail("beyond 200 member profiles not rendered");
    page=read_rendered(out);
    if(!strstr(page,"member2048.aurekai.work")||strstr(page,"More people"))
        fail("member directory did not reach more than 2,000 identities");
    free(page);
    out=tmpfile();if(!out)fail("member search output");
    if(!bf_people_index(out,archive,"/people","member2048",0,30))fail("member search disappeared");
    page=read_rendered(out);
    if(!strstr(page,"member2048.aurekai.work")||strstr(page,"data-person-subject='person:member2047'"))
        fail("member search merged unrelated people");
    free(page);
    out=tmpfile();if(!out)fail("member archive-absent output");
    if(!bf_people_index(out,"/tmp/bonfyre-people-archive-missing-by-design","/people","member2048",0,30))
        fail("CMS member homepage incorrectly depends on archive");
    page=read_rendered(out);
    if(!strstr(page,"member2048.aurekai.work"))fail("archive absence erased published profile");
    free(page);
    puts("people published-member paging: 2048 synthetic identities, four-host grouping, search, archive independence: PASS");
}
static void people_lookup_scaling(const char *archive, const char *cms) {
    sqlite3 *db = NULL;
    const char *items =
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2000)"
        " INSERT INTO item(source,key,title,kind,publisher,at,by,family)"
        " SELECT 'source-one','scale-'||i,'Scale witness','article','One News',"
        "1760000000,'Alex Rivera','news' FROM n";
    const char *sources =
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<4000)"
        " INSERT INTO internet_source"
        " SELECT 'scale-source-'||i,'published','Other label '||i,'Other publisher '||i FROM n;"
        "INSERT INTO internet_source VALUES"
        "('unrelated-source','published','Robin Shah','Alex Rivera'),"
        "('source-one','draft','Alex Rivera','Alex Rivera');";
    if (sqlite3_open(archive, &db) != SQLITE_OK ||
        sqlite3_exec(db, items, NULL, NULL, NULL) != SQLITE_OK)
        fail("could not populate item scaling fixture");
    sqlite3_close(db); db = NULL;
    if (sqlite3_open(cms, &db) != SQLITE_OK ||
        sqlite3_exec(db, sources, NULL, NULL, NULL) != SQLITE_OK)
        fail("could not populate source scaling fixture");
    sqlite3_close(db);
    people_query_ticks = 0;
    people_query_metering = 1;
    FILE *out = tmpfile();
    if (!out) fail("could not open scaling output");
    int shown = bf_people_index(out, archive, "/people", "", 0, 30);
    people_query_metering = 0;
    char *body = read_rendered(out);
    if (people_query_ticks > 4000)
        fail("People exceeded 4,000,000 SQLite VM steps for 2,000 items and 4,000 sources");
    if (shown != 3 || !strstr(body, "who=Alex%20Rivera&amp;source=source-one") ||
        !strstr(body, "who=Robin%20Shah&amp;source=source-three") ||
        strstr(body, "What A Day") || strstr(body, "Taylor Stitch"))
        fail("scaled lookup changed exact-source or draft boundaries");
    if(sqlite3_open(archive,&db)!=SQLITE_OK || sqlite3_exec(db,BF_ITEM_CREDIT_INDEXES,NULL,NULL,NULL)!=SQLITE_OK)
        fail("archive owner credit read indexes");
    sqlite3_close(db);
    FILE *indexed=tmpfile();if(!indexed)fail("indexed output");
    if(bf_people_index(indexed,archive,"/people","",0,30)!=shown)fail("indexed credit count changed");
    char *indexed_body=read_rendered(indexed);
    if(strcmp(body,indexed_body))fail("credit read indexes changed rendered evidence or source boundaries");
    free(indexed_body);
    free(body);
    printf("people lookup scaling: %u SQLite VM steps; budget 4000000: PASS\n",
           people_query_ticks * 1000);
}

int main(void) {
    char path[] = "/tmp/bonfyre-people-projection-XXXXXX";
    char cms_path[] = "/tmp/bonfyre-people-cms-XXXXXX";
    int fd = mkstemp(path), cms_fd = mkstemp(cms_path);
    sqlite3 *db = NULL; char *index, *one, *legacy;
    char host_a[128],host_b[128],host_again[128];
    if(!bf_credit_profile_host("Alex Rivera","source-one",host_a) ||
       !bf_credit_profile_host("Alex Rivera","source-two",host_b) ||
       !bf_credit_profile_host("Alex Rivera","source-one",host_again) ||
       strcmp(host_a,host_again) || !strcmp(host_a,host_b) ||
       strncmp(host_a,"credited-alex-rivera-",21) ||
       !strstr(host_a,".aurekai.work") ||
       bf_credit_profile_host("Alex Rivera","",host_again))
        fail("source-credit host lost deterministic exact-source boundary");
    const char *sql =
        "CREATE TABLE item(source TEXT,key TEXT,title TEXT,kind TEXT,publisher TEXT,at INTEGER,by TEXT,family TEXT DEFAULT 'news');"
        "INSERT INTO item(source,key,title,kind,publisher,at,by) VALUES"
        "('source-one','a','Story One','article','One News',1760000030,'Alex Rivera'),"
        "('source-one','b','Story Two','article','One News',1760000020,'Alex Rivera'),"
        "('source-two','c','Other Alex Story','article','Two News',1760000010,'Alex Rivera'),"
        "('source-three','d','Another Name','article','Three News',1760000005,'Robin Shah'),"
        "('source-three','e','Domain credit','article','Three News',1760000004,'npr.org'),"
        "('source-three','f','Mailbox credit','article','Three News',1760000003,'webmaster@example.edu'),"
        "('source-three','g','Outlet credit','article','Three News',1760000002,'Three News'),"
        "('source-three','h','Show credit','article','Three News',1760000001,'Pod Save America Podcast'),"
        "('source-three','i','Handle credit','article','Three News',1760000000,'Rokket');"
        "INSERT INTO item VALUES('source-show','k','Episode title','episode','Crooked Media',1760000000,'What A Day','podcasts');"
        "INSERT INTO item VALUES('source-brand','l','Jacket title','product','Retailer',1760000000,'Taylor Stitch','style');"
        "INSERT INTO item VALUES('source-four','j','Recall notice','recall','Four Agency',1760000040,'Best Friends Bakery','recalls');";
    const char *cms_sql =
        "CREATE TABLE site_presentation(host TEXT,subject_address TEXT,status TEXT,namespace TEXT,profile TEXT);"
        "INSERT INTO site_presentation VALUES"
        "('nick.aurekai.life','person:nick','published','root','member-profile-v1'),"
        "('nick.aurekai.live','person:nick','published','root','member-profile-v1'),"
        "('nick.aurekai.work','person:nick','published','root','member-profile-v1'),"
        "('alex.aurekai.work','person:alex','published','root','member-profile-v1'),"
        "('draft.aurekai.work','person:draft','draft','root','member-profile-v1');"
        "CREATE TABLE internet_source(source TEXT,status TEXT,label TEXT,publisher_name TEXT);"
        "INSERT INTO internet_source VALUES"
        "('source-show','published','What A Day','Crooked Media'),"
        "('source-brand','published','Taylor Stitch products','Taylor Stitch');";
    if (fd < 0 || cms_fd < 0) fail("could not reserve fixture path");
    close(fd); close(cms_fd);
    if (sqlite3_open(path, &db) != SQLITE_OK || sqlite3_exec(db, sql, NULL, NULL, NULL) != SQLITE_OK)
        fail("could not create archive fixture");
    sqlite3_close(db);
    if (sqlite3_open(cms_path, &db) != SQLITE_OK || sqlite3_exec(db, cms_sql, NULL, NULL, NULL) != SQLITE_OK)
        fail("could not create cms fixture");
    sqlite3_close(db);
    if (setenv("BONFYRE_CMS_DB", cms_path, 1) != 0) fail("could not set cms fixture");

    {
        char home[128]={0};
        if(!bf_public_person_home("person:nick",home,sizeof(home)) || strcmp(home,"nick.aurekai.work"))fail("published Work person did not own the canonical home");
        if(!bf_public_person_home("person:alex",home,sizeof(home)) || strcmp(home,"alex.aurekai.work"))fail("second person did not use their own Work home");
        if(bf_public_person_home("person:draft",home,sizeof(home)) || bf_public_person_home("person:missing",home,sizeof(home)) ||
           bf_public_person_home("person:Alex Rivera",home,sizeof(home)) || bf_public_person_home("person:nick/evil",home,sizeof(home)))
           fail("unpublished, missing or unclaimed name acquired a public home");
        if(sqlite3_open(cms_path,&db)!=SQLITE_OK)fail("member home CMS fixture");
        if(sqlite3_exec(db,"UPDATE site_presentation SET status='draft' WHERE host='nick.aurekai.work'",NULL,NULL,NULL)!=SQLITE_OK)fail("pause published Work home");
        if(!bf_public_person_home("person:nick",home,sizeof(home)) || strcmp(home,"nick.aurekai.life"))fail("no published Work home must select an existing alternate territory");
        if(sqlite3_exec(db,"UPDATE site_presentation SET status='published' WHERE host='nick.aurekai.work'",NULL,NULL,NULL)!=SQLITE_OK)fail("restore Work home");
        if(sqlite3_exec(db,"INSERT INTO site_presentation VALUES('nick.aurekai.work','person:impersonator','published','root','member-profile-v1')",NULL,NULL,NULL)!=SQLITE_OK)fail("conflicting host fixture");
        if(!bf_public_person_home("person:nick",home,sizeof(home)) || strcmp(home,"nick.aurekai.life"))fail("conflicting published host was trusted as someone else's Work profile");
        if(sqlite3_exec(db,"DELETE FROM site_presentation WHERE subject_address='person:impersonator'",NULL,NULL,NULL)!=SQLITE_OK)fail("remove conflicting host fixture");
        sqlite3_close(db);db=NULL;
    }
    index = render_index(path);
    if (!strstr(index, "3 publisher contributor subaccounts</strong> from what this estate reads, across 3 sources"))
        fail("index did not count credited names and their sources");
    if (!strstr(index, "2 people</strong> on Aurekai with 4 pages"))
        fail("overview did not distinguish addressed people from their pages");
    if (occurrences(index, "data-person-subject='person:nick'") != 1 ||
        !strstr(index, "nick.aurekai.life") || !strstr(index, "nick.aurekai.live") || !strstr(index, "data-profile-primary href='https://nick.aurekai.work/'"))
        fail("published hosts for one exact subject were not grouped");
    if (strstr(index, "draft.aurekai.work") || strstr(index, "person:draft"))
        fail("draft person presentation escaped onto public people surface");
    if (!strstr(index, "who=Alex%20Rivera&amp;source=source-one") || !strstr(index, "who=Alex%20Rivera&amp;source=source-two"))
        fail("index did not address same spelling at each source separately");
    if (!strstr(index, "data-relationship-review") ||
        !strstr(index, "/relationships?context=self&amp;who=Alex%20Rivera&amp;source=source-one") ||
        !strstr(index, "/relationships?context=self&amp;who=Alex%20Rivera&amp;source=source-two"))
        fail("relationship continuations collapsed source-bound observations");
    if (!strstr(index, "One News</a>") || !strstr(index, "Latest: <a href='/o/source-one/a'>Story One</a>"))
        fail("index named sources by their feed id or lost the latest piece");
    if (!strstr(index, "not taken to be one person") || !strstr(index, "data-identity-boundary"))
        fail("source credits lost their public evidence boundary");
    if (strstr(index, "npr.org") || strstr(index, "webmaster@") || strstr(index, "data-credit-name='Three News'") || strstr(index, "Pod Save America"))
        fail("domains, mailboxes, outlets or shows were listed as people");
    if (strstr(index, "Rokket") || strstr(index, "Best Friends Bakery"))
        fail("single handles or non-author families were listed as people");
    if (strstr(index, "What A Day") || strstr(index, "Taylor Stitch"))
        fail("exact source self-credits were listed as people");
    {
        char sole_host[128],direct[256];
        if(!bf_credit_profile_host("Robin Shah","source-three",sole_host))fail("unique source profile host");
        snprintf(direct,sizeof(direct),"data-credit-name='Robin Shah' data-publisher-account='' data-publisher-subaccount=''><h3><a href='https://%s/' data-profile-host='%s'",sole_host,sole_host);
        if(!strstr(index,direct))fail("single-source People name did not open its public profile");
        char alex_a[128],alex_b[128],link_a[256],link_b[256];
        if(!bf_credit_profile_host("Alex Rivera","source-one",alex_a)||!bf_credit_profile_host("Alex Rivera","source-two",alex_b))fail("source profile hosts");
        snprintf(link_a,sizeof(link_a),"data-credit-name='Alex Rivera' data-publisher-account='' data-publisher-subaccount=''><h3><a href='https://%s/' data-profile-host='%s'",alex_a,alex_a);
        snprintf(link_b,sizeof(link_b),"data-credit-name='Alex Rivera' data-publisher-account='' data-publisher-subaccount=''><h3><a href='https://%s/' data-profile-host='%s'",alex_b,alex_b);
        if(!strstr(index,link_a)||!strstr(index,link_b)||!strcmp(alex_a,alex_b))
            fail("each source credit must open its own public profile");
        snprintf(link_a,sizeof(link_a),"data-credit-profile href='https://%s/'",alex_a);
        snprintf(link_b,sizeof(link_b),"data-credit-profile href='https://%s/'",alex_b);
        if(!strstr(index,link_a)||!strstr(index,link_b))fail("source-profile continuations remained on legacy dossier URLs");
    }
    free(index);

    one = render_person(path, "Alex Rivera", "source-one");
    if (!strstr(one, "Story One") || !strstr(one, "Story Two") || strstr(one, "Other Alex Story"))
        fail("source-bound page crossed a publisher boundary");
    if (!strstr(one, "Credited maker") ||
        !strstr(one, "/relationships?context=self&amp;who=Alex%20Rivera&amp;source=source-one") ||
        !strstr(one, "Keep track of them"))
        fail("source-bound page lost its account relationship continuation");
    free(one);

    legacy = render_person(path, "Alex Rivera", "");
    if (!strstr(legacy, "equal spellings are not merged into a person") ||
        !strstr(legacy, "source-one") || !strstr(legacy, "source-two") || strstr(legacy, "Story One"))
        fail("name-only page inferred identity instead of offering source choices");
    if (!strstr(legacy, "data-source-chooser") || occurrences(legacy, "data-source-observation") != 2)
        fail("source chooser did not expose exact observation choices");
    free(legacy);

    people_member_paging(path,cms_path);
    people_lookup_scaling(path, cms_path);

    unsetenv("BONFYRE_CMS_DB");
    unlink(path);
    unlink(cms_path);
    puts("people projection: PASS");
    return 0;
}
