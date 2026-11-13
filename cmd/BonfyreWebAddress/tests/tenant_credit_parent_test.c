#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "tenant_host_binding.h"

/* Isolate the publication owner's parent resolver: the digest host generator
 * is exercised separately by people_projection_test.c with real SHA-256. */
static int bf_credit_profile_host(const char *by,const char *source,char out[128]){
    if(!by||!source||!strcmp(by,"")||!strcmp(source,""))return 0;
    snprintf(out,128,"credited-maker.stories.example.net");return 1;
}
#include "credit_profile_parent.h"

#define CHECK(expr, why) do {if(!(expr)){fprintf(stderr,"FAIL: %s\n",why);return 1;}} while(0)
static int exec(sqlite3 *db,const char *sql){return sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK;}

int main(void) {
    BfCreditProfileParent parent;
    CHECK(bf_tenant_hosts_load(NULL)==0,"legacy mode");
    bf_credit_profile_parent(NULL,NULL,"Writer","source",&parent);
    CHECK(!strcmp(parent.subject,"institution:aurekai:work"),"legacy managed owner remains compatible");
    CHECK(!strcmp(parent.host,"aurekai.work"),"legacy managed host remains compatible");

    char mapfile[]="/tmp/bf-credit-tenant-XXXXXX";
    int fd=mkstemp(mapfile);CHECK(fd>=0,"mapfile create");
    FILE *fp=fdopen(fd,"w");CHECK(fp,"mapfile open");
    CHECK(fputs("research stories.example.net login.stories.example.net\n",fp)>=0,"mapfile content");
    CHECK(!fclose(fp),"mapfile flush");
    CHECK(bf_tenant_hosts_load(mapfile)==1,"generic tenant configuration");

    bf_credit_profile_parent(NULL,NULL,"Writer","source",&parent);
    CHECK(!parent.subject[0]&&!parent.host[0]&&!parent.label[0],
          "configured tenant may not inherit Aurekai as unclaimed publisher");

    sqlite3 *cms=NULL,*archive=NULL;
    CHECK(sqlite3_open(":memory:",&cms)==SQLITE_OK,"cms");
    CHECK(sqlite3_open(":memory:",&archive)==SQLITE_OK,"archive");
    CHECK(exec(cms,
      "CREATE TABLE publisher_account(account_id TEXT,account_subject TEXT,name TEXT,"
      "publisher_subject TEXT,publisher_profile_url TEXT,namespace TEXT,status TEXT,source TEXT);"
      "CREATE TABLE publisher_subaccount(id INTEGER PRIMARY KEY,publisher_account_id TEXT,namespace TEXT,"
      "subaccount_id TEXT,account_subject TEXT,source TEXT,display_name TEXT,host TEXT,"
      "status TEXT,identity_state TEXT);"
      "CREATE TABLE site_presentation(host TEXT,subject_address TEXT,status TEXT,"
      "namespace TEXT,profile TEXT);"),
      "CMS schema");
    CHECK(exec(archive,
      "CREATE TABLE item(source TEXT,by TEXT,publisher TEXT);"
      "INSERT INTO item VALUES('news','Writer','Paper Desk');"
      "INSERT INTO item VALUES('news','Writer','Paper Desk');"),
      "archive schema");

    /* Refuse an unrelated host even if its account row appears published. */
    CHECK(exec(cms,
      "INSERT INTO publisher_account VALUES"
      "('acct-1','institution:publisher:desk','Paper Desk',"
      "'institution:publisher:desk','https://outside.other.net/','root','published','news');"
      "INSERT INTO publisher_subaccount VALUES"
      "(NULL,'acct-1','root','sub-1','institution:publisher:desk:writer',"
      "'news','Writer','credited-maker.stories.example.net','published','unclaimed');"),
      "account and subaccount");
    bf_credit_profile_parent(cms,archive,"Writer","news",&parent);
    CHECK(parent.canonical_subaccount==1,"matching exact account closure");
    CHECK(!strcmp(parent.subject,"institution:publisher:desk"),"canonical published institution");
    CHECK(!parent.publisher_site&&!parent.host[0],
          "untrusted profile URL cannot be turned into an admitted site");

    CHECK(exec(cms,
      "UPDATE publisher_account SET publisher_profile_url="
      "'https://credited-maker.stories.example.net/'"),
      "switch to member host");
    bf_credit_profile_parent(cms,archive,"Writer","news",&parent);
    CHECK(parent.publisher_site==1 &&
          !strcmp(parent.host,"credited-maker.stories.example.net"),
          "exact publisher site under selected tenant is admitted");
    CHECK(!strcmp(parent.label,"Paper Desk"),"actual publisher label");

    CHECK(exec(cms,
      "UPDATE publisher_account SET publisher_profile_url="
      "'https://login.stories.example.net/'"),
      "switch to account host");
    bf_credit_profile_parent(cms,archive,"Writer","news",&parent);
    CHECK(!parent.publisher_site,"login origin may not become public publisher site");

    /* No source-pinned publisher account -> exact archive+published-site lookup.
     * A different institution domain must never be selected by its spelling. */
    CHECK(exec(cms,
      "DELETE FROM publisher_subaccount;"
      "INSERT INTO site_presentation VALUES"
      "('paper.bad-example.net','institution:publisher:paper-desk','published','root','newsroom'),"
      "('paper.stories.example.net','institution:publisher:paper-desk','published','root','newsroom');"),
      "site-presentation fixture");
    bf_credit_profile_parent(cms,archive,"Writer","news",&parent);
    CHECK(parent.publisher_site && !strcmp(parent.host,"paper.stories.example.net"),
          "only installed tenant's published profile wins");

    CHECK(exec(cms,
      "INSERT INTO site_presentation VALUES"
      "('paper2.stories.example.net','institution:publisher:paper-desk','published','root','newsroom')"),
      "second admitted publication");
    bf_credit_profile_parent(cms,archive,"Writer","news",&parent);
    CHECK(!parent.subject[0]&&!parent.publisher_site,
          "two same-publisher admitted sites are ambiguous and do not grant ownership");

    CHECK(bf_tenant_hosts_load("/missing/current-tenant-host-map")==-1,"invalid configured source");
    bf_credit_profile_parent(NULL,NULL,"Writer","news",&parent);
    CHECK(!parent.subject[0],"invalid tenant source cannot fall back to Aurekai");
    sqlite3_close(cms);sqlite3_close(archive);
    unlink(mapfile);
    puts("tenant_credit_parent_test: PASS");
    return 0;
}
