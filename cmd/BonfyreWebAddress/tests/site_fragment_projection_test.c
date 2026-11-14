#include "../src/site_fragment_projection.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void die(const char *msg) { fprintf(stderr,"site fragment projection: %s\n",msg); exit(1); }
static void sql(sqlite3 *db,const char *q) {
    char *err=NULL;
    if (sqlite3_exec(db,q,NULL,NULL,&err)!=SQLITE_OK) {
        fprintf(stderr,"sqlite: %s\n",err?err:"unknown"); sqlite3_free(err); exit(1);
    }
}
static void expect(int ok,const char *msg) { if(!ok) die(msg); }

int main(void) {
    sqlite3 *db=NULL; char out[8192]; BfSiteFragmentProjectionStats s;
    BfSiteFragmentProjectionRequest r={"aurekai","protocols","hero","public",BF_SITE_FRAGMENT_HTMX};
    expect(sqlite3_open(":memory:",&db)==SQLITE_OK,"open");
    sql(db,
      "CREATE TABLE site_fragment("
      "id INTEGER PRIMARY KEY,namespace TEXT,coordinate TEXT,page TEXT,slot TEXT,audience TEXT,"
      "kind TEXT,position INTEGER,eyebrow TEXT,heading TEXT,body TEXT,label TEXT,href TEXT,"
      "status TEXT,projection_contract TEXT);"
      "INSERT INTO site_fragment VALUES"
      "(1,'aurekai','life:protocols:hero:public:default','protocols','hero','public','copy',10,"
      "'Open <world>','One & many','Safe <script>alert(1)</script>','Read','/notes',"
      "'published','{\"html\":true,\"htmx\":true}'),"
      "(2,'aurekai','life:protocols:hero:public:draft','protocols','hero','public','copy',20,"
      "NULL,'SECRET DRAFT','must not leak',NULL,NULL,'draft','{\"html\":true,\"htmx\":true}'),"
      "(3,'aurekai','life:protocols:hero:self:default','protocols','hero','self','copy',10,"
      "NULL,'SELF ONLY','must not cross audience',NULL,NULL,'published','{\"html\":true,\"htmx\":true}'),"
      "(4,'aurekai','life:protocols:hero:public:html-only','protocols','hero','public','copy',30,"
      "NULL,'HTML ONLY','must not cross carrier contract',NULL,NULL,'published','{\"html\":true,\"htmx\":false}');");
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==1,"published HTMX projection");
    expect(s.exact_coordinate_rows==3,"exact coordinate count includes authored draft/html-only");
    expect(s.published_rows==2,"published count");
    expect(s.rows==1 && s.contract_rows==1,"carrier contract count");
    expect(strstr(out,"One &amp; many")!=NULL,"heading escaped");
    expect(strstr(out,"&lt;script&gt;alert(1)&lt;/script&gt;")!=NULL,"body escaped");
    expect(strstr(out,"SECRET DRAFT")==NULL,"draft leaked");
    expect(strstr(out,"SELF ONLY")==NULL,"audience leaked");
    expect(strstr(out,"HTML ONLY")==NULL,"carrier contract leaked");
    expect(strstr(out,"href=\"/notes\"")!=NULL,"safe href");
    r.slot="missing";
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==0,"miss should preserve caller fallback");
    expect(out[0]=='\0',"miss emitted bytes");
    r.slot="hero"; r.representation=BF_SITE_FRAGMENT_HTML;
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==1,"html projection");
    expect(strstr(out,"HTML ONLY")!=NULL,"html contract row missing");
    r.audience="self";
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==1,"self projection");
    expect(strstr(out,"SELF ONLY")!=NULL && strstr(out,"One &amp; many")==NULL,"self closure not isolated");
    /* Generated block grammar is consumed by native site-fragment projection. */
    sql(db,
      "INSERT INTO site_fragment VALUES"
      "(5,'aurekai','authored-allowed','protocols','authored','public','profile-card',10,"
      "NULL,'Allowed','Safe copy',NULL,NULL,'published','{\"html\":true}'),"
      "(6,'aurekai','authored-too-long','protocols','authored','public','profile-card',20,"
      "NULL,'Denied',printf('%0*d',5001,1),NULL,NULL,'published','{\"html\":true}');");
    r.audience="public";r.slot="authored";r.representation=BF_SITE_FRAGMENT_HTML;
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==1,"generated block contract projection");
    expect(s.rows==1 && s.published_rows==2,"native grammar rejects oversized authored block");
    expect(strstr(out,"Allowed")!=NULL && strstr(out,"Denied")==NULL,"oversized block leaked");
    /* Root-page authored profiles need actual slash routes, exact tenancy,
     * published status, representation contract and sanitized links. */
    sql(db,
      "INSERT INTO site_fragment VALUES"
      "(7,'site-one','root-hero','/','showcase','public','profile-hero',1,"
      "'From CMS','Real published profile','<not HTML>','Open','/work',"
      "'published','{\"html\":true}'),"
      "(8,'site-two','foreign-hero','/','showcase','public','profile-hero',1,"
      "NULL,'Other owner','Hidden',NULL,NULL,'published','{\"html\":true}'),"
      "(9,'site-one','draft-hero','/','showcase','public','profile-hero',2,"
      "NULL,'Private draft','Hidden',NULL,NULL,'draft','{\"html\":true}'),"
      "(10,'site-one','malicious-href','/','showcase','public','profile-action',3,"
      "NULL,'Safe copy','No open redirect','Unsafe','//attacker.test/steal',"
      "'published','{\"html\":true}');");
    r.namespace_name="site-one";r.page="/";r.slot="showcase";
    r.audience="public";r.representation=BF_SITE_FRAGMENT_HTML;
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==1,"root published profile");
    expect(strstr(out,"Real published profile")!=NULL,"native homepage rendered");
    expect(strstr(out,"&lt;not HTML&gt;")!=NULL,"root profile escaped");
    expect(strstr(out,"Other owner")==NULL&&strstr(out,"Private draft")==NULL,"profile isolation");
    expect(strstr(out,"//attacker.test")==NULL,"protocol relative link refused");
    r.page="/../../private";
    expect(bf_site_fragment_project(db,&r,out,sizeof(out),&s)==-1,"unsafe page path refused");
    sqlite3_close(db);
    puts("site fragment projection: PASS");
    return 0;
}
