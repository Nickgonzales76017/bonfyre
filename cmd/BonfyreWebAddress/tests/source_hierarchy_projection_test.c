#define _POSIX_C_SOURCE 200809L
#include "source_hierarchy_projection.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>

#ifdef HIERARCHY_COMPARE_ORIGINAL
int bf_source_hierarchy_emit_original(FILE *, const char *, const char *const *, size_t, const char *, unsigned, unsigned);
#endif

static int must(const char *html,const char *needle,const char *why){
    if(strstr(html,needle))return 1;
    fprintf(stderr,"source hierarchy projection: missing %s (%s)\n",needle,why);return 0;
}
static int must_not(const char *html,const char *needle,const char *why){
    if(!strstr(html,needle))return 1;
    fprintf(stderr,"source hierarchy projection: unexpected %s (%s)\n",needle,why);return 0;
}

/* Hashes are captured from the original projection, including every HTML byte. */
static int parity_fixture(int indexed, int nocase, unsigned limit, uint64_t expected) {
    char path[]="/tmp/bonfyre-hierarchy-parity-XXXXXX", buf[4096];
    int fd=mkstemp(path), ok=1, rendered; sqlite3 *db=NULL; FILE *out=NULL;
    uint64_t hash=UINT64_C(14695981039346656037); size_t n, i;
    const char *readers[]={"beta", "alpha", "missing", "beta"};
    if(fd<0)return 0; close(fd);
    if(sqlite3_open(path,&db)!=SQLITE_OK){unlink(path);return 0;}
    const char *schema = nocase ?
      "CREATE TABLE channel(channel_id TEXT PRIMARY KEY,family_id TEXT COLLATE NOCASE,root_id TEXT,address TEXT,title TEXT,state TEXT COLLATE NOCASE,virtual INTEGER,material TEXT,contributions INTEGER,first_seen TEXT,last_contributed TEXT);" :
      "CREATE TABLE channel(channel_id TEXT PRIMARY KEY,family_id TEXT,root_id TEXT,address TEXT,title TEXT,state TEXT,virtual INTEGER,material TEXT,contributions INTEGER,first_seen TEXT,last_contributed TEXT);";
    const char *data =
      "CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT,name TEXT,identity TEXT);"
      "CREATE TABLE family(family_id TEXT PRIMARY KEY,root_id TEXT,protocol TEXT);"
      "CREATE TABLE channel_census(channel_id TEXT,census_source TEXT,material TEXT);"
      "INSERT INTO root VALUES('r1','one.test','One',''),('r2','two.test','Two','');"
      "INSERT INTO family VALUES('f1','r1','rss'),('f2','r1','ics'),('f3','r2','rss');"
      "INSERT INTO channel VALUES"
      "('a','f1','r1','https://one/a','A','active',0,'pub',8,'1','9'),"
      "('b','f1','r1','https://one/b','B','admitted',0,'',2,'1','8'),"
      "('c','f1','r2','https://one/c','C','inactive',1,'',1,'1','7'),"
      "('d','f2','r1','https://one/d','D','active',0,'',4,'1','6'),"
      "('e','f3','r2','https://two/e','E','active',0,'',6,'1','5'),"
      "('f','F1','r1','https://one/f','F','ACTIVE',0,'',3,'1','4');"
      "INSERT INTO channel_census VALUES('a','alpha','x'),('a','beta','y'),('a','beta','z'),"
      "('b','other',''),('d','beta',''),('e','alpha',''),('f','other','');";
    if(sqlite3_exec(db,schema,NULL,NULL,NULL)!=SQLITE_OK || sqlite3_exec(db,data,NULL,NULL,NULL)!=SQLITE_OK ||
       (indexed && sqlite3_exec(db,"CREATE INDEX ix_cf ON channel(family_id); CREATE INDEX ix_fr ON family(root_id); CREATE INDEX ix_cs ON channel_census(census_source); CREATE INDEX ix_cc ON channel_census(channel_id);",NULL,NULL,NULL)!=SQLITE_OK)) ok=0;
    sqlite3_close(db);
    out=tmpfile(); if(!out)ok=0;
    if(ok){
        rendered=bf_source_hierarchy_emit(out,path,readers,4,"/channels",8,limit);
        if(rendered!=2)ok=0;
#ifdef HIERARCHY_COMPARE_ORIGINAL
        {
            FILE *old=tmpfile(); int a,b,old_rendered;
            if(!old)ok=0;
            else {
                old_rendered=bf_source_hierarchy_emit_original(old,path,readers,4,"/channels",8,limit);
                if(old_rendered!=rendered)ok=0;
                rewind(old);rewind(out);
                do { a=fgetc(old);b=fgetc(out);if(a!=b){ok=0;break;} } while(a!=EOF);
                if(!ok)fprintf(stderr,"original output differs: indexed=%d nocase=%d limit=%u\n",indexed,nocase,limit);
                fclose(old);
            }
        }
#endif
        rewind(out); while((n=fread(buf,1,sizeof(buf),out))) for(i=0;i<n;i++){hash^=(unsigned char)buf[i];hash*=UINT64_C(1099511628211);}
        if(hash!=expected){fprintf(stderr,"parity indexed=%d nocase=%d limit=%u: %016" PRIx64 " expected %016" PRIx64 "\n",indexed,nocase,limit,hash,expected);ok=0;}
    }
    if(out)fclose(out); unlink(path); return ok;
}
int main(void){
    char path[]="/tmp/bonfyre-source-hierarchy-XXXXXX";int fd=mkstemp(path),ok=1;sqlite3 *db=NULL;FILE *out=NULL;char *html=NULL;long n;
    if(fd<0)return 1;close(fd);
    if(sqlite3_open(path,&db)!=SQLITE_OK)return 1;
    const char *schema=
      "CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT,name TEXT,identity TEXT);"
      "CREATE TABLE family(family_id TEXT PRIMARY KEY,root_id TEXT,protocol TEXT);"
      "CREATE TABLE channel(channel_id TEXT PRIMARY KEY,family_id TEXT,root_id TEXT,address TEXT,title TEXT,state TEXT,virtual INTEGER,material TEXT,contributions INTEGER,first_seen TEXT,last_contributed TEXT);"
      "CREATE TABLE channel_census(channel_id TEXT,census_source TEXT,material TEXT);"
      "INSERT INTO root VALUES('r1','publisher.test','Publisher','Q1');"
      "INSERT INTO family VALUES('f-rss','r1','rss');"
      "INSERT INTO family VALUES('f-ics','r1','ics');"
      "INSERT INTO channel VALUES('c1','f-rss','r1','https://publisher.test/feed','Main','active',0,'publication',12,'2026','2026');"
      "INSERT INTO channel VALUES('c2','f-rss','r1','https://publisher.test/music','Music','admitted',0,'publication',0,'2026','');"
      "INSERT INTO channel VALUES('c3','f-ics','r1','https://publisher.test/events.ics','Events','active',0,'event',5,'2026','2026');"
      "INSERT INTO channel_census VALUES('c1','reader-news','publication');"
      "INSERT INTO channel_census VALUES('c1','reader-news-alt','publication');"
      "INSERT INTO channel_census VALUES('c2','reader-music','publication');"
      "INSERT INTO channel_census VALUES('c3','reader-events','event');";
    if(sqlite3_exec(db,schema,NULL,NULL,NULL)!=SQLITE_OK)return 1;sqlite3_close(db);db=NULL;
    out=tmpfile();if(!out)return 1;
    const char *readers[]={"reader-news"};
    if(bf_source_hierarchy_emit(out,path,readers,1,"/channels",8,8)!=1)return 1;
    fflush(out);fseek(out,0,SEEK_END);n=ftell(out);rewind(out);html=calloc((size_t)n+1,1);if(!html)return 1;
    if(fread(html,1,(size_t)n,out)!=(size_t)n)return 1;
    ok&=must(html,"data-source-edge","exact connected branch");
    ok&=must(html,"/sources?q=reader-news","first-party reader continuation");
    ok&=must(html,"Used here by","edge evidence label");
    ok&=must(html,"Main","connected channel");
    ok&=must(html,"source-related","explicit sibling branch");
    ok&=must(html,"Music","same-publisher sibling retained");
    ok&=must(html,"not used on this page","no implied sibling contribution");
    ok&=must_not(html,"reader-music","unrelated reader must not be projected as page evidence");
    ok&=must_not(html,"reader-news-alt","reader not participating in the current page must not be projected");
    {
      const char *mainp=strstr(html,">Main</a>"),*related=strstr(html,"source-related"),*music=strstr(html,">Music</a>");
      if(!(mainp&&related&&music&&mainp<related&&related<music)){fprintf(stderr,"source hierarchy projection: connected branch is not before related sibling branch\n");ok=0;}
    }
    free(html);fclose(out);unlink(path);
    ok &= parity_fixture(0,0,8,UINT64_C(0x51240f0e279cc582));
    ok &= parity_fixture(1,0,2,UINT64_C(0xf2d538edbb470c5b));
    ok &= parity_fixture(0,1,8,UINT64_C(0xeffe6087f88f7083));
    ok &= parity_fixture(1,1,2,UINT64_C(0xb01e4a3f37d07762));
    if(ok)puts("source hierarchy projection: PASS");
    return ok?0:1;
}
