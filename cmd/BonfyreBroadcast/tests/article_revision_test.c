#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include "../src/narration_intake.h"
#define REQUIRE(x) do { if(!(x)){fprintf(stderr,"article revision check failed at line %d\n",__LINE__);return 1;} } while(0)
int main(int argc,char **argv){
 sqlite3 *db=NULL;char draft[65],current[65],other[65];
 if(argc==4){REQUIRE(sqlite3_open_v2(argv[1],&db,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK);
  REQUIRE(bf_narration_article_revision(db,argv[2],argv[3],"published",current));puts(current);sqlite3_close(db);return 0;}
 REQUIRE(sqlite3_open(":memory:",&db)==SQLITE_OK);
 REQUIRE(sqlite3_exec(db,
  "CREATE TABLE site_page(id INTEGER,namespace TEXT,route TEXT,title TEXT,heading TEXT,lede TEXT,media_subject_ref TEXT,editorial_kind TEXT,status TEXT);"
  "CREATE TABLE site_fragment(id INTEGER,namespace TEXT,page TEXT,heading TEXT,body TEXT,kind TEXT,status TEXT,audience TEXT,position INTEGER,semantic_tags TEXT,coordinate TEXT,slot TEXT);"
  "INSERT INTO site_page VALUES(1,'site-a','/story','Story','Story','A lede','article:story','article','draft');"
  "INSERT INTO site_fragment VALUES(1,'site-a','/story','Section','A complete public passage.','profile-story','draft','public',1,'{}','one','showcase');"
  "INSERT INTO site_fragment VALUES(2,'site-a','/story','Private','Private text','profile-story','draft','private',2,'{}','two','showcase');",NULL,NULL,NULL)==SQLITE_OK);
 REQUIRE(bf_narration_article_revision(db,"site-a","article:story","draft",draft));
 REQUIRE(!bf_narration_article_revision(db,"site-b","article:story","draft",other));
 REQUIRE(!bf_narration_article_revision(db,"site-a","article:story","published",other));
 REQUIRE(sqlite3_exec(db,"UPDATE site_fragment SET body='Changed private text' WHERE id=2;",NULL,NULL,NULL)==SQLITE_OK);
 REQUIRE(bf_narration_article_revision(db,"site-a","article:story","draft",current)&&!strcmp(draft,current));
 REQUIRE(sqlite3_exec(db,"UPDATE site_page SET status='published'; UPDATE site_fragment SET status='published';",NULL,NULL,NULL)==SQLITE_OK);
 REQUIRE(bf_narration_article_revision(db,"site-a","article:story","published",current)&&!strcmp(draft,current));
 REQUIRE(sqlite3_exec(db,"UPDATE site_fragment SET semantic_tags='{\"authoring_v1\":{\"editorial\":[{\"type\":\"paragraph\",\"data\":{\"text\":\"Changed text\"}}]}}' WHERE id=1;",NULL,NULL,NULL)==SQLITE_OK);
 REQUIRE(bf_narration_article_revision(db,"site-a","article:story","published",current)&&strcmp(draft,current));
 REQUIRE(sqlite3_exec(db,"INSERT INTO site_page SELECT 3,namespace,route,title,heading,lede,media_subject_ref,editorial_kind,status FROM site_page WHERE id=1;",NULL,NULL,NULL)==SQLITE_OK);
 REQUIRE(!bf_narration_article_revision(db,"site-a","article:story","published",other));
 sqlite3_close(db);puts("Article revision: draft/public parity, public editor changes and isolation: PASS");return 0;
}
