#include "../src/native_publications_query.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned checks;
#define CHECK(value) do { checks++; if (!(value)) { fprintf(stderr,"FAIL line %d: %s\n",__LINE__,#value); exit(1); } } while (0)
/* Retained reference query: compare complete rows and ordering, including
 * source-join multiplicity. This fixture cannot change with the new query. */
static const char previous_query[] =
    "SELECT p.title,COALESCE(p.canonical_href,''),COALESCE(p.published_at,p.observed_at,''),"
    "COALESCE(i.says,p.source_ref) FROM publication_occurrence p "
    "LEFT JOIN internet_source i ON i.source=p.source_ref AND i.status='published' "
    "WHERE p.status='published' AND p.visibility='public' AND p.assertion_state='OBSERVED' "
    "AND (?2='' OR EXISTS (SELECT 1 FROM source_collection sc "
    "WHERE sc.status='published' "
    "AND EXISTS (SELECT 1 FROM json_each(sc.source_selector,'$.domains') domain_value "
    "WHERE domain_value.value=?2) "
    "AND EXISTS (SELECT 1 FROM json_each(sc.source_selector,'$.source_refs') source_value "
    "WHERE source_value.value=p.source_ref))) "
    "ORDER BY COALESCE(p.published_at,p.observed_at) DESC,p.id DESC LIMIT ?1";

static void execute(sqlite3 *db,const char *sql) {
    char *error=NULL;int rc=sqlite3_exec(db,sql,NULL,NULL,&error);
    if(rc!=SQLITE_OK)fprintf(stderr,"fixture SQL: %s\n",error?error:"failed");
    sqlite3_free(error);CHECK(rc==SQLITE_OK);
}
static sqlite3 *fixture(void) {
    sqlite3 *db=NULL;CHECK(sqlite3_open(":memory:",&db)==SQLITE_OK);
    execute(db,
        "CREATE TABLE publication_occurrence(id INTEGER PRIMARY KEY,title TEXT,canonical_href TEXT,published_at TEXT,observed_at TEXT,source_ref TEXT,status TEXT,visibility TEXT,assertion_state TEXT);"
        "CREATE TABLE internet_source(source TEXT,says TEXT,status TEXT);"
        "CREATE TABLE source_collection(status TEXT,source_selector TEXT);"
        "INSERT INTO internet_source VALUES('source-a','Publisher A','published'),('source-a','Withdrawn name','draft'),('source-b',NULL,'published');"
        "INSERT INTO publication_occurrence VALUES"
        "(1,'Old A','https://example.com/a1','2026-01-01','2026-10-01','source-a','published','public','OBSERVED'),"
        "(2,'New B',NULL,NULL,'2026-10-01','source-b','published','public','OBSERVED'),"
        "(3,'Tie A','https://example.com/a3','2026-10-01',NULL,'source-a','published','public','OBSERVED'),"
        "(4,'Uncollected',NULL,NULL,NULL,'uncollected','published','public','OBSERVED'),"
        "(5,'Draft',NULL,'2026-12-01',NULL,'source-a','draft','public','OBSERVED'),"
        "(6,'Private',NULL,'2026-12-01',NULL,'source-a','published','private','OBSERVED'),"
        "(7,'Candidate',NULL,'2026-12-01',NULL,'source-a','published','public','CANDIDATE'),"
        "(8,'Numeric source',NULL,'2026-01-08',NULL,'42','published','public','OBSERVED'),"
        "(9,'Empty source',NULL,'2026-01-09',NULL,'','published','public','OBSERVED'),"
        "(10,'Null source',NULL,'2026-01-10',NULL,NULL,'published','public','OBSERVED');"
        "INSERT INTO source_collection VALUES"
        "('published','{\"domains\":[\"discover\",\"food\",\"discover\"],\"source_refs\":[\"source-a\",\"source-a\",\"source-b\"]}'),"
        "('published','{\"domains\":[\"discover\"],\"source_refs\":[\"source-b\"]}'),"
        "('draft','{\"domains\":[\"discover\"],\"source_refs\":[\"uncollected\"]}'),"
        "('published','{\"domains\":[\"other\"],\"source_refs\":[\"uncollected\"]}'),"
        "('published','{\"domains\":[\"typed\"],\"source_refs\":[42,null,\"\"]}'),"
        "('published','{\"domains\":[\"empty\"],\"source_refs\":[]}'),"
        "('published','{}');");
    return db;
}
static sqlite3_stmt *prepare(sqlite3 *db,const char *sql,const char *domain,int limit) {
    sqlite3_stmt *st=NULL;int rc=sqlite3_prepare_v2(db,sql,-1,&st,NULL);
    if(rc!=SQLITE_OK)fprintf(stderr,"query preparation: %s\n",sqlite3_errmsg(db));
    CHECK(rc==SQLITE_OK);CHECK(sqlite3_bind_int(st,1,limit)==SQLITE_OK);
    CHECK(sqlite3_bind_text(st,2,domain,-1,SQLITE_STATIC)==SQLITE_OK);return st;
}
static int compare(sqlite3 *db,const char *domain,int limit,int *old_steps,int *new_steps) {
    sqlite3_stmt *old=prepare(db,previous_query,domain,limit),*next=prepare(db,bf_native_publications_query,domain,limit);
    int count=0,a,b;
    for(;;) {
        a=sqlite3_step(old);b=sqlite3_step(next);CHECK(a==b);
        if(a!=SQLITE_ROW)break;
        CHECK(sqlite3_column_count(old)==4&&sqlite3_column_count(next)==4);
        for(int i=0;i<4;i++) {
            CHECK(sqlite3_column_type(old,i)==sqlite3_column_type(next,i));
            int bytes=sqlite3_column_bytes(old,i);CHECK(bytes==sqlite3_column_bytes(next,i));
            if(bytes)CHECK(!memcmp(sqlite3_column_text(old,i),sqlite3_column_text(next,i),(size_t)bytes));
        }
        count++;
    }
    CHECK(a==SQLITE_DONE);
    if(old_steps)*old_steps=sqlite3_stmt_status(old,SQLITE_STMTSTATUS_VM_STEP,0);
    if(new_steps)*new_steps=sqlite3_stmt_status(next,SQLITE_STMTSTATUS_VM_STEP,0);
    CHECK(sqlite3_finalize(old)==SQLITE_OK);CHECK(sqlite3_finalize(next)==SQLITE_OK);return count;
}
static void semantics(void) {
    sqlite3 *db=fixture();
    CHECK(compare(db,"discover",20,NULL,NULL)==3);CHECK(compare(db,"food",20,NULL,NULL)==3);
    CHECK(compare(db,"discover",1,NULL,NULL)==1);CHECK(compare(db,"discover",0,NULL,NULL)==0);
    CHECK(compare(db,"unknown",20,NULL,NULL)==0);CHECK(compare(db,"empty",20,NULL,NULL)==0);
    CHECK(compare(db,"typed",20,NULL,NULL)==1);CHECK(compare(db,"other",20,NULL,NULL)==1);
    CHECK(compare(db,"",20,NULL,NULL)==7);
    sqlite3_stmt *st=prepare(db,bf_native_publications_query,"discover",20);
    CHECK(sqlite3_step(st)==SQLITE_ROW);CHECK(!strcmp((const char *)sqlite3_column_text(st,0),"Tie A"));
    CHECK(!strcmp((const char *)sqlite3_column_text(st,3),"Publisher A"));
    CHECK(sqlite3_step(st)==SQLITE_ROW);CHECK(!strcmp((const char *)sqlite3_column_text(st,0),"New B"));
    CHECK(!strcmp((const char *)sqlite3_column_text(st,3),"source-b"));sqlite3_finalize(st);
    execute(db,"INSERT INTO internet_source VALUES('source-a','Second published attribution','published');");
    CHECK(compare(db,"discover",20,NULL,NULL)==5);
    execute(db,"DELETE FROM source_collection;");CHECK(compare(db,"discover",20,NULL,NULL)==0);CHECK(compare(db,"",20,NULL,NULL)==9);
    CHECK(sqlite3_close(db)==SQLITE_OK);
}
static void work_bound(void) {
    sqlite3 *db=fixture();
    execute(db,"DELETE FROM publication_occurrence;DELETE FROM source_collection;DELETE FROM internet_source;"
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000) "
        "INSERT INTO publication_occurrence SELECT x,'Occurrence '||x,NULL,'2026-10-01',NULL,'outside-'||x,'published','public','OBSERVED' FROM n;"
        "WITH RECURSIVE refs(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM refs WHERE x<64), "
        "collections(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM collections WHERE x<12), "
        "selector(s) AS (SELECT json_object('domains',json_array('discover'),'source_refs',json_group_array('allowed-'||x)) FROM refs) "
        "INSERT INTO source_collection SELECT 'published',s FROM collections,selector;");
    int old_steps=0,new_steps=0;
    CHECK(compare(db,"discover",24,&old_steps,&new_steps)==0);
    CHECK(old_steps>new_steps*20);
    printf("native publication empty cut: old %d VM steps, new %d VM steps\n",old_steps,new_steps);
    execute(db,"UPDATE publication_occurrence SET source_ref='allowed-1' WHERE id=2000;");
    CHECK(compare(db,"discover",24,NULL,NULL)==1);
    CHECK(sqlite3_close(db)==SQLITE_OK);
}
int main(void) {
    CHECK(sqlite3_libversion_number()>=3035000);
    semantics();work_bound();printf("native publication query: %u checks passed\n",checks);return 0;
}
