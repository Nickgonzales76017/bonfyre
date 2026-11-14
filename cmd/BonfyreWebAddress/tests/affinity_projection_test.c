#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "../src/affinity_projection.h"

static void check(int ok, const char *message) {
    if (!ok) { fprintf(stderr, "affinity projection FAIL: %s\n", message); exit(1); }
}

static void execute(sqlite3 *db, const char *sql) {
    char *error = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &error) != SQLITE_OK) {
        fprintf(stderr, "SQL: %s\n", error ? error : "unknown");
        sqlite3_free(error);
        exit(1);
    }
}

int main(void) {
    sqlite3 *db = NULL;
    char kind[16];
    check(sqlite3_open(":memory:", &db) == SQLITE_OK, "db open");
    check(bf_affinity_status(db, "person:a", "person:b", kind, sizeof(kind)) == 0,
          "missing table reads as none");
    check(bf_affinity_mutual(db, "person:a", "person:b") == 0, "missing table not mutual");
    execute(db, "CREATE TABLE person_affinity (id INTEGER PRIMARY KEY, affinity TEXT,"
                "actor_subject TEXT, target_subject TEXT, kind TEXT, state TEXT,"
                "created_at TEXT, status TEXT);");
    check(bf_affinity_status(db, "person:a", "person:b", kind, sizeof(kind)) == 0,
          "no edge reads as none");
    check(bf_affinity_status(NULL, "person:a", "person:b", kind, sizeof(kind)) == -1,
          "null db refused");
    check(bf_affinity_status(db, "x", "person:b", kind, sizeof(kind)) == -1,
          "bad actor refused");
    check(bf_affinity_status(db, "person:a", "person:a", kind, sizeof(kind)) == 0,
          "self edge reads as none");
    check(bf_affinity_mutual(db, "person:a", "person:a") == 0, "self never mutual");
    execute(db, "INSERT INTO person_affinity VALUES"
                "(1,'a1','person:a','person:b','like','active','t','published'),"
                "(2,'a2','person:b','person:a','like','active','t','published'),"
                "(3,'a3','person:a','person:c','pass','active','t','published'),"
                "(4,'a4','person:a','person:d','superlike','active','t','published'),"
                "(5,'a5','person:e','person:a','like','withdrawn','t','published'),"
                "(6,'a6','person:a','person:f','like','active','t','draft');");
    check(bf_affinity_status(db, "person:a", "person:b", kind, sizeof(kind)) == 1 &&
          !strcmp(kind, "like"), "like reads");
    check(bf_affinity_status(db, "person:a", "person:c", kind, sizeof(kind)) == 1 &&
          !strcmp(kind, "pass"), "pass reads");
    check(bf_affinity_status(db, "person:a", "person:d", kind, sizeof(kind)) == 1 &&
          !strcmp(kind, "superlike"), "superlike reads");
    check(bf_affinity_status(db, "person:e", "person:a", kind, sizeof(kind)) == 0,
          "withdrawn reads as never-existed");
    check(bf_affinity_status(db, "person:a", "person:f", kind, sizeof(kind)) == 0,
          "draft reads as none");
    check(bf_affinity_mutual(db, "person:a", "person:b") == 1, "mutual likes");
    check(bf_affinity_mutual(db, "person:a", "person:c") == 0, "one-sided pass not mutual");
    check(bf_affinity_mutual(db, "person:a", "person:d") == 0, "one-sided superlike not mutual");
    execute(db, "INSERT INTO person_affinity VALUES"
                "(7,'a7','person:d','person:a','like','active','t','published');");
    check(bf_affinity_mutual(db, "person:a", "person:d") == 1, "superlike counts toward mutual");
    execute(db, "INSERT INTO person_affinity VALUES"
                "(8,'a8','person:a','person:b','pass','active','t','published');");
    check(bf_affinity_status(db, "person:a", "person:b", kind, sizeof(kind)) == 1 &&
          !strcmp(kind, "pass"), "latest edge wins");
    check(bf_affinity_mutual(db, "person:a", "person:b") == 0, "revoked like ends mutual");
    execute(db, "INSERT INTO person_affinity VALUES"
                "(9,'a9','person:a','person:g','adore','active','t','published');");
    check(bf_affinity_status(db, "person:a", "person:g", kind, sizeof(kind)) == 0,
          "unknown kind reads as none");
    execute(db, "CREATE TABLE site_presentation (site_id TEXT, host TEXT,"
                "subject_address TEXT, territory TEXT, namespace TEXT,"
                "profile TEXT, status TEXT);"
                "CREATE TABLE site_page (namespace TEXT, route TEXT, heading TEXT,"
                "lede TEXT, status TEXT);"
                "INSERT INTO site_presentation VALUES"
                "('s-a','a.aurekai.work','person:a','work','root','member-profile-v1','published'),"
                "('s-b','b.aurekai.life','person:b','life','root','member-profile-v1','published'),"
                "('s-c','c.aurekai.work','person:c','work','root','member-profile-v1','published'),"
                "('s-d','d.aurekai.work','person:d','work','root','member-profile-v1','published'),"
                "('s-e','e.aurekai.work','person:e','work','root','member-profile-v1','published'),"
                "('s-x','x.example.com','org:x','work','root','member-profile-v1','published');"
                "INSERT INTO site_page VALUES"
                "('s-a','/','Ann <b>','Bio & more','published'),"
                "('s-b','/','Bea','Second bio','published');");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open companions memory");
        /* viewer a passed b and c; d is a superlike mutual, e is fresh. */
        check(bf_companions_page(mem, db, "person:a", "", "", 0, 24, NULL, NULL, NULL) == 2, "two companions");
        check(fclose(mem) == 0, "close companions memory");
        check(strstr(html, "person:d") != NULL, "mutual candidate listed");
        check(strstr(html, "person:e") != NULL, "fresh candidate listed");
        check(strstr(html, "person:a") == NULL, "self excluded");
        check(strstr(html, "person:b") == NULL, "passed b excluded");
        check(strstr(html, "person:c") == NULL, "passed c excluded");
        check(strstr(html, "org:x") == NULL, "non-person excluded");
        check(strstr(html, ">Mutual</span>") != NULL, "mutual badge shown");
        check(strstr(html, "<form") != NULL, "viewer gets forms");
        check(strstr(html, "ak-pattern ak-card") != NULL, "rows speak component language");
        free(html);
    }
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open anonymous memory");
        check(bf_companions_page(mem, db, NULL, "", "", 0, 24, NULL, NULL, NULL) == 5, "anonymous sees all");
        check(fclose(mem) == 0, "close anonymous memory");
        check(strstr(html, "Ann &lt;b&gt;") != NULL, "name escaped");
        check(strstr(html, "Bio &amp; more") != NULL, "bio escaped");
        check(strstr(html, "<form") == NULL, "anonymous gets no forms");
        check(strstr(html, ">Mutual</span>") == NULL, "anonymous gets no mutuals");
        free(html);
    }
    {
        char *js = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&js, &len);
        check(mem != NULL, "open json memory");
        check(bf_companions_json(mem, db, "person:b", "", "", 0, 24, NULL, NULL, NULL) == 4, "json rows");
        check(fclose(mem) == 0, "close json memory");
        check(strstr(js, "\"subject\":\"person:a\"") != NULL, "json subject");
        check(strstr(js, "\"affinity\":\"like\"") != NULL, "json affinity");
        check(strstr(js, "\"mutual\":false") != NULL, "json mutual false");
        check(strstr(js, "\"bio\":\"Bio & more\"") != NULL, "json bio carried");
        free(js);
    }
    check(bf_companions_page(NULL, db, "person:a", "", "", 0, 24, NULL, NULL, NULL) == -1, "null out refused");
    check(bf_companions_page(stdout, db, "nope", "", "", 0, 24, NULL, NULL, NULL) == -1, "bad viewer refused");
    check(bf_companions_page(stdout, db, "", "mars", "", 0, 24, NULL, NULL, NULL) == -1, "bad territory refused");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(bf_companions_page(mem, db, "", "life", "", 0, 24, NULL, NULL, NULL) == 1, "territory filter");
        check(fclose(mem) == 0, "close territory memory");
        check(strstr(html, "b.aurekai.life") != NULL && strstr(html, "a.aurekai.work") == NULL,
              "territory narrows");
        free(html);
    }
    execute(db, "CREATE TABLE person_interest (interest TEXT, actor_subject TEXT,"
                "label TEXT, weight TEXT, status TEXT);"
                "INSERT INTO person_interest VALUES"
                "('i1','person:b','hiking','2','published'),"
                "('i2','person:b','cycling','1','published'),"
                "('i3','person:a','hiking','1','published'),"
                "('i4','person:a','cooking','1','published'),"
                "('i5','person:c','cycling','1','published'),"
                "('i6','person:c','hiking','1','published'),"
                "('i7','person:e','hiking','1','draft');"
                "CREATE TABLE community_post (author_subject TEXT, status TEXT,"
                "visibility TEXT, moderation_state TEXT, published_at TEXT);"
                "INSERT INTO community_post VALUES"
                "('person:c','published','public','accepted',date('now')),"
                "('person:d','published','public','accepted','2020-01-01');");
    {
        int score = -1;
        char shared[341];
        check(bf_match_score(db, "person:b", "person:c", &score, shared, sizeof(shared)) == 0 &&
              score == 5 && !strcmp(shared, "cycling,hiking"), "overlap plus recency");
        check(bf_match_score(db, "person:b", "person:a", &score, shared, sizeof(shared)) == 0 &&
              score == 2 && !strcmp(shared, "hiking"), "single overlap");
        check(bf_match_score(db, "person:b", "person:d", &score, shared, sizeof(shared)) == 0 &&
              score == 0, "stale post adds nothing");
        check(bf_match_score(db, "person:b", "person:e", &score, shared, sizeof(shared)) == 0 &&
              score == 0, "draft interest ignored");
        check(bf_match_score(db, "person:b", "person:b", &score, shared, sizeof(shared)) == 0 &&
              score == 0, "self scores zero");
        check(bf_match_score(db, "nope", "person:a", &score, shared, sizeof(shared)) == 0 &&
              score == 0, "bad viewer scores zero");
        check(bf_match_score(NULL, "person:b", "person:a", &score, shared, sizeof(shared)) == -1,
              "null db refused");
        check(bf_match_score(db, "person:b", "person:a", NULL, shared, sizeof(shared)) == -1,
              "null score refused");
    }
    execute(db, "CREATE TABLE person_quiz_answer (id INTEGER PRIMARY KEY,"
                "answer TEXT, actor_subject TEXT, question TEXT, choice TEXT,"
                "weight TEXT, status TEXT);"
                "INSERT INTO person_quiz_answer VALUES"
                "(1,'qb1','person:b','q-weekend','a','2','published'),"
                "(2,'qc1','person:c','q-weekend','a','3','published'),"
                "(3,'qb2','person:b','q-morning','b','3','published'),"
                "(4,'qc2','person:c','q-morning','c','1','published'),"
                "(5,'qb3','person:b','q-plans','a','3','published'),"
                "(6,'qc3','person:c','q-plans','a','3','published'),"
                "(7,'qb4','person:b','q-social','a','3','published'),"
                "(8,'qc4','person:c','q-social','a','3','published'),"
                "(9,'qb5','person:b','q-food','a','1','published'),"
                "(10,'qb6','person:b','q-food','b','2','published'),"
                "(11,'qd1','person:d','q-food','b','3','published'),"
                "(12,'qb7','person:b','q-travel','z','2','published'),"
                "(13,'qd2','person:d','q-travel','a','1','published'),"
                "(14,'qb8','person:b','q-conflict','a','7','published'),"
                "(15,'qd3','person:d','q-conflict','a','1','published'),"
                "(16,'qb9','person:b','q-nope','a','1','published'),"
                "(17,'qd4','person:d','q-nope','a','1','published');");
    {
        int score = -1;
        char shared[341];
        check(bf_match_score(db, "person:b", "person:c", &score, shared, sizeof(shared)) == 0 &&
              score == 11 && !strcmp(shared, "cycling,hiking,quiz+6"),
              "quiz agreement capped with token");
        check(bf_match_score(db, "person:b", "person:d", &score, shared, sizeof(shared)) == 0 &&
              score == 2 && !strcmp(shared, "quiz+2"),
              "latest answer wins, invalid ignored");
        check(bf_match_score(db, "person:b", "person:a", &score, shared, sizeof(shared)) == 0 &&
              score == 2 && !strcmp(shared, "hiking"),
              "no co-answered quiz adds nothing");
        check(bf_quiz_bank_n == 8 && bf_quiz_bank[0].choices == 4 &&
              !strcmp(bf_quiz_bank[0].key, "q-weekend"),
              "bank carries eight original questions");
    }
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        const char *pc, *pa;
        check(mem != NULL, "open ranked memory");
        check(bf_companions_page(mem, db, "person:b", "", "", 0, 24, "match", NULL, NULL) == 4, "ranked rows");
        check(fclose(mem) == 0, "close ranked memory");
        pc = strstr(html, "person:c");
        pa = strstr(html, "person:a");
        check(pc && pa && pc < pa, "higher score first");
        check(strstr(html, ">Match 11 ") != NULL, "score badge shown");
        check(strstr(html, "cycling,hiking") != NULL, "shared labels shown");
        free(html);
    }
    {
        char *js = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&js, &len);
        check(mem != NULL, "open ranked json memory");
        check(bf_companions_json(mem, db, "person:b", "", "", 0, 2, "match", NULL, NULL) == 2, "ranked page");
        check(fclose(mem) == 0, "close ranked json memory");
        check(strstr(js, "\"subject\":\"person:c\"") != NULL, "json leader");
        check(strstr(js, "\"score\":11,\"shared\":[\"cycling\",\"hiking\",\"quiz+6\"]") != NULL, "json score shape");
        {
            const char *pc = strstr(js, "person:c"), *pa = strstr(js, "person:a");
            check(pc && pa && pc < pa, "json order ranked");
        }
        free(js);
    }
    check(bf_companions_page(stdout, db, "person:b", "", "", 0, 24, "nope", NULL, NULL) == -1,
          "bad sort refused");
    check(bf_companions_page(stdout, db, "person:b", "", "", 0, 24, NULL, "nope", NULL) == -1,
          "bad view refused");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open needle memory");
        check(bf_companions_page(mem, db, NULL, "", "cyc", 0, 24, NULL, NULL, NULL) == 2, "needle rows");
        check(fclose(mem) == 0, "close needle memory");
        check(strstr(html, "b.aurekai.life") != NULL && strstr(html, "c.aurekai.work") != NULL,
              "needle matches interest labels");
        check(strstr(html, "a.aurekai.work") == NULL, "needle excludes nonmatching");
        free(html);
    }
    execute(db, "INSERT INTO site_presentation VALUES"
                "('s-f','f.aurekai.work','person:f','work','root','member-profile-v1','published');"
                "INSERT INTO person_affinity VALUES"
                "(10,'a10','person:b','person:f','like','active','t','published'),"
                "(11,'a11','person:f','person:b','block','active','t','published'),"
                "(12,'a12','person:c','person:b','like','active','2026-11-09T10:00:00Z','published'),"
                "(13,'a13','person:d','person:b','superlike','active','2026-11-10T10:00:00Z','published'),"
                "(14,'a14','person:e','person:b','like','active','t','published'),"
                "(15,'a15','person:e','person:b','pass','active','t','published');"
                "INSERT INTO person_interest VALUES('i8','person:f','hiking','1','published');"
                "CREATE TABLE person_report (report TEXT, actor_subject TEXT,"
                "target_subject TEXT, reason TEXT, status TEXT, created_at TEXT);"
                "INSERT INTO person_report VALUES"
                "('r1','person:a','person:e','spam','published',datetime('now')),"
                "('r2','person:b','person:x','spam','published',datetime('now','-2 days'));");
    check(bf_affinity_status(db, "person:f", "person:b", kind, sizeof(kind)) == 1 &&
          !strcmp(kind, "block"), "block reads");
    check(bf_affinity_mutual(db, "person:b", "person:f") == 0, "block breaks mutual");
    check(bf_affinity_blocked(db, "person:b", "person:f") == 1, "blocked either way");
    check(bf_affinity_blocked(db, "person:f", "person:b") == 1, "blocked symmetric");
    check(bf_affinity_blocked(db, "person:a", "person:b") == 0, "unblocked pair");
    {
        int score = -1;
        char shared[341];
        check(bf_match_score(db, "person:b", "person:f", &score, shared, sizeof(shared)) == 0 &&
              score == 0, "blocked pair scores zero");
    }
    check(bf_report_quota_ok(db, "person:a") == 1, "quota open");
    check(bf_report_quota_ok(db, "nope") == -1, "quota misuse refused");
    execute(db, "INSERT INTO person_report VALUES"
                "('r3','person:c','person:a','spam','published',datetime('now')),"
                "('r4','person:c','person:b','spam','published',datetime('now')),"
                "('r5','person:c','person:d','spam','published',datetime('now')),"
                "('r6','person:c','person:e','spam','published',datetime('now')),"
                "('r7','person:c','person:f','spam','published',datetime('now'));");
    check(bf_report_quota_ok(db, "person:c") == 0, "quota exhausted");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open safety memory");
        check(bf_companions_page(mem, db, "person:a", "", "", 0, 24, NULL, NULL, NULL) == 2,
              "reported target excluded");
        check(fclose(mem) == 0, "close safety memory");
        check(strstr(html, "person:e") == NULL, "reporter hides target");
        check(strstr(html, "person:d") != NULL, "others stay");
        free(html);
    }
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open blockview memory");
        check(bf_companions_page(mem, db, "person:b", "", "", 0, 24, NULL, NULL, NULL) == 4,
              "blocked target excluded");
        check(fclose(mem) == 0, "close blockview memory");
        check(strstr(html, "person:f") == NULL, "block hides both ways");
        free(html);
    }
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        const char *pd, *pc;
        check(mem != NULL, "open attention memory");
        check(bf_attention_page(mem, db, "person:b", 24) == 2, "attention rows");
        check(fclose(mem) == 0, "close attention memory");
        pd = strstr(html, "d.aurekai.work");
        pc = strstr(html, "c.aurekai.work");
        check(pd && pc && pd < pc, "attention newest first");
        check(strstr(html, "superlike") != NULL, "attention kind shown");
        check(strstr(html, "2026-11-10") != NULL, "attention day shown");
        check(strstr(html, "ak-pattern ak-card") != NULL, "attention speaks components");
        check(strstr(html, "person:e") == NULL, "revoked attention gone");
        check(strstr(html, "person:f") == NULL, "blocked attention gone");
        free(html);
    }
    {
        char *js = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&js, &len);
        check(mem != NULL, "open attention json memory");
        check(bf_attention_json(mem, db, "person:b", 24) == 2, "attention json rows");
        check(fclose(mem) == 0, "close attention json memory");
        check(strstr(js, "\"subject\":\"person:d\",\"kind\":\"superlike\"") != NULL,
              "attention json shape");
        free(js);
    }
    check(bf_attention_page(NULL, db, "person:b", 24) == -1, "attention null refused");
    check(bf_attention_page(stdout, db, "nope", 24) == -1, "attention viewer refused");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open matches memory");
        check(bf_companions_page(mem, db, "person:a", "", "", 0, 24, NULL, "matches", NULL) == 1,
              "matches view");
        check(fclose(mem) == 0, "close matches memory");
        check(strstr(html, "person:d") != NULL, "mutual listed");
        check(strstr(html, "person:e") == NULL, "non-mutual cut");
        free(html);
    }
    check(bf_companions_page(stdout, db, "person:b", "", "", 0, 24, NULL, "matches", NULL) == 0,
          "no mutuals, empty view");
    check(bf_companions_page(stdout, db, NULL, "", "", 0, 24, NULL, "matches", NULL) == -1,
          "anonymous matches refused");
    execute(db, "CREATE TABLE community_membership (membership TEXT, community TEXT,"
                "member_subject TEXT, state TEXT, status TEXT, namespace TEXT);"
                "INSERT INTO community_membership VALUES"
                "('m1','hiking-club','person:b','active','published','root'),"
                "('m2','hiking-club','person:c','active','published','root'),"
                "('m3','hiking-club','person:d','withdrawn','published','root');");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open club memory");
        check(bf_companions_page(mem, db, NULL, "", "", 0, 24, NULL, NULL, "hiking-club") == 2,
              "club filter");
        check(fclose(mem) == 0, "close club memory");
        check(strstr(html, "b.aurekai.life") != NULL && strstr(html, "c.aurekai.work") != NULL,
              "club members listed");
        check(strstr(html, "d.aurekai.work") == NULL, "withdrawn member cut");
        free(html);
    }
    check(bf_companions_page(stdout, db, NULL, "", "", 0, 24, NULL, NULL, "BAD!") == -1,
          "bad club refused");
    {
        char *html = NULL;
        size_t len = 0;
        FILE *mem = open_memstream(&html, &len);
        check(mem != NULL, "open view attention memory");
        check(bf_companions_page(mem, db, "person:b", "", "", 0, 24, NULL, "attention", NULL) == 2,
              "attention view dispatches");
        check(fclose(mem) == 0, "close view attention memory");
        free(html);
    }
    {
        sqlite3 *old = NULL;
        char *html = NULL;
        size_t len = 0;
        FILE *mem;
        check(sqlite3_open(":memory:", &old) == SQLITE_OK, "old db open");
        execute(old, "CREATE TABLE site_presentation (site_id TEXT, host TEXT,"
                     "subject_address TEXT, territory TEXT, namespace TEXT,"
                     "profile TEXT, status TEXT);"
                     "CREATE TABLE site_page (namespace TEXT, route TEXT, heading TEXT,"
                     "lede TEXT, status TEXT);"
                     "INSERT INTO site_presentation VALUES"
                     "('s-a','a.aurekai.work','person:a','work','root','member-profile-v1','published');");
        mem = open_memstream(&html, &len);
        check(mem != NULL, "open legacy memory");
        check(bf_companions_page(mem, old, "person:a", "", "", 0, 24, NULL, NULL, NULL) == 0,
              "unmigrated store lists without exclusion");
        check(fclose(mem) == 0, "close legacy memory");
        check(strstr(html, "data-empty") != NULL, "legacy page completes");
        free(html);
        sqlite3_close(old);
    }
    sqlite3_close(db);
    puts("affinity projection: PASS");
    return 0;
}
