#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "../src/club_projection.h"

static void check(int ok, const char *message) {
    if (!ok) { fprintf(stderr, "club projection FAIL: %s\n", message); exit(1); }
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
    char *html = NULL;
    size_t len = 0;
    FILE *mem;
    int left = -9;
    check(sqlite3_open(":memory:", &db) == SQLITE_OK, "db open");
    check(bf_gold_active(db, "person:a") == 0, "missing tables not gold");
    check(bf_gold_active(NULL, "person:a") == -1, "gold null refused");
    check(bf_gold_active(db, "nope") == -1, "gold viewer refused");
    check(bf_quota_superlike(db, "person:a", &left) == 0 && left == 1, "superlike default");
    check(bf_quota_plans(db, "person:a", &left) == 0 && left == 3, "plans default");
    check(bf_quota_superlike(db, "person:a", NULL) == -1, "quota null refused");
    execute(db, "CREATE TABLE community_membership (membership TEXT, community TEXT,"
                "member_subject TEXT, role TEXT, state TEXT, joined_at TEXT,"
                "expires_at TEXT, status TEXT, namespace TEXT);"
                "INSERT INTO community_membership VALUES"
                "('m1','gold','person:a','gold','active',NULL,datetime('now','+30 days'),'published','root'),"
                "('m2','gold','person:b','gold','active',NULL,datetime('now','-1 day'),'published','root'),"
                "('m3','gold','person:c','gold','active',NULL,NULL,'published','root'),"
                "('m4','hiking-club','person:a','member','active',NULL,NULL,'published','root');"
                "CREATE TABLE community_body (community TEXT, title TEXT, interest_label TEXT,"
                "namespace TEXT, status TEXT);"
                "INSERT INTO community_body VALUES"
                "('hiking-club','Hiking Club','hiking','root','published'),"
                "('book-club','Book Club','reading','root','published'),"
                "('plain','Plain Group',NULL,'root','published');"
                "CREATE TABLE person_affinity (id INTEGER PRIMARY KEY, actor_subject TEXT,"
                "kind TEXT, state TEXT, status TEXT, created_at TEXT);"
                "INSERT INTO person_affinity VALUES"
                "(1,'person:a','superlike','active','published',datetime('now'));"
                "CREATE TABLE date_plan (id INTEGER PRIMARY KEY, actor_subject TEXT,"
                "status TEXT, created_at TEXT);"
                "INSERT INTO date_plan VALUES"
                "(1,'person:a','published',datetime('now')),"
                "(2,'person:a','published',datetime('now','-1 day')),"
                "(3,'person:a','published',datetime('now','-8 days'));"
                "CREATE TABLE community_post (post TEXT, title TEXT, event_at TEXT, venue TEXT,"
                "community TEXT, visibility TEXT, moderation_state TEXT, status TEXT);"
                "INSERT INTO community_post VALUES"
                "('e1','Trail morning',datetime('now','+2 days'),'Trailhead','hiking-club',"
                "'public','accepted','published'),"
                "('e2','Old meetup',datetime('now','-2 days'),'Hall','hiking-club',"
                "'public','accepted','published'),"
                "('p1','Hello',NULL,NULL,'hiking-club','public','accepted','published');"
                "CREATE TABLE community_reaction (id INTEGER PRIMARY KEY, post TEXT,"
                "actor_subject TEXT, kind TEXT, state TEXT, status TEXT);"
                "INSERT INTO community_reaction VALUES"
                "(1,'e1','person:b','rsvp','active','published'),"
                "(2,'e1','person:c','rsvp','active','published'),"
                "(3,'e1','person:d','rsvp','withdrawn','published');");
    check(bf_gold_active(db, "person:a") == 1, "gold current");
    check(bf_gold_active(db, "person:b") == 0, "gold expired");
    check(bf_gold_active(db, "person:c") == 0, "gold dateless fails closed");
    check(bf_quota_superlike(db, "person:a", &left) == 0 && left == -1, "gold unlimited likes");
    check(bf_quota_superlike(db, "person:b", &left) == 0 && left == 1, "free like left");
    check(bf_quota_plans(db, "person:a", &left) == 0 && left == -1, "gold unlimited plans");
    check(bf_quota_plans(db, "person:b", &left) == 0 && left == 3, "free plans left");
    execute(db, "INSERT INTO date_plan VALUES(4,'person:b','published',datetime('now')),"
                "(5,'person:b','published',datetime('now')),(6,'person:b','published',datetime('now'));");
    check(bf_quota_plans(db, "person:b", &left) == 0 && left == 0, "plans exhausted");
    mem = open_memstream(&html, &len);
    check(mem != NULL, "open clubs memory");
    check(bf_clubs_page(mem, db, "person:a") == 2, "two clubs");
    check(fclose(mem) == 0, "close clubs memory");
    check(strstr(html, "Hiking Club") != NULL && strstr(html, "1 members") != NULL,
          "club with count");
    check(strstr(html, "Plain Group") == NULL, "unflavored body excluded");
    check(strstr(html, ">Joined</span>") != NULL, "membership shown");
    check(strstr(html, "ak-pattern ak-card") != NULL, "clubs speak components");
    free(html);
    html = NULL;
    mem = open_memstream(&html, &len);
    check(mem != NULL, "open events memory");
    check(bf_events_page(mem, db, "person:a", NULL) == 1, "one upcoming event");
    check(fclose(mem) == 0, "close events memory");
    check(strstr(html, "Trail morning") != NULL, "event shown");
    check(strstr(html, "2 going") != NULL, "rsvp count excludes withdrawn");
    check(strstr(html, "Old meetup") == NULL, "past event cut");
    check(strstr(html, "action=\"/companions/events/rsvp\"") != NULL, "rsvp form shown");
    check(strstr(html, "ak-pattern ak-card") != NULL, "events speak components");
    free(html);
    html = NULL;
    {
        FILE *m2 = open_memstream(&html, &len);
        check(m2 != NULL, "open club events memory");
        check(bf_events_page(m2, db, NULL, "book-club") == 0, "empty club events");
        check(fclose(m2) == 0, "close club events memory");
        check(strstr(html, "No upcoming events.") != NULL, "empty events shell");
        free(html);
    }
    {
        sqlite3 *old = NULL;
        char *h2 = NULL;
        size_t l2 = 0;
        FILE *m3;
        check(sqlite3_open(":memory:", &old) == SQLITE_OK, "old db open");
        m3 = open_memstream(&h2, &l2);
        check(m3 != NULL, "open legacy memory");
        check(bf_clubs_page(m3, old, NULL) == 0, "legacy clubs empty");
        check(bf_events_page(m3, old, NULL, NULL) == 0, "legacy events empty");
        check(fclose(m3) == 0, "close legacy memory");
        check(h2 == NULL || h2[0] == '\0', "legacy stores render silent");
        free(h2);
        sqlite3_close(old);
    }
    sqlite3_close(db);
    puts("club projection: PASS");
    return 0;
}
