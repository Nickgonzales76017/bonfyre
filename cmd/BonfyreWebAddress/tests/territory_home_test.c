#include "../src/territory_home.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *TEMPLATE =
    "<body>"
    "<b id=\"territory\">PUBLIC</b>"
    "<span id=\"eyebrow\">generic</span>"
    "<h1 id=\"title\">generic</h1>"
    "<p id=\"sub\">generic</p>"
    "<span id=\"orbit-label\">generic</span>"
    "<h3 id=\"look-title\">generic</h3>"
    "<p id=\"look-copy\">generic</p>"
    "<h3 id=\"exchange-title\">generic</h3>"
    "<p id=\"exchange-copy\">generic</p>"
    "<h3 id=\"work-title\">generic</h3>"
    "<p id=\"work-copy\">generic</p>"
    "</body>";

static char *owned(const char *s) {
    size_t n = strlen(s) + 1;
    char *out = malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}

static int check(sqlite3 *db, const char *territory, const char *body_class, const char *title) {
    char *body = owned(TEMPLATE);
    int ok;
    if (!body) return 0;
    ok = bf_apply_territory_home_presentation(db, &body, territory) &&
         strstr(body, body_class) != NULL &&
         strstr(body, title) != NULL &&
         strstr(body, "generic</h1>") == NULL;
    if (!ok) fprintf(stderr, "territory home: failed %s\n", territory);
    free(body);
    return ok;
}

int main(void) {
    sqlite3 *db = NULL;
    char *neutral = owned(TEMPLATE);
    char *incomplete = owned("<body><h1 id=\"title\">unchanged</h1></body>");
    char *incomplete_original = incomplete;
    char *unsafe = owned(TEMPLATE);
    char *unsafe_original = unsafe;
    if (!neutral || !incomplete || !unsafe || sqlite3_open(":memory:", &db) != SQLITE_OK ||
        sqlite3_exec(db, "CREATE TABLE territory_presentation (id INTEGER PRIMARY KEY,territory TEXT,eyebrow TEXT,head TEXT,lede TEXT,question TEXT,character TEXT,cannot_infer TEXT,status TEXT)", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_exec(db,
          "INSERT INTO territory_presentation(territory,eyebrow,head,lede,question,character,cannot_infer,status) VALUES "
          "('life','aurekai.life · your own world','The things you <em>kept.</em>','A personal world is not a feed.','What matters in my world?','shelf','A kept thing is not an endorsed thing.','published'),"
          "('live','aurekai.live · what is on','Tonight is <em>already happening.</em>','Rooms and occurrences, ranked by what is on now.','What deserves attention now?','marquee','A room listed here is not confirmed.','published'),"
          "('shop','aurekai.shop · what is offered','An offer is not a <em>promise.</em>','Fifteen things happen between a product and owning it.','What economic opportunity is useful?','counter','A price shown is a price quoted.','published'),"
          "('work','aurekai.work · what moves','What is <em>blocking</em> what.','Obligations and requirements, ordered by what stops something else.','What should move forward?','desk','An ordered queue is not an instruction.','published')", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_exec(db,
          "INSERT INTO territory_presentation(territory,eyebrow,head,lede,question,character,cannot_infer,status) VALUES "
          "('bad\" onload=\"x','eyebrow','head','lede','question','character','boundary','published')", NULL, NULL, NULL) != SQLITE_OK ||
        !check(db, "life", "<body class=\"life\">", "The things you <em>kept.</em>") ||
        !check(db, "live", "<body class=\"live\">", "Tonight is <em>already happening.</em>") ||
        !check(db, "shop", "<body class=\"shop\">", "An offer is not a <em>promise.</em>") ||
        !check(db, "work", "<body class=\"work\">", "What is <em>blocking</em> what.") ||
        bf_apply_territory_home_presentation(db, &incomplete, "life") ||
        incomplete != incomplete_original ||
        strcmp(incomplete, "<body><h1 id=\"title\">unchanged</h1></body>") != 0 ||
        bf_apply_territory_home_presentation(db, &unsafe, "bad\" onload=\"x") ||
        unsafe != unsafe_original ||
        strcmp(unsafe, TEMPLATE) != 0 ||
        !bf_apply_territory_home_presentation(db, &neutral, "independent") ||
        strstr(neutral, "<body class=\"independent\">") == NULL) {
        free(neutral);
        free(incomplete);
        free(unsafe);
        if (db) sqlite3_close(db);
        return 1;
    }
    free(neutral);
    free(incomplete);
    free(unsafe);
    sqlite3_close(db);
    {
        /* Current schema: a territory record that names a published template wears it. */
        sqlite3 *tdb = NULL;
        char *dressed = owned(TEMPLATE);
        int ok = dressed && sqlite3_open(":memory:", &tdb) == SQLITE_OK &&
          sqlite3_exec(tdb,
            "CREATE TABLE territory_presentation (id INTEGER PRIMARY KEY,territory TEXT,eyebrow TEXT,head TEXT,lede TEXT,question TEXT,character TEXT,cannot_infer TEXT,template TEXT,status TEXT);"
            "CREATE TABLE presentation_template (id INTEGER PRIMARY KEY,template TEXT,status TEXT,layout_tokens TEXT,hero_mode TEXT,navigation_mode TEXT,density TEXT,card_mode TEXT,typography_mode TEXT);"
            "INSERT INTO territory_presentation(territory,eyebrow,head,lede,question,character,cannot_infer,template,status) VALUES"
            "('work','e','What moves','l','q','desk','b','night-ledger','published');"
            "INSERT INTO presentation_template(template,status,layout_tokens,hero_mode,navigation_mode,density,card_mode,typography_mode) VALUES"
            "('night-ledger','published','{\"ground\":\"#0f1511\",\"accent\":\"#d0fc4f\"}','lead','header','standard','soft-panel','mixed');",
            NULL, NULL, NULL) == SQLITE_OK &&
          bf_apply_territory_home_presentation(tdb, &dressed, "work") &&
          strstr(dressed, "<body data-bf-template=\"night-ledger\" class=\"work\">") &&
          strstr(dressed, "body[data-bf-template]{--bf-ground:#0f1511;--bf-accent:#d0fc4f;}");
        free(dressed);
        if (tdb) sqlite3_close(tdb);
        if (!ok) { fputs("territory home: failed template\n", stderr); return 1; }
    }
    puts("territory home: PASS");
    return 0;
}
