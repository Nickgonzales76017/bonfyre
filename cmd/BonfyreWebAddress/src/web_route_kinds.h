#ifndef BONFYRE_WEB_ROUTE_KINDS_H
#define BONFYRE_WEB_ROUTE_KINDS_H
/* The route kinds past a fixed list (schemas/control/web-routes.cue).
 *
 * records   a row whose paths are rows of a declared store, read at run time.
 *           The dispatcher asks the store, read-only through the store's
 *           declared durability and request lease, whether the path is a
 *           record; only then is the owner asked, in the row's place. The
 *           owner still decides everything it decided before.
 * semantic  the not-found tail. When the resolver finds nothing at a path,
 *           the page offers up to five declared pages and records whose
 *           names are nearest the missing path: the distilled static
 *           embedding (lib/libbonfyre bf_static_embed), which finds names
 *           that mean the same, or a character-trigram overlap, which finds
 *           a misspelling, whichever is higher. A suggestion is a link. It is never
 *           dispatched, and it does not say the caller may open it.
 *
 * cannot infer: a suggestion -> the route exists for this caller or the
 * caller may open it; a high score -> the same meaning; a record match ->
 * the owner will answer 200. */
#include <math.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_static_embed.h"
#include "web_request.h"
#include "ui_tree.h"
#include "app_shell.h"

static sqlite3 *cms_open_ro(void);

/* Each store a records row names opens here. The route compiler refuses a
 * store that schemas/storage/stores does not declare as opened by this
 * program; a declared store with no opener here does not build. */
typedef sqlite3 *(*BfRouteStoreOpen)(void);
static inline sqlite3 *bf_route_store_open_cms(void) { return cms_open_ro(); }

/* 1 when the declared read-only query finds the path. A statement that would
 * write is refused, whatever the declaration says. */
static inline int bf_route_records_match(BfRouteStoreOpen open, const char *sql, const char *target) {
    sqlite3 *db = open ? open() : NULL;
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!db) return 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_stmt_readonly(st)) {
        sqlite3_bind_text(st, 1, target, -1, SQLITE_STATIC);
        found = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return found;
}

typedef struct { const char *name; BfRouteStoreOpen open; const char *sql; } BfRouteSuggestSet;
typedef struct { const char *ranker; int limit, min_score; } BfRouteSemantic;

/* The request facts of the path nobody answered, set by the resolver's
 * owner. A declared page is offered only when its row's facts would hold for
 * a browser that followed the link. */
static unsigned bf_route_tail_facts;
/* Defined by generated/native_web_routes.h over the compiled table; the
 * resolver calls it when an HTML address resolves to nothing. */
static int bf_web_route_not_found(const char *path);

/* Lower-case words of a path or a title: letters and digits, the rest one space. */
static inline void bf_route_words(const char *in, char *out, size_t cap) {
    size_t w = 0;
    int space = 1;
    for (const unsigned char *p = (const unsigned char *)(in ? in : ""); *p && w + 1 < cap; p++) {
        int c = *p;
        if (c >= 'A' && c <= 'Z') c += 32;
        if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c >= 0x80) { out[w++] = (char)c; space = 0; }
        else if (!space) { out[w++] = ' '; space = 1; }
    }
    while (w && out[w - 1] == ' ') w--;
    out[w] = '\0';
}

/* Dice overlap of the padded character trigrams of two word strings. */
static inline float bf_route_trigram(const char *a, const char *b) {
    char pa[260], pb[260];
    unsigned char used[258] = {0};
    int na, nb, hit = 0;
    snprintf(pa, sizeof pa, " %.256s ", a);
    snprintf(pb, sizeof pb, " %.256s ", b);
    na = (int)strlen(pa) - 2;
    nb = (int)strlen(pb) - 2;
    if (na < 1 || nb < 1 || !a[0] || !b[0]) return 0.0f;
    for (int i = 0; i < na; i++)
        for (int j = 0; j < nb; j++)
            if (!used[j] && !memcmp(pa + i, pb + j, 3)) { used[j] = 1; hit++; break; }
    return 2.0f * (float)hit / (float)(na + nb);
}

/* The distilled model, loaded once per process; NULL when absent. */
static inline const BfStaticModel *bf_route_static_model(void) {
    static BfStaticModel *model;
    static int tried;
    if (!tried) {
        char path[1024], err[160];
        const char *given = getenv("BONFYRE_STATIC_MODEL"), *home = getenv("HOME");
        tried = 1;
        if (given && *given) snprintf(path, sizeof path, "%s", given);
        else snprintf(path, sizeof path, "%s/BonfyreModels/static/nomic-static-128.bfse", home ? home : "");
        model = bf_static_load(path, err, sizeof err);
    }
    return model;
}

typedef struct { char href[512]; char title[160]; float score; } BfRouteSuggestion;

static inline int bf_route_safe_href(const char *p) {
    if (!p || p[0] != '/' || strlen(p) >= 512) return 0;
    for (const unsigned char *c = (const unsigned char *)p; *c; c++)
        if (*c <= ' ' || *c == '"' || *c == '<' || *c == '>' || *c == '\\' || *c == '?' || *c == '#') return 0;
    return !strstr(p, "/_") && !strstr(p, "//") && !strstr(p, "..");
}

/* Scores one candidate and keeps it if it ranks in the top `limit`. */
static inline void bf_route_consider(BfRouteSuggestion *top, int *n, int limit, const char *missing,
                                     const char *query_words, const float *query_vec, int dims,
                                     const char *href, const char *title) {
    char path_words[512], title_words[256], joined[800];
    float lex, score;
    if (!bf_route_safe_href(href) || !title || !*title || !strcmp(href, missing)) return;
    for (int i = 0; i < *n; i++) if (!strcmp(top[i].href, href)) return;
    bf_route_words(href, path_words, sizeof path_words);
    bf_route_words(title, title_words, sizeof title_words);
    lex = bf_route_trigram(query_words, path_words);
    { float t = bf_route_trigram(query_words, title_words); if (t > lex) lex = t; }
    score = lex;
    if (query_vec && dims > 0 && dims <= 1024) {
        float v[1024], dot = 0.0f;
        snprintf(joined, sizeof joined, "%s %s", title_words, path_words);
        if (bf_static_embed(bf_route_static_model(), joined, v) > 0) {
            for (int i = 0; i < dims; i++) dot += query_vec[i] * v[i];
            if (dot > score) score = dot;
        }
    }
    if (*n == limit && score <= top[limit - 1].score) return;
    int at = *n < limit ? (*n)++ : limit - 1;
    while (at > 0 && top[at - 1].score < score) { top[at] = top[at - 1]; at--; }
    snprintf(top[at].href, sizeof top[at].href, "%s", href);
    snprintf(top[at].title, sizeof top[at].title, "%s", title);
    top[at].score = score;
}

/* The not-found page: 404, the one shell, and up to `limit` declared pages
 * and records nearest the missing path. Returns 1 when it wrote the reply. */
static inline int bf_route_not_found_page(const BfRoute *routes, size_t count, const BfRouteSuggestSet *sets,
                                          size_t nsets, const BfRouteSemantic *semantic, const char *missing) {
    BfRouteSuggestion top[5];
    UiRow rows[5];
    char query_words[512];
    float query_vec[1024];
    const float *qv = NULL;
    int n = 0, dims = 0;
    int limit = semantic && semantic->limit > 0 && semantic->limit <= 5 ? semantic->limit : 5;
    float floor = semantic ? (float)semantic->min_score / 100.0f : 0.3f;
    /* A browser following a suggestion asks with GET for HTML, on this host
     * and this server. */
    unsigned follow = (bf_route_tail_facts & (BF_WHEN_SITE_ROOT | BF_WHEN_NOT_TERRITORY_APEX)) |
                      BF_WHEN_GET | BF_WHEN_HTML | BF_WHEN_NOT_JSON;
    if (!missing) return 0;
    bf_route_words(missing, query_words, sizeof query_words);
    if (query_words[0] && bf_route_static_model()) {
        dims = bf_static_dims(bf_route_static_model());
        if (dims > 0 && dims <= 1024 && bf_static_embed(bf_route_static_model(), query_words, query_vec) > 0) qv = query_vec;
    }
    for (size_t i = 0; query_words[0] && i < count; i++) {
        if (!routes[i].title || !routes[i].path || (routes[i].when & follow) != routes[i].when) continue;
        bf_route_consider(top, &n, limit, missing, query_words, qv, dims, routes[i].path, routes[i].title);
    }
    for (size_t s = 0; query_words[0] && s < nsets; s++) {
        sqlite3 *db = sets[s].open ? sets[s].open() : NULL;
        sqlite3_stmt *st = NULL;
        if (!db) continue;
        if (sqlite3_prepare_v2(db, sets[s].sql, -1, &st, NULL) == SQLITE_OK && sqlite3_stmt_readonly(st))
            while (sqlite3_step(st) == SQLITE_ROW)
                bf_route_consider(top, &n, limit, missing, query_words, qv, dims,
                                  (const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1));
        sqlite3_finalize(st);
        sqlite3_close(db);
    }
    while (n > 0 && top[n - 1].score < floor) n--;
    for (int i = 0; i < n; i++) rows[i] = (UiRow){top[i].href, top[i].title, top[i].href};
    ak_app_http(404, 1);
    ak_app_open("Page not found", "workbench", "route-not-found");
    ui_open(stdout, "header", "ak-wb-hero is-solo");
    ui_open(stdout, "div", NULL);
    ui_text(stdout, "h1", NULL, "Nothing is at this address");
    ui_close(stdout, "div");
    ui_close(stdout, "header");
    if (n) {
        ui_section_open(stdout, "not-found-suggestions", "Pages with a similar name");
        ui_rows(stdout, "ul", "ak-rows", rows, (unsigned)n);
        ui_section_close(stdout);
    } else {
        ui_empty(stdout, "Nothing here has a similar name.", "/", "Go to the front page");
    }
    ak_app_close();
    return 1;
}
#endif
