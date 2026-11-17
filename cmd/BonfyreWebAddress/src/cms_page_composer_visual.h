#ifndef BONFYRE_CMS_PAGE_COMPOSER_VISUAL_H
#define BONFYRE_CMS_PAGE_COMPOSER_VISUAL_H
#include <fcntl.h>
#include <unistd.h>
#include "form_tree.h"
#include "ui_tree.h"
/* Shell for the visual composition workspace (Stitch screen
 * aurekai_publishing_cms_artboard_imposition_editor: pages rail, canvas,
 * inspector). The left rail is drawn here from the page records: the site's
 * pages, one way to start a page, and the open page's settings as a short
 * summary whose values are edited in place. The @bonfyre/authoring bundle
 * mounts the canvas into [data-authoring-editor], reads the settings form when
 * it saves, and talks to /cms/compose/page, the same digest-guarded endpoint
 * as the section composer. ?site= selects a profile site workspace the bearer
 * owns; the endpoint enforces it. Styles: authoring.pcss on the Stitch tokens. */

/* A route or site id percent-encoded into a query value. */
static void cmc_query_put(char *out, size_t cap, size_t *at, const char *v) {
    static const char hex[] = "0123456789ABCDEF";
    for (; v && *v && *at + 4 < cap; v++) {
        unsigned char c = (unsigned char)*v;
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') out[(*at)++] = (char)c;
        else { out[(*at)++] = '%'; out[(*at)++] = hex[c >> 4]; out[(*at)++] = hex[c & 15]; }
    }
    out[*at] = 0;
}
/* base?route=<route>[&new=article][&site=<site>] */
static void cmc_href(char *out, size_t cap, const char *base, const char *route, int article, const char *site) {
    size_t at = (size_t)snprintf(out, cap, "%s", base);
    if (at >= cap) { out[0] = 0; return; }
    if (route && *route) { at += (size_t)snprintf(out + at, cap - at, "%sroute=", strchr(out, '?') ? "&" : "?"); cmc_query_put(out, cap, &at, route); }
    if (article && at + 16 < cap) at += (size_t)snprintf(out + at, cap - at, "%snew=article", strchr(out, '?') ? "&" : "?");
    if (site && *site && at + 8 < cap) { at += (size_t)snprintf(out + at, cap - at, "%ssite=", strchr(out, '?') ? "&" : "?"); cmc_query_put(out, cap, &at, site); }
}
/* An unguessable draft address: <prefix>draft-<20 hex>/. */
static int cmc_draft_route(char *out, size_t cap, const char *prefix) {
    unsigned char b[10]; size_t done = 0; int fd = open("/dev/urandom", O_RDONLY);
    static const char hex[] = "0123456789abcdef";
    if (fd < 0) return 0;
    while (done < sizeof(b)) { ssize_t n = read(fd, b + done, sizeof(b) - done); if (n <= 0) { close(fd); return 0; } done += (size_t)n; }
    close(fd);
    char id[21]; for (size_t i = 0; i < sizeof(b); i++) { id[2 * i] = hex[b[i] >> 4]; id[2 * i + 1] = hex[b[i] & 15]; } id[20] = 0;
    return snprintf(out, cap, "%sdraft-%s/", prefix, id) < (int)cap;
}
static const char *cmc_col(sqlite3_stmt *st, int i) { const char *v = (const char *)sqlite3_column_text(st, i); return v ? v : ""; }

/* The site's pages, newest first, and the one way to start another. */
static void cmc_pages_panel(sqlite3 *db, const char *ns, const char *route, const char *site) {
    enum { LIMIT = 200 };
    static char hrefs[LIMIT][300], titles[LIMIT][224], metas[LIMIT][40];
    UiRow rows[LIMIT]; unsigned n = 0; int total = 0;
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT route,title,status,(SELECT count(*) FROM site_page WHERE namespace=?1) FROM site_page "
                               "WHERE namespace=?1 ORDER BY updated_at DESC,route LIMIT 200", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW && n < LIMIT) {
            const char *r = cmc_col(st, 0), *status = cmc_col(st, 2);
            total = sqlite3_column_int(st, 3);
            cmc_href(hrefs[n], sizeof(hrefs[n]), "/cms/compose/advanced", r, 0, site);
            snprintf(titles[n], sizeof(titles[n]), "%s", *cmc_col(st, 1) ? cmc_col(st, 1) : r);
            snprintf(metas[n], sizeof(metas[n]), "%s%s", !strcmp(r, route) ? "Open \xc2\xb7 " : "",
                     !strcmp(status, "published") ? "Published" : !strcmp(status, "draft") ? "Draft" : "Page");
            rows[n] = (UiRow){hrefs[n], titles[n], metas[n]};
            n++;
        }
    }
    sqlite3_finalize(st);
    ui_section_open(stdout, "authoring-pages-panel", "Pages");
    if (n) ui_rows(stdout, "ul", "authoring-page-list", rows, n);
    else ui_text(stdout, "p", "authoring-pages-status", "No saved pages yet.");
    if (total > (int)n) { char more[96]; snprintf(more, sizeof(more), "Showing the %u most recent of %d pages.", n, total); ui_text(stdout, "p", "authoring-pages-status", more); }
    char fresh[64], start[300], article[300];
    ui_open(stdout, "p", "ak-actions");
    if (cmc_draft_route(fresh, sizeof(fresh), "/pages/")) { cmc_href(start, sizeof(start), "/cms/compose/advanced", fresh, 0, site); ui_link(stdout, start, "ak-button is-primary", "Start a page"); }
    cmc_href(article, sizeof(article), "/cms/compose/advanced", NULL, 1, site);
    ui_link(stdout, article, "ak-button", "Write an article");
    ui_close(stdout, "p");
    ui_section_close(stdout);
}

/* The open page's settings: every value comes from its record (or the
 * defaults a new page or article starts with), shown as a summary the
 * author edits in place. One action saves the page with its canvas. */
static void cmc_settings_panel(sqlite3 *db, const char *ns, const char *route, int new_article, const char *site) {
    char v[10][5008] = {{0}}; int found = 0;
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT title,heading,lede,editorial_kind,media_subject_ref,author_credit,author_subject_ref,"
                               "editorial_published_on,editorial_updated_on,status FROM site_page WHERE namespace=?1 AND route=?2 LIMIT 1",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) { found = 1; for (int i = 0; i < 10; i++) snprintf(v[i], sizeof(v[i]), "%s", cmc_col(st, i)); }
    }
    sqlite3_finalize(st);
    if (!found) {
        char name[128] = "";
        size_t len = strlen(route), b;
        while (len > 1 && route[len - 1] == '/') len--;
        for (b = len; b > 0 && route[b - 1] != '/'; b--) {}
        snprintf(name, sizeof(name), "%.*s", (int)(len - b), route + b);
        snprintf(v[0], sizeof(v[0]), "%s", new_article ? "Untitled article" : "Untitled page");
        snprintf(v[1], sizeof(v[1]), "%s", v[0]);
        if (new_article && *name) { snprintf(v[3], sizeof(v[3]), "article"); snprintf(v[4], sizeof(v[4]), "article:%s", name); }
    }
    if (!v[0][0]) snprintf(v[0], sizeof(v[0]), "Untitled page");
    if (!v[1][0]) snprintf(v[1], sizeof(v[1]), "%s", v[0]);
    ui_section_open(stdout, "authoring-page-settings", "This page");
    ui_start(stdout,"p");ui_attr(stdout,"class","authoring-page-visibility");ui_attr_open(stdout,"data-status");
    bf_lex_html_emit(stdout, found ? v[9] : "new");
    ui_attr_close(stdout);ui_gt(stdout);
    fputs(!found ? "New page \xc2\xb7 nothing saved yet" : !strcmp(v[9], "published") ? "Published \xc2\xb7 changes are reviewed before they replace it"
                                                       : "Private draft \xc2\xb7 not published", stdout);
    ui_close(stdout, "p");
    ui_text(stdout, "p", "authoring-page-path", route);
    static const char *const kinds[][2] = {{"", "Not an article"}, {"article", "Article"}, {"essay", "Essay"}, {"report", "Report"}, {"feature", "Feature"}, {"longform", "Long read"}};
    FtOption kind[6];
    for (int i = 0; i < 6; i++) kind[i] = (FtOption){kinds[i][0], kinds[i][1], !strcmp(kinds[i][0], v[3])};
    const FtInput in[] = {
        {.label = "Browser title", .name = "title", .value = v[0], .maxlength = "220", .required = 1},
        {.label = "Heading", .name = "heading", .value = v[1], .maxlength = "220", .required = 1},
        {.label = "Introduction", .name = "lede", .value = v[2], .maxlength = "5000", .placeholder = "One or two sentences under the heading", .rows = 2},
        {.label = "Article", .type = "select", .name = "editorial_kind", .options = kind, .noptions = 6},
        {.label = "Article address", .name = "media_subject_ref", .value = v[4], .maxlength = "104", .placeholder = "article:your-slug"},
        {.label = "Byline", .name = "author_credit", .value = v[5], .maxlength = "159", .placeholder = "Who wrote it"},
        {.label = "Author page", .name = "author_subject_ref", .value = v[6], .maxlength = "159", .placeholder = "person:handle"},
        {.label = "Published on", .type = "date", .name = "editorial_published_on", .value = v[7]},
        {.label = "Updated on", .type = "date", .name = "editorial_updated_on", .value = v[8]},
    };
    const FtSubmit save[] = {{.label = "Save this page"}};
    const FtForm form = {.id = "page-settings", .method = "post", .action = "/cms/compose/page", .label = "Save this page",
                         .hook = "authoring-page-settings", .hook_value = "1", .inputs = in, .ninputs = 9, .submits = save, .nsubmits = 1};
    if (!ft_form(stdout, &form)) {
        char back[300]; cmc_href(back, sizeof(back), "/cms/compose", route, 0, site);
        ui_empty(stdout, "This page's settings hold text the editor cannot show here. Open it in the section composer.", back, "Open in the composer");
    }
    ui_section_close(stdout);
}

static void cms_page_composer_visual_emit(sqlite3 *db, const char *ns, const char *route, int new_article, const char *site) {
    ui_open_a(stdout,"link","rel","stylesheet","href","/assets/authoring-shell.css");
    ui_open_a(stdout,"link","rel","stylesheet","href","/assets/authoring-grapes.css");
    ui_open(stdout,"div","ak-wb-strip");ui_open(stdout,"span",NULL);ui_copy(stdout,"Studio");ui_close(stdout,"span");
    ui_open(stdout,"span",NULL);ui_copy(stdout,"Visual editor");ui_close(stdout,"span");
    ui_open(stdout,"span","ak-live");ui_copy(stdout,"Same saved page as the composer");ui_close(stdout,"span");ui_close(stdout,"div");
    static const UiLayoutValue head_desktop[] = {{BF_NP_LAYOUT, 0 /* stack */}, {BF_NP_GAP, 12}};
    static const UiLayoutValue head_phone[] = {{BF_NP_GAP, 8}};
    static const UiLayout head = {{[BF_NP_VP_DESKTOP] = {head_desktop, 2}, [BF_NP_VP_PHONE] = {head_phone, 1}}};
    ui_layout_open(stdout, "section", "cmc-head cmc-visual-head", &head);
    ui_open(stdout, "div", NULL); ui_open(stdout, "p", "ak-kicker");
    if (site && *site) { char back[300]; cmc_href(back, sizeof(back), "/profile", NULL, 0, site); ui_link(stdout, back, NULL, "Your profile"); }
    else { ui_link(stdout, "/studio", NULL, "Studio"); fputs(" &middot; ", stdout); ui_link(stdout, "/cms/compose", NULL, "Composer"); }
    fputs(" &middot; Visual", stdout); ui_close(stdout, "p");
    ui_text(stdout, "h1", NULL, "Arrange it by eye.");
    ui_text(stdout, "p", "ak-lede", "Pick a page on the left, shape it on the canvas, and set the selected part on the right. Saving keeps a private draft; publishing is a separate review.");
    ui_close(stdout, "div"); ui_close(stdout, "section");
    ui_start(stdout,"div");ui_attr(stdout,"class","authoring-page cmc-visual");ui_flag(stdout,"data-aurekai-authoring");ui_gt(stdout);
    ui_open(stdout,"div","authoring-site-workspace");
    ui_start(stdout,"aside");ui_attr(stdout,"class","authoring-page-navigation");ui_attr(stdout,"aria-label","Pages and page settings");ui_attr_open(stdout,"data-authoring-rail");
    bf_lex_html_emit(stdout, route);
    ui_attr_close(stdout);ui_gt(stdout);
    cmc_pages_panel(db, ns, route, site);
    cmc_settings_panel(db, ns, route, new_article, site);
    ui_close(stdout, "aside");
    ui_start(stdout,"div");ui_attr(stdout,"class","authoring-page-editor");ui_flag(stdout,"data-authoring-editor");ui_gt(stdout);
    ui_text(stdout, "p", "ak-empty", "Opening the visual workspace\xe2\x80\xa6");
    ui_close(stdout,"div");ui_close(stdout,"div");ui_close(stdout,"div");
    ui_start(stdout,"script");ui_attr(stdout,"type","module");ui_attr(stdout,"src","/assets/authoring-editor.js");ui_gt(stdout);ui_close(stdout,"script");
}
#endif
