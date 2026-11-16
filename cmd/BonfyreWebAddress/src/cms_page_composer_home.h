#ifndef BONFYRE_CMS_PAGE_COMPOSER_HOME_H
#define BONFYRE_CMS_PAGE_COMPOSER_HOME_H
#include "cms_page_composer_visual.h"
/* /cms/compose with no page open, laid out as the Stitch artboard screen
 * (aurekai_publishing_cms_artboard_imposition_editor): the pages rail on the
 * left, the one way to start a page in the middle with the drafts waiting
 * under it, and a short summary of the workspace on the right. Every row is
 * a site_page record of this namespace; nothing here writes. Opening a page
 * (?route=) hands over to the section composer. */
static void cms_page_composer_home_emit(sqlite3 *db, const char *ns, const char *site) {
    enum { LIMIT = 200 };
    static char hrefs[LIMIT][300], titles[LIMIT][224], metas[LIMIT][160], dates[8][24];
    UiRow rows[LIMIT], drafts[8]; unsigned n = 0, nd = 0; int published = 0, draft = 0, total = 0;
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT route,title,status,substr(updated_at,1,10) FROM site_page WHERE namespace=?1 "
                               "ORDER BY updated_at DESC,route LIMIT 200", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW && n < LIMIT) {
            const char *r = cmc_col(st, 0), *status = cmc_col(st, 2);
            int is_draft = !strcmp(status, "draft"), is_pub = !strcmp(status, "published");
            cmc_href(hrefs[n], sizeof(hrefs[n]), "/cms/compose", r, 0, site);
            snprintf(titles[n], sizeof(titles[n]), "%s", *cmc_col(st, 1) ? cmc_col(st, 1) : r);
            snprintf(metas[n], sizeof(metas[n]), "%s \xc2\xb7 %s", is_pub ? "Published" : is_draft ? "Draft" : "Page", r);
            rows[n] = (UiRow){hrefs[n], titles[n], metas[n]};
            if (is_draft && nd < 8) {
                snprintf(dates[nd], sizeof(dates[nd]), "Changed %s", cmc_col(st, 3));
                drafts[nd] = (UiRow){hrefs[n], titles[n], *cmc_col(st, 3) ? dates[nd] : NULL};
                nd++;
            }
            published += is_pub; draft += is_draft; n++;
        }
    }
    sqlite3_finalize(st);
    {
        sqlite3_stmt *c = NULL;
        if (sqlite3_prepare_v2(db, "SELECT count(*),sum(status='published'),sum(status='draft') FROM site_page WHERE namespace=?1", -1, &c, NULL) == SQLITE_OK) {
            sqlite3_bind_text(c, 1, ns, -1, SQLITE_STATIC);
            if (sqlite3_step(c) == SQLITE_ROW) { total = sqlite3_column_int(c, 0); published = sqlite3_column_int(c, 1); draft = sqlite3_column_int(c, 2); }
        }
        sqlite3_finalize(c);
    }
    ui_open(stdout, "div", "ak-wb-strip");
    ui_text(stdout, "span", NULL, "Studio"); ui_text(stdout, "span", NULL, "Composer");
    ui_text(stdout, "span", "ak-live", site && *site ? "Your site" : "Your workspace");
    ui_close(stdout, "div");
    ui_open(stdout, "section", "cmc-head cmc-home-head");
    ui_open(stdout, "div", NULL); ui_open(stdout, "p", "ak-kicker");
    ui_link(stdout, "/studio", NULL, "Studio"); fputs(" &middot; Page composition", stdout); ui_close(stdout, "p");
    ui_text(stdout, "h1", NULL, "Shape a page people can use.");
    ui_text(stdout, "p", "ak-lede", "Open one of your pages or start a new one. Saving keeps a private draft; publishing is a separate review.");
    ui_close(stdout, "div"); ui_close(stdout, "section");

    static const UiLayoutValue desk_desktop[] = {{BF_NP_LAYOUT, 1 /* grid */}, {BF_NP_COLUMNS, 3}, {BF_NP_GAP, 20}};
    static const UiLayoutValue desk_tablet[] = {{BF_NP_LAYOUT, 1 /* grid */}, {BF_NP_COLUMNS, 2}, {BF_NP_GAP, 16}};
    static const UiLayoutValue desk_phone[] = {{BF_NP_GAP, 14}};
    static const UiLayout desk = {{[BF_NP_VP_DESKTOP] = {desk_desktop, 3}, [BF_NP_VP_TABLET] = {desk_tablet, 3}, [BF_NP_VP_PHONE] = {desk_phone, 1}}};
    ui_layout_open(stdout, "div", "cmc-home", &desk);

    /* Left: every page, newest first. */
    ui_section_open(stdout, "cmc-home-pages", "Your pages");
    if (n) ui_rows(stdout, "ul", "cmc-home-list", rows, n);
    else ui_text(stdout, "p", "quiet", "No pages yet. Start your first one here.");
    if (total > (int)n) { char more[96]; snprintf(more, sizeof(more), "Showing the %u most recent of %d pages.", n, total); ui_text(stdout, "p", "quiet", more); }
    ui_section_close(stdout);

    /* Middle: one field, one action, then the drafts waiting. */
    ui_section_open(stdout, "cmc-home-start", "Start a page");
    ui_text(stdout, "p", "quiet", "Give it an address. It stays a private draft until you publish it.");
    const FtField hidden[] = {{site && *site ? "site" : NULL, site}};
    const FtInput in[] = {{.label = "Page address", .name = "route", .placeholder = "/your-page/", .maxlength = "127", .pattern = "/[^?#]*", .required = 1}};
    const FtSubmit go[] = {{.label = "Start a page"}};
    const FtForm form = {.id = "compose-start", .method = "get", .action = "/cms/compose", .label = "Start a page", .hook = "cmc-open", .hook_value = "1",
                         .hidden = hidden, .nhidden = 1, .inputs = in, .ninputs = 1, .submits = go, .nsubmits = 1};
    ft_form(stdout, &form);
    {
        char visual[300]; cmc_href(visual, sizeof(visual), "/cms/compose/advanced", NULL, 0, site);
        ui_open(stdout, "p", "quiet"); ui_link(stdout, visual, NULL, "Or arrange a page by eye in the visual editor"); ui_close(stdout, "p");
    }
    if (nd) { ui_text(stdout, "h3", NULL, "Drafts waiting"); ui_rows(stdout, "ul", "cmc-home-list", drafts, nd); }
    ui_section_close(stdout);

    /* Right: the workspace in three numbers. */
    ui_section_open(stdout, "cmc-home-summary", "This workspace");
    char a[16], b[16], c[16];
    snprintf(a, sizeof(a), "%d", total); snprintf(b, sizeof(b), "%d", draft); snprintf(c, sizeof(c), "%d", published);
    ui_open(stdout, "div", "cmc-home-stats");
    ui_stat(stdout, "Pages", a); ui_stat(stdout, "Drafts", b); ui_stat(stdout, "Published", c);
    ui_close(stdout, "div");
    ui_text(stdout, "p", "quiet", site && *site ? "Pages here belong to your site. Publishing puts one live at its address."
                                                 : "Pages here are private to your account until you publish them.");
    ui_section_close(stdout);
    ui_close(stdout, "div");
}
#endif
