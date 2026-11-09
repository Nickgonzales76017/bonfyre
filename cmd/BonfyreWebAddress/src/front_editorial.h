/* A territory's front, edited.
 *
 * A front that only stacks grammars by date reads as nobody's.  The
 * publications people come back to choose: a lead picture from today, the
 * shows and the people who host them, tonight's lineup, a numbered list of
 * what to read, beats with bylines, the drops coming up.  Each territory's
 * front program lives in its own file and runs after the opening, before the
 * declared regions and sections; it returns how many blocks it showed.
 *
 * Included into main.c after the page grammars it composes from, so it uses
 * the same regions, streams and dressing as every other page and cannot
 * invent markup of its own beyond the editorial frame. */
#ifndef BONFYRE_FRONT_EDITORIAL_H
#define BONFYRE_FRONT_EDITORIAL_H
static void front_section_glyph(const char *name) {
    fputs("<span class=\"front-section-glyph\" aria-hidden=\"true\">",stdout);
    page_glyph(name);
    fputs("</span>",stdout);
}
static void front_editorial_icon_style(void) {
    fputs("<style>.front-section-glyph{display:inline-grid;place-items:center;width:1.35em;height:1.35em;margin-right:.35em;vertical-align:-.2em;border:1px solid color-mix(in srgb,currentColor 24%,transparent);border-radius:999px}.front-section-glyph .ak-icon{width:.78em;height:.78em}.section-head h2>a,.lf-head h2,.wf-head,.wf-beat-name>a,.community-front h2,.ex-made .section-head h2{display:inline-flex;align-items:center;gap:.15em}</style>",stdout);
}
#include "front_community.h"
#include "front_community.h"
#include "front_arrival.h"
#include "front_brand.h"

#ifndef FRONT_SKIP_LIVE
#include "front_live.h"
#endif
#ifndef FRONT_SKIP_SHOP
#include "front_shop.h"
#endif
#ifndef FRONT_SKIP_LIFE
#include "front_life.h"
#endif
#ifndef FRONT_SKIP_WORK
#include "front_work.h"
#endif

static int page_front_editorial(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    const char *territory = page_render_host ? territory_of(page_render_host) : NULL;
    char apex[32];
    if (!doc || !doc->route || strcmp(doc->route, "/") || !territory) return 0;
    /* A publisher or person's host keeps its own material and composition. */
    if (snprintf(apex, sizeof(apex), "aurekai.%s", territory) >= (int)sizeof(apex) ||
        strcmp(page_render_host, apex)) return 0;
    (void)db; (void)projection; (void)material; (void)ctx;
    int shown=0;
    front_editorial_icon_style();
    front_brand_emit(territory); /* brand-owned frame; the existing editorial lead retains its sources */
#ifndef FRONT_SKIP_LIVE
    if (!strcmp(territory, "live")) shown=front_editorial_live(db, projection, doc, material, ctx);
#endif
#ifndef FRONT_SKIP_SHOP
    if (!strcmp(territory, "shop")) shown=front_editorial_shop(db, projection, doc, material, ctx);
#endif
#ifndef FRONT_SKIP_LIFE
    if (!strcmp(territory, "life")) shown=front_editorial_life(db, projection, doc, material, ctx);
#endif
#ifndef FRONT_SKIP_WORK
    if (!strcmp(territory, "work")) shown=front_editorial_work(db, projection, doc, material, ctx);
#endif
    shown += front_arrival(db, territory);
    front_community(db);
    return shown;
}

#endif
