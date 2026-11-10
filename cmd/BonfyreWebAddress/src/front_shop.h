/* The shop front's editorial program.  See front_editorial.h.
 *
 * SHOP reads as a lookbook and a calendar of drops, the way a style magazine
 * run as a channel does: pictures first and tall, few words, where it is from
 * in small type; then what is coming out and when; then the picks of the
 * people whose taste readers follow, each under that person's or
 * publication's name; then the shops made here, as doors; and recalls last,
 * the part of shopping that protects people.  Everything comes from the
 * routes the shop front already declares (/style, /picks, /deals, /retail,
 * /recalls) and from the front's own material; nothing is priced, stocked
 * or recommended by the estate itself. */

#define FRONT_SHOP_LOOKS 7
#define FRONT_SHOP_DROPS 8
#define FRONT_SHOP_PICKS 48
#define FRONT_SHOP_RECALLS 5
#define FRONT_SHOP_DEALS 8

enum { FRONT_SHOP_TASTE, FRONT_SHOP_DEAL, FRONT_SHOP_RECALL };

typedef struct {
    PageItem looks[24]; int look_count;
    PageItem drops[24]; int drop_count;
    PageItem picks[FRONT_SHOP_PICKS]; int pick_count;
    PageItem recalls[FRONT_SHOP_RECALLS]; int recall_count;
    PageItem deals[24]; int deal_count;
    const char *shown[64]; int shown_count;   /* what the program put on the page */
} FrontShopPool;

static void front_shop_mark(FrontShopPool *pool, const PageItem *item) {
    if (item->href[0] && pool->shown_count < (int)(sizeof(pool->shown) / sizeof(pool->shown[0])))
        pool->shown[pool->shown_count++] = item->href;
    page_front_use(item);
}

/* What the program showed is spent: the front's own regions below carry on
 * from whatever is left rather than showing it twice. */
static void front_shop_spend(const FrontShopPool *pool, PageMaterial *material) {
    size_t kind; int i, j, keep;
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) {
        PageStream *stream = &material->stream[kind];
        for (i = keep = stream->cursor; i < stream->count; i++) {
            int spent = 0;
            for (j = 0; j < pool->shown_count && !spent; j++) spent = !strcmp(stream->item[i].href, pool->shown[j]);
            if (!spent) { if (keep != i) stream->item[keep] = stream->item[i]; keep++; }
        }
        stream->count = keep;
    }
}

static const char *front_shop_picture(const PageItem *item) {
    if (page_image_url_safe(item->image)) return item->image;
    if (page_image_url_safe(item->poster)) return item->poster;
    return NULL;
}

static int front_shop_seen(const FrontShopPool *pool, const PageItem *item) {
    int i;
    if (!item->href[0]) return 0;
    for (i = 0; i < pool->look_count; i++) if (!strcmp(pool->looks[i].href, item->href)) return 1;
    for (i = 0; i < pool->drop_count; i++) if (!strcmp(pool->drops[i].href, item->href)) return 1;
    for (i = 0; i < pool->pick_count; i++) if (!strcmp(pool->picks[i].href, item->href)) return 1;
    for (i = 0; i < pool->recall_count; i++) if (!strcmp(pool->recalls[i].href, item->href)) return 1;
    for (i = 0; i < pool->deal_count; i++) if (!strcmp(pool->deals[i].href, item->href)) return 1;
    return 0;
}

/* The publisher's own host, from where an item links: how a lookbook keeps
 * one publication from filling it. */
static void front_shop_host(const char *href, char *out, size_t cap) {
    const char *p = strstr(href, "://"); size_t n = 0;
    out[0] = '\0';
    if (!p) return;
    p += 3;
    if (!strncmp(p, "www.", 4)) p += 4;
    while (p[n] && p[n] != '/' && p[n] != ':' && p[n] != '?' && n + 1 < cap) { out[n] = p[n]; n++; }
    out[n] = '\0';
}

/* Sort what one route holds into the program's parts.  A recall route's
 * items are recalls and a deal route's are deals whatever they look like;
 * from the taste routes a dated thing still ahead is a drop, a pictured
 * thing is a look, and a worded one is a pick. */
static void front_shop_take(FrontShopPool *pool, PageMaterial *material, int role, time_t now) {
    size_t kind; int i;
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) {
        PageStream *stream = &material->stream[kind];
        for (i = stream->cursor; i < stream->count; i++) {
            const PageItem *item = &stream->item[i];
            if (!item->title[0] || !item->href[0] || front_shop_seen(pool, item)) continue;
            if (kind == BF_PAGE_MATERIAL_AUDIO || kind == BF_PAGE_MATERIAL_PLACE ||
                kind == BF_PAGE_MATERIAL_CHANNEL || kind == BF_PAGE_MATERIAL_ORGANIZATION) continue;
            if (role == FRONT_SHOP_RECALL) {
                if (pool->recall_count < FRONT_SHOP_RECALLS && (kind == BF_PAGE_MATERIAL_PUBLICATION || kind == BF_PAGE_MATERIAL_POST))
                    pool->recalls[pool->recall_count++] = *item;
                continue;
            }
            if (role == FRONT_SHOP_DEAL) {
                if (pool->deal_count < (int)(sizeof(pool->deals) / sizeof(pool->deals[0])) && kind != BF_PAGE_MATERIAL_EVENT)
                    pool->deals[pool->deal_count++] = *item;
                continue;
            }
            if (kind == BF_PAGE_MATERIAL_EVENT && item->at >= now - 6 * 3600) {
                if (pool->drop_count < (int)(sizeof(pool->drops) / sizeof(pool->drops[0]))) pool->drops[pool->drop_count++] = *item;
                continue;
            }
            if (front_shop_picture(item) && kind != BF_PAGE_MATERIAL_EVENT) {
                if (pool->look_count < (int)(sizeof(pool->looks) / sizeof(pool->looks[0]))) pool->looks[pool->look_count++] = *item;
                continue;
            }
            if ((kind == BF_PAGE_MATERIAL_PUBLICATION || kind == BF_PAGE_MATERIAL_POST) && item->by[0] &&
                pool->pick_count < FRONT_SHOP_PICKS)
                pool->picks[pool->pick_count++] = *item;
        }
    }
}

/* One declared route's material, read exactly as its own section reads it. */
static void front_shop_route(sqlite3 *db, PageProjectionRead *projection, const char *route,
                             FrontShopPool *pool, time_t now) {
    const BfPageProgram *program = page_program_for_route(route);
    PageMaterial *material; char sources[4096]; PageDocument cut = {0};
    PageProjectionRead local = {0};
    if (!program || !(material = calloc(1, sizeof(*material)))) return;
    if (!projection || !projection->db) { page_projection_read_open(&local, db, page_render_host); projection = &local; }
    if (page_subject_projection_material(projection, program->route, material) < 0) {
        if (page_subject_fabric_sources(projection->db, projection->subject, program, 8, sources, sizeof(sources)) < 0)
            page_program_sources(db, program, 8, sources, sizeof(sources));
        page_collect_stories(db, sources, material);
    }
    cut.route = program->route; cut.program = program;
    page_filter_route_contract(db, &cut, material);
    page_projection_read_close(&local);
    front_shop_take(pool, material, !strcmp(route, "/recalls") ? FRONT_SHOP_RECALL :
                    !strcmp(route, "/deals") ? FRONT_SHOP_DEAL : FRONT_SHOP_TASTE, now);
    free(material);
}

static int front_shop_by_time(const void *a, const void *b) {
    const PageItem *x = a, *y = b;
    return x->at < y->at ? -1 : x->at > y->at;
}
static int front_shop_newest(const void *a, const void *b) {
    const PageItem *x = a, *y = b;
    return x->at > y->at ? -1 : x->at < y->at;
}

/* A block's band in the house rhythm: field, paper or night. */
static void front_shop_band(sqlite3 *db, const char *selector, int band) {
    page_house_band = band;
    page_emit_profile_for(db, selector, BF_PAGE_MATERIAL_PHOTO, selector, NULL, 0);
}

static void front_shop_where(const PageItem *item, time_t now) {
    fputs("<p class=\"shop-where\">", stdout); page_emit_by(item, now); fputs("</p>", stdout);
}

static int front_shop_lookbook(sqlite3 *db, FrontShopPool *pool, time_t now) {
    /* A cover and its run fill whole rows: the cover takes four cells, so
     * five or nine pictures leave no hole.  No publication gets more than
     * two, so the book reads as many people's taste. */
    const PageItem *chosen[FRONT_SHOP_LOOKS]; char hosts[FRONT_SHOP_LOOKS][128];
    int i, j, count = 0, want;
    qsort(pool->looks, (size_t)pool->look_count, sizeof(pool->looks[0]), front_shop_newest);
    for (i = 0; i < pool->look_count && count < FRONT_SHOP_LOOKS; i++) {
        char host[128]; int same = 0;
        front_shop_host(pool->looks[i].href, host, sizeof(host));
        for (j = 0; j < count; j++) same += !strcmp(hosts[j], host);
        if (same >= 2) continue;
        snprintf(hosts[count], sizeof(hosts[count]), "%s", host);
        chosen[count++] = &pool->looks[i];
    }
    want = count >= 9 ? 9 : count >= 5 ? 5 : 0;
    if (!want) return 0;
    front_shop_band(db, ".shop-look", 0);
    fputs("<section class=\"section shop-look\" aria-label=\"Lookbook\"><p class=\"shop-kicker\">",stdout); front_section_glyph("shirt"); fputs("Lookbook</p><ol class=\"shop-looks\">", stdout);
    for (i = 0; i < want; i++) {
        const PageItem *item = chosen[i];
        front_shop_mark(pool, item);
        fputs(i ? "<li class=\"shop-look-item\">" : "<li class=\"shop-look-item shop-cover\">", stdout);
        { const char *close = page_emit_item_open(item, NULL);
        page_emit_picture(front_shop_picture(item), "shop-look-picture");
        fputs("<span class=\"shop-look-title\">", stdout); html(item->title); fputs("</span>", stdout); fputs(close, stdout); }
        front_shop_where(item, now);
        fputs("</li>", stdout);
    }
    fputs("</ol></section>", stdout);
    return 1;
}

/* Deals as the deal hunters post them: the price is in their words, so the
 * line is their headline as written, with who posted it and when. */
static int front_shop_deals(sqlite3 *db, FrontShopPool *pool, time_t now) {
    int i, take;
    if (pool->deal_count < 3) return 0;
    qsort(pool->deals, (size_t)pool->deal_count, sizeof(pool->deals[0]), front_shop_newest);
    take = pool->deal_count < FRONT_SHOP_DEALS ? pool->deal_count : FRONT_SHOP_DEALS;
    front_shop_band(db, ".shop-deals", 3);
    fputs("<section class=\"section shop-deals\"><header class=\"section-head\"><h2><a href=\"/deals\">",stdout); front_section_glyph("tag"); fputs("Deals</a></h2><p>Prices worth acting on, as the deal hunters post them.</p></header><ol class=\"shop-deal-list\">", stdout);
    for (i = 0; i < take; i++) {
        const PageItem *item = &pool->deals[i];
        front_shop_mark(pool, item);
        fputs("<li>", stdout); { const char *close = page_emit_item_open(item, NULL);
        page_emit_picture(front_shop_picture(item), "shop-deal-picture");
        fputs("<span class=\"shop-deal-title\">", stdout); html(item->title); fputs("</span>", stdout); fputs(close, stdout); }
        front_shop_where(item, now); fputs("</li>", stdout);
    }
    fputs("</ol></section>", stdout);
    return 1;
}

static int front_shop_drops(sqlite3 *db, FrontShopPool *pool, time_t now) {
    int i, take;
    if (!pool->drop_count) return 0;
    qsort(pool->drops, (size_t)pool->drop_count, sizeof(pool->drops[0]), front_shop_by_time);
    take = pool->drop_count < FRONT_SHOP_DROPS ? pool->drop_count : FRONT_SHOP_DROPS;
    front_shop_band(db, ".shop-drops", 1);
    fputs("<section class=\"section shop-drops\"><header class=\"section-head\"><h2>",stdout); front_section_glyph("calendar"); fputs("Coming out</h2><p>Releases, launches and sales, in the order they land.</p></header><ol class=\"shop-calendar\">", stdout);
    for (i = 0; i < take; i++) {
        const PageItem *item = &pool->drops[i];
        struct tm tm; char ahead[64];
        front_shop_mark(pool, item);
        page_when_ahead(item->at, 0, now, ahead, sizeof(ahead));
        fputs("<li>", stdout); { const char *close = page_emit_item_open(item, NULL);
        if (page_local_time(item->at, &tm))
            printf("<span class=\"shop-day\"><b>%d</b>%s</span>", tm.tm_mday, page_month_names[tm.tm_mon]);
        fputs("<span class=\"shop-drop-title\">", stdout); html(item->title); fputs("</span>", stdout);
        fputs("<span class=\"shop-drop-when\">", stdout); html(ahead); if (item->by[0]) { fputs(" &middot; ", stdout); html(item->by); }
        fputs("</span>", stdout); fputs(close, stdout); } fputs("</li>", stdout);
    }
    fputs("</ol></section>", stdout);
    return 1;
}

/* Picks are grouped under whoever picked them: a reader follows a voice, so
 * the voice is the heading and its last few picks sit under it. */
static int front_shop_voices(sqlite3 *db, FrontShopPool *pool, time_t now) {
    int i, j, voices = 0; unsigned char used[FRONT_SHOP_PICKS] = {0};
    if (pool->pick_count < 2) return 0;
    qsort(pool->picks, (size_t)pool->pick_count, sizeof(pool->picks[0]), front_shop_newest);
    front_shop_band(db, ".shop-voices", 2);
    fputs("<section class=\"section shop-voices\"><header class=\"section-head\"><h2>",stdout); front_section_glyph("mic"); fputs("Worth buying</h2><p>What reviewers and editors are picking, under their own names.</p></header><div class=\"shop-voice-grid\">", stdout);
    for (i = 0; i < pool->pick_count && voices < 6; i++) {
        int shown = 0;
        if (used[i]) continue;
        {
            char host[128];
            front_shop_host(pool->picks[i].href, host, sizeof(host));
            fputs("<section class=\"shop-voice\"><h3>", stdout); html(pool->picks[i].by); fputs("</h3>", stdout);
            if (host[0] && strcasecmp(host, pool->picks[i].by)) { fputs("<p class=\"shop-voice-from\">", stdout); html(host); fputs("</p>", stdout); }
            fputs("<ul>", stdout);
        }
        for (j = i; j < pool->pick_count && shown < 3; j++) {
            const PageItem *item = &pool->picks[j];
            char when[48];
            if (used[j] || strcmp(item->by, pool->picks[i].by)) continue;
            used[j] = 1; shown++; front_shop_mark(pool, item);
            page_when(item->at, now, when, sizeof(when));
            fputs("<li>", stdout); { const char *close = page_emit_item_open(item, NULL); html(item->title);
            fputs(close, stdout); }
            if (when[0]) { fputs("<time>", stdout); html(when); fputs("</time>", stdout); }
            fputs("</li>", stdout);
        }
        fputs("</ul></section>", stdout);
        for (j = i; j < pool->pick_count; j++) if (!strcmp(pool->picks[j].by, pool->picks[i].by)) used[j] = 1;
        voices++;
    }
    fputs("</div></section>", stdout);
    return voices > 0;
}

/* The shops made on this estate, as doors.  Each is a working sample site;
 * its own pages say so, and the door says it too. */
static int front_shop_doors(sqlite3 *db) {
    sqlite3_stmt *st = NULL; int shown = 0;
    if (!db || sqlite3_prepare_v2(db,
        "SELECT p.host, t.reference_name,"
        " (SELECT f.heading FROM site_fragment f WHERE f.namespace=p.site_id AND f.page='/offers' ORDER BY f.position LIMIT 1),"
        " (SELECT f.body FROM site_fragment f WHERE f.namespace=p.site_id AND f.page='/offers' ORDER BY f.position LIMIT 1)"
        " FROM site_presentation p JOIN presentation_template t ON t.template = replace(p.profile,'ghost:','') AND t.status='published'"
        " WHERE p.status='published' AND p.host LIKE '%.aurekai.shop' ORDER BY p.host", -1, &st, NULL) != SQLITE_OK)
        return 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *host = (const char *)sqlite3_column_text(st, 0), *name = (const char *)sqlite3_column_text(st, 1);
        const char *offer = (const char *)sqlite3_column_text(st, 2), *says = (const char *)sqlite3_column_text(st, 3);
        if (!host || !name || strspn(host, "abcdefghijklmnopqrstuvwxyz0123456789.-") != strlen(host)) continue;
        if (!shown++) {
            front_shop_band(db, ".shop-doors", 0);
            fputs("<section class=\"section shop-doors\"><header class=\"section-head\"><h2>",stdout); front_section_glyph("store"); fputs("Shops made here</h2><p>Sample shops built on Aurekai. Each one runs its own pages, offers and enquiries.</p></header><ul class=\"shop-door-list\">", stdout);
        }
        fputs("<li><a href=\"https://", stdout); html(host); fputs("/\"><span class=\"shop-door-name\">", stdout); html(name);
        fputs("</span>", stdout);
        if (offer && *offer) { fputs("<span class=\"shop-door-offer\">", stdout); html(offer); if (says && *says) { fputs(". ", stdout); html(says); } fputs("</span>", stdout); }
        fputs("<span class=\"shop-door-host\">", stdout); html(host); fputs("</span></a></li>", stdout);
    }
    sqlite3_finalize(st);
    if (shown) fputs("</ul></section>", stdout);
    return shown > 0;
}

static int front_shop_recalls(sqlite3 *db, FrontShopPool *pool, time_t now) {
    int i;
    if (!pool->recall_count) return 0;
    qsort(pool->recalls, (size_t)pool->recall_count, sizeof(pool->recalls[0]), front_shop_newest);
    front_shop_band(db, ".shop-recalls", 1);
    fputs("<section class=\"section shop-recalls\"><header class=\"section-head\"><h2><a href=\"/recalls\">",stdout); front_section_glyph("warning"); fputs("Recalled</a></h2><p>Products and food pulled from shelves, as the agencies announced them.</p></header><ul>", stdout);
    for (i = 0; i < pool->recall_count; i++) {
        const PageItem *item = &pool->recalls[i];
        front_shop_mark(pool, item);
        fputs("<li>", stdout); { const char *close = page_emit_item_open(item, NULL); html(item->title);
        fputs(close, stdout); } front_shop_where(item, now); fputs("</li>", stdout);
    }
    fputs("</ul></section>", stdout);
    return 1;
}

static int front_editorial_shop(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    /* Retail is trade news about stores, not taste; it keeps its own section. */
    static const char *const routes[] = {"/style", "/picks", "/deals", "/recalls"};
    FrontShopPool *pool; time_t now = ctx && ctx->now ? ctx->now : time(NULL);
    int shown = 0; size_t r;
    (void)doc;
    if (!(pool = calloc(1, sizeof(*pool)))) return 0;
    /* Only the shop's own routes feed the program: the front's general pool
     * is whatever the shopping family happens to hold, and a conference
     * poster is not a look. */
    for (r = 0; r < sizeof(routes) / sizeof(routes[0]); r++) {
        front_shop_route(db, projection, routes[r], pool, now);
        page_front_take_route(routes[r]);
    }
    /* The page column and its fonts first, so every band below is dressed. */
    page_emit_profile_for(db, "shop", BF_PAGE_MATERIAL_PHOTO, ".wrap", NULL, 0);
    fputs("<link rel=\"stylesheet\" href=\"/assets/front-shop.css\">", stdout);
    shown += front_shop_lookbook(db, pool, now);
    shown += front_shop_drops(db, pool, now);
    shown += front_shop_voices(db, pool, now);
    shown += front_shop_deals(db, pool, now);
    shown += front_shop_doors(db);
    shown += front_shop_recalls(db, pool, now);
    if (material) front_shop_spend(pool, material);
    free(pool);
    return shown;
}
