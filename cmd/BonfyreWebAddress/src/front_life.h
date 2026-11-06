/* The life front's editorial program.  See front_editorial.h.
 *
 * Life is explained and useful, the way the publications people keep open
 * for it are (Vox's numbered reads under series names, Eater's dining out and
 * at home, Air Mail's weekly issue).  The front is edited in this order:
 *
 *   a lead with a picture, the freshest one any of Life's sections holds;
 *   the day's reads, numbered, one from each section with its name over it,
 *   marked when it plays;
 *   something to cook tonight: pictured dishes from the food section, then
 *   three from the household recipe archive chosen for the season;
 *   out in Dallas: what starts in the next fortnight, on a map when three or
 *   more of those things say where they are.
 *
 * Every block reads material the estate already holds through the same
 * loaders, regions and pictures every other page uses, and a block with too
 * little behind it does not appear.  The front's own publications and events
 * are spent here, so the declared lead underneath does not repeat this one. */

#define FRONT_LIFE_MAX_KEYS 48
static char front_life_keys[FRONT_LIFE_MAX_KEYS][25];
static int front_life_key_count;

static void front_life_keep(const PageItem *item) {
    page_front_use(item);
    if (item->key[0] && front_life_key_count < FRONT_LIFE_MAX_KEYS)
        snprintf(front_life_keys[front_life_key_count++], sizeof(front_life_keys[0]), "%s", item->key);
}

static int front_life_shown(const PageItem *item) {
    int i;
    for (i = 0; i < front_life_key_count; i++)
        if (item->key[0] && !strcmp(front_life_keys[i], item->key)) return 1;
    return 0;
}

/* One section's material, read exactly as its own preview reads it, with the
 * section's sources as the fallback when its projection holds nothing. */
static PageMaterial *front_life_load(sqlite3 *db, PageProjectionRead *projection, const BfPageProgram *program) {
    PageMaterial *m; char sources[4096] = ""; PageDocument cut = {0}; int projected = -1; size_t k, held = 0;
    if (!program || !(m = calloc(1, sizeof(*m)))) return NULL;
    if (projection && projection->db) projected = page_subject_projection_material(projection, program->route, m);
    for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) held += (size_t)m->stream[k].count;
    if (projected < 0 || !held) {
        if (!projection || !projection->db ||
            page_subject_fabric_sources(projection->db, projection->subject, program, 8, sources, sizeof(sources)) < 0 || !sources[0])
            page_program_sources(db, program, 8, sources, sizeof(sources));
        if (sources[0]) page_collect_stories(db, sources, m);
    }
    cut.route = program->route; cut.program = program;
    page_filter_route_contract(db, &cut, m);
    return m;
}

static const char *front_life_name(const char *route) {
    const BfPageProgram *program = page_program_for_route(route);
    return program && program->title ? program->title : "";
}

static int front_life_plays(const PageItem *item) {
    return item->kind == BF_PAGE_MATERIAL_AUDIO || item->kind == BF_PAGE_MATERIAL_VIDEO;
}

/* The newest unshown item in a stream with a title, optionally one that has a
 * picture.  Streams are newest first; a dated item beats an undated one. */
static const PageItem *front_life_pick(const PageStream *s, int want_picture, time_t not_before) {
    int k;
    for (k = s->cursor; k < s->count; k++) {
        const PageItem *item = &s->item[k];
        if (!item->title[0] || front_life_shown(item)) continue;
        /* A read is already out: a thing dated after tomorrow (an event a year
         * off) is a listing.  "This week" also needs a date to be this week. */
        if (item->at > time(NULL) + 86400) continue;
        if (not_before && (!item->at || item->at < not_before)) continue;
        if (want_picture && !page_image_url_safe(item->image)) continue;
        return item;
    }
    return NULL;
}

/* The best thing to read from one section: a story first, then something
 * that plays, then a picture. */
static const PageItem *front_life_best(const PageMaterial *m, int want_picture, time_t not_before) {
    static const BfPageMaterial order[] = {BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_AUDIO,
                                           BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_PHOTO};
    size_t i; const PageItem *best = NULL;
    for (i = 0; i < sizeof(order) / sizeof(order[0]); i++) {
        const PageItem *item = front_life_pick(&m->stream[order[i]], want_picture, not_before);
        if (item && (!best || item->at > best->at)) best = item;
    }
    return best;
}

static void front_life_mark(const PageItem *item) {
    if (!front_life_plays(item)) return;
    fputs("<span class=\"lf-mark\">", stdout); page_glyph("play");
    fputs(item->kind == BF_PAGE_MATERIAL_AUDIO ? "Listen" : "Watch", stdout);
    if (item->duration_ms > 0) { fputs(" &middot; ", stdout); page_emit_duration(item->duration_ms); }
    fputs("</span>", stdout);
}

static void front_life_lead(const PageItem *item, const char *kicker, time_t now) {
    const char *close; char sized[1024];
    fputs("<article class=\"lf-lead\">", stdout);
    close = page_emit_item_open(item, "lf-lead-link");
    page_picture_at(item->image, 1280, sized, sizeof(sized));
    page_emit_picture(sized, "lf-lead-picture");
    fputs("<span class=\"lf-lead-words\">", stdout);
    if (kicker && *kicker) { fputs("<span class=\"lf-kicker\">", stdout); html(kicker); fputs("</span>", stdout); }
    fputs("<b class=\"lf-lead-title\">", stdout); html(item->title); fputs("</b>", stdout);
    if (item->body[0]) { fputs("<span class=\"dek\">", stdout); html(item->body); fputs("</span>", stdout); }
    fputs("</span>", stdout); fputs(close, stdout);
    fputs("<p class=\"by\">", stdout); page_emit_by(item, now); front_life_mark(item); fputs("</p></article>", stdout);
}

/* The season's dishes, in this part of Texas, as words a recipe title uses. */
static const char *front_life_season(int month) {
    static const char *const terms[12] = {
        "soup OR stew OR chili OR braised OR roast",
        "soup OR stew OR chili OR braised OR casserole",
        "asparagus OR lamb OR peas OR spring",
        "asparagus OR salad OR shrimp OR herb",
        "grilled OR salad OR shrimp OR tacos",
        "grilled OR tomato OR corn OR salad",
        "grilled OR tomato OR corn OR peach",
        "grilled OR tomato OR corn OR okra",
        "tomato OR corn OR okra OR enchiladas",
        "chili OR squash OR pumpkin OR stew OR soup",
        "roast OR squash OR sweet OR turkey OR casserole",
        "roast OR stew OR soup OR braised OR ham",
    };
    return terms[month < 0 || month > 11 ? 0 : month];
}

/* An archive title written in capitals reads as shouting; set it in title case. */
static void front_life_title(const char *title, char *out, size_t cap) {
    size_t n = 0; int lower = 0, start = 1; const char *p;
    for (p = title; *p; p++) if (*p >= 'a' && *p <= 'z') { lower = 1; break; }
    for (p = title; *p && n + 1 < cap; p++) {
        char c = *p;
        if (!lower && c >= 'A' && c <= 'Z' && !start) c = (char)(c - 'A' + 'a');
        out[n++] = c;
        start = !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '\'');
    }
    out[n] = '\0';
}

/* Three dishes from the household recipe archive for tonight: the season's
 * words, nothing sweet or on the side, a different three each day. */
#include "front_recipe_query.h"
static int front_life_archive_dishes(time_t now) {
    sqlite3 *rdb = NULL; sqlite3_stmt *st = NULL; char match[512]; struct tm tm;
    long total = 0, day = (long)(now / 86400); int shown = 0, i;
    localtime_r(&now, &tm);
    snprintf(match, sizeof(match), "title : (%s) NOT title : (dip OR cake OR cookies OR bars OR squares OR pie OR bread OR"
             " muffins OR relish OR sauce OR dressing OR canape OR cheesecake OR candy OR fudge OR punch OR jam OR drink)",
             front_life_season(tm.tm_mon));
    if (sqlite3_open_v2(bf_recipe_material_path(), &rdb, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(rdb); return 0; }
    if (sqlite3_prepare_v2(rdb, "SELECT count(*) FROM record_search WHERE record_search MATCH ?1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, match, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st); st = NULL;
    if (total >= 3 && sqlite3_prepare_v2(rdb, BF_FRONT_RECIPE_PICK_SQL, -1, &st, NULL) == SQLITE_OK) {
        fputs("<p class=\"lf-also\">From the recipe archive: ", stdout);
        for (i = 0; i < 3; i++) {
            char title[256];
            sqlite3_reset(st);
            sqlite3_bind_text(st, 1, match, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 2, (sqlite3_int64)(((day * 7919L) + i * (total / 3 + 1)) % total));
            if (sqlite3_step(st) != SQLITE_ROW) continue;
            front_life_title((const char *)sqlite3_column_text(st, 1), title, sizeof(title));
            if (shown) fputs(shown == 2 ? " or " : ", ", stdout);
            fputs("<a href=\"/recipes?id=", stdout); page_url_escape((const char *)sqlite3_column_text(st, 0));
            fputs("\">", stdout); html(title); fputs("</a>", stdout);
            shown++;
        }
        fputs(".</p>", stdout);
    }
    sqlite3_finalize(st); sqlite3_close(rdb);
    return shown;
}

static const BfPageRegion *front_life_region(const char *role) {
    size_t i;
    for (i = 0; i < BF_PAGE_REGION_COUNT; i++) if (!strcmp(bf_page_regions[i].role, role)) return &bf_page_regions[i];
    return NULL;
}

/* Dallas and Fort Worth, as a box on the map. */
static int front_life_in_dfw(const PageItem *item) {
    /* A real coordinate outranks a passing place-name mention in the text. */
    if (item->placed) return item->lat > 32.3 && item->lat < 33.5 &&
                             item->lon > -97.7 && item->lon < -96.3;
    return item->where[0] && (strstr(item->where, "Dallas") || strstr(item->where, "Fort Worth") ||
                              strstr(item->where, "Arlington") || strstr(item->where, "Plano") || strstr(item->where, "Denton"));
}

/* What starts in the next fortnight, the Dallas things first, as an agenda;
 * on a map as well when three or more say where they are. */
static int front_life_out(PageMaterial *const *sections, size_t count, const PageEmitContext *ctx) {
    PageStream *soon = calloc(1, sizeof(*soon));
    const BfPageRegion *agenda = front_life_region("soon"), *map = front_life_region("places");
    int pass, placed = 0, local = 0, k; size_t s; time_t now = ctx->now;
    if (!soon) return 0;
    for (pass = 0; pass < (local ? 1 : 2); pass++)
        for (s = 0; s < count; s++) {
            const PageStream *events = sections[s] ? &sections[s]->stream[BF_PAGE_MATERIAL_EVENT] : NULL;
            for (k = events ? events->cursor : 0; events && k < events->count && soon->count < 8; k++) {
                const PageItem *item = &events->item[k];
                int dfw = front_life_in_dfw(item);
                if (!item->at || item->at < now - 6 * 3600 || item->at > now + 14 * 86400) continue;
                if ((pass == 0) != dfw || front_life_shown(item)) continue;
                soon->item[soon->count++] = *item; front_life_keep(item);
                local += dfw; placed += item->placed;
            }
        }
    if (!soon->count || !agenda) { free(soon); return 0; }
    /* Read forwards, soonest first. */
    for (k = 1; k < soon->count; k++) {
        PageItem hold = soon->item[k]; int j = k - 1;
        while (j >= 0 && soon->item[j].at > hold.at) { soon->item[j + 1] = soon->item[j]; j--; }
        soon->item[j + 1] = hold;
    }
    fputs("<section class=\"lf-block lf-out\"><header class=\"lf-head\"><h2>", stdout);
    front_section_glyph(local ? "pin" : "calendar");
    fputs(local ? "Out in Dallas" : "Coming up", stdout);
    fputs("</h2><a class=\"lf-more\" href=\"/whats-on\">Everything on</a></header>", stdout);
    if (placed >= 3 && map) page_emit_region(map, soon, soon->count, ctx);
    page_emit_region(agenda, soon, soon->count, ctx);
    fputs("</section>", stdout);
    free(soon);
    return 1;
}

/* A dish is a recipe: its own address says so, or it comes from a recipe
 * index.  A food section also carries gift guides and restaurant news, which
 * are reads, not dinner. */
static int front_life_is_dish(const PageItem *item) {
    return strstr(item->href, "/recipe") || strstr(item->href, "themealdb") || strstr(item->source, "mealdb");
}

static int front_life_cook(const PageMaterial *food, time_t now) {
    const PageItem *dishes[3]; int n = 0, k, archive;
    const PageStream *streams[2];
    size_t s;
    if (food) {
        streams[0] = &food->stream[BF_PAGE_MATERIAL_PUBLICATION]; streams[1] = &food->stream[BF_PAGE_MATERIAL_PHOTO];
        for (s = 0; s < 2 && n < 3; s++)
            for (k = streams[s]->cursor; k < streams[s]->count && n < 3; k++) {
                const PageItem *item = &streams[s]->item[k];
                if (!item->title[0] || !page_image_url_safe(item->image) || front_life_shown(item)) continue;
                if (!front_life_is_dish(item)) continue;
                dishes[n++] = item; front_life_keep(item);
            }
    }
    fputs("<section class=\"lf-block lf-cook\"><header class=\"lf-head\"><h2>",stdout);
    front_section_glyph("chef");
    fputs("Something to cook tonight</h2><a class=\"lf-more\" href=\"/recipes/plan\">Plan the week</a></header>", stdout);
    if (n) {
        fputs("<ul class=\"lf-dishes\">", stdout);
        for (k = 0; k < n; k++) {
            const char *close; char sized[1024];
            fputs("<li>", stdout);
            close = page_emit_item_open(dishes[k], "lf-dish");
            page_picture_at(dishes[k]->image, 640, sized, sizeof(sized));
            page_emit_picture(sized, "lf-dish-picture");
            fputs("<b>", stdout); html(dishes[k]->title); fputs("</b>", stdout); fputs(close, stdout);
            fputs("<span class=\"by\">", stdout); page_emit_by(dishes[k], now); fputs("</span></li>", stdout);
        }
        fputs("</ul>", stdout);
    }
    archive = front_life_archive_dishes(now);
    fputs("<p class=\"lf-doors\"><a href=\"/recipes\">Find a recipe</a><a href=\"/food\">Food</a></p></section>", stdout);
    return n || archive;
}

static int front_editorial_life(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    /* The reads, in the order a person moves through a day off. */
    static const char *const reads[] = {"/local", "/food", "/home", "/health", "/travel", "/books", "/ideas"};
    static const char *const outings[] = {"/whats-on", "/local"};
    PageMaterial *section[sizeof(reads) / sizeof(reads[0])] = {0}, *going[2] = {0};
    PageProjectionRead local_projection = {0};
    const PageItem *lead = NULL; const char *lead_kicker = NULL;
    const PageItem *picks[sizeof(reads) / sizeof(reads[0])]; const char *kickers[sizeof(reads) / sizeof(reads[0])];
    size_t i; int shown = 0, n = 0;
    time_t now = ctx->now, week = ctx->now - 7 * 86400;
    (void)doc;
    front_life_key_count = 0;
    if (!projection || !projection->db) {
        page_projection_read_open(&local_projection, db, page_render_host);
        projection = &local_projection;
    }
    for (i = 0; i < sizeof(reads) / sizeof(reads[0]); i++) section[i] = front_life_load(db, projection, page_program_for_route(reads[i]));
    going[0] = front_life_load(db, projection, page_program_for_route(outings[0]));
    going[1] = section[0];

    /* The lead: the freshest picture any section holds this week, else the
     * front's own freshest picture. */
    for (i = 0; i < sizeof(reads) / sizeof(reads[0]); i++) {
        const PageItem *item = section[i] ? front_life_best(section[i], 1, week) : NULL;
        if (item && (!lead || item->at > lead->at)) { lead = item; lead_kicker = front_life_name(reads[i]); }
    }
    /* Nothing pictured this week: the freshest of the month, dated as it is. */
    for (i = 0; !lead && i < sizeof(reads) / sizeof(reads[0]); i++) {
        const PageItem *item = section[i] ? front_life_best(section[i], 1, now - 30 * 86400) : NULL;
        if (item) { lead = item; lead_kicker = front_life_name(reads[i]); }
    }
    /* Then the front's own family: pictured this week, else its newest story,
     * so a quiet week still opens on the city rather than on nothing. */
    if (!lead && material) {
        const PageItem *item = front_life_pick(&material->stream[BF_PAGE_MATERIAL_PUBLICATION], 1, week);
        if (!item) item = front_life_pick(&material->stream[BF_PAGE_MATERIAL_PUBLICATION], 0, 0);
        if (item) { lead = item; lead_kicker = NULL; }
    }
    fputs("<link rel=\"stylesheet\" href=\"/assets/front-life.css\">", stdout);
    fputs("<div class=\"lf\">", stdout);
    if (lead) { page_emit_profile_for(db, "life|lead", BF_PAGE_MATERIAL_PUBLICATION, ".lf-lead", NULL, 0); front_life_lead(lead, lead_kicker, now); front_life_keep(lead); shown++; }

    /* The day's reads: one per section, newest this week first, else newest. */
    for (i = 0; i < sizeof(reads) / sizeof(reads[0]); i++) {
        const PageItem *item = NULL;
        if (!section[i]) continue;
        if (!(item = front_life_best(section[i], 0, week))) item = front_life_best(section[i], 0, 0);
        if (!item) continue;
        picks[n] = item; kickers[n] = front_life_name(reads[i]); front_life_keep(item); n++;
    }
    if (n >= 3) {
        int k;
        page_emit_profile_for(db, "life|reads", BF_PAGE_MATERIAL_PUBLICATION, ".lf-reads", NULL, 0);
        fputs("<section class=\"lf-block lf-reads\"><header class=\"lf-head\"><h2>", stdout); front_section_glyph("book"); fputs("Worth reading today</h2></header><ol class=\"lf-list\">", stdout);
        for (k = 0; k < n; k++) {
            const char *close;
            fputs("<li><span class=\"lf-kicker\">", stdout); html(kickers[k]); fputs("</span>", stdout);
            close = page_emit_item_open(picks[k], "lf-read");
            fputs("<b>", stdout); html(picks[k]->title); fputs("</b>", stdout); fputs(close, stdout);
            if (picks[k]->body[0]) { fputs("<span class=\"dek\">", stdout); html(picks[k]->body); fputs("</span>", stdout); }
            fputs("<span class=\"by\">", stdout); page_emit_by(picks[k], now); front_life_mark(picks[k]); fputs("</span></li>", stdout);
        }
        fputs("</ol></section>", stdout);
        shown++;
    }

    /* Food and what's on are composed here in full, so their declared
     * sections below would only repeat them; the reads take one item from
     * each other section and leave the rest of it to its own preview. */
    if (section[1]) page_emit_profile_for(db, "life|cook", BF_PAGE_MATERIAL_PUBLICATION, ".lf-cook", NULL, 0);
    if (front_life_cook(section[1], now)) { shown++; page_front_take_route("/food"); }
    if (going[0] || going[1]) page_emit_profile_for(db, "life|out", BF_PAGE_MATERIAL_EVENT, ".lf-out", NULL, 0);
    if (front_life_out(going, 2, ctx)) { shown++; page_front_take_route("/whats-on"); }
    fputs("</div>", stdout);

    /* What this program showed is struck from the front's own material by
     * page_front_use; the declared lead and stage are skipped once it led. */
    for (i = 0; i < sizeof(reads) / sizeof(reads[0]); i++) free(section[i]);
    free(going[0]);
    page_projection_read_close(&local_projection);
    return shown;
}
