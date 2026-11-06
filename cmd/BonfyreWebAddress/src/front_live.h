/* The live front's editorial program.  See front_editorial.h.
 *
 * Live is shows and the people who make them, what dropped today and what is
 * on tonight; the way Crooked opens on its lineup and The Ringer runs its shows
 * across the top.  So the front opens on one picture from today, then the
 * shows with their newest episode and who is on it, then what is on this week,
 * then a few things to watch and to put on.  Everything comes from the same
 * routes the sections below read (watch, listen, what's on, film, music,
 * sports), gathered once, and nothing appears twice. */

#define LIVE_FRESH_LEAD (3 * 86400)   /* a lead older than this is not today's */
#define LIVE_FRESH_SHOW (21 * 86400)  /* a show quiet for longer is off the strip */

static const char *const live_routes[] = {"/watch", "/listen", "/whats-on", "/film", "/music", "/sports", "/stage"};

/* Keys this front has already shown, so a later block does not repeat one. */
static char live_seen[96][25];
static int live_seen_count;

static int live_was_seen(const PageItem *item) {
    int i;
    if (!item->key[0]) return 0;
    for (i = 0; i < live_seen_count; i++) if (!strcmp(live_seen[i], item->key)) return 1;
    return 0;
}

static void live_mark_seen(const PageItem *item) {
    page_front_use(item);   /* the sections below strike it from their material */
    if (!item->key[0] || live_seen_count >= (int)(sizeof(live_seen) / sizeof(live_seen[0]))) return;
    snprintf(live_seen[live_seen_count++], sizeof(live_seen[0]), "%s", item->key);
}

/* One route's material, read the way its own section reads it. */
static void live_collect_route(sqlite3 *db, PageProjectionRead *projection, const char *route, PageMaterial *into) {
    const BfPageProgram *program = page_program_for_route(route);
    PageMaterial *material; PageDocument cut = {0}; char sources[4096];
    PageProjectionRead local = {0};
    size_t kind; int k;
    if (!program || !(material = calloc(1, sizeof(*material)))) return;
    if (!projection || !projection->db) { page_projection_read_open(&local, db, page_render_host); projection = &local; }
    int projected = page_subject_projection_material(projection, program->route, material);
    if (projected < 0) {
        if (page_subject_fabric_sources(projection->db, projection->subject, program, 8, sources, sizeof(sources)) < 0)
            page_program_sources(db, program, 8, sources, sizeof(sources));
        page_collect_stories(db, sources, material);
    }
    cut.route = program->route; cut.program = program;
    page_filter_route_contract(db, &cut, material);
    page_projection_read_close(&local);
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) {
        PageStream *from = &material->stream[kind], *to = &into->stream[kind];
        for (k = from->cursor; k < from->count && to->count < PAGE_STREAM_MAX; k++) {
            const PageItem *item = &from->item[k];
            PageItem *held = page_same_item(to, item);
            if (held) page_fold_item(held, item);
            else to->item[to->count++] = *item;
        }
    }
    free(material);
}

/* The shows themselves: every published feed of episodes, video or audio,
 * filed under one of this front's families or as a creator's own channel.
 * The routes above read stories about entertainment; this reads the shows. */
static void live_collect_shows(sqlite3 *db, const BfPageProgram *front, PageMaterial *into) {
    sqlite3_stmt *st = NULL; char sources[4096] = ""; size_t used = 0, k;
    PageMaterial *material; int kind, i;
    if (!db || !front || !cms_type_registered(db, "internet_source")) return;
    if (sqlite3_prepare_v2(db, "SELECT source FROM internet_source WHERE status='published' AND COALESCE(twin,'')<>''"
                               " AND kind IN ('video','audio') AND (family=?1 OR family='creators')"
                               " GROUP BY source ORDER BY MIN(id) LIMIT 24", -1, &st, NULL) != SQLITE_OK) return;
    for (k = 0; k < front->family_count; k++) {
        sqlite3_reset(st); sqlite3_bind_text(st, 1, front->families[k], -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0); int n;
            if (!source || strchr(source, ',') || page_csv_has(sources, source)) continue;
            n = snprintf(sources + used, sizeof(sources) - used, "%s%s", used ? "," : "", source);
            if (n < 0 || used + (size_t)n >= sizeof(sources)) break;
            used += (size_t)n;
        }
    }
    sqlite3_finalize(st);
    if (!used || !(material = calloc(1, sizeof(*material)))) return;
    page_collect_stories(db, sources, material);
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) {
        PageStream *from = &material->stream[kind], *to = &into->stream[kind];
        for (i = from->cursor; i < from->count && to->count < PAGE_STREAM_MAX; i++) to->item[to->count++] = from->item[i];
    }
    free(material);
}

static int live_has_picture(const PageItem *item) {
    return page_image_url_safe(item->image) || page_image_url_safe(item->poster);
}

static const char *live_picture(const PageItem *item) {
    return page_image_url_safe(item->image) ? item->image : item->poster;
}

/* A picture at the width the band shows it.  The commons size their own; a
 * WordPress newsroom (Deadline, Variety, most of the trades) answers ?w= with
 * a resized copy, where the original can run to eight megabytes. */
static void live_sized(const char *url, int width, char *out, size_t cap) {
    page_picture_at(url, width, out, cap);
    if (!page_image_url_safe(out)) snprintf(out, cap, "%s", url);
    if (strstr(out, "/wp-content/uploads/") && !strchr(out, '?')) {
        size_t n = strlen(out);
        if (n + 16 < cap) snprintf(out + n, cap - n, "?w=%d", width);
    }
}

/* A show's own name is what its feed calls itself, not who is credited on an
 * episode: "SmartLess", not the company that distributes it. */
static void live_show_name(sqlite3 *db, const PageItem *item, char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    snprintf(out, cap, "%s", item->group[0] ? item->group : item->by);
    if (!db || !item->source[0] || sqlite3_prepare_v2(db, "SELECT label FROM internet_source WHERE source=?1 AND status='published'"
                                                          " AND COALESCE(label,'')<>'' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, item->source, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
}

/* A picture every episode of a show carries is the show's cover, not a
 * picture of this episode: right for the strip of shows, wrong for a lead. */
static int live_is_cover(PageMaterial *m, const PageItem *item) {
    PageStream *s = &m->stream[item->kind]; int k, same = 0;
    const char *picture = live_picture(item);
    for (k = s->cursor; k < s->count; k++)
        if (&s->item[k] != item && !strcmp(s->item[k].source, item->source) && !strcmp(live_picture(&s->item[k]), picture)) same++;
    return same > 0;
}

/* The newest item with a picture of its own, from today or close to it.  A
 * video beats a story; nothing stale, nothing behind a logo or a cover. */
static const PageItem *live_pick_lead(PageMaterial *m, time_t now) {
    static const int order[] = {BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_AUDIO, BF_PAGE_MATERIAL_PHOTO};
    const PageItem *best = NULL; size_t o; int k;
    for (o = 0; o < sizeof(order) / sizeof(order[0]); o++) {
        PageStream *s = &m->stream[order[o]];
        for (k = s->cursor; k < s->count; k++) {
            const PageItem *item = &s->item[k]; const char *picture = live_picture(item);
            if (!item->title[0] || !live_has_picture(item) || item->at <= 0 || item->at > now + 3600 ||
                now - item->at > LIVE_FRESH_LEAD || live_was_seen(item)) continue;
            if (strstr(picture, "logo") || strstr(picture, "favicon") || strstr(picture, "/icon") || live_is_cover(m, item)) continue;
            /* A story with words to stand on beats a bare headline. */
            if (!best || item->at > best->at + 6 * 3600 || (!best->body[0] && item->body[0] && item->at + 6 * 3600 > best->at)) best = item;
        }
        if (best) return best;
    }
    return NULL;
}

static void live_emit_lead(sqlite3 *db, const PageItem *item, time_t now) {
    char sized[1024], show[256]; const char *close;
    live_sized(live_picture(item), 1600, sized, sizeof(sized));
    live_show_name(db, item, show, sizeof(show));
    /* A long headline takes a smaller size rather than a taller column. */
    fputs(strlen(item->title) > 64 ? "<section class=\"live-lead region-dress-live-lead is-long\">"
                                   : "<section class=\"live-lead region-dress-live-lead\">", stdout);
    close = page_emit_item_open(item, "live-lead-link");
    page_emit_picture(sized, "live-lead-picture");
    fputs("<div class=\"live-lead-words\"><p class=\"live-kicker\">", stdout);
    html(item->kind == BF_PAGE_MATERIAL_VIDEO ? "New episode" : item->kind == BF_PAGE_MATERIAL_AUDIO ? "New episode" : "Today");
    if (show[0]) { fputs(" &middot; ", stdout); html(show); }
    fputs("</p><h2>", stdout); html(item->title); fputs("</h2>", stdout);
    if (item->body[0]) { fputs("<p class=\"live-dek\">", stdout); html(item->body); fputs("</p>", stdout); }
    fputs("<p class=\"by\">", stdout); page_emit_by(item, now); fputs("</p></div>", stdout);
    fputs(close, stdout);
    fputs("</section>", stdout);
}

/* The shows: each one once, newest episode first, with who is on it.  The
 * strip is the programme grid a network leads with: the show's name, its
 * cover, who hosts it and what it put out last. */
typedef struct { const PageItem *latest; const PageItem *cover; char name[256]; } LiveShow;

static int live_emit_shows(sqlite3 *db, PageMaterial *m, time_t now) {
    static const int kinds[] = {BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_AUDIO};
    LiveShow shows[16]; int count = 0, i, j, k; size_t o;
    for (o = 0; o < sizeof(kinds) / sizeof(kinds[0]); o++) {
        PageStream *s = &m->stream[kinds[o]];
        for (k = s->cursor; k < s->count; k++) {
            const PageItem *item = &s->item[k]; char name[256];
            if (!item->title[0] || item->at <= 0 || item->at > now + 3600 || now - item->at > LIVE_FRESH_SHOW || live_was_seen(item)) continue;
            live_show_name(db, item, name, sizeof(name));
            if (!name[0]) continue;
            for (i = 0; i < count; i++) {
                const char *source = shows[i].latest->source;
                if (item->source[0] && source[0] && !strcmp(item->source, source)) break;
                if (!item->source[0] && !source[0] && !strcasecmp(shows[i].name, name)) break;
            }
            if (i == count) {
                if (count == (int)(sizeof(shows) / sizeof(shows[0]))) continue;
                memset(&shows[count], 0, sizeof(shows[count]));
                snprintf(shows[count].name, sizeof(shows[count].name), "%s", name);
                shows[count++].latest = item;
            } else if (item->at > shows[i].latest->at) shows[i].latest = item;
            if (!shows[i].cover && live_has_picture(item)) shows[i].cover = item;
        }
    }
    if (!count) return 0;
    for (i = 1; i < count; i++)
        for (j = i; j > 0 && shows[j].latest->at > shows[j - 1].latest->at; j--) {
            LiveShow keep = shows[j]; shows[j] = shows[j - 1]; shows[j - 1] = keep;
        }
    if (count > 8) count = 8;
    page_emit_profile_for(db, "live|shows", BF_PAGE_MATERIAL_VIDEO, ".region-dress-live-shows", NULL, 0);
    fputs("<section class=\"live-shows region-dress-live-shows\"><header class=\"section-head\"><h2>",stdout);
    front_section_glyph("play");
    fputs("The shows</h2><a class=\"see-all\" href=\"/listen\">See all</a></header><ol class=\"live-show-list\">", stdout);
    for (i = 0; i < count; i++) {
        const PageItem *item = shows[i].latest; const char *close; char when[48], sized[1024];
        page_when(item->at, now, when, sizeof(when));
        fputs("<li data-live-source=\"", stdout); html(item->source); fputs("\">", stdout);
        close = page_emit_item_open(item, "live-show");
        /* Artwork belongs to this source. Another source's missing cover
         * must not erase supplied pictures from the entire lineup. */
        if (shows[i].cover) {
            live_sized(live_picture(shows[i].cover), 480, sized, sizeof(sized));
            page_emit_picture(sized, "live-show-cover");
        }
        fputs("<span class=\"live-show-name\">", stdout); html(shows[i].name); fputs("</span>", stdout);
        fputs("<span class=\"live-show-latest\">", stdout); html(item->title); fputs("</span>", stdout);
        fputs(close, stdout);
        fputs("<span class=\"live-show-meta\">", stdout);
        if (item->by[0] && strcasecmp(item->by, shows[i].name) && !strstr(item->by, "LLC") && !strstr(item->by, " | ")) {
            fputs("With ", stdout); html(item->by); fputs(" &middot; ", stdout);
        }
        html(item->kind == BF_PAGE_MATERIAL_AUDIO ? "Listen" : "Watch");
        if (when[0]) { fputs(" &middot; ", stdout); html(when); }
        fputs("</span></li>", stdout);
        live_mark_seen(item);
    }
    fputs("</ol></section>", stdout);
    return 1;
}

static const BfPageRegion *live_region(const char *role) {
    size_t i;
    for (i = 0; i < BF_PAGE_REGION_COUNT; i++) if (!strcmp(bf_page_regions[i].role, role)) return &bf_page_regions[i];
    return NULL;
}

/* Picks from one stream into a scratch stream, unseen only, a few from any
 * one source so a band is a choice and not one feed's back catalogue.
 * Events read forwards from now; everything else newest first. */
static int live_pick(PageStream *from, PageStream *into, int want, time_t now, time_t ahead, int need_picture, int per_source,
                     time_t max_age) {
    int k, j, round;
    into->count = into->cursor = 0;
    for (round = 1; round <= per_source && into->count < want; round++)
        for (k = from->cursor; k < from->count && into->count < want; k++) {
            const PageItem *item = &from->item[k]; int same = 0, taken = 0;
            if (!item->title[0] || live_was_seen(item) || (need_picture && !live_has_picture(item))) continue;
            if (ahead) { if (item->at <= 0 || item->at + 3 * 3600 < now || item->at > now + ahead) continue; }
            else if (item->at <= 0 || now - item->at > max_age) continue;
            for (j = 0; j < into->count; j++) {
                if (!strcmp(into->item[j].source, item->source)) same++;
                if (!strcmp(into->item[j].key, item->key) && item->key[0]) taken = 1;
            }
            if (taken || same >= round) continue;
            into->item[into->count++] = *item;
        }
    for (k = 1; k < into->count; k++)
        for (j = k; j > 0 && (ahead ? into->item[j].at < into->item[j - 1].at : into->item[j].at > into->item[j - 1].at); j--) {
            PageItem keep = into->item[j]; into->item[j] = into->item[j - 1]; into->item[j - 1] = keep;
        }
    return into->count;
}

static int live_emit_block(sqlite3 *db, const char *role, const char *title, const char *route, const char *dress,
                           BfPageMaterial material, PageStream *picks, PageEmitContext *ctx) {
    const BfPageRegion *region = live_region(role); char seed[64], selector[64]; int k;
    if (!region || !picks->count) return 0;
    snprintf(seed, sizeof(seed), "live|%s", dress);
    snprintf(selector, sizeof(selector), ".region-dress-%s", dress);
    page_emit_profile_for(db, seed, material, selector, NULL, 0);
    printf("<section class=\"live-block region-dress-%s section-%s\"><header class=\"section-head\"><h2><a href=\"", dress, role);
    html(route); fputs("\">", stdout); front_section_glyph(page_nav_glyph(route)); html(title); fputs("</a></h2><a class=\"see-all\" href=\"", stdout);
    html(route); fputs("\">See all</a></header>", stdout);
    page_emit_region(region, picks, picks->count, ctx);
    fputs("</section>", stdout);
    for (k = 0; k < picks->count; k++) live_mark_seen(&picks->item[k]);
    /* This front has shown that route's preview; its section is not repeated. */
    page_front_take_route(route);
    return 1;
}

static int front_editorial_live(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    PageMaterial *all = NULL; PageStream *picks = NULL;
    const PageItem *lead; size_t r, kind; int shown = 0, k, preview;
    time_t now = ctx ? ctx->now : time(NULL);
    live_seen_count = 0;
    if (!ctx || !(all = calloc(1, sizeof(*all))) || !(picks = calloc(1, sizeof(*picks)))) { free(all); return 0; }
    /* The front's own material first, then the shows, then every route its
     * sections read. */
    if (material)
        for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++)
            for (k = material->stream[kind].cursor; k < material->stream[kind].count && all->stream[kind].count < PAGE_STREAM_MAX; k++)
                all->stream[kind].item[all->stream[kind].count++] = material->stream[kind].item[k];
    live_collect_shows(db, doc ? doc->program : NULL, all);
    for (r = 0; r < sizeof(live_routes) / sizeof(live_routes[0]); r++) live_collect_route(db, projection, live_routes[r], all);

    fputs("<link rel=\"stylesheet\" href=\"/assets/front-live.css\">", stdout);
    /* Each band takes the house's next ground as it is shown: the cover story
     * on the field, then paper, paper, night. */
    if ((lead = live_pick_lead(all, now))) {
        page_emit_profile_for(db, "live|lead", BF_PAGE_MATERIAL_VIDEO, ".region-dress-live-lead", NULL, 0);
        live_emit_lead(db, lead, now); live_mark_seen(lead); shown++;
    }
    shown += live_emit_shows(db, all, now);
    preview = ctx->preview; ctx->preview = 1;
    if (live_pick(&all->stream[BF_PAGE_MATERIAL_EVENT], picks, 8, now, 7 * 86400, 0, 4, 0))
        shown += live_emit_block(db, "soon", "This week", "/whats-on", "live-week", BF_PAGE_MATERIAL_EVENT, picks, ctx);
    if (live_pick(&all->stream[BF_PAGE_MATERIAL_VIDEO], picks, 8, now, 0, 1, 2, LIVE_FRESH_SHOW) >= 2)
        shown += live_emit_block(db, "watch", "Watch", "/watch", "live-watch", BF_PAGE_MATERIAL_VIDEO, picks, ctx);
    if (live_pick(&all->stream[BF_PAGE_MATERIAL_AUDIO], picks, 6, now, 0, 0, 2, 60 * 86400) >= 2)
        shown += live_emit_block(db, "listen", "Put this on", "/listen", "live-listen", BF_PAGE_MATERIAL_AUDIO, picks, ctx);
    ctx->preview = preview;

    /* What this front showed is not shown again by the regions under it. */
    if (material)
        for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) {
            PageStream *s = &material->stream[kind]; int kept = s->cursor;
            for (k = s->cursor; k < s->count; k++) if (!live_was_seen(&s->item[k])) s->item[kept++] = s->item[k];
            s->count = kept;
        }
    free(picks); free(all);
    return shown;
}
