/* A territory's front, edited.
 *
 * One composer for the four apex fronts and their /discover and /today
 * routes.  Work reads as a daily dispatch (the editorial discover hub): an
 * edition strip, the headline with its beats as chips, one lead story, a grid
 * of dispatches to read or hear, the standard this front keeps, and then the
 * page ends.  Live, life and shop read as a discover page: the same opening,
 * then what there is to watch, to hear, to read, to go to and to look at, each
 * only when its sources published something.
 *
 * Every item is one its beat's own sources published, newest first; nothing
 * is shown twice; anything to hear is handed to the shared player in
 * BF_ESTATE_FOOT (data-ak-track), never to a player of its own.
 *
 * Included into main.c after the page grammars it composes from. */
#ifndef BONFYRE_FRONT_EDITORIAL_H
#define BONFYRE_FRONT_EDITORIAL_H
#include "front_community.h"

#define FRONT_BEATS 10
#define FRONT_PER_KIND 14
#define FRONT_POOL_MAX (FRONT_BEATS * 6 * FRONT_PER_KIND)
#define FRONT_DAY 86400

typedef struct { PageItem item; int beat; int used; } FrontPick;
typedef struct {
    FrontPick *pick; int count;
    const BfPageProgram *beat[FRONT_BEATS]; int beats;
    char sources[64][96]; int source_count;
    time_t now, newest;
} FrontPool;

static int front_is_apex(const PageDocument *doc, const char **territory_out) {
    const char *territory = page_render_host ? territory_of(page_render_host) : NULL;
    char apex[32];
    if (!doc || !doc->route || !territory) return 0;
    if (strcmp(doc->route, "/") && strcmp(doc->route, "/discover") && strcmp(doc->route, "/today")) return 0;
    /* A publisher or person's host keeps its own material and composition. */
    if (snprintf(apex, sizeof(apex), "aurekai.%s", territory) >= (int)sizeof(apex) ||
        strcmp(page_render_host, apex)) return 0;
    if (territory_out) *territory_out = territory;
    return 1;
}

/* A beat's material, read exactly as its own section reads it: the subject
 * projection when one is compiled, otherwise the program's sources. */
static void front_beat_material(sqlite3 *db, PageProjectionRead *projection, const BfPageProgram *program,
                                PageMaterial *material) {
    PageProjectionRead local = {0}; PageDocument cut = {0}; char sources[4096];
    if (!projection || !projection->db) { page_projection_read_open(&local, db, page_render_host); projection = &local; }
    if (page_subject_projection_material(projection, program->route, material) < 0) {
        if (page_subject_fabric_sources(projection->db, projection->subject, program, 8, sources, sizeof(sources)) < 0)
            page_program_sources(db, program, 8, sources, sizeof(sources));
        page_collect_stories(db, sources, material);
    }
    cut.route = program->route; cut.program = program;
    page_filter_route_contract(db, &cut, material);
    {
        size_t k; int held = 0;
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) held += material->stream[k].count - material->stream[k].cursor;
        if (!held && page_program_sources(db, program, 8, sources, sizeof(sources))) {
            memset(material, 0, sizeof(*material));
            page_collect_stories(db, sources, material);
            page_filter_route_contract(db, &cut, material);
        }
    }
    page_projection_read_close(&local);
}

static int front_same(const PageItem *a, const PageItem *b) {
    if (a->source[0] && a->key[0] && b->source[0] && b->key[0])
        return !strcmp(a->source, b->source) && !strcmp(a->key, b->key);
    if (a->href[0] && b->href[0]) return !strcmp(a->href, b->href);
    return !strcmp(a->source, b->source) && !strcmp(a->title, b->title);
}

static int front_current(const PageItem *item, time_t now) {
    if (!item->title[0] || item->at <= 0) return 0;
    if (item->kind == BF_PAGE_MATERIAL_EVENT) {
        time_t ends = item->duration_ms > 0 ? item->at + (time_t)(item->duration_ms / 1000) : item->at + FRONT_DAY;
        return ends >= now;
    }
    return item->at <= now;
}

/* Earlier-is-first for what is coming, newest-first for what was published. */
static int front_before(const PageItem *a, const PageItem *b) {
    if (a->kind == BF_PAGE_MATERIAL_EVENT && b->kind == BF_PAGE_MATERIAL_EVENT) return a->at < b->at;
    return a->at > b->at;
}

static void front_note_source(FrontPool *pool, const PageItem *item) {
    int i;
    if (!item->source[0]) return;
    for (i = 0; i < pool->source_count; i++) if (!strcmp(pool->sources[i], item->source)) return;
    if (pool->source_count < 64) snprintf(pool->sources[pool->source_count++], sizeof(pool->sources[0]), "%s", item->source);
}

/* The freshest current items of each kind a beat holds go into the pool. */
/* The shows: every published feed of episodes, video or audio, filed under
 * one of this front's families or as a creator's own channel. */
static void front_collect_shows(sqlite3 *db, const BfPageProgram *front, PageMaterial *into) {
    sqlite3_stmt *st = NULL; char sources[4096] = ""; size_t used = 0, k;
    if (!db || !front || !into || !cms_type_registered(db, "internet_source")) return;
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
    if (used) page_collect_stories(db, sources, into);
}

static void front_pool_take(FrontPool *pool, PageMaterial *material, int beat) {
    static const BfPageMaterial kinds[] = {BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_POST, BF_PAGE_MATERIAL_AUDIO,
                                           BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_EVENT, BF_PAGE_MATERIAL_PHOTO};
    size_t k; int i, j, n;
    for (k = 0; k < sizeof(kinds) / sizeof(kinds[0]); k++) {
        PageStream *stream = &material->stream[kinds[k]];
        PageItem *best[FRONT_PER_KIND];
        n = 0;
        for (i = stream->cursor; i < stream->count; i++) {
            PageItem *item = &stream->item[i];
            item->kind = (int)kinds[k];
            if (!front_current(item, pool->now)) continue;
            for (j = 0; j < n && !front_same(best[j], item); j++) {}
            if (j < n) continue;
            for (j = 0; j < n && !front_before(item, best[j]); j++) {}
            if (j >= FRONT_PER_KIND) continue;
            if (n < FRONT_PER_KIND) n++;
            for (int m = n - 1; m > j; m--) best[m] = best[m - 1];
            best[j] = item;
        }
        for (i = 0; i < n && pool->count < FRONT_POOL_MAX; i++) {
            for (j = 0; j < pool->count && !front_same(&pool->pick[j].item, best[i]); j++) {}
            if (j < pool->count) continue;
            pool->pick[pool->count].item = *best[i];
            pool->pick[pool->count].beat = beat;
            pool->pick[pool->count].used = 0;
            pool->count++;
            front_note_source(pool, best[i]);
            if (kinds[k] != BF_PAGE_MATERIAL_EVENT && best[i]->at > pool->newest) pool->newest = best[i]->at;
        }
    }
}

/* Choose up to cap unused picks of the kinds in mask.  max_age bounds how old
 * a published item may be (0: no bound); pictured puts items with a picture
 * first among those of the last week. */
static int front_choose(FrontPool *pool, unsigned mask, time_t max_age, int pictured, FrontPick **out, int cap) {
    int n = 0, i, j;
    for (i = 0; i < pool->count; i++) {
        FrontPick *p = &pool->pick[i];
        if (p->used || !(mask & (1u << p->item.kind))) continue;
        if (max_age && p->item.kind != BF_PAGE_MATERIAL_EVENT && p->item.at < pool->now - max_age) continue;
        for (j = 0; j < n; j++) {
            const PageItem *a = &p->item, *b = &out[j]->item;
            if (pictured) {
                int af = a->at >= pool->now - 7 * FRONT_DAY, bf = b->at >= pool->now - 7 * FRONT_DAY;
                int ap = af && page_image_url_safe(a->image), bp = bf && page_image_url_safe(b->image);
                if (ap != bp) { if (ap) break; continue; }
            }
            if (front_before(a, b)) break;
        }
        if (j >= cap) continue;
        if (n < cap) n++;
        for (int m = n - 1; m > j; m--) out[m] = out[m - 1];
        out[j] = p;
    }
    return n;
}

static void front_use(FrontPick *p) { p->used = 1; page_front_use(&p->item); }

#define FRONT_KIND(k) (1u << (k))
#define FRONT_STORIES (FRONT_KIND(BF_PAGE_MATERIAL_PUBLICATION) | FRONT_KIND(BF_PAGE_MATERIAL_POST) | \
                       FRONT_KIND(BF_PAGE_MATERIAL_AUDIO) | FRONT_KIND(BF_PAGE_MATERIAL_VIDEO))

static const char *front_beat_title(const FrontPool *pool, const FrontPick *p) {
    return p->beat >= 0 && p->beat < pool->beats && pool->beat[p->beat] ? pool->beat[p->beat]->title : "";
}

static void front_time(time_t at, time_t now) {
    char when[48], iso[32]; struct tm tm;
    page_when(at, now, when, sizeof(when));
    if (!when[0] || !gmtime_r(&at, &tm) || !strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tm)) return;
    fputs("<time datetime=\"", stdout); html(iso); fputs("\">", stdout); html(when); fputs("</time>", stdout);
}

static void front_minutes(long ms) {
    long minutes = (ms + 59999) / 60000;
    if (minutes > 0) printf(" &middot; %ld min", minutes);
}

static sqlite3 *front_db;
/* A show is named by what its feed calls itself ("SmartLess"), not by the
 * host that serves the episode; other items keep their publisher note. */
static const char *front_publisher(const PageItem *item) {
    static char shows[4][192]; static int turn;
    if ((item->kind == BF_PAGE_MATERIAL_AUDIO || item->kind == BF_PAGE_MATERIAL_VIDEO) && item->source[0]) {
        char *out = shows[turn++ & 3]; sqlite3_stmt *st = NULL;
        snprintf(out, sizeof(shows[0]), "%s", item->group[0] ? item->group : "");
        if (!out[0] && front_db && sqlite3_prepare_v2(front_db, "SELECT label FROM internet_source WHERE source=?1 AND status='published'"
                " AND COALESCE(label,'')<>'' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, item->source, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(out, sizeof(shows[0]), "%s", (const char *)sqlite3_column_text(st, 0));
            sqlite3_finalize(st);
        }
        if (out[0]) return out;
    }
    return item->note[0] ? item->note : item->by;
}

/* The control that hands an item to the shared player: its recording when it
 * has one, otherwise its own words read aloud. */
static void front_listen(const PageItem *item, const char *text_id, int is_default, const char *klass) {
    int recording = item->kind == BF_PAGE_MATERIAL_AUDIO && item->asset[0] && public_http_url(item->asset);
    fputs("<button type=\"button\" class=\"", stdout); html(klass); fputs("\" data-ak-track data-title=\"", stdout); html(item->title);
    fputs("\" data-by=\"", stdout); html(front_publisher(item));
    if (recording) { fputs("\" data-src=\"", stdout); html(item->asset); }
    else { fputs("\" data-narrate=\"#", stdout); html(text_id); }
    fputs("\"", stdout);
    if (is_default) fputs(" data-ak-default", stdout);
    fputs(">", stdout); page_glyph(recording ? "listen" : "narration");
    fputs(recording ? "<span>Listen" : "<span>Read aloud", stdout);
    if (recording) front_minutes(item->duration_ms);
    fputs("</span></button>", stdout);
}

static const char *front_open_label(const PageItem *item) {
    switch (item->kind) {
        case BF_PAGE_MATERIAL_AUDIO: return "Open episode";
        case BF_PAGE_MATERIAL_VIDEO: return "Watch";
        case BF_PAGE_MATERIAL_EVENT: return "Details";
        default: return "Read";
    }
}

static void front_open(const PageItem *item, const char *label) {
    const char *close = page_emit_item_open(item, "fd-open");
    html(label ? label : front_open_label(item)); fputs(" <span aria-hidden=\"true\">&rsaquo;</span>", stdout); fputs(close, stdout);
}

static void front_strip(const FrontPool *pool, int shown) {
    char day[64] = ""; struct tm tm;
    if (page_local_time(pool->now, &tm)) strftime(day, sizeof(day), "%A, %B %e", &tm);
    fputs("<div class=\"fd-strip\" role=\"note\"><p><span class=\"fd-dot\" aria-hidden=\"true\"></span>Today&#039;s edition", stdout);
    if (day[0]) { fputs(" &middot; ", stdout); html(day); }
    fputs("</p><p class=\"fd-strip-facts\">", stdout);
    printf("<span>%d sources read</span>", pool->source_count);
    if (pool->newest) { fputs("<span>Updated ", stdout); front_time(pool->newest, pool->now); fputs("</span>", stdout); }
    printf("<span class=\"fd-strip-count\">%d items today</span></p></div>", shown);
}

static void front_head(const PageDocument *doc, const FrontPool *pool, const char *kinds_present[][2], int kinds_n) {
    int i;
    fputs("<header class=\"fd-head\"><div class=\"fd-head-words\"><h1>", stdout); html(doc->title); fputs("</h1>", stdout);
    if (doc->lede && *doc->lede) { fputs("<p class=\"fd-lede\">", stdout); html(doc->lede); fputs("</p>", stdout); }
    fputs("</div><form class=\"fd-find\" action=\"/search\" method=\"get\" role=\"search\"><label>", stdout);
    page_glyph("search");
    fputs("<span class=\"fd-sr\">Search</span><input type=\"search\" name=\"q\" placeholder=\"Search stories, sources, makers\"></label></form>"
          "<nav class=\"fd-chips\" aria-label=\"Beats\"><a href=\"", stdout);
    html(doc->route); fputs("\" aria-current=\"page\">All</a>", stdout);
    for (i = 0; i < pool->beats; i++) {
        if (!pool->beat[i] || !strcmp(pool->beat[i]->route, doc->route)) continue;
        fputs("<a href=\"", stdout); html(pool->beat[i]->route); fputs("\">", stdout); html(pool->beat[i]->title); fputs("</a>", stdout);
    }
    for (i = 0; i < kinds_n; i++) {
        fputs("<a href=\"#", stdout); html(kinds_present[i][0]); fputs("\">", stdout); html(kinds_present[i][1]); fputs("</a>", stdout);
    }
    fputs("</nav></header>", stdout);
}

static void front_lead(const FrontPool *pool, FrontPick *lead) {
    const PageItem *item = &lead->item; const char *close;
    int pictured = page_image_url_safe(item->image);
    fputs("<section class=\"fd-lead", stdout); if (!pictured) fputs(" fd-lead-plain", stdout);
    fputs("\" aria-label=\"Lead story\">", stdout);
    if (pictured) {
        close = page_emit_item_open(item, "fd-lead-picture");
        page_emit_picture(item->image, "fd-lead-img");
        fputs(close, stdout);
    }
    fputs("<div class=\"fd-lead-words\"><p class=\"fd-kicker\"><span>", stdout);
    html(front_beat_title(pool, lead)[0] ? front_beat_title(pool, lead) : "Lead story");
    fputs("</span><span>", stdout); front_time(item->at, pool->now); fputs("</span></p>", stdout);
    fputs("<div id=\"fd-lead-text\"><h2>", stdout);
    close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
    fputs("</h2>", stdout);
    if (item->body[0] && strcmp(item->body, item->title)) { fputs("<p class=\"fd-dek\">", stdout); html(item->body); fputs("</p>", stdout); }
    fputs("</div><p class=\"fd-by\">", stdout);
    if (item->by[0]) { html(item->by); }
    if (item->note[0] && strcmp(item->note, item->by)) { fputs(" &middot; ", stdout); html(item->note); }
    fputs("</p><div class=\"fd-lead-actions\">", stdout);
    if (public_http_url(item->href)) {
        fputs("<a class=\"fd-primary\" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(item->href); fputs("\">Open on ", stdout);
        html(front_publisher(item)[0] ? front_publisher(item) : "the source"); fputs(" <span aria-hidden=\"true\">&nearr;</span></a>", stdout);
    }
    front_listen(item, "fd-lead-text", 1, "fd-ghost");
    if (page_item_has_page(item)) front_open(item, "Read here");
    fputs("</div></div></section>", stdout);
}

/* One dispatch: its beat and publisher, the byline, the headline and its
 * dek, then the way to hear it and the way to read it. */
static void front_card(const FrontPool *pool, FrontPick *p, int n) {
    const PageItem *item = &p->item; char id[24]; const char *close;
    snprintf(id, sizeof(id), "fd-t%d", n);
    fputs("<article class=\"fd-card\"><p class=\"fd-card-top\"><span class=\"fd-beat\">", stdout);
    html(front_beat_title(pool, p)); fputs("</span><span class=\"fd-pub\">", stdout); html(front_publisher(item)); fputs("</span></p>", stdout);
    if (item->by[0] && strcmp(item->by, front_publisher(item))) { fputs("<p class=\"fd-card-by\">", stdout); html(item->by); fputs("</p>", stdout); }
    fputs("<div class=\"fd-card-text\" id=\"", stdout); html(id); fputs("\"><h3>", stdout);
    close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout); fputs("</h3>", stdout);
    if (item->body[0] && strcmp(item->body, item->title)) { fputs("<p class=\"fd-dek\">", stdout); html(item->body); fputs("</p>", stdout); }
    fputs("</div><footer>", stdout);
    front_listen(item, id, 0, "fd-act");
    fputs("<span class=\"fd-when\">", stdout); front_time(item->at, pool->now); fputs("</span>", stdout);
    front_open(item, NULL);
    fputs("</footer></article>", stdout);
}

static int front_dispatches(FrontPool *pool, const char *heading, int cap) {
    FrontPick *picks[12]; int n, i;
    if (cap > 12) cap = 12;
    n = front_choose(pool, FRONT_STORIES, 0, 0, picks, cap);
    if (!n) return 0;
    fputs("<section class=\"fd-dispatches\" aria-labelledby=\"fd-dispatch-h\"><div class=\"fd-section-head\"><h2 id=\"fd-dispatch-h\"><span class=\"fd-dot\" aria-hidden=\"true\"></span>", stdout);
    html(heading); fputs("</h2><p>Newest first</p></div><div class=\"fd-grid\">", stdout);
    for (i = 0; i < n; i++) { front_card(pool, picks[i], i); front_use(picks[i]); }
    fputs("</div></section>", stdout);
    return n;
}

static int front_watch(FrontPool *pool) {
    FrontPick *picks[8]; int n, i;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_VIDEO), 45 * FRONT_DAY, 1, picks, 4);
    if (!n) return 0;
    fputs("<section class=\"fd-watch\" id=\"fd-watch\" aria-labelledby=\"fd-watch-h\"><div class=\"fd-section-head\"><h2 id=\"fd-watch-h\">Watch</h2><a href=\"/watch\">All video</a></div><div class=\"fd-shelf\">", stdout);
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; const char *close = page_emit_item_open(item, "fd-film");
        fputs("<span class=\"fd-frame\">", stdout);
        if (page_image_url_safe(item->image)) page_emit_picture(item->image, "fd-film-img");
        else if (page_image_url_safe(item->poster)) page_emit_picture(item->poster, "fd-film-img");
        fputs("<span class=\"fd-play\" aria-hidden=\"true\">", stdout); page_glyph("play"); fputs("</span>", stdout);
        if (item->duration_ms > 0) { fputs("<span class=\"fd-dur\">", stdout); page_emit_duration(item->duration_ms); fputs("</span>", stdout); }
        fputs("</span><span class=\"fd-film-title\">", stdout); html(item->title); fputs("</span><span class=\"fd-film-by\">", stdout);
        html(front_publisher(item)); fputs("</span>", stdout); fputs(close, stdout);
        front_use(picks[i]);
    }
    fputs("</div></section>", stdout);
    return n;
}

static int front_hear(FrontPool *pool) {
    FrontPick *found[24], *picks[8]; int n = 0, got, i, j;
    /* One episode per show: the newest, and none from a show already leading. */
    got = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_AUDIO), 30 * FRONT_DAY, 0, found, 24);
    for (i = 0; i < got && n < 6; i++) {
        int seen = 0;
        for (j = 0; j < n && !seen; j++) seen = !strcmp(picks[j]->item.source, found[i]->item.source);
        for (j = 0; j < pool->count && !seen; j++) seen = pool->pick[j].used && !strcmp(pool->pick[j].item.source, found[i]->item.source);
        if (!seen) picks[n++] = found[i];
    }
    if (!n) return 0;
    fputs("<section class=\"fd-hear\" id=\"fd-listen\" aria-labelledby=\"fd-listen-h\"><div class=\"fd-section-head\"><h2 id=\"fd-listen-h\">Listen</h2>", stdout);
    printf("<p>%d new episodes</p></div><ol class=\"fd-tracks\">", n);
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; char id[24]; const char *close;
        snprintf(id, sizeof(id), "fd-h%d", i);
        fputs("<li data-source=\"", stdout); html(item->source); fputs("\">", stdout);
        if (page_image_url_safe(item->image)) page_emit_picture(item->image, "fd-cover");
        else fputs("<span class=\"fd-cover\" aria-hidden=\"true\"></span>", stdout);
        fputs("<div class=\"fd-track-words\" id=\"", stdout); html(id); fputs("\"><h3>", stdout);
        close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
        fputs("</h3><p class=\"fd-show\">", stdout); html(front_publisher(item)); fputs("</p></div>", stdout);
        fputs("<span class=\"fd-when\">", stdout); front_time(item->at, pool->now); fputs("</span>", stdout);
        front_listen(item, id, 0, "fd-act");
        fputs("</li>", stdout);
        front_use(picks[i]);
    }
    fputs("</ol></section>", stdout);
    return n;
}

static int front_reading(FrontPool *pool, const char *heading, int cap) {
    FrontPick *picks[12]; int n, i;
    if (cap > 12) cap = 12;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_PUBLICATION) | FRONT_KIND(BF_PAGE_MATERIAL_POST), 0, 1, picks, cap);
    if (!n) return 0;
    fputs("<section class=\"fd-reading\" id=\"fd-read\" aria-labelledby=\"fd-read-h\"><div class=\"fd-section-head\"><h2 id=\"fd-read-h\">", stdout);
    html(heading); fputs("</h2><p>Newest first</p></div><div class=\"fd-columns\">", stdout);
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; char id[24]; const char *close;
        snprintf(id, sizeof(id), "fd-r%d", i);
        fputs("<article class=\"fd-read\">", stdout);
        if (page_image_url_safe(item->image)) { close = page_emit_item_open(item, "fd-read-picture"); page_emit_picture(item->image, "fd-read-img"); fputs(close, stdout); }
        fputs("<p class=\"fd-card-top\"><span class=\"fd-beat\">", stdout); html(front_publisher(item));
        fputs("</span><span>", stdout); front_time(item->at, pool->now); fputs("</span></p><div class=\"fd-card-text\" id=\"", stdout); html(id); fputs("\"><h3>", stdout);
        close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout); fputs("</h3>", stdout);
        if (item->body[0] && strcmp(item->body, item->title)) { fputs("<p class=\"fd-dek\">", stdout); html(item->body); fputs("</p>", stdout); }
        fputs("</div><footer>", stdout); front_listen(item, id, 0, "fd-act"); front_open(item, NULL); fputs("</footer></article>", stdout);
        front_use(picks[i]);
    }
    fputs("</div></section>", stdout);
    return n;
}

static int front_going(FrontPool *pool) {
    FrontPick *picks[8]; int n, i;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_EVENT), 0, 0, picks, 6);
    if (!n) return 0;
    fputs("<section class=\"fd-going\" id=\"fd-on\" aria-labelledby=\"fd-on-h\"><div class=\"fd-section-head\"><h2 id=\"fd-on-h\">What&#039;s on</h2><a href=\"/whats-on\">Full calendar</a></div><ol class=\"fd-agenda\">", stdout);
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; struct tm tm; char date[24] = "", weekday[16] = "", ahead[48] = ""; const char *close;
        if (page_local_time(item->at, &tm)) { strftime(date, sizeof(date), "%e %b", &tm); strftime(weekday, sizeof(weekday), "%A", &tm); }
        page_when_ahead(item->at, item->duration_ms > 0 ? item->at + (time_t)(item->duration_ms / 1000) : 0, pool->now, ahead, sizeof(ahead));
        fputs("<li><p class=\"fd-date\"><b>", stdout); html(date); fputs("</b><span>", stdout); html(weekday); fputs("</span></p><div><h3>", stdout);
        close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
        fputs("</h3><p>", stdout); html(item->where[0] ? item->where : front_publisher(item)); fputs("</p></div><p class=\"fd-ahead\">", stdout);
        html(ahead); fputs("</p></li>", stdout);
        front_use(picks[i]);
    }
    fputs("</ol></section>", stdout);
    return n;
}

static int front_pictured(FrontPool *pool, const char *heading) {
    FrontPick *picks[10]; int n, i, kept = 0;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_PHOTO), 0, 1, picks, 8);
    for (i = 0; i < n; i++) if (page_image_url_safe(picks[i]->item.image)) kept++;
    if (!kept) return 0;
    fputs("<section class=\"fd-pictured\" id=\"fd-pictured\" aria-labelledby=\"fd-pic-h\"><div class=\"fd-section-head\"><h2 id=\"fd-pic-h\">", stdout);
    html(heading); fputs("</h2></div><div class=\"fd-covers\">", stdout);
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; const char *close;
        if (!page_image_url_safe(item->image)) continue;
        close = page_emit_item_open(item, "fd-cover-item");
        page_emit_picture(item->image, "fd-cover-img");
        fputs("<span class=\"fd-film-title\">", stdout); html(item->title); fputs("</span><span class=\"fd-film-by\">", stdout);
        html(front_publisher(item)); fputs("</span>", stdout); fputs(close, stdout);
        front_use(picks[i]);
    }
    fputs("</div></section>", stdout);
    return kept;
}

static void front_standard(void) {
    fputs("<section class=\"fd-standard\" aria-labelledby=\"fd-standard-h\"><p class=\"fd-kicker\"><span>The Aurekai standard</span></p>"
          "<h2 id=\"fd-standard-h\">A front page that ends.</h2>"
          "<p>Everything here was published by the source named on it, and every item links back there. Nothing is ranked to keep you scrolling: "
          "stories run newest first, and when today&#039;s run out, so does the page.</p>"
          "<p class=\"fd-standard-ways\"><a href=\"/channels\">See the sources</a><a href=\"/people\">See the people behind them</a></p></section>", stdout);
}

/* The count of picks the composer would show, for the edition strip: the
 * strip is drawn first, so the count is the regions' caps against the pool. */
static int front_available(FrontPool *pool, unsigned mask, time_t max_age, int cap) {
    FrontPick *picks[12]; int n = front_choose(pool, mask, max_age, 0, picks, cap > 12 ? 12 : cap);
    return n;
}

static int page_front_editorial(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    const char *territory = NULL; FrontPool pool; int shown = 0, i, hub;
    FrontPick *lead[1];
    if (!front_is_apex(doc, &territory)) return 0;
    front_db = db;
    memset(&pool, 0, sizeof(pool));
    pool.now = ctx ? ctx->now : time(NULL);
    if (!(pool.pick = calloc(FRONT_POOL_MAX, sizeof(*pool.pick)))) return 0;
    hub = !strcmp(territory, "work");
    /* The beats are the program's sections; a route without sections is its
     * own single beat over the material the page already read. */
    if (doc->program && doc->program->section_count && doc->program->sections[0]) {
        for (i = 0; i < (int)doc->program->section_count && pool.beats < FRONT_BEATS; i++) {
            const BfPageProgram *program = doc->program->sections[i] ? page_program_for_route(doc->program->sections[i]) : NULL;
            PageMaterial *beat;
            if (!program || !(beat = calloc(1, sizeof(*beat)))) continue;
            front_beat_material(db, projection, program, beat);
            pool.beat[pool.beats] = program;
            front_pool_take(&pool, beat, pool.beats);
            pool.beats++;
            page_front_take_route(program->route);
            free(beat);
        }
        /* The front's own reading still counts: a source the territory admits
         * but no beat names would otherwise never reach its front. The pool
         * keeps one copy of an item a beat already took. */
        if (material && doc->program && pool.beats < FRONT_BEATS) {
            pool.beat[pool.beats] = doc->program;
            front_pool_take(&pool, material, pool.beats);
            pool.beats++;
        }
        /* Live also carries the shows themselves: every published feed of
         * episodes filed under one of its families or as a creator's channel. */
        if (!strcmp(territory, "live") && doc->program && pool.beats < FRONT_BEATS) {
            PageMaterial *shows = calloc(1, sizeof(*shows));
            if (shows) {
                front_collect_shows(db, doc->program, shows);
                pool.beat[pool.beats] = doc->program;
                front_pool_take(&pool, shows, pool.beats);
                pool.beats++;
                free(shows);
            }
        }
    } else if (material && doc->program) {
        pool.beat[0] = doc->program; pool.beats = 1;
        front_pool_take(&pool, material, 0);
    }
    {
        const char *kinds[6][2]; int kinds_n = 0, today;
        if (!pool.beats || (doc->program && !doc->program->section_count)) {
            if (!hub && front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_VIDEO), 45 * FRONT_DAY, 1)) { kinds[kinds_n][0] = "fd-watch"; kinds[kinds_n++][1] = "Watch"; }
            if (!hub && front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_AUDIO), 30 * FRONT_DAY, 1)) { kinds[kinds_n][0] = "fd-listen"; kinds[kinds_n++][1] = "Listen"; }
            if (!hub && front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_EVENT), 0, 1)) { kinds[kinds_n][0] = "fd-on"; kinds[kinds_n++][1] = "What's on"; }
        }
        today = hub ? front_available(&pool, FRONT_STORIES, 0, 10)
                    : front_available(&pool, FRONT_STORIES, 0, 1) + front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_VIDEO), 45 * FRONT_DAY, 4) +
                      front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_AUDIO), 30 * FRONT_DAY, 6) +
                      front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_PUBLICATION) | FRONT_KIND(BF_PAGE_MATERIAL_POST), 0, 6) +
                      front_available(&pool, FRONT_KIND(BF_PAGE_MATERIAL_EVENT), 0, 6);
        fputs("<div class=\"fd ", stdout); fputs(hub ? "fd-hub" : "fd-discover", stdout); fputs("\" data-front=\"", stdout); html(territory); fputs("\">", stdout);
        front_strip(&pool, today);
        front_head(doc, &pool, kinds, kinds_n);
    }
    if (front_choose(&pool, FRONT_STORIES, 0, 1, lead, 1)) { front_lead(&pool, lead[0]); front_use(lead[0]); shown++; }
    if (hub) {
        shown += front_dispatches(&pool, "Dispatches and recordings", 9);
    } else {
        shown += front_watch(&pool);
        shown += front_hear(&pool);
        shown += front_reading(&pool, "Reading", 6);
        shown += front_going(&pool);
        shown += front_pictured(&pool, !strcmp(territory, "shop") ? "In the shops" : "Pictured");
    }
    if (!shown)
        fputs("<p class=\"fd-empty\">Nothing new from this front&#039;s sources yet. The sources are read through the day; "
              "<a href=\"/channels\">see which ones</a>.</p>", stdout);
    front_standard();
    fputs("</div>", stdout);
    free(pool.pick);
    return shown ? shown : 1;
}

#endif
