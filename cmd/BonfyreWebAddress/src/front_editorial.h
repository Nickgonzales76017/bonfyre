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
#include "front_brand.h"

/* Set by the page when an owned apex root has already opened on its brand
 * masthead H1; the front's own head then titles itself as an H2. */
static int page_front_brand_opened = 0;

#define FRONT_BEATS 13
/* At most this many picks from one source in any one run of the front;
 * the rest of a run goes to other publishers before a source repeats. */
#define FRONT_PER_SOURCE 2
#define FRONT_PER_KIND 14
#define FRONT_POOL_MAX (FRONT_BEATS * 6 * FRONT_PER_KIND)
#define FRONT_DAY 86400
/* Sources one beat reads into the front's pool. The route lengths still walk
 * program->source_limit at a time; the front is a mosaic over all its beats,
 * and eight sources a beat collapsed every region onto the same ~10 weakest
 * publishers. Thirty-two fills the regions' ~25 places at FRONT_PER_SOURCE. */
#define FRONT_BEAT_SOURCES 32

typedef struct { PageItem item; int beat; int used; } FrontPick;
typedef struct {
    FrontPick *pick; int count;
    const BfPageProgram *beat[FRONT_BEATS]; int beats;
    char sources[64][96]; int source_count;
    time_t now, newest;
    const char *back;
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
        if (page_subject_fabric_sources(projection->db, projection->subject, program, FRONT_BEAT_SOURCES, sources, sizeof(sources)) < 0)
            page_program_sources(db, program, FRONT_BEAT_SOURCES, sources, sizeof(sources));
        page_collect_stories(db, sources, material);
    }
    cut.route = program->route; cut.program = program;
    page_filter_route_contract(db, &cut, material);
    {
        size_t k; int held = 0;
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) held += material->stream[k].count - material->stream[k].cursor;
        if (!held && page_program_sources(db, program, FRONT_BEAT_SOURCES, sources, sizeof(sources))) {
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
        PageItem *best[FRONT_PER_KIND], **ranked; unsigned char *taken; int total = 0, distinct = 0, share, pass;
        n = 0;
        if (stream->count <= stream->cursor) continue;
        ranked = calloc((size_t)(stream->count - stream->cursor), sizeof(*ranked));
        taken = calloc((size_t)(stream->count - stream->cursor), 1);
        if (!ranked || !taken) { free(ranked); free(taken); continue; }
        /* Every current item, newest first, once. */
        for (i = stream->cursor; i < stream->count; i++) {
            PageItem *item = &stream->item[i];
            item->kind = (int)kinds[k];
            if (!front_current(item, pool->now)) continue;
            for (j = 0; j < total && !front_same(ranked[j], item); j++) {}
            if (j < total) continue;
            for (j = 0; j < total && !front_before(item, ranked[j]); j++) {}
            for (int m = total; m > j; m--) ranked[m] = ranked[m - 1];
            ranked[j] = item; total++;
        }
        /* A beat's places are shared among its sources: each takes an even
         * share of them, newest first, before any source takes another. The
         * more sources a beat reads, the more of them reach the front. */
        for (i = 0; i < total; i++) { for (j = 0; j < i && strcmp(ranked[j]->source, ranked[i]->source); j++) {} if (j == i) distinct++; }
        share = distinct ? (FRONT_PER_KIND + distinct - 1) / distinct : FRONT_PER_KIND;
        for (pass = 0; pass < 2 && n < FRONT_PER_KIND; pass++)
            for (i = 0; i < total && n < FRONT_PER_KIND; i++) {
                int same = 0;
                if (taken[i]) continue;
                for (j = 0; j < n; j++) same += !strcmp(best[j]->source, ranked[i]->source);
                if (pass == 0 && same >= share) continue;
                taken[i] = 1; best[n++] = ranked[i];
            }
        free(ranked); free(taken);
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
static int front_rank(FrontPool *pool, unsigned mask, time_t max_age, int pictured, FrontPick **out, int cap) {
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

/* Choose up to cap picks in rank order, no source taking more than
 * FRONT_PER_SOURCE of them while another publisher still has one to offer.
 * A front that only one source can fill still fills: the second pass hands
 * the remaining places back in rank order. */
#define FRONT_RANK_MAX 160
static int front_choose(FrontPool *pool, unsigned mask, time_t max_age, int pictured, FrontPick **out, int cap) {
    FrontPick *ranked[FRONT_RANK_MAX]; unsigned char taken[FRONT_RANK_MAX] = {0};
    int n = 0, total, i, j, pass;
    if (cap <= 0) return 0;
    total = front_rank(pool, mask, max_age, pictured, ranked, FRONT_RANK_MAX);
    for (pass = 0; pass < 2 && n < cap; pass++)
        for (i = 0; i < total && n < cap; i++) {
            int same = 0;
            if (taken[i]) continue;
            if (pass == 0 && ranked[i]->item.source[0]) {
                for (j = 0; j < n; j++) same += !strcmp(out[j]->item.source, ranked[i]->item.source);
                if (same >= FRONT_PER_SOURCE) continue;
            }
            taken[i] = 1; out[n++] = ranked[i];
        }
    /* Places keep rank order on the page: newest (or pictured) first. */
    for (i = 1; i < n; i++) {
        FrontPick *p = out[i]; int a = 0;
        while (a < total && ranked[a] != p) a++;
        for (j = i; j > 0; j--) {
            int b = 0; while (b < total && ranked[b] != out[j - 1]) b++;
            if (b <= a) break;
            out[j] = out[j - 1];
        }
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

static void front_time(time_t at, time_t now);
/* A pick's date, unless its beat reads only material that does not date. */
static void front_pick_time(const FrontPool *pool, const FrontPick *p) {
    if (p->beat >= 0 && p->beat < pool->beats && page_program_timeless(pool->beat[p->beat])) return;
    front_time(p->item.at, pool->now);
}
static void front_time(time_t at, time_t now) {
    char when[48], iso[32]; struct tm tm;
    page_when(at, now, when, sizeof(when));
    if (!when[0] || !gmtime_r(&at, &tm) || !strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tm)) return;
    ui_open_a(stdout, "time", "datetime", iso); html(when); ui_close(stdout, "time");
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
    ui_start(stdout,"button");ui_attr(stdout,"type","button");ui_attr_open(stdout,"class");html(klass);ui_attr_close(stdout);ui_flag(stdout,"data-ak-track");ui_attr_open(stdout,"data-title");html(item->title);
    ui_lit(stdout, "\" data-by=\""); html(front_publisher(item));
    if (recording) { ui_lit(stdout, "\" data-src=\""); html(item->asset); }
    else { ui_lit(stdout, "\" data-narrate=\"#"); html(text_id); }
    ui_lit(stdout, "\"");
    if (is_default) ui_lit(stdout, " data-ak-default");
    ui_lit(stdout, ">"); page_glyph(recording ? "listen" : "narration");
    fputs(recording ? "<span>Listen" : "<span>Read aloud", stdout);
    if (recording) front_minutes(item->duration_ms);
    ui_close_n(stdout, "span", "button", NULL);
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
    html(label ? label : front_open_label(item)); (ui_lit(stdout, " "), ui_elem(stdout, "span", (const UiAttr[]){{"aria-hidden", "true"}}, 1, 0), ui_lit(stdout, "&rsaquo;"), ui_close(stdout, "span")); fputs(close, stdout);
}

static void front_strip(const FrontPool *pool, int shown) {
    char day[64] = ""; struct tm tm;
    if (page_local_time(pool->now, &tm)) strftime(day, sizeof(day), "%A, %B %e", &tm);
    (ui_elem(stdout, "div", (const UiAttr[]){{"class", "fd-strip"}, {"role", "note"}}, 2, 0), ui_open(stdout, "p", NULL), ui_elem(stdout, "span", (const UiAttr[]){{"class", "fd-dot"}, {"aria-hidden", "true"}}, 2, 0), ui_close(stdout, "span"), ui_lit(stdout, "Today&#039;s edition"));
    if (day[0]) { ui_lit(stdout, " &middot; "); html(day); }
    (ui_close(stdout, "p"), ui_open(stdout, "p", "fd-strip-facts"));
    ui_open(stdout,"span",NULL);printf("%d",pool->source_count);ui_lit(stdout," sources read</span>");
    if (pool->newest) { (ui_open(stdout, "span", NULL), ui_lit(stdout, "Updated ")); front_time(pool->newest, pool->now); ui_close(stdout, "span"); }
    ui_open(stdout,"span","fd-strip-count");printf("%d",shown);ui_lit(stdout," items today</span></p></div>");
}

static void front_head(const PageDocument *doc, const FrontPool *pool, const char *kinds_present[][2], int kinds_n) {
    int i;
    ui_open(stdout,"header","fd-head");ui_open(stdout,"div","fd-head-words");ui_open(stdout,page_front_brand_opened?"h2":"h1",NULL);
    html(doc->title); ui_close(stdout,page_front_brand_opened?"h2":"h1");
    if (doc->lede && *doc->lede) { ui_text(stdout, "p", "fd-lede", doc->lede); }
    (ui_close(stdout, "div"), ui_elem(stdout, "form", (const UiAttr[]){{"class", "fd-find"}, {"action", "/search"}, {"method", "get"}, {"role", "search"}}, 4, 0), ui_open(stdout, "label", NULL));
    page_glyph("search");
    ui_text(stdout,"span","fd-sr","Search");ui_elem(stdout,"input",(const UiAttr[]){{"type","search"},{"name","q"},{"placeholder","Search stories, sources, makers"}},3,0);ui_close(stdout,"label");ui_close(stdout,"form");
    ui_start(stdout,"nav");ui_attr(stdout,"class","fd-chips");ui_attr(stdout,"aria-label","Beats");ui_gt(stdout);ui_start(stdout,"a");ui_attr_open(stdout,"href");
    html(doc->route); (ui_attr_close(stdout),ui_lit(stdout, " aria-current=\"page\""),ui_gt(stdout),ui_lit(stdout,"All"), ui_close(stdout, "a"));
    for (i = 0; i < pool->beats; i++) {
        if (!pool->beat[i] || !strcmp(pool->beat[i]->route, doc->route)) continue;
        ui_link(stdout, pool->beat[i]->route, NULL, pool->beat[i]->title);
    }
    for (i = 0; i < kinds_n; i++) {
        ui_link_path(stdout, (const char *const[]){"#", kinds_present[i][0]}, 2, kinds_present[i][1]);
    }
    ui_close_n(stdout, "nav", "header", NULL);
}

static void front_lead(const FrontPool *pool, FrontPick *lead) {
    const PageItem *item = &lead->item; const char *close;
    int pictured = page_image_url_safe(item->image);
    ui_start(stdout,"section");ui_attr_open(stdout,"class");ui_lit(stdout,"fd-lead"); if (!pictured) ui_lit(stdout, " fd-lead-plain");
    ui_attr_close(stdout);ui_attr(stdout,"aria-label","Lead story");ui_gt(stdout);
    if (pictured) {
        close = page_emit_item_open(item, "fd-lead-picture");
        page_emit_picture(item->image, "fd-lead-img");
        fputs(close, stdout);
    }
    (ui_open(stdout, "div", "fd-lead-words"), ui_open(stdout, "p", "fd-kicker"), ui_open(stdout, "span", NULL));
    html(front_beat_title(pool, lead)[0] ? front_beat_title(pool, lead) : "Lead story");
    (ui_close(stdout, "span"), ui_open(stdout, "span", NULL)); front_pick_time(pool, lead); ui_close_n(stdout, "span", "p", NULL);
    (ui_elem(stdout, "div", (const UiAttr[]){{"id", "fd-lead-text"}}, 1, 0), ui_open(stdout, "h2", NULL));
    close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
    ui_close(stdout, "h2");
    if (item->body[0] && strcmp(item->body, item->title)) { ui_text(stdout, "p", "fd-dek", item->body); }
    (ui_close(stdout, "div"), ui_open(stdout, "p", "fd-by"));
    if (item->by[0]) { html(item->by); }
    if (item->note[0] && strcmp(item->note, item->by)) { ui_lit(stdout, " &middot; "); html(item->note); }
    (ui_close(stdout, "p"), ui_open(stdout, "div", "fd-lead-actions"));
    if (public_http_url(item->href)) {
        ui_start(stdout,"a");ui_attr(stdout,"class","fd-primary");ui_attr(stdout,"rel","noopener");ui_attr(stdout,"target","_blank");ui_attr_open(stdout,"href"); html(item->href); ui_attr_close(stdout);ui_gt(stdout);ui_lit(stdout,"Open on ");
        html(front_publisher(item)[0] ? front_publisher(item) : "the source"); (ui_lit(stdout, " "), ui_elem(stdout, "span", (const UiAttr[]){{"aria-hidden", "true"}}, 1, 0), ui_lit(stdout, "&nearr;"), ui_close(stdout, "span"), ui_close(stdout, "a"));
    }
    front_listen(item, "fd-lead-text", 1, "fd-ghost");
    if (page_item_has_page(item)) front_open(item, "Read here");
    ms_keep(item, pool->back, 0);
    ui_close_n(stdout, "div", "div", "section", NULL);
}

/* One dispatch: its beat and publisher, the byline, the headline and its
 * dek, then the way to hear it and the way to read it. */
static void front_card(const FrontPool *pool, FrontPick *p, int n) {
    const PageItem *item = &p->item; char id[24]; const char *close;
    snprintf(id, sizeof(id), "fd-t%d", n);
    (ui_open(stdout, "article", "fd-card"), ui_open(stdout, "p", "fd-card-top"), ui_open(stdout, "span", "fd-beat"));
    html(front_beat_title(pool, p)); (ui_close(stdout, "span"), ui_open(stdout, "span", "fd-pub")); html(front_publisher(item)); ui_close_n(stdout, "span", "p", NULL);
    if (item->by[0] && strcmp(item->by, front_publisher(item))) { ui_text(stdout, "p", "fd-card-by", item->by); }
    (ui_open_a(stdout, "div", "class", "fd-card-text", "id", id), ui_open(stdout, "h3", NULL));
    close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout); ui_close(stdout, "h3");
    if (item->body[0] && strcmp(item->body, item->title)) { ui_text(stdout, "p", "fd-dek", item->body); }
    (ui_close(stdout, "div"), ui_open(stdout, "footer", NULL));
    front_listen(item, id, 0, "fd-act");
    ui_open(stdout, "span", "fd-when"); front_pick_time(pool, p); ui_close(stdout, "span");
    front_open(item, NULL);
    ms_keep(item, pool->back, 1);
    ui_close_n(stdout, "footer", "article", NULL);
}

static int front_dispatches(FrontPool *pool, const char *heading, int cap) {
    FrontPick *picks[12]; int n, i;
    if (cap > 12) cap = 12;
    n = front_choose(pool, FRONT_STORIES, 0, 0, picks, cap);
    if (!n) return 0;
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "fd-dispatches"}, {"aria-labelledby", "fd-dispatch-h"}}, 2, 0), ui_open(stdout, "div", "fd-section-head"), ui_elem(stdout, "h2", (const UiAttr[]){{"id", "fd-dispatch-h"}}, 1, 0), ui_elem(stdout, "span", (const UiAttr[]){{"class", "fd-dot"}, {"aria-hidden", "true"}}, 2, 0), ui_close(stdout, "span"));
    html(heading); (ui_close(stdout, "h2"), ui_open(stdout, "p", NULL), ui_lit(stdout, "Newest first"), ui_close(stdout, "p"), ui_close(stdout, "div"), ui_open(stdout, "div", "fd-grid"));
    for (i = 0; i < n; i++) { front_card(pool, picks[i], i); front_use(picks[i]); }
    ui_close_n(stdout, "div", "section", NULL);
    return n;
}

static int front_watch(FrontPool *pool) {
    FrontPick *picks[8]; int n, i;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_VIDEO), 45 * FRONT_DAY, 1, picks, 4);
    if (!n) return 0;
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "fd-watch"}, {"id", "fd-watch"}, {"aria-labelledby", "fd-watch-h"}}, 3, 0), ui_open(stdout, "div", "fd-section-head"), ui_elem(stdout, "h2", (const UiAttr[]){{"id", "fd-watch-h"}}, 1, 0), ui_lit(stdout, "Watch"), ui_close(stdout, "h2"), ui_elem(stdout, "a", (const UiAttr[]){{"href", "/watch"}}, 1, 0), ui_lit(stdout, "All video"), ui_close(stdout, "a"), ui_close(stdout, "div"), ui_open(stdout, "div", "fd-shelf"));
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; const char *close = page_emit_item_open(item, "fd-film");
        ui_open(stdout, "span", "fd-frame");
        if (page_image_url_safe(item->image)) page_emit_picture(item->image, "fd-film-img");
        else if (page_image_url_safe(item->poster)) page_emit_picture(item->poster, "fd-film-img");
        ui_elem(stdout, "span", (const UiAttr[]){{"class", "fd-play"}, {"aria-hidden", "true"}}, 2, 0); page_glyph("play"); ui_close(stdout, "span");
        if (item->duration_ms > 0) { ui_open(stdout, "span", "fd-dur"); page_emit_duration(item->duration_ms); ui_close(stdout, "span"); }
        (ui_close(stdout, "span"), ui_open(stdout, "span", "fd-film-title")); html(item->title); (ui_close(stdout, "span"), ui_open(stdout, "span", "fd-film-by"));
        html(front_publisher(item)); ui_close(stdout, "span"); fputs(close, stdout);
        front_use(picks[i]);
    }
    ui_close_n(stdout, "div", "section", NULL);
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
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "fd-hear"}, {"id", "fd-listen"}, {"aria-labelledby", "fd-listen-h"}}, 3, 0), ui_open(stdout, "div", "fd-section-head"), ui_elem(stdout, "h2", (const UiAttr[]){{"id", "fd-listen-h"}}, 1, 0), ui_lit(stdout, "Listen"), ui_close(stdout, "h2"));
    ui_open(stdout,"p",NULL);printf("%d",n);ui_lit(stdout," new episodes</p></div>");ui_open(stdout,"ol","fd-tracks");
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; char id[24]; const char *close;
        snprintf(id, sizeof(id), "fd-h%d", i);
        ui_open_a(stdout, "li", "data-source", item->source);
        if (page_image_url_safe(item->image)) page_emit_picture(item->image, "fd-cover");
        else (ui_elem(stdout, "span", (const UiAttr[]){{"class", "fd-cover"}, {"aria-hidden", "true"}}, 2, 0), ui_close(stdout, "span"));
        (ui_open_a(stdout, "div", "class", "fd-track-words", "id", id), ui_open(stdout, "h3", NULL));
        close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
        (ui_close(stdout, "h3"), ui_open(stdout, "p", "fd-show")); html(front_publisher(item)); ui_close_n(stdout, "p", "div", NULL);
        ui_open(stdout, "span", "fd-when"); front_time(item->at, pool->now); ui_close(stdout, "span");
        front_listen(item, id, 0, "fd-act");
        ms_keep(item, pool->back, 1);
        ui_close(stdout, "li");
        front_use(picks[i]);
    }
    ui_close_n(stdout, "ol", "section", NULL);
    return n;
}

static int front_reading(FrontPool *pool, const char *heading, int cap) {
    FrontPick *picks[12]; int n, i;
    if (cap > 12) cap = 12;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_PUBLICATION) | FRONT_KIND(BF_PAGE_MATERIAL_POST), 0, 1, picks, cap);
    if (!n) return 0;
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "fd-reading"}, {"id", "fd-read"}, {"aria-labelledby", "fd-read-h"}}, 3, 0), ui_open(stdout, "div", "fd-section-head"), ui_elem(stdout, "h2", (const UiAttr[]){{"id", "fd-read-h"}}, 1, 0));
    html(heading); (ui_close(stdout, "h2"), ui_open(stdout, "p", NULL), ui_lit(stdout, "Newest first"), ui_close(stdout, "p"), ui_close(stdout, "div"), ui_open(stdout, "div", "fd-columns"));
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; char id[24]; const char *close;
        snprintf(id, sizeof(id), "fd-r%d", i);
        ui_open(stdout, "article", "fd-read");
        if (page_image_url_safe(item->image)) { close = page_emit_item_open(item, "fd-read-picture"); page_emit_picture(item->image, "fd-read-img"); fputs(close, stdout); }
        (ui_open(stdout, "p", "fd-card-top"), ui_open(stdout, "span", "fd-beat")); html(front_publisher(item));
        (ui_close(stdout, "span"), ui_open(stdout, "span", NULL)); front_pick_time(pool, picks[i]); ui_close(stdout,"span");ui_close(stdout,"p");ui_start(stdout,"div");ui_attr(stdout,"class","fd-card-text");ui_attr_open(stdout,"id"); html(id); (ui_attr_close(stdout),ui_gt(stdout), ui_open(stdout, "h3", NULL));
        close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout); ui_close(stdout, "h3");
        if (item->body[0] && strcmp(item->body, item->title)) { ui_text(stdout, "p", "fd-dek", item->body); }
        (ui_close(stdout, "div"), ui_open(stdout, "footer", NULL)); front_listen(item, id, 0, "fd-act"); front_open(item, NULL); ms_keep(item, pool->back, 1); ui_close_n(stdout, "footer", "article", NULL);
        front_use(picks[i]);
    }
    ui_close_n(stdout, "div", "section", NULL);
    return n;
}

static int front_going(FrontPool *pool) {
    FrontPick *picks[8]; int n, i;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_EVENT), 0, 0, picks, 6);
    if (!n) return 0;
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "fd-going"}, {"id", "fd-on"}, {"aria-labelledby", "fd-on-h"}}, 3, 0), ui_open(stdout, "div", "fd-section-head"), ui_elem(stdout, "h2", (const UiAttr[]){{"id", "fd-on-h"}}, 1, 0), ui_lit(stdout, "What&#039;s on"), ui_close(stdout, "h2"), ui_elem(stdout, "a", (const UiAttr[]){{"href", "/whats-on"}}, 1, 0), ui_lit(stdout, "Full calendar"), ui_close(stdout, "a"), ui_close(stdout, "div"), ui_open(stdout, "ol", "fd-agenda"));
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; struct tm tm; char date[24] = "", weekday[16] = "", ahead[48] = ""; const char *close;
        if (page_local_time(item->at, &tm)) { strftime(date, sizeof(date), "%e %b", &tm); strftime(weekday, sizeof(weekday), "%A", &tm); }
        page_when_ahead(item->at, item->duration_ms > 0 ? item->at + (time_t)(item->duration_ms / 1000) : 0, pool->now, ahead, sizeof(ahead));
        (ui_open(stdout, "li", NULL), ui_open(stdout, "p", "fd-date"), ui_open(stdout, "b", NULL)); html(date); (ui_close(stdout, "b"), ui_open(stdout, "span", NULL)); html(weekday); (ui_close(stdout, "span"), ui_close(stdout, "p"), ui_open(stdout, "div", NULL), ui_open(stdout, "h3", NULL));
        close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
        (ui_close(stdout, "h3"), ui_open(stdout, "p", NULL)); html(item->where[0] ? item->where : front_publisher(item)); (ui_close(stdout, "p"), ui_close(stdout, "div"), ui_open(stdout, "p", "fd-ahead"));
        html(ahead); ui_close(stdout, "p"); ms_keep(item, pool->back, 1); ui_close(stdout, "li");
        front_use(picks[i]);
    }
    ui_close_n(stdout, "ol", "section", NULL);
    return n;
}

static int front_pictured(FrontPool *pool, const char *heading) {
    FrontPick *picks[10]; int n, i, kept = 0;
    n = front_choose(pool, FRONT_KIND(BF_PAGE_MATERIAL_PHOTO), 0, 1, picks, 8);
    for (i = 0; i < n; i++) if (page_image_url_safe(picks[i]->item.image)) kept++;
    if (!kept) return 0;
    (ui_elem(stdout, "section", (const UiAttr[]){{"class", "fd-pictured"}, {"id", "fd-pictured"}, {"aria-labelledby", "fd-pic-h"}}, 3, 0), ui_open(stdout, "div", "fd-section-head"), ui_elem(stdout, "h2", (const UiAttr[]){{"id", "fd-pic-h"}}, 1, 0));
    html(heading); (ui_close(stdout, "h2"), ui_close(stdout, "div"), ui_open(stdout, "div", "fd-covers"));
    for (i = 0; i < n; i++) {
        const PageItem *item = &picks[i]->item; const char *close;
        if (!page_image_url_safe(item->image)) continue;
        close = page_emit_item_open(item, "fd-cover-item");
        page_emit_picture(item->image, "fd-cover-img");
        ui_open(stdout, "span", "fd-film-title"); html(item->title); (ui_close(stdout, "span"), ui_open(stdout, "span", "fd-film-by"));
        html(front_publisher(item)); ui_close(stdout, "span"); fputs(close, stdout);
        front_use(picks[i]);
    }
    ui_close_n(stdout, "div", "section", NULL);
    return kept;
}

static void front_standard(void) {
    ui_start(stdout,"section");ui_attr(stdout,"class","fd-standard");ui_attr(stdout,"aria-labelledby","fd-standard-h");ui_gt(stdout);
    ui_open(stdout,"p","fd-kicker");ui_text(stdout,"span",NULL,"The Aurekai standard");ui_close(stdout,"p");
    ui_elem(stdout,"h2",(const UiAttr[]){{"id","fd-standard-h"}},1,0);ui_lit(stdout,"A front page that ends.");ui_close(stdout,"h2");
    ui_open(stdout,"p",NULL);ui_lit(stdout,"Everything here was published by the source named on it, and every item links back there. Nothing is ranked to keep you scrolling: stories run newest first, and when today&#039;s run out, so does the page.");ui_close(stdout,"p");
    ui_open(stdout,"p","fd-standard-ways");ui_elem(stdout,"a",(const UiAttr[]){{"href","/channels"}},1,0);ui_lit(stdout,"See the sources");ui_close(stdout,"a");ui_elem(stdout,"a",(const UiAttr[]){{"href","/people"}},1,0);ui_lit(stdout,"See the people behind them");ui_close(stdout,"a");ui_close(stdout,"p");ui_close(stdout,"section");
}

/* The count of picks the composer would show, for the edition strip: the
 * strip is drawn first, so the count is the regions' caps against the pool. */
static int front_available(FrontPool *pool, unsigned mask, time_t max_age, int cap) {
    FrontPick *picks[12]; int n = front_choose(pool, mask, max_age, 0, picks, cap > 12 ? 12 : cap);
    return n;
}

/* A front's pool is the same for everyone who opens it: its beats read the
 * same sources whoever is asking. Assembling it parses every beat's sources,
 * so a worker keeps the last pool per host and route for two minutes and each
 * request composes from a copy; what is personal (saved state, navigation)
 * is still decided per request. The routes the beats stood for are replayed so
 * the sections beneath behave as if read again. */
#define FRONT_CACHE_SECONDS 120
#define FRONT_CACHE_SLOTS 8
typedef struct {
    char key[160]; time_t at; FrontPick *pick; int count, beats, taken, source_count;
    const BfPageProgram *beat[FRONT_BEATS]; char sources[64][96]; time_t newest;
} FrontPoolCache;
static FrontPoolCache front_cache[FRONT_CACHE_SLOTS];
static void front_cache_key(const PageDocument *doc, char *key, size_t cap) {
    snprintf(key, cap, "%s|%s", page_render_host ? page_render_host : "", doc->route ? doc->route : "");
}
static int front_cache_restore(const PageDocument *doc, FrontPool *pool) {
    char key[160]; int s, i;
    front_cache_key(doc, key, sizeof key);
    for (s = 0; s < FRONT_CACHE_SLOTS; s++) {
        FrontPoolCache *c = &front_cache[s];
        if (!c->pick || strcmp(c->key, key) || pool->now - c->at > FRONT_CACHE_SECONDS || pool->now < c->at) continue;
        memcpy(pool->pick, c->pick, sizeof(*pool->pick) * (size_t)c->count);
        pool->count = c->count; pool->beats = c->beats; pool->source_count = c->source_count; pool->newest = c->newest;
        memcpy(pool->beat, c->beat, sizeof(pool->beat)); memcpy(pool->sources, c->sources, sizeof(pool->sources));
        for (i = 0; i < c->taken && i < c->beats; i++) if (c->beat[i]) page_front_take_route(c->beat[i]->route);
        return 1;
    }
    return 0;
}
static void front_cache_store(const PageDocument *doc, const FrontPool *pool, int taken) {
    char key[160]; int s, slot = 0;
    front_cache_key(doc, key, sizeof key);
    for (s = 0; s < FRONT_CACHE_SLOTS; s++) {
        if (!strcmp(front_cache[s].key, key)) { slot = s; break; }
        if (front_cache[s].at < front_cache[slot].at) slot = s;
    }
    FrontPoolCache *c = &front_cache[slot];
    free(c->pick); memset(c, 0, sizeof(*c));
    if (!(c->pick = malloc(sizeof(*c->pick) * (size_t)(pool->count ? pool->count : 1)))) return;
    memcpy(c->pick, pool->pick, sizeof(*c->pick) * (size_t)pool->count);
    snprintf(c->key, sizeof c->key, "%s", key); c->at = pool->now; c->count = pool->count; c->beats = pool->beats;
    c->taken = taken; c->source_count = pool->source_count; c->newest = pool->newest;
    memcpy(c->beat, pool->beat, sizeof(c->beat)); memcpy(c->sources, pool->sources, sizeof(c->sources));
}

static int page_front_editorial(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    const char *territory = NULL; FrontPool pool; int shown = 0, i, hub;
    FrontPick *lead[1];
    if (!front_is_apex(doc, &territory)) return 0;
    front_db = db;
    memset(&pool, 0, sizeof(pool));
    pool.now = ctx ? ctx->now : time(NULL);
    pool.back = doc->route;
    if (!(pool.pick = calloc(FRONT_POOL_MAX, sizeof(*pool.pick)))) return 0;
    ms_saved_open();
    /* The work front is a dossier of dispatches; Discover is the whole
     * estate on every host, so it keeps the full reading rhythm. */
    hub = !strcmp(territory, "work") && strcmp(doc->route, "/discover");
    /* The beats are the program's sections; a route without sections is its
     * own single beat over the material the page already read. */
    if (doc->program && doc->program->section_count && doc->program->sections[0] && front_cache_restore(doc, &pool)) {
        /* Composed from the shared pool below. */
    } else if (doc->program && doc->program->section_count && doc->program->sections[0]) {
        int taken = 0;
        for (i = 0; i < (int)doc->program->section_count && pool.beats < FRONT_BEATS; i++) {
            const BfPageProgram *program = doc->program->sections[i] ? page_program_for_route(doc->program->sections[i]) : NULL;
            PageMaterial *beat;
            if (!program || !(beat = calloc(1, sizeof(*beat)))) continue;
            front_beat_material(db, projection, program, beat);
            pool.beat[pool.beats] = program;
            front_pool_take(&pool, beat, pool.beats);
            pool.beats++;
            page_front_take_route(program->route);
            taken = pool.beats;
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
        front_cache_store(doc, &pool, taken);
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
        ui_start(stdout,"div");ui_attr_open(stdout,"class");ui_lit(stdout,"fd ");fputs(hub ? "fd-hub" : "fd-discover", stdout);ui_attr_close(stdout);ui_attr_open(stdout,"data-front");html(territory);ui_attr_close(stdout);ui_gt(stdout);
        front_strip(&pool, today);
        front_head(doc, &pool, kinds, kinds_n);
    }
    if (front_choose(&pool, FRONT_STORIES, 0, 1, lead, 1)) {
        front_lead(&pool, lead[0]); front_use(lead[0]); shown++;
        /* Discover carries the lead's longer history from the whole archive. */
        if (!strcmp(doc->route, "/discover")) page_emit_archive_thread(&lead[0]->item, pool.now);
    }
    if (hub) {
        shown += front_dispatches(&pool, "Dispatches and recordings", 9);
    } else {
        shown += front_watch(&pool);
        shown += front_hear(&pool);
        shown += front_reading(&pool, "Reading", 6);
        shown += front_going(&pool);
        shown += front_pictured(&pool, !strcmp(territory, "shop") ? "In the shops" : "Pictured");
    }
    if (!shown) {
        ui_open(stdout,"p","fd-empty");ui_lit(stdout,"Nothing new from this front&#039;s sources yet. The sources are read through the day; ");
        ui_elem(stdout,"a",(const UiAttr[]){{"href","/channels"}},1,0);ui_lit(stdout,"see which ones");ui_close(stdout,"a");ui_lit(stdout,".");ui_close(stdout,"p");
    }
    front_standard();
    ui_close(stdout, "div");
    ms_saved_close();
    free(pool.pick);
    return shown ? shown : 1;
}

#endif
