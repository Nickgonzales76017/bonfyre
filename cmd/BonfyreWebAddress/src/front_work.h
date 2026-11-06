/* The work front's editorial program.  See front_editorial.h.
 *
 * Work reads as insiders by beat, the way Puck and Defector are put together:
 * one story on the house's field with its photograph printed in the field's
 * colour, then the desk -- each beat the front names (the day's news, jobs,
 * small business, money) led by its newest story with who wrote it and when,
 * two or three more under it, and a column of short notes down the side --
 * then the ways this estate itself does work for people.
 *
 * Every story is one the beat's own sources published; a beat with nothing
 * from its sources is left out rather than filled.  A story shown here is not
 * shown twice on the front. */

/* The public entrance uses the same reviewed image and card projection as
 * setup and installed work. No editor, worker or private record is mounted. */
#include "pack_presentation.h"

#define FRONT_WORK_SHOWN_MAX 96
static char front_work_shown[FRONT_WORK_SHOWN_MAX][128];
static int front_work_shown_count;

static void front_work_item_id(const PageItem *item, char *out, size_t cap) {
    if (item->source[0] && item->key[0]) snprintf(out, cap, "%s|%s", item->source, item->key);
    else snprintf(out, cap, "%.120s", item->href[0] ? item->href : item->title);
}

static int front_work_seen(const PageItem *item) {
    char id[128]; int i;
    front_work_item_id(item, id, sizeof(id));
    for (i = 0; i < front_work_shown_count; i++) if (!strcmp(front_work_shown[i], id)) return 1;
    return 0;
}

static void front_work_mark(const PageItem *item) {
    page_front_use(item);   /* the front's sections below strike it from theirs */
    if (front_work_shown_count >= FRONT_WORK_SHOWN_MAX) return;
    front_work_item_id(item, front_work_shown[front_work_shown_count], sizeof(front_work_shown[0]));
    front_work_shown_count++;
}

/* A beat's material, read exactly as its own section reads it: the subject
 * projection when one is compiled, otherwise the program's sources. */
static void front_work_material(sqlite3 *db, PageProjectionRead *projection, const BfPageProgram *program,
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
        /* The territory's readers were chosen before this beat had sources of
         * its own; when they leave the beat with nothing, the beat reads the
         * sources its families name. */
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

/* Stories a beat can run: written things and recordings that say what they
 * are and when, not yet shown, newest first.  Places and events belong to
 * other fronts. */
static int front_work_precedes(const PageItem *a, const PageItem *b, int pictured_first) {
    int a_pic = pictured_first && page_image_url_safe(a->image);
    int b_pic = pictured_first && page_image_url_safe(b->image);
    if (a_pic != b_pic) return a_pic > b_pic;
    if (a->at != b->at) return a->at > b->at;
    int source_order = strcmp(a->source, b->source);
    return source_order ? source_order < 0 : strcmp(a->key, b->key) < 0;
}
/* Headline text is not an identity. Retained source/item coordinates decide
 * whether two candidates are the same published material. */
static int front_work_same_story(const PageItem *a,const PageItem *b) {
    if(a->source[0]&&a->key[0]&&b->source[0]&&b->key[0])
        return !strcmp(a->source,b->source)&&!strcmp(a->key,b->key);
    if(a->href[0]&&b->href[0])return !strcmp(a->href,b->href);
    return !strcmp(a->source,b->source)&&!strcmp(a->title,b->title);
}
static int front_work_stories(PageMaterial *material, PageItem **out, int cap, int pictured_first, time_t now) {
    static const BfPageMaterial kinds[] = {BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_VIDEO,
                                           BF_PAGE_MATERIAL_AUDIO, BF_PAGE_MATERIAL_POST};
    int n = 0, i, j; size_t k;
    if (!material || !out || cap <= 0) return 0;
    for (k = 0; k < sizeof(kinds) / sizeof(kinds[0]); k++) {
        PageStream *stream = &material->stream[kinds[k]];
        /* The requested output count is not the input scan bound. In
         * particular a one-item lead must consider every retained candidate,
         * rather than picking the first record of the first source. */
        for (i = stream->cursor; i < stream->count; i++) {
            PageItem *item = &stream->item[i];
            if (!item->title[0] || !item->at || item->at > now || front_work_seen(item)) continue;
            for (j = 0; j < n && !front_work_same_story(out[j], item); j++) {}
            if (j < n) {
                if (!front_work_precedes(item, out[j], pictured_first)) continue;
                for (int move = j; move + 1 < n; move++) out[move] = out[move + 1];
                n--;
            }
            for (j = 0; j < n && !front_work_precedes(item, out[j], pictured_first); j++) {}
            if (j >= cap) continue;
            if (n < cap) n++;
            for (int move = n - 1; move > j; move--) out[move] = out[move - 1];
            out[j] = item;
        }
    }
    return n;
}

static void front_work_byline(const PageItem *item, time_t now) {
    char when[48];
    page_when(item->at, now, when, sizeof(when));
    fputs("<p class=\"wf-by\">", stdout);
    if (item->by[0]) { fputs("<span class=\"wf-who\">", stdout); html(item->by); fputs("</span>", stdout); }
    if (item->note[0] && strcmp(item->note, item->by)) { fputs("<span>", stdout); html(item->note); fputs("</span>", stdout); }
    if (when[0]) {
        char iso[32]; struct tm tm;
        if (gmtime_r(&item->at, &tm) && strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tm)) {
            fputs("<time datetime=\"", stdout); html(iso); fputs("\">", stdout); html(when); fputs("</time>", stdout);
        }
    }
    fputs("</p>", stdout);
}

static void front_work_story(const PageItem *item, const char *heading_tag, int with_dek, time_t now) {
    const char *close = page_emit_item_open(item, "wf-story");
    if (!strcmp(heading_tag, "h3") && with_dek && page_image_url_safe(item->image)) page_emit_picture(item->image, "wf-beat-photo");
    printf("<%s>", heading_tag); html(item->title); printf("</%s>", heading_tag);
    fputs(close, stdout);
    if (with_dek && item->body[0] && strcmp(item->body, item->title)) { fputs("<p class=\"wf-dek\">", stdout); html(item->body); fputs("</p>", stdout); }
    front_work_byline(item, now);
}

/* The ways in this estate offers for work, as the CMS has them for this front:
 * site_door rows on page "front-work", each a heading, a sentence and its
 * labelled addresses. */
static int front_work_doors(sqlite3 *db) {
    sqlite3_stmt *st = NULL, *ways = NULL; int rows = 0;
    if (!db || !cms_type_registered(db, "site_door")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT eyebrow, heading, body, ways FROM site_door"
                               " WHERE page='front-work' AND status='published' ORDER BY position, id", -1, &st, NULL) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(value,'$.label'), json_extract(value,'$.href') FROM json_each(?1)",
                           -1, &ways, NULL) != SQLITE_OK) { sqlite3_finalize(st); return 0; }
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *eyebrow = (const char *)sqlite3_column_text(st, 0), *heading = (const char *)sqlite3_column_text(st, 1);
        const char *body = (const char *)sqlite3_column_text(st, 2), *list = (const char *)sqlite3_column_text(st, 3);
        if (!heading || !list) continue;
        if (!rows) {
            page_emit_profile_for(db, "front-work|doors", BF_PAGE_MATERIAL_PUBLICATION, ".wf-doors", NULL, 0);
            fputs("<section class=\"section wf wf-doors\"><h2 class=\"wf-head\">",stdout); front_section_glyph("tool"); fputs("Work with Aurekai</h2><div class=\"wf-door-row\">", stdout);
        }
        fputs("<div class=\"wf-door\">", stdout);
        if (eyebrow && *eyebrow) { fputs("<p class=\"wf-kicker\">", stdout); html(eyebrow); fputs("</p>", stdout); }
        fputs("<h3>", stdout); html(heading); fputs("</h3>", stdout);
        if (body && *body) { fputs("<p>", stdout); html(body); fputs("</p>", stdout); }
        fputs("<p class=\"wf-ways\">", stdout);
        sqlite3_reset(ways); sqlite3_bind_text(ways, 1, list, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(ways) == SQLITE_ROW) {
            const char *label = (const char *)sqlite3_column_text(ways, 0), *href = (const char *)sqlite3_column_text(ways, 1);
            if (!label || !href || strncmp(href, "https://", 8) || strpbrk(href, "\"<> ")) continue;
            fputs("<a href=\"", stdout); html(href); fputs("\">", stdout); html(label); fputs("</a>", stdout);
        }
        fputs("</p></div>", stdout);
        rows++;
    }
    if (rows) fputs("</div></section>", stdout);
    sqlite3_finalize(ways); sqlite3_finalize(st);
    return rows ? 1 : 0;
}

static int front_work_tools(sqlite3 *db) {
    if (!db) return 0;
    fputs("<section class=\"wf-tools\" aria-labelledby=\"work-tools-heading\" data-work-entrance>"
          "<p class=\"wf-kicker\">YOUR WORKSPACE</p><h2 id=\"work-tools-heading\">",stdout);
    front_section_glyph("apps");
    fputs("Your work, in one place</h2><p>Open your apps and saved work, or choose an application for your account or organization.</p>"
          "<nav aria-label=\"Workspaces\"><a data-work-open href=\"/apps?context=self\">Open your work</a>"
          "<a data-work-setup href=\"/packs/setup\">Explore applications</a></nav></section>"
          "<style>main.wrap>.wf-tools{margin:0 0 56px;padding:clamp(28px,5vw,64px) clamp(20px,4vw,48px);background:#b5df32;color:#111913}"
          ".wf-tools h2{font:400 clamp(44px,7vw,104px)/.95 'Instrument Serif',Georgia,serif;margin:10px 0 18px;color:#111913}"
          ".wf-tools p{max-width:62ch;font:400 17px/1.55 'DM Sans',system-ui,sans-serif}.wf-tools nav{display:flex;gap:12px;flex-wrap:wrap;margin-top:24px}"
          ".wf-tools nav a{display:inline-flex;align-items:center;min-height:44px;padding:10px 18px;border:1px solid #243e2b;border-radius:999px;color:#111913;text-decoration:none;font:600 14px 'DM Sans',system-ui}"
          ".wf-tools nav a:first-child{background:#172b1d;color:#fff}.wf-tools nav a:focus-visible{outline:3px solid #172b1d;outline-offset:3px}</style>",stdout);
    return 1;
}

static int front_editorial_work(sqlite3 *db, PageProjectionRead *projection, const PageDocument *doc,
                                PageMaterial *material, PageEmitContext *ctx) {
    enum { BEATS = 6, TAKE = 40 };
    const BfPageProgram *beats[BEATS]; PageMaterial *beat_material[BEATS] = {0};
    PageItem *pool[TAKE], *lead = NULL; const BfPageProgram *lead_beat = NULL;
    int beat_count = 0, shown = 0, i, n;
    time_t now = ctx ? ctx->now : time(NULL);
    (void)material;
    if (!doc->program) return 0;
    front_work_shown_count = 0;
    shown += front_work_tools(db);
    for (i = 0; i < (int)doc->program->section_count && beat_count < BEATS; i++) {
        const BfPageProgram *program = doc->program->sections[i] ? page_program_for_route(doc->program->sections[i]) : NULL;
        if (!program || !(beat_material[beat_count] = calloc(1, sizeof(PageMaterial)))) continue;
        front_work_material(db, projection, program, beat_material[beat_count]);
        beats[beat_count++] = program;
    }
    /* The lead: the newest pictured story across every beat, else the newest. */
    for (i = 0; i < beat_count; i++) {
        n = front_work_stories(beat_material[i], pool, 1, 1, now);
        if (n && (!lead || (page_image_url_safe(pool[0]->image) > page_image_url_safe(lead->image)) ||
                  (page_image_url_safe(pool[0]->image) == page_image_url_safe(lead->image) && pool[0]->at > lead->at))) {
            lead = pool[0]; lead_beat = beats[i];
        }
    }
    if (!lead) goto done;
    /* This front dresses itself: its opening and its fonts come from the
     * house before anything is drawn on it. */
    page_emit_profile_for(db, "front-work", BF_PAGE_MATERIAL_PUBLICATION, ".wrap", NULL, 0);
    fputs("<link rel=\"stylesheet\" href=\"/assets/front-work.css\">", stdout);
    fputs("<style>"
      ".wf-beats{gap:16px}.wf-beat{padding:clamp(18px,2.3vw,30px);border:1px solid var(--w-rule);border-radius:18px;background:color-mix(in srgb,var(--w-ink) 5%,var(--w-ground))}"
      ".wf .wf-beat-name{display:flex;align-items:center;min-height:48px;margin-bottom:16px;padding-bottom:14px}.wf .wf-beat-name a{display:inline-flex;align-items:center;gap:10px;text-decoration:none}"
      ".wf-beat .wf-beat-photo{display:block;width:100%;aspect-ratio:16/9;object-fit:cover;border-radius:12px;margin:0 0 16px}.wf-beat .wf-story:focus-visible{outline:3px solid var(--w-accent);outline-offset:4px;border-radius:6px}"
      ".wf-beat .wf-more li{padding:12px 0}.wf-beat .wf-more h4{line-height:1.4}.wf-notes{padding:22px;border:1px solid var(--w-rule);border-radius:18px;background:color-mix(in srgb,var(--w-ink) 4%,var(--w-ground))}"
      ".wf-notes li{gap:8px}.wf-door{padding:20px;border:1px solid var(--w-rule);border-radius:16px}.wf-door-row{gap:14px}"
      "@media(max-width:860px){.wf-beat{padding:18px}.wf-notes{margin-top:16px;padding:18px}.wf-door{padding:18px}}"
      "</style>", stdout);
    page_emit_profile_for(db, "front-work|lead", BF_PAGE_MATERIAL_PUBLICATION, ".wf-lead", NULL, 0);
    fputs("<section class=\"section wf wf-lead\">", stdout);
    if (page_image_url_safe(lead->image)) {
        const char *close = page_emit_item_open(lead, "wf-picture");
        page_emit_picture(lead->image, "wf-photo");
        fputs(close, stdout);
    }
    fputs("<div class=\"wf-lead-words\"><p class=\"wf-kicker\"><a href=\"", stdout); html(lead_beat->route); fputs("\">", stdout);
    html(lead_beat->title); fputs("</a></p>", stdout);
    front_work_story(lead, "h2", 1, now);
    fputs("</div></section>", stdout);
    front_work_mark(lead);
    shown++;

    /* The desk: each beat led by its newest story, a few more under it, and
     * the short notes down the side. */
    page_emit_profile_for(db, "front-work|desk", BF_PAGE_MATERIAL_PUBLICATION, ".wf-desk", NULL, 0);
    fputs("<section class=\"section wf wf-desk\"><div class=\"wf-beats\">", stdout);
    for (i = 0; i < beat_count; i++) {
        int k;
        n = front_work_stories(beat_material[i], pool, TAKE, 0, now);
        if (!n) continue;
        fputs("<div class=\"wf-beat\"><h2 class=\"wf-beat-name\"><a href=\"", stdout); html(beats[i]->route); fputs("\">", stdout);
        front_section_glyph(page_nav_glyph(beats[i]->route));
        html(beats[i]->title);
        fputs("</a></h2>", stdout);
        front_work_story(pool[0], "h3", 1, now);
        front_work_mark(pool[0]);
        if (n > 1) {
            fputs("<ol class=\"wf-more\">", stdout);
            for (k = 1; k < n && k < 4; k++) { fputs("<li>", stdout); front_work_story(pool[k], "h4", 0, now); fputs("</li>", stdout); front_work_mark(pool[k]); }
            fputs("</ol>", stdout);
        }
        fputs("</div>", stdout);
        /* The beat's own section below would repeat it. */
        page_front_take_route(beats[i]->route);
    }
    fputs("</div>", stdout);
    {
        /* Short notes: what is left on every beat, newest first, a line each. */
        PageItem *notes[TAKE]; int count = 0, k;
        for (i = 0; i < beat_count; i++) {
            n = front_work_stories(beat_material[i], pool, TAKE, 0, now);
            for (k = 0; k < n && count < TAKE; k++) notes[count++] = pool[k];
        }
        for (i = 1; i < count; i++)
            for (k = i; k > 0 && notes[k - 1]->at < notes[k]->at; k--) { PageItem *t = notes[k - 1]; notes[k - 1] = notes[k]; notes[k] = t; }
        if (count) {
            int emitted = 0;
            fputs("<aside class=\"wf-notes\"><h2 class=\"wf-head\">", stdout); front_section_glyph("file-check"); fputs("In brief</h2><ol>", stdout);
            for (i = 0; i < count && emitted < 8; i++) {
                if (front_work_seen(notes[i])) continue;
                fputs("<li>", stdout); front_work_story(notes[i], "h3", 0, now); fputs("</li>", stdout);
                front_work_mark(notes[i]);
                emitted++;
            }
            fputs("</ol></aside>", stdout);
        }
    }
    fputs("</section>", stdout);
    shown++;
done:
    shown += front_work_doors(db);
    for (i = 0; i < beat_count; i++) free(beat_material[i]);
    return shown;
}
