/* Detail depth.
 *
 * A detail page used to end at its own kind: a piece led to more pieces from
 * the same source, a maker's page led to that maker's pieces, a feed's page to
 * what it carried.  The material already reached further than that.  The
 * ledger knows the feed every kept piece came through, the publisher behind
 * it and the way it was read; the archive knows who each piece credits; the
 * compiled application shapes say which record kinds exist; and the retained
 * feed bodies carry topics, credits and links nothing had read out of them.
 *
 * So each detail page closes with the other kinds of page its own material
 * reaches -- feeds, publishers, makers, readers, application shapes, topics,
 * printed names -- each one more detail page with its own continuations.
 * That is what lets a visitor keep going without ever reaching a dead end
 * or a list of one kind.
 *
 * Two halves:
 *   - render: read-only joins over the stores named in detail_depth.h;
 *   - derive: an offline pass over the retained bodies that writes facets
 *     (topic, maker, mention, linked publisher, month, media) into its own
 *     store, never into the archive or the ledger.
 *
 * Boundaries, said on the pages that show them: a topic is a label the
 * publisher printed, a printed name is a run of capitalised words in what the
 * publisher wrote, a maker is a credit the publisher printed, and an
 * application shape is the kind of record an application would keep such a
 * thing as -- none of them an identity, an endorsement, or a record held.
 */
#define _POSIX_C_SOURCE 200809L
#include "detail_depth.h"
#include "people_projection.h"
#include "glyph_set.h"
#include "bf_sqlite_path.h"
#include "bf_lex.h"

#include <ctype.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <zlib.h>

/* ------------------------------------------------------------------ words */

static void esc(FILE *out, const char *s) { bf_lex_html_emit(out, s ? s : ""); }

static void url(FILE *out, const char *s) {
    static const char *hex = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        if (isalnum(*p) || *p == '-' || *p == '.' || *p == '_' || *p == '~') fputc(*p, out);
        else { fputc('%', out); fputc(hex[*p >> 4], out); fputc(hex[*p & 15], out); }
    }
}

static const char *glyph(const char *name) {
    static const struct { const char *name, *svg; } set[] = {
        {"feed", BF_ICON_RSS}, {"publisher", BF_ICON_WORLD_WWW}, {"maker", BF_ICON_USER}, {"reader", BF_ICON_TOOL},
        {"app", BF_ICON_APPS}, {"topic", BF_ICON_TAG}, {"mention", BF_ICON_FILE_TEXT}, {"named", BF_ICON_LINK},
        {"item", BF_ICON_LIST}, {"group", BF_ICON_GROUP},
    };
    for (size_t i = 0; i < sizeof(set) / sizeof(set[0]); i++) if (!strcmp(set[i].name, name)) return set[i].svg;
    return "";
}

const char *bf_detail_reader_words(const char *p) {
    if (!p || !*p) return "Web page reader";
    if (!strcmp(p, "rss") || !strcmp(p, "rss-xml") || !strcmp(p, "feed")) return "RSS reader";
    if (!strcmp(p, "atom")) return "Atom reader";
    if (!strcmp(p, "json_feed")) return "JSON Feed reader";
    if (!strcmp(p, "ics")) return "Calendar reader";
    if (!strcmp(p, "sitemap")) return "Sitemap reader";
    if (!strcmp(p, "json_api")) return "JSON API reader";
    if (!strcmp(p, "text")) return "Text feed reader";
    if (!strcmp(p, "https")) return "Web page reader";
    if (!strcmp(p, "audio")) return "Audio reader";
    if (!strcmp(p, "video")) return "Video reader";
    if (!strcmp(p, "image")) return "Picture reader";
    if (!strcmp(p, "dcat")) return "Data catalogue reader";
    if (!strcmp(p, "socrata")) return "Open data reader";
    if (!strcmp(p, "mcp")) return "MCP reader";
    if (!strcmp(p, "openapi")) return "OpenAPI reader";
    if (!strcmp(p, "graphql")) return "GraphQL reader";
    if (!strcmp(p, "x402")) return "Paid endpoint reader";
    return "Reader";
}

/* -------------------------------------------------------- application shapes
 *
 * Which record kind an application would keep a thing like this as.  Declared
 * here and in detail_depth.cue; a row is shown only when the compiled shape
 * store actually holds that slug.  The shape is not a record and nothing is
 * imported anywhere. */
typedef struct { const char *what, *slug; } Shape;
static const Shape shapes[] = {
    {"story", "frappe-blog-post"}, {"story", "frappe-web-page"},
    {"event", "frappe-event"},
    {"video", "erpnext-video"},
    {"photo", "frappe-file"}, {"audio", "frappe-file"},
    {"family:jobs", "hrms-job-opening"}, {"family:jobs", "lms-job-opportunity"},
    {"maker", "frappe-blogger"}, {"maker", "frappe-contact"}, {"maker", "crm-crm-contacts"},
    {"publisher", "crm-crm-organization"},
    {"topic", "frappe-tag"}, {"topic", "frappe-blog-category"},
};
#define SHAPES (sizeof(shapes) / sizeof(shapes[0]))

static int platform_host(const char *h);

/* ------------------------------------------------------------------ stores */

typedef struct { sqlite3 *db; int fr, fp, dd, jt; } Sess;

static void plausible_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc; sqlite3_result_int(ctx, bf_people_name_plausible((const char *)sqlite3_value_text(argv[0])));
}

static int attach(sqlite3 *db, const char *env, const char *as) {
    const char *path = getenv(env); char uri[PATH_MAX + 32], sql[96]; sqlite3_stmt *st = NULL; int ok = 0;
    if (!path || !*path || bf_sqlite_readonly_uri(uri, sizeof(uri), path) >= (int)sizeof(uri)) return 0;
    snprintf(sql, sizeof(sql), "ATTACH DATABASE ?1 AS %s", as);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, uri, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }
    sqlite3_finalize(st);
    return ok;
}

static int has_table(sqlite3 *db, const char *schema, const char *table) {
    char sql[160]; sqlite3_stmt *st = NULL; int ok = 0;
    snprintf(sql, sizeof(sql), "SELECT 1 FROM %s.sqlite_master WHERE type='table' AND name=?1", schema);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, table, -1, SQLITE_STATIC);
        ok = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    return ok;
}

static int sess_open(Sess *s) {
    const char *archive = getenv("BONFYRE_ITEM_ARCHIVE_DB"); char uri[PATH_MAX + 32];
    memset(s, 0, sizeof(*s));
    if (!archive || !*archive || bf_sqlite_readonly_uri(uri, sizeof(uri), archive) >= (int)sizeof(uri)) return 0;
    if (sqlite3_open_v2(uri, &s->db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { sqlite3_close(s->db); s->db = NULL; return 0; }
    sqlite3_busy_timeout(s->db, 1500);
    sqlite3_create_function(s->db, "plausible", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, plausible_fn, NULL, NULL);
    s->fr = attach(s->db, "BONFYRE_SOURCE_FRONTIER_DB", "fr") && has_table(s->db, "fr", "channel");
    s->fp = attach(s->db, "BONFYRE_FRAPPE_SURFACE_DB", "fp") && has_table(s->db, "fp", "frappe_program");
    s->dd = attach(s->db, "BONFYRE_DETAIL_DEPTH_DB", "dd") && has_table(s->db, "dd", "item_facet");
    s->jt = attach(s->db, "BONFYRE_CMS_DB", "jt") && has_table(s->db, "jt", "internet_source");
    if (sqlite3_exec(s->db, "CREATE TEMP TABLE focus(source TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(source,key))", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(s->db); s->db = NULL; return 0;
    }
    return 1;
}

static void sess_close(Sess *s) { if (s->db) sqlite3_close(s->db); s->db = NULL; }

static void fill(Sess *s, const char *sql, const char *a, const char *b) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &st, NULL) != SQLITE_OK) return;
    if (a) sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT);
    if (b) sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
}

/* --------------------------------------------------------------- the rows */

typedef struct {
    FILE *out; int desk, rows, open;
    const char *self;          /* this page's own address, never offered */
    char heading[96];
} Emit;

static void row_open(Emit *e) {
    if (e->open) return;
    e->open = 1;
    if (e->desk) {
        fputs("<h3 class=\"room-h\">", e->out); esc(e->out, e->heading); fputs("</h3><div class=\"keep-going\" data-detail-depth>", e->out);
    } else {
        fputs("<section class=\"region keep-going\" aria-label=\"", e->out); esc(e->out, e->heading);
        fputs("\" data-detail-depth><h2 class=\"region-label\">", e->out); esc(e->out, e->heading); fputs("</h2><ul class=\"more\">", e->out);
    }
}

static void row_close(Emit *e, const char *boundary) {
    if (!e->open) return;
    if (e->desk) { fputs("</div>", e->out); if (boundary) { fputs("<p class=\"subj-note\">", e->out); esc(e->out, boundary); fputs("</p>", e->out); } }
    else { fputs("</ul>", e->out); if (boundary) { fputs("<p class=\"src\">", e->out); esc(e->out, boundary); fputs("</p>", e->out); } fputs("</section>", e->out); }
}

/* One continuation: an icon, a name that opens the page, what it is. */
static void row(Emit *e, const char *kind, const char *href_prefix, const char *v1, const char *k2, const char *v2,
                const char *label, const char *caption, int count) {
    char self[2048]; FILE *m = fmemopen(self, sizeof(self), "w");
    if (m) { fputs(href_prefix, m); if (v1) url(m, v1); if (k2) { fputs(k2, m); url(m, v2); } fputc(0, m); fclose(m); }
    else self[0] = 0;
    if (e->self && !strcmp(self, e->self)) return;
    if (!label || !*label) return;
    row_open(e);
    if (e->desk) {
        fputs("<a class=\"desk-row\" data-depth-kind=\"", e->out); esc(e->out, kind); fputs("\" href=\"", e->out); esc(e->out, self);
        fputs("\"><span class=\"desk-l\">", e->out); fputs(glyph(kind), e->out); esc(e->out, label);
        fputs("</span><span class=\"desk-a\">", e->out); esc(e->out, caption);
        fputs("</span><span class=\"desk-s\">", e->out);
        if (count > 1) fprintf(e->out, "%d pieces", count);
        fputs("</span></a>", e->out);
    } else {
        fputs("<li data-depth-kind=\"", e->out); esc(e->out, kind); fputs("\"><a href=\"", e->out); esc(e->out, self); fputs("\">", e->out);
        fputs(glyph(kind), e->out); esc(e->out, label); fputs("</a><span class=\"by\">", e->out); esc(e->out, caption);
        if (count > 1) fprintf(e->out, " &middot; %d", count);
        fputs("</span></li>", e->out);
    }
    e->rows++;
}

static const char *col(sqlite3_stmt *st, int i) { const char *t = (const char *)sqlite3_column_text(st, i); return t ? t : ""; }
/* Editorially admitted research resources. These are external reading links,
 * not a copied toolkit, a licensed integration, or proof of any item claim.
 * A published CMS row is necessary, but does not assert a third-party tool
 * is free, open-source, endorsed or safe for a particular investigation. */
static int research_word(const char *text, const char *term) {
    size_t n = strlen(term);
    if (!text || !n || n > 64) return 0;
    for (const char *p = text; (p = strcasestr(p, term)); p++) {
        unsigned char before = p == text ? 0 : (unsigned char)p[-1];
        unsigned char after = (unsigned char)p[n];
        if ((!before || !isalnum(before)) && (!after || !isalnum(after))) return 1;
    }
    return 0;
}
static int research_fit(const char *terms, const char *title, const char *body) {
    char words[256], *save = NULL;
    if (!terms || !*terms) return 0;
    if (!strcmp(terms, "general")) return 1;
    snprintf(words, sizeof(words), "%s", terms);
    for (char *term = strtok_r(words, ",", &save); term; term = strtok_r(NULL, ",", &save)) {
        while (isspace((unsigned char)*term)) term++;
        size_t n = strlen(term);
        while (n && isspace((unsigned char)term[n - 1])) term[--n] = 0;
        if (n >= 3 && (research_word(title, term) || research_word(body, term))) return 1;
    }
    return 0;
}
static int research_url(const char *href) {
    if (!href || strncmp(href, "https://", 8) || strlen(href) > 400) return 0;
    const char *p = href + 8, *end = strpbrk(p, "/?#");
    size_t host = end ? (size_t)(end - p) : strlen(p);
    if (!host || host > 250 || memchr(p, '@', host) || memchr(p, ':', host)) return 0;
    for (; *href; href++) if ((unsigned char)*href <= 32 || (unsigned char)*href == 127 ||
                           *href == '"' || *href == '<' || *href == '>') return 0;
    return 1;
}
static int research_toolkits(Sess *s, FILE *out, const char *source, const char *key) {
    sqlite3_stmt *item = NULL, *rows = NULL;
    char title[768] = "", body[3000] = "", kind[64] = "", family[64] = "";
    int shown = 0;
    if (!s->jt || !source || !key || !*source || !*key) return 0;
    if (sqlite3_prepare_v2(s->db,
          "SELECT COALESCE(title,''),COALESCE(body,''),COALESCE(kind,''),COALESCE(family,'')"
          " FROM item WHERE source=?1 AND key=?2 LIMIT 1", -1, &item, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(item, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(item, 2, key, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(item) == SQLITE_ROW) {
        snprintf(title, sizeof(title), "%s", col(item, 0));
        snprintf(body, sizeof(body), "%s", col(item, 1));
        snprintf(kind, sizeof(kind), "%s", col(item, 2));
        snprintf(family, sizeof(family), "%s", col(item, 3));
    }
    sqlite3_finalize(item);
    if (strcmp(kind, "story") && strcmp(family, "news")) return 0;
    if (sqlite3_prepare_v2(s->db,
          "SELECT label,url,says,COALESCE(note,''),COALESCE(carries,'')"
          " FROM jt.internet_source WHERE namespace='aurekai-journalism-review'"
          " AND family='journalism-toolkit' AND status='published' AND state='reviewed'"
          " ORDER BY CASE WHEN carries='general' THEN 1 ELSE 0 END,label LIMIT 64",
          -1, &rows, NULL) != SQLITE_OK) return 0;
    while (sqlite3_step(rows) == SQLITE_ROW && shown < 5) {
        const char *label = col(rows, 0), *href = col(rows, 1);
        const char *description = col(rows, 2), *limitations = col(rows, 3);
        const char *terms = col(rows, 4);
        if (!*label || !research_url(href) || !research_fit(terms, title, body)) continue;
        if (!shown) {
            fputs("<section class='region region-depth research-guides' data-journalism-toolkits "
                  "aria-label='Research and verification resources'><h2 class='region-label'>"
                  "Research and verification tools</h2><p>Explore independent methods for checking "
                  "the story. Topic matches are suggestions, not fact-checks or endorsements.</p>"
                  "<ul class='more'>", out);
        }
        fputs("<li><a rel='noopener noreferrer nofollow' href='", out); esc(out, href);
        fputs("'>", out); esc(out, label); fputs("</a>", out);
        if (*description) { fputs("<span class='by'>", out); esc(out, description); fputs("</span>", out); }
        if (*limitations) { fputs("<small class='src'>", out); esc(out, limitations); fputs("</small>", out); }
        fputs("</li>", out);
        shown++;
    }
    sqlite3_finalize(rows);
    if (shown) fputs("</ul><p class='src'>Links open their original publishers. Public access "
                     "does not imply open-source licensing, reuse rights, current availability or "
                     "verified claims. Research ethically and check each provider's terms.</p></section>", out);
    return shown;
}

static void rows_feeds(Sess *s, Emit *e, int limit, const char *caption) {
    sqlite3_stmt *st = NULL;
    if (!s->fr || sqlite3_prepare_v2(s->db,
          "SELECT c.exact_address, COALESCE(NULLIF(c.title,''),''), COALESCE(c.material,''), r.host, COUNT(*) n"
          " FROM focus f CROSS JOIN fr.channel c ON c.census_source=f.source JOIN fr.root r ON r.root_id=c.root_id"
          " GROUP BY c.channel_id ORDER BY n DESC, c.exact_address LIMIT ?1", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char label[400];
        if (*col(st, 1)) snprintf(label, sizeof(label), "%s", col(st, 1));
        else if (*col(st, 2)) snprintf(label, sizeof(label), "%c%s from %s", toupper((unsigned char)col(st, 2)[0]), col(st, 2) + 1, col(st, 3));
        else snprintf(label, sizeof(label), "The feed at %s", col(st, 3));
        row(e, "feed", "/channels?channel=", col(st, 0), NULL, NULL, label, caption, sqlite3_column_int(st, 4));
    }
    sqlite3_finalize(st);
}

static void rows_publishers(Sess *s, Emit *e, int limit) {
    sqlite3_stmt *st = NULL;
    if (!s->fr || sqlite3_prepare_v2(s->db,
          "SELECT r.host, COALESCE(NULLIF(r.name,''),r.host), COUNT(*) n"
          " FROM focus f CROSS JOIN fr.channel c ON c.census_source=f.source JOIN fr.root r ON r.root_id=c.root_id"
          " GROUP BY r.host ORDER BY n DESC, r.host LIMIT ?1", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        /* A hosting platform's ledger name is one of the many shows it
         * hosts; the platform is named by its host instead. */
        int platform = platform_host(col(st, 0));
        row(e, "publisher", "/channels?root=", col(st, 0), NULL, NULL, platform ? col(st, 0) : col(st, 1), platform ? "Hosting platform" : "Publisher", sqlite3_column_int(st, 2));
    }
    sqlite3_finalize(st);
}

static void rows_readers(Sess *s, Emit *e, int limit) {
    sqlite3_stmt *st = NULL;
    if (!s->fr || sqlite3_prepare_v2(s->db,
          "SELECT c.protocol, COUNT(*) n FROM focus f CROSS JOIN fr.channel c ON c.census_source=f.source"
          " WHERE COALESCE(c.protocol,'')<>'' GROUP BY c.protocol ORDER BY n DESC LIMIT ?1", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW)
        row(e, "reader", "/channels?way=", col(st, 0), NULL, NULL, bf_detail_reader_words(col(st, 0)), "The tool that read it", sqlite3_column_int(st, 1));
    sqlite3_finalize(st);
}

static void rows_makers(Sess *s, Emit *e, int limit, const char *caption) {
    sqlite3_stmt *st = NULL;
    char sql[1024];
    snprintf(sql, sizeof(sql),
        "SELECT who, source, SUM(n) FROM ("
        " SELECT i.by who, i.source source, COUNT(*) n FROM focus f CROSS JOIN item i ON i.source=f.source AND i.key=f.key WHERE plausible(i.by) GROUP BY 1,2"
        "%s) GROUP BY who, source ORDER BY 3 DESC, who LIMIT ?1",
        s->dd ? " UNION ALL SELECT d.value, d.source, COUNT(*) FROM focus f CROSS JOIN dd.item_facet d ON d.source=f.source AND d.key=f.key WHERE d.facet='maker' GROUP BY 1,2" : "");
    if (sqlite3_prepare_v2(s->db, sql, -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW)
        row(e, "maker", "/people?who=", col(st, 0), "&source=", col(st, 1), col(st, 0), caption, sqlite3_column_int(st, 2));
    sqlite3_finalize(st);
}

static void rows_facet(Sess *s, Emit *e, const char *facet, int limit, const char *caption) {
    sqlite3_stmt *st = NULL;
    if (!s->dd || sqlite3_prepare_v2(s->db,
          "SELECT d.value, COUNT(*) n FROM focus f CROSS JOIN dd.item_facet d ON d.source=f.source AND d.key=f.key"
          " WHERE d.facet=?1 GROUP BY d.value ORDER BY n DESC, d.value LIMIT ?2", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_text(st, 1, facet, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, limit);
    while (sqlite3_step(st) == SQLITE_ROW)
        row(e, !strcmp(facet, "topic") ? "topic" : "mention", !strcmp(facet, "topic") ? "/channels?topic=" : "/channels?named=",
            col(st, 0), NULL, NULL, col(st, 0), caption, sqlite3_column_int(st, 1));
    sqlite3_finalize(st);
}

/* Publishers a piece links that this estate also reads: their own pages. */
static void rows_linked(Sess *s, Emit *e, int limit) {
    sqlite3_stmt *st = NULL;
    if (!s->dd || sqlite3_prepare_v2(s->db,
          "SELECT d.value, d.value, COUNT(*) n FROM focus f"
          " JOIN dd.item_facet d ON d.source=f.source AND d.key=f.key AND d.facet='linked_root'"
          " GROUP BY d.value ORDER BY n DESC, d.value LIMIT ?1", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW)
        row(e, "named", "/channels?root=", col(st, 0), NULL, NULL, col(st, 1), "Linked from the text", sqlite3_column_int(st, 2));
    sqlite3_finalize(st);
}

static void shape_row(Sess *s, Emit *e, const char *slug, const char *caption) {
    sqlite3_stmt *st = NULL;
    if (!s->fp || sqlite3_prepare_v2(s->db, "SELECT doctype, app FROM fp.frappe_program WHERE slug=?1", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_text(st, 1, slug, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        char cap[200];
        snprintf(cap, sizeof(cap), "%s the %s app", caption, col(st, 1));
        row(e, "app", "/s/record-kind/", slug, NULL, NULL, col(st, 0), cap, 0);
    }
    sqlite3_finalize(st);
}

static void rows_shapes_for(Sess *s, Emit *e, const char *what) {
    for (size_t i = 0; i < SHAPES; i++) if (!strcmp(shapes[i].what, what)) shape_row(s, e, shapes[i].slug, "How it would be filed in");
}

static void rows_shapes(Sess *s, Emit *e) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s->db, "SELECT DISTINCT i.kind, COALESCE(i.family,'') FROM focus f CROSS JOIN item i ON i.source=f.source AND i.key=f.key LIMIT 12", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    {
        char seen[SHAPES] = {0};
        while (sqlite3_step(st) == SQLITE_ROW) {
            char fam[96]; snprintf(fam, sizeof(fam), "family:%s", col(st, 1));
            for (size_t i = 0; i < SHAPES; i++)
                if (!seen[i] && (!strcmp(shapes[i].what, col(st, 0)) || !strcmp(shapes[i].what, fam))) {
                    seen[i] = 1; shape_row(s, e, shapes[i].slug, "How it would be filed in");
                }
        }
    }
    sqlite3_finalize(st);
}

static void rows_items(Sess *s, Emit *e, int limit, const char *caption) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s->db, "SELECT i.source, i.key, i.title FROM focus f CROSS JOIN item i ON i.source=f.source AND i.key=f.key"
                                  " WHERE i.title<>'' ORDER BY i.at DESC LIMIT ?1", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char p[300]; snprintf(p, sizeof(p), "/o/%s/", col(st, 0));
        row(e, "item", p, col(st, 1), NULL, NULL, col(st, 2), caption, 0);
    }
    sqlite3_finalize(st);
}

/* Pieces from other sources filed under the same topic or printing the same
 * name as this one.  A shared label, not a shared story. */
static void rows_alongside(Sess *s, Emit *e, const char *source, const char *key, int limit) {
    sqlite3_stmt *mine = NULL, *other = NULL; int shown = 0;
    char used[8][160]; int used_n = 0;
    if (!s->dd) return;
    /* Printed names first: they are more particular than a topic. */
    if (sqlite3_prepare_v2(s->db, "SELECT facet, value FROM dd.item_facet WHERE source=?1 AND key=?2 AND facet IN ('mention','topic')"
                                  " ORDER BY facet='topic', value LIMIT 16", -1, &mine, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(s->db, "SELECT i.source, i.key, i.title FROM dd.item_facet d JOIN item i ON i.source=d.source AND i.key=d.key"
                                  " WHERE d.facet=?1 AND d.value=?2 AND d.source<>?3 AND i.title<>'' LIMIT 8", -1, &other, NULL) != SQLITE_OK) {
        sqlite3_finalize(mine); sqlite3_finalize(other); return;
    }
    sqlite3_bind_text(mine, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(mine, 2, key, -1, SQLITE_TRANSIENT);
    while (shown < limit && sqlite3_step(mine) == SQLITE_ROW) {
        sqlite3_reset(other);
        sqlite3_bind_text(other, 1, col(mine, 0), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(other, 2, col(mine, 1), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(other, 3, source, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(other) == SQLITE_ROW) {
            int dup = 0;
            for (int i = 0; i < used_n; i++) if (!strcmp(used[i], col(other, 0))) dup = 1;
            if (dup) continue;
            if (used_n < 8) snprintf(used[used_n++], sizeof(used[0]), "%s", col(other, 0));
            char p[300], cap[300];
            snprintf(p, sizeof(p), "/o/%s/", col(other, 0));
            snprintf(cap, sizeof(cap), "Also %s %s", !strcmp(col(mine, 0), "topic") ? "filed under" : "names", col(mine, 1));
            row(e, "item", p, col(other, 1), NULL, NULL, col(other, 2), cap, 0);
            shown++;
            break;
        }
    }
    sqlite3_finalize(mine); sqlite3_finalize(other);
}

/* Other makers credited at the same source. */
static void rows_colleagues(Sess *s, Emit *e, const char *who, const char *source, int limit) {
    sqlite3_stmt *st = NULL; char sql[900];
    snprintf(sql, sizeof(sql),
        "SELECT who, SUM(n) FROM (SELECT by who, COUNT(*) n FROM item WHERE source=?1 AND by<>?2 AND plausible(by) GROUP BY by"
        "%s) GROUP BY who ORDER BY 2 DESC, who LIMIT ?3",
        s->dd ? " UNION ALL SELECT value, COUNT(*) FROM dd.item_facet WHERE source=?1 AND facet='maker' AND value<>?2 GROUP BY value" : "");
    if (sqlite3_prepare_v2(s->db, sql, -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return; }
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, who, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 3, limit);
    while (sqlite3_step(st) == SQLITE_ROW)
        row(e, "maker", "/people?who=", col(st, 0), "&source=", source, col(st, 0), "Also credited here", sqlite3_column_int(st, 1));
    sqlite3_finalize(st);
}

int bf_detail_continue(FILE *out, const BfDetailFocus *f) {
    Sess s; Emit e = {0}; char self[2048] = "";
    const char *boundary = "Each of these is read from what this estate already kept: the feed a piece came through, "
                           "the publisher behind it, the credits and topics the publisher printed. None of it is a "
                           "recommendation, an identity or a record kept anywhere else.";
    if (!out || !f || !f->a || !*f->a || !sess_open(&s)) return 0;
    e.out = out; e.desk = f->desk;
    snprintf(e.heading, sizeof(e.heading), "%s", f->desk ? "What this estate has read in this shape" : "Keep going");
    {   FILE *m = fmemopen(self, sizeof(self), "w");
        if (m) {
            switch (f->kind) {
            case BF_DETAIL_PERSON:  fputs("/people?who=", m); url(m, f->a); if (f->b) { fputs("&source=", m); url(m, f->b); } break;
            case BF_DETAIL_CHANNEL: fputs("/channels?channel=", m); url(m, f->a); break;
            case BF_DETAIL_ROOT:    fputs("/channels?root=", m); url(m, f->a); break;
            case BF_DETAIL_READER:  fputs("/channels?way=", m); url(m, f->a); break;
            case BF_DETAIL_TOPIC:   fputs("/channels?topic=", m); url(m, f->a); break;
            case BF_DETAIL_NAMED:   fputs("/channels?named=", m); url(m, f->a); break;
            case BF_DETAIL_RECORD:  fputs("/s/record-kind/", m); url(m, f->a); break;
            case BF_DETAIL_APP:     fputs("/s/apps/", m); url(m, f->a); break;
            case BF_DETAIL_ITEM:    fputs("/o/", m); url(m, f->a); fputs("/", m); url(m, f->b); break;
            }
            fputc(0, m); fclose(m);
        }
        e.self = self;
    }
    switch (f->kind) {
    case BF_DETAIL_ITEM:
        if (!f->b) break;
        fill(&s, "INSERT OR IGNORE INTO focus VALUES(?1,?2)", f->a, f->b);
        rows_feeds(&s, &e, 2, "The feed it came through");
        rows_publishers(&s, &e, 1);
        rows_makers(&s, &e, 4, "Credited on this piece");
        rows_readers(&s, &e, 1);
        rows_shapes(&s, &e);
        rows_facet(&s, &e, "topic", 8, "Filed under");
        rows_facet(&s, &e, "mention", 6, "Named in the text");
        rows_linked(&s, &e, 4);
        rows_alongside(&s, &e, f->a, f->b, 4);
        break;
    case BF_DETAIL_PERSON:
        if (!f->b) break;
        fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE by=?1 AND source=?2 ORDER BY at DESC LIMIT 200", f->a, f->b);
        if (s.dd) fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM dd.item_facet WHERE facet='maker' AND value=?1 AND source=?2 LIMIT 200", f->a, f->b);
        rows_feeds(&s, &e, 3, "Where their work came through");
        rows_publishers(&s, &e, 2);
        rows_readers(&s, &e, 1);
        rows_shapes_for(&s, &e, "maker");
        rows_shapes(&s, &e);
        rows_facet(&s, &e, "topic", 8, "Their work is filed under");
        rows_facet(&s, &e, "mention", 6, "Named in their work");
        rows_colleagues(&s, &e, f->a, f->b, 6);
        {   /* The same spelling at other sources is offered as a choice, not merged. */
            sqlite3_stmt *st = NULL;
            if (sqlite3_prepare_v2(s.db, "SELECT COUNT(DISTINCT source) FROM item WHERE by=?1", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, f->a, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_int(st, 0) > 1)
                    row(&e, "group", "/people?who=", f->a, NULL, NULL, f->a, "The same name printed at other sources", sqlite3_column_int(st, 0));
            }
            sqlite3_finalize(st);
        }
        break;
    case BF_DETAIL_CHANNEL:
        if (!s.fr) break;
        fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE source IN (SELECT census_source FROM fr.channel WHERE exact_address=?1 AND census_source<>'') ORDER BY at DESC LIMIT 200", f->a, NULL);
        rows_makers(&s, &e, 8, "Credited in this feed");
        rows_readers(&s, &e, 1);
        rows_shapes(&s, &e);
        rows_facet(&s, &e, "topic", 10, "Filed under here");
        rows_facet(&s, &e, "mention", 8, "Named here");
        rows_linked(&s, &e, 6);
        break;
    case BF_DETAIL_ROOT:
        if (!s.fr) break;
        fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE source IN (SELECT c.census_source FROM fr.channel c JOIN fr.root r ON r.root_id=c.root_id WHERE r.host=?1 AND c.census_source<>'') ORDER BY at DESC LIMIT 300", f->a, NULL);
        rows_items(&s, &e, 4, "Newest kept");
        rows_makers(&s, &e, 8, "Credited by this publisher");
        rows_readers(&s, &e, 2);
        rows_shapes_for(&s, &e, "publisher");
        rows_shapes(&s, &e);
        rows_facet(&s, &e, "topic", 10, "What it files under");
        rows_linked(&s, &e, 6);
        break;
    case BF_DETAIL_READER:
        if (!s.fr) break;
        fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE source IN (SELECT census_source FROM fr.channel WHERE protocol=?1 AND census_source<>'' ORDER BY last_contributed DESC LIMIT 60) ORDER BY at DESC LIMIT 300", f->a, NULL);
        rows_items(&s, &e, 4, "Read this way");
        rows_feeds(&s, &e, 6, "A feed read this way");
        rows_publishers(&s, &e, 4);
        rows_makers(&s, &e, 6, "Credited in what it read");
        rows_shapes(&s, &e);
        rows_facet(&s, &e, "topic", 8, "Filed under");
        break;
    case BF_DETAIL_TOPIC:
    case BF_DETAIL_NAMED:
        if (!s.dd) break;
        fill(&s, f->kind == BF_DETAIL_TOPIC ? "INSERT OR IGNORE INTO focus SELECT source,key FROM dd.item_facet WHERE facet='topic' AND value=?1 LIMIT 300"
                                            : "INSERT OR IGNORE INTO focus SELECT source,key FROM dd.item_facet WHERE facet='mention' AND value=?1 LIMIT 300", f->a, NULL);
        rows_feeds(&s, &e, 6, "A feed that carries it");
        rows_publishers(&s, &e, 4);
        rows_makers(&s, &e, 8, "Credited on it");
        rows_readers(&s, &e, 2);
        if (f->kind == BF_DETAIL_TOPIC) rows_shapes_for(&s, &e, "topic");
        rows_shapes(&s, &e);
        rows_facet(&s, &e, "topic", 8, "Filed alongside");
        rows_facet(&s, &e, "mention", 8, "Named alongside");
        break;
    case BF_DETAIL_APP:
    case BF_DETAIL_RECORD: {
        int person = 0, publisher = 0, topic = 0; char kinds[200] = ""; size_t kw = 0; const char *family = NULL;
        size_t app_len = strlen(f->a);
        for (size_t i = 0; i < SHAPES; i++) {
            if (f->kind == BF_DETAIL_RECORD ? strcmp(shapes[i].slug, f->a) != 0
                                            : strncmp(shapes[i].slug, f->a, app_len) || shapes[i].slug[app_len] != '-') continue;
            if (f->kind == BF_DETAIL_APP) shape_row(&s, &e, shapes[i].slug, "Fits what is read here, in");
            if (!strcmp(shapes[i].what, "maker")) person = 1;
            else if (!strcmp(shapes[i].what, "publisher")) publisher = 1;
            else if (!strcmp(shapes[i].what, "topic")) topic = 1;
            else if (!strncmp(shapes[i].what, "family:", 7)) family = shapes[i].what + 7;
            else kw += (size_t)snprintf(kinds + kw, sizeof(kinds) - kw, "%s%s", kw ? "," : "", shapes[i].what);
        }
        if (kinds[0]) fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE instr(','||?1||',', ','||kind||',')>0 AND at<=strftime('%s','now') ORDER BY at DESC LIMIT 200", kinds, NULL);
        if (family) fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE family=?1 ORDER BY at DESC LIMIT 200", family, NULL);
        if (person) fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE plausible(by) ORDER BY at DESC LIMIT 200", NULL, NULL);
        if (publisher) fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM item WHERE at<=strftime('%s','now') ORDER BY at DESC LIMIT 200", NULL, NULL);
        if (topic && s.dd) fill(&s, "INSERT OR IGNORE INTO focus SELECT source,key FROM dd.item_facet WHERE facet='topic' LIMIT 200", NULL, NULL);
        if (!kinds[0] && !family && !person && !publisher && !topic) break;
        boundary = "These are things this estate read that an application would keep in this shape. None of them "
                   "is a record of this kind, nothing was imported, and the application holds none of it.";
        if (person) rows_makers(&s, &e, 8, "A credited maker");
        if (publisher) rows_publishers(&s, &e, 8);
        if (topic) rows_facet(&s, &e, "topic", 10, "A topic as printed");
        rows_items(&s, &e, 6, "Read in this shape");
        rows_feeds(&s, &e, 4, "A feed that carries this shape");
        if (!publisher) rows_publishers(&s, &e, 3);
        if (!person) rows_makers(&s, &e, 4, "Credited on one");
        rows_readers(&s, &e, 2);
        break; }
    }
    row_close(&e, e.rows ? boundary : NULL);
    if (f->kind == BF_DETAIL_ITEM && f->b) e.rows += research_toolkits(&s, out, f->a, f->b);
    sess_close(&s);
    return e.rows;
}

int bf_detail_facet_page(FILE *out, const char *facet, const char *value, const char *route) {
    Sess s; sqlite3_stmt *st = NULL; int shown = 0, pieces = 0, sources = 0;
    int topic = !strcmp(facet, "topic");
    if (!out || !value || !*value || !sess_open(&s)) return 0;
    if (!s.dd) { sess_close(&s); return 0; }
    if (sqlite3_prepare_v2(s.db, "SELECT COUNT(*), COUNT(DISTINCT source) FROM dd.item_facet WHERE facet=?1 AND value=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, topic ? "topic" : "mention", -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, value, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { pieces = sqlite3_column_int(st, 0); sources = sqlite3_column_int(st, 1); }
    }
    sqlite3_finalize(st); st = NULL;
    if (!pieces) { sess_close(&s); return 0; }
    fputs("<section class='region region-depth' aria-label='", out); esc(out, topic ? "One topic" : "One printed name");
    fputs("' data-detail-facet='", out); esc(out, topic ? "topic" : "mention"); fputs("'><h2 class='region-label'>", out);
    fputs(glyph(topic ? "topic" : "mention"), out); esc(out, value);
    fprintf(out, "</h2><p class='source-count'>%s by %d piece%s from %d source%s &middot; <a href='", topic ? "Filed under this" : "Printed in",
            pieces, pieces == 1 ? "" : "s", sources, sources == 1 ? "" : "s");
    esc(out, route ? route : "/channels"); fputs("'>the whole ledger</a></p>", out);
    if (sqlite3_prepare_v2(s.db,
          "SELECT i.source, i.key, i.title, i.kind, COALESCE(NULLIF(i.publisher,''),i.by),"
          " COALESCE(strftime('%Y-%m-%d', NULLIF(i.at,0), 'unixepoch'),'')"
          " FROM dd.item_facet d JOIN item i ON i.source=d.source AND i.key=d.key"
          " WHERE d.facet=?1 AND d.value=?2 AND i.title<>'' ORDER BY i.at DESC LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, topic ? "topic" : "mention", -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, value, -1, SQLITE_TRANSIENT);
        fputs("<ol class='source-channels'>", out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            fputs("<li><div><a href='/o/", out); url(out, col(st, 0)); fputs("/", out); esc(out, col(st, 1)); fputs("'>", out);
            esc(out, col(st, 2)); fputs("</a><span class='by'>", out);
            if (*col(st, 4)) { fputs("<span>", out); esc(out, col(st, 4)); fputs("</span>", out); }
            if (*col(st, 3)) { fputs("<span>", out); esc(out, col(st, 3)); fputs("</span>", out); }
            if (*col(st, 5)) { fputs("<time>", out); esc(out, col(st, 5)); fputs("</time>", out); }
            fputs("</span></div></li>", out);
            shown++;
        }
        fputs("</ol>", out);
    }
    sqlite3_finalize(st);
    fputs("<p class='src'>", out);
    esc(out, topic ? "A topic here is a label each publisher printed on its own piece. Equal labels from different publishers are shown together; that does not make them one subject."
                   : "A printed name is a run of capitalised words as a publisher wrote them. Equal spellings are shown together; that does not make them one person, place or organisation.");
    fputs("</p></section>", out);
    sess_close(&s);
    {   BfDetailFocus focus = {topic ? BF_DETAIL_TOPIC : BF_DETAIL_NAMED, value, NULL, 0};
        shown += bf_detail_continue(out, &focus); }
    return shown;
}

int bf_detail_person_work(FILE *out, const char *who, const char *source) {
    Sess s; sqlite3_stmt *st = NULL; int shown = 0;
    if (!out || !who || !source || !sess_open(&s)) return 0;
    if (!s.dd) { sess_close(&s); return 0; }
    if (sqlite3_prepare_v2(s.db,
          "SELECT i.source, i.key, i.title, i.kind, COALESCE(i.publisher,''),"
          " COALESCE(strftime('%Y-%m-%d', NULLIF(i.at,0), 'unixepoch'),'')"
          " FROM dd.item_facet d JOIN item i ON i.source=d.source AND i.key=d.key"
          " WHERE d.facet='maker' AND d.value=?1 AND d.source=?2 AND i.title<>'' ORDER BY i.at DESC LIMIT 60", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_finalize(st); sess_close(&s); return 0;
    }
    sqlite3_bind_text(st, 1, who, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!shown) {
            fputs("<section class='region region-person people-dossier' aria-label='Source-bound public credit' data-source-dossier>"
                  "<div class='people-dossier-head'><p class='people-kicker'>", out); fputs(glyph("maker"), out);
            fputs("Credited maker</p><h2>", out); esc(out, who);
            fputs("</h2><p class='people-credit-source'>Credited by <strong>", out); esc(out, source);
            fputs("</strong> in the credit lines of its own feed.</p></div><ol class='people-work-list' aria-label='Their work at this source'>", out);
        }
        fputs("<li><a href='/o/", out); url(out, col(st, 0)); fputs("/", out); esc(out, col(st, 1)); fputs("'>", out); esc(out, col(st, 2));
        fputs("</a><p>", out);
        if (*col(st, 4)) { fputs("<span>", out); esc(out, col(st, 4)); fputs("</span>", out); }
        if (*col(st, 3)) { fputs("<span>", out); esc(out, col(st, 3)); fputs("</span>", out); }
        if (*col(st, 5)) { fputs("<time datetime='", out); esc(out, col(st, 5)); fputs("'>", out); esc(out, col(st, 5)); fputs("</time>", out); }
        fputs("</p></li>", out);
        shown++;
    }
    sqlite3_finalize(st);
    if (shown) fputs("</ol><p class='people-boundary' data-identity-boundary>Everything here carries this exact name in its "
                     "publisher's credit line at this exact source. It is publication evidence, not a profile, a verified "
                     "identity, consent, or an established relationship.</p></section>", out);
    sess_close(&s);
    return shown;
}

/* ============================================================== derivation */

/* Decode the entities feeds use, in place. */
static void decode(char *s) {
    char *w = s;
    for (char *p = s; *p;) {
        if (*p == '&') {
            if (!strncmp(p, "&amp;", 5)) { *w++ = '&'; p += 5; continue; }
            if (!strncmp(p, "&lt;", 4)) { *w++ = '<'; p += 4; continue; }
            if (!strncmp(p, "&gt;", 4)) { *w++ = '>'; p += 4; continue; }
            if (!strncmp(p, "&quot;", 6)) { *w++ = '"'; p += 6; continue; }
            if (!strncmp(p, "&apos;", 6) || !strncmp(p, "&#39;", 5)) { *w++ = '\''; p += p[1] == 'a' ? 6 : 5; continue; }
            if (p[1] == '#') {
                char *end; long c = p[2] == 'x' || p[2] == 'X' ? strtol(p + 3, &end, 16) : strtol(p + 2, &end, 10);
                if (*end == ';' && c > 0 && c < 0x110000) {
                    if (c < 0x80) *w++ = (char)c;
                    else if (c < 0x800) { *w++ = (char)(0xC0 | (c >> 6)); *w++ = (char)(0x80 | (c & 63)); }
                    else if (c < 0x10000) { *w++ = (char)(0xE0 | (c >> 12)); *w++ = (char)(0x80 | ((c >> 6) & 63)); *w++ = (char)(0x80 | (c & 63)); }
                    else { *w++ = (char)(0xF0 | (c >> 18)); *w++ = (char)(0x80 | ((c >> 12) & 63)); *w++ = (char)(0x80 | ((c >> 6) & 63)); *w++ = (char)(0x80 | (c & 63)); }
                    p = end + 1; continue;
                }
            }
        }
        *w++ = *p++;
    }
    *w = 0;
}

static void trim(char *s) {
    size_t n = strlen(s), a = 0, w = 0;
    while (a < n && isspace((unsigned char)s[a])) a++;
    for (size_t i = a; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (isspace(c)) { if (w && s[w - 1] != ' ') s[w++] = ' '; }
        else s[w++] = (char)c;
    }
    while (w && s[w - 1] == ' ') w--;
    s[w] = 0;
}

/* The text of the element starting at p (just after "<name"), CDATA unwrapped. */
static int element_text(const char *p, const char *end, const char *name, char *out, size_t cap) {
    const char *gt = memchr(p, '>', (size_t)(end - p)), *close;
    char tag[64]; size_t n = 0;
    if (!gt || gt[-1] == '/') return 0;
    snprintf(tag, sizeof(tag), "</%s", name);
    for (close = gt + 1; close < end && strncmp(close, tag, strlen(tag)); close++) {}
    if (close >= end) return 0;
    const char *a = gt + 1, *b = close;
    if (!strncmp(a, "<![CDATA[", 9)) { a += 9; const char *c = strstr(a, "]]>"); if (c && c < b) b = c; }
    n = (size_t)(b - a); if (n >= cap) n = cap - 1;
    memcpy(out, a, n); out[n] = 0;
    decode(out);
    return 1;
}

static int attr(const char *p, const char *end, const char *name, char *out, size_t cap) {
    const char *gt = memchr(p, '>', (size_t)(end - p));
    char key[48]; snprintf(key, sizeof(key), "%s=", name);
    for (const char *a = p; gt && a < gt; a++) {
        if (strncmp(a, key, strlen(key)) || (a > p && !isspace((unsigned char)a[-1]))) continue;
        char q = a[strlen(key)]; const char *v = a + strlen(key) + 1, *e;
        if (q != '"' && q != '\'') return 0;
        for (e = v; e < gt && *e != q; e++) {}
        size_t n = (size_t)(e - v); if (n >= cap) n = cap - 1;
        memcpy(out, v, n); out[n] = 0; decode(out);
        return 1;
    }
    return 0;
}

static void strip_tags(char *s) {
    char *w = s; int in = 0;
    for (char *p = s; *p; p++) { if (*p == '<') { in = 1; *w++ = ' '; } else if (*p == '>') in = 0; else if (!in) *w++ = *p; }
    *w = 0;
}

typedef struct { char *key, *href, *title, *body; long at; int taken; } Kept;

typedef struct {
    sqlite3 *out; sqlite3_stmt *put, *root;
    long facets, items, matched, bodies;
} Derive;

static void put(Derive *d, const char *source, const char *key, const char *facet, const char *value, const char *origin) {
    if (!value || !*value || strlen(value) > 120) return;
    sqlite3_reset(d->put);
    sqlite3_bind_text(d->put, 1, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(d->put, 2, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(d->put, 3, facet, -1, SQLITE_STATIC);
    sqlite3_bind_text(d->put, 4, value, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(d->put, 5, origin, -1, SQLITE_STATIC);
    if (sqlite3_step(d->put) == SQLITE_DONE && sqlite3_changes(d->out) > 0) d->facets++;
}

static const char *const stop[] = {"The","A","An","In","On","At","This","That","These","Those","It","He","She","We","They","I","You","But","And","Or",
    "For","With","After","Before","When","While","How","What","Why","Who","Where","Our","Your","His","Her","Its","Their","My","Last","Next",
    "Some","More","Most","From","By","As","If","Is","Are","Was","Were","Be","To","Of","Not","No","So","Then","Now","Here","There","Today","Also",
    "Plus","Listen","Watch","Read","Join","Subscribe","Click","Learn","Find","Get","See","Episode","Season","Part","Chapter","Mr","Mrs","Ms","Dr",
    "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday","January","February","March","April","May","June","July","August",
    "September","October","November","December",NULL};

/* Boilerplate every show and shop prints: a name of a page, not of anything. */
static int boilerplate(const char *name) {
    static const char *const words[] = {"Privacy","Policy","Notice","Podcasts","Podcast Choices","Show Notes","Terms","Cookie","Advertis",
        "Subscribe","Newsletter","Sign Up","Read More","Learn More","Click","Rights Reserved","Sponsor","Promo Code","Listen","Follow Us",NULL};
    for (int i = 0; words[i]; i++) if (strstr(name, words[i])) return 1;
    return 0;
}

static int stopword(const char *w, size_t n) {
    for (int i = 0; stop[i]; i++) if (strlen(stop[i]) == n && !strncmp(stop[i], w, n)) return 1;
    return 0;
}

/* Runs of two to four capitalised words in sentence-case prose. */
static void mentions(Derive *d, const char *source, const char *key, const char *text) {
    char buf[4096]; snprintf(buf, sizeof(buf), "%s", text ? text : "");
    decode(buf); strip_tags(buf);
    const char *p = buf; int sentence = 1, found = 0;
    const char *run_start = NULL, *run_end = NULL; int run_words = 0;
    while (*p && found < 8) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        const char *w = p; while (*p && !isspace((unsigned char)*p)) p++;
        size_t n = (size_t)(p - w), core = n;
        while (core && strchr(".,;:!?)\"'”’", w[core - 1])) core--;
        int ends = core < n && strchr(".!?", w[core]) != NULL;
        int ends_run = core < n;   /* punctuation closes a run */
        int cap = core >= 2 && isupper((unsigned char)w[0]) && islower((unsigned char)w[1]);
        for (size_t i = 1; cap && i < core; i++) if (!isalpha((unsigned char)w[i]) && w[i] != '-' && w[i] != '\'') cap = 0;
        if (cap && sentence && stopword(w, core)) cap = 0;
        if (cap && stopword(w, core) && !run_start) cap = 0;
        if (cap) { if (!run_start) run_start = w; run_end = w + core; run_words++; }
        if (!cap || ends_run || run_words == 4) {
            if (run_start && run_words >= 2 && run_words <= 4) {
                char name[160]; size_t len = (size_t)(run_end - run_start);
                if (len < sizeof(name)) { memcpy(name, run_start, len); name[len] = 0; if (!boilerplate(name)) put(d, source, key, "mention", name, "archive-text"); found++; }
            }
            run_start = NULL; run_words = 0;
        }
        sentence = ends || (!cap && n && strchr(":", w[n - 1]));
    }
}

static int platform_host(const char *h) {
    static const char *const p[] = {"youtube.com","youtu.be","twitter.com","x.com","facebook.com","instagram.com","linkedin.com","tiktok.com",
        "vimeo.com","bsky.app","mastodon.social","threads.net","flickr.com","doi.org","bit.ly","t.co","google.com","goo.gl","apple.com",
        "spotify.com","soundcloud.com","medium.com","github.com","podtrac.com","megaphone.fm","simplecast.com","libsyn.com","art19.com",
        "feedburner.com","pinterest.com","podcastchoices.com","adswizz.com","omnystudio.com","acast.com","megaphone.fm","pdst.fm","chartable.com","podtrac.com","linktr.ee","buymeacoffee.com","reddit.com","wikipedia.org","amazon.com","patreon.com","substack.com",NULL};
    for (int i = 0; p[i]; i++) { size_t a = strlen(h), b = strlen(p[i]); if (a >= b && !strcmp(h + a - b, p[i]) && (a == b || h[a - b - 1] == '.')) return 1; }
    return 0;
}

static void host_of(const char *u, char *out, size_t cap) {
    const char *p = strstr(u, "://"); size_t n = 0;
    out[0] = 0; if (!p) return; p += 3;
    if (!strncasecmp(p, "www.", 4)) p += 4;
    while (p[n] && p[n] != '/' && p[n] != ':' && p[n] != '?' && p[n] != '#' && n + 1 < cap) { out[n] = (char)tolower((unsigned char)p[n]); n++; }
    out[n] = 0;
}

static void linked_hosts(Derive *d, const char *source, const char *key, const char *html_text, const char *own) {
    char seen[16][128]; int k = 0;
    for (const char *p = html_text; (p = strstr(p, "href=")) && k < 16; p += 5) {
        char q = p[5], link[1024]; size_t n = 0; const char *s = p + 6;
        if (q != '"' && q != '\'') continue;
        while (s[n] && s[n] != q && n + 1 < sizeof(link)) { link[n] = s[n]; n++; }
        link[n] = 0;
        if (strncmp(link, "http", 4)) continue;
        char h[128]; host_of(link, h, sizeof(h));
        if (!h[0] || !strchr(h, '.') || platform_host(h) || (own[0] && (strstr(h, own) || strstr(own, h)))) continue;
        int dup = 0; for (int i = 0; i < k; i++) if (!strcmp(seen[i], h)) dup = 1;
        if (dup) continue;
        snprintf(seen[k++], sizeof(seen[0]), "%s", h);
        put(d, source, key, "linked", h, "retained-body");
        /* A linked host the ledger reads is a publisher with its own page. */
        if (d->root) {
            char www[140]; snprintf(www, sizeof(www), "www.%s", h);
            sqlite3_reset(d->root);
            sqlite3_bind_text(d->root, 1, h, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(d->root, 2, www, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(d->root) == SQLITE_ROW) put(d, source, key, "linked_root", (const char *)sqlite3_column_text(d->root, 0), "retained-body+ledger");
        }
    }
}

static void split_credit(Derive *d, const char *source, const char *key, char *credit) {
    char *open = strchr(credit, '(');      /* "mail@host (Name)" */
    if (strchr(credit, '@') && open) { char *close = strchr(open, ')'); if (close) { *close = 0; credit = open + 1; } }
    for (char *part = credit; part && *part;) {
        char *next = NULL, *c = strstr(part, " and "), *comma = strchr(part, ',');
        if (comma && (!c || comma < c)) { *comma = 0; next = comma + 1; }
        else if (c) { *c = 0; next = c + 5; }
        trim(part);
        if (bf_people_name_plausible(part)) put(d, source, key, "maker", part, "retained-body");
        part = next;
    }
}

/* One entry of a retained RSS or Atom body, matched to its kept item. */
static void entry(Derive *d, const char *source, const char *own, Kept *kept, int kept_n, const char *p, const char *end) {
    char link[1200] = "", title[600] = "", buf[8192];
    Kept *k = NULL;
    for (const char *t = p; t < end; t++) {
        if (*t != '<') continue;
        if (!strncmp(t, "<link", 5) && (t[5] == '>' || isspace((unsigned char)t[5]))) {
            if (!link[0] && !attr(t + 5, end, "href", link, sizeof(link))) element_text(t + 5, end, "link", link, sizeof(link));
            trim(link);
        } else if (!strncmp(t, "<title", 6) && !title[0]) { element_text(t + 6, end, "title", title, sizeof(title)); trim(title); }
    }
    /* One entry per kept piece.  A show that gives every episode its own
     * home page as the link must not pour all its episodes into one piece:
     * the link counts only when the titles agree too. */
    for (int i = 0; i < kept_n && !k; i++)
        if (!kept[i].taken && link[0] && !strcmp(kept[i].href, link) && (!title[0] || !kept[i].title[0] || !strcmp(kept[i].title, title))) k = &kept[i];
    for (int i = 0; i < kept_n && !k; i++) if (!kept[i].taken && title[0] && !strcmp(kept[i].title, title)) k = &kept[i];
    if (!k) return;
    k->taken = 1;
    d->matched++;
    for (const char *t = p; t < end; t++) {
        if (*t != '<') continue;
        if (!strncmp(t, "<category", 9)) {
            if (attr(t + 9, end, "term", buf, sizeof(buf)) || element_text(t + 9, end, "category", buf, sizeof(buf))) {
                trim(buf);
                if (strlen(buf) <= 60 && !strstr(buf, "://") && strspn(buf, "0123456789 -") != strlen(buf)) put(d, source, k->key, "topic", buf, "retained-body");
            }
        } else if (!strncmp(t, "<dc:subject", 11) && element_text(t + 11, end, "dc:subject", buf, sizeof(buf))) {
            trim(buf); if (strlen(buf) <= 60) put(d, source, k->key, "topic", buf, "retained-body");
        } else if (!strncmp(t, "<itunes:keywords", 16) && element_text(t + 16, end, "itunes:keywords", buf, sizeof(buf))) {
            int n = 0;
            for (char *w = strtok(buf, ","); w && n < 12; w = strtok(NULL, ","), n++) { trim(w); if (*w && strlen(w) <= 40) put(d, source, k->key, "topic", w, "retained-body"); }
        } else if (!strncmp(t, "<dc:creator", 11) && element_text(t + 11, end, "dc:creator", buf, sizeof(buf))) {
            trim(buf); split_credit(d, source, k->key, buf);
        } else if (!strncmp(t, "<itunes:author", 14) && element_text(t + 14, end, "itunes:author", buf, sizeof(buf))) {
            trim(buf); split_credit(d, source, k->key, buf);
        } else if (!strncmp(t, "<author", 7) && (t[7] == '>' || isspace((unsigned char)t[7]))) {
            const char *nm = strstr(t, "<name");
            if (nm && nm < end && element_text(nm + 5, end, "name", buf, sizeof(buf))) { trim(buf); split_credit(d, source, k->key, buf); }
            else if (element_text(t + 7, end, "author", buf, sizeof(buf)) && !strchr(buf, '<')) { trim(buf); split_credit(d, source, k->key, buf); }
        } else if (!strncmp(t, "<enclosure", 10) && attr(t + 10, end, "type", buf, sizeof(buf))) {
            trim(buf); if (strchr(buf, '/')) put(d, source, k->key, "media", buf, "retained-body");
        } else if ((!strncmp(t, "<description", 12) && element_text(t + 12, end, "description", buf, sizeof(buf))) ||
                   (!strncmp(t, "<content:encoded", 16) && element_text(t + 16, end, "content:encoded", buf, sizeof(buf))) ||
                   (!strncmp(t, "<content", 8) && (t[8] == '>' || isspace((unsigned char)t[8])) && element_text(t + 8, end, "content", buf, sizeof(buf))) ||
                   (!strncmp(t, "<summary", 8) && element_text(t + 8, end, "summary", buf, sizeof(buf)))) {
            linked_hosts(d, source, k->key, buf, own);
            mentions(d, source, k->key, buf);
        }
    }
}

static char *read_body(const char *dir, const char *source, size_t *len) {
    char path[PATH_MAX]; gzFile g; size_t cap = 1 << 20, n = 0; char *buf;
    *len = 0;
    snprintf(path, sizeof(path), "%s/%s.body", dir, source);
    if (!(g = gzopen(path, "rb"))) { snprintf(path, sizeof(path), "%s/%s.body.gz", dir, source); if (!(g = gzopen(path, "rb"))) return NULL; }
    if (!(buf = malloc(cap))) { gzclose(g); return NULL; }
    for (;;) {
        if (n + 65536 >= cap) { if (cap >= (64u << 20)) break; char *more = realloc(buf, cap *= 2); if (!more) break; buf = more; }
        int r = gzread(g, buf + n, (unsigned)(cap - n - 1));
        if (r <= 0) break;
        n += (size_t)r;
    }
    gzclose(g);
    buf[n] = 0; *len = n;
    return buf;
}

static char *dup_col(sqlite3_stmt *st, int i) { const char *t = (const char *)sqlite3_column_text(st, i); return strdup(t ? t : ""); }

int bf_detail_depth_derive(const char *archive, const char *bodies_dir, const char *out_db, FILE *report) {
    sqlite3 *in = bf_sqlite_open_readonly_path(archive, 5000), *fr = NULL;
    sqlite3_stmt *sources = NULL, *items = NULL, *own = NULL;
    Derive d = {0};
    if (!in || !out_db) { if (in) sqlite3_close(in); return 2; }
    if (sqlite3_open(out_db, &d.out) != SQLITE_OK) { sqlite3_close(in); sqlite3_close(d.out); return 2; }
    if (sqlite3_exec(d.out,
          "PRAGMA journal_mode=WAL;"
          "CREATE TABLE IF NOT EXISTS item_facet(source TEXT NOT NULL, key TEXT NOT NULL, facet TEXT NOT NULL, value TEXT NOT NULL,"
          " origin TEXT NOT NULL, PRIMARY KEY(source,key,facet,value)) WITHOUT ROWID;"
          "CREATE INDEX IF NOT EXISTS item_facet_value ON item_facet(facet,value);"
          "CREATE INDEX IF NOT EXISTS item_facet_source ON item_facet(source,facet,value);"
          "CREATE TABLE IF NOT EXISTS derivation(at TEXT NOT NULL, archive TEXT NOT NULL, bodies TEXT NOT NULL, items INTEGER, matched INTEGER, facets INTEGER);"
          "BEGIN;", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(d.out, "INSERT OR IGNORE INTO item_facet VALUES(?1,?2,?3,?4,?5)", -1, &d.put, NULL) != SQLITE_OK) {
        sqlite3_close(in); sqlite3_close(d.out); return 2;
    }
    fr = bf_sqlite_open_readonly_path(getenv("BONFYRE_SOURCE_FRONTIER_DB"), 5000);
    /* The ledger has no host index; a temporary one is built in memory. */
    if (fr && sqlite3_exec(fr, "CREATE TEMP TABLE hosts AS SELECT DISTINCT host FROM root; CREATE INDEX temp.hosts_host ON hosts(host)", NULL, NULL, NULL) == SQLITE_OK)
        sqlite3_prepare_v2(fr, "SELECT host FROM temp.hosts WHERE host=?1 OR host=?2 LIMIT 1", -1, &d.root, NULL);
    if (fr) sqlite3_prepare_v2(fr, "SELECT r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.census_source=?1 LIMIT 1", -1, &own, NULL);
    sqlite3_prepare_v2(in, "SELECT DISTINCT source FROM item ORDER BY source", -1, &sources, NULL);
    sqlite3_prepare_v2(in, "SELECT key, href, title, body, at FROM item WHERE source=?1", -1, &items, NULL);
    while (sources && items && sqlite3_step(sources) == SQLITE_ROW) {
        char source[160]; Kept *kept = NULL; int kn = 0, kc = 0; char host[160] = "";
        snprintf(source, sizeof(source), "%s", (const char *)sqlite3_column_text(sources, 0));
        sqlite3_reset(items); sqlite3_bind_text(items, 1, source, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(items) == SQLITE_ROW) {
            if (kn == kc) { Kept *more = realloc(kept, sizeof(*kept) * (size_t)(kc = kc ? kc * 2 : 64)); if (!more) break; kept = more; }
            Kept *k = &kept[kn++];
            k->key = dup_col(items, 0); k->href = dup_col(items, 1); k->title = dup_col(items, 2); k->body = dup_col(items, 3);
            k->at = (long)sqlite3_column_int64(items, 4); k->taken = 0;
            d.items++;
            if (k->at > 0) { char month[16]; time_t t = (time_t)k->at; struct tm tm; gmtime_r(&t, &tm); strftime(month, sizeof(month), "%Y-%m", &tm); put(&d, source, k->key, "month", month, "archive"); }
            mentions(&d, source, k->key, k->body);
        }
        if (own) { sqlite3_reset(own); sqlite3_bind_text(own, 1, source, -1, SQLITE_TRANSIENT); if (sqlite3_step(own) == SQLITE_ROW) { const char *h = (const char *)sqlite3_column_text(own, 0); snprintf(host, sizeof(host), "%s", h && !strncmp(h, "www.", 4) ? h + 4 : h ? h : ""); } }
        if (bodies_dir) {
            size_t len = 0; char *body = read_body(bodies_dir, source, &len);
            if (body) {
                d.bodies++;
                for (char *p = body; p && *p;) {
                    char *ri = strstr(p, "<item"), *ae = strstr(p, "<entry");
                    p = ri && (!ae || ri < ae) ? ri : ae;
                    if (!p) break;
                    int atom = !strncmp(p, "<entry", 6);
                    char *end = strstr(p, atom ? "</entry>" : "</item>");
                    if (!end) break;
                    if (isspace((unsigned char)p[atom ? 6 : 5]) || p[atom ? 6 : 5] == '>') entry(&d, source, host, kept, kn, p, end);
                    p = end + 1;
                }
                free(body);
            }
        }
        for (int i = 0; i < kn; i++) { free(kept[i].key); free(kept[i].href); free(kept[i].title); free(kept[i].body); }
        free(kept);
    }
    sqlite3_finalize(sources); sqlite3_finalize(items); sqlite3_finalize(own); sqlite3_finalize(d.put); sqlite3_finalize(d.root);
    {
        sqlite3_stmt *st = NULL; char at[32]; time_t now = time(NULL); struct tm tm; gmtime_r(&now, &tm); strftime(at, sizeof(at), "%Y-%m-%dT%H:%M:%SZ", &tm);
        if (sqlite3_prepare_v2(d.out, "INSERT INTO derivation VALUES(?1,?2,?3,?4,?5,?6)", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, at, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, archive, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, bodies_dir ? bodies_dir : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(st, 4, d.items); sqlite3_bind_int64(st, 5, d.matched); sqlite3_bind_int64(st, 6, d.facets);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
    }
    /* Readers open it read-only; a WAL store without its -shm file cannot be
     * read that way, so it is left in rollback mode. */
    sqlite3_exec(d.out, "COMMIT; ANALYZE; PRAGMA journal_mode=DELETE;", NULL, NULL, NULL);
    if (report) fprintf(report, "{\"items\":%ld,\"bodies\":%ld,\"entries_matched\":%ld,\"facets_written\":%ld}\n", d.items, d.bodies, d.matched, d.facets);
    sqlite3_close(d.out); sqlite3_close(in); if (fr) sqlite3_close(fr);
    return 0;
}

/* Per source: what the estate derived before the facets, and with them.
 *   before = item pages + credited makers (plausible bylines) + its feed page
 *            + its publisher page + kin and neighbour rows + kinds + families
 *   after  = before + distinct facet values (each a page or an entity)
 *            + facet rows (each an edge from a piece to that page)
 * A shared topic counts once per source that carries it. */
int bf_detail_depth_measure(const char *archive, const char *frontier, const char *depth_db, FILE *out) {
    sqlite3 *db = bf_sqlite_open_readonly_path(archive, 5000); sqlite3_stmt *st = NULL;
    char sql[4096];
    if (!db) return 2;
    sqlite3_create_function(db, "plausible", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, plausible_fn, NULL, NULL);
    {   char uri[PATH_MAX + 32]; sqlite3_stmt *a = NULL;
        const char *paths[2] = {frontier, depth_db}, *names[2] = {"fr", "dd"};
        for (int i = 0; i < 2; i++) {
            if (!paths[i] || bf_sqlite_readonly_uri(uri, sizeof(uri), paths[i]) >= (int)sizeof(uri)) continue;
            snprintf(sql, sizeof(sql), "ATTACH DATABASE ?1 AS %s", names[i]);
            if (sqlite3_prepare_v2(db, sql, -1, &a, NULL) == SQLITE_OK) { sqlite3_bind_text(a, 1, uri, -1, SQLITE_TRANSIENT); sqlite3_step(a); }
            sqlite3_finalize(a); a = NULL;
        }
    }
    int dd = has_table(db, "dd", "item_facet"), fr = has_table(db, "fr", "channel");
    snprintf(sql, sizeof(sql),
        "WITH s AS (SELECT source, COUNT(*) items, COUNT(DISTINCT CASE WHEN plausible(by) THEN by END) makers,"
        "  COUNT(DISTINCT kind) kinds, COUNT(DISTINCT family) families FROM item GROUP BY source)"
        " SELECT s.source, s.items, s.makers, s.kinds + s.families,"
        "  %s, %s,"
        "  (SELECT COUNT(*) FROM kin WHERE address >= 'o/'||s.source||'/' AND address < 'o/'||s.source||'0')"
        "  + (SELECT COUNT(*) FROM neighbour WHERE address >= 'o/'||s.source||'/' AND address < 'o/'||s.source||'0'),"
        "  %s, %s"
        " FROM s ORDER BY s.items DESC",
        fr ? "(SELECT COUNT(*) FROM fr.channel c WHERE c.census_source=s.source)" : "0",
        fr ? "(SELECT COUNT(DISTINCT c.root_id) FROM fr.channel c WHERE c.census_source=s.source)" : "0",
        dd ? "(SELECT COUNT(*) FROM (SELECT DISTINCT facet, value FROM dd.item_facet d WHERE d.source=s.source))" : "0",
        dd ? "(SELECT COUNT(*) FROM dd.item_facet d WHERE d.source=s.source)" : "0");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) { fprintf(stderr, "%s\n", sqlite3_errmsg(db)); sqlite3_close(db); return 2; }
    fputs("source\titems\tmakers\tfacets_before\tfeed_pages\tpublisher_pages\trelations_before\tbefore\tnew_values\tnew_edges\tafter\tmultiplier\n", out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        long items = sqlite3_column_int64(st, 1), makers = sqlite3_column_int64(st, 2), facets = sqlite3_column_int64(st, 3);
        long feeds = sqlite3_column_int64(st, 4) ? 1 : 0, pubs = sqlite3_column_int64(st, 5) ? 1 : 0, rel = sqlite3_column_int64(st, 6);
        long values = sqlite3_column_int64(st, 7), edges = sqlite3_column_int64(st, 8);
        long before = items + makers + facets + feeds + pubs + rel, after = before + values + edges;
        fprintf(out, "%s\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%.2f\n", col(st, 0), items, makers, facets, feeds, pubs, rel,
                before, values, edges, after, before ? (double)after / (double)before : 0.0);
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}
