/* Walking the source ledger.
 *
 * The channels page shows the publishers whose material a route is already
 * carrying.  That is the top of a lattice the estate keeps thousands of rows
 * deep: one publisher addresses many protocol families, each family many exact
 * channels, most of them listed rather than read.  Without a way in, the depth
 * is invisible and the page is a list of eight names.
 *
 * This renders three walkable levels, each a bounded page with its own address:
 *
 *   /channels?roots=1           the publishers, a page at a time
 *   /channels?root=<host>       one publisher's families and channels
 *   /channels?root=<host>&family=<protocol>   every channel in that family
 *
 * Presentation only.  Nothing here admits a channel, asks a source for
 * anything, ranks publishers or claims a listed endpoint works: each row says
 * what standing that channel actually has in the ledger.
 */
#define _POSIX_C_SOURCE 200809L
#include "source_depth_projection.h"
#include "bf_sqlite_path.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include "bf_lex.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>
#include "estate_reference.h"

static void esc(FILE *out, const char *s) {
    bf_lex_html_emit(out, s);
}

/* A query value this page writes into its own links. */
static void url(FILE *out, const char *s) {
    static const char *hex = "0123456789ABCDEF";
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    for (; *p; p++) {
        if ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
            *p == '-' || *p == '.' || *p == '_' || *p == '~') fputc(*p, out);
        else { fputc('%', out); fputc(hex[*p >> 4], out); fputc(hex[*p & 15], out); }
    }
}

/* What a channel's state means to a person, in this estate's own words. */
static const char *standing(const char *state, const char *refusal) {
    if (refusal && *refusal) return "refused, with its reason";
    if (!state) return "known";
    if (!strcmp(state, "active")) return "read, and shown on a page";
    if (!strcmp(state, "admitted")) return "read here";
    if (!strcmp(state, "eligible")) return "answered, waiting to be read";
    if (!strcmp(state, "waiting_credential")) return "waiting for a key";
    if (!strcmp(state, "shadow")) return "named by something this estate retained";
    return "listed";
}

static sqlite3 *open_ledger(const char *db_path) {
    return bf_sqlite_open_readonly_path(db_path, 1500);
}

static void pager(FILE *out, const char *route, const char *host, const char *family,
                  unsigned offset, unsigned page_size, int shown, int total) {
    if (offset) {
        unsigned back = offset > page_size ? offset - page_size : 0;
        fputs("<a class='source-page' href='", out); esc(out, route);
        fputs(host ? "?root=" : "?roots=1", out);
        if (host) url(out, host);
        if (family && *family) { fputs("&amp;family=", out); url(out, family); }
        fprintf(out, "&amp;from=%u'>Back</a>", back);
    }
    if (total > (int)(offset + (unsigned)shown)) {
        fputs("<a class='source-page' href='", out); esc(out, route);
        fputs(host ? "?root=" : "?roots=1", out);
        if (host) url(out, host);
        if (family && *family) { fputs("&amp;family=", out); url(out, family); }
        fprintf(out, "&amp;from=%u'>More (%d left)</a>", offset + (unsigned)shown, total - (int)(offset + (unsigned)shown));
    }
}

int bf_source_depth_roots(FILE *out, const char *db_path, const char *route,
                          const char *needle, unsigned offset, unsigned page_size) {
    sqlite3 *db = open_ledger(db_path); sqlite3_stmt *st = NULL; int shown = 0, total = 0;
    if (!db) return 0;
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM root r WHERE (?1='' OR r.host LIKE '%'||?1||'%' OR r.name LIKE '%'||?1||'%')",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db,
            "WITH matched AS (SELECT root_id,host,name,identity FROM root"
            " WHERE (?1='' OR host LIKE '%'||?1||'%' OR name LIKE '%'||?1||'%')) ,"
            " counts AS (SELECT c.root_id,COUNT(*) AS channels,COUNT(DISTINCT c.protocol) AS families,"
            " SUM(c.state IN ('admitted','active')) AS readable"
            " FROM channel c JOIN matched m ON m.root_id=c.root_id GROUP BY c.root_id)"
            " SELECT m.host,COALESCE(NULLIF(m.name,''),m.host),"
            " COALESCE(counts.channels,0),COALESCE(counts.families,0),COALESCE(counts.readable,0),"
            " COALESCE(m.identity,'') FROM matched m LEFT JOIN counts ON counts.root_id=m.root_id"
            " ORDER BY COALESCE(counts.channels,0) DESC,m.host"
            " LIMIT ?2 OFFSET ?3", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 0; }
    sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, (int)page_size);
    sqlite3_bind_int(st, 3, (int)offset);
    fputs("<section class='region region-depth' aria-label='Publishers'>"
          "<h2 class='region-label'>Every publisher this estate has listed</h2>", out);
    fprintf(out, "<p class='source-count'>%d publishers. A publisher is one origin; what it publishes is below it.</p><ol class='source-roots'>", total);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *host = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        int channels = sqlite3_column_int(st, 2), families = sqlite3_column_int(st, 3), read = sqlite3_column_int(st, 4);
        const char *identity = (const char *)sqlite3_column_text(st, 5);
        fputs("<li><a href='", out); esc(out, route); fputs("?root=", out); url(out, host); fputs("'>", out);
        esc(out, name); fputs("</a><span class='by'>", out);
        fprintf(out, "<span>%d channel%s</span><span>%d way%s in</span>", channels, channels == 1 ? "" : "s", families, families == 1 ? "" : "s");
        if (read) fprintf(out, "<span>%d read here</span>", read);
        { char own[600]; if (estate_publisher_link(NULL, host, identity, own, sizeof(own))) { fputs("<a href='", out); esc(out, own); fputs("'>Its page</a>", out); } }
        fputs("</span></li>", out);
        shown++;
    }
    fputs("</ol>", out);
    pager(out, route, NULL, NULL, offset, page_size, shown, total);
    fputs("<p class='src'>A listed channel is an address this estate knows of. It is not a source it reads, "
          "and nothing here says the endpoint answers.</p></section>", out);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

int bf_source_depth_emit(FILE *out, const char *db_path, const char *host,
                         const char *family, const char *route,
                         unsigned offset, unsigned page_size) {
    sqlite3 *db = open_ledger(db_path); sqlite3_stmt *st = NULL;
    char name[256] = "", identity[64] = "";
    int shown = 0, total = 0;
    if (!db || !host || !*host) { if (db) sqlite3_close(db); return 0; }
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(NULLIF(name,''), host), COALESCE(identity,'') FROM root WHERE host=?1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(name, sizeof(name), "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(identity, sizeof(identity), "%s", (const char *)sqlite3_column_text(st, 1));
        }
    }
    sqlite3_finalize(st); st = NULL;
    if (!name[0]) { sqlite3_close(db); return 0; }

    fputs("<section class='region region-depth' aria-label='One publisher'><h2 class='region-label'>", out);
    esc(out, name);
    fputs("</h2><p class='source-count'><a href='", out); esc(out, route); fputs("?roots=1'>All publishers</a>", out);
    { char own[600]; if (estate_publisher_link(NULL, host, identity, own, sizeof(own))) { fputs(" &middot; <a href='", out); esc(out, own); fputs("'>Its page</a>", out); } }
    fputs("</p>", out);

    /* The ways in, with how much sits behind each. */
    if (!family || !*family) {
        if (sqlite3_prepare_v2(db,
                "SELECT c.protocol, COUNT(*), SUM(c.state IN ('admitted','active'))"
                " FROM channel c JOIN root r ON r.root_id=c.root_id WHERE r.host=?1"
                " GROUP BY c.protocol ORDER BY COUNT(*) DESC", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
            fputs("<ol class='source-ways'>", out);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *protocol = (const char *)sqlite3_column_text(st, 0);
                int count = sqlite3_column_int(st, 1), read = sqlite3_column_int(st, 2);
                fputs("<li><a href='", out); esc(out, route); fputs("?root=", out); url(out, host);
                fputs("&amp;family=", out); url(out, protocol); fputs("'>", out); esc(out, protocol);
                fprintf(out, "</a><span class='by'><span>%d channel%s</span>", count, count == 1 ? "" : "s");
                if (read) fprintf(out, "<span>%d read here</span>", read);
                fputs("</span></li>", out);
            }
            fputs("</ol>", out);
        }
        sqlite3_finalize(st); st = NULL;
    }

    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM channel c JOIN root r ON r.root_id=c.root_id"
                               " WHERE r.host=?1 AND (?2='' OR c.protocol=?2)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, family ? family : "", -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); st = NULL;

    if (sqlite3_prepare_v2(db,
            "SELECT c.exact_address, c.protocol, COALESCE(NULLIF(c.title,''),''), c.state, COALESCE(c.refusal,''),"
            " COALESCE(c.census_source,''), COALESCE(c.material,''), c.contributions"
            " FROM channel c JOIN root r ON r.root_id=c.root_id"
            " WHERE r.host=?1 AND (?2='' OR c.protocol=?2)"
            " ORDER BY (c.state IN ('admitted','active')) DESC, c.contributions DESC, c.first_seen"
            " LIMIT ?3 OFFSET ?4", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, family ? family : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, (int)page_size);
        sqlite3_bind_int(st, 4, (int)offset);
        fputs("<ol class='source-channels'>", out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *address = (const char *)sqlite3_column_text(st, 0);
            const char *protocol = (const char *)sqlite3_column_text(st, 1);
            const char *title = (const char *)sqlite3_column_text(st, 2);
            const char *state = (const char *)sqlite3_column_text(st, 3);
            const char *refusal = (const char *)sqlite3_column_text(st, 4);
            const char *source = (const char *)sqlite3_column_text(st, 5);
            const char *material = (const char *)sqlite3_column_text(st, 6);
            int contributions = sqlite3_column_int(st, 7);
            fputs("<li><div><a href='", out); esc(out, route); fputs("?channel=", out); url(out, address);
            fputs("'>", out);
            esc(out, title && *title ? title : address);
            fputs("</a><span class='by'><span class='channel-state'>", out); esc(out, standing(state, refusal));
            fputs("</span>", out);
            if (protocol && *protocol) { fputs("<span>", out); esc(out, protocol); fputs("</span>", out); }
            if (material && *material) { fputs("<span>", out); esc(out, material); fputs("</span>", out); }
            if (contributions > 0) fprintf(out, "<span>%d shown</span>", contributions);
            if (source && *source) { fputs("<span>read as ", out); esc(out, source); fputs("</span>", out); }
            fputs("</span>", out);
            if (refusal && *refusal) { fputs("<p class='channel-refusal'>", out); esc(out, refusal); fputs("</p>", out); }
            fputs("</div></li>", out);
            shown++;
        }
        fputs("</ol>", out);
    }
    sqlite3_finalize(st);
    pager(out, route, host, family, offset, page_size, shown, total);
    fprintf(out, "<p class='src'>%d channel%s at this publisher. A channel's standing is what this estate has done with it, "
                 "never a judgement of what it publishes.</p></section>", total, total == 1 ? "" : "s");
    sqlite3_close(db);
    return shown;
}

/* One count, asked plainly. */
static int one_count(sqlite3 *db, const char *sql, const char *bind) {
    sqlite3_stmt *st = NULL; int n = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    if (bind) sqlite3_bind_text(st, 1, bind, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return n;
}

/* A row of counted things, each a way further in. */
static void counted_list(FILE *out, sqlite3 *db, const char *sql, const char *route,
                         const char *param, const char *label, const char *nothing) {
    sqlite3_stmt *st = NULL; int rows = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return;
    fputs("<div class='ledger-cut'><h3>", out); esc(out, label); fputs("</h3><ol class='source-ways'>", out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0);
        int count = sqlite3_column_int(st, 1);
        int read = sqlite3_column_count(st) > 2 ? sqlite3_column_int(st, 2) : 0;
        if (!name || !*name) continue;
        fputs("<li>", out);
        if (param) {
            fputs("<a href='", out); esc(out, route); fputs("?", out); esc(out, param); fputs("=", out);
            url(out, name); fputs("'>", out); esc(out, name); fputs("</a>", out);
        } else { fputs("<span>", out); esc(out, name); fputs("</span>", out); }
        fprintf(out, "<span class='by'><span>%d</span>", count);
        if (read) fprintf(out, "<span>%d read</span>", read);
        fputs("</span></li>", out);
        rows++;
    }
    if (!rows && nothing) { fputs("<li><span class='by'>", out); esc(out, nothing); fputs("</span></li>", out); }
    fputs("</ol></div>", out);
    sqlite3_finalize(st);
}

int bf_source_depth_index(FILE *out, const char *db_path, const char *route, const char *needle) {
    sqlite3 *db = open_ledger(db_path);
    int roots, channels, listed, read, refused, waiting, families;
    if (!db) return 0;
    roots = one_count(db, "SELECT COUNT(*) FROM root", NULL);
    channels = one_count(db, "SELECT COUNT(*) FROM channel", NULL);
    read = one_count(db, "SELECT COUNT(*) FROM channel WHERE state IN ('admitted','active')", NULL);
    refused = one_count(db, "SELECT COUNT(*) FROM channel WHERE COALESCE(refusal,'')<>''", NULL);
    waiting = one_count(db, "SELECT COUNT(*) FROM channel WHERE state='waiting_credential'", NULL);
    families = one_count(db, "SELECT COUNT(DISTINCT protocol) FROM channel", NULL);
    listed = channels - read;

    fputs("<section class='region region-depth' aria-label='The ledger'>"
          "<h2 class='region-label'>Everything this estate has listed</h2>", out);
    fputs("<p class='source-count'>", out);
    fprintf(out, "%d publishers, %d channels, %d ways in. %d are read here; %d are listed and unread.",
            roots, channels, families, read, listed);
    if (waiting) fprintf(out, " %d wait on a key that has not been given.", waiting);
    if (refused) fprintf(out, " %d were refused, each with the reason kept.", refused);
    fputs("</p>", out);

    fputs("<form class='ledger-find' method='get' action='", out); esc(out, route);
    fputs("'><label>Find a publisher <input type='search' name='q' value='", out);
    esc(out, needle ? needle : ""); fputs("' placeholder='a name, or a host'></label>"
          "<button type='submit'>Look</button></form>", out);

    fputs("<div class='ledger-cuts'>", out);
    counted_list(out, db,
        "SELECT protocol, COUNT(*), SUM(state IN ('admitted','active')) FROM channel"
        " GROUP BY protocol ORDER BY COUNT(*) DESC LIMIT 14",
        route, "way", "The ways in", "nothing listed yet");
    counted_list(out, db,
        "SELECT material, COUNT(*), SUM(state IN ('admitted','active')) FROM channel"
        " WHERE COALESCE(material,'')<>'' GROUP BY material ORDER BY COUNT(*) DESC LIMIT 14",
        route, "publishes", "What they publish", "nothing measured yet");
    counted_list(out, db,
        "SELECT state, COUNT(*) FROM channel GROUP BY state ORDER BY COUNT(*) DESC",
        route, "standing", "Where each one stands", NULL);
    counted_list(out, db,
        "SELECT refusal, COUNT(*) FROM channel WHERE COALESCE(refusal,'')<>''"
        " GROUP BY refusal ORDER BY COUNT(*) DESC LIMIT 8",
        route, NULL, "Why the estate said no", "nothing refused");
    fputs("</div>", out);

    fputs("<p class='src'>A listed channel is an address this estate knows of. It is not a source it reads, "
          "and nothing here says the endpoint answers.</p></section>", out);
    sqlite3_close(db);
    return roots;
}

int bf_source_depth_channel(FILE *out, const char *db_path, const char *address,
                            const char *route, const char *archive_path) {
    sqlite3 *db = open_ledger(db_path); sqlite3_stmt *st = NULL;
    char host[256] = "", source[128] = "";
    int shown = 0;
    if (!db || !address || !*address) { if (db) sqlite3_close(db); return 0; }
    if (sqlite3_prepare_v2(db,
            "SELECT r.host, COALESCE(NULLIF(r.name,''),r.host), c.protocol, COALESCE(c.title,''), c.state,"
            " COALESCE(c.refusal,''), COALESCE(c.census_source,''), COALESCE(c.material,''), c.contributions,"
            " COALESCE(c.truth_level,''), COALESCE(c.discovered_from,''), COALESCE(c.first_seen,''),"
            " COALESCE(c.last_seen,''), COALESCE(c.last_contributed,''), COALESCE(c.cadence,''),"
            " COALESCE(c.credential,''), COALESCE(c.rights,''), c.exact_address,"
            " (SELECT COALESCE(carries,'') FROM channel c2 WHERE c2.channel_id=c.channel_id)"
            " FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.exact_address=?1 LIMIT 1",
            -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 0; }
    sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); sqlite3_close(db); return 0; }
    {
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *protocol = (const char *)sqlite3_column_text(st, 2);
        const char *title = (const char *)sqlite3_column_text(st, 3);
        const char *state = (const char *)sqlite3_column_text(st, 4);
        const char *refusal = (const char *)sqlite3_column_text(st, 5);
        const char *census = (const char *)sqlite3_column_text(st, 6);
        const char *material = (const char *)sqlite3_column_text(st, 7);
        int contributions = sqlite3_column_int(st, 8);
        const char *truth = (const char *)sqlite3_column_text(st, 9);
        const char *from = (const char *)sqlite3_column_text(st, 10);
        const char *first = (const char *)sqlite3_column_text(st, 11);
        const char *last = (const char *)sqlite3_column_text(st, 12);
        const char *gave = (const char *)sqlite3_column_text(st, 13);
        const char *cadence = (const char *)sqlite3_column_text(st, 14);
        const char *credential = (const char *)sqlite3_column_text(st, 15);
        const char *rights = (const char *)sqlite3_column_text(st, 16);
        snprintf(host, sizeof(host), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(source, sizeof(source), "%s", census ? census : "");

        fputs("<section class='region region-depth' aria-label='One channel'><h2 class='region-label'>", out);
        esc(out, title && *title ? title : address);
        fputs("</h2><p class='source-count'><a href='", out); esc(out, route); fputs("?root=", out); url(out, host);
        fputs("'>", out); esc(out, name); fputs("</a> &middot; <a href='", out); esc(out, route);
        fputs("'>the whole ledger</a></p>", out);

        fputs("<dl class='channel-standing'>", out);
        fputs("<dt>Standing</dt><dd>", out); esc(out, standing(state, refusal)); fputs("</dd>", out);
        if (protocol && *protocol) { fputs("<dt>Read as</dt><dd>", out); esc(out, protocol); fputs("</dd>", out); }
        if (material && *material) { fputs("<dt>Publishes</dt><dd>", out); esc(out, material); fputs("</dd>", out); }
        {   /* What a reading of its own body actually found beside that. */
            const char *carries = sqlite3_column_count(st) > 18 ? (const char *)sqlite3_column_text(st, 18) : NULL;
            if (carries && *carries) {
                fputs("<dt>Also carries</dt><dd>", out); esc(out, carries);
                fputs(" <span class='src'>measured from what it sent</span></dd>", out);
            }
        }
        if (truth && *truth) { fputs("<dt>How this address is known</dt><dd>", out); esc(out, truth); fputs("</dd>", out); }
        if (from && *from) { fputs("<dt>Found through</dt><dd>", out); esc(out, from); fputs("</dd>", out); }
        if (first && *first) { fputs("<dt>First listed</dt><dd>", out); esc(out, first); fputs("</dd>", out); }
        if (last && *last) { fputs("<dt>Last looked at</dt><dd>", out); esc(out, last); fputs("</dd>", out); }
        if (gave && *gave) { fputs("<dt>Last gave something</dt><dd>", out); esc(out, gave); fputs("</dd>", out); }
        if (cadence && *cadence) { fputs("<dt>Asked no more often than</dt><dd>", out); esc(out, cadence); fputs("</dd>", out); }
        if (credential && *credential) { fputs("<dt>Needs a key from</dt><dd>", out); esc(out, credential); fputs("</dd>", out); }
        if (rights && *rights) { fputs("<dt>Terms it states</dt><dd>", out); esc(out, rights); fputs("</dd>", out); }
        if (contributions > 0) { fprintf(out, "<dt>Things shown from it</dt><dd>%d</dd>", contributions); }
        fputs("<dt>Address</dt><dd><a rel='nofollow' href='", out); esc(out, address); fputs("'>", out);
        esc(out, address); fputs("</a></dd></dl>", out);
        if (refusal && *refusal) { fputs("<p class='channel-refusal'>", out); esc(out, refusal); fputs("</p>", out); }
    }
    sqlite3_finalize(st); st = NULL;
    sqlite3_close(db);

    /* What the estate actually kept from it: the floor of the lattice. */
    if (source[0] && archive_path && *archive_path) {
        sqlite3 *archive = bf_sqlite_open_readonly_path(archive_path, 1500);
        if (archive) {
            if (sqlite3_prepare_v2(archive,
                    "SELECT key, title, kind, COALESCE(first_read,'') FROM item WHERE source=?1"
                    " ORDER BY at DESC LIMIT 24", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
                fputs("<h3>What this estate kept from it</h3><ol class='source-channels'>", out);
                while (sqlite3_step(st) == SQLITE_ROW) {
                    const char *key = (const char *)sqlite3_column_text(st, 0);
                    const char *title = (const char *)sqlite3_column_text(st, 1);
                    const char *kind = (const char *)sqlite3_column_text(st, 2);
                    const char *when = (const char *)sqlite3_column_text(st, 3);
                    fputs("<li><div><a href='/o/", out); url(out, source); fputs("/", out); esc(out, key);
                    fputs("'>", out); esc(out, title && *title ? title : key); fputs("</a><span class='by'>", out);
                    if (kind && *kind) { fputs("<span>", out); esc(out, kind); fputs("</span>", out); }
                    if (when && *when) { fputs("<span>kept ", out); esc(out, when); fputs("</span>", out); }
                    fputs("</span></div></li>", out);
                    shown++;
                }
                fputs("</ol>", out);
                sqlite3_finalize(st);
                if (!shown) fputs("<p class='src'>Nothing from this channel is kept yet.</p>", out);
            }
            sqlite3_close(archive);
        }
    }
    fputs("<p class='src'>A channel's standing is what this estate has done with it, never a judgement of "
          "what it publishes. What was kept is what this estate read at a time, not a claim that the "
          "channel still publishes it.</p></section>", out);
    return 1;
}

int bf_source_depth_slice(FILE *out, const char *db_path, const char *route,
                          const char *way, const char *publishes, const char *state,
                          unsigned offset, unsigned page_size) {
    sqlite3 *db = open_ledger(db_path); sqlite3_stmt *st = NULL;
    int shown = 0, total = 0;
    const char *where = " WHERE (?1='' OR c.protocol=?1) AND (?2='' OR c.material=?2) AND (?3='' OR c.state=?3)";
    char sql[1024];
    if (!db) return 0;
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM channel c%s", where);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, way ? way : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, publishes ? publishes : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, state ? state : "", -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); st = NULL;
    snprintf(sql, sizeof(sql),
        "SELECT c.exact_address, COALESCE(NULLIF(c.title,''),''), c.protocol, c.state, COALESCE(c.refusal,''),"
        " COALESCE(NULLIF(r.name,''), r.host), r.host, COALESCE(c.material,''), c.contributions"
        " FROM channel c JOIN root r ON r.root_id=c.root_id%s"
        " ORDER BY (c.state IN ('admitted','active')) DESC, c.contributions DESC, r.host, c.first_seen"
        " LIMIT ?4 OFFSET ?5", where);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 0; }
    sqlite3_bind_text(st, 1, way ? way : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, publishes ? publishes : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, state ? state : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 4, (int)page_size);
    sqlite3_bind_int(st, 5, (int)offset);
    fputs("<section class='region region-depth' aria-label='Channels'><h2 class='region-label'>", out);
    if (way && *way) { esc(out, way); fputs(" channels", out); }
    else if (publishes && *publishes) { esc(out, publishes); fputs(" channels", out); }
    else if (state && *state) { esc(out, standing(state, "")); fputs(" channels", out); }
    else fputs("Channels", out);
    fputs("</h2><p class='source-count'><a href='", out); esc(out, route);
    fprintf(out, "'>the whole ledger</a> &middot; %d here, across every publisher.</p><ol class='source-channels'>", total);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *address = (const char *)sqlite3_column_text(st, 0);
        const char *title = (const char *)sqlite3_column_text(st, 1);
        const char *protocol = (const char *)sqlite3_column_text(st, 2);
        const char *channel_state = (const char *)sqlite3_column_text(st, 3);
        const char *refusal = (const char *)sqlite3_column_text(st, 4);
        const char *name = (const char *)sqlite3_column_text(st, 5);
        const char *host = (const char *)sqlite3_column_text(st, 6);
        const char *material = (const char *)sqlite3_column_text(st, 7);
        int contributions = sqlite3_column_int(st, 8);
        fputs("<li><div><a href='", out); esc(out, route); fputs("?channel=", out); url(out, address);
        fputs("'>", out); esc(out, title && *title ? title : address);
        fputs("</a><span class='by'><a href='", out); esc(out, route); fputs("?root=", out); url(out, host);
        fputs("'>", out); esc(out, name); fputs("</a><span class='channel-state'>", out);
        esc(out, standing(channel_state, refusal)); fputs("</span>", out);
        if (protocol && *protocol) { fputs("<span>", out); esc(out, protocol); fputs("</span>", out); }
        if (material && *material) { fputs("<span>", out); esc(out, material); fputs("</span>", out); }
        if (contributions > 0) fprintf(out, "<span>%d shown</span>", contributions);
        fputs("</span></div></li>", out);
        shown++;
    }
    fputs("</ol>", out);
    sqlite3_finalize(st);
    {   /* The pager carries whichever cut this is. */
        int left = total - (int)(offset + (unsigned)shown);
        if (offset) {
            unsigned back = offset > page_size ? offset - page_size : 0;
            fputs("<p class='pager'><a href='", out); esc(out, route);
            fputs("?", out);
            if (way && *way) { fputs("way=", out); url(out, way); }
            else if (publishes && *publishes) { fputs("publishes=", out); url(out, publishes); }
            else { fputs("standing=", out); url(out, state ? state : ""); }
            fprintf(out, "&amp;from=%u'>Back</a></p>", back);
        }
        if (left > 0) {
            fputs("<p class='pager'><a href='", out); esc(out, route);
            fputs("?", out);
            if (way && *way) { fputs("way=", out); url(out, way); }
            else if (publishes && *publishes) { fputs("publishes=", out); url(out, publishes); }
            else { fputs("standing=", out); url(out, state ? state : ""); }
            fprintf(out, "&amp;from=%u'>More (%d left)</a></p>", offset + (unsigned)shown, left);
        }
    }
    fputs("<p class='src'>A listed channel is an address this estate knows of, not a source it reads.</p></section>", out);
    sqlite3_close(db);
    return shown;
}
