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
#include "web_store.h"
#include "bf_sqlite_path.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include "bf_lex.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>
#include "estate_reference.h"
#include "ui_tree.h"

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
    return bf_store_open_ro(db_path, 1500, &bf_store_policy_source_frontier);
}

static void pager(FILE *out, const char *route, const char *host, const char *family,
                  unsigned offset, unsigned page_size, int shown, int total) {
    if (offset) {
        unsigned back = offset > page_size ? offset - page_size : 0;
        ui_start(out, "a"); ui_attr_sq(out, "class", "source-page"); ui_attr_open_sq(out, "href"); esc(out, route);
        fputs(host ? "?root=" : "?roots=1", out);
        if (host) url(out, host);
        if (family && *family) { fputs("&amp;family=", out); url(out, family); }
        fprintf(out, "&amp;from=%u'>Back", back); ui_close(out, "a");
    }
    if (total > (int)(offset + (unsigned)shown)) {
        ui_start(out, "a"); ui_attr_sq(out, "class", "source-page"); ui_attr_open_sq(out, "href"); esc(out, route);
        fputs(host ? "?root=" : "?roots=1", out);
        if (host) url(out, host);
        if (family && *family) { fputs("&amp;family=", out); url(out, family); }
        fprintf(out, "&amp;from=%u'>More (%d left)", offset + (unsigned)shown, total - (int)(offset + (unsigned)shown)); ui_close(out, "a");
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
    ui_start(out, "section"); ui_attr_sq(out, "class", "region region-depth"); ui_attr_sq(out, "aria-label", "Publishers"); ui_gt(out);
    ui_start(out, "h2"); ui_attr_sq(out, "class", "region-label"); ui_gt(out);
    ui_copy(out, "Every publisher this estate has listed"); ui_close(out, "h2");
    ui_start(out, "p"); ui_attr_sq(out, "class", "source-count"); ui_gt(out);
    fprintf(out, "%d", total);
    ui_copy(out, " publishers. A publisher is one origin; what it publishes is below it."); ui_close(out, "p");
    ui_start(out, "ol"); ui_attr_sq(out, "class", "source-roots"); ui_gt(out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *host = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        int channels = sqlite3_column_int(st, 2), families = sqlite3_column_int(st, 3), read = sqlite3_column_int(st, 4);
        const char *identity = (const char *)sqlite3_column_text(st, 5);
        ui_open(out, "li", NULL); ui_start(out, "a"); ui_attr_open_sq(out, "href");
        esc(out, route); fputs("?root=", out); url(out, host); fputs("'>", out);
        esc(out, name); ui_close(out, "a");
        ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out);
        ui_open(out, "span", NULL); fprintf(out, "%d channel%s", channels, channels == 1 ? "" : "s"); ui_close(out, "span");
        ui_open(out, "span", NULL); fprintf(out, "%d way%s in", families, families == 1 ? "" : "s"); ui_close(out, "span");
        if (read) do { ui_open(out, "span", NULL); fprintf(out, "%d", read); ui_copy(out, " read here"); ui_close(out, "span"); } while (0);
        { char own[600]; if (estate_publisher_link(NULL, host, identity, own, sizeof(own))) { ui_start(out, "a"); ui_attr_open_sq(out, "href"); esc(out, own); fputs("'>Its page", out); ui_close(out, "a"); } }
        ui_close(out, "span"); ui_close(out, "li");
        shown++;
    }
    ui_close(out, "ol");
    pager(out, route, NULL, NULL, offset, page_size, shown, total);
    ui_start(out, "p"); ui_attr_sq(out, "class", "src"); ui_gt(out);
    ui_copy(out, "A listed channel is an address this estate knows of. It is not a source it reads, "
          "and nothing here says the endpoint answers."); ui_close(out, "p"); ui_close(out, "section");
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

    ui_start(out, "section"); ui_attr_sq(out, "class", "region region-depth"); ui_attr_sq(out, "aria-label", "One publisher"); ui_gt(out);
    ui_start(out, "h2"); ui_attr_sq(out, "class", "region-label"); ui_gt(out);
    esc(out, name);
    ui_close(out, "h2");
    ui_start(out, "p"); ui_attr_sq(out, "class", "source-count"); ui_gt(out);
    ui_start(out, "a"); ui_attr_open_sq(out, "href"); esc(out, route); fputs("?roots=1'>All publishers", out); ui_close(out, "a");
    { char own[600]; if (estate_publisher_link(NULL, host, identity, own, sizeof(own))) { fputs(" &middot; <a href='", out); esc(out, own); fputs("'>Its page", out); ui_close(out, "a"); } }
    ui_close(out, "p");

    /* The ways in, with how much sits behind each. */
    if (!family || !*family) {
        if (sqlite3_prepare_v2(db,
                "SELECT c.protocol, COUNT(*), SUM(c.state IN ('admitted','active'))"
                " FROM channel c JOIN root r ON r.root_id=c.root_id WHERE r.host=?1"
                " GROUP BY c.protocol ORDER BY COUNT(*) DESC", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
            ui_start(out, "ol"); ui_attr_sq(out, "class", "source-ways"); ui_gt(out);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *protocol = (const char *)sqlite3_column_text(st, 0);
                int count = sqlite3_column_int(st, 1), read = sqlite3_column_int(st, 2);
                ui_open(out, "li", NULL); ui_start(out, "a"); ui_attr_open_sq(out, "href");
                esc(out, route); fputs("?root=", out); url(out, host);
                fputs("&amp;family=", out); url(out, protocol); fputs("'>", out); esc(out, protocol);
                ui_close(out, "a");
                ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out);
                ui_open(out, "span", NULL); fprintf(out, "%d channel%s", count, count == 1 ? "" : "s"); ui_close(out, "span");
                if (read) do { ui_open(out, "span", NULL); fprintf(out, "%d", read); ui_copy(out, " read here"); ui_close(out, "span"); } while (0);
                ui_close(out, "span"); ui_close(out, "li");
            }
            ui_close(out, "ol");
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
        ui_start(out, "ol"); ui_attr_sq(out, "class", "source-channels"); ui_gt(out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *address = (const char *)sqlite3_column_text(st, 0);
            const char *protocol = (const char *)sqlite3_column_text(st, 1);
            const char *title = (const char *)sqlite3_column_text(st, 2);
            const char *state = (const char *)sqlite3_column_text(st, 3);
            const char *refusal = (const char *)sqlite3_column_text(st, 4);
            const char *source = (const char *)sqlite3_column_text(st, 5);
            const char *material = (const char *)sqlite3_column_text(st, 6);
            int contributions = sqlite3_column_int(st, 7);
            ui_open(out, "li", NULL); ui_open(out, "div", NULL);
            ui_start(out, "a"); ui_attr_open_sq(out, "href");
            esc(out, route); fputs("?channel=", out); url(out, address);
            fputs("'>", out);
            esc(out, title && *title ? title : address);
            ui_close(out, "a");
            ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out);
            ui_start(out, "span"); ui_attr_sq(out, "class", "channel-state"); ui_gt(out);
            esc(out, standing(state, refusal));
            ui_close(out, "span");
            if (protocol && *protocol) { ui_open(out, "span", NULL); esc(out, protocol); ui_close(out, "span"); }
            if (material && *material) { ui_open(out, "span", NULL); esc(out, material); ui_close(out, "span"); }
            if (contributions > 0) do { ui_open(out, "span", NULL); fprintf(out, "%d", contributions); ui_copy(out, " shown"); ui_close(out, "span"); } while (0);
            if (source && *source) { ui_open(out, "span", NULL); ui_copy(out, "read as "); esc(out, source); ui_close(out, "span"); }
            ui_close(out, "span");
            if (refusal && *refusal) { ui_start(out, "p"); ui_attr_sq(out, "class", "channel-refusal"); ui_gt(out); esc(out, refusal); ui_close(out, "p"); }
            ui_close(out, "div"); ui_close(out, "li");
            shown++;
        }
        ui_close(out, "ol");
    }
    sqlite3_finalize(st);
    pager(out, route, host, family, offset, page_size, shown, total);
    ui_start(out, "p"); ui_attr_sq(out, "class", "src"); ui_gt(out);
    fprintf(out, "%d channel%s", total, total == 1 ? "" : "s");
    ui_copy(out, " at this publisher. A channel's standing is what this estate has done with it, "
                 "never a judgement of what it publishes."); ui_close(out, "p"); ui_close(out, "section");
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
    ui_start(out, "div"); ui_attr_sq(out, "class", "ledger-cut"); ui_gt(out);
    ui_open(out, "h3", NULL); esc(out, label); ui_close(out, "h3");
    ui_start(out, "ol"); ui_attr_sq(out, "class", "source-ways"); ui_gt(out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0);
        int count = sqlite3_column_int(st, 1);
        int read = sqlite3_column_count(st) > 2 ? sqlite3_column_int(st, 2) : 0;
        if (!name || !*name) continue;
        ui_open(out, "li", NULL);
        if (param) {
            ui_start(out, "a"); ui_attr_open_sq(out, "href");
            esc(out, route); fputs("?", out); esc(out, param); fputs("=", out);
            url(out, name); fputs("'>", out); esc(out, name); ui_close(out, "a");
        } else { ui_open(out, "span", NULL); esc(out, name); ui_close(out, "span"); }
        ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out);
        ui_open(out, "span", NULL); fprintf(out, "%d", count); ui_close(out, "span");
        if (read) do { ui_open(out, "span", NULL); fprintf(out, "%d", read); ui_copy(out, " read"); ui_close(out, "span"); } while (0);
        ui_close(out, "span"); ui_close(out, "li");
        rows++;
    }
    if (!rows && nothing) { ui_open(out, "li", NULL); ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out); esc(out, nothing); ui_close(out, "span"); ui_close(out, "li"); }
    ui_close(out, "ol"); ui_close(out, "div");
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

    ui_start(out, "section"); ui_attr_sq(out, "class", "region region-depth"); ui_attr_sq(out, "aria-label", "The ledger"); ui_gt(out);
    ui_start(out, "h2"); ui_attr_sq(out, "class", "region-label"); ui_gt(out);
    ui_copy(out, "Everything this estate has listed"); ui_close(out, "h2");
    ui_start(out, "p"); ui_attr_sq(out, "class", "source-count"); ui_gt(out);
    fprintf(out, "%d publishers, %d channels, %d ways in. %d are read here; %d are listed and unread.",
            roots, channels, families, read, listed);
    if (waiting) fprintf(out, " %d wait on a key that has not been given.", waiting);
    if (refused) fprintf(out, " %d were refused, each with the reason kept.", refused);
    ui_close(out, "p");

    ui_start(out, "form"); ui_attr_sq(out, "class", "ledger-find"); ui_attr_sq(out, "method", "get");
    ui_attr_open_sq(out, "action"); esc(out, route); fputs("'>", out);
    ui_open(out, "label", NULL); ui_copy(out, "Find a publisher ");
    ui_start(out, "input"); ui_attr_sq(out, "type", "search"); ui_attr_sq(out, "name", "q");
    ui_attr_open_sq(out, "value"); esc(out, needle ? needle : ""); ui_attr_close_sq(out);
    ui_attr_sq(out, "placeholder", "a name, or a host"); ui_gt(out);
    ui_close(out, "label");
    ui_start(out, "button"); ui_attr_sq(out, "type", "submit"); ui_gt(out);
    ui_copy(out, "Look"); ui_close(out, "button"); ui_close(out, "form");

    ui_start(out, "div"); ui_attr_sq(out, "class", "ledger-cuts"); ui_gt(out);
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
    ui_close(out, "div");

    ui_start(out, "p"); ui_attr_sq(out, "class", "src"); ui_gt(out);
    ui_copy(out, "A listed channel is an address this estate knows of. It is not a source it reads, "
          "and nothing here says the endpoint answers."); ui_close(out, "p"); ui_close(out, "section");
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

        ui_start(out, "section"); ui_attr_sq(out, "class", "region region-depth"); ui_attr_sq(out, "aria-label", "One channel"); ui_gt(out);
        ui_start(out, "h2"); ui_attr_sq(out, "class", "region-label"); ui_gt(out);
        esc(out, title && *title ? title : address);
        ui_close(out, "h2");
        ui_start(out, "p"); ui_attr_sq(out, "class", "source-count"); ui_gt(out);
        ui_start(out, "a"); ui_attr_open_sq(out, "href");
        esc(out, route); fputs("?root=", out); url(out, host);
        fputs("'>", out); esc(out, name); ui_close(out, "a");
        ui_copy(out, " &middot; ");
        ui_start(out, "a"); ui_attr_open_sq(out, "href"); esc(out, route);
        fputs("'>the whole ledger", out); ui_close(out, "a"); ui_close(out, "p");

        ui_start(out, "dl"); ui_attr_sq(out, "class", "channel-standing"); ui_gt(out);
        ui_open(out, "dt", NULL); ui_copy(out, "Standing"); ui_close(out, "dt");
        ui_open(out, "dd", NULL); esc(out, standing(state, refusal)); ui_close(out, "dd");
        if (protocol && *protocol) { ui_open(out, "dt", NULL); ui_copy(out, "Read as"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, protocol); ui_close(out, "dd"); }
        if (material && *material) { ui_open(out, "dt", NULL); ui_copy(out, "Publishes"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, material); ui_close(out, "dd"); }
        {   /* What a reading of its own body actually found beside that. */
            const char *carries = sqlite3_column_count(st) > 18 ? (const char *)sqlite3_column_text(st, 18) : NULL;
            if (carries && *carries) {
                ui_open(out, "dt", NULL); ui_copy(out, "Also carries"); ui_close(out, "dt");
                ui_open(out, "dd", NULL); esc(out, carries);
                ui_copy(out, " "); ui_start(out, "span"); ui_attr_sq(out, "class", "src"); ui_gt(out);
                ui_copy(out, "measured from what it sent"); ui_close(out, "span"); ui_close(out, "dd");
            }
        }
        if (truth && *truth) { ui_open(out, "dt", NULL); ui_copy(out, "How this address is known"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, truth); ui_close(out, "dd"); }
        if (from && *from) { ui_open(out, "dt", NULL); ui_copy(out, "Found through"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, from); ui_close(out, "dd"); }
        if (first && *first) { ui_open(out, "dt", NULL); ui_copy(out, "First listed"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, first); ui_close(out, "dd"); }
        if (last && *last) { ui_open(out, "dt", NULL); ui_copy(out, "Last looked at"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, last); ui_close(out, "dd"); }
        if (gave && *gave) { ui_open(out, "dt", NULL); ui_copy(out, "Last gave something"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, gave); ui_close(out, "dd"); }
        if (cadence && *cadence) { ui_open(out, "dt", NULL); ui_copy(out, "Asked no more often than"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, cadence); ui_close(out, "dd"); }
        if (credential && *credential) { ui_open(out, "dt", NULL); ui_copy(out, "Needs a key from"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, credential); ui_close(out, "dd"); }
        if (rights && *rights) { ui_open(out, "dt", NULL); ui_copy(out, "Terms it states"); ui_close(out, "dt"); ui_open(out, "dd", NULL); esc(out, rights); ui_close(out, "dd"); }
        if (contributions > 0) { ui_open(out, "dt", NULL); ui_copy(out, "Things shown from it"); ui_close(out, "dt"); ui_open(out, "dd", NULL); fprintf(out, "%d", contributions); ui_close(out, "dd"); }
        ui_open(out, "dt", NULL); ui_copy(out, "Address"); ui_close(out, "dt"); ui_open(out, "dd", NULL);
        ui_start(out, "a"); ui_attr_sq(out, "rel", "nofollow"); ui_attr_open_sq(out, "href");
        esc(out, address); fputs("'>", out);
        esc(out, address); ui_close(out, "a"); ui_close(out, "dd"); ui_close(out, "dl");
        if (refusal && *refusal) { ui_start(out, "p"); ui_attr_sq(out, "class", "channel-refusal"); ui_gt(out); esc(out, refusal); ui_close(out, "p"); }
    }
    sqlite3_finalize(st); st = NULL;
    sqlite3_close(db);

    /* What the estate actually kept from it: the floor of the lattice. */
    if (source[0] && archive_path && *archive_path) {
        sqlite3 *archive = bf_store_open_ro(archive_path, 1500, &bf_store_policy_item_archive);
        if (archive) {
            if (sqlite3_prepare_v2(archive,
                    "SELECT key, title, kind, COALESCE(first_read,'') FROM item WHERE source=?1"
                    " ORDER BY at DESC LIMIT 24", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
                ui_open(out, "h3", NULL); ui_copy(out, "What this estate kept from it"); ui_close(out, "h3");
                ui_start(out, "ol"); ui_attr_sq(out, "class", "source-channels"); ui_gt(out);
                while (sqlite3_step(st) == SQLITE_ROW) {
                    const char *key = (const char *)sqlite3_column_text(st, 0);
                    const char *title = (const char *)sqlite3_column_text(st, 1);
                    const char *kind = (const char *)sqlite3_column_text(st, 2);
                    const char *when = (const char *)sqlite3_column_text(st, 3);
                    ui_open(out, "li", NULL); ui_open(out, "div", NULL);
                    ui_start(out, "a"); ui_attr_open_sq(out, "href"); ui_copy(out, "/o/");
                    url(out, source); fputs("/", out); esc(out, key);
                    fputs("'>", out); esc(out, title && *title ? title : key); ui_close(out, "a");
                    ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out);
                    if (kind && *kind) { ui_open(out, "span", NULL); esc(out, kind); ui_close(out, "span"); }
                    if (when && *when) { ui_open(out, "span", NULL); ui_copy(out, "kept "); esc(out, when); ui_close(out, "span"); }
                    ui_close(out, "span"); ui_close(out, "div"); ui_close(out, "li");
                    shown++;
                }
                ui_close(out, "ol");
                sqlite3_finalize(st);
                if (!shown) do { ui_start(out, "p"); ui_attr_sq(out, "class", "src"); ui_gt(out); ui_copy(out, "Nothing from this channel is kept yet."); ui_close(out, "p"); } while (0);
            }
            sqlite3_close(archive);
        }
    }
    ui_start(out, "p"); ui_attr_sq(out, "class", "src"); ui_gt(out);
    ui_copy(out, "A channel's standing is what this estate has done with it, never a judgement of "
          "what it publishes. What was kept is what this estate read at a time, not a claim that the "
          "channel still publishes it."); ui_close(out, "p"); ui_close(out, "section");
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
    ui_start(out, "section"); ui_attr_sq(out, "class", "region region-depth"); ui_attr_sq(out, "aria-label", "Channels"); ui_gt(out);
    ui_start(out, "h2"); ui_attr_sq(out, "class", "region-label"); ui_gt(out);
    if (way && *way) { esc(out, way); fputs(" channels", out); }
    else if (publishes && *publishes) { esc(out, publishes); fputs(" channels", out); }
    else if (state && *state) { esc(out, standing(state, "")); fputs(" channels", out); }
    else fputs("Channels", out);
    ui_close(out, "h2");
    ui_start(out, "p"); ui_attr_sq(out, "class", "source-count"); ui_gt(out);
    ui_start(out, "a"); ui_attr_open_sq(out, "href"); esc(out, route);
    fputs("'>the whole ledger", out); ui_close(out, "a");
    fprintf(out, " &middot; %d here, across every publisher.", total); ui_close(out, "p");
    ui_start(out, "ol"); ui_attr_sq(out, "class", "source-channels"); ui_gt(out);
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
        ui_open(out, "li", NULL); ui_open(out, "div", NULL);
        ui_start(out, "a"); ui_attr_open_sq(out, "href");
        esc(out, route); fputs("?channel=", out); url(out, address);
        fputs("'>", out); esc(out, title && *title ? title : address);
        ui_close(out, "a");
        ui_start(out, "span"); ui_attr_sq(out, "class", "by"); ui_gt(out);
        ui_start(out, "a"); ui_attr_open_sq(out, "href");
        esc(out, route); fputs("?root=", out); url(out, host);
        fputs("'>", out); esc(out, name); ui_close(out, "a");
        ui_start(out, "span"); ui_attr_sq(out, "class", "channel-state"); ui_gt(out);
        esc(out, standing(channel_state, refusal)); ui_close(out, "span");
        if (protocol && *protocol) { ui_open(out, "span", NULL); esc(out, protocol); ui_close(out, "span"); }
        if (material && *material) { ui_open(out, "span", NULL); esc(out, material); ui_close(out, "span"); }
        if (contributions > 0) do { ui_open(out, "span", NULL); fprintf(out, "%d", contributions); ui_copy(out, " shown"); ui_close(out, "span"); } while (0);
        ui_close(out, "span"); ui_close(out, "div"); ui_close(out, "li");
        shown++;
    }
    ui_close(out, "ol");
    sqlite3_finalize(st);
    {   /* The pager carries whichever cut this is. */
        int left = total - (int)(offset + (unsigned)shown);
        if (offset) {
            unsigned back = offset > page_size ? offset - page_size : 0;
            ui_start(out, "p"); ui_attr_sq(out, "class", "pager"); ui_gt(out);
            ui_start(out, "a"); ui_attr_open_sq(out, "href"); esc(out, route);
            fputs("?", out);
            if (way && *way) { fputs("way=", out); url(out, way); }
            else if (publishes && *publishes) { fputs("publishes=", out); url(out, publishes); }
            else { fputs("standing=", out); url(out, state ? state : ""); }
            fprintf(out, "&amp;from=%u'>Back", back); ui_close(out, "a"); ui_close(out, "p");
        }
        if (left > 0) {
            ui_start(out, "p"); ui_attr_sq(out, "class", "pager"); ui_gt(out);
            ui_start(out, "a"); ui_attr_open_sq(out, "href"); esc(out, route);
            fputs("?", out);
            if (way && *way) { fputs("way=", out); url(out, way); }
            else if (publishes && *publishes) { fputs("publishes=", out); url(out, publishes); }
            else { fputs("standing=", out); url(out, state ? state : ""); }
            fprintf(out, "&amp;from=%u'>More (%d left)", offset + (unsigned)shown, left);
            ui_close(out, "a"); ui_close(out, "p");
        }
    }
    ui_start(out, "p"); ui_attr_sq(out, "class", "src"); ui_gt(out);
    ui_copy(out, "A listed channel is an address this estate knows of, not a source it reads.");
    ui_close(out, "p"); ui_close(out, "section");
    sqlite3_close(db);
    return shown;
}
