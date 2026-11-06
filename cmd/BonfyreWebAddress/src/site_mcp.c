#define _POSIX_C_SOURCE 200809L
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#include "markdown_fence.h"
/*
 * A site's own MCP server.
 *
 * Every site an operator stands up answers the Model Context Protocol at /mcp
 * on its own host: an agent can ask what the site is, list and read what it
 * has published, search it, and be handed a view of it.  The same request
 * path serves the site's page to people; nothing is copied into a second
 * store and nothing here can change the site.
 *
 * Carriers of one set of facts, as the estate's presentation contract has it:
 *
 *   POST /mcp                     JSON-RPC (Streamable HTTP, stateless)
 *   GET  /mcp                     the same facts for a person: what this is,
 *                                 how to connect, what an agent may ask
 *   GET  /llms.txt                the site's writing as plain markdown
 *   GET  /.well-known/agent.json  a card that names this site, not the estate
 *
 * Read tools return typed structuredContent and a compact text reading of it.
 * One separate render tool carries a ui:// view (MCP Apps) for hosts that show
 * views; a host that does not gets the text and loses nothing.  The view wears
 * the site's own tokens (ground, ink, accent) and holds no fact the text lacks.
 *
 * Only an operator's own site (territory "independent", with a published
 * program) is served here.  Any other host is left to the handlers it had.
 */

#include <ctype.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "site_mcp.h"
#include "site_presentation.h"

void bf_sha256_hex(const unsigned char *data, size_t len, char hex[65]);

/* Who is asking.  No token is the public; a token is one the content store
 * issued for this site's namespace, and what it may do is what that store
 * recorded for it.  Every call is written to the meter under the caller. */
typedef struct {
    char key_id[200], label[80];
    int authenticated, refused, can_write;
} McpCaller;

static void organ_path(char *out, size_t cap, const char *env, const char *name);

#define MCP_LATEST "2025-06-18"
#define UI_URI "ui://site/posts"
#define UI_MIME "text/html;profile=mcp-app"

typedef struct {
    char id[128], host[256], title[256], tagline[512], profile[128];
    char ground[40], ink[40], accent[40], muted[40], rule[64], panel[40];
    long posts;
    /* 1 when this is not an operator's own site but the estate's reading of
     * someone else's publication: read-only, and said to be so everywhere. */
    int reading;
} McpSite;

static void jtext(FILE *out, const char *text) {
    fputc('"', out);
    for (; text && *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') { fputc('\\', out); fputc(c, out); }
        else if (c == '\n') fputs("\\n", out);
        else if (c == '\r') fputs("\\r", out);
        else if (c == '\t') fputs("\\t", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}
/* The knight identifies Aurekai's own published readings. A customer's
 * independently branded site does not silently inherit the platform icon. */
static void site_icon_metadata(FILE *out, const McpSite *site) {
    if (!site->reading) return;
    /* Syndicated publisher hosts may not have Matrix asset routes. */
    fputs(",\"icons\":[{\"src\":", out);
    jtext(out, "https://aurekai.live/assets/aurekai-knight-48.png");
    fputs(",\"mimeType\":\"image/png\",\"sizes\":[\"48x48\"]},{\"src\":", out);
    jtext(out, "https://aurekai.live/assets/aurekai-knight.png");
    fputs(",\"mimeType\":\"image/png\",\"sizes\":[\"168x168\"]}]", out);
}
static void site_browser_icon(FILE *out, const McpSite *site) {
    if (site->reading) fputs(BF_AUREKAI_FAVICON_LINK, out);
}


static void htext(FILE *out, const char *text) {
    for (; text && *text; text++) {
        if (*text == '&') fputs("&amp;", out);
        else if (*text == '<') fputs("&lt;", out);
        else if (*text == '>') fputs("&gt;", out);
        else if (*text == '"') fputs("&quot;", out);
        else fputc(*text, out);
    }
}

static void reply(int code, const char *type, const char *body, size_t len) {
    const char *reason = code == 200 ? "OK" : code == 202 ? "Accepted" : code == 401 ? "Unauthorized" : code == 403 ? "Forbidden" :
                         code == 429 ? "Too Many Requests" :
                         code == 404 ? "Not Found" : code == 405 ? "Method Not Allowed" : "Bad Request";
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nCache-Control: no-store\r\n"
           "X-Bonfyre-Site-Mcp: native\r\nLink: </.well-known/mcp/server-card.json>; rel=\"mcp\"\r\nConnection: close\r\n\r\n", code, reason, type, len);
    fwrite(body, 1, len, stdout);
    fflush(stdout);
}

static int colour_ok(const char *c) {
    size_t n = c ? strlen(c) : 0;
    if (!n || n > 38) return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)c[i]) && !strchr("#(),.% ", c[i])) return 0;
    return 1;
}

static void token(sqlite3_stmt *st, int col, char *out, size_t cap, const char *fallback) {
    const char *value = (const char *)sqlite3_column_text(st, col);
    snprintf(out, cap, "%s", value && colour_ok(value) ? value : fallback);
}

/* The operator's own site answering at this host, if there is one. */
static int load_site(sqlite3 *db, const char *host_header, const char *site_id, McpSite *site) {
    sqlite3_stmt *st = NULL;
    char host[256], *colon;
    const char *bare;
    int found = 0;
    memset(site, 0, sizeof(*site));
    snprintf(host, sizeof(host), "%s", host_header ? host_header : "");
    colon = strrchr(host, ':');
    if (colon && !strchr(colon, ']')) *colon = '\0';
    if (sqlite3_prepare_v2(db,
            "SELECT s.site_id, s.host, COALESCE(s.profile,'') FROM site_presentation s "
            "JOIN site_program p ON p.site_id=s.site_id AND p.status='published' "
            "WHERE ((?2 IS NULL AND s.host=?1) OR s.site_id=?2) AND s.status='published' AND s.territory='independent' "
            "ORDER BY s.id DESC LIMIT 1",
            -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
    if (site_id) sqlite3_bind_text(st, 2, site_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(site->id, sizeof(site->id), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(site->host, sizeof(site->host), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(site->profile, sizeof(site->profile), "%s", (const char *)sqlite3_column_text(st, 2));
        found = 1;
    }
    sqlite3_finalize(st);
    if (!found && !site_id) {
        /* One of the estate's newsroom addresses: what the estate has read
         * from a publisher's public feed, standing at an address of its own. */
        if (sqlite3_prepare_v2(db,
                "SELECT s.site_id, s.host, COALESCE(s.profile,''), "
                "COALESCE((SELECT i.label FROM internet_source i WHERE i.source LIKE 'feed-' || substr(s.site_id, 11) || '-%' LIMIT 1), '') "
                "FROM site_presentation s WHERE s.host=?1 AND s.status='published' AND s.site_id LIKE 'publisher-%' "
                "AND s.territory<>'independent' ORDER BY s.id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, host, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *label = (const char *)sqlite3_column_text(st, 3);
                snprintf(site->id, sizeof(site->id), "%s", (const char *)sqlite3_column_text(st, 0));
                snprintf(site->host, sizeof(site->host), "%s", (const char *)sqlite3_column_text(st, 1));
                snprintf(site->profile, sizeof(site->profile), "%s", (const char *)sqlite3_column_text(st, 2));
                snprintf(site->title, sizeof(site->title), "%s", label && *label ? label : site->id + 10);
                snprintf(site->tagline, sizeof(site->tagline),
                         "What Aurekai has read from this publisher's public feed: titles, dates and links to the originals. "
                         "It is not the publisher's own site and is not published or endorsed by them.");
                site->reading = 1;
                found = 1;
            }
            sqlite3_finalize(st);
        }
    }
    if (!found) return 0;
    {
        /* Whatever kind of site this is, its dated material is read under one
         * name for the length of this request. */
        char *view = site->reading ?
            sqlite3_mprintf("CREATE TEMP VIEW site_posts(id,title,published_at,body) AS SELECT id, title, COALESCE(observed_at,''), "
                            "trim(COALESCE(summary,'') || char(10) || 'Original: ' || COALESCE(canonical_href,'')) "
                            "FROM main.publication_occurrence WHERE source_ref LIKE 'feed-' || %Q || '-%%' AND status='published' AND visibility='public'",
                            site->id + 10) :
            sqlite3_mprintf("CREATE TEMP VIEW site_posts(id,title,published_at,body) AS SELECT id, title, COALESCE(published_at,''), COALESCE(body,'') "
                            "FROM main.community_post WHERE community=%Q AND status='published'", site->id);
        int made = view && sqlite3_exec(db, "DROP VIEW IF EXISTS temp.site_posts", NULL, NULL, NULL) == SQLITE_OK &&
                   sqlite3_exec(db, view, NULL, NULL, NULL) == SQLITE_OK;
        sqlite3_free(view);
        if (!made) return 0;
    }
    if (site->reading) goto counted;
    snprintf(site->title, sizeof(site->title), "%s", site->id);
    if (sqlite3_prepare_v2(db, "SELECT title, COALESCE(description,'') FROM community_body WHERE community=?1 "
                               "AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(site->title, sizeof(site->title), "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(site->tagline, sizeof(site->tagline), "%s", (const char *)sqlite3_column_text(st, 1));
        }
        sqlite3_finalize(st);
    }
    snprintf(site->ground, sizeof(site->ground), "#0d130f"); snprintf(site->ink, sizeof(site->ink), "#dde6da");
    snprintf(site->accent, sizeof(site->accent), "#b5df32"); snprintf(site->muted, sizeof(site->muted), "#8c9a8d");
    snprintf(site->rule, sizeof(site->rule), "rgba(221,230,218,.17)"); snprintf(site->panel, sizeof(site->panel), "#17211a");
    bare = strchr(site->profile, ':');
    if (sqlite3_prepare_v2(db,
            "SELECT json_extract(layout_tokens,'$.ground'), json_extract(layout_tokens,'$.ink'), json_extract(layout_tokens,'$.accent'), "
            "json_extract(layout_tokens,'$.muted'), json_extract(layout_tokens,'$.rule'), json_extract(layout_tokens,'$.panel') "
            "FROM presentation_template WHERE (template=?1 OR template=?2) AND status='published' AND json_valid(layout_tokens) "
            "ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->profile, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, bare ? bare + 1 : site->profile, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            token(st, 0, site->ground, sizeof(site->ground), "#0d130f"); token(st, 1, site->ink, sizeof(site->ink), "#dde6da");
            token(st, 2, site->accent, sizeof(site->accent), "#b5df32"); token(st, 3, site->muted, sizeof(site->muted), "#8c9a8d");
            token(st, 4, site->rule, sizeof(site->rule), "rgba(221,230,218,.17)"); token(st, 5, site->panel, sizeof(site->panel), "#17211a");
        }
        sqlite3_finalize(st);
    }
counted:
    if (site->reading) {
        snprintf(site->ground, sizeof(site->ground), "#0d130f"); snprintf(site->ink, sizeof(site->ink), "#dde6da");
        snprintf(site->accent, sizeof(site->accent), "#b5df32"); snprintf(site->muted, sizeof(site->muted), "#8c9a8d");
        snprintf(site->rule, sizeof(site->rule), "rgba(221,230,218,.17)"); snprintf(site->panel, sizeof(site->panel), "#17211a");
    }
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM site_posts WHERE 1=1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) site->posts = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    return 1;
}

/* The first sentence or so of a body, on one line. */
static void excerpt(const char *body, char *out, size_t cap) {
    size_t n = 0;
    int space = 0;
    for (; body && *body && n + 4 < cap && n < 220; body++) {
        if (isspace((unsigned char)*body)) { space = n > 0; continue; }
        if (space) { out[n++] = ' '; space = 0; }
        out[n++] = *body;
    }
    /* Never end inside a multi-byte character. */
    while (n && ((unsigned char)out[n - 1] & 0xC0) == 0x80) n--;
    if (n && ((unsigned char)out[n - 1] & 0x80) && body && *body) n--;
    out[n] = '\0';
    if (body && *body) { strcat(out, "…"); }
}

static void post_json(FILE *out, const McpSite *site, sqlite3_stmt *st, int with_body) {
    char brief[260];
    const char *body = (const char *)sqlite3_column_text(st, 3);
    excerpt(body, brief, sizeof(brief));
    fprintf(out, "{\"id\":%lld,\"title\":", sqlite3_column_int64(st, 0));
    jtext(out, (const char *)sqlite3_column_text(st, 1));
    fputs(",\"published_at\":", out); jtext(out, (const char *)sqlite3_column_text(st, 2));
    fputs(",\"excerpt\":", out); jtext(out, brief);
    if (with_body) { fputs(",\"body\":", out); jtext(out, body ? body : ""); }
    fprintf(out, ",\"uri\":\"site://%s/post/%lld\",\"source\":\"https://%s/\"}", site->id, sqlite3_column_int64(st, 0), site->host);
}

#define POST_COLUMNS "id, title, published_at, body"

/* ---------- tools ---------- */

static void tools_list(FILE *out, const McpSite *site) {
    static const char *post_schema =
        "{\"type\":\"object\",\"required\":[\"id\",\"title\",\"published_at\",\"excerpt\",\"uri\"],\"properties\":{"
        "\"id\":{\"type\":\"integer\"},\"title\":{\"type\":\"string\"},\"published_at\":{\"type\":\"string\"},"
        "\"excerpt\":{\"type\":\"string\"},\"body\":{\"type\":\"string\"},\"uri\":{\"type\":\"string\"},\"source\":{\"type\":\"string\"}}}";
    fputs("{\"tools\":[", out);
    fputs("{\"name\":\"site_info\"", out); site_icon_metadata(out, site);
    fputs(",\"title\":\"About this site\",\"description\":\"What this site is: its name, what it says about itself, "
          "its address and how much it has published. Start here.\",\"inputSchema\":{\"type\":\"object\",\"properties\":{},\"additionalProperties\":false},"
          "\"outputSchema\":{\"type\":\"object\",\"required\":[\"name\",\"address\",\"posts\"],\"properties\":{\"name\":{\"type\":\"string\"},"
          "\"about\":{\"type\":\"string\"},\"address\":{\"type\":\"string\"},\"posts\":{\"type\":\"integer\"}}},"
          "\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", out);
    fprintf(out, "{\"name\":\"list_posts\",\"title\":\"List what the site has published\",\"description\":\"Titles, dates and opening lines of the "
          "site's posts, newest first. Use get_post for the full text of one.\",\"inputSchema\":{\"type\":\"object\",\"properties\":{"
          "\"limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":50,\"default\":10},\"offset\":{\"type\":\"integer\",\"minimum\":0,\"default\":0}},"
          "\"additionalProperties\":false},\"outputSchema\":{\"type\":\"object\",\"required\":[\"posts\",\"total\"],\"properties\":{\"total\":{\"type\":\"integer\"},"
          "\"posts\":{\"type\":\"array\",\"items\":%s}}},\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", post_schema);
    fprintf(out, "{\"name\":\"get_post\",\"title\":\"Read one post\",\"description\":\"The full text of one post, by the id list_posts or "
          "search_posts gave.\",\"inputSchema\":{\"type\":\"object\",\"required\":[\"id\"],\"properties\":{\"id\":{\"type\":\"integer\"}},"
          "\"additionalProperties\":false},\"outputSchema\":%s,\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", post_schema);
    fprintf(out, "{\"name\":\"search_posts\",\"title\":\"Search the site\",\"description\":\"Find posts on this site. Results say how they were "
          "found: by meaning and words together when the site's index is built, by words alone otherwise. A result is a place to read, not an "
          "answer.\",\"inputSchema\":{\"type\":\"object\",\"required\":[\"query\"],\"properties\":{\"query\":{\"type\":\"string\",\"minLength\":2,\"maxLength\":300},"
          "\"top\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":20,\"default\":5}},\"additionalProperties\":false},\"outputSchema\":{\"type\":\"object\","
          "\"required\":[\"results\",\"method\"],\"properties\":{\"method\":{\"type\":\"string\"},\"results\":{\"type\":\"array\",\"items\":%s}}},"
          "\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", post_schema);
    if (!site->reading) {
    fputs("{\"name\":\"list_pages\",\"title\":\"List the site's pages\",\"description\":\"The standing pages of the site (about, services, "
          "contact and the like), as distinct from its dated posts. Use get_page for one page's text.\",\"inputSchema\":{\"type\":\"object\","
          "\"properties\":{},\"additionalProperties\":false},\"outputSchema\":{\"type\":\"object\",\"required\":[\"pages\"],\"properties\":{\"pages\":"
          "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"required\":[\"route\",\"title\"],\"properties\":{\"route\":{\"type\":\"string\"},"
          "\"title\":{\"type\":\"string\"},\"summary\":{\"type\":\"string\"},\"address\":{\"type\":\"string\"}}}}}},"
          "\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", out);
    fputs("{\"name\":\"get_page\",\"title\":\"Read one page\",\"description\":\"The full text of one standing page, by the route list_pages "
          "gave.\",\"inputSchema\":{\"type\":\"object\",\"required\":[\"route\"],\"properties\":{\"route\":{\"type\":\"string\"}},"
          "\"additionalProperties\":false},\"outputSchema\":{\"type\":\"object\",\"required\":[\"route\",\"title\",\"text\"],\"properties\":{"
          "\"route\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"summary\":{\"type\":\"string\"},\"text\":{\"type\":\"string\"},"
          "\"address\":{\"type\":\"string\"}}},\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", out);
    fputs("{\"name\":\"list_offers\",\"title\":\"What the site offers, and for how much\",\"description\":\"The products or services the site "
          "lists, each with its price as the owner words it, the same price as a number where one was given, and whether a time can be requested. "
          "Prices are what is published; nothing is charged or reserved here.\",\"inputSchema\":{\"type\":\"object\",\"properties\":{},"
          "\"additionalProperties\":false},\"outputSchema\":{\"type\":\"object\",\"required\":[\"offers\"],\"properties\":{\"offers\":{\"type\":\"array\","
          "\"items\":{\"type\":\"object\",\"required\":[\"id\",\"title\",\"price\",\"bookable\"],\"properties\":{\"id\":{\"type\":\"integer\"},"
          "\"title\":{\"type\":\"string\"},\"description\":{\"type\":\"string\"},\"price\":{\"type\":\"string\"},\"amount\":{\"type\":\"number\"},"
          "\"currency\":{\"type\":\"string\"},\"unit\":{\"type\":\"string\"},\"bookable\":{\"type\":\"boolean\"},\"address\":{\"type\":\"string\"}}}}}},"
          "\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false}},", out);
    fputs("{\"name\":\"request_booking\",\"title\":\"Ask the owner for a time\",\"description\":\"Ask for a time for one bookable offer, on a "
          "person's behalf and only when they ask you to. It is a request: nothing is reserved until the owner replies, and the result says so. "
          "Needs a key the owner issued for writing.\",\"inputSchema\":{\"type\":\"object\",\"required\":[\"offer_id\",\"when\",\"name\",\"contact\"],"
          "\"properties\":{\"offer_id\":{\"type\":\"integer\"},\"when\":{\"type\":\"string\",\"minLength\":4,\"maxLength\":120,"
          "\"description\":\"The time asked for, as a date and time or in the person's words\"},\"name\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":120},"
          "\"contact\":{\"type\":\"string\",\"minLength\":3,\"maxLength\":200},\"note\":{\"type\":\"string\",\"maxLength\":2000}},\"additionalProperties\":false},"
          "\"outputSchema\":{\"type\":\"object\",\"required\":[\"requested\",\"confirmed\",\"reference\"],\"properties\":{\"requested\":{\"type\":\"boolean\"},"
          "\"confirmed\":{\"type\":\"boolean\"},\"reference\":{\"type\":\"string\"},\"offer\":{\"type\":\"string\"}}},"
          "\"annotations\":{\"readOnlyHint\":false,\"destructiveHint\":false,\"idempotentHint\":false,\"openWorldHint\":false}},", out);
    fputs("{\"name\":\"send_inquiry\",\"title\":\"Send the site owner a message\",\"description\":\"Leave a message for whoever runs this site, "
          "on a person's behalf and only when they ask you to. It is delivered to the owner's inbox as written; nothing is promised in reply. "
          "Needs a key the owner issued for writing; without one it is refused.\",\"inputSchema\":{\"type\":\"object\","
          "\"required\":[\"name\",\"contact\",\"message\"],\"properties\":{\"name\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":120},"
          "\"contact\":{\"type\":\"string\",\"minLength\":3,\"maxLength\":200,\"description\":\"How the owner can reply: an email address or phone number\"},"
          "\"message\":{\"type\":\"string\",\"minLength\":5,\"maxLength\":4000}},\"additionalProperties\":false},"
          "\"outputSchema\":{\"type\":\"object\",\"required\":[\"delivered\",\"reference\"],\"properties\":{\"delivered\":{\"type\":\"boolean\"},"
          "\"reference\":{\"type\":\"string\"}}},\"annotations\":{\"readOnlyHint\":false,\"destructiveHint\":false,\"idempotentHint\":false,\"openWorldHint\":false}},", out);
    }
    fputs("{\"name\":\"show_posts\"", out); site_icon_metadata(out, site);
    fprintf(out, ",\"title\":\"Show posts as a view\",\"description\":\"Hand the person a readable view of posts from this "
          "site, in the site's own look. Use it after choosing which posts matter, to present them; use the other tools to read and compare. "
          "A host that does not show views gets the same posts as text.\",\"inputSchema\":{\"type\":\"object\",\"properties\":{"
          "\"ids\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"},\"maxItems\":20,\"description\":\"Posts to show; omit for the newest\"}},"
          "\"additionalProperties\":false},\"outputSchema\":{\"type\":\"object\",\"required\":[\"site\",\"posts\"],\"properties\":{\"site\":{\"type\":\"object\"},"
          "\"posts\":{\"type\":\"array\",\"items\":%s}}},\"annotations\":{\"readOnlyHint\":true,\"destructiveHint\":false,\"openWorldHint\":false},"
          "\"_meta\":{\"ui\":{\"resourceUri\":\"" UI_URI "\"},\"ui/resourceUri\":\"" UI_URI "\"}}", post_schema);
    fputs("]}", out);
}

/* A tool result: the facts as structure, and the same facts as plain text. */
static void tool_result(FILE *out, const char *text, const char *structured, int is_error) {
    fputs("{\"content\":[{\"type\":\"text\",\"text\":", out);
    jtext(out, text);
    fputs("}]", out);
    if (structured) fprintf(out, ",\"structuredContent\":%s", structured);
    fprintf(out, ",\"isError\":%s}", is_error ? "true" : "false");
}

static void tool_error(FILE *out, const char *message) { tool_result(out, message, NULL, 1); }

static void site_json(FILE *out, const McpSite *site) {
    fputs("{\"name\":", out); jtext(out, site->title);
    fputs(",\"about\":", out); jtext(out, site->tagline);
    fprintf(out, ",\"address\":\"https://%s/\",\"posts\":%ld}", site->host, site->posts);
}

static void call_site_info(FILE *out, const McpSite *site) {
    char *structured = NULL, *text = NULL;
    size_t a = 0, b = 0;
    FILE *s = open_memstream(&structured, &a), *t = open_memstream(&text, &b);
    site_json(s, site);
    fprintf(t, "%s\n%s%s%ld %s%s %s · https://%s/", site->title, site->tagline, site->tagline[0] ? "\n" : "",
            site->posts, site->reading ? "item" : "post", site->posts == 1 ? "" : "s", site->reading ? "read" : "published", site->host);
    fclose(s); fclose(t);
    tool_result(out, text, structured, 0);
    free(structured); free(text);
}

/* Writes posts selected by a prepared statement as {posts:[...]} plus text. */
static long emit_posts(FILE *structured, FILE *text, const McpSite *site, sqlite3_stmt *st, int with_body) {
    long n = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        char brief[260];
        const char *when = (const char *)sqlite3_column_text(st, 2);
        if (n++) fputc(',', structured);
        post_json(structured, site, st, with_body);
        excerpt((const char *)sqlite3_column_text(st, 3), brief, sizeof(brief));
        fprintf(text, "%s#%lld  %s  (%.10s)\n    %s\n", n > 1 ? "\n" : "", sqlite3_column_int64(st, 0),
                (const char *)sqlite3_column_text(st, 1), when ? when : "", brief);
    }
    return n;
}

static void call_list_posts(FILE *out, sqlite3 *db, const McpSite *site, long limit, long offset) {
    char *structured = NULL, *text = NULL;
    size_t a = 0, b = 0;
    FILE *s = open_memstream(&structured, &a), *t = open_memstream(&text, &b);
    sqlite3_stmt *st = NULL;
    long n = 0;
    if (limit < 1 || limit > 50) limit = 10;
    if (offset < 0) offset = 0;
    fprintf(s, "{\"total\":%ld,\"posts\":[", site->posts);
    if (sqlite3_prepare_v2(db, "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 "
                               "ORDER BY published_at DESC, id DESC LIMIT ?2 OFFSET ?3", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, limit);
        sqlite3_bind_int64(st, 3, offset);
        fprintf(t, "%s — %ld post%s", site->title, site->posts, site->posts == 1 ? "" : "s");
        if (site->posts > limit) fprintf(t, ", showing %ld from %ld", limit, offset + 1);
        fputs("\n\n", t);
        n = emit_posts(s, t, site, st, 0);
        sqlite3_finalize(st);
    }
    if (!n) fputs(site->reading ? "Nothing has been read from this source yet." : "Nothing published here yet.", t);
    fputs("]}", s);
    fclose(s); fclose(t);
    tool_result(out, text, structured, 0);
    free(structured); free(text);
}

static void call_get_post(FILE *out, sqlite3 *db, const McpSite *site, long long id) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 AND id=?2",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, id);
        if (sqlite3_step(st) == SQLITE_ROW) {
            char *structured = NULL, *text = NULL;
            size_t a = 0, b = 0;
            FILE *s = open_memstream(&structured, &a), *t = open_memstream(&text, &b);
            const char *when = (const char *)sqlite3_column_text(st, 2);
            post_json(s, site, st, 1);
            fprintf(t, "%s\n%.10s · %s\n\n%s", (const char *)sqlite3_column_text(st, 1), when ? when : "", site->title,
                    (const char *)sqlite3_column_text(st, 3));
            fclose(s); fclose(t);
            tool_result(out, text, structured, 0);
            free(structured); free(text);
            sqlite3_finalize(st);
            return;
        }
        sqlite3_finalize(st);
    }
    tool_error(out, "No published post with that id on this site. list_posts gives the ids.");
}

/* The index `bonfyre site search` builds beside the content store, if it is
 * there and no older than the store. */
static int recall_index(const char *cms_db, char *store, size_t cap) {
    char dir[1024], *slash;
    unsigned long long h = 1469598103934665603ULL;
    struct stat a, b;
    for (const char *p = cms_db; *p; p++) { h ^= (unsigned char)*p; h *= 1099511628211ULL; }
    snprintf(dir, sizeof(dir), "%s", cms_db);
    slash = strrchr(dir, '/');
    if (!slash) return 0;
    *slash = '\0';
    snprintf(store, cap, "%s/search-%016llx.db", dir, h);
    return !stat(store, &b) && !stat(cms_db, &a) && b.st_mtime >= a.st_mtime;
}

/* Asks Recall; returns its JSON or NULL. */
static char *recall_query(const char *store, const char *query, long top) {
    const char *bin = getenv("BONFYRE_RECALL_BINARY"), *home = getenv("HOME");
    char fallback[1024], top_text[16], *out;
    int pipefd[2], status = 0;
    size_t used = 0, cap = 1 << 18;
    pid_t pid;
    if (!bin || !*bin) { snprintf(fallback, sizeof(fallback), "%s/.bonfyre/bin/bonfyre-recall", home ? home : ""); bin = fallback; }
    if (access(bin, X_OK) || pipe(pipefd)) return NULL;
    snprintf(top_text, sizeof(top_text), "%ld", top * 4 < 20 ? 20 : top * 4);
    pid = fork();
    if (pid == 0) {
        dup2(pipefd[1], 1); close(pipefd[0]); close(pipefd[1]);
        { FILE *quiet = freopen("/dev/null", "w", stderr); (void)quiet; }
        execl(bin, bin, "query", "--store", store, "--text", query, "--top", top_text, "--hybrid", (char *)NULL);
        _exit(127);
    }
    close(pipefd[1]);
    out = malloc(cap);
    for (;;) {
        ssize_t n;
        if (!out) break;
        if (used + 4096 > cap) { cap *= 2; out = realloc(out, cap); if (!out) break; }
        n = read(pipefd[0], out + used, cap - used - 1);
        if (n <= 0) break;
        used += (size_t)n;
    }
    close(pipefd[0]);
    waitpid(pid, &status, 0);
    if (!out || !WIFEXITED(status) || WEXITSTATUS(status) || !used) { free(out); return NULL; }
    out[used] = '\0';
    return out;
}

static void call_search(FILE *out, sqlite3 *db, const McpSite *site, const char *cms_db, const char *query, long top) {
    char *structured = NULL, *text = NULL, store[1100], *recall = NULL;
    size_t a = 0, b = 0;
    FILE *s, *t;
    sqlite3_stmt *st = NULL;
    const char *method = "words";
    long n = 0;
    if (!query || strlen(query) < 2 || strlen(query) > 300) { tool_error(out, "query is 2 to 300 characters."); return; }
    if (top < 1 || top > 20) top = 5;
    s = open_memstream(&structured, &a); t = open_memstream(&text, &b);
    fputs("{\"results\":[", s);
    /* The estate's readings are searched by their words; the index beside the
     * store is of operators' own sites, and nothing is written beside the
     * estate's store from here. */
    if (!site->reading && recall_index(cms_db, store, sizeof(store)) && (recall = recall_query(store, query, top))) {
        /* Recall's candidates, kept only where they are this site's posts. */
        if (sqlite3_prepare_v2(db,
                "SELECT p.id, p.title, COALESCE(p.published_at,''), COALESCE(p.body,'') FROM json_each(?1,'$.fused.results') r "
                "JOIN site_posts p ON p.id = CAST(substr(json_extract(r.value,'$.id'), 16) AS INTEGER) "
                "WHERE json_extract(r.value,'$.id') LIKE 'community_post:%' AND ?2 IS NOT NULL "
                "GROUP BY p.id ORDER BY min(r.key) LIMIT ?3", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, recall, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, site->id, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 3, top);
            n = emit_posts(s, t, site, st, 0);
            sqlite3_finalize(st);
            st = NULL;
            if (n) method = "meaning and words";
        }
    }
    if (!n && !recall && !site->reading) {
        /* The index is behind the store.  It is brought up to date in the
         * background, once at a time, and this answer is by words. */
        char recall_bin[1024], lock[1200], work[1200], r1[1200], r2[1200], r3[1200];
        struct stat lock_st;
        organ_path(recall_bin, sizeof(recall_bin), "BONFYRE_RECALL_BINARY", "bonfyre-recall");
        recall_index(cms_db, store, sizeof(store));
        snprintf(lock, sizeof(lock), "%s.rebuilding", store);
        snprintf(work, sizeof(work), "%.*s.work", (int)(strlen(store) - 3), store);
        if (!access(recall_bin, X_OK) && (stat(lock, &lock_st) || time(NULL) - lock_st.st_mtime > 600)) {
            FILE *mark = fopen(lock, "w");
            if (mark) fclose(mark);
            snprintf(r1, sizeof(r1), "%s:site_presentation", cms_db);
            snprintf(r2, sizeof(r2), "%s:community_body", cms_db);
            snprintf(r3, sizeof(r3), "%s:community_post", cms_db);
            if (fork() == 0) {
                if (fork() == 0) {
                    FILE *quiet;
                    setsid();
                    quiet = freopen("/dev/null", "w", stdout); (void)quiet;
                    quiet = freopen("/dev/null", "w", stderr); (void)quiet;
                    if (fork() == 0) {
                        execl(recall_bin, recall_bin, "index-sources", "--store", store, "--work-dir", work, "--records", r1,
                              "--records", r2, "--records", r3, "--apply", (char *)NULL);
                        _exit(127);
                    }
                    wait(NULL);
                    unlink(lock);
                    _exit(0);
                }
                _exit(0);
            }
            wait(NULL);
            method = "words (the site's index is being brought up to date)";
        }
    }
    if (!n) {
        /* Words alone: posts ranked by how many of the question's words they hold. */
        char *score = NULL, terms[12][64];
        size_t score_size = 0;
        FILE *sc = open_memstream(&score, &score_size);
        int count = 0;
        for (const char *p = query; *p && count < 12; ) {
            size_t len = 0;
            while (*p && !isalnum((unsigned char)*p) && (unsigned char)*p < 0x80) p++;
            while (p[len] && (isalnum((unsigned char)p[len]) || (unsigned char)p[len] >= 0x80) && len < 63) len++;
            if (len >= 3) { snprintf(terms[count], sizeof(terms[0]), "%%%.*s%%", (int)len, p); count++; }
            p += len ? len : 1;
        }
        fputs("SELECT " POST_COLUMNS " FROM (SELECT *, (0", sc);
        for (int i = 0; i < count; i++) fprintf(sc, " + (title LIKE ?%d) * 3 + (body LIKE ?%d)", i + 3, i + 3);
        fputs(") AS hits FROM site_posts WHERE 1=1) WHERE hits > 0 "
              "ORDER BY hits DESC, published_at DESC LIMIT ?2", sc);
        fclose(sc);
        if (count && sqlite3_prepare_v2(db, score, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 2, top);
            for (int i = 0; i < count; i++) sqlite3_bind_text(st, i + 3, terms[i], -1, SQLITE_STATIC);
            n = emit_posts(s, t, site, st, 0);
            sqlite3_finalize(st);
        }
        free(score);
    }
    fprintf(s, "],\"method\":\"%s\"}", method);
    if (!n) fprintf(t, "Nothing on %s matched.", site->title);
    else fprintf(t, "\n\nFound by %s. These are places to read; get_post gives the full text.", method);
    fclose(s); fclose(t);
    tool_result(out, text, structured, 0);
    free(structured); free(text); free(recall);
}

static void call_show_posts(FILE *out, sqlite3 *db, const McpSite *site, const char *ids_json) {
    char *structured = NULL, *text = NULL;
    size_t a = 0, b = 0;
    FILE *s = open_memstream(&structured, &a), *t = open_memstream(&text, &b);
    sqlite3_stmt *st = NULL;
    long n = 0;
    int chosen = ids_json && ids_json[0] == '[' && strcmp(ids_json, "[]");
    fputs("{\"site\":", s);
    site_json(s, site);
    fputs(",\"posts\":[", s);
    if (sqlite3_prepare_v2(db, chosen ?
            "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 "
            "AND id IN (SELECT value FROM json_each(?2)) ORDER BY published_at DESC, id DESC LIMIT 20" :
            "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 "
            "ORDER BY published_at DESC, id DESC LIMIT 6", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        if (chosen) sqlite3_bind_text(st, 2, ids_json, -1, SQLITE_STATIC);
        fprintf(t, "%s\n\n", site->title);
        n = emit_posts(s, t, site, st, 1);
        sqlite3_finalize(st);
    }
    fputs("]}", s);
    if (!n) fputs("No posts to show.", t);
    fclose(s); fclose(t);
    tool_result(out, text, structured, 0);
    free(structured); free(text);
}

/* ---------- the view ---------- */

/* The site's look, as custom properties.  Dark is the same palette turned
 * over, so the accent stays the site's own in either. */
static void style_tokens(FILE *out, const McpSite *site) {
    fprintf(out, ":root{--ground:%s;--ink:%s;--accent:%s;--muted:%s;--hairline:%s;--surface:%s}"
                 "@media (prefers-color-scheme:dark){:root{--ground:%s;--ink:%s;--surface:rgba(255,255,255,.06);"
                 "--hairline:rgba(255,255,255,.16);--muted:rgba(255,255,255,.62)}}"
                 ":root[data-theme=dark]{--ground:%s;--ink:%s;--surface:rgba(255,255,255,.06);--hairline:rgba(255,255,255,.16);--muted:rgba(255,255,255,.62)}"
                 ":root[data-theme=light]{--ground:%s;--ink:%s;--surface:%s;--hairline:%s;--muted:%s}",
            site->ground, site->ink, site->accent, site->muted, site->rule, site->panel,
            site->ink, site->ground, site->ink, site->ground,
            site->ground, site->ink, site->panel, site->rule, site->muted);
}

static const char *SHARED_CSS =
    "*{box-sizing:border-box}html{-webkit-text-size-adjust:100%}"
    "body{margin:0;background:var(--ground);color:var(--ink);font:16px/1.55 ui-serif,Georgia,'Iowan Old Style',serif}"
    "main{max-width:44rem;margin:0 auto;padding:1.5rem 1.25rem 2.5rem}"
    "header.mast{border-bottom:1px solid var(--hairline);padding-bottom:1rem;margin-bottom:1.5rem}"
    ".eyebrow{font:600 .72rem/1.2 ui-sans-serif,system-ui,sans-serif;letter-spacing:.09em;text-transform:uppercase;color:var(--muted);margin:0 0 .4rem}"
    "h1{font-size:1.7rem;line-height:1.15;margin:0;font-weight:650;letter-spacing:-.01em}"
    ".lede{color:var(--muted);margin:.4rem 0 0}"
    "article{padding:1.1rem 0;border-bottom:1px solid var(--hairline)}article:last-of-type{border-bottom:0}"
    "h2{font-size:1.15rem;line-height:1.3;margin:0 0 .15rem;font-weight:650}"
    "time,.meta{font:.78rem/1.3 ui-sans-serif,system-ui,sans-serif;color:var(--muted)}"
    "p{margin:.55rem 0 0}.body{white-space:pre-line}"
    "button,.button{font:600 .82rem/1 ui-sans-serif,system-ui,sans-serif;color:var(--accent);background:none;border:1px solid var(--hairline);"
    "border-radius:999px;padding:.45rem .8rem;margin-top:.7rem;cursor:pointer;text-decoration:none;display:inline-block}"
    "button:hover,.button:hover{border-color:var(--accent)}button:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}"
    "a{color:var(--accent)}code,pre{font:.84rem/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}"
    "pre{background:var(--surface);border:1px solid var(--hairline);border-radius:8px;padding:.8rem 1rem;overflow-x:auto;white-space:pre-wrap;word-break:break-all}"
    "dl{margin:0}dt{font-weight:650;margin-top:1rem}dd{margin:.15rem 0 0;color:var(--muted)}"
    "footer{margin-top:1.75rem;padding-top:1rem;border-top:1px solid var(--hairline);font:.78rem/1.5 ui-sans-serif,system-ui,sans-serif;color:var(--muted)}"
    ".empty{color:var(--muted);padding:2rem 0}"
    "@media (prefers-reduced-motion:no-preference){button,.button{transition:border-color .15s}}";

/* The view a host shows for show_posts.  It is drawn once here from the
 * site's newest posts, so it is never blank, and redraws from whatever the
 * tool returns when the host delivers it.  It holds nothing the text lacks. */
static char *ui_view(sqlite3 *db, const McpSite *site, size_t *len) {
    char *html = NULL;
    FILE *out = open_memstream(&html, len);
    sqlite3_stmt *st = NULL;
    long n = 0;
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<meta name=\"color-scheme\" content=\"light dark\"><title>", out);
    htext(out, site->title);
    fputs("</title>", out); site_browser_icon(out, site); fputs("<style>", out);
    style_tokens(out, site);
    fputs(SHARED_CSS, out);
    fputs("</style></head><body><main><header class=\"mast\"><p class=\"eyebrow\" id=\"host\">", out);
    htext(out, site->host);
    fputs("</p><h1 id=\"name\">", out); htext(out, site->title);
    fputs("</h1><p class=\"lede\" id=\"about\">", out); htext(out, site->tagline);
    fputs("</p></header><section id=\"posts\" aria-live=\"polite\">", out);
    if (sqlite3_prepare_v2(db, "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 "
                               "ORDER BY published_at DESC, id DESC LIMIT 6", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char brief[260];
            const char *when = (const char *)sqlite3_column_text(st, 2);
            excerpt((const char *)sqlite3_column_text(st, 3), brief, sizeof(brief));
            fputs("<article><h2>", out); htext(out, (const char *)sqlite3_column_text(st, 1));
            fprintf(out, "</h2><time>%.10s</time><p>", when ? when : "");
            htext(out, brief);
            fputs("</p></article>", out);
            n++;
        }
        sqlite3_finalize(st);
    }
    if (!n) fputs("<p class=\"empty\">Nothing published here yet.</p>", out);
    fputs("</section><footer>From <span id=\"src\">", out); htext(out, site->host);
    fputs("</span></footer></main><script>", out);
    /* Text is set as text, never as markup: a post cannot script the view. */
    fputs(
        "(function(){var pending={},seq=1,box=document.getElementById('posts');"
        "function send(m){window.parent&&window.parent.postMessage(m,'*')}"
        "function request(method,params){var id=seq++;send({jsonrpc:'2.0',id:id,method:method,params:params||{}});"
        "return new Promise(function(ok){pending[id]=ok})}"
        "function note(method,params){send({jsonrpc:'2.0',method:method,params:params||{}})}"
        "function el(tag,text,cls){var e=document.createElement(tag);if(text!=null)e.textContent=text;if(cls)e.className=cls;return e}"
        "function size(){note('ui/notifications/size-changed',{width:document.documentElement.scrollWidth,height:document.documentElement.scrollHeight})}"
        "function draw(data){if(!data||!Array.isArray(data.posts))return;"
        "if(data.site){document.getElementById('name').textContent=data.site.name||'';document.getElementById('about').textContent=data.site.about||''}"
        "box.textContent='';if(!data.posts.length){box.appendChild(el('p','No posts to show.','empty'));size();return}"
        "data.posts.forEach(function(p){var a=el('article');a.appendChild(el('h2',p.title));a.appendChild(el('time',(p.published_at||'').slice(0,10)));"
        "var body=el('p',p.excerpt);a.appendChild(body);"
        "if(p.body&&p.body.length>(p.excerpt||'').length){var b=el('button','Read all');b.setAttribute('aria-expanded','false');"
        "b.onclick=function(){var open=b.getAttribute('aria-expanded')==='true';body.textContent=open?p.excerpt:p.body;body.className=open?'':'body';"
        "b.textContent=open?'Read all':'Show less';b.setAttribute('aria-expanded',String(!open));size()};a.appendChild(b)}"
        "box.appendChild(a)});size()}"
        "function theme(ctx){var t=ctx&&(ctx.theme||(ctx.hostContext&&ctx.hostContext.theme));if(t==='dark'||t==='light')document.documentElement.setAttribute('data-theme',t)}"
        "window.addEventListener('message',function(ev){var m=ev.data;if(!m||m.jsonrpc!=='2.0')return;"
        "if(m.id!=null&&pending[m.id]&&!m.method){pending[m.id](m.result||{});delete pending[m.id];return}"
        "if(m.method==='ui/notifications/tool-result')draw(m.params&&m.params.structuredContent);"
        "else if(m.method==='ui/notifications/host-context-changed')theme(m.params);"
        "else if(m.method==='ping'&&m.id!=null)send({jsonrpc:'2.0',id:m.id,result:{}})});"
        "if(window.openai&&window.openai.toolOutput)draw(window.openai.toolOutput);"
        "request('ui/initialize',{protocolVersion:'2025-06-18',appInfo:{name:'site-posts',version:'1'},appCapabilities:{}})"
        ".then(function(r){theme(r);note('ui/notifications/initialized');size()});"
        "window.addEventListener('resize',size)})();", out);
    fputs("</script></body></html>", out);
    fclose(out);
    return html;
}

/* ---------- who is asking, how often, and the record of it ---------- */

static void now_iso(char *out, size_t cap, long back_seconds) {
    time_t t = time(NULL) - back_seconds;
    strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
}

/* A caller with no key is told apart by where it calls from, so one cannot
 * spend another's allowance.  The address itself is not kept: only eight
 * characters of a digest of it, salted with the site. */
static void address_mark(const McpSite *site, const char *client, char mark[9]) {
    char salted[400], digest[65];
    snprintf(salted, sizeof(salted), "%s|%s", site->id, client && *client ? client : "unknown");
    bf_sha256_hex((const unsigned char *)salted, strlen(salted), digest);
    snprintf(mark, 9, "%.8s", digest);
}

static void resolve_caller(sqlite3 *db, const McpSite *site, const char *bearer, const char *client, McpCaller *caller) {
    sqlite3_stmt *st = NULL;
    char hash[65], mark[9];
    memset(caller, 0, sizeof(*caller));
    address_mark(site, client, mark);
    snprintf(caller->label, sizeof(caller->label), "public:%s", mark);
    snprintf(caller->key_id, sizeof(caller->key_id), "%s:public:%s", site->id, mark);
    if (!bearer || !*bearer) return;
    caller->refused = 1;
    bf_sha256_hex((const unsigned char *)bearer, strlen(bearer), hash);
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(label,'key'), EXISTS(SELECT 1 FROM _permissions p WHERE p.token_hash=t.token_hash "
                               "AND p.content_type='service_lead' AND p.action='create') FROM _tokens t "
                               "WHERE t.token_hash=?1 AND t.active=1 AND t.scope=?2", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, site->id, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(caller->label, sizeof(caller->label), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(caller->key_id, sizeof(caller->key_id), "%s:%s", site->id, caller->label);
        caller->can_write = sqlite3_column_int(st, 1);
        caller->authenticated = 1;
        caller->refused = 0;
    }
    sqlite3_finalize(st);
}

static long env_number(const char *name, long fallback) {
    const char *value = getenv(name);
    long n = value && *value ? atol(value) : 0;
    return n > 0 ? n : fallback;
}

/* Calls this caller made in the last minute, read from the meter's own store.
 * Returns -1 when no meter is configured. */
static long calls_since(const char *key_id, long seconds);
static long calls_last_minute(const McpCaller *caller) { return calls_since(caller->key_id, 60); }

static long calls_since(const char *key_id, long seconds) {
    const char *meter_db = getenv("BONFYRE_METER_DB");
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char since[32];
    long n = -1;
    if (!meter_db || !*meter_db) return -1;
    if (sqlite3_open_v2(meter_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return 0; }
    sqlite3_busy_timeout(db, 1000);
    now_iso(since, sizeof(since), seconds);
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM events WHERE key_id=?1 AND timestamp>=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, key_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, since, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    } else n = 0;
    sqlite3_close(db);
    return n;
}

static int run_quiet(char *const argv[], char *out, size_t cap) {
    int pipefd[2], status = 0;
    size_t used = 0;
    pid_t pid;
    if (pipe(pipefd)) return -1;
    pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        FILE *quiet;
        dup2(pipefd[1], 1); close(pipefd[0]); close(pipefd[1]);
        quiet = freopen("/dev/null", "w", stderr); (void)quiet;
        execv(argv[0], argv);
        _exit(127);
    }
    close(pipefd[1]);
    for (;;) {
        char sink[1024];
        ssize_t n = read(pipefd[0], out && used + 1 < cap ? out + used : sink, out && used + 1 < cap ? cap - used - 1 : sizeof(sink));
        if (n <= 0) break;
        if (out && used + 1 < cap) used += (size_t)n;
    }
    if (out && cap) out[used] = '\0';
    close(pipefd[0]);
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static void organ_path(char *out, size_t cap, const char *env, const char *name) {
    const char *value = getenv(env), *home = getenv("HOME");
    if (value && *value) snprintf(out, cap, "%s", value);
    else snprintf(out, cap, "%s/.bonfyre/bin/%s", home ? home : "", name);
}

/* One line in the meter for one call: who, which tool, when. */
static void meter_line(const McpSite *site, const char *key_id, const char *receipt, int wrote);
static void meter_call(const McpSite *site, const McpCaller *caller, const char *tool, int wrote) {
    char receipt[160];
    snprintf(receipt, sizeof(receipt), "mcp:%s", tool);
    meter_line(site, caller->key_id, receipt, wrote);
}

static void meter_line(const McpSite *site, const char *key_id, const char *receipt, int wrote) {
    static unsigned long sequence = 0;
    const char *meter_db = getenv("BONFYRE_METER_DB");
    char meter[1024], at[32], attempt[300], realization[200];
    struct timespec ts;
    if (!meter_db || !*meter_db) return;
    organ_path(meter, sizeof(meter), "BONFYRE_METER_BINARY", "bonfyre-meter");
    if (access(meter, X_OK)) return;
    clock_gettime(CLOCK_REALTIME, &ts);
    now_iso(at, sizeof(at), 0);
    snprintf(attempt, sizeof(attempt), "mcp:%s:%lld.%09ld:%d:%lu", site->id, (long long)ts.tv_sec, ts.tv_nsec, (int)getpid(), ++sequence);
    snprintf(realization, sizeof(realization), "site:%s", site->id);
    {
        char *const av[] = {meter, "record", "--db", (char *)meter_db, "--key", (char *)key_id, "--op", wrote ? "site.write" : "site.read",
                            "--provider", "site", "--receipt", (char *)receipt, "--attempt-ref", attempt, "--realization-ref", realization, "--at", at, NULL};
        run_quiet(av, NULL, 0);
    }
}

/* ---------- pages ---------- */

static void page_address(char *out, size_t cap, const McpSite *site, const char *route) {
    snprintf(out, cap, "https://%s%s", site->host, route);
}

static void call_list_pages(FILE *out, sqlite3 *db, const McpSite *site) {
    char *structured = NULL, *text = NULL;
    size_t a = 0, b = 0;
    FILE *s = open_memstream(&structured, &a), *t = open_memstream(&text, &b);
    sqlite3_stmt *st = NULL;
    long n = 0;
    fputs("{\"pages\":[", s);
    if (sqlite3_prepare_v2(db, "SELECT route, title, COALESCE(lede,'') FROM site_page WHERE namespace=?1 AND status='published' ORDER BY id",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char address[400];
            const char *route = (const char *)sqlite3_column_text(st, 0);
            page_address(address, sizeof(address), site, route);
            fprintf(s, "%s{\"route\":", n ? "," : ""); jtext(s, route);
            fputs(",\"title\":", s); jtext(s, (const char *)sqlite3_column_text(st, 1));
            fputs(",\"summary\":", s); jtext(s, (const char *)sqlite3_column_text(st, 2));
            fputs(",\"address\":", s); jtext(s, address);
            fputc('}', s);
            fprintf(t, "%s%s  %s\n    %s\n", n ? "\n" : "", route, (const char *)sqlite3_column_text(st, 1), (const char *)sqlite3_column_text(st, 2));
            n++;
        }
        sqlite3_finalize(st);
    }
    fputs("]}", s);
    if (!n) fprintf(t, "%s has no standing pages; what it has published is in its posts.", site->title);
    fclose(s); fclose(t);
    tool_result(out, text, structured, 0);
    free(structured); free(text);
}

/* A page's text: its fragments in order.  Returns the page's title, or NULL. */
static char *page_read(sqlite3 *db, const McpSite *site, const char *route, char **summary, char **text) {
    sqlite3_stmt *st = NULL;
    char *title = NULL, *body = NULL;
    size_t len = 0;
    FILE *t;
    *summary = NULL; *text = NULL;
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(heading,title), COALESCE(lede,'') FROM site_page WHERE namespace=?1 AND route=?2 "
                               "AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        title = strdup((const char *)sqlite3_column_text(st, 0));
        *summary = strdup((const char *)sqlite3_column_text(st, 1));
    }
    sqlite3_finalize(st);
    if (!title) return NULL;
    t = open_memstream(&body, &len);
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(heading,''), COALESCE(body,'') FROM site_fragment WHERE namespace=?1 AND page=?2 "
                               "AND status='published' ORDER BY position, id", -1, &st, NULL) == SQLITE_OK) {
        int n = 0;
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *heading = (const char *)sqlite3_column_text(st, 0), *piece = (const char *)sqlite3_column_text(st, 1);
            if (n++) fputs("\n\n", t);
            if (heading && *heading) fprintf(t, "%s\n\n", heading);
            fputs(piece ? piece : "", t);
        }
        sqlite3_finalize(st);
    }
    fclose(t);
    *text = body;
    return title;
}

static void call_get_page(FILE *out, sqlite3 *db, const McpSite *site, const char *route) {
    char *summary, *text, *title = route ? page_read(db, site, route, &summary, &text) : NULL;
    char *structured = NULL, *plain = NULL, address[400];
    size_t a = 0, b = 0;
    FILE *s, *t;
    if (!title) { tool_error(out, "No published page at that route on this site. list_pages gives the routes."); return; }
    page_address(address, sizeof(address), site, route);
    s = open_memstream(&structured, &a); t = open_memstream(&plain, &b);
    fputs("{\"route\":", s); jtext(s, route);
    fputs(",\"title\":", s); jtext(s, title);
    fputs(",\"summary\":", s); jtext(s, summary ? summary : "");
    fputs(",\"text\":", s); jtext(s, text ? text : "");
    fputs(",\"address\":", s); jtext(s, address);
    fputc('}', s);
    fprintf(t, "%s\n%s · %s\n\n%s%s%s", title, site->title, address, summary && *summary ? summary : "", summary && *summary ? "\n\n" : "", text ? text : "");
    fclose(s); fclose(t);
    tool_result(out, plain, structured, 0);
    free(structured); free(plain); free(title); free(summary); free(text);
}

/* ---------- what may be written: a message for the owner ---------- */

/* One message into the owner's inbox, by whichever door it came through: an
 * assistant's inquiry, an assistant's booking request, or the contact form.
 * extra is a JSON object's inner members (",\"k\":\"v\"...") or "".  Returns 0
 * and fills reference when the content store took it. */
static int create_lead(const McpSite *site, const char *cms_db, const char *form, const char *name, const char *contact,
                       const char *message, const char *extra, const char *note, char *reference, size_t reference_cap) {
    const char *schemas = getenv("BONFYRE_CMS_SCHEMAS");
    char cms[1024], at[32], answer[2048], brief[260];
    char *fields = NULL, *record = NULL;
    size_t a = 0, b = 0;
    unsigned char noise[4] = {0};
    FILE *f, *r, *random_source;
    int rc;
    organ_path(cms, sizeof(cms), "BONFYRE_CMS_BINARY", "bonfyre-cms");
    if (access(cms, X_OK) || !schemas || !*schemas) return 1;
    random_source = fopen("/dev/urandom", "rb");
    if (random_source) { if (fread(noise, 1, sizeof(noise), random_source) != sizeof(noise)) memset(noise, 0, sizeof(noise)); fclose(random_source); }
    now_iso(at, sizeof(at), 0);
    snprintf(reference, reference_cap, "%s-%lld-%02x%02x%02x%02x", site->id, (long long)time(NULL), noise[0], noise[1], noise[2], noise[3]);
    excerpt(message, brief, sizeof(brief));
    f = open_memstream(&fields, &a);
    fputs("{\"name\":", f); jtext(f, name);
    fputs(",\"contact\":", f); jtext(f, contact);
    fputs(",\"message\":", f); jtext(f, message);
    fputs(extra ? extra : "", f);
    fputc('}', f);
    fclose(f);
    r = open_memstream(&record, &b);
    fputs("{\"lead_key\":", r); jtext(r, reference);
    fputs(",\"site_id\":", r); jtext(r, site->id);
    fputs(",\"form\":", r); jtext(r, form);
    fputs(",\"received_at\":", r); jtext(r, at);
    fputs(",\"fields_json\":", r); jtext(r, fields);
    fputs(",\"brief\":", r); jtext(r, brief);
    fputs(",\"handled\":\"new\",\"note\":", r); jtext(r, note);
    fputc('}', r);
    fclose(r);
    {
        char *const av[] = {cms, "entry", "create", "service_lead", record, "--ns", (char *)site->id, "--db", (char *)cms_db,
                            "--schemas", (char *)schemas, NULL};
        rc = run_quiet(av, answer, sizeof(answer));
    }
    free(fields); free(record);
    return rc || !strstr(answer, "\"created\":true");
}

static int message_ok(const char *name, const char *contact, const char *message) {
    return name && *name && strlen(name) <= 120 && contact && strlen(contact) >= 3 && strlen(contact) <= 200 &&
           message && strlen(message) >= 5 && strlen(message) <= 4000;
}

static const char *NEEDS_WRITE_KEY =
    "This needs a key the site's owner issued for writing. Without one, point the person to the site's contact page instead; "
    "nothing was sent. (The owner issues one with: bonfyre site key NAME --write)";

static void call_send_inquiry(FILE *out, const McpSite *site, const McpCaller *caller, const char *cms_db,
                              const char *name, const char *contact, const char *message) {
    char reference[200], note[200], text[400], *structured = NULL;
    size_t c = 0;
    FILE *s;
    if (!caller->can_write) { tool_error(out, NEEDS_WRITE_KEY); return; }
    if (!message_ok(name, contact, message)) {
        tool_error(out, "An inquiry needs a name, a way to reply (3 to 200 characters) and a message of 5 to 4000 characters. Nothing was sent.");
        return;
    }
    snprintf(note, sizeof(note), "left through the site's assistant endpoint with key %s", caller->label);
    if (create_lead(site, cms_db, "assistant", name, contact, message, "", note, reference, sizeof(reference))) {
        tool_error(out, "The site's store did not accept the message. Nothing was sent.");
        return;
    }
    s = open_memstream(&structured, &c);
    fputs("{\"delivered\":true,\"reference\":", s); jtext(s, reference);
    fputc('}', s);
    fclose(s);
    snprintf(text, sizeof(text), "Delivered to %s's inbox, reference %s. That is the message arriving, not a reply or a promise of one.", site->title, reference);
    tool_result(out, text, structured, 0);
    free(structured);
}

/* ---------- offers: what the site sells, for how much, and how to take it up ----------
 *
 * An offer is a fragment of kind "offer" on one of the site's pages: its
 * heading is what is offered, its body the description, its label the price
 * as the owner words it, and its contract the same price as numbers plus
 * whether a time can be asked for.  Nothing here takes payment or holds a
 * slot: a booking is a request the owner answers. */

#define OFFER_COLUMNS "f.id, COALESCE(f.heading,''), COALESCE(f.body,''), COALESCE(f.label,''), f.page, " \
    "json_extract(f.projection_contract,'$.amount'), COALESCE(json_extract(f.projection_contract,'$.currency'),''), " \
    "COALESCE(json_extract(f.projection_contract,'$.unit'),''), COALESCE(json_extract(f.projection_contract,'$.bookable'),0)"

static void call_list_offers(FILE *out, sqlite3 *db, const McpSite *site) {
    char *structured = NULL, *text = NULL;
    size_t a = 0, b = 0;
    FILE *s = open_memstream(&structured, &a), *t = open_memstream(&text, &b);
    sqlite3_stmt *st = NULL;
    long n = 0;
    fputs("{\"offers\":[", s);
    if (sqlite3_prepare_v2(db, "SELECT " OFFER_COLUMNS " FROM site_fragment f WHERE f.namespace=?1 AND f.kind='offer' AND f.status='published' "
                               "ORDER BY f.position, f.id", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char address[400];
            int bookable = sqlite3_column_int(st, 8);
            page_address(address, sizeof(address), site, (const char *)sqlite3_column_text(st, 4));
            fprintf(s, "%s{\"id\":%lld,\"title\":", n ? "," : "", sqlite3_column_int64(st, 0)); jtext(s, (const char *)sqlite3_column_text(st, 1));
            fputs(",\"description\":", s); jtext(s, (const char *)sqlite3_column_text(st, 2));
            fputs(",\"price\":", s); jtext(s, (const char *)sqlite3_column_text(st, 3));
            if (sqlite3_column_type(st, 5) != SQLITE_NULL) fprintf(s, ",\"amount\":%.2f", sqlite3_column_double(st, 5));
            fputs(",\"currency\":", s); jtext(s, (const char *)sqlite3_column_text(st, 6));
            fputs(",\"unit\":", s); jtext(s, (const char *)sqlite3_column_text(st, 7));
            fprintf(s, ",\"bookable\":%s,\"address\":", bookable ? "true" : "false"); jtext(s, address);
            fputc('}', s);
            fprintf(t, "%s#%lld  %s — %s%s\n    %s\n", n ? "\n" : "", sqlite3_column_int64(st, 0), (const char *)sqlite3_column_text(st, 1),
                    sqlite3_column_text(st, 3)[0] ? (const char *)sqlite3_column_text(st, 3) : "price on request",
                    bookable ? "  (a time can be requested)" : "", (const char *)sqlite3_column_text(st, 2));
            n++;
        }
        sqlite3_finalize(st);
    }
    fputs("]}", s);
    if (!n) fprintf(t, "%s lists nothing for sale here.", site->title);
    else fputs("\n\nPrices are the owner's own words as published. Nothing is charged or reserved through this site.", t);
    fclose(s); fclose(t);
    tool_result(out, text, structured, 0);
    free(structured); free(text);
}

static void call_request_booking(FILE *out, sqlite3 *db, const McpSite *site, const McpCaller *caller, const char *cms_db,
                                 long long offer_id, const char *when, const char *name, const char *contact, const char *note_text) {
    sqlite3_stmt *st = NULL;
    char offer[300] = "", reference[200], note[240], message[4400], text[500], *extra = NULL, *structured = NULL;
    size_t a = 0, c = 0;
    int bookable = 0;
    FILE *e, *s;
    if (!caller->can_write) { tool_error(out, NEEDS_WRITE_KEY); return; }
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(heading,''), COALESCE(json_extract(projection_contract,'$.bookable'),0) FROM site_fragment "
                               "WHERE namespace=?1 AND kind='offer' AND status='published' AND id=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, offer_id);
        if (sqlite3_step(st) == SQLITE_ROW) { snprintf(offer, sizeof(offer), "%s", (const char *)sqlite3_column_text(st, 0)); bookable = sqlite3_column_int(st, 1); }
        sqlite3_finalize(st);
    }
    if (!offer[0]) { tool_error(out, "No such offer on this site. list_offers gives the ids. Nothing was sent."); return; }
    if (!bookable) { tool_error(out, "The owner has not opened that offer to booking requests; send_inquiry reaches them instead. Nothing was sent."); return; }
    if (!when || strlen(when) < 4 || strlen(when) > 120) { tool_error(out, "Say when, as the person put it or as a date and time. Nothing was sent."); return; }
    snprintf(message, sizeof(message), "Booking request for \"%s\" at: %s.%s%s", offer, when, note_text && *note_text ? " " : "", note_text ? note_text : "");
    if (!message_ok(name, contact, message)) { tool_error(out, "A booking request needs a name and a way to reply. Nothing was sent."); return; }
    e = open_memstream(&extra, &a);
    fputs(",\"offer\":", e); jtext(e, offer);
    fprintf(e, ",\"offer_id\":%lld,\"requested_time\":", offer_id); jtext(e, when);
    fclose(e);
    snprintf(note, sizeof(note), "booking request through the site's assistant endpoint with key %s", caller->label);
    if (create_lead(site, cms_db, "booking", name, contact, message, extra, note, reference, sizeof(reference))) {
        free(extra);
        tool_error(out, "The site's store did not accept the request. Nothing was sent.");
        return;
    }
    free(extra);
    s = open_memstream(&structured, &c);
    fputs("{\"requested\":true,\"confirmed\":false,\"reference\":", s); jtext(s, reference);
    fputs(",\"offer\":", s); jtext(s, offer);
    fputc('}', s);
    fclose(s);
    snprintf(text, sizeof(text), "Requested, not confirmed: %s has been asked for \"%s\" at %s (reference %s). Nothing is reserved until the owner replies.",
             site->title, offer, when, reference);
    tool_result(out, text, structured, 0);
    free(structured);
}

/* ---------- text with structure ---------- */

/* A heading as the fragment of an address: lower-case words joined by dashes. */
static void slug_text(FILE *out, const char *text) {
    int dash = 0, any = 0;
    for (; text && *text; text++) {
        if (isalnum((unsigned char)*text)) { if (dash && any) fputc('-', out); fputc(tolower((unsigned char)*text), out); dash = 0; any = 1; }
        else dash = 1;
    }
    if (!any) fputs("section", out);
}

static void hspan(FILE *out, const char *text, size_t n) {
    for (size_t i = 0; i < n; i++) {
        char one[2] = {text[i], '\0'};
        htext(out, one);
    }
}

/* Code, strong text and links inside a line.  A link goes only to this site
 * or to an http address; anything else is left as the words it was. */
static void inline_text(FILE *out, const char *text, size_t n) {
    for (size_t i = 0; i < n; ) {
        const char *at = text + i, *close;
        if (*at == '`' && (close = memchr(at + 1, '`', n - i - 1))) {
            fputs("<code>", out); hspan(out, at + 1, (size_t)(close - at - 1)); fputs("</code>", out);
            i = (size_t)(close - text) + 1;
            continue;
        }
        if (at[0] == '*' && i + 1 < n && at[1] == '*') {
            close = at + 2;
            while (close + 1 < text + n && !(close[0] == '*' && close[1] == '*')) close++;
            if (close + 1 < text + n) {
                fputs("<strong>", out); inline_text(out, at + 2, (size_t)(close - at - 2)); fputs("</strong>", out);
                i = (size_t)(close - text) + 2;
                continue;
            }
        }
        if (*at == '[' && (close = memchr(at, ']', n - i)) && close + 1 < text + n && close[1] == '(') {
            const char *end = memchr(close + 2, ')', (size_t)(text + n - close - 2));
            size_t length = end ? (size_t)(end - close - 2) : 0;
            if (end && length && length < 400 && (close[2] == '/' || close[2] == '#' || !strncmp(close + 2, "https://", 8) || !strncmp(close + 2, "http://", 7))) {
                fputs("<a href=\"", out); hspan(out, close + 2, length); fputs("\">", out);
                inline_text(out, at + 1, (size_t)(close - at - 1)); fputs("</a>", out);
                i = (size_t)(end - text) + 1;
                continue;
            }
        }
        hspan(out, at, 1);
        i++;
    }
}

static int starts_block(const char *line) {
    return !strncmp(line, "```", 3) || !strncmp(line, "### ", 4) || !strncmp(line, "- ", 2) || !strncmp(line, "* ", 2) ||
           !strncmp(line, "> ", 2) || line[0] == '|' || (isdigit((unsigned char)line[0]) && line[1] == '.' && line[2] == ' ');
}

static void table_row(FILE *out, const char *line, size_t n, const char *cell) {
    const char *at = line + 1, *end = line + n;
    fputs("<tr>", out);
    while (at < end) {
        const char *bar = memchr(at, '|', (size_t)(end - at)), *stop = bar ? bar : end, *from = at;
        if (!bar && stop == at) break;
        while (from < stop && *from == ' ') from++;
        while (stop > from && stop[-1] == ' ') stop--;
        fprintf(out, "<%s>", cell); inline_text(out, from, (size_t)(stop - from)); fprintf(out, "</%s>", cell);
        if (!bar) break;
        at = bar + 1;
    }
    fputs("</tr>", out);
}

/* A section's text as a page shows it: paragraphs, lists, tables, notes and
 * code.  Code blocks that carry a label and follow one another are one set,
 * shown one at a time.  Everything written is escaped. */
static void rich_text(FILE *out, const char *body) {
    const char *at = body ? body : "";
    int set = 0;
    while (*at) {
        const char *eol = strchr(at, '\n');
        size_t n = eol ? (size_t)(eol - at) : strlen(at);
        if (!n) { at += eol ? 1 : 0; if (!eol) break; continue; }
        size_t fence_width = bf_markdown_fence_width(at);
        if (fence_width) {
            const char *info = at + fence_width, *gap = memchr(info, ' ', n - fence_width), *code = eol ? eol + 1 : at + n, *close = code;
            size_t lang = gap ? (size_t)(gap - info) : n - fence_width, label = gap ? n - fence_width - lang - 1 : 0;
            int shell = lang == 2 && !strncmp(info, "sh", 2);
            while (*close && !bf_markdown_fence_closes(close, fence_width)) { const char *next = strchr(close, '\n'); close = next ? next + 1 : close + strlen(close); }
            if (label && !set) { fputs("<div class=\"code-set\">", out); set = 1; }
            if (!label && set) { fputs("</div>", out); set = 0; }
            fputs("<figure class=\"code\" data-lang=\"", out); hspan(out, info, lang);
            fputs("\"><figcaption><span>", out);
            if (label) hspan(out, gap + 1, label); else if (shell) fputs("Terminal", out); else hspan(out, info, lang);
            fputs("</span><button type=\"button\" class=\"code-copy\">Copy</button></figcaption><pre><code>", out);
            for (const char *line = code; line < close; ) {
                const char *end = memchr(line, '\n', (size_t)(close - line));
                size_t length = end ? (size_t)(end - line) : (size_t)(close - line);
                const char *note = shell ? strstr(line, "  # ") : NULL;
                if (note && note >= line + length) note = NULL;
                if (shell && length && line[0] != '#' && line[0] != ' ') fputs("<span class=\"typed\">", out);
                hspan(out, line, note ? (size_t)(note - line) : length);
                if (shell && length && line[0] != '#' && line[0] != ' ') fputs("</span>", out);
                if (note) { fputs("<span class=\"aside\">", out); hspan(out, note, length - (size_t)(note - line)); fputs("</span>", out); }
                fputc('\n', out);
                line += length + (end ? 1 : 0);
                if (!end) break;
            }
            fputs("</code></pre></figure>", out);
            at = *close ? (strchr(close, '\n') ? strchr(close, '\n') + 1 : close + strlen(close)) : close;
            continue;
        }
        if (set) { fputs("</div>", out); set = 0; }
        if (!strncmp(at, "### ", 4)) {
            char heading[200];
            snprintf(heading, sizeof(heading), "%.*s", (int)(n - 4), at + 4);
            fputs("<h3 id=\"", out); slug_text(out, heading); fputs("\">", out); inline_text(out, at + 4, n - 4); fputs("</h3>", out);
        } else if (!strncmp(at, "- ", 2) || !strncmp(at, "* ", 2) || (isdigit((unsigned char)at[0]) && at[1] == '.' && at[2] == ' ')) {
            int ordered = isdigit((unsigned char)at[0]);
            size_t lead = ordered ? 3 : 2;
            fputs(ordered ? "<ol>" : "<ul>", out);
            for (;;) {
                fputs("<li>", out); inline_text(out, at + lead, n - lead); fputs("</li>", out);
                if (!eol) break;
                if (ordered ? !(isdigit((unsigned char)eol[1]) && eol[2] == '.' && eol[3] == ' ') : (strncmp(eol + 1, "- ", 2) && strncmp(eol + 1, "* ", 2))) break;
                at = eol + 1; eol = strchr(at, '\n'); n = eol ? (size_t)(eol - at) : strlen(at);
            }
            fputs(ordered ? "</ol>" : "</ul>", out);
        } else if (at[0] == '|') {
            int row = 0, headed = eol && !strncmp(eol + 1, "|-", 2), blank_head = 1;
            for (size_t i = 0; i < n; i++) if (at[i] != '|' && at[i] != ' ') blank_head = 0;
            fputs("<div class=\"table\"><table>", out);
            for (;;) {
                if (row == 0 && headed) { if (!blank_head) { fputs("<thead>", out); table_row(out, at, n, "th"); fputs("</thead>", out); } }
                else if (!(row == 1 && headed)) table_row(out, at, n, "td");
                row++;
                if (!eol || eol[1] != '|') break;
                at = eol + 1; eol = strchr(at, '\n'); n = eol ? (size_t)(eol - at) : strlen(at);
            }
            fputs("</table></div>", out);
        } else if (!strncmp(at, "> ", 2)) {
            fputs("<aside class=\"note\"><p>", out);
            for (;;) {
                inline_text(out, at + 2, n - 2);
                if (!eol || strncmp(eol + 1, "> ", 2)) break;
                fputc(' ', out);
                at = eol + 1; eol = strchr(at, '\n'); n = eol ? (size_t)(eol - at) : strlen(at);
            }
            fputs("</p></aside>", out);
        } else {
            fputs("<p>", out);
            for (;;) {
                inline_text(out, at, n);
                if (!eol || eol[1] == '\n' || !eol[1] || starts_block(eol + 1)) break;
                fputc('\n', out);
                at = eol + 1; eol = strchr(at, '\n'); n = eol ? (size_t)(eol - at) : strlen(at);
            }
            fputs("</p>", out);
        }
        if (!eol) break;
        at = eol + 1;
    }
    if (set) fputs("</div>", out);
}

/* ---------- pages for people ---------- */

static void emit_tokens(FILE *out, sqlite3 *db, const McpSite *site) {
    bf_site_presentation_emit(out, db, site->profile);
}

/* Links to the site's standing pages; shared by the home page and each page. */
static void pages_nav(FILE *out, sqlite3 *db, const char *site_id, const char *current) {
    sqlite3_stmt *st = NULL;
    int n = 0;
    if (sqlite3_prepare_v2(db, "SELECT route, title, COALESCE(lede,'') FROM site_page WHERE namespace=?1 AND status='published' ORDER BY id", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *route = (const char *)sqlite3_column_text(st, 0);
        if (!n++) fprintf(out, "<nav class=\"site-program-nav\" aria-label=\"Pages\"><a href=\"/\"%s>Home</a>", current && !strcmp(current, "/") ? " aria-current=\"page\"" : "");
        fputs("<a href=\"", out); htext(out, route);
        fprintf(out, "\"%s>", current && !strcmp(current, route) ? " aria-current=\"page\"" : "");
        htext(out, (const char *)sqlite3_column_text(st, 1));
        fputs("</a>", out);
        /* What the page is for, beside its link rather than inside it. */
        { const char *lede = (const char *)sqlite3_column_text(st, 2);
          if (lede && *lede) { char line[260]; excerpt(lede, line, sizeof(line)); fputs("<small>", out); htext(out, line); fputs("</small>", out); } }
    }
    sqlite3_finalize(st);
    /* Every site can be written to; the way in is always in the menu. */
    if (!n) fprintf(out, "<nav class=\"site-program-nav\" aria-label=\"Pages\"><a href=\"/\"%s>Home</a>", current && !strcmp(current, "/") ? " aria-current=\"page\"" : "");
    fprintf(out, "<a href=\"/contact\"%s>Contact</a></nav>", current && !strcmp(current, "/contact") ? " aria-current=\"page\"" : "");
}

void bf_site_pages_nav(sqlite3 *db, const char *site_id) { pages_nav(stdout, db, site_id, "/"); }

/* The same bar on every page of a site: its name on the left, the way to
 * write to it on the right.  A manual adds its finder between the two. */
static void site_bar(FILE *out, const char *title, const char *current) {
    fputs("<header class=\"manual-bar\"><a class=\"manual-name\" href=\"/\">", out); htext(out, title);
    fprintf(out, "</a><a class=\"manual-contact\" href=\"/contact\"%s>Contact</a></header>",
            current && !strcmp(current, "/contact") ? " aria-current=\"page\"" : "");
}
void bf_site_bar(const char *title) { site_bar(stdout, title, "/"); }

/* Every page of a site ends the same way: the way to write to it, and its
 * name once more, which the sheet draws the width of the page. */
/* The foot of every page.  A site may keep one line there of its own -- who
 * made it, or that the business is an example -- as a published fragment in
 * the slot "foot"; its label and href, when present, close the line as a link. */
static void site_foot(FILE *out, sqlite3 *db, const char *site_id, const char *title) {
    sqlite3_stmt *st = NULL;
    fputs("<footer class=\"site-foot\"><a class=\"site-foot-ask\" href=\"/contact\">Contact</a>", out);
    if (db && site_id && sqlite3_prepare_v2(db, "SELECT CASE WHEN kind='cleared' THEN '' ELSE COALESCE(body,'') END, COALESCE(label,''), COALESCE(href,'') FROM site_fragment "
                                                "WHERE namespace=?1 AND slot='foot' AND status='published' ORDER BY id DESC LIMIT 1",
                                            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site_id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *label = (const char *)sqlite3_column_text(st, 1), *href = (const char *)sqlite3_column_text(st, 2);
            const char *line = (const char *)sqlite3_column_text(st, 0);
            if (!line || !*line) { sqlite3_finalize(st); st = NULL; goto name; }
            fputs("<p class=\"site-foot-note\">", out); htext(out, line);
            if (label && *label && href && !strncmp(href, "https://", 8)) {
                fputs(" <a href=\"", out); htext(out, href); fputs("\">", out); htext(out, label); fputs("</a>", out);
            }
            fputs("</p>", out);
        }
        sqlite3_finalize(st);
    }
name:
    fputs("<p class=\"site-foot-name\" aria-hidden=\"true\">", out);
    htext(out, title); fputs("</p></footer>", out);
}

/* The pages of a site in the order a reader meets them: group by group, in
 * the order the groups first appeared. */
#define PAGES_SQL \
    "SELECT p.route, p.title, COALESCE(p.eyebrow,''), COALESCE(p.lede,''), " \
    "COALESCE((SELECT group_concat(f.heading,' ') FROM site_fragment f WHERE f.namespace=p.namespace AND f.page=p.route AND f.status='published'),'') " \
    "FROM site_page p WHERE p.namespace=?1 AND p.status='published' " \
    "ORDER BY (SELECT min(q.id) FROM site_page q WHERE q.namespace=p.namespace AND q.status='published' AND COALESCE(q.eyebrow,'')=COALESCE(p.eyebrow,'')), p.id"

static void page_sections(FILE *out, sqlite3 *db, const McpSite *site, const char *route, int outline) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(heading,''), COALESCE(body,''), kind, COALESCE(label,''), "
                               "COALESCE(json_extract(projection_contract,'$.bookable'),0) FROM site_fragment WHERE namespace=?1 AND page=?2 "
                               "AND status='published' ORDER BY position, id", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *heading = (const char *)sqlite3_column_text(st, 0), *kind = (const char *)sqlite3_column_text(st, 2);
        if (outline) {
            if (heading && *heading && !(kind && !strcmp(kind, "offer"))) {
                fputs("<a href=\"#", out); slug_text(out, heading); fputs("\">", out); htext(out, heading); fputs("</a>", out);
            }
            continue;
        }
        if (kind && !strcmp(kind, "offer")) {
            const char *price = (const char *)sqlite3_column_text(st, 3);
            fputs("<article class=\"offer\"><div class=\"offer-head\"><h2>", out); htext(out, heading);
            fputs("</h2><p class=\"offer-price\">", out); htext(out, price && *price ? price : "Price on request");
            fputs("</p></div><p>", out); htext(out, (const char *)sqlite3_column_text(st, 1));
            fputs("</p><a class=\"offer-action\" href=\"/contact?about=", out);
            for (const char *c = heading; c && *c; c++) {
                if (isalnum((unsigned char)*c)) fputc(*c, out); else fprintf(out, "%%%02X", (unsigned char)*c);
            }
            fprintf(out, "\">%s</a></article>", sqlite3_column_int(st, 4) ? "Request a time" : "Ask about this");
            continue;
        }
        if (heading && *heading) {
            fputs("<section id=\"", out); slug_text(out, heading); fputs("\"><h2><a href=\"#", out); slug_text(out, heading);
            fputs("\">", out); htext(out, heading); fputs("</a></h2>", out);
        } else fputs("<section>", out);
        rich_text(out, (const char *)sqlite3_column_text(st, 1));
        fputs("</section>", out);
    }
    sqlite3_finalize(st);
}

#include "site_search.h"

/* The front page of a site that sells something shows what it sells, in the
 * same lines its offers page uses, and then closes like every other page. */
void bf_site_home_extras(sqlite3 *db, const char *site_id, const char *title) {
    McpSite site;
    sqlite3_stmt *st = NULL;
    int offers = 0;
    memset(&site, 0, sizeof(site));
    snprintf(site.id, sizeof(site.id), "%s", site_id ? site_id : "");
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM site_fragment WHERE namespace=?1 AND page='/offers' AND kind='offer' AND status='published'",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site.id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) offers = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);
    }
    if (offers) {
        fputs("<section class=\"site-page site-home-offers\"><h2 class=\"site-home-heading\"><a href=\"/offers\">Offers</a></h2>", stdout);
        page_sections(stdout, db, &site, "/offers", 0);
        fputs("</section>", stdout);
    }
    site_foot(stdout, db, site.id, title);
}

/* A site with a shelf of pages reads as a manual: every page in a grouped
 * list that can be searched, the page itself, and its own outline. */
static void manual_page(FILE *out, sqlite3 *db, const McpSite *site, const char *route, const char *title, const char *summary) {
    sqlite3_stmt *st = NULL;
    char group[160] = "\x01", mine[160] = "", before[2][200] = {"", ""}, after[2][200] = {"", ""}, last[2][200] = {"", ""};
    int found = 0;
    fputs("<header class=\"manual-bar\"><a class=\"manual-name\" href=\"/\">", out); htext(out, site->title);
    fputs("</a><label class=\"manual-find\"><span>Find</span><input type=\"search\" placeholder=\"Find a page or a section\" autocomplete=\"off\" "
          "aria-label=\"Find a page or a section\"><kbd>/</kbd></label><a class=\"manual-contact\" href=\"/contact\">Contact</a></header>"
          "<div class=\"manual-frame\"><nav class=\"manual-pages\" aria-label=\"Pages\">", out);
    if (sqlite3_prepare_v2(db, PAGES_SQL, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *r = (const char *)sqlite3_column_text(st, 0), *t = (const char *)sqlite3_column_text(st, 1);
            const char *g = (const char *)sqlite3_column_text(st, 2);
            int current = !strcmp(r, route);
            if (strcmp(g, group)) {
                snprintf(group, sizeof(group), "%s", g);
                fputs("<p>", out); htext(out, *g ? g : "Pages"); fputs("</p>", out);
            }
            fputs("<a href=\"", out); htext(out, r); fputs("\" data-find=\"", out); htext(out, t); fputc(' ', out);
            htext(out, (const char *)sqlite3_column_text(st, 3)); fputc(' ', out); htext(out, (const char *)sqlite3_column_text(st, 4));
            fprintf(out, "\"%s>", current ? " aria-current=\"page\"" : ""); htext(out, t); fputs("</a>", out);
            if (current) {
                found = 1; snprintf(mine, sizeof(mine), "%s", g);
                snprintf(before[0], sizeof(before[0]), "%s", last[0]); snprintf(before[1], sizeof(before[1]), "%s", last[1]);
            } else if (found && !after[0][0]) {
                snprintf(after[0], sizeof(after[0]), "%s", r); snprintf(after[1], sizeof(after[1]), "%s", t);
            }
            snprintf(last[0], sizeof(last[0]), "%s", r); snprintf(last[1], sizeof(last[1]), "%s", t);
        }
        sqlite3_finalize(st);
    }
    fputs("<p class=\"manual-none\" hidden>Nothing here by that name.</p></nav><article class=\"site-page manual-article\">", out);
    if (mine[0]) { fputs("<p class=\"manual-group\">", out); htext(out, mine); fputs("</p>", out); }
    fputs("<h1>", out); htext(out, title); fputs("</h1>", out);
    if (summary && *summary) { fputs("<p class=\"lede\">", out); inline_text(out, summary, strlen(summary)); fputs("</p>", out); }
    page_sections(out, db, site, route, 0);
    fputs("<nav class=\"manual-turn\" aria-label=\"Previous and next\">", out);
    if (before[0][0]) { fputs("<a rel=\"prev\" href=\"", out); htext(out, before[0]); fputs("\"><small>Previous</small>", out); htext(out, before[1]); fputs("</a>", out); }
    else fputs("<span></span>", out);
    if (after[0][0]) { fputs("<a rel=\"next\" href=\"", out); htext(out, after[0]); fputs("\"><small>Next</small>", out); htext(out, after[1]); fputs("</a>", out); }
    fputs("</nav></article><nav class=\"manual-outline\" aria-label=\"On this page\"><p>On this page</p>", out);
    page_sections(out, db, site, route, 1);
    fputs("</nav></div>", out);
}

static int serve_page(sqlite3 *db, const McpSite *site, const char *route) {
    char *summary, *text, *title = page_read(db, site, route, &summary, &text), *html = NULL;
    size_t len = 0;
    sqlite3_stmt *st = NULL;
    FILE *out;
    int manual = 0;
    if (!title) return 0;
    if (sqlite3_prepare_v2(db, "SELECT count(*) >= 6 OR COALESCE(max(length(COALESCE(eyebrow,''))),0) > 0 FROM site_page WHERE namespace=?1 AND status='published'",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) manual = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);
    }
    out = open_memstream(&html, &len);
    fputs("<!doctype html><html lang=\"en\" class=\"site-program\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>", out);
    htext(out, title); fputs(" · ", out); htext(out, site->title);
    fputs("</title>", out);
    if (summary && *summary) { fputs("<meta name=\"description\" content=\"", out); htext(out, summary); fputs("\">", out); }
    site_browser_icon(out, site); fputs("<link rel=\"stylesheet\" href=\"/assets/own-site.css?v=6\"><link rel=\"mcp\" href=\"/.well-known/mcp/server-card.json\">"
          "<script src=\"/assets/own-site-agent.js\" defer></script><script src=\"/assets/own-site-reading.js\" defer></script>", out);
    emit_tokens(out, db, site);
    fprintf(out, "</head><body class=\"site-program-body\"><main class=\"site-composition%s\" data-site=\"", manual ? " site-manual" : "");
    htext(out, site->id); fputs("\"", out); bf_site_presentation_attributes(out,db,site->profile); fputs(">", out);
    if (manual) manual_page(out, db, site, route, title, summary);
    else {
        site_bar(out, site->title, route);
        fputs("<header class=\"site-program-masthead\"><p><a href=\"/\">", out); htext(out, site->title);
        fputs("</a></p><h1>", out); htext(out, title); fputs("</h1></header>", out);
        pages_nav(out, db, site->id, route);
        fputs("<section class=\"site-page\">", out);
        if (summary && *summary) { fputs("<p class=\"lede\">", out); htext(out, summary); fputs("</p>", out); }
        page_sections(out, db, site, route, 0);
        fputs("</section>", out);
    }
    site_foot(out, db, site->id, site->title);
    fputs("</main></body></html>", out);
    fclose(out);
    reply(200, "text/html; charset=utf-8", html, len);
    free(html); free(title); free(summary); free(text);
    return 1;
}

/* ---------- JSON-RPC ---------- */

static void rpc_error(const char *id_json, int code, const char *message) {
    char *body = NULL;
    size_t len = 0;
    FILE *out = open_memstream(&body, &len);
    fprintf(out, "{\"jsonrpc\":\"2.0\",\"id\":%s,\"error\":{\"code\":%d,\"message\":", id_json ? id_json : "null", code);
    jtext(out, message);
    fputs("}}\n", out);
    fclose(out);
    reply(code == -32700 || code == -32600 ? 400 : 200, "application/json", body, len);
    free(body);
}

/* One value out of the request, as text.  The caller frees it. */
static char *request_value(sqlite3 *db, const char *body, const char *sql) {
    sqlite3_stmt *st = NULL;
    char *value = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, body, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL)
        value = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return value;
}

static void serve_rpc(sqlite3 *db, const McpSite *site, const char *cms_db, const char *body, const McpCaller *caller) {
    char *valid, *is_array, *method, *id, *out_body = NULL;
    size_t out_len = 0;
    FILE *out;
    if (!body || !*body) { rpc_error(NULL, -32700, "the request has no body"); return; }
    valid = request_value(db, body, "SELECT json_valid(?1)");
    if (!valid || strcmp(valid, "1")) { free(valid); rpc_error(NULL, -32700, "the request is not JSON"); return; }
    free(valid);
    is_array = request_value(db, body, "SELECT json_type(?1)");
    if (!is_array || strcmp(is_array, "object")) { free(is_array); rpc_error(NULL, -32600, "one request at a time"); return; }
    free(is_array);
    method = request_value(db, body, "SELECT json_extract(?1,'$.method')");
    id = request_value(db, body, "SELECT CASE WHEN json_type(?1,'$.id') IS NULL THEN NULL ELSE json_quote(json_extract(?1,'$.id')) END");
    if (!method) { rpc_error(id, -32600, "the request names no method"); free(id); return; }
    if (!id) {
        /* A notification asks for nothing back. */
        reply(202, "application/json", "", 0);
        free(method);
        return;
    }
    out = open_memstream(&out_body, &out_len);
    fprintf(out, "{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":", id);
    if (!strcmp(method, "initialize")) {
        char *asked = request_value(db, body, "SELECT json_extract(?1,'$.params.protocolVersion')");
        const char *version = asked && (!strcmp(asked, "2025-06-18") || !strcmp(asked, "2025-03-26") || !strcmp(asked, "2024-11-05")) ? asked : MCP_LATEST;
        fprintf(out, "{\"protocolVersion\":\"%s\",\"capabilities\":{\"tools\":{\"listChanged\":false},\"resources\":{\"listChanged\":false,\"subscribe\":false}},"
                     "\"serverInfo\":{\"name\":", version);
        jtext(out, site->id);
        fputs(",\"title\":", out); jtext(out, site->title);
        fputs(",\"version\":\"1\"", out); site_icon_metadata(out, site); fputs("},\"instructions\":", out);
        {
            char text[1500];
            if (site->reading) snprintf(text, sizeof(text), "This is Aurekai's reading of %s, at %s: titles, dates and links read from the publisher's "
                     "public feed. It is not the publisher's own site and nothing here is their statement to you. Use list_posts and search_posts to find "
                     "items and get_post for one; always give the person the original link, which is in each item. Say that it was read by Aurekai, "
                     "and when: an item here may since have changed or been withdrawn at its source.", site->title, site->host);
            else snprintf(text, sizeof(text), "This is %s (%s). Everything here is read-only and is what the site itself has published. "
                     "Start with site_info; list_pages and get_page give its standing pages, list_posts, search_posts and get_post its dated writing. "
                     "list_offers gives what it sells and for how much. Use show_posts only to hand the person a view. send_inquiry and request_booking "
                     "leave the owner a message and need a key issued for writing; a booking is a request, never a reservation. "
                     "A search result is a place to read, not an answer; quote by title and address.", site->title, site->host);
            jtext(out, text);
        }
        fputc('}', out);
        free(asked);
    } else if (!strcmp(method, "ping")) {
        fputs("{}", out);
    } else if (!strcmp(method, "tools/list")) {
        tools_list(out, site);
    } else if (!strcmp(method, "tools/call")) {
        char *name = request_value(db, body, "SELECT json_extract(?1,'$.params.name')");
        long used = calls_last_minute(caller);
        long allowed = caller->authenticated ? env_number("BONFYRE_SITE_MCP_KEYED_PER_MINUTE", 600)
                                             : env_number("BONFYRE_SITE_MCP_PUBLIC_PER_MINUTE", 60);
        int wrote = name && (!strcmp(name, "send_inquiry") || !strcmp(name, "request_booking"));
        if (used >= allowed) {
            /* The count is the meter's own; the caller is told when to return. */
            char limit_body[400];
            int n = snprintf(limit_body, sizeof(limit_body), "{\"jsonrpc\":\"2.0\",\"id\":%s,\"error\":{\"code\":-32029,\"message\":"
                             "\"%ld calls in the last minute is this caller's limit; ask again in a minute\"}}\n", id, allowed);
            fclose(out); free(out_body); free(name); free(method); free(id);
            reply(429, "application/json", limit_body, (size_t)n);
            return;
        }
        if (!name) tool_error(out, "tools/call names no tool.");
        else if (site->reading && (!strcmp(name, "list_pages") || !strcmp(name, "get_page") || !strcmp(name, "list_offers") ||
                                   !strcmp(name, "send_inquiry") || !strcmp(name, "request_booking")))
            tool_error(out, "This address holds the estate's reading of a publisher's feed, not the publisher's own site: "
                            "it has no pages, offers or inbox. Their own site is where to reach them.");
        else if (!strcmp(name, "list_pages")) call_list_pages(out, db, site);
        else if (!strcmp(name, "list_offers")) call_list_offers(out, db, site);
        else if (!strcmp(name, "request_booking")) {
            char *offer = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.offer_id')");
            char *when = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.when')");
            char *who = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.name')");
            char *contact = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.contact')");
            char *extra_note = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.note')");
            call_request_booking(out, db, site, caller, cms_db, offer ? atoll(offer) : 0, when, who, contact, extra_note);
            free(offer); free(when); free(who); free(contact); free(extra_note);
        }
        else if (!strcmp(name, "get_page")) {
            char *route = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.route')");
            call_get_page(out, db, site, route);
            free(route);
        } else if (!strcmp(name, "send_inquiry")) {
            char *who = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.name')");
            char *contact = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.contact')");
            char *message = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.message')");
            call_send_inquiry(out, site, caller, cms_db, who, contact, message);
            free(who); free(contact); free(message);
        }
        else if (!strcmp(name, "site_info")) call_site_info(out, site);
        else if (!strcmp(name, "list_posts")) {
            char *limit = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.limit')");
            char *offset = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.offset')");
            call_list_posts(out, db, site, limit ? atol(limit) : 10, offset ? atol(offset) : 0);
            free(limit); free(offset);
        } else if (!strcmp(name, "get_post")) {
            char *post = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.id')");
            if (!post) tool_error(out, "get_post needs an id.");
            else call_get_post(out, db, site, atoll(post));
            free(post);
        } else if (!strcmp(name, "search_posts")) {
            char *query = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.query')");
            char *top = request_value(db, body, "SELECT json_extract(?1,'$.params.arguments.top')");
            call_search(out, db, site, cms_db, query, top ? atol(top) : 5);
            free(query); free(top);
        } else if (!strcmp(name, "show_posts")) {
            char *ids = request_value(db, body, "SELECT CASE WHEN json_type(?1,'$.params.arguments.ids')='array' THEN json_extract(?1,'$.params.arguments.ids') END");
            call_show_posts(out, db, site, ids);
            free(ids);
        } else {
            fclose(out); free(out_body); free(name);
            rpc_error(id, -32602, "this site has no tool of that name");
            free(method); free(id);
            return;
        }
        if (name) meter_call(site, caller, name, wrote);
        free(name);
    } else if (!strcmp(method, "resources/list")) {
        sqlite3_stmt *st = NULL;
        fputs("{\"resources\":[{\"uri\":\"" UI_URI "\",\"name\":\"posts-view\",\"title\":\"Posts, as a view\",\"mimeType\":\"" UI_MIME "\"", out);
        site_icon_metadata(out, site);
        fputs("}", out);
        if (sqlite3_prepare_v2(db, "SELECT id, title FROM site_posts WHERE 1=1 "
                                   "ORDER BY published_at DESC, id DESC LIMIT 200", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                fprintf(out, ",{\"uri\":\"site://%s/post/%lld\",\"name\":\"post-%lld\",\"title\":", site->id, sqlite3_column_int64(st, 0), sqlite3_column_int64(st, 0));
                jtext(out, (const char *)sqlite3_column_text(st, 1));
                fputs(",\"mimeType\":\"text/markdown\"}", out);
            }
            sqlite3_finalize(st);
        }
        fputs("]}", out);
    } else if (!strcmp(method, "resources/read")) {
        char *uri = request_value(db, body, "SELECT json_extract(?1,'$.params.uri')");
        char prefix[200];
        snprintf(prefix, sizeof(prefix), "site://%s/post/", site->id);
        if (uri && !strcmp(uri, UI_URI)) {
            size_t view_len = 0;
            char *view = ui_view(db, site, &view_len);
            fputs("{\"contents\":[{\"uri\":\"" UI_URI "\",\"mimeType\":\"" UI_MIME "\",\"text\":", out);
            jtext(out, view ? view : "");
            fputs("}]}", out);
            free(view);
        } else if (uri && !strncmp(uri, prefix, strlen(prefix))) {
            sqlite3_stmt *st = NULL;
            int found = 0;
            if (sqlite3_prepare_v2(db, "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 AND id=?2",
                                   -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
                sqlite3_bind_int64(st, 2, atoll(uri + strlen(prefix)));
                if (sqlite3_step(st) == SQLITE_ROW) {
                    char *text = NULL;
                    size_t n = 0;
                    FILE *t = open_memstream(&text, &n);
                    const char *when = (const char *)sqlite3_column_text(st, 2);
                    fprintf(t, "# %s\n\n*%.10s · %s*\n\n%s\n", (const char *)sqlite3_column_text(st, 1), when ? when : "", site->title,
                            (const char *)sqlite3_column_text(st, 3));
                    fclose(t);
                    fputs("{\"contents\":[{\"uri\":", out); jtext(out, uri);
                    fputs(",\"mimeType\":\"text/markdown\",\"text\":", out); jtext(out, text);
                    fputs("}]}", out);
                    free(text);
                    found = 1;
                }
                sqlite3_finalize(st);
            }
            if (!found) { fclose(out); free(out_body); rpc_error(id, -32002, "no such resource on this site"); free(uri); free(method); free(id); return; }
        } else { fclose(out); free(out_body); rpc_error(id, -32002, "no such resource on this site"); free(uri); free(method); free(id); return; }
        free(uri);
    } else if (!strcmp(method, "resources/templates/list")) {
        fprintf(out, "{\"resourceTemplates\":[{\"uriTemplate\":\"site://%s/post/{id}\",\"name\":\"post\",\"title\":\"A post by id\",\"mimeType\":\"text/markdown\"}]}", site->id);
    } else if (!strcmp(method, "prompts/list")) {
        fputs("{\"prompts\":[]}", out);
    } else {
        fclose(out); free(out_body);
        rpc_error(id, -32601, "this site does not answer that method");
        free(method); free(id);
        return;
    }
    fputs("}\n", out);
    fclose(out);
    reply(200, "application/json", out_body, out_len);
    free(out_body); free(method); free(id);
}

/* ---------- the same facts for a person, and for a plain reader ---------- */

static void serve_human(const McpSite *site, const char *endpoint) {
    char *html = NULL;
    size_t len = 0;
    FILE *out = open_memstream(&html, &len);
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<meta name=\"color-scheme\" content=\"light dark\"><title>Connect an assistant · ", out);
    htext(out, site->title);
    fputs("</title>", out); site_browser_icon(out, site); fputs("<style>", out);
    style_tokens(out, site);
    fputs(SHARED_CSS, out);
    fputs("</style></head><body><main><header class=\"mast\"><p class=\"eyebrow\">For AI assistants</p><h1>", out);
    htext(out, site->title);
    fputs(" can be read by an assistant</h1><p class=\"lede\">Add this address to an assistant that speaks the Model Context Protocol "
          "and it can read this site, search it, and show it to you.</p></header>"
          "<h2>The address</h2><pre id=\"endpoint\">", out);
    htext(out, endpoint); fputs("</pre>", out);
    fputs("<h2 style=\"margin-top:1.5rem\">Claude Code</h2><pre>claude mcp add --transport http ", out);
    htext(out, site->id); fputc(' ', out); htext(out, endpoint); fputs("</pre>", out);
    fputs("<h2 style=\"margin-top:1.5rem\">Any client that takes a configuration</h2><pre>{\n  \"mcpServers\": {\n    \"", out);
    htext(out, site->id); fputs("\": { \"type\": \"http\", \"url\": \"", out);
    htext(out, endpoint); fputs("\" }\n  }\n}</pre>", out);
    fputs("<h2 style=\"margin-top:1.5rem\">What an assistant may ask</h2><dl>"
          "<dt>site_info</dt><dd>What this site is and how much it has published.</dd>"
          "<dt>list_posts</dt><dd>Titles, dates and opening lines, newest first.</dd>"
          "<dt>get_post</dt><dd>The full text of one post.</dd>"
          "<dt>search_posts</dt><dd>Posts that match a question, and how they were found.</dd>"
          "<dt>show_posts</dt><dd>A readable view of chosen posts, in this site's own look, where the assistant can show one.</dd></dl>", out);
    fprintf(out, "<footer>%ld post%s published. Also for plain readers: <a href=\"/llms.txt\">/llms.txt</a>. "
                 "Back to <a href=\"/\">", site->posts, site->posts == 1 ? "" : "s");
    htext(out, site->title);
    fputs("</a>.</footer></main></body></html>", out);
    fclose(out);
    reply(200, "text/html; charset=utf-8", html, len);
    free(html);
}

static void serve_llms(sqlite3 *db, const McpSite *site) {
    char *text = NULL;
    size_t len = 0;
    FILE *out = open_memstream(&text, &len);
    sqlite3_stmt *st = NULL;
    fprintf(out, "# %s\n\n", site->title);
    if (site->tagline[0]) fprintf(out, "> %s\n\n", site->tagline);
    fprintf(out, "Address: https://%s/\nFor assistants (Model Context Protocol): https://%s/mcp\n\n", site->host, site->host);
    if (sqlite3_prepare_v2(db, "SELECT route FROM site_page WHERE namespace=?1 AND status='published' ORDER BY id", -1, &st, NULL) == SQLITE_OK) {
        int pages = 0;
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char *summary, *text, *title = page_read(db, site, (const char *)sqlite3_column_text(st, 0), &summary, &text);
            if (!title) continue;
            if (!pages++) fputs("## Pages\n\n", out);
            fprintf(out, "### %s\n\n%s%s%s\n\n", title, summary && *summary ? summary : "", summary && *summary ? "\n\n" : "", text ? text : "");
            free(title); free(summary); free(text);
        }
        sqlite3_finalize(st);
        st = NULL;
    }
    fputs("## Posts\n\n", out);
    if (sqlite3_prepare_v2(db, "SELECT " POST_COLUMNS " FROM site_posts WHERE 1=1 "
                               "ORDER BY published_at DESC, id DESC LIMIT 100", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *when = (const char *)sqlite3_column_text(st, 2);
            fprintf(out, "### %s\n\n%.10s\n\n%s\n\n", (const char *)sqlite3_column_text(st, 1), when ? when : "",
                    (const char *)sqlite3_column_text(st, 3));
        }
        sqlite3_finalize(st);
    }
    fclose(out);
    reply(200, "text/markdown; charset=utf-8", text, len);
    free(text);
}

/* The MCP server card: where the endpoint is and what it offers, at the path
 * agents look for it.  The tool list is the endpoint's own, not a second copy. */
static void serve_server_card(const McpSite *site, const char *endpoint) {
    char *body = NULL, *tools = NULL;
    size_t len = 0, tools_len = 0;
    FILE *out = open_memstream(&body, &len), *t = open_memstream(&tools, &tools_len);
    tools_list(t, site);
    fclose(t);
    fputs("{\"$schema\":\"https://static.modelcontextprotocol.io/schemas/mcp-server-card/v1.json\",\"name\":", out); jtext(out, site->id);
    fputs(",\"title\":", out); jtext(out, site->title);
    fputs(",\"description\":", out); jtext(out, site->tagline[0] ? site->tagline : "A site that answers assistants about what it has published");
    site_icon_metadata(out, site);
    fputs(",\"version\":\"1\",\"protocolVersion\":\"" MCP_LATEST "\",\"transport\":{\"type\":\"streamable-http\",\"endpoint\":", out); jtext(out, endpoint);
    fprintf(out, "},\"endpoint\":"); jtext(out, endpoint);
    fprintf(out, ",\"capabilities\":{\"tools\":{\"listChanged\":false},\"resources\":{\"listChanged\":false,\"subscribe\":false}},"
                 "\"authentication\":{\"required\":false,\"schemes\":[\"bearer\"],\"note\":\"reading needs no key; %s\"},",
            site->reading ? "nothing here can be written" : "leaving the owner a message needs a key the owner issued");
    /* tools_list writes {"tools":[...]}; the card carries the array. */
    fprintf(out, "%.*s,\"prompts\":[]}\n", (int)(tools_len > 2 ? tools_len - 2 : 0), tools ? tools + 1 : "");
    fclose(out);
    reply(200, "application/json; charset=utf-8", body, len);
    free(body); free(tools);
}

static void serve_card(const McpSite *site) {
    char *body = NULL;
    size_t len = 0;
    FILE *out = open_memstream(&body, &len);
    fputs("{\"name\":", out); jtext(out, site->title);
    fputs(",\"description\":", out); jtext(out, site->tagline[0] ? site->tagline : "A site that publishes posts");
    fprintf(out, ",\"url\":\"https://%s/\",\"version\":\"1\",\"interfaces\":[{\"protocol\":\"mcp\",\"transport\":\"streamable-http\","
                 "\"url\":\"https://%s/mcp\"}],\"capabilities\":{\"streaming\":false,\"pushNotifications\":false},"
                 "\"defaultInputModes\":[\"application/json\"],\"defaultOutputModes\":[\"application/json\",\"text/markdown\"],\"skills\":["
                 "{\"id\":\"read\",\"name\":\"Read what the site has published\",\"description\":\"List posts, read one, search them\",\"tags\":[\"read-only\"]}]}\n",
            site->host, site->host);
    fclose(out);
    reply(200, "application/json; charset=utf-8", body, len);
    free(body);
}

/* On this machine, names ending in .localhost reach a browser and little
 * else: most programs cannot look them up.  So on a loopback address a site
 * is also reachable by path, /mcp/NAME, which every client can open. */
static int loopback_host(const char *host_header) {
    char host[256], *colon;
    snprintf(host, sizeof(host), "%s", host_header ? host_header : "");
    colon = strrchr(host, ':');
    if (colon && !strchr(colon, ']')) *colon = '\0';
    return !strcmp(host, "localhost") || !strcmp(host, "127.0.0.1") || !strcmp(host, "[::1]");
}

static int site_origin_allowed(const char *origin, const char *host, const McpSite *site) {
    const char *authority, *port;
    char named[400];
    /* CLI and native MCP clients need not send an Origin. An opaque browser
     * origin cannot identify a site and is not admitted as a named origin. */
    if (!origin || !*origin) return 1;
    if (!strncmp(origin, "https://", 8)) authority = origin + 8;
    else if (!strncmp(origin, "http://", 7)) authority = origin + 7;
    else return 0;
    if (!*authority || !host || !*host) return 0;
    for (const char *p = authority; *p; p++)
        if (*p == '/' || *p == '@' || *p == '?' || *p == '#' || isspace((unsigned char)*p)) return 0;
    if (!strcasecmp(authority, host)) return 1;
    /* The local path endpoint also accepts this site's named browser host
     * on the same port. It does not accept another site's host on that port. */
    if (!loopback_host(host)) return 0;
    port = strrchr(host, ':');
    if (port && strchr(port, ']')) port = NULL;
    snprintf(named, sizeof(named), "%s%s", site->host, port ? port : "");
    return !strcasecmp(authority, named);
}

/* ---------- the contact page: the same inbox, for people ---------- */

/* A site under one of the estate's territories is handed out by the edge,
 * which only answers questions.  Its contact form therefore posts to the
 * territory's member host, which reaches this server, at a path that names the
 * site; the answer is drawn against the site's own address. */
static char relay_base[300] = "";
static int member_host_for(const char *site_host, char *out, size_t cap) {
    static const struct { const char *root, *member; } territories[] = {
        {"aurekai.life", "auth.aurekai.life"}, {"aurekai.live", "auth.aurekai.live"},
        {"aurekai.shop", "auth.aurekai.shop"}, {"aurekai.work", "signin.aurekai.work"}};
    size_t n = site_host ? strlen(site_host) : 0;
    for (size_t i = 0; i < sizeof(territories) / sizeof(territories[0]); i++) {
        size_t r = strlen(territories[i].root);
        if (n > r + 1 && site_host[n - r - 1] == '.' && !strcmp(site_host + n - r, territories[i].root)) {
            snprintf(out, cap, "%s", territories[i].member);
            return 1;
        }
    }
    return 0;
}

static void page_open(FILE *out, const McpSite *site, sqlite3 *db, const char *title, const char *route) {
    fputs("<!doctype html><html lang=\"en\" class=\"site-program\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>", out);
    htext(out, title); fputs(" · ", out); htext(out, site->title);
    fputs("</title>", out);
    if (relay_base[0]) { fputs("<base href=\"https://", out); htext(out, relay_base); fputs("/\">", out); }
    site_browser_icon(out, site); fputs("<link rel=\"stylesheet\" href=\"/assets/own-site.css?v=6\"><link rel=\"mcp\" href=\"/.well-known/mcp/server-card.json\">"
          "<script src=\"/assets/own-site-agent.js\" defer></script>", out);
    emit_tokens(out, db, site);
    fputs("</head><body class=\"site-program-body\"><main class=\"site-composition\" data-site=\"", out); htext(out, site->id); fputs("\"",out); bf_site_presentation_attributes(out,db,site->profile);
    fputs(">", out); site_bar(out, site->title, route);
    fputs("<header class=\"site-program-masthead\"><p><a href=\"/\">", out); htext(out, site->title);
    fputs("</a></p><h1>", out); htext(out, title); fputs("</h1></header>", out);
    pages_nav(out, db, site->id, route);
}

/* One field of a form body or a query string, decoded.  Returns 1 when present. */
static int form_field(const char *encoded, const char *name, char *out, size_t cap) {
    size_t name_len = strlen(name), n = 0;
    for (const char *p = encoded; p && *p; ) {
        const char *amp = strchr(p, '&'), *end = amp ? amp : p + strlen(p);
        if ((size_t)(end - p) > name_len && !strncmp(p, name, name_len) && p[name_len] == '=') {
            for (const char *c = p + name_len + 1; c < end && n + 4 < cap; c++) {
                if (*c == '+') out[n++] = ' ';
                else if (*c == '%' && c + 2 < end + 1 && isxdigit((unsigned char)c[1]) && isxdigit((unsigned char)c[2])) {
                    char hex[3] = {c[1], c[2], 0};
                    char v = (char)strtol(hex, NULL, 16);
                    if (v == '\r') { c += 2; continue; }
                    out[n++] = (unsigned char)v < 0x20 && v != '\n' ? ' ' : v;
                    c += 2;
                } else out[n++] = *c;
            }
            out[n] = '\0';
            return 1;
        }
        p = amp ? amp + 1 : NULL;
    }
    out[0] = '\0';
    return 0;
}

/* The intake questions of the offer a message is about, as the owner wrote
 * them with "site offer --ask FACT=Question".  Answers are optional: a person
 * may not know how many mailboxes they have, and the message still goes. */
#define OFFER_ASKS_SQL \
    "SELECT json_extract(a.value,'$.fact'), json_extract(a.value,'$.question') FROM site_fragment f, " \
    "json_each(f.projection_contract,'$.asks') a WHERE f.id=(SELECT max(id) FROM site_fragment WHERE namespace=?1 AND kind='offer' " \
    "AND status='published' AND heading=?2) AND json_valid(f.projection_contract) ORDER BY a.key"

static void intake_fields(FILE *out, sqlite3 *db, const McpSite *site, const char *about) {
    sqlite3_stmt *st = NULL;
    int n = 0;
    if (!about || !*about || sqlite3_prepare_v2(db, OFFER_ASKS_SQL, -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, about, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *fact = (const char *)sqlite3_column_text(st, 0), *question = (const char *)sqlite3_column_text(st, 1);
        if (!fact || !question) continue;
        if (!n++) fputs("<fieldset class=\"contact-intake\"><legend>A few facts, if you have them</legend>"
                        "<p>Leave any blank you are unsure of. We confirm each one with you before relying on it.</p>", out);
        fputs("<label>", out); htext(out, question); fputs("<input name=\"ask_", out); htext(out, fact);
        fputs("\" maxlength=\"300\" data-fact=\"", out); htext(out, fact); fputs("\"></label>", out);
    }
    if (n) fputs("</fieldset>", out);
    sqlite3_finalize(st);
}

/* The answers given to those questions, as ",\"answers\":{fact:answer...}"
 * for the message record; nothing when none was answered. */
static void intake_answers(FILE *e, sqlite3 *db, const McpSite *site, const char *about, const char *body) {
    sqlite3_stmt *st = NULL;
    int n = 0;
    if (!about || !*about || sqlite3_prepare_v2(db, OFFER_ASKS_SQL, -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, about, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *fact = (const char *)sqlite3_column_text(st, 0);
        char field[64], answer[320];
        if (!fact || strlen(fact) > 40) continue;
        snprintf(field, sizeof(field), "ask_%s", fact);
        if (!form_field(body, field, answer, sizeof(answer)) || !answer[0]) continue;
        if (strlen(answer) > 300) { size_t k = 300; while (k && ((unsigned char)answer[k] & 0xC0) == 0x80) k--; answer[k] = '\0'; }
        fputs(n++ ? "," : ",\"answers\":{", e); jtext(e, fact); fputc(':', e); jtext(e, answer);
    }
    if (n) fputc('}', e);
    sqlite3_finalize(st);
}

static void serve_contact_form(sqlite3 *db, const McpSite *site, const char *query, const char *problem, int status) {
    char *html = NULL, about[200];
    size_t len = 0;
    FILE *out = open_memstream(&html, &len);
    form_field(query, "about", about, sizeof(about));
    page_open(out, site, db, "Contact", "/contact");
    fputs("<section class=\"site-page\">", out);
    if (problem) { fputs("<p class=\"form-problem\" role=\"alert\">", out); htext(out, problem); fputs("</p>", out); }
    else fputs("<p class=\"lede\">Write to us here. Your message goes straight to the owner's inbox.</p>", out);
    {
        char member[64];
        fputs("<form class=\"contact-form\" method=\"post\" action=\"", out);
        if (member_host_for(site->host, member, sizeof(member))) { fputs("https://", out); htext(out, member); fputs("/site-contact/", out); htext(out, site->id); }
        else fputs("/contact", out);
        fputs("\">", out);
    }
    fputs("<label>Your name<input name=\"name\" required maxlength=\"120\" autocomplete=\"name\"></label>"
          "<label>How to reach you<input name=\"contact\" required minlength=\"3\" maxlength=\"200\" placeholder=\"Email or phone\" autocomplete=\"email\"></label>"
          "<label>Message<textarea name=\"message\" required minlength=\"5\" maxlength=\"4000\" rows=\"6\">", out);
    if (about[0]) { fputs("About: ", out); htext(out, about); fputs("\n\n", out); }
    fputs("</textarea></label>", out);
    intake_fields(out, db, site, about);
    fputs(/* A field no person sees or fills; a program that fills every field gives itself away. */
          "<label class=\"contact-extra\" aria-hidden=\"true\">Website<input name=\"website\" tabindex=\"-1\" autocomplete=\"off\"></label>"
          "<input type=\"hidden\" name=\"about\" value=\"", out);
    htext(out, about);
    fputs("\"><button type=\"submit\">Send message</button></form></section>", out);
    site_foot(out, db, site->id, site->title);
    fputs("</main></body></html>", out);
    fclose(out);
    reply(status, "text/html; charset=utf-8", html, len);
    free(html);
}

static void serve_contact_submit(sqlite3 *db, const McpSite *site, const char *cms_db, const char *body, const char *client) {
    char name[200], contact[300], message[4400], website[100], about[200], key_id[220], mark[9], reference[200], *extra = NULL, *html = NULL;
    size_t a = 0, len = 0;
    FILE *e, *out;
    form_field(body, "name", name, sizeof(name));
    form_field(body, "contact", contact, sizeof(contact));
    form_field(body, "message", message, sizeof(message));
    form_field(body, "website", website, sizeof(website));
    form_field(body, "about", about, sizeof(about));
    address_mark(site, client, mark);
    snprintf(key_id, sizeof(key_id), "%s:form:%s", site->id, mark);
    if (website[0]) {
        /* Told it was sent; nothing is kept.  A program learns nothing from the answer. */
        out = open_memstream(&html, &len);
        page_open(out, site, db, "Message sent", "/contact");
        fputs("<section class=\"site-page\"><p class=\"lede\">Thank you. Your message is with the owner.</p></section></main></body></html>", out);
        fclose(out);
        reply(200, "text/html; charset=utf-8", html, len);
        free(html);
        return;
    }
    if (calls_since(key_id, 3600) >= env_number("BONFYRE_SITE_FORM_PER_HOUR", 5)) {
        serve_contact_form(db, site, "", "That is several messages in an hour from one place. Please try again later.", 429);
        return;
    }
    if (!message_ok(name, contact, message)) {
        serve_contact_form(db, site, "", "Please give your name, a way to reach you, and a message of at least a few words.", 400);
        return;
    }
    e = open_memstream(&extra, &a);
    if (about[0]) { fputs(",\"about\":", e); jtext(e, about); }
    intake_answers(e, db, site, about, body);
    fclose(e);
    if (create_lead(site, cms_db, "contact", name, contact, message, extra, "left through the site's contact page", reference, sizeof(reference))) {
        free(extra);
        serve_contact_form(db, site, "", "The message could not be taken just now. Please try again.", 400);
        return;
    }
    free(extra);
    meter_line(site, key_id, "form:contact", 1);
    out = open_memstream(&html, &len);
    page_open(out, site, db, "Message sent", "/contact");
    fputs("<section class=\"site-page\"><p class=\"lede\">Thank you, ", out); htext(out, name);
    fputs(". Your message is with the owner.</p><p>Reference <code>", out); htext(out, reference);
    fputs("</code>. This confirms it arrived; it is not yet a reply.</p><p><a href=\"/\">Back to the start</a></p></section></main></body></html>", out);
    fclose(out);
    reply(200, "text/html; charset=utf-8", html, len);
    free(html);
}

/* Hosts the estate itself answers for; an operator's own site is never one. */
static int estate_host(const char *host_header) {
    static const char *roots[] = {"aurekai.life", "aurekai.live", "aurekai.shop", "aurekai.work", NULL};
    char host[256], *colon;
    size_t n;
    snprintf(host, sizeof(host), "%s", host_header ? host_header : "");
    colon = strrchr(host, ':');
    if (colon && !strchr(colon, ']')) *colon = '\0';
    n = strlen(host);
    for (int i = 0; roots[i]; i++) {
        size_t r = strlen(roots[i]);
        if (n >= r && !strcmp(host + n - r, roots[i]) && (n == r || host[n - r - 1] == '.')) return 1;
    }
    return 0;
}

/* A name under one of the estate's territories is the estate's own surface
 * unless an independent site was given that exact address.  The apexes are
 * never anyone else's. */
static int estate_shared_host(const char *cms_db, const char *host_header) {
    static const char *roots[] = {"aurekai.life", "aurekai.live", "aurekai.shop", "aurekai.work", NULL};
    char host[256], *colon;
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    int own = 0;
    if (!estate_host(host_header)) return 0;
    snprintf(host, sizeof(host), "%s", host_header ? host_header : "");
    colon = strrchr(host, ':');
    if (colon && !strchr(colon, ']')) *colon = '\0';
    for (int i = 0; roots[i]; i++) if (!strcmp(host, roots[i])) return 1;
    if (sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return 1; }
    sqlite3_busy_timeout(db, 2000);
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM site_presentation s JOIN site_program p ON p.site_id=s.site_id AND p.status='published' "
                               "WHERE s.host=?1 AND s.status='published' AND s.territory='independent' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, host, -1, SQLITE_STATIC);
        own = sqlite3_step(st) == SQLITE_ROW;
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return !own;
}

int bf_site_mcp_serve(const char *method, const char *target, const char *query, const char *body, const char *host, const char *origin, const char *bearer, const char *client) {
    const char *cms_db = getenv("BONFYRE_CMS_DB");
    sqlite3 *db = NULL;
    McpSite site;
    char by_path[64] = "", endpoint[400];
    int is_mcp, is_llms, is_card, is_a2a, is_contact, is_server_card, maybe_page, local = loopback_host(host), shared;
    if (!target || !method || !cms_db || !*cms_db) return 0;
    shared = estate_shared_host(cms_db, host);
    is_mcp = !strcmp(target, "/mcp") || !strcmp(target, "/mcp/");
    if (!is_mcp && local && !strncmp(target, "/mcp/", 5) && strlen(target + 5) < sizeof(by_path)) {
        size_t n = 0;
        for (const char *p = target + 5; *p && *p != '/'; p++) {
            if (!islower((unsigned char)*p) && !isdigit((unsigned char)*p) && *p != '-') { n = 0; break; }
            by_path[n++] = *p;
        }
        by_path[n] = '\0';
        is_mcp = n > 0 && strcmp(by_path, "resources");
        if (!is_mcp) by_path[0] = '\0';
    }
    is_llms = !strcmp(target, "/llms.txt");
    is_card = !strcmp(target, "/.well-known/agent.json");
    is_server_card = !strcmp(target, "/.well-known/mcp/server-card.json") && !strcmp(method, "GET");
    is_a2a = !strcmp(target, "/a2a") || !strncmp(target, "/a2a/", 5);
    /* A message for a site the edge hands out, arriving at the member host. */
    if (shared && !strcmp(method, "POST") && !strncmp(target, "/site-contact/", 14) && strlen(target + 14) < sizeof(by_path)) {
        size_t n = 0;
        for (const char *p = target + 14; *p; p++) {
            if (!islower((unsigned char)*p) && !isdigit((unsigned char)*p) && *p != '-') { n = 0; break; }
            by_path[n++] = *p;
        }
        by_path[n] = '\0';
        if (n && sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
            sqlite3_busy_timeout(db, 2000);
            if (load_site(db, host, by_path, &site) && !site.reading) {
                snprintf(relay_base, sizeof(relay_base), "%s", site.host);
                serve_contact_submit(db, &site, cms_db, body, client);
                relay_base[0] = '\0';
                sqlite3_close(db);
                return 1;
            }
        }
        if (db) sqlite3_close(db);
        db = NULL; by_path[0] = '\0';
        return 0;
    }
    /* A site's own search: its published sections as a static index. */
    if (!strcmp(method, "GET") && !local && !shared &&
        (!strcmp(target, "/_search/passages.json") || !strcmp(target, "/_search/index.bfsi") || !strcmp(target, "/_search/model.bfse"))) {
        if (sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return 0; }
        sqlite3_busy_timeout(db, 2000);
        if (!load_site(db, host, NULL, &site)) { sqlite3_close(db); return 0; }
        serve_site_search(db, &site, target, cms_db);
        sqlite3_close(db);
        return 1;
    }
    /* A standing page lives at a route of the operator's choosing. */
    is_contact = !strcmp(target, "/contact") && !local && !shared && (!strcmp(method, "GET") || !strcmp(method, "POST"));
    maybe_page = !is_mcp && !is_llms && !is_card && !is_server_card && !is_a2a && !is_contact && !strcmp(method, "GET") && !local && !shared &&
                 target[0] == '/' && target[1] && strlen(target) < 120 && strncmp(target, "/assets/", 8) && strncmp(target, "/_", 2) &&
                 !strchr(target, '.');
    if (!is_mcp && !is_llms && !is_card && !is_server_card && !is_a2a && !is_contact && !maybe_page) return 0;
    if (sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return 0; }
    sqlite3_busy_timeout(db, 2000);
    if (!load_site(db, host, by_path[0] ? by_path : NULL, &site)) { sqlite3_close(db); return 0; }
    if (is_mcp && !site_origin_allowed(origin, host, &site)) {
        static const char denied[] = "{\"error\":\"this origin may not call the site's assistant endpoint\"}\n";
        reply(403, "application/json", denied, sizeof(denied) - 1);
        sqlite3_close(db);
        return 1;
    }
    {
        /* The address to hand out is the one the asker used, when it works
         * for programs; a .localhost name is given in its path form. */
        char named[256], *colon;
        const char *port = host ? strrchr(host, ':') : NULL;
        size_t n;
        snprintf(named, sizeof(named), "%s", host ? host : site.host);
        colon = strrchr(named, ':');
        if (colon && !strchr(colon, ']')) *colon = '\0';
        n = strlen(named);
        if (by_path[0] || (n > 10 && !strcmp(named + n - 10, ".localhost")))
            snprintf(endpoint, sizeof(endpoint), "http://127.0.0.1%s/mcp/%s", port && !strchr(port, ']') ? port : "", site.id);
        else snprintf(endpoint, sizeof(endpoint), "https://%s/mcp", site.host);
    }

    if (is_contact) {
        if (!strcmp(method, "POST")) serve_contact_submit(db, &site, cms_db, body, client);
        else serve_contact_form(db, &site, query, NULL, 200);
        sqlite3_close(db);
        return 1;
    }
    if (site.reading && !is_mcp && !is_llms && !is_server_card) { sqlite3_close(db); return 0; }
    if (is_server_card) { serve_server_card(&site, endpoint); sqlite3_close(db); return 1; }
    if (maybe_page) {
        /* On an operator's own site an address is one of its pages or it is
         * nothing: the estate's shared surfaces are not behind its name. */
        if (!serve_page(db, &site, target)) {
            char *html = NULL;
            size_t len = 0;
            FILE *out = open_memstream(&html, &len);
            fputs("<!doctype html><html lang=\"en\" class=\"site-program\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" "
                  "content=\"width=device-width,initial-scale=1\"><title>Not found · ", out);
            htext(out, site.title);
            fputs("</title>", out); site_browser_icon(out, &site); fputs("<link rel=\"stylesheet\" href=\"/assets/own-site.css?v=6\">", out);
            emit_tokens(out, db, &site);
            fputs("</head><body class=\"site-program-body\"><main class=\"site-composition\">", out);
            site_bar(out, site.title, NULL);
            fputs("<header class=\"site-program-masthead\"><p><a href=\"/\">", out);
            htext(out, site.title);
            fputs("</a></p><h1>Nothing at this address</h1></header>", out);
            pages_nav(out, db, site.id, NULL);
            fputs("<section class=\"site-page\"><p class=\"lede\">This site has no page here. <a href=\"/\">Back to the start.</a></p></section></main></body></html>", out);
            fclose(out);
            reply(404, "text/html; charset=utf-8", html, len);
            free(html);
        }
        sqlite3_close(db);
        return 1;
    }
    if (is_a2a) {
        /* The estate's own agent skills are not this site's to offer. */
        static const char gone[] = "{\"error\":\"this site does not take agent-to-agent tasks; assistants read it at /mcp\"}\n";
        reply(404, "application/json", gone, sizeof(gone) - 1);
        sqlite3_close(db);
        return 1;
    }
    if (is_card && !strcmp(method, "GET")) serve_card(&site);
    else if (is_llms && !strcmp(method, "GET")) serve_llms(db, &site);
    else if (is_mcp && !strcmp(method, "GET")) serve_human(&site, endpoint);
    else if (is_mcp && !strcmp(method, "POST")) {
        McpCaller caller;
        resolve_caller(db, &site, bearer, client, &caller);
        if (caller.refused) {
            static const char denied[] = "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32001,\"message\":"
                                         "\"that key is not one this site issued, or it has been withdrawn\"}}\n";
            reply(401, "application/json", denied, sizeof(denied) - 1);
        } else serve_rpc(db, &site, cms_db, body, &caller);
    } else reply(405, "application/json", "{\"error\":\"method not allowed\"}\n", 31);
    sqlite3_close(db);
    return 1;
}
