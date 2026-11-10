#define _POSIX_C_SOURCE 200809L
#include "atom_feed.h"

#include "bonfyre.h"

#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum { FEED_BODY_CAP = 262144 };

typedef struct {
    char *bytes;
    size_t used;
    size_t capacity;
    int failed;
} FeedWriter;

static int feed_stamp_valid(const char *stamp);

static void feed_text(FeedWriter *w, const char *text, size_t n) {
    if (w->failed || n >= w->capacity - w->used) { w->failed = 1; return; }
    memcpy(w->bytes + w->used, text, n);
    w->used += n;
    w->bytes[w->used] = '\0';
}

static void feed_literal(FeedWriter *w, const char *text) {
    feed_text(w, text, strlen(text));
}

static void feed_format(FeedWriter *w, const char *fmt, ...) {
    va_list ap;
    int n;
    if (w->failed) return;
    va_start(ap, fmt);
    n = vsnprintf(w->bytes + w->used, w->capacity - w->used, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= w->capacity - w->used) { w->failed = 1; return; }
    w->used += (size_t)n;
}

/* XML 1.0 text is not HTML. Skip forbidden control bytes and escape text at
 * the one emission boundary, so a retained title cannot open a second entry. */
static void feed_xml_text(FeedWriter *w, const char *text, size_t max_input) {
    if (!text) return;
    for (size_t i = 0; i < max_input && text[i] && !w->failed; i++) {
        const char *replacement = NULL;
        unsigned char c = (unsigned char)text[i];
        if (c >= 0x80) {
            size_t width = c >= 0xf0 && c <= 0xf4 ? 4 :
                           c >= 0xe0 && c <= 0xef ? 3 :
                           c >= 0xc2 && c <= 0xdf ? 2 : 0;
            int valid = width && i + width <= max_input;
            unsigned char b1 = valid ? (unsigned char)text[i + 1] : 0;
            if (valid) {
                for (size_t j = 1; j < width; j++)
                    if (!text[i + j] || ((unsigned char)text[i + j] & 0xc0) != 0x80) {
                        valid = 0;
                        break;
                    }
            }
            if (valid && width == 3 &&
                ((c == 0xe0 && b1 < 0xa0) || (c == 0xed && b1 >= 0xa0))) valid = 0;
            if (valid && width == 4 &&
                ((c == 0xf0 && b1 < 0x90) || (c == 0xf4 && b1 >= 0x90))) valid = 0;
            if (valid) { feed_text(w, text + i, width); i += width - 1; }
            continue;
        }
        if (c == '&') replacement = "&amp;";
        else if (c == '<') replacement = "&lt;";
        else if (c == '>') replacement = "&gt;";
        else if (c == '\"') replacement = "&quot;";
        else if (c == '\'') replacement = "&#39;";
        else if (c < 32 && c != 9 && c != 10 && c != 13) continue;
        if (replacement) feed_literal(w, replacement);
        else feed_text(w, (const char *)&text[i], 1);
    }
}

static int feed_source_key(const char *source, const char *key) {
    size_t n;
    if (!source || !key || !(n = strlen(source)) || n >= 96 || strlen(key) != 24) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = source[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
              c == '-' || c == '_' || c == '.')) return 0;
    }
    for (size_t i = 0; i < 24; i++) {
        char c = key[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return 0;
    }
    return 1;
}

static int feed_compiled_stamp(sqlite3 *fabric, const char *subject, char stamp[32]) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!fabric || !subject || !*subject) return 0;
    if (sqlite3_prepare_v2(fabric,
        "SELECT b.compiled_at FROM subject_projection_build b WHERE b.build_id=1 "
        "AND EXISTS(SELECT 1 FROM subject_route r WHERE r.subject=?1 AND r.scope='public')",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *at = (const char *)sqlite3_column_text(st, 0);
            if (feed_stamp_valid(at)) memcpy(stamp, at, 21);
            found = 1;
        }
    }
    if (st) sqlite3_finalize(st);
    return found;
}

static void feed_compiled_entries(FeedWriter *w, sqlite3 *fabric,
                                  const char *subject, const char *origin,
                                  const char *stamp) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(fabric,
        /* Driven from the route items: the route-item key puts route before
         * source, so probing it per member row rescanned every route item of
         * the subject and an apex feed took over a minute, pinning a worker. */
        "SELECT m.source,m.item_key,m.title,m.body,m.at FROM "
        "(SELECT DISTINCT source,item_key,material FROM subject_route_item "
        "WHERE subject=?1 AND scope='public' AND material<>'place') r "
        "JOIN subject_material m ON m.subject=?1 AND m.scope='public' "
        "AND m.source=r.source AND m.item_key=r.item_key AND m.material=r.material "
        "ORDER BY m.at DESC,m.source,m.item_key LIMIT 50",
        -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW && !w->failed) {
        const char *source = (const char *)sqlite3_column_text(st, 0);
        const char *key = (const char *)sqlite3_column_text(st, 1);
        const char *title = (const char *)sqlite3_column_text(st, 2);
        const char *summary = (const char *)sqlite3_column_text(st, 3);
        time_t at = (time_t)sqlite3_column_int64(st, 4);
        char updated[32]; struct tm at_tm;
        size_t prior = w->used;
        if (!feed_source_key(source, key)) continue;
        memcpy(updated, stamp, strlen(stamp) + 1);
        if (at > 0 && gmtime_r(&at, &at_tm))
            strftime(updated, sizeof(updated), "%Y-%m-%dT%H:%M:%SZ", &at_tm);
        feed_literal(w, "<entry><title>");
        feed_xml_text(w, title, 1024);
        feed_format(w, "</title><id>%s/o/%s/%s</id><link href=\"%s/o/%s/%s\"/>"
                    "<updated>%s</updated><summary>",
                    origin, source, key, origin, source, key, updated);
        feed_xml_text(w, summary, 2048);
        feed_literal(w, "</summary></entry>\n");
        if (w->failed) { w->used = prior; w->bytes[prior] = '\0'; w->failed = 0; break; }
    }
    sqlite3_finalize(st);
}

static void feed_note_entries(FeedWriter *w, sqlite3 *cms,
                              const char *origin, const char *stamp) {
    sqlite3_stmt *st = NULL;
    if (!cms || sqlite3_prepare_v2(cms,
        "SELECT title,route,published_at,meta_description FROM wiki_page "
        "WHERE status='published' AND allow_guest=1 AND route LIKE 'notes/%' "
        "ORDER BY id DESC LIMIT 50", -1, &st, NULL) != SQLITE_OK) return;
    while (sqlite3_step(st) == SQLITE_ROW && !w->failed) {
        const char *title = (const char *)sqlite3_column_text(st, 0);
        const char *route = (const char *)sqlite3_column_text(st, 1);
        const char *at = (const char *)sqlite3_column_text(st, 2);
        const char *summary = (const char *)sqlite3_column_text(st, 3);
        size_t prior = w->used;
        if (!route || strncmp(route, "notes/", 6) ||
            strspn(route, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/_-.") != strlen(route)) continue;
        feed_literal(w, "<entry><title>");
        feed_xml_text(w, title ? title : route, 1024);
        feed_format(w, "</title><id>%s/%s</id><link href=\"%s/%s\"/><updated>%s</updated><summary>",
                    origin, route, origin, route, feed_stamp_valid(at) ? at : stamp);
        feed_xml_text(w, summary, 2048);
        feed_literal(w, "</summary></entry>\n");
        if (w->failed) { w->used = prior; w->bytes[prior] = '\0'; w->failed = 0; break; }
    }
    sqlite3_finalize(st);
}

static int feed_safe_hub(const char *hub) {
    if (!hub || strncmp(hub, "https://", 8) || !hub[8]) return 0;
    for (const char *p = hub; *p; p++)
        if ((unsigned char)*p <= 32 || (unsigned char)*p >= 127 ||
            *p == '>' || *p == '<' || *p == '\"' || *p == '\\') return 0;
    return strlen(hub) < 1024;
}

static int feed_safe_origin(const char *origin) {
    const char *start;
    if (!origin) return 0;
    if (!strncmp(origin, "https://", 8)) start = origin + 8;
    else if (!strncmp(origin, "http://", 7)) start = origin + 7;
    else return 0;
    if (!*start || strlen(origin) >= 512) return 0;
    for (const char *p = start; *p; p++)
        if ((unsigned char)*p <= 32 || (unsigned char)*p >= 127 ||
            *p == '>' || *p == '<' || *p == '\"' || *p == '\\') return 0;
    return 1;
}

static int feed_stamp_valid(const char *stamp) {
    if (!stamp || strlen(stamp) != 20) return 0;
    for (size_t i = 0; i < 20; i++) {
        if (i == 4 || i == 7) { if (stamp[i] != '-') return 0; }
        else if (i == 10) { if (stamp[i] != 'T') return 0; }
        else if (i == 13 || i == 16) { if (stamp[i] != ':') return 0; }
        else if (i == 19) { if (stamp[i] != 'Z') return 0; }
        else if (stamp[i] < '0' || stamp[i] > '9') return 0;
    }
    return 1;
}

static int feed_etag_matches(const char *request, const char *tag) {
    const char *p = request;
    if (!p || !*p) return 0;
    while (*p) {
        const char *end = strchr(p, ',');
        size_t n = end ? (size_t)(end - p) : strlen(p);
        while (n && (*p == ' ' || *p == '\t')) { p++; n--; }
        while (n && (p[n - 1] == ' ' || p[n - 1] == '\t')) n--;
        if ((n == strlen(tag) && !memcmp(p, tag, n)) ||
            (n == strlen(tag + 2) && !memcmp(p, tag + 2, n)) ||
            (n == 1 && *p == '*')) return 1;
        if (!end) break;
        p = end + 1;
    }
    return 0;
}

int bf_atom_feed_serve(FILE *out, sqlite3 *cms, sqlite3 *fabric,
                       const char *subject, const char *normalized_host,
                       const char *default_origin, const char *if_none_match,
                       int head_only, const char *websub_hub) {
    FeedWriter w = {0};
    char stamp[32], host_origin[320], tag[64], digest[65], headers[2048];
    const char *origin = default_origin;
    int compiled, n;
    time_t now = time(NULL);
    struct tm utc;
    if (!out || !feed_safe_origin(default_origin)) return 0;
    w.bytes = malloc(FEED_BODY_CAP);
    if (!w.bytes) return 0;
    w.capacity = FEED_BODY_CAP;
    w.bytes[0] = '\0';
    if (!gmtime_r(&now, &utc) ||
        !strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%SZ", &utc)) goto fail;
    compiled = feed_compiled_stamp(fabric, subject, stamp);
    if (compiled && normalized_host && *normalized_host) {
        n = snprintf(host_origin, sizeof(host_origin), "https://%s", normalized_host);
        if (n > 0 && (size_t)n < sizeof(host_origin)) origin = host_origin;
    }
    feed_format(&w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                "<feed xmlns=\"http://www.w3.org/2005/Atom\">\n"
                "<title>Aurekai</title>\n<id>%s/</id>\n"
                "<link rel=\"self\" href=\"%s/feed.xml\"/>\n<link href=\"%s/\"/>\n"
                "<updated>%s</updated>\n"
                "<subtitle>Receipted public-record observation, work and capability.</subtitle>\n",
                origin, origin, origin, stamp);
    if (compiled) feed_compiled_entries(&w, fabric, subject, origin, stamp);
    else feed_note_entries(&w, cms, origin, stamp);
    feed_literal(&w, "</feed>\n");
    if (w.failed) goto fail;
    bf_sha256_hex((const uint8_t *)w.bytes, w.used, digest);
    snprintf(tag, sizeof(tag), "W/\"%.16s\"", digest);
    if (feed_etag_matches(if_none_match, tag)) {
        n = snprintf(headers, sizeof(headers),
            "HTTP/1.1 304 Not Modified\r\nETag: %s\r\n"
            "Cache-Control: public, max-age=60\r\nConnection: close\r\n\r\n", tag);
    } else {
        const char *hub = feed_safe_hub(websub_hub) ? websub_hub : NULL;
        n = snprintf(headers, sizeof(headers),
            "HTTP/1.1 200 OK\r\nContent-Type: application/atom+xml; charset=utf-8\r\n"
            "Content-Length: %zu\r\nETag: %s\r\n"
            "Cache-Control: public, max-age=60\r\nVary: Accept, Accept-Encoding\r\n"
            "Link: <%s/feed.xml>; rel=\"self\"%s%s%s\r\n"
            "Connection: close\r\n\r\n", w.used, tag, origin,
            hub ? ", <" : "", hub ? hub : "", hub ? ">; rel=\"hub\"" : "");
        if (n < 0 || (size_t)n >= sizeof(headers) ||
            fwrite(headers, 1, (size_t)n, out) != (size_t)n ||
            (!head_only && fwrite(w.bytes, 1, w.used, out) != w.used)) goto fail;
        fflush(out);
        free(w.bytes);
        return 1;
    }
    if (n < 0 || (size_t)n >= sizeof(headers) ||
        fwrite(headers, 1, (size_t)n, out) != (size_t)n) goto fail;
    fflush(out);
    free(w.bytes);
    return 1;
fail:
    free(w.bytes);
    return 0;
}
