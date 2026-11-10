/* site_search.h — a site's own search, built from its published CMS sections.
 *
 * Every published section of every published page is a passage (long
 * sections are cut at about 160 words). The passages are embedded with the
 * static model (libbonfyre bf_static_embed) into a .bfsi index, cached on disk
 * by a digest of the passages and the model, so an edit in the CMS is in the
 * next request's index and nothing is backfilled by hand. Three routes serve
 * it, and the static export copies them like any page:
 *   /_search/passages.json   route, anchor, page title, heading, excerpt
 *   /_search/index.bfsi      one row per passage, in that order
 *   /_search/model.bfse      the model, for embedding a question in the page
 * The page's reader (own-site-reading.js -> static-embed.mjs) embeds the
 * question in the browser and ranks the rows; nothing is sent anywhere.
 * Included by site_mcp.c after page_sections. */
#include "bf_static_embed.h"
#include <sys/stat.h>

#define SEARCH_WORDS 160

static char *bf_read_whole(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long size = ftell(f);
    if (size < 0 || fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)size + 1);
    if (!buf || fread(buf, 1, (size_t)size, f) != (size_t)size) { free(buf); fclose(f); return NULL; }
    fclose(f); buf[size] = 0; *len = (size_t)size;
    return buf;
}

typedef struct { char *route, *anchor, *page, *heading, *text; } SearchPassage;
typedef struct { SearchPassage *items; int count, cap; } SearchSet;

static const char *search_model_path(void) {
    static char path[1024];
    const char *given = getenv("BONFYRE_STATIC_MODEL"), *home = getenv("HOME");
    if (given && *given) return given;
    snprintf(path, sizeof path, "%s/BonfyreModels/static/nomic-static-128.bfse", home ? home : "");
    return path;
}

static char *search_dup(const char *s, size_t n) { char *c = malloc(n + 1); if (c) { memcpy(c, s, n); c[n] = 0; } return c; }

/* Markdown-ish rich text to plain words: drop markup characters, keep code. */
static char *search_plain(const char *text) {
    size_t n = strlen(text);
    char *out = malloc(n + 1), *w = out;
    if (!out) return NULL;
    for (const char *p = text; *p; p++) {
        /* A code fence's label line ("```sh Terminal") is markup, not text. */
        if (p[0] == '`' && p[1] == '`' && p[2] == '`') { while (*p && *p != '\n') p++; *w++ = ' '; if (!*p) break; continue; }
        if (strchr("*_`#|>", *p)) { *w++ = ' '; continue; }
        if (*p == '[') continue;
        if (*p == ']' && p[1] == '(') { const char *close = strchr(p, ')'); if (close) { p = close; *w++ = ' '; continue; } }
        *w++ = (*p == '\n' || *p == '\t' || *p == '\r') ? ' ' : *p;
    }
    *w = 0;
    return out;
}

static void search_add(SearchSet *set, const char *route, const char *anchor, const char *page, const char *heading, const char *text, size_t len) {
    if (set->count == set->cap) { set->cap = set->cap ? set->cap * 2 : 64; set->items = realloc(set->items, sizeof(SearchPassage) * (size_t)set->cap); }
    SearchPassage *p = &set->items[set->count++];
    p->route = strdup(route); p->anchor = strdup(anchor); p->page = strdup(page); p->heading = strdup(heading); p->text = search_dup(text, len);
}

static void search_free(SearchSet *set) {
    for (int i = 0; i < set->count; i++) { free(set->items[i].route); free(set->items[i].anchor); free(set->items[i].page); free(set->items[i].heading); free(set->items[i].text); }
    free(set->items);
}

static void search_collect(sqlite3 *db, const McpSite *site, SearchSet *set) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
            "SELECT p.route, p.title, COALESCE(p.lede,''), COALESCE(f.heading,''), COALESCE(f.body,''), COALESCE(f.kind,'') "
            "FROM site_page p LEFT JOIN site_fragment f ON f.namespace=p.namespace AND f.page=p.route AND f.status='published' "
            "WHERE p.namespace=?1 AND p.status='published' ORDER BY p.id, f.position, f.id", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, site->id, -1, SQLITE_STATIC);
    char last_route[512] = "";
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *route = (const char *)sqlite3_column_text(st, 0), *title = (const char *)sqlite3_column_text(st, 1);
        const char *lede = (const char *)sqlite3_column_text(st, 2), *heading = (const char *)sqlite3_column_text(st, 3);
        const char *body = (const char *)sqlite3_column_text(st, 4), *kind = (const char *)sqlite3_column_text(st, 5);
        if (!route || !title) continue;
        if (strcmp(route, last_route)) {
            snprintf(last_route, sizeof last_route, "%s", route);
            if (lede && *lede) { char *plain = search_plain(lede); if (plain) { search_add(set, route, "", title, title, plain, strlen(plain)); free(plain); } }
        }
        if (!body || !*body || (kind && !strcmp(kind, "offer"))) continue;
        char *anchor = NULL; size_t anchor_len = 0;
        FILE *slug = open_memstream(&anchor, &anchor_len);
        if (!slug) continue;
        if (heading && *heading) slug_text(slug, heading);
        fclose(slug);
        char *plain = search_plain(body);
        if (!plain) { free(anchor); continue; }
        /* Cut at about SEARCH_WORDS words so one passage stays one answer. */
        const char *start = plain;
        while (*start) {
            while (*start == ' ') start++;
            if (!*start) break;
            const char *end = start; int words = 0;
            while (*end && words < SEARCH_WORDS) { while (*end && *end != ' ') end++; words++; while (*end == ' ') end++; }
            search_add(set, route, anchor ? anchor : "", title, heading ? heading : "", start, (size_t)(end - start));
            start = end;
        }
        free(plain); free(anchor);
    }
    sqlite3_finalize(st);
}

static void search_json_text(FILE *out, const char *s, size_t limit) {
    fputc('"', out);
    size_t n = 0;
    for (const unsigned char *p = (const unsigned char *)s; *p && n < limit; p++, n++) {
        if (*p == '"' || *p == '\\') { fputc('\\', out); fputc(*p, out); }
        else if (*p < 0x20) fputc(' ', out);
        else fputc(*p, out);
    }
    fputc('"', out);
}

static void search_reply_bytes(int code, const char *type, const char *body, size_t len, const char *etag) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nCache-Control: public, max-age=300\r\n%s%s%s"
           "X-Bonfyre-Site-Search: static\r\nConnection: close\r\n\r\n", code, code == 200 ? "OK" : "Not Found", type, len,
           etag ? "ETag: \"" : "", etag ? etag : "", etag ? "\"\r\n" : "");
    fwrite(body, 1, len, stdout);
    fflush(stdout);
}

/* Builds (or reuses) the site's index and answers one of the three routes. */
static int serve_site_search(sqlite3 *db, const McpSite *site, const char *target, const char *cms_db) {
    const char *model_path = search_model_path();
    if (!strcmp(target, "/_search/model.bfse")) {
        size_t len = 0;
        char *bytes = bf_read_whole(model_path, &len);
        if (!bytes) { static const char none[] = "{\"error\":\"this site's search model is not installed\"}\n"; reply(404, "application/json", none, sizeof none - 1); return 1; }
        search_reply_bytes(200, "application/octet-stream", bytes, len, NULL);
        free(bytes);
        return 1;
    }
    char err[256];
    BfStaticModel *model = bf_static_load(model_path, err, sizeof err);
    if (!model) { char body[400]; int n = snprintf(body, sizeof body, "{\"error\":\"search model unavailable: %s\"}\n", err); reply(404, "application/json", body, (size_t)n); return 1; }
    SearchSet set = {0};
    search_collect(db, site, &set);
    /* Identity: model bytes and every passage, in order. */
    char hex[65];
    {
        char *all = NULL; size_t all_len = 0;
        FILE *id = open_memstream(&all, &all_len);
        if (!id) { search_free(&set); bf_static_free(model); return 0; }
        fwrite(bf_static_model_sha256(model), 1, 32, id);
        for (int i = 0; i < set.count; i++) fprintf(id, "%s%c%s%c%s%c%s%c", set.items[i].route, 0, set.items[i].anchor, 0, set.items[i].heading, 0, set.items[i].text, 0);
        fclose(id);
        bf_sha256_hex((const unsigned char *)all, all_len, hex);
        free(all);
    }
    char dir[1024], index_path[1100], passages_path[1100];
    const char *cache = getenv("BONFYRE_SEARCH_CACHE");
    if (cache && *cache) snprintf(dir, sizeof dir, "%s", cache);
    else { snprintf(dir, sizeof dir, "%s", cms_db); char *slash = strrchr(dir, '/'); if (slash) *slash = 0; else snprintf(dir, sizeof dir, "."); strncat(dir, "/search-cache", sizeof dir - strlen(dir) - 1); }
    mkdir(dir, 0700);
    snprintf(index_path, sizeof index_path, "%s/%.24s.bfsi", dir, hex);
    snprintf(passages_path, sizeof passages_path, "%s/%.24s.json", dir, hex);
    struct stat st_index, st_passages;
    if (stat(index_path, &st_index) != 0 || stat(passages_path, &st_passages) != 0) {
        int dims = bf_static_dims(model), kept = 0;
        float *vectors = malloc(sizeof(float) * (size_t)dims * (size_t)(set.count ? set.count : 1));
        char tmp[1200]; snprintf(tmp, sizeof tmp, "%s.%d.tmp", passages_path, (int)getpid());
        FILE *out = fopen(tmp, "w");
        if (out && vectors) {
            fprintf(out, "{\"schema\":\"bonfyre.site-search.v1\",\"site\":"); search_json_text(out, site->id, 200);
            fprintf(out, ",\"index_sha256\":\"%s\",\"passages\":[", hex);
            for (int i = 0; i < set.count; i++) {
                char *joined = NULL; size_t joined_len = 0;
                FILE *j = open_memstream(&joined, &joined_len);
                fprintf(j, "%s. %s. %s", set.items[i].page, set.items[i].heading, set.items[i].text);
                fclose(j);
                int used = bf_static_embed(model, joined, vectors + (size_t)kept * dims);
                free(joined);
                if (!used) continue;   /* a passage the model cannot see answers nothing */
                fprintf(out, "%s{\"route\":", kept ? "," : ""); search_json_text(out, set.items[i].route, 400);
                fputs(",\"anchor\":", out); search_json_text(out, set.items[i].anchor, 200);
                fputs(",\"page\":", out); search_json_text(out, set.items[i].page, 300);
                fputs(",\"heading\":", out); search_json_text(out, set.items[i].heading, 300);
                fputs(",\"excerpt\":", out); search_json_text(out, set.items[i].text, 600);
                fputc('}', out);
                kept++;
            }
            fputs("]}\n", out);
            if (fclose(out) == 0 && bf_static_index_write(index_path, model, kept, vectors) == 0) rename(tmp, passages_path);
            else remove(tmp);
        } else if (out) fclose(out);
        free(vectors);
    }
    search_free(&set);
    bf_static_free(model);
    const char *path = !strcmp(target, "/_search/index.bfsi") ? index_path : passages_path;
    size_t len = 0;
    char *bytes = bf_read_whole(path, &len);
    if (!bytes) { static const char none[] = "{\"error\":\"the search index could not be built\"}\n"; reply(404, "application/json", none, sizeof none - 1); return 1; }
    search_reply_bytes(200, !strcmp(target, "/_search/index.bfsi") ? "application/octet-stream" : "application/json", bytes, len, hex);
    free(bytes);
    return 1;
}
