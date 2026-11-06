/* item_depth.h -- what an item page can say from what the estate already holds.
 *
 * Three readings, each from an existing owner, none copied into a new store:
 *
 *   what it says     the item's own entry in its source's retained feed body
 *                    (BONFYRE_FOREIGN_ROOT/bodies/<source>.body): the lead,
 *                    the section headings with their opening paragraph, and
 *                    every name the publisher linked
 *   who published    the source frontier (BONFYRE_SOURCE_FRONTIER_DB): the
 *                    publisher's root, its identity, every channel it has and
 *                    which of them this item came through
 *   names, resolved  each linked name whose host is a frontier root opens that
 *                    root's profile here; the rest open where the publisher
 *                    pointed
 *
 * The publisher's lead is what it offered for syndication; sections show a
 * heading and its first paragraph, cut, so the original stays theirs and is
 * one link away. A body that cannot be read, or an entry that cannot be found
 * in it, shows nothing rather than a guess. Included by main.c before
 * emit_item_page. */

#define DEPTH_PARAS 10
#define DEPTH_SECTIONS 8
#define DEPTH_NAMES 24
#define DEPTH_BODY_CAP (48u << 20)

typedef struct { char text[760]; int section; } DepthPara;
typedef struct { char name[120]; char href[600]; char host[160]; char group[160]; char ident[48]; char root_name[200]; int section, platform; } DepthName;

/* A link to a video, a post or a profile on a platform names the platform's
 * page, not an organization the piece is about. */
static int depth_platform(const char *group) {
    static const char *const platforms[] = {"youtube.com", "youtu.be", "twitter.com", "x.com", "facebook.com", "instagram.com",
        "linkedin.com", "tiktok.com", "vimeo.com", "bsky.app", "mastodon.social", "threads.net", "flickr.com", "doi.org",
        "bit.ly", "t.co", "google.com", "goo.gl", "apple.com", "spotify.com", "soundcloud.com", "medium.com", "github.com"};
    for (size_t i = 0; i < sizeof(platforms) / sizeof(platforms[0]); i++) if (!strcmp(group, platforms[i])) return 1;
    return 0;
}
#define DEPTH_FACTS 24
typedef struct {
    char fact_key[DEPTH_FACTS][80], fact_val[DEPTH_FACTS][320]; int facts;
    struct { char date[80], description[600]; } provenance[8]; int provenances;
    DepthPara para[DEPTH_PARAS]; int paras;
    char section[DEPTH_SECTIONS][200]; int sections;
    DepthName name[DEPTH_NAMES]; int names;
} ItemDepth;

/* Text of one HTML fragment: tags dropped, entities decoded, space folded. */
static void depth_text(const char *from, const char *to, char *out, size_t cap) {
    size_t n = (size_t)(to - from), w = 0;
    char *tmp = malloc(n + 1);
    if (!tmp || !cap) { free(tmp); if (cap) out[0] = '\0'; return; }
    memcpy(tmp, from, n); tmp[n] = '\0';
    page_strip_markup(tmp); page_decode_entities(tmp);
    for (const char *p = tmp; *p && w + 1 < cap; p++) {
        unsigned char c = (unsigned char)*p;
        if (isspace(c)) { if (w && out[w - 1] != ' ') out[w++] = ' '; continue; }
        out[w++] = *p;
    }
    while (w && out[w - 1] == ' ') w--;
    /* Never end inside a UTF-8 sequence. */
    while (w && ((unsigned char)out[w - 1] & 0xC0) == 0x80) w--;
    if (w && (unsigned char)out[w - 1] >= 0xC0) w--;
    out[w] = '\0';
    free(tmp);
}

/* Cut at a word near limit and mark the cut. */
static void depth_cut(char *s, size_t limit) {
    size_t n = strlen(s), cut = limit;
    if (n <= limit) return;
    while (cut > limit * 2 / 3 && s[cut] != ' ') cut--;
    snprintf(s + cut, 8, "%s", "…");
}

static int depth_tag_is(const char *p, const char *name) {
    size_t n = strlen(name);
    return !strncasecmp(p, name, n) && (p[n] == '>' || p[n] == ' ' || p[n] == '\t' || p[n] == '\n' || p[n] == '/');
}

static void depth_host(const char *url, char *out, size_t cap) {
    const char *p = strstr(url, "://"); size_t w = 0;
    out[0] = '\0';
    if (!p) return;
    p += 3;
    if (!strncasecmp(p, "www.", 4)) p += 4;
    while (*p && *p != '/' && *p != ':' && *p != '?' && *p != '#' && w + 1 < cap) out[w++] = (char)tolower((unsigned char)*p++);
    out[w] = '\0';
}

/* Read an entry's HTML: paragraphs grouped under headings, and linked names. */
static void depth_parse(const char *s, const char *end, ItemDepth *d, const char *own_host) {
    int section = -1;
    const char *p = s;
    while (p < end && (p = memchr(p, '<', (size_t)(end - p)))) {
        const char *q = p + 1;
        int heading = (q[0] == 'h' || q[0] == 'H') && q[1] >= '2' && q[1] <= '4' && (q[2] == '>' || q[2] == ' ');
        if (heading || depth_tag_is(q, "p") || depth_tag_is(q, "li") || depth_tag_is(q, "blockquote")) {
            char close[16];
            const char *open_end = memchr(q, '>', (size_t)(end - q)), *stop;
            if (!open_end) break;
            if (heading) snprintf(close, sizeof(close), "</h%c", q[1]);
            else snprintf(close, sizeof(close), "</%s", depth_tag_is(q, "p") ? "p" : depth_tag_is(q, "li") ? "li" : "blockquote");
            stop = strcasestr(open_end, close);
            if (!stop || stop > end) stop = end;
            if (heading) {
                char title[200]; int known = -1;
                depth_text(open_end + 1, stop, title, sizeof(title));
                /* Publishers repeat a box for small screens; one heading is one section. */
                for (int k = 0; k < d->sections; k++) if (!strcmp(d->section[k], title)) known = k;
                if (known >= 0) section = known;
                else if (title[0] && d->sections < DEPTH_SECTIONS) { snprintf(d->section[d->sections], sizeof(d->section[0]), "%s", title); section = d->sections++; }
            } else if (d->paras < DEPTH_PARAS) {
                DepthPara *para = &d->para[d->paras];
                int repeated = 0;
                depth_text(open_end + 1, stop, para->text, sizeof(para->text));
                depth_cut(para->text, 700);
                for (int k = 0; k < d->paras; k++) if (!strcmp(d->para[k].text, para->text)) repeated = 1;
                /* A dateline or a lone label is not something said. */
                if (!repeated && strlen(para->text) >= 60) { para->section = section; d->paras++; }
            }
        }
        if (depth_tag_is(q, "a") && d->names < DEPTH_NAMES) {
            char href[600] = "", name[120] = "", host[160];
            const char *h = strcasestr(q, "href="), *open_end = memchr(q, '>', (size_t)(end - q)), *stop;
            if (h && open_end && h < open_end && (h[5] == '"' || h[5] == '\'')) {
                const char *v = h + 6, *ve = strchr(v, h[5]);
                if (ve && ve < open_end && (size_t)(ve - v) < sizeof(href)) { memcpy(href, v, (size_t)(ve - v)); href[ve - v] = '\0'; }
            }
            stop = open_end ? strcasestr(open_end, "</a") : NULL;
            if (href[0] && stop && stop < end) {
                depth_text(open_end + 1, stop, name, sizeof(name));
                page_decode_entities(href);
                depth_host(href, host, sizeof(host));
                /* A name, not a sentence or a "read more"; the publisher's own
                 * pages are the publisher, not a name it mentions. */
                size_t len = strlen(name);
                int dup = 0;
                for (int k = 0; k < d->names; k++) if (!strcasecmp(d->name[k].name, name)) dup = 1;
                if (public_http_url(href) && host[0] && strcmp(host, own_host) && len >= 2 && len <= 80 && !dup &&
                    strcasecmp(name, "here") && strcasecmp(name, "read more") && strcasecmp(name, "link") && !strstr(name, "http")) {
                    DepthName *n = &d->name[d->names++];
                    memset(n, 0, sizeof(*n));
                    snprintf(n->name, sizeof(n->name), "%s", name);
                    snprintf(n->href, sizeof(n->href), "%s", href);
                    snprintf(n->host, sizeof(n->host), "%s", host);
                    n->section = section;
                }
            }
        }
        p = q;
    }
}

/* A field name as words: "date_published" and "datePublished" read
 * "Date published"; a type prefix ("strMeal") and a trailing ordinal
 * ("strIngredient3") are not part of the name. */
static void depth_label(const char *key, int len, char *out, size_t cap) {
    size_t w = 0;
    if (len > 3 && !strncmp(key, "str", 3) && isupper((unsigned char)key[3])) { key += 3; len -= 3; }
    while (len > 1 && isdigit((unsigned char)key[len - 1])) len--;
    for (int i = 0; i < len && w + 2 < cap; i++) {
        char c = key[i];
        if (c == '_' || c == '-' || c == '.') { if (w && out[w - 1] != ' ') out[w++] = ' '; continue; }
        if (i && isupper((unsigned char)c) && islower((unsigned char)key[i - 1])) out[w++] = ' ';
        out[w] = w ? (char)tolower((unsigned char)c) : (char)toupper((unsigned char)c);
        w++;
    }
    out[w] = '\0';
}

/* A value as text, its JSON escapes decoded and its whitespace folded. */
static void depth_fact_value(const bf_json_node_t *n, char *out, size_t cap) {
    out[0] = '\0';
    if (n->type == BF_JSON_BOOL) { snprintf(out, cap, "%s", n->val.b ? "yes" : "no"); return; }
    if (!page_json_text(n, 0, out, cap)) { out[0] = '\0'; return; }
    size_t w = 0;
    for (size_t r = 0; out[r]; r++) {
        if (isspace((unsigned char)out[r])) { if (w && out[w - 1] != ' ') out[w++] = ' '; continue; }
        out[w++] = out[r];
    }
    while (w && out[w - 1] == ' ') w--;
    out[w] = '\0';
}

/* An identifier is a key, not something a reader learns from. */
static int depth_is_id(const char *key, int len) {
    return (len == 2 && !strncasecmp(key, "id", 2)) || (len > 2 && !strncmp(key, "id", 2) && isupper((unsigned char)key[2])) ||
           (len > 3 && (!strncmp(key + len - 3, "_id", 3) || !strncmp(key + len - 2, "Id", 2) || !strncmp(key + len - 4, "uuid", 4)));
}

/* A record in a structured body: the object holding the item's link (or its
 * title), read as facts -- every scalar field it states, and those of one
 * nested object, named in words.  The fields shown already (title, link,
 * picture) and empty values are left out. */
static void depth_record(const char *body, size_t len, const PageItem *item, ItemDepth *d) {
    char err[96];
    bf_json_doc_t *doc = bf_json_parse(body, len, err, sizeof(err));
    const bf_json_node_t *record = NULL;
    if (!doc) return;
    for (int pass = 0; pass < 2 && !record; pass++) for (int i = 0, n = bf_json_count(doc); i < n && !record; i++) {
        const bf_json_node_t *node = bf_json_at(doc, i);
        if (node->type != BF_JSON_STRING || node->parent < 0) continue;
        const char *needle = pass == 0 ? item->href : item->title;
        int match = needle[0] && node->val.str.len == (int)strlen(needle) && !memcmp(node->val.str.ptr, needle, (size_t)node->val.str.len);
        if (!match) continue;
        const bf_json_node_t *parent = bf_json_at(doc, node->parent);
        if (!parent || parent->type != BF_JSON_OBJECT) continue;
        /* Records sharing a landing page are told apart by their title. */
        int titled = !item->title[0];
        for (const bf_json_node_t *c = bf_json_child_first(doc, parent); c && !titled; c = bf_json_child_next(doc, c))
            titled = c->type == BF_JSON_STRING && c->val.str.len == (int)strlen(item->title) && !memcmp(c->val.str.ptr, item->title, (size_t)c->val.str.len);
        if (titled) record = parent;
    }
    for (const bf_json_node_t *f = record ? bf_json_child_first(doc, record) : NULL; f && d->facts < DEPTH_FACTS; f = bf_json_child_next(doc, f)) {
        const bf_json_node_t *inner = f->type == BF_JSON_OBJECT ? bf_json_child_first(doc, f) : NULL;
        for (const bf_json_node_t *g = inner ? inner : f; g && d->facts < DEPTH_FACTS; g = inner ? bf_json_child_next(doc, g) : NULL) {
            char key[160] = "", value[320];
            if (!g->key || g->type == BF_JSON_ARRAY || g->type == BF_JSON_OBJECT || g->type == BF_JSON_NULL) { if (!inner) break; continue; }
            depth_fact_value(g, value, sizeof(value));
            /* Already on the page (title, link, picture, the lead it opened
             * with), empty, or an identifier: not a fact worth repeating. */
            if (!value[0] || depth_is_id(g->key, g->key_len) || !strcmp(value, item->title) || !strcmp(value, item->href) ||
                !strcmp(value, item->image) || (item->body[0] && !strncmp(value, item->body, strlen(item->body) > 80 ? 80 : strlen(item->body)))) { if (!inner) break; continue; }
            if (inner && f->key) { depth_label(f->key, f->key_len, key, sizeof(key)); size_t k = strlen(key); if (k + 2 < sizeof(key)) { key[k++] = ' '; key[k] = '\0'; } }
            { char part[80]; depth_label(g->key, g->key_len, part, sizeof(part)); size_t k = strlen(key);
              if (k) part[0] = (char)tolower((unsigned char)part[0]);
              snprintf(key + k, sizeof(key) - k, "%s", part); }
            /* Numbered fields ("ingredient1", "ingredient2", ...) are one list. */
            int joined = 0;
            for (int k = 0; k < d->facts && !joined; k++)
                if (!strcmp(d->fact_key[k], key)) {
                    size_t used = strlen(d->fact_val[k]);
                    if (used + strlen(value) + 3 < sizeof(d->fact_val[0])) snprintf(d->fact_val[k] + used, sizeof(d->fact_val[0]) - used, ", %s", value);
                    joined = 1;
                }
            if (!joined) {
                snprintf(d->fact_key[d->facts], sizeof(d->fact_key[0]), "%s", key);
                snprintf(d->fact_val[d->facts], sizeof(d->fact_val[0]), "%s", value);
                d->facts++;
            }
            if (!inner) break;
        }
    }
    /* A provenance chain is shown only when its containing record has an
     * exact, unique source link and matching title. Shared landing pages and
     * duplicate titles cannot transfer another object's history. */
    int matches = 0;
    for (int i = 0, n = bf_json_count(doc); item->href[0] && i < n; i++) {
        const bf_json_node_t *node = bf_json_at(doc, i), *parent;
        if (node->type != BF_JSON_STRING || node->parent < 0 ||
            node->val.str.len != (int)strlen(item->href) ||
            memcmp(node->val.str.ptr, item->href, (size_t)node->val.str.len)) continue;
        parent = bf_json_at(doc, node->parent);
        if (!parent || parent->type != BF_JSON_OBJECT) continue;
        for (const bf_json_node_t *v = bf_json_child_first(doc, parent); v; v = bf_json_child_next(doc, v))
            if (v->type == BF_JSON_STRING && v->val.str.len == (int)strlen(item->title) &&
                !memcmp(v->val.str.ptr, item->title, (size_t)v->val.str.len)) { matches++; break; }
    }
    if (record && matches == 1) {
        int linked = 0;
        for (const bf_json_node_t *v = bf_json_child_first(doc, record); v; v = bf_json_child_next(doc, v))
            if (v->type == BF_JSON_STRING && v->val.str.len == (int)strlen(item->href) &&
                !memcmp(v->val.str.ptr, item->href, (size_t)v->val.str.len)) linked = 1;
        const bf_json_node_t *list = linked ? bf_json_obj_get(doc, record, "provenance") : NULL;
        if (list && list->type == BF_JSON_ARRAY)
            for (const bf_json_node_t *entry = bf_json_child_first(doc, list);
                 entry && d->provenances < 8; entry = bf_json_child_next(doc, entry)) {
                if (entry->type != BF_JSON_OBJECT) continue;
                const bf_json_node_t *description = bf_json_obj_get(doc, entry, "description");
                const bf_json_node_t *date = bf_json_obj_get(doc, entry, "date");
                if (!description || description->type != BF_JSON_STRING) continue;
                char raw[1200];
                depth_fact_value(description, raw, sizeof(raw));
                depth_text(raw, raw + strlen(raw), d->provenance[d->provenances].description,
                           sizeof(d->provenance[0].description));
                if (!d->provenance[d->provenances].description[0]) continue;
                if (date && date->type == BF_JSON_STRING)
                    depth_fact_value(date, d->provenance[d->provenances].date,
                                     sizeof(d->provenance[0].date));
                d->provenances++;
            }
    }
    bf_json_free(doc);
}

/* The entry for this item in its source's retained body, by its link first and
 * its title second; its richest content field, as HTML. */
static int depth_read(const char *source, const PageItem *item, ItemDepth *d) {
    const char *root = getenv("BONFYRE_FOREIGN_ROOT");
    char path[PATH_MAX], own[160];
    memset(d, 0, sizeof(*d));
    if (!root || !*root || !source[0] || strchr(source, '/') ||
        snprintf(path, sizeof(path), "%s/bodies/%s.body", root, source) >= (int)sizeof(path)) return 0;
    /* One body, read once: a publisher's page asks for many entries from the
     * same retained body, so the last body read is kept by path, size and
     * modification time and handed out as a copy. */
    static char cached_path[PATH_MAX]; static char *cached; static off_t cached_size; static time_t cached_mtime;
    struct stat st;
    if (stat(path, &st) || !S_ISREG(st.st_mode) || st.st_size <= 0 || (unsigned long long)st.st_size > DEPTH_BODY_CAP) return 0;
    if (!cached || strcmp(cached_path, path) || cached_size != st.st_size || cached_mtime != st.st_mtime) {
        FILE *f = fopen(path, "rb");
        char *fresh = f ? malloc((size_t)st.st_size + 1) : NULL;
        if (!fresh || fread(fresh, 1, (size_t)st.st_size, f) != (size_t)st.st_size) { free(fresh); if (f) fclose(f); return 0; }
        fclose(f); fresh[st.st_size] = '\0';
        free(cached); cached = fresh; cached_size = st.st_size; cached_mtime = st.st_mtime;
        snprintf(cached_path, sizeof(cached_path), "%s", path);
    }
    char *body = malloc((size_t)st.st_size + 1);
    if (!body) return 0;
    memcpy(body, cached, (size_t)st.st_size + 1);
    /* A structured body is read as the item's own record. */
    { const char *first = body; while (*first && isspace((unsigned char)*first)) first++;
      if (*first == '{' || *first == '[') { depth_record(body, (size_t)st.st_size, item, d); free(body); return d->facts > 0; } }
    const char *at = NULL;
    if (item->href[0]) at = strstr(body, item->href);
    if (!at && item->title[0] && strlen(item->title) > 12) at = strstr(body, item->title);
    if (!at) { free(body); return 0; }
    /* The entry around it. */
    const char *start = NULL, *stop = NULL;
    for (const char *p = at; p > body; p--)
        if (*p == '<' && (depth_tag_is(p + 1, "item") || depth_tag_is(p + 1, "entry"))) { start = p; break; }
    if (start) { const char *a = strstr(at, "</item>"), *b = strstr(at, "</entry>"); stop = a && (!b || a < b) ? a : b; }
    if (!start || !stop) { free(body); return 0; }
    static const char *fields[] = {"content:encoded", "content", "description", "summary", "media:description", "itunes:summary"};
    const char *content = NULL, *content_end = NULL;
    for (size_t k = 0; k < sizeof(fields) / sizeof(fields[0]) && !content; k++) {
        char open[40], close[40];
        snprintf(open, sizeof(open), "<%s", fields[k]); snprintf(close, sizeof(close), "</%s>", fields[k]);
        for (const char *o = start; (o = strstr(o, open)) && o < stop; o++) {
            char after = o[strlen(open)];
            if (after != '>' && after != ' ') continue;
            const char *gt = strchr(o, '>'), *c = gt ? strstr(gt, close) : NULL;
            if (gt && c && c < stop && c - gt > 40) { content = gt + 1; content_end = c; }
            break;
        }
    }
    if (!content) { free(body); return 0; }
    size_t n = (size_t)(content_end - content);
    char *html_text = malloc(n + 1);
    if (!html_text) { free(body); return 0; }
    memcpy(html_text, content, n); html_text[n] = '\0';
    { char *c = strstr(html_text, "<![CDATA["); if (c == html_text || (c && c - html_text < 8)) { memmove(html_text, c + 9, strlen(c + 9) + 1); char *e = strstr(html_text, "]]>"); if (e) *e = '\0'; } }
    /* Escaped markup ("&lt;p&gt;") is markup once decoded. */
    if (strstr(html_text, "&lt;")) page_decode_entities(html_text);
    depth_host(item->href, own, sizeof(own));
    depth_parse(html_text, html_text + strlen(html_text), d, own);
    /* Plain text with no paragraphs is one paragraph. */
    if (!d->paras && !strchr(html_text, '<')) {
        depth_text(html_text, html_text + strlen(html_text), d->para[0].text, sizeof(d->para[0].text));
        if (strlen(d->para[0].text) >= 60) { d->para[0].section = -1; depth_cut(d->para[0].text, 700); d->paras = 1; }
    }
    free(html_text); free(body);
    return d->paras || d->names;
}

/* Each linked name, resolved to where it opens here: a publisher this estate
 * addresses opens its subject host, a known identity its page. */
static void depth_resolve_names(ItemDepth *d, EstateReferenceStores *stores) {
    for (int k = 0; k < d->names; k++) {
        DepthName *n = &d->name[k];
        EstateReference r;
        estate_reference_resolve(stores, n->href, &r);
        if (r.estate) snprintf(n->ident, sizeof(n->ident), "%s", r.url[0] == '/' ? r.url : "");
        if (r.name[0]) snprintf(n->root_name, sizeof(n->root_name), "%s", r.name);
        /* Group by the publisher the name belongs to, or its registrable name. */
        if (r.host[0]) snprintf(n->group, sizeof(n->group), "%s", !strncasecmp(r.host, "www.", 4) ? r.host + 4 : r.host);
        else {
            const char *last = strrchr(n->host, '.'), *g = n->host;
            int labels = 1;
            for (const char *c = n->host; *c; c++) labels += *c == '.';
            if (last && labels > 2) {
                const char *second = last - 1;
                while (second > n->host && *second != '.') second--;
                int keep = strlen(last + 1) == 2 && (size_t)(last - second - (*second == '.')) <= 3 ? 3 : 2;
                g = n->host + strlen(n->host);
                for (int seen = 0; g > n->host; g--) if (g[-1] == '.' && ++seen == keep) break;
            }
            snprintf(n->group, sizeof(n->group), "%s", g);
        }
        snprintf(n->href, sizeof(n->href), "%s", r.estate && r.url[0] ? r.url : n->href);
        n->platform = depth_platform(n->group);
        n->section = n->section; /* kept */
    }
}

static void depth_join(char *into, size_t cap, const char *text) {
    size_t used = strlen(into);
    if (!text || !*text || strstr(into, text)) return;
    snprintf(into + used, cap - used, "%s%s", used ? " · " : "", text);
}

/* The piece's parts, the organizations it names and its publisher's channels,
 * as relation material for the scene compiler.  Nothing here is rendered. */
static void depth_fill_material(const ItemDepth *d, const PageItem *item, const char *publisher, PageMaterial *rel) {
    PageStream *parts = &rel->stream[BF_PAGE_MATERIAL_PASSAGE], *named = &rel->stream[BF_PAGE_MATERIAL_ORGANIZATION];
    for (int s = 0; s < d->sections && parts->count < PAGE_STREAM_MAX; s++) {
        PageItem *it = &parts->item[parts->count++];
        memset(it, 0, sizeof(*it));
        it->kind = BF_PAGE_MATERIAL_PASSAGE;
        snprintf(it->title, sizeof(it->title), "%s", d->section[s]);
        for (int k = 0; k < d->paras; k++) if (d->para[k].section == s) { snprintf(it->body, 300, "%s", d->para[k].text); depth_cut(it->body, 260); break; }
        snprintf(it->href, sizeof(it->href), "%s", item->href);
        snprintf(it->by, sizeof(it->by), "%s", publisher);
    }
    for (int k = 0; k < d->names && named->count < PAGE_STREAM_MAX; k++) {
        int seen = d->name[k].platform;
        for (int j = 0; j < k; j++) if (!strcmp(d->name[j].group, d->name[k].group)) seen = 1;
        if (seen) continue;
        PageItem *it = &named->item[named->count++];
        const DepthName *n = &d->name[k];
        memset(it, 0, sizeof(*it));
        it->kind = BF_PAGE_MATERIAL_ORGANIZATION;
        snprintf(it->title, sizeof(it->title), "%s", n->root_name[0] ? n->root_name : n->group);
        snprintf(it->href, sizeof(it->href), "%s", n->href);
        snprintf(it->by, sizeof(it->by), "%s", n->group);
        /* What the piece links of theirs, and where in the piece. */
        for (int j = k; j < d->names; j++) {
            if (strcmp(d->name[j].group, n->group)) continue;
            if (strcmp(d->name[j].name, it->title)) depth_join(it->body, sizeof(it->body), d->name[j].name);
        }
        if (n->section >= 0 && n->section < d->sections) { char in[240]; snprintf(in, sizeof(in), "in %s", d->section[n->section]); depth_join(it->body, sizeof(it->body), in); }
    }
}

/* Words for a channel a person can read: what it carries and how. */
static void depth_channel_words(const char *protocol, const char *material, char *out, size_t cap) {
    const char *how = !strcmp(protocol, "rss") ? "RSS feed" : !strcmp(protocol, "atom") ? "Atom feed" : !strcmp(protocol, "json") || !strcmp(protocol, "jsonfeed") ? "JSON feed" :
                      !strcmp(protocol, "ical") ? "calendar" : !strcmp(protocol, "activitypub") ? "fediverse account" : !strcmp(protocol, "websub") ? "WebSub hub" : NULL;
    if (how && material[0]) snprintf(out, cap, "%c%s %s", toupper((unsigned char)material[0]), material + 1, how);
    else if (how) snprintf(out, cap, "%c%s", toupper((unsigned char)how[0]), how + 1);
    else snprintf(out, cap, "Website");
}

/* Every channel the publisher of this source has, as the frontier knows it. */
static void depth_fill_channels(const char *source, const char *publisher, const char *publisher_url, PageMaterial *rel) {
    sqlite3 *db = bf_sqlite_open_readonly_path(getenv("BONFYRE_SOURCE_FRONTIER_DB"), 1000);
    sqlite3_stmt *st = NULL;
    PageStream *channels = &rel->stream[BF_PAGE_MATERIAL_CHANNEL];
    if (!db) return;
    if (sqlite3_prepare_v2(db, "SELECT c.address,COALESCE(c.material,''),COALESCE(c.protocol,''),c.state,COALESCE(c.census_source,''),"
                               "COALESCE(c.discovered_from,''),COALESCE(c.refusal,''),r.host,COALESCE(c.last_seen,''),COALESCE(c.title,'') "
                               "FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.root_id=(SELECT root_id FROM channel WHERE census_source=?1 LIMIT 1) "
                               "ORDER BY c.census_source=?1 DESC, c.state IN ('admitted','active') DESC, c.address LIMIT 24", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW && channels->count < PAGE_STREAM_MAX) {
            const char *address = (const char *)sqlite3_column_text(st, 0), *material = (const char *)sqlite3_column_text(st, 1);
            const char *protocol = (const char *)sqlite3_column_text(st, 2), *state = (const char *)sqlite3_column_text(st, 3);
            const char *census = (const char *)sqlite3_column_text(st, 4), *why = (const char *)sqlite3_column_text(st, 5);
            const char *refusal = (const char *)sqlite3_column_text(st, 6), *host = (const char *)sqlite3_column_text(st, 7);
            const char *seen = (const char *)sqlite3_column_text(st, 8);
            PageItem *it = &channels->item[channels->count++];
            struct tm tm;
            memset(it, 0, sizeof(*it));
            it->kind = BF_PAGE_MATERIAL_CHANNEL;
            /* What kind of channel it is, led by its own title where the
             * ledger has one: a dozen rows all reading "Website" said nothing. */
            {
                char words[160];
                const char *named = (const char *)sqlite3_column_text(st, 9);
                depth_channel_words(protocol, material, words, sizeof(words));
                if (named && *named) snprintf(it->title, sizeof(it->title), "%s · %s", named, words);
                else snprintf(it->title, sizeof(it->title), "%s", words);
            }
            {   /* The feed opens its own page here; its address is on that page. */
                static const char hex[] = "0123456789ABCDEF";
                size_t w = (size_t)snprintf(it->href, sizeof(it->href), "/channels?channel=");
                for (const unsigned char *c = (const unsigned char *)address; *c && w + 4 < sizeof(it->href); c++) {
                    if (isalnum(*c) || strchr("-._~", *c)) it->href[w++] = (char)*c;
                    else { it->href[w++] = '%'; it->href[w++] = hex[*c >> 4]; it->href[w++] = hex[*c & 15]; }
                }
                it->href[w] = '\0';
            }
            snprintf(it->by, sizeof(it->by), "%s", publisher);
            snprintf(it->note, sizeof(it->note), "%s", host ? host : "");
            /* Said once, as a fact about the channel: where this piece came
             * through, or that the estate reads it.  Why a channel is not
             * read is the frontier's own business. */
            if (census && !strcmp(census, source)) snprintf(it->body, sizeof(it->body), "This piece came through here");
            else if (state && (!strcmp(state, "admitted") || !strcmp(state, "active"))) snprintf(it->body, sizeof(it->body), "Read here");
            (void)why; (void)refusal;
            memset(&tm, 0, sizeof(tm));
            if (seen && strptime(seen, "%Y-%m-%dT%H:%M:%S", &tm)) it->at = timegm(&tm);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    (void)publisher_url;
}

/* The piece's own lead, as the item's body. */
static void depth_emit_said(const ItemDepth *d) {
    for (int k = 0; k < d->paras && d->para[k].section < 0 && k < 2; k++) { fputs("<p class=\"thing-body\">", stdout); html(d->para[k].text); fputs("</p>", stdout); }
}

/* A publisher's relations across its recent pieces: every organization those
 * pieces link, counted by how many name it, beside the publisher's channels.
 * Reads the retained entries of the newest pieces (bounded) from the archive. */
static void depth_publisher_relations(const char *sources_csv, const char *publisher, EstateReferenceStores *stores, PageMaterial *rel) {
    sqlite3 *archive = page_archive_open(0);
    sqlite3_stmt *st = NULL;
    PageStream *named = &rel->stream[BF_PAGE_MATERIAL_ORGANIZATION];
    char first[96] = "";
    int counts[PAGE_STREAM_MAX] = {0}, pieces = 0;
    if (!sources_csv || !*sources_csv) return;
    { const char *comma = strchr(sources_csv, ','); size_t n = comma ? (size_t)(comma - sources_csv) : strlen(sources_csv);
      if (n < sizeof(first)) { memcpy(first, sources_csv, n); first[n] = '\0'; } }
    if (archive && sqlite3_prepare_v2(archive, "SELECT source,key,title,href FROM item WHERE instr(','||?1||',', ','||source||',')>0 ORDER BY at DESC LIMIT 24", -1, &st, NULL) == SQLITE_OK) {
        ItemDepth *d = calloc(1, sizeof(*d));
        PageItem *probe = calloc(1, sizeof(*probe));
        sqlite3_bind_text(st, 1, sources_csv, -1, SQLITE_TRANSIENT);
        while (d && probe && sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            snprintf(probe->title, sizeof(probe->title), "%s", sqlite3_column_text(st, 2));
            snprintf(probe->href, sizeof(probe->href), "%s", sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
            if (!depth_read(source, probe, d) || !d->names) continue;
            pieces++;
            depth_resolve_names(d, stores);
            for (int k = 0; k < d->names; k++) {
                int at = -1;
                if (d->name[k].platform) continue;
                for (int j = 0; j < named->count; j++) if (!strcmp(named->item[j].by, d->name[k].group)) { at = j; break; }
                if (at < 0 && named->count < PAGE_STREAM_MAX) {
                    PageItem *it = &named->item[at = named->count++];
                    memset(it, 0, sizeof(*it));
                    it->kind = BF_PAGE_MATERIAL_ORGANIZATION;
                    snprintf(it->title, sizeof(it->title), "%s", d->name[k].root_name[0] ? d->name[k].root_name : d->name[k].group);
                    snprintf(it->href, sizeof(it->href), "%s", d->name[k].href);
                    snprintf(it->by, sizeof(it->by), "%s", d->name[k].group);
                }
                if (at >= 0) {
                    int seen = 0;
                    /* Count a piece once per organization. */
                    for (int j = 0; j < k; j++) if (!strcmp(d->name[j].group, d->name[k].group)) seen = 1;
                    if (!seen) counts[at]++;
                }
            }
        }
        free(d); free(probe);
    }
    sqlite3_finalize(st);
    if (archive) sqlite3_close(archive);
    /* Most often named first; each says how often. */
    for (int i = 1; i < named->count; i++)
        for (int j = i; j > 0 && counts[j] > counts[j - 1]; j--) {
            PageItem keep = named->item[j]; int c = counts[j];
            named->item[j] = named->item[j - 1]; counts[j] = counts[j - 1];
            named->item[j - 1] = keep; counts[j - 1] = c;
        }
    for (int i = 0; i < named->count; i++)
        snprintf(named->item[i].body, sizeof(named->item[i].body), "In %d of the last %d pieces", counts[i], pieces);
    (void)publisher;
    if (first[0]) depth_fill_channels(first, publisher, "", rel);
}

/* A structured record's facts, in the item's existing facts list. */
static void depth_emit_facts(const ItemDepth *d) {
    if (d->facts) {
        fputs("<dl class=\"facts record\">", stdout);
        for (int k = 0; k < d->facts; k++) {
            fputs("<div><dt>", stdout); html(d->fact_key[k]); fputs("</dt><dd>", stdout);
            if (public_http_url(d->fact_val[k])) { fputs("<a href=\"", stdout); html(d->fact_val[k]); fputs("\" rel=\"noopener\">", stdout); html(d->fact_val[k]); fputs("</a>", stdout); }
            else html(d->fact_val[k]);
            fputs("</dd></div>", stdout);
        }
        fputs("</dl>", stdout);
    }
    if (d->provenances) {
        fputs("<section class=\"source-provenance\"><h2>Provenance</h2><p>As recorded by the source.</p><ol>", stdout);
        for (int i = 0; i < d->provenances; i++) {
            fputs("<li>", stdout);
            if (d->provenance[i].date[0]) { fputs("<strong>", stdout); html(d->provenance[i].date); fputs("</strong> ", stdout); }
            html(d->provenance[i].description); fputs("</li>", stdout);
        }
        fputs("</ol></section>", stdout);
    }
}
