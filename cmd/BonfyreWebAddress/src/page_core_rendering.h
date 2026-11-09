#ifndef BONFYRE_PAGE_CORE_RENDERING_H
#define BONFYRE_PAGE_CORE_RENDERING_H
/* Extracted verbatim from main.c lines 6405-10681 (mechanical split, no logic changes). */
static long page_days_from_civil(int y, int m, int d) {
    long era; long yoe, doy, doe; int mp = m > 2 ? m - 3 : m + 9;
    y -= m <= 2;
    era = (y >= 0 ? y : y - 399) / 400;
    yoe = (long)y - era * 400;
    doy = (153L * mp + 2) / 5 + d - 1;
    doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

static void page_zone_offset(const char *p, long *offset) {
    *offset = 0;
    while (p && *p == ' ') p++;
    if (p && (*p == '+' || *p == '-') && isdigit((unsigned char)p[1]) && isdigit((unsigned char)p[2])) {
        long sign = *p == '-' ? -1 : 1, hh = (p[1] - '0') * 10 + (p[2] - '0'), mm = 0;
        const char *q = p + 3;
        if (*q == ':') q++;
        if (isdigit((unsigned char)q[0]) && isdigit((unsigned char)q[1])) mm = (q[0] - '0') * 10 + (q[1] - '0');
        *offset = sign * (hh * 3600 + mm * 60);
    }
}

/* Publication carriers date things as RFC 822 (RSS) or ISO 8601 (everything
 * else).  An unreadable date is unknown, never guessed and never "now". */
static time_t page_time(const char *s) {
    int y = 0, mo = 0, d = 0, h = 0, mi = 0, se = 0, n = 0, i;
    long offset = 0; char mon[4] = {0};
    if (!s) return 0;
    while (isspace((unsigned char)*s)) s++;
    if (!*s) return 0;
    {
        /* Seconds since 1970, the way APIs that count time say when. */
        size_t digits = strspn(s, "0123456789");
        if (digits >= 9 && digits <= 10 && !s[digits]) return (time_t)strtoll(s, NULL, 10);
    }
    if (isdigit((unsigned char)s[0]) && sscanf(s, "%4d-%2d-%2d%n", &y, &mo, &d, &n) == 3) {
        const char *p = s + n; int m2 = 0;
        if ((*p == 'T' || *p == ' ') && sscanf(p + 1, "%2d:%2d:%2d%n", &h, &mi, &se, &m2) == 3) {
            p += 1 + m2;
            if (*p == '.') { p++; while (isdigit((unsigned char)*p)) p++; }
            page_zone_offset(p, &offset);
        }
    } else {
        const char *p = strchr(s, ',');
        p = p ? p + 1 : s;
        if (sscanf(p, " %2d %3s %4d %2d:%2d:%2d%n", &d, mon, &y, &h, &mi, &se, &n) < 6) return 0;
        for (i = 0; i < 12; i++) if (!strncasecmp(mon, page_month_names[i], 3)) { mo = i + 1; break; }
        page_zone_offset(p + n, &offset);
    }
    if (y < 1970 || mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || se > 60) return 0;
    return (time_t)(page_days_from_civil(y, mo, d) * 86400L + h * 3600L + mi * 60L + se - offset);
}

/* "2h ago" for today, a date otherwise -- the way every feed people read
 * already says it.  The public projection lives at most a minute, so a
 * relative time cannot drift into a false one. */
static struct tm *page_local_time(time_t at, struct tm *tm);

static void page_when(time_t at, time_t now, char *out, size_t cap) {
    struct tm tm, now_tm; long diff;
    out[0] = '\0';
    if (at <= 0) return;
    diff = (long)(now - at);
    if (diff >= 0 && diff < 60) snprintf(out, cap, "just now");
    else if (diff >= 0 && diff < 3600) snprintf(out, cap, "%ldm ago", diff / 60);
    else if (diff >= 0 && diff < 86400) snprintf(out, cap, "%ldh ago", diff / 3600);
    else if (diff >= 0 && diff < 7 * 86400) snprintf(out, cap, "%ldd ago", diff / 86400);
    else if (gmtime_r(&at, &tm) && gmtime_r(&now, &now_tm)) {
        if (tm.tm_year == now_tm.tm_year) snprintf(out, cap, "%s %d", page_month_names[tm.tm_mon], tm.tm_mday);
        else snprintf(out, cap, "%s %d, %d", page_month_names[tm.tm_mon], tm.tm_mday, tm.tm_year + 1900);
    }
}

/* An event is spoken of forwards.  "2h ago" is the wrong tense for something
 * that starts at eight, so the agenda says when it starts and how long from
 * now, and an event already under way says so. */
static void page_when_ahead(time_t at, time_t ends, time_t now, char *out, size_t cap) {
    struct tm tm, now_tm; long ahead = (long)(at - now);
    out[0] = '\0';
    if (at <= 0) return;
    if (ends > 0 && at <= now && now < ends) { snprintf(out, cap, "on now"); return; }
    if (ahead < 0) { page_when(at, now, out, cap); return; }
    if (ahead < 3600) snprintf(out, cap, "in %ldm", ahead / 60 ? ahead / 60 : 1);
    else if (ahead < 12 * 3600) snprintf(out, cap, "in %ldh", ahead / 3600);
    else if (page_local_time(at, &tm) && page_local_time(now, &now_tm)) {
        static const char *const days[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
        int hour = tm.tm_hour % 12; const char *half = tm.tm_hour < 12 ? "am" : "pm";
        char clock[16] = "";
        if (!(tm.tm_hour == 0 && tm.tm_min == 0)) {
            if (tm.tm_min) snprintf(clock, sizeof(clock), " %d:%02d%s", hour ? hour : 12, tm.tm_min, half);
            else snprintf(clock, sizeof(clock), " %d%s", hour ? hour : 12, half);
        }
        if (ahead < 7 * 86400) snprintf(out, cap, "%s%s", days[tm.tm_wday], clock);
        else snprintf(out, cap, "%s %d%s", page_month_names[tm.tm_mon], tm.tm_mday, clock);
    }
}

/* A subject reference such as person:maya-lee or artist:field-notes is an
 * address, not a name.  Until a profile owner supplies a display name the page
 * shows the handle as words rather than as a coordinate. */
static void page_humanize(const char *ref, char *out, size_t cap) {
    static const char *const small[] = {"a ", "an ", "and ", "at ", "for ", "in ", "of ", "on ", "the ", "to "};
    const char *p = ref ? strchr(ref, ':') : NULL; size_t n = 0, i; int word = 1;
    p = p && p[1] ? p + 1 : (ref ? ref : "");
    for (; *p && n + 1 < cap; p++) {
        char c = *p == '-' || *p == '_' ? ' ' : *p;
        int keep_small = 0;
        if (word && n > 0)
            for (i = 0; i < sizeof(small) / sizeof(small[0]); i++) {
                size_t len = strlen(small[i]) - 1;
                if (!strncmp(p, small[i], len) && (p[len] == '-' || p[len] == '_')) keep_small = 1;
            }
        out[n++] = word && !keep_small && isalpha((unsigned char)c) ? (char)toupper((unsigned char)c) : c;
        word = c == ' ';
    }
    out[n] = '\0';
}

/* A feed's title is often its whole marketing line ("The best vintage and
 * classic cars for sale online - Bring a Trailer").  A person reads the name
 * in it when a segment matches the address it came from. Otherwise keep the
 * full source label: a short phrase alone does not establish a publisher. */
static void page_short_publisher(const char *name, const char *href, char *out, size_t cap) {
    static const char *const seps[] = {" - ", " | ", " \xe2\x80\x93 ", " \xe2\x80\x94 ", " \xc2\xb7 ", " :: "};
    const char *seg[8]; size_t len[8], count = 0, i, j;
    char host[128] = "", squashed[128] = "";
    const char *p, *h;
    if (!out || !cap) return;
    out[0] = '\0';
    if (!name || !*name) return;
    if (href && (h = strstr(href, "://"))) {
        h += 3; i = strcspn(h, "/:?#");
        if (i < sizeof(host)) { memcpy(host, h, i); host[i] = '\0'; }
        if (!strncasecmp(host, "www.", 4)) memmove(host, host + 4, strlen(host + 4) + 1);
        /* the registrable label, letters only: bringatrailer */
        for (i = 0, j = 0; host[i] && host[i] != '.' && j + 1 < sizeof(squashed); i++) if (isalnum((unsigned char)host[i])) squashed[j++] = (char)tolower((unsigned char)host[i]);
        squashed[j] = '\0';
    }
    p = name;
    while (*p && count < 8) {
        const char *next = NULL; size_t sl = 0, s;
        for (s = 0; s < sizeof(seps) / sizeof(seps[0]); s++) {
            const char *q = strstr(p, seps[s]);
            if (q && (!next || q < next)) { next = q; sl = strlen(seps[s]); }
        }
        seg[count] = p; len[count] = next ? (size_t)(next - p) : strlen(p); count++;
        if (!next) break;
        p = next + sl;
    }
    if (count < 2) { snprintf(out, cap, "%s", name); return; }
    if (squashed[0]) for (i = 0; i < count; i++) {
        char letters[256]; size_t k, m = 0;
        for (k = 0; k < len[i] && m + 1 < sizeof(letters); k++) if (isalnum((unsigned char)seg[i][k])) letters[m++] = (char)tolower((unsigned char)seg[i][k]);
        letters[m] = '\0';
        if (m >= 3 && (strstr(squashed, letters) || strstr(letters, squashed))) { snprintf(out, cap, "%.*s", (int)len[i], seg[i]); return; }
    }
    snprintf(out, cap, "%s", name);
}

/* A byline names who published a thing the way feeds people read already do:
 * by the publisher's own domain (usgs.gov, schneier.com).  A source handle is
 * an internal slug and its label is a cataloguing phrase; neither is a name,
 * and this page does not invent one. */
static void page_publisher(const char *url, char *out, size_t cap) {
    const char *host, *end, *labels[8]; size_t count = 0, keep = 2, n;
    const char *p;
    out[0] = '\0';
    if (!url || !(host = strstr(url, "://"))) return;
    host += 3;
    end = host + strcspn(host, "/:?#");
    if (end <= host) return;
    {
        /* A path-named publisher on a shared host is more exact than the host. */
        const char *path = end + strcspn(end, "/?#"); int pass;
        for (pass = 0; pass < 2; pass++)
            for (n = 0; n < BF_PAGE_PUBLISHER_HOST_COUNT; n++) {
                const BfPagePublisherHost *known = &bf_page_publisher_hosts[n];
                if (strlen(known->host) != (size_t)(end - host) || strncasecmp(host, known->host, (size_t)(end - host))) continue;
                if (pass == 0 ? !known->path[0] || strncmp(path, known->path, strlen(known->path)) : known->path[0] != '\0') continue;
                snprintf(out, cap, "%s", known->publisher);
                return;
            }
    }
    labels[count++] = host;
    for (p = host; p < end && count < 8; p++) if (*p == '.' && p + 1 < end) labels[count++] = p + 1;
    if (count < 2) keep = 1;
    else if (count >= 3 && end - labels[count - 1] == 2) {
        size_t second = (size_t)(labels[count - 1] - labels[count - 2]) - 1;
        if (second <= 3) keep = 3;   /* bbc.co.uk, abc.net.au */
    }
    if (keep > count) keep = count;
    n = (size_t)(end - labels[count - keep]);
    if (n >= cap) n = cap - 1;
    memcpy(out, labels[count - keep], n); out[n] = '\0';
    for (n = 0; out[n]; n++) out[n] = (char)tolower((unsigned char)out[n]);
}

static unsigned page_read_order;

static PageItem *page_push(PageStream *stream, const char *title) {
    PageItem *item;
    if (!stream || !title || !*title || stream->count >= PAGE_STREAM_MAX) return NULL;
    item = &stream->item[stream->count++];
    memset(item, 0, sizeof(*item));
    item->order = ++page_read_order;
    snprintf(item->title, sizeof(item->title), "%s", title);
    return item;
}

/* Feed text arrives entity-escaped at least once; the page escapes it again
 * on the way out.  Decoding here is what keeps "A&amp;M" from reaching a
 * person as five characters. */
static void page_utf8(unsigned long cp, char **w, const char *end) {
    char *o = *w;
    if (cp < 0x80 && o + 1 <= end) *o++ = (char)cp;
    else if (cp < 0x800 && o + 2 <= end) { *o++ = (char)(0xC0 | (cp >> 6)); *o++ = (char)(0x80 | (cp & 0x3F)); }
    else if (cp < 0x10000 && o + 3 <= end) { *o++ = (char)(0xE0 | (cp >> 12)); *o++ = (char)(0x80 | ((cp >> 6) & 0x3F)); *o++ = (char)(0x80 | (cp & 0x3F)); }
    else if (cp < 0x110000 && o + 4 <= end) { *o++ = (char)(0xF0 | (cp >> 18)); *o++ = (char)(0x80 | ((cp >> 12) & 0x3F)); *o++ = (char)(0x80 | ((cp >> 6) & 0x3F)); *o++ = (char)(0x80 | (cp & 0x3F)); }
    *w = o;
}

static void page_decode_entities(char *s) {
    static const struct { const char *name; char ch; } named[] = {
        {"&amp;", '&'}, {"&lt;", '<'}, {"&gt;", '>'}, {"&quot;", '"'}, {"&apos;", '\''}, {"&#39;", '\''}, {"&nbsp;", ' '}
    };
    /* Typographic names feeds use often; each is longer than its UTF-8. */
    static const struct { const char *name; unsigned long cp; } wide[] = {
        {"&mdash;", 0x2014}, {"&ndash;", 0x2013}, {"&hellip;", 0x2026}, {"&rsquo;", 0x2019}, {"&lsquo;", 0x2018},
        {"&ldquo;", 0x201C}, {"&rdquo;", 0x201D}, {"&rarr;", 0x2192}, {"&larr;", 0x2190}, {"&sect;", 0x00A7},
        {"&copy;", 0x00A9}, {"&eacute;", 0x00E9}, {"&middot;", 0x00B7}, {"&bull;", 0x2022}, {"&trade;", 0x2122}
    };
    char *r = s, *w = s;
    size_t i;
    if (!s) return;
    while (*r) {
        int done = 0;
        if (*r == '&') {
            for (i = 0; i < sizeof(named) / sizeof(named[0]); i++) {
                size_t len = strlen(named[i].name);
                if (!strncmp(r, named[i].name, len)) { *w++ = named[i].ch; r += len; done = 1; break; }
            }
            for (i = 0; !done && i < sizeof(wide) / sizeof(wide[0]); i++) {
                size_t len = strlen(wide[i].name);
                if (!strncmp(r, wide[i].name, len)) { page_utf8(wide[i].cp, &w, r + len); r += len; done = 1; }
            }
            if (!done && r[1] == '#') {
                char *stop = NULL; unsigned long cp;
                cp = (r[2] == 'x' || r[2] == 'X') ? strtoul(r + 3, &stop, 16) : strtoul(r + 2, &stop, 10);
                /* UTF-8 is never longer than the entity it replaces, so the
                 * write stays behind the read. */
                if (stop && *stop == ';' && cp > 0 && stop - r < 12) { page_utf8(cp, &w, stop + 1); r = stop + 1; done = 1; }
            }
        }
        if (!done) *w++ = *r++;
    }
    *w = '\0';
}

/* One story reached through two carriers is still one story. */
/* A source that declares a key field gives each record its own identity: the
 * provider's record id is the key, so records sharing a landing page or a
 * title stay distinct.  Set by the reader around one push, never left set. */
static char page_record_id[256];

static PageItem *page_push_story(PageStream *stream, const char *title, const char *by,
                                 const char *when, const char *href) {
    PageItem *item; char clean[768]; int i;
    if (!title || !*title) return NULL;
    snprintf(clean, sizeof(clean), "%s", title);
    /* Feeds are routinely escaped twice ("A&amp;amp;M").  Two passes reach
     * the words; output is escaped again, so nothing decoded here is markup. */
    page_decode_entities(clean);
    page_decode_entities(clean);
    for (i = 0; i < stream->count; i++)
        if (!page_record_id[0] && (!strcmp(stream->item[i].title, clean) ||
            (href && *href && !strcmp(stream->item[i].href, href)))) return NULL;
    if (!(item = page_push(stream, clean))) return NULL;
    snprintf(item->by, sizeof(item->by), "%s", by ? by : "");
    page_decode_entities(item->by);
    if (public_http_url(href)) snprintf(item->href, sizeof(item->href), "%s", href);
    item->at = page_time(when);
    return item;
}

/* Newest first; things of the same time (or none) keep the order their
 * sources gave them, on every platform's sort. */
static int page_newest_first(const void *a, const void *b) {
    const PageItem *x = a, *y = b;
    if (x->at != y->at) return x->at < y->at ? 1 : -1;
    return x->order < y->order ? -1 : x->order > y->order ? 1 : 0;
}

/* A picture reaches the page only as an https URL with nothing that could
 * close the attribute it is written into. */
static int page_image_url_safe(const char *url) {
    const char *p;
    if (!url || strncmp(url, "https://", 8) || strlen(url) >= 1024) return 0;
    for (p = url; *p; p++) if (*p == '"' || *p == '\'' || *p == '<' || *p == '>' || *p == '\\' || isspace((unsigned char)*p)) return 0;
    return 1;
}

/* The host of an https address and the path after it. */
static int page_https_host_path(const char *url, const char **host, size_t *host_len, const char **path) {
    if (!url || strncmp(url, "https://", 8)) return 0;
    *host = url + 8;
    *host_len = strcspn(*host, "/:?#");
    *path = *host + *host_len;
    if (**path == ':') *path += strcspn(*path, "/?#");
    return *host_len > 0;
}

/* A picture whose terms hold only where it was first used is not shown here
 * (page-composition.cue nativePageWithheldPictures). */
static int page_picture_withheld(const char *url) {
    const char *host, *path; size_t host_len, i;
    if (!page_https_host_path(url, &host, &host_len, &path)) return 0;
    for (i = 0; i < BF_PAGE_WITHHELD_PICTURE_COUNT; i++)
        if (strlen(bf_page_withheld_pictures[i].host) == host_len && !strncasecmp(host, bf_page_withheld_pictures[i].host, host_len) &&
            !strncmp(path, bf_page_withheld_pictures[i].path, strlen(bf_page_withheld_pictures[i].path))) return 1;
    return 0;
}

/* The commons that keeps a picture or a playable file, and the file's name
 * there, exactly as its address spells it. */
static const BfPagePictureOrigin *page_picture_origin(const char *url, char *name, size_t cap) {
    const char *host, *path; size_t host_len, i;
    if (!page_https_host_path(url, &host, &host_len, &path)) return NULL;
    for (i = 0; i < BF_PAGE_PICTURE_ORIGIN_COUNT; i++) {
        const BfPagePictureOrigin *origin = &bf_page_picture_origins[i];
        const char *rest; size_t len; int level;
        if (strlen(origin->host) != host_len || strncasecmp(host, origin->host, host_len) ||
            strncmp(path, origin->path, strlen(origin->path))) continue;
        rest = path + strlen(origin->path);
        if (origin->hashed) {
            if (!strncmp(rest, "thumb/", 6)) rest += 6;
            for (level = 1; level <= 2; level++) {
                size_t part = strcspn(rest, "/");
                if (part != (size_t)level || rest[part] != '/' || strspn(rest, "0123456789abcdef") < part) return NULL;
                rest += part + 1;
            }
        }
        len = strcspn(rest, "/?#");
        if (!len || len >= cap) return NULL;
        memcpy(name, rest, len); name[len] = '\0';
        return origin;
    }
    return NULL;
}

/* A commons picture is asked for at the width its place on the page needs;
 * the commons never enlarges past the original.  Any other picture is used as
 * its source gave it. */
static void page_picture_at(const char *url, int width, char *out, size_t cap) {
    char name[512]; const BfPagePictureOrigin *origin = page_picture_origin(url, name, sizeof(name));
    const char *t; size_t n = 0;
    if (cap) out[0] = '\0';
    if (!page_image_url_safe(url)) return;
    if (!origin) { snprintf(out, cap, "%s", url); return; }
    for (t = origin->sized; *t && n + 1 < cap; ) {
        int w = 0;
        if (!strncmp(t, "{name}", 6)) { w = snprintf(out + n, cap - n, "%s", name); t += 6; }
        else if (!strncmp(t, "{width}", 7)) { w = snprintf(out + n, cap - n, "%d", width); t += 7; }
        else if (!strncmp(t, "{size}", 6)) {
            /* The host's word for the smallest size at least this wide. */
            char word[32] = ""; const char *z = origin->sizes;
            while (z && *z) {
                char *stop = NULL; long at = strtol(z, &stop, 10); size_t len;
                if (!stop || *stop != ':') break;
                len = strcspn(stop + 1, ",");
                if (len < sizeof(word)) { memcpy(word, stop + 1, len); word[len] = '\0'; }
                if (at >= width) break;
                z = stop + 1 + len; if (*z == ',') z++;
            }
            if (!word[0]) { snprintf(out, cap, "%s", url); return; }
            w = snprintf(out + n, cap - n, "%s", word); t += 6;
        } else if (!strncmp(t, "{ext}", 5)) {
            const char *end = url + strcspn(url, "?#"), *dot = end;
            while (dot > url && *dot != '.' && *dot != '/') dot--;
            if (*dot != '.' || end - dot > 6) { snprintf(out, cap, "%s", url); return; }
            w = snprintf(out + n, cap - n, "%.*s", (int)(end - dot - 1), dot + 1); t += 5;
        }
        else { out[n++] = *t++; continue; }
        if (w < 0 || (size_t)w >= cap - n) { out[0] = '\0'; return; }
        n += (size_t)w;
    }
    out[n < cap ? n : cap - 1] = '\0';
    if (!page_image_url_safe(out)) snprintf(out, cap, "%s", url);
}

/* What a stated licence lets a person do (page-composition.cue
 * nativePageLicences).  Terms nobody recognises promise nothing. */
static const BfPageLicence *page_licence(const char *words, const char *href) {
    char text[768]; size_t i, n = 0; const char *parts[2] = {href, words}; int k;
    for (k = 0; k < 2; k++)
        for (const char *p = parts[k]; p && *p && n + 2 < sizeof(text); p++) text[n++] = (char)tolower((unsigned char)*p);
    text[n] = '\0';
    if (!n) return NULL;
    { /* "CC BY 4.0" is matched as "cc by " */ size_t len = strlen(text); if (len + 1 < sizeof(text)) { text[len] = ' '; text[len + 1] = '\0'; } }
    for (i = 0; i < BF_PAGE_LICENCE_COUNT; i++) if (strstr(text, bf_page_licences[i].match)) return &bf_page_licences[i];
    return NULL;
}

/* A licence's words with the version its address carries: ".../by-nc/4.0/"
 * reads "CC BY-NC 4.0". */
static void page_licence_words(const BfPageLicence *licence, const char *href, char *out, size_t cap) {
    const char *p = href; char version[16] = "";
    snprintf(out, cap, "%s", licence ? licence->words : "");
    if (!licence || !href || !strncmp(licence->words, "Public domain", 13) || !strcmp(licence->words, "CC0")) return;
    for (; p && *p; p++)
        if (*p == '/' && isdigit((unsigned char)p[1]) && p[2] == '.' && isdigit((unsigned char)p[3])) { snprintf(version, sizeof(version), "%c.%c", p[1], p[3]); break; }
    if (version[0]) snprintf(out, cap, "%s %s", licence->words, version);
}

/* A page a host also publishes a player for (nativePageEmbedHosts). */
static int page_embed_for(const char *href, char *out, size_t cap) {
    const char *host, *path; size_t host_len, i;
    if (cap) out[0] = '\0';
    if (!href || !page_https_host_path(href, &host, &host_len, &path)) return 0;
    for (i = 0; i < BF_PAGE_EMBED_HOST_COUNT; i++) {
        const BfPageEmbedHost *e = &bf_page_embed_hosts[i];
        const char *id, *slot; size_t id_len;
        if (strlen(e->host) != host_len || strncasecmp(host, e->host, host_len) || strncmp(path, e->path, strlen(e->path))) continue;
        id = path + strlen(e->path);
        id_len = strcspn(id, "/?#");
        if (!id_len || strspn(id, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-") < id_len) return 0;
        if (!(slot = strstr(e->embed, "{id}"))) return 0;
        if (snprintf(out, cap, "%.*s%.*s%s", (int)(slot - e->embed), e->embed, (int)id_len, id, slot + 4) >= (int)cap) { out[0] = '\0'; return 0; }
        return page_image_url_safe(out);
    }
    return 0;
}

/* A screen that boxed a retained body wins over every page that would show
 * it, until a person clears it.  The retired browser reader honoured this;
 * the native page must too. */
static int page_twin_withheld(sqlite3 *db, const char *twin) {
    sqlite3_stmt *st = NULL; int held = 0;
    if (!db || !twin || !*twin || !cms_type_registered(db, "content_flag")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM content_flag WHERE twin=?1 AND status='published' "
          "AND COALESCE(review_state,'')<>'cleared' LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, twin, -1, SQLITE_TRANSIENT);
    held = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return held;
}

/* The census date vocabulary: a source that answers for today keys its list
 * by today's date, so the extract path carries a token rather than a date. */
static void page_date_tokens(const char *in, char *out, size_t cap) {
    static const struct { const char *token; long back; int iso; } tokens[] = {
        {"{today-iso}", 0, 1}, {"{yesterday-iso}", 86400, 1}, {"{week-ago-iso}", 7 * 86400, 1}, {"{month-ago-iso}", 30 * 86400, 1},
        {"{today}", 0, 0}, {"{yesterday}", 86400, 0}, {"{week-ago}", 7 * 86400, 0}
    };
    size_t n = 0, i; time_t now = time(NULL);
    if (!cap) return;
    while (in && *in && n + 1 < cap) {
        int matched = 0;
        for (i = 0; *in == '{' && i < sizeof(tokens) / sizeof(tokens[0]); i++) {
            size_t len = strlen(tokens[i].token);
            if (!strncmp(in, tokens[i].token, len)) {
                time_t t = now - tokens[i].back; struct tm tm; char date[16];
                if (gmtime_r(&t, &tm) && strftime(date, sizeof(date), tokens[i].iso ? "%Y-%m-%d" : "%Y/%m/%d", &tm)) {
                    size_t dl = strlen(date);
                    if (n + dl < cap) { memcpy(out + n, date, dl); n += dl; }
                }
                in += len; matched = 1; break;
            }
        }
        if (!matched) out[n++] = *in++;
    }
    out[n] = '\0';
}

/* K.walk, natively: dotted keys, numeric segments index arrays, an empty path
 * is the node itself. */
static const bf_json_node_t *page_walk(const bf_json_doc_t *doc, const bf_json_node_t *node, const char *path) {
    char buf[256], *seg, *save = NULL;
    if (!node || !path || !*path) return node;
    page_date_tokens(path, buf, sizeof(buf));
    for (seg = strtok_r(buf, ".", &save); seg && node; seg = strtok_r(NULL, ".", &save)) {
        if (node->type == BF_JSON_ARRAY) {
            char *stop = NULL; long index = strtol(seg, &stop, 10);
            node = stop && !*stop && index >= 0 && index < 100000 ? bf_json_arr_get(doc, node, (int)index) : NULL;
        } else if (node->type == BF_JSON_OBJECT) node = bf_json_obj_get(doc, node, seg);
        else node = NULL;
    }
    return node;
}

/* JSON string escapes are decoded here; numbers read as their text, the way
 * the census reader stringified them. */
static int page_json_text(const bf_json_node_t *node, int strings_only, char *out, size_t cap) {
    const char *s; int len, i; char *w, *end;
    if (!out || cap < 2) return 0;
    w = out; end = out + cap - 1;
    out[0] = '\0';
    if (!node) return 0;
    if (node->type == BF_JSON_INT && !strings_only) { snprintf(out, cap, "%lld", (long long)node->val.i); return 1; }
    if (node->type == BF_JSON_DOUBLE && !strings_only) { snprintf(out, cap, "%g", node->val.d); return 1; }
    if (node->type != BF_JSON_STRING || !(s = bf_json_get_str(node, &len))) return 0;
    for (i = 0; i < len && w < end; i++) {
        if (s[i] != '\\' || i + 1 >= len) { *w++ = s[i]; continue; }
        i++;
        switch (s[i]) {
        case 'n': case 'r': case 't': *w++ = ' '; break;
        case 'b': case 'f': break;
        case 'u':
            if (i + 4 < len) {
                char hex[5] = {s[i+1], s[i+2], s[i+3], s[i+4], 0};
                unsigned long cp = strtoul(hex, NULL, 16);
                i += 4;
                if (cp >= 0xD800 && cp < 0xDC00 && i + 6 < len && s[i+1] == '\\' && s[i+2] == 'u') {
                    char lo_hex[5] = {s[i+3], s[i+4], s[i+5], s[i+6], 0};
                    unsigned long lo = strtoul(lo_hex, NULL, 16);
                    if (lo >= 0xDC00 && lo < 0xE000) { cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00); i += 6; }
                }
                page_utf8(cp, &w, end);
            }
            break;
        default: *w++ = s[i];
        }
    }
    *w = '\0';
    return out[0] != '\0';
}

/* A source that marks its own adult or sensitive material is taken at its
 * word.  Honouring the flag is reading a field, not editorial judgement. */
/* A contract field may name alternatives, "caption.value|name.value": the
 * first that holds text is the one read. */
static int page_field_text(const bf_json_doc_t *doc, const bf_json_node_t *item, const char *field,
                           int strings_only, char *out, size_t cap) {
    char path[256]; const char *p = field;
    if (!out || cap < 2) return 0;
    out[0] = '\0';
    while (p && *p) {
        size_t n = strcspn(p, "|");
        if (n && n < sizeof(path)) {
            memcpy(path, p, n); path[n] = '\0';
            const bf_json_node_t *node = page_walk(doc, item, path);
            if (node && node->type == BF_JSON_ARRAY) {
                /* Several values ("creator": ["A", "B"]) read as one line. */
                const bf_json_node_t *part; size_t used = 0; int count = 0; char one[256];
                for (part = bf_json_child_first(doc, node); part && count < 3; part = bf_json_child_next(doc, part))
                    if (page_json_text(part, 1, one, sizeof(one)) && one[0] && used + strlen(one) + 3 < cap) {
                        used += (size_t)snprintf(out + used, cap - used, "%s%s", count ? ", " : "", one); count++;
                    }
                if (count) return 1;
            } else if (page_json_text(node, strings_only, out, cap) && out[0]) return 1;
        }
        p += n;
        if (*p == '|') p++;
    }
    out[0] = '\0';
    return 0;
}

/* Some sources say who made a thing, or what it is, in markup.  The words are
 * kept; the markup is not, and the page escapes the words again. */
static void page_strip_markup(char *s) {
    char *r = s, *w = s; int space = 0;
    if (!s) return;
    while (*r) {
        if (*r == '<') {
            char *close = strchr(r, '>');
            if (close) {
                if (!strncasecmp(r, "<br", 3) || !strncasecmp(r, "<p", 2) || !strncasecmp(r, "</p", 3) ||
                    !strncasecmp(r, "<li", 3) || !strncasecmp(r, "<div", 4) || !strncasecmp(r, "</div", 5)) space = 1;
                r = close + 1;
                continue;
            }
        }
        if (isspace((unsigned char)*r)) { space = 1; r++; continue; }
        if (space && w > s) *w++ = ' ';
        space = 0;
        *w++ = *r++;
    }
    *w = '\0';
    page_decode_entities(s);
    page_decode_entities(s);
}

/* A file's name is not a title.  "File:Eurasian_coot_(Fulica_atra).jpg" is
 * read as "Eurasian coot (Fulica atra)"; a title that is not a file name is
 * left exactly as its source wrote it. */
static void page_file_title(char *title) {
    static const char *const kinds[] = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".tif", ".tiff", ".svg", ".webm", ".ogv",
                                        ".ogg", ".oga", ".opus", ".wav", ".mp3", ".flac", ".mid", ".pdf"};
    size_t len = strlen(title), i; char *r, *w; int named = 0;
    for (i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        size_t k = strlen(kinds[i]);
        if (len > k && !strcasecmp(title + len - k, kinds[i])) { title[len - k] = '\0'; named = 1; break; }
    }
    if (!named) return;
    r = title;
    if (!strncmp(r, "File:", 5)) r += 5;
    for (w = title; *r; r++) {
        if (*r == '%' && isxdigit((unsigned char)r[1]) && isxdigit((unsigned char)r[2])) {
            char hex[3] = {r[1], r[2], '\0'};
            *w++ = (char)strtol(hex, NULL, 16); r += 2;
        } else *w++ = *r == '_' ? ' ' : *r;
    }
    *w = '\0';
}

static int page_item_flagged(const bf_json_doc_t *doc, const bf_json_node_t *item) {
    static const char *const flags[] = {"nsfw", "sensitive", "over_18", "is_nsfw"};
    static const char *const scopes[] = {NULL, "post", "community"};
    size_t i, k;
    for (k = 0; k < sizeof(scopes) / sizeof(scopes[0]); k++) {
        const bf_json_node_t *scope = scopes[k] ? bf_json_obj_get(doc, item, scopes[k]) : item;
        if (!scope || scope->type != BF_JSON_OBJECT) continue;
        for (i = 0; i < sizeof(flags) / sizeof(flags[0]); i++) {
            const bf_json_node_t *flag = bf_json_obj_get(doc, scope, flags[i]);
            if (flag && flag->type == BF_JSON_BOOL && flag->val.b) return 1;
        }
    }
    return 0;
}

/* K._imageURL: absolute stays; @field prefixes the maker's host; pre|post
 * wraps an identifier; anything else is a fixed origin. */
static void page_image_url(const bf_json_doc_t *doc, const bf_json_node_t *item, const char *field,
                           const char *base, char *out, size_t cap) {
    char path[1024], host[256];
    out[0] = '\0';
    if (!field || !*field || !page_field_text(doc, item, field, 1, path, sizeof(path))) return;
    /* A truncated URL names a different resource, so it is dropped, not cut. */
    int written = -1;
    if (!strncmp(path, "https://", 8) || !strncmp(path, "http://", 7)) written = snprintf(out, cap, "%s", path);
    else if (base && base[0] == '@') {
        if (page_json_text(page_walk(doc, item, base + 1), 1, host, sizeof(host))) written = snprintf(out, cap, "https://%s%s", host, path);
    } else if (base && strchr(base, '|')) {
        const char *cut = strchr(base, '|');
        written = snprintf(out, cap, "%.*s%s%s", (int)(cut - base), base, path, cut + 1);
    } else if (base && *base) written = snprintf(out, cap, "%s%s", base, path);
    if (written < 0 || (size_t)written >= cap || !page_image_url_safe(out)) out[0] = '\0';
}

/* Feed markup helpers for what RSS and Atom put in attributes. */
static int page_tag_attr(const char *start, const char *end, const char *tag, const char *attr, char *out, size_t cap) {
    char open[64]; const char *p; size_t tl;
    if (snprintf(open, sizeof(open), "<%s", tag) >= (int)sizeof(open)) return 0;
    tl = strlen(open);
    for (p = strstr(start, open); p && p < end; p = strstr(p + 1, open)) {
        const char *close, *a; char want[64];
        if (!(p[tl] == ' ' || p[tl] == '\t' || p[tl] == '\n' || p[tl] == '/' || p[tl] == '>')) continue;
        close = strchr(p, '>'); if (!close || close > end) return 0;
        if (snprintf(want, sizeof(want), " %s=", attr) >= (int)sizeof(want)) return 0;
        a = strstr(p, want);
        if (a && a < close) {
            char q; const char *v = a + strlen(want); const char *stop; size_t n;
            q = *v; if (q != '"' && q != '\'') continue;
            v++; stop = strchr(v, q); if (!stop || stop > close) continue;
            n = (size_t)(stop - v); if (n >= cap) n = cap - 1;
            memcpy(out, v, n); out[n] = '\0'; page_decode_entities(out);
            return n > 0;
        }
    }
    return 0;
}

static void page_feed_picture(const char *start, const char *end, char *out, size_t cap) {
    static const char *const tags[] = {"media:thumbnail", "media:content", "enclosure", "itunes:image", "image"};
    size_t i;
    out[0] = '\0';
    for (i = 0; i < sizeof(tags) / sizeof(tags[0]); i++) {
        char type[64] = "";
        if (!(page_tag_attr(start, end, tags[i], "url", out, cap) || page_tag_attr(start, end, tags[i], "href", out, cap)) || !page_image_url_safe(out)) continue;
        /* An enclosure or media element is a picture only when it says so; a
         * podcast's enclosure is its audio. */
        if ((!strcmp(tags[i], "enclosure") || !strcmp(tags[i], "media:content")) &&
            ((page_tag_attr(start, end, tags[i], "type", type, sizeof(type)) && strncmp(type, "image/", 6)) ||
             (page_tag_attr(start, end, tags[i], "medium", type, sizeof(type)) && strcmp(type, "image")))) continue;
        return;
    }
    out[0] = '\0';
    {   /* Some feeds only put the picture inside escaped HTML in the body. */
        const char *img = strstr(start, "&lt;img"), *raw = strstr(start, "<img");
        const char *at = img && img < end ? img : (raw && raw < end ? raw : NULL);
        const char *src = at ? strstr(at, "src=") : NULL;
        if (src && src < end) {
            const char *v = src + 4, *stop; size_t n; int escaped = !strncmp(v, "&quot;", 6);
            if (escaped) v += 6; else if (*v == '"' || *v == '\'') v++;
            stop = escaped ? strstr(v, "&quot;") : strpbrk(v, "\"' >");
            if (stop && stop < end && (n = (size_t)(stop - v)) < cap) { memcpy(out, v, n); out[n] = '\0'; page_decode_entities(out); }
            if (!page_image_url_safe(out)) out[0] = '\0';
        }
    }
}

static const char *page_feed_open(const char *at, const char *element) {
    char open[64]; const char *p = at; size_t len;
    if (!element || snprintf(open, sizeof(open), "<%s", element) >= (int)sizeof(open)) return NULL;
    len = strlen(open);
    while ((p = strstr(p, open)) != NULL) {
        char next = p[len];
        if (next == '>' || isspace((unsigned char)next)) { p = strchr(p, '>'); return p ? p + 1 : NULL; }
        p += len;
    }
    return NULL;
}

/* Routes a retained item to the rhythm its source kind and picture allow. */
/* An item's page key is a digest of its source and its link (or its title
 * when it has none), so the page that lists it and the page that opens it
 * find the same thing without storing anything. */

static void page_item_key(PageItem *item, const char *source) {
    char seed[1400], hex[65]; int n;
    if (!item || !source || !*source) return;
    snprintf(item->source, sizeof(item->source), "%.*s", (int)sizeof(item->source) - 1, source);
    n = page_record_id[0] ? snprintf(seed, sizeof(seed), "%s\n#%s", item->source, page_record_id)
                          : snprintf(seed, sizeof(seed), "%s\n%s", item->source, item->href[0] ? item->href : item->title);
    if (n < 0) return;
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), hex);
    memcpy(item->key, hex, 24); item->key[24] = '\0';
}

static int page_per_source = 24;
/* Set only by callers that are reading a source into the archive rather than
 * composing a page: the archive wants everything a feed carries. */
static int page_read_everything;

/* What a source that knows more says about each thing, beyond its title,
 * link and picture.  Every field is optional and read only as text. */
typedef struct {
    const char *summary, *maker, *rights, *rights_href, *media, *ident;
    const char *people, *where, *transcript_href, *chapters_href, *talk_href;
    long duration_ms;
    const char *about;
    /* What the source said the file is ("audio/mpeg", "video/mp4"): a thing's
     * own evidence about what it is, separate from what its source is filed
     * under. */
    const char *media_type;
} PageRetainedExtra;

/* The identity a thing shares with other sources is the last segment of what
 * the source gave: http://www.wikidata.org/entity/Q42 and Q42 are one thing. */
static void page_ident(const char *raw, char *out, size_t cap) {
    const char *p = raw ? strrchr(raw, '/') : NULL;
    size_t n;
    if (cap) out[0] = '\0';
    if (!raw || !cap) return;
    p = p ? p + 1 : raw;
    n = strspn(p, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-");
    if (!n || n != strlen(p) || n >= cap) return;
    memcpy(out, p, n); out[n] = '\0';
}

static PageItem *page_push_retained(PageMaterial *material, const char *source, BfPageMaterial media, const char *title, const char *by,
                                    const char *sub, const char *when, const char *href, const char *image,
                                    int placed, double lon, double lat, const PageRetainedExtra *extra) {
    char picture[1024] = {0}, who[256], words[768], ident[48] = {0}, name[512], embed[1100];
    const char *asset = extra && extra->media && page_image_url_safe(extra->media) ? extra->media : "";
    const BfPagePictureOrigin *origin;
    BfPageMaterial target;
    PageItem *item;
    int k, i, plays;
    if (image && *image && !page_picture_withheld(image)) {
        const BfPagePictureOrigin *given = page_picture_origin(image, name, sizeof(name));
        /* A host that hands out full-size originals is asked for a picture a
         * page can carry. */
        if (given && given->everywhere) page_picture_at(image, 500, picture, sizeof(picture));
        else snprintf(picture, sizeof(picture), "%s", image);
    }
    plays = asset[0] || page_embed_for(href, embed, sizeof(embed));
    /* What a thing carries outranks what its source is filed under.  Most of
     * this estate's admitted channels were listed as news because that is what
     * their publisher mostly does; a great many of them also hand out an audio
     * or a video file, and a person looking for something to listen to should
     * meet those recordings rather than a page of two.  The evidence is the
     * source's own statement about the file it published, never a guess from
     * an address, and it can only promote a story to what it demonstrably is. */
    if (media == BF_PAGE_MATERIAL_PUBLICATION && extra && extra->media_type) {
        if (asset[0] && !strncmp(extra->media_type, "audio/", 6)) media = BF_PAGE_MATERIAL_AUDIO;
        else if (asset[0] && !strncmp(extra->media_type, "video/", 6)) media = BF_PAGE_MATERIAL_VIDEO;
        /* A picture file the source itself typed and published is a
         * photograph; a thumbnail standing beside a story is not, and is left
         * where it belongs. */
        else if (picture[0] && !strncmp(extra->media_type, "image/", 6)) media = BF_PAGE_MATERIAL_PHOTO;
    }
    /* A thing joins its kind's rhythm only with what that rhythm shows: a
     * picture for a photograph, a picture or something that plays for a
     * video, something that plays for a recording.  Without it, it is a story. */
    target = media == BF_PAGE_MATERIAL_PHOTO ? (picture[0] ? media : BF_PAGE_MATERIAL_PUBLICATION)
           : media == BF_PAGE_MATERIAL_VIDEO ? (picture[0] || plays ? media : BF_PAGE_MATERIAL_PUBLICATION)
           : media == BF_PAGE_MATERIAL_AUDIO ? (plays ? media : BF_PAGE_MATERIAL_PUBLICATION)
           /* An event is an event whether or not it has a picture or a file:
            * what makes it one is that it happens at a time. */
           : media == BF_PAGE_MATERIAL_EVENT ? media
           : BF_PAGE_MATERIAL_PUBLICATION;
    if (extra) page_ident(extra->ident, ident, sizeof(ident));
    /* One thing reported by two sources is one thing on a page: the first
     * report keeps its place. */
    for (k = 0; ident[0] && k < BF_PAGE_MATERIAL_COUNT; k++) {
        if (k == BF_PAGE_MATERIAL_MIX || k == BF_PAGE_MATERIAL_PLACE) continue;
        for (i = 0; i < material->stream[k].count; i++) if (!strcmp(material->stream[k].item[i].ident, ident)) return NULL;
    }
    snprintf(words, sizeof(words), "%s", title ? title : "");
    page_file_title(words);
    /* A short secondary field names who made the item; a long one is what it
     * says, and the publisher stays the byline. */
    if (extra && extra->maker && *extra->maker) snprintf(who, sizeof(who), "%s", extra->maker);
    else if (sub && *sub && strlen(sub) <= 80) snprintf(who, sizeof(who), "%s", sub);
    else snprintf(who, sizeof(who), "%s", by ? by : "");
    if (!(item = page_push_story(&material->stream[target], words, who, when, href))) return NULL;
    if (extra && extra->summary && *extra->summary) snprintf(item->body, sizeof(item->body), "%s", extra->summary);
    else if (sub && strlen(sub) > 80) { snprintf(item->body, sizeof(item->body), "%s", sub); page_decode_entities(item->body); }
    if (picture[0]) snprintf(item->image, sizeof(item->image), "%s", picture);
    if (asset[0]) { snprintf(item->asset, sizeof(item->asset), "%s", asset); snprintf(item->poster, sizeof(item->poster), "%s", picture); }
    if ((origin = page_picture_origin(asset[0] ? asset : picture, name, sizeof(name))) != NULL) {
        snprintf(item->origin, sizeof(item->origin), "%s", origin->name);
        if (snprintf(item->origin_href, sizeof(item->origin_href), "%s%s", origin->page, name) >= (int)sizeof(item->origin_href) ||
            !page_image_url_safe(item->origin_href)) item->origin[0] = item->origin_href[0] = '\0';
    }
    if (extra && ((extra->rights && *extra->rights) || (extra->rights_href && *extra->rights_href))) {
        if (extra->rights_href && public_http_url(extra->rights_href) && strlen(extra->rights_href) < sizeof(item->rights_href))
            snprintf(item->rights_href, sizeof(item->rights_href), "%s", extra->rights_href);
        if (extra->rights && *extra->rights) snprintf(item->rights, sizeof(item->rights), "%s", extra->rights);
        else page_licence_words(page_licence(NULL, item->rights_href), item->rights_href, item->rights, sizeof(item->rights));
    }
    if (extra) {
        /* What an episode's feed says about it, kept as the show's words. */
        if (extra->people) snprintf(item->people, sizeof(item->people), "%s", extra->people);
        if (extra->where) snprintf(item->where, sizeof(item->where), "%s", extra->where);
        if (extra->transcript_href && public_http_url(extra->transcript_href) && strlen(extra->transcript_href) < sizeof(item->transcript_href)) snprintf(item->transcript_href, sizeof(item->transcript_href), "%s", extra->transcript_href);
        if (extra->chapters_href && public_http_url(extra->chapters_href) && strlen(extra->chapters_href) < sizeof(item->chapters_href)) snprintf(item->chapters_href, sizeof(item->chapters_href), "%s", extra->chapters_href);
        if (extra->talk_href && public_http_url(extra->talk_href) && strlen(extra->talk_href) < sizeof(item->talk_href)) snprintf(item->talk_href, sizeof(item->talk_href), "%s", extra->talk_href);
        if (extra->duration_ms > 0) item->duration_ms = extra->duration_ms;
    }
    snprintf(item->ident, sizeof(item->ident), "%s", ident);
    if (extra && extra->about) page_ident(extra->about, item->about, sizeof(item->about));
    snprintf(item->note, sizeof(item->note), "%s", by ? by : "");
    item->kind = (int)target;
    page_item_key(item, source);
    if (placed) {
        /* A placed item is also a point on the page's map; the map and the
         * reading list are two views of the same report, not two reports. */
        PageItem *point = page_push_story(&material->stream[BF_PAGE_MATERIAL_PLACE], words, who, when, href);
        if (point) {
            point->placed = 1; point->lon = lon; point->lat = lat; point->kind = BF_PAGE_MATERIAL_PLACE;
            snprintf(point->note, sizeof(point->note), "%s", by ? by : "");
            snprintf(point->image, sizeof(point->image), "%s", picture);
            page_item_key(point, source);
            item->placed = 1; item->lon = lon; item->lat = lat;
        }
    }
    return item;
}

static int page_json_number(const bf_json_node_t *node, double *out) {
    char text[64], *stop = NULL;
    if (!node) return 0;
    if (node->type == BF_JSON_INT) { *out = (double)node->val.i; return 1; }
    if (node->type == BF_JSON_DOUBLE) { *out = node->val.d; return 1; }
    if (!page_json_text(node, 1, text, sizeof(text))) return 0;
    *out = strtod(text, &stop);
    return stop && stop != text && !*stop;
}

/* Where an item says it is, in the shapes public data already uses: GeoJSON
 * geometry, Darwin Core, and plain latitude/longitude pairs.  A point outside
 * the globe, or the 0,0 that means "unknown", is not a place. */
/* A WKT point, "POINT(-97.33 32.75)", the shape GeoSPARQL answers carry. */
static int page_wkt_point(const char *text, double *lon, double *lat) {
    const char *p = text; char *stop = NULL;
    while (p && isspace((unsigned char)*p)) p++;
    if (!p || strncasecmp(p, "POINT", 5)) return 0;
    p += 5;
    while (isspace((unsigned char)*p)) p++;
    if (*p++ != '(') return 0;
    *lon = strtod(p, &stop);
    if (stop == p) return 0;
    p = stop;
    *lat = strtod(p, &stop);
    return stop != p;
}

static int page_item_place(const bf_json_doc_t *doc, const bf_json_node_t *item, const char *field, double *lon, double *lat) {
    static const char *const pairs[][2] = {
        {"decimalLongitude", "decimalLatitude"}, {"location.longitude", "location.latitude"},
        {"longitude", "latitude"}, {"lon", "lat"}, {"lng", "lat"}, {"coordinates.lon", "coordinates.lat"}
    };
    const bf_json_node_t *coords = page_walk(doc, item, "geometry.coordinates");
    size_t i; int found = 0;
    if (field && *field) {
        /* The census names where the item says it is: a WKT point, or an
         * object holding a longitude and latitude pair. */
        char path[256], text[128]; const char *p = field;
        while (!found && *p) {
            size_t n = strcspn(p, "|");
            if (n && n < sizeof(path)) {
                const bf_json_node_t *node;
                memcpy(path, p, n); path[n] = '\0';
                node = page_walk(doc, item, path);
                if (node && node->type == BF_JSON_STRING && page_json_text(node, 1, text, sizeof(text))) found = page_wkt_point(text, lon, lat);
                for (i = 0; node && node->type == BF_JSON_OBJECT && !found && i < 3; i++)
                    if (page_json_number(page_walk(doc, node, pairs[i + 2][0]), lon) && page_json_number(page_walk(doc, node, pairs[i + 2][1]), lat)) found = 1;
            }
            p += n;
            if (*p == '|') p++;
        }
    }
    if (!found && coords && coords->type == BF_JSON_ARRAY &&
        page_json_number(bf_json_arr_get(doc, coords, 0), lon) && page_json_number(bf_json_arr_get(doc, coords, 1), lat)) found = 1;
    for (i = 0; !found && i < sizeof(pairs) / sizeof(pairs[0]); i++)
        if (page_json_number(page_walk(doc, item, pairs[i][0]), lon) && page_json_number(page_walk(doc, item, pairs[i][1]), lat)) found = 1;
    return found && *lon >= -180 && *lon <= 180 && *lat >= -90 && *lat <= 90 && !(*lon == 0 && *lat == 0);
}

static void page_collect_occurrences(sqlite3 *db, const char *source, PageMaterial *material) {
    sqlite3_stmt *st = NULL;
    if (!material || sqlite3_prepare_v2(db,
          "SELECT p.title,COALESCE(p.published_at,p.observed_at,''),COALESCE(p.canonical_href,''),"
          "COALESCE((SELECT i.url FROM internet_source i WHERE i.source=p.source_ref AND i.status='published' ORDER BY i.id DESC LIMIT 1),''),"
          "COALESCE(p.summary,''),COALESCE((SELECT i.kind FROM internet_source i WHERE i.source=p.source_ref AND i.status='published' ORDER BY i.id DESC LIMIT 1),'') FROM publication_occurrence p "
          "WHERE p.source_ref=?1 AND p.status='published' AND p.visibility='public' AND p.assertion_state='OBSERVED' "
          "ORDER BY p.published_at DESC,p.id DESC LIMIT 18", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char by[256]; const char *kind = (const char *)sqlite3_column_text(st, 5); PageItem *item;
        BfPageMaterial owner_kind = kind && !strcmp(kind, "event")
                                   ? BF_PAGE_MATERIAL_EVENT : BF_PAGE_MATERIAL_PUBLICATION;
        page_publisher((const char *)sqlite3_column_text(st, 3), by, sizeof(by));
        item = page_push_story(&material->stream[owner_kind], (const char *)sqlite3_column_text(st, 0), by,
                               (const char *)sqlite3_column_text(st, 1), (const char *)sqlite3_column_text(st, 2));
        if (item) {
            snprintf(item->note, sizeof(item->note), "%.*s", (int)sizeof(item->note) - 1, by);
            item->kind = owner_kind;
            page_item_key(item, source);
        }
        if (item && sqlite3_column_text(st, 4)) {
            snprintf(item->body, sizeof(item->body), "%s", (const char *)sqlite3_column_text(st, 4));
            page_decode_entities(item->body);
        }
    }
    sqlite3_finalize(st);
}

/* ---- iCalendar ------------------------------------------------------------
 *
 * A calendar line may be folded across several physical lines (a continuation
 * begins with a space or tab) and may carry parameters before its value
 * (DTSTART;TZID=America/Chicago:20260918T200000).  Text values escape commas,
 * semicolons and newlines.  This reads one property out of one VEVENT.
 */
static int page_ics_field(const char *begin, const char *end, const char *name,
                          char *out, size_t cap, char *zone, size_t zone_cap) {
    const char *p = begin; size_t n = strlen(name), used = 0;
    out[0] = '\0';
    if (zone && zone_cap) zone[0] = '\0';
    while (p < end) {
        const char *line_end = memchr(p, '\n', (size_t)(end - p));
        const char *value = NULL, *scan;
        if (!line_end) line_end = end;
        if ((p == begin || p[-1] == '\n') && !strncasecmp(p, name, n) && (p[n] == ':' || p[n] == ';')) {
            for (scan = p + n; scan < line_end && *scan != ':'; scan++) {
                if (zone && zone_cap && !strncasecmp(scan, "TZID=", 5)) {
                    const char *z = scan + 5; size_t zn = 0;
                    while (z < line_end && *z != ':' && *z != ';' && zn + 1 < zone_cap) zone[zn++] = *z++;
                    zone[zn] = '\0';
                }
            }
            value = scan < line_end ? scan + 1 : NULL;
        }
        if (value) {
            const char *v = value;
            for (;;) {
                while (v < line_end && used + 1 < cap) {
                    char c = *v++;
                    if (c == '\r') continue;              /* the line ending, not part of the value */
                    if (c == '\\' && v < line_end) {
                        char esc = *v++;
                        c = esc == 'n' || esc == 'N' ? ' ' : esc;
                    }
                    if ((unsigned char)c < 0x20) c = ' ';   /* one value, one line */
                    out[used++] = c;
                }
                out[used] = '\0';
                /* a folded continuation line belongs to this value */
                if (line_end >= end || line_end[1] != ' ') break;
                v = line_end + 2;
                line_end = memchr(v, '\n', (size_t)(end - v));
                if (!line_end) line_end = end;
            }
            while (used && (out[used - 1] == ' ' || out[used - 1] == '\t')) out[--used] = '\0';
            return 1;
        }
        if (line_end >= end) break;
        p = line_end + 1;
    }
    return 0;
}

/* An event's time is what its calendar states: a UTC stamp, a stamp in a named
 * zone, or a whole day.  A named zone is honoured through the host's own zone
 * database rather than assumed, and an unreadable stamp is no time at all. */
static time_t page_ics_utc(struct tm *tm) {
    /* Arithmetic, not a zone switch: a year's calendar holds thousands of
     * entries, and asking the host to reload its timezone database twice for
     * each of them was the slowest thing a page did. */
    long days = page_days_from_civil(tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);
    return (time_t)(days * 86400L + tm->tm_hour * 3600L + tm->tm_min * 60L + tm->tm_sec);
}

static time_t page_ics_time(const char *value, const char *zone) {
    struct tm tm; int year = 0, mon = 0, day = 0, hour = 0, min = 0, sec = 0; size_t n;
    if (!value || !*value) return 0;
    n = strlen(value);
    if (n >= 15 && value[8] == 'T') {
        if (sscanf(value, "%4d%2d%2dT%2d%2d%2d", &year, &mon, &day, &hour, &min, &sec) != 6) return 0;
    } else if (n >= 8) {
        if (sscanf(value, "%4d%2d%2d", &year, &mon, &day) != 3) return 0;
    } else return 0;
    if (year < 1900 || mon < 1 || mon > 12 || day < 1 || day > 31) return 0;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year = year - 1900; tm.tm_mon = mon - 1; tm.tm_mday = day;
    tm.tm_hour = hour; tm.tm_min = min; tm.tm_sec = sec;
    tm.tm_isdst = -1;
    if (value[n - 1] == 'Z') return page_ics_utc(&tm);
    if (zone && strchr(zone, '/')) {
        /* A named zone is honoured through the host's own database, but the
         * database is only reloaded when the zone actually changes: one
         * calendar states one zone for every entry in it. */
        static char current[64] = "";
        if (strcmp(current, zone)) {
            snprintf(current, sizeof(current), "%s", zone);
            setenv("TZ", zone, 1);
            tzset();
        }
        return mktime(&tm);
    }
    return page_ics_utc(&tm);    /* a floating time is read as it was written */
}

/* The retained-body reader for a source whose material has not been promoted.
 * It reads the census contract exactly as the retired browser reader did --
 * title, date, link, maker, picture, the source's own sensitivity flags -- and
 * gathers instead of emitting, so a page composes across sources. */
static void page_collect_retained(sqlite3 *db, const char *source, PageMaterial *material) {
    sqlite3_stmt *st = NULL;
    char by[256], twin[256], extract[256], title_field[256], meta_field[256], link_field[256];
    char image_field[256], image_base[512], sub_field[256], kind[64];
    char summary_field[256] = "", maker_field[256] = "", rights_field[256] = "", rights_link_field[256] = "";
    char media_field[256] = "", ident_field[256] = "", place_field[256] = "", link_base[512] = "", about_field[256] = "";
    BfPageMaterial media = BF_PAGE_MATERIAL_PUBLICATION;
    char *body = NULL; size_t i; int extended;
    /* A census written before the reading contract grew still reads by its
     * original fields. */
    extended = sqlite3_prepare_v2(db, "SELECT COALESCE(url,''),twin,COALESCE(extract,''),COALESCE(title_field,''),COALESCE(meta_field,''),"
          "COALESCE(link_field,''),COALESCE(image_field,''),COALESCE(image_base,''),COALESCE(sub_field,''),COALESCE(kind,''),"
          "COALESCE(summary_field,''),COALESCE(maker_field,''),COALESCE(rights_field,''),COALESCE(rights_link_field,''),"
          "COALESCE(media_field,''),COALESCE(ident_field,''),COALESCE(place_field,''),COALESCE(link_base,''),COALESCE(about_field,'') "
          "FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK;
    if (!extended && sqlite3_prepare_v2(db, "SELECT COALESCE(url,''),twin,COALESCE(extract,''),COALESCE(title_field,''),COALESCE(meta_field,''),"
          "COALESCE(link_field,''),COALESCE(image_field,''),COALESCE(image_base,''),COALESCE(sub_field,''),COALESCE(kind,'') "
          "FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return; }
#define PCOPY(dst,col) snprintf(dst, sizeof(dst), "%s", sqlite3_column_text(st,col) ? (const char *)sqlite3_column_text(st,col) : "")
    PCOPY(by, 0); PCOPY(twin, 1); PCOPY(extract, 2); PCOPY(title_field, 3); PCOPY(meta_field, 4);
    PCOPY(link_field, 5); PCOPY(image_field, 6); PCOPY(image_base, 7); PCOPY(sub_field, 8); PCOPY(kind, 9);
    if (extended) {
        PCOPY(summary_field, 10); PCOPY(maker_field, 11); PCOPY(rights_field, 12); PCOPY(rights_link_field, 13);
        PCOPY(media_field, 14); PCOPY(ident_field, 15); PCOPY(place_field, 16); PCOPY(link_base, 17); PCOPY(about_field, 18);
    }
#undef PCOPY
    sqlite3_finalize(st);
    /* Read apart from the rest, so a store without the column still reads. */
    char key_field[256] = "";
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(key_field,'') FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(key_field, sizeof(key_field), "%s", sqlite3_column_text(st, 0));
    }
    sqlite3_finalize(st); st = NULL;
    { char url[256]; snprintf(url, sizeof(url), "%s", by); page_publisher(url, by, sizeof(by)); }
    for (i = 0; i < BF_PAGE_SOURCE_KIND_COUNT; i++)
        if (!strcmp(kind, bf_page_source_kinds[i].kind)) media = bf_page_source_kinds[i].material;
    if (page_twin_withheld(db, twin)) return;
    {
        /* A calendar is a year of a place's life and runs to megabytes, while a
         * feed is a page of recent items.  Reading only the first half-megabyte
         * of a calendar shows January and nothing that is coming. */
        /* A page needs the top of a feed; the archive needs the whole of it,
         * and half a megabyte cuts a long feed off in the middle. */
        /* A JSON document cut off is no document at all: a structured body is
         * read whole, up to the archive's bound, where a feed's top suffices. */
        size_t cap = !strncmp(extract, "ics:", 4) ? 12u << 20
                   : (page_read_everything || strncmp(extract, "rss:", 4) ? 6u << 20 : 512u << 10);
        if (!(body = malloc(cap)) || !world_body_for_twin(twin, body, cap)) { free(body); return; }
    }
    if (!strncmp(extract, "ics:", 4)) {
        /* An organizer's own calendar, read as the calendar it is: folded
         * lines rejoined, one item per VEVENT, its start kept in the zone the
         * calendar names, and something published about an event never
         * mistaken for the event itself. */
        const char *cursor = body; int read = 0; time_t now_seconds = time(NULL);
        while (read < page_per_source && (cursor = strstr(cursor, "BEGIN:VEVENT")) != NULL) {
            const char *end = strstr(cursor, "END:VEVENT");
            char summary[768] = "", href[1024] = "", where[512] = "", detail[2048] = "";
            char starts[64] = "", ends[64] = "", start_zone[64] = "", geo[64] = "";
            time_t at = 0, until = 0; double lon = 0, lat = 0; int placed = 0;
            if (!end) break;
            /* When it happens is read first, because most of a year's calendar
             * has already happened: an entry that is over costs two fields
             * instead of seven, which is the difference between a page that
             * waits on a year of history and one that does not. */
            page_ics_field(cursor, end, "DTSTART", starts, sizeof(starts), start_zone, sizeof(start_zone));
            page_ics_field(cursor, end, "DTEND", ends, sizeof(ends), NULL, 0);
            at = page_ics_time(starts, start_zone);
            until = page_ics_time(ends, start_zone);
            /* A calendar is a whole year of a place's life; a page is asking
             * what is still to come, so an event that has already finished is
             * passed over rather than filling the page from January. */
            if (at > 0 && (until > at ? until : at + 86400) < now_seconds) { cursor = end + 10; continue; }
            page_ics_field(cursor, end, "SUMMARY", summary, sizeof(summary), NULL, 0);
            page_ics_field(cursor, end, "URL", href, sizeof(href), NULL, 0);
            page_ics_field(cursor, end, "LOCATION", where, sizeof(where), NULL, 0);
            page_ics_field(cursor, end, "DESCRIPTION", detail, sizeof(detail), NULL, 0);
            page_ics_field(cursor, end, "GEO", geo, sizeof(geo), NULL, 0);
            if (geo[0] && sscanf(geo, "%lf;%lf", &lat, &lon) == 2) placed = 1;
            if (summary[0] && at > 0) {
                PageRetainedExtra extra = {detail, NULL, NULL, NULL, NULL, NULL, NULL, where, NULL, NULL, NULL, 0, NULL, NULL};
                char link[1024], when[32]; struct tm at_tm;
                if (until > at) extra.duration_ms = (long)(until - at) * 1000L;
                snprintf(link, sizeof(link), "%s", href[0] ? href : by);
                page_strip_markup(detail);
                /* The calendar's own stamp, already read into a moment, handed
                 * on as a time the rest of the page understands. */
                gmtime_r(&at, &at_tm);
                strftime(when, sizeof(when), "%Y-%m-%dT%H:%M:%SZ", &at_tm);
                page_push_retained(material, source, BF_PAGE_MATERIAL_EVENT, summary, by, "", when, link, "", placed, lon, lat, &extra);
                read++;
            }
            cursor = end + 10;
        }
        free(body);
        return;
    }
    if (!strncmp(extract, "rss:", 4)) {
        const char *cursor = body, *item_end; char close[64]; int read = 0;
        if (snprintf(close, sizeof(close), "</%s>", extract + 4) >= (int)sizeof(close)) { free(body); return; }
        while (read < page_per_source && (cursor = page_feed_open(cursor, extract + 4)) != NULL && (item_end = strstr(cursor, close)) != NULL) {
            char title[768] = {0}, href[1024] = {0}, when[256] = {0}, who[256] = {0}, image[1024] = {0};
            if (rss_tag_text(cursor, item_end, "title", title, sizeof(title))) {
                if (!page_tag_attr(cursor, item_end, "link", "href", href, sizeof(href)) || !public_http_url(href))
                    rss_tag_text(cursor, item_end, "link", href, sizeof(href));
                if (!public_http_url(href)) { href[0] = '\0'; rss_tag_text(cursor, item_end, "guid", href, sizeof(href)); }
                if (!public_http_url(href)) href[0] = '\0';
                if (!rss_tag_text(cursor, item_end, "pubDate", when, sizeof(when)) &&
                    !rss_tag_text(cursor, item_end, "updated", when, sizeof(when)) &&
                    !rss_tag_text(cursor, item_end, "published", when, sizeof(when)))
                    rss_tag_text(cursor, item_end, "dc:date", when, sizeof(when));
                if (!rss_tag_text(cursor, item_end, "dc:creator", who, sizeof(who))) {
                    const char *author = strstr(cursor, "<author");
                    if (author && author < item_end) rss_tag_text(author, item_end, "name", who, sizeof(who));
                }
                page_feed_picture(cursor, item_end, image, sizeof(image));
                {
                    /* The feed's own vocabulary: an audio or video enclosure
                     * plays; itunes and the Podcasting 2.0 namespace name the
                     * show's picture, people, place, transcript, chapters and
                     * where it is being discussed. */
                    char asset[1024] = "", type[64] = "", summary[2048] = "", people[256] = "", where[128] = "";
                    char transcript[512] = "", chapters[512] = "", talk[512] = "", duration[32] = "";
                    PageRetainedExtra extra = {summary, NULL, NULL, NULL, asset, NULL, people, where, transcript, chapters, talk, 0, NULL, type};
                    double lon = 0, lat = 0; int placed = 0;
                    if (page_tag_attr(cursor, item_end, "enclosure", "url", asset, sizeof(asset)) &&
                        (!page_tag_attr(cursor, item_end, "enclosure", "type", type, sizeof(type)) || (strncmp(type, "audio/", 6) && strncmp(type, "video/", 6))))
                        asset[0] = '\0';
                    if (!image[0]) page_tag_attr(cursor, item_end, "itunes:image", "href", image, sizeof(image));
                    if (!who[0]) rss_tag_text(cursor, item_end, "itunes:author", who, sizeof(who));
                    /* A story keeps the lead its publisher offered with it: the
                     * description, as text, cut near two sentences.  Every list
                     * that shows the story then says what it is about instead
                     * of only its title; the whole piece stays with them. */
                    if (!rss_tag_text(cursor, item_end, "itunes:summary", summary, sizeof(summary)))
                        rss_tag_text(cursor, item_end, "description", summary, sizeof(summary));
                    page_decode_entities(summary); page_strip_markup(summary); page_decode_entities(summary);
                    {
                        size_t limit = media == BF_PAGE_MATERIAL_PUBLICATION ? 360 : 600;
                        /* Many feeds open the description with the title again;
                         * the lead is what follows it, and a description that is
                         * only the title says nothing more. */
                        size_t tl = strlen(title);
                        if (tl && !strncasecmp(summary, title, tl)) {
                            const char *rest = summary + tl;
                            while (*rest == ' ' || *rest == '.' || *rest == ':' || *rest == '-') rest++;
                            memmove(summary, rest, strlen(rest) + 1);
                        }
                        if (strlen(summary) < 40) summary[0] = '\0';
                        if (strlen(summary) > limit) { size_t cut = limit; while (cut > limit * 2 / 3 && summary[cut] != ' ') cut--; snprintf(summary + cut, sizeof(summary) - cut, "%s", "\u2026"); }
                    }
                    page_tag_attr(cursor, item_end, "podcast:transcript", "url", transcript, sizeof(transcript));
                    page_tag_attr(cursor, item_end, "podcast:chapters", "url", chapters, sizeof(chapters));
                    page_tag_attr(cursor, item_end, "podcast:socialInteract", "uri", talk, sizeof(talk));
                    rss_tag_text(cursor, item_end, "podcast:location", where, sizeof(where));
                    {
                        const char *person = cursor; size_t used = 0; int count = 0;
                        while ((person = strstr(person, "<podcast:person")) && person < item_end && count < 6) {
                            char one[96];
                            if (rss_tag_text(person, item_end, "podcast:person", one, sizeof(one)) && !strstr(people, one) && used + strlen(one) + 3 < sizeof(people)) {
                                used += (size_t)snprintf(people + used, sizeof(people) - used, "%s%s", count ? ", " : "", one); count++;
                            }
                            person += 15;
                        }
                    }
                    if (rss_tag_text(cursor, item_end, "itunes:duration", duration, sizeof(duration))) {
                        int h = 0, m = 0, sec = 0;
                        if (sscanf(duration, "%d:%d:%d", &h, &m, &sec) == 3) extra.duration_ms = (h * 3600L + m * 60L + sec) * 1000L;
                        else if (sscanf(duration, "%d:%d", &m, &sec) == 2) extra.duration_ms = (m * 60L + sec) * 1000L;
                        else extra.duration_ms = atol(duration) * 1000L;
                    }
                    {
                        char geo[96];
                        if (page_tag_attr(cursor, item_end, "podcast:location", "geo", geo, sizeof(geo)) && !strncmp(geo, "geo:", 4) &&
                            sscanf(geo + 4, "%lf,%lf", &lat, &lon) == 2 && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat == 0 && lon == 0)) placed = 1;
                    }
                    read++;
                    page_push_retained(material, source, media, title, by, who, when, href, image, placed, lon, lat, &extra);
                }
            }
            cursor = item_end + strlen(close);
        }
    } else {
        char err[160]; bf_json_doc_t *doc = bf_json_parse_str(body, err, sizeof(err));
        const bf_json_node_t *items = doc ? page_walk(doc, bf_json_root(doc), extract) : NULL, *item;
        /* An extract that names one object (today's picture, today's
         * article) is a list of one. */
        int read = 0, single = items && items->type == BF_JSON_OBJECT;
        if (items && (items->type == BF_JSON_ARRAY || single))
            for (item = single ? items : bf_json_child_first(doc, items); item && read < page_per_source;
                 item = single ? NULL : bf_json_child_next(doc, item)) {
                char title[768], when[256], href[1024], who[2048], image[1024];
                char summary[2048], maker[256], rights[256], rights_href[1024], asset[1024], ident[256], about[256];
                PageRetainedExtra extra = {summary, maker, rights, rights_href, asset, ident, NULL, NULL, NULL, NULL, NULL, 0, about, NULL};
                double lon = 0, lat = 0; int placed;
                if (item->type != BF_JSON_OBJECT || page_item_flagged(doc, item)) continue;
                if (!page_field_text(doc, item, title_field, 0, title, sizeof(title))) continue;
                page_field_text(doc, item, meta_field, 0, when, sizeof(when));
                page_field_text(doc, item, link_field, 1, href, sizeof(href));
                if (link_base[0] && href[0] && !public_http_url(href)) {
                    /* A key the source gives in place of a link resolves the
                     * way a relative picture does. */
                    char resolved[1024]; const char *cut = strchr(link_base, '|');
                    int w = cut ? snprintf(resolved, sizeof(resolved), "%.*s%s%s", (int)(cut - link_base), link_base, href, cut + 1)
                                : snprintf(resolved, sizeof(resolved), "%s%s", link_base, href);
                    if (w > 0 && (size_t)w < sizeof(resolved) && public_http_url(resolved)) snprintf(href, sizeof(href), "%s", resolved);
                }
                page_field_text(doc, item, sub_field, 1, who, sizeof(who));
                page_image_url(doc, item, image_field, image_base, image, sizeof(image));
                page_field_text(doc, item, summary_field, 1, summary, sizeof(summary)); page_strip_markup(summary);
                page_field_text(doc, item, maker_field, 0, maker, sizeof(maker)); page_strip_markup(maker);
                page_field_text(doc, item, rights_field, 1, rights, sizeof(rights)); page_strip_markup(rights);
                page_field_text(doc, item, rights_link_field, 1, rights_href, sizeof(rights_href));
                page_field_text(doc, item, media_field, 1, asset, sizeof(asset));
                page_field_text(doc, item, ident_field, 1, ident, sizeof(ident));
                page_field_text(doc, item, about_field, 1, about, sizeof(about));
                placed = page_item_place(doc, item, place_field, &lon, &lat);
                page_record_id[0] = '\0';
                /* Numeric ids read as their text, like every other key. */
                if (key_field[0]) page_field_text(doc, item, key_field, 0, page_record_id, sizeof(page_record_id));
                page_push_retained(material, source, media, title, by, who, when, href, image, placed, lon, lat, &extra);
                page_record_id[0] = '\0';
                read++;
            }
        if (doc) bf_json_free(doc);
    }
    free(body);
}

/* A thing with no date has no place in a time order.  Rather than sinking
 * below everything dated, undated things are spread evenly through the
 * stream, so a film or an old recording is met at the same pace as the news. */
static void page_spread_undated(PageStream *stream) {
    static PageItem dated[PAGE_STREAM_MAX], undated[PAGE_STREAM_MAX];
    int k, d = 0, u = 0, n = 0, di = 0, ui = 0, every;
    for (k = 0; k < stream->count; k++) {
        if (stream->item[k].at) dated[d++] = stream->item[k];
        else undated[u++] = stream->item[k];
    }
    if (!u) return;
    {
        /* Undated things take turns by source, so a second archive is met
         * before the first has been read to its end. */
        static PageItem turned[PAGE_STREAM_MAX];
        int rank[PAGE_STREAM_MAX], turn, placed = 0, j, m;
        for (k = 0; k < u; k++) {
            rank[k] = 0;
            for (j = 0; j < k; j++) if (!strcmp(undated[j].source, undated[k].source)) rank[k]++;
        }
        for (turn = 0; placed < u; turn++)
            for (k = 0; k < u; k++) if (rank[k] == turn) turned[placed++] = undated[k];
        for (m = 0; m < u; m++) undated[m] = turned[m];
    }
    if (!d) { for (k = 0; k < u; k++) stream->item[k] = undated[k]; return; }
    every = d / u > 1 ? d / u : 1;
    while (di < d || ui < u) {
        for (k = 0; k < every && di < d; k++) stream->item[n++] = dated[di++];
        if (ui < u) stream->item[n++] = undated[ui++];
        if (di >= d) while (ui < u) stream->item[n++] = undated[ui++];
    }
}

/* Sooner first, and an event with no readable time last rather than guessed. */
static int page_soonest_first(const void *a, const void *b) {
    const PageItem *x = a, *y = b;
    if ((x->at <= 0) != (y->at <= 0)) return x->at <= 0 ? 1 : -1;
    if (x->at != y->at) return x->at < y->at ? -1 : 1;
    return x->order < y->order ? -1 : x->order > y->order;
}

static void page_finalize_material(PageMaterial *material) {
    int k;
    if (!material) return;
    for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) {
        if (k == BF_PAGE_MATERIAL_EVENT) continue;
        qsort(material->stream[k].item, (size_t)material->stream[k].count,
              sizeof(material->stream[k].item[0]), page_newest_first);
        page_spread_undated(&material->stream[k]);
    }
    {
        PageStream *events = &material->stream[BF_PAGE_MATERIAL_EVENT];
        time_t now = time(NULL);
        int kept = 0;
        for (k = 0; k < events->count; k++) {
            const PageItem *item = &events->item[k];
            time_t ends = item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : item->at + 86400;
            /* An agenda is a claim about what is going to happen. An event its
             * source has stopped publishing is a thing this estate read once;
             * it keeps its own page, where it says so, and it does not go on
             * being advertised as upcoming from here. */
            if (item->archived) continue;
            if (ends >= now) events->item[kept++] = events->item[k];
        }
        events->count = kept;
        qsort(events->item, (size_t)events->count, sizeof(events->item[0]), page_soonest_first);
    }
}

/* The item archive is the durable reading record.  Public programs still
 * prefer a source's current retained body because it carries richer media
 * details, but a body that moved, expired or no longer parses must not erase
 * an item this estate demonstrably retained from that source. */
static sqlite3 *page_archive_open(int writable);
static int page_collect_archive_source(const char *source, PageMaterial *material);

static int page_material_count(const PageMaterial *material) {
    int kind, count = 0;
    if (!material) return 0;
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) count += material->stream[kind].count;
    return count;
}

/* A source can have enough current body shape to produce a row while still
 * lacking the stable key that lets a reader return to it.  That row is useful
 * context, but it does not satisfy a public source window: the archive is the
 * durable record that can supply the missing pageable item. */
static int page_material_has_page(const PageMaterial *material) {
    int kind, at;
    if (!material) return 0;
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++)
        for (at = 0; at < material->stream[kind].count; at++)
            if (material->stream[kind].item[at].source[0] && material->stream[kind].item[at].key[0]) return 1;
    return 0;
}

/* The same story reached through a second carrier is not a second story.
 *
 * page_push_story already refuses a repeat inside one stream, but each source
 * is read into a material of its own and merged here, so two carriers of the
 * same publisher's story arrive as two streams that have never met.  Left
 * alone, a page leads with a story and then reports it again further down, and
 * every region below inherits the miscount.
 *
 * What the second copy is good for is what the first one lacks: one carrier
 * often has the picture and the other the dateline.  So the repeat is folded
 * into the copy already held rather than dropped outright. */
static PageItem *page_same_item(PageStream *stream, const PageItem *other) {
    int at;
    for (at = 0; at < stream->count; at++) {
        PageItem *held = &stream->item[at];
        /* A shared headline is not an item identity. Independent publishers
         * and distinct episodes often choose exactly the same words. */
        if (held->key[0] && other->key[0] && !strcmp(held->source, other->source)) {
            if (!strcmp(held->key, other->key)) return held;
            continue;
        }
        if (other->href[0] && held->href[0] && !strcmp(held->href, other->href)) return held;
        if (!held->href[0] && !other->href[0] && held->source[0] &&
            !strcmp(held->source, other->source) && !strcmp(held->title, other->title)) return held;
    }
    return NULL;
}

static void page_fold_item(PageItem *held, const PageItem *other) {
    if (!held->href[0] && other->href[0]) snprintf(held->href, sizeof(held->href), "%s", other->href);
    if (!held->image[0] && other->image[0]) snprintf(held->image, sizeof(held->image), "%s", other->image);
    if (!held->body[0] && other->body[0]) snprintf(held->body, sizeof(held->body), "%s", other->body);
    if (!held->by[0] && other->by[0]) snprintf(held->by, sizeof(held->by), "%s", other->by);
    /* A carrier that gave no time should not make an undated story of one that
     * was dated, and a page that can say when is better than one that cannot. */
    if (!held->at && other->at) held->at = other->at;
    /* If either carrier still publishes it, it is not merely kept. */
    if (!other->archived) held->archived = 0;
    /* A copy that has a page of its own is the one worth pointing at. */
    if (!(held->source[0] && held->key[0]) && other->source[0] && other->key[0]) {
        snprintf(held->source, sizeof(held->source), "%s", other->source);
        snprintf(held->key, sizeof(held->key), "%s", other->key);
    }
}

static void page_merge_material(PageMaterial *into, const PageMaterial *from) {
    int kind, at;
    if (!into || !from) return;
    for (kind = 0; kind < BF_PAGE_MATERIAL_COUNT; kind++) {
        PageStream *target = &into->stream[kind];
        const PageStream *source = &from->stream[kind];
        for (at = 0; at < source->count && target->count < PAGE_STREAM_MAX; at++) {
            PageItem *held = page_same_item(target, &source->item[at]);
            if (held) { page_fold_item(held, &source->item[at]); continue; }
            target->item[target->count++] = source->item[at];
        }
    }
}

static void page_collect_stories(sqlite3 *db, const char *sources, PageMaterial *material) {
    char list[4096], *p; int k, count = 0;
    static const BfPageMaterial mixed[] = {BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_PHOTO, BF_PAGE_MATERIAL_AUDIO};
    size_t m;
    PageMaterial *one;
    if (!(one = calloc(1, sizeof(*one)))) return;
    snprintf(list, sizeof(list), "%s", sources ? sources : "");
    for (p = list; *p; p++) if (*p == ',') count++;
    count += list[0] ? 1 : 0;
    /* Every source gets a fair share of the page instead of the first few
     * filling it; a single source may use the whole page.
     *
     * Unless the caller is not composing a page at all.  The archive reads a
     * source for everything it published, not for what one page can show, and
     * clamping that to a page's share was throwing away most of every feed
     * that carries more than forty items. */
    if (page_read_everything) {
        page_per_source = PAGE_STREAM_MAX;
    } else {
        page_per_source = count ? PAGE_STREAM_MAX / count : 24;
        /* The share may be smaller than six.  At a 96-source program length,
         * a floor of six let the first 54 readers fill a 320-item stream and
         * silently erased the rest of the selected page.  One is enough to
         * give every selected publisher a turn; a sparse source simply leaves
         * its unused share for the stream rather than taking another's turn. */
        if (page_per_source < 1) page_per_source = 1;
        if (page_per_source > 40) page_per_source = 40;
    }
    for (p = list; *p;) {
        char *start = p, *end;
        while (*p && *p != ',') p++;
        if (*p) *p++ = '\0';
        while (*start && isspace((unsigned char)*start)) start++;
        end = start + strlen(start);
        while (end > start && isspace((unsigned char)end[-1])) *--end = '\0';
        if (!*start) continue;
        memset(one, 0, sizeof(*one));
        if (source_has_publication_occurrences(db, start)) page_collect_occurrences(db, start, one);
        else page_collect_retained(db, start, one);
        if (page_material_count(one)) {
            page_merge_material(material, one);
            /* Keep current rich fields when they lowered, but add the
             * retained record when none of those rows has a stable page. */
            if (!page_material_has_page(one)) page_collect_archive_source(start, material);
        } else page_collect_archive_source(start, material);
    }
    /* The mix is every retained story, video, picture and recording in one time order. */
    for (m = 0; m < sizeof(mixed) / sizeof(mixed[0]); m++) {
        PageStream *from = &material->stream[mixed[m]], *mix = &material->stream[BF_PAGE_MATERIAL_MIX];
        for (k = 0; k < from->count && mix->count < PAGE_STREAM_MAX; k++)
            if (from->item[k].key[0]) mix->item[mix->count++] = from->item[k];
    }
    page_finalize_material(material);
    free(one);
}

/* A World fragment is the published edge from this page to a subject another
 * owner holds.  The address scheme selects that owner's existing reader;
 * nothing here queries media, music or conversation directly. */
static void page_read_subject(sqlite3 *db, BfPageMaterial material, const char *id,
                              SiteMaterialStream *read, PageStream *stream) {
    int i, ok = 0;
    memset(read, 0, sizeof(*read));
    switch (material) {
    case BF_PAGE_MATERIAL_POST: ok = site_material_community(db, id, SITE_MATERIAL_MAX, read); break;
    case BF_PAGE_MATERIAL_AUDIO: ok = site_material_recording(db, id, read); break;
    case BF_PAGE_MATERIAL_VIDEO: ok = site_material_media_program(db, id, read); break;
    default: return;
    }
    if (!ok) return;
    for (i = 0; i < read->count; i++) {
        const SiteMaterialItem *from = &read->item[i];
        const char *title = from->title[0] ? from->title : from->body;
        PageItem *item;
        if (material != BF_PAGE_MATERIAL_POST && !listen_safe_asset(from->asset)) continue;
        if (!(item = page_push(stream, title))) return;
        if (material == BF_PAGE_MATERIAL_VIDEO) page_humanize(from->title, item->title, sizeof(item->title));
        if (material == BF_PAGE_MATERIAL_POST) {
            if (!from->title[0]) item->title[0] = '\0';
            snprintf(item->body, sizeof(item->body), "%s", from->body);
            snprintf(item->group, sizeof(item->group), "%s", read->title);
            if (strncmp(read->metric, "0 ", 2)) snprintf(item->note, sizeof(item->note), "%s", read->metric);
        }
        page_humanize(from->byline, item->by, sizeof(item->by));
        snprintf(item->asset, sizeof(item->asset), "%s", from->asset);
        if (listen_safe_asset(from->poster)) snprintf(item->poster, sizeof(item->poster), "%s", from->poster);
        item->at = page_time(from->when);
        item->duration_ms = from->duration_ms;
    }
}

static void page_collect_subjects(sqlite3 *db, const char *world, PageMaterial *material) {
    sqlite3_stmt *st = NULL; SiteMaterialStream *read = NULL;
    char seen[32][256]; int seen_count = 0;
    if (!cms_type_registered(db, "world_fragment") || !(read = malloc(sizeof(*read))) ||
        sqlite3_prepare_v2(db, "SELECT subject_address FROM world_fragment WHERE world=?1 AND status='published' "
          "ORDER BY ordinal,id", -1, &st, NULL) != SQLITE_OK) { free(read); return; }
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *address = (const char *)sqlite3_column_text(st, 0), *colon;
        size_t i; int k, repeat = 0;
        if (!address || strlen(address) >= sizeof(seen[0]) || !(colon = strchr(address, ':')) || !colon[1]) continue;
        for (k = 0; k < seen_count; k++) if (!strcmp(seen[k], address)) repeat = 1;
        if (repeat || seen_count >= 32) continue;
        snprintf(seen[seen_count++], sizeof(seen[0]), "%s", address);
        for (i = 0; i < BF_PAGE_SUBJECT_SCHEME_COUNT; i++) {
            const BfPageSubjectScheme *scheme = &bf_page_subject_schemes[i];
            if (strlen(scheme->scheme) == (size_t)(colon - address) && !strncmp(address, scheme->scheme, (size_t)(colon - address)))
                page_read_subject(db, scheme->material, colon + 1, read, &material->stream[scheme->material]);
        }
    }
    sqlite3_finalize(st); free(read);
}

static const char *page_grammar_label(const char *grammar) {
    size_t i;
    for (i = 0; grammar && i < BF_PAGE_GRAMMAR_LABEL_COUNT; i++)
        if (!strcmp(grammar, bf_page_grammar_labels[i].grammar)) return bf_page_grammar_labels[i].label;
    return NULL;
}

/* ------------------------------------------------------------- page links
 *
 * Every item a composed page shows opens somewhere real.  A retained item has
 * its own Aurekai page (/o/<source>/<key>), where its verbs live; an item from
 * an owner without one links to where it was published; an item with neither
 * is text, never an anchor that goes nowhere. */
static void page_url_escape(const char *in) {
    static const char hex[] = "0123456789ABCDEF";
    const unsigned char *p = (const unsigned char *)(in ? in : "");
    for (; *p; p++) {
        if (isalnum(*p) || *p == '-' || *p == '_' || *p == '.' || *p == '~') putchar(*p);
        else { putchar('%'); putchar(hex[*p >> 4]); putchar(hex[*p & 15]); }
    }
}

static int page_csv_has(const char *csv, const char *item);

static int page_item_has_page(const PageItem *item) {
    return item && item->source[0] && item->key[0];
}

#include "estate_reference.h"
#include "item_depth.h"


/* Opens an item's link and returns the closing tag to write after it. */
static const char *page_emit_item_open(const PageItem *item, const char *class_name) {
    if (page_item_has_page(item)) {
        fputs("<a", stdout);
        if (class_name) { fputs(" class=\"", stdout); html(class_name); fputs("\"", stdout); }
        fputs(" href=\"/o/", stdout); page_url_escape(item->source); fputs("/", stdout); html(item->key); fputs("\">", stdout);
        return "</a>";
    }
    if (item && estate_address_is_ours(item->href)) {
        fputs("<a", stdout);
        if (class_name) { fputs(" class=\"", stdout); html(class_name); fputs("\"", stdout); }
        fputs(" href=\"", stdout); html(item->href); fputs("\">", stdout);
        return "</a>";
    }
    if (item && public_http_url(item->href)) {
        fputs("<a", stdout);
        if (class_name) { fputs(" class=\"", stdout); html(class_name); fputs("\"", stdout); }
        fputs(" rel=\"noopener\" target=\"_blank\" href=\"", stdout); html(item->href); fputs("\">", stdout);
        return "</a>";
    }
    fputs("<span", stdout);
    if (class_name) { fputs(" class=\"", stdout); html(class_name); fputs("\"", stdout); }
    fputs(">", stdout);
    return "</span>";
}

static void page_emit_by(const PageItem *item, time_t now) {
    char when[48];
    page_when(item->at, now, when, sizeof(when));
    if (item->by[0]) { fputs("<span>", stdout); html(item->by); fputs("</span>", stdout); }
    if (item->note[0] && strcmp(item->note, item->by) && !item->group[0]) { fputs(" <span class=\"via\">", stdout); html(item->note); fputs("</span>", stdout); }
    if (when[0]) {
        char iso[32]; struct tm tm;
        if (gmtime_r(&item->at, &tm) && strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tm)) {
            fputs(" <time datetime=\"", stdout); html(iso); fputs("\">", stdout); html(when); fputs("</time>", stdout);
        }
    }
}
/* One retained item, multiple lawful continuations. These are navigation, not
 * installation or attribution claims. App destinations recheck sign-in, the
 * selected account, installation and the exact item before any effect. */
static void page_emit_item_continuations(const PageItem *item) {
    if (!page_item_has_page(item)) return;
    fputs("<nav class=\"page-item-continuations\" aria-label=\"Retained item continuations\">",stdout);
    fputs("<a data-item-detail href=\"/o/",stdout); page_url_escape(item->source);
    fputs("/",stdout); page_url_escape(item->key); fputs("\">Read item</a>",stdout);
    if (item->by[0]) {
        fputs("<a data-item-credit href=\"/people?who=",stdout); page_url_escape(item->by);
        fputs("&amp;source=",stdout); page_url_escape(item->source);
        fputs("\">Credited maker</a>",stdout);
    }
    fputs("<a data-item-keep href=\"/o/",stdout);page_url_escape(item->source);
    fputs("/",stdout);page_url_escape(item->key);fputs("#item-keep\">Keep for later</a>",stdout);
    fputs("<a data-item-continue href=\"/feed-use?context=self&amp;item=",stdout);
    page_url_escape(item->source); fputs("%2F",stdout); page_url_escape(item->key);
    fputs("\">Use in my apps</a></nav>",stdout);
}
static void page_emit_continuation_style(void) {
    fputs("<style>.page-item-continuations{display:flex;flex-wrap:wrap;gap:8px 14px;align-items:center;margin:10px 0 4px;font:600 12px/1.45 system-ui}"
          ".page-item-continuations a{min-height:36px;display:inline-flex;align-items:center;color:var(--w-accent,#d0fc4f);text-underline-offset:3px}"
          ".page-item-continuations a:focus-visible{outline:2px solid currentColor;outline-offset:3px}"
          "@media(max-width:390px){.page-item-continuations{gap:4px 12px}.page-item-continuations a{min-height:44px}}</style>",stdout);
}

static void page_emit_picture(const char *url, const char *class_name) {
    if (!page_image_url_safe(url)) return;
    fputs("<img class=\"", stdout); html(class_name); fputs("\" src=\"", stdout); html(url);
    fputs("\" alt=\"\" loading=\"lazy\" decoding=\"async\" referrerpolicy=\"no-referrer\">", stdout);
}

static void page_emit_duration(long ms) {
    long s = ms / 1000;
    if (s <= 0) return;
    if (s >= 3600) printf("%ld:%02ld:%02ld", s / 3600, (s / 60) % 60, s % 60);
    else printf("%ld:%02ld", s / 60, s % 60);
}

/* One semantic glyph vocabulary for nav, pages, account chrome and generic
 * ObjectContract actions. The table reuses the exact Tabler geometry above. */
static void page_glyph(const char *name) {
    static const struct { const char *name, *svg; } glyphs[] = {
        {"play",BF_ICON_PLAY},{"out",BF_ICON_EXTERNAL},{"save",BF_ICON_BOOKMARK},{"share",BF_ICON_SHARE},
        {"pin",BF_ICON_PIN},{"list",BF_ICON_LIST},{"picture",BF_ICON_PHOTO},{"text",BF_ICON_FILE_TEXT},
        {"search",BF_ICON_SEARCH},{"pause",BF_ICON_PAUSE},{"back",BF_ICON_PREV},{"next",BF_ICON_NEXT},
        {"world",BF_ICON_WORLD},{"life",BF_ICON_HEART},{"shop",BF_ICON_SHOP},{"work",BF_ICON_BRIEFCASE},{"briefcase",BF_ICON_BRIEFCASE},
        {"discover",BF_ICON_COMPASS},{"calendar",BF_ICON_CALENDAR},{"listen",BF_ICON_HEADPHONES},{"board",BF_ICON_BOARD},
        {"needs",BF_ICON_LIST_CHECK},{"list-check",BF_ICON_LIST_CHECK},{"cost",BF_ICON_COINS},{"coins",BF_ICON_COINS},{"examples",BF_ICON_SPARKLES},{"guides",BF_ICON_BOOK},{"book",BF_ICON_BOOK},
        {"user",BF_ICON_USER},{"login",BF_ICON_LOGIN},{"settings",BF_ICON_SETTINGS},{"apps",BF_ICON_APPS},
        {"bell",BF_ICON_BELL},{"robot",BF_ICON_ROBOT},{"logout",BF_ICON_LOGOUT},{"link",BF_ICON_LINK},
        {"group",BF_ICON_GROUP},{"public",BF_ICON_WORLD_WWW},{"tool",BF_ICON_TOOL},{"file-check",BF_ICON_FILE_CHECK},
        {"warning",BF_ICON_WARNING},{"clock",BF_ICON_CLOCK},{"activity",BF_ICON_ACTIVITY},{"plus",BF_ICON_PLUS},
        {"edit",BF_ICON_EDIT},{"eye",BF_ICON_EYE},{"receipt",BF_ICON_RECEIPT},{"check",BF_ICON_CHECK},
        {"rss",BF_ICON_RSS},{"chef",BF_ICON_CHEF},{"shirt",BF_ICON_SHIRT},{"tag",BF_ICON_TAG},{"store",BF_ICON_STORE},{"mic",BF_ICON_MIC},
        {"relationship",BF_ICON_GROUP},{"source-library",BF_ICON_BOOK},{"research",BF_ICON_SEARCH},
        {"narration",BF_ICON_MIC},{"finance",BF_ICON_COINS},{"feed",BF_ICON_RSS},
    };
    size_t i;
    if (!name || !*name) return;
    for (i = 0; i < sizeof(glyphs) / sizeof(glyphs[0]); i++)
        if (!strcmp(name, glyphs[i].name)) { fputs(glyphs[i].svg, stdout); return; }
}
#include "page_media_spaces.h"

static const char *page_nav_glyph(const char *route) {
    if(!route)return "discover";
    /* Shared semantic routing: child views inherit their nearest declared parent. */
    static const struct {const char *route,*glyph;} routes[]={
        {"/work-board/outputs","receipt"},{"/work-board","board"},
        {"/s/prices/observed","cost"},{"/s/work/all","needs"},{"/s/economics","cost"},
        {"/foreign/workbench","link"},
        {"/feed-use","feed"},{"/relationships","relationship"},{"/inbox","bell"},
        {"/apps","apps"},{"/discover","discover"},{"/whats-on","calendar"},
        {"/watch","play"},{"/listen","listen"},{"/board","board"},{"/examples","examples"},
        {"/people","group"},{"/profile","user"},{"/saved","save"},{"/collections","save"},
        {"/sources","rss"},{"/channels","rss"},{"/link","link"},{"/connect","link"},
        {"/packs","apps"},{"/programs","board"},{"/agents","robot"},
        {"/stage","play"},{"/community","group"},{"/listings","shop"},
        {"/market","shop"},{"/parcels","pin"},{"/property","pin"},
        {"/habitat","life"},{"/township","pin"},{"/territory","world"},
        {"/console","settings"},{"/studio","tool"},{"/search","search"},
        {"/guides","guides"},{"/learn","guides"},{"/finance","cost"},
        {"/pricing","cost"},{"/receipts","receipt"}
    };
    for(size_t i=0;i<sizeof(routes)/sizeof(routes[0]);i++){
        size_t length=strlen(routes[i].route);
        if(!strcmp(route,routes[i].route)||(strncmp(route,routes[i].route,length)==0&&route[length]=='/'))return routes[i].glyph;
    }
    return "discover";
}

static const char *page_operation_glyph(const char *operation) {
    const char *verb = operation ? strrchr(operation,'.') : NULL;
    verb = verb ? verb + 1 : operation;
    /* ObjectContract operations are typed semantics, not a product-name icon list. */
    static const struct {const char *operation,*glyph;} cuts[]={
        {"feed-intake","feed"},{"relationship.observe","relationship"},
        {"source.save","source-library"},{"research.plan","research"},
        {"narration.create","narration"},{"finance.review","finance"}
    };
    if(operation)for(size_t i=0;i<sizeof(cuts)/sizeof(cuts[0]);i++)
        if(!strcmp(operation,cuts[i].operation))return cuts[i].glyph;
    if (!verb) return "activity";
    if (!strcmp(verb,"support")) return "life";
    if (!strcmp(verb,"join") || !strcmp(verb,"connect") || !strcmp(verb,"participate")) return "group";
    if (!strcmp(verb,"vote") || !strcmp(verb,"approve") || !strcmp(verb,"complete")) return "check";
    if (!strcmp(verb,"follow")) return "calendar";
    if (!strcmp(verb,"save")) return "save";
    if (!strcmp(verb,"run") || !strcmp(verb,"play")) return "play";
    if (!strcmp(verb,"publish") || !strcmp(verb,"share")) return "share";
    if (!strcmp(verb,"view") || !strncmp(verb,"view",4) || !strcmp(verb,"observe")) return "eye";
    if (!strcmp(verb,"edit") || !strcmp(verb,"update")) return "edit";
    if (!strcmp(verb,"offer") || !strcmp(verb,"settle") || !strcmp(verb,"price")) return "cost";
    if (!strcmp(verb,"create") || !strcmp(verb,"propose") || !strcmp(verb,"open")) return "plus";
    return "activity";
}
typedef struct {
    const char *route;      /* the route this page answers, for filter links */
    const char *stream;     /* a selected stream on a two-pane page */
    const char *query;      /* what the person asked of this route, if anything */
    int preview;            /* a front-page section shows a short preview */
    time_t now;
    /* The census this page is being composed from, so a region can ask for a
     * presentation profile of its own.  It lives exactly as long as the
     * request that opened it: never hold it anywhere that outlives one. */
    sqlite3 *db;
    PageCompatScene *surface_scene;
    const char *experience_pattern; /* exact route or canonical subject owner */
} PageEmitContext;

/* One deck under a track list.  It plays the files the rows already name,
 * in the order the page put them in, and stops when the list ends: a person
 * who wanted to listen should not have to press play forty times.  It fetches
 * nothing itself -- the browser reads each file from the source that published
 * it -- it starts only when a person asks, it remembers nothing between pages,
 * and where a row has no file of its own the row still links to the page its
 * host plays it on. */
static void page_emit_deck(void) {
    fputs(bf_public_audio_deck, stdout);
}

static int page_is_video(const PageItem *item) { return item->kind == BF_PAGE_MATERIAL_VIDEO; }
static int page_is_photo(const PageItem *item) { return item->kind == BF_PAGE_MATERIAL_PHOTO; }

/* Carry only reading coordinates. A link cannot carry authority, source
 * membership, or cache-control inputs into the next view. */
static void page_emit_reading_query(const char *query, const char *replace, const char *value) {
    static const char *keys[] = {"q", "from", "s"};
    size_t i; int started = 0;
    for (i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        char current[256] = {0};
        const char *chosen;
        query_value(query, keys[i], current, sizeof(current));
        chosen = replace && !strcmp(replace, keys[i]) ? value : current;
        if (!chosen || !*chosen) continue;
        fputs(started ? "&amp;" : "?", stdout); started = 1;
        fputs(keys[i], stdout); fputc('=', stdout); page_url_escape(chosen);
    }
}

static void page_emit_filter_link(const PageEmitContext *ctx, const char *key, const char *value) {
    fputs("<a href=\"", stdout); html(ctx && ctx->route ? ctx->route : "/discover");
    page_emit_reading_query(ctx ? ctx->query : NULL, key, value); fputs("\">", stdout);
}

/* Web Mercator pixels at a zoom level: the projection street map tiles use. */
static void page_mercator(double lon, double lat, int zoom, double *x, double *y) {
    const double pi = 3.14159265358979323846;
    double scale = 256.0 * (double)(1L << zoom), rad;
    if (lat > 85.0511) lat = 85.0511;
    if (lat < -85.0511) lat = -85.0511;
    rad = lat * pi / 180.0;
    *x = (lon + 180.0) / 360.0 * scale;
    *y = (1.0 - log(tan(rad) + 1.0 / cos(rad)) / pi) / 2.0 * scale;
}

static void page_emit_map_plate(const PageStream *stream, int take, int numbered) {
    int line, k, placed = 0;
    double west = 180, east = -180, south = 90, north = -90;
    for (k = 0; k < take; k++) {
        const PageItem *item = &stream->item[stream->cursor + k];
        if (!item->placed) continue;
        placed++;
        if (item->lon < west) west = item->lon;
        if (item->lon > east) east = item->lon;
        if (item->lat < south) south = item->lat;
        if (item->lat > north) north = item->lat;
    }
    if (placed && east - west <= 12.0 && north - south <= 8.0) {
        /* Places close together are drawn on the streets they are on, from
         * OpenStreetMap's tiles, which carry their own credit.  The zoom is
         * the closest at which every point fits. */
        const double width = 960.0, height = 480.0;
        double wx, ny, ex, sy, x0, y0, scale = 1.0;
        int zoom, tiles, tx, ty, shown = 0;
        for (zoom = 15; zoom > 3; zoom--) {
            page_mercator(west, north, zoom, &wx, &ny);
            page_mercator(east, south, zoom, &ex, &sy);
            if (ex - wx <= width * 0.82 && sy - ny <= height * 0.78) break;
        }
        page_mercator(west, north, zoom, &wx, &ny);
        page_mercator(east, south, zoom, &ex, &sy);
        {
            /* The view is cropped to the places with a margin, never tighter
             * than half a plate, so tiles are enlarged at most twice. */
            double view_w = (ex - wx) * 1.25, view_h = (sy - ny) * 1.3;
            if (view_w < view_h * 2.0) view_w = view_h * 2.0;
            if (view_w < width / 2.0) view_w = width / 2.0;
            if (view_w > width) view_w = width;
            view_h = view_w / 2.0;
            x0 = (wx + ex) / 2.0 - view_w / 2.0;
            y0 = (ny + sy) / 2.0 - view_h / 2.0;
            scale = width / view_w;
        }
        tiles = 1 << zoom;
        fputs("<div class=\"plate streets\"><svg viewBox=\"0 0 960 480\" role=\"img\" aria-label=\"Street map of these places\">", stdout);
        printf("<g transform=\"scale(%.4f)\">", scale);
        for (ty = (int)floor(y0 / 256.0); ty * 256.0 < y0 + height / scale; ty++) {
            if (ty < 0 || ty >= tiles) continue;
            for (tx = (int)floor(x0 / 256.0); tx * 256.0 < x0 + width / scale; tx++)
                printf("<image href=\"https://tile.openstreetmap.org/%d/%d/%d.png\" x=\"%.1f\" y=\"%.1f\" width=\"256\" height=\"256\"/>",
                       zoom, ((tx % tiles) + tiles) % tiles, ty, tx * 256.0 - x0, ty * 256.0 - y0);
        }
        fputs("</g>", stdout);
        /* Drawn last-first, so the first place sits on top. */
        for (k = take - 1; k >= 0; k--) {
            const PageItem *item = &stream->item[stream->cursor + k];
            double px, py;
            if (!item->placed) continue;
            page_mercator(item->lon, item->lat, zoom, &px, &py);
            px = (px - x0) * scale; py = (py - y0) * scale;
            if (page_item_has_page(item)) { fputs("<a href=\"/o/", stdout); page_url_escape(item->source); fputs("/", stdout); html(item->key); fputs("\">", stdout); }
            printf("<g class=\"point%s\" data-map-point=\"%d\"><circle cx=\"%.1f\" cy=\"%.1f\" r=\"%s\"/>", k == 0 ? " newest" : "", k, px, py, numbered ? "11" : "20");
            if (numbered) printf("<text x=\"%.1f\" y=\"%.1f\">%d</text>", px, py + 4.0, k + 1);
            fputs("<title>", stdout); html(item->title); fputs("</title></g>", stdout);
            if (page_item_has_page(item)) fputs("</a>", stdout);
            shown++;
        }
        (void)shown;
        fputs("</svg><small class=\"map-credit\">Map &copy; <a href=\"https://www.openstreetmap.org/copyright\" rel=\"noopener\" target=\"_blank\">OpenStreetMap contributors</a></small></div>", stdout);
        return;
    }
    fputs("<div class=\"plate\"><svg viewBox=\"0 0 360 180\" role=\"img\" aria-label=\"Map of reported places\">", stdout);
    for (line = 30; line < 360; line += 30) printf("<line class=\"grid\" x1=\"%d\" y1=\"0\" x2=\"%d\" y2=\"180\"/>", line, line);
    for (line = 30; line < 180; line += 30) printf("<line class=\"grid%s\" x1=\"0\" y1=\"%d\" x2=\"360\" y2=\"%d\"/>", line == 90 ? " equator" : "", line, line);
    for (k = 0; k < take; k++) {
        const PageItem *item = &stream->item[stream->cursor + k];
        if (!item->placed) continue;
        if (page_item_has_page(item)) { fputs("<a href=\"/o/", stdout); page_url_escape(item->source); fputs("/", stdout); html(item->key); fputs("\">", stdout); }
        printf("<circle class=\"point%s\" data-map-point=\"%d\" cx=\"%.2f\" cy=\"%.2f\" r=\"%s\"><title>", k == 0 ? " newest" : "", k, item->lon + 180.0, 90.0 - item->lat, k == 0 ? "3" : "1.9");
        if (numbered) printf("%d. ", k + 1);
        html(item->title); fputs("</title></circle>", stdout);
        if (page_item_has_page(item)) fputs("</a>", stdout);
    }
    fputs("</svg></div>", stdout);
}

/* One page offers one place to start listening, however many listening rhythms
 * it carries: two players at the top of one page is two invitations. */
static int page_offered_a_start = 0;

static void page_emit_region(const BfPageRegion *region, const PageStream *stream, int take, const PageEmitContext *ctx) {
    int k; const char *group = NULL, *close;
    time_t now = ctx->now;
    const PageItem *first = &stream->item[stream->cursor];
    switch (region->topology) {
    case BF_PAGE_TOPOLOGY_LEAD:
        fputs(first->image[0] ? "<article class=\"lead with-picture\">" : "<article class=\"lead\">", stdout);
        close = page_emit_item_open(first, NULL);
        { char sized[1024]; page_picture_at(first->image, 960, sized, sizeof(sized)); page_emit_picture(sized, "lead-picture"); }
        fputs("<h2>", stdout); html(first->title); fputs("</h2>", stdout); fputs(close, stdout);
        if (first->body[0]) { fputs("<p class=\"dek\">", stdout); html(first->body); fputs("</p>", stdout); }
        fputs("<p class=\"by\">", stdout); page_emit_by(first, now); fputs("</p>",stdout); page_emit_item_continuations(first); fputs("</article>",stdout);
        break;
    case BF_PAGE_TOPOLOGY_INDEX:
        fputs("<div class=\"index\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            close = page_emit_item_open(item, NULL);
            fputs("<b>", stdout); html(item->title); fputs("</b>", stdout);
            if (item->body[0]) { fputs("<span class=\"dek\">", stdout); html(item->body); fputs("</span>", stdout); }
            fputs("<span class=\"by\">", stdout); page_emit_by(item, now); fputs("</span>", stdout);
            page_emit_picture(item->image, "thumb");
            fputs(close, stdout);
        }
        fputs("</div>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_AGENDA:
        /* An agenda is read forwards and grouped by the day something happens,
         * with the place under it: the rhythm of a printed listings page. */
        fputs("<style>.agenda-tools{display:grid;grid-template-columns:auto minmax(190px,1fr) auto;align-items:end;gap:12px 18px;padding:21px 24px;margin:18px 0 18px;border:1px solid var(--w-rule,#444935);border-radius:17px;background:var(--w-panel,#171d19);color:var(--w-ink,#dee4dd);font:14px/1.45 system-ui}.agenda-tools .agenda-choices{display:flex;flex-wrap:wrap;gap:7px}.agenda-tools button{min-height:44px;padding:9px 16px;border:1px solid var(--w-rule,#444935);border-radius:999px;color:inherit;background:transparent;font:600 13px/1.2 system-ui;cursor:pointer}.agenda-tools button[aria-pressed=true]{background:var(--w-accent,#d0fc4f);color:var(--w-ground,#0f1511);border-color:var(--w-accent,#d0fc4f)}.agenda-tools label{display:grid;gap:6px;font:600 12px/1.2 system-ui;min-width:0}.agenda-tools input{width:100%;min-width:0;min-height:44px;padding:9px 12px;border:1px solid var(--w-rule,#444935);background:var(--w-ground,#0f1511);color:var(--w-ink,#dee4dd);border-radius:8px}.agenda-tools output{font:12px/1.4 system-ui}.agenda-tools small{grid-column:1/-1;color:var(--w-muted,#c5c9af)}ol.agenda li[hidden]{display:none!important}.agenda-tools :is(button,input):focus-visible{outline:2px solid var(--w-accent,#d0fc4f);outline-offset:3px}@media(max-width:760px){.agenda-tools{grid-template-columns:1fr 1fr;padding:17px}.agenda-tools .agenda-choices{grid-column:1/-1}.agenda-tools label{grid-column:1/-1}}@media(max-width:530px){.agenda-tools{grid-template-columns:1fr}.agenda-tools output{grid-column:1/-1}}@media(prefers-reduced-motion:reduce){.agenda-tools *{transition:none!important}}</style><div class=\"agenda-tools\" data-agenda-controls role=\"group\" aria-label=\"Filter published upcoming events\"><div class=\"agenda-choices\"><button type=\"button\" data-agenda-filter=\"all\" aria-pressed=\"true\">All dates</button><button type=\"button\" data-agenda-filter=\"today\" aria-pressed=\"false\">Starts today</button><button type=\"button\" data-agenda-filter=\"seven\" aria-pressed=\"false\">Next 7 days</button><button type=\"button\" data-agenda-filter=\"weekend\" aria-pressed=\"false\">This weekend</button></div><label>Find event or venue<input type=\"search\" data-agenda-search maxlength=\"120\" placeholder=\"Find in this agenda\"></label><output data-agenda-count role=\"status\" aria-live=\"polite\"></output><small>Filters only the published events loaded in this agenda. Open an event for its source details.</small></div>",stdout);
        fputs("<ol class=\"agenda\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            char day[32] = "", when[48] = ""; struct tm tm;
            /* An event with no stated end runs to the end of its day: the same
             * reading that kept it on the page, so the words cannot contradict
             * the list. */
            time_t ends = item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : item->at + 86400;
            if (item->at > 0 && page_local_time(item->at, &tm))
                snprintf(day, sizeof(day), "%s %d", page_month_names[tm.tm_mon], tm.tm_mday);
            if (day[0] && (!group || strcmp(group, day))) {
                static char held[32];
                snprintf(held, sizeof(held), "%s", day); group = held;
                fputs("<li class=\"agenda-day\"><h3>", stdout); html(day); fputs("</h3></li>", stdout);
            }
            page_when_ahead(item->at, ends, now, when, sizeof(when));
            fputs("<li class=\"agenda-item\">", stdout);
            close = page_emit_item_open(item, NULL);
            fputs("<b>", stdout); html(item->title); fputs("</b>", stdout);
            fputs(close, stdout);
            /* Same local calendar as the event display. This weekend means the
             * upcoming Saturday/Sunday through this Sunday, not all future weekends.
             * A missing timestamp never becomes a weekend claim. */
            {
                struct tm event_tm, current_tm; int weekend = 0, offset = -1;
                if (item->at > 0 && page_local_time(item->at, &event_tm) && page_local_time(now, &current_tm)) {
                    int year = current_tm.tm_year + 1900;
                    int year_days = 365 + (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
                    offset = event_tm.tm_year == current_tm.tm_year ? event_tm.tm_yday - current_tm.tm_yday :
                        event_tm.tm_year == current_tm.tm_year + 1 ? year_days - current_tm.tm_yday + event_tm.tm_yday : -1;
                    int until_sunday = current_tm.tm_wday == 0 ? 0 : 7 - current_tm.tm_wday;
                    weekend = offset >= 0 && offset <= until_sunday &&
                        (event_tm.tm_wday == 0 || event_tm.tm_wday == 6);
                }
                printf("<data data-agenda-weekend=\"%d\" data-agenda-day-offset=\"%d\" hidden></data>", weekend, offset);
            }
            if (when[0]) { fputs("<span class=\"starts\">", stdout); html(when); fputs("</span>", stdout); }
            if (item->where[0]) { fputs("<span class=\"at\">", stdout); html(item->where); fputs("</span>", stdout); }
            fputs("<span class=\"by\">", stdout); html(item->by); fputs("</span></li>", stdout);
        }
        fputs("</ol>", stdout);
        fputs("<script>(function(){\n const script=document.currentScript,agenda=script?.previousElementSibling,tools=agenda?.previousElementSibling;\n if(!agenda?.matches('ol.agenda')||!tools?.hasAttribute('data-agenda-controls'))return;\n const entries=[...agenda.children].filter(n=>n.matches('li.agenda-item'));\n const dates=[...agenda.children].filter(n=>n.matches('li.agenda-day'));\n const query=tools.querySelector('[data-agenda-search]'),count=tools.querySelector('[data-agenda-count]');\n const filters=[...tools.querySelectorAll('[data-agenda-filter]')];let mode='all';\n function update(){const term=query.value.trim().toLocaleLowerCase();let visible=0;\n  for(const item of entries){const signal=item.querySelector('[data-agenda-weekend]');const weekend=signal?.dataset.agendaWeekend==='1';const offset=Number(signal?.dataset.agendaDayOffset);const known=signal&&Number.isInteger(offset)&&offset>=0;const windowOk=mode==='all'||(mode==='weekend'&&weekend)||(mode==='today'&&known&&offset===0)||(mode==='seven'&&known&&offset<7);\n   const match=windowOk&&(!term||item.textContent.toLocaleLowerCase().includes(term));\n   item.hidden=!match;if(match)visible++;}\n  for(const date of dates){let cursor=date.nextElementSibling,hasVisible=false;\n   while(cursor&&!cursor.matches('li.agenda-day')){if(cursor.matches('li.agenda-item')&&!cursor.hidden)hasVisible=true;cursor=cursor.nextElementSibling;}\n   date.hidden=!hasVisible;}\n  for(const button of filters)button.setAttribute('aria-pressed',String(button.dataset.agendaFilter===mode));\n  count.textContent='Showing '+visible+' of '+entries.length+' published events on this page.';\n }\n for(const button of filters)button.addEventListener('click',()=>{mode=['all','today','seven','weekend'].includes(button.dataset.agendaFilter)?button.dataset.agendaFilter:'all';update();});\n query.addEventListener('input',update);update();\n})();</script>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_LIST:
        fputs("<ol class=\"more\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            fputs("<li>", stdout); close = page_emit_item_open(item, NULL); html(item->title);
            fputs(close, stdout); fputs("<span class=\"by\">", stdout); page_emit_by(item, now); fputs("</span></li>", stdout);
        }
        fputs("</ol>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_SHELF:
        fputs("<div class=\"shelf\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            if (item->asset[0]) {
                fputs("<figure class=\"clip\"><video controls playsinline preload=\"none\" src=\"", stdout); html(item->asset); fputs("\"", stdout);
                if (item->poster[0]) { fputs(" poster=\"", stdout); html(item->poster); fputs("\"", stdout); }
                fputs("></video><figcaption>", stdout); html(item->title);
                if (item->duration_ms > 0) { fputs("<span class=\"by\">", stdout); page_emit_duration(item->duration_ms); fputs("</span>", stdout); }
                fputs("</figcaption></figure>", stdout);
            } else {
                fputs("<figure class=\"clip\">", stdout); close = page_emit_item_open(item, NULL);
                fputs("<span class=\"frame\">", stdout); page_emit_picture(item->image, "poster"); fputs("<span class=\"play\">", stdout); page_glyph("play"); fputs("</span></span>", stdout);
                fputs("<figcaption>", stdout); html(item->title);
                fputs("<span class=\"by\">", stdout); page_emit_by(item, now); fputs("</span></figcaption>", stdout); fputs(close, stdout); fputs("</figure>", stdout);
            }
        }
        fputs("</div>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_FIELD:
        fputs("<div class=\"field\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            fputs("<figure class=\"photo\">", stdout); close = page_emit_item_open(item, NULL);
            page_emit_picture(item->image, "picture");
            fputs("<figcaption>", stdout); html(item->title);
            fputs("<span class=\"by\">", stdout); page_emit_by(item, now);
            if (item->rights[0]) { fputs("<span class=\"terms\">", stdout); html(item->rights); fputs("</span>", stdout); }
            fputs("</span></figcaption>", stdout); fputs(close, stdout); fputs("</figure>", stdout);
        }
        fputs("</div>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_WALL:
        /* Pinterest's wall: the picture is the item; its words appear when a
         * person points at it or moves focus to it. */
        fputs("<div class=\"wall-pins\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            if (!page_image_url_safe(item->image)) continue;
            close = page_emit_item_open(item, "pin");
            page_emit_picture(item->image, "picture");
            fputs("<span class=\"cap\"><b>", stdout); html(item->title); fputs("</b><span>", stdout); html(item->by); fputs("</span>", stdout);
            if (item->rights[0]) { fputs("<span class=\"terms\">", stdout); html(item->rights); fputs("</span>", stdout); }
            fputs("</span>", stdout);
            fputs(close, stdout);
        }
        fputs("</div>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_GRID:
        if (!ctx->preview) fputs("<style>.media-catalogue-controls{display:grid;grid-template-columns:minmax(0,1fr) minmax(180px,300px);gap:12px 20px;align-items:center;margin:0 0 24px;padding:17px 0;border-bottom:1px solid var(--w-rule,#444935)}.media-catalogue-controls .media-modes{display:flex;flex-wrap:wrap;gap:8px}.media-catalogue-controls button{min-height:44px;padding:8px 13px;border:1px solid var(--w-rule,#444935);color:inherit;background:transparent;border-radius:999px;cursor:pointer}.media-catalogue-controls button[aria-pressed=true]{border-color:var(--w-accent,#d0fc4f);color:var(--w-ground,#0f1511);background:var(--w-accent,#d0fc4f)}.media-catalogue-controls label{display:grid;gap:5px;font:600 12px/1.4 system-ui}.media-catalogue-controls input{min-height:44px;width:100%;min-width:0;padding:10px;border:1px solid var(--w-rule,#444935);color:inherit;background:transparent;border-radius:8px}.media-catalogue-controls output{grid-column:1/-1;font:12px/1.4 system-ui}.media-catalogue .page-media-card[hidden]{display:none!important}.media-catalogue[data-media-view=list] .video-grid{display:grid;grid-template-columns:minmax(0,1fr)!important}.media-catalogue[data-media-view=list] .page-media-card{display:grid;grid-template-columns:minmax(96px,170px) minmax(0,1fr);column-gap:22px;padding:16px 0;border-bottom:1px solid var(--w-rule,#444935);align-items:start}.media-catalogue[data-media-view=list] .page-media-card>a.video{grid-column:1}.media-catalogue[data-media-view=list] .page-media-card>.page-item-continuations{grid-column:2}.media-catalogue .media-catalogue-proof{display:flex;flex-wrap:wrap;gap:5px 12px;font:11px/1.5 system-ui;color:var(--w-muted,#c5c9af);overflow-wrap:anywhere}.media-catalogue .media-catalogue-proof .terms{max-width:100%}@media(max-width:740px){.media-catalogue-controls{grid-template-columns:1fr}.media-catalogue[data-media-view=list] .page-media-card{grid-template-columns:1fr}.media-catalogue[data-media-view=list] .page-media-card>*{grid-column:1!important}}</style><section class=\"media-catalogue\" data-media-catalogue data-media-view=\"grid\"><div class=\"media-catalogue-controls\"><div class=\"media-modes\" role=\"group\" aria-label=\"Source material type\"><button type=\"button\" data-media-kind=\"all\" aria-pressed=\"true\">All</button><button type=\"button\" data-media-kind=\"video\" aria-pressed=\"false\">Films &amp; video</button><button type=\"button\" data-media-kind=\"audio\" aria-pressed=\"false\">Audio</button><button type=\"button\" data-media-kind=\"photo\" aria-pressed=\"false\">Images</button><button type=\"button\" data-media-kind=\"other\" aria-pressed=\"false\">Writing &amp; other</button></div><label>Search these retained items<input type=\"search\" data-media-search maxlength=\"120\" placeholder=\"Title, publisher, source\"></label><div class=\"media-modes\" role=\"group\" aria-label=\"Catalogue presentation\"><button type=\"button\" data-media-layout=\"grid\" aria-pressed=\"true\">Gallery</button><button type=\"button\" data-media-layout=\"list\" aria-pressed=\"false\">List</button></div><output role=\"status\" aria-live=\"polite\" data-media-count></output></div>",stdout);
        fputs("<div class=\"video-grid\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            char when[48];
            const char *media_kind = item->kind == BF_PAGE_MATERIAL_VIDEO ? "video" :
                item->kind == BF_PAGE_MATERIAL_AUDIO ? "audio" : item->kind == BF_PAGE_MATERIAL_PHOTO ? "photo" : "other";
            fputs("<article class=\"page-media-card\" data-media-card data-media-kind=\"",stdout);
            fputs(media_kind,stdout); fputs("\">",stdout);
            page_when(item->at, now, when, sizeof(when));
            close = page_emit_item_open(item, "video");
            fputs("<span class=\"frame\">", stdout); page_emit_picture(item->image, "poster");
            if (item->kind == BF_PAGE_MATERIAL_VIDEO || item->kind == BF_PAGE_MATERIAL_AUDIO) {
                fputs("<span class=\"play\" aria-hidden=\"true\">",stdout);page_glyph("play");fputs("</span>",stdout);
            } else if (item->kind == BF_PAGE_MATERIAL_PHOTO) {
                fputs("<span class=\"play\" aria-hidden=\"true\">",stdout);page_glyph("picture");fputs("</span>",stdout);
            }
            fputs("</span>",stdout);
            fputs("<b>", stdout); html(item->title); fputs("</b><span class=\"by\"><span>", stdout); html(item->by); fputs("</span>", stdout);
            if (item->note[0] && strcmp(item->note, item->by)) { fputs(" <span class=\"via\">", stdout); html(item->note); fputs("</span>", stdout); }
            if (when[0]) { fputs(" <time>", stdout); html(when); fputs("</time>", stdout); }
            fputs("</span>",stdout);fputs(close,stdout);
            fputs("<div class=\"media-catalogue-proof\"><span>Source: ",stdout);html(item->source);fputs("</span>",stdout);
            if(item->rights[0]){fputs("<span class=\"terms\">Terms: ",stdout);html(item->rights);fputs("</span>",stdout);}
            if(item->duration_ms>0){fputs("<span>Duration: ",stdout);page_emit_duration(item->duration_ms);fputs("</span>",stdout);}
            fputs("</div>",stdout);page_emit_item_continuations(item);fputs("</article>",stdout);
        }
        fputs("</div>", stdout);
        if (!ctx->preview) {
            fputs("</section>",stdout);
            fputs("<script>(()=>{\nconst root=document.currentScript?.previousElementSibling;\nif(!root?.matches('[data-media-catalogue]'))return;\nconst cards=[...root.querySelectorAll('[data-media-card]')],input=root.querySelector('[data-media-search]'),count=root.querySelector('[data-media-count]');\nconst kinds=[...root.querySelectorAll('[data-media-kind]:not([data-media-card])')],layouts=[...root.querySelectorAll('[data-media-layout]')];\nlet kind='all',layout='grid';\nfunction update(){const query=input.value.trim().toLocaleLowerCase();let shown=0;\nfor(const card of cards){const matching=(kind==='all'||card.dataset.mediaKind===kind)&&(!query||card.textContent.toLocaleLowerCase().includes(query));card.hidden=!matching;if(matching)shown++;}\nroot.dataset.mediaView=layout;\nfor(const button of kinds)button.setAttribute('aria-pressed',String(button.dataset.mediaKind===kind));\nfor(const button of layouts)button.setAttribute('aria-pressed',String(button.dataset.mediaLayout===layout));\ncount.textContent='Showing '+shown+' of '+cards.length+' retained items on this page. Rights and sources remain as published.';\n}\ninput.addEventListener('input',update);\nfor(const button of kinds)button.addEventListener('click',()=>{kind=button.dataset.mediaKind;update();});\nfor(const button of layouts)button.addEventListener('click',()=>{layout=button.dataset.mediaLayout;update();});\nupdate();\n})();</script>",stdout);
        }
        break;
    case BF_PAGE_TOPOLOGY_FEED:
        /* One stream that changes shape with what arrives: a picture takes
         * its own height, a video shows it will play, a story is set as type.
         * Nothing is boxed; the column rhythm does the separating. */
        fputs("<style>.feed-tools{display:grid;grid-template-columns:minmax(0,1fr) minmax(160px,260px);gap:12px;padding:18px 0;margin:0 0 20px;border-bottom:1px solid var(--w-rule,#444935);align-items:center}.feed-modes{display:flex;gap:8px;flex-wrap:wrap}.feed-modes button{min-height:44px;border:1px solid var(--w-rule,#444935);border-radius:999px;padding:10px 13px;background:transparent;color:inherit;cursor:pointer}.feed-modes button[aria-pressed=true]{background:var(--w-accent,#d0fc4f);color:var(--w-ground,#0f1511)}.feed-tools label{display:grid;gap:5px;font:600 12px/1.4 system-ui}.feed-tools input{min-width:0;width:100%;min-height:44px;padding:9px;border:1px solid var(--w-rule,#444935);border-radius:8px;background:var(--w-ground,#0f1511);color:inherit}.feed-tools output{grid-column:1/-1;font:12px/1.5 system-ui;color:var(--w-muted,#c5c9af)}.feed article[hidden]{display:none!important}@media(max-width:720px){.feed-tools{grid-template-columns:1fr}}</style><section class=\"feed-tools\" data-feed-tools aria-label=\"Filter retained feed\"><div class=\"feed-modes\" role=\"group\" aria-label=\"Post format\"><button type=\"button\" data-feed-mode=\"all\" aria-pressed=\"true\">All material</button><button type=\"button\" data-feed-mode=\"is-story\" aria-pressed=\"false\">Stories</button><button type=\"button\" data-feed-mode=\"is-picture\" aria-pressed=\"false\">Images</button><button type=\"button\" data-feed-mode=\"is-video\" aria-pressed=\"false\">Video</button></div><label>Find in loaded feed<input type=\"search\" data-feed-search maxlength=\"120\" placeholder=\"Title, source or text\"></label><output data-feed-count role=\"status\" aria-live=\"polite\"></output></section>", stdout);
        fputs("<div class=\"feed\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            const char *kind = page_is_video(item) ? "is-video" : page_is_photo(item) || page_image_url_safe(item->image) ? "is-picture" : "is-story";
            fputs("<article class=\"tile ", stdout); fputs(kind, stdout); fputs("\">", stdout);
            close = page_emit_item_open(item, NULL);
            if (page_image_url_safe(item->image)) {
                fputs("<span class=\"frame\">", stdout); page_emit_picture(item->image, "picture");
                if (page_is_video(item)) { fputs("<span class=\"play\">", stdout); page_glyph("play"); fputs("</span>", stdout); }
                fputs("</span>", stdout);
            }
            fputs("<b>", stdout); html(item->title); fputs("</b>", stdout);
            if (item->body[0] && !page_image_url_safe(item->image)) { fputs("<span class=\"dek\">", stdout); html(item->body); fputs("</span>", stdout); }
            fputs(close, stdout);
            fputs("<span class=\"by\">", stdout); page_emit_by(item, now); fputs("</span>",stdout); page_emit_item_continuations(item); fputs("</article>",stdout);
        }
        fputs("</div>", stdout);
        fputs("<script>(()=>{\n const feed=document.currentScript?.previousElementSibling,tools=feed?.previousElementSibling;\n if(!feed?.matches('.feed')||!tools?.matches('[data-feed-tools]'))return;\n const cards=[...feed.querySelectorAll(':scope > article.tile')],search=tools.querySelector('[data-feed-search]');\n const modes=[...tools.querySelectorAll('[data-feed-mode]')],count=tools.querySelector('[data-feed-count]');\n let mode='all';function update(){const term=search.value.trim().toLocaleLowerCase();let shown=0;\n  for(const card of cards){const yes=(mode==='all'||card.classList.contains(mode))&&(!term||card.textContent.toLocaleLowerCase().includes(term));card.hidden=!yes;if(yes)shown++;}\n  for(const button of modes)button.setAttribute('aria-pressed',String(mode===button.dataset.feedMode));\n  count.textContent='Showing '+shown+' of '+cards.length+' retained items loaded on this page. Other feeds and earlier pages are unchanged.';\n }\n search.addEventListener('input',update);for(const button of modes)button.addEventListener('click',()=>{mode=button.dataset.feedMode;update();});update();\n})();</script>",stdout);
        break;
    case BF_PAGE_TOPOLOGY_RESULTS:
        fputs("<ol class=\"results\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            fputs("<li>", stdout); close = page_emit_item_open(item, "result");
            fputs("<span class=\"kind\">", stdout);
            page_glyph(page_is_video(item) ? "play" : page_is_photo(item) ? "picture" : item->placed ? "pin" : "text");
            fputs("</span><span class=\"what\"><b>", stdout); html(item->title); fputs("</b>", stdout);
            if (item->body[0]) { fputs("<span class=\"dek\">", stdout); html(item->body); fputs("</span>", stdout); }
            fputs("<span class=\"by\">", stdout); page_emit_by(item, now); fputs("</span></span>", stdout);
            page_emit_picture(item->image, "thumb");
            fputs(close, stdout); fputs("</li>", stdout);
        }
        fputs("</ol>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_TABLE:
        /* A ledger: one line per thing, columns that line up, the way issue
         * trackers and registries let a person scan hundreds at once. */
        fputs("<div class=\"ledger-wrap\"><table class=\"ledger\"><thead><tr><th>Name</th><th>By</th><th>Where</th><th>When</th></tr></thead><tbody>", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            char when[48];
            page_when(item->at, now, when, sizeof(when));
            fputs("<tr><td class=\"name\">", stdout); close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
            if (item->body[0]) { fputs("<span class=\"dek\">", stdout); html(item->body); fputs("</span>", stdout); }
            fputs("</td><td>", stdout);
            if (item->by[0] && strcmp(item->by, item->note)) { page_emit_filter_link(ctx, "from", item->by); html(item->by); fputs("</a>", stdout); }
            fputs("</td><td>", stdout);
            if (item->note[0]) { page_emit_filter_link(ctx, "from", item->note); html(item->note); fputs("</a>", stdout); }
            fputs("</td><td class=\"when\">", stdout); html(when); fputs("</td></tr>", stdout);
        }
        fputs("</tbody></table></div>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_COMMUNITIES: {
        /* Lemmy's community list: who is posting, how much, and what. The
         * busiest group leads; each name narrows the page to that group. */
        char names[24][256]; int counts[24] = {0}, order[24], group_count = 0, g, h;
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            const char *name = item->by[0] ? item->by : item->note;
            for (g = 0; g < group_count; g++) if (!strcmp(names[g], name)) break;
            if (g == group_count) { if (group_count >= 24) continue; snprintf(names[group_count++], sizeof(names[0]), "%s", name); }
            counts[g]++;
        }
        for (g = 0; g < group_count; g++) order[g] = g;
        for (g = 1; g < group_count; g++) for (h = g; h > 0 && counts[order[h]] > counts[order[h - 1]]; h--) { int t = order[h]; order[h] = order[h - 1]; order[h - 1] = t; }
        fputs("<div class=\"communities\">", stdout);
        for (g = 0; g < group_count && g < (ctx->preview ? 4 : 18); g++) {
            int shown = 0;
            const char *name = names[order[g]];
            fputs("<section class=\"community\"><header>", stdout);
            page_emit_filter_link(ctx, "from", name); fputs("<h3>", stdout); html(name[0] ? name : "Elsewhere"); fputs("</h3></a>", stdout);
            printf("<small>%d post%s</small></header><ol>", counts[order[g]], counts[order[g]] == 1 ? "" : "s");
            for (k = 0; k < take && shown < (ctx->preview ? 2 : 4); k++) {
                const PageItem *item = &stream->item[stream->cursor + k];
                const char *item_name = item->by[0] ? item->by : item->note;
                char when[48];
                if (strcmp(item_name, name)) continue;
                shown++;
                page_when(item->at, now, when, sizeof(when));
                fputs("<li>", stdout); close = page_emit_item_open(item, NULL);
                page_emit_picture(item->image, "thumb");
                fputs("<b>", stdout); html(item->title); fputs("</b>", stdout);
                if (when[0]) { fputs("<time>", stdout); html(when); fputs("</time>", stdout); }
                fputs(close, stdout); fputs("</li>", stdout);
            }
            fputs("</ol></section>", stdout);
        }
        fputs("</div>", stdout);
        break;
    }
    case BF_PAGE_TOPOLOGY_MAP:
        /* An equirectangular plate beside a numbered list: every point where
         * its source placed it, and every point one tap from its own page. */
        fputs("<figure class=\"map with-list\">", stdout);
        if (!ctx->preview) fputs("<style>.map-inspector{margin:20px 0 0;padding:22px;border:1px solid #60765e;border-radius:16px;background:var(--w-panel,#171d19);color:inherit}.map-inspector[hidden]{display:none!important}.map-inspector h3{font:600 24px/1.2 system-ui;margin:0 0 12px}.map-inspector [data-map-inspector-body]{display:grid;gap:8px;overflow-wrap:anywhere}.map-inspector [data-map-inspector-body] a{display:inline-flex;align-items:center;min-height:44px;color:var(--w-accent,#d0fc4f);text-decoration:underline;text-underline-offset:3px}.map-inspector p{font:12px/1.5 system-ui}.map-inspector button{min-height:44px;border:1px solid #60765e;border-radius:9px;padding:10px 14px;background:transparent;color:inherit;cursor:pointer}@media(max-width:620px){.map-inspector{padding:16px}}.map-tools{display:flex;gap:12px;align-items:end;flex-wrap:wrap;padding:12px 0 18px}.map-tools label{display:grid;gap:5px;flex:1 1 190px;max-width:460px;font:600 12px/1.5 system-ui}.map-tools input{width:100%;min-height:44px;padding:10px;border:1px solid #6e806c;border-radius:8px;background:transparent;color:inherit}.map-tools output{font:12px/1.5 system-ui}.places-list [data-locate-point]{min-height:42px;margin-right:10px;border:1px solid #6e806c;border-radius:8px;background:transparent;color:inherit;cursor:pointer}.places-list [data-locate-point][aria-pressed=true]{background:#d0fc4f;color:#0f1511}.places-list li[hidden]{display:none!important}.map .point[data-selected=true] :is(circle){stroke:#d0fc4f;stroke-width:3;fill:#f0af46}.map circle.point[data-selected=true]{stroke:#d0fc4f;stroke-width:2;fill:#f0af46}@media(max-width:620px){.map-tools{display:grid}}</style><div class=\"map-tools\" data-map-tools><label>Find a mapped place<input type=\"search\" data-map-search maxlength=\"120\" placeholder=\"Title or source\"></label><output role=\"status\" aria-live=\"polite\" data-map-count></output></div>",stdout);
        page_emit_map_plate(stream, take, 1);
        if (!ctx->preview) {
            fputs("<ol class=\"places-list\">", stdout);
            for (k = 0; k < take; k++) {
                const PageItem *item = &stream->item[stream->cursor + k];
                char when[48];
                if (!item->placed) continue;
                page_when(item->at, now, when, sizeof(when));
                printf("<li data-map-point=\"%d\"><button type=\"button\" data-locate-point=\"%d\" aria-pressed=\"false\" aria-label=\"Highlight place %d on map\">Locate</button>", k, k, k+1); close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
                fputs("<span class=\"by\"><span>", stdout); html(item->note[0] ? item->note : item->by); fputs("</span>", stdout);
                if (when[0]) { fputs("<time>", stdout); html(when); fputs("</time>", stdout); }
                fputs("</span></li>", stdout);
            }
            fputs("</ol>", stdout);
        }
        if (!ctx->preview) fputs("<aside data-map-inspector class=\"map-inspector\" aria-label=\"Selected source-placed location\" aria-live=\"polite\" hidden><h3>Selected place</h3><div data-map-inspector-body></div><p>This view repeats the retained source. It does not verify whether a position is current.</p><button type=\"button\" data-map-clear>Clear map focus</button></aside>",stdout);
        printf("<figcaption>%d place%s", take, take == 1 ? "" : "s");
        if (first->title[0]) { fputs(" &middot; newest: ", stdout); html(first->title); }
        fputs("</figcaption></figure>", stdout);
        if (!ctx->preview) fputs("<script>(()=>{\nconst figure=document.currentScript?.previousElementSibling;\nif(!figure?.matches('figure.map.with-list'))return;\nconst tools=figure.querySelector('[data-map-tools]'),rows=[...figure.querySelectorAll('ol.places-list>li[data-map-point]')];\nif(!tools||!rows.length)return;\nconst search=tools.querySelector('[data-map-search]'),count=tools.querySelector('[data-map-count]');\nconst markers=[...figure.querySelectorAll('svg [data-map-point]')],inspector=figure.querySelector('[data-map-inspector]');\nconst inspectorBody=inspector?.querySelector('[data-map-inspector-body]'),clear=inspector?.querySelector('[data-map-clear]');\nlet selected='';\nfunction update(){const query=search.value.trim().toLocaleLowerCase();let visible=0;\n for(const row of rows){const yes=!query||row.textContent.toLocaleLowerCase().includes(query);row.hidden=!yes;if(yes)visible++;}\n count.textContent=visible+' of '+rows.length+' source-placed locations in this list. All plotted positions remain visible.';\n for(const row of rows)row.querySelector('[data-locate-point]')?.setAttribute('aria-pressed',String(row.dataset.mapPoint===selected));\n for(const mark of markers)mark.dataset.selected=String(mark.dataset.mapPoint===selected);\n}\nfunction inspect(id){const row=rows.find(r=>r.dataset.mapPoint===id);if(!row)return;selected=id;update();if(!inspectorBody)return;inspectorBody.replaceChildren();\n const source=row.querySelector('a[href]'),name=source?.textContent?.trim()||row.textContent.replace(/^Locate\\s*/,'').trim();\n const title=document.createElement('strong');title.textContent=name;inspectorBody.append(title);\n const about=row.querySelector('.by')?.textContent?.trim();if(about){const meta=document.createElement('p');meta.textContent=about;inspectorBody.append(meta);}\n if(source){const link=document.createElement('a');link.href=source.href;link.textContent='Open the exact retained source';inspectorBody.append(link);}\n else{const label=document.createElement('p');label.textContent='This source did not declare a navigable record address.';inspectorBody.append(label);}\n inspector.hidden=false;inspector.dataset.selected=id;\n figure.querySelector('.plate')?.scrollIntoView({block:'nearest',behavior:'instant'});\n}\nfunction reset(){selected='';if(inspector){inspector.hidden=true;delete inspector.dataset.selected;}update();}\nfor(const row of rows)row.querySelector('[data-locate-point]')?.addEventListener('click',()=>inspect(row.dataset.mapPoint));\nclear?.addEventListener('click',reset);\nfigure.addEventListener('keydown',event=>{if(event.key==='Escape'&&selected){event.preventDefault();reset();}});\nsearch.addEventListener('input',update);update();\n})();</script>",stdout);
        break;
    case BF_PAGE_TOPOLOGY_THREAD:
        if (!first->group[0]) {
            /* Zulip's two panes: places on the left with their counts, the
             * selected place's topics on the right.  Choosing a place is a
             * link, so it works without a script and can be shared. */
            char groups[24][256]; int counts[24] = {0}, group_count = 0, g, selected = 0, shown = 0;
            for (k = 0; k < take; k++) {
                const PageItem *item = &stream->item[stream->cursor + k];
                for (g = 0; g < group_count; g++) if (!strcmp(groups[g], item->note)) break;
                if (g == group_count) { if (group_count >= 24) continue; snprintf(groups[group_count++], sizeof(groups[0]), "%s", item->note); }
                counts[g]++;
            }
            for (g = 0; g < group_count; g++) if (ctx->stream && !strcasecmp(ctx->stream, groups[g])) selected = g;
            if (ctx->preview) {
                fputs("<div class=\"streams\">", stdout);
                for (g = 0; g < group_count && g < 3; g++) {
                    int n = 0;
                    fputs("<section class=\"stream\"><h3>", stdout); page_emit_filter_link(ctx, "s", groups[g]); html(groups[g][0] ? groups[g] : "Elsewhere"); fputs("</a></h3><ol>", stdout);
                    for (k = 0; k < take && n < 3; k++) {
                        const PageItem *item = &stream->item[stream->cursor + k];
                        if (strcmp(groups[g], item->note)) continue;
                        n++; fputs("<li>", stdout); close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout); fputs("</li>", stdout);
                    }
                    fputs("</ol></section>", stdout);
                }
                fputs("</div>", stdout);
                break;
            }
            {
                const char *frontier = getenv("BONFYRE_SOURCE_FRONTIER_DB");
                const char *readers[32]; size_t reader_count = 0;
                for (k = 0; k < take && reader_count < 32; k++) {
                    const PageItem *item = &stream->item[stream->cursor + k];
                    size_t rr; int seen = 0;
                    if (!item->source[0]) continue;
                    for (rr = 0; rr < reader_count; rr++)
                        if (!strcmp(readers[rr], item->source)) { seen = 1; break; }
                    if (!seen) readers[reader_count++] = item->source;
                }
                if (frontier && *frontier && reader_count)
                    bf_source_hierarchy_emit(stdout, frontier, readers, reader_count,
                                             ctx->route ? ctx->route : "/channels", 8, 8);
            }
            fputs("<div class=\"panes\"><nav class=\"pane-list\" aria-label=\"Places\"><ol>", stdout);
            for (g = 0; g < group_count; g++) {
                fputs("<li>", stdout);
                fputs("<a href=\"", stdout); html(ctx->route ? ctx->route : "/channels"); fputs("?s=", stdout); page_url_escape(groups[g]); fputs("\"", stdout);
                if (g == selected) fputs(" aria-current=\"true\"", stdout);
                fputs(">", stdout); html(groups[g][0] ? groups[g] : "Elsewhere"); printf("<small>%d</small></a></li>", counts[g]);
            }
            fputs("</ol></nav><section class=\"pane-topics\"><h3>", stdout); html(group_count ? groups[selected] : ""); fputs("</h3><ol>", stdout);
            for (k = 0; k < take; k++) {
                const PageItem *item = &stream->item[stream->cursor + k];
                char when[48];
                if (!group_count || strcmp(groups[selected], item->note)) continue;
                shown++;
                page_when(item->at, now, when, sizeof(when));
                fputs("<li>", stdout); close = page_emit_item_open(item, "topic");
                fputs("<b>", stdout); html(item->title); fputs("</b><span class=\"by\">", stdout);
                if (strcmp(item->by, item->note)) { fputs("<span>", stdout); html(item->by); fputs("</span>", stdout); }
                if (when[0]) { fputs("<time>", stdout); html(when); fputs("</time>", stdout); }
                fputs("</span>", stdout); page_emit_picture(item->image, "thumb"); fputs(close, stdout); fputs("</li>", stdout);
            }
            (void)shown;
            fputs("</ol></section></div>", stdout);
            break;
        }
        /* Owners hand over the newest posts; a conversation reads oldest first. */
        for (k = take - 1; k >= 0; k--) {
            const PageItem *item = &stream->item[stream->cursor + k];
            if (!group || strcmp(group, item->group)) {
                if (group) fputs("</div>", stdout);
                group = item->group;
                fputs("<div class=\"topic\"><h3>", stdout); html(item->group);
                if (item->note[0]) { fputs(" <small>", stdout); html(item->note); fputs("</small>", stdout); }
                fputs("</h3>", stdout);
            }
            fputs("<div class=\"msg\"><header><b>", stdout); html(item->by); fputs("</b>", stdout);
            {
                char when[48]; page_when(item->at, now, when, sizeof(when));
                if (when[0]) { fputs("<time>", stdout); html(when); fputs("</time>", stdout); }
            }
            fputs("</header>", stdout);
            if (item->title[0]) { fputs("<h4>", stdout); html(item->title); fputs("</h4>", stdout); }
            if (item->body[0]) { fputs("<p>", stdout); html(item->body); fputs("</p>", stdout); }
            fputs("</div>", stdout);
        }
        if (group) fputs("</div>", stdout);
        break;
    case BF_PAGE_TOPOLOGY_TRACKS: {
        /* A listening surface, not a list of links: the first recording that
         * plays stands at the top with its art, the rest are rows, and one
         * deck at the foot of the page plays them in turn.  A person who
         * starts listening keeps listening; nothing plays until they ask, and
         * every file is the one its own source published, fetched from that
         * source by the person's own browser. */
        const PageItem *first = NULL;
        unsigned playable = 0;
        /* Only a listening surface itself opens with something ready to play:
         * a preview of it elsewhere is a list, not a player. */
        if (!ctx->preview && take >= 3 && !page_offered_a_start)
            for (k = 0; k < take && !first; k++)
                if (stream->item[stream->cursor + k].asset[0]) first = &stream->item[stream->cursor + k];
        if (first) {
            char sized[1024];
            page_offered_a_start = 1;
            page_picture_at(first->image, 480, sized, sizeof(sized));
            fputs("<div class=\"now-playing\">", stdout);
            if (page_image_url_safe(sized)) page_emit_picture(sized, "sleeve");
            fputs("<div class=\"np-text\"><p class=\"np-kicker\">Start here</p><h3>", stdout);
            close = page_emit_item_open(first, NULL); html(first->title); fputs(close, stdout);
            fputs("</h3><p class=\"np-by\">", stdout); html(first->by);
            if (first->duration_ms > 0) { fputs("<span class=\"dur\">", stdout); page_emit_duration(first->duration_ms); fputs("</span>", stdout); }
            fputs("</p>", stdout);
            if (first->rights[0]) { fputs("<p class=\"terms\">", stdout); html(first->rights); fputs("</p>", stdout); }
            fputs("</div></div>", stdout);
        }
        fputs("<ol class=\"tracks\">", stdout);
        for (k = 0; k < take; k++) {
            const PageItem *item = &stream->item[stream->cursor + k];
            char sized[1024];
            printf("<li%s", item == first ? " class=\"is-first\"" : "");
            if (item->asset[0]) {
                playable++;
                fputs(" data-play=\"", stdout); html(item->asset);
                fputs("\" data-title=\"", stdout); html(item->title);
                fputs("\" data-by=\"", stdout); html(item->by);
                if (item->image[0]) { char art[1024]; page_picture_at(item->image, 120, art, sizeof(art));
                    if (page_image_url_safe(art)) { fputs("\" data-art=\"", stdout); html(art); } }
                fputs("\" data-href=\"/o/", stdout); page_url_escape(item->source); fputs("/", stdout); html(item->key);
                fputs("\"", stdout);
            }
            printf("><span class=\"no\">%d</span>", k + 1);
            page_picture_at(item->image, 120, sized, sizeof(sized));
            if (page_image_url_safe(sized)) page_emit_picture(sized, "art");
            fputs("<span class=\"tt\"><b>", stdout);
            close = page_emit_item_open(item, NULL); html(item->title); fputs(close, stdout);
            fputs("</b><span>", stdout); html(item->by); fputs("</span></span><span class=\"dur\">", stdout);
            page_emit_duration(item->duration_ms);
            fputs("</span>", stdout);
            if (item->asset[0]) {
                /* The track hands the deck what it needs and nothing else: one
                 * player for the whole queue, so a recording is followed by the
                 * next instead of ending in silence. */
                fputs("<button class=\"lift\" type=\"button\">", stdout); page_glyph("play"); fputs("<span>Play</span></button>", stdout);
            } else if (page_item_has_page(item)) {
                /* Played by its host's own player, on its page. */
                fputs("<a class=\"play-on-page\" href=\"/o/", stdout); page_url_escape(item->source); fputs("/", stdout); html(item->key);
                fputs("\">", stdout); page_glyph("play"); fputs("<span>Play</span></a>", stdout);
            }
            fputs("</li>", stdout);
        }
        fputs("</ol>", stdout);
        if (playable) page_emit_deck();
        break;
    }
    }
}

/* A rhythm that shows many things at once (a wall, a grid, a field, a map, a
 * shelf, a track list) takes turns across sources, each source newest first,
 * so one prolific source cannot fill it.  A reading list keeps pure time
 * order.  Only the part of the stream not yet consumed is reordered. */
static void page_take_turns(PageStream *stream) {
    static PageItem turned[PAGE_STREAM_MAX];
    int rank[PAGE_STREAM_MAX], k, j, turn, placed = 0, remaining = stream->count - stream->cursor, sources = 0;
    if (remaining < 2) return;
    for (k = 0; k < remaining; k++) {
        const PageItem *item = &stream->item[stream->cursor + k];
        rank[k] = 0;
        for (j = 0; j < k; j++) if (!strcmp(stream->item[stream->cursor + j].source, item->source)) rank[k]++;
        if (!rank[k]) sources++;
    }
    if (sources < 2) return;
    for (turn = 0; placed < remaining; turn++)
        for (k = 0; k < remaining; k++) if (rank[k] == turn) turned[placed++] = stream->item[stream->cursor + k];
    for (k = 0; k < remaining; k++) stream->item[stream->cursor + k] = turned[k];
}

/* --------------------------------------------------- the estate's own looks
 *
 * The estate holds hundreds of art-direction profiles, each one a studied
 * reading surface with its own ground, ink, accent, faces, measure and card
 * treatment.  Until now one template dressed every composed page, so a
 * recording, a city and a day of events all read the same.
 *
 * A page takes its look from what it is: the material its scene is mostly made
 * of chooses the family of profile (recordings take an audio surface, pictures
 * a gallery or portfolio one, events and places a magazine one, publications a
 * newsroom one), and the subject's own address chooses which profile inside
 * that family, stably, so the same subject always looks like itself.
 *
 * This is presentation only.  A profile cannot select material, admit a
 * source, change a subject or alter a word on the page.
 */
static const char *page_profile_families(BfPageMaterial material) {
    switch (material) {
    case BF_PAGE_MATERIAL_AUDIO: return "'audio','portfolio','magazine'";
    case BF_PAGE_MATERIAL_PHOTO: return "'gallery','portfolio','magazine'";
    case BF_PAGE_MATERIAL_VIDEO: return "'portfolio','magazine','editorial'";
    case BF_PAGE_MATERIAL_EVENT: return "'magazine','editorial','newsroom'";
    case BF_PAGE_MATERIAL_PLACE: return "'editorial','magazine','docs'";
    case BF_PAGE_MATERIAL_POST:  return "'letter','editorial','docs'";
    default: return "'newsroom','editorial','magazine'";
    }
}

/* One token out of the profile's own JSON, written as a CSS variable. */
#include "page_style_token.h"
static void page_profile_token(sqlite3 *db, const char *tokens, const char *key, const char *variable) {
    char value[256];
    if (page_style_value(db, tokens, key, value, sizeof(value))) printf("%s:%s;", variable, value);
}

/* --------------------------------------------------------- a territory's house
 *
 * A publication people come back to looks like itself on every band: Crooked
 * is cobalt and yellow all the way down, Puck leads on a red field and reads on
 * paper.  A front that walks through a different catalogued palette per band
 * reads as nobody's.  So when a territory has a published house (a studied
 * presentation_template whose territory_affinity names it), every band on its
 * pages takes that house, and only the band's place in a fixed rhythm changes:
 * the signature field, paper, paper, then night for pictures.  The page column
 * takes paper and its opening takes the field edge to edge. */
static const char *page_render_host;
static int page_house_band;

/* One token out of the house, read by SQLite's own JSON reader so a quoted
 * family list ("\"Unbounded\",sans-serif") survives intact. */
static void page_house_value(sqlite3 *db, const char *tokens, const char *key, char *out, size_t cap) {
    (void)page_style_value(db, tokens, key, out, cap);
}

static void page_house_token(sqlite3 *db, const char *tokens, const char *key, const char *variable) {
    char value[256];
    page_house_value(db, tokens, key, value, sizeof(value));
    if (value[0]) printf("%s:%s;", variable, value);
}

/* The band's grounds: field, paper or night, with ink and accent to match. */
static void page_house_ground(sqlite3 *db, const char *tokens, const char *band) {
    char ground[64], ink[64], accent[64], key[32];
    if (!strcmp(band, "paper")) {
        page_house_token(db, tokens, "ground", "--w-ground"); page_house_token(db, tokens, "ink", "--w-ink");
        page_house_token(db, tokens, "display", "--w-display"); page_house_token(db, tokens, "accent", "--w-accent");
        page_house_token(db, tokens, "muted", "--w-muted"); page_house_token(db, tokens, "rule", "--w-rule");
        page_house_token(db, tokens, "panel", "--w-panel");
        return;
    }
    page_house_value(db, tokens, band, ground, sizeof(ground));
    snprintf(key, sizeof(key), "%s_ink", band); page_house_value(db, tokens, key, ink, sizeof(ink));
    snprintf(key, sizeof(key), "%s_accent", band); page_house_value(db, tokens, key, accent, sizeof(accent));
    if (!ground[0] || !ink[0]) { page_house_ground(db, tokens, "paper"); return; }
    printf("--w-ground:%s;--w-ink:%s;--w-display:%s;--w-accent:%s;"
           "--w-muted:color-mix(in srgb,%s 68%%,%s);--w-rule:color-mix(in srgb,%s 24%%,transparent);"
           "--w-panel:color-mix(in srgb,%s 90%%,%s);",
           ground, ink, ink, accent[0] ? accent : ink, ink, ground, ink, ground, ink);
}

static int page_emit_house(sqlite3 *db, const char *selector, char *name_out, size_t name_cap) {
    static const char *const rhythm[] = {"field", "paper", "paper", "night"};
    sqlite3_stmt *st = NULL; const char *territory, *template, *tokens, *band;
    int whole = !strcmp(selector, ".wrap");
    char font[1100], media[32], apex[32];
    territory = page_render_host ? territory_of(page_render_host) : NULL;
    if (!territory || !strcmp(territory, "independent")) return 0;
    /* A territory house is the apex publication's default, not a theme
     * assignment to every person, merchant or institution below its DNS name.
     * Those subjects keep their own presentation selection. */
    if (snprintf(apex, sizeof(apex), "aurekai.%s", territory) >= (int)sizeof(apex) ||
        strcmp(page_render_host, apex)) return 0;
    if (sqlite3_prepare_v2(db, "SELECT template, layout_tokens FROM presentation_template"
                               " WHERE status='published' AND territory_affinity=?1 AND template LIKE 'house-%'"
                               "   AND json_valid(layout_tokens) ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, territory, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    template = (const char *)sqlite3_column_text(st, 0);
    tokens = (const char *)sqlite3_column_text(st, 1);
    if (!template || !tokens) { sqlite3_finalize(st); return 0; }
    if (name_out && name_cap) snprintf(name_out, name_cap, "%s", template);
    if (whole) page_house_band = 0;
    band = whole ? "paper" : rhythm[page_house_band++ % 4];
    page_house_value(db, tokens, "font_href", font, sizeof(font));
    if (whole && !strncmp(font, "https://fonts.googleapis.com/css2?", 34) && !strpbrk(font, "\"' ")) {
        fputs("<link rel=\"stylesheet\" data-bf-fonts href=\"", stdout); html(font); fputs("\">", stdout);
    }
    fputs("<style data-bf-source=\"presentation_template\" data-bf-profile=\"", stdout); html(template);
    fputs("\" data-bf-standing=\"published\" data-bf-band=\"", stdout); html(band); fputs("\">", stdout);
    /* The estate's shared front rules dress body, column and bands at this
     * weight; a house says the same at the same weight, later, so it wins. */
    if (whole) fputs("html body[data-character],html body[data-character] .wrap{", stdout);
    else { fputs("html body[data-character] .wrap ", stdout); fputs(selector, stdout); fputc('{', stdout); }
    page_house_ground(db, tokens, band);
    page_house_token(db, tokens, "face_display", "--w-face-display");
    page_house_token(db, tokens, "face_body", "--w-face-body");
    page_house_token(db, tokens, "face_mono", "--w-face-mono");
    page_house_token(db, tokens, "display_size", "--w-display-size");
    page_house_token(db, tokens, "display_weight", "--w-display-weight");
    page_house_token(db, tokens, "display_transform", "--w-display-transform");
    page_house_token(db, tokens, "display_tracking", "--w-display-tracking");
    page_house_token(db, tokens, "display_leading", "--w-display-leading");
    page_house_token(db, tokens, "radius", "--w-radius");
    page_house_token(db, tokens, "measure", "--w-measure");
    fputs("background:var(--w-ground);color:var(--w-ink);}", stdout);
    if (whole) {
        /* The opening is the masthead: the house's field, edge to edge. */
        fputs(".wrap>.hero{", stdout); page_house_ground(db, tokens, "field");
        fputs("background:var(--w-ground);color:var(--w-ink);box-shadow:0 0 0 100vmax var(--w-ground);clip-path:inset(0 -100vmax)}", stdout);
        fputs(".wrap>.hero h1,.wrap>.hero .lede{color:var(--w-display)}", stdout);
    }
    page_house_value(db, tokens, "media", media, sizeof(media));
    /* A duotone house prints every photograph in its own field colour, so
     * pictures from a hundred publishers read as one paper's. */
    if (!strcmp(media, "duotone") && !strcmp(band, "field")) {
        fputs("html body[data-character] .wrap ", stdout); fputs(selector, stdout);
        fputs(" img{filter:grayscale(1) contrast(1.12);mix-blend-mode:multiply}", stdout);
    }
    fputs("</style>", stdout);
    sqlite3_finalize(st);
    return 1;
}

/* The pool of profiles one kind of material can be dressed in, read once per
 * worker and kept for a few minutes.  Every region of every item page asks for
 * one; reading the table each time put the whole origin behind its own disk
 * once crawlers walked the item pages (2026-10-31: 10-17 s for a stylesheet). */
#define PAGE_PROFILE_POOL_SECONDS 300
typedef struct { char *template, *tokens, *standing; } PageProfileRow;
static struct { const char *families; time_t read_at; PageProfileRow *rows; int count; } page_profile_pools[8];
static const PageProfileRow *page_profile_pool(sqlite3 *db, BfPageMaterial dominant, int *count) {
    const char *families = page_profile_families(dominant);
    time_t now = time(NULL); sqlite3_stmt *st = NULL; char sql[512]; int slot = -1, i, cap = 0;
    PageProfileRow *rows = NULL; int n = 0;
    *count = 0;
    for (i = 0; i < 8; i++) {
        if (page_profile_pools[i].families == families) { slot = i; break; }
        if (slot < 0 && !page_profile_pools[i].families) slot = i;
    }
    if (slot < 0) slot = 0;
    if (page_profile_pools[slot].families == families &&
        now - page_profile_pools[slot].read_at < PAGE_PROFILE_POOL_SECONDS) {
        *count = page_profile_pools[slot].count; return page_profile_pools[slot].rows;
    }
    /* A published profile is one this estate has actually studied; a reference
     * candidate is a palette it has only catalogued.  Both can dress a region
     * -- the dressing is this estate's own composition, not a claim about
     * anyone's design -- but a studied one is always preferred, and the markup
     * says which standing the one it used has, so nothing here can be read as
     * observation the estate has not done. */
    snprintf(sql, sizeof(sql), "SELECT template, layout_tokens, status FROM presentation_template"
                               " WHERE status IN ('published','reference_candidate')"
                               "   AND template NOT LIKE 'house-%%'"
                               "   AND COALESCE(layout_tokens,'')<>'' AND family IN (%s)"
                               " ORDER BY (status='published') DESC, template", families);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return NULL;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *t = (const char *)sqlite3_column_text(st, 0), *k = (const char *)sqlite3_column_text(st, 1),
                   *s = (const char *)sqlite3_column_text(st, 2);
        if (n == cap) { PageProfileRow *grown = realloc(rows, (size_t)(cap = cap ? cap * 2 : 64) * sizeof(*rows)); if (!grown) break; rows = grown; }
        rows[n].template = strdup(t ? t : ""); rows[n].tokens = strdup(k ? k : ""); rows[n].standing = strdup(s ? s : "");
        if (!rows[n].template || !rows[n].tokens || !rows[n].standing) { free(rows[n].template); free(rows[n].tokens); free(rows[n].standing); break; }
        n++;
    }
    sqlite3_finalize(st);
    for (i = 0; i < page_profile_pools[slot].count; i++) {
        free(page_profile_pools[slot].rows[i].template); free(page_profile_pools[slot].rows[i].tokens); free(page_profile_pools[slot].rows[i].standing);
    }
    free(page_profile_pools[slot].rows);
    page_profile_pools[slot].families = families; page_profile_pools[slot].read_at = now;
    page_profile_pools[slot].rows = rows; page_profile_pools[slot].count = n;
    *count = n; return rows;
}

/* One of the estate's published presentation profiles, dressed onto whatever
 * selector is asked for.  The seed decides which profile: the same seed always
 * chooses the same one, so a page is stable between visits, and different
 * seeds spread the estate's profiles across a page instead of spending one
 * profile on the whole of it. */
static void page_emit_profile_for(sqlite3 *db, const char *seed, BfPageMaterial dominant,
                                  const char *selector, char *name_out, size_t name_cap) {
    char digest[65]; unsigned pick = 0; int count = 0, index = 0;
    const char *subject = seed; const PageProfileRow *pool;
    if (name_out && name_cap) name_out[0] = '\0';
    if (!db || !selector || !*selector || !cms_type_registered(db, "presentation_template")) return;
    if (page_emit_house(db, selector, name_out, name_cap)) return;
    pool = page_profile_pool(db, dominant, &count);
    if (!pool || !count) return;
    bf_sha256_hex((const uint8_t *)(subject ? subject : ""), strlen(subject ? subject : ""), digest);
    { int k; for (k = 0; k < 8; k++) pick = pick * 16 + (unsigned)(digest[k] >= 'a' ? digest[k] - 'a' + 10 : digest[k] - '0'); }
    /* Two regions carrying the same kind of material would otherwise walk the
     * same pool in the same order and often land on the same palette; the seed
     * decides where in the pool each one starts. */
    index = (int)(pick % (unsigned)count);
    {
        {
            const char *template = pool[index].template;
            const char *tokens = pool[index].tokens;
            if (name_out && name_cap) snprintf(name_out, name_cap, "%s", template ? template : "");
            const char *standing = pool[index].standing;
            /* Which catalogued palette dressed this, and at what standing: a
             * reader can check the claim against the census row. */
            fputs("<style data-bf-source=\"presentation_template\" data-bf-profile=\"", stdout);
            html(template ? template : "");
            fputs("\" data-bf-standing=\"", stdout);
            html(standing ? standing : "");
            fputs("\">", stdout);
            /* The page's dress reaches its edges: the column alone left the
             * character's own ground showing down both sides of it. */
            if (!strcmp(selector, ".wrap")) fputs("body[data-character],", stdout);
            fputs(selector, stdout);
            fputs("{", stdout);
            page_profile_token(db, tokens, "ground", "--w-ground");
            page_profile_token(db, tokens, "ink", "--w-ink");
            page_profile_token(db, tokens, "display", "--w-display");
            page_profile_token(db, tokens, "accent", "--w-accent");
            page_profile_token(db, tokens, "muted", "--w-muted");
            page_profile_token(db, tokens, "rule", "--w-rule");
            page_profile_token(db, tokens, "panel", "--w-panel");
            page_profile_token(db, tokens, "face_display", "--w-face-display");
            page_profile_token(db, tokens, "face_body", "--w-face-body");
            page_profile_token(db, tokens, "face_mono", "--w-face-mono");
            page_profile_token(db, tokens, "display_size", "--w-display-size");
            page_profile_token(db, tokens, "display_weight", "--w-display-weight");
            page_profile_token(db, tokens, "display_transform", "--w-display-transform");
            page_profile_token(db, tokens, "display_tracking", "--w-display-tracking");
            page_profile_token(db, tokens, "radius", "--w-radius");
            page_profile_token(db, tokens, "card_border", "--w-card-border");
            page_profile_token(db, tokens, "card_shadow", "--w-card-shadow");
            page_profile_token(db, tokens, "gap", "--w-gap");
            page_profile_token(db, tokens, "measure", "--w-measure");
            /* The page's own ground follows the profile, not the template's. */
            fputs("background:var(--w-ground);color:var(--w-ink);}", stdout);
            fputs("</style>", stdout);
        }
    }
}

/* ------------------------------------------------------------ scene compiler
 *
 * A scene is not an authored page.  Given whatever material a subject actually
 * has, this asks each declared reading rhythm whether the material argues for
 * it -- three placed things make a map worth drawing, two future events make
 * an agenda, recordings that play make a tracklist -- and orders the ones that
 * qualify by how strongly their own shape argues.  Nothing is written down
 * per subject: a venue with a calendar composes as an agenda, the same
 * compiler gives a photographed place a map and a gallery, and a subject with
 * one story stays one story instead of growing empty headings.
 *
 * The compiler chooses rhythms.  It cannot choose sources, admit material,
 * change a subject or invent a region: it returns a sequence of the same
 * declared regions the executor already lowers.
 */
/* A route reads each material one way: the strongest rhythm it argues for.
 * A detail page -- one publisher, one identity, one piece -- reads its
 * material in depth instead: each rhythm consumes the stream in order while
 * enough remains (lead, then index, then list), every item still read once.
 * The detail pages set this around their own composition. */
static int page_scene_in_depth;

static size_t page_compile_scene(const PageMaterial *material, unsigned char *sequence, size_t cap,
                                 char *why, size_t why_cap, time_t now) {
    typedef struct { unsigned weight; unsigned char region; BfPageMaterial material; const char *name; } SceneChoice;
    SceneChoice chosen[BF_PAGE_SCENE_GRAMMAR_COUNT];
    size_t count = 0, i, j, order[BF_PAGE_SCENE_GRAMMAR_COUNT];
    int remaining[BF_PAGE_MATERIAL_COUNT];
    size_t used = 0;
    if (why && why_cap) why[0] = '\0';
    for (i = 0; i < BF_PAGE_MATERIAL_COUNT; i++) {
        const PageStream *stream = &material->stream[i];
        remaining[i] = stream->count;
        /* An event that has already finished does not argue for an agenda. */
        if (i == BF_PAGE_MATERIAL_EVENT) {
            int k; remaining[i] = 0;
            for (k = 0; k < stream->count; k++) {
                const PageItem *item = &stream->item[k];
                time_t ends = item->duration_ms > 0 ? item->at + item->duration_ms / 1000 : item->at + 86400;
                if (item->at > 0 && ends >= now) remaining[i]++;
            }
        }
    }
    /* Strongest argument first.  A material is read once per item, not once
     * per page: the lead takes the newest story, an index the next ones and a
     * compact list what is left, each only while enough remains to argue for
     * it -- regions that share a stream consume it in order, as the declared
     * regions say.  One region still carries one rhythm. */
    for (i = 0; i < BF_PAGE_SCENE_GRAMMAR_COUNT; i++) order[i] = i;
    for (i = 1; i < BF_PAGE_SCENE_GRAMMAR_COUNT; i++)
        for (j = i; j > 0 && bf_page_scene_grammars[order[j]].weight > bf_page_scene_grammars[order[j - 1]].weight; j--) {
            size_t t = order[j]; order[j] = order[j - 1]; order[j - 1] = t;
        }
    for (i = 0; i < BF_PAGE_SCENE_GRAMMAR_COUNT; i++) {
        const BfPageSceneGrammar *g = &bf_page_scene_grammars[order[i]];
        int take;
        if (remaining[g->material] < (int)g->at_least) continue;
        for (j = 0; j < count; j++)
            if (chosen[j].region == g->region || (!page_scene_in_depth && chosen[j].material == g->material)) break;
        if (j < count) continue;
        chosen[count].weight = g->weight;
        chosen[count].region = (unsigned char)g->region;
        chosen[count].material = g->material;
        chosen[count].name = g->name;
        count++;
        take = (int)g->take < remaining[g->material] ? (int)g->take : remaining[g->material];
        remaining[g->material] -= take;
    }
    /* Except a mix, which is every other stream in one rhythm: showing it
     * spends all of them, so wherever it lands the page ends there.  It sorts
     * last however strong its argument, and gathers what the rhythms above it
     * did not use. */
    for (i = 0; i + 1 < count; i++)
        for (j = i + 1; j < count; j++) {
            int j_mixes = chosen[j].material == BF_PAGE_MATERIAL_MIX;
            int i_mixes = chosen[i].material == BF_PAGE_MATERIAL_MIX;
            if ((i_mixes && !j_mixes) || (i_mixes == j_mixes && chosen[j].weight > chosen[i].weight)) {
                SceneChoice keep = chosen[i];
                chosen[i] = chosen[j];
                chosen[j] = keep;
            }
        }
    for (i = 0; i < count && i < cap; i++) {
        sequence[i] = chosen[i].region;
        if (why && why_cap && used + 24 < why_cap)
            used += (size_t)snprintf(why + used, why_cap - used, "%s%s", used ? " " : "", chosen[i].name);
    }
    return count < cap ? count : cap;
}

/* The whole page's own dressing, for surfaces that want one look. */
static void page_emit_profile(sqlite3 *db, const char *subject, BfPageMaterial dominant, char *name_out, size_t name_cap) {
    page_emit_profile_for(db, subject, dominant, ".wrap", name_out, name_cap);
}

#include "native_experience_composition.h"
static int page_front_shown;   /* set once a front's own program has led; see page_front_use */
static int page_emit_regions(PageMaterial *material, const unsigned char *sequence, size_t sequence_count,
                             const PageEmitContext *ctx) {
    size_t i, count = sequence ? sequence_count : BF_PAGE_REGION_COUNT;
    int shown = 0, experience_open = 0;
    const BfNativeExperienceProgram *experience =
        ctx && !ctx->preview ? bf_native_experience_find(ctx->experience_pattern) : NULL;
    for (i = 0; i < count; i++) {
        size_t index = sequence ? sequence[i] : i;
        const BfPageRegion *region;
        PageStream *stream;
        int available, take;
        if (index >= BF_PAGE_REGION_COUNT) continue;
        region = &bf_page_regions[index];
        stream = &material->stream[region->material];
        available = stream->count - stream->cursor;
        if (available < (int)region->min_items) continue;
        /* A front whose own program already led does not lead twice. */
        if (page_front_shown && (region->topology == BF_PAGE_TOPOLOGY_LEAD || !strcmp(region->role, "stage"))) continue;
        /* A lead is the newest thing, so it must be able to say when; an
         * undated item leads only with a picture or its own words to lead with. */
        if (region->topology == BF_PAGE_TOPOLOGY_LEAD && !stream->item[stream->cursor].at &&
            !stream->item[stream->cursor].image[0] && !stream->item[stream->cursor].body[0]) continue;
        take = available < (int)region->max_items ? available : (int)region->max_items;
        switch (region->topology) {
        case BF_PAGE_TOPOLOGY_WALL: case BF_PAGE_TOPOLOGY_GRID: case BF_PAGE_TOPOLOGY_FIELD: case BF_PAGE_TOPOLOGY_MAP:
        case BF_PAGE_TOPOLOGY_SHELF: case BF_PAGE_TOPOLOGY_TRACKS: case BF_PAGE_TOPOLOGY_FEED:
            page_take_turns(stream); break;
        default: break;
        }
        if (experience && !experience_open) {
            bf_native_experience_begin(stdout,experience);
            experience_open=1;
        }
        {
            /* Every region down the page is dressed in a profile of its own,
             * chosen for the material it carries: a wall of photographs is
             * dressed as a gallery, the recordings under it as a sleeve, the
             * stories under those as a newsroom.  A long page is therefore a
             * walk through the estate's published presentations rather than
             * one look repeated, and the seed keeps each region's choice the
             * same between visits. */
            char seed[256], selector[64];
            snprintf(seed, sizeof(seed), "%s|%s|%u", ctx && ctx->route ? ctx->route : "", region->role, (unsigned)i);
            snprintf(selector, sizeof(selector), ".region-dress-%u", (unsigned)i);
            page_emit_profile_for(ctx ? ctx->db : NULL, seed, region->material, selector, NULL, 0);
            printf("<section class=\"region region-dress-%u region-", (unsigned)i);
        }
        html(region->role); fputs("\"", stdout);
        if (experience_open) bf_native_experience_region(stdout,experience,index,i);
        {
            PageCompatSceneRegion *surface_region =
                ctx && ctx->surface_scene ? page_surface_scene_record(ctx->surface_scene, region, i) : NULL;
            if (surface_region) {
                fputs(" data-bonfyre-region=\"", stdout); html(surface_region->id);
                fputs("\" data-bonfyre-continuity=\"", stdout); html(surface_region->continuity);
                fputs("\"", stdout);
            }
        }
        if (region->label[0]) { fputs(" aria-label=\"", stdout); html(region->label); fputs("\"><h2 class=\"region-label\">", stdout); html(region->label); fputs("</h2>", stdout); }
        else fputs(">", stdout);
        if (experience_open) bf_native_experience_focus(stdout,experience);
        page_emit_region(region, stream, take, ctx);
        fputs("</section>", stdout);
        stream->cursor += take; shown += take;
        /* The mix is the same items in another order; consuming it consumes
         * them, so a later region on the page never repeats one.  That is why
         * a mix belongs at the foot of a page: everything after it is empty by
         * construction, and a page that opens with one is a page of one
         * region however many it named. */
        if (region->material == BF_PAGE_MATERIAL_MIX) {
            int k;
            for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) if (k != BF_PAGE_MATERIAL_MIX) material->stream[k].cursor = material->stream[k].count;
        }
    }
    if (experience_open) bf_native_experience_end(stdout,experience);
    return shown;
}

/* The authored template's head still says what the character is; the page's
 * own title and description say what the subject is. */
static void page_emit_head(const char *template, const char *head_end, const char *title, const char *lede) {
    const char *cursor = template;
    const char *t0 = strstr(template, "<title>"), *t1 = t0 ? strstr(t0, "</title>") : NULL;
    const char *d0 = strstr(template, "<meta name=\"description\""), *d1 = d0 ? strchr(d0, '>') : NULL;
    if (t0 && t1 && t1 < head_end) {
        fwrite(cursor, 1, (size_t)(t0 - cursor), stdout);
        fputs("<title>", stdout); html(title); fputs("</title>", stdout);
        cursor = t1 + strlen("</title>");
    }
    if (d0 && d1 && d0 >= cursor && d1 < head_end) {
        fwrite(cursor, 1, (size_t)(d0 - cursor), stdout);
        fputs("<meta name=\"description\" content=\"", stdout); html(lede); fputs("\">", stdout);
        cursor = d1 + 1;
    }
    fwrite(cursor, 1, (size_t)(head_end - cursor), stdout);
    {
        /* The commons that keep this estate's pictures, named early so a
         * browser can open those connections while it is still reading the
         * page rather than after it meets the first picture.  These are the
         * hosts the composition table already declares; nothing is fetched
         * here and no other host is contacted. */
        size_t k, said = 0;
        for (k = 0; k < BF_PAGE_PICTURE_ORIGIN_COUNT && said < 3; k++) {
            const char *host = bf_page_picture_origins[k].host;
            size_t seen;
            for (seen = 0; seen < k; seen++)
                if (!strcmp(bf_page_picture_origins[seen].host, host)) break;
            if (seen < k) continue;
            fputs("<link rel=\"preconnect\" href=\"https://", stdout); html(host);
            fputs("\" crossorigin><link rel=\"dns-prefetch\" href=\"https://", stdout); html(host);
            fputs("\">", stdout);
            said++;
        }
    }
}

/* Primary page lowering.  The template contributes only its authored visual
 * character (the head/style block); the composer contributes the subject's
 * title, its tabs, and the regions its material supports.  No raw source JSON,
 * chain text, browser renderer or machine vocabulary crosses this boundary:
 * what the page cannot be read to imply travels as metadata, not as copy. */

typedef struct {
    const char *title, *lede, *character, *template_name, *cannot, *sources;
    const char *world;              /* the addressed World, or NULL for a page program */
    const char *requested_grammar;  /* a nested World cut, or "" */
    const char *route;              /* a page program's own route, or NULL */
    const char *territory;          /* the territory a front page speaks for, or NULL */
    char *grammars;                 /* authored tab list (mutable), or NULL */
    const unsigned char *sequence; size_t sequence_count;
    const char *from, *stream, *q;  /* a person's narrowing of the page */
    const BfPageProgram *program;   /* the program, for a front page's sections */
    const char *actor;              /* the signed-in person, or NULL for a public page */
    const char *self_path;          /* where sign-in should return to */
    PageMaterial *prefilled;        /* material an owner already supplied, e.g. saved items */
    const char *const *phase_sections;  /* the hour's own order of the routes below, or NULL */
    size_t phase_section_count;
    const char *raw_query;          /* what the person asked of this route, as written */
    const char *subject_address; /* exact owner-published subject, if resolved */
} PageDocument;

/* A dynamic World or subject page is composed from admitted identity and
 * rendered native material, never from the last path component or a mock
 * donor. Exact apex route programs still take precedence. */
static int page_experience_subject_is(const char *subject, const char *prefix) {
    return subject && prefix && !strncmp(subject,prefix,strlen(prefix));
}
static const char *page_native_experience_pattern(const PageDocument *doc, const PageMaterial *material) {
    const char *exact, *subject, *grammar;
    unsigned publications=0,media=0,places=0,posts=0;
    if (!doc) return NULL;
    exact=bf_route_presentation_pattern(page_render_host,doc->route,doc->raw_query);
    if (exact) return exact;
    subject=doc->subject_address;
    if (!(doc->world && *doc->world) && !(subject && *subject)) return NULL;
    grammar=doc->requested_grammar;
    if (grammar && *grammar) {
        if (!strcmp(grammar,"watch") || !strcmp(grammar,"listen") || !strcmp(grammar,"film") ||
            !strcmp(grammar,"music") || !strcmp(grammar,"cinema")) return "media";
        if (!strcmp(grammar,"community") || !strcmp(grammar,"feed")) return "feed";
        if (!strcmp(grammar,"board") || !strcmp(grammar,"records")) return "directory";
        if (!strcmp(grammar,"map") || !strcmp(grammar,"places") || !strcmp(grammar,"territory")) return "spatial";
        if (!strcmp(grammar,"stage")) return "discover";
    }
    if (page_experience_subject_is(subject,"person:")) return "profile";
    if (page_experience_subject_is(subject,"organization:") ||
        page_experience_subject_is(subject,"organisation:") ||
        page_experience_subject_is(subject,"institution:")) return "organization";
    if (page_experience_subject_is(subject,"community:") ||
        page_experience_subject_is(subject,"channel:")) return "feed";
    if (page_experience_subject_is(subject,"recording:") ||
        page_experience_subject_is(subject,"program:")) return "media";
    if (page_experience_subject_is(subject,"publication:") ||
        page_experience_subject_is(subject,"book:")) return "reader";
    if (page_experience_subject_is(subject,"listing:") ||
        page_experience_subject_is(subject,"offer:")) return "market";
    if (page_experience_subject_is(subject,"property:")) return "detail";
    if (material) {
        publications=(unsigned)material->stream[BF_PAGE_MATERIAL_PUBLICATION].count;
        media=(unsigned)(material->stream[BF_PAGE_MATERIAL_VIDEO].count+
                         material->stream[BF_PAGE_MATERIAL_AUDIO].count);
        places=(unsigned)material->stream[BF_PAGE_MATERIAL_PLACE].count;
        posts=(unsigned)material->stream[BF_PAGE_MATERIAL_POST].count;
        if (publications && (media || places || posts)) return "discover";
        if (media && !publications && !places && !posts) return "media";
        if (places && !publications && !media && !posts) return "spatial";
        if (posts && !publications && !media && !places) return "feed";
        if (publications && !media && !places && !posts) return "reader";
    }
    return doc->world ? "detail" : NULL;
}

static void page_surface_scene_digest(const PageDocument *doc, const PageCompatScene *scene, char out[65]) {
    BfSha256 sha; unsigned char digest[32]; char owner_fingerprint[65], material[32]; size_t i;
    const char *archive = getenv("BONFYRE_ITEM_ARCHIVE_DB");
    const char *frontier = getenv("BONFYRE_SOURCE_FRONTIER_DB");
    const char *fabric = getenv("BONFYRE_SUBJECT_FABRIC_DB");
    const char *scope = doc && doc->actor && *doc->actor ? "self" : "public";
    public_projection_fingerprint(owner_fingerprint, archive, frontier, fabric);
    bf_sha256_init(&sha);
#define PAGE_SCENE_HASH_TEXT(value) do { \
        const char *value_ = (value); /* arrays decay first: no -Waddress */ \
        if (!value_) value_ = ""; \
        bf_sha256_update(&sha, (const uint8_t *)value_, strlen(value_)); \
        bf_sha256_update(&sha, (const uint8_t *)"\0", 1); \
    } while (0)
    PAGE_SCENE_HASH_TEXT(owner_fingerprint);
    PAGE_SCENE_HASH_TEXT(doc ? doc->route : "");
    PAGE_SCENE_HASH_TEXT(doc ? doc->world : "");
    PAGE_SCENE_HASH_TEXT(doc ? doc->requested_grammar : "");
    PAGE_SCENE_HASH_TEXT(doc ? doc->territory : "");
    PAGE_SCENE_HASH_TEXT(doc ? doc->raw_query : "");
    PAGE_SCENE_HASH_TEXT(doc ? doc->subject_address : "");
    PAGE_SCENE_HASH_TEXT(scope);
    for (i = 0; scene && i < scene->count; i++) {
        PAGE_SCENE_HASH_TEXT(scene->region[i].id);
        PAGE_SCENE_HASH_TEXT(scene->region[i].role);
        PAGE_SCENE_HASH_TEXT(scene->region[i].continuity);
        snprintf(material, sizeof(material), "%u", scene->region[i].material);
        PAGE_SCENE_HASH_TEXT(material);
    }
#undef PAGE_SCENE_HASH_TEXT
    bf_sha256_final(&sha, digest);
    bf_sha256_digest_hex(digest, out);
}

static void page_emit_surface_scene(const PageDocument *doc, const PageCompatScene *scene) {
    char digest[65], subject[512], dependency[512]; size_t i;
    const char *scope = doc && doc->actor && *doc->actor ? "self" : "public";
    const char *route = doc && doc->route ? doc->route : "";
    const char *world = doc && doc->world ? doc->world : "";
    if (doc && doc->subject_address && *doc->subject_address)
        snprintf(subject,sizeof(subject),"%s",doc->subject_address);
    else if (world[0]) snprintf(subject, sizeof(subject), "world:%s", world);
    else snprintf(subject, sizeof(subject), "route:%s:%s",
                  doc && doc->territory && *doc->territory ? doc->territory : "public",
                  route[0] ? route : "/");
    page_surface_scene_digest(doc, scene, digest);

    /* An inert template is deliberate: PageComposition does not mount the
     * runtime. It is a machine-readable compatibility hand-off beside the
     * complete, readable server document; an owned ApplicationBinding may
     * later bind the projected canonical module over these declared regions. */
    fputs("<template id=\"bonfyre-surface-scene\" data-schema=\"bonfyre-surface-scene.v1\">{\"schema\":\"bonfyre-surface-scene.v1\",\"source_digest\":", stdout);
    json(digest);
    fputs(",\"focus\":{\"subject_address\":", stdout); json(subject);
    fputs(",\"world\":", stdout); json(world);
    fputs(",\"site_id\":\"\",\"actor_scope\":", stdout); json(scope);
    fputs(",\"frontier\":\"\",\"intent\":\"read\"},\"lenses\":[\"READ\"],", stdout);
    fputs("\"world_material\":{\"compatibility_adapter\":\"PageComposition\",\"initial_markup\":\"server\",\"complete_without_script\":true},", stdout);
    fputs("\"slots\":[{\"slot_id\":\"page-main\",\"extent\":\"page\",\"accepts\":[\"ComposedPublicPage\"],\"max_items\":1,\"htmx_boundary\":\"\"}],", stdout);
    fputs("\"fragments\":[", stdout);
    for (i = 0; scene && i < scene->count; i++) {
        const PageCompatSceneRegion *region = &scene->region[i];
        if (i) putchar(',');
        fputs("{\"fragment_id\":", stdout); json(region->id);
        fputs(",\"surface_grammar\":\"READ\",\"renderer_family\":\"PageComposition\",\"renderer_variant\":", stdout); json(region->role);
        fputs(",\"extent\":\"section\",\"continuity_group\":", stdout); json(region->continuity);
        fputs(",\"source_projection\":\"\",\"media_posture\":\"\",\"temporal_posture\":\"\",\"action_aperture\":\"\"}", stdout);
    }
    fputs("],\"bindings\":[],\"stages\":[],\"renderers\":[],\"patterns\":[],\"presentation\":[],", stdout);
    fputs("\"operations\":{\"apertures\":[],\"object_contracts\":[],\"proposal_contracts\":[]},", stdout);
    fputs("\"interaction\":{\"profile_id\":\"page-composition-compatibility\",\"default_lens\":\"READ\",\"lens_order\":[\"READ\"],", stdout);
    fputs("\"semantic_zoom\":[{\"resolution\":0,\"name\":\"page\"},{\"resolution\":1,\"name\":\"region\"}],\"interaction_depths\":[\"focus\"],", stdout);
    fputs("\"context_rail\":{},\"relation_halo\":{\"enabled\":false},\"command_aperture\":{},", stdout);
    fputs("\"transition_policy\":{\"object_permanence\":true,\"view_transition\":\"auto\"},\"mobile_posture\":{},\"desktop_posture\":{},", stdout);
    fputs("\"live_update_policy\":{\"mode\":\"SurfaceDelta\",\"transport\":\"document\"}},", stdout);
    fputs("\"runtime_queries\":{\"compatibility_adapter\":\"PageComposition\",\"route\":", stdout); json(route); fputs("},", stdout);
    fputs("\"dependencies\":[\"compat:page-composition\"", stdout);
    if (route[0]) { snprintf(dependency, sizeof(dependency), "compat:route:%s", route); putchar(','); json(dependency); }
    if (world[0]) { snprintf(dependency, sizeof(dependency), "cms:world:%s", world); putchar(','); json(dependency); }
    fputs("],\"cannot_infer\":\"compatibility Scene is derived interaction state over server-rendered PageComposition material; it is not source truth, identity, authority, availability, price, proof, commitment, presence or effect state\"}</template>", stdout);
}

static void page_emit_world_tabs(sqlite3 *db, const PageDocument *doc) {
    /* Tabs are the authored grammar list followed by any other grammar a
     * published fragment opens.  A published way into the page that has
     * no tab would be reachable only by typing its address. */
    char tabs[24][64]; int tab_count = 0, t;
    char *part = doc->grammars;
    sqlite3_stmt *fragments = NULL;
    while (part && *part) {
        char *comma = strchr(part, ',');
        if (comma) *comma = '\0';
        while (*part && isspace((unsigned char)*part)) part++;
        { char *tail = part + strlen(part); while (tail > part && isspace((unsigned char)tail[-1])) *--tail = '\0'; }
        if (*part && strlen(part) < sizeof(tabs[0]) && tab_count < 24) snprintf(tabs[tab_count++], sizeof(tabs[0]), "%s", part);
        part = comma ? comma + 1 : NULL;
    }
    if (cms_type_registered(db, "world_fragment") &&
        sqlite3_prepare_v2(db, "SELECT surface_grammar FROM world_fragment WHERE world=?1 AND status='published' ORDER BY ordinal,id",
                           -1, &fragments, NULL) == SQLITE_OK) {
        sqlite3_bind_text(fragments, 1, doc->world, -1, SQLITE_TRANSIENT);
        while (tab_count < 24 && sqlite3_step(fragments) == SQLITE_ROW) {
            const char *grammar = (const char *)sqlite3_column_text(fragments, 0);
            if (grammar && *grammar && strlen(grammar) < sizeof(tabs[0])) snprintf(tabs[tab_count++], sizeof(tabs[0]), "%s", grammar);
        }
        sqlite3_finalize(fragments);
    }
    for (t = 0; t < tab_count; t++) {
        char nested_route[320]; const char *route, *label = page_grammar_label(tabs[t]); int k, repeat = 0;
        for (k = 0; k < t; k++) if (!strcmp(tabs[k], tabs[t])) repeat = 1;
        route = label && !repeat ? world_nested_grammar_route(doc->world, tabs[t], nested_route, sizeof(nested_route)) : NULL;
        if (route) { fputs("<a href=\"", stdout); html(route); page_emit_reading_query(doc->raw_query, NULL, NULL); fputs("\">", stdout); html(label); fputs("</a>", stdout); }
    }
}


static const BfPageProgram *page_program_for_route(const char *route) {
    size_t i;
    for (i = 0; route && i < BF_PAGE_PROGRAM_COUNT; i++)
        if (!strcmp(bf_page_programs[i].route, route) && strcmp(route, "/")) return &bf_page_programs[i];
    return NULL;
}

/* ---- which channels a surface may draw on ---------------------------------
 *
 * The estate lists far more sources than any page can carry, so the question
 * is not "which rows come first" but "which admitted channels have waited
 * longest for a reader, without letting one publisher own the page".  The
 * source-frontier ledger holds that: every admitted channel, the census row a
 * page reads it by, when it last reached a person and how often.
 *
 * Two shares are enforced here rather than described: no single publisher may
 * hold more than an eighth of a surface's sources, and no single protocol more
 * than a third.  Where the ledger is absent or thin, the declared families
 * still fill the rest, so a page is never emptier for the ledger existing.
 */
static sqlite3 *page_frontier_open(void) {
    const char *path = getenv("BONFYRE_SOURCE_FRONTIER_DB");
    return bf_sqlite_open_readonly_path(path, 1500);
}

static int page_csv_has(const char *list, const char *value);
static const char *page_render_host;

static int page_share_full(char *taken, size_t cap, const char *value, unsigned allowance) {
    const char *p = taken; unsigned seen = 0; size_t used;
    if (!value || !*value) return 0;
    for (; *p; ) {
        const char *end = strchr(p, ',');
        size_t n = end ? (size_t)(end - p) : strlen(p);
        if (n == strlen(value) && !strncmp(p, value, n)) seen++;
        if (!end) break;
        p = end + 1;
    }
    if (seen >= allowance) return 1;
    used = strlen(taken);
    snprintf(taken + used, cap - used, "%s%s", used ? "," : "", value);
    return 0;
}

/* The expensive question -- which of the estate's channels belong to this
 * addressed subject -- is compiled ahead of time. Request time only applies
 * the same diversity/freshness projection over that exact subject cut.
 * A missing index is compatibility, not authority: the caller may use the
 * older global PageProgram selector only when this returns zero. */
static sqlite3 *page_subject_fabric_open(void) {
    const char *path = getenv("BONFYRE_SUBJECT_FABRIC_DB");
    return bf_sqlite_open_readonly_path(path, 1000);
}
static int page_subject_reads_fabric(const char *subject);


/* -1 means no compiled bundle; zero means an authoritative empty cut. */
static int page_subject_fabric_sources(sqlite3 *db, const char *subject,
        const BfPageProgram *program, unsigned limit, char *sources, size_t cap) {
    sqlite3_stmt *st = NULL, *bundle = NULL;
    char roots[2048] = "", protocols[1024] = "";
    size_t used = 0, k; unsigned count = 0, per_root, per_protocol;
    sources[0] = '\0';
    if (!program || !db || !subject[0]) return -1;
    if (sqlite3_prepare_v2(db,
          "SELECT 1 FROM subject_bundle WHERE subject=?1 AND scope='public' LIMIT 1",
          -1, &bundle, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(bundle, 1, subject, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(bundle) != SQLITE_ROW) {
        sqlite3_finalize(bundle); return -1;
    }
    sqlite3_finalize(bundle);
    if (!page_subject_reads_fabric(subject)) return -1;
    if (limit > program->source_limit) limit = program->source_limit;
    per_root = limit / 8 ? limit / 8 : 1;
    per_protocol = limit / 3 ? limit / 3 : 1;
    {
        /* A share exists so one publisher cannot fill a page that belongs to a
         * territory.  On a publisher's own address there is nothing to share
         * with: the subject is that one publisher, and a per-root share turned
         * its front page into a single item. */
        sqlite3_stmt *roots_here = NULL; int distinct_roots = 0;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(DISTINCT root_id) FROM subject_reader"
                                   " WHERE subject=?1 AND scope='public'", -1, &roots_here, NULL) == SQLITE_OK) {
            sqlite3_bind_text(roots_here, 1, subject, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(roots_here) == SQLITE_ROW) distinct_roots = sqlite3_column_int(roots_here, 0);
        }
        sqlite3_finalize(roots_here);
        if (distinct_roots <= 1) { per_root = limit; per_protocol = limit; }
    }
    if (sqlite3_prepare_v2(db,
          "SELECT census_source,root_id,protocol FROM subject_reader "
          "WHERE subject=?1 AND scope='public' AND state IN ('admitted','active') "
          "AND census_source<>'' AND material=?2 "
          "ORDER BY (last_contributed='') DESC,last_contributed ASC,contributions ASC,first_seen ASC,census_source ASC LIMIT 800",
          -1, &st, NULL) != SQLITE_OK) return 0;
    for (k = 0; k < program->family_count && count < limit; k++) {
        unsigned taken = 0, share = program->family_count ? limit / (unsigned)program->family_count : limit;
        if (share < 1) share = 1;
        sqlite3_reset(st); sqlite3_clear_bindings(st);
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, program->families[k], -1, SQLITE_STATIC);
        while (count < limit && taken < share && sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            const char *root = (const char *)sqlite3_column_text(st, 1);
            const char *protocol = (const char *)sqlite3_column_text(st, 2);
            int n;
            if (!source || !*source || strchr(source, ',') || page_csv_has(sources, source)) continue;
            if (page_share_full(roots, sizeof(roots), root, per_root)) continue;
            if (page_share_full(protocols, sizeof(protocols), protocol, per_protocol)) continue;
            n = snprintf(sources + used, cap - used, "%s%s", used ? "," : "", source);
            if (n < 0 || used + (size_t)n >= cap) break;
            used += (size_t)n; count++; taken++;
        }
    }
    sqlite3_finalize(st); return (int)count;
}

static int page_compiled_material_kind(const char *name) {
    if (!name) return -1;
    if (!strcmp(name, "publication") || !strcmp(name, "story")) return BF_PAGE_MATERIAL_PUBLICATION;
    if (!strcmp(name, "video")) return BF_PAGE_MATERIAL_VIDEO;
    if (!strcmp(name, "photo")) return BF_PAGE_MATERIAL_PHOTO;
    if (!strcmp(name, "place")) return BF_PAGE_MATERIAL_PLACE;
    if (!strcmp(name, "audio")) return BF_PAGE_MATERIAL_AUDIO;
    if (!strcmp(name, "post")) return BF_PAGE_MATERIAL_POST;
    if (!strcmp(name, "event")) return BF_PAGE_MATERIAL_EVENT;
    return -1;
}

static void page_compiled_text(char *out, size_t cap, sqlite3_stmt *st, int col) {
    const unsigned char *value = sqlite3_column_text(st, col);
    size_t size;
    const unsigned char *terminator;
    if (!out || !cap) return;
    if (!value) { out[0] = '\0'; return; }
    size = (size_t)sqlite3_column_bytes(st, col);
    if (size >= cap) size = cap - 1;
    terminator = memchr(value, '\0', size);
    if (terminator) size = (size_t)(terminator - value);
    memcpy(out, value, size);
    out[size] = '\0';
}

/* One route render reads one subject projection generation.  Keep the fabric
 * connection and resolved subject together so material, regions and editorial
 * contract cannot each pay for a fresh lookup/open (or see different file
 * generations during an atomic index replacement). */
typedef struct {
    sqlite3 *db;
    char subject[384];
} PageProjectionRead;

static int page_projection_read_open(PageProjectionRead *read, sqlite3 *cms, const char *host) {
    if (!read->subject[0] && !page_subject_for_host(cms, host, read->subject, sizeof(read->subject))) return 0;
    if (read->db) return 1;
    if (!page_subject_reads_fabric(read->subject)) return 0;
    read->db = page_subject_fabric_open();
    return read->db != NULL;
}

static void page_projection_read_close(PageProjectionRead *read) {
    if (read->db) sqlite3_close(read->db);
    read->db = NULL;
}

/* The route compiler owns the editorial contract that travelled with an exact
 * material cut.  Request time may use that contract, but it must remain
 * compatible with an older SubjectFabric index which only carried route
 * membership.  A missing/newer column is therefore a compatibility miss, not
 * a reason to manufacture replacement editorial state at request time. */
typedef struct {
    char territory[32];
    char title[512];
    char lede[2048];
    char character[128];
    char cannot[2048];
} PageProjectionContract;

static int page_subject_projection_contract(PageProjectionRead *read,
        const char *route, PageProjectionContract *contract) {
    sqlite3 *db = read->db;
    sqlite3_stmt *st = NULL;
    int found = 0;

    if (!contract || !route || !db) return 0;
    memset(contract, 0, sizeof(*contract));
    if (sqlite3_prepare_v2(db,
          "SELECT territory,title,lede,character,cannot_infer FROM subject_route "
          "WHERE subject=?1 AND scope='public' AND route=?2 LIMIT 1",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, read->subject, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, route, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            page_compiled_text(contract->territory, sizeof(contract->territory), st, 0);
            page_compiled_text(contract->title, sizeof(contract->title), st, 1);
            page_compiled_text(contract->lede, sizeof(contract->lede), st, 2);
            page_compiled_text(contract->character, sizeof(contract->character), st, 3);
            page_compiled_text(contract->cannot, sizeof(contract->cannot), st, 4);
            found = 1;
        }
    }
    sqlite3_finalize(st);
    return found;
}

/* Subject Projection is the request-ready material cut. Once this row exists,
 * request time must not rediscover source membership or reopen retained bodies.
 * -1 means unavailable and permits compatibility fallback; zero is an
 * authoritative empty compiled route. */
static void page_projection_item_from_row(PageItem *item, sqlite3_stmt *st, int kind) {
    memset(item, 0, sizeof(*item));
    item->order = (unsigned)sqlite3_column_int(st, 0);
    page_compiled_text(item->source, sizeof(item->source), st, 1);
    page_compiled_text(item->key, sizeof(item->key), st, 2);
    page_compiled_text(item->title, sizeof(item->title), st, 4);
    page_compiled_text(item->by, sizeof(item->by), st, 5);
    page_compiled_text(item->note, sizeof(item->note), st, 6);
    page_compiled_text(item->href, sizeof(item->href), st, 7);
    page_compiled_text(item->group, sizeof(item->group), st, 8);
    page_compiled_text(item->image, sizeof(item->image), st, 9);
    page_compiled_text(item->asset, sizeof(item->asset), st, 10);
    page_compiled_text(item->poster, sizeof(item->poster), st, 11);
    page_compiled_text(item->body, sizeof(item->body), st, 12);
    page_compiled_text(item->ident, sizeof(item->ident), st, 13);
    page_compiled_text(item->about, sizeof(item->about), st, 14);
    page_compiled_text(item->rights, sizeof(item->rights), st, 15);
    page_compiled_text(item->rights_href, sizeof(item->rights_href), st, 16);
    page_compiled_text(item->origin, sizeof(item->origin), st, 17);
    page_compiled_text(item->origin_href, sizeof(item->origin_href), st, 18);
    page_compiled_text(item->people, sizeof(item->people), st, 19);
    page_compiled_text(item->where, sizeof(item->where), st, 20);
    page_compiled_text(item->transcript_href, sizeof(item->transcript_href), st, 21);
    page_compiled_text(item->chapters_href, sizeof(item->chapters_href), st, 22);
    page_compiled_text(item->talk_href, sizeof(item->talk_href), st, 23);
    item->at = (time_t)sqlite3_column_int64(st, 24);
    item->duration_ms = (long)sqlite3_column_int64(st, 25);
    item->placed = sqlite3_column_int(st, 26) ? 1 : 0;
    item->lon = sqlite3_column_double(st, 27);
    item->lat = sqlite3_column_double(st, 28);
    item->kind = kind;
}

static int page_subject_projection_material(PageProjectionRead *read,
        const char *route, PageMaterial *material) {
    sqlite3 *db = read->db;
    sqlite3_stmt *exists = NULL, *st = NULL;
    int loaded = 0;

    if (!material || !route || !db) return -1;
    if (sqlite3_prepare_v2(db,
          "SELECT 1 FROM subject_route WHERE subject=?1 AND scope='public' AND route=?2 LIMIT 1",
          -1, &exists, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(exists, 1, read->subject, -1, SQLITE_STATIC);
    sqlite3_bind_text(exists, 2, route, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(exists) != SQLITE_ROW) {
        sqlite3_finalize(exists);
        return -1;
    }
    sqlite3_finalize(exists);

    memset(material, 0, sizeof(*material));
    /* Route membership is deliberately compact: an item body belongs once to
     * SubjectMaterial, however many routes (or regions) reference it.  Older
     * restored indexes carried a denormalized route row, so retain that read
     * solely as a compatibility fallback while the staged compiler converges
     * every active index on the compact relation. */
    if (sqlite3_prepare_v2(db,
          "SELECT r.ordinal,m.source,m.item_key,m.material,m.title,m.byline,m.publisher,m.href,m.group_name,"
          "m.image,m.asset,m.poster,m.body,m.ident,m.about,m.rights,m.rights_href,m.origin,m.origin_href,m.people,"
          "m.where_text,m.transcript_href,m.chapters_href,m.talk_href,m.at,m.duration_ms,m.placed,m.lon,m.lat "
          "FROM subject_route_item r JOIN subject_material m ON "
          "m.subject=r.subject AND m.scope=r.scope AND m.source=r.source AND m.item_key=r.item_key AND m.material=r.material "
          "WHERE r.subject=?1 AND r.scope='public' AND r.route=?2 ORDER BY r.ordinal LIMIT 4096",
          -1, &st, NULL) != SQLITE_OK &&
        sqlite3_prepare_v2(db,
          "SELECT ordinal,source,item_key,material,title,byline,publisher,href,group_name,"
          "image,asset,poster,body,ident,about,rights,rights_href,origin,origin_href,people,"
          "where_text,transcript_href,chapters_href,talk_href,at,duration_ms,placed,lon,lat "
          "FROM subject_route_item WHERE subject=?1 AND scope='public' AND route=?2 "
          "ORDER BY ordinal LIMIT 4096",
          -1, &st, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(st, 1, read->subject, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, route, -1, SQLITE_TRANSIENT);

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *material_name = (const char *)sqlite3_column_text(st, 3);
        int kind = page_compiled_material_kind(material_name);
        PageStream *stream;
        PageItem *item;
        if (kind < 0 || kind >= BF_PAGE_MATERIAL_COUNT) continue;
        stream = &material->stream[kind];
        if (stream->count >= PAGE_STREAM_MAX) continue;
        item = &stream->item[stream->count++];
        page_projection_item_from_row(item, st, kind);

        if ((kind == BF_PAGE_MATERIAL_PUBLICATION || kind == BF_PAGE_MATERIAL_VIDEO ||
             kind == BF_PAGE_MATERIAL_PHOTO || kind == BF_PAGE_MATERIAL_AUDIO) &&
            material->stream[BF_PAGE_MATERIAL_MIX].count < PAGE_STREAM_MAX) {
            material->stream[BF_PAGE_MATERIAL_MIX]
                .item[material->stream[BF_PAGE_MATERIAL_MIX].count++] = *item;
        }
        loaded++;
    }
    sqlite3_finalize(st);
    page_finalize_material(material);
    /* A compiled route with nothing in it is not this subject having nothing:
     * its own sources (the archive the projection does not read) still answer. */
    return loaded ? loaded : -1;
}

/* An item reached from a compiled public route keeps the same bounded
 * normalized material on its own page.  Membership is checked through the
 * exact route relation: subject_material alone includes admitted items that
 * no route selected and is not a public item-page authorization. */
/* A bundle precomputed with no readers at all (a territory precomputed
 * where its demand store was not present) is a precompute that had nothing
 * to read, not that subject having nothing: it composes from its program
 * sources, as an uncompiled subject does.  Every other compiled subject
 * answers from the fabric. */
static int page_subject_reads_fabric(const char *subject) {
    sqlite3 *db = page_subject_fabric_open(); sqlite3_stmt *st = NULL; int empty_bundle = 0;
    if (!db) return 0;
    if (sqlite3_prepare_v2(db, "SELECT EXISTS(SELECT 1 FROM subject_bundle WHERE subject=?1 AND scope='public') "
                               "AND NOT EXISTS(SELECT 1 FROM subject_reader WHERE subject=?1 AND scope='public')", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) empty_bundle = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); sqlite3_close(db);
    return !empty_bundle;
}

static int page_peertube_embed(const char *href, char *out, size_t cap);
static int page_has_playable_video(const PageItem *item) {
    char embed[1100];
    return item && item->kind == BF_PAGE_MATERIAL_VIDEO &&
        (page_image_url_safe(item->asset) || page_peertube_embed(item->href, embed, sizeof(embed)) ||
         (!item->asset[0] && page_embed_for(item->href, embed, sizeof(embed))));
}

static int page_subject_projection_item(sqlite3 *cms, const char *host,
        const char *source, const char *key, PageItem *item,
        char *family, size_t family_cap) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL, *gate = NULL;
    char subject[384];
    int found = 0, authoritative = 0;
    if (!item || !source || !key || !page_subject_for_host(cms, host, subject, sizeof(subject)) ||
        !page_subject_reads_fabric(subject) || !(db = page_subject_fabric_open())) return 0;
    if (sqlite3_prepare_v2(db,
          "SELECT 1 FROM subject_projection_build b WHERE b.build_id=1 AND EXISTS("
          "SELECT 1 FROM subject_route r WHERE r.subject=?1 AND r.scope='public') LIMIT 1",
          -1, &gate, NULL) == SQLITE_OK) {
        sqlite3_bind_text(gate, 1, subject, -1, SQLITE_TRANSIENT);
        authoritative = sqlite3_step(gate) == SQLITE_ROW;
    }
    sqlite3_finalize(gate);
    if (sqlite3_prepare_v2(db,
          "SELECT r.ordinal,m.source,m.item_key,m.material,m.title,m.byline,m.publisher,m.href,m.group_name,"
          "m.image,m.asset,m.poster,m.body,m.ident,m.about,m.rights,m.rights_href,m.origin,m.origin_href,m.people,"
          "m.where_text,m.transcript_href,m.chapters_href,m.talk_href,m.at,m.duration_ms,m.placed,m.lon,m.lat,m.family "
          "FROM subject_route_item r JOIN subject_material m ON "
          "m.subject=r.subject AND m.scope=r.scope AND m.source=r.source AND m.item_key=r.item_key AND m.material=r.material "
          "WHERE r.subject=?1 AND r.scope='public' AND r.source=?2 AND r.item_key=?3 "
          "GROUP BY m.material ORDER BY m.material LIMIT 16",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, key, -1, SQLITE_TRANSIENT);
        int best = -1;
        while (sqlite3_step(st) == SQLITE_ROW) {
            int kind = page_compiled_material_kind((const char *)sqlite3_column_text(st, 3));
            PageItem candidate; char embed[1100]; int score = 0;
            if (kind < 0 || kind >= BF_PAGE_MATERIAL_COUNT) continue;
            page_projection_item_from_row(&candidate, st, kind);
            if (kind == BF_PAGE_MATERIAL_PUBLICATION) score = 2;
            else if (kind == BF_PAGE_MATERIAL_EVENT) score = 1;
            else if (kind == BF_PAGE_MATERIAL_PHOTO && page_image_url_safe(candidate.image)) score = 4;
            else if (kind == BF_PAGE_MATERIAL_VIDEO && page_has_playable_video(&candidate)) score = 5;
            else if (kind == BF_PAGE_MATERIAL_AUDIO &&
                     (page_image_url_safe(candidate.asset) || (!candidate.asset[0] && page_embed_for(candidate.href, embed, sizeof(embed))))) score = 3;
            if (score > best) {
                *item = candidate;
                if (family && family_cap) page_compiled_text(family, family_cap, st, 29);
                found = 1; best = score;
            }
        }
    }
    sqlite3_finalize(st);
    st = NULL;
    /* A compiled item selected for another subject is not a legacy global
     * feed item on this host, even when this host has no compiled route yet. */
    if (!found && !authoritative && sqlite3_prepare_v2(db,
          "SELECT 1 FROM subject_route_item WHERE scope='public' AND source=?1 AND item_key=?2 LIMIT 1",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, key, -1, SQLITE_TRANSIENT);
        authoritative = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return found ? 1 : authoritative ? -1 : 0;
}

static int page_subject_projection_regions(PageProjectionRead *read,
        const char *route, unsigned char *sequence, size_t cap) {
    sqlite3 *db = read->db;
    sqlite3_stmt *st = NULL;
    int count = 0;
    if (!sequence || !cap || !route || !db) return -1;
    if (sqlite3_prepare_v2(db,
          "SELECT role FROM subject_route_region "
          "WHERE subject=?1 AND scope='public' AND route=?2 ORDER BY ordinal",
          -1, &st, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(st, 1, read->subject, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, route, -1, SQLITE_TRANSIENT);
    while ((size_t)count < cap && sqlite3_step(st) == SQLITE_ROW) {
        const char *role = (const char *)sqlite3_column_text(st, 0);
        size_t i;
        if (!role) continue;
        for (i = 0; i < BF_PAGE_REGION_COUNT; i++) {
            if (!strcmp(bf_page_regions[i].role, role)) {
                sequence[count++] = (unsigned char)i;
                break;
            }
        }
    }
    sqlite3_finalize(st);
    return count;
}

/* How far down the estate's publishers this page starts.  Zero is the front
 * page of a route; every step past it is the same route reading publishers it
 * has not shown yet, which is what lets a route go on for as long as the
 * estate has material rather than for as long as one screen. */
static unsigned page_source_offset;

static unsigned page_frontier_sources(const BfPageProgram *program, unsigned limit, char *sources, size_t cap) {
    sqlite3 *db = page_frontier_open(), *archive = NULL;
    sqlite3_stmt *st = NULL, *archived = NULL;
    char roots[4096] = "", protocols[1024] = "";
    /* A route length is a slice of selections, not of one family query.  The
     * ledger remembers candidates crossed while replaying earlier lengths, so
     * a source carried by two families cannot re-enter when SQL resets. */
    char *walked = NULL;
    size_t walked_used = 0, walked_cap = 512 * 1024;
    size_t used = strlen(sources), k; unsigned count = 0, per_root, per_protocol, pass, skip;
    if (!db || !(walked = calloc(1, walked_cap))) { if (db) sqlite3_close(db); return 0; }
    /* Admission may arrive before a source has yielded a retained item.  It
     * has no honest item link yet, so leave it in the ledger but do not spend
     * a finite visible source window on an empty promise.  When the archive
     * is unavailable the compatibility selector remains usable. */
    if ((archive = page_archive_open(0))) {
        if (sqlite3_prepare_v2(archive, "SELECT 1 FROM item WHERE source=?1 LIMIT 1", -1, &archived, NULL) != SQLITE_OK) {
            sqlite3_close(archive);
            archive = NULL;
        }
    }
    /* A share is only a limit when there is something to share with.  An
     * estate whose admitted channels are nearly all feeds was throttling every
     * page to a third of its sources, because a cap meant to stop one protocol
     * dominating was being applied where only one protocol existed. */
    {
        sqlite3_stmt *count = NULL; int publishers = 0, protocols = 0;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(DISTINCT root_id), COUNT(DISTINCT protocol) FROM channel"
                                   " WHERE state IN ('admitted','active') AND census_source<>''", -1, &count, NULL) == SQLITE_OK &&
            sqlite3_step(count) == SQLITE_ROW) {
            publishers = sqlite3_column_int(count, 0);
            protocols = sqlite3_column_int(count, 1);
        }
        sqlite3_finalize(count);
        per_root = publishers > 0 ? (limit + publishers - 1) / (unsigned)publishers : limit;
        if (per_root < limit / 8) per_root = limit / 8;
        per_protocol = protocols > 0 ? (limit + protocols - 1) / (unsigned)protocols : limit;
        if (per_protocol < limit / 3) per_protocol = limit / 3;
    }
    if (per_root < 1) per_root = 1;
    if (per_protocol < 1) per_protocol = 1;
    /* A channel is drawn for what it demonstrably publishes.  Most of this
     * estate's channels are filed under their publisher's main business, and a
     * measured carriage says which of them also hand out recordings, films or
     * pictures; a ledger written before that measurement existed is read the
     * old way rather than refused. */
    /* Do not cap this cursor before page_source_offset is applied below.  A
     * route length is an offset into one stable admitted ordering; LIMIT 400
     * made every source after that prefix unreachable no matter how many
     * continuation pages the route and static exporter were prepared to walk.
     * The page still takes at most program->source_limit sources, and the
     * route-number guard remains 400, so this restores reachability without
     * turning one request into an unbounded page. */
    if (sqlite3_prepare_v2(db, "SELECT census_source, root_id, protocol FROM channel"
                               " WHERE state IN ('admitted','active') AND census_source<>''"
                               " AND (material=?1 OR instr(','||COALESCE(carries,'')||',', ','||?1||',')>0)"
                               " ORDER BY (material=?1) DESC, (last_contributed='') DESC, last_contributed ASC, contributions ASC, first_seen ASC, state, rowid",
                           -1, &st, NULL) != SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT census_source, root_id, protocol FROM channel"
                               " WHERE state IN ('admitted','active') AND census_source<>'' AND material=?1"
                               " ORDER BY (last_contributed='') DESC, last_contributed ASC, contributions ASC, first_seen ASC, state, rowid",
                           -1, &st, NULL) != SQLITE_OK) {
        sqlite3_finalize(archived); if (archive) sqlite3_close(archive);
        free(walked); sqlite3_close(db); return 0;
    }
    /* Two passes: every family the route names gets its share first, so a page
     * carries each of them; then whatever is left of the page is filled from
     * the families that still have channels waiting.  Without the second pass
     * an estate whose admitted channels are mostly one family showed a
     * fraction of what it had, because each family was capped at its share
     * whether or not the others could use theirs. */
    /* Four passes, each relaxing one thing:
     *   0  every family the route names takes its share, under both shares
     *   1  the page is filled from any family, still under both shares
     *   2  the protocol share is lifted, because a page half empty for the
     *      sake of protocol variety is worse than a page of feeds
     *   3  the publisher share is lifted too, rather than leave a page thin
     * Diversity is preferred and then yielded, in that order, so an estate
     * whose admitted channels are nearly all one kind still fills its pages. */
    /* Replay prior selections through their exact family/root/protocol
     * allowances.  The offset spans the whole selected ledger, never a
     * per-family raw row cursor. */
    skip = page_source_offset;
    for (pass = 0; pass < 4 && count < limit; pass++)
    for (k = 0; k < program->family_count && count < limit; k++) {
        unsigned taken = 0, share = pass == 0 && program->family_count ? limit / (unsigned)program->family_count : limit;
        if (share < 1) share = 1;
        sqlite3_reset(st); sqlite3_clear_bindings(st);
            sqlite3_bind_text(st, 1, program->families[k], -1, SQLITE_STATIC);
        while (count < limit && taken < share && sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            const char *root = (const char *)sqlite3_column_text(st, 1);
            const char *protocol = (const char *)sqlite3_column_text(st, 2);
            int n;
            size_t source_n;
            if (!source || !*source || strchr(source, ',')) continue;
            if (archived) {
                sqlite3_reset(archived); sqlite3_clear_bindings(archived);
                sqlite3_bind_text(archived, 1, source, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(archived) != SQLITE_ROW) continue;
            }
            source_n = strlen(source);
            if (source_n + 2 > walked_cap - walked_used || page_csv_has(walked, source)) continue;
            if (pass < 3 && page_share_full(roots, sizeof(roots), root, per_root)) continue;
            if (pass < 2 && page_share_full(protocols, sizeof(protocols), protocol, per_protocol)) continue;
            /* A source excluded by a strict early diversity pass is allowed
             * to be considered again when that pass is deliberately relaxed. */
            memcpy(walked + walked_used, source, source_n);
            walked_used += source_n;
            walked[walked_used++] = ',';
            walked[walked_used] = '\0';
            /* A skipped source still consumes the allowance it consumed on
             * its earlier page; otherwise page N starts beside page N-1. */
            if (skip) { skip--; taken++; continue; }
            if (page_csv_has(sources, source)) continue;
            n = snprintf(sources + used, cap - used, "%s%s", used ? "," : "", source);
            if (n < 0 || used + (size_t)n >= cap) break;
            used += (size_t)n; count++; taken++;
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    sqlite3_finalize(archived);
    if (archive) sqlite3_close(archive);
    free(walked);
    return count;
}

/* Families fill a source cap in their declared order, a share at a time, so a
 * page carries every family it names instead of whichever rows came first. */
static unsigned page_program_sources(sqlite3 *db, const BfPageProgram *program, unsigned limit, char *sources, size_t cap) {
    sqlite3_stmt *st = NULL; size_t used; unsigned count, share, pass, skipped_for_offset; size_t k;
    sources[0] = '\0';
    if (!program) return 0;
    if (limit > program->source_limit) limit = program->source_limit;
    count = page_frontier_sources(program, limit, sources, cap);
    used = strlen(sources);
    if (count >= limit) return count;
    if (!db || !cms_type_registered(db, "internet_source")) return count;
    if (sqlite3_prepare_v2(db, "SELECT source FROM internet_source WHERE status='published' AND COALESCE(twin,'')<>'' "
          "AND (family=?1 OR instr(','||COALESCE(carries,'')||',', ','||?1||',')>0) "
          "GROUP BY source ORDER BY (MIN(family)=?1) DESC, MIN(id)", -1, &st, NULL) != SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT source FROM internet_source WHERE status='published' AND COALESCE(twin,'')<>'' "
          "AND family=?1 GROUP BY source ORDER BY MIN(id)", -1, &st, NULL) != SQLITE_OK) return count;
    share = program->family_count ? limit / (unsigned)program->family_count : 1;
    if (share < 1) share = 1;
    for (pass = 0; pass < 2 && count < limit; pass++)
        for (k = 0; k < program->family_count && count < limit; k++) {
            unsigned taken = 0, skipped = 0;
            sqlite3_reset(st); sqlite3_clear_bindings(st);
            sqlite3_bind_text(st, 1, program->families[k], -1, SQLITE_STATIC);
            skipped_for_offset = page_source_offset;
            while (count < limit && sqlite3_step(st) == SQLITE_ROW) {
                const char *source = (const char *)sqlite3_column_text(st, 0);
                int n;
                if (!source || !*source || strchr(source, ',')) continue;
                if (skipped_for_offset) { skipped_for_offset--; continue; }
                if (pass == 0 && taken >= share) break;
                if (pass == 1 && skipped < share) { skipped++; continue; }
                if (page_csv_has(sources, source)) continue;
                n = snprintf(sources + used, cap - used, "%s%s", used ? "," : "", source);
                if (n < 0 || used + (size_t)n >= cap) break;
                used += (size_t)n; count++; taken++;
            }
        }
    sqlite3_finalize(st);
    return count;
}

static int page_contains_ci(const char *haystack, const char *needle) {
    size_t n = needle ? strlen(needle) : 0;
    const char *p;
    if (!n) return 1;
    for (p = haystack ? haystack : ""; *p; p++) if (!strncasecmp(p, needle, n)) return 1;
    return 0;
}

/* A person's narrowing keeps only what matches, in every stream at once. */
static void page_filter(PageMaterial *material, const char *from, const char *q) {
    int s, k, w;
    if ((!from || !*from) && (!q || !*q)) return;
    for (s = 0; s < BF_PAGE_MATERIAL_COUNT; s++) {
        PageStream *stream = &material->stream[s];
        for (k = 0, w = 0; k < stream->count; k++) {
            const PageItem *item = &stream->item[k];
            int keep = 1;
            if (from && *from) keep = !strcasecmp(item->by, from) || !strcasecmp(item->note, from) || !strcasecmp(item->group, from);
            if (keep && q && *q) keep = page_contains_ci(item->title, q) || page_contains_ci(item->by, q) || page_contains_ci(item->body, q) || page_contains_ci(item->note, q);
            if (keep) { if (w != k) stream->item[w] = stream->item[k]; w++; }
        }
        stream->count = w;
    }
}

/* High-claim routes need a stronger admission than a source family.  `gaming`
 * says how a channel entered the estate; it cannot make a discussion thread a
 * release.  The additional route/source contracts are authored in
 * page-composition.cue and compiled into the same bounded header as the page
 * programs.  This is selection only: it does not rewrite an item's kind,
 * promote an occurrence, or infer that two publishers are the same. */
static int page_source_host_suffix(const char *host, size_t host_len, const char *suffix) {
    size_t suffix_len;
    if (!host || !suffix || !(suffix_len = strlen(suffix)) || host_len < suffix_len) return 0;
    if (host_len == suffix_len) return !strncasecmp(host, suffix, host_len);
    return host[host_len - suffix_len - 1] == '.' &&
           !strncasecmp(host + host_len - suffix_len, suffix, suffix_len);
}

/* Terms in a source contract are words, not arbitrary byte substrings.
 * `prelaunch` is not an asserted `launch`, and treating it as one is exactly
 * the kind of accidental semantic promotion this gate exists to prevent. */
static int page_title_has_contract_term(const char *title, const char *term) {
    const char *at;
    size_t n = term ? strlen(term) : 0;
    if (!title || !term || !n) return 0;
    for (at = title; *at; at++) {
        if (strncasecmp(at, term, n)) continue;
        if (at != title && isalnum((unsigned char)at[-1])) continue;
        if (isalnum((unsigned char)at[n])) continue;
        return 1;
    }
    return 0;
}

static int page_route_source_contract_permits(const char *route, const char *territory,
                                              const char *host, size_t host_len,
                                              const char *title) {
    size_t i, term;
    for (i = 0; i < BF_PAGE_ROUTE_SOURCE_CONTRACT_COUNT; i++) {
        const BfPageRouteSourceContract *contract = &bf_page_route_source_contracts[i];
        if (strcmp(route ? route : "", contract->route) ||
            (strcmp(contract->territory, "any") && strcmp(territory ? territory : "", contract->territory)) ||
            !page_source_host_suffix(host, host_len, contract->host_suffix)) continue;
        if (!contract->title_term_count) return 1;
        for (term = 0; term < contract->title_term_count; term++)
            if (page_title_has_contract_term(title, contract->title_terms[term])) return 1;
    }
    return 0;
}

/* The archive item deliberately retains its short, stable source coordinate.
 * The source's URL remains in the canonical CMS row, so resolve that bounded
 * coordinate once per page source before applying a route contract. */
static int page_route_source_allowed(sqlite3 *db, const PageDocument *doc,
                                     const char *source, const char *title) {
    sqlite3_stmt *st = NULL;
    const char *url, *host, *path;
    size_t host_len;
    int allowed = 0;
    if (!doc || !doc->program || !doc->route ||
        (strcmp(doc->route, "/releases") && strcmp(doc->route, "/advisories") &&
         (strcmp(doc->route, "/") || strcmp(doc->territory ? doc->territory : "", "work")))) return 1;
    if (!db || !source || !*source ||
        sqlite3_prepare_v2(db,
            "SELECT url FROM internet_source WHERE source=?1 AND status='published' "
            "ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && (url = (const char *)sqlite3_column_text(st, 0)) &&
        page_https_host_path(url, &host, &host_len, &path))
        allowed = page_route_source_contract_permits(doc->route, doc->territory, host, host_len, title);
    sqlite3_finalize(st);
    return allowed;
}

/* Every stream is filtered independently because a mix is a presentation
 * copy of the other streams.  Removing only the publication stream would let
 * an ineligible forum item re-enter through the mixed feed later on. */
static void page_filter_route_contract(sqlite3 *db, const PageDocument *doc, PageMaterial *material) {
    int stream, at, kept;
    if (!doc || !doc->program || !material || !doc->route) return;
    if (!strcmp(doc->route, "/watch") || !strcmp(doc->route, "/board")) {
        int watch = !strcmp(doc->route, "/watch");
        for (stream = 0; stream < BF_PAGE_MATERIAL_COUNT; stream++) {
            PageStream *items = &material->stream[stream];
            if (stream != (watch ? BF_PAGE_MATERIAL_VIDEO : BF_PAGE_MATERIAL_PHOTO) && stream != BF_PAGE_MATERIAL_MIX) {
                items->count = items->cursor = 0;
                continue;
            }
            for (at = 0, kept = 0; at < items->count; at++) {
                PageItem *item = &items->item[at];
                if (watch ? !page_has_playable_video(item) :
                    (!page_is_photo(item) || !page_image_url_safe(item->image))) continue;
                if (kept != at) items->item[kept] = *item;
                kept++;
            }
            items->count = kept;
            if (items->cursor > kept) items->cursor = kept;
        }
        return;
    }
    if ((strcmp(doc->route, "/releases") && strcmp(doc->route, "/advisories") &&
         (strcmp(doc->route, "/") || strcmp(doc->territory ? doc->territory : "", "work")))) return;
    for (stream = 0; stream < BF_PAGE_MATERIAL_COUNT; stream++) {
        PageStream *items = &material->stream[stream];
        for (at = 0, kept = 0; at < items->count; at++) {
            PageItem *item = &items->item[at];
            if (!page_route_source_allowed(db, doc, item->source, item->title)) continue;
            if (kept != at) items->item[kept] = *item;
            kept++;
        }
        items->count = kept;
        if (items->cursor > kept) items->cursor = kept;
    }
}

static int page_stage_focus(PageMaterial *material, const char *focus_ref) {
    PageItem focus;
    sqlite3 *atlas = NULL;
    sqlite3_stmt *st = NULL;
    char source[96], key[25], address[160];
    char related[32][48];
    int related_count = 0, s, k, w, found = 0;
    const char *slash, *atlas_path;

    if (!material || !focus_ref || !(slash = strchr(focus_ref, '/'))) return 0;
    if ((size_t)(slash - focus_ref) >= sizeof(source) || strlen(slash + 1) >= sizeof(key)) return 0;
    snprintf(source, sizeof(source), "%.*s", (int)(slash - focus_ref), focus_ref);
    snprintf(key, sizeof(key), "%s", slash + 1);

    memset(&focus, 0, sizeof(focus));
    for (k = 0; k < material->stream[BF_PAGE_MATERIAL_EVENT].count; k++) {
        PageItem *item = &material->stream[BF_PAGE_MATERIAL_EVENT].item[k];
        if (!strcmp(item->source, source) && !strcmp(item->key, key)) {
            focus = *item; found = 1; break;
        }
    }
    if (!found) return 0;

    if (focus.ident[0]) snprintf(related[related_count++], sizeof(related[0]), "%s", focus.ident);
    if (focus.about[0] && related_count < 32)
        snprintf(related[related_count++], sizeof(related[0]), "%s", focus.about);

    atlas_path = getenv("BONFYRE_LATENT_ATLAS_DB");
    if (atlas_path && *atlas_path &&
        sqlite3_open_v2(atlas_path, &atlas, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        snprintf(address, sizeof(address), "o/%s/%s", source, key);
        if (sqlite3_prepare_v2(atlas,
            "SELECT CASE WHEN src=?1 THEN dst ELSE src END "
            "FROM relation WHERE (src=?1 OR dst=?1) "
            "AND (src LIKE 'qid:%' OR dst LIKE 'qid:%') LIMIT 64",
            -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
            while (related_count < 32 && sqlite3_step(st) == SQLITE_ROW) {
                const char *v = (const char *)sqlite3_column_text(st, 0);
                int seen = 0, r;
                if (!v) continue;
                if (!strncmp(v, "qid:", 4)) v += 4;
                if (!*v || strlen(v) >= sizeof(related[0])) continue;
                for (r = 0; r < related_count; r++)
                    if (!strcmp(related[r], v)) { seen = 1; break; }
                if (!seen) snprintf(related[related_count++], sizeof(related[0]), "%s", v);
            }
        }
    }
    sqlite3_finalize(st);
    if (atlas) sqlite3_close(atlas);

    for (s = 0; s < BF_PAGE_MATERIAL_COUNT; s++) {
        PageStream *stream = &material->stream[s];
        for (k = 0, w = 0; k < stream->count; k++) {
            PageItem *item = &stream->item[k];
            int keep = !strcmp(item->source, source) && !strcmp(item->key, key);
            int r;
            /* Where both things state coordinates, the coordinates decide:
             * a place name is what an organizer typed, and a whole calendar
             * often carries one venue's name on events across a county. */
            if (!keep && focus.placed && item->placed) {
                double dx = (item->lon - focus.lon) *
                            cos((item->lat + focus.lat) * 0.008726646259971648);
                double dy = item->lat - focus.lat;
                if (sqrt(dx * dx + dy * dy) * 111195.0 <= 2000.0) keep = 1;
            } else if (!keep && focus.where[0] && item->where[0] &&
                       !strcasecmp(focus.where, item->where)) keep = 1;
            if (!keep) for (r = 0; r < related_count; r++) {
                if ((item->ident[0] && !strcmp(item->ident, related[r])) ||
                    (item->about[0] && !strcmp(item->about, related[r]))) {
                    keep = 1; break;
                }
            }
            if (keep) {
                if (w != k) stream->item[w] = stream->item[k];
                w++;
            }
        }
        stream->count = w;
        stream->cursor = 0;
    }
    return 1;
}



/* The territory of the request in hand, set once where the host is read, so a
 * page can mark its place in the bar without a script. */
static char estate_request_territory[24] = "";

static void page_emit_nav(const PageDocument *doc) {
    static const struct { const char *territory, *label, *glyph; } places[] = {
        {"live","Live","world"},{"life","Life","life"},{"shop","Shop","shop"},{"work","Work","work"}
    };
    size_t i, p;
    /* The same bar as every other page family (BF_ESTATE_BAR), printed here
     * because this renderer knows the place and the reader without a script. */
    fputs(BF_ESTATE_BAR_HEAD "<header class=\"ak-bar\"><div class=\"ak-bar-top\"><a class=\"ak-brand\" href=\"/\">Aurekai</a>"
          "<nav class=\"ak-places\" aria-label=\"Territories\">", stdout);
    for (p = 0; p < sizeof(places) / sizeof(places[0]); p++) {
        fputs("<a href=\"https://aurekai.", stdout); html(places[p].territory); fputs("/\"", stdout);
        /* A front page names its territory; any other page stands in the one the request came to. */
        if (!strcmp(doc->territory ? doc->territory : estate_request_territory, places[p].territory)) fputs(" aria-current=\"page\"", stdout);
        fputs(">", stdout); page_glyph(places[p].glyph); fputs("<span>", stdout); html(places[p].label); fputs("</span></a>", stdout);
    }
    fputs("</nav>", stdout);
    /* Search is a form, so it works before and without any script. */
    fputs("<form class=\"ak-find\" action=\"", stdout);
    html(doc->world && doc->route ? doc->route : "/search");
    fputs("\" method=\"get\" role=\"search\">", stdout);
    if (doc->world) {
        if (doc->from && *doc->from) { fputs("<input type=\"hidden\" name=\"from\" value=\"", stdout); html(doc->from); fputs("\">", stdout); }
        if (doc->stream && *doc->stream) { fputs("<input type=\"hidden\" name=\"s\" value=\"", stdout); html(doc->stream); fputs("\">", stdout); }
    }
    fputs("<label>", stdout); page_glyph("search");
    fputs("<input type=\"search\" name=\"q\" placeholder=\"Search\" aria-label=\"Search\" value=\"", stdout); html(doc->q ? doc->q : "");
    fputs("\"></label></form>", stdout);
    /* A person is either signed in, and their saved things are one tap away,
     * or not, and signing in brings them straight back here. */
    fputs("<div class=\"ak-session-slot\" data-account-slot>",stdout);
    /* The page's resolved actor also selects private cache headers. A later
     * successful session lookup must not personalize a declared public body. */
    account_navigation_emit(doc->actor&&*doc->actor?page_request_bearer:NULL,page_request_auth_db);
    fputs("</div>",stdout);
    fputs("</div><nav class=\"ak-sections\" aria-label=\"Sections\">", stdout);
    for (i = 0; !doc->world && i < BF_PAGE_NAV_COUNT; i++) {
        fputs("<a href=\"", stdout); html(bf_page_nav[i].route); fputs("\"", stdout);
        if (doc->route && !strcmp(doc->route, bf_page_nav[i].route)) fputs(" aria-current=\"page\"", stdout);
        fputs(">", stdout); page_glyph(page_nav_glyph(bf_page_nav[i].route)); fputs("<span>", stdout); html(bf_page_nav[i].label); fputs("</span></a>", stdout);
    }
    /* The two places a visitor arriving from an offer is looking for. */
    fputs(BF_ESTATE_BAR_OUT "</nav>", stdout);
    fputs(BF_ACCOUNT_STYLE BF_ACCOUNT_REFRESH "</header>" BF_HOME_CONTINUATION, stdout);
}

static int page_open_template(const char *path, char **template, char **head_end) {
    FILE *f = NULL; long size;
    *template = NULL;
    if (!(f = fopen(path, "rb")) || fseek(f, 0, SEEK_END) || (size = ftell(f)) < 1 ||
        size > 2 * 1024 * 1024 || fseek(f, 0, SEEK_SET) ||
        !(*template = malloc((size_t)size + 1)) ||
        fread(*template, 1, (size_t)size, f) != (size_t)size) { if (f) fclose(f); free(*template); *template = NULL; return 0; }
    fclose(f); (*template)[size] = '\0';
    if (!(*head_end = strstr(*template, "</head>"))) { free(*template); *template = NULL; return 0; }
    return 1;
}

static void page_collect_stories(sqlite3 *db, const char *sources, PageMaterial *material);

/* A SourceDepth coordinate is a maintained current relation over the frontier,
 * not a request to rediscover every retained body in the estate.  The generic
 * page composer remains the owner of ordinary /channels material, but these
 * explicit ledger cuts lower directly to SourceDepth below.  This is both the
 * public route contract and the static compiler's exact dependency cut. */
static int page_query_has_key(const char *query, const char *key) {
    size_t key_len = key ? strlen(key) : 0;
    const char *p = query;
    if (!key_len) return 0;
    while (p && *p) {
        if ((!p || p == query || p[-1] == '&') && !strncmp(p, key, key_len) && p[key_len] == '=') return 1;
        p = strchr(p, '&');
        if (p) p++;
    }
    return 0;
}

static int page_is_source_depth_cut(const PageDocument *doc) {
    const char *query;
    if (!doc || !doc->route || strcmp(doc->route, "/channels") || !(query = doc->raw_query) || !*query) return 0;
    return page_query_has_key(query, "channel") || page_query_has_key(query, "root") ||
           page_query_has_key(query, "roots") || page_query_has_key(query, "way") ||
           page_query_has_key(query, "publishes") || page_query_has_key(query, "standing");
}

/* These directories are maintained relational cuts over retained evidence.
 * Their renderers read the archive/frontier directly below; hydrating every
 * retained body before that would make one credited-name address pay for the
 * entire estate and then discard it. */
static int page_is_directory_cut(const PageDocument *doc) {
    return doc && doc->route &&
           (!strcmp(doc->route, "/people") || !strcmp(doc->route, "/organisations"));
}

/* What a front's own program already showed.  The sections under it preview
 * other routes, and those routes hold the same stories; each item the program
 * used is struck from every section's material so nothing appears twice on
 * one front.  An item is known by its page key, or by its link when the
 * source gave it none. */
static int page_front_shown;
static char page_front_used[384][96];
static int page_front_used_count;
static void page_front_use(const PageItem *item) {
    const char *id;
    if (!item || page_front_used_count >= (int)(sizeof(page_front_used) / sizeof(page_front_used[0]))) return;
    id = item->key[0] ? item->key : item->href[0] ? item->href : item->title;
    if (!*id) return;
    snprintf(page_front_used[page_front_used_count++], sizeof(page_front_used[0]), "%s", id);
}
static int page_front_was_used(const PageItem *item) {
    const char *id = item->key[0] ? item->key : item->href[0] ? item->href : item->title;
    int i;
    for (i = 0; *id && i < page_front_used_count; i++) if (!strncmp(page_front_used[i], id, sizeof(page_front_used[0]) - 1)) return 1;
    return 0;
}
static void page_front_drop_used(PageMaterial *material) {
    size_t k; int i, kept;
    if (!material || !page_front_used_count) return;
    for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) {
        PageStream *stream = &material->stream[k];
        for (i = stream->cursor, kept = stream->cursor; i < stream->count; i++) {
            if (page_front_was_used(&stream->item[i])) continue;
            if (kept != i) stream->item[kept] = stream->item[i];
            kept++;
        }
        stream->count = kept;
    }
}

#include "page_preview_region.h"
/* A front page section previews another route in that route's own rhythm,
 * with its words as the heading and the heading as the way in. */
static void page_emit_section(sqlite3 *db, PageProjectionRead *projection, const char *route, time_t now) {
    const BfPageProgram *program = page_program_for_route(route);
    PageMaterial *material; char sources[4096];
    PageDocument route_cut = {0};
    const BfPageRegion *region = NULL;
    PageEmitContext ctx;
    PageProjectionRead local_projection = {0};
    unsigned char compiled_regions[32];
    int projected, compiled_region_count = -1;
    if (!program || !(material = calloc(1, sizeof(*material)))) return;
    memset(&ctx, 0, sizeof(ctx));
    ctx.route = program->route; ctx.stream = NULL; ctx.query = NULL; ctx.preview = 1; ctx.now = now; ctx.db = db;
    if (!projection || !projection->db) {
        page_projection_read_open(&local_projection, db, page_render_host);
        projection = &local_projection;
    }
    projected = page_subject_projection_material(projection, program->route, material);
    if (projected < 0) {
        if (page_subject_fabric_sources(projection->db, projection->subject, program, 8, sources, sizeof(sources)) < 0)
            page_program_sources(db, program, 8, sources, sizeof(sources));
        page_collect_stories(db, sources, material);
    }
    /* A front page preview is the route's own material cut in miniature.  It
     * cannot bypass a route's source/form contract simply because it is being
     * rendered as a section rather than at the route's address. */
    route_cut.route = program->route;
    route_cut.program = program;
    page_filter_route_contract(db, &route_cut, material);
    page_front_drop_used(material);
    if (projected < 0) region = page_preview_region(material, program->regions, program->region_count);
    else {
        compiled_region_count = page_subject_projection_regions(
            projection, program->route, compiled_regions, sizeof(compiled_regions));
        region = page_preview_region(material, compiled_regions, compiled_region_count > 0 ? (size_t)compiled_region_count : 0);
    }
    page_projection_read_close(&local_projection);
    if (region) {
        PageStream *stream = &material->stream[region->material];
        static const struct { BfPageTopology topology; int count; } previews[] = {
            {BF_PAGE_TOPOLOGY_FEED, 8}, {BF_PAGE_TOPOLOGY_WALL, 10}, {BF_PAGE_TOPOLOGY_GRID, 4},
            {BF_PAGE_TOPOLOGY_COMMUNITIES, 30}, {BF_PAGE_TOPOLOGY_TABLE, 6}, {BF_PAGE_TOPOLOGY_MAP, 40},
            {BF_PAGE_TOPOLOGY_THREAD, 30}, {BF_PAGE_TOPOLOGY_INDEX, 6}, {BF_PAGE_TOPOLOGY_LIST, 6}
        };
        int take = 6, available = stream->count - stream->cursor; size_t p;
        for (p = 0; p < sizeof(previews) / sizeof(previews[0]); p++) if (previews[p].topology == region->topology) take = previews[p].count;
        if (take > available) take = available;
        if (take > (int)region->max_items) take = (int)region->max_items;
        if (take > 0) {
            /* A homepage preview is a choice of publishers, not one feed's
             * backlog. Reuse the gallery's lossless source rotation after the
             * route contract and duplicate cut. Full reading routes keep their
             * time order; events, tables and conversations keep native order. */
            if (region->material != BF_PAGE_MATERIAL_EVENT &&
                region->topology != BF_PAGE_TOPOLOGY_TABLE &&
                region->topology != BF_PAGE_TOPOLOGY_THREAD)
                page_take_turns(stream);
            {
                /* A front page is a long walk down the estate, and every stop
                 * on it is dressed for what that stop actually holds: the
                 * pictures as a gallery, the recordings as a sleeve, the
                 * conversation as a letter.  The route is the seed, so a
                 * section looks the same wherever a person meets it. */
                char seed[256], selector[96];
                snprintf(seed, sizeof(seed), "section|%s|%s", program->route, region->role);
                snprintf(selector, sizeof(selector), ".section-dress-%.40s", program->route + 1);
                page_emit_profile_for(db, seed, region->material, selector, NULL, 0);
                fputs("<section class=\"section section-dress-", stdout); html(program->route + 1);
                fputs(" section-", stdout);
            }
            html(region->role); fputs("\"><header class=\"section-head\"><h2><a href=\"", stdout);
            html(program->route); fputs("\">", stdout); html(program->title); fputs("</a></h2>", stdout);
            fputs("<a class=\"see-all\" href=\"", stdout); html(program->route); fputs("\">See all</a></header>", stdout);
            page_emit_region(region, stream, take, &ctx);
            fputs("</section>", stdout);
        }
    }
    free(material);
}

/* Primary page lowering, shared by addressed Worlds and main public routes.
 * The template contributes only its authored visual character (the head and
 * style block); the composer contributes the subject's title, its tabs, and
 * the regions its material supports.  No raw source JSON, chain text, browser
 * renderer or machine vocabulary crosses this boundary: what the page cannot
 * be read to imply travels as metadata, not as copy. */
/* Another length of the same route, and the small script that spares a click.
 *
 * Nothing here composes anything: the next length is a page this estate has
 * already composed at its own address, and the script appends that page's own
 * regions.  With scripting off the link is an ordinary link and the page is
 * whole; with it on, the reading simply continues.  It fetches only this
 * estate's own addresses, appends what it received without rewriting it, and
 * stops at the end. */
static void page_emit_keep_reading(const char *route, unsigned offset, unsigned per_page) {
    unsigned next = per_page ? (offset / per_page) + 2 : 2;
    /* The home route is "/", so appending "/2" produced "//2" -- which a browser
     * reads as a protocol-relative URL and resolves to a host called "2". Every
     * home page this estate has ever served carried a "More from other
     * publishers" link pointing at https://2/, live and unnoticed, because the
     * same function is correct for every other route. */
    const char *base = (route && route[0] == '/' && route[1] == '\0') ? "" : route;
    fputs("<nav class=\"keep-reading\" data-keep-reading><a rel=\"next\" href=\"", stdout);
    html(base);
    printf("/%u\">More from other publishers</a></nav>", next);
    fputs("<script>(function(){"
          "var foot=document.currentScript.previousElementSibling,busy=false,done=false;"
          "function next(){"
          "if(busy||done||!foot)return;var link=foot.querySelector('a[rel=next]');if(!link){done=true;return;}"
          "busy=true;foot.setAttribute('data-reading','');"
          "fetch(link.getAttribute('href'),{credentials:'omit'}).then(function(r){"
          "if(!r.ok)throw 0;return r.text();}).then(function(text){"
          "var got=new DOMParser().parseFromString(text,'text/html');"
          "var more=got.querySelectorAll('main .region, main .section');"
          "if(!more.length){done=true;foot.remove();return;}"
          "var host=foot.parentNode;"
          "for(var i=0;i<more.length;i++)host.insertBefore(document.importNode(more[i],true),foot);"
          "var dress=got.querySelectorAll('style[data-bf-source]');"
          "for(var j=0;j<dress.length;j++)document.head.appendChild(document.importNode(dress[j],true));"
          "var onward=got.querySelector('.keep-reading a[rel=next]');"
          "if(onward){link.setAttribute('href',onward.getAttribute('href'));}else{done=true;foot.remove();}"
          "busy=false;foot.removeAttribute('data-reading');"
          "}).catch(function(){busy=false;done=true;foot.removeAttribute('data-reading');});}"
          /* A page with nothing further to read has no footer to observe, and
           * observing null throws -- which killed everything appended after it
           * on exactly those pages. The continuation half is guarded; the
           * apertures below are not, because every page has notes. */
          "if(foot){"
          "if('IntersectionObserver' in window){"
          "new IntersectionObserver(function(e){if(e[0].isIntersecting)next();},{rootMargin:'800px'}).observe(foot);}"
          "foot.addEventListener('click',function(e){"
          "if(e.target.tagName==='A'&&e.button===0&&!e.metaKey&&!e.ctrlKey&&!e.shiftKey&&!e.altKey&&!done){e.preventDefault();next();}});}"
          /* This estate writes what it cannot infer beside what it shows, and
           * those notes are the easiest thing on the page to scroll past.
           * Here they become openable, and the words shown are the server's
           * own -- moved, never rewritten.
           *
           * A composed page fetches no renderer, so this borrows nothing: no
           * library, no second script, no positioning engine. It is the one
           * enhancement that can be had without breaking the rule that makes
           * these pages what they are. A reader with no script still reads
           * every note, because the note is already in the page. */
          "function apertures(root){"
          "var notes=(root||document).querySelectorAll('.src,.by');"
          "for(var i=0;i<notes.length;i++){(function(note){"
          "if(note.hasAttribute('data-bf-open'))return;"
          "var words=(note.textContent||'').trim();if(words.length<40)return;"
          "note.setAttribute('data-bf-open','');note.setAttribute('tabindex','0');"
          "note.setAttribute('role','button');"
          "note.setAttribute('aria-label','what this estate cannot infer here');"
          "function show(){"
          "var was=document.querySelector('[data-bf-aperture]');if(was)was.remove();"
          "var panel=document.createElement('div');panel.setAttribute('data-bf-aperture','');"
          "panel.textContent=words;"
          "var box=note.getBoundingClientRect();"
          "panel.style.cssText='position:absolute;z-index:60;max-width:min(38rem,84vw);'"
          "+'padding:.7rem .85rem;font-size:.86rem;line-height:1.5;border-radius:2px;'"
          "+'background:var(--w-panel,#14161a);color:var(--w-ink,#f2efe9);'"
          "+'border:1px solid var(--w-rule,rgba(242,239,233,.2));'"
          "+'box-shadow:0 8px 30px rgba(0,0,0,.35);'"
          "+'left:'+Math.max(8,Math.min(box.left+window.scrollX,"
          "document.documentElement.clientWidth-360))+'px;'"
          "+'top:'+(box.bottom+window.scrollY+8)+'px';"
          "document.body.appendChild(panel);}"
          "note.addEventListener('click',show);"
          "note.addEventListener('keydown',function(e){"
          "if(e.key==='Enter'||e.key===' '){e.preventDefault();show();}});"
          "})(notes[i]);}}"
          "document.addEventListener('click',function(e){"
          "var open=document.querySelector('[data-bf-aperture]');"
          "if(open&&!open.contains(e.target)&&!e.target.hasAttribute('data-bf-open'))open.remove();});"
          "document.addEventListener('keydown',function(e){"
          "if(e.key==='Escape'){var open=document.querySelector('[data-bf-aperture]');"
          "if(open)open.remove();}});"
          /* This script runs while the document is still being parsed, so the
           * notes further down the page do not exist yet. Marking them has to
           * wait for the parse to finish; doing it inline found only whatever
           * happened to precede the script. */
          "if(document.readyState==='loading'){"
          "document.addEventListener('DOMContentLoaded',function(){apertures(document);});"
          "}else{apertures(document);}"
          "window.bfApertures=apertures;"
          "})();</script>", stdout);
}

/* Routes a front's own program already composed, so the declared sections
 * below it do not show the same material a second time. */
static const char *page_front_took[16];
static int page_front_took_count;
static void page_front_take_route(const char *route) {
    if (route && page_front_took_count < (int)(sizeof(page_front_took) / sizeof(page_front_took[0])))
        page_front_took[page_front_took_count++] = route;
}
static int page_front_route_taken(const char *route) {
    int i;
    for (i = 0; route && i < page_front_took_count; i++) if (!strcmp(page_front_took[i], route)) return 1;
    return 0;
}
#include "front_editorial.h"

static int page_emit_document(sqlite3 *db, const char *path, const PageDocument *doc,
                              PageProjectionRead *projection) {
    char *template = NULL, *head_end; PageMaterial *material = NULL; int shown;
    int narrowed = (doc->from && *doc->from) || (doc->q && *doc->q);
    int source_depth_cut = page_is_source_depth_cut(doc);
    int directory_cut = page_is_directory_cut(doc);
    PageCompatScene surface_scene = {0};
    page_offered_a_start = 0;
    PageEmitContext ctx;
    if (!page_open_template(path, &template, &head_end)) return 0;
    if (doc->prefilled) material = doc->prefilled;
    else if (!(material = calloc(1, sizeof(*material)))) { free(template); return 0; }
    else if (!source_depth_cut && !directory_cut) {
        page_collect_stories(db, doc->sources, material);
        if (doc->world) page_collect_subjects(db, doc->world, material);
    }
    page_filter(material, doc->from, doc->q);
    page_filter_route_contract(db, doc, material);
    /* A route with no composed owner material is not an empty replacement for
     * its declared native surface program. Let that program lower its own
     * CUE slots instead. The compatibility page wins only when it has actual
     * retained material to carry; a quiet addressed World still says so here
     * because it has no alternate native route. */
    if (doc->route && native_surface_program_route(doc->route)) {
        size_t material_kind; int has_material = 0;
        for (material_kind = 0; material_kind < BF_PAGE_MATERIAL_COUNT; material_kind++)
            if (material->stream[material_kind].count) { has_material = 1; break; }
        if (!has_material && !source_depth_cut && !narrowed) {
            free(template); if (material != doc->prefilled) free(material);
            return 0;
        }
    }
    memset(&ctx, 0, sizeof(ctx));
    ctx.route = doc->route; ctx.stream = doc->stream; ctx.query = doc->raw_query; ctx.preview = 0; ctx.now = time(NULL); ctx.db = db;
    ctx.surface_scene = &surface_scene;
    ctx.experience_pattern = page_native_experience_pattern(doc,material);
    /* A page that knows who is reading it is theirs alone and is never shared
     * by a cache. */
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: %s\r\n"
           "Vary: Host, Accept, Cookie\r\nX-Bonfyre-World-Projection: native\r\nConnection: close\r\n\r\n",
           doc->actor && *doc->actor ? "private, no-store" : front_community_route(page_render_host,doc->route) ?
           "public, no-cache, max-age=0, must-revalidate" : "public, max-age=60, stale-while-revalidate=300");
    const FrontBrandCut *brand_head=front_brand_public_host(page_render_host,doc->route);
    page_emit_head(template, head_end, brand_head?brand_head->headline:doc->title,
                   brand_head?brand_head->description:doc->lede);
    if(brand_head){
        fputs("<meta property=\"og:type\" content=\"website\"><meta property=\"og:title\" content=\"",stdout);
        html(brand_head->headline);
        fputs("\"><meta property=\"og:description\" content=\"",stdout);
        html(brand_head->description);
        fputs("\"><meta name=\"twitter:card\" content=\"summary\">"
              "<meta property=\"og:url\" content=\"https://aurekai.",stdout);
        html(brand_head->territory);
        fputs("/\"><link rel=\"canonical\" href=\"https://aurekai.",stdout);
        html(brand_head->territory);
        fputs("/\">",stdout);
    }
    bf_route_presentation_emit_head_for(stdout, bf_route_presentation_territory(page_render_host),
        doc->route, doc->raw_query, ctx.experience_pattern);
    bf_native_experience_emit_head(stdout,bf_native_experience_find(ctx.experience_pattern));
    if (doc->route)
        fputs("<link rel=\"alternate\" type=\"application/atom+xml\" title=\"Aurekai public updates\" href=\"/feed.atom\">", stdout);
    if (doc->world) {
        fputs("<link rel=\"alternate\" type=\"application/atom+xml\" href=\"/w/",stdout); html(doc->world); fputs("/feed.atom\">",stdout);
        fputs("<link rel=\"alternate\" type=\"application/activity+json\" href=\"/w/",stdout); html(doc->world); fputs("/ap/outbox\">",stdout);
        fputs("<link rel=\"alternate\" type=\"application/cloudevents-batch+json\" href=\"/w/",stdout); html(doc->world); fputs("/events\">",stdout);
        fputs("<link rel=\"describedby\" type=\"application/json\" href=\"/w/",stdout); html(doc->world); fputs("/manifest.json\">",stdout);
    }
    if (doc->cannot && *doc->cannot) { fputs("<meta name=\"bonfyre:cannot-infer\" content=\"",stdout); html(doc->cannot); fputs("\">",stdout); }
    if (doc->route && (!strcmp(doc->route, "/discover") || !strcmp(doc->route, "/watch") || !strcmp(doc->route, "/board")))
        fputs("<style id=\"reading-surface-components\">"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .wrap>.hero{margin:clamp(18px,3vw,34px) 0 0;padding:clamp(26px,4vw,52px);border:1px solid var(--w-rule);border-radius:24px;background:color-mix(in srgb,var(--w-accent) 10%,var(--w-ground))}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .wrap>.hero h1{display:flex;align-items:center;gap:clamp(12px,2vw,24px);font-size:clamp(3rem,7vw,6.5rem);animation:none}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .reading-mark{display:grid;place-items:center;flex:none;width:clamp(48px,5vw,70px);height:clamp(48px,5vw,70px);border-radius:18px;background:var(--w-accent);color:var(--w-ground)}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .reading-mark .ak-icon{width:48%;height:48%}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .wrap>.hero .lede{margin-top:18px;max-width:54ch;font-size:clamp(1.05rem,1.5vw,1.35rem);line-height:1.45}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .wrap>.region{padding-block:clamp(32px,5vw,64px)}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .video-grid{gap:16px}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .video{padding:12px;border:1px solid var(--w-rule);border-radius:18px;background:var(--w-panel);text-decoration:none}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .video .frame{background:linear-gradient(135deg,var(--w-panel),color-mix(in srgb,var(--w-accent) 28%,var(--w-ground)))}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .video .play{width:44px;height:44px;background:var(--w-accent);color:var(--w-ground)}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .video .play .ak-icon{width:20px;height:20px}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .video .by{margin:8px 0 4px}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .tile{border:1px solid var(--w-rule);border-radius:16px;overflow:hidden;background:var(--w-panel)}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .tile>a{padding:14px;text-decoration:none}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .tile .frame{margin:-14px -14px 14px}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .pin{border:1px solid var(--w-rule);background:var(--w-panel)}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) .pin .cap{position:static;opacity:1;background:none;color:var(--w-ink);padding:12px}"
          "body[data-character]:is(.route-discover,.route-watch,.route-board) :is(.video,.pin,.tile a):focus-visible{outline:3px solid var(--w-accent);outline-offset:3px}"
          "@media(max-width:700px){body[data-character]:is(.route-discover,.route-watch,.route-board) .wrap>.hero{border-radius:18px;padding:24px 20px}body[data-character]:is(.route-discover,.route-watch,.route-board) .wrap>.hero h1{font-size:clamp(2.6rem,11vw,4rem)}body[data-character]:is(.route-discover,.route-watch,.route-board) .reading-mark{width:44px;height:44px;border-radius:13px}}"
          "</style>",stdout);
    if (brand_head) fputs("<link rel=\"stylesheet\" href=\"/assets/front-brand.css\">",stdout);
    fputs("</head>",stdout);
    fputs("<body class=\"", stdout); html(doc->character);
    if (doc->route) { fputs(" route-", stdout); html(doc->route[1] ? doc->route + 1 : "front"); }
    if (doc->requested_grammar && *doc->requested_grammar) { fputs(" world-grammar-", stdout); html(doc->requested_grammar); }
    fputs("\" data-character=\"", stdout); html(doc->character);
    if (doc->template_name && *doc->template_name && studio_safe_slug_arg(doc->template_name)) { fputs("\" data-template=\"", stdout); html(doc->template_name); }
    fputs("\"", stdout);
    bf_route_presentation_emit_body_attribute_for(stdout, bf_route_presentation_territory(page_render_host),
        doc->route, doc->raw_query, ctx.experience_pattern);
    fputs("><main class=\"wrap\" data-bonfyre-region=\"page-main\" data-bonfyre-continuity=\"page-main\">", stdout);
    page_emit_nav(doc);
    if (doc->world) { fputs("<nav class=\"tabs\" aria-label=\"Ways in\">", stdout); page_emit_world_tabs(db, doc); fputs("</nav>", stdout); }
    if (brand_head) {
        /* The four owned apex roots use the canonical composed brand H1 as their opening. */
        front_brand_emit(brand_head->territory);
    } else {
    fputs("<header class=\"hero\"><h1>", stdout);
    if (doc->route && (!strcmp(doc->route, "/discover") || !strcmp(doc->route, "/watch") || !strcmp(doc->route, "/board"))) {
        fputs("<span class=\"reading-mark\" aria-hidden=\"true\">", stdout); page_glyph(page_nav_glyph(doc->route)); fputs("</span>", stdout);
    }
    html(doc->title);
    if (doc->requested_grammar && *doc->requested_grammar && page_grammar_label(doc->requested_grammar)) { fputs(" <em>&middot; ",stdout); html(page_grammar_label(doc->requested_grammar)); fputs("</em>",stdout); }
    fputs("</h1>", stdout);
    {
        const char *display_lede = doc->lede;
        if (doc->route && !strcmp(doc->route, "/people"))
            display_lede = "The writers, hosts and makers behind what this estate reads, "
                           "and the people who keep an address here.";
        if (display_lede && *display_lede) {
            fputs("<p class=\"lede\">", stdout); html(display_lede); fputs("</p>", stdout);
        }
    }
    if (narrowed) {
        fputs("<p class=\"narrowed\">Showing ", stdout);
        if (doc->q && *doc->q) { fputs("matches for <b>", stdout); html(doc->q); fputs("</b>", stdout); }
        if (doc->from && *doc->from) { fputs(doc->q && *doc->q ? " from <b>" : "items from <b>", stdout); html(doc->from); fputs("</b>", stdout); }
        fputs(" &middot; <a href=\"", stdout); html(doc->route ? doc->route : "/"); fputs("\">Show all</a></p>", stdout);
    }
    fputs("</header>", stdout);
    } /* Other routes and independently owned sites retain their existing document hero. */
    page_emit_continuation_style();
    if(!narrowed)page_emit_media_spaces(db,doc->route,doc->raw_query,page_render_host);
    /* A territory's front is edited before it is composed: its own program
     * leads, and the declared regions and sections follow. */
    page_front_took_count = 0; page_front_used_count = 0; page_front_shown = 0;
    if (!narrowed) page_front_shown = page_front_editorial(db, projection, doc, material, &ctx);
    page_front_drop_used(material);
    if (doc->route && !strcmp(doc->route, "/search") && (!doc->q || !*doc->q)) {
        size_t i;
        fputs("<section class=\"region\"><p class=\"note\">Type what you are looking for above, or start from a section:</p><p class=\"jump\">", stdout);
        for (i = 0; i < BF_PAGE_NAV_COUNT; i++) { fputs("<a href=\"", stdout); html(bf_page_nav[i].route); fputs("\">", stdout); html(bf_page_nav[i].label); fputs("</a>", stdout); }
        fputs("</p></section>", stdout);
        shown = 1;
    } else {
        shown = 0;
        if (doc->route && !strcmp(doc->route, "/search") && doc->q && *doc->q) {
            fflush(stdout);
            page_emit_property_results(doc->q);
        }
        {
            /* A person walking the ledger itself: one publisher, one family, or
             * the publishers themselves, each a bounded page with its own
             * address.  A route reading eight publishers is the top of a
             * lattice thousands deep, and this is the way down into it. */
            const char *frontier = getenv("BONFYRE_SOURCE_FRONTIER_DB");
            const char *route = doc->route ? doc->route : "/channels";
            /* Everything public here is somebody's work, and until now the
             * estate showed the work and never the maker or the institution.
             * Both are read out of what it has already kept. */
            if (doc->route && (!strcmp(doc->route, "/people") || !strcmp(doc->route, "/organisations"))) {
                const char *archive = getenv("BONFYRE_ITEM_ARCHIVE_DB");
                /* A byline is retained external evidence, not a display
                 * nickname. Keep the full bounded archive coordinate when a
                 * static manifest asks for it; truncating it would lower a
                 * different query than the one the edge materialized. */
                char who[512] = "", source[2048] = "", find[128] = "", from[16] = "", sort[16] = "";
                unsigned offset;
                if (doc->raw_query && *doc->raw_query) {
                    query_value(doc->raw_query, "who", who, sizeof(who));
                    query_value(doc->raw_query, "source", source, sizeof(source));
                    query_value(doc->raw_query, "q", find, sizeof(find));
                    query_value(doc->raw_query, "from", from, sizeof(from));
                    query_value(doc->raw_query, "sort", sort, sizeof(sort));
                }
                offset = (unsigned)atoi(from[0] ? from : "0");
                if (!strcmp(doc->route, "/people")) {
                    if (who[0]) shown += bf_person_page(stdout, archive, who, source, doc->route);
                    /* A maker credited only in a feed's own credit lines. */
                    if (who[0] && source[0] && !shown) shown += bf_detail_person_work(stdout, who, source);
                    if (who[0] && source[0] && shown) {
                        BfDetailFocus focus = {BF_DETAIL_PERSON, who, source, 0};
                        shown += bf_detail_continue(stdout, &focus);
                    }
                        shown += bf_people_index_ordered(stdout, archive, doc->route, find, offset, 60, !strcmp(sort, "recent"));
                        shown += bf_people_index(stdout, archive, doc->route, find, offset, 60);
                } else if (frontier && *frontier) {
                    shown += bf_organisations_index(stdout, frontier, doc->route, find, offset, 60);
                }
            } else if (frontier && *frontier && doc->route && !strcmp(doc->route, "/channels")) {
                /* `channel` is an exact foreign coordinate, not a friendly
                 * slug.  The largest retained admitted coordinate is a
                 * QLever query several kilobytes long.  Do not truncate it
                 * into a different, non-existent channel merely because the
                 * public representation has a compact material key. */
                char root[256] = "", family[64] = "", from[16] = "", roots[8] = "", channel[8192] = "", find[128] = "";
                char way[48] = "", publishes[48] = "", standing[32] = "", topic[256] = "", named[256] = "";
                unsigned offset;
                if (doc->raw_query && *doc->raw_query) {
                    query_value(doc->raw_query, "topic", topic, sizeof(topic));
                    query_value(doc->raw_query, "named", named, sizeof(named));
                    query_value(doc->raw_query, "root", root, sizeof(root));
                    query_value(doc->raw_query, "family", family, sizeof(family));
                    query_value(doc->raw_query, "from", from, sizeof(from));
                    query_value(doc->raw_query, "roots", roots, sizeof(roots));
                    query_value(doc->raw_query, "channel", channel, sizeof(channel));
                    query_value(doc->raw_query, "q", find, sizeof(find));
                    query_value(doc->raw_query, "way", way, sizeof(way));
                    query_value(doc->raw_query, "publishes", publishes, sizeof(publishes));
                    query_value(doc->raw_query, "standing", standing, sizeof(standing));
                }
                offset = (unsigned)atoi(from[0] ? from : "0");
                /* Four addressable depths, each its own page: the ledger's
                 * shape, the publishers, one publisher's lattice, and one
                 * channel with what this estate kept from it.  A route that
                 * reads eight publishers stands on top of thousands. */
                /* Each depth closes with the other kinds of page its own
                 * material reaches (detail_depth.c): makers, readers, topics,
                 * application shapes, publishers. */
                if (topic[0] || named[0]) shown += bf_detail_facet_page(stdout, topic[0] ? "topic" : "mention", topic[0] ? topic : named, route);
                else if (channel[0]) {
                    int one = bf_source_depth_channel(stdout, frontier, channel, route, getenv("BONFYRE_ITEM_ARCHIVE_DB"));
                    shown += one;
                    if (one) { BfDetailFocus focus = {BF_DETAIL_CHANNEL, channel, NULL, 0}; shown += bf_detail_continue(stdout, &focus); }
                }
                else if (way[0] || publishes[0] || standing[0]) {
                    shown += bf_source_depth_slice(stdout, frontier, route, way, publishes, standing, offset, 60);
                    if (way[0] && !publishes[0] && !standing[0] && !offset) { BfDetailFocus focus = {BF_DETAIL_READER, way, NULL, 0}; shown += bf_detail_continue(stdout, &focus); }
                }
                else if (root[0]) {
                    shown += bf_source_depth_emit(stdout, frontier, root, family, route, offset, 40);
                    if (!family[0] && !offset) { BfDetailFocus focus = {BF_DETAIL_ROOT, root, NULL, 0}; shown += bf_detail_continue(stdout, &focus); }
                }
                else if (roots[0] || find[0]) shown += bf_source_depth_roots(stdout, frontier, route, find, offset, 40);
                else {
                    shown += bf_source_depth_index(stdout, frontier, route, "");
                    shown += bf_source_depth_roots(stdout, frontier, route, "", 0, 40);
                }
            } else if (frontier && *frontier && doc->raw_query && *doc->raw_query) {
                char root[256] = "", family[64] = "", from[16] = "", roots[8] = "";
                unsigned offset;
                query_value(doc->raw_query, "root", root, sizeof(root));
                query_value(doc->raw_query, "family", family, sizeof(family));
                query_value(doc->raw_query, "from", from, sizeof(from));
                query_value(doc->raw_query, "roots", roots, sizeof(roots));
                offset = (unsigned)atoi(from[0] ? from : "0");
                if (root[0]) shown += bf_source_depth_emit(stdout, frontier, root, family, route, offset, 40);
                else if (roots[0]) shown += bf_source_depth_roots(stdout, frontier, route, "", offset, 40);
            }
        }
        {
            /* A route names its regions, but what it actually holds decides
             * how that reads: where the material argues for rhythms the route
             * did not name -- a day of events, a field of pictures, a list of
             * recordings that play -- the scene compiler composes those first,
             * and the route's own sequence carries whatever is left. */
            unsigned char scene[BF_PAGE_SCENE_GRAMMAR_COUNT];
            char why[256] = "", look[64] = "";
            size_t scene_count = doc->program ? page_compile_scene(material, scene, sizeof(scene), why, sizeof(why), ctx.now) : 0;
            if (scene_count) {
                /* What it is mostly made of decides how it is dressed. */
                page_emit_profile(db, doc->route ? doc->route : doc->title,
                                  bf_page_regions[scene[0]].material, look, sizeof(look));
                if (look[0]) { fputs("<meta name=\"bonfyre:look\" content=\"", stdout); html(look); fputs("\">", stdout); }
            }
            if (scene_count > 1) {
                /* One sequence for the whole page: what the material argues
                 * for, then whatever else the route named, and any mix at the
                 * very end -- a mix spends every stream, so a page continues
                 * only as long as none has been shown yet. */
                unsigned char walk[BF_PAGE_REGION_COUNT * 2]; size_t steps = 0, a, b, pass;
                if (why[0]) { fputs("<meta name=\"bonfyre:scene\" content=\"", stdout); html(why); fputs("\">", stdout); }
                for (pass = 0; pass < 2; pass++) {
                    int want_mix = (int)pass;      /* everything else first, mixes last */
                    for (a = 0; a < scene_count && steps < sizeof(walk); a++) {
                        int mixes = bf_page_regions[scene[a]].material == BF_PAGE_MATERIAL_MIX;
                        if (mixes != want_mix) continue;
                        walk[steps++] = scene[a];
                    }
                    for (a = 0; a < doc->sequence_count && steps < sizeof(walk); a++) {
                        int mixes = bf_page_regions[doc->sequence[a]].material == BF_PAGE_MATERIAL_MIX;
                        int seen = 0;
                        if (mixes != want_mix) continue;
                        for (b = 0; b < steps; b++) if (walk[b] == doc->sequence[a]) seen = 1;
                        if (!seen) walk[steps++] = doc->sequence[a];
                    }
                }
                shown += page_emit_regions(material, walk, steps, &ctx);
            } else shown += page_emit_regions(material, doc->sequence, doc->sequence_count, &ctx);
        }
        if (!narrowed && !directory_cut && doc->phase_sections && doc->phase_section_count) {
            size_t i;
            for (i = 0; i < doc->phase_section_count; i++) { page_emit_section(db, projection, doc->phase_sections[i], ctx.now); shown++; }
        } else if (!narrowed && !directory_cut && doc->program && doc->program->section_count && doc->program->sections[0]) {
            size_t i;
            for (i = 0; i < doc->program->section_count; i++) {
                if (page_front_route_taken(doc->program->sections[i])) continue;
                page_emit_section(db, projection, doc->program->sections[i], ctx.now); shown++;
            }
        }
        /* The way on.  A route reads as far down the estate's publishers as a
         * person cares to go, and the next length of it is a link: it works
         * with nothing switched on, it can be opened in a new tab, and a
         * browser that runs the script below simply saves the click. */
        if (shown && doc->program && doc->route && doc->route[0] == '/' && !doc->q[0] && !doc->from[0]) {
            /* Only where there is plainly more to read.  A route that could
             * not fill its own complement of publishers has reached the end of
             * what this estate holds for it, and offering another length would
             * be offering an empty page. */
            unsigned drawn = 0; const char *walk = doc->sources;
            while (walk && *walk) { drawn++; walk = strchr(walk, ','); if (walk) walk++; }
            if (drawn >= doc->program->source_limit)
                page_emit_keep_reading(doc->route, page_source_offset, doc->program->source_limit);
        }
    }
    if (!shown && !page_front_shown) {
        fputs("<p class=\"note\">Nothing here", stdout);
        if (doc->q && *doc->q) { fputs(" matches <b>", stdout); html(doc->q); fputs("</b>", stdout); }
        fputs(" right now.</p>", stdout);
    }

    page_emit_surface_scene(doc, &surface_scene);
    fputs("</main></body></html>", stdout);
    free(template); if (material != doc->prefilled) free(material);
    return 1;
}

#endif /* BONFYRE_PAGE_CORE_RENDERING_H */
