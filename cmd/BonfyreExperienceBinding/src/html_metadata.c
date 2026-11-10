#define _POSIX_C_SOURCE 200809L
#include "html_metadata.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

static char *read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    long size;
    char *body;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 || fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    body = (char *)malloc((size_t)size + 1);
    if (!body) { fclose(f); return NULL; }
    if (size && fread(body, 1, (size_t)size, f) != (size_t)size) {
        fclose(f);
        free(body);
        return NULL;
    }
    body[size] = '\0';
    fclose(f);
    return body;
}

static int ci_prefix(const char *p, const char *needle) {
    while (*needle) {
        if (!*p || tolower((unsigned char)*p) != tolower((unsigned char)*needle)) return 0;
        ++p;
        ++needle;
    }
    return 1;
}

static const char *ci_find(const char *haystack, const char *needle) {
    const char *p;
    if (!haystack || !needle || !*needle) return haystack;
    for (p = haystack; *p; ++p)
        if (ci_prefix(p, needle)) return p;
    return NULL;
}

static const char *tag_end(const char *p) {
    char quote = 0;
    for (; *p; ++p) {
        if (quote) {
            if (*p == quote) quote = 0;
        } else if (*p == '\'' || *p == '"') {
            quote = *p;
        } else if (*p == '>') {
            return p;
        }
    }
    return NULL;
}

static int hex_digit(int c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    return -1;
}

static size_t emit_utf8(char *out, unsigned long cp) {
    if (cp <= 0x7f) { out[0] = (char)cp; return 1; }
    if (cp <= 0x7ff) {
        out[0] = (char)(0xc0 | (cp >> 6));
        out[1] = (char)(0x80 | (cp & 0x3f));
        return 2;
    }
    if (cp <= 0xffff) {
        out[0] = (char)(0xe0 | (cp >> 12));
        out[1] = (char)(0x80 | ((cp >> 6) & 0x3f));
        out[2] = (char)(0x80 | (cp & 0x3f));
        return 3;
    }
    if (cp <= 0x10ffff) {
        out[0] = (char)(0xf0 | (cp >> 18));
        out[1] = (char)(0x80 | ((cp >> 12) & 0x3f));
        out[2] = (char)(0x80 | ((cp >> 6) & 0x3f));
        out[3] = (char)(0x80 | (cp & 0x3f));
        return 4;
    }
    return 0;
}

static char *decode_trim(const char *begin, const char *end) {
    const char *p;
    char *out, *w;
    while (begin < end && isspace((unsigned char)*begin)) ++begin;
    while (end > begin && isspace((unsigned char)end[-1])) --end;
    out = (char *)malloc((size_t)(end - begin) + 1);
    if (!out) return NULL;
    w = out;
    for (p = begin; p < end; ++p) {
        const char *semi;
        if (*p != '&' || !(semi = memchr(p, ';', (size_t)(end - p)))) {
            *w++ = *p;
            continue;
        }
        if (p + 2 < semi && p[1] == '#') {
            unsigned long cp = 0;
            int base = 10, valid = 1;
            const char *q = p + 2;
            if (q < semi && (*q == 'x' || *q == 'X')) { base = 16; ++q; }
            if (q == semi) valid = 0;
            for (; q < semi && valid; ++q) {
                int d = base == 16 ? hex_digit((unsigned char)*q) :
                    (isdigit((unsigned char)*q) ? *q - '0' : -1);
                if (d < 0 || d >= base) valid = 0;
                else cp = cp * (unsigned long)base + (unsigned long)d;
            }
            if (valid) {
                size_t n = emit_utf8(w, cp);
                if (n) { w += n; p = semi; continue; }
            }
        } else {
            size_t n = (size_t)(semi - (p + 1));
            const char *name = p + 1;
            unsigned long cp = 0;
            if (n == 3 && !memcmp(name, "amp", 3)) cp = '&';
            else if (n == 4 && !memcmp(name, "quot", 4)) cp = '"';
            else if (n == 4 && !memcmp(name, "apos", 4)) cp = '\'';
            else if (n == 2 && !memcmp(name, "lt", 2)) cp = '<';
            else if (n == 2 && !memcmp(name, "gt", 2)) cp = '>';
            else if (n == 4 && !memcmp(name, "nbsp", 4)) cp = 0xa0;
            else if (n == 5 && !memcmp(name, "ndash", 5)) cp = 0x2013;
            else if (n == 5 && !memcmp(name, "mdash", 5)) cp = 0x2014;
            if (cp) {
                size_t bytes = emit_utf8(w, cp);
                if (bytes) { w += bytes; p = semi; continue; }
            }
        }
        *w++ = *p;
    }
    *w = '\0';
    return out;
}

static int name_equal(const char *begin, const char *end, const char *wanted) {
    size_t n = (size_t)(end - begin);
    return strlen(wanted) == n && strncasecmp(begin, wanted, n) == 0;
}

static char *attribute_value(const char *begin, const char *end, const char *wanted) {
    const char *p = begin;
    while (p < end) {
        const char *name_begin, *name_end, *value_begin, *value_end;
        char quote = 0;
        while (p < end && isspace((unsigned char)*p)) ++p;
        if (p >= end || *p == '/' || *p == '>') break;
        name_begin = p;
        while (p < end && (isalnum((unsigned char)*p) || *p == ':' || *p == '_' || *p == '-')) ++p;
        name_end = p;
        if (name_begin == name_end) { ++p; continue; }
        while (p < end && isspace((unsigned char)*p)) ++p;
        if (p >= end || *p != '=') continue;
        ++p;
        while (p < end && isspace((unsigned char)*p)) ++p;
        if (p >= end) break;
        if (*p == '\'' || *p == '"') quote = *p++;
        value_begin = p;
        if (quote) while (p < end && *p != quote) ++p;
        else while (p < end && !isspace((unsigned char)*p) && *p != '>') ++p;
        value_end = p;
        if (quote && p < end) ++p;
        if (name_equal(name_begin, name_end, wanted)) return decode_trim(value_begin, value_end);
    }
    return NULL;
}

static void replace(char **slot, char *value) {
    if (!value) return;
    free(*slot);
    *slot = value;
}

void bf_html_metadata_free(BfHtmlMetadata *metadata) {
    if (!metadata) return;
    free(metadata->name);
    free(metadata->authority);
    free(metadata->description);
    free(metadata->source_url);
    free(metadata->updated_at);
    memset(metadata, 0, sizeof(*metadata));
}

int bf_html_metadata_parse_file(const char *path, const char *fallback_url, BfHtmlMetadata *out) {
    char *body = NULL, *title = NULL, *og_title = NULL, *site_name = NULL;
    char *description = NULL, *og_description = NULL, *og_url = NULL;
    char *og_updated = NULL, *article_modified = NULL;
    const char *p, *end;
    int ok = 0;
    if (!path || !fallback_url || !out) return 0;
    memset(out, 0, sizeof(*out));
    body = read_file(path);
    if (!body) goto done;

    p = ci_find(body, "<title");
    if (p && (end = tag_end(p))) {
        const char *close = ci_find(end + 1, "</title>");
        if (close) title = decode_trim(end + 1, close);
    }

    p = body;
    while ((p = ci_find(p, "<meta")) != NULL) {
        char *property = NULL, *name = NULL, *content = NULL;
        const char *after = p + 5;
        if (*after && !isspace((unsigned char)*after) && *after != '/' && *after != '>') {
            p = after;
            continue;
        }
        end = tag_end(after);
        if (!end) break;
        property = attribute_value(after, end, "property");
        name = attribute_value(after, end, "name");
        content = attribute_value(after, end, "content");
        if (content && property) {
            if (!strcasecmp(property, "og:title")) replace(&og_title, content), content = NULL;
            else if (!strcasecmp(property, "og:site_name")) replace(&site_name, content), content = NULL;
            else if (!strcasecmp(property, "og:description")) replace(&og_description, content), content = NULL;
            else if (!strcasecmp(property, "og:url")) replace(&og_url, content), content = NULL;
            else if (!strcasecmp(property, "og:updated_time")) replace(&og_updated, content), content = NULL;
            else if (!strcasecmp(property, "article:modified_time")) replace(&article_modified, content), content = NULL;
        } else if (content && name && !strcasecmp(name, "description")) {
            replace(&description, content);
            content = NULL;
        }
        free(property);
        free(name);
        free(content);
        p = end + 1;
    }

    out->name = og_title ? strdup(og_title) : (title ? strdup(title) : NULL);
    out->authority = site_name ? strdup(site_name) : NULL;
    out->description = description ? strdup(description) :
        (og_description ? strdup(og_description) : NULL);
    out->source_url = og_url ? strdup(og_url) : strdup(fallback_url);
    out->updated_at = og_updated ? strdup(og_updated) :
        (article_modified ? strdup(article_modified) : NULL);
    if (!out->name || !*out->name || !out->authority || !*out->authority ||
        !out->description || !*out->description || !out->source_url || !*out->source_url)
        goto done;
    ok = 1;

done:
    if (!ok) bf_html_metadata_free(out);
    free(article_modified);
    free(og_updated);
    free(og_url);
    free(og_description);
    free(description);
    free(site_name);
    free(og_title);
    free(title);
    free(body);
    return ok;
}
