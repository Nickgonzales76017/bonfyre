#include "campaign.h"
#include "bf_lex.h"
#include "bf_utf8.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

static const char *const dimensions[] = {
    "utm_source", "utm_medium", "utm_campaign", "utm_id", "utm_term", "utm_content"
};
typedef struct {
    char url[BF_CAMPAIGN_URL_MAX + 1];
    char values[6][BF_CAMPAIGN_VALUE_MAX + 1];
    unsigned present;
} Coordinates;

static void member(bf_jsonw *w, const char *key, const char *value) {
    bf_jsonw_key(w, key); bf_jsonw_str(w, value);
}
int bf_campaign_error(bf_jsonw *w, const char *code) {
    bf_jsonw_reset(w); bf_jsonw_obj(w);
    member(w, "schema", "bonfyre.campaign-link.v1");
    bf_jsonw_key(w, "ok"); bf_jsonw_bool(w, 0);
    member(w, "error", code); bf_jsonw_obj_end(w);
    return 2;
}
static int hex(unsigned char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
static int clean(const char *s, size_t n) {
    if (!bf_utf8_valid(s, n, NULL)) return 0;
    for (size_t i = 0; i < n; i++)
        if ((unsigned char)s[i] < 0x20 || (unsigned char)s[i] == 0x7f) return 0;
    return 1;
}

/* bf_json accepts a broader syntax. Gate this small, two-object request
 * grammar before using its DOM, including exact input consumption. */
typedef struct { const char *p, *end; } Syntax;
static void space(Syntax *s) { while (s->p < s->end && strchr(" \t\r\n", *s->p)) s->p++; }
static int quoted(Syntax *s) {
    if (s->p == s->end || *s->p++ != '"') return 0;
    while (s->p < s->end) {
        unsigned char c = (unsigned char)*s->p++;
        if (c == '"') return 1;
        if (c < 0x20) return 0;
        if (c == '\\') {
            if (s->p == s->end) return 0;
            c = (unsigned char)*s->p++;
            if (c == 'u') { for (int i = 0; i < 4; i++) if (s->p == s->end || hex((unsigned char)*s->p++) < 0) return 0; }
            else if (!strchr("\"\\/bfnrt", c)) return 0;
        }
    }
    return 0;
}
static int object_syntax(Syntax *s, unsigned depth) {
    if (depth > 2 || s->p == s->end || *s->p++ != '{') return 0;
    space(s);
    if (s->p < s->end && *s->p == '}') { s->p++; return 1; }
    for (;;) {
        if (!quoted(s)) return 0;
        space(s); if (s->p == s->end || *s->p++ != ':') return 0; space(s);
        if (s->p == s->end) return 0;
        if (*s->p == '"') { if (!quoted(s)) return 0; }
        else if (*s->p == '{') { if (!object_syntax(s, depth + 1)) return 0; }
        else if (*s->p == '1') s->p++;
        else return 0;
        space(s); if (s->p == s->end) return 0;
        if (*s->p == '}') { s->p++; return 1; }
        if (*s->p++ != ',') return 0;
        space(s);
    }
}
static int decoded(const char *raw, int n, char *out, size_t cap) {
    size_t size = 0;
    char *s = bf_json_unescape(raw, n, &size);
    int ok = s && size < cap && clean(s, size);
    if (ok) { memcpy(out, s, size); out[size] = 0; }
    free(s); return ok;
}
static int string_node(const bf_json_node_t *n, char *out, size_t cap) {
    return n && n->type == BF_JSON_STRING && decoded(n->val.str.ptr, n->val.str.len, out, cap);
}
static int dimension(const char *key) {
    for (int i = 0; i < 6; i++) if (!strcmp(key, dimensions[i])) return i;
    return -1;
}
static int coordinates(const bf_json_doc_t *doc, const bf_json_node_t *node, Coordinates *out) {
    if (!node || node->type != BF_JSON_OBJECT || node->child_count > 6) return 0;
    for (const bf_json_node_t *p = bf_json_child_first(doc, node); p; p = bf_json_child_next(doc, p)) {
        char key[32];
        if (!decoded(p->key, p->key_len, key, sizeof(key))) return 0;
        int i = dimension(key);
        if (i < 0 || (out->present & (1u << i)) || !string_node(p, out->values[i], sizeof(out->values[i])) || !out->values[i][0] || strchr(out->values[i], '\\')) return 0;
        out->present |= 1u << i;
    }
    return 1;
}

/* Reuse the native query decoder on one bounded field. Key decoding happens
 * before duplicate checks, so utm_source and utm%5Fsource cannot coexist. */
static int query_text(const char *p, size_t n, char *out, size_t cap) {
    char form[BF_CAMPAIGN_URL_MAX + 3];
    if (n > BF_CAMPAIGN_URL_MAX) return 0;
    memcpy(form, "v=", 2); memcpy(form + 2, p, n); form[n + 2] = 0;
    return bf_lex_form_query(form, "v", out, cap) == 1 && clean(out, strlen(out));
}
static int suffix(const char *host, const char *tail) {
    size_t a = strlen(host), b = strlen(tail);
    return a >= b && !strcmp(host + a - b, tail);
}
/* Syntactic destination policy: DNS names only, no DNS lookup. Rejecting IP
 * literals and numeric final labels also excludes browser IPv4 shorthand. */
static int public_name(const char *authority, size_t n) {
    char host[254];
    const char *colon = memchr(authority, ':', n);
    size_t hn = colon ? (size_t)(colon - authority) : n;
    if (!hn || hn >= sizeof(host)) return 0;
    if (colon) {
        unsigned port = 0; size_t pn = n - hn - 1;
        if (!pn || pn > 5) return 0;
        for (size_t i = 0; i < pn; i++) {
            unsigned char c = (unsigned char)colon[1 + i];
            if (c < '0' || c > '9') return 0;
            port = port * 10 + c - '0';
        }
        if (!port || port > 65535) return 0;
    }
    for (size_t i = 0; i < hn; i++) {
        unsigned char c = (unsigned char)authority[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.')) return 0;
        host[i] = (char)tolower(c);
    }
    host[hn] = 0;
    char *last = strrchr(host, '.');
    if (!last || last[1] < 'a' || last[1] > 'z') return 0;
    const char *label = host;
    while (*label) {
        size_t length = strcspn(label, ".");
        if (!length || length > 63 || label[0] == '-' || label[length - 1] == '-') return 0;
        label += length;
        if (*label && !*++label) return 0;
    }
    static const char *const local[] = { ".localhost", ".local", ".internal", ".lan", ".home", ".test", ".invalid", ".onion", ".home.arpa" };
    for (size_t i = 0; i < sizeof(local) / sizeof(*local); i++) if (suffix(host, local[i])) return 0;
    return 1;
}
static const char *parse_url(Coordinates *out) {
    const char *url = out->url;
    size_t n = strlen(url);
    if (!n || n > BF_CAMPAIGN_URL_MAX || !clean(url, n)) return "invalid_url";
    char unescaped[BF_CAMPAIGN_URL_MAX + 1]; size_t used = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)url[i];
        if (c <= 0x20 || c == 0x7f || strchr("\\<>\"`", c)) return "invalid_url";
        if (c == '%') {
            if (i + 2 >= n || hex((unsigned char)url[i + 1]) < 0 || hex((unsigned char)url[i + 2]) < 0) return "invalid_url";
            c = (unsigned char)(hex((unsigned char)url[i + 1]) * 16 + hex((unsigned char)url[i + 2])); i += 2;
            if (c < 0x20 || c == 0x7f || c == '\\') return "invalid_url";
        }
        unescaped[used++] = (char)c;
    }
    if (!clean(unescaped, used)) return "invalid_url";
    const char *authority;
    if (!strncasecmp(url, "https://", 8)) authority = url + 8;
    else if (!strncasecmp(url, "http://", 7)) authority = url + 7;
    else return "invalid_url";
    size_t an = strcspn(authority, "/?#");
    if (!public_name(authority, an)) return "destination_not_public_dns";
    const char *fragment = strchr(authority + an, '#');
    const char *end = fragment ? fragment : url + n;
    const char *query = memchr(authority + an, '?', (size_t)(end - authority - an));
    if (!query) return NULL;
    for (const char *p = query + 1; p < end;) {
        const char *amp = memchr(p, '&', (size_t)(end - p));
        const char *stop = amp ? amp : end;
        const char *eq = memchr(p, '=', (size_t)(stop - p));
        char key[BF_CAMPAIGN_URL_MAX + 1];
        size_t kn = (size_t)((eq ? eq : stop) - p);
        if (!query_text(p, kn, key, sizeof(key))) return "invalid_query";
        int i = dimension(key);
        for (int j = 0; j < 6; j++) if (i < 0 && !strcasecmp(key, dimensions[j])) return "noncanonical_utm_key";
        if (i >= 0) {
            if (out->present & (1u << i)) return "duplicate_utm";
            if (!eq || !query_text(eq + 1, (size_t)(stop - eq - 1), out->values[i], sizeof(out->values[i])) || !out->values[i][0]) return "invalid_utm_value";
            out->present |= 1u << i;
        }
        p = amp ? amp + 1 : end;
    }
    return NULL;
}
static int append(char *out, size_t *used, const char *p, size_t n) {
    if (*used + n > BF_CAMPAIGN_URL_MAX) return 0;
    memcpy(out + *used, p, n); *used += n; out[*used] = 0; return 1;
}
static int encode(char *out, size_t *used, const char *p) {
    static const char digits[] = "0123456789ABCDEF";
    for (; *p; p++) {
        unsigned char c = (unsigned char)*p;
        int plain = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || strchr("-._~", c);
        char escaped[3] = {'%', digits[c >> 4], digits[c & 15]};
        if (!append(out, used, plain ? p : escaped, plain ? 1 : 3)) return 0;
    }
    return 1;
}
static int build_url(Coordinates *out, const Coordinates *supplied) {
    char result[BF_CAMPAIGN_URL_MAX + 1]; size_t used = 0;
    const char *fragment = strchr(out->url, '#');
    size_t base = fragment ? (size_t)(fragment - out->url) : strlen(out->url);
    if (!append(result, &used, out->url, base)) return 0;
    int has_query = memchr(out->url, '?', base) != NULL;
    for (int i = 0; i < 6; i++) if (supplied->present & (1u << i)) {
        const char *separator = !has_query ? "?" : (used && (result[used - 1] == '?' || result[used - 1] == '&') ? "" : "&");
        if (!append(result, &used, separator, strlen(separator)) || !append(result, &used, dimensions[i], strlen(dimensions[i])) || !append(result, &used, "=", 1) || !encode(result, &used, supplied->values[i])) return 0;
        has_query = 1;
    }
    if (fragment && !append(result, &used, fragment, strlen(fragment))) return 0;
    memcpy(out->url, result, used + 1);
    memcpy(out->values, supplied->values, sizeof(out->values)); out->present = supplied->present;
    return 1;
}

int bf_campaign_request(const char *input, size_t length, bf_jsonw *out) {
    if (!input || !length || length > BF_CAMPAIGN_REQUEST_MAX || memchr(input, 0, length) || !bf_utf8_valid(input, length, NULL)) return bf_campaign_error(out, "invalid_request");
    Syntax syntax = { input, input + length }; space(&syntax);
    if (!object_syntax(&syntax, 1)) return bf_campaign_error(out, "invalid_request");
    space(&syntax); if (syntax.p != syntax.end) return bf_campaign_error(out, "invalid_request");
    bf_json_doc_t *doc = bf_json_parse(input, length, NULL, 0);
    if (!doc) return bf_campaign_error(out, "invalid_request");
    Coordinates result = {0}, supplied = {0}; char operation[16] = "";
    const char *error = "invalid_request"; unsigned seen = 0;
    const bf_json_node_t *root = bf_json_root(doc);
    for (const bf_json_node_t *p = bf_json_child_first(doc, root); p; p = bf_json_child_next(doc, p)) {
        char key[32]; unsigned bit;
        if (!decoded(p->key, p->key_len, key, sizeof(key))) goto done;
        if (!strcmp(key, "version")) bit = 1;
        else if (!strcmp(key, "operation")) bit = 2;
        else if (!strcmp(key, "url")) bit = 4;
        else if (!strcmp(key, "utm")) bit = 8;
        else goto done;
        if (seen & bit) goto done;
        seen |= bit;
        if (bit == 1) { if (p->type != BF_JSON_INT || p->val.i != 1) goto done; }
        else if (bit == 2) { if (!string_node(p, operation, sizeof(operation))) goto done; }
        else if (bit == 4) { if (!string_node(p, result.url, sizeof(result.url))) goto done; }
        else if (!coordinates(doc, p, &supplied)) goto done;
    }
    if ((seen & 7) != 7) goto done;
    int building = !strcmp(operation, "build");
    if (!building && strcmp(operation, "parse") && strcmp(operation, "validate")) goto done;
    if (building ? seen != 15 : seen != 7) goto done;
    error = parse_url(&result);
    if (error) goto done;
    if (building) {
        if ((supplied.present & 7) != 7) { error = "required_utm_missing"; goto done; }
        if (result.present) { error = "destination_already_tagged"; goto done; }
        if (!build_url(&result, &supplied)) { error = "generated_url_too_large"; goto done; }
    }
    bf_jsonw_reset(out); bf_jsonw_obj(out);
    member(out, "schema", "bonfyre.campaign-link.v1");
    bf_jsonw_key(out, "ok"); bf_jsonw_bool(out, 1);
    member(out, "operation", operation); member(out, "url", result.url);
    member(out, "tagging", !result.present ? "absent" : (result.present & 7) == 7 ? "complete" : "partial");
    bf_jsonw_key(out, "utm"); bf_jsonw_obj(out);
    for (int i = 0; i < 6; i++) if (result.present & (1u << i)) member(out, dimensions[i], result.values[i]);
    bf_jsonw_obj_end(out);
    member(out, "provenance", building ? "authored_coordinates" : "untrusted_url_coordinates");
    bf_jsonw_key(out, "external_effect_executed"); bf_jsonw_bool(out, 0);
    bf_jsonw_obj_end(out);
done:
    bf_json_free(doc);
    return error ? bf_campaign_error(out, error) : 0;
}
