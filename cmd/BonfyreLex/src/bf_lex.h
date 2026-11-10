#ifndef BONFYRE_LEX_H
#define BONFYRE_LEX_H

/* One implementation per field kind.
 *
 * The point is not that these are clever; it is that there is exactly one of
 * each. A hand-rolled extractor is written at the moment it is needed, by
 * someone thinking about something else, and the escaping is remembered or it
 * is not. This estate had 163 of them in C and 49 more as grep, sed and awk in
 * its witnesses, and two were wrong in ways nothing could notice.
 *
 * Every function here takes a bounded output and never writes past it, returns
 * 1 when it found a value and 0 when it did not, and distinguishes "absent"
 * from "present and empty" because those are different facts about a document.
 */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>   /* strtol: percent-decoding */
#include <string.h>
#include <strings.h>   /* strncasecmp: header names are case-insensitive */

/* A JSON string value, unescaped, read from a cursor already past the opening
 * quote. `*end` is always advanced past the closing quote even when the value
 * did not fit, because a tokenizer walking a vocabulary file has to keep its
 * place regardless of whether it kept the token.
 *
 * The escaped-quote case is the reason this exists: an HTTP ETag is `W/"abc"`
 * and appears in JSON as `W/\"abc\"`. A reader that stops at the first quote
 * returns `W/\`, which every source rejects as a validator -- silently, because
 * a rejected validator looks exactly like a source that had no new answer.
 *
 * \u is decoded to UTF-8, surrogate pairs included. One copy of this in the
 * estate wrote `?` for everything above ASCII and another dropped the low half
 * of a surrogate pair, so a vocabulary entry outside the BMP came back as two
 * replacement characters and matched nothing. */
static inline int bf_lex_json_string_at(const char *p, char *out, size_t cap, const char **end) {
    size_t written = 0;
    int whole = 1;
    if (end) *end = p;
    if (!p || !out || cap == 0) return 0;
    out[0] = '\0';
    while (*p && *p != '"') {
        unsigned value = 0;
        char utf8[4];
        int bytes = 0, i;
        if (*p == '\\' && p[1]) {
            p++;
            switch (*p) {
                case 'n': utf8[0] = '\n'; bytes = 1; p++; break;
                case 't': utf8[0] = '\t'; bytes = 1; p++; break;
                case 'r': utf8[0] = '\r'; bytes = 1; p++; break;
                case 'b': utf8[0] = '\b'; bytes = 1; p++; break;
                case 'f': utf8[0] = '\f'; bytes = 1; p++; break;
                case 'u': {
                    int digits = 0;
                    while (digits < 4 && isxdigit((unsigned char)p[1])) {
                        char c = *++p;
                        value = value * 16 + (unsigned)(isdigit((unsigned char)c)
                                 ? c - '0' : (tolower((unsigned char)c) - 'a' + 10));
                        digits++;
                    }
                    p++;
                    if (digits != 4) { whole = 0; break; }
                    /* A high surrogate is half a character. Taking it alone is
                     * how an astral codepoint became two broken ones. */
                    if (value >= 0xD800 && value <= 0xDBFF &&
                        p[0] == '\\' && p[1] == 'u' && isxdigit((unsigned char)p[2])) {
                        unsigned low = 0;
                        int d = 0;
                        const char *save = p;
                        p++;
                        while (d < 4 && isxdigit((unsigned char)p[1])) {
                            char c = *++p;
                            low = low * 16 + (unsigned)(isdigit((unsigned char)c)
                                    ? c - '0' : (tolower((unsigned char)c) - 'a' + 10));
                            d++;
                        }
                        if (d == 4 && low >= 0xDC00 && low <= 0xDFFF) {
                            value = 0x10000u + ((value - 0xD800u) << 10) + (low - 0xDC00u);
                            p++;
                        } else {
                            p = save;
                        }
                    }
                    if (value < 0x80) { utf8[0] = (char)value; bytes = 1; }
                    else if (value < 0x800) {
                        utf8[0] = (char)(0xC0 | (value >> 6));
                        utf8[1] = (char)(0x80 | (value & 0x3F));
                        bytes = 2;
                    } else if (value < 0x10000) {
                        utf8[0] = (char)(0xE0 | (value >> 12));
                        utf8[1] = (char)(0x80 | ((value >> 6) & 0x3F));
                        utf8[2] = (char)(0x80 | (value & 0x3F));
                        bytes = 3;
                    } else {
                        utf8[0] = (char)(0xF0 | (value >> 18));
                        utf8[1] = (char)(0x80 | ((value >> 12) & 0x3F));
                        utf8[2] = (char)(0x80 | ((value >> 6) & 0x3F));
                        utf8[3] = (char)(0x80 | (value & 0x3F));
                        bytes = 4;
                    }
                    break;
                }
                default: utf8[0] = *p; bytes = 1; p++; break;   /* \" \\ \/ and anything else */
            }
        } else {
            utf8[0] = *p++;
            bytes = 1;
        }
        /* Capacity is tested after the byte is known and after the closing
         * quote has had its chance. Testing it first meant a 64-character
         * digest into a 65-byte buffer exited one character early, never saw
         * the closing quote, and reported the value absent -- which read as
         * "this receipt has no digest" rather than as "this buffer is one byte
         * too small". */
        if (written + (size_t)bytes + 1 > cap) { whole = 0; continue; }
        for (i = 0; i < bytes; i++) out[written++] = utf8[i];
    }
    out[written] = '\0';
    if (*p == '"') p++;
    else whole = 0;                       /* ran off the end of the document */
    if (end) *end = p;
    if (!whole) out[0] = '\0';
    return whole;
}

/* Where a key's value begins, searching only the outermost object.
 *
 * A flat search for `"key":` finds the key at any depth, which is how a record
 * was selected by an address nested inside its own profile rather than by its
 * own. It also matches inside a value that happens to contain the text, which
 * one copy of this did. Strings are skipped and depth is tracked, so a key
 * matches only where it is actually a key of the object being read. */
static inline const char *bf_lex_json_seek(const char *json, const char *key) {
    size_t klen;
    int depth = 0;
    if (!json || !key) return 0;
    klen = strlen(key);
    for (const char *p = json; *p; p++) {
        if (*p == '"') {
            const char *start = p + 1;
            const char *q = start;
            while (*q && *q != '"') { if (*q == '\\' && q[1]) q++; q++; }
            if (!*q) return 0;
            if (depth == 1 && (size_t)(q - start) == klen && !strncmp(start, key, klen)) {
                const char *after = q + 1;
                while (*after == ' ' || *after == '\t' || *after == '\n' || *after == '\r') after++;
                if (*after == ':') {
                    after++;
                    while (*after == ' ' || *after == '\t' || *after == '\n' || *after == '\r') after++;
                    return after;
                }
            }
            p = q;
            continue;
        }
        if (*p == '{' || *p == '[') depth++;
        else if (*p == '}' || *p == ']') { depth--; if (depth <= 0 && *p == '}') return 0; }
    }
    return 0;
}

/* A dotted path: `session.token` is the token of the session, not whichever
 * token the document happens to contain first.
 *
 * The witnesses that searched flat for "token" found this one by luck -- the
 * reply carries `session.token` and nothing else named token, so a blind
 * search and a correct address agreed. They stop agreeing the day a second
 * token appears, and nothing would say which one the witness had asserted on.
 *
 * A path segment cannot itself contain a dot. No key in this estate does. */
static inline const char *bf_lex_json_seek_path(const char *json, const char *path) {
    char segment[128];
    const char *dot;
    if (!json || !path) return 0;
    while ((dot = strchr(path, '.')) != 0) {
        size_t n = (size_t)(dot - path);
        if (n == 0 || n >= sizeof(segment)) return 0;
        memcpy(segment, path, n);
        segment[n] = '\0';
        json = bf_lex_json_seek(json, segment);
        if (!json || (*json != '{' && *json != '[')) return 0;
        path = dot + 1;
    }
    return bf_lex_json_seek(json, path);
}

/* The same value, located by key or by path. */
static inline int bf_lex_json_string(const char *json, const char *key, char *out, size_t cap) {
    const char *p;
    if (!json || !key || !out || cap == 0) return 0;
    out[0] = '\0';
    p = bf_lex_json_seek_path(json, key);
    if (!p || *p != '"') return 0;
    return bf_lex_json_string_at(p + 1, out, cap, NULL);
}

/* A JSON number or boolean, as written. Kept as text because a receipt's
 * `http_status` is compared, printed and passed on far more often than it is
 * arithmetic, and converting here would lose how the author wrote it. */
static inline int bf_lex_json_scalar(const char *json, const char *key, char *out, size_t cap) {
    const char *p, *q;
    size_t n;
    if (!json || !key || !out || cap == 0) return 0;
    out[0] = '\0';
    p = bf_lex_json_seek_path(json, key);
    if (!p) return 0;
    if (*p == '"') return 0;              /* a string is not a scalar */
    q = p;
    while (*q && *q != ',' && *q != '}' && *q != ']' && *q != '\n') q++;
    while (q > p && (q[-1] == ' ' || q[-1] == '\t')) q--;
    n = (size_t)(q - p);
    if (!n || n >= cap) return 0;
    memcpy(out, p, n);
    out[n] = '\0';
    return 1;
}

/* An HTTP header value. Names are case-insensitive and the first value wins,
 * except that a redirect chain writes several blocks and the last block
 * describes what was actually served -- so the caller resets between blocks. */
static inline int bf_lex_http_header(const char *line, const char *name, char *out, size_t cap) {
    size_t n;
    if (!line || !name || !out || cap == 0) return 0;
    n = strlen(name);
    if (strncasecmp(line, name, n) || line[n] != ':') return 0;
    {
        const char *value = line + n + 1;
        while (*value == ' ' || *value == '\t') value++;
        snprintf(out, cap, "%s", value);
    }
    return 1;
}

/* One relation out of a Link header that carries several. Both the quoted and
 * unquoted spellings appear in the wild. */
static inline int bf_lex_http_link_rel(const char *line, const char *rel, char *out, size_t cap) {
    char quoted[64], bare[64];
    const char *value;
    if (!line || !rel || !out || cap == 0) return 0;
    if (strncasecmp(line, "link:", 5)) return 0;
    value = line + 5;
    while (*value == ' ' || *value == '\t') value++;
    snprintf(quoted, sizeof(quoted), "rel=\"%s\"", rel);
    snprintf(bare, sizeof(bare), "rel=%s", rel);
    if (!strstr(value, quoted) && !strstr(value, bare)) return 0;
    snprintf(out, cap, "%s", value);
    return 1;
}

/* A tab-separated column, 1-indexed. Empty trailing columns are present and
 * empty rather than absent, which a naive split loses. */
static inline int bf_lex_tsv_column(const char *line, int column, char *out, size_t cap) {
    const char *p = line;
    int at = 1;
    if (!line || column < 1 || !out || cap == 0) return 0;
    out[0] = '\0';
    while (at < column) {
        const char *tab = strchr(p, '\t');
        if (!tab) return 0;
        p = tab + 1;
        at++;
    }
    {
        const char *end = strchr(p, '\t');
        size_t n = end ? (size_t)(end - p) : strlen(p);
        while (n && (p[n-1] == '\n' || p[n-1] == '\r')) n--;
        if (n >= cap) return 0;
        memcpy(out, p, n);
        out[n] = '\0';
    }
    return 1;
}

/* A whitespace-separated column of a report table, 1-indexed.
 *
 * This estate prints fixed-width tables and three witnesses read one with a
 * JSON substitution. It matched nothing, sed exited 0, the variable was empty,
 * and the witness asserted on the empty string and passed -- a proof that
 * proved nothing, for as long as it had been written.
 *
 * Runs of spaces or tabs separate columns, so a padded table and a
 * single-space one read the same. A value containing a space cannot be a
 * column; use tsv_column for documents where that matters. */
static inline int bf_lex_text_column(const char *line, int column, char *out, size_t cap) {
    const char *p = line;
    int at = 0;
    if (!line || column < 1 || !out || cap == 0) return 0;
    out[0] = '\0';
    for (;;) {
        const char *start, *end;
        while (*p == ' ' || *p == '\t') p++;
        if (!*p || *p == '\n' || *p == '\r') return 0;
        start = p;
        while (*p && *p != ' ' && *p != '\t' && *p != '\n' && *p != '\r') p++;
        end = p;
        if (++at == column) {
            size_t n = (size_t)(end - start);
            if (n >= cap) return 0;
            memcpy(out, start, n);
            out[n] = '\0';
            return 1;
        }
    }
}

/* An indented `key value` line inside a .yaff block. The whole file's shape is
 * the atlas parser's business; this reads one line the same way every time. */
static inline int bf_lex_yaff_field(const char *line, const char *key, char *out, size_t cap) {
    const char *p = line;
    size_t n;
    if (!line || !key || !out || cap == 0) return 0;
    out[0] = '\0';
    if (!isspace((unsigned char)*p)) return 0;      /* column zero is a block header */
    while (*p == ' ' || *p == '\t') p++;
    n = strlen(key);
    if (strncmp(p, key, n) || (p[n] != ' ' && p[n] != '\0')) return 0;
    p += n;
    while (*p == ' ') p++;
    {
        size_t len = strlen(p);
        while (len && (p[len-1] == '\n' || p[len-1] == '\r')) len--;
        if (len >= cap) return 0;
        memcpy(out, p, len);
        out[len] = '\0';
    }
    return 1;
}

/* A URL query parameter, percent-decoded.
 *
 * Two copies of this existed under one name and they disagreed about malformed
 * input: the web surface refused a truncated escape or a control character,
 * and the spatial frontier passed both through as literal text. The strict
 * reading is the one kept -- a half-decoded escape is a caller error, and a
 * control byte in a coordinate is not a coordinate.
 *
 * A key present with an empty value is skipped rather than returned, and the
 * scan continues, so `?at=&at=2026-01-01` answers with the parameter that has
 * one. */
/* One decoder, two contracts. Coordinates still reject all control bytes.
 * Authored form text admits CR/LF/TAB, carries an explicit empty value, and
 * refuses ambiguous duplicate fields. Internal result: 1=value, 0=absent,
 * -1=invalid or too large. An invalid field must never look like an omission. */
static inline int bf_lex_query_value(const char *query, const char *key, char *out, size_t cap, int form_text) {
    size_t klen; int found = 0;
    if (out && cap) out[0] = '\0';
    if (!query || !key || !out || cap < 2) return -1;
    klen = strlen(key);
    for (const char *part = query; *part; ) {
        const char *amp = strchr(part, '&');
        size_t part_len = amp ? (size_t)(amp - part) : strlen(part);
        if (part_len > klen && !strncmp(part, key, klen) && part[klen] == '=') {
            const char *p = part + klen + 1, *limit = part + part_len;
            size_t used = 0;
            int ok = !found;
            while (ok && p < limit) {
                unsigned char c = (unsigned char)*p++;
                if (c == '%') {
                    if (limit - p < 2 || !isxdigit((unsigned char)p[0]) || !isxdigit((unsigned char)p[1])) { ok = 0; break; }
                    char hex[3] = { p[0], p[1], 0 };
                    c = (unsigned char)strtol(hex, NULL, 16); p += 2;
                } else if (c == '+') c = ' ';
                if ((c < 0x20 && !(form_text && (c == '\r' || c == '\n' || c == '\t'))) ||
                    c == 0x7f || used + 1 >= cap) { ok = 0; break; }
                out[used++] = (char)c;
            }
            if (!ok) { out[0] = '\0'; return -1; }
            out[used] = '\0';
            if (!form_text && used) return 1;
            if (form_text) found = 1;
        }
        if (!amp) break;
        part = amp + 1;
    }
    if (!found) out[0] = '\0';
    return found;
}

static inline int bf_lex_url_query(const char *query, const char *key, char *out, size_t cap) {
    return bf_lex_query_value(query, key, out, cap, 0) > 0;
}

static inline int bf_lex_form_query(const char *form, const char *key, char *out, size_t cap) {
    return bf_lex_query_value(form, key, out, cap, 1);
}

/* A command-line flag's value. Four copies of this existed and they agreed,
 * which is luck rather than design. */
static inline const char *bf_lex_argv_option(int argc, char **argv, const char *option) {
    if (!argv || !option) return 0;
    for (int i = 2; i + 1 < argc; i++)
        if (argv[i] && !strcmp(argv[i], option)) return argv[i + 1];
    return 0;
}

/* A bounded copy that always terminates. Three copies of this existed under
 * one name and more under others. */
static inline void bf_lex_copy_bounded(char *dst, size_t cap, const char *start, size_t len) {
    size_t n;
    if (!dst || cap == 0) return;
    n = len < cap - 1 ? len : cap - 1;
    if (start && n) memcpy(dst, start, n);
    dst[n] = '\0';
}

/* The bounds of the object-th top-level JSON object in a document, counting
 * from zero. Strings and their escapes are respected, so a brace inside a
 * value does not open a record.
 *
 * A document is a sequence of records whether it is written one per line or
 * all on one. Three witnesses selected a record with a greedy `.*` spanning
 * the whole line: on JSON Lines that happened to be right, and on a single
 * line carrying an array it picks whichever id happens to sit nearest the
 * matching email, which is that record's id only by the order the writer chose.
 * Returns 0 when there is no such record. */
static inline int bf_lex_json_record(const char *doc, int index,
                                     const char **begin, const char **end) {
    int depth = 0, at = -1, in_string = 0;
    const char *start = 0;
    if (!doc || index < 0) return 0;
    for (const char *p = doc; *p; p++) {
        if (in_string) {
            if (*p == '\\' && p[1]) p++;
            else if (*p == '"') in_string = 0;
            continue;
        }
        if (*p == '"') { in_string = 1; continue; }
        if (*p == '{') { if (depth++ == 0) { start = p; at++; } }
        else if (*p == '}') {
            if (--depth == 0 && at == index) {
                if (begin) *begin = start;
                if (end) *end = p + 1;
                return 1;
            }
            if (depth < 0) depth = 0;
        }
    }
    return 0;
}

/* ---- emitters -------------------------------------------------------------
 *
 * Reading a document wrongly produces a wrong answer. Writing one wrongly
 * produces a document nothing can read, and the estate had nineteen copies of
 * this with sixteen different opinions about it. Four were defects:
 *
 *   - one passed control bytes through unescaped, emitting invalid JSON from
 *     the address plane's own advertisement writer;
 *   - one dropped every character below 0x20 silently, so a two-line authored
 *     description was emitted as one line with the newline simply gone;
 *   - one reserved four bytes for an escape that needs six, then advanced its
 *     write cursor by six and wrote the terminator past the end of the buffer;
 *   - one was a reader wearing a writer's name, and it turned \t into t.
 *
 * There is one of each shape here. Bytes at or above 0x80 pass through: a JSON
 * document is UTF-8, so escaping them would be a second encoding of something
 * already encoded. */

/* How many bytes the escaped form of `in` needs, not counting the terminator. */
static inline size_t bf_lex_json_escape_len(const char *in, size_t len) {
    size_t need = 0, i;
    if (!in) return 0;
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)in[i];
        if (c == '"' || c == '\\' || c == '\b' || c == '\f' ||
            c == '\n' || c == '\r' || c == '\t') need += 2;
        else if (c < 0x20) need += 6;
        else need += 1;
    }
    return need;
}

/* The escaped body of a JSON string, without its surrounding quotes, into a
 * bounded buffer. Returns 1 when the whole value fit and 0 when it did not --
 * a truncated result is still valid JSON, never half an escape, because the
 * copy that truncated mid-escape produced a document no parser would accept
 * and no caller could detect. */
static inline int bf_lex_json_escape(const char *in, char *out, size_t cap) {
    size_t w = 0;
    if (!out || cap == 0) return 0;
    out[0] = '\0';
    if (!in) return 1;
    for (; *in; in++) {
        unsigned char c = (unsigned char)*in;
        const char *esc = 0;
        char unicode[7];
        size_t n;
        switch (c) {
            case '"':  esc = "\\\""; break;
            case '\\': esc = "\\\\"; break;
            case '\b': esc = "\\b";  break;
            case '\f': esc = "\\f";  break;
            case '\n': esc = "\\n";  break;
            case '\r': esc = "\\r";  break;
            case '\t': esc = "\\t";  break;
            default:
                if (c < 0x20) {
                    snprintf(unicode, sizeof(unicode), "\\u%04x", c);
                    esc = unicode;
                }
        }
        n = esc ? strlen(esc) : 1;
        if (w + n + 1 > cap) { out[w] = '\0'; return 0; }
        if (esc) { memcpy(out + w, esc, n); w += n; }
        else out[w++] = (char)c;
    }
    out[w] = '\0';
    return 1;
}

/* The same value, allocated to exactly the size it needs. The caller frees. */
static inline char *bf_lex_json_escape_alloc(const char *in, size_t len) {
    size_t need = bf_lex_json_escape_len(in, len);
    char *out = (char *)malloc(need + 1);
    char *w = out;
    size_t i;
    if (!out) return 0;
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)in[i];
        switch (c) {
            case '"':  *w++ = '\\'; *w++ = '"';  break;
            case '\\': *w++ = '\\'; *w++ = '\\'; break;
            case '\b': *w++ = '\\'; *w++ = 'b';  break;
            case '\f': *w++ = '\\'; *w++ = 'f';  break;
            case '\n': *w++ = '\\'; *w++ = 'n';  break;
            case '\r': *w++ = '\\'; *w++ = 'r';  break;
            case '\t': *w++ = '\\'; *w++ = 't';  break;
            default:
                if (c < 0x20) { snprintf(w, 7, "\\u%04x", c); w += 6; }
                else *w++ = (char)c;
        }
    }
    *w = '\0';
    return out;
}

/* A complete JSON string, quotes included, to a stream. A null value is the
 * empty string rather than a crash or the word "(null)", because a writer
 * asked for a field it does not have should still emit a document. */
static inline void bf_lex_json_emit(FILE *out, const char *text) {
    const char *p = text ? text : "";
    const char *span = p;
    if (!out) return;
    fputc('"', out);
    for (;; p++) {
        unsigned char c = (unsigned char)*p;
        if (c && c != '"' && c != '\\' && c >= 0x20) continue;
        if (p > span) fwrite(span, 1, (size_t)(p - span), out);
        if (!c) break;
        switch (c) {
            case '"':  fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out);  break;
            case '\f': fputs("\\f", out);  break;
            case '\n': fputs("\\n", out);  break;
            case '\r': fputs("\\r", out);  break;
            case '\t': fputs("\\t", out);  break;
            default: fprintf(out, "\\u%04x", c);
        }
        span = p + 1;
    }
    fputc('"', out);
}

/* Emit ordinary text in spans; only metacharacters need separate writes.
 * The apostrophe matters because several consumers place text in single-quoted
 * attributes. */
static inline void bf_lex_html_emit(FILE *out, const char *text) {
    const char *at = text ? text : "";
    if (!out) return;
    for (;;) {
        const char *special = strpbrk(at, "&<>\"'");
        if (!special) { fputs(at, out); return; }
        if (special > at) fwrite(at, 1, (size_t)(special - at), out);
        switch (*special) {
            case '&': fputs("&amp;", out); break;
            case '<': fputs("&lt;", out); break;
            case '>': fputs("&gt;", out); break;
            case '"': fputs("&quot;", out); break;
            case '\'': fputs("&#039;", out); break;
        }
        at = special + 1;
    }
}

#endif
