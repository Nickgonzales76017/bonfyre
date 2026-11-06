/* ---------- reading a page's embedded state ----------
 *
 * A page that builds itself from script still ships its words: they sit in the
 * script as data the page will later draw (a serialized state table, a JSON
 * island, a stream of chunks).  Those words can be read without running
 * anything.  Every string literal inside the page's scripts is decoded; a
 * string that is itself serialized data is opened and read the same way; what
 * reads as written language is kept, in document order, once.
 *
 * What this gives is the page's prose, not its layout: who said what, and any
 * heading structure the page draws at run time, are not recovered.
 */

typedef struct {
    unsigned long long *seen;
    size_t seen_count, seen_cap;
    FILE *out;
    long written;
    long passages;
} StateReader;

static int state_seen(StateReader *r, const char *text, size_t len) {
    unsigned long long h = 1469598103934665603ULL;
    for (size_t i = 0; i < len; i++) { h ^= (unsigned char)text[i]; h *= 1099511628211ULL; }
    for (size_t i = 0; i < r->seen_count; i++) if (r->seen[i] == h) return 1;
    if (r->seen_count == r->seen_cap) {
        size_t next = r->seen_cap ? r->seen_cap * 2 : 1024;
        unsigned long long *grown = realloc(r->seen, next * sizeof(*grown));
        if (!grown) return 0;
        r->seen = grown;
        r->seen_cap = next;
    }
    r->seen[r->seen_count++] = h;
    return 0;
}

/* Written language: several words, mostly letters, and not code or markup. */
static int reads_as_prose(const char *text, size_t len) {
    size_t letters = 0, spaces = 0, code = 0, high = 0;
    if (len < 12) return 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)text[i];
        if (c >= 0x80) { high++; letters++; }
        else if (isalpha(c)) letters++;
        else if (c == ' ' || c == '\n') spaces++;
        else if (strchr("{}<>;=\\|", c)) code++;
    }
    (void)high;
    if (spaces < 2) return 0;
    if (letters * 100 < len * 60) return 0;
    if (code * 100 > len * 2) return 0;
    if (!strncmp(text, "http", 4) || !strncmp(text, "data:", 5)) return 0;
    return 1;
}

static size_t put_utf8(char *out, unsigned long code) {
    if (code < 0x80) { out[0] = (char)code; return 1; }
    if (code < 0x800) { out[0] = (char)(0xC0 | (code >> 6)); out[1] = (char)(0x80 | (code & 0x3F)); return 2; }
    if (code < 0x10000) {
        out[0] = (char)(0xE0 | (code >> 12)); out[1] = (char)(0x80 | ((code >> 6) & 0x3F));
        out[2] = (char)(0x80 | (code & 0x3F));
        return 3;
    }
    out[0] = (char)(0xF0 | (code >> 18)); out[1] = (char)(0x80 | ((code >> 12) & 0x3F));
    out[2] = (char)(0x80 | ((code >> 6) & 0x3F)); out[3] = (char)(0x80 | (code & 0x3F));
    return 4;
}

static int hex_value(const char *p, int digits, unsigned long *value) {
    *value = 0;
    for (int i = 0; i < digits; i++) {
        unsigned char c = (unsigned char)p[i];
        if (!isxdigit(c)) return 0;
        *value = *value * 16 + (unsigned long)(isdigit(c) ? c - '0' : tolower(c) - 'a' + 10);
    }
    return 1;
}

/* Decodes the string literal starting after its opening quote.  Returns the
 * position after the closing quote, or NULL when the literal never closes. */
static const char *decode_literal(const char *p, const char *end, char quote, char *out, size_t *out_len) {
    size_t n = 0;
    for (; p < end && *p != quote; p++) {
        unsigned long code;
        if (*p == '\n' && quote != '`') return NULL;
        if (*p != '\\') { out[n++] = *p; continue; }
        if (++p >= end) return NULL;
        switch (*p) {
            case 'n': out[n++] = '\n'; break;
            case 't': out[n++] = ' '; break;
            case 'r': break;
            case 'b': case 'f': case 'v': out[n++] = ' '; break;
            case 'x':
                if (p + 2 < end && hex_value(p + 1, 2, &code)) { n += put_utf8(out + n, code); p += 2; }
                break;
            case 'u':
                if (p + 4 < end && hex_value(p + 1, 4, &code)) {
                    unsigned long low;
                    p += 4;
                    if (code >= 0xD800 && code < 0xDC00 && p + 6 < end && p[1] == '\\' && p[2] == 'u' &&
                        hex_value(p + 3, 4, &low) && low >= 0xDC00 && low < 0xE000) {
                        code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                        p += 6;
                    }
                    if (code >= 0xD800 && code < 0xE000) code = ' ';
                    n += put_utf8(out + n, code ? code : ' ');
                }
                break;
            default: out[n++] = *p;
        }
    }
    if (p >= end) return NULL;
    out[n] = '\0';
    *out_len = n;
    return p + 1;
}

static void read_state(StateReader *r, const char *text, size_t len, int depth) {
    const char *end = text + len;
    char *buffer = malloc(len + 8);
    if (!buffer) return;
    for (const char *p = text; p < end; p++) {
        const char *after;
        size_t n = 0, start = 0;
        if (*p != '"' && !(depth == 0 && (*p == '\'' || *p == '`'))) continue;
        after = decode_literal(p + 1, end, *p, buffer, &n);
        if (!after) continue;
        p = after - 1;
        while (start < n && isspace((unsigned char)buffer[start])) start++;
        while (n > start && isspace((unsigned char)buffer[n - 1])) n--;
        if (n - start < 12) continue;
        /* A string that is itself serialized data is opened, not kept. */
        if (depth < 3 && (buffer[start] == '[' || buffer[start] == '{') && memchr(buffer + start, '"', n - start)) {
            read_state(r, buffer + start, n - start, depth + 1);
            continue;
        }
        if (!reads_as_prose(buffer + start, n - start) || state_seen(r, buffer + start, n - start)) continue;
        /* Each string is its own piece of the page: a break line between
         * them keeps a heading inside one from being read over the next. */
        if (r->passages) fputs("---\n\n", r->out);
        fwrite(buffer + start, 1, n - start, r->out);
        fputs("\n\n", r->out);
        r->written += (long)(n - start);
        r->passages++;
    }
    free(buffer);
}

/* Reads every script body in the page.  Returns the bytes of prose written. */
static long page_state_to_text(const char *html, FILE *out, long *passages) {
    StateReader reader = {0};
    reader.out = out;
    for (const char *p = find_nocase(html, "<script"); p; p = find_nocase(p, "<script")) {
        const char *body = strchr(p, '>');
        const char *close = body ? find_nocase(body, "</script") : NULL;
        if (!close) break;
        read_state(&reader, body + 1, (size_t)(close - body - 1), 0);
        p = close + 8;
    }
    free(reader.seen);
    *passages = reader.passages;
    return reader.written;
}
