/*
 * books_xlsx.c — XLSX as an exchange surface for Books (books-workbook.mjs).
 * Decisions stay in run_books; this keeps cell provenance, refuses formulas,
 * errors, merged mapped columns and oversized archives, and never posts,
 * learns or calls a model. Reading follows SheetJS 0.20.3's interpretation
 * of the cells the original module used; writing produces a review workbook
 * with the same cells, formulas and formats.
 */
#include "books.h"
#include "bonfyre.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define MAX_FILE (8 * 1024 * 1024)
#define MAX_EXPANDED (32 * 1024 * 1024)
#define MAX_ROWS 5000
#define MAX_COLUMNS 128
#define MAX_CELLS 200000

static uint32_t u32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint16_t u16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }

/* ── bounded ZIP ────────────────────────────────────────────────────────── */

typedef struct { char *name; uint8_t *data; size_t len; } Member;
typedef struct { Member *m; int n; } Zip;

static Member *member(Zip *z, const char *name) { for (int i = 0; i < z->n; i++) if (!strcmp(z->m[i].name, name)) return &z->m[i]; return NULL; }

static int unsafe_name(const char *n) {
    if (n[0] == '/' || strchr(n, '\\')) return 1;
    for (const char *p = n; *p;) {
        size_t len = strcspn(p, "/");
        if (len == 2 && p[0] == '.' && p[1] == '.') return 1;
        p += len; if (*p) p++;
    }
    return strcasestr(n, "vbaProject") || strcasestr(n, "macrosheets");
}

static Zip bounded_zip(const uint8_t *b, size_t len) {
    if (len < 22 || len > MAX_FILE || u32(b) != 0x04034b50) dp_fail("Choose an XLSX workbook of at most 8 MiB.");
    long end = -1;
    for (long p = (long)len - 22; p >= (long)(len > 65557 ? len - 65557 : 0); p--)
        if (u32(b + p) == 0x06054b50 && (size_t)p + 22 + u16(b + p + 20) == len) { end = p; break; }
    if (end < 0 || u16(b + end + 4) || u16(b + end + 6)) dp_fail("Unsupported XLSX archive.");
    unsigned count = u16(b + end + 10); uint32_t directory = u32(b + end + 16);
    if (!count || count > 2000 || count != u16(b + end + 8) || (uint64_t)directory + u32(b + end + 12) != (uint64_t)end)
        dp_fail("Unsupported XLSX archive directory.");
    Zip z = {calloc(count, sizeof(Member)), 0};
    size_t p = directory, expanded = 0;
    for (unsigned i = 0; i < count; i++) {
        if (p + 46 > (size_t)end || u32(b + p) != 0x02014b50) dp_fail("Invalid XLSX member.");
        unsigned flags = u16(b + p + 8), method = u16(b + p + 10);
        uint32_t size = u32(b + p + 20), raw = u32(b + p + 24), local = u32(b + p + 42);
        unsigned nlen = u16(b + p + 28);
        size_t next = p + 46 + nlen + u16(b + p + 30) + u16(b + p + 32);
        if (next > (size_t)end || (flags & 1) || (method != 0 && method != 8) || raw > MAX_EXPANDED - expanded || (size_t)local + 30 > directory)
            dp_fail("Encrypted, oversized, or unsupported XLSX member.");
        char *name = strndup((const char *)b + p + 46, nlen);
        if (member(&z, name) || unsafe_name(name)) dp_fail("Duplicate, unsafe, or macro workbook member.");
        if (u32(b + local) != 0x04034b50 || u16(b + local + 8) != method) dp_fail("Invalid XLSX member header.");
        unsigned llen = u16(b + local + 26);
        size_t start = (size_t)local + 30 + llen + u16(b + local + 28);
        if (start + size > directory || llen != nlen || memcmp(b + local + 30, name, nlen)) dp_fail("Invalid XLSX member location.");
        uint8_t *out; size_t olen;
        if (method == 0) { out = malloc(size + 1); memcpy(out, b + start, size); olen = size; }
        else {
            size_t cap = MAX_EXPANDED - expanded; if (!cap) cap = 1;
            out = malloc(raw + 1);
            z_stream s; memset(&s, 0, sizeof s);
            if (inflateInit2(&s, -15) != Z_OK) dp_fail("XLSX member cannot be expanded within the workbook limit.");
            s.next_in = (Bytef *)(b + start); s.avail_in = size;
            s.next_out = out; s.avail_out = raw;
            int rc = inflate(&s, Z_FINISH);
            olen = s.total_out;
            int more = rc == Z_BUF_ERROR || (rc == Z_OK && s.avail_out == 0 && s.avail_in);
            inflateEnd(&s);
            if (more) { olen = raw + 1; }   /* produced more than declared */
            else if (rc != Z_STREAM_END) dp_fail("XLSX member cannot be expanded within the workbook limit.");
            if (olen > cap) dp_fail("XLSX member cannot be expanded within the workbook limit.");
        }
        if (olen != raw) dp_fail("XLSX member size does not match.");
        out[olen] = 0;
        expanded += olen;
        z.m[z.n].name = name; z.m[z.n].data = out; z.m[z.n].len = olen; z.n++;
        p = next;
    }
    if (p != (size_t)end || !member(&z, "xl/workbook.xml") || !member(&z, "[Content_Types].xml")) dp_fail("Choose an XLSX workbook.");
    return z;
}

/* ── tiny XML ───────────────────────────────────────────────────────────── */

static char *xml_unescape(const char *s, size_t n) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '&') {
            const char *semi = memchr(s + i, ';', n - i);
            if (semi) {
                size_t len = (size_t)(semi - (s + i)) + 1; const char *e = s + i;
                const char *rep = NULL; char u[8];
                if (!strncmp(e, "&lt;", 4)) rep = "<"; else if (!strncmp(e, "&gt;", 4)) rep = ">"; else if (!strncmp(e, "&amp;", 5)) rep = "&";
                else if (!strncmp(e, "&quot;", 6)) rep = "\""; else if (!strncmp(e, "&apos;", 6)) rep = "'";
                else if (e[1] == '#') {
                    unsigned long cp = e[2] == 'x' || e[2] == 'X' ? strtoul(e + 3, NULL, 16) : strtoul(e + 2, NULL, 10);
                    int k = 0;
                    if (cp < 0x80) u[k++] = (char)cp;
                    else if (cp < 0x800) { u[k++] = (char)(0xC0 | cp >> 6); u[k++] = (char)(0x80 | (cp & 63)); }
                    else if (cp < 0x10000) { u[k++] = (char)(0xE0 | cp >> 12); u[k++] = (char)(0x80 | (cp >> 6 & 63)); u[k++] = (char)(0x80 | (cp & 63)); }
                    else { u[k++] = (char)(0xF0 | cp >> 18); u[k++] = (char)(0x80 | (cp >> 12 & 63)); u[k++] = (char)(0x80 | (cp >> 6 & 63)); u[k++] = (char)(0x80 | (cp & 63)); }
                    u[k] = 0; rep = u;
                }
                if (rep) { bf_re_append(&b, rep, strlen(rep)); i += len - 1; continue; }
            }
        }
        /* OOXML _xHHHH_ escapes */
        if (s[i] == '_' && i + 6 < n && s[i + 1] == 'x' && s[i + 6] == '_') {
            char hex[5] = {s[i + 2], s[i + 3], s[i + 4], s[i + 5], 0}; char *end;
            unsigned long cp = strtoul(hex, &end, 16);
            if (!*end) {
                char u[4]; int k = 0;
                if (cp < 0x80) u[k++] = (char)cp;
                else if (cp < 0x800) { u[k++] = (char)(0xC0 | cp >> 6); u[k++] = (char)(0x80 | (cp & 63)); }
                else { u[k++] = (char)(0xE0 | cp >> 12); u[k++] = (char)(0x80 | (cp >> 6 & 63)); u[k++] = (char)(0x80 | (cp & 63)); }
                bf_re_append(&b, u, (size_t)k); i += 6; continue;
            }
        }
        bf_re_append(&b, s + i, 1);
    }
    return b.buf;
}

/* Value of attribute `name` in the tag starting at `tag` (up to '>'). */
static char *attr(const char *tag, const char *name) {
    const char *end = strchr(tag, '>'); if (!end) return NULL;
    size_t nl = strlen(name);
    for (const char *p = tag; p && p < end; p++) {
        if ((p[-1] == ' ' || p[-1] == '\t' || p[-1] == '\n' || p[-1] == '\r') && !strncmp(p, name, nl) && p[nl] == '=') {
            char q = p[nl + 1]; const char *v = p + nl + 2, *e = strchr(v, q);
            if (!e || e > end) return NULL;
            return xml_unescape(v, (size_t)(e - v));
        }
    }
    return NULL;
}
/* Next tag `<name` (optionally with a namespace prefix) at or after p. */
static const char *next_tag(const char *p, const char *name) {
    size_t nl = strlen(name);
    for (; (p = strchr(p, '<')); p++) {
        const char *q = p + 1; const char *colon = q;
        while (*colon && *colon != ':' && *colon != ' ' && *colon != '>' && *colon != '/') colon++;
        if (*colon == ':') q = colon + 1;
        if (!strncmp(q, name, nl) && (q[nl] == ' ' || q[nl] == '>' || q[nl] == '/' || q[nl] == '\t' || q[nl] == '\n' || q[nl] == '\r')) return p;
    }
    return NULL;
}
static int self_closing(const char *tag) { const char *e = strchr(tag, '>'); return e && e[-1] == '/'; }
/* Text of all <t> inside [p, end) concatenated (shared strings, inline strings). */
static char *text_runs(const char *p, const char *end) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    while ((p = next_tag(p, "t")) && p < end) {
        if (self_closing(p)) { p++; continue; }
        const char *s = strchr(p, '>') + 1, *e = strstr(s, "</");
        if (!e || e > end) break;
        char *t = xml_unescape(s, (size_t)(e - s)); bf_re_append(&b, t, strlen(t)); free(t);
        p = e;
    }
    return b.buf;
}
static const char *close_of(const char *p, const char *name) {
    char *pat = dp_fmt("</%s>", name); const char *e = strstr(p, pat); free(pat);
    if (e) return e;
    /* namespaced close: </x:name> */
    for (const char *q = p; (q = strstr(q, "</")); q += 2) { const char *c = strchr(q, ':'), *g = strchr(q, '>'); if (c && g && c < g && !strncmp(c + 1, name, strlen(name)) && c + 1 + strlen(name) == g) return q; }
    return NULL;
}

/* ── cells ──────────────────────────────────────────────────────────────── */

static void decode_cell(const char *ref, int *r, int *c) {
    int col = 0, row = 0; const char *p = ref;
    while (*p == '$') p++;
    while ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z')) { col = col * 26 + ((*p | 32) - 'a' + 1); p++; }
    while (*p == '$') p++;
    row = atoi(p);
    *c = col - 1; *r = row - 1;
}
static char *encode_cell(int r, int c) {
    char col[8]; int n = 0, x = c + 1;
    while (x > 0) { int m = (x - 1) % 26; col[n++] = (char)('A' + m); x = (x - 1) / 26; }
    char out[24]; int o = 0; while (n) out[o++] = col[--n];
    snprintf(out + o, sizeof out - (size_t)o, "%d", r + 1);
    return strdup(out);
}

typedef struct { int r, c; char t; char *v; int has_v; int has_f; int has_F; char *z; double num; } Cell;
typedef struct { int sr, sc, er, ec; } Range;
typedef struct {
    char *name; int hidden; char *path;
    Cell *cells; int ncells; Range range; int has_range;
    Range *merges; int nmerges; int *hidden_rows; int nhidden;
} Sheet;
typedef struct { Sheet *s; int n; int date1904; char **sst; int nsst; char **fmt_of_xf; int nxf; } Book;

static const char *builtin_fmt(int id) {
    switch (id) {
    case 0: return "General"; case 1: return "0"; case 2: return "0.00"; case 3: return "#,##0"; case 4: return "#,##0.00";
    case 9: return "0%"; case 10: return "0.00%"; case 11: return "0.00E+00"; case 12: return "# ?/?"; case 13: return "# ?\?/?\?";
    case 14: return "m/d/yy"; case 15: return "d-mmm-yy"; case 16: return "d-mmm"; case 17: return "mmm-yy"; case 18: return "h:mm AM/PM";
    case 19: return "h:mm:ss AM/PM"; case 20: return "h:mm"; case 21: return "h:mm:ss"; case 22: return "m/d/yy h:mm";
    case 37: return "#,##0 ;(#,##0)"; case 38: return "#,##0 ;[Red](#,##0)"; case 39: return "#,##0.00;(#,##0.00)"; case 40: return "#,##0.00;[Red](#,##0.00)";
    case 45: return "mm:ss"; case 46: return "[h]:mm:ss"; case 47: return "mmss.0"; case 48: return "##0.0E+0"; case 49: return "@";
    default:
        if ((id >= 27 && id <= 36) || (id >= 50 && id <= 58)) return "m/d/yy";   /* SheetJS default map */
        if (id >= 5 && id <= 8) return builtin_fmt(id + 32);
        return NULL;
    }
}

/* SSF.is_date */
static int fmt_is_date(const char *f) {
    size_t i = 0, n = strlen(f);
    while (i < n) {
        char c = f[i];
        switch (c) {
        case 'G': if (!strncasecmp(f + i, "General", 7)) i += 6; i++; break;
        case '"': for (i++; i < n && f[i] != '"'; i++) {} i++; break;
        case '\\': case '_': i += 2; break;
        case '@': i++; break;
        case 'B': case 'b': if (f[i + 1] == '1' || f[i + 1] == '2') return 1; return 1;
        case 'M': case 'D': case 'Y': case 'H': case 'S': case 'E':
        case 'm': case 'd': case 'y': case 'h': case 's': case 'e': case 'g': return 1;
        case 'A': case 'a':
            if (!strncasecmp(f + i, "A/P", 3) || !strncasecmp(f + i, "AM/PM", 5)) return 1;
            i++; break;
        case '[': {
            size_t j = i + 1; int abs = 1;
            while (j < n && f[j] != ']') { if (!strchr("HhMmSs", f[j])) abs = 0; j++; }
            if (abs && j < n) return 1;
            i = j + 1;
            break;
        }
        case '.': case '0': case '#':
            while (i < n && (strchr("0#?.,E+-%", f[++i]) || (f[i] == '\\' && f[i + 1] == '-' && strchr("0#", f[i + 2])))) {}
            break;
        case '?': while (i < n && f[++i] == c) {} break;
        case '*': i++; if (f[i] == ' ' || f[i] == '*') i++; break;
        case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
            while (i < n && strchr("0123456789", f[++i])) {}
            break;
        default: i++; break;
        }
    }
    return 0;
}

/* SSF.parse_date_code -> y, m, d; 0 when out of range */
static int parse_date_code(double v, int date1904, int *y, int *m, int *d) {
    if (v > 2958465 || v < 0) return 0;
    long date = (long)floor(v);
    double t = 86400 * (v - (double)date); long time = (long)floor(t);
    if (t - (double)time > 0.9999) { if (++time == 86400) date++; }
    if (date1904) date += 1462;
    if (date == 0) { *y = 1900; *m = 1; *d = 0; return 1; }
    if (date == 60) { *y = 1900; *m = 2; *d = 29; return 1; }
    if (date > 60) date--;
    /* Excel day 1 is 1900-01-01; civil_from_days (Hinnant) from 1970-01-01 */
    long z = -25567 + (date - 1) + 719468;
    long era = (z >= 0 ? z : z - 146096) / 146097, doe = z - era * 146097;
    long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365, doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    long mp = (5 * doy + 2) / 153;
    *d = (int)(doy - (153 * mp + 2) / 5 + 1);
    *m = (int)(mp < 10 ? mp + 3 : mp - 9);
    *y = (int)(yoe + era * 400 + (*m <= 2));
    return 1;
}

static void parse_sheet(Book *bk, Sheet *s, const char *xml) {
    const char *dim = next_tag(xml, "dimension");
    if (dim) {
        char *ref = attr(dim, "ref");
        if (ref) {
            char *colon = strchr(ref, ':'); int a, b, c2, d2;
            decode_cell(ref, &a, &b);
            if (colon) decode_cell(colon + 1, &c2, &d2); else { c2 = a; d2 = b; }
            if (d2 >= b && c2 >= a && d2 >= 0 && c2 >= 0) { s->range = (Range){a, b, c2, d2}; s->has_range = 1; }
            free(ref);
        }
    }
    Range guess = {1 << 30, 1 << 30, -1, -1};
    const char *sd = next_tag(xml, "sheetData");
    const char *sd_end = sd ? (self_closing(sd) ? sd : close_of(sd, "sheetData")) : NULL;
    int cap = 0, next_row = 0;
    for (const char *row = sd; row && sd_end && (row = next_tag(row + 1, "row")) && row < sd_end;) {
        char *rr = attr(row, "r"), *hid = attr(row, "hidden");
        int rnum = rr ? atoi(rr) - 1 : next_row;
        next_row = rnum + 1;
        if (hid && (!strcmp(hid, "1") || !strcmp(hid, "true"))) { s->hidden_rows = realloc(s->hidden_rows, sizeof(int) * (size_t)(s->nhidden + 1)); s->hidden_rows[s->nhidden++] = rnum; }
        free(rr); free(hid);
        const char *row_end = self_closing(row) ? row : close_of(row, "row");
        if (!row_end) break;
        int next_col = 0;
        for (const char *c = row; (c = next_tag(c + 1, "c")) && c < row_end;) {
            Cell cell; memset(&cell, 0, sizeof cell);
            char *ref = attr(c, "r"), *t = attr(c, "t"), *st = attr(c, "s");
            if (ref) decode_cell(ref, &cell.r, &cell.c); else { cell.r = rnum; cell.c = next_col; }
            next_col = cell.c + 1;
            const char *c_end = self_closing(c) ? c : close_of(c, "c");
            if (!c_end) break;
            const char *v = next_tag(c, "v"), *f = next_tag(c, "f"), *is = next_tag(c, "is");
            if (f && f < c_end) {
                cell.has_f = 1;
                char *ft = attr(f, "t"); if (ft && !strcmp(ft, "array")) cell.has_F = 1; free(ft);
            }
            char *raw = NULL;
            if (v && v < c_end && !self_closing(v)) { const char *a = strchr(v, '>') + 1, *e = strstr(a, "</"); raw = xml_unescape(a, (size_t)(e - a)); cell.has_v = 1; }
            char type = 'n';
            if (t && !strcmp(t, "s")) type = 's';
            else if (t && !strcmp(t, "str")) type = 'S';
            else if (t && !strcmp(t, "inlineStr")) type = 'i';
            else if (t && !strcmp(t, "b")) type = 'b';
            else if (t && !strcmp(t, "e")) type = 'e';
            else if (t && !strcmp(t, "d")) type = 'd';
            if (type == 's') {
                if (raw) { int k = atoi(raw); free(raw); raw = strdup(k >= 0 && k < bk->nsst ? bk->sst[k] : ""); }
                cell.t = 's';
            } else if (type == 'i') { cell.t = 's'; free(raw); raw = is && is < c_end ? text_runs(is, c_end) : strdup(""); cell.has_v = 1; }
            else if (type == 'S') { cell.t = 's'; if (!raw) { raw = strdup(""); } }
            else if (type == 'b') { cell.t = 'b'; if (raw) { int truth = !strcmp(raw, "1") || !strcmp(raw, "true"); free(raw); raw = strdup(truth ? "true" : "false"); } }
            else if (type == 'e') { cell.t = 'e'; }
            else if (type == 'd') { cell.t = 'd'; }
            else {
                cell.t = 'n';
                if (raw) { cell.num = strtod(raw, NULL); char b[32]; bf_js_number(cell.num, b); free(raw); raw = strdup(b); }
            }
            if (!cell.has_v && !cell.has_f) cell.t = 'z';
            if (st && cell.t == 'n') { int x = atoi(st); if (x >= 0 && x < bk->nxf && bk->fmt_of_xf[x]) cell.z = strdup(bk->fmt_of_xf[x]); }
            else if (cell.t == 'n' && bk->nxf && bk->fmt_of_xf[0]) cell.z = strdup(bk->fmt_of_xf[0]);
            cell.v = raw;
            if (cell.r < guess.sr) guess.sr = cell.r; if (cell.c < guess.sc) guess.sc = cell.c;
            if (cell.r > guess.er) guess.er = cell.r; if (cell.c > guess.ec) guess.ec = cell.c;
            if (s->ncells == cap) s->cells = realloc(s->cells, sizeof(Cell) * (size_t)(cap = cap ? cap * 2 : 256));
            s->cells[s->ncells++] = cell;
            free(ref); free(t); free(st);
        }
        row = row_end;
    }
    if (!s->has_range && guess.er >= guess.sr && guess.ec >= guess.sc) { s->range = guess; s->has_range = 1; }
    const char *mc = xml;
    while ((mc = next_tag(mc + 1, "mergeCell"))) {
        char *ref = attr(mc, "ref"); if (!ref) continue;
        char *colon = strchr(ref, ':'); Range r;
        decode_cell(ref, &r.sr, &r.sc);
        if (colon) decode_cell(colon + 1, &r.er, &r.ec); else { r.er = r.sr; r.ec = r.sc; }
        s->merges = realloc(s->merges, sizeof(Range) * (size_t)(s->nmerges + 1)); s->merges[s->nmerges++] = r;
        free(ref);
    }
}

static Cell *cell_at(Sheet *s, int r, int c) {
    for (int i = s->ncells - 1; i >= 0; i--) if (s->cells[i].r == r && s->cells[i].c == c) return &s->cells[i];
    return NULL;
}

static Book open_book(const uint8_t *bytes, size_t len) {
    Zip z = bounded_zip(bytes, len);
    Book bk; memset(&bk, 0, sizeof bk);
    Member *sst = member(&z, "xl/sharedStrings.xml");
    if (sst) for (const char *p = (const char *)sst->data; (p = next_tag(p + 1, "si"));) {
        const char *e = close_of(p, "si"); if (!e) break;
        bk.sst = realloc(bk.sst, sizeof *bk.sst * (size_t)(bk.nsst + 1)); bk.sst[bk.nsst++] = text_runs(p, e);
        p = e;
    }
    Member *styles = member(&z, "xl/styles.xml");
    if (styles) {
        const char *x = (const char *)styles->data;
        int *ids = NULL; char **codes = NULL; int ncodes = 0;
        for (const char *p = x; (p = next_tag(p + 1, "numFmt"));) {
            char *id = attr(p, "numFmtId"), *code = attr(p, "formatCode");
            if (id && code) { ids = realloc(ids, sizeof(int) * (size_t)(ncodes + 1)); codes = realloc(codes, sizeof(char *) * (size_t)(ncodes + 1)); ids[ncodes] = atoi(id); codes[ncodes++] = code; }
            else free(code);
            free(id);
        }
        const char *xfs = next_tag(x, "cellXfs"), *xfs_end = xfs ? close_of(xfs, "cellXfs") : NULL;
        for (const char *p = xfs; p && xfs_end && (p = next_tag(p + 1, "xf")) && p < xfs_end;) {
            char *id = attr(p, "numFmtId"); int n = id ? atoi(id) : 0; free(id);
            const char *code = NULL;
            for (int k = 0; k < ncodes; k++) if (ids[k] == n) code = codes[k];
            if (!code) code = builtin_fmt(n);
            bk.fmt_of_xf = realloc(bk.fmt_of_xf, sizeof(char *) * (size_t)(bk.nxf + 1)); bk.fmt_of_xf[bk.nxf++] = code ? strdup(code) : NULL;
        }
    }
    Member *wb = member(&z, "xl/workbook.xml"), *rels = member(&z, "xl/_rels/workbook.xml.rels");
    const char *w = (const char *)wb->data;
    const char *pr = next_tag(w, "workbookPr");
    if (pr) { char *d = attr(pr, "date1904"); bk.date1904 = d && (!strcmp(d, "1") || !strcasecmp(d, "true")); free(d); }
    for (const char *p = w; (p = next_tag(p + 1, "sheet"));) {
        Sheet s; memset(&s, 0, sizeof s);
        s.name = attr(p, "name");
        char *state = attr(p, "state"); s.hidden = state && (!strcmp(state, "hidden") || !strcmp(state, "veryHidden")); free(state);
        char *rid = attr(p, "r:id");
        if (rels && rid) for (const char *q = (const char *)rels->data; (q = next_tag(q + 1, "Relationship"));) {
            char *id = attr(q, "Id");
            if (id && !strcmp(id, rid)) { char *target = attr(q, "Target"); s.path = target[0] == '/' ? strdup(target + 1) : dp_fmt("xl/%s", target); free(target); free(id); break; }
            free(id);
        }
        free(rid);
        Member *m = s.path ? member(&z, s.path) : NULL;
        if (m) parse_sheet(&bk, &s, (const char *)m->data);
        bk.s = realloc(bk.s, sizeof(Sheet) * (size_t)(bk.n + 1)); bk.s[bk.n++] = s;
    }
    if (bk.n > 32) dp_fail("A workbook may contain at most 32 sheets.");
    return bk;
}

static char *range_text(const Range *r) {
    char *a = encode_cell(r->sr, r->sc), *b = encode_cell(r->er, r->ec);
    char *o = r->sr == r->er && r->sc == r->ec ? strdup(a) : dp_fmt("%s:%s", a, b);
    free(a); free(b); return o;
}

static uint8_t *workbook_bytes(const char *path, size_t *len) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { if (errno == ENOENT) dp_fail("ENOENT: no such file or directory, open '%s'", path); dp_fail("EACCES: permission denied, open '%s'", path); }
    struct stat st; fstat(fd, &st);
    if (!S_ISREG(st.st_mode) || st.st_size > MAX_FILE) { close(fd); dp_fail("Choose an XLSX file of at most 8 MiB."); }
    uint8_t *b = malloc(MAX_FILE + 2); size_t n = 0;
    for (ssize_t got; n < MAX_FILE + 1 && (got = read(fd, b + n, MAX_FILE + 1 - n)) > 0;) n += (size_t)got;
    close(fd);
    if (n > MAX_FILE) dp_fail("Choose an XLSX file of at most 8 MiB.");
    *len = n;
    return b;
}

static bf_jv *sheet_info(const Sheet *s) {
    bf_jv *o = bf_jv_obj();
    bf_jv_set(o, "name", bf_jv_str(s->name));
    if (s->has_range) { char *r = range_text(&s->range); bf_jv_set(o, "range", bf_jv_str(r)); free(r); } else bf_jv_set(o, "range", bf_jv_null());
    bf_jv_set(o, "rows", bf_jv_num(s->has_range ? s->range.er - s->range.sr + 1 : 0));
    bf_jv_set(o, "columns", bf_jv_num(s->has_range ? s->range.ec - s->range.sc + 1 : 0));
    bf_jv_set(o, "hidden", bf_jv_bool(s->hidden));
    return o;
}

bf_jv *books_inspect_workbook(const char *path) {
    size_t len; uint8_t *bytes = workbook_bytes(path, &len);
    Book bk = open_book(bytes, len);
    char h[65]; bf_sha256_hex(bytes, len, h);
    bf_jv *out = bf_jv_obj(), *sheets = bf_jv_arr();
    bf_jv_set(out, "sha256", bf_jv_str(h));
    bf_jv_set(out, "bytes", bf_jv_num((double)len));
    for (int i = 0; i < bk.n; i++) bf_jv_push(sheets, sheet_info(&bk.s[i]));
    bf_jv_set(out, "sheets", sheets);
    return out;
}

static char *cell_value(Book *bk, Sheet *s, int r, int c) {
    Cell *cell = cell_at(s, r, c);
    if (!cell || cell->t == 'z') return strdup("");
    if (cell->t == 'n' && cell->z && fmt_is_date(cell->z)) {
        int y, m, d;
        if (!parse_date_code(cell->num, bk->date1904, &y, &m, &d)) { char *a = encode_cell(r, c); dp_fail("Invalid date at %s!%s.", s->name, a); }
        return dp_fmt("%04d-%02d-%02d", y, m, d);
    }
    return strdup(cell->v ? cell->v : "");
}

static char *quote(const char *v) {
    bf_re_buf b = {0}; bf_re_append(&b, "\"", 1);
    for (const char *p = v; *p; p++) { if (*p == '"') bf_re_append(&b, "\"\"", 2); else bf_re_append(&b, p, 1); }
    bf_re_append(&b, "\"", 1);
    return b.buf;
}

typedef struct { int row; char *cells_ref[6]; int hidden; } SourceRow;

bf_jv *books_run_workbook(const char *path, const char *chosen, const char *header_row, BooksRun *run, bf_jv *(*runner)(BooksRun *)) {
    size_t len; uint8_t *bytes = workbook_bytes(path, &len);
    Book bk = open_book(bytes, len);
    if (!chosen && bk.n != 1) {
        bf_re_buf b = {0}; bf_re_append(&b, "", 0);
        for (int i = 0; i < bk.n; i++) { if (i) bf_re_append(&b, ", ", 2); bf_re_append(&b, bk.s[i].name, strlen(bk.s[i].name)); }
        dp_fail("Choose a sheet: %s.", b.buf);
    }
    Sheet *s = NULL;
    const char *want = chosen ? chosen : bk.n ? bk.s[0].name : "";
    for (int i = 0; i < bk.n; i++) if (!strcmp(bk.s[i].name, want)) { s = &bk.s[i]; break; }
    if (!s || !s->has_range) dp_fail("The selected sheet is missing or empty.");
    int rows = s->range.er - s->range.sr + 1, columns = s->range.ec - s->range.sc + 1;
    if (rows > MAX_ROWS || columns > MAX_COLUMNS || (long)rows * columns > MAX_CELLS) dp_fail("The selected sheet exceeds 5,000 rows, 128 columns, or 200,000 cells.");
    Range rg = s->range;
    double header;
    if (!header_row) {
        header = rg.sr;
        while (header < rg.er) {
            int any = 0;
            for (int i = 0; i < columns && !any; i++) {
                Cell *c = cell_at(s, (int)header, rg.sc + i);
                if (!c) continue;
                if (c->has_f) any = 1;
                else if (c->t != 'z' && c->v) { char *t = dp_js_trim(c->v); any = *t != 0; free(t); }
            }
            if (any) break;
            header++;
        }
    } else {
        char *end; header = strtod(header_row, &end) - 1;
        if (!*header_row || *end) header = NAN;
    }
    if (header != floor(header) || header < rg.sr || header >= rg.er) dp_fail("Choose a header row followed by transaction rows.");
    int hr = (int)header;
    Row names = {calloc((size_t)columns, sizeof(char *)), columns};
    for (int i = 0; i < columns; i++) names.c[i] = cell_value(&bk, s, hr, rg.sc + i);
    for (int i = 0; i < columns; i++) {
        char *a = dp_js_trim(names.c[i]); for (char *p = a; *p; p++) if (*p >= 'A' && *p <= 'Z') *p += 32;
        for (int j = 0; *a && j < i; j++) {
            char *b = dp_js_trim(names.c[j]); for (char *p = b; *p; p++) if (*p >= 'A' && *p <= 'Z') *p += 32;
            if (!strcmp(a, b)) dp_fail("Column names must be unique.");
            free(b);
        }
        free(a);
    }
    Cols roles = resolve_columns(&names, &run->map);
    for (int m = 0; m < s->nmerges; m++) {
        const Range *mg = &s->merges[m];
        if (mg->er < hr) continue;
        for (int r = 0; r < 6; r++) if (roles.at[r] >= 0 && rg.sc + roles.at[r] >= mg->sc && rg.sc + roles.at[r] <= mg->ec)
            dp_fail("Mapped transaction columns contain merged cells. Unmerge them before import.");
    }
    bf_re_buf csv = {0};
    for (int i = 0; i < columns; i++) { char *q = quote(names.c[i]); if (i) bf_re_append(&csv, ",", 1); bf_re_append(&csv, q, strlen(q)); free(q); }
    /* role order: Object.values(roles) iterates resolve order */
    int order[6], no = 0;
    for (int k = 0; k < run->map.n; k++) if (roles.at[run->map.order[k]] >= 0) order[no++] = run->map.order[k];
    for (int r = 0; r < 6; r++) { int seen = 0; for (int k = 0; k < no; k++) if (order[k] == r) seen = 1; if (!seen && roles.at[r] >= 0) order[no++] = r; }
    SourceRow *src = calloc((size_t)rows + 1, sizeof *src); int nsrc = 0;
    for (int r = hr; r <= rg.er; r++) {
        char **cells = malloc(sizeof(char *) * (size_t)columns); int any = 0;
        for (int i = 0; i < columns; i++) { cells[i] = cell_value(&bk, s, r, rg.sc + i); char *t = dp_js_trim(cells[i]); if (*t) any = 1; free(t); }
        if (r != hr && !any) {
            int formula = 0;
            for (int k = 0; k < no; k++) { Cell *c = cell_at(s, r, rg.sc + roles.at[order[k]]); if (c && c->has_f) formula = 1; }
            if (!formula) { for (int i = 0; i < columns; i++) free(cells[i]); free(cells); continue; }
        }
        SourceRow sr; memset(&sr, 0, sizeof sr);
        for (int k = 0; k < no; k++) {
            int role = order[k];
            char *address = encode_cell(r, rg.sc + roles.at[role]);
            Cell *c = cell_at(s, r, rg.sc + roles.at[role]);
            if (c && (c->has_f || c->has_F || c->t == 'e'))
                dp_fail("%s!%s contains a formula or error. Recalculate and supply reviewed values for %s.", s->name, address, ROLE_NAMES[role]);
            if ((role == 2 || role == 3 || role == 4) && c && c->t == 'n') {
                double x = c->num * 100, rr = floor(x); rr = x - rr >= 0.5 ? rr + 1 : rr;
                if (!(fabs(rr) <= 9007199254740991.0)) dp_fail("%s!%s exceeds the supported money range.", s->name, address);
            }
            sr.cells_ref[role] = address;
        }
        if (r == hr) { for (int i = 0; i < columns; i++) free(cells[i]); free(cells); continue; }
        bf_re_append(&csv, "\n", 1);
        for (int i = 0; i < columns; i++) { char *q = quote(cells[i]); if (i) bf_re_append(&csv, ",", 1); bf_re_append(&csv, q, strlen(q)); free(q); free(cells[i]); }
        free(cells);
        sr.row = r + 1;
        for (int k = 0; k < s->nhidden; k++) if (s->hidden_rows[k] == r) sr.hidden = 1;
        src[nsrc++] = sr;
    }
    if (!nsrc) dp_fail("The selected sheet has no transaction rows.");
    bf_re_append(&csv, "\n", 1);
    run->transactions_text = csv.buf;
    bf_jv *report = runner(run);
    char h[65]; bf_sha256_hex(bytes, len, h);
    char *range = range_text(&rg);
    bf_jv *sw = bf_jv_obj();
    const char *slash = strrchr(path, '/'); (void)slash;
    bf_jv_set(sw, "filename", bf_jv_str(path));
    bf_jv_set(sw, "sha256", bf_jv_str(h));
    bf_jv_set(sw, "sheet", bf_jv_str(s->name));
    bf_jv_set(sw, "range", bf_jv_str(range));
    bf_jv_set(sw, "header_row", bf_jv_num(hr + 1));
    bf_jv_set(sw, "hidden_sheet", bf_jv_bool(s->hidden));
    bf_jv_set(report, "source_workbook", sw);
    bf_jv *tx = bf_jv_get(report, "transactions");
    for (size_t i = 0; i < tx->len && (int)i < nsrc; i++) {
        bf_jv *source = bf_jv_obj(), *cells = bf_jv_obj();
        bf_jv_set(source, "sha256", bf_jv_str(h));
        bf_jv_set(source, "sheet", bf_jv_str(s->name));
        bf_jv_set(source, "row", bf_jv_num(src[i].row));
        for (int k = 0; k < no; k++) bf_jv_set(cells, ROLE_NAMES[order[k]], bf_jv_str(src[i].cells_ref[order[k]]));
        bf_jv_set(source, "cells", cells);
        bf_jv_set(source, "hidden", bf_jv_bool(src[i].hidden));
        bf_jv_set(tx->items[i], "source", source);
    }
    free(range);
    return report;
}

/* ── review workbook ────────────────────────────────────────────────────── */

typedef struct { bf_re_buf b; } X;
static void xs(X *x, const char *s) { bf_re_append(&x->b, s, strlen(s)); }
static void xesc(X *x, const char *s) {
    for (; *s; s++) {
        switch (*s) {
        case '&': xs(x, "&amp;"); break; case '<': xs(x, "&lt;"); break; case '>': xs(x, "&gt;"); break; case '"': xs(x, "&quot;"); break;
        default:
            if ((unsigned char)*s < 0x20 && *s != '\t' && *s != '\n' && *s != '\r') { char e[16]; snprintf(e, sizeof e, "_x%04X_", (unsigned char)*s); xs(x, e); }
            else bf_re_append(&x->b, s, 1);
        }
    }
}

/* cell kinds for the writer */
typedef struct { char type; const char *s; double n; const char *f; int fmt; } WCell;   /* type: 0 none, 's', 'n' */

static void cell_xml(X *x, int r, int c, const WCell *w) {
    if (!w->type) return;
    char *ref = encode_cell(r, c);
    xs(x, "<c r=\""); xs(x, ref); xs(x, "\"");
    if (w->fmt) { char s[16]; snprintf(s, sizeof s, " s=\"%d\"", w->fmt); xs(x, s); }
    if (w->type == 's') { xs(x, " t=\"inlineStr\"><is><t xml:space=\"preserve\">"); xesc(x, w->s); xs(x, "</t></is></c>"); }
    else {
        xs(x, ">");
        if (w->f) { xs(x, "<f>"); xesc(x, w->f); xs(x, "</f>"); }
        char num[32]; bf_js_number(w->n, num); xs(x, "<v>"); xs(x, num); xs(x, "</v></c>");
    }
    free(ref);
}

typedef struct { WCell *c; int rows, cols; const double *widths; int nwidths; const char *autofilter; const char *ref; } WSheet;

static char *sheet_xml(const WSheet *s) {
    X x = {{0}};
    xs(&x, "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">");
    xs(&x, "<dimension ref=\""); xs(&x, s->ref); xs(&x, "\"/><sheetViews><sheetView workbookViewId=\"0\"/></sheetViews>");
    if (s->nwidths) {
        xs(&x, "<cols>");
        for (int i = 0; i < s->nwidths; i++) { char col[128]; snprintf(col, sizeof col, "<col min=\"%d\" max=\"%d\" width=\"%.2f\" customWidth=\"1\"/>", i + 1, i + 1, s->widths[i] + 0.71); xs(&x, col); }
        xs(&x, "</cols>");
    }
    xs(&x, "<sheetData>");
    for (int r = 0; r < s->rows; r++) {
        int any = 0; for (int c = 0; c < s->cols; c++) if (s->c[r * s->cols + c].type) any = 1;
        if (!any) continue;
        char row[32]; snprintf(row, sizeof row, "<row r=\"%d\">", r + 1); xs(&x, row);
        for (int c = 0; c < s->cols; c++) cell_xml(&x, r, c, &s->c[r * s->cols + c]);
        xs(&x, "</row>");
    }
    xs(&x, "</sheetData>");
    if (s->autofilter) { xs(&x, "<autoFilter ref=\""); xs(&x, s->autofilter); xs(&x, "\"/>"); }
    xs(&x, "</worksheet>");
    return x.b.buf;
}

/* ZIP writer: deflate, CRC-32 */
typedef struct { bf_re_buf out, dir; int count; } ZipW;
static void le16(bf_re_buf *b, unsigned v) { uint8_t x[2] = {(uint8_t)v, (uint8_t)(v >> 8)}; bf_re_append(b, (const char *)x, 2); }
static void le32(bf_re_buf *b, uint32_t v) { uint8_t x[4] = {(uint8_t)v, (uint8_t)(v >> 8), (uint8_t)(v >> 16), (uint8_t)(v >> 24)}; bf_re_append(b, (const char *)x, 4); }
static void zip_add(ZipW *z, const char *name, const char *data) {
    size_t len = strlen(data);
    uLong crc = crc32(0L, (const Bytef *)data, (uInt)len);
    uLongf bound = compressBound((uLong)len) + 64; Bytef *comp = malloc(bound);
    z_stream s; memset(&s, 0, sizeof s);
    deflateInit2(&s, 9, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
    s.next_in = (Bytef *)data; s.avail_in = (uInt)len; s.next_out = comp; s.avail_out = (uInt)bound;
    deflate(&s, Z_FINISH); size_t clen = s.total_out; deflateEnd(&s);
    uint32_t offset = (uint32_t)z->out.len; size_t nl = strlen(name);
    le32(&z->out, 0x04034b50); le16(&z->out, 20); le16(&z->out, 0); le16(&z->out, 8); le16(&z->out, 0); le16(&z->out, 0x21);
    le32(&z->out, (uint32_t)crc); le32(&z->out, (uint32_t)clen); le32(&z->out, (uint32_t)len); le16(&z->out, (unsigned)nl); le16(&z->out, 0);
    bf_re_append(&z->out, name, nl); bf_re_append(&z->out, (const char *)comp, clen);
    le32(&z->dir, 0x02014b50); le16(&z->dir, 20); le16(&z->dir, 20); le16(&z->dir, 0); le16(&z->dir, 8); le16(&z->dir, 0); le16(&z->dir, 0x21);
    le32(&z->dir, (uint32_t)crc); le32(&z->dir, (uint32_t)clen); le32(&z->dir, (uint32_t)len); le16(&z->dir, (unsigned)nl);
    le16(&z->dir, 0); le16(&z->dir, 0); le16(&z->dir, 0); le16(&z->dir, 0); le32(&z->dir, 0); le32(&z->dir, offset);
    bf_re_append(&z->dir, name, nl);
    z->count++;
    free(comp);
}
static char *zip_finish(ZipW *z, size_t *len) {
    uint32_t at = (uint32_t)z->out.len;
    bf_re_append(&z->out, z->dir.buf, z->dir.len);
    le32(&z->out, 0x06054b50); le16(&z->out, 0); le16(&z->out, 0); le16(&z->out, (unsigned)z->count); le16(&z->out, (unsigned)z->count);
    le32(&z->out, (uint32_t)z->dir.len); le32(&z->out, at); le16(&z->out, 0);
    *len = z->out.len;
    return z->out.buf;
}

static WCell S(const char *s) { WCell w = {'s', s, 0, NULL, 0}; return w; }
static WCell Nf(double n, int fmt) { WCell w = {'n', NULL, n, NULL, fmt}; return w; }

char *books_review_workbook(const bf_jv *report, size_t *out_len) {
    const bf_jv *rows = bf_jv_get(report, "transactions");
    if (!bf_jv_is(rows, BF_JV_ARR) || rows->len > MAX_ROWS) dp_fail("Invalid Books report.");
    for (size_t i = 0; i < rows->len; i++) {
        const bf_jv *c = bf_jv_get(rows->items[i], "amount_cents");
        if (c && c->type != BF_JV_NULL && !(c->type == BF_JV_NUM && c->n == floor(c->n) && fabs(c->n) <= 9007199254740991.0)) dp_fail("Invalid transaction amount.");
    }
    int n = (int)rows->len, R = n + 1;
    static const char *head[12] = {"Date", "Description", "Amount", "Amount as supplied", "Account", "Suggested account", "Review", "Confidence", "Source sheet", "Source row", "Source amount cell", "Source SHA-256"};
    WSheet tx = {calloc((size_t)R * 12, sizeof(WCell)), R, 12, (const double[]){14, 44, 18, 22, 28, 28, 16, 14, 22, 14, 22, 66}, 12, NULL, NULL};
    for (int c = 0; c < 12; c++) tx.c[c] = S(head[c]);
    char **owned = calloc((size_t)n * 3 + 1, sizeof(char *)); int no = 0;
    for (int i = 0; i < n; i++) {
        const bf_jv *r = rows->items[i], *src = bf_jv_get(r, "source"), *cents = bf_jv_get(r, "amount_cents"), *conf = bf_jv_get(r, "confidence");
        WCell *row = &tx.c[(i + 1) * 12];
        row[0] = S(bf_jv_s(bf_jv_get(r, "date")) ? bf_jv_s(bf_jv_get(r, "date")) : "");
        row[1] = S(bf_jv_s(bf_jv_get(r, "description")) ? bf_jv_s(bf_jv_get(r, "description")) : "");
        if (cents && cents->type == BF_JV_NUM) row[2] = Nf(cents->n / 100, 1);
        row[3] = S(bf_jv_s(bf_jv_get(r, "amount_text")) ? bf_jv_s(bf_jv_get(r, "amount_text")) : "");
        row[4] = S(bf_jv_s(bf_jv_get(r, "account")) ? bf_jv_s(bf_jv_get(r, "account")) : "");
        row[5] = S(bf_jv_s(bf_jv_get(r, "suggested")) ? bf_jv_s(bf_jv_get(r, "suggested")) : "");
        const bf_jv *rv = bf_jv_get(r, "review");
        row[6] = S(rv && rv->type == BF_JV_BOOL && rv->b ? "Required" : "Categorized");
        if (conf && conf->type == BF_JV_NUM) row[7] = Nf(conf->n, 2);
        row[8] = S(bf_jv_s(bf_jv_get(src, "sheet")) ? bf_jv_s(bf_jv_get(src, "sheet")) : "");
        const bf_jv *srow = bf_jv_get(src, "row"); if (!srow || srow->type == BF_JV_NULL) srow = bf_jv_get(r, "line");
        if (srow && srow->type == BF_JV_NUM) row[9] = Nf(srow->n, 0);
        const char *amt = bf_jv_s(bf_jv_path(src, "cells.amount"));
        if (amt) row[10] = S(amt);
        else {
            const char *d = bf_jv_s(bf_jv_path(src, "cells.debit")), *c = bf_jv_s(bf_jv_path(src, "cells.credit"));
            char *j = d && *d && c && *c ? dp_fmt("%s / %s", d, c) : strdup(d && *d ? d : c && *c ? c : "");
            owned[no++] = j; row[10] = S(j);
        }
        row[11] = S(bf_jv_s(bf_jv_get(src, "sha256")) ? bf_jv_s(bf_jv_get(src, "sha256")) : "");
    }
    char *txref = dp_fmt("A1:L%d", R); tx.ref = txref; tx.autofilter = txref;
    /* accounts */
    char **acc = NULL; int na = 0;
    for (int i = 0; i < n; i++) {
        const char *a = bf_jv_s(bf_jv_get(rows->items[i], "account"));
        if (!a || !*a) continue;
        int dup = 0; for (int k = 0; k < na; k++) if (!strcmp(acc[k], a)) dup = 1;
        if (!dup) { acc = realloc(acc, sizeof *acc * (size_t)(na + 1)); acc[na++] = (char *)a; }
    }
    for (int a = 1; a < na; a++) for (int b = a; b > 0 && strcmp(acc[b - 1], acc[b]) > 0; b--) { char *t = acc[b]; acc[b] = acc[b - 1]; acc[b - 1] = t; }
    int last = R > 2 ? R : 2;
    WSheet as = {calloc((size_t)(na + 1) * 4, sizeof(WCell)), na + 1, 4, (const double[]){32, 22, 22, 22}, 4, NULL, NULL};
    as.c[0] = S("Account"); as.c[1] = S("Categorized lines"); as.c[2] = S("Known amount"); as.c[3] = S("Missing amounts");
    char **formulas = calloc((size_t)na * 3 + 1, sizeof(char *));
    for (int k = 0; k < na; k++) {
        int r = k + 2; double count = 0, missing = 0; __int128 total = 0;
        for (int i = 0; i < n; i++) {
            const bf_jv *row = rows->items[i]; const char *a = bf_jv_s(bf_jv_get(row, "account"));
            if (!a || strcmp(a, acc[k])) continue;
            count++;
            const bf_jv *c = bf_jv_get(row, "amount_cents");
            if (c && c->type == BF_JV_NUM) total += (__int128)c->n; else missing++;
        }
        if (total > (__int128)9007199254740991LL || total < -(__int128)9007199254740991LL) dp_fail("Account total exceeds the supported money range.");
        char *criterion = dp_fmt("\"=\"&SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(A%d,\"~\",\"~~\"),\"*\",\"~*\"),\"?\",\"~?\")", r);
        formulas[k * 3] = dp_fmt("COUNTIF('Transactions'!E2:E%d,%s)", last, criterion);
        formulas[k * 3 + 1] = dp_fmt("SUMIF('Transactions'!E2:E%d,%s,'Transactions'!C2:C%d)", last, criterion, last);
        formulas[k * 3 + 2] = dp_fmt("COUNTIFS('Transactions'!E2:E%d,%s,'Transactions'!C2:C%d,\"=\")", last, criterion, last);
        free(criterion);
        WCell *row = &as.c[(k + 1) * 4];
        row[0] = S(acc[k]);
        row[1] = (WCell){'n', NULL, count, formulas[k * 3], 0};
        row[2] = (WCell){'n', NULL, (double)total / 100, formulas[k * 3 + 1], 1};
        row[3] = (WCell){'n', NULL, missing, formulas[k * 3 + 2], 0};
    }
    char *asref = dp_fmt("A1:D%d", na + 1 > 1 ? na + 1 : 1); as.ref = asref;
    /* exceptions */
    int nh = 0; for (int i = 0; i < n; i++) { const bf_jv *rv = bf_jv_get(rows->items[i], "review"); if (rv && rv->type == BF_JV_BOOL && rv->b) nh++; }
    WSheet ex = {calloc((size_t)(nh + 1) * 4, sizeof(WCell)), nh + 1, 4, (const double[]){20, 44, 42, 28}, 4, NULL, NULL};
    ex.c[0] = S("Transaction row"); ex.c[1] = S("Description"); ex.c[2] = S("Reason"); ex.c[3] = S("Suggested account");
    for (int i = 0, k = 1; i < n; i++) {
        const bf_jv *r = rows->items[i], *rv = bf_jv_get(r, "review");
        if (!(rv && rv->type == BF_JV_BOOL && rv->b)) continue;
        WCell *row = &ex.c[k * 4];
        row[0] = Nf(i + 2, 0);
        row[1] = S(bf_jv_s(bf_jv_get(r, "description")) ? bf_jv_s(bf_jv_get(r, "description")) : "");
        const char *reason = bf_jv_s(bf_jv_get(r, "review_reason"));
        row[2] = S(reason && *reason ? reason : "Account needs review");
        row[3] = S(bf_jv_s(bf_jv_get(r, "suggested")) ? bf_jv_s(bf_jv_get(r, "suggested")) : "");
        k++;
    }
    char *exref = dp_fmt("A1:D%d", nh + 1); ex.ref = exref;
    /* sources */
    const bf_jv *sw = bf_jv_get(report, "source_workbook");
    const bf_jv *hr = bf_jv_get(sw, "header_row");
    WSheet so = {calloc(11 * 2, sizeof(WCell)), 11, 2, (const double[]){26, 92}, 2, NULL, "A1:B11"};
    const char *labels[11] = {"Property", "Input file", "SHA-256", "Sheet", "Range", "Header row", "Hidden source sheet", "Status", "Account totals", "Calculation", "Source formulas"};
    for (int i = 0; i < 11; i++) so.c[i * 2] = S(labels[i]);
    so.c[1] = S("Value");
    so.c[3] = S(bf_jv_s(bf_jv_get(sw, "filename")) ? bf_jv_s(bf_jv_get(sw, "filename")) : "CSV input");
    so.c[5] = S(bf_jv_s(bf_jv_get(sw, "sha256")) ? bf_jv_s(bf_jv_get(sw, "sha256")) : "");
    so.c[7] = S(bf_jv_s(bf_jv_get(sw, "sheet")) ? bf_jv_s(bf_jv_get(sw, "sheet")) : "");
    so.c[9] = S(bf_jv_s(bf_jv_get(sw, "range")) ? bf_jv_s(bf_jv_get(sw, "range")) : "");
    so.c[11] = Nf(hr && hr->type == BF_JV_NUM ? hr->n : 1, 0);
    const bf_jv *hs = bf_jv_get(sw, "hidden_sheet");
    so.c[13] = S(hs && hs->type == BF_JV_BOOL && hs->b ? "Yes" : "No");
    so.c[15] = S("Categorization draft. No ledger entries have been posted.");
    so.c[17] = S("Known amounts only. Review missing amounts before using totals.");
    so.c[19] = S("Account formulas recalculate in a compatible spreadsheet application.");
    so.c[21] = S("Formula-based input fields require recalculation and reviewed values before import.");
    ZipW z; memset(&z, 0, sizeof z);
    zip_add(&z, "[Content_Types].xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
        "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/>"
        "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
        "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
        "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
        "<Override PartName=\"/xl/worksheets/sheet3.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
        "<Override PartName=\"/xl/worksheets/sheet4.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
        "<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/></Types>");
    zip_add(&z, "_rels/.rels",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>");
    zip_add(&z, "xl/workbook.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<workbookPr/><sheets><sheet name=\"Transactions\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"Accounts\" sheetId=\"2\" r:id=\"rId2\"/>"
        "<sheet name=\"Exceptions\" sheetId=\"3\" r:id=\"rId3\"/><sheet name=\"Sources\" sheetId=\"4\" r:id=\"rId4\"/></sheets>"
        "<calcPr calcMode=\"auto\" fullCalcOnLoad=\"1\"/></workbook>");
    zip_add(&z, "xl/_rels/workbook.xml.rels",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>"
        "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet2.xml\"/>"
        "<Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet3.xml\"/>"
        "<Relationship Id=\"rId4\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet4.xml\"/>"
        "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/></Relationships>");
    zip_add(&z, "xl/styles.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
        "<numFmts count=\"2\"><numFmt numFmtId=\"164\" formatCode=\"#,##0.00;[Red](#,##0.00)\"/><numFmt numFmtId=\"165\" formatCode=\"0.0%\"/></numFmts>"
        "<fonts count=\"1\"><font><sz val=\"12\"/><name val=\"Calibri\"/></font></fonts><fills count=\"2\"><fill><patternFill patternType=\"none\"/></fill><fill><patternFill patternType=\"gray125\"/></fill></fills>"
        "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
        "<cellXfs count=\"3\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/><xf numFmtId=\"164\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>"
        "<xf numFmtId=\"165\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/></cellXfs>"
        "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles></styleSheet>");
    char *x1 = sheet_xml(&tx), *x2 = sheet_xml(&as), *x3 = sheet_xml(&ex), *x4 = sheet_xml(&so);
    zip_add(&z, "xl/worksheets/sheet1.xml", x1); zip_add(&z, "xl/worksheets/sheet2.xml", x2);
    zip_add(&z, "xl/worksheets/sheet3.xml", x3); zip_add(&z, "xl/worksheets/sheet4.xml", x4);
    free(x1); free(x2); free(x3); free(x4);
    for (int i = 0; i < no; i++) free(owned[i]);
    for (int i = 0; i < na * 3; i++) free(formulas[i]);
    free(owned); free(formulas); free(acc); free(tx.c); free(as.c); free(ex.c); free(so.c); free(txref); free(asref); free(exref);
    return zip_finish(&z, out_len);
}
