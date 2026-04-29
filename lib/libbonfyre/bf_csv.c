/*
 * bf_csv.c — RFC 4180 CSV reader/writer
 *
 * Parsing: state machine handles quoted fields, embedded newlines,
 * and doubled-quote escaping. Zero-copy: fields point into the
 * reader's internal line buffer (unquoted in-place).
 */

#include "bf_csv.h"

#include <stdlib.h>
#include <string.h>

/* ── Reader internals ────────────────────── */

#define CSV_INITIAL_BUF  4096
#define CSV_INITIAL_FIELDS 32

struct bf_csv_reader {
    /* input source */
    FILE       *fp;
    const char *mem;
    size_t      mem_len;
    size_t      mem_off;

    char        delim;
    int         has_header;

    /* line buffer (owned) */
    char       *buf;
    size_t      buf_cap;
    size_t      buf_len;

    /* field scratch */
    char       *field_buf;
    size_t      field_cap;

    /* current row */
    bf_csv_field_t *fields;
    size_t          fields_cap;
    bf_csv_row_t    row;

    /* header row (if requested) */
    bf_csv_row_t    header;
    bf_csv_field_t *header_fields;
    int             header_read;
};

/* ── Helpers ─────────────────────────────── */

static int read_char(bf_csv_reader_t *r)
{
    if (r->fp) {
        int c = fgetc(r->fp);
        return c;
    }
    if (r->mem && r->mem_off < r->mem_len) {
        return (unsigned char)r->mem[r->mem_off++];
    }
    return -1;
}

static void unread_char(bf_csv_reader_t *r, int c)
{
    if (c < 0) return;
    if (r->fp) {
        ungetc(c, r->fp);
    } else if (r->mem && r->mem_off > 0) {
        r->mem_off--;
    }
}

static void field_buf_reset(bf_csv_reader_t *r)
{
    r->buf_len = 0;
}

static void field_buf_push(bf_csv_reader_t *r, char c)
{
    if (r->buf_len >= r->buf_cap) {
        r->buf_cap *= 2;
        r->buf = realloc(r->buf, r->buf_cap);
    }
    r->buf[r->buf_len++] = c;
}

static void ensure_fields(bf_csv_reader_t *r, size_t needed)
{
    if (needed > r->fields_cap) {
        while (r->fields_cap < needed) r->fields_cap *= 2;
        r->fields = realloc(r->fields, r->fields_cap * sizeof(bf_csv_field_t));
    }
}

/* ── Create ──────────────────────────────── */

static bf_csv_reader_t *reader_alloc(const bf_csv_opts_t *opts)
{
    bf_csv_reader_t *r = calloc(1, sizeof(*r));
    if (!r) return NULL;

    r->delim = (opts && opts->delimiter) ? opts->delimiter : ',';
    r->has_header = opts ? opts->has_header : 0;

    r->buf_cap = CSV_INITIAL_BUF;
    r->buf = malloc(r->buf_cap);

    r->fields_cap = CSV_INITIAL_FIELDS;
    r->fields = malloc(r->fields_cap * sizeof(bf_csv_field_t));

    if (!r->buf || !r->fields) {
        free(r->buf); free(r->fields); free(r);
        return NULL;
    }
    return r;
}

bf_csv_reader_t *bf_csv_reader_open(FILE *fp, const bf_csv_opts_t *opts)
{
    if (!fp) return NULL;
    bf_csv_reader_t *r = reader_alloc(opts);
    if (r) r->fp = fp;
    return r;
}

bf_csv_reader_t *bf_csv_reader_mem(const char *buf, size_t len,
                                    const bf_csv_opts_t *opts)
{
    if (!buf) return NULL;
    bf_csv_reader_t *r = reader_alloc(opts);
    if (r) { r->mem = buf; r->mem_len = len; }
    return r;
}

void bf_csv_reader_free(bf_csv_reader_t *r)
{
    if (!r) return;
    free(r->buf);
    free(r->fields);
    free(r->header_fields);
    free(r);
}

/* ── Parse one row ───────────────────────── */

/*
 * Parse state machine — returns 1 if a row was parsed, 0 on EOF.
 * Fields in r->row point into r->buf.
 */
static int parse_row(bf_csv_reader_t *r)
{
    field_buf_reset(r);
    size_t nfields = 0;
    int eof_reached = 0;

    for (;;) {
        size_t field_start = r->buf_len;
        int c = read_char(r);

        if (c < 0) {
            eof_reached = 1;
            /* if we have accumulated fields, emit the row */
            if (nfields > 0 || r->buf_len > field_start) {
                goto emit_field;
            }
            return 0;
        }

        if (c == '"') {
            /* quoted field */
            for (;;) {
                c = read_char(r);
                if (c < 0) break; /* unterminated quote — treat as end */
                if (c == '"') {
                    int next = read_char(r);
                    if (next == '"') {
                        /* doubled quote → literal " */
                        field_buf_push(r, '"');
                    } else {
                        /* end of quoted field */
                        unread_char(r, next);
                        break;
                    }
                } else {
                    field_buf_push(r, (char)c);
                }
            }
            /* consume delimiter or newline after closing quote */
            c = read_char(r);
            if (c == r->delim) {
                goto emit_field;
            } else if (c == '\r') {
                int next = read_char(r);
                if (next != '\n') unread_char(r, next);
                goto emit_field_end;
            } else if (c == '\n' || c < 0) {
                eof_reached = (c < 0);
                goto emit_field_end;
            }
            /* unexpected char after close quote — just consume */
            goto emit_field;
        }

        /* unquoted field */
        while (c >= 0 && c != r->delim && c != '\n' && c != '\r') {
            field_buf_push(r, (char)c);
            c = read_char(r);
        }

        if (c == r->delim) {
            goto emit_field;
        } else if (c == '\r') {
            int next = read_char(r);
            if (next != '\n') unread_char(r, next);
            eof_reached = 0;
            goto emit_field_end;
        } else if (c == '\n') {
            goto emit_field_end;
        } else {
            eof_reached = 1;
            goto emit_field_end;
        }

emit_field:
        /* terminate field in buffer */
        field_buf_push(r, '\0');
        ensure_fields(r, nfields + 1);
        r->fields[nfields].data = r->buf + field_start;
        r->fields[nfields].len  = r->buf_len - field_start - 1; /* exclude NUL */
        nfields++;
        continue;

emit_field_end:
        field_buf_push(r, '\0');
        ensure_fields(r, nfields + 1);
        r->fields[nfields].data = r->buf + field_start;
        r->fields[nfields].len  = r->buf_len - field_start - 1;
        nfields++;
        break;
    }

    r->row.fields = r->fields;
    r->row.count  = nfields;
    return 1;
}

const bf_csv_row_t *bf_csv_next_row(bf_csv_reader_t *r)
{
    if (!r) return NULL;

    /* first call: consume header if requested */
    if (r->has_header && !r->header_read) {
        r->header_read = 1;
        if (!parse_row(r)) return NULL;

        /* copy header fields */
        r->header.count = r->row.count;
        r->header_fields = malloc(r->row.count * sizeof(bf_csv_field_t));
        if (r->header_fields) {
            memcpy(r->header_fields, r->row.fields,
                   r->row.count * sizeof(bf_csv_field_t));
            r->header.fields = r->header_fields;
        }
        /* now parse the first data row */
    }

    return parse_row(r) ? &r->row : NULL;
}

const bf_csv_row_t *bf_csv_header(const bf_csv_reader_t *r)
{
    if (!r || !r->header_read || !r->header_fields) return NULL;
    return &r->header;
}

const bf_csv_field_t *bf_csv_field(const bf_csv_row_t *row, size_t col)
{
    if (!row || col >= row->count) return NULL;
    return &row->fields[col];
}

/* ── Writer ──────────────────────────────── */

struct bf_csv_writer {
    FILE *fp;
    char  delim;
    int   fields_in_row; /* count of fields written in current row */
};

bf_csv_writer_t *bf_csv_writer_open(FILE *fp, char delimiter)
{
    if (!fp) return NULL;
    bf_csv_writer_t *w = calloc(1, sizeof(*w));
    if (!w) return NULL;
    w->fp    = fp;
    w->delim = delimiter ? delimiter : ',';
    return w;
}

void bf_csv_writer_free(bf_csv_writer_t *w)
{
    free(w);
}

static int needs_quoting(const char *data, size_t len, char delim)
{
    for (size_t i = 0; i < len; i++) {
        char c = data[i];
        if (c == delim || c == '"' || c == '\n' || c == '\r')
            return 1;
    }
    return 0;
}

int bf_csv_write_field(bf_csv_writer_t *w, const char *data, size_t len)
{
    if (!w) return -1;

    /* separator between fields */
    if (w->fields_in_row > 0) {
        if (fputc(w->delim, w->fp) == EOF) return -1;
    }

    if (!data) { data = ""; len = 0; }

    if (needs_quoting(data, len, w->delim)) {
        if (fputc('"', w->fp) == EOF) return -1;
        for (size_t i = 0; i < len; i++) {
            if (data[i] == '"') {
                if (fputc('"', w->fp) == EOF) return -1;
            }
            if (fputc(data[i], w->fp) == EOF) return -1;
        }
        if (fputc('"', w->fp) == EOF) return -1;
    } else {
        if (fwrite(data, 1, len, w->fp) != len) return -1;
    }

    w->fields_in_row++;
    return 0;
}

int bf_csv_end_row(bf_csv_writer_t *w)
{
    if (!w) return -1;
    if (fputs("\r\n", w->fp) == EOF) return -1;
    w->fields_in_row = 0;
    return 0;
}
