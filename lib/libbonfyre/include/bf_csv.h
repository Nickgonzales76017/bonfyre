/*
 * bf_csv.h — RFC 4180 CSV reader/writer
 *
 * Features:
 *   - Streaming row iterator (zero-copy field access into source buffer)
 *   - Proper quoted-field handling (embedded commas, newlines, doubled quotes)
 *   - Configurable delimiter (comma, tab, pipe, etc.)
 *   - Writer with automatic quoting/escaping
 */

#ifndef BF_CSV_H
#define BF_CSV_H

#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Reader ──────────────────────────────── */

typedef struct bf_csv_reader bf_csv_reader_t;

/* A single field: pointer into reader's buffer + length. */
typedef struct {
    const char *data;
    size_t      len;
} bf_csv_field_t;

/* A parsed row: array of fields. */
typedef struct {
    bf_csv_field_t *fields;
    size_t          count;
} bf_csv_row_t;

typedef struct {
    char   delimiter;   /* '\0' = default (',') */
    int    has_header;  /* non-zero: first row is header */
} bf_csv_opts_t;

/*
 * Create a reader from a FILE* stream.
 * Reads lazily — one row at a time.
 */
bf_csv_reader_t *bf_csv_reader_open(FILE *fp, const bf_csv_opts_t *opts);

/*
 * Create a reader from an in-memory buffer (not copied).
 * The buffer must remain valid for the reader's lifetime.
 */
bf_csv_reader_t *bf_csv_reader_mem(const char *buf, size_t len,
                                    const bf_csv_opts_t *opts);

void bf_csv_reader_free(bf_csv_reader_t *r);

/*
 * Read the next row. Returns a pointer to an internal row struct,
 * or NULL on EOF / error. The row is valid until the next call to
 * bf_csv_next_row() or bf_csv_reader_free().
 */
const bf_csv_row_t *bf_csv_next_row(bf_csv_reader_t *r);

/*
 * If has_header was set, returns the header row (available after first
 * bf_csv_next_row()). Returns NULL if no header.
 */
const bf_csv_row_t *bf_csv_header(const bf_csv_reader_t *r);

/*
 * Convenience: get field by column index, or NULL if out of range.
 */
const bf_csv_field_t *bf_csv_field(const bf_csv_row_t *row, size_t col);

/* ── Writer ──────────────────────────────── */

typedef struct bf_csv_writer bf_csv_writer_t;

bf_csv_writer_t *bf_csv_writer_open(FILE *fp, char delimiter);
void             bf_csv_writer_free(bf_csv_writer_t *w);

/* Write a single field (auto-quoted if needed). */
int bf_csv_write_field(bf_csv_writer_t *w, const char *data, size_t len);

/* End current row (writes line terminator). */
int bf_csv_end_row(bf_csv_writer_t *w);

#ifdef __cplusplus
}
#endif

#endif /* BF_CSV_H */
