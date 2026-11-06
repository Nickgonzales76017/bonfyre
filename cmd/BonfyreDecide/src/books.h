/* books.h — what books.c shares with its XLSX exchange (books_xlsx.c). */
#ifndef BOOKS_H
#define BOOKS_H
#include "dp.h"

typedef struct { char **c; int n; } Row;
typedef struct { int at[6]; int mapped_amount; } Cols;   /* -1 = undefined */
typedef struct { char *role[6]; char *given[6]; int order[6]; int n; } Mapping;
typedef struct { char *transactions_text, *chart_text, *learned_bytes; size_t learned_len; Mapping map; DpLocal *local; } BooksRun;

extern const char *ROLE_NAMES[6];   /* date, description, debit, credit, amount, category */
Cols   resolve_columns(const Row *header, const Mapping *map);
bf_jv *run_books(BooksRun *run);

bf_jv *books_inspect_workbook(const char *path);
bf_jv *books_run_workbook(const char *path, const char *sheet, const char *header_row, BooksRun *run, bf_jv *(*runner)(BooksRun *));
char  *books_review_workbook(const bf_jv *report, size_t *len);
#endif
