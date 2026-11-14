#ifndef BONFYRE_MODEL_CATALOGUE_INDEX_H
#define BONFYRE_MODEL_CATALOGUE_INDEX_H
#include <stddef.h>

/* Read-only Model Commons index projection.
   1: rendered models; 0: empty; -1: unavailable, invalid or truncated.
   Never authorizes execution, caches, installation or a publication effect.
   The caller owns HTML composition, account actions and price observations. */
int bf_model_catalogue_index(const char *db_path, char *html, size_t cap, size_t *used);
/* The same list narrowed to models whose name, description or users contain
   `want` (NULL or empty: every model). A catalogue with models but no match
   still answers 1 and says nothing matched. */
int bf_model_catalogue_find(const char *db_path, const char *want,
    char *html, size_t cap, size_t *used);
/* Detail projection: 1 rendered, 0 missing node, -1 unavailable.
   The output carries the two markers below, in order; the caller places the
   try-it workbench at the first and one price tile at the second. */
#define BF_MODEL_TRY_MARK "<!--ak-mc-try-->"
#define BF_MODEL_PRICE_MARK "<!--ak-mc-price-->"
int bf_model_catalogue_detail(const char *db_path,const char *ident,
    char *name,size_t name_cap,char *html,size_t cap,size_t *used);
#endif
