#ifndef BONFYRE_MODEL_CATALOGUE_INDEX_H
#define BONFYRE_MODEL_CATALOGUE_INDEX_H
#include <stddef.h>

/* Read-only Model Commons index projection.
   1: rendered models; 0: empty; -1: unavailable, invalid or truncated.
   Never authorizes execution, caches, installation or a publication effect.
   The caller owns HTML composition, account actions and price observations. */
int bf_model_catalogue_index(const char *db_path, char *html, size_t cap, size_t *used);
/* Detail projection: 1 rendered, 0 missing node, -1 unavailable. */
int bf_model_catalogue_detail(const char *db_path,const char *ident,
    char *name,size_t name_cap,char *html,size_t cap,size_t *used);
#endif
