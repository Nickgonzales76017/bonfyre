#ifndef BF_SITE_PRESENTATION_H
#define BF_SITE_PRESENTATION_H
#include <stdio.h>
#include <sqlite3.h>
/* One closed presentation contract for native homepages and standing pages. */
int bf_site_presentation_validate(sqlite3 *db, const char *json, const char **why);
void bf_site_presentation_emit(FILE *out, sqlite3 *db, const char *profile);
int bf_site_presentation_emit_scoped(FILE *out, sqlite3 *db, const char *profile, const char *selector);
void bf_site_presentation_attributes(FILE *out, sqlite3 *db, const char *profile);
#endif
