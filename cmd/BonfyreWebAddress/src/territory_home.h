#ifndef BONFYRE_TERRITORY_HOME_H
#define BONFYRE_TERRITORY_HOME_H

#include <sqlite3.h>

/*
 * Apply the published TerritoryPresentation selected by an already-resolved
 * Matrix territory. The CMS owns every human string; this native boundary only
 * addresses explicit template slots. The buffer must be heap-owned.
 * Returns 1 for success, including an independent territory or a CMS fallback.
 */
int bf_apply_territory_home_presentation(sqlite3 *db, char **body, const char *territory);
/* Explicitly installed owner selected by the resolved request host.
 * The caller must supply this across the separate C translation unit. */
int bf_apply_territory_home_presentation_owned(sqlite3 *db, char **body,
                                                const char *territory, const char *cms_namespace);

#endif
