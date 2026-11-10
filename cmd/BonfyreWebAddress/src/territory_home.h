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

#endif
