#ifndef BONFYRE_SITE_FRAGMENT_PROJECTION_H
#define BONFYRE_SITE_FRAGMENT_PROJECTION_H

#include <stddef.h>
#include <sqlite3.h>

typedef enum {
    BF_SITE_FRAGMENT_HTML = 1,
    BF_SITE_FRAGMENT_HTMX = 2
} BfSiteFragmentRepresentation;

typedef struct {
    const char *namespace_name;
    const char *page;
    const char *slot;
    const char *audience;
    BfSiteFragmentRepresentation representation;
} BfSiteFragmentProjectionRequest;

typedef struct {
    int rows;
    int exact_coordinate_rows;
    int published_rows;
    int contract_rows;
} BfSiteFragmentProjectionStats;

/* Returns:
 *   1: authored projection emitted into out
 *   0: no lawful authored projection; caller should retain its compiled fallback
 *  -1: malformed request, schema/read error, or output overflow
 *
 * This function owns no address, publication, or authority semantics. The
 * caller supplies the already-resolved namespace/page/slot/audience cut. It
 * reads only status=published rows and additionally requires the requested
 * representation in projection_contract. */
int bf_site_fragment_project(sqlite3 *db,
                             const BfSiteFragmentProjectionRequest *request,
                             char *out,
                             size_t out_size,
                             BfSiteFragmentProjectionStats *stats);

#endif
