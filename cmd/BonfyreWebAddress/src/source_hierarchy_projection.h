#ifndef BONFYRE_SOURCE_HIERARCHY_PROJECTION_H
#define BONFYRE_SOURCE_HIERARCHY_PROJECTION_H

#include <stddef.h>
#include <stdio.h>

/* Render a bounded human projection of SourceFrontier for the readers already
 * participating in a page. Returns the number of publisher roots rendered.
 * This is read-only presentation: it does not admit, fetch, rank or mutate. */
int bf_source_hierarchy_emit(FILE *out, const char *db_path,
                             const char *const *readers, size_t reader_count,
                             const char *route, unsigned root_limit,
                             unsigned channels_per_family);

#endif
