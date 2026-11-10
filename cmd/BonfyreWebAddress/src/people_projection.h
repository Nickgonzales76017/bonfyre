#ifndef BONFYRE_PEOPLE_PROJECTION_H
#define BONFYRE_PEOPLE_PROJECTION_H

#include <stdio.h>

/* The makers this estate has read, a page at a time: a name a publisher
 * printed beside something, never an account or a verified identity. */
int bf_people_index(FILE *out, const char *archive_path, const char *route,
                    const char *needle, unsigned offset, unsigned page_size);
int bf_people_index_ordered(FILE *out, const char *archive_path, const char *route,
                            const char *needle, unsigned offset, unsigned page_size, int recent_first);

/* One source-bound credited name, and what that exact source printed under it.
 * A legacy name-only address produces a source chooser; it never joins same
 * spelling from distinct sources into an asserted person. */
int bf_person_page(FILE *out, const char *archive_path, const char *who,
                   const char *source, const char *route);

/* The institutions whose channels this estate actually reads. Being listed is
 * this estate's decision to read them, never a relationship either agreed to. */
int bf_organisations_index(FILE *out, const char *ledger_path, const char *route,
                           const char *needle, unsigned offset, unsigned page_size);

/* Published personal home for an exact person:<slug> identity only. Never
 * derive a host from a publisher credit or an unclaimed name. Work is primary.
 * Existing published Life/Live/Shop homes are fallback when Work is absent. */
int bf_public_person_home(const char *subject, char *out, size_t cap);

/* A printed credit that reads as a person's name; filters, never joins. */
int bf_people_name_plausible(const char *s);

#endif
