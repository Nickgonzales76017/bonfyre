#ifndef BONFYRE_SOURCE_DEPTH_PROJECTION_H
#define BONFYRE_SOURCE_DEPTH_PROJECTION_H

#include <stddef.h>
#include <stdio.h>

/* One publisher's whole lattice, or one family inside it, as a walkable page.
 * The channels page shows the publishers a route is already reading; this is
 * how a person walks past them into everything the estate has listed, a
 * bounded page at a time.
 *
 * Read-only presentation over the source ledger: it admits nothing, fetches
 * nothing, ranks nothing and states what each channel's standing actually is.
 * Returns the number of channels rendered. */
int bf_source_depth_emit(FILE *out, const char *db_path, const char *host,
                         const char *family, const char *route,
                         unsigned offset, unsigned page_size);

/* The publishers themselves, a page at a time, newest listings first: the way
 * into a ledger of thousands of roots without naming any of them in code. */
int bf_source_depth_roots(FILE *out, const char *db_path, const char *route,
                          const char *needle, unsigned offset, unsigned page_size);

/* The shape of the whole ledger, in words: how much is listed, how much is
 * read, what the ways in are, what the estate has refused and why, and the
 * publishers holding the most.  This is what a person meets before they have
 * asked for anything in particular. */
int bf_source_depth_index(FILE *out, const char *db_path, const char *route,
                          const char *needle);

/* One cut through every publisher at once: all the channels read one way, or
 * publishing one kind of thing, or standing in one state.  A ledger of
 * thousands is unusable if the only way through it is publisher by publisher. */
int bf_source_depth_slice(FILE *out, const char *db_path, const char *route,
                          const char *way, const char *publishes, const char *state,
                          unsigned offset, unsigned page_size);

/* One channel: its standing, where it was found, what it was read as, what it
 * carries beyond its family, and the things this estate kept from it.  The
 * floor of the lattice, where the ledger meets the pages. */
int bf_source_depth_channel(FILE *out, const char *db_path, const char *address,
                            const char *route, const char *archive_path);

#endif
