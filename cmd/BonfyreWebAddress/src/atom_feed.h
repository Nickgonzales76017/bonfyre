#ifndef BONFYRE_ATOM_FEED_H
#define BONFYRE_ATOM_FEED_H

#include <sqlite3.h>
#include <stdio.h>

/* Read-only public feed over one resolved AddressPlane subject. The caller owns
 * both database handles and the host resolution; this module cannot broaden
 * subject scope or create material. A missing compiled cut uses published,
 * guest-readable CMS notes for compatibility. */
int bf_atom_feed_serve(FILE *out, sqlite3 *cms, sqlite3 *fabric,
                       const char *subject, const char *normalized_host,
                       const char *default_origin, const char *if_none_match,
                       int head_only, const char *websub_hub);

#endif
