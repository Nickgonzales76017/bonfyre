#ifndef BONFYRE_WEB_SUBJECT_ADDRESS_H
#define BONFYRE_WEB_SUBJECT_ADDRESS_H

#include <stddef.h>
#include <sqlite3.h>

/* AddressPlane boundary shared by public pages, feeds and item actions.
 * Territory suffixes are presentation hints; only an exact apex/www host or a
 * published SitePresentation binds a subject. The CMS handle may be NULL for
 * an apex whose binding is built in. */
const char *territory_of(const char *host);
int lower_host(const char *in, char out[256]);
int page_subject_for_host(sqlite3 *cms, const char *host,
                          char *subject, size_t capacity);

#endif
