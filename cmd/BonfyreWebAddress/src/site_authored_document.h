#ifndef BONFYRE_SITE_AUTHORED_DOCUMENT_H
#define BONFYRE_SITE_AUTHORED_DOCUMENT_H
#include <sqlite3.h>
#include <stddef.h>
#define BF_SITE_AUTHORED_MAX_HTML 1048576u
#define BF_SITE_AUTHORED_BUFFER_CAP (BF_SITE_AUTHORED_MAX_HTML + 1u)
/* Reproject a RELEASED exact CMS site_page + its allowed site_fragment tree.
 * 1: complete public C-rendered component tree; 0: no admitted authored page;
 * -1: invalid, incomplete or oversized public composition (no partial output).
 * This does not publish, resolve source rights or override existing surfaces. */
int bf_site_authored_document_project(sqlite3 *db,const char *site_id,
 const char *route,char *out,size_t cap,int *rendered_blocks);
#endif
