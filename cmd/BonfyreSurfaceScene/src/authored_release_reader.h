#ifndef BONFYRE_AUTHORED_RELEASE_READER_H
#define BONFYRE_AUTHORED_RELEASE_READER_H
#include <stddef.h>
/* Immutable published Aurekai site byte projection. No authorization or
 * publication occurs here: the caller MUST first obtain exact namespace,
 * CMS aggregate revision, and publication identity from the native owner.
 * This reader verifies all three against the sealed release manifest and
 * the requested member's FILES.tsv and page digest before returning bytes.
 * 1=verified complete member, 0=missing route/member, -1=invalid/oversized.
 * Caller output remains empty on failure. */
int bf_authored_release_page(const char *root,const char *namespace_id,
 const char *release_id,const char *cms_revision,const char *route,
 char *output,size_t cap,size_t *written);
int bf_authored_release_asset(const char *root,const char *namespace_id,
 const char *release_id,const char *cms_revision,const char *asset_path,
 char *output,size_t cap,size_t *written);
#endif
