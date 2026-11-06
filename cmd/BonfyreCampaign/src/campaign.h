#ifndef BONFYRE_CAMPAIGN_H
#define BONFYRE_CAMPAIGN_H
#include <stddef.h>
#include "bf_jsonw.h"

#define BF_CAMPAIGN_REQUEST_MAX 16384
#define BF_CAMPAIGN_URL_MAX 8192
#define BF_CAMPAIGN_VALUE_MAX 256

/* Pure, bounded request: no storage, DNS, identity, consent or tag dispatch.
 * The caller initializes/frees out. Returns 0 on success, 2 on refusal. */
int bf_campaign_request(const char *input, size_t length, bf_jsonw *out);
int bf_campaign_error(bf_jsonw *out, const char *code);
#endif
