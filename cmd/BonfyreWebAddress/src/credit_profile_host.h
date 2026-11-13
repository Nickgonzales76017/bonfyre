#ifndef BONFYRE_CREDIT_PROFILE_HOST_H
#define BONFYRE_CREDIT_PROFILE_HOST_H
/* An observed credit gets a stable public host without claiming person identity.
 * Same bytes in People and in the host exporter; source identity is part of
 * the digest. This does not authorize editing or verified member features. */
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "tenant_host_binding.h"
#include "compat/aurekai_legacy.h"
#include <bonfyre.h>

static inline int bf_credit_profile_host(const char *by, const char *source, char out[128]) {
    size_t n = by ? strlen(by) : 0, m = source ? strlen(source) : 0, used = 0;
    char label[24] = {0}, hex[65];
    uint8_t digest[32], separator = 0;
    BfSha256 sha;
    if (!out) return 0;
    out[0] = 0;
    if (!n || n > 512 || !m || m > 4096) return 0;
    for (size_t i = 0; i < n && used < 22; i++) {
        unsigned char c = (unsigned char)by[i];
        if (c >= 'A' && c <= 'Z') c = (unsigned char)(c - 'A' + 'a');
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))) c = '-';
        if (c == '-' && (!used || label[used-1] == '-')) continue;
        label[used++] = (char)c;
    }
    while (used && label[used-1] == '-') used--;
    label[used] = 0;
    if (!used) snprintf(label, sizeof(label), "maker");
    bf_sha256_init(&sha);
    bf_sha256_update(&sha, (const uint8_t *)by, n);
    bf_sha256_update(&sha, &separator, 1);
    bf_sha256_update(&sha, (const uint8_t *)source, m);
    bf_sha256_final(&sha, digest);
    bf_sha256_digest_hex(digest, hex);
    const char *base=NULL;
    int tenant_state=bf_tenant_hosts_status();
    if(tenant_state==1){
        /* The operator must name the destination. A source hash is not
         * permission to mint a new host in an arbitrary institution. */
        const char *territory=getenv("BONFYRE_CREDIT_PROFILE_TERRITORY");
        base=bf_tenant_public_host(territory);
    }else if(tenant_state==0)base=bf_legacy_aurekai_public_host("work");
    if(!base)return 0;
    int written=snprintf(out,128,"credited-%s-%.20s.%s",label,hex,base);
    if(written<=0||written>=128){out[0]=0;return 0;}
    return 1;
}
#endif
