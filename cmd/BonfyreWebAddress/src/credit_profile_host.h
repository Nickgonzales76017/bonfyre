#ifndef BONFYRE_CREDIT_PROFILE_HOST_H
#define BONFYRE_CREDIT_PROFILE_HOST_H
/* An observed credit gets a stable public host without claiming person identity.
 * Same bytes in People and in the host exporter; source identity is part of
 * the digest. This does not authorize editing or verified member features. */
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
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
    int written = snprintf(out, 128, "credited-%s-%.20s.aurekai.work", label, hex);
    return written > 0 && written < 128;
}
#endif
