#ifndef BONFYRE_SPINES_SHA256_COMPAT_H
#define BONFYRE_SPINES_SHA256_COMPAT_H
#include "bonfyre.h"
#include <stddef.h>
#include <stdint.h>
static int bf_sha256(const unsigned char *data, size_t length, unsigned char out[32]) {
    BfSha256 state;
    bf_sha256_init(&state);
    bf_sha256_update(&state, data, length);
    bf_sha256_final(&state, out);
    return 0;
}
static void bf_hex(const unsigned char *bytes, size_t length, char *out) {
    static const char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < length; i++) {
        out[2 * i] = digits[bytes[i] >> 4];
        out[2 * i + 1] = digits[bytes[i] & 15];
    }
    out[2 * length] = '\0';
}
#endif
