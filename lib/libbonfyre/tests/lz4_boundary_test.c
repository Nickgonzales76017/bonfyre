#include "bf_lz4.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef BONFYRE_LZ4_REFERENCE
#include <lz4.h>
#endif

static int roundtrip(size_t len, unsigned seed) {
    uint8_t *plain = malloc(len ? len : 1);
    uint8_t *back = malloc(len ? len : 1);
    size_t bound = bf_lz4_bound(len);
    uint8_t *packed = malloc(bound ? bound : 1);
    if (!plain || !back || !packed) return 0;
    for (size_t i = 0; i < len; i++) {
        seed = seed * 1664525u + 1013904223u;
        plain[i] = (uint8_t)((seed >> 23) & 31u);
    }
    size_t n = bf_lz4_compress(plain, len, packed, bound);
    size_t m = n ? bf_lz4_decompress(packed, n, back, len) : 0;
    int ok = len == 0 ? n == 0 : n > 0 && m == len && memcmp(plain, back, len) == 0;
#ifdef BONFYRE_LZ4_REFERENCE
    if (ok && len > 0) {
        memset(back, 0, len);
        int reference = LZ4_decompress_safe((const char *)packed, (char *)back, (int)n, (int)len);
        ok = reference == (int)len && memcmp(plain, back, len) == 0;
    }
#endif
    free(plain); free(back); free(packed);
    return ok;
}

int main(void) {
    for (size_t len = 0; len < 4096; len++) {
        if (!roundtrip(len, (unsigned)len)) {
            fprintf(stderr, "roundtrip failed at length %zu\n", len);
            return 1;
        }
    }
    for (unsigned seed = 0; seed < 128; seed++) {
        if (!roundtrip(16384, seed)) {
            fprintf(stderr, "roundtrip failed at seed %u\n", seed);
            return 1;
        }
    }
    uint8_t output[16] = {0};
    const uint8_t bad_offset[] = {0, 17, 0};
    if (bf_lz4_decompress(bad_offset, sizeof(bad_offset), output, sizeof(output)) != 0) return 2;
    const uint8_t bad_literal[] = {0xf0, 255};
    if (bf_lz4_decompress(bad_literal, sizeof(bad_literal), output, sizeof(output)) != 0) return 3;
    const uint8_t good_literal[] = {0x10, 'A'};
    if (bf_lz4_decompress(good_literal, sizeof(good_literal), output, 1) != 1 || output[0] != 'A') return 4;
    puts("lz4 boundaries: PASS");
    return 0;
}
