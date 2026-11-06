#ifndef BF_STATIC_QUERY_H
#define BF_STATIC_QUERY_H

#include <stddef.h>

/* Encode a UTF-8 byte string as an encodeURIComponent-compatible query value.
 * Callers reserve at least 3 * length bytes. The returned pointer is the end
 * of the encoded span; an embedded NUL is encoded as %00, not a terminator. */
static inline char *bf_static_query_encode(char *out, const unsigned char *value, size_t length) {
    static const char hex[] = "0123456789ABCDEF";
    for (size_t i = 0; i < length; i++) {
        unsigned char c = value[i];
        int allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9');
        switch (c) {
            case '-': case '_': case '.': case '!': case '~':
            case '*': case '\'': case '(': case ')': allowed = 1; break;
        }
        if (allowed) *out++ = (char)c;
        else {
            *out++ = '%';
            *out++ = hex[c >> 4];
            *out++ = hex[c & 15];
        }
    }
    return out;
}

#endif
