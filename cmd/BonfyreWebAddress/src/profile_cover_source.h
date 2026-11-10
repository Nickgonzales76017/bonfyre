#ifndef BONFYRE_PROFILE_COVER_SOURCE_H
#define BONFYRE_PROFILE_COVER_SOURCE_H

/* A review fingerprint for an already resolved public source item. This does
 * not admit a source, authorize an actor, or publish a fragment. Callers must
 * resolve the exact source through its owner and recheck it at commit. */
#include <stdint.h>
#include <string.h>
#include "bonfyre.h"
#include "bf_msgpack.h"

typedef struct {
    const char *source, *key, *title, *maker, *href, *image, *rights;
    int focal_x, focal_y;
} BfProfileCoverSource;

static int bf_cover_text(const char *s, size_t max, int required) {
    if (!s) return 0;
    size_t n = strlen(s);
    if (n > max || (required && !n)) return 0;
    for (size_t i = 0; i < n; i++)
        if ((unsigned char)s[i] < 32 || (unsigned char)s[i] == 127) return 0;
    return 1;
}

/* URL syntax is deliberately conservative. The source reader must still
 * verify its own image policy and publication/withdrawal state. */
static int bf_cover_https(const char *s) {
    if (!bf_cover_text(s, 1023, 1) || strncmp(s, "https://", 8)) return 0;
    const char *host = s + 8, *end = host + strcspn(host, "/?#");
    if (end == host || *end == '?' || *end == '#') return 0;
    for (const char *p = host; p < end; p++)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') || *p == '.' || *p == '-')) return 0;
    return !strpbrk(s, " \\\"'<>");
}

/* The same reviewed fields under a placement label: a header and a gallery
 * entry for one artwork have different fingerprints, so neither review can
 * stand in for the other. */
static int bf_profile_art_review_digest(const char *label, const BfProfileCoverSource *s, char out[65]) {
    if (!out) return 0;
    out[0] = 0;
    if (!s || !bf_cover_text(s->source, 95, 1) ||
        strspn(s->source, "abcdefghijklmnopqrstuvwxyz0123456789-_.") != strlen(s->source) ||
        !s->key || strlen(s->key) != 24 ||
        strspn(s->key, "0123456789abcdef") != 24 ||
        !bf_cover_text(s->title, 767, 1) || !bf_cover_text(s->maker, 255, 0) ||
        !bf_cover_https(s->href) || !bf_cover_https(s->image) ||
        !s->rights || strcmp(s->rights, "CC0") ||
        s->focal_x < 0 || s->focal_x > 100 || s->focal_y < 0 || s->focal_y > 100) return 0;
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, 512);
    if (!label || (strcmp(label, "bonfyre.profile-cover-review.v1") &&
                   strcmp(label, "bonfyre.profile-gallery-review.v1"))) return 0;
    const char *values[] = {label, s->source, s->key,
                            s->title, s->maker, s->href, s->image, s->rights};
    int ok = !bf_mp_pack_array(&w, 10);
    for (size_t i = 0; i < sizeof(values)/sizeof(values[0]) && ok; i++)
        ok = !bf_mp_pack_str_cstr(&w, values[i]);
    if (ok) ok = !bf_mp_pack_uint(&w, (uint64_t)s->focal_x) &&
                 !bf_mp_pack_uint(&w, (uint64_t)s->focal_y);
    if (ok) bf_sha256_hex(w.buf, w.len, out);
    bf_mp_writer_free(&w);
    return ok;
}

static int bf_profile_cover_source_digest(const BfProfileCoverSource *s, char out[65]) {
    return bf_profile_art_review_digest("bonfyre.profile-cover-review.v1", s, out);
}
#endif
