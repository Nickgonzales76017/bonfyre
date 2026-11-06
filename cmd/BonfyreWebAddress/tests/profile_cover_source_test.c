#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "../src/profile_cover_source.h"

int main(void) {
    BfProfileCoverSource s = {
        "cleveland-artworks", "a47812567fb8271d725264a7", "Stag at Sharkey's",
        "George Bellows", "https://clevelandart.org/art/1922.1133",
        "https://openaccess-cdn.clevelandart.org/1922.1133/1922.1133_web.jpg",
        "CC0", 50, 50
    };
    char first[65], next[65];
    assert(bf_profile_cover_source_digest(&s, first));
    assert(strlen(first) == 64);
    assert(bf_profile_cover_source_digest(&s, next) && !strcmp(first, next));
    s.image = "https://example.org/replacement.jpg";
    assert(bf_profile_cover_source_digest(&s, next) && strcmp(first, next));
    s.image = "https://openaccess-cdn.clevelandart.org/1922.1133/1922.1133_web.jpg";
    s.focal_x = 51;
    assert(bf_profile_cover_source_digest(&s, next) && strcmp(first, next));
    s.focal_x = 50; s.focal_y = 101;
    assert(!bf_profile_cover_source_digest(&s, next) && !next[0]);
    s.focal_y = 50; s.rights = "";
    assert(!bf_profile_cover_source_digest(&s, next));
    s.rights = "copyright";
    assert(!bf_profile_cover_source_digest(&s, next));
    s.rights = "CC0"; s.image = "https://example.org@attacker.test/x";
    assert(!bf_profile_cover_source_digest(&s, next));
    s.image = "https://example.org/\" onerror=\"x";
    assert(!bf_profile_cover_source_digest(&s, next));
    s.image = "http://example.org/x";
    assert(!bf_profile_cover_source_digest(&s, next));
    s.image = "https://example.org/x"; s.key = "other";
    assert(!bf_profile_cover_source_digest(&s, next));
    s.key = "a47812567fb8271d725264a7"; s.source = "../other";
    assert(!bf_profile_cover_source_digest(&s, next));
    puts("profile_cover_source_test: PASS");
    return 0;
}
