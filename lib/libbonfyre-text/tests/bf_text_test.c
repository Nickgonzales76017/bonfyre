/* The two primitives, used from C as an owner would use them. */
#define _POSIX_C_SOURCE 200809L
#include "../bf_text.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int failed;
static void same(const char *a, const char *b, const char *what) {
    char ka[256], kb[256];
    if (bf_text_identity_key(a, ka, sizeof(ka)) < 0 || bf_text_identity_key(b, kb, sizeof(kb)) < 0 || strcmp(ka, kb)) { printf("FAIL same: %s (%s | %s)\n", what, ka, kb); failed = 1; }
}
static void differ(const char *a, const char *b, const char *what) {
    char ka[256], kb[256];
    bf_text_identity_key(a, ka, sizeof(ka)); bf_text_identity_key(b, kb, sizeof(kb));
    if (!strcmp(ka, kb)) { printf("FAIL differ: %s (%s)\n", what, ka); failed = 1; }
}
static double now_ms(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6; }

int main(void) {
    char key[64], why[200];
    /* One person, written as different byte sequences by different sources. */
    same("Jos\xC3\xA9 Mart\xC3\xADnez", "Jose\xCC\x81 Marti\xCC\x81nez", "composed and decomposed accents");
    same("JOS\xC3\x89 MART\xC3\x8DNEZ", "jos\xC3\xA9 mart\xC3\xADnez", "case");
    same("Stra\xC3\x9F" "e", "STRASSE", "sharp s folds to ss");
    same("\xEF\xBC\xA1\xEF\xBD\x95rekai", "Aurekai", "fullwidth letters");
    same("\xEF\xAC\x81nance", "finance", "the fi ligature");
    same("  Neon   June \t", "Neon\xC2\xA0June", "spacing, including a no-break space");
    same("Au\xE2\x80\x8Brekai", "Aurekai", "a zero-width space inside a word");
    /* And what must stay apart. */
    differ("Jos\xC3\xA9", "Jose", "an accent is part of the name");
    differ("resume", "r\xC3\xA9sum\xC3\xA9", "accented and unaccented words");
    differ("Neon June", "NeonJune", "a space between words is kept");
    if (bf_text_identity_key("bad \xC3\x28 bytes", key, sizeof(key)) != -1) { printf("FAIL: invalid UTF-8 was given a key\n"); failed = 1; }
    if (bf_text_identity_key("a name longer than the room for it", key, 8) != -1) { printf("FAIL: a key that does not fit was cut\n"); failed = 1; }

    /* Matching that finishes. */
    bf_match_t *feed = bf_match_compile("^https://[a-z0-9.-]+/(feed|rss)(\\.xml)?/?$", why, sizeof(why));
    if (!feed || !bf_match_whole(feed, "https://example.org/feed.xml", 28) || bf_match_whole(feed, "https://example.org/feed.xml/evil", 33) || !bf_match_search(feed, "https://a.b/rss", 15)) { printf("FAIL: an ordinary pattern\n"); failed = 1; }
    bf_match_free(feed);
    if (bf_match_compile("(a)\\1", why, sizeof(why)) || !why[0]) { printf("FAIL: a backreference was accepted\n"); failed = 1; }
    if (bf_match_compile("(?=a)b", why, sizeof(why))) { printf("FAIL: a lookahead was accepted\n"); failed = 1; }
    if (bf_match_compile("a(b", why, sizeof(why))) { printf("FAIL: a broken pattern was accepted\n"); failed = 1; }
    /* The pattern that takes a backtracking engine time doubling with each character. */
    bf_match_t *trap = bf_match_compile("^(a+)+$", why, sizeof(why));
    size_t n = 200000;
    char *text = malloc(n + 2);
    memset(text, 'a', n); text[n] = 'b'; text[n + 1] = '\0';
    double started = now_ms();
    int hit = bf_match_whole(trap, text, n + 1);
    double took = now_ms() - started;
    if (hit || took > 2000.0) { printf("FAIL: the trap pattern took %.0f ms (hit %d)\n", took, hit); failed = 1; }
    printf("trap pattern ^(a+)+$ over %zu characters: %.1f ms, no match\n", n + 1, took);
    bf_match_free(trap); free(text);
    printf(failed ? "FAIL bf_text\n" : "PASS bf_text: identity keys and linear-time matching\n");
    return failed;
}
