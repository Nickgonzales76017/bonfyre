/* Two primitives every owner that handles names, addresses or rules needs,
 * and should not each rediscover: one canonical form for text that is to be
 * compared as an identity, and one matcher for patterns that is guaranteed to
 * finish. Unicode normalisation is utf8proc's; matching is RE2's. */
#ifndef BF_TEXT_H
#define BF_TEXT_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif

/* The identity key of a piece of text: composed, case-folded, compatibility
 * forms unified, default-ignorable code points removed, and runs of white
 * space collapsed to one space with none at the ends. Two spellings a person
 * would call the same name give the same key. Writes at most cap-1 bytes and
 * a terminator; returns the key's length, or -1 if the input is not valid
 * UTF-8 or the key does not fit. */
long bf_text_identity_key(const char *text, char *out, size_t cap);

typedef struct bf_match bf_match_t;

/* A pattern compiled for matching in time linear in the text. A pattern that
 * needs backtracking to mean anything (a backreference, a lookaround) is
 * refused here, with the reason in `why`, rather than accepted and left to
 * run for ever on the wrong input. Returns NULL on refusal. */
bf_match_t *bf_match_compile(const char *pattern, char *why, size_t why_cap);
/* 1 if the pattern is found anywhere in the text, 0 if not. */
int bf_match_search(const bf_match_t *match, const char *text, size_t len);
/* 1 if the pattern matches the whole text, 0 if not. */
int bf_match_whole(const bf_match_t *match, const char *text, size_t len);
void bf_match_free(bf_match_t *match);

#ifdef __cplusplus
}
#endif
#endif
