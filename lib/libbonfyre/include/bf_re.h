/*
 * bf_re.h — JavaScript-compatible regular expressions for C ports.
 *
 * The JS estate is full of regexes using what POSIX lacks: \b \d \s \w,
 * (?:...), lazy quantifiers, lookahead. bf_re is a small backtracking engine
 * with JS semantics over UTF-8 text: flags i (ASCII case folding), g, m, s;
 * classes with ranges and escapes; greedy and lazy * + ? {n,m}; capture and
 * non-capture groups; alternation; ^ $ \b \B; (?=...) (?!...); \1..\9.
 * '.' and negated classes consume one whole code point, as JS consumes one
 * BMP character. Not supported (refused at compile): lookbehind, named groups,
 * unicode property escapes, the y and u flags.
 *
 * tests/bf_re_parity.sh holds it to Bun's RegExp on the patterns the ports use.
 */
#ifndef BF_RE_H
#define BF_RE_H

#include <stddef.h>

#define BF_RE_MAX_GROUPS 16

typedef struct bf_re bf_re;

typedef struct {
    long start[BF_RE_MAX_GROUPS];   /* byte offsets; -1 when the group did not take part */
    long end[BF_RE_MAX_GROUPS];
    int  groups;                    /* number of capture groups + 1 */
} bf_re_match;

bf_re *bf_re_compile(const char *pattern, const char *flags, char *err, size_t err_sz);
void   bf_re_free(bf_re *re);
int    bf_re_global(const bf_re *re);

/* First match at or after byte offset `from`. Returns 1 and fills m, else 0. */
int    bf_re_exec(const bf_re *re, const char *s, size_t len, size_t from, bf_re_match *m);
int    bf_re_test(const bf_re *re, const char *s);

/* String.prototype.replace with a template ($& $1..$9 $$ $` $'); replaces all
 * matches when the regex has the g flag, else the first. Returns malloc'd text. */
char  *bf_re_replace(const bf_re *re, const char *s, const char *tmpl);

/* Replace with a callback that appends its replacement to `out` via bf_re_append. */
typedef struct { char *buf; size_t len, cap; } bf_re_buf;
void   bf_re_append(bf_re_buf *b, const char *s, size_t n);
typedef void (*bf_re_fn)(bf_re_buf *out, const char *s, const bf_re_match *m, void *ctx);
char  *bf_re_replace_fn(const bf_re *re, const char *s, bf_re_fn fn, void *ctx);

/* Convenience: compile, test, free. Returns -1 on a bad pattern. */
int    bf_re_match_once(const char *pattern, const char *flags, const char *s);
/* Copy capture group g of m out of s (malloc'd, "" when unset). */
char  *bf_re_group(const char *s, const bf_re_match *m, int g);

#endif
