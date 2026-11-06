/* One JSON case per line: [pattern, flags, input, replacement]. Prints the
 * first match's group byte offsets and the replace() result; bf_re_parity.sh
 * diffs this with Bun's RegExp. */
#include "bf_jv.h"
#include "bf_re.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(void) {
    char *line = NULL; size_t cap = 0; ssize_t n;
    while ((n = getline(&line, &cap, stdin)) > 0) {
        char err[128];
        bf_jv *c = bf_jv_parse(line, (size_t)n, err, sizeof err);
        if (!c) { printf("bad case\n"); continue; }
        const char *pat = bf_jv_s(bf_jv_at(c, 0)), *fl = bf_jv_s(bf_jv_at(c, 1)), *in = bf_jv_s(bf_jv_at(c, 2)), *rep = bf_jv_s(bf_jv_at(c, 3));
        bf_re *re = bf_re_compile(pat, fl, err, sizeof err);
        if (!re) { printf("compile error\n\n"); bf_jv_free(c); continue; }
        bf_re_match m;
        if (bf_re_exec(re, in, strlen(in), 0, &m)) {
            printf("match");
            for (int g = 0; g < m.groups; g++) printf(" %ld:%ld", m.start[g], m.end[g]);
            printf("\n");
        } else printf("no match\n");
        char *r = bf_re_replace(re, in, rep ? rep : "<$&>");
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_str(&w, r); printf("%s\n", w.buf); bf_jsonw_free(&w);
        free(r); bf_re_free(re); bf_jv_free(c);
    }
    return 0;
}
