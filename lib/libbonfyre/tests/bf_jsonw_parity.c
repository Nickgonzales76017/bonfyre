/* Reads one JSON document per line; prints JSON.stringify(v,null,2) then the
 * canonical (sorted, compact) form. tests/bf_jsonw_parity.sh diffs it with bun. */
#include "bf_jsonw.h"
#include "bf_jv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(void) {
    char *line = NULL; size_t cap = 0; ssize_t n;
    bf_jsonw p, c; bf_jsonw_init(&p, 2); bf_jsonw_init(&c, 0);
    while ((n = getline(&line, &cap, stdin)) > 0) {
        if (line[n - 1] == '\n') line[--n] = 0;
        char err[128]; bf_json_doc_t *d = bf_json_parse(line, (size_t)n, err, sizeof err);
        if (!d) { fprintf(stderr, "parse: %s\n", err); return 1; }
        bf_jsonw_reset(&p); bf_jsonw_reset(&c);
        bf_jsonw_node(&p, d, bf_json_root(d), 0);
        bf_jsonw_node(&c, d, bf_json_root(d), 1);
        char hex[65]; bf_jsonw_digest(d, bf_json_root(d), hex);
        printf("%s\n%s\n%s\n", p.buf, c.buf, hex);
        /* the same through bf_jv: parse, clone, dump, digest */
        bf_jv *v = bf_jv_parse(line, (size_t)n, err, sizeof err), *k = bf_jv_clone(v);
        char *pp = bf_jv_dump(k, 2, 0, NULL), *cc = bf_jv_dump(k, 0, 1, NULL); char h2[65]; bf_jv_digest(k, h2);
        if (strcmp(pp, p.buf) || strcmp(cc, c.buf) || strcmp(h2, hex) || !bf_jv_equal(v, k)) { fprintf(stderr, "bf_jv differs\n"); return 1; }
        free(pp); free(cc); bf_jv_free(v); bf_jv_free(k);
        bf_json_free(d);
    }
    return 0;
}
