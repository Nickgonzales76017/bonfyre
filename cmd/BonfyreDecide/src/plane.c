/*
 * bonfyre-decide plane — the DecisionPlane core over JSON, for callers that
 * used components/BonfyreDecision/decision-plane.mjs directly.
 *
 *   plane digest            stdin JSON value       -> 64-hex decisionDigest
 *   plane policy                                   -> the compiled policy
 *   plane compile           stdin decision case    -> compiled case
 *   plane proposal          stdin receipt          -> proposal (or null)
 *
 * Every verb reads one JSON document from stdin and writes one line of JSON
 * (or hex) to stdout. Output objects are written with sorted keys, matching
 * the JS plane's own `sorted` serialization.
 */
#include "dp.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static bf_jv *read_stdin_json(void) {
    size_t cap = 1 << 16, len = 0; char *buf = malloc(cap);
    if (!buf) dp_fail("out of memory");
    for (;;) {
        if (len == cap) { cap *= 2; if (cap > ((size_t)64 << 20)) dp_fail("input exceeds 64 MiB"); buf = realloc(buf, cap); if (!buf) dp_fail("out of memory"); }
        size_t got = fread(buf + len, 1, cap - len, stdin);
        len += got;
        if (got == 0) break;
    }
    char err[200] = {0};
    bf_jv *v = bf_jv_parse(buf, len, err, sizeof err);
    free(buf);
    if (!v) dp_fail("invalid JSON input: %s", err);
    return v;
}

static void emit(const bf_jv *v) {
    size_t n = 0; char *s = bf_jv_dump(v, 0, 1, &n);
    fwrite(s, 1, n, stdout); fputc('\n', stdout); free(s);
}

int plane_main(int argc, char **argv) {
    if (argc < 1) { fputs("usage: bonfyre-decide plane digest|policy|compile|proposal < input.json\n", stderr); return 2; }
    const char *verb = argv[0];
    bf_jv *policy = dp_policy();
    if (!strcmp(verb, "policy")) { emit(policy); bf_jv_free(policy); return 0; }
    bf_jv *in = read_stdin_json();
    if (!strcmp(verb, "digest")) {
        char hex[65]; dp_digest(in, hex); puts(hex);
    } else if (!strcmp(verb, "compile")) {
        bf_jv *c = dp_compile_case(in, policy); emit(c); bf_jv_free(c);
    } else if (!strcmp(verb, "proposal")) {
        bf_jv *p = dp_proposal(in, policy);
        if (p) { emit(p); bf_jv_free(p); } else puts("null");
    } else {
        fprintf(stderr, "bonfyre-decide plane: unknown verb %s\n", verb); bf_jv_free(in); bf_jv_free(policy); return 2;
    }
    bf_jv_free(in); bf_jv_free(policy);
    return 0;
}
