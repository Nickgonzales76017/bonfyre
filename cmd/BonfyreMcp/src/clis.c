/* The owner command clients: spines, v6 (owned MCP call), canary, recursive.
 * Each mirrors its retired .mjs grammar and wire checks exactly; the golden
 * vectors in tests/vectors were recorded from the JS before it was retired. */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "bf_jsonw.h"
#include "mcp.h"

#define MAX_OWNED 20000000UL

static const char *home_dir(void) { const char *h = getenv("HOME"); return h ? h : ""; }
static const char *env_or(const char *name, const char *fallback) {   /* `??` */
    const char *v = getenv(name); return v ? v : fallback;
}
static const char *env_truthy(const char *name) {   /* `||` operand */
    const char *v = getenv(name); return v && *v ? v : NULL;
}
static char *request_body(const char *id, const char *name, const bf_jv *args, size_t *len) {
    bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "jsonrpc"); bf_jsonw_str(&w, "2.0");
    bf_jsonw_key(&w, "id"); bf_jsonw_str(&w, id);
    bf_jsonw_key(&w, "method"); bf_jsonw_str(&w, "tools/call");
    bf_jsonw_key(&w, "params"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "name"); bf_jsonw_str(&w, name);
    bf_jsonw_key(&w, "arguments"); bf_jv_write(&w, args, BF_JV_SORT_NONE);
    bf_jsonw_obj_end(&w); bf_jsonw_obj_end(&w);
    *len = w.len;
    char *s = w.buf; w.buf = NULL; bf_jsonw_free(&w);
    return s;
}
static void uuid4(char out[37]) {
    char h[33]; random_hex(h, 16);
    h[12] = '4'; h[16] = "89ab"[strtol((char[]){h[16], 0}, NULL, 16) & 3];
    snprintf(out, 37, "%.8s-%.4s-%.4s-%.4s-%.12s", h, h + 8, h + 12, h + 16, h + 20);
}

/* ── owned MCP call (owned_mcp_client.mjs) ──────────────────── */
typedef struct { js_url url; char *token; } Owned;

static int owned_endpoint(const char *env, const char *fallback, js_url *u, char *err) {
    const char *base = env_truthy(env); if (!base) base = fallback;
    size_t n = strlen(base); char *s = malloc(n + 5);
    memcpy(s, base, n); if (n && s[n - 1] == '/') n--;
    memcpy(s + n, "/mcp", 5);
    int bad = url_parse(s, u);
    if (bad) { snprintf(err, ERR_MAX, "\"%s\" cannot be parsed as a URL.", s); free(s); return -1; }
    free(s);
    if ((strcmp(u->scheme, "http") && strcmp(u->scheme, "https")) || u->has_user || u->has_hash || u->has_search ||
        (!strcmp(u->scheme, "http") && !url_loopback(u))) {
        snprintf(err, ERR_MAX, "MCP endpoint requires loopback or HTTPS without URL credentials"); return -1;
    }
    return 0;
}
static int owned_token(const char *path, Owned *o, char *err) {
    char *text; size_t n;
    if (read_text_file(path, &text, &n, err)) return -1;
    o->token = jsx_trim(text, n); free(text);
    if (!*o->token) { snprintf(err, ERR_MAX, "invalid MCP owner binding"); return -1; }
    return 0;
}
/* Returns the owner result (caller frees) or NULL with err classified. */
static bf_jv *owned_call(Owned *o, const char *name, const bf_jv *args, char *err) {
    char id[37]; uuid4(id);
    size_t blen; char *body = request_body(id, name, args, &blen);
    bf_jv *out = NULL, *value = NULL; http_reply r; memset(&r, 0, sizeof r);
    if (blen > MAX_OWNED) { snprintf(err, ERR_MAX, "MCP request exceeds budget"); goto done; }
    if (http_post(&o->url, o->token, body, blen, 300000, MAX_OWNED, &r, err)) goto done;
    if (r.status < 200 || r.status > 299) { snprintf(err, ERR_MAX, "MCP owner HTTP %d", r.status); goto done; }
    if (r.content_length == -2 || (r.content_length >= 0 && (size_t)r.content_length > MAX_OWNED) || !r.has_body) {
        snprintf(err, ERR_MAX, "invalid MCP owner body size"); goto done;
    }
    if (r.over_budget || r.body_len > MAX_OWNED) { snprintf(err, ERR_MAX, "MCP owner body exceeds budget"); goto done; }
    if (!r.body_len) { snprintf(err, ERR_MAX, "invalid MCP owner body size"); goto done; }
    if (!(value = jsx_parse(r.body, r.body_len, err))) goto done;
    const bf_jv *result = jsx_get(value, "result");
    if (value->type != BF_JV_OBJ || !jsx_str_is(jsx_get(value, "jsonrpc"), "2.0") || !jsx_str_is(jsx_get(value, "id"), id) ||
        jsx_truthy(jsx_get(value, "error")) || !result || result->type != BF_JV_OBJ) {
        snprintf(err, ERR_MAX, "MCP owner returned invalid response identity"); goto done;
    }
    const bf_jv *is_error = bf_jv_get(result, "isError");
    if (is_error && is_error->type == BF_JV_BOOL && is_error->b) {
        const bf_jv *reason = jsx_get(jsx_get(result, "structuredContent"), "error");
        snprintf(err, ERR_MAX, "MCP owner reported error; effect outcome requires reconciliation%s%s",
                 reason && reason->type == BF_JV_STR ? "; owner reported: " : "", reason && reason->type == BF_JV_STR ? reason->s : "");
        goto done;
    }
    if (bf_jv_has(result, "structuredContent")) { out = bf_jv_clone(bf_jv_get(result, "structuredContent")); goto done; }
    const bf_jv *content = bf_jv_get(result, "content");
    if (content && content->type == BF_JV_ARR) for (size_t i = 0; i < content->len; i++) {
        const bf_jv *part = content->items[i];
        if (!jsx_str_is(jsx_get(part, "type"), "text")) continue;
        const bf_jv *text = jsx_get(part, "text"); char e2[ERR_MAX];
        if (text && text->type == BF_JV_STR && (out = jsx_parse(text->s, text->len, e2))) goto done;
        out = bf_jv_obj(); jsx_set_opt(out, "text", text); goto done;
    }
    out = bf_jv_clone(result);
done:
    if (!out) jsx_redact(err, o->token, "[redacted]", 2000);
    free(body); http_reply_free(&r); bf_jv_free(value);
    return out;
}

/* ── node:util parseArgs (strict, allowPositionals) ─────────── */
typedef struct { const char *name; int boolean; const char *dflt; const char *value; int set; } Opt;
static int parse_args(int argc, char **argv, Opt *opts, int nopts, char **pos, int *npos, char *err) {
    *npos = 0;
    for (int i = 0; i < nopts; i++) { opts[i].value = opts[i].dflt; opts[i].set = 0; }
    for (int i = 0; i < argc; i++) {
        const char *a = argv[i];
        if (!strcmp(a, "--")) { while (++i < argc) pos[(*npos)++] = argv[i]; break; }
        if (a[0] == '-' && a[1] == '-') {
            const char *eq = strchr(a, '='); size_t nl = eq ? (size_t)(eq - a - 2) : strlen(a + 2);
            Opt *o = NULL;
            for (int k = 0; k < nopts; k++) if (strlen(opts[k].name) == nl && !strncmp(opts[k].name, a + 2, nl)) o = &opts[k];
            if (!o) {
                snprintf(err, ERR_MAX, "Unknown option '--%.*s'. To specify a positional argument starting with a '-', place it at the end of the command after '--', as in '-- \"--%.*s\"", (int)nl, a + 2, (int)nl, a + 2);
                return -1;
            }
            if (o->boolean) {
                if (eq) { snprintf(err, ERR_MAX, "Option '--%s' does not take an argument", o->name); return -1; }
                o->value = "true"; o->set = 1; continue;
            }
            if (eq) { o->value = eq + 1; o->set = 1; continue; }
            if (i + 1 >= argc) { snprintf(err, ERR_MAX, "Option '--%s <value>' argument missing", o->name); return -1; }
            const char *v = argv[i + 1];
            if (v[0] == '-' && v[1]) {
                snprintf(err, ERR_MAX, "Option '--%s' argument is ambiguous.\nDid you forget to specify the option argument for '--%s'?\nTo specify an option argument starting with a dash use '--%s=-XYZ'.", o->name, o->name, o->name);
                return -1;
            }
            o->value = v; o->set = 1; i++; continue;
        }
        if (a[0] == '-' && a[1]) {
            snprintf(err, ERR_MAX, "Unknown option '-%c'. To specify a positional argument starting with a '-', place it at the end of the command after '--', as in '-- \"-%c\"", a[1], a[1]);
            return -1;
        }
        pos[(*npos)++] = (char *)a;
    }
    return 0;
}
static const char *opt(Opt *opts, int n, const char *name) { for (int i = 0; i < n; i++) if (!strcmp(opts[i].name, name)) return opts[i].value; return NULL; }
static int opt_on(Opt *opts, int n, const char *name) { const char *v = opt(opts, n, name); return v && !strcmp(v, "true"); }

/* integer() of spines_cli: /^-?\d+$/ and safe. */
static int spines_integer(const char *t, double *out, char *err) {
    const char *p = t + (*t == '-'); int ok = *p != 0;
    for (; *p; p++) if (*p < '0' || *p > '9') ok = 0;
    double d = ok ? jsx_to_number(t) : 0;
    if (!ok || !jsx_safe_integer(d)) { snprintf(err, ERR_MAX, "invalid integer option"); return -1; }
    *out = d; return 0;
}

/* ── spines ─────────────────────────────────────────────────── */
static bf_jv *sp_call(Owned *o, const char *name, bf_jv *args, char *err) { bf_jv *r = owned_call(o, name, args, err); bf_jv_free(args); return r; }
static bf_jv *obj_kv(const char *k, bf_jv *v) { bf_jv *o = bf_jv_obj(); bf_jv_set(o, k, v); return o; }
#define S(x) bf_jv_str(x)
#define N(x) bf_jv_num(x)
#define CALL(var, name, args) do { var = sp_call(o, name, args, err); if (!var) goto fail; } while (0)

static int spines_run(Owned *o, int argc, char **argv, bf_jv **result, char *err) {
    static const char *names[] = { "status", "verify", "flux", "closure", "saturate", "authority", "phenotype", "place",
                                   "r5-smoke", "r6-smoke", "r7-smoke", "full-smoke" };
    const char *command = argc ? argv[0] : NULL; int known = 0;
    for (size_t i = 0; command && i < sizeof names / sizeof *names; i++) if (!strcmp(names[i], command)) known = 1;
    if (!known) { snprintf(err, ERR_MAX, "usage: bonfyre-spines status|verify|flux|closure|saturate|authority|phenotype|place|r5-smoke|r6-smoke|r7-smoke|full-smoke"); return -1; }
    Opt opts[8]; int nopts = 0, arity = 0;
#define O(n, b, d) opts[nopts++] = (Opt){ n, b, d, NULL, 0 }
    if (!strcmp(command, "flux")) arity = 5;
    else if (!strcmp(command, "closure")) { arity = 2; O("depth", 0, "16"); }
    else if (!strcmp(command, "saturate")) { arity = 1; O("iterations", 0, "12"); O("terms", 0, "2048"); }
    else if (!strcmp(command, "authority")) { arity = 1; O("actor", 0, "operator"); O("resource", 0, ""); }
    else if (!strcmp(command, "phenotype")) { arity = 1; O("mission-id", 0, ""); O("tokens", 0, "0"); O("privacy", 0, "internal"); }
    else if (!strcmp(command, "place")) { O("mission-id", 0, ""); O("phenotype-id", 0, ""); O("privacy", 0, "internal"); O("effect", 0, "CANDIDATE_WRITE");
        O("bytes", 0, "0"); O("gpu", 1, "false"); O("local-only", 1, "false"); }
    char **pos = calloc((size_t)argc + 1, sizeof *pos); int npos;
    bf_jv *a = NULL, *b = NULL, *c = NULL, *d = NULL, *e = NULL, *f = NULL, *g = NULL, *r5 = NULL, *r6 = NULL, *r7 = NULL;
    if (parse_args(argc - 1, argv + 1, opts, nopts, pos, &npos, err)) goto fail;
    if (npos != arity) { snprintf(err, ERR_MAX, "expected %d positional arguments for %s", arity, command); goto fail; }
    double n1, n2;
#define V(name) opt(opts, nopts, name)
    if (!strcmp(command, "status")) { CALL(*result, "spines_status", bf_jv_obj()); goto ok; }
    if (!strcmp(command, "verify")) { CALL(*result, "spines_verify", bf_jv_obj()); goto ok; }
    if (!strcmp(command, "flux")) {
        bf_jv *payload = jsx_parse(pos[4], strlen(pos[4]), err); if (!payload) goto fail;
        bf_jv *x = bf_jv_obj(); bf_jv_set(x, "stream", S(pos[0])); bf_jv_set(x, "kind", S("operator.append"));
        bf_jv_set(x, "subject_kind", S(pos[1])); bf_jv_set(x, "subject_id", S(pos[2])); bf_jv_set(x, "operation", S(pos[3]));
        bf_jv_set(x, "payload", payload); CALL(*result, "r5_flux_append", x); goto ok;
    }
    if (!strcmp(command, "closure")) {
        if (spines_integer(V("depth"), &n1, err)) goto fail;
        bf_jv *x = bf_jv_obj(); bf_jv_set(x, "root_kind", S(pos[0])); bf_jv_set(x, "root_id", S(pos[1])); bf_jv_set(x, "max_depth", N(n1));
        CALL(*result, "r5_affected_closure", x); goto ok;
    }
    if (!strcmp(command, "saturate")) {
        if (spines_integer(V("iterations"), &n1, err) || spines_integer(V("terms"), &n2, err)) goto fail;
        bf_jv *x = bf_jv_obj(); bf_jv_set(x, "term", S(pos[0])); bf_jv_set(x, "max_iterations", N(n1)); bf_jv_set(x, "max_terms", N(n2));
        CALL(*result, "r6_saturate", x); goto ok;
    }
    if (!strcmp(command, "authority")) {
        bf_jv *x = bf_jv_obj(); bf_jv_set(x, "actor", S(V("actor"))); bf_jv_set(x, "effect_class", S(pos[0]));
        if (*V("resource")) bf_jv_set(x, "resource", S(V("resource")));
        CALL(*result, "r6_authority_preflight", x); goto ok;
    }
    if (!strcmp(command, "phenotype")) {
        if (spines_integer(V("tokens"), &n1, err)) goto fail;
        bf_jv *x = bf_jv_obj(); if (*V("mission-id")) bf_jv_set(x, "mission_id", S(V("mission-id")));
        bf_jv_set(x, "model_family", S(pos[0])); bf_jv_set(x, "token_count", N(n1)); bf_jv_set(x, "privacy_class", S(V("privacy")));
        CALL(*result, "r7_phenotype_compile", x); goto ok;
    }
    if (!strcmp(command, "place")) {
        if (spines_integer(V("bytes"), &n1, err)) goto fail;
        bf_jv *x = bf_jv_obj(); if (*V("mission-id")) bf_jv_set(x, "mission_id", S(V("mission-id")));
        if (*V("phenotype-id")) bf_jv_set(x, "phenotype_id", S(V("phenotype-id")));
        bf_jv_set(x, "privacy_class", S(V("privacy"))); bf_jv_set(x, "effect_class", S(V("effect"))); bf_jv_set(x, "data_bytes", N(n1));
        bf_jv_set(x, "requires_gpu", bf_jv_bool(opt_on(opts, nopts, "gpu"))); bf_jv_set(x, "remote_allowed", bf_jv_bool(!opt_on(opts, nopts, "local-only")));
        CALL(*result, "r7_placement_compile", x); goto ok;
    }
    int full = !strcmp(command, "full-smoke");
    if (full || !strcmp(command, "r5-smoke")) {
        bf_jv *x = bf_jv_obj(); bf_jv_set(x, "stream", S("smoke")); bf_jv_set(x, "kind", S("smoke.object")); bf_jv_set(x, "subject_kind", S("smoke"));
        bf_jv_set(x, "subject_id", S("r5-r7")); bf_jv_set(x, "operation", S("UPSERT"));
        bf_jv *pl = bf_jv_obj(); bf_jv_set(pl, "state", S("ready")); bf_jv_set(pl, "ready", bf_jv_bool(1)); bf_jv_set(pl, "counter", N(1)); bf_jv_set(x, "payload", pl);
        bf_jv *dep = bf_jv_obj(); bf_jv_set(dep, "target_kind", S("smoke_dependency")); bf_jv_set(dep, "target_id", S("downstream")); bf_jv_set(dep, "edge_kind", S("invalidates"));
        bf_jv *deps = bf_jv_arr(); bf_jv_push(deps, dep); bf_jv_set(x, "dependencies", deps);
        CALL(a, "r5_flux_append", x);
        x = bf_jv_obj(); bf_jv_set(x, "stream", S("smoke")); bf_jv_set(x, "kind", S("smoke.object")); bf_jv_set(x, "subject_kind", S("smoke"));
        bf_jv_set(x, "subject_id", S("r5-r7")); bf_jv_set(x, "operation", S("UPSERT")); bf_jv_set(x, "payload", obj_kv("counter", N(2)));
        CALL(b, "r5_flux_append", x);
        CALL(c, "r5_parity_verify", bf_jv_obj());
        int pass = jsx_truthy(jsx_get(c, "equal")) && jsx_str_is(jsx_get(jsx_get(a, "receipt"), "state"), "passed") && jsx_str_is(jsx_get(jsx_get(b, "receipt"), "state"), "passed");
        r5 = bf_jv_obj(); bf_jv_set(r5, "state", S(pass ? "passed" : "failed"));
        bf_jv_set(r5, "first", a); bf_jv_set(r5, "second", b); bf_jv_set(r5, "parity", c); a = b = c = NULL;
        if (!full) { *result = r5; r5 = NULL; goto ok; }
    }
    if (full || !strcmp(command, "r6-smoke")) {
        CALL(a, "r6_saturate", obj_kv("term", S("add(0,mul(add(2,3),1))")));
        bf_jv *x = bf_jv_obj(); jsx_set_opt(x, "term", jsx_get(a, "selected_term")); jsx_set_opt(x, "source_run_id", jsx_get(a, "run_id")); bf_jv_set(x, "fuel", N(10000));
        CALL(b, "r6_helix_compile", x);
        x = bf_jv_obj(); jsx_set_opt(x, "helix_id", jsx_get(b, "helix_id")); CALL(c, "r6_helix_reduce", x);
        x = bf_jv_obj(); bf_jv_set(x, "actor", S("smoke")); bf_jv_set(x, "effect_class", S("CANDIDATE_WRITE")); bf_jv_set(x, "resource", S("smoke:r6"));
        bf_jv *refs = bf_jv_arr(); bf_jv_push(refs, jsx_get(c, "reduction_id") ? bf_jv_clone(jsx_get(c, "reduction_id")) : bf_jv_null()); bf_jv_set(x, "proof_refs", refs);
        CALL(d, "r6_authority_preflight", x);
        x = bf_jv_obj(); bf_jv_set(x, "claim", S("R6 bounded reduction smoke."));
        bf_jv *inputs = bf_jv_arr(), *in = bf_jv_obj(); jsx_set_opt(in, "term", jsx_get(a, "input_term")); bf_jv_push(inputs, in); bf_jv_set(x, "inputs", inputs);
        bf_jv *wits = bf_jv_arr(), *wi = bf_jv_obj(); jsx_set_opt(wi, "reduction_id", jsx_get(c, "reduction_id")); jsx_set_opt(wi, "state", jsx_get(c, "state"));
        bf_jv_push(wits, wi); bf_jv_set(x, "witnesses", wits); jsx_set_opt(x, "authority_decision_id", jsx_get(d, "decision_id"));
        CALL(e, "r6_crux_seal", x);
        r6 = bf_jv_obj(); jsx_set_opt(r6, "state", jsx_get(c, "state"));
        bf_jv_set(r6, "saturation", a); bf_jv_set(r6, "helix", b); bf_jv_set(r6, "reduction", c); bf_jv_set(r6, "authority", d); bf_jv_set(r6, "crux", e);
        a = b = c = d = e = NULL;
        if (!full) { *result = r6; r6 = NULL; goto ok; }
    }
    if (full || !strcmp(command, "r7-smoke")) {
        CALL(a, "r7_hardware_census", obj_kv("node_id", S("local")));
        CALL(b, "r7_model_discover", bf_jv_obj());
        bf_jv *x = bf_jv_obj(); bf_jv_set(x, "mission_id", S("smoke-r7")); bf_jv_set(x, "model_family", S("BonfyreQwenFPQ")); bf_jv_set(x, "token_count", N(4096)); bf_jv_set(x, "privacy_class", S("internal"));
        CALL(c, "r7_phenotype_compile", x);
        CALL(d, "r7_kernel_parity", bf_jv_obj());
        x = bf_jv_obj(); bf_jv_set(x, "mission_id", S("smoke-r7")); jsx_set_opt(x, "phenotype_id", jsx_get(c, "phenotype_id")); bf_jv_set(x, "privacy_class", S("internal"));
        bf_jv_set(x, "effect_class", S("CANDIDATE_WRITE")); bf_jv_set(x, "data_bytes", N(4096)); bf_jv_set(x, "requires_gpu", bf_jv_bool(0)); bf_jv_set(x, "remote_allowed", bf_jv_bool(1));
        CALL(e, "r7_placement_compile", x);
        x = bf_jv_obj(); bf_jv_set(x, "mission_id", S("smoke-r7")); jsx_set_opt(x, "phenotype_id", jsx_get(c, "phenotype_id")); jsx_set_opt(x, "placement_id", jsx_get(e, "placement_id"));
        CALL(f, "r7_fuse_compile", x);
        r7 = bf_jv_obj();
        const bf_jv *ps = jsx_get(d, "state");
        if (!jsx_str_is(ps, "passed")) jsx_set_opt(r7, "state", ps);
        else bf_jv_set(r7, "state", S(jsx_str_is(jsx_get(a, "state"), "passed") && jsx_str_is(jsx_get(jsx_get(b, "receipt"), "state"), "passed") ? "passed" : "degraded"));
        bf_jv_set(r7, "census", a); bf_jv_set(r7, "inventory", b); bf_jv_set(r7, "phenotype", c); bf_jv_set(r7, "parity", d); bf_jv_set(r7, "placement", e); bf_jv_set(r7, "fuse", f);
        a = b = c = d = e = f = NULL;
        if (!full) { *result = r7; r7 = NULL; goto ok; }
    }
    CALL(g, "spines_verify", bf_jv_obj());
    {
        int pass = jsx_truthy(jsx_get(g, "verified")) && jsx_str_is(jsx_get(r5, "state"), "passed") && jsx_str_is(jsx_get(r6, "state"), "passed") && jsx_str_is(jsx_get(r7, "state"), "passed");
        bf_jv *out = bf_jv_obj(); bf_jv_set(out, "state", S(pass ? "passed" : "failed"));
        bf_jv *x5 = bf_jv_obj(); jsx_set_opt(x5, "parity", jsx_get(r5, "parity")); bf_jv_set(out, "r5", x5);
        bf_jv *x6 = bf_jv_obj(); const char *k6[] = { "saturation", "reduction", "authority", "crux" };
        for (int i = 0; i < 4; i++) jsx_set_opt(x6, k6[i], jsx_get(r6, k6[i]));
        bf_jv_set(out, "r6", x6);
        bf_jv *x7 = bf_jv_obj(); const char *k7[] = { "census", "inventory", "phenotype", "parity", "placement", "fuse" };
        for (int i = 0; i < 6; i++) jsx_set_opt(x7, k7[i], jsx_get(r7, k7[i]));
        bf_jv_set(out, "r7", x7); bf_jv_set(out, "verification", g); g = NULL;
        *result = out;
    }
ok:
    free(pos); bf_jv_free(r5); bf_jv_free(r6); bf_jv_free(r7);
    return 0;
fail:
    free(pos); bf_jv_free(a); bf_jv_free(b); bf_jv_free(c); bf_jv_free(d); bf_jv_free(e); bf_jv_free(f); bf_jv_free(g);
    bf_jv_free(r5); bf_jv_free(r6); bf_jv_free(r7);
    return -1;
}

int cli_spines(int argc, char **argv) {
    char err[ERR_MAX];
    if (argc == 1 && !strcmp(argv[0], "--version")) { puts("bonfyre-spines 5.7.0-native-spines-r7"); return 0; }
    Owned o = { 0 }; bf_jv *result = NULL; char path[4096];
    if (owned_endpoint("BONFYRE_SPINES_URL", "http://127.0.0.1:3057", &o.url, err)) goto fail;
    const char *tf = env_truthy("BONFYRE_MCP_TOKEN_FILE");
    if (!tf) { snprintf(path, sizeof path, "%s/.bonfyre/mcp/token", home_dir()); tf = path; }
    if (owned_token(tf, &o, err)) goto fail;
    if (spines_run(&o, argc, argv, &result, err)) goto fail;
    jsx_print(stdout, result); bf_jv_free(result); free(o.token);
    return 0;
fail:
    fprintf(stderr, "%s\n", err); free(o.token);
    return 2;
}

/* ── v6 ─────────────────────────────────────────────────────── */
int cli_v6(int argc, char **argv) {
    char err[ERR_MAX], path[4096];
    if (argc == 1 && (!strcmp(argv[0], "--version") || !strcmp(argv[0], "version"))) { puts("bonfyre-v6 6.1.0"); return 0; }
    static const char *cmds[] = { "status", "verify", "estate", "workgraph", "resources", "backends", "missions", "history", "sparse", "proof", "outcomes" };
    Owned o = { 0 }; bf_jv *result = NULL;
    if (owned_endpoint("BONFYRE_V6_URL", "http://127.0.0.1:3058", &o.url, err)) goto fail;
    if (!strcmp(o.url.scheme, "https") && !env_truthy("BONFYRE_V6_TOKEN_FILE")) { snprintf(err, ERR_MAX, "remote V6 requires a dedicated credential file"); goto fail; }
    const char *tf = env_truthy("BONFYRE_V6_TOKEN_FILE"); if (!tf) tf = env_truthy("BONFYRE_MCP_TOKEN_FILE");
    if (!tf) { snprintf(path, sizeof path, "%s/.bonfyre/mcp/token", home_dir()); tf = path; }
    if (owned_token(tf, &o, err)) goto fail;
    int known = 0;
    for (size_t i = 0; argc == 1 && i < sizeof cmds / sizeof *cmds; i++) if (!strcmp(cmds[i], argv[0])) known = 1;
    if (!known) { snprintf(err, ERR_MAX, "usage: bonfyre-v6 version|status|verify|estate|workgraph|resources|backends|missions|history|sparse|proof|outcomes"); goto fail; }
    char tool[64]; snprintf(tool, sizeof tool, "v6_%s", argv[0]);
    bf_jv *args = bf_jv_obj(); result = owned_call(&o, tool, args, err); bf_jv_free(args);
    if (!result) goto fail;
    jsx_print(stdout, result);
    const bf_jv *ver = jsx_get(result, "verified");
    int code = !strcmp(argv[0], "verify") && !(ver && ver->type == BF_JV_BOOL && ver->b) ? 1 : 0;
    bf_jv_free(result); free(o.token);
    return code;
fail:
    fprintf(stderr, "%s\n", err); free(o.token);
    return 2;
}

/* ── canary and recursive: their own wire (readRequest) ─────── */
typedef struct { const char *label; const char *url_env; const char *url_default; size_t max_body; size_t slice; long max_timeout; int token_optional_exists; } Wire;
static const Wire CANARY = { "canary", "BONFYRE_CANARY_URL", "http://127.0.0.1:3056", 10000000, 2000, 7200000, 1 };
static const Wire RECURSIVE = { "recursive", "BONFYRE_RECURSIVE_URL", "http://127.0.0.1:3055", 10000000, 500, 3600000, 0 };

static bf_jv *invoke(const Wire *wd, const bf_jv *tools, const char *tool, const bf_jv *args, long timeout_ms, char *err) {
    const bf_jv *def = catalog_tool(tools, tool);
    if (!def) { snprintf(err, ERR_MAX, "unknown %s tool", wd->label); return NULL; }
    if (schema_validate(args, jsx_get(def, "inputSchema"), "arguments", err)) return NULL;
    if (timeout_ms < 1 || timeout_ms > wd->max_timeout) { snprintf(err, ERR_MAX, "invalid %s timeout", wd->label); return NULL; }
    js_url u; const char *base = env_or(wd->url_env, wd->url_default);
    if (url_parse(base, &u)) { snprintf(err, ERR_MAX, "invalid %s URL", wd->label); return NULL; }
    if ((strcmp(u.scheme, "http") && strcmp(u.scheme, "https")) || u.has_user || u.has_search || u.has_hash || strcmp(u.path, "/") ||
        (!strcmp(u.scheme, "http") && !url_loopback(&u))) {
        snprintf(err, ERR_MAX, "%s URL requires loopback HTTP or credential-free HTTPS", wd->label); return NULL;
    }
    strcpy(u.path, "/mcp");
    char path[4096]; const char *tf = getenv("BONFYRE_MCP_TOKEN_FILE");
    if (!tf) { snprintf(path, sizeof path, "%s/.bonfyre/mcp/token", home_dir()); tf = path; }
    char *raw = NULL, *secret = NULL; size_t rn = 0; char e2[ERR_MAX];
    if (wd->token_optional_exists) {
        struct stat st; if (*tf && stat(tf, &st) == 0) { if (read_text_file(tf, &raw, &rn, err)) return NULL; }
    } else if (read_text_file(tf, &raw, &rn, e2)) { snprintf(err, ERR_MAX, "%s token missing", wd->label); return NULL; }
    secret = raw ? jsx_trim(raw, rn) : strdup(""); free(raw);
    if (!*secret) { free(secret); snprintf(err, ERR_MAX, "%s token missing", wd->label); return NULL; }
    char id[17]; random_hex(id, 8);
    size_t blen; char *body = request_body(id, tool, args, &blen);
    http_reply r; bf_jv *value = NULL, *out = NULL;
    if (http_post(&u, secret, body, blen, timeout_ms, wd->max_body, &r, err)) goto done;
    if (r.status < 200 || r.status > 299) { snprintf(err, ERR_MAX, "%s HTTP %d", wd->label, r.status); goto done; }
    if (r.content_length == -2 || (r.content_length >= 0 && (size_t)r.content_length > wd->max_body) || !r.has_body ||
        r.over_budget || r.body_len > wd->max_body || !r.body_len) { snprintf(err, ERR_MAX, "invalid body size"); goto done; }
    if (!(value = jsx_parse(r.body, r.body_len, err))) goto done;
    const bf_jv *result = jsx_get(value, "result");
    if (value->type != BF_JV_OBJ || !jsx_str_is(jsx_get(value, "jsonrpc"), "2.0") || !jsx_str_is(jsx_get(value, "id"), id) ||
        jsx_truthy(jsx_get(value, "error")) || !result || result->type != BF_JV_OBJ) {
        snprintf(err, ERR_MAX, "%s response identity or RPC error refusal", wd->label); goto done;
    }
    if (jsx_truthy(bf_jv_get(result, "isError"))) { snprintf(err, ERR_MAX, "%s tool refused: %s", wd->label, tool); goto done; }
    const bf_jv *sc = bf_jv_get(result, "structuredContent");
    out = bf_jv_clone(sc && sc->type != BF_JV_NULL ? sc : result);
done:
    if (!out) jsx_redact(err, secret, "[REDACTED]", wd->slice);
    free(secret); free(body); http_reply_free(&r); bf_jv_free(value);
    return out;
}

typedef struct { const char *flag; const char *key; char kind; const char *const *choices; } Spec;   /* kind: f flag, l list, i integer, n number, s string, r repeat */
typedef struct { const char *key; bf_jv *value; } Found;
static int in_list(const char *v, const char *const *list) { for (; list && *list; list++) if (!strcmp(*list, v)) return 1; return 0; }
static bf_jv *found_get(Found *f, int n, const char *key) { for (int i = 0; i < n; i++) if (!strcmp(f[i].key, key)) return f[i].value; return NULL; }
static void found_set(Found *f, int *n, const char *key, bf_jv *v) {
    for (int i = 0; i < *n; i++) if (!strcmp(f[i].key, key)) { bf_jv_free(f[i].value); f[i].value = v; return; }
    f[*n].key = key; f[*n].value = v; (*n)++;
}
static void found_free(Found *f, int n) { for (int i = 0; i < n; i++) bf_jv_free(f[i].value); }

/* integer() shared by canary/recursive: /^[+-]?\d+$/ then safe. */
static int signed_digits(const char *t) { const char *p = t + (*t == '+' || *t == '-'); if (!*p) return 0; for (; *p; p++) if (*p < '0' || *p > '9') return 0; return 1; }

/* canary_cli options() and recursive_cli options() differ: recursive has no
 * flag/list kinds and its integer message differs; `recursive` selects that. */
static int options(int recursive, char **tokens, int n, const Spec *specs, Found *found, int *nfound, char **pos, int *npos, char *err) {
    int literal = 0; *npos = 0;
    for (int i = 0; i < n; i++) {
        const char *part = tokens[i];
        if (!literal && !strcmp(part, "--")) { literal = 1; continue; }
        if (!literal && part[0] == '-') {
            const Spec *s = NULL;
            for (const Spec *k = specs; k && k->flag; k++) if (!strcmp(k->flag, part)) s = k;
            if (!s) { snprintf(err, ERR_MAX, "unknown option: %s", part); return -1; }
            if (s->kind == 'f') { found_set(found, nfound, s->key, bf_jv_bool(1)); continue; }
            if (s->kind == 'l') {
                bf_jv *list = bf_jv_arr();
                while (i + 1 < n && tokens[i + 1][0] != '-') bf_jv_push(list, bf_jv_str(tokens[++i]));
                if (!list->len) { bf_jv_free(list); snprintf(err, ERR_MAX, "%s requires one or more values", part); return -1; }
                for (size_t k = 0; s->choices && k < list->len; k++) if (!in_list(list->items[k]->s, s->choices)) { bf_jv_free(list); snprintf(err, ERR_MAX, "%s has an unsupported value", part); return -1; }
                found_set(found, nfound, s->key, list); continue;
            }
            if (++i >= n) { snprintf(err, ERR_MAX, "%s requires a value", part); return -1; }
            const char *value = tokens[i]; bf_jv *parsed;
            if (s->kind == 'i') {
                double d = jsx_to_number(value);
                if (recursive) { if (!signed_digits(value) || !jsx_safe_integer(d)) { snprintf(err, ERR_MAX, "%s requires a safe integer", part); return -1; } }
                else {
                    if (!signed_digits(value)) { snprintf(err, ERR_MAX, "%s requires an integer", part); return -1; }
                    if (!jsx_safe_integer(d)) { snprintf(err, ERR_MAX, "%s is outside integer range", part); return -1; }
                }
                parsed = bf_jv_num(d);
            } else if (s->kind == 'n') {
                double d = jsx_to_number(value);
                if (!*value || !(d == d) || d == 1.0 / 0.0 || d == -1.0 / 0.0) { snprintf(err, ERR_MAX, "%s requires a finite number", part); return -1; }
                parsed = bf_jv_num(d);
            } else parsed = bf_jv_str(value);
            if (s->choices && !(parsed->type == BF_JV_STR && in_list(parsed->s, s->choices))) { bf_jv_free(parsed); snprintf(err, ERR_MAX, "%s has an unsupported value", part); return -1; }
            if (s->kind == 'r') {
                bf_jv *list = found_get(found, *nfound, s->key);
                if (!list) { list = bf_jv_arr(); found_set(found, nfound, s->key, list); }
                bf_jv_push(list, parsed);
            } else found_set(found, nfound, s->key, parsed);
        } else pos[(*npos)++] = (char *)part;
    }
    return 0;
}
static int positional(int have, int count, const char *command, char *err) {
    if (have != count) { snprintf(err, ERR_MAX, "%s requires %d positional argument%s", command, count, count == 1 ? "" : "s"); return -1; }
    return 0;
}

/* ── canary ─────────────────────────────────────────────────── */
static const char CANARY_HELP[] =
    "Bonfyre Hypervisor v5 Fixture Correction + Capability Canary R4.\n"
    "Usage: bonfyre-canary [--version | --help] <command> [arguments]\n"
    "Commands:\n"
    "  status                         Canary status\n"
    "  certificates                   List capability certificates\n"
    "  verify                         Verify the receipt chain\n"
    "  policies                       List canary policies\n"
    "  fixtures run [--providers rtk cavekit bernstein agent-deck] [--max-agent-turns N]\n"
    "  certificate verify PROVIDER CAPABILITY\n"
    "  policy upsert CAPABILITY PROVIDER FALLBACK_PROVIDER [--percentage N] [--effect-class CLASS]... [--enable]\n"
    "  policy state POLICY_ID proposed|enabled|suspended|disabled\n"
    "  route MISSION_ID CAPABILITY [--effect-class CLASS] [--task-id ID] [--fallback-provider PROVIDER]\n"
    "  outcome DECISION_ID passed|failed|fallback|cancelled [--latency-ms N] [--receipt-digest DIGEST]\n"
    "  decisions [--limit N]\n"
    "  outcomes [--limit N]\n";

static const char *const PROVIDERS[] = { "rtk", "cavekit", "bernstein", "agent-deck", NULL };
static const char *const EFFECTS[] = { "PURE", "READ_ONLY", "CANDIDATE_WRITE", NULL };
static const char *const POLICY_STATES[] = { "proposed", "enabled", "suspended", "disabled", NULL };
static const char *const OUTCOME_STATES[] = { "passed", "failed", "fallback", "cancelled", NULL };

/* Returns 0 call, 1 printed text (exit in *code), -1 usage error. */
static int canary_parse(int argc, char **argv, const bf_jv *tools, const char **tool, bf_jv **args, long *timeout, const char **text, int *code, char *err) {
    if (!argc) { *text = CANARY_HELP; *code = 2; return 1; }
    if (!strcmp(argv[0], "--version")) { *text = "bonfyre-canary 5.4.0-fixture-canary-r4\n"; *code = 0; return 1; }
    for (int i = 0; i < argc; i++) if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) { *text = CANARY_HELP; *code = 0; return 1; }
    const char *first = argv[0], *second = argc > 1 ? argv[1] : "";
    Found found[8]; int nf = 0, np = 0; char **pos = calloc((size_t)argc + 1, sizeof *pos); int rc = -1;
    bf_jv *a = NULL; *timeout = 120000;
    if (!strcmp(first, "status") || !strcmp(first, "certificates") || !strcmp(first, "verify") || !strcmp(first, "policies")) {
        if (positional(argc - 1, 0, first, err)) goto out;
        *tool = !strcmp(first, "status") ? "canary_status" : !strcmp(first, "certificates") ? "canary_certificate_list" : !strcmp(first, "verify") ? "canary_receipt_verify" : "canary_policy_list";
        a = bf_jv_obj();
    } else if (!strcmp(first, "fixtures") && !strcmp(second, "run")) {
        const Spec s[] = { { "--providers", "providers", 'l', PROVIDERS }, { "--max-agent-turns", "turns", 'i', NULL }, { 0 } };
        if (options(0, argv + 2, argc - 2, s, found, &nf, pos, &np, err) || positional(np, 0, "fixtures run", err)) goto out;
        a = bf_jv_obj(); bf_jv *p = found_get(found, nf, "providers");
        if (p) bf_jv_set(a, "providers", bf_jv_clone(p));
        else { bf_jv *l = bf_jv_arr(); for (int i = 0; PROVIDERS[i]; i++) bf_jv_push(l, S(PROVIDERS[i])); bf_jv_set(a, "providers", l); }
        bf_jv *t = found_get(found, nf, "turns"); double turns = t ? t->n : 4; if (turns > 4) turns = 4; if (turns < 1) turns = 1;
        bf_jv_set(a, "max_agent_turns", N(turns)); *tool = "fixture_correction_run"; *timeout = 7200000;
    } else if (!strcmp(first, "certificate") && !strcmp(second, "verify")) {
        if (positional(argc - 2, 2, "certificate verify", err)) goto out;
        a = bf_jv_obj(); bf_jv_set(a, "provider", S(argv[2])); bf_jv_set(a, "capability", S(argv[3])); *tool = "canary_certificate_verify";
    } else if (!strcmp(first, "policy") && !strcmp(second, "upsert")) {
        const Spec s[] = { { "--percentage", "percentage", 'i', NULL }, { "--effect-class", "effects", 'r', EFFECTS }, { "--enable", "enable", 'f', NULL }, { 0 } };
        if (options(0, argv + 2, argc - 2, s, found, &nf, pos, &np, err) || positional(np, 3, "policy upsert", err)) goto out;
        a = bf_jv_obj(); bf_jv_set(a, "capability", S(pos[0])); bf_jv_set(a, "provider", S(pos[1])); bf_jv_set(a, "fallback_provider", S(pos[2]));
        bf_jv *pc = found_get(found, nf, "percentage"); bf_jv_set(a, "percentage", pc ? bf_jv_clone(pc) : N(5));
        bf_jv *ef = found_get(found, nf, "effects");
        if (ef && ef->len) bf_jv_set(a, "effect_classes", bf_jv_clone(ef));
        else { bf_jv *l = bf_jv_arr(); bf_jv_push(l, S("PURE")); bf_jv_push(l, S("READ_ONLY")); bf_jv_set(a, "effect_classes", l); }
        bf_jv_set(a, "enable", bf_jv_bool(found_get(found, nf, "enable") != NULL)); *tool = "canary_policy_upsert";
    } else if (!strcmp(first, "policy") && !strcmp(second, "state")) {
        if (positional(argc - 2, 2, "policy state", err)) goto out;
        if (!in_list(argv[3], POLICY_STATES)) { snprintf(err, ERR_MAX, "unsupported policy state"); goto out; }
        a = bf_jv_obj(); bf_jv_set(a, "policy_id", S(argv[2])); bf_jv_set(a, "state", S(argv[3])); *tool = "canary_policy_state";
    } else if (!strcmp(first, "route")) {
        const Spec s[] = { { "--effect-class", "effect_class", 's', NULL }, { "--task-id", "task_id", 's', NULL }, { "--fallback-provider", "fallback_provider", 's', NULL }, { 0 } };
        if (options(0, argv + 1, argc - 1, s, found, &nf, pos, &np, err) || positional(np, 2, "route", err)) goto out;
        a = bf_jv_obj(); bf_jv_set(a, "mission_id", S(pos[0])); bf_jv_set(a, "capability", S(pos[1]));
        bf_jv *ec = found_get(found, nf, "effect_class"), *ti = found_get(found, nf, "task_id"), *fp = found_get(found, nf, "fallback_provider");
        bf_jv_set(a, "effect_class", ec ? bf_jv_clone(ec) : S("CANDIDATE_WRITE"));
        if (jsx_truthy(ti)) bf_jv_set(a, "task_id", bf_jv_clone(ti));
        bf_jv_set(a, "fallback_provider", fp ? bf_jv_clone(fp) : S("bonfyre-native")); *tool = "canary_route_decide";
    } else if (!strcmp(first, "outcome")) {
        const Spec s[] = { { "--latency-ms", "latency_ms", 'n', NULL }, { "--receipt-digest", "receipt_digest", 's', NULL }, { 0 } };
        if (options(0, argv + 1, argc - 1, s, found, &nf, pos, &np, err) || positional(np, 2, "outcome", err)) goto out;
        if (!in_list(pos[1], OUTCOME_STATES)) { snprintf(err, ERR_MAX, "unsupported outcome state"); goto out; }
        a = bf_jv_obj(); bf_jv_set(a, "decision_id", S(pos[0])); bf_jv_set(a, "state", S(pos[1]));
        bf_jv *lm = found_get(found, nf, "latency_ms"), *rd = found_get(found, nf, "receipt_digest");
        if (lm) bf_jv_set(a, "latency_ms", bf_jv_clone(lm));
        if (jsx_truthy(rd)) bf_jv_set(a, "receipt_digest", bf_jv_clone(rd));
        *tool = "canary_outcome_record";
    } else if (!strcmp(first, "decisions") || !strcmp(first, "outcomes")) {
        const Spec s[] = { { "--limit", "limit", 'i', NULL }, { 0 } };
        if (options(0, argv + 1, argc - 1, s, found, &nf, pos, &np, err) || positional(np, 0, first, err)) goto out;
        bf_jv *l = found_get(found, nf, "limit");
        a = bf_jv_obj(); bf_jv_set(a, "limit", l ? bf_jv_clone(l) : N(100));
        *tool = !strcmp(first, "decisions") ? "canary_decision_list" : "canary_outcome_list";
    } else { snprintf(err, ERR_MAX, "unknown or incomplete canary command"); goto out; }
    {
        const bf_jv *def = catalog_tool(tools, *tool);
        if (!def) { snprintf(err, ERR_MAX, "canary command has no catalog owner"); rc = -2; goto out; }
        if (schema_validate(a, jsx_get(def, "inputSchema"), "arguments", err)) goto out;
    }
    *args = a; a = NULL; rc = 0;
out:
    bf_jv_free(a); found_free(found, nf); free(pos);
    return rc;
}

int cli_canary(int argc, char **argv) {
    char err[ERR_MAX];
    bf_jv *tools = catalog_load("integrations/mcp/canary_catalog.json", err);
    if (!tools) { fprintf(stderr, "%s\n", err); return 1; }
    const char *tool = NULL, *text = NULL; bf_jv *args = NULL; long timeout = 0; int code = 0;
    int r = canary_parse(argc, argv, tools, &tool, &args, &timeout, &text, &code, err);
    if (r == 1) { fputs(text, stdout); bf_jv_free(tools); return code; }
    if (r == -1) { fprintf(stderr, "%s\n%s", err, CANARY_HELP); bf_jv_free(tools); return 2; }
    if (r == -2) { fprintf(stderr, "%s\n", err); bf_jv_free(tools); return 1; }
    bf_jv *result = invoke(&CANARY, tools, tool, args, timeout, err);
    bf_jv_free(args); bf_jv_free(tools);
    if (!result) { fprintf(stderr, "%s\n", err); return 1; }
    jsx_print(stdout, result); bf_jv_free(result);
    return 0;
}

/* ── recursive ──────────────────────────────────────────────── */
static const char RECURSIVE_HELP[] =
    "Bonfyre v5 recursive substrate control.\n"
    "Usage: bonfyre-recursive [--version | --help] <command> [arguments]\n"
    "Commands:\n"
    "  status | replay | tokenizers | native-smoke\n"
    "  ingest PATH [--kind KIND] [--classification CLASS] [--schema SCHEMA]\n"
    "  tokenize ARTIFACT_ID [--tokenizer TOKENIZER]\n"
    "  delta SOURCE_TOKEN_ARTIFACT_ID TARGET_TOKEN_ARTIFACT_ID\n"
    "  dependency SOURCE_KIND SOURCE_ID TARGET_KIND TARGET_ID EDGE_KIND\n"
    "  closure ROOT_KIND ROOT_ID [--depth N]\n"
    "  mission-bootstrap MISSION_JSON SPEC_PATH [--context PATH] [--tokenizer TOKENIZER]\n"
    "  mission-finalize MISSION_JSON [--source-bfp-root ROOT] [--target-bfp-root ROOT] [--changed-section SECTION]...\n";

static bf_jv *read_mission(const char *path, char *err) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) { snprintf(err, ERR_MAX, "mission JSON is missing or invalid"); return NULL; }
    struct stat st; bf_jv *v = NULL;
    if (fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size > 1000000) { close(fd); snprintf(err, ERR_MAX, "mission JSON must be a regular file at most 1 MB"); return NULL; }
    char *buf = malloc((size_t)st.st_size + 1); size_t n = 0;
    for (ssize_t r; (r = read(fd, buf + n, (size_t)st.st_size - n)) > 0 && n < (size_t)st.st_size; ) n += (size_t)r;
    close(fd);
    char e2[ERR_MAX]; v = jsx_parse(buf, n, e2); free(buf);
    if (!v) { snprintf(err, ERR_MAX, "mission JSON is missing or invalid"); return NULL; }
    if (v->type != BF_JV_OBJ) { bf_jv_free(v); snprintf(err, ERR_MAX, "mission JSON must be an object"); return NULL; }
    return v;
}

/* Returns 0 call, 1 printed text, 2 smoke, -1 usage, -2 plain error. */
static int recursive_parse(int argc, char **argv, const bf_jv *tools, const char **tool, bf_jv **args, long *timeout, const char **text, int *code, char *err) {
    if (!argc) { *text = RECURSIVE_HELP; *code = 2; return 1; }
    if (!strcmp(argv[0], "--version")) {
        if (positional(argc - 1, 0, "--version", err)) return -1;
        *text = "bonfyre-recursive 5.1.0-recursive-r1\n"; *code = 0; return 1;
    }
    for (int i = 0; i < argc; i++) if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) { *text = RECURSIVE_HELP; *code = 0; return 1; }
    const char *command = argv[0]; char **rest = argv + 1; int nrest = argc - 1;
    Found found[8]; int nf = 0, np = 0; char **pos = calloc((size_t)argc + 1, sizeof *pos); int rc = -1;
    bf_jv *a = NULL; *timeout = 120000;
    if (!strcmp(command, "status") || !strcmp(command, "replay") || !strcmp(command, "tokenizers")) {
        if (positional(nrest, 0, command, err)) goto out;
        *tool = !strcmp(command, "status") ? "recursive_status" : !strcmp(command, "replay") ? "substrate_replay_verify" : "tokenizer_list";
        a = bf_jv_obj();
    } else if (!strcmp(command, "native-smoke")) {
        if (positional(nrest, 0, command, err)) goto out;
        rc = 2; goto out;
    } else if (!strcmp(command, "ingest")) {
        const Spec s[] = { { "--kind", "kind", 's', NULL }, { "--classification", "classification", 's', NULL }, { "--schema", "schema", 's', NULL }, { 0 } };
        if (options(1, rest, nrest, s, found, &nf, pos, &np, err) || positional(np, 1, command, err)) goto out;
        bf_jv *k = found_get(found, nf, "kind"), *c = found_get(found, nf, "classification"), *sc = found_get(found, nf, "schema");
        a = bf_jv_obj(); bf_jv_set(a, "path", S(pos[0])); bf_jv_set(a, "kind", k ? bf_jv_clone(k) : S("source"));
        bf_jv_set(a, "classification", c ? bf_jv_clone(c) : S("internal"));
        if (jsx_truthy(sc)) bf_jv_set(a, "schema_name", bf_jv_clone(sc));
        *tool = "artifact_ingest"; *timeout = 1800000;
    } else if (!strcmp(command, "tokenize")) {
        const Spec s[] = { { "--tokenizer", "tokenizer", 's', NULL }, { 0 } };
        if (options(1, rest, nrest, s, found, &nf, pos, &np, err) || positional(np, 1, command, err)) goto out;
        bf_jv *t = found_get(found, nf, "tokenizer");
        a = bf_jv_obj(); bf_jv_set(a, "artifact_id", S(pos[0])); bf_jv_set(a, "tokenizer_id", t ? bf_jv_clone(t) : S("lexeme-v1"));
        *tool = "token_artifact_compile"; *timeout = 1800000;
    } else if (!strcmp(command, "delta")) {
        if (positional(nrest, 2, command, err)) goto out;
        a = bf_jv_obj(); bf_jv_set(a, "source_token_artifact_id", S(rest[0])); bf_jv_set(a, "target_token_artifact_id", S(rest[1]));
        *tool = "token_delta_derive"; *timeout = 1800000;
    } else if (!strcmp(command, "dependency")) {
        if (positional(nrest, 5, command, err)) goto out;
        const char *k[] = { "source_kind", "source_id", "target_kind", "target_id", "edge_kind" };
        a = bf_jv_obj(); for (int i = 0; i < 5; i++) bf_jv_set(a, k[i], S(rest[i]));
        *tool = "dependency_register";
    } else if (!strcmp(command, "closure")) {
        const Spec s[] = { { "--depth", "depth", 'i', NULL }, { 0 } };
        if (options(1, rest, nrest, s, found, &nf, pos, &np, err) || positional(np, 2, command, err)) goto out;
        bf_jv *dv = found_get(found, nf, "depth"); double depth = dv ? dv->n : 12;
        if (depth < 0 || depth > 64) { snprintf(err, ERR_MAX, "--depth must be between 0 and 64"); goto out; }
        a = bf_jv_obj(); bf_jv_set(a, "root_kind", S(pos[0])); bf_jv_set(a, "root_id", S(pos[1])); bf_jv_set(a, "max_depth", N(depth));
        *tool = "affected_closure_compile";
    } else if (!strcmp(command, "mission-bootstrap")) {
        const Spec s[] = { { "--context", "context", 's', NULL }, { "--tokenizer", "tokenizer", 's', NULL }, { 0 } };
        if (options(1, rest, nrest, s, found, &nf, pos, &np, err) || positional(np, 2, command, err)) goto out;
        bf_jv *m = read_mission(pos[0], err); if (!m) goto out;
        bf_jv *cx = found_get(found, nf, "context"), *t = found_get(found, nf, "tokenizer");
        a = bf_jv_obj(); bf_jv_set(a, "mission", m); bf_jv_set(a, "spec_path", S(pos[1]));
        if (jsx_truthy(cx)) bf_jv_set(a, "context_packet_path", bf_jv_clone(cx));
        bf_jv_set(a, "tokenizer_id", t ? bf_jv_clone(t) : S("lexeme-v1"));
        *tool = "recursive_mission_bootstrap"; *timeout = 3600000;
    } else if (!strcmp(command, "mission-finalize")) {
        const Spec s[] = { { "--source-bfp-root", "source", 's', NULL }, { "--target-bfp-root", "target", 's', NULL }, { "--changed-section", "sections", 'r', NULL }, { 0 } };
        if (options(1, rest, nrest, s, found, &nf, pos, &np, err) || positional(np, 1, command, err)) goto out;
        bf_jv *m = read_mission(pos[0], err); if (!m) goto out;
        bf_jv *so = found_get(found, nf, "source"), *ta = found_get(found, nf, "target"), *se = found_get(found, nf, "sections");
        a = bf_jv_obj(); bf_jv_set(a, "mission", m);
        if (jsx_truthy(so)) bf_jv_set(a, "source_bfp_root", bf_jv_clone(so));
        if (jsx_truthy(ta)) bf_jv_set(a, "target_bfp_root", bf_jv_clone(ta));
        bf_jv_set(a, "changed_sections", se ? bf_jv_clone(se) : bf_jv_arr());
        *tool = "recursive_mission_finalize"; *timeout = 3600000;
    } else { snprintf(err, ERR_MAX, "unknown recursive command"); goto out; }
    {
        const bf_jv *def = catalog_tool(tools, *tool);
        if (!def) { snprintf(err, ERR_MAX, "recursive command has no catalog owner"); rc = -2; goto out; }
        if (schema_validate(a, jsx_get(def, "inputSchema"), "arguments", err)) goto out;
    }
    *args = a; a = NULL; rc = 0;
out:
    bf_jv_free(a); found_free(found, nf); free(pos);
    return rc;
}

static bf_jv *recursive_smoke(const bf_jv *tools, char *err) {
    /* join(tmpdir(), name): path.join normalises '//', '.' and '..'. */
    const char *t = getenv("TMPDIR"); char raw[4096], dir[4096];
    snprintf(raw, sizeof raw, "%s/bonfyre-recursive-smoke-XXXXXX", t && *t ? t : "/tmp");
    {
        size_t o = 0; const char *p = raw; int abs_ = *p == '/';
        while (*p) {
            while (*p == '/') p++;
            const char *seg = p; while (*p && *p != '/') p++;
            size_t sl = (size_t)(p - seg);
            if (!sl || (sl == 1 && seg[0] == '.')) continue;
            if (sl == 2 && seg[0] == '.' && seg[1] == '.') { while (o > 0 && dir[o - 1] != '/') o--; if (o > 0) o--; continue; }
            if (o || abs_) dir[o++] = '/';
            memcpy(dir + o, seg, sl); o += sl;
        }
        dir[o] = 0;
    }
    if (!mkdtemp(dir)) { snprintf(err, ERR_MAX, "%s", strerror(errno)); return NULL; }
    char smoke[4200]; snprintf(smoke, sizeof smoke, "%s/smoke.txt", dir);
    bf_jv *artifact = NULL, *tokens = NULL, *replay = NULL, *out = NULL, *x;
    FILE *f = fopen(smoke, "w"); if (f) { fputs("alpha\r\nbeta\nalpha\n", f); fclose(f); }
    x = bf_jv_obj(); bf_jv_set(x, "path", S(smoke)); bf_jv_set(x, "kind", S("smoke"));
    artifact = invoke(&RECURSIVE, tools, "artifact_ingest", x, 1800000, err); bf_jv_free(x);
    if (!artifact) goto done;
    const bf_jv *aid = jsx_get(artifact, "artifact_id");
    if (artifact->type != BF_JV_OBJ || !aid || aid->type != BF_JV_STR || !aid->len) { snprintf(err, ERR_MAX, "smoke ingest lacks artifact identity"); goto done; }
    x = bf_jv_obj(); bf_jv_set(x, "artifact_id", bf_jv_clone(aid)); bf_jv_set(x, "tokenizer_id", S("lexeme-v1"));
    tokens = invoke(&RECURSIVE, tools, "token_artifact_compile", x, 1800000, err); bf_jv_free(x);
    if (!tokens) goto done;
    x = bf_jv_obj(); replay = invoke(&RECURSIVE, tools, "substrate_replay_verify", x, 120000, err); bf_jv_free(x);
    if (!replay) goto done;
    const bf_jv *ver = jsx_get(replay, "verified");
    if (replay->type != BF_JV_OBJ || !ver || ver->type != BF_JV_BOOL) { snprintf(err, ERR_MAX, "smoke replay lacks verification result"); goto done; }
    out = bf_jv_obj(); bf_jv_set(out, "state", S(ver->b ? "passed" : "failed"));
    bf_jv_set(out, "artifact", artifact); bf_jv_set(out, "tokens", tokens); bf_jv_set(out, "replay", replay);
    artifact = tokens = replay = NULL;
done:
    bf_jv_free(artifact); bf_jv_free(tokens); bf_jv_free(replay);
    unlink(smoke); rmdir(dir);
    return out;
}

int cli_recursive(int argc, char **argv) {
    char err[ERR_MAX];
    bf_jv *tools = catalog_load("integrations/mcp/recursive_catalog.json", err);
    if (!tools) { fprintf(stderr, "%s\n", err); return 1; }
    const char *tool = NULL, *text = NULL; bf_jv *args = NULL, *result; long timeout = 0; int code = 0;
    int r = recursive_parse(argc, argv, tools, &tool, &args, &timeout, &text, &code, err);
    if (r == 1) { fputs(text, stdout); bf_jv_free(tools); return code; }
    if (r == -1) { fprintf(stderr, "%s\n%s", err, RECURSIVE_HELP); bf_jv_free(tools); return 2; }
    if (r == -2) { fprintf(stderr, "%s\n", err); bf_jv_free(tools); return 1; }
    result = r == 2 ? recursive_smoke(tools, err) : invoke(&RECURSIVE, tools, tool, args, timeout, err);
    bf_jv_free(args); bf_jv_free(tools);
    if (!result) { fprintf(stderr, "%s\n", err); return 1; }
    jsx_print(stdout, result);
    int failed = r == 2 && jsx_str_is(jsx_get(result, "state"), "failed");
    bf_jv_free(result);
    return failed ? 1 : 0;
}
