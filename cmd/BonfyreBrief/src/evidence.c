/*
 * Transformation evidence for a cited migration brief.
 *
 * A brief may cite receipts that DatabaseBridge wrote (bonfyre-bridge
 * schema-map and map-verify).  Each is accepted only when:
 *   - a schema-map still matches its own sealed mapping_digest;
 *   - a map-verify names the mapping_digest of a schema-map given with it,
 *     so a diff cannot be shown without the mapping it measured;
 *   - a rule source observed through ForeignTwin has an allowed HTTP 200
 *     receipt in the brief's receipts directory with the same content sha.
 * The brief quotes what the receipts measured; it never re-runs them and
 * never upgrades a rehearsal into a statement about the customer's system.
 */
#include <ctype.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_jsonw.h>
#include "evidence.h"

#define MAX_EVIDENCE 8

typedef struct {
    char path[1024], kind[16], file_sha[65], digest[65], mapping_digest[65], plan_id[96];
    char *buf; size_t len; bf_json_doc_t *doc;
} Ev;

static Ev g_ev[MAX_EVIDENCE];
static int g_nev;
static char g_digest[65] = "none";

static int sval(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k, char *out, size_t sz) {
    const bf_json_node_t *n = o ? bf_json_obj_get(d, o, k) : NULL;
    out[0] = 0;
    if (!n || n->type != BF_JSON_STRING) return 0;
    int len = 0; const char *raw = bf_json_get_str(n, &len);
    size_t ul = 0; char *u = bf_json_unescape(raw, len, &ul);
    if (!u) return 0;
    snprintf(out, sz, "%.*s", (int)ul, u);
    free(u);
    return 1;
}

static long long ival(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k) {
    const bf_json_node_t *n = o ? bf_json_obj_get(d, o, k) : NULL;
    return n && n->type == BF_JSON_INT ? (long long)bf_json_get_int(n) : 0;
}

static int bval(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k) {
    const bf_json_node_t *n = o ? bf_json_obj_get(d, o, k) : NULL;
    return n && n->type == BF_JSON_BOOL && bf_json_get_bool(n);
}

/* Same seal as bonfyre-bridge: sha256 of the document without the digest key. */
static int sealed(const char *buf, size_t len, const char *key, char hex[65]) {
    char marker[96];
    snprintf(marker, sizeof marker, ",\n  \"%s\": \"", key);
    const char *at = NULL;
    for (const char *p = buf; (p = strstr(p, marker)); p++) at = p;
    if (!at || (size_t)(at - buf) + strlen(marker) + 64 > len) return 0;
    char stated[65];
    memcpy(stated, at + strlen(marker), 64); stated[64] = 0;
    size_t head = (size_t)(at - buf);
    char *body = malloc(head + 3);
    if (!body) return 0;
    memcpy(body, buf, head); memcpy(body + head, "\n}", 2);
    bf_sha256_hex((const uint8_t *)body, head + 2, hex);
    free(body);
    return !strcmp(stated, hex);
}

static int receipt_matches(const char *dir, const char *name, const char *sha) {
    if (!name[0] || strchr(name, '/')) return 0;
    char path[2048]; snprintf(path, sizeof path, "%s/%s", dir, name);
    size_t len = 0; char *b = bf_read_file(path, &len), err[128];
    bf_json_doc_t *d = b ? bf_json_parse(b, len, err, sizeof err) : NULL;
    int ok = 0;
    if (d) {
        const bf_json_node_t *r = bf_json_root(d);
        char pol[32], csha[80];
        sval(d, r, "policy_decision", pol, sizeof pol); sval(d, r, "content_sha256", csha, sizeof csha);
        ok = !strcmp(pol, "allow") && ival(d, r, "http_status") == 200 && !strcmp(csha, sha);
        bf_json_free(d);
    }
    free(b);
    return ok;
}

int brief_evidence_load(const char *const *paths, int n, const char *receipts, brief_exc_fn exc) {
    if (n > MAX_EVIDENCE) { fprintf(stderr, "cited: at most %d evidence files\n", MAX_EVIDENCE); return -1; }
    for (int i = 0; i < n; i++) {
        Ev *e = &g_ev[g_nev];
        memset(e, 0, sizeof *e);
        snprintf(e->path, sizeof e->path, "%s", paths[i]);
        char err[160], schema[64];
        e->buf = bf_read_file(paths[i], &e->len);
        e->doc = e->buf ? bf_json_parse(e->buf, e->len, err, sizeof err) : NULL;
        if (!e->doc) { fprintf(stderr, "cited: evidence %s unreadable\n", paths[i]); return -1; }
        bf_sha256_hex((const uint8_t *)e->buf, e->len, e->file_sha);
        const bf_json_node_t *r = bf_json_root(e->doc);
        sval(e->doc, r, "schema", schema, sizeof schema);
        sval(e->doc, r, "plan_id", e->plan_id, sizeof e->plan_id);
        if (!strcmp(schema, "bonfyre.bridge.schema-map.v1")) {
            snprintf(e->kind, sizeof e->kind, "schema-map");
            if (!sealed(e->buf, e->len, "mapping_digest", e->digest)) { fprintf(stderr, "cited: mapping %s does not match its own digest\n", paths[i]); return -1; }
            snprintf(e->mapping_digest, sizeof e->mapping_digest, "%s", e->digest);
            const bf_json_node_t *prov = bf_json_dotpath(e->doc, "rules.provenance");
            char pk[48], rc[256], csha[80], src[512];
            sval(e->doc, prov, "kind", pk, sizeof pk); sval(e->doc, prov, "receipt", rc, sizeof rc);
            sval(e->doc, prov, "content_sha256", csha, sizeof csha); sval(e->doc, prov, "source", src, sizeof src);
            if (!strcmp(pk, "foreign-twin-observation") && !receipt_matches(receipts, rc, csha))
                exc("evidence-rules-unreceipted", src, "the mapping's rule source has no allowed HTTP 200 receipt with the same sha in this receipts directory; its rules are shown but not counted as observed");
            if (!strcmp(pk, "vendored-upstream-file"))
                exc("evidence-rules-vendored", src, "the mapping's rules come from a file vendored in this repository, not from a ForeignTwin observation");
        } else if (!strcmp(schema, "bonfyre.bridge.map-verify.v1")) {
            snprintf(e->kind, sizeof e->kind, "map-verify");
            sval(e->doc, r, "diff_digest", e->digest, sizeof e->digest);
            sval(e->doc, r, "mapping_digest", e->mapping_digest, sizeof e->mapping_digest);
            if (strlen(e->digest) != 64) { fprintf(stderr, "cited: diff %s has no diff_digest\n", paths[i]); return -1; }
        } else { fprintf(stderr, "cited: %s is not a DatabaseBridge transformation receipt\n", paths[i]); return -1; }
        g_nev++;
    }
    /* A diff only with the mapping it measured. */
    for (int i = 0; i < g_nev; i++) {
        if (strcmp(g_ev[i].kind, "map-verify")) continue;
        int found = 0;
        for (int j = 0; j < g_nev; j++) if (!strcmp(g_ev[j].kind, "schema-map") && !strcmp(g_ev[j].digest, g_ev[i].mapping_digest)) found = 1;
        if (!found) { fprintf(stderr, "cited: diff %s measured a mapping that was not given with it\n", g_ev[i].path); return -1; }
    }
    if (g_nev) {
        /* Order-independent: digests sorted. */
        char *ds[MAX_EVIDENCE];
        for (int i = 0; i < g_nev; i++) ds[i] = g_ev[i].file_sha;
        for (int i = 0; i < g_nev; i++) for (int j = i + 1; j < g_nev; j++) if (strcmp(ds[j], ds[i]) < 0) { char *t = ds[i]; ds[i] = ds[j]; ds[j] = t; }
        BfSha256 h; uint8_t hb[32];
        bf_sha256_init(&h);
        for (int i = 0; i < g_nev; i++) { bf_sha256_update(&h, (const uint8_t *)ds[i], 64); bf_sha256_update(&h, (const uint8_t *)"\n", 1); }
        bf_sha256_final(&h, hb); bf_sha256_digest_hex(hb, g_digest);
    }
    return g_nev;
}

const char *brief_evidence_digest(void) { return g_digest; }

int brief_evidence_has(const char *kind) {
    for (int i = 0; i < g_nev; i++) if (!strcmp(g_ev[i].kind, kind)) return 1;
    return 0;
}

void brief_evidence_refs(bf_jsonw *w, const char *kind) {
    bf_jsonw_arr(w);
    for (int i = 0; i < g_nev; i++) if (!strcmp(g_ev[i].kind, kind)) bf_jsonw_str(w, g_ev[i].digest);
    bf_jsonw_arr_end(w);
}

static void copy_keys(bf_jsonw *w, const bf_json_doc_t *d, const bf_json_node_t *o, const char *const *keys) {
    for (int k = 0; keys[k]; k++) {
        const bf_json_node_t *n = bf_json_obj_get(d, o, keys[k]);
        if (!n) continue;
        bf_jsonw_key(w, keys[k]); bf_jsonw_node(w, d, n, 0);
    }
}

void brief_evidence_json(bf_jsonw *w) {
    bf_jsonw_key(w, "transformation_evidence"); bf_jsonw_arr(w);
    for (int i = 0; i < g_nev; i++) {
        const Ev *e = &g_ev[i];
        const bf_json_node_t *r = bf_json_root(e->doc);
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "kind"); bf_jsonw_str(w, e->kind);
        bf_jsonw_key(w, "file_sha256"); bf_jsonw_str(w, e->file_sha);
        bf_jsonw_key(w, "digest"); bf_jsonw_str(w, e->digest);
        bf_jsonw_key(w, "mapping_digest"); bf_jsonw_str(w, e->mapping_digest);
        if (!strcmp(e->kind, "schema-map")) {
            static const char *const top[] = {"plan_id", "target_engine", "columns_total", "columns_typed_by_rule", "exceptions", NULL};
            copy_keys(w, e->doc, r, top);
            bf_jsonw_key(w, "source_file_sha256"); bf_jsonw_node(w, e->doc, bf_json_dotpath(e->doc, "source.file_sha256"), 0);
            bf_jsonw_key(w, "source_objects"); bf_jsonw_node(w, e->doc, bf_json_dotpath(e->doc, "source.objects"), 0);
            bf_jsonw_key(w, "rules_provenance"); bf_jsonw_node(w, e->doc, bf_json_dotpath(e->doc, "rules.provenance"), 0);
            bf_jsonw_key(w, "column_mappings"); bf_jsonw_arr(w);
            for (const bf_json_node_t *t = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, r, "tables")); t; t = bf_json_child_next(e->doc, t)) {
                char ts[64], tt[96]; sval(e->doc, t, "source", ts, sizeof ts); sval(e->doc, t, "target", tt, sizeof tt);
                for (const bf_json_node_t *c = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, t, "columns")); c; c = bf_json_child_next(e->doc, c)) {
                    char cs[64], ct[64], st[96], tty[128], col[200];
                    sval(e->doc, c, "source", cs, sizeof cs); sval(e->doc, c, "target", ct, sizeof ct);
                    sval(e->doc, c, "source_type", st, sizeof st); sval(e->doc, c, "target_type", tty, sizeof tty);
                    bf_jsonw_obj(w);
                    snprintf(col, sizeof col, "%s.%s", ts, cs); bf_jsonw_key(w, "source"); bf_jsonw_str(w, col);
                    snprintf(col, sizeof col, "%s.%s", tt, ct); bf_jsonw_key(w, "target"); bf_jsonw_str(w, col);
                    bf_jsonw_key(w, "source_type"); bf_jsonw_str(w, st);
                    bf_jsonw_key(w, "target_type"); bf_jsonw_str(w, tty);
                    const bf_json_node_t *rule = bf_json_obj_get(e->doc, c, "rule");
                    bf_jsonw_key(w, "rule");
                    if (rule && rule->type == BF_JSON_OBJECT) {
                        static const char *const rk[] = {"excerpt", "line", "byte_offset", "byte_length", NULL};
                        bf_jsonw_obj(w); copy_keys(w, e->doc, rule, rk); bf_jsonw_obj_end(w);
                    } else bf_jsonw_null(w);
                    const bf_json_node_t *tf = bf_json_obj_get(e->doc, c, "transform");
                    bf_jsonw_key(w, "transform"); if (tf) bf_jsonw_node(w, e->doc, tf, 1); else bf_jsonw_null(w);
                    bf_jsonw_obj_end(w);
                }
            }
            bf_jsonw_arr_end(w);
        } else {
            static const char *const top[] = {"plan_id", "source_rows", "target_rows", "verdict", "recheck", "exceptions", NULL};
            copy_keys(w, e->doc, r, top);
            bf_jsonw_key(w, "source_file_sha256"); bf_jsonw_node(w, e->doc, bf_json_dotpath(e->doc, "source.file_sha256"), 0);
            bf_jsonw_key(w, "target_engine"); bf_jsonw_node(w, e->doc, bf_json_dotpath(e->doc, "target.engine"), 0);
            bf_jsonw_key(w, "tables"); bf_jsonw_arr(w);
            for (const bf_json_node_t *t = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, r, "tables")); t; t = bf_json_child_next(e->doc, t)) {
                static const char *const tk[] = {"source", "target", "state", "source_rows", "target_rows", "expected_rows_sha256", "target_rows_sha256", "missing_in_target", "extra_in_target", "constraints_match", NULL};
                bf_jsonw_obj(w); copy_keys(w, e->doc, t, tk);
                int nc = 0, vm = 0, tm = 0;
                for (const bf_json_node_t *c = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, t, "columns")); c; c = bf_json_child_next(e->doc, c)) {
                    nc++; vm += bval(e->doc, c, "values_match"); tm += bval(e->doc, c, "type_matches");
                }
                bf_jsonw_key(w, "columns"); bf_jsonw_int(w, nc);
                bf_jsonw_key(w, "columns_values_match"); bf_jsonw_int(w, vm);
                bf_jsonw_key(w, "columns_types_match"); bf_jsonw_int(w, tm);
                bf_jsonw_obj_end(w);
            }
            bf_jsonw_arr_end(w);
            bf_jsonw_key(w, "probes"); bf_jsonw_node(w, e->doc, bf_json_obj_get(e->doc, r, "probes"), 0);
        }
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
}

static void md_text(FILE *o, const char *s) {
    for (; *s; s++) { if (*s == '\n' || *s == '\r') fputc(' ', o); else if (*s == '`') fputc('\'', o); else fputc(*s, o); }
}

void brief_evidence_md(FILE *o) {
    if (!g_nev) return;
    fprintf(o, "## What a rehearsal measured\n\n");
    fprintf(o, "These numbers come from receipts written by the database bridge on the copy each one names. They describe that copy only. They say nothing about your database until the same steps run on a rehearsal copy of it.\n\n");
    for (int i = 0; i < g_nev; i++) {
        const Ev *e = &g_ev[i];
        const bf_json_node_t *r = bf_json_root(e->doc);
        if (!strcmp(e->kind, "schema-map")) {
            char src[512], tgt[32];
            sval(e->doc, bf_json_dotpath(e->doc, "rules.provenance"), "source", src, sizeof src);
            sval(e->doc, r, "target_engine", tgt, sizeof tgt);
            fprintf(o, "Mapping `%s` (digest `%.12s…`) types %lld of %lld columns for %s using rules quoted from %s:\n\n",
                    e->plan_id, e->digest, ival(e->doc, r, "columns_typed_by_rule"), ival(e->doc, r, "columns_total"), tgt, src);
            /* Each rule once, with the columns it typed. */
            char seen[64][160]; int ns = 0;
            for (const bf_json_node_t *t = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, r, "tables")); t; t = bf_json_child_next(e->doc, t))
                for (const bf_json_node_t *c = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, t, "columns")); c; c = bf_json_child_next(e->doc, c)) {
                    const bf_json_node_t *rule = bf_json_obj_get(e->doc, c, "rule");
                    if (!rule || rule->type != BF_JSON_OBJECT) continue;
                    char ex[160]; sval(e->doc, rule, "excerpt", ex, sizeof ex);
                    int dup = 0; for (int k = 0; k < ns; k++) if (!strcmp(seen[k], ex)) dup = 1;
                    if (dup || ns >= 64) continue;
                    snprintf(seen[ns++], 160, "%s", ex);
                    fprintf(o, "- \""); md_text(o, ex); fprintf(o, "\" [line %lld] typed", ival(e->doc, rule, "line"));
                    int first = 1;
                    for (const bf_json_node_t *t2 = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, r, "tables")); t2; t2 = bf_json_child_next(e->doc, t2))
                        for (const bf_json_node_t *c2 = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, t2, "columns")); c2; c2 = bf_json_child_next(e->doc, c2)) {
                            const bf_json_node_t *r2 = bf_json_obj_get(e->doc, c2, "rule");
                            char ex2[160], tn[96], cn[64];
                            if (!r2 || r2->type != BF_JSON_OBJECT) continue;
                            sval(e->doc, r2, "excerpt", ex2, sizeof ex2);
                            if (strcmp(ex2, ex)) continue;
                            sval(e->doc, t2, "target", tn, sizeof tn); sval(e->doc, c2, "target", cn, sizeof cn);
                            fprintf(o, "%s %s.%s", first ? "" : ",", tn, cn); first = 0;
                        }
                    fprintf(o, "\n");
                }
            fprintf(o, "\n");
        } else {
            char eng[32]; sval(e->doc, bf_json_dotpath(e->doc, "target"), "engine", eng, sizeof eng);
            const bf_json_node_t *v = bf_json_obj_get(e->doc, r, "verdict");
            fprintf(o, "Rehearsal on %s for `%s` (diff `%.12s…`): %lld source rows, %lld rehearsal rows.\n\n", eng, e->plan_id, e->digest,
                    ival(e->doc, r, "source_rows"), ival(e->doc, r, "target_rows"));
            for (const bf_json_node_t *t = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, r, "tables")); t; t = bf_json_child_next(e->doc, t)) {
                char ts[64], tt[96], st[32]; sval(e->doc, t, "source", ts, sizeof ts); sval(e->doc, t, "target", tt, sizeof tt); sval(e->doc, t, "state", st, sizeof st);
                int nc = 0, vm = 0, tm = 0;
                for (const bf_json_node_t *c = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, t, "columns")); c; c = bf_json_child_next(e->doc, c)) {
                    nc++; vm += bval(e->doc, c, "values_match"); tm += bval(e->doc, c, "type_matches");
                }
                fprintf(o, "- %s to %s: %lld rows here, %lld there; %d of %d columns have matching checksums and %d of %d the expected type%s.\n",
                        ts, tt, ival(e->doc, t, "source_rows"), ival(e->doc, t, "target_rows"), vm, nc, tm, nc,
                        bval(e->doc, t, "constraints_match") ? "" : "; constraint counts differ");
            }
            for (const bf_json_node_t *p = bf_json_child_first(e->doc, bf_json_obj_get(e->doc, r, "probes")); p; p = bf_json_child_next(e->doc, p)) {
                char pu[256], a[200], b[200]; sval(e->doc, p, "purpose", pu, sizeof pu); sval(e->doc, p, "source_result", a, sizeof a); sval(e->doc, p, "target_result", b, sizeof b);
                if (bval(e->doc, p, "same")) fprintf(o, "- Checked that %s: both answered the same.\n", pu);
                else { fprintf(o, "- Checked that %s: they did not. The old system answered `", pu); md_text(o, a); fprintf(o, "`, the rehearsal `"); md_text(o, b); fprintf(o, "`.\n"); }
            }
            fprintf(o, "\nRows and values %s; types %s; constraints %s; views and triggers %s; query behaviour %s.\n\n",
                    bval(e->doc, v, "rows_and_values_match") ? "match" : "do not match",
                    bval(e->doc, v, "types_match") ? "match" : "do not match",
                    bval(e->doc, v, "constraints_match") ? "match" : "do not match",
                    bval(e->doc, v, "objects_carried") ? "carried" : "not all carried",
                    bval(e->doc, v, "behavior_matches") ? "matches" : "differs");
        }
        const bf_json_node_t *ex = bf_json_obj_get(e->doc, r, "exceptions");
        int nx = 0;
        for (const bf_json_node_t *x = bf_json_child_first(e->doc, ex); x; x = bf_json_child_next(e->doc, x)) {
            char d[400], rf[200]; sval(e->doc, x, "detail", d, sizeof d); sval(e->doc, x, "ref", rf, sizeof rf);
            if (!nx++) fprintf(o, "What %s left open:\n\n", !strcmp(e->kind, "schema-map") ? "the mapping" : "the rehearsal");
            fprintf(o, "- "); md_text(o, d);
            if (rf[0] && !strstr(d, rf)) { fprintf(o, " (`"); md_text(o, rf); fprintf(o, "`)"); }
            fprintf(o, "\n");
        }
        if (nx) fprintf(o, "\n");
    }
}

void brief_evidence_receipt(bf_jsonw *w) {
    bf_jsonw_key(w, "evidence_digest"); bf_jsonw_str(w, g_digest);
    bf_jsonw_key(w, "evidence"); bf_jsonw_arr(w);
    for (int i = 0; i < g_nev; i++) {
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "kind"); bf_jsonw_str(w, g_ev[i].kind);
        bf_jsonw_key(w, "file_sha256"); bf_jsonw_str(w, g_ev[i].file_sha);
        bf_jsonw_key(w, "digest"); bf_jsonw_str(w, g_ev[i].digest);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w);
}

void brief_evidence_free(void) {
    for (int i = 0; i < g_nev; i++) { bf_json_free(g_ev[i].doc); free(g_ev[i].buf); }
    g_nev = 0;
}
