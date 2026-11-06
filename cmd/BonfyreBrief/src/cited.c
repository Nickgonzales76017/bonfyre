/*
 * bonfyre-brief cited — a migration assessment brief built only from
 * receipt-bound source documents.
 *
 * Inputs, all read-only:
 *   - a brief type from data/migration-brief-types.json (constrained by the
 *     .cue beside it): which CMS source collections and document families
 *     the brief may read, what it looks for, its criteria, range and tasks;
 *   - the native CMS database: source_collection rows name source refs, and
 *     internet_source rows give each ref its family, twin and retained sha;
 *   - ForeignTwin receipts: a ref is usable only when a receipt for that twin
 *     records policy allow, HTTP 200 and the same sha, and the retained body
 *     still hashes to it;
 *   - optionally, a customer inventory for this exact subject and scope,
 *     carrying the grant it was supplied under;
 *   - optionally, DatabaseBridge transformation receipts (--evidence, see
 *     evidence.c): a sealed schema mapping and the diff measured over it.
 *
 * Output: brief.json (bf_jsonw), brief.md for the recipient, receipt.json.
 * Every excerpt is an exact byte range of a verified body. A source outside
 * the type's families is never read. Nothing is fetched, published, priced,
 * admitted or enqueued here.
 *
 * Exit: 0 written, 2 usage, 3 refused input, 4 stale sources digest,
 *       5 no usable source.
 */
#include <ctype.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_jsonw.h>
#include "evidence.h"

#define MAX_SOURCES 48
#define MAX_PER_SOURCE 8
#define MAX_EXCERPT 280
#define MAX_EXCEPTIONS 64

typedef struct { size_t off, len; int line; int truncated; } Seg;

typedef struct {
    char ns[64], collection[128], ref[256], family[96], label[256], url[1024], twin[128];
    char sha[65], receipt[1024], body_path[1024], observed_at[32], content_type[96];
    long long row_id;
    char *body; size_t body_len;
    Seg cites[MAX_PER_SOURCE]; int ncites;
    Seg license[4]; int nlicense;
} Source;

typedef struct { char kind[40]; char ref[256]; char detail[512]; } Exc;

static Source g_src[MAX_SOURCES];
static int g_nsrc;
static Exc g_exc[MAX_EXCEPTIONS];
static int g_nexc;
static int g_excluded_family;

static void add_exc(const char *kind, const char *ref, const char *detail) {
    if (g_nexc >= MAX_EXCEPTIONS) return;
    snprintf(g_exc[g_nexc].kind, sizeof g_exc[g_nexc].kind, "%s", kind);
    snprintf(g_exc[g_nexc].ref, sizeof g_exc[g_nexc].ref, "%s", ref ? ref : "");
    snprintf(g_exc[g_nexc].detail, sizeof g_exc[g_nexc].detail, "%s", detail ? detail : "");
    g_nexc++;
}

static int jstr(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k, char *out, size_t sz) {
    const bf_json_node_t *n = o ? bf_json_obj_get(d, o, k) : NULL;
    out[0] = 0;
    if (!n || n->type != BF_JSON_STRING) return 0;
    return bf_json_get_str_copy(n, out, sz) >= 0;
}

static int ci_contains(const char *hay, size_t len, const char *needle) {
    size_t n = strlen(needle);
    if (!n || n > len) return 0;
    for (size_t i = 0; i + n <= len; i++) {
        size_t j = 0;
        while (j < n && tolower((unsigned char)hay[i + j]) == tolower((unsigned char)needle[j])) j++;
        if (j == n) return 1;
    }
    return 0;
}

/* A fact name as the recipient would say it. */
static const char *fact_words(const char *f) {
    static const char *map[][2] = { {"site_count", "Sites"}, {"domain_list", "Domains"}, {"mailbox_count", "Mailboxes"},
        {"database_count", "Databases"}, {"subdomain_count", "Subdomains"}, {"source_panel", "Control panel"},
        {"dns_provider", "DNS"}, {"backup_location", "Backups"} };
    for (size_t i = 0; i < sizeof map / sizeof map[0]; i++) if (!strcmp(map[i][0], f)) return map[i][1];
    return "One answer";
}

/* With every need stated: "disputed" when any need is one the customer's answers
 * and exported listing disagree on, "read-from-exported-listing" when every need
 * came from an export, else "stated-by-customer-inventory". Never "verified". */
static const char *criterion_provenance(const bf_json_doc_t *td, const bf_json_node_t *c, const bf_json_doc_t *id,
                                        const bf_json_node_t *prov, const bf_json_node_t *dis) {
    int all_export = 1;
    for (const bf_json_node_t *nd = bf_json_child_first(td, bf_json_obj_get(td, c, "needs")); nd; nd = bf_json_child_next(td, nd)) {
        char need[64], from[64] = "";
        bf_json_get_str_copy(nd, need, sizeof need);
        for (const bf_json_node_t *d = dis ? bf_json_child_first(id, dis) : NULL; d; d = bf_json_child_next(id, d)) {
            char f[64] = ""; bf_json_get_str_copy(bf_json_obj_get(id, d, "fact"), f, sizeof f);
            if (!strcmp(f, need)) return "disputed";
        }
        bf_json_get_str_copy(bf_json_obj_get(id, prov, need), from, sizeof from);
        if (strncmp(from, "export:", 7)) all_export = 0;
    }
    return all_export ? "read-from-exported-listing" : "stated-by-customer-inventory";
}

static int subject_ok(const char *s) {
    size_t n = strlen(s);
    if (n < 3 || n > 127 || !strchr(s, ':')) return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)s[i]) && !strchr(":._-", s[i])) return 0;
    return 1;
}

static int sha_from_note(const char *note, char out[65]) {
    const char *p = note;
    while ((p = strstr(p, "sha256 ")) != NULL) {
        p += 7;
        int i = 0;
        while (i < 64 && isxdigit((unsigned char)p[i]) && !isupper((unsigned char)p[i])) i++;
        if (i == 64 && !isxdigit((unsigned char)p[64])) { memcpy(out, p, 64); out[64] = 0; return 1; }
    }
    return 0;
}

static int family_allowed(const bf_json_doc_t *d, const bf_json_node_t *fams, const char *family) {
    for (const bf_json_node_t *f = bf_json_child_first(d, fams); f; f = bf_json_child_next(d, f)) {
        char v[96];
        if (bf_json_get_str_copy(f, v, sizeof v) >= 0 && !strcmp(v, family)) return 1;
    }
    return 0;
}

/* Find the newest receipt for this twin whose sha equals the CMS sha and
 * whose retained body still hashes to it. */
static int resolve_receipt(Source *s, const char *dir) {
    DIR *dp = opendir(dir);
    if (!dp) { add_exc("receipts-unreadable", s->ref, dir); return 0; }
    struct dirent *e;
    size_t tl = strlen(s->twin);
    char best_at[32] = "";
    while ((e = readdir(dp)) != NULL) {
        size_t n = strlen(e->d_name);
        if (n <= tl + 6 || strncmp(e->d_name, s->twin, tl) || e->d_name[tl] != '-' || strcmp(e->d_name + n - 5, ".json")) continue;
        /* The twin prefix must not be the start of a longer twin id. */
        if (!isdigit((unsigned char)e->d_name[tl + 1])) continue;
        char path[2048];
        snprintf(path, sizeof path, "%s/%s", dir, e->d_name);
        size_t len = 0;
        char *buf = bf_read_file(path, &len);
        if (!buf) continue;
        char err[128];
        bf_json_doc_t *d = bf_json_parse(buf, len, err, sizeof err);
        if (d) {
            const bf_json_node_t *r = bf_json_root(d);
            char twin[128], sha[80], pol[32], at[32], body[1024], ctype[96] = "";
            const bf_json_node_t *st = bf_json_obj_get(d, r, "http_status");
            const bf_json_node_t *ev = bf_json_obj_get(d, r, "http_evidence");
            jstr(d, r, "twin_id", twin, sizeof twin); jstr(d, r, "content_sha256", sha, sizeof sha);
            jstr(d, r, "policy_decision", pol, sizeof pol); jstr(d, r, "observed_at", at, sizeof at);
            jstr(d, r, "body_path", body, sizeof body);
            if (ev) jstr(d, ev, "content_type", ctype, sizeof ctype);
            if (!strcmp(twin, s->twin) && !strcmp(sha, s->sha) && !strcmp(pol, "allow") && st &&
                st->type == BF_JSON_INT && bf_json_get_int(st) == 200 && strcmp(at, best_at) > 0) {
                snprintf(best_at, sizeof best_at, "%s", at);
                snprintf(s->receipt, sizeof s->receipt, "%s", path);
                snprintf(s->body_path, sizeof s->body_path, "%s", body);
                snprintf(s->observed_at, sizeof s->observed_at, "%s", at);
                snprintf(s->content_type, sizeof s->content_type, "%s", ctype);
            }
            bf_json_free(d);
        }
        free(buf);
    }
    closedir(dp);
    if (!best_at[0]) { add_exc("no-matching-receipt", s->ref, "no allowed HTTP 200 ForeignTwin receipt records this sha"); return 0; }
    char hex[65];
    if (bf_sha256_file(s->body_path, hex) != 0) { add_exc("body-missing", s->ref, "retained body named by the receipt cannot be read"); return 0; }
    if (strcmp(hex, s->sha)) { add_exc("body-changed", s->ref, "retained body no longer hashes to the recorded sha"); return 0; }
    s->body = bf_read_file(s->body_path, &s->body_len);
    if (!s->body) { add_exc("body-missing", s->ref, "retained body cannot be read"); return 0; }
    return 1;
}

static int is_html(const Source *s) {
    if (strstr(s->content_type, "html")) return 1;
    size_t i = 0;
    while (i < s->body_len && isspace((unsigned char)s->body[i])) i++;
    return i + 5 < s->body_len && (s->body[i] == '<') &&
           (ci_contains(s->body + i, s->body_len - i < 16 ? s->body_len - i : 16, "<!doctype") ||
            ci_contains(s->body + i, s->body_len - i < 16 ? s->body_len - i : 16, "<html"));
}

static int line_at(const char *b, size_t off) {
    int line = 1;
    for (size_t i = 0; i < off; i++) if (b[i] == '\n') line++;
    return line;
}

/* Trim an exact byte range; cap at MAX_EXCERPT on a UTF-8 boundary. */
static int make_seg(const Source *s, size_t a, size_t z, Seg *out) {
    while (a < z && isspace((unsigned char)s->body[a])) a++;
    while (z > a && isspace((unsigned char)s->body[z - 1])) z--;
    if (z - a < 12) return 0;
    out->truncated = 0;
    if (z - a > MAX_EXCERPT) {
        z = a + MAX_EXCERPT;
        while (z > a && ((unsigned char)s->body[z] & 0xC0) == 0x80) z--;
        out->truncated = 1;
    }
    out->off = a; out->len = z - a; out->line = line_at(s->body, a);
    return 1;
}

static int seg_dup(const Seg *list, int n, const char *b, const Seg *x) {
    for (int i = 0; i < n; i++)
        if (list[i].len == x->len && !memcmp(b + list[i].off, b + x->off, x->len)) return 1;
    return 0;
}

/* The words a person reads in a segment: link targets, tags and bare
 * addresses removed, so a term inside a URL or badge never selects a line. */
static size_t prose(const char *t, size_t len, char *out, size_t cap) {
    size_t o = 0;
    for (size_t i = 0; i < len && o + 1 < cap; i++) {
        if (t[i] == ']' && i + 1 < len && t[i + 1] == '(') { while (i < len && t[i] != ')') i++; continue; }
        if (t[i] == '<') { while (i < len && t[i] != '>') i++; continue; }
        if ((i == 0 || isspace((unsigned char)t[i - 1]) || t[i - 1] == '(') && len - i > 7 &&
            (!strncmp(t + i, "http://", 7) || !strncmp(t + i, "https://", 8))) {
            while (i < len && !isspace((unsigned char)t[i])) i++;
            continue;
        }
        out[o++] = t[i];
    }
    out[o] = 0;
    return o;
}

static int word_hit(const char *p, size_t len, const char *w) {
    size_t n = strlen(w);
    for (size_t i = 0; i + n <= len; i++) {
        if (i && isalnum((unsigned char)p[i - 1])) continue;
        size_t j = 0;
        while (j < n && tolower((unsigned char)p[i + j]) == tolower((unsigned char)w[j])) j++;
        if (j == n) return 1;
    }
    return 0;
}

static int markup_only(const char *t, size_t len) {
    static const char *lead[] = {"![", "[![", ".. ", ":target:", ":alt:", "<img", "<a ", "<p align", NULL};
    for (int i = 0; lead[i]; i++) if (len >= strlen(lead[i]) && !strncmp(t, lead[i], strlen(lead[i]))) return 1;
    return 0;
}

static void consider(Source *s, const bf_json_doc_t *d, const bf_json_node_t *sel, size_t a, size_t z) {
    Seg g;
    if (!make_seg(s, a, z, &g)) return;
    const char *t = s->body + g.off;
    if (markup_only(t, g.len)) return;
    char p[MAX_EXCERPT + 1];
    size_t pl = prose(t, g.len, p, sizeof p);
    int letters = 0;
    for (size_t i = 0; i < pl; i++) letters += isalpha((unsigned char)p[i]) != 0;
    if (letters < 16 || !strchr(p, ' ')) return;
    static const char *lic[] = {"AGPL", "LGPL", "GPL", "Apache License", "MIT License", "BSD", "license", NULL};
    for (int i = 0; lic[i] && s->nlicense < 4; i++)
        if (word_hit(p, pl, lic[i])) { if (!seg_dup(s->license, s->nlicense, s->body, &g)) s->license[s->nlicense++] = g; break; }
    if (s->ncites >= MAX_PER_SOURCE) return;
    for (const bf_json_node_t *k = bf_json_child_first(d, sel); k; k = bf_json_child_next(d, k)) {
        char w[40];
        if (bf_json_get_str_copy(k, w, sizeof w) < 0) continue;
        if (word_hit(p, pl, w)) {
            if (!seg_dup(s->cites, s->ncites, s->body, &g)) s->cites[s->ncites++] = g;
            return;
        }
    }
}

static void segment(Source *s, const bf_json_doc_t *d, const bf_json_node_t *sel) {
    const char *b = s->body;
    size_t n = s->body_len;
    if (!is_html(s)) {
        size_t a = 0;
        for (size_t i = 0; i <= n; i++) if (i == n || b[i] == '\n') { consider(s, d, sel, a, i); a = i + 1; }
        return;
    }
    /* HTML: text between tags, split at newlines. The document's own content
     * is the first <article> (a rendered README) or <main>, else the body;
     * head, navigation, scripts and styles are never read. */
    size_t i = 0;
    const char *art = strcasestr(b, "<article"), *end = NULL;
    if (art) end = strcasestr(art, "</article>");
    if (!art || !end) { art = strcasestr(b, "<main"); end = art ? strcasestr(art, "</main>") : NULL; }
    if (art && end) { i = (size_t)(art - b); n = (size_t)(end - b); }
    while (i < n) {
        if (b[i] == '<') {
            static const char *skip[] = {"head", "script", "style", "svg", "noscript", "nav", "header", "footer", "button", NULL};
            int skipped = 0;
            for (int k = 0; skip[k]; k++) {
                size_t kl = strlen(skip[k]);
                if (i + 1 + kl < n && !strncasecmp(b + i + 1, skip[k], kl) && !isalnum((unsigned char)b[i + 1 + kl])) {
                    char close[16]; snprintf(close, sizeof close, "</%s", skip[k]);
                    size_t j = i + 1;
                    while (j + strlen(close) <= n && strncasecmp(b + j, close, strlen(close))) j++;
                    i = j; skipped = 1; break;
                }
            }
            while (i < n && b[i] != '>') i++;
            i++;
            (void)skipped;
            continue;
        }
        size_t a = i;
        while (i < n && b[i] != '<') {
            if (b[i] == '\n') { consider(s, d, sel, a, i); a = i + 1; }
            i++;
        }
        consider(s, d, sel, a, i);
    }
}

static int cmp_src(const void *x, const void *y) { return strcmp(((const Source *)x)->ref, ((const Source *)y)->ref); }

static void w_seg(bf_jsonw *w, const Source *s, const Seg *g) {
    bf_jsonw_obj(w);
    bf_jsonw_key(w, "line"); bf_jsonw_int(w, g->line);
    bf_jsonw_key(w, "byte_offset"); bf_jsonw_int(w, (int64_t)g->off);
    bf_jsonw_key(w, "byte_length"); bf_jsonw_int(w, (int64_t)g->len);
    bf_jsonw_key(w, "excerpt"); bf_jsonw_strn(w, s->body + g->off, g->len);
    bf_jsonw_key(w, "truncated"); bf_jsonw_bool(w, g->truncated);
    bf_jsonw_obj_end(w);
}

/* For reading only: the JSON keeps exact bytes; the page decodes the few
 * entities HTML documentation uses so a person sees the characters. */
static void md_excerpt(FILE *o, const char *t, size_t len) {
    static const char *ent[][2] = {{"&lt;", "<"}, {"&gt;", ">"}, {"&amp;", "&"}, {"&quot;", "\""}, {"&#39;", "'"},
                                   {"&#x27;", "'"}, {"&mdash;", "\xe2\x80\x94"}, {"&nbsp;", " "}, {NULL, NULL}};
    for (size_t i = 0; i < len; i++) {
        int hit = 0;
        if (t[i] == '&')
            for (int k = 0; ent[k][0]; k++) {
                size_t el = strlen(ent[k][0]);
                if (i + el <= len && !memcmp(t + i, ent[k][0], el)) { fputs(ent[k][1], o); i += el - 1; hit = 1; break; }
            }
        if (!hit) fputc(t[i] == '\r' || t[i] == '\n' ? ' ' : t[i], o);
    }
}

static int write_all(const char *path, const char *data, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    int ok = fwrite(data, 1, len, f) == len;
    return fclose(f) == 0 && ok;
}

static void cited_usage(void) {
    fprintf(stderr,
        "usage: bonfyre-brief cited --type ID --types FILE --cms DB --receipts DIR --subject SUBJECT --scope SCOPE --out DIR\n"
        "                           [--inventory FILE] [--expect-sources-digest HEX] [--generated-at ISO]\n"
        "                           [--evidence mapping.json --evidence diff.json ...]\n");
}

int brief_cited_main(int argc, char **argv) {
    const char *type_id = NULL, *types = NULL, *cms = NULL, *receipts = NULL, *subject = NULL, *scope = NULL;
    const char *outdir = NULL, *inventory = NULL, *expect = NULL, *generated = NULL;
    const char *evidence[16]; int nevidence = 0;
    for (int i = 2; i < argc; i += 2) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!v) { cited_usage(); return 2; }
        if (!strcmp(a, "--type")) type_id = v; else if (!strcmp(a, "--types")) types = v;
        else if (!strcmp(a, "--cms")) cms = v; else if (!strcmp(a, "--receipts")) receipts = v;
        else if (!strcmp(a, "--subject")) subject = v; else if (!strcmp(a, "--scope")) scope = v;
        else if (!strcmp(a, "--out")) outdir = v; else if (!strcmp(a, "--inventory")) inventory = v;
        else if (!strcmp(a, "--expect-sources-digest")) expect = v; else if (!strcmp(a, "--generated-at")) generated = v;
        else if (!strcmp(a, "--evidence") && nevidence < 16) evidence[nevidence++] = v;
        else { cited_usage(); return 2; }
    }
    if (!type_id || !types || !cms || !receipts || !subject || !scope || !outdir) { cited_usage(); return 2; }
    if (!subject_ok(subject) || (strcmp(scope, "self") && strcmp(scope, "institution") && strcmp(scope, "household"))) {
        fprintf(stderr, "cited: subject or scope refused\n"); return 3;
    }

    size_t tlen = 0;
    char *tbuf = bf_read_file(types, &tlen), err[160];
    bf_json_doc_t *td = tbuf ? bf_json_parse(tbuf, tlen, err, sizeof err) : NULL;
    if (!td) { fprintf(stderr, "cited: cannot read brief types\n"); free(tbuf); return 2; }
    const bf_json_node_t *troot = bf_json_root(td), *ty = NULL;
    for (const bf_json_node_t *t = bf_json_child_first(td, bf_json_obj_get(td, troot, "types")); t; t = bf_json_child_next(td, t)) {
        char id[64];
        if (jstr(td, t, "id", id, sizeof id) && !strcmp(id, type_id)) { ty = t; break; }
    }
    if (!ty) { fprintf(stderr, "cited: unknown brief type %s\n", type_id); bf_json_free(td); free(tbuf); return 2; }
    const bf_json_node_t *fams = bf_json_obj_get(td, ty, "families"), *sel = bf_json_obj_get(td, ty, "selectors");

    /* Inventory: optional, but when given it must be this subject's and scope's,
     * and name the grant it was supplied under. */
    bf_json_doc_t *id_doc = NULL; const bf_json_node_t *facts = NULL, *iprov = NULL, *idis = NULL, *iunparsed = NULL;
    int idraft = 0;
    char *ibuf = NULL, inv_digest[65] = "none", grant[160] = "";
    if (inventory) {
        size_t il = 0;
        ibuf = bf_read_file(inventory, &il);
        id_doc = ibuf ? bf_json_parse(ibuf, il, err, sizeof err) : NULL;
        char isub[128], iscope[32];
        const bf_json_node_t *ir = id_doc ? bf_json_root(id_doc) : NULL;
        if (!ir || !jstr(id_doc, ir, "subject", isub, sizeof isub) || !jstr(id_doc, ir, "scope", iscope, sizeof iscope) ||
            strcmp(isub, subject) || strcmp(iscope, scope)) {
            fprintf(stderr, "cited: inventory is not for this subject and scope\n"); return 3;
        }
        if (!jstr(id_doc, ir, "grant_ref", grant, sizeof grant) || !grant[0]) { fprintf(stderr, "cited: inventory names no grant\n"); return 3; }
        facts = bf_json_obj_get(id_doc, ir, "facts");
        if (!facts || facts->type != BF_JSON_OBJECT) { fprintf(stderr, "cited: inventory has no facts object\n"); return 3; }
        bf_sha256_hex((const uint8_t *)ibuf, il, inv_digest);
        /* A draft built by `bonfyre-brief inventory` says where each fact came
         * from and where the customer's answers and their exported listing
         * disagree. Older inventories carry none of this and read as before. */
        iprov = bf_json_obj_get(id_doc, ir, "provenance");
        if (iprov && iprov->type != BF_JSON_OBJECT) iprov = NULL;
        idis = bf_json_obj_get(id_doc, ir, "disagreements");
        iunparsed = bf_json_obj_get(id_doc, ir, "unparsed_answers");
        idraft = bf_json_get_bool(bf_json_obj_get(id_doc, ir, "draft"));
    }

    if (nevidence && brief_evidence_load(evidence, nevidence, receipts, add_exc) < 0) return 3;

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(cms, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { fprintf(stderr, "cited: cannot open CMS\n"); return 2; }
    sqlite3_stmt *qc = NULL, *qs = NULL;
    if (sqlite3_prepare_v2(db, "SELECT source_selector FROM source_collection WHERE namespace=?1 AND collection_id=?2", -1, &qc, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT id,COALESCE(family,''),COALESCE(label,''),COALESCE(url,''),COALESCE(twin,''),COALESCE(state,''),COALESCE(note,'') "
                               "FROM internet_source WHERE namespace=?1 AND source=?2 ORDER BY id DESC LIMIT 1", -1, &qs, NULL) != SQLITE_OK) {
        fprintf(stderr, "cited: CMS lacks source tables\n"); sqlite3_close(db); return 2;
    }
    for (const bf_json_node_t *c = bf_json_child_first(td, bf_json_obj_get(td, ty, "collections")); c; c = bf_json_child_next(td, c)) {
        char cref[200], ns[64], coll[128];
        if (bf_json_get_str_copy(c, cref, sizeof cref) < 0) continue;
        char *slash = strchr(cref, '/');
        if (!slash) continue;
        *slash = 0; snprintf(ns, sizeof ns, "%s", cref); snprintf(coll, sizeof coll, "%s", slash + 1); *slash = '/';
        sqlite3_reset(qc);
        sqlite3_bind_text(qc, 1, ns, -1, SQLITE_TRANSIENT); sqlite3_bind_text(qc, 2, coll, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(qc) != SQLITE_ROW) { add_exc("collection-missing", cref, "source collection not present in this CMS"); continue; }
        const char *selj = (const char *)sqlite3_column_text(qc, 0);
        bf_json_doc_t *sd = selj ? bf_json_parse_str(selj, err, sizeof err) : NULL;
        const bf_json_node_t *refs = sd ? bf_json_obj_get(sd, bf_json_root(sd), "source_refs") : NULL;
        if (!refs) { add_exc("collection-unreadable", cref, "source_selector has no source_refs"); bf_json_free(sd); continue; }
        for (const bf_json_node_t *r = bf_json_child_first(sd, refs); r; r = bf_json_child_next(sd, r)) {
            char ref[256];
            if (bf_json_get_str_copy(r, ref, sizeof ref) < 0) continue;
            int seen = 0;
            for (int k = 0; k < g_nsrc; k++) if (!strcmp(g_src[k].ref, ref)) seen = 1;
            if (seen) continue;
            sqlite3_reset(qs);
            sqlite3_bind_text(qs, 1, ns, -1, SQLITE_TRANSIENT); sqlite3_bind_text(qs, 2, ref, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(qs) != SQLITE_ROW) { add_exc("source-row-missing", ref, cref); continue; }
            const char *family = (const char *)sqlite3_column_text(qs, 1);
            if (!family_allowed(td, fams, family)) { g_excluded_family++; continue; }
            if (g_nsrc >= MAX_SOURCES) { add_exc("source-limit", ref, "more sources than one brief reads"); continue; }
            Source *s = &g_src[g_nsrc];
            memset(s, 0, sizeof *s);
            s->row_id = sqlite3_column_int64(qs, 0);
            snprintf(s->ns, sizeof s->ns, "%s", ns); snprintf(s->collection, sizeof s->collection, "%s", coll);
            snprintf(s->ref, sizeof s->ref, "%s", ref); snprintf(s->family, sizeof s->family, "%s", family);
            snprintf(s->label, sizeof s->label, "%s", sqlite3_column_text(qs, 2));
            snprintf(s->url, sizeof s->url, "%s", sqlite3_column_text(qs, 3));
            snprintf(s->twin, sizeof s->twin, "%s", sqlite3_column_text(qs, 4));
            const char *state = (const char *)sqlite3_column_text(qs, 5);
            if (strcmp(state, "observed")) { add_exc("source-not-observed", ref, state); continue; }
            if (!sha_from_note((const char *)sqlite3_column_text(qs, 6), s->sha) || !s->twin[0]) {
                add_exc("source-unpinned", ref, "CMS row records no twin or retained sha256"); continue;
            }
            if (!resolve_receipt(s, receipts)) continue;
            segment(s, td, sel);
            g_nsrc++;
        }
        bf_json_free(sd);
    }
    sqlite3_finalize(qc); sqlite3_finalize(qs); sqlite3_close(db);
    if (!g_nsrc) { fprintf(stderr, "cited: no usable source for %s\n", type_id); return 5; }
    qsort(g_src, (size_t)g_nsrc, sizeof g_src[0], cmp_src);

    /* Sources digest: what a reviewer saw. A stale expectation is refused. */
    BfSha256 h; uint8_t hb[32]; char src_digest[65], brief_id[65];
    bf_sha256_init(&h);
    for (int i = 0; i < g_nsrc; i++) {
        bf_sha256_update(&h, (const uint8_t *)g_src[i].ref, strlen(g_src[i].ref)); bf_sha256_update(&h, (const uint8_t *)"\t", 1);
        bf_sha256_update(&h, (const uint8_t *)g_src[i].sha, 64); bf_sha256_update(&h, (const uint8_t *)"\n", 1);
    }
    bf_sha256_final(&h, hb); bf_sha256_digest_hex(hb, src_digest);
    if (expect && strcmp(expect, src_digest)) {
        printf("{\"stale\":true,\"expected\":\"%.64s\",\"current\":\"%s\"}\n", expect, src_digest);
        fprintf(stderr, "cited: sources changed since the reviewed revision\n"); return 4;
    }
    char idin[1024];
    snprintf(idin, sizeof idin, "%s\n%s\n%s\n%s\n%s", type_id, subject, scope, src_digest, inv_digest);
    /* Evidence joins the identity only when given, so earlier briefs keep their ids. */
    if (nevidence) { size_t il = strlen(idin); snprintf(idin + il, sizeof idin - il, "\n%s", brief_evidence_digest()); }
    bf_sha256_hex((const uint8_t *)idin, strlen(idin), brief_id);
    brief_id[24] = 0;

    char now[40];
    if (generated) snprintf(now, sizeof now, "%s", generated); else bf_iso_timestamp(now, sizeof now);
    char title[160], recipient[256], ptemplate[64], est_label[256];
    jstr(td, ty, "title", title, sizeof title); jstr(td, ty, "recipient", recipient, sizeof recipient);
    jstr(td, troot, "program_template", ptemplate, sizeof ptemplate); jstr(td, troot, "estimate_label", est_label, sizeof est_label);
    const bf_json_node_t *est = bf_json_obj_get(td, ty, "estimate");
    double lo = bf_json_get_double(bf_json_obj_get(td, est, "low")), hi = bf_json_get_double(bf_json_obj_get(td, est, "high"));
    char unit[64], scale_fact[64], basis[256], currency[8];
    jstr(td, est, "unit", unit, sizeof unit); jstr(td, est, "scale_fact", scale_fact, sizeof scale_fact);
    jstr(td, est, "basis", basis, sizeof basis); jstr(td, est, "currency", currency, sizeof currency);
    int64_t scale = 0;
    if (facts) {
        const bf_json_node_t *sf = bf_json_obj_get(id_doc, facts, scale_fact);
        if (sf && sf->type == BF_JSON_INT && bf_json_get_int(sf) > 0 && bf_json_get_int(sf) < 100000) scale = bf_json_get_int(sf);
    }

    for (int i = 0; i < g_nsrc; i++) {
        if (!g_src[i].ncites) add_exc("nothing-selected", g_src[i].ref, "no passage in this document matched the brief's terms");
        for (int k = 0; k < g_src[i].nlicense; k++) {
            const Seg *g = &g_src[i].license[k];
            const char *t = g_src[i].body + g->off;
            /* A LICENSE file spells the family out ("GNU Lesser General Public
             * License") rather than abbreviating it, so the phrase counts too. */
            if (word_hit(t, g->len, "GPL") || word_hit(t, g->len, "AGPL") || word_hit(t, g->len, "LGPL") ||
                word_hit(t, g->len, "General Public License")) { add_exc("license-review", g_src[i].ref, "the README text mentions a GPL-family license; check the project license before adopting any of its code"); break; }
        }
    }
    if (!inventory) add_exc("no-inventory", "", "no authorized customer inventory supplied; every criterion stays unassessed");
    if (idraft) add_exc("inventory-draft", "", "this inventory was put together from your answers and the account listings you exported; nobody has gone through it with you yet");
    for (const bf_json_node_t *d = idis ? bf_json_child_first(id_doc, idis) : NULL; d; d = bf_json_child_next(id_doc, d)) {
        char fact[64] = "", said[200] = "", shown[200] = "", detail[512];
        jstr(id_doc, d, "fact", fact, sizeof fact); jstr(id_doc, d, "intake", said, sizeof said); jstr(id_doc, d, "export", shown, sizeof shown);
        snprintf(detail, sizeof detail, "%s: you told us %s and your exported listing shows %s; the listing's figure is used until you confirm", fact_words(fact), said, shown);
        add_exc("inventory-disagreement", fact, detail);
    }
    for (const bf_json_node_t *u = iunparsed ? bf_json_child_first(id_doc, iunparsed) : NULL; u; u = bf_json_child_next(id_doc, u)) {
        char fact[64] = ""; bf_json_get_str_copy(u, fact, sizeof fact);
        char detail[200];
        snprintf(detail, sizeof detail, "%s: the answer was not a plain number, so it was left out of the inventory", fact_words(fact));
        add_exc("inventory-unparsed", fact, detail);
    }

    /* brief.json */
    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.migration-brief.v1");
    bf_jsonw_key(&w, "brief_id"); bf_jsonw_str(&w, brief_id);
    bf_jsonw_key(&w, "type"); bf_jsonw_str(&w, type_id);
    bf_jsonw_key(&w, "title"); bf_jsonw_str(&w, title);
    bf_jsonw_key(&w, "recipient"); bf_jsonw_str(&w, recipient);
    bf_jsonw_key(&w, "subject"); bf_jsonw_str(&w, subject);
    bf_jsonw_key(&w, "scope"); bf_jsonw_str(&w, scope);
    bf_jsonw_key(&w, "program_template"); bf_jsonw_str(&w, ptemplate);
    bf_jsonw_key(&w, "generated_at"); bf_jsonw_str(&w, now);
    bf_jsonw_key(&w, "sources_digest"); bf_jsonw_str(&w, src_digest);
    bf_jsonw_key(&w, "inventory_digest"); bf_jsonw_str(&w, inv_digest);
    bf_jsonw_key(&w, "inventory_grant"); if (grant[0]) bf_jsonw_str(&w, grant); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "sources"); bf_jsonw_arr(&w);
    for (int i = 0; i < g_nsrc; i++) {
        const Source *s = &g_src[i];
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "ref"); bf_jsonw_str(&w, s->ref);
        bf_jsonw_key(&w, "label"); bf_jsonw_str(&w, s->label);
        bf_jsonw_key(&w, "url"); bf_jsonw_str(&w, s->url);
        bf_jsonw_key(&w, "family"); bf_jsonw_str(&w, s->family);
        bf_jsonw_key(&w, "cms"); bf_jsonw_str(&w, s->ns);
        bf_jsonw_key(&w, "cms_row"); bf_jsonw_int(&w, s->row_id);
        bf_jsonw_key(&w, "collection"); bf_jsonw_str(&w, s->collection);
        bf_jsonw_key(&w, "content_sha256"); bf_jsonw_str(&w, s->sha);
        bf_jsonw_key(&w, "observed_at"); bf_jsonw_str(&w, s->observed_at);
        const char *rb = strrchr(s->receipt, '/');
        bf_jsonw_key(&w, "receipt"); bf_jsonw_str(&w, rb ? rb + 1 : s->receipt);
        bf_jsonw_key(&w, "citations"); bf_jsonw_arr(&w);
        for (int k = 0; k < s->ncites; k++) w_seg(&w, s, &s->cites[k]);
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "license_mentions"); bf_jsonw_arr(&w);
        for (int k = 0; k < s->nlicense; k++) w_seg(&w, s, &s->license[k]);
        bf_jsonw_arr_end(&w);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "excluded_outside_type"); bf_jsonw_int(&w, g_excluded_family);
    if (nevidence) { bf_jsonw_key(&w, "evidence_digest"); bf_jsonw_str(&w, brief_evidence_digest()); brief_evidence_json(&w); }
    bf_jsonw_key(&w, "criteria"); bf_jsonw_arr(&w);
    for (const bf_json_node_t *c = bf_json_child_first(td, bf_json_obj_get(td, ty, "criteria")); c; c = bf_json_child_next(td, c)) {
        char cid[64], text[512];
        jstr(td, c, "id", cid, sizeof cid); jstr(td, c, "text", text, sizeof text);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "id"); bf_jsonw_str(&w, cid);
        bf_jsonw_key(&w, "text"); bf_jsonw_str(&w, text);
        int missing = 0;
        bf_jsonw_key(&w, "stated"); bf_jsonw_obj(&w);
        for (const bf_json_node_t *nd = bf_json_child_first(td, bf_json_obj_get(td, c, "needs")); nd; nd = bf_json_child_next(td, nd)) {
            char need[64]; bf_json_get_str_copy(nd, need, sizeof need);
            const bf_json_node_t *fv = facts ? bf_json_obj_get(id_doc, facts, need) : NULL;
            if (!fv) { missing++; continue; }
            bf_jsonw_key(&w, need); bf_jsonw_node(&w, id_doc, fv, 1);
        }
        bf_jsonw_obj_end(&w);
        bf_jsonw_key(&w, "missing"); bf_jsonw_arr(&w);
        for (const bf_json_node_t *nd = bf_json_child_first(td, bf_json_obj_get(td, c, "needs")); nd; nd = bf_json_child_next(td, nd)) {
            char need[64]; bf_json_get_str_copy(nd, need, sizeof need);
            if (!facts || !bf_json_obj_get(id_doc, facts, need)) bf_jsonw_str(&w, need);
        }
        bf_jsonw_arr_end(&w);
        const char *status = !inventory ? "unassessed" : missing ? "needs-inventory" : "stated-by-customer-inventory";
        if (iprov && !missing) status = criterion_provenance(td, c, id_doc, iprov, idis);
        bf_jsonw_key(&w, "status"); bf_jsonw_str(&w, status);
        if (iprov) {
            bf_jsonw_key(&w, "provenance"); bf_jsonw_obj(&w);
            for (const bf_json_node_t *nd = bf_json_child_first(td, bf_json_obj_get(td, c, "needs")); nd; nd = bf_json_child_next(td, nd)) {
                char need[64], from[64]; bf_json_get_str_copy(nd, need, sizeof need);
                if (!jstr(id_doc, iprov, need, from, sizeof from)) continue;
                bf_jsonw_key(&w, need); bf_jsonw_str(&w, from);
            }
            bf_jsonw_obj_end(&w);
        }
        {
            char ek[32];
            if (nevidence && jstr(td, c, "evidence", ek, sizeof ek) && brief_evidence_has(ek)) {
                bf_jsonw_key(&w, "rehearsal_evidence"); brief_evidence_refs(&w, ek);
            }
        }
        bf_jsonw_key(&w, "verified"); bf_jsonw_bool(&w, 0);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "estimate"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "is_estimate"); bf_jsonw_bool(&w, 1);
    bf_jsonw_key(&w, "label"); bf_jsonw_str(&w, est_label);
    bf_jsonw_key(&w, "currency"); bf_jsonw_str(&w, currency);
    bf_jsonw_key(&w, "per_unit_low"); bf_jsonw_num(&w, lo);
    bf_jsonw_key(&w, "per_unit_high"); bf_jsonw_num(&w, hi);
    bf_jsonw_key(&w, "unit"); bf_jsonw_str(&w, unit);
    bf_jsonw_key(&w, "units_stated"); if (scale) bf_jsonw_int(&w, scale); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "total_low"); if (scale) bf_jsonw_num(&w, lo * (double)scale); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "total_high"); if (scale) bf_jsonw_num(&w, hi * (double)scale); else bf_jsonw_null(&w);
    bf_jsonw_key(&w, "basis"); bf_jsonw_str(&w, basis);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "exceptions"); bf_jsonw_arr(&w);
    for (int i = 0; i < g_nexc; i++) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, g_exc[i].kind);
        bf_jsonw_key(&w, "ref"); bf_jsonw_str(&w, g_exc[i].ref);
        bf_jsonw_key(&w, "detail"); bf_jsonw_str(&w, g_exc[i].detail);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "proposed_tasks"); bf_jsonw_arr(&w);
    int tn = 0;
    for (const bf_json_node_t *t = bf_json_child_first(td, bf_json_obj_get(td, ty, "tasks")); t; t = bf_json_child_next(td, t)) {
        char op[96], tt[256], node[16];
        jstr(td, t, "operator", op, sizeof op); jstr(td, t, "title", tt, sizeof tt);
        snprintf(node, sizeof node, "task-%02d", ++tn);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "node"); bf_jsonw_str(&w, node);
        bf_jsonw_key(&w, "operator"); bf_jsonw_str(&w, op);
        bf_jsonw_key(&w, "title"); bf_jsonw_str(&w, tt);
        bf_jsonw_key(&w, "admitted"); bf_jsonw_bool(&w, 0);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "cannot_infer"); bf_jsonw_node(&w, td, bf_json_obj_get(td, troot, "cannot_infer"), 0);
    bf_jsonw_obj_end(&w);

    if (bf_ensure_dir(outdir) != 0) { fprintf(stderr, "cited: cannot create %s\n", outdir); return 2; }
    char p_json[2048], p_md[2048], p_rcpt[2048];
    snprintf(p_json, sizeof p_json, "%s/brief.json", outdir);
    snprintf(p_md, sizeof p_md, "%s/brief.md", outdir);
    snprintf(p_rcpt, sizeof p_rcpt, "%s/receipt.json", outdir);
    if (w.oom || !write_all(p_json, w.buf, w.len)) { fprintf(stderr, "cited: cannot write brief.json\n"); return 2; }

    /* brief.md, for the recipient. */
    FILE *o = fopen(p_md, "wb");
    if (!o) { fprintf(stderr, "cited: cannot write brief.md\n"); return 2; }
    fprintf(o, "# %s\n\n", title);
    fprintf(o, "For %s.\n\n", recipient);
    fprintf(o, "Brief %s, written %s from %d source document%s. Every quotation below is copied exactly from a document we retained, and each one says where it came from.\n\n",
            brief_id, now, g_nsrc, g_nsrc == 1 ? "" : "s");
    fprintf(o, "## What we read\n\n");
    for (int i = 0; i < g_nsrc; i++)
        fprintf(o, "%d. %s, %s, read %s (sha256 `%.12s…`)\n", i + 1, g_src[i].label, g_src[i].url, g_src[i].observed_at, g_src[i].sha);
    fprintf(o, "\n## What the sources say\n\n");
    for (int i = 0; i < g_nsrc; i++) {
        const Source *s = &g_src[i];
        if (!s->ncites) continue;
        fprintf(o, "From %s:\n\n", s->label);
        for (int k = 0; k < s->ncites; k++) {
            fprintf(o, "- \"");
            md_excerpt(o, s->body + s->cites[k].off, s->cites[k].len);
            fprintf(o, "%s\" [%d, line %d]\n", s->cites[k].truncated ? "…" : "", i + 1, s->cites[k].line);
        }
        fprintf(o, "\n");
    }
    brief_evidence_md(o);
    fprintf(o, "## What has to be true before the move\n\n");
    for (const bf_json_node_t *c = bf_json_child_first(td, bf_json_obj_get(td, ty, "criteria")); c; c = bf_json_child_next(td, c)) {
        char text[512]; jstr(td, c, "text", text, sizeof text);
        int missing = 0, have = 0;
        for (const bf_json_node_t *nd = bf_json_child_first(td, bf_json_obj_get(td, c, "needs")); nd; nd = bf_json_child_next(td, nd)) {
            char need[64]; bf_json_get_str_copy(nd, need, sizeof need);
            if (facts && bf_json_obj_get(id_doc, facts, need)) have++; else missing++;
        }
        fprintf(o, "- %s ", text);
        if (!inventory) fprintf(o, "Not assessed yet: we have no inventory from you.");
        else if (missing) fprintf(o, "Your inventory covers %d of %d facts this needs; we still need the rest.", have, have + missing);
        else if (iprov) {
            const char *st = criterion_provenance(td, c, id_doc, iprov, idis);
            if (!strcmp(st, "disputed")) fprintf(o, "Your answers and your exported listing disagree on this; we settle it with you before anything moves.");
            else if (!strcmp(st, "read-from-exported-listing")) fprintf(o, "Your exported account listing states what this needs. Nobody has checked it against the live server yet.");
            else fprintf(o, "Your inventory states what this needs, partly from your answers and partly from your exported listing. Nobody has checked it against the live server yet.");
        }
        else fprintf(o, "Your inventory states what this needs. Nobody has checked it against the system yet.");
        {
            char ek[32];
            if (nevidence && jstr(td, c, "evidence", ek, sizeof ek) && brief_evidence_has(ek))
                fprintf(o, " The rehearsal above measured this on the copy it names, not on your system.");
        }
        fputc('\n', o);
    }
    fprintf(o, "\n## Proposed range\n\n");
    if (scale) fprintf(o, "%s %.0f to %.0f for %lld %s%s (%.0f to %.0f per %s). %s\n\n", currency, lo * (double)scale, hi * (double)scale,
                       (long long)scale, unit, scale == 1 ? "" : "s", lo, hi, unit, est_label);
    else fprintf(o, "%s %.0f to %.0f per %s. %s\n\n", currency, lo, hi, unit, est_label);
    fprintf(o, "The range covers %s.\n\n", basis);
    fprintf(o, "## What is missing or needs review\n\n");
    if (!g_nexc) fprintf(o, "Nothing recorded.\n");
    for (int i = 0; i < g_nexc; i++) {
        /* Inventory exceptions name their fact in words; the field name stays in brief.json. */
        int words = !strncmp(g_exc[i].kind, "inventory-", 10);
        fprintf(o, "- %s%s%s\n", g_exc[i].detail, g_exc[i].ref[0] && !words ? ": " : "", words ? "" : g_exc[i].ref);
    }
    fprintf(o, "\n## Next steps we would take\n\nNone of these has started. Each one waits for your go-ahead and for access you grant.\n\n");
    tn = 0;
    for (const bf_json_node_t *t = bf_json_child_first(td, bf_json_obj_get(td, ty, "tasks")); t; t = bf_json_child_next(td, t)) {
        char tt[256]; jstr(td, t, "title", tt, sizeof tt);
        fprintf(o, "%d. %s\n", ++tn, tt);
    }
    fprintf(o, "\n## What this brief cannot tell you\n\n");
    for (const bf_json_node_t *ci = bf_json_child_first(td, bf_json_obj_get(td, troot, "cannot_infer")); ci; ci = bf_json_child_next(td, ci)) {
        char line[512]; bf_json_get_str_copy(ci, line, sizeof line);
        char *arrow = strstr(line, " -> ");
        if (arrow) { *arrow = 0; line[0] = (char)toupper((unsigned char)line[0]); fprintf(o, "- %s does not show %s.\n", line, arrow + 4); }
        else fprintf(o, "- %s\n", line);
    }
    if (fclose(o) != 0) return 2;

    char d_json[65], d_md[65], d_types[65];
    bf_sha256_file(p_json, d_json); bf_sha256_file(p_md, d_md); bf_sha256_hex((const uint8_t *)tbuf, tlen, d_types);
    bf_jsonw r; bf_jsonw_init(&r, 2);
    bf_jsonw_obj(&r);
    bf_jsonw_key(&r, "schema"); bf_jsonw_str(&r, "bonfyre.migration-brief.receipt.v1");
    bf_jsonw_key(&r, "owner"); bf_jsonw_str(&r, "BonfyreBrief cited");
    bf_jsonw_key(&r, "brief_id"); bf_jsonw_str(&r, brief_id);
    bf_jsonw_key(&r, "subject"); bf_jsonw_str(&r, subject);
    bf_jsonw_key(&r, "scope"); bf_jsonw_str(&r, scope);
    bf_jsonw_key(&r, "generated_at"); bf_jsonw_str(&r, now);
    bf_jsonw_key(&r, "types_sha256"); bf_jsonw_str(&r, d_types);
    bf_jsonw_key(&r, "sources_digest"); bf_jsonw_str(&r, src_digest);
    bf_jsonw_key(&r, "inventory_digest"); bf_jsonw_str(&r, inv_digest);
    bf_jsonw_key(&r, "brief_json_sha256"); bf_jsonw_str(&r, d_json);
    bf_jsonw_key(&r, "brief_md_sha256"); bf_jsonw_str(&r, d_md);
    if (nevidence) brief_evidence_receipt(&r);
    bf_jsonw_key(&r, "effects"); bf_jsonw_obj(&r);
    bf_jsonw_key(&r, "network"); bf_jsonw_bool(&r, 0);
    bf_jsonw_key(&r, "publication"); bf_jsonw_bool(&r, 0);
    bf_jsonw_key(&r, "spend"); bf_jsonw_bool(&r, 0);
    bf_jsonw_key(&r, "work_admitted"); bf_jsonw_bool(&r, 0);
    bf_jsonw_obj_end(&r);
    bf_jsonw_obj_end(&r);
    if (r.oom || !write_all(p_rcpt, r.buf, r.len)) return 2;
    printf("{\"brief_id\":\"%s\",\"sources\":%d,\"sources_digest\":\"%s\",\"brief\":\"%s\"}\n", brief_id, g_nsrc, src_digest, p_md);
    bf_jsonw_free(&w); bf_jsonw_free(&r);
    bf_json_free(td); free(tbuf);
    if (id_doc) bf_json_free(id_doc);
    free(ibuf);
    brief_evidence_free();
    for (int i = 0; i < g_nsrc; i++) free(g_src[i].body);
    return 0;
}
