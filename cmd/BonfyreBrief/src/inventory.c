/*
 * bonfyre-brief inventory — a draft hosting-account inventory for
 * `bonfyre-brief cited --inventory`, assembled only from files a person
 * handed over:
 *
 *   - the practice site's intake: `bonfyre-site site inbox NAME --json`
 *     (bonfyre.site.inbox.v1); the request named by --reference gives the
 *     answers its owner typed, keyed by the brief type's fact names;
 *   - exported account listings (--export PANEL:FILE, read-only adapters):
 *       cpanel     WHM API 1 listaccts; cPanel API 2 AddonDomain::listaddondomains,
 *                  Email::listpops, MysqlFE::listdbs (JSON as documented)
 *       interworx  NodeWorx Siteworx listAccounts / listAllDomains, SiteWorx
 *                  EmailBox listEmailBoxes, MysqlDb listMysqlDatabases, as the
 *                  CLI's `-o json` or the API's {status,payload,reply_code}
 *       plesk      XML API <site><get> response packet (gen_info dataset)
 *
 * Nothing here connects to a panel, server or customer system: every input
 * is a local file. Only counts and domain names leave an export; credential
 * fields (crypt_password, ftp_password and anything else) are never copied,
 * and the number of such fields seen is recorded. A panel's limits
 * (maxpop, maxsql) are never read as counts.
 *
 * An exported listing is read only under the owner's authorization, so
 * --export requires --grant. Intake alone carries a customer-statement grant
 * naming the request. Where intake and an export state the same fact, the
 * export's value is used and the disagreement is recorded; `cited` turns it
 * into a named exception and never marks a criterion verified.
 *
 * Exit: 0 written, 2 usage, 3 refused input.
 */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_jsonw.h>

#define MAX_EXPORTS 32

typedef struct { char **v; int n, cap; } StrSet;

static void set_add(StrSet *s, const char *x) {
    for (int i = 0; i < s->n; i++) if (!strcmp(s->v[i], x)) return;
    if (s->n == s->cap) { s->cap = s->cap ? s->cap * 2 : 16; s->v = realloc(s->v, (size_t)s->cap * sizeof *s->v); }
    s->v[s->n++] = strdup(x);
}
static int set_has(const StrSet *s, const char *x) { for (int i = 0; i < s->n; i++) if (!strcmp(s->v[i], x)) return 1; return 0; }
static int cmp_str(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
static void set_sort(StrSet *s) { if (s->n) qsort(s->v, (size_t)s->n, sizeof *s->v, cmp_str); }
static void set_free(StrSet *s) { for (int i = 0; i < s->n; i++) free(s->v[i]); free(s->v); memset(s, 0, sizeof *s); }

/* A domain as a panel or a person wrote it: lowercased, trailing dot gone,
 * and refused unless it is a plain hostname. */
static int norm_domain(const char *in, size_t len, char out[256]) {
    while (len && isspace((unsigned char)*in)) { in++; len--; }
    while (len && (isspace((unsigned char)in[len - 1]) || in[len - 1] == '.')) len--;
    if (!len || len > 253) return 0;
    int dot = 0;
    for (size_t i = 0; i < len; i++) {
        char c = (char)tolower((unsigned char)in[i]);
        if (!isalnum((unsigned char)c) && c != '.' && c != '-') return 0;
        if (c == '.') dot = 1;
        out[i] = c;
    }
    out[len] = 0;
    return dot;
}

static int norm_mailbox(const char *in, size_t len, char out[320]) {
    const char *at = memchr(in, '@', len);
    if (!at || at == in || len >= 320) return 0;   /* cPanel's "Main Account" row has no address */
    char dom[256];
    if (!norm_domain(at + 1, len - (size_t)(at + 1 - in), dom)) return 0;
    size_t ll = (size_t)(at - in);
    for (size_t i = 0; i < ll; i++) {
        if (isspace((unsigned char)in[i]) || in[i] == '"') return 0;
        out[i] = (char)tolower((unsigned char)in[i]);
    }
    snprintf(out + ll, 320 - ll, "@%s", dom);
    return 1;
}

/* String value of a JSON node, unescaped. Caller frees. */
static char *jtext(const bf_json_node_t *n) {
    if (!n || n->type != BF_JSON_STRING) return NULL;
    size_t ol = 0;
    return bf_json_unescape(n->val.str.ptr, n->val.str.len, &ol);
}
static char *jget(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k) { return jtext(bf_json_obj_get(d, o, k)); }

typedef struct {
    StrSet sites, domains, subdomains, mailboxes, databases;
    int secret_fields;
} Account;

/* Fields whose names say they hold a secret; seen, counted, never copied. */
static void count_secrets(const bf_json_doc_t *d, const bf_json_node_t *n, int *count) {
    if (!n) return;
    for (const bf_json_node_t *c = bf_json_child_first(d, n); c; c = bf_json_child_next(d, c)) {
        if (c->key) {
            char k[64]; size_t kl = (size_t)c->key_len < sizeof k - 1 ? (size_t)c->key_len : sizeof k - 1;
            for (size_t i = 0; i < kl; i++) k[i] = (char)tolower((unsigned char)c->key[i]);
            k[kl] = 0;
            if (strstr(k, "pass") || strstr(k, "secret") || strstr(k, "token") || strstr(k, "crypt")) (*count)++;
        }
        if (c->type == BF_JSON_OBJECT || c->type == BF_JSON_ARRAY) count_secrets(d, c, count);
    }
}

typedef struct { char panel[16], path[1024], listing[48], sha[65]; size_t bytes; int items; } Input;

static int add_domain_node(Account *a, StrSet *into, const bf_json_doc_t *d, const bf_json_node_t *o, const char *key) {
    char *v = jget(d, o, key), nd[256];
    int ok = v && norm_domain(v, strlen(v), nd);
    if (ok) { set_add(into, nd); if (into != &a->domains) set_add(&a->domains, nd); }
    free(v);
    return ok;
}

/* cPanel: WHM API 1 {data:{acct:[]},metadata:{result}} or API 2 {cpanelresult:{func,data,event:{result}}}. */
static int read_cpanel(Account *a, Input *in, const bf_json_doc_t *d, char *why, size_t wn) {
    const bf_json_node_t *root = bf_json_root(d), *meta = bf_json_obj_get(d, root, "metadata"), *cr = bf_json_obj_get(d, root, "cpanelresult");
    if (meta) {
        char *cmd = jget(d, meta, "command");
        int ok = cmd && !strcmp(cmd, "listaccts");
        free(cmd);
        if (!ok) { snprintf(why, wn, "WHM API 1 output of a call other than listaccts"); return 0; }
        if (bf_json_get_int(bf_json_obj_get(d, meta, "result")) != 1) { snprintf(why, wn, "WHM reported listaccts as failed"); return 0; }
        const bf_json_node_t *acct = bf_json_dotpath(d, "data.acct");
        if (!acct || acct->type != BF_JSON_ARRAY) { snprintf(why, wn, "listaccts has no data.acct array"); return 0; }
        snprintf(in->listing, sizeof in->listing, "whm-listaccts");
        for (const bf_json_node_t *x = bf_json_child_first(d, acct); x; x = bf_json_child_next(d, x))
            in->items += add_domain_node(a, &a->sites, d, x, "domain");
        return 1;
    }
    if (!cr) { snprintf(why, wn, "not a WHM API 1 or cPanel API 2 response"); return 0; }
    const bf_json_node_t *ev = bf_json_obj_get(d, cr, "event"), *data = bf_json_obj_get(d, cr, "data");
    if (ev && bf_json_get_int(bf_json_obj_get(d, ev, "result")) != 1) { snprintf(why, wn, "cPanel reported the call as failed"); return 0; }
    if (!data || data->type != BF_JSON_ARRAY) { snprintf(why, wn, "cPanel API 2 response has no data array"); return 0; }
    char *func = jget(d, cr, "func");
    const bf_json_node_t *first = bf_json_child_first(d, data);
    /* MysqlFE::listdbs is documented without func; its rows carry db and usercount. */
    if (!func && first && bf_json_obj_get(d, first, "db") && bf_json_obj_get(d, first, "usercount")) func = strdup("listdbs");
    int ok = 1;
    if (func && !strcmp(func, "listaddondomains")) {
        snprintf(in->listing, sizeof in->listing, "cpanel-api2-listaddondomains");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) in->items += add_domain_node(a, &a->sites, d, x, "domain");
    } else if (func && !strcmp(func, "listpops")) {
        snprintf(in->listing, sizeof in->listing, "cpanel-api2-listpops");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) {
            char *e = jget(d, x, "email"), mb[320];
            if (e && norm_mailbox(e, strlen(e), mb)) { set_add(&a->mailboxes, mb); in->items++; }
            free(e);
        }
    } else if (func && !strcmp(func, "listdbs")) {
        snprintf(in->listing, sizeof in->listing, "cpanel-api2-mysqlfe-listdbs");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) {
            char *n = jget(d, x, "db");
            if (n && *n) { set_add(&a->databases, n); in->items++; }
            free(n);
        }
    } else { snprintf(why, wn, "cPanel API 2 function %s is not one this adapter reads", func ? func : "(unnamed)"); ok = 0; }
    free(func);
    return ok;
}

/* InterWorx: {status:0,payload:[...],reply_code} from the API, or the bare
 * payload array the CLI prints with -o json. The listing is told by its rows. */
static int read_interworx(Account *a, Input *in, const bf_json_doc_t *d, char *why, size_t wn) {
    const bf_json_node_t *root = bf_json_root(d), *payload = root;
    if (root->type == BF_JSON_OBJECT) {
        const bf_json_node_t *st = bf_json_obj_get(d, root, "status");
        payload = bf_json_obj_get(d, root, "payload");
        if (!st || !payload) { snprintf(why, wn, "not an InterWorx API reply"); return 0; }
        if (bf_json_get_int(st) != 0) { snprintf(why, wn, "InterWorx reported the action as failed"); return 0; }
    }
    if (!payload || payload->type != BF_JSON_ARRAY) { snprintf(why, wn, "InterWorx payload is not a list"); return 0; }
    const bf_json_node_t *first = bf_json_child_first(d, payload);
    if (!first) { snprintf(why, wn, "InterWorx payload is empty, so its listing cannot be told"); return 0; }
    if (bf_json_obj_get(d, first, "siteworx_id") || bf_json_obj_get(d, first, "uniqname")) {
        snprintf(in->listing, sizeof in->listing, "interworx-siteworx-listAccounts");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) in->items += add_domain_node(a, &a->sites, d, x, "domain");
    } else if (bf_json_obj_get(d, first, "domain_type") && bf_json_obj_get(d, first, "master_domain")) {
        snprintf(in->listing, sizeof in->listing, "interworx-siteworx-listAllDomains");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) {
            char *t = jget(d, x, "domain_type");
            StrSet *into = !t ? &a->domains : !strcmp(t, "master") || !strcmp(t, "secondary") ? &a->sites
                         : !strcmp(t, "subdomain") ? &a->subdomains : &a->domains;
            char *v = jget(d, x, "domain"), nd[256];
            if (v && norm_domain(v, strlen(v), nd)) {
                set_add(into, nd);
                if (into == &a->sites) set_add(&a->domains, nd);
                in->items++;
            }
            free(v); free(t);
        }
    } else if (bf_json_obj_get(d, first, "username") && bf_json_obj_get(d, first, "email")) {
        snprintf(in->listing, sizeof in->listing, "interworx-email-box-listEmailBoxes");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) {
            char *e = jget(d, x, "email"), mb[320];
            if (e && norm_mailbox(e, strlen(e), mb)) { set_add(&a->mailboxes, mb); in->items++; }
            free(e);
        }
    } else if (bf_json_obj_get(d, first, "fqdn") && bf_json_obj_get(d, first, "host")) {
        snprintf(in->listing, sizeof in->listing, "interworx-mysql-db-listMysqlDatabases");
        for (const bf_json_node_t *x = first; x; x = bf_json_child_next(d, x)) {
            char *n = jget(d, x, "fqdn");
            if (n && *n) { set_add(&a->databases, n); in->items++; }
            free(n);
        }
    } else { snprintf(why, wn, "InterWorx rows match no listing this adapter reads"); return 0; }
    return 1;
}

/* Text of the first <tag>…</tag> inside [p, end), trimmed. */
static int xml_text(const char *p, const char *end, const char *tag, char *out, size_t on) {
    char open[48], close[48];
    snprintf(open, sizeof open, "<%s>", tag); snprintf(close, sizeof close, "</%s>", tag);
    size_t ol = strlen(open);
    for (const char *s = p; s + ol <= end; s++) {
        if (memcmp(s, open, ol)) continue;
        const char *b = s + ol, *e = b;
        while (e + strlen(close) <= end && memcmp(e, close, strlen(close))) e++;
        if (e + strlen(close) > end) return 0;
        while (b < e && isspace((unsigned char)*b)) b++;
        while (e > b && isspace((unsigned char)e[-1])) e--;
        size_t n = (size_t)(e - b) < on - 1 ? (size_t)(e - b) : on - 1;
        memcpy(out, b, n); out[n] = 0;
        return 1;
    }
    return 0;
}
static const char *find_in(const char *p, const char *end, const char *needle) {
    size_t nl = strlen(needle);
    for (const char *s = p; s + nl <= end; s++) if (!memcmp(s, needle, nl)) return s;
    return NULL;
}

/* Plesk XML API: <packet><site><get><result>… with the gen_info dataset. */
static int read_plesk(Account *a, Input *in, const char *buf, size_t len, int *errors, char *why, size_t wn) {
    const char *end = buf + len;
    if (!find_in(buf, end, "<packet") || !find_in(buf, end, "<site>") || !find_in(buf, end, "<get>")) {
        snprintf(why, wn, "not a Plesk XML API site get response"); return 0;
    }
    snprintf(in->listing, sizeof in->listing, "plesk-xml-site-get");
    for (const char *p = buf; (p = find_in(p, end, "<result>")); ) {
        const char *re = find_in(p, end, "</result>");
        if (!re) { snprintf(why, wn, "unterminated <result> in the Plesk packet"); return 0; }
        char status[16] = "", ascii[256] = "", htype[32] = "";
        xml_text(p, re, "status", status, sizeof status);
        if (strcmp(status, "ok")) { (*errors)++; p = re; continue; }
        const char *gi = find_in(p, re, "<gen_info>"), *ge = gi ? find_in(gi, re, "</gen_info>") : NULL;
        if (!gi || !ge) { (*errors)++; p = re; continue; }
        if (!xml_text(gi, ge, "ascii-name", ascii, sizeof ascii) || !ascii[0]) xml_text(gi, ge, "name", ascii, sizeof ascii);
        xml_text(gi, ge, "htype", htype, sizeof htype);
        /* Secret-bearing hosting properties: counted, never copied. */
        for (const char *q = p; (q = find_in(q, re, "<name>")) && q < re; q++) {
            char pn[64] = ""; xml_text(q, re, "name", pn, sizeof pn);
            if (strstr(pn, "password")) a->secret_fields++;
        }
        char nd[256];
        if (norm_domain(ascii, strlen(ascii), nd)) {
            set_add(&a->domains, nd);
            if (!strcmp(htype, "vrt_hst")) set_add(&a->sites, nd);
            in->items++;
        }
        p = re;
    }
    return 1;
}

typedef struct { char key[64]; char *text; long long num; int is_num; } Ans;
static const Ans *find_ans(const Ans *a, int n, const char *k) { for (int i = 0; i < n; i++) if (!strcmp(a[i].key, k)) return &a[i]; return NULL; }

static int all_digits(const char *s) { if (!*s) return 0; for (; *s; s++) if (!isdigit((unsigned char)*s)) return 0; return 1; }
static int is_count_fact(const char *k) { size_t n = strlen(k); return n > 6 && !strcmp(k + n - 6, "_count"); }

static void usage(void) {
    fprintf(stderr,
        "usage: bonfyre-brief inventory --subject SUBJECT --scope SCOPE --out FILE\n"
        "                               [--intake INBOX.json --reference REF]\n"
        "                               [--export cpanel|interworx|plesk:FILE ... --grant REF]\n"
        "  Builds a draft inventory for `bonfyre-brief cited --inventory` from local files only.\n");
}

int brief_inventory_main(int argc, char **argv) {
    const char *subject = NULL, *scope = NULL, *out = NULL, *intake = NULL, *reference = NULL, *grant = NULL;
    const char *exports[MAX_EXPORTS]; int nexp = 0;
    for (int i = 2; i < argc; i += 2) {
        const char *k = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!v) { usage(); return 2; }
        if (!strcmp(k, "--subject")) subject = v; else if (!strcmp(k, "--scope")) scope = v;
        else if (!strcmp(k, "--out")) out = v; else if (!strcmp(k, "--intake")) intake = v;
        else if (!strcmp(k, "--reference")) reference = v; else if (!strcmp(k, "--grant")) grant = v;
        else if (!strcmp(k, "--export") && nexp < MAX_EXPORTS) exports[nexp++] = v;
        else { usage(); return 2; }
    }
    if (!subject || !scope || !out || (!intake && !nexp) || (intake && !reference)) { usage(); return 2; }
    if (strcmp(scope, "self") && strcmp(scope, "institution") && strcmp(scope, "household")) { fprintf(stderr, "inventory: scope refused\n"); return 3; }
    if (nexp && (!grant || !*grant)) {
        fprintf(stderr, "inventory: an exported listing is read only under the owner's authorization; name it with --grant\n"); return 3;
    }

    /* Intake answers. */
    bf_json_doc_t *idoc = NULL; char *ibuf = NULL; const bf_json_node_t *msg = NULL, *answers = NULL;
    char err[160], intake_sha[65] = "";
    if (intake) {
        size_t il = 0;
        ibuf = bf_read_file(intake, &il);
        idoc = ibuf ? bf_json_parse(ibuf, il, err, sizeof err) : NULL;
        char *schema = idoc ? jget(idoc, bf_json_root(idoc), "schema") : NULL;
        int ok = schema && !strcmp(schema, "bonfyre.site.inbox.v1");
        free(schema);
        if (!ok) { fprintf(stderr, "inventory: %s is not a site inbox (bonfyre.site.inbox.v1)\n", intake); return 3; }
        bf_sha256_hex((const uint8_t *)ibuf, il, intake_sha);
        for (const bf_json_node_t *m = bf_json_child_first(idoc, bf_json_obj_get(idoc, bf_json_root(idoc), "messages")); m; m = bf_json_child_next(idoc, m)) {
            char *r = jget(idoc, m, "reference");
            int hit = r && !strcmp(r, reference);
            free(r);
            if (hit) { msg = m; break; }
        }
        if (!msg) { fprintf(stderr, "inventory: no request %s in the inbox\n", reference); return 3; }
        answers = bf_json_obj_get(idoc, msg, "answers");
        if (!answers || answers->type != BF_JSON_OBJECT) { fprintf(stderr, "inventory: request %s carries no answers\n", reference); return 3; }
    }

    /* Exports, one panel at a time; every file must be read or the run is refused. */
    Account acct; memset(&acct, 0, sizeof acct);
    Input inputs[MAX_EXPORTS]; memset(inputs, 0, sizeof inputs);
    StrSet panels = {0};
    int plesk_errors = 0, has[5] = {0}; /* sites, domains, mailboxes, databases, subdomains */
    for (int i = 0; i < nexp; i++) {
        Input *in = &inputs[i];
        const char *colon = strchr(exports[i], ':');
        if (!colon || colon == exports[i] || (size_t)(colon - exports[i]) >= sizeof in->panel) { usage(); return 2; }
        memcpy(in->panel, exports[i], (size_t)(colon - exports[i])); in->panel[colon - exports[i]] = 0;
        snprintf(in->path, sizeof in->path, "%s", colon + 1);
        if (strcmp(in->panel, "cpanel") && strcmp(in->panel, "interworx") && strcmp(in->panel, "plesk")) {
            fprintf(stderr, "inventory: no adapter for panel %s\n", in->panel); return 2;
        }
        size_t len = 0;
        char *buf = bf_read_file(in->path, &len), why[200] = "";
        if (!buf) { fprintf(stderr, "inventory: cannot read %s\n", in->path); return 2; }
        in->bytes = len;
        bf_sha256_hex((const uint8_t *)buf, len, in->sha);
        int ok;
        if (!strcmp(in->panel, "plesk")) ok = read_plesk(&acct, in, buf, len, &plesk_errors, why, sizeof why);
        else {
            bf_json_doc_t *d = bf_json_parse(buf, len, err, sizeof err);
            if (!d) { fprintf(stderr, "inventory: %s is not JSON\n", in->path); free(buf); return 3; }
            ok = !strcmp(in->panel, "cpanel") ? read_cpanel(&acct, in, d, why, sizeof why) : read_interworx(&acct, in, d, why, sizeof why);
            if (ok) count_secrets(d, bf_json_root(d), &acct.secret_fields);
            bf_json_free(d);
        }
        free(buf);
        if (!ok) { fprintf(stderr, "inventory: %s refused: %s\n", in->path, why); return 3; }
        set_add(&panels, in->panel);
        /* Which facts this listing can speak to, even when it lists none. */
        if (strstr(in->listing, "listaccts") || strstr(in->listing, "listAccounts") || strstr(in->listing, "listAllDomains") ||
            strstr(in->listing, "listaddondomains") || strstr(in->listing, "site-get")) has[0] = has[1] = 1;
        if (strstr(in->listing, "listAllDomains")) has[4] = 1;
        if (strstr(in->listing, "pops") || strstr(in->listing, "EmailBoxes")) has[2] = 1;
        if (strstr(in->listing, "listdbs") || strstr(in->listing, "MysqlDatabases")) has[3] = 1;
    }
    set_sort(&acct.sites); set_sort(&acct.domains); set_sort(&acct.mailboxes); set_sort(&acct.databases); set_sort(&acct.subdomains); set_sort(&panels);
    char export_src[64] = "";
    if (panels.n) {
        snprintf(export_src, sizeof export_src, "export:");
        for (int i = 0; i < panels.n; i++) { size_t l = strlen(export_src); snprintf(export_src + l, sizeof export_src - l, "%s%s", i ? "+" : "", panels.v[i]); }
    }

    /* Intake values, normalised: counts become integers when they are plain digits. */
    Ans ans[48]; int nans = 0;
    StrSet intake_domains = {0}, unparsed = {0};
    for (const bf_json_node_t *c = answers ? bf_json_child_first(idoc, answers) : NULL; c && nans < 48; c = bf_json_child_next(idoc, c)) {
        Ans *x = &ans[nans];
        size_t kl = (size_t)c->key_len < sizeof x->key - 1 ? (size_t)c->key_len : sizeof x->key - 1;
        memcpy(x->key, c->key, kl); x->key[kl] = 0;
        for (size_t i = 0; i < kl; i++) if (!islower((unsigned char)x->key[i]) && !isdigit((unsigned char)x->key[i]) && x->key[i] != '_') { x->key[0] = 0; break; }
        if (!x->key[0]) continue;
        x->text = jtext(c);
        if (!x->text) { if (c->type == BF_JSON_INT) { x->num = bf_json_get_int(c); x->is_num = 1; x->text = strdup(""); } else continue; }
        char *t = x->text; while (*t && isspace((unsigned char)*t)) t++;
        memmove(x->text, t, strlen(t) + 1);
        for (size_t l = strlen(x->text); l && isspace((unsigned char)x->text[l - 1]); ) x->text[--l] = 0;
        if (!x->is_num && !x->text[0]) { free(x->text); continue; }
        if (is_count_fact(x->key) && !x->is_num) {
            if (all_digits(x->text) && strlen(x->text) < 7) { x->num = atoll(x->text); x->is_num = 1; }
            else set_add(&unparsed, x->key);
        }
        if (!strcmp(x->key, "domain_list")) {
            const char *s = x->text;
            while (*s) {
                while (*s && (isspace((unsigned char)*s) || *s == ',' || *s == ';')) s++;
                const char *e = s; while (*e && !isspace((unsigned char)*e) && *e != ',' && *e != ';') e++;
                char nd[256];
                if (e > s && norm_domain(s, (size_t)(e - s), nd)) set_add(&intake_domains, nd);
                s = e;
            }
            set_sort(&intake_domains);
        }
        nans++;
    }

    /* facts, provenance and disagreements, written in one fixed order. */
    bf_jsonw w; bf_jsonw_init(&w, 2);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "schema"); bf_jsonw_str(&w, "bonfyre.hosting-inventory.v1");
    bf_jsonw_key(&w, "draft"); bf_jsonw_bool(&w, 1);
    bf_jsonw_key(&w, "subject"); bf_jsonw_str(&w, subject);
    bf_jsonw_key(&w, "scope"); bf_jsonw_str(&w, scope);
    char gref[300];
    if (nexp || (grant && *grant)) snprintf(gref, sizeof gref, "%s", grant);
    else snprintf(gref, sizeof gref, "intake:%s", reference);
    bf_jsonw_key(&w, "grant_ref"); bf_jsonw_str(&w, gref);
    bf_jsonw_key(&w, "grant_kind"); bf_jsonw_str(&w, nexp ? "export-authorization" : "customer-statement");

    /* Each fact: export value when a listing speaks to it, else the answer. */
    typedef struct { const char *fact; int idx; } FactDef; /* idx into has[] / -1 intake only */
    static const FactDef defs[] = {
        {"source_panel", -2}, {"site_count", 0}, {"domain_list", 1}, {"subdomain_count", 4},
        {"mailbox_count", 2}, {"database_count", 3}, {"dns_provider", -1}, {"backup_location", -1},
    };
    const int nd = (int)(sizeof defs / sizeof defs[0]);
    const char *prov[16] = {0};
    typedef struct { const char *fact; char intake[512]; char exported[512]; } Dis;
    Dis dis[16]; int ndis = 0;

    bf_jsonw_key(&w, "facts"); bf_jsonw_obj(&w);
    for (int f = 0; f < nd; f++) {
        const char *k = defs[f].fact;
        const Ans *a = find_ans(ans, nans, k);
        int ex = defs[f].idx >= 0 && has[defs[f].idx];
        if (defs[f].idx == -2 && panels.n) ex = 1;
        if (ex) {
            bf_jsonw_key(&w, k); prov[f] = export_src;
            long long n = -1;
            if (!strcmp(k, "source_panel")) {
                bf_jsonw_str(&w, export_src + 7);
                if (a && a->text[0]) {
                    char low[128]; size_t i = 0;
                    for (; a->text[i] && i < sizeof low - 1; i++) low[i] = (char)tolower((unsigned char)a->text[i]);
                    low[i] = 0;
                    int match = 0;
                    for (int p = 0; p < panels.n; p++) if (strstr(low, panels.v[p])) match = 1;
                    if (!match && ndis < 16) { dis[ndis].fact = k; snprintf(dis[ndis].intake, 512, "%s", a->text); snprintf(dis[ndis].exported, 512, "%s", export_src + 7); ndis++; }
                }
                continue;
            }
            if (!strcmp(k, "domain_list")) {
                bf_jsonw_arr(&w); for (int i = 0; i < acct.domains.n; i++) bf_jsonw_str(&w, acct.domains.v[i]); bf_jsonw_arr_end(&w);
                if (a && intake_domains.n) {
                    int same = intake_domains.n == acct.domains.n;
                    for (int i = 0; same && i < intake_domains.n; i++) same = set_has(&acct.domains, intake_domains.v[i]);
                    if (!same && ndis < 16) {
                        dis[ndis].fact = k;
                        snprintf(dis[ndis].intake, 512, "%d domain%s", intake_domains.n, intake_domains.n == 1 ? "" : "s");
                        snprintf(dis[ndis].exported, 512, "%d domain%s", acct.domains.n, acct.domains.n == 1 ? "" : "s");
                        ndis++;
                    }
                }
                continue;
            }
            n = !strcmp(k, "site_count") ? acct.sites.n : !strcmp(k, "mailbox_count") ? acct.mailboxes.n
              : !strcmp(k, "database_count") ? acct.databases.n : acct.subdomains.n;
            bf_jsonw_int(&w, n);
            if (a && ndis < 16 && (!a->is_num || a->num != n)) {
                dis[ndis].fact = k; snprintf(dis[ndis].intake, 512, "%s", a->is_num ? "" : a->text);
                if (a->is_num) snprintf(dis[ndis].intake, 512, "%lld", a->num);
                snprintf(dis[ndis].exported, 512, "%lld", n); ndis++;
            }
            continue;
        }
        if (!a) continue;
        if (!strcmp(k, "domain_list")) {
            if (!intake_domains.n) continue;
            bf_jsonw_key(&w, k); bf_jsonw_arr(&w);
            for (int i = 0; i < intake_domains.n; i++) bf_jsonw_str(&w, intake_domains.v[i]);
            bf_jsonw_arr_end(&w);
        } else if (is_count_fact(k)) {
            if (!a->is_num) continue;   /* "about four" is kept out of facts and named as unparsed */
            bf_jsonw_key(&w, k); bf_jsonw_int(&w, a->num);
        } else { bf_jsonw_key(&w, k); bf_jsonw_str(&w, a->text); }
        prov[f] = "intake";
    }
    bf_jsonw_obj_end(&w);

    bf_jsonw_key(&w, "provenance"); bf_jsonw_obj(&w);
    for (int f = 0; f < nd; f++) if (prov[f]) { bf_jsonw_key(&w, defs[f].fact); bf_jsonw_str(&w, prov[f]); }
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "disagreements"); bf_jsonw_arr(&w);
    for (int i = 0; i < ndis; i++) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "fact"); bf_jsonw_str(&w, dis[i].fact);
        bf_jsonw_key(&w, "intake"); bf_jsonw_str(&w, dis[i].intake);
        bf_jsonw_key(&w, "export"); bf_jsonw_str(&w, dis[i].exported);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "unparsed_answers"); bf_jsonw_arr(&w);
    set_sort(&unparsed);
    for (int i = 0; i < unparsed.n; i++) bf_jsonw_str(&w, unparsed.v[i]);
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "inputs"); bf_jsonw_arr(&w);
    if (intake) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, "intake");
        bf_jsonw_key(&w, "reference"); bf_jsonw_str(&w, reference);
        char *about = jget(idoc, msg, "about"), *at = jget(idoc, msg, "received_at");
        bf_jsonw_key(&w, "about"); if (about) bf_jsonw_str(&w, about); else bf_jsonw_null(&w);
        bf_jsonw_key(&w, "received_at"); if (at) bf_jsonw_str(&w, at); else bf_jsonw_null(&w);
        free(about); free(at);
        bf_jsonw_key(&w, "inbox_sha256"); bf_jsonw_str(&w, intake_sha);
        bf_jsonw_obj_end(&w);
    }
    for (int i = 0; i < nexp; i++) {
        const char *base = strrchr(inputs[i].path, '/');
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, "export");
        bf_jsonw_key(&w, "panel"); bf_jsonw_str(&w, inputs[i].panel);
        bf_jsonw_key(&w, "listing"); bf_jsonw_str(&w, inputs[i].listing);
        bf_jsonw_key(&w, "file"); bf_jsonw_str(&w, base ? base + 1 : inputs[i].path);
        bf_jsonw_key(&w, "sha256"); bf_jsonw_str(&w, inputs[i].sha);
        bf_jsonw_key(&w, "bytes"); bf_jsonw_int(&w, (int64_t)inputs[i].bytes);
        bf_jsonw_key(&w, "items"); bf_jsonw_int(&w, inputs[i].items);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "secret_fields_dropped"); bf_jsonw_int(&w, acct.secret_fields);
    bf_jsonw_key(&w, "panel_error_results"); bf_jsonw_int(&w, plesk_errors);
    bf_jsonw_key(&w, "not_read_from_exports"); bf_jsonw_arr(&w);
    bf_jsonw_str(&w, "dns_provider"); bf_jsonw_str(&w, "backup_location");
    if (panels.n && !has[2]) bf_jsonw_str(&w, "mailbox_count");
    if (panels.n && !has[3]) bf_jsonw_str(&w, "database_count");
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "effects"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "network"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "panel_connection"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "cms_write"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "publication"); bf_jsonw_bool(&w, 0);
    bf_jsonw_obj_end(&w);
    bf_jsonw_obj_end(&w);

    int rc = 0;
    FILE *o = w.oom ? NULL : fopen(out, "wb");
    if (!o || fwrite(w.buf, 1, w.len, o) != w.len || fputc('\n', o) == EOF) rc = 2;
    if (o && fclose(o) != 0) rc = 2;
    if (rc) fprintf(stderr, "inventory: cannot write %s\n", out);
    else {
        char d[65]; bf_sha256_file(out, d);
        printf("{\"inventory\":\"%s\",\"sha256\":\"%s\",\"exports\":%d,\"disagreements\":%d,\"secret_fields_dropped\":%d}\n",
               out, d, nexp, ndis, acct.secret_fields);
    }
    bf_jsonw_free(&w);
    for (int i = 0; i < nans; i++) free(ans[i].text);
    set_free(&acct.sites); set_free(&acct.domains); set_free(&acct.mailboxes); set_free(&acct.databases); set_free(&acct.subdomains);
    set_free(&panels); set_free(&intake_domains); set_free(&unparsed);
    if (idoc) bf_json_free(idoc);
    free(ibuf);
    return rc;
}
