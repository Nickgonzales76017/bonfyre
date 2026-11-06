/*
 * manifest.c — bonfyre manifest: an itemised manifest for an equipment lot
 * from the seller's photos and listing. Port of manifest.mjs and
 * cmd/BonfyreManifest. Fields the text states are copied exactly; the local
 * model only classifies (manifest_unit). Nothing is invented.
 */
#include "dp.h"
#include "usage.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

static const char *BRANDS[] = {"Dell", "HP", "Hewlett Packard", "Lenovo", "Apple", "Acer", "Asus", "Microsoft", "Samsung", "Toshiba", "Fujitsu", "Panasonic",
    "Canon", "Brother", "Lexmark", "Epson", "Xerox", "Ricoh", "Kyocera", "Konica", "Sharp", "Fujifilm", "Fuji Film", "Zebra",
    "Cisco", "Juniper", "Aruba", "Netgear", "Ubiquiti", "Meraki", "Vertiv", "APC", "Eaton", "Tripp Lite", "Supermicro", "NetApp", "Synology",
    "LG", "ViewSonic", "BenQ", "Seagate", "Western Digital", "Intel", "AMD", "Nvidia", NULL};
static const char *FAMILIES[] = {"OptiPlex", "Latitude", "Precision", "Inspiron", "Vostro", "XPS", "PowerEdge", "EliteDesk", "ProDesk", "EliteBook", "ProBook", "ZBook",
    "Engage", "ThinkCentre", "ThinkPad", "ThinkStation", "MacBook", "iMac", "Mac mini", "Surface", "Chromebook", "LaserJet", "OfficeJet", "Catalyst", NULL};
static const char *FAMILY_MAKER[][2] = {{"OptiPlex", "Dell"}, {"Latitude", "Dell"}, {"Precision", "Dell"}, {"Inspiron", "Dell"}, {"Vostro", "Dell"}, {"XPS", "Dell"}, {"PowerEdge", "Dell"},
    {"EliteDesk", "HP"}, {"ProDesk", "HP"}, {"EliteBook", "HP"}, {"ProBook", "HP"}, {"ZBook", "HP"}, {"Engage", "HP"}, {"LaserJet", "HP"}, {"OfficeJet", "HP"},
    {"ThinkCentre", "Lenovo"}, {"ThinkPad", "Lenovo"}, {"ThinkStation", "Lenovo"}, {"MacBook", "Apple"}, {"iMac", "Apple"}, {"Mac mini", "Apple"}, {"Surface", "Microsoft"}, {"Catalyst", "Cisco"}, {NULL, NULL}};

static double js_round(double x) { double r = floor(x); return x - r >= 0.5 ? r + 1 : r; }
static bf_re *RE(const char *p, const char *f) {
    char err[128]; bf_re *re = bf_re_compile(p, f, err, sizeof err);
    if (!re) { fprintf(stderr, "bonfyre manifest: bad pattern %s: %s\n", p, err); exit(70); }
    return re;
}
static int test(bf_re *re, const char *s) { return bf_re_exec(re, s, strlen(s), 0, NULL); }
static char *escape_re(const char *s) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (; *s; s++) { if (strchr(".*+?^${}()|[]\\", *s)) bf_re_append(&b, "\\", 1); bf_re_append(&b, s, 1); }
    return b.buf;
}
static char *group(bf_re *re, const char *s, int g) {
    bf_re_match m;
    if (!bf_re_exec(re, s, strlen(s), 0, &m) || m.start[g] < 0) return NULL;
    return bf_re_group(s, &m, g);
}
/* slice(0, n) in UTF-16 units */
static char *slice16(const char *s, size_t start, size_t end) {
    size_t units = 0; const unsigned char *p = (const unsigned char *)s, *a = NULL, *b = NULL;
    while (1) {
        if (units >= start && !a) a = p;
        if (units >= end || !*p) { b = p; break; }
        int len = *p < 0x80 ? 1 : (*p >> 5) == 6 ? 2 : (*p >> 4) == 14 ? 3 : 4;
        units += len == 4 ? 2 : 1; p += len;
    }
    if (!a) a = b;
    size_t n = (size_t)(b - a); char *o = malloc(n + 1); memcpy(o, a, n); o[n] = 0;
    return o;
}
static size_t units16(const char *s) { size_t u = 0; for (const unsigned char *p = (const unsigned char *)s; *p;) { int len = *p < 0x80 ? 1 : (*p >> 5) == 6 ? 2 : (*p >> 4) == 14 ? 3 : 4; u += len == 4 ? 2 : 1; p += len; } return u; }
static const char *base_name(const char *p) { const char *s = strrchr(p, '/'); return s ? s + 1 : p; }

/* ── OCR ────────────────────────────────────────────────────────────────── */

static char *ocr_binary(void) {
    const char *env = getenv("BONFYRE_OCR_BIN");
    struct stat st;
    if (env && *env && !stat(env, &st)) return strdup(env);
    const char *home = getenv("BONFYRE_HOME");
    char *installed = home && *home ? dp_fmt("%s/bin/bonfyre-ocr", home) : dp_fmt("%s/.bonfyre/bin/bonfyre-ocr", getenv("HOME") ? getenv("HOME") : "");
    if (!stat(installed, &st)) return installed;
    free(installed);
    const char *path = getenv("PATH");
    for (const char *p = path ? path : ""; *p;) {
        size_t n = strcspn(p, ":");
        if (n) { char *cand = dp_fmt("%.*s/bonfyre-ocr", (int)n, p); if (!stat(cand, &st)) return cand; free(cand); }
        p += n; if (*p == ':') p++;
    }
    return NULL;
}

typedef struct { double confidence; char *text; } OcrLine;
typedef struct { OcrLine *l; int n; } OcrLines;

static OcrLines ocr_lines(const char *path, const char *binary) {
    if (!binary) dp_fail("photos need bonfyre-ocr, which is not installed here (looked in ~/.bonfyre/bin and PATH); give the listing with --listing instead, or set BONFYRE_OCR_BIN");
    int fd[2]; if (pipe(fd)) dp_fail("OCR could not read %s", path);
    pid_t pid = fork();
    if (!pid) {
        dup2(fd[1], 1); close(fd[0]); close(fd[1]);
        int null = open("/dev/null", 1); if (null >= 0) dup2(null, 2);
        execl(binary, binary, "lines", path, (char *)NULL);
        _exit(127);
    }
    close(fd[1]);
    bf_re_buf out = {0}; char buf[8192];
    for (ssize_t n; (n = read(fd[0], buf, sizeof buf)) > 0;) bf_re_append(&out, buf, (size_t)n);
    close(fd[0]);
    int status = 0; waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status)) dp_fail("OCR could not read %s", path);
    OcrLines r = {0};
    for (char *line = out.buf; line && *line;) {
        char *nl = strchr(line, '\n'); if (nl) *nl = 0;
        if (*line) {
            char *cells[7]; int nc = 0; char *p = line;
            while (nc < 6) { char *tab = strchr(p, '\t'); if (!tab) break; cells[nc++] = p; *tab = 0; p = tab + 1; }
            if (nc == 6) {
                char *text = dp_js_trim(p);
                if (*text) {
                    char *end; double conf = strtod(cells[5], &end);
                    if (!*cells[5]) conf = 0; else if (*end) conf = NAN;
                    r.l = realloc(r.l, sizeof *r.l * (size_t)(r.n + 1)); r.l[r.n].confidence = conf; r.l[r.n].text = text; r.n++;
                } else free(text);
            }
        }
        line = nl ? nl + 1 : NULL;
    }
    free(out.buf);
    return r;
}

static char *listing_text(const char *raw) {
    static bf_re *scripts, *tags, *amp, *apos, *quot, *ents, *ws;
    if (!scripts) {
        scripts = RE("<(script|style)[\\s\\S]*?<\\/\\1>", "gi"); tags = RE("<[^>]+>", "g"); amp = RE("&amp;", "g");
        apos = RE("&#39;|&apos;", "g"); quot = RE("&quot;", "g"); ents = RE("&[a-z#0-9]+;", "gi"); ws = RE("\\s+", "g");
    }
    char *a = bf_re_replace(scripts, raw, " "), *b = bf_re_replace(tags, a, " "), *c = bf_re_replace(amp, b, "&"), *d = bf_re_replace(apos, c, "'"),
         *e = bf_re_replace(quot, d, "\""), *f = bf_re_replace(ents, e, " "), *g = bf_re_replace(ws, f, " "), *t = dp_js_trim(g);
    free(a); free(b); free(c); free(d); free(e); free(f); free(g);
    return t;
}

/* ── exact fields ───────────────────────────────────────────────────────── */

typedef struct {
    char *source, *text, *title; char **files; int nfiles;
    char **brands; int nbrands; char *family, *part_number, *stated_condition, *stated_make, *maker_from_family; double quantity;
} Unit;

static void exact_fields(Unit *u, const char *text) {
    u->brands = NULL; u->nbrands = 0;
    for (int i = 0; BRANDS[i]; i++) {
        char *e = escape_re(BRANDS[i]), *p = dp_fmt("(^|[^a-z])%s([^a-z]|$)", e);
        bf_re *re = RE(p, "i"); int hit = test(re, text); bf_re_free(re); free(e); free(p);
        if (!hit) continue;
        const char *name = !strcmp(BRANDS[i], "Fuji Film") ? "Fujifilm" : !strcmp(BRANDS[i], "Hewlett Packard") ? "HP" : BRANDS[i];
        int dup = 0; for (int k = 0; k < u->nbrands; k++) if (!strcmp(u->brands[k], name)) dup = 1;
        if (!dup) { u->brands = realloc(u->brands, sizeof *u->brands * (size_t)(u->nbrands + 1)); u->brands[u->nbrands++] = strdup(name); }
    }
    char *family_pattern = NULL; u->family = NULL;
    for (int i = 0; FAMILIES[i]; i++) {
        const char *name = FAMILIES[i]; size_t len = strlen(name), keep = len - 2 > 6 ? len - 2 : 6;
        char *stem_src = strndup(name, keep), *stem = escape_re(stem_src);
        char *pattern = dp_fmt("(^|[^a-z])(%s[a-z]*)\\s*([A-Z]?\\d{2,4}[A-Za-z]{0,3}(?:\\s?G\\d)?)?", stem);
        bf_re *re = RE(pattern, "i"); bf_re_match m;
        int hit = bf_re_exec(re, text, strlen(text), 0, &m);
        if (hit) {
            if (!family_pattern) family_pattern = strdup(pattern);
            if (!u->family) { char *model = m.start[3] >= 0 && m.end[3] > m.start[3] ? bf_re_group(text, &m, 3) : NULL; u->family = model ? dp_fmt("%s %s", name, model) : strdup(name); free(model); }
        }
        bf_re_free(re); free(stem_src); free(stem); free(pattern);
    }
    if (!u->family) u->family = strdup("");
    static bf_re *part, *qty, *units, *cond, *make;
    if (!part) {
        part = RE("PART\\s*(?:NO|NUMBER|#)\\.?\\s*[:#]?\\s*([A-Z0-9-]{4,})", "i");
        qty = RE("\\b(?:qty|quantity|lot of)\\s*[:.]?\\s*(\\d{1,5})\\b", "i");
        units = RE("\\b(\\d{1,5})\\s*(?:x\\s|pcs\\b|pieces\\b|units?\\b|new\\b|lot\\b|computers?\\b|desktops?\\b|laptops?\\b|cartridges?\\b|toners?\\b|monitors?\\b|printers?\\b)", "i");
        cond = RE("condition:\\s*([a-z ]{3,20}?)(?=\\s+[a-z /]+:|$)", "i");
        make = RE("make \\/ manufacturer:\\s*([A-Za-z0-9 &.-]{2,60}?)(?=\\s+[A-Z][A-Za-z ]+:|\\s+QR Code|$)", "");
    }
    char *pn = group(part, text, 1); u->part_number = pn ? pn : strdup("");
    char *counted;
    if (family_pattern) { bf_re *fp = RE(family_pattern, "gi"); counted = bf_re_replace(fp, text, " "); bf_re_free(fp); free(family_pattern); }
    else counted = strdup(text);
    char *q = group(qty, counted, 1); if (!q) q = group(units, counted, 1);
    u->quantity = q && *q ? strtod(q, NULL) : 0; free(q); free(counted);
    char *c = group(cond, text, 1); if (c) { u->stated_condition = dp_js_trim(c); free(c); } else u->stated_condition = strdup("");
    char *mk = group(make, text, 1); if (mk) { u->stated_make = dp_js_trim(mk); free(mk); } else u->stated_make = strdup("");
    const char *maker = NULL;
    if (*u->family) {
        char first[64]; snprintf(first, sizeof first, "%.*s", (int)strcspn(u->family, " "), u->family);
        for (int i = 0; FAMILY_MAKER[i][0]; i++) if (!strcmp(FAMILY_MAKER[i][0], first)) maker = FAMILY_MAKER[i][1];
    }
    int in_brands = 0; for (int k = 0; maker && k < u->nbrands; k++) if (!strcmp(u->brands[k], maker)) in_brands = 1;
    u->maker_from_family = strdup(maker && !in_brands ? maker : "");
}

typedef struct { Unit *u; int n; } Lot;

static Lot read_lot(char **photos, int np, char **listings, int nl) {
    Lot lot = {0};
    static bf_re *numbered, *ext, *alnum2;
    if (!numbered) { numbered = RE("[-_ ]?\\d+\\.(jpe?g|png|heic|tiff?|pdf)$", "i"); ext = RE("\\.(jpe?g|png|heic|tiff?|pdf)$", "i"); alnum2 = RE("[A-Za-z0-9]{2,}", ""); }
    char **keys = NULL; int nk = 0; int **members = NULL; int *nmem = NULL;
    for (int i = 0; i < np; i++) {
        char *a = bf_re_replace(numbered, base_name(photos[i]), ""), *key = bf_re_replace(ext, a, ""); free(a);
        int k = 0; while (k < nk && strcmp(keys[k], key)) k++;
        if (k == nk) { keys = realloc(keys, sizeof *keys * (size_t)(nk + 1)); members = realloc(members, sizeof *members * (size_t)(nk + 1)); nmem = realloc(nmem, sizeof *nmem * (size_t)(nk + 1)); keys[nk] = key; members[nk] = NULL; nmem[nk] = 0; nk++; }
        else free(key);
        members[k] = realloc(members[k], sizeof(int) * (size_t)(nmem[k] + 1)); members[k][nmem[k]++] = i;
    }
    char *binary = np ? ocr_binary() : NULL;
    for (int k = 0; k < nk; k++) {
        bf_re_buf joined = {0}; bf_re_append(&joined, "", 0); int first = 1;
        for (int j = 0; j < nmem[k]; j++) {
            OcrLines lines = ocr_lines(photos[members[k][j]], binary);
            for (int i = 0; i < lines.n; i++) {
                if (!(lines.l[i].confidence >= 0.5) || !test(alnum2, lines.l[i].text)) continue;
                if (!first) bf_re_append(&joined, " ", 1);
                bf_re_append(&joined, lines.l[i].text, strlen(lines.l[i].text)); first = 0;
            }
        }
        Unit u; memset(&u, 0, sizeof u);
        u.source = dp_fmt("photos %s", keys[k]);
        u.files = malloc(sizeof *u.files * (size_t)nmem[k]); u.nfiles = nmem[k];
        for (int j = 0; j < nmem[k]; j++) u.files[j] = strdup(base_name(photos[members[k][j]]));
        u.text = slice16(joined.buf, 0, 1200);
        exact_fields(&u, u.text);
        lot.u = realloc(lot.u, sizeof *lot.u * (size_t)(lot.n + 1)); lot.u[lot.n++] = u;
        free(joined.buf);
    }
    static bf_re *title_split;
    if (!title_split) title_split = RE(" - (?:business|computers|electronics|for sale)", "i");
    for (int i = 0; i < nl; i++) {
        char *raw = dp_read_file(listings[i], NULL);
        if (!raw) dp_fail("ENOENT: no such file or directory, open '%s'", listings[i]);
        char *text = listing_text(raw); free(raw);
        bf_re_match m;
        char *head = bf_re_exec(title_split, text, strlen(text), 0, &m) ? strndup(text, (size_t)m.start[0]) : strdup(text);
        char *title = slice16(head, 0, 200); free(head);
        const char *qr = strstr(text, "QR Code Link to This Post");
        size_t at = qr ? units16(strndup(text, (size_t)(qr - text))) : 0;   /* indexOf in UTF-16 units */
        char *body = slice16(text, at, 1600);
        if (!*body) { free(body); body = slice16(text, 0, 1600); }
        char *words = dp_fmt("%s %s", title, body);
        Unit u; memset(&u, 0, sizeof u);
        const char *name = base_name(listings[i]);
        u.source = dp_fmt("listing %s", name);
        u.files = malloc(sizeof *u.files); u.files[0] = strdup(name); u.nfiles = 1;
        u.text = slice16(words, 0, 1200);
        u.title = title;
        exact_fields(&u, words);
        lot.u = realloc(lot.u, sizeof *lot.u * (size_t)(lot.n + 1)); lot.u[lot.n++] = u;
        free(text); free(body); free(words);
    }
    return lot;
}

static char *join(char **v, int n, const char *sep) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (int i = 0; i < n; i++) { if (i) bf_re_append(&b, sep, strlen(sep)); bf_re_append(&b, v[i], strlen(v[i])); }
    return b.buf;
}

static bf_jv *unit_case(const Unit *u, int index, const char *mask, const bf_jv *policy) {
    char *brands = join(u->brands, u->nbrands, ", ");
    const char *brand = *brands ? brands : *u->stated_make ? u->stated_make : u->maker_from_family;
    char qty[32] = ""; if (u->quantity) bf_js_number(u->quantity, qty);
    bf_jv *unit = bf_jv_obj();
    const char *shown[6][2] = {{"text", u->text}, {"brand", brand}, {"model", u->family}, {"part_number", u->part_number},
                               {"seller_condition", u->stated_condition}, {"quantity", qty}};
    for (int i = 0; i < 6; i++) if (*shown[i][1]) bf_jv_set(unit, shown[i][0], bf_jv_str(shown[i][1]));
    char *id = dp_fmt("unit-%d", index + 1), *subject = dp_fmt("manifest/%s", id), *ref = dp_fmt("Manifest:%s", u->source);
    const char *descsrc = u->title && *u->title ? u->title : *u->text ? u->text : u->source;
    char *desc = slice16(descsrc, 0, 200);
    bf_jv *in = bf_jv_obj(), *fr = bf_jv_arr(), *f = bf_jv_obj(), *state = bf_jv_obj();
    bf_jv_set(in, "program", bf_jv_str("manifest_unit"));
    bf_jv_set(in, "owner", bf_jv_str("Manifest"));
    bf_jv_set(in, "subject_id", bf_jv_str(subject));
    bf_jv_set(in, "mask_digest", bf_jv_str(mask));
    bf_jv_set(in, "gate_state", bf_jv_str("ADMIT"));
    bf_jv_set(in, "information_policy_class", bf_jv_str("private"));
    bf_jv_set(f, "id", bf_jv_str(id)); bf_jv_set(f, "description", bf_jv_str(desc)); bf_jv_set(f, "owner_ref", bf_jv_str(ref));
    bf_jv_push(fr, f);
    bf_jv_set(in, "frontier", fr);
    bf_jv_set(state, "unit", unit);
    bf_jv_set(in, "state", state);
    bf_jv *c = dp_compile_case(in, policy);
    bf_jv_free(in); free(brands); free(id); free(subject); free(ref); free(desc);
    return c;
}

static int readable(const Unit *u) {
    static bf_re *w3;
    if (!w3) w3 = RE("[A-Za-z0-9]{3,}", "g");
    if (u->nbrands || *u->maker_from_family || *u->family) return 1;
    int count = 0; size_t at = 0, len = strlen(u->text); bf_re_match m;
    while (at <= len && bf_re_exec(w3, u->text, len, at, &m)) { count++; at = (size_t)m.end[0]; if (count >= 2) return 1; }
    return 0;
}

typedef struct { const char *category; int brand_only, no_storage; const char *condition; } Settles;

static int brand_only(const Unit *u) {
    if (*u->family || *u->part_number) return 0;
    static bf_re *brands_re, *nonalpha;
    if (!brands_re) {
        bf_re_buf b = {0}; bf_re_append(&b, "", 0);
        for (int i = 0; BRANDS[i]; i++) { char *e = escape_re(BRANDS[i]); if (i) bf_re_append(&b, "|", 1); bf_re_append(&b, e, strlen(e)); free(e); }
        brands_re = RE(b.buf, "gi"); free(b.buf);
        nonalpha = RE("[^A-Za-z]+", "g");
    }
    char *a = bf_re_replace(brands_re, u->text, " "), *b = bf_re_replace(nonalpha, a, " "), *t = dp_js_trim(b);
    int words = 0;
    for (char *p = t; *p;) {
        while (*p == ' ' || (*p >= 9 && *p <= 13)) p++;
        char *e = p; while (*e && !(*e == ' ' || (*e >= 9 && *e <= 13))) e++;
        if (e - p > 3) words++;
        p = e;
    }
    free(a); free(b); free(t);
    return words == 0;
}

static Settles exact_settles(const Unit *u) {
    static bf_re *consumable, *equipment, *condition_words, *no_storage;
    if (!consumable) {
        consumable = RE("\\b(toner|ink\\s*(?:cartridges?|lot)?|drum\\s+units?|ribbons?|cable\\s+managers?|patch\\s+panels?|rail\\s+kits?|rack\\s+(?:shel(?:f|ves)|kits?))\\b", "i");
        equipment = RE("\\b(computers?|desktops?|laptops?|notebooks?|servers?|workstations?|towers?|monitors?|displays?|printers?|copiers?|scanners?|switch(?:es)?|routers?|firewalls?|phones?|tablets?|drives?|hdds?|ssds?|disks?|nas|imacs?|macbooks?|chromebooks?)\\b", "i");
        condition_words = RE("\\b(new|used|refurb\\w*|tested|working|works|untested|broken|for\\s+parts|as[\\s-]is|grade\\s+[a-d]|excellent|good|fair|damaged)\\b", "i");
        no_storage = RE("\\b(no|without)\\s+(hard\\s*drives?|hdd|ssd|storage|drives?)\\b", "i");
    }
    Settles s;
    s.category = test(consumable, u->text) && !test(equipment, u->text) ? "consumable" : NULL;
    s.brand_only = brand_only(u);
    s.no_storage = test(no_storage, u->text);
    char *both = dp_fmt("%s %s", u->text, u->stated_condition);
    s.condition = !test(condition_words, both) ? "unknown" : NULL;
    free(both);
    return s;
}

static bf_jv *str_array(char **v, int n) { bf_jv *a = bf_jv_arr(); for (int i = 0; i < n; i++) bf_jv_push(a, bf_jv_str(v[i])); return a; }

static bf_jv *run_manifest(char **photos, int np, char **listings, int nl, const char *lot_name, DpLocal *local) {
    Lot all = read_lot(photos, np, listings, nl);
    if (!all.n) dp_fail("give at least one photo or listing");
    Unit **units = malloc(sizeof *units * (size_t)all.n), **unreadable = malloc(sizeof *unreadable * (size_t)all.n);
    int nu = 0, nr = 0;
    for (int i = 0; i < all.n; i++) { if (readable(&all.u[i])) units[nu++] = &all.u[i]; else unreadable[nr++] = &all.u[i]; }
    const bf_jv *policy = dp_policy();
    bf_jv *m = bf_jv_obj(), *list = bf_jv_arr();
    bf_jv_set(m, "lot", bf_jv_str(lot_name));
    for (int i = 0; i < nu; i++) { bf_jv *pair = bf_jv_arr(); bf_jv_push(pair, bf_jv_str(units[i]->source)); bf_jv_push(pair, bf_jv_str(units[i]->text)); bf_jv_push(list, pair); }
    bf_jv_set(m, "units", list);
    char *mask = dp_digest_str(m); bf_jv_free(m);
    bf_jv *cases = bf_jv_arr();
    for (int i = 0; i < nu; i++) bf_jv_push(cases, unit_case(units[i], i, mask, policy));
    double started = dp_now_ms();
    DpOwner *model = cases->len ? dp_local_acquire(local) : NULL;
    bf_jv *receipts = cases->len ? dp_assess_cases(cases, model, (long)cases->len, policy) : bf_jv_arr();
    bf_jv *rows = bf_jv_arr();
    for (int i = 0; i < nu; i++) {
        const Unit *u = units[i];
        const bf_jv *answers = bf_jv_get(receipts->items[i], "answers");
        bf_jv *proposal = dp_proposal(receipts->items[i], policy);
        Settles s = exact_settles(u);
        char *brands = join(u->brands, u->nbrands, "/");
        const char *make = *brands ? brands : *u->stated_make ? u->stated_make : u->maker_from_family;
        const char *second = *u->family ? u->family : u->part_number;
        char *line = *make && *second ? dp_fmt("%s %s", make, second) : strdup(*make ? make : second);
        bf_jv *confirm = bf_jv_arr();
        if (!*u->family && !*u->part_number) bf_jv_push(confirm, bf_jv_str("model not stated"));
        if (!u->quantity) bf_jv_push(confirm, bf_jv_str("count not stated"));
        if (bf_jv_path(answers, "identity_clear.noul")->n < 0.5) bf_jv_push(confirm, bf_jv_str("make and model unclear"));
        if (s.brand_only && !s.category) bf_jv_push(confirm, bf_jv_str("only a brand name is readable: say what the items are"));
        else if (!s.category && bf_jv_path(answers, "category.confidence")->n < 0.5) bf_jv_push(confirm, bf_jv_str("kind of equipment unsure"));
        const char *pcat = bf_jv_s(bf_jv_get(proposal, "category"));
        int computer = pcat && (!strcmp(pcat, "desktop") || !strcmp(pcat, "laptop") || !strcmp(pcat, "server") || !strcmp(pcat, "phone_tablet"));
        double db = bf_jv_path(answers, "data_bearing.noul")->n;
        bf_jv *row = bf_jv_obj();
        bf_jv_set(row, "source", bf_jv_str(u->source));
        bf_jv_set(row, "files", str_array(u->files, u->nfiles));
        bf_jv_set(row, "make", bf_jv_str(make));
        bf_jv_set(row, "make_by", bf_jv_str(u->nbrands || *u->stated_make ? "text" : *make ? "model family" : ""));
        bf_jv_set(row, "model_family", bf_jv_str(u->family));
        bf_jv_set(row, "part_number", bf_jv_str(u->part_number));
        bf_jv_set(row, "quantity", u->quantity ? bf_jv_num(u->quantity) : bf_jv_null());
        bf_jv_set(row, "category", s.category ? bf_jv_str(s.category) : s.brand_only ? bf_jv_str("") : bf_jv_clone(bf_jv_get(proposal, "category")));
        bf_jv_set(row, "category_confidence", s.category ? bf_jv_num(1) : bf_jv_clone(bf_jv_path(answers, "category.confidence")));
        bf_jv_set(row, "category_by", bf_jv_str(s.category ? "text" : "local model"));
        bf_jv_set(row, "data_bearing", s.category && !strcmp(s.category, "consumable") ? bf_jv_bool(0) : s.brand_only ? bf_jv_null()
                                      : computer ? bf_jv_bool(!s.no_storage) : bf_jv_bool(db >= 0.5));
        bf_jv_set(row, "data_bearing_probability", bf_jv_num(db));
        bf_jv_set(row, "condition", s.condition ? bf_jv_str(s.condition) : bf_jv_clone(bf_jv_get(proposal, "condition")));
        bf_jv_set(row, "condition_by", bf_jv_str(s.condition ? "text says nothing" : "local model"));
        bf_jv_set(row, "seller_condition", bf_jv_str(u->stated_condition));
        bf_jv_set(row, "confirm_with_seller", confirm);
        bf_jv_set(row, "line", bf_jv_str(*line ? line : u->title && *u->title ? u->title : u->source));
        bf_jv_set(row, "receipt", bf_jv_clone(bf_jv_get(receipts->items[i], "response_digest")));
        bf_jv_push(rows, row);
        bf_jv_free(proposal); free(brands); free(line);
    }
    for (int i = 0; i < nr; i++) {
        const Unit *u = unreadable[i];
        bf_jv *row = bf_jv_obj(), *confirm = bf_jv_arr();
        bf_jv_push(confirm, bf_jv_str("no readable text: attach the listing or a photo of the label"));
        bf_jv_set(row, "source", bf_jv_str(u->source)); bf_jv_set(row, "files", str_array(u->files, u->nfiles));
        bf_jv_set(row, "make", bf_jv_str("")); bf_jv_set(row, "make_by", bf_jv_str("")); bf_jv_set(row, "model_family", bf_jv_str(""));
        bf_jv_set(row, "part_number", bf_jv_str("")); bf_jv_set(row, "quantity", bf_jv_null()); bf_jv_set(row, "category", bf_jv_str(""));
        bf_jv_set(row, "category_confidence", bf_jv_num(0)); bf_jv_set(row, "data_bearing", bf_jv_null()); bf_jv_set(row, "data_bearing_probability", bf_jv_null());
        bf_jv_set(row, "condition", bf_jv_str("unknown")); bf_jv_set(row, "seller_condition", bf_jv_str(""));
        bf_jv_set(row, "confirm_with_seller", confirm); bf_jv_set(row, "line", bf_jv_str(u->source)); bf_jv_set(row, "receipt", bf_jv_null());
        bf_jv_push(rows, row);
    }
    int wipe = 0, to_confirm = 0;
    for (size_t i = 0; i < rows->len; i++) {
        const bf_jv *db = bf_jv_get(rows->items[i], "data_bearing");
        if (db && db->type == BF_JV_BOOL && db->b) wipe++;
        if (bf_jv_get(rows->items[i], "confirm_with_seller")->len) to_confirm++;
    }
    bf_jv *report = bf_jv_obj();
    bf_jv_set(report, "schema", bf_jv_str("bonfyre.manifest.v1"));
    bf_jv_set(report, "lot", bf_jv_str(lot_name));
    bf_jv_set(report, "units", bf_jv_num((double)rows->len));
    bf_jv_set(report, "needs_wipe", bf_jv_num(wipe));
    bf_jv_set(report, "to_confirm", bf_jv_num(to_confirm));
    bf_jv_set(report, "model", model ? bf_jv_str(model->model) : bf_jv_null());
    bf_jv_set(report, "model_artifact_sha256", model ? bf_jv_str(model->artifact_digest) : bf_jv_null());
    bf_jv_set(report, "seconds", bf_jv_num(js_round(dp_now_ms() - started) / 1000));
    bf_jv_set(report, "rows", rows);
    bf_jv_set(report, "receipts", receipts);
    bf_jv_set(report, "cannot_infer", bf_jv_str("a manifest read from photos and a listing -> units counted, tested or owned; it says what the seller showed and wrote, and what to confirm"));
    free(mask); free(units); free(unreadable);
    return report;
}

/* ── exports ────────────────────────────────────────────────────────────── */

static char *tsv_cell(const bf_jv *v) {
    char *s;
    char b[32];
    if (!v || v->type == BF_JV_NULL) s = strdup("");
    else if (v->type == BF_JV_STR) s = strdup(v->s);
    else if (v->type == BF_JV_NUM) { bf_js_number(v->n, b); s = strdup(b); }
    else if (v->type == BF_JV_BOOL) s = strdup(v->b ? "true" : "false");
    else s = strdup("");
    for (char *p = s; *p; p++) if (*p == '\t' || *p == '\n') *p = ' ';
    return s;
}
static void tsv_row(bf_re_buf *b, bf_jv **cells, int n) {
    for (int i = 0; i < n; i++) { char *c = tsv_cell(cells[i]); if (i) bf_re_append(b, "\t", 1); bf_re_append(b, c, strlen(c)); free(c); bf_jv_free(cells[i]); }
    bf_re_append(b, "\n", 1);
}
static bf_jv *joined(const bf_jv *arr, const char *sep) {
    bf_re_buf b = {0}; bf_re_append(&b, "", 0);
    for (size_t i = 0; arr && i < arr->len; i++) { if (i) bf_re_append(&b, sep, strlen(sep)); bf_re_append(&b, arr->items[i]->s, arr->items[i]->len); }
    bf_jv *v = bf_jv_str(b.buf); free(b.buf); return v;
}

static char *manifest_tsv(const bf_jv *report) {
    bf_re_buf b = {0};
    const char *head = "line\tmake\tmodel_family\tpart_number\tquantity\tcategory\tdata_bearing\tcondition\tconfirm_with_seller\tsource\n";
    bf_re_append(&b, head, strlen(head));
    const bf_jv *rows = bf_jv_get(report, "rows");
    for (size_t i = 0; i < rows->len; i++) {
        const bf_jv *r = rows->items[i], *db = bf_jv_get(r, "data_bearing");
        bf_jv *cells[10] = {bf_jv_clone(bf_jv_get(r, "line")), bf_jv_clone(bf_jv_get(r, "make")), bf_jv_clone(bf_jv_get(r, "model_family")),
            bf_jv_clone(bf_jv_get(r, "part_number")), bf_jv_clone(bf_jv_get(r, "quantity")), bf_jv_clone(bf_jv_get(r, "category")),
            bf_jv_str(db->type == BF_JV_NULL ? "" : db->b ? "yes" : "no"), bf_jv_clone(bf_jv_get(r, "condition")),
            joined(bf_jv_get(r, "confirm_with_seller"), "; "), bf_jv_clone(bf_jv_get(r, "source"))};
        tsv_row(&b, cells, 10);
    }
    return b.buf;
}

typedef struct { bf_jv *ready, *held; } Itad;
static Itad itad_rows(const bf_jv *report) {
    Itad t = {bf_jv_arr(), bf_jv_arr()};
    const bf_jv *rows = bf_jv_get(report, "rows");
    char **seen = NULL; int ns = 0;
    for (size_t i = 0; i < rows->len; i++) {
        const bf_jv *r = rows->items[i];
        const bf_jv *receipt = bf_jv_get(r, "receipt"), *qty = bf_jv_get(r, "quantity"), *cat = bf_jv_get(r, "category");
        bf_jv *reasons = bf_jv_arr();
        if (!receipt || receipt->type == BF_JV_NULL || (receipt->type == BF_JV_STR && !receipt->len)) bf_jv_push(reasons, bf_jv_str("not read"));
        if (!qty || qty->type == BF_JV_NULL || (qty->type == BF_JV_NUM && !qty->n)) bf_jv_push(reasons, bf_jv_str("count not stated"));
        if (!cat || cat->type == BF_JV_NULL || (cat->type == BF_JV_STR && !cat->len)) bf_jv_push(reasons, bf_jv_str("kind of equipment not known"));
        if (reasons->len) {
            bf_jv *h = bf_jv_obj();
            bf_jv_set(h, "line", bf_jv_clone(bf_jv_get(r, "line"))); bf_jv_set(h, "source", bf_jv_clone(bf_jv_get(r, "source")));
            bf_jv_set(h, "reasons", reasons); bf_jv_set(h, "confirm_with_seller", bf_jv_clone(bf_jv_get(r, "confirm_with_seller")));
            bf_jv_push(t.held, h);
            continue;
        }
        bf_jv_free(reasons);
        const char *line = bf_jv_s(bf_jv_get(r, "line"));
        char *model = slice16(line && *line ? line : "Unnamed unit", 0, 90); int n = 2;
        for (;;) {
            char *lm = strdup(model); for (char *p = lm; *p; p++) if (*p >= 'A' && *p <= 'Z') *p += 32;
            int dup = 0; for (int k = 0; k < ns; k++) if (!strcmp(seen[k], lm)) dup = 1;
            if (!dup) { seen = realloc(seen, sizeof *seen * (size_t)(ns + 1)); seen[ns++] = lm; break; }
            free(lm); free(model);
            char *cut = slice16(line, 0, 85); model = dp_fmt("%s (%d)", cut, n++); free(cut);
        }
        bf_jv *o = bf_jv_obj();
        bf_jv_set(o, "model", bf_jv_str(model)); bf_jv_set(o, "qty", bf_jv_clone(qty)); bf_jv_set(o, "grade", bf_jv_clone(bf_jv_get(r, "condition")));
        bf_jv_push(t.ready, o); free(model);
    }
    return t;
}
static char *itad_tsv(const bf_jv *report) {
    Itad t = itad_rows(report);
    bf_re_buf b = {0}; const char *head = "model\tqty\tgrade\trecovery_per_unit\n"; bf_re_append(&b, head, strlen(head));
    for (size_t i = 0; i < t.ready->len; i++) {
        const bf_jv *q = bf_jv_get(t.ready->items[i], "qty");
        if (!(q && q->type == BF_JV_NUM && q->n == floor(q->n) && q->n > 0)) { char *v = tsv_cell(q); dp_fail("refusing to export %s: quantity %s", bf_jv_s(bf_jv_get(t.ready->items[i], "model")), v); }
    }
    for (size_t i = 0; i < t.ready->len; i++) {
        const bf_jv *o = t.ready->items[i];
        bf_jv *cells[4] = {bf_jv_clone(bf_jv_get(o, "model")), bf_jv_clone(bf_jv_get(o, "qty")), bf_jv_clone(bf_jv_get(o, "grade")), bf_jv_str("")};
        tsv_row(&b, cells, 4);
    }
    return b.buf;
}
static char *itad_held_tsv(const bf_jv *report) {
    Itad t = itad_rows(report);
    bf_re_buf b = {0}; const char *head = "line\tsource\twhy_held\tconfirm_with_seller\n"; bf_re_append(&b, head, strlen(head));
    for (size_t i = 0; i < t.held->len; i++) {
        const bf_jv *h = t.held->items[i];
        bf_jv *cells[4] = {bf_jv_clone(bf_jv_get(h, "line")), bf_jv_clone(bf_jv_get(h, "source")), joined(bf_jv_get(h, "reasons"), "; "), joined(bf_jv_get(h, "confirm_with_seller"), "; ")};
        tsv_row(&b, cells, 4);
    }
    return b.buf;
}

int manifest_main(int argc, char **argv) {
    if (argc > 0 && (!strcmp(argv[0], "--help") || !strcmp(argv[0], "-h") || !strcmp(argv[0], "help"))) { fputs(MANIFEST_USAGE, stdout); return 0; }
    static const char *const repeatable[] = {"photo", "listing", NULL};
    DpFlags fl; dp_flags(&fl, argc, argv, repeatable);
    char **photos, **listings; int np = dp_flag_list(&fl, "photo", &photos), nl = dp_flag_list(&fl, "listing", &listings);
    if (!np && !nl) dp_fail("bonfyre manifest needs --photo FILE or --listing FILE (see bonfyre manifest --help)");
    DpLocal local;
    dp_local_init(&local, dp_flag(&fl, "model-endpoint"), dp_flag_is_true(&fl, "model"), dp_flag(&fl, "model") && !dp_flag_is_true(&fl, "model") ? dp_flag(&fl, "model") : NULL);
    const char *lot = dp_flag(&fl, "lot");
    bf_jv *report = NULL; DpCatch c; char failure[2048] = "";
    if (DP_TRY(&c)) { report = run_manifest(photos, np, listings, nl, lot && strcmp(lot, DP_TRUE) ? lot : "", &local); DP_END(&c); }
    else snprintf(failure, sizeof failure, "%s", c.msg);
    dp_local_close(&local);
    if (*failure) dp_fail("%s", failure);
    const char *out = dp_flag(&fl, "out"), *itad = dp_flag(&fl, "itad");
    if (out) {
        size_t n = strlen(out);
        if (n >= 5 && !strcmp(out + n - 5, ".json")) { size_t len; char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, &len); char *w = dp_fmt("%s\n", s); dp_write_file(out, w, len + 1); }
        else { char *t = manifest_tsv(report); dp_write_file(out, t, strlen(t)); }
    }
    Itad t = itad_rows(report);
    char *held_path = NULL;
    if (itad) {
        size_t n = strlen(itad);
        held_path = n >= 4 && !strcmp(itad + n - 4, ".tsv") ? dp_fmt("%.*s.held.tsv", (int)(n - 4), itad) : dp_fmt("%s.held.tsv", itad);
        char *x = itad_tsv(report); dp_write_file(itad, x, strlen(x));
        if (t.held->len) { char *h = itad_held_tsv(report); dp_write_file(held_path, h, strlen(h)); }
    }
    if (dp_flag(&fl, "json")) { char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, NULL); printf("%s\n", s); return 0; }
    char *tsv = manifest_tsv(report); fputs(tsv, stdout);
    char units[32], wipe[32], confirm[32], secs[32];
    bf_js_number(bf_jv_get(report, "units")->n, units); bf_js_number(bf_jv_get(report, "needs_wipe")->n, wipe);
    bf_js_number(bf_jv_get(report, "to_confirm")->n, confirm); bf_js_number(bf_jv_get(report, "seconds")->n, secs);
    printf("\n%s units; %s hold data to wipe; %s have something to confirm with the seller. ", units, wipe, confirm);
    if (bf_jv_s(bf_jv_get(report, "model"))) printf("Read on this machine by %s in %ss.", bf_jv_s(bf_jv_get(report, "model")), secs);
    else printf("No unit had text to ask the local model about.");
    printf("\n");
    if (itad && t.held->len) printf("%zu units are not in %s because they cannot be priced yet (count not stated, or not read); they are listed in %s.\n", t.held->len, itad, held_path);
    return 0;
}
