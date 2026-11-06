/* estate_reference.h -- one reading of "where does this reference open".
 *
 * A page names publishers, organizations, sources and identities in many
 * forms: a URL the publisher linked, a bare host, a Wikidata identity, a
 * census source id, a matrix subject (org:, inst:) or a publisher subject
 * (institution:publisher:).  Each resolves to the address this estate already
 * answers for -- the publisher's own subject host from matrix_domains, found
 * through the source frontier -- and otherwise to the identity page, and
 * otherwise stays what it was.  Nothing here creates an address: a reference
 * the estate holds no address for is reported unresolved, never invented.
 *
 * Reads BONFYRE_SOURCE_FRONTIER_DB and BONFYRE_MATRIX_DB (serve defaults both
 * to the estate directory).  Included by main.c before the page emitters. */

typedef struct {
    char root_id[64];   /* the frontier root the reference resolved through */
    char url[600];      /* where it opens: https://<subject host>/, /about/<Q>, or the given URL */
    char name[256];     /* what the estate calls it, when it knows */
    char host[200];     /* the publisher's own host, when known */
    char ident[48];     /* its Wikidata identity, when known */
    int estate;         /* 1 when url is an address this estate answers for */
} EstateReference;

static inline void estate_slug(const char *in, char *out, size_t cap) {
    size_t w = 0; int dash = 0;
    for (const unsigned char *p = (const unsigned char *)in; *p && w + 1 < cap && w < 48; p++) {
        if (isalnum(*p)) { out[w++] = (char)tolower(*p); dash = 0; }
        else if (!dash && w) { out[w++] = '-'; dash = 1; }
    }
    while (w && out[w - 1] == '-') w--;
    out[w] = '\0';
}

/* A host as a browser would read it: letters, digits, dots and hyphens only,
 * ended by the first character that ends an authority.  Anything else
 * (a backslash a browser reads as "/", userinfo, a space) makes it no host. */
static inline int estate_host_of(const char *url, char *out, size_t cap) {
    const char *p = strstr(url, "://");
    size_t w = 0;
    out[0] = '\0';
    if (!p) return 0;
    p += 3;
    if (!strncasecmp(p, "www.", 4)) p += 4;
    for (; *p && !strchr("/?#:", *p); p++) {
        unsigned char c = (unsigned char)*p;
        if (!(isalnum(c) || c == '.' || c == '-')) { out[0] = '\0'; return 0; }
        if (w + 1 >= cap) { out[0] = '\0'; return 0; }
        out[w++] = (char)tolower(c);
    }
    out[w] = '\0';
    return w > 0 && out[0] != '.' && out[w - 1] != '.' && !strstr(out, "..");
}

/* Two-letter country domains with a short second level ("co.uk", "com.au",
 * "ac.jp") are public suffixes: nothing above a registrable name is a
 * publisher of its own. */
static inline int estate_public_suffix(const char *host) {
    const char *dot = strchr(host, '.');
    if (!dot) return 1;
    if (strchr(dot + 1, '.')) return 0;
    return strlen(dot + 1) == 2 && (size_t)(dot - host) <= 3;
}

static inline int estate_valid_ident(const char *s) {
    return s && s[0] == 'Q' && s[1] && strspn(s + 1, "0123456789") == strlen(s + 1) && strlen(s) < 40;
}

/* The publisher's subject host for a root host ("sib.swiss"), as
 * address_every_publisher.mjs bound it: inst:<slug of the host>. */
/* The subject a publisher host stands for must read this root: "sib.swiss"
 * and "sib-swiss.example" share a slug, never a publisher.  Without a fabric
 * to ask, the slug binding stands. */
static inline int estate_host_reads_root(const char *slug, const char *root_id) {
    sqlite3 *fabric; sqlite3_stmt *st = NULL; char subject[96]; int ok = 1;
    if (!root_id || !*root_id || !(fabric = bf_sqlite_open_readonly_path(getenv("BONFYRE_SUBJECT_FABRIC_DB"), 1000))) return 1;
    snprintf(subject, sizeof(subject), "institution:publisher:%s", slug);
    if (sqlite3_prepare_v2(fabric, "SELECT EXISTS(SELECT 1 FROM subject_reader WHERE subject=?1 AND scope='public' AND root_id=?2), "
                                   "EXISTS(SELECT 1 FROM subject_reader WHERE subject=?1 AND scope='public' AND root_id<>'')", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, root_id, -1, SQLITE_TRANSIENT);
        /* Only a subject that records its roots can contradict the binding. */
        if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_int(st, 1)) ok = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); sqlite3_close(fabric);
    return ok;
}

static inline int estate_publisher_host(sqlite3 *matrix, const char *root_host, char *url, size_t cap) {
    char slug[64], inst[80]; sqlite3_stmt *st = NULL; int found = 0;
    if (!matrix || !root_host || !*root_host) return 0;
    if (!strncasecmp(root_host, "www.", 4)) root_host += 4;
    estate_slug(root_host, slug, sizeof(slug));
    if (!slug[0]) return 0;
    snprintf(inst, sizeof(inst), "inst:%s", slug);
    if (sqlite3_prepare_v2(matrix, "SELECT hostname FROM matrix_domains WHERE institution_id=?1 AND default_scope='public' "
                                   "ORDER BY canonical_hostname<>'' DESC, hostname LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, inst, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW && snprintf(url, cap, "https://%s/", sqlite3_column_text(st, 0)) < (int)cap) found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

typedef struct { sqlite3 *frontier, *matrix; } EstateReferenceStores;

static inline void estate_reference_open(EstateReferenceStores *s) {
    s->frontier = bf_sqlite_open_readonly_path(getenv("BONFYRE_SOURCE_FRONTIER_DB"), 1000);
    s->matrix = bf_sqlite_open_readonly_path(getenv("BONFYRE_MATRIX_DB"), 1000);
}
static inline void estate_reference_close(EstateReferenceStores *s) {
    if (s->frontier) sqlite3_close(s->frontier);
    if (s->matrix) sqlite3_close(s->matrix);
    s->frontier = s->matrix = NULL;
}

/* A frontier root by host (and each parent domain), by identity, or by the
 * census source one of its channels feeds. */
static inline int estate_root_by(sqlite3 *frontier, const char *sql, const char *key, EstateReference *r) {
    sqlite3_stmt *st = NULL; int found = 0;
    if (!frontier || sqlite3_prepare_v2(frontier, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *root = (const char *)sqlite3_column_text(st, 0), *host = (const char *)sqlite3_column_text(st, 1);
        const char *name = (const char *)sqlite3_column_text(st, 2), *ident = (const char *)sqlite3_column_text(st, 3);
        snprintf(r->root_id, sizeof(r->root_id), "%s", root ? root : "");
        snprintf(r->host, sizeof(r->host), "%s", host ? host : "");
        if (name && *name) snprintf(r->name, sizeof(r->name), "%s", name);
        if (estate_valid_ident(ident)) snprintf(r->ident, sizeof(r->ident), "%s", ident);
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

#define ESTATE_ROOT_COLUMNS "SELECT r.root_id, r.host, COALESCE(NULLIF(r.name,''),(SELECT title FROM channel c WHERE c.root_id=r.root_id AND COALESCE(c.title,'')<>'' " \
    "ORDER BY c.state IN ('admitted','active') DESC LIMIT 1),''), COALESCE(NULLIF(r.identity,''),(SELECT identity FROM channel c WHERE c.root_id=r.root_id AND COALESCE(c.identity,'')<>'' LIMIT 1),'') FROM root r "

static inline int estate_root_by_host(sqlite3 *frontier, const char *host, EstateReference *r) {
    for (const char *h = host; h && *h; ) {
        if (estate_public_suffix(h)) break;
        if (estate_root_by(frontier, ESTATE_ROOT_COLUMNS "WHERE r.host=?1 OR r.host='www.'||?1 ORDER BY r.identity<>'' DESC LIMIT 1", h, r)) return 1;
        const char *dot = strchr(h, '.');
        if (!dot || !strchr(dot + 1, '.')) break;   /* nothing above a registrable name */
        h = dot + 1;
    }
    return 0;
}

/* Resolve one reference. Returns 1 when it opens somewhere in this estate. */
static inline int estate_reference_resolve(EstateReferenceStores *s, const char *ref, EstateReference *r) {
    char host[200] = "";
    memset(r, 0, sizeof(*r));
    if (!ref || !*ref) return 0;
    if (!strncmp(ref, "http://", 7) || !strncmp(ref, "https://", 8)) {
        /* A reference longer than the address can hold is refused, never cut. */
        if (strlen(ref) >= sizeof(r->url) || !estate_host_of(ref, host, sizeof(host))) return 0;
        snprintf(r->url, sizeof(r->url), "%s", ref);
        /* An address of this estate is already where it opens. */
        if (strstr(host, ".aurekai.")) { r->estate = 1; return 1; }
        if (!estate_root_by_host(s->frontier, host, r)) return 0;
    } else if (estate_valid_ident(ref)) {
        if (!estate_root_by(s->frontier, ESTATE_ROOT_COLUMNS "WHERE r.identity=?1 OR r.root_id IN (SELECT root_id FROM channel WHERE identity=?1) LIMIT 1", ref, r))
            snprintf(r->ident, sizeof(r->ident), "%s", ref);
    } else if (!strncmp(ref, "org:", 4) || !strncmp(ref, "inst:", 5) || !strncmp(ref, "institution:publisher:", 22)) {
        const char *label = strrchr(ref, ':') + 1;
        char inst[96]; sqlite3_stmt *st = NULL;
        snprintf(inst, sizeof(inst), "inst:%s", label);
        if (s->matrix && sqlite3_prepare_v2(s->matrix, "SELECT hostname FROM matrix_domains WHERE institution_id=?1 AND default_scope='public' ORDER BY hostname LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, inst, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) { snprintf(r->url, sizeof(r->url), "https://%s/", sqlite3_column_text(st, 0)); r->estate = 1; }
        }
        sqlite3_finalize(st);
        return r->estate;
    } else if (strchr(ref, '.') && !strchr(ref, '/') && !strchr(ref, ' ')) {
        snprintf(host, sizeof(host), "%s", ref);
        if (!estate_root_by_host(s->frontier, host, r)) return 0;
    } else {
        /* A census source id: the root whose channel feeds it. */
        if (!estate_root_by(s->frontier, ESTATE_ROOT_COLUMNS "WHERE r.root_id=(SELECT root_id FROM channel WHERE census_source=?1 LIMIT 1)", ref, r)) return 0;
    }
    if (r->host[0]) {
        char slug[64];
        estate_slug(!strncasecmp(r->host, "www.", 4) ? r->host + 4 : r->host, slug, sizeof(slug));
        if (estate_host_reads_root(slug, r->root_id) && estate_publisher_host(s->matrix, r->host, r->url, sizeof(r->url))) { r->estate = 1; return 1; }
    }
    if (r->ident[0]) { snprintf(r->url, sizeof(r->url), "/about/%s", r->ident); r->estate = 1; return 1; }
    return r->estate;
}

/* An address this estate answers for opens in the same window. */
static inline int estate_address_is_ours(const char *href) {
    if (!href || !*href) return 0;
    if (href[0] == '/' && href[1] != '/') return 1;
    if (strncmp(href, "https://", 8)) return 0;
    char parsed[256];
    if (!estate_host_of(href, parsed, sizeof(parsed)) || !strncmp(href + 8, "www.", 4)) return 0;
    const char *host = parsed, *end = parsed + strlen(parsed);
    static const char *const ours[] = {".aurekai.life", ".aurekai.live", ".aurekai.work", ".aurekai.shop"};
    for (size_t i = 0; i < sizeof(ours) / sizeof(ours[0]); i++) {
        size_t n = strlen(ours[i]);
        if ((size_t)(end - host) >= n && !strncmp(end - n, ours[i], n)) return 1;
        if ((size_t)(end - host) == n - 1 && !strncmp(host, ours[i] + 1, n - 1)) return 1;
    }
    return 0;
}

/* Where a publisher known by its root host (and identity) opens here: its
 * subject host, or its identity page when the estate holds that identity.
 * Returns 1 with url set, or 0 when the estate answers for neither. */
static inline int estate_publisher_link(EstateReferenceStores *s, const char *host, const char *identity, char *url, size_t cap) {
    EstateReference r; char ref[300];
    /* Without stores of its own, a caller shares one read-only set per
     * worker process, opened on first use. */
    static EstateReferenceStores shared; static int opened;
    if (!s) { if (!opened) { estate_reference_open(&shared); opened = 1; } s = &shared; }
    url[0] = '\0';
    if (host && *host && snprintf(ref, sizeof(ref), "https://%s/", host) < (int)sizeof(ref) &&
        estate_reference_resolve(s, ref, &r) && r.estate && strlen(r.url) < cap) { snprintf(url, cap, "%s", r.url); return 1; }
    if (identity && *identity && estate_reference_resolve(s, identity, &r) && r.estate && strlen(r.url) < cap) { snprintf(url, cap, "%s", r.url); return 1; }
    return 0;
}
