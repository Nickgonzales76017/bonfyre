#ifndef BONFYRE_MATRIX_CLOSURE_BOOTSTRAP_H
#define BONFYRE_MATRIX_CLOSURE_BOOTSTRAP_H
/* Extracted verbatim from main.c lines 122-1016 (mechanical split, no logic changes). */
static int opt_positive_int(int argc, char **argv, const char *name,
                            int fallback, int ceiling) {
    const char *value = opt(argc, argv, name);
    char *end = NULL;
    long parsed;
    if (!value || !*value) return fallback;
    errno = 0;
    parsed = strtol(value, &end, 10);
    if (errno || end == value || *end || parsed < 1 || parsed > ceiling) return -1;
    return (int)parsed;
}
static int safe(const char *s, size_t max) { if (!s || !*s || strlen(s) >= max) return 0; for (; *s; s++) if (!(isalnum((unsigned char)*s) || strchr("._:-/@?=&%+", *s))) return 0; return 1; }
static BfMatrixWorld world_of(const char *s) { if (!s || !strcasecmp(s, "live")) return BF_MATRIX_LIVE; if (!strcasecmp(s, "shadow")) return BF_MATRIX_SHADOW; if (!strcasecmp(s, "replay")) return BF_MATRIX_REPLAY; if (!strcasecmp(s, "candidate")) return BF_MATRIX_CANDIDATE; if (!strcasecmp(s, "demo")) return BF_MATRIX_DEMO; return -1; }
static BfMatrixParticipationScope scope_of(const char *s) { static const char *v[] = {"self","household","group","community","public","market","institution"}; for (int i=0;i<7;i++) if (s && !strcasecmp(s,v[i])) return (BfMatrixParticipationScope)i; return -1; }
static const char *default_scope(const char *territory) { return territory && !strcmp(territory,"life") ? "self" : territory && !strcmp(territory,"live") ? "public" : territory && !strcmp(territory,"shop") ? "market" : territory && !strcmp(territory,"work") ? "institution" : "public"; }
static const char *repr_from_accept(const char *accept) { if(!accept)return NULL; if(strstr(accept,"application/mcp+json"))return "MCP"; if(strstr(accept,"application/a2a+json"))return "A2A"; if(strstr(accept,"application/vnd.bonfyre.fs+json"))return "BonfyreFS"; if(strstr(accept,"application/json"))return "API"; return NULL; }
static const char *content_type(const char *repr) { return !strcasecmp(repr,"MCP") ? "application/mcp+json" : !strcasecmp(repr,"A2A") ? "application/a2a+json" : !strcasecmp(repr,"BonfyreFS") ? "application/vnd.bonfyre.fs+json" : "application/json"; }
static const char *site_root(int argc, char **argv) { const char *root=opt(argc,argv,"--site-root"); return root && *root ? root : getenv("BONFYRE_SITE_ROOT"); }
/* The estate worker owns public apex delivery.  A member session moves to one
 * exact sibling hostname so a static response can never accidentally become
 * an authentication origin.  Those four names are deliberately not Matrix
 * subject domains, but an account page served there still needs the same
 * checked-in client runtime as its native fragments.  Do not widen this to a
 * suffix match: arbitrary subdomains must first become real AddressPlane
 * domains before they receive estate assets. */
static int native_member_session_host(const char *host) {
    return host && (!strcmp(host, "auth.aurekai.life") ||
                    !strcmp(host, "auth.aurekai.live") ||
                    !strcmp(host, "auth.aurekai.shop") ||
                    !strcmp(host, "signin.aurekai.work"));
}
static int session_valid(const char *bearer, const char *db);
static int studio_auth_user(const char *bearer, const char *auth_db, char *out, size_t out_sz);
static int session_authorized(const char *bearer, const char *db, const char *scope);
static int session_self_authoring(const char *bearer, const char *db);
static void html_escape(const char *in, char *out, size_t out_sz);
static int emit_native_donor_surface(const char *path, const char *kind, const char *territory,
const char *domain, const char *world, const char *community,
const char *bearer, const char *auth_db, const char *target, const char *query);
static int studio_safe_slug_arg(const char *text);
static int emit_native_world_page(const char *path, const char *target, const char *query);
static int emit_page_program_route(const char *root, const char *target, const char *host, const char *query);
#include "public_code.h"
static int emit_item_page(const char *root, const char *target, const char *host, const char *bearer, const char *auth_db);
static void wire_send(int socket_fd, FILE *captured, const char *accept_encoding, const char *if_none_match);
static const char *wire_site_root;
static int emit_item_calendar(const char *target, const char *host);
static int emit_agenda_calendar(const char *target, const char *host);
static int serve_page_changes(const char *root, const char *method, const char *target, const char *body, const char *raw,
                              const char *host, const char *bearer, const char *auth_db);
static int emit_about_page(const char *root, const char *target, const char *host, const char *bearer, const char *auth_db);
static int serve_page_save(const char *method, const char *target, const char *body, const char *raw,
                           const char *host, const char *bearer, const char *auth_db);
static int govern_act(const char *method, const char *target, const char *body, const char *raw,
                      const char *host, const char *bearer, const char *auth_db);
static int emit_saved_page(const char *root, const char *host, const char *bearer, const char *auth_db);
static int emit_parcel_page(const char *root, const char *target, const char *host, const char *bearer, const char *auth_db);
static int flow_safe_term(const char *in, char *out, size_t cap);
static void num_fix(char *v, size_t cap);
static void page_emit_property_results(const char *q);
static void page_title_case(const char *in, char *out, size_t cap);
static const char *page_grammar_label(const char *grammar);
static int emit_native_world_page_cached(const char *path, const char *target, const char *query);
static int emit_native_world_shelf(const char *path);
static int emit_native_world_shelf_cached(const char *path);
static int world_fragment_route_published(const char *world, const char *grammar);
static int studio_safe_slug_arg(const char *text);
static int listen_safe_asset(const char *asset);
/* Every control character is escaped, not only the newline: a calendar or feed
 * may carry a carriage return or a tab inside a value, and one raw control
 * character makes the whole line unreadable to whoever parses it. */
static void json(const char *s) { bf_lex_json_emit(stdout, s); }
static void json_file(FILE *file, const char *s) { bf_lex_json_emit(file, s); }
static void html(const char *s) { bf_lex_html_emit(stdout, s); }
/* Set while an operator's own site is being drawn; this server answers one
 * request at a time, and the flag is cleared when the page is finished. */
static int site_program_own_render = 0;


/* Public projections are compiled, disposable responses. They are deliberately
 * unavailable for actor-bearing calls: an anonymous response can be shared;
 * an authority closure cannot. Exact lowerer handoffs retire dependencies;
 * this coarse source fingerprint plus the cache's bounded source-TTL is only
 * an independent backstop, never a claim of source truth. */
typedef int (*BfProjectionEmitter)(void *ctx);
static const char *public_projection_cache_db(void) {
    const char *path = getenv("BONFYRE_PUBLIC_PROJECTION_DB");
    return path && *path ? path : NULL;
}
static void public_projection_fingerprint(char out[65], const char *first,
                                          const char *second, const char *third) {
    const char *cms = getenv("BONFYRE_CMS_DB");
    const char *foreign = getenv("BONFYRE_FOREIGN_ROOT");
    const char *paths[5];
    char seed[2048];
    size_t used = 0;
    if (!cms || !*cms) cms = "/var/lib/bonfyre/cms/bonfyre_cms.db";
    if (!foreign || !*foreign) foreign = "/var/lib/bonfyre/foreign";
    paths[0] = cms; paths[1] = foreign; paths[2] = first; paths[3] = second; paths[4] = third;
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); i++) {
        struct stat st;
        int n;
        if (!paths[i] || !*paths[i]) continue;
        memset(&st, 0, sizeof(st));
        (void)stat(paths[i], &st);
        n = snprintf(seed + used, sizeof(seed) - used, "%s|%lld|%lld|%lld;", paths[i],
                     (long long)st.st_mtime, (long long)st.st_size, (long long)st.st_ino);
        if (n < 0 || (size_t)n >= sizeof(seed) - used) break;
        used += (size_t)n;
    }
    bf_sha256_hex((const uint8_t *)seed, used, out);
}
static int public_projection_emit(BfProjectionEmitter emitter, void *ctx,
                                  const char *key, const char *fingerprint,
                                  const char *const *dependencies, size_t dependency_count) {
    const char *cache = public_projection_cache_db();
    FILE *capture;
    int saved, handled;
    long length;
    unsigned char *response = NULL;
    if (!cache || !key || !fingerprint) return emitter(ctx);
    if (bf_public_projection_cache_read(cache, key, fingerprint, stdout) == 1) return 1;
    capture = tmpfile();
    if (!capture) return emitter(ctx);
    fflush(stdout);
    saved = dup(STDOUT_FILENO);
    if (saved < 0 || dup2(fileno(capture), STDOUT_FILENO) < 0) {
        if (saved >= 0) close(saved);
        fclose(capture);
        return emitter(ctx);
    }
    handled = emitter(ctx);
    fflush(stdout);
    (void)dup2(saved, STDOUT_FILENO);
    close(saved);
    if (!handled) { fclose(capture); return 0; }
    if (fseek(capture, 0, SEEK_END) != 0 || (length = ftell(capture)) <= 0) {
        fclose(capture);
        return emitter(ctx);
    }
    if (length > 16 * 1024 * 1024 || !(response = malloc((size_t)length)) ||
        fseek(capture, 0, SEEK_SET) != 0 ||
        fread(response, 1, (size_t)length, capture) != (size_t)length) {
        free(response);
        (void)bf_public_projection_forward(capture, stdout);
        fclose(capture);
        return handled;
    }
    /* Errors and redirects may be transient; send them but never cache them. */
    if (bf_public_projection_response_cacheable(response, (size_t)length))
        (void)bf_public_projection_cache_write(cache, key, fingerprint, response, (size_t)length,
                                                dependencies, dependency_count, NULL);
    fwrite(response, 1, (size_t)length, stdout);
    free(response);
    fclose(capture);
    return handled;
}

/* A value going into a URL query, escaped for a URL rather than for HTML.
 *
 * html() is for text a browser will render; a query value needs percent
 * encoding, and using the wrong one produces a link that mostly works and
 * silently breaks on the first ampersand. */
static void url_query_value_to(FILE *stream, const char *v) {
    for (; v && *v; v++) {
        unsigned char c = (unsigned char)*v;
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') fputc(c, stream);
        else fprintf(stream, "%%%02X", c);
    }
}
static void url_query_value(const char *v) { url_query_value_to(stdout, v); }
static int url_query_buffer(const char *v, char *out, size_t cap) {
    static const char hex[] = "0123456789ABCDEF";
    size_t n = 0;
    for (const unsigned char *p = (const unsigned char *)v; p && *p; p++) {
        unsigned char c = *p;
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            if (n + 1 >= cap) return 0;
            out[n++] = (char)c;
        } else {
            if (n + 3 >= cap) return 0;
            out[n++] = '%'; out[n++] = hex[c >> 4]; out[n++] = hex[c & 15];
        }
    }
    if (!cap) return 0;
    out[n] = 0;
    return 1;
}

/* Compile one exact Matrix cut for a carrier. This knows address context only;
 * it deliberately does not read a session or import Authority, Evidence,
 * Provider, Resource, Work, or finance state from their owners. */
static int matrix_path_resolution(BfMatrix *m, const char *host_in,
                                  const char *path, BfMatrixAddressResolution *out) {
    char host[256], address[1600]; BfMatrixDomainResolution domain;
    if (!m || !out || !lower_host(host_in, host) ||
        bf_matrix_domain_resolve(m, host, &domain) != 1 ||
        !path || path[0] != '/' || strlen(path) >= 1000 ||
        snprintf(address, sizeof(address), "https://%s%s", host, path) >= (int)sizeof(address)) return 0;
    return bf_matrix_address_resolve(m, address, domain.territory, domain.world,
                                     domain.scope, domain.representation, out) == 1;
}

static int matrix_root_resolution(BfMatrix *m, const char *host_in,
                                  BfMatrixAddressResolution *out) {
    return matrix_path_resolution(m, host_in, "/", out);
}

static int closure_urls_for_path(const char *host_in, const char *path,
                                 char crate[1024], char zarr[1024]) {
    char host[256];
    const char *suffix = path && strcmp(path, "/") ? "?address=" : "";
    const char *value = path && strcmp(path, "/") ? path : "";
    if (!lower_host(host_in, host) || !path || path[0] != '/' || strlen(path) >= 900 ||
        snprintf(crate, 1024, "https://%s/ro-crate/ro-crate-metadata.json%s%s", host, suffix, value) >= 1024 ||
        snprintf(zarr, 1024, "https://%s/zarr/zarr.json%s%s", host, suffix, value) >= 1024) return 0;
    return 1;
}

static int closure_urls(const char *host_in, char crate[768], char zarr[768]) {
    char full_crate[1024], full_zarr[1024];
    if (!closure_urls_for_path(host_in, "/", full_crate, full_zarr) ||
        snprintf(crate, 768, "%s", full_crate) >= 768 ||
        snprintf(zarr, 768, "%s", full_zarr) >= 768) return 0;
    return 1;
}

static int closure_hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* The machine closure permits an exact Matrix address cut, never a URL to
 * fetch. Only a bounded path on the already-controlled Host is decoded. */
static int closure_query_path(const char *query, char path[1024]) {
    const char *start; size_t n = 0;
    if (!query || !*query) { snprintf(path, 1024, "/"); return 1; }
    start = strstr(query, "address=");
    if (!start || (start != query && start[-1] != '&')) return 0;
    start += 8;
    while (*start && *start != '&') {
        unsigned char value;
        if (n + 1 >= 1024) return 0;
        if (*start == '%') {
            int hi, lo;
            if (!start[1] || !start[2] || (hi = closure_hex_value(start[1])) < 0 || (lo = closure_hex_value(start[2])) < 0) return 0;
            value = (unsigned char)((hi << 4) | lo); start += 3;
        } else {
            value = (unsigned char)*start++;
        }
        if (value < 0x21 || value == '\\' || value == '?' || value == '#') return 0;
        path[n++] = (char)value;
    }
    path[n] = '\0';
    return n > 0 && path[0] == '/' && !strstr(path, "/../") && strcmp(path, "/..") && !strstr(path, "//");
}

static int bind_subject(BfMatrix *m, int argc, char **argv) {
    BfMatrixSubjectSpec s = { opt(argc,argv,"--id"),opt(argc,argv,"--kind"),opt(argc,argv,"--digest"),opt(argc,argv,"--evidence") };
    return bf_matrix_subject_bind(m,&s)==BF_MATRIX_OK ? 0 : 2;
}
static int bind_address(BfMatrix *m, int argc, char **argv) { return bf_matrix_address_bind(m,opt(argc,argv,"--subject"),opt(argc,argv,"--kind"),opt(argc,argv,"--address"),opt(argc,argv,"--evidence"))==BF_MATRIX_OK ? 0 : 2; }
static int bind_alias(BfMatrix *m, int argc, char **argv) { return bf_matrix_address_alias(m,opt(argc,argv,"--alias"),opt(argc,argv,"--canonical"),opt(argc,argv,"--evidence"))==BF_MATRIX_OK ? 0 : 2; }
/* Register an institution and the world it executes in.
 *
 * bf_matrix_institution_bind has existed in the engine with no way to call it,
 * so matrix_institutions was empty while all twenty-two bound hosts carried an
 * institution_id: every host claimed a reference to a registry with nothing in
 * it. Domain binding checks that reference, which means re-binding any existing
 * host would have failed, and the DEMO world could not be given to an
 * institution because no institution row existed to hold it.
 *
 * The world lives here rather than only on the domain because an institution
 * executes in one world across every host it serves. A per-host world would let
 * the same institution be DEMO on one address and LIVE on another, which is the
 * confusion the worlds contract exists to prevent.
 *
 * Cannot infer: a registered institution -> an authorized or populated one.
 * This records that the institution exists and which world it executes in. */
static void iso_utc_now(char *buf, size_t sz);

/* Withdraw a presence.
 *
 * Registration and activation are different acts, and conflating them is how
 * every observed favicon acquired an active public presence. Registering says a
 * stable foreign identity exists. Activating says it is projected into a world,
 * a territory and a scope -- that it is *present*. Withdrawal is the inverse of
 * the second, and like the second it is authority-gated: turning a subject off
 * in the world is as much an act as turning it on.
 *
 * Cannot infer: a withdrawn presence -> a deleted subject. The identity remains
 * registered and every observation that produced it remains recorded; what ends
 * is the projection. */
static int withdraw_presence(BfMatrix *m, int argc, char **argv) {
    const char *authdb = opt(argc, argv, "--authority-db");
    const char *actor = opt(argc, argv, "--actor");
    const char *pid = opt(argc, argv, "--id");
    const char *ref = opt(argc, argv, "--authority-ref");
    const char *purpose = opt(argc, argv, "--purpose");
    BfAuthority *auth = NULL;
    BfMatrixStatus st;
    char stamp[32];
    if (!authdb || !*authdb) authdb = "/var/lib/bonfyre/auth.db";
    if (!pid || !actor) { fprintf(stderr, "--id and --actor required\n"); return 2; }
    if (bf_authority_open(authdb, &auth, stderr) || !auth) {
        fprintf(stderr, "cannot open authority at %s\n", authdb);
        return 2;
    }
    iso_utc_now(stamp, sizeof(stamp));
    st = bf_matrix_presence_withdraw(m, auth, actor, pid,
                                     ref ? ref : "registration-is-not-activation",
                                     stamp, purpose ? purpose : "deactivate");
    bf_authority_close(auth);
    if (st != BF_MATRIX_OK) {
        fprintf(stderr, "presence withdraw refused: %s\n", bf_matrix_status_name(st));
        return 2;
    }
    return 0;
}

static int bind_relation(BfMatrix *m, int argc, char **argv) {
    BfMatrixStatus st = bf_matrix_subject_relate(m,
        opt(argc, argv, "--from"), opt(argc, argv, "--relation"),
        opt(argc, argv, "--to"), opt(argc, argv, "--evidence"));
    if (st != BF_MATRIX_OK) {
        fprintf(stderr, "relation refused: %s\n", bf_matrix_status_name(st));
        return 2;
    }
    return 0;
}

static int bind_institution(BfMatrix *m, int argc, char **argv) {
    BfMatrixInstitutionSpec spec = {
        opt(argc, argv, "--id"),
        opt(argc, argv, "--kind"),
        world_of(opt(argc, argv, "--world")),
        opt(argc, argv, "--evidence")
    };
    BfMatrixStatus st = bf_matrix_institution_bind(m, &spec);
    if (st != BF_MATRIX_OK) {
        fprintf(stderr, "institution bind refused: %s\n", bf_matrix_status_name(st));
        return 2;
    }
    return 0;
}

static int bind_domain(BfMatrix *m, int argc, char **argv) {
    const char *host=opt(argc,argv,"--host"), *territory=opt(argc,argv,"--territory"), *scope=opt(argc,argv,"--scope"), *world=opt(argc,argv,"--world"), *repr=opt(argc,argv,"--repr"); char normalized[256];
    BfMatrixDomainSpec d;
    if (!lower_host(host,normalized)) return 2;
    d=(BfMatrixDomainSpec){normalized,opt(argc,argv,"--institution"),territory,scope_of(scope),world_of(world),repr,opt(argc,argv,"--canonical-host"),opt(argc,argv,"--evidence")};
    {
        /* This returned 2 and printed nothing, so an unregistered institution
         * and a malformed world were indistinguishable from a usage error. */
        BfMatrixStatus st = bf_matrix_domain_bind(m, &d);
        if (st != BF_MATRIX_OK) {
            fprintf(stderr, "domain bind refused: %s\n", bf_matrix_status_name(st));
            return 2;
        }
    }
    return 0;
}
static int bootstrap_owned(BfMatrix *m) {
    static const struct { const char *host, *territory, *scope, *subject, *kind, *digest; } hosts[] = {
        {"aurekai.life", "life", "self", "institution:aurekai:life", "HumanTerritory", "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"},
        {"aurekai.live", "live", "public", "institution:aurekai:live", "HumanTerritory", "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"},
        {"aurekai.shop", "shop", "market", "institution:aurekai:shop", "HumanTerritory", "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"},
        {"aurekai.work", "work", "institution", "institution:aurekai:work", "HumanTerritory", "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4"},
        /* The Observatory is a Work-territory institution with a deliberately
         * public read aperture.  It is not a new generic app or a suffix rule:
         * this exact controlled host resolves to one Matrix subject. */
        {"internet.aurekai.work", "work", "public", "institution:aurekai:internet", "Institution", "f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6"},
        {"theleaflounge.store", "independent", "public", "institution:leaf-lounge", "Institution", "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5"}
    };
    static const char *internet_views[] = {
        "/agents", "/services", "/protocols", "/gaps", "/changes", "/sources",
        "/agents/auth-md", "/agents/a2a", "/agents/mcp", "/agents/multi-protocol", "/agents/workos-auth",
        "/services/agent-registerable", "/services/a2a-without-authmd", "/services/mcp-without-a2a", "/services/authmd-without-a2a",
        "/services/machine-readable", "/services/machine-purchasable", "/services/protocol-diverged", "/services/stale-manifest",
        "/protocols/auth-md", "/protocols/a2a", "/protocols/mcp", "/protocols/oauth", "/protocols/webfinger", "/protocols/activitypub",
        "/protocols/x402", "/protocols/mpp", "/protocols/ucp", "/protocols/acp", "/protocols/beckn", "/protocols/wot", "/protocols/ogc",
        "/gaps/auth", "/gaps/discovery", "/gaps/commerce", "/gaps/identity", "/gaps/protocol", "/gaps/interoperability",
        "/changes/today", "/changes/week", "/changes/new", "/changes/removed", "/changes/changed",
        "/sources/registries", "/sources/common-crawl", "/sources/search", "/sources/github", "/sources/dns-ct", "/sources/feeds", "/sources/government", "/sources/direct",
        "/operators"
    };
    size_t i; int inserted=0;
    for (i=0;i<sizeof(hosts)/sizeof(hosts[0]);++i) {
        BfMatrixDomainResolution existing; int exists;
        BfMatrixDomainSpec domain={hosts[i].host,NULL,hosts[i].territory,scope_of(hosts[i].scope),BF_MATRIX_LIVE,"HTML",NULL,"configured:owned-domain"};
        BfMatrixSubjectSpec subject={hosts[i].subject,hosts[i].kind,hosts[i].digest,"configured:owned-domain"};
        BfMatrixPresenceSpec presence;
        char address[320], presence_id[320];
        BfMatrixStatus status;
        exists=bf_matrix_domain_resolve(m,hosts[i].host,&existing); if(exists<0)return 2;
        if (!(exists==1 && !strcmp(existing.matched_hostname,hosts[i].host))) { if (bf_matrix_domain_bind(m,&domain) != BF_MATRIX_OK) return 2; inserted++; }
        if (bf_matrix_subject_bind(m,&subject) != BF_MATRIX_OK) return 2;
        if (snprintf(address,sizeof(address),"https://%s/",hosts[i].host)>=(int)sizeof(address) || snprintf(presence_id,sizeof(presence_id),"presence:%s:root",hosts[i].subject)>=(int)sizeof(presence_id)) return 2;
        if (bf_matrix_address_bind(m,hosts[i].subject,"web_url",address,"configured:owned-domain") != BF_MATRIX_OK) return 2;
        presence=(BfMatrixPresenceSpec){presence_id,hosts[i].subject,NULL,hosts[i].territory,scope_of(hosts[i].scope),BF_MATRIX_LIVE,"HTML","configured:owned-domain"};
        status=bf_matrix_presence_bind(m,&presence); if(status != BF_MATRIX_OK && status != BF_MATRIX_DUPLICATE_PRESENCE) return 2;
    }
    /* These are maintained-query coordinates, not copied folders.  Binding a
     * coordinate lets every carrier resolve the same exact Observatory cut;
     * membership remains with the discovery owner and starts honestly empty. */
    for (i = 0; i < sizeof(internet_views) / sizeof(internet_views[0]); i++) {
        char address[512];
        if (snprintf(address, sizeof(address), "https://internet.aurekai.work%s", internet_views[i]) >= (int)sizeof(address) ||
            bf_matrix_address_bind(m, "institution:aurekai:internet", "standing_query", address,
                                   "configured:internet-observatory-view") != BF_MATRIX_OK) return 2;
    }
    printf("{\"bootstrapped_owned_domains\":%d}\n",inserted); return 0;
}
static int bind_standing(BfMatrix *m, int argc, char **argv) {
    const char *scope=opt(argc,argv,"--scope"), *world=opt(argc,argv,"--world");
    BfMatrixStandingAddressSpec s={opt(argc,argv,"--address"),opt(argc,argv,"--subject-kind"),opt(argc,argv,"--territory"),scope ? (int)scope_of(scope) : -1,world ? (int)world_of(world) : -1,opt(argc,argv,"--repr"),opt(argc,argv,"--evidence")};
    return bf_matrix_standing_address_bind(m,&s)==BF_MATRIX_OK ? 0 : 2;
}
static int standing_members(BfMatrix *m, int argc, char **argv) {
    const char *address=opt(argc,argv,"--address"); int members=0,rc=bf_matrix_standing_address_member_count(m,address,&members);
    if(rc != 1) return rc == 0 ? 1 : 2;
    printf("{\"address\":");json(address);printf(",\"members\":%d}\n",members); return 0;
}
static int bind_presence(BfMatrix *m, int argc, char **argv) {
    const char *territory=opt(argc,argv,"--territory"),*scope=opt(argc,argv,"--scope"),*world=opt(argc,argv,"--world"),*repr=opt(argc,argv,"--repr");
    BfMatrixPresenceSpec p={opt(argc,argv,"--id"),opt(argc,argv,"--subject"),NULL,territory,scope_of(scope),world_of(world),repr,opt(argc,argv,"--evidence")};
    return bf_matrix_presence_bind(m,&p)==BF_MATRIX_OK ? 0 : 2;
}
static int bind_closure_contribution(BfMatrix *m, int argc, char **argv) {
    BfMatrixClosureContributionSpec value = {
        opt(argc,argv,"--subject"), opt(argc,argv,"--presence"), opt(argc,argv,"--plane"),
        opt(argc,argv,"--owner"), opt(argc,argv,"--resource"), opt(argc,argv,"--representation"),
        opt(argc,argv,"--digest"), opt(argc,argv,"--evidence")
    };
    return bf_matrix_closure_contribution_bind(m, &value) == BF_MATRIX_OK ? 0 : 2;
}
typedef struct ContributionCliOut { int first; } ContributionCliOut;
static int emit_closure_contribution(const BfMatrixClosureContributionSpec *value, void *context) {
    ContributionCliOut *out = context;
    if (!out->first) putchar(',');
    out->first = 0;
    printf("{\"plane\":"); json(value->plane);
    printf(",\"owner\":"); json(value->owner);
    printf(",\"resource\":"); json(value->resource_uri);
    printf(",\"representation\":"); json(value->representation);
    printf(",\"digest\":"); json(value->digest);
    printf(",\"evidence\":"); json(value->evidence_ref);
    printf("}"); return 0;
}
static int closure_contributions_cli(BfMatrix *m, int argc, char **argv) {
    const char *subject = opt(argc,argv,"--subject"), *presence = opt(argc,argv,"--presence"), *plane = opt(argc,argv,"--plane");
    ContributionCliOut out = {1}; int count;
    if (!subject || !presence) return 2;
    printf("{\"subject\":"); json(subject); printf(",\"presence\":"); json(presence); printf(",\"contributions\":[");
    count = bf_matrix_closure_contributions(m, subject, presence, plane, emit_closure_contribution, &out);
    if (count < 0) return 2;
    printf("]}\n"); return 0;
}
typedef struct ConstellationOut { int first; } ConstellationOut;
static int emit_constellation_address(const char *address, const char *kind, const char *canonical, const char *evidence, void *context) {
    ConstellationOut *out=context; (void)evidence;
    if (!out->first) putchar(',');
    out->first=0;
    printf("{\"kind\":");json(kind);printf(",\"address\":");json(address);printf(",\"canonical_address\":");json(canonical);printf("}"); return 0;
}
static int constellation(BfMatrix *m, int argc, char **argv) {
    const char *subject=opt(argc,argv,"--subject"); ConstellationOut out={1}; int count;
    if (!subject) return 2;
    printf("{\"subject_id\":");json(subject);printf(",\"addresses\":["); count=bf_matrix_subject_addresses(m,subject,emit_constellation_address,&out); if(count<0)return 2; printf("]}\n"); return count ? 0 : 1;
}
/* Static assets, served so a second page view does not pay for them again.
 *
 * This sent `Cache-Control: no-cache` and no validator at all. no-cache means
 * revalidate before reuse, and with nothing to revalidate against every
 * navigation re-downloaded the whole body: 61KB of htmx, 517ms on the measured
 * load, with the page's nav and panels blocked behind it because they arrive as
 * htmx fragments. Every page view paid it. That is the slowness.
 *
 * The ETag is the content's own digest, so it changes when and only when the
 * bytes do. stale-while-revalidate lets the browser paint from cache
 * immediately and refresh in the background, which bounds staleness after a
 * deploy to a single view instead of trading correctness for speed. */
static int emit_file_response_cached(const char *root, const char *name, const char *type,
                                     const char *inm, const char *versioned) {
    char path[1024]; FILE *file; long size;
    static char body[4 * 1024 * 1024];
    char digest[65], etag[32];
    if (!root || !*root || !name || strchr(name,'/')) return 0;
    if (snprintf(path,sizeof(path),"%s/%s",root,name)>=(int)sizeof(path) || !(file=fopen(path,"rb"))) return 0;
    if (fseek(file,0,SEEK_END)!=0 || (size=ftell(file))<0 || (size_t)size>sizeof(body) ||
        fseek(file,0,SEEK_SET)!=0) { fclose(file); return 0; }
    if (fread(body,1,(size_t)size,file)!=(size_t)size) { fclose(file); return 0; }
    fclose(file);
    bf_sha256_hex((const uint8_t *)body, (size_t)size, digest);
    /* The ETag is a 16-char prefix of the digest, so the comparison has to be
     * against that prefix. Matching the full 64-char digest inside a 16-char
     * header never succeeds, and every conditional request answers 200 with
     * the whole body -- the exact failure this function exists to remove,
     * reintroduced one line lower. */
    /* %.16s, not a truncated buffer: gcc cannot prove the digest is short and
     * rejects the format as possibly truncating. Say the width in the format. */
    snprintf(etag, sizeof(etag), "\"%.16s\"", digest);
    digest[16] = '\0';
    /* A conditional request matches on the digest itself; the client may send
     * it weak, quoted, or in a list, so look for the digest inside whatever it
     * sent rather than requiring an exact header match. */
    if (inm && *inm && strstr(inm, digest)) {
        printf("HTTP/1.1 304 Not Modified\r\nETag: %s\r\n"
               "Cache-Control: public, max-age=60, stale-while-revalidate=60\r\n"
               "Connection: close\r\n\r\n", etag);
        return 1;
    }
    /* A caller that named the digest asked for exactly these bytes and can
     * hold them forever. A caller that named the bare path gets a short life,
     * because those bytes change under that name. */
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nETag: %s\r\n"
           "Cache-Control: %s\r\n"
           "Content-Length: %ld\r\nConnection: close\r\n\r\n", type, etag,
           (versioned && *versioned && strstr(versioned, digest))
             ? "public, max-age=31536000, immutable"
             : "public, max-age=60, stale-while-revalidate=60",
           size);
    fwrite(body, 1, (size_t)size, stdout);
    return 1;
}



/* The kit and the page that loads it must not be able to disagree.
 *
 * The page is served no-cache and the kit was served with a day of
 * stale-while-revalidate, so a freshly deployed page could run against a kit
 * from yesterday -- and when the page called something the old kit did not
 * have, the whole script threw and the surface rendered as a skeleton. That is
 * not a stale view; it is a broken one, and it lasted until a reader happened
 * to hard-refresh.
 *
 * So the page names the kit by the kit's own digest. New bytes are a new URL,
 * which no cache can answer from an old entry, and the long cache lifetime
 * becomes correct instead of dangerous.
 *
 * The token is written the same width as its replacement so the substitution
 * is in place: no second buffer, no length arithmetic, and nothing to get
 * wrong on a path that serves every surface.
 */
#define BF_KIT_TOKEN "BFKITVERSIONXXXX"     /* 16 chars, replaced by 16 hex */

static void kit_version_into(const char *surface_path, char *body) {
    static char cached[17];
    char kit[1024], digest[65];
    const char *cut;
    FILE *f;
    long size;
    static char buf[1024 * 1024];
    char *at;

    if (!body || !strstr(body, BF_KIT_TOKEN)) return;
    if (!cached[0]) {
        /* <root>/surfaces/<name>.html -> <root>/bonfyre-kit.js */
        cut = surface_path ? strstr(surface_path, "/surfaces/") : NULL;
        if (!cut) return;
        if (snprintf(kit, sizeof(kit), "%.*s/bonfyre-kit.js",
                     (int)(cut - surface_path), surface_path) >= (int)sizeof(kit)) return;
        if (!(f = fopen(kit, "rb"))) return;
        if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 1 ||
            (size_t)size > sizeof(buf) || fseek(f, 0, SEEK_SET) != 0) { fclose(f); return; }
        if (fread(buf, 1, (size_t)size, f) != (size_t)size) { fclose(f); return; }
        fclose(f);
        bf_sha256_hex((const uint8_t *)buf, (size_t)size, digest);
        memcpy(cached, digest, 16);
        cached[16] = '\0';
    }
    /* Every occurrence: a surface may load the kit more than once. */
    for (at = strstr(body, BF_KIT_TOKEN); at; at = strstr(at, BF_KIT_TOKEN))
        memcpy(at, cached, 16);
}

static const char *site_nav_links(void);
static void internet_context(const BfMatrixAddressResolution *resolved);
static void emit_resolved_identity_metadata(const BfMatrixAddressResolution *resolved);

static const BfNativeTemplateSlot *native_template_slot(const char *template_name, unsigned short slot) {
    size_t i;
    for (i = 0; i < BF_NATIVE_TEMPLATE_SLOT_COUNT; i++)
        if (!strcmp(bf_native_template_slots[i].template_name, template_name) &&
            bf_native_template_slots[i].slot == slot) return &bf_native_template_slots[i];
    return NULL;
}

static int native_template_marker(char *out, size_t out_sz, const char *template_name,
                                  unsigned short slot, BfTemplateOpcode opcode) {
    const BfNativeTemplateSlot *declared = native_template_slot(template_name, slot);
    if (!out || !out_sz || !declared || declared->opcode != opcode) return 0;
    return snprintf(out, out_sz, "<!--BF_TEMPLATE_SLOT:%u-->", slot) < (int)out_sz;
}

/* A /w/<slug> page still owns its visual DOM, but it no longer gets to infer
 * composition from the authored comma-list. This tiny client consumes the
 * native plan after the page has mounted and replaces only the view rail. */
/* All human HTML projections can name the same machine closure without asking
 * the browser to reconstruct anything from JSON. The links are carrier
 * metadata for agents/crawlers; HTMX continues to receive server-rendered
 * fragments. */
static sqlite3 *cms_open_ro(void);

static int emit_closure_linked_html(BfMatrix *m, const char *path, const char *host,
                                    const char *target, const char *query) {
    FILE *file; long size; char *body, *head; char crate[768], zarr[768];
    BfMatrixAddressResolution resolved;
    int has_resolution = m && target && matrix_path_resolution(m, host, target, &resolved);
    char *identity;
    char identity_marker[40], nav_marker[40];
    const char *template_name = path && strstr(path, "/surfaces/home.html") ? "home" : "static";
    if (!path || !host || !closure_urls(host, crate, zarr)) return 0;
    /* Discover and Stage are authored Pixelfed/Mobilizon grammars.  Their
     * public document is lowered before send from published World state, not
     * filled by the old browser renderer and not replaced with a generic
     * catalogue page. */
    /* The old donor-shell experiment remains intentionally inactive until
     * SitePresentation can supply real typed application bindings. A native
     * document built from demo subjects would only replace one catalogue with
     * another, which is precisely what this boundary is preventing. */
    /* A World is not a browser application.  Its authored character is a
     * server-side grammar, and its observed material is lowered before the
     * document crosses the network.  The static file still supplies the
     * studied CSS grammar; its retired script never gets to reconstruct a
     * world from raw census rows. */
    if (target && (!strcmp(target, "/w") || !strcmp(target, "/w/")) &&
        emit_native_world_shelf_cached(path)) return 1;
    if (target && !strncmp(target, "/w/", 3) && target[3] &&
        emit_native_world_page_cached(path, target, query)) return 1;
    if (!(file = fopen(path, "rb"))) return 0;
    if (fseek(file, 0, SEEK_END) != 0 || (size = ftell(file)) < 1 || size > 2 * 1024 * 1024 ||
        fseek(file, 0, SEEK_SET) != 0) { fclose(file); return 0; }
    body = malloc((size_t)size + 1);
    if (!body) { fclose(file); return 0; }
    if (fread(body, 1, (size_t)size, file) != (size_t)size) { free(body); fclose(file); return 0; }
    fclose(file); body[size] = '\0';
    kit_version_into(path, body);
    /* The current CMS authoring app shares the estate's signed-in shell.
     * It remains a cached public document with a private, session-checked
     * navigation slot. No principal, credential or draft enters this HTML. */
    if (target && (!strcmp(target,"/cms") || !strcmp(target,"/cms/"))) {
        char *start = strstr(body,"<body>");
        if (start) {
            const size_t prefix = (size_t)(start + 6 - body);
            const size_t barlen = sizeof(BF_ESTATE_BAR) - 1;
            if ((size_t)size <= 2 * 1024 * 1024 - barlen) {
                char *revised = malloc((size_t)size + barlen + 1);
                if (!revised) { free(body); return 0; }
                memcpy(revised,body,prefix);
                memcpy(revised+prefix,BF_ESTATE_BAR,barlen);
                memcpy(revised+prefix+barlen,body+prefix,(size_t)size-prefix+1);
                free(body);body=revised;size+=(long)barlen;
            }
        }
    }
    if (has_resolution && target && !strcmp(target, "/")) {
        sqlite3 *presentation_db = cms_open_ro();
        int presentation_ok = bf_apply_territory_home_presentation(presentation_db, &body, resolved.territory);
        if (presentation_db) sqlite3_close(presentation_db);
        if (!presentation_ok) { free(body); return 0; }
    }
    head = strstr(body, "</head>");
    if (!head) { free(body); return 0; }
    identity = has_resolution && native_template_marker(identity_marker, sizeof(identity_marker), template_name, 0, BF_TEMPLATE_RESOLVED_IDENTITY) ? strstr(body, identity_marker) : NULL;
    /* A surface that carries the marker gets the authored navigation rendered
     * into it, server side. The landing page needs it in the bytes rather than
     * after a fetch: a crawler and a reader without script both have to be
     * able to reach every surface this institution serves. */
    {
        char *slot = native_template_marker(nav_marker, sizeof(nav_marker), template_name, 1, BF_TEMPLATE_AUTHORED_NAVIGATION) ? strstr(body, nav_marker) : NULL;
        if (slot) {
            const char *links = site_nav_links();
            if (links && *links) {
                printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                       "Cache-Control: no-cache\r\nVary: Host\r\nConnection: close\r\n\r\n");
                if (identity && identity < head) {
                    fwrite(body, 1, (size_t)(identity - body), stdout);
                    emit_resolved_identity_metadata(&resolved);
                    fwrite(identity + strlen(identity_marker), 1,
                           (size_t)(head - (identity + strlen(identity_marker))), stdout);
                } else fwrite(body, 1, (size_t)(head - body), stdout);
                fputs("<link rel=\"describedby\" type=\"application/ld+json\" href=\"", stdout);
                html(crate);
                fputs("\"><link rel=\"alternate\" type=\"application/vnd.zarr+json\" href=\"", stdout);
                html(zarr);
                fputs("\">", stdout);
                if (has_resolution) internet_context(&resolved);
                /* head .. slot, then the links, then the rest. */
                fwrite(head, 1, (size_t)(slot - head), stdout);
                fputs(links, stdout);
                fputs(slot + strlen(nav_marker), stdout);
                free(body);
                return 1;
            }
        }
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: no-cache\r\nVary: Host\r\nConnection: close\r\n\r\n");
    if (identity && identity < head) {
        fwrite(body, 1, (size_t)(identity - body), stdout);
        emit_resolved_identity_metadata(&resolved);
        fwrite(identity + strlen(identity_marker), 1,
               (size_t)(head - (identity + strlen(identity_marker))), stdout);
    } else fwrite(body, 1, (size_t)(head - body), stdout);
    fputs("<link rel=\"describedby\" type=\"application/ld+json\" href=\"", stdout); html(crate);
    fputs("\"><link rel=\"alternate\" type=\"application/vnd.zarr+json\" href=\"", stdout); html(zarr);
    fputs("\">", stdout);
    if (has_resolution) internet_context(&resolved);
    fputs(head, stdout); free(body); return 1;
}

/* What each resolved audience is, in words a fetcher can read.
 *
 * The four owned hosts resolve to four audiences and then rendered one page
 * whose identity tags were hard-coded to aurekai.life -- including
 * <link rel="canonical">, which told every crawler that .live, .shop and .work
 * were duplicates of .life. Three territories were suppressing themselves, and
 * the one signal that distinguished them lived inside a <script> that no social
 * preview fetcher executes. The membrane counted 332 preview requests answered
 * with nothing.
 *
 * The audience is already resolved before the page is written. This is the
 * server saying which territory it resolved, in tags read without executing
 * anything.
 *
 * Cannot infer: a description states what a territory is for. It is not a claim
 * that anything is currently offered there, and never a price. */
static const BfNativeAudiencePresentation *audience_copy(const char *scope) {
    size_t i;
    if (scope) for (i = 0; i < BF_NATIVE_AUDIENCE_PRESENTATION_COUNT; i++)
        if (!strcmp(scope, bf_native_audience_presentations[i].scope)) return &bf_native_audience_presentations[i];
    /* An unmapped scope gets the neutral institution-free wording rather than
     * another territory's, which would be a claim about the wrong audience. */
    return NULL;
}

/* The public home is a visual compiler output, but its title, discovery
 * metadata and canonical address come from the resolved Matrix presence. */
static void emit_resolved_identity_metadata(const BfMatrixAddressResolution *resolved) {
    const char *scope = resolved ? bf_matrix_scope_name(resolved->scope) : NULL;
    const BfNativeAudiencePresentation *copy = audience_copy(scope);
    const char *title = copy ? copy->title : "Bonfÿre Matrix";
    const char *desc = copy ? copy->description :
        "Receipted observation, work and capability — legible to people and to machines.";
    const char *canon = resolved && *resolved->canonical_address ? resolved->canonical_address : NULL;
    fputs("<title>", stdout); html(title); fputs("</title>", stdout);
    fputs(BF_AUREKAI_FAVICON_LINK, stdout);
    fputs("<meta name=\"description\" content=\"", stdout); html(desc); fputs("\">", stdout);
    fputs("<meta property=\"og:title\" content=\"", stdout); html(title); fputs("\">", stdout);
    fputs("<meta property=\"og:description\" content=\"", stdout); html(desc); fputs("\">", stdout);
    if (canon) {
        fputs("<meta property=\"og:url\" content=\"", stdout); html(canon); fputs("\">", stdout);
        fputs("<link rel=\"canonical\" href=\"", stdout); html(canon); fputs("\">", stdout);
    }
    fputs("<meta name=\"twitter:title\" content=\"", stdout); html(title); fputs("\">", stdout);
    fputs("<meta name=\"twitter:description\" content=\"", stdout); html(desc); fputs("\">", stdout);
}

/* The matrix root is a real resolved subject, not a brand-neutral static page.
 *
 * `index.html` remains the reusable visual grammar, but the server injects the
 * exact resolved address context before it is rendered.  This preserves the
 * useful separation: SurfaceIR may vary by host/presence without each host
 * becoming a hand-maintained application or a copy of the subject. */
static int emit_matrix_index(const char *root, const BfMatrixAddressResolution *out) {
    char path[1024], marker[40]; FILE *file; long size; char *body, *slot;
    if (!root || !*root || !out) return 0;
    if (snprintf(path, sizeof(path), "%s/index.html", root) >= (int)sizeof(path)) return 0;
    file = fopen(path, "rb");
    if (!file) return 0;
    if (fseek(file, 0, SEEK_END) != 0 || (size = ftell(file)) < 1 || size > 1024 * 1024 ||
        fseek(file, 0, SEEK_SET) != 0) { fclose(file); return 0; }
    body = (char *)malloc((size_t)size + 1);
    if (!body) { fclose(file); return 0; }
    if (fread(body, 1, (size_t)size, file) != (size_t)size) { free(body); fclose(file); return 0; }
    fclose(file); body[size] = '\0';
    if (!native_template_marker(marker, sizeof(marker), "matrix_index", 0, BF_TEMPLATE_MATRIX_CONTEXT)) { free(body); return 0; }
    slot = strstr(body, marker);
    if (!slot) { free(body); return 0; }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Cache-Control: no-cache\r\nConnection: close\r\n\r\n");
    fwrite(body, 1, (size_t)(slot - body), stdout);
    fputs("<link rel=\"describedby\" type=\"application/ld+json\" href=\"/ro-crate/ro-crate-metadata.json\">"
          "<link rel=\"alternate\" type=\"application/vnd.zarr+json\" href=\"/zarr/zarr.json\">", stdout);
    {
        /* Identity, emitted per resolved host. These tags used to be written
         * into index.html pointing at one host, so three of four territories
         * declared themselves duplicates of the fourth. */
        const char *scope = bf_matrix_scope_name(out->scope);
        const BfNativeAudiencePresentation *copy = audience_copy(scope);
        const char *title = copy ? copy->title : "Bonfÿre Matrix";
        const char *desc  = copy ? copy->description
                                 : "Receipted observation, work and capability — legible to people and to machines.";
        const char *canon = *out->canonical_address ? out->canonical_address : NULL;
        fputs("<title>", stdout); html(title); fputs("</title>", stdout);
        fputs(BF_AUREKAI_FAVICON_LINK, stdout);
        fputs("<meta name=\"description\" content=\"", stdout); html(desc); fputs("\">", stdout);
        fputs("<meta property=\"og:title\" content=\"", stdout); html(title); fputs("\">", stdout);
        fputs("<meta property=\"og:description\" content=\"", stdout); html(desc); fputs("\">", stdout);
        if (canon) {
            fputs("<meta property=\"og:url\" content=\"", stdout); html(canon); fputs("\">", stdout);
            fputs("<link rel=\"canonical\" href=\"", stdout); html(canon); fputs("\">", stdout);
        }
        fputs("<meta name=\"twitter:title\" content=\"", stdout); html(title); fputs("\">", stdout);
        fputs("<meta name=\"twitter:description\" content=\"", stdout); html(desc); fputs("\">", stdout);
    }
    fputs("<script>window.BonfyreMatrixContext={\"subject\":", stdout); json(out->subject_id);
    fputs(",\"presence\":", stdout); json(out->presence_id);
    fputs(",\"territory\":", stdout); json(out->territory);
    fputs(",\"world\":", stdout); json(bf_matrix_world_name(out->world));
    fputs(",\"audience\":", stdout); json(bf_matrix_scope_name(out->scope));
    fputs(",\"canonicalAddress\":", stdout); json(out->canonical_address);
    fputs("};</script>", stdout);
    fwrite(slot + strlen(marker), 1, strlen(slot + strlen(marker)), stdout);
    free(body);
    return 1;
}

/* Decode one query value without assigning it semantic meaning.  The browser
 * has only proposed a route; the signed-in Studio action remains the evidence
 * and authority boundary. */
/* Delegates to the one implementation. Two copies of this existed under one
 * name and disagreed about malformed input -- one refused a truncated percent
 * escape and a control byte, the other passed both through as literal text.
 * The strict reading is the one that survived. */
static int query_value(const char *query, const char *key, char *out, size_t cap) {
    return bf_lex_url_query(query, key, out, cap);
}

static int public_http_url(const char *url) {
    const char *host;
    size_t len;
    if (!url || !(len = strlen(url)) || len >= 1024) return 0;
    if (!strncmp(url, "https://", 8)) host = url + 8;
    else if (!strncmp(url, "http://", 7)) host = url + 7;
    else return 0;
    if (!*host || *host == '/' || *host == '?' || *host == '#') return 0;
    for (const unsigned char *p = (const unsigned char *)url; *p; p++) if (*p < 0x20 || *p == 0x7f) return 0;
    return 1;
}

static sqlite3 *cms_open_ro(void);
static int cms_type_registered(sqlite3 *db, const char *type);
static int foreign_safe_file(const char *text);
static int studio_safe_ident(const char *value);
static int session_authorized(const char *bearer, const char *auth_db, const char *scope);
static void json_escape_into(const char *in, char *out, size_t cap);
static int wants_json(const char *accept);
static int query_value(const char *query, const char *key, char *out, size_t cap);
static void ways_html(const char *ways_json);
static void compose_slug(const char *text, char *out, size_t cap, const char *prefix);
static int twin_classified_kind(const char *subject, char *out, size_t cap);
static int valid_token(const char *t);
static const char *auth_binary(void);
static int auth_call(char *const argv[], const char *stdin_data, char *out, size_t out_sz);
static int studio_json_field(const char *json, const char *name, char *out, size_t size);

/* SurfaceIR pattern compiler.
 *
 * The doors fragment worked, and adding a second pattern the same way would
 * have meant a second handler with its own SQL, its own markup and its own
 * name compiled in -- ten patterns, ten places to change, and no answer to
 * "what does this surface read and what may it do". The atlas already named
 * this: surface-ir carries the known_gap that the general grammar compiler
 * still owes arbitrary subject/view compilation.
 *
 * A pattern is now a descriptor in the CMS. It declares which authored type it
 * reads, how rows are selected and ordered, which fields an item carries, its
 * effect class, and the authority a reader needs. This function is the whole
 * renderer for every pattern.
 *
 * The point is not that HTML got easier to author. It is that a person and a
 * machine are served from one descriptor and one query: /_surface/<pattern>
 * returns HTMX to a browser and JSON to a machine, over the same rows in the
 * same order. Two carriers of one capability cannot drift into two
 * implementations of it, because there is only one.
 *
 * Every identifier below is interpolated into SQL, so every identifier is
 * validated first. They come from the institution's own CMS rather than from a
 * caller, which lowers the risk and does not remove it: a descriptor is data,
 * and a store that can be written is a store that can be written wrongly. The
 * selector VALUE is a caller's, and is always bound, never interpolated.
 *
 * Cannot infer: a rendered affordance -> an authorized effect. This decides
 * what a reader may see; Authority still gates at effect time. And a declared
 * pattern is not a served one -- if the descriptor names a type the store does
 * not have, the fragment is empty and the page keeps its fallback. */

/* An ORDER BY built from a descriptor: identifiers, commas, spaces, and an
 * optional direction. Anything else is refused outright rather than sanitized,
 * because a half-understood ORDER BY is how an injection survives review. */
#endif /* BONFYRE_MATRIX_CLOSURE_BOOTSTRAP_H */
