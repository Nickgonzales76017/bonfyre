#define _POSIX_C_SOURCE 200809L
#include "auth_continuation.h"
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "site_product.h"
#include "site_mcp.h"
#include "site_presentation.h"
#include "bf_authority.h"
#include "bf_command_lookup.h"
#include "bf_institutions.h"
#include "bf_matrix.h"
#include "bf_matrix_closure.h"
#include "bf_json.h"
#include "bf_receipt_store.h"
#include "bonfyre.h"
#include "bf_sqlite_path.h"
#include "community_projection.h"
#include "atom_feed.h"
#include "domain_profile_projection.h"
#include "public_projection_cache.h"
#include "source_hierarchy_projection.h"
#include "model_catalogue_index.h"
#include "source_depth_projection.h"
#include "subject_address.h"
#include "bounded_output.h"
#include "http_readiness.h"
#include "public_audio_deck.h"
#include "people_projection.h"
#include "spatial_frontier.h"
#include "territory_home.h"
#include "recipe_material.h"
#include "recipe_week.h"
#include "../generated/native_surface_programs.h"
#include "../generated/native_presentation_cells.h"
#include "../generated/native_machine_surfaces.h"
#include "../generated/native_template_slots.h"
#include "../generated/native_site_application_lowerers.h"
#include "../generated/native_world_material_renderers.h"
#include "../generated/native_page_composition.h"
#include "../generated/native_governance.h"
#include "../generated/native_operators.h"
#include "../generated/native_planes.h"
/* Thirty interaction grammars and the primitives they share, compiled from CUE.
 * The point of the table is the cross-reference: a proposal is one cell whatever
 * vocabulary asked for it. */
#include "../generated/native_clone_grammars.h"
/* Criterion kinds bound to the columns they read, and which verdicts a
 * machine may set. A page shows the column a criterion queries rather than
 * the text it was given. */
#include "../generated/native_discovery_sets.h"
#include "bf_commitment.h"
#include "../generated/native_audience_presentations.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <math.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <strings.h>
#include <time.h>
#include <zlib.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sqlite3.h>

#include "bf_lex.h"

#include "glyph_set.h"
#include "detail_depth.h"
#include "route_presentation.h"

/* One bar for every page family.  Front pages print it with the reader's place
 * already marked; a native surface, a donor surface, a subject room and the
 * sign-in pages carry this literal, whose last element marks the place from the
 * address.  The section list repeats bf_page_nav from page-composition.cue: a
 * route added there is added here.  It holds no percent sign, so it is safe
 * inside the format strings that open a room. */
#include "account_navigation_markup.h"
#include "account_home_continuation.h"
#include "production_home.h"
#include "account_connections_presentation.h"
static int studio_safe_ident(const char *name);
static int production_reference(const char *ref,char *ns,char *slug);
static void page_glyph(const char *name);
static const char *page_operation_glyph(const char *operation);
static const char *page_nav_glyph(const char *route);
#include "cms_workbench.h"
#define BF_ESTATE_BAR_HEAD \
"<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\"><link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&" \
"amp;family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&amp;family=Instrument+Serif:ital@0;1&amp;display=swap\"><link rel=\"stylesheet\" href=\"/assets/aurekai-bar.css?v=5\">" BF_ICON_STYLE
#define BF_ESTATE_BAR_OUT \
"<span class=\"ak-out\"><a href=\"https://docs.aurekai.work/\">" BF_ICON_BOOK "<span>Guides</span></a></span>"
#define BF_ESTATE_BAR_CORE \
"<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\"><link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&" \
"amp;family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&amp;display=swap\"><link rel=\"stylesheet\" href=\"/assets/aurekai-bar.css?v=5\">" BF_ICON_STYLE \
"<header class=\"ak-bar\"><div class=\"ak-bar-top\"><a class=\"ak-brand\" href=\"/\">Aurekai</a><nav class=\"ak-places\" aria-label=\"Territories\">" \
"<a href=\"https://aurekai.live/\">" BF_ICON_WORLD "<span>Live</span></a><a href=\"https://aurekai.life/\">" BF_ICON_HEART "<span>Life</span></a>" \
"<a href=\"https://aurekai.shop/\">" BF_ICON_SHOP "<span>Shop</span></a><a href=\"https://aurekai.work/\">" BF_ICON_BRIEFCASE "<span>Work</span></a></nav>" \
"<form class=\"ak-find\" action=\"/search\" method=\"get\" role=\"search\"><label>" BF_ICON_SEARCH "<input type=\"search\" name=\"q\" placeholder=\"Search\" aria-label=\"Search\"></label></form>" \
"<div class=\"ak-session-slot\" data-account-slot><!--BF_SESSION_BEGIN--><a class=\"ak-account\" id=\"session-link\" href=\"/login\">" BF_ICON_LOGIN "<span>Sign in</span></a><!--BF_SESSION_END--></div></div>" \
"<nav class=\"ak-sections\" aria-label=\"Sections\"><a href=\"/discover\">" BF_ICON_COMPASS "<span>Discover</span></a><a href=\"/whats-on\">" BF_ICON_CALENDAR "<span>What&#039;s on</span></a>" \
"<a href=\"/watch\">" BF_ICON_PLAY "<span>Watch</span></a><a href=\"/listen\">" BF_ICON_HEADPHONES "<span>Listen</span></a><a href=\"/board\">" BF_ICON_BOARD "<span>Collections</span></a>" \
"<span class=\"ak-out\"><a href=\"https://docs.aurekai.work/\">" BF_ICON_BOOK "<span>Guides</span></a></span></nav>" \
"<script>(function(){var b=document.currentScript.parentNode,h=location.hostname.split('.').slice(-2).join('.'),p=location.pathname;b.querySelectorAll('.ak-places a').forEach(function(a){if(a.hostname===h)a.setAttribute('aria-current','page')});b.querySelectorAll('.ak-sections>a').forEach(function(a){var r=a.getAttribute('href');if(p===r||p.indexOf(r+'/')===0)a.setAttribute('aria-current','page')});})();</script>" \
BF_ACCOUNT_STYLE BF_ACCOUNT_REFRESH \
"</header>"
#define BF_ESTATE_BAR BF_ESTATE_BAR_CORE BF_HOME_CONTINUATION

static const char *opt(int argc, char **argv, const char *name) { for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1]; return NULL; }
#include "matrix_closure_bootstrap.h"
#include "surface_catalogue_routes.h"
#include "surface_fragments_and_ops.h"
#include "auth_routes_and_studio_core.h"
#include "surface_world_and_community.h"
#include "site_and_world_material.h"
#include "page_core_rendering.h"
#include "page_routes_and_emitters.h"
#include "world_cache_and_cms_route.h"
#include "x402_capability_and_aperture_forms.h"
#include "aperture_foreign_and_packs.h"
#include "credit_profile_projection.h"
#include "world_federation_and_protocols.h"
#include "governance_self_and_discovery.h"
#include "role_directory.h"
#include "private_session_and_flow_routes.h"
#include "machine_web_and_websub.h"
#include "source_push_operations.h"
#include "subject_and_lens.h"
#include "room_compose_and_flow_cards.h"
#include "desk_and_internet_observatory.h"
static int estate_directory(void) {
    const char *mdb = getenv("BONFYRE_MATRIX_DB");
    sqlite3 *db = NULL;
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
           "Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n");
    printf("{\"version\":\"bonfyre-estate-v1\",\"hosts\":[");
    int wrote = 0;
    if (mdb && sqlite3_open_v2(mdb, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT hostname, institution_id, territory, default_scope, "
                "default_world, default_representation, evidence_ref "
                "FROM matrix_domains ORDER BY institution_id, territory, hostname",
                -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                if (wrote++) printf(",");
                printf("{\"host\":");        json((const char *)sqlite3_column_text(st, 0));
                printf(",\"institution\":");  json((const char *)sqlite3_column_text(st, 1));
                printf(",\"territory\":");    json((const char *)sqlite3_column_text(st, 2));
                printf(",\"audience\":");     json((const char *)sqlite3_column_text(st, 3));
                printf(",\"world\":");        json((const char *)sqlite3_column_text(st, 4));
                printf(",\"representation\":");json((const char *)sqlite3_column_text(st, 5));
                printf(",\"evidence\":");     json((const char *)sqlite3_column_text(st, 6));
                printf("}");
            }
            sqlite3_finalize(st);
        }
    }
    if (db) sqlite3_close(db);
    printf("],\"count\":%d,", wrote);
    printf("\"note\":\"Every host this Matrix carries. A host listed here answers for its "
           "institution in its territory; it does not follow that any subject is readable to "
           "any caller, or that an institution grants anything by holding an address.\"}\n");
    return 1;
}

static int discovery(BfMatrix *m, const char *host_in, const char *root) {
    char host[256]; BfMatrixDomainResolution domain; int rc;
    if (!lower_host(host_in,host)) return 2;
    rc=bf_matrix_domain_resolve(m,host,&domain);
    if (rc != 1) { printf("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"verdict\":\"unknown_host\"}\n"); return rc < 0 ? 2 : 1; }
    /* The address document is the one thing on this estate that has to be
     * readable from the other three territories: the whole point of an address
     * plane is that a subject can be resolved from anywhere, and a page on
     * .work asking .live what it is was being refused by the browser rather
     * than by this organ. It is a public, self-describing document with no
     * caller state in it, so it answers any origin. Nothing else here gains
     * that; a gated pattern stays gated, and 403 is still 403. */
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
           "Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n");
    printf("{\"version\":\"bonfyre-address-v1\",\"host\":");json(host);
    printf(",\"canonical_host\":");json(domain.canonical_hostname);
    printf(",\"territory\":");json(domain.territory);
    printf(",\"world\":");json(bf_matrix_world_name(domain.world));
    printf(",\"audience\":");json(bf_matrix_scope_name(domain.scope));
    printf(",\"representation\":");json(domain.representation);

    /* Whose host this is. The Matrix has always known it -- it is in the domain
     * resolution -- and not publishing it meant a surface served on
     * leaflounge.aurekai.shop could not tell it was the Leaf Lounge's. A
     * surface that cannot name the institution it is standing in is a
     * projection of nobody. */
    printf(",\"institution\":");json(domain.institution_id);

    /* The same institution across territories. This is the address plane's
     * actual shape: one institution, one host per territory it participates
     * in, and sometimes a domain of its own. Read from the Matrix rather than
     * derived from the hostname, because leaf-lounge holds theleaflounge.store
     * and no amount of string surgery on a subdomain would find it. */
    {
        const char *mdb = getenv("BONFYRE_MATRIX_DB");
        sqlite3 *hdb = NULL;
        int wrote = 0;
        printf(",\"institution_hosts\":[");
        if (mdb && sqlite3_open_v2(mdb, &hdb, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
            sqlite3_stmt *st = NULL;
            if (sqlite3_prepare_v2(hdb,
                    "SELECT hostname, territory, default_scope FROM matrix_domains "
                    "WHERE institution_id = ? ORDER BY territory, hostname",
                    -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, domain.institution_id, -1, SQLITE_STATIC);
                while (sqlite3_step(st) == SQLITE_ROW) {
                    if (wrote++) printf(",");
                    printf("{\"host\":");
                    json((const char *)sqlite3_column_text(st, 0));
                    printf(",\"territory\":");
                    json((const char *)sqlite3_column_text(st, 1));
                    printf(",\"audience\":");
                    json((const char *)sqlite3_column_text(st, 2));
                    printf("}");
                }
                sqlite3_finalize(st);
            }
        }
        if (hdb) sqlite3_close(hdb);
        printf("]");
    }

    /* web is always advertised: this organ serves it. */
    char url[512];
    snprintf(url,sizeof(url),"https://%s/",host);
    printf(",\"web\":");json(url);

    /* bonfyrefs is a fabric path, not an HTTP surface, so it is not gated. */
    snprintf(url,sizeof(url),"/Bonfyre/%s",domain.territory);
    printf(",\"bonfyrefs\":");json(url);

    /* Both are generated from the same Matrix resolution as this descriptor.
     * They are deliberately sparse compiler outputs, not new public stores. */
    snprintf(url,sizeof(url),"https://%s/ro-crate/ro-crate-metadata.json",host);
    printf(",\"ro_crate\":");json(url);
    snprintf(url,sizeof(url),"https://%s/zarr/zarr.json",host);
    printf(",\"zarr\":");json(url);

    /* Auth is advertised on the same terms as any other surface: only if this
     * organ can actually reach the binary that serves it. */
    if (auth_reachable()) {
        snprintf(url,sizeof(url),"https://%s/login",host);
        printf(",\"auth\":{\"login\":");json(url);
        snprintf(url,sizeof(url),"https://%s/signup",host);
        printf(",\"signup\":");json(url);
        snprintf(url,sizeof(url),"https://%s/account",host);
        printf(",\"account\":");json(url);
        snprintf(url,sizeof(url),"https://%s/logout",host);
        printf(",\"logout\":");json(url);
        printf(",\"scheme\":\"bearer\"}");
    }

    emit_surface("api",     host, "https://%s/api/",           root);
    emit_surface("mcp",     host, "https://%s/mcp/resources/",  root);
    emit_surface("a2a",     host, "https://%s/a2a/resources/",  root);
    emit_surface("cms",     host, "https://%s/cms/",            root);
    emit_surface("habitat", host, "https://%s/habitat/",        root);

    printf("}\n");
    return 0;
}
/* The edge is deliberately a native process pool rather than a thread pool.
 * A request temporarily redirects stdout while its compiler lowers a document;
 * separate processes preserve that simple, per-request carrier boundary while
 * every worker opens its own SQLite/Matrix connection after exec.  The parent
 * owns only the listening socket and child lifecycle -- never semantic state. */
#define BF_WEB_MAX_WORKERS 64
#define BF_WEB_DEFAULT_BACKLOG 1024
/* Request-target limits are a transport contract, not a semantic coordinate
 * limit.  SourceFrontier retains exact external locators and the static
 * compiler lowers their public query coordinates to bounded material keys.
 * The native renderer must still be able to read the retained coordinate once
 * while compiling that material.  These ceilings cover the current 3.7 KiB
 * coordinate corpus after URL encoding, while keeping every request bounded. */
/* A declared 48 KB CMS field may expand threefold in URL form encoding.
 * Keep a finite whole-request ceiling without rejecting our own native forms. */
#define BF_WEB_MAX_REQUEST_BYTES (640 * 1024)
#define BF_WEB_LEGACY_REQUEST_BYTES (256 * 1024)
#define BF_WEB_MAX_REQUEST_TARGET (16 * 1024)
#define BF_WEB_MAX_QUERY (16 * 1024)

static int web_listen_socket(int port, int backlog) {
    int fd, one = 1;
    struct sockaddr_in address = {0};
    if (port < 1 || port > 65535 || backlog < 1) return -1;
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
        close(fd);
        return -1;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    address.sin_port = htons((unsigned short)port);
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) != 0 ||
        listen(fd, backlog) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static volatile sig_atomic_t web_supervisor_stopping = 0;

static void web_supervisor_stop(int signal_number) {
    (void)signal_number;
    web_supervisor_stopping = 1;
}

/* This runs before Matrix is opened.  Each child execs the same binary with
 * the inherited socket and consequently opens fresh native owner handles.
 * Forking an already-live SQLite connection is intentionally avoided. */
static int serve_prefork(int argc, char **argv) {
    const char *port_arg = opt(argc, argv, "--port");
    int port = port_arg ? opt_positive_int(argc, argv, "--port", 8787, 65535) : 8787;
    int workers = opt_positive_int(argc, argv, "--workers", 1, BF_WEB_MAX_WORKERS);
    int backlog = opt_positive_int(argc, argv, "--backlog", BF_WEB_DEFAULT_BACKLOG, 65535);
    pid_t children[BF_WEB_MAX_WORKERS];
    int child_count = 0;
    int fd;
    struct sigaction action = {0};

    if (port < 1 || workers < 1 || backlog < 1) return 2;
    fd = web_listen_socket(port, backlog);
    if (fd < 0) return 2;
    action.sa_handler = web_supervisor_stop;
    sigemptyset(&action.sa_mask);
    (void)sigaction(SIGTERM, &action, NULL);
    (void)sigaction(SIGINT, &action, NULL);

    for (int i = 0; i < workers; i++) {
        pid_t child = fork();
        if (child == 0) {
            char listen_fd[32];
            char **child_argv = calloc((size_t)argc + 3, sizeof(*child_argv));
            if (!child_argv) _exit(127);
            snprintf(listen_fd, sizeof(listen_fd), "%d", fd);
            for (int j = 0; j < argc; j++) child_argv[j] = argv[j];
            child_argv[argc] = "--listen-fd";
            child_argv[argc + 1] = listen_fd;
            child_argv[argc + 2] = NULL;
            execv(argv[0], child_argv);
            _exit(127);
        }
        if (child < 0) {
            web_supervisor_stopping = 1;
            break;
        }
        children[child_count++] = child;
    }

    while (!web_supervisor_stopping && child_count > 0) {
        int status;
        pid_t exited = wait(&status);
        if (exited > 0) {
            /* A lost worker makes the process set non-uniform.  Restart the
             * whole bounded pool rather than silently reducing capacity. */
            web_supervisor_stopping = 1;
        } else if (errno != EINTR) {
            web_supervisor_stopping = 1;
        }
    }
    for (int i = 0; i < child_count; i++) (void)kill(children[i], SIGTERM);
    for (int i = 0; i < child_count; i++) (void)waitpid(children[i], NULL, 0);
    close(fd);
    return 2;
}

static int serve(BfMatrix *m, int argc, char **argv) {
    const char *p=opt(argc,argv,"--port");
    const char *listen_fd_arg=opt(argc,argv,"--listen-fd");
    int port=p?atoi(p):8787;
    int backlog=opt_positive_int(argc,argv,"--backlog",BF_WEB_DEFAULT_BACKLOG,65535);
    int fd;
    if (listen_fd_arg) {
        fd=opt_positive_int(argc,argv,"--listen-fd",-1,INT_MAX);
        if (fd < 0) return 2;
    } else {
        fd=web_listen_socket(port,backlog);
    }
    const char *root=site_root(argc,argv);
    wire_site_root=root;
    const char *auth_db=opt(argc,argv,"--auth-db");
    if(!auth_db||!*auth_db) auth_db=getenv("BONFYRE_AUTH_DB");
    if(!auth_db||!*auth_db) auth_db="/var/lib/bonfyre/auth.db";
    if (fd < 0 || port < 1 || port > 65535 || backlog < 1) return 2;
    /* A reader may give up on a large response. That is one request's
     * outcome, never grounds to terminate the public origin and strand every
     * remaining static-export coordinate behind it. */
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) { close(fd); return 2; }
    BfHttpReadiness *requests = bf_http_readiness_create(BF_WEB_MAX_REQUEST_BYTES);
    if (!requests || bf_http_readiness_narrow(requests, BF_WEB_LEGACY_REQUEST_BYTES, "/apps/")) { bf_http_readiness_destroy(requests); close(fd); return 2; }
    for (;;) {
        char b[BF_WEB_MAX_REQUEST_BYTES] = {0}, method[8] = {0}, target[BF_WEB_MAX_REQUEST_TARGET] = {0};
        char host[256] = {0}, query[BF_WEB_MAX_QUERY] = {0}, *q;
        size_t body_offset = 0;
        int c = bf_http_readiness_next(requests, fd, b, &body_offset);
        if (c < 0) { bf_http_readiness_destroy(requests); close(fd); return 2; }
        char *bodyp = b + body_offset;
        /* Framing is complete before an owner runs. Header lookups cannot
         * inspect submitted body text, even when Host or Accept is absent. */
        if (body_offset >= 4 && !memcmp(bodyp - 4, "\r\n\r\n", 4)) bodyp[-4] = '\0';
        else bodyp[-2] = '\0';
        sscanf(b,"%7s %16383s",method,target);/* Header names are not case-sensitive, and clients built on Node send them in lower case. */ char *h=strstr(b,"\r\nHost:");if(!h)h=strstr(b,"\nHost:");if(!h)h=strstr(b,"\r\nhost:");if(!h)h=strstr(b,"\nhost:");if(h)sscanf(h+(h[0]=='\r'?7:6),"%255s",host);q=strchr(target,'?');if(q){*q++='\0';snprintf(query,sizeof(query),"%s",q);}char *accept=strstr(b,"\r\nAccept:");if(!accept)accept=strstr(b,"\nAccept:");if(!accept)accept=strstr(b,"\r\naccept:");if(!accept)accept=strstr(b,"\naccept:");if(accept&&!strstr(query,"repr=")){char value[256]={0};sscanf(accept+(accept[0]=='\r'?9:8),"%255[^\r\n]",value);const char *repr=repr_from_accept(value);if(repr)snprintf(query+strlen(query),sizeof(query)-strlen(query),"%srepr=%s",query[0]?"&":"",repr);}
        char bearer[128]={0}, cookie_bearer[128]={0};
        if(strncmp(target,"/apps/",6)){
            char length[32];unsigned long long bytes=0;
            if(aw_header(b,"Content-Length",length,sizeof(length)))bytes=strtoull(length,NULL,10);
            if(body_offset>=BF_WEB_LEGACY_REQUEST_BYTES || bytes>=BF_WEB_LEGACY_REQUEST_BYTES-body_offset){
                const char refusal[]="HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                (void)send(c,refusal,sizeof(refusal)-1,0);close(c);continue;
            }
        }
        {char *a=strstr(b,"\nAuthorization:"); if(!a) a=strstr(b,"\nauthorization:");
         if(a){char v[160]={0}; sscanf(a+15,"%159[^\r\n]",v); char *t=v; while(*t==' ')t++;
               if(!strncmp(t,"Bearer ",7)||!strncmp(t,"bearer ",7)){t+=7; while(*t==' ')t++;
                   snprintf(bearer,sizeof(bearer),"%s",t);
                   char *e=bearer+strlen(bearer); while(e>bearer&&(e[-1]==' '||e[-1]=='\t'))*--e='\0';}}}
        /* A public sign-in establishes an HttpOnly, SameSite session cookie
         * for the controlled Aurekai subdomains.  Prefer an explicit bearer
         * for machine clients; otherwise read exactly the named cookie and
         * run the same native validation path. */
        /* An HttpOnly session cookie is the browser transport. Some legacy
         * surfaces still emit a bearer from the retired localStorage transport.
         * A stale token can keep the exact bfy_+64hex shape after revocation, so
         * syntax alone cannot identify the browser's current credential. Read
         * the named cookie independently and let a valid cookie win when both
         * transports arrive. Machine clients normally carry only Authorization. */
        if (bearer[0] && !valid_token(bearer)) bearer[0] = '\0';
        {
            char *ck=strstr(b,"\nCookie:"); if(!ck) ck=strstr(b,"\ncookie:");
            if (ck) {
                char value[512]={0}; char *p;
                sscanf(ck+8,"%511[^\r\n]",value);
                p=value;
                while (*p) {
                    while (*p==' ' || *p=='\t' || *p==';') p++;
                    if (!strncmp(p,"bonfyre_session=",16)) {
                        char *v=p+16, *e=v;
                        while (*e && *e!=';' && *e!=' ' && *e!='\t') e++;
                        if ((size_t)(e-v) < sizeof(cookie_bearer)) {
                            memcpy(cookie_bearer,v,(size_t)(e-v)); cookie_bearer[e-v]='\0';
                            if (!valid_token(cookie_bearer)) cookie_bearer[0]='\0';
                        }
                        break;
                    }
                    p=strchr(p,';'); if (!p) break;
                }
            }
        }
        if (cookie_bearer[0])
            snprintf(bearer,sizeof(bearer),"%s",cookie_bearer);
        char origin_method[16]={0};
        {char *om=strstr(b,"\nX-Original-Method:"); if(!om) om=strstr(b,"\nx-original-method:");
         if(om){sscanf(om+19,"%15s",origin_method);}}
        char institution[64]={0};
        {char *ih=strstr(b,"\nX-Bonfyre-Institution:"); if(!ih) ih=strstr(b,"\nx-bonfyre-institution:");
         if(ih){sscanf(ih+24,"%63[^\r\n]",institution);
                char *t=institution; while(*t==' '||*t=='\t') t++;
                if(t!=institution) memmove(institution,t,strlen(t)+1);
                char *e=institution+strlen(institution); while(e>institution&&(e[-1]==' '||e[-1]=='\t'))*--e='\0';}}
        char xpayment[2048]={0};
        {char *xp=strstr(b,"\nX-PAYMENT:"); if(!xp) xp=strstr(b,"\nx-payment:");
         if(xp){sscanf(xp+11,"%2047[^\r\n]",xpayment);
                char *t=xpayment; while(*t==' ')t++;
                if(t!=xpayment) memmove(xpayment,t,strlen(t)+1);
                char *e=xpayment+strlen(xpayment); while(e>xpayment&&(e[-1]==' '||e[-1]=='\t'))*--e='\0';}}
        /* Hoisted out of the dispatch block: the fallthrough surface path below
         * negotiates representation too, and it must read the same header the
         * dispatched handlers did rather than default to the human one. */
        char accept_hdr[256]={0};
        {char *ah=strstr(b,"\nAccept:"); if(!ah) ah=strstr(b,"\naccept:");
         if(ah) sscanf(ah+8,"%255[^\r\n]",accept_hdr);}
        /* Hoisted for the same reason as Accept: the fallthrough path serves
         * the static assets, and a conditional request it cannot see is a
         * conditional request that always answers with the whole body. */
        char inm_hdr[256]={0};
        {char *ih=strstr(b,"\nIf-None-Match:"); if(!ih) ih=strstr(b,"\nif-none-match:");
         if(ih) sscanf(ih+15,"%255[^\r\n]",inm_hdr);}
        char accept_encoding[128]={0};
        {char *ae=strstr(b,"\nAccept-Encoding:"); if(!ae) ae=strstr(b,"\naccept-encoding:");
         if(ae) sscanf(ae+17,"%127[^\r\n]",accept_encoding);}
        {FILE *wire=tmpfile(); int sfd=dup(STDOUT_FILENO);
         if (sfd < 0 || dup2(wire ? fileno(wire) : c, STDOUT_FILENO) < 0) {
             if (sfd >= 0) close(sfd);
             if (wire) fclose(wire);
             close(c);
             continue;
         }
         /* Per-request, before dispatch: this is process-global state and the
          * server does not fork per connection, so a value left by a browser
          * request was still set when the next machine request arrived and
          * x402 clients were handed HTML. */
         studio_html_mode = accept_hdr[0] && strstr(accept_hdr, "text/html") != NULL;
         page_request_bearer=bearer;page_request_auth_db=auth_db;
         psetup_presentation_host=host;psetup_presentation_route=target;psetup_presentation_query=query;
         snprintf(account_request_territory,sizeof(account_request_territory),"%s",territory_of(host));
         account_signin_path=sso_territory(host)?"/session/continue":"/login";
         int nav_path=snprintf(account_return_path,sizeof(account_return_path),"%s%s%s",target,*query?"?":"",query);
         if(nav_path<1||nav_path>=(int)sizeof(account_return_path))snprintf(account_return_path,sizeof(account_return_path),"/");
         /* The discovery tier answers before anything else looks at the request.
          * A sitemap or a feed is the same bytes for every caller, so it must
          * not pay for session lookup or per-viewer computation. */
         /* A delegated host gets its private dispatcher before the public
          * machine web.  This ordering is a disclosure invariant: no
          * sitemap, protocol card, RO-Crate, feed, or fallback resolver can
          * observe that a client estate exists. */
         /* A private CMS namespace is a one-request closure coordinate.  This
          * server is intentionally single-threaded, so leaving it set would
         * let the next unrelated request inherit a previous client's scope. */
         private_cms_namespace[0]='\0';
         private_foreign_root[0]='\0';
         /* An operator's own site answers assistants itself, before the
          * estate's shared surfaces see the request. */
         char origin_hdr[256]={0};
         {char *oh=strstr(b,"\nOrigin:"); if(!oh) oh=strstr(b,"\norigin:");
          if(oh) sscanf(oh+8," %255[^\r\n]",origin_hdr);}
         char site_bearer[160]={0};
         {char *zh=strstr(b,"\nAuthorization:"); if(!zh) zh=strstr(b,"\nauthorization:");
          if(zh) sscanf(zh+15," Bearer %159[^\r\n ]",site_bearer);}
         /* Where the request came from: the address an edge in front names,
          * else the peer's own.  Used only to tell keyless callers apart. */
         char site_client[96]={0};
         {char *fh=strstr(b,"\nCF-Connecting-IP:"); if(!fh) fh=strstr(b,"\ncf-connecting-ip:");
          if(fh) sscanf(fh+18," %95[^\r\n, ]",site_client);
          if(!site_client[0]){fh=strstr(b,"\nX-Forwarded-For:"); if(!fh) fh=strstr(b,"\nx-forwarded-for:"); if(fh) sscanf(fh+17," %95[^\r\n, ]",site_client);}
          if(!site_client[0]){struct sockaddr_storage peer; socklen_t peer_len=sizeof(peer);
            if(!getpeername(c,(struct sockaddr *)&peer,&peer_len)) getnameinfo((struct sockaddr *)&peer,peer_len,site_client,sizeof(site_client),NULL,0,NI_NUMERICHOST);}}
         snprintf(estate_request_territory, sizeof(estate_request_territory), "%s", territory_of(host));
         int handled=bf_site_mcp_serve(method,target,query[0]?query:"",bodyp,host,origin_hdr,site_bearer,site_client);
         if(!handled) handled=serve_vertical_private(method,target,bodyp,bearer,auth_db,host);
         /* The Observatory owns its human/public maintained views.  It runs
          * before generic machine-web dispatch so a host-specific closure
          * cannot accidentally inherit another institution's carrier page. */
         if(!handled) handled=serve_internet_observatory(m,method,target,bodyp,bearer,auth_db,host);
         /* Existing /people/<person> aliases resolve one published public profile.
          * Never manufacture a person or host from a publisher byline. */
         if(!handled && root && !strcmp(method,"GET") &&
            !strncmp(target,"/people/",8) && target[8] &&
            !(wants_json(accept_hdr) && !wants_html(accept_hdr))){
             char subject[144]={0},public_host[128]={0},tab[24]={0};
             const char *slug=target+8,*slash=strchr(slug,'/');
             size_t n=slash?(size_t)(slash-slug):strlen(slug);
             if(n>0&&n<=63){
                 if(n>=7&&!strncmp(slug,"person:",7))snprintf(subject,sizeof(subject),"%.*s",(int)n,slug);
                 else snprintf(subject,sizeof(subject),"person:%.*s",(int)n,slug);
             }
             if(slash&&slash[1]&&strlen(slash+1)<sizeof(tab))snprintf(tab,sizeof(tab),"%s",slash+1);
             else if(!slash&&query[0])query_value(query,"tab",tab,sizeof(tab));
             static const char *const tabs[]={"overview","posts","gallery","about","communities","organizations"};
             int valid=!tab[0]&&(!slash||!slash[1]);
             for(size_t i=0;i<sizeof(tabs)/sizeof(tabs[0]);i++)if(!strcmp(tab,tabs[i]))valid=1;
             if(valid&&subject[0]&&bf_public_person_home(subject,public_host,sizeof(public_host)))
                 printf("HTTP/1.1 303 See Other\r\nLocation: https://%s/%s%s\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",public_host,tab[0]?"?tab=":"",tab);
             else fputs("HTTP/1.1 404 Not Found\r\nCache-Control: no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);
             handled=1;
         }
        /* These are public, server-lowered relational directories. Their
         * exact route cannot be a capability, commerce, studio or protocol
         * request, so dispatch it before probing those unrelated owners. This
         * keeps a static full-estate export proportional to its own cut. */
        if(!handled && root && !(wants_json(accept_hdr) && !wants_html(accept_hdr)))
            handled=serve_credit_profile(method,target,query);
        if(!handled && root && !strcmp(method,"GET") &&
           (!strcmp(target,"/people") || !strcmp(target,"/organisations")) &&
           !(wants_json(accept_hdr) && !wants_html(accept_hdr))) {
            /* Canonical person navigation must use the exact published subject.
             * Byline names without an admitted person identity stay source-bound. */
            if (!strcmp(target,"/people") && query[0]) {
                char subject[144] = "", public_host[128] = "";
                int requested = query_value(query,"subject",subject,sizeof(subject));
                if (!requested && query_value(query,"who",subject,sizeof(subject)) &&
                    !strncmp(subject,"person:",7)) requested = 1;
                if (requested) {
                    if (bf_public_person_home(subject,public_host,sizeof(public_host)))
                        printf("HTTP/1.1 303 See Other\r\nLocation: https://%s/\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",public_host);
                    else fputs("HTTP/1.1 404 Not Found\r\nCache-Control: no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);
                    handled=1;
                }
            }
            page_request_bearer = bearer; page_request_auth_db = auth_db; page_request_raw = b;
            if(!handled) handled=emit_page_program_route(root,target,host,query);
        }
        if(!handled) handled=serve_machine_web(method,target,host,inm_hdr);
        if(!handled) handled=subject_artifact(method,target,inm_hdr);
        if(!handled) handled=serve_indexnow_key(method,target,inm_hdr);
        if(!handled) handled=serve_websub(method,target,query,b,bodyp);
        /* A published SitePresentation owns its matching apex before legacy
         * donor/static fallback.  Hosts without one continue down unchanged. */
         /* A /w/<world>/spatial request carrying a frontier is a different
          * question from the spatial surface: it asks what was there at an
          * instant. Without `at` this returns 0 and the surface answers exactly
          * as it always did. */
         if(!handled) handled=serve_spatial_frontier(method,target,query[0]?query:NULL,accept_hdr);
        /* A territory's own apex opens on its composed front page, ahead of
         * any SitePresentation that names the same apex.  Institution and
         * subdomain hosts are not territory apexes and keep their homes. */
        page_request_bearer = bearer; page_request_auth_db = auth_db; page_request_raw = b;
        /* A subject's own apex opens on the program published for that
         * subject.  A member's address exists to carry their surfaces, and a
         * composed territory front page standing there would make every member
         * home show the same material -- which is exactly what it did before
         * this line.  Territory apexes keep the composed home, and a subject
         * host without a published program still falls through to it. */
        /* Resolve the source-specific workbench before general SiteProgram
         * or /s/work material. Ordinary /link retains its native CUE route.
         * Explicit JSON requests still reach their previous owner. */
        if(!handled)handled=serve_account_activity(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled)handled=serve_source_web_work(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled)handled=serve_source_push_operations(method,target,query,host,bearer,auth_db);
        if(!handled && root)handled=serve_source_desk(method,target,accept_hdr);
        if(!handled)handled=serve_community_journey(method,target,query,bodyp,b,host,bearer,auth_db,accept_hdr);
        if(!handled)handled=serve_account_world_workspace(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled)handled=serve_profile_home(method,target,query,bodyp,b,host,bearer,auth_db,accept_hdr);
        if(!handled)handled=serve_participant_journey(method,target,query,bodyp,b,host,bearer,auth_db,accept_hdr);
        if(!handled && !strcmp(target,"/") && !strcmp(method,"GET") && !host_is_territory_apex(host))
            handled=serve_site_program(method,target,query[0]?query:NULL,host,accept_hdr,bearer,auth_db);
        if(!handled && root && !strcmp(target,"/") && !strcmp(method,"GET") &&
           !(wants_json(accept_hdr) && !wants_html(accept_hdr)))
            handled=emit_page_program_route(root,target,host,query);
        if(!handled) handled=serve_page_save(method,target,bodyp,b,host,bearer,auth_db);
        /* Saying where you stand, putting your name to a role, stating support. */
        if(!handled) handled=govern_act(method,target,bodyp,b,host,bearer,auth_db);
        if(!handled) handled=serve_role_assume(method,target,bodyp,b,host,bearer,auth_db);
        if(!handled) handled=serve_role_workspace(method,target,query,bearer,auth_db);
        if(!handled) handled=serve_role_directory(method,target,query,host,bearer,auth_db);
        if(!handled) handled=serve_role_leave(method,target,bodyp,b,host,bearer,auth_db);
        if(!handled && root && !strcmp(target,"/saved") && !strcmp(method,"GET"))
            handled=emit_saved_page(root,host,bearer,auth_db);
        if(!handled && root && !strncmp(target,"/o/",3))
            handled=serve_page_changes(root,method,target,bodyp,b,host,bearer,auth_db);
        if(!handled && root && !strncmp(target,"/about/",7) && !strcmp(method,"GET"))
            handled=emit_about_page(root,target,host,bearer,auth_db);
        if(!handled && root && !strncmp(target,"/o/",3) && !strcmp(method,"GET"))
            handled=emit_item_page(root,target,host,bearer,auth_db);
        if(!handled && (!strcmp(target,"/examples") || !strncmp(target,"/examples/",10)) && !strcmp(method,"GET")) {
            protocol_reply(410,"text/plain; charset=utf-8","Released work now appears on the account profile that published it.\n");
            handled=1;
        }
        if(!handled) handled=serve_site_program(method,target,query[0]?query:NULL,host,accept_hdr,bearer,auth_db);
        /* A signed-in person's /inbox is their own; the public donor inbox
         * keeps answering everyone else (account_inbox_web.h). */
        if(!handled) handled=serve_account_inbox(method,target,query,bearer,auth_db);
        if(!handled) handled=serve_native_donor_route(method,target,accept_hdr,root,host,query,bearer,auth_db);
        /* An authored surface answers before the auth handler, which only
         * matters for GET: serve_authored_page declines anything else, so a
         * credential POST still reaches the organ that owns sessions. Without
         * this ordering the authored /login was written, published, and never
         * rendered -- the older handler claimed the route first. */
        if(!handled) handled=serve_account_navigation(method,target,query,bearer,auth_db);
        if(!handled) handled=serve_authored_page(method,target,query,accept_hdr,bearer,auth_db,root,host);
        if(!handled) handled=serve_external_candidate_fragment(method,target,query,accept_hdr,bearer,auth_db);
        if(!handled) handled=serve_auth(method,target,bodyp,bearer,auth_db,accept_hdr,root,host,query);
        if(!handled) handled=serve_superuser_control(method,target,bodyp,bearer,auth_db,host);
        if(!handled) handled=serve_desk_external_candidate(method,target,query,bearer,auth_db,accept_hdr);
        if(!handled) handled=serve_desk(method,target,bearer,auth_db,accept_hdr);
         if(!handled) handled=serve_membrane(method,target,bearer,auth_db,accept_hdr);
         if(!handled && root && !strcmp(method,"GET") && !strncmp(target,"/s/parcel/",10) && wants_html(accept_hdr))
             handled=emit_parcel_page(root,target,host,bearer,auth_db);
         if(!handled) handled=serve_subject(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_lens(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_room(method,target,query,bearer,auth_db,accept_hdr);
         if(!handled) handled=serve_compose(method,target,query,bodyp,bearer,auth_db);
         if(!handled) handled=serve_surfaceir(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_flow(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_property_flow(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_market_flow(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_sources_flow(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_agents_flow(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_a2a(method,target,bodyp,host,xpayment,accept_hdr);
         if(!handled) handled=serve_capability(method,target,xpayment,host,accept_hdr);
         if(!handled) handled=serve_aperture_submit(method,target,bodyp,accept_hdr,bearer,auth_db);
         if(!handled) handled=serve_aperture_form(method,target,query,accept_hdr,bearer,auth_db,host);
         if(!handled) handled=serve_aperture(method,target,query,accept_hdr,bearer,auth_db);
         if(!handled) handled=serve_twins_page(method,target,accept_hdr);
         if(!handled) handled=serve_observations_page(method,target,accept_hdr);
         if(!handled) handled=serve_night_page(method,target,host,accept_hdr);
         if(!handled) handled=serve_protocols(method,target,host,accept_hdr,query);
         if(!handled) handled=serve_asset_twin(method,target);
         if(!handled) handled=serve_site_program(method,target,query[0]?query:NULL,host,accept_hdr,bearer,auth_db);
         if(!handled) handled=serve_world_composition(method,target,query[0]?query:NULL,bearer,auth_db);
         if(!handled) handled=serve_world_fragments(method,target,query[0]?query:NULL,bearer,auth_db);
         if(!handled) handled=serve_world_material(method,target,query[0]?query:NULL,accept_hdr);
         if(!handled) handled=serve_listen_program(method,target,query[0]?query:NULL,accept_hdr);
         if(!handled) handled=serve_human_community_page(method,target,query[0]?query:NULL,accept_hdr);
         if(!handled) handled=bf_serve_human_community(method,target,query[0]?query:NULL);
         if(!handled) handled=serve_production_publication(method,target,query);
         if(!handled) handled=serve_cms_composer(method,target,query,bodyp,bearer,auth_db,origin_hdr,host);
         if(!handled) handled=serve_source_publication(method,target,query,bodyp,b,bearer,auth_db,host);
         if(!handled) handled=serve_cms_api(m,method,target,bodyp,bearer,auth_db,accept_hdr);
         if(!handled) handled=serve_discovery_review(method,target,bodyp,bearer,auth_db,origin_hdr,host);
         if(!handled) handled=serve_discovery_verification(method,target,bodyp,bearer,auth_db,origin_hdr,host);
         if(!handled) handled=serve_discovery_brief(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_cms_partials(method,target,query,bearer,auth_db);
         if(!handled) handled=serve_cms_route(method,target,bearer,auth_db);
         if(!handled) handled=serve_studio(m,method,target,bodyp,bearer,auth_db,host,origin_method,institution,accept_hdr);
        if(!handled) handled=serve_subject_index(method,target,query,accept_hdr);
        if(!handled) handled=serve_pack_setup(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled) handled=serve_account_choices(method,target,query);
        if(!handled) handled=serve_organization_workspace(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled) handled=serve_household_members(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled) handled=serve_application_workspace(method,target,query,bodyp,b,host,bearer,auth_db);
        if(!handled) handled=serve_packs(method,target,query,host,accept_hdr,bearer,auth_db);
        if(!handled) handled=serve_home_weather_fragment(method,target);
        if(!handled) handled=serve_foreign_twins(method,target,query);
        if(!handled) handled=serve_foreign_weather(method,target);
        if(!handled) handled=serve_foreign_receipts(method,target,query);
        if(!handled) handled=serve_foreign_body(method,target,query);
        if(!handled) handled=serve_foreign_edges(method,target,accept_hdr,bearer,auth_db);
        if(!handled) handled=serve_ops(method,target);
        if(!handled) handled=serve_matrix_closure(m,method,host,target,query[0]?query:NULL,bearer,auth_db);
         fflush(stdout); dup2(sfd,STDOUT_FILENO); close(sfd);
         if(wire){ if(handled) wire_send(c,wire,accept_encoding,inm_hdr); fclose(wire); }
         if(handled){close(c);continue;}}
        if(strcmp(method,"GET")) {dprintf(c,"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n");} else if(!strcmp(target,"/healthz")) {dprintf(c,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"verdict\":\"healthy\"}\n");} else {FILE *tail_wire=tmpfile();int saved_fd=dup(STDOUT_FILENO);dup2(tail_wire?fileno(tail_wire):c,STDOUT_FILENO);if(!strcmp(target,"/.well-known/bonfyre/estate"))estate_directory();else if(!strcmp(target,"/.well-known/bonfyre"))discovery(m,host,root);else if((!strcmp(target,"/")||!*target)&&wants_json(accept_hdr))discovery(m,host,root);else if(serve_presentation_class(method,target,query[0]?query:NULL,bearer,auth_db)){}else if(serve_surface_pattern(target,query[0]?query:NULL,accept_hdr,bearer,auth_db)){}else if(serve_door_fragment(target,query[0]?query:NULL)){}else if(serve_aperture_fragment(m,host,target,query[0]?query:NULL)){}else if(serve_surface(m,host,target,root,accept_hdr,query)){}else if(serve_matrix_asset(m,host,target,root,inm_hdr,query[0]?query:NULL)){}else resolve(m,host,target,query[0]?query:NULL,1,root,bearer,auth_db);fflush(stdout);dup2(saved_fd,STDOUT_FILENO);close(saved_fd);if(tail_wire){wire_send(c,tail_wire,accept_encoding,inm_hdr);fclose(tail_wire);}}close(c);} }
/* ---------------------------------------------------------- what goes on the wire
 *
 * Every response this server writes goes through one boundary, so the estate
 * can be honest about transport without touching a single page: a composed
 * page is text, and text belongs on the wire compressed.  A first load of a
 * dense page was carrying a hundred kilobytes of markup that gzip takes to
 * about a seventh of that.
 *
 * Two things happen here and nothing else:
 *   - a body is gzipped when the client said it reads gzip, the body is worth
 *     compressing, and it is not already an image, a video or an archive;
 *   - a page that has not changed since the caller last saw it is answered
 *     with 304 and no body at all.
 *
 * The bytes a person receives are the bytes the page composed.  Nothing here
 * changes what a page says, what it may be read to imply, or who may see it:
 * a private response is compressed exactly as it would have been sent.
 */
/* A case-insensitive find, written here because the file is compiled to strict
 * POSIX and strcasestr is not in it. */
static const char *wire_find(const char *haystack, const char *needle) {
    size_t n = needle ? strlen(needle) : 0;
    if (!haystack || !n) return NULL;
    for (; *haystack; haystack++) {
        size_t k = 0;
        while (k < n && haystack[k] &&
               tolower((unsigned char)haystack[k]) == tolower((unsigned char)needle[k])) k++;
        if (k == n) return haystack;
    }
    return NULL;
}

static int wire_gzip(const char *body, size_t len, char **out, size_t *out_len) {
    z_stream zs; unsigned char *buffer; size_t cap = len + (len / 100) + 64;
    if (!(buffer = malloc(cap))) return 0;
    memset(&zs, 0, sizeof(zs));
    if (deflateInit2(&zs, Z_BEST_SPEED, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) { free(buffer); return 0; }
    zs.next_in = (Bytef *)(uintptr_t)body; zs.avail_in = (uInt)len;
    zs.next_out = buffer; zs.avail_out = (uInt)cap;
    if (deflate(&zs, Z_FINISH) != Z_STREAM_END) { deflateEnd(&zs); free(buffer); return 0; }
    *out_len = cap - zs.avail_out;
    deflateEnd(&zs);
    *out = (char *)buffer;
    return 1;
}

/* A body already carrying its own compression, or one too small to gain. */
/* Whether this answer is worth packing.  Only the header block decides: a page
 * that merely mentions a media type in its own markup -- a track list naming
 * audio files, a gallery naming pictures -- is still a page, and reading the
 * body here left the estate's richest pages travelling uncompressed. */
static int wire_worth_compressing(const char *headers, size_t head_len, size_t body_len) {
    static const char *const packed[] = {"image/", "video/", "audio/", "font/", "application/zip",
                                         "application/gzip", "application/octet-stream", "application/pdf"};
    char head[4096];
    size_t k, keep = head_len < sizeof(head) - 1 ? head_len : sizeof(head) - 1;
    if (body_len < 1400) return 0;
    memcpy(head, headers, keep);
    head[keep] = '\0';
    if (wire_find(head, "content-encoding:")) return 0;
    for (k = 0; k < sizeof(packed) / sizeof(packed[0]); k++)
        if (wire_find(head, packed[k])) return 0;
    return 1;
}

/* Cache law at the one place every answer leaves.
 *
 * A refusal must never be kept. Origin 404s carried no Cache-Control, the edge
 * stamped four hours on them, and a sheet that answered 404 once before its
 * declaration landed stayed a 404 in readers' browsers: aurekai.work drew its
 * whole front unstyled from a cached refusal. Every 4xx/5xx and temporary
 * redirect therefore leaves as no-store, whatever the handler wrote.
 *
 * And a page names each sheet and script by its bytes. /assets/NAME in a 200
 * HTML body is rewritten to /assets/NAME?v=<digest>, which the asset owner
 * already answers as immutable, so a deploy is a new URL rather than a stale
 * hit, and a repeat view fetches nothing. */
static const char *wire_site_root;

static int wire_status(const char *raw) {
    int status = 0;
    return sscanf(raw, "HTTP/1.1 %d", &status) == 1 ? status : 0;
}

static int wire_asset_version(const char *name, size_t length, char hex[17]) {
    static struct { char name[96]; time_t mtime; off_t size; char hex[17]; } seen[160];
    static unsigned used;
    char path[1024], digest[65];
    struct stat st;
    unsigned k;
    if (!wire_site_root || !*wire_site_root || !length || length >= sizeof(seen[0].name)) return 0;
    if (snprintf(path, sizeof(path), "%s/%.*s", wire_site_root, (int)length, name) >= (int)sizeof(path) ||
        stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0 || st.st_size > 4 * 1024 * 1024) return 0;
    for (k = 0; k < used; k++)
        if (strlen(seen[k].name) == length && !memcmp(seen[k].name, name, length)) {
            if (seen[k].mtime == st.st_mtime && seen[k].size == st.st_size) { memcpy(hex, seen[k].hex, 17); return 1; }
            break;
        }
    {
        FILE *file = fopen(path, "rb");
        unsigned char *bytes = file ? malloc((size_t)st.st_size) : NULL;
        int ok = bytes && fread(bytes, 1, (size_t)st.st_size, file) == (size_t)st.st_size;
        if (file) fclose(file);
        if (!ok) { free(bytes); return 0; }
        bf_sha256_hex(bytes, (size_t)st.st_size, digest);
        free(bytes);
    }
    memcpy(hex, digest, 16); hex[16] = '\0';
    if (k == used && used < sizeof(seen) / sizeof(seen[0])) used++;
    if (k < used) {
        memcpy(seen[k].name, name, length); seen[k].name[length] = '\0';
        seen[k].mtime = st.st_mtime; seen[k].size = st.st_size; memcpy(seen[k].hex, hex, 17);
    }
    return 1;
}

/* A new body carrying ?v= on every same-origin /assets/ reference that names a
 * file this release holds; NULL when nothing was named. */
static char *wire_version_assets(const char *body, size_t body_len, size_t *out_len) {
    static const char marker[] = "\"/assets/";
    char *out = NULL, hex[17];
    size_t used = 0, cap = 0, from = 0;
    const char *at = body, *end = body + body_len;
    while ((at = memmem(at, (size_t)(end - at), marker, sizeof(marker) - 1))) {
        const char *name = at + sizeof(marker) - 1, *stop = name;
        const char *quoted;
        while (stop < end && (isalnum((unsigned char)*stop) || *stop == '-' || *stop == '_' || *stop == '.')) stop++;
        /* A hand-set ?v=N names a release by hope; the digest replaces it. */
        quoted = stop;
        if (end - stop > 3 && !memcmp(stop, "?v=", 3))
            for (quoted = stop + 3; quoted < end && isalnum((unsigned char)*quoted); quoted++) {}
        if (quoted < end && *quoted == '"' && wire_asset_version(name, (size_t)(stop - name), hex)) {
            size_t chunk = (size_t)(stop - body) - from;
            if (used + chunk + 20 > cap) {
                char *grown;
                cap = (used + chunk + 20) * 2 + body_len / 4;
                if (!(grown = realloc(out, cap))) { free(out); return NULL; }
                out = grown;
            }
            memcpy(out + used, body + from, chunk); used += chunk;
            used += (size_t)snprintf(out + used, cap - used, "?v=%s", hex);
            from = (size_t)(quoted - body);
        }
        at = quoted;
    }
    if (!out) return NULL;
    if (used + (body_len - from) + 1 > cap) {
        char *grown = realloc(out, used + (body_len - from) + 1);
        if (!grown) { free(out); return NULL; }
        out = grown;
    }
    memcpy(out + used, body + from, body_len - from); used += body_len - from;
    *out_len = used;
    return out;
}

/* The header block again, with Content-Length dropped when the body changed,
 * Cache-Control replaced when a policy is forced, and the block left open so
 * the caller's remaining lines and terminator follow. */
static char *wire_rehead(const char *raw, size_t head_len, int body_changed, size_t body_len,
                         const char *policy, size_t *out_len) {
    char *head = malloc(head_len + 128);
    const char *line = raw, *stop = raw + head_len - 2;
    size_t used = 0;
    if (!head) return NULL;
    while (line < stop) {
        const char *next = memmem(line, (size_t)(stop - line), "\r\n", 2);
        size_t length = next ? (size_t)(next - line) + 2 : (size_t)(stop - line);
        /* A validator named the old bytes; the one derived below names these. */
        if (!(body_changed && (!strncasecmp(line, "Content-Length:", 15) || !strncasecmp(line, "ETag:", 5))) &&
            !(policy && !strncasecmp(line, "Cache-Control:", 14))) {
            memcpy(head + used, line, length); used += length;
            if (!next) { memcpy(head + used, "\r\n", 2); used += 2; }
        }
        line += length;
    }
    if (policy) used += (size_t)snprintf(head + used, 128, "Cache-Control: %s\r\n", policy);
    if (body_changed) used += (size_t)snprintf(head + used, 64, "Content-Length: %zu\r\n", body_len);
    *out_len = used;
    return head;
}

static void wire_send(int socket_fd, FILE *captured, const char *accept_encoding, const char *if_none_match) {
    long size; char *raw = NULL, *split, *body, *packed = NULL; size_t body_len, packed_len = 0, head_len;
    char etag[80], extra[256];
    if (!captured) return;
    fflush(captured);
    if (fseek(captured, 0, SEEK_END) != 0 || (size = ftell(captured)) <= 0) return;
    if (!(raw = malloc((size_t)size + 1))) return;
    rewind(captured);
    if (fread(raw, 1, (size_t)size, captured) != (size_t)size) { free(raw); return; }
    raw[size] = '\0';
    split = strstr(raw, "\r\n\r\n");
    if (!split) { (void)!write(socket_fd, raw, (size_t)size); free(raw); return; }
    head_len = (size_t)(split - raw) + 4;
    body = split + 4;
    body_len = (size_t)size - head_len;
    {
        int status = wire_status(raw), html;
        const char *forced = status >= 400 || status == 302 || status == 303 || status == 307 ? "no-store" : NULL;
        char *versioned = NULL, *head;
        size_t versioned_len = 0, new_head = 0;
        *split = '\0'; html = status == 200 && wire_find(raw, "\ncontent-type: text/html") != NULL; *split = '\r';
        if (html && body_len) versioned = wire_version_assets(body, body_len, &versioned_len);
        if ((forced || versioned) &&
            (head = wire_rehead(raw, head_len, versioned != NULL, versioned_len, forced, &new_head))) {
            const char *from = versioned ? versioned : body;
            size_t length = versioned ? versioned_len : body_len;
            char *rebuilt = malloc(new_head + 2 + length + 1);
            if (rebuilt) {
                memcpy(rebuilt, head, new_head);
                memcpy(rebuilt + new_head, "\r\n", 2);
                memcpy(rebuilt + new_head + 2, from, length);
                rebuilt[new_head + 2 + length] = '\0';
                free(raw); raw = rebuilt;
                size = (long)(new_head + 2 + length);
                head_len = new_head + 2; split = raw + head_len - 4;
                body = raw + head_len; body_len = length;
            }
            free(head);
        }
        free(versioned);
    }
    etag[0] = extra[0] = '\0';
    /* What this body is, so a caller that already has it is told so instead of
     * being sent it again. */
    if (body_len && !wire_find(raw, "\netag:") && !strncmp(raw, "HTTP/1.1 200", 12)) {
        char digest[65];
        bf_sha256_hex((const uint8_t *)body, body_len, digest);
        snprintf(etag, sizeof(etag), "\"%.32s\"", digest);
        if (if_none_match && *if_none_match && strstr(if_none_match, etag + 1)) {
            char answer[512],policy[160]="public, max-age=60";
            /* A script can contain the words no-store or no-cache. Only the
             * response header decides the validation response's policy. */
            const char *control=wire_find(raw,"\nCache-Control:");
            if(control&&control<split){
                control+=sizeof("\nCache-Control:")-1;while(*control==' '||*control=='\t')control++;
                size_t length=strcspn(control,"\r\n");
                if(length&&length<sizeof(policy)&&(size_t)(split-control)>=length){memcpy(policy,control,length);policy[length]=0;}
            }
            int n = snprintf(answer, sizeof(answer),
                             "HTTP/1.1 304 Not Modified\r\nETag: %s\r\nCache-Control: %s\r\nConnection: close\r\n\r\n",
                             etag, policy);
            (void)!write(socket_fd, answer, (size_t)n);
            free(raw);
            return;
        }
        snprintf(extra, sizeof(extra), "ETag: %s\r\n", etag);
    }
    if (body_len && accept_encoding && wire_find(accept_encoding, "gzip") &&
        wire_worth_compressing(raw, head_len, body_len) && wire_gzip(body, body_len, &packed, &packed_len) &&
        packed_len < body_len) {
        char head[8192];
        size_t keep = head_len - 2;            /* keep the header block's own CRLF terminator off */
        if (keep > sizeof(head) - 256) keep = sizeof(head) - 256;
        memcpy(head, raw, keep);
        head[keep] = '\0';
        /* The original Content-Length described the body before it was packed,
         * and a second one is written below for the packed body. A response
         * carrying two of them is malformed: curl tolerates it and every
         * browser refuses the page outright, which made each composed room
         * unreadable in exactly the client it was written for. The stale line
         * is dropped rather than the new one being appended beside it. */
        {
            char *line = head;
            while (line && *line) {
                char *next = strstr(line, "\r\n");
                if (!strncasecmp(line, "Content-Length:", 15)) {
                    size_t drop = next ? (size_t)(next + 2 - line) : strlen(line);
                    memmove(line, line + drop, strlen(line + drop) + 1);
                    keep -= drop;
                    continue;          /* another may follow */
                }
                if (!next) break;
                line = next + 2;
            }
        }
        (void)!write(socket_fd, head, keep);
        {
            char tail[512];
            int n = snprintf(tail, sizeof(tail), "%sContent-Encoding: gzip\r\nContent-Length: %zu\r\nVary: Accept-Encoding\r\n\r\n",
                             extra, packed_len);
            (void)!write(socket_fd, tail, (size_t)n);
        }
        (void)!write(socket_fd, packed, packed_len);
        free(packed);
        free(raw);
        return;
    }
    free(packed);
    if (extra[0]) {
        size_t keep = head_len - 2;
        (void)!write(socket_fd, raw, keep);
        (void)!write(socket_fd, extra, strlen(extra));
        (void)!write(socket_fd, "\r\n", 2);
        (void)!write(socket_fd, body, body_len);
    } else (void)!write(socket_fd, raw, (size_t)size);
    free(raw);
}

/* This is the declared `SurfaceIR:*` lowerer consumer. The AddressPlane
 * outbox supplies an immutable dependency-cut envelope; consuming it retires
 * matching derived public bodies and nothing else. Rendering is demand-led on
 * the next anonymous read, so a FactDelta cannot fan out into eager origin
 * work or accidentally execute an institutional action. */
static int public_projection_lower_handoff(const char *cache, const char *handoff) {
    FILE *file = NULL;
    char body[8192], identity[768] = {0}, lowerer[256] = {0}, schema[128] = {0};
    sqlite3 *json_db = NULL;
    sqlite3_stmt *st = NULL;
    size_t used;
    int invalidated = 0;
    if (!cache || !*cache || !handoff || !*handoff || !(file = fopen(handoff, "rb"))) return 0;
    used = fread(body, 1, sizeof(body) - 1, file);
    fclose(file);
    body[used] = '\0';
    if (!used || sqlite3_open(":memory:", &json_db) != SQLITE_OK ||
        sqlite3_prepare_v2(json_db,
          "SELECT json_extract(?1,'$.schema'),json_extract(?1,'$.identity'),json_extract(?1,'$.lowerer_ref') "
          "WHERE json_valid(?1)", -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, body, (int)used, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW || !sqlite3_column_text(st, 0) ||
        !sqlite3_column_text(st, 1) || !sqlite3_column_text(st, 2)) goto out;
    snprintf(schema, sizeof(schema), "%s", (const char *)sqlite3_column_text(st, 0));
    snprintf(identity, sizeof(identity), "%s", (const char *)sqlite3_column_text(st, 1));
    snprintf(lowerer, sizeof(lowerer), "%s", (const char *)sqlite3_column_text(st, 2));
    if (strcmp(schema, "bonfyre-lowering-handoff.v1") || strncmp(lowerer, "SurfaceIR:", 10) ||
        !identity[0]) goto out;
    if (!bf_public_projection_cache_invalidate(cache, identity, &invalidated)) goto out;
    printf("{\"lowerer\":"); json(lowerer);
    printf(",\"identity\":"); json(identity);
    printf(",\"invalidated\":%d,\"state\":\"derived_projection_retired\","
           "\"forbidden_inference\":\"a SurfaceIR dependency cut does not mutate CMS, owner state, authority, evidence, commitment, or effect state\"}\n", invalidated);
    sqlite3_finalize(st);
    sqlite3_close(json_db);
    return 1;
out:
    if (st) sqlite3_finalize(st);
    if (json_db) sqlite3_close(json_db);
    return 0;
}

static int public_projection_drain_directory(const char *cache, const char *directory,
                                             int limit, int *handled_out) {
    DIR *dir = NULL;
    struct dirent *entry;
    int handled = 0;
    if (handled_out) *handled_out = 0;
    if (!cache || !*cache || !directory || !*directory || limit < 1 ||
        !(dir = opendir(directory))) return 0;
    while (handled < limit && (entry = readdir(dir)) != NULL) {
        char path[PATH_MAX], done[PATH_MAX];
        size_t name_len;
        if (entry->d_name[0] == '.') continue;
        name_len = strlen(entry->d_name);
        if (name_len < 6 || strcmp(entry->d_name + name_len - 5, ".json") ||
            snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name) >= (int)sizeof(path) ||
            snprintf(done, sizeof(done), "%s.handled", path) >= (int)sizeof(done)) continue;
        if (!public_projection_lower_handoff(cache, path) || rename(path, done) != 0) {
            closedir(dir);
            return 0;
        }
        handled++;
    }
    closedir(dir);
    if (handled_out) *handled_out = handled;
    return 1;
}

/* The AddressPlane outbox partitions handoffs by declared lowerer. This worker
 * recognizes the SurfaceIR partitions without interpreting other lowerers;
 * protocol, MCP, material and inverse-compiler envelopes remain for their
 * actual owners. A completed envelope is renamed, never deleted, so replay is
 * observable and a cache retirement is idempotent. */
static int public_projection_drain(const char *cache, const char *root, int limit) {
    DIR *dir = NULL;
    struct dirent *entry;
    int total = 0;
    if (!cache || !*cache || !root || !*root || limit < 1 || !(dir = opendir(root))) return 0;
    while (total < limit && (entry = readdir(dir)) != NULL) {
        char child[PATH_MAX];
        struct stat st;
        int handled = 0;
        if (strncmp(entry->d_name, "SurfaceIR-", 10) ||
            snprintf(child, sizeof(child), "%s/%s", root, entry->d_name) >= (int)sizeof(child) ||
            stat(child, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        if (!public_projection_drain_directory(cache, child, limit - total, &handled)) {
            closedir(dir);
            return 0;
        }
        total += handled;
    }
    closedir(dir);
    printf("{\"worker\":\"public-projection\",\"handoffs_handled\":%d,"
           "\"state\":\"derived_cuts_retired\","
           "\"forbidden_inference\":\"a drained SurfaceIR handoff does not execute a renderer, mutate an owner, or perform an effect\"}\n", total);
    return 1;
}

#include "public_asset_export.h"

/* ------------------------------------------------------------ publish-static
 *
 * The estate's pages as files, composed in this process exactly as a request
 * composes them, for any host that serves files: every published host's own
 * page and each territory's routes, written as <out>/h/<host>/<route>/index.html.
 * A page that does not compose (not 200) is not written; nothing is fetched,
 * admitted or changed.  --shard i/n splits the hosts so n processes publish
 * in parallel, each writing only its own share. */
static int publish_static_one(const char *root, const char *out, const char *host, const char *route) {
    char dir[PATH_MAX], path[PATH_MAX], tmp[PATH_MAX];
    FILE *capture = tmpfile();
    int fd, saved, ok, status = 0;
    long size;
    char *page, *body;
    if (!capture) return 0;
    fflush(stdout);
    fd = fileno(capture);
    saved = dup(STDOUT_FILENO);
    (void)dup2(fd, STDOUT_FILENO);
    /* The owner a request reaches first answers first: a site program (a
     * client's site, a member's page) before the estate's own route. */
    snprintf(estate_request_territory, sizeof(estate_request_territory), "%s", territory_of(host));
    ok = bf_site_mcp_serve("GET", route, "", "", host, "", "", "");
    if (!ok) ok = serve_site_program("GET", route, NULL, host, "text/html", NULL, NULL);
    if (!ok) ok = emit_page_program_route(root, route, host, NULL);
    fflush(stdout);
    (void)dup2(saved, STDOUT_FILENO); close(saved);
    if (!ok || fseek(capture, 0, SEEK_END) || (size = ftell(capture)) <= 0 || fseek(capture, 0, SEEK_SET) || !(page = malloc((size_t)size + 1))) { fclose(capture); return 0; }
    size = (long)fread(page, 1, (size_t)size, capture); fclose(capture); page[size] = '\0';
    if (sscanf(page, "HTTP/1.1 %d", &status) != 1 || status != 200 || !(body = strstr(page, "\r\n\r\n"))) { free(page); return 0; }
    body += 4;
    if (snprintf(dir, sizeof(dir), "%s/h/%s%s", out, host, strcmp(route, "/") ? route : "") >= (int)sizeof(dir) ||
        snprintf(path, sizeof(path), "%s/index.html", dir) >= (int)sizeof(path) ||
        snprintf(tmp, sizeof(tmp), "%s.%d.tmp", path, (int)getpid()) >= (int)sizeof(tmp)) { free(page); return 0; }
    {   /* mkdir -p */
        char partial[PATH_MAX]; snprintf(partial, sizeof(partial), "%s", dir);
        for (char *p = partial + 1; *p; p++) if (*p == '/') { *p = '\0'; mkdir(partial, 0755); *p = '/'; }
        mkdir(partial, 0755);
    }
    FILE *f = fopen(tmp, "wb");
    ok = f && fwrite(body, 1, strlen(body), f) == strlen(body);
    if (f && fclose(f)) ok = 0;
    ok = ok && rename(tmp, path) == 0;
    if (!ok) remove(tmp);
    free(page);
    return ok;
}

static int publish_static_cli(const char *root, const char *out, const char *routes, const char *shard, const char *only) {
    sqlite3 *cms; sqlite3_stmt *st = NULL;
    unsigned index = 0, count = 1, n = 0;
    int written = 0, skipped = 0;
    struct timespec started, ended;
    static const char *const territories[] = {"aurekai.life", "aurekai.live", "aurekai.shop", "aurekai.work"};
    if (!root || !*root || !out || !*out) { fputs("publish-static: --site-root and --out required\n", stderr); return 2; }
    if (shard && (sscanf(shard, "%u/%u", &index, &count) != 2 || !count || index >= count)) { fputs("publish-static: --shard i/n with i < n\n", stderr); return 2; }
    mkdir(out, 0755);
    clock_gettime(CLOCK_REALTIME, &started);
    /* Each territory's routes, on shard 0; --only names the ones wanted, as
     * whole words, so "aurekai.live" does not also mean every host under it. */
    if (index == 0) {
        const char *list = routes && *routes ? routes : "";
        for (size_t t = 0; t < sizeof(territories) / sizeof(territories[0]); t++) {
            if (only && *only) {
                const char *at = only; size_t len = strlen(territories[t]); int named = 0;
                while ((at = strstr(at, territories[t]))) {
                    if ((at == only || at[-1] == ' ' || at[-1] == ',') && (!at[len] || at[len] == ' ' || at[len] == ',')) { named = 1; break; }
                    at += len;
                }
                if (!named) continue;
            }
            for (size_t i = 0; i < BF_PAGE_PROGRAM_COUNT; i++) {
                const BfPageProgram *program = &bf_page_programs[i];
                if (strcmp(program->territory, "any") && strcmp(program->territory, territories[t] + 8)) continue;
                if (*list && !strstr(list, program->route)) continue;
                if (publish_static_one(root, out, territories[t], program->route)) written++; else skipped++;
            }
        }
    }
    /* Every published host's own page, this shard's share of them. */
    if (!(cms = cms_open_ro())) { fputs("publish-static: BONFYRE_CMS_DB unreadable\n", stderr); return 2; }
    if (sqlite3_prepare_v2(cms, "SELECT DISTINCT host FROM site_presentation WHERE status='published' AND host LIKE '%.aurekai.%' ORDER BY host", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *host = (const char *)sqlite3_column_text(st, 0);
            if (!host || strspn(host, "abcdefghijklmnopqrstuvwxyz0123456789.-") != strlen(host)) continue;
            if (only && *only && !strstr(only, host)) continue;
            if (n++ % count != index) continue;
            if (publish_static_one(root, out, host, "/")) written++; else skipped++;
        }
    }
    sqlite3_finalize(st); sqlite3_close(cms);
    clock_gettime(CLOCK_REALTIME, &ended);
    printf("{\"written\":%d,\"not_composed\":%d,\"shard\":\"%u/%u\",\"seconds\":%.1f}\n", written, skipped, index, count,
           (double)(ended.tv_sec - started.tv_sec) + (double)(ended.tv_nsec - started.tv_nsec) / 1e9);
    return written ? 0 : 1;
}

/* Compose every public route once, so the first person through the door is not
 * the one who pays for it.  A page is kept for a ten-minute bucket of the
 * clock; a warmer run more often than that means no visitor ever meets a cold
 * one.  It reads exactly what a request reads and writes nothing but the
 * cache: no source is fetched, nothing is admitted, and a route that cannot be
 * composed is reported rather than hidden. */
static int page_warm_cli(const char *root, const char *host) {
    size_t i; int warmed = 0, missed = 0;
    struct timespec started, ended;
    int devnull = open("/dev/null", O_WRONLY), saved = -1;
    if (!root || !*root) { fputs("page-warm: --site-root required\n", stderr); return 2; }
    if (!public_projection_cache_db()) { fputs("page-warm: BONFYRE_PUBLIC_PROJECTION_DB not set\n", stderr); return 2; }
    clock_gettime(CLOCK_REALTIME, &started);
    for (i = 0; i < BF_PAGE_PROGRAM_COUNT; i++) {
        const BfPageProgram *program = &bf_page_programs[i];
        const char *as = host && *host ? host : NULL;
        char apex[64];
        int ok;
        if (!as) {
            /* A route belongs to a territory; compose it as that territory's
             * own address, which is how a visitor asks for it. */
            snprintf(apex, sizeof(apex), "aurekai.%s", strcmp(program->territory, "any") ? program->territory : "life");
            as = apex;
        }
        fflush(stdout);
        if (devnull >= 0) { saved = dup(STDOUT_FILENO); (void)dup2(devnull, STDOUT_FILENO); }
        ok = emit_page_program_route(root, program->route, as, NULL);
        fflush(stdout);
        if (saved >= 0) { (void)dup2(saved, STDOUT_FILENO); close(saved); saved = -1; }
        if (ok) warmed++; else missed++;
    }
    {   /* The stage is its own program and answers on every territory. */
        static const char *const extra[] = {"/stage", "/channels", "/podcasts"};
        size_t e;
        for (e = 0; e < sizeof(extra) / sizeof(extra[0]); e++) {
            int ok;
            fflush(stdout);
            if (devnull >= 0) { saved = dup(STDOUT_FILENO); (void)dup2(devnull, STDOUT_FILENO); }
            ok = emit_page_program_route(root, extra[e], host && *host ? host : "aurekai.life", NULL);
            fflush(stdout);
            if (saved >= 0) { (void)dup2(saved, STDOUT_FILENO); close(saved); saved = -1; }
            if (ok) warmed++;
        }
    }
    if (devnull >= 0) close(devnull);
    clock_gettime(CLOCK_REALTIME, &ended);
    printf("{\"warmed\":%d,\"not_composed\":%d,\"seconds\":%.2f,"
           "\"cannot_infer\":\"a warmed page is a composition kept for a moment, never a source read, "
           "an admission, or a claim that what it shows is still current\"}\n",
           warmed, missed,
           (double)(ended.tv_sec - started.tv_sec) + (double)(ended.tv_nsec - started.tv_nsec) / 1e9);
    return warmed ? 0 : 1;
}

/* A served estate is one directory: matrix.db beside the stores its pages
 * read.  A store variable the service left unset is the sibling file of that
 * name when it exists, so a page section cannot go silently empty because
 * one line was missing from a service definition; an explicit variable still
 * wins, and a store that is not there stays unset. */
static void web_serve_estate_stores(const char *matrix_db) {
    static const char *const stores[][2] = {
        {"BONFYRE_MATRIX_DB", "matrix.db"}, {"BONFYRE_CMS_DB", "cms.db"}, {"BONFYRE_AUTH_DB", "auth.db"},
        {"BONFYRE_ITEM_ARCHIVE_DB", "items.db"}, {"BONFYRE_SUBJECT_FABRIC_DB", "subject_fabric.db"},
        {"BONFYRE_SOURCE_FRONTIER_DB", "frontier.db"}, {"BONFYRE_ENTITY_DB", "entity.db"},
        {"BONFYRE_LATENT_ATLAS_DB", "atlas.db"}, {"BONFYRE_EXTERNAL_ESTATE_DB", "external-estate.db"},
        {"BONFYRE_PUBLIC_PROJECTION_DB", "projection.db"}, {"BONFYRE_METER_DB", "meter.db"},
        {"BONFYRE_ECONOMY_DB", "economy.db"}, {"BONFYRE_PRICES_DB", "prices.db"},
        {"BONFYRE_FINANCE_DB", "finance.db"}, {"BONFYRE_COMMITMENT_DB", "commitments.db"}, {"BONFYRE_COMMERCE_DB", "commerce.db"},
        {"BONFYRE_FRAPPE_SURFACE_DB", "frappe-surfaces.db"}, {"BONFYRE_FOREIGN_ROOT", "foreign"},
    };
    char dir[PATH_MAX], path[PATH_MAX];
    const char *slash = strrchr(matrix_db, '/');
    struct stat st;
    if (!slash) snprintf(dir, sizeof(dir), ".");
    else if ((size_t)(slash - matrix_db) < sizeof(dir)) { memcpy(dir, matrix_db, (size_t)(slash - matrix_db)); dir[slash - matrix_db] = '\0'; }
    else return;
    for (size_t i = 0; i < sizeof(stores) / sizeof(stores[0]); i++) {
        const char *set = getenv(stores[i][0]);
        if (set && *set) continue;
        if (snprintf(path, sizeof(path), "%s/%s", dir, stores[i][1]) >= (int)sizeof(path) || stat(path, &st)) continue;
        setenv(stores[i][0], path, 0);
    }
    /* The model catalogue lives with the local model tooling, not beside the
     * estate. Left unset, Model Commons answered 404 for every visitor and the
     * activity rail's link to it went nowhere. */
    {
        const char *set = getenv("BONFYRE_MODEL_CATALOG"), *home = getenv("HOME");
        if ((!set || !*set) && home && *home &&
            snprintf(path, sizeof(path), "%s/.local/share/bonfyre/catalog.db", home) < (int)sizeof(path) && !stat(path, &st))
            setenv("BONFYRE_MODEL_CATALOG", path, 0);
    }
}

static void usage(void){fputs("usage: bonfyre-web-address bootstrap-owned|subject|address|alias|institution|relate|presence|presence-withdraw|domain|presence|standing|standing-members|constellation ... --db FILE\n       bonfyre-web-address closure-contribution --db FILE --subject ID --presence ID --plane material|latent|execution|economic --owner OWNER --resource URI --representation FORMAT --digest HEX --evidence RECEIPT\n       bonfyre-web-address closure-contributions --db FILE --subject ID --presence ID [--plane PLANE]\n       bonfyre-web-address resolve --db FILE --host HOST --path PATH [--world live --audience public --repr HTML]\n       bonfyre-web-address closure --db FILE (--host HOST [--path PATH] | --subject ID --presence ID) [--format ro-crate|zarr]\n       bonfyre-web-address public-projection-invalidate --cache-db FILE --dependency COORDINATE\n       bonfyre-web-address public-projection-lower --cache-db FILE --handoff FILE\n       bonfyre-web-address public-projection-drain --cache-db FILE --lowerer-dir DIRECTORY [--limit N]\n       bonfyre-web-address serve --db FILE [--port 8787 --workers 1..64 --backlog 1024 --site-root DIR]\n",stderr);}
/* The census as items, exactly as pages read it: one JSON line per retained
 * item with its page key, kind, words, links, identities and place.  Automation
 * (identity linking, the atlas, source quality) reads this instead of
 * re-implementing the census contract. */
static const char *page_material_name(int kind) {
    switch (kind) {
    case BF_PAGE_MATERIAL_VIDEO: return "video"; case BF_PAGE_MATERIAL_PHOTO: return "photo"; case BF_PAGE_MATERIAL_AUDIO: return "audio";
    case BF_PAGE_MATERIAL_PLACE: return "place"; case BF_PAGE_MATERIAL_POST: return "post";
    case BF_PAGE_MATERIAL_EVENT: return "event"; default: return "story";
    }
}

static int census_items_cli(const char *only) {
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; PageMaterial *material = malloc(sizeof(PageMaterial)); int sources = 0, items = 0;
    if (!db || !material || sqlite3_prepare_v2(db, "SELECT source,COALESCE(family,'') FROM internet_source WHERE status='published' AND COALESCE(twin,'')<>'' "
          "AND (?1 IS NULL OR source=?1) GROUP BY source ORDER BY MIN(id)", -1, &st, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); free(material); return 2; }
    if (only) sqlite3_bind_text(st, 1, only, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *source = (const char *)sqlite3_column_text(st, 0), *family = (const char *)sqlite3_column_text(st, 1);
        int k, i;
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) material->stream[k].count = material->stream[k].cursor = 0;
        page_read_everything = 1;
        page_per_source = PAGE_STREAM_MAX;
        page_collect_stories(db, source, material);
        sources++;
        for (k = 0; k < BF_PAGE_MATERIAL_COUNT; k++) {
            if (k == BF_PAGE_MATERIAL_MIX || k == BF_PAGE_MATERIAL_PLACE) continue;
            for (i = 0; i < material->stream[k].count; i++) {
                const PageItem *it = &material->stream[k].item[i];
                fputs("{\"source\":", stdout); json(source); fputs(",\"family\":", stdout); json(family);
                fputs(",\"key\":", stdout); json(it->key); printf(",\"kind\":\"%s\",\"title\":", page_material_name(it->kind)); json(it->title);
                fputs(",\"by\":", stdout); json(it->by); fputs(",\"publisher\":", stdout); json(it->note);
                fputs(",\"href\":", stdout); json(it->href); fputs(",\"group\":", stdout); json(it->group); fputs(",\"image\":", stdout); json(it->image); fputs(",\"asset\":", stdout); json(it->asset); fputs(",\"poster\":", stdout); json(it->poster);
                fputs(",\"body\":", stdout); json(it->body); fputs(",\"ident\":", stdout); json(it->ident); fputs(",\"about\":", stdout); json(it->about);
                fputs(",\"rights\":", stdout); json(it->rights); fputs(",\"rights_href\":", stdout); json(it->rights_href);
                fputs(",\"origin\":", stdout); json(it->origin); fputs(",\"origin_href\":", stdout); json(it->origin_href);
                fputs(",\"people\":", stdout); json(it->people); fputs(",\"transcript_href\":", stdout); json(it->transcript_href); fputs(",\"chapters_href\":", stdout); json(it->chapters_href); fputs(",\"talk_href\":", stdout); json(it->talk_href);
                /* Where the source said the thing is: an event's own place line,
                 * an episode's stated place.  It is their words, not a resolved
                 * place, and whoever joins it must treat it as words. */
                fputs(",\"where\":", stdout); json(it->where);
                printf(",\"at\":%lld,\"duration_ms\":%ld,\"placed\":%s",
                       (long long)it->at, it->duration_ms, it->placed ? "true" : "false");
                if (it->placed) printf(",\"lon\":%.6f,\"lat\":%.6f", it->lon, it->lat);
                fputs("}\n", stdout);
                items++;
            }
        }
    }
    sqlite3_finalize(st); sqlite3_close(db); free(material);
    fprintf(stderr, "{\"sources\":%d,\"items\":%d}\n", sources, items);
    return 0;
}

/* What each public route would show right now, by the source each item came
 * from: the receipt that lets the source ledger say which admitted channels
 * reach a person and which never do.  It composes the routes exactly as the
 * server does and shows nobody anything; a receipt is a measurement of this
 * estate's own surfaces, never of a reader. */
static int page_contributions_cli(const char *only_route) {
    sqlite3 *db = cms_open_ro(); PageMaterial *material = NULL; size_t i;
    char day[16]; time_t now = time(NULL); struct tm tm;
    int routes = 0;
    if (!db) return 2;
    gmtime_r(&now, &tm);
    strftime(day, sizeof(day), "%Y-%m-%d", &tm);
    for (i = 0; i < BF_PAGE_PROGRAM_COUNT; i++) {
        const BfPageProgram *program = &bf_page_programs[i];
        char sources[4096]; unsigned chosen; size_t r; int k, visible = 0, contributing = 0;
        struct { char source[96]; int items; } seen[128]; int seen_count = 0;
        if (only_route && strcmp(program->route, only_route)) continue;
        if (!strcmp(program->route, "/")) continue;      /* a territory home previews the routes below it */
        chosen = page_program_sources(db, program, program->source_limit, sources, sizeof(sources));
        if (!(material = calloc(1, sizeof(*material)))) break;
        page_collect_stories(db, sources, material);
        for (r = 0; r < program->region_count; r++) {
            const BfPageRegion *region = &bf_page_regions[program->regions[r]];
            const PageStream *stream = &material->stream[region->material];
            int take = stream->count < (int)region->max_items ? stream->count : (int)region->max_items;
            for (k = 0; k < take; k++) {
                const PageItem *it = &stream->item[k];
                int f;
                if (!it->source[0]) continue;
                for (f = 0; f < seen_count; f++) if (!strcmp(seen[f].source, it->source)) break;
                if (f == seen_count) {
                    if (seen_count >= (int)(sizeof(seen) / sizeof(seen[0]))) continue;
                    snprintf(seen[seen_count].source, sizeof(seen[seen_count].source), "%s", it->source);
                    seen[seen_count].items = 0;
                    seen_count++;
                }
                seen[f].items++;
                visible++;
            }
        }
        for (k = 0; k < seen_count; k++) {
            if (!seen[k].items) continue;
            contributing++;
            fputs("{\"route\":", stdout); json(program->route);
            fputs(",\"source\":", stdout); json(seen[k].source);
            printf(",\"items\":%d,\"day\":", seen[k].items); json(day); fputs("}\n", stdout);
        }
        fputs("{\"surface_receipt\":", stdout); json(program->route);
        printf(",\"sources_chosen\":%u,\"sources_contributing\":%d,\"visible_items\":%d,\"source_limit\":%u,\"families\":%u,\"day\":",
               chosen, contributing, visible, program->source_limit, (unsigned)program->family_count);
        json(day); fputs("}\n", stdout);
        free(material); material = NULL;
        routes++;
    }
    free(material);
    sqlite3_close(db);
    fprintf(stderr, "{\"routes\":%d}\n", routes);
    return 0;
}

/* Keep every item the census produced, once, with when it was first and last
 * read.  A title that changed updates in place; nothing is ever removed
 * because its source stopped publishing it. */
static int archive_items_cli(const char *path) {
    sqlite3 *archive = page_archive_open(1); FILE *f = path && strcmp(path, "-") ? fopen(path, "r") : stdin;
    sqlite3_stmt *st = NULL; char *line = NULL; size_t cap = 0; ssize_t len; char stamp[32];
    int kept = 0;
    struct tm tm; time_t now = time(NULL);
    if (!archive) { fprintf(stderr, "archive-items: BONFYRE_ITEM_ARCHIVE_DB is not set\n"); if (f && f != stdin) fclose(f); return 2; }
    if (!f) { perror(path); sqlite3_close(archive); return 2; }
    gmtime_r(&now, &tm); strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%SZ", &tm);
    sqlite3_prepare_v2(archive, "INSERT INTO item(source,key,kind,family,title,body,href,image,by,publisher,ident,at,first_read,last_read)"
        " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13)"
        /* A thing already kept is re-filed as well as re-read.  Leaving kind
         * and family out of this made the archive remember the first answer
         * for ever: a source refiled under the family it actually carries kept
         * emitting its old one, and a territory composing from that family
         * stayed empty while the material sat in the archive under the wrong
         * name. */
        " ON CONFLICT(source,key) DO UPDATE SET kind=?3, family=?4, title=?5, body=?6, href=?7, image=?8, by=?9, publisher=?10, ident=?11, at=?12, last_read=?13",
        -1, &st, NULL);
    sqlite3_exec(archive, "BEGIN", NULL, NULL, NULL);
    while ((len = getline(&line, &cap, f)) > 0) {
        char err[160], source[128], key[40], kind[32], family[96], title[1024], body[1024], href[1024], image[1024], by[256], publisher[256], ident[64];
        bf_json_doc_t *doc; const bf_json_node_t *root, *at_node; long long at = 0;
        if (len < 3) continue;
        doc = bf_json_parse(line, (size_t)len, err, sizeof(err));
        root = doc ? bf_json_root(doc) : NULL;
        if (!root) { if (doc) bf_json_free(doc); continue; }
#define AFIELD(name, buf) do { const bf_json_node_t *n = bf_json_obj_get(doc, root, name); buf[0] = '\0'; \
        if (n && n->type == BF_JSON_STRING) bf_json_get_str_copy(n, buf, sizeof(buf)); } while (0)
        AFIELD("source", source); AFIELD("key", key); AFIELD("kind", kind); AFIELD("family", family);
        AFIELD("title", title); AFIELD("body", body); AFIELD("href", href); AFIELD("image", image);
        AFIELD("by", by); AFIELD("publisher", publisher); AFIELD("ident", ident);
#undef AFIELD
        at_node = bf_json_obj_get(doc, root, "at");
        if (at_node && at_node->type == BF_JSON_INT) at = (long long)bf_json_get_int(at_node);
        bf_json_free(doc);
        if (!source[0] || !key[0] || !title[0]) continue;
        sqlite3_reset(st);
        sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, kind[0] ? kind : "story", -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, family, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, title, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 6, body, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, href, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 8, image, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 9, by, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 10, publisher, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 11, ident, -1, SQLITE_TRANSIENT); sqlite3_bind_int64(st, 12, at);
        sqlite3_bind_text(st, 13, stamp, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_DONE) kept++;
    }
    sqlite3_exec(archive, "COMMIT", NULL, NULL, NULL);
    sqlite3_finalize(st); free(line);
    if (f != stdin) fclose(f);
    {
        sqlite3_stmt *count = NULL; int total = 0;
        if (sqlite3_prepare_v2(archive, "SELECT COUNT(*) FROM item", -1, &count, NULL) == SQLITE_OK && sqlite3_step(count) == SQLITE_ROW)
            total = sqlite3_column_int(count, 0);
        sqlite3_finalize(count);
        printf("{\"read\":%d,\"kept\":%d}\n", kept, total);
    }
    sqlite3_close(archive);
    return 0;
}

#define WEB_ADDRESS_COMMANDS(X) \
    X("--help", WC_HELP) \
    X("address", WC_ADDRESS) \
    X("alias", WC_ALIAS) \
    X("archive-items", WC_ARCHIVE_ITEMS) \
    X("bootstrap-owned", WC_BOOTSTRAP_OWNED) \
    X("census-items", WC_CENSUS_ITEMS) \
    X("closure", WC_CLOSURE) \
    X("closure-contribution", WC_CLOSURE_CONTRIBUTION) \
    X("closure-contributions", WC_CLOSURE_CONTRIBUTIONS) \
    X("constellation", WC_CONSTELLATION) \
    X("detail-depth", WC_DETAIL_DEPTH) \
    X("domain", WC_DOMAIN) \
    X("help", WC_HELP_ALIAS) \
    X("institution", WC_INSTITUTION) \
    X("page-contributions", WC_PAGE_CONTRIBUTIONS) \
    X("page-warm", WC_PAGE_WARM) \
    X("presence", WC_PRESENCE) \
    X("presence-withdraw", WC_PRESENCE_WITHDRAW) \
    X("public-projection-drain", WC_PUBLIC_PROJECTION_DRAIN) \
    X("public-projection-invalidate", WC_PUBLIC_PROJECTION_INVALIDATE) \
    X("public-projection-lower", WC_PUBLIC_PROJECTION_LOWER) \
    X("publish-assets", WC_PUBLISH_ASSETS) \
    X("publish-static", WC_PUBLISH_STATIC) \
    X("relate", WC_RELATE) \
    X("resolve", WC_RESOLVE) \
    X("serve", WC_SERVE) \
    X("site", WC_SITE) \
    X("skill-execute", WC_SKILL_EXECUTE) \
    X("standing", WC_STANDING) \
    X("standing-members", WC_STANDING_MEMBERS) \
    X("subject", WC_SUBJECT)

enum {
#define WEB_ADDRESS_ENUM(name, id) id,
    WEB_ADDRESS_COMMANDS(WEB_ADDRESS_ENUM)
#undef WEB_ADDRESS_ENUM
};
static const BfCommandEntry web_address_commands[] = {
#define WEB_ADDRESS_ENTRY(name, id) {name, id, 0, NULL},
    WEB_ADDRESS_COMMANDS(WEB_ADDRESS_ENTRY)
#undef WEB_ADDRESS_ENTRY
};

int main(int argc, char **argv) {
    /* The product face: "bonfyre-web-address site ..." or, under its own
     * name, "bonfyre-site ...". */
    {
        const char *base = strrchr(argv[0], '/');
        base = base ? base + 1 : argv[0];
        if (!strcmp(base, "bonfyre-site")) {
            char **shifted = calloc((size_t)argc + 2, sizeof(char *));
            if (!shifted) return 1;
            shifted[0] = argv[0]; shifted[1] = "site";
            for (int i = 1; i < argc; i++) shifted[i + 1] = argv[i];
            return bf_site_main(argc + 1, shifted, argv[0]);
        }
    }
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) { usage(); return 2; }
    const BfCommandEntry *command = bf_command_lookup(web_address_commands,
        sizeof(web_address_commands) / sizeof(web_address_commands[0]), argv[1]);
    if (!command) { usage(); return 2; }
    if (command->id == WC_HELP || command->id == WC_HELP_ALIAS) { usage(); return 0; }
    if (command->id == WC_SITE) return bf_site_main(argc, argv, argv[0]);
    /* Local direct invocation of a declared, free, in-process skill. This
     * reuses the A2A executor; paid skills are not made free by a CLI carrier. */
    if (command->id == WC_SKILL_EXECUTE) {
        const char *id=argc>2?argv[2]:NULL,*params=opt(argc,argv,"--params");
        const char *cms=opt(argc,argv,"--cms-db"),*items=opt(argc,argv,"--archive-db");
        if (!id||!params||strlen(params)>8192) {
            fputs("usage: skill-execute SKILL --params JSON [--cms-db FILE --archive-db FILE]\n",stderr);
            return 2;
        }
        const A2ASkill *skill=a2a_find(id);
        if (!skill||skill->capability||!skill->execute) {
            fputs("not a free, executable native skill\n",stderr);return 4;
        }
        if ((cms&&setenv("BONFYRE_CMS_DB",cms,1))||
            (items&&setenv("BONFYRE_ITEM_ARCHIVE_DB",items,1))) return 2;
        char *result=calloc(32768,1);
        if (!result) return 5;
        int accepted=skill->execute(params,result,32768);
        puts(result);free(result);
        return accepted?0:4;
    }
    /* detail-depth derive ARCHIVE BODIES OUT | detail-depth measure ARCHIVE FRONTIER DEPTH
     * Offline: facets from the retained bodies into their own store, and the
     * per-source yield before and after them. */
    if (command->id == WC_DETAIL_DEPTH) {
        if (argc >= 6 && !strcmp(argv[2], "derive"))
            return bf_detail_depth_derive(argv[3], argv[4], argv[5], stdout);
        if (argc >= 6 && !strcmp(argv[2], "measure"))
            return bf_detail_depth_measure(argv[3], argv[4], argv[5], stdout);
        usage();
        return 2;
    }
    switch (command->id) {
    case WC_CENSUS_ITEMS: return census_items_cli(opt(argc, argv, "--source"));
    case WC_PAGE_CONTRIBUTIONS: return page_contributions_cli(opt(argc, argv, "--route"));
    case WC_PAGE_WARM: return page_warm_cli(opt(argc, argv, "--site-root"), opt(argc, argv, "--host"));
    case WC_PUBLISH_ASSETS: return publish_assets_cli(opt(argc, argv, "--site-root"), opt(argc, argv, "--out"));
    case WC_PUBLISH_STATIC:
        if (opt(argc, argv, "--db")) web_serve_estate_stores(opt(argc, argv, "--db"));
        return publish_static_cli(opt(argc, argv, "--site-root"), opt(argc, argv, "--out"), opt(argc, argv, "--routes"),
                                  opt(argc, argv, "--shard"), opt(argc, argv, "--only"));
    case WC_ARCHIVE_ITEMS: return archive_items_cli(opt(argc, argv, "--items"));
    case WC_PUBLIC_PROJECTION_INVALIDATE: {
        const char *cache = opt(argc, argv, "--cache-db");
        const char *dependency = opt(argc, argv, "--dependency");
        int invalidated = 0;
        if (!cache || !*cache || !dependency || !*dependency ||
            !bf_public_projection_cache_invalidate(cache, dependency, &invalidated)) return 2;
        printf("{\"invalidated\":%d,\"dependency\":", invalidated); json(dependency);
        fputs(",\"forbidden_inference\":\"invalidating a derived public projection does not alter any canonical owner fact or cause a lowerer to execute\"}\n", stdout);
        return 0;
    }
    case WC_PUBLIC_PROJECTION_LOWER:
        return public_projection_lower_handoff(opt(argc, argv, "--cache-db"),
                                               opt(argc, argv, "--handoff")) ? 0 : 2;
    case WC_PUBLIC_PROJECTION_DRAIN: {
        int limit = opt_positive_int(argc, argv, "--limit", 64, 4096);
        return limit > 0 && public_projection_drain(opt(argc, argv, "--cache-db"),
                                                    opt(argc, argv, "--lowerer-dir"), limit) ? 0 : 2;
    }
    default: break;
    }
    /* Prefork before opening any native owner handle.  Children re-exec with
     * one inherited listening socket and independently open Matrix/SQLite. */
    if (command->id == WC_SERVE && !opt(argc, argv, "--listen-fd")) {
        int worker_count = opt_positive_int(argc, argv, "--workers", 1, BF_WEB_MAX_WORKERS);
        if (worker_count < 1) { usage(); return 2; }
        if (worker_count > 1) return serve_prefork(argc, argv);
    }
    const char *db = opt(argc, argv, "--db");
    BfMatrix *matrix = NULL;
    if (!db || bf_matrix_open(db, &matrix, stderr)) return 2;
    if (command->id == WC_SERVE) web_serve_estate_stores(db);
    int rc;
    switch (command->id) {
    case WC_BOOTSTRAP_OWNED: rc = bootstrap_owned(matrix); break;
    case WC_SUBJECT: rc = bind_subject(matrix, argc, argv); break;
    case WC_ADDRESS: rc = bind_address(matrix, argc, argv); break;
    case WC_ALIAS: rc = bind_alias(matrix, argc, argv); break;
    case WC_RELATE: rc = bind_relation(matrix, argc, argv); break;
    case WC_PRESENCE_WITHDRAW: rc = withdraw_presence(matrix, argc, argv); break;
    case WC_INSTITUTION: rc = bind_institution(matrix, argc, argv); break;
    case WC_DOMAIN: rc = bind_domain(matrix, argc, argv); break;
    case WC_PRESENCE: rc = bind_presence(matrix, argc, argv); break;
    case WC_CLOSURE_CONTRIBUTION: rc = bind_closure_contribution(matrix, argc, argv); break;
    case WC_CLOSURE_CONTRIBUTIONS: rc = closure_contributions_cli(matrix, argc, argv); break;
    case WC_STANDING: rc = bind_standing(matrix, argc, argv); break;
    case WC_STANDING_MEMBERS: rc = standing_members(matrix, argc, argv); break;
    case WC_CONSTELLATION: rc = constellation(matrix, argc, argv); break;
    case WC_RESOLVE: rc = resolve_cli(matrix, argc, argv); break;
    case WC_CLOSURE: rc = closure_cli(matrix, argc, argv); break;
    case WC_SERVE: rc = serve(matrix, argc, argv); break;
    default: rc = 2; break;
    }
    bf_matrix_close(matrix);
    if (rc == 2) usage();
    return rc;
}
static size_t world_federation_dependencies(sqlite3 *db, const BfWorldFederation *world,
                                            char storage[][640], const char **out,
                                            size_t cap) {
    sqlite3_stmt *st = NULL;
    size_t count;
    if (!world || !out || !storage || !cap) return 0;
    count = world_cache_dependencies(world->world, storage, out, cap);
    if (world->site_id[0] && count < cap &&
        snprintf(storage[count], sizeof(storage[count]), "cms:site_presentation:%s", world->site_id) < (int)sizeof(storage[count])) {
        out[count] = storage[count]; count++;
    }
    if (world->program_id[0] && count < cap &&
        snprintf(storage[count], sizeof(storage[count]), "cms:site_program:%s", world->program_id) < (int)sizeof(storage[count])) {
        out[count] = storage[count]; count++;
    }
    if (!db || count >= cap || sqlite3_prepare_v2(db,
          "SELECT 'cms:source_collection:'||value FROM json_each(?1) WHERE json_valid(?1) AND json_type(?1)='array' "
          "UNION ALL SELECT 'cms:capability_program:'||value FROM json_each(?2) WHERE json_valid(?2) AND json_type(?2)='array' ORDER BY 1",
          -1, &st, NULL) != SQLITE_OK) return count;
    sqlite3_bind_text(st, 1, world->source_collections, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, world->capability_programs, -1, SQLITE_TRANSIENT);
    while (count < cap && sqlite3_step(st) == SQLITE_ROW) {
        const char *dep = (const char *)sqlite3_column_text(st, 0); int duplicate = 0;
        if (!dep || !*dep || strlen(dep) >= sizeof(storage[count])) continue;
        for (size_t i = 0; i < count; i++) if (!strcmp(out[i], dep)) { duplicate = 1; break; }
        if (duplicate) continue;
        snprintf(storage[count], sizeof(storage[count]), "%s", dep);
        out[count] = storage[count]; count++;
    }
    sqlite3_finalize(st);
    return count;
}
