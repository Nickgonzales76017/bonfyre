#define _POSIX_C_SOURCE 200809L
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#include "markdown_fence.h"
#include "../../../lib/libbonfyre/include/bf_help.h"
/*
 * site — the product face of the estate's site machinery.
 *
 *   bonfyre site new NAME [--title T] [--tagline T] [--host H] [--accent #hex]
 *   bonfyre site list [--json]
 *   bonfyre site show NAME|HOST
 *   bonfyre site search TEXT [--top N]
 *   bonfyre site export NAME --out FILE.html
 *   bonfyre site import SOURCE --as NAME [--limit N]
 *   bonfyre site serve | stop
 *
 * Nothing here owns a site.  A site is the records the content store already
 * holds for it (presentation, template, slots, bindings, program and its
 * material) and the address facts the resolver already holds (subject,
 * domain, address, presence).  These verbs write those records through their
 * owners in the order the owners need, start the resolver's own server, and
 * read the result back.  Search is Recall's, text extraction is Ingest's and
 * passage cutting is Paragraph's.
 *
 * A site home (--home, BONFYRE_SITE_HOME, else ~/.bonfyre/sites) holds the
 * stores.  --cms-db and --matrix-db point the read verbs at another estate.
 */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "site_product.h"
#include "site_presentation.h"
#include "bf_commitment.h"

void bf_sha256_hex(const unsigned char *data, size_t len, char hex[65]);

typedef struct {
    char home[PATH_MAX], cms_db[PATH_MAX], matrix_db[PATH_MAX], auth_db[PATH_MAX], projection_db[PATH_MAX];
    char schemas[PATH_MAX], assets[PATH_MAX], cms_bin[PATH_MAX], self[PATH_MAX], meter_db[PATH_MAX], meter_bin[PATH_MAX];
    int port, json;
} Site;

static const char *arg_value(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static int arg_flag(int argc, char **argv, const char *name) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], name)) return 1;
    return 0;
}

/* The first argument after the verb that is not an option or an option's value. */
static const char *arg_positional(int argc, char **argv) {
    for (int i = 3; i < argc; i++) {
        if (!strncmp(argv[i], "--", 2)) { if (strcmp(argv[i], "--json") && strcmp(argv[i], "--no-open") && strcmp(argv[i], "--write") && strcmp(argv[i], "--bookable") && strcmp(argv[i], "--apply") &&
                                           strcmp(argv[i], "--remove") && strcmp(argv[i], "--replace") && strcmp(argv[i], "--list") && strcmp(argv[i], "--stop")) i++; continue; }
        return argv[i];
    }
    return NULL;
}

static int mkdirs(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++)
        if (*p == '/') { *p = '\0'; if (mkdir(tmp, 0755) && errno != EEXIST) return 1; *p = '/'; }
    return mkdir(tmp, 0755) && errno != EEXIST;
}

static int is_dir(const char *path) { struct stat st; return !stat(path, &st) && S_ISDIR(st.st_mode); }
static int is_file(const char *path) { struct stat st; return !stat(path, &st) && S_ISREG(st.st_mode); }

/* Runs argv without a shell.  stdout is captured when out is given. */
static int run(char *const argv[], char *out, size_t cap, char *const envp_extra[]) {
    int pipefd[2] = {-1, -1};
    pid_t pid;
    int status = 0;
    if (out && pipe(pipefd)) return -1;
    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        int devnull = open("/dev/null", O_RDWR);
        if (out) { dup2(pipefd[1], 1); close(pipefd[0]); close(pipefd[1]); }
        else if (devnull >= 0) dup2(devnull, 1);
        if (devnull >= 0) dup2(devnull, 2);
        for (int i = 0; envp_extra && envp_extra[i]; i++) putenv(envp_extra[i]);
        execvp(argv[0], argv);
        _exit(127);
    }
    if (out) {
        size_t used = 0;
        ssize_t n;
        close(pipefd[1]);
        while ((n = read(pipefd[0], out + used, cap - used - 1)) > 0) used += (size_t)n;
        out[used] = '\0';
        /* Drain what did not fit so the child is never blocked. */
        { char sink[4096]; while (read(pipefd[0], sink, sizeof(sink)) > 0) {} }
        close(pipefd[0]);
    }
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static void json_text(FILE *out, const char *text) {
    fputc('"', out);
    for (; text && *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') { fputc('\\', out); fputc(c, out); }
        else if (c == '\n') fputs("\\n", out);
        else if (c == '\r') fputs("\\r", out);
        else if (c == '\t') fputs("\\t", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

/* Builds a JSON object from name/value pairs.  A value beginning with '[',
 * '{' or a digit that the caller marked raw (name prefixed '=') is written as
 * given; every other value is written as a string. */
static char *json_object(const char *first_name, ...) {
    char *buffer = NULL;
    size_t size = 0;
    FILE *out = open_memstream(&buffer, &size);
    va_list ap;
    const char *name = first_name;
    int count = 0;
    if (!out) return NULL;
    va_start(ap, first_name);
    fputc('{', out);
    while (name) {
        const char *value = va_arg(ap, const char *);
        int raw = name[0] == '=';
        if (count++) fputc(',', out);
        json_text(out, raw ? name + 1 : name);
        fputc(':', out);
        if (raw) fputs(value, out); else json_text(out, value);
        name = va_arg(ap, const char *);
    }
    fputc('}', out);
    va_end(ap);
    fclose(out);
    return buffer;
}

static int slug_ok(const char *name) {
    size_t n = name ? strlen(name) : 0;
    if (n < 1 || n > 48 || !islower((unsigned char)name[0])) return 0;
    for (size_t i = 0; i < n; i++)
        if (!islower((unsigned char)name[i]) && !isdigit((unsigned char)name[i]) && name[i] != '-') return 0;
    return name[n - 1] != '-';
}

static int host_ok(const char *host) {
    size_t n = host ? strlen(host) : 0;
    if (n < 3 || n > 200) return 0;
    for (size_t i = 0; i < n; i++)
        if (!islower((unsigned char)host[i]) && !isdigit((unsigned char)host[i]) && host[i] != '-' && host[i] != '.') return 0;
    return 1;
}

static int color_ok(const char *c) {
    if (!c || c[0] != '#' || (strlen(c) != 7 && strlen(c) != 4)) return 0;
    for (const char *p = c + 1; *p; p++) if (!isxdigit((unsigned char)*p)) return 0;
    return 1;
}

/* Where the product's content types and page assets live: named in the
 * environment, installed under ~/.bonfyre/share/site, or beside the source
 * tree this binary was built in. */
static int find_share(Site *s) {
    const char *schemas = getenv("BONFYRE_CMS_SCHEMAS"), *assets = getenv("BONFYRE_SITE_ROOT"), *home = getenv("HOME");
    char candidate[PATH_MAX], *slash;
    if (schemas && is_dir(schemas)) snprintf(s->schemas, sizeof(s->schemas), "%s", schemas);
    if (assets && is_dir(assets)) snprintf(s->assets, sizeof(s->assets), "%s", assets);
    if (!s->schemas[0]) { snprintf(candidate, sizeof(candidate), "%s/.bonfyre/share/site/content-types", home ? home : ""); if (is_dir(candidate)) snprintf(s->schemas, sizeof(s->schemas), "%s", candidate); }
    if (!s->assets[0]) { snprintf(candidate, sizeof(candidate), "%s/.bonfyre/share/site/root", home ? home : ""); if (is_dir(candidate)) snprintf(s->assets, sizeof(s->assets), "%s", candidate); }
    snprintf(candidate, sizeof(candidate), "%s", s->self);
    slash = strrchr(candidate, '/');
    if (slash) {
        char probe[PATH_MAX];
        *slash = '\0';
        if (!s->schemas[0]) { snprintf(probe, sizeof(probe), "%s/../BonfyreCMS/content-types", candidate); if (is_dir(probe)) snprintf(s->schemas, sizeof(s->schemas), "%s", probe); }
        if (!s->assets[0]) { snprintf(probe, sizeof(probe), "%s/../../site/matrix", candidate); if (is_dir(probe)) snprintf(s->assets, sizeof(s->assets), "%s", probe); }
    }
    return s->schemas[0] && s->assets[0] ? 0 : 1;
}

static void find_binary(char *out, size_t cap, const char *env, const char *name, const char *self) {
    const char *value = getenv(env), *home = getenv("HOME");
    char probe[PATH_MAX], dir[PATH_MAX], *slash;
    if (value && *value) { snprintf(out, cap, "%s", value); return; }
    snprintf(dir, sizeof(dir), "%s", self);
    slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; snprintf(probe, sizeof(probe), "%s/%s", dir, name); if (!access(probe, X_OK)) { snprintf(out, cap, "%s", probe); return; } }
    snprintf(probe, sizeof(probe), "%s/.bonfyre/bin/%s", home ? home : "", name);
    if (!access(probe, X_OK)) { snprintf(out, cap, "%s", probe); return; }
    snprintf(out, cap, "%s", name);
}

static int site_init(Site *s, int argc, char **argv, const char *self) {
    const char *home = arg_value(argc, argv, "--home"), *user_home = getenv("HOME");
    const char *cms = arg_value(argc, argv, "--cms-db"), *matrix = arg_value(argc, argv, "--matrix-db");
    const char *port = arg_value(argc, argv, "--port");
    memset(s, 0, sizeof(*s));
    if (!realpath(self, s->self)) snprintf(s->self, sizeof(s->self), "%s", self);
    if (!home || !*home) home = getenv("BONFYRE_SITE_HOME");
    if (home && *home) snprintf(s->home, sizeof(s->home), "%s", home);
    else snprintf(s->home, sizeof(s->home), "%s/.bonfyre/sites", user_home ? user_home : ".");
    snprintf(s->cms_db, sizeof(s->cms_db), "%s", cms && *cms ? cms : "");
    if (!s->cms_db[0]) snprintf(s->cms_db, sizeof(s->cms_db), "%s/cms.db", s->home);
    snprintf(s->matrix_db, sizeof(s->matrix_db), "%s", matrix && *matrix ? matrix : "");
    if (!s->matrix_db[0]) snprintf(s->matrix_db, sizeof(s->matrix_db), "%s/matrix.db", s->home);
    snprintf(s->auth_db, sizeof(s->auth_db), "%s/auth.db", s->home);
    snprintf(s->projection_db, sizeof(s->projection_db), "%s/projections.db", s->home);
    snprintf(s->meter_db, sizeof(s->meter_db), "%s/meter.db", s->home);
    s->port = port ? atoi(port) : 8787;
    if (s->port < 1024 || s->port > 65535) { fputs("site: --port must be 1024..65535\n", stderr); return 2; }
    s->json = arg_flag(argc, argv, "--json");
    find_binary(s->cms_bin, sizeof(s->cms_bin), "BONFYRE_CMS_BINARY", "bonfyre-cms", s->self);
    find_binary(s->meter_bin, sizeof(s->meter_bin), "BONFYRE_METER_BINARY", "bonfyre-meter", s->self);
    /* The server is the resolver; when this runs under its product name the
     * resolver is the binary beside it. */
    {
        char *base = strrchr(s->self, '/');
        if (base && !strcmp(base + 1, "bonfyre-site")) {
            char resolver[PATH_MAX];
            snprintf(resolver, sizeof(resolver), "%.*s/bonfyre-web-address", (int)(base - s->self), s->self);
            if (!access(resolver, X_OK)) snprintf(s->self, sizeof(s->self), "%s", resolver);
        }
    }
    return 0;
}

static sqlite3 *open_cms(const Site *s) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(s->cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return NULL; }
    sqlite3_busy_timeout(db, 3000);
    return db;
}

static long count_where(sqlite3 *db, const char *table, const char *column, const char *value) {
    char sql[256];
    sqlite3_stmt *st = NULL;
    long n = -1;
    snprintf(sql, sizeof(sql), "SELECT count(*) FROM %s WHERE %s=?1", table, column);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, value, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return n;
}

/* The content store with the product's types in it. */
static int ensure_store(Site *s) {
    sqlite3 *db;
    long known = -1;
    char out[4096];
    if (find_share(s)) {
        fputs("site: the site content types and page assets were not found; set BONFYRE_CMS_SCHEMAS and BONFYRE_SITE_ROOT, "
              "or install them under ~/.bonfyre/share/site\n", stderr);
        return 3;
    }
    if (mkdirs(s->home)) { fprintf(stderr, "site: cannot create %s\n", s->home); return 3; }
    db = open_cms(s);
    if (db) { known = count_where(db, "sqlite_master", "name", "site_program"); sqlite3_close(db); }
    if (known > 0) return 0;
    {
        char *const av[] = {s->cms_bin, "schema", "migrate", "--db", s->cms_db, "--schemas", s->schemas, NULL};
        if (run(av, out, sizeof(out), NULL) || !strstr(out, "migrated")) {
            fprintf(stderr, "site: the content store could not load its types (%s)\n", s->cms_bin);
            return 4;
        }
    }
    return 0;
}

static int cms_create(const Site *s, const char *type, const char *json) {
    char out[2048];
    char *const av[] = {(char *)s->cms_bin, "entry", "create", (char *)type, (char *)json, "--ns", "root",
                        "--db", (char *)s->cms_db, "--schemas", (char *)s->schemas, NULL};
    if (run(av, out, sizeof(out), NULL) || !strstr(out, "\"created\":true")) {
        fprintf(stderr, "site: the content store refused a %s record: %.300s\n", type, out);
        return 5;
    }
    return 0;
}

static int resolver(const Site *s, char *out, size_t cap, const char *verb, ...) {
    char *av[32];
    int n = 0;
    va_list ap;
    const char *a;
    av[n++] = (char *)s->self; av[n++] = (char *)verb; av[n++] = "--db"; av[n++] = (char *)s->matrix_db;
    va_start(ap, verb);
    while ((a = va_arg(ap, const char *)) && n < 30) av[n++] = (char *)a;
    va_end(ap);
    av[n] = NULL;
    return run(av, out, cap, NULL);
}

static int http_get(const Site *s, const char *host, const char *path, const char *out_file, char *code, size_t code_cap) {
    char url[512], resolve[300];
    snprintf(url, sizeof(url), "http://%s:%d%s", host, s->port, path);
    /* The request goes to this machine whatever the host's DNS says. */
    snprintf(resolve, sizeof(resolve), "%s:%d:127.0.0.1", host, s->port);
    char *const av[] = {"curl", "-s", "-m", "20", "--resolve", resolve, "-H", "Accept: text/html", "-o", (char *)out_file,
                        "-w", "%{http_code}", url, NULL};
    return run(av, code, code_cap, NULL);
}

static int server_up(const Site *s) {
    char code[64] = "", url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d/healthz", s->port);
    char *const av[] = {"curl", "-s", "-m", "2", "-o", "/dev/null", "-w", "%{http_code}", url, NULL};
    return !run(av, code, sizeof(code), NULL) && !strncmp(code, "200", 3);
}

/* Starts the resolver's own server over this home, detached, unless one is
 * already answering on the port. */
static int ensure_server(Site *s) {
    char log_path[PATH_MAX], pid_path[PATH_MAX], port[16];
    pid_t pid;
    if (server_up(s)) return 0;
    if (find_share(s)) return 3;
    /* What assistants ask is written to the meter.  Reading and leaving a
     * message are both free; the rate says so on the record. */
    if (!access(s->meter_bin, X_OK)) {
        sqlite3 *meter = NULL;
        long known = 0;
        if (sqlite3_open_v2(s->meter_db, &meter, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) known = count_where(meter, "rate", "provider", "site");
        if (meter) sqlite3_close(meter);
        if (known < 2) {
            static const char *ops[] = {"site.read", "site.write"};
            for (int i = 0; i < 2; i++) {
                char *const av[] = {s->meter_bin, "admit-rate", "--db", s->meter_db, "--provider", "site", "--op", (char *)ops[i], "--unit", "call",
                                    "--amount", "0", "--evidence", "policy:site-assistant-calls-are-free", "--effective-at", "2026-01-01T00:00:00Z", NULL};
                run(av, NULL, 0, NULL);
            }
        }
    }
    snprintf(log_path, sizeof(log_path), "%s/serve.log", s->home);
    snprintf(pid_path, sizeof(pid_path), "%s/serve.pid", s->home);
    snprintf(port, sizeof(port), "%d", s->port);
    pid = fork();
    if (pid < 0) return 6;
    if (pid == 0) {
        int log_fd;
        setsid();
        log_fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (log_fd >= 0) { dup2(log_fd, 1); dup2(log_fd, 2); }
        { int devnull = open("/dev/null", O_RDONLY); if (devnull >= 0) dup2(devnull, 0); }
        setenv("BONFYRE_CMS_BINARY", s->cms_bin, 1);
        setenv("BONFYRE_CMS_DB", s->cms_db, 1);
        setenv("BONFYRE_CMS_SCHEMAS", s->schemas, 1);
        setenv("BONFYRE_PUBLIC_PROJECTION_DB", s->projection_db, 1);
        if (!access(s->meter_bin, X_OK)) { setenv("BONFYRE_METER_DB", s->meter_db, 1); setenv("BONFYRE_METER_BINARY", s->meter_bin, 1); }
        execl(s->self, s->self, "serve", "--db", s->matrix_db, "--auth-db", s->auth_db, "--port", port,
              "--site-root", s->assets, (char *)NULL);
        _exit(127);
    }
    { FILE *f = fopen(pid_path, "w"); if (f) { fprintf(f, "%d\n", (int)pid); fclose(f); } }
    for (int i = 0; i < 60; i++) {
        struct timespec nap = {0, 100000000L};
        if (server_up(s)) return 0;
        nanosleep(&nap, NULL);
    }
    fprintf(stderr, "site: the server did not answer on port %d; see %s\n", s->port, log_path);
    return 6;
}

static int file_contains(const char *path, const char *needle) {
    FILE *f = fopen(path, "rb");
    char *data;
    long size;
    int found;
    if (!f) return 0;
    fseek(f, 0, SEEK_END); size = ftell(f); rewind(f);
    data = size > 0 ? malloc((size_t)size + 1) : NULL;
    if (!data || fread(data, 1, (size_t)size, f) != (size_t)size) { fclose(f); free(data); return 0; }
    fclose(f);
    data[size] = '\0';
    found = strstr(data, needle) != NULL;
    free(data);
    return found;
}

/* ---------- new ---------- */

typedef struct { const char *name, *title, *tagline, *host, *accent, *first_title, *first_body; } NewSite;

/* Writes a site's records through their owners.  first_title may be NULL when
 * the caller will add the material itself. */
static int site_create(Site *s, const NewSite *n) {
    char profile[96], subject[96], slot[96], binding[96], program[96], post[96], url[300], presence[128], digest[65], tokens[512];
    char out[2048];
    char *json;
    sqlite3 *db;
    int rc;
    if ((rc = ensure_store(s))) return rc;
    db = open_cms(s);
    if (!db) return 4;
    if (count_where(db, "site_presentation", "site_id", n->name) > 0 || count_where(db, "site_presentation", "host", n->host) > 0) {
        sqlite3_close(db);
        fprintf(stderr, "site: a site named %s or answering at %s already exists here\n", n->name, n->host);
        return 7;
    }
    sqlite3_close(db);
    snprintf(profile, sizeof(profile), "ghost:%s", n->name);
    snprintf(subject, sizeof(subject), "institution:%s", n->name);
    snprintf(slot, sizeof(slot), "%s:body", n->name);
    snprintf(binding, sizeof(binding), "%s:community", n->name);
    snprintf(program, sizeof(program), "%s:v1", n->name);
    snprintf(post, sizeof(post), "%s:welcome", n->name);
    snprintf(url, sizeof(url), "https://%s/", n->host);
    snprintf(presence, sizeof(presence), "presence:%s:root", n->name);
    snprintf(tokens, sizeof(tokens),
             "{\"ground\":\"#0d130f\",\"ink\":\"#dde6da\",\"display\":\"#f5f7ee\",\"accent\":\"%s\",\"muted\":\"#8c9a8d\","
             "\"rule\":\"rgba(221,230,218,.17)\",\"panel\":\"#17211a\",\"measure\":\"44rem\",\"gap\":\"3rem\",\"radius\":\"14px\","
             "\"section_space\":\"3rem\"}", n->accent);

    json = json_object("template", n->name, "reference_source", "measured-local", "reference_name", n->title,
        "reference_url", url, "reference_categories", "site", "family", "editorial", "renderer_mix", "vertical thread",
        "density", "standard", "hero_mode", "lead", "navigation_mode", "header", "media_mode", "editorial",
        "typography_mode", "mixed", "card_mode", "borderless", "chrome_mode", "quiet",
        "=semantic_roles", "{\"accepts\":[\"title\"]}", "=layout_tokens", tokens,
        "acceptance_contract", "site new default", "cannot_infer", "profile is presentation only", "status", "published", NULL);
    rc = json ? cms_create(s, "presentation_template", json) : 5; free(json); if (rc) return rc;
    json = json_object("site_id", n->name, "host", n->host, "subject_address", subject, "profile", profile,
        "territory", "independent", "=navigation", "[]", "status", "published", NULL);
    rc = json ? cms_create(s, "site_presentation", json) : 5; free(json); if (rc) return rc;
    json = json_object("community", n->name, "subject_address", subject, "title", n->title, "description", n->tagline,
        "visibility", "public", "governance_scope", "public", "moderation_policy", "accepted posts only",
        "cannot_infer", "federation is not a human post", "status", "published", NULL);
    rc = json ? cms_create(s, "community_body", json) : 5; free(json); if (rc) return rc;
    if (n->first_title) {
        char when[32];
        time_t now = time(NULL);
        strftime(when, sizeof(when), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
        json = json_object("post", post, "community", n->name, "author_subject", subject, "title", n->first_title,
            "body", n->first_body, "published_at", when, "visibility", "public", "moderation_state", "accepted",
            "cannot_infer", "a human post is not a foreign publication", "status", "published", NULL);
        rc = json ? cms_create(s, "community_post", json) : 5; free(json); if (rc) return rc;
    }
    json = json_object("slot_id", slot, "site_id", n->name, "slot_name", "body", "=ordinal", "0", "extent", "page",
        "=accepts", "[\"Community\"]", "min_material_quality", "standard", "=max_items", "50", "status", "published", NULL);
    rc = json ? cms_create(s, "semantic_slot", json) : 5; free(json); if (rc) return rc;
    {
        char query[160];
        snprintf(query, sizeof(query), "{\"community\":\"%s\"}", n->name);
        json = json_object("binding_id", binding, "site_id", n->name, "slot_id", slot, "application_kind", "Community",
            "=material_query", query, "continuity_key", binding, "status", "published", NULL);
    }
    rc = json ? cms_create(s, "application_binding", json) : 5; free(json); if (rc) return rc;
    {
        char bindings[160], prefs[160];
        snprintf(bindings, sizeof(bindings), "[\"%s\"]", binding);
        snprintf(prefs, sizeof(prefs), "{\"profile\":\"%s\"}", profile);
        json = json_object("program_id", program, "site_id", n->name, "=experience_programs", "[\"Community\"]",
            "=application_bindings", bindings, "=renderer_preferences", prefs, "=source_collections", "[]",
            "=capability_programs", "[]", "=actor_presentations", "{}", "status", "published", NULL);
    }
    rc = json ? cms_create(s, "site_program", json) : 5; free(json); if (rc) return rc;

    /* The address facts, each with a receipt naming this act. */
    {
        char evidence[128];
        unsigned long long h = 1469598103934665603ULL;
        for (const char *p = n->name; *p; p++) { h ^= (unsigned char)*p; h *= 1099511628211ULL; }
        snprintf(digest, sizeof(digest), "%016llx%016llx%016llx%016llx", h, h * 31, h * 131, h * 1031);
        snprintf(evidence, sizeof(evidence), "receipt:site-new:%s", n->name);
        resolver(s, out, sizeof(out), "bootstrap-owned", NULL);
        if (resolver(s, out, sizeof(out), "subject", "--id", subject, "--kind", "Institution", "--digest", digest, "--evidence", evidence, NULL) ||
            resolver(s, out, sizeof(out), "domain", "--host", n->host, "--territory", "independent", "--scope", "public",
                     "--world", "live", "--repr", "HTML", "--evidence", evidence, NULL) ||
            resolver(s, out, sizeof(out), "address", "--subject", subject, "--kind", "web_url", "--address", url, "--evidence", evidence, NULL) ||
            resolver(s, out, sizeof(out), "presence", "--id", presence, "--subject", subject, "--territory", "independent",
                     "--scope", "public", "--world", "live", "--repr", "HTML", "--evidence", evidence, NULL)) {
            fprintf(stderr, "site: the resolver refused an address fact for %s\n", n->host);
            return 8;
        }
    }
    return 0;
}

/* Reads the site back the way a visitor would and says what was seen. */
static int site_readback(Site *s, const char *name, const char *host, const char *expect, int open_browser) {
    char page[PATH_MAX], code[64] = "", url[300], out[2048];
    int rc, resolved, shown;
    if ((rc = ensure_server(s))) return rc;
    snprintf(page, sizeof(page), "%s/.readback-%s.html", s->home, name);
    http_get(s, host, "/", page, code, sizeof(code));
    shown = !strncmp(code, "200", 3) && file_contains(page, "site-program-masthead") && (!expect || file_contains(page, expect));
    unlink(page);
    resolved = !resolver(s, out, sizeof(out), "resolve", "--host", host, "--path", "/", NULL) && strstr(out, "\"verdict\":\"resolved\"");
    snprintf(url, sizeof(url), "http://%s:%d/", host, s->port);
    printf("{\"schema\":\"bonfyre.site.v1\",\"site\":\"%s\",\"host\":\"%s\",\"url\":\"%s\",\"resolved\":%s,"
           "\"page_served\":%s,\"http_status\":\"%.3s\",\"home\":", name, host, url, resolved ? "true" : "false",
           shown ? "true" : "false", code);
    json_text(stdout, s->home);
    fputs(",\"cannot_infer\":\"a page served on this machine is not a site published to the internet\"}\n", stdout);
    if (!shown || !resolved) return 9;
    if (open_browser) {
#ifdef __APPLE__
        char *const av[] = {"open", url, NULL};
#else
        char *const av[] = {"xdg-open", url, NULL};
#endif
        run(av, NULL, 0, NULL);
    }
    return 0;
}

static int site_new(Site *s, int argc, char **argv) {
    NewSite n = {0};
    char host[256], title[200];
    int rc;
    n.name = arg_positional(argc, argv);
    if (!slug_ok(n.name)) { fputs("site new NAME: a name is lowercase letters, digits and dashes, starting with a letter\n", stderr); return 2; }
    n.title = arg_value(argc, argv, "--title");
    if (!n.title || !*n.title) { snprintf(title, sizeof(title), "%s", n.name); title[0] = (char)toupper((unsigned char)title[0]); n.title = title; }
    n.tagline = arg_value(argc, argv, "--tagline");
    if (!n.tagline) n.tagline = "";
    n.host = arg_value(argc, argv, "--host");
    if (!n.host || !*n.host) { snprintf(host, sizeof(host), "%s.localhost", n.name); n.host = host; }
    if (!host_ok(n.host)) { fputs("site new: --host is a lowercase host name\n", stderr); return 2; }
    n.accent = arg_value(argc, argv, "--accent");
    if (!n.accent) n.accent = "#b5df32";
    if (!color_ok(n.accent)) { fputs("site new: --accent is a colour such as #b5df32\n", stderr); return 2; }
    n.first_title = "This site is live";
    n.first_body = "It was made with one command. Replace this with your own words: every post, page and colour here is a record you own.";
    if ((rc = site_create(s, &n))) return rc;
    return site_readback(s, n.name, n.host, n.title, !arg_flag(argc, argv, "--no-open"));
}

/* ---------- list / show ---------- */

static int site_list(Site *s) {
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    long total = 0;
    int first = 1;
    if (!db || sqlite3_prepare_v2(db,
            "SELECT p.site_id, p.host, p.profile, p.territory, p.status, "
            "(SELECT count(*) FROM site_program g WHERE g.site_id=p.site_id AND g.status='published') "
            "FROM site_presentation p ORDER BY p.site_id", -1, &st, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        if (s->json) puts("{\"schema\":\"bonfyre.site.list.v1\",\"count\":0,\"sites\":[]}");
        else puts("No sites here yet. Make one: bonfyre site new NAME");
        return 0;
    }
    if (s->json) fputs("{\"schema\":\"bonfyre.site.list.v1\",\"sites\":[", stdout);
    else printf("%-32s %-44s %-22s %-12s %s\n", "SITE", "HOST", "PROFILE", "TERRITORY", "PAGE");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0), *host = (const char *)sqlite3_column_text(st, 1);
        const char *profile = (const char *)sqlite3_column_text(st, 2), *territory = (const char *)sqlite3_column_text(st, 3);
        int programs = sqlite3_column_int(st, 5);
        total++;
        if (s->json) {
            if (!first) fputc(',', stdout);
            first = 0;
            fputs("{\"site\":", stdout); json_text(stdout, id);
            fputs(",\"host\":", stdout); json_text(stdout, host);
            fputs(",\"profile\":", stdout); json_text(stdout, profile);
            fputs(",\"territory\":", stdout); json_text(stdout, territory);
            printf(",\"has_program\":%s}", programs ? "true" : "false");
        } else {
            printf("%-32.32s %-44.44s %-22.22s %-12.12s %s\n", id ? id : "", host ? host : "", profile ? profile : "",
                   territory ? territory : "", programs ? "composed" : "profile only");
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (s->json) printf("],\"count\":%ld}\n", total);
    else printf("\n%ld site%s\n", total, total == 1 ? "" : "s");
    return 0;
}

static int site_lookup(sqlite3 *db, const char *key, char *name, size_t name_cap, char *host, size_t host_cap,
                       char *profile, size_t profile_cap, char *subject, size_t subject_cap) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT site_id, host, profile, subject_address FROM site_presentation "
                               "WHERE site_id=?1 OR host=?1 ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, key, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(name, name_cap, "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(host, host_cap, "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(profile, profile_cap, "%s", sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "");
        snprintf(subject, subject_cap, "%s", sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
        found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

static int site_show(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    char name[128], host[256], profile[128], subject[160], out[4096];
    sqlite3 *db = open_cms(s);
    long slots, bindings, programs, posts;
    int resolved;
    if (!key) { fputs("site show NAME|HOST\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    slots = count_where(db, "semantic_slot", "site_id", name);
    bindings = count_where(db, "application_binding", "site_id", name);
    programs = count_where(db, "site_program", "site_id", name);
    posts = count_where(db, "community_post", "community", name);
    sqlite3_close(db);
    resolved = !resolver(s, out, sizeof(out), "resolve", "--host", host, "--path", "/", NULL) && strstr(out, "\"verdict\":\"resolved\"");
    if (s->json) {
        fputs("{\"schema\":\"bonfyre.site.show.v1\",\"site\":", stdout); json_text(stdout, name);
        fputs(",\"host\":", stdout); json_text(stdout, host);
        fputs(",\"profile\":", stdout); json_text(stdout, profile);
        fputs(",\"subject\":", stdout); json_text(stdout, subject);
        printf(",\"programs\":%ld,\"slots\":%ld,\"bindings\":%ld,\"posts\":%ld,\"resolved\":%s,\"local_url\":\"http://%s:%d/\"}\n",
               programs < 0 ? 0 : programs, slots < 0 ? 0 : slots, bindings < 0 ? 0 : bindings, posts < 0 ? 0 : posts,
               resolved ? "true" : "false", host, s->port);
    } else {
        printf("site       %s\nhost       %s\nprofile    %s\nsubject    %s\n", name, host, profile, subject);
        printf("composed   %ld program, %ld slot%s, %ld binding%s\n", programs < 0 ? 0 : programs, slots < 0 ? 0 : slots,
               slots == 1 ? "" : "s", bindings < 0 ? 0 : bindings, bindings == 1 ? "" : "s");
        printf("material   %ld post%s\n", posts < 0 ? 0 : posts, posts == 1 ? "" : "s");
        printf("address    %s\n", resolved ? "resolves" : "does not resolve in this home's resolver store");
        printf("local      http://%s:%d/   (bonfyre site serve)\n", host, s->port);
    }
    return 0;
}

/* ---------- check ---------- */

static char *slurp(const char *path, long *size);
static int rpc_call(const Site *s, const char *endpoint, const char *request, char *out, size_t cap);
static int styled(void);

static double channel(int v) { double c = v / 255.0; return c <= 0.03928 ? c / 12.92 : pow((c + 0.055) / 1.055, 2.4); }

/* Relative luminance of #rgb or #rrggbb; -1 when the colour is not one of those. */
static double luminance(const char *hex) {
    unsigned r, g, b;
    if (!hex || hex[0] != '#') return -1;
    if (strlen(hex) == 7 && sscanf(hex + 1, "%2x%2x%2x", &r, &g, &b) == 3) {}
    else if (strlen(hex) == 4 && sscanf(hex + 1, "%1x%1x%1x", &r, &g, &b) == 3) { r *= 17; g *= 17; b *= 17; }
    else return -1;
    return 0.2126 * channel((int)r) + 0.7152 * channel((int)g) + 0.0722 * channel((int)b);
}

static double contrast(const char *a, const char *b) {
    double la = luminance(a), lb = luminance(b);
    if (la < 0 || lb < 0) return -1;
    return la > lb ? (la + 0.05) / (lb + 0.05) : (lb + 0.05) / (la + 0.05);
}

/* The value of a CSS custom property written into the page. */
static int page_token(const char *html, const char *name, char *out, size_t cap) {
    char key[64];
    const char *p, *end;
    snprintf(key, sizeof(key), "%s:", name);
    p = strstr(html, key);
    if (!p) return 0;
    p += strlen(key);
    end = strchr(p, ';');
    if (!end || (size_t)(end - p) >= cap) return 0;
    snprintf(out, cap, "%.*s", (int)(end - p), p);
    return 1;
}

static void check_line(int json, int *first, int ok, const char *name, const char *detail, int *failed) {
    if (!ok) (*failed)++;
    if (json) {
        printf("%s{\"check\":\"%s\",\"ok\":%s,\"detail\":", *first ? "" : ",", name, ok ? "true" : "false");
        json_text(stdout, detail);
        fputc('}', stdout);
    } else printf("  %s  %-22s %s\n", ok ? "ok  " : "FAIL", name, detail);
    *first = 0;
}

/* What a visitor and a reviewer would find wrong with a site, found by
 * reading the page it serves: links that lead nowhere, stylesheets that do
 * not load, a missing name, language or viewport, text that cannot be read
 * against its ground, and a site with nothing on it.  It reads; it changes
 * nothing.  Passing is not the page being good, only free of these faults. */
static int site_check(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    char name[128], host[256], profile[128], subject[160], page[PATH_MAX], code[64] = "", detail[400];
    char ground[32] = "", ink[32] = "", accent[32] = "";
    sqlite3 *db = open_cms(s);
    char *html;
    long size = 0, posts, links = 0, dead = 0, sheets = 0, sheets_dead = 0, scripts = 0, images = 0, images_bare = 0;
    char dead_list[240] = "";
    int rc, failed = 0, first = 1;
    double ratio;
    if (!key) { fputs("site check NAME|HOST [--json]\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    posts = count_where(db, "community_post", "community", name);
    sqlite3_close(db);
    if ((rc = ensure_server(s))) return rc;
    snprintf(page, sizeof(page), "%s/.check-%s.html", s->home, name);
    http_get(s, host, "/", page, code, sizeof(code));
    html = slurp(page, &size);
    if (!html) { fprintf(stderr, "site: %s served nothing\n", host); return 9; }
    if (s->json) { fputs("{\"schema\":\"bonfyre.site.check.v1\",\"site\":", stdout); json_text(stdout, name); fputs(",\"checks\":[", stdout); }
    else printf("site check  %s  (http://%s:%d/)\n", name, host, s->port);

    snprintf(detail, sizeof(detail), "HTTP %.3s, %ld bytes", code, size);
    check_line(s->json, &first, !strncmp(code, "200", 3) && strstr(html, "site-program-masthead") != NULL, "page-served", detail, &failed);

    {
        const char *t = strstr(html, "<title>"), *e = t ? strstr(t, "</title>") : NULL;
        char title[200] = "";
        if (t && e) snprintf(title, sizeof(title), "%.*s", (int)(e - t - 7), t + 7);
        snprintf(detail, sizeof(detail), "%s", title[0] ? title : "no title");
        check_line(s->json, &first, title[0] && strcmp(title, host) && !strstr(title, "Aurekai"), "own-title", detail, &failed);
    }
    check_line(s->json, &first, strstr(html, "<html lang=") != NULL || strstr(html, " lang=\"") != NULL, "language-declared",
               "the page says what language it is in, for readers and search", &failed);
    check_line(s->json, &first, strstr(html, "name=\"viewport\"") != NULL, "fits-a-phone", "viewport declared", &failed);

    /* Every address the page offers on its own host. */
    for (const char *p = html; (p = strstr(p, "href=\"/")); p += 6) {
        char path[256], out_file[PATH_MAX], link_code[16] = "";
        const char *end = strchr(p + 6, '"');
        int is_sheet = p >= html + 30 && !strncmp(p - 23, "<link rel=\"stylesheet\" ", 23);
        if (!end || (size_t)(end - p - 6) >= sizeof(path)) continue;
        snprintf(path, sizeof(path), "%.*s", (int)(end - p - 6), p + 6);
        snprintf(out_file, sizeof(out_file), "%s/.check-link", s->home);
        http_get(s, host, path, out_file, link_code, sizeof(link_code));
        unlink(out_file);
        if (is_sheet) sheets++; else links++;
        if (strncmp(link_code, "200", 3)) {
            if (is_sheet) sheets_dead++; else dead++;
            if (strlen(dead_list) + strlen(path) + 3 < sizeof(dead_list)) { if (dead_list[0]) strcat(dead_list, ", "); strcat(dead_list, path); }
        }
    }
    snprintf(detail, sizeof(detail), "%ld link%s, %ld lead nowhere%s%s", links, links == 1 ? "" : "s", dead, dead ? ": " : "", dead ? dead_list : "");
    check_line(s->json, &first, dead == 0, "links-lead-somewhere", detail, &failed);
    snprintf(detail, sizeof(detail), "%ld stylesheet%s, %ld did not load", sheets, sheets == 1 ? "" : "s", sheets_dead);
    check_line(s->json, &first, sheets > 0 && sheets_dead == 0, "styles-load", detail, &failed);

    page_token(html, "--bf-ground", ground, sizeof(ground));
    page_token(html, "--bf-ink", ink, sizeof(ink));
    page_token(html, "--bf-accent", accent, sizeof(accent));
    ratio = contrast(ink, ground);
    snprintf(detail, sizeof(detail), ratio < 0 ? "ink %s on ground %s could not be measured" : "ink %s on ground %s is %.1f to 1 (4.5 needed)", ink, ground, ratio);
    check_line(s->json, &first, ratio >= 4.5, "text-readable", detail, &failed);
    ratio = contrast(accent, ground);
    snprintf(detail, sizeof(detail), ratio < 0 ? "accent %s on ground %s could not be measured" : "accent %s on ground %s is %.1f to 1 (3 needed)", accent, ground, ratio);
    check_line(s->json, &first, ratio >= 3.0, "accent-visible", detail, &failed);

    {
        /* What an assistant is told about this site, and whether it is true. */
        char mcp_url[300], card_file[PATH_MAX], card_code[16] = "", *reply_text = malloc(1 << 16), *card;
        long card_size = 0;
        int answers = 0, own_card = 0;
        snprintf(mcp_url, sizeof(mcp_url), "http://127.0.0.1:%d/mcp/%s", s->port, name);
        if (reply_text && !rpc_call(s, mcp_url, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}", reply_text, 1 << 16))
            answers = strstr(reply_text, "\"list_posts\"") != NULL;
        free(reply_text);
        check_line(s->json, &first, answers, "assistants-can-read", answers ? "the site answers the Model Context Protocol at /mcp" :
                   "nothing answers at /mcp; an assistant cannot read this site", &failed);
        snprintf(card_file, sizeof(card_file), "%s/.check-card", s->home);
        http_get(s, host, "/.well-known/agent.json", card_file, card_code, sizeof(card_code));
        card = slurp(card_file, &card_size);
        unlink(card_file);
        own_card = card && !strstr(card, "parcel") && !strstr(card, "\"name\":\"Bonfyre\"");
        {
            /* The one tool that writes must refuse a caller with no key. */
            char *answer = malloc(1 << 14);
            int refused = 0;
            if (answer && !rpc_call(s, mcp_url, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"send_inquiry\","
                                                "\"arguments\":{\"name\":\"site check\",\"contact\":\"check@localhost\",\"message\":\"this must be refused\"}}}", answer, 1 << 14))
                refused = strstr(answer, "\"isError\":true") != NULL;
            free(answer);
            check_line(s->json, &first, refused, "writing-needs-a-key", refused ? "a caller with no key cannot leave a message" :
                       "a caller with no key could write to this site", &failed);
        }
        {
            /* Where agents look first: the server card at its well-known
             * path, and the page offering its tools to a browser's assistant. */
            char sc_file[PATH_MAX], sc_code[16] = "", *sc;
            long sc_size = 0;
            int has_card, has_page_tools;
            snprintf(sc_file, sizeof(sc_file), "%s/.check-server-card", s->home);
            http_get(s, host, "/.well-known/mcp/server-card.json", sc_file, sc_code, sizeof(sc_code));
            sc = slurp(sc_file, &sc_size);
            unlink(sc_file);
            has_card = sc && !strncmp(sc_code, "200", 3) && strstr(sc, "\"streamable-http\"") && strstr(sc, "\"tools\":[");
            free(sc);
            check_line(s->json, &first, has_card, "agents-can-discover-it", has_card ? "an MCP server card is at /.well-known/mcp/server-card.json" :
                       "no MCP server card at /.well-known/mcp/server-card.json; an agent has to be told the address", &failed);
            has_page_tools = strstr(html, "own-site-agent.js") != NULL;
            check_line(s->json, &first, has_page_tools, "browser-assistants", has_page_tools ? "the page offers its read tools to a browser's own assistant (WebMCP)" :
                       "the page offers nothing to an assistant running in the visitor's browser", &failed);
        }
        check_line(s->json, &first, own_card, "agent-card-is-its-own", own_card ? "the card agents read describes this site" :
                   "the card agents read describes someone else's skills", &failed);
        free(card);
    }
    for (const char *p = html; (p = strstr(p, "<img")); p += 4) {
        const char *end = strchr(p, '>');
        images++;
        if (end && !memmem(p, (size_t)(end - p), " alt=\"", 6)) images_bare++;
    }
    snprintf(detail, sizeof(detail), "%ld image%s, %ld with no description", images, images == 1 ? "" : "s", images_bare);
    check_line(s->json, &first, images_bare == 0, "images-described", detail, &failed);
    for (const char *p = html; (p = strstr(p, "<script")); p += 7) scripts++;
    snprintf(detail, sizeof(detail), "%ld post%s", posts < 0 ? 0 : posts, posts == 1 ? "" : "s");
    check_line(s->json, &first, posts > 0, "has-material", detail, &failed);
    snprintf(detail, sizeof(detail), "%ld bytes of page, %ld script%s", size, scripts, scripts == 1 ? "" : "s");
    check_line(s->json, &first, size < 500000, "page-weight", detail, &failed);

    free(html);
    unlink(page);
    if (s->json) printf("],\"failed\":%d,\"cannot_infer\":\"no fault found by these checks is not a page a person has judged good\"}\n", failed);
    else printf("\n%s\n", failed ? "Fix the lines marked FAIL." : "All checks passed.");
    return failed ? 12 : 0;
}

/* ---------- search ---------- */

/* Recall's index over this home's sites, built from their records. */
static int build_search_index(Site *s, char *recall, const char *store, const char *work, int have_community) {
    char r1[PATH_MAX + 64], r2[PATH_MAX + 64], r3[PATH_MAX + 64];
    char *out = malloc(1 << 16);
    int rc;
    if (!out) return 3;
    snprintf(r1, sizeof(r1), "%s:site_presentation", s->cms_db);
    snprintf(r2, sizeof(r2), "%s:community_body", s->cms_db);
    snprintf(r3, sizeof(r3), "%s:community_post", s->cms_db);
    fprintf(stderr, "site: indexing the sites in %s (once, until they change)\n", s->cms_db);
    if (have_community) {
        char *const av[] = {recall, "index-sources", "--store", (char *)store, "--work-dir", (char *)work, "--records", r1,
                            "--records", r2, "--records", r3, "--apply", NULL};
        rc = run(av, out, 1 << 16, NULL);
    } else {
        char *const av[] = {recall, "index-sources", "--store", (char *)store, "--work-dir", (char *)work, "--records", r1, "--apply", NULL};
        rc = run(av, out, 1 << 16, NULL);
    }
    free(out);
    if (rc) { fprintf(stderr, "site: Recall could not index these sites (%s)\n", recall); return 10; }
    return 0;
}

/* Where that index lives: beside the store it reads, named for it.  The
 * site's own assistant endpoint looks in the same place. */
static void search_index_paths(const Site *s, char *store, size_t store_cap, char *work, size_t work_cap) {
    char dir[PATH_MAX], *slash;
    unsigned long long h = 1469598103934665603ULL;
    for (const char *p = s->cms_db; *p; p++) { h ^= (unsigned char)*p; h *= 1099511628211ULL; }
    snprintf(dir, sizeof(dir), "%s", s->cms_db);
    slash = strrchr(dir, '/');
    if (slash) *slash = '\0'; else snprintf(dir, sizeof(dir), ".");
    /* Another estate's store is read, never written beside. */
    if (strncmp(s->cms_db, s->home, strlen(s->home))) snprintf(dir, sizeof(dir), "%s", s->home);
    snprintf(store, store_cap, "%s/search-%016llx.db", dir, h);
    snprintf(work, work_cap, "%s/search-%016llx.work", dir, h);
}

static int site_search(Site *s, int argc, char **argv) {
    const char *text = arg_positional(argc, argv), *top = arg_value(argc, argv, "--top");
    char recall[PATH_MAX], store[PATH_MAX], work[PATH_MAX];
    char *out = malloc(1 << 20);
    struct stat cms_st, store_st;
    sqlite3 *db = NULL, *mem = NULL;
    sqlite3_stmt *st = NULL;
    int rc = 0, have_community = 0;
    if (!text || !out) { fputs("site search TEXT [--top N]\n", stderr); free(out); return 2; }
    if (stat(s->cms_db, &cms_st)) { fprintf(stderr, "site: no content store at %s\n", s->cms_db); free(out); return 7; }
    find_binary(recall, sizeof(recall), "BONFYRE_RECALL_BINARY", "bonfyre-recall", s->self);
    search_index_paths(s, store, sizeof(store), work, sizeof(work));
    db = open_cms(s);
    if (db) { have_community = count_where(db, "sqlite_master", "name", "community_post") > 0 && count_where(db, "community_post", "status", "published") > 0; sqlite3_close(db); }
    if ((stat(store, &store_st) || store_st.st_mtime < cms_st.st_mtime) && (rc = build_search_index(s, recall, store, work, have_community))) { free(out); return rc; }
    {
        char *const av[] = {recall, "query", "--store", store, "--text", (char *)text, "--top", (char *)(top ? top : "8"), "--hybrid", NULL};
        if (run(av, out, 1 << 20, NULL)) { fputs("site: Recall could not answer\n", stderr); free(out); return 10; }
    }
    if (s->json) { fputs(out, stdout); free(out); return 0; }
    if (sqlite3_open(":memory:", &mem) == SQLITE_OK &&
        sqlite3_prepare_v2(mem, "SELECT json_extract(value,'$.id'), json_extract(value,'$.vector_rank'), "
                                "json_extract(value,'$.word_rank'), replace(substr(json_extract(value,'$.text'),1,150),char(10),' · ') "
                                "FROM json_each(?1,'$.fused.results')", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, out, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW)
            printf("%-28s meaning #%-3d words #%-3d %s\n", (const char *)sqlite3_column_text(st, 0), sqlite3_column_int(st, 1),
                   sqlite3_column_int(st, 2), (const char *)sqlite3_column_text(st, 3));
        puts("\nCandidates, by meaning and by words. A rank of 0 means that list did not hold it.");
    }
    if (st) sqlite3_finalize(st);
    if (mem) sqlite3_close(mem);
    free(out);
    return 0;
}

/* ---------- mcp ---------- */

static int rpc_call(const Site *s, const char *endpoint, const char *request, char *out, size_t cap) {
    char *const av[] = {"curl", "-s", "-m", "30", "-X", "POST", "-H", "content-type: application/json",
                        "-H", "accept: application/json, text/event-stream", "--data-binary", (char *)request, (char *)endpoint, NULL};
    (void)s;
    return run(av, out, cap, NULL);
}

static int styled(void) { return isatty(1) && !getenv("NO_COLOR"); }

/* A site's assistant endpoint, stood up and proved in one command: the server
 * is started, the site's search index is built, and the endpoint is asked the
 * way a client asks it (handshake, tool list, one call) before anything is
 * said about it.  Then the lines a person pastes to connect. */
static int site_mcp(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    char name[128], host[256], profile[128], subject[160], endpoint[400], public_endpoint[400];
    char recall[PATH_MAX], store[PATH_MAX], work[PATH_MAX];
    char *out = malloc(1 << 18);
    sqlite3 *db = open_cms(s), *mem = NULL;
    sqlite3_stmt *st = NULL;
    struct stat cms_st, store_st;
    char tools[512] = "", info[400] = "", method[64] = "words", version[32] = "";
    int rc, tool_count = 0, view_tools = 0, indexed = 0, have_posts = 0;
    const char *bold = styled() ? "\033[1m" : "", *dim = styled() ? "\033[2m" : "", *off = styled() ? "\033[0m" : "";
    if (!key || !out) { fputs("site mcp NAME|HOST [--json]\n", stderr); if (db) sqlite3_close(db); free(out); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        free(out);
        return 7;
    }
    have_posts = count_where(db, "community_post", "community", name) > 0;
    sqlite3_close(db);
    if ((rc = ensure_server(s))) { free(out); return rc; }

    /* Search by meaning needs Recall's index; without Recall the endpoint
     * still searches, by words, and says so. */
    find_binary(recall, sizeof(recall), "BONFYRE_RECALL_BINARY", "bonfyre-recall", s->self);
    search_index_paths(s, store, sizeof(store), work, sizeof(work));
    if (have_posts && !access(recall, X_OK) && !stat(s->cms_db, &cms_st)) {
        if (!stat(store, &store_st) && store_st.st_mtime >= cms_st.st_mtime) indexed = 1;
        else indexed = !build_search_index(s, recall, store, work, 1);
    }

    snprintf(endpoint, sizeof(endpoint), "http://127.0.0.1:%d/mcp/%s", s->port, name);
    snprintf(public_endpoint, sizeof(public_endpoint), "https://%s/mcp", host);
    if (sqlite3_open(":memory:", &mem) != SQLITE_OK) { free(out); return 4; }
    rc = 13;
    if (rpc_call(s, endpoint, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\","
                              "\"capabilities\":{},\"clientInfo\":{\"name\":\"bonfyre-site\",\"version\":\"1\"}}}", out, 1 << 18) ||
        sqlite3_prepare_v2(mem, "SELECT json_extract(?1,'$.result.protocolVersion')", -1, &st, NULL) != SQLITE_OK) goto failed;
    sqlite3_bind_text(st, 1, out, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW || !sqlite3_column_text(st, 0)) goto failed;
    snprintf(version, sizeof(version), "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); st = NULL;
    if (rpc_call(s, endpoint, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}", out, 1 << 18) ||
        sqlite3_prepare_v2(mem, "SELECT json_extract(value,'$.name'), json_extract(value,'$._meta.ui.resourceUri') IS NOT NULL "
                                "FROM json_each(?1,'$.result.tools')", -1, &st, NULL) != SQLITE_OK) goto failed;
    sqlite3_bind_text(st, 1, out, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        size_t used = strlen(tools);
        snprintf(tools + used, sizeof(tools) - used, "%s%s%s", tool_count ? ", " : "", (const char *)sqlite3_column_text(st, 0),
                 sqlite3_column_int(st, 1) ? " (view)" : "");
        view_tools += sqlite3_column_int(st, 1);
        tool_count++;
    }
    sqlite3_finalize(st); st = NULL;
    if (!tool_count) goto failed;
    if (rpc_call(s, endpoint, "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"site_info\",\"arguments\":{}}}", out, 1 << 18) ||
        sqlite3_prepare_v2(mem, "SELECT json_extract(?1,'$.result.structuredContent.name') || ' (' || json_extract(?1,'$.result.structuredContent.posts') || CASE json_extract(?1,'$.result.structuredContent.posts') WHEN 1 THEN ' post)' ELSE ' posts)' END",
                           -1, &st, NULL) != SQLITE_OK) goto failed;
    sqlite3_bind_text(st, 1, out, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW || !sqlite3_column_text(st, 0)) goto failed;
    snprintf(info, sizeof(info), "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); st = NULL;
    if (have_posts) {
        if (!rpc_call(s, endpoint, "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"search_posts\",\"arguments\":{\"query\":\"the site\"}}}", out, 1 << 18) &&
            sqlite3_prepare_v2(mem, "SELECT json_extract(?1,'$.result.structuredContent.method')", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, out, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(method, sizeof(method), "%s", (const char *)sqlite3_column_text(st, 0));
            sqlite3_finalize(st); st = NULL;
        }
    }
    rc = 0;
    if (s->json) {
        fputs("{\"schema\":\"bonfyre.site.mcp.v1\",\"site\":", stdout); json_text(stdout, name);
        fputs(",\"endpoint\":", stdout); json_text(stdout, endpoint);
        fputs(",\"public_endpoint\":", stdout); json_text(stdout, public_endpoint);
        printf(",\"protocol\":\"%s\",\"tools\":%d,\"view_tools\":%d,\"search\":\"%s\",\"index_built\":%s,\"answered\":", version, tool_count,
               view_tools, method, indexed ? "true" : "false");
        json_text(stdout, info);
        fputs(",\"cannot_infer\":\"an endpoint answering on this machine is not one an assistant on the internet can reach\"}\n", stdout);
    } else {
        size_t host_len = strlen(host);
        int local_name = host_len > 10 && !strcmp(host + host_len - 10, ".localhost");
        printf("\n%sAssistants can now read %s%s\n\n", bold, info, off);
        printf("  address    %s%s%s\n", bold, endpoint, off);
        if (local_name) printf("             %sthis machine only; a site made with --host its.real.name also answers at https://its.real.name/mcp%s\n", dim, off);
        else printf("             %s%s  once the site is on the internet%s\n", dim, public_endpoint, off);
        printf("  tools      %s\n", tools);
        printf("  search     by %s%s\n", method, indexed ? "" : "  (Recall's index is not built; words only)");
        printf("  checked    handshake (%s), %d tools listed, site_info answered\n\n", version, tool_count);        printf("  limits     60 calls a minute without a key, 600 with one; every call is recorded (bonfyre site usage %s)\n", name);
        printf("  writing    only send_inquiry, and only with a key issued for it (bonfyre site key %s --write)\n\n", name);
        printf("  %sClaude Code%s      claude mcp add --transport http %s %s\n", bold, off, name, endpoint);
        printf("  %sOther clients%s    {\"mcpServers\":{\"%s\":{\"type\":\"http\",\"url\":\"%s\"}}}\n", bold, off, name, endpoint);
        printf("  %sFor a person%s     open %s in a browser\n", bold, off, endpoint);
        printf("  %sPlain text%s       http://%s:%d/llms.txt  (the site's writing as markdown, in a browser)\n\n", bold, off, host, s->port);
        printf("\n");
    }
failed:
    if (rc) fprintf(stderr, "site: %s did not answer as an assistant endpoint should\n", endpoint);
    if (st) sqlite3_finalize(st);
    if (mem) sqlite3_close(mem);
    free(out);
    return rc;
}

/* ---------- page / key / usage / inbox ---------- */

static int cms_create_in(const Site *s, const char *ns, const char *type, const char *json) {
    char out[2048];
    char *const av[] = {(char *)s->cms_bin, "entry", "create", (char *)type, (char *)json, "--ns", (char *)ns,
                        "--db", (char *)s->cms_db, "--schemas", (char *)s->schemas, NULL};
    if (run(av, out, sizeof(out), NULL) || !strstr(out, "\"created\":true")) {
        fprintf(stderr, "site: the content store refused a %s record: %.300s\n", type, out);
        return 5;
    }
    return 0;
}

/* Submit a prepared aggregate once; the CMS owns its transaction. */
static int cms_page_write(Site *s, const char *ns, const char *request) {
    char path[PATH_MAX], input[PATH_MAX + 2], out[2048] = "";
    if (!request || strlen(request) > (1U << 20) || find_share(s)) return 5;
    snprintf(path, sizeof(path), "%s/.page-write-XXXXXX", s->home);
    int fd = mkstemp(path);
    if (fd < 0) return 5;
    FILE *f = fdopen(fd, "w");
    if (!f) { close(fd); unlink(path); return 5; }
    int failed = fputs(request, f) == EOF;
    if (fclose(f)) failed = 1;
    if (failed) { unlink(path); return 5; }
    snprintf(input, sizeof(input), "@%s", path);
    char *const av[] = {s->cms_bin, "entry", "page-write", input, "--ns", (char *)ns,
                       "--db", s->cms_db, "--schemas", s->schemas, NULL};
    int rc = run(av, out, sizeof(out), NULL);
    unlink(path);
    if (!strstr(out, "\"committed\":true")) {
        if (strstr(out, "\"committed\":false"))
            fprintf(stderr, "site: the content store refused the page; its previous content is intact: %.300s\n", out);
        else
            fprintf(stderr, "site: the content store did not confirm the page write; check the saved page: %.300s\n", out);
        return 5;
    }
    if (rc) {
        fprintf(stderr, "site: the page was saved, but publication notification failed: %.300s\n", out);
        return 5;
    }
    return 0;
}

/* Ask the page owner for the exact aggregate being replaced or removed. */
static int cms_page_version(Site *s,sqlite3 *db,const char *ns,const char *route,long long expected_id,char digest[65]) {
    char *out=malloc((1U<<20)+1024); sqlite3_stmt *st=NULL; int ok=0;
    if (!out || find_share(s)) { free(out); return 0; }
    char *const av[]={s->cms_bin,"entry","page-read",(char*)route,"--ns",(char*)ns,
        "--db",s->cms_db,"--schemas",s->schemas,NULL};
    if (run(av,out,(1U<<20)+1024,NULL) || sqlite3_prepare_v2(db,
        "SELECT json_extract(?1,'$.id'),json_extract(?1,'$.digest') WHERE json_extract(?1,'$.schema')='bonfyre.page-snapshot.v1'",
        -1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,out,-1,SQLITE_STATIC);
    if (sqlite3_step(st)==SQLITE_ROW && sqlite3_column_int64(st,0)==expected_id) {
        const char *hash=(const char*)sqlite3_column_text(st,1);
        if (hash && strlen(hash)==64 && strspn(hash,"0123456789abcdef")==64) { memcpy(digest,hash,65); ok=1; }
    }
 done:
    sqlite3_finalize(st); free(out); return ok;
}

/* A standing page: its record, its text as fragments, and the page read back
 * from the server before it is called published.  Blank lines in the text
 * separate sections; a section whose first line is short and ends without a
 * full stop is taken as that section's heading. */
static int site_page(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *title = arg_value(argc, argv, "--title"), *text = arg_value(argc, argv, "--text");
    const char *file = arg_value(argc, argv, "--file"), *route_arg = arg_value(argc, argv, "--route"), *summary = arg_value(argc, argv, "--summary");
    const char *group = arg_value(argc, argv, "--group");
    char name[128], host[256], profile[128], subject[160], route[128], page_file[PATH_MAX], code[64] = "", url[400];
    char *marked = NULL, *opening = NULL;
    sqlite3 *db = open_cms(s);
    char *body = NULL, *json, *request = NULL;
    size_t request_size = 0;
    FILE *prepared = NULL;
    long size = 0, position = 0;
    long long existing_page = 0;
    char expected_digest[65] = "";
    int rc;
    int removing = arg_flag(argc, argv, "--remove"), replacing = arg_flag(argc, argv, "--replace");
    if (removing && replacing) {
        if (db) sqlite3_close(db);
        fputs("site page: choose --remove or --replace, not both\n", stderr);
        return 2;
    }
    if (!key || (removing ? !route_arg : (!title || (!text && !file)))) {
        fputs("site page NAME --title T (--text \"...\" | --file F) [--summary S] [--group G] [--route /path] [--replace]\n"
              "site page NAME --route /path --remove\n", stderr);
        if (db) sqlite3_close(db);
        return 2;
    }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    if (route_arg) snprintf(route, sizeof(route), "%s%s", route_arg[0] == '/' ? "" : "/", route_arg);
    else {
        /* The route is the title, lower-cased, with what is not a letter or digit as a dash. */
        size_t n = 1;
        route[0] = '/';
        for (const char *p = title; *p && n + 2 < sizeof(route); p++) {
            if (isalnum((unsigned char)*p)) route[n++] = (char)tolower((unsigned char)*p);
            else if (n > 1 && route[n - 1] != '-') route[n++] = '-';
        }
        while (n > 1 && route[n - 1] == '-') n--;
        route[n] = '\0';
    }
    for (const char *p = route + 1; *p; p++)
        if (!islower((unsigned char)*p) && !isdigit((unsigned char)*p) && *p != '-' && *p != '/') { sqlite3_close(db); fputs("site page: a route is lowercase letters, digits and dashes\n", stderr); return 2; }
    if (strlen(route) < 2 || !strncmp(route, "/mcp", 4) || !strncmp(route, "/assets", 7) || !strncmp(route, "/a2a", 4) || !strcmp(route, "/community") ||
        !strcmp(route, "/contact")) {
        sqlite3_close(db); fputs("site page: that route is taken by the site itself\n", stderr); return 2;
    }
    /* Read the replacement once, before any CMS deletion. A missing file,
     * directory, empty body or embedded NUL must leave the existing page
     * and its fragments untouched. Removal does not need a new body. */
    if (!removing) {
        if (file) {
            struct stat info;
            if (stat(file, &info) || !S_ISREG(info.st_mode) || !(body = slurp(file, &size))) {
                sqlite3_close(db); fprintf(stderr, "site: cannot read a regular page file: %s\n", file); return 3;
            }
            if (size < 0 || memchr(body, '\0', (size_t)size)) {
                sqlite3_close(db); free(body); fputs("site page: the page contains a NUL byte\n", stderr); return 2;
            }
        } else body = strdup(text);
        if (!body || !body[strspn(body, " \t\r\n")]) {
            sqlite3_close(db); free(body); fputs("site page: the page has no text\n", stderr); return 2;
        }
        if (find_share(s)) { sqlite3_close(db); free(body); return 3; }
    }
    {
        sqlite3_stmt *st = NULL;
        int taken = 0;
        if (sqlite3_prepare_v2(db, "SELECT id FROM site_page WHERE namespace=?1 AND route=?2", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
            taken = sqlite3_step(st) == SQLITE_ROW;
            if (taken) existing_page = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
        if (taken && (removing || replacing)) {
            if (!cms_page_version(s,db,name,route,existing_page,expected_digest)) {
                sqlite3_close(db); free(body); fputs("site: the page changed or its snapshot is unavailable; read it again before saving\n",stderr); return 5;
            }
            taken = 0;
        } else if (removing) { sqlite3_close(db); fprintf(stderr, "site: %s has no page at %s\n", name, route); return 7; }
        sqlite3_close(db);
        if (removing) {
            prepared = open_memstream(&request, &request_size);
            if (!prepared) return 5;
            fputs("{\"operation\":\"remove\",\"route\":", prepared); json_text(prepared, route);
            fprintf(prepared, ",\"expected_id\":%lld,\"expected_digest\":\"%s\",\"page\":null,\"fragments\":[]}", existing_page, expected_digest);
            rc = fclose(prepared) ? 5 : cms_page_write(s, name, request);
            free(request);
            if (rc) return rc;
            char gone_file[PATH_MAX], gone_code[16] = "";
            int rc_server = ensure_server(s);
            if (rc_server) return rc_server;
            snprintf(gone_file, sizeof(gone_file), "%s/.page-gone.html", s->home);
            http_get(s, host, route, gone_file, gone_code, sizeof(gone_code));
            unlink(gone_file);
            fputs("{\"schema\":\"bonfyre.site.page.v1\",\"site\":", stdout); json_text(stdout, name);
            fputs(",\"route\":", stdout); json_text(stdout, route);
            printf(",\"removed\":true,\"now_answers\":\"%.3s\"}\n", gone_code);
            return strncmp(gone_code, "404", 3) ? 9 : 0;
        }
        if (taken) { free(body); fprintf(stderr, "site: %s already has a page at %s (use --replace to rewrite it)\n", name, route); return 7; }
    }
    /* A Markdown page is cut where its own second-level headings are, so a
     * code block or a table stays in one piece; its opening paragraph is the
     * summary unless one was given. */
    {
        char *start = body;
        while (*start == '\n' || *start == ' ') start++;
        if (start[0] == '#' && start[1] == ' ') {
            char *eol = strchr(start, '\n'), *end;
            marked = eol ? eol + 1 : start + strlen(start);
            while (*marked == '\n') marked++;
            if (strncmp(marked, "## ", 3) && strncmp(marked, "```", 3) && !summary) {
                end = strstr(marked, "\n\n");
                opening = end ? strndup(marked, (size_t)(end - marked)) : strdup(marked);
                marked = end ? end + 2 : marked + strlen(marked);
            }
        }
    }
    if (existing_page && !group)
        json = json_object("route", route, "title", title, "heading", title,
                           "lede", summary ? summary : opening ? opening : "", "status", "published", NULL);
    else
        json = json_object("route", route, "title", title, "heading", title, "eyebrow", group ? group : "",
                           "lede", summary ? summary : opening ? opening : "", "status", "published", NULL);
    prepared = open_memstream(&request, &request_size);
    if (!json || !prepared) {
        free(json); free(opening); free(body);
        if (prepared) fclose(prepared);
        free(request); return 5;
    }
    fputs("{\"operation\":", prepared); json_text(prepared, existing_page ? "replace" : "create");
    fputs(",\"route\":", prepared); json_text(prepared, route);
    if (existing_page) fprintf(prepared, ",\"expected_digest\":\"%s\"", expected_digest);
    fprintf(prepared, ",\"expected_id\":%lld,\"page\":%s,\"fragments\":[", existing_page, json);
    rc = 0;
    free(json); free(opening);
    for (char *section = marked, *next; !rc && section && *section; section = next) {
        char coordinate[300], ordinal[24], heading[200] = "", *text_start = section, *eol;
        size_t fence_width = 0;
        next = NULL;
        for (char *line = section; line; line = strchr(line, '\n') ? strchr(line, '\n') + 1 : NULL) {
            if (fence_width) { if (bf_markdown_fence_closes(line, fence_width)) fence_width = 0; }
            else if (bf_markdown_fence_width(line)) fence_width = bf_markdown_fence_width(line);
            else if ( line != section && !strncmp(line, "## ", 3)) { line[-1] = '\0'; next = line; break; }
        }
        if (!strncmp(text_start, "## ", 3)) {
            eol = strchr(text_start, '\n');
            snprintf(heading, sizeof(heading), "%.*s", (int)((eol ? eol : text_start + strlen(text_start)) - text_start - 3), text_start + 3);
            text_start = eol ? eol + 1 : text_start + strlen(text_start);
        }
        while (*text_start == '\n') text_start++;
        snprintf(coordinate, sizeof(coordinate), "%s:%s:body:%ld", name, route + 1, position);
        snprintf(ordinal, sizeof(ordinal), "%ld", position++);
        json = json_object("coordinate", coordinate, "page", route, "slot", "body", "audience", "anonymous", "kind", "copy",
                           "=position", ordinal, "heading", heading, "body", text_start, "status", "published", NULL);
        if (!json) rc = 5;
        else fprintf(prepared, "%s%s", position > 1 ? "," : "", json);
        free(json);
    }
    for (char *section = marked ? NULL : body, *next; !rc && section && *section; section = next) {
        char coordinate[300], ordinal[24], heading[200] = "", *text_start = section, *eol;
        next = strstr(section, "\n\n");
        if (next) { *next = '\0'; next += 2; while (*next == '\n') next++; }
        while (*text_start == '\n' || *text_start == ' ') text_start++;
        if (!*text_start) continue;
        eol = strchr(text_start, '\n');
        if (eol && eol - text_start < 80 && eol > text_start && !strchr(".:;,!?", eol[-1])) {
            snprintf(heading, sizeof(heading), "%.*s", (int)(eol - text_start), text_start);
            text_start = eol + 1;
        }
        snprintf(coordinate, sizeof(coordinate), "%s:%s:body:%ld", name, route + 1, position);
        snprintf(ordinal, sizeof(ordinal), "%ld", position++);
        json = json_object("coordinate", coordinate, "page", route, "slot", "body", "audience", "anonymous", "kind", "copy",
                           "=position", ordinal, "heading", heading, "body", text_start, "status", "published", NULL);
        if (!json) rc = 5;
        else fprintf(prepared, "%s%s", position > 1 ? "," : "", json);
        free(json);
    }
    free(body);
    fputs("]}", prepared);
    if (fclose(prepared)) rc = 5;
    if (!rc) rc = cms_page_write(s, name, request);
    free(request);
    if (rc) return rc;
    if ((rc = ensure_server(s))) return rc;
    snprintf(page_file, sizeof(page_file), "%s/.page-readback.html", s->home);
    http_get(s, host, route, page_file, code, sizeof(code));
    rc = !strncmp(code, "200", 3) && file_contains(page_file, "site-page") ? 0 : 9;
    unlink(page_file);
    snprintf(url, sizeof(url), "http://%s:%d%s", host, s->port, route);
    fputs("{\"schema\":\"bonfyre.site.page.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"route\":", stdout); json_text(stdout, route);
    fputs(",\"url\":", stdout); json_text(stdout, url);
    printf(",\"sections\":%ld,\"page_served\":%s,\"in_navigation\":true,\"assistants\":\"list_pages and get_page now return it\"}\n", position, rc ? "false" : "true");
    if (!rc && !arg_flag(argc, argv, "--no-open") && isatty(1)) {
#ifdef __APPLE__
        char *const av[] = {"open", url, NULL};
#else
        char *const av[] = {"xdg-open", url, NULL};
#endif
        run(av, NULL, 0, NULL);
    }
    return rc;
}

/* A key for one assistant: issued by the content store for this site's
 * namespace alone, tried against the endpoint before it is handed over, and
 * shown once.  --write lets its holder leave the owner a message. */
static int site_key(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *label = arg_value(argc, argv, "--label");
    int write = arg_flag(argc, argv, "--write");
    char name[128], host[256], profile[128], subject[160], token[128] = "", endpoint[400], header[220];
    char *out = malloc(1 << 16);
    sqlite3 *db = open_cms(s);
    const char *found;
    int rc, accepted, refused;
    if (!key || !out) { fputs("site key NAME [--label WHO] [--write] | --withdraw WHO | --list\n", stderr); if (db) sqlite3_close(db); free(out); return 2; }
    if (arg_flag(argc, argv, "--list") && db) {
        /* Who holds a key, what it may do, and whether it still works.  Keys themselves are not kept. */
        sqlite3_stmt *st = NULL;
        long n = 0;
        if (s->json) fputs("{\"schema\":\"bonfyre.site.keys.v1\",\"keys\":[", stdout);
        else printf("%-18s %-22s %-8s %s\n", "HOLDER", "ISSUED", "WRITES", "STATE");
        if (sqlite3_prepare_v2(db, "SELECT COALESCE(t.label,''), t.created_at, EXISTS(SELECT 1 FROM _permissions p WHERE p.token_hash=t.token_hash "
                                   "AND p.content_type='service_lead' AND p.action='create'), t.active FROM _tokens t WHERE t.scope=?1 "
                                   "ORDER BY t.created_at", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, key, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                if (s->json) {
                    printf("%s{\"holder\":", n ? "," : ""); json_text(stdout, (const char *)sqlite3_column_text(st, 0));
                    printf(",\"issued\":\"%s\",\"may_write\":%s,\"active\":%s}", (const char *)sqlite3_column_text(st, 1),
                           sqlite3_column_int(st, 2) ? "true" : "false", sqlite3_column_int(st, 3) ? "true" : "false");
                } else printf("%-18.18s %-22.22s %-8s %s\n", (const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1),
                              sqlite3_column_int(st, 2) ? "yes" : "no", sqlite3_column_int(st, 3) ? "works" : "withdrawn");
                n++;
            }
            sqlite3_finalize(st);
        }
        sqlite3_close(db);
        free(out);
        if (s->json) printf("],\"count\":%ld}\n", n);
        else if (!n) printf("No keys issued for %s. Anyone may read it; nobody may write.\n", key);
        return 0;
    }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        free(out);
        return 7;
    }
    sqlite3_close(db);
    {
        /* Withdrawing is the store's own act; it is then tried, by asking the
         * endpoint with a key of that holder if one is supplied to check. */
        const char *withdraw = arg_value(argc, argv, "--withdraw");
        if (withdraw) {
            int rc_revoke;
            if (find_share(s)) { free(out); return 3; }
            {
                char *const av[] = {s->cms_bin, "token", "revoke", "--scope", name, "--label", (char *)withdraw, "--db", s->cms_db,
                                    "--schemas", s->schemas, NULL};
                rc_revoke = run(av, out, 1 << 16, NULL);
            }
            if (rc_revoke || !strstr(out, "\"revoked\":")) {
                fprintf(stderr, "site: %s has no working key held by %s\n", name, withdraw);
                free(out);
                return 7;
            }
            fputs("{\"schema\":\"bonfyre.site.key.v1\",\"site\":", stdout); json_text(stdout, name);
            fputs(",\"withdrawn\":", stdout); json_text(stdout, withdraw);
            fputs(",\"effect\":\"refused from the next request on; what it asked before stays in the record\"}\n", stdout);
            free(out);
            return 0;
        }
    }
    if (!label || !*label) label = write ? "writer" : "reader";
    for (const char *p = label; *p; p++)
        if (!isalnum((unsigned char)*p) && *p != '-' && *p != '_') { fputs("site key: a label is letters, digits, dashes\n", stderr); free(out); return 2; }
    if (find_share(s)) { free(out); return 3; }
    {
        char *const av[] = {s->cms_bin, "token", "issue", "--scope", name, "--actions", write ? "find,findOne,create" : "find,findOne",
                            "--label", (char *)label, "--db", s->cms_db, "--schemas", s->schemas, NULL};
        if (run(av, out, 1 << 16, NULL) || !(found = strstr(out, "bfcms_"))) { fputs("site: the content store issued no key\n", stderr); free(out); return 5; }
        snprintf(token, sizeof(token), "%.70s", found);
    }
    if (strlen(token) != 70) { fputs("site: the content store here issues short keys; install a current bonfyre-cms\n", stderr); free(out); return 5; }
    if ((rc = ensure_server(s))) { free(out); return rc; }
    snprintf(endpoint, sizeof(endpoint), "http://127.0.0.1:%d/mcp/%s", s->port, name);
    snprintf(header, sizeof(header), "Authorization: Bearer %s", token);
    {
        /* The key is tried, and so is one that was never issued. */
        char *const good[] = {"curl", "-s", "-m", "20", "-X", "POST", "-H", "content-type: application/json", "-H", header, "--data-binary",
                              "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"site_info\",\"arguments\":{}}}", endpoint, NULL};
        char *const bad[] = {"curl", "-s", "-m", "20", "-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", "-H", "content-type: application/json",
                             "-H", "Authorization: Bearer bfcms_0000000000000000000000000000000000000000000000000000000000000000", "--data-binary",
                             "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}", endpoint, NULL};
        accepted = !run(good, out, 1 << 16, NULL) && strstr(out, "\"structuredContent\"") != NULL;
        refused = !run(bad, out, 1 << 16, NULL) && !strncmp(out, "401", 3);
    }
    free(out);
    if (!accepted || !refused) { fputs("site: the endpoint did not treat the new key as it should; the key was issued but is unproven\n", stderr); return 13; }
    if (s->json) {
        fputs("{\"schema\":\"bonfyre.site.key.v1\",\"site\":", stdout); json_text(stdout, name);
        fputs(",\"label\":", stdout); json_text(stdout, label);
        printf(",\"may_write\":%s,\"key\":\"%s\",\"endpoint\":\"%s\",\"accepted\":true,\"unissued_key_refused\":true}\n", write ? "true" : "false", token, endpoint);
    } else {
        const char *bold = styled() ? "\033[1m" : "", *dim = styled() ? "\033[2m" : "", *off = styled() ? "\033[0m" : "";
        printf("\n%sA key for %s%s  (%s; %s)\n\n", bold, label, off, name, write ? "may read and leave the owner a message" : "may read");
        printf("  key        %s%s%s\n", bold, token, off);
        printf("             %sshown once; only its fingerprint is kept%s\n", dim, off);
        printf("  checked    accepted by the endpoint; a key that was never issued is refused\n\n");
        printf("  %sClaude Code%s      claude mcp add --transport http %s %s --header \"Authorization: Bearer %s\"\n", bold, off, name, endpoint, token);
        printf("  %sOther clients%s    send the header  Authorization: Bearer <key>\n\n", bold, off);
        printf("  %sWhat it asks is recorded under \"%s:%s\": bonfyre site usage %s%s\n\n", dim, name, label, name, off);
    }
    return 0;
}

/* What assistants have asked of a site, from the meter. */
static int site_meter_usage(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char prefix[160];
    long total = 0;
    int first = 1;
    if (!key || !slug_ok(key)) { fputs("site usage NAME [--json]\n", stderr); return 2; }
    snprintf(prefix, sizeof(prefix), "%s:%%", key);
    if (sqlite3_open_v2(s->meter_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT substr(key_id, instr(key_id,':')+1), substr(receipt_ref,5), count(*), max(timestamp) FROM events "
                               /* what a customer is billed for sits in the same meter under its own key; it is not an assistant's call */
                               "WHERE key_id LIKE ?1 AND key_id NOT LIKE '%:customer:%' GROUP BY 1, 2 ORDER BY 1, 3 DESC", -1, &st, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        if (s->json) puts("{\"schema\":\"bonfyre.site.usage.v1\",\"calls\":0,\"by_caller\":[]}");
        else printf("No assistant has asked %s anything yet.\n", key);
        return 0;
    }
    sqlite3_bind_text(st, 1, prefix, -1, SQLITE_STATIC);
    if (s->json) fputs("{\"schema\":\"bonfyre.site.usage.v1\",\"by_caller\":[", stdout);
    else printf("%-18s %-16s %8s  %s\n", "CALLER", "ASKED", "CALLS", "LAST");
    while (sqlite3_step(st) == SQLITE_ROW) {
        total += sqlite3_column_int64(st, 2);
        if (s->json) {
            printf("%s{\"caller\":", first ? "" : ","); json_text(stdout, (const char *)sqlite3_column_text(st, 0));
            fputs(",\"tool\":", stdout); json_text(stdout, (const char *)sqlite3_column_text(st, 1));
            printf(",\"calls\":%lld,\"last\":\"%s\"}", sqlite3_column_int64(st, 2), (const char *)sqlite3_column_text(st, 3));
        } else printf("%-18.18s %-16.16s %8lld  %s\n", (const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1),
                      sqlite3_column_int64(st, 2), (const char *)sqlite3_column_text(st, 3));
        first = 0;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (s->json) printf("],\"calls\":%ld}\n", total);
    else if (!total) printf("\nNo assistant has asked %s anything yet.\n", key);
    else printf("\n%ld call%s. Callers without a key are \"public\" (60 a minute); a key gets 600.\n", total, total == 1 ? "" : "s");
    return 0;
}

/* Messages left for the owner. */
static int site_inbox(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    long n = 0;
    if (!key || !slug_ok(key)) { fputs("site inbox NAME [--json]\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (s->json) fputs("{\"schema\":\"bonfyre.site.inbox.v1\",\"messages\":[", stdout);
    if (db && sqlite3_prepare_v2(db, "SELECT lead_key, received_at, form, json_extract(fields_json,'$.name'), json_extract(fields_json,'$.contact'), "
                                     "json_extract(fields_json,'$.message'), COALESCE(note,''), COALESCE(json_extract(fields_json,'$.about'),''), "
                                     "COALESCE(json_extract(fields_json,'$.answers'),'') FROM service_lead WHERE namespace=?1 AND site_id=?1 "
                                     "ORDER BY received_at DESC, id DESC LIMIT 100", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, key, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (s->json) {
                printf("%s{\"reference\":", n ? "," : ""); json_text(stdout, (const char *)sqlite3_column_text(st, 0));
                fputs(",\"received_at\":", stdout); json_text(stdout, (const char *)sqlite3_column_text(st, 1));
                fputs(",\"name\":", stdout); json_text(stdout, (const char *)sqlite3_column_text(st, 3));
                fputs(",\"contact\":", stdout); json_text(stdout, (const char *)sqlite3_column_text(st, 4));
                fputs(",\"message\":", stdout); json_text(stdout, (const char *)sqlite3_column_text(st, 5));
                fputs(",\"how\":", stdout); json_text(stdout, (const char *)sqlite3_column_text(st, 6));
                {
                    const char *about = (const char *)sqlite3_column_text(st, 7), *answers = (const char *)sqlite3_column_text(st, 8);
                    if (about && *about) { fputs(",\"about\":", stdout); json_text(stdout, about); }
                    /* The answers were written as a JSON object by the contact page itself. */
                    if (answers && *answers == '{') printf(",\"answers\":%s", answers);
                }
                fputc('}', stdout);
            } else printf("%s%s  %s <%s>\n  %s\n  %s\n  reference %s\n", n ? "\n" : "", (const char *)sqlite3_column_text(st, 1), (const char *)sqlite3_column_text(st, 3),
                          (const char *)sqlite3_column_text(st, 4), (const char *)sqlite3_column_text(st, 5), (const char *)sqlite3_column_text(st, 6),
                          (const char *)sqlite3_column_text(st, 0));
            if (!s->json && sqlite3_column_text(st, 8) && *sqlite3_column_text(st, 8) == '{') {
                sqlite3_stmt *a = NULL;
                if (sqlite3_prepare_v2(db, "SELECT key, value FROM json_each(?1)", -1, &a, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(a, 1, (const char *)sqlite3_column_text(st, 8), -1, SQLITE_TRANSIENT);
                    while (sqlite3_step(a) == SQLITE_ROW) printf("  %s: %s\n", (const char *)sqlite3_column_text(a, 0), (const char *)sqlite3_column_text(a, 1));
                    sqlite3_finalize(a);
                }
            }
            n++;
        }
        sqlite3_finalize(st);
    }
    if (db) sqlite3_close(db);
    if (s->json) printf("],\"count\":%ld}\n", n);
    else if (!n) printf("No messages for %s yet.\n", key);
    return 0;
}

/* ---------- offer / post / share ---------- */

/* A page record, made if the site does not have one at this route yet. */
static int ensure_page(Site *s, const char *site, const char *route, const char *title, const char *summary) {
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    char *json;
    int exists = 0, rc;
    if (db && sqlite3_prepare_v2(db, "SELECT 1 FROM site_page WHERE namespace=?1 AND route=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        exists = sqlite3_step(st) == SQLITE_ROW;
        sqlite3_finalize(st);
    }
    if (db) sqlite3_close(db);
    if (exists) return 0;
    json = json_object("route", route, "title", title, "heading", title, "lede", summary, "status", "published", NULL);
    rc = json ? cms_create_in(s, site, "site_page", json) : 5;
    free(json);
    return rc;
}

static long next_position(Site *s, const char *site, const char *route) {
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    long position = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT COALESCE(max(position),-1)+1 FROM site_fragment WHERE namespace=?1 AND page=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, route, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) position = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    if (db) sqlite3_close(db);
    return position;
}

/* Something the site sells: what it is, the price as the owner words it, the
 * same price as a number when one is given, and whether a time can be asked
 * for.  It appears on the site's offers page with a way to take it up, and
 * assistants read it through list_offers.  Nothing takes payment. */
static int site_offer(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *title = arg_value(argc, argv, "--title"), *price = arg_value(argc, argv, "--price");
    const char *text = arg_value(argc, argv, "--text"), *amount = arg_value(argc, argv, "--amount"), *currency = arg_value(argc, argv, "--currency");
    const char *unit = arg_value(argc, argv, "--unit"), *route = arg_value(argc, argv, "--page"), *remove = arg_value(argc, argv, "--withdraw");
    int bookable = arg_flag(argc, argv, "--bookable"), rc;
    char name[128], host[256], profile[128], subject[160], coordinate[300], ordinal[24], page_file[PATH_MAX], code[64] = "", href[400], url[400];
    char *contract = NULL;
    size_t contract_len = 0;
    int asks = 0;
    sqlite3 *db = open_cms(s);
    char *json;
    long position;
    /* Intake questions: each --ask is FACT=Question, where FACT names the fact
     * the answer states (site_count, php_version ...).  The contact form asks
     * them when someone takes up this offer; answers stay the person's word. */
    for (int i = 2; i + 1 < argc; i++) {
        const char *eq;
        size_t fact_len;
        if (strcmp(argv[i], "--ask")) continue;
        eq = strchr(argv[i + 1], '=');
        fact_len = eq ? (size_t)(eq - argv[i + 1]) : 0;
        if (!eq || !fact_len || fact_len > 40 || strlen(eq + 1) < 3 || strlen(eq + 1) > 200 || ++asks > 8) {
            fputs("site offer: --ask is FACT=Question (FACT in a-z, 0-9 and _; up to eight questions of 3 to 200 characters)\n", stderr);
            if (db) sqlite3_close(db);
            return 2;
        }
        for (size_t k = 0; k < fact_len; k++) {
            char c = argv[i + 1][k];
            if (!(islower((unsigned char)c) || isdigit((unsigned char)c) || c == '_')) {
                fputs("site offer: an --ask fact is written in a-z, 0-9 and _, such as site_count\n", stderr);
                if (db) sqlite3_close(db);
                return 2;
            }
        }
        for (int j = 2; j < i; j++)
            if (!strcmp(argv[j], "--ask") && !strncmp(argv[j + 1], argv[i + 1], fact_len + 1)) {
                fputs("site offer: each --ask fact is asked once\n", stderr);
                if (db) sqlite3_close(db);
                return 2;
            }
    }
    if (!key || (!remove && (!title || !text))) {
        fputs("site offer NAME --title T --text \"...\" [--price \"$450 per project\"] [--amount 450 --currency USD --unit \"per project\"] [--bookable]\n"
              "          [--page /route] [--ask FACT=Question ...]   questions the contact form asks whoever takes this up\n"
              "site offer NAME --withdraw ID\n", stderr);
        if (db) sqlite3_close(db);
        return 2;
    }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    if (remove) {
        sqlite3_stmt *st = NULL;
        int mine = 0;
        if (sqlite3_prepare_v2(db, "SELECT 1 FROM site_fragment WHERE namespace=?1 AND kind='offer' AND id=?2", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 2, atoll(remove));
            mine = sqlite3_step(st) == SQLITE_ROW;
            sqlite3_finalize(st);
        }
        sqlite3_close(db);
        if (!mine || find_share(s)) { fprintf(stderr, "site: %s has no offer %s\n", name, remove); return 7; }
        {
            char out[1024];
            char *const av[] = {s->cms_bin, "entry", "delete", "site_fragment", (char *)remove, "--db", s->cms_db, "--schemas", s->schemas, NULL};
            if (run(av, out, sizeof(out), NULL)) { fputs("site: the content store did not remove it\n", stderr); return 5; }
        }
        printf("{\"schema\":\"bonfyre.site.offer.v1\",\"site\":\"%s\",\"withdrawn\":%lld}\n", name, atoll(remove));
        return 0;
    }
    sqlite3_close(db);
    /* A price written as "$85 per person" already states the amount and its
     * unit; "from $600" is a floor, not an amount, and stays words only. */
    if (!amount && price && price[0] == '$' && isdigit((unsigned char)price[1])) {
        static char read_amount[32], read_unit[120];
        const char *p = price + 1;
        size_t k = 0;
        while ((isdigit((unsigned char)*p) || *p == '.' || *p == ',') && k + 1 < sizeof(read_amount)) { if (*p != ',') read_amount[k++] = *p; p++; }
        read_amount[k] = '\0';
        while (*p == ' ') p++;
        snprintf(read_unit, sizeof(read_unit), "%s", p);
        amount = read_amount;
        if (!currency) currency = "USD";
        if (!unit && read_unit[0]) unit = read_unit;
    }
    if (amount) { char *end; double v = strtod(amount, &end); if (*end || v < 0) { fputs("site offer: --amount is a number\n", stderr); return 2; } }
    if (currency && (strlen(currency) != 3 || !isupper((unsigned char)currency[0]))) { fputs("site offer: --currency is three capital letters, such as USD\n", stderr); return 2; }
    if (!route) route = "/offers";
    if (find_share(s)) return 3;
    if ((rc = ensure_page(s, name, route, "Offers", "What we offer, and what it costs."))) return rc;
    position = next_position(s, name, route);
    snprintf(coordinate, sizeof(coordinate), "%s:%s:offer:%ld:%lld", name, route + 1, position, (long long)time(NULL));
    snprintf(ordinal, sizeof(ordinal), "%ld", position);
    {
        FILE *c = open_memstream(&contract, &contract_len);
        int first = 1;
        if (!c) return 5;
        fprintf(c, "{\"offer\":true%s%s,\"currency\":\"%s\",\"unit\":", amount ? ",\"amount\":" : "", amount ? amount : "",
                currency ? currency : (amount ? "USD" : ""));
        json_text(c, unit ? unit : "");
        fprintf(c, ",\"bookable\":%d", bookable);
        if (asks) {
            fputs(",\"asks\":[", c);
            for (int i = 2; i + 1 < argc; i++) {
                const char *eq;
                if (strcmp(argv[i], "--ask")) continue;
                eq = strchr(argv[i + 1], '=');
                fprintf(c, "%s{\"fact\":\"%.*s\",\"question\":", first ? "" : ",", (int)(eq - argv[i + 1]), argv[i + 1]);
                json_text(c, eq + 1);
                fputc('}', c);
                first = 0;
            }
            fputc(']', c);
        }
        fputc('}', c);
        fclose(c);
    }
    snprintf(href, sizeof(href), "/contact");
    json = json_object("coordinate", coordinate, "page", route, "slot", "body", "audience", "anonymous", "kind", "offer", "=position", ordinal,
                       "heading", title, "body", text, "label", price ? price : "", "href", href, "=projection_contract", contract, "status", "published", NULL);
    free(contract);
    rc = json ? cms_create_in(s, name, "site_fragment", json) : 5;
    free(json);
    if (rc) return rc;
    if ((rc = ensure_server(s))) return rc;
    snprintf(page_file, sizeof(page_file), "%s/.offer-readback.html", s->home);
    http_get(s, host, route, page_file, code, sizeof(code));
    rc = !strncmp(code, "200", 3) && file_contains(page_file, "class=\"offer\"") && file_contains(page_file, title) ? 0 : 9;
    unlink(page_file);
    snprintf(url, sizeof(url), "http://%s:%d%s", host, s->port, route);
    fputs("{\"schema\":\"bonfyre.site.offer.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"offer\":", stdout); json_text(stdout, title);
    fputs(",\"price\":", stdout); json_text(stdout, price ? price : "");
    fputs(",\"url\":", stdout); json_text(stdout, url);
    printf(",\"bookable\":%s,\"intake_questions\":%d,\"on_page\":%s,\"assistants\":\"list_offers returns it%s\",\"cannot_infer\":\"a listed price is not a sale, and a requested time is not a booking\"}\n",
           bookable ? "true" : "false", asks, rc ? "false" : "true", bookable ? "; request_booking can ask for a time" : "");
    return rc;
}

/* A dated post added to a site, or one taken down. */
static int site_post(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *title = arg_value(argc, argv, "--title"), *text = arg_value(argc, argv, "--text");
    const char *file = arg_value(argc, argv, "--file"), *remove = arg_value(argc, argv, "--withdraw");
    char name[128], host[256], profile[128], subject[160], post[160], author[96], when[32], page_file[PATH_MAX], code[64] = "";
    sqlite3 *db = open_cms(s);
    char *body = NULL, *json;
    long size = 0;
    time_t now = time(NULL);
    int rc;
    if (!key || (!remove && (!title || (!text && !file)))) {
        fputs("site post NAME --title T (--text \"...\" | --file F)\nsite post NAME --withdraw ID\n", stderr);
        if (db) sqlite3_close(db);
        return 2;
    }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    if (remove) {
        sqlite3_stmt *st = NULL;
        int mine = 0;
        if (sqlite3_prepare_v2(db, "SELECT 1 FROM community_post WHERE community=?1 AND id=?2", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
            sqlite3_bind_int64(st, 2, atoll(remove));
            mine = sqlite3_step(st) == SQLITE_ROW;
            sqlite3_finalize(st);
        }
        sqlite3_close(db);
        if (!mine || find_share(s)) { fprintf(stderr, "site: %s has no post %s\n", name, remove); return 7; }
        {
            char out[1024];
            char *const av[] = {s->cms_bin, "entry", "delete", "community_post", (char *)remove, "--db", s->cms_db, "--schemas", s->schemas, NULL};
            if (run(av, out, sizeof(out), NULL)) { fputs("site: the content store did not remove it\n", stderr); return 5; }
        }
        printf("{\"schema\":\"bonfyre.site.post.v1\",\"site\":\"%s\",\"withdrawn\":%lld}\n", name, atoll(remove));
        return 0;
    }
    /* Two posts written in the same second each keep their own name. */
    snprintf(post, sizeof(post), "%s:post-%lld", name, (long long)now);
    for (int n = 2; count_where(db, "community_post", "post", post) > 0 && n < 1000; n++)
        snprintf(post, sizeof(post), "%s:post-%lld-%d", name, (long long)now, n);
    sqlite3_close(db);
    if (file) { body = slurp(file, &size); if (!body) { fprintf(stderr, "site: cannot read %s\n", file); return 3; } }
    else body = strdup(text);
    if (!body || strlen(body) < 3 || find_share(s)) { free(body); fputs("site post: the post has no text\n", stderr); return 2; }
    snprintf(author, sizeof(author), "institution:%s", name);
    strftime(when, sizeof(when), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
    json = json_object("post", post, "community", name, "author_subject", author, "title", title, "body", body, "published_at", when,
                       "visibility", "public", "moderation_state", "accepted", "cannot_infer", "a post is what its site published", "status", "published", NULL);
    rc = json ? cms_create(s, "community_post", json) : 5;
    free(json); free(body);
    if (rc) return rc;
    if ((rc = ensure_server(s))) return rc;
    snprintf(page_file, sizeof(page_file), "%s/.post-readback.html", s->home);
    http_get(s, host, "/", page_file, code, sizeof(code));
    rc = !strncmp(code, "200", 3) && file_contains(page_file, title) ? 0 : 9;
    unlink(page_file);
    fputs("{\"schema\":\"bonfyre.site.post.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"title\":", stdout); json_text(stdout, title);
    printf(",\"on_page\":%s,\"assistants\":\"list_posts returns it; search finds it once the index catches up, which asking starts\"}\n", rc ? "false" : "true");
    return rc;
}

/* A site reachable from anywhere, for as long as the command is left running
 * in the background: Cloudflare's tunnel gives it a public HTTPS address and
 * hands each request to this machine's server under the site's own host name.
 * The address is tried from the outside before it is given out.  --stop ends it. */
static int site_share(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    char name[128], host[256], profile[128], subject[160], log_path[PATH_MAX], pid_path[PATH_MAX], origin[64], public_url[300] = "", endpoint[400];
    char *out;
    sqlite3 *db = open_cms(s);
    pid_t pid;
    int rc;
    if (!key) { fputs("site share NAME [--stop]\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    sqlite3_close(db);
    snprintf(log_path, sizeof(log_path), "%s/share-%s.log", s->home, name);
    snprintf(pid_path, sizeof(pid_path), "%s/share-%s.pid", s->home, name);
    {
        FILE *f = fopen(pid_path, "r");
        int old = 0;
        if (f) { if (fscanf(f, "%d", &old) == 1 && old > 1) kill(old, SIGTERM); fclose(f); unlink(pid_path); }
        if (arg_flag(argc, argv, "--stop")) { printf("{\"schema\":\"bonfyre.site.share.v1\",\"site\":\"%s\",\"shared\":false}\n", name); return 0; }
    }
    if ((rc = ensure_server(s))) return rc;
    snprintf(origin, sizeof(origin), "http://127.0.0.1:%d", s->port);
    unlink(log_path);
    pid = fork();
    if (pid < 0) return 6;
    if (pid == 0) {
        int log_fd;
        setsid();
        log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd >= 0) { dup2(log_fd, 1); dup2(log_fd, 2); }
        { int devnull = open("/dev/null", O_RDONLY); if (devnull >= 0) dup2(devnull, 0); }
        execlp("cloudflared", "cloudflared", "tunnel", "--no-autoupdate", "--url", origin, "--http-host-header", host, (char *)NULL);
        _exit(127);
    }
    { FILE *f = fopen(pid_path, "w"); if (f) { fprintf(f, "%d\n", (int)pid); fclose(f); } }
    for (int i = 0; i < 300 && !public_url[0]; i++) {
        struct timespec nap = {0, 100000000L};
        long size = 0;
        char *log_text = slurp(log_path, &size);
        const char *at = log_text ? strstr(log_text, "https://") : NULL;
        while (at && !strstr(at, ".trycloudflare.com")) at = NULL;
        for (; at; at = strstr(at + 8, "https://")) {
            const char *end = at;
            while (*end && !isspace((unsigned char)*end) && *end != '|') end++;
            if ((size_t)(end - at) < sizeof(public_url) && memmem(at, (size_t)(end - at), ".trycloudflare.com", 18)) { snprintf(public_url, sizeof(public_url), "%.*s", (int)(end - at), at); break; }
        }
        free(log_text);
        if (kill(pid, 0)) break;
        nanosleep(&nap, NULL);
    }
    if (!public_url[0]) {
        kill(pid, SIGTERM); unlink(pid_path);
        fprintf(stderr, "site: no public address was given (is cloudflared installed?); see %s\n", log_path);
        return 14;
    }
    /* Asked through the public address, which leaves this machine and comes back. */
    snprintf(endpoint, sizeof(endpoint), "%s/mcp", public_url);
    out = malloc(1 << 16);
    rc = 14;
    for (int i = 0; i < 40 && out; i++) {
        struct timespec nap = {0, 500000000L};
        /* The name is minutes old; this machine's resolver may not know it
         * yet, so it is looked up over HTTPS at a public resolver. */
        char *const av[] = {"curl", "-s", "-m", "15", "--doh-url", "https://cloudflare-dns.com/dns-query", "-X", "POST",
                            "-H", "content-type: application/json", "--data-binary",
                            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"site_info\",\"arguments\":{}}}", endpoint, NULL};
        if (!run(av, out, 1 << 16, NULL) && strstr(out, "\"structuredContent\"")) { rc = 0; break; }
        nanosleep(&nap, NULL);
    }
    free(out);
    if (rc) { kill(pid, SIGTERM); unlink(pid_path); fprintf(stderr, "site: %s did not answer from outside\n", endpoint); return rc; }
    if (s->json) {
        fputs("{\"schema\":\"bonfyre.site.share.v1\",\"site\":", stdout); json_text(stdout, name);
        fputs(",\"shared\":true,\"url\":", stdout); json_text(stdout, public_url);
        fputs(",\"mcp\":", stdout); json_text(stdout, endpoint);
        fputs(",\"answered_from_outside\":true,\"lasts\":\"until bonfyre site share NAME --stop, or this machine sleeps\"}\n", stdout);
    } else {
        const char *bold = styled() ? "\033[1m" : "", *dim = styled() ? "\033[2m" : "", *off = styled() ? "\033[0m" : "";
        printf("\n%s%s is on the internet%s\n\n", bold, name, off);
        printf("  site       %s%s/%s\n", bold, public_url, off);
        printf("  assistants %s%s%s\n", bold, endpoint, off);
        printf("  checked    asked through the public address and answered\n\n");
        printf("  %sClaude Code%s   claude mcp add --transport http %s %s\n\n", bold, off, name, endpoint);
        printf("  %sA temporary address from Cloudflare's tunnel, served from this machine. It ends with: bonfyre site share %s --stop%s\n\n", dim, name, off);
    }
    return 0;
}

/* ---------- accept / paid / done / domain: the owners that already exist ----------
 *
 * A request the owner accepts becomes three things, each in the organ that
 * already owns it:
 *   a commitment   (the estate's commitment store): the site promises this
 *                  offer at this time to this person.  Its id is the offer and
 *                  the time, so the same time cannot be promised twice.
 *   an invoice     (Meter, then Pay): the offer's price is a rate, the
 *                  acceptance is one metered event under the customer, and Pay
 *                  invoices that usage.
 *   an opportunity (Finance): the request enters the books the estate ranks
 *                  and closes from.
 * None of this sends anything to the customer or moves money.
 */

typedef struct {
    char reference[200], form[40], name[160], contact[240], message[1200], offer[300], when[160];
    long long lead_row, offer_id;
    double amount;
    char currency[8];
    int has_amount, bookable;
} Lead;

static int read_lead(sqlite3 *db, const char *site, const char *reference, Lead *lead) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    memset(lead, 0, sizeof(*lead));
    if (sqlite3_prepare_v2(db, "SELECT id, form, COALESCE(json_extract(fields_json,'$.name'),''), COALESCE(json_extract(fields_json,'$.contact'),''), "
                               "COALESCE(json_extract(fields_json,'$.message'),''), COALESCE(json_extract(fields_json,'$.offer'),''), "
                               "COALESCE(json_extract(fields_json,'$.requested_time'),''), COALESCE(json_extract(fields_json,'$.offer_id'),0) "
                               "FROM service_lead WHERE namespace=?1 AND site_id=?1 AND lead_key=?2", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, site, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, reference, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        lead->lead_row = sqlite3_column_int64(st, 0);
        snprintf(lead->reference, sizeof(lead->reference), "%s", reference);
        snprintf(lead->form, sizeof(lead->form), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(lead->name, sizeof(lead->name), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(lead->contact, sizeof(lead->contact), "%s", (const char *)sqlite3_column_text(st, 3));
        snprintf(lead->message, sizeof(lead->message), "%s", (const char *)sqlite3_column_text(st, 4));
        snprintf(lead->offer, sizeof(lead->offer), "%s", (const char *)sqlite3_column_text(st, 5));
        snprintf(lead->when, sizeof(lead->when), "%s", (const char *)sqlite3_column_text(st, 6));
        lead->offer_id = sqlite3_column_int64(st, 7);
        found = 1;
    }
    sqlite3_finalize(st);
    if (found && lead->offer_id &&
        sqlite3_prepare_v2(db, "SELECT json_extract(projection_contract,'$.amount'), COALESCE(json_extract(projection_contract,'$.currency'),'USD'), "
                               "COALESCE(json_extract(projection_contract,'$.bookable'),0), COALESCE(heading,'') FROM site_fragment "
                               "WHERE namespace=?1 AND kind='offer' AND id=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, site, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, lead->offer_id);
        if (sqlite3_step(st) == SQLITE_ROW) {
            if (sqlite3_column_type(st, 0) != SQLITE_NULL) { lead->amount = sqlite3_column_double(st, 0); lead->has_amount = 1; }
            snprintf(lead->currency, sizeof(lead->currency), "%s", (const char *)sqlite3_column_text(st, 1));
            lead->bookable = sqlite3_column_int(st, 2);
            if (!lead->offer[0]) snprintf(lead->offer, sizeof(lead->offer), "%s", (const char *)sqlite3_column_text(st, 3));
        }
        sqlite3_finalize(st);
    }
    return found;
}

/* Lower-case letters and digits of a text, dashes between, for use in an id. */
static void slugify(const char *text, char *out, size_t cap) {
    size_t n = 0;
    for (; *text && n + 2 < cap; text++) {
        if (isalnum((unsigned char)*text)) out[n++] = (char)tolower((unsigned char)*text);
        else if (n && out[n - 1] != '-') out[n++] = '-';
    }
    while (n && out[n - 1] == '-') n--;
    out[n] = '\0';
}

static void commitment_id_for(const char *site, const Lead *lead, const char *when, char *out, size_t cap) {
    char slot[120];
    slugify(when, slot, sizeof(slot));
    if (lead->offer_id) snprintf(out, cap, "site:%s:offer-%lld:%s", site, lead->offer_id, slot);
    else snprintf(out, cap, "site:%s:lead:%s", site, lead->reference);
}

static int mark_lead(Site *s, const char *site, const Lead *lead, const char *handled, const char *note) {
    char id[32], out[512], *json = json_object("handled", handled, "note", note, NULL);
    int rc;
    snprintf(id, sizeof(id), "%lld", lead->lead_row);
    {
        char *const av[] = {s->cms_bin, "entry", "update", "service_lead", id, json, "--ns", (char *)site, "--db", s->cms_db, "--schemas", s->schemas, NULL};
        rc = json ? run(av, out, sizeof(out), NULL) : 5;
    }
    free(json);
    return rc;
}

static int site_accept(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *reference = arg_value(argc, argv, "--request"), *when_arg = arg_value(argc, argv, "--when");
    char name[128], host[256], profile[128], subject[160], commitment_id[BF_COMMITMENT_ID_MAX], committer[160], at[32], conditions[300], origin[260];
    char commitments_db[PATH_MAX], pay_db[PATH_MAX], finance_db[PATH_MAX], customer_key[260], op[64], amount_text[32], out[4096], period[8];
    char pay_bin[PATH_MAX], finance_bin[PATH_MAX], invoice[200] = "", opportunity[40] = "not recorded";
    const char *when;
    sqlite3 *db = open_cms(s);
    BfCommitment *commitments = NULL;
    BfCommitmentSpec spec = {0};
    BfCommitmentStatus status;
    Lead lead;
    time_t now = time(NULL);
    int invoiced = 0;
    if (!key || !reference) { fputs("site accept NAME --request REFERENCE [--when \"2026-10-14 14:00\"]\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject)) ||
        !read_lead(db, name, reference, &lead)) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: %s has no request %s (see: bonfyre site inbox %s)\n", key, reference, key);
        return 7;
    }
    sqlite3_close(db);
    when = when_arg && *when_arg ? when_arg : lead.when;
    if (lead.offer_id && lead.bookable && !*when) { fputs("site accept: say when with --when; the request named no time\n", stderr); return 2; }
    if (find_share(s)) return 3;
    strftime(at, sizeof(at), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
    strftime(period, sizeof(period), "%Y-%m", gmtime(&now));
    commitment_id_for(name, &lead, when, commitment_id, sizeof(commitment_id));
    snprintf(committer, sizeof(committer), "institution:%s", name);
    snprintf(conditions, sizeof(conditions), "%s%s", *when ? "at " : "as requested", when);
    snprintf(origin, sizeof(origin), "site-lead:%s", lead.reference);
    snprintf(commitments_db, sizeof(commitments_db), "%s/commitments.db", s->home);

    /* The promise, in the commitment store.  The same offer at the same time
     * is the same commitment, and the store refuses a second one. */
    if (bf_commitment_open(commitments_db, &commitments, stderr)) { fputs("site: the commitment store could not be opened\n", stderr); return 4; }
    spec.commitment_id = commitment_id; spec.committer = committer; spec.beneficiary = lead.contact;
    spec.resource = lead.offer[0] ? lead.offer : "reply to an inquiry"; spec.conditions = conditions; spec.expires_at = "";
    spec.authority = "site-owner"; spec.origin = origin; spec.enactment = "bonfyre site accept"; spec.at = at;
    status = bf_commitment_record(commitments, &spec);
    bf_commitment_close(commitments);
    if (status == BF_COMMITMENT_EXISTS) {
        fprintf(stderr, "site: %s is already promised for %s. Offer another time with --when, or see: bonfyre site promises %s\n",
                lead.offer[0] ? lead.offer : "that", when, name);
        return 15;
    }
    if (status != BF_COMMITMENT_OK) { fprintf(stderr, "site: the commitment store refused the promise (%d)\n", (int)status); return 5; }

    /* The price, through the meter and Pay: a rate for this offer, one event
     * under this customer, and the invoice Pay makes of that usage. */
    find_binary(pay_bin, sizeof(pay_bin), "BONFYRE_PAY_BINARY", "bonfyre-pay", s->self);
    snprintf(pay_db, sizeof(pay_db), "%s/pay.db", s->home);
    snprintf(customer_key, sizeof(customer_key), "%s:customer:%s", name, lead.reference);
    if (lead.has_amount && lead.amount > 0 && !access(s->meter_bin, X_OK) && !access(pay_bin, X_OK)) {
        unsigned long long h = 1469598103934665603ULL;
        char user_id[24], evidence[200];
        for (const char *p = lead.contact; *p; p++) { h ^= (unsigned char)*p; h *= 1099511628211ULL; }
        snprintf(user_id, sizeof(user_id), "%llu", h % 900000000ULL + 1000);
        snprintf(op, sizeof(op), "offer.%lld", lead.offer_id);
        snprintf(amount_text, sizeof(amount_text), "%.2f", lead.amount);
        snprintf(evidence, sizeof(evidence), "site-offer:%s:%lld", name, lead.offer_id);
        {
            char *const rate[] = {s->meter_bin, "admit-rate", "--db", s->meter_db, "--provider", "site", "--op", op, "--unit", "call",
                                  "--amount", amount_text, "--evidence", evidence, "--effective-at", at, NULL};
            char *const event[] = {s->meter_bin, "record", "--db", s->meter_db, "--key", customer_key, "--op", op, "--provider", "site",
                                   "--receipt", origin, "--attempt-ref", commitment_id, "--realization-ref", commitment_id, "--at", at, NULL};
            char *const bill[] = {pay_bin, "--db", pay_db, "invoice", "--user-id", user_id, "--api-key", customer_key, "--meter-db", s->meter_db,
                                  "--period", period, NULL};
            if (!run(rate, out, sizeof(out), NULL) && !run(event, out, sizeof(out), NULL) && !run(bill, out, sizeof(out), NULL)) {
                size_t n = 0;
                invoiced = 1;
                for (const char *p = out; *p && n + 1 < sizeof(invoice); p++) invoice[n++] = *p == '\n' ? ' ' : *p;
                invoice[n] = '\0';
            }
        }
    }

    /* The books: Finance imports the request as an opportunity. */
    find_binary(finance_bin, sizeof(finance_bin), "BONFYRE_FINANCE_BINARY", "bonfyre-finance", s->self);
    snprintf(finance_db, sizeof(finance_db), "%s/finance.db", s->home);
    if (!access(finance_bin, X_OK)) {
        char tsv[PATH_MAX], digest[65], cents[32] = "-", title[400];
        FILE *f;
        snprintf(tsv, sizeof(tsv), "%s/.opportunity-%s.tsv", s->home, lead.reference);
        bf_sha256_hex((const unsigned char *)lead.message, strlen(lead.message), digest);
        if (lead.has_amount) snprintf(cents, sizeof(cents), "%lld", (long long)(lead.amount * 100 + 0.5));
        snprintf(title, sizeof(title), "%s for %s", lead.offer[0] ? lead.offer : "Inquiry", lead.name);
        for (char *p = title; *p; p++) if (*p == '\t' || *p == '\n') *p = ' ';
        f = fopen(tsv, "w");
        if (f) {
            fputs("id\tsource_kind\tsource_ref\tsource_digest\tobserved_at\ttitle\tdesired_state\tcompensation_cents\tcurrency\tdeadline\t"
                  "contact_boundary\texpected_compute_cents\texpected_external_cents\thuman_minutes\treuse_points\trisk_points\n", f);
            fprintf(f, "%s\tclassified\tsite://%s/request/%s\t%s\t%s\t%s\tDeliver what the site promised\t%s\t%s\t-\tsite-inbox\t0\t0\t60\t3\t2\n",
                    lead.reference, name, lead.reference, digest, at, title, cents, lead.has_amount ? lead.currency : "USD");
            fclose(f);
            {
                char *const av[] = {finance_bin, "--db", finance_db, "opportunity", "import", "--file", tsv, NULL};
                if (!run(av, out, sizeof(out), NULL) && strstr(out, "imported\t1")) snprintf(opportunity, sizeof(opportunity), "recorded");
                else if (strstr(out, "duplicates\t1")) snprintf(opportunity, sizeof(opportunity), "already recorded");
            }
            unlink(tsv);
        }
    }
    {
        char note[300];
        snprintf(note, sizeof(note), "accepted by the owner; commitment %.200s", commitment_id);
        if (mark_lead(s, name, &lead, "accepted", note)) fputs("site: the request could not be marked accepted in the inbox; the promise stands\n", stderr);
    }
    if (s->json) {
        fputs("{\"schema\":\"bonfyre.site.accept.v1\",\"site\":", stdout); json_text(stdout, name);
        fputs(",\"request\":", stdout); json_text(stdout, lead.reference);
        fputs(",\"commitment\":", stdout); json_text(stdout, commitment_id);
        fputs(",\"promised\":", stdout); json_text(stdout, lead.offer[0] ? lead.offer : "a reply");
        fputs(",\"when\":", stdout); json_text(stdout, when);
        fputs(",\"to\":", stdout); json_text(stdout, lead.contact);
        printf(",\"invoiced\":%s,\"invoice\":", invoiced ? "true" : "false"); json_text(stdout, invoice);
        fputs(",\"finance_opportunity\":", stdout); json_text(stdout, opportunity);
        fputs(",\"cannot_infer\":\"a promise recorded and an invoice raised are not the customer told, the service delivered, or money received\"}\n", stdout);
    } else {
        const char *bold = styled() ? "\033[1m" : "", *dim = styled() ? "\033[2m" : "", *off = styled() ? "\033[0m" : "";
        printf("\n%sAccepted: %s for %s%s\n\n", bold, lead.offer[0] ? lead.offer : "a reply", lead.name, off);
        if (*when) printf("  when        %s\n", when);
        printf("  promise     %s  %s(commitment store; this time cannot be promised again)%s\n", commitment_id, dim, off);
        if (invoiced) printf("  invoice     %.2f %s  %s(Pay, from the offer's rate in the meter)%s\n", lead.amount, lead.currency, dim, off);
        else printf("  invoice     none  %s(%s)%s\n", dim, lead.has_amount ? "the meter or Pay did not raise one; the promise and the books entry stand" : "the offer states no amount", off);
        printf("  books       opportunity %s in Finance\n", opportunity);
        printf("  reach them  %s\n\n", lead.contact);
        printf("  %sNothing was sent to them. When it is paid: bonfyre site paid %s --request %s --reference BANK-REF%s\n", dim, name, lead.reference, off);
        printf("  %sWhen it is delivered: bonfyre site done %s --request %s%s\n\n", dim, name, lead.reference, off);
    }
    return 0;
}

/* A payment the owner received, recorded where payments are recorded: Pay,
 * against the invoice raised at acceptance, under the bank's own reference. */
static int site_paid(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *reference = arg_value(argc, argv, "--request"), *bank = arg_value(argc, argv, "--reference");
    const char *method = arg_value(argc, argv, "--method");
    char pay_bin[PATH_MAX], pay_db[PATH_MAX], customer_key[260], out[4096], invoice_id[32] = "", cents[32] = "";
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    if (!key || !slug_ok(key) || !reference || !bank || strlen(bank) < 3) { fputs("site paid NAME --request REFERENCE --reference BANK-REF [--method wire]\n", stderr); return 2; }
    find_binary(pay_bin, sizeof(pay_bin), "BONFYRE_PAY_BINARY", "bonfyre-pay", s->self);
    snprintf(pay_db, sizeof(pay_db), "%s/pay.db", s->home);
    snprintf(customer_key, sizeof(customer_key), "%s:customer:%s", key, reference);
    if (sqlite3_open_v2(pay_db, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT id, amount_cents FROM invoices WHERE api_key=?1 ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, customer_key, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(invoice_id, sizeof(invoice_id), "%lld", sqlite3_column_int64(st, 0));
            snprintf(cents, sizeof(cents), "%lld", sqlite3_column_int64(st, 1));
        }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (!invoice_id[0]) { fprintf(stderr, "site: no invoice was raised for request %s (accept it first, and the offer must state an amount)\n", reference); return 7; }
    {
        char *const av[] = {pay_bin, "--db", pay_db, "pay", "--invoice-id", invoice_id, "--amount", cents, "--method", (char *)(method ? method : "transfer"),
                            "--reference", (char *)bank, NULL};
        if (run(av, out, sizeof(out), NULL)) { fprintf(stderr, "site: Pay refused the payment: %.300s\n", out); return 5; }
        if (strstr(out, "already paid")) {
            fprintf(stderr, "site: Pay already holds a payment for that invoice; nothing was recorded twice\n");
            return 16;
        }
    }
    fputs("{\"schema\":\"bonfyre.site.paid.v1\",\"site\":", stdout); json_text(stdout, key);
    fputs(",\"request\":", stdout); json_text(stdout, reference);
    printf(",\"invoice\":%s,\"amount_cents\":%s,\"reference\":", invoice_id, cents); json_text(stdout, bank);
    fputs(",\"recorded_in\":\"Pay\",\"cannot_infer\":\"the reference is the owner's statement of a payment; this does not see a bank\"}\n", stdout);
    return 0;
}

/* The promise kept: the commitment is discharged, which is the only thing
 * the commitment store counts as delivery. */
static int site_done(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *reference = arg_value(argc, argv, "--request"), *detail = arg_value(argc, argv, "--note");
    char name[128], host[256], profile[128], subject[160], commitment_id[BF_COMMITMENT_ID_MAX], commitments_db[PATH_MAX], at[32], actor[160], note[400] = "";
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    BfCommitment *commitments = NULL;
    BfCommitmentStatus status;
    Lead lead;
    time_t now = time(NULL);
    int delivered = 0;
    if (!key || !reference) { fputs("site done NAME --request REFERENCE [--note \"...\"]\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject)) ||
        !read_lead(db, name, reference, &lead)) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: %s has no request %s\n", key, reference);
        return 7;
    }
    /* The commitment's id was written on the request when it was accepted. */
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(note,'') FROM service_lead WHERE id=?1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, lead.lead_row);
        if (sqlite3_step(st) == SQLITE_ROW) snprintf(note, sizeof(note), "%s", (const char *)sqlite3_column_text(st, 0));
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    {
        const char *mark = strstr(note, "commitment ");
        if (!mark) { fprintf(stderr, "site: request %s was never accepted, so there is no promise to discharge\n", reference); return 7; }
        snprintf(commitment_id, sizeof(commitment_id), "%s", mark + 11);
    }
    if (find_share(s)) return 3;
    snprintf(commitments_db, sizeof(commitments_db), "%s/commitments.db", s->home);
    snprintf(actor, sizeof(actor), "institution:%s", name);
    strftime(at, sizeof(at), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
    if (bf_commitment_open(commitments_db, &commitments, stderr)) return 4;
    status = bf_commitment_discharge(commitments, commitment_id, actor, detail ? detail : "delivered as promised", at);
    if (status == BF_COMMITMENT_OK) bf_commitment_delivered(commitments, commitment_id, &delivered);
    bf_commitment_close(commitments);
    if (status != BF_COMMITMENT_OK || !delivered) { fprintf(stderr, "site: the commitment store did not discharge %s (%d)\n", commitment_id, (int)status); return 5; }
    {
        char done_note[400];
        snprintf(done_note, sizeof(done_note), "delivered; commitment %.200s", commitment_id);
        mark_lead(s, name, &lead, "done", done_note);
    }
    fputs("{\"schema\":\"bonfyre.site.done.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"request\":", stdout); json_text(stdout, reference);
    fputs(",\"commitment\":", stdout); json_text(stdout, commitment_id);
    fputs(",\"state\":\"discharged\",\"delivered\":true}\n", stdout);
    return 0;
}

/* A site moved to a name of its own: the record takes the new host, the
 * resolver learns it, and the page is read back under that name on this
 * machine.  Pointing the name here in DNS is the domain organ's, which plans
 * the change and applies it only when told to. */
static int site_domain(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *domain = arg_value(argc, argv, "--to"), *ip = arg_value(argc, argv, "--ip");
    const char *secrets = arg_value(argc, argv, "--secrets");
    char name[128], host[256], profile[128], subject[160], url[400], presence[160], evidence[200], page[PATH_MAX], code[16] = "", out[8192], row[32];
    char domain_bin[PATH_MAX], plan[2000] = "";
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    long long presentation = 0;
    int rc, served;
    if (!key || !domain || !host_ok(domain) || !strchr(domain, '.')) { fputs("site domain NAME --to example.com [--ip A.B.C.D --secrets FILE [--apply]]\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    if (count_where(db, "site_presentation", "host", domain) > 0 && strcmp(host, domain)) { sqlite3_close(db); fprintf(stderr, "site: another site already answers at %s\n", domain); return 7; }
    if (sqlite3_prepare_v2(db, "SELECT id FROM site_presentation WHERE site_id=?1 ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) presentation = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    if (!presentation || find_share(s)) return 3;
    snprintf(row, sizeof(row), "%lld", presentation);
    {
        char *json = json_object("host", domain, NULL);
        char *const av[] = {s->cms_bin, "entry", "update", "site_presentation", row, json, "--db", s->cms_db, "--schemas", s->schemas, NULL};
        rc = json ? run(av, out, sizeof(out), NULL) : 5;
        free(json);
        if (rc) { fputs("site: the content store did not take the new name\n", stderr); return 5; }
    }
    snprintf(url, sizeof(url), "https://%s/", domain);
    snprintf(presence, sizeof(presence), "presence:%s:%s", name, domain);
    snprintf(evidence, sizeof(evidence), "receipt:site-domain:%s:%s", name, domain);
    if (resolver(s, out, sizeof(out), "domain", "--host", domain, "--territory", "independent", "--scope", "public", "--world", "live",
                 "--repr", "HTML", "--evidence", evidence, NULL) ||
        resolver(s, out, sizeof(out), "address", "--subject", subject, "--kind", "web_url", "--address", url, "--evidence", evidence, NULL) ||
        resolver(s, out, sizeof(out), "presence", "--id", presence, "--subject", subject, "--territory", "independent", "--scope", "public",
                 "--world", "live", "--repr", "HTML", "--evidence", evidence, NULL)) {
        fprintf(stderr, "site: the resolver refused an address fact for %s\n", domain);
        return 8;
    }
    if ((rc = ensure_server(s))) return rc;
    snprintf(page, sizeof(page), "%s/.domain-readback.html", s->home);
    http_get(s, domain, "/", page, code, sizeof(code));
    served = !strncmp(code, "200", 3) && file_contains(page, "site-program-masthead");
    unlink(page);
    /* DNS is the domain organ's: it plans, and applies only on --apply. */
    find_binary(domain_bin, sizeof(domain_bin), "BONFYRE_DOMAIN_BINARY", "bonfyre-domain", s->self);
    if (ip && secrets && !access(domain_bin, X_OK)) {
        char *const plan_av[] = {domain_bin, "plan", "--ip", (char *)ip, "--only", (char *)domain, "--secrets", (char *)secrets, NULL};
        char *const apply_av[] = {domain_bin, "apply", "--ip", (char *)ip, "--only", (char *)domain, "--secrets", (char *)secrets, "--apply", NULL};
        size_t n = 0;
        run(arg_flag(argc, argv, "--apply") ? apply_av : plan_av, out, sizeof(out), NULL);
        for (const char *p = out; *p && n + 1 < sizeof(plan); p++) plan[n++] = *p;
        plan[n] = '\0';
    }
    fputs("{\"schema\":\"bonfyre.site.domain.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"was\":", stdout); json_text(stdout, host);
    fputs(",\"now\":", stdout); json_text(stdout, domain);
    printf(",\"served_under_new_name_here\":%s,\"dns\":", served ? "true" : "false");
    json_text(stdout, plan[0] ? plan : "not planned: give --ip and --secrets and the domain organ plans the records; add --apply to make them");
    fputs(",\"cannot_infer\":\"answering under the name on this machine is not the name pointing here on the internet\"}\n", stdout);
    return served ? 0 : 9;
}

/* What the site has promised, from the commitment store. */
static int site_promises(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv);
    char commitments_db[PATH_MAX], prefix[160];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    long n = 0;
    if (!key || !slug_ok(key)) { fputs("site promises NAME\n", stderr); return 2; }
    snprintf(commitments_db, sizeof(commitments_db), "%s/commitments.db", s->home);
    snprintf(prefix, sizeof(prefix), "site:%s:%%", key);
    if (sqlite3_open_v2(commitments_db, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        /* Read through the store's own tables' names as it reports them. */
        if (sqlite3_prepare_v2(db, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%commitment%' ORDER BY length(name) LIMIT 1", -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            char sql[400];
            snprintf(sql, sizeof(sql), "SELECT * FROM \"%s\" WHERE commitment_id LIKE ?1", (const char *)sqlite3_column_text(st, 0));
            sqlite3_finalize(st); st = NULL;
            if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, prefix, -1, SQLITE_STATIC);
                while (sqlite3_step(st) == SQLITE_ROW) {
                    const char *id = NULL, *who = NULL, *what = NULL, *terms = NULL, *state = NULL;
                    for (int c = 0; c < sqlite3_column_count(st); c++) {
                        const char *col = sqlite3_column_name(st, c), *val = (const char *)sqlite3_column_text(st, c);
                        if (!strcmp(col, "commitment_id")) id = val;
                        else if (!strcmp(col, "beneficiary")) who = val;
                        else if (!strcmp(col, "resource")) what = val;
                        else if (!strcmp(col, "conditions")) terms = val;
                        else if (!strcmp(col, "state")) state = val;
                    }
                    printf("%s%s\n  %s %s, for %s%s%s\n", n ? "\n" : "", id ? id : "", what ? what : "", terms ? terms : "", who ? who : "",
                           state ? "  — " : "", state ? state : "");
                    n++;
                }
            }
        }
        if (st) sqlite3_finalize(st);
        sqlite3_close(db);
    }
    if (!n) printf("%s has promised nothing yet.\n", key);
    return 0;
}

/* ---------- export ---------- */

static char *slurp(const char *path, long *size) {
    FILE *f = fopen(path, "rb");
    char *data;
    if (!f) return NULL;
    fseek(f, 0, SEEK_END); *size = ftell(f); rewind(f);
    data = malloc((size_t)*size + 1);
    if (!data || fread(data, 1, (size_t)*size, f) != (size_t)*size) { fclose(f); free(data); return NULL; }
    fclose(f);
    data[*size] = '\0';
    return data;
}

static void xml_text(FILE *out, const char *text) {
    for (; text && *text; text++) {
        if (*text == '&') fputs("&amp;", out);
        else if (*text == '<') fputs("&lt;", out);
        else if (*text == '>') fputs("&gt;", out);
        else if ((unsigned char)*text >= 0x20 || *text == '\n' || *text == '\t') fputc(*text, out);
    }
}

/* Plain text as markup another system will accept: one paragraph per line,
 * with the three characters markup reserves written as entities. */
static char *text_as_markup(const char *body) {
    char *buffer = NULL;
    size_t size = 0;
    FILE *out = open_memstream(&buffer, &size);
    if (!out) return NULL;
    for (const char *line = body; line && *line; ) {
        const char *eol = strchr(line, '\n');
        size_t len = eol ? (size_t)(eol - line) : strlen(line);
        if (len) {
            char *piece = strndup(line, len);
            fputs("<p>", out);
            if (piece) xml_text(out, piece);
            free(piece);
            fputs("</p>\n", out);
        }
        line = eol ? eol + 1 : line + len;
    }
    fclose(out);
    return buffer;
}

/* A site's posts in another system's own import format: WordPress's export
 * file (read by its importer) or Frappe documents (read by its data import).
 * The file is then read back by this estate's own reader of that format and
 * the titles and text compared with what was written, so a file that would
 * not survive its own format is caught here.  That is not the other system
 * having accepted it. */
static int site_export_posts(Site *s, const char *name, const char *host, const char *format, const char *out_path) {
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    FILE *out;
    long count = 0;
    int wordpress = !strcmp(format, "wordpress");
    char site_title[256] = "";
    if (!db) return 4;
    if (sqlite3_prepare_v2(db, "SELECT title FROM community_body WHERE community=?1 ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) snprintf(site_title, sizeof(site_title), "%s", (const char *)sqlite3_column_text(st, 0));
        sqlite3_finalize(st); st = NULL;
    }
    if (sqlite3_prepare_v2(db, "SELECT title, body, published_at FROM community_post WHERE community=?1 AND status='published' "
                               "ORDER BY published_at, id", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 4; }
    sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
    out = fopen(out_path, "wb");
    if (!out) { sqlite3_finalize(st); sqlite3_close(db); fprintf(stderr, "site: cannot write %s\n", out_path); return 3; }
    if (wordpress) {
        fputs("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rss version=\"2.0\" xmlns:excerpt=\"http://wordpress.org/export/1.2/excerpt/\" "
              "xmlns:content=\"http://purl.org/rss/1.0/modules/content/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" "
              "xmlns:wp=\"http://wordpress.org/export/1.2/\">\n<channel>\n<title>", out);
        xml_text(out, site_title[0] ? site_title : name);
        fprintf(out, "</title>\n<link>https://%s</link>\n<description></description>\n<language>en</language>\n"
                     "<wp:wxr_version>1.2</wp:wxr_version>\n<wp:base_site_url>https://%s</wp:base_site_url>\n"
                     "<wp:base_blog_url>https://%s</wp:base_blog_url>\n"
                     "<wp:author><wp:author_login><![CDATA[%s]]></wp:author_login><wp:author_display_name><![CDATA[", host, host, host, name);
        fputs(site_title[0] ? site_title : name, out);
        fputs("]]></wp:author_display_name></wp:author>\n", out);
    } else fputs("[\n", out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *title = (const char *)sqlite3_column_text(st, 0), *body = (const char *)sqlite3_column_text(st, 1);
        const char *when = (const char *)sqlite3_column_text(st, 2);
        char stamp[20] = "1970-01-01 00:00:00", *markup = text_as_markup(body ? body : "");
        if (when && strlen(when) >= 19) { snprintf(stamp, sizeof(stamp), "%.19s", when); stamp[10] = ' '; }
        if (!markup) continue;
        count++;
        if (wordpress) {
            fputs("<item>\n<title>", out); xml_text(out, title ? title : "");
            fprintf(out, "</title>\n<dc:creator><![CDATA[%s]]></dc:creator>\n<content:encoded><![CDATA[", name);
            /* The one sequence a CDATA section cannot hold is split across two. */
            for (const char *p = markup; *p; p++) { if (!strncmp(p, "]]>", 3)) { fputs("]]]]><![CDATA[>", out); p += 2; } else fputc(*p, out); }
            fprintf(out, "]]></content:encoded>\n<excerpt:encoded><![CDATA[]]></excerpt:encoded>\n<wp:post_id>%ld</wp:post_id>\n"
                         "<wp:post_date_gmt>%s</wp:post_date_gmt>\n<wp:post_date>%s</wp:post_date>\n<wp:status>publish</wp:status>\n"
                         "<wp:post_type>post</wp:post_type>\n</item>\n", count, stamp, stamp);
        } else {
            if (count > 1) fputs(",\n", out);
            fputs(" {\"doctype\":\"Blog Post\",\"title\":", out); json_text(out, title ? title : "");
            fputs(",\"blog_category\":\"general\",\"blogger\":", out); json_text(out, name);
            fprintf(out, ",\"published\":1,\"published_on\":\"%.10s\",\"content_type\":\"HTML\",\"content_html\":", stamp);
            json_text(out, markup);
            fputc('}', out);
        }
        free(markup);
    }
    fputs(wordpress ? "</channel>\n</rss>\n" : "\n]\n", out);
    fclose(out);
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (!count) { unlink(out_path); fprintf(stderr, "site: %s has no published posts to export\n", name); return 11; }
    fputs("{\"schema\":\"bonfyre.site.export.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"format\":", stdout); json_text(stdout, wordpress ? "wordpress export file (WXR 1.2)" : "frappe documents (Blog Post)");
    fputs(",\"file\":", stdout); json_text(stdout, out_path);
    printf(",\"posts\":%ld,\"cannot_infer\":\"a file in another system's import format is not that system having imported it\"}\n", count);
    return 0;
}

/* One file that can be attached to a message: the page as served, with each
 * stylesheet written into it and its scripts left out.  It asks nothing of the
 * network when opened. */
static int site_export(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *out_path = arg_value(argc, argv, "--out");
    char name[128], host[256], profile[128], subject[160], page[PATH_MAX], code[64] = "";
    sqlite3 *db = open_cms(s);
    char *html;
    long size = 0, sheets = 0, missing = 0, bytes = 0;
    FILE *out;
    int rc;
    if (!key || !out_path) { fputs("site export NAME --out FILE.html\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    sqlite3_close(db);
    {
        const char *format = arg_value(argc, argv, "--format");
        if (format && strcmp(format, "html")) {
            if (strcmp(format, "wordpress") && strcmp(format, "frappe")) { fputs("site export: --format is html, wordpress or frappe\n", stderr); return 2; }
            return site_export_posts(s, name, host, format, out_path);
        }
    }
    if (find_share(s)) { fputs("site: page assets were not found\n", stderr); return 3; }
    if ((rc = ensure_server(s))) return rc;
    snprintf(page, sizeof(page), "%s/.export-%s.html", s->home, name);
    http_get(s, host, "/", page, code, sizeof(code));
    html = slurp(page, &size);
    unlink(page);
    if (strncmp(code, "200", 3) || !html || !strstr(html, "site-program-masthead")) {
        fprintf(stderr, "site: %s did not serve its page (HTTP %.3s)\n", host, code);
        free(html);
        return 9;
    }
    out = fopen(out_path, "wb");
    if (!out) { fprintf(stderr, "site: cannot write %s\n", out_path); free(html); return 3; }
    for (const char *p = html; *p; ) {
        if (!strncmp(p, "<link rel=\"stylesheet\" href=\"/", 30)) {
            const char *start = p + 29, *end = strchr(start, '"'), *close = end ? strchr(end, '>') : NULL;
            char rel[256], path[PATH_MAX];
            long css_size = 0;
            char *css;
            if (!close || (size_t)(end - start) >= sizeof(rel)) { fputc(*p++, out); continue; }
            snprintf(rel, sizeof(rel), "%.*s", (int)(end - start), start);
            /* A version after the name is for caches, not part of the file. */
            { char *version = strchr(rel, '?'); if (version) *version = 0; }
            /* The server answers /assets/NAME from the asset root itself. */
            snprintf(path, sizeof(path), "%s%s", s->assets, !strncmp(rel, "/assets/", 8) ? rel + 7 : rel);
            css = strstr(rel, "..") ? NULL : slurp(path, &css_size);
            if (css) { fprintf(out, "<style data-from=\"%s\">", rel); fwrite(css, 1, (size_t)css_size, out); fputs("</style>", out); sheets++; bytes += css_size; free(css); }
            else missing++;
            p = close + 1;
            continue;
        }
        if (!strncmp(p, "<script", 7)) {
            const char *close = strstr(p, "</script>");
            if (close) { p = close + 9; continue; }
        }
        /* A link to the live site's server card means nothing in a file. */
        if (!strncmp(p, "<link rel=\"mcp\"", 16)) { const char *close = strchr(p, '>'); if (close) { p = close + 1; continue; } }
        fputc(*p++, out);
    }
    fclose(out);
    bytes += size;
    free(html);
    fputs("{\"schema\":\"bonfyre.site.export.v1\",\"site\":", stdout); json_text(stdout, name);
    fputs(",\"file\":", stdout); json_text(stdout, out_path);
    printf(",\"stylesheets_inlined\":%ld,\"stylesheets_missing\":%ld,\"bytes\":%ld,\"scripts\":\"removed\","
           "\"cannot_infer\":\"a file exported now is the page as it was when exported\"}\n", sheets, missing, bytes);
    return missing ? 9 : 0;
}

/* ---------- set ---------- */

/* One field of one record, changed through the content store.  The server
 * reads its records on every request, so the next page load shows it. */
static int set_field(Site *s, sqlite3 *db, const char *type, const char *key_col, const char *key, const char *field, const char *value) {
    char sql[200], row[32], out[4096];
    sqlite3_stmt *st = NULL;
    long long id = 0;
    char *json;
    int rc;
    snprintf(sql, sizeof(sql), "SELECT id FROM %s WHERE %s=?1 ORDER BY id DESC LIMIT 1", type, key_col);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, key, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    if (!id) return 7;
    snprintf(row, sizeof(row), "%lld", id);
    json = json_object(field, value, NULL);
    if (!json) return 5;
    { char *const av[] = {s->cms_bin, "entry", "update", (char *)type, row, json, "--db", s->cms_db, "--schemas", s->schemas, NULL};
      rc = run(av, out, sizeof(out), NULL); }
    free(json);
    return rc ? 5 : 0;
}

/* Apply one presentation patch through CMS, after validating the whole patch. */
static int site_set_presentation(Site *s, const char *key, const char *path) {
    char name[128], host[256], profile[128], subject[160], row[32], out[4096];
    char expected[160];
    sqlite3 *db = open_cms(s); sqlite3_stmt *st = NULL;
    struct stat info; long size = 0; char *patch = NULL; const char *why = NULL;
    if (!key || !db || !site_lookup(db,key,name,sizeof(name),host,sizeof(host),profile,sizeof(profile),subject,sizeof(subject))) {
        if(db) sqlite3_close(db); fputs("site: customer not found\n",stderr); return 7;
    }
    if (stat(path,&info) || !S_ISREG(info.st_mode) || info.st_size<=0 || info.st_size>16384 || !(patch=slurp(path,&size)) ||
        strlen(patch)!=(size_t)size || !bf_site_presentation_validate(db,patch,&why)) {
        fprintf(stderr,"site: invalid presentation: %s\n",why ? why : "expected a JSON file of at most 16 KB");
        free(patch);sqlite3_close(db);return 2;
    }
    snprintf(expected,sizeof(expected),"ghost:%s",name);
    if(strcmp(profile,name) && strcmp(profile,expected)) {
        free(patch);sqlite3_close(db);fputs("site: presentation must belong to this customer\n",stderr);return 2;
    }
    if(sqlite3_prepare_v2(db,"SELECT id FROM presentation_template WHERE template=?1 AND status='published' ORDER BY id DESC LIMIT 1",-1,&st,NULL)!=SQLITE_OK) {
        free(patch);sqlite3_close(db);return 5;
    }
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW) {sqlite3_finalize(st);sqlite3_close(db);free(patch);return 7;}
    snprintf(row,sizeof(row),"%lld",sqlite3_column_int64(st,0));sqlite3_finalize(st);sqlite3_close(db);
    if(find_share(s)) {free(patch);return 3;}
    char *const av[]={s->cms_bin,"entry","update","presentation_template",row,patch,"--db",s->cms_db,"--schemas",s->schemas,NULL};
    int rc=run(av,out,sizeof(out),NULL);free(patch);
    if(rc) {fputs("site: presentation update failed\n",stderr);return 5;}
    return site_readback(s,name,host,NULL,0);
}

/* A site's name, its line and its colour, changed while it runs. */
static int site_set(Site *s, int argc, char **argv) {
    const char *key = arg_positional(argc, argv), *title = arg_value(argc, argv, "--title"), *tagline = arg_value(argc, argv, "--tagline");
    const char *accent = arg_value(argc, argv, "--accent");
    const char *presentation = arg_value(argc, argv, "--presentation");
    if(presentation) {
        if(title || tagline || accent) {fputs("site: apply --presentation separately from title, tagline and accent\n",stderr);return 2;}
        return site_set_presentation(s,key,presentation);
    }
    const char *footnote = arg_flag(argc, argv, "--no-footnote") ? "" : arg_value(argc, argv, "--footnote"), *foot_label = arg_value(argc, argv, "--footnote-label");
    const char *foot_link = arg_value(argc, argv, "--footnote-link");
    char name[128], host[256], profile[128], subject[160], tokens[1024] = "";
    sqlite3 *db = open_cms(s);
    sqlite3_stmt *st = NULL;
    int rc = 0, changed = 0;
    if (!key || (!title && !tagline && !accent && !footnote)) { fputs("site set NAME [--title T] [--tagline T] [--accent #hex] [--footnote T [--footnote-label L --footnote-link https://...] | --no-footnote] | --presentation FILE.json\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (footnote && ((foot_label == NULL) != (foot_link == NULL) || (foot_link && strncmp(foot_link, "https://", 8)))) {
        fputs("site set: a footnote's link needs both --footnote-label and an https:// --footnote-link\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (accent && !color_ok(accent)) { fputs("site set: --accent is a colour such as #b5df32\n", stderr); if (db) sqlite3_close(db); return 2; }
    if (!db || !site_lookup(db, key, name, sizeof(name), host, sizeof(host), profile, sizeof(profile), subject, sizeof(subject))) {
        if (db) sqlite3_close(db);
        fprintf(stderr, "site: no site named or answering at %s here\n", key);
        return 7;
    }
    if (accent && sqlite3_prepare_v2(db, "SELECT json_set(layout_tokens,'$.accent',?2) FROM presentation_template WHERE template=?1 ORDER BY id DESC LIMIT 1",
                                     -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, accent, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(tokens, sizeof(tokens), "%s", (const char *)sqlite3_column_text(st, 0));
        sqlite3_finalize(st);
    }
    if (find_share(s)) { sqlite3_close(db); return 3; }
    if (title && !(rc = set_field(s, db, "community_body", "community", name, "title", title)) &&
        !(rc = set_field(s, db, "presentation_template", "template", name, "reference_name", title))) changed++;
    if (!rc && tagline && !(rc = set_field(s, db, "community_body", "community", name, "description", tagline))) changed++;
    if (!rc && accent) { rc = tokens[0] ? set_field(s, db, "presentation_template", "template", name, "=layout_tokens", tokens) : 5; if (!rc) changed++; }
    /* The line at the foot of every page: who made the site, or that the
     * business is an example.  The newest one stands; an empty one clears it. */
    if (!rc && footnote) {
        char coordinate[256];
        char *json;
        /* Two changes in one second are still two records. */
        snprintf(coordinate, sizeof(coordinate), "%s:foot:%ld-%ld", name, (long)time(NULL), (long)getpid());
        /* The store keeps no empty record, so taking the line away is a
         * record too: the newest one says there is none. */
        json = json_object("coordinate", coordinate, "page", "/_foot", "slot", "foot", "audience", "anonymous", "kind", *footnote ? "note" : "cleared",
                           "=position", "0", "body", *footnote ? footnote : "No footnote.", "label", foot_label ? foot_label : "", "href", foot_link ? foot_link : "",
                           "status", "published", NULL);
        rc = json ? cms_create_in(s, name, "site_fragment", json) : 5;
        free(json);
        if (!rc) changed++;
    }
    sqlite3_close(db);
    if (rc) { fputs("site: the content store did not take the change\n", stderr); return rc; }
    fprintf(stderr, "site: %d setting%s changed on %s; the next page load shows it\n", changed, changed == 1 ? "" : "s", name);
    return site_readback(s, name, host, title, 0);
}

/* ---------- import ---------- */

static int add_post(Site *s, const char *site, long ordinal, const char *title, const char *raw_body, const char *when) {
    char post[128], subject[96];
    char *json, *body = malloc(strlen(raw_body) + 1), *w = body;
    int rc;
    if (!body) return 3;
    /* Ingest marks a source's headings with "# "; a post is plain text, so a
     * heading is kept as its own line without the mark. */
    for (const char *r = raw_body; *r; ) {
        if ((r == raw_body || r[-1] == '\n') && r[0] == '#') {
            const char *q = r;
            while (*q == '#') q++;
            if (*q == ' ') { r = q + 1; continue; }
        }
        *w++ = *r++;
    }
    *w = '\0';
    snprintf(post, sizeof(post), "%s:imported-%ld", site, ordinal);
    snprintf(subject, sizeof(subject), "institution:%s", site);
    json = json_object("post", post, "community", site, "author_subject", subject, "title", title, "body", body,
        "published_at", when, "visibility", "public", "moderation_state", "accepted",
        "cannot_infer", "an imported post is a copy of its source as read at import", "status", "published", NULL);
    rc = json ? cms_create(s, "community_post", json) : 5;
    free(json);
    free(body);
    return rc;
}

typedef struct {
    Site *s; const NewSite *n; const char *ingest, *work;
    long imported, limit; int created;
} Import;

/* One post that arrives as markup (WordPress, a WordPress export file, a
 * Frappe document).  Its markup is read by Ingest, like any page; the site is
 * made when the first post is in hand, so a source with nothing in it leaves
 * no empty site behind.  date is "YYYY-MM-DD HH:MM:SS" or ISO, or NULL. */
static int import_markup_post(Import *im, const char *post_title, const char *content, const char *date) {
    char page[PATH_MAX], dir[PATH_MAX], text_path[PATH_MAX], when[40], stamp[20] = "1970-01-01T00:00:00";
    long text_size = 0;
    char *text, *body, clean_title[300];
    FILE *f;
    int rc = 0;
    if (!post_title || !content || im->imported >= im->limit) return 0;
    snprintf(page, sizeof(page), "%s/post-%ld.html", im->work, im->imported);
    snprintf(dir, sizeof(dir), "%s/post-%ld", im->work, im->imported);
    f = fopen(page, "w");
    if (!f) return 3;
    fprintf(f, "<html><body><p>%s</p>\n<hr>\n%s</body></html>", post_title, content);
    fclose(f);
    { char *const av[] = {(char *)im->ingest, page, dir, NULL}; if (run(av, NULL, 0, NULL)) return 0; }
    snprintf(text_path, sizeof(text_path), "%s/normalized.txt", dir);
    text = slurp(text_path, &text_size);
    if (!text) return 0;
    /* First line is the title as text; the rest is the body. */
    body = strchr(text, '\n');
    snprintf(clean_title, sizeof(clean_title), "%.*s", body ? (int)(body - text) : (int)strlen(text), text);
    while (body && isspace((unsigned char)*body)) body++;
    if (date && strlen(date) >= 19) { snprintf(stamp, sizeof(stamp), "%.19s", date); stamp[10] = 'T'; }
    snprintf(when, sizeof(when), "%sZ", stamp);
    if (body && *body) {
        if (!im->created && !(rc = site_create(im->s, im->n))) im->created = 1;
        if (!rc && !(rc = add_post(im->s, im->n->name, im->imported, clean_title, body, when))) im->imported++;
    }
    free(text);
    return rc;
}

/* The text between an opening tag and its closing tag, with a CDATA wrapper
 * removed.  Returns a new string, and where the search may continue. */
static char *xml_element(const char *from, const char *limit, const char *tag, const char **after) {
    char open_tag[96], close_tag[96];
    const char *start, *end;
    char *value;
    size_t len;
    snprintf(open_tag, sizeof(open_tag), "<%s>", tag);
    snprintf(close_tag, sizeof(close_tag), "</%s>", tag);
    start = strstr(from, open_tag);
    if (!start || (limit && start >= limit)) return NULL;
    start += strlen(open_tag);
    end = strstr(start, close_tag);
    if (!end || (limit && end > limit)) return NULL;
    if (after) *after = end + strlen(close_tag);
    if (!strncmp(start, "<![CDATA[", 9) && end - start >= 12 && !strncmp(end - 3, "]]>", 3)) { start += 9; end -= 3; }
    len = (size_t)(end - start);
    value = malloc(len + 1);
    if (!value) return NULL;
    memcpy(value, start, len);
    value[len] = '\0';
    return value;
}

/* Brings a site's writing in.  A WordPress address is read through its own
 * public posts interface; anything else Ingest can read (a page, a saved
 * file, a PDF) is cut into sections by Paragraph and each becomes a post. */
static int site_import(Site *s, int argc, char **argv) {
    const char *source = arg_positional(argc, argv), *name = arg_value(argc, argv, "--as"), *limit_arg = arg_value(argc, argv, "--limit");
    char ingest[PATH_MAX], paragraph[PATH_MAX], work[PATH_MAX], host[256], title[200], file[PATH_MAX], code[64];
    NewSite n = {0};
    sqlite3 *mem = NULL;
    sqlite3_stmt *st = NULL;
    char *data = NULL;
    long size = 0, imported = 0, limit = limit_arg ? atol(limit_arg) : 20;
    const char *kind = "document";
    int rc, is_url, created = 0;
    Import im = {0};
    if (!source || !slug_ok(name)) { fputs("site import SOURCE --as NAME [--limit N]\n  SOURCE is a WordPress site address or export file, Frappe documents, any page address, or a file\n", stderr); return 2; }
    if (limit < 1 || limit > 100) { fputs("site import: --limit is 1..100\n", stderr); return 2; }
    is_url = !strncmp(source, "http://", 7) || !strncmp(source, "https://", 8);
    find_binary(ingest, sizeof(ingest), "BONFYRE_INGEST_BINARY", "bonfyre-ingest", s->self);
    find_binary(paragraph, sizeof(paragraph), "BONFYRE_PARAGRAPH_BINARY", "bonfyre-paragraph", s->self);
    if ((rc = ensure_store(s))) return rc;
    snprintf(work, sizeof(work), "%s/import-%s", s->home, name);
    { char *const av[] = {"rm", "-rf", work, NULL}; run(av, NULL, 0, NULL); }
    if (mkdirs(work)) return 3;
    snprintf(host, sizeof(host), "%s.localhost", name);
    snprintf(title, sizeof(title), "%s", name);
    title[0] = (char)toupper((unsigned char)title[0]);
    n.name = name; n.title = arg_value(argc, argv, "--title") ? arg_value(argc, argv, "--title") : title;
    n.tagline = arg_value(argc, argv, "--tagline") ? arg_value(argc, argv, "--tagline") : "";
    n.host = host; n.accent = arg_value(argc, argv, "--accent") ? arg_value(argc, argv, "--accent") : "#b5df32";
    if (!color_ok(n.accent)) { fputs("site import: --accent is a colour such as #b5df32\n", stderr); return 2; }
    if (sqlite3_open(":memory:", &mem) != SQLITE_OK) return 4;

    if (is_url) {
        /* WordPress publishes its posts as data at a fixed path. */
        char api[1024], per_page[16];
        size_t len = strlen(source);
        snprintf(per_page, sizeof(per_page), "%ld", limit);
        snprintf(api, sizeof(api), "%.*s/wp-json/wp/v2/posts?per_page=%s&_fields=title,content,date_gmt,link",
                 (int)(len && source[len - 1] == '/' ? len - 1 : len), source, per_page);
        snprintf(file, sizeof(file), "%s/wordpress-posts.json", work);
        char *const av[] = {"curl", "-sS", "-L", "--fail", "-m", "60", "-A", "Mozilla/5.0 (compatible; bonfyre-site-import)",
                            "-o", file, "-w", "%{content_type}", api, NULL};
        code[0] = '\0';
        if (!run(av, code, sizeof(code), NULL) && strstr(code, "json") && (data = slurp(file, &size)) && data[0] == '[') kind = "wordpress";
    }

    im.s = s; im.n = &n; im.ingest = ingest; im.work = work; im.limit = limit;
    if (!is_url && !data && (data = slurp(source, &size))) {
        /* A file may be another system's own export. */
        if (strstr(data, "<wp:wxr_version>")) kind = "wordpress-export";
        else if (data[0] == '[' && strstr(data, "\"doctype\"")) kind = "frappe-documents";
        else { free(data); data = NULL; }
    }

    if (!strcmp(kind, "wordpress")) {
        if (sqlite3_prepare_v2(mem, "SELECT json_extract(value,'$.title.rendered'), json_extract(value,'$.content.rendered'), "
                                    "json_extract(value,'$.date_gmt') FROM json_each(?1)", -1, &st, NULL) != SQLITE_OK) { rc = 4; goto done; }
        sqlite3_bind_text(st, 1, data, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW)
            if ((rc = import_markup_post(&im, (const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1),
                                         (const char *)sqlite3_column_text(st, 2)))) goto done;
        imported = im.imported; created = im.created;
    } else if (!strcmp(kind, "wordpress-export")) {
        /* WordPress's own export file: one item per post or page. */
        for (const char *item = strstr(data, "<item>"); item; item = strstr(item + 6, "<item>")) {
            const char *item_end = strstr(item, "</item>");
            char *post_title, *content, *date, *status, *type;
            if (!item_end) break;
            post_title = xml_element(item, item_end, "title", NULL);
            content = xml_element(item, item_end, "content:encoded", NULL);
            date = xml_element(item, item_end, "wp:post_date_gmt", NULL);
            status = xml_element(item, item_end, "wp:status", NULL);
            type = xml_element(item, item_end, "wp:post_type", NULL);
            rc = 0;
            if (status && !strcmp(status, "publish") && type && (!strcmp(type, "post") || !strcmp(type, "page")))
                rc = import_markup_post(&im, post_title, content, date);
            free(post_title); free(content); free(date); free(status); free(type);
            if (rc) goto done;
        }
        imported = im.imported; created = im.created;
    } else if (!strcmp(kind, "frappe-documents")) {
        /* Frappe documents as its own export writes them: blog posts and web pages. */
        if (sqlite3_prepare_v2(mem,
                "SELECT json_extract(value,'$.title'), coalesce(json_extract(value,'$.content_html'), json_extract(value,'$.content'), "
                "json_extract(value,'$.main_section_html'), json_extract(value,'$.main_section'), json_extract(value,'$.content_md')), "
                "coalesce(json_extract(value,'$.published_on') || ' 00:00:00', json_extract(value,'$.creation')) "
                "FROM json_each(?1) WHERE json_extract(value,'$.doctype') IN ('Blog Post','Web Page') "
                "AND coalesce(json_extract(value,'$.published'),1) != 0", -1, &st, NULL) != SQLITE_OK) { rc = 4; goto done; }
        sqlite3_bind_text(st, 1, data, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW)
            if ((rc = import_markup_post(&im, (const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1),
                                         (const char *)sqlite3_column_text(st, 2)))) goto done;
        imported = im.imported; created = im.created;
    } else {
        char dir[PATH_MAX], text_path[PATH_MAX], passages[PATH_MAX], when[32];
        time_t now = time(NULL);
        strftime(when, sizeof(when), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
        snprintf(dir, sizeof(dir), "%s/source", work);
        { char *const av[] = {ingest, (char *)source, dir, NULL};
          if (run(av, NULL, 0, NULL)) { fprintf(stderr, "site: Ingest could not read %s\n", source); rc = 11; goto done; } }
        snprintf(text_path, sizeof(text_path), "%s/normalized.txt", dir);
        if (!is_file(text_path)) snprintf(text_path, sizeof(text_path), "%s/extracted.txt", dir);
        if (!is_file(text_path)) {
            /* Ingest read a recording: the words come from Transcribe, tidied
             * by TranscriptClean, and from there it is a document like any other. */
            char audio[PATH_MAX], heard[PATH_MAX], raw[PATH_MAX], transcribe[PATH_MAX], clean[PATH_MAX];
            snprintf(audio, sizeof(audio), "%s/normalized.wav", dir);
            if (is_file(audio)) {
                find_binary(transcribe, sizeof(transcribe), "BONFYRE_TRANSCRIBE_BINARY", "bonfyre-transcribe", s->self);
                find_binary(clean, sizeof(clean), "BONFYRE_TRANSCRIPT_CLEAN_BINARY", "bonfyre-transcript-clean", s->self);
                snprintf(heard, sizeof(heard), "%s/heard", work);
                snprintf(raw, sizeof(raw), "%s/transcript.txt", heard);
                { char *const av[] = {transcribe, audio, heard, NULL};
                  if (run(av, NULL, 0, NULL) || !is_file(raw)) { fprintf(stderr, "site: Transcribe could not hear %s\n", source); rc = 11; goto done; } }
                snprintf(text_path, sizeof(text_path), "%s/transcript.clean.txt", heard);
                { char *const av[] = {clean, "--transcript", raw, "--out", text_path, NULL};
                  if (run(av, NULL, 0, NULL) || !is_file(text_path)) snprintf(text_path, sizeof(text_path), "%s", raw); }
                kind = "recording";
            }
        }
        snprintf(passages, sizeof(passages), "%s/passages.jsonl", work);
        { char *const av[] = {paragraph, "--input", text_path, "--passages", passages, "--source-id", (char *)name, NULL};
          if (run(av, NULL, 0, NULL)) { fprintf(stderr, "site: Paragraph could not cut %s\n", text_path); rc = 11; goto done; } }
        free(data);
        data = slurp(passages, &size);
        if (!data || !size) { fprintf(stderr, "site: nothing readable in %s\n", source); rc = 11; goto done; }
        if (sqlite3_prepare_v2(mem, "SELECT json_extract(?1,'$.heading'), json_extract(?1,'$.text')", -1, &st, NULL) != SQLITE_OK) { rc = 4; goto done; }
        for (char *line = data, *next; line && *line && imported < limit; line = next) {
            const char *heading, *text;
            char fallback[80];
            next = strchr(line, '\n');
            if (next) *next++ = '\0';
            sqlite3_reset(st);
            sqlite3_bind_text(st, 1, line, -1, SQLITE_STATIC);
            if (sqlite3_step(st) != SQLITE_ROW) continue;
            heading = (const char *)sqlite3_column_text(st, 0);
            text = (const char *)sqlite3_column_text(st, 1);
            if (!text || strlen(text) < 40) continue;
            if ((!heading || !*heading) && !strcmp(kind, "recording") && !imported) {
                /* A recording has no headings; its file is what it was called. */
                const char *base = strrchr(source, '/'), *dot;
                size_t k = 0;
                base = base ? base + 1 : source;
                dot = strrchr(base, '.');
                for (const char *c = base; *c && c != dot && k + 1 < sizeof(fallback); c++)
                    fallback[k++] = (*c == '-' || *c == '_') ? ' ' : *c;
                fallback[k] = '\0';
                fallback[0] = (char)toupper((unsigned char)fallback[0]);
                heading = fallback;
            } else if (!heading || !*heading) {
                size_t k = 0;
                while (text[k] && text[k] != '\n' && text[k] != '.' && k < 60) k++;
                snprintf(fallback, sizeof(fallback), "%.*s", (int)k, text);
                heading = fallback;
            } else if (!strncmp(text, heading, strlen(heading))) {
                text += strlen(heading);
                while (isspace((unsigned char)*text)) text++;
            }
            if (!*text) continue;
            if (!created) { if ((rc = site_create(s, &n))) goto done; created = 1; }
            if ((rc = add_post(s, name, imported, heading, text, when))) goto done;
            imported++;
        }
    }
    if (!imported) { fprintf(stderr, "site: %s held nothing to import\n", source); rc = 11; goto done; }
    fprintf(stderr, "site: imported %ld post%s from %s as a %s source\n", imported, imported == 1 ? "" : "s", source, kind);
    rc = site_readback(s, name, host, NULL, !arg_flag(argc, argv, "--no-open"));
done:
    if (st) sqlite3_finalize(st);
    if (mem) sqlite3_close(mem);
    free(data);
    return rc;
}

/* ---------- serve / stop ---------- */

static int site_stop(Site *s) {
    char pid_path[PATH_MAX];
    FILE *f;
    int pid = 0;
    snprintf(pid_path, sizeof(pid_path), "%s/serve.pid", s->home);
    f = fopen(pid_path, "r");
    if (!f || fscanf(f, "%d", &pid) != 1 || pid < 2) { if (f) fclose(f); puts("{\"stopped\":false,\"reason\":\"no server was started from this home\"}"); return 0; }
    fclose(f);
    kill(pid, SIGTERM);
    unlink(pid_path);
    puts("{\"stopped\":true}");
    return 0;
}

static void site_usage(void) {
    fputs(
        "bonfyre site — make, find, move and hand over sites\n\n"
        "  bonfyre site new NAME [--title T] [--tagline T] [--accent #hex] [--host H] [--no-open]\n"
        "        one command: the site's records, its address, a running server, the page in a browser\n"
        "  bonfyre site set NAME [--title T] [--tagline T] [--accent #hex] [--footnote T [--footnote-label L --footnote-link URL] | --no-footnote] | --presentation FILE.json   change a running site; the next page load shows it\n"
        "  bonfyre site list [--json]\n"
        "  bonfyre site show NAME|HOST [--json]\n"
        "  bonfyre site search TEXT [--top N] [--json]      by meaning and by words, across every site\n"
        "  bonfyre site page NAME --title T --text \"...\"|--file F   a standing page: served, in the menu, readable by assistants\n"
        "                    [--group G]                      Markdown keeps its sections, code and tables; six pages or a group make a manual\n"
        "  bonfyre site offer NAME --title T --price \"$450 per project\" --text \"...\" [--bookable]\n"
        "        something the site sells: on its offers page, and readable by assistants; nothing takes payment\n"
        "  bonfyre site accept NAME --request REF [--when T]  a request becomes a promise (commitments), an invoice (Pay), an opportunity (Finance)\n"
        "  bonfyre site paid NAME --request REF --reference BANK-REF | done NAME --request REF | promises NAME\n"
        "  bonfyre site domain NAME --to example.com [--ip A.B.C.D --secrets FILE [--apply]]\n"
        "  bonfyre site post NAME --title T --text \"...\"      a dated post;  --withdraw ID takes one down\n"
        "  bonfyre site page NAME --route /path --remove | --replace ...\n"
        "  bonfyre site key NAME [--label WHO] [--write]      a key for one assistant, tried before it is handed over\n"
        "  bonfyre site key NAME --withdraw WHO | keys NAME   withdraw a holder's key; who holds what\n"
        "  bonfyre site share NAME [--stop]                   a public address for the site, tried from outside first\n"
        "  bonfyre site usage NAME | inbox NAME               what assistants asked; messages left for the owner\n"
        "  bonfyre site mcp NAME [--json]                   the site as an assistant's tool: stood up, checked, ready to paste\n"
        "  bonfyre site check NAME [--json]                 dead links, unreadable colours, missing name or language\n"
        "  bonfyre site export NAME --out FILE [--format html|wordpress|frappe]\n"
        "        html: one self-contained page, fit to attach.  wordpress: its export file.  frappe: its documents\n"
        "  bonfyre site import SOURCE --as NAME [--limit N]\n"
        "        a WordPress address or export file, Frappe documents, any page address, or a file\n"
        "  bonfyre site serve | stop\n\n"
        "  --home DIR      where this network of sites lives (default ~/.bonfyre/sites)\n"
        "  --cms-db FILE   read another estate's content store (list, show, search)\n"
        "  --port N        local port (default 8787)\n", stderr);
}

int bf_site_main(int argc, char **argv, const char *self) {
    Site s;
    const char *verb = argc > 2 ? argv[2] : NULL;
    int rc;
    if (verb && bf_help_requested(argc - 1, argv + 1)) { site_usage(); return 0; }
    if (!verb) { site_usage(); return 2; }
    if ((rc = site_init(&s, argc, argv, self))) return rc;
    if (!strcmp(verb, "new")) return site_new(&s, argc, argv);
    if (!strcmp(verb, "list")) return site_list(&s);
    if (!strcmp(verb, "show")) return site_show(&s, argc, argv);
    if (!strcmp(verb, "search")) return site_search(&s, argc, argv);
    if (!strcmp(verb, "check")) return site_check(&s, argc, argv);
    if (!strcmp(verb, "mcp")) return site_mcp(&s, argc, argv);
    if (!strcmp(verb, "page")) return site_page(&s, argc, argv);
    if (!strcmp(verb, "key")) return site_key(&s, argc, argv);
    if (!strcmp(verb, "usage")) return site_meter_usage(&s, argc, argv);
    if (!strcmp(verb, "inbox")) return site_inbox(&s, argc, argv);
    if (!strcmp(verb, "offer")) return site_offer(&s, argc, argv);
    if (!strcmp(verb, "accept")) return site_accept(&s, argc, argv);
    if (!strcmp(verb, "paid")) return site_paid(&s, argc, argv);
    if (!strcmp(verb, "done")) return site_done(&s, argc, argv);
    if (!strcmp(verb, "domain")) return site_domain(&s, argc, argv);
    if (!strcmp(verb, "set")) return site_set(&s, argc, argv);
    if (!strcmp(verb, "promises")) return site_promises(&s, argc, argv);
    if (!strcmp(verb, "post")) return site_post(&s, argc, argv);
    if (!strcmp(verb, "share")) return site_share(&s, argc, argv);
    if (!strcmp(verb, "keys")) { int n = argc; char **v = calloc((size_t)argc + 2, sizeof(char *)); if (!v) return 3;
        for (int i = 0; i < argc; i++) v[i] = argv[i]; v[n] = "--list"; return site_key(&s, n + 1, v); }
    if (!strcmp(verb, "export")) return site_export(&s, argc, argv);
    if (!strcmp(verb, "import")) return site_import(&s, argc, argv);
    if (!strcmp(verb, "serve")) {
        if ((rc = ensure_store(&s)) || (rc = ensure_server(&s))) return rc;
        printf("{\"serving\":true,\"port\":%d,\"home\":", s.port); json_text(stdout, s.home); puts("}");
        return 0;
    }
    if (!strcmp(verb, "stop")) return site_stop(&s);
    site_usage();
    return 2;
}
