/* Customer containers: the downstream account each customer gets inside a
 * provider account Aurekai already holds. One verb set across providers:
 *
 *   bonfyre-provider-estate customer open|show|close|access [--mode reveal|scoped|wire] --provider P --customer C
 *        [--env FILE] [--api BASE]
 *
 * neon    a project per customer in Aurekai's Neon organization
 * sentry  a team and a project per customer in Aurekai's Sentry organization
 * vercel  a project per customer in Aurekai's Vercel team
 * render  a project per customer in Aurekai's Render workspace
 * supabase a project per customer in Aurekai's first Supabase organization;
 *         its database password is random, never printed and never kept
 * gcp     a Google Cloud project per customer through the signed-in gcloud,
 *         fixed id akc-<hash>, labelled aurekai_container=<name>; close is
 *         Google's 30-day soft delete and reopening restores it
 * stripe  a Connect account per customer under Aurekai's platform account,
 *         through the signed-in Stripe CLI (its key stays in the keychain;
 *         test mode until live is chosen)
 *
 * The held key is read from the env file (default ~/.bonfyre/secrets/
 * cloud.refs.env) and passed to curl on stdin, never in argv. Output is
 * identifiers and state only; a provider's connection strings and keys are
 * never printed. Every container is named aurekai-c-<customer>, so show and
 * close can only reach containers this command made. --api is for the test
 * double and accepts only http://127.0.0.1. */
#include <fcntl.h>
#include <stdint.h>
#include <sys/wait.h>
#include <unistd.h>
#include "bf_json.h"

typedef struct { const char *slug, *env_name, *base, *env_file; } CustomerProvider;
static const CustomerProvider customer_providers[] = {
    {"neon", "NEON_API_KEY", "https://console.neon.tech/api/v2", NULL},
    {"sentry", "SENTRY_AUTH_TOKEN", "https://sentry.io/api/0", NULL},
    {"vercel", "VERCEL_TOKEN", "https://api.vercel.com", NULL},
    {"render", "RENDER_API_KEY", "https://api.render.com/v1", NULL},
    {"supabase", "SUPABASE_ACCESS_TOKEN", "https://api.supabase.com/v1", NULL},
    {"stripe", NULL, "https://api.stripe.com/v1", NULL},
    {NULL, NULL, NULL, NULL}};

static int read_env_value(const char *file, const char *name, char *out, size_t cap) {
    FILE *f = fopen(file, "r"); char line[LINE_MAX_BYTES]; size_t nl = strlen(name); int found = 0;
    if (!f) return 0;
    while (!found && fgets(line, sizeof(line), f)) {
        char *s = line; while (*s == ' ' || *s == '\t') s++; if (!strncmp(s, "export ", 7)) s += 7;
        if (strncmp(s, name, nl)) continue;
        s += nl; while (*s == ' ') s++; if (*s != '=') continue; s++; while (*s == ' ') s++;
        s[strcspn(s, "\r\n")] = 0;
        size_t len = strlen(s); if (len >= 2 && (s[0] == '"' || s[0] == '\'') && s[len - 1] == s[0]) { s[len - 1] = 0; s++; }
        if (strlen(s) < cap && strlen(s) > 0 && !strpbrk(s, "\"\\\n")) { strcpy(out, s); found = 1; }
    }
    fclose(f); return found;
}

/* Stripe's key stays in the Stripe CLI's keychain entry; requests go through
 * `stripe get|post|delete PATH`, and the reply's error object decides status. */
static int customer_stripe(const char *method, const char *url, const char *body, char *out, size_t cap, int *status) {
    const char *path = strstr(url, "/v1/"); char verb[8], fields[1024]; char *argv[48]; int n = 0, outp[2]; *status = 0; out[0] = 0;
    if (!path) return -1;
    snprintf(verb, sizeof(verb), "%s", !strcmp(method, "GET") ? "get" : !strcmp(method, "DELETE") ? "delete" : "post");
    argv[n++] = "stripe"; argv[n++] = verb; argv[n++] = (char *)path;
    if (!strcmp(verb, "delete")) argv[n++] = "--confirm";
    snprintf(fields, sizeof(fields), "%s", body ? body : "");
    for (char *f = body ? strtok(fields, "&") : NULL; f && n < 44; f = strtok(NULL, "&")) { argv[n++] = "-d"; argv[n++] = f; }
    argv[n] = NULL;
    if (pipe(outp)) return -1;
    pid_t pid = fork(); if (pid < 0) return -1;
    if (pid == 0) { int nul = open("/dev/null", O_RDWR); dup2(nul, 0); dup2(nul, 2); dup2(outp[1], 1); close(outp[0]); const char *bin = getenv("BONFYRE_STRIPE_BINARY"); execv(bin && *bin ? bin : "/opt/homebrew/bin/stripe", argv); _exit(127); }
    close(outp[1]); size_t got = 0; ssize_t k;
    while ((k = read(outp[0], out + got, cap - 1 - got)) > 0) { got += (size_t)k; if (got >= cap - 1) break; }
    out[got] = 0; close(outp[0]); int st; waitpid(pid, &st, 0);
    char *json = strchr(out, '{'); if (json && json != out) memmove(out, json, strlen(json) + 1);
    *status = !json ? 502 : strstr(out, "\"error\"") ? (strstr(out, "resource_missing") ? 404 : 400) : 200;
    return 0;
}

/* One request; the token and body travel in a curl config on stdin. */
static int customer_http(const char *method, const char *url, const char *token, const char *body, char *out, size_t cap, int *status) {
    if (!strcmp(token, "stripe-cli")) return customer_stripe(method, url, body, out, cap, status);
    int in[2], outp[2]; *status = 0; out[0] = 0;
    if (pipe(in) || pipe(outp)) return -1;
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        dup2(in[0], 0); dup2(outp[1], 1); close(in[1]); close(outp[0]);
        execlp("curl", "curl", "-sS", "--max-time", getenv("BONFYRE_PROVIDER_HTTP_TIMEOUT") && *getenv("BONFYRE_PROVIDER_HTTP_TIMEOUT") ? getenv("BONFYRE_PROVIDER_HTTP_TIMEOUT") : "30", "-K", "-", (char *)NULL); _exit(127);
    }
    close(in[0]); close(outp[1]);
    FILE *cfg = fdopen(in[1], "w");
    fprintf(cfg, "url = \"%s\"\nrequest = \"%s\"\nheader = \"Authorization: Bearer %s\"\nheader = \"Accept: application/json\"\n", url, method, token);
    /* A JSON body says so; anything else is Stripe's form encoding, curl's default. */
    if (body) fprintf(cfg, "%sdata = \"%s\"\n", body[0] == '{' ? "header = \"Content-Type: application/json\"\n" : "", body);
    fputs("write-out = \"\\n%{http_code}\"\n", cfg); fclose(cfg);
    size_t n = 0; ssize_t k;
    while ((k = read(outp[0], out + n, cap - 1 - n)) > 0) { n += (size_t)k; if (n >= cap - 1) break; }
    out[n] = 0; close(outp[0]);
    int st; waitpid(pid, &st, 0);
    char *last = strrchr(out, '\n'); if (last) { *status = atoi(last + 1); *last = 0; }
    return 0;
}

static int customer_slug_ok(const char *s) {
    size_t n = strlen(s); if (!n || n > 40 || s[0] == '-' || s[n - 1] == '-') return 0;
    for (size_t i = 0; i < n; i++) if (!(islower((unsigned char)s[i]) || isdigit((unsigned char)s[i]) || s[i] == '-')) return 0;
    return 1;
}

static void customer_report(const char *provider, const char *customer, const char *state, const char *id, const char *where, int status) {
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider);
    bf_jsonw_key(&w, "customer"); bf_jsonw_str(&w, customer);
    bf_jsonw_key(&w, "state"); bf_jsonw_str(&w, state);
    if (id && *id) { bf_jsonw_key(&w, "id"); bf_jsonw_str(&w, id); }
    if (where && *where) { bf_jsonw_key(&w, "container"); bf_jsonw_str(&w, where); }
    bf_jsonw_key(&w, "status"); bf_jsonw_int(&w, status);
    bf_jsonw_obj_end(&w); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
}

/* The first organization the key can see; containers live under it. */
static int customer_org(const CustomerProvider *p, const char *base, const char *token, char *org, size_t cap) {
    static char buf[1 << 20]; char url[512]; int status;
    snprintf(url, sizeof(url), !strcmp(p->slug, "neon") ? "%s/users/me/organizations" : !strcmp(p->slug, "stripe") ? "%s/account" : !strcmp(p->slug, "vercel") ? "%s/v2/teams" : !strcmp(p->slug, "render") ? "%s/owners?limit=1" : !strcmp(p->slug, "supabase") ? "%s/organizations" : "%s/organizations/", base);
    if (customer_http("GET", url, token, NULL, buf, sizeof(buf), &status) || status != 200) return 0;
    bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); if (!d) return 0;
    if (!strcmp(p->slug, "stripe")) { const bf_json_node_t *id = bf_json_obj_get(d, bf_json_root(d), "id"); int ok = id && bf_json_get_str_copy(id, org, cap) > 0; bf_json_free(d); return ok; }
    if (!strcmp(p->slug, "render")) { const bf_json_node_t *id = bf_json_dotpath(d, "0.owner.id"); int ok = id && bf_json_get_str_copy(id, org, cap) > 0; bf_json_free(d); return ok; }
    const bf_json_node_t *list = !strcmp(p->slug, "neon") ? bf_json_obj_get(d, bf_json_root(d), "organizations") : !strcmp(p->slug, "vercel") ? bf_json_obj_get(d, bf_json_root(d), "teams") : bf_json_root(d);
    const bf_json_node_t *first = list ? bf_json_arr_get(d, list, 0) : NULL;
    const bf_json_node_t *id = first ? bf_json_obj_get(d, first, !strcmp(p->slug, "sentry") ? "slug" : "id") : NULL;
    int ok = id && bf_json_get_str_copy(id, org, cap) > 0; bf_json_free(d); return ok;
}

/* Find this customer's container: its provider id, or empty when there is none. */
static int customer_find(const CustomerProvider *p, const char *base, const char *token, const char *org, const char *name, char *id, size_t cap) {
    static char buf[1 << 20]; char url[512]; int status; id[0] = 0;
    if (!strcmp(p->slug, "neon")) snprintf(url, sizeof(url), "%s/projects?org_id=%s&search=%s", base, org, name);
    else if (!strcmp(p->slug, "stripe")) snprintf(url, sizeof(url), "%s/accounts?limit=100", base);
    else if (!strcmp(p->slug, "vercel")) snprintf(url, sizeof(url), "%s/v9/projects/%s?teamId=%s", base, name, org);
    else if (!strcmp(p->slug, "render")) snprintf(url, sizeof(url), "%s/projects?ownerId=%s&name=%s&limit=100", base, org, name);
    else if (!strcmp(p->slug, "supabase")) snprintf(url, sizeof(url), "%s/projects", base);
    else snprintf(url, sizeof(url), "%s/projects/%s/%s/", base, org, name);
    if (customer_http("GET", url, token, NULL, buf, sizeof(buf), &status)) return -1;
    if (!strcmp(p->slug, "sentry") || !strcmp(p->slug, "vercel")) {
        if (status == 404) return 0;
        if (status != 200) return -status;
        bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); const bf_json_node_t *v = d ? bf_json_obj_get(d, bf_json_root(d), "id") : NULL;
        if (v) bf_json_get_str_copy(v, id, cap); bf_json_free(d); return id[0] ? 1 : -status;
    }
    if (status != 200) return -status;
    bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); if (!d) return -1;
    int stripe = !strcmp(p->slug, "stripe"), render = !strcmp(p->slug, "render"), supa = !strcmp(p->slug, "supabase");
    const bf_json_node_t *list = render || supa ? bf_json_root(d) : bf_json_obj_get(d, bf_json_root(d), stripe ? "data" : "projects");
    for (const bf_json_node_t *row = list ? bf_json_child_first(d, list) : NULL; row; row = bf_json_child_next(d, row)) {
        const bf_json_node_t *c = render ? bf_json_obj_get(d, row, "project") : row; if (!c) continue;
        char nm[128] = ""; const bf_json_node_t *m = stripe ? bf_json_obj_get(d, c, "metadata") : c, *v = m ? bf_json_obj_get(d, m, stripe ? "aurekai_container" : "name") : NULL; if (v) bf_json_get_str_copy(v, nm, sizeof(nm));
        if (supa) { char o[64] = "", st[32] = ""; const bf_json_node_t *ov = bf_json_obj_get(d, c, "organization_id"), *sv = bf_json_obj_get(d, c, "status"); if (ov) bf_json_get_str_copy(ov, o, sizeof(o)); if (sv) bf_json_get_str_copy(sv, st, sizeof(st)); if (strcmp(o, org) || !strncmp(st, "REMOVED", 7) || !strcmp(st, "GOING_DOWN")) continue; }
        if (!strcmp(nm, name)) { v = bf_json_obj_get(d, c, "id"); if (v) bf_json_get_str_copy(v, id, cap); break; }
    }
    bf_json_free(d); return id[0] ? 1 : 0;
}

/* How many customer containers this provider account already holds. Every
 * container carries the aurekai-c- prefix, so the count never includes
 * Aurekai's own resources. -1 when the provider did not answer. */
static int customer_count(const CustomerProvider *p, const char *base, const char *token, const char *org) {
    static char buf[1 << 20]; char url[512]; int status, n = 0; const char *key, *field; int meta = 0;
    if (!strcmp(p->slug, "neon")) { snprintf(url, sizeof(url), "%s/projects?org_id=%s&search=aurekai-c-&limit=400", base, org); key = "projects"; field = "name"; }
    else if (!strcmp(p->slug, "stripe")) { snprintf(url, sizeof(url), "%s/accounts?limit=100", base); key = "data"; field = "aurekai_container"; meta = 1; }
    else if (!strcmp(p->slug, "vercel")) { snprintf(url, sizeof(url), "%s/v9/projects?teamId=%s&search=aurekai-c-&limit=100", base, org); key = "projects"; field = "name"; }
    else if (!strcmp(p->slug, "render")) { snprintf(url, sizeof(url), "%s/projects?ownerId=%s&limit=100", base, org); key = NULL; field = "name"; }
    else if (!strcmp(p->slug, "supabase")) { snprintf(url, sizeof(url), "%s/projects", base); key = NULL; field = "name"; }
    else { snprintf(url, sizeof(url), "%s/organizations/%s/projects/?query=aurekai-c-", base, org); key = NULL; field = "slug"; }
    if (customer_http("GET", url, token, NULL, buf, sizeof(buf), &status) || status != 200) return -1;
    bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); if (!d) return -1;
    const bf_json_node_t *list = key ? bf_json_obj_get(d, bf_json_root(d), key) : bf_json_root(d);
    for (const bf_json_node_t *row = list ? bf_json_child_first(d, list) : NULL; row; row = bf_json_child_next(d, row)) {
        const bf_json_node_t *c = !strcmp(p->slug, "render") ? bf_json_obj_get(d, row, "project") : row; if (!c) continue;
        char nm[128] = ""; const bf_json_node_t *m = meta ? bf_json_obj_get(d, c, "metadata") : c, *v = m ? bf_json_obj_get(d, m, field) : NULL;
        if (v) bf_json_get_str_copy(v, nm, sizeof(nm)); if (!strncmp(nm, "aurekai-c-", 10)) n++;
    }
    bf_json_free(d); return n;
}

/* Google Cloud goes through the signed-in gcloud; its credentials stay in
 * gcloud's own store. Returns gcloud's exit status, stdout in out. */
static int customer_gcloud(char *const args[], char *out, size_t cap) {
    int outp[2]; out[0] = 0; if (pipe(outp)) return -1;
    pid_t pid = fork(); if (pid < 0) return -1;
    if (pid == 0) { int nul = open("/dev/null", O_RDWR); dup2(nul, 0); dup2(nul, 2); dup2(outp[1], 1); close(outp[0]); const char *bin = getenv("BONFYRE_GCLOUD_BINARY"); execv(bin && *bin ? bin : "/opt/homebrew/bin/gcloud", args); _exit(127); }
    close(outp[1]); size_t got = 0; ssize_t k;
    while ((k = read(outp[0], out + got, cap - 1 - got)) > 0) { got += (size_t)k; if (got >= cap - 1) break; }
    out[got] = 0; close(outp[0]); int st; waitpid(pid, &st, 0);
    while (got && (out[got - 1] == '\n' || out[got - 1] == ' ')) out[--got] = 0;
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}
void bf_sha256_hex(const uint8_t *data, size_t len, char hex[65]);
/* Google's project search trails a create by minutes, so a customer's project
 * has a fixed id (akc- and 10 hex of the container name's SHA-256) and is
 * looked up directly. A closed project sits in Google's 30-day soft delete;
 * reopening the same customer restores it with undelete. */
static int customer_gcp(const char *verb, const char *customer) {
    char name[64], label[128], id[24], hex[65], out[4096], state[32];
    snprintf(name, sizeof(name), "aurekai-c-%s", customer); bf_sha256_hex((const uint8_t *)name, strlen(name), hex); snprintf(id, sizeof(id), "akc-%.10s", hex);
    char *const describe[] = {"gcloud", "projects", "describe", id, "--format=value(lifecycleState,labels.aurekai_container)", NULL};
    int known = customer_gcloud(describe, out, sizeof(out)) == 0; char owner[64] = "";
    state[0] = 0; if (known) { char *tab = strchr(out, '\t'); if (tab) { *tab = 0; snprintf(owner, sizeof(owner), "%s", tab + 1); } snprintf(state, sizeof(state), "%s", out); }
    if (known && *owner && strcmp(owner, name)) { fprintf(stderr, "%s belongs to another container\n", id); return 1; }
    int active = known && !strcmp(state, "ACTIVE");
    if (!strcmp(verb, "show")) { customer_report("gcp", customer, active ? "open" : "none", active ? id : "", active ? name : "", 200); return active ? 0 : 3; }
    if (!strcmp(verb, "open")) {
        if (active) { customer_report("gcp", customer, "open", id, name, 200); return 0; }
        if (known && !strcmp(state, "DELETE_REQUESTED")) {
            char *const undelete[] = {"gcloud", "projects", "undelete", id, "--quiet", NULL};
            if (customer_gcloud(undelete, out, sizeof(out)) != 0) { fputs("google cloud refused to restore the project\n", stderr); return 1; }
        } else {
            snprintf(label, sizeof(label), "aurekai_container=%s", name); char display[31]; snprintf(display, sizeof(display), "%.30s", name);
            char *const create[] = {"gcloud", "projects", "create", id, "--name", display, "--labels", label, "--quiet", NULL};
            if (customer_gcloud(create, out, sizeof(out)) != 0) { fputs("google cloud refused the project\n", stderr); return 1; }
        }
        int seen = 0; for (int tries = 0; tries < 6 && !seen; tries++) { if (customer_gcloud(describe, out, sizeof(out)) == 0 && !strncmp(out, "ACTIVE", 6)) seen = 1; else sleep(3); }
        if (!seen) { fprintf(stderr, "google cloud accepted %s but it does not read back active\n", id); return 1; }
        customer_report("gcp", customer, "open", id, name, 200); return 0;
    }
    if (!active) { customer_report("gcp", customer, "none", "", "", 200); return 0; }
    char *const del[] = {"gcloud", "projects", "delete", id, "--quiet", NULL};
    if (customer_gcloud(del, out, sizeof(out)) != 0) { fputs("google cloud refused the close\n", stderr); return 1; }
    int gone = customer_gcloud(describe, out, sizeof(out)) == 0 && !strncmp(out, "DELETE_REQUESTED", 16);
    customer_report("gcp", customer, gone ? "closed" : "closing", "", "", 200); return 0;
}

static int customer_cmd(int argc, char **argv) {
    const char *verb = argc > 2 ? argv[2] : "", *provider = NULL, *customer = NULL, *api = NULL, *mode = "reveal"; char env[1024]; int explicit_env = 0;
    if (getenv("BONFYRE_PROVIDER_ENV") && *getenv("BONFYRE_PROVIDER_ENV")) snprintf(env, sizeof(env), "%s", getenv("BONFYRE_PROVIDER_ENV"));
    else snprintf(env, sizeof(env), "%s/.bonfyre/secrets/cloud.refs.env", getenv("HOME") ? getenv("HOME") : "");
    for (int i = 3; i < argc; i++) {
        if (!strcmp(argv[i], "--provider") && i + 1 < argc) provider = argv[++i];
        else if (!strcmp(argv[i], "--customer") && i + 1 < argc) customer = argv[++i];
        else if (!strcmp(argv[i], "--env") && i + 1 < argc) { snprintf(env, sizeof(env), "%s", argv[++i]); explicit_env = 1; }
        else if (!strcmp(argv[i], "--api") && i + 1 < argc) api = argv[++i];
        else if (!strcmp(argv[i], "--mode") && i + 1 < argc) mode = argv[++i];
        else { fprintf(stderr, "unknown argument %s\n", argv[i]); return 2; }
    }
    if ((strcmp(verb, "open") && strcmp(verb, "show") && strcmp(verb, "close") && strcmp(verb, "access")) || !provider || !customer) {
        fputs("usage: bonfyre-provider-estate customer open|show|close|access [--mode reveal|scoped|wire] --provider neon|sentry|vercel|render|supabase|gcp|stripe --customer SLUG [--env FILE]\n", stderr); return 2;
    }
    if (!strcmp(provider, "gcp")) {
        if (!customer_slug_ok(customer)) { fputs("customer must be lowercase letters, digits and hyphens, at most 40\n", stderr); return 2; }
        if (!strcmp(verb, "access")) { fputs("gcp has no customer access to hand over yet\n", stderr); return 2; }
        return customer_gcp(verb, customer);
    }
    const CustomerProvider *p = NULL; for (int i = 0; customer_providers[i].slug; i++) if (!strcmp(customer_providers[i].slug, provider)) p = &customer_providers[i];
    if (!p) { fprintf(stderr, "no customer containers for %s yet\n", provider); return 2; }
    if (!customer_slug_ok(customer)) { fputs("customer must be lowercase letters, digits and hyphens, at most 40\n", stderr); return 2; }
    if (api && strncmp(api, "http://127.0.0.1:", 17)) { fputs("--api accepts only http://127.0.0.1:PORT\n", stderr); return 2; }
    const char *base = api ? api : p->base;
    char token[1024], org[128], name[64], id[128], url[512], body[512]; static char buf[1 << 20]; int status;
    (void)explicit_env;
    if (!p->env_name) { if (api) { fputs("stripe runs through the Stripe CLI and has no test API\n", stderr); return 2; } snprintf(token, sizeof(token), "stripe-cli"); }
    else if (!read_env_value(env, p->env_name, token, sizeof(token))) { fprintf(stderr, "no %s in %s\n", p->env_name, env); return 1; }
    if (!customer_org(p, base, token, org, sizeof(org))) { fprintf(stderr, "%s refused the held key or showed no organization\n", provider); return 1; }
    snprintf(name, sizeof(name), "aurekai-c-%s", customer);
    int found = customer_find(p, base, token, org, name, id, sizeof(id));
    if (found < 0) { fprintf(stderr, "%s did not answer the lookup (%d)\n", provider, -found); return 1; }
    char where[256]; snprintf(where, sizeof(where), "%s/%s", org, name);
    /* access hands the customer the way into its own container, in one of
     * three modes the operator can turn on or off (BONFYRE_PROVIDER_ACCESS_MODES,
     * default "reveal,scoped,wire"):
     *   reveal  the container's own owner login (Neon owner role, Sentry's
     *           first DSN), printed once for the caller to show
     *   scoped  a new login made for this handoff alone (a Neon role, a
     *           Sentry client key) that can be revoked without touching
     *           the rest, printed once
     *   wire    a scoped login written straight into the customer's own
     *           Vercel project as an encrypted environment variable;
     *           nothing is printed and no person sees it
     * Nothing is stored here in any mode. */
    if (!strcmp(verb, "access")) {
        const char *modes = getenv("BONFYRE_PROVIDER_ACCESS_MODES"); if (!modes || !*modes) modes = "reveal,scoped,wire";
        if (!strstr(modes, mode)) { fprintf(stderr, "access mode %s is turned off here\n", mode); return 2; }
        if (strcmp(mode, "reveal") && strcmp(mode, "scoped") && strcmp(mode, "wire")) { fputs("--mode is reveal, scoped or wire\n", stderr); return 2; }
        if (!found) { customer_report(provider, customer, "none", "", "", 200); return 3; }
        char acc[1024] = "", role[48] = "", envkey[32] = ""; const char *kind = ""; int scoped = strcmp(mode, "reveal") != 0;
        if (!strcmp(p->slug, "neon")) {
            snprintf(envkey, sizeof(envkey), "DATABASE_URL");
            snprintf(role, sizeof(role), "neondb_owner");
            if (scoped) {
                char branch[64] = ""; unsigned char r[4]; FILE *u = fopen("/dev/urandom", "rb"); if (!u || fread(r, 1, 4, u) != 4) { if (u) fclose(u); return 1; } fclose(u);
                snprintf(url, sizeof(url), "%s/projects/%s/branches", base, id);
                if (customer_http("GET", url, token, NULL, buf, sizeof(buf), &status) || status != 200) { fprintf(stderr, "neon did not list branches (%d)\n", status); return 1; }
                bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); const bf_json_node_t *v = d ? bf_json_dotpath(d, "branches.0.id") : NULL; if (v) bf_json_get_str_copy(v, branch, sizeof(branch)); bf_json_free(d);
                snprintf(role, sizeof(role), "aurekai_%02x%02x%02x%02x", r[0], r[1], r[2], r[3]);
                snprintf(url, sizeof(url), "%s/projects/%s/branches/%s/roles", base, id, branch);
                snprintf(body, sizeof(body), "{\\\"role\\\":{\\\"name\\\":\\\"%s\\\"}}", role);
                if (!*branch || customer_http("POST", url, token, body, buf, sizeof(buf), &status) || (status != 201 && status != 200)) { fprintf(stderr, "neon refused the role (%d)\n", status); return 1; }
            }
            kind = scoped ? "postgres login made for this handoff, on this project only" : "postgres owner login for this project only";
            snprintf(url, sizeof(url), "%s/projects/%s/connection_uri?database_name=neondb&role_name=%s", base, id, role);
            for (int tries = 0; tries < 5; tries++) {
                if (!customer_http("GET", url, token, NULL, buf, sizeof(buf), &status) && status == 200) break;
                sleep(2);
            }
            if (status != 200) { fprintf(stderr, "neon refused the connection string (%d)\n", status); return 1; }
            bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); const bf_json_node_t *v = d ? bf_json_obj_get(d, bf_json_root(d), "uri") : NULL; if (v) bf_json_get_str_copy(v, acc, sizeof(acc)); bf_json_free(d);
        } else if (!strcmp(p->slug, "sentry")) {
            snprintf(envkey, sizeof(envkey), "SENTRY_DSN");
            snprintf(url, sizeof(url), "%s/projects/%s/%s/keys/", base, org, name);
            if (scoped) snprintf(body, sizeof(body), "{\\\"name\\\":\\\"aurekai handoff\\\"}");
            if (customer_http(scoped ? "POST" : "GET", url, token, scoped ? body : NULL, buf, sizeof(buf), &status) || (status != 200 && status != 201)) { fprintf(stderr, "sentry refused the key (%d)\n", status); return 1; }
            kind = scoped ? "Sentry DSN made for this handoff, on this project only" : "Sentry DSN for this project only";
            bf_json_doc_t *d = bf_json_parse_str(buf, NULL, 0); const bf_json_node_t *v = d ? bf_json_dotpath(d, scoped ? "dsn.public" : "0.dsn.public") : NULL; if (v) bf_json_get_str_copy(v, acc, sizeof(acc)); bf_json_free(d);
        } else { fprintf(stderr, "%s has no customer access to hand over yet\n", provider); return 2; }
        if (!acc[0] || strpbrk(acc, "\"\\\n")) { fprintf(stderr, "%s returned no usable access\n", provider); return 1; }
        if (!strcmp(mode, "wire")) {
            /* The customer's own Vercel project receives it; the same
             * aurekai-c-<customer> name ties the two containers together. */
            char vtoken[1024], team[96] = "", vurl[512], vbody[2048]; static char vbuf[1 << 16]; int vs;
            if (!read_env_value(env, "VERCEL_TOKEN", vtoken, sizeof(vtoken))) { fputs("wire needs the held Vercel key\n", stderr); return 1; }
            const CustomerProvider *vp = NULL; for (int i = 0; customer_providers[i].slug; i++) if (!strcmp(customer_providers[i].slug, "vercel")) vp = &customer_providers[i];
            const char *vbase = api ? api : vp->base; char vid[128];
            if (!customer_org(vp, vbase, vtoken, team, sizeof(team)) || customer_find(vp, vbase, vtoken, team, name, vid, sizeof(vid)) != 1) { fputs("the customer has no Vercel project to wire into; open one first\n", stderr); memset(acc, 0, sizeof(acc)); return 3; }
            snprintf(vurl, sizeof(vurl), "%s/v10/projects/%s/env?teamId=%s&upsert=true", vbase, name, team);
            snprintf(vbody, sizeof(vbody), "{\\\"key\\\":\\\"%s\\\",\\\"value\\\":\\\"%s\\\",\\\"type\\\":\\\"encrypted\\\",\\\"target\\\":[\\\"production\\\",\\\"preview\\\",\\\"development\\\"]}", envkey, acc);
            int rc = customer_http("POST", vurl, vtoken, vbody, vbuf, sizeof(vbuf), &vs); memset(vbody, 0, sizeof(vbody)); memset(acc, 0, sizeof(acc));
            if (rc || (vs != 200 && vs != 201)) { fprintf(stderr, "vercel refused the environment variable (%d)\n", vs); return 1; }
            bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider); bf_jsonw_key(&w, "customer"); bf_jsonw_str(&w, customer);
            bf_jsonw_key(&w, "mode"); bf_jsonw_str(&w, "wire"); bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, kind);
            bf_jsonw_key(&w, "wired_into"); bf_jsonw_str(&w, name); bf_jsonw_key(&w, "variable"); bf_jsonw_str(&w, envkey);
            bf_jsonw_obj_end(&w); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w); return 0;
        }
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "provider"); bf_jsonw_str(&w, provider); bf_jsonw_key(&w, "customer"); bf_jsonw_str(&w, customer);
        bf_jsonw_key(&w, "mode"); bf_jsonw_str(&w, mode); bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, kind); bf_jsonw_key(&w, "access"); bf_jsonw_str(&w, acc);
        bf_jsonw_obj_end(&w); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w); memset(acc, 0, sizeof(acc)); return 0;
    }
    if (!strcmp(verb, "show")) { customer_report(provider, customer, found ? "open" : "none", id, found ? where : "", 200); return found ? 0 : 3; }
    if (!strcmp(verb, "open")) {
        if (found) { customer_report(provider, customer, "open", id, where, 200); return 0; }
        /* Free plans cap projects; a ceiling per provider account keeps one
         * busy organization from exhausting it for everyone. */
        const char *cap_env = getenv("BONFYRE_PROVIDER_CUSTOMER_MAX"); int ceiling = cap_env && atoi(cap_env) > 0 ? atoi(cap_env) : 20;
        int held = customer_count(p, base, token, org);
        if (held < 0) { fprintf(stderr, "%s did not answer the count\n", provider); return 1; }
        if (held >= ceiling) { fprintf(stderr, "%s already holds %d customer containers, the ceiling is %d\n", provider, held, ceiling); customer_report(provider, customer, "full", "", "", 429); return 4; }
        if (!strcmp(p->slug, "supabase")) {
            unsigned char r[24]; char pass[49]; FILE *u = fopen("/dev/urandom", "rb"); if (!u || fread(r, 1, sizeof(r), u) != sizeof(r)) { if (u) fclose(u); return 1; } fclose(u);
            for (int k = 0; k < 24; k++) snprintf(pass + 2 * k, 3, "%02x", r[k]);
            snprintf(url, sizeof(url), "%s/projects", base);
            snprintf(body, sizeof(body), "{\\\"name\\\":\\\"%s\\\",\\\"organization_id\\\":\\\"%s\\\",\\\"region\\\":\\\"us-east-1\\\",\\\"db_pass\\\":\\\"%s\\\"}", name, org, pass);
            memset(pass, 0, sizeof(pass));
            int rc = customer_http("POST", url, token, body, buf, sizeof(buf), &status); memset(body, 0, sizeof(body));
            if (rc || (status != 201 && status != 200)) { fprintf(stderr, "supabase refused the project (%d)\n", status); return 1; }
        } else if (!strcmp(p->slug, "render")) {
            snprintf(url, sizeof(url), "%s/projects", base);
            snprintf(body, sizeof(body), "{\\\"name\\\":\\\"%s\\\",\\\"ownerId\\\":\\\"%s\\\",\\\"environments\\\":[{\\\"name\\\":\\\"production\\\"}]}", name, org);
            if (customer_http("POST", url, token, body, buf, sizeof(buf), &status) || (status != 201 && status != 200)) { fprintf(stderr, "render refused the project (%d)\n", status); return 1; }
        } else if (!strcmp(p->slug, "vercel")) {
            snprintf(url, sizeof(url), "%s/v11/projects?teamId=%s", base, org);
            snprintf(body, sizeof(body), "{\\\"name\\\":\\\"%s\\\"}", name);
            if (customer_http("POST", url, token, body, buf, sizeof(buf), &status) || status != 200) { fprintf(stderr, "vercel refused the project (%d)\n", status); return 1; }
        } else if (!strcmp(p->slug, "neon")) {
            snprintf(url, sizeof(url), "%s/projects", base);
            snprintf(body, sizeof(body), "{\\\"project\\\":{\\\"name\\\":\\\"%s\\\",\\\"org_id\\\":\\\"%s\\\"}}", name, org);
            if (customer_http("POST", url, token, body, buf, sizeof(buf), &status) || (status != 201 && status != 200)) { fprintf(stderr, "neon refused the project (%d)\n", status); return 1; }
        } else if (!strcmp(p->slug, "stripe")) {
            /* Express dashboard, Aurekai pays fees and carries losses: the
             * platform-managed shape a customer completes through Stripe's
             * own onboarding link. */
            snprintf(url, sizeof(url), "%s/accounts", base);
            snprintf(body, sizeof(body), "controller[stripe_dashboard][type]=express&controller[fees][payer]=application&controller[losses][payments]=application&metadata[aurekai_container]=%s", name);
            if (customer_http("POST", url, token, body, buf, sizeof(buf), &status) || status != 200) { fprintf(stderr, "stripe refused the account (%d)\n", status); return 1; }
        } else {
            snprintf(url, sizeof(url), "%s/organizations/%s/teams/", base, org);
            snprintf(body, sizeof(body), "{\\\"slug\\\":\\\"%s\\\"}", name);
            if (customer_http("POST", url, token, body, buf, sizeof(buf), &status) || (status != 201 && status != 409)) { fprintf(stderr, "sentry refused the team (%d)\n", status); return 1; }
            snprintf(url, sizeof(url), "%s/teams/%s/%s/projects/", base, org, name);
            snprintf(body, sizeof(body), "{\\\"name\\\":\\\"%s\\\",\\\"slug\\\":\\\"%s\\\"}", name, name);
            if (customer_http("POST", url, token, body, buf, sizeof(buf), &status) || status != 201) { fprintf(stderr, "sentry refused the project (%d)\n", status); return 1; }
        }
        /* Read it back rather than trusting the create response. */
        if (customer_find(p, base, token, org, name, id, sizeof(id)) != 1) { fprintf(stderr, "%s accepted the container but it does not read back\n", provider); return 1; }
        customer_report(provider, customer, "open", id, where, status); return 0;
    }
    if (!found) { customer_report(provider, customer, "none", "", "", 200); return 0; }
    if (!strcmp(p->slug, "neon")) snprintf(url, sizeof(url), "%s/projects/%s", base, id);
    else if (!strcmp(p->slug, "stripe")) snprintf(url, sizeof(url), "%s/accounts/%s", base, id);
    else if (!strcmp(p->slug, "vercel")) snprintf(url, sizeof(url), "%s/v9/projects/%s?teamId=%s", base, name, org);
    else if (!strcmp(p->slug, "render")) snprintf(url, sizeof(url), "%s/projects/%s", base, id);
    else if (!strcmp(p->slug, "supabase")) snprintf(url, sizeof(url), "%s/projects/%s", base, id);
    else snprintf(url, sizeof(url), "%s/projects/%s/%s/", base, org, name);
    if (customer_http("DELETE", url, token, NULL, buf, sizeof(buf), &status) || (status != 200 && status != 202 && status != 204)) { fprintf(stderr, "%s refused the close (%d)\n", provider, status); return 1; }
    if (!strcmp(p->slug, "sentry")) {
        snprintf(url, sizeof(url), "%s/teams/%s/%s/", base, org, name);
        if (customer_http("DELETE", url, token, NULL, buf, sizeof(buf), &status) || (status != 204 && status != 202 && status != 200)) { fprintf(stderr, "sentry kept the team (%d)\n", status); return 1; }
    }
    int still = customer_find(p, base, token, org, name, id, sizeof(id));
    if (!strcmp(p->slug, "sentry") && still == 1) {
        /* Sentry schedules deletion; a project pending deletion still reads back for a moment. */
        sleep(3); still = customer_find(p, base, token, org, name, id, sizeof(id));
    }
    customer_report(provider, customer, still == 0 ? "closed" : "closing", "", "", status); return 0;
}
