#ifndef PROVIDER_ACCOUNTS_WEB_H
#define PROVIDER_ACCOUNTS_WEB_H
/* Provider accounts for an Aurekai organization. Each organization is a
 * customer sub-account; an administrator opens or closes that organization's
 * own container inside a provider account Aurekai holds, and connects
 * providers that the customer owns directly (GitHub). The containers belong
 * to bonfyre-provider-estate's customer verbs; this page only asks for them
 * and shows what the provider read back. Keys never reach the page. */

typedef struct { const char *slug, *name, *gives; } PaProvider;
static const PaProvider pa_providers[] = {
    {"neon", "Neon", "a Postgres project of its own"},
    {"sentry", "Sentry", "an error-tracking team and project of its own"},
    {"vercel", "Vercel", "a Vercel project of its own to deploy a site"},
    {"render", "Render", "a Render project of its own for services and databases"},
    {"supabase", "Supabase", "a Supabase project of its own (database, auth and storage)"},
    {"gcp", "Google Cloud", "a Google Cloud project of its own"},
    {"stripe", "Stripe", "a Stripe account to take payments, set up through Stripe's own onboarding"},
    {NULL, NULL, NULL}};

static const char *pa_binary(void) {
    const char *b = getenv("BONFYRE_PROVIDER_ESTATE_BINARY"); static char path[1024];
    if (b && *b) return b;
    snprintf(path, sizeof(path), "%s/.bonfyre/bin/bonfyre-provider-estate", getenv("HOME") ? getenv("HOME") : "");
    return path;
}
static const PaProvider *pa_find(const char *slug) { for (int i = 0; pa_providers[i].slug; i++) if (!strcmp(pa_providers[i].slug, slug)) return &pa_providers[i]; return NULL; }

/* The organization id becomes the provider-side customer name: lowercase
 * letters, digits and single hyphens, at most 40. */
static int pa_customer(const char *org, char out[41]) {
    size_t n = 0; int dash = 0;
    for (const char *p = org; *p && n < 40; p++) {
        unsigned char c = (unsigned char)*p;
        if (isalnum(c)) { out[n++] = (char)tolower(c); dash = 0; }
        else if (n && !dash) { out[n++] = '-'; dash = 1; }
    }
    while (n && out[n - 1] == '-') n--;
    out[n] = 0; return n > 0;
}

/* Runs one customer verb. Returns 1 when the provider reports a container,
 * 0 when it reports none, -1 when it did not answer. id receives the
 * provider's identifier. */
static int pa_run(const char *verb, const char *provider, const char *customer, char *id, size_t cap) {
    char out[2048], err[512]; id[0] = 0;
    char *const argv[] = {(char *)pa_binary(), "customer", (char *)verb, "--provider", (char *)provider, "--customer", (char *)customer, NULL};
    int rc = auth_call_split(argv, "", out, sizeof(out), err, sizeof(err));
    if (strstr(out, "\"state\":\"full\"")) return -2;
    if (rc != 0 && rc != 3) return -1;
    if (strstr(out, "\"state\":\"none\"") || strstr(out, "\"state\":\"closed\"")) return 0;
    if (!strstr(out, "\"state\":\"open\"")) return strstr(out, "\"state\":\"closing\"") ? 0 : -1;
    const char *k = strstr(out, "\"id\":\""); if (k) { k += 6; size_t n = strcspn(k, "\""); if (n < cap) { memcpy(id, k, n); id[n] = 0; } }
    return 1;
}

/* The holding organization's page lists the provider accounts Aurekai holds
 * and whether customers can use each yet, read from the estate ledger. */
static void pa_held(const char *org) {
    const char *holding = getenv("BONFYRE_PROVIDER_HOLDING_INSTITUTION"), *dir = getenv("BONFYRE_PROVIDER_ESTATE_DIR");
    char fallback[1024], out[16384], err[256];
    if (strcmp(org, holding && *holding ? holding : "bonfyre")) return;
    if (!dir || !*dir) { snprintf(fallback, sizeof(fallback), "%s/Developer/Bonfyre/cmd/BonfyreProviderEstate", getenv("HOME") ? getenv("HOME") : ""); dir = fallback; }
    char *const argv[] = {(char *)pa_binary(), "held", "--dir", (char *)dir, NULL};
    if (auth_call_split(argv, "", out, sizeof(out), err, sizeof(err)) != 0) return;
    bf_json_doc_t *d = bf_json_parse_str(out, NULL, 0); const bf_json_node_t *list = d ? bf_json_root(d) : NULL;
    if (!list) { bf_json_free(d); return; }
    fputs("<section data-provider-held><h2>Accounts Aurekai holds</h2><p>These are the outside accounts this organization holds for its customers. Usable means a customer organization can open its own account there from its page.</p><div class=\"scroll\"><table><thead><tr><th>Service</th><th>Account</th><th>For customers</th></tr></thead><tbody>", stdout);
    for (const bf_json_node_t *c = bf_json_child_first(d, list); c; c = bf_json_child_next(d, c)) {
        char name[96] = "", account[200] = "", customers[24] = "", gives[240] = "", slug[48] = "";
        const char *keys[] = {"name", "account", "customers", "gives", "slug"}; char *dest[] = {name, account, customers, gives, slug}; size_t caps[] = {sizeof(name), sizeof(account), sizeof(customers), sizeof(gives), sizeof(slug)};
        for (int k = 0; k < 5; k++) { const bf_json_node_t *v = bf_json_obj_get(d, c, keys[k]); if (v) bf_json_get_str_copy(v, dest[k], caps[k]); }
        fputs("<tr data-held=\"", stdout); html(slug); fputs("\"><td>", stdout); html(name); fputs("<small>", stdout); html(gives); fputs("</small></td><td>", stdout); html(*account ? account : "Connected"); fputs("</td><td>", stdout);
        html(!strcmp(customers, "usable") ? "Usable" : "Customer side not built yet"); fputs("</td></tr>", stdout);
    }
    fputs("</tbody></table></div></section>", stdout); bf_json_free(d);
}

/* The organization's own login for a container it holds, shown once on the
 * response page and never stored. Returns 1 with kind and access filled. */
/* The organization's own login for a container it holds, in one of the
 * enabled modes (reveal, scoped, wire). reveal and scoped return the login
 * for the response page to show once; wire writes it into the organization's
 * own Vercel project and returns only where it went. Nothing is stored. */
static int pa_access(const char *provider, const char *customer, const char *mode, char *kind, size_t kcap, char *access, size_t acap) {
    char out[4096], err[512]; kind[0] = access[0] = 0;
    char *const argv[] = {(char *)pa_binary(), "customer", "access", "--mode", (char *)mode, "--provider", (char *)provider, "--customer", (char *)customer, NULL};
    if (auth_call_split(argv, "", out, sizeof(out), err, sizeof(err)) != 0) return 0;
    bf_json_doc_t *d = bf_json_parse_str(out, NULL, 0); const bf_json_node_t *k = d ? bf_json_obj_get(d, bf_json_root(d), "kind") : NULL, *a = d ? bf_json_obj_get(d, bf_json_root(d), !strcmp(mode, "wire") ? "variable" : "access") : NULL;
    if (k) bf_json_get_str_copy(k, kind, kcap); if (a) bf_json_get_str_copy(a, access, acap); bf_json_free(d);
    memset(out, 0, sizeof(out));
    return kind[0] && access[0];
}
static int pa_mode_on(const char *mode) { const char *m = getenv("BONFYRE_PROVIDER_ACCESS_MODES"); return strstr(m && *m ? m : "reveal,scoped,wire", mode) != NULL; }
static int pa_has_access(const char *slug) { return !strcmp(slug, "neon") || !strcmp(slug, "sentry"); }

/* Every service's lookup runs at once, so the page waits for the slowest
 * provider instead of the sum of all of them. state[i]: 1 open, 0 none, -1 no
 * answer, and the provider id when open. */
static void pa_show_all(const char *customer, int *state, char ids[][128], int n) {
    int fd[16]; pid_t pid[16]; char buf[16][2048]; size_t got[16];
    for (int i = 0; i < n && i < 16; i++) {
        int p[2]; fd[i] = -1; pid[i] = -1; got[i] = 0; buf[i][0] = 0; state[i] = -1; ids[i][0] = 0;
        if (pipe(p)) continue;
        pid[i] = fork();
        if (pid[i] == 0) { int nul = open("/dev/null", O_RDWR); dup2(nul, 0); dup2(nul, 2); dup2(p[1], 1); close(p[0]);
            execl(pa_binary(), pa_binary(), "customer", "show", "--provider", pa_providers[i].slug, "--customer", customer, (char *)NULL); _exit(127); }
        close(p[1]); if (pid[i] < 0) { close(p[0]); continue; } fd[i] = p[0];
    }
    for (int i = 0; i < n && i < 16; i++) {
        if (fd[i] < 0) continue; ssize_t k;
        while ((k = read(fd[i], buf[i] + got[i], sizeof(buf[i]) - 1 - got[i])) > 0) { got[i] += (size_t)k; if (got[i] >= sizeof(buf[i]) - 1) break; }
        buf[i][got[i]] = 0; close(fd[i]); int st; waitpid(pid[i], &st, 0);
        if (strstr(buf[i], "\"state\":\"open\"")) { state[i] = 1; const char *k2 = strstr(buf[i], "\"id\":\""); if (k2) { k2 += 6; size_t m = strcspn(k2, "\""); if (m < 128) { memcpy(ids[i], k2, m); ids[i][m] = 0; } } }
        else if (strstr(buf[i], "\"state\":\"none\"")) state[i] = 0;
    }
}

static void pa_section(const char *org, const char *nonce) {
    char customer[41], id[128], ids[16][128]; int states[16], count = 0;
    if (!pa_customer(org, customer)) return;
    while (pa_providers[count].slug && count < 16) count++;
    pa_show_all(customer, states, ids, count);
    pa_held(org);
    fputs("<section data-provider-accounts><h2>Provider accounts</h2><p>This organization can have accounts of its own at outside services, run under Aurekai's provider accounts and closed from here. Closing removes the provider's copy.</p><div class=\"scroll\"><table><thead><tr><th>Service</th><th>This organization</th><th></th></tr></thead><tbody>", stdout);
    for (int i = 0; pa_providers[i].slug; i++) {
        const PaProvider *p = &pa_providers[i];
        /* Which services organizations are offered is the operator's setting;
         * unset offers every one that has been built. */
        const char *on = getenv("BONFYRE_PROVIDER_CUSTOMER_PROVIDERS"); char want[32]; snprintf(want, sizeof(want), ",%s,", p->slug); char list[512]; snprintf(list, sizeof(list), ",%s,", on ? on : "");
        if (on && *on && !strstr(list, want)) continue;
        int state = states[i]; snprintf(id, sizeof(id), "%s", ids[i]);
        fputs("<tr data-provider=\"", stdout); html(p->slug); fputs("\"><td>", stdout); html(p->name); fputs("<small>", stdout); html(p->gives); fputs("</small></td><td>", stdout);
        if (state < 0) fputs("Not answering right now", stdout);
        else if (state) { fputs("Open <small>", stdout); html(id); fputs("</small>", stdout); }
        else fputs("None yet", stdout);
        fputs("</td><td>", stdout);
        if (state >= 0) {
            fputs("<form method=\"post\" action=\"/organizations\" data-provider-form>", stdout);
            psetup_hidden("operation", state ? "provider-close" : "provider-open"); psetup_hidden("organization", org); psetup_hidden("provider", p->slug); psetup_hidden("nonce", nonce);
            fputs(state ? "<button>Close this account</button>" : "<button>Open an account</button>", stdout); fputs("</form>", stdout);
            if (state && pa_has_access(p->slug)) {
                static const char *modes[] = {"reveal", "scoped", "wire"}, *labels[] = {"Show the owner login once", "Make a new login and show it once", "Put a new login into this organization's Vercel project"};
                for (int m = 0; m < 3; m++) {
                    if (!pa_mode_on(modes[m])) continue;
                    fputs("<form method=\"post\" action=\"/organizations\" data-provider-access=\"", stdout); html(modes[m]); fputs("\">", stdout);
                    psetup_hidden("operation", "provider-access"); psetup_hidden("organization", org); psetup_hidden("provider", p->slug); psetup_hidden("mode", modes[m]); psetup_hidden("nonce", nonce);
                    fputs("<button>", stdout); html(labels[m]); fputs("</button></form>", stdout);
                }
            }
        }
        fputs("</td></tr>", stdout);
    }
    fputs("<tr data-provider=\"github\"><td>GitHub<small>access to repositories the organization chooses</small></td><td>Owned by the organization</td><td><a href=\"https://github.com/apps/aurekai-3b0542/installations/new\">Connect GitHub</a></td></tr>", stdout);
    fputs("</tbody></table></div></section>", stdout);
}
#endif
