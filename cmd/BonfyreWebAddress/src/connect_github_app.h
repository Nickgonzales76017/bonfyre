/* Connect GitHub as an app, with one approval on github.com and nothing typed.
 *
 *   GET /connect/github-app        the estate owner is sent to GitHub with an app
 *                                  manifest (name, permissions, return address)
 *   GET /connect/github-app/done   GitHub returns a one-time code; BonfyreCredentials
 *                                  exchanges it, seals the app's key and secrets,
 *                                  and proves the key by asking GitHub as the app
 *
 * Only the account named BONFYRE_ACCOUNT_WORK_OWNER may connect, because the
 * credentials are the estate's own. The state is one random value per attempt,
 * kept as a file bound to the session's actor for fifteen minutes and removed on
 * use; a return without it, late, or for another actor changes nothing. */
#ifndef BONFYRE_CONNECT_GITHUB_APP_H
#define BONFYRE_CONNECT_GITHUB_APP_H

static int cga_owner(const char *bearer, const char *auth_db, char actor[160]) {
    char subject[160]; const char *scope = NULL, *owner = getenv("BONFYRE_ACCOUNT_WORK_OWNER");
    int access = psetup_context(bearer, auth_db, "self", 0, actor, 160, subject, sizeof(subject), &scope);
    if (access != 1) return access == 0 ? 0 : -1;
    return owner && *owner && !strcmp(subject, owner) ? 1 : -1;
}
static int cga_state_dir(char *out, size_t cap) {
    const char *home = getenv("HOME"); char base[PATH_MAX];
    if (!home || *home != '/' || snprintf(base, sizeof(base), "%s/.bonfyre", home) >= (int)sizeof(base)) return 0;
    mkdir(base, 0700);
    if (snprintf(base, sizeof(base), "%s/.bonfyre/state", home) >= (int)sizeof(base)) return 0;
    mkdir(base, 0700);
    if (snprintf(out, cap, "%s/connect", base) >= (int)cap) return 0;
    mkdir(out, 0700); return 1;
}
static int cga_credentials(char *bin, char *db, char *key) {
    const char *b = getenv("BONFYRE_CREDENTIALS_BINARY"), *d = getenv("BONFYRE_CREDENTIALS_DB"), *k = getenv("BONFYRE_CREDENTIALS_KEY"), *home = getenv("HOME");
    char dir[PATH_MAX]; struct stat st;
    if (!b || *b != '/' || access(b, X_OK) || !home || *home != '/') return 0;
    snprintf(bin, PATH_MAX, "%s", b);
    if (snprintf(dir, sizeof(dir), "%s/.bonfyre", home) >= (int)sizeof(dir)) return 0;
    mkdir(dir, 0700);
    if (snprintf(dir, sizeof(dir), "%s/.bonfyre/credentials", home) >= (int)sizeof(dir)) return 0;
    mkdir(dir, 0700);
    snprintf(db, PATH_MAX, "%s", d && *d == '/' ? d : ""); if (!*db) snprintf(db, PATH_MAX, "%s/credentials.db", dir);
    snprintf(key, PATH_MAX, "%s", k && *k == '/' ? k : ""); if (!*key) snprintf(key, PATH_MAX, "%s/credentials.key", dir);
    if (stat(key, &st)) { char *args[] = {bin, "init-key", "--key", key, NULL}; unsigned char *out = NULL; size_t n = 0; aw_native_result(args, NULL, 0, &out, &n, 0); free(out); }
    return !stat(key, &st);
}
static void cga_page(int code, const char *title, const char *lead) {
    psetup_start(code, title);
    fputs("<style>.cga{max-width:720px;margin-top:28px}.cga h1{margin-bottom:8px}.cga p{line-height:1.6}.cga .cga-go{font:inherit;border:0;border-radius:999px;padding:12px 20px;cursor:pointer;background:currentColor}.cga .cga-go span{filter:invert(1)}.cga dl{display:grid;grid-template-columns:auto 1fr;gap:6px 18px}.cga dt{opacity:.7}</style><section class=\"cga\"><h1>", stdout);
    html(title); fputs("</h1><p>", stdout); html(lead); fputs("</p>", stdout);
}
static void cga_tenancy_script(void) {
    fputs(
      "<section class=\"cga-tenancy\" data-provider-tenancy><h2>Use this installation for an Aurekai account</h2>"
      "<p>Choose the Aurekai account this verified GitHub installation belongs with. This is an internal operator binding, not GitHub attesting customer identity. "
      "The app key stays in BonfyreCredentials; every later operation must re-check both the destination and GitHub access.</p>"
      "<label>Aurekai account <select data-tenancy-destination><option>Reading current accounts…</option></select></label>"
      "<p data-tenancy-status role=\"status\"></p></section>"
      "<script type=\"module\">"
      "import{readWorkspaceDestinations,readWorkspacePacket}from'/apps/workspace-destinations.mjs';"
      "import{openDestinationStore}from'/apps/workspace-store.mjs';"
      "const root=document.querySelector('[data-provider-tenancy]'),pick=root.querySelector('[data-tenancy-destination]'),status=root.querySelector('[data-tenancy-status]'),pack='experience.connection-studio.workspace';"
      "const say=x=>status.textContent=x;"
      "async function binding(context){const p=await readWorkspacePacket('/apps/_bindings?'+new URLSearchParams({context,pack}));"
      "const scope=context==='self'?'self':context.split(':')[0];"
      "if(!Array.isArray(p)||p.length!==4||p[0]!==1||p[2]!==scope||!Array.isArray(p[3]))throw Error('Connection Studio returned a different account.');"
      "const rows=p[3].filter(r=>Array.isArray(r)&&r.length===6&&r[1]===pack&&r[5]===true);"
      "if(rows.length!==1)throw Error('Enable one current Connection Studio in this account first.');return rows[0][0];}"
      "try{const ds=(await readWorkspaceDestinations()).filter(d=>d.canWrite);pick.textContent='';"
      "for(const d of ds){const o=document.createElement('option');o.value=d.context;o.textContent=d.label;pick.append(o);}"
      "if(!ds.length){pick.disabled=true;say('No writable Aurekai account is available.');}}catch(e){pick.disabled=true;say(e.message);}"
      "for(const b of document.querySelectorAll('[data-bind-installation]'))b.addEventListener('click',async()=>{"
      "if(b.disabled||pick.disabled)return;b.disabled=true;say('Checking the destination and saving this installation…');"
      "try{const context=pick.value,id=b.dataset.installation;if(!/^\\d{1,20}$/.test(id))throw Error('The GitHub installation identity changed.');"
      "const ws=await binding(context),store=openDestinationStore('/apps/'+ws+'/_records'),key='connections:provider-tenancy:github:'+id,prior=await store.readVersion(key);"
      "if(prior?.deleted)throw Error('This tenancy record was removed. Review it before restoring it.');"
      "const value={version:1,provider:'github',kind:'app-installation-binding',installation:id,remoteAccount:b.dataset.account,remoteAccountType:b.dataset.accountType,repositorySelection:b.dataset.repositorySelection,"
      "reachableRepositoryCount:Number(b.dataset.repositoryCount),capabilities:{metadata:'read',contents:'read',issues:'write',pullRequests:'write'},credentialRef:'bonfyre-credentials:github-app',"
      "verification:{state:'provider-access-verified-operator-bound',observedAt:new Date().toISOString()},lifecycle:'active',"
      "cannotInfer:'This is an Aurekai operator assignment after an exact GitHub installation access check; GitHub did not attest Aurekai customer identity. Every effect must revalidate current destination write access and the installation before acting.'};"
      "if(!Number.isSafeInteger(value.reachableRepositoryCount)||value.reachableRepositoryCount<0)throw Error('The provider readback changed.');"
      "await store.saveLocal(key,value,prior?.revision||0);b.textContent='Bound to '+context;say('Bound the verified GitHub installation to '+context+'. Later operations still re-check the destination and GitHub before acting.');"
      "}catch(e){say(e.message);}finally{b.disabled=false;}});"
      "</script>", stdout);
}
static int connect_github_app(const char *method, const char *target, const char *query, const char *host, const char *bearer, const char *auth_db) {
    int start = !strcmp(target, "/connect/github-app"), done = !strcmp(target, "/connect/github-app/done"), customers = !strcmp(target, "/connect/github-app/customers");
    char actor[160], dir[PATH_MAX], path[PATH_MAX], state[65];
    if (!start && !done && !customers) return 0;
    if (strcmp(method, "GET")) { aw_error(405, "Open this from Aurekai.\n"); return 1; }
    int owner = cga_owner(bearer, auth_db, actor);
    if (owner == 0) {
        fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=%2Fconnect%2Fgithub-app", stdout);
        if (customers) fputs("%2Fcustomers", stdout);
        fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", stdout); return 1;
    }
    if (owner < 0) { aw_error(403, "Only the estate owner can connect the estate's GitHub app.\n"); return 1; }
    if (customers) {
        /* Listing installations does not mint customer tokens. An explicit
         * check mints one installation's bounded token and reads only what
         * that customer shared. */
        char bin[PATH_MAX], db[PATH_MAX], key[PATH_MAX], slug[128] = "", check_id[24] = "", validated_id[24] = ""; const char *api = getenv("BONFYRE_GITHUB_API");
        unsigned char *list = NULL; size_t size = 0; int n = 0, bindable = 0;
        if (query && *query && (!query_value(query, "check", check_id, sizeof(check_id)) || !*check_id || strspn(check_id, "0123456789") != strlen(check_id) || strlen(check_id) > 20 || strlen(query) != strlen(check_id) + 6)) {
            aw_error(400, "Choose one GitHub installation to check.\n"); return 1;
        }
        if (check_id[0]) { snprintf(validated_id, sizeof(validated_id), "%s", check_id); check_id[0] = 0; }
        if (!cga_credentials(bin, db, key)) { aw_error(503, "The credential store is not available here.\n"); return 1; }
        char *la[12] = {bin, "github-app-installations", "--db", db, "--key", key, "--actor", "github-customers"}; int k = 8;
        if (api && *api) { la[k++] = "--api"; la[k++] = (char *)api; } la[k] = NULL;
        if (aw_native_result(la, NULL, 0, &list, &size, 0) || !list) { free(list); cga_page(503, "GitHub customers unavailable", "The app installations could not be read. This does not mean the app is disconnected. Stored credentials were not changed."); fputs("<p><a href=\"/connect/github-app/customers\">Retry customer list</a></p></section>", stdout); psetup_end(); return 1; }
        if (validated_id[0]) {
            char plain[40];
            snprintf(plain, sizeof(plain), "%cid%c:%s,", 34, 34, validated_id);
            if (!strstr((const char *)list, plain)) { free(list); aw_error(404, "That GitHub installation is not available to this app.\n"); return 1; }
        }
        { char *vb[12] = {bin, "github-app-verify", "--db", db, "--key", key, "--actor", "github-customers"}; unsigned char *v = NULL; size_t vs = 0; int j = 8;
          if (api && *api) { vb[j++] = "--api"; vb[j++] = (char *)api; } vb[j] = NULL;
          if (!aw_native_result(vb, NULL, 0, &v, &vs, 1) && v) { const char *sl = strstr((const char *)v, "\"slug\":\""); if (sl) sscanf(sl + 8, "%127[a-z0-9-]", slug); } free(v); }
        cga_page(200, "GitHub customers", "Installed GitHub accounts and what each one chose to share. Check access explicitly before recording a candidate association.");
        if (slug[0]) { fputs("<p>GitHub installation link: <a data-install-link href=\"https://github.com/apps/", stdout); html(slug); fputs("/installations/new\">https://github.com/apps/", stdout); html(slug); fputs("/installations/new</a></p>", stdout); }
        fputs("<p data-github-install-boundary>Other accounts can install only if this app is public on GitHub. Installing on your own account does not prove customer onboarding. Existing apps keep their current GitHub visibility.</p>", stdout);
        fputs("<table class=\"price-table\" data-github-customers><thead><tr><th scope=\"col\">Customer</th><th scope=\"col\">Shared</th><th scope=\"col\">Access check</th></tr></thead><tbody>", stdout);
        for (const char *p = strstr((const char *)list, "{\"id\":"); p; p = strstr(p + 1, "{\"id\":")) {
            char id[24] = "", account[128] = "", type[32] = "", sel[16] = "";
            sscanf(p, "{\"id\":%23[0-9],\"account\":\"%127[^\"]\",\"type\":\"%31[^\"]\",\"repositories\":\"%15[^\"]\"", id, account, type, sel);
            if (!id[0]) continue;
            int requested = validated_id[0] && !strcmp(validated_id, id), ok = 0, count = 0; unsigned char *r = NULL; size_t rs = 0;
            if (requested) {
                char *cb[14] = {bin, "github-app-customer", "--installation", id, "--db", db, "--key", key, "--actor", "github-customers"}; int c = 10;
                if (api && *api) { cb[c++] = "--api"; cb[c++] = (char *)api; } cb[c] = NULL;
                ok = !aw_native_result(cb, NULL, 0, &r, &rs, 0) && r; bindable += ok;
            }
            fputs("<tr data-installation=\"", stdout); html(id); fputs("\"><th scope=\"row\">", stdout); html(account); fputs(" <small>", stdout); html(type); fputs("</small></th><td>", stdout);
            html(!strcmp(sel, "all") ? "all repositories" : "chosen repositories"); fputs("</td><td>", stdout);
            if (ok) { const char *names = strstr((const char *)r, "\"names\":["); const char *cn = strstr((const char *)r, "\"repositories\":"); if (cn) count = atoi(cn + 15);
                printf("%d repositor%s", count, count == 1 ? "y" : "ies");
                if (names) { char list_names[600] = ""; sscanf(names + 9, "%599[^]]", list_names); for (char *q = list_names; *q; q++) if (*q == '"') *q = ' '; fputs(": ", stdout); html(list_names); } }
            else if (requested) fputs("access refused by GitHub", stdout);
            else { fputs("Not checked this visit. <a data-check-installation href=\"/connect/github-app/customers?check=", stdout); html(id); fputs("\">Check access</a>", stdout); }
            fputs("</td></tr>", stdout);
            if (ok) {
                fputs("<tr class=\"cga-bind\"><td colspan=\"3\"><button type=\"button\" data-bind-installation data-installation=\"", stdout); html(id);
                fputs("\" data-account=\"", stdout); html(account); fputs("\" data-account-type=\"", stdout); html(type);
                fputs("\" data-repository-selection=\"", stdout); html(sel);
                printf("\" data-repository-count=\"%d\">Use for selected Aurekai account</button></td></tr>", count);
            }
            free(r); n++;
        }
        if (!n) fputs("<tr><td colspan=\"3\">No customer has installed the app yet.</td></tr>", stdout);
        fputs("</tbody></table>", stdout); if (bindable) cga_tenancy_script(); fputs("<p><a href=\"/work-board?context=self\">Back to the work board</a></p></section>", stdout);
        free(list); psetup_end(); return 1;
    }
    if (!cga_state_dir(dir, sizeof(dir))) { aw_error(503, "Connections are not configured here.\n"); return 1; }
    if (start) {
        unsigned char raw[16]; int fd = open("/dev/urandom", O_RDONLY); char name[48], redirect[300]; FILE *f;
        if (fd < 0 || read(fd, raw, sizeof(raw)) != (ssize_t)sizeof(raw)) { if (fd >= 0) close(fd); aw_error(503, "No randomness available.\n"); return 1; }
        close(fd);
        for (int i = 0; i < 16; i++) snprintf(state + i * 2, 3, "%02x", raw[i]);
        if (snprintf(path, sizeof(path), "%s/%s", dir, state) >= (int)sizeof(path) || !(f = fopen(path, "wx"))) { aw_error(503, "Could not start the connection.\n"); return 1; }
        fprintf(f, "%s\n%lld\n", actor, (long long)time(NULL) + 900); fclose(f); chmod(path, 0600);
        /* GitHub app names are global; a short suffix keeps the estate's unique. */
        snprintf(name, sizeof(name), "Aurekai %.6s", state + 26);
        snprintf(redirect, sizeof(redirect), "https://%s/connect/github-app/done", host && *host && !strchr(host, '"') ? host : "aurekai.work");
        const char *web = getenv("BONFYRE_GITHUB_WEB"); if (!web || strncmp(web, "http", 4)) web = "https://github.com";
        cga_page(200, "Connect GitHub", "GitHub will show the Aurekai app it is about to create: its name, what it may read and change, and where it returns. Other accounts will be able to install this new app, with their own approval and repository selection. Press Create there. Aurekai then keeps the app's key sealed and checks it works. Nothing is typed.");
        fputs("<form method=\"post\" data-cga action=\"", stdout); html(web); fputs("/settings/apps/new?state=", stdout); html(state); fputs("\"><input type=\"hidden\" name=\"manifest\" value=\"", stdout);
        char manifest[1400];
        snprintf(manifest, sizeof(manifest),
            "{\"name\":\"%s\",\"url\":\"https://aurekai.work\",\"redirect_url\":\"%s\",\"public\":true,"
            "\"hook_attributes\":{\"url\":\"https://aurekai.work/\",\"active\":false},"
            "\"default_permissions\":{\"metadata\":\"read\",\"contents\":\"read\",\"issues\":\"write\",\"pull_requests\":\"write\"},"
            "\"description\":\"Aurekai operates repositories it is installed on: reads code and opens issues and pull requests.\"}", name, redirect);
        html(manifest);
        fputs("\"><dl><dt>App</dt><dd>", stdout); html(name);
        fputs("</dd><dt>May read</dt><dd>repository contents and metadata</dd><dt>May change</dt><dd>issues and pull requests</dd><dt>Installed on</dt><dd>only the repositories you choose next</dd></dl>"
              "<p><button class=\"cga-go\" type=\"submit\"><span>Continue to GitHub</span></button></p></form></section>", stdout);
        psetup_end(); return 1;
    }
    {
        char code[128] = "", got[80] = "", saved_actor[160] = "", bin[PATH_MAX], db[PATH_MAX], key[PATH_MAX]; long long expires = 0; FILE *f;
        if (!query || !*query) { aw_error(400, "GitHub did not return a code.\n"); return 1; }
        query_value(query, "code", code, sizeof(code)); query_value(query, "state", got, sizeof(got));
        if (strlen(got) != 32 || strspn(got, "0123456789abcdef") != 32 || !*code || strspn(code, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != strlen(code)) { aw_error(400, "That return is not one Aurekai started.\n"); return 1; }
        snprintf(path, sizeof(path), "%s/%s", dir, got);
        if (!(f = fopen(path, "r"))) { aw_error(409, "That connection attempt is unknown or already used. Start again from Aurekai.\n"); return 1; }
        if (!fgets(saved_actor, sizeof(saved_actor), f) || fscanf(f, "%lld", &expires) != 1) expires = 0;
        fclose(f); unlink(path); saved_actor[strcspn(saved_actor, "\n")] = 0;
        if (strcmp(saved_actor, actor) || expires < (long long)time(NULL)) { aw_error(409, "That connection attempt expired or belongs to another session. Start again from Aurekai.\n"); return 1; }
        if (!cga_credentials(bin, db, key)) { aw_error(503, "The credential store is not available here.\n"); return 1; }
        const char *api = getenv("BONFYRE_GITHUB_API");
        char *convert[16] = {bin, "github-app-convert", "--db", db, "--key", key, "--actor", "connect-github-app"}; int n = 8;
        if (api && *api) { convert[n++] = "--api"; convert[n++] = (char *)api; } convert[n] = NULL;
        unsigned char *out = NULL, *proof = NULL; size_t size = 0, proof_size = 0;
        int rc = aw_native_result(convert, code, strlen(code), &out, &size, 0);
        memset(code, 0, sizeof(code));
        if (rc || !out) { free(out); cga_page(502, "GitHub did not hand over the app", "GitHub refused the one-time code, or it was already used. Nothing was stored. Start again from Aurekai."); fputs("<p><a href=\"/connect/github-app\">Start again</a></p></section>", stdout); psetup_end(); return 1; }
        char *verify[16] = {bin, "github-app-verify", "--db", db, "--key", key, "--actor", "connect-github-app"}; n = 8;
        if (api && *api) { verify[n++] = "--api"; verify[n++] = (char *)api; } verify[n] = NULL;
        rc = aw_native_result(verify, NULL, 0, &proof, &proof_size, 1);
        char slug[128] = "", app_id[32] = ""; const char *s = strstr((const char *)out, "\"slug\":\""), *i = strstr((const char *)out, "\"app_id\":");
        if (s) sscanf(s + 8, "%127[^\"]", slug);
        if (i) sscanf(i + 9, "%31[0-9]", app_id);
        int verified = !rc && proof && strstr((const char *)proof, "\"verified\":true");
        cga_page(200, verified ? "GitHub is connected" : "GitHub app stored, not yet proven",
                 verified ? "The app exists, its key is sealed in the estate's credential store, and GitHub accepted a request signed with it just now."
                          : "The app was created and its key sealed, but GitHub did not accept a request signed with it. Try the check again from the work board.");
        fputs("<dl data-github-app><dt>App</dt><dd>", stdout); html(slug); fputs("</dd><dt>App id</dt><dd>", stdout); html(app_id);
        fputs("</dd><dt>Checked</dt><dd>", stdout); fputs(verified ? "accepted by GitHub" : "not accepted", stdout); fputs("</dd></dl>", stdout);
        if (slug[0] && strspn(slug, "abcdefghijklmnopqrstuvwxyz0123456789-") == strlen(slug)) {
            fputs("<p>Next: choose which repositories it works on. <a class=\"cga-install\" rel=\"noopener\" href=\"https://github.com/apps/", stdout); html(slug); fputs("/installations/new\">Install on repositories</a></p>", stdout);
        }
        fputs("<p><a href=\"/connect/github-app/customers\">GitHub customers</a> · <a href=\"/work-board?context=self\">Back to the work board</a></p></section>", stdout);
        free(out); free(proof); psetup_end(); return 1;
    }
}
#endif
