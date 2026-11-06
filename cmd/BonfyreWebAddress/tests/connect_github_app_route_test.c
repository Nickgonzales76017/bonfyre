/* Native route regression witness. Compiles the production header against
 * deterministic auth/credential doubles, in an isolated HOME. No network calls,
 * credentials, customer accounts or production databases are touched.
 * This is not a browser test or a GitHub API integration test. */
#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int failures, assertions, auth_mode = 1, native_calls, list_mode;
static int observed_status, customer_calls, convert_calls;
static const char *fixture_subject = "fixture-owner";
static char home[PATH_MAX], key_path[PATH_MAX];
static void check(int condition, const char *message) {
    assertions++;
    if (!condition) { fprintf(stderr, "FAIL: %s\n", message); failures++; }
}
static void html(const char *s) {
    for (; s && *s; s++) {
        switch (*s) {
            case '&': fputs("&amp;", stdout); break;
            case '<': fputs("&lt;", stdout); break;
            case '>': fputs("&gt;", stdout); break;
            case '\"': fputs("&quot;", stdout); break;
            case '\'': fputs("&#39;", stdout); break;
            default: fputc(*s, stdout);
        }
    }
}
static int psetup_context(const char *bearer, const char *auth_db,
    const char *context, int write, char *actor, size_t actor_cap,
    char *subject, size_t subject_cap, const char **scope) {
    (void)bearer; (void)auth_db; (void)context; (void)write;
    snprintf(actor, actor_cap, "%s", "user:fixture");
    snprintf(subject, subject_cap, "%s", fixture_subject);
    *scope = "self";
    return auth_mode;
}
static void psetup_start(int code, const char *title) {
    observed_status = code; fputs("<html><title>", stdout);
    html(title); fputs("</title><body>", stdout);
}
static void psetup_end(void) { fputs("</body></html>", stdout); }
static void aw_error(int code, const char *message) {
    observed_status = code; html(message);
}
static int query_value(const char *query, const char *name, char *out, size_t cap) {
    size_t len = strlen(name);
    out[0] = '\0';
    for (const char *p = query; p && *p; ) {
        const char *end = strchr(p, '&');
        size_t n = end ? (size_t)(end - p) : strlen(p);
        if (n > len && !strncmp(p, name, len) && p[len] == '=') {
            n -= len + 1; if (n >= cap) return 0;
            memcpy(out, p + len + 1, n); out[n] = '\0'; return 1;
        }
        p = end ? end + 1 : NULL;
    }
    return 0;
}
static int aw_native_result(char **args, const void *input, size_t input_size,
    unsigned char **out, size_t *size, int quiet) {
    (void)input; (void)input_size; (void)quiet;
    const char *body = NULL;
    native_calls++; *out = NULL; *size = 0;
    if (!strcmp(args[1], "github-app-installations")) {
        if (list_mode == 1) return 1;
        body = list_mode == 2 ? "{\"installations\":[],\"app_id\":4242}" :
            "{\"installations\":[{\"id\":77,\"account\":\"customer-org\",\"type\":\"Organization\",\"repositories\":\"selected\"}],\"app_id\":4242}";
    } else if (!strcmp(args[1], "github-app-verify")) {
        body = "{\"verified\":true,\"app_id\":4242,\"slug\":\"aurekai-fixture\",\"status\":200}";
    } else if (!strcmp(args[1], "github-app-customer")) {
        customer_calls++;
        check(!strcmp(args[2], "--installation") && !strcmp(args[3], "77"), "exact installation passed to credential owner");
        body = "{\"installation\":77,\"token_issued\":true,\"repositories\":1,\"names\":[\"customer-org/site\"],\"token\":\"DO_NOT_RENDER_FIXTURE_TOKEN\"}";
    } else if (!strcmp(args[1], "github-app-convert")) {
        convert_calls++;
        body = "{\"stored\":true,\"app_id\":4242,\"slug\":\"aurekai-fixture\"}";
    } else {
        check(0, "unexpected credential command"); return 1;
    }
    *out = (unsigned char *)strdup(body);
    if (!*out) { perror("strdup"); exit(2); }
    *size = strlen(body); return 0;
}

#ifndef CGA_HEADER
#define CGA_HEADER "../src/connect_github_app.h"
#endif
#include CGA_HEADER

static char *render(const char *method, const char *target, const char *query, int *handled) {
    FILE *capture = tmpfile();
    if (!capture) { perror("tmpfile"); exit(2); }
    fflush(stdout); int saved = dup(STDOUT_FILENO);
    if (saved < 0 || dup2(fileno(capture), STDOUT_FILENO) < 0) { perror("dup2"); exit(2); }
    observed_status = native_calls = customer_calls = convert_calls = 0;
    *handled = connect_github_app(method, target, query, "aurekai.work", "fixture", "fixture-auth");
    fflush(stdout);
    if (dup2(saved, STDOUT_FILENO) < 0) { perror("restore"); exit(2); }
    close(saved);
    if (fseek(capture, 0, SEEK_END)) exit(2);
    long n = ftell(capture); if (n < 0) exit(2); rewind(capture);
    char *result = calloc((size_t)n + 1, 1);
    if (!result || fread(result, 1, (size_t)n, capture) != (size_t)n) exit(2);
    fclose(capture); return result;
}
static void clean_fixture(void) {
    char dir[PATH_MAX], path[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/.bonfyre/state/connect", home);
    DIR *d = opendir(dir);
    if (d) { struct dirent *e; while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        snprintf(path, sizeof(path), "%s/%s", dir, e->d_name); unlink(path);
    } closedir(d); }
    rmdir(dir); snprintf(dir, sizeof(dir), "%s/.bonfyre/state", home); rmdir(dir);
    snprintf(dir, sizeof(dir), "%s/.bonfyre/credentials", home); rmdir(dir);
    snprintf(dir, sizeof(dir), "%s/.bonfyre", home); rmdir(dir);
    unlink(key_path); rmdir(home);
}
int main(void) {
    char pattern[] = "/tmp/bonfyre-cga-route-XXXXXX";
    char *tmp = mkdtemp(pattern); if (!tmp) { perror("mkdtemp"); return 2; }
    snprintf(home, sizeof(home), "%s", tmp);
    snprintf(key_path, sizeof(key_path), "%s/fixture.key", home);
    int fd = open(key_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (fd < 0) { perror("fixture key"); return 2; } close(fd);
    atexit(clean_fixture);
    setenv("HOME", home, 1); setenv("BONFYRE_ACCOUNT_WORK_OWNER", "fixture-owner", 1);
    setenv("BONFYRE_CREDENTIALS_BINARY", access("/usr/bin/true", X_OK) == 0 ? "/usr/bin/true" : "/bin/true", 1);
    setenv("BONFYRE_CREDENTIALS_KEY", key_path, 1);
    unsetenv("BONFYRE_CREDENTIALS_DB"); unsetenv("BONFYRE_GITHUB_API"); unsetenv("BONFYRE_GITHUB_WEB");
    int handled = 0; char *body;
    auth_mode = 0;
    body = render("GET", "/connect/github-app/customers", "", &handled);
    check(handled && strstr(body, "HTTP/1.1 303"), "guest redirected");
    check(strstr(body, "Location: /login?next=%2Fconnect%2Fgithub-app%2Fcustomers\r\n") != NULL, "customer destination survives sign-in");
    check(strstr(body, "Cache-Control: private, no-store") != NULL && !native_calls, "guest reads no credentials"); free(body);
    body = render("GET", "/connect/github-app/done", "code=private-code", &handled);
    check(strstr(body, "next=%2Fconnect%2Fgithub-app\r\n") != NULL && !strstr(body, "private-code"), "guest callback restarts without forwarding code"); free(body);
    auth_mode = 1; fixture_subject = "other-account";
    body = render("GET", "/connect/github-app/customers", "", &handled);
    check(observed_status == 403 && !native_calls, "other account refused before credential access"); free(body);
    fixture_subject = "fixture-owner";
    body = render("POST", "/connect/github-app/customers", "", &handled);
    check(observed_status == 405 && !native_calls, "unsupported method has no credential effects"); free(body);
    body = render("GET", "/unrelated", "", &handled);
    check(!handled && !native_calls, "unrelated route is not claimed"); free(body);
    list_mode = 1;
    body = render("GET", "/connect/github-app/customers", "", &handled);
    check(observed_status == 503, "installation read failure is a service failure, not successful empty state");
    check(!strstr(body, "No GitHub app is connected yet") && strstr(body, "Stored credentials were not changed"), "failure does not invent disconnection");
    check(native_calls == 1 && !customer_calls && !convert_calls, "failure does not recreate app or issue tokens");
    check(strstr(body, "Retry customer list") != NULL && !strstr(body, ">Connect GitHub<"), "failure retries listing instead of asking for a replacement app"); free(body);
    list_mode = 2;
    body = render("GET", "/connect/github-app/customers", "", &handled);
    check(observed_status == 200 && strstr(body, "No customer has installed"), "confirmed empty installation list stays an empty state");
    check(!customer_calls && !convert_calls, "empty installation list issues no customer token"); free(body);
    list_mode = 0;
    body = render("GET", "/connect/github-app/customers", "", &handled);
    check(observed_status == 200 && strstr(body, "data-installation=\"77\""), "owner sees exact fixture installation");
    check(!customer_calls && strstr(body, "?check=77") != NULL, "listing installations does not mint a customer token");
    check(!strstr(body, "data-bind-installation") && !strstr(body, "workspace-store.mjs"), "unchecked installation cannot be associated to an Aurekai account");
    check(strstr(body, "data-github-install-boundary") != NULL && strstr(body, "Existing apps keep their current GitHub visibility"), "existing app visibility is not inferred or changed");
    free(body);
    body = render("GET", "/connect/github-app/customers", "check=77", &handled);
    check(customer_calls == 1 && strstr(body, "customer-org/site") != NULL && !strstr(body, "DO_NOT_RENDER_FIXTURE_TOKEN"), "explicit access check displays repository result without credential token");
    check(strstr(body, "data-bind-installation") != NULL && strstr(body, "data-repository-count=\"1\"") != NULL, "checked installation can be bound to a writable Aurekai destination");
    check(strstr(body, "workspace-destinations.mjs") != NULL && strstr(body, "workspace-store.mjs") != NULL && strstr(body, "connections:provider-tenancy:github:") != NULL, "binding reuses destination discovery and the native workspace store");
    check(strstr(body, "provider-access-verified-operator-bound") != NULL && strstr(body, "lifecycle:'active'") != NULL && strstr(body, "GitHub did not attest Aurekai customer identity") != NULL, "operator binding stays explicit without inventing provider identity attestation");
    check(strstr(body, "bonfyre-credentials:github-app") != NULL && strstr(body, "Every effect must revalidate current destination write access and the installation") != NULL, "binding retains the credential owner reference and mandatory effect-time revalidation");
    free(body);
    body = render("GET", "/connect/github-app/customers", "check=77;x", &handled);
    check(observed_status == 400 && !customer_calls, "malformed explicit access check is refused before token minting"); free(body);
    body = render("GET", "/connect/github-app/customers", "check=99", &handled);
    check(observed_status == 404 && !customer_calls, "unknown installation is refused before token minting"); free(body);
    body = render("GET", "/connect/github-app", "", &handled);
    check(observed_status == 200 && strstr(body, "&quot;public&quot;:true") != NULL, "new manifest admits installation by other accounts");
    check(strstr(body, "Other accounts will be able to install") != NULL, "visibility explained before provider submission");
    check(strstr(body, "&quot;contents&quot;:&quot;read&quot;") != NULL && strstr(body, "&quot;issues&quot;:&quot;write&quot;") != NULL, "existing manifest permissions unchanged");
    check(!native_calls, "starting the flow does not exchange credentials or create a remote app"); free(body);
    body = render("GET", "/connect/github-app/done", "code=good-code&state=ffffffffffffffffffffffffffffffff", &handled);
    check(observed_status == 409 && !native_calls, "unknown callback state refused"); free(body);
    printf("GitHub route witness: %d assertions, %d failures\n", assertions, failures);
    return failures ? 1 : 0;
}
