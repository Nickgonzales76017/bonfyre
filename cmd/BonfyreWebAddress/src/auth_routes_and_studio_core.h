#ifndef BONFYRE_AUTH_ROUTES_AND_STUDIO_CORE_H
#define BONFYRE_AUTH_ROUTES_AND_STUDIO_CORE_H
/* Extracted verbatim from main.c lines 2780-4027 (mechanical split, no logic changes). */
static int valid_token(const char *t) {
    if (!t || strncmp(t, "bfy_", 4)) return 0;
    const char *h = t + 4;
    size_t n = strlen(h);
    if (n != 64) return 0;
    for (size_t i = 0; i < n; i++)
        if (!((h[i]>='0'&&h[i]<='9')||(h[i]>='a'&&h[i]<='f'))) return 0;
    return 1;
}

/* True when the auth binary can actually be executed. Same discipline as the
 * file-backed surfaces: never advertise what cannot be served. */
static int auth_reachable(void) {
    const char *e = getenv("BONFYRE_AUTH_BINARY");
    if (e && *e) return access(e, X_OK) == 0;
    const char *path = getenv("PATH");
    if (!path) return 0;
    char buf[PATH_MAX];
    for (const char *p = path; *p; ) {
        const char *start = p;
        while (*p && *p != ':') p++;
        size_t len = (size_t)(p - start);
        if (len && len + 14 < sizeof(buf)) {
            memcpy(buf, start, len);
            snprintf(buf + len, sizeof(buf) - len, "/bonfyre-auth");
            if (access(buf, X_OK) == 0) return 1;
        }
        if (*p == ':') p++;
    }
    return 0;
}

static const char *auth_binary(void) {
    const char *e = getenv("BONFYRE_AUTH_BINARY");
    return (e && *e) ? e : "bonfyre-auth";
}

/* Run BonfyreAuth with argv, feed `body` on stdin, capture stdout. */
/* Run an organ, keeping stdout and stderr apart.
 *
 * auth_call() folds stderr into stdout so that organs which print their usage
 * there still produce a visible body. That is wrong for an organ whose stdout
 * is a machine format: bonfyre-query announces "[query] Running SQL on ..." on
 * stderr, and merging it prefixed the JSON with a line that made the response
 * invalid under its own Content-Type. Callers that need parseable output read
 * stdout, and fall back to stderr only to report a failure. */
static int auth_call_split(char *const argv[], const char *body,
                           char *out, size_t out_sz,
                           char *errbuf, size_t err_sz) {
    int in_pipe[2], out_pipe[2], err_pipe[2];
    if (pipe(in_pipe)) return -1;
    if (pipe(out_pipe)) { close(in_pipe[0]); close(in_pipe[1]); return -1; }
    if (pipe(err_pipe)) { close(in_pipe[0]); close(in_pipe[1]);
                          close(out_pipe[0]); close(out_pipe[1]); return -1; }
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        dup2(err_pipe[1], STDERR_FILENO);
        close(in_pipe[1]); close(out_pipe[0]); close(err_pipe[0]);
        execvp(argv[0], argv);
        _exit(127);
    }
    close(in_pipe[0]); close(out_pipe[1]); close(err_pipe[1]);
    if (body && *body) {
        ssize_t ignored = write(in_pipe[1], body, strlen(body));
        (void)ignored;
    }
    close(in_pipe[1]);
    /* Drain both pipes without blocking on either: a child that fills the pipe
     * we are not reading would deadlock against a strictly sequential read. */
    {
        int fds[2] = { out_pipe[0], err_pipe[0] };
        char *bufs[2] = { out, errbuf };
        size_t caps[2] = { out_sz, err_sz }, used[2] = { 0, 0 };
        int open_count = 2;
        for (int i = 0; i < 2; i++) {
            int fl = fcntl(fds[i], F_GETFL, 0);
            if (fl != -1) fcntl(fds[i], F_SETFL, fl | O_NONBLOCK);
        }
        while (open_count > 0) {
            fd_set rd; FD_ZERO(&rd);
            int maxfd = -1;
            for (int i = 0; i < 2; i++)
                if (fds[i] >= 0) { FD_SET(fds[i], &rd); if (fds[i] > maxfd) maxfd = fds[i]; }
            if (maxfd < 0) break;
            if (select(maxfd + 1, &rd, NULL, NULL, NULL) < 0) { if (errno == EINTR) continue; break; }
            for (int i = 0; i < 2; i++) {
                if (fds[i] < 0 || !FD_ISSET(fds[i], &rd)) continue;
                if (used[i] + 1 >= caps[i]) { close(fds[i]); fds[i] = -1; open_count--; continue; }
                ssize_t n = read(fds[i], bufs[i] + used[i], caps[i] - used[i] - 1);
                if (n > 0) used[i] += (size_t)n;
                else if (n == 0) { close(fds[i]); fds[i] = -1; open_count--; }
                else if (errno != EAGAIN && errno != EINTR) { close(fds[i]); fds[i] = -1; open_count--; }
            }
        }
        for (int i = 0; i < 2; i++) { if (fds[i] >= 0) close(fds[i]); bufs[i][used[i]] = '\0'; }
    }
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static int auth_call(char *const argv[], const char *body, char *out, size_t out_sz) {
    int in_pipe[2], out_pipe[2];
    if (pipe(in_pipe) || pipe(out_pipe)) return -1;
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        /* Capture stderr as well. Most organs print their usage and their
         * diagnostics to stderr, so reading only stdout returned an empty body
         * and the surface reported a healthy organ as producing nothing. */
        dup2(out_pipe[1], STDERR_FILENO);
        close(in_pipe[1]); close(out_pipe[0]);
        execvp(argv[0], argv);
        _exit(127);
    }
    close(in_pipe[0]); close(out_pipe[1]);
    if (body && *body) {
        ssize_t ignored = write(in_pipe[1], body, strlen(body));
        (void)ignored;
    }
    close(in_pipe[1]);
    size_t total = 0;
    ssize_t n;
    while (total + 1 < out_sz &&
           (n = read(out_pipe[0], out + total, out_sz - total - 1)) > 0)
        total += (size_t)n;
    out[total] = '\0';
    close(out_pipe[0]);
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/* Content-Length is required, not optional.
 *
 * Relying on Connection: close to delimit the body works between HTTP/1.1 peers
 * but gives nginx nothing to frame an HTTP/2 response with, and the client sees
 * the stream torn down with INTERNAL_ERROR instead of a reply. Every response
 * from this surface declares its length. */
static void auth_reply(int code, const char *body) {
    const char *b = (body && *body) ? body : "{}\n";
    printf("HTTP/1.1 %d %s\r\nContent-Type: application/json; charset=utf-8\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, code == 200 ? "OK" : (code == 401 ? "Unauthorized" : "Bad Request"),
           strlen(b), b);
}

/* Browser sessions must survive the move from the public sign-in surface to a
 * protected Aurekai subdomain.  The credential is still verified against the
 * native auth owner on every request; this cookie only lets a normal browser
 * carry that credential without exposing it to page JavaScript.  It is never
 * an institution or role grant. */
static int auth_response_token(const char *body, char *out, size_t cap) {
    const char *p;
    size_t n = 0;
    if (!body || !out || cap < 2) return 0;
    p = strstr(body, "\"token\":\"");
    if (!p) return 0;
    p += strlen("\"token\":\"");
    while (*p && *p != '"' && n + 1 < cap) out[n++] = *p++;
    out[n] = '\0';
    return *p == '"' && valid_token(out);
}

/* A browser may accept Domain only for the current host or one of its parents.
 * The four public territories are deliberately separate cookie sites: a Life
 * session must never be sprayed onto the sibling Shop/Work/Live zones.  Each
 * zone is served by the native carrier, including its controlled subnames, so
 * a person who signs in at aurekai.life can still use their life subname.
 * Cross-territory continuation is an explicit authorization exchange, never a
 * browser-cookie shortcut. */
static int auth_cookie_domain(const char *host_in, char *out, size_t cap) {
    static const char *zones[] = {
        "aurekai.life", "aurekai.live", "aurekai.shop", "aurekai.work"
    };
    char host[256];
    size_t i;
    if (!out || cap < 2 || !lower_host(host_in, host)) return 0;
    for (i = 0; i < sizeof(zones) / sizeof(zones[0]); i++) {
        size_t n = strlen(zones[i]), h = strlen(host);
        if (!strcmp(host, zones[i]) ||
            (h > n && host[h - n - 1] == '.' && !strcmp(host + h - n, zones[i]))) {
            if (n + 2 > cap) return 0;
            snprintf(out, cap, ".%s", zones[i]);
            return 1;
        }
    }
    return 0;
}

static void auth_login_reply(int code, const char *body, const char *host) {
    const char *b = (body && *body) ? body : "{}\n";
    char token[128] = {0};
    char cookie_domain[64] = {0};
    int have_cookie = code == 200 && auth_response_token(b, token, sizeof(token));
    printf("HTTP/1.1 %d %s\r\nContent-Type: application/json; charset=utf-8\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\n",
           code, code == 200 ? "OK" : "Bad Request", strlen(b));
    if (have_cookie && auth_cookie_domain(host, cookie_domain, sizeof(cookie_domain))) {
        /* The auth database expiry remains the decisive clock.  A bounded
         * browser cookie merely avoids carrying a credential in localStorage
         * while a user moves through the controlled subnames of the same
         * territory.  It grants neither institution membership nor role. */
        printf("Set-Cookie: bonfyre_session=%s; Domain=%s; Path=/; "
               "Max-Age=86400; Secure; HttpOnly; SameSite=Lax\r\n", token, cookie_domain);
    }
    printf("Connection: close\r\n\r\n%s", b);
}

/* A person signing in through the page's own form is sent back to where they
 * were -- the page that asked them to sign in -- rather than shown the owner's
 * JSON.  Only a same-site path is ever a destination. */
static const char *page_request_raw;

static void auth_form_next(const char *body, const char *query, const char *host,
                            char *out, size_t cap) {
    char value[BF_AUTH_NEXT_MAX] = {0};
    int found = body && *body != '{' ? bf_auth_next_field(body, value, sizeof(value)) : 0;
    if (!found) found = bf_auth_next_field(query, value, sizeof(value));
    /* Older forms did not carry the hidden field. Retain that transition,
     * but validate the resulting address exactly like a newly rendered form. */
    if (!found && body) {
        const char *referer = page_request_raw ? strstr(page_request_raw, "\nReferer:") : NULL;
        if (!referer && page_request_raw) referer = strstr(page_request_raw, "\nreferer:");
        if (referer) {
            char ref[16384] = {0}; const char *q;
            sscanf(referer + 9, " %16383[^\r\n]", ref);
            if ((q = strchr(ref, '?')) != NULL) found = bf_auth_next_field(q + 1, value, sizeof(value));
        }
    }
    bf_auth_return_address(found == 1 ? value : NULL, host, out, cap);
}

static void auth_login_redirect(int code, const char *body, const char *host, const char *next) {
    const char *b = (body && *body) ? body : "{}\n";
    char token[128] = {0}, cookie_domain[64] = {0};
    if (code == 200 && auth_response_token(b, token, sizeof(token))) {
        printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nCache-Control: no-store\r\nContent-Length: 0\r\n", next);
        if (auth_cookie_domain(host, cookie_domain, sizeof(cookie_domain)))
            printf("Set-Cookie: bonfyre_session=%s; Domain=%s; Path=/; Max-Age=86400; Secure; HttpOnly; SameSite=Lax\r\n", token, cookie_domain);
        printf("Connection: close\r\n\r\n");
        return;
    }
    fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=", stdout);
    url_query_value(next);
    fputs("&failed=1\r\nCache-Control: no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", stdout);
}

/* Registration preserves the chosen work but still requires sign-in. */
static void auth_signup_redirect(int code, const char *next) {
    printf("HTTP/1.1 303 See Other\r\nLocation: %s&next=",
           code == 200 ? "/login?created=1" : "/signup?failed=1");
    bf_auth_query(stdout, next);
    fputs("\r\nCache-Control: no-store\r\nContent-Length: 0\r\n"
          "Connection: close\r\n\r\n", stdout);
}

/* Revocation at BonfyreAuth is already decisive, but remove the browser's
 * transport copy too.  This uses the identical territory derivation as login
 * so logout cannot clear a cookie belonging to a sibling territory. */
static void auth_logout_reply(int code, const char *body, const char *host) {
    const char *b = (body && *body) ? body : "{}\n";
    char cookie_domain[64] = {0};
    printf("HTTP/1.1 %d %s\r\nContent-Type: application/json; charset=utf-8\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\n",
           code, code == 200 ? "OK" : "Unauthorized", strlen(b));
    if (code == 200 && auth_cookie_domain(host, cookie_domain, sizeof(cookie_domain)))
        printf("Set-Cookie: bonfyre_session=; Domain=%s; Path=/; Max-Age=0; "
               "Secure; HttpOnly; SameSite=Lax\r\n", cookie_domain);
    printf("Connection: close\r\n\r\n%s", b);
}

/* A browser asking for a page must get a page.
 *
 * Returning {"error":"POST required"} to someone who typed /login into an
 * address bar is a broken product, not an API contract. The same endpoint
 * serves a form to a browser and JSON to a client; the Accept header decides.
 */
static int wants_html(const char *accept_hdr) {
    return accept_hdr && strstr(accept_hdr, "text/html") != NULL;
}

/* An explicit ask for JSON -- not merely the absence of a browser's ask.
 *
 * curl sends a wildcard Accept by default, so treating "not HTML" as "machine"
 * would
 * silently flip every plain fetch of a dual-representation surface from the page
 * to the JSON, changing what a shell script has been reading. A client that says
 * application/json gets JSON; a client that says nothing keeps what it had. */
static int wants_json(const char *accept_hdr) {
    return accept_hdr && (strstr(accept_hdr, "application/json") != NULL ||
                          strstr(accept_hdr, "+json") != NULL);
}

static void html_error(int code, const char *title, const char *detail) {
    char page[2048];
    int n = snprintf(page, sizeof(page),
      "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
      "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
      "<title>%s</title><style>"
      ":root{color-scheme:light dark}"
      "body{margin:0;min-height:100vh;display:grid;place-items:center;"
      "background:#F0EEE9;color:#141613;font:16px/1.6 system-ui,sans-serif}"
      "@media(prefers-color-scheme:dark){body{background:#0E100D;color:#E4E7DE}}"
      "main{max-width:34rem;padding:2rem}h1{font-size:2rem;margin:0 0 .5rem;"
      "letter-spacing:-.02em}p{margin:0 0 1.5rem;opacity:.72}"
      "a{color:inherit;text-decoration:none;border-bottom:2px solid currentColor;"
      "padding-bottom:1px;font-weight:600}"
      "</style></head><body><main><h1>%s</h1><p>%s</p>"
      "<a href=\"/\">Back to the entry point</a></main></body></html>\n",
      title, title, detail);
    if (n < 0) return;
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Content-Length: %d\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, code == 404 ? "Not Found" : (code == 401 ? "Unauthorized" : "Bad Request"),
           n, page);
}

/* Account-link inputs are submitted either by an ordinary form or by the
 * progressively enhanced account page. The edge only translates shape; the
 * Auth owner authenticates the session, validates the locator, and owns the
 * relationship record. No provider credential or OAuth code crosses here. */
static int auth_body_field(const char *body, const char *name, char *out, size_t cap) {
    if (!body || !*body || !name || !out || !cap) return 0;
    if (*body == '{') return studio_json_field(body, name, out, cap);
    return query_value(body, name, out, cap);
}

static void auth_account_redirect(const char *notice) {
    printf("HTTP/1.1 303 See Other\r\nLocation: /account?%s\r\nCache-Control: no-store\r\n"
           "Content-Length: 0\r\nConnection: close\r\n\r\n", notice && *notice ? notice : "updated=1");
}

#include "session_handoff_web.h"
static int serve_auth(const char *method, const char *target, const char *body,
                      const char *bearer, const char *db, const char *accept,
                      const char *root, const char *host, const char *query) {
    char out[8192];
    if(serve_session_handoff(method,target,bearer,db,host,query))return 1;
    const char *bin = auth_binary();

    if (!strcmp(target, "/signup") || !strcmp(target, "/login")) {
        if (strcmp(method, "POST")) {
            /* A GET here is a person, not a client. Serve the form. */
            if (!strcmp(method, "GET") && wants_html(accept)) {
                char path[PATH_MAX];
                const char *page = !strcmp(target, "/signup") ? "signup" : "login";
                if (root && *root &&
                    snprintf(path, sizeof(path), "%s/surfaces/%s.html", root, page)
                      < (int)sizeof(path) && access(path, R_OK) == 0) {
                    FILE *f = fopen(path, "rb");
                    if (f) {
                        long len = 0;
                        char *page_text = NULL, *open = NULL, *close = NULL;
                        if (!fseek(f, 0, SEEK_END)) { len = ftell(f); rewind(f); }
                        if (len > 0 && len < 1024 * 1024 && (page_text = malloc((size_t)len + 1)) &&
                            fread(page_text, 1, (size_t)len, f) == (size_t)len) {
                            page_text[len] = '\0';
                            open = strstr(page_text, "<body"); close = open ? strchr(open, '>') : NULL;
                        }
                        fclose(f);
                        if (page_text && close) {
                            char next[BF_AUTH_NEXT_MAX], flag[32], *rendered = NULL;
                            size_t rendered_len = 0;
                            FILE *view = open_memstream(&rendered, &rendered_len);
                            const char *notice = ""; int ready = 0;
                            auth_form_next(NULL, query, host, next, sizeof(next));
                            if (query_value(query, "failed", flag, sizeof(flag)) && !strcmp(flag, "1"))
                                notice = !strcmp(page, "signup") ? "Account creation failed. Check your details or sign in." : "Sign-in failed. Check your email and password.";
                            else if (!strcmp(page, "login") && query_value(query, "created", flag, sizeof(flag)) && !strcmp(flag, "1"))
                                notice = "Account created. Sign in to continue.";
                            if (view) {
                                ready = bf_auth_form_document(view, page_text, next, notice, BF_ESTATE_BAR);
                                if (fclose(view)) ready = 0;
                            }
                            free(page_text);
                            if (!ready || !rendered) {
                                free(rendered);
                                html_error(503, "Sign-in unavailable", "The account form could not be opened. Try again.");
                                return 1;
                            }
                            printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                                   "Content-Length: %zu\r\nCache-Control: no-store\r\n"
                                   "Referrer-Policy: no-referrer\r\nConnection: close\r\n\r\n", rendered_len);
                            fwrite(rendered, 1, rendered_len, stdout);
                            free(rendered);
                            return 1;
                        }
                        free(page_text);
                    }
                }
            }
            if (wants_html(accept))
                html_error(400, "Use the form",
                    "This address accepts a submitted form. Open it in a browser "
                    "or send a POST with a JSON body.");
            else
                auth_reply(400, "{\"error\":\"POST required\"}\n");
            return 1;
        }
        /* A native HTML form posts urlencoded; the authority organ speaks JSON.
         *
         * Translating here keeps both contracts intact: the form stays a plain
         * form that works without JavaScript, and the owner keeps the single
         * body shape it validates. Requiring JSON at this edge would have meant
         * a login that only works with a script running, which is the pattern
         * this estate is deliberately not building.
         *
         * The translated body is built and passed in memory and never logged;
         * it carries a credential. */
        static char translated[4096];
        const char *send = body;
        if (body && *body && *body != '{') {
            char email[320], password[256], name[256];
            char e_email[640], e_name[512], e_pw[512];
            if (query_value(body, "email", email, sizeof(email)) &&
                query_value(body, "password", password, sizeof(password))) {
                json_escape_into(email, e_email, sizeof(e_email));
                json_escape_into(password, e_pw, sizeof(e_pw));
                if (query_value(body, "name", name, sizeof(name))) {
                    json_escape_into(name, e_name, sizeof(e_name));
                    snprintf(translated, sizeof(translated),
                             "{\"email\":\"%s\",\"name\":\"%s\",\"password\":\"%s\"}",
                             e_email, e_name, e_pw);
                } else {
                    snprintf(translated, sizeof(translated),
                             "{\"email\":\"%s\",\"password\":\"%s\"}", e_email, e_pw);
                }
                send = translated;
            }
        }
        char *const argv[] = { (char*)bin, "--db", (char*)db,
            (char*)(!strcmp(target,"/signup") ? "http-signup" : "http-login"), NULL };
        int rc = auth_call(argv, send, out, sizeof(out));
        /* Overwrite the credential copy as soon as the owner has it. */
        memset(translated, 0, sizeof(translated));
        /* The authority organ can distinguish an absent user from a wrong
         * password internally; the public carrier must not turn that into an
         * account-enumeration oracle. */
        if (body && *body && *body != '{' && wants_html(accept)) {
            char next[BF_AUTH_NEXT_MAX];
            auth_form_next(body, query, host, next, sizeof(next));
            if (!strcmp(target, "/signup")) auth_signup_redirect(rc == 0 ? 200 : 400, next);
            else auth_login_redirect(rc == 0 ? 200 : 400, out, host, next);
            return 1;
        }
        if (rc != 0) auth_login_reply(400, "{\"error\":\"authentication_failed\"}\n", host);
        else auth_login_reply(200, out, host);
        return 1;
    }
    if (!strcmp(target, "/logout")) {
        if (!valid_token(bearer)) {
            if (wants_html(accept))
                html_error(401, "Not signed in",
                    "There is no session on this request to sign out of.");
            else auth_reply(401, "{\"error\":\"bearer token required\"}\n");
            return 1;
        }
        char *const argv[] = { (char*)bin, "--db", (char*)db, "logout",
                               "--token", (char*)bearer, NULL };
        int rc = auth_call(argv, NULL, out, sizeof(out));
        auth_logout_reply(rc == 0 ? 200 : 401, out, host);
        return 1;
    }
    if (!strcmp(target, "/account/connections") || !strcmp(target, "/account/connections/revoke")) {
        char out[16384];
        if (!valid_token(bearer)) {
            if (wants_html(accept)) html_error(401, "Sign in to manage account links",
                                                "Account links belong to a signed-in person.");
            else auth_reply(401, "{\"error\":\"bearer token required\"}\n");
            return 1;
        }
        if (!strcmp(target, "/account/connections") && !strcmp(method, "GET")) {
            char *const argv[] = { (char*)bin, "--db", (char*)db, "external-link-list",
                                   "--token", (char*)bearer, NULL };
            int rc = auth_call(argv, NULL, out, sizeof(out));
            auth_reply(rc == 0 ? 200 : 401, rc == 0 ? out : "{\"error\":\"account link session invalid\"}\n");
            return 1;
        }
        if (!strcmp(target, "/account/connections") && !strcmp(method, "POST")) {
            char provider[64] = {0}, remote[256] = {0}, profile[1100] = {0}, label[256] = {0};
            if (!auth_body_field(body, "provider", provider, sizeof(provider)) ||
                !auth_body_field(body, "remote_account", remote, sizeof(remote)) ||
                !auth_body_field(body, "profile_url", profile, sizeof(profile))) {
                if (wants_html(accept)) html_error(400, "Complete the account link",
                                                    "Choose a provider, account handle, and HTTPS public profile URL.");
                else auth_reply(400, "{\"error\":\"provider, remote_account, and profile_url are required\"}\n");
                return 1;
            }
            (void)auth_body_field(body, "display_name", label, sizeof(label));
            char *const argv[] = { (char*)bin, "--db", (char*)db, "external-link-claim",
                                   "--token", (char*)bearer, "--provider", provider,
                                   "--remote-account", remote, "--profile-url", profile,
                                   "--display-name", label, NULL };
            int rc = auth_call(argv, NULL, out, sizeof(out));
            if (wants_html(accept)) {
                if (rc == 0) auth_account_redirect("linked=declared");
                else html_error(400, "That account link could not be saved",
                                "Use a lowercase provider name, a public account handle, and an HTTPS profile URL without credentials.");
            } else auth_reply(rc == 0 ? 200 : 400, rc == 0 ? out : "{\"error\":\"invalid external account link\"}\n");
            return 1;
        }
        if (!strcmp(target, "/account/connections/revoke") && !strcmp(method, "POST")) {
            char link_id[32] = {0};
            if (!auth_body_field(body, "link_id", link_id, sizeof(link_id))) {
                auth_reply(400, "{\"error\":\"link_id required\"}\n");
                return 1;
            }
            char *const argv[] = { (char*)bin, "--db", (char*)db, "external-link-revoke",
                                   "--token", (char*)bearer, "--link-id", link_id, NULL };
            int rc = auth_call(argv, NULL, out, sizeof(out));
            if (wants_html(accept)) {
                if (rc == 0) auth_account_redirect("unlinked=1");
                else html_error(400, "That account link could not be removed", "It may already be gone or belong to another account.");
            } else auth_reply(rc == 0 ? 200 : 400, rc == 0 ? out : "{\"error\":\"account link not found\"}\n");
            return 1;
        }
        if (wants_html(accept)) html_error(400, "Use the account controls", "This account-link endpoint accepts a read or submitted form.");
        else auth_reply(400, "{\"error\":\"method not allowed\"}\n");
        return 1;
    }
    if (!strcmp(target, "/account")) {
        /* A browser has no way to attach a bearer header by typing a URL, so it
         * always gets the account page.  A valid HttpOnly cookie is still
         * presented on the page's same-origin requests, but returning the
         * machine JSON merely because that cookie was already parsed made the
         * post-login redirect visibly land on raw JSON instead of the account.
         * JSON clients keep the native verification response below. */
        if (!strcmp(method, "GET") && wants_html(accept)) {
            char path[PATH_MAX];
            if (root && *root &&
                snprintf(path, sizeof(path), "%s/surfaces/account-native.html", root)
                  < (int)sizeof(path) && access(path, R_OK) == 0) {
                FILE *f = fopen(path, "rb");
                if (f) {
                    long len=0;char *page=NULL,*body=NULL,*body_end=NULL,*main_end=NULL;
                    if(!fseek(f,0,SEEK_END)){len=ftell(f);rewind(f);}
                    if(len>0&&len<512*1024&&(page=malloc((size_t)len+1))&&fread(page,1,(size_t)len,f)==(size_t)len){
                        page[len]=0;body=strstr(page,"<body");body_end=body?strchr(body,'>'):NULL;main_end=body_end?strstr(body_end,"</main>"):NULL;
                    }
                    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                           "Cache-Control: private, no-store\r\nVary: Cookie, Authorization\r\n"
                           "Connection: close\r\n\r\n");
                    if(page&&body_end&&main_end){
                        const char *pattern = bf_route_presentation_pattern(host, target, query);
                        char *head = strstr(page, "</head>");
                        if (pattern && head && head < body && body_end > body) {
                            fwrite(page, 1, (size_t)(head - page), stdout);
                            bf_route_presentation_emit_head_for(stdout, bf_route_presentation_territory(host), target, query, pattern);
                            fwrite(head, 1, (size_t)(body_end - head), stdout);
                            bf_route_presentation_emit_body_attribute_for(stdout, bf_route_presentation_territory(host), target, query, pattern);
                            fputc('>', stdout);
                        } else fwrite(page,1,(size_t)(body_end+1-page),stdout);
                        fputs("<style id=\"ak-account-depth\">.top{display:none!important}.shell{max-width:1450px!important;padding:clamp(18px,3.5vw,54px)!important}.lead{position:relative;max-width:none;padding:clamp(40px,8vw,100px) clamp(22px,5vw,72px)!important;margin:16px 0 34px;isolation:isolate;overflow:hidden;border-radius:30px;background:radial-gradient(ellipse at 96% 8%,#9db9885e,transparent 48%),linear-gradient(123deg,#172d24 4%,#2e5339 62%,#12251f 100%);color:#f7f7eb}.lead:after{content:'';pointer-events:none;position:absolute;right:-90px;top:-130px;width:clamp(250px,35vw,600px);aspect-ratio:1;border:1px solid #cfe8b866;border-radius:50%;box-shadow:0 0 0 47px #def1d414,0 0 0 110px #def1d409;transform:rotate(-18deg)}.lead .eyebrow{color:#c6e7ad!important;position:relative;z-index:1}.lead h1{font-family:'Instrument Serif',Georgia,serif;font-weight:400;letter-spacing:-.06em;font-size:clamp(56px,7.8vw,114px);line-height:.94;max-width:850px;position:relative;z-index:1}.lead em{color:#cbebad}.lead p:not(.eyebrow){color:#d5e7d1!important;max-width:850px;line-height:1.65;position:relative;z-index:1}.layout,.stack{gap:clamp(16px,2.3vw,30px)!important}.card{border-radius:22px!important;border:1px solid var(--rule)!important;box-shadow:0 10px 42px #07190c12;transition:box-shadow .3s ease,border-color .3s ease;min-width:0}.card:focus-within{border-color:var(--live)!important;box-shadow:0 16px 48px #07190c20}.card h2{font-family:'Instrument Serif',Georgia,serif;font-size:clamp(29px,3vw,43px)!important;font-weight:400;line-height:1.08}.link{position:relative;border-radius:12px!important;overflow:hidden;transition:transform .25s ease,background .25s ease,border-color .25s ease}.link:hover,.link:focus-visible{transform:translateX(4px);background:var(--soft);border-color:var(--live)!important}.link small{display:block;line-height:1.5;margin-top:4px}.external{position:relative;gap:14px!important}.external-item{position:relative;overflow:hidden;isolation:isolate;min-width:0;gap:12px!important;border:1px solid var(--rule)!important;border-radius:18px;padding:24px 20px 20px 24px!important;background:linear-gradient(133deg,var(--paper),color-mix(in srgb,var(--soft) 40%,var(--paper)));box-shadow:0 4px 16px #101f1510;transition:transform .3s cubic-bezier(.2,.7,.2,1),box-shadow .3s,border-color .3s}.external-item:before{content:'↗';position:absolute;top:-20px;right:18px;color:var(--live);opacity:.16;font:90px/.9 'Instrument Serif',Georgia,serif;pointer-events:none;transition:transform .45s,opacity .45s}.external-item:hover,.external-item:focus-within{transform:translateY(-4px);box-shadow:0 19px 44px #101f1522;border-color:var(--live)!important}.external-item:hover:before{transform:translate(6px,-5px) rotate(10deg);opacity:.3}.external-item>span{min-width:0;position:relative;z-index:1}.external-item a{font-size:clamp(16px,1.7vw,20px);text-decoration-thickness:1px;text-underline-offset:4px}.external-item small{display:inline-block;margin-top:6px;line-height:1.5;overflow-wrap:anywhere}.external-item .status{display:inline-flex;border:1px solid var(--rule);border-radius:999px;padding:6px 9px;font-size:11px;font-weight:700}.external-item p{max-width:44ch;font-size:12px;line-height:1.6}.external-item .remove{min-height:38px;border-radius:999px;transition:transform .2s}.external-item .remove:hover{transform:translateY(-2px)}.identity{border-radius:12px;overflow:hidden}.account-tools-host{margin:clamp(40px,5vw,80px) 0 0}.account-tools-host>h2{font:400 clamp(39px,5vw,70px)/1 'Instrument Serif',Georgia,serif;letter-spacing:-.05em;margin:0 0 26px}.account-tools-host [data-account-tools]{min-width:0}@media(max-width:760px){.lead{margin-top:8px;border-radius:20px}.lead:after{opacity:.45}.shell{padding:14px!important}.card{padding:20px!important}.external-item{padding:18px!important}.account-tools-host{margin-top:48px}}@media(prefers-reduced-motion:reduce){.link,.card,.external-item,.external-item:before,.external-item .remove{transition:none!important;transform:none!important}}</style>",stdout);
                        fputs(BF_ESTATE_BAR,stdout);
                        fputs(BF_ACCOUNT_CONNECTION_STYLE,stdout);
                        fwrite(body_end+1,1,(size_t)(main_end-(body_end+1)),stdout);
                        fputs("<section class=\"account-tools-host\" aria-labelledby=\"account-tools-title\"><h2 id=\"account-tools-title\">Your tools and saved work</h2><div hx-get=\"/apps/_cards?context=self\" hx-trigger=\"load\" hx-swap=\"innerHTML\"><p class=\"empty\">Opening the tools already installed for you…</p></div></section>",stdout);
                        fputs(BF_ACCOUNT_CONNECTION_SCRIPT,stdout);
                        fwrite(main_end,1,(size_t)len-(size_t)(main_end-page),stdout);
                    }else if(page)fwrite(page,1,(size_t)len,stdout);
                    else{rewind(f);char buf[8192];size_t r;while((r=fread(buf,1,sizeof(buf),f))>0)fwrite(buf,1,r,stdout);}
                    free(page);
                    fclose(f);
                    return 1;
                }
            }
        }
        if (!valid_token(bearer)) {
            if (wants_html(accept))
                html_error(401, "Sign in to see your account",
                    "This page needs a session. Sign in and your account details "
                    "will appear here.");
            else auth_reply(401, "{\"error\":\"bearer token required\"}\n");
            return 1;
        }
        char *const argv[] = { (char*)bin, "--db", (char*)db, "verify",
                               "--token", (char*)bearer, NULL };
        int rc = auth_call(argv, NULL, out, sizeof(out));
        int ok = (rc == 0) && strstr(out, "\"valid\":true") != NULL;
        auth_reply(ok ? 200 : 401, out);
        return 1;
    }
    return 0;
}

/* Narrow witness for nginx auth_request: it proves only that a bearer remains
 * valid, never that it has effect authority. */
static int session_valid(const char *bearer, const char *db) {
    char out[2048]; const char *bin = auth_binary();
    if (!valid_token(bearer)) return 0;
    char *const argv[] = { (char*)bin, "--db", (char*)db, "verify", "--token", (char*)bearer, NULL };
    return auth_call(argv, NULL, out, sizeof(out)) == 0 && strstr(out, "\"valid\":true") != NULL;
}

/* The Studio API remains a real machine carrier, but an HTMX request is a
 * human carrier.  Do not leak a command receipt as a JSON wall into the human
 * carrier: compile it server-side into an ordinary result fragment. */
static int studio_html_mode = 0;
static int studio_json_string(const char *body, const char *name, char *out, size_t size);

static void studio_html_append(char *out, size_t cap, size_t *used, const char *text) {
    for (; text && *text && *used + 8 < cap; text++) {
        const char *replacement = NULL;
        switch (*text) {
            case '&': replacement = "&amp;"; break;
            case '<': replacement = "&lt;"; break;
            case '>': replacement = "&gt;"; break;
            case '\"': replacement = "&quot;"; break;
            case '\'': replacement = "&#39;"; break;
            default: break;
        }
        if (replacement) {
            size_t n = strlen(replacement);
            if (*used + n >= cap) break;
            memcpy(out + *used, replacement, n); *used += n;
        } else out[(*used)++] = *text;
    }
    out[*used] = '\0';
}

/* Write markup this function owns, unescaped.
 *
 * studio_html_append() escapes its input, which is right for a value out of an
 * organ and wrong for the tags this receipt emits itself: every <dl> and <dt>
 * reached the page as literal "&lt;dl&gt;" text, so the fragment rendered as a
 * wall of entities instead of a list. */
static void studio_html_raw(char *out, size_t cap, size_t *used, const char *markup) {
    size_t n;
    if (!markup) return;
    n = strlen(markup);
    if (*used + n >= cap) return;
    memcpy(out + *used, markup, n);
    *used += n;
    out[*used] = '\0';
}

static void studio_html_label(char *out, size_t cap, const char *key) {
    size_t used = 0;
    if (!key || !*key) { snprintf(out, cap, "Detail"); return; }
    for (; *key && used + 1 < cap; key++) {
        char c = *key == '_' || *key == '-' ? ' ' : *key;
        if (!used || out[used - 1] == ' ') c = (char)toupper((unsigned char)c);
        out[used++] = c;
    }
    out[used] = '\0';
}

static const char *studio_jsonish_string(const char *p, char *out, size_t cap) {
    size_t used = 0;
    if (!p || *p != '\"' || !cap) return NULL;
    p++;
    while (*p && *p != '\"' && used + 1 < cap) {
        if (*p == '\\' && p[1]) p++;
        if ((unsigned char)*p < 0x20) return NULL;
        out[used++] = *p++;
    }
    if (*p != '\"') return NULL;
    out[used] = '\0';
    return p + 1;
}

static void studio_html_receipt(int code, const char *raw, char *out, size_t cap) {
    const char *p = raw ? raw : "";
    size_t used = 0; int fields = 0;
    char error[512] = {0};
    (void)studio_json_string(raw, "error", error, sizeof(error));
    snprintf(out, cap,
        "<section class=\"studio-result %s\" aria-live=\"polite\"><span class=\"result-kicker\">Native owner</span><h3>%s</h3><p>%s</p>",
        code >= 400 ? "is-error" : "is-ok",
        code >= 400 ? "This needs attention" : "Updated from the canonical owner",
        error[0] ? "The request was not applied. Review the required field and try again." :
                   "The result below is live operating state, not a copied dashboard.");
    used = strlen(out);
    if (error[0]) {
        studio_html_append(out, cap, &used, "<p class=\"result-message\">");
        studio_html_append(out, cap, &used, error);
        studio_html_raw(out, cap, &used, "</p></section>");
        return;
    }
    studio_html_raw(out, cap, &used, "<dl class=\"result-facts\">");
    while ((p = strchr(p, '\"')) && fields < 80 && used + 1024 < cap) {
        char key[128], value[768], label[160]; const char *after;
        after = studio_jsonish_string(p, key, sizeof(key));
        if (!after) { p++; continue; }
        while (*after == ' ' || *after == '\t' || *after == '\n' || *after == '\r') after++;
        if (*after != ':') { p = after; continue; }
        after++; while (*after == ' ' || *after == '\t' || *after == '\n' || *after == '\r') after++;
        if (*after == '\"') {
            const char *next = studio_jsonish_string(after, value, sizeof(value));
            if (!next) { p = after + 1; continue; }
            p = next;
        } else {
            size_t n = 0;
            while (after[n] && after[n] != ',' && after[n] != '}' && after[n] != ']' && n + 1 < sizeof(value)) n++;
            while (n && isspace((unsigned char)after[n - 1])) n--;
            memcpy(value, after, n); value[n] = '\0'; p = after + n;
        }
        /* Shape/version labels are implementation detail, not a task outcome. */
        if (!strcmp(key, "schema") || !strcmp(key, "forbidden_inference") || !strcmp(key, "error") || !value[0]) continue;
        studio_html_label(label, sizeof(label), key);
        studio_html_raw(out, cap, &used, "<div><dt>"); studio_html_append(out, cap, &used, label);
        studio_html_raw(out, cap, &used, "</dt><dd>"); studio_html_append(out, cap, &used, value);
        studio_html_raw(out, cap, &used, "</dd></div>"); fields++;
    }
    studio_html_raw(out, cap, &used, fields ? "</dl>" : "<p>No additional facts were returned.</p>");
    studio_html_raw(out, cap, &used, "</section>");
}

static void studio_reply(int code, const char *type, const char *body) {
    const char *out = body && *body ? body : "{}\n";
    if (studio_html_mode) {
        static char rendered[524288];
        studio_html_receipt(code, out, rendered, sizeof(rendered));
        printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
               code, code == 200 ? "OK" : (code == 401 ? "Unauthorized" : "Service Unavailable"), strlen(rendered), rendered);
        return;
    }
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, code == 200 ? "OK" : (code == 401 ? "Unauthorized" : "Service Unavailable"), type, strlen(out), out);
}

/* Authenticated, read-only projections over real native organs. No request
 * field becomes an executable or filesystem path. */
static int studio_json_string(const char *body, const char *name, char *out, size_t size) {
    char key[96]; const char *p; size_t n = 0;
    if (!body || !name || !out || !size || snprintf(key, sizeof(key), "\"%s\"", name) >= (int)sizeof(key)) return 0;
    p = strstr(body, key);
    /* HTMX forms use the ordinary browser form encoding.  Native owners keep
     * their typed validation below; this merely gives the human carrier the
     * same input path as the machine JSON carrier. */
    if (!p) {
        size_t name_n = strlen(name); const char *field = body;
        while (field && *field) {
            const char *end = strchr(field, '&'); const char *eq = strchr(field, '=');
            size_t field_n = end ? (size_t)(end - field) : strlen(field);
            if (eq && (size_t)(eq - field) == name_n && !strncmp(field, name, name_n)) {
                const char *v = eq + 1; const char *limit = field + field_n;
                while (v < limit && n + 1 < size) {
                    unsigned char c = (unsigned char)*v++;
                    if (c == '+') c = ' ';
                    else if (c == '%' && v + 1 < limit && isxdigit((unsigned char)v[0]) && isxdigit((unsigned char)v[1])) {
                        int hi = isdigit((unsigned char)v[0]) ? v[0] - '0' : (tolower((unsigned char)v[0]) - 'a' + 10);
                        int lo = isdigit((unsigned char)v[1]) ? v[1] - '0' : (tolower((unsigned char)v[1]) - 'a' + 10);
                        c = (unsigned char)((hi << 4) | lo); v += 2;
                    }
                    if (!c || c < 0x20) return 0;
                    out[n++] = (char)c;
                }
                out[n] = '\0'; return n > 0;
            }
            field = end ? end + 1 : NULL;
        }
        return 0;
    }
    if (!(p = strchr(p + strlen(key), ':'))) return 0;
    while (*++p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') {}
    if (*p++ != '\"') return 0;
    while (*p && *p != '\"' && n + 1 < size) {
        if (*p == '\\' || (unsigned char)*p < 0x20) return 0;
        out[n++] = *p++;
    }
    if (*p != '\"' || !n) return 0;
    out[n] = '\0';
    return 1;
}

static int studio_safe_twin(const char *text) {
    if (!text || !*text || strlen(text) > 96) return 0;
    for (; *text; text++) if (!(isalnum((unsigned char)*text) || *text == '-' || *text == '_' || *text == ':')) return 0;
    return 1;
}

static const char *matrix_receipt_dir(void) {
    const char *configured = getenv("BONFYRE_MATRIX_RECEIPT_DIR");
    return configured && *configured ? configured : "/var/lib/bonfyre/matrix/receipts";
}

static int studio_safe_slug(const char *text) {
    size_t n;
    if (!text || !(n = strlen(text)) || n > 80 || text[0] == '-' || text[n - 1] == '-') return 0;
    for (; *text; text++) if (!(islower((unsigned char)*text) || isdigit((unsigned char)*text) || *text == '-')) return 0;
    return 1;
}

static int studio_safe_kind(const char *text) {
    if (!text || !*text || strlen(text) > 64 || !isalpha((unsigned char)*text)) return 0;
    for (; *text; text++) if (!(isalnum((unsigned char)*text) || *text == '_' || *text == '-')) return 0;
    return 1;
}

static int studio_safe_recipe(const char *text) {
    if (!text || !*text || strlen(text) > 120) return 0;
    for (; *text; text++) if (!(isalnum((unsigned char)*text) || *text == '.' || *text == '_' || *text == '-' || *text == ':')) return 0;
    return 1;
}

static int studio_safe_usd(const char *text, char out[32]) {
    char *end = NULL; double value;
    if (!text || !*text || strlen(text) > 24) return 0;
    errno = 0; value = strtod(text, &end);
    if (errno || !end || *end || value < 0.0 || value > 1000000.0) return 0;
    return snprintf(out, 32, "%.6f", value) > 0;
}

static int studio_safe_title(const char *text) {
    size_t n;
    if (!text || !(n = strlen(text)) || n > 160) return 0;
    for (; *text; text++) if ((unsigned char)*text < 0x20 || (unsigned char)*text == 0x7f) return 0;
    return 1;
}

static int studio_json_append(char *out, size_t size, size_t *used, const char *text) {
    size_t escaped;
    if (!out || !used || *used >= size || size - *used < 3) return 0;
    escaped = bf_lex_json_escape_len(text, text ? strlen(text) : 0);
    if (escaped > size - *used - 3) return 0;
    out[(*used)++] = '"';
    if (!bf_lex_json_escape(text, out + *used, size - *used - 1)) return 0;
    *used += escaped;
    out[(*used)++] = '"'; out[*used] = '\0';
    return 1;
}

static int studio_json_append_raw(char *out, size_t size, size_t *used, const char *json) {
    size_t n;
    if (!out || !used || !json || !(n = strlen(json)) || *used + n >= size) return 0;
    memcpy(out + *used, json, n);
    *used += n;
    out[*used] = '\0';
    return 1;
}

static int studio_auth_user(const char *bearer, const char *auth_db, char *out, size_t out_sz) {
    const char *bin = auth_binary();
    char verified[4096], *id;
    unsigned int user_id;
    char *const argv[] = { (char*)bin, "--db", (char*)auth_db, "verify", "--token", (char*)bearer, NULL };
    if (!valid_token(bearer) || auth_call(argv, NULL, verified, sizeof(verified)) != 0 ||
        !strstr(verified, "\"valid\":true")) return 0;
    id = strstr(verified, "\"id\":");
    if (!id || sscanf(id + 5, "%u", &user_id) != 1) return 0;
    snprintf(out, out_sz, "%u", user_id);
    return 1;
}

/* A generic Matrix binding surface may extend only a subject that this session
 * published locally. Institutional delegation has a separate AuthorityGraph
 * path; a valid browser session is deliberately not a delegation grant. */
static int studio_owns_subject(const char *subject, const char *bearer, const char *auth_db) {
    char user[32], prefix[64];
    int n;
    if (!subject || !studio_auth_user(bearer, auth_db, user, sizeof(user))) return 0;
    n = snprintf(prefix, sizeof(prefix), "subject:actor-%s:", user);
    return n > 0 && (size_t)n < sizeof(prefix) && !strncmp(subject, prefix, (size_t)n);
}

/* A publication is a local Matrix effect: it creates no foreign account,
 * contract, settlement, or authority grant.  The receipt survives separately
 * from the compact Matrix coordinates so it can be audited without turning
 * title text into an executable instruction. */
static int studio_publish(BfMatrix *matrix, const char *body, const char *bearer,
                          const char *auth_db, const char *host) {
    char user[32], title[161], slug[81], kind[65], scope_text[16], host_norm[256];
    char stamp[32], material[640], digest[65], subject[128], presence[160], url[512];
    char fs_path[512], api[512], mcp[512], receipt_ref[160], receipt_path[PATH_MAX];
    const char *territory, *host_scope;
    BfMatrixParticipationScope scope;
    BfMatrixSubjectSpec subject_spec;
    BfMatrixPresenceSpec presence_spec;
    BfMatrixStatus status;
    FILE *receipt;
    char response[4096]; size_t used = 0;
    if (!studio_auth_user(bearer, auth_db, user, sizeof(user)) || !lower_host(host, host_norm) ||
        !studio_json_string(body, "title", title, sizeof(title)) || !studio_safe_title(title) ||
        !studio_json_string(body, "slug", slug, sizeof(slug)) || !studio_safe_slug(slug) ||
        !studio_json_string(body, "kind", kind, sizeof(kind)) || !studio_safe_kind(kind) ||
        !studio_json_string(body, "scope", scope_text, sizeof(scope_text)) || (scope = scope_of(scope_text)) < 0) {
        studio_reply(503, "application/json", "{\"error\":\"title, lowercase slug, extensible kind, and participation scope required\"}\n");
        return 1;
    }
    territory = territory_of(host_norm);
    host_scope = default_scope(territory);
    if (scope != scope_of(host_scope)) {
        char refusal[192];
        int n = snprintf(refusal, sizeof(refusal), "{\"error\":\"scope does not resolve on this host\",\"required_scope\":\"%s\"}\n", host_scope);
        studio_reply(503, "application/json", n > 0 && n < (int)sizeof(refusal) ? refusal : "{\"error\":\"invalid_host_scope\"}\n"); return 1;
    }
    iso_utc_now(stamp, sizeof(stamp));
    if (snprintf(material, sizeof(material), "matrix-publication-v1|%s|%s|%s|%s|%s|%s|%s", user, host_norm, territory, scope_text, kind, slug, stamp) >= (int)sizeof(material)) {
        studio_reply(503, "application/json", "{\"error\":\"publication_material_overflow\"}\n"); return 1;
    }
    bf_sha256_hex((const uint8_t *)material, strlen(material), digest);
    if (snprintf(subject, sizeof(subject), "subject:actor-%s:%.*s", user, 24, digest) >= (int)sizeof(subject) ||
        snprintf(presence, sizeof(presence), "presence:%s:%s:%.*s", subject, territory, 12, digest) >= (int)sizeof(presence) ||
        snprintf(url, sizeof(url), "https://%s/%s", host_norm, slug) >= (int)sizeof(url) ||
        snprintf(fs_path, sizeof(fs_path), "/Bonfyre/%s/%s", territory, slug) >= (int)sizeof(fs_path) ||
        snprintf(api, sizeof(api), "/api/subjects/%s", subject) >= (int)sizeof(api) ||
        snprintf(mcp, sizeof(mcp), "resource://%s/%s", territory, slug) >= (int)sizeof(mcp) ||
        snprintf(receipt_ref, sizeof(receipt_ref), "receipt:matrix-publication:%s", digest) >= (int)sizeof(receipt_ref) ||
        snprintf(receipt_path, sizeof(receipt_path), "%s/%s.json", matrix_receipt_dir(), digest) >= (int)sizeof(receipt_path)) {
        studio_reply(503, "application/json", "{\"error\":\"publication_path_overflow\"}\n"); return 1;
    }
    if (!getenv("BONFYRE_MATRIX_RECEIPT_DIR") && mkdir("/var/lib/bonfyre/matrix", 0750) != 0 && errno != EEXIST) {
        studio_reply(503, "application/json", "{\"error\":\"receipt_store_unavailable\"}\n"); return 1;
    }
    if (mkdir(matrix_receipt_dir(), 0750) != 0 && errno != EEXIST) {
        studio_reply(503, "application/json", "{\"error\":\"receipt_store_unavailable\"}\n"); return 1;
    }
    if (!(receipt = fopen(receipt_path, "wx"))) {
        if (errno != EEXIST) { studio_reply(503, "application/json", "{\"error\":\"receipt_store_unavailable\"}\n"); return 1; }
    } else {
        fputs("{\"schema\":\"bonfyre-matrix-publication-v1\",\"actor_id\":", receipt); json_file(receipt, user);
        fputs(",\"subject_id\":", receipt); json_file(receipt, subject); fputs(",\"title\":", receipt); json_file(receipt, title);
        fputs(",\"kind\":", receipt); json_file(receipt, kind); fputs(",\"host\":", receipt); json_file(receipt, host_norm);
        fputs(",\"territory\":", receipt); json_file(receipt, territory); fputs(",\"scope\":", receipt); json_file(receipt, scope_text);
        fputs(",\"created_at\":", receipt); json_file(receipt, stamp); fputs(",\"identity_digest\":", receipt); json_file(receipt, digest); fputs("}\n", receipt);
        fclose(receipt);
    }
    subject_spec = (BfMatrixSubjectSpec){subject, kind, digest, receipt_ref};
    status = bf_matrix_subject_bind(matrix, &subject_spec);
    if (status != BF_MATRIX_OK ||
        (status = bf_matrix_address_bind(matrix, subject, "web_url", url, receipt_ref)) != BF_MATRIX_OK ||
        (status = bf_matrix_address_bind(matrix, subject, "slug", slug, receipt_ref)) != BF_MATRIX_OK ||
        (status = bf_matrix_address_bind(matrix, subject, "fs_path", fs_path, receipt_ref)) != BF_MATRIX_OK ||
        (status = bf_matrix_address_bind(matrix, subject, "api", api, receipt_ref)) != BF_MATRIX_OK ||
        (status = bf_matrix_address_bind(matrix, subject, "mcp", mcp, receipt_ref)) != BF_MATRIX_OK) {
        int n = snprintf(response, sizeof(response), "{\"error\":\"matrix_publication_refused\",\"status\":\"%s\"}\n", bf_matrix_status_name(status));
        studio_reply(503, "application/json", n > 0 && n < (int)sizeof(response) ? response : "{\"error\":\"matrix_publication_refused\"}\n"); return 1;
    }
    presence_spec = (BfMatrixPresenceSpec){presence, subject, NULL, territory, scope, BF_MATRIX_LIVE, "HTML", receipt_ref};
    status = bf_matrix_presence_bind(matrix, &presence_spec);
    if (status != BF_MATRIX_OK) {
        int n = snprintf(response, sizeof(response), "{\"error\":\"matrix_presence_refused\",\"status\":\"%s\"}\n", bf_matrix_status_name(status));
        studio_reply(503, "application/json", n > 0 && n < (int)sizeof(response) ? response : "{\"error\":\"matrix_presence_refused\"}\n"); return 1;
    }
    used = (size_t)snprintf(response, sizeof(response), "{\"created\":true,\"subject_id\":");
    if (used >= sizeof(response) || !studio_json_append(response, sizeof(response), &used, subject) ||
        (size_t)snprintf(response + used, sizeof(response) - used, ",\"presence_id\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, presence) ||
        (size_t)snprintf(response + used, sizeof(response) - used, ",\"title\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, title) ||
        (size_t)snprintf(response + used, sizeof(response) - used, ",\"address_constellation\":{\"web_url\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, url) ||
        (size_t)snprintf(response + used, sizeof(response) - used, ",\"bonfyrefs\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, fs_path) ||
        (size_t)snprintf(response + used, sizeof(response) - used, ",\"api\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, api) ||
        (size_t)snprintf(response + used, sizeof(response) - used, ",\"mcp\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, mcp) ||
        (size_t)snprintf(response + used, sizeof(response) - used, "},\"receipt_ref\":") >= sizeof(response) - used) goto overflow;
    used = strlen(response); if (!studio_json_append(response, sizeof(response), &used, receipt_ref) ||
        (size_t)snprintf(response + used, sizeof(response) - used, "}\n") >= sizeof(response) - used) goto overflow;
    if (studio_html_mode) {
        char e_title[384], e_url[1200], fragment[2048];
        int n;
        html_escape(title, e_title, sizeof(e_title)); html_escape(url, e_url, sizeof(e_url));
        n = snprintf(fragment, sizeof(fragment),
            "<section class=\"studio-result is-ok\" aria-live=\"polite\"><span class=\"result-kicker\">Life presence created</span>"
            "<h3>%s</h3><p>Your actor-owned address is now available at <a href=\"%s\">%s</a>. "
            "It is still only a local Matrix publication: no institutional role, relationship, payment, foreign enrollment, or external effect was created.</p>"
            "<p><a href=\"/cms/\">Prepare a governed carrier recipe</a></p></section>", e_title, e_url, e_url);
        if (n < 0 || n >= (int)sizeof(fragment)) goto overflow;
        printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s", fragment);
        return 1;
    }
    studio_reply(200, "application/json; charset=utf-8", response);
    return 1;
overflow:
    studio_reply(503, "application/json", "{\"error\":\"matrix_response_overflow\"}\n"); return 1;
}

/* A regular signed-in person may originate only their own, explicitly
 * self-scoped Life presence.  This retains no institution relationship,
 * provider state, payment authority, ForeignTwin observation, or external
 * effect.  studio_publish() still creates the receipt and enforces actor
 * ownership; this wrapper limits the publication grammar before it reaches
 * that general Matrix binder. */
static int studio_self_publish(BfMatrix *matrix, const char *body, const char *bearer,
                               const char *auth_db, const char *host) {
    char host_norm[256], kind[65], scope_text[16], form_json[640], title[161], slug[81];
    const char *publication_body = body;
    size_t used;
    if (!session_self_authoring(bearer, auth_db)) {
        studio_reply(403, "application/json", "{\"error\":\"self_authoring_scope_required\"}\n");
        return 1;
    }
    if (!lower_host(host, host_norm) || strcmp(host_norm, "aurekai.life")) {
        studio_reply(403, "application/json", "{\"error\":\"self_publication_requires_life_territory\"}\n");
        return 1;
    }
    if (!studio_json_string(body, "scope", scope_text, sizeof(scope_text))) {
        /* The human SurfaceIR uses a normal HTMX form. Compile its two
         * authorable fields into the fixed self-publication grammar here;
         * callers cannot smuggle kind/scope/territory through form fields. */
        if (!query_value(body, "title", title, sizeof(title)) || !studio_safe_title(title) ||
            !query_value(body, "slug", slug, sizeof(slug)) || !studio_safe_slug(slug)) {
            studio_reply(503, "application/json", "{\"error\":\"title_and_lowercase_slug_required\"}\n");
            return 1;
        }
        used = (size_t)snprintf(form_json, sizeof(form_json), "{\"title\":");
        if (used >= sizeof(form_json) || !studio_json_append(form_json, sizeof(form_json), &used, title) ||
            (size_t)snprintf(form_json + used, sizeof(form_json) - used, ",\"slug\":") >= sizeof(form_json) - used) goto form_overflow;
        used = strlen(form_json);
        if (!studio_json_append(form_json, sizeof(form_json), &used, slug) ||
            (size_t)snprintf(form_json + used, sizeof(form_json) - used,
                             ",\"kind\":\"PersonalProfile\",\"scope\":\"self\"}") >= sizeof(form_json) - used) goto form_overflow;
        publication_body = form_json;
    }
    if (!studio_json_string(publication_body, "scope", scope_text, sizeof(scope_text)) || strcmp(scope_text, "self") ||
        !studio_json_string(publication_body, "kind", kind, sizeof(kind)) || strcmp(kind, "PersonalProfile")) {
        studio_reply(403, "application/json", "{\"error\":\"self_publication_is_limited_to_PersonalProfile_self_scope\"}\n");
        return 1;
    }
    return studio_publish(matrix, publication_body, bearer, auth_db, host);
form_overflow:
    studio_reply(503, "application/json", "{\"error\":\"self_publication_form_overflow\"}\n");
    return 1;
}


/* ── Fleming estate surface ──────────────────────────────────────────────
 *
 * The Fleming pack's receipted observation history is columnar: the immutable
 * past lives in a Parquet file and rows admitted since the last compaction are
 * still in SQLite. A read must see exactly one row per observation, so a source
 * present in the hot table shadows that source in Parquet -- that is what makes
 * a re-harvest replace history instead of doubling it. The views are composed
 * per query rather than stored, because a copied table goes stale the moment a
 * harvest writes, and a stale copy that still answers is worse than none. */
static const char *fleming_root(void) {
    const char *configured = getenv("BONFYRE_FLEMING_ROOT");
    return configured && *configured ? configured : "/var/lib/bonfyre/state/fleming";
}

/* DuckDB resolves its extension directory under $HOME. This service runs with
 * ProtectHome=true, so $HOME is masked and `INSTALL sqlite` fails with
 * "Can't find the home directory" -- the extension the whole read path depends
 * on. Point it at a directory inside the unit's own ReadWritePaths instead. */
static const char *duckdb_home(void) {
    const char *configured = getenv("BONFYRE_DUCKDB_HOME");
    return configured && *configured ? configured : "/var/lib/bonfyre/state/duckdb";
}

static int fleming_query(const char *bindir, const char *sql, char *out, size_t out_sz) {
    const char *root = fleming_root();
    char lite[PATH_MAX], parquet[PATH_MAX], qbin[PATH_MAX];
    static char composed[32768];
    int n;

    if (snprintf(lite, sizeof(lite), "%s/estate-lite.db", root) >= (int)sizeof(lite)) return -1;
    if (snprintf(parquet, sizeof(parquet), "%s/estate.parquet/observation.parquet", root) >= (int)sizeof(parquet)) return -1;
    if (snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir) >= (int)sizeof(qbin)) return -1;
    if (access(lite, R_OK) != 0 || access(qbin, X_OK) != 0) return -1;

    if (access(parquet, R_OK) == 0) {
        n = snprintf(composed, sizeof(composed),
            "SET home_directory='%s'; "
            "INSTALL sqlite; LOAD sqlite; ATTACH '%s' AS hot (TYPE sqlite, READ_ONLY); "
            "CREATE OR REPLACE TEMP VIEW observation AS "
            "SELECT * FROM parquet_scan('%s') "
            "WHERE src_id NOT IN (SELECT DISTINCT src_id FROM hot.observation) "
            "UNION ALL SELECT * FROM hot.observation; "
            "CREATE OR REPLACE TEMP VIEW page AS SELECT * FROM hot.page; "
            "CREATE OR REPLACE TEMP VIEW harvest AS SELECT * FROM hot.harvest; "
            "CREATE OR REPLACE TEMP VIEW parcel_flat AS SELECT * FROM hot.parcel_flat; %s",
            duckdb_home(), lite, parquet, sql);
    } else {
        /* No compacted history yet: answer from the hot table alone rather than
         * failing, but never silently substitute a different question. */
        n = snprintf(composed, sizeof(composed),
            "SET home_directory='%s'; "
            "INSTALL sqlite; LOAD sqlite; ATTACH '%s' AS hot (TYPE sqlite, READ_ONLY); "
            "CREATE OR REPLACE TEMP VIEW observation AS SELECT * FROM hot.observation; "
            "CREATE OR REPLACE TEMP VIEW page AS SELECT * FROM hot.page; "
            "CREATE OR REPLACE TEMP VIEW harvest AS SELECT * FROM hot.harvest; "
            "CREATE OR REPLACE TEMP VIEW parcel_flat AS SELECT * FROM hot.parcel_flat; %s",
            duckdb_home(), lite, sql);
    }
    if (n < 0 || (size_t)n >= sizeof(composed)) return -1;

    {
        char *qargv[] = { qbin, "sql", ":memory:", composed, NULL };
        char diag[2048];
        int rc = auth_call_split(qargv, NULL, out, out_sz, diag, sizeof(diag));
        if (rc != 0) fprintf(stderr, "fleming_query: %s\n", diag);
        return rc;
    }
}

/* ── viewer identity ─────────────────────────────────────────────────────
 *
 * session_valid() answers only "is someone signed in". Several surfaces need
 * "who", because what a profile shows depends on whether the viewer IS that
 * person. The identity comes from the verified session and nothing else: it is
 * never read from a path, a query parameter, or a header the caller controls,
 * so there is no shape of request that lets one signed-in person be served
 * another person's private view. */
/* PageComposition is retained material, not the semantic owner.  This scene
 * records only the server-emitted regions so a future owned binding can take
 * an exact, continuity-preserving hand-off without asking the browser to
 * compose a page or discover owner material. */
#define PAGE_COMPAT_SCENE_REGION_MAX (BF_PAGE_REGION_COUNT * 2 + 8)
typedef struct {
    char id[48];
    char role[64];
    char label[128];
    char continuity[96];
    unsigned material;
} PageCompatSceneRegion;
typedef struct {
    PageCompatSceneRegion region[PAGE_COMPAT_SCENE_REGION_MAX];
    size_t count;
} PageCompatScene;

static PageCompatSceneRegion *page_surface_scene_record(PageCompatScene *scene,
                                                          const BfPageRegion *region,
                                                          size_t ordinal) {
    PageCompatSceneRegion *out;
    if (!scene || !region || scene->count >= PAGE_COMPAT_SCENE_REGION_MAX) return NULL;
    out = &scene->region[scene->count++];
    snprintf(out->id, sizeof(out->id), "page-region-%u", (unsigned)scene->count);
    snprintf(out->role, sizeof(out->role), "%s", region->role);
    snprintf(out->label, sizeof(out->label), "%s", region->label);
    snprintf(out->continuity, sizeof(out->continuity), "page:%s:%u", region->role, (unsigned)ordinal);
    out->material = (unsigned)region->material;
    return out;
}

typedef struct {
    long id;
    char email[192];
    char name[128];
    char tier[32];
} StudioViewer;

/* Reading a field that is not there must not leave the caller's buffer holding
 * whatever was in it.
 *
 * This returned 0 without touching `out`, which reads as harmless until a
 * caller's buffer is a stack slot the same function filled on the previous
 * request. /foreign/body did exactly that: a receipt line with no body_path
 * left bpath pointing at the last twin served, the path passed the "inside the
 * bodies directory" guard because it was a real body, and the estate answered
 * 200 with one twin's retained bytes under another twin's name. Clearing is
 * the contract; a caller that wants a default sets it after. */
static int studio_json_field(const char *json, const char *name, char *out, size_t size) {
    char key[96]; const char *p; size_t n = 0;
    if (out && size) out[0] = '\0';
    if (!json || snprintf(key, sizeof(key), "\"%s\"", name) >= (int)sizeof(key)) return 0;
    p = strstr(json, key); if (!p || !(p = strchr(p + strlen(key), ':'))) return 0;
    while (*++p == ' ' || *p == '\t') {}
    if (*p == '"') {
        p++;
        while (*p && *p != '"' && n + 1 < size) out[n++] = *p++;
    } else {
        while (*p && *p != ',' && *p != '}' && n + 1 < size) out[n++] = *p++;
    }
    out[n] = '\0';
    return n > 0;
}

static int session_identity(const char *bearer, const char *db, StudioViewer *who) {
    char out[2048], idbuf[32];
    const char *bin = auth_binary();
    if (!valid_token(bearer) || !who) return 0;
    memset(who, 0, sizeof(*who));
    {
        char *const argv[] = { (char*)bin, "--db", (char*)db, "verify", "--token", (char*)bearer, NULL };
        if (auth_call(argv, NULL, out, sizeof(out)) != 0) return 0;
    }
    if (!strstr(out, "\"valid\":true")) return 0;
    studio_json_field(out, "email", who->email, sizeof(who->email));
    studio_json_field(out, "name",  who->name,  sizeof(who->name));
    studio_json_field(out, "tier",  who->tier,  sizeof(who->tier));
    if (studio_json_field(out, "id", idbuf, sizeof(idbuf))) who->id = strtol(idbuf, NULL, 10);
    return who->email[0] != '\0';
}

static const char *page_request_bearer, *page_request_auth_db;
static char account_return_path[4096]="/";
static const char *account_signin_path="/login";
#include "account_navigation.h"
static void protocol_reply(int code, const char *type, const char *body);

/* Presentation class is deliberately narrower than identity or authority.
 *
 * The browser can ask which of the four authored presentation cells its
 * currently verified native closure may use, but cannot nominate an actor,
 * audience, institution, role, delegation, entitlement or authority in that
 * request. An institutional grant changes density/wording only; every data
 * disclosure and effect still crosses InformationPolicy and its native owner.
 */
static int presentation_grammar_allowed(const char *grammar) {
    size_t i;
    if (!grammar || !*grammar) return 0;
    for (i = 0; i < BF_NATIVE_PRESENTATION_CELL_COUNT; i++)
        if (!strcmp(bf_native_presentation_cells[i].grammar, grammar)) return 1;
    return 0;
}

static int presentation_has_forbidden_coordinate(const char *query) {
    static const char *const forbidden[] = {
        "actor", "actor_id", "audience", "institution", "role", "delegation", "entitlement",
        "authority", "effect_authority", "spend_authority", "payment_authority",
        "presentation_class", "presentation-class", NULL
    };
    char value[256]; size_t i;
    for (i = 0; forbidden[i]; i++)
        if (query_value(query, forbidden[i], value, sizeof(value))) return 1;
    return 0;
}

static const char *native_presentation_class(const char *bearer, const char *auth_db) {
    StudioViewer who;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char now[64];
    const char *answer = "anonymous";
    if (!session_identity(bearer, auth_db, &who)) return answer;
    answer = "personal";
    if (sqlite3_open_v2(auth_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT g.institution,g.role,g.expires_at FROM sessions s JOIN institution_grants g ON g.user_id=s.user_id "
        "WHERE s.token=? AND g.active=1", -1, &st, NULL) != SQLITE_OK) goto done;
    iso_utc_now(now, sizeof(now));
    sqlite3_bind_text(st, 1, bearer, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *institution = (const char *)sqlite3_column_text(st, 0);
        const char *role = (const char *)sqlite3_column_text(st, 1);
        const char *expires = (const char *)sqlite3_column_text(st, 2);
        if (expires && *expires && strcmp(now, expires) > 0) continue;
        /* Presentation classes are exact closure classes, not a role ladder.
         * Administering a client institution still means member/worker on the
         * shared Aurekai surface. Only the explicitly named Bonfyre admin
         * closure selects the owner/operator presentation. Nothing here grants
         * the operations that presentation happens to describe. */
        if (institution && role && !strcmp(institution, "bonfyre") && !strcmp(role, "admin")) {
            answer = "operator";
            break;
        }
        if (institution && *institution && role &&
            (!strcmp(role, "read") || !strcmp(role, "write") || !strcmp(role, "admin")))
            answer = "member_worker";
    }
done:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    return answer;
}

#endif /* BONFYRE_AUTH_ROUTES_AND_STUDIO_CORE_H */
