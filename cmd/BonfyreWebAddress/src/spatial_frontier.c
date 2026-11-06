/* The time-addressed World, served.
 *
 * The frontier was addressable before this file existed -- the spatial owner
 * could parse and render /w/<world>/spatial?at=...&as-of=... -- and it was not
 * reachable, because nothing routed that address to it. This is that route, and
 * it is deliberately thin.
 *
 * It does not reimplement the closure. The spatial owner holds the append-only
 * log and the bitemporal cut, and asking it is the whole of the work here;
 * WebAddress projects an answer it did not compute. A second implementation of
 * "what was there at 22:00" living in a web server is a second answer waiting to
 * disagree with the first.
 *
 * Disclosure is not decided here either, and the handler must not invent it. The
 * population it shows is exactly the one the AddressPlane index advertises,
 * which means a subject appears only because a published CMS row said it
 * participates. Publication is a decision the institution already made. Without
 * that index this route refuses rather than falling back to everything the
 * owner happens to hold: a spatial store is not a public document.
 *
 * Cannot infer, and stated in the response itself: a position being served is
 * not a position being current, disclosable to every audience, or authoritative;
 * and the frontier in the URL grants nothing -- a recipient opening this link
 * gets the closure recompiled, not a snapshot someone else could see.
 */
#define _POSIX_C_SOURCE 200809L
#include "spatial_frontier.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "bf_lex.h"
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FRONTIER_OUT 262144

/* Same shape the owner enforces. Two implementations of "is this an instant"
 * that disagree would let a URL mean one thing here and another there, so this
 * is the owner's rule repeated exactly and nothing more permissive. */
static int instant_valid(const char *t) {
    static const int digit[] = {0,1,2,3,5,6,8,9,11,12,14,15,17,18};
    if (!t || strlen(t) != 20) return 0;
    for (size_t i = 0; i < sizeof(digit)/sizeof(*digit); i++)
        if (!isdigit((unsigned char)t[digit[i]])) return 0;
    return t[4]=='-' && t[7]=='-' && t[10]=='T' && t[13]==':' && t[16]==':' && t[19]=='Z';
}

static int safe_coordinate(const char *s) {
    if (!s || !*s || strlen(s) > 128) return 0;
    for (const char *p = s; *p; p++)
        if (!isalnum((unsigned char)*p) && *p != '-' && *p != '_' && *p != ':' && *p != '.')
            return 0;
    return 1;
}

/* Delegates to the one implementation. Two copies of this existed under one
 * name and disagreed about malformed input -- one refused a truncated percent
 * escape and a control byte, the other passed both through as literal text.
 * The strict reading is the one that survived. */
static int query_value(const char *query, const char *key, char *out, size_t cap) {
    return bf_lex_url_query(query, key, out, cap);
}

static void html_escape(const char *in, char *out, size_t cap) {
    size_t j = 0;
    for (const char *p = in; p && *p && j + 7 < cap; p++) {
        switch (*p) {
            case '<': memcpy(out + j, "&lt;", 4);   j += 4; break;
            case '>': memcpy(out + j, "&gt;", 4);   j += 4; break;
            case '&': memcpy(out + j, "&amp;", 5);  j += 5; break;
            case '"': memcpy(out + j, "&quot;", 6); j += 6; break;
            default:  out[j++] = *p;
        }
    }
    out[j] = '\0';
}

static void reply(int code, const char *status, const char *type, const char *body) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, status, type, strlen(body), body);
    fflush(stdout);
}

/* Ask the owner. No shell: the frontier comes off the wire and a shell would
 * make a query coordinate an execution coordinate. */
static int ask_owner(char *const argv[], char *out, size_t cap) {
    int fds[2];
    pid_t pid;
    size_t used = 0;
    int status = 0;
    if (pipe(fds) != 0) return -1;
    if ((pid = fork()) < 0) { close(fds[0]); close(fds[1]); return -1; }
    if (pid == 0) {
        close(fds[0]);
        if (dup2(fds[1], STDOUT_FILENO) < 0) _exit(127);
        close(fds[1]);
        execv(argv[0], argv);
        _exit(127);
    }
    close(fds[1]);
    for (;;) {
        ssize_t n = read(fds[0], out + used, cap - used - 1);
        if (n <= 0) break;
        used += (size_t)n;
        if (used + 1 >= cap) break;
    }
    out[used] = '\0';
    close(fds[0]);
    if (waitpid(pid, &status, 0) < 0) return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

int serve_spatial_frontier(const char *method, const char *target,
                           const char *query, const char *accept) {
    char world[128] = {0};
    char at[32] = {0}, as_of[32] = {0}, cell[96] = {0}, region[96] = {0}, subject[256] = {0};
    char owner_out[FRONTIER_OUT];
    char bin[PATH_MAX], spatial_db[PATH_MAX], index_db[PATH_MAX];
    const char *e;
    const char *tail, *slash;

    if (!method || strcmp(method, "GET") || !target) return 0;
    if (strncmp(target, "/w/", 3)) return 0;
    tail = target + 3;
    slash = strchr(tail, '/');
    if (!slash || strcmp(slash, "/spatial")) return 0;
    if ((size_t)(slash - tail) >= sizeof(world) || slash == tail) return 0;
    memcpy(world, tail, (size_t)(slash - tail));
    world[slash - tail] = '\0';
    if (!safe_coordinate(world)) return 0;

    /* No frontier means this is the ordinary spatial surface, which someone else
     * already serves. Claiming the route here would silently replace it. */
    if (!query || !query_value(query, "at", at, sizeof(at))) return 0;

    if (!instant_valid(at)) {
        reply(400, "Bad Request", "application/json; charset=utf-8",
              "{\"error\":\"at must be an instant shaped YYYY-MM-DDTHH:MM:SSZ\","
              "\"truth\":\"a half-read frontier would show a reader an instant they did not ask for\"}\n");
        return 1;
    }
    if (!query_value(query, "as-of", as_of, sizeof(as_of)) || !as_of[0])
        snprintf(as_of, sizeof(as_of), "%s", at);
    if (!instant_valid(as_of)) {
        reply(400, "Bad Request", "application/json; charset=utf-8",
              "{\"error\":\"as-of must be an instant shaped YYYY-MM-DDTHH:MM:SSZ\"}\n");
        return 1;
    }
    (void)query_value(query, "cell", cell, sizeof(cell));
    (void)query_value(query, "region", region, sizeof(region));
    (void)query_value(query, "subject", subject, sizeof(subject));
    if ((cell[0] && !safe_coordinate(cell)) || (region[0] && !safe_coordinate(region)) ||
        (subject[0] && !safe_coordinate(subject))) {
        reply(400, "Bad Request", "application/json; charset=utf-8",
              "{\"error\":\"a projection coordinate carried a shape this route will not pass on\"}\n");
        return 1;
    }

    e = getenv("BONFYRE_SPATIAL_DB");
    if (!e || !*e || strlen(e) >= sizeof(spatial_db)) {
        reply(503, "Service Unavailable", "application/json; charset=utf-8",
              "{\"error\":\"no spatial store is configured here\","
              "\"truth\":\"answering an empty frontier would read as a world with nothing in it\"}\n");
        return 1;
    }
    snprintf(spatial_db, sizeof(spatial_db), "%s", e);
    /* The advertised population is the disclosure decision. Without the index
     * there is no published set, and everything the owner holds is not a
     * substitute for it. */
    e = getenv("BONFYRE_EXTERNAL_ESTATE_DB");
    if (!e || !*e || strlen(e) >= sizeof(index_db)) {
        reply(503, "Service Unavailable", "application/json; charset=utf-8",
              "{\"error\":\"no advertised population is available to this route\","
              "\"truth\":\"publication is the disclosure decision and the owner's whole store is not a public document\"}\n");
        return 1;
    }
    snprintf(index_db, sizeof(index_db), "%s", e);

    e = getenv("BONFYRE_BIN_DIR");
    snprintf(bin, sizeof(bin), "%s/bonfyre-spatial", (e && *e) ? e : "/usr/local/bin");

    {
        char *argv[20];
        int n = 0;
        argv[n++] = bin;
        argv[n++] = (char *)"world";
        argv[n++] = (char *)"--db";        argv[n++] = spatial_db;
        argv[n++] = (char *)"--index";     argv[n++] = index_db;
        argv[n++] = (char *)"--world";     argv[n++] = world;
        argv[n++] = (char *)"--at";        argv[n++] = at;
        argv[n++] = (char *)"--as-of";     argv[n++] = as_of;
        if (cell[0])   { argv[n++] = (char *)"--cell";   argv[n++] = cell; }
        if (region[0]) { argv[n++] = (char *)"--region"; argv[n++] = region; }
        argv[n] = NULL;
        if (ask_owner(argv, owner_out, sizeof(owner_out)) != 0) {
            reply(502, "Bad Gateway", "application/json; charset=utf-8",
                  "{\"error\":\"the spatial owner did not answer\","
                  "\"truth\":\"an unanswered owner is not an empty world\"}\n");
            return 1;
        }
    }

    if (accept && strstr(accept, "text/html")) {
        static char page[FRONTIER_OUT + 8192];
        char e_world[512], e_at[64], e_as_of[64];
        html_escape(world, e_world, sizeof(e_world));
        html_escape(at, e_at, sizeof(e_at));
        html_escape(as_of, e_as_of, sizeof(e_as_of));
        snprintf(page, sizeof(page),
          "<!doctype html><meta charset=\"utf-8\">"
          "<title>%s at %s</title>"
          "<article class=\"bf-spatial-frontier\">"
          "<p class=\"eyebrow\">World &middot; spatial &middot; reconstructed frontier</p>"
          "<h2>%s as it stood at %s</h2>"
          "<p class=\"bf-frontier\">Valid instant <time datetime=\"%s\">%s</time>, "
          "as this estate understood it at <time datetime=\"%s\">%s</time>. "
          "Two instants, because when a position held and when we learned of it "
          "are different questions.</p>"
          "<p class=\"bf-why\">Every subject here appears because a published row said it "
          "participates in this World. A position shown is not a position that is current, "
          "disclosable to every audience, or authoritative, and this address grants nothing: "
          "opening it recompiles the closure rather than handing over someone else's view.</p>"
          "<script type=\"application/json\" id=\"bf-spatial-closure\">%s</script>"
          "</article>",
          e_world, e_at, e_world, e_at, e_at, e_at, e_as_of, e_as_of, owner_out);
        reply(200, "OK", "text/html; charset=utf-8", page);
    } else {
        reply(200, "OK", "application/json; charset=utf-8", owner_out);
    }
    return 1;
}
