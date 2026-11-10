/* detail_depth_witness -- how far a visitor can keep going from a detail page.
 *
 * Plain HTTP against a running bonfyre-web-address.  From each start page it
 * walks only links a person can click, only to detail pages on the same host,
 * never revisiting a page in the same chain, and looks for one chain of at
 * least --hops hops that passes through every kind the walk names:
 *
 *   item     /o/<source>/<key>
 *   feed     /channels?channel=  /channels?root=&family=  /channels?topic=  /channels?named=
 *   profile  /people?who=  /channels?root=<host>
 *   tool     /channels?way=  /capability/<name>  /protocols/<name>
 *   app      /s/apps/<name>  /s/record-kind/<name>
 *
 * The walk prefers a kind the chain has not crossed yet, then a kind other
 * than the current page's, and backtracks inside a fetch budget.  The result
 * is an existence proof per start, not a shortest path and not a diameter.
 *
 * Output: one line per start ("start hops kinds chain..."), then a summary
 * line with the distribution of the best chain found.  Exit 1 when any start
 * fails --require (default: report only).
 *
 *   detail_depth_witness --port P --host H [--hops 10] [--budget 400]
 *                        [--require] START...
 */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

enum { K_NONE = 0, K_ITEM = 1, K_FEED = 2, K_PROFILE = 4, K_TOOL = 8, K_APP = 16 };
#define K_CROSS (K_FEED | K_PROFILE | K_TOOL | K_APP)
#define MAX_NODES 60000
#define MAX_LINKS 160
#define MAX_DEPTH 32

typedef struct { char *path; int kind, fetched, status, nlinks; int *links; } Node;
static Node nodes[MAX_NODES];
static int node_count, fetches, port = 0, budget = 400, want_hops = 10;
static const char *host = "aurekai.live";

static int kind_of(const char *p) {
    if (!strncmp(p, "/o/", 3)) {
        const char *s = strchr(p + 3, '/');
        if (s && strlen(s + 1) == 24 && strspn(s + 1, "0123456789abcdef") == 24) return K_ITEM;
        return K_NONE;
    }
    if (strstr(p, "from=") || strstr(p, "context=")) return K_NONE;   /* pagination and account views */
    if (!strncmp(p, "/channels?", 10)) {
        const char *q = p + 10;
        if (!strncmp(q, "channel=", 8) || strstr(q, "family=") || !strncmp(q, "topic=", 6) || !strncmp(q, "named=", 6)) return K_FEED;
        if (!strncmp(q, "way=", 4)) return K_TOOL;
        if (!strncmp(q, "root=", 5)) return K_PROFILE;
        return K_NONE;
    }
    if (!strncmp(p, "/people?who=", 12)) return K_PROFILE;
    if (!strncmp(p, "/capability/", 12) && p[12]) return K_TOOL;
    if (!strncmp(p, "/protocols/", 11) && p[11]) return K_TOOL;
    if (!strncmp(p, "/s/apps/", 8) && p[8] && strcmp(p + 8, "all")) return K_APP;
    if (!strncmp(p, "/s/record-kind/", 15) && p[15]) return K_APP;
    return K_NONE;
}

static const char *kind_name(int k) {
    return k == K_ITEM ? "item" : k == K_FEED ? "feed" : k == K_PROFILE ? "profile" : k == K_TOOL ? "tool" : k == K_APP ? "app" : "-";
}

#define SLOTS 131072
static int slot[SLOTS];
static int node_id(const char *path) {
    unsigned h = 2166136261u;
    for (const unsigned char *c = (const unsigned char *)path; *c; c++) h = (h ^ *c) * 16777619u;
    for (unsigned s = h & (SLOTS - 1);; s = (s + 1) & (SLOTS - 1)) {
        if (!slot[s]) {
            if (node_count >= MAX_NODES) return -1;
            nodes[node_count].path = strdup(path);
            nodes[node_count].kind = kind_of(path);
            slot[s] = ++node_count;
            return node_count - 1;
        }
        if (!strcmp(nodes[slot[s] - 1].path, path)) return slot[s] - 1;
    }
}

static char *http_get(const char *path, int *status) {
    struct sockaddr_in a = {0};
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    char req[8192]; size_t cap = 1 << 20, len = 0; char *buf;
    *status = 0;
    if (fd < 0) return NULL;
    a.sin_family = AF_INET; a.sin_port = htons((unsigned short)port); inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (connect(fd, (struct sockaddr *)&a, sizeof(a))) { close(fd); return NULL; }
    int n = snprintf(req, sizeof(req), "GET %s HTTP/1.1\r\nHost: %s\r\nAccept: text/html\r\nConnection: close\r\n\r\n", path, host);
    if (n <= 0 || n >= (int)sizeof(req) || write(fd, req, (size_t)n) != n) { close(fd); return NULL; }
    if (!(buf = malloc(cap))) { close(fd); return NULL; }
    for (;;) {
        if (len + 65536 > cap) { char *g = realloc(buf, cap *= 2); if (!g) break; buf = g; }
        ssize_t r = read(fd, buf + len, cap - len - 1);
        if (r <= 0) break;
        len += (size_t)r;
    }
    close(fd);
    buf[len] = '\0';
    sscanf(buf, "HTTP/1.%*d %d", status);
    return buf;
}

/* Every same-host link a person can follow, decoded from &amp;. */
static void fetch(int id) {
    Node *nd = &nodes[id];
    char *body, *p;
    if (nd->fetched) return;
    nd->fetched = 1; fetches++;
    nd->links = calloc(MAX_LINKS, sizeof(int));
    if (!(body = http_get(nd->path, &nd->status)) || nd->status != 200) { free(body); return; }
    char origin[300]; snprintf(origin, sizeof(origin), "https://%s/", host);
    for (p = body; (p = strstr(p, "href=")) && nd->nlinks < MAX_LINKS; p += 5) {
        char q = p[5], link[4096]; size_t w = 0; const char *s = p + 6;
        if (q != '"' && q != '\'') continue;
        if (!strncmp(s, origin, strlen(origin))) s += strlen(origin) - 1;
        if (*s != '/' || s[1] == '/') continue;
        for (; *s && *s != q && w + 1 < sizeof(link); s++) {
            if (!strncmp(s, "&amp;", 5)) { link[w++] = '&'; s += 4; }
            else link[w++] = *s;
        }
        link[w] = '\0';
        { char *hash = strchr(link, '#'); if (hash) *hash = '\0'; }
        if (!kind_of(link) || !strcmp(link, nd->path)) continue;
        int to = node_id(link), dup = 0;
        if (to < 0) continue;
        for (int i = 0; i < nd->nlinks; i++) if (nd->links[i] == to) dup = 1;
        if (!dup) nd->links[nd->nlinks++] = to;
    }
    free(body);
}

static int chain[MAX_DEPTH], best[MAX_DEPTH], best_len, best_mask, start_budget;

static int mask_of(const int *c, int n) { int m = 0; for (int i = 0; i < n; i++) m |= nodes[c[i]].kind; return m; }
static int in_chain(int id, int n) { for (int i = 0; i < n; i++) if (chain[i] == id) return 1; return 0; }
static int bits(int m) { int b = 0; for (; m; m &= m - 1) b++; return b; }

/* Is this chain better than the best: full crossing first, then length. */
static void consider(int n) {
    int m = mask_of(chain, n);
    int full = (m & K_CROSS) == K_CROSS, best_full = (best_mask & K_CROSS) == K_CROSS;
    int hops = n - 1, best_hops = best_len - 1;
    int better = (full && hops >= want_hops) ? !(best_full && best_hops >= want_hops) || hops > best_hops
               : !(best_full && best_hops >= want_hops) && (bits(m & K_CROSS) > bits(best_mask & K_CROSS) ||
                   (bits(m & K_CROSS) == bits(best_mask & K_CROSS) && hops > best_hops));
    if (better) { memcpy(best, chain, sizeof(int) * (size_t)n); best_len = n; best_mask = m; }
}

static int done(void) { return best_len - 1 >= want_hops && (best_mask & K_CROSS) == K_CROSS; }

static void walk(int n) {
    int id = chain[n - 1], order[MAX_LINKS], count = 0, have = mask_of(chain, n);
    consider(n);
    if (done() || n >= MAX_DEPTH || n > want_hops + 1 || fetches >= start_budget) return;
    fetch(id);
    /* Unseen kinds first, then a change of kind, then the rest; at most six. */
    for (int pass = 0; pass < 3; pass++)
        for (int i = 0; i < nodes[id].nlinks && count < MAX_LINKS; i++) {
            int to = nodes[id].links[i], k = nodes[to].kind;
            int cls = (k & ~have & K_CROSS) ? 0 : k != nodes[id].kind ? 1 : 2;
            if (cls == pass && !in_chain(to, n)) order[count++] = to;
        }
    for (int i = 0; i < count && i < 6 && !done() && fetches < start_budget; i++) {
        chain[n] = order[i];
        walk(n + 1);
    }
}

int main(int argc, char **argv) {
    int require = 0, starts = 0, passed = 0, hist[MAX_DEPTH + 1] = {0}, crossing = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--port") && i + 1 < argc) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--host") && i + 1 < argc) host = argv[++i];
        else if (!strcmp(argv[i], "--hops") && i + 1 < argc) want_hops = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--budget") && i + 1 < argc) budget = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--require")) require = 1;
    }
    if (!port || want_hops < 1 || want_hops + 1 >= MAX_DEPTH) { fprintf(stderr, "usage: detail_depth_witness --port P [--host H] [--hops N] [--budget F] [--require] START...\n"); return 2; }
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] != '/') { if (!strcmp(argv[i], "--port") || !strcmp(argv[i], "--host") || !strcmp(argv[i], "--hops") || !strcmp(argv[i], "--budget")) i++; continue; }
        int id = node_id(argv[i]);
        if (id < 0) continue;
        starts++;
        best_len = 1; best[0] = id; best_mask = nodes[id].kind;
        start_budget = fetches + budget;
        chain[0] = id;
        walk(1);
        int hops = best_len - 1, full = (best_mask & K_CROSS) == K_CROSS;
        hist[hops]++;
        if (full) crossing++;
        if (full && hops >= want_hops) passed++;
        printf("start %s hops=%d kinds=", nodes[id].path, hops);
        for (int k = K_ITEM; k <= K_APP; k <<= 1) if (best_mask & k) printf("%s%s", kind_name(k), (best_mask & ~(2 * k - 1)) ? "," : "");
        printf(" crossing=%s status=%d\n", full ? "all-four" : "partial", nodes[id].status);
        for (int c = 0; c < best_len; c++) printf("  %2d %-7s %s\n", c, kind_name(nodes[best[c]].kind), nodes[best[c]].path);
    }
    printf("summary starts=%d passed=%d crossing_all_four=%d target_hops=%d fetches=%d distribution=", starts, passed, crossing, want_hops, fetches);
    for (int h = 0; h <= MAX_DEPTH; h++) if (hist[h]) printf("%d:%d ", h, hist[h]);
    printf("\n");
    return require && passed < starts ? 1 : 0;
}
