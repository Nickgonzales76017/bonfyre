/* Render a prepared product-copy review as a printable page, and print it to
 * PDF through headless Chrome when the output ends in .pdf.
 *   product-review INPUT.json OUTPUT.html|OUTPUT.pdf
 * Reads local material only. It never fetches, publishes or contacts anyone:
 * the browser prints a local file from a throwaway profile with networking
 * pointed at nothing, and the product photo is read from its local path.
 * Replaces product-review-pdf.py (reportlab), retired 2026-11-06. */
#define _DEFAULT_SOURCE
#define _DARWIN_C_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "bf_json.h"
#include "bf_jsonw.h"

extern char **environ;
static bf_json_doc_t *doc;
static const bf_json_node_t *review;

static void die(const char *message) { fprintf(stderr, "product-review: %s\n", message); exit(1); }

static const bf_json_node_t *at(const bf_json_node_t *n, const char *path) {
    char key[64]; const char *p = path;
    while (n && *p) {
        size_t k = strcspn(p, "."); if (k >= sizeof key) return NULL;
        memcpy(key, p, k); key[k] = 0; p += k + (p[k] == '.');
        n = bf_json_obj_get(doc, n, key);
    }
    return n;
}

/* Decode a JSON string value (escapes included) into UTF-8. Caller frees. */
static char *text(const bf_json_node_t *n) {
    int len = 0; const char *s = n && n->type == BF_JSON_STRING ? bf_json_get_str(n, &len) : NULL;
    if (!s) die("review is missing a required text field");
    char *o = malloc((size_t)len * 3 + 1); size_t w = 0;
    for (int i = 0; i < len; i++) {
        if (s[i] != '\\' || i + 1 >= len) { o[w++] = s[i]; continue; }
        char c = s[++i];
        if (c == 'n') o[w++] = '\n'; else if (c == 't') o[w++] = '\t'; else if (c == 'r') o[w++] = '\r';
        else if (c == 'b') o[w++] = '\b'; else if (c == 'f') o[w++] = '\f';
        else if (c == 'u' && i + 4 < len) {
            unsigned cp = (unsigned)strtoul((char[5]){s[i + 1], s[i + 2], s[i + 3], s[i + 4], 0}, NULL, 16); i += 4;
            if (cp >= 0xd800 && cp < 0xdc00 && i + 6 < len && s[i + 1] == '\\' && s[i + 2] == 'u') {
                unsigned lo = (unsigned)strtoul((char[5]){s[i + 3], s[i + 4], s[i + 5], s[i + 6], 0}, NULL, 16);
                if (lo >= 0xdc00 && lo < 0xe000) { cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00); i += 6; }
            }
            if (cp < 0x80) o[w++] = (char)cp;
            else if (cp < 0x800) { o[w++] = (char)(0xc0 | cp >> 6); o[w++] = (char)(0x80 | (cp & 63)); }
            else if (cp < 0x10000) { o[w++] = (char)(0xe0 | cp >> 12); o[w++] = (char)(0x80 | (cp >> 6 & 63)); o[w++] = (char)(0x80 | (cp & 63)); }
            else { o[w++] = (char)(0xf0 | cp >> 18); o[w++] = (char)(0x80 | (cp >> 12 & 63)); o[w++] = (char)(0x80 | (cp >> 6 & 63)); o[w++] = (char)(0x80 | (cp & 63)); }
        } else o[w++] = c;
    }
    o[w] = 0; return o;
}
static char *field(const char *path) { return text(at(review, path)); }

static void esc(FILE *f, const char *s) {
    for (; *s; s++) switch (*s) {
        case '&': fputs("&amp;", f); break; case '<': fputs("&lt;", f); break; case '>': fputs("&gt;", f); break;
        case '"': fputs("&quot;", f); break; case '\'': fputs("&#39;", f); break; default: fputc(*s, f);
    }
}
static void put(FILE *f, const char *open, const char *path, const char *close) { char *s = field(path); fputs(open, f); esc(f, s); fputs(close, f); free(s); }
static void list(FILE *f, const char *path) {
    const bf_json_node_t *a = at(review, path);
    if (!a || a->type != BF_JSON_ARRAY) die("review is missing a required list");
    fputs("<ul>", f);
    for (const bf_json_node_t *n = bf_json_child_first(doc, a); n; n = bf_json_child_next(doc, n)) { char *s = text(n); fputs("<li>", f); esc(f, s); fputs("</li>", f); free(s); }
    fputs("</ul>\n", f);
}
static void row(FILE *f, const char *label, const char *value) { fputs("<tr><th>", f); esc(f, label); fputs("</th><td>", f); esc(f, value); fputs("</td></tr>\n", f); }

/* observed_at (ISO 8601, UTC) as a Central Time calendar date. */
static void central_date(const char *iso, char *out, size_t n) {
    struct tm t = {0}; int y, mo, d, h = 0, mi = 0, s = 0;
    if (sscanf(iso, "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &h, &mi, &s) < 3) die("observed_at is not an ISO date");
    t.tm_year = y - 1900; t.tm_mon = mo - 1; t.tm_mday = d; t.tm_hour = h; t.tm_min = mi; t.tm_sec = s;
    time_t at_utc = timegm(&t);
    setenv("TZ", "America/Chicago", 1); tzset();
    struct tm local; localtime_r(&at_utc, &local);
    strftime(out, n, "%B %d, %Y", &local);
}

static void render(FILE *f, const char *image_uri) {
    char date[64], *observed = field("source.observed_at"); central_date(observed, date, sizeof date); free(observed);
    char *client = field("client");
    fputs("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n<title>", f); esc(f, client);
    fputs(" product copy review</title>\n<meta name=\"author\" content=\"Nick Gonzales / Aurekai\">\n<style>\n"
          "@page{size:A4;margin:36pt 50pt 50pt}\n"
          ":root{--ink:#202623;--muted:#52605a;--accent:#a93329;--rule:#dfe3df}\n"
          "html{background:#fff}body{margin:0;color:var(--ink);font:9pt/12pt Helvetica,Arial,sans-serif}\n"
          "p,li{margin:0 0 5pt}ul{margin:0;padding-left:12pt}\n"
          ".label{font-weight:700;font-size:8pt;line-height:10pt;color:var(--accent);margin-bottom:5pt}\n"
          "h1{font-size:21pt;line-height:24pt;margin:0 0 8pt}h2{font-size:11pt;line-height:14pt;margin:9pt 0 6pt}\n"
          ".panel{display:flex;gap:10pt;align-items:flex-start}.panel>div{flex:1}.panel img{width:112pt;height:auto;flex:none}\n"
          "table{border-collapse:collapse;width:100%}th,td{text-align:left;vertical-align:top;padding:0 0 4pt;border-bottom:.3pt solid var(--rule)}\n"
          "th{width:112pt;font-weight:400;font-size:8pt;line-height:10pt;color:var(--muted)}\n"
          ".small{font-size:8pt;line-height:10pt;color:var(--muted)}a{color:inherit}\n"
          "footer{margin-top:14pt;padding-top:6pt;border-top:.5pt solid var(--rule);font-size:8pt;color:var(--muted)}\n"
          "</style></head><body>\n", f);
    fputs("<p class=\"label\">", f); esc(f, client); fputs(" / PRODUCT COPY REVIEW</p>\n", f); free(client);
    put(f, "<h1>", "title", "</h1>\n"); put(f, "<p>", "summary", "</p>\n");
    fputs("<div class=\"panel\"><div><h2>Proposed product copy</h2>\n", f); put(f, "<p>", "proposed_copy.description", "</p>\n");
    list(f, "proposed_copy.features");
    fputs("</div><img src=\"", f); esc(f, image_uri); fputs("\" alt=\"", f); { char *alt = field("proposed_copy.image_alt"); esc(f, alt); free(alt); } fputs("\"></div>\n", f);
    fputs("<h2>Search listing and image</h2>\n<table>\n", f);
    const char *labels[][2] = {{"Search title", "proposed_copy.seo_title"}, {"Search description", "proposed_copy.seo_description"},
                               {"Image filename", "proposed_copy.image_filename"}, {"Image alt text", "proposed_copy.image_alt"}};
    for (int i = 0; i < 4; i++) { char *v = field(labels[i][1]); row(f, labels[i][0], v); free(v); }
    { const bf_json_node_t *a = at(review, "proposed_copy.collection_proposals"); size_t cap = 1, w = 0; char *joined = calloc(1, 1);
      if (!a || a->type != BF_JSON_ARRAY) die("review is missing collection proposals");
      for (const bf_json_node_t *n = bf_json_child_first(doc, a); n; n = bf_json_child_next(doc, n)) {
          char *s = text(n); size_t k = strlen(s); joined = realloc(joined, cap += k + 2);
          if (w) { memcpy(joined + w, "; ", 2); w += 2; } memcpy(joined + w, s, k); w += k; joined[w] = 0; free(s); }
      row(f, "Collection candidates", joined); free(joined); }
    { char *price = field("source.price_observed"), *note = field("source.price_note"); size_t k = strlen(price) + strlen(note) + 3; char *v = malloc(k);
      snprintf(v, k, "%s. %s", price, note); row(f, "Legacy price", v); free(v); free(price); free(note); }
    fputs("</table>\n<h2>Before this goes into Shopify</h2>\n", f); list(f, "questions");
    { const bf_json_node_t *notes = at(review, "review_notes"); const bf_json_node_t *third = notes ? bf_json_arr_get(doc, notes, 2) : NULL;
      char *s = text(third); fputs("<p class=\"small\">", f); esc(f, s); fputs("</p>\n", f); free(s); }
    put(f, "<p>", "handoff", "</p>\n");
    fputs("<p class=\"small\">Sources: <a href=\"", f); { char *u = field("source.url"); esc(f, u); free(u); }
    fputs("\">Legacy product page</a> and <a href=\"", f); { char *u = field("source.image_url"); esc(f, u); free(u); }
    fputs("\">product photo</a>. Read ", f); esc(f, date); fputs(", Central Time. Proposed copy; pending client review.</p>\n", f);
    fputs("<footer>Prepared by Nick / Aurekai</footer>\n</body></html>\n", f);
}

static const char *chrome(void) {
    const char *env = getenv("BONFYRE_CHROME");
    if (env && *env) return env;
    static const char *known[] = {"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"};
    for (size_t i = 0; i < sizeof known / sizeof *known; i++) if (access(known[i], X_OK) == 0) return known[i];
    die("no Chrome found; set BONFYRE_CHROME"); return NULL;
}

static int print_pdf(const char *html, const char *pdf) {
    char profile[] = "/tmp/bonfyre-review-chrome.XXXXXX", url[PATH_MAX + 8], profile_arg[64], out_arg[PATH_MAX + 20];
    if (!mkdtemp(profile)) die("cannot create a browser profile");
    snprintf(url, sizeof url, "file://%s", html); snprintf(profile_arg, sizeof profile_arg, "--user-data-dir=%s", profile);
    snprintf(out_arg, sizeof out_arg, "--print-to-pdf=%s", pdf);
    char *argv[] = {(char *)chrome(), "--headless", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
                    "--proxy-server=127.0.0.1:9", "--proxy-bypass-list=<-loopback>", "--no-pdf-header-footer",
                    "--allow-file-access-from-files", profile_arg, out_arg, url, NULL};
    posix_spawn_file_actions_t fa; posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0); posix_spawn_file_actions_addopen(&fa, 2, "/dev/null", O_WRONLY, 0);
    /* Headless Chrome can write the PDF and then keep running, so the print
     * is done when the file ends in %%EOF; the browser is then stopped. */
    pid_t pid; int status = 1, printed = 0;
    unlink(pdf);
    if (posix_spawn(&pid, argv[0], &fa, NULL, argv, environ) == 0) {
        for (int tick = 0; tick < 600 && !printed; tick++) {
            if (waitpid(pid, &status, WNOHANG) == pid) { pid = 0; }
            FILE *f = fopen(pdf, "rb"); char tail[16] = {0};
            if (f) { if (!fseek(f, -6, SEEK_END)) { size_t k = fread(tail, 1, 6, f); tail[k] = 0; } fclose(f); }
            if (strstr(tail, "%%EOF")) printed = 1;
            else if (!pid) break;
            else usleep(100000);
        }
        if (pid) { kill(pid, SIGTERM); waitpid(pid, NULL, 0); }
    }
    posix_spawn_file_actions_destroy(&fa);
    pid_t rm; char *rmv[] = {"/bin/rm", "-rf", profile, NULL};
    if (posix_spawn(&rm, rmv[0], NULL, NULL, rmv, environ) == 0) waitpid(rm, NULL, 0);
    (void)status; return printed ? 0 : -1;
}

static int ends_with(const char *s, const char *suffix) { size_t a = strlen(s), b = strlen(suffix); return a >= b && !strcmp(s + a - b, suffix); }

int main(int argc, char **argv) {
    if (argc != 3 || !(ends_with(argv[2], ".pdf") || ends_with(argv[2], ".html"))) { fputs("usage: product-review INPUT.json OUTPUT.html|OUTPUT.pdf\n", stderr); return 2; }
    FILE *in = fopen(argv[1], "rb"); if (!in) die("cannot read the review");
    fseek(in, 0, SEEK_END); long n = ftell(in); fseek(in, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1); if (fread(buf, 1, (size_t)n, in) != (size_t)n) die("cannot read the review"); buf[n] = 0; fclose(in);
    char err[200]; doc = bf_json_parse(buf, (size_t)n, err, sizeof err); if (!doc) die(err);
    review = bf_json_root(doc);
    { char *schema = field("schema"), *state = field("state");
      if (strcmp(schema, "service-site.product-copy-review.v1") || strcmp(state, "prepared_for_review")) die("Expected a prepared product-copy review.");
      free(schema); free(state); }
    char image[PATH_MAX], image_uri[PATH_MAX + 8], *image_path = field("source.image_path");
    if (!realpath(image_path, image)) die("the product photo is not on disk"); free(image_path);
    snprintf(image_uri, sizeof image_uri, "file://%s", image);

    int pdf = ends_with(argv[2], ".pdf");
    char html[PATH_MAX + 8];
    if (pdf) snprintf(html, sizeof html, "%.*s.html", (int)(strlen(argv[2]) - 4), argv[2]); else snprintf(html, sizeof html, "%s", argv[2]);
    FILE *out = fopen(html, "wb"); if (!out) die("cannot write the page");
    render(out, image_uri); if (fclose(out)) die("cannot write the page");
    char resolved_html[PATH_MAX]; if (!realpath(html, resolved_html)) die("cannot resolve the page");
    char resolved_out[PATH_MAX];
    if (pdf) {
        char dir[PATH_MAX]; snprintf(dir, sizeof dir, "%s", resolved_html); *strrchr(dir, '/') = 0;
        const char *leaf = strrchr(argv[2], '/'); leaf = leaf ? leaf + 1 : argv[2];
        snprintf(resolved_out, sizeof resolved_out, "%s/%s", dir, leaf);
        if (print_pdf(resolved_html, resolved_out)) die("the browser did not print the page");
    } else snprintf(resolved_out, sizeof resolved_out, "%s", resolved_html);

    char *state = field("state"); bf_jsonw w; bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w); bf_jsonw_key(&w, "output"); bf_jsonw_str(&w, resolved_out); bf_jsonw_key(&w, "page"); bf_jsonw_str(&w, resolved_html);
    bf_jsonw_key(&w, "state"); bf_jsonw_str(&w, state); bf_jsonw_key(&w, "transmitted"); bf_jsonw_bool(&w, 0);
    bf_jsonw_key(&w, "merchant_published"); bf_jsonw_bool(&w, 0); bf_jsonw_obj_end(&w);
    if (w.oom) die("out of memory"); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w); free(state);
    bf_json_free(doc); free(buf); return 0;
}
