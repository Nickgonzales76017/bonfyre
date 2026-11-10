#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/stitch_identity.h"

static char *capture(void (*emit)(FILE *, const char *), const char *arg) {
    FILE *out = tmpfile(); assert(out);
    emit(out, arg);
    fflush(out); assert(fseek(out, 0, SEEK_END) == 0);
    long n = ftell(out); assert(n >= 0 && fseek(out, 0, SEEK_SET) == 0);
    char *buffer = calloc((size_t)n + 1, 1); assert(buffer);
    assert(fread(buffer, 1, (size_t)n, out) == (size_t)n);
    fclose(out);
    return buffer;
}

/* Visible words only: drop tags, then test the same banned list as
 * tests/page_composition_test.sh. */
static void assert_plain_words(const char *html, int no_numbers) {
    size_t n = strlen(html), j = 0; int in_tag = 0;
    char *text = calloc(n + 1, 1); assert(text);
    for (size_t i = 0; i < n; i++) {
        if (html[i] == '<') { in_tag = 1; text[j++] = ' '; continue; }
        if (html[i] == '>') { in_tag = 0; continue; }
        if (!in_tag) text[j++] = html[i];
    }
    /* A number in fixed copy would be an invented count. */
    if (no_numbers) for (const char *p = text; *p; p++) assert(*p < '0' || *p > '9');
    regex_t banned;
    assert(regcomp(&banned,
        "(^|[^[:alnum:]_])(worlds?|observed|observation|closure|candidates?|grammars?|projection|retained|admitted|"
        "occurrences?|zine|boundary|subject|fragment|renderer|carrier)([^[:alnum:]_]|$)"
        "|(^|[^[:alnum:]_])(person|artist|recording|program|community|venue|place):",
        REG_EXTENDED | REG_ICASE | REG_NOSUB) == 0);
    assert(regexec(&banned, text, 0, NULL, 0) == REG_NOMATCH);
    regfree(&banned);
    free(text);
}

static void lead_on_host(FILE *out, const char *host) { bf_stitch_identity_organisations_lead(out, host); }

int main(void) {
    /* Composer view: exact apex hosts and the exact route only. */
    assert(!strcmp(bf_stitch_identity_view("aurekai.work", "/organisations", NULL), "organisations"));
    assert(!strcmp(bf_stitch_identity_view("aurekai.life", "/organisations", "q=x"), "organisations"));
    assert(!strcmp(bf_stitch_identity_view("aurekai.shop", "/organisations", NULL), "organisations"));
    assert(bf_stitch_identity_view("aurekai.work", "/organizations", NULL) == NULL);
    assert(bf_stitch_identity_view("aurekai.work", "/people", NULL) == NULL);
    assert(bf_stitch_identity_view("aurekai.work", "/organisations/x", NULL) == NULL);
    assert(bf_stitch_identity_view("nick.aurekai.work", "/organisations", NULL) == NULL);
    assert(bf_stitch_identity_view("aurekai.work.evil.test", "/organisations", NULL) == NULL);
    assert(bf_stitch_identity_view("aurekai.example", "/organisations", NULL) == NULL);
    assert(bf_stitch_identity_view("example.com", "/organisations", NULL) == NULL);
    assert(bf_stitch_identity_view(NULL, "/organisations", NULL) == NULL);

    /* Every style block is scoped to its own id and nothing broader. */
    static const char *const views[] = {"organisations", "profile", "organizations"};
    for (size_t v = 0; v < sizeof(views) / sizeof(*views); v++) {
        char *css = capture(bf_stitch_identity_head, views[v]);
        char open[96], scope[96];
        snprintf(open, sizeof(open), "<style id=\"aurekai-identity-%s\">", views[v]);
        snprintf(scope, sizeof(scope), ":root:has(#aurekai-identity-%s)", views[v]);
        assert(!strncmp(css, open, strlen(open)));
        assert(strstr(css, scope) && strstr(css, "</style>"));
        assert(strlen(css) > 1024 && strlen(css) < 16384);
        /* Shared lane selectors and scripts never appear here. */
        assert(!strstr(css, "data-aurekai-pattern") && !strstr(css, "<script"));
        /* Bare element selectors would leak onto other pages. */
        assert(!strstr(css, "}body{") && !strstr(css, "}main{") && !strstr(css, "}section{"));
        free(css);
    }
    char *none = capture(bf_stitch_identity_head, "people");
    assert(!*none); free(none);
    none = capture(bf_stitch_identity_head, NULL);
    assert(!*none); free(none);

    /* The organisation band links only to existing owner pages, posts
     * nothing, and invents no organisation, person or number. */
    char *lead = capture(lead_on_host, "aurekai.work");
    assert(strstr(lead, "data-identity-org-lead"));
    assert(strstr(lead, "href=\"/organizations\""));
    assert(strstr(lead, "href=\"/organizations/join\""));
    assert(strstr(lead, "href=\"/people\""));
    assert(!strstr(lead, "<form") && !strstr(lead, "method=") && !strstr(lead, "<script"));
    assert(!strstr(lead, "Julian") && !strstr(lead, "Vane") && !strstr(lead, "Zürich"));
    assert_plain_words(lead, 1);
    free(lead);
    lead = capture(lead_on_host, "example.com");
    assert(!*lead); free(lead);

    printf("stitch_identity_test: PASS exact views, scoped styles, owner-only organisation actions, plain words\n");
    return 0;
}
