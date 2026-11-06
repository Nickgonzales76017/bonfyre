#define _POSIX_C_SOURCE 200809L
#include "public_home_search.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int need(int ok, const char *label) {
    if (!ok) fprintf(stderr, "public home search: missing %s\n", label);
    return ok;
}

int main(void) {
    const char *fixture = "<!doctype html><html><head></head><body><section class=\"aperture\"></section><section class=\"wrap moves\"></section></body></html>";
    char *body = strdup(fixture);
    int ok = body != NULL;
    const char *first;
    if (!body) return 1;
    ok &= need(bf_apply_public_home_search(&body), "lowering");
    ok &= need(strstr(body, "id=\"estate-try\"") != NULL, "search aperture");
    ok &= need(strstr(body, "hx-get=\"/_surface/external-estate\"") != NULL, "HTMX endpoint");
    ok &= need(strstr(body, "name=\"q\"") != NULL, "query field");
    ok &= need(strstr(body, "name=\"grammar\" value=\"observations\"") != NULL, "observations grammar");
    ok &= need(strstr(body, "/external-estate/v1/") == NULL, "no browser JSON API");
    ok &= need(strstr(body, "createElement") == NULL, "no browser renderer");
    ok &= need(bf_apply_public_home_search(&body), "idempotent lowering");
    first = strstr(body, "id=\"estate-try\"");
    ok &= need(first && strstr(first + 1, "id=\"estate-try\"") == NULL, "single aperture");
    free(body);
    if (!ok) return 1;
    puts("public home search: PASS");
    return 0;
}
