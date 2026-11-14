#ifndef BONFYRE_APP_SHELL_H
#define BONFYRE_APP_SHELL_H
/* The one application shell for pages that are built in C rather than
 * composed by page_core_rendering: the estate bar, a single <main>, the one
 * footer and the one player. A page states its title and its composition
 * pattern (workbench, spatial, ...) and writes only what goes inside <main>.
 * Layout for each pattern lives in /assets/composition.css; ui:* component
 * styles in /assets/authoring-reader.css. Callers print the HTTP head. */
static void ak_app_open_with_head(const char *title, const char *pattern, const char *route_class,
                                  void (*emit_head)(void *), void *head_context) {
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
          "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
          "<meta name=\"theme-color\" content=\"#0f1511\"><title>", stdout);
    html(title && *title ? title : "Aurekai");
    fputs(" \xc2\xb7 Aurekai</title>"
          "<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">"
          "<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:opsz,wght@7..72,400;7..72,600&amp;display=swap\">"
          "<link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\">"
          "<link rel=\"stylesheet\" href=\"/assets/composition.css\">", stdout);
    if (emit_head) emit_head(head_context);
    fputs("</head><body class=\"ak-app", stdout);
    if (route_class && *route_class) { fputc(' ', stdout); html(route_class); }
    fputs("\" data-aurekai-pattern=\"", stdout);
    html(pattern && *pattern ? pattern : "workbench");
    fputs("\">" BF_ESTATE_BAR "<main class=\"ak-wb\">", stdout);
}
static void ak_app_open(const char *title, const char *pattern, const char *route_class) {
    ak_app_open_with_head(title, pattern, route_class, NULL, NULL);
}
static void ak_app_close(void) {
    fputs("</main>" BF_ESTATE_FOOT "</body></html>", stdout);
}
static void ak_app_http(int code, int personal) {
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: %s\r\n"
           "Vary: Host, Accept, Cookie, Authorization\r\nX-Content-Type-Options: nosniff\r\n"
           "Referrer-Policy: same-origin\r\nConnection: close\r\n\r\n",
           code, code == 200 ? "OK" : code == 403 ? "Forbidden" : code == 404 ? "Not Found" : "Error",
           personal ? "private, no-store" : "public, max-age=60, stale-while-revalidate=300");
}

/* The private apps a member profile hosts as tabs. Each names its views and the
 * one view that may address a peer; profile routing and the sign-in return read
 * this table, so an app tab is a row here, never a branch in the routes. */
typedef struct { const char *tab; const char *views[6]; const char *peer_view; const char *icon; } AkAppTab;
static const AkAppTab ak_app_tabs[] = {
    {"dating", {"discover", "matches", "chat", "studio", NULL}, "chat", BF_ICON_HEART},
};
static const AkAppTab *ak_app_tab(const char *tab) {
    for (size_t i = 0; tab && i < sizeof(ak_app_tabs) / sizeof(ak_app_tabs[0]); i++)
        if (!strcmp(tab, ak_app_tabs[i].tab)) return &ak_app_tabs[i];
    return NULL;
}
static int ak_app_tab_view(const AkAppTab *app, const char *view) {
    for (size_t i = 0; app && view && i < sizeof(app->views) / sizeof(app->views[0]) && app->views[i]; i++)
        if (!strcmp(view, app->views[i])) return 1;
    return 0;
}
#endif
