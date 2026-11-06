#ifndef BONFYRE_STITCH_IDENTITY_H
#define BONFYRE_STITCH_IDENTITY_H
/* Stitch lane 1 (identity), 2026-11-06: People, profile and organisation
 * pages take the dark Stitch layout. This file is presentation only. Data
 * comes from the owners that already render these pages: people_projection.c
 * for /people and /organisations, profile_home.h for /profile and member
 * sites, organization_workspace_web.h for the signed-in /organizations
 * workspace. Every action links to a form or page those owners already
 * serve; nothing here reads a store, grants access or creates a route.
 *
 * Each style block is scoped to :root:has(#aurekai-identity-<view>), so a
 * rule fires only on a page that emitted that block. The shared
 * data-aurekai-pattern styles from stitch_work_presentation.h, which other
 * lanes also use, are left as they are. */
#include <stdio.h>
#include <string.h>

/* Composer pages restyled by this lane: only exact Aurekai apex hosts, only
 * these exact routes. Profile and workspace pages name their view directly
 * because their owners also answer on member hosts (nick.aurekai.work). */
static const char *bf_stitch_identity_view(const char *host, const char *route, const char *query) {
    const char *apex;
    (void)query;
    if (!host || !route || strncmp(host, "aurekai.", 8)) return NULL;
    apex = host + 8;
    if (strcmp(apex, "work") && strcmp(apex, "life") && strcmp(apex, "live") && strcmp(apex, "shop")) return NULL;
    if (!strcmp(route, "/organisations")) return "organisations";
    return NULL;
}

#define BF_SI_TOKENS \
    "--si-ground:#0f1511;--si-panel:#171d19;--si-high:#252b27;--si-soft:#303631;" \
    "--si-line:#444935;--si-ink:#dee4dd;--si-dim:#c5c9af;--si-accent:#d0fc4f;--si-on-accent:#283500;" \
    "--si-sans:'DM Sans',system-ui,sans-serif;--si-serif:Literata,Georgia,serif;"

/* /organisations: the public organisation directory (#9). */
static const char bf_si_organisations_css[] =
    ":root:has(#aurekai-identity-organisations) body{" BF_SI_TOKENS
    "--paper:#0f1511;--ink:#dee4dd;--dim:#c5c9af;--rule:#444935;--panel:#171d19;--accent:#d0fc4f;"
    "background:var(--si-ground);color:var(--si-ink);color-scheme:dark}"
    ":root:has(#aurekai-identity-organisations) .wrap>.hero{border-bottom:0;padding-block:clamp(34px,5vw,72px) 28px}"
    ":root:has(#aurekai-identity-organisations) .wrap>.hero h1{color:var(--si-ink);font:600 clamp(38px,4.2vw,56px)/1.08 var(--si-sans);letter-spacing:-.035em;margin:4px 0 18px}"
    ":root:has(#aurekai-identity-organisations) .wrap>.hero .lede{font:400 clamp(18px,1.8vw,24px)/1.5 var(--si-serif);color:var(--si-dim);max-width:58ch}"
    ".si-org-lead{display:grid;grid-template-columns:minmax(0,7fr) minmax(0,5fr);gap:clamp(18px,3vw,40px);align-items:center;"
    "margin:8px 0 40px;padding:clamp(22px,3.5vw,44px);background:var(--si-panel);border:1px solid var(--si-high);border-radius:32px}"
    ".si-org-lead h2{margin:0 0 10px;font:600 clamp(24px,2.5vw,32px)/1.15 var(--si-sans);letter-spacing:-.025em;color:var(--si-ink)}"
    ".si-org-lead p{margin:0;font:400 16px/1.65 var(--si-serif);color:var(--si-dim);max-width:60ch}"
    ".si-org-lead .si-eyebrow{font:600 11px/1.3 var(--si-sans);letter-spacing:.08em;text-transform:uppercase;color:var(--si-accent);margin:0 0 10px}"
    ".si-org-actions{display:flex;flex-wrap:wrap;gap:10px;justify-content:flex-end}"
    ".si-org-actions a{display:inline-flex;align-items:center;min-height:44px;padding:10px 20px;border-radius:999px;"
    "background:var(--si-high);color:var(--si-ink);border:1px solid var(--si-line);font:600 13px/1.2 var(--si-sans);text-decoration:none}"
    ".si-org-actions a:first-child{background:var(--si-accent);color:var(--si-on-accent);border-color:var(--si-accent)}"
    ".si-org-actions a:is(:hover,:focus-visible){outline:2px solid var(--si-accent);outline-offset:2px}"
    ":root:has(#aurekai-identity-organisations) .region-orgs{margin-top:8px}"
    ":root:has(#aurekai-identity-organisations) .region-orgs .region-label{font:600 clamp(24px,2.5vw,32px)/1.15 var(--si-sans);letter-spacing:-.025em;color:var(--si-ink);text-transform:none;margin:0 0 6px}"
    ":root:has(#aurekai-identity-organisations) .region-orgs .source-count{font:600 11px/1.3 var(--si-sans);letter-spacing:.08em;text-transform:uppercase;color:var(--si-dim);margin:0 0 18px}"
    ":root:has(#aurekai-identity-organisations) .ledger-find{display:flex;align-items:center;gap:8px;max-width:560px;margin:0 0 22px;padding:7px 7px 7px 18px;"
    "border:1px solid var(--si-soft);border-radius:999px;background:var(--si-panel)}"
    ":root:has(#aurekai-identity-organisations) .ledger-find label{flex:1;display:flex;align-items:center;gap:10px;min-width:0;font:600 12px var(--si-sans);color:var(--si-dim)}"
    ":root:has(#aurekai-identity-organisations) .ledger-find input{flex:1;min-width:0;min-height:38px;background:transparent!important;border:0!important;color:var(--si-ink)!important;font:500 14px var(--si-sans)}"
    ":root:has(#aurekai-identity-organisations) .ledger-find button{border:0;border-radius:999px;min-height:38px;padding:7px 18px;background:var(--si-accent);color:var(--si-on-accent);font:700 12px var(--si-sans);cursor:pointer}"
    ":root:has(#aurekai-identity-organisations) .source-roots{list-style:none;margin:0;padding:0;display:grid;"
    "grid-template-columns:repeat(auto-fill,minmax(min(100%,300px),1fr));gap:12px}"
    ":root:has(#aurekai-identity-organisations) .source-roots>li{display:grid;gap:10px;align-content:start;padding:22px 24px;min-width:0;"
    "background:var(--si-panel);border:1px solid var(--si-high);border-radius:24px}"
    ":root:has(#aurekai-identity-organisations) .source-roots>li>a{font:600 19px/1.25 var(--si-sans);letter-spacing:-.015em;color:var(--si-ink);text-decoration:none;overflow-wrap:anywhere}"
    ":root:has(#aurekai-identity-organisations) .source-roots>li>a:hover{color:var(--si-accent)}"
    ":root:has(#aurekai-identity-organisations) .source-roots .by{display:flex;flex-wrap:wrap;gap:6px}"
    ":root:has(#aurekai-identity-organisations) .source-roots .by>*{display:inline-flex;align-items:center;padding:5px 11px;border-radius:999px;"
    "background:var(--si-high);color:var(--si-dim);font:600 11px/1.3 var(--si-sans);text-decoration:none;overflow-wrap:anywhere}"
    ":root:has(#aurekai-identity-organisations) .source-roots .by a{color:var(--si-ink)}"
    ":root:has(#aurekai-identity-organisations) .source-roots .by a:hover{background:var(--si-accent);color:var(--si-on-accent)}"
    ":root:has(#aurekai-identity-organisations) .region-orgs .pager{display:flex;justify-content:center;margin:28px 0 0}"
    ":root:has(#aurekai-identity-organisations) .region-orgs .pager a{display:inline-flex;padding:10px 20px;border-radius:999px;border:1px solid var(--si-line);"
    "background:var(--si-high);color:var(--si-ink);font:600 12px var(--si-sans);text-decoration:none}"
    "@media(max-width:800px){.si-org-lead{grid-template-columns:1fr;border-radius:24px}.si-org-actions{justify-content:flex-start}}"
    "@media(max-width:540px){:root:has(#aurekai-identity-organisations) .ledger-find{flex-wrap:wrap;border-radius:18px;padding:10px}"
    ":root:has(#aurekai-identity-organisations) .ledger-find button{flex:1}}";

/* /profile and member sites (#7, #2, #1): the light profile shell from
 * profile_home.h in the dark Stitch palette. */
static const char bf_si_profile_css[] =
    ":root:has(#aurekai-identity-profile){" BF_SI_TOKENS "color-scheme:dark}"
    ":root:has(#aurekai-identity-profile) body{background:var(--si-ground);color:var(--si-ink);font:15px/1.6 var(--si-sans)}"
    ":root:has(#aurekai-identity-profile) .ph-shell{--ph-ink:#dee4dd;--ph-paper:#0f1511;--ph-line:#303631;--ph-muted:#c5c9af}"
    ":root:has(#aurekai-identity-profile) .ph-cover{background:linear-gradient(110deg,#0a100c,#1b211d 55%,#3b4d00);border-radius:32px 32px 0 0}"
    ":root:has(#aurekai-identity-profile) .ph-identity{background:var(--si-panel);border-color:var(--si-high);border-radius:0 0 32px 32px}"
    ":root:has(#aurekai-identity-profile) .ph-avatar{background:var(--si-high);color:var(--si-accent);border-color:var(--si-panel);font:600 46px var(--si-sans)}"
    ":root:has(#aurekai-identity-profile) .ph-name h1{font:600 clamp(36px,4.2vw,56px)/1.08 var(--si-sans);letter-spacing:-.035em;color:var(--si-ink)}"
    ":root:has(#aurekai-identity-profile) .ph-kicker{font:600 11px/1.3 var(--si-sans);letter-spacing:.08em;color:var(--si-accent)}"
    ":root:has(#aurekai-identity-profile) .ph-name p{font:400 17px/1.65 var(--si-serif);color:var(--si-dim)}"
    ":root:has(#aurekai-identity-profile) .ph-shell a{color:var(--si-ink)}"
    ":root:has(#aurekai-identity-profile) :is(.ph-shell button,.ph-actions a,.ph-button){background:var(--si-high);color:var(--si-ink);border-color:var(--si-line);border-radius:999px;font:600 13px var(--si-sans)}"
    ":root:has(#aurekai-identity-profile) :is(.ph-actions a:first-child,.ph-shell button[type=submit]){background:var(--si-accent);color:var(--si-on-accent);border-color:var(--si-accent)}"
    ":root:has(#aurekai-identity-profile) .ph-sites h2{font:600 11px var(--si-sans);letter-spacing:.08em;text-transform:uppercase;color:var(--si-dim)}"
    ":root:has(#aurekai-identity-profile) .ph-sites a{background:var(--si-panel);border-color:var(--si-high);border-radius:16px}"
    ":root:has(#aurekai-identity-profile) .ph-sites a:hover{border-color:var(--si-line)}"
    ":root:has(#aurekai-identity-profile) .ph-sites a[aria-current=page]{background:var(--si-high);border-color:var(--si-accent)}"
    ":root:has(#aurekai-identity-profile) .ph-sites small{color:var(--si-dim)}"
    ":root:has(#aurekai-identity-profile) .ph-site-icon{background:var(--si-high);color:var(--si-accent)}"
    ":root:has(#aurekai-identity-profile) .ph-tabs{border-color:var(--si-high)}"
    ":root:has(#aurekai-identity-profile) .ph-tabs a{background:var(--si-high);color:var(--si-dim);font:600 13px var(--si-sans)}"
    ":root:has(#aurekai-identity-profile) .ph-tabs a[aria-current=page]{background:var(--si-accent);color:var(--si-on-accent)}"
    ":root:has(#aurekai-identity-profile) .ph-panel h2{font:600 clamp(24px,2.5vw,32px)/1.15 var(--si-sans);letter-spacing:-.025em;color:var(--si-ink)}"
    ":root:has(#aurekai-identity-profile) :is(.ph-panel>p,.ph-privacy){color:var(--si-dim)}"
    ":root:has(#aurekai-identity-profile) .ph-privacy{background:var(--si-panel);border:1px solid var(--si-high);border-radius:16px}"
    ":root:has(#aurekai-identity-profile) :is(.ph-shell input:not([type=hidden]),.ph-shell textarea,.ph-shell select){background:#0a100c;color:var(--si-ink);border-color:var(--si-line)}"
    ":root:has(#aurekai-identity-profile) :is(.ph-form,.ph-saved,.ph-review){background:var(--si-panel);border-color:var(--si-high);color:var(--si-ink)}"
    ":root:has(#aurekai-identity-profile) .ph-saved h3,:root:has(#aurekai-identity-profile) .ph-post h3{font:600 21px/1.25 var(--si-sans);letter-spacing:-.015em;color:var(--si-ink)}"
    ":root:has(#aurekai-identity-profile) :is(.ph-saved p,.ph-saved small,.ph-post time){color:var(--si-dim)}"
    ":root:has(#aurekai-identity-profile) .ph-post{border-color:var(--si-high)}"
    ":root:has(#aurekai-identity-profile) .ph-post p{font:400 16px/1.7 var(--si-serif);color:var(--si-ink)}"
    ":root:has(#aurekai-identity-profile) .ph-empty{background:var(--si-panel);border:1px dashed var(--si-line);border-radius:24px;color:var(--si-dim)}"
    ":root:has(#aurekai-identity-profile) .ph-next{border-color:var(--si-line);background:var(--si-high)}"
    ":root:has(#aurekai-identity-profile) .ph-shell :focus-visible{outline:2px solid var(--si-accent);outline-offset:3px}"
    "@media(max-width:600px){:root:has(#aurekai-identity-profile) .ph-cover{border-radius:24px 24px 0 0}"
    ":root:has(#aurekai-identity-profile) .ph-identity{border-radius:0 0 24px 24px}}";

/* Signed-in /organizations workspace (#9 actions, #10 people and invites). */
static const char bf_si_organizations_css[] =
    ":root:has(#aurekai-identity-organizations){" BF_SI_TOKENS "color-scheme:dark}"
    ":root:has(#aurekai-identity-organizations) body{background:var(--si-ground);color:var(--si-ink);font:15px/1.6 var(--si-sans)}"
    ":root:has(#aurekai-identity-organizations) main{max-width:1280px;padding-inline:clamp(16px,4vw,64px)}"
    ":root:has(#aurekai-identity-organizations) main>nav:first-child{gap:6px;font:600 13px var(--si-sans)}"
    ":root:has(#aurekai-identity-organizations) main>nav:first-child a{padding:9px 17px;border-radius:999px;background:var(--si-high);text-decoration:none}"
    ":root:has(#aurekai-identity-organizations) main>nav:first-child a:first-child{background:transparent;padding-left:0;font-weight:700;letter-spacing:.04em}"
    ":root:has(#aurekai-identity-organizations) h1{font:600 clamp(36px,4.2vw,56px)/1.08 var(--si-sans);letter-spacing:-.035em}"
    ":root:has(#aurekai-identity-organizations) h2{font:600 clamp(22px,2.3vw,28px)/1.2 var(--si-sans);letter-spacing:-.02em}"
    ":root:has(#aurekai-identity-organizations) p{color:var(--si-dim)}"
    ":root:has(#aurekai-identity-organizations) section,"
    ":root:has(#aurekai-identity-organizations) .org-grid:has([data-organization]) [data-organization]{"
    "background:var(--si-panel);color:var(--si-ink);border:1px solid var(--si-high);border-radius:24px;padding:clamp(18px,2.6vw,30px)}"
    ":root:has(#aurekai-identity-organizations) .org-grid{gap:14px}"
    ":root:has(#aurekai-identity-organizations) :is(input,select){background:#0a100c;color:var(--si-ink);border:1px solid var(--si-line);border-radius:12px}"
    ":root:has(#aurekai-identity-organizations) label{font:600 12px var(--si-sans);color:var(--si-dim)}"
    ":root:has(#aurekai-identity-organizations) button{background:var(--si-accent);color:var(--si-on-accent);border:0;border-radius:999px;padding:10px 20px;font:700 13px var(--si-sans)}"
    ":root:has(#aurekai-identity-organizations) .actions{gap:8px}"
    ":root:has(#aurekai-identity-organizations) .actions a{display:inline-flex;align-items:center;min-height:44px;padding:10px 18px;border-radius:999px;"
    "background:var(--si-high);border:1px solid var(--si-line);font:600 13px var(--si-sans);text-decoration:none}"
    ":root:has(#aurekai-identity-organizations) .actions a:first-child{background:var(--si-accent);color:var(--si-on-accent);border-color:var(--si-accent)}"
    ":root:has(#aurekai-identity-organizations) :is(th,td){border-color:var(--si-high)}"
    ":root:has(#aurekai-identity-organizations) th{font:600 11px var(--si-sans);letter-spacing:.08em;text-transform:uppercase;color:var(--si-dim)}"
    ":root:has(#aurekai-identity-organizations) :is(.result,.invite-link){border-color:var(--si-accent);background:var(--si-high);border-radius:16px}"
    ":root:has(#aurekai-identity-organizations) article{padding:14px 0;border-bottom:1px solid var(--si-high)}"
    ":root:has(#aurekai-identity-organizations) :focus-visible{outline:2px solid var(--si-accent);outline-offset:3px}";

static const char *bf_si_css(const char *view) {
    if (!view) return NULL;
    if (!strcmp(view, "organisations")) return bf_si_organisations_css;
    if (!strcmp(view, "profile")) return bf_si_profile_css;
    if (!strcmp(view, "organizations")) return bf_si_organizations_css;
    return NULL;
}

/* One style block per page. Its id is the scope every rule tests for. */
static void bf_stitch_identity_head(FILE *out, const char *view) {
    const char *css = bf_si_css(view);
    if (!css) return;
    fprintf(out, "<style id=\"aurekai-identity-%s\">", view);
    fputs(css, out);
    fputs("</style>", out);
}

/* The opening band of /organisations. Every link goes to an existing owner:
 * /organizations asks for sign-in and only creates a workspace after the
 * person submits its reviewed form; /organizations/join only accepts an
 * invitation for the account it was sent to. */
static void bf_stitch_identity_organisations_lead(FILE *out, const char *host) {
    if (!bf_stitch_identity_view(host, "/organisations", NULL)) return;
    fputs("<section class=\"si-org-lead\" aria-label=\"Your organisation\" data-identity-org-lead>"
          "<div><p class=\"si-eyebrow\">Organisations</p>"
          "<h2>Run your organisation here</h2>"
          "<p>Give a business, team or club its own shared space. You choose who joins and what each person can change, "
          "and its work stays apart from your own. Starting one does not claim an existing company's name.</p></div>"
          "<nav class=\"si-org-actions\" aria-label=\"Organisation actions\">"
          "<a href=\"/organizations\" data-identity-org-start>Start or manage an organisation</a>"
          "<a href=\"/organizations/join\">Open an invitation</a>"
          "<a href=\"/people\">Find people</a>"
          "</nav></section>", out);
}
#endif
