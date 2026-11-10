#include "public_home_search.h"

#include <stdlib.h>
#include <string.h>

static int insert_before(char **bodyp, const char *needle, const char *insert) {
    char *body, *at, *next;
    size_t before, insert_len, after;
    if (!bodyp || !(body = *bodyp) || !needle || !*needle || !insert) return 0;
    at = strstr(body, needle);
    if (!at) return 0;
    before = (size_t)(at - body); insert_len = strlen(insert); after = strlen(at);
    next = malloc(before + insert_len + after + 1);
    if (!next) return 0;
    memcpy(next, body, before);
    memcpy(next + before, insert, insert_len);
    memcpy(next + before + insert_len, at, after + 1);
    free(body);
    *bodyp = next;
    return 1;
}

int bf_apply_public_home_search(char **bodyp) {
    static const char css[] = "<style id=\"bf-public-search-css\">.estate-try{padding:72px 0;border-top:1px solid var(--line)}.estate-try-head{display:grid;grid-template-columns:1fr 1fr;gap:30px;align-items:end;margin-bottom:22px}.estate-try h2{font-size:clamp(2.3rem,5.6vw,5.1rem);line-height:.86;letter-spacing:-.07em;margin:10px 0 0}.estate-try h2 em{font-family:var(--serif);font-weight:400}.estate-try-copy{color:var(--muted);line-height:1.5;max-width:48ch;margin:0}.estate-search{display:flex;border:1px solid var(--line);background:var(--panel);padding:6px;gap:6px}.estate-search input[type=search]{min-width:0;flex:1;border:0;background:transparent;color:var(--ink);font:500 .92rem var(--mono);padding:.8rem}.estate-search button{border:0;background:var(--ink);color:var(--paper);font:500 .68rem var(--mono);letter-spacing:.08em;text-transform:uppercase;padding:.7rem 1rem;cursor:pointer}.estate-presets{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0 0}.estate-presets a{border:1px solid var(--line);border-radius:999px;padding:.45rem .68rem;font:500 .65rem var(--mono);color:var(--muted)}.estate-results{margin-top:18px}.estate-results:empty{display:none}.estate-results .bf-material-list{display:grid;grid-template-columns:repeat(3,1fr);border-top:1px solid var(--line);border-left:1px solid var(--line)}.estate-results .bf-material-card,.estate-results .bf-material-empty{background:var(--panel);padding:20px;border-right:1px solid var(--line);border-bottom:1px solid var(--line);min-width:0}.estate-results .bf-material-card h2{font-size:1.35rem;line-height:1.05;margin:18px 0 9px}.estate-results .bf-material-card p,.estate-results .bf-stream-boundary{font-size:.76rem;line-height:1.45;color:var(--muted)}.estate-results .bf-material-card footer{display:flex;flex-wrap:wrap;gap:10px;justify-content:space-between;border-top:1px solid var(--line);padding-top:13px;margin-top:16px;font:500 .64rem var(--mono)}@media(max-width:760px){.estate-try-head{grid-template-columns:1fr}.estate-search{display:block}.estate-search button{width:100%;margin-top:6px}.estate-results .bf-material-list{grid-template-columns:1fr}}</style>";
    static const char markup[] = "<section class=\"wrap estate-try\" id=\"estate-try\"><div class=\"estate-try-head\"><div><p class=\"eyebrow\"><i></i>02 / ask the outside</p><h2>Search the world Aurekai already <em>knows how to reach.</em></h2></div><p class=\"estate-try-copy\">Search maintained public observations, registries, APIs, feeds, tools, research and machine-capable sources. Results stay observations until a real owner admits evidence, authority, money or an effect.</p></div><form class=\"estate-search\" role=\"search\" action=\"/_surface/external-estate\" method=\"get\" hx-get=\"/_surface/external-estate\" hx-target=\"#estate-results\" hx-swap=\"innerHTML\"><input type=\"hidden\" name=\"grammar\" value=\"observations\"><input type=\"hidden\" name=\"limit\" value=\"9\"><input name=\"q\" type=\"search\" autocomplete=\"off\" placeholder=\"weather, grants, music venues, public records, research, x402…\" aria-label=\"Search public observations\" required><button type=\"submit\">Search Aurekai &rarr;</button></form><div class=\"estate-presets\" aria-label=\"Try a public search\"><a href=\"/_surface/external-estate?grammar=observations&amp;q=government&amp;limit=9\" hx-get=\"/_surface/external-estate?grammar=observations&amp;q=government&amp;limit=9\" hx-target=\"#estate-results\">public records</a><a href=\"/_surface/external-estate?grammar=observations&amp;q=science&amp;limit=9\" hx-get=\"/_surface/external-estate?grammar=observations&amp;q=science&amp;limit=9\" hx-target=\"#estate-results\">science</a><a href=\"/_surface/external-estate?grammar=observations&amp;q=agents&amp;limit=9\" hx-get=\"/_surface/external-estate?grammar=observations&amp;q=agents&amp;limit=9\" hx-target=\"#estate-results\">agents &amp; tools</a><a href=\"/_surface/external-estate?grammar=observations&amp;q=x402&amp;limit=9\" hx-get=\"/_surface/external-estate?grammar=observations&amp;q=x402&amp;limit=9\" hx-target=\"#estate-results\">machine commerce</a><a href=\"/_surface/external-estate?grammar=observations&amp;q=music&amp;limit=9\" hx-get=\"/_surface/external-estate?grammar=observations&amp;q=music&amp;limit=9\" hx-target=\"#estate-results\">music &amp; events</a></div><div class=\"estate-results\" id=\"estate-results\" aria-live=\"polite\"></div></section>";
    if (!bodyp || !*bodyp) return 0;
    if (strstr(*bodyp, "id=\"estate-try\"")) return 1;
    if (!insert_before(bodyp, "</head>", css)) return 0;
    return insert_before(bodyp, "<section class=\"wrap moves\">", markup);
}
