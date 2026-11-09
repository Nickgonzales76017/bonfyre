#ifndef BONFYRE_FRONT_BRAND_H
#define BONFYRE_FRONT_BRAND_H
/* Public brand identity on the four institution-owned apex fronts only.
 * The editorial stream and its retained source ownership stay untouched.
 * These are real navigational destinations, not a client endorsement,
 * a fake CTA, or an effect aperture. No JavaScript and no customer-site CSS.
 */
typedef struct {
    const char *territory, *index, *category, *headline, *description;
    const char *action, *label;
} FrontBrandCut;
#include "../generated/native_brand_cuts.h"
static void front_brand_emit(const char *territory){
    if(!territory)return;
    const FrontBrandCut *selected=NULL;
    for(size_t i=0;i<sizeof(front_brand_cuts)/sizeof(front_brand_cuts[0]);i++)
        if(!strcmp(front_brand_cuts[i].territory,territory)){selected=&front_brand_cuts[i];break;}
    if(!selected)return;
    fputs("<section class=\"af-brand\" data-brand-masthead data-brand-territory=\"",stdout);
    html(selected->territory);
    fputs("\" aria-labelledby=\"af-brand-title\"><div class=\"af-brand-top\">"
          "<div class=\"af-brand-mark\">AUREKAI<span>",stdout);
    html(selected->index);
    fputs(" &middot; THE OPERATING BUSINESS</span></div><span class=\"af-brand-signature\">",stdout);
    html(front_brand_signature_first);
    fputs("<br>",stdout);
    html(front_brand_signature_second);
    fputs("</span></div><div class=\"af-brand-body\"><div><p class=\"af-brand-category\">",stdout);
    html(selected->category);
    fputs("</p><h2 class=\"af-brand-title\" id=\"af-brand-title\">",stdout);
    html(selected->headline);
    fputs("</h2></div><div class=\"af-brand-side\"><p>",stdout);
    html(selected->description);
    fputs("</p><div class=\"af-brand-links\"><a href=\"",stdout);
    html(selected->action);
    fputs("\">",stdout);
    html(selected->label);
    fputs(" &rarr;</a><a href=\"https://nickgonzales76017.github.io/bonfyre/about/\">"
          "About Aurekai</a></div></div></div>"
          "<nav class=\"af-brand-nav\" aria-label=\"Aurekai destinations\">",stdout);
    static const char *const domains[]={"work","life","live","shop"};
    static const char *const names[]={"Work","Life","Live","Shop"};
    for(size_t i=0;i<4;i++){
        fputs("<a href=\"https://aurekai.",stdout);
        html(domains[i]);fputs("/\"",stdout);
        if(!strcmp(domains[i],territory))fputs(" aria-current=\"page\"",stdout);
        fputs("><strong>",stdout);html(names[i]);fputs("</strong><span>",stdout);
        printf("%02zu",i+1);fputs("</span></a>",stdout);
    }
    fputs("</nav></section><style>",stdout);
    fputs(".af-brand{--af-ink:#152d22;--af-paper:#e5ead9;--af-rule:#a8bb9f;--af-accent:#8f431e;isolation:isolate;position:relative;overflow:hidden;margin:0 0 clamp(26px,5vw,64px);padding:clamp(22px,3.5vw,46px);background:var(--af-paper);color:var(--af-ink);border:1px solid var(--af-rule);font-family:'DM Sans',system-ui,sans-serif}\n.af-brand *{box-sizing:border-box}.af-brand[data-brand-territory=work]{--af-paper:#e3ead9;--af-ink:#132d20;--af-rule:#b5c5a7;--af-accent:#a14523}\n.af-brand[data-brand-territory=life]{--af-paper:#f4e9d8;--af-ink:#4c2d26;--af-rule:#dac5aa;--af-accent:#a64e38}\n.af-brand[data-brand-territory=live]{--af-paper:#242033;--af-ink:#f2eaf1;--af-rule:#67576a;--af-accent:#f1ad8b}\n.af-brand[data-brand-territory=shop]{--af-paper:#eae5d4;--af-ink:#27291f;--af-rule:#bfbba5;--af-accent:#8c4c2e}\n.af-brand-top{display:flex;justify-content:space-between;align-items:center;gap:16px;border-bottom:1px solid var(--af-rule);padding:0 0 20px}\n.af-brand-mark{font-weight:900;letter-spacing:.14em;font-size:clamp(14px,1.5vw,19px)}.af-brand-mark span{display:block;font:550 10px/1.5 ui-monospace,monospace;letter-spacing:.13em;text-transform:uppercase;opacity:.75}\n",stdout);
    fputs(".af-brand-signature{max-width:26ch;text-align:right;font:650 10px/1.45 ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;color:var(--af-accent)}\n.af-brand-body{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(245px,.56fr);gap:clamp(26px,4vw,75px);padding:clamp(32px,5vw,74px) 0 clamp(26px,4vw,50px);align-items:end}\n.af-brand-category{margin:0 0 16px;font:700 11px/1.4 ui-monospace,monospace;letter-spacing:.15em;text-transform:uppercase;color:var(--af-accent)}\n.af-brand-title{font:400 clamp(42px,6.6vw,105px)/.94 'Instrument Serif',Georgia,serif;letter-spacing:-.043em;max-width:13ch;margin:0;text-wrap:balance}\n.af-brand[data-brand-territory=life] .af-brand-title{font-style:italic}.af-brand[data-brand-territory=live] .af-brand-title{max-width:12ch;letter-spacing:-.055em}.af-brand[data-brand-territory=shop] .af-brand-title{font-style:normal;max-width:12ch}\n.af-brand-side{border-top:3px solid var(--af-accent);padding-top:18px;max-width:44ch}.af-brand-side p{margin:0;font:430 clamp(15px,1.35vw,19px)/1.58 'DM Sans',system-ui,sans-serif;opacity:.92}\n",stdout);
    fputs(".af-brand-links{display:flex;flex-wrap:wrap;gap:9px;margin-top:26px}.af-brand-links a{display:inline-flex;align-items:center;justify-content:center;min-height:44px;padding:11px 16px;border:1px solid var(--af-ink);text-decoration:none;font:700 13px/1.2 system-ui,sans-serif;color:var(--af-ink);background:transparent}\n.af-brand-links a:first-child{background:var(--af-ink);color:var(--af-paper)}.af-brand-links a:hover{box-shadow:inset 0 0 0 2px var(--af-accent)}\n.af-brand-nav{border-top:1px solid var(--af-rule);display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0}\n.af-brand-nav a{display:flex;justify-content:space-between;align-items:center;gap:8px;min-height:60px;padding:15px 16px;color:inherit;text-decoration:none;border-right:1px solid var(--af-rule);font:700 12px/1.2 system-ui,sans-serif;letter-spacing:.05em}\n.af-brand-nav a:first-child{padding-left:0}.af-brand-nav a:last-child{padding-right:0;border-right:0}.af-brand-nav a span{font:500 10px/1 ui-monospace,monospace;opacity:.6}\n.af-brand-nav a[aria-current=page]{color:var(--af-accent)}.af-brand-nav a[aria-current=page]::after{content:'●';font-size:10px}.af-brand-nav a:hover{text-decoration:underline;text-underline-offset:5px}\n.af-brand :focus-visible{outline:3px solid var(--af-accent);outline-offset:3px}\n",stdout);
    fputs("@media(max-width:900px){.af-brand-body{grid-template-columns:1fr}.af-brand-title{max-width:19ch}.af-brand-side{max-width:65ch}.af-brand-nav a{padding-inline:10px}}\n@media(max-width:600px){.af-brand{padding:22px 18px}.af-brand-top{align-items:flex-start}.af-brand-signature{font-size:9px;max-width:17ch}.af-brand-body{padding:35px 0 28px;gap:28px}.af-brand-title{font-size:clamp(40px,10vw,69px)}.af-brand-nav{grid-template-columns:repeat(2,minmax(0,1fr))}.af-brand-nav a{border-bottom:1px solid var(--af-rule);padding:13px 10px}.af-brand-nav a:nth-child(2){border-right:0}.af-brand-nav a:first-child{padding-left:10px}.af-brand-nav a:last-child{padding-right:10px;border-bottom:0}.af-brand-nav a:nth-child(3){border-bottom:0}.af-brand-links a{flex:1 1 auto}}\n@media(prefers-reduced-motion:reduce){.af-brand *{scroll-behavior:auto}}",stdout);
    fputs("</style>",stdout);
}
#endif
