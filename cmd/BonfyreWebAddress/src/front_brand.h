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
/* Only the exact Aurekai apex root may inherit this public brand identity.
 * Neither an independently branded site nor a nested route is affected. */
static const FrontBrandCut *front_brand_public_host(const char *host,const char *route){
    if(!host||!route||strcmp(route,"/"))return NULL;
    for(size_t i=0;i<sizeof(front_brand_cuts)/sizeof(front_brand_cuts[0]);i++){
        char expected[64];
        int n=snprintf(expected,sizeof(expected),"aurekai.%s",front_brand_cuts[i].territory);
        if(n>0&&(size_t)n<sizeof(expected)&&!strcmp(host,expected))return &front_brand_cuts[i];
    }
    return NULL;
}
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
    (ui_lit(stdout, " &middot; THE OPERATING BUSINESS"), ui_close(stdout, "span"), ui_close(stdout, "div"), ui_open(stdout, "span", "af-brand-signature"));
    html(front_brand_signature_first);
    ui_open(stdout, "br", NULL);
    html(front_brand_signature_second);
    (ui_close(stdout, "span"), ui_close(stdout, "div"), ui_open(stdout, "div", "af-brand-body"), ui_open(stdout, "div", NULL), ui_open(stdout, "p", "af-brand-category"));
    html(selected->category);
    (ui_close(stdout, "p"), ui_elem(stdout, "h1", (const UiAttr[]){{"class", "af-brand-title"}, {"id", "af-brand-title"}}, 2, 0));
    html(selected->headline);
    (ui_close(stdout, "h1"), ui_close(stdout, "div"), ui_open(stdout, "div", "af-brand-side"), ui_open(stdout, "p", NULL));
    html(selected->description);
    fputs("</p><div class=\"af-brand-links\"><a href=\"",stdout);
    html(selected->action);
    ui_lit(stdout, "\">");
    html(selected->label);
    fputs(" &rarr;</a><a href=\"https://nickgonzales76017.github.io/bonfyre/about/\">"
          "About Aurekai</a></div></div></div>"
          "<nav class=\"af-brand-nav\" aria-label=\"Aurekai destinations\">",stdout);
    static const char *const domains[]={"work","life","live","shop"};
    static const char *const names[]={"Work","Life","Live","Shop"};
    for(size_t i=0;i<4;i++){
        fputs("<a href=\"https://aurekai.",stdout);
        html(domains[i]);ui_lit(stdout, "/\"");
        if(!strcmp(domains[i],territory))ui_lit(stdout, " aria-current=\"page\"");
        (ui_lit(stdout, ">"), ui_open(stdout, "strong", NULL));html(names[i]);(ui_close(stdout, "strong"), ui_open(stdout, "span", NULL));
        printf("%02zu",i+1);ui_close_n(stdout, "span", "a", NULL);
    }
    ui_close_n(stdout, "nav", "section", NULL);

}
#endif
