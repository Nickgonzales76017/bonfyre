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
    fputs(" &middot; THE OPERATING BUSINESS</span></div><span class=\"af-brand-signature\">",stdout);
    html(front_brand_signature_first);
    fputs("<br>",stdout);
    html(front_brand_signature_second);
    fputs("</span></div><div class=\"af-brand-body\"><div><p class=\"af-brand-category\">",stdout);
    html(selected->category);
    fputs("</p><h1 class=\"af-brand-title\" id=\"af-brand-title\">",stdout);
    html(selected->headline);
    fputs("</h1></div><div class=\"af-brand-side\"><p>",stdout);
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
    fputs("</nav></section>",stdout);

}
#endif
