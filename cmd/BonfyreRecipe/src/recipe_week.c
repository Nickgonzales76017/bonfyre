#define _POSIX_C_SOURCE 200809L
#include "recipe_week.h"
#include "recipe_nutrition.h"
#include "address_resolve.h"
#include "record_factor.h"
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#define WEEK_AMOUNTS 2048
#define WEEK_BRIEF_MAX (512u*1024u)
void bf_sha256_hex(const uint8_t *, size_t, char *);
typedef struct {
    char id[241],name[192],unit[32],raw[1024];
    BfRecipeQuantity q;
    unsigned sources;
    int known,declared,copies;
} Demand;
typedef struct {
    const BfRecipeWeek *week;
    BfRecipeMaterialInfo materials[BF_RECIPE_WEEK_MAX];
    Demand *amounts;
    int count,unknown,index;
    char digest[65];
    BfRecipeNutrition nutrition;
} Combined;
typedef struct {char *p;size_t used,cap;int failed;} Output;
static void emit(Output *o,const char *fmt,...){
    if(o->failed)return;
    va_list a;va_start(a,fmt);int n=vsnprintf(o->p+o->used,o->cap-o->used,fmt,a);va_end(a);
    if(n<0||(size_t)n>=o->cap-o->used)o->failed=1;else o->used+=(size_t)n;
}
static void escape(Output *o,const char *s){
    for(;s&&*s;s++)switch(*s){
    case '&':emit(o,"&amp;");break;case '<':emit(o,"&lt;");break;case '>':emit(o,"&gt;");break;
    case '"':emit(o,"&quot;");break;case '\'':emit(o,"&#39;");break;default:emit(o,"%c",*s);
    }
}
static void encoded(Output *o,const char *s){
    for(const unsigned char *p=(const unsigned char*)s;p&&*p;p++){
        if((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||strchr("-._~",*p))emit(o,"%c",*p);
        else emit(o,"%%%02X",*p);
    }
}
static int valid_key(const char *s){
    if(!s||!*s||strlen(s)>240)return 0;
    for(const unsigned char *p=(const unsigned char*)s;*p;p++)
        if(!((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||strchr("-._:/@",*p)))return 0;
    return 1;
}
static int order(const void *a,const void *b){return strcmp(((const BfRecipeSelection*)a)->key,((const BfRecipeSelection*)b)->key);}
int bf_recipe_week_set(BfRecipeWeek *w,const char *key,int copies){
    if(!w||w->count<0||w->count>BF_RECIPE_WEEK_MAX||!valid_key(key)||copies<0||copies>100)return 1;
    for(int i=0;i<w->count;i++)if(!strcmp(w->items[i].key,key)){
        if(copies)w->items[i].copies=copies;
        else{memmove(&w->items[i],&w->items[i+1],(size_t)(w->count-i-1)*sizeof(w->items[0]));w->count--;}
        return 0;
    }
    if(!copies)return 0;
    if(w->count>=BF_RECIPE_WEEK_MAX)return 1;
    strcpy(w->items[w->count].key,key);w->items[w->count++].copies=copies;
    qsort(w->items,(size_t)w->count,sizeof(w->items[0]),order);return 0;
}
int bf_recipe_week_parse(const char *spec,BfRecipeWeek *out){
    BfRecipeWeek w={0};char text[BF_RECIPE_WEEK_SPEC];
    if(!out||!spec||strlen(spec)>=sizeof(text))return 1;
    strcpy(text,spec);char *p=text;
    while(*p){
        char *next=strchr(p,';');if(next)*next=0;
        char *star=strchr(p,'*');if(!star)return 1;*star++=0;
        if(!*star||strspn(star,"0123456789")!=strlen(star))return 1;
        char *end;errno=0;long n=strtol(star,&end,10);if(errno||*end||n<1||n>100)return 1;
        for(int i=0;i<w.count;i++)if(!strcmp(w.items[i].key,p))return 1;
        if(bf_recipe_week_set(&w,p,(int)n))return 1;
        if(!next)break;
        p=next+1;if(!*p)return 1;
    }
    *out=w;return 0;
}
int bf_recipe_week_spec(const BfRecipeWeek *w,char *out,size_t cap){
    if(!w||!out||!cap||w->count<0||w->count>BF_RECIPE_WEEK_MAX)return 1;
    Output o={out,0,cap,0};*out=0;
    for(int i=0;i<w->count;i++)emit(&o,"%s%s*%d",i?";":"",w->items[i].key,w->items[i].copies);
    return o.failed;
}
static int demand(void *context,const BfRecipeDemand *v){
    Combined *p=context;
    if(!v||!v->id||!v->name||!v->unit||!v->raw||strlen(v->id)>=241||strlen(v->name)>=192||strlen(v->unit)>=32||strlen(v->raw)>=1024)return 1;
    if(v->known)for(int i=0;i<p->count;i++){
        Demand *a=&p->amounts[i];
        if(a->known&&a->declared==v->identity_declared&&!strcmp(a->id,v->id)&&!strcmp(a->unit,v->unit)){
            if(strcmp(a->name,v->name)||bf_recipe_quantity_sum(a->q,v->quantity,&a->q))return 1;
            a->sources|=1u<<p->index;return 0;
        }
    }
    if(p->count==WEEK_AMOUNTS)return 1;
    Demand *a=&p->amounts[p->count++];memset(a,0,sizeof(*a));
    strcpy(a->id,v->id);strcpy(a->name,v->name);strcpy(a->unit,v->unit);strcpy(a->raw,v->raw);
    a->q=v->quantity;a->known=v->known;a->declared=v->identity_declared;
    a->sources=1u<<p->index;a->copies=p->week->items[p->index].copies;
    if(!v->known)p->unknown++;
    return 0;
}
static int compile(const char *path,const BfRecipeWeek *week,Combined *p){
    sqlite3 *db=NULL;char identity[8192];Output id={identity,0,sizeof(identity),0};int rc=1;
    memset(p,0,sizeof(*p));p->week=week;p->amounts=calloc(WEEK_AMOUNTS,sizeof(Demand));
    if(!p->amounts||!week||week->count<1||week->count>BF_RECIPE_WEEK_MAX||bf_record_factor_open(path,&db))goto done;
    if(sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    emit(&id,"aurekai.preparation-selection.v1\n");
    for(int i=0;i<week->count;i++){
        p->index=i;
        if(bf_recipe_material_demands(db,week->items[i].key,week->items[i].copies,demand,p,&p->materials[i]))goto done;
        emit(&id,"%zu:%s:%d:%s\n",strlen(week->items[i].key),week->items[i].key,week->items[i].copies,p->materials[i].digest);
    }
    const char *expected[BF_RECIPE_WEEK_MAX];for(int i=0;i<week->count;i++)expected[i]=p->materials[i].digest;
    int nr=bf_recipe_nutrition_bundle(path,NULL,week,expected,&p->nutrition);
    if(nr==1)goto done;
    emit(&id,"nutrition:%s\n",nr==2?"unbound":p->nutrition.digest);
    if(id.failed)goto done;
    bf_sha256_hex((const uint8_t*)identity,id.used,p->digest);rc=0;
 done:if(db)sqlite3_close(db);if(rc){free(p->amounts);p->amounts=NULL;bf_recipe_nutrition_free(&p->nutrition);}return rc;
}
static void number(Output *o,BfRecipeQuantity q){if(q.d==1)emit(o,"%lld",(long long)q.n);else emit(o,"%lld/%lld",(long long)q.n,(long long)q.d);}
static void tsv(FILE *out,const char *s){
    if(*s&&strchr("=+-@",*s))fputc('\'',out);
    for(;*s;s++){if(*s=='\t')fputs("\\t",out);else if(*s=='\n')fputs("\\n",out);else if(*s=='\r')fputs("\\r",out);else if(*s=='\\')fputs("\\\\",out);else fputc((unsigned char)*s,out);}
}
static int render_export(const char *path,const Combined *p,int brief,FILE *out){
    if(!brief){
        fputs("ingredient_id\tingredient\tnumerator\tdenominator\tunit\tquantity_status\tidentity_basis\tsource_keys\tprice_status\toriginal_quantity\n",out);
        for(int i=0;i<p->count;i++){
            const Demand *a=&p->amounts[i];tsv(out,a->id);fputc('\t',out);tsv(out,a->name);fputc('\t',out);
            if(a->known)fprintf(out,"%lld\t%lld",(long long)a->q.n,(long long)a->q.d);else fputc('\t',out);
            fputc('\t',out);tsv(out,a->unit);fprintf(out,"\t%s\t%s\t",a->known?"exact_as_declared":"review_required",a->declared?"declared_id":"exact_source_label");
            int first=1;for(int j=0;j<p->week->count;j++)if(a->sources&(1u<<j)){if(!first)fputc(';',out);tsv(out,p->week->items[j].key);first=0;}
            fputs("\tnot_quoted\t",out);if(!a->known){fprintf(out,"%d source copies: ",a->copies);tsv(out,a->raw);}fputc('\n',out);
        }
    }else{
        fprintf(out,"HOUSEHOLD PREPARATION PLAN\nPlan identity: %s\nSelections: %d\nUnresolved ingredient rows: %d\n\nCOMBINED SHOPPING DEMAND\n",p->digest,p->week->count,p->unknown);
        char selection[BF_RECIPE_WEEK_SPEC],encoded_selection[BF_RECIPE_WEEK_SPEC*3];
        if(bf_recipe_week_spec(p->week,selection,sizeof(selection)))return 1;
        Output link={encoded_selection,0,sizeof(encoded_selection),0};encoded(&link,selection);if(link.failed)return 1;
        fprintf(out,"Selection programme: %s\n[Review these selections against current sources](/recipes/plan?items=%s)\nThe saved brief remains a snapshot; reopening may use newer sources.\n\n",selection,encoded_selection);
        for(int i=0;i<p->count;i++){
            const Demand *a=&p->amounts[i];
            if(a->known)fprintf(out,"- %s: %lld/%lld %s [%s]\n",a->name,(long long)a->q.n,(long long)a->q.d,a->unit,a->declared?"declared ingredient identity":"same source label only");
            else fprintf(out,"- REVIEW %s: %d source copies of [%s]; no numeric total asserted\n",a->name,a->copies,a->raw);
        }
        fputs("\nShopping quantities may add where identities and units agree. Cooking batches remain separate; no cross-recipe substitution, oven schedule, safe storage duration, price or booking is inferred.\n",out);
        for(int i=0;i<p->week->count;i++){
            fprintf(out,"\n===== SELECTION %d: %s =====\nRetained source SHA-256: %s\n",i+1,p->week->items[i].key,p->materials[i].digest);
            if(bf_recipe_material_brief_exact(path,p->week->items[i].key,p->week->items[i].copies,p->materials[i].digest,out))return 1;
        }
        if(p->nutrition.state==0)fputs(p->nutrition.text,out);
        else fputs("\nNutrition has no bound food records here; no nutrient total inferred.\n",out);
    }
    return ferror(out)?1:0;
}
/* Assemble completely before delivering; a failed input cannot produce a
 * successful-looking partial shopping list. The UI and CLI call this function. */
int bf_recipe_week_export(const char *path,const BfRecipeWeek *w,int brief,FILE *out){
    Combined p;char *text=NULL;size_t n=0;int rc=1;
    if(!out||compile(path,w,&p))return 1;
    FILE *s=open_memstream(&text,&n);if(!s)goto done;
    int bad=render_export(path,&p,brief,s);if(fclose(s))bad=1;
    if(!bad&&n<=WEEK_BRIEF_MAX&&fwrite(text,1,n,out)==n&&!fflush(out))rc=0;
 done:free(text);free(p.amounts);bf_recipe_nutrition_free(&p.nutrition);return rc;
}
static void link_start(Output *o,const char *spec){emit(o,"/recipes/plan?items=");encoded(o,spec);}
typedef struct {Output *o;const char *spec;int shown;} Choices;
static int choice(void *context,sqlite3_int64 seq,const char *key,const char *title,const char *kind){
    Choices *c=context;(void)seq;(void)kind;if(c->shown++>=12)return 0;
    if(!valid_key(key))return 0;
    emit(c->o,"<div class=\"desk-row\"><span class=\"desk-label\">");escape(c->o,title);
    emit(c->o,"</span><a class=\"button quiet\" href=\"");link_start(c->o,c->spec);emit(c->o,"&amp;add=");encoded(c->o,key);emit(c->o,"\">Add to plan</a></div>");return c->o->failed;
}
/* The page asks AddressPlane, not an estate file/schema. The same resolver is
 * used by programme binding and the normal owner command. */
static int food_source_row(void *context,const BfAddressCandidate *c){
    Output *out=context;const char *url=c->url;
    if(!url||strpbrk(url,"\r\n\t @\\"))return 0;
    const char *host=!strncmp(url,"https://",8)?url+8:!strncmp(url,"http://",7)?url+7:NULL;
    if(!host||!*host||*host=='/'||*host=='?'||*host=='#')return 0;
    emit(out,"<p><a rel=\"noreferrer\" href=\"");escape(out,url);emit(out,"\">");escape(out,c->name);
    emit(out,"</a> <small>");escape(out,c->identity);emit(out,"</small></p>");return out->failed;
}
static void food_reference_links(Output *out){
    BfAddressQuery q={.kind="external_occurrence",.need={"domain:food","source_kind:public_api"},.need_count=2,.limit=64};
    BfAddressResolution result;
    emit(out,"<details class=\"desk-q\" data-food-reference-owner=\"AddressPlane\"><summary>Food data already in this estate</summary><p>Existing owner references selected by the shared address plane; not matched ingredient facts or a completed recall check.</p>");
    if(bf_address_resolve_path(NULL,&q,food_source_row,out,&result,NULL))
        emit(out,"<p>Source resolution is unavailable here. This does not mean the estate has no food data.</p>");
    else emit(out,"<p class=\"subj-note\">%d of %lld eligible owner references. Exact food/product and quantity-basis verification remains separate.</p>",result.returned,(long long)result.eligible);
    emit(out,"</details>");
}
int bf_recipe_week_view(const char *path,const BfRecipeWeek *w,const char *search,int offset,int can_save,char *out,size_t cap){
    Combined p={0};char spec[BF_RECIPE_WEEK_SPEC];Output o={out,0,cap,0};int rc=1;sqlite3 *db=NULL;char *brief=NULL;size_t brief_n=0;
    if(!out||!cap||!w||offset<0||offset>1000000||bf_recipe_week_spec(w,spec,sizeof(spec)))return 1;
    *out=0;if(w->count&&compile(path,w,&p))goto done;
    emit(&o,"<article class=\"subj\"><p class=\"eyebrow\"><i></i>Home &middot; one plan, distinct preparations</p><h2 class=\"subj-name\">Plan the week together.</h2><p class=\"subj-desc\">Choose recipes and preparation families, combine the shopping demand, and keep each preparation tied to its source.</p>");
    if(w->count){
        emit(&o,"<section class=\"desk-q\"><h3>Selected preparations</h3>");
        for(int i=0;i<w->count;i++){
            emit(&o,"<form action=\"/recipes/plan\" method=\"get\" class=\"mk-commit\"><input type=\"hidden\" name=\"items\" value=\"");escape(&o,spec);
            emit(&o,"\"><input type=\"hidden\" name=\"edit\" value=\"");escape(&o,w->items[i].key);emit(&o,"\"><label>");escape(&o,p.materials[i].title);
            emit(&o," <input name=\"copies\" type=\"number\" min=\"0\" max=\"100\" value=\"%d\"></label><button class=\"button quiet\">Update quantity</button><p class=\"subj-note\">0 removes this selection; quantities count %s, not promised meals.</p></form>",w->items[i].copies,p.materials[i].is_family?"complete variant sets":"copies of the source recipe");
        }
        emit(&o,"</section><h3>One shopping list</h3><table><thead><tr><th>Ingredient</th><th>Total</th><th>Basis</th></tr></thead><tbody>");
        for(int i=0;i<p.count;i++){
            Demand *a=&p.amounts[i];emit(&o,"<tr><td>");escape(&o,a->name);emit(&o,"</td><td>");
            if(a->known){number(&o,a->q);emit(&o," ");escape(&o,a->unit);}else{emit(&o,"Review %d source copies: ",a->copies);escape(&o,a->raw);}
            emit(&o,"</td><td>%s</td></tr>",a->declared?"Declared ingredient identity":"Exact source label; not a substitution");
        }
        emit(&o,"</tbody></table><p class=\"subj-note\">%d unresolved ingredient rows. Units are not silently converted; cooking batches are not merged across sources. Prices, store availability, equipment schedules and safe storage windows remain unquoted.</p>",p.unknown);
        emit(&o,"<a class=\"button quiet\" href=\"");link_start(&o,spec);emit(&o,"&amp;format=list\">Combined grocery list</a><a class=\"button primary\" href=\"");link_start(&o,spec);emit(&o,"&amp;format=brief\">Complete preparation brief</a>");
        if(p.nutrition.state==0)emit(&o,"%s",p.nutrition.html);
        else emit(&o,"<p class=\"subj-note\">Nutrition has no bound food records here; no nutrient total inferred.</p>");
        if(can_save){
            FILE *f=open_memstream(&brief,&brief_n);if(!f)goto done;
            int bad=render_export(path,&p,1,f);if(fclose(f))bad=1;if(bad)goto done;
            if(brief_n<=48000){
                emit(&o,"<div id=\"cms-pane\"><form class=\"mk-commit\" method=\"post\" action=\"/cms/v1/api/wiki_page\" hx-post=\"/cms/v1/api/wiki_page\" hx-target=\"#week-save-result\" hx-swap=\"innerHTML\"><h3>Keep this in your workspace</h3><label>Plan name<input name=\"title\" required maxlength=\"240\" value=\"Household preparation plan\"></label><input type=\"hidden\" name=\"route\" value=\"notes/preparation-%.24s\"><input type=\"hidden\" name=\"status\" value=\"draft\"><textarea name=\"content\" hidden>",p.digest);
                escape(&o,brief);emit(&o,"</textarea><button class=\"button primary\" type=\"submit\">Save private working plan</button><p class=\"subj-note\">The existing CMS keeps this snapshot in your namespace and the Desk lists it with your other drafts. This does not book a helper, publish a menu or place an order.</p></form><div id=\"week-save-result\"></div></div>");
            }else emit(&o,"<p class=\"subj-note\">This complete brief exceeds the inline draft form. Export the full brief; nothing is truncated.</p>");
        }else emit(&o,"<p class=\"subj-note\">Sign in with write scope to save a private working plan. Browsing and export do not create a record.</p>");
    }else emit(&o,"<p class=\"flow-empty\">Add a recipe or preparation family below to begin.</p>");
    food_reference_links(&o);
    emit(&o,"<section class=\"desk-q\"><h3>Add something to the plan</h3><form action=\"/recipes/plan\" method=\"get\"><input type=\"hidden\" name=\"items\" value=\"");escape(&o,spec);emit(&o,"\"><label>Find recipes<input name=\"q\" value=\"");escape(&o,search?search:"");emit(&o,"\"></label><button class=\"button quiet\">Find</button></form>");
    if(bf_record_factor_open(path,&db))goto done;
    Choices choices={&o,spec,0};int n=bf_record_factor_visit(db,search,13,offset,choice,&choices);if(n<0)goto done;
    if(n==13){emit(&o,"<a class=\"button quiet\" href=\"");link_start(&o,spec);emit(&o,"&amp;q=");encoded(&o,search?search:"");emit(&o,"&amp;offset=%d\">More choices</a>",offset+12);}
    emit(&o,"</section><p><a href=\"/recipes\">Full recipe collection</a> &middot; <a href=\"/flow\">Mixed feed</a> &middot; <a href=\"/studio/\">Your Desk</a></p></article><script defer src=\"/assets/matrix-htmx.js\"></script>");
    rc=o.failed?1:0;
 done:free(p.amounts);bf_recipe_nutrition_free(&p.nutrition);free(brief);if(db)sqlite3_close(db);if(rc&&cap)snprintf(out,cap,"<p class=\"flow-empty\">The complete plan could not be verified. No partial list or job was created.</p>");return rc;
}
int bf_recipe_week_command(int argc,char **argv){
    enum{RW_CORPUS};static const BfCommandEntry week_commands[]={{"corpus",RW_CORPUS,0,NULL},};
    const BfCommandEntry*command=argc<2?NULL:bf_command_lookup(week_commands,sizeof(week_commands)/sizeof(week_commands[0]),argv[1]);
    if(argc<3||!command||command->id!=RW_CORPUS||strcmp(argv[2],"week"))return -1;
    if(argc!=6 || (strcmp(argv[5],"list") && strcmp(argv[5],"brief"))){fputs("recipe corpus week PACK 'KEY*COPIES;KEY*COPIES' list|brief\n",stderr);return 2;}
    BfRecipeWeek w;if(bf_recipe_week_parse(argv[4],&w))return 2;
    return bf_recipe_week_export(argv[3],&w,!strcmp(argv[5],"brief"),stdout);
}
