#define _POSIX_C_SOURCE 200809L
#include "recipe_material.h"
#include "record_factor.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <ctype.h>
#define PLAN_MAX 256
/* Quantities are rational and dimensioned. No guessed drop/mass conversion. */
typedef BfRecipeQuantity Qty;
void bf_sha256_hex(const uint8_t *,size_t,char *);
static int source_recipe_brief(sqlite3 *,const char *,const char *,int,FILE *);
typedef struct {char id[128],name[192],unit[32];Qty q;} Amount;
typedef struct {Amount items[PLAN_MAX];int count,variants,copies,batches;Qty produced,required,remainder;char unit[32];} Plan;
typedef struct {char *p;size_t used,cap;int failed;} Html;
static int stmt(sqlite3 *d,sqlite3_stmt **s,const char *q){return sqlite3_prepare_v2(d,q,-1,s,NULL)!=SQLITE_OK;}
static void add(Html *h,const char *f,...){if(h->failed)return;va_list a;va_start(a,f);int n=vsnprintf(h->p+h->used,h->cap-h->used,f,a);va_end(a);if(n<0||(size_t)n>=h->cap-h->used)h->failed=1;else h->used+=(size_t)n;}
static void esc(Html *h,const char *s){if(!s)return;for(;*s;s++){switch(*s){case '&':add(h,"&amp;");break;case '<':add(h,"&lt;");break;case '>':add(h,"&gt;");break;case '"':add(h,"&quot;");break;case '\'':add(h,"&#39;");break;default:add(h,"%c",*s);}}}
static void url(Html *h,const char *s){for(const unsigned char *p=(const unsigned char*)s;*p;p++){if(isalnum(*p)||strchr("-_.~",*p))add(h,"%c",*p);else add(h,"%%%02X",*p);}}
static int copycol(sqlite3_stmt *s,int i,char *p,size_t cap){const char *t=(const char*)sqlite3_column_text(s,i);if(!t||sqlite3_column_type(s,i)!=SQLITE_TEXT||strlen(t)>=cap||strpbrk(t,"\r\n\t"))return 1;memcpy(p,t,strlen(t)+1);return 0;}
static int64_t gcd(int64_t a,int64_t b){while(b){int64_t t=a%b;a=b;b=t;}return a;}
static int makeqty(int64_t n,int64_t d,Qty *q){if(n<0||d<=0||n>1000000000||d>1000000)return 1;int64_t g=gcd(n,d);q->n=n/g;q->d=d/g;return 0;}
static int sumqty(Qty a,Qty b,Qty *q){int64_t g=gcd(a.d,b.d);__int128 d=(__int128)(a.d/g)*b.d,n=(__int128)a.n*(b.d/g)+(__int128)b.n*(a.d/g);if(n>INT64_MAX||d>INT64_MAX)return 1;g=gcd((int64_t)n,(int64_t)d);q->n=(int64_t)n/g;q->d=(int64_t)d/g;return 0;}
static int scaleqty(Qty a,int n,Qty *q){if(n<0||a.n>INT64_MAX/(n?n:1))return 1;q->n=a.n*n;q->d=a.d;int64_t g=gcd(q->n,q->d);q->n/=g;q->d/=g;return 0;}
static void qty(Html *h,Qty q){if(q.d==1)add(h,"%lld",(long long)q.n);else add(h,"%lld/%lld",(long long)q.n,(long long)q.d);}
static int amount(Plan *p,const Amount *a,int times){Qty q;if(scaleqty(a->q,times,&q))return 1;for(int i=0;i<p->count;i++)if(!strcmp(p->items[i].id,a->id)&&!strcmp(p->items[i].unit,a->unit)){if(strcmp(p->items[i].name,a->name))return 1;return sumqty(p->items[i].q,q,&p->items[i].q);}if(p->count==PLAN_MAX)return 1;p->items[p->count]=*a;p->items[p->count++].q=q;return 0;}
static int gather(sqlite3 *d,const char *body,const char *path,int times,Plan *p){sqlite3_stmt *s=NULL;int rc=1,step;
    if(stmt(d,&s,"SELECT json_type(?1,?2)='array' AND NOT EXISTS(SELECT 1 FROM json_each(?1,?2) WHERE json_type(value,'$.num') IS NOT 'integer' OR json_type(value,'$.den') IS NOT 'integer')"))return 1;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,path,-1,SQLITE_STATIC);
    int is_array=sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,0)==1;sqlite3_finalize(s);s=NULL;if(!is_array)return 1;
    if(stmt(d,&s,"SELECT json_extract(value,'$.id'),json_extract(value,'$.name'),json_extract(value,'$.unit'),json_extract(value,'$.num'),json_extract(value,'$.den') FROM json_each(?1,?2)"))return 1;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,path,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){Amount a={0};if(copycol(s,0,a.id,sizeof(a.id))||copycol(s,1,a.name,sizeof(a.name))||copycol(s,2,a.unit,sizeof(a.unit))||!*a.id||!*a.name||!*a.unit||sqlite3_column_type(s,3)!=SQLITE_INTEGER||sqlite3_column_type(s,4)!=SQLITE_INTEGER||makeqty(sqlite3_column_int64(s,3),sqlite3_column_int64(s,4),&a.q)||amount(p,&a,times))goto done;}
    if(step==SQLITE_DONE)rc=0;
done:sqlite3_finalize(s);return rc;
}
static int plan(sqlite3 *d,const char *body,int copies,Plan *p){sqlite3_stmt *s=NULL;int rc=1,step;Qty yield,portion;memset(p,0,sizeof(*p));
    if(copies<1||copies>100)return 1;
    if(stmt(d,&s,"SELECT json_type(?1,'$.preparation.shared_batch.yield.num')='integer' AND json_type(?1,'$.preparation.shared_batch.yield.den')='integer' AND json_type(?1,'$.preparation.shared_batch.portion.num')='integer' AND json_type(?1,'$.preparation.shared_batch.portion.den')='integer'"))return 1;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);int kinds=sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,0)==1;sqlite3_finalize(s);s=NULL;if(!kinds)return 1;
    if(stmt(d,&s,"SELECT json_type(?1,'$.preparation.variants')='array' AND json_array_length(?1,'$.preparation.shared_batch.inputs')>0 AND NOT EXISTS(SELECT 1 FROM json_each(?1,'$.preparation.variants') WHERE type<>'object' OR coalesce(json_type(value,'$.id'),'missing')<>'text' OR coalesce(json_type(value,'$.name'),'missing')<>'text' OR coalesce(json_type(value,'$.instructions'),'missing')<>'text' OR length(json_extract(value,'$.id'))=0) AND (SELECT count(DISTINCT json_extract(value,'$.id')) FROM json_each(?1,'$.preparation.variants'))=json_array_length(?1,'$.preparation.variants')"))return 1;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);int valid=sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,0)==1;sqlite3_finalize(s);s=NULL;if(!valid)return 1;
    if(stmt(d,&s,"SELECT json_array_length(?1,'$.preparation.variants'),json_extract(?1,'$.preparation.shared_batch.yield.num'),json_extract(?1,'$.preparation.shared_batch.yield.den'),json_extract(?1,'$.preparation.shared_batch.portion.num'),json_extract(?1,'$.preparation.shared_batch.portion.den'),json_extract(?1,'$.preparation.shared_batch.yield.unit'),json_extract(?1,'$.preparation.shared_batch.portion.unit')"))return 1;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    for(int i=0;i<5;i++)if(sqlite3_column_type(s,i)!=SQLITE_INTEGER)goto done;
    p->variants=sqlite3_column_int(s,0);p->copies=copies;
    if(p->variants<1||p->variants>128||copycol(s,5,p->unit,sizeof(p->unit))||sqlite3_column_type(s,6)!=SQLITE_TEXT||strcmp(p->unit,(const char*)sqlite3_column_text(s,6)))goto done;
    if(makeqty(sqlite3_column_int64(s,1),sqlite3_column_int64(s,2),&yield)||!yield.n||makeqty(sqlite3_column_int64(s,3),sqlite3_column_int64(s,4),&portion)||!portion.n||scaleqty(portion,p->variants*copies,&p->required))goto done;
    __int128 n=(__int128)p->required.n*yield.d,den=(__int128)p->required.d*yield.n;
    __int128 batches=(n+den-1)/den;if(batches<1||batches>100000)goto done;p->batches=(int)batches;
    if(scaleqty(yield,p->batches,&p->produced))goto done;
    __int128 rn=(__int128)p->produced.n*p->required.d-(__int128)p->required.n*p->produced.d,rd=(__int128)p->produced.d*p->required.d;
    if(rn<0||rn>INT64_MAX||rd>INT64_MAX)goto done;
    int64_t g=gcd((int64_t)rn,(int64_t)rd);p->remainder.n=(int64_t)rn/g;p->remainder.d=(int64_t)rd/g;
    sqlite3_finalize(s);s=NULL;
    if(gather(d,body,"$.preparation.shared_batch.inputs",p->batches,p)||gather(d,body,"$.preparation.per_variant",p->variants*copies,p))goto done;
    if(stmt(d,&s,"SELECT key FROM json_each(?1,'$.preparation.variants')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){char path[96];snprintf(path,sizeof(path),"$.preparation.variants[%d].additions",sqlite3_column_int(s,0));if(gather(d,body,path,copies,p))goto done;}
    if(step==SQLITE_DONE&&p->count>0)rc=0;
done:sqlite3_finalize(s);return rc;
}
const char *bf_recipe_material_path(void){static char p[4096];const char *x=getenv("BONFYRE_RECIPE_MATERIAL");if(x&&*x)return x;const char *h=getenv("HOME");if(!h)return "/var/lib/bonfyre/recipe-material.db";snprintf(p,sizeof(p),"%s/.bonfyre/estate-local/recipe-material.db",h);return p;}
static int record(sqlite3 *d,const char *key,sqlite3_int64 *row){sqlite3_stmt *s=NULL;int rc=1;if(stmt(d,&s,"SELECT seq FROM record WHERE record_key=?"))return 1;sqlite3_bind_text(s,1,key,-1,SQLITE_STATIC);if(sqlite3_step(s)==SQLITE_ROW){*row=sqlite3_column_int64(s,0);rc=0;}sqlite3_finalize(s);return rc;}
int bf_recipe_material_card(const char *path,int nth,char *out,size_t cap){sqlite3 *d=NULL;sqlite3_stmt *s=NULL;int rc=0;Html h={out,0,cap,0};if(!cap)return -1;*out=0;if(nth<0||bf_record_factor_open(path,&d))return 0;
    if(stmt(d,&s,"SELECT record_key,title,kind FROM record ORDER BY seq LIMIT 1 OFFSET ?"))goto done;
    sqlite3_bind_int(s,1,nth);
    if(sqlite3_step(s)==SQLITE_ROW){char *verified=NULL;size_t vn=0;sqlite3_stmt *seq=NULL;
        if(stmt(d,&seq,"SELECT seq FROM record ORDER BY seq LIMIT 1 OFFSET ?"))goto done;
        sqlite3_bind_int(seq,1,nth);int got=sqlite3_step(seq);
        int invalid=got!=SQLITE_ROW||bf_record_factor_read(d,sqlite3_column_int64(seq,0),&verified,&vn);
        free(verified);sqlite3_finalize(seq);if(invalid){rc=-1;goto done;}
        const char *kind=(const char*)sqlite3_column_text(s,2);add(&h,"<article class=\"fl-card is-note\"><p class=\"fl-kind\">%s</p><h2 class=\"fl-title fl-serif\">",!strcmp(kind,"aurekai.preparation-family.v1")?"One preparation, distinct variations":"A source recipe");esc(&h,(const char*)sqlite3_column_text(s,1));add(&h,"</h2><p class=\"fl-sub\">Exact retained material; ingredients and source stay attached.</p><a class=\"button primary\" href=\"/recipes?id=");url(&h,(const char*)sqlite3_column_text(s,0));add(&h,"\">Open ingredients and preparation <span>&rarr;</span></a><a class=\"button quiet\" href=\"/recipes\">Recipe collection</a></article>");rc=h.failed?-1:1;}
done:sqlite3_finalize(s);sqlite3_close(d);return rc;
}
static void field(Html *h,sqlite3 *d,const char *body,const char *path){sqlite3_stmt *s=NULL;if(stmt(d,&s,"SELECT json_extract(?1,?2)"))return;sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,path,-1,SQLITE_STATIC);if(sqlite3_step(s)==SQLITE_ROW)esc(h,(const char*)sqlite3_column_text(s,0));sqlite3_finalize(s);}
typedef struct { Html *html; int visible; } RecipePage;
static int recipe_page_row(void *context,sqlite3_int64 seq,const char *key,const char *title,const char *kind){
    RecipePage *page=context;(void)seq;(void)kind;
    if(page->visible++>=60)return 0;
    add(page->html,"<p><a href=\"/recipes?id=");url(page->html,key);add(page->html,"\">");esc(page->html,title);add(page->html,"</a></p>");return page->html->failed;
}
int bf_recipe_material_view_page(const char *path,const char *key,const char *query,int copies,int offset,char *out,size_t cap){sqlite3 *d=NULL;sqlite3_stmt *s=NULL;char *body=NULL;size_t n=0;Html h={out,0,cap,0};int rc=1,step;if(!cap)return 1;*out=0;
    if(offset<0||offset>1000000)return 1;
    if(bf_record_factor_open(path,&d)){add(&h,"<p class=\"flow-empty\">No recipe material is bound here.</p>");return 0;}
    add(&h,"<article class=\"subj\"><p class=\"eyebrow\"><i></i>Recipes &middot; retained sources and shared preparation</p>");
    if(!key||!*key){add(&h,"<h2 class=\"subj-name\">Make more from one preparation.</h2><a class=\"button primary\" href=\"/recipes/plan\">Plan a week together</a><form action=\"/recipes\" method=\"get\"><label>Find a recipe <input name=\"q\" value=\"");esc(&h,query);add(&h,"\"></label><button class=\"button primary\">Search</button></form><div class=\"subj-body\">");
        RecipePage page={&h,0};int nrows=bf_record_factor_visit(d,query,61,offset,recipe_page_row,&page);if(nrows<0)goto done;
        if(nrows==61){add(&h,"<a class=\"button quiet\" href=\"/recipes?q=");url(&h,query?query:"");add(&h,"&amp;offset=%d\">More recipes</a>",offset+60);}
        add(&h,"</div><p class=\"subj-note\">Search reads the factored corpus. Shared stored text is not permission to combine cooking processes.</p></article>");rc=h.failed?1:0;goto done;
    }
    sqlite3_int64 row;if(record(d,key,&row)||bf_record_factor_read(d,row,&body,&n))goto done;
    add(&h,"<h2 class=\"subj-name\">");field(&h,d,body,"$.title");add(&h,"</h2>");
    add(&h,"<a class=\"button primary\" href=\"/recipes/plan?add=");url(&h,key);add(&h,"\">Start a household plan with this</a>");
    if(stmt(d,&s,"SELECT json_type(?1,'$.preparation')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);step=sqlite3_step(s);int hasplan=step==SQLITE_ROW&&sqlite3_column_type(s,0)==SQLITE_TEXT&&!strcmp((const char*)sqlite3_column_text(s,0),"object");sqlite3_finalize(s);s=NULL;
    if(hasplan){Plan p;if(plan(d,body,copies,&p))goto done;
        add(&h,"<p class=\"subj-desc\">%d distinct variants &times; %d set(s). %d declared-capacity base batch(es), not %d separately prepared bases.</p><form action=\"/recipes\"><input type=\"hidden\" name=\"id\" value=\"",p.variants,p.copies,p.batches,p.variants*p.copies);esc(&h,key);add(&h,"\"><label>Sets <input name=\"copies\" type=\"number\" min=\"1\" max=\"100\" value=\"%d\"></label><button class=\"button primary\">Recalculate the shared plan</button></form>",copies);
        add(&h,"<h3>Shared preparation</h3><p>");field(&h,d,body,"$.preparation.shared_batch.instructions");add(&h,"</p><p>Declared output: ");qty(&h,p.produced);add(&h," ");esc(&h,p.unit);add(&h,"; assigned to variants: ");qty(&h,p.required);add(&h," ");esc(&h,p.unit);add(&h,"; arithmetic remainder: ");qty(&h,p.remainder);add(&h," ");esc(&h,p.unit);add(&h,".</p><h3>One combined ingredient list</h3><table><thead><tr><th>Ingredient</th><th>Quantity</th><th>Unit</th></tr></thead><tbody>");
        for(int i=0;i<p.count;i++){add(&h,"<tr><td>");esc(&h,p.items[i].name);add(&h,"</td><td>");qty(&h,p.items[i].q);add(&h,"</td><td>");esc(&h,p.items[i].unit);add(&h,"</td></tr>");}add(&h,"</tbody></table><h3>Keep the variations distinct</h3><ol>");
        if(stmt(d,&s,"SELECT json_extract(value,'$.name'),json_extract(value,'$.instructions') FROM json_each(?1,'$.preparation.variants')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
        while((step=sqlite3_step(s))==SQLITE_ROW){add(&h,"<li><strong>");esc(&h,(const char*)sqlite3_column_text(s,0));add(&h,"</strong>: ");esc(&h,(const char*)sqlite3_column_text(s,1));add(&h,"</li>");}if(step!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
        add(&h,"</ol><h3>Preparation and storage review</h3><p>");field(&h,d,body,"$.preparation.review_note");add(&h,"</p><p class=\"subj-note\">Amounts use declared yields, not measured kitchen output. Drop volumes, heating, cooling, storage life, prices and provider availability are not inferred. No order or helper booking has been made.</p><a class=\"button quiet\" href=\"/recipes?id=");url(&h,key);add(&h,"&amp;copies=%d&amp;format=list\">Save the grocery / helper ingredient list</a>",copies);
        add(&h,"<a class=\"button primary\" href=\"/recipes?id=");url(&h,key);add(&h,"&amp;copies=%d&amp;format=brief\">Open the complete helper brief</a>",copies);
    }else{add(&h,"<p class=\"subj-desc\">Yield as recorded: ");field(&h,d,body,"$.servings.raw");add(&h,"</p><h3>Ingredients</h3><ul>");
        if(stmt(d,&s,"SELECT json_extract(value,'$.raw') FROM json_each(?1,'$.ingredients')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
        while((step=sqlite3_step(s))==SQLITE_ROW){add(&h,"<li>");esc(&h,(const char*)sqlite3_column_text(s,0));add(&h,"</li>");}if(step!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
        add(&h,"</ul><h3>Instructions from the source</h3><p class=\"subj-desc\">");field(&h,d,body,"$.instructions");add(&h,"</p><p class=\"subj-note\">A retained source recipe, not a validated weekly storage plan. No shared cooking procedure has been inferred from similar wording.</p>");
    }
    add(&h,"<h3>Source</h3><p>");field(&h,d,body,"$.source.publisher");add(&h,"</p><p>");field(&h,d,body,"$.source.archive_url");add(&h,"</p><a class=\"button quiet\" href=\"/recipes\">Recipe collection</a><a class=\"button quiet\" href=\"/flow\">Back to the mixed feed</a></article>");rc=h.failed?1:0;
done:free(body);sqlite3_finalize(s);if(d)sqlite3_close(d);if(rc&&cap)snprintf(out,cap,"<p class=\"flow-empty\">Recipe material or preparation inputs could not be read and verified.</p>");return rc;
}
static const char *tsv_guard(const char *s){return *s&&strchr("=+-@",*s)?"'":"";}
int bf_recipe_material_list(const char *path,const char *key,int copies,FILE *out){sqlite3 *d=NULL;sqlite3_int64 row;char *body=NULL;size_t n;Plan p;int rc=1;
    if(bf_record_factor_open(path,&d)||record(d,key,&row)||bf_record_factor_read(d,row,&body,&n)||plan(d,body,copies,&p))goto done;
    if(fprintf(out,"ingredient_id\tingredient\tnumerator\tdenominator\tunit\tprice_status\n")<0)goto done;
    for(int i=0;i<p.count;i++){Amount *a=&p.items[i];if(fprintf(out,"%s%s\t%s%s\t%lld\t%lld\t%s%s\tnot_quoted\n",tsv_guard(a->id),a->id,tsv_guard(a->name),a->name,(long long)a->q.n,(long long)a->q.d,tsv_guard(a->unit),a->unit)<0)goto done;}rc=fflush(out)?1:0;
done:free(body);if(d)sqlite3_close(d);return rc;
}
/* The helper brief is another delivery representation of the same native
 * quantity plan, not a second planner, a booking, or a safety certification. */
static int brief_field(FILE *out,sqlite3 *db,const char *body,const char *path){
    sqlite3_stmt *s=NULL;int rc=1;
    if(stmt(db,&s,"SELECT json_extract(?1,?2)"))return 1;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,path,-1,SQLITE_STATIC);
    if(sqlite3_step(s)==SQLITE_ROW){const char *v=(const char*)sqlite3_column_text(s,0);if(v&&fputs(v,out)==EOF)goto done;rc=0;}
 done:sqlite3_finalize(s);return rc;
}
int bf_recipe_material_brief_exact(const char *path,const char *key,int copies,const char *expected,FILE *out){
    sqlite3 *db=NULL;sqlite3_stmt *s=NULL;sqlite3_int64 row;char *body=NULL;size_t n=0;Plan p;int rc=1,step;
    if(!out||copies<1||copies>100||bf_record_factor_open(path,&db)||record(db,key,&row)||bf_record_factor_read(db,row,&body,&n))goto done;
    char digest[65];bf_sha256_hex((const uint8_t*)body,n,digest);
    if(expected&&strcmp(expected,digest))goto done;
    if(stmt(db,&s,"SELECT json_type(?1,'$.preparation')='object'"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    int family=sqlite3_column_int(s,0)==1;sqlite3_finalize(s);s=NULL;
    if(!family){rc=source_recipe_brief(db,body,key,copies,out);goto done;}
    if(plan(db,body,copies,&p))goto done;
    if(fputs("PREPARATION BRIEF\n\n",out)==EOF||brief_field(out,db,body,"$.title"))goto done;
    fprintf(out,"\nSource key: %s\nSets: %d\nVariants per set: %d\nBase batches at declared capacity: %d\n",key,p.copies,p.variants,p.batches);
    fprintf(out,"Declared base output: %lld/%lld %s\nAssigned output: %lld/%lld %s\nUnassigned remainder: %lld/%lld %s\n",
      (long long)p.produced.n,(long long)p.produced.d,p.unit,(long long)p.required.n,(long long)p.required.d,p.unit,(long long)p.remainder.n,(long long)p.remainder.d,p.unit);
    fputs("\nCOMBINED INPUTS\n",out);
    for(int i=0;i<p.count;i++)fprintf(out,"- %s: %lld/%lld %s\n",p.items[i].name,(long long)p.items[i].q.n,(long long)p.items[i].q.d,p.items[i].unit);
    fputs("\nBASE INPUTS PER DECLARED-CAPACITY BATCH (not the whole-plan totals above)\n",out);
    Plan base={0};if(gather(db,body,"$.preparation.shared_batch.inputs",1,&base))goto done;
    for(int i=0;i<base.count;i++)fprintf(out,"- %s: %lld/%lld %s\n",base.items[i].name,(long long)base.items[i].q.n,(long long)base.items[i].q.d,base.items[i].unit);
    fprintf(out,"Repeat the base instruction for %d batch(es); do not assume one container can hold all batches.\n",p.batches);
    fputs("\nSHARED PREPARATION\n",out);if(brief_field(out,db,body,"$.preparation.shared_batch.instructions"))goto done;
    fputs("\n\nVARIATIONS: source instruction is per portion; the labeled addition totals cover all portions of that variant. Keep names, additions and lots distinct.\n",out);
    if(stmt(db,&s,"SELECT json_extract(value,'$.name'),json_extract(value,'$.instructions'),key FROM json_each(?1,'$.preparation.variants')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        const char *name=(const char*)sqlite3_column_text(s,0),*instructions=(const char*)sqlite3_column_text(s,1);
        fprintf(out,"\n%d. %s (%d portion(s))\n%s\n",sqlite3_column_int(s,2)+1,name?name:"",copies,instructions?instructions:"");
        char pathbuf[96];snprintf(pathbuf,sizeof(pathbuf),"$.preparation.variants[%d].additions",sqlite3_column_int(s,2));
        Plan additions={0};if(gather(db,body,pathbuf,copies,&additions))goto done;
        for(int i=0;i<additions.count;i++)fprintf(out,"   Total addition for this variant: %s = %lld/%lld %s\n",additions.items[i].name,(long long)additions.items[i].q.n,(long long)additions.items[i].q.d,additions.items[i].unit);
    }
    if(step!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
    fputs("\nREVIEW BEFORE PREPARING OR STORING\n",out);if(brief_field(out,db,body,"$.preparation.review_note"))goto done;
    fputs("\n\nSOURCE\n",out);if(brief_field(out,db,body,"$.source.publisher"))goto done;
    fputc('\n',out);if(brief_field(out,db,body,"$.source.archive_url"))goto done;
    fputs("\n\nThis is a proposed brief using declared yields. It does not establish tested cooking, cooling, storage, ingredient suitability, store prices, provider availability, or an accepted helper booking.\n",out);
    rc=ferror(out)||fflush(out)?1:0;
 done:sqlite3_finalize(s);free(body);if(db)sqlite3_close(db);return rc;
}
/* Nutritional allocation reuses the validated preparation plan. Shopping inputs
 * remain unchanged; only the homogeneous binder portion is allocated here. */
int bf_recipe_material_portion_demands(sqlite3 *db,const char *key,int copies,int index,
    BfRecipeDemandVisitor visitor,void *context,BfRecipeMaterialInfo *info,
    char *label,size_t label_cap,int *parts){
    sqlite3_int64 row;sqlite3_stmt *s=NULL;char *body=NULL;size_t bytes=0;int rc=1;
    if(!db||!key||!visitor||!info||!label||!label_cap||!parts||index<0||copies<1||copies>100)return 1;
    if(record(db,key,&row)||bf_record_factor_read(db,row,&body,&bytes))goto done;
    memset(info,0,sizeof(*info));bf_sha256_hex((const uint8_t*)body,bytes,info->digest);
    if(stmt(db,&s,"SELECT title,json_type(?1,'$.preparation')='object' FROM record WHERE seq=?2"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_int64(s,2,row);
    if(sqlite3_step(s)!=SQLITE_ROW||copycol(s,0,info->title,sizeof(info->title)))goto done;
    info->is_family=sqlite3_column_int(s,1)==1;sqlite3_finalize(s);s=NULL;
    if(!info->is_family){*parts=1;if(index||strlen(info->title)>=label_cap)goto done;
        strcpy(label,info->title);rc=bf_recipe_material_demands(db,key,copies,visitor,context,info);goto done;}
    Plan p,base={0},portion={0};if(plan(db,body,copies,&p)||index>p.variants)goto done;*parts=p.variants+1;
    Qty assigned=index==p.variants?p.remainder:p.required;
    __int128 rn=(__int128)assigned.n*p.produced.d*p.batches;
    __int128 rd=(__int128)assigned.d*p.produced.n*(index==p.variants?1:p.variants);
    if(rn<0||rd<=0||rn>INT64_MAX||rd>INT64_MAX)goto done;
    int64_t g=gcd((int64_t)rn,(int64_t)rd);Qty ratio={(int64_t)rn/g,(int64_t)rd/g};
    if(gather(db,body,"$.preparation.shared_batch.inputs",1,&base))goto done;
    for(int i=0;i<base.count;i++){
        Amount a=base.items[i];rn=(__int128)a.q.n*ratio.n;rd=(__int128)a.q.d*ratio.d;
        if(rn>INT64_MAX||rd>INT64_MAX)goto done;g=gcd((int64_t)rn,(int64_t)rd);
        a.q=(Qty){(int64_t)rn/g,(int64_t)rd/g};if(amount(&portion,&a,1))goto done;
    }
    if(index==p.variants){const char *v="Unassigned binder remainder";if(strlen(v)>=label_cap)goto done;strcpy(label,v);}
    else{
        char path[128];snprintf(path,sizeof(path),"$.preparation.variants[%d].name",index);
        if(stmt(db,&s,"SELECT json_extract(?1,?2)"))goto done;
        sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,path,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(s)!=SQLITE_ROW||copycol(s,0,label,label_cap))goto done;sqlite3_finalize(s);s=NULL;
        if(gather(db,body,"$.preparation.per_variant",copies,&portion))goto done;
        snprintf(path,sizeof(path),"$.preparation.variants[%d].additions",index);
        if(gather(db,body,path,copies,&portion))goto done;
    }
    for(int i=0;i<portion.count;i++){Amount *a=&portion.items[i];BfRecipeDemand v={a->id,a->name,a->unit,"",a->q,1,1};if(visitor(context,&v))goto done;}
    rc=0;
 done:sqlite3_finalize(s);free(body);return rc;
}
int bf_recipe_material_command(int argc,char **argv){if(argc<3)return 2;const char *op=argv[2];
    if(!strcmp(op,"factor")&&argc==5)return bf_record_factor_build(argv[3],argv[4],stdout);
    if(!strcmp(op,"verify")&&argc==5)return bf_record_factor_verify(argv[3],argv[4],stdout);
    if(!strcmp(op,"export")&&argc==4)return bf_record_factor_export(argv[3],stdout);
    if(!strcmp(op,"brief")&&argc==6){char *end;long n=strtol(argv[5],&end,10);if(*end||n<1||n>100)return 2;return bf_recipe_material_brief(argv[3],argv[4],(int)n,stdout);}
    if(!strcmp(op,"list")&&argc==6){char *end;long n=strtol(argv[5],&end,10);if(*end||n<1||n>100)return 2;return bf_recipe_material_list(argv[3],argv[4],(int)n,stdout);}
    fprintf(stderr,"recipe corpus factor SOURCE OUTPUT | verify PACK SOURCE | export PACK | list PACK KEY SETS | brief PACK KEY SETS\n");return 2;
}

int bf_recipe_material_view(const char *path,const char *key,const char *query,int copies,char *out,size_t cap){return bf_recipe_material_view_page(path,key,query,copies,0,out,cap);}

int bf_recipe_material_brief(const char *path,const char *key,int copies,FILE *out){return bf_recipe_material_brief_exact(path,key,copies,NULL,out);}

/* Source fractions never go through floating point. Unknown expressions
 * remain review rows instead of being coerced to zero or silently dropped. */
static int source_integer(const char **text,int64_t *value){
    const unsigned char *p=(const unsigned char*)*text;int64_t n=0;
    if(*p<'0'||*p>'9')return 1;
    while(*p>='0'&&*p<='9'){if(n>100000000)return 1;n=n*10+(*p++-'0');}
    if(n>1000000000)return 1;
    *text=(const char*)p;*value=n;return 0;
}
static int source_quantity(const char *text,Qty *out){
    if(!text||!*text)return 1;
    const char *p=text;int64_t whole=0,part=0,den=1;
    while(*p==' ')p++;
    if(*p!='.'&&source_integer(&p,&whole))return 1;
    if(*p=='.'){
        p++;const char *start=p;
        while(*p>='0'&&*p<='9'){if(den>=1000000)return 1;part=part*10+(*p++-'0');den*=10;}
        if(p==start||whole>(1000000000-part)/den)return 1;
        whole=whole*den+part;
    }else if(*p=='/'){
        p++;if(source_integer(&p,&den)||!den)return 1;
    }else if(*p==' '){
        while(*p==' ')p++;
        if(*p){if(source_integer(&p,&part)||*p++!='/'||source_integer(&p,&den)||!den||part>=den||whole>(1000000000-part)/den)return 1;whole=whole*den+part;}
    }
    while(*p==' ')p++;
    return *p?1:makeqty(whole,den,out);
}
int bf_recipe_quantity_sum(BfRecipeQuantity a,BfRecipeQuantity b,BfRecipeQuantity *out){
    if(!out||a.n<0||b.n<0||a.d<=0||b.d<=0)return 1;
    return sumqty(a,b,out);
}
int bf_recipe_material_demands(sqlite3 *db,const char *key,int copies,
                               BfRecipeDemandVisitor visitor,void *context,
                               BfRecipeMaterialInfo *info){
    sqlite3_stmt *s=NULL;sqlite3_int64 row;char *body=NULL;size_t bytes=0;int rc=1,step;
    if(!db||!key||!visitor||!info||copies<1||copies>100||record(db,key,&row)||bf_record_factor_read(db,row,&body,&bytes))goto done;
    memset(info,0,sizeof(*info));
    if(stmt(db,&s,"SELECT title,digest,json_type(?1,'$.preparation')='object' FROM record WHERE seq=?2"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);sqlite3_bind_int64(s,2,row);
    if(sqlite3_step(s)!=SQLITE_ROW||copycol(s,0,info->title,sizeof(info->title))||copycol(s,1,info->digest,sizeof(info->digest)))goto done;
    info->is_family=sqlite3_column_int(s,2)==1;sqlite3_finalize(s);s=NULL;
    if(info->is_family){
        Plan p;if(plan(db,body,copies,&p))goto done;
        for(int i=0;i<p.count;i++){
            BfRecipeDemand v={p.items[i].id,p.items[i].name,p.items[i].unit,"",p.items[i].q,1,1};
            if(visitor(context,&v))goto done;
        }
        rc=0;goto done;
    }
    if(stmt(db,&s,"SELECT json_type(?1,'$.ingredients')='array' AND json_array_length(?1,'$.ingredients')>0 AND json_array_length(?1,'$.ingredients')<=256"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW||sqlite3_column_int(s,0)!=1)goto done;
    sqlite3_finalize(s);s=NULL;
    if(stmt(db,&s,"SELECT json_extract(value,'$.name'),coalesce(json_extract(value,'$.unit'),''),json_extract(value,'$.quantity'),json_extract(value,'$.raw') FROM json_each(?1,'$.ingredients')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        char name[192],unit[32],raw[1024],id[96],digest[65];Qty q={0,1},scaled={0,1};
        if(copycol(s,0,name,sizeof(name))||!*name||copycol(s,1,unit,sizeof(unit))||copycol(s,3,raw,sizeof(raw)))goto done;
        int known=sqlite3_column_type(s,2)==SQLITE_TEXT&&*unit&&!source_quantity((const char*)sqlite3_column_text(s,2),&q)&&!scaleqty(q,copies,&scaled);
        bf_sha256_hex((const uint8_t*)name,strlen(name),digest);
        snprintf(id,sizeof(id),"source-label:%s",digest);
        BfRecipeDemand v={id,name,unit,raw,scaled,known,0};if(visitor(context,&v))goto done;
    }
    if(step==SQLITE_DONE)rc=0;
 done:sqlite3_finalize(s);free(body);return rc;
}
static int source_recipe_brief(sqlite3 *db,const char *body,const char *key,int copies,FILE *out){
    sqlite3_stmt *s=NULL;int step,rc=1;
    fprintf(out,"SOURCE RECIPE: %s\nSource recipe copies: %d\n",key,copies);
    if(brief_field(out,db,body,"$.title"))goto done;
    fputs("\nYield as recorded (per source recipe): ",out);if(brief_field(out,db,body,"$.servings.raw"))goto done;
    fputs("\n\nSOURCE INPUTS PER RECIPE (use the combined shopping list for scaled totals)\n",out);
    if(stmt(db,&s,"SELECT json_extract(value,'$.raw') FROM json_each(?1,'$.ingredients')"))goto done;
    sqlite3_bind_text(s,1,body,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){const char *raw=(const char*)sqlite3_column_text(s,0);if(!raw)goto done;fprintf(out,"- %s\n",raw);}
    if(step!=SQLITE_DONE)goto done;
    fputs("\nSOURCE INSTRUCTIONS\n",out);if(brief_field(out,db,body,"$.instructions"))goto done;
    fputs("\n\nSOURCE\n",out);if(brief_field(out,db,body,"$.source.publisher"))goto done;
    fputc('\n',out);if(brief_field(out,db,body,"$.source.archive_url"))goto done;
    fputs("\nKeep preparation batches separate. Multiplying ingredients does not establish equipment capacity, scaled cooking time, safe storage, or a tested shared procedure.\n",out);
    rc=ferror(out)?1:0;
 done:sqlite3_finalize(s);return rc;
}
