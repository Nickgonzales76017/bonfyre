#define _POSIX_C_SOURCE 200809L
#include "recipe_nutrition.h"
#include "record_factor.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
/* Distinct energy definitions are never added. This first projection requests
 * the SR Legacy energy measure and six named macronutrient/sodium measures. */
static const int ids[]={1008,1003,1004,1005,1079,2000,1093};
static const char *names[]={"Energy","Protein","Fat","Carbohydrate","Fibre","Sugars","Sodium"};
static const char *units[]={"kcal","g","g","g","g","g","mg"};

#define NUTRIENT_MAX 256
typedef struct {int id;char name[192],unit[24];} Measure;
typedef struct {Measure row[NUTRIENT_MAX];int count;} Measures;
typedef struct FoodProfile {sqlite3_int64 id;double amount[NUTRIENT_MAX];unsigned char known[NUTRIENT_MAX];struct FoodProfile *next;} FoodProfile;
typedef struct {sqlite3 *db;Measures measures;FoodProfile *profiles;int count;} NutrientSession;
static int compare_measure(const void *a,const void *b){int x=((const Measure*)a)->id,y=((const Measure*)b)->id;return (x>y)-(x<y);}
static int load_measures(NutrientSession *session,sqlite3 *pack,const BfRecipeWeek *w){
    Measures *m=&session->measures;sqlite3_stmt *s=NULL,*r=NULL;int rc=1,step;
    for(int i=0;i<7;i++){m->row[i].id=ids[i];strcpy(m->row[i].name,names[i]);strcpy(m->row[i].unit,units[i]);}m->count=7;
    if(sqlite3_prepare_v2(pack,"SELECT digest FROM record WHERE record_key=?",-1,&r,NULL)!=SQLITE_OK)return 1;
    if(sqlite3_prepare_v2(session->db,"SELECT DISTINCT v.nutrient_id,v.name,v.unit FROM recipe_food_binding b JOIN nutrition_food f ON f.fdc_id=b.fdc_id AND f.dataset_digest=b.dataset_digest JOIN nutrition_value v ON v.fdc_id=f.fdc_id WHERE b.recipe_key=?1 AND b.recipe_digest=?2 ORDER BY v.nutrient_id",-1,&s,NULL)!=SQLITE_OK)goto done;
    for(int k=0;k<w->count;k++){
        sqlite3_reset(r);sqlite3_bind_text(r,1,w->items[k].key,-1,SQLITE_STATIC);if(sqlite3_step(r)!=SQLITE_ROW)goto done;
        const char *digest=(const char*)sqlite3_column_text(r,0);if(!digest)goto done;
        sqlite3_reset(s);sqlite3_bind_text(s,1,w->items[k].key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,digest,-1,SQLITE_TRANSIENT);
        while((step=sqlite3_step(s))==SQLITE_ROW){
            int id=sqlite3_column_int(s,0),found=-1;const char *name=(const char*)sqlite3_column_text(s,1),*unit=(const char*)sqlite3_column_text(s,2);
            if(id<=0||!name||!unit||strlen(name)>=sizeof(m->row[0].name)||strlen(unit)>=sizeof(m->row[0].unit))goto done;
            for(int i=0;i<m->count;i++)if(m->row[i].id==id){found=i;break;}
            if(found>=0){if(strcmp(m->row[found].unit,unit))goto done;continue;}
            if(m->count==NUTRIENT_MAX)goto done;Measure *v=&m->row[m->count++];v->id=id;strcpy(v->name,name);strcpy(v->unit,unit);
        }if(step!=SQLITE_DONE)goto done;
    }
    qsort(m->row+7,(size_t)(m->count-7),sizeof(m->row[0]),compare_measure);rc=0;
 done:sqlite3_finalize(s);sqlite3_finalize(r);return rc;
}
static FoodProfile *food_profile(NutrientSession *session,sqlite3_int64 id){
    for(FoodProfile *p=session->profiles;p;p=p->next)if(p->id==id)return p;
    if(session->count>=4096)return NULL;
    FoodProfile *p=calloc(1,sizeof(*p));sqlite3_stmt *s=NULL;int step;if(!p)return NULL;p->id=id;
    if(sqlite3_prepare_v2(session->db,"SELECT nutrient_id,amount,unit,loq FROM nutrition_value WHERE fdc_id=?",-1,&s,NULL)!=SQLITE_OK)goto bad;
    sqlite3_bind_int64(s,1,id);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        if(sqlite3_column_type(s,1)==SQLITE_NULL)continue;
        if(sqlite3_column_type(s,1)!=SQLITE_INTEGER&&sqlite3_column_type(s,1)!=SQLITE_FLOAT)goto bad;
        double amount=sqlite3_column_double(s,1);const char *unit=(const char*)sqlite3_column_text(s,2);
        if(!unit||!isfinite(amount)||amount<0)goto bad;
        if(sqlite3_column_type(s,3)!=SQLITE_NULL){double loq=sqlite3_column_double(s,3);if(!isfinite(loq)||loq<0)goto bad;if(loq>0)continue;}
        int nid=sqlite3_column_int(s,0);
        for(int i=0;i<session->measures.count;i++)if(session->measures.row[i].id==nid){if(strcmp(unit,session->measures.row[i].unit))goto bad;p->amount[i]=amount;p->known[i]=1;break;}
    }
    if(step!=SQLITE_DONE)goto bad;sqlite3_finalize(s);p->next=session->profiles;session->profiles=p;session->count++;return p;
 bad:sqlite3_finalize(s);free(p);return NULL;
}
typedef struct {
    NutrientSession *session;
    sqlite3 *db; const char *key,*digest; FILE *trace; int html;
    long double sum[NUTRIENT_MAX]; int supplied[NUTRIENT_MAX],count,matched,missing; int reference;
} Nutrition;
static void escaped(FILE *f,const char *p,int html){
    for(;p&&*p;p++){if(html&&*p=='&')fputs("&amp;",f);else if(html&&*p=='<')fputs("&lt;",f);
        else if(html&&*p=='>')fputs("&gt;",f);else if(html&&*p=='\"')fputs("&quot;",f);
        else if(*p=='\n'||*p=='\r'||*p=='\t')fputc(' ',f);else fputc((unsigned char)*p,f);}
}
static void note(Nutrition *n,const BfRecipeDemand *v,const char *why){
    n->missing++;fputs(n->html?"<li>":"UNRESOLVED\t",n->trace);escaped(n->trace,v->name,n->html);
    fputs(n->html?": ":"\t",n->trace);escaped(n->trace,why,n->html);fputs(n->html?"</li>":"\n",n->trace);
}
static int ingredient(void *context,const BfRecipeDemand *v){
    Nutrition *n=context;sqlite3_stmt *s=NULL;int rc=1,step;long double grams;
    if(!v)return 1;
    if(v->known&&(v->quantity.d<=0||v->quantity.n<0))return 1;
    if(v->known&&v->quantity.n==0)return 0;
    n->count++;
    if(!v->known){note(n,v,"quantity unmeasured");return 0;}
    const char *query="SELECT b.fdc_id,b.grams_per_unit,b.basis,f.description,f.dataset_digest,b.basis_ref "
        "FROM recipe_food_binding b JOIN nutrition_food f ON f.fdc_id=b.fdc_id "
        "WHERE b.recipe_key=?1 AND b.recipe_digest=?2 AND b.ingredient_id=?3 AND b.unit=?4 "
        "AND b.dataset_digest=f.dataset_digest";
    if(sqlite3_prepare_v2(n->db,query,-1,&s,NULL)!=SQLITE_OK)return 1;
    sqlite3_bind_text(s,1,n->key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,n->digest,-1,SQLITE_STATIC);
    sqlite3_bind_text(s,3,v->id,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,v->unit,-1,SQLITE_STATIC);
    step=sqlite3_step(s);
    if(step==SQLITE_DONE){sqlite3_finalize(s);note(n,v,"no current food identity and measured/declared unit binding");return 0;}
    if(step!=SQLITE_ROW)goto done;
    double scale=sqlite3_column_double(s,1);const char *basis=(const char*)sqlite3_column_text(s,2);
    if(!basis||!isfinite(scale)||scale<=0)goto done;
    grams=(long double)v->quantity.n/v->quantity.d*scale;if(!isfinite(grams)||grams>1e12L)goto done;
    sqlite3_int64 food=sqlite3_column_int64(s,0);n->matched++;
    if(strcmp(basis,"measured_product"))n->reference=1;
    fputs(n->html?"<li>":"BINDING\t",n->trace);escaped(n->trace,v->name,n->html);
    fprintf(n->trace,n->html?": %.6Lg g; FDC %lld; ":"\t%.9Lg\tg\t%lld\t",grams,(long long)food);
    escaped(n->trace,(const char*)sqlite3_column_text(s,3),n->html);fputs(n->html?"; ":"\t",n->trace);
    escaped(n->trace,basis,n->html);fputs(n->html?"; data ":"\t",n->trace);
    escaped(n->trace,(const char*)sqlite3_column_text(s,4),n->html);fputs(n->html?"; ":"\t",n->trace);
    escaped(n->trace,(const char*)sqlite3_column_text(s,5),n->html);fputs(n->html?"</li>":"\n",n->trace);
    sqlite3_finalize(s);s=NULL;
    FoodProfile *profile=food_profile(n->session,food);if(!profile)goto done;
    for(int i=0;i<n->session->measures.count;i++)if(profile->known[i]){
        n->sum[i]+=grams*profile->amount[i]/100.0L;if(!isfinite(n->sum[i]))goto done;n->supplied[i]++;
    }
    rc=0;
 done:sqlite3_finalize(s);return rc;
}
static const char *nutrition_path(void){
    static char p[4096];const char *v=getenv("BONFYRE_NUTRITION_DB");if(v&&*v)return v;
    const char *h=getenv("HOME");if(!h)return NULL;
    if(snprintf(p,sizeof(p),"%s/.bonfyre/recipes.db",h)>=(int)sizeof(p))return NULL;return p;
}
typedef struct {Nutrition n;char label[512],digest[65];char *trace;int copies,remainder;} FormulationRow;
static int render_nutrition(NutrientSession *session,sqlite3 *pack,const BfRecipeWeek *w,const char *const *expected,FILE *out,int html){
    FormulationRow *rows=NULL;size_t count=0,capacity=0;char *text=NULL;size_t size=0;FILE *f=NULL;int rc=1;
    for(int k=0;k<w->count;k++){
        int parts=1;
        for(int part=0;part<parts;part++){
            if(count==capacity){size_t c=capacity?capacity*2:16;if(c>4096)c=4096;if(count==c)goto done;
                FormulationRow *p=realloc(rows,c*sizeof(*p));if(!p)goto done;rows=p;capacity=c;}
            FormulationRow *row=&rows[count++];memset(row,0,sizeof(*row));row->copies=w->items[k].copies;
            BfRecipeMaterialInfo info={0};row->n=(Nutrition){.session=session,.db=session->db,.key=w->items[k].key,.digest=info.digest,.html=html};
            size_t trace_size=0;row->n.trace=open_memstream(&row->trace,&trace_size);if(!row->n.trace)goto done;
            int failed=bf_recipe_material_portion_demands(pack,row->n.key,row->copies,part,ingredient,&row->n,&info,row->label,sizeof(row->label),&parts);
            if(expected&&strcmp(expected[k],info.digest))failed=1;
            if(fclose(row->n.trace))failed=1;row->n.trace=NULL;if(failed)goto done;
            strcpy(row->digest,info.digest);
            row->remainder=info.is_family && part==parts-1;
        }
    }
    Nutrition total[2]={{0},{0}};
    for(size_t j=0;j<count;j++){
        Nutrition *sum=&total[rows[j].remainder?1:0],*n=&rows[j].n;
        sum->count+=n->count;
        for(int i=0;i<session->measures.count;i++){
            sum->sum[i]+=n->sum[i];sum->supplied[i]+=n->supplied[i];
            if(!isfinite(sum->sum[i]))goto done;
        }
    }
    f=open_memstream(&text,&size);if(!f)goto done;
    if(!html){
        fputs("NUTRITIONAL FORMULATION\nFood-reference estimates, not laboratory measurements. Each cell is amount|covered/required ingredient rows. ? means unknown, not zero. Units and definitions remain distinct. V columns include the labelled number of sets; unassigned binder is separate.\n\nPORTIONS\ncolumn\tpreparation\tsets\trecipe_sha256\n",f);
        for(size_t j=0;j<count;j++){fprintf(f,"V%zu\t",j+1);escaped(f,rows[j].label,0);fprintf(f,"\t%d\t%s\n",rows[j].copies,rows[j].digest);}
        fputs("\nNUTRIENTS\nnutrient_id\tnutrient\tunit",f);for(size_t j=0;j<count;j++)fprintf(f,"\tV%zu",j+1);fputc('\n',f);
        for(int i=0;i<session->measures.count;i++){
            Measure *m=&session->measures.row[i];fprintf(f,"%d\t",m->id);escaped(f,m->name,0);fputc('\t',f);escaped(f,m->unit,0);
            for(size_t j=0;j<count;j++){Nutrition *n=&rows[j].n;fputc('\t',f);if(n->supplied[i]||!n->count)fprintf(f,"%.9Lg",n->sum[i]);else fputc('?',f);fprintf(f,"|%d/%d",n->supplied[i],n->count);}fputc('\n',f);
        }
        fputs("\nFORMULATION TOTALS\nInput composition, not cooked retention or food actually consumed. The unassigned binder is excluded from assigned preparations.\nnutrient_id\tnutrient\tunit\tassigned_preparations\tunassigned_binder\n",f);
        for(int i=0;i<session->measures.count;i++){
            Measure *m=&session->measures.row[i];fprintf(f,"%d\t",m->id);escaped(f,m->name,0);fputc('\t',f);escaped(f,m->unit,0);
            for(int t=0;t<2;t++){
                Nutrition *n=&total[t];fputc('\t',f);
                if(n->supplied[i]||!n->count)fprintf(f,"%.9Lg",n->sum[i]);else fputc('?',f);
                fprintf(f,"|%d/%d",n->supplied[i],n->count);
            }fputc('\n',f);
        }
        fputs("\nBINDINGS AND UNRESOLVED INPUTS\n",f);
        for(size_t j=0;j<count;j++){fprintf(f,"\nV%zu\n",j+1);if(rows[j].trace)fputs(rows[j].trace,f);}
    }else{
        fputs("<section class=\"desk-q\" data-nutrition-owner=\"Recipe\"><h3>Calculated nutritional formulation</h3><p>Source-bound food composition estimates. Shared binder is allocated to each portion; unused binder is separate. Missing nutrients and dispenser-dependent drops are not zero.</p>",f);
        fputs("<details open data-nutrition-totals=\"assigned-versus-remainder\"><summary>Whole-plan formulation</summary><p>Input composition, not cooked retention or food actually consumed. Unassigned binder is excluded from the preparations.</p><table><thead><tr><th>Nutrient</th><th>Assigned preparations</th><th>Unassigned binder</th></tr></thead><tbody>",f);
        for(int i=0;i<7;i++){
            Measure *m=&session->measures.row[i];fputs("<tr><td>",f);escaped(f,m->name,1);fputs("</td>",f);
            for(int t=0;t<2;t++){
                Nutrition *n=&total[t];fputs("<td>",f);
                if(n->supplied[i]||!n->count){fprintf(f,"%.3Lf ",n->sum[i]);escaped(f,m->unit,1);}else fputs("Unknown",f);
                fprintf(f," <small>%s (%d/%d)</small></td>",n->supplied[i]==n->count?"complete reference estimate":"known subtotal; incomplete",n->supplied[i],n->count);
            }fputs("</tr>",f);
        }fputs("</tbody></table></details>",f);
        for(size_t j=0;j<count;j++){
            Nutrition *n=&rows[j].n;fputs("<details><summary>",f);escaped(f,rows[j].label,1);fprintf(f," (%d set(s))</summary><p>Recipe SHA-256: %s</p><table><thead><tr><th>Nutrient</th><th>Amount</th><th>Coverage</th></tr></thead><tbody>",rows[j].copies,rows[j].digest);
            for(int i=0;i<session->measures.count;i++){
                Measure *m=&session->measures.row[i];if(i==7)fputs("</tbody></table><details><summary>Additional nutrient measures</summary><table><tbody>",f);
                fputs("<tr><td>",f);escaped(f,m->name,1);fputs("</td><td>",f);if(n->supplied[i]||!n->count){fprintf(f,"%.3Lf ",n->sum[i]);escaped(f,m->unit,1);}else fputs("Unknown",f);
                fprintf(f,"</td><td>%s (%d/%d)</td></tr>",n->supplied[i]==n->count?"complete reference estimate":"known subtotal; incomplete",n->supplied[i],n->count);
            }
            fputs("</tbody></table>",f);if(session->measures.count>7)fputs("</details>",f);fputs("<ul>",f);if(rows[j].trace)fputs(rows[j].trace,f);fputs("</ul></details>",f);
        }
        fputs("</section>",f);
    }
    if(fclose(f)){f=NULL;goto done;}f=NULL;if(size>1024*1024)goto done;
    if(fwrite(text,1,size,out)!=size||ferror(out))goto done;rc=0;
 done:if(f)fclose(f);free(text);for(size_t j=0;j<count;j++){if(rows[j].n.trace)fclose(rows[j].n.trace);free(rows[j].trace);}free(rows);return rc;
}

void bf_sha256_hex(const uint8_t *,size_t,char *);
void bf_recipe_nutrition_free(BfRecipeNutrition *r){if(r){free(r->text);free(r->html);memset(r,0,sizeof(*r));}}
int bf_recipe_nutrition_bundle(const char *material,const char *nutrition,const BfRecipeWeek *w,
    const char *const *expected,BfRecipeNutrition *r){
    sqlite3 *db=NULL,*pack=NULL;sqlite3_stmt *s=NULL;FILE *f=NULL;size_t n=0;int rc=1;NutrientSession session={0};
    if(!r)return 1;memset(r,0,sizeof(*r));
    if(!w||w->count<1||w->count>BF_RECIPE_WEEK_MAX)return 1;
    if(!nutrition)nutrition=nutrition_path();
    if(!nutrition||sqlite3_open_v2(nutrition,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){rc=2;goto done;}
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name='recipe_food_binding'",-1,&s,NULL)!=SQLITE_OK)goto done;
    int present=sqlite3_step(s)==SQLITE_ROW;sqlite3_finalize(s);s=NULL;if(!present){rc=2;goto done;}
    if(bf_record_factor_open(material,&pack)||sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK||sqlite3_exec(pack,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    session.db=db;if(load_measures(&session,pack,w))goto done;
    /* Both delivery forms share snapshots and reuse each food vector. */
    f=open_memstream(&r->text,&n);if(!f)goto done;
    int bad=render_nutrition(&session,pack,w,expected,f,0);if(fclose(f))bad=1;f=NULL;if(bad)goto done;
    bf_sha256_hex((const uint8_t*)r->text,n,r->digest);
    f=open_memstream(&r->html,&n);if(!f)goto done;
    bad=render_nutrition(&session,pack,w,expected,f,1);if(fclose(f))bad=1;f=NULL;if(bad)goto done;rc=0;
 done:if(f)fclose(f);sqlite3_finalize(s);if(pack)sqlite3_close(pack);if(db)sqlite3_close(db);
    while(session.profiles){FoodProfile *p=session.profiles;session.profiles=p->next;free(p);}
    if(rc)bf_recipe_nutrition_free(r);r->state=rc;return rc;
}
int bf_recipe_nutrition_report(const char *material,const char *nutrition,const BfRecipeWeek *w,FILE *out,int html){
    if(!out)return 1;BfRecipeNutrition r;int rc=bf_recipe_nutrition_bundle(material,nutrition,w,NULL,&r);
    if(rc)return rc;const char *p=html?r.html:r.text;size_t n=strlen(p);if(fwrite(p,1,n,out)!=n||ferror(out))rc=1;
    bf_recipe_nutrition_free(&r);return rc;
}
int bf_recipe_nutrition_command(int argc,char **argv){
    enum{RN_CORPUS};static const BfCommandEntry nutrition_commands[]={{"corpus",RN_CORPUS,0,NULL},};
    const BfCommandEntry*command=argc<2?NULL:bf_command_lookup(nutrition_commands,sizeof(nutrition_commands)/sizeof(nutrition_commands[0]),argv[1]);
    if(argc<3||!command||command->id!=RN_CORPUS||strcmp(argv[2],"nutrition"))return -1;
    if(argc!=5&&argc!=6){fputs("recipe corpus nutrition PACK 'KEY*COPIES;...' [EXISTING_RECIPE_DB]\n",stderr);return 2;}
    BfRecipeWeek w;if(bf_recipe_week_parse(argv[4],&w))return 2;
    int rc=bf_recipe_nutrition_report(argv[3],argc==6?argv[5]:NULL,&w,stdout,0);
    if(rc)fputs("Nutrition inputs unavailable, stale, or invalid; no complete total emitted.\n",stderr);return rc;
}
