#ifndef BF_RECIPE_MATERIAL_H
#define BF_RECIPE_MATERIAL_H
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <sqlite3.h>
/* Shared native projection: the CLI and WebAddress read the same factored body
 * and compile the same quantity plan. No browser-owned business calculation. */
const char *bf_recipe_material_path(void);
int bf_recipe_material_card(const char *path,int nth,char *out,size_t cap);
int bf_recipe_material_view(const char *path,const char *key,const char *query,int copies,char *out,size_t cap);
int bf_recipe_material_view_page(const char *path,const char *key,const char *query,int copies,int offset,char *out,size_t cap);
int bf_recipe_material_list(const char *path,const char *key,int copies,FILE *out);
int bf_recipe_material_brief(const char *path,const char *key,int copies,FILE *out);
int bf_recipe_material_command(int argc,char **argv);
/* Typed demand passes directly from material to its consumers. Source labels
 * are not promoted to supplier identities; unknown quantities remain explicit. */
typedef struct { int64_t n,d; } BfRecipeQuantity;
typedef struct {
    const char *id,*name,*unit,*raw;
    BfRecipeQuantity quantity;
    int known,identity_declared;
} BfRecipeDemand;
typedef struct { char title[4097],digest[65]; int is_family; } BfRecipeMaterialInfo;
typedef int (*BfRecipeDemandVisitor)(void *,const BfRecipeDemand *);
int bf_recipe_quantity_sum(BfRecipeQuantity a,BfRecipeQuantity b,BfRecipeQuantity *out);
int bf_recipe_material_demands(sqlite3 *db,const char *key,int copies,
                               BfRecipeDemandVisitor visitor,void *context,
                               BfRecipeMaterialInfo *info);
int bf_recipe_material_portion_demands(sqlite3 *db,const char *key,int copies,int index,
    BfRecipeDemandVisitor visitor,void *context,BfRecipeMaterialInfo *info,
    char *label,size_t label_cap,int *parts);
int bf_recipe_material_brief_exact(const char *path,const char *key,int copies,
                                   const char *digest,FILE *out);
#endif
