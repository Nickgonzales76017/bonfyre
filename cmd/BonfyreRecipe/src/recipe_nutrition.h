#ifndef BF_RECIPE_NUTRITION_H
#define BF_RECIPE_NUTRITION_H
#include "recipe_week.h"
/* Read-only calculation over Recipe-owned food records and explicit bindings.
 * Binding and material digests must agree; missing quantities are not zero. */
int bf_recipe_nutrition_report(const char *material,const char *nutrition,
    const BfRecipeWeek *week,FILE *out,int html);
typedef struct {char *text,*html;char digest[65];int state;} BfRecipeNutrition;
int bf_recipe_nutrition_bundle(const char *material,const char *nutrition,
    const BfRecipeWeek *week,const char *const *expected,BfRecipeNutrition *result);
void bf_recipe_nutrition_free(BfRecipeNutrition *result);
int bf_recipe_nutrition_command(int argc,char **argv);
#endif
