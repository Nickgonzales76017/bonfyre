#ifndef BF_RECIPE_WEEK_H
#define BF_RECIPE_WEEK_H
#include "recipe_material.h"
#define BF_RECIPE_WEEK_MAX 16
#define BF_RECIPE_WEEK_SPEC 4096
/* A selection carries references, never a second copy of recipe meaning. */
typedef struct { char key[241]; int copies; } BfRecipeSelection;
typedef struct { BfRecipeSelection items[BF_RECIPE_WEEK_MAX]; int count; } BfRecipeWeek;
int bf_recipe_week_parse(const char *spec, BfRecipeWeek *week);
int bf_recipe_week_set(BfRecipeWeek *week, const char *key, int copies);
int bf_recipe_week_spec(const BfRecipeWeek *week, char *out, size_t cap);
int bf_recipe_week_view(const char *path, const BfRecipeWeek *week, const char *search,
                        int offset, int can_save, char *out, size_t cap);
int bf_recipe_week_export(const char *path, const BfRecipeWeek *week, int brief, FILE *out);
int bf_recipe_week_command(int argc, char **argv);
#endif
