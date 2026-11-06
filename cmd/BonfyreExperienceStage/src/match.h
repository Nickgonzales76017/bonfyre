#ifndef BONFYRE_EXPERIENCE_STAGE_MATCH_H
#define BONFYRE_EXPERIENCE_STAGE_MATCH_H

#include "chains.h"
#include <stddef.h>

typedef struct {
    char **items;
    size_t count;
} BfStageStrings;

int bf_stage_select(const BfStageChain *chain,
                    const char *const *hints, size_t hint_count,
                    BfStageStrings *out);
int bf_stage_select_presentation(const BfStageChain *chain, BfStageStrings *out);
int bf_stage_app_hints(const char *name, BfStageStrings *out);
void bf_stage_strings_free(BfStageStrings *values);

#endif
