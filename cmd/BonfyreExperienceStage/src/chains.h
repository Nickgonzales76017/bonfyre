#ifndef BONFYRE_EXPERIENCE_STAGE_CHAINS_H
#define BONFYRE_EXPERIENCE_STAGE_CHAINS_H
#include <stddef.h>

typedef struct {
    char *id;
    char **stages;
    size_t stage_count;
} BfStageChain;

typedef struct {
    BfStageChain *items;
    size_t count;
} BfStageChains;

int bf_stage_chains_load(const char *path, BfStageChains *out);
const BfStageChain *bf_stage_chain_find(const BfStageChains *chains, const char *id);
void bf_stage_chains_free(BfStageChains *chains);

#endif
