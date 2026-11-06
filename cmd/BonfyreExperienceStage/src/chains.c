#define _POSIX_C_SOURCE 200809L
#include "chains.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void free_chain(BfStageChain *c) {
    if (!c) return;
    free(c->id);
    for (size_t i = 0; i < c->stage_count; ++i) free(c->stages[i]);
    free(c->stages);
    memset(c, 0, sizeof(*c));
}

static int split_stages(char *text, BfStageChain *out) {
    char *save = NULL;
    char *tok = strtok_r(text, "|", &save);
    while (tok) {
        char **next = realloc(out->stages, (out->stage_count + 1) * sizeof(*next));
        if (!next) return 0;
        out->stages = next;
        out->stages[out->stage_count] = strdup(tok);
        if (!out->stages[out->stage_count]) return 0;
        out->stage_count++;
        tok = strtok_r(NULL, "|", &save);
    }
    return out->stage_count >= 5 && out->stage_count <= 20;
}

static char *take_tab(char **cursor) {
    char *start, *tab;
    if (!cursor || !*cursor) return NULL;
    start = *cursor;
    tab = strchr(start, '\t');
    if (tab) { *tab = '\0'; *cursor = tab + 1; }
    else *cursor = NULL;
    return start;
}

int bf_stage_chains_load(const char *path, BfStageChains *out) {
    FILE *f;
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    int first = 1;
    if (!path || !out) return 0;
    memset(out, 0, sizeof(*out));
    f = fopen(path, "r");
    if (!f) return 0;
    while ((n = getline(&line, &cap, f)) >= 0) {
        char *id, *territories, *intents, *stages;
        char *p = line;
        BfStageChain item = {0};
        if (n && line[n - 1] == '\n') line[--n] = '\0';
        if (first) { first = 0; continue; }
        id = take_tab(&p);
        territories = take_tab(&p);
        intents = take_tab(&p);
        stages = p;
        (void)territories;
        (void)intents;
        if (!id || !*id || !stages || !*stages) goto fail;
        item.id = strdup(id);
        if (!item.id || !split_stages(stages, &item)) { free_chain(&item); goto fail; }
        BfStageChain *next = realloc(out->items, (out->count + 1) * sizeof(*next));
        if (!next) { free_chain(&item); goto fail; }
        out->items = next;
        out->items[out->count++] = item;
    }
    free(line);
    fclose(f);
    return out->count > 0;
fail:
    free(line);
    fclose(f);
    bf_stage_chains_free(out);
    return 0;
}

const BfStageChain *bf_stage_chain_find(const BfStageChains *chains, const char *id) {
    if (!chains || !id) return NULL;
    for (size_t i = 0; i < chains->count; ++i)
        if (!strcmp(chains->items[i].id, id)) return &chains->items[i];
    return NULL;
}

void bf_stage_chains_free(BfStageChains *chains) {
    if (!chains) return;
    for (size_t i = 0; i < chains->count; ++i) free_chain(&chains->items[i]);
    free(chains->items);
    memset(chains, 0, sizeof(*chains));
}
