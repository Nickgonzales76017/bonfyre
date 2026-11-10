#define _POSIX_C_SOURCE 200809L
#include "match.h"
#include "stage_vocabulary.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

static int strings_has(const BfStageStrings *s, const char *value) {
    for (size_t i = 0; i < s->count; ++i) if (!strcmp(s->items[i], value)) return 1;
    return 0;
}

static int strings_add(BfStageStrings *s, const char *value) {
    char **next;
    if (!value || !*value || strings_has(s, value)) return 1;
    next = realloc(s->items, (s->count + 1) * sizeof(*next));
    if (!next) return 0;
    s->items = next;
    s->items[s->count] = strdup(value);
    if (!s->items[s->count]) return 0;
    s->count++;
    return 1;
}

void bf_stage_strings_free(BfStageStrings *values) {
    if (!values) return;
    for (size_t i = 0; i < values->count; ++i) free(values->items[i]);
    free(values->items);
    values->items = NULL;
    values->count = 0;
}

static char *lowerdup(const char *value) {
    size_t n = strlen(value);
    char *out = malloc(n + 1);
    if (!out) return NULL;
    for (size_t i = 0; i < n; ++i) out[i] = (char)tolower((unsigned char)value[i]);
    out[n] = '\0';
    return out;
}

static int add_words(BfStageStrings *out, const char *words) {
    char *copy = strdup(words ? words : "");
    char *save = NULL;
    char *tok;
    if (!copy) return 0;
    tok = strtok_r(copy, " \t\r\n", &save);
    while (tok) {
        if (!strings_add(out, tok)) { free(copy); return 0; }
        tok = strtok_r(NULL, " \t\r\n", &save);
    }
    free(copy);
    return 1;
}

static const char *hint_terms(const BfStageHint *rows, size_t count, const char *key) {
    for (size_t i = 0; i < count; ++i) if (!strcmp(rows[i].key, key)) return rows[i].terms;
    return NULL;
}

static int stage_parts_intersect(const char *stage_lower, const BfStageStrings *terms) {
    char *copy = strdup(stage_lower);
    char *p;
    int hit = 0;
    if (!copy) return 0;
    for (p = copy; *p; ++p) if (!isalnum((unsigned char)*p)) *p = ' ';
    char *save = NULL;
    for (char *tok = strtok_r(copy, " ", &save); tok && !hit; tok = strtok_r(NULL, " ", &save))
        hit = strings_has(terms, tok);
    free(copy);
    if (hit) return 1;
    for (size_t i = 0; i < terms->count; ++i)
        if (strlen(terms->items[i]) >= 4 && strstr(stage_lower, terms->items[i])) return 1;
    return 0;
}

int bf_stage_select(const BfStageChain *chain,
                    const char *const *hints, size_t hint_count,
                    BfStageStrings *out) {
    BfStageStrings terms = {0};
    if (!chain || !out) return 0;
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < hint_count; ++i) {
        const char *a = hint_terms(bf_stage_affordance_hints, BF_STAGE_AFFORDANCE_HINT_COUNT, hints[i]);
        const char *r = hint_terms(bf_stage_role_hints, BF_STAGE_ROLE_HINT_COUNT, hints[i]);
        char *lower = lowerdup(hints[i]);
        if ((a && !add_words(&terms, a)) || (r && !add_words(&terms, r)) ||
            (lower && !strings_add(&terms, lower))) {
            free(lower);
            bf_stage_strings_free(&terms);
            return 0;
        }
        free(lower);
    }
    for (size_t i = 0; i < chain->stage_count && out->count < 8; ++i) {
        char *lower = lowerdup(chain->stages[i]);
        int hit = lower && stage_parts_intersect(lower, &terms);
        free(lower);
        if (hit && !strings_add(out, chain->stages[i])) {
            bf_stage_strings_free(&terms);
            bf_stage_strings_free(out);
            return 0;
        }
    }
    bf_stage_strings_free(&terms);
    return 1;
}

int bf_stage_select_presentation(const BfStageChain *chain, BfStageStrings *out) {
    BfStageStrings terms = {0};
    if (!chain || !out || !add_words(&terms, bf_stage_presentation_terms)) return 0;
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < chain->stage_count && out->count < 8; ++i) {
        char *lower = lowerdup(chain->stages[i]);
        int hit = 0;
        if (!lower) { bf_stage_strings_free(&terms); return 0; }
        for (size_t j = 0; j < terms.count && !hit; ++j) hit = strstr(lower, terms.items[j]) != NULL;
        free(lower);
        if (hit && !strings_add(out, chain->stages[i])) {
            bf_stage_strings_free(&terms);
            bf_stage_strings_free(out);
            return 0;
        }
    }
    bf_stage_strings_free(&terms);
    return 1;
}

static int name_has_any(const char *lower_name, const char *words) {
    char *copy = strdup(words ? words : "");
    char *save = NULL;
    char *tok;
    int found = 0;
    if (!copy) return 0;
    tok = strtok_r(copy, " \t\r\n", &save);
    while (tok && !found) {
        found = strstr(lower_name, tok) != NULL;
        tok = strtok_r(NULL, " \t\r\n", &save);
    }
    free(copy);
    return found;
}

int bf_stage_app_hints(const char *name, BfStageStrings *out) {
    char *lower;
    if (!name || !out) return 0;
    memset(out, 0, sizeof(*out));
    lower = lowerdup(name);
    if (!lower) return 0;
    for (size_t i = 0; i < BF_STAGE_AFFORDANCE_HINT_COUNT; ++i)
        if (name_has_any(lower, bf_stage_affordance_hints[i].terms) &&
            !strings_add(out, bf_stage_affordance_hints[i].key)) goto fail;
    for (size_t i = 0; i < BF_STAGE_NAME_RULE_COUNT; ++i) {
        if (!name_has_any(lower, bf_stage_name_rules[i].contains)) continue;
        for (size_t j = 0; j < bf_stage_name_rules[i].hint_count; ++j)
            if (!strings_add(out, bf_stage_name_rules[i].hints[j])) goto fail;
    }
    if (!out->count && !strings_add(out, "OBSERVE")) goto fail;
    free(lower);
    return 1;
fail:
    free(lower);
    bf_stage_strings_free(out);
    return 0;
}
