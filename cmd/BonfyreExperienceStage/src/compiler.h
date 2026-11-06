#ifndef BONFYRE_EXPERIENCE_STAGE_COMPILER_H
#define BONFYRE_EXPERIENCE_STAGE_COMPILER_H

typedef struct {
    const char *chains_tsv;
    const char *external_bindings;
    const char *external_raw;
    const char *ghost_bindings;
    const char *app_bindings;
    const char *output_jsonl;
    const char *manifest_json;
} BfStageCompileOptions;

int bf_stage_compile(const BfStageCompileOptions *options);

#endif
