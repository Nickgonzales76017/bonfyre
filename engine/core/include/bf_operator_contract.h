#ifndef BF_OPERATOR_CONTRACT_H
#define BF_OPERATOR_CONTRACT_H

#include <stddef.h>
#include <stdio.h>
#include <limits.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct sqlite3 sqlite3;

#define BF_CONTRACT_ID_MAX 96
#define BF_CONTRACT_TEMPLATE_MAX 1024
#define BF_CONTRACT_ARGV_MAX 32
#define BF_CONTRACT_ARGUMENT_MAX 32

typedef struct BfContractArgument {
    const char *name;
    const char *value;
} BfContractArgument;

typedef struct BfOperatorContract {
    char id[BF_CONTRACT_ID_MAX];
    char invocation_kind[32];
    char argv_template[BF_CONTRACT_TEMPLATE_MAX];
    char input_binding[64];
    char output_discovery[128];
    char environment_allowlist[256];
    char working_directory_policy[64];
    unsigned timeout_seconds;
    size_t output_limit_bytes;
    char retry_policy[64];
    char workload_probe[64];
    char quality_probe[64];
    char generation[65];
} BfOperatorContract;

typedef struct BfContractExpansion {
    char *argv[BF_CONTRACT_ARGV_MAX];
    size_t argc;
} BfContractExpansion;

typedef struct BfContractContext {
    const char *binary;
    const char *input_path;
    const char *input_dir;
    const char *input_uri;
    const char *mission_id;
    const char *mission_dir;
    const char *output_dir;
    const char *state_dir;
    const BfContractArgument *arguments;
    size_t argument_count;
} BfContractContext;

typedef struct BfOperatorContractBinding {
    char operator_id[160];
    char contract_id[BF_CONTRACT_ID_MAX];
    char generation[65];
    char family[96];
    char argument_defaults[1024];
    char workload_fixture[PATH_MAX];
    char quality_probe_override[64];
} BfOperatorContractBinding;

int bf_operator_contract_validate(const BfOperatorContract *contract, char *error, size_t error_size);
int bf_operator_contract_expand(const BfOperatorContract *contract, const BfContractContext *context, BfContractExpansion *expanded, char *error, size_t error_size);
void bf_operator_contract_free_expansion(BfContractExpansion *expanded);
int bf_operator_contracts_compile(sqlite3 *db, const char *contracts_path,
                                  const char *generation, FILE *error_stream);
int bf_operator_contract_bindings_compile(sqlite3 *db, const char *bindings_path,
                                          const char *generation, FILE *error_stream);
int bf_operator_contract_load(sqlite3 *db, const char *operator_id,
                              const char *active_generation,
                              BfOperatorContract *contract,
                              BfOperatorContractBinding *binding,
                              FILE *error_stream);

#endif
