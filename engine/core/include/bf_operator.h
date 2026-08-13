#ifndef BF_OPERATOR_H
#define BF_OPERATOR_H

#include <limits.h>
#include <stddef.h>
#include <sys/types.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/*
 * Bounded native-process contract used by the Fabric Operator ABI.  Callers
 * supply an already validated argument vector; this module never interprets
 * shell syntax or performs PATH lookup.
 */
typedef struct BfProcessRequest {
    const char *executable;
    const char *const *argv;
    const char *working_directory;
    const char *stdout_path;
    const char *stderr_path;
    const char *const *environment;
    unsigned timeout_seconds;
    size_t output_limit_bytes;
} BfProcessRequest;

typedef struct BfProcessResult {
    int exit_code;
    int terminating_signal;
    int timed_out;
    long duration_ms;
    long cpu_user_ms;
    long cpu_system_ms;
    size_t stdout_bytes;
    size_t stderr_bytes;
    char workload_result[32];
    char quality_result[32];
    char contract_id[96];
    char contract_generation[65];
    char contract_family[96];
    char raw_output_uri[256];
    char normalized_output_uri[256];
} BfProcessResult;

/* Returns zero only for a successfully exited process within its bounds. */
int bf_process_operator_run(const BfProcessRequest *request,
                            BfProcessResult *result,
                            char *error,
                            size_t error_size);

#endif
