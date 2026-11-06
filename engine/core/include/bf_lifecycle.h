#ifndef BF_LIFECYCLE_H
#define BF_LIFECYCLE_H

#include <stdio.h>

typedef enum BfLifecycleStatus {
    BF_LIFECYCLE_OK = 0,
    BF_LIFECYCLE_NOT_FOUND,
    BF_LIFECYCLE_CONFLICT,
    BF_LIFECYCLE_ILLEGAL_TRANSITION,
    BF_LIFECYCLE_INVALID,
    BF_LIFECYCLE_STORAGE_ERROR
} BfLifecycleStatus;

typedef struct BfLifecycleResult {
    BfLifecycleStatus status;
    char identity[128];
    char state[64];
    char event_id[65];
    char error_message[256];
    int replayed;
} BfLifecycleResult;

typedef struct BfWorkerRunSpec {
    const char *run_id;
    const char *worker_id;
    const char *mission_id;
    const char *protocol_instance_id;
} BfWorkerRunSpec;

typedef struct BfProtocolInstanceSpec {
    const char *instance_id;
    const char *protocol_id;
    const char *worker_run_id;
    const char *initial_receipt_ref;
} BfProtocolInstanceSpec;

int bf_lifecycle_migrate_database(void *sqlite_database, FILE *err);

/* Registers the live Restate -> WorkGraph protocol definition. Registration is
 * replay-safe and conflicts if an existing definition has different states. */
BfLifecycleResult bf_lifecycle_register_restate(void *sqlite_database);

/* Registers the capability fulfillment machine shared by x402/MPP/MCP/A2A
 * and their commerce projections. It records quote and payment presentation
 * only; verification/execution/delivery require a separately bound rail
 * adapter and are intentionally absent from this generic machine. */
BfLifecycleResult bf_lifecycle_register_capability_fulfillment(void *sqlite_database);

/* v3 of the same quote, carried through settlement to delivery. Registered
 * beside v2 rather than over it: instances are bound to their definition, and
 * redefining one in use is refused. Its rail-issued events each carry the
 * adapter's receipt; the machine records verification, it never performs it. */
BfLifecycleResult bf_lifecycle_register_capability_fulfillment_rail(void *sqlite_database);

/* Caller-supplied identities are semantic identities. PIDs, provider invocation
 * IDs and transport request IDs are deliberately not accepted by this API. */
BfLifecycleResult bf_worker_run_start(void *sqlite_database,
                                      const BfWorkerRunSpec *spec);
BfLifecycleResult bf_worker_run_step(void *sqlite_database, const char *run_id,
                                     const char *operation,
                                     const char *idempotency_key,
                                     const char *receipt_ref);
BfLifecycleResult bf_worker_run_status(void *sqlite_database, const char *run_id);

BfLifecycleResult bf_protocol_instance_start(
    void *sqlite_database, const BfProtocolInstanceSpec *spec);
BfLifecycleResult bf_protocol_instance_step(
    void *sqlite_database, const char *instance_id, const char *operation,
    const char *idempotency_key, const char *receipt_ref);
BfLifecycleResult bf_protocol_instance_status(void *sqlite_database,
                                               const char *instance_id);

const char *bf_lifecycle_status_name(BfLifecycleStatus status);

#endif
