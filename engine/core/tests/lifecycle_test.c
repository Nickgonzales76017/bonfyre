#include "bf_lifecycle.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

static void expect(int condition, const char *message) {
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        failures++;
    }
}

static int count_rows(sqlite3 *db, const char *table) {
    sqlite3_stmt *statement = NULL;
    char sql[160];
    int count = -1;
    snprintf(sql, sizeof(sql), "SELECT count(*) FROM %s", table);
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW) {
        count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

int main(void) {
    sqlite3 *db = NULL;
    BfLifecycleResult value;
    BfWorkerRunSpec run = {"run-stable-1", "worker-profile-a", "mission-1", "protocol-1"};
    BfProtocolInstanceSpec protocol = {"protocol-1", "restate-workgraph-v1", "run-stable-1", NULL};

    expect(sqlite3_open(":memory:", &db) == SQLITE_OK, "open in-memory database");
    expect(bf_lifecycle_migrate_database(db, stderr) == 0, "migrate lifecycle schema");
    value = bf_lifecycle_register_restate(db);
    expect(value.status == BF_LIFECYCLE_OK && !value.replayed,
           "register live Restate protocol");
    value = bf_lifecycle_register_restate(db);
    expect(value.status == BF_LIFECYCLE_OK && value.replayed,
           "protocol registration replays without a second identity");
    value = bf_lifecycle_register_capability_fulfillment(db);
    expect(value.status == BF_LIFECYCLE_OK && !value.replayed,
           "register capability fulfillment protocol");
    value = bf_lifecycle_register_capability_fulfillment(db);
    expect(value.status == BF_LIFECYCLE_OK && value.replayed,
           "capability fulfillment registration replays safely");
    {
        BfWorkerRunSpec capability_run = {
            "capability-run-1", "mcp-agent", "capability:parcel-underwrite", "capability-instance-1"
        };
        BfProtocolInstanceSpec capability_instance = {
            "capability-instance-1", "capability-fulfillment-v2", "capability-run-1", "quote:fixture"
        };
        value = bf_worker_run_start(db, &capability_run);
        expect(value.status == BF_LIFECYCLE_OK, "start capability WorkerRun");
        value = bf_protocol_instance_start(db, &capability_instance);
        expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "quoted") == 0,
               "start capability quote instance");
        value = bf_protocol_instance_step(db, "capability-instance-1", "present-payment",
                                          "payment-presented", "payment-claim:sha256");
        expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "payment-presented") == 0,
               "record payment presentation without settlement");
        value = bf_protocol_instance_step(db, "capability-instance-1", "verify-payment",
                                          "forged-verification", "arbitrary-receipt");
        expect(value.status == BF_LIFECYCLE_ILLEGAL_TRANSITION,
               "generic lifecycle refuses forged payment verification");
    }

    value = bf_worker_run_start(db, &run);
    expect(value.status == BF_LIFECYCLE_OK && !value.replayed &&
           strcmp(value.state, "running") == 0, "start stable WorkerRun");
    value = bf_worker_run_start(db, &run);
    expect(value.status == BF_LIFECYCLE_OK && value.replayed,
           "same WorkerRun binding is replay-safe");
    {
        BfWorkerRunSpec collision = {"run-stable-1", "provider-process-999", "mission-2", "protocol-2"};
        value = bf_worker_run_start(db, &collision);
        expect(value.status == BF_LIFECYCLE_CONFLICT,
               "process-shaped collision cannot replace WorkerRun identity");
    }
    expect(count_rows(db, "worker_runs") == 2,
           "one original and one capability WorkerRun exist without duplicates");

    value = bf_protocol_instance_start(db, &protocol);
    expect(value.status == BF_LIFECYCLE_OK && !value.replayed,
           "start ProtocolMachine instance");
    value = bf_protocol_instance_start(db, &protocol);
    expect(value.status == BF_LIFECYCLE_OK && value.replayed,
           "same ProtocolMachine instance is replay-safe");

    value = bf_protocol_instance_step(db, "protocol-1", "invoke", "invoke-once", NULL);
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "running-a") == 0,
           "invoke protocol");
    value = bf_protocol_instance_step(db, "protocol-1", "invoke", "invoke-once", NULL);
    expect(value.status == BF_LIFECYCLE_OK && value.replayed,
           "replayed operation keeps first durable transition");
    value = bf_protocol_instance_step(db, "protocol-1", "resume", "invoke-once", NULL);
    expect(value.status == BF_LIFECYCLE_CONFLICT,
           "idempotency key cannot be rebound to another operation");

    value = bf_protocol_instance_step(db, "protocol-1", "node-a-complete",
                                      "missing-node-a-receipt", NULL);
    expect(value.status == BF_LIFECYCLE_INVALID,
           "acknowledgement transition fails closed without a receipt");

    value = bf_protocol_instance_step(db, "protocol-1", "node-a-complete",
                                      "node-a-once", "receipt:node-a");
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "waiting") == 0,
           "node A acknowledgement reaches durable wait");
    value = bf_worker_run_step(db, "run-stable-1", "wait", "wait-once",
                               "receipt:node-a");
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "waiting") == 0,
           "WorkerRun records durable wait");
    value = bf_protocol_instance_step(db, "protocol-1", "node-b-complete",
                                      "too-early", "receipt:node-b");
    expect(value.status == BF_LIFECYCLE_ILLEGAL_TRANSITION,
           "ProtocolMachine rejects out-of-order node B completion");

    value = bf_protocol_instance_step(db, "protocol-1", "resume",
                                      "missing-recovery-receipt", NULL);
    expect(value.status == BF_LIFECYCLE_INVALID,
           "resume fails closed without an EffectKernel recovery receipt");

    value = bf_protocol_instance_step(db, "protocol-1", "resume", "resume-once",
                                      "effect-recovery:0");
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "running-b") == 0,
           "protocol resumes after recovery fence");
    value = bf_worker_run_step(db, "run-stable-1", "resume", "resume-once",
                               "effect-recovery:0");
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "running") == 0,
           "WorkerRun resumes under same identity");
    value = bf_protocol_instance_step(db, "protocol-1", "node-b-complete",
                                      "node-b-once", "receipt:node-b");
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "completed") == 0,
           "protocol reaches terminal state");
    value = bf_worker_run_step(db, "run-stable-1", "complete",
                               "missing-completion-receipt", NULL);
    expect(value.status == BF_LIFECYCLE_INVALID,
           "WorkerRun cannot report success without a receipt");
    value = bf_worker_run_step(db, "run-stable-1", "complete", "complete-once",
                               "receipt:node-b");
    expect(value.status == BF_LIFECYCLE_OK && strcmp(value.state, "completed") == 0,
           "WorkerRun completes against receipt");

    expect(count_rows(db, "protocol_instances") == 2,
           "one original and one capability ProtocolMachine instance exist");
    expect(count_rows(db, "protocol_instance_transitions") == 5,
           "protocol replay and forged verification create no duplicate transitions");
    expect(count_rows(db, "worker_run_transitions") == 3,
           "WorkerRun has one transition per lifecycle operation");

    sqlite3_close(db);
    if (failures != 0) return 1;
    puts("lifecycle: all checks passed");
    return 0;
}
