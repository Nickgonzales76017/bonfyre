#include "protocol_derivation_bridge.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int present(const char *value)
{
    return value != NULL && value[0] != '\0';
}

static BfProtocolDerivationBridgeStatus from_derivation_status(BfDerivationStatus status)
{
    if (status == BF_DERIVATION_OK) return BF_PROTOCOL_DERIVATION_BRIDGE_OK;
    if (status == BF_DERIVATION_CAPACITY) return BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY;
    return BF_PROTOCOL_DERIVATION_BRIDGE_DERIVATION;
}

const char *bf_protocol_derivation_bridge_status_name(
    BfProtocolDerivationBridgeStatus status)
{
    switch (status) {
    case BF_PROTOCOL_DERIVATION_BRIDGE_OK: return "ok";
    case BF_PROTOCOL_DERIVATION_BRIDGE_INVALID: return "invalid";
    case BF_PROTOCOL_DERIVATION_BRIDGE_NONCONFORMANT: return "nonconformant";
    case BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY: return "capacity";
    case BF_PROTOCOL_DERIVATION_BRIDGE_DERIVATION: return "derivation";
    }
    return "unknown";
}

static BfProtocolDerivationBridgeStatus scoped_refs(
    const BfProtocolContract *contract,
    uint64_t mask,
    const char *enactment_id,
    char storage[BF_DERIVATION_REF_MAX_PER_PROGRAM][BF_DERIVATION_REF_MAX],
    const char *refs[BF_DERIVATION_REF_MAX_PER_PROGRAM],
    size_t *out_count)
{
    size_t count = 0;
    if (contract == NULL || !present(enactment_id) || out_count == NULL) {
        return BF_PROTOCOL_DERIVATION_BRIDGE_INVALID;
    }
    for (size_t parameter = 0; parameter < contract->parameter_count; parameter++) {
        int length;
        if ((mask & (UINT64_C(1) << parameter)) == 0) continue;
        if (count == BF_DERIVATION_REF_MAX_PER_PROGRAM) {
            return BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY;
        }
        length = snprintf(storage[count], BF_DERIVATION_REF_MAX, "%s/%s",
                          enactment_id, contract->parameters[parameter]);
        if (length < 0 || length >= BF_DERIVATION_REF_MAX) {
            return BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY;
        }
        refs[count] = storage[count];
        count++;
    }
    *out_count = count;
    return BF_PROTOCOL_DERIVATION_BRIDGE_OK;
}

BfProtocolDerivationBridgeStatus bf_protocol_derivation_register(
    const BfProtocolContract *contract,
    const char *enactment_id,
    BfSemanticDerivationGraph *graph,
    size_t *out_added)
{
    BfProtocolConformance conformance;
    BfSemanticDerivationGraph *staged;
    BfProtocolStatus protocol_status;

    if (out_added != NULL) *out_added = 0;
    if (contract == NULL || !present(contract->name) || !present(enactment_id) ||
        graph == NULL || out_added == NULL) {
        return BF_PROTOCOL_DERIVATION_BRIDGE_INVALID;
    }
    protocol_status = bf_protocol_contract_conformance(
        contract, NULL, BF_AUDIENCE_PUBLIC, &conformance);
    if (protocol_status != BF_PROTOCOL_OK) {
        return protocol_status == BF_PROTOCOL_CAPACITY
            ? BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY
            : BF_PROTOCOL_DERIVATION_BRIDGE_INVALID;
    }
    if (conformance.liveness_roles || conformance.safety_parameters ||
        conformance.integrity_roles) {
        return BF_PROTOCOL_DERIVATION_BRIDGE_NONCONFORMANT;
    }
    if (contract->role_count > BF_DERIVATION_PROGRAM_MAX - graph->program_count) {
        return BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY;
    }

    staged = malloc(sizeof(*staged));
    if (staged == NULL) return BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY;
    *staged = *graph;

    for (size_t role_index = 0; role_index < contract->role_count; role_index++) {
        const BfProtocolRole *role = &contract->roles[role_index];
        char program_id[BF_DERIVATION_ID_MAX];
        char operator_name[BF_DERIVATION_ID_MAX];
        char read_storage[BF_DERIVATION_REF_MAX_PER_PROGRAM][BF_DERIVATION_REF_MAX];
        char write_storage[BF_DERIVATION_REF_MAX_PER_PROGRAM][BF_DERIVATION_REF_MAX];
        const char *reads[BF_DERIVATION_REF_MAX_PER_PROGRAM];
        const char *writes[BF_DERIVATION_REF_MAX_PER_PROGRAM];
        size_t read_count = 0;
        size_t write_count = 0;
        BfSemanticProgramSpec spec = {0};
        BfProtocolDerivationBridgeStatus bridge_status;
        BfDerivationStatus derivation_status;

        if (snprintf(program_id, sizeof(program_id), "%s:%s",
                     enactment_id, role->name) >= (int)sizeof(program_id) ||
            snprintf(operator_name, sizeof(operator_name), "%s/%s",
                     contract->name, role->name) >= (int)sizeof(operator_name)) {
            free(staged);
            return BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY;
        }

        bridge_status = scoped_refs(
            contract, role->reads, enactment_id, read_storage, reads, &read_count);
        if (bridge_status != BF_PROTOCOL_DERIVATION_BRIDGE_OK) {
            free(staged);
            return bridge_status;
        }
        bridge_status = scoped_refs(
            contract, role->produces, enactment_id, write_storage, writes, &write_count);
        if (bridge_status != BF_PROTOCOL_DERIVATION_BRIDGE_OK) {
            free(staged);
            return bridge_status;
        }

        spec.program_id = program_id;
        spec.operator_name = operator_name;
        spec.reads = reads;
        spec.read_count = read_count;
        spec.writes = writes;
        spec.write_count = write_count;
        derivation_status = bf_semantic_derivation_add(staged, &spec);
        if (derivation_status != BF_DERIVATION_OK) {
            bridge_status = from_derivation_status(derivation_status);
            free(staged);
            return bridge_status;
        }
    }

    *graph = *staged;
    *out_added = contract->role_count;
    free(staged);
    return BF_PROTOCOL_DERIVATION_BRIDGE_OK;
}
