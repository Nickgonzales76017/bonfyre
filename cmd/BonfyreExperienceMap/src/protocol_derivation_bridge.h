#ifndef BONFYRE_EXPERIENCE_PROTOCOL_DERIVATION_BRIDGE_H
#define BONFYRE_EXPERIENCE_PROTOCOL_DERIVATION_BRIDGE_H

#include "bf_protocol_contract.h"
#include "bf_semantic_derivation.h"

typedef enum BfProtocolDerivationBridgeStatus {
    BF_PROTOCOL_DERIVATION_BRIDGE_OK = 0,
    BF_PROTOCOL_DERIVATION_BRIDGE_INVALID,
    BF_PROTOCOL_DERIVATION_BRIDGE_NONCONFORMANT,
    BF_PROTOCOL_DERIVATION_BRIDGE_CAPACITY,
    BF_PROTOCOL_DERIVATION_BRIDGE_DERIVATION
} BfProtocolDerivationBridgeStatus;

BfProtocolDerivationBridgeStatus bf_protocol_derivation_register(
    const BfProtocolContract *contract,
    const char *enactment_id,
    BfSemanticDerivationGraph *graph,
    size_t *out_added);

const char *bf_protocol_derivation_bridge_status_name(
    BfProtocolDerivationBridgeStatus status);

#endif
