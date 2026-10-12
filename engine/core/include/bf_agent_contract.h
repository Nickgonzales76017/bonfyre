#ifndef BF_AGENT_CONTRACT_H
#define BF_AGENT_CONTRACT_H

#include <stddef.h>
#include <stdint.h>

/*
 * Native AgentSession / ReceiptEnvelope fidelity contract.
 *
 * AgentSession is the stable governed identity. Provider-native session IDs are
 * representations carried by individual events and never replace session_id.
 * The event stream makes context compaction, strategy changes, and model turns
 * separately observable. Receipt evidence then binds each event to an explicit
 * subject/host/external origin and keeps artifact, producer/model, and transport
 * identity in different fields.
 */

typedef enum BfAgentEventKind {
    BF_AGENT_EVENT_UNKNOWN = 0,
    BF_AGENT_EVENT_MODEL_TURN,
    BF_AGENT_EVENT_CONTEXT_COMPACTION,
    BF_AGENT_EVENT_STRATEGY_CHANGE
} BfAgentEventKind;

typedef enum BfEvidenceOrigin {
    BF_EVIDENCE_ORIGIN_UNKNOWN = 0,
    BF_EVIDENCE_ORIGIN_SUBJECT,
    BF_EVIDENCE_ORIGIN_HOST,
    BF_EVIDENCE_ORIGIN_EXTERNAL
} BfEvidenceOrigin;

typedef enum BfEvidenceIdentityDimension {
    BF_EVIDENCE_IDENTITY_ARTIFACT = 0,
    BF_EVIDENCE_IDENTITY_PRODUCER,
    BF_EVIDENCE_IDENTITY_MODEL,
    BF_EVIDENCE_IDENTITY_TRANSPORT
} BfEvidenceIdentityDimension;

typedef struct BfAgentEvent {
    const char *session_id;
    uint64_t sequence;
    BfAgentEventKind kind;
    BfEvidenceOrigin evidence_origin;
    const char *work_node;
    const char *observed_at;

    /* Required for every model turn. provider_session_ref remains optional and
     * can change between turns without changing the Bonfyre AgentSession. */
    const char *provider_identity;
    const char *model_identity;
    const char *provider_session_ref;

    /* Required as before/after pairs only for their corresponding event kind. */
    const char *context_before_ref;
    const char *context_after_ref;
    const char *strategy_before_ref;
    const char *strategy_after_ref;
    const char *reason_ref;
} BfAgentEvent;

typedef struct BfAgentSession {
    const char *session_id;
    const char *agent_profile_ref;
    const char *root_work_node;
    const BfAgentEvent *events;
    size_t event_count;
} BfAgentSession;

typedef struct BfReceiptEvidence {
    uint64_t event_sequence;
    const char *evidence_ref;
    BfEvidenceOrigin origin;

    /* Orthogonal identity dimensions. Equality or presence in one dimension
     * never establishes any other dimension. */
    const char *artifact_identity;
    const char *producer_identity;
    const char *model_identity;
    const char *producer_verifier_ref;
    const char *transport_identity;
} BfReceiptEvidence;

typedef struct BfReceiptEnvelope {
    const char *identity;
    const char *subject;
    const char *occurrence_ref;
    const char *status;
    const char *session_id;
    uint64_t first_event_sequence;
    uint64_t last_event_sequence;
    const BfReceiptEvidence *evidence;
    size_t evidence_count;
} BfReceiptEnvelope;

typedef enum BfAgentContractStatus {
    BF_AGENT_CONTRACT_OK = 0,
    BF_AGENT_CONTRACT_INVALID,
    BF_AGENT_CONTRACT_INVALID_SEQUENCE,
    BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE,
    BF_AGENT_CONTRACT_MISSING_PROVIDER_FIDELITY,
    BF_AGENT_CONTRACT_RECEIPT_GAP,
    BF_AGENT_CONTRACT_ORIGIN_MISMATCH,
    BF_AGENT_CONTRACT_PRODUCER_MISMATCH
} BfAgentContractStatus;

BfAgentContractStatus bf_agent_session_validate(const BfAgentSession *session);
BfAgentContractStatus bf_receipt_envelope_validate(const BfReceiptEnvelope *receipt);

/* Validate the composite boundary: the receipt covers the exact session event
 * frontier and every event has same-sequence, same-origin evidence. A model
 * turn additionally requires exact provider and model identity plus an explicit
 * verifier observation; repeated self-attribution is not producer proof. */
BfAgentContractStatus bf_receipt_envelope_covers_session(
    const BfReceiptEnvelope *receipt, const BfAgentSession *session);

int bf_receipt_evidence_has_identity(
    const BfReceiptEvidence *evidence, BfEvidenceIdentityDimension dimension);
const char *bf_agent_event_kind_name(BfAgentEventKind kind);
const char *bf_evidence_origin_name(BfEvidenceOrigin origin);
const char *bf_agent_contract_status_name(BfAgentContractStatus status);

#endif
