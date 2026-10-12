#include "bf_agent_contract.h"

#include <string.h>

static int present(const char *value)
{
    return value != NULL && value[0] != '\0';
}

static int changed(const char *before, const char *after)
{
    return present(before) && present(after) && strcmp(before, after) != 0;
}

static int valid_origin(BfEvidenceOrigin origin)
{
    return origin == BF_EVIDENCE_ORIGIN_SUBJECT ||
           origin == BF_EVIDENCE_ORIGIN_HOST ||
           origin == BF_EVIDENCE_ORIGIN_EXTERNAL;
}

static BfAgentContractStatus validate_event(const BfAgentEvent *event)
{
    if (event == NULL || !present(event->session_id) || event->sequence == 0 ||
        !present(event->work_node) || !present(event->observed_at) ||
        !valid_origin(event->evidence_origin)) {
        return BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE;
    }

    switch (event->kind) {
    case BF_AGENT_EVENT_MODEL_TURN:
        if (!present(event->provider_identity) || !present(event->model_identity)) {
            return BF_AGENT_CONTRACT_MISSING_PROVIDER_FIDELITY;
        }
        return BF_AGENT_CONTRACT_OK;
    case BF_AGENT_EVENT_CONTEXT_COMPACTION:
        if (!changed(event->context_before_ref, event->context_after_ref) ||
            !present(event->reason_ref)) {
            return BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE;
        }
        return BF_AGENT_CONTRACT_OK;
    case BF_AGENT_EVENT_STRATEGY_CHANGE:
        if (!changed(event->strategy_before_ref, event->strategy_after_ref) ||
            !present(event->reason_ref)) {
            return BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE;
        }
        return BF_AGENT_CONTRACT_OK;
    case BF_AGENT_EVENT_UNKNOWN:
        break;
    }
    return BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE;
}

BfAgentContractStatus bf_agent_session_validate(const BfAgentSession *session)
{
    if (session == NULL || !present(session->session_id) ||
        !present(session->agent_profile_ref) || !present(session->root_work_node) ||
        session->events == NULL || session->event_count == 0) {
        return BF_AGENT_CONTRACT_INVALID;
    }

    uint64_t previous = 0;
    for (size_t index = 0; index < session->event_count; index++) {
        const BfAgentEvent *event = &session->events[index];
        BfAgentContractStatus status = validate_event(event);
        if (status != BF_AGENT_CONTRACT_OK) return status;
        if (strcmp(event->session_id, session->session_id) != 0) {
            return BF_AGENT_CONTRACT_INVALID;
        }
        if (event->sequence != previous + 1) {
            return BF_AGENT_CONTRACT_INVALID_SEQUENCE;
        }
        previous = event->sequence;
    }
    return BF_AGENT_CONTRACT_OK;
}

int bf_receipt_evidence_has_identity(
    const BfReceiptEvidence *evidence, BfEvidenceIdentityDimension dimension)
{
    if (evidence == NULL) return 0;
    switch (dimension) {
    case BF_EVIDENCE_IDENTITY_ARTIFACT:
        return present(evidence->artifact_identity);
    case BF_EVIDENCE_IDENTITY_PRODUCER:
        return present(evidence->producer_identity);
    case BF_EVIDENCE_IDENTITY_MODEL:
        return present(evidence->model_identity);
    case BF_EVIDENCE_IDENTITY_TRANSPORT:
        return present(evidence->transport_identity);
    }
    return 0;
}

BfAgentContractStatus bf_receipt_envelope_validate(const BfReceiptEnvelope *receipt)
{
    if (receipt == NULL || !present(receipt->identity) || !present(receipt->subject) ||
        !present(receipt->occurrence_ref) || !present(receipt->status) ||
        !present(receipt->session_id) || receipt->first_event_sequence == 0 ||
        receipt->last_event_sequence < receipt->first_event_sequence ||
        receipt->evidence == NULL || receipt->evidence_count == 0) {
        return BF_AGENT_CONTRACT_INVALID;
    }

    for (size_t index = 0; index < receipt->evidence_count; index++) {
        const BfReceiptEvidence *evidence = &receipt->evidence[index];
        if (evidence->event_sequence < receipt->first_event_sequence ||
            evidence->event_sequence > receipt->last_event_sequence ||
            !present(evidence->evidence_ref) || !valid_origin(evidence->origin) ||
            !bf_receipt_evidence_has_identity(
                evidence, BF_EVIDENCE_IDENTITY_ARTIFACT) ||
            !bf_receipt_evidence_has_identity(
                evidence, BF_EVIDENCE_IDENTITY_PRODUCER)) {
            return BF_AGENT_CONTRACT_INVALID;
        }
        if (evidence->origin == BF_EVIDENCE_ORIGIN_EXTERNAL &&
            !bf_receipt_evidence_has_identity(
                evidence, BF_EVIDENCE_IDENTITY_TRANSPORT)) {
            return BF_AGENT_CONTRACT_INVALID;
        }
    }
    return BF_AGENT_CONTRACT_OK;
}

BfAgentContractStatus bf_receipt_envelope_covers_session(
    const BfReceiptEnvelope *receipt, const BfAgentSession *session)
{
    BfAgentContractStatus status = bf_agent_session_validate(session);
    if (status != BF_AGENT_CONTRACT_OK) return status;
    status = bf_receipt_envelope_validate(receipt);
    if (status != BF_AGENT_CONTRACT_OK) return status;

    if (strcmp(receipt->session_id, session->session_id) != 0 ||
        receipt->first_event_sequence != session->events[0].sequence ||
        receipt->last_event_sequence !=
            session->events[session->event_count - 1].sequence) {
        return BF_AGENT_CONTRACT_RECEIPT_GAP;
    }

    for (size_t event_index = 0; event_index < session->event_count; event_index++) {
        const BfAgentEvent *event = &session->events[event_index];
        int found_sequence = 0;
        int found_origin = 0;
        int found_provider = 0;
        for (size_t evidence_index = 0;
             evidence_index < receipt->evidence_count; evidence_index++) {
            const BfReceiptEvidence *evidence = &receipt->evidence[evidence_index];
            if (evidence->event_sequence != event->sequence) continue;
            found_sequence = 1;
            if (evidence->origin != event->evidence_origin) continue;
            found_origin = 1;
            if (event->kind != BF_AGENT_EVENT_MODEL_TURN ||
                (strcmp(evidence->producer_identity, event->provider_identity) == 0 &&
                 present(evidence->model_identity) &&
                 strcmp(evidence->model_identity, event->model_identity) == 0 &&
                 present(evidence->producer_verifier_ref))) {
                found_provider = 1;
                break;
            }
        }
        if (!found_sequence) return BF_AGENT_CONTRACT_RECEIPT_GAP;
        if (!found_origin) return BF_AGENT_CONTRACT_ORIGIN_MISMATCH;
        if (!found_provider) {
            if (event->kind == BF_AGENT_EVENT_MODEL_TURN) {
                for (size_t evidence_index = 0;
                     evidence_index < receipt->evidence_count; evidence_index++) {
                    const BfReceiptEvidence *evidence =
                        &receipt->evidence[evidence_index];
                    if (evidence->event_sequence == event->sequence &&
                        evidence->origin == event->evidence_origin &&
                        strcmp(evidence->producer_identity,
                               event->provider_identity) == 0 &&
                        present(evidence->model_identity) &&
                        strcmp(evidence->model_identity,
                               event->model_identity) == 0 &&
                        !present(evidence->producer_verifier_ref)) {
                        return BF_AGENT_CONTRACT_MISSING_PROVIDER_FIDELITY;
                    }
                }
            }
            return BF_AGENT_CONTRACT_PRODUCER_MISMATCH;
        }
    }
    return BF_AGENT_CONTRACT_OK;
}

const char *bf_agent_event_kind_name(BfAgentEventKind kind)
{
    switch (kind) {
    case BF_AGENT_EVENT_MODEL_TURN: return "model_turn";
    case BF_AGENT_EVENT_CONTEXT_COMPACTION: return "context_compaction";
    case BF_AGENT_EVENT_STRATEGY_CHANGE: return "strategy_change";
    case BF_AGENT_EVENT_UNKNOWN: break;
    }
    return "unknown";
}

const char *bf_evidence_origin_name(BfEvidenceOrigin origin)
{
    switch (origin) {
    case BF_EVIDENCE_ORIGIN_SUBJECT: return "subject";
    case BF_EVIDENCE_ORIGIN_HOST: return "host";
    case BF_EVIDENCE_ORIGIN_EXTERNAL: return "external";
    case BF_EVIDENCE_ORIGIN_UNKNOWN: break;
    }
    return "unknown";
}

const char *bf_agent_contract_status_name(BfAgentContractStatus status)
{
    switch (status) {
    case BF_AGENT_CONTRACT_OK: return "ok";
    case BF_AGENT_CONTRACT_INVALID: return "invalid";
    case BF_AGENT_CONTRACT_INVALID_SEQUENCE: return "invalid_sequence";
    case BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE: return "invalid_event_shape";
    case BF_AGENT_CONTRACT_MISSING_PROVIDER_FIDELITY:
        return "missing_provider_fidelity";
    case BF_AGENT_CONTRACT_RECEIPT_GAP: return "receipt_gap";
    case BF_AGENT_CONTRACT_ORIGIN_MISMATCH: return "origin_mismatch";
    case BF_AGENT_CONTRACT_PRODUCER_MISMATCH: return "producer_mismatch";
    }
    return "unknown";
}
