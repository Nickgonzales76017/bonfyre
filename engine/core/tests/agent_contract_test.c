#include "bf_agent_contract.h"

#include <stdio.h>
#include <string.h>

static int failures;
static int checks;

static void expect(BfAgentContractStatus actual, BfAgentContractStatus expected,
                   const char *name)
{
    checks++;
    if (actual == expected) return;
    fprintf(stderr, "FAIL %s: expected %s, got %s\n", name,
            bf_agent_contract_status_name(expected),
            bf_agent_contract_status_name(actual));
    failures++;
}

static BfAgentEvent events[] = {
    {
        .session_id = "agent-session-1", .sequence = 1,
        .kind = BF_AGENT_EVENT_MODEL_TURN,
        .evidence_origin = BF_EVIDENCE_ORIGIN_SUBJECT,
        .work_node = "work:root", .observed_at = "2026-08-21T07:00:00Z",
        .provider_identity = "codex", .model_identity = "gpt-5",
        .provider_session_ref = "provider-session-a",
    },
    {
        .session_id = "agent-session-1", .sequence = 2,
        .kind = BF_AGENT_EVENT_CONTEXT_COMPACTION,
        .evidence_origin = BF_EVIDENCE_ORIGIN_HOST,
        .work_node = "work:root", .observed_at = "2026-08-21T07:01:00Z",
        .context_before_ref = "context:full", .context_after_ref = "context:cut-1",
        .reason_ref = "policy:bounded-context",
    },
    {
        .session_id = "agent-session-1", .sequence = 3,
        .kind = BF_AGENT_EVENT_STRATEGY_CHANGE,
        .evidence_origin = BF_EVIDENCE_ORIGIN_HOST,
        .work_node = "work:root", .observed_at = "2026-08-21T07:02:00Z",
        .strategy_before_ref = "strategy:single",
        .strategy_after_ref = "strategy:review",
        .reason_ref = "observation:provider-divergence",
    },
    {
        .session_id = "agent-session-1", .sequence = 4,
        .kind = BF_AGENT_EVENT_MODEL_TURN,
        .evidence_origin = BF_EVIDENCE_ORIGIN_EXTERNAL,
        .work_node = "work:review", .observed_at = "2026-08-21T07:03:00Z",
        .provider_identity = "openai-external", .model_identity = "review-model",
        .provider_session_ref = "provider-session-b",
    },
};

static BfReceiptEvidence evidence[] = {
    {
        .event_sequence = 1, .evidence_ref = "evidence:turn-1",
        .origin = BF_EVIDENCE_ORIGIN_SUBJECT,
        .artifact_identity = "sha256:turn-1", .producer_identity = "codex",
        .model_identity = "gpt-5", .producer_verifier_ref = "verify:host-turn-1",
        .transport_identity = "carrier:cli",
    },
    {
        .event_sequence = 2, .evidence_ref = "evidence:compaction",
        .origin = BF_EVIDENCE_ORIGIN_HOST,
        .artifact_identity = "sha256:compaction",
        .producer_identity = "context-compiler",
    },
    {
        .event_sequence = 3, .evidence_ref = "evidence:strategy",
        .origin = BF_EVIDENCE_ORIGIN_HOST,
        .artifact_identity = "sha256:strategy", .producer_identity = "agent-session",
    },
    {
        .event_sequence = 4, .evidence_ref = "evidence:external-turn",
        .origin = BF_EVIDENCE_ORIGIN_EXTERNAL,
        .artifact_identity = "sha256:external-turn",
        .producer_identity = "openai-external", .model_identity = "review-model",
        .producer_verifier_ref = "verify:external-receipt-4",
        .transport_identity = "https:lease-7",
    },
};

static BfAgentSession session = {
    .session_id = "agent-session-1", .agent_profile_ref = "profile:reviewer/v1",
    .root_work_node = "work:root", .events = events,
    .event_count = sizeof(events) / sizeof(events[0]),
};

static BfReceiptEnvelope receipt = {
    .identity = "receipt:1", .subject = "work:root",
    .occurrence_ref = "occurrence:terminal", .status = "completed",
    .session_id = "agent-session-1", .first_event_sequence = 1,
    .last_event_sequence = 4, .evidence = evidence,
    .evidence_count = sizeof(evidence) / sizeof(evidence[0]),
};

int main(void)
{
    expect(bf_agent_session_validate(&session), BF_AGENT_CONTRACT_OK,
           "valid heterogeneous session");
    expect(bf_receipt_envelope_validate(&receipt), BF_AGENT_CONTRACT_OK,
           "valid receipt envelope");
    expect(bf_receipt_envelope_covers_session(&receipt, &session),
           BF_AGENT_CONTRACT_OK, "receipt covers exact session frontier");

    /* Provider-native session references and providers may change while the
     * governed AgentSession identity stays constant. */
    checks++;
    if (strcmp(events[0].session_id, events[3].session_id) != 0 ||
        strcmp(events[0].provider_session_ref, events[3].provider_session_ref) == 0) {
        fprintf(stderr, "FAIL provider-independent AgentSession identity\n");
        failures++;
    }

    {
        const char *saved = events[0].model_identity;
        events[0].model_identity = NULL;
        expect(bf_agent_session_validate(&session),
               BF_AGENT_CONTRACT_MISSING_PROVIDER_FIDELITY,
               "model turn requires model identity");
        events[0].model_identity = saved;
    }
    {
        const char *saved = events[1].context_after_ref;
        events[1].context_after_ref = events[1].context_before_ref;
        expect(bf_agent_session_validate(&session),
               BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE,
               "compaction requires changed before and after references");
        events[1].context_after_ref = saved;
    }
    {
        const char *saved = events[2].strategy_after_ref;
        events[2].strategy_after_ref = NULL;
        expect(bf_agent_session_validate(&session),
               BF_AGENT_CONTRACT_INVALID_EVENT_SHAPE,
               "strategy change is a first-class transition");
        events[2].strategy_after_ref = saved;
    }
    {
        BfEvidenceOrigin saved = evidence[0].origin;
        evidence[0].origin = BF_EVIDENCE_ORIGIN_HOST;
        expect(bf_receipt_envelope_covers_session(&receipt, &session),
               BF_AGENT_CONTRACT_ORIGIN_MISMATCH,
               "host evidence cannot satisfy subject evidence");
        evidence[0].origin = saved;
    }
    {
        const char *saved = evidence[0].producer_identity;
        evidence[0].producer_identity = "different-provider";
        expect(bf_receipt_envelope_covers_session(&receipt, &session),
               BF_AGENT_CONTRACT_PRODUCER_MISMATCH,
               "model turn provider substitution is rejected");
        evidence[0].producer_identity = saved;
    }
    {
        const char *saved = evidence[0].producer_verifier_ref;
        evidence[0].producer_verifier_ref = NULL;
        expect(bf_receipt_envelope_covers_session(&receipt, &session),
               BF_AGENT_CONTRACT_MISSING_PROVIDER_FIDELITY,
               "self-attribution is not verified producer fidelity");
        evidence[0].producer_verifier_ref = saved;
    }
    {
        const char *saved = evidence[3].transport_identity;
        evidence[3].transport_identity = NULL;
        expect(bf_receipt_envelope_validate(&receipt), BF_AGENT_CONTRACT_INVALID,
               "external evidence requires transport identity");
        evidence[3].transport_identity = saved;
    }
    {
        const char *saved = evidence[1].transport_identity;
        evidence[1].transport_identity = NULL;
        checks++;
        if (!bf_receipt_evidence_has_identity(
                &evidence[1], BF_EVIDENCE_IDENTITY_ARTIFACT) ||
            !bf_receipt_evidence_has_identity(
                &evidence[1], BF_EVIDENCE_IDENTITY_PRODUCER) ||
            bf_receipt_evidence_has_identity(
                &evidence[1], BF_EVIDENCE_IDENTITY_TRANSPORT)) {
            fprintf(stderr, "FAIL identity dimensions collapsed\n");
            failures++;
        }
        evidence[1].transport_identity = saved;
    }
    {
        uint64_t saved = events[3].sequence;
        events[3].sequence = 5;
        expect(bf_agent_session_validate(&session),
               BF_AGENT_CONTRACT_INVALID_SEQUENCE,
               "event sequence is contiguous and monotonic");
        events[3].sequence = saved;
    }

    printf("%d checks, %d failures\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
