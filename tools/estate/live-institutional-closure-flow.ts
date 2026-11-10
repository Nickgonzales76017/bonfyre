import { runScenario } from "./live-scenario-runner";

const seed = Date.now();
const demandRoot = `closure_${seed}`;

const output = await runScenario("institutional-closure-flow", [
  {
    name: "pack registry",
    path: "/api/runtime/pack-registry"
  },
  {
    name: "capability demand create",
    method: "POST",
    path: "/api/capability/demands",
    body: {
      capability_family: "payment_route_lms",
      effect_class: "external_apply",
      institution_id: "food_sharing",
      actor_id: "operator_review",
      protocol_id: "food_sharing_protocol",
      frame_id: "payment_route_frame",
      root_correlation_id: demandRoot,
      recursion_depth_class: "object_level"
    }
  }
]);

const demandId = expectString(
  output.steps.find((step) => step.name === "capability demand create")?.body,
  "demand_id"
);

const resolution = await runScenario("institutional-closure-flow-resolution", [
  {
    name: "capability demand detail",
    path: `/api/capability/demands/${demandId}`
  },
  {
    name: "capability demand resolve",
    method: "POST",
    path: `/api/capability/demands/${demandId}/resolve`
  }
]);

const selectedExecution = (resolution.steps.find((step) => step.name === "capability demand resolve")?.body as
  | {
      selected_execution?: {
        selected_execution_id?: string;
        provider_actor_id?: string;
        provider_kind?: string;
        effect_class?: string;
        review_required?: boolean;
        notification_required?: boolean;
      };
    }
  | undefined)?.selected_execution;

if (!selectedExecution?.selected_execution_id || selectedExecution.provider_actor_id !== "payment_route_lms") {
  throw new Error("expected provider auction to select payment_route_lms");
}
if (selectedExecution.review_required !== true || selectedExecution.notification_required !== true) {
  throw new Error("expected selected execution to require both review and notification");
}

const selectedExecutionId = selectedExecution.selected_execution_id;
const providerActorId = selectedExecution.provider_actor_id ?? "payment_route_lms";
const providerKind = selectedExecution.provider_kind ?? "CommercialRouteBid";
const effectClass = selectedExecution.effect_class ?? "external_apply";

const executionAndCost = await runScenario("institutional-closure-flow-execution-cost", [
  {
    name: "execution lane resolve",
    method: "POST",
    path: "/api/execution/resolve-lane",
    body: {
      selected_execution_id: selectedExecutionId,
      capability_family: "payment_route_lms",
      provider_actor_id: providerActorId,
      provider_kind: providerKind,
      effect_class: effectClass,
      boundary_kind: "commercial_external"
    }
  },
  {
    name: "cost envelope check",
    method: "POST",
    path: "/api/cost/envelopes/check",
    body: {
      selected_execution_id: selectedExecutionId,
      lane_id: "lane_payment_external_apply",
      provider_actor_id: providerActorId,
      capability_family: "payment_route_lms",
      actor_id: "operator_review",
      institution_id: "food_sharing",
      effect_class: effectClass,
      chargeability: "capacity_lease_drawdown",
      expected_cost: 18,
      max_cost: 22,
      idempotency_key: `idem_${seed}`,
      retry_policy_ref: "retry_same_idempotency"
    }
  }
]);

const laneBody = executionAndCost.steps.find((step) => step.name === "execution lane resolve")?.body as
  | { ok?: boolean; lane?: { lane_id?: string; lane_kind?: string; execution_authority?: string; receipt_contract_ref?: string } }
  | undefined;
if (!laneBody?.ok || laneBody.lane?.lane_id !== "lane_payment_external_apply") {
  throw new Error("expected commercial route lane resolution for external apply");
}

const costBody = executionAndCost.steps.find((step) => step.name === "cost envelope check")?.body as
  | { allowed?: boolean; review_required?: boolean; cost_envelope_id?: string; required_review_kind?: string }
  | undefined;
if (costBody?.allowed !== true || costBody.review_required !== true || costBody.required_review_kind !== "external_apply_review_required" || !costBody.cost_envelope_id) {
  throw new Error("expected cost envelope to stay review-gated for external apply");
}

const approvalRun = await runScenario("institutional-closure-flow-approval", [
  {
    name: "approval policy evaluate",
    method: "POST",
    path: "/api/approval/policies/evaluate",
    body: {
      selected_execution_id: selectedExecutionId,
      capability_family: "payment_route_lms",
      effect_class: effectClass,
      lane_id: "lane_payment_external_apply",
      boundary_kind: "commercial_external",
      chargeability: "capacity_lease_drawdown",
      actor_role: "operator_review",
      cost_envelope_id: costBody.cost_envelope_id,
      expected_cost: 18,
      max_cost: 22,
      idempotency_key: `idem_${seed}`,
      ledger_authority_precedence: false,
      receipt_contract_ref: "apply_receipt_v1"
    }
  }
]);

const approvalBody = approvalRun.steps.find((step) => step.name === "approval policy evaluate")?.body as
  | {
      status?: string;
      review_required?: boolean;
      required_review_kind?: string;
      review_id?: string;
      decision_id?: string;
      approval_occurrence_id?: string;
    }
  | undefined;
if (approvalBody?.status !== "review_required" || approvalBody.review_required !== true || !approvalBody.review_id) {
  throw new Error("expected approval policy to create a review interrupt");
}

const reviewId = approvalBody.review_id;
const notificationDemand = await runScenario("institutional-closure-flow-notification", [
  {
    name: "notification demand create",
    method: "POST",
    path: "/api/notifications/demands",
    body: {
      triggering_event_ref: approvalBody.approval_occurrence_id ?? approvalBody.decision_id ?? selectedExecutionId,
      recipient_actor_id: "operator_review",
      institution_id: "food_sharing",
      urgency: "time_sensitive",
      content_payload_ref: "approval_review_required",
      privacy_class: "internal",
      interaction_required: "approve_reject",
      delivery_confirmation_required: true,
      correlation_id: reviewId,
      causation_id: approvalBody.approval_occurrence_id ?? "",
      review_id: reviewId
    }
  }
]);

const notificationDemandId = expectString(
  notificationDemand.steps.find((step) => step.name === "notification demand create")?.body,
  "notification_demand_id"
);

const notificationFlow = await runScenario("institutional-closure-flow-notification-resolution", [
  {
    name: "notification demand resolve",
    method: "POST",
    path: `/api/notifications/demands/${notificationDemandId}/resolve`
  },
  {
    name: "notification send fixture",
    method: "POST",
    path: `/api/notifications/demands/${notificationDemandId}/send-fixture`
  }
]);

const notificationResolve = notificationFlow.steps.find((step) => step.name === "notification demand resolve")?.body as
  | { selected_provider?: { channel?: string; supports_two_way_interaction?: boolean } }
  | undefined;
if (!notificationResolve?.selected_provider || notificationResolve.selected_provider.supports_two_way_interaction !== true) {
  throw new Error("expected notification provider with two-way interaction");
}

const notificationSend = notificationFlow.steps.find((step) => step.name === "notification send fixture")?.body as
  | { status?: string; attempt_id?: string; delivery_state?: string }
  | undefined;
if (notificationSend?.status !== "ok" || notificationSend.delivery_state !== "sent" || !notificationSend.attempt_id) {
  throw new Error("expected notification fixture to create a sent delivery attempt");
}

const attemptId = notificationSend.attempt_id;
const completion = await runScenario("institutional-closure-flow-completion", [
  {
    name: "notification delivered",
    method: "POST",
    path: `/api/notifications/delivery/${attemptId}/mark-state`,
    body: {
      delivery_state: "delivered"
    }
  },
  {
    name: "notification approve action",
    method: "POST",
    path: `/api/notifications/delivery/${attemptId}/action`,
    body: {
      action: "approve",
      review_id: reviewId,
      approval_allowed: true
    }
  },
  {
    name: "notification recent",
    path: "/api/notifications/recent"
  },
  {
    name: "capability auction recent",
    path: "/api/capability/auction/recent"
  },
  {
    name: "operator overview",
    path: "/api/operator/overview"
  },
  {
    name: "durable events",
    path: "/api/runtime/durable/events"
  },
  {
    name: "desk shell",
    path: "/app/bonfyre-native/desk"
  }
]);

const reviewAction = completion.steps.find((step) => step.name === "notification approve action")?.body as
  | {
      status?: string;
      review_action?: {
        status?: string;
        review_id?: string;
        typed_commit_occurrence_id?: string;
        institution_memory_updated?: boolean;
        bfp_delta_emitted?: boolean;
      };
    }
  | undefined;
if (reviewAction?.status !== "ok" || reviewAction.review_action?.status !== "committed") {
  throw new Error("expected notification action to commit the reviewed path");
}
if (!reviewAction.review_action?.typed_commit_occurrence_id || reviewAction.review_action.institution_memory_updated !== true) {
  throw new Error("expected review action to emit typed commit and memory update");
}

const overview = completion.steps.find((step) => step.name === "operator overview")?.body as
  | {
      approval?: { decision_count?: number };
      auction?: { selected_execution_count?: number };
      notifications?: { attempt_count?: number; actioned_count?: number };
      reviews?: { recent_approved_commits?: Array<{ review_id?: string }> };
    }
  | undefined;
// The counts go in the message. This failed for months reading "expected
// operator overview to show notification attempts and actioned state" while
// attempt_count was 156 -- the sentence pointed at the half that was fine, and
// the half that was zero (nobody has ever actioned a notification) had to be
// dug out of a live endpoint to be seen at all.
const approvals = overview?.approval?.decision_count ?? 0;
const executions = overview?.auction?.selected_execution_count ?? 0;
if (approvals < 1 || executions < 1) {
  throw new Error(
    `expected operator overview to show approval and selected execution data; ` +
    `approval.decision_count=${approvals}, auction.selected_execution_count=${executions}`
  );
}
const attempts = overview?.notifications?.attempt_count ?? 0;
const actioned = overview?.notifications?.actioned_count ?? 0;
if (attempts < 1 || actioned < 1) {
  throw new Error(
    `expected operator overview to show notification attempts and actioned state; ` +
    `notifications.attempt_count=${attempts}, notifications.actioned_count=${actioned}` +
    (attempts > 0 && actioned === 0
      ? ` -- notifications are being sent and none has ever been actioned, which is `
        + `institutional state rather than a defect in this flow`
      : "")
  );
}
if (!(overview?.reviews?.recent_approved_commits ?? []).some((item) => item.review_id === reviewId)) {
  throw new Error("expected committed review to appear in operator overview");
}

const durableEvents = completion.steps.find((step) => step.name === "durable events")?.body as
  | { items?: Array<{ event_kind?: string }> }
  | { recent_events?: Array<{ event_kind?: string }> }
  | undefined;
const durableKinds = [
  ...((durableEvents as { items?: Array<{ event_kind?: string }> })?.items ?? []).map((item) => item.event_kind ?? ""),
  ...((durableEvents as { recent_events?: Array<{ event_kind?: string }> })?.recent_events ?? []).map((item) => item.event_kind ?? "")
];
for (const requiredKind of [
  "capability_demand_created",
  "capability_execution_selected",
  "approval_review_required",
  "notification_demand_created",
  "notification_provider_selected",
  "notification_delivery_attempt_created",
  "notification_action_received",
  "review_approved",
  "typed_commit_applied"
]) {
  if (!durableKinds.includes(requiredKind)) {
    throw new Error(`expected durable event kind ${requiredKind}`);
  }
}

const finalOutput = {
  demand_id: demandId,
  selected_execution_id: selectedExecutionId,
  cost_envelope_id: costBody.cost_envelope_id,
  review_id: reviewId,
  notification_demand_id: notificationDemandId,
  notification_attempt_id: attemptId,
  typed_commit_occurrence_id: reviewAction.review_action?.typed_commit_occurrence_id
};

console.log(JSON.stringify(finalOutput, null, 2));

function expectString(value: unknown, key: string) {
  if (!value || typeof value !== "object") {
    throw new Error(`expected object with ${key}`);
  }
  const candidate = (value as Record<string, unknown>)[key];
  if (typeof candidate !== "string" || candidate.length === 0) {
    throw new Error(`expected non-empty string for ${key}`);
  }
  return candidate;
}
