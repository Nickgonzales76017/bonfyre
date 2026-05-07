import { createHash } from "node:crypto";

function stableHash(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeLifecycle(phase) {
  const allowed = new Set(["create", "mutate", "transport", "fork", "merge", "archive", "reopen"]);
  const value = String(phase || "create").trim().toLowerCase();
  if (!allowed.has(value)) {
    throw new Error(`Invalid lifecycle_phase: ${phase}`);
  }
  return value;
}

function normalizeStateKind(stateKind) {
  const allowed = new Set(["state_snapshot", "state_transition", "attachment_artifact", "trajectory_projection"]);
  const value = String(stateKind || "state_snapshot").trim().toLowerCase();
  if (!allowed.has(value)) {
    throw new Error(`Invalid state_kind: ${stateKind}`);
  }
  return value;
}

function makeStateObject(input = {}) {
  const stateKind = normalizeStateKind(input.state_kind);
  const lifecyclePhase = normalizeLifecycle(input.lifecycle_phase);
  const payload = input.payload || {};
  const commitmentRef = input.commitment_ref || stableHash({ stateKind, lifecyclePhase, payload, now: nowIso() });

  return {
    schema_version: "aurekai.state_object.v1",
    state_object_id: input.state_object_id || stableHash({ commitmentRef, lifecyclePhase }).slice(0, 24),
    state_kind: stateKind,
    lifecycle_phase: lifecyclePhase,
    commitment_ref: commitmentRef,
    chart_family: input.chart_family || "generic",
    policy_family: input.policy_family || "finality",
    payload,
  };
}

export function createStateSnapshot(input = {}) {
  return makeStateObject({
    ...input,
    state_kind: "state_snapshot",
    lifecycle_phase: input.lifecycle_phase || "create",
  });
}

export function createStateTransition(input = {}) {
  return makeStateObject({
    ...input,
    state_kind: "state_transition",
    lifecycle_phase: input.lifecycle_phase || "mutate",
  });
}

export function createAttachmentArtifact(input = {}) {
  return makeStateObject({
    ...input,
    state_kind: "attachment_artifact",
    lifecycle_phase: input.lifecycle_phase || "transport",
  });
}

export function createTrajectoryProjection(input = {}) {
  return makeStateObject({
    ...input,
    state_kind: "trajectory_projection",
    lifecycle_phase: input.lifecycle_phase || "merge",
  });
}

export function mutateStateObject(previous, patch = {}, nextLifecycle = "mutate") {
  if (!previous || previous.schema_version !== "aurekai.state_object.v1") {
    throw new Error("mutateStateObject requires a valid aurekai.state_object.v1 object");
  }

  return makeStateObject({
    ...previous,
    ...patch,
    payload: {
      ...(previous.payload || {}),
      ...(patch.payload || {}),
    },
    lifecycle_phase: nextLifecycle,
    commitment_ref: stableHash({
      previous_commitment_ref: previous.commitment_ref,
      next_payload: {
        ...(previous.payload || {}),
        ...(patch.payload || {}),
      },
      lifecycle_phase: nextLifecycle,
    }),
  });
}

export function forkStateObject(source, forkPatch = {}) {
  return mutateStateObject(source, forkPatch, "fork");
}

export function mergeStateObjects(base, incoming, mergePayload = {}) {
  if (!base || !incoming) {
    throw new Error("mergeStateObjects requires both base and incoming state objects");
  }

  return makeStateObject({
    ...base,
    lifecycle_phase: "merge",
    payload: {
      ...(base.payload || {}),
      ...(incoming.payload || {}),
      ...mergePayload,
    },
    commitment_ref: stableHash({
      base: base.commitment_ref,
      incoming: incoming.commitment_ref,
      merged: mergePayload,
    }),
  });
}

export function archiveStateObject(source, archivePayload = {}) {
  return mutateStateObject(source, { payload: archivePayload }, "archive");
}

export function reopenStateObject(source, reopenPayload = {}) {
  return mutateStateObject(source, { payload: reopenPayload }, "reopen");
}
