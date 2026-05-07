import {
  executeIntegration,
  executeStateMachineBatch,
  executeSurfaceBatch,
  listIntegrationTargets,
} from "../../src/integration-engine.mjs";
import {
  ingestIntegrationBatch,
  ingestIntegrationTarget,
  validateIntegrationEvent,
} from "../../src/integration-connectors.mjs";
import {
  archiveStateObject,
  createAttachmentArtifact,
  createStateSnapshot,
  createStateTransition,
  createTrajectoryProjection,
  forkStateObject,
  mergeStateObjects,
  mutateStateObject,
  reopenStateObject,
} from "./state-runtime.mjs";
import {
  buildBranchModel,
  buildReplaySlices,
  buildTrajectoryCalculus,
  compressBranches,
  runContinuityQuery,
} from "./trajectory-calculus.mjs";
import { createTransition, createTransitionSequence } from "./transition-runtime.mjs";
import { createTrajectory } from "./trajectory-runtime.mjs";
import { createContinuityClaim, createFunctionalClaim, createInvoiceBackingClaim } from "./claim-runtime.mjs";
import { getChartFamily, getChartRegistry, listChartFamilies } from "./chart-registry.mjs";
import { getPolicyFamily, getPolicyRegistry, listPolicyFamilies } from "./policy-registry.mjs";
import { getCanonicalSchema, getCanonicalSchemaRegistry, listCanonicalSchemas } from "./schema-registry.mjs";

export const CONTINUITY_CORE_VERSION = "aurekai.continuity_core.v2";

export function listKernelTargets(repoRoot, options = {}) {
  return listIntegrationTargets(repoRoot, options);
}

export function runKernelIntegration(repoRoot, input) {
  return executeIntegration(repoRoot, input);
}

export function runKernelSurfaceBatch(repoRoot, input) {
  return executeSurfaceBatch(repoRoot, input);
}

export function runKernelStateMachineBatch(repoRoot, input) {
  return executeStateMachineBatch(repoRoot, input);
}

export async function ingestKernelIntegration(repoRoot, input) {
  return ingestIntegrationTarget(repoRoot, input);
}

export async function ingestKernelBatch(repoRoot, input) {
  return ingestIntegrationBatch(repoRoot, input);
}

export async function evaluateFunctionalClaims(repoRoot, input) {
  return validateIntegrationEvent(repoRoot, input);
}

export const stateRuntime = {
  createStateSnapshot,
  createStateTransition,
  createAttachmentArtifact,
  createTrajectoryProjection,
  mutateStateObject,
  forkStateObject,
  mergeStateObjects,
  archiveStateObject,
  reopenStateObject,
};

export const trajectoryCalculus = {
  buildTrajectoryCalculus,
  buildReplaySlices,
  buildBranchModel,
  compressBranches,
  runContinuityQuery,
};

export const transitionRuntime = {
  createTransition,
  createTransitionSequence,
};

export const trajectoryRuntime = {
  createTrajectory,
};

export const claimRuntime = {
  createFunctionalClaim,
  createContinuityClaim,
  createInvoiceBackingClaim,
};

export const policyRegistry = {
  listPolicyFamilies,
  getPolicyFamily,
  getPolicyRegistry,
};

export const chartRegistry = {
  listChartFamilies,
  getChartFamily,
  getChartRegistry,
};

export const schemaRegistry = {
  listCanonicalSchemas,
  getCanonicalSchema,
  getCanonicalSchemaRegistry,
};

export {
  archiveStateObject,
  createAttachmentArtifact,
  createStateSnapshot,
  createStateTransition,
  createTrajectoryProjection,
  forkStateObject,
  mergeStateObjects,
  mutateStateObject,
  reopenStateObject,
  buildBranchModel,
  buildReplaySlices,
  buildTrajectoryCalculus,
  compressBranches,
  runContinuityQuery,
  createTransition,
  createTransitionSequence,
  createTrajectory,
  createFunctionalClaim,
  createContinuityClaim,
  createInvoiceBackingClaim,
  listPolicyFamilies,
  getPolicyFamily,
  getPolicyRegistry,
  listChartFamilies,
  getChartFamily,
  getChartRegistry,
  listCanonicalSchemas,
  getCanonicalSchema,
  getCanonicalSchemaRegistry,
};
