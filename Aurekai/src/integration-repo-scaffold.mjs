import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { listIntegrationTargets } from "./integration-engine.mjs";

function normalizeKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function ensureDirectory(path) {
  if (!existsSync(path)) mkdirSync(path, { recursive: true });
}

function unique(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function toRepoName(slug) {
  return `aurekai-integration-${slug}`;
}

function toPackageName(slug) {
  return `@aurekai/integration-${slug}`;
}

function groupTargets(repoRoot, options = {}) {
  const rows = listIntegrationTargets(repoRoot, options);
  const grouped = new Map();

  for (const row of rows) {
    const key = normalizeKey(row.target);
    if (!grouped.has(key)) {
      grouped.set(key, {
        target: row.target,
        slug: key,
        surfaces: [],
        categories: [],
        chart_families: [],
        policy_families: [],
        runtime_depths: [],
        state_machine_types: [],
      });
    }

    const current = grouped.get(key);
    current.surfaces.push(row.surface);
    current.categories.push(row.category);
    current.chart_families.push(row.chart_family || "generic");
    current.policy_families.push(
      row.surface === "soft-fork"
        ? "workflow"
        : String(row.chart_family || "generic").includes("oracle")
          ? "oracle"
          : String(row.chart_family || "generic").includes("bridge")
            ? "settlement"
            : String(row.chart_family || "generic").includes("availability")
              ? "availability"
              : String(row.chart_family || "generic").includes("collateral")
                ? "collateral"
                : String(row.chart_family || "generic").includes("liquidity")
                  ? "liquidity"
                  : "finality",
    );
    current.runtime_depths.push(row.runtime_depth);
    current.state_machine_types.push(...(row.state_machine_types || []));
  }

  return Array.from(grouped.values())
    .map((item) => ({
      ...item,
      surfaces: unique(item.surfaces),
      categories: unique(item.categories),
      chart_families: unique(item.chart_families),
      policy_families: unique(item.policy_families),
      runtime_depths: unique(item.runtime_depths),
      state_machine_types: unique(item.state_machine_types),
    }))
    .sort((a, b) => a.target.localeCompare(b.target));
}

function packageJsonForTarget(targetInfo) {
  return {
    name: toPackageName(targetInfo.slug),
    version: "0.1.0-alpha.1",
    description: `Standalone Aurekai integration scaffold for ${targetInfo.target}.`,
    type: "module",
    private: false,
    license: "MIT",
    repository: {
      type: "git",
      url: `git+https://github.com/aurekai/${toRepoName(targetInfo.slug)}.git`,
    },
    keywords: [
      "aurekai",
      "akai",
      "integration",
      targetInfo.slug,
      ...targetInfo.surfaces,
    ],
    publishConfig: {
      access: "public",
    },
    dependencies: {
      "@aurekai/continuity-core": "^0.1.0-alpha.0",
    },
    engines: {
      node: ">=18",
    },
  };
}

function manifestForTarget(targetInfo) {
  const ingressDepth = targetInfo.surfaces.includes("hard-fork")
    ? "sovereign_fork"
    : targetInfo.surfaces.includes("state-machine")
      ? "coprocessor"
      : targetInfo.surfaces.includes("soft-fork")
        ? "shim"
        : targetInfo.surfaces.includes("observer")
          ? "observer"
          : "adapter";

  return {
    schema_version: "aurekai.integration_repo_manifest.v1",
    target: targetInfo.target,
    slug: targetInfo.slug,
    repo_name: toRepoName(targetInfo.slug),
    package_name: toPackageName(targetInfo.slug),
    surfaces: targetInfo.surfaces,
    categories: targetInfo.categories,
    chart_families: targetInfo.chart_families,
    policy_families: targetInfo.policy_families,
    runtime_depths: targetInfo.runtime_depths,
    state_machine_types: targetInfo.state_machine_types,
    ingress_depth: ingressDepth,
    required_kernel_layers: [
      "representation",
      "commitment",
      "trajectory",
      "policy",
      "lineage",
      "transport",
      "claims",
    ],
    must_map_into: [
      "committed_state",
      "transitions",
      "policies",
      "lineage_edges",
      "export_projections",
      "functional_claims",
    ],
    scaffold_notes: [
      "Thin adapters must project into Aurekai kernel semantics only.",
      "Hard-fork targets should emit protocol-native committed state and transition witnesses.",
      "This scaffold is repo-ready and intended for extraction into its own repository when needed.",
    ],
  };
}

function readmeForTarget(targetInfo) {
  const lines = [
    `# ${targetInfo.target}`,
    "",
    `Standalone Aurekai integration scaffold for ${targetInfo.target}.`,
    "",
    "## Target Profile",
    "",
    `- Slug: ${targetInfo.slug}`,
    `- Repo name: ${toRepoName(targetInfo.slug)}`,
    `- Package name: ${toPackageName(targetInfo.slug)}`,
    `- Surfaces: ${targetInfo.surfaces.join(", ")}`,
    `- Categories: ${targetInfo.categories.join(", ")}`,
    `- Runtime depths: ${targetInfo.runtime_depths.join(", ")}`,
    `- State-machine types: ${targetInfo.state_machine_types.length ? targetInfo.state_machine_types.join(", ") : "none"}`,
    "",
    "## Kernel Mapping Rule",
    "",
    "This integration must map host semantics into Aurekai kernel objects only:",
    "",
    "- committed_state",
    "- transitions",
    "- policies",
    "- lineage_edges",
    "- export_projections",
    "- functional_claims",
    "",
    "## Kernel Dependency",
    "",
    "This scaffold expects @aurekai/continuity-core as the substrate contract.",
    "Chart and policy declarations should map into kernel registries.",
    "",
    "## Suggested Extraction",
    "",
    "- Keep parser presets and endpoint templates aligned with the runtime registry.",
    "- Preserve continuity policy semantics from the main Aurekai engine.",
    "- Treat this repo as a publishable integration surface, not a semantic fork of the kernel.",
    "",
    "## Source of Truth",
    "",
    "Generated from the Aurekai integration registry and intended for repo extraction planning.",
    "",
  ];

  return `${lines.join("\n")}`;
}

function writeTargetScaffold(baseDir, targetInfo) {
  const repoDir = join(baseDir, targetInfo.slug);
  ensureDirectory(repoDir);
  ensureDirectory(join(repoDir, "src"));
  ensureDirectory(join(repoDir, "test-vectors"));

  writeFileSync(join(repoDir, "package.json"), `${JSON.stringify(packageJsonForTarget(targetInfo), null, 2)}\n`);
  writeFileSync(join(repoDir, "integration.manifest.json"), `${JSON.stringify(manifestForTarget(targetInfo), null, 2)}\n`);
  writeFileSync(join(repoDir, "README.md"), `${readmeForTarget(targetInfo)}\n`);
  writeFileSync(join(repoDir, "src", "index.mjs"), `import {\n  createStateSnapshot,\n  createTransitionSequence,\n  createTrajectory,\n  createContinuityClaim,\n} from "@aurekai/continuity-core";\n\nexport function mapHostEventToKernel(event) {\n  const committedState = createStateSnapshot({\n    chart_family: ${JSON.stringify(targetInfo.chart_families[0] || "generic")},\n    policy_family: ${JSON.stringify(targetInfo.policy_families[0] || "finality")},\n    payload: { target: ${JSON.stringify(targetInfo.target)}, event },\n  });\n\n  const transitions = createTransitionSequence(["prepare", "commit"], {\n    transition_type: "continuity-projection",\n    predecessor_state_hash: committedState.commitment_ref,\n    metadata: { target: ${JSON.stringify(targetInfo.target)} },\n  });\n\n  const trajectory = createTrajectory({\n    target: ${JSON.stringify(targetInfo.target)},\n    transitions,\n    lineage_edges: [],\n  });\n\n  const claim = createContinuityClaim({\n    target_commitment: committedState.commitment_ref,\n    proof_ref: trajectory.folded_witness,\n    threshold: 0.7,\n    value: Number(event?.deformation_score || 0),\n  });\n\n  return {\n    committed_state: committedState,\n    transitions,\n    trajectory,\n    policies: [],\n    lineage_edges: [],\n    export_projections: [],\n    functional_claims: [claim],\n  };\n}\n`);
  writeFileSync(join(repoDir, "test-vectors", "continuity-pass.json"), `${JSON.stringify({
    schema_version: "aurekai.integration_test_vector.v1",
    target: targetInfo.target,
    case: "continuity-pass",
    input_event: {
      event_id: `${targetInfo.slug}-pass-1`,
      action: "observe",
      risk_score: 0.2,
    },
    expected: {
      policy_decision: "allow",
      functional_claim_result: "pass",
    },
  }, null, 2)}\n`);
  writeFileSync(join(repoDir, "test-vectors", "continuity-fail.json"), `${JSON.stringify({
    schema_version: "aurekai.integration_test_vector.v1",
    target: targetInfo.target,
    case: "continuity-fail",
    input_event: {
      event_id: `${targetInfo.slug}-fail-1`,
      action: "observe",
      risk_score: 0.95,
    },
    expected: {
      policy_decision: "halt",
      functional_claim_result: "fail",
    },
  }, null, 2)}\n`);

  return repoDir;
}

export function scaffoldIntegrationRepos(repoRoot, options = {}) {
  const targets = groupTargets(repoRoot, options);
  const baseDir = options.outDir || join(repoRoot, "generated", "integration-repos");
  ensureDirectory(baseDir);

  const selected = options.target
    ? targets.filter((item) => normalizeKey(item.target) === normalizeKey(options.target))
    : targets;

  if (selected.length === 0) {
    throw new Error(`No integration targets matched for scaffold generation${options.target ? `: ${options.target}` : ""}.`);
  }

  const repos = selected.map((item) => ({
    target: item.target,
    slug: item.slug,
    path: writeTargetScaffold(baseDir, item),
    surfaces: item.surfaces,
    categories: item.categories,
  }));

  const index = {
    schema_version: "aurekai.integration_repo_catalog.v1",
    generated_at: new Date().toISOString(),
    count: repos.length,
    out_dir: baseDir,
    repos,
  };

  writeFileSync(join(baseDir, "catalog.json"), `${JSON.stringify(index, null, 2)}\n`);
  return index;
}