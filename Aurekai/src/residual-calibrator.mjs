import { readFileSync } from "node:fs";
import { join } from "node:path";

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp01(value) {
  const n = toNumber(value, 0);
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function readProfiles(repoRoot) {
  const path = join(repoRoot, "registry", "residual-calibration-profiles.json");
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    return {
      defaultProfile: parsed?.default_profile || "generic",
      profiles: Array.isArray(parsed?.profiles) ? parsed.profiles : [],
    };
  } catch {
    return {
      defaultProfile: "generic",
      profiles: [],
    };
  }
}

function readChartFamily(repoRoot, chartFamily) {
  const path = join(repoRoot, "registry", "chart-families.json");
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    const families = Array.isArray(parsed?.families) ? parsed.families : [];
    return families.find((item) => item?.id === chartFamily) || null;
  } catch {
    return null;
  }
}

function pickProfile(profiles, chartFamily, defaultProfile) {
  const byChart = profiles.find((item) => item?.chart_family === chartFamily);
  if (byChart) return byChart;
  const byId = profiles.find((item) => item?.id === defaultProfile);
  if (byId) return byId;
  return {
    id: "generic",
    chart_family: chartFamily || "generic",
    weights: {
      deformation_score: 0.4,
      witness_skew: 0.2,
      continuity_gap: 0.2,
      risk_score: 0.2,
    },
    taxonomy: {
      stable: [0, 0.25],
      drifting: [0.25, 0.55],
      strained: [0.55, 0.8],
      rupture: [0.8, 1],
    },
  };
}

function taxonomyForSeverity(taxonomy, severity) {
  const entries = Object.entries(taxonomy || {});
  for (const [label, range] of entries) {
    if (!Array.isArray(range) || range.length !== 2) continue;
    const min = toNumber(range[0], 0);
    const max = toNumber(range[1], 1);
    if (severity >= min && severity < max) return label;
    if (severity === 1 && max === 1) return label;
  }
  return "unknown";
}

export function calibrateResidual(repoRoot, input) {
  const chartFamily = input.chartFamily || "generic";
  const { profiles, defaultProfile } = readProfiles(repoRoot);
  const profile = pickProfile(profiles, chartFamily, defaultProfile);
  const chart = readChartFamily(repoRoot, chartFamily);

  const raw = {
    deformation_score: clamp01(input.event?.deformation_score),
    witness_skew: clamp01(input.event?.witness_skew),
    continuity_gap: clamp01(input.event?.continuity_gap),
    risk_score: clamp01(input.event?.risk_score),
  };

  const weights = {
    deformation_score: toNumber(profile?.weights?.deformation_score, 0.4),
    witness_skew: toNumber(profile?.weights?.witness_skew, 0.2),
    continuity_gap: toNumber(profile?.weights?.continuity_gap, 0.2),
    risk_score: toNumber(profile?.weights?.risk_score, 0.2),
  };

  const weighted = {
    deformation_score: raw.deformation_score * weights.deformation_score,
    witness_skew: raw.witness_skew * weights.witness_skew,
    continuity_gap: raw.continuity_gap * weights.continuity_gap,
    risk_score: raw.risk_score * weights.risk_score,
  };

  const composite = clamp01(
    weighted.deformation_score +
    weighted.witness_skew +
    weighted.continuity_gap +
    weighted.risk_score,
  );

  const taxonomy = taxonomyForSeverity(profile?.taxonomy, composite);

  return {
    schema_version: "aurekai.residual_calibration_result.v1",
    chart_family: chartFamily,
    profile_id: profile?.id || "generic",
    semantics: chart?.residual_semantics || "generalized deformation",
    raw,
    weights,
    weighted,
    composite_severity: composite,
    taxonomy,
  };
}
