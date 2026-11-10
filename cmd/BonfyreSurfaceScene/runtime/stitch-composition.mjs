// Presentation-only reference application to an already-compiled native Scene.
// The authoritative SurfaceScene continues to supply source, subject, material,
// application bindings, action apertures, and release generation.
import {composeSurfaceRuntime} from "./surface-runtime.mjs";
import {STITCH_LOOKS,STITCH_PATTERNS,STITCH_REFERENCES} from "./stitch-grammar.mjs";
import {compileSceneExperience} from "./experience-programs.mjs";

const topologies = new Set(["lead","shelf","field","map","index","tracks","thread","list","feed","wall","grid","communities","table","results","agenda"]);
const array = x => Array.isArray(x) ? x : [];
const object = x => x && typeof x === "object" && !Array.isArray(x) ? x : {};
const own = (o,k) => Object.prototype.hasOwnProperty.call(o,k);

/**
 * Adds an Aurekai presentation vocabulary to a real native SurfaceScene.
 * No route, actor, dependency, fragment, pattern, binding, stage, query,
 * effect, action or digest is added or changed. The reference is non-authoritative.
 */
export function composeStitchScene(scene, {reference, pattern, look="editorial"}={}) {
  if (!scene || scene.schema !== "bonfyre-surface-scene.v1" ||
      !scene.source_digest || !object(scene.focus).subject_address) {
    throw new TypeError("requires resolved, digested native SurfaceScene");
  }
  if (reference !== undefined && !own(STITCH_REFERENCES,reference)) throw new RangeError("unknown Stitch reference");
  if (reference !== undefined && pattern !== undefined && STITCH_REFERENCES[reference] !== pattern)
    throw new RangeError("reference pattern mismatch");
  const selected = pattern ?? STITCH_REFERENCES[reference];
  if (!own(STITCH_PATTERNS,selected)) throw new RangeError("unknown composition pattern");
  if (!own(STITCH_LOOKS,look)) throw new RangeError("unknown Aurekai design language");

  const spec = STITCH_PATTERNS[selected];
  const regions = [
    ...array(scene.slots).map(x => ({id:x.slot_id, topology:"field"})),
    ...array(scene.fragments).map(x => ({id:x.fragment_id, topology: topologies.has(x.renderer_variant) ? x.renderer_variant : topologies.has(x.renderer_family) ? x.renderer_family : "field"}))
  ].filter(x => typeof x.id === "string" && x.id);
  const seen = new Set();
  const ordered = regions.filter(x => {
    if (seen.has(x.id)) return false;
    seen.add(x.id);
    return true;
  }).sort((a,b) => {
    const ai = spec.rhythm.indexOf(a.topology), bi=spec.rhythm.indexOf(b.topology);
    return (ai < 0 ? 100 : ai) - (bi < 0 ? 100 : bi);
  });
  // This derived order is a visual hint only; semantic slots remain in owner order.
  const result = structuredClone(scene);
  const experience=compileSceneExperience(scene,{reference,pattern:selected});
  result.presentation = [
    ...array(result.presentation).filter(x => x?.kind !== "aurekai-stitch-composition.v1"),
    {kind:"aurekai-stitch-composition.v1", reference:reference || "", pattern:selected,
     shell:spec.shell, posture:spec.posture, density:spec.density, look,
     tokens:STITCH_LOOKS[look], region_order:ordered.map(x=>x.id),
     experience_schema:experience.schema,
     experience_zones:experience.zones,
     experience_modes:experience.modes,
     experience_regions:experience.regions.map(({id,role,zone,owner,extent,sourceProjection})=>({id,role,zone,owner,extent,sourceProjection})),
     preferred_topologies:[...spec.rhythm], design_source:"Google Stitch 2026-11-04",
     warning:"presentation only; no source, identity, record, authority or effect"}
  ];
  return result;
}

/** Reuse the actual native SurfaceRuntime projection, never render Stitch HTML. */
export function composeStitchRuntime(scene, options, ephemeral={}) {
  return composeSurfaceRuntime(composeStitchScene(scene,options),ephemeral);
}
