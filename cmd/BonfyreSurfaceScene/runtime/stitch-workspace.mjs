// Compose any Stitch family from already installed native application bindings.
// Non-workspace families remain source-led; no preview can grant a new command.
import {compileWorkspace} from "./renderers/workspace-composition.mjs";
import {composeStitchScene} from "./stitch-composition.mjs";
import {STITCH_PATTERNS} from "./stitch-grammar.mjs";
import {compileSceneExperience} from "./experience-programs.mjs";

// All fourteen patterns can compose existing application panes. A page with
// no account-bound applications is NOT given simulated panes or actions.
const accepted = new Set(Object.keys(STITCH_PATTERNS));
const focusShells=new Set(["reading","stream","device","record"]);
const compareShells=new Set(["index","editorial","market","player","immersive"]);
const panelPlan = (bindings,direction,zones) => bindings.map((item,i) => i===0 ?
  {id:item.binding_id,direction:"within"} :
  {id:item.binding_id,direction:direction==="comparison"?(i%2?"right":"below") :
    direction==="desk"?(["wide","hero"].includes(zones.get(item.slot_id))?"below":"right"):"below",
    relative:bindings[i-1].binding_id});

export function composeStitchWorkspace(scene,contributions,options={}) {
  const decorated=composeStitchScene(scene,options);
  const design=decorated.presentation.at(-1);
  const composition=compileSceneExperience(decorated,options);
  if(!accepted.has(design.pattern)) throw new RangeError("reference does not compose a native workspace");
  if(!Array.isArray(decorated.bindings) || decorated.bindings.length===0)
    throw new Error("No installed account-owned application bindings; do not create donor panels");
  if(decorated.bindings.length>128) throw new Error("Workspace binding bound exceeded");
  if(!decorated.interaction || typeof decorated.interaction!=="object")
    throw new Error("Native interaction required");
  const existing=decorated.interaction.desktop_posture;
  if (!Array.isArray(existing?.layouts) || existing.layouts.length===0) {
    const bindings=decorated.bindings;
    const zones=new Map(composition.regions.map(region=>[region.id,region.zone]));
    const layouts=bindings.length===1 ? [
      {id:"aurekai-focus",panels:panelPlan(bindings,"focus",zones)}
    ] : [
      {id:"aurekai-focus",panels:panelPlan(bindings,"focus",zones)},
      {id:"aurekai-desk",panels:panelPlan(bindings,"desk",zones)},
      {id:"aurekai-comparison",panels:panelPlan(bindings,"comparison",zones)}
    ];
    const first=bindings.length===1?"aurekai-focus":
      focusShells.has(design.shell)?"aurekai-focus":
      compareShells.has(design.shell)?"aurekai-comparison":"aurekai-desk";
    decorated.interaction.desktop_posture={...existing,layouts,initial_layout:first};
  }
  // The existing native compiler verifies each binding, application kind,
  // slot, action aperture, owner adapter and enabled command.
  return compileWorkspace(decorated,contributions);
}
