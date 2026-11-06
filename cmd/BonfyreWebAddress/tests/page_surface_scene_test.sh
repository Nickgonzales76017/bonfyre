#!/bin/sh
set -eu

root="$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)"
web="${1:-$root/cmd/BonfyreWebAddress/bonfyre-web-address}"
runtime="$root/cmd/BonfyreSurfaceScene/runtime/surface-runtime.mjs"
asset="$root/site/matrix/bonfyre-surface-runtime.mjs"
source="$root/cmd/BonfyreWebAddress/src"

[ -x "$web" ]
cmp -s "$runtime" "$asset"
cmp -s "$root/cmd/BonfyreSurfaceScene/runtime/renderers/workspace-composition.mjs" "$root/site/matrix/bonfyre-workspace-composition.mjs"
grep -Fq "'./renderers/workspace-composition.mjs'" "$asset"
grep -RFq '"/assets/renderers/workspace-composition.mjs", "bonfyre-workspace-composition.mjs"' "$source"
grep -RFq 'id=\"bonfyre-surface-scene\"' "$source"
grep -Rq 'bonfyre-surface-scene.v1' "$source"
grep -RFq 'data-bonfyre-region=\"page-main\"' "$source"
grep -RFq 'data-bonfyre-continuity=\"page-main\"' "$source"
grep -Rq 'compat:page-composition' "$source"
grep -Rq 'complete, readable server document' "$source"
! grep -Rq "new SurfaceRuntime(scene,{root:document}).bind()" "$source"
! grep -Rq 'serve_composed_public_route' "$source"

# The projected canonical runtime accepts exactly the compatibility envelope.
# PageComposition does not bind it; an owned ApplicationBinding must do that.
node --input-type=module - "$runtime" <<'NODE'
import {pathToFileURL} from 'node:url';
const {composeSurfaceRuntime, SurfaceRuntime} = await import(pathToFileURL(process.argv[2]));
const scene = {
  schema:'bonfyre-surface-scene.v1', source_digest:'sha256:test',
  focus:{subject_address:'route:life:/discover',world:'',site_id:'',actor_scope:'public',frontier:'',intent:'read'},
  lenses:['READ'], world_material:{compatibility_adapter:'PageComposition',initial_markup:'server',complete_without_script:true},
  slots:[{slot_id:'page-main',extent:'page',accepts:['ComposedPublicPage'],max_items:1,htmx_boundary:''}],
  fragments:[{fragment_id:'page-region-1',surface_grammar:'READ',renderer_family:'PageComposition',renderer_variant:'lead',extent:'section',continuity_group:'page:lead:0',source_projection:'',media_posture:'',temporal_posture:'',action_aperture:''}],
  bindings:[],stages:[],renderers:[],patterns:[],presentation:[],
  operations:{apertures:[],object_contracts:[],proposal_contracts:[]},
  interaction:{profile_id:'page-composition-compatibility',default_lens:'READ',lens_order:['READ'],semantic_zoom:[{resolution:0,name:'page'},{resolution:1,name:'region'}],interaction_depths:['focus'],context_rail:{},relation_halo:{enabled:false},command_aperture:{},transition_policy:{object_permanence:true,view_transition:'auto'},mobile_posture:{},desktop_posture:{},live_update_policy:{mode:'SurfaceDelta',transport:'document'}},
  runtime_queries:{compatibility_adapter:'PageComposition',route:'/discover'},
  dependencies:['compat:page-composition','compat:route:/discover'],
  cannot_infer:'compatibility Scene is derived interaction state only'
};
const model = composeSurfaceRuntime(scene);
if (model.schema !== 'bonfyre-surface-runtime.v1') throw new Error('runtime schema');
for (const plane of ['actual','possible','presence','material']) if (!model.planes[plane]) throw new Error('missing '+plane);
if (!model.regions.some(x=>x.id==='page-main') || !model.regions.some(x=>x.id==='page-region-1')) throw new Error('regions');
const runtime = new SurfaceRuntime(scene,{root:null});
if (runtime.patchRegion('not-declared','<p>x</p>') !== false) throw new Error('undeclared patch accepted');
console.log('page_surface_scene_test: PASS');
NODE
