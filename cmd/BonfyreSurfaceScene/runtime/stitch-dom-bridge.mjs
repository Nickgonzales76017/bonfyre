// Opt-in native DOM treatment. This never manufactures markup, records or actions.
// Host routes still use their current server-side native lowerer.
import {SurfaceRuntime} from "./surface-runtime.mjs";
import {composeStitchScene} from "./stitch-composition.mjs";

const cleanHost = root => root?.documentElement || root;
export function mountStitchPresentation(root, scene, options={}) {
  const host=cleanHost(root);
  if (!host?.dataset || typeof host.style?.setProperty !== "function")
    throw new TypeError("Aurekai design needs an existing DOM root");
  const decorate = source => composeStitchScene(source,options);
  const adopt = resolved => {
    const cell=resolved.presentation.at(-1);
    host.dataset.aurekaiPresentation="stitch-v1";
    host.dataset.aurekaiPattern=cell.pattern;
    host.dataset.aurekaiShell=cell.shell;
    host.dataset.aurekaiLook=cell.look;
    const colors=cell.tokens;
    for (const [key,value] of Object.entries(colors)) {
      if (!/^(ground|ink|muted|accent|panel|line|display|body)$/.test(key)) continue;
      host.style.setProperty("--aurekai-"+key,value);
    }
    // Only mark regions that the native owner actually published.
    const regions=new Map((cell.experience_regions||[]).map(region=>[region.id,region]));
    const known=new Set([...resolved.slots||[],...resolved.fragments||[]]
      .map(x=>x.slot_id||x.fragment_id).filter(Boolean));
    for(const node of root?.querySelectorAll?.("[data-bonfyre-region]")||[]) {
      const id=node.getAttribute("data-bonfyre-region");
      const region=regions.get(id);
      if(known.has(id)&&region){
        node.dataset.aurekaiNativeRegion=id;
        node.dataset.aurekaiZone=region.zone;
        node.dataset.aurekaiRole=region.role;
        node.dataset.aurekaiMaterialization=region.materialization||"declared";
      }else{
        for(const key of ["aurekaiNativeRegion","aurekaiZone","aurekaiRole","aurekaiMaterialization"])delete node.dataset[key];
      }
    }
  };
  const initial=decorate(scene);
  const runtime=new SurfaceRuntime(initial,{root});
  const ownerUpdate=runtime.updateScene.bind(runtime);
  // Native scene events and SurfaceDelta must preserve the composed design.
  // Only adopt the presentation after the owner's update passes its gates.
  runtime.updateScene=next=>{
    const prepared=decorate(next), result=ownerUpdate(prepared);
    adopt(prepared);
    return result;
  };
  adopt(initial);
  runtime.bind();
  return {
    runtime,
    update(next){return runtime.updateScene(next);},
    get model(){return runtime.model;},
    get scene(){return runtime.scene;}
  };
}
