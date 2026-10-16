import {createSceneWorkbench} from '../../../shared/interaction-98777b6f3a622c96dbfe/scene-workbench.mjs';
export {studioCSS} from './product-scene-style.mjs';

/** Domain binding for the existing product viewer and room editor. The shared
 * scene component cannot infer a SKU, a footprint, a price or a placement unit.
 */
export function createStudioStage(params) {
  const {root,objects,sceneObjects,readState,onSelect,onMove,canEdit}=params;
  const planner=root.closest('[data-planner]');
  const historyActions=['undo','redo'].flatMap(action=>{
    const source=planner?.querySelector(`[data-ui="${action}"]`);
    return source?[{
      label:action==='undo'?'Undo':'Redo',enabled:()=>!source.disabled,run:()=>source.click(),
      subscribe(fn){const observer=new MutationObserver(fn);observer.observe(source,{attributes:true,attributeFilter:['disabled']});return ()=>observer.disconnect();}
    }]:[];
  });
  return createSceneWorkbench({
    ...params,historyActions,onSelect,
    words:{complete:'Show complete product',whole:'Complete product',notice:'Inspection only. Source geometry and saleable options stay unchanged.'},
    objectKey:mesh=>mesh.userData.fixtureKey,
    formatDimension:(length,axis,basis)=>(length*100).toFixed(1)+' cm · '+basis+' '+axis,
    decorateScene(){for(const n of objects.children){if(n.isMesh&&n.geometry?.type==='PlaneGeometry')n.material.color.setHex(0xd0d0bf);if(n.isLineSegments)n.visible=false;}},
    readView(){
      const s=readState(),spatial=s.mode==='room',item=s.plan?.items.find(i=>i.key===s.selectedKey);
      return {spatial,selection:spatial?sceneObjects.get(s.selectedKey):s.currentMesh,
        selectionKey:s.selectedKey,pickObjects:spatial?[...sceneObjects.values()]:[s.currentMesh].filter(Boolean),
        ground:spatial&&s.plan?{width:s.plan.width*.3048,depth:s.plan.depth*.3048,cell:.3048,section:.3048*4}:null,
        selectionLabel:item?`Selected fixture · ${item.x.toFixed(2)}, ${item.y.toFixed(2)} ft · ${item.rotation}°`:''};
    },
    manipulation:{
      allowed:()=>readState().mode==='room'&&!readState().moveMode&&canEdit(),
      capture:()=>structuredClone(readState().plan),
      preview(key,position,delta){const step=.3048/4;return {x:position.x+Math.round(delta.x/step)*step,z:position.z+Math.round(delta.z/step)*step};},
      commit:(key,position,object,base)=>onMove(key,position,object.userData.size,base)
    }
  });
}
