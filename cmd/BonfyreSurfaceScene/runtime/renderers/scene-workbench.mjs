/** Inspect and manipulate an owner-supplied scene without owning its models.
 * The binding supplies selection, world-unit geometry, gesture validity, history
 * and effect commitment. Camera, part inspection and annotations remain reusable.
 */
export function createSceneWorkbench({engine,renderer,scene,camera,controls,root,viewport,objects,
 readView,objectKey,manipulation,historyActions=[],decorateScene=()=>{},formatDimension,
 words={complete:'Show complete object',whole:'Complete object',notice:'Inspection does not change source geometry.'},
 onSelect=()=>{},invalidate,say}) {
 if(typeof readView!=='function'||typeof objectKey!=='function'||typeof formatDimension!=='function')throw Error('A scene requires its selection, identity and measurement binding.');
 const {THREE:T,Grid,ViewHelper,EffectComposer,RenderPass,OutlinePass,OutputPass,CSS2DRenderer,CSS2DObject}=engine;
 const ui=(tag,text,cls)=>{const n=document.createElement(tag);if(text)n.textContent=text;if(cls)n.className=cls;return n;};
 const button=(label,fn)=>{const n=ui('button',label);n.type='button';n.addEventListener('click',fn);return n;};
 const tools=ui('div','','ps-stage-tools'),labels=new CSS2DRenderer(),annotations=new T.Group(),ground=new T.Group();
 labels.domElement.className='ps-label-layer';Object.assign(labels.domElement.style,{position:'absolute',inset:'0',pointerEvents:'none'});viewport.append(labels.domElement);scene.add(annotations,ground);
 const composer=new EffectComposer(renderer),outline=new OutlinePass(new T.Vector2(1,1),scene,camera);
 outline.edgeStrength=3;outline.edgeThickness=1;outline.visibleEdgeColor.set('#d28a4f');outline.hiddenEdgeColor.set('#7e6958');
 composer.addPass(new RenderPass(scene,camera));composer.addPass(outline);composer.addPass(new OutputPass());
 const view=new ViewHelper(camera,viewport);
 let dimensions=false,partsOpen=false,activePart=null,partRoot=null,parts=[],exploded=0,drag=null,pointer=null,gizmoPointer=false,context='',lastSize='',disposed=false,renders=0;
 const positions=new Map();
 const dims=button('Dimensions',()=>{dimensions=!dimensions;sync();});
 const partToggle=button('Parts',()=>{partsOpen=!partsOpen;panel.hidden=!partsOpen;partToggle.setAttribute('aria-pressed',String(partsOpen));});
 const lighting=ui('select');lighting.setAttribute('aria-label','Studio lighting');for(const [id,label] of [['studio','Studio softbox'],['daylight','Daylight'],['evening','Warm interior']])lighting.add(new Option(label,id));
 lighting.addEventListener('change',()=>setLighting(lighting.value));
 const expand=button('Expand workspace',()=>{root.classList.toggle('ps-expanded');expand.textContent=root.classList.contains('ps-expanded')?'Close expanded view':'Expand workspace';resize();invalidate();});
 const zoomOut=button('−',()=>{controls.dolly(-.35,true);invalidate();}),zoomIn=button('+',()=>{controls.dolly(.35,true);invalidate();});zoomOut.setAttribute('aria-label','Zoom out');zoomIn.setAttribute('aria-label','Zoom in');
 const historyButtons=historyActions.map(action=>{const control=button(action.label,()=>{if(action.enabled())action.run();});tools.append(control);return [action,control];});
 const syncHistory=()=>{for(const [action,control]of historyButtons)control.disabled=!action.enabled();};
 const historySubscriptions=historyActions.map(action=>action.subscribe?.(syncHistory)).filter(Boolean);syncHistory();
 tools.append(dims,partToggle,lighting,zoomOut,zoomIn,expand);root.querySelector('.ps-toolbar').after(tools);
 const panel=ui('aside','','ps-part-panel');panel.hidden=true;
 const partSelect=ui('select'),summary=ui('p','','ps-part-summary'),explodeLabel=ui('label','Exploded inspection'),explodeInput=ui('input');
 partSelect.setAttribute('aria-label','Inspect model part');explodeInput.type='range';explodeInput.min='0';explodeInput.max='1';explodeInput.step='.05';explodeInput.value='0';explodeInput.setAttribute('aria-label','Exploded model inspection');explodeLabel.append(explodeInput);
 const reset=button(words.complete,()=>{activePart=null;exploded=0;explodeInput.value='0';partSelect.value='';applyParts();fit('perspective','selection');});
 const isolate=button('Isolate selected part',()=>{if(!activePart)return;for(const n of parts)n.visible=n===activePart;fit('perspective','part');invalidate();});
 panel.append(ui('strong','Model parts'),partSelect,summary,isolate,reset,explodeLabel,ui('small',words.notice));viewport.append(panel);
 partSelect.addEventListener('change',()=>{activePart=partSelect.value===''?null:parts[Number(partSelect.value)]??null;applyParts();});
 explodeInput.addEventListener('input',()=>{exploded=Number(explodeInput.value);applyParts();fit('perspective','selection');});
 const orientation=ui('div','','ps-orientation');for(const [label,id] of [['Plan','top'],['Front','front'],['Perspective','perspective']])orientation.append(button(label,()=>fit(id,'all')));viewport.append(orientation);
 const badge=ui('div','','ps-selection-card');viewport.append(badge);
 const light=scene.children.find(n=>n.isDirectionalLight),ambient=scene.children.find(n=>n.isHemisphereLight);if(light){scene.add(light.target);light.shadow.bias=-.00015;light.shadow.normalBias=.015;}
 const boxFor=target=>new T.Box3().setFromObject(target,true);
 const selectedObject=()=>readView().selection;
 function fit(viewName='perspective',scope='all'){
  if(disposed)return;const state=readView(),target=scope==='part'&&activePart?activePart:scope==='selection'?selectedObject()??objects:objects,box=boxFor(target);if(box.isEmpty())return;
  const size=box.getSize(new T.Vector3()),center=box.getCenter(new T.Vector3()),extent=Math.max(size.x,size.z,size.y,.25);
  const fov=2*Math.atan(Math.tan(camera.fov*Math.PI/360)*Math.min(1,camera.aspect));
  const distance=Math.max(.6,extent/(2*Math.tan(fov/2)))*(scope!=='all'||!state.spatial?1.7:1.38);
  const direction=viewName==='top'?new T.Vector3(0,1,.0001):viewName==='front'?new T.Vector3(0,.08,1):(!state.spatial?new T.Vector3(.45,.3,1):new T.Vector3(.8,.72,1)).normalize();
  camera.near=.001;camera.far=Math.max(100,distance*20);camera.updateProjectionMatrix();
  const position=center.clone().addScaledVector(direction,distance),smooth=!matchMedia('(prefers-reduced-motion: reduce)').matches;
  controls.setLookAt(position.x,position.y,position.z,center.x,center.y,center.z,smooth).catch(e=>say(e.message,true));view.center.copy(center);root.dataset.camera=scope;root.dataset.orientation=viewName;invalidate();
 }
 function clearAnnotations(){for(const n of [...annotations.children]){n.traverse(x=>{x.geometry?.dispose();x.material?.dispose();x.element?.remove();});annotations.remove(n);}}
 function dimension(from,to,text,offset){
  const start=from.clone().add(offset),end=to.clone().add(offset),line=new T.Line(new T.BufferGeometry().setFromPoints([from,start,end,to]),new T.LineBasicMaterial({color:0x514b3f,depthTest:false,transparent:true,opacity:.8}));line.renderOrder=1000;annotations.add(line);
  const label=new CSS2DObject(ui('span',text,'ps-dimension'));label.position.copy(start).lerp(end,.5);annotations.add(label);
 }
 function updateDimensions(){clearAnnotations();dims.setAttribute('aria-pressed',String(dimensions));if(!dimensions)return;const target=selectedObject();if(!target)return;
  const b=boxFor(target),size=b.getSize(new T.Vector3()),pad=Math.max(.075,Math.max(size.x,size.y,size.z)*.13),p=b.min,q=b.max,basis=exploded>0?'inspection span':'model';
  dimension(new T.Vector3(p.x,p.y,q.z),new T.Vector3(q.x,p.y,q.z),formatDimension(size.x,'width',basis),new T.Vector3(0,0,pad));
  dimension(new T.Vector3(q.x,p.y,q.z),new T.Vector3(q.x,q.y,q.z),formatDimension(size.y,'height',basis),new T.Vector3(pad,0,pad));
  dimension(new T.Vector3(q.x,p.y,p.z),new T.Vector3(q.x,p.y,q.z),formatDimension(size.z,'depth',basis),new T.Vector3(pad,0,0));
 }
 function restoreParts(){for(const [n,p] of positions){n.position.copy(p.position);n.visible=p.visible;}}
 function updatePartRoot(){const selected=selectedObject();if(selected===partRoot)return;restoreParts();partRoot=selected;parts=[];positions.clear();activePart=null;exploded=0;explodeInput.value='0';
  partRoot?.traverse(n=>{if(n.isMesh){parts.push(n);positions.set(n,{position:n.position.clone(),visible:n.visible});}});
  partSelect.replaceChildren(new Option(words.whole,''));parts.forEach((n,i)=>partSelect.add(new Option(n.name||'Mesh '+(i+1),String(i))));isolate.disabled=true;explodeInput.disabled=!parts.length;updatePartSummary();
 }
 function updatePartSummary(){const n=activePart,m=Array.isArray(n?.material)?n.material[0]:n?.material;summary.textContent=n?`${n.name||'Selected mesh'} · ${m?.name||m?.type||'Authored material'}${Number.isFinite(m?.roughness)?' · roughness '+m.roughness.toFixed(2):''}`:`${parts.length} mesh parts in the source model`;outline.selectedObjects=activePart?[activePart]:selectedObject()?[selectedObject()]:[];}
 function applyParts(){restoreParts();if(exploded&&partRoot){const box=boxFor(partRoot),center=box.getCenter(new T.Vector3()),scale=box.getSize(new T.Vector3()).length()*.3;for(const n of parts){const direction=boxFor(n).getCenter(new T.Vector3()).sub(center);if(direction.lengthSq()<.000001)direction.set(0,1,0);direction.normalize().multiplyScalar(scale*exploded);const world=n.getWorldPosition(new T.Vector3()).add(direction);n.position.copy(n.parent.worldToLocal(world));}partRoot.updateMatrixWorld(true);}
  isolate.disabled=!activePart;updatePartSummary();root.dataset.part=activePart?.name||'';root.dataset.explode=String(exploded);updateDimensions();invalidate();
 }
 function setLighting(value){const p={studio:[0xe8e5dc,0xfff8ea,2,.6,1],daylight:[0xe3ebeb,0xeaf6ff,2.7,.8,1],evening:[0x37332f,0xffbb79,1.8,.3,.93]}[value];if(!p)return;scene.background.setHex(p[0]);if(light){light.color.setHex(p[1]);light.intensity=p[2];}if(ambient)ambient.intensity=p[3];renderer.toneMappingExposure=p[4];root.dataset.lighting=value;invalidate();}
 function sync(){const state=readView();updatePartRoot();const key=[state.spatial,state.ground?.width,state.ground?.depth,state.ground?.cell,state.ground?.section].join(':');
  if(key!==context){context=key;for(const n of [...ground.children]){n.geometry?.dispose();n.material?.dispose();ground.remove(n);}
   if(state.spatial&&state.ground){const {width:w,depth:d,cell,section}=state.ground,g=Grid({args:[w,d],cellSize:cell,sectionSize:section,cellColor:new T.Color('#9b9e93'),sectionColor:new T.Color('#777f72'),cellThickness:.55,sectionThickness:1,fadeDistance:40,fadeStrength:1,infiniteGrid:false});g.mesh.position.set(w/2,.004,d/2);ground.add(g.mesh);}
   else{const floor=new T.Mesh(new T.PlaneGeometry(200,200),new T.ShadowMaterial({color:0x4f4635,opacity:.2}));floor.rotation.x=-Math.PI/2;floor.position.y=-.006;floor.receiveShadow=true;ground.add(floor);}
  }
  decorateScene(objects);
  const bound=boxFor(objects);if(light&&!bound.isEmpty()){const center=bound.getCenter(new T.Vector3()),size=bound.getSize(new T.Vector3()),span=Math.max(2,size.x,size.z,size.y)*.8;light.target.position.copy(center);light.shadow.camera.left=-span;light.shadow.camera.right=span;light.shadow.camera.top=span;light.shadow.camera.bottom=-span;light.shadow.camera.updateProjectionMatrix();}
  badge.hidden=!state.selectionLabel;badge.textContent=state.selectionLabel??'';badge.dataset.selection=state.selectionKey??'';updatePartSummary();updateDimensions();syncHistory();invalidate();
 }
 function resize(){const w=Math.max(1,viewport.clientWidth),h=Math.max(330,viewport.clientHeight),key=w+'x'+h;if(key===lastSize)return;lastSize=key;renderer.setSize(w,h,false);camera.aspect=w/h;camera.updateProjectionMatrix();composer.setSize(w,h);labels.setSize(w,h);invalidate();}
 function hitAt(e){const b=renderer.domElement.getBoundingClientRect(),ray=new T.Raycaster();ray.setFromCamera(new T.Vector2((e.clientX-b.left)/b.width*2-1,-(e.clientY-b.top)/b.height*2+1),camera);return{ray,hit:ray.intersectObjects(readView().pickObjects,true).find(h=>h.object.isMesh)};}
 const inGizmo=e=>{const b=viewport.getBoundingClientRect();return e.clientX>b.right-128&&e.clientY>b.bottom-128;};
 const down=e=>{if(e.button!==0)return;if(inGizmo(e)){gizmoPointer=true;controls.enabled=false;e.stopImmediatePropagation();return;}pointer={x:e.clientX,y:e.clientY};const state=readView();if(!state.spatial||!manipulation?.allowed(state))return;
  const {ray,hit}=hitAt(e),key=hit?objectKey(hit.object):null;if(!key||key!==state.selectionKey)return;const point=ray.ray.intersectPlane(new T.Plane(new T.Vector3(0,1,0),0),new T.Vector3());if(!point)return;
  const object=state.selection;if(!object)return;drag={key,start:point.clone(),position:object.position.clone(),base:manipulation.capture(key),object,moved:false};controls.enabled=false;renderer.domElement.setPointerCapture(e.pointerId);e.stopImmediatePropagation();};
 const move=e=>{if(!drag)return;const point=hitAt(e).ray.ray.intersectPlane(new T.Plane(new T.Vector3(0,1,0),0),new T.Vector3());if(!point)return;if(Math.hypot(e.clientX-pointer.x,e.clientY-pointer.y)>4)drag.moved=true;
  try{const next=manipulation.preview(drag.key,drag.position,point.sub(drag.start),drag.base);if(!next||!Number.isFinite(next.x)||!Number.isFinite(next.z))throw Error('The scene owner refused this position.');drag.object.position.x=next.x;drag.object.position.z=next.z;}catch(error){cancel();say(error.message,true);return;}updateDimensions();invalidate();e.stopImmediatePropagation();};
 const up=e=>{if(gizmoPointer){gizmoPointer=false;controls.enabled=true;controls.getTarget(view.center);if(view.handleClick(e))invalidate();e.stopImmediatePropagation();return;}if(drag){const d=drag;drag=null;controls.enabled=true;if(renderer.domElement.hasPointerCapture(e.pointerId))renderer.domElement.releasePointerCapture(e.pointerId);if(d.moved){try{if(!manipulation.allowed(readView()))throw Error('This scene can no longer be edited.');manipulation.commit(d.key,{x:d.object.position.x,z:d.object.position.z},d.object,d.base);}catch(error){d.object.position.copy(d.position);say(error.message,true);}}e.stopImmediatePropagation();invalidate();return;}
  if(pointer&&Math.hypot(e.clientX-pointer.x,e.clientY-pointer.y)<5&&!readView().spatial){const {hit}=hitAt(e);if(hit){activePart=hit.object;partsOpen=true;panel.hidden=false;partToggle.setAttribute('aria-pressed','true');partSelect.value=String(parts.indexOf(activePart));applyParts();}}};
 const cancel=()=>{gizmoPointer=false;controls.enabled=true;if(!drag)return;drag.object.position.copy(drag.position);drag=null;invalidate();};
 renderer.domElement.addEventListener('pointerdown',down,true);renderer.domElement.addEventListener('pointermove',move,true);renderer.domElement.addEventListener('pointerup',up,true);renderer.domElement.addEventListener('pointercancel',cancel,true);
 const dbl=e=>{const {hit}=hitAt(e);if(!hit)return;const key=objectKey(hit.object);if(key)onSelect(key);fit('perspective','selection');};renderer.domElement.addEventListener('dblclick',dbl);
 const esc=e=>{if(e.key==='Escape'){cancel();if(root.classList.contains('ps-expanded')){root.classList.remove('ps-expanded');expand.textContent='Expand workspace';resize();}}};root.addEventListener('keydown',esc);
 controls.addEventListener('wake',invalidate);controls.addEventListener('control',invalidate);setLighting('studio');root.dataset.studioTools='2';root.dataset.component='scene-workbench';resize();
 return {fit,sync,resize,
  render(delta){if(disposed)return false;const dt=Math.min(delta||1/60,.1);if(view.animating){view.update(dt);controls.setLookAt(camera.position.x,camera.position.y,camera.position.z,view.center.x,view.center.y,view.center.z,false);}const moved=controls.update(dt);renderer.info.autoReset=false;renderer.info.reset();composer.render();labels.render(scene,camera);const auto=renderer.autoClear;renderer.autoClear=false;view.render(renderer);renderer.autoClear=auto;renders++;root.dataset.renderFrames=String(renders);return moved||view.animating;},
  inspect(){return{helpers:'camera-controls, drei-grid, outline-pass, css2d, view-helper',renders,parts:parts.length,selectedPart:activePart?.name??null,lighting:lighting.value,dimensions,exploded};},
  dispose(){disposed=true;cancel();for(const unsubscribe of historySubscriptions)unsubscribe();restoreParts();clearAnnotations();annotations.removeFromParent();for(const n of ground.children){n.geometry?.dispose();n.material?.dispose();}ground.removeFromParent();composer.dispose();outline.dispose();labels.domElement.remove();view.dispose();tools.remove();panel.remove();orientation.remove();badge.remove();delete root.dataset.component;controls.removeEventListener('wake',invalidate);controls.removeEventListener('control',invalidate);renderer.domElement.removeEventListener('pointerdown',down,true);renderer.domElement.removeEventListener('pointermove',move,true);renderer.domElement.removeEventListener('pointerup',up,true);renderer.domElement.removeEventListener('pointercancel',cancel,true);renderer.domElement.removeEventListener('dblclick',dbl);root.removeEventListener('keydown',esc);}
 };
}
