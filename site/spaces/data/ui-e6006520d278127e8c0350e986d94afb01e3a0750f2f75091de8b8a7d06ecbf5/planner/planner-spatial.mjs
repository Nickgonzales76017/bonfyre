import {moveFixtures, editFixtures, displayFootprint} from './planner-spatial-model.mjs';

export function createSpatialEditor({root,catalogue,onSelect,onChange,canEdit,fail}) {
  const ui=name=>root.querySelector(`[data-ui="${name}"]`), host=ui('spatial');
  let K,stage,layer,world,outline,marquee,plan,primary,keys=new Set(),nodes=new Map(),zoom=1,pan={x:0,y:0},mode='select',dragging=false,boxStart=null,panStart=null,attempt=0;
  const product=id=>catalogue.products.find(p=>p.id===id);
  function controls() {
    ui('selected-count').textContent=keys.size?`${keys.size} selected`:'Select fixtures';
    for(const button of root.querySelectorAll('[data-spatial-action]'))button.disabled=!canEdit()||!keys.size||(button.dataset.spatialAction.startsWith('distribute')&&keys.size<3);
    ui('zoom-level').textContent=`${Math.round(zoom*100)}%`;
    ui('pan').setAttribute('aria-pressed',String(mode==='pan'));
  }
  function select(list,notify=true) {
    keys=new Set(list.filter(key=>plan.items.some(i=>i.key===key)));primary=[...keys].at(-1)??null;
    if(notify)onSelect(primary);else drawSelection();controls();
  }
  function choose(key,extend=false) {
    const next=extend?new Set(keys):new Set();if(extend&&next.has(key))next.delete(key);else next.add(key);select([...next]);
  }
  function drawSelection() {
    for(const button of root.querySelectorAll('[data-select]'))button.setAttribute('aria-pressed',String(keys.has(button.dataset.select)));
    if(!stage)return;
    for(const [key,node] of nodes){const selected=keys.has(key);node.findOne('.footprint').stroke(selected?'#b5282c':node.getAttr('known')?'#62765f':'#a27b34');node.findOne('.footprint').strokeWidth((selected?2:1)/world.scaleX());}
    outline.nodes([...keys].map(k=>nodes.get(k)).filter(Boolean));layer.batchDraw();
  }
  function draw() {
    if(!stage||!plan||dragging)return;
    const width=Math.max(240,host.clientWidth),height=width<500?Math.max(300,Math.min(420,(width-80)*plan.depth/plan.width+85)):Math.max(340,Math.min(590,innerHeight*.64));stage.size({width,height});
    layer.destroyChildren();nodes=new Map();
    const fit=Math.min((width-80)/plan.width,(height-85)/plan.depth),scale=fit*zoom;
    world=new K.Group({x:(width-plan.width*fit)/2+pan.x,y:35+pan.y,scaleX:scale,scaleY:scale});layer.add(world);
    const text=(x,y,value,extra={})=>new K.Text({x,y,text:value,fontSize:11/scale,fill:'#626a60',fontFamily:'Arial',listening:false,...extra});
    world.add(new K.Rect({x:0,y:0,width:plan.width,height:plan.depth,fill:'#fffef9',stroke:'#879382',strokeWidth:1.5/scale,listening:false}));
    for(let x=1;x<plan.width;x++)world.add(new K.Line({points:[x,0,x,plan.depth],stroke:'#e2e5dc',strokeWidth:1/scale,listening:false}));
    for(let y=1;y<plan.depth;y++)world.add(new K.Line({points:[0,y,plan.width,y],stroke:'#e2e5dc',strokeWidth:1/scale,listening:false}));
    world.add(text(0,-22/scale,`${plan.width} ft`,{width:plan.width,align:'center'}));
    world.add(text(-24/scale,plan.depth,`${plan.depth} ft`,{rotation:-90,width:plan.depth,align:'center'}));
    world.add(new K.Line({points:[plan.width*.4,plan.depth,plan.width*.6,plan.depth],stroke:'#fffef9',strokeWidth:5/scale,listening:false}));
    world.add(text(0,plan.depth+12/scale,'FRONT / ACCESS TO CONFIRM',{width:plan.width,align:'center',fontSize:9/scale}));
    plan.items.forEach((item,index)=>{
      const p=product(item.product),dim=displayFootprint(p,item),g=new K.Group({id:item.key,x:item.x,y:item.y,draggable:mode==='select'&&canEdit(),known:dim.known});
      const rect=new K.Rect({name:'footprint',width:dim.w,height:dim.h,fill:dim.known?'#d5dfce':p.allowance?'#f2e7ce':'#c75a3d',stroke:dim.known?'#62765f':'#a27b34',strokeWidth:1/scale,dash:!dim.known&&p.allowance?[5/scale,3/scale]:undefined,cornerRadius:p.allowance?3/scale:dim.w/2});
      g.add(rect);g.add(text(0,Math.max(0,(dim.h-12/scale)/2),String(index+1),{width:dim.w,align:'center',fontStyle:'bold',fill:p.allowance?'#344334':'white'}));
      if(p.allowance)g.add(text(-.5,dim.h+5/scale,p.use,{width:dim.w+1,align:'center',fontSize:9/scale}));
      world.add(g);nodes.set(item.key,g);
      g.on('click tap',event=>{if(mode==='select')choose(item.key,event.evt.shiftKey||event.evt.metaKey);});
      let original,candidate;
      g.on('dragstart',()=>{
        dragging=true;original=structuredClone(plan);if(!keys.has(item.key))keys=new Set([item.key]);primary=item.key;onSelect(primary);drawSelection();controls();
      });
      g.on('dragmove',()=>{
        try{candidate=moveFixtures(original,catalogue,[...keys],g.x()-item.x,g.y()-item.y);
          for(const it of candidate.items)if(keys.has(it.key))nodes.get(it.key)?.position({x:it.x,y:it.y});
          const moved=candidate.items.find(i=>i.key===item.key);ui('spatial-status').textContent=`${moved.x}, ${moved.y} ft · ¼ ft grid`;layer.batchDraw();
        }catch(error){fail(error.message);}
      });
      g.on('dragend',()=>{dragging=false;if(candidate&&canEdit())onChange(candidate);else draw();candidate=null;});
    });
    outline=new K.Transformer({resizeEnabled:false,rotateEnabled:false,borderStroke:'#b5282c',borderDash:[4,3],padding:5,borderStrokeWidth:1,listening:false});world.add(outline);
    marquee=new K.Rect({fill:'#b5282c18',stroke:'#b5282c',strokeWidth:1/scale,visible:false,listening:false});world.add(marquee);drawSelection();controls();layer.draw();
  }
  const point=()=>{const p=stage.getPointerPosition();return{x:(p.x-world.x())/world.scaleX(),y:(p.y-world.y())/world.scaleY()};};
  function zoomTo(value) {zoom=Math.min(3,Math.max(.5,value));draw();}
  async function load() {
    ui('spatial-retry').hidden=true;ui('spatial-status').textContent='Loading spatial editor…';
    try{
      await import(`./vendor/konva-10.9.0/konva.min.js${attempt?'?retry='+attempt:''}`);K=globalThis.Konva;if(!K)throw Error('Canvas library did not load.');
      host.hidden=false;ui('canvas').hidden=true;ui('canvas').setAttribute('hidden','');stage=new K.Stage({container:host,width:host.clientWidth,height:440});layer=new K.Layer();stage.add(layer);
      stage.on('mousedown touchstart',event=>{
        if(!canEdit())return;
        if(mode==='pan'){const p=stage.getPointerPosition();panStart={point:p,pan:{...pan}};return;}
        if(event.target!==stage)return;
        boxStart=point();marquee.position(boxStart);marquee.size({width:0,height:0});marquee.show();layer.batchDraw();
      });
      stage.on('mousemove touchmove',()=>{
        if(panStart){const p=stage.getPointerPosition();pan={x:panStart.pan.x+p.x-panStart.point.x,y:panStart.pan.y+p.y-panStart.point.y};draw();return;}
        if(!boxStart)return;const p=point();marquee.position({x:Math.min(p.x,boxStart.x),y:Math.min(p.y,boxStart.y)});marquee.size({width:Math.abs(p.x-boxStart.x),height:Math.abs(p.y-boxStart.y)});layer.batchDraw();
      });
      stage.on('mouseup touchend',()=>{
        panStart=null;if(!boxStart)return;
        const box=marquee.getClientRect(),hits=[...nodes].filter(([,node])=>K.Util.haveIntersection(box,node.findOne('.footprint').getClientRect())).map(([key])=>key);boxStart=null;marquee.hide();select(hits);
      });
      stage.on('wheel',event=>{if(!event.evt.ctrlKey&&!event.evt.metaKey)return;event.evt.preventDefault();zoomTo(zoom*(event.evt.deltaY<0?1.1:1/1.1));});
      host.dataset.ready='true';ui('spatial-status').textContent='¼ ft grid · Shift-click to select several';draw();
    }catch(error){stage?.destroy();stage=null;host.hidden=true;ui('canvas').hidden=false;ui('canvas').removeAttribute('hidden');ui('spatial-retry').hidden=false;ui('spatial-status').textContent='Spatial editor unavailable. Select fixtures in the list to edit their positions.';}
  }
  host.addEventListener('keydown',event=>{
    if(!canEdit()||event.target!==host)return;
    const delta={ArrowLeft:[-.25,0],ArrowRight:[.25,0],ArrowUp:[0,-.25],ArrowDown:[0,.25]}[event.key];
    if(delta&&keys.size){event.preventDefault();onChange(moveFixtures(plan,catalogue,[...keys],...delta.map(n=>n*(event.shiftKey?4:1))));}
    if((event.ctrlKey||event.metaKey)&&event.key.toLowerCase()==='a'){event.preventDefault();select(plan.items.map(i=>i.key));}
    if(event.key==='Escape'){boxStart=null;select([]);}
    if(event.key==='Delete'||event.key==='Backspace'){event.preventDefault();action('remove');}
  });
  function action(name){if(!canEdit())return;try{const next=editFixtures(plan,catalogue,[...keys],name);if(name==='duplicate')keys=new Set(next.items.slice(plan.items.length).map(i=>i.key));if(name==='remove')keys.clear();primary=[...keys].at(-1)??null;onChange(next,primary);controls();}catch(error){fail(error.message);}}
  root.addEventListener('click',event=>{const button=event.target.closest('[data-spatial-action]');if(button)action(button.dataset.spatialAction);});
  ui('zoom-in').onclick=()=>zoomTo(zoom+.25);ui('zoom-out').onclick=()=>zoomTo(zoom-.25);ui('fit-layout').onclick=()=>{zoom=1;pan={x:0,y:0};draw();};
  ui('pan').onclick=()=>{mode=mode==='pan'?'select':'pan';host.style.cursor=mode==='pan'?'grab':'default';draw();};
  ui('spatial-retry').onclick=()=>{attempt++;void load();};
  ui('layout-image').onclick=()=>{
    if(!stage)return;const link=document.createElement('a');link.href=stage.toDataURL({pixelRatio:2});link.download='fixture-layout.png';link.click();
  };
  const observer=new ResizeObserver(draw);observer.observe(host);
  addEventListener('pageshow',event=>{if(event.persisted)location.reload();});
  addEventListener('pagehide',()=>{observer.disconnect();stage?.destroy();});
  void load();
  return {
    update(next,selected){if(plan?.id!==next.id){keys.clear();pan={x:0,y:0};zoom=1;}plan=structuredClone(next);primary=selected;keys=new Set([...keys].filter(key=>plan.items.some(i=>i.key===key)));if(selected&&!keys.has(selected))keys=new Set([selected]);if(!selected)keys.clear();draw();},
    select:choose,
    refresh:draw
  };
}
