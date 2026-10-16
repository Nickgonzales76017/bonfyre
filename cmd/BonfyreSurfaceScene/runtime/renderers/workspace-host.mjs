import {WorkspaceSession} from './workspace-composition.mjs';

/** Assemble live application bindings. Content nodes are moved, never cloned. */
export function createWorkspaceHost(root, {scene, contributions, docking, before = null, report = () => {}}) {
  if(!root?.ownerDocument || typeof docking?.createDockview!=='function')throw Error('A workspace needs its content root and docking renderer.');
  const session = new WorkspaceSession(scene, contributions), doc = root.ownerDocument;
  const resolved=session.plan.panels.map(p=>({panel:p,node:p.adapter.element()})), seen=new Set();
  for(const {node}of resolved){if(!node||!root.contains(node)||node===root||seen.has(node)||[...seen].some(other=>other.contains(node)||node.contains(other)))throw Error('Workspace panels must be distinct, non-nested elements of their own host.');seen.add(node);}
  const el=(tag,text,cls)=>{const node=doc.createElement(tag);if(text)node.textContent=text;if(cls)node.className=cls;return node;};
  const shell=el('section','','bf-workspace'),bar=el('header','','bf-workspace-bar'),area=el('div','','bf-workspace-dock'),parking=el('div','','bf-workspace-parking');
  parking.hidden=true;shell.dataset.bonfyreWorkspace=scene.focus.subject_address;
  const title=el('strong',scene.interaction?.desktop_posture?.title||'Workspace'),layout=el('select'),views=el('select');
  layout.setAttribute('aria-label','Workspace arrangement');views.setAttribute('aria-label','Open workspace panel');
  for(const recipe of session.plan.layouts)layout.add(new Option(recipe.label||recipe.id,recipe.id));
  views.add(new Option('Open a panel…',''));for(const panel of session.plan.panels)views.add(new Option(panel.label,panel.id));
  const paletteButton=el('button','Commands ⌘K'),reset=el('button','Reset arrangement'),note=el('span','','bf-workspace-note');
  for(const b of [paletteButton,reset])b.type='button';
  bar.append(title,layout,views,paletteButton,reset,note);shell.append(bar,area,parking);
  if(before){if(!root.contains(before))throw Error('Workspace insertion point is outside its host.');before.before(shell);}else root.append(shell);
  const originals=new Map(),nodes=new Map();
  for(const {panel,node}of resolved){const anchor=doc.createComment('binding:'+panel.id);node.before(anchor);originals.set(panel.id,{node,anchor,hidden:node.hidden});nodes.set(panel.id,node);parking.append(node);}
  let disposed=false,currentLayout=session.plan.initial,mobile=false,api;
  const live=new AbortController(),{signal}=live;
  const restore=()=>{for(const {node,anchor,hidden}of originals.values()){anchor.replaceWith(node);node.hidden=hidden;}};
  try{api=docking.createDockview(area,{
    theme:scene.interaction?.desktop_posture?.theme==='dark'?docking.themeDark:docking.themeLight,
    disableFloatingGroups:true,disableDnd:false,
    createComponent({id}){const definition=session.plan.panels.find(p=>p.id===id),content=el('div','','bf-workspace-content');content.dataset.binding=id;
      return {element:content,init(){const node=nodes.get(id);content.append(node);node.hidden=false;definition.adapter.mounted?.({session,host:content});},layout(){definition.adapter.resize?.();},dispose(){const node=nodes.get(id);if(node)parking.append(node);}};}
  });}catch(error){restore();shell.remove();session.dispose();throw error;}
  function notifyResize(){if(disposed)return;api.layout(Math.max(1,area.clientWidth),Math.max(240,area.clientHeight));for(const p of session.plan.panels)p.adapter.resize?.();}
  function add(id,position){const p=session.plan.panels.find(p=>p.id===id);if(!p)throw Error('Unknown workspace panel.');return api.addPanel({id,component:p.kind,title:p.label,renderer:'always',minimumWidth:160,minimumHeight:140,...(position?.relative?{position:{referencePanel:position.relative,direction:position.direction||'within'}}:{}),initialWidth:position?.width,initialHeight:position?.height});}
  function arrange(id){if(disposed)return;const recipe=session.plan.layouts.find(r=>r.id===id);if(!recipe)throw Error('Unknown workspace arrangement.');for(const panel of [...api.panels])api.removePanel(panel);currentLayout=id;layout.value=id;
    for(const [i,placement]of recipe.panels.entries())add(placement.id,mobile&&i?{relative:recipe.panels[0].id,direction:'within'}:placement);api.getPanel(recipe.panels[0].id)?.api.setActive();note.textContent='';notifyResize();}
  function open(id,{side=false}={}){if(disposed)return;let panel=api.getPanel(id);if(!panel)panel=add(id,api.activePanel?{relative:api.activePanel.id,direction:side&&!mobile?'right':'within'}:undefined);panel.api.setActive();nodes.get(id).hidden=false;notifyResize();return panel;}
  function focus(id){const panel=open(id);panel?.group?.api.maximize?.();notifyResize();}
  const palette=el('dialog','','bf-command-palette'),search=el('input'),results=el('div','','bf-command-results'),status=el('p','','bf-command-status');
  search.type='search';search.placeholder='Find an action or panel…';search.setAttribute('aria-label','Search workspace commands');
  const close=el('button','Close');close.type='button';close.addEventListener('click',()=>palette.close(),{signal});palette.append(search,close,results,status);root.append(palette);
  let returnFocus=null;
  function renderCommands(){const q=search.value.toLocaleLowerCase().trim();results.replaceChildren();
    for(const c of session.commands(q)){const item=el('button','','bf-command-result');item.type='button';item.disabled=!c.enabled;item.dataset.command=c.id;item.append(el('strong',c.label),el('small',c.enabled?(c.group||'Current workspace'):c.reason));
      item.onclick=async()=>{palette.close();try{await session.execute(c.id);}catch(error){note.textContent=error.message;report(error.message);}};results.append(item);}
    for(const p of session.plan.panels.filter(p=>('open '+p.label).toLocaleLowerCase().includes(q))){const item=el('button','Open '+p.label,'bf-command-result');item.type='button';item.dataset.openPanel=p.id;item.onclick=()=>{palette.close();open(p.id);};results.append(item);}
    if(!results.children.length)results.append(el('p','No matching action in this workspace.'));}
  function showPalette(){returnFocus=doc.activeElement;search.value='';status.textContent='';renderCommands();palette.showModal();search.focus();}
  palette.addEventListener('close',()=>{if(returnFocus?.isConnected&&!doc.querySelector('dialog[open]'))returnFocus.focus();},{signal});
  search.addEventListener('input',renderCommands,{signal});
  palette.addEventListener('keydown',event=>{const enabled=[...results.querySelectorAll('button:not(:disabled)')];if(event.key==='ArrowDown'||event.key==='ArrowUp'){event.preventDefault();const i=enabled.indexOf(doc.activeElement),n=event.key==='ArrowDown'?i+1:i-1;enabled[(n+enabled.length)%enabled.length]?.focus();}if(event.key==='Enter'&&event.target===search){event.preventDefault();enabled[0]?.click();}},{signal});
  root.addEventListener('keydown',event=>{if(!shell.contains(event.target)&&event.target!==root)return;if(event.key.toLowerCase()==='k'&&(event.ctrlKey||event.metaKey)){event.preventDefault();event.stopPropagation();showPalette();}},{signal});
  paletteButton.onclick=showPalette;layout.onchange=()=>arrange(layout.value);reset.onclick=()=>arrange(currentLayout);views.onchange=()=>{if(views.value)open(views.value,{side:true});views.value='';};
  const unsubscribe=session.subscribe(event=>{if(event.type==='selection')for(const p of session.plan.panels)if(p.continuity===event.continuity&&p.id!==event.origin){try{p.adapter.selection?.(event.selected,session.context(p.id));}catch(error){report(error.message);}}if(palette.open)renderCommands();});
  const resize=new ResizeObserver(()=>{const next=area.clientWidth<700;if(next!==mobile){mobile=next;arrange(currentLayout);}else notifyResize();});
  function dispose(){if(disposed)return;disposed=true;live.abort();resize.disconnect();unsubscribe();session.dispose();api.dispose();palette.remove();restore();shell.remove();}
  try{resize.observe(area);mobile=root.clientWidth<700;arrange(currentLayout);shell.dataset.ready='true';}catch(error){dispose();throw error;}
  return {session,element:shell,activate:open,open,focus,arrange,refresh:()=>session.refresh(),inspect:()=>({subject:session.plan.focus.subject_address,revision:session.plan.revision,layout:currentLayout,panels:api.panels.map(p=>({id:p.id,group:p.group.id})),groups:api.groups.length,mobile}),dispose};
}
