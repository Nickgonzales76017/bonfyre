// Executable composition of existing SurfaceScene coordinates. Data and effects
// stay in the registered binding. This module owns transient interaction only.
const list = value => Array.isArray(value) ? value : [];
const terms = value => typeof value === 'string' ? value.split(',').map(s => s.trim()).filter(Boolean) : list(value);
const id = value => { if (typeof value !== 'string' || !value || value.length > 240 || /[\u0000-\u001f\u007f]/u.test(value)) throw Error('Invalid workspace coordinate.'); return value; };
const immutable = value => Object.freeze(value);
export function compileWorkspace(scene, contributions) {
  if (scene?.schema !== 'bonfyre-surface-scene.v1') throw Error('A workspace requires its derived SurfaceScene.');
  id(scene.source_digest); id(scene.focus?.subject_address); id(scene.focus?.actor_scope);
  if (!(contributions instanceof Map)) throw Error('Bindings must be explicitly registered.');
  const slots = new Map(), bindings = new Set(), panels = [], commands = new Map();
  if (list(scene.slots).length > 64 || list(scene.bindings).length > 128) throw Error('Workspace exceeds its composition bound.');
  for (const slot of list(scene.slots)) {
    id(slot.slot_id); if (slots.has(slot.slot_id)) throw Error('Duplicate semantic slot.');
    if (!['section','page','workspace'].includes(slot.extent)) throw Error('This renderer needs a section, page or workspace extent.');
    slots.set(slot.slot_id, slot);
  }
  for (const binding of list(scene.bindings)) {
    id(binding.binding_id); if (bindings.has(binding.binding_id)) throw Error('Duplicate application binding.');
    bindings.add(binding.binding_id);
    const slot = slots.get(binding.slot_id), adapter = contributions.get(binding.binding_id);
    if (!slot) throw Error('Application binding has no semantic slot.');
    if (!list(slot.accepts).includes(binding.application_kind)) throw Error('The slot does not accept this application kind.');
    if (!adapter) throw Error(`No executable binding for ${binding.binding_id}.`);
    if (adapter.kind !== binding.application_kind) throw Error('The registered binding has a different application kind.');
    if (typeof adapter.element !== 'function') throw Error('A binding must supply its existing content element.');
    id(adapter.label);
    if (list(adapter.commands).length > 64) throw Error('Binding exceeds its command bound.');
    if (slot.extent === 'section' && list(adapter.commands).length) throw Error('A section is a reference without effect controls.');
    const aperture = new Set(terms(binding.action_aperture));
    for (const command of list(adapter.commands)) {
      id(command.id); id(command.label);
      if (!aperture.has(command.id) || commands.has(command.id)) throw Error('Command is ambiguous or outside its action aperture.');
      if (typeof command.run !== 'function' || typeof command.enabled !== 'function') throw Error('Command requires its owner handler and current availability.');
      commands.set(command.id, immutable({...command, binding: binding.binding_id}));
    }
    panels.push(immutable({id: binding.binding_id, slot: slot.slot_id, kind: binding.application_kind,
      continuity: binding.continuity_key || '', extent: slot.extent, label: adapter.label,
      adapter, ordinal: slot.ordinal ?? panels.length}));
  }
  // max_items bounds owner material, not the number of mounted panes.
  const recipes = list(scene.interaction?.desktop_posture?.layouts);
  if (!recipes.length || recipes.length > 12) throw Error('A workspace needs bounded declared layouts.');
  const recipeNames = new Set();
  for (const recipe of recipes) {
    id(recipe.id); if (recipeNames.has(recipe.id)) throw Error('Duplicate workspace layout.'); recipeNames.add(recipe.id);
    const placed = new Set();
    for (const placement of list(recipe.panels)) {
      if (!bindings.has(placement.id) || placed.has(placement.id)) throw Error('Layout names an unknown or repeated binding.');
      if (placement.relative && !placed.has(placement.relative)) throw Error('Place a referenced panel before its dependant.');
      if (!['within','left','right','above','below'].includes(placement.direction ?? 'within')) throw Error('Unsupported panel direction.');
      placed.add(placement.id);
    }
    if (!placed.size) throw Error('A workspace layout cannot be empty.');
  }
  const initial = scene.interaction?.desktop_posture?.initial_layout ?? recipes[0].id;
  if (!recipeNames.has(initial)) throw Error('Initial layout is undeclared.');
  return immutable({revision: scene.source_digest, focus: immutable({...scene.focus}),
    panels: immutable(panels.sort((a,b) => a.ordinal-b.ordinal)), commands,
    layouts: recipes.map(r => immutable({...r, panels: r.panels.map(p => immutable({...p}))})),
    initial});
}

export class WorkspaceSession {
  #plan; #closed = false; #listeners = new Set(); #selection = new Map(); #running = new Map(); #epoch = 0;
  constructor(scene, contributions) { this.#plan = compileWorkspace(scene, contributions); }
  get plan() { return this.#plan; }
  get closed() { return this.#closed; }
  subscribe(listener) { if (this.#closed) throw Error('Workspace is closed.'); this.#listeners.add(listener); return () => this.#listeners.delete(listener); }
  #emit(event) { for (const listener of this.#listeners) listener(event); }
  context(bindingId) {
    const panel = this.#plan.panels.find(p => p.id === bindingId);
    if (!panel || this.#closed) throw Error('This workspace binding is unavailable.');
    return immutable({revision:this.#plan.revision, focus:this.#plan.focus, binding:panel.id,
      selected: this.#selection.get(panel.continuity) ?? null, epoch:this.#epoch, ownerVersion:panel.adapter.version?.()});
  }
  select(bindingId, selected) {
    const panel = this.#plan.panels.find(p => p.id === bindingId), context = this.context(bindingId);
    if (!panel.continuity) throw Error('This binding has no selection continuity.');
    if (selected !== null && (typeof panel.adapter.acceptSelection !== 'function' || !panel.adapter.acceptSelection(selected, context))) throw Error('Selection does not belong to this source.');
    if (Object.is(this.#selection.get(panel.continuity) ?? null,selected)) return false;
    this.#selection.set(panel.continuity,selected); this.#epoch++;
    this.#emit({type:'selection', continuity:panel.continuity, selected, origin:bindingId}); return true;
  }
  available(commandId) {
    const command = this.#plan.commands.get(commandId);
    if (!command || this.#closed) return {enabled:false,reason:'This command is not part of the current workspace.'};
    if (this.#running.has(commandId)) return {enabled:false,reason:'Operation in progress.'};
    let result;
    try {result = command.enabled(this.context(command.binding));} catch {return {enabled:false,reason:'The owning workspace is currently unavailable.'};}
    return result === true ? {enabled:true,reason:''} : {enabled:false,reason:typeof result === 'string' ? result : 'Unavailable in the current selection.'};
  }
  commands(query='') {
    const words = String(query).toLocaleLowerCase().trim().split(/\s+/u).filter(Boolean);
    return [...this.#plan.commands.values()].filter(c => words.every(w => `${c.label} ${c.group ?? ''} ${c.id}`.toLocaleLowerCase().includes(w)))
      .map(c => ({...c,...this.available(c.id)}));
  }
  async execute(commandId) {
    const command = this.#plan.commands.get(commandId), availability = this.available(commandId);
    if (!availability.enabled) throw Error(availability.reason);
    const context = this.context(command.binding), controller = new AbortController();
    this.#running.set(commandId,controller); this.#emit({type:'commands'});
    try {
      // Context captured here must still be checked by the domain owner at write.
      const result = await command.run({...context,signal:controller.signal,isCurrent:()=>!this.#closed && context.epoch===this.#epoch && Object.is(context.ownerVersion,this.context(command.binding).ownerVersion)});
      if (!this.#closed) {this.#emit({type:'result',id:commandId,result});}
      return result;
    } finally { this.#running.delete(commandId); if(!this.#closed)this.#emit({type:'commands'}); }
  }
  refresh() { if (!this.#closed) this.#emit({type:'commands'}); }
  rebind(scene, contributions) {
    if(this.#closed)throw Error('Workspace is closed.');
    const next = compileWorkspace(scene,contributions);
    // An active editor must decide how to retain its draft before its source changes.
    if (this.#running.size || this.#plan.panels.some(p=>p.adapter.dirty?.())) throw Error('Finish or retain open work before changing the workspace source.');
    this.#plan=next; this.#selection.clear(); this.#epoch++; this.#emit({type:'rebind'});
  }
  dispose() {if(this.#closed)return;this.#closed=true;for(const c of this.#running.values())c.abort();this.#running.clear();this.#listeners.clear();this.#selection.clear();}
}
